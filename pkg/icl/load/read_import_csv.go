// Copyright 2016  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package load

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/encoding/csv"
	"github.com/znbasedb/znbase/pkg/util/errorutil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

type csvInputReader struct {
	flowCtx        *runbase.FlowCtx
	kvCh           chan KVBatch
	recordCh       chan csvRecord
	batchSize      int
	batch          csvRecord
	opts           roachpb.CSVOptions
	walltime       int64
	tableDesc      *sqlbase.TableDescriptor
	expectedCols   int
	metrics        *Metrics
	fileContext    importFileContext
	znsparkInput   bool
	conversionFunc map[string]*roachpb.ConversionFunc
}

func (c *csvInputReader) saveRejectRecord(
	ctx context.Context, cp *readImportDataProcessor, group *ctxgroup.Group,
) error {
	var rejected chan string
	rejected = make(chan string)
	job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
	if err != nil {
		return err
	}
	details := job.Details().(jobspb.ImportDetails)
	c.fileContext = importFileContext{
		skip:     0,
		maxRows:  details.Format.MaxRejectedRows,
		rejected: rejected,
		jobID:    cp.spec.Progress.JobID,
		flowCtx:  cp.flowCtx,
	}
	group.GoCtx(func(ctx context.Context) error {
		var buf []byte
		job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
		if job == nil || err != nil {
			return err
		}
		details := job.Details().(jobspb.ImportDetails)
		path := ""
		if details.Format.RejectedAddress != "" {
			path = details.Format.RejectedAddress
		} else {
			if cp.spec.Uri != nil && len(cp.spec.Uri) != 0 {
				for _, v := range cp.spec.Uri {
					if v.Path != "" {
						urlPath, err := url.Parse(v.Path)
						if err != nil {
							return err
						}
						index := strings.LastIndexAny(urlPath.Path, string(os.PathSeparator))
						if index >= len(urlPath.Path) {
							return errors.New("file path err")
						}
						urlPath.Path = urlPath.Path[:index+1]
						path = urlPath.String()
						break
					}
				}

			} else {
				return errors.New("file path cannot be empty")
			}
		}
		fileName := path
		rejectedRows := int64(0)
		for s := range rejected {
			//atomic.AddInt64(&details.Format.RejectedRows,1)
			rejectedRows++
			c.fileContext.skip++
			buf = append(buf, s...)
		}
		if rejectedRows == 0 {
			// no rejected rows
			return nil
		}
		rejFn, err := rejectedFilename(fileName, strconv.FormatInt(int64(cp.spec.Progress.JobID), 10))
		//cp.spec.Format.RejectedAddress=rejFn
		job, err = cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
		if err != nil {
			return errors.New("too many parse errors,please check if there is a problem with the imported data file")
		}
		{
			if err := job.FractionDetailProgressed(ctx,
				func(ctx context.Context, details jobspb.Details, progress jobspb.ProgressDetails) float32 {
					detail := details.(*jobspb.Payload_Import).Import
					detail.Format.RejectedAddress = rejFn
					prog := progress.(*jobspb.Progress_Import).Import
					return prog.Completed()
				},
			); err != nil {
				return err
			}
		}
		conf, err := dumpsink.ConfFromURI(ctx, rejFn)
		if err != nil {
			return err
		}
		rejectedStorage, err := cp.flowCtx.Cfg.DumpSink(ctx, conf)
		if err != nil {
			return err
		}
		defer rejectedStorage.Close()
		err = rejectedStorage.WriteFile(ctx, "", bytes.NewReader(buf))
		return err
	})
	return nil
}

var _ inputConverter = &csvInputReader{}

func newCSVInputReader(
	kvCh chan KVBatch,
	opts roachpb.CSVOptions,
	walltime int64,
	tableDesc *sqlbase.TableDescriptor,
	flowCtx *runbase.FlowCtx,
	metrics *Metrics,
	fileContext importFileContext,
	dateTransforms map[string]*roachpb.ConversionFunc,
) *csvInputReader {
	//hash分区表列属于隐藏列，非数据列。
	expectedCols := len(tableDesc.VisibleColumns())
	if tableDesc.IsHashPartition {
		expectedCols--
	}
	return &csvInputReader{
		flowCtx:        flowCtx,
		opts:           opts,
		walltime:       walltime,
		kvCh:           kvCh,
		expectedCols:   expectedCols,
		tableDesc:      tableDesc,
		recordCh:       make(chan csvRecord, storageicl.LoadRecordChCap(flowCtx.Cfg.Settings)),
		batchSize:      int(storageicl.LoadRecordBatchSize(flowCtx.Cfg.Settings)),
		metrics:        metrics,
		conversionFunc: dateTransforms,
	}
}

func (c *csvInputReader) start(group ctxgroup.Group) {
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "convertcsv")
		defer tracing.FinishSpan(span)
		defer close(c.kvCh)
		defer c.metrics.ConvertGoroutines.Update(0)
		//转换限制转换并发处理数,用户自定义并发数，可减缓cpu负载从而减少rpc error
		concurrentNumber := storageicl.LoadConcurrency(c.flowCtx.Cfg.Settings)
		c.metrics.ConvertGoroutines.Update(concurrentNumber)
		kvBatchSize = int(storageicl.LoadKvRecordSize(c.flowCtx.Cfg.Settings))
		defer c.closeRejectCh(ctx)
		return ctxgroup.GroupWorkers(ctx, int(concurrentNumber), func(ctx context.Context) error {
			return c.convertRecordWorker(ctx)
		})
	})
}

func (c *csvInputReader) inputFinished(_ context.Context) {
	close(c.recordCh)
}

func (c *csvInputReader) closeRejectCh(_ context.Context) {
	if c.fileContext.rejected != nil {
		close(c.fileContext.rejected)
	}
}
func (c *csvInputReader) readFiles(
	ctx context.Context,
	cp *readImportDataProcessor,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
	details jobspb.ImportDetails,
) error {
	return readInputFiles(ctx, cp, c.readFile, &c.fileContext, encryption, encoding, details)
}

func (c *csvInputReader) flushBatch(ctx context.Context, finished bool, progFn progressFn) error {
	// if the batch isn't empty, we need to flush it.
	before := timeutil.Now()
	if len(c.batch.r) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.recordCh <- c.batch:
		}
	}
	after := timeutil.Now()
	c.metrics.ReadWaitTime.RecordValue(after.Sub(before).Nanoseconds())
	if progressErr := progFn(finished); progressErr != nil {
		return progressErr
	}
	if !finished {
		c.batch.r = make([][]string, 0, c.batchSize)
	}
	return nil
}

func handleEncryptedFile(
	input io.Reader, encryption *roachpb.FileEncryptionOptions,
) (io.Reader, error) {
	//TODO 暂时加密文件为小数据量,但是此处存在隐患。不应该使用ReadAll.需要重构一下Read()方法以及加密解密方式
	if encryption != nil {
		bufByte, err := ioutil.ReadAll(input)
		if err != nil {
			return input, err
		}
		byte, err := storageicl.DecryptFile(bufByte, encryption.Key)
		if err != nil {
			return input, err
		}
		input = bytes.NewReader(byte)
	}
	return input, nil
}

func readPreCheck(cr *csv.Reader, encoding string) error {
	if err := cr.JudgeDelimiter(); err != nil {
		return err
	}
	if encoding != "utf8" && encoding != "gbk" && encoding != "" {
		return errors.Errorf("Unsupported encoding format: %v", encoding)
	}
	return nil
}

func (c *csvInputReader) handleRecordByOpts(
	ctx context.Context,
	i int,
	record []string,
	input io.Reader,
	cr *csv.Reader,
	fileContext *importFileContext,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
	encryptionIndex *int,
	loadMetrics *Metrics,
) error {
	// TODO: Temporary method, need to improve
	var err error
	if i == 1 && encoding != "" {
		record, cr, err = decodeToGBK(record, input, cr, c)
		if err != nil {
			return err
		}
	}
	if i == *encryptionIndex {
		if len(record) != 0 {
			if encryption == nil && storageicl.AppearsEncrypted([]byte(record[0])) {
				return errors.Errorf("file appears encrypted -- try specifying %q", dumpOptEncPassphrase)
			}
		} else {
			*encryptionIndex++
		}
	}
	if c.opts.Rowma != 0 {
		s := string(cr.Rowma)
		n := len(record)
		record[n-1] = strings.TrimSuffix(record[n-1], s)
	}
	// Ignore the first N lines.
	if uint32(i) <= c.opts.Skip {
		return nil
	}
	if len(record) == c.expectedCols {
		// Expected number of columns.
	} else if len(record) == c.expectedCols+1 && record[c.expectedCols] == "" {
		// Line has the optional trailing comma, ignore the empty field.
		record = record[:c.expectedCols]
	} else {
		rowError := newImportRowError(
			errors.Errorf("row %d: expected %d fields, got %d", i, c.expectedCols, len(record)),
			strRecord(record, c.opts.Comma),
			int64(i))
		err := handleCorruptRow(ctx, fileContext, rowError)
		if err != nil {
			return err
		}
		return nil
	}
	loadMetrics.LoadReadRecordTotal.Inc(1)
	c.batch.r = append(c.batch.r, record)
	return nil
}

func (c *csvInputReader) readFile(
	ctx context.Context,
	input io.Reader,
	inputIdx int32,
	inputName *distsqlpb.ReadImportDataSpec_TableURL,
	progressFn progressFn,
	fileContext *importFileContext,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
) error {
	var err error
	input, err = handleEncryptedFile(input, encryption)
	if err != nil {
		return err
	}
	cr := csv.NewReader(input)
	c.assignmentCR(cr)
	err = readPreCheck(cr, encoding)
	if err != nil {
		return err
	}
	if c.opts.HasEscape == true || c.opts.Enclose == roachpb.CSVOptions_Always {
		return c.Loadfile(ctx, input, inputIdx, inputName.Path, progressFn, fileContext, encryption)
	}
	c.batch = csvRecord{
		file:      inputName.Path,
		fileIndex: inputIdx,
		rowOffset: 1,
		r:         make([][]string, 0, c.batchSize),
	}
	loadMetrics := c.flowCtx.Cfg.JobRegistry.MetricsStruct().Load.(*Metrics)
	encryptionIndex := 1
	for i := 1; ; i++ {
		if inputName.EndOffset != 0 {
			if cr.ReadSize() == (inputName.EndOffset - inputName.Offset) {
				if err := c.flushBatch(ctx, true, progressFn); err != nil {
					return err
				}
				c.batch.rowOffset = i
				break
			}
		}
		record, err := cr.Read()
		//log.Errorf(ctx,"当前行数据，:%v",record)
		finished := err == io.EOF
		if finished || len(c.batch.r) >= c.batchSize {
			if err := c.flushBatch(ctx, finished, progressFn); err != nil {
				return err
			}
			c.batch.rowOffset = i
		}
		if finished {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "row %d: reading CSV record", i)
		}
		err = c.handleRecordByOpts(ctx, i, record, input, cr, fileContext, encryption, encoding, &encryptionIndex, loadMetrics)
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeToGBK(
	record []string, input io.Reader, cr *csv.Reader, c *csvInputReader,
) ([]string, *csv.Reader, error) {
	judgeRecord := strings.Join(record, "")
	judge := utf8.ValidString(judgeRecord)
	if !judge {
		for i, re := range record {
			temp, _, err := transform.String(simplifiedchinese.GBK.NewDecoder(), re)
			if err != nil {
				return nil, nil, err
			}
			record[i] = temp
		}
		input = transform.NewReader(cr.R, simplifiedchinese.GBK.NewDecoder())
		//todo list
		//read, ok := input.(csvbufio.ReadSeekCloser)
		//if !ok {
		//	return nil, nil, errors.New("gbk read transform failed")
		//}
		cr = csv.NewReader(input)
		c.assignmentCR(cr)
	}
	return record, cr, nil
}

// assignmentCR assignments the values of c to cr
func (c *csvInputReader) assignmentCR(cr *csv.Reader) {
	if c.opts.Comma != 0 {
		cr.Comma = c.opts.Comma
	}
	if c.opts.Rowma != 0 {
		cr.Rowma = c.opts.Rowma
	}
	if c.opts.Escape != 0 {
		cr.Escape = c.opts.Escape
	}
	if c.opts.Encloser != 0 {
		cr.Encloser = c.opts.Encloser
	}
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.Comment = c.opts.Comment
}

type csvRecord struct {
	r         [][]string
	file      string
	fileIndex int32
	rowOffset int
	//mu        struct {
	//	syncutil.Mutex
	//	sources map[int32]int64
	//}
	sources []int64
}

// convertRecordWorker converts CSV records into KV pairs and sends them on the
// kvCh chan.
func (c *csvInputReader) convertRecordWorker(ctx context.Context) error {
	// Create a new evalCtx per converter so each go routine gets its own
	// collationenv, which can't be accessed in parallel.
	conv, err := newRowConverter(c.tableDesc, c.flowCtx.NewEvalCtx(), c.kvCh)
	if err != nil {
		return err
	}
	if c.znsparkInput {
		conv.kafkaDataSource = true
	}
	if conv.evalCtx.SessionData == nil {
		panic("uninitialized session data")
	}

	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	const precision = uint64(10 * time.Microsecond)
	timestamp := uint64(c.walltime-epoch) / precision
	befor := timeutil.Now()
	after := timeutil.Now()
	for batch := range c.recordCh {
		after = timeutil.Now()
		c.metrics.ConversionWaitTime.RecordValue(after.Sub(befor).Nanoseconds())
		isContinue := false
		for batchIdx, record := range batch.r {
			isContinue = false
			rowNum := int64(batch.rowOffset + batchIdx)
			for i, v := range record {
				col := conv.visibleCols[i]
				if c.opts.NullEncoding != nil && v == *c.opts.NullEncoding {
					conv.datums[i] = tree.DNull
				} else {
					if c.opts.NullEncoding != nil && c.opts.Null == *c.opts.NullEncoding {
						v, err = escapeString(v)
						if err != nil {
							rowError := newImportRowError(
								errors.Wrapf(err, "parse %q as %s", col.Name, col.Type.SQLString()),
								strRecord(record, c.opts.Comma),
								rowNum)
							if err = handleCorruptRow(ctx, &c.fileContext, rowError); err != nil {
								//return err
								return makeRowErr(batch.file, rowNum, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
							}
							isContinue = true

						}
					}
					var err error
					if c.conversionFunc != nil {
						if conversionFunc, ok := c.conversionFunc[col.Name]; ok {
							v, err = covertFunc(v, conversionFunc)
							if err != nil {
								return err
							}
						}
					}
					conv.datums[i], err = tree.ParseDatumStringAs(conv.visibleColTypes[i], v, conv.evalCtx, true)
					if err != nil {
						err = newImportRowError(
							errors.Wrapf(err, "parse %q as %s", col.Name, col.Type.SQLString()),
							strRecord(record, c.opts.Comma),
							rowNum)
						if err = handleCorruptRow(ctx, &c.fileContext, err); err != nil {
							//return err
							return makeRowErr(batch.file, rowNum, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
						}
						isContinue = true
						//return makeRowErr(batch.file, rowNum, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
					}
				}
			}
			if isContinue {
				continue
			}
			rowIndex := int64(timestamp) + rowNum
			if err := conv.row(ctx, batch.fileIndex, rowIndex, c.metrics); err != nil {
				rowErr := newImportRowError(
					errors.Wrapf(err, "%q: row %d: ", batch.file, rowNum),
					strRecord(record, c.opts.Comma),
					rowNum)
				if rowErr = handleCorruptRow(ctx, &c.fileContext, rowErr); rowErr != nil {
					//return err
					return makeRowErr(batch.file, rowNum, "%s", err)
				}
				isContinue = true
			}
			if isContinue {
				continue
			}
		}
		befor = timeutil.Now()
	}
	c.metrics.ConversionWaitTime.RecordValue(0)
	return conv.sendBatch(ctx, c.metrics)
}

func covertFunc(v string, conversionFunc *roachpb.ConversionFunc) (string, error) {
	switch conversionFunc.Name {
	case "STR_TO_DATE":
		if len(conversionFunc.Param) != 1 {
			return "", errors.Errorf("func str_to_Date expect 1 param ,got : %d", len(conversionFunc.Param))
		}
		return tree.StrToDate(v, conversionFunc.Param[0], false)
	default:
		return "", errors.New("the conversion func is not currently supported")
	}
}

// importRowError is an error type describing malformed import data.
type importRowError struct {
	err    error
	row    string
	rowNum int64
}

func (e *importRowError) Error() string {
	return fmt.Sprintf("error parsing row %d: %v (row: %q)", e.rowNum, e.err, e.row)
}

func newImportRowError(err error, row string, num int64) error {
	return &importRowError{
		err:    err,
		row:    row,
		rowNum: num,
	}
}

// importFileContext describes state specific to a file being imported.
type importFileContext struct {
	maxRows  int64       // 允许转换失败的最大行数
	skip     int64       // 已经转换失败并跳过的行数
	rejected chan string // Channel for reporting corrupt "rows"
	jobID    int64       //获取当前导入的JOB ID
	flowCtx  *runbase.FlowCtx
}

// handleCorruptRow reports an error encountered while processing a row
// in an input file.
func handleCorruptRow(ctx context.Context, fileCtx *importFileContext, err error) error {
	//TODO 此处决定什么样的错误不终止JOB,目前此处先吃掉所有的转换错误,吃掉的行数大小为默认值1000，之后写成参数设置
	if rowErr := (*importRowError)(nil); errorutil.As(err, &rowErr) && fileCtx != nil && fileCtx.rejected != nil {
		log.Errorf(ctx, "a conversion error occurred during the loading process, and the reason for the error:%v,rows:%v,content:%v", rowErr.err, rowErr.rowNum, rowErr.row)
		job, err := fileCtx.flowCtx.Cfg.JobRegistry.LoadJob(ctx, fileCtx.jobID)
		if err != nil {
			return errors.New("too many parse errors,please check if there is a problem with the imported data file")
		}
		isDouble := false
		{
			if err := job.FractionDetailProgressed(ctx,
				func(ctx context.Context, details jobspb.Details, progress jobspb.ProgressDetails) float32 {
					detail := details.(*jobspb.Payload_Import).Import
					detail.Format.RejectedRows++
					prog := progress.(*jobspb.Progress_Import).Import
					if prog.SamplingProgress == nil && !detail.IngestDirectly {
						isDouble = true
					}
					return prog.Completed()
				},
			); err != nil {
				return err
			}
		}
		maxRows := fileCtx.maxRows
		if isDouble {
			maxRows = fileCtx.maxRows * 2
		}
		details := job.Details().(jobspb.ImportDetails)
		if atomic.LoadInt64(&details.Format.RejectedRows) > maxRows {
			return errors.New("too many parse errors,please check if there is a problem with the imported data file")
		}
		fileCtx.rejected <- rowErr.row + "\n"
		return nil
	}
	return err
}

func strRecord(record []string, sep rune) string {
	csvSep := ","
	if sep != 0 {
		csvSep = string(sep)
	}
	return strings.Join(record, csvSep)
}
func escapeString(value string) (string, error) {
	reg, err := regexp.MatchString("^\\\\[\\\\]+N$", value)
	if err != nil {
		return value, err
	}
	if !reg {
		return value, nil
	}
	valueLen := len(value)
	if valueLen < 3 {
		return value, nil
	}
	l := (valueLen - 1) / 2
	value = value[valueLen-l-1 : valueLen]
	return value, nil

}

// readText is used to get data
func (c *csvInputReader) readText(
	ctx context.Context,
	input io.Reader,
	inputIdx int32,
	inputName string,
	progressFn progressFn,
	fileContext *importFileContext,
	encryption *roachpb.FileEncryptionOptions,
	ch chan []string,
) error {
	defer close(ch)
	var count int64 = 1

	var field []byte
	var record []string
	isEncrypted := false
	// If we have an escaping char defined, seeing it means the next char is to be
	// treated as escaped -- usually that means literal but has some specific
	// mappings defined as well.
	var nextLiteral bool

	// If we have an enclosing char defined, seeing it begins reading a field --
	// which means we do not look for separators until we see the end of the field
	// as indicated by the matching enclosing char.
	var readingField bool

	var gotNull bool

	reader := bufio.NewReader(input)
	addField := func() error {
		if gotNull {
			if len(field) != 0 {
				return makeRowErr(inputName, count, "unexpected data with null encoding: %s", field)
			}
			record = append(record, "NULL")
			gotNull = false
		} else {
			record = append(record, string(field))
		}
		if !isEncrypted {
			isEncrypted = true
			if len(record) != 0 {
				if encryption == nil && storageicl.AppearsEncrypted([]byte(record[0])) {
					return errors.Errorf("file appears encrypted -- try specifying %q", dumpOptEncPassphrase)
				}
			}
		}
		field = field[:0]
		return nil
	}
	addRow := func() error {
		ch <- record
		record = nil
		return nil
	}

	for {
		r, w, err := reader.ReadRune()
		finished := err == io.EOF
		// First check that if we're done and everything looks good.
		if finished {
			if nextLiteral {
				return makeRowErr(inputName, count, "unmatched literal")
			}
			if readingField {
				return makeRowErr(inputName, count, "unmatched field enclosure")
			}
			if len(field) > 0 {
				if err := addField(); err != nil {
					return err
				}
			}
		}

		if finished {
			break
		}

		if err != nil {
			return err
		}
		if r == unicode.ReplacementChar && w == 1 {
			if err := reader.UnreadRune(); err != nil {
				return err
			}
			raw, err := reader.ReadByte()
			if err != nil {
				return err
			}
			field = append(field, raw)
			continue
		}

		// Do we need to check for escaping?
		if c.opts.HasEscape {
			if nextLiteral {
				nextLiteral = false
				// See https://dev.mysql.com/doc/refman/8.0/en/load-data.html.
				switch r {
				case '0':
					field = append(field, byte(0))
				case 'b':
					field = append(field, '\b')
				case 'n':
					field = append(field, '\n')
				case 'r':
					field = append(field, '\r')
				case 't':
					field = append(field, '\t')
				case 'Z':
					field = append(field, byte(26))
				case 'N':
					if gotNull {
						return makeRowErr(inputName, count, "unexpected null encoding")
					}
					gotNull = true
				default:
					field = append(field, string(r)...)
				}
				continue
			}

			if r == c.opts.Escape {
				nextLiteral = true
				continue
			}
		}

		// If enclosing is not disabled, check for the encloser.
		// Technically when it is not optional, we could _require_ it to start and
		// end fields, but for the purposes of decoding, we don't actually care --
		// we'll handle it if we see it either way.
		if c.opts.Enclose != roachpb.CSVOptions_Never && r == c.opts.Encloser {
			readingField = !readingField
			continue
		}

		// Are we done with the field, or even the whole row?
		if !readingField && ((r == c.opts.Comma) || (r == c.opts.Rowma)) {
			if err := addField(); err != nil {
				return err
			}
			if r == c.opts.Rowma {
				if err := addRow(); err != nil {
					return err
				}
			}
			continue
		}
		field = append(field, string(r)...)
	}
	return nil
}

//  It is used to import data from CSV
func (c *csvInputReader) Loadfile(
	ctx context.Context,
	input io.Reader,
	inputIdx int32,
	inputName string,
	progressFn progressFn,
	fileContext *importFileContext,
	encryption *roachpb.FileEncryptionOptions,
) error {
	ch := make(chan []string, 1)
	group := ctxgroup.WithContext(ctx)
	group.GoCtx(func(ctx context.Context) error {
		return c.readText(ctx, input, inputIdx, inputName, progressFn, fileContext, encryption, ch)
	})
	loadMetrics := c.flowCtx.Cfg.JobRegistry.MetricsStruct().Load.(*Metrics)
	i := 0
	for record := range ch {
		if len(c.batch.r) >= c.batchSize {
			if err := c.flushBatch(ctx, false, progressFn); err != nil {
				return err
			}
			c.batch.rowOffset = i + 1
		}
		if uint32(i+1) <= c.opts.Skip {
			continue
		}
		if len(record) == c.expectedCols {
			// Expected number of columns.
		} else if len(record) == c.expectedCols+1 && record[c.expectedCols] == "" {
			// Line has the optional trailing comma, ignore the empty field.
			record = record[:c.expectedCols]
		} else {
			//return errors.Errorf("row %d: expected %d fields, got %d", i, c.expectedCols, len(record))
			rowError := newImportRowError(
				errors.Errorf("row %d: expected %d fields, got %d", i+1, c.expectedCols, len(record)),
				strRecord(record, c.opts.Comma),
				int64(i+1))
			err := handleCorruptRow(ctx, fileContext, rowError)
			if err != nil {
				return err
			}
			continue
		}
		loadMetrics.LoadReadRecordTotal.Inc(1)
		c.batch.r = append(c.batch.r, record)
	}
	if err := c.flushBatch(ctx, true, progressFn); err != nil {
		return err
	}
	c.batch.rowOffset = i + 1

	return group.Wait()
}
