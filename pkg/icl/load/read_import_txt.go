// Copyright 2016  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package load

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
	"github.com/znbasedb/znbase/pkg/util/encoding/txt"
	"github.com/znbasedb/znbase/pkg/util/errorutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

type txtInputReader struct {
	flowCtx      *runbase.FlowCtx
	kvCh         chan KVBatch
	recordCh     chan txtRecord
	batchSize    int
	batch        txtRecord
	opts         roachpb.TXTOptions
	walltime     int64
	tableDesc    *sqlbase.TableDescriptor
	expectedCols int
	metrics      *Metrics
	fileContext  importFileContext
}

func (c *txtInputReader) saveRejectRecord(
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
	}
	group.GoCtx(func(ctx context.Context) error {
		var buf []byte
		job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
		details := job.Details().(jobspb.ImportDetails)
		path := ""
		if details.Format.RejectedAddress != "" {
			path = details.Format.RejectedAddress
		} else {
			if cp.spec.Uri != nil && len(cp.spec.Uri) != 0 {
				for _, v := range cp.spec.Uri {
					if v.Path != "" {
						path = v.Path
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
		rejFn, err := rejectedFilenameTxt(fileName, strconv.FormatInt(int64(cp.spec.Progress.JobID), 10))
		//cp.spec.Format.RejectedAddress=rejFn
		job, err = cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
		if err != nil {
			return errors.New("too many parse errors,job canceled")
		}
		details = job.Details().(jobspb.ImportDetails)
		details.Format.RejectedAddress = rejFn
		atomic.AddInt64(&details.Format.RejectedRows, rejectedRows)
		err = job.SetDetails(ctx, details)
		if err != nil {
			return err
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

var _ inputConverter = &txtInputReader{}

func newTXTInputReader(
	kvCh chan KVBatch,
	opts roachpb.TXTOptions,
	walltime int64,
	tableDesc *sqlbase.TableDescriptor,
	flowCtx *runbase.FlowCtx,
	metrics *Metrics,
	fileContext importFileContext,
) *txtInputReader {
	return &txtInputReader{
		flowCtx:      flowCtx,
		opts:         opts,
		walltime:     walltime,
		kvCh:         kvCh,
		expectedCols: len(tableDesc.VisibleColumns()),
		tableDesc:    tableDesc,
		recordCh:     make(chan txtRecord, storageicl.LoadRecordChCap(flowCtx.Cfg.Settings)),
		batchSize:    int(storageicl.LoadRecordBatchSize(flowCtx.Cfg.Settings)),
		metrics:      metrics,
	}
}

func (c *txtInputReader) start(group ctxgroup.Group) {
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "converttxt")
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

func (c *txtInputReader) inputFinished(_ context.Context) {
	close(c.recordCh)
}

func (c *txtInputReader) closeRejectCh(_ context.Context) {
	if c.fileContext.rejected != nil {
		close(c.fileContext.rejected)
	}
}
func (c *txtInputReader) readFiles(
	ctx context.Context,
	cp *readImportDataProcessor,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
	details jobspb.ImportDetails,
) error {
	return readInputFiles(ctx, cp, c.readFile, &c.fileContext, encryption, encoding, details)
}

func (c *txtInputReader) flushBatch(ctx context.Context, finished bool, progFn progressFn) error {
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

func (c *txtInputReader) readFile(
	ctx context.Context,
	input io.Reader,
	inputIdx int32,
	inputName *distsqlpb.ReadImportDataSpec_TableURL,
	progressFn progressFn,
	fileContext *importFileContext,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
) error {
	isEncrypted := false
	c.batch = txtRecord{
		file:      inputName.Path,
		fileIndex: inputIdx,
		rowOffset: 1,
		r:         make([][]string, 0, c.batchSize),
	}
	if encryption != nil {
		bufByte, err := ioutil.ReadAll(input)
		if err != nil {
			return err
		}
		byte, err := storageicl.DecryptFile(bufByte, encryption.Key)
		if err != nil {
			return err
		}
		input = bytes.NewReader(byte)
		if err != nil {
			return err
		}
	}
	if encoding != "utf8" && encoding != "gbk" && encoding != "" {
		return errors.Errorf("Unsupported encoding format: %v", encoding)
	}
	cr := txt.NewReader(input)
	c.assignmentCR(cr)
	loadMetrics := c.flowCtx.Cfg.JobRegistry.MetricsStruct().Load.(*Metrics)
	for i := 1; ; i++ {
		record, err := cr.Read()
		if i == 1 && encoding != "" {
			judgeRecord := strings.Join(record, "")
			judge := utf8.ValidString(judgeRecord)
			if !judge {
				temp := make([]string, len(record))
				for i, re := range record {
					temp[i], _, err = transform.String(simplifiedchinese.GBK.NewDecoder(), re)
					if err != nil {
						return err
					}
				}
				input = transform.NewReader(cr.R, simplifiedchinese.GBK.NewDecoder())
				cr = txt.NewReader(input)
				c.assignmentCR(cr)
			}
		}
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
		if !isEncrypted {
			isEncrypted = true
			if len(record) != 0 {
				if encryption == nil && storageicl.AppearsEncrypted([]byte(record[0])) {
					return errors.Errorf("file appears encrypted -- try specifying %q", dumpOptEncPassphrase)
				}
			}
		}
		if err != nil {
			return errors.Wrapf(err, "row %d: reading TXT record", i)
		}
		// Ignore the first N lines.
		if uint32(i) <= c.opts.Skip {
			continue
		}
		if len(record) == c.expectedCols {
			// Expected number of columns.
		} else if len(record) == c.expectedCols+1 && record[c.expectedCols] == "" {
			// Line has the optional trailing comma, ignore the empty field.
			record = record[:c.expectedCols]
		} else {
			//return errors.Errorf("row %d: expected %d fields, got %d", i, c.expectedCols, len(record))
			rowError := newImportRowErrortxt(
				errors.Errorf("row %d: expected %d fields, got %d", i, c.expectedCols, len(record)),
				strRecordTXT(record, c.opts.Comma),
				int64(i))
			err := handleCorruptRowtxt(ctx, fileContext, rowError)
			if err != nil {
				return err
			}
			continue
		}
		loadMetrics.LoadReadRecordTotal.Inc(1)
		c.batch.r = append(c.batch.r, record)
	}
	return nil
}

// assignmentCR assignments the values of c to cr
func (c *txtInputReader) assignmentCR(cr *txt.Reader) {
	if c.opts.Comma != 0 {
		cr.Comma = c.opts.Comma
	}
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.Comment = c.opts.Comment
}

type txtRecord struct {
	r         [][]string
	file      string
	fileIndex int32
	rowOffset int
}

// convertRecordWorker converts TXT records into KV pairs and sends them on the
// kvCh chan.
func (c *txtInputReader) convertRecordWorker(ctx context.Context) error {
	// Create a new evalCtx per converter so each go routine gets its own
	// collationenv, which can't be accessed in parallel.
	conv, err := newRowConverter(c.tableDesc, c.flowCtx.NewEvalCtx(), c.kvCh)
	if err != nil {
		return err
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
						v, err = escapeStringtxt(v)
						if err != nil {
							rowError := newImportRowErrortxt(
								errors.Wrapf(err, "parse %q as %s", col.Name, col.Type.SQLString()),
								strRecordTXT(record, c.opts.Comma),
								rowNum)
							if err = handleCorruptRowtxt(ctx, &c.fileContext, rowError); err != nil {
								//return err
								return makeRowErr(batch.file, rowNum, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
							}
							isContinue = true

						}
					}
					var err error
					conv.datums[i], err = tree.ParseDatumStringAs(conv.visibleColTypes[i], v, conv.evalCtx, true)
					if err != nil {
						err = newImportRowErrortxt(
							errors.Wrapf(err, "parse %q as %s", col.Name, col.Type.SQLString()),
							strRecordTXT(record, c.opts.Comma),
							rowNum)
						if err = handleCorruptRowtxt(ctx, &c.fileContext, err); err != nil {
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
				return makeRowErr(batch.file, rowNum, "%s", err)
			}
		}
		befor = timeutil.Now()
	}
	c.metrics.ConversionWaitTime.RecordValue(0)
	return conv.sendBatch(ctx, c.metrics)
}

// importRowErrortxt is an error type describing malformed import data.
type importRowErrortxt struct {
	err    error
	row    string
	rowNum int64
}

func (e *importRowErrortxt) Error() string {
	return fmt.Sprintf("error parsing row %d: %v (row: %q)", e.rowNum, e.err, e.row)
}

func newImportRowErrortxt(err error, row string, num int64) error {
	return &importRowErrortxt{
		err:    err,
		row:    row,
		rowNum: num,
	}
}

// importFileContext describes state specific to a file being imported.

// handleCorruptRowtxt reports an error encountered while processing a row
// in an input file.
func handleCorruptRowtxt(ctx context.Context, fileCtx *importFileContext, err error) error {
	//TODO 此处决定什么样的错误不终止JOB,目前此处先吃掉所有的转换错误,吃掉的行数大小为默认值1000，之后写成参数设置
	if rowErr := (*importRowErrortxt)(nil); errorutil.As(err, &rowErr) && fileCtx != nil && fileCtx.rejected != nil {
		if fileCtx.skip > fileCtx.maxRows {
			return errors.New("too many parse errors,please check if there is a problem with the imported data file")
		}
		fileCtx.rejected <- rowErr.row + "\n"
		return nil
	}
	return err
}

func strRecordTXT(record []string, sep rune) string {
	txtSep := ","
	if sep != 0 {
		txtSep = string(sep)
	}
	return strings.Join(record, txtSep)
}
func escapeStringtxt(value string) (string, error) {
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
