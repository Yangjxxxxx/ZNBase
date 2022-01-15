// Copyright 2016  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package load

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/icl/load/utils"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/transform"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/bulk"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/storage/storagebase"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/envutil"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

var useWorkloadFastpath = envutil.EnvOrDefaultBool("ZNBASE_IMPORT_WORKLOAD_FASTER", false)

type readFileFunc func(context.Context, io.Reader, int32, *distsqlpb.ReadImportDataSpec_TableURL, progressFn, *importFileContext, *roachpb.FileEncryptionOptions, string) error

func getDumpSink(
	ctx context.Context,
	dataFile *distsqlpb.ReadImportDataSpec_TableURL,
	makeDumpSink dumpsink.Factory,
	format roachpb.IOFileFormat,
) (roachpb.DumpSink, dumpsink.DumpSink, error) {
	conf, err := dumpsink.ConfFromURI(ctx, dataFile.Path)
	if err != nil {
		return conf, nil, err
	}
	if conf.Provider == roachpb.ExportStorageProvider_Http {
		conf.HttpPath.Header = format.HttpHeader
	}
	es, err := makeDumpSink(ctx, conf)
	if err != nil {
		return conf, es, err
	}
	defer es.Close()
	return conf, es, nil
}

func preReadInputFile(
	ctx context.Context,
	cp *readImportDataProcessor,
	dataFileIndex int32,
	dataFile *distsqlpb.ReadImportDataSpec_TableURL,
	fileFunc readFileFunc,
	fileContext *importFileContext,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
	currentFile int,
	totalBytes int64,
	updateFromFiles, updateFromBytes bool,
) error {
	dataFiles := cp.spec.Uri
	format := cp.spec.Format
	makeDumpSink := cp.flowCtx.Cfg.DumpSink

	conf, es, err := getDumpSink(ctx, dataFile, makeDumpSink, format)
	if err != nil {
		return err
	}
	var f io.ReadCloser
	if SupportSeek(conf) {
		f, err = es.Seek(ctx, "", dataFile.Offset, dataFile.Size_, io.SeekStart)
	} else {
		f, err = es.ReadFile(ctx, "")
	}
	if err != nil {
		return err
	}
	defer f.Close()
	bc := &byteCounter{r: f}
	src, err := decompressingReader(bc, dataFile.Path, format.Compression)
	if err != nil {
		return err
	}
	defer src.Close()
	var readBytes int64
	wrappedProgressFn := func(finished bool) error { return nil }
	if updateFromBytes {
		const progressBytes = 100 << 20
		wrappedProgressFn = func(finished bool) error {
			// progressBytes is the number of read bytes at which to report job progress. A
			// low value may cause excessive updates in the job table which can lead to
			// very large rows due to MVCC saving each version.
			if finished || bc.n > progressBytes {
				readBytes += bc.n
				bc.n = 0
				if err := FractionProg(ctx, cp, float32(readBytes)/float32(totalBytes)); err != nil {
					return err
				}
			}
			return nil
		}
	}
	if err := fileFunc(ctx, src, dataFileIndex, dataFile, wrappedProgressFn, fileContext /* rejected */, encryption, encoding); err != nil {
		return errors.Wrap(err, dataFile.Path)
	}
	if updateFromFiles {
		if err := FractionProg(ctx, cp, float32(currentFile)/float32(len(dataFiles))); err != nil {
			return err
		}
	}
	return nil
}

// readInputFile reads each of the passed dataFiles using the passed func. The
// key part of dataFiles is the unique index of the data file among all files in
// the IMPORT.
func readInputFiles(
	ctx context.Context,
	cp *readImportDataProcessor,
	fileFunc readFileFunc,
	fileContext *importFileContext,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
	details jobspb.ImportDetails,
) error {
	dataFiles := cp.spec.Uri
	format := cp.spec.Format
	makeDumpSink := cp.flowCtx.Cfg.DumpSink

	var totalBytes int64
	// Attempt to fetch total number of bytes for all files.
	for _, dataFile := range dataFiles {
		_, es, err := getDumpSink(ctx, dataFile, makeDumpSink, format)
		if err != nil {
			return err
		}
		sz, err := es.Size(ctx, "")
		es.Close()

		if sz <= 0 {
			// Don't log dataFile here because it could leak auth information.
			log.Infof(ctx, "could not fetch file size; falling back to per-file progress: %v", err)
			totalBytes = 0
			break
		}
		totalBytes += sz
	}
	updateFromFiles := totalBytes == 0
	updateFromBytes := totalBytes > 0
	currentFile := 0
	for dataFileIndex, dataFile := range dataFiles {
		currentFile++
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err := preReadInputFile(ctx, cp, dataFileIndex, dataFile, fileFunc,
			fileContext, encryption, encoding, currentFile, totalBytes, updateFromFiles, updateFromBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func rejectedFilename(datafile string, jobID string) (string, error) {
	parsedURI, err := url.Parse(datafile)
	if err != nil {
		return "", err
	}
	fileBase := `rejected.` + jobID + ".csv"
	if strings.Contains(parsedURI.Path, fileBase) {
		return parsedURI.String(), nil
	}
	parsedURI.Path = parsedURI.Path + fileBase
	return parsedURI.String(), nil
}
func rejectedFilenameTxt(datafile string, jobID string) (string, error) {
	parsedURI, err := url.Parse(datafile)
	if err != nil {
		return "", err
	}
	//parsedURI.Path = parsedURI.Path + ".rejected"
	parsedURI.Path = `/rejected.` + jobID + ".txt"
	return parsedURI.String(), nil
}
func decompressingReader(
	in io.Reader, name string, hint roachpb.IOFileFormat_Compression,
) (io.ReadCloser, error) {
	switch guessCompressionFromName(name, hint) {
	case roachpb.IOFileFormat_Gzip:
		return gzip.NewReader(in)
	case roachpb.IOFileFormat_Bzip:
		return ioutil.NopCloser(bzip2.NewReader(in)), nil
	default:
		return ioutil.NopCloser(in), nil
	}
}

func guessCompressionFromName(
	name string, hint roachpb.IOFileFormat_Compression,
) roachpb.IOFileFormat_Compression {
	if hint != roachpb.IOFileFormat_Auto {
		return hint
	}
	switch {
	case strings.HasSuffix(name, ".gz"):
		return roachpb.IOFileFormat_Gzip
	case strings.HasSuffix(name, ".bz2") || strings.HasSuffix(name, ".bz"):
		return roachpb.IOFileFormat_Bzip
	default:
		return roachpb.IOFileFormat_None
	}
}

type byteCounter struct {
	r io.Reader
	n int64
}

func (b *byteCounter) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	return n, err
}

//type kvBatch []roachpb.KeyValue

// KVBatch represents a batch of KVs generated from converted rows.
type KVBatch struct {
	// Source is where the row data in the batch came from.
	Source int32
	// LastRow is the index of the last converted row in source in this batch.
	LastRow int64
	sources []int64
	// KVs is the actual converted KV data.
	KVs []roachpb.KeyValue
}

type rowConverter struct {
	// current row buf
	datums []tree.Datum

	// kv destination and current batch
	kvCh chan<- KVBatch
	//kvCh     chan<- kvBatch
	//kvBatch  kvBatch
	kvBatch  KVBatch
	batchCap int

	tableDesc *sqlbase.ImmutableTableDescriptor

	// The rest of these are derived from tableDesc, just cached here.
	hidden                int
	ri                    row.Inserter
	evalCtx               *tree.EvalContext
	cols                  []sqlbase.ColumnDescriptor
	visibleCols           []sqlbase.ColumnDescriptor
	visibleColTypes       []types.T
	defaultExprs          []tree.TypedExpr
	computedIVarContainer sqlbase.RowIndexedVarContainer
	//数据源是否为kafka
	kafkaDataSource bool
	flushTs         time.Time
}

var kvBatchSize = 5000

func newRowConverter(
	tableDesc *sqlbase.TableDescriptor, evalCtx *tree.EvalContext, kvCh chan<- KVBatch,
) (*rowConverter, error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*tableDesc)
	c := &rowConverter{
		tableDesc: immutDesc,
		kvCh:      kvCh,
		evalCtx:   evalCtx,
	}

	ri, err := row.MakeInserter(nil, nil /* txn */, nil, immutDesc, nil, /* fkTables */
		immutDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}
	c.ri = ri

	var txCtx transform.ExprTransformContext
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(immutDesc.Columns, immutDesc, &txCtx, c.evalCtx)
	if err != nil {
		return nil, errors.Wrap(err, "process default columns")
	}
	c.cols = cols
	c.defaultExprs = defaultExprs

	c.visibleCols = immutDesc.VisibleColumns()
	c.visibleColTypes = make([]types.T, len(c.visibleCols))
	for i := range c.visibleCols {
		c.visibleColTypes[i] = c.visibleCols[i].DatumType()
	}
	c.datums = make([]tree.Datum, len(c.visibleCols), len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	c.hidden = -1
	for i, col := range cols {
		if col.Hidden {
			if col.DefaultExpr == nil || *col.DefaultExpr != "unique_rowid()" || c.hidden != -1 {
				if tableDesc.IsHashPartition && col.Name == "hashnum" {
					continue
				}
				return nil, errors.New("unexpected hidden column")
			}
			c.hidden = i
			c.datums = append(c.datums, nil)
		}
	}
	if len(c.datums) != len(cols) {
		return nil, errors.New("unexpected hidden column")
	}

	padding := 2 * (len(immutDesc.Indexes) + len(immutDesc.Families))
	c.batchCap = kvBatchSize + padding
	c.kvBatch = KVBatch{}
	c.kvBatch.KVs = make([]roachpb.KeyValue, 0, c.batchCap)
	//c.kvBatch = make(kvBatch, 0, c.batchCap)
	c.computedIVarContainer = sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    immutDesc.Columns,
	}
	return c, nil
}

const rowIDBits = 64 - builtins.NodeIDBits

func (c *rowConverter) row(
	ctx context.Context, fileIndex int32, rowIndex int64, metrics *Metrics,
) error {
	if c.hidden >= 0 {
		// We don't want to call unique_rowid() for the hidden PK column because it
		// is not idempotent and has unfortunate overlapping of output spans since
		// it puts the uniqueness-ensuring per-generator part (nodeID) in the
		// low-bits. Instead, make our own IDs that attempt to keep each generator
		// (sourceID) writing to its own key-space with sequential rowIndexes
		// mapping to sequential unique IDs, by putting the rowID in the lower
		// bits. To avoid collisions with the SQL-genenerated IDs (at least for a
		// very long time) we also flip the top bit to 1.
		//
		// Producing sequential keys in non-overlapping spans for each source yields
		// observed improvements in ingestion performance of ~2-3x and even more
		// significant reductions in required compactions during IMPORT.
		//
		// TODO(dt): Note that currently some callers (e.g. CSV IMPORT, which can be
		// used on a table more than once) offset their rowIndex by a wall-time at
		// which their overall job is run, so that subsequent ingestion jobs pick
		// different row IDs for the i'th row and don't collide. However such
		// time-offset rowIDs mean each row imported consumes some unit of time that
		// must then elapse before the next IMPORT could run without colliding e.g.
		// a 100m row file would use 10µs/row or ~17min worth of IDs. For now it is
		// likely that IMPORT's write-rate is still the limiting factor, but this
		// scheme means rowIndexes are very large (1 yr in 10s of µs is about 2^42).
		// Finding an alternative scheme for avoiding collisions (like sourceID *
		// fileIndex*desc.Version) could improve on this. For now, if this
		// best-effort collision avoidance scheme doesn't work in some cases we can
		// just recommend an explicit PK as a workaround.
		avoidCollisionsWithSQLsIDs := uint64(1 << 63)
		rowID := (uint64(fileIndex) << rowIDBits) ^ uint64(rowIndex)
		//当前对于rowid 的生成存在可能会冲突的可能性，而kafka流式导入过程中，会跳过局部数据的主键冲突问题。因此需要对于rowid的生成
		//添加对应的随机数因子，但是因为添加随机数因子会导致rowid 并不是递增生成，因此对于入库会导致速度变慢，并且每次导入相同数据，
		//查询出的数据顺序是随机的。因此对于kafka 流式导入暂时启用随机数，其他数据源导入还继续沿用之前有序的生成方式。
		if c.kafkaDataSource {
			c.datums[c.hidden] = tree.NewDInt(tree.DInt(avoidCollisionsWithSQLsIDs | rowID + rand.Uint64()))
		} else {
			c.datums[c.hidden] = tree.NewDInt(tree.DInt(avoidCollisionsWithSQLsIDs | rowID))
		}
	}

	// TODO(justin): we currently disallow computed columns in import statements.
	var computeExprs []tree.TypedExpr
	var computedCols []sqlbase.ColumnDescriptor

	insertRow, err := sql.GenerateInsertRow(
		c.defaultExprs, computeExprs, c.cols, computedCols, *c.evalCtx, c.tableDesc, c.datums, &c.computedIVarContainer)
	if err != nil {
		for _, c := range c.datums {
			log.Errorf(ctx, "error column  %v", c)
		}
		return errors.Wrapf(err, "generate insert row")
	}
	var pm schemaexpr.PartialIndexUpdateHelper
	if err := c.ri.InsertRow(
		ctx,
		inserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.kvBatch.KVs = append(c.kvBatch.KVs, kv)
		}),
		insertRow,
		true, /* ignoreConflicts */
		row.SkipFKs,
		false, /* traceKV */
		pm,
	); err != nil {
		return errors.Wrapf(err, "insert row")
	}
	// If our batch is full, flush it and start a new one.
	if len(c.kvBatch.KVs) >= kvBatchSize {
		if err := c.sendBatch(ctx, metrics); err != nil {
			return err
		}
	}
	return nil
}

func (c *rowConverter) sendBatch(ctx context.Context, metrics *Metrics) error {
	if len(c.kvBatch.KVs) == 0 {
		return nil
	}
	before := timeutil.Now()
	select {
	case c.kvCh <- c.kvBatch:
	case <-ctx.Done():
		return ctx.Err()
	}
	//记录上次数据发送时间
	c.flushTs = timeutil.Now()
	after := timeutil.Now()
	metrics.KvWriteChannelWaitTime.RecordValue(after.Sub(before).Nanoseconds())
	c.kvBatch.KVs = make([]roachpb.KeyValue, 0, c.batchCap)
	return nil
}

var csvOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}
var txtOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newReadImportDataProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec distsqlpb.ReadImportDataSpec,
	output runbase.RowReceiver,
) (runbase.Processor, error) {
	cp := &readImportDataProcessor{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		output:      output,
	}

	if err := cp.out.Init(&distsqlpb.PostProcessSpec{}, csvOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	if err := cp.out.Init(&distsqlpb.PostProcessSpec{}, txtOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return cp, nil
}

type progressFn func(finished bool) error

type inputConverter interface {
	start(group ctxgroup.Group)
	readFiles(ctx context.Context,
		cp *readImportDataProcessor,
		encryption *roachpb.FileEncryptionOptions,
		encoding string,
		details jobspb.ImportDetails,
	) error
	inputFinished(ctx context.Context)
	saveRejectRecord(ctx context.Context, cp *readImportDataProcessor, group *ctxgroup.Group) error
	closeRejectCh(ctx context.Context)
}

type readImportDataProcessor struct {
	flowCtx     *runbase.FlowCtx
	processorID int32
	spec        distsqlpb.ReadImportDataSpec
	out         runbase.ProcOutputHelper
	output      runbase.RowReceiver
	fileContext importFileContext
}

var _ runbase.Processor = &readImportDataProcessor{}

func (cp *readImportDataProcessor) OutputTypes() []sqlbase.ColumnType {
	switch cp.spec.Format.Format {
	case roachpb.IOFileFormat_CSV:
		return csvOutputTypes
	}
	return txtOutputTypes
}

func isMultiTableFormat(format roachpb.IOFileFormat_FileFormat) bool {
	switch format {
	case roachpb.IOFileFormat_Mysqldump,
		roachpb.IOFileFormat_PgDump:
		return true
	}
	return false
}

func (cp *readImportDataProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "readImportDataProcessor")
	defer log.Info(ctx, "the current job has been completed or exited")
	defer tracing.FinishSpan(span)
	defer closeRejectCh(cp.fileContext.rejected)
	if err := cp.doRun(ctx); err != nil {
		runbase.DrainAndClose(ctx, cp.output, err, func(context.Context) {} /* pushTrailingMeta */)
	} else {
		cp.out.Close()
	}
}

//关闭存储转换错误的通道
func closeRejectCh(rejected chan string) {
	if rejected != nil {
		close(rejected)
	}
}

// FractionProg is periodically invoked with a percentage
// of the total progress of reading through all of the files. This percentage
// attempts to use the Size() method of ExportStorage to determine how many
// bytes must be read of the input files, and reports the percent of bytes read
// among all dataFiles. If any Size() fails for any file, then progress is
// reported only after each file has been read.
func FractionProg(ctx context.Context, cp *readImportDataProcessor, percent float32) error {
	job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
	if err != nil {
		return err
	}

	progFn := func(pct float32) error {
		return job.FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			d := details.(*jobspb.Progress_Import).Import
			slotpct := pct * cp.spec.Progress.Contribution
			if len(d.SamplingProgress) > 0 {
				d.SamplingProgress[cp.spec.Progress.Slot] = slotpct
			} else {
				d.ReadProgress[cp.spec.Progress.Slot] = slotpct
			}
			return d.Completed()
		})
	}
	err = progFn(percent)
	return err
}

func preCheck(
	spec distsqlpb.ReadImportDataSpec,
	singleTable **sqlbase.TableDescriptor,
	format roachpb.IOFileFormat_FileFormat,
) error {
	if len(spec.Tables) == 1 {
		for _, table := range spec.Tables {
			*singleTable = table
		}
	}
	if *singleTable == nil && !isMultiTableFormat(format) {
		return errors.Errorf("%s only supports reading a single, pre-specified table", format.String())
	}
	return nil
}

func newTxtOrCsvInputReader(
	ctx context.Context,
	cp *readImportDataProcessor,
	kvCh chan KVBatch,
	singleTable *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
	metrics *Metrics,
) (inputConverter, error) {
	isWorkload := useWorkloadFastpath
	for _, file := range cp.spec.Uri {
		if conf, err := dumpsink.ConfFromURI(ctx, file.Path); err != nil || conf.Provider != roachpb.ExportStorageProvider_Workload {
			isWorkload = false
			break
		}
	}
	format := cp.spec.Format.Format
	var conv inputConverter
	var err error
	if isWorkload {
		conv, err = newWorkloadReader(kvCh, singleTable, evalCtx, metrics)
	} else {
		if format == roachpb.IOFileFormat_TXT {
			conv = newTXTInputReader(kvCh, cp.spec.Format.Txt, cp.spec.WalltimeNanos, singleTable, cp.flowCtx, metrics, cp.fileContext)
		} else if format == roachpb.IOFileFormat_CSV {
			conv = newCSVInputReader(kvCh, cp.spec.Format.Csv, cp.spec.WalltimeNanos, singleTable, cp.flowCtx, metrics, cp.fileContext, cp.spec.Format.ConversionFunc)
		}
	}
	return conv, err
}

func newInputConv(
	ctx context.Context,
	cp *readImportDataProcessor,
	kvCh chan KVBatch,
	singleTable *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
	metrics *Metrics,
) (inputConverter, error) {
	var conv inputConverter
	var err error
	format := cp.spec.Format.Format
	switch format {
	case roachpb.IOFileFormat_TXT:
		conv, err = newTxtOrCsvInputReader(ctx, cp, kvCh, singleTable, evalCtx, metrics)
	case roachpb.IOFileFormat_CSV:
		conv, err = newTxtOrCsvInputReader(ctx, cp, kvCh, singleTable, evalCtx, metrics)
	case roachpb.IOFileFormat_MysqlOutfile:
		conv, err = newMysqloutfileReader(kvCh, cp.spec.Format.MysqlOut, singleTable, evalCtx, metrics)
	case roachpb.IOFileFormat_Mysqldump:
		conv, err = newMysqldumpReader(kvCh, cp.spec.Tables, evalCtx, metrics)
	case roachpb.IOFileFormat_PgCopy:
		conv, err = newPgCopyReader(kvCh, cp.spec.Format.PgCopy, singleTable, evalCtx, metrics)
	case roachpb.IOFileFormat_PgDump:
		conv, err = newPgDumpReader(kvCh, cp.spec.Format.PgDump, cp.spec.Tables, evalCtx, metrics)
	case roachpb.IOFileFormat_ZNBaseDUMP:
		conv, err = newBiniDumpReader(kvCh, cp.spec.Format.PgDump, cp.spec.Tables, evalCtx, metrics)
	case roachpb.IOFileFormat_Kafka:
		conv = newKafkaInputReader(kvCh, cp.spec.Format.Csv, cp.spec.WalltimeNanos, singleTable, cp.flowCtx, metrics, cp.fileContext)
	default:
		err = errors.Errorf("Requested LOAD format (%d) not supported by this node", format)
	}
	return conv, err
}

func supportSaveRejectFormat(format roachpb.IOFileFormat_FileFormat) bool {
	if format == roachpb.IOFileFormat_CSV ||
		format == roachpb.IOFileFormat_TXT ||
		format == roachpb.IOFileFormat_Kafka {
		return true
	}
	return false
}

// doRun uses a more familiar error return API, allowing concise early returns
// on errors in setup that all then are handled by the actual DistSQL Run method
// wrapper, doing the correct DrainAndClose error handling logic.
func (cp *readImportDataProcessor) doRun(ctx context.Context) error {
	group := ctxgroup.WithContext(ctx)
	kvChCap := storageicl.LoadKvChCap(cp.flowCtx.Cfg.Settings)
	kvCh := make(chan KVBatch, kvChCap)
	evalCtx := cp.flowCtx.NewEvalCtx()
	var singleTable *sqlbase.TableDescriptor
	format := cp.spec.Format.Format
	metrics := cp.flowCtx.Cfg.JobRegistry.MetricsStruct().Load.(*Metrics)
	var err error
	/* 1. preCheck */
	err = preCheck(cp.spec, &singleTable, format)
	if err != nil {
		return err
	}
	/* 2. newInputConv */
	var conv inputConverter
	conv, err = newInputConv(ctx, cp, kvCh, singleTable, evalCtx, metrics)
	if err != nil {
		return err
	}
	/* 3. start */
	conv.start(group)
	/* 4. saveRejectRecord */
	if cp.spec.Format.SaveRejected && supportSaveRejectFormat(format) {
		err := conv.saveRejectRecord(ctx, cp, &group)
		if err != nil {
			return err
		}
	}
	/* 5. Read input files into kvs records */
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "readImportFiles")
		defer tracing.FinishSpan(span)
		defer conv.inputFinished(ctx)

		job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
		if err != nil {
			return err
		}
		return conv.readFiles(ctx, cp, job.Details().(jobspb.ImportDetails).Encryption, job.Details().(jobspb.ImportDetails).Encoding, job.Details().(jobspb.ImportDetails))
	})
	typeBytes := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
	//kafka 流式导入
	if format == roachpb.IOFileFormat_Kafka {
		// IngestDirectly means this reader will just ingest the KVs that the
		// producer emitted to the chan, and the only result we push into distsql at
		// the end is one row containing an encoded BulkOpSummary.
		group.GoCtx(func(ctx context.Context) error {
			ctx, span := tracing.ChildSpan(ctx, "ingestKVs")
			defer tracing.FinishSpan(span)
			//转换限制转换并发处理数,用户自定义并发数，可减缓cpu负载从而减少rpc error
			concurrentNumber := storageicl.LoadIngestConcurrency(cp.flowCtx.Cfg.Settings)
			//countsBytes := make([]byte,0)
			resSummary := roachpb.BulkOpSummary{}
			var consume chan ConsumerOffset
			{
				if kafka, ok := conv.(*kafkaInputReader); ok {
					consume = kafka.consumerCh
				} else {
					return errors.New("there is an internal error in load Kafka, please contact technical staff for handling")
				}
			}
			defer close(consume)
			error := ctxgroup.GroupWorkers(ctx, int(concurrentNumber), func(ctx context.Context) error {
				// Drain the kvCh using the BulkAdder until it closes.
				//var kafkaSink *KafkaSink
				////获取kafkaSink
				//{
				//	var kafkaURLs []string
				//	job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
				//	if err != nil {
				//		return err
				//	}
				//	var groupID string
				//	details := job.Details().(jobspb.ImportDetails)
				//	for _, file := range cp.spec.Uri {
				//		kafkaURLs = append(kafkaURLs, file.Path)
				//		groupID = strconv.FormatInt(file.EndOffset, 10)
				//	}
				//	//用户是否设置groupID
				//	if details.Format.Kafka.GroupId != nil {
				//		groupID = *details.Format.Kafka.GroupId
				//	}
				//	kafkaSink, err = NewKafkaSink(ctx, kafkaURLs, groupID)
				//	if err != nil {
				//		return err
				//	}
				//}
				added, err := ingestKafkaKvs(ctx, cp.flowCtx, &cp.spec, metrics, kvCh, consume)
				if err != nil {
					return err
				}
				//并发处理时，回显数据的整合
				//added := adder.GetSummary()
				atomic.AddInt64(&resSummary.Rows, added.Rows)
				atomic.AddInt64(&resSummary.DataSize, added.DataSize)
				atomic.AddInt64(&resSummary.IndexEntries, added.IndexEntries)
				atomic.AddInt64(&resSummary.SystemRecords, added.SystemRecords)
				return nil
			})
			if error != nil {
				return error
			}
			countsBytes, err := protoutil.Marshal(&resSummary)
			if err != nil {
				return err
			}
			cs, err := cp.out.EmitRow(ctx, sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(countsBytes))),
				sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes([]byte{}))),
			})
			if err != nil {
				return err
			}
			if cs != runbase.NeedMoreRows {
				return errors.New("unexpected closure of consumer")
			}
			return nil
		})
	} else {
		if cp.spec.IngestDirectly {
			// IngestDirectly means this reader will just ingest the KVs that the
			// producer emitted to the chan, and the only result we push into distsql at
			// the end is one row containing an encoded BulkOpSummary.
			group.GoCtx(func(ctx context.Context) error {
				ctx, span := tracing.ChildSpan(ctx, "ingestKVs")
				defer tracing.FinishSpan(span)
				//转换限制转换并发处理数,用户自定义并发数，可减缓cpu负载从而减少rpc error
				concurrentNumber := storageicl.LoadIngestConcurrency(cp.flowCtx.Cfg.Settings)
				writeTS := hlc.Timestamp{WallTime: cp.spec.WalltimeNanos}
				bufferSize := storageicl.LoadBufferSize(cp.flowCtx.Cfg.Settings)
				flushSize := storageicl.LoadFlushSize(cp.flowCtx.Cfg.Settings)
				//countsBytes := make([]byte,0)
				resSummary := roachpb.BulkOpSummary{}
				error := ctxgroup.GroupWorkers(ctx, int(concurrentNumber), func(ctx context.Context) error {
					adder, err := cp.flowCtx.Cfg.BulkAdder(ctx, cp.flowCtx.Cfg.DB, bufferSize, flushSize, writeTS, true)
					if err != nil {
						return err
					}
					defer adder.Close(ctx)
					sortBatchSize := storageicl.LoadSortBatchSize(cp.flowCtx.Cfg.Settings)
					// Drain the kvCh using the BulkAdder until it closes.
					if err := ingestKvs(ctx, adder, kvCh, metrics, sortBatchSize); err != nil {
						return err
					}
					//并发处理时，回显数据的整合
					added := adder.GetSummary()
					atomic.AddInt64(&resSummary.Rows, added.Rows)
					atomic.AddInt64(&resSummary.DataSize, added.DataSize)
					atomic.AddInt64(&resSummary.IndexEntries, added.IndexEntries)
					atomic.AddInt64(&resSummary.SystemRecords, added.SystemRecords)
					return nil
				})
				if error != nil {
					return error
				}
				countsBytes, err := protoutil.Marshal(&resSummary)
				if err != nil {
					return err
				}
				cs, err := cp.out.EmitRow(ctx, sqlbase.EncDatumRow{
					sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(countsBytes))),
					sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes([]byte{}))),
				})
				if err != nil {
					return err
				}
				if cs != runbase.NeedMoreRows {
					return errors.New("unexpected closure of consumer")
				}
				return nil
			})
		} else {
			// Sample KVs
			group.GoCtx(func(ctx context.Context) error {

				ctx, span := tracing.ChildSpan(ctx, "sendImportKVs")
				defer tracing.FinishSpan(span)

				var fn sampleFunc
				var sampleAll bool
				if cp.spec.SampleSize == 0 {
					sampleAll = true
				} else {
					sr := sampleRate{
						rnd:        rand.New(rand.NewSource(rand.Int63())),
						sampleSize: float64(cp.spec.SampleSize),
					}
					fn = sr.sample
				}

				// Populate the split-point spans which have already been imported.
				var completedSpans roachpb.SpanGroup
				job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
				if err != nil {
					return err
				}
				progress := job.Progress()
				if details, ok := progress.Details.(*jobspb.Progress_Import); ok {
					for _, spanProgress := range details.Import.SpanProgress {
						var table *sqlbase.TableDescriptor
						//TODO only one table
						for _, tbl := range cp.spec.Tables {
							table = tbl
						}

						if spanProgress.Table.ID == table.ID {
							completedSpans.Add(spanProgress.SpanProgress...)
						}
					}
				} else {
					return errors.Errorf("unexpected progress type %T", progress)
				}

				for kvBatch := range kvCh {
					for _, kv := range kvBatch.KVs {
						// Allow KV pairs to be dropped if they belong to a completed span.
						if completedSpans.Contains(kv.Key) {
							continue
						}

						rowRequired := sampleAll || keys.IsDescriptorKey(kv.Key)
						if rowRequired || fn(kv) {
							var row sqlbase.EncDatumRow
							if rowRequired {
								row = sqlbase.EncDatumRow{
									sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Key))),
									sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Value.RawBytes))),
								}
							} else {
								// Don't send the value for rows returned for sampling
								row = sqlbase.EncDatumRow{
									sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Key))),
									sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes([]byte{}))),
								}
							}

							cs, err := cp.out.EmitRow(ctx, row)
							if err != nil {
								return err
							}
							if cs != runbase.NeedMoreRows {
								return errors.New("unexpected closure of consumer")
							}
						}
					}
				}
				return nil
			})
		}
	}

	return group.Wait()
}

type sampleFunc func(roachpb.KeyValue) bool

// sampleRate is a sampleFunc that samples a row with a probability of the
// row's size / the sample size.
type sampleRate struct {
	rnd        *rand.Rand
	sampleSize float64
}

func (s sampleRate) sample(kv roachpb.KeyValue) bool {
	sz := float64(len(kv.Key) + len(kv.Value.RawBytes))
	prob := sz / s.sampleSize
	return prob > s.rnd.Float64()
}

func makeRowErr(file string, row int64, format string, args ...interface{}) error {
	return errors.Errorf("%q: row %d: "+format, append([]interface{}{file, row}, args...)...)
}

// ingestKvs drains kvs from the channel until it closes, ingesting them using
// the BulkAdder. It handles the required buffering/sorting/etc.
func ingestKvs(
	ctx context.Context,
	adder storagebase.BulkAdder,
	kvCh <-chan KVBatch,
	metrics *Metrics,
	sortBatchSize int64,
) error {
	//const sortBatchSize = 48 << 20 // 48MB
	// TODO(dt): buffer to disk instead of all in-mem.

	// Batching all kvs together leads to worst case overlap behavior in the
	// resulting AddSSTable calls, leading to compactions and potentially L0
	// stalls. Instead maintain a separate buffer for each table's primary data.
	// This optimizes for the case when the data arriving to IMPORT is already
	// sorted by primary key, leading to no overlapping AddSSTable requests. Given
	// that many workloads (and actual imported data) will be sorted by primary
	// key, it makes sense to try to exploit this.
	//
	// TODO(dan): This was merged because it stabilized direct ingest IMPORT, but
	// we may be able to do something simpler (such as chunking along index
	// boundaries in flush) or more general (such as chunking based on the common
	// prefix of the last N kvs).
	kvsByTableIDIndexID := make(map[string]roachpb.KeyValueByKey)
	sizeByTableIDIndexID := make(map[string]int64)

	flush := func(ctx context.Context, buf roachpb.KeyValueByKey) error {
		flushBefore := timeutil.Now()
		if len(buf) == 0 {
			return nil
		}
		for i := range buf {
			if err := adder.Add(ctx, buf[i].Key, buf[i].Value.RawBytes); err != nil {
				if _, ok := err.(storagebase.DuplicateKeyError); ok {
					return pgerror.Wrap(err, pgcode.DataException, "")
				}
				return err
			}
		}
		flushAfter := timeutil.Now()
		metrics.AddSSTableTime.RecordValue(flushAfter.Sub(flushBefore).Nanoseconds())
		return nil
	}
	befor := timeutil.Now()
	after := timeutil.Now()
	for kvBatch := range kvCh {
		after = timeutil.Now()
		metrics.KvWriteSSTWaitTime.RecordValue(after.Sub(befor).Nanoseconds())
		for _, kv := range kvBatch.KVs {
			tableLen, err := encoding.PeekLength(kv.Key)
			if err != nil {
				return err
			}
			indexLen, err := encoding.PeekLength(kv.Key[tableLen:])
			if err != nil {
				return err
			}
			bufKey := kv.Key[:tableLen+indexLen]
			kvsByTableIDIndexID[string(bufKey)] = append(kvsByTableIDIndexID[string(bufKey)], kv)
			sizeByTableIDIndexID[string(bufKey)] += int64(len(kv.Key) + len(kv.Value.RawBytes))

			// TODO(dan): Prevent unbounded memory usage by flushing the largest
			// buffer when the total size of all buffers exceeds some threshold.
			if s := sizeByTableIDIndexID[string(bufKey)]; s > sortBatchSize {
				buf := kvsByTableIDIndexID[string(bufKey)]
				if err := flush(ctx, buf); err != nil {
					return err
				}
				kvsByTableIDIndexID[string(bufKey)] = buf[:0]
				sizeByTableIDIndexID[string(bufKey)] = 0
			}
		}
		befor = timeutil.Now()
	}
	for _, buf := range kvsByTableIDIndexID {
		if err := flush(ctx, buf); err != nil {
			return err
		}
	}

	if err := adder.Flush(ctx); err != nil {
		if err, ok := err.(storagebase.DuplicateKeyError); ok {
			return pgerror.Wrap(err, pgcode.DataException, "")
		}
		return err
	}
	return nil
}

// ingestKafkaKvs drains kvs from the channel until it closes, ingesting them using
// the BulkAdder. It handles the required buffering/sorting/etc.
func ingestKafkaKvs(
	ctx context.Context,
	flowCtx *runbase.FlowCtx,
	spec *distsqlpb.ReadImportDataSpec,
	metrics *Metrics,
	kvCh <-chan KVBatch,
	consumer chan ConsumerOffset,
) (*roachpb.BulkOpSummary, error) {
	ctx, span := tracing.ChildSpan(ctx, "ingestKVs")
	defer tracing.FinishSpan(span)

	writeTS := hlc.Timestamp{WallTime: spec.WalltimeNanos}
	bufferSize := storageicl.LoadBufferSize(flowCtx.Cfg.Settings)
	flushSize := storageicl.LoadFlushSize(flowCtx.Cfg.Settings)
	// We create two bulk adders so as to combat the excessive flushing of small
	// SSTs which was observed when using a single adder for both primary and
	// secondary index kvs. The number of secondary index kvs are small, and so we
	// expect the indexAdder to flush much less frequently than the pkIndexAdder.
	//
	// It is highly recommended that the cluster setting controlling the max size
	// of the pkIndexAdder buffer be set below that of the indexAdder buffer.
	// Otherwise, as a consequence of filling up faster the pkIndexAdder buffer
	// will hog memory as it tries to grow more aggressively.
	pkIndexAdder, err := flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB, bufferSize, flushSize, writeTS, !spec.SkipPrimaryKeyConflict)
	if err != nil {
		return nil, err
	}
	defer pkIndexAdder.Close(ctx)

	indexAdder, err := flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB, bufferSize, flushSize, writeTS, !spec.SkipPrimaryKeyConflict)
	if err != nil {
		return nil, err
	}
	defer indexAdder.Close(ctx)

	var topic string
	var partitions []int64
	//单topic
	//{
	//	if len(kafkaSink.topics) == 0 {
	//		return nil, errors.New("topic cannot be empty")
	//	}
	//	topic = kafkaSink.topics[0]
	//}
	// When the PK adder flushes, everything written has been flushed, so we set
	// pkFlushedRow to writtenRow. Additionally if the indexAdder is empty then we
	// can treat it as flushed as well (in case we're not adding anything to it).
	pkIndexAdder.SetOnFlush(func() {
		consumer <- ConsumerOffset{
			topic:      topic,
			partitions: partitions,
		}
		//for partition, offset := range partitions {
		//	err := kafkaSink.MarkOffset(topic, partition, offset)
		//	if err!=nil {
		//		fmt.Println(err)
		//	}
		//}
	})
	indexAdder.SetOnFlush(func() {
		consumer <- ConsumerOffset{
			topic:      topic,
			partitions: partitions,
		}
		//for partition, offset := range partitions {
		//	_ = kafkaSink.MarkOffset(topic, partition, offset)
		//}
	})

	// When the PK adder flushes, everything written has been flushed, so we set
	// pkFlushedRow to writtenRow. Additionally if the indexAdder is empty then we
	// can treat it as flushed as well (in case we're not adding anything to it).

	indexAdder.SkipLocalDuplicates(spec.SkipPrimaryKeyConflict)
	pkIndexAdder.SkipLocalDuplicates(spec.SkipPrimaryKeyConflict)
	//g := ctxgroup.WithContext(ctx)
	//g.GoCtx(func(ctx context.Context) error {
	//
	//	return nil
	//})
	//
	//if err := g.Wait(); err != nil {
	//	return nil, err
	//}

	loadSummary := roachpb.BulkOpSummary{}
	//defer close(stopProgress)

	// We insert splits at every index span of the table above. Since the
	// BulkAdder is split aware when constructing SSTs, there is no risk of worst
	// case overlap behavior in the resulting AddSSTable calls.
	//
	// NB: We are getting rid of the pre-buffering stage which constructed
	// separate buckets for each table's primary data, and flushed to the
	// BulkAdder when the bucket was full. This is because, a tpcc 1k IMPORT would
	// OOM when maintaining this buffer. Two big wins we got from this
	// pre-buffering stage were:
	//
	// 1. We avoided worst case overlapping behavior in the AddSSTable calls as a
	// result of flushing keys with the same TableIDIndexID prefix, together.
	//
	// 2. Secondary index KVs which were few and filled the bucket infrequently
	// were flushed rarely, resulting in fewer L0 (and total) files.
	//
	// While we continue to achieve the first property as a result of the splits
	// mentioned above, the KVs sent to the BulkAdder are no longer grouped which
	// results in flushing a much larger number of small SSTs. This increases the
	// number of L0 (and total) files, but with a lower memory usage.
	//for kvBatch := range kvCh {
	for {
		select {
		case kvBatch, ok := <-kvCh:
			if !ok {
				if err := pkIndexAdder.Flush(ctx); err != nil {
					if _, ok := err.(storagebase.DuplicateKeyError); ok {
						return nil, errors.Wrap(err, "duplicate key in primary index")
					}
					return nil, err
				}

				if err := indexAdder.Flush(ctx); err != nil {
					if _, ok := err.(storagebase.DuplicateKeyError); ok {
						return nil, errors.Wrap(err, "duplicate key in index")
					}
					return nil, err
				}

				addedSummary := pkIndexAdder.GetSummary()
				addedSummary.Add(indexAdder.GetSummary())
				return &addedSummary, nil
			}
			partitions = kvBatch.sources
			for _, kv := range kvBatch.KVs {
				_, _, indexID, indexErr := keys.DecodeIndexPrefix(kv.Key)
				if indexErr != nil {
					return nil, indexErr
				}

				// Decide which adder to send the KV to by extracting its index id.
				//
				// TODO(adityamaru): There is a potential optimization of plumbing the
				// different putters, and differentiating based on their type. It might be
				// more efficient than parsing every kv.
				if indexID == 1 {
					if err := pkIndexAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
						if _, ok := err.(storagebase.DuplicateKeyError); ok {
							return nil, errors.Wrap(err, "duplicate key in primary index")
						}
						return nil, err
					}
					//推送数据
					duration := storageicl.LoadKafkaFlushDuration(flowCtx.Cfg.Settings)
					now := flowCtx.Cfg.DB.Clock().Now().GoTime()
					if now.Sub(pkIndexAdder.GetFlushTs().GoTime()) > duration {
						err = pkIndexAdder.Flush(ctx)
						if err != nil {
							return nil, err
						}
					}
				} else {
					if err := indexAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
						if _, ok := err.(storagebase.DuplicateKeyError); ok {
							return nil, errors.Wrap(err, "duplicate key in index")
						}
						return nil, err
					}
					//推送数据
					duration := storageicl.LoadKafkaFlushDuration(flowCtx.Cfg.Settings)
					now := flowCtx.Cfg.DB.Clock().Now().GoTime()
					if now.Sub(pkIndexAdder.GetFlushTs().GoTime()) > duration {
						err = pkIndexAdder.Flush(ctx)
						if err != nil {
							return nil, err
						}
					}
				}
			}
			currentSummary := pkIndexAdder.GetSummary()
			currentSummary.Add(indexAdder.GetSummary())
			metrics.LoadSpeed.RecordValue(currentSummary.DataSize - loadSummary.DataSize)
			loadSummary = currentSummary
		case <-time.After(storageicl.LoadKafkaFlushDuration(flowCtx.Cfg.Settings)):
			if err := pkIndexAdder.Flush(ctx); err != nil {
				if _, ok := err.(storagebase.DuplicateKeyError); ok {
					return nil, errors.Wrap(err, "duplicate key in primary index")
				}
				return nil, err
			}
			if err := indexAdder.Flush(ctx); err != nil {
				if _, ok := err.(storagebase.DuplicateKeyError); ok {
					return nil, errors.Wrap(err, "duplicate key in index")
				}
				return nil, err
			}
			currentSummary := pkIndexAdder.GetSummary()
			currentSummary.Add(indexAdder.GetSummary())
			metrics.LoadSpeed.RecordValue(currentSummary.DataSize - loadSummary.DataSize)
			loadSummary = currentSummary
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	//if err := pkIndexAdder.Flush(ctx); err != nil {
	//	if _, ok := err.(storagebase.DuplicateKeyError); ok {
	//		return nil, errors.Wrap(err, "duplicate key in primary index")
	//	}
	//	return nil, err
	//}
	//
	//if err := indexAdder.Flush(ctx); err != nil {
	//	if _, ok := err.(storagebase.DuplicateKeyError); ok {
	//		return nil, errors.Wrap(err, "duplicate key in index")
	//	}
	//	return nil, err
	//}
	//
	//addedSummary := pkIndexAdder.GetSummary()
	//addedSummary.Add(indexAdder.GetSummary())
	//return &addedSummary, nil
}

//BatchLoadFromAdmin Load data from admin.
func BatchLoadFromAdmin(
	ctx context.Context,
	req *serverpb.RawBatchLoadRequest,
	st *cluster.Settings,
	db *client.DB,
	rangeCache *kv.RangeDescriptorCache,
	nodeID roachpb.NodeID,
	tableDesc *sqlbase.TableDescriptor,
	histogramWindowInterval time.Duration,
) (*serverpb.RawBatchLoadResponse, error) {
	resp := serverpb.RawBatchLoadResponse{}

	groupCtx := ctxgroup.WithContext(ctx)

	kvChCap := storageicl.LoadKvChCap(st)
	kvCh := make(chan KVBatch, kvChCap)

	writeTS := hlc.NewClock(hlc.UnixNano, 0).Now()

	expectedCols := len(tableDesc.VisibleColumns())
	if tableDesc.IsHashPartition {
		expectedCols--
	}

	c := &csvInputReader{
		kvCh: kvCh,
		flowCtx: &runbase.FlowCtx{
			EvalCtx: &tree.EvalContext{
				Context:     ctx,
				Sequence:    nil,
				SessionData: &sessiondata.SessionData{},
			},
			Cfg: &runbase.ServerConfig{
				DiskMonitor: &mon.BytesMonitor{},
				Settings:    st,
				BulkAdder: func(ctx context.Context, db *client.DB, bufferSize, flushSize int64,
					ts hlc.Timestamp, disallowShadowing bool) (storagebase.BulkAdder, error) {
					return bulk.MakeBulkAdder(db, rangeCache, bufferSize, flushSize,
						ts, disallowShadowing)
				},
			},
			NodeID: nodeID,
		},
		recordCh:     make(chan csvRecord, storageicl.LoadRecordChCap(st)),
		tableDesc:    tableDesc,
		metrics:      MakeLoadMetrics(histogramWindowInterval).(*Metrics),
		expectedCols: expectedCols,
		batchSize:    int(storageicl.LoadRecordBatchSize(st)),
		opts:         roachpb.CSVOptions{NullEncoding: &req.NullEncoding},
		znsparkInput: true,
	}

	//整体的数据通路为：3->1->2,因此在使用channel 传输消息的时候，要先保证消费端已经开启，否则如果是顺序执行的话，可能就会出现死锁
	//基本解决方案就是：1.把每个流程都放到协程中执行 或者 先将消费端启动

	//1. 数据转换逻辑，从源数据转换为znbase的KV
	// start up workers.
	groupCtx.Go(func() error {
		defer close(c.kvCh)
		return ctxgroup.GroupWorkers(ctx, runtime.NumCPU(), func(ctx context.Context) error {
			return c.convertRecordWorker(ctx)
		})
	})
	//2.KV数据消费入库逻辑
	bufferSize := storageicl.LoadBufferSize(st)
	flushSize := storageicl.LoadFlushSize(st)
	sortBatchSize := storageicl.LoadSortBatchSize(st)
	concurrentNumber := storageicl.LoadIngestConcurrency(st)

	groupCtx.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "ingestKVs")
		defer tracing.FinishSpan(span)
		//转换限制转换并发处理数,用户自定义并发数，可减缓cpu负载从而减少rpc error
		//countsBytes := make([]byte,0)
		resSummary := roachpb.BulkOpSummary{}
		error := ctxgroup.GroupWorkers(ctx, int(concurrentNumber), func(ctx context.Context) error {
			adder, err := c.flowCtx.Cfg.BulkAdder(ctx, db, bufferSize, flushSize, writeTS, !req.Replace)
			if req.Replace {
				adder.SkipLocalDuplicates(true)
			}

			if err != nil {
				return err
			}
			defer adder.Close(ctx)
			// Drain the kvCh using the BulkAdder until it closes.
			if err := ingestKvs(ctx, adder, kvCh, c.metrics, sortBatchSize); err != nil {
				return err
			}
			//并发处理时，回显数据的整合
			added := adder.GetSummary()
			atomic.AddInt64(&resSummary.Rows, added.Rows)
			atomic.AddInt64(&resSummary.DataSize, added.DataSize)
			atomic.AddInt64(&resSummary.IndexEntries, added.IndexEntries)
			atomic.AddInt64(&resSummary.SystemRecords, added.SystemRecords)
			return nil
		})
		if error != nil {
			return error
		}
		_, err := protoutil.Marshal(&resSummary)
		if err != nil {
			return err
		}

		return nil
	})
	//3.从源数据源读取数据，并对数据进行处理的处理，比如初步校验数据等
	//数据处理逻辑也建议单独起协程进行处理
	const batchSize = 10000

	batch := csvRecord{
		file:      "some/path/to/some/file/of/csv/data.tbl",
		rowOffset: 1,
		r:         make([][]string, 0, batchSize),
		fileIndex: rand.Int31(),
	}

	for i := 0; i < len(req.Values); i++ {
		if len(batch.r) > batchSize {
			c.recordCh <- batch
			batch.r = make([][]string, 0, batchSize)
			batch.rowOffset = i
		}

		value := req.Values[i%len(req.Values)]
		record := strings.Split(value, req.Sep)

		if len(record) == c.expectedCols {
			// Expected number of columns.
		} else if len(record) == c.expectedCols+1 && record[c.expectedCols] == "" {
			// Line has the optional trailing comma, ignore the empty field.
			record = record[:c.expectedCols]
		} else {
			return &resp, errors.Errorf("row %d: expected %d fields, got %d", i, c.expectedCols, len(record))
		}

		batch.r = append(batch.r, record)
	}
	c.recordCh <- batch
	close(c.recordCh)
	if err := groupCtx.Wait(); err != nil {
		return &resp, err
	}

	resp.PutNums = int64(len(req.Values))
	resp.ErrorMessage = ""

	return &resp, nil
}

func init() {
	rowexec.NewReadImportDataProcessor = newReadImportDataProcessor

	loadutils.RawBatchLoadCallbackFunc = BatchLoadFromAdmin

	rand.Seed(timeutil.Now().UnixNano())
}
