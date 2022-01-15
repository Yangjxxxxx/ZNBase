// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rowexec

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// ParallelScanResultThreshold is the number of results up to which, if the
// maximum number of results returned by a scan is known, the table reader
// disables batch limits in the dist sender. This results in the parallelization
// of these scans.
const ParallelScanResultThreshold = 10000

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	*TablereaderBlocked
	runbase.ProcessorBase
	// InternalOpt opt switch
	InternalOpt bool
	spans       roachpb.Spans
	limitHint   int64

	// maxResults is non-zero if there is a limit on the total number of rows
	// that the tableReader will read.
	maxResults             uint64
	switchCount            bool
	ignoreMisplannedRanges bool
	done                   bool
	countRet               int

	// input is really the fetcher below, possibly wrapped in a stats generator.
	input runbase.RowSource
	// fetcher is the underlying Fetcher, should only be used for
	// initialization, call input.Next() to retrieve rows once initialized.
	fetcher   rowFetcher
	alloc     sqlbase.DatumAlloc
	startTime time.Time
}

var _ runbase.Processor = &tableReader{}
var _ runbase.RowSource = &tableReader{}

const tableReaderProcName = "table reader"

var trPool = sync.Pool{
	New: func() interface{} {
		return &tableReader{}
	},
}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.TableReaderSpec,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (*tableReader, error) {
	if flowCtx.NodeID == 0 {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}

	tr := trPool.Get().(*tableReader)
	tr.limitHint = runbase.LimitHint(spec.LimitHint, post)
	tr.maxResults = spec.MaxResults

	returnMutations := spec.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	types := spec.Table.ColumnTypesWithMutations(returnMutations)
	tr.ignoreMisplannedRanges = flowCtx.Local
	if err := tr.Init(
		tr,
		post,
		types,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		runbase.ProcStateOpts{
			// We don't pass tr.input as an inputToDrain; tr.input is just an adapter
			// on top of a Fetcher; draining doesn't apply to it. Moreover, Andrei
			// doesn't trust that the adapter will do the right thing on a Next() call
			// after it had previously returned an error.
			InputsToDrain:        nil,
			TrailingMetaCallback: tr.generateTrailingMeta,
		},
	); err != nil {
		return nil, err
	}

	neededColumns := tr.Out.NeededColumns()
	var fetcher row.Fetcher
	tr.input = &rowFetcherWrapper{Fetcher: &fetcher}
	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)
	if _, _, err := initRowFetcher(
		&fetcher, &spec.Table, int(spec.IndexIdx), columnIdxMap, spec.Reverse,
		neededColumns, spec.IsCheck, &tr.alloc, spec.Visibility,
		spec.LockingStrength, spec.LockingWaitPolicy,
	); err != nil {
		return nil, err
	}
	tr.fetcher = &fetcher

	if spec.Table.ReplicationTable &&
		!flowCtx.EvalCtx.SessionData.ReplicaionSync &&
		spec.Table.Checks == nil {
		fetcher.ReplicationTable = true
	}

	nSpans := len(spec.Spans)
	if cap(tr.spans) >= nSpans {
		tr.spans = tr.spans[:nSpans]
	} else {
		tr.spans = make(roachpb.Spans, nSpans)
	}
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}

	if flowCtx.EvalCtx.SessionData.PC[sessiondata.TableReaderSet].Parallel && spec.Table.ID > keys.PostgresSchemaID && flowCtx.CanParallel && tr.limitHint == 0 {
		flowCtx.EmergencyClose = true
		tr.InternalOpt = true
		if tr.TablereaderBlocked == nil {
			tr.TablereaderBlocked = new(TablereaderBlocked)
		}
		if flowCtx.EvalCtx.ShouldOrdered {
			tr.parallel = 1
		} else {
			tr.parallel = int(flowCtx.EvalCtx.SessionData.PC[sessiondata.TableReaderSet].ParallelNum)
			if tr.parallel <= 0 {
				tr.parallel = 1
			}
			if tr.parallel > runtime.NumCPU()*2 {
				tr.parallel = runtime.NumCPU() * 2
			}
		}

		fetchers := make([]row.Fetcher, tr.parallel)
		for idx := range fetchers {
			if _, _, err := initRowFetcher(
				&fetchers[idx], &spec.Table, int(spec.IndexIdx), columnIdxMap, spec.Reverse,
				neededColumns, spec.IsCheck, &tr.alloc, spec.Visibility,
				spec.LockingStrength, spec.LockingWaitPolicy,
			); err != nil {
				return nil, err
			}
		}
		if err := tr.InitBlockedTr(tr, fetchers, tr, post, types, flowCtx, processorID, output, nil, runbase.ProcStateOpts{
			InputsToDrain:        nil,
			TrailingMetaCallback: tr.generateTrailingMeta,
		}); err != nil {
			return nil, err
		}
		tr.RowFetchers[0].MakeCount()
	}
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		tr.input = NewInputStatCollector(tr.input)
		tr.FinishTrace = tr.OutputStatsToTrace
		tr.fetcher = newRowFetcherStatCollector(&fetcher)
	}
	return tr, nil
}

// rowFetcherWrapper is used only by a tableReader to wrap calls to
// Fetcher.NextRow() in a RowSource implementation.
type rowFetcherWrapper struct {
	ctx context.Context
	*row.Fetcher
	Fetchers
}

//Fetchers is for parallel TableReader
type Fetchers []row.Fetcher

var _ runbase.RowSource = &rowFetcherWrapper{}

// Start is part of the RowSource interface.
func (w *rowFetcherWrapper) Start(ctx context.Context) context.Context {
	w.ctx = ctx
	return ctx
}

//GetBatch is part of the RowSource interface.
func (w *rowFetcherWrapper) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	w.Fetcher.SetBatch(respons)
	for i := range w.Fetchers {
		w.Fetchers[i].SetBatch(respons)
	}
	return ctx
}

//SetChan is part of the RowSource interface.
func (w *rowFetcherWrapper) SetChan() {}

// Next() calls NextRow() on the underlying Fetcher. If the returned
// ProducerMetadata is not nil, only its Err field will be set.
func (w *rowFetcherWrapper) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if runbase.OptJoin {
		rowTr, _, _, err := w.NextRow1(w.ctx)
		if err != nil {
			return rowTr, &distsqlpb.ProducerMetadata{Err: err}
		}
		return rowTr, nil
	}

	rowTr, _, _, err := w.NextRow(w.ctx)
	if err != nil {
		return rowTr, &distsqlpb.ProducerMetadata{Err: err}
	}
	return rowTr, nil
}

func (w rowFetcherWrapper) OutputTypes() []sqlbase.ColumnType { return nil }
func (w rowFetcherWrapper) ConsumerDone()                     {}
func (w rowFetcherWrapper) ConsumerClosed()                   {}

func (tr *tableReader) generateTrailingMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	var trailingMeta []distsqlpb.ProducerMetadata
	if !tr.ignoreMisplannedRanges {
		ranges := runbase.MisplannedRanges(tr.Ctx, tr.fetcher.GetRangesInfo(), tr.FlowCtx.NodeID)
		if ranges != nil {
			trailingMeta = append(trailingMeta, distsqlpb.ProducerMetadata{Ranges: ranges})
		}
	}
	if meta := runbase.GetTxnCoordMeta(ctx, tr.FlowCtx.Txn); meta != nil {
		trailingMeta = append(trailingMeta, distsqlpb.ProducerMetadata{TxnCoordMeta: meta})
	}
	tr.InternalClose()
	return trailingMeta
}

//for tablereader opt
func (tr *tableReader) StartBatch(ctx context.Context) context.Context {
	if tr.FlowCtx.Txn == nil {
		log.Fatalf(ctx, "tableReader outside of txn")
	}

	// Like every processor, the tableReader will have a context with a log tag
	// and a span. The underlying fetcher inherits the proc's span, but not the
	// log tag.
	fetcherCtx := ctx
	ctx = tr.StartInternal(ctx, tableReaderProcName)
	if procSpan := opentracing.SpanFromContext(ctx); procSpan != nil {
		fetcherCtx = opentracing.ContextWithSpan(fetcherCtx, procSpan)
	}

	// This call doesn't do much; the real "starting" is below.
	tr.input.Start(fetcherCtx)
	limitBatches := true
	// We turn off limited batches if we know we have no limit and if the
	// tableReader spans will return less than the ParallelScanResultThreshold.
	// This enables distsender parallelism - if limitBatches is true, distsender
	// does *not* parallelize multi-range scan requests.
	if tr.maxResults != 0 &&
		tr.maxResults < ParallelScanResultThreshold &&
		tr.limitHint == 0 &&
		sqlbase.ParallelScans.Get(&tr.FlowCtx.Cfg.Settings.SV) {
		limitBatches = false
	}
	log.VEventf(ctx, 1, "starting  UnSync scan with limitBatches %t", limitBatches)
	tr.ErrorChan = make(chan error, 1)
	if err := tr.RowFetchers[0].StartGetBatch(
		fetcherCtx, tr.FlowCtx.Txn, tr.spans,
		limitBatches, tr.limitHint, tr.FlowCtx.TraceKV, tr.BlockRepons, tr.ErrorChan, tr.switchCount,
	); err != nil {
		tr.MoveToDraining(err)
	}
	return ctx
}

// Start is part of the RowSource interface.
func (tr *tableReader) Start(ctx context.Context) context.Context {
	if tr.fetcher.GetIfFuncIndex() {
		tr.Out.Outputforfuncindex()
	}
	if tr.InternalOpt {
		var fetcherCtx context.Context
		if procSpan := opentracing.SpanFromContext(ctx); procSpan != nil {
			fetcherCtx = opentracing.ContextWithSpan(ctx, procSpan)
		}
		if _, ok := tr.input.(*InputStatCollector); ok && tr.switchCount {
			tr.startTime = timeutil.Now()
		}
		tr.StartBatch(fetcherCtx)
		if !tr.switchCount {
			tr.StartWorkers(ctx)
		}
		return ctx
	}
	if tr.FlowCtx.Txn == nil {
		log.Fatalf(ctx, "tableReader outside of txn")
	}

	// Like every processor, the tableReader will have a context with a log tag
	// and a span. The underlying fetcher inherits the proc's span, but not the
	// log tag.
	fetcherCtx := ctx
	ctx = tr.StartInternal(ctx, tableReaderProcName)
	if procSpan := opentracing.SpanFromContext(ctx); procSpan != nil {
		fetcherCtx = opentracing.ContextWithSpan(fetcherCtx, procSpan)
	}

	// This call doesn't do much; the real "starting" is below. tr.input.Start(fetcherCtx)
	tr.input.Start(fetcherCtx)
	limitBatches := true
	// We turn off limited batches if we know we have no limit and if the
	// tableReader spans will return less than the ParallelScanResultThreshold.
	// This enables distsender parallelism - if limitBatches is true, distsender
	// does *not* parallelize multi-range scan requests.
	if tr.maxResults != 0 &&
		tr.maxResults < ParallelScanResultThreshold &&
		tr.limitHint == 0 &&
		sqlbase.ParallelScans.Get(&tr.FlowCtx.Cfg.Settings.SV) {
		limitBatches = false
	}
	log.VEventf(ctx, 1, "starting scan with limitBatches %t", limitBatches)
	if err := tr.fetcher.StartScan(
		fetcherCtx, tr.FlowCtx.Txn, tr.spans,
		limitBatches, tr.limitHint, tr.FlowCtx.TraceKV,
	); err != nil {
		tr.MoveToDraining(err)
	}
	return ctx
}

//GetBatch is part of the RowSource interface.
func (tr *tableReader) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (tr *tableReader) SetChan() {}

// Release releases this tableReader back to the pool.
func (tr *tableReader) Release() {
	tr.ProcessorBase.Reset()
	tr.fetcher.Reset()
	*tr = tableReader{
		ProcessorBase: tr.ProcessorBase,
		fetcher:       tr.fetcher,
		spans:         tr.spans[:0],
	}
	trPool.Put(tr)
}

// Next is part of the RowSource interface.
func (tr *tableReader) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if tr.InternalOpt {
		for tr.State == runbase.StateRunning {
			rowTr, meta := tr.NextRow()

			if meta != nil {
				if meta.Err != nil {
					tr.MoveToDraining(nil /* err */)
				}
				return nil, meta
			}
			if rowTr == nil {
				//if t, ok := tr.input.(*InputStatCollector); ok {
				//	t.StallTime = timeutil.Since(tr.startTime)
				//}
				tr.MoveToDraining(nil)
				break
			}
			//if t, ok := tr.input.(*InputStatCollector); ok {
			//	atomic.AddInt64(&t.NumRows, 1)
			//}
			return rowTr, meta
		}
	} else {
		for tr.State == runbase.StateRunning {
			rowTr, meta := tr.input.Next()
			if meta != nil {
				if meta.Err != nil {
					tr.MoveToDraining(nil /* err */)
				}
				return nil, meta
			}
			if rowTr == nil {
				tr.MoveToDraining(nil /* err */)
				break
			}

			if outRow := tr.ProcessRowHelper(rowTr); outRow != nil {
				return outRow, nil
			}
		}
	}
	return nil, tr.DrainHelper()
}

// just count  the row and return
func (tr *tableReader) Count() (int, *distsqlpb.ProducerMetadata) {
	if !tr.done {
		select {
		case err := <-tr.ErrorChan:
			if err != nil {
				return 0, &distsqlpb.ProducerMetadata{Err: err}
			}
		default:
		}
		tr.countRet += tr.RowFetchers[0].CountRet()
		if r, ok := tr.input.(*InputStatCollector); ok {
			r.StallTime = timeutil.Since(tr.startTime)
			r.NumRows = int64(tr.countRet)
		}
		tr.MoveToDraining(nil /* err */)
		//	tr.ProcessorBase.State = runbase.StateDraining
		tr.done = true
		return tr.countRet, tr.DrainHelper()
	}
	return tr.countRet, tr.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (tr *tableReader) ConsumerClosed() {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	close(tr.RowResCH)
	close(tr.RowCollectCH)
	// The consumer is done, Next() will not be called again.
	tr.InternalClose()
}

var _ distsqlpb.DistSQLSpanStats = &TableReaderStats{}

const tableReaderTagPrefix = "tablereader."

// Stats implements the SpanStats interface.
func (trs *TableReaderStats) Stats() map[string]string {
	return trs.InputStats.Stats(tableReaderTagPrefix)
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (trs *TableReaderStats) StatsForQueryPlan() []string {
	s := trs.InputStats.StatsForQueryPlan("" /* prefix */)
	return append(s,
		fmt.Sprintf("%s: %t", "isParallel", trs.IsParallel),
		fmt.Sprintf("%s: %d", "ParallelNum", trs.ParallelNum),
	)
}

// outputStatsToTrace outputs the collected tableReader stats to the trace. Will
// fail silently if the tableReader is not collecting stats.
func (tr *tableReader) OutputStatsToTrace() {
	is, ok := getInputStats(tr.FlowCtx, tr.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(tr.Ctx); sp != nil {
		if tr.InternalOpt {
			tracing.SetSpanStats(sp, &TableReaderStats{
				ParallelNum: int32(tr.parallel),
				IsParallel:  tr.InternalOpt,
				InputStats:  is,
			})
		} else {
			tracing.SetSpanStats(sp, &TableReaderStats{
				InputStats: is,
			})
		}
	}
}
