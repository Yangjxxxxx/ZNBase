package rowexec

import (
	"context"
	"hash/crc32"
	"runtime"
	"sync"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/mon"
)

// AggregatorOpt switch for tablereader opt
var AggregatorOpt *settings.BoolSetting

// AggregatorParallelNums tablereader parallel num
var AggregatorParallelNums *settings.IntSetting

func init() {
	AggregatorOpt = settings.RegisterBoolSetting(
		"sql.opt.operator.aggregator",
		"for tablereader parallel and unsync",
		false,
	)
	AggregatorParallelNums = settings.RegisterIntSetting(
		"sql.opt.operator.aggregator.parallelnum",
		"for tablereader parallel nums,max 8",
		4,
	)
}

//AgStruct is for agg chan
type AgStruct struct {
	row   []sqlbase.EncDatumRow
	meta  *distsqlpb.ProducerMetadata
	index int
}

// hashAggregator is a specialization of aggregatorBase that must keep track of
// multiple grouping buckets at a time.
type parallehashAggregator struct {
	aggregatorBase
	//aggregatorBase
	wokers            []hashAggregator
	RowSourceChan     []chan sqlbase.EncDatumRows
	RowCollectChan    []chan sqlbase.EncDatumRows
	currentDestMap    map[int]sqlbase.EncDatumRows
	currentDestMapIdx map[int]int
	ErrorIdx          int
	currAggWorkerIdx  int
	wg                sync.WaitGroup
}

func (ag *parallehashAggregator) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	panic("implement me")
}

func (ag *aggregatorBase) initMon(acc *mon.BoundAccount) {
	ag.acc = acc
}

//newParalleAggregator is made to control all parallel hash agg
func newParalleAggregator(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.AggregatorSpec,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (runbase.Processor, error) {
	if len(spec.GroupCols) == 0 &&
		len(spec.Aggregations) == 1 &&
		spec.Aggregations[0].FilterColIdx == nil &&
		spec.Aggregations[0].Func == distsqlpb.AggregatorSpec_COUNT_ROWS &&
		!spec.Aggregations[0].Distinct {
		if tr, ok := input.(*tableReader); ok && tr.InternalOpt {
			if tr.Out.GetFilter() == nil {
				tr.switchCount = true
				if s, ok := tr.fetcher.(*row.Fetcher); ok {
					s.SwitchCount = true
				}
				if s, ok := tr.fetcher.(*rowFetcherStatCollector); ok {
					s.SwitchCount = true
				}
			}
		}
		return newCountAggregator(flowCtx, processorID, input, post, output)
	}
	ag := &parallehashAggregator{}
	// 初始化并行度
	ag.parallelNum = int(flowCtx.EvalCtx.SessionData.PC[sessiondata.HashAggSet].ParallelNum)
	if ag.parallelNum <= 0 {
		ag.parallelNum = 1
	}
	if ag.parallelNum > runtime.NumCPU()*2 {
		ag.parallelNum = runtime.NumCPU() * 2
	}
	ag.InternalOpt = true
	// 初始化工作协程数
	ag.wokers = make([]hashAggregator, ag.parallelNum)
	err := ag.init(ag, flowCtx, processorID, spec, input, post, output, func(ctx context.Context) []distsqlpb.ProducerMetadata {
		ag.close()
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i := range ag.wokers {
		account := flowCtx.EvalCtx.Mon.MakeBoundAccount()
		ag.wokers[i].initMon(&account)
		err := ag.wokers[i].init(&ag.wokers[i], flowCtx, processorID, spec, input, post, output, func(context.Context) []distsqlpb.ProducerMetadata {
			ag.close()
			return nil
		})
		if err != nil {
			return nil, err
		}
		ag.wokers[i].buckets = make(map[string]aggregateFuncs)
	}
	ag.currAggWorkerIdx = -1
	ag.ErrorIdx = -1
	return ag, nil
}

// prepareReadInput 准备预读输入端的数据
func (ag *parallehashAggregator) prepareReadInput() {
	ag.RowSourceChan = make([]chan sqlbase.EncDatumRows, ag.parallelNum)
	ag.RowCollectChan = make([]chan sqlbase.EncDatumRows, ag.parallelNum)
	ag.currentDestMap = make(map[int]sqlbase.EncDatumRows, ag.parallelNum)
	ag.currentDestMapIdx = make(map[int]int, ag.parallelNum)
	for i := 0; i < ag.parallelNum; i++ {
		ag.RowSourceChan[i] = make(chan sqlbase.EncDatumRows, ag.parallelNum*8)
		ag.RowCollectChan[i] = make(chan sqlbase.EncDatumRows, ag.parallelNum*8)
		ag.currentDestMapIdx[i] = -1
	}
	go func() {
		defer func() {
			for i := range ag.RowSourceChan {
				close(ag.RowSourceChan[i])
			}
		}()
		for {
			rowAg, meta := ag.input.Next()
			if meta != nil {
				if meta.Err != nil {
					ag.MoveToDraining(meta.Err)
					return
				}
				continue
			}
			if rowAg == nil {
				for i := range ag.currentDestMapIdx {
					if ag.currentDestMapIdx[i] != -1 {
						ag.RowSourceChan[i] <- ag.currentDestMap[i][:ag.currentDestMapIdx[i]+1]
					}
				}
				ag.inputDone = true
				ag.MoveToDraining(nil)
				ag.State = runbase.StateRunning
				return
			}
			dest, err := ag.computeAggDestination(rowAg)
			if err != nil {
				return
			}
			if ag.currentDestMapIdx[dest] == -1 {
				select {
				case ag.currentDestMap[dest] = <-ag.RowCollectChan[dest]:
				default:
					ag.currentDestMap[dest] = make(sqlbase.EncDatumRows, RowBufferSize)
				}
			}
			ag.currentDestMapIdx[dest]++
			if len(ag.currentDestMap[dest][ag.currentDestMapIdx[dest]]) == 0 {
				ag.currentDestMap[dest][ag.currentDestMapIdx[dest]] = make(sqlbase.EncDatumRow, len(ag.input.OutputTypes()))
			}
			copy(ag.currentDestMap[dest][ag.currentDestMapIdx[dest]], rowAg)
			if ag.currentDestMapIdx[dest] == RowBufferSize-1 {
				ag.RowSourceChan[dest] <- ag.currentDestMap[dest]
				ag.currentDestMapIdx[dest] = -1
			}
		}
	}()
}

func (ag *parallehashAggregator) runAggWorker(ctx context.Context, workerID int) {
	var currRows sqlbase.EncDatumRows
	var moreRows bool
	var agWorker = &ag.wokers[workerID]
	defer func() {
		if len(agWorker.TrailingMeta) > 0 {
			ag.TrailingMeta = agWorker.TrailingMeta
			ag.ErrorIdx = workerID
		}
		ag.wg.Done()
	}()
	for {
		currRows, moreRows = <-ag.RowSourceChan[workerID]
		if !moreRows {
			agWorker.bucketsIter = make([]string, 0, len(agWorker.buckets))
			for bucket := range agWorker.buckets {
				agWorker.bucketsIter = append(agWorker.bucketsIter, bucket)
			}
			return
		}
		for i := range currRows {
			enc, err := agWorker.encode(agWorker.scratch, currRows[i])
			if err != nil {
				agWorker.TrailingMeta = append(agWorker.TrailingMeta, distsqlpb.ProducerMetadata{Err: err})
				return
			}
			_, ok := agWorker.buckets[string(enc)]
			agWorker.scratch = agWorker.scratch[:0]
			if !ok {
				s, err := agWorker.arena.AllocBytes(ag.Ctx, enc)
				funcs, err := agWorker.createAggregateFuncs()
				if err != nil {
					agWorker.TrailingMeta = append(agWorker.TrailingMeta, distsqlpb.ProducerMetadata{Err: err})
					return
				}
				agWorker.buckets[s] = funcs
			}
			err = agWorker.accumulateRowIntoBucket(currRows[i], enc, agWorker.buckets[string(enc)])
			if err != nil {
				agWorker.TrailingMeta = append(agWorker.TrailingMeta, distsqlpb.ProducerMetadata{Err: err})
				return
			}
		}
		select {
		case ag.RowCollectChan[workerID] <- currRows:
		default:
		}
	}
}

// computeAggDestination 计算重分布之后的目标
func (ag *parallehashAggregator) computeAggDestination(row sqlbase.EncDatumRow) (int, error) {
	_, err := ag.encode(ag.scratch, row)
	if err != nil {
		ag.MoveToDraining(err)
		return -1, err
	}
	idx := int(crc32.Update(0, crc32Tb, ag.scratch) % uint32(ag.parallelNum))
	ag.scratch = ag.scratch[:0]
	return idx, nil
}

// Start is part of the RowSource interface.
func (ag *parallehashAggregator) Start(ctx context.Context) context.Context {
	ag.start(ctx, hashAggregatorProcName)
	ag.prepareReadInput()
	ag.wg.Add(ag.parallelNum)
	for i := 0; i < ag.parallelNum; i++ {
		go ag.runAggWorker(ctx, i)
	}
	ag.State = runbase.StateRunning
	return ctx
}

//SetChan is part of the RowSource interface.
func (ag *parallehashAggregator) SetChan() {

}

//GetBatch is part of the RowSource interface.
func (ag *orderedAggregator) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (ag *orderedAggregator) SetChan() {
	ag.AgStructChannel = make(chan AgStruct, 100)
	ag.agDuplicateChannel = make(chan []sqlbase.EncDatumRow, 100)
	go ag.goAggregate()
}

func (ag *orderedAggregator) goAggregate() {
	if ag.agrows == nil {
		ag.agrows = make([]sqlbase.EncDatumRow, 40)
	}
	for ag.State == runbase.StateRunning {
		switch ag.runningState {
		case aggAccumulating:
			ag.accumulateRowsOpt()
		default:
			log.Fatalf(ag.Ctx, "unsupported state: %d", ag.runningState)
		}

	}
}

func (ag *parallehashAggregator) start(ctx context.Context, procName string) context.Context {
	ag.input.Start(ctx)
	ctx = ag.StartInternal(ctx, procName)
	ag.cancelChecker = sqlbase.NewCancelChecker(ctx)
	ag.runningState = aggAccumulating
	return ctx
}

func (ag *parallehashAggregator) close() {
	if ag.InternalClose() {
		log.VEventf(ag.Ctx, 2, "exiting aggregator")
		ag.bucketsAcc.Close(ag.Ctx)
		if ag.acc != nil {
			ag.acc.Close(ag.Ctx)
		}
		// If we have started emitting rows, bucketsIter will represent which
		// buckets are still open, since buckets are closed once their results are
		// emitted.
		for i, h := range ag.wokers {
			close(ag.RowCollectChan[i])
			h.closeOpt()
		}
		ag.MemMonitor.Stop(ag.Ctx)
	}
}

func (ag *orderedAggregator) accumulateRowsOpt() {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(nil)
				ag.runningState = aggStateUnknown
				ag.AgStructChannel <- AgStruct{
					ag.agrows,
					meta,
					ag.agi,
				}

				close(ag.AgStructChannel)
				return
			}
			ag.runningState = aggAccumulating
			return
		}
		if row == nil {
			log.VEvent(ag.Ctx, 1, "accumulation complete")
			ag.inputDone = true
			ag.MoveToDraining(nil)
			ag.runningState = aggStateUnknown
			ag.AgStructChannel <- AgStruct{
				ag.agrows,
				meta,
				ag.agi,
			}
			close(ag.AgStructChannel)
			return
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.MoveToDraining(err)
				ag.runningState = aggStateUnknown
				ag.AgStructChannel <- AgStruct{
					ag.agrows,
					meta,
					ag.agi,
				}
				close(ag.AgStructChannel)
				return
			}
			if !matched {
				ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.MoveToDraining(err)
			ag.runningState = aggStateUnknown
			ag.AgStructChannel <- AgStruct{
				ag.agrows,
				meta,
				ag.agi,
			}
			close(ag.AgStructChannel)
			return
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if ag.bucket == nil && ag.isScalar {
		var err error
		ag.bucket, err = ag.createAggregateFuncs()
		if err != nil {
			ag.MoveToDraining(err)
			ag.runningState = aggStateUnknown
			ag.AgStructChannel <- AgStruct{
				ag.agrows,
				nil,
				ag.agi,
			}
			close(ag.AgStructChannel)
			return
		}
	}
	// Transition to aggEmittingRows, and let it generate the next row/meta.
	for {
		if ag.bucket == nil {
			// We've exhausted all of the aggregation buckets.

			// We've only consumed part of the input where the rows are equal over
			// the columns specified by ag.orderedGroupCols, so we need to continue
			// accumulating the remaining rows.
			if ag.inputDone {
				// The input has been fully consumed. Transition to draining so that we
				// emit any metadata that we've produced.
				ag.MoveToDraining(nil /* err */)
				ag.runningState = aggStateUnknown
				ag.AgStructChannel <- AgStruct{
					ag.agrows,
					nil,
					ag.agi,
				}
				close(ag.AgStructChannel)
				return
			}
			if err := ag.arena.UnsafeReset(ag.Ctx); err != nil {
				ag.MoveToDraining(err)
				ag.runningState = aggStateUnknown
				ag.AgStructChannel <- AgStruct{
					ag.agrows,
					nil,
					ag.agi,
				}
				close(ag.AgStructChannel)
				return
			}
			for _, f := range ag.funcs {
				if f.seen != nil {
					f.seen = make(map[string]struct{})
				}
			}

			if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
				ag.MoveToDraining(err)
				ag.runningState = aggStateUnknown
				ag.AgStructChannel <- AgStruct{
					ag.agrows,
					nil,
					ag.agi,
				}
				close(ag.AgStructChannel)
				return
			}
			ag.accumulateRowsOpt()
			if ag.inputDone == true {
				return
			}
		} else {
			ag.emitRowOpt()
		}
	}
}

// emitRowOpt constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered the current row out.
func (ag *parallehashAggregator) emitRowOpt() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if ag.currAggWorkerIdx == -1 {
		for i := range ag.wokers {
			if len(ag.wokers[i].bucketsIter) == 0 {
				continue
			}
			ag.currAggWorkerIdx = i
			break
		}
	}
	if ag.currAggWorkerIdx == -1 {
		ag.MoveToDraining(nil)
		ag.runningState = aggStateUnknown
		return nil, nil
	}
	if len(ag.wokers[ag.currAggWorkerIdx].bucketsIter) > 0 {
		bucket := ag.wokers[ag.currAggWorkerIdx].bucketsIter[0]
		ag.wokers[ag.currAggWorkerIdx].bucketsIter = ag.wokers[ag.currAggWorkerIdx].bucketsIter[1:]
		bucket1 := ag.wokers[ag.currAggWorkerIdx].buckets[bucket]
		for i, b := range bucket1 {
			result, err := b.Result()
			if err != nil {
				ag.MoveToDraining(err)
				ag.runningState = aggStateUnknown
				return nil, nil
			}
			if result == nil {
				// We can't encode nil into an EncDatum, so we represent it with DNull.
				result = tree.DNull
			}
			ag.row[i] = sqlbase.DatumToEncDatum(ag.outputTypes[i], result)
		}

		if outRow := ag.ProcessRowHelper(ag.row); outRow != nil {
			return outRow, nil
		}
		return nil, nil
	}
	ag.currAggWorkerIdx++
	if ag.currAggWorkerIdx >= ag.parallelNum {
		ag.MoveToDraining(nil)
		ag.runningState = aggStateUnknown
		return nil, nil
	}
	return nil, nil
}

func (ag *orderedAggregator) emitRowOpt() {
	if ag.bucket == nil {
		// We've exhausted all of the aggregation buckets.

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.Ctx); err != nil {
			ag.MoveToDraining(err)
			ag.runningState = aggStateUnknown
			ag.AgStructChannel <- AgStruct{
				ag.agrows,
				nil,
				ag.agi,
			}
			close(ag.AgStructChannel)
			return
		}
		for _, f := range ag.funcs {
			if f.seen != nil {
				f.seen = make(map[string]struct{})
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.MoveToDraining(err)
			ag.runningState = aggStateUnknown
			ag.AgStructChannel <- AgStruct{
				ag.agrows,
				nil,
				ag.agi,
			}
			close(ag.AgStructChannel)
			return
		}
		ag.accumulateRowsOpt()
		if ag.inputDone == true {
			return
		}
	}

	bucket := ag.bucket
	ag.bucket = nil

	for i, b := range bucket {
		result, err := b.Result()
		if err != nil {
			ag.MoveToDraining(err)
			ag.runningState = aggStateUnknown
			ag.AgStructChannel <- AgStruct{
				ag.agrows,
				nil,
				ag.agi,
			}
			close(ag.AgStructChannel)
			return
		}
		if result == nil {
			// We can't encode nil into an EncDatum, so we represent it with DNull.
			result = tree.DNull
		}
		ag.row[i] = sqlbase.DatumToEncDatum(ag.outputTypes[i], result)
	}
	bucket.close(ag.Ctx)

	if outRow := ag.ProcessRowHelper(ag.row); outRow != nil {
		var temp = make([]sqlbase.EncDatum, len(outRow))
		copy(temp, outRow)
		ag.agrows[ag.agi] = temp
		ag.agi++
		if ag.agi > 31 {
			ag.AgStructChannel <- AgStruct{ag.agrows, nil, ag.agi}
			if len(ag.agDuplicateChannel) > 0 {
				ag.agrows = <-ag.agDuplicateChannel
				ag.agi = 0
			} else {
				ag.agrows = make([]sqlbase.EncDatumRow, 40)
				ag.agi = 0
			}
		}
		return
	}
}

func (ag *parallehashAggregator) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	ag.wg.Wait()
	if ag.ErrorIdx != -1 {
		return nil, ag.DrainHelper()
	}
	for ag.State == runbase.StateRunning {
		row, meta := ag.emitRowOpt()
		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (ag *parallehashAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ag.close()
}
