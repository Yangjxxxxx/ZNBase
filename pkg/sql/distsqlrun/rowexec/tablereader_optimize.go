package rowexec

import (
	"context"
	"sync"
	"time"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

const (
	// RowBufferSize for tablereader opt
	RowBufferSize = 64
)

// TableReaderOpt switch for tablereader opt
var TableReaderOpt *settings.BoolSetting

// DistributedBatchOpt switch for tablereader opt
var DistributedBatchOpt *settings.BoolSetting

// DistributedBatchParallelNums parallel num
var DistributedBatchParallelNums *settings.IntSetting

// TableReaderParallelNums tablereader parallel num
var TableReaderParallelNums *settings.IntSetting

func init() {
	TableReaderOpt = settings.RegisterBoolSetting(
		"sql.opt.operator.tablereader",
		"for tablereader parallel and unsync",
		false,
	)
	TableReaderParallelNums = settings.RegisterIntSetting(
		"sql.opt.operator.tablereader.parallelnum",
		"for tablereader parallel nums,max 32",
		4,
	)
	DistributedBatchParallelNums = settings.RegisterIntSetting(
		"sql.opt.operator.distributedbatch.parallelnum",
		"for distributedbatch parallel nums",
		4,
	)

	DistributedBatchOpt = settings.RegisterBoolSetting(
		"sql.opt.operator.distributedbatch",
		"for distributedbatch",
		false,
	)
}

type bufferRow struct {
	rows   sqlbase.EncDatumRows
	maxIdx int
}

// TablereaderBlocked for tablereader opt
type TablereaderBlocked struct {
	sync.WaitGroup
	ErrorChan    chan error
	processBases []runbase.ProcessorBase
	parallel     int
	curIdx       int
	// Parent pointer to main tablereader
	Parent *tableReader
	// RowFetchers for each worker fetch row
	RowFetchers []row.Fetcher
	// BlockRepons for tablereader get kvs
	BlockRepons chan roachpb.BlockStruct
	// CurRowBuf for tablereader opt nextrow
	CurRowBuf *bufferRow
	// RowResCH tablereader result set
	RowResCH chan *bufferRow
	// RowCollectCH res collect
	RowCollectCH chan *bufferRow
	errWorkerID  int
}

func (trBlocked *TablereaderBlocked) runWorkers(ctx context.Context, workerID int) {
	var probCKBuf *bufferRow
	var curIdx = 0
	defer func() {
		if err := recover(); err != nil {
		}
		trBlocked.Done()
	}()
label1:
	block, fetcher, pb := trBlocked.GetKVBlockAndFetcher(ctx, workerID)
	if block == nil {
		if curIdx != 0 {
			probCKBuf.maxIdx = curIdx
			trBlocked.RowResCH <- probCKBuf
		}
		return
	}
	for {
		if probCKBuf == nil {
			select {
			case probCKBuf = <-trBlocked.RowCollectCH:
			default:
				probCKBuf = new(bufferRow)
				probCKBuf.rows = make(sqlbase.EncDatumRows, RowBufferSize)
			}
		}
		rowTr, _, _, err := fetcher.BlockedNextRow(ctx, block)
		if err != nil {
			pb.MoveToDraining(err)
			trBlocked.errWorkerID = workerID
			return
		}
		if rowTr == nil {
			goto label1
		}
		row1, ok, err := pb.ProcessRowHelperOpt(rowTr)
		if err != nil {
			trBlocked.errWorkerID = workerID
			return
		}
		if row1 == nil {
			if !ok {
				if curIdx != 0 {
					probCKBuf.maxIdx = curIdx
					trBlocked.RowResCH <- probCKBuf
				}
				return
			}
		} else {
			if len(probCKBuf.rows[curIdx]) == 0 {
				probCKBuf.rows[curIdx] = make(sqlbase.EncDatumRow, len(trBlocked.Parent.OutputTypes()))
			}
			copy(probCKBuf.rows[curIdx], row1)
			curIdx++
			if curIdx == RowBufferSize {
				probCKBuf.maxIdx = RowBufferSize
				trBlocked.RowResCH <- probCKBuf
				probCKBuf = nil
				curIdx = 0
			}
		}
	}

}

// InitBlockedTr for tablereader parallel init
func (trBlocked *TablereaderBlocked) InitBlockedTr(
	tr *tableReader,
	fs []row.Fetcher,
	self runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	types []sqlbase.ColumnType,
	flowCtx *runbase.FlowCtx,
	processorID int32,
	output runbase.RowReceiver,
	memMonitor *mon.BytesMonitor,
	opts runbase.ProcStateOpts,
) error {
	trBlocked.RowFetchers = fs
	trBlocked.BlockRepons = make(chan roachpb.BlockStruct, trBlocked.parallel*8)
	trBlocked.processBases = make([]runbase.ProcessorBase, trBlocked.parallel)
	for idx := range trBlocked.processBases {
		err := trBlocked.processBases[idx].Init(self, post, types, flowCtx, processorID, output, memMonitor, opts)
		if err != nil {
			return err
		}
	}
	trBlocked.Parent = tr
	return nil
}

// StartWorkers for tablereader worker
func (trBlocked *TablereaderBlocked) StartWorkers(ctx context.Context) {
	workerChLen := trBlocked.parallel * 8
	trBlocked.RowResCH = make(chan *bufferRow, workerChLen)
	trBlocked.RowCollectCH = make(chan *bufferRow, workerChLen)
	trBlocked.Add(trBlocked.parallel)
	go func() {
		defer func() {
			if err := recover(); err != nil {
			}
		}()
		//for i := 0; i < workerChLen; i++ {
		//	bfr := new(bufferRow)
		//	bfr.rows = make(sqlbase.EncDatumRows, RowBufferSize)
		//	trBlocked.RowCollectCH <- bfr
		//}
		for i := 0; i < trBlocked.parallel; i++ {
			go trBlocked.runWorkers(ctx, i)
		}
		trBlocked.WaitGroup.Wait()
		close(trBlocked.RowResCH)
	}()
}

// NextRow for tablereader opt next
func (trBlocked *TablereaderBlocked) NextRow() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	var ok bool
	var p sqlbase.EncDatumRow
	var t1 time.Time
	select {
	case err := <-trBlocked.ErrorChan:
		if err != nil {
			return nil, &distsqlpb.ProducerMetadata{Err: err}
		}
	default:
	}
	if _, ok := trBlocked.Parent.input.(*InputStatCollector); ok {
		t1 = timeutil.Now()
	}
	for {
		if trBlocked.CurRowBuf == nil {
			trBlocked.CurRowBuf, ok = <-trBlocked.RowResCH
			if !ok {
				close(trBlocked.RowCollectCH)
				trBlocked.Parent.ProcessorBase.TrailingMeta = trBlocked.processBases[trBlocked.errWorkerID].TrailingMeta
				return nil, nil
			}
			p = trBlocked.CurRowBuf.rows[trBlocked.curIdx]
			trBlocked.curIdx++
		} else {
			if trBlocked.curIdx < trBlocked.CurRowBuf.maxIdx {
				p = trBlocked.CurRowBuf.rows[trBlocked.curIdx]
				trBlocked.curIdx++
			} else {
				select {
				case trBlocked.RowCollectCH <- trBlocked.CurRowBuf:
				default:
				}
				trBlocked.CurRowBuf = nil
				trBlocked.curIdx = 0
				continue
			}
		}
		if t, ok := trBlocked.Parent.input.(*InputStatCollector); ok {
			t.NumRows++
			t.StallTime += timeutil.Since(t1)
		}
		return p, nil
	}
}

// NextChunk for operator chunked row
func (trBlocked *TablereaderBlocked) NextChunk(reqChunk *runbase.ChunkBuf) (bool, error) {
	select {
	case err := <-trBlocked.ErrorChan:
		if err != nil {
			return false, err
		}
	default:
	}
	ck, ok := <-trBlocked.RowResCH
	if !ok {
		return false, nil
	}
	for i := 0; i < ck.maxIdx; i++ {
		copy(reqChunk.Buffer[i].Row, ck.rows[i])
	}
	reqChunk.MaxIdx = ck.maxIdx - 1
	select {
	case trBlocked.RowCollectCH <- ck:
	default:
	}
	return true, nil
}

// GetKVBlockAndFetcher for tablereader opt
func (trBlocked *TablereaderBlocked) GetKVBlockAndFetcher(
	ctx context.Context, idx int,
) (*roachpb.BlockStruct, *row.Fetcher, *runbase.ProcessorBase) {
	var res roachpb.BlockStruct
	var ok bool
	res, ok = <-trBlocked.BlockRepons
	if ok {
		_, err := trBlocked.RowFetchers[idx].BlockedNextKey(ctx, &res)
		if err != nil {
			return nil, nil, nil
		}
		return &res, &trBlocked.RowFetchers[idx], &trBlocked.processBases[idx]
	}
	return nil, nil, nil
}
