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
	"io/ioutil"
	"math"
	"os"
	"runtime"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// BatchReadFrom is a batch rows
const BatchReadFrom = 1024

// sorter sorts the input rows according to the specified ordering.
type sorterBase struct {
	runbase.ProcessorBase

	input    runbase.RowSource
	ordering sqlbase.ColumnOrdering
	matchLen uint32

	rows rowcontainer.SortableRowContainer
	i    rowcontainer.RowIterator

	// Only set if the ability to spill to disk is enabled.
	diskMonitor *mon.BytesMonitor

	parallel  int
	ChunkBufs []runbase.ChunkBuf

	optRows     []rowcontainer.SortableRowContainer
	optI        []rowcontainer.RowIterator
	optValid    []bool
	resultRow   chan *runbase.ChunkBuf
	resRows     sqlbase.EncDatumRows
	recycleRows chan *runbase.ChunkBuf
	isSortChunk bool
	UseTempFile bool
	fmemmon     *mon.BytesMonitor
	frc         *rowcontainer.FileBackedRowContainer
	RowLen      int
	internalOpt bool
}

func (s *sorterBase) init(
	self runbase.RowSource,
	flowCtx *runbase.FlowCtx,
	processorID int32,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
	ordering sqlbase.ColumnOrdering,
	matchLen uint32,
	opts runbase.ProcStateOpts,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		input = NewInputStatCollector(input)
		s.FinishTrace = s.outputStatsToTrace
	}

	useTempStorage := runbase.SettingUseTempStorageSorts.Get(&flowCtx.Cfg.Settings.SV) ||
		flowCtx.TestingKnobs().MemoryLimitBytes > 0
	var memMonitor *mon.BytesMonitor
	if useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The processor will overflow to disk if this limit is not enough.
		limit := flowCtx.TestingKnobs().MemoryLimitBytes
		if limit <= 0 {
			limit = runbase.SettingWorkMemBytes.Get(&flowCtx.Cfg.Settings.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit(
			"sortall-limited", limit, flowCtx.EvalCtx.Mon,
		)
		limitedMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		memMonitor = &limitedMon
	} else {
		memMonitor = runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "sorter-mem")
	}

	if err := s.ProcessorBase.Init(
		self, post, input.OutputTypes(), flowCtx, processorID, output, memMonitor, opts,
	); err != nil {
		memMonitor.Stop(ctx)
		return err
	}

	if useTempStorage {
		s.diskMonitor = runbase.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "sorter-disk")
		rc := rowcontainer.DiskBackedRowContainer{}
		rc.Init(
			ordering,
			input.OutputTypes(),
			s.EvalCtx,
			flowCtx.Cfg.TempStorage,
			memMonitor,
			s.diskMonitor,
			0, /* rowCapacity */
			false,
		)
		s.rows = &rc
	} else {
		rc := rowcontainer.MemRowContainer{}
		rc.InitWithMon(ordering, input.OutputTypes(), s.EvalCtx, memMonitor, 0 /* rowCapacity */)
		s.rows = &rc
	}

	s.input = input
	s.ordering = ordering
	s.matchLen = matchLen
	return nil
}

func (s *sorterBase) initSortAll(
	self runbase.RowSource,
	flowCtx *runbase.FlowCtx,
	processorID int32,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
	ordering sqlbase.ColumnOrdering,
	matchLen uint32,
	opts runbase.ProcStateOpts,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		input = NewInputStatCollector(input)
		s.FinishTrace = s.outputStatsToTrace
	}

	useTempStorage := runbase.SettingUseTempStorageSorts.Get(&flowCtx.Cfg.Settings.SV) ||
		flowCtx.TestingKnobs().MemoryLimitBytes > 0
	var memMonitor, fmemMonitor *mon.BytesMonitor
	if useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The processor will overflow to disk if this limit is not enough.
		limit := flowCtx.TestingKnobs().MemoryLimitBytes
		if limit <= 0 {
			limit = runbase.SettingWorkMemBytes.Get(&flowCtx.Cfg.Settings.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit(
			"sortall-limited", limit, flowCtx.EvalCtx.Mon,
		)
		limitedMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})

		fileBackmemMon := mon.MakeMonitorInheritWithLimit(
			"sortall-fileback1", limit, flowCtx.EvalCtx.Mon,
		)
		fileBackmemMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})

		memMonitor = &limitedMon
		fmemMonitor = &fileBackmemMon
	} else {
		memMonitor = runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "sorter-mem")
		fmemMonitor = runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "sorter-fileback2")
	}
	s.fmemmon = fmemMonitor

	if err := s.ProcessorBase.Init(
		self, post, input.OutputTypes(), flowCtx, processorID, output, memMonitor, opts,
	); err != nil {
		memMonitor.Stop(ctx)
		fmemMonitor.Stop(ctx)
		return err
	}
	tr, ok := input.(*tableReader)
	if ok {
		ok = tr.InternalOpt
	}
	if useTempStorage && TempFileStorageAble.Get(&flowCtx.Cfg.Settings.SV) && ok && len(input.OutputTypes())/len(ordering) >= 4 {
		s.RowLen = len(input.OutputTypes())
		s.UseTempFile = true
		s.diskMonitor = runbase.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "sorter-disk")
		if flowCtx.EvalCtx.SessionData.PC[sessiondata.SortSet].Parallel {
			flowCtx.EmergencyClose = true
			s.internalOpt = true
			s.resultRow = make(chan *runbase.ChunkBuf, 32)
			s.recycleRows = make(chan *runbase.ChunkBuf, 32)
			s.parallel = int(flowCtx.EvalCtx.SessionData.PC[sessiondata.SortSet].ParallelNum)
			if s.parallel <= 0 {
				s.parallel = 1
			}
			if s.parallel > 4 {
				s.parallel = 4
			}
			s.ChunkBufs = make([]runbase.ChunkBuf, s.parallel)
			s.optValid = make([]bool, s.parallel)
			s.resRows = make(sqlbase.EncDatumRows, s.parallel)
			for i := range s.ChunkBufs {
				s.ChunkBufs[i].Buffer = make([]*runbase.ChunkRow, RowBufferSize)
				for j := 0; j < RowBufferSize; j++ {
					s.ChunkBufs[i].Buffer[j] = new(runbase.ChunkRow)
					s.ChunkBufs[i].Buffer[j].Row = make(sqlbase.EncDatumRow, len(input.OutputTypes()))
				}
				s.ChunkBufs[i].MaxIdx = -1
			}

			s.optI = make([]rowcontainer.RowIterator, s.parallel)
			s.optRows = make([]rowcontainer.SortableRowContainer, s.parallel)
			for i := 0; i < s.parallel; i++ {
				FileStore := new(rowcontainer.Hstore)
				tempName, err := ioutil.TempDir("", "sort_temp_file")
				if err != nil {
					return err
				}
				err = os.Remove(tempName)
				if err != nil {
					return err
				}
				FileStore.Init(32, 1, tempName[5:], flowCtx.Cfg.TempPath+"/hstore", s.FlowCtx.Cfg.HsMgr)
				rc := rowcontainer.FileBackedRowContainer{}
				rc.FileStore = FileStore
				rc.Init(
					ordering,
					input.OutputTypes(),
					s.EvalCtx,
					flowCtx.Cfg.TempStorage,
					memMonitor,
					fmemMonitor,
					s.diskMonitor,
					0, /* rowCapacity */
				)
				s.optRows[i] = &rc
			}
		} else {
			FileStore := new(rowcontainer.Hstore)
			tempName, err := ioutil.TempDir("", "sort_temp_file")
			if err != nil {
				return err
			}
			err = os.Remove(tempName)
			if err != nil {
				return err
			}
			FileStore.Init(32, 1, tempName[5:], flowCtx.Cfg.TempPath+"/hstore", s.FlowCtx.Cfg.HsMgr)
			rc := rowcontainer.FileBackedRowContainer{}
			rc.FileStore = FileStore
			rc.Init(
				ordering,
				input.OutputTypes(),
				s.EvalCtx,
				flowCtx.Cfg.TempStorage,
				memMonitor,
				fmemMonitor,
				s.diskMonitor,
				0, /* rowCapacity */
			)
			s.rows = &rc
			s.frc = &rc
		}

		s.input = input
		s.ordering = ordering
		s.matchLen = matchLen
		return nil
	}

	if useTempStorage {
		s.diskMonitor = runbase.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "sorter-disk")
		rc := rowcontainer.DiskBackedRowContainer{}
		rc.Init(
			ordering,
			input.OutputTypes(),
			s.EvalCtx,
			flowCtx.Cfg.TempStorage,
			memMonitor,
			s.diskMonitor,
			0, /* rowCapacity */
			false,
		)
		s.rows = &rc

	} else {
		rc := rowcontainer.MemRowContainer{}
		rc.InitWithMon(ordering, input.OutputTypes(), s.EvalCtx, memMonitor, 0 /* rowCapacity */)
		s.rows = &rc
	}

	s.input = input
	s.ordering = ordering
	s.matchLen = matchLen
	return nil
}

func (s *sorterBase) initSortChunk(
	self runbase.RowSource,
	flowCtx *runbase.FlowCtx,
	processorID int32,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
	ordering sqlbase.ColumnOrdering,
	matchLen uint32,
	opts runbase.ProcStateOpts,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		input = NewInputStatCollector(input)
		s.FinishTrace = s.outputStatsToTrace
	}

	useTempStorage := runbase.SettingUseTempStorageSorts.Get(&flowCtx.Cfg.Settings.SV) ||
		flowCtx.TestingKnobs().MemoryLimitBytes > 0
	var memMonitor *mon.BytesMonitor
	if useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The processor will overflow to disk if this limit is not enough.
		limit := flowCtx.TestingKnobs().MemoryLimitBytes
		if limit <= 0 {
			limit = runbase.SettingWorkMemBytes.Get(&flowCtx.Cfg.Settings.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit(
			"sortall-limited", limit, flowCtx.EvalCtx.Mon,
		)
		limitedMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		memMonitor = &limitedMon
	} else {
		memMonitor = runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "sorter-mem")
	}

	if err := s.ProcessorBase.Init(
		self, post, input.OutputTypes(), flowCtx, processorID, output, memMonitor, opts,
	); err != nil {
		memMonitor.Stop(ctx)
		return err
	}

	if useTempStorage {
		s.diskMonitor = runbase.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "sorter-disk")
		if runbase.OptJoin {
			s.optRows = make([]rowcontainer.SortableRowContainer, 10)
			for i := 0; i < 10; i++ {
				rc := rowcontainer.DiskBackedRowContainer{}
				rc.Init(
					ordering,
					input.OutputTypes(),
					s.EvalCtx,
					flowCtx.Cfg.TempStorage,
					memMonitor,
					s.diskMonitor,
					0, /* rowCapacity */
					false,
				)
				s.optRows[i] = &rc
			}
		} else {
			rc := rowcontainer.DiskBackedRowContainer{}
			rc.Init(
				ordering,
				input.OutputTypes(),
				s.EvalCtx,
				flowCtx.Cfg.TempStorage,
				memMonitor,
				s.diskMonitor,
				0, /* rowCapacity */
				false,
			)
			s.rows = &rc
		}
	} else {
		if runbase.OptJoin {
			s.optRows = make([]rowcontainer.SortableRowContainer, 10)
			for i := 0; i < 10; i++ {
				rc := rowcontainer.MemRowContainer{}
				rc.InitWithMon(ordering, input.OutputTypes(), s.EvalCtx, memMonitor, 0 /* rowCapacity */)
				s.optRows[i] = &rc
			}
		} else {
			rc := rowcontainer.MemRowContainer{}
			rc.InitWithMon(ordering, input.OutputTypes(), s.EvalCtx, memMonitor, 0 /* rowCapacity */)
			s.rows = &rc
		}
	}

	s.input = input
	s.ordering = ordering
	s.matchLen = matchLen
	return nil
}

func (s *sortAllProcessor) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for s.State == runbase.StateRunning {
		if s.UseTempFile && s.internalOpt {
			if s.parallel == 1 && s.frc[0].Spilled() {
				if s.tmprows == nil {
					s.tmprows, s.needMore = <-s.rowsBuffer
				}
				if !s.needMore {
					s.MoveToDraining(nil)
					break
				} else {
					for s.curIdx < len(s.tmprows) {
						outRow := s.ProcessRowHelper(s.tmprows[s.curIdx])
						s.curIdx++
						if outRow != nil {
							return outRow, nil
						}
					}
					select {
					case s.rowsBufferCol <- s.tmprows:
					default:
					}
					s.tmprows = nil
					s.curIdx = 0
					continue
				}

			} else {
				if s.curBuf == nil {
					s.curBuf, s.needMore = <-s.resultRow
				}

				if !s.needMore {
					s.MoveToDraining(nil)
					break
				} else {
					for s.curIdx <= s.curBuf.MaxIdx {
						outRow := s.ProcessRowHelper(s.curBuf.Buffer[s.curIdx].Row)
						s.curIdx++
						if outRow != nil {
							return outRow, nil
						}
					}
					s.curBuf.MaxIdx = -1
					select {
					case s.recycleRows <- s.curBuf:
					default:
					}
					s.curBuf = nil
					s.curIdx = 0
					continue
				}
			}

		}

		if s.FlowCtx.EvalCtx.SessionData.PC[sessiondata.SortSet].Parallel {
			if s.tmprows == nil {
				s.tmprows, s.needMore = <-s.rowsBuffer
			}
			if !s.needMore {
				s.MoveToDraining(nil)
				break
			} else {
				for s.curIdx < len(s.tmprows) {
					outRow := s.ProcessRowHelper(s.tmprows[s.curIdx])
					s.curIdx++
					if outRow != nil {
						return outRow, nil
					}
				}
				select {
				case s.rowsBufferCol <- s.tmprows:
				default:
				}
				s.tmprows = nil
				s.curIdx = 0
				continue
			}
		}

		if ok, err := s.i.Valid(); err != nil || !ok {
			s.MoveToDraining(err)
			break
		}

		row, err := s.i.Row()
		if err != nil {
			s.MoveToDraining(err)
			break
		}

		if s.UseTempFile && s.sorterBase.frc.Spilled() {
			row, err = s.sorterBase.frc.GetRow(row)
			if err != nil {
				s.MoveToDraining(err)
				break
			}
		}

		s.i.Next()

		if outRow := s.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, s.DrainHelper()
}

func (s *sortAllProcessor) PopResultRowAndIdx(rows sqlbase.EncDatumRows) tempResult {
	var temp = rows[0]
	var idx = 0
	for i := 1; i < s.parallel; i++ {
		if s.compare(temp, rows[i]) >= 0 {
			temp = rows[i]
			idx = i
		}
	}
	return tempResult{idx: idx, row: temp}
}

func (s *sortAllProcessor) compare(lrow, rrow sqlbase.EncDatumRow) (cmp int) {
	if lrow == nil {
		return 1
	}
	if rrow == nil {
		return -1
	}
	for _, c := range s.ordering {
		cmp = lrow[c.ColIdx].Datum.Compare(s.EvalCtx, rrow[c.ColIdx].Datum)
		if cmp == 0 {
			continue
		} else if c.Direction == encoding.Descending {
			cmp = -cmp
			break
		} else {
			break
		}
	}
	return cmp
}

// Next is part of the RowSource interface. It is extracted into sorterBase
// because this implementation of next is shared between the sortAllProcessor
// and the sortTopKProcessor.
func (s *sorterBase) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for s.State == runbase.StateRunning {
		if ok, err := s.i.Valid(); err != nil || !ok {
			s.MoveToDraining(err)
			break
		}

		row, err := s.i.Row()
		if err != nil {
			s.MoveToDraining(err)
			break
		}
		s.i.Next()

		if outRow := s.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, s.DrainHelper()
}

func (s *sorterBase) close() {
	// We are done sorting rows, close the iterator we have open.
	if s.InternalClose() {
		if s.i != nil {
			s.i.Close()
		}
		ctx := s.Ctx
		if runbase.OptJoin && s.isSortChunk {
			for i := 0; i < 10; i++ {
				s.optRows[i].Close(ctx)
			}
		} else if s.UseTempFile {
			if !s.internalOpt {
				s.frc.Close(ctx)
			} else {
				for i := range s.optRows {
					s.optI[i].Close()
					s.optRows[i].Close(ctx)
				}
			}
		} else {
			s.rows.Close(ctx)
		}

		s.MemMonitor.Stop(ctx)
		if s.fmemmon != nil {
			s.fmemmon.Stop(ctx)
		}
		if s.diskMonitor != nil {
			s.diskMonitor.Stop(ctx)
		}
	}
}

var _ distsqlpb.DistSQLSpanStats = &SorterStats{}

const sorterTagPrefix = "sorter."

// Stats implements the SpanStats interface.
func (ss *SorterStats) Stats() map[string]string {
	statsMap := ss.InputStats.Stats(sorterTagPrefix)
	statsMap[sorterTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(ss.MaxAllocatedMem)
	statsMap[sorterTagPrefix+MaxDiskTagSuffix] = humanizeutil.IBytes(ss.MaxAllocatedDisk)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ss *SorterStats) StatsForQueryPlan() []string {
	return append(
		ss.InputStats.StatsForQueryPlan("" /* prefix */),
		fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(ss.MaxAllocatedMem)),
		fmt.Sprintf("%s: %s", MaxDiskQueryPlanSuffix, humanizeutil.IBytes(ss.MaxAllocatedDisk)),
	)
}

// outputStatsToTrace outputs the collected sorter stats to the trace. Will fail
// silently if stats are not being collected.
func (s *sorterBase) outputStatsToTrace() {
	is, ok := getInputStats(s.FlowCtx, s.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(s.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&SorterStats{
				InputStats:       is,
				MaxAllocatedMem:  s.MemMonitor.MaximumBytes(),
				MaxAllocatedDisk: s.diskMonitor.MaximumBytes(),
			},
		)
	}
}

func newSorter(
	ctx context.Context,
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.SorterSpec,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (runbase.Processor, error) {
	count := uint64(0)
	if post.Limit != 0 && post.Filter.Empty() {
		// The sorter needs to produce Offset + Limit rows. The ProcOutputHelper
		// will discard the first Offset ones.
		// LIMIT and OFFSET should each never be greater than math.MaxInt64, the
		// parser ensures this.
		if post.Limit > math.MaxInt64 || post.Offset > math.MaxInt64 {
			return nil, errors.Errorf("error creating sorter: limit %d offset %d too large", post.Limit, post.Offset)
		}
		count = post.Limit + post.Offset
	}

	// Choose the optimal processor.
	if spec.OrderingMatchLen == 0 {
		if count == 0 {
			// No specified ordering match length and unspecified limit; no
			// optimizations are possible so we simply load all rows into memory and
			// sort all values in-place. It has a worst-case time complexity of
			// O(n*log(n)) and a worst-case space complexity of O(n).
			return newSortAllProcessor(ctx, flowCtx, processorID, spec, input, post, output)
		}
		// No specified ordering match length but specified limit; we can optimize
		// our sort procedure by maintaining a max-heap populated with only the
		// smallest k rows seen. It has a worst-case time complexity of
		// O(n*log(k)) and a worst-case space complexity of O(k).
		return newSortTopKProcessor(flowCtx, processorID, spec, input, post, output, count)
	}
	// Ordering match length is specified. We will be able to use existing
	// ordering in order to avoid loading all the rows into memory. If we're
	// scanning an index with a prefix matching an ordering prefix, we can only
	// accumulate values for equal fields in this prefix, sort the accumulated
	// chunk and then output.
	// TODO(irfansharif): Add optimization for case where both ordering match
	// length and limit is specified.
	return newSortChunksProcessor(flowCtx, processorID, spec, input, post, output)
}

// sortAllProcessor reads in all values into the wrapped rows and
// uses sort.Sort to sort all values in-place. It has a worst-case time
// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
//
// This processor is intended to be used when all values need to be sorted.
type sortAllProcessor struct {
	sorterBase
	wg         sync.WaitGroup
	curBuf     *runbase.ChunkBuf
	needMore   bool
	curIdx     int
	tempResult tempResult

	frc           []*rowcontainer.FileBackedRowContainer
	rowsBuffer    chan sqlbase.EncDatumRows
	rowsBufferCol chan sqlbase.EncDatumRows
	tmprows       sqlbase.EncDatumRows
	datumAlloc    sqlbase.DatumAlloc
}

var _ runbase.Processor = &sortAllProcessor{}
var _ runbase.RowSource = &sortAllProcessor{}

const sortAllProcName = "sortAll"

func newSortAllProcessor(
	ctx context.Context,
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.SorterSpec,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	out runbase.RowReceiver,
) (runbase.Processor, error) {
	proc := &sortAllProcessor{}
	if err := proc.sorterBase.initSortAll(
		proc, flowCtx, processorID, input, post, out,
		distsqlpb.ConvertToColumnOrdering(spec.OutputOrdering),
		spec.OrderingMatchLen,
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{input},
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				proc.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	if proc.UseTempFile && len(proc.optRows) > 0 {
		proc.frc = make([]*rowcontainer.FileBackedRowContainer, len(proc.optRows))
		for i, c := range proc.optRows {
			if frc, ok := c.(*rowcontainer.FileBackedRowContainer); ok {
				proc.frc[i] = frc
			}
		}
	}

	return proc, nil
}

// Start is part of the RowSource interface.
func (s *sortAllProcessor) Start(ctx context.Context) context.Context {
	s.input.Start(ctx)
	ctx = s.StartInternal(ctx, sortAllProcName)

	if s.UseTempFile && s.internalOpt {
		s.tempResult.idx = -1
		s.wg.Add(s.parallel)
		for i := 0; i < s.parallel; i++ {
			go s.fillchunk(s.ChunkBufs[i], i)
		}
		s.wg.Wait()
		if s.parallel == 1 && s.frc[0].Spilled() {
			s.rowsBuffer = make(chan sqlbase.EncDatumRows, 2)
			s.rowsBufferCol = make(chan sqlbase.EncDatumRows, 2)
			for i := 0; i < 2; i++ {
				s.rowsBufferCol <- make(sqlbase.EncDatumRows, BatchReadFrom)
			}
			go s.BlockReadFromFileAndSend(s.frc[0])
		} else {
			go s.PopResultRowToChannel(s.resultRow)
		}
		return ctx
	}

	valid, err := s.fill()
	if !valid || err != nil {
		s.MoveToDraining(err)
	}
	if s.FlowCtx.EvalCtx.SessionData.PC[sessiondata.SortSet].Parallel && s.State == runbase.StateRunning {
		s.rowsBuffer = make(chan sqlbase.EncDatumRows, 2)
		s.rowsBufferCol = make(chan sqlbase.EncDatumRows, 2)
		for i := 0; i < 2; i++ {
			s.rowsBufferCol <- make(sqlbase.EncDatumRows, BatchReadFrom)
		}
		go s.BatchReadFromTempStore()
	}

	return ctx
}

//GetBatch is part of the RowSource interface.
func (s *sortAllProcessor) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (s *sortAllProcessor) SetChan() {}

// fill fills s.rows with the input's rows.
//
// Metadata is buffered in s.trailingMeta.
//
// The ok retval is false if an error occurred or if the input returned an error
// metadata record. The caller is expected to inspect the error (if any) and
// drain if it's not recoverable. It is possible for ok to be false even if no
// error is returned - in case an error metadata was received.
func (s *sortAllProcessor) fill() (ok bool, _ error) {
	ctx := s.EvalCtx.Ctx()

	for {
		row, meta := s.input.Next()
		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				return false, nil
			}
			continue
		}
		if row == nil {
			break
		}

		if err := s.rows.AddRow(ctx, row); err != nil {
			return false, err
		}
	}
	s.rows.Sort(ctx)

	s.i = s.rows.NewFinalIterator(ctx)
	s.i.Rewind()
	return true, nil
}

func (s *sortAllProcessor) fillchunk(curChunk runbase.ChunkBuf, i int) {
	defer func() {
		s.wg.Done()
	}()
	ctx := s.EvalCtx.Ctx()
	for {
		moreRows, err := s.input.(*tableReader).NextChunk(&curChunk)
		if !moreRows {
			if err != nil {
				s.MoveToDraining(err)
			}
			break
		}
		for idx := 0; idx <= curChunk.MaxIdx; idx++ {
			row, meta := curChunk.Buffer[idx].Row, curChunk.Buffer[idx].Meta
			if err := s.optRows[i].AddRow(ctx, row); err != nil {
				s.MoveToDraining(err)
				return
			}
			if meta != nil {
				if meta.Err != nil {
					s.MoveToDraining(nil)
					return
				}
			}
		}

	}
	s.optRows[i].Sort(ctx)
	s.optI[i] = s.optRows[i].NewFinalIterator(ctx)
	s.optI[i].Rewind()
}

// ConsumerDone is part of the RowSource interface.
func (s *sortAllProcessor) ConsumerDone() {
	s.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortAllProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}

func (s *sortAllProcessor) BatchReadFromTempStore() {
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
		defer close(s.rowsBuffer)
	}()
	var idx int
	var resRows sqlbase.EncDatumRows
	resRows = <-s.rowsBufferCol
	for {
		if ok, err := s.i.Valid(); err != nil || !ok {
			if err != nil {
				s.MoveToDraining(err)
			}
			if !ok {
				s.rowsBuffer <- resRows[:idx]
			}
			return
		}
		row, err := s.i.Row()
		if err != nil {
			s.MoveToDraining(err)
			return
		}
		if len(resRows[idx]) == 0 {
			resRows[idx] = make(sqlbase.EncDatumRow, len(s.input.OutputTypes()))
		}
		copy(resRows[idx], row)
		s.i.Next()
		idx++
		if idx == BatchReadFrom {
			idx = 0
			s.rowsBuffer <- resRows
			select {
			case resRows = <-s.rowsBufferCol:
			default:
				resRows = make(sqlbase.EncDatumRows, BatchReadFrom)
			}
		}
	}
}

func (s *sortAllProcessor) BlockReadFromFileAndSend(rc *rowcontainer.FileBackedRowContainer) {
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
	}()
	rowidx := 0
	ba := rc.FileStore.GetBatchForRead(BatchReadFrom)
	resRows := <-s.rowsBufferCol
	for {
		if ok, err := s.optI[0].Valid(); err != nil || !ok {
			rc.FileStore.BatchRead(ba)
			for i := 0; i < rowidx; i++ {
				data := ba.Data[i]
				for _, idx := range rc.ValueIdx() {
					var err error
					resRows[i][idx], data, err = sqlbase.EncDatumFromBuffer(&rc.OriginTypes()[idx], sqlbase.DatumEncoding_VALUE, data)
					if err != nil {
						s.MoveToDraining(err)
					}
				}
				for j := range resRows[i] {
					err := resRows[i][j].EnsureDecoded(&rc.OriginTypes()[j], &s.datumAlloc)
					if err != nil {
						s.MoveToDraining(err)
					}
				}
			}
			s.rowsBuffer <- resRows[:rowidx]
			close(s.rowsBuffer)
			break
		}
		keyRow, err := s.optI[0].Row()
		if len(resRows[rowidx]) == 0 {
			resRows[rowidx] = make(sqlbase.EncDatumRow, s.RowLen)
		}
		if err != nil {
			s.MoveToDraining(err)
		}

		for i, orderInfo := range rc.OriginOrder() {
			// Types with composite key encodings are decoded from the value.
			if sqlbase.HasCompositeKeyEncoding(rc.OriginTypes()[orderInfo.ColIdx].SemanticType) {
				continue
			}
			col := orderInfo.ColIdx
			resRows[rowidx][col] = keyRow[i]
		}
		err = keyRow[len(keyRow)-1].EnsureDecoded(&sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}, &s.datumAlloc)
		if err != nil {
			s.MoveToDraining(err)
		}
		rc.FileStore.ApplyOffsetToBacth([]byte(*keyRow[len(keyRow)-1].Datum.(*tree.DBytes)), ba)
		rowidx++
		s.optI[0].Next()
		if rowidx == BatchReadFrom {
			rc.FileStore.BatchRead(ba)
			for i, data := range ba.Data {
				for _, idx := range rc.ValueIdx() {
					var err error
					resRows[i][idx], data, err = sqlbase.EncDatumFromBuffer(&rc.OriginTypes()[idx], sqlbase.DatumEncoding_VALUE, data)
					if err != nil {
						s.MoveToDraining(err)
					}
				}
				for j := range resRows[i] {
					err := resRows[i][j].EnsureDecoded(&rc.OriginTypes()[j], &s.datumAlloc)
					if err != nil {
						s.MoveToDraining(err)
					}
				}
			}
			s.rowsBuffer <- resRows
			rowidx = 0
			rc.FileStore.ResetBatch(ba)
			select {
			case resRows = <-s.rowsBufferCol:
			default:
				resRows = make(sqlbase.EncDatumRows, BatchReadFrom)
			}
		}
	}

}

// sortTopKProcessor creates a max-heap in its wrapped rows and keeps
// this heap populated with only the top k values seen. It accomplishes this
// by comparing new values (before the deep copy) with the top of the heap.
// If the new value is less than the current top, the top will be replaced
// and the heap will be fixed. If not, the new value is dropped. When finished,
// the max heap is converted to a min-heap effectively sorting the values
// correctly in-place. It has a worst-case time complexity of O(n*log(k)) and a
// worst-case space complexity of O(k).
//
// This processor is intended to be used when exactly k values need to be sorted,
// where k is known before sorting begins.
//
// TODO(irfansharif): (taken from TODO found in sql/sort.go) There are better
// algorithms that can achieve a sorted top k in a worst-case time complexity
// of O(n + k*log(k)) while maintaining a worst-case space complexity of O(k).
// For instance, the top k can be found in linear time, and then this can be
// sorted in linearithmic time.
type sortTopKProcessor struct {
	sorterBase
	k uint64
}

var _ runbase.Processor = &sortTopKProcessor{}
var _ runbase.RowSource = &sortTopKProcessor{}

const sortTopKProcName = "sortTopK"

var errSortTopKZeroK = errors.New("invalid value 0 for k")

func newSortTopKProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.SorterSpec,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	out runbase.RowReceiver,
	k uint64,
) (runbase.Processor, error) {
	if k == 0 {
		return nil, errors.Wrap(errSortTopKZeroK, "error creating top k sorter")
	}
	ordering := distsqlpb.ConvertToColumnOrdering(spec.OutputOrdering)
	proc := &sortTopKProcessor{k: k}
	if err := proc.sorterBase.init(
		proc, flowCtx, processorID, input, post, out,
		ordering, spec.OrderingMatchLen,
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{input},
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				proc.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return proc, nil
}

// Start is part of the RowSource interface.
func (s *sortTopKProcessor) Start(ctx context.Context) context.Context {
	s.input.Start(ctx)
	ctx = s.StartInternal(ctx, sortTopKProcName)

	// The execution loop for the SortTopK processor is similar to that of the
	// SortAll processor; the difference is that we push rows into a max-heap
	// of size at most K, and only sort those.
	heapCreated := false
	for {
		row, meta := s.input.Next()
		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				s.MoveToDraining(nil /* err */)
				break
			}
			continue
		}
		if row == nil {
			break
		}

		if uint64(s.rows.Len()) < s.k {
			// Accumulate up to k values.
			if err := s.rows.AddRow(ctx, row); err != nil {
				s.MoveToDraining(err)
				break
			}
		} else {
			if !heapCreated {
				// Arrange the k values into a max-heap.
				s.rows.InitTopK()
				heapCreated = true
			}
			// Replace the max value if the new row is smaller, maintaining the
			// max-heap.
			if err := s.rows.MaybeReplaceMax(ctx, row); err != nil {
				s.MoveToDraining(err)
				break
			}
		}
	}
	s.rows.Sort(ctx)
	s.i = s.rows.NewFinalIterator(ctx)
	s.i.Rewind()
	return ctx
}

//GetBatch is part of the RowSource interface.
func (s *sortTopKProcessor) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (s *sortTopKProcessor) SetChan() {}

// ConsumerDone is part of the RowSource interface.
func (s *sortTopKProcessor) ConsumerDone() {
	s.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortTopKProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}

// If we're scanning an index with a prefix matching an ordering prefix, we only accumulate values
// for equal fields in this prefix, sort the accumulated chunk and then output.
type sortChunksProcessor struct {
	sorterBase

	alloc sqlbase.DatumAlloc

	// sortChunksProcessor accumulates rows that are equal on a prefix, until it
	// encounters a row that is greater. It stores that greater row in nextChunkRow
	nextChunkRow sqlbase.EncDatumRow

	resultChunk    chan rowcontainer.SortableRowContainer
	duplicateChunk chan rowcontainer.SortableRowContainer
	chunks         rowcontainer.SortableRowContainer
}

var _ runbase.Processor = &sortChunksProcessor{}
var _ runbase.RowSource = &sortChunksProcessor{}

const sortChunksProcName = "sortChunks"

func newSortChunksProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.SorterSpec,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	out runbase.RowReceiver,
) (runbase.Processor, error) {
	ordering := distsqlpb.ConvertToColumnOrdering(spec.OutputOrdering)

	proc := &sortChunksProcessor{}
	if err := proc.sorterBase.initSortChunk(
		proc, flowCtx, processorID, input, post, out, ordering, spec.OrderingMatchLen,
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{input},
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				proc.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	if runbase.OptJoin {

		proc.i = proc.optRows[9].NewFinalIterator(proc.Ctx)

		return proc, nil
	}
	proc.i = proc.rows.NewFinalIterator(proc.Ctx)
	return proc, nil
}

// chunkCompleted is a helper function that determines if the given row shares the same
// values for the first matchLen ordering columns with the given prefix.
func (s *sortChunksProcessor) chunkCompleted(
	nextChunkRow, prefix sqlbase.EncDatumRow,
) (bool, error) {
	types := s.input.OutputTypes()
	for _, ord := range s.ordering[:s.matchLen] {
		col := ord.ColIdx
		cmp, err := nextChunkRow[col].Compare(&types[col], &s.alloc, s.EvalCtx, &prefix[col])
		if cmp != 0 || err != nil {
			return true, err
		}
	}
	return false, nil
}

// fill one chunk of rows from the input and sort them.
//
// Metadata is buffered in s.trailingMeta. Returns true if a valid chunk of rows
// has been read and sorted, false otherwise (if the input had no more rows or
// if a metadata record was encountered). The caller is expected to drain when
// this returns false.
func (s *sortChunksProcessor) fill() (bool, error) {
	ctx := s.Ctx

	var meta *distsqlpb.ProducerMetadata

	nextChunkRow := s.nextChunkRow
	s.nextChunkRow = nil
	for nextChunkRow == nil {
		nextChunkRow, meta = s.input.Next()
		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				return false, nil
			}
			continue
		} else if nextChunkRow == nil {
			return false, nil
		}
		break
	}
	prefix := nextChunkRow

	// Add the chunk
	if err := s.rows.AddRow(ctx, nextChunkRow); err != nil {
		return false, err
	}

	// We will accumulate rows to form a chunk such that they all share the same values
	// as prefix for the first s.matchLen ordering columns.
	for {
		nextChunkRow, meta = s.input.Next()

		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				return false, nil
			}
			continue
		}
		if nextChunkRow == nil {
			break
		}

		chunkCompleted, err := s.chunkCompleted(nextChunkRow, prefix)

		if err != nil {
			return false, err
		}
		if chunkCompleted {
			s.nextChunkRow = nextChunkRow
			break
		}

		if err := s.rows.AddRow(ctx, nextChunkRow); err != nil {
			return false, err
		}
	}

	s.rows.Sort(ctx)

	return true, nil
}

func (s *sortChunksProcessor) fillRows() {
	ctx := s.Ctx

	var meta *distsqlpb.ProducerMetadata
BEGIN:

	s.rows = <-s.duplicateChunk

	if err := s.rows.UnsafeReset(ctx); err != nil {
		s.MoveToDraining(err)
		close(s.resultChunk)
		return
	}

	nextChunkRow := s.nextChunkRow
	s.nextChunkRow = nil
	for nextChunkRow == nil {
		nextChunkRow, meta = s.input.Next()
		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				close(s.resultChunk)
				return
			}
			continue
		} else if nextChunkRow == nil {
			close(s.resultChunk)
			//s.MoveToDraining(nil)
			return
		}
		break
	}
	prefix := nextChunkRow

	// Add the chunk
	if err := s.rows.AddRow(ctx, nextChunkRow); err != nil {
		close(s.resultChunk)
		return
	}

	// We will accumulate rows to form a chunk such that they all share the same values
	// as prefix for the first s.matchLen ordering columns.
	for {
		nextChunkRow, meta = s.input.Next()

		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				close(s.resultChunk)
				return
			}
			continue
		}
		if nextChunkRow == nil {
			break
		}

		chunkCompleted, err := s.chunkCompleted(nextChunkRow, prefix)

		if err != nil {
			close(s.resultChunk)
			return
		}
		if chunkCompleted {
			s.nextChunkRow = nextChunkRow
			break
		}

		if err := s.rows.AddRow(ctx, nextChunkRow); err != nil {
			close(s.resultChunk)
			return
		}
	}

	s.rows.Sort(ctx)
	s.resultChunk <- s.rows

	goto BEGIN
}

// Start is part of the RowSource interface.
func (s *sortChunksProcessor) Start(ctx context.Context) context.Context {
	s.input.Start(ctx)
	return s.StartInternal(ctx, sortChunksProcName)
}

//GetBatch is part of the RowSource interface.
func (s *sortChunksProcessor) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (s *sortChunksProcessor) SetChan() {
	s.isSortChunk = true
	s.resultChunk = make(chan rowcontainer.SortableRowContainer, 9)
	s.duplicateChunk = make(chan rowcontainer.SortableRowContainer, 9)
	for i := 0; i < 9; i++ {
		s.duplicateChunk <- s.optRows[i]
	}
	go s.fillRows()
}

// Next is part of the RowSource interface.
func (s *sortChunksProcessor) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	ctx := s.Ctx
	if runbase.OptJoin {
		for s.State == runbase.StateRunning {
			ok, err := s.i.Valid()
			if err != nil {
				s.MoveToDraining(err)
				break
			}
			if !ok {
				if s.chunks != nil {
					s.duplicateChunk <- s.chunks
				}
				s.chunks, ok = <-s.resultChunk
				if !ok {
					s.MoveToDraining(nil)
					close(s.duplicateChunk)
					break
				} else {
					s.i.Close()
					s.i = s.chunks.NewFinalIterator(ctx)
					s.i.Rewind()
					continue
				}
			}
			row, err := s.i.Row()
			if err != nil {
				s.MoveToDraining(err)
				break
			}
			s.i.Next()

			if outRow := s.ProcessRowHelper(row); outRow != nil {
				return outRow, nil
			}
		}
		return nil, s.DrainHelper()
	}
	for s.State == runbase.StateRunning {
		ok, err := s.i.Valid()
		if err != nil {
			s.MoveToDraining(err)
			break
		}
		// If we don't have an active chunk, clear and refill it.
		if !ok {
			if err := s.rows.UnsafeReset(ctx); err != nil {
				s.MoveToDraining(err)
				break
			}
			valid, err := s.fill()
			if !valid || err != nil {
				s.MoveToDraining(err)
				break
			}
			s.i.Close()
			s.i = s.rows.NewFinalIterator(ctx)
			s.i.Rewind()
			if ok, err := s.i.Valid(); err != nil || !ok {
				s.MoveToDraining(err)
				break
			}
		}

		// If we have an active chunk, get a row from it.
		row, err := s.i.Row()
		if err != nil {
			s.MoveToDraining(err)
			break
		}
		s.i.Next()

		if outRow := s.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, s.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortChunksProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}
