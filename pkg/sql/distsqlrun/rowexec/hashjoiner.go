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
	"os"
	"runtime"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// hashJoinerInitialBufferSize controls the size of the initial buffering phase
// (see hashJoiner). This only applies when falling back to disk is disabled.
const hashJoinerInitialBufferSize = 4 * 1024 * 1024

// hashJoinerState represents the state of the processor.
type hashJoinerState int

const (
	hjStateUnknown hashJoinerState = iota
	// hjBuilding represents the state the hashJoiner is in when it is trying to
	// determine which side to store (i.e. which side is smallest).
	// At most hashJoinerInitialBufferSize is used to buffer rows from either
	// side. The first input to be finished within this limit is the smallest
	// side. If both inputs still have rows, the hashJoiner will default to
	// storing the right side. When a side is stored, a hash map is also
	// constructed from the equality columns to the rows.
	hjBuilding
	// hjConsumingStoredSide represents the state the hashJoiner is in if a small
	// side was not found. In this case, the hashJoiner will fully consume the
	// right side. This state is skipped if the hashJoiner determined the smallest
	// side, since it must have fully consumed that side.
	hjConsumingStoredSide
	// hjReadingProbeSide represents the state the hashJoiner is in when it reads
	// rows from the input that wasn't chosen to be stored.
	hjReadingProbeSide
	// hjProbingRow represents the state the hashJoiner is in when it uses a row
	// read in hjReadingProbeSide to probe the stored hash map with.
	hjProbingRow
	// hjEmittingUnmatched represents the state the hashJoiner is in when it is
	// emitting unmatched rows from its stored side after having consumed the
	// other side. This only happens when executing a FULL OUTER, LEFT/RIGHT
	// OUTER and ANTI joins (depending on which side we store).
	hjContinue

	hjEmittingUnmatched
)

// hashJoiner performs a hash join. There is no guarantee on the output
// ordering.

type hashJoiner struct {
	joinerBase
	*HashJoinOpt
	InternalOpt  bool
	runningState hashJoinerState

	diskMonitor *mon.BytesMonitor
	fMemMonitor *mon.BytesMonitor

	leftSource, rightSource runbase.RowSource

	// initialBufferSize is the maximum amount of data we buffer from each stream
	// as part of the initial buffering phase. Normally
	// hashJoinerInitialBufferSize, can be tweaked for tests.
	initialBufferSize int64

	// We read a portion of both streams, in the hope that one is small. One of
	// the containers will contain the entire "stored" stream, the other just the
	// start of the other stream.
	rows [2]rowcontainer.MemRowContainer

	// storedSide is set by the initial buffering phase and indicates which
	// stream we store fully and build the hashRowContainer from.
	storedSide joinSide

	// nullEquality indicates that NULL = NULL should be considered true. Used for
	// INTERSECT and EXCEPT.
	nullEquality bool

	useTempStorage bool
	storedRows     rowcontainer.HashRowContainer

	// Used by tests to force a storedSide.
	forcedStoredSide *joinSide

	// probingRowState is state used when hjProbingRow.
	probingRowState struct {
		// row is the row being probed with.
		row sqlbase.EncDatumRow
		// iter is an iterator over the bucket that matches row on the equality
		// columns.
		iter rowcontainer.RowMarkerIterator
		// matched represents whether any row that matches row on equality columns
		// has also passed the ON condition.
		matched bool
	}

	// emittingUnmatchedState is used when hjEmittingUnmatched.
	emittingUnmatchedState struct {
		iter rowcontainer.RowIterator
	}

	// testingKnobMemFailPoint specifies a state in which the hashJoiner will
	// fail at a random point during this phase.
	testingKnobMemFailPoint hashJoinerState

	// Context cancellation checker.
	cancelChecker *sqlbase.CancelChecker

	readerType bool

	tableName string

	ProbeStructChan chan ProbeStruct

	newrows ProbeStruct

	index int

	hashi int

	hashrows []sqlbase.EncDatumRow

	duplicate chan []sqlbase.EncDatumRow

	hashflag bool
}

var _ runbase.Processor = &hashJoiner{}
var _ runbase.RowSource = &hashJoiner{}

//ProbeStruct contain a block of result rows and index
type ProbeStruct struct {
	row   []sqlbase.EncDatumRow
	meta  *distsqlpb.ProducerMetadata
	index int
}

const hashJoinerProcName = "hash joiner"

//for hashjoin opt
//new hashjoiner for each joinworker
func newHashJoinerForParallel(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.HashJoinerSpec,
	leftSource runbase.RowSource,
	rightSource runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (*hashJoiner, error) {
	h := &hashJoiner{
		initialBufferSize: hashJoinerInitialBufferSize,
		leftSource:        leftSource,
		rightSource:       rightSource,
	}
	if spec.ForceSide != 2 {
		h.forcedStoredSide = new(joinSide)
		*h.forcedStoredSide = joinSide(spec.ForceSide)
	}
	numMergedColumns := 0
	if spec.MergedColumns {
		numMergedColumns = len(spec.LeftEqColumns)
	}
	if err := h.joinerBase.init(
		h,
		flowCtx,
		processorID,
		leftSource.OutputTypes(),
		rightSource.OutputTypes(),
		spec.Type,
		spec.OnExpr,
		spec.LeftEqColumns,
		spec.RightEqColumns,
		uint32(numMergedColumns),
		post,
		output,
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{h.leftSource, h.rightSource},
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				h.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	st := h.FlowCtx.Cfg.Settings
	ctx := h.FlowCtx.EvalCtx.Ctx()
	h.useTempStorage = runbase.SettingUseTempStorageJoins.Get(&st.SV) ||
		h.FlowCtx.TestingKnobs().MemoryLimitBytes > 0 ||
		h.testingKnobMemFailPoint != hjStateUnknown
	if h.useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The hashJoiner will overflow to disk if this limit is not enough.
		limit := h.FlowCtx.TestingKnobs().MemoryLimitBytes
		if limit <= 0 {
			limit = runbase.SettingWorkMemBytes.Get(&st.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit("hashjoiner-limited", limit, flowCtx.EvalCtx.Mon)
		limitedMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		limitedMon1 := mon.MakeMonitorInheritWithLimit("hashjoiner-file-back-limited", limit, flowCtx.EvalCtx.Mon)
		limitedMon1.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		h.fMemMonitor = &limitedMon1
		h.MemMonitor = &limitedMon
		h.diskMonitor = runbase.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "hashjoiner-disk")
		// Override initialBufferSize to be half of this processor's memory
		// limit. We consume up to h.initialBufferSize bytes from each input
		// stream.
		h.initialBufferSize = limit / 2
	} else {
		h.MemMonitor = runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "hashjoiner-mem")
	}
	// If the trace is recording, instrument the hashJoiner to collect stats.
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		h.leftSource = NewInputStatCollector(h.leftSource)
		h.rightSource = NewInputStatCollector(h.rightSource)
		h.FinishTrace = h.outputStatsToTrace
	}
	h.rows[leftSide].InitWithMon(
		nil /* ordering */, h.leftSource.OutputTypes(), h.EvalCtx, h.MemMonitor, 0, /* rowCapacity */
	)
	h.rows[rightSide].InitWithMon(
		nil /* ordering */, h.rightSource.OutputTypes(), h.EvalCtx, h.MemMonitor, 0, /* rowCapacity */
	)

	if h.joinType == sqlbase.IntersectAllJoin || h.joinType == sqlbase.ExceptAllJoin {
		h.nullEquality = true
	}
	return h, nil
}

func newHashJoiner(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.HashJoinerSpec,
	leftSource runbase.RowSource,
	rightSource runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (*hashJoiner, error) {
	h := &hashJoiner{
		initialBufferSize: hashJoinerInitialBufferSize,
		leftSource:        leftSource,
		rightSource:       rightSource,
	}
	//if spec.ForceSide != 2 {
	//	h.forcedStoredSide = new(joinSide)
	//	*h.forcedStoredSide = joinSide(spec.ForceSide)
	//}
	numMergedColumns := 0
	if spec.MergedColumns {
		numMergedColumns = len(spec.LeftEqColumns)
	}
	if err := h.joinerBase.init(
		h,
		flowCtx,
		processorID,
		leftSource.OutputTypes(),
		rightSource.OutputTypes(),
		spec.Type,
		spec.OnExpr,
		spec.LeftEqColumns,
		spec.RightEqColumns,
		uint32(numMergedColumns),
		post,
		output,
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{h.leftSource, h.rightSource},
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				h.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	if len(h.eqCols[0]) != 0 && spec.ParallelFlag && flowCtx.CanParallel {
		h.InternalOpt = true
	}
	//if hashjoin opt
	if h.InternalOpt {
		h.FlowCtx.EmergencyClose = true
		h.HashJoinOpt = new(HashJoinOpt)
		h.HashJoinOpt.parent = h
		h.parallel = int(flowCtx.EvalCtx.SessionData.PC[sessiondata.HashJoinerSet].ParallelNum)
		if flowCtx.EvalCtx.ShouldOrdered {
			h.parallel = 1
		} else {
			if h.parallel > runtime.NumCPU()*2 {
				h.parallel = runtime.NumCPU() * 2
			}
			if h.parallel <= 0 {
				h.parallel = 1
			}
		}
		h.joinWorkers = make([]*joinWorker, h.parallel)
		for i := 0; i < h.parallel; i++ {
			jW := new(joinWorker)
			jW.hashJoiner, _ = newHashJoinerForParallel(flowCtx,
				processorID,
				spec,
				leftSource,
				rightSource,
				post,
				output)
			h.HashJoinOpt.joinWorkers[i] = jW
		}
		h.HashJoinOpt.initHashJoinOpt()
		//if the hashtable can be split
		if len(h.eqCols[0]) != 0 && len(h.eqCols[1]) != 0 {
			h.isSplitHashTable = true
		}
	}
	st := h.FlowCtx.Cfg.Settings
	ctx := h.FlowCtx.EvalCtx.Ctx()
	h.useTempStorage = runbase.SettingUseTempStorageJoins.Get(&st.SV) ||
		h.FlowCtx.TestingKnobs().MemoryLimitBytes > 0 ||
		h.testingKnobMemFailPoint != hjStateUnknown
	if h.useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The hashJoiner will overflow to disk if this limit is not enough.
		limit := h.FlowCtx.TestingKnobs().MemoryLimitBytes
		if limit <= 0 {
			limit = runbase.SettingWorkMemBytes.Get(&st.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit("hashjoiner-limited", limit, flowCtx.EvalCtx.Mon)
		limitedMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		limitedMon1 := mon.MakeMonitorInheritWithLimit("hashjoiner-file-back-limited", limit, flowCtx.EvalCtx.Mon)
		limitedMon1.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		h.fMemMonitor = &limitedMon1
		h.MemMonitor = &limitedMon
		h.diskMonitor = runbase.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "hashjoiner-disk")
		// Override initialBufferSize to be half of this processor's memory
		// limit. We consume up to h.initialBufferSize bytes from each input
		// stream.
		h.initialBufferSize = limit / 2
	} else {
		h.MemMonitor = runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "hashjoiner-mem")
	}

	// If the trace is recording, instrument the hashJoiner to collect stats.
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		h.leftSource = NewInputStatCollector(h.leftSource)
		h.rightSource = NewInputStatCollector(h.rightSource)
		h.FinishTrace = h.outputStatsToTrace
	}

	h.rows[leftSide].InitWithMon(
		nil /* ordering */, h.leftSource.OutputTypes(), h.EvalCtx, h.MemMonitor, 0, /* rowCapacity */
	)
	h.rows[rightSide].InitWithMon(
		nil /* ordering */, h.rightSource.OutputTypes(), h.EvalCtx, h.MemMonitor, 0, /* rowCapacity */
	)

	if h.joinType == sqlbase.IntersectAllJoin || h.joinType == sqlbase.ExceptAllJoin {
		h.nullEquality = true
	}

	return h, nil
}

// Start is part of the RowSource interface.
func (h *hashJoiner) Start(ctx context.Context) context.Context {
	h.leftSource.Start(ctx)
	h.rightSource.Start(ctx)
	ctx = h.StartInternal(ctx, hashJoinerProcName)
	h.cancelChecker = sqlbase.NewCancelChecker(ctx)
	h.runningState = hjBuilding
	switch h.rightSource.(type) {
	case *tableReader:
		h.readerType = false
		input := h.rightSource.(*tableReader)
		h.tableName = input.fetcher.GetDesc()
	default:
		h.readerType = true
	}
	return ctx
}

//GetBatch is part of the RowSource interface.
func (h *hashJoiner) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (h *hashJoiner) SetChan() {
	h.ProbeStructChan = make(chan ProbeStruct, 100)
	h.duplicate = make(chan []sqlbase.EncDatumRow, 100)
}

// Next is part of the RowSource interface.
func (h *hashJoiner) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if h.InternalOpt {
		var row sqlbase.EncDatumRow
		var meta *distsqlpb.ProducerMetadata
		for h.State == runbase.StateRunning {
			if err := h.cancelChecker.Check(); err != nil {
				h.MoveToDraining(err)
				return nil, h.DrainHelper()
			}
			if h.hashState == 0 {
				switch h.runningState {
				case hjBuilding:
					h.runningState, row, meta = h.build()
				case hjConsumingStoredSide:
					h.runningState, row, meta = h.consumeStoredSide()
				case hjReadingProbeSide:
					if !h.isSplitHashTable {
						h.HashJoinOpt.isDisk = h.storedRows.IsDisk()
					}
					go h.startOuterAndWorker()
					h.hashState = 1
				}
				if row == nil && meta == nil {
					continue
				}
				if meta != nil {
					return nil, meta
				}
				if outRow := h.ProcessRowHelper(row); outRow != nil {
					return outRow, nil
				}
			}
			if h.hashState == 1 {
				res, ok := <-h.resultCh
				if !ok {
					h.MoveToDraining(nil)
					return nil, h.DrainHelper()
				}
				if res.Meta != nil {
					h.Res.Meta = res.Meta
					h.resCollect <- res
					if h.Res.Meta.Err != nil {
						h.MoveToDraining(h.Res.Meta.Err)
						return nil, h.DrainHelper()
					}
					return nil, h.Res.Meta
				}
				copy(h.Res.Row, res.Row)
				h.resCollect <- res
				if out := h.ProcessRowHelper(h.Res.Row); out != nil {
					return out, nil
				}
			}
		}
		return nil, h.DrainHelper()
	}
	//for h.State == StateRunning {

	if !runbase.OptJoin {
		for h.State == runbase.StateRunning {
			var row sqlbase.EncDatumRow
			var meta *distsqlpb.ProducerMetadata
			switch h.runningState {
			case hjBuilding:
				h.runningState, row, meta = h.build()
			case hjConsumingStoredSide:
				h.runningState, row, meta = h.consumeStoredSide()
			case hjReadingProbeSide:
				h.runningState, row, meta = h.readProbeSide()
			case hjProbingRow:
				h.runningState, row, meta = h.probeRow()
			case hjEmittingUnmatched:
				h.runningState, row, meta = h.emitUnmatched()
			default:
				log.Fatalf(h.Ctx, "unsupported state: %d", h.runningState)
			}

			if row == nil && meta == nil {
				continue
			}
			if meta != nil {
				return nil, meta
			}
			if outRow := h.ProcessRowHelper(row); outRow != nil {
				if h.EvalCtx != nil && h.EvalCtx.RecursiveData != nil && h.EvalCtx.RecursiveData.FormalJoin {
					rowLength := h.ProcessorBase.EvalCtx.RecursiveData.RowLength
					virtualColID := h.ProcessorBase.EvalCtx.RecursiveData.VirtualColID
					if len(outRow) == rowLength {
						h.ProcessorBase.IsCycle(outRow, virtualColID)
					}
				}
				return outRow, nil
			}
		}
		return nil, h.DrainHelper()
	}
	if h.readerType == false {
		if h.tableName == "settings" || h.tableName == "role_members" {
			for h.State == runbase.StateRunning {
				var row sqlbase.EncDatumRow
				var meta *distsqlpb.ProducerMetadata
				switch h.runningState {
				case hjBuilding:
					h.runningState, row, meta = h.build()
				case hjConsumingStoredSide:
					h.runningState, row, meta = h.consumeStoredSide()
				case hjReadingProbeSide:
					h.runningState, row, meta = h.readProbeSide()
				case hjProbingRow:
					h.runningState, row, meta = h.probeRow()
				case hjEmittingUnmatched:
					h.runningState, row, meta = h.emitUnmatched()
				default:
					log.Fatalf(h.Ctx, "unsupported state: %d", h.runningState)
				}

				if row == nil && meta == nil {
					continue
				}
				if meta != nil {
					return nil, meta
				}
				if outRow := h.ProcessRowHelper(row); outRow != nil {
					return outRow, nil
				}
			}
			return nil, h.DrainHelper()
		}
		if h.hashrows == nil {
			h.hashrows = make([]sqlbase.EncDatumRow, 40)
		}
		for h.State == runbase.StateRunning {
			var row sqlbase.EncDatumRow
			var meta *distsqlpb.ProducerMetadata
			switch h.runningState {
			case hjBuilding:
				h.runningState, row, meta = h.build()
				if row == nil && meta == nil {
					continue
				}
				if meta != nil {
					return nil, meta
				}
				if outRow := h.ProcessRowHelper(row); outRow != nil {
					return outRow, nil
				}
			case hjConsumingStoredSide:
				h.runningState, row, meta = h.consumeStoredSide()
				if row == nil && meta == nil {
					continue
				}
				if meta != nil {
					return nil, meta
				}
				if outRow := h.ProcessRowHelper(row); outRow != nil {
					return outRow, nil
				}
			case hjReadingProbeSide:
				go h.readProbeSide1()
				h.runningState = hjContinue
			case hjContinue:

			case hjEmittingUnmatched:
				h.emitUnmatched1()
			default:
				log.Fatalf(h.Ctx, "unsupported state: %d", h.runningState)
			}
			for {
				if h.newrows.row != nil {
					for h.index < h.newrows.index {
						outRow := h.ProcessRowHelper(h.newrows.row[h.index])
						h.runningState = hjContinue
						h.index++
						if outRow != nil {
							return outRow, nil
						}
					}
					h.index = 0
					h.newrows.index = 0
					if len(h.duplicate) < 90 {
						h.duplicate <- h.newrows.row

					}
					h.newrows.row = nil
				} else {
					var ok bool
					h.newrows, ok = <-h.ProbeStructChan
					if ok {
						if h.newrows.meta != nil {
							if h.newrows.meta.Err != nil {
								h.MoveToDraining(nil /* err */)
							}
							h.runningState = hjContinue
							return nil, h.newrows.meta
						}
						continue
					} else {
						if h.newrows.row == nil && !ok {
							h.MoveToDraining(nil /* err */)
							h.runningState = hjContinue
							for len(h.duplicate) > 1 {
								_ = <-h.duplicate
							}
							_, ok := <-h.duplicate
							if ok {
								close(h.duplicate)
							}
							break
						}
					}
				}
			}

		}
		return nil, h.DrainHelper()
	}
	for h.State == runbase.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *distsqlpb.ProducerMetadata
		switch h.runningState {
		case hjBuilding:
			h.runningState, row, meta = h.build()
		case hjConsumingStoredSide:
			h.runningState, row, meta = h.consumeStoredSide()
		case hjReadingProbeSide:
			h.runningState, row, meta = h.readProbeSide()
		case hjProbingRow:
			h.runningState, row, meta = h.probeRow()
		case hjEmittingUnmatched:
			h.runningState, row, meta = h.emitUnmatched()
		default:
			log.Fatalf(h.Ctx, "unsupported state: %d", h.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		if meta != nil {
			return nil, meta
		}
		if outRow := h.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, h.DrainHelper()

}

// ConsumerClosed is part of the RowSource interface.
func (h *hashJoiner) ConsumerClosed() {
	h.close()
}

func (h *hashJoiner) build() (hashJoinerState, sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	// setStoredSideTransition is a helper function that sets storedSide on the
	// hashJoiner and performs initialization before a transition to
	// hjConsumingStoredSide.
	setStoredSideTransition := func(
		side joinSide,
	) (hashJoinerState, sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
		if h.HashJoinOpt != nil && h.isSplitHashTable {
			h.storedSide = side
			for _, jw := range h.joinWorkers {
				jw.storedSide = side
				//初始化每个joinworker的stored端的rowcontainer
				if err := jw.initStoredRows(); err != nil {
					h.MoveToDraining(err)
					return hjStateUnknown, nil, h.DrainHelper()
				}
			}
			return hjConsumingStoredSide, nil, nil
		}
		h.storedSide = side
		if err := h.initStoredRows(); err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
		return hjConsumingStoredSide, nil, nil
	}
	setStoredSideTransitionForJoinOpt := func(
		side joinSide, row sqlbase.EncDatumRow, idx int,
	) (hashJoinerState, sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
		h.storedSide = side
		for _, jw := range h.joinWorkers {
			jw.storedSide = side
			//初始化每个joinworker的stored端的rowcontainer
			if err := jw.initStoredRows(); err != nil {
				h.MoveToDraining(err)
				return hjStateUnknown, nil, h.DrainHelper()
			}
		}
		err := h.joinWorkers[idx].storedRows.AddRow(h.Ctx, row)
		if err != nil {
			return hjStateUnknown, nil, nil
		}
		return hjConsumingStoredSide, nil, nil
	}
	if h.forcedStoredSide != nil {
		return setStoredSideTransition(*h.forcedStoredSide)
	}

	for {
		leftUsage := h.rows[leftSide].MemUsage()
		rightUsage := h.rows[rightSide].MemUsage()

		if leftUsage >= h.initialBufferSize && rightUsage >= h.initialBufferSize {
			// Both sides have reached the buffer size limit. Move on to storing and
			// fully consuming the right side.
			log.VEventf(h.Ctx, 1, "buffer phase found no short stream with buffer size %d", h.initialBufferSize)
			return setStoredSideTransition(rightSide)
		}

		side := rightSide
		if leftUsage < rightUsage {
			side = leftSide
		}

		row, meta, emitDirectly, err := h.receiveNext(side)
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		} else if meta != nil {
			if meta.Err != nil {
				h.MoveToDraining(nil /* err */)
				return hjStateUnknown, nil, meta
			}
			return hjBuilding, nil, meta
		} else if emitDirectly {
			return hjBuilding, row, nil
		}

		if row == nil {
			// This side has been fully consumed, it is the shortest side.
			// If storedSide is empty, we might be able to short-circuit.
			if h.rows[side].Len() == 0 &&
				(h.joinType == sqlbase.InnerJoin ||
					(h.joinType == sqlbase.LeftOuterJoin && side == leftSide) ||
					(h.joinType == sqlbase.RightOuterJoin && side == rightSide)) {
				h.MoveToDraining(nil /* err */)
				return hjStateUnknown, nil, h.DrainHelper()
			}
			// We could skip hjConsumingStoredSide and move straight to
			// hjReadingProbeSide apart from the fact that hjConsumingStoredSide
			// pre-reserves mark memory. To keep the code simple and avoid
			// duplication, we move to hjConsumingStoredSide.
			return setStoredSideTransition(side)
		}

		// Add the row to the correct container.
		if err := h.rows[side].AddRow(h.Ctx, row); err != nil {
			// If this error is a memory limit error, move to hjConsumingStoredSide.
			h.storedSide = side
			if sqlbase.IsOutOfMemoryError(err) {
				if !h.useTempStorage {
					err = errors.Wrap(err, "error while attempting hashJoiner disk spill: temp storage disabled")
				} else {
					if h.InternalOpt && h.isSplitHashTable {
						idx := h.ReHash(row, side)
						return setStoredSideTransitionForJoinOpt(side, row, idx)
					}
					if err := h.initStoredRows(); err != nil {
						h.MoveToDraining(err)
						return hjStateUnknown, nil, h.DrainHelper()
					}
					addErr := h.storedRows.AddRow(h.Ctx, row)
					if addErr == nil {
						return hjConsumingStoredSide, nil, nil
					}
					err = errors.Wrap(err, addErr.Error())
				}
			}
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
}

// consumeStoredSide fully consumes the stored side and adds the rows to
// h.storedRows. It assumes that h.storedRows has been initialized through
// h.initStoredRows().
func (h *hashJoiner) consumeStoredSide() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*distsqlpb.ProducerMetadata,
) {
	side := h.storedSide
	var row sqlbase.EncDatumRow
	var meta *distsqlpb.ProducerMetadata
	var emitDirectly bool
	var err error
	for {
		//for hashjoin opt
		//如果stored端有预读数据，先将数据从memcontainer读出
		if h.HashJoinOpt != nil && h.isSplitHashTable && h.rows[side].Len() > 0 {
			row = h.rows[side].EncRow(0)
			h.rows[side].PopFirst()
		} else {
			row, meta, emitDirectly, err = h.receiveNext(side)
			if err != nil {
				h.MoveToDraining(err)
				return hjStateUnknown, nil, h.DrainHelper()
			} else if meta != nil {
				if meta.Err != nil {
					h.MoveToDraining(nil /* err */)
					return hjStateUnknown, nil, meta
				}
				return hjConsumingStoredSide, nil, meta
			} else if emitDirectly {
				return hjConsumingStoredSide, row, nil
			}
		}

		if row == nil {
			//for hashjoin opt
			//直接进入读outer表阶段
			if h.HashJoinOpt != nil && h.isSplitHashTable {
				return hjReadingProbeSide, nil, nil
			}
			// The stored side has been fully consumed, move on to hjReadingProbeSide.
			// If storedRows is in-memory, pre-reserve the memory needed to mark.
			if rc, ok := h.storedRows.(*rowcontainer.HashMemRowContainer); ok {
				// h.storedRows is hashMemRowContainer and not a disk backed one, so
				// h.useTempStorage is false and we cannot spill to disk, so we simply
				// will return an error if it occurs.
				err = rc.ReserveMarkMemoryMaybe(h.Ctx)
			} else if hdbrc, ok := h.storedRows.(*rowcontainer.HashDiskBackedRowContainer); ok {
				err = hdbrc.ReserveMarkMemoryMaybe(h.Ctx)
			} else if hdbrc, ok := h.storedRows.(*rowcontainer.HashFileBackedRowContainer); ok {
				err = hdbrc.ReserveMarkMemoryMaybe(h.Ctx)
			} else {
				panic("unexpected type of storedRows in hashJoiner")
			}
			if err != nil {
				h.MoveToDraining(err)
				return hjStateUnknown, nil, h.DrainHelper()
			}
			return hjReadingProbeSide, nil, nil
		}
		//for hashjoin opt
		//hash重分布，分配到hashrow的joinworker标记为true
		if h.HashJoinOpt != nil && h.isSplitHashTable {
			idx := h.ReHash(row, h.storedSide)
			if !h.joinWorkers[idx].isGetHashRow {
				h.joinWorkers[idx].isGetHashRow = true
			}
			err = h.joinWorkers[idx].storedRows.AddRow(h.Ctx, row)
			if err != nil {
				h.MoveToDraining(err)
				return hjStateUnknown, nil, h.DrainHelper()
			}
		} else {
			err = h.storedRows.AddRow(h.Ctx, row)
			// Regardless of the underlying row container (disk backed or in-memory
			// only), we cannot do anything about an error if it occurs.
			if err != nil {
				h.MoveToDraining(err)
				return hjStateUnknown, nil, h.DrainHelper()
			}
		}
	}
}

func (h *hashJoiner) readProbeSide() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*distsqlpb.ProducerMetadata,
) {
	side := otherSide(h.storedSide)

	var row sqlbase.EncDatumRow
	// First process the rows that were already buffered.
	if h.rows[side].Len() > 0 {
		row = h.rows[side].EncRow(0)
		h.rows[side].PopFirst()
	} else {
		var meta *distsqlpb.ProducerMetadata
		var emitDirectly bool
		var err error
		row, meta, emitDirectly, err = h.receiveNext(side)
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		} else if meta != nil {
			if meta.Err != nil {
				h.MoveToDraining(nil /* err */)
				return hjStateUnknown, nil, meta
			}
			return hjReadingProbeSide, nil, meta
		} else if emitDirectly {
			return hjReadingProbeSide, row, nil
		}

		if row == nil {
			// The probe side has been fully consumed. Move on to hjEmittingUnmatched
			// if unmatched rows on the stored side need to be emitted, otherwise
			// finish.
			if shouldEmitUnmatchedRow(h.storedSide, h.joinType) {
				i := h.storedRows.NewUnmarkedIterator(h.Ctx)
				i.Rewind()
				h.emittingUnmatchedState.iter = i
				return hjEmittingUnmatched, nil, nil
			}
			h.MoveToDraining(nil /* err */)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}

	// Probe with this row. Get the iterator over the matching bucket ready for
	// hjProbingRow.
	h.probingRowState.row = row
	h.probingRowState.matched = false
	if h.probingRowState.iter == nil {
		i, err := h.storedRows.NewBucketIterator(h.Ctx, row, h.eqCols[side])
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
		h.probingRowState.iter = i
	} else {
		if err := h.probingRowState.iter.Reset(h.Ctx, row); err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
	h.probingRowState.iter.Rewind()
	return hjProbingRow, nil, nil
}

func (h *hashJoiner) readProbeSide1() {
	side := otherSide(h.storedSide)

	var row sqlbase.EncDatumRow
	// First process the rows that were already buffered.
	for {
		if h.rows[side].Len() > 0 {
			row = h.rows[side].EncRow(0)
			h.rows[side].PopFirst()
		} else {
			var meta *distsqlpb.ProducerMetadata
			var emitDirectly bool
			var err error
			row, meta, emitDirectly, err = h.receiveNext(side)
			if err != nil {
				h.MoveToDraining(err)
				h.runningState = hjStateUnknown
				h.ProbeStructChan <- ProbeStruct{
					h.hashrows,
					h.DrainHelper(),
					h.hashi,
				}
				close(h.ProbeStructChan)
				return
			} else if meta != nil {
				if meta.Err != nil {
					h.MoveToDraining(nil /* err */)
					h.runningState = hjStateUnknown
					h.ProbeStructChan <- ProbeStruct{
						h.hashrows,
						meta,
						h.hashi,
					}
					close(h.ProbeStructChan)
					return
				}
				h.ProbeStructChan <- ProbeStruct{
					h.hashrows,
					meta,
					h.hashi,
				}
				close(h.ProbeStructChan)
				return
			} else if emitDirectly {
				var temp = make([]sqlbase.EncDatum, len(row))
				copy(temp, row)
				h.hashrows[h.hashi] = temp
				h.hashi++
				if len(h.hashrows) > 31 {
					h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
					if len(h.duplicate) > 0 {
						h.hashrows = <-h.duplicate
						h.hashi = 0
					} else {
						h.hashrows = make([]sqlbase.EncDatumRow, 40)
						h.hashi = 0
					}
				}
				continue
			}

			if row == nil {
				// The probe side has been fully consumed. Move on to hjEmittingUnmatched
				// if unmatched rows on the stored side need to be emitted, otherwise
				// finish.
				if shouldEmitUnmatchedRow(h.storedSide, h.joinType) {
					i := h.storedRows.NewUnmarkedIterator(h.Ctx)
					i.Rewind()
					h.emittingUnmatchedState.iter = i
					h.runningState = hjEmittingUnmatched
					if len(h.hashrows) > 31 {
						h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
						if len(h.duplicate) > 0 {
							h.hashrows = <-h.duplicate
							h.hashi = 0
						} else {
							h.hashrows = make([]sqlbase.EncDatumRow, 40)
							h.hashi = 0
						}
					}
					continue
				}
				h.MoveToDraining(nil /* err */)
				h.runningState = hjStateUnknown
				h.ProbeStructChan <- ProbeStruct{
					h.hashrows,
					h.DrainHelper(),
					h.hashi,
				}
				close(h.ProbeStructChan)
				return
			}
		}

		// Probe with this row. Get the iterator over the matching bucket ready for
		// hjProbingRow.
		h.probingRowState.row = row
		h.probingRowState.matched = false
		if h.probingRowState.iter == nil {
			i, err := h.storedRows.NewBucketIterator(h.Ctx, row, h.eqCols[side])
			if err != nil {
				h.MoveToDraining(err)
				h.runningState = hjStateUnknown
				h.ProbeStructChan <- ProbeStruct{
					h.hashrows,
					h.DrainHelper(),
					h.hashi,
				}
				close(h.ProbeStructChan)
				return
			}
			h.probingRowState.iter = i
		} else {
			if err := h.probingRowState.iter.Reset(h.Ctx, row); err != nil {
				h.MoveToDraining(err)
				h.runningState = hjStateUnknown
				h.ProbeStructChan <- ProbeStruct{
					h.hashrows,
					h.DrainHelper(),
					h.hashi,
				}
				close(h.ProbeStructChan)
				return
			}
		}
		h.probingRowState.iter.Rewind()

		h.probeRow1()
		for h.hashflag {
			h.probeRow1()
		}
		continue
	}
}

func (h *hashJoiner) probeRow() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*distsqlpb.ProducerMetadata,
) {
	i := h.probingRowState.iter
	if ok, err := i.Valid(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	} else if !ok {
		// In this case we have reached the end of the matching bucket. Check if any
		// rows passed the ON condition. If they did, move back to
		// hjReadingProbeSide to get the next probe row.
		if h.probingRowState.matched {
			return hjReadingProbeSide, nil, nil
		}
		// If not, this probe row is unmatched. Check if it needs to be emitted.
		if renderedRow, shouldEmit := h.shouldEmitUnmatched(
			h.probingRowState.row, otherSide(h.storedSide),
		); shouldEmit {
			return hjReadingProbeSide, renderedRow, nil
		}
		return hjReadingProbeSide, nil, nil
	}

	if err := h.cancelChecker.Check(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	row := h.probingRowState.row
	otherRow, err := i.Row()
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}
	defer i.Next()

	var renderedRow sqlbase.EncDatumRow
	if h.storedSide == rightSide {
		renderedRow, err = h.render(row, otherRow)
	} else {
		renderedRow, err = h.render(otherRow, row)
	}
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	// If the ON condition failed, renderedRow is nil.
	if renderedRow == nil {
		return hjProbingRow, nil, nil
	}

	h.probingRowState.matched = true
	shouldEmit := h.joinType != sqlbase.LeftAntiJoin && h.joinType != sqlbase.ExceptAllJoin
	if shouldMark(h.storedSide, h.joinType) {
		// Matched rows are marked on the stored side for 2 reasons.
		// 1: For outer joins, anti joins, and EXCEPT ALL to iterate through
		// the unmarked rows.
		// 2: For semi-joins and INTERSECT ALL where the left-side is stored,
		// multiple rows from the right may match to the same row on the left.
		// The rows on the left should only be emitted the first time
		// a right row matches it, then marked to not be emitted again.
		// (Note: an alternative is to remove the entry from the stored
		// side, but our containers do not support that today).
		// TODO(peter): figure out a way to reduce this special casing below.
		if i.IsMarked(h.Ctx) {
			switch h.joinType {
			case sqlbase.LeftSemiJoin:
				shouldEmit = false
			case sqlbase.IntersectAllJoin:
				shouldEmit = false
			case sqlbase.ExceptAllJoin:
				// We want to mark a stored row if possible, so move on to the next
				// match. Reset h.probingRowState.matched in case we don't find any more
				// matches and want to emit this row.
				h.probingRowState.matched = false
				return hjProbingRow, nil, nil
			}
		} else if err := i.Mark(h.Ctx, true); err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
	nextState := hjProbingRow
	if shouldShortCircuit(h.storedSide, h.joinType) {
		nextState = hjReadingProbeSide
	}
	if shouldEmit {
		if h.joinType == sqlbase.IntersectAllJoin {
			// We found a match, so we are done with this row.
			return hjReadingProbeSide, renderedRow, nil
		}
		return nextState, renderedRow, nil
	}

	return nextState, nil, nil
}

func (h *hashJoiner) probeRow1() {
	i := h.probingRowState.iter
	if ok, err := i.Valid(); err != nil {
		h.MoveToDraining(err)
		h.runningState = hjStateUnknown
		h.ProbeStructChan <- ProbeStruct{
			h.hashrows,
			h.DrainHelper(),
			h.hashi,
		}
		close(h.ProbeStructChan)
		return
	} else if !ok {
		// In this case we have reached the end of the matching bucket. Check if any
		// rows passed the ON condition. If they did, move back to
		// hjReadingProbeSide to get the next probe row.
		if h.probingRowState.matched {
			if h.hashi > 31 {
				h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
				if len(h.duplicate) > 0 {
					h.hashrows = <-h.duplicate
					h.hashi = 0
				} else {
					h.hashrows = make([]sqlbase.EncDatumRow, 40)
					h.hashi = 0
				}
			}
			h.hashflag = false
			return
		}
		// If not, this probe row is unmatched. Check if it needs to be emitted.
		if renderedRow, shouldEmit := h.shouldEmitUnmatched(
			h.probingRowState.row, otherSide(h.storedSide),
		); shouldEmit {
			var temp = make([]sqlbase.EncDatum, len(renderedRow))
			copy(temp, renderedRow)
			h.hashrows[h.hashi] = temp
			h.hashi++
			if h.hashi > 31 {
				h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
				if len(h.duplicate) > 0 {
					h.hashrows = <-h.duplicate
					h.hashi = 0
				} else {
					h.hashrows = make([]sqlbase.EncDatumRow, 40)
					h.hashi = 0
				}
			}
			h.hashflag = false
			return
		}
		if h.hashi > 31 {
			h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
			if len(h.duplicate) > 0 {
				h.hashrows = <-h.duplicate
				h.hashi = 0
			} else {
				h.hashrows = make([]sqlbase.EncDatumRow, 40)
				h.hashi = 0
			}
		}
		h.hashflag = false
		return
	}

	if err := h.cancelChecker.Check(); err != nil {
		h.MoveToDraining(err)
		h.runningState = hjStateUnknown
		h.ProbeStructChan <- ProbeStruct{
			h.hashrows,
			h.DrainHelper(),
			h.hashi,
		}
		close(h.ProbeStructChan)
		h.hashflag = false
		return
	}

	row := h.probingRowState.row
	otherRow, err := i.Row()
	if err != nil {
		h.MoveToDraining(err)
		h.runningState = hjStateUnknown
		h.ProbeStructChan <- ProbeStruct{
			h.hashrows,
			h.DrainHelper(),
			h.hashi,
		}
		close(h.ProbeStructChan)
		h.hashflag = false
		return
	}
	defer i.Next()

	if h.storedSide == rightSide {
		err = h.render1(row, otherRow, h.hashrows, uint32(h.hashi))
	} else {
		err = h.render1(otherRow, row, h.hashrows, uint32(h.hashi))
	}
	if err != nil {
		h.MoveToDraining(err)
		h.runningState = hjStateUnknown
		h.ProbeStructChan <- ProbeStruct{
			h.hashrows,
			h.DrainHelper(),
			h.hashi,
		}
		close(h.ProbeStructChan)
		h.hashflag = false
		return
	}

	// If the ON condition failed, renderedRow is nil.

	h.probingRowState.matched = true
	shouldEmit := h.joinType != sqlbase.LeftAntiJoin && h.joinType != sqlbase.ExceptAllJoin
	if shouldMark(h.storedSide, h.joinType) {
		// Matched rows are marked on the stored side for 2 reasons.
		// 1: For outer joins, anti joins, and EXCEPT ALL to iterate through
		// the unmarked rows.
		// 2: For semi-joins and INTERSECT ALL where the left-side is stored,
		// multiple rows from the right may match to the same row on the left.
		// The rows on the left should only be emitted the first time
		// a right row matches it, then marked to not be emitted again.
		// (Note: an alternative is to remove the entry from the stored
		// side, but our containers do not support that today).
		// TODO(peter): figure out a way to reduce this special casing below.
		if i.IsMarked(h.Ctx) {
			switch h.joinType {
			case sqlbase.LeftSemiJoin:
				shouldEmit = false
			case sqlbase.IntersectAllJoin:
				shouldEmit = false
			case sqlbase.ExceptAllJoin:
				// We want to mark a stored row if possible, so move on to the next
				// match. Reset h.probingRowState.matched in case we don't find any more
				// matches and want to emit this row.
				h.probingRowState.matched = false
				if h.hashi > 31 {
					h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
					if len(h.duplicate) > 0 {
						h.hashrows = <-h.duplicate
						h.hashi = 0
					} else {
						h.hashrows = make([]sqlbase.EncDatumRow, 40)
						h.hashi = 0
					}
				}
				h.probeRow1()
				h.hashflag = false
				return
			}
		} else if err := i.Mark(h.Ctx, true); err != nil {
			h.MoveToDraining(err)
			h.runningState = hjStateUnknown
			h.ProbeStructChan <- ProbeStruct{
				h.hashrows,
				h.DrainHelper(),
				h.hashi,
			}
			close(h.ProbeStructChan)
			h.hashflag = false
			return
		}
	}
	var nextState hashJoinerState
	if shouldShortCircuit(h.storedSide, h.joinType) {
		nextState = hjReadingProbeSide
	}
	if shouldEmit {
		if h.joinType == sqlbase.IntersectAllJoin {
			// We found a match, so we are done with this row.
			//var temp  = make([]sqlbase.EncDatum,len(renderedRow))
			//copy(temp,renderedRow)
			//h.hashrows[h.hashi] = temp
			h.hashi++
			if h.hashi > 31 {
				h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
				if len(h.duplicate) > 0 {
					h.hashrows = <-h.duplicate
					h.hashi = 0
				} else {
					h.hashrows = make([]sqlbase.EncDatumRow, 40)
					h.hashi = 0
				}
			}
			h.hashflag = false
			return
		}
		if nextState == hjReadingProbeSide {
			h.hashi++
			if h.hashi > 31 {
				h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
				if len(h.duplicate) > 0 {
					h.hashrows = <-h.duplicate
					h.hashi = 0
				} else {
					h.hashrows = make([]sqlbase.EncDatumRow, 40)
					h.hashi = 0
				}
			}
		} else {
			//var temp= make([]sqlbase.EncDatum, len(renderedRow))
			//copy(temp, renderedRow)
			//h.hashrows[h.hashi] = temp
			h.hashi++
			if h.hashi > 31 {
				h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
				if len(h.duplicate) > 0 {
					h.hashrows = <-h.duplicate
					h.hashi = 0
				} else {
					h.hashrows = make([]sqlbase.EncDatumRow, 40)
					h.hashi = 0
				}
			}
			h.hashflag = true
			return
		}
		h.hashflag = false
		return
	}
	if nextState == hjReadingProbeSide {
		if h.hashi > 31 {
			h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
			if len(h.duplicate) > 0 {
				h.hashrows = <-h.duplicate
				h.hashi = 0
			} else {
				h.hashrows = make([]sqlbase.EncDatumRow, 40)
				h.hashi = 0
			}
		}
	} else {
		if h.hashi > 31 {
			h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
			if len(h.duplicate) > 0 {
				h.hashrows = <-h.duplicate
				h.hashi = 0
			} else {
				h.hashrows = make([]sqlbase.EncDatumRow, 40)
				h.hashi = 0
			}
		}
		h.probeRow1()
	}
	h.hashflag = false
	return
}

func (h *hashJoiner) emitUnmatched() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*distsqlpb.ProducerMetadata,
) {
	i := h.emittingUnmatchedState.iter
	if ok, err := i.Valid(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	} else if !ok {
		// Done.
		h.MoveToDraining(nil /* err */)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	if err := h.cancelChecker.Check(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	row, err := i.Row()
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}
	defer i.Next()

	return hjEmittingUnmatched, h.renderUnmatchedRow(row, h.storedSide), nil
}

func (h *hashJoiner) emitUnmatched1() {
	i := h.emittingUnmatchedState.iter
	if ok, err := i.Valid(); err != nil {
		h.MoveToDraining(err)
		h.runningState = hjStateUnknown
		h.ProbeStructChan <- ProbeStruct{
			h.hashrows,
			h.DrainHelper(),
			h.hashi,
		}
		close(h.ProbeStructChan)
		return
	} else if !ok {
		// Done.
		h.MoveToDraining(nil /* err */)
		h.runningState = hjStateUnknown
		h.ProbeStructChan <- ProbeStruct{
			h.hashrows,
			h.DrainHelper(),
			h.hashi,
		}
		close(h.ProbeStructChan)
		return
	}

	if err := h.cancelChecker.Check(); err != nil {
		h.MoveToDraining(err)
		h.runningState = hjStateUnknown
		h.ProbeStructChan <- ProbeStruct{
			h.hashrows,
			h.DrainHelper(),
			h.hashi,
		}
		close(h.ProbeStructChan)
		return
	}

	row, err := i.Row()
	if err != nil {
		h.MoveToDraining(err)
		h.runningState = hjStateUnknown
		h.ProbeStructChan <- ProbeStruct{
			h.hashrows,
			h.DrainHelper(),
			h.hashi,
		}
		close(h.ProbeStructChan)
		return
	}
	defer i.Next()
	h.runningState = hjEmittingUnmatched
	h.renderUnmatchedRow1(row, h.storedSide, h.hashrows, uint32(h.hashi))
	h.hashi++
	if h.hashi > 31 {
		h.ProbeStructChan <- ProbeStruct{h.hashrows, nil, h.hashi}
		if len(h.duplicate) > 0 {
			h.hashrows = <-h.duplicate
			h.hashi = 0
		} else {
			h.hashrows = make([]sqlbase.EncDatumRow, 40)
			h.hashi = 0
		}
	}
	return
}

func (h *hashJoiner) close() {
	if h.InternalClose() {
		if h.InternalOpt {
			h.isClosed = true
			h.HashJoinOpt.ReleaseResource()
		}
		// We need to close only memRowContainer of the probe side because the
		// stored side container will be closed by closing h.storedRows.
		if h.storedSide == rightSide {
			h.rows[leftSide].Close(h.Ctx)
		} else {
			h.rows[rightSide].Close(h.Ctx)
		}
		if h.storedRows != nil {
			h.storedRows.Close(h.Ctx)
		} else {
			// h.storedRows has not been initialized, so we need to close the stored
			// side container explicitly.
			h.rows[h.storedSide].Close(h.Ctx)
		}
		if h.probingRowState.iter != nil {
			h.probingRowState.iter.Close()
		} else {
			if rc, ok := h.storedRows.(*rowcontainer.HashFileBackedRowContainer); ok {
				rc.MayBeCloseDumpKeyRow()
			}
		}

		if h.emittingUnmatchedState.iter != nil {
			h.emittingUnmatchedState.iter.Close()
		}
		if h.fMemMonitor != nil {
			h.fMemMonitor.Stop(h.Ctx)
		}
		h.MemMonitor.Stop(h.Ctx)
		if h.diskMonitor != nil {
			h.diskMonitor.Stop(h.Ctx)
		}
	}
}

// receiveNext reads from the source specified by side and returns the next row
// or metadata to be processed by the hashJoiner. Unless h.nullEquality is true,
// rows with NULLs in their equality columns are only returned if the joinType
// specifies that unmatched rows should be returned for the given side. In this
// case, a rendered row and true is returned, notifying the caller that the
// returned row may be emitted directly.
func (h *hashJoiner) receiveNext(
	side joinSide,
) (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata, bool, error) {
	source := h.leftSource
	if side == rightSide {
		source = h.rightSource
	}
	for {
		if err := h.cancelChecker.Check(); err != nil {
			return nil, nil, false, err
		}
		row, meta := source.Next()
		if meta != nil {
			return nil, meta, false, nil
		} else if row == nil {
			return nil, nil, false, nil
		}
		// We make the explicit check for whether or not the row contained a NULL value
		// on an equality column. The reasoning here is because of the way we expect
		// NULL equality checks to behave (i.e. NULL != NULL) and the fact that we
		// use the encoding of any given row as key into our bucket. Thus if we
		// encountered a NULL row when building the hashmap we have to store in
		// order to use it for RIGHT OUTER joins but if we encounter another
		// NULL row when going through the left stream (probing phase), matching
		// this with the first NULL row would be incorrect.
		//
		// If we have have the following:
		// CREATE TABLE t(x INT); INSERT INTO t(x) VALUES (NULL);
		//    |  x   |
		//     ------
		//    | NULL |
		//
		// For the following query:
		// SELECT * FROM t AS a FULL OUTER JOIN t AS b USING(x);
		//
		// We expect:
		//    |  x   |
		//     ------
		//    | NULL |
		//    | NULL |
		//
		// The following examples illustrates the behavior when joining on two
		// or more columns, and only one of them contains NULL.
		// If we have have the following:
		// CREATE TABLE t(x INT, y INT);
		// INSERT INTO t(x, y) VALUES (44,51), (NULL,52);
		//    |  x   |  y   |
		//     ------
		//    |  44  |  51  |
		//    | NULL |  52  |
		//
		// For the following query:
		// SELECT * FROM t AS a FULL OUTER JOIN t AS b USING(x, y);
		//
		// We expect:
		//    |  x   |  y   |
		//     ------
		//    |  44  |  51  |
		//    | NULL |  52  |
		//    | NULL |  52  |
		hasNull := false
		for _, c := range h.eqCols[side] {
			if row[c].IsNull() {
				hasNull = true
				break
			}
		}
		// row has no NULLs in its equality columns (or we are considering NULLs to
		// be equal), so it might match a row from the other side.
		if !hasNull || h.nullEquality {
			return row, nil, false, nil
		}

		if renderedRow, shouldEmit := h.shouldEmitUnmatched(row, side); shouldEmit {
			return renderedRow, nil, true, nil
		}

		// If this point is reached, row had NULLs in its equality columns but
		// should not be emitted. Throw it away and get the next row.
	}
}

// shouldEmitUnmatched returns whether this row should be emitted if it doesn't
// match. If this is the case, a rendered row ready for emitting is returned as
// well.
func (h *hashJoiner) shouldEmitUnmatched(
	row sqlbase.EncDatumRow, side joinSide,
) (sqlbase.EncDatumRow, bool) {
	if !shouldEmitUnmatchedRow(side, h.joinType) {
		return nil, false
	}
	return h.renderUnmatchedRow(row, side), true
}

// initStoredRows initializes a hashRowContainer and sets h.storedRows.
func (h *hashJoiner) initStoredRows() error {
	if h.useTempStorage {
		if TempFileStorageAble.Get(&h.FlowCtx.Cfg.Settings.SV) {
			var FileStore rowcontainer.Hstore
			tempName, err := ioutil.TempDir("", "hash_temp_file")
			if err != nil {
				return err
			}
			err = os.Remove(tempName)
			if err != nil {
				return err
			}
			FileStore.Init(32, 1, tempName[5:], h.FlowCtx.Cfg.TempPath+"/hstore", h.FlowCtx.Cfg.HsMgr)
			hrc := rowcontainer.MakeHashFileBackedRowContainer(
				&h.rows[h.storedSide],
				h.EvalCtx,
				h.MemMonitor,
				h.fMemMonitor,
				h.diskMonitor,
				h.FlowCtx.Cfg.TempStorage,
				&FileStore,
			)
			h.storedRows = &hrc
		} else {
			hrc := rowcontainer.MakeHashDiskBackedRowContainer(
				&h.rows[h.storedSide],
				h.EvalCtx,
				h.MemMonitor,
				h.diskMonitor,
				h.FlowCtx.Cfg.TempStorage,
				false,
			)
			h.storedRows = &hrc
		}
	} else {
		hrc := rowcontainer.MakeHashMemRowContainer(&h.rows[h.storedSide])
		h.storedRows = &hrc
	}
	return h.storedRows.Init(
		h.Ctx,
		shouldMark(h.storedSide, h.joinType),
		h.rows[h.storedSide].Types(),
		h.eqCols[h.storedSide],
		h.nullEquality,
	)
}

var _ distsqlpb.DistSQLSpanStats = &HashJoinerStats{}

const hashJoinerTagPrefix = "hashjoiner."

// Stats implements the SpanStats interface.
func (hjs *HashJoinerStats) Stats() map[string]string {
	// statsMap starts off as the left input stats map.
	statsMap := hjs.LeftInputStats.Stats(hashJoinerTagPrefix + "left.")
	rightInputStatsMap := hjs.RightInputStats.Stats(hashJoinerTagPrefix + "right.")
	// Merge the two input maps.
	for k, v := range rightInputStatsMap {
		statsMap[k] = v
	}
	statsMap[hashJoinerTagPrefix+"stored_side"] = hjs.StoredSide
	statsMap[hashJoinerTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(hjs.MaxAllocatedMem)
	statsMap[hashJoinerTagPrefix+MaxDiskTagSuffix] = humanizeutil.IBytes(hjs.MaxAllocatedDisk)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (hjs *HashJoinerStats) StatsForQueryPlan() []string {
	leftInputStats := hjs.LeftInputStats.StatsForQueryPlan("left ")
	leftInputStats = append(leftInputStats, hjs.RightInputStats.StatsForQueryPlan("right ")...)
	return append(
		leftInputStats,
		fmt.Sprintf("stored side: %s", hjs.StoredSide),
		fmt.Sprintf("%s: %t", "isParallel", hjs.IsParallel),
		fmt.Sprintf("%s: %d", "ParallelNum", hjs.ParallelNum),
		fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(hjs.MaxAllocatedMem)),
		fmt.Sprintf("%s: %s", MaxDiskQueryPlanSuffix, humanizeutil.IBytes(hjs.MaxAllocatedDisk)),
	)
}

// outputStatsToTrace outputs the collected hashJoiner stats to the trace. Will
// fail silently if the hashJoiner is not collecting stats.
func (h *hashJoiner) outputStatsToTrace() {
	lis, ok := getInputStats(h.FlowCtx, h.leftSource)
	if !ok {
		return
	}
	ris, ok := getInputStats(h.FlowCtx, h.rightSource)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(h.Ctx); sp != nil {
		if h.InternalOpt {
			tracing.SetSpanStats(
				sp,
				&HashJoinerStats{
					IsParallel:       h.InternalOpt,
					ParallelNum:      int32(h.parallel),
					LeftInputStats:   lis,
					RightInputStats:  ris,
					StoredSide:       h.storedSide.String(),
					MaxAllocatedMem:  h.MemMonitor.MaximumBytes(),
					MaxAllocatedDisk: h.diskMonitor.MaximumBytes(),
				},
			)
		} else {
			tracing.SetSpanStats(
				sp,
				&HashJoinerStats{
					LeftInputStats:   lis,
					RightInputStats:  ris,
					StoredSide:       h.storedSide.String(),
					MaxAllocatedMem:  h.MemMonitor.MaximumBytes(),
					MaxAllocatedDisk: h.diskMonitor.MaximumBytes(),
				},
			)
		}
	}
}

// Some types of joins need to mark rows that matched.
func shouldMark(storedSide joinSide, joinType sqlbase.JoinType) bool {
	switch {
	case joinType == sqlbase.LeftSemiJoin && storedSide == leftSide:
		return true
	case joinType == sqlbase.LeftAntiJoin && storedSide == leftSide:
		return true
	case joinType == sqlbase.ExceptAllJoin:
		return true
	case joinType == sqlbase.IntersectAllJoin:
		return true
	case shouldEmitUnmatchedRow(storedSide, joinType):
		return true
	default:
		return false
	}
}

// Some types of joins only need to know of the existence of a matching row in
// the storedSide, depending on the storedSide, and don't need to know all the
// rows. These can 'short circuit' to avoid iterating through them all.
func shouldShortCircuit(storedSide joinSide, joinType sqlbase.JoinType) bool {
	switch joinType {
	case sqlbase.LeftSemiJoin:
		return storedSide == rightSide
	case sqlbase.ExceptAllJoin:
		return true
	default:
		return false
	}
}
