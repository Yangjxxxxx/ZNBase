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
	"errors"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// mergeJoiner performs merge join, it has two input row sources with the same
// ordering on the columns that have equality constraints.
//
// It is guaranteed that the results preserve this ordering.
type mergeJoiner struct {
	joinerBase

	cancelChecker *sqlbase.CancelChecker

	leftSource, rightSource runbase.RowSource
	leftRows, rightRows     []sqlbase.EncDatumRow
	leftIdx, rightIdx       int
	emitUnmatchedRight      bool
	matchedRight            util.FastIntSet
	matchedRightCount       int

	streamMerger streamMerger

	result chan runbase.ResultRow

	rows runbase.ResultRow

	duplicate chan []sqlbase.EncDatumRow

	index int
}

var _ runbase.Processor = &mergeJoiner{}
var _ runbase.RowSource = &mergeJoiner{}

const mergeJoinerProcName = "merge joiner"
const blockLen = 32

func newMergeJoiner(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.MergeJoinerSpec,
	leftSource runbase.RowSource,
	rightSource runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (*mergeJoiner, error) {
	leftEqCols := make([]uint32, 0, len(spec.LeftOrdering.Columns))
	rightEqCols := make([]uint32, 0, len(spec.RightOrdering.Columns))
	for i, c := range spec.LeftOrdering.Columns {
		if spec.RightOrdering.Columns[i].Direction != c.Direction {
			return nil, errors.New("Unmatched column orderings")
		}
		leftEqCols = append(leftEqCols, c.ColIdx)
		rightEqCols = append(rightEqCols, spec.RightOrdering.Columns[i].ColIdx)
	}

	m := &mergeJoiner{
		leftSource:  leftSource,
		rightSource: rightSource,
	}
	if flowCtx.EvalCtx.SessionData.PC[sessiondata.TableReaderSet].Parallel {
		switch t1 := m.leftSource.(type) {
		case *tableReader:
			if t1.InternalOpt {
				t1.parallel = 1
			}
		}
		switch t2 := m.leftSource.(type) {
		case *tableReader:
			if t2.InternalOpt {
				t2.parallel = 1
			}
		}
	}
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		m.leftSource = NewInputStatCollector(m.leftSource)
		m.rightSource = NewInputStatCollector(m.rightSource)
		m.FinishTrace = m.outputStatsToTrace
	}

	if err := m.joinerBase.init(
		m /* self */, flowCtx, processorID, leftSource.OutputTypes(), rightSource.OutputTypes(),
		spec.Type, spec.OnExpr, leftEqCols, rightEqCols, 0, post, output,
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{leftSource, rightSource},
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				m.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	m.MemMonitor = runbase.NewMonitor(flowCtx.EvalCtx.Ctx(), flowCtx.EvalCtx.Mon, "mergejoiner-mem")

	var err error
	m.streamMerger, err = makeStreamMerger(
		m.leftSource,
		distsqlpb.ConvertToColumnOrdering(spec.LeftOrdering),
		m.rightSource,
		distsqlpb.ConvertToColumnOrdering(spec.RightOrdering),
		spec.NullEquality,
		m.MemMonitor,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

//GetBatch is part of the RowSource interface.
func (m *mergeJoiner) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (m *mergeJoiner) SetChan() {
	m.result = make(chan runbase.ResultRow, 50)
	m.duplicate = make(chan []sqlbase.EncDatumRow, 50)
	m.StartMerge()
}

// Start is part of the RowSource interface.
func (m *mergeJoiner) Start(ctx context.Context) context.Context {
	m.streamMerger.start(ctx)
	ctx = m.StartInternal(ctx, mergeJoinerProcName)
	m.cancelChecker = sqlbase.NewCancelChecker(ctx)
	return ctx
}

func (m *mergeJoiner) StartMerge() {
	if m.State == runbase.StateRunning {
		go m.nextRow1(m.result)
	}
}

// Next is part of the Processor interface.
func (m *mergeJoiner) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if runbase.OptJoin {
		for {
			if m.rows.Row != nil {
				for m.index < int(m.rows.Index) {
					outRow := m.ProcessRowHelper(m.rows.Row[m.index])
					m.index++
					if outRow != nil {
						return outRow, nil
					}
				}
				m.index = 0
				m.rows.Index = 0
				if len(m.duplicate) < 90 {
					m.duplicate <- m.rows.Row
				}
				m.rows.Row = nil
			} else {
				var ok bool
				m.rows, ok = <-m.result
				if ok {
					if m.rows.Mata != nil {
						if m.rows.Mata.Err != nil {
							m.MoveToDraining(nil /* err */)
						}
						return nil, m.rows.Mata
					}
					continue
				} else {
					if m.rows.Row == nil && !ok {
						m.MoveToDraining(nil /* err */)
						for len(m.duplicate) > 1 {
							_ = <-m.duplicate
						}
						_, ok := <-m.duplicate
						if ok {
							close(m.duplicate)
						}
						break
					}
				}
			}
		}
		return nil, m.DrainHelper()
	}

	for m.State == runbase.StateRunning {
		row, meta := m.nextRow()
		if meta != nil {
			if meta.Err != nil {
				m.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			m.MoveToDraining(nil /* err */)
			break
		}

		outRow := m.ProcessRowHelper(row)
		if outRow != nil {
			if m.EvalCtx != nil && m.EvalCtx.RecursiveData != nil && m.EvalCtx.RecursiveData.FormalJoin {
				rowLength := m.ProcessorBase.EvalCtx.RecursiveData.RowLength
				virtualColID := m.ProcessorBase.EvalCtx.RecursiveData.VirtualColID
				if len(outRow) == rowLength {
					m.ProcessorBase.IsCycle(outRow, virtualColID)
				}
			}
			return outRow, nil
		}
	}
	return nil, m.DrainHelper()
}

func (m *mergeJoiner) nextRow() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	// The loops below form a restartable state machine that iterates over a
	// batch of rows from the left and right side of the join. The state machine
	// returns a result for every row that should be output.

	for {
		for m.leftIdx < len(m.leftRows) {
			// We have unprocessed rows from the left-side batch.
			lrow := m.leftRows[m.leftIdx]
			for m.rightIdx < len(m.rightRows) {
				// We have unprocessed rows from the right-side batch.
				ridx := m.rightIdx
				m.rightIdx++
				renderedRow, err := m.render(lrow, m.rightRows[ridx])
				if err != nil {
					return nil, &distsqlpb.ProducerMetadata{Err: err}
				}
				if renderedRow != nil {
					m.matchedRightCount++
					if m.joinType == sqlbase.LeftAntiJoin || m.joinType == sqlbase.ExceptAllJoin {
						break
					}
					if m.emitUnmatchedRight {
						m.matchedRight.Add(ridx)
					}
					if m.joinType == sqlbase.LeftSemiJoin || m.joinType == sqlbase.IntersectAllJoin {
						// Semi-joins and INTERSECT ALL only need to know if there is at
						// least one match, so can skip the rest of the right rows.
						m.rightIdx = len(m.rightRows)
					}
					return renderedRow, nil
				}
			}

			// Perform the cancellation check. We don't perform this on every row,
			// but once for every iteration through the right-side batch.
			if err := m.cancelChecker.Check(); err != nil {
				return nil, &distsqlpb.ProducerMetadata{Err: err}
			}

			// We've exhausted the right-side batch. Adjust the indexes for the next
			// row from the left-side of the batch.
			m.leftIdx++
			m.rightIdx = 0

			// For INTERSECT ALL and EXCEPT ALL, adjust rightIdx to skip all
			// previously matched rows on the next right-side iteration, since we
			// don't want to match them again.
			if m.joinType.IsSetOpJoin() {
				m.rightIdx = m.leftIdx
			}

			// If we didn't match any rows on the right-side of the batch and this is
			// a left outer join, full outer join, anti join, or EXCEPT ALL, emit an
			// unmatched left-side row.
			if m.matchedRightCount == 0 && shouldEmitUnmatchedRow(leftSide, m.joinType) {
				return m.renderUnmatchedRow(lrow, leftSide), nil
			}

			m.matchedRightCount = 0
		}

		// We've exhausted the left-side batch. If this is a right or full outer
		// join (and thus matchedRight!=nil), emit unmatched right-side rows.
		if m.emitUnmatchedRight {
			for m.rightIdx < len(m.rightRows) {
				ridx := m.rightIdx
				m.rightIdx++
				if m.matchedRight.Contains(ridx) {
					continue
				}
				return m.renderUnmatchedRow(m.rightRows[ridx], rightSide), nil
			}

			m.matchedRight = util.FastIntSet{}
			m.emitUnmatchedRight = false
		}

		// Retrieve the next batch of rows to process.
		var meta *distsqlpb.ProducerMetadata
		// TODO(paul): Investigate (with benchmarks) whether or not it's
		// worthwhile to only buffer one row from the right stream per batch
		// for semi-joins.
		m.leftRows, m.rightRows, meta = m.streamMerger.NextBatch(m.Ctx, m.EvalCtx)
		if meta != nil {
			return nil, meta
		}
		if m.leftRows == nil && m.rightRows == nil {
			return nil, nil
		}

		// Prepare for processing the next batch.
		m.emitUnmatchedRight = shouldEmitUnmatchedRow(rightSide, m.joinType)
		m.leftIdx, m.rightIdx = 0, 0
	}
}

func (m *mergeJoiner) nextRow1(res chan runbase.ResultRow) {
	// The loops below form a restartable state machine that iterates over a
	// batch of rows from the left and right side of the join. The state machine
	// returns a result for every row that should be output.
	var rows = make([]sqlbase.EncDatumRow, blockLen)
	var i uint32
	for {
		for m.leftIdx < len(m.leftRows) {
			// We have unprocessed rows from the left-side batch.
			lrow := m.leftRows[m.leftIdx]
			for m.rightIdx < len(m.rightRows) {
				// We have unprocessed rows from the right-side batch.
				ridx := m.rightIdx
				m.rightIdx++
				err := m.render1(lrow, m.rightRows[ridx], rows, i)
				if err != nil {
					res <- runbase.ResultRow{
						Row:   nil,
						Mata:  &distsqlpb.ProducerMetadata{Err: err},
						Index: 0,
					}
					close(res)
					return
				}
				if rows[i] != nil {
					m.matchedRightCount++
					i++
					if m.joinType == sqlbase.LeftAntiJoin || m.joinType == sqlbase.ExceptAllJoin {
						i--
						break
					}
					if m.emitUnmatchedRight {
						m.matchedRight.Add(ridx)
					}
					if m.joinType == sqlbase.LeftSemiJoin || m.joinType == sqlbase.IntersectAllJoin {
						// Semi-joins and INTERSECT ALL only need to know if there is at
						// least one match, so can skip the rest of the right rows.
						m.rightIdx = len(m.rightRows)
					}
					if i > blockLen-1 {
						res <- runbase.ResultRow{
							Row:   rows,
							Mata:  nil,
							Index: i,
						}
						if len(m.duplicate) > 0 {
							rows = <-m.duplicate
							i = 0
							continue
						} else {
							rows = make([]sqlbase.EncDatumRow, 30)
							i = 0
							continue
						}
					}
					continue
				}
			}

			// Perform the cancellation check. We don't perform this on every row,
			// but once for every iteration through the right-side batch.
			if err := m.cancelChecker.Check(); err != nil {
				res <- runbase.ResultRow{
					Row:   nil,
					Mata:  &distsqlpb.ProducerMetadata{Err: err},
					Index: 0,
				}
				close(res)
				return
			}

			// We've exhausted the right-side batch. Adjust the indexes for the next
			// row from the left-side of the batch.
			m.leftIdx++
			m.rightIdx = 0

			// For INTERSECT ALL and EXCEPT ALL, adjust rightIdx to skip all
			// previously matched rows on the next right-side iteration, since we
			// don't want to match them again.
			if m.joinType.IsSetOpJoin() {
				m.rightIdx = m.leftIdx
			}

			// If we didn't match any rows on the right-side of the batch and this is
			// a left outer join, full outer join, anti join, or EXCEPT ALL, emit an
			// unmatched left-side row.
			if m.matchedRightCount == 0 && shouldEmitUnmatchedRow(leftSide, m.joinType) {
				m.renderUnmatchedRow1(lrow, leftSide, rows, i)
				i++
				if i > blockLen-1 {
					res <- runbase.ResultRow{
						Row:   rows,
						Mata:  nil,
						Index: i,
					}
					if len(m.duplicate) > 0 {
						rows = <-m.duplicate
						i = 0
						continue
					} else {
						rows = make([]sqlbase.EncDatumRow, 30)
						i = 0
						continue
					}
				}
				continue
			}

			m.matchedRightCount = 0
		}

		// We've exhausted the left-side batch. If this is a right or full outer
		// join (and thus matchedRight!=nil), emit unmatched right-side rows.
		if m.emitUnmatchedRight {
			for m.rightIdx < len(m.rightRows) {
				ridx := m.rightIdx
				m.rightIdx++
				if m.matchedRight.Contains(ridx) {
					continue
				}
				m.renderUnmatchedRow1(m.rightRows[ridx], rightSide, rows, i)
				i++
				if i > blockLen-1 {
					res <- runbase.ResultRow{
						Row:   rows,
						Mata:  nil,
						Index: i,
					}
					if len(m.duplicate) > 0 {
						rows = <-m.duplicate
						i = 0
						continue
					} else {
						rows = make([]sqlbase.EncDatumRow, 30)
						i = 0
						continue
					}
				}
				continue

			}

			m.matchedRight = util.FastIntSet{}
			m.emitUnmatchedRight = false
		}

		// Retrieve the next batch of rows to process.
		var meta *distsqlpb.ProducerMetadata
		// TODO(paul): Investigate (with benchmarks) whether or not it's
		// worthwhile to only buffer one row from the right stream per batch
		// for semi-joins.
		m.leftRows, m.rightRows, meta = m.streamMerger.NextBatch(m.Ctx, m.EvalCtx)
		if meta != nil {
			res <- runbase.ResultRow{
				Row:   rows,
				Mata:  meta,
				Index: i,
			}
			close(res)
			return
		}
		if m.leftRows == nil && m.rightRows == nil {
			res <- runbase.ResultRow{
				Row:   rows,
				Mata:  nil,
				Index: i,
			}
			close(res)
			return
		}

		// Prepare for processing the next batch.
		m.emitUnmatchedRight = shouldEmitUnmatchedRow(rightSide, m.joinType)
		m.leftIdx, m.rightIdx = 0, 0
	}
}

func (m *mergeJoiner) close() {
	if m.InternalClose() {
		ctx := m.Ctx
		m.streamMerger.close(ctx)
		m.MemMonitor.Stop(ctx)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (m *mergeJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	m.close()
}

var _ distsqlpb.DistSQLSpanStats = &MergeJoinerStats{}

const mergeJoinerTagPrefix = "mergejoiner."

// Stats implements the SpanStats interface.
func (mjs *MergeJoinerStats) Stats() map[string]string {
	// statsMap starts off as the left input stats map.
	statsMap := mjs.LeftInputStats.Stats(mergeJoinerTagPrefix + "left.")
	rightInputStatsMap := mjs.RightInputStats.Stats(mergeJoinerTagPrefix + "right.")
	// Merge the two input maps.
	for k, v := range rightInputStatsMap {
		statsMap[k] = v
	}
	statsMap[mergeJoinerTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(mjs.MaxAllocatedMem)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (mjs *MergeJoinerStats) StatsForQueryPlan() []string {
	stats := append(
		mjs.LeftInputStats.StatsForQueryPlan("left "),
		mjs.RightInputStats.StatsForQueryPlan("right ")...,
	)
	return append(stats, fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(mjs.MaxAllocatedMem)))
}

// outputStatsToTrace outputs the collected mergeJoiner stats to the trace. Will
// fail silently if the mergeJoiner is not collecting stats.
func (m *mergeJoiner) outputStatsToTrace() {
	lis, ok := getInputStats(m.FlowCtx, m.leftSource)
	if !ok {
		return
	}
	ris, ok := getInputStats(m.FlowCtx, m.rightSource)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(m.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&MergeJoinerStats{
				LeftInputStats:  lis,
				RightInputStats: ris,
				MaxAllocatedMem: m.MemMonitor.MaximumBytes(),
			},
		)
	}
}
