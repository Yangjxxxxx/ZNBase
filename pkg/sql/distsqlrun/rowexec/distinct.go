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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/stringarena"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// Distinct is the physical processor implementation of the DISTINCT relational operator.
type Distinct struct {
	runbase.ProcessorBase

	input            runbase.RowSource
	types            []sqlbase.ColumnType
	haveLastGroupKey bool
	lastGroupKey     sqlbase.EncDatumRow
	arena            stringarena.Arena
	seen             map[string]struct{}
	orderedCols      []uint32
	distinctCols     util.FastIntSet
	memAcc           mon.BoundAccount
	datumAlloc       sqlbase.DatumAlloc
	scratch          []byte
	scratches        [][]byte

	//resultRows chan runbase.ResultRow
	//rows       runbase.ResultRow
	//index      int

	duplicate chan sqlbase.EncDatumRow

	resRows0         chan sqlbase.EncDatumRow
	resRows1         chan sqlbase.EncDatumRow
	resRows2         chan sqlbase.EncDatumRow
	resRows3         chan sqlbase.EncDatumRow
	resRows4         chan sqlbase.EncDatumRow
	distinctCount    int
	tableReaderCount int
	tr               *tableReader
	parallel         bool
	matched          []bool
	outmatch         bool
	seens            [5]map[string]struct{}
	datumAllocs      []sqlbase.DatumAlloc
	arenas           []stringarena.Arena
	begin            bool
	first            bool
	beginChan        chan int
	end              bool
}

// SortedDistinct is a specialized distinct that can be used when all of the
// distinct columns are also ordered.
type SortedDistinct struct {
	Distinct
}

var _ runbase.Processor = &Distinct{}
var _ runbase.RowSource = &Distinct{}

const distinctProcName = "distinct"

var _ runbase.Processor = &SortedDistinct{}
var _ runbase.RowSource = &SortedDistinct{}

const sortedDistinctProcName = "sorted distinct"

// NewDistinct instantiates a new Distinct processor.
func NewDistinct(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.DistinctSpec,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (runbase.RowSourcedProcessor, error) {
	if len(spec.DistinctColumns) == 0 {
		return nil, pgerror.NewAssertionErrorf("0 distinct columns specified for distinct processor")
	}

	var distinctCols, orderedCols util.FastIntSet
	allSorted := true

	for _, col := range spec.OrderedColumns {
		orderedCols.Add(int(col))
	}
	for _, col := range spec.DistinctColumns {
		if !orderedCols.Contains(int(col)) {
			allSorted = false
		}
		distinctCols.Add(int(col))
	}
	if !orderedCols.SubsetOf(distinctCols) {
		return nil, pgerror.NewAssertionErrorf("ordered cols must be a subset of distinct cols")
	}

	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "distinct-mem")
	d := &Distinct{
		input:        input,
		orderedCols:  spec.OrderedColumns,
		distinctCols: distinctCols,
		memAcc:       memMonitor.MakeBoundAccount(),
		types:        input.OutputTypes(),
	}

	var returnProcessor runbase.RowSourcedProcessor = d
	if allSorted {
		// We can use the faster sortedDistinct processor.
		sd := &SortedDistinct{
			Distinct: *d,
		}
		// Set d to the new distinct copy for further initialization.
		// We should have a distinctBase, rather than making a copy
		// of a distinct processor.
		d = &sd.Distinct
		returnProcessor = sd
		if err := sd.Init(
			sd, post, sd.types, flowCtx, processorID, output, memMonitor, /* memMonitor */
			runbase.ProcStateOpts{
				InputsToDrain: []runbase.RowSource{sd.input},
				TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
					sd.close()
					return nil
				},
			}); err != nil {
			return nil, err
		}
		sd.lastGroupKey = sd.Out.RowAlloc.AllocRow(len(sd.types))
		sd.haveLastGroupKey = false

		if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
			sd.input = NewInputStatCollector(sd.input)
			sd.FinishTrace = sd.outputStatsToTrace
		}
	} else {
		tr, ok := d.input.(*tableReader)
		if ok && runbase.OptJoin {
			d.initParallel(tr)
		}
		if err := d.Init(
			d, post, d.types, flowCtx, processorID, output, memMonitor, /* memMonitor */
			runbase.ProcStateOpts{
				InputsToDrain: []runbase.RowSource{d.input},
				TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
					d.close()
					return nil
				},
			}); err != nil {
			return nil, err
		}
		d.lastGroupKey = d.Out.RowAlloc.AllocRow(len(d.types))
		d.haveLastGroupKey = false

		if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
			d.input = NewInputStatCollector(d.input)
			d.FinishTrace = d.outputStatsToTrace
		}
	}

	return returnProcessor, nil
}

// Start is part of the RowSource interface.
func (d *Distinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.StartInternal(ctx, distinctProcName)
}

//GetBatch is part of the RowSource interface.
func (d *Distinct) GetBatch(ctx context.Context, respons chan roachpb.BlockStruct) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (d *Distinct) SetChan() {
	if d.parallel {
		var blockStructs = make([]runbase.BlockStruct, 5)
		blockStructs[0].Index = 0
		blockStructs[1].Index = 1
		blockStructs[2].Index = 2
		blockStructs[3].Index = 3
		blockStructs[4].Index = 4
		switch fr := d.tr.input.(type) {
		case *rowFetcherWrapper:
			blockStructs[0].Rf = &fr.Fetchers[0]
			blockStructs[1].Rf = &fr.Fetchers[1]
			blockStructs[2].Rf = &fr.Fetchers[2]
			blockStructs[3].Rf = &fr.Fetchers[3]
			blockStructs[4].Rf = &fr.Fetchers[4]
		}
		go d.startDistinct(&blockStructs[0])
		go d.startDistinct(&blockStructs[1])
		go d.startDistinct(&blockStructs[2])
		go d.startDistinct(&blockStructs[3])
		go d.startDistinct(&blockStructs[4])
	}
}

// Start is part of the RowSource interface.
func (d *SortedDistinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.StartInternal(ctx, sortedDistinctProcName)
}

//GetBatch is part of the RowSource interface.
func (d *SortedDistinct) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (d *SortedDistinct) SetChan() {}

func (d *Distinct) matchLastGroupKey(row sqlbase.EncDatumRow) (bool, error) {
	if !d.haveLastGroupKey {
		return false, nil
	}
	for _, colIdx := range d.orderedCols {
		res, err := d.lastGroupKey[colIdx].Compare(
			&d.types[colIdx], &d.datumAlloc, d.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// encode appends the encoding of non-ordered columns, which we use as a key in
// our 'seen' set.
func (d *Distinct) encode(appendTo []byte, row sqlbase.EncDatumRow) ([]byte, error) {
	var err error
	for i, datum := range row {
		// Ignore columns that are not in the distinctCols, as if we are
		// post-processing to strip out column Y, we cannot include it as
		// (X1, Y1) and (X1, Y2) will appear as distinct rows, but if we are
		// stripping out Y, we do not want (X1) and (X1) to be in the results.
		if !d.distinctCols.Contains(i) {
			continue
		}

		// TODO(irfansharif): Different rows may come with different encodings,
		// e.g. if they come from different streams that were merged, in which
		// case the encodings don't match (despite having the same underlying
		// datums). We instead opt to always choose sqlbase.DatumEncoding_ASCENDING_KEY
		// but we may want to check the first row for what encodings are already
		// available.
		appendTo, err = datum.Encode(&d.types[i], &d.datumAlloc, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}

func (d *Distinct) encode1(appendTo []byte, row sqlbase.EncDatumRow, index int) ([]byte, error) {
	var err error
	for i, datum := range row {
		// Ignore columns that are not in the distinctCols, as if we are
		// post-processing to strip out column Y, we cannot include it as
		// (X1, Y1) and (X1, Y2) will appear as distinct rows, but if we are
		// stripping out Y, we do not want (X1) and (X1) to be in the results.
		if !d.distinctCols.Contains(i) {
			continue
		}

		// TODO(irfansharif): Different rows may come with different encodings,
		// e.g. if they come from different streams that were merged, in which
		// case the encodings don't match (despite having the same underlying
		// datums). We instead opt to always choose sqlbase.DatumEncoding_ASCENDING_KEY
		// but we may want to check the first row for what encodings are already
		// available.
		appendTo, err = datum.Encode(&d.types[i], &d.datumAllocs[index], sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}

func (d *Distinct) close() {
	if d.InternalClose() {
		d.memAcc.Close(d.Ctx)
		d.MemMonitor.Stop(d.Ctx)
	}
}

// Next is part of the RowSource interface.
func (d *Distinct) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if d.parallel {
		for {
			if !d.outmatch {
				d.outmatch = true
				if err := d.arena.UnsafeReset(d.Ctx); err != nil {
					d.MoveToDraining(err)
					break
				}
				d.seen = make(map[string]struct{})
			}
			if !d.first {
				_ = <-d.beginChan
				d.first = true
			}
			if len(d.resRows0) > 0 || len(d.resRows1) > 0 || len(d.resRows2) > 0 || len(d.resRows3) > 0 || len(d.resRows4) > 0 {
				select {
				case row := <-d.resRows0:
					if row != nil {
						encoding, err := d.encode(d.scratch, row)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.scratch = encoding[:0]
						if _, ok := d.seen[string(encoding)]; ok {
							continue
						}
						s, err := d.arena.AllocBytes(d.Ctx, encoding)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.seen[s] = struct{}{}

						if outRow := d.ProcessRowHelper(row); outRow != nil {
							if len(d.duplicate) < 99 {
								d.duplicate <- row
							}
							return outRow, nil
						}
					} else {
						continue
					}
				case row := <-d.resRows1:
					if row != nil {
						encoding, err := d.encode(d.scratch, row)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.scratch = encoding[:0]
						if _, ok := d.seen[string(encoding)]; ok {
							continue
						}
						s, err := d.arena.AllocBytes(d.Ctx, encoding)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.seen[s] = struct{}{}

						if outRow := d.ProcessRowHelper(row); outRow != nil {
							if len(d.duplicate) < 99 {
								d.duplicate <- row
							}
							return outRow, nil
						}
					} else {
						continue
					}
				case row := <-d.resRows2:
					if row != nil {
						encoding, err := d.encode(d.scratch, row)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.scratch = encoding[:0]
						if _, ok := d.seen[string(encoding)]; ok {
							continue
						}
						s, err := d.arena.AllocBytes(d.Ctx, encoding)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.seen[s] = struct{}{}

						if outRow := d.ProcessRowHelper(row); outRow != nil {
							if len(d.duplicate) < 99 {
								d.duplicate <- row
							}
							return outRow, nil
						}
					} else {
						continue
					}
				case row := <-d.resRows3:
					if row != nil {
						encoding, err := d.encode(d.scratch, row)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.scratch = encoding[:0]
						if _, ok := d.seen[string(encoding)]; ok {
							continue
						}
						s, err := d.arena.AllocBytes(d.Ctx, encoding)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.seen[s] = struct{}{}

						if outRow := d.ProcessRowHelper(row); outRow != nil {
							if len(d.duplicate) < 99 {
								d.duplicate <- row
							}
							return outRow, nil
						}
					} else {
						continue
					}
				case row := <-d.resRows4:
					if row != nil {
						encoding, err := d.encode(d.scratch, row)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.scratch = encoding[:0]
						if _, ok := d.seen[string(encoding)]; ok {
							continue
						}
						s, err := d.arena.AllocBytes(d.Ctx, encoding)
						if err != nil {
							d.MoveToDraining(err)
							break
						}
						d.seen[s] = struct{}{}

						if outRow := d.ProcessRowHelper(row); outRow != nil {
							if len(d.duplicate) < 99 {
								d.duplicate <- row
							}
							return outRow, nil
						}
					} else {
						continue
					}
				}
			} else if d.end {
				d.MoveToDraining(nil)

				break
			}

			time.Sleep(100)
		}
		return nil, d.DrainHelper()
	}

	for d.State == runbase.StateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			d.MoveToDraining(nil /* err */)
			break
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		encoding, err := d.encode(d.scratch, row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		d.scratch = encoding[:0]

		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}

		if !matched {
			// Since the sorted distinct columns have changed, we know that all the
			// distinct keys in the 'seen' set will never be seen again. This allows
			// us to keep the current arena block and overwrite strings previously
			// allocated on it, which implies that UnsafeReset() is safe to call here.
			copy(d.lastGroupKey, row)
			d.haveLastGroupKey = true
			if err := d.arena.UnsafeReset(d.Ctx); err != nil {
				d.MoveToDraining(err)
				break
			}
			d.seen = make(map[string]struct{})
		}

		if _, ok := d.seen[string(encoding)]; ok {
			continue
		}
		s, err := d.arena.AllocBytes(d.Ctx, encoding)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		d.seen[s] = struct{}{}

		if outRow := d.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.DrainHelper()
}

func (d *Distinct) startDistinct(blockstruct *runbase.BlockStruct) {
	for d.State == runbase.StateRunning {

		row, meta := d.NextResult(blockstruct)
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			d.closeChan(blockstruct)
			d.distinctCount--
			if d.distinctCount == 0 {
				d.end = true
			}
			return
		}
		if row == nil {
			d.distinctCount--
			if d.distinctCount == 0 {
				d.end = true
			}
			d.closeChan(blockstruct)
			return
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		encoding, err := d.encode1(d.scratches[blockstruct.Index], row, blockstruct.Index)
		if err != nil {
			d.MoveToDraining(err)
			d.closeChan(blockstruct)
			return
		}
		d.scratches[blockstruct.Index] = encoding[:0]

		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.

		if !d.matched[blockstruct.Index] {
			// Since the sorted distinct columns have changed, we know that all the
			// distinct keys in the 'seen' set will never be seen again. This allows
			// us to keep the current arena block and overwrite strings previously
			// allocated on it, which implies that UnsafeReset() is safe to call here.
			d.matched[blockstruct.Index] = true
			if err := d.arenas[blockstruct.Index].UnsafeReset(d.Ctx); err != nil {
				d.MoveToDraining(err)
				d.closeChan(blockstruct)
				break
			}
			d.seens[blockstruct.Index] = make(map[string]struct{})
		}

		if _, ok := d.seens[blockstruct.Index][string(encoding)]; ok {
			continue
		}
		s, err := d.arenas[blockstruct.Index].AllocBytes(d.Ctx, encoding)
		if err != nil {
			d.MoveToDraining(err)
			d.closeChan(blockstruct)
			break
		}
		d.seens[blockstruct.Index][s] = struct{}{}
		d.sendRow(blockstruct.Index, row)
	}
}

// Next is part of the RowSource interface.
//
// sortedDistinct is simpler than distinct. All it has to do is keep track
// of the last row it saw, emitting if the new row is different.
func (d *SortedDistinct) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for d.State == runbase.StateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				d.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			d.MoveToDraining(nil /* err */)
			break
		}
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.MoveToDraining(err)
			break
		}
		if matched {
			continue
		}

		d.haveLastGroupKey = true
		copy(d.lastGroupKey, row)

		if outRow := d.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (d *Distinct) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}

var _ distsqlpb.DistSQLSpanStats = &DistinctStats{}

const distinctTagPrefix = "distinct."

// Stats implements the SpanStats interface.
func (ds *DistinctStats) Stats() map[string]string {
	inputStatsMap := ds.InputStats.Stats(distinctTagPrefix)
	inputStatsMap[distinctTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(ds.MaxAllocatedMem)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ds *DistinctStats) StatsForQueryPlan() []string {
	return append(
		ds.InputStats.StatsForQueryPlan(""),
		fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(ds.MaxAllocatedMem)),
	)
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (d *Distinct) outputStatsToTrace() {
	is, ok := getInputStats(d.FlowCtx, d.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(d.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp, &DistinctStats{InputStats: is, MaxAllocatedMem: d.MemMonitor.MaximumBytes()},
		)
	}
}

//NextResult implents interface BatchReader
func (d *Distinct) NextResult(
	BlockStruct *runbase.BlockStruct,
) (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	var meta *distsqlpb.ProducerMetadata
	for d.tr.State == runbase.StateRunning {
		rowTr, _, _, err := d.NextRow(d.Ctx, BlockStruct)
		if err != nil {
			meta = &distsqlpb.ProducerMetadata{Err: err}
		} else {
			meta = nil
		}
		if meta != nil {
			if meta.Err != nil {
				d.tr.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if rowTr == nil {
			d.tableReaderCount--
			if d.tableReaderCount != 0 {
				return nil, nil
			}
			d.tr.MoveToDraining(nil /* err */)
			break
		}
		if outRow := d.tr.OptProcessRowHelper(rowTr, BlockStruct.Index); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.tr.DrainHelper()
}

//NextRow implents interface BatchReader
func (d *Distinct) NextRow(
	ctx context.Context, BlockStruct *runbase.BlockStruct,
) (
	row sqlbase.EncDatumRow,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	err error,
) {

	for {
		rowDone, err := d.NextKey(ctx, BlockStruct)
		if err != nil {
			return nil, nil, nil, err
		}

		if BlockStruct.Rf.GetKvEnd() {
			return nil, nil, nil, nil
		}

		prettyKey, prettyVal, err := BlockStruct.Rf.ProcessKV(ctx, BlockStruct.Rf.Kv)
		if err != nil {
			return nil, nil, nil, err
		}
		if BlockStruct.Rf.GetTraceKV() {
			log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
		}
		if BlockStruct.Rf.GetIsCheck() {
			BlockStruct.Rf.GetRowReadyTable().SetlastKV(BlockStruct.Rf.Kv)
		}

		if rowDone {
			err := BlockStruct.Rf.FinalizeRow()
			return BlockStruct.Rf.GetRowReadyTable().Getrow(), BlockStruct.Rf.GetRowReadyTable().Getdesc().TableDesc(), BlockStruct.Rf.GetRowReadyTable().Getindex(), err
		}
	}
}

//NextKey implents interface BatchReader
func (d *Distinct) NextKey(
	ctx context.Context, BlockRespon *runbase.BlockStruct,
) (rowDone bool, err error) {
	var first bool
	var ok bool
	for {
		first, ok, BlockRespon.Rf.Kv, err = d.NextKV(BlockRespon)

		if err != nil {
			return false, err
		}
		BlockRespon.Rf.SetKvEnd(!ok)
		if BlockRespon.Rf.GetKvEnd() {
			//Rf.rowReadyTable = Rf.currentTable
			return true, nil
		}

		// See Init() for a detailed description of when we can get away with not
		// reading the index key.
		if BlockRespon.Rf.GetmustDecodeIndexKey() || BlockRespon.Rf.GetTraceKV() {
			keyRemainingBytes, ok, err := BlockRespon.Rf.ReadIndexKey(BlockRespon.Rf.Kv.Key)
			BlockRespon.Rf.SetKeyRemainingBytes(keyRemainingBytes)
			if err != nil {
				return false, err
			}
			if !ok {
				// The key did not match any of the table
				// descriptors, which means it's interleaved
				// data from some other table or index.
				continue
			}
		} else {
			// We still need to consume the key until the family
			// id, so processKV can know whether we've finished a
			// row or not.
			prefixLen, err := keys.GetRowPrefixLength(BlockRespon.Rf.Kv.Key)
			if err != nil {
				return false, err
			}
			BlockRespon.Rf.SetKeyRemainingBytes(BlockRespon.Rf.Kv.Key[prefixLen:])
		}

		if first && BlockRespon.Rf.GetRowReadyTable().Getdesc().TableDesc().ParentID == 50 {
			BlockRespon.Rf.SetIndexKey(nil)
			return true, nil
		}

		// For unique secondary indexes, the index-key does not distinguish one row
		// from the next if both rows contain identical values along with a NULL.
		// Consider the keys:
		//
		//   /test/unique_idx/NULL/0
		//   /test/unique_idx/NULL/1
		//
		// The index-key extracted from the above keys is /test/unique_idx/NULL. The
		// trailing /0 and /1 are the primary key used to unique-ify the keys when a
		// NULL is present. Currently we don't detect NULLs on decoding. If we did
		// we could detect this case and enlarge the index-key. A simpler fix for
		// this problem is to simply always output a row for each key scanned from a
		// secondary index as secondary indexes have only one key per row.
		// If Rf.rowReadyTable differs from Rf.currentTable, this denotes
		// a row is ready for output.
		switch {
		case first && BlockRespon.Rf.GetRowReadyTable().Getdesc().TableDesc().Name != "jobs":
			return true, nil
		case !first && BlockRespon.Rf.GetRowReadyTable().Getdesc().TableDesc().Name == "jobs":
			return true, nil
		case BlockRespon.Rf.GetCurrentTable().GetisSecondaryIndex():
			// Secondary indexes have only one key per row.
			rowDone = true
		case !bytes.HasPrefix(BlockRespon.Rf.Kv.Key, BlockRespon.Rf.GetIndexKey()):
			// If the prefix of the key has changed, current key is from a different
			// row than the previous one.
			rowDone = true
		case BlockRespon.Rf.GetRowReadyTable() != BlockRespon.Rf.GetCurrentTable():
			// For rowFetchers with more than one table, if the table changes the row
			// is done.
			rowDone = true
		default:
			rowDone = false
		}

		if BlockRespon.Rf.GetIndexKey() != nil && rowDone {
			// The current key belongs to a new row. Output the
			// current row.
			BlockRespon.Rf.SetIndexKey(nil)
			return true, nil
		}

		return false, nil
	}
}

//NextKV implents interface BatchReader
func (d *Distinct) NextKV(
	BlockRespon *runbase.BlockStruct,
) (first bool, ok bool, kv roachpb.KeyValue, err error) {
	for {
		if len(BlockRespon.BlockKv.Kvs) != 0 {
			kv = BlockRespon.BlockKv.Kvs[0]
			BlockRespon.BlockKv.Kvs = BlockRespon.BlockKv.Kvs[1:]
			return false, true, kv, nil
		}
		if len(BlockRespon.BlockKv.BatchResp) > 0 {
			var key []byte
			var rawBytes []byte
			var err error
			key, rawBytes, BlockRespon.BlockKv.BatchResp, err = enginepb.ScanDecodeKeyValueNoTS(BlockRespon.BlockKv.BatchResp)
			if err != nil {
				return false, false, kv, err
			}
			return false, true, roachpb.KeyValue{
				Key: key,
				Value: roachpb.Value{
					RawBytes: rawBytes,
				},
			}, nil
		}

		BlockRespon.BlockKv, ok = <-BlockRespon.Rf.BlockBatch

		if ok {
			if len(BlockRespon.BlockKv.Kvs) != 0 {
				kv = BlockRespon.BlockKv.Kvs[0]
				BlockRespon.BlockKv.Kvs = BlockRespon.BlockKv.Kvs[1:]
				return true, true, kv, nil
			}
			if len(BlockRespon.BlockKv.BatchResp) > 0 {
				var key []byte
				var rawBytes []byte
				var err error
				key, rawBytes, BlockRespon.BlockKv.BatchResp, err = enginepb.ScanDecodeKeyValueNoTS(BlockRespon.BlockKv.BatchResp)
				if err != nil {
					return false, false, kv, err
				}
				BlockRespon.First++
				if BlockRespon.First == 1 {
					return true, true, roachpb.KeyValue{
						Key: key,
						Value: roachpb.Value{
							RawBytes: rawBytes,
						},
					}, nil
				}
				return false, true, roachpb.KeyValue{
					Key: key,
					Value: roachpb.Value{
						RawBytes: rawBytes,
					},
				}, nil
			}
		}

		if (BlockRespon.BlockKv.BatchResp == nil && BlockRespon.BlockKv.Kvs == nil) && !ok {
			return false, false, kv, nil
		}
	}
}

func (d *Distinct) closeChan(blockstruct *runbase.BlockStruct) {
	if blockstruct.Index == 0 {
		close(d.resRows0)
	} else if blockstruct.Index == 1 {
		close(d.resRows1)
	} else if blockstruct.Index == 2 {
		close(d.resRows2)
	} else if blockstruct.Index == 3 {
		close(d.resRows3)
	} else {
		close(d.resRows4)
	}
}

func (d *Distinct) sendRow(index int, row sqlbase.EncDatumRow) {
	var cprow sqlbase.EncDatumRow
	if len(d.duplicate) > 0 {
		cprow = <-d.duplicate
	} else {
		cprow = make(sqlbase.EncDatumRow, len(row))
	}
	copy(cprow, row)
	if index == 0 {
		d.resRows0 <- cprow
		if !d.begin {
			d.beginChan <- 1
			d.begin = true
		}
	} else if index == 1 {
		d.resRows1 <- cprow
		if !d.begin {
			d.beginChan <- 1
			d.begin = true
		}
	} else if index == 2 {
		d.resRows2 <- cprow
		if !d.begin {
			d.beginChan <- 1
			d.begin = true
		}
	} else if index == 3 {
		d.resRows3 <- cprow
		if !d.begin {
			d.beginChan <- 1
			d.begin = true
		}
	} else {
		d.resRows4 <- cprow
		if !d.begin {
			d.beginChan <- 1
			d.begin = true
		}
	}
}

func (d *Distinct) initParallel(tr *tableReader) {
	d.matched = make([]bool, 5)
	d.scratches = make([][]byte, 5)
	d.parallel = true
	d.arenas = make([]stringarena.Arena, 5)
	d.datumAllocs = make([]sqlbase.DatumAlloc, 5)
	d.distinctCount = 5
	d.tableReaderCount = 5
	d.tr = tr
	d.resRows0 = make(chan sqlbase.EncDatumRow, 50)
	d.resRows1 = make(chan sqlbase.EncDatumRow, 50)
	d.resRows2 = make(chan sqlbase.EncDatumRow, 50)
	d.resRows3 = make(chan sqlbase.EncDatumRow, 50)
	d.resRows4 = make(chan sqlbase.EncDatumRow, 50)
	d.duplicate = make(chan sqlbase.EncDatumRow, 100)
	d.beginChan = make(chan int)
}
