// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/log/logtags"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// ExportPlanResultTypes is the result types for EXPORT plans.
var ExportPlanResultTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_STRING}, // queryname
	{SemanticType: sqlbase.ColumnType_STRING}, // filename
	{SemanticType: sqlbase.ColumnType_INT},    // rows
	{SemanticType: sqlbase.ColumnType_INT},    // bytes
}

// PlanAndRunExport makes and runs an EXPORT plan for the given input and output
// planNode and spec respectively.  The input planNode must be runnable via
// DistSQL. The output spec's results must conform to the ExportResultTypes.
func PlanAndRunExport(
	ctx context.Context,
	dsp *DistSQLPlanner,
	execCfg *ExecutorConfig,
	txn *client.Txn,
	evalCtx *extendedEvalContext,
	ins []planNode,
	outs []distsqlpb.ProcessorCoreUnion,
	resultRows *RowResultWriter,
) error {
	planCtx, _, err := dsp.setupAllNodesPlanning(ctx, evalCtx, execCfg)
	if err != nil {
		return err
	}
	plan := PhysicalPlan{}
	plan.ResultRouters = make([]distsqlplan.ProcessorIdx, 0)
	//当planNode仅有一个的时候，说明此时只涉及select以及单表的查询，并不涉及到多表整表导出和整库导出以及 TODO 整模式导出，因此此时并不需要进行执行计划的重组
	if len(ins) == 1 {
		rec, err := dsp.checkSupportForNode(ins[0])
		planCtx.isLocal = err != nil || rec == cannotDistribute
		p, err := dsp.createPlanForNode(planCtx, ins[0])
		if err != nil {
			return errors.Wrap(err, "constructing distSQL plan")
		}
		if len(outs) == 0 {
			return errors.New("an internal error occurred during the export process")
		}
		p.AddNoGroupingStage(
			outs[0], distsqlpb.PostProcessSpec{}, ExportPlanResultTypes, distsqlpb.Ordering{},
		)
		plan = p
	} else {
		beforeStageID := plan.NewStageID()
		afterStageID := plan.NewStageID()
		for i, in := range ins {
			rec, err := dsp.checkSupportForNode(in)
			planCtx.isLocal = err != nil || rec == cannotDistribute
			p, err := dsp.createPlanForNode(planCtx, in)
			if err != nil {
				return errors.Wrap(err, "constructing distSQL plan")
			}
			p.AddNoGroupingStage(
				outs[i], distsqlpb.PostProcessSpec{}, ExportPlanResultTypes, distsqlpb.Ordering{},
			)
			if len(p.Processors) < 2 {
				return errors.Wrap(err, "export error")
			}
			for index, procID := range p.ResultRouters {
				p.Processors[index].Spec.StageID = beforeStageID
				p.Processors[procID].Spec.StageID = afterStageID
				pIdxBefore := plan.AddProcessor(p.Processors[index])
				pIdx := plan.AddProcessor(p.Processors[procID])
				plan.Streams = append(plan.Streams, distsqlplan.Stream{
					SourceProcessor:  pIdxBefore,
					DestProcessor:    pIdx,
					SourceRouterSlot: 0,
					DestInput:        0,
				})
				plan.ResultRouters = append(plan.ResultRouters, pIdx)
			}
		}
	}
	// Overwrite PlanToStreamColMap (used by recv below) to reflect the output of
	// the non-grouping stage we've added above. That stage outputs produces
	// columns filename/rows/bytes.
	plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(ExportPlanResultTypes))
	plan.ResultTypes = ExportPlanResultTypes
	dsp.FinalizePlan(planCtx, &plan)
	recv := MakeDistSQLReceiver(
		ctx, resultRows, tree.Rows,
		execCfg.RangeDescriptorCache, execCfg.LeaseHolderCache, txn, func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)
	defer recv.Release()
	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, txn, &plan, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	return resultRows.Err()
}

// RowResultWriter is a thin wrapper around a RowContainer.
type RowResultWriter struct {
	rowContainer *rowcontainer.RowContainer
	rowsAffected int
	err          error
}

var _ rowResultWriter = &RowResultWriter{}

// NewRowResultWriter creates a new RowResultWriter.
func NewRowResultWriter(rowContainer *rowcontainer.RowContainer) *RowResultWriter {
	return &RowResultWriter{rowContainer: rowContainer}
}

// IncrementRowsAffected implements the rowResultWriter interface.
func (b *RowResultWriter) IncrementRowsAffected(n int) {
	b.rowsAffected += n
}

// AddRow implements the rowResultWriter interface.
func (b *RowResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	_, err := b.rowContainer.AddRow(ctx, row)
	return err
}

// SetError is part of the rowResultWriter interface.
func (b *RowResultWriter) SetError(err error) {
	b.err = err
}

// Err is part of the rowResultWriter interface.
func (b *RowResultWriter) Err() error {
	return b.err
}

// callbackResultWriter is a rowResultWriter that runs a callback function
// on AddRow.
type callbackResultWriter struct {
	fn           func(ctx context.Context, row tree.Datums) error
	rowsAffected int
	err          error
	IsUDRPlanner bool
	curRow       tree.Datums
}

var _ rowResultWriter = &callbackResultWriter{}

// newCallbackResultWriter creates a new callbackResultWriter.
func newCallbackResultWriter(
	fn func(ctx context.Context, row tree.Datums) error,
) *callbackResultWriter {
	return &callbackResultWriter{fn: fn}
}

func (c *callbackResultWriter) IncrementRowsAffected(n int) {
	c.rowsAffected += n
}

func (c *callbackResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	if c.IsUDRPlanner {
		if c.rowsAffected == 0 && UDRExecRes.RowCount == 0 { // && c.rowsAffected == 0
			c.curRow = make(tree.Datums, len(row))
			copy(c.curRow, row)
			// may have error use UDRExecRes.RowCount == 0, when error happen, use flow judge
			// c.rowsAffected++
		}
	}
	return c.fn(ctx, row)
}

func (c *callbackResultWriter) SetError(err error) {
	c.err = err
}

func (c *callbackResultWriter) Err() error {
	return c.err
}

var colTypeBytes = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}

// KeyRewriter describes helpers that can rewrite keys (possibly in-place).
type KeyRewriter interface {
	RewriteKey(key []byte) (res []byte, ok bool, err error)
}

// LoadCSV performs a distributed transformation of the CSV files at from
// and stores them in enterprise backup format at to.
func LoadCSV(
	ctx context.Context,
	phs PlanHookState,
	job *jobs.Job,
	resultRows *RowResultWriter,
	tables map[string]*sqlbase.TableDescriptor,
	from []*encoding.TableRegion,
	to string,
	format roachpb.IOFileFormat,
	walltime int64,
	splitSize int64,
	oversample int64,
	makeRewriter func(map[sqlbase.ID]*sqlbase.TableDescriptor) (KeyRewriter, error),
) error {
	ctx = logtags.AddTag(ctx, "import-distsql", nil)
	dsp := phs.DistSQLPlanner()
	evalCtx := phs.ExtendedEvalContext()

	planCtx, nodes, err := dsp.setupAllNodesPlanning(ctx, evalCtx, phs.ExecCfg())
	if err != nil {
		return err
	}

	inputSpecs := makeImportReaderSpecs(job, tables, from, format, nodes, walltime)

	sstSpecs := make([]distsqlpb.SSTWriterSpec, len(nodes))
	var table sqlbase.TableDescriptor
	//TODO only one table
	for _, tbl := range tables {
		table = *tbl
	}
	for i := range nodes {
		sstSpecs[i] = distsqlpb.SSTWriterSpec{
			Destination:   to,
			WalltimeNanos: walltime,
			Table:         table,
		}
	}

	db := phs.ExecCfg().DB
	thisNode := phs.ExecCfg().NodeID.Get()

	// Determine if we need to run the sampling plan or not.

	details := job.Details().(jobspb.ImportDetails)
	samples := details.Samples
	if samples == nil {
		var err error
		samples, err = dsp.loadCSVSamplingPlan(ctx, job, db, evalCtx, thisNode, nodes, from, splitSize, oversample, planCtx, inputSpecs, sstSpecs)
		if err != nil {
			return err
		}
	}

	/*
		TODO(dt): when we enable reading schemasMap during sampling, might do this:
		// If sampling returns parsed table definitions, we need to potentially assign
		// them real IDs and re-key the samples with those IDs, then update the job
		// details to record the tables and their matching samples.
			if len(parsedTables) > 0 {
				importing := to == "" // are we actually ingesting, or just transforming?

				rekeys := make(map[sqlbase.ID]*sqlbase.TableDescriptor, len(parsedTables))

				// Update the tables map with the parsed tables and allocate them real IDs.
				for _, parsed := range parsedTables {
					name := parsed.Name
					if existing, ok := tables[name]; ok && existing != nil {
						return errors.Errorf("unexpected parsed table definition for %q", name)
					}
					tables[name] = parsed

					// If we're actually importing, we'll need a real ID for this table.
					if importing {
						rekeys[parsed.ID] = parsed
						parsed.ID, err = GenerateUniqueDescID(ctx, phs.ExecCfg().DB)
						if err != nil {
							return err
						}
					}
				}

				// The samples were created using the dummy IDs, but the IMPORT run will use
				// the actual IDs, so we need to re-key the samples so that they actually
				// act as splits in the IMPORTed key-space.
				if importing {
					kr, err := makeRewriter(rekeys)
					if err != nil {
						return err
					}
					for i := range samples {
						var ok bool
						samples[i], ok, err = kr.RewriteKey(samples[i])
						if err != nil {
							return err
						}
						if !ok {
							return errors.Errorf("expected rewriter to rewrite key %v", samples[i])
						}
					}
				}
			}
	*/

	if len(tables) == 0 {
		return errors.Errorf("must specify table(s) to import")
	}

	splits := make([][]byte, 0, 2*len(tables)+len(samples))
	// Add the table keys to the spans.
	for _, desc := range tables {
		tableSpan := desc.TableSpan()
		splits = append(splits, tableSpan.Key, tableSpan.EndKey)
	}

	splits = append(splits, samples...)
	sort.Slice(splits, func(a, b int) bool { return roachpb.Key(splits[a]).Compare(splits[b]) < 0 })

	// Remove duplicates. These occur when the end span of a descriptor is the
	// same as the start span of another.
	origSplits := splits
	splits = splits[:0]
	for _, x := range origSplits {
		if len(splits) == 0 || !bytes.Equal(x, splits[len(splits)-1]) {
			splits = append(splits, x)
		}
	}

	// jobSpans is a slice of split points, including table start and end keys
	// for the table. We create router range spans then from taking each pair
	// of adjacent keys.
	spans := make([]distsqlpb.OutputRouterSpec_RangeRouterSpec_Span, len(splits)-1)
	encFn := func(b []byte) []byte {
		return encoding.EncodeBytesAscending(nil, b)
	}
	for i := 1; i < len(splits); i++ {
		start := splits[i-1]
		end := splits[i]
		stream := int32((i - 1) % len(nodes))
		spans[i-1] = distsqlpb.OutputRouterSpec_RangeRouterSpec_Span{
			Start:  encFn(start),
			End:    encFn(end),
			Stream: stream,
		}
		sstSpecs[stream].Spans = append(sstSpecs[stream].Spans, distsqlpb.SSTWriterSpec_SpanName{
			Name: fmt.Sprintf("%d.sst", i),
			End:  end,
		})
	}

	// We have the split ranges. Now re-read the CSV files and route them to SST writers.

	p := PhysicalPlan{}
	// This is a hardcoded two stage plan. The first stage is the mappers,
	// the second stage is the reducers. We have to keep track of all the mappers
	// we create because the reducers need to hook up a stream for each mapper.
	firstStageRouters := make([]distsqlplan.ProcessorIdx, len(inputSpecs))
	firstStageTypes := []sqlbase.ColumnType{colTypeBytes, colTypeBytes}

	routerSpec := distsqlpb.OutputRouterSpec_RangeRouterSpec{
		Spans: spans,
		Encodings: []distsqlpb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: sqlbase.DatumEncoding_ASCENDING_KEY,
			},
		},
	}

	stageID := p.NewStageID()
	// We can reuse the phase 1 ReadCSV specs, just have to clear sampling.
	for i, rcs := range inputSpecs {
		rcs.SampleSize = 0
		proc := distsqlplan.Processor{
			Node: nodes[i],
			Spec: distsqlpb.ProcessorSpec{
				Core: distsqlpb.ProcessorCoreUnion{ReadImport: rcs},
				Output: []distsqlpb.OutputRouterSpec{{
					Type:             distsqlpb.OutputRouterSpec_BY_RANGE,
					RangeRouterSpec:  routerSpec,
					DisableBuffering: true,
				}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		firstStageRouters[i] = pIdx
	}

	// The SST Writer returns 5 columns: name of the file, encoded BulkOpSummary,
	// checksum, start key, end key.
	p.PlanToStreamColMap = []int{0, 1, 2, 3, 4}
	p.ResultTypes = []sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_STRING},
		colTypeBytes,
		colTypeBytes,
		colTypeBytes,
		colTypeBytes,
	}

	stageID = p.NewStageID()
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, 0, len(nodes))
	for i, node := range nodes {
		swSpec := sstSpecs[i]
		if len(swSpec.Spans) == 0 {
			continue
		}
		swSpec.Progress = distsqlpb.JobProgress{
			JobID:        *job.ID(),
			Slot:         int32(len(p.ResultRouters)),
			Contribution: float32(len(swSpec.Spans)) / float32(len(spans)),
		}
		proc := distsqlplan.Processor{
			Node: node,
			Spec: distsqlpb.ProcessorSpec{
				Input: []distsqlpb.InputSyncSpec{{
					ColumnTypes: firstStageTypes,
				}},
				Core:    distsqlpb.ProcessorCoreUnion{SSTWriter: &swSpec},
				Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		for _, router := range firstStageRouters {
			p.Streams = append(p.Streams, distsqlplan.Stream{
				SourceProcessor:  router,
				SourceRouterSlot: i,
				DestProcessor:    pIdx,
				DestInput:        0,
			})
		}
		p.ResultRouters = append(p.ResultRouters, pIdx)
	}

	// Update job details with the sampled keys, as well as any parsed tables,
	// clear SamplingProgress and prep second stage job details for progress
	// tracking.
	if err := job.FractionDetailProgressed(ctx,
		func(ctx context.Context, details jobspb.Details, progress jobspb.ProgressDetails) float32 {
			prog := progress.(*jobspb.Progress_Import).Import
			prog.SamplingProgress = nil
			prog.ReadProgress = make([]float32, len(inputSpecs))
			prog.WriteProgress = make([]float32, len(p.ResultRouters))

			d := details.(*jobspb.Payload_Import).Import
			d.Samples = samples
			d.PrepareComplete = true
			return prog.Completed()
		},
	); err != nil {
		return err
	}

	dsp.FinalizePlan(planCtx, &p)

	recv := MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.Rows,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)
	defer recv.Release()

	defer log.VEventf(ctx, 1, "finished job %s", job.Payload().Description)
	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		evalCtxCopy.Txn = txn
		dsp.Run(planCtx, txn, &p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
		return resultRows.Err()
	})
}

func (dsp *DistSQLPlanner) setupAllNodesPlanning(
	ctx context.Context, evalCtx *extendedEvalContext, execCfg *ExecutorConfig,
) (*PlanningCtx, []roachpb.NodeID, error) {
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, nil /* txn */)

	resp, err := execCfg.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, nil, err
	}
	// Because we're not going through the normal pathways, we have to set up
	// the nodeID -> nodeAddress map ourselves.
	for _, node := range resp.Nodes {
		if err := dsp.CheckNodeHealthAndVersion(planCtx, &node.Desc); err != nil {
			continue
		}
	}
	nodes := make([]roachpb.NodeID, 0, len(planCtx.NodeAddresses))
	for nodeID := range planCtx.NodeAddresses {
		nodes = append(nodes, nodeID)
	}
	// Shuffle node order so that multiple IMPORTs done in parallel will not
	// identically schedule CSV reading. For example, if there are 3 nodes and 4
	// files, the first node will get 2 files while the other nodes will each get 1
	// file. Shuffling will make that first node random instead of always the same.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return planCtx, nodes, nil
}

func makeImportReaderSpecs(
	job *jobs.Job,
	tables map[string]*sqlbase.TableDescriptor,
	from []*encoding.TableRegion,
	format roachpb.IOFileFormat,
	nodes []roachpb.NodeID,
	walltime int64,
) []*distsqlpb.ReadImportDataSpec {

	// For each input file, assign it to a node.
	inputSpecs := make([]*distsqlpb.ReadImportDataSpec, 0, len(nodes))
	var n int
	var url *distsqlpb.ReadImportDataSpec_TableURL
	for i, input := range from {
		// Round robin assign CSV files to nodes. Files 0 through len(nodes)-1
		// creates the spec. Future files just add themselves to the Uris.
		if i < len(nodes) {
			spec := &distsqlpb.ReadImportDataSpec{
				Tables: tables,
				Format: format,
				Progress: distsqlpb.JobProgress{
					JobID: *job.ID(),
					Slot:  int32(i),
				},
				WalltimeNanos:          walltime,
				SkipPrimaryKeyConflict: format.Kafka.SkipPrimaryKeyConflicts,
			}
			inputSpecs = append(inputSpecs, spec)
			url = &distsqlpb.ReadImportDataSpec_TableURL{Path: input.File, Offset: input.Chunk.Offset, EndOffset: input.Chunk.EndOffset, Size_: input.Chunk.Size}
			inputSpecs[i].Uri = make(map[int32]*distsqlpb.ReadImportDataSpec_TableURL)
		}
		n = i % len(nodes)
		url = &distsqlpb.ReadImportDataSpec_TableURL{Path: input.File, Offset: input.Chunk.Offset, EndOffset: input.Chunk.EndOffset, Size_: input.Chunk.Size}
		inputSpecs[n].Uri[int32(i)] = url
	}

	for i := range inputSpecs {
		// TODO(mjibson): using the actual file sizes here would improve progress
		// accuracy.
		inputSpecs[i].Progress.Contribution = float32(len(inputSpecs[i].Uri)) / float32(len(from))
	}
	return inputSpecs
}

func (dsp *DistSQLPlanner) loadCSVSamplingPlan(
	ctx context.Context,
	job *jobs.Job,
	db *client.DB,
	evalCtx *extendedEvalContext,
	thisNode roachpb.NodeID,
	nodes []roachpb.NodeID,
	from []*encoding.TableRegion,
	splitSize int64,
	oversample int64,
	planCtx *PlanningCtx,
	csvSpecs []*distsqlpb.ReadImportDataSpec,
	sstSpecs []distsqlpb.SSTWriterSpec,
) ([][]byte, error) {
	// splitSize is the target number of bytes at which to create SST files. We
	// attempt to do this by sampling, which is what the first DistSQL plan of this
	// function does. CSV rows are converted into KVs. The total size of the KV is
	// used to determine if we should sample it or not. For example, if we had a
	// 100 byte KV and a 30MB splitSize, we would sample the KV with probability
	// 100/30000000. Over many KVs, this produces samples at approximately the
	// correct spacing, but obviously also with some error. We use oversample
	// below to decrease the error. We divide the splitSize by oversample to
	// produce the actual sampling rate. So in the example above, oversampling by a
	// factor of 3 would sample the KV with probability 100/10000000 since we are
	// sampling at 3x. Since we're now getting back 3x more samples than needed,
	// we only use every 1/(oversample), or 1/3 here, in our final sampling.
	if oversample < 1 {
		oversample = 3
	}
	sampleSize := splitSize / oversample
	if sampleSize > math.MaxInt32 {
		return nil, errors.Errorf("SST size must fit in an int32: %d", splitSize)
	}

	var p PhysicalPlan
	stageID := p.NewStageID()

	for _, cs := range csvSpecs {
		cs.SampleSize = int32(sampleSize)
	}

	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(csvSpecs))
	for i, rcs := range csvSpecs {
		proc := distsqlplan.Processor{
			Node: nodes[i],
			Spec: distsqlpb.ProcessorSpec{
				Core:    distsqlpb.ProcessorCoreUnion{ReadImport: rcs},
				Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	if err := job.FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
		d := details.(*jobspb.Progress_Import).Import
		d.SamplingProgress = make([]float32, len(csvSpecs))
		return d.Completed()
	}); err != nil {
		return nil, err
	}

	// We only need the key during sorting.
	p.PlanToStreamColMap = []int{0, 1}
	p.ResultTypes = []sqlbase.ColumnType{colTypeBytes, colTypeBytes}

	kvOrdering := distsqlpb.Ordering{
		Columns: []distsqlpb.Ordering_Column{{
			ColIdx:    0,
			Direction: distsqlpb.Ordering_Column_ASC,
		}, {
			ColIdx:    1,
			Direction: distsqlpb.Ordering_Column_ASC,
		}},
	}

	sorterSpec := distsqlpb.SorterSpec{
		OutputOrdering: kvOrdering,
	}

	p.AddSingleGroupStage(thisNode,
		distsqlpb.ProcessorCoreUnion{Sorter: &sorterSpec},
		distsqlpb.PostProcessSpec{},
		[]sqlbase.ColumnType{colTypeBytes, colTypeBytes},
	)

	var samples [][]byte

	sampleCount := 0
	rowResultWriter := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		key := roachpb.Key(*row[0].(*tree.DBytes))

		/*
			TODO(dt): when we enable reading schemasMap during sampling, might do this:
			if keys.IsDescriptorKey(key) {
				kv := roachpb.KeyValue{Key: key}
				kv.Value.RawBytes = []byte(*row[1].(*tree.DBytes))
				var desc sqlbase.TableDescriptor
				if err := kv.Value.GetProto(&desc); err != nil {
					return err
				}
				parsedTables[desc.ID] = &desc
				return nil
			}
		*/
		sampleCount++
		sampleCount = sampleCount % int(oversample)
		if sampleCount == 0 {
			k, err := keys.EnsureSafeSplitKey(key)
			if err != nil {
				return err
			}
			samples = append(samples, k)
		}
		return nil
	})

	// TODO(dan): Consider making FinalizePlan take a map explicitly instead
	// of this PlanCtx. https://reviewable.io/reviews/znbasedb/znbase/17279#-KqOrLpy9EZwbRKHLYe6:-KqOp00ntQEyzwEthAsl:bd4nzje
	dsp.FinalizePlan(planCtx, &p)

	recv := MakeDistSQLReceiver(
		ctx,
		rowResultWriter,
		tree.Rows,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)
	defer recv.Release()
	log.VEventf(ctx, 1, "begin sampling phase of job %s", job.Payload().Description)
	// Clear the stage 2 data in case this function is ever restarted (it shouldn't be).
	samples = nil
	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, nil, &p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	if err := rowResultWriter.Err(); err != nil {
		return nil, err
	}

	log.VEventf(ctx, 1, "generated %d splits; begin routing for job %s", len(samples), job.Payload().Description)

	return samples, nil
}

// DistIngest is used by IMPORT to run a DistSQL flow to ingest data by starting
// reader processes on many nodes that each read and ingest their assigned files
// and then send back a summary of what they ingested. The combined summary is
// returned.
func DistIngest(
	ctx context.Context,
	phs PlanHookState,
	job *jobs.Job,
	tables map[string]*sqlbase.TableDescriptor,
	from []*encoding.TableRegion,
	format roachpb.IOFileFormat,
	walltime int64,
) (roachpb.BulkOpSummary, error) {
	ctx = logtags.AddTag(ctx, "import-distsql-ingest", nil)

	dsp := phs.DistSQLPlanner()
	evalCtx := phs.ExtendedEvalContext()

	planCtx, nodes, err := dsp.setupAllNodesPlanning(ctx, evalCtx, phs.ExecCfg())
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}

	inputSpecs := makeImportReaderSpecs(job, tables, from, format, nodes, walltime)

	for i := range inputSpecs {
		inputSpecs[i].IngestDirectly = true
	}

	var p PhysicalPlan

	// Setup a one-stage plan with one proc per input spec.
	stageID := p.NewStageID()
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(inputSpecs))
	for i, rcs := range inputSpecs {
		proc := distsqlplan.Processor{
			Node: nodes[i],
			Spec: distsqlpb.ProcessorSpec{
				Core:    distsqlpb.ProcessorCoreUnion{ReadImport: rcs},
				Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	var res roachpb.BulkOpSummary

	// The direct-ingest readers will emit a binary encoded BulkOpSummary.
	p.PlanToStreamColMap = []int{0}
	p.ResultTypes = []sqlbase.ColumnType{colTypeBytes}

	rowResultWriter := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		var counts roachpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
			return err
		}
		res.Add(counts)
		return nil
	})

	dsp.FinalizePlan(planCtx, &p)

	if err := job.FractionDetailProgressed(ctx,
		func(ctx context.Context, details jobspb.Details, progress jobspb.ProgressDetails) float32 {
			prog := progress.(*jobspb.Progress_Import).Import
			prog.ReadProgress = make([]float32, len(inputSpecs))
			return prog.Completed()
		},
	); err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	if err := presplitTableBoundaries(ctx, phs.ExecCfg(), tables); err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	recv := MakeDistSQLReceiver(
		ctx,
		rowResultWriter,
		tree.Rows,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)
	defer recv.Release()
	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, nil, &p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	if err := rowResultWriter.Err(); err != nil {
		return roachpb.BulkOpSummary{}, err
	}

	return res, nil
}

func presplitTableBoundaries(
	ctx context.Context, cfg *ExecutorConfig, tables map[string]*sqlbase.TableDescriptor,
) error {
	for _, tblDesc := range tables {
		// TODO(ajwerner): Consider passing in the wrapped descriptors.
		for _, span := range tblDesc.AllIndexSpans() {
			if err := cfg.DB.AdminSplit(ctx, span.Key, span.Key); err != nil {
				return err
			}
			log.VEventf(ctx, 1, "scattering index range %s", span.Key)
			scatterReq := &roachpb.AdminScatterRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(span),
			}
			if _, pErr := client.SendWrapped(ctx, cfg.DB.NonTransactionalSender(), scatterReq); pErr != nil {
				log.Errorf(ctx, "failed to scatter span %s: %s", span.Key, pErr)
			}
		}
	}
	return nil
}
