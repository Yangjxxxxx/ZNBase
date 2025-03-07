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

// This file defines structures and basic functionality that is useful when
// building distsql plans. It does not contain the actual physical planning
// code.

package distsqlplan

import (
	"fmt"
	"math"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// Processor contains the information associated with a processor in a plan.
type Processor struct {
	// Node where the processor must be instantiated.
	Node roachpb.NodeID

	// Spec for the processor; note that the StreamEndpointSpecs in the input
	// synchronizers and output routers are not set until the end of the planning
	// process.
	Spec distsqlpb.ProcessorSpec
}

// ProcessorIdx identifies a processor by its index in PhysicalPlan.Processors.
type ProcessorIdx int

// Stream connects the output router of one processor to an input synchronizer
// of another processor.
type Stream struct {
	// SourceProcessor index (within the same plan).
	SourceProcessor ProcessorIdx

	// SourceRouterSlot identifies the position of this stream among the streams
	// that originate from the same router. This is important when routing by hash
	// where the order of the streams in the OutputRouterSpec matters.
	SourceRouterSlot int

	// DestProcessor index (within the same plan).
	DestProcessor ProcessorIdx

	// DestInput identifies the input of DestProcessor (some processors have
	// multiple inputs).
	DestInput int
}

// PhysicalPlan represents a network of processors and streams along with
// information about the results output by this network. The results come from
// unconnected output routers of a subset of processors; all these routers
// output the same kind of data (same schema).
type PhysicalPlan struct {
	// Processors in the plan.
	Processors []Processor

	// LocalProcessors contains all of the planNodeToRowSourceWrappers that were
	// installed in this physical plan to wrap any planNodes that couldn't be
	// properly translated into DistSQL processors. This will be empty if no
	// wrapping had to happen.
	LocalProcessors []runbase.LocalProcessor

	// LocalProcessorIndexes contains pointers to all of the RowSourceIdx fields
	// of the  LocalPlanNodeSpecs that were created. This list is in the same
	// order as LocalProcessors, and is kept up-to-date so that LocalPlanNodeSpecs
	// always have the correct index into the LocalProcessors slice.
	LocalProcessorIndexes []*uint32

	// Streams accumulates the streams in the plan - both local (intra-node) and
	// remote (inter-node); when we have a final plan, the streams are used to
	// generate processor input and output specs (see PopulateEndpoints).
	Streams []Stream

	// ResultRouters identifies the output routers which output the results of the
	// plan. These are the routers to which we have to connect new streams in
	// order to extend the plan.
	//
	// The processors which have this routers are all part of the same "stage":
	// they have the same "schema" and PostProcessSpec.
	//
	// We assume all processors have a single output so we only need the processor
	// index.
	ResultRouters []ProcessorIdx

	// ResultTypes is the schema (column types) of the rows produced by the
	// ResultRouters.
	//
	// This is aliased with InputSyncSpec.ColumnTypes, so it must not be modified
	// in-place during planning.
	ResultTypes []sqlbase.ColumnType

	ResultColumns sqlbase.ResultColumns

	// MergeOrdering is the ordering guarantee for the result streams that must be
	// maintained when the streams eventually merge. The column indexes refer to
	// columns for the rows produced by ResultRouters.
	//
	// Empty when there is a single result router. The reason is that maintaining
	// an ordering sometimes requires to add columns to streams for the sole
	// reason of correctly merging the streams later (see AddProjection); we don't
	// want to pay this cost if we don't have multiple streams to merge.
	MergeOrdering distsqlpb.Ordering

	// Used internally for numbering stages.
	stageCounter int32

	// Used internally to avoid creating flow IDs for local flows. This boolean
	// specifies whether there is more than one node involved in a plan.
	remotePlan bool

	// GatewayNodeID is the gateway node of the physical plan.
	GatewayNodeID roachpb.NodeID
	// Distribution is the indicator of the distribution of the physical plan.
	Distribution PlanDistribution

	// MaxEstimatedRowCount跟踪表的最大估计行数
	// 此计划的读者将输出。这个信息是用来做决定的
	// 是否使用矢量化的执行引擎。
	MaxEstimatedRowCount uint64
	storeEngine          cat.DataStoreEngine

	// ChangePlanForReplica indicates whether it is necessary to change the
	// physical plan of the replicated table so that it does not require hash
	// redistribution.
	ChangePlanForReplica bool
	// HaveReplicaTable indicates whether the table participating in the plan
	// has a replica table.
	HaveReplicaTable bool
}

// IsLastStageDistributed returns whether the last stage of processors is
// distributed (meaning that it contains at least one remote processor).
func (p *PhysicalPlan) IsLastStageDistributed() bool {
	return p.GetLastStageDistribution() != LocalPlan
}

// GetLastStageDistribution returns the distribution *only* of the last stage.
// Note that if the last stage consists of a single processor planned on a
// remote node, such stage is considered distributed.
func (p *PhysicalPlan) GetLastStageDistribution() PlanDistribution {
	if len(p.ResultRouters) == 1 && p.Processors[p.ResultRouters[0]].Node == p.GatewayNodeID {
		return LocalPlan
	}
	return FullyDistributedPlan
}

// PlanDistribution describes the distribution of the physical plan.
type PlanDistribution int

const (
	// LocalPlan indicates that the whole plan is executed on the gateway node.
	LocalPlan PlanDistribution = iota

	// PartiallyDistributedPlan indicates that some parts of the plan are
	// distributed while other parts are not (due to limitations of DistSQL).
	// Note that such plans can only be created by distSQLSpecExecFactory.
	//
	// An example of such plan is the plan with distributed scans that have a
	// filter which operates with an OID type (DistSQL currently doesn't
	// support distributed operations with such type). As a result, we end
	// up planning a noop processor on the gateway node that receives all
	// scanned rows from the remote nodes while performing the filtering
	// locally.
	PartiallyDistributedPlan

	// FullyDistributedPlan indicates that the whole plan is distributed.
	FullyDistributedPlan
)

// NewStageID creates a stage identifier that can be used in processor specs.
func (p *PhysicalPlan) NewStageID() int32 {
	p.stageCounter++
	return p.stageCounter
}

// AddProcessor adds a processor to a PhysicalPlan and returns the index that
// can be used to refer to that processor.
func (p *PhysicalPlan) AddProcessor(proc Processor) ProcessorIdx {
	idx := ProcessorIdx(len(p.Processors))
	p.Processors = append(p.Processors, proc)
	return idx
}

// SetMergeOrdering sets p.MergeOrdering.
func (p *PhysicalPlan) SetMergeOrdering(o distsqlpb.Ordering) {
	if len(p.ResultRouters) > 1 {
		p.MergeOrdering = o
	} else {
		p.MergeOrdering = distsqlpb.Ordering{}
	}
}

// AddNoGroupingStage adds a processor for each result router, on the same node
// with the source of the stream; all processors have the same core. This is for
// stages that correspond to logical blocks that don't require any grouping
// (e.g. evaluator, sorting, etc).
func (p *PhysicalPlan) AddNoGroupingStage(
	core distsqlpb.ProcessorCoreUnion,
	post distsqlpb.PostProcessSpec,
	outputTypes []sqlbase.ColumnType,
	newOrdering distsqlpb.Ordering,
) {
	p.AddNoGroupingStageWithCoreFunc(
		func(_ int, _ *Processor) distsqlpb.ProcessorCoreUnion { return core },
		post,
		outputTypes,
		newOrdering,
	)
}

// AddNoGroupingStageWithCoreFunc is like AddNoGroupingStage, but creates a core
// spec based on the input processor's spec.
func (p *PhysicalPlan) AddNoGroupingStageWithCoreFunc(
	coreFunc func(int, *Processor) distsqlpb.ProcessorCoreUnion,
	post distsqlpb.PostProcessSpec,
	outputTypes []sqlbase.ColumnType,
	newOrdering distsqlpb.Ordering,
) {
	stageID := p.NewStageID()
	for i, resultProc := range p.ResultRouters {
		prevProc := &p.Processors[resultProc]

		proc := Processor{
			Node: prevProc.Node,
			Spec: distsqlpb.ProcessorSpec{
				Input: []distsqlpb.InputSyncSpec{{
					Type:        distsqlpb.InputSyncSpec_UNORDERED,
					ColumnTypes: p.ResultTypes,
				}},
				Core: coreFunc(int(resultProc), prevProc),
				Post: post,
				Output: []distsqlpb.OutputRouterSpec{{
					Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
				}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)

		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			DestProcessor:    pIdx,
			SourceRouterSlot: 0,
			DestInput:        0,
		})

		p.ResultRouters[i] = pIdx
	}
	p.ResultTypes = outputTypes
	p.SetMergeOrdering(newOrdering)
}

// MergeResultStreams connects a set of resultRouters to a synchronizer. The
// synchronizer is configured with the provided ordering.
func (p *PhysicalPlan) MergeResultStreams(
	resultRouters []ProcessorIdx,
	sourceRouterSlot int,
	ordering distsqlpb.Ordering,
	destProcessor ProcessorIdx,
	destInput int,
) {
	proc := &p.Processors[destProcessor]
	if len(ordering.Columns) == 0 || len(resultRouters) == 1 {
		proc.Spec.Input[destInput].Type = distsqlpb.InputSyncSpec_UNORDERED
	} else {
		proc.Spec.Input[destInput].Type = distsqlpb.InputSyncSpec_ORDERED
		proc.Spec.Input[destInput].Ordering = ordering
	}

	for _, resultProc := range resultRouters {
		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			SourceRouterSlot: sourceRouterSlot,
			DestProcessor:    destProcessor,
			DestInput:        destInput,
		})
	}
}

// MergeResultStreamsForReplica connects a set of resultRouters to a synchronizer. The
// synchronizer is configured with the provided ordering.
func (p *PhysicalPlan) MergeResultStreamsForReplica(
	resultRouters []ProcessorIdx,
	sourceRouterSlot int,
	ordering distsqlpb.Ordering,
	destProcessor ProcessorIdx,
	destInput int,
) {
	proc := &p.Processors[destProcessor]
	if len(ordering.Columns) == 0 || len(resultRouters) == 1 {
		proc.Spec.Input[destInput].Type = distsqlpb.InputSyncSpec_UNORDERED
	} else {
		proc.Spec.Input[destInput].Type = distsqlpb.InputSyncSpec_ORDERED
		proc.Spec.Input[destInput].Ordering = ordering
	}

	for _, resultProc := range resultRouters {
		// If there is a replication table, the input will only come from
		// the node where it is located.
		if p.Processors[resultProc].Node != proc.Node {
			continue
		}
		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			SourceRouterSlot: sourceRouterSlot,
			DestProcessor:    destProcessor,
			DestInput:        destInput,
		})
	}
}

// MergeResultStreams1 connects a set of resultRouters to a synchronizer. The
// synchronizer is configured with the provided ordering.
func (p *PhysicalPlan) MergeResultStreams1(
	resultRouters []ProcessorIdx,
	sourceRouterSlot int,
	ordering distsqlpb.Ordering,
	destProcessor ProcessorIdx,
	destInput int,
	forceSerialization bool,
) {
	proc := &p.Processors[destProcessor]
	useUnorderedSync := len(ordering.Columns) == 0 && !forceSerialization
	if len(resultRouters) == 1 {
		useUnorderedSync = true
	}
	if useUnorderedSync {
		proc.Spec.Input[destInput].Type = distsqlpb.InputSyncSpec_UNORDERED
	} else {
		proc.Spec.Input[destInput].Type = distsqlpb.InputSyncSpec_ORDERED
		proc.Spec.Input[destInput].Ordering = ordering
	}

	for _, resultProc := range resultRouters {
		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			SourceRouterSlot: sourceRouterSlot,
			DestProcessor:    destProcessor,
			DestInput:        destInput,
		})
	}
}

// AddSingleGroupStage adds a "single group" stage (one that cannot be
// parallelized) which consists of a single processor on the specified node. The
// previous stage (ResultRouters) are all connected to this processor.
func (p *PhysicalPlan) AddSingleGroupStage(
	nodeID roachpb.NodeID,
	core distsqlpb.ProcessorCoreUnion,
	post distsqlpb.PostProcessSpec,
	outputTypes []sqlbase.ColumnType,
) {
	proc := Processor{
		Node: nodeID,
		Spec: distsqlpb.ProcessorSpec{
			Input: []distsqlpb.InputSyncSpec{{
				// The other fields will be filled in by mergeResultStreams.
				ColumnTypes: p.ResultTypes,
			}},
			Core: core,
			Post: post,
			Output: []distsqlpb.OutputRouterSpec{{
				Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
			}},
			StageID: p.NewStageID(),
		},
	}

	pIdx := p.AddProcessor(proc)

	// Connect the result routers to the processor.
	p.MergeResultStreams(p.ResultRouters, 0, p.MergeOrdering, pIdx, 0)

	// We now have a single result stream.
	p.ResultRouters = p.ResultRouters[:1]
	p.ResultRouters[0] = pIdx

	p.ResultTypes = outputTypes
	p.MergeOrdering = distsqlpb.Ordering{}
}

// CheckLastStagePost checks that the processors of the last stage of the
// PhysicalPlan have identical post-processing, returning an error if not.
func (p *PhysicalPlan) CheckLastStagePost() error {
	post := p.Processors[p.ResultRouters[0]].Spec.Post

	// All processors of a stage should be identical in terms of post-processing;
	// verify this assumption.
	for i := 1; i < len(p.ResultRouters); i++ {
		pi := &p.Processors[p.ResultRouters[i]].Spec.Post
		if pi.Filter != post.Filter ||
			pi.Projection != post.Projection ||
			len(pi.OutputColumns) != len(post.OutputColumns) ||
			len(pi.RenderExprs) != len(post.RenderExprs) {
			return errors.Errorf("inconsistent post-processing: %v vs %v", post, pi)
		}
		for j, col := range pi.OutputColumns {
			if col != post.OutputColumns[j] {
				return errors.Errorf("inconsistent post-processing: %v vs %v", post, pi)
			}
		}
		for j, col := range pi.RenderExprs {
			if col != post.RenderExprs[j] {
				return errors.Errorf("inconsistent post-processing: %v vs %v", post, pi)
			}
		}
	}

	return nil
}

// GetLastStagePost returns the PostProcessSpec for the processors in the last
// stage (ResultRouters).
func (p *PhysicalPlan) GetLastStagePost() distsqlpb.PostProcessSpec {
	if err := p.CheckLastStagePost(); err != nil {
		panic(err)
	}
	return p.Processors[p.ResultRouters[0]].Spec.Post
}

// SetLastStagePost changes the PostProcess spec of the processors in the last
// stage (ResultRouters).
// The caller must update the ordering via SetOrdering.
func (p *PhysicalPlan) SetLastStagePost(
	post distsqlpb.PostProcessSpec, outputTypes []sqlbase.ColumnType,
) {
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].Spec.Post = post
	}
	p.ResultTypes = outputTypes
}

func isIdentityProjection(columns []uint32, numExistingCols int) bool {
	if len(columns) != numExistingCols {
		return false
	}
	for i, c := range columns {
		if c != uint32(i) {
			return false
		}
	}
	return true
}

// AddProjection applies a projection to a plan. The new plan outputs the
// columns of the old plan as listed in the slice. The Ordering is updated;
// columns in the ordering are added to the projection as needed.
//
// The PostProcessSpec may not be updated if the resulting projection keeps all
// the columns in their original order.
//
// Note: the columns slice is relinquished to this function, which can modify it
// or use it directly in specs.
func (p *PhysicalPlan) AddProjection(columns []uint32) {
	// If the projection we are trying to apply projects every column, don't
	// update the spec.
	if isIdentityProjection(columns, len(p.ResultTypes)) {
		return
	}

	// Update the ordering.
	if len(p.MergeOrdering.Columns) > 0 {
		newOrdering := make([]distsqlpb.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			// Look for the column in the new projection.
			found := -1
			for j, projCol := range columns {
				if projCol == c.ColIdx {
					found = j
				}
			}
			if found == -1 {
				// We have a column that is not in the projection but will be necessary
				// later when the streams are merged; add it.
				found = len(columns)
				columns = append(columns, c.ColIdx)
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}

	newResultTypes := make([]sqlbase.ColumnType, len(columns))
	for i, c := range columns {
		newResultTypes[i] = p.ResultTypes[c]
	}

	post := p.GetLastStagePost()

	if post.RenderExprs != nil {
		// Apply the projection to the existing rendering; in other words, keep
		// only the renders needed by the new output columns, and reorder them
		// accordingly.
		oldRenders := post.RenderExprs
		post.RenderExprs = make([]distsqlpb.Expression, len(columns))
		for i, c := range columns {
			post.RenderExprs[i] = oldRenders[c]
		}
	} else {
		// There is no existing rendering; we can use OutputColumns to set the
		// projection.
		if post.Projection {
			// We already had a projection: compose it with the new one.
			for i, c := range columns {
				columns[i] = post.OutputColumns[c]
			}
		}
		post.OutputColumns = columns
		post.Projection = true
	}

	p.SetLastStagePost(post, newResultTypes)
}

// exprColumn returns the column that is referenced by the expression, if the
// expression is just an IndexedVar.
//
// See MakeExpression for a description of indexVarMap.
func exprColumn(expr tree.TypedExpr, indexVarMap []int) (int, bool) {
	v, ok := expr.(*tree.IndexedVar)
	if !ok {
		return -1, false
	}
	return indexVarMap[v.Idx], true
}

// AddRendering adds a rendering (expression evaluation) to the output of a
// plan. The rendering is achieved either through an adjustment on the last
// stage post-process spec, or via a new stage.
//
// The Ordering is updated; columns in the ordering are added to the render
// expressions as necessary.
//
// See MakeExpression for a description of indexVarMap.
func (p *PhysicalPlan) AddRendering(
	exprs []tree.TypedExpr, exprCtx ExprContext, indexVarMap []int, outTypes []sqlbase.ColumnType,
) error {
	// First check if we need an Evaluator, or we are just shuffling values. We
	// also check if the rendering is a no-op ("identity").
	needRendering := false
	identity := (len(exprs) == len(p.ResultTypes))

	for exprIdx, e := range exprs {
		varIdx, ok := exprColumn(e, indexVarMap)
		if !ok {
			needRendering = true
			break
		}
		identity = identity && (varIdx == exprIdx)
	}

	if !needRendering {
		if identity {
			// Nothing to do.
			return nil
		}
		// We don't need to do any rendering: the expressions effectively describe
		// just a projection.
		cols := make([]uint32, len(exprs))
		for i, e := range exprs {
			streamCol, _ := exprColumn(e, indexVarMap)
			if streamCol == -1 {
				panic(fmt.Sprintf("render %d refers to column not in source: %s", i, e))
			}
			cols[i] = uint32(streamCol)
		}
		p.AddProjection(cols)
		return nil
	}

	post := p.GetLastStagePost()
	if len(post.RenderExprs) > 0 {
		post = distsqlpb.PostProcessSpec{}
		// The last stage contains render expressions. The new renders refer to
		// the output of these, so we need to add another "no-op" stage to which
		// to attach the new rendering.
		p.AddNoGroupingStage(
			distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			post,
			p.ResultTypes,
			p.MergeOrdering,
		)
	}

	compositeMap := indexVarMap
	if post.Projection {
		compositeMap = reverseProjection(post.OutputColumns, indexVarMap)
	}
	post.RenderExprs = make([]distsqlpb.Expression, len(exprs))
	for i, e := range exprs {
		var err error
		post.RenderExprs[i], err = MakeExpression(e, exprCtx, compositeMap)
		if err != nil {
			return err
		}
	}

	if len(p.MergeOrdering.Columns) > 0 {
		outTypes = outTypes[:len(outTypes):len(outTypes)]
		newOrdering := make([]distsqlpb.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			found := -1
			// Look for the column in the new projection.
			for exprIdx, e := range exprs {
				if varIdx, ok := exprColumn(e, indexVarMap); ok && varIdx == int(c.ColIdx) {
					found = exprIdx
					break
				}
			}
			if found == -1 {
				// We have a column that is not being rendered but will be necessary
				// later when the streams are merged; add it.

				// The new expression refers to column post.OutputColumns[c.ColIdx].
				internalColIdx := c.ColIdx
				if post.Projection {
					internalColIdx = post.OutputColumns[internalColIdx]
				}
				newExpr, err := MakeExpression(tree.NewTypedOrdinalReference(
					int(internalColIdx),
					p.ResultTypes[c.ColIdx].ToDatumType()),
					exprCtx, nil /* indexVarMap */)
				if err != nil {
					return err
				}

				found = len(post.RenderExprs)
				post.RenderExprs = append(post.RenderExprs, newExpr)
				outTypes = append(outTypes, p.ResultTypes[c.ColIdx])
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}

	post.Projection = false
	post.OutputColumns = nil
	p.SetLastStagePost(post, outTypes)
	return nil
}

// reverseProjection remaps expression variable indices to refer to internal
// columns (i.e. before post-processing) of a processor instead of output
// columns (i.e. after post-processing).
//
// Inputs:
//   indexVarMap is a mapping from columns that appear in an expression
//               (planNode columns) to columns in the output stream of a
//               processor.
//   outputColumns is the list of output columns in the processor's
//                 PostProcessSpec; it is effectively a mapping from the output
//                 schema to the internal schema of a processor.
//
// Result: a "composite map" that maps the planNode columns to the internal
//         columns of the processor.
//
// For efficiency, the indexVarMap and the resulting map are represented as
// slices, with missing elements having values -1.
//
// Used when adding expressions (filtering, rendering) to a processor's
// PostProcessSpec. For example:
//
//  TableReader // table columns A,B,C,D
//  Internal schema (before post-processing): A, B, C, D
//  OutputColumns:  [1 3]
//  Output schema (after post-processing): B, D
//
//  Expression "B < D" might be represented as:
//    IndexedVar(4) < IndexedVar(1)
//  with associated indexVarMap:
//    [-1 1 -1 -1 0]  // 1->1, 4->0
//  This is effectively equivalent to "IndexedVar(0) < IndexedVar(1)"; 0 means
//  the first output column (B), 1 means the second output column (D).
//
//  To get an index var map that refers to the internal schema:
//    reverseProjection(
//      [1 3],           // OutputColumns
//      [-1 1 -1 -1 0],
//    ) =
//      [-1 3 -1 -1 1]   // 1->3, 4->1
//  This is effectively equivalent to "IndexedVar(1) < IndexedVar(3)"; 1
//  means the second internal column (B), 3 means the fourth internal column
//  (D).
func reverseProjection(outputColumns []uint32, indexVarMap []int) []int {
	if indexVarMap == nil {
		panic("no indexVarMap")
	}
	compositeMap := make([]int, len(indexVarMap))
	for i, col := range indexVarMap {
		if col == -1 {
			compositeMap[i] = -1
		} else {
			compositeMap[i] = int(outputColumns[col])
		}
	}
	return compositeMap
}

// AddFilter adds a filter on the output of a plan. The filter is added either
// as a post-processing step to the last stage or to a new "no-op" stage, as
// necessary.
//
// See MakeExpression for a description of indexVarMap.
func (p *PhysicalPlan) AddFilter(
	expr tree.TypedExpr, exprCtx ExprContext, indexVarMap []int,
) error {
	post := p.GetLastStagePost()
	if len(post.RenderExprs) > 0 || post.Offset != 0 || post.Limit != 0 {
		// The last stage contains render expressions or a limit. The filter refers
		// to the output as described by the existing spec, so we need to add
		// another "no-op" stage to which to attach the filter.
		//
		// In general, we might be able to push the filter "through" the rendering;
		// but the higher level planning code should figure this out when
		// propagating filters.
		post = distsqlpb.PostProcessSpec{}
		p.AddNoGroupingStage(
			distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			post,
			p.ResultTypes,
			p.MergeOrdering,
		)
	}

	compositeMap := indexVarMap
	if post.Projection {
		compositeMap = reverseProjection(post.OutputColumns, indexVarMap)
	}
	filter, err := MakeExpression(expr, exprCtx, compositeMap)
	if err != nil {
		return err
	}
	if !post.Filter.Empty() {
		if filter.Expr != "" {
			filter.Expr = fmt.Sprintf("(%s) AND (%s)", post.Filter.Expr, filter.Expr)
		} else if filter.LocalExpr != nil {
			filter.LocalExpr = tree.NewTypedAndExpr(
				post.Filter.LocalExpr,
				filter.LocalExpr,
			)
		}
	}
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].Spec.Post.Filter = filter
	}
	return nil
}

// emptyPlan creates a plan with a single processor that generates no rows; the
// output stream has the given types.
func emptyPlan(types []sqlbase.ColumnType, node roachpb.NodeID) PhysicalPlan {
	s := distsqlpb.ValuesCoreSpec{
		Columns: make([]distsqlpb.DatumInfo, len(types)),
	}
	for i, t := range types {
		s.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
		s.Columns[i].Type = t
	}

	return PhysicalPlan{
		Processors: []Processor{{
			Node: node,
			Spec: distsqlpb.ProcessorSpec{
				Core:   distsqlpb.ProcessorCoreUnion{Values: &s},
				Output: make([]distsqlpb.OutputRouterSpec, 1),
			},
		}},
		ResultRouters: []ProcessorIdx{0},
		ResultTypes:   types,
	}
}

func (p *PhysicalPlan) emptyPlan() {
	s := distsqlpb.ValuesCoreSpec{
		Columns: make([]distsqlpb.DatumInfo, len(p.ResultTypes)),
	}
	for i, t := range p.ResultTypes {
		s.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
		s.Columns[i].Type = t
	}

	*p = PhysicalPlan{
		Processors: []Processor{{
			Node: p.GatewayNodeID,
			Spec: distsqlpb.ProcessorSpec{
				Core:   distsqlpb.ProcessorCoreUnion{Values: &s},
				Output: make([]distsqlpb.OutputRouterSpec, 1),
			},
		}},
		ResultRouters: []ProcessorIdx{0},
		ResultTypes:   p.ResultTypes,
		ResultColumns: p.ResultColumns,
		GatewayNodeID: p.GatewayNodeID,
		Distribution:  LocalPlan,
	}
}

// AddLimit adds a limit and/or offset to the results of the current plan. If
// there are multiple result streams, they are joined into a single processor
// that is placed on the given node.
//
// For no limit, count should be MaxInt64.
func (p *PhysicalPlan) AddLimit(
	count int64, offset int64, exprCtx ExprContext, node roachpb.NodeID,
) error {
	if count < 0 {
		return errors.Errorf("negative limit")
	}
	if offset < 0 {
		return errors.Errorf("negative offset")
	}
	// limitZero is set to true if the limit is a legitimate LIMIT 0 requested by
	// the user. This needs to be tracked as a separate condition because DistSQL
	// uses count=0 to mean no limit, not a limit of 0. Normally, DistSQL will
	// short circuit 0-limit plans, but wrapped local planNodes sometimes need to
	// be fully-executed despite having 0 limit, so if we do in fact have a
	// limit-0 case when there's local planNodes around, we add an empty plan
	// instead of completely eliding the 0-limit plan.
	limitZero := false
	if count == 0 {
		if len(p.LocalProcessors) == 0 {
			*p = emptyPlan(p.ResultTypes, node)
			return nil
		}
		count = 1
		limitZero = true
	}

	if len(p.ResultRouters) == 1 {
		// We only have one processor producing results. Just update its PostProcessSpec.
		// SELECT FROM (SELECT OFFSET 10 LIMIT 1000) OFFSET 5 LIMIT 20 becomes
		// SELECT OFFSET 10+5 LIMIT min(1000, 20).
		post := p.GetLastStagePost()
		if offset != 0 {
			if post.Limit > 0 && post.Limit <= uint64(offset) {
				// The previous limit is not enough to reach the offset; we know there
				// will be no results. For example:
				//   SELECT * FROM (SELECT * FROM .. LIMIT 5) OFFSET 10
				// TODO(radu): perform this optimization while propagating filters
				// instead of having to detect it here.
				if len(p.LocalProcessors) == 0 {
					// Even though we know there will be no results, we don't elide the
					// plan if there are local processors. See comment above limitZero
					// for why.
					*p = emptyPlan(p.ResultTypes, node)
					return nil
				}
				count = 1
				limitZero = true
			}
			post.Offset += uint64(offset)
		}
		if count != math.MaxInt64 && (post.Limit == 0 || post.Limit > uint64(count)) {
			post.Limit = uint64(count)
		}
		p.SetLastStagePost(post, p.ResultTypes)
		if limitZero {
			if err := p.AddFilter(tree.DBoolFalse, exprCtx, nil); err != nil {
				return err
			}
		}
		return nil
	}

	// We have multiple processors producing results. We will add a single
	// processor stage that limits. As an optimization, we also set a
	// "local" limit on each processor producing results.
	if count != math.MaxInt64 {
		post := p.GetLastStagePost()
		// If we have OFFSET 10 LIMIT 5, we may need as much as 15 rows from any
		// processor.
		localLimit := uint64(count + offset)
		if post.Limit == 0 || post.Limit > localLimit {
			post.Limit = localLimit
			p.SetLastStagePost(post, p.ResultTypes)
		}
	}

	post := distsqlpb.PostProcessSpec{
		Offset: uint64(offset),
	}
	if count != math.MaxInt64 {
		post.Limit = uint64(count)
	}
	p.AddSingleGroupStage(
		node,
		distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
		post,
		p.ResultTypes,
	)
	if limitZero {
		if err := p.AddFilter(tree.DBoolFalse, exprCtx, nil); err != nil {
			return err
		}
	}
	return nil
}

// AddLimit1 adds a limit and/or offset to the results of the current plan. If
// there are multiple result streams, they are joined into a single processor
// that is placed on the given node.
//
// For no limit, count should be MaxInt64.
func (p *PhysicalPlan) AddLimit1(count int64, offset int64, exprCtx ExprContext) error {
	if count < 0 {
		return errors.Errorf("negative limit")
	}
	if offset < 0 {
		return errors.Errorf("negative offset")
	}
	// limitZero is set to true if the limit is a legitimate LIMIT 0 requested by
	// the user. This needs to be tracked as a separate condition because DistSQL
	// uses count=0 to mean no limit, not a limit of 0. Normally, DistSQL will
	// short circuit 0-limit plans, but wrapped local planNodes sometimes need to
	// be fully-executed despite having 0 limit, so if we do in fact have a
	// limit-0 case when there's local planNodes around, we add an empty plan
	// instead of completely eliding the 0-limit plan.
	limitZero := false
	if count == 0 {
		if len(p.LocalProcessors) == 0 {
			p.emptyPlan()
			return nil
		}
		count = 1
		limitZero = true
	}

	if len(p.ResultRouters) == 1 {
		// We only have one processor producing results. Just update its PostProcessSpec.
		// SELECT FROM (SELECT OFFSET 10 LIMIT 1000) OFFSET 5 LIMIT 20 becomes
		// SELECT OFFSET 10+5 LIMIT min(1000, 20).
		post := p.GetLastStagePost()
		if offset != 0 {
			if post.Limit > 0 && post.Limit <= uint64(offset) {
				// The previous limit is not enough to reach the offset; we know there
				// will be no results. For example:
				//   SELECT * FROM (SELECT * FROM .. LIMIT 5) OFFSET 10
				// TODO(radu): perform this optimization while propagating filters
				// instead of having to detect it here.
				if len(p.LocalProcessors) == 0 {
					// Even though we know there will be no results, we don't elide the
					// plan if there are local processors. See comment above limitZero
					// for why.
					p.emptyPlan()
					return nil
				}
				count = 1
				limitZero = true
			}
			// If we're collapsing an offset into a stage that already has a limit,
			// we have to be careful, since offsets always are applied first, before
			// limits. So, if the last stage already has a limit, we subtract the
			// offset from that limit to preserve correctness.
			//
			// As an example, consider the requirement of applying an offset of 3 on
			// top of a limit of 10. In this case, we need to emit 7 result rows. But
			// just propagating the offset blindly would produce 10 result rows, an
			// incorrect result.
			post.Offset += uint64(offset)
			if post.Limit > 0 {
				// Note that this can't fall below 0 - we would have already caught this
				// case above and returned an empty plan.
				post.Limit -= uint64(offset)
			}
		}
		if count != math.MaxInt64 && (post.Limit == 0 || post.Limit > uint64(count)) {
			post.Limit = uint64(count)
		}
		p.SetLastStagePost(post, p.ResultTypes)
		if limitZero {
			if err := p.AddFilter(tree.DBoolFalse, exprCtx, nil); err != nil {
				return err
			}
		}
		return nil
	}

	// We have multiple processors producing results. We will add a single
	// processor stage that limits. As an optimization, we also set a
	// "local" limit on each processor producing results.
	if count != math.MaxInt64 {
		post := p.GetLastStagePost()
		// If we have OFFSET 10 LIMIT 5, we may need as much as 15 rows from any
		// processor.
		localLimit := uint64(count + offset)
		if post.Limit == 0 || post.Limit > localLimit {
			post.Limit = localLimit
			p.SetLastStagePost(post, p.ResultTypes)
		}
	}

	post := distsqlpb.PostProcessSpec{
		Offset: uint64(offset),
	}
	if count != math.MaxInt64 {
		post.Limit = uint64(count)
	}
	p.AddSingleGroupStage(
		p.GatewayNodeID,
		distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
		post,
		p.ResultTypes,
	)
	if limitZero {
		if err := p.AddFilter(tree.DBoolFalse, exprCtx, nil); err != nil {
			return err
		}
	}
	return nil
}

//IsRemotePlan return p.remotePlan
func (p *PhysicalPlan) IsRemotePlan() bool {
	return p.remotePlan
}

// PopulateEndpoints processes p.Streams and adds the corresponding
// StreamEndpointSpecs to the processors' input and output specs. This should be
// used when the plan is completed and ready to be executed.
//
// The nodeAddresses map contains the address of all the nodes referenced in the
// plan.
func (p *PhysicalPlan) PopulateEndpoints(nodeAddresses map[roachpb.NodeID]string) {
	// Note: instead of using p.Streams, we could fill in the input/output specs
	// directly throughout the planning code, but this makes the rest of the code
	// a bit simpler.
	for sIdx, s := range p.Streams {
		p1 := &p.Processors[s.SourceProcessor]
		p2 := &p.Processors[s.DestProcessor]
		endpoint := distsqlpb.StreamEndpointSpec{StreamID: distsqlpb.StreamID(sIdx)}
		if p1.Node == p2.Node {
			endpoint.Type = distsqlpb.StreamEndpointSpec_LOCAL
		} else {
			endpoint.Type = distsqlpb.StreamEndpointSpec_REMOTE
		}
		p2.Spec.Input[s.DestInput].Streams = append(p2.Spec.Input[s.DestInput].Streams, endpoint)
		if endpoint.Type == distsqlpb.StreamEndpointSpec_REMOTE {
			if !p.remotePlan {
				p.remotePlan = true
			}
			endpoint.TargetNodeID = p2.Node
		}

		router := &p1.Spec.Output[0]
		// We are about to put this stream on the len(router.Streams) position in
		// the router; verify this matches the sourceRouterSlot. We expect it to
		// because the streams should be in order; if that assumption changes we can
		// reorder them here according to sourceRouterSlot.
		if len(router.Streams) != s.SourceRouterSlot && (!p.HaveReplicaTable || !p.ChangePlanForReplica) {
			panic(fmt.Sprintf(
				"sourceRouterSlot mismatch: %d, expected %d", len(router.Streams), s.SourceRouterSlot,
			))
		}
		router.Streams = append(router.Streams, endpoint)
	}
}

// GenerateFlowSpecs takes a plan (with populated endpoints) and generates the
// set of FlowSpecs (one per node involved in the plan).
//
// gateway is the current node's NodeID.
func (p *PhysicalPlan) GenerateFlowSpecs(
	gateway roachpb.NodeID, ctx *tree.EvalContext,
) map[roachpb.NodeID]*distsqlpb.FlowSpec {
	// Only generate a flow ID for a remote plan because it will need to be
	// referenced by remote nodes when connecting streams. This id generation is
	// skipped for performance reasons on local flows.
	flowID := distsqlpb.FlowID{}
	if p.remotePlan {
		flowID.UUID = uuid.MakeV4()
	}
	flows := make(map[roachpb.NodeID]*distsqlpb.FlowSpec, 1)

	for _, proc := range p.Processors {
		flowSpec, ok := flows[proc.Node]
		if !ok {
			flowSpec = NewFlowSpec(flowID, gateway)
			flows[proc.Node] = flowSpec
		}
		flowSpec.Processors = append(flowSpec.Processors, proc.Spec)
	}
	return flows
}

// MergePlans merges the processors and streams of two plan into a new plan.
// The result routers for each side are also returned (they point at processors
// in the merged plan).
func MergePlans(
	left, right *PhysicalPlan,
) (mergedPlan PhysicalPlan, leftRouters []ProcessorIdx, rightRouters []ProcessorIdx) {
	if left.HaveReplicaTable || right.HaveReplicaTable {
		mergedPlan.HaveReplicaTable = true
	}
	mergedPlan.Processors = append(left.Processors, right.Processors...)
	rightProcStart := ProcessorIdx(len(left.Processors))

	mergedPlan.Streams = append(left.Streams, right.Streams...)

	// Update the processor indices in the right streams.
	for i := len(left.Streams); i < len(mergedPlan.Streams); i++ {
		mergedPlan.Streams[i].SourceProcessor += rightProcStart
		mergedPlan.Streams[i].DestProcessor += rightProcStart
	}

	// Renumber the stages from the right plan.
	for i := rightProcStart; int(i) < len(mergedPlan.Processors); i++ {
		s := &mergedPlan.Processors[i].Spec
		if s.StageID != 0 {
			s.StageID += left.stageCounter
		}
	}
	mergedPlan.stageCounter = left.stageCounter + right.stageCounter

	mergedPlan.LocalProcessors = append(left.LocalProcessors, right.LocalProcessors...)
	mergedPlan.LocalProcessorIndexes = append(left.LocalProcessorIndexes, right.LocalProcessorIndexes...)
	// Update the local processor indices in the right streams.
	for i := len(left.LocalProcessorIndexes); i < len(mergedPlan.LocalProcessorIndexes); i++ {
		*mergedPlan.LocalProcessorIndexes[i] += uint32(len(left.LocalProcessorIndexes))
	}

	leftRouters = left.ResultRouters
	rightRouters = append([]ProcessorIdx(nil), right.ResultRouters...)
	// Update the processor indices in the right routers.
	for i := range rightRouters {
		rightRouters[i] += rightProcStart
	}

	return mergedPlan, leftRouters, rightRouters
}

// MergePlans1 merges the processors and streams of two plan into a new plan.
// The result routers for each side are also returned (they point at processors
// in the merged plan).
func MergePlans1(
	mergedPlan *PhysicalPlan,
	left, right *PhysicalPlan,
	leftPlanDistribution, rightPlanDistribution PlanDistribution,
) (leftRouters []ProcessorIdx, rightRouters []ProcessorIdx) {
	mergedPlan.Processors = append(left.Processors, right.Processors...)
	rightProcStart := ProcessorIdx(len(left.Processors))

	mergedPlan.Streams = append(left.Streams, right.Streams...)

	// Update the processor indices in the right streams.
	for i := len(left.Streams); i < len(mergedPlan.Streams); i++ {
		mergedPlan.Streams[i].SourceProcessor += rightProcStart
		mergedPlan.Streams[i].DestProcessor += rightProcStart
	}

	// Renumber the stages from the right plan.
	for i := rightProcStart; int(i) < len(mergedPlan.Processors); i++ {
		s := &mergedPlan.Processors[i].Spec
		if s.StageID != 0 {
			s.StageID += left.stageCounter
		}
	}
	mergedPlan.stageCounter = left.stageCounter + right.stageCounter

	mergedPlan.LocalProcessors = append(left.LocalProcessors, right.LocalProcessors...)
	mergedPlan.LocalProcessorIndexes = append(left.LocalProcessorIndexes, right.LocalProcessorIndexes...)
	// Update the local processor indices in the right streams.
	for i := len(left.LocalProcessorIndexes); i < len(mergedPlan.LocalProcessorIndexes); i++ {
		*mergedPlan.LocalProcessorIndexes[i] += uint32(len(left.LocalProcessorIndexes))
	}

	leftRouters = left.ResultRouters
	rightRouters = append([]ProcessorIdx(nil), right.ResultRouters...)
	// Update the processor indices in the right routers.
	for i := range rightRouters {
		rightRouters[i] += rightProcStart
	}

	mergedPlan.SetRowEstimates(left, right)
	mergedPlan.Distribution = leftPlanDistribution.compose(rightPlanDistribution)
	return leftRouters, rightRouters
}

// SetRowEstimates updates p according to the row estimates of left and right
// plans.
func (p *PhysicalPlan) SetRowEstimates(left, right *PhysicalPlan) {
	p.MaxEstimatedRowCount = left.MaxEstimatedRowCount
	if right.MaxEstimatedRowCount > p.MaxEstimatedRowCount {
		p.MaxEstimatedRowCount = right.MaxEstimatedRowCount
	}
}

func (a PlanDistribution) compose(b PlanDistribution) PlanDistribution {
	if a != b {
		return PartiallyDistributedPlan
	}
	return a
}

// MergeResultTypes reconciles the ResultTypes between two plans. It enforces
// that each pair of ColumnTypes must either match or be null, in which case the
// non-null type is used. This logic is necessary for cases like
// SELECT NULL UNION SELECT 1.
func MergeResultTypes(left, right []sqlbase.ColumnType) ([]sqlbase.ColumnType, error) {
	if len(left) != len(right) {
		return nil, errors.Errorf("ResultTypes length mismatch: %d and %d", len(left), len(right))
	}
	merged := make([]sqlbase.ColumnType, len(left))
	for i := range left {
		leftType, rightType := &left[i], &right[i]
		// only type.tInt is able to has different Width, so if the width of two tInt type column is differen
		// make their Width to be the bigger one
		if leftType.SemanticType == rightType.SemanticType {
			if leftType.SemanticType == sqlbase.ColumnType_INT {
				if leftType.Width > rightType.Width {
					rightType.Width = leftType.Width
				} else {
					leftType.Width = rightType.Width
				}
			} else if leftType.SemanticType == sqlbase.ColumnType_STRING {
				if leftType.Width > 0 {
					leftType.Width = 0
				}
				if rightType.Width > 0 {
					rightType.Width = 0
				}
			}
		}
		if rightType.SemanticType == sqlbase.ColumnType_NULL {
			merged[i] = *leftType
		} else if leftType.SemanticType == sqlbase.ColumnType_NULL {
			merged[i] = *rightType
		} else if equivalentTypes(leftType, rightType) {
			merged[i] = *leftType
		} else if rightType.SemanticType == sqlbase.ColumnType_SET {
			merged[i] = *leftType
		} else if leftType.SemanticType == sqlbase.ColumnType_SET {
			merged[i] = *rightType
		} else {
			return nil, errors.Errorf("conflicting ColumnTypes: %v and %v", leftType, rightType)
		}
	}
	return merged, nil
}

// equivalentType checks whether a column type is equivalent to
// another for the purpose of UNION. This excludes its VisibleType
// type alias, which doesn't effect the merging of values. There is
// also special handling for "naked" int types of no defined size.
func equivalentTypes(c, other *sqlbase.ColumnType) bool {
	// Convert pre-2.1 and pre-2.2 INTs to INT8.
	lhs := *c
	if lhs.SemanticType == sqlbase.ColumnType_INT {
		// Pre-2.2 INT without size was assigned width 0.
		// Pre-2.1 BIT was assigned arbitrary width, and is mapped to INT8 post-2.1. See #34161.
		if lhs.Width != 64 && lhs.Width != 32 && lhs.Width != 16 {
			lhs.Width = 64
		}
	}

	rhs := *other
	if rhs.SemanticType == sqlbase.ColumnType_INT {
		// See above.
		if rhs.Width != 64 && rhs.Width != 32 && rhs.Width != 16 {
			rhs.Width = 64
		}
	}
	rhs.VisibleType = c.VisibleType
	return lhs.Equal(rhs)
}

// AddJoinStage adds join processors at each of the specified nodes, and wires
// the left and right-side outputs to these processors.
func (p *PhysicalPlan) AddJoinStage(
	nodes []roachpb.NodeID,
	core distsqlpb.ProcessorCoreUnion,
	post distsqlpb.PostProcessSpec,
	leftEqCols, rightEqCols []uint32,
	leftTypes, rightTypes []sqlbase.ColumnType,
	leftMergeOrd, rightMergeOrd distsqlpb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
) {
	pIdxStart := ProcessorIdx(len(p.Processors))
	stageID := p.NewStageID()

	for _, n := range nodes {
		inputs := make([]distsqlpb.InputSyncSpec, 0, 2)
		inputs = append(inputs, distsqlpb.InputSyncSpec{ColumnTypes: leftTypes})
		inputs = append(inputs, distsqlpb.InputSyncSpec{ColumnTypes: rightTypes})

		proc := Processor{
			Node: n,
			Spec: distsqlpb.ProcessorSpec{
				Input: inputs,
				Core:  core,
				Post:  post,
				Output: []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
					ReplicationTable: p.HaveReplicaTable}},
				StageID: stageID,
			},
		}
		p.Processors = append(p.Processors, proc)
	}

	if len(nodes) > 1 {
		// Parallel hash or merge join: we distribute rows (by hash of
		// equality columns) to len(nodes) join processors.

		// Set up the left routers.
		for _, resultProc := range leftRouters {
			if p.ChangePlanForReplica && p.HaveReplicaTable {
				p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:             distsqlpb.OutputRouterSpec_PASS_THROUGH,
					ReplicationTable: true,
				}
				continue
			}
			p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
				Type:        distsqlpb.OutputRouterSpec_BY_HASH,
				HashColumns: leftEqCols,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			if p.ChangePlanForReplica && p.HaveReplicaTable {
				p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:             distsqlpb.OutputRouterSpec_PASS_THROUGH,
					ReplicationTable: true,
				}
				continue
			}
			p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
				Type:        distsqlpb.OutputRouterSpec_BY_HASH,
				HashColumns: rightEqCols,
			}
		}
	}
	p.ResultRouters = p.ResultRouters[:0]

	// Connect the left and right routers to the output joiners when there is a
	// replication table.
	if p.HaveReplicaTable && p.ChangePlanForReplica {
		for bucket := 0; bucket < len(nodes); bucket++ {
			pIdx := pIdxStart + ProcessorIdx(bucket)
			p.MergeResultStreamsForReplica(leftRouters, bucket, leftMergeOrd, pIdx, 0)
			p.MergeResultStreamsForReplica(rightRouters, bucket, rightMergeOrd, pIdx, 1)
			p.ResultRouters = append(p.ResultRouters, pIdx)
		}
		return
	}

	// Connect the left and right routers to the output joiners. Each joiner
	// corresponds to a hash bucket.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := pIdxStart + ProcessorIdx(bucket)

		// Connect left routers to the processor's first input. Currently the join
		// node doesn't care about the orderings of the left and right results.
		p.MergeResultStreams(leftRouters, bucket, leftMergeOrd, pIdx, 0)
		// Connect right routers to the processor's second input if it has one.
		p.MergeResultStreams(rightRouters, bucket, rightMergeOrd, pIdx, 1)

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
}

type hashJoinType int

const (
	BASEHASH = iota

	PnR

	PRPD
)

type HashJoinWorkArgs struct {
	HJType hashJoinType

	LeftHeavyHitters 	[]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter
	RightHeavyHitters 	[]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter
}

func (p *PhysicalPlan) AddJoinStageWithWorkArgs(
	nodes []roachpb.NodeID,
	core distsqlpb.ProcessorCoreUnion,
	post distsqlpb.PostProcessSpec,
	leftEqCols, rightEqCols []uint32,
	leftTypes, rightTypes []sqlbase.ColumnType,
	leftMergeOrd, rightMergeOrd distsqlpb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
	hjWorkArgs HashJoinWorkArgs,
) {
	pIdxStart := ProcessorIdx(len(p.Processors))
	stageID := p.NewStageID()

	for _, n := range nodes {
		inputs := make([]distsqlpb.InputSyncSpec, 0, 2)
		inputs = append(inputs, distsqlpb.InputSyncSpec{ColumnTypes: leftTypes})
		inputs = append(inputs, distsqlpb.InputSyncSpec{ColumnTypes: rightTypes})

		proc := Processor{
			Node: n,
			Spec: distsqlpb.ProcessorSpec{
				Input: inputs,
				Core:  core,
				Post:  post,
				Output: []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
					ReplicationTable: p.HaveReplicaTable}},
				StageID: stageID,
			},
		}
		p.Processors = append(p.Processors, proc)
	}

	if len(nodes) > 1 {
		switch hjWorkArgs.HJType {
		case BASEHASH:
			// Parallel hash or merge join: we distribute rows (by hash of
			// equality columns) to len(nodes) join processors.

			// Set up the left routers.
			for _, resultProc := range leftRouters {
				if p.ChangePlanForReplica && p.HaveReplicaTable {
					p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
						Type:             distsqlpb.OutputRouterSpec_PASS_THROUGH,
						ReplicationTable: true,
					}
					continue
				}
				p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:        distsqlpb.OutputRouterSpec_BY_HASH,
					HashColumns: leftEqCols,
				}
			}
			// Set up the right routers.
			for _, resultProc := range rightRouters {
				if p.ChangePlanForReplica && p.HaveReplicaTable {
					p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
						Type:             distsqlpb.OutputRouterSpec_PASS_THROUGH,
						ReplicationTable: true,
					}
					continue
				}
				p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:        distsqlpb.OutputRouterSpec_BY_HASH,
					HashColumns: rightEqCols,
				}
			}

		case PRPD:
			// Set up the left routers.
			leftRules := make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec, 2)
			leftRules[0] = distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec{
				MixHashType: distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HASH_LOCAL,
				SkewData: hjWorkArgs.LeftHeavyHitters,
			}
			leftRules[1] = distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec{
				MixHashType: distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HASH_MIRROR,
				SkewData: hjWorkArgs.RightHeavyHitters,
			}
			for _, resultProc := range leftRouters {
				p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:        distsqlpb.OutputRouterSpec_BY_MIX_HASH,
					HashColumns: leftEqCols,
					MixHashRules: leftRules,
				}
			}
			// Set up the right routers.
			rightRules := make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec, 2)
			rightRules[0] = distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec{
				MixHashType: distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HASH_LOCAL,
				SkewData: hjWorkArgs.RightHeavyHitters,
			}
			rightRules[1] = distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec{
				MixHashType:distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HASH_MIRROR,
				SkewData: hjWorkArgs.LeftHeavyHitters,
			}
			for _, resultProc := range rightRouters {
				p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:        distsqlpb.OutputRouterSpec_BY_MIX_HASH,
					HashColumns: rightEqCols,
					MixHashRules: rightRules,
				}
			}

		case PnR:
			// Set up the left routers.
			leftRules := make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec, 1)
			leftRules[0] = distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec{
				MixHashType: distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HASH_AVERAGE,
				SkewData: hjWorkArgs.LeftHeavyHitters,
			}
			for _, resultProc := range leftRouters {
				p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:        distsqlpb.OutputRouterSpec_BY_MIX_HASH,
					HashColumns: leftEqCols,
					MixHashRules: leftRules,
				}
			}
			// Set up the right routers.
			rightRules := make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec, 1)
			rightRules[0] = distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec{
				MixHashType: distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HASH_MIRROR,
				SkewData: hjWorkArgs.LeftHeavyHitters,
			}
			for _, resultProc := range rightRouters {
				p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:        distsqlpb.OutputRouterSpec_BY_MIX_HASH,
					HashColumns: rightEqCols,
					MixHashRules: rightRules,
				}
			}

		}
	}
	p.ResultRouters = p.ResultRouters[:0]

	// Connect the left and right routers to the output joiners when there is a
	// replication table.
	if p.HaveReplicaTable && p.ChangePlanForReplica {
		for bucket := 0; bucket < len(nodes); bucket++ {
			pIdx := pIdxStart + ProcessorIdx(bucket)
			p.MergeResultStreamsForReplica(leftRouters, bucket, leftMergeOrd, pIdx, 0)
			p.MergeResultStreamsForReplica(rightRouters, bucket, rightMergeOrd, pIdx, 1)
			p.ResultRouters = append(p.ResultRouters, pIdx)
		}
		return
	}

	// Connect the left and right routers to the output joiners. Each joiner
	// corresponds to a hash bucket.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := pIdxStart + ProcessorIdx(bucket)

		// Connect left routers to the processor's first input. Currently the join
		// node doesn't care about the orderings of the left and right results.
		p.MergeResultStreams(leftRouters, bucket, leftMergeOrd, pIdx, 0)
		// Connect right routers to the processor's second input if it has one.
		p.MergeResultStreams(rightRouters, bucket, rightMergeOrd, pIdx, 1)

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
}

// AddDistinctSetOpStage creates a distinct stage and a join stage to implement
// INTERSECT and EXCEPT plans.
//
// TODO(abhimadan): If there's a strong key on the left or right side, we
// can elide the distinct stage on that side.
func (p *PhysicalPlan) AddDistinctSetOpStage(
	nodes []roachpb.NodeID,
	joinCore distsqlpb.ProcessorCoreUnion,
	distinctCores []distsqlpb.ProcessorCoreUnion,
	post distsqlpb.PostProcessSpec,
	eqCols []uint32,
	leftTypes, rightTypes []sqlbase.ColumnType,
	leftMergeOrd, rightMergeOrd distsqlpb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
) {
	const numSides = 2
	inputResultTypes := [numSides][]sqlbase.ColumnType{leftTypes, rightTypes}
	inputMergeOrderings := [numSides]distsqlpb.Ordering{leftMergeOrd, rightMergeOrd}
	inputResultRouters := [numSides][]ProcessorIdx{leftRouters, rightRouters}

	// Create distinct stages for the left and right sides, where left and right
	// sources are sent by hash to the node which will contain the join processor.
	// The distinct stage must be before the join stage for EXCEPT queries to
	// produce correct results (e.g., (VALUES (1),(1),(2)) EXCEPT (VALUES (1))
	// would return (1),(2) instead of (2) if there was no distinct processor
	// before the EXCEPT ALL join).
	distinctIdxStart := len(p.Processors)
	distinctProcs := make(map[roachpb.NodeID][]ProcessorIdx)

	for side, types := range inputResultTypes {
		distinctStageID := p.NewStageID()
		for _, n := range nodes {
			proc := Processor{
				Node: n,
				Spec: distsqlpb.ProcessorSpec{
					Input: []distsqlpb.InputSyncSpec{
						{ColumnTypes: types},
					},
					Core:    distinctCores[side],
					Post:    distsqlpb.PostProcessSpec{},
					Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
					StageID: distinctStageID,
				},
			}
			pIdx := p.AddProcessor(proc)
			distinctProcs[n] = append(distinctProcs[n], pIdx)
		}
	}

	if len(nodes) > 1 {
		// Set up the left routers.
		for _, resultProc := range leftRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
				Type:        distsqlpb.OutputRouterSpec_BY_HASH,
				HashColumns: eqCols,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
				Type:        distsqlpb.OutputRouterSpec_BY_HASH,
				HashColumns: eqCols,
			}
		}
	}

	// Connect the left and right streams to the distinct processors.
	for side, routers := range inputResultRouters {
		// Get the processor index offset for the current side.
		sideOffset := side * len(nodes)
		for bucket := 0; bucket < len(nodes); bucket++ {
			pIdx := ProcessorIdx(distinctIdxStart + sideOffset + bucket)
			p.MergeResultStreams(routers, bucket, inputMergeOrderings[side], pIdx, 0)
		}
	}

	// Create a join stage, where the distinct processors on the same node are
	// connected to a join processor.
	joinStageID := p.NewStageID()
	p.ResultRouters = p.ResultRouters[:0]

	for _, n := range nodes {
		proc := Processor{
			Node: n,
			Spec: distsqlpb.ProcessorSpec{
				Input: []distsqlpb.InputSyncSpec{
					{ColumnTypes: leftTypes},
					{ColumnTypes: rightTypes},
				},
				Core:    joinCore,
				Post:    post,
				Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
				StageID: joinStageID,
			},
		}
		pIdx := p.AddProcessor(proc)

		for side, distinctProc := range distinctProcs[n] {
			p.Streams = append(p.Streams, Stream{
				SourceProcessor:  distinctProc,
				SourceRouterSlot: 0,
				DestProcessor:    pIdx,
				DestInput:        side,
			})
		}

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
}

//SetDataStoreEngine inherits from DataStoreEngine interface
func (p *PhysicalPlan) SetDataStoreEngine(ds cat.DataStoreEngine) {
	p.storeEngine = ds
}

//GetDataStoreEngine inherits from DataStoreEngine interface
func (p *PhysicalPlan) GetDataStoreEngine() cat.DataStoreEngine {
	return p.storeEngine
}

// ProcessorCorePlacement indicates on which node a particular processor core
// needs to be planned.
type ProcessorCorePlacement struct {
	NodeID roachpb.NodeID
	Core   distsqlpb.ProcessorCoreUnion
}

// AddNoInputStage creates a stage of processors that don't have any input from
// the other stages (if such exist). nodes and cores must be a one-to-one
// mapping so that a particular processor core is planned on the appropriate
// node.
func (p *PhysicalPlan) AddNoInputStage(
	corePlacements []ProcessorCorePlacement,
	post distsqlpb.PostProcessSpec,
	outputTypes []sqlbase.ColumnType,
	newOrdering distsqlpb.Ordering,
) {
	nodes := make([]roachpb.NodeID, len(corePlacements))
	for i := range corePlacements {
		nodes[i] = corePlacements[i].NodeID
	}
	stageID := p.NewStageID()
	p.ResultRouters = make([]ProcessorIdx, len(nodes))
	for i := range p.ResultRouters {
		proc := Processor{
			Node: corePlacements[i].NodeID,
			Spec: distsqlpb.ProcessorSpec{
				Core: corePlacements[i].Core,
				Post: post,
				Output: []distsqlpb.OutputRouterSpec{{
					Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
				}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	p.ResultTypes = outputTypes
	p.SetMergeOrdering(newOrdering)
}

// AddStageOnNodes adds a stage of processors that take in a single input
// logical stream on the specified nodes and connects them to the previous
// stage via a hash router.
func (p *PhysicalPlan) AddStageOnNodes(
	nodes []roachpb.NodeID,
	core distsqlpb.ProcessorCoreUnion,
	post distsqlpb.PostProcessSpec,
	hashCols []uint32,
	types []sqlbase.ColumnType,
	mergeOrd distsqlpb.Ordering,
	routers []ProcessorIdx,
) {
	pIdxStart := len(p.Processors)
	newStageID := p.NewStageID()

	for _, n := range nodes {
		proc := Processor{
			Node: n,
			Spec: distsqlpb.ProcessorSpec{
				Input: []distsqlpb.InputSyncSpec{
					{ColumnTypes: types},
				},
				Core:    core,
				Post:    post,
				Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
				StageID: newStageID,
			},
		}
		p.AddProcessor(proc)
	}

	if len(nodes) > 1 {
		// Set up the routers.
		for _, resultProc := range routers {
			p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
				Type:        distsqlpb.OutputRouterSpec_BY_HASH,
				HashColumns: hashCols,
			}
		}
	}

	// Connect the result streams to the processors.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := ProcessorIdx(pIdxStart + bucket)
		p.MergeResultStreams1(routers, bucket, mergeOrd, pIdx, 0, false /* forceSerialization */)
	}

	// Set the new result routers.
	p.ResultRouters = p.ResultRouters[:0]
	for i := 0; i < len(nodes); i++ {
		p.ResultRouters = append(p.ResultRouters, ProcessorIdx(pIdxStart+i))
	}
}
