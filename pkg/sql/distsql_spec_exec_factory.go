package sql

import (
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/constraint"
	"github.com/znbasedb/znbase/pkg/sql/opt/exec"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/opt/props/physical"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/errorutil/unimplemented"
)

type distSQLSpecExecFactory struct {
	planner *planner
	dsp     *DistSQLPlanner
	// planContexts is a utility struct that stores already instantiated
	// planning contexts. It should not be accessed directly, use getPlanCtx()
	// instead. The struct allows for lazy instantiation of the planning
	// contexts which are then reused between different calls to Construct*
	// methods. We need to keep both because every stage of processors can
	// either be distributed or local regardless of the distribution of the
	// previous stages.
	planContexts struct {
		distPlanCtx *PlanningCtx
		// localPlanCtx stores the local planning context of the gateway.
		localPlanCtx *PlanningCtx
	}
	//singleTenant  bool
	planningMode  distSQLPlanningMode
	gatewayNodeID roachpb.NodeID
}

var _ exec.Factory = &distSQLSpecExecFactory{}

// distSQLPlanningMode indicates the planning mode in which
// distSQLSpecExecFactory is operating.
type distSQLPlanningMode int

const (
	// distSQLDefaultPlanning is the default planning mode in which the factory
	// can create a physical plan with any plan distribution (local, partially
	// distributed, or fully distributed).
	distSQLDefaultPlanning distSQLPlanningMode = iota
	// distSQLLocalOnlyPlanning is the planning mode in which the factory
	// only creates local physical plans.
	distSQLLocalOnlyPlanning
)

func newDistSQLSpecExecFactory(p *planner, planningMode distSQLPlanningMode) exec.Factory {
	return &distSQLSpecExecFactory{
		planner:       p,
		dsp:           p.extendedEvalCtx.DistSQLPlanner,
		planningMode:  planningMode,
		gatewayNodeID: p.extendedEvalCtx.DistSQLPlanner.nodeDesc.NodeID,
	}
}

func (f *distSQLSpecExecFactory) ConstructWindow(
	n exec.Node, wi exec.WindowInfo,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *distSQLSpecExecFactory) getPlanCtx(recommendation distRecommendation) *PlanningCtx {
	distribute := false
	if f.planningMode != distSQLLocalOnlyPlanning {
		distribute = shouldDistributeGivenRecAndMode(recommendation, f.planner.extendedEvalCtx.SessionData.DistSQLMode)
	}
	if distribute {
		if f.planContexts.distPlanCtx == nil {
			evalCtx := f.planner.ExtendedEvalContext()
			f.planContexts.distPlanCtx = f.dsp.NewPlanningCtx1(evalCtx.Context, evalCtx, f.planner, f.planner.txn, distribute)
		}
		return f.planContexts.distPlanCtx
	}
	if f.planContexts.localPlanCtx == nil {
		evalCtx := f.planner.ExtendedEvalContext()
		f.planContexts.localPlanCtx = f.dsp.NewPlanningCtx1(evalCtx.Context, evalCtx, f.planner, f.planner.txn, distribute)
	}
	return f.planContexts.localPlanCtx
}

func (f *distSQLSpecExecFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	if (len(cols) == 0 && len(rows) == 1) || len(rows) == 0 {
		physPlan, err := f.dsp.createValuesPlan(
			getTypesFromResultColumns(cols), len(rows), nil, /* rawBytes */
		)
		if err != nil {
			return nil, err
		}
		physPlan.ResultColumns = cols
		return makePlanMaybePhysical(&physPlan, nil /* planNodesToClose */), nil
	}
	recommendation := shouldDistribute
	for _, exprs := range rows {
		recommendation = recommendation.compose(
			f.checkExprsAndMaybeMergeLastStage(exprs, nil /* physPlan */),
		)
		if recommendation == cannotDistribute {
			break
		}
	}

	var (
		physPlan         PhysicalPlan
		err              error
		planNodesToClose []planNode
	)
	planCtx := f.getPlanCtx(recommendation)
	if mustWrapValuesNode(planCtx, true /* specifiedInQuery */) {
		// The valuesNode must be wrapped into the physical plan, so we cannot
		// avoid creating it. See mustWrapValuesNode for more details.
		v := &valuesNode{
			columns:          cols,
			tuples:           rows,
			specifiedInQuery: true,
		}
		planNodesToClose = []planNode{v}
		physPlan, err = f.dsp.wrapPlan(planCtx, v)
	} else {
		// We can create a spec for the values processor, so we don't create a
		// valuesNode.
		physPlan, err = f.dsp.createPhysPlanForTuples(planCtx, rows, cols)
	}
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = cols
	return makePlanMaybePhysical(&physPlan, planNodesToClose), nil
}

func (f *distSQLSpecExecFactory) ConstructScan(
	table cat.Table,
	index cat.Index,
	needed exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	reverse bool,
	maxResults uint64,
	reqOrdering exec.OutputOrdering,
	fl float64,
	locking *tree.LockingItem,
) (exec.Node, error) {
	if locking != nil {
		return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Update")
	}

	p := MakePhysicalPlan(f.gatewayNodeID)
	// Although we don't yet recommend distributing plans where soft limits
	// propagate to scan nodes because we don't have infrastructure to only
	// plan for a few ranges at a time, the propagation of the soft limits
	// to scan nodes has been added in 20.1 release, so to keep the
	// previous behavior we continue to ignore the soft limits for now.
	// TODO(yuzefovich): pay attention to the soft limits.
	recommendation := canDistribute

	// Phase 1: set up all necessary infrastructure for table reader planning
	// below. This phase is equivalent to what execFactory.ConstructScan does.
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	colCfg := makeScanColumnsConfig(table, needed)

	// Note that initColsForScan and setting ResultColumns below are equivalent
	// to what scan.initTable call does in execFactory.ConstructScan.
	cols, err := initColsForScan(tabDesc, colCfg)
	if err != nil {
		return nil, err
	}
	p.ResultColumns = sqlbase.ResultColumnsFromColDescs(cols)

	if indexConstraint != nil && indexConstraint.IsContradiction() {
		// Note that empty rows argument is handled by ConstructValues first -
		// it will always create an appropriate values processor spec, so there
		// will be no planNodes created (which is what we want in this case).
		return f.ConstructValues([][]tree.TypedExpr{} /* rows */, p.ResultColumns)
	}

	var spans roachpb.Spans
	spans, err = spansFromConstraint(tabDesc, indexDesc, indexConstraint, needed, false /* forDelete */)

	if err != nil {
		return nil, err
	}

	isFullTableOrIndexScan := len(spans) == 1 && spans[0].EqualValue(
		tabDesc.IndexSpan(indexDesc.ID),
	)
	if err = colCfg.assertValidReqOrdering(reqOrdering); err != nil {
		return nil, err
	}

	// Check if we are doing a full scan.
	if isFullTableOrIndexScan {
		recommendation = recommendation.compose(shouldDistribute)
	}

	// Phase 2: perform the table reader planning. This phase is equivalent to
	// what DistSQLPlanner.createTableReaders does.
	colsToTableOrdinalMap := toTableOrdinals(cols, tabDesc, colCfg.visibility)
	trSpec := distsqlplan.NewTableReaderSpec()
	*trSpec = distsqlpb.TableReaderSpec{
		Table:      *tabDesc.TableDesc(),
		Reverse:    reverse,
		IsCheck:    false,
		Visibility: distsqlpb.ScanVisibility(colCfg.visibility),
		// Retain the capacity of the spans slice.
		Spans: trSpec.Spans[:0],
	}
	trSpec.IndexIdx, err = getIndexIdx1(indexDesc, tabDesc)
	if err != nil {
		return nil, err
	}

	// Note that we don't do anything about the possible filter here since we
	// don't know yet whether we will have it. ConstructFilter is responsible
	// for pushing the filter down into the post-processing stage of this scan.
	post := distsqlpb.PostProcessSpec{}
	if hardLimit != 0 {
		post.Limit = uint64(hardLimit)
	}

	err = f.dsp.planTableReaders(
		f.getPlanCtx(recommendation),
		&p,
		&tableReaderPlanningInfo{
			spec:                  trSpec,
			post:                  post,
			desc:                  tabDesc,
			spans:                 spans,
			reverse:               reverse,
			scanVisibility:        colCfg.visibility.toDistSQLScanVisibility(),
			parallelize:           false,
			estimatedRowCount:     uint64(fl),
			reqOrdering:           reqOrdering,
			cols:                  cols,
			colsToTableOrdinalMap: colsToTableOrdinalMap,
		},
	)

	return makePlanMaybePhysical(&p, nil /* planNodesToClose */), err
}

func (f *distSQLSpecExecFactory) ConstructVirtualScan(table cat.Table) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning VirtualScan")
}

func (f *distSQLSpecExecFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	recommendation := f.checkExprsAndMaybeMergeLastStage([]tree.TypedExpr{filter}, physPlan)
	// AddFilter will attempt to push the filter into the last stage of
	// processors.
	if err := physPlan.AddFilter(filter, f.getPlanCtx(recommendation), physPlan.PlanToStreamColMap); err != nil {
		return nil, err
	}
	physPlan.SetMergeOrdering(f.dsp.convertOrdering1(sqlbase.ColumnOrdering(reqOrdering), physPlan.PlanToStreamColMap))
	return plan, nil
}

func (f *distSQLSpecExecFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.ColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	projection := make([]uint32, len(cols))
	for i := range cols {
		projection[i] = uint32(cols[physPlan.PlanToStreamColMap[i]])
	}
	physPlan.AddProjection(projection)
	physPlan.ResultColumns = getResultColumnsForSimpleProject(
		cols, nil /* colNames */, physPlan.ResultTypes, physPlan.ResultColumns,
	)
	physPlan.PlanToStreamColMap = identityMap(physPlan.PlanToStreamColMap, len(cols))
	physPlan.SetMergeOrdering(f.dsp.convertOrdering1(sqlbase.ColumnOrdering(reqOrdering), physPlan.PlanToStreamColMap))
	return plan, nil
}

func getResultColumnsForSimpleProject(
	cols []exec.ColumnOrdinal,
	colNames []string,
	resultTypes []sqlbase.ColumnType,
	inputCols sqlbase.ResultColumns,
) sqlbase.ResultColumns {
	resultCols := make(sqlbase.ResultColumns, len(cols))
	for i, col := range cols {
		if colNames == nil {
			resultCols[i] = inputCols[col]
			// If we have a SimpleProject, we should clear the hidden bit on any
			// column since it indicates it's been explicitly selected.
			resultCols[i].Hidden = false
		} else {
			resultCols[i] = sqlbase.ResultColumn{
				Name: colNames[i],
				Typ:  resultTypes[i].ToDatumType(),
			}
		}
	}
	return resultCols
}

func (f *distSQLSpecExecFactory) ConstructRender(
	n exec.Node,
	columns sqlbase.ResultColumns,
	exprs tree.TypedExprs,
	reqOrdering exec.OutputOrdering,
	functions []cat.UdrFunction,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	recommendation := f.checkExprsAndMaybeMergeLastStage(exprs, physPlan)
	if err := physPlan.AddRendering(
		exprs, f.getPlanCtx(recommendation), physPlan.PlanToStreamColMap, getTypesFromResultColumns(columns),
	); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = columns
	physPlan.PlanToStreamColMap = identityMap(physPlan.PlanToStreamColMap, len(exprs))
	physPlan.SetMergeOrdering(f.dsp.convertOrdering1(sqlbase.ColumnOrdering(reqOrdering), physPlan.PlanToStreamColMap))
	return plan, nil
}

func (f *distSQLSpecExecFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
	forceSide int,
	ParallelFlag bool,
) (exec.Node, error) {
	return f.constructHashOrMergeJoin(
		joinType, left, right, extraOnCond, leftEqCols, rightEqCols,
		leftEqColsAreKey, rightEqColsAreKey,
		sqlbase.ColumnOrdering{} /* mergeJoinOrdering */, exec.OutputOrdering{}, /* reqOrdering */
		forceSide,
		ParallelFlag,
	)
}

func (f *distSQLSpecExecFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	leftBoundColMap opt.ColMap,
	memo *memo.Memo,
	rightProps *physical.Required,
	fakeRight exec.Node,
	right memo.RelExpr,
	onCond tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning ApplyJoin")
}

func (f *distSQLSpecExecFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	leftEqCols, rightEqCols, mergeJoinOrdering, err := getEqualityIndicesAndMergeJoinOrdering(leftOrdering, rightOrdering)
	if err != nil {
		return nil, err
	}
	return f.constructHashOrMergeJoin(
		joinType, left, right, onCond, leftEqCols, rightEqCols,
		leftEqColsAreKey, rightEqColsAreKey, mergeJoinOrdering, reqOrdering, 0, false,
	)
}

func populateAggFuncSpec(
	spec *distsqlpb.AggregatorSpec_Aggregation,
	funcName string,
	distinct bool,
	argCols []exec.ColumnOrdinal,
	constArgs []tree.Datum,
	filter exec.ColumnOrdinal,
	planCtx *PlanningCtx,
	physPlan *PhysicalPlan,
) (argumentsColumnTypes []types.T, err error) {
	funcIdx, err := distsqlpb.GetAggregateFuncIdx(funcName)
	if err != nil {
		return nil, err
	}
	spec.Func = distsqlpb.AggregatorSpec_Func(funcIdx)
	spec.Distinct = distinct
	spec.ColIdx = make([]uint32, len(argCols))
	for i, col := range argCols {
		spec.ColIdx[i] = uint32(col)
	}
	if filter != tree.NoColumnIdx {
		filterColIdx := uint32(physPlan.PlanToStreamColMap[filter])
		spec.FilterColIdx = &filterColIdx
	}
	if len(constArgs) > 0 {
		spec.Arguments = make([]distsqlpb.Expression, len(constArgs))
		argumentsColumnTypes = make([]types.T, len(constArgs))
		for k, argument := range constArgs {
			var err error
			spec.Arguments[k], err = distsqlplan.MakeExpression(argument, planCtx, nil)
			if err != nil {
				return nil, err
			}
			argumentsColumnTypes[k] = argument.ResolvedType()
		}
	}
	return argumentsColumnTypes, nil
}

func (f *distSQLSpecExecFactory) constructAggregators(
	input exec.Node,
	groupCols []exec.ColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
	isScalar bool,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	// planAggregators() itself decides whether to distribute the aggregation.
	planCtx := f.getPlanCtx(shouldDistribute)
	aggregationSpecs := make([]distsqlpb.AggregatorSpec_Aggregation, len(groupCols)+len(aggregations))
	argumentsColumnTypes := make([][]types.T, len(groupCols)+len(aggregations))
	var err error
	if len(groupCols) > 0 {
		argColsScratch := []exec.ColumnOrdinal{0}
		noFilter := exec.ColumnOrdinal(tree.NoColumnIdx)
		for i, col := range groupCols {
			spec := &aggregationSpecs[i]
			argColsScratch[0] = col
			_, err = populateAggFuncSpec(
				spec, builtins.AnyNotNull, false /* distinct*/, argColsScratch,
				nil /* constArgs */, noFilter, planCtx, physPlan,
			)
			if err != nil {
				return nil, err
			}
		}
	}
	for j := range aggregations {
		i := len(groupCols) + j
		spec := &aggregationSpecs[i]
		agg := &aggregations[j]
		argumentsColumnTypes[i], err = populateAggFuncSpec(
			spec, agg.FuncName, agg.Distinct, agg.ArgCols,
			agg.ConstArgs, agg.Filter, planCtx, physPlan,
		)
		if err != nil {
			return nil, err
		}
	}
	if err := f.dsp.planAggregators(
		planCtx,
		physPlan,
		&aggregatorPlanningInfo{
			aggregations:         aggregationSpecs,
			argumentsColumnTypes: argumentsColumnTypes,
			isScalar:             isScalar,
			groupCols:            convertOrdinalsToInts(groupCols),
			groupColOrdering:     groupColOrdering,
			inputMergeOrdering:   physPlan.MergeOrdering,
			reqOrdering:          reqOrdering,
		},
	); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = getResultColumnsForGroupBy(physPlan.ResultColumns, groupCols, aggregations)
	return plan, nil
}

func getResultColumnsForGroupBy(
	inputCols sqlbase.ResultColumns, groupCols []exec.ColumnOrdinal, aggregations []exec.AggInfo,
) sqlbase.ResultColumns {
	columns := make(sqlbase.ResultColumns, 0, len(groupCols)+len(aggregations))
	for _, col := range groupCols {
		columns = append(columns, inputCols[col])
	}
	for _, agg := range aggregations {
		columns = append(columns, sqlbase.ResultColumn{
			Name: agg.FuncName,
			Typ:  agg.ResultType,
		})
	}
	return columns
}

func (f *distSQLSpecExecFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.ColumnOrdinal,
	orderedGroupCols exec.ColumnOrdinalSet,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
	groupColOrdering sqlbase.ColumnOrdering,
) (exec.Node, error) {
	return f.constructAggregators(
		input,
		groupCols,
		groupColOrdering,
		aggregations,
		reqOrdering,
		false, /* isScalar */
	)
}

func (f *distSQLSpecExecFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	return f.constructAggregators(
		input,
		nil, /* groupCols */
		nil, /* groupColOrdering */
		aggregations,
		exec.OutputOrdering{}, /* reqOrdering */
		true,                  /* isScalar */
	)
}

func (f *distSQLSpecExecFactory) ConstructDistinct(
	input exec.Node, distinctCols, orderedCols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	spec := createDistinctSpec1(
		distinctCols,
		orderedCols,
		physPlan.PlanToStreamColMap,
	)
	f.dsp.addDistinctProcessors(physPlan, spec, reqOrdering)
	return plan, nil
}

func (f *distSQLSpecExecFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning SetOp")
}

func (f *distSQLSpecExecFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	f.dsp.addPhySorters(physPlan, ordering, alreadyOrderedPrefix)
	return plan, nil
}

func (f *distSQLSpecExecFactory) ConstructOrdinality(
	input exec.Node, colName string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Ordinality")
}

func (f *distSQLSpecExecFactory) ConstructIndexJoin(
	input exec.Node, table cat.Table, cols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning IndexJoin")
}

func (f *distSQLSpecExecFactory) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	keyCols []exec.ColumnOrdinal,
	eqColsAreKey bool,
	lookupCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning LookupJoin")
}

func (f *distSQLSpecExecFactory) ConstructZigzagJoin(
	leftTable cat.Table,
	leftIndex cat.Index,
	rightTable cat.Table,
	rightIndex cat.Index,
	leftEqCols []exec.ColumnOrdinal,
	rightEqCols []exec.ColumnOrdinal,
	leftCols exec.ColumnOrdinalSet,
	rightCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	fixedVals []exec.Node,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning ZigzagJoin")
}

func (f *distSQLSpecExecFactory) ConstructLimit(
	input exec.Node, limitexpr, offsetexpr tree.TypedExpr,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	// Note that we pass in nil slice for exprs because we will evaluate both
	// expressions below, locally.
	recommendation := f.checkExprsAndMaybeMergeLastStage(nil /* exprs */, physPlan)
	count, offset, err := evalLimit(f.planner.EvalContext(), limitexpr, offsetexpr)
	if err != nil {
		return nil, err
	}
	if err = physPlan.AddLimit1(count, offset, f.getPlanCtx(recommendation)); err != nil {
		return nil, err
	}
	// Since addition of limit and/or offset doesn't change any properties of
	// the physical plan, we don't need to update any of those (like
	// PlanToStreamColMap, etc).
	return plan, nil
}

func (f *distSQLSpecExecFactory) ConstructMax1Row(input exec.Node) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Max1Row")
}

func (f *distSQLSpecExecFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	cols := append(plan.physPlan.ResultColumns, zipCols...)
	err := f.dsp.addProjectSet(
		physPlan,
		// Currently, projectSetProcessors are always planned as a "grouping"
		// stage (meaning a single processor on the gateway), so we use
		// cannotDistribute as the recommendation.
		f.getPlanCtx(cannotDistribute),
		&projectSetPlanningInfo{
			columns:         cols,
			numColsInSource: len(plan.physPlan.ResultColumns),
			exprs:           exprs,
			numColsPerGen:   numColsPerGen,
		},
	)
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = cols
	return plan, nil
}

func (f *distSQLSpecExecFactory) RenameColumns(
	input exec.Node, colNames []string,
) (exec.Node, error) {
	plan := input.(planMaybePhysical)
	for i := range plan.physPlan.ResultColumns {
		plan.physPlan.ResultColumns[i].Name = colNames[i]
	}
	return input, nil
}

func (f *distSQLSpecExecFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery,
) (exec.Plan, error) {
	return constructPlan(f.planner, root, subqueries)
}

func (f *distSQLSpecExecFactory) ConstructExplainOpt(
	plan string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning ExplainOpt")
}

func (f *distSQLSpecExecFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan exec.Plan,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Explain")
}

func (f *distSQLSpecExecFactory) ConstructShowTrace(
	typ tree.ShowTraceType, compact bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning ShowTrace")
}

func (f *distSQLSpecExecFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertCols exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	rowsNeeded bool,
	upperOwner string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Insert")
}

func (f *distSQLSpecExecFactory) ConstructUpdate(
	input exec.Node,
	table cat.Table,
	fetchCols exec.ColumnOrdinalSet,
	updateCols exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	passthrough sqlbase.ResultColumns,
	rowsNeeded bool,
	upperOwner string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Update")
}

func (f *distSQLSpecExecFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	canaryCol exec.ColumnOrdinal,
	additionalCol exec.ColumnOrdinal,
	insertCols exec.ColumnOrdinalSet,
	fetchCols exec.ColumnOrdinalSet,
	updateCols exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	rowsNeeded bool,
	onlyMatch bool,
	isMerge bool,
	upperOwner string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Upsert")
}

func (f *distSQLSpecExecFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.ColumnOrdinalSet,
	rowsNeeded bool,
	upperOwner string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Delete")
}

func (f *distSQLSpecExecFactory) ConstructDeleteRange(
	table cat.Table, needed exec.ColumnOrdinalSet, indexConstraint *constraint.Constraint,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning DeleteRange")
}

func (f *distSQLSpecExecFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning CreateTable")
}

func (f *distSQLSpecExecFactory) ConstructCreateView(
	schema cat.Schema,
	cv *tree.CreateView,
	columns sqlbase.ResultColumns,
	deps opt.ViewDeps,
	hasStar bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning CreateView")
}

func (f *distSQLSpecExecFactory) ConstructBuffer(value exec.Node, label string) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning Buffer")
}

func (f *distSQLSpecExecFactory) ConstructScanBuffer(
	ref exec.Node, label string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning ScanBuffer")
}

func (f *distSQLSpecExecFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string, unionAll bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning RecursiveCTE")
}

func (f *distSQLSpecExecFactory) ConstructSequenceSelect(seq cat.Sequence) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning SequenceSelect")
}

func (f *distSQLSpecExecFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	plan, err := constructOpaque(metadata)
	if err != nil {
		return nil, err
	}
	physPlan, err := f.dsp.wrapPlan(f.getPlanCtx(cannotDistribute), plan)
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = planColumns(plan)
	return makePlanMaybePhysical(&physPlan, []planNode{plan}), nil
}

func (f *distSQLSpecExecFactory) ConstructDataStoreEngineInfo(m memo.RelExpr, object exec.Node) {
	_, ok := object.(DataStoreEngine)
	if ok {
		object.(DataStoreEngine).SetDataStoreEngine(m.GetDataStoreEngine())
	}
}

func (f *distSQLSpecExecFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: alter table split")
}

func (f *distSQLSpecExecFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: alter table relocate")
}

func (f *distSQLSpecExecFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: control jobs")
}

func (f *distSQLSpecExecFactory) ConstructCancelQueries(
	input exec.Node, ifExists bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: cancel queries")
}

func (f *distSQLSpecExecFactory) ConstructCancelSessions(
	input exec.Node, ifExists bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: cancel sessions")
}

func getPhysPlan(n exec.Node) (*PhysicalPlan, planMaybePhysical) {
	plan := n.(planMaybePhysical)
	return plan.physPlan.PhysicalPlan, plan
}

// checkExprsAndMaybeMergeLastStage is a helper method that returns a
// recommendation about exprs' distribution. If one of the expressions cannot
// be distributed, then all expressions cannot be distributed either. In such
// case if the last stage is distributed, this method takes care of merging the
// streams on the gateway node. physPlan may be nil.
func (f *distSQLSpecExecFactory) checkExprsAndMaybeMergeLastStage(
	exprs tree.TypedExprs, physPlan *PhysicalPlan,
) distRecommendation {
	// We recommend for exprs' distribution to be the same as the last stage
	// of processors (if there is such).
	recommendation := shouldDistribute
	if physPlan != nil && !physPlan.IsLastStageDistributed() {
		recommendation = shouldNotDistribute
	}
	for _, expr := range exprs {
		if err := checkExpr(expr); err != nil {
			recommendation = cannotDistribute
			if physPlan != nil {
				// The filter expression cannot be distributed, so we need to
				// make sure that there is a single stream on a node. We could
				// do so on one of the nodes that streams originate from, but
				// for now we choose the gateway.
				physPlan.EnsureSingleStreamOnGateway()
			}
			break
		}
	}
	return recommendation
}

// checkExpr verifies that an expression doesn't contain things that are not yet
// supported by distSQL, like distSQL-blocklisted functions.
func checkExpr(expr tree.Expr) error {
	if expr == nil {
		return nil
	}
	v := distSQLExprCheckVisitor{}
	tree.WalkExprConst(&v, expr)
	return v.err
}

// EnsureSingleStreamOnGateway ensures that there is only one stream on the
// gateway node in the plan (meaning it possibly merges multiple streams or
// brings a single stream from a remote node to the gateway).
func (p *PhysicalPlan) EnsureSingleStreamOnGateway() {
	// If we don't already have a single result router on the gateway, add a
	// single grouping stage.
	if len(p.ResultRouters) != 1 ||
		p.Processors[p.ResultRouters[0]].Node != p.GatewayNodeID {
		p.AddSingleGroupStage(
			p.GatewayNodeID,
			distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			distsqlpb.PostProcessSpec{},
			p.ResultTypes,
		)
		if len(p.ResultRouters) != 1 || p.Processors[p.ResultRouters[0]].Node != p.GatewayNodeID {
			panic("ensuring a single stream on the gateway failed")
		}
	}
}

func (f *distSQLSpecExecFactory) constructHashOrMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	mergeJoinOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	forceSide int,
	ParallelFlag bool,
) (exec.Node, error) {
	leftPhysPlan, leftPlan := getPhysPlan(left)
	rightPhysPlan, rightPlan := getPhysPlan(right)
	resultColumns := getJoinResultColumns(joinType, leftPhysPlan.ResultColumns, rightPhysPlan.ResultColumns)
	leftMap, rightMap := leftPhysPlan.PlanToStreamColMap, rightPhysPlan.PlanToStreamColMap
	helper := &joinPlanningHelper{
		numLeftOutCols:          len(leftPhysPlan.ResultTypes),
		numRightOutCols:         len(rightPhysPlan.ResultTypes),
		numAllLeftCols:          len(leftPhysPlan.ResultTypes),
		leftPlanToStreamColMap:  leftMap,
		rightPlanToStreamColMap: rightMap,
	}
	post, joinToStreamColMap := helper.joinOutColumns(joinType, resultColumns)
	// We always try to distribute the join, but planJoiners() itself might
	// decide not to.
	onExpr, err := helper.remapOnExpr(f.getPlanCtx(shouldDistribute), onCond)
	if err != nil {
		return nil, err
	}

	leftcols := make([]int, len(leftEqCols))
	rightcols := make([]int, len(rightEqCols))
	for i := range leftEqCols {
		leftcols[i] = int(leftEqCols[i])
	}
	for i := range rightEqCols {
		rightcols[i] = int(rightEqCols[i])
	}

	leftEqColsRemapped := eqCols(leftcols, leftMap)
	rightEqColsRemapped := eqCols(rightcols, rightMap)
	p := f.dsp.planJoiners(&joinPlanningInfo{
		leftPlan:              leftPhysPlan,
		rightPlan:             rightPhysPlan,
		joinType:              joinType,
		joinResultTypes:       getTypesFromResultColumns(resultColumns),
		onExpr:                onExpr,
		post:                  post,
		joinToStreamColMap:    joinToStreamColMap,
		leftEqCols:            leftEqColsRemapped,
		rightEqCols:           rightEqColsRemapped,
		leftEqColsAreKey:      leftEqColsAreKey,
		rightEqColsAreKey:     rightEqColsAreKey,
		leftMergeOrd:          distsqlOrdering(mergeJoinOrdering, leftEqColsRemapped),
		rightMergeOrd:         distsqlOrdering(mergeJoinOrdering, rightEqColsRemapped),
		leftPlanDistribution:  leftPhysPlan.Distribution,
		rightPlanDistribution: rightPhysPlan.Distribution,
		forceSide:             forceSide,
		ParallelFlag:          ParallelFlag,
	}, reqOrdering)
	p.ResultColumns = resultColumns
	return makePlanMaybePhysical(p, append(leftPlan.physPlan.planNodesToClose, rightPlan.physPlan.planNodesToClose...)), nil
}

func getEqualityIndicesAndMergeJoinOrdering(
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
) (
	leftEqualityIndices, rightEqualityIndices []exec.ColumnOrdinal,
	mergeJoinOrdering sqlbase.ColumnOrdering,
	err error,
) {
	n := len(leftOrdering)
	if n == 0 || len(rightOrdering) != n {
		return nil, nil, nil, errors.Errorf(
			"orderings from the left and right side must be the same non-zero length",
		)
	}
	leftEqualityIndices = make([]exec.ColumnOrdinal, n)
	rightEqualityIndices = make([]exec.ColumnOrdinal, n)
	for i := 0; i < n; i++ {
		leftColIdx, rightColIdx := leftOrdering[i].ColIdx, rightOrdering[i].ColIdx
		leftEqualityIndices[i] = exec.ColumnOrdinal(leftColIdx)
		rightEqualityIndices[i] = exec.ColumnOrdinal(rightColIdx)
	}

	mergeJoinOrdering = make(sqlbase.ColumnOrdering, n)
	for i := 0; i < n; i++ {
		// The mergeJoinOrdering "columns" are equality column indices.  Because of
		// the way we constructed the equality indices, the ordering will always be
		// 0,1,2,3..
		mergeJoinOrdering[i].ColIdx = i
		mergeJoinOrdering[i].Direction = leftOrdering[i].Direction
	}
	return leftEqualityIndices, rightEqualityIndices, mergeJoinOrdering, nil
}

func convertOrdinalsToInts(ordinals []exec.ColumnOrdinal) []int {
	ints := make([]int, len(ordinals))
	for i := range ordinals {
		ints[i] = int(ordinals[i])
	}
	return ints
}

func (f *distSQLSpecExecFactory) IsContainTrigger(table cat.Table) bool {
	tabDesc := table.(*optTable).desc
	trigger, err := MakeTriggerDesc(f.planner.extendedEvalCtx.Ctx(), f.planner.txn, tabDesc)
	if err != nil {
		return true
	}
	if trigger == nil || len(trigger.Triggers) == 0 {
		return false
	}
	for i := range trigger.Triggers {
		if trigger.Triggers[i].Tgtype&TriggerTypeDelete == TriggerTypeDelete {
			return true
		}
	}
	return false
}
