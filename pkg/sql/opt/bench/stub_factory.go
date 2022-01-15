// Copyright 2018  The Cockroach Authors.
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

package bench

import (
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/constraint"
	"github.com/znbasedb/znbase/pkg/sql/opt/exec"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/opt/props/physical"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// stubFactory is a do-nothing implementation of exec.Factory, used for testing.
type stubFactory struct{}

func (f *stubFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScan(
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
	return struct{}{}, nil
}

func (f *stubFactory) ConstructVirtualScan(table cat.Table) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.ColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructRender(
	n exec.Node,
	columns sqlbase.ResultColumns,
	exprs tree.TypedExprs,
	reqOrdering exec.OutputOrdering,
	functions []cat.UdrFunction,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
	forceSide int,
	ParallelFlag bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	leftBoundColMap opt.ColMap,
	memo *memo.Memo,
	rightProps *physical.Required,
	fakeRight exec.Node,
	right memo.RelExpr,
	onCond tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.ColumnOrdinal,
	orderedGroupCols exec.ColumnOrdinalSet,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
	groupColOrdering sqlbase.ColumnOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDistinct(
	input exec.Node, distinctCols, orderedCols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructIndexJoin(
	input exec.Node, table cat.Table, cols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructLookupJoin(
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
	return struct{}{}, nil
}

func (f *stubFactory) ConstructZigzagJoin(
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
	return struct{}{}, nil
}

func (f *stubFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructMax1Row(input exec.Node) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) RenameColumns(input exec.Node, colNames []string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructPlan(root exec.Node, subqueries []exec.Subquery) (exec.Plan, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExplainOpt(
	plan string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan exec.Plan,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertCols exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	rowsNeeded bool,
	upperOwner string,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructUpdate(
	input exec.Node,
	table cat.Table,
	fetchCols exec.ColumnOrdinalSet,
	updateCols exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	passthrough sqlbase.ResultColumns,
	rowsNeeded bool,
	upperOwner string,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructUpsert(
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
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.ColumnOrdinalSet,
	rowsNeeded bool,
	upperOwner string,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDeleteRange(
	table cat.Table, needed exec.ColumnOrdinalSet, indexConstraint *constraint.Constraint,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCreateView(
	schema cat.Schema,
	cv *tree.CreateView,
	columns sqlbase.ResultColumns,
	deps opt.ViewDeps,
	hasStar bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructBuffer(value exec.Node, label string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructWindow(n exec.Node, wi exec.WindowInfo) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScanBuffer(ref exec.Node, label string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string, unionAll bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSequenceSelect(seq cat.Sequence) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDataStoreEngineInfo(e memo.RelExpr, object exec.Node) {
	_, ok := object.(sql.DataStoreEngine)
	if ok {
		object.(sql.DataStoreEngine).SetDataStoreEngine(e.GetDataStoreEngine())
	}
}

func (f *stubFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCancelQueries(input exec.Node, ifExists bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCancelSessions(input exec.Node, ifExists bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) IsContainTrigger(table cat.Table) bool {
	return true
}
