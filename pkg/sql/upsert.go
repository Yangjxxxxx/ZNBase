// Copyright 2016  The Cockroach Authors.
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
	"strconv"
	"strings"
	"sync"

	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/transform"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

var upsertNodePool = sync.Pool{
	New: func() interface{} {
		return &upsertNode{}
	},
}

type upsertNode struct {
	source planNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run       upsertRun
	onlyMatch bool
	isMerge   bool
}

// upsertNode implements the autoCommitNode interface.
var _ autoCommitNode = &upsertNode{}

func (p *planner) walkMlistAndGetUpexpr(
	matchList tree.CondAndOpList,
) (tree.UpdateExprs, tree.NameList, error) {

	updExprs := make(tree.UpdateExprs, 0)
	names := make(tree.NameList, 0)
	for _, match := range matchList {
		for _, expr := range match.Operation {
			// ToDo need to consider all case of update Expr
			updExprs = append(updExprs, expr)
			names = append(names, expr.Names...)
		}
	}
	return updExprs, names, nil
}

func (p *planner) newUpsertNodeInMgMode(
	ctx context.Context,
	n *tree.Insert,
	desc *sqlbase.ImmutableTableDescriptor,
	ri row.Inserter,
	tn, alias *tree.TableName,
	sourceRows planNode,
	needRows bool,
	resultCols sqlbase.ResultColumns,
	defaultExprs []tree.TypedExpr,
	computeExprs []tree.TypedExpr,
	computedCols []sqlbase.ColumnDescriptor,
	fkTables row.FkTableMetadata,
	desiredTypes []types.T,
	canaryColNum int,
	addtionalColNum int,
	onlyMatch bool,
	isMerge bool,
) (res batchedPlanNode, err error) {
	// Extract the index that will detect upsert conflicts
	un := upsertNodePool.Get().(*upsertNode)
	*un = upsertNode{
		source:  sourceRows,
		columns: resultCols,
		run: upsertRun{
			checkHelper:  fkTables[desc.ID].CheckHelper,
			insertCols:   ri.InsertCols,
			defaultExprs: defaultExprs,
			computedCols: computedCols,
			computeExprs: computeExprs,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
				Cols:    desc.Columns,
				Mapping: ri.InsertColIDtoRowIndex,
			},
		},
		onlyMatch: onlyMatch,
		isMerge:   isMerge,
	}
	defer func() {
		// If anything below fails, we don't want to leak
		// resources. Ensure the thing is always closed and put back to
		// its alloc pool.
		if err != nil {
			un.Close(ctx)
		}
	}()
	_, names, err := p.walkMlistAndGetUpexpr(n.OnConflict.MatchList)
	if err != nil {
		return nil, err
	}
	newArr := make([]tree.Name, 0)
	for i := 0; i < len(names); i++ {
		repeat := false
		for j := i + 1; j < len(names); j++ {
			if names[i] == names[j] {
				repeat = true
				break
			}
		}
		if !repeat {
			newArr = append(newArr, names[i])
		}
	}
	names = newArr
	// Get updateColsDesc with updateExpr in match list
	updateCols, err := p.processColumns(desc, names,
		false /* ensureColumns */, true /* allowMutations */)
	if err != nil {
		return nil, err
	}

	// sort updateCols according to columnID
	for i := 0; i < len(updateCols); i++ {
		flag := false
		for j := 0; j < len(updateCols)-i-1; j++ {
			if updateCols[j].ID > updateCols[j+1].ID {
				updateCols[j], updateCols[j+1] = updateCols[j+1], updateCols[j]
				flag = true
			}
		}
		if !flag {
			break
		}
	}

	// Create the table update
	ru, err := row.MakeUpdater(
		p.txn,
		nil,
		ri.Helper.TableDesc,
		fkTables,
		updateCols,
		ri.Helper.TableDesc.Columns,
		row.UpdaterDefault,
		p.EvalContext(),
		&p.alloc,
	)
	if err != nil {
		return nil, err
	}

	un.run.tw = &optTableUpserter{
		tableUpserterBase: tableUpserterBase{
			ri:    ri,
			alloc: &p.alloc,
		},
		canaryOrdinal:     canaryColNum,
		additionalOrdinal: addtionalColNum,
		fkTables:          fkTables,
		fetchCols:         ri.Helper.TableDesc.Columns,
		updateCols:        updateCols,
		ru:                ru,
		onlyMatch:         onlyMatch,
	}
	return un, err
}

func (p *planner) newUpsertNode(
	ctx context.Context,
	n *tree.Insert,
	desc *sqlbase.ImmutableTableDescriptor,
	ri row.Inserter,
	tn, alias *tree.TableName,
	sourceRows planNode,
	needRows bool,
	resultCols sqlbase.ResultColumns,
	defaultExprs []tree.TypedExpr,
	computeExprs []tree.TypedExpr,
	computedCols []sqlbase.ColumnDescriptor,
	fkTables row.FkTableMetadata,
	desiredTypes []types.T,
	canaryColNum int,
	addtionalColNum int,
	onlyMatch bool,
) (res batchedPlanNode, err error) {

	if n.OnConflict.IsMergeStmt() {
		// merge into mode
		return p.newUpsertNodeInMgMode(ctx, n, desc, ri, tn, alias, sourceRows, needRows, resultCols, defaultExprs,
			computeExprs, computedCols, fkTables, desiredTypes, canaryColNum, addtionalColNum, onlyMatch, true)

	}
	// Extract the index that will detect upsert conflicts
	// (conflictIndex) and the assignment expressions to use when
	// conflicts are detected (updateExprs).
	autoGenUpdates, updateExprs, conflictIndex, err := upsertExprsAndIndex(desc, *n.OnConflict, ri.InsertCols)
	if err != nil {
		return nil, err
	}
	// Instantiate the upsert node.
	un := upsertNodePool.Get().(*upsertNode)
	*un = upsertNode{
		source:  sourceRows,
		columns: resultCols,
		run: upsertRun{
			checkHelper:  fkTables[desc.ID].CheckHelper,
			insertCols:   ri.InsertCols,
			defaultExprs: defaultExprs,
			computedCols: computedCols,
			computeExprs: computeExprs,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
				Cols:    desc.Columns,
				Mapping: ri.InsertColIDtoRowIndex,
			},
		},
	}
	defer func() {
		// If anything below fails, we don't want to leak
		// resources. Ensure the thing is always closed and put back to
		// its alloc pool.
		if err != nil {
			un.Close(ctx)
		}
	}()

	if n.OnConflict.DoNothing {
		if conflictIndex == nil {
			un.run.tw = &strictTableUpserter{
				tableUpserterBase: tableUpserterBase{
					ri:          ri,
					collectRows: needRows,
					alloc:       &p.alloc,
				},
			}
		} else {
			un.run.tw = &tableUpserter{
				conflictIndex: *conflictIndex,
				tableUpserterBase: tableUpserterBase{
					ri:          ri,
					collectRows: needRows,
					alloc:       &p.alloc,
				},
			}
		}
	} else {
		// We're going to work on allocating an upsertHelper here, even
		// though it might end up not being used below in this fast
		// path. This is because this also performs a semantic check, that
		// the upsert column references are not ambiguous (in particular,
		// this rejects attempts to upsert into a table called "excluded"
		// without an AS clause).

		// Determine which columns are updated by the RHS of INSERT
		// ... ON CONFLICT DO UPDATE, or the non-PK columns in an
		// UPSERT.

		names, newUpdateExprs, err := p.namesForExprs(ctx, updateExprs)
		if err != nil {
			return nil, err
		}
		// namesForExprs is responsible for re-shaping the SET RHS, we
		// need to use its result as new input for newUpsertHelper()
		// below.
		updateExprs = newUpdateExprs

		// We use ensureColumns = false in processColumns, because
		// updateCols may be legitimately empty (when there is no DO
		// UPDATE clause).
		//
		// If the SET expressions were automatically generated (UPSERT
		// alias) then we are assigning all columns and we want also
		// to include mutation columns being added. If the SET
		// expressions were explicit (specified by the client),
		// then we want to reject assignments to mutation columns.
		updateCols, err := p.processColumns(desc, names,
			false /* ensureColumns */, autoGenUpdates /* allowMutations */)
		if err != nil {
			return nil, err
		}

		// Ensure that the user cannot include computed columns
		// in the list of columns explicitly updated.
		if err := checkHasNoComputedCols(updateCols); err != nil {
			return nil, err
		}

		// We also need to include any computed columns in the set of UpdateCols.
		// They can't have been set explicitly so there's no chance of
		// double-including a computed column.
		updateCols = append(updateCols, computedCols...)

		// Instantiate the helper that will take over the evaluation of
		// SQL expressions. As described above, this also performs a
		// semantic check, so it cannot be skipped on the fast path below.
		helper, err := p.newUpsertHelper(
			ctx, tn, desc,
			ri.InsertCols,
			updateCols,
			updateExprs,
			computeExprs,
			conflictIndex,
			n.OnConflict.Where,
		)
		if err != nil {
			return nil, err
		}

		// Determine whether to use the fast path or the slow path.
		// TODO(dan): The fast path is currently only enabled when the UPSERT alias
		// is explicitly selected by the user. It's possible to fast path some
		// queries of the form INSERT ... ON CONFLICT, but the utility is low and
		// there are lots of edge cases (that caused real correctness bugs #13437
		// #13962). As a result, we've decided to remove this until after 1.0 and
		// re-enable it then. See #14482.
		enableFastPath := n.OnConflict.IsUpsertAlias() &&
			// Tables with secondary indexes are not eligible for fast path (it
			// would be easy to add the new secondary index entry but we can't clean
			// up the old one without the previous values).
			len(desc.Indexes) == 0 &&
			// When adding or removing a column in a schema change (mutation), the user
			// can't specify it, which means we need to do a lookup and so we can't use
			// the fast path. When adding or removing an index, same result, so the fast
			// path is disabled during all mutations.
			len(desc.MutationColumns()) == 0 &&
			len(desc.MutationIndexes()) == 0 &&
			// For the fast path, all columns must be specified in the insert.
			len(ri.InsertCols) == len(desc.Columns) &&
			// We cannot use the fast path if we also have a RETURNING clause, because
			// RETURNING wants to see only the updated rows.
			!needRows

		if enableFastPath {
			// We then use the super-simple, super-fast writer. There's not
			// much else to prepare.
			un.run.tw = &fastTableUpserter{
				tableUpserterBase: tableUpserterBase{
					ri: ri,
				},
			}
		} else {
			// General/slow path.
			un.run.tw = &tableUpserter{
				tableUpserterBase: tableUpserterBase{
					ri:          ri,
					alloc:       &p.alloc,
					collectRows: needRows,
				},
				anyComputed:   len(computeExprs) >= 0,
				fkTables:      fkTables,
				updateCols:    updateCols,
				conflictIndex: *conflictIndex,
				evaler:        helper,
			}
		}
	}

	return un, nil
}

func (p *planner) constructCaseExpr(
	ctx context.Context, join tree.JoinTableExpr, ins *tree.Insert,
) (project tree.SelectExprs, canaryColNum int, addtionalColNum int, onlyMatch bool, err error) {
	targetTab, sourceTab := join.Right, join.Left
	var targetDesc, sourceDesc *ImmutableTableDescriptor
	var targetAlias *tree.TableName
	var targetTableName string
	tn, targetAlias, err := p.getAliasedTableName(targetTab)

	onlyMatch = ins.OnConflict.IsMergeStmt() && ins.OnConflict.MatchList != nil && ins.OnConflict.NotMatchList == nil
	selectExprs := make(tree.SelectExprs, 0)
	if err != nil {
		return nil, -1, -1, onlyMatch, err
	}
	targetDesc, err = ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, -1, -1, onlyMatch, err
	}
	primaryIndex := &targetDesc.PrimaryIndex
	columnNames := primaryIndex.ColumnNames
	var columnNameMorNm string
	hasPrimaryKey := true
	for _, name := range columnNames {
		col, _, _ := targetDesc.FindColumnByName(tree.Name(name))
		if col.Nullable == false {
			columnNameMorNm = name
			if col.IsRowIDColumn() {
				hasPrimaryKey = false
			}
			break
		}
	}

	tn, _, err = p.getAliasedTableName(sourceTab)
	if err != nil {
		return nil, -1, -1, onlyMatch, err
	}

	sourceDesc, err = ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, -1, -1, onlyMatch, err
	}
	// get table name
	if targetAlias != nil {
		targetTableName = string(targetAlias.TableName)
	} else {
		targetTableName = targetDesc.GetName()
	}

	//if sourceAlias != nil {
	//	sourceTableName = string(sourceAlias.TableName)
	//} else {
	//	sourceTableName = sourceDesc.GetName()
	//}

	addSelectExpr := func(name string, tableName string, expr tree.Expr, notMatchCond tree.Expr) {
		when := &tree.When{
			Cond: notMatchCond,
		}
		whenSlice := make([]*tree.When, 0)
		whenSlice = append(whenSlice, when)
		rootCaseExp := tree.CaseExpr{
			Expr:  nil,
			Whens: whenSlice,
		}

		rootCaseExp.Whens[0].Val = expr
		rootCaseExp.Else = &tree.UnresolvedName{
			NumParts: 2,
			Star:     false,
			Parts:    [4]string{name, tableName},
		}
		selectExpr := tree.SelectExpr{
			Expr: &rootCaseExp,
		}
		selectExprs = append(selectExprs, selectExpr)
	}

	if sourceDesc != nil && targetDesc != nil {
		notMatchCond := &tree.ComparisonExpr{
			Operator: tree.IsNotDistinctFrom,
			Left: &tree.UnresolvedName{
				NumParts: 2,
				Star:     false,
				Parts:    [4]string{columnNameMorNm, targetTableName},
			},
			Right: tree.DNull,
		}
		// in order to return error normally, check merge-match here
		if _, errorMatch := ins.OnConflict.GetUnionColListOfMatch(); errorMatch != nil {
			return nil, -1, -1, onlyMatch, errorMatch
		}
		// first build not match/insert cols caseExpr
		colNames := make([]tree.Name, 0)
		for _, p := range targetDesc.Columns {
			colNames = append(colNames, tree.Name(p.Name))
		}
		var notMatchCols []tree.Name
		if onlyMatch {
			// 将满足notMatch的数据均传入到执行阶段, 在执行阶段通过onlyMatch的值决定插入与否
			for _, column := range targetDesc.Columns {
				if !hasPrimaryKey && column.Name == columnNameMorNm {
					continue
				}
				notMatchCols = append(notMatchCols, tree.Name(column.Name))
			}
		} else {
			var errNotMatch error
			notMatchCols, errNotMatch = ins.OnConflict.GetUnionColsOfNotMatch(colNames, columnNameMorNm, hasPrimaryKey)
			if errNotMatch != nil {
				return nil, -1, -1, onlyMatch, errNotMatch
			}
		}
		for i := 0; i < len(targetDesc.Columns); i++ {
			names := targetDesc.GetColumns()
			name := names[i].ColName()
			// if this column is not exist in notMatchCols, just skip it
			flag := false
			for _, j := range notMatchCols {
				if name == j {
					flag = true
					break
				}
			}
			if !flag {
				//construct NULL expr
				if !hasPrimaryKey && string(name) == columnNameMorNm {
					addSelectExpr(string(name), targetTableName, uniqueRowIDExpr, notMatchCond)
				} else {
					addSelectExpr(string(name), targetTableName, tree.DNull, notMatchCond)
				}
				continue
			}
			expr, _ := ins.OnConflict.ConstructCaseExprByColName(name, i, targetTableName, false, true, targetDesc.Columns[i].Type.ToDatumType())
			addSelectExpr(string(name), targetTableName, expr, notMatchCond)
		}
		// second build fetchCols
		for i := 0; i < len(targetDesc.Columns); i++ {
			names := targetDesc.GetColumns()
			name := names[i].ColName()
			unResolvedExp := &tree.UnresolvedName{
				NumParts: 2,
				Star:     false,
				Parts:    [4]string{string(name), targetTableName},
			}
			selectExpr := tree.SelectExpr{
				Expr: unResolvedExp,
			}
			selectExprs = append(selectExprs, selectExpr)
		}
		// third build updateCols
		for i := 0; i < len(targetDesc.Columns); i++ {
			if targetDesc.Columns[i].Name == columnNameMorNm {
				canaryColNum = i + len(sourceDesc.Columns)
			}
			names := targetDesc.GetColumns()
			name := names[i].ColName()
			expr, _ := ins.OnConflict.ConstructCaseExprByColName(name, i, targetTableName, true, false, nil)
			if expr == nil {
				continue
			}
			addSelectExpr(string(name), targetTableName, expr, ins.OnConflict.Condition.Expr)
		}
		// build additionalExpr for additionalCols at last
		expr, errAdditionalCol := ins.OnConflict.ConstructCaseExprOfAdditionalCol(targetTableName, columnNameMorNm)
		if errAdditionalCol != nil {
			return nil, -1, -1, onlyMatch, errAdditionalCol
		}
		selectExpr := tree.SelectExpr{
			Expr: expr,
		}
		selectExprs = append(selectExprs, selectExpr)
		return selectExprs, canaryColNum, len(selectExprs) - 1, onlyMatch, nil
	}
	return tree.SelectExprs{}, -1, -1, onlyMatch, pgerror.NewErrorf(pgcode.InvalidName, "Merge Stmt source table/ target table should not be nil!")

}

func (p *planner) constructJoinNode(
	ctx context.Context, ins *tree.Insert,
) (planNode, int, int, bool, error) {
	/* construct select caluse */
	var sourceTab, targetTab tree.TableExpr
	/* left source table - source table */
	if selectClause, ok := ins.Rows.Select.(*tree.SelectClause); ok {
		if len(selectClause.From.Tables) != 1 {
			return nil, -1, -1, false, pgerror.NewErrorf(pgcode.InvalidColumnReference,
				"Merge Stmt using table should only be one")
		}
		sourceTab = selectClause.From.Tables[0]

	}
	targetTab = ins.Table
	joinExpr := tree.JoinTableExpr{
		JoinType: "LEFT",
		Left:     sourceTab,
		Right:    targetTab,
		Cond:     ins.OnConflict.Condition,
	}
	// Todo construct caseExpr for eacho select project
	// insertCols || fetchCols || updateCols || matchCols || addtionalCols

	exprs, canaryColNum, addtionalColNum, onlyMatch, err := p.constructCaseExpr(ctx, joinExpr, ins)
	if err != nil {
		return nil, canaryColNum, addtionalColNum, onlyMatch, err
	}

	// construct select expr by match or not match list
	newSelectClause := tree.SelectClause{
		//Exprs: tree.SelectExprs{tree.StarSelectExpr()},
		Exprs: exprs,
		From: &tree.From{
			Tables: tree.TableExprs{
				&joinExpr,
			},
		},
	}
	node, err := p.newPlan(ctx, &newSelectClause, nil)
	return node, canaryColNum, addtionalColNum, onlyMatch, err
}

// upsertRun contains the run-time state of upsertNode during local execution.
type upsertRun struct {
	// In contrast with the run part of insert/delete/update, the
	// upsertRun has a tableWriter interface reference instead of
	// embedding a direct struct because it can run with either the
	// fastTableUpdater or the regular tableUpdater.
	tw          batchedTableWriter
	checkHelper *sqlbase.CheckHelper

	// insertCols are the columns being inserted/upserted into.
	insertCols []sqlbase.ColumnDescriptor

	// defaultExprs are the expressions used to generate default values.
	defaultExprs []tree.TypedExpr

	// computedCols are the columns that need to be (re-)computed as
	// the result of computing some of the source rows prior to the upsert.
	computedCols []sqlbase.ColumnDescriptor
	// computeExprs are the expressions to evaluate to re-compute the
	// columns in computedCols.
	computeExprs []tree.TypedExpr
	// iVarContainerForComputedCols is used as a temporary buffer that
	// holds the updated values for every column in the source, to
	// serve as input for indexed vars contained in the computeExprs.
	iVarContainerForComputedCols sqlbase.RowIndexedVarContainer

	// done informs a new call to BatchedNext() that the previous call to
	// BatchedNext() has completed the work already.
	done bool

	// traceKV caches the current KV tracing flag.
	traceKV bool

	checkOrds checkSet
}

func (n *upsertNode) startExec(params runParams) error {
	// cache traceKV during execution, to avoid re-evaluating it for every row.
	n.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	return n.run.tw.init(params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *upsertNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *upsertNode) Values() tree.Datums { panic("not valid") }

// maxUpsertBatchSize is the max number of entries in the KV batch for
// the upsert operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxUpsertBatchSize = 10000

func getColIndexByID(fetchScope []sqlbase.ColumnDescriptor, col sqlbase.ColumnID) int {
	for i := range fetchScope {
		if fetchScope[i].ID == col {
			return i
		}
	}
	return -1
}

func findInNewRows(vecNewRows []string, sNewRow string) int {
	for i := range vecNewRows {
		if vecNewRows[i] == sNewRow {
			return i
		}
	}
	return -1
}

func findIndexInRows(updateRow tree.Datums, vecIndex []string, step int) int {
	for i, index := range vecIndex {
		flag := true
		sIndexs := strings.Split(index, ";")
		for _, sIndex := range sIndexs {
			nIndex, _ := strconv.Atoi(sIndex)
			if updateRow[nIndex].String() != updateRow[nIndex+step].String() {
				flag = false
				break
			}
		}
		if flag {
			return i
		}
	}
	return -1
}

// BatchedNext implements the batchedPlanNode interface.
func (n *upsertNode) BatchedNext(params runParams) (bool, error) {
	if n.run.done {
		return false, nil
	}

	tracing.AnnotateTrace()

	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	flag := false
	// on duplicate key update
	if insert, ok := params.p.stmt.Statement.AST.(*tree.Insert); ok {
		if insert.OnConflict.IsDuplicate {
			flag = true
			vecNewRows := make([]string, 0)
			vecUpRows := make([]tree.Datums, 0)
			vecIndex := make([]string, 0) // 索引数据列表
			sPrimary := bytes.Buffer{}
			// 添加主键数据
			for _, col := range params.p.tableTrigger.TableDesc().PrimaryIndex.ColumnIDs {
				colIndex := getColIndexByID(n.run.insertCols, col)
				if len(sPrimary.String()) == 0 {
					sPrimary.WriteString(strconv.Itoa(colIndex))
				} else {
					sPrimary.WriteString(";")
					sPrimary.WriteString(strconv.Itoa(colIndex))
				}
			}
			vecIndex = append(vecIndex, sPrimary.String())
			// 添加唯一键数据
			for _, index := range params.p.tableTrigger.TableDesc().Indexes {
				sIndex := bytes.Buffer{}
				for _, col := range index.ColumnIDs {
					colIndex := getColIndexByID(n.run.insertCols, col)
					if len(sIndex.String()) == 0 {
						sIndex.WriteString(strconv.Itoa(colIndex))
					} else {
						sIndex.WriteString(";")
						sIndex.WriteString(strconv.Itoa(colIndex))
					}
				}
				vecIndex = append(vecIndex, sIndex.String())
			}
			// 处理数据
			upColCount := 0
			if tw, ok := n.run.tw.(*optTableUpserter); ok {
				upColCount = len(tw.updateCols) // 获取更新的列数
			}
			for {
				if next, err := n.source.Next(params); !next {
					lastBatch = true
					if err != nil {
						return false, err
					}
					break
				}

				var updateRow tree.Datums
				for i := range n.source.Values() {
					updateRow = append(updateRow, n.source.Values()[i])
				}
				// updateRow := n.source.Values() // 获取更新的数据与原始数据
				var newRows tree.Datums
				for i := 0; i < (len(updateRow)-upColCount-1)/2; i++ {
					newRows = append(newRows, updateRow[i])
				}
				sNewRows := bytes.Buffer{}
				for _, row := range newRows {
					sNewRows.WriteString(row.String())
					sNewRows.WriteString(";")
				}
				rowNum := findInNewRows(vecNewRows, sNewRows.String())
				if (len(vecNewRows) == 0 && len(vecUpRows) == 0) ||
					rowNum == -1 {
					vecNewRows = append(vecNewRows, sNewRows.String())
					vecUpRows = append(vecUpRows, updateRow)
				} else if rowNum > -1 {
					iOrigin := findIndexInRows(vecUpRows[rowNum], vecIndex, len(newRows))
					iConflict := findIndexInRows(updateRow, vecIndex, len(newRows))
					if iOrigin == -1 || iConflict == -1 {
						continue
					}
					if iConflict < iOrigin {
						vecUpRows[rowNum] = updateRow
					}
				}
			}

			// 更新数据
			for i := range vecUpRows {
				if err := n.processSourceRow(params, vecUpRows[i]); err != nil {
					return false, err
				}
				if n.run.tw.curBatchSize() >= maxUpsertBatchSize {
					break
				}
			}
		}
	}

	if !flag {
		for {
			if err := params.p.cancelChecker.Check(); err != nil {
				return false, err
			}

			// Advance one individual row.
			if next, err := n.source.Next(params); !next {
				lastBatch = true
				if err != nil {
					return false, err
				}
				break
			}

			// Process the insertion for the current source row, potentially
			// accumulating the result row for later.
			if err := n.processSourceRow(params, n.source.Values()); err != nil {
				return false, err
			}

			// Are we done yet with the current batch?
			if n.run.tw.curBatchSize() >= maxUpsertBatchSize {
				break
			}
		}
	}

	// In Upsert, curBatchSize indicates whether "there is still work to do in this batch".
	batchSize := n.run.tw.curBatchSize()

	if batchSize > 0 {
		if err := n.run.tw.atBatchEnd(params.ctx, n.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := n.run.tw.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := n.run.tw.finalize(params.ctx, params, n.run.traceKV, nil); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		n.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		n.run.tw.tableDesc().ID,
		n.run.tw.batchedCount(),
	)

	return n.run.tw.batchedCount() > 0, nil
}

// processSourceRow processes one row from the source for upsertion.
// The table writer is in charge of accumulating the result rows.
func (n *upsertNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// Process the incoming row tuple and generate the full inserted
	// row. This fills in the defaults, computes computed columns, and
	// checks the data width complies with the schema constraints.
	if n.isMerge && sourceVals[len(sourceVals)-1].String() == "3" {
		return nil
	}
	rowVals, err := GenerateInsertRow(
		n.run.defaultExprs,
		n.run.computeExprs,
		n.run.insertCols,
		n.run.computedCols,
		*params.EvalContext().Copy(),
		n.run.tw.tableDesc(),
		sourceVals,
		&n.run.iVarContainerForComputedCols,
	)
	if err != nil {
		return err
	}

	// Create a set of partial index IDs to not add or remove entries from.
	var pm schemaexpr.PartialIndexUpdateHelper
	partialIndexOrds := n.run.tw.tableDesc().PartialIndexOrds()
	if !partialIndexOrds.Empty() {
		partialIndexValOffset := len(n.run.insertCols) + len(n.run.tw.(*optTableUpserter).fetchCols) + len(n.run.tw.(*optTableUpserter).fetchCols) + n.run.checkOrds.Len()
		//if n.run.tw.(*optTableUpserter).canaryOrdinal != -1 {
		//	partialIndexValOffset++
		//}
		partialIndexVals := rowVals[partialIndexValOffset:]
		partialIndexPutVals := partialIndexVals[len(partialIndexVals)/2:]
		partialIndexDelVals := partialIndexVals[:len(partialIndexVals)/2]

		err := pm.Init(partialIndexPutVals, partialIndexDelVals, n.run.tw.tableDesc())
		if err != nil {
			return err
		}

		// Truncate rowVals so that it no longer includes partial index predicate
		// values.
		rowVals = rowVals[:partialIndexValOffset]
	}

	// Run the CHECK constraints, if any. CheckHelper will either evaluate the
	// constraints itself, or else inspect boolean columns from the input that
	// contain the results of evaluation.
	if n.run.checkHelper != nil {
		if n.run.checkHelper.NeedsEval() {
			insertColIDtoRowIndex := n.run.iVarContainerForComputedCols.Mapping
			if err := n.run.checkHelper.LoadEvalRow(insertColIDtoRowIndex, rowVals, false); err != nil {
				return err
			}
			if err := n.run.checkHelper.CheckEval(params.EvalContext()); err != nil {
				return err
			}
		} else {
			checkVals := sourceVals[len(sourceVals)-n.run.checkHelper.Count():]
			if err := n.run.checkHelper.CheckInput(checkVals); err != nil {
				return err
			}
		}
	}

	// Find all the function index expression in the table.
	var exprString []string
	tabDesc := n.run.tw.tableDesc()
	if tabDesc.Indexes != nil {
		for _, index := range tabDesc.Indexes {
			if index.IsFunc != nil {
				for i, isFunc := range index.IsFunc {
					if isFunc {
						exprString = append(exprString, index.ColumnNames[i])
					}
				}
			}
		}
	}

	// Calculate the value of the function expression and add the result to
	// the rowVals array.
	if exprString != nil {
		var txCtx transform.ExprTransformContext
		typedExprs, err := sqlbase.MakeFuncExprs(exprString, tabDesc, tree.NewUnqualifiedTableName(tree.Name(tabDesc.Name)),
			&txCtx, params.EvalContext(), true)
		if err != nil {
			return err
		}
		oldValues := rowVals[len(tabDesc.Columns) : len(tabDesc.Columns)*2]
		var newFuncResults []tree.Datum
		for _, typedExpr := range typedExprs {
			var idxes []int
			for i := 0; i < len(typedExpr.(*tree.FuncExpr).Exprs); i++ {
				if _, ok := typedExpr.(*tree.FuncExpr).Exprs[i].(*tree.IndexedVar); !ok {
					continue
				}
				idx := typedExpr.(*tree.FuncExpr).Exprs[i].(*tree.IndexedVar).Idx
				idxes = append(idxes, idx)
				typedExpr.(*tree.FuncExpr).Exprs[i] = oldValues[idx]
			}
			funcRes, err := typedExpr.Eval(params.EvalContext())
			if err != nil {
				return err
			}
			rowVals = append(rowVals, funcRes)
			if idxes != nil {
				for i := 0; i < len(typedExpr.(*tree.FuncExpr).Exprs); i++ {
					idx := idxes[i]
					typedExpr.(*tree.FuncExpr).Exprs[i] = rowVals[idx]
				}
			}
			newFuncRes, err := typedExpr.Eval(params.EvalContext())
			if err != nil {
				return err
			}
			newFuncResults = append(newFuncResults, newFuncRes)
		}
		rowVals = append(rowVals, newFuncResults...)
	}

	// Process the row. This is also where the tableWriter will accumulate
	// the row for later.
	return n.run.tw.row(params.ctx, rowVals, n.run.traceKV, pm)
}

// BatchedCount implements the batchedPlanNode interface.
func (n *upsertNode) BatchedCount() int { return n.run.tw.batchedCount() }

// BatchedCount implements the batchedPlanNode interface.
func (n *upsertNode) BatchedValues(rowIdx int) tree.Datums { return n.run.tw.batchedValues(rowIdx) }

func (n *upsertNode) Close(ctx context.Context) {
	n.source.Close(ctx)
	if n.run.tw != nil {
		n.run.tw.close(ctx)
	}
	*n = upsertNode{}
	upsertNodePool.Put(n)
}

// enableAutoCommit is part of the autoCommitNode interface.
func (n *upsertNode) enableAutoCommit() {
	n.run.tw.enableAutoCommit()
}

// upsertExcludedTable is the name of a synthetic table used in an upsert's set
// expressions to refer to the values that would be inserted for a row if it
// didn't conflict.
// Example: `INSERT INTO kv VALUES (1, 2) ON CONFLICT (k) DO UPDATE SET v = excluded.v`
var upsertExcludedTable = tree.MakeUnqualifiedTableName("excluded")

// upsertHelper is the helper struct in charge of evaluating SQL
// expression during an UPSERT. Its main responsibilities are:
//
// - eval(): compute the UPDATE statements given the previous and new values,
//   for INSERT ... ON CONFLICT DO UPDATE SET ...
//
// - evalComputedCols(): evaluate and add the computed columns
//   to the current upserted row.
//
// - shouldUpdate(): evaluates the WHERE clause of an ON CONFLICT
//   ... DO UPDATE clause.
//
// See the tableUpsertEvaler interface definition in tablewriter.go.
type upsertHelper struct {
	// p is used to provide an evalContext.
	p *planner

	// evalExprs contains the individual columns of SET RHS in an ON
	// CONFLICT DO UPDATE clause.
	// This decomposes tuple assignments, e.g. DO UPDATE SET (a,b) = (x,y)
	// into their individual expressions, in this case x, y.
	//
	// Its entries correspond 1-to-1 to the columns in
	// tableUpserter.updateCols.
	//
	// This is the main input for eval().
	evalExprs []tree.TypedExpr

	// whereExpr is the RHS of an ON CONFLICT DO UPDATE SET ... WHERE <expr>.
	// This is the main input for shouldUpdate().
	whereExpr tree.TypedExpr

	// sourceInfo describes the columns provided by the table being
	// upserted into.
	sourceInfo *sqlbase.DataSourceInfo
	// excludedSourceInfo describes the columns provided by the values
	// being upserted. This may be fewer columns than the
	// table, for example:
	// INSERT INTO kv(k) VALUES (3) ON CONFLICT(k) DO UPDATE SET v = excluded.v;
	//        invalid, value v is not part of the source in VALUES   ^^^^^^^^^^
	excludedSourceInfo *sqlbase.DataSourceInfo

	// curSourceRow buffers the current values from the table during
	// an upsert conflict. Used as input when evaluating
	// evalExprs and whereExpr.
	curSourceRow tree.Datums
	// curExcludedRow buffers the current values being upserted
	// during an upsert conflict. Used as input when evaluating
	// evalExprs and whereExpr.
	curExcludedRow tree.Datums

	// computeExprs is the list of expressions to (re-)compute computed
	// columns.
	// This is the main input for evalComputedCols().
	computeExprs []tree.TypedExpr

	// ccIvarContainer buffers the current values after the upsert
	// conflict resolution, to serve as input while evaluating
	// computeExprs.
	ccIvarContainer sqlbase.RowIndexedVarContainer

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy

	IsInsertOrUpdate bool
}

var _ tableUpsertEvaler = (*upsertHelper)(nil)

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.curExcludedRow[idx-numSourceColumns].Eval(ctx)
	}
	return uh.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarResolvedType(idx int) types.T {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.excludedSourceInfo.SourceColumns[idx-numSourceColumns].Typ
	}
	return uh.sourceInfo.SourceColumns[idx].Typ
}

// GetVisibleType implements the parser.IndexedVarContainer interface.
func (uh *upsertHelper) GetVisibleType(idx int) string {
	return ""
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.excludedSourceInfo.NodeFormatter(idx - numSourceColumns)
	}
	return uh.sourceInfo.NodeFormatter(idx)
}

// SetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (uh *upsertHelper) SetForInsertOrUpdate(b bool) {
	uh.IsInsertOrUpdate = b
}

// GetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (uh *upsertHelper) GetForInsertOrUpdate() bool {
	return uh.IsInsertOrUpdate
}

// newUpsertHelper instantiates an upsertHelper based on the extracted
// metadata.
// It needs to prepare three things:
// - the RHS expressions of UPDATE SET ... (evalExprs)
// - the UPDATE WHERE clause, if any (whereExpr)
// - the computed expressions, if any (computeExprs).
func (p *planner) newUpsertHelper(
	ctx context.Context,
	tn *tree.TableName,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	insertCols []sqlbase.ColumnDescriptor,
	updateCols []sqlbase.ColumnDescriptor,
	updateExprs tree.UpdateExprs,
	computeExprs []tree.TypedExpr,
	upsertConflictIndex *sqlbase.IndexDescriptor,
	whereClause *tree.Where,
) (*upsertHelper, error) {
	// Extract the parsed default expressions for every updated
	// column. This populates nil for every entry in updateCols which
	// has no default.
	defaultExprs, err := sqlbase.MakeDefaultExprs(
		updateCols, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// Flatten any tuple assignment in the UPDATE SET clause(s).
	//
	// For example, if we have two tuple assignments like SET (a,b) =
	// (c,d), (x,y) = (u,v), we turn this into four expressions
	// [c,d,u,v] in untupledExprs.
	//
	// If any of the RHS expressions is the special DEFAULT keyword,
	// we substitute the default expression computed above.
	//
	// This works because the LHS of each SET clause has already been
	// untupled by the caller, so that for the example above updateCols
	// contains the metadata for [a,b,c,d] already, so there's always
	// exactly one entry in defaultExprs for the untuplification to
	// find.
	//
	// However we need to be careful when the RHS is not a tuple
	// constructor, for example it is a tuple-returning unary expression
	// or a subquery.
	untupledExprs := make(tree.Exprs, 0, len(updateExprs))
	i := 0
	for _, updateExpr := range updateExprs {
		if updateExpr.Tuple {
			if t, ok := updateExpr.Expr.(*tree.Tuple); ok {
				for _, e := range t.Exprs {
					e = fillDefault(e, i, defaultExprs)
					untupledExprs = append(untupledExprs, e)
					i++
				}
			} else {
				return nil, pgerror.UnimplementedWithIssueErrorf(8330,
					"cannot use this type of expression on the right of UPDATE SET: %s", updateExpr.Expr)
			}
		} else {
			e := fillDefault(updateExpr.Expr, i, defaultExprs)
			untupledExprs = append(untupledExprs, e)
			i++
		}
	}

	// At this point, the entries in updateCols and untupledExprs match
	// 1-to-1.

	// Start preparing the helper.
	//
	// We need to allocate early because the ivarHelper below needs a
	// heap reference to inject in the resolved indexed vars.
	helper := &upsertHelper{p: p, computeExprs: computeExprs}

	// Now on to analyze the evalExprs and the whereExpr.  These can
	// refer to both the original table and the upserted values, so they
	// need a double dataSourceInfo.

	// sourceInfo describes the columns provided by the table.
	helper.sourceInfo = sqlbase.NewSourceInfoForSingleTable(
		*tn, sqlbase.ResultColumnsFromColDescs(tableDesc.Columns),
	)
	// excludedSourceInfo describes the columns provided by the
	// insert/upsert data source. This will be used to resolve
	// expressions of the form `excluded.x`, which refer to the values
	// coming from the data source.
	helper.excludedSourceInfo = sqlbase.NewSourceInfoForSingleTable(
		upsertExcludedTable, sqlbase.ResultColumnsFromColDescs(insertCols),
	)

	// Name resolution needs a multi-source which knows both about the
	// table being upsert into which contains values pre-upsert, and a
	// pseudo-table "excluded" which will contain values from the upsert
	// data source.
	sources := sqlbase.MultiSourceInfo{helper.sourceInfo, helper.excludedSourceInfo}

	// Prepare the ivarHelper which connects the indexed vars
	// back to this helper.
	ivarHelper := tree.MakeIndexedVarHelper(helper,
		len(helper.sourceInfo.SourceColumns)+len(helper.excludedSourceInfo.SourceColumns))

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer p.semaCtx.Properties.Restore(p.semaCtx.Properties)

	// Ensure there are no special functions in the clause.
	p.semaCtx.Properties.Require("ON CONFLICT", tree.RejectSpecial)

	// Start with evalExprs, which will contain all the RHS expressions
	// of UPDATE SET clauses. Again, evalExprs will contain one entry
	// per column in updateCols and untupledExprs.
	helper.evalExprs = make([]tree.TypedExpr, len(untupledExprs))
	for i, expr := range untupledExprs {
		// Analyze the expression.

		// We require the type from the column descriptor.
		desiredType := updateCols[i].Type.ToDatumType()

		// Resolve names, type and normalize.
		normExpr, err := p.analyzeExpr(
			ctx, expr, sources, ivarHelper, desiredType, true /* requireType */, "ON CONFLICT")
		if err != nil {
			return nil, err
		}

		helper.evalExprs[i] = normExpr
	}

	// If there's a conflict WHERE clause, analyze it.
	if whereClause != nil {
		p.semaCtx.Properties.Require("ON CONFLICT...WHERE", tree.RejectSpecial)

		// Resolve names, type and normalize.
		whereExpr, err := p.analyzeExpr(
			ctx, whereClause.Expr, sources, ivarHelper, types.Bool, true, /* requireType */
			"ON CONFLICT...WHERE")
		if err != nil {
			return nil, err
		}

		helper.whereExpr = whereExpr
	}

	// To (re-)compute computed columns, we use a 1-row buffer with an
	// IVarContainer interface (a rowIndexedVarContainer).
	//
	// This will use the layout from the table columns. The mapping from
	// column IDs to row datum positions is straightforward.
	helper.ccIvarContainer = sqlbase.RowIndexedVarContainer{
		Cols:    tableDesc.Columns,
		Mapping: row.ColIDtoRowIndexFromCols(tableDesc.Columns),
	}

	return helper, nil
}

func (uh *upsertHelper) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
	for i, evalExpr := range uh.evalExprs {
		walk("eval", i, evalExpr)
	}
}

// eval returns the values for the update case of an upsert, given the row
// that would have been inserted and the existing (conflicting) values.
func (uh *upsertHelper) eval(insertRow, existingRow, resultRow tree.Datums) (tree.Datums, error) {
	// Buffer the rows.
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	// Evaluate the update expressions.
	uh.p.EvalContext().PushIVarContainer(uh)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
	for _, evalExpr := range uh.evalExprs {
		res, err := evalExpr.Eval(uh.p.EvalContext())
		if err != nil {
			return nil, err
		}
		resultRow = append(resultRow, res)
	}

	return resultRow, nil
}

// evalComputedCols handles after we've figured out the values of all regular
// columns, what the values of any incoming computed columns should be.
// It then appends those new values to the end of a given slice.
func (uh *upsertHelper) evalComputedCols(
	updatedRow tree.Datums, appendTo tree.Datums,
) (tree.Datums, error) {
	// Buffer the row.
	uh.ccIvarContainer.CurSourceRow = updatedRow

	// Evaluate the computed columns.
	uh.p.EvalContext().PushIVarContainer(&uh.ccIvarContainer)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
	for i := range uh.computeExprs {
		res, err := uh.computeExprs[i].Eval(uh.p.EvalContext())
		if err != nil {
			return nil, err
		}
		appendTo = append(appendTo, res)
	}
	return appendTo, nil
}

// shouldUpdate returns the result of evaluating the WHERE clause of the
// ON CONFLICT ... DO UPDATE clause.
func (uh *upsertHelper) shouldUpdate(insertRow tree.Datums, existingRow tree.Datums) (bool, error) {
	// Buffer the rows.
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	// Evaluate the predicate.
	uh.p.EvalContext().PushIVarContainer(uh)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
	return sqlbase.RunFilter(uh.whereExpr, uh.p.EvalContext())
}

// upsertExprsAndIndex returns the upsert conflict index and the (possibly
// synthetic) SET expressions used when a row conflicts.
//
// Return values:
// - autoGeneratedAssignments is true iff the UPSERT statement was used
//   and there was no explicit ON CONFLICT DO UPDATE clause.
// - updateExprs: the assignment expressions in ON CONFLICT DO UPDATE,
//   or auto-generated assignments for UPSERT.
// - conflictIdx: the conflicting index, if specified.
func upsertExprsAndIndex(
	tableDesc *sqlbase.ImmutableTableDescriptor,
	onConflict tree.OnConflict,
	insertCols []sqlbase.ColumnDescriptor,
) (
	autoGeneratedAssignments bool,
	updateExprs tree.UpdateExprs,
	conflictIdx *sqlbase.IndexDescriptor,
	err error,
) {
	if onConflict.IsUpsertAlias() {
		// Construct a fake set of UPDATE SET expressions. This enables sharing
		// the same implementation for UPSERT and INSERT ... ON CONFLICT DO UPDATE.

		// The UPSERT syntactic sugar is the same as the longhand specifying the
		// primary index as the conflict index and SET expressions for the columns
		// in insertCols minus any columns in the conflict index. Example:
		// `UPSERT INTO abc VALUES (1, 2, 3)` is syntactic sugar for
		// `INSERT INTO abc VALUES (1, 2, 3) ON CONFLICT a DO UPDATE SET b = 2, c = 3`.
		conflictIndex := &tableDesc.PrimaryIndex
		indexColSet := make(map[sqlbase.ColumnID]struct{}, len(conflictIndex.ColumnIDs))
		for _, colID := range conflictIndex.ColumnIDs {
			indexColSet[colID] = struct{}{}
		}

		updateExprs := make(tree.UpdateExprs, 0, len(insertCols))
		for _, c := range insertCols {
			if c.ComputeExpr != nil {
				// Computed columns must not appear in the pseudo-SET
				// expression.
				continue
			}
			if _, ok := indexColSet[c.ID]; ok {
				// If the column is part of the PK, there is no need for a
				// pseudo-assignment.
				continue
			}
			n := tree.Name(c.Name)
			expr := tree.NewColumnItem(&upsertExcludedTable, n)
			updateExprs = append(updateExprs, &tree.UpdateExpr{Names: tree.NameList{n}, Expr: expr})
		}
		return true, updateExprs, conflictIndex, nil
	}

	if onConflict.DoNothing && len(onConflict.Columns) == 0 {
		return false, onConflict.Exprs, nil, nil
	}

	// General case: INSERT with an ON CONFLICT clause.

	indexMatch := func(index sqlbase.IndexDescriptor) bool {
		if !index.Unique {
			return false
		}
		if len(index.ColumnNames) != len(onConflict.Columns) {
			return false
		}
		for i, colName := range index.ColumnNames {
			if colName != string(onConflict.Columns[i]) {
				return false
			}
		}
		return true
	}

	if indexMatch(tableDesc.PrimaryIndex) {
		return false, onConflict.Exprs, &tableDesc.PrimaryIndex, nil
	}
	for i := range tableDesc.Indexes {
		if indexMatch(tableDesc.Indexes[i]) {
			return false, onConflict.Exprs, &tableDesc.Indexes[i], nil
		}
	}
	return false, nil, nil, pgerror.NewErrorf(pgcode.InvalidColumnReference,
		"there is no unique or exclusion constraint matching the ON CONFLICT specification")
}
