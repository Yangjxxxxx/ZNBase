// Copyright 2015  The Cockroach Authors.
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
	"context"
	"fmt"
	"go/constant"
	"sync"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/transform"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

var updateNodePool = sync.Pool{
	New: func() interface{} {
		return &updateNode{}
	},
}

type updateNode struct {
	source planNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run updateRun
}

// updateNode implements the autoCommitNode interface.
var _ autoCommitNode = &updateNode{}

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
func (p *planner) Update(
	ctx context.Context, n *tree.Update, desiredTypes []types.T,
) (result planNode, resultErr error) {
	// current cursor
	if n.Where != nil {
		if cursor, ok := n.Where.Expr.(*tree.CurrentOfExpr); ok {
			tableName, _, err := p.getAliasedTableName(n.Table)
			if err != nil {
				return nil, err
			}
			curName := cursor.CursorName.String()
			// Get primaryKeysAndDatums Map.
			pkMaps, err := GetCurrentCursorPrimaryKeyMap(p, curName, tableName.String())
			if err != nil {
				return nil, err
			}

			// 构造where
			andExpr := &tree.AndExpr{}
			for colName, datum := range pkMaps {
				comparExpr := &tree.ComparisonExpr{
					Operator:    tree.EQ,
					SubOperator: tree.EQ,
					Left: &tree.UnresolvedName{
						NumParts: 1,
						Star:     false,
						Parts:    [...]string{colName, "", "", ""},
					},
				}

				switch datum.(type) {
				case *tree.DInt:
					datumDType := datum.(*tree.DInt)
					comparExpr.Right = &tree.NumVal{
						Value:      constant.MakeInt64(int64(*datumDType)),
						Negative:   false,
						OrigString: datumDType.String(),
					}
				case *tree.DFloat:
					datumDType := datum.(*tree.DFloat)
					comparExpr.Right = &tree.NumVal{
						Value:      constant.MakeFloat64(float64(*datumDType)),
						Negative:   false,
						OrigString: datumDType.String(),
					}
				case *tree.DBool:
					datumDType := datum.(*tree.DBool)
					comparExpr.Right = datumDType
				case *tree.DOid:
					datumDType := datum.(*tree.DOid)
					comparExpr.Right = datumDType
				default:
					s := datum.String()
					comparExpr.Right = tree.NewStrVal(WithoutSingleQuotationMark(s))
				}

				if andExpr.Left == nil && andExpr.Right == nil {
					andExpr.Left = comparExpr
				} else {
					andExpr.Right = comparExpr
					andExprNew := &tree.AndExpr{Left: andExpr}
					andExpr = andExprNew
				}
			}

			convertToRowidWhere := &tree.Where{
				Type: "WHERE",
				Expr: andExpr.Left,
			}
			n.Where = convertToRowidWhere
		}
	}

	// UX friendliness safeguard.
	if n.Where == nil && p.SessionData().SafeUpdates {
		return nil, pgerror.NewDangerousStatementErrorf("UPDATE without WHERE clause")
	}

	// CTE analysis.
	resetter, err := p.initWith(ctx, n.With)
	if err != nil {
		return nil, err
	}
	if resetter != nil {
		defer func() {
			if cteErr := resetter(p); cteErr != nil && resultErr == nil {
				// If no error was found in the inner planning but a CTE error
				// is occurring during the final checks on the way back from
				// the recursion, use that error as final error for this
				// stage.
				resultErr = cteErr
				result = nil
			}
		}()
	}

	tracing.AnnotateTrace()

	// UPDATE xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	// Find which table we're working on, check the permissions.
	desc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilegeAccessToDataSource(ctx, tn); err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, desc, privilege.UPDATE); err != nil {
		return nil, err
	}

	// Analyze any check constraints.
	checkHelper, err := sqlbase.NewEvalCheckHelper(ctx, p.analyzeExpr, desc)
	if err != nil {
		return nil, err
	}

	// Determine what are the foreign key tables that are involved in the update.
	fkTables, err := row.MakeFkMetadata(
		ctx,
		desc,
		row.CheckUpdates,
		p.LookupTableByID,
		func(ctx context.Context, tbl *sqlbase.TableDescriptor, cols tree.NameList, priv privilege.Kind) error {
			return p.CheckDMLPrivilege(ctx, tbl, cols, priv, "")
		},
		p.checkPrivilegeAccessToTable,
		p.analyzeExpr,
		checkHelper,
	)
	if err != nil {
		return nil, err
	}

	// when table created with some column defined on update current_timestamp,
	// it should reconsturct updateExpr such as update ... set starttime = current_timestamp
	n.Exprs, _ = buildAdditionalUpdateExprs(desc, n.Exprs)

	// Extract all the LHS column names, and verify that the arity of
	// the LHS and RHS match when assigning tuples.
	names, setExprs, err := p.namesForExprs(ctx, n.Exprs)
	if err != nil {
		return nil, err
	}
	if desc.IsHashPartition {
		names = append(names, "hashnum")
	}

	// Extract the column descriptors for the column names listed
	// in the LHS operands of SET expressions. This also checks
	// that each column is assigned at most once.
	updateCols, err := p.processColumns(desc, names,
		true /* ensureColumns */, false /* allowMutations */)
	if err != nil {
		return nil, err
	}

	// Ensure that the columns being updated are not computed.
	// We do this check as early as possible to avoid doing
	// unnecessary work below in case there's an error.
	//
	// TODO(justin): this is too restrictive. It should
	// be possible to allow UPDATE foo SET x = DEFAULT
	// when x is a computed column. See #22434.
	if err := checkHasNoComputedCols(updateCols); err != nil {
		return nil, err
	}

	// Extract the pre-analyzed, pre-typed default expressions for all
	// the updated columns. There are as many defaultExprs as there are
	// updateCols.
	defaultExprs, err := sqlbase.MakeDefaultExprs(
		updateCols, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// Extract the computed columns, if any. This extends updateCols with
	// all the computed columns. The computedCols result is an alias for the suffix
	// of updateCols that corresponds to computed columns.
	updateCols, computedCols, computeExprs, err :=
		sqlbase.ProcessComputedColumns(ctx, updateCols, tn, desc, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// rowsNeeded will help determine whether we need to allocate a
	// rowsContainer.
	rowsNeeded := resultsNeeded(n.Returning)

	var requestedCols []sqlbase.ColumnDescriptor
	if rowsNeeded {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = desc.Columns
	} else if len(desc.ActiveChecks()) > 0 {
		// Request any columns we'll need when validating check constraints. We
		// could be smarter and only validate check constraints which depend on
		// columns that are being modified in the UPDATE statement, in which
		// case we'd only need to request the columns used by that subset of
		// check constraints, but that doesn't seem worth the effort.
		//
		// TODO(nvanbenschoten): These conditions shouldn't be mutually
		// exclusive, but since rowsNeeded implies that requestedCols =
		// desc.Columns, there's no reason to enter this block if rowsNeeded is
		// true. Remove this when the TODO above is addressed.
		var requestedColSet util.FastIntSet
		for _, col := range requestedCols {
			requestedColSet.Add(int(col.ID))
		}
		for _, ck := range desc.ActiveChecks() {
			cols, err := ck.ColumnsUsed(desc.TableDesc())
			if err != nil {
				return nil, err
			}
			for _, colID := range cols {
				if !requestedColSet.Contains(int(colID)) {
					col, err := desc.FindColumnByID(colID)
					if err != nil {
						return nil, errors.Wrapf(err, "error finding column %d in table %s",
							colID, desc.Name)
					}
					requestedCols = append(requestedCols, *col)
					requestedColSet.Add(int(colID))
				}
			}
		}
	}

	// Create the table updater, which does the bulk of the work.
	// As a result of MakeUpdater, ru.FetchCols include all the
	// columns in the table descriptor + any columns currently in the
	// process of being added.
	ru, err := row.MakeUpdater(
		p.txn,
		func(ctx context.Context, tbl *sqlbase.TableDescriptor, cols tree.NameList, priv privilege.Kind) error {
			return p.CheckDMLPrivilege(ctx, tbl, cols, priv, "")
		},
		desc,
		fkTables,
		updateCols,
		requestedCols,
		row.UpdaterDefault,
		p.EvalContext(),
		&p.alloc,
	)
	if err != nil {
		return nil, err
	}

	tracing.AnnotateTrace()

	// We construct a query containing the columns being updated, and
	// then later merge the values they are being updated with into that
	// renderNode to ideally reuse some of the queries.
	rows, err := p.SelectClause(ctx, &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(ru.FetchCols, true /* forUpdateOrDelete */),
		From:  &tree.From{Tables: []tree.TableExpr{n.Table}},
		Where: n.Where,
	}, n.OrderBy, n.Limit, nil /* with */, nil /*desiredTypes*/, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	// sourceSlots describes the RHS operands to potential tuple-wise
	// assignments to the LHS operands. See the comment on
	// updateRun.sourceSlots for details.
	//
	// UPDATE ... SET (a,b) = (...), (c,d) = (...)
	//                ^^^^^^^^^^(1)  ^^^^^^^^^^(2) len(n.Exprs) == len(sourceSlots)
	sourceSlots := make([]sourceSlot, 0, len(n.Exprs))

	// currentUpdateIdx is the index of the first column descriptor in updateCols
	// that is assigned to by the current setExpr.
	currentUpdateIdx := 0

	// We need to add renders below, for the RHS of each
	// assignment. Where can we do this? If we already have a
	// renderNode, then use that. Otherwise, add one.
	var render *renderNode
	if r, ok := rows.(*renderNode); ok {
		render = r
	} else {
		render, err = p.insertRender(ctx, rows, tn)
		if err != nil {
			return nil, err
		}
		// The new renderNode is also the new data source for the update.
		rows = render
	}

	// Capture the columns of the source, prior to the insertion of
	// extra renders. This will be the input for RETURNING, if any, and
	// this must not see the additional renders added below.
	// It also must not see the additional columns captured in FetchCols
	// but which were not in requestedCols.
	var columns sqlbase.ResultColumns
	if rowsNeeded {
		columns = planColumns(rows)
		// If rowsNeeded is set, we have requested from the source above
		// all the columns from the descriptor. However, to ensure that
		// modified rows include all columns, the construction of the
		// source has used publicAndNonPublicColumns so the source may
		// contain additional columns for every newly added column not yet
		// visible.
		// We do not want these to be available for RETURNING below.
		//
		// MakeUpdater guarantees that the first columns of the source
		// are those specified in requestedCols, which, in the case where
		// rowsNeeded is true, is also desc.Columns. So we can truncate to
		// the length of that to only see public columns.
		columns = columns[:len(desc.Columns)]
	}

	// For the analysis below, we need to restrict the planning to only
	// allow simple expressions. Before we restrict anything, we need to
	// save the current context.
	scalarProps := &p.semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	p.semaCtx.Properties.Require("UPDATE SET", tree.RejectSpecial)

	for _, setExpr := range setExprs {
		if setExpr.Tuple {
			switch t := setExpr.Expr.(type) {
			case *tree.Tuple:
				// The user assigned an explicit set of values to the columns. We can't
				// treat this case the same as when we have a subquery (and just evaluate
				// the tuple) because when assigning a literal tuple like this it's valid
				// to assign DEFAULT to some of the columns, which is not valid generally.
				for _, e := range t.Exprs {
					colIdx, err := p.addOrMergeExpr(ctx, e, currentUpdateIdx, updateCols, defaultExprs, render)
					if err != nil {
						return nil, err
					}

					sourceSlots = append(sourceSlots, scalarSlot{
						column:      updateCols[currentUpdateIdx],
						sourceIndex: colIdx,
					})

					currentUpdateIdx++
				}

			case *tree.Subquery:
				selectExpr := tree.SelectExpr{Expr: t}
				desiredTupleType := types.TTuple{Types: make([]types.T, len(setExpr.Names))}
				for i := range setExpr.Names {
					desiredTupleType.Types[i] = updateCols[currentUpdateIdx+i].Type.ToDatumType()
				}
				col, expr, err := p.computeRender(ctx, selectExpr, desiredTupleType, render.sourceInfo, render.ivarHelper, autoGenerateRenderOutputName, -1)
				if err != nil {
					return nil, err
				}

				colIdx := render.addOrReuseRender(col, expr, false)

				tSlot := tupleSlot{
					columns:     updateCols[currentUpdateIdx : currentUpdateIdx+len(setExpr.Names)],
					emptySlot:   make(tree.Datums, len(setExpr.Names)),
					sourceIndex: colIdx,
				}
				for i := range tSlot.emptySlot {
					tSlot.emptySlot[i] = tree.DNull
				}
				sourceSlots = append(sourceSlots, tSlot)
				currentUpdateIdx += len(setExpr.Names)

			default:
				return nil, pgerror.NewAssertionErrorf("assigning to tuple with expression that is neither a tuple nor a subquery: %s", setExpr.Expr)
			}

		} else {
			colIdx, err := p.addOrMergeExpr(ctx, setExpr.Expr, currentUpdateIdx, updateCols, defaultExprs, render)
			if err != nil {
				return nil, err
			}

			sourceSlots = append(sourceSlots, scalarSlot{
				column:      updateCols[currentUpdateIdx],
				sourceIndex: colIdx,
			})
			currentUpdateIdx++
		}
	}

	// Placeholders have their types populated in the above Select if they are part
	// of an expression ("SET a = 2 + $1") in the type check step where those
	// types are inferred. For the simpler case ("SET a = $1"), populate them
	// using checkColumnType. This step also verifies that the expression
	// types match the column types.
	for _, sourceSlot := range sourceSlots {
		if err := sourceSlot.checkColumnTypes(render.render, &p.semaCtx.Placeholders); err != nil {
			return nil, err
		}
	}

	if desc.IsHashPartition {
		hashIdx := 0
		sourceIdx := 0
		for i, hashDesc := range updateCols {
			if hashDesc.Name == desc.HashField {
				for idx, col := range ru.FetchCols {
					if col.Name == "hashnum" {
						hashIdx = idx
						break
					}
				}
			}
			if hashDesc.Name == "hashnum" {
				sourceIdx = i
				sourceSlots = append(sourceSlots, scalarSlot{column: ru.FetchCols[hashIdx], sourceIndex: len(ru.FetchCols) + sourceIdx})
			}
		}
	}

	// updateColsIdx inverts the mapping of UpdateCols to FetchCols. See
	// the explanatory comments in updateRun.
	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i, col := range ru.UpdateCols {
		updateColsIdx[col.ID] = i
	}

	un := updateNodePool.Get().(*updateNode)
	*un = updateNode{
		source:  rows,
		columns: columns,
		run: updateRun{
			tu:           tableUpdater{ru: ru},
			checkHelper:  checkHelper,
			rowsNeeded:   rowsNeeded,
			computedCols: computedCols,
			computeExprs: computeExprs,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
				CurSourceRow: make(tree.Datums, len(ru.FetchCols)),
				Cols:         desc.Columns,
				Mapping:      ru.FetchColIDtoRowIndex,
			},
			sourceSlots:   sourceSlots,
			updateValues:  make(tree.Datums, len(ru.UpdateCols)),
			updateColsIdx: updateColsIdx,
		},
	}

	// Finally, handle RETURNING, if any.
	r, err := p.Returning(ctx, un, n.Returning, desiredTypes, alias)
	if err != nil {
		// We close explicitly here to release the node to the pool.
		un.Close(ctx)
	}
	return r, err
}

// updateRun contains the run-time state of updateNode during local execution.
type updateRun struct {
	tu          tableUpdater
	checkHelper *sqlbase.CheckHelper
	rowsNeeded  bool

	// rowCount is the number of rows in the current batch.
	rowCount int

	// done informs a new call to BatchedNext() that the previous call to
	// BatchedNext() has completed the work already.
	done bool

	// rows contains the accumulated result rows if rowsNeeded is set.
	rows *rowcontainer.RowContainer

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// computedCols are the columns that need to be (re-)computed as
	// the result of updating some of the columns in updateCols.
	computedCols []sqlbase.ColumnDescriptor
	// computeExprs are the expressions to evaluate to re-compute the
	// columns in computedCols.
	computeExprs []tree.TypedExpr
	// iVarContainerForComputedCols is used as a temporary buffer that
	// holds the updated values for every column in the source, to
	// serve as input for indexed vars contained in the computeExprs.
	iVarContainerForComputedCols sqlbase.RowIndexedVarContainer

	// sourceSlots is the helper that maps RHS expressions to LHS targets.
	// This is necessary because there may be fewer RHS expressions than
	// LHS targets. For example, SET (a, b) = (SELECT 1,2) has:
	// - 2 targets (a, b)
	// - 1 source slot, the subquery (SELECT 1, 2).
	// Each call to extractValues() on a sourceSlot will return 1 or more
	// datums suitable for assignments. In the example above, the
	// method would return 2 values.
	sourceSlots []sourceSlot

	// updateValues will hold the new values for every column
	// mentioned in the LHS of the SET expressions, in the
	// order specified by those SET expressions (thus potentially
	// a different order than the source).
	updateValues tree.Datums

	// During the update, the expressions provided by the source plan
	// contain the columns that are being assigned in the order
	// specified by the table descriptor.
	//
	// For example, with UPDATE kv SET v=3, k=2, the source plan will
	// provide the values in the order k, v (assuming this is the order
	// the columns are defined in kv's descriptor).
	//
	// Then during the update, the columns are updated in the order of
	// the setExprs (or, equivalently, the order of the sourceSlots),
	// for the example above that would be v, k. The results
	// are stored in updateValues above.
	//
	// Then at the end of the update, the values need to be presented
	// back to the TableRowUpdater in the order of the table descriptor
	// again.
	//
	// updateVals is the buffer for this 2nd stage.
	// updateColsIdx maps the order of the 2nd stage into the order of the 3rd stage.
	// This provides the inverse mapping of sourceSlots.
	//
	updateColsIdx map[sqlbase.ColumnID]int
	checkOrds     checkSet

	// numPassthrough is the number of columns in addition to the set of
	// columns of the target table being returned, that we must pass through
	// from the input node.
	numPassthrough int

	// trigger for update
	Trigger *sqlbase.TriggerDesc
}

// maxUpdateBatchSize is the max number of entries in the KV batch for
// the update operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxUpdateBatchSize = 10000

func (u *updateNode) startExec(params runParams) error {
	if err := params.p.maybeSetSystemConfig(u.run.tu.tableDesc().GetID()); err != nil {
		return err
	}

	// trigger constructor
	tgDesc, err := MakeTriggerDesc(params.ctx, params.p.txn, u.run.tu.tableDesc())
	if err != nil {
		return err
	}
	u.run.Trigger = tgDesc

	// cache traceKV during execution, to avoid re-evaluating it for every row.
	u.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if u.run.rowsNeeded {
		u.run.rows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(u.columns), 0)
	}
	return u.run.tu.init(params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (u *updateNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (u *updateNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (u *updateNode) BatchedNext(params runParams) (bool, error) {
	// for _, tg := range u.run.Trigger.Triggers {
	// 	if tg.IsAfterStmt(TriggerTypeAfter, TriggerTypeStatement) {
	// 		u.run.tu.isAfter = true
	// 		break
	// 	}
	// }

	if u.run.done {
		return false, nil
	}

	err := ExecuteTriggers(params.ctx, params.p.txn, params, u.run.Trigger, nil, nil, nil,
		TriggerTypeUpdate, TriggerTypeBefore, TriggerTypeStatement)
	if err != nil {
		return false, err
	}

	tracing.AnnotateTrace()

	// Advance one batch. First, clear the current batch.
	u.run.rowCount = 0
	if u.run.rows != nil {
		u.run.rows.Clear(params.ctx)
	}
	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := u.source.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// for _, tg := range u.run.Trigger.Triggers {
		// 	if u.run.Trigger == nil || u.run.Trigger.Triggers == nil {
		// 		break
		// 	} else {
		// 		err := u.execUpdateTriggers(params.ctx, params.p.txn, params, TriggerTypeUpdate, TriggerTypeBefore, TriggerTypeRow, tg)
		// 		if err != nil {
		// 			return false, err
		// 		}
		// 	}
		// }

		// Process the update for the current source row, potentially
		// accumulating the result row for later.
		if err := u.processSourceRow(params, u.source.Values()); err != nil {
			return false, err
		}

		u.run.rowCount++

		// for _, tg := range u.run.Trigger.Triggers {
		// 	if u.run.Trigger == nil || u.run.Trigger.Triggers == nil {
		// 		break
		// 	} else {
		// 		err := u.execUpdateTriggers(params.ctx, params.p.txn, params, TriggerTypeUpdate, TriggerTypeAfter, TriggerTypeRow, tg)
		// 		if err != nil {
		// 			return false, err
		// 		}
		// 	}
		// }

		// Are we done yet with the current batch?
		if u.run.tu.curBatchSize() >= maxUpdateBatchSize {
			break
		}
	}

	if u.run.rowCount > 0 {
		if err := u.run.tu.atBatchEnd(params.ctx, u.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := u.run.tu.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := u.run.tu.finalize(params.ctx, params, u.run.traceKV, u.run.Trigger); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		u.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		u.run.tu.tableDesc().ID,
		u.run.rowCount,
	)

	return u.run.rowCount > 0, nil
}

// processSourceRow processes one row from the source for update and, if
// result rows are needed, saves it in the result row container.
func (u *updateNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// sourceVals contains values for the columns from the table, in the order of the
	// table descriptor. (One per column in u.tw.ru.FetchCols)
	//
	// And then after that, all the extra expressions potentially added via
	// a renderNode for the RHS of the assignments.

	// oldValues is the prefix of sourceVals that corresponds to real
	// stored columns in the table, that is, excluding the RHS assignment
	// expressions.
	oldValues := sourceVals[:len(u.run.tu.ru.FetchCols)]
	primaryIdxName := ""
	desc := u.run.tu.tableDesc()
	if desc.IsHashPartition {
		primaryIdxName = desc.HashField
		if len(sourceVals) < len(u.run.updateValues)+len(oldValues) {
			for _, slot := range u.run.sourceSlots {
				if slot.(scalarSlot).column.Name == primaryIdxName {
					newHashnum := util.CRC32Origin([]byte(sourceVals[slot.(scalarSlot).sourceIndex].String()))
					mod := desc.HashParts
					if mod == 0 {
						return errors.New("zero mod")
					}
					newHashnum = newHashnum % uint32(mod)
					sourceVals = append(sourceVals, tree.NewDInt(tree.DInt(newHashnum)))
					break
				}
			}
		}
	}
	// valueIdx is used in the loop below to map sourceSlots to
	// entries in updateValues.
	valueIdx := 0

	// Propagate the values computed for the RHS expressions into
	// updateValues at the right positions. The positions in
	// updateValues correspond to the columns named in the LHS
	// operands for SET.
	for _, slot := range u.run.sourceSlots {
		for _, value := range slot.extractValues(sourceVals) {
			u.run.updateValues[valueIdx] = value
			valueIdx++
		}
	}

	// At this point, we have populated updateValues with the result of
	// computing the RHS for every assignment.
	//

	if len(u.run.computeExprs) > 0 {
		// We now need to (re-)compute the computed column values, using
		// the updated values above as input.
		//
		// This needs to happen in the context of a row containing all the
		// table's columns as if they had been updated already. This is not
		// yet reflected neither by oldValues (which contain non-updated values)
		// nor updateValues (which contain only those columns mentioned in the SET LHS).
		//
		// So we need to construct a buffer that groups them together.
		// iVarContainerForComputedCols does this.
		copy(u.run.iVarContainerForComputedCols.CurSourceRow, oldValues)
		for i, col := range u.run.tu.ru.UpdateCols {
			u.run.iVarContainerForComputedCols.CurSourceRow[u.run.tu.ru.FetchColIDtoRowIndex[col.ID]] = u.run.updateValues[i]
		}

		// Now (re-)compute the computed columns.
		// Note that it's safe to do this in any order, because we currently
		// prevent computed columns from depending on other computed columns.
		params.EvalContext().PushIVarContainer(&u.run.iVarContainerForComputedCols)
		for i := range u.run.computedCols {
			d, err := u.run.computeExprs[i].Eval(params.EvalContext())
			if err != nil {
				params.EvalContext().IVarContainer = nil
				return errors.Wrapf(err,
					"computed column %s", tree.ErrString((*tree.Name)(&u.run.computedCols[i].Name)))
			}
			u.run.updateValues[u.run.updateColsIdx[u.run.computedCols[i].ID]] = d
		}
		params.EvalContext().PopIVarContainer()
	}

	// Verify the schema constraints. For consistency with INSERT/UPSERT
	// and compatibility with PostgreSQL, we must do this before
	// processing the CHECK constraints.
	if err := enforceLocalColumnConstraints(u.run.updateValues, u.run.tu.ru.UpdateCols); err != nil {
		return err
	}

	// Run the CHECK constraints, if any. CheckHelper will either evaluate the
	// constraints itself, or else inspect boolean columns from the input that
	// contain the results of evaluation.
	if u.run.checkHelper != nil {
		if u.run.checkHelper.NeedsEval() {
			// TODO(justin): we have actually constructed the whole row at this point and
			// thus should be able to avoid loading it separately like this now.
			if err := u.run.checkHelper.LoadEvalRow(
				u.run.tu.ru.FetchColIDtoRowIndex, oldValues, false); err != nil {
				return err
			}
			if err := u.run.checkHelper.LoadEvalRow(
				u.run.updateColsIdx, u.run.updateValues, true); err != nil {
				return err
			}
			if err := u.run.checkHelper.CheckEval(params.EvalContext()); err != nil {
				return err
			}
		} else {
			checkVals := sourceVals[len(u.run.tu.ru.FetchCols)+len(u.run.tu.ru.UpdateCols)+u.run.numPassthrough:]
			if err := u.run.checkHelper.CheckInput(checkVals); err != nil {
				return err
			}
		}
	}

	// Create a set of partial index IDs to not add entries or remove entries
	// from.
	var pm schemaexpr.PartialIndexUpdateHelper
	partialIndexOrds := u.run.tu.tableDesc().PartialIndexOrds()
	if !partialIndexOrds.Empty() {
		partialIndexValOffset := len(u.run.tu.ru.FetchCols) + len(u.run.tu.ru.UpdateCols) + u.run.checkOrds.Len()
		partialIndexVals := sourceVals[partialIndexValOffset:]
		partialIndexPutVals := partialIndexVals[:len(partialIndexVals)/2]
		partialIndexDelVals := partialIndexVals[len(partialIndexVals)/2:]

		err := pm.Init(partialIndexPutVals, partialIndexDelVals, u.run.tu.tableDesc())
		if err != nil {
			return err
		}
	}
	var exprStrings []string

	// If the updated column affects the functional index, the value of the
	// expression is calculated here.
	if desc.Indexes != nil {
		for _, index := range desc.Indexes {
			if index.IsFunc != nil {
				for i, isFunc := range index.IsFunc {
					if isFunc {
						exprStrings = append(exprStrings, index.ColumnNames[i])
					}
				}
			}
		}
	}

	if exprStrings != nil {
		var txCtx transform.ExprTransformContext
		typedExprs, err := sqlbase.MakeFuncExprs(exprStrings, desc, tree.NewUnqualifiedTableName(tree.Name(desc.Name)),
			&txCtx, params.EvalContext(), true)
		if err != nil {
			return err
		}
		var newFuncResults []tree.Datum
		for _, typedExpr := range typedExprs {
			for i := 0; i < len(typedExpr.(*tree.FuncExpr).Exprs); i++ {
				// First use oldValues to calculate the value of FuncExpr, and add
				// the result to the end of the array.
				if _, ok := typedExpr.(*tree.FuncExpr).Exprs[i].(*tree.IndexedVar); !ok {
					continue
				}
				idx := typedExpr.(*tree.FuncExpr).Exprs[i].(*tree.IndexedVar).Idx
				typedExpr.(*tree.FuncExpr).Exprs[i] = oldValues[idx]
				funcRes, err := typedExpr.Eval(params.EvalContext())
				if err != nil {
					return err
				}
				oldValues = append(oldValues, funcRes)
				// Then if the parameter of FuncExpr is a column that needs to be updated,
				// assign the new value to it. And calculate the new funcexpr value and
				// add it to the end of the array.
				if v, ok := u.run.updateColsIdx[sqlbase.ColumnID(idx+1)]; ok {
					typedExpr.(*tree.FuncExpr).Exprs[i] = u.run.updateValues[v]
				} else {
					typedExpr.(*tree.FuncExpr).Exprs[i] = oldValues[idx]
				}
				newFuncRes, err := typedExpr.Eval(params.EvalContext())
				if err != nil {
					return err
				}
				newFuncResults = append(newFuncResults, newFuncRes)
			}
		}
		oldValues = append(oldValues, newFuncResults...)
	}

	// Queue the insert in the KV batch.
	newValues, err := u.run.tu.rowForUpdate(params.ctx, oldValues, u.run.updateValues, pm, u.run.traceKV)
	if err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if u.run.rows != nil {
		// The new values can include all columns, the construction of the
		// values has used publicAndNonPublicColumns so the values may
		// contain additional columns for every newly added column not yet
		// visible. We do not want them to be available for RETURNING.
		//
		// MakeUpdater guarantees that the first columns of the new values
		// are those specified u.columns.
		idx := -1
		var resultValues []tree.Datum
		if len(newValues) > len(u.columns) {
			resultValues = newValues[:len(u.columns)]
			idx = len(u.columns) - 1
		} else {
			resultValues = make([]tree.Datum, len(u.columns))
			for i := range newValues {
				resultValues[i] = newValues[i]
				idx = i
			}
		}

		// At this point we've extracted all the RETURNING values that are part
		// of the target table. We must now extract the columns in the RETURNING
		// clause that refer to other tables (from the FROM clause of the update).
		if u.run.numPassthrough > 0 {
			passthroughBegin := len(u.run.tu.ru.FetchCols) + len(u.run.tu.ru.UpdateCols)
			passthroughEnd := passthroughBegin + u.run.numPassthrough
			passthroughValues := sourceVals[passthroughBegin:passthroughEnd]

			for i := 0; i < u.run.numPassthrough; i++ {
				idx++
				resultValues[idx] = passthroughValues[i]
			}
		}

		if _, err := u.run.rows.AddRow(params.ctx, resultValues); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (u *updateNode) BatchedCount() int { return u.run.rowCount }

// BatchedCount implements the batchedPlanNode interface.
func (u *updateNode) BatchedValues(rowIdx int) tree.Datums { return u.run.rows.At(rowIdx) }

func (u *updateNode) Close(ctx context.Context) {
	u.source.Close(ctx)
	if u.run.rows != nil {
		u.run.rows.Close(ctx)
		u.run.rows = nil
	}
	u.run.tu.close(ctx)
	*u = updateNode{}
	updateNodePool.Put(u)
}

// enableAutoCommit implements the autoCommitNode interface.
func (u *updateNode) enableAutoCommit() {
	u.run.tu.enableAutoCommit()
}

// sourceSlot abstracts the idea that our update sources can either be tuples
// or scalars. Tuples are for cases such as SET (a, b) = (1, 2) or SET (a, b) =
// (SELECT 1, 2), and scalars are for situations like SET a = b. A sourceSlot
// represents how to extract and type-check the results of the right-hand side
// of a single SET statement. We could treat everything as tuples, including
// scalars as tuples of size 1, and eliminate this indirection, but that makes
// the query plan more complex.
type sourceSlot interface {
	// extractValues returns a slice of the values this slot is responsible for,
	// as extracted from the row of results.
	extractValues(resultRow tree.Datums) tree.Datums
	// checkColumnTypes compares the types of the results that this slot refers to to the types of
	// the columns those values will be assigned to. It returns an error if those types don't match up.
	// It also populates the types of any placeholders by way of calling into sqlbase.CheckColumnType.
	checkColumnTypes(row []tree.TypedExpr, pmap *tree.PlaceholderInfo) error
	checkColumnName() string
}

type tupleSlot struct {
	columns     []sqlbase.ColumnDescriptor
	sourceIndex int
	// emptySlot is to be returned when the source subquery
	// returns no rows.
	emptySlot tree.Datums
}

func (ts tupleSlot) extractValues(row tree.Datums) tree.Datums {
	if row[ts.sourceIndex] == tree.DNull {
		return ts.emptySlot
	}
	return row[ts.sourceIndex].(*tree.DTuple).D
}

func (ts tupleSlot) checkColumnTypes(row []tree.TypedExpr, pmap *tree.PlaceholderInfo) error {
	renderedResult := row[ts.sourceIndex]
	for i, typ := range renderedResult.ResolvedType().(types.TTuple).Types {
		if err := sqlbase.CheckDatumTypeFitsColumnType(ts.columns[i], typ, pmap); err != nil {
			return err
		}
	}
	return nil
}

func (ts tupleSlot) checkColumnName() string {
	return ""
}

type scalarSlot struct {
	column      sqlbase.ColumnDescriptor
	sourceIndex int
}

func (ss scalarSlot) extractValues(row tree.Datums) tree.Datums {
	return row[ss.sourceIndex : ss.sourceIndex+1]
}

func (ss scalarSlot) checkColumnTypes(row []tree.TypedExpr, pmap *tree.PlaceholderInfo) error {
	renderedResult := row[ss.sourceIndex]
	typ := renderedResult.ResolvedType()
	return sqlbase.CheckDatumTypeFitsColumnType(ss.column, typ, pmap)
}

func (ss scalarSlot) checkColumnName() string {
	return ss.column.Name
}

// addOrMergeExpr inserts an Expr into a renderNode, attempting to reuse
// previous renders if possible by using render.addOrMergeRender, returning the
// column index at which the rendered value can be accessed.
func (p *planner) addOrMergeExpr(
	ctx context.Context,
	e tree.Expr,
	currentUpdateIdx int,
	updateCols []sqlbase.ColumnDescriptor,
	defaultExprs []tree.TypedExpr,
	render *renderNode,
) (colIdx int, err error) {
	e = fillDefault(e, currentUpdateIdx, defaultExprs)
	selectExpr := tree.SelectExpr{Expr: e}
	typ := updateCols[currentUpdateIdx].Type.ToDatumType()
	col, expr, err := p.computeRender(ctx, selectExpr, typ, render.sourceInfo, render.ivarHelper, autoGenerateRenderOutputName, currentUpdateIdx)
	if err != nil {
		return -1, err
	}

	return render.addOrReuseRender(col, expr, true), nil
}

// namesForExprs both preperforms early subquery analysis and extraction and
// then collects all the names mentioned in the LHS of the UpdateExprs.  That
// is, it will transform SET (a,b) = (1,2), b = 3, (a,c) = 4 into [a,b,b,a,c].
//
// It also checks that the arity of the LHS and RHS match when
// assigning tuples.
func (p *planner) namesForExprs(
	ctx context.Context, exprs tree.UpdateExprs,
) (tree.NameList, tree.UpdateExprs, error) {
	// Pre-perform early subquery analysis and extraction. Usually
	// this is done by analyzeExpr(), and, in fact, analyzeExpr() is indirectly
	// called below as well. Why not call analyzeExpr() already?
	//
	// The obstacle is that there's a circular dependency here.
	//
	// - We can't call analyzeExpr() at this point because the RHS of
	//   UPDATE SET can contain the special expression "DEFAULT", and
	//   analyzeExpr() does not work with DEFAULT.
	//
	// - The substitution of DEFAULT by the actual default expression
	//   occurs below (in addOrMergeExpr), but we can't do that yet here
	//   because we first need to decompose a tuple in the LHS into
	//   multiple assignments.
	//
	// - We can't decompose the tuple in the LHS without validating it
	//   against the arity of the RHS (to properly reject mismatched
	//   arities with an error) until subquery analysis has occurred.
	//
	// So we need the subquery analysis to be done early and we can't
	// call analyzeExpr() to do so.
	//
	// TODO(knz): arguably we could do the tuple decomposition _before_
	// the arity check, in this order: decompose tuples, substitute
	// DEFAULT / run addOrMergeExpr which itself calls analyzeExpr, and
	// then check the arity. This improvement is left as an exercise for
	// the reader.
	setExprs := make(tree.UpdateExprs, len(exprs))
	for i, expr := range exprs {
		// Analyze the sub-query nodes.
		err := p.analyzeSubqueries(ctx, expr.Expr, len(expr.Names))
		if err != nil {
			return nil, nil, err
		}
		setExprs[i] = &tree.UpdateExpr{Tuple: expr.Tuple, Expr: expr.Expr, Names: expr.Names}
	}

	var names tree.NameList
	for _, expr := range exprs {
		if expr.Tuple {
			n := -1
			switch t := expr.Expr.(type) {
			case *tree.Subquery:
				if tup, ok := t.ResolvedType().(types.TTuple); ok {
					n = len(tup.Types)
				}
			case *tree.Tuple:
				n = len(t.Exprs)
			case *tree.DTuple:
				n = len(t.D)
			}
			if n < 0 {
				return nil, nil, pgerror.UnimplementedWithIssueDetailErrorf(35713,
					fmt.Sprintf("%T", expr.Expr),
					"source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression; not supported: %T", expr.Expr)
			}
			if len(expr.Names) != n {
				return nil, nil, pgerror.NewErrorf(pgcode.Syntax,
					"number of columns (%d) does not match number of values (%d)", len(expr.Names), n)
			}
		}
		names = append(names, expr.Names...)
	}
	return names, setExprs, nil
}

func fillDefault(expr tree.Expr, index int, defaultExprs []tree.TypedExpr) tree.Expr {
	switch expr.(type) {
	case tree.DefaultVal:
		if defaultExprs == nil {
			return tree.DNull
		}
		return defaultExprs[index]
	}
	return expr
}

func checkHasNoComputedCols(cols []sqlbase.ColumnDescriptor) error {
	for i := range cols {
		if cols[i].IsComputed() {
			return sqlbase.CannotWriteToComputedColError(cols[i].Name)
		}
	}
	return nil
}

// enforceLocalColumnConstraints asserts the column constraints that
// do not require data validation from other sources than the row data
// itself. This includes:
// - rejecting null values in non-nullable columns;
// - checking width constraints from the column type;
// - truncating results to the requested precision (not width).
// Note: the second point is what distinguishes this operation
// from a regular SQL cast -- here widths are checked, not
// used to truncate the value silently.
//
// The row buffer is modified in-place with the result of the
// checks.
func enforceLocalColumnConstraints(row tree.Datums, cols []sqlbase.ColumnDescriptor) error {
	for i := range cols {
		col := &cols[i]
		if !col.Nullable && row[i] == tree.DNull {
			return sqlbase.NewNonNullViolationError(col.Name)
		}
		outVal, err := sqlbase.LimitValueWidth(col.Type, row[i], &col.Name, false)
		if err != nil {
			return err
		}
		row[i] = outVal
	}
	return nil
}

// buildAdditionalUpdateExprs build addtionnal updateExpr if this table have on update flag
func buildAdditionalUpdateExprs(
	desc *ImmutableTableDescriptor, updateExprs tree.UpdateExprs,
) (up tree.UpdateExprs, err error) {
	colLen := len(desc.Columns)
	for i := 0; i < colLen; i++ {
		col := desc.Columns[i]
		if col.IsOnUpdateCurrentTimeStamp() {
			// construct updateExprs
			addUpdExpr := &tree.UpdateExpr{
				Tuple: false,
				Names: []tree.Name{
					col.ColName(),
				},
				Expr: &tree.FuncExpr{
					Func: tree.WrapFunction("current_timestamp"),
				},
			}
			updateExprs = append(updateExprs, addUpdExpr)
		}
	}
	return updateExprs, nil
}

// func (u *updateNode) execUpdateTriggers(
// 	ctx context.Context,
// 	txn *client.Txn,
// 	params runParams,
// 	eventType uint32,
// 	runType uint32,
// 	levelType uint32,
// 	tg sqlbase.Trigger,
// ) error {
//
// 	if tg.IsType(eventType, runType, levelType) {
// 		if runType == TriggerTypeAfter && levelType == TriggerTypeRow {
// 			if txn != nil {
// 				err := txn.Run(ctx, u.run.tu.tableWriterBase.b)
// 				if err != nil {
// 					return err
// 				}
// 				*u.run.tu.tableWriterBase.b = *txn.NewBatch()
// 			}
// 		}
// 		if tg.Tgwhenexpr == "" {
// 			err := Call(ctx, txn, params, tg.Tgargs, int(tg.TgfuncID), tg.Tgname, tg.Tgrelid)
// 			if err != nil {
// 				return err
// 			}
// 		} else {
// 			comexpr, err := parser.ParseExpr(tg.Tgwhenexpr)
// 			if err != nil {
// 				panic(err)
// 			}
// 			switch t := comexpr.(type) {
// 			case *tree.ComparisonExpr:
// 				op := t.Operator
// 				lstar, lval, err := u.getvalue(t.Left)
// 				if err != nil {
// 					return err
// 				}
// 				rstar, rval, err := u.getvalue(t.Right)
// 				if err != nil {
// 					return err
// 				}
// 				if lstar || rstar {
// 					if lval == nil || rval == nil {
// 						return nil
// 					}
// 					var ectx *tree.EvalContext
// 					for i := 0; i < len(lval); i++ {
// 						cmp := lval[i].Compare(ectx, rval[i])
// 						if !Boolfromcmp(op, cmp) {
// 							return nil
// 						}
// 						if i == len(lval)-1 {
// 							err := Call(ctx, txn, params, tg.Tgargs, int(tg.TgfuncID), tg.Tgname, tg.Tgrelid)
// 							if err != nil {
// 								return err
// 							}
// 						}
// 					}
// 				} else {
// 					if lval == nil || rval == nil {
// 						return nil
// 					}
// 					var ectx *tree.EvalContext
// 					cmp := lval[0].Compare(ectx, rval[0])
// 					if Boolfromcmp(op, cmp) {
// 						err := Call(ctx, txn, params, tg.Tgargs, int(tg.TgfuncID), tg.Tgname, tg.Tgrelid)
// 						if err != nil {
// 							return err
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}
//
// 	return nil
// }
//
// func (u *updateNode) getvalue(expr tree.Expr) (star bool, value tree.Datums, err error) {
// 	switch t := expr.(type) {
// 	case *tree.UnresolvedName:
// 		if t.Star {
// 			var col int
// 			if u.run.tu.ru.FetchCols[len(u.run.tu.ru.FetchCols)-1].Hidden {
// 				col = len(u.run.tu.ru.FetchCols) - 1
// 			} else {
// 				col = len(u.run.tu.ru.FetchCols)
// 			}
// 			if t.Parts[1] == "old" {
// 				return true, u.source.Values()[:col], nil
// 			}
// 			if t.Parts[1] == "new" {
// 				if len(u.run.sourceSlots) == col {
// 					return true, u.source.Values()[len(u.source.Values())-col : len(u.source.Values())], nil
// 				}
// 				return true, nil, nil
// 			}
// 		} else {
// 			if t.Parts[1] == "old" {
// 				for i := range u.run.tu.ru.FetchCols {
// 					if u.run.tu.ru.FetchCols[i].Name == t.Parts[0] {
// 						return false, tree.Datums{u.source.Values()[i]}, nil
// 					}
// 				}
// 			}
// 			if t.Parts[1] == "new" {
// 				for i := range u.run.sourceSlots {
// 					if u.run.sourceSlots[i].checkColumnName() == t.Parts[0] {
// 						return false, u.run.sourceSlots[i].extractValues(u.source.Values()), nil
// 					}
// 					if i == len(u.run.sourceSlots)-1 {
// 						for i := range u.run.iVarContainerForComputedCols.Cols {
// 							if u.run.iVarContainerForComputedCols.Cols[i].Name == t.Parts[0] {
// 								return false, u.source.Values()[i : i+1], nil
// 							}
// 						}
// 						return false, nil, nil
// 					}
// 				}
// 			}
// 		}
// 	default:
// 		var ctx *tree.SemaContext
// 		var ectx *tree.EvalContext
// 		texpr, err := tree.TypeCheck(t, ctx, types.Any, false)
// 		if err != nil {
// 			return false, nil, err
// 		}
// 		val, err := texpr.Eval(ectx)
// 		if err != nil {
// 			return false, nil, err
// 		}
// 		return false, tree.Datums{val}, nil
// 	}
// 	return false, nil, nil
// }
