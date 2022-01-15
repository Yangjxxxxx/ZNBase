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
	"go/constant"
	"sync"

	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/transform"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

var deleteNodePool = sync.Pool{
	New: func() interface{} {
		return &deleteNode{}
	},
}

type deleteNode struct {
	source planNode

	// columns is set if this DELETE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run deleteRun
}

// deleteNode implements the autoCommitNode interface.
var _ autoCommitNode = &deleteNode{}

// Delete removes rows from a table.
// Privileges: DELETE and SELECT on table. We currently always use a SELECT statement.
//   Notes: postgres requires DELETE. Also requires SELECT for "USING" and "WHERE" with tables.
//          mysql requires DELETE. Also requires SELECT if a table is used in the "WHERE" clause.
func (p *planner) Delete(
	ctx context.Context, n *tree.Delete, desiredTypes []types.T,
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
					},
				}
				comparExpr.Left = &tree.UnresolvedName{
					NumParts: 1,
					Star:     false,
					Parts:    [...]string{colName, "", "", ""},
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

			// 构造一个where对象
			convertToRowidWhere := &tree.Where{
				Type: "WHERE",
				Expr: andExpr.Left,
			}
			n.Where = convertToRowidWhere
		}
	}

	// UX friendliness safeguard.
	if n.Where == nil && p.SessionData().SafeUpdates {
		return nil, pgerror.NewDangerousStatementErrorf("DELETE without WHERE clause")
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

	// DELETE FROM xx AS yy - we want to know about xx (tn) because
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

	if err := p.CheckPrivilege(ctx, desc, privilege.DELETE); err != nil {
		return nil, err
	}

	// Determine what are the foreign key tables that are involved in the deletion.
	fkTables, err := row.MakeFkMetadata(
		ctx,
		desc,
		row.CheckDeletes,
		p.LookupTableByID,
		func(ctx context.Context, tbl *sqlbase.TableDescriptor, cols tree.NameList, priv privilege.Kind) error {
			return p.CheckDMLPrivilege(ctx, tbl, cols, priv, "")
		},
		p.checkPrivilegeAccessToTable,
		p.analyzeExpr,
		nil, /* checkHelper */
	)
	if err != nil {
		return nil, err
	}

	// rowsNeeded will help determine whether we can use the fast path
	// in startExec.
	rowsNeeded := resultsNeeded(n.Returning)

	// Also, rowsNeeded determines which rows of the source we need
	// in the table deleter.
	var requestedCols []sqlbase.ColumnDescriptor
	if rowsNeeded {
		// Note: in contrast to INSERT and UPDATE which also require the
		// data if there are CHECK expressions, DELETE does not care about
		// constraint checking (because the rows are being deleted after
		// all).

		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = desc.Columns
	}

	// Create the table deleter, which does the bulk of the work.
	rd, err := row.MakeDeleter(
		p.txn, desc, fkTables, requestedCols, row.CheckFKs, p.EvalContext(), &p.alloc,
	)
	if err != nil {
		return nil, err
	}

	tracing.AnnotateTrace()

	// Determine the source for the deletion: the rows that are read,
	// filtered, limited, ordered, etc, prior to the deletion. One would
	// think there is only so much one wants to do with rows prior to a
	// deletion, but ORDER BY / LIMIT really determines which rows are
	// being deleted. Also RETURNING will expose this.
	rows, err := p.SelectClause(ctx, &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(rd.FetchCols, true /* forUpdateOrDelete */),
		From:  &tree.From{Tables: []tree.TableExpr{n.Table}},
		Where: n.Where,
	}, n.OrderBy, n.Limit, nil /*with*/, nil /*desiredTypes*/, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	var columns sqlbase.ResultColumns
	if rowsNeeded {
		columns = planColumns(rows)
		// If rowsNeeded is set, we have requested from the source above
		// all the columns from the descriptor. However, to ensure that
		// modified rows include all columns, the construction of the
		// source has used publicAndNonPublicColumns so the source may
		// contain additional columns for every newly dropped column not
		// visible.
		// We do not want these to be available for RETURNING below.
		//
		// In the case where rowsNeeded is true, the requested columns are
		// requestedCols. So we can truncate to the length of that to
		// only see public columns.
		columns = columns[:len(requestedCols)]
	}

	// Now make a delete node. We use a pool.
	dn := deleteNodePool.Get().(*deleteNode)
	*dn = deleteNode{
		source:  rows,
		columns: columns,
		run: deleteRun{
			td:                  tableDeleter{rd: rd, alloc: &p.alloc},
			rowsNeeded:          rowsNeeded,
			fastPathInterleaved: canDeleteFastInterleaved(desc, fkTables),
		},
	}

	// Finally, handle RETURNING, if any.
	r, err := p.Returning(ctx, dn, n.Returning, desiredTypes, alias)
	if err != nil {
		// We close explicitly here to release the node to the pool.
		dn.Close(ctx)
	}
	return r, err
}

// deleteRun contains the run-time state of deleteNode during local execution.
type deleteRun struct {
	td         tableDeleter
	rowsNeeded bool

	// fastPathInterleaved indicates whether the delete operation can run
	// the interleaved fast path (all interleaved tables have no indexes and ON DELETE CASCADE).
	fastPathInterleaved bool

	// rowCount is the number of rows in the current batch.
	rowCount int

	// done informs a new call to BatchedNext() that the previous call
	// to BatchedNext() has completed the work already.
	done bool

	// rows contains the accumulated result rows if rowsNeeded is set.
	rows *rowcontainer.RowContainer

	// traceKV caches the current KV tracing flag.
	traceKV bool

	Trigger *sqlbase.TriggerDesc
}

// maxDeleteBatchSize is the max number of entries in the KV batch for
// the delete operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxDeleteBatchSize = 10000

func (d *deleteNode) startExec(params runParams) error {
	if err := params.p.maybeSetSystemConfig(d.run.td.tableDesc().GetID()); err != nil {
		return err
	}

	// trigger constructor
	tgDesc, err := MakeTriggerDesc(params.ctx, params.p.txn, d.run.td.tableDesc())
	if err != nil {
		return err
	}
	d.run.Trigger = tgDesc

	err = ExecuteTriggers(params.ctx, params.p.txn, params, d.run.Trigger, nil, nil, nil,
		TriggerTypeDelete, TriggerTypeBefore, TriggerTypeStatement)
	if err != nil {
		return err
	}

	// cache traceKV during execution, to avoid re-evaluating it for every row.
	d.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if d.run.rowsNeeded {
		d.run.rows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(d.columns), 0)
	}
	return d.run.td.init(params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (d *deleteNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (d *deleteNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (d *deleteNode) BatchedNext(params runParams) (bool, error) {
	// for _, tg := range d.run.Trigger.Triggers {
	// 	if tg.IsAfterStmt(TriggerTypeAfter, TriggerTypeStatement) {
	// 		d.run.td.isAfter = true
	// 		break
	// 	}
	// }

	if d.run.done {
		return false, nil
	}

	tracing.AnnotateTrace()

	// Advance one batch. First, clear the current batch.
	d.run.rowCount = 0
	if d.run.rows != nil {
		d.run.rows.Clear(params.ctx)
	}

	//isfuncIndex := false
	//if d.run.td.rd.Helper.Indexes != nil {
	//	for _, index := range d.run.td.rd.Helper.Indexes {
	//		if index.IsFunc != nil {
	//			for _, isfunc := range index.IsFunc {
	//				if isfunc {
	//					isfuncIndex = true
	//					break
	//				}
	//			}
	//		}
	//	}
	//}

	//if isfuncIndex {
	//	oldoutput := d.source.(*rowSourceToPlanNode).source.(*rowexec.TableReader).Out.GetOutPut()
	//	newoutput := d.source.(*rowSourceToPlanNode).source.(*rowexec.TableReader).Out.GetOutPut()
	//	var addColumns []sqlbase.ColumnDescriptor
	//	for i,_ := range d.run.td.rd.Helper.TableDesc.Columns {
	//		for j,value := range oldoutput{
	//			if i == int(value) {
	//				break
	//			}else if i != int(value) && j == len(oldoutput)-1{
	//				newoutput = append(newoutput, uint32(i))
	//				d.source.(*rowSourceToPlanNode).source.(*rowexec.TableReader).Out.AppendOutPut()
	//				d.source.(*rowSourceToPlanNode).datumRow = append(d.source.(*rowSourceToPlanNode).datumRow,nil)
	//				for k,addcolumn := range d.run.td.rd.Helper.TableDesc.Columns {
	//					if int(addcolumn.ID) - 1 == i{
	//						addColumns = append(addColumns,addcolumn)
	//						d.source.(*rowSourceToPlanNode).source.(*rowexec.TableReader).Out.OutputTypes = append(d.source.(*rowSourceToPlanNode).source.(*rowexec.TableReader).Out.OutputTypes,d.run.td.rd.Helper.TableDesc.Columns[k].Type)
	//					}
	//				}
	//			}
	//		}
	//	}
	//	d.source.(*rowSourceToPlanNode).source.(*rowexec.TableReader).Out.PutOutPut(newoutput)
	//	addCols := sqlbase.ResultColumnsFromColDescs(addColumns)
	//	for _,addCol := range addCols{
	//		d.source.(*rowSourceToPlanNode).planCols = append(d.source.(*rowSourceToPlanNode).planCols,addCol)
	//	}
	//}

	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := d.source.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		err := ExecuteTriggers(params.ctx, params.p.txn, params, d.run.Trigger, d.run.td.tableDesc(), d.source.Values(),
			nil, TriggerTypeDelete, TriggerTypeBefore, TriggerTypeRow)
		if err != nil {
			return false, err
		}

		// Process the deletion of the current source row,
		// potentially accumulating the result row for later.
		if err := d.processSourceRow(params, d.source.Values()); err != nil {
			return false, err
		}

		err = ExecuteTriggers(params.ctx, params.p.txn, params, d.run.Trigger, d.run.td.tableDesc(), d.source.Values(),
			d.run.td.b, TriggerTypeDelete, TriggerTypeAfter, TriggerTypeRow)
		if err != nil {
			return false, err
		}

		d.run.rowCount++

		// Are we done yet with the current batch?
		if d.run.td.curBatchSize() >= maxDeleteBatchSize {
			break
		}
	}

	if d.run.rowCount > 0 {
		if err := d.run.td.atBatchEnd(params.ctx, d.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := d.run.td.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := d.run.td.finalize(params.ctx, params, d.run.traceKV, d.run.Trigger); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		d.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		d.run.td.tableDesc().ID,
		d.run.rowCount,
	)

	return d.run.rowCount > 0, nil
}

// processSourceRow processes one row from the source for deletion and, if
// result rows are needed, saves it in the result row container
func (d *deleteNode) processSourceRow(params runParams, sourceVals tree.Datums) error {

	var exprstrings []string

	if d.run.td.rd.Helper.Indexes != nil {
		for _, index := range d.run.td.rd.Helper.Indexes {
			if index.IsFunc != nil {
				for i, isfunc := range index.IsFunc {
					if isfunc {
						exprstrings = append(exprstrings, index.ColumnNames[i])
					}
				}
			}
		}
	}

	if exprstrings != nil {
		var txCtx transform.ExprTransformContext
		typeexprs, err := sqlbase.MakeFuncExprs(exprstrings, d.run.td.rd.Helper.TableDesc, tree.NewUnqualifiedTableName(tree.Name(d.run.td.rd.Helper.TableDesc.Name)), &txCtx, params.EvalContext(), true /* addingCols */)
		if err != nil {
			return err
		}
		var exprIndex int
		for _, typeexpr := range typeexprs {
			for i := 0; i < len(typeexpr.(*tree.FuncExpr).Exprs); i++ {
				for j := 0; j < len(d.run.td.rd.Helper.TableDesc.Columns); j++ {
					if _, ok := typeexpr.(*tree.FuncExpr).Exprs[i].(*tree.IndexedVar); !ok {
						continue
					}
					if int(d.run.td.rd.Helper.TableDesc.Columns[j].ID)-1 == typeexpr.(*tree.FuncExpr).Exprs[i].(*tree.IndexedVar).Idx {
						for k := 0; k < len(d.source.(*rowSourceToPlanNode).planCols); k++ {
							if d.run.td.rd.Helper.TableDesc.Columns[j].Name == d.source.(*rowSourceToPlanNode).planCols[k].Name {
								exprIndex = k
							}
						}

					}
				}
				typeexpr.(*tree.FuncExpr).Exprs[i] = sourceVals[exprIndex]
			}
			newrow, err := typeexpr.Eval(params.EvalContext())
			if err != nil {
				return err
			}
			sourceVals = append(sourceVals, newrow)
		}
	}

	// Queue the deletion in the KV batch.
	var pm schemaexpr.PartialIndexUpdateHelper
	if err := d.run.td.row(params.ctx, sourceVals, d.run.traceKV, pm); err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if d.run.rows != nil {
		// The new values can include all columns, the construction of the
		// values has used publicAndNonPublicColumns so the values may
		// contain additional columns for every newly dropped column not
		// visible. We do not want them to be available for RETURNING.
		//
		// d.columns is guaranteed to only contain the requested
		// public columns.
		resultValues := sourceVals[:len(d.columns)]
		if _, err := d.run.rows.AddRow(params.ctx, resultValues); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedCount() int { return d.run.rowCount }

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedValues(rowIdx int) tree.Datums { return d.run.rows.At(rowIdx) }

func (d *deleteNode) Close(ctx context.Context) {
	d.source.Close(ctx)
	if d.run.rows != nil {
		d.run.rows.Close(ctx)
		d.run.rows = nil
	}
	d.run.td.close(ctx)
	*d = deleteNode{}
	deleteNodePool.Put(d)
}

func canDeleteFastInterleaved(table *ImmutableTableDescriptor, fkTables row.FkTableMetadata) bool {
	// If there are no interleaved tables then don't take the fast path.
	// This avoids superfluous use of DelRange in cases where there isn't as much of a performance boost.
	hasInterleaved := false
	for _, idx := range table.AllNonDropIndexes() {
		if len(idx.InterleavedBy) > 0 {
			hasInterleaved = true
			break
		}
	}
	if !hasInterleaved {
		return false
	}

	// if the base table is interleaved in another table, fail
	for _, idx := range fkTables[table.ID].Desc.AllNonDropIndexes() {
		if len(idx.Interleave.Ancestors) > 0 {
			return false
		}
	}

	interleavedQueue := []sqlbase.ID{table.ID}
	for len(interleavedQueue) > 0 {
		tableID := interleavedQueue[0]
		interleavedQueue = interleavedQueue[1:]
		if _, ok := fkTables[tableID]; !ok {
			return false
		}
		if fkTables[tableID].Desc == nil {
			return false
		}
		for _, idx := range fkTables[tableID].Desc.AllNonDropIndexes() {
			// Don't allow any secondary indexes
			// TODO(emmanuel): identify the cases where secondary indexes can still work with the fast path and allow them
			if idx.ID != fkTables[tableID].Desc.PrimaryIndex.ID {
				return false
			}

			// interleavedIdxs will contain all of the table and index IDs of the indexes interleaved in this one
			interleavedIdxs := make(map[sqlbase.ID]map[sqlbase.IndexID]struct{})
			for _, ref := range idx.InterleavedBy {
				if _, ok := interleavedIdxs[ref.Table]; !ok {
					interleavedIdxs[ref.Table] = make(map[sqlbase.IndexID]struct{})
				}
				interleavedIdxs[ref.Table][ref.Index] = struct{}{}

			}
			// The index can't be referenced by anything that's not the interleaved relationship
			for _, ref := range idx.ReferencedBy {
				if _, ok := interleavedIdxs[ref.Table]; !ok {
					return false
				}
				if _, ok := interleavedIdxs[ref.Table][ref.Index]; !ok {
					return false
				}

				referencingIdx, err := fkTables[ref.Table].Desc.FindIndexByID(ref.Index)
				if err != nil {
					return false
				}

				// All of these references MUST be ON DELETE CASCADE
				if referencingIdx.ForeignKey.OnDelete != sqlbase.ForeignKeyReference_CASCADE {
					return false
				}
			}

			for _, ref := range idx.InterleavedBy {
				interleavedQueue = append(interleavedQueue, ref.Table)
			}

		}
	}
	return true
}

// enableAutoCommit is part of the autoCommitNode interface.
func (d *deleteNode) enableAutoCommit() {
	d.run.td.enableAutoCommit()
}
