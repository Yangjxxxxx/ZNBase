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
	"strconv"
	"sync"

	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

type valuesNode struct {
	columns sqlbase.ResultColumns
	tuples  [][]tree.TypedExpr
	// isConst is set if the valuesNode only contains constant expressions (no
	// subqueries). In this case, rows will be evaluated during the first call
	// to planNode.Start and memoized for future consumption. A valuesNode with
	// isConst = true can serve its values multiple times. See valuesNode.Reset.
	isConst bool

	// specifiedInQuery is set if the valuesNode represents a literal
	// relational expression that was present in the original SQL text,
	// as opposed to e.g. a valuesNode resulting from the expansion of
	// a vtable value generator. This changes distsql physical planning.
	specifiedInQuery bool

	valuesRun
}

// Values implements the VALUES clause.
func (p *planner) Values(
	ctx context.Context, origN tree.Statement, desiredTypes []types.T,
) (planNode, error) {
	v := &valuesNode{
		specifiedInQuery: true,
		isConst:          true,
	}

	// If we have names, extract them.
	var n *tree.ValuesClause
	switch t := origN.(type) {
	case *tree.ValuesClauseWithNames:
		n = &t.ValuesClause
	case *tree.ValuesClause:
		n = t
	default:
		return nil, pgerror.NewAssertionErrorf("unhandled case in values: %T %v", origN, origN)
	}

	if len(n.Rows) == 0 {
		return v, nil
	}

	numCols := len(n.Rows[0])

	v.columns = make(sqlbase.ResultColumns, 0, numCols)

	lastKnownSubqueryIndex := len(p.curPlan.subqueryPlans)

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer p.semaCtx.Properties.Restore(p.semaCtx.Properties)

	// Ensure there are no special functions in the clause.
	p.semaCtx.Properties.Require("VALUES", tree.RejectSpecial)

	// judge if the curPlan is a insert statement
	var insertOrUpdateCols []sqlbase.ColumnDescriptor
	InsertStmt, isInsert := p.curPlan.AST.(*tree.Insert)
	// get insert target columns
	if isInsert {
		insertColumns, err := getInsertTargetCols(ctx, p, InsertStmt)
		if err != nil {
			return nil, err
		}
		insertOrUpdateCols = insertColumns
	}

	if len(n.Rows) >= 10 && len(p.semaCtx.Placeholders.Types) == 0 {
		var wg sync.WaitGroup
		v.tuples = make([][]tree.TypedExpr, len(n.Rows))
		tupleRow := make([]tree.TypedExpr, len(n.Rows[0]))
		for i, expr := range n.Rows[0] {
			desired := types.Any
			if len(desiredTypes) > i {
				desired = desiredTypes[i]
			}
			typedExpr, err := p.analyzeExpr(ctx, expr, nil, tree.IndexedVarHelper{}, desired, false, "")
			if err != nil {
				return nil, err
			}

			typ := typedExpr.ResolvedType()
			if desired != types.Any && typ != desired {
				if ok, c := tree.IsCastDeepValid(typ, desired); ok && !tree.IsNullExpr(typedExpr) {
					telemetry.Inc(c)
					width := 0
					if isInsert {
						width, _ = getColWidthAndPrecision(insertOrUpdateCols, i)
					}
					CastExpr := tree.ChangeToCastExpr(typedExpr, desired, width)
					typedExpr = &CastExpr
					typ = desired
				}
			}

			v.columns = append(v.columns, sqlbase.ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})

			tupleRow[i] = typedExpr
		}
		v.tuples[0] = tupleRow

		chunkLen := len(n.Rows) / 10
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go p.fillRows(
				ctx,
				i,
				&wg,
				v.columns,
				desiredTypes,
				numCols,
				v.tuples[chunkLen*i:chunkLen*(i+1)],
				n.Rows[chunkLen*i:chunkLen*(i+1)],
				isInsert,
				insertOrUpdateCols,
			)
		}
		if len(n.Rows)%10 != 0 {
			wg.Add(1)
			go p.fillRows(
				ctx,
				10,
				&wg,
				v.columns,
				desiredTypes,
				numCols,
				v.tuples[chunkLen*10:],
				n.Rows[chunkLen*10:],
				isInsert,
				insertOrUpdateCols,
			)
		}
		wg.Wait()
	} else {
		v.tuples = make([][]tree.TypedExpr, 0, len(n.Rows))
		tupleBuf := make([]tree.TypedExpr, len(n.Rows)*numCols)
		for num, tuple := range n.Rows {
			if a, e := len(tuple), numCols; a != e {
				return nil, newValuesListLenErr(e, a)
			}

			// Chop off prefix of tupleBuf and limit its capacity.
			tupleRow := tupleBuf[:numCols:numCols]
			tupleBuf = tupleBuf[numCols:]

			for i, expr := range tuple {
				desired := types.Any
				if len(desiredTypes) > i {
					desired = desiredTypes[i]
				}

				// Clear the properties so we can check them below.
				typedExpr, err := p.analyzeExpr(ctx, expr, nil, tree.IndexedVarHelper{}, desired, false, "")
				if err != nil {
					return nil, err
				}

				typ := typedExpr.ResolvedType()
				if desired != types.Any && typ != desired {
					if ok, c := tree.IsCastDeepValid(typ, desired); ok && !tree.IsNullExpr(typedExpr) {
						telemetry.Inc(c)
						width := 0
						if isInsert {
							width, _ = getColWidthAndPrecision(insertOrUpdateCols, i)
						}
						CastExpr := tree.ChangeToCastExpr(typedExpr, desired, width)
						typedExpr = &CastExpr
						typ = desired
					}
				}
				if num == 0 {
					v.columns = append(v.columns, sqlbase.ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
				} else if v.columns[i].Typ == types.Unknown {
					v.columns[i].Typ = typ
				} else if typ != types.Unknown && !typ.Equivalent(v.columns[i].Typ) {
					return nil, pgerror.NewErrorf(pgcode.DatatypeMismatch,
						"VALUES types %s and %s cannot be matched", typ, v.columns[i].Typ)
				}

				tupleRow[i] = typedExpr
			}
			v.tuples = append(v.tuples, tupleRow)
		}
	}
	// TODO(nvanbenschoten): if v.isConst, we should be able to evaluate n.rows
	// ahead of time. This requires changing the contract for planNode.Close such
	// that it must always be called unless an error is returned from a planNode
	// constructor. This would simplify the Close contract, but would make some
	// code (like in planner.SelectClause) more messy.
	v.isConst = (len(p.curPlan.subqueryPlans) == lastKnownSubqueryIndex)
	return v, nil
}

func (p *planner) newContainerValuesNode(columns sqlbase.ResultColumns, capacity int) *valuesNode {
	return &valuesNode{
		columns: columns,
		isConst: true,
		valuesRun: valuesRun{
			rows: rowcontainer.NewRowContainer(
				p.EvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromResCols(columns), capacity,
			),
		},
	}
}

// valuesRun is the run-time state of a valuesNode during local execution.
type valuesRun struct {
	rows    *rowcontainer.RowContainer
	nextRow int // The index of the next row.
}

func (n *valuesNode) startExec(params runParams) error {
	if n.rows != nil {
		if !n.isConst {
			log.Fatalf(params.ctx, "valuesNode evaluated twice")
		}
		return nil
	}

	// planner := params.p
	hasTrigger := false
	// if planner != nil && planner.tableTrigger != nil {
	// 	tableDesc := planner.tableTrigger.TableDesc()
	// 	if tableDesc != nil {
	// 		tableID := tableDesc.ID
	// 		key := sqlbase.MakeTrigMetadataKey(tableID, "")
	// 		kvs, err := planner.txn.Scan(params.ctx, key, key.PrefixEnd(), 0)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		if len(kvs) > 0 {
	// 			hasTrigger = true
	// 		}
	// 	}
	// }

	// if hasTrigger on table, then execute the previous logic
	if !hasTrigger && params.p != nil && len(n.tuples) >= 10 {
		rowCount, ok := params.p.curPlan.plan.(*rowCountNode)
		if ok {
			insert, ok := rowCount.source.(*insertNode)
			if ok && !insert.run.rowsNeeded && len(insert.run.ti.ri.Helper.TableDesc.Families) == 1 {
				if !(len(insert.run.ti.ri.Helper.TableDesc.Families[0].ColumnIDs) == 1 &&
					insert.run.ti.ri.Helper.TableDesc.Families[0].ColumnIDs[0] ==
						insert.run.ti.ri.Helper.TableDesc.Families[0].DefaultColumnID) && insert.run.ti.ri.Fks.GetChecker() == nil {
					insert.isKv = true
					//insert.kvRedundancy = make(chan sqlbase.KvRedundancy, 32)
					rows := make([][]tree.Datum, len(n.tuples))
					for i, tupleRow := range n.tuples {
						row := make([]tree.Datum, len(n.columns))
						for i, typedExpr := range tupleRow {
							if n.columns[i].Omitted {
								row[i] = tree.DNull
							} else {
								var err error
								row[i], err = typedExpr.Eval(params.EvalContext())
								if err != nil {
									return err
								}
							}
						}
						rows[i] = row
					}

					var wg sync.WaitGroup

					var errChan = make(chan error)
					var goCount int

					insert.kvs = make([]sqlbase.KvRedundancy, len(n.tuples))
					wg.Add(1)
					if err := insert.BuildKv1(0, &wg, rows[0:1], insert.kvs[0:1], params); err != nil {
						return err
					}
					chunkLen := len(n.tuples) / 5
					for i := 0; i < 5; i++ {
						wg.Add(1)
						goCount++
						go func(i int) {
							err := insert.BuildKv1(i, &wg, rows[chunkLen*i:chunkLen*(i+1)], insert.kvs[chunkLen*i:chunkLen*(i+1)], params)
							if err != nil {
								errChan <- err
							}
							errChan <- nil
						}(i)
					}
					if len(n.tuples)%5 != 0 {
						wg.Add(1)
						goCount++
						go func() {
							err := insert.BuildKv1(5, &wg, rows[chunkLen*5:], insert.kvs[chunkLen*5:], params)
							if err != nil {
								errChan <- err
							}
							errChan <- nil
						}()
					}
					for i := 0; i < goCount; i++ {
						err := <-errChan
						if err != nil {
							return err
						}
					}

					wg.Wait()
					//go insert.BuildKv(rows, params)
					return nil
				}
			}
		}
	}

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a valuesNode internally for storing results
	// from other planNodes), so its expressions need evaluating.
	// This may run subqueries.
	n.rows = rowcontainer.NewRowContainer(
		params.extendedEvalCtx.Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(n.columns),
		len(n.tuples),
	)

	row := make([]tree.Datum, len(n.columns))
	for _, tupleRow := range n.tuples {
		for i, typedExpr := range tupleRow {
			curUsed := n.rows.MemUsage()
			curReserved := n.rows.Allocated() - curUsed
			curAllocated := n.rows.MonitorCurAllocated()
			if n.columns[i].Omitted {
				row[i] = tree.DNull
			} else {
				var err error
				row[i], err = typedExpr.Eval(params.EvalContext())
				if err != nil {
					n.rows.SetUsed(curUsed)
					n.rows.SetReserved(curReserved)
					n.rows.SetCurAlocated(curAllocated)
					return err
				}
			}
		}
		if _, err := n.rows.AddRow(params.ctx, row); err != nil {
			return err
		}
	}

	return nil
}

// Reset resets the valuesNode processing state without requiring recomputation
// of the values tuples if the valuesNode is processed again. Reset can only
// be called if valuesNode.isConst.
func (n *valuesNode) Reset(ctx context.Context) {
	if !n.isConst {
		log.Fatalf(ctx, "valuesNode.Reset can only be called on constant valuesNodes")
	}
	n.nextRow = 0
}

func (n *valuesNode) Next(runParams) (bool, error) {
	if n.nextRow >= n.rows.Len() {
		return false, nil
	}
	n.nextRow++
	return true, nil
}

func (n *valuesNode) Values() tree.Datums {
	return n.rows.At(n.nextRow - 1)
}

func (n *valuesNode) Close(ctx context.Context) {
	if n.rows != nil {
		n.rows.Close(ctx)
		n.rows = nil
	}
}

func newValuesListLenErr(exp, got int) error {
	return pgerror.NewErrorf(
		pgcode.Syntax,
		"VALUES lists must all be the same length, expected %d columns, found %d",
		exp, got)
}

// get colType and precision of the column that at index targetCol
func getColWidthAndPrecision(columns []sqlbase.ColumnDescriptor, targetCol int) (int, int) {
	if len(columns) < targetCol {
		return 0, 0
	}
	width := columns[targetCol].ColTypeWidth()
	precision := columns[targetCol].ColTypePrecision()
	return width, precision
}

// get target insert columns' descriptor
func getInsertTargetCols(
	ctx context.Context, p *planner, InsertStmt *tree.Insert,
) ([]sqlbase.ColumnDescriptor, error) {
	tn, _, err := p.getAliasedTableName(InsertStmt.Table)
	if err != nil {
		return nil, err
	}

	// Find which table we're working on, check the permissions.
	TabDesc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}

	// Determine which columns we're inserting into.
	var insertCols []sqlbase.ColumnDescriptor
	if InsertStmt.DefaultValues() {
		insertCols = TabDesc.WritableColumns()
	} else {
		var err error
		if insertCols, err = p.processColumns(TabDesc, InsertStmt.Columns,
			true /* ensureColumns */, false /* allowMutations */); err != nil {
			return nil, err
		}
	}
	// We update the set of columns being inserted into with any computed columns.
	insertCols, _, _, err =
		sqlbase.ProcessComputedColumns(ctx, insertCols, tn, TabDesc, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// We update the set of columns being inserted into with any default values
	// for columns. This needs to happen after we process the computed columns,
	// because `defaultExprs` is expected to line up with the final set of
	// columns being inserted into.
	insertCols, _, err =
		sqlbase.ProcessDefaultColumns(insertCols, TabDesc, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}
	return insertCols, nil
}

func (p *planner) fillRows(
	ctx context.Context,
	i int,
	wg *sync.WaitGroup,
	columns sqlbase.ResultColumns,
	desiredTypes []types.T,
	numcols int,
	tuples [][]tree.TypedExpr,
	rows []tree.Exprs,
	isInsert bool,
	insertOrUpdateCols []sqlbase.ColumnDescriptor,
) {
	tupleBuf := make([]tree.TypedExpr, len(rows)*numcols)

	for num, tuple := range rows {
		if i == 0 && num == 0 {
			continue
		}
		if a, e := len(tuple), numcols; a != e {
			return
		}

		// Chop off prefix of tupleBuf and limit its capacity.
		tupleRow := tupleBuf[:numcols:numcols]
		tupleBuf = tupleBuf[numcols:]

		for j, expr := range tuple {
			desired := types.Any
			if len(desiredTypes) > j {
				desired = desiredTypes[j]
			}

			// Clear the properties so we can check them below.
			typedExpr, err := p.analyzeExpr1(ctx, i, expr, nil, tree.IndexedVarHelper{}, desired, false, "")
			if err != nil {
				return
			}
			typ := typedExpr.ResolvedType()
			if desired != types.Any && typ != desired {
				if ok, c := tree.IsCastDeepValid(typ, desired); ok && !tree.IsNullExpr(typedExpr) {
					telemetry.Inc(c)
					width := 0
					if isInsert {
						width, _ = getColWidthAndPrecision(insertOrUpdateCols, j)
					}
					CastExpr := tree.ChangeToCastExpr(typedExpr, desired, width)
					typedExpr = &CastExpr
					typ = desired
				}
			}

			if columns[j].Typ == types.Unknown {
				columns[j].Typ = typ
			} else if typ != types.Unknown && !typ.Equivalent(columns[j].Typ) {
				return
			}

			tupleRow[j] = typedExpr
		}
		tuples[num] = tupleRow
	}
	wg.Done()
}
