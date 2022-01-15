package sql

import (
	"context"
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/opt/exec"
	"github.com/znbasedb/znbase/pkg/sql/opt/optbuilder"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// recursiveCTENode implements the logic for a recursive CTE:
//  1. Evaluate the initial query; emit the results and also save them in
//     a "working" table.
//  2. So long as the working table is not empty:
//     * evaluate the recursive query, substituting the current contents of
//       the working table for the recursive self-reference;
//     * emit all resulting rows, and save them as the next iteration's
//       working table.
// The recursive query tree is regenerated each time using a callback
// (implemented by the execbuilder).
type recursiveCTENode struct {
	initial planNode

	genIterationFn exec.RecursiveCTEIterationFn

	label string

	unionAll bool

	recursiveCTERun
}

type recursiveCTERun struct {
	// workingRows contains the rows produced by the current iteration (aka the
	// "working" table).
	workingRows *rowcontainer.RowContainer
	// nextRowIdx is the index inside workingRows of the next row to be returned
	// by the operator.
	nextRowIdx int

	initialDone bool
	done        bool
}

func (n *recursiveCTENode) startExec(params runParams) error {
	n.workingRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
		0, /* rowCapacity */
	)
	n.nextRowIdx = 0
	return nil
}

var (
	lastWorkingRows            *rowcontainer.RowContainer
	isUnionClauseFitstJoinDone bool
)

func (n *recursiveCTENode) Next(params runParams) (bool, error) {
	var sel *tree.Select
	isStartWith := false
	AST := params.PlanStmtAST()
	switch t := AST.(type) {
	case *tree.Select:
		if unionSel, ok := t.Select.(*tree.UnionClause); ok {
			if unionSel.Left.With != nil && unionSel.Left.With.StartWith && isUnionClauseFitstJoinDone == false {
				sel = unionSel.Left
				isStartWith = true
			}
			if unionSel.Right.With != nil && unionSel.Right.With.StartWith && isUnionClauseFitstJoinDone {
				sel = unionSel.Right
				isStartWith = true
			}
		}

		if t.With != nil && t.With.StartWith {
			sel = t
			isStartWith = true
		}
		if t.With == nil {
			switch s := t.Select.(type) {
			case *tree.SelectClause:
				switch aliase := s.From.Tables[0].(type) {
				case *tree.AliasedTableExpr:
					if Sub, ok := aliase.Expr.(*tree.Subquery); ok {
						if paren, ok := Sub.Select.(*tree.ParenSelect); ok {
							if paren.Select.With != nil && paren.Select.With.StartWith {
								sel = paren.Select
								isStartWith = true
							}
						}
					}

				}
			}
		}
	case *tree.CreateTable:
		if t.AsSource.With != nil && t.AsSource.With.StartWith {
			sel = t.AsSource
			isStartWith = true
		}
	}

	if isStartWith {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		n.nextRowIdx++

		if !n.initialDone {
			for {
				ok, err := n.initial.Next(params)
				if err != nil {
					return false, err
				}
				if ok {
					if _, err = n.workingRows.AddRow(params.ctx, n.initial.Values()); err != nil {
						return false, err
					}
				} else {
					n.initialDone = true
					break
				}
			}
			lastWorkingRows = n.workingRows
			//Join in advance,in order to find the cycle column
			_, err := n.PreJoin(params, sel, lastWorkingRows)
			if err != nil {
				//it will panic without Close
				lastWorkingRows.Close(params.ctx)
				return false, err
			}
			n.workingRows.Close(params.ctx)
			n.workingRows = lastWorkingRows
		}

		if n.nextRowIdx <= n.workingRows.Len() {
			return true, nil
		}

		if n.done {
			return false, nil
		}

		if n.workingRows.Len() == 0 {
			// Last iteration returned no rows.
			n.done = true
			return false, nil
		}

		lastWorkingRowsOne := n.workingRows

		n.workingRows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
			0, /* rowCapacity */
		)

		// Set up a bufferNode that can be used as a reference for a scanBufferNode.
		buf := &bufferNode{
			// The plan here is only useful for planColumns, so it's ok to always use
			// the initial plan.
			plan:         n.initial,
			bufferedRows: lastWorkingRowsOne,
			label:        n.label,
		}
		newPlan, err := n.genIterationFn(buf)
		if err != nil {
			return false, err
		}

		if sel != nil && sel.With != nil && sel.With.RecursiveData != nil {
			sel.With.RecursiveData.FormalJoin = true
		}
		// formal join
		if err := runPlanInsidePlan(params, newPlan.(*planComponents), n.workingRows); err != nil {
			//it will panic without Close
			lastWorkingRowsOne.Close(params.ctx)
			return false, err
		}

		n.workingRows.MemAccClose(params.ctx)

		//recursive done
		if n.workingRows.Len() == 0 {
			lastWorkingRows.Close(params.ctx)
			return n.workingRows.Len() > 0, nil
		}

		lastWorkingRowsTwo := n.workingRows

		// Pre join
		_, err = n.PreJoin(params, sel, lastWorkingRowsTwo)
		if err != nil {
			//it will panic without Close
			lastWorkingRows.Close(params.ctx)
			return false, err
		}

		n.workingRows.Close(params.ctx)
		n.workingRows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
			0, /* rowCapacity */
		)
		n.workingRows = lastWorkingRowsTwo

		n.nextRowIdx = 1
		return n.workingRows.Len() > 0, nil
	}
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}
	n.nextRowIdx++

	if !n.initialDone {
		ok, err := n.initial.Next(params)
		if err != nil {
			return false, err
		}
		if ok {
			if _, err = n.workingRows.AddRow(params.ctx, n.initial.Values()); err != nil {
				return false, err
			}
			return true, nil
		}
		n.initialDone = true
	}

	if n.done {
		return false, nil
	}

	if n.workingRows.Len() == 0 {
		// Last iteration returned no rows.
		n.done = true
		return false, nil
	}

	// There are more rows to return from the last iteration.
	if n.nextRowIdx <= n.workingRows.Len() {
		return true, nil
	}

	// Let's run another iteration.

	lastWorkingRows := n.workingRows
	defer lastWorkingRows.Close(params.ctx)

	n.workingRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
		0, /* rowCapacity */
	)

	// Set up a bufferNode that can be used as a reference for a scanBufferNode.
	buf := &bufferNode{
		// The plan here is only useful for planColumns, so it's ok to always use
		// the initial plan.
		plan:         n.initial,
		bufferedRows: lastWorkingRows,
		label:        n.label,
	}
	newPlan, err := n.genIterationFn(buf)
	if err != nil {
		return false, err
	}

	if err := runPlanInsidePlan(params, newPlan.(*planComponents), n.workingRows); err != nil {
		return false, err
	}

	if !n.unionAll {
		ret := false
		if n.workingRows.Len() == lastWorkingRows.Len() {
			for i := 0; i < n.workingRows.Len(); i++ {
				ret = n.workingRows.At(i).IsDistinctFrom(params.EvalContext(), lastWorkingRows.At(i))
				if ret {
					break
				}
			}
			if !ret {
				n.done = true
				return false, nil
			}
		}
	}
	n.nextRowIdx = 1
	return n.workingRows.Len() > 0, nil

}

func (n *recursiveCTENode) Values() tree.Datums {
	return n.workingRows.At(n.nextRowIdx - 1)
}

func (n *recursiveCTENode) Close(ctx context.Context) {
	n.initial.Close(ctx)
	if n.workingRows != nil {
		n.workingRows.Close(ctx)
	}
}

func (n *recursiveCTENode) IsUnionAll() bool {
	return n.unionAll
}

func (n *recursiveCTENode) PreJoin(
	params runParams, sel *tree.Select, lastWorkingRows *rowcontainer.RowContainer,
) (bool, error) {
	n.workingRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
		0, /* rowCapacity */
	)

	// Set up a bufferNode that can be used as a reference for a scanBufferNode.
	buf := &bufferNode{
		// The plan here is only useful for planColumns, so it's ok to always use
		// the initial plan.
		plan:         n.initial,
		bufferedRows: lastWorkingRows,
		label:        n.label,
	}
	newPlan, err := n.genIterationFn(buf)
	if err != nil {
		return false, err
	}

	// initial join
	if err := runPlanInsidePlan(params, newPlan.(*planComponents), n.workingRows); err != nil {
		return false, err
	}

	talColID := sel.With.RecursiveData.TalColID
	priorColID := sel.With.RecursiveData.PriorColID
	cycleID := sel.With.RecursiveData.CycleID
	isLeafID := sel.With.RecursiveData.IsLeafID
	cycleStmt := sel.With.RecursiveData.CycleStmt
	operator := sel.With.RecursiveData.Operator

	for i := 0; i < n.workingRows.Len(); i++ {
		row := n.workingRows.At(i)
		j := 0
		if _, ok := row[len(row)-1].(*tree.DInt); ok {
			j = 2
		} else {
			j = 1
		}
		if s, ok := row[len(row)-j].(*tree.DString); ok {
			parts := strings.Split(string(*s), optbuilder.DELIMITER)
			if parts[0] == "" {
				parts = parts[1:]
			}
			for key, value := range parts {
				for j := key; j < len(parts)-1; j++ {
					//if Nocycle ,CycleStopFlag = true stop sycle; else ISCycle = true report error
					if value == parts[j+1] && cycleStmt {
						for k := 0; k < lastWorkingRows.Len(); k++ {
							lastDatum := lastWorkingRows.At(k)
							if row[talColID].String() != "NULL" && lastDatum[priorColID].String() != "NULL" {
								switch operator {
								case tree.EQ:
									if row[talColID].String() == lastDatum[priorColID].String() {
										lastDatum[cycleID] = tree.NewDInt(1)
									}
								case tree.LT:
									if lastDatum[priorColID].String() < row[talColID].String() {
										lastDatum[cycleID] = tree.NewDInt(1)
									}
								case tree.GT:
									if lastDatum[priorColID].String() > row[talColID].String() {
										lastDatum[cycleID] = tree.NewDInt(1)
									}
								case tree.LE:
									if lastDatum[priorColID].String() <= row[talColID].String() {
										lastDatum[cycleID] = tree.NewDInt(1)
									}
								case tree.GE:
									if lastDatum[priorColID].String() >= row[talColID].String() {
										lastDatum[cycleID] = tree.NewDInt(1)
									}
								case tree.NE:
									if row[talColID].String() != lastDatum[priorColID].String() {
										lastDatum[cycleID] = tree.NewDInt(1)
									}
								}
							} else {
								if row[talColID].String() == lastDatum[priorColID].String() {
									lastDatum[cycleID] = tree.NewDInt(1)
								}
							}
						}
					} else if value == parts[j+1] {
						sel.With.RecursiveData.ISCycle = true
					}
				}
			}
		}
	}

	for k := 0; k < lastWorkingRows.Len(); k++ {
		lastDatum := lastWorkingRows.At(k)
		j := 0
		if _, ok := lastDatum[len(lastDatum)-1].(*tree.DInt); ok {
			j = 2
		} else {
			j = 1
		}

		var lastDatumStr string
		if s, ok := lastDatum[len(lastDatum)-j].(*tree.DString); ok {
			lastparts := strings.Split(string(*s), optbuilder.DELIMITER)
			if lastparts[0] == "" {
				for _, value := range lastparts[1:] {
					lastDatumStr += value
				}
			}

			for i := 0; i < n.workingRows.Len(); i++ {
				row := n.workingRows.At(i)
				j := 0
				if _, ok := row[len(row)-1].(*tree.DInt); ok {
					j = 2
				} else {
					j = 1
				}

				var rowDatumStr string
				if s, ok := row[len(lastDatum)-j].(*tree.DString); ok {
					rowparts := strings.Split(string(*s), optbuilder.DELIMITER)
					if rowparts[0] == "" {
						for _, value := range rowparts[1 : len(rowparts)-1] {
							rowDatumStr += value
						}
					}
					isSame := false
					if lastDatumStr == rowDatumStr {
						for _, value := range lastparts {
							if value == rowparts[len(rowparts)-1] {
								isSame = true
							}
						}
						if !isSame {
							lastDatum[isLeafID] = tree.NewDInt(0)
						}
					}
				}
			}
		}
	}
	return false, nil
}
