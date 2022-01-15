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
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

var errEmptyColumnName = pgerror.NewError(pgcode.Syntax, "empty column name")

type renameColumnNode struct {
	n         *tree.RenameColumn
	tableDesc *sqlbase.MutableTableDescriptor
}

// RenameColCmds 表示子表要执行某个ddl语句的次数
type RenameColCmds struct {
	desc     *MutableTableDescriptor
	cmdCount int
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(ctx context.Context, n *tree.RenameColumn) (planNode, error) {
	// Check if table exists.
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.REFERENCES); err != nil {
		return nil, err
	}

	return &renameColumnNode{n: n, tableDesc: tableDesc}, nil
}

func (n *renameColumnNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc

	descChanged, err := params.p.renameColumn(params.ctx, tableDesc, &n.n.Name, &n.n.NewName)
	if err != nil {
		return err
	}

	if !descChanged {
		return nil
	}

	if err := tableDesc.RefreshValidate(ctx, p.txn, p.EvalContext().Settings); err != nil {
		return err
	}

	return p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
}

func (p *planner) renameColumn(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, oldName, newName *tree.Name,
) (bool, error) {
	if *newName == "" {
		return false, errEmptyColumnName
	}

	col, _, err := tableDesc.FindColumnByName(*oldName)
	if err != nil {
		return false, err
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.ID {
				found = true
			}
		}
		if found {
			return false, p.dependentViewRenameError(
				ctx, "column", oldName.String(), tableDesc.ParentID, tableRef.ID)
		}
	}

	if *oldName == *newName {
		// Noop.
		return false, nil
	}

	if col, _, err := tableDesc.FindColumnByName(*newName); err == nil {
		if col.Hidden {
			return false, fmt.Errorf("column name %q already exists but is hidden", tree.ErrString(newName))
		}
		return false, fmt.Errorf("column name %q already exists", tree.ErrString(newName))
	}

	if !(col.InhCount == 1 && !col.IsInherits) {
		return false, fmt.Errorf(`cannot rename inherited column "%s"`, col.Name)
	}

	var alterTableMap = make(map[sqlbase.ID]*RenameColCmds)
	for _, id := range tableDesc.InheritsBy {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return false, err
		}
		if inhTableDesc == nil {
			return false, fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		if _, ok := alterTableMap[id]; ok {
			alterTableMap[id].cmdCount++
		} else {
			alterTableMap[id] = &RenameColCmds{desc: inhTableDesc, cmdCount: 1}
		}
		err = p.addInhCmds(ctx, inhTableDesc, alterTableMap)
		if err != nil {
			return false, err
		}
	}

	for id := range alterTableMap {
		err = p.renameInhTableColumn(ctx, alterTableMap[id].desc, oldName, newName, alterTableMap[id].cmdCount)
		if err != nil {
			return false, err
		}
	}

	preFn := func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return err, false, nil
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				if string(c.ColumnName) == string(*oldName) {
					c.ColumnName = *newName
				}
			}
			return nil, false, v
		}
		return nil, true, expr
	}

	renameIn := func(expression string) (string, error) {
		parsed, err := parser.ParseExpr(expression)
		if err != nil {
			return "", err
		}

		renamed, err := tree.SimpleVisit(parsed, preFn)
		if err != nil {
			return "", err
		}

		return renamed.String(), nil
	}

	// Rename the column in CHECK constraints.
	// Renaming columns that are being referenced by checks that are being added is not allowed.
	for i := range tableDesc.Checks {
		var err error
		tableDesc.Checks[i].Expr, err = renameIn(tableDesc.Checks[i].Expr)
		if err != nil {
			return false, err
		}
	}

	// Rename the column in computed columns.
	for i := range tableDesc.Columns {
		if tableDesc.Columns[i].IsComputed() {
			newExpr, err := renameIn(*tableDesc.Columns[i].ComputeExpr)
			if err != nil {
				return false, err
			}
			tableDesc.Columns[i].ComputeExpr = &newExpr
		}
	}

	for i := range tableDesc.Indexes {
		if index := &tableDesc.Indexes[i]; index.IsPartial() {
			newExpr, err := schemaexpr.RenameColumn(index.PredExpr, *oldName, *newName)
			if err != nil {
				return false, err
			}
			index.PredExpr = newExpr
		}
	}

	// Rename the column in the indexes.
	tableDesc.RenameColumnDescriptor(col, string(*newName))

	return true, nil
}

func (n *renameColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameColumnNode) Close(context.Context)        {}

func (p *planner) renameInhTableColumn(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	oldName, newName *tree.Name,
	cmdCount int,
) error {
	col, _, err := tableDesc.FindColumnByName(*oldName)
	if err != nil {
		return err
	}
	var count = col.InhCount
	if !col.IsInherits {
		count--
	}
	if count > uint32(cmdCount) {
		return fmt.Errorf(`cannot rename inherited column "%s"`, col.Name)
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		found := false
		for _, colID := range tableRef.ColumnIDs {
			if colID == col.ID {
				found = true
			}
		}
		if found {
			return p.dependentViewRenameError(
				ctx, "column", oldName.String(), tableDesc.ParentID, tableRef.ID)
		}
	}

	if _, _, err := tableDesc.FindColumnByName(*newName); err == nil {
		return fmt.Errorf(`column name %q in inherit table %q already exists`, tree.ErrString(newName), tableDesc.Name)
	}

	preFn := func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return err, false, nil
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				if string(c.ColumnName) == string(*oldName) {
					c.ColumnName = *newName
				}
			}
			return nil, false, v
		}
		return nil, true, expr
	}

	renameIn := func(expression string) (string, error) {
		parsed, err := parser.ParseExpr(expression)
		if err != nil {
			return "", err
		}

		renamed, err := tree.SimpleVisit(parsed, preFn)
		if err != nil {
			return "", err
		}

		return renamed.String(), nil
	}

	// Rename the column in CHECK constraints.
	// Renaming columns that are being referenced by checks that are being added is not allowed.
	for i := range tableDesc.Checks {
		var err error
		tableDesc.Checks[i].Expr, err = renameIn(tableDesc.Checks[i].Expr)
		if err != nil {
			return err
		}
	}

	// Rename the column in computed columns.
	for i := range tableDesc.Columns {
		if tableDesc.Columns[i].IsComputed() {
			newExpr, err := renameIn(*tableDesc.Columns[i].ComputeExpr)
			if err != nil {
				return err
			}
			tableDesc.Columns[i].ComputeExpr = &newExpr
		}
	}

	// Rename the column in the indexes.
	tableDesc.RenameColumnDescriptor(col, string(*newName))

	err = p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
	return err
}

func (p *planner) addInhCmds(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, tableMap map[sqlbase.ID]*RenameColCmds,
) error {
	for _, id := range desc.InheritsBy {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inhTableDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		if _, ok := tableMap[id]; ok {
			tableMap[id].cmdCount++
		} else {
			tableMap[id] = &RenameColCmds{desc: inhTableDesc, cmdCount: 1}
		}

		err = p.addInhCmds(ctx, inhTableDesc, tableMap)
		if err != nil {
			return err
		}
	}
	return nil
}
