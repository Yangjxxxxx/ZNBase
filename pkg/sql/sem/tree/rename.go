// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import "github.com/znbasedb/znbase/pkg/security/audit/util"

// RenameDatabase represents a RENAME DATABASE statement.
type RenameDatabase struct {
	Name    Name
	NewName Name
}

// Format implements the NodeFormatter interface.
func (n *RenameDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&n.Name)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&n.NewName)
}

// RenameTrigger represents an ALTER TRIGGER statement.
type RenameTrigger struct {
	Trigname  Name
	Tablename TableName
	Newname   Name
	IfExists  bool
}

// Format implements the NodeFormatter interface.
func (node *RenameTrigger) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TRIGGER ")
	ctx.FormatNode(&node.Trigname)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.Tablename)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.Newname)
}

// RenameFunction represents an ALTER PROCEDURE statement.
type RenameFunction struct {
	FuncName     TableName
	Parameters   FunctionParameters
	NewName      TableName
	HasNewSchema bool
	MissingOk    bool
	IsFunction   bool
}

func (n *RenameFunction) String() string { return n.NewName.Table() }

// StatementTag returns a short string identifying the type of statement.
func (n *RenameFunction) StatementTag() string {
	if n.IsFunction {
		return "ALTER FUNCTION NAME"
	}
	return "ALTER PROCEDURE NAME"
}

// StatAbbr implements the StatAbbr interface.
func (n *RenameFunction) StatAbbr() string {
	if n.IsFunction {
		return "alter_function_name"
	}
	return "alter_procedure_name"
}

// StatObj implements the StatObj interface.
func (n *RenameFunction) StatObj() string {
	return util.StringConcat("-", n.FuncName.Table())
}

// StatementType implements the Statement interface.
func (n *RenameFunction) StatementType() StatementType {
	return DDL
}

// Format implements the NodeFormatter interface.
func (n *RenameFunction) Format(ctx *FmtCtx) {
	if n.IsFunction {
		ctx.WriteString("ALTER FUNCTION ")
	} else {
		ctx.WriteString("ALTER PROCEDURE ")
	}
	ctx.FormatNode(&n.FuncName)
	//ctx.WriteString("(")
	str, err := n.Parameters.GetFuncParasTypeStr()
	if err != nil {
		return
	}
	ctx.WriteString(str)
	//for k, i := range n.Parameters {
	//	if k != 0 {
	//		ctx.WriteString(",")
	//	}
	//	ctx.WriteString(i.Name)
	//}
	//ctx.WriteString(")")
	ctx.FormatNode(&n.Parameters)
	ctx.WriteString(" RENAME  TO ")
	ctx.FormatNode(&n.NewName)
}

// RenameTable represents a RENAME TABLE or RENAME VIEW statement.
// Whether the user has asked to rename a table or view is indicated
// by the IsView field.
type RenameTable struct {
	Name       TableName
	NewName    TableName
	IfExists   bool
	IsView     bool
	IsSequence bool

	IsMaterialized bool
}

// Format implements the NodeFormatter interface.
func (r *RenameTable) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if r.IsView {
		if r.IsMaterialized {
			ctx.WriteString("MATERIALIZED ")
		}
		ctx.WriteString("VIEW ")
	} else if r.IsSequence {
		ctx.WriteString("SEQUENCE ")
	} else {
		ctx.WriteString("TABLE ")
	}
	if r.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&r.Name)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&r.NewName)
}

// RenameIndex represents a RENAME INDEX statement.
type RenameIndex struct {
	Index    *TableIndexName
	NewName  UnrestrictedName
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (n *RenameIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER INDEX ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(n.Index)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&n.NewName)
}

// RenameColumn represents a RENAME COLUMN statement.
type RenameColumn struct {
	Table   TableName
	Name    Name
	NewName Name
	// IfExists refers to the table, not the column.
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (n *RenameColumn) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TABLE ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Table)
	ctx.WriteString(" RENAME COLUMN ")
	ctx.FormatNode(&n.Name)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&n.NewName)
}
