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

// DropBehavior represents options for dropping schema elements.
type DropBehavior int

// DropBehavior values.
const (
	DropDefault DropBehavior = iota
	DropRestrict
	DropCascade
)

var dropBehaviorName = [...]string{
	DropDefault:  "",
	DropRestrict: "RESTRICT",
	DropCascade:  "CASCADE",
}

func (d DropBehavior) String() string {
	return dropBehaviorName[d]
}

// DropDatabase represents a DROP DATABASE statement.
type DropDatabase struct {
	Name         Name
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (n *DropDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP DATABASE ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Name)
	if n.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(n.DropBehavior.String())
	}
}

//DropTrigger used to drop  trigger
type DropTrigger struct {
	Trigname     Name
	Tablename    TableName
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropTrigger) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP TRIGGER ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Trigname)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.Tablename)
}

// DropIndex represents a DROP INDEX statement.
type DropIndex struct {
	IndexList    TableIndexNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (n *DropIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP INDEX ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.IndexList)
	if n.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(n.DropBehavior.String())
	}
}

// DropTable represents a DROP TABLE statement.
type DropTable struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (n *DropTable) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP TABLE ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Names)
	if n.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(n.DropBehavior.String())
	}
}

// DropView represents a DROP VIEW statement.
type DropView struct {
	Names          TableNames
	IfExists       bool
	DropBehavior   DropBehavior
	IsMaterialized bool
}

// Format implements the NodeFormatter interface.
func (n *DropView) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP ")
	if n.IsMaterialized {
		ctx.WriteString("MATERIALIZED ")
	}
	ctx.WriteString("VIEW ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Names)
	if n.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(n.DropBehavior.String())
	}
}

// DropSequence represents a DROP SEQUENCE statement.
type DropSequence struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (n *DropSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP SEQUENCE ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Names)
	if n.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(n.DropBehavior.String())
	}
}

// DropUser represents a DROP USER statement
type DropUser struct {
	Names    Exprs
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (n *DropUser) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP USER ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Names)
}

// DropRole represents a DROP ROLE statement
type DropRole struct {
	Names    Exprs
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (n *DropRole) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP ROLE ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Names)
}

// DropSchema represents a DROP SCHEMA statement.
type DropSchema struct {
	Schemas      SchemaList
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (n *DropSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP SCHEMA ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Schemas)
	if n.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(n.DropBehavior.String())
	}
}
