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

package tree

import (
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/lex"
)

// Prepare represents a PREPARE statement.
type Prepare struct {
	Name      Name
	Types     []coltypes.T
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (n *Prepare) Format(ctx *FmtCtx) {
	ctx.WriteString("PREPARE ")
	ctx.FormatNode(&n.Name)
	if len(n.Types) > 0 {
		ctx.WriteString(" (")
		for i, t := range n.Types {
			if i > 0 {
				ctx.WriteString(", ")
			}
			if ctx.flags.HasFlags(FmtVisableType) {
				ctx.WriteString(t.ColumnType())
			} else {
				t.Format(&ctx.Buffer, ctx.flags.EncodeFlags())
			}
		}
		ctx.WriteRune(')')
	}
	ctx.WriteString(" AS ")
	ctx.FormatNode(n.Statement)
}

// CannedOptPlan is used as the AST for a PREPARE .. AS OPT PLAN statement.
// This is a testing facility that allows execution (and benchmarking) of
// specific plans. See exprgen package for more information on the syntax.
type CannedOptPlan struct {
	Plan string
}

// Format implements the NodeFormatter interface.
func (n *CannedOptPlan) Format(ctx *FmtCtx) {
	// This node can only be used as the AST for a Prepare statement of the form:
	//   PREPARE name AS OPT PLAN '...').
	ctx.WriteString("OPT PLAN ")
	ctx.WriteString(lex.EscapeSQLString(n.Plan))
}

// Execute represents an EXECUTE statement.
type Execute struct {
	Name   Name
	Params Exprs
	// DiscardRows is set when we want to throw away all the rows rather than
	// returning for client (used for testing and benchmarking).
	DiscardRows bool
}

// Format implements the NodeFormatter interface.
func (n *Execute) Format(ctx *FmtCtx) {
	ctx.WriteString("EXECUTE ")
	ctx.FormatNode(&n.Name)
	if len(n.Params) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&n.Params)
		ctx.WriteByte(')')
	}
	if n.DiscardRows {
		ctx.WriteString(" DISCARD ROWS")
	}
}

// Deallocate represents a DEALLOCATE statement.
type Deallocate struct {
	Name Name // empty for ALL
}

// Format implements the NodeFormatter interface.
func (d *Deallocate) Format(ctx *FmtCtx) {
	ctx.WriteString("DEALLOCATE ")
	if d.Name == "" {
		ctx.WriteString("ALL")
	} else {
		ctx.FormatNode(&d.Name)
	}
}
