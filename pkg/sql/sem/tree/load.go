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

package tree

import "github.com/znbasedb/znbase/pkg/util/pretty"

// LoadImport represents a IMPORT statement.
type LoadImport struct {
	Table           *TableName
	Database        string
	SchemaName      *SchemaName
	Into            bool
	IntoCols        NameList
	CreateFile      Expr
	CreateDefs      TableDefs
	PartitionBy     *PartitionBy
	LocateSpaceName *Location
	FileFormat      string
	Files           Exprs
	Bundle          bool
	Options         KVOptions
	Settings        bool
	Conversions     ConversionFunctions
}

// ConversionFunc contains the convert func
type ConversionFunc struct {
	FuncName   string
	ColumnName Name
	Params     Exprs
}

// Format implements the NodeFormatter interface.
func (n *ConversionFunc) Format(ctx *FmtCtx) {
	ctx.WriteString("SET ")
	ctx.FormatNode(&n.ColumnName)
	ctx.WriteRune('=')
	ctx.WriteString(" " + n.FuncName + " ")
	ctx.WriteRune('<')
	ctx.FormatNode(&n.Params)
	ctx.WriteRune('>')
}

//ConversionFunctions slice of ConversionFunc
type ConversionFunctions []ConversionFunc

// Format implements the NodeFormatter interface.
func (node *ConversionFunctions) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&n)
	}
}

var _ Statement = &LoadImport{}

// StatementType implements the Statement interface.
func (n *LoadImport) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*LoadImport) StatementTag() string { return "LOAD" }

func (n *LoadImport) String() string { return AsString(n) }

// Format implements the NodeFormatter interface.
func (n *LoadImport) Format(ctx *FmtCtx) {
	ctx.WriteString("LOAD ")

	if n.Bundle {
		if n.Table != nil {
			ctx.WriteString("TABLE ")
			ctx.FormatNode(n.Table)
			ctx.WriteString(" FROM ")
		}
		ctx.WriteString(n.FileFormat)
		ctx.WriteByte(' ')
		ctx.FormatNode(&n.Files)
	} else {
		if n.Into {
			if n.Settings {
				ctx.WriteString("")
			} else {
				ctx.WriteString("INTO ")
				ctx.FormatNode(n.Table)
				if n.IntoCols != nil {
					ctx.WriteByte('(')
					ctx.FormatNode(&n.IntoCols)
					ctx.WriteString(") ")
				} else {
					ctx.WriteString(" ")
				}
			}
		} else {
			if n.Table != nil {
				ctx.WriteString("TABLE ")
				ctx.FormatNode(n.Table)

				if n.CreateFile != nil {
					ctx.WriteString(" CREATE USING ")
					ctx.FormatNode(n.CreateFile)
					ctx.WriteString(" ")
				} else {
					ctx.WriteString(" (")
					ctx.FormatNode(&n.CreateDefs)
					ctx.WriteString(") ")
					if n.PartitionBy != nil {
						ctx.FormatNode(n.PartitionBy)
					}
					if n.LocateSpaceName != nil {
						ctx.FormatNode(n.LocateSpaceName)
					}
				}
			} else if n.Database != "" {
				ctx.WriteString("DATABASE ")
				ctx.WriteString(n.Database)
				if n.CreateFile != nil {
					ctx.WriteString(" CREATE USING ")
					ctx.FormatNode(n.CreateFile)
					ctx.WriteString(" ")
				}
			} else if n.SchemaName != nil {
				ctx.WriteString("SCHEMA ")
				ctx.WriteString(n.SchemaName.Schema())
				if n.CreateFile != nil {
					ctx.WriteString(" CREATE USING ")
					ctx.FormatNode(n.CreateFile)
					ctx.WriteString(" ")
				}
			}
		}

		ctx.WriteString(n.FileFormat)
		ctx.WriteString(" DATA (")
		ctx.FormatNode(&n.Files)
		ctx.WriteString(")")

		if n.Conversions != nil {
			ctx.FormatNode(&n.Conversions)
		}

		if n.Options != nil {
			ctx.WriteString(" WITH ")
			ctx.FormatNode(&n.Options)
		}
	}
}

func (p *PrettyCfg) bracket(l string, d pretty.Doc, r string) pretty.Doc {
	return p.bracketDoc(pretty.Text(l), d, pretty.Text(r))
}

func (p *PrettyCfg) bracketDoc(l, d, r pretty.Doc) pretty.Doc {
	return pretty.BracketDoc(l, d, r)
}

func (n *LoadImport) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.RLTableRow, 0, 5)
	items = append(items, p.row("Load", pretty.Nil))

	if n.Bundle {
		if n.Table != nil {
			items = append(items, p.row("TABLE", p.Doc(n.Table)))
			items = append(items, p.row("FROM", pretty.Nil))
		}
		items = append(items, p.row(n.FileFormat, p.Doc(&n.Files)))
	} else {
		if n.Into {
			into := p.Doc(n.Table)
			if n.IntoCols != nil {
				into = p.nestUnder(into, p.bracket("(", p.Doc(&n.IntoCols), ")"))
			}
			items = append(items, p.row("INTO", into))
		} else {
			if n.CreateFile != nil {
				items = append(items, p.row("TABLE", p.Doc(n.Table)))
				items = append(items, p.row("CREATE USING", p.Doc(n.CreateFile)))
			} else {
				table := pretty.BracketDoc(
					pretty.ConcatSpace(p.Doc(n.Table), pretty.Text("(")),
					p.Doc(&n.CreateDefs),
					pretty.Text(")"),
				)
				items = append(items, p.row("TABLE", table))
			}
		}

		data := prettyBracketKeyword(
			"DATA", " (",
			p.Doc(&n.Files),
			")", "",
		)
		items = append(items, p.row(n.FileFormat, data))
	}
	if n.Options != nil {
		items = append(items, p.row("WITH", p.Doc(&n.Options)))
	}
	return p.rlTable(items...)
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *LoadImport) copyNode() *LoadImport {
	stmtCopy := *n
	stmtCopy.Files = append(Exprs(nil), n.Files...)
	stmtCopy.Options = append(KVOptions(nil), n.Options...)
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *LoadImport) walkStmt(v Visitor) Statement {
	ret := n
	if n.CreateFile != nil {
		e, changed := WalkExpr(v, n.CreateFile)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.CreateFile = e
		}
	}
	for i, expr := range n.Files {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Files[i] = e
		}
	}
	{
		opts, changed := walkKVOptions(v, n.Options)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Options = opts
		}
	}
	return ret
}

// LoadRestore represents a RESTORE statement.
type LoadRestore struct {
	TableName          *TableName
	DatabaseName       string
	SchemaName         *SchemaName
	Files              Exprs
	AsOf               AsOfClause
	Options            KVOptions
	DescriptorCoverage DescriptorCoverage
}

var _ Statement = &LoadRestore{}

// Format implements the NodeFormatter interface.
func (node *LoadRestore) Format(ctx *FmtCtx) {
	ctx.WriteString("LOAD ")
	if node.DescriptorCoverage == RequestedDescriptors {
		if node.TableName != nil {
			ctx.WriteString("TABLE ")
			ctx.FormatNode(node.TableName)
		} else if node.SchemaName != nil {
			ctx.WriteString("SCHEMA ")
			ctx.FormatNode(node.SchemaName)
		} else {
			ctx.WriteString("DATABASE ")
			ctx.WriteString(node.DatabaseName)
		}
	}
	ctx.WriteString(" FROM ")
	ctx.FormatNode(&node.Files)
	if node.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// StatementType implements the Statement interface.
func (*LoadRestore) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*LoadRestore) StatementTag() string { return "LOAD" }

func (*LoadRestore) hiddenFromShowQueries() {}

func (node *LoadRestore) String() string { return AsString(node) }

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (node *LoadRestore) copyNode() *LoadRestore {
	stmtCopy := *node
	stmtCopy.Files = append(Exprs(nil), node.Files...)
	stmtCopy.Options = append(KVOptions(nil), node.Options...)
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (node *LoadRestore) walkStmt(v Visitor) Statement {
	ret := node
	if node.AsOf.Expr != nil {
		e, changed := WalkExpr(v, node.AsOf.Expr)
		if changed {
			if ret == node {
				ret = node.copyNode()
			}
			ret.AsOf.Expr = e
		}
	}
	for i, expr := range node.Files {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == node {
				ret = node.copyNode()
			}
			ret.Files[i] = e
		}
	}
	{
		opts, changed := walkKVOptions(v, node.Options)
		if changed {
			if ret == node {
				ret = node.copyNode()
			}
			ret.Options = opts
		}
	}
	return ret
}

func (node *LoadRestore) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.RLTableRow, 0, 5)

	items = append(items, p.row("LOAD", pretty.Nil))
	if node.TableName != nil {
		items = append(items, p.row("TABLE", p.Doc(node.TableName)))
	} else if node.SchemaName != nil {
		items = append(items, p.row("SCHEMA", pretty.Nil))
		items = append(items, p.row(node.SchemaName.String(), pretty.Nil))
	} else {
		items = append(items, p.row("DATABASE", pretty.Nil))
		items = append(items, p.row(node.DatabaseName, pretty.Nil))
	}
	//      items = append(items, node.Targets.docRow(p))
	items = append(items, p.row("FROM", p.Doc(&node.Files)))

	if node.AsOf.Expr != nil {
		items = append(items, node.AsOf.docRow(p))
	}
	if node.Options != nil {
		items = append(items, p.row("WITH", p.Doc(&node.Options)))
	}
	return p.rlTable(items...)
}
