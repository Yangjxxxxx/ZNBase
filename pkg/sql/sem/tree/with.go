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

// With represents a WITH statement.
type With struct {
	Recursive     bool
	StartWith     bool
	CTEList       []*CTE
	RecursiveData *StartWithRecursiveData
}

// CTE represents a common table expression inside of a WITH clause.
type CTE struct {
	Name AliasClause
	Stmt Statement
}

// Format implements the NodeFormatter interface.
func (node *With) Format(ctx *FmtCtx) {
	if node == nil {
		return
	}
	ctx.WriteString("WITH ")
	if node.Recursive {
		ctx.WriteString("RECURSIVE ")
	}
	for i, cte := range node.CTEList {
		if i != 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&cte.Name)
		ctx.WriteString(" AS (")
		ctx.FormatNode(cte.Stmt)
		ctx.WriteString(")")
	}
	ctx.WriteByte(' ')
}

//YaccBuildCET use in YACC to build CET
func YaccBuildCET(sel *SelectClause, tempTblName string, tableExpr TableExprs) *With {
	startWith := sel.StartWith
	connectBy := sel.ConnectBy
	var newWhere string

	name := tempTblName
	recursiveTable, _ := NewUnresolvedObjectName(1, [3]string{name})
	tbName := recursiveTable.ToTableName()

	if startWith == nil {
		newWhere = ""
	} else {
		newWhere = AstWhere
	}

	return &With{
		Recursive: true,
		StartWith: true,
		CTEList: []*CTE{
			{
				Name: AliasClause{Alias: Name(name)},
				Stmt: &Select{
					Select: &UnionClause{
						Type: UnionOp,
						Left: &Select{
							Select: &SelectClause{
								Exprs: SelectExprs{StarSelectExpr()},
								From: &From{
									Tables: tableExpr,
								},
								Where: NewWhere(newWhere, startWith),
							},
						},
						Right: &Select{
							Select: &SelectClause{
								//Exprs: SelectExprs{SelectExpr{Expr: &UnresolvedName{NumParts: 2, Star: true, Parts: NameParts{"", NAME}}}},
								Exprs: SelectExprs{StarSelectExpr()},
								From: &From{
									Tables: append(tableExpr,
										&AliasedTableExpr{Expr: &tbName}),
								},
								Where: NewWhere(AstWhere, connectBy),
							},
						},
					},
				},
			},
		},
	}
}
