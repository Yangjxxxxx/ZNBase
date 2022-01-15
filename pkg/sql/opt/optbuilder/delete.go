// Copyright 2018  The Cockroach Authors.
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

package optbuilder

import (
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// buildDelete builds a memo group for a DeleteOp expression, which deletes all
// rows projected by the input expression. All columns from the deletion table
// are projected, including mutation columns (the optimizer may later prune the
// columns if they are not needed).
//
// Note that the ORDER BY clause can only be used if the LIMIT clause is also
// present. In that case, the ordering determines which rows are included by the
// limit. The ORDER BY makes no additional guarantees about the order in which
// mutations are applied, or the order of any returned rows (i.e. it won't
// become a physical property required of the Delete operator).
func (b *Builder) buildDelete(del *tree.Delete, inScope *scope) (outScope *scope) {
	// UX friendliness safeguard.
	if del.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(pgerror.NewDangerousStatementErrorf("DELETE without WHERE clause"))
	}

	if del.OrderBy != nil && del.Limit == nil {
		panic(pgerror.NewErrorf(pgcode.Syntax,
			"DELETE statement requires LIMIT when ORDER BY is used"))
	}

	var ctes []cteSource
	if del.With != nil {
		inScope, ctes = b.buildCTEs(del.With, inScope, nil)
	}

	// DELETE FROM xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias := getAliasedTableName(del.Table)

	// Find which table we're working on, check the permissions access to database and schema.
	tab, tabXQ, resName, viewFlag, _, NotMutilView := b.resolveTable(tn, privilege.DELETE)

	var hasAlias bool
	if alias == nil {
		alias = &resName
		hasAlias = false
	} else {
		hasAlias = true
	}

	if viewFlag {
		if !NotMutilView {
			panic(pgerror.NewErrorf(pgcode.Syntax, "feature unsupported"))
		}
		columnAST := make(map[string]tree.Expr)
		ds, _ := b.resolveDataSource(tn, privilege.DELETE)
		b.checkPrivilegeForUser(tn, ds, nil, privilege.DELETE, "")

		view := ds.(cat.View)
		b.upperOwner = view.GetOwner()
		b.writeOnView = true
		stmt, err := parser.ParseOne(view.Query(), b.CaseSensitive)
		if err != nil {
			panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
		}
		//获取建视图的select语句并进行语义解析
		if stat, ok := stmt.AST.(*tree.Select).Select.(*tree.SelectClause); ok {
			if tables, ok := stat.From.Tables[0].(*tree.AliasedTableExpr); ok {
				if tableName, ok := tables.Expr.(*tree.TableName); ok {
					inScope.UpdatableViewDepends.TableName = append(inScope.UpdatableViewDepends.TableName, tableName.TableName, tableName.SchemaName, tableName.CatalogName)
				}
			}
			if inScope.UpdatableViewDepends.TableName != nil {
				b.nameAST(stat, inScope.UpdatableViewDepends.TableName)
			}
		}
		if stat, ok := stmt.AST.(*tree.Select); ok {
			if StatExpr, ok := stat.Select.(*tree.SelectClause); ok {
				for i := range StatExpr.Exprs {
					columnAST[string(view.ColumnName(i))] = StatExpr.Exprs[i].Expr
				}
			}
		}

		inScope.UpdatableViewDepends.ViewDependsColExpr = columnAST
		tnName := make([]tree.Name, 4)
		copy(tnName, inScope.UpdatableViewDepends.TableName)
		if hasAlias {
			tnName[0] = alias.TableName
		} else {
			tnName[0] = tn.TableName
		}
		tnName[3] = inScope.UpdatableViewDepends.TableName[0]
		if asName, ok := del.Table.(*tree.AliasedTableExpr); ok {
			if asName.As.Alias != "" {
				tnName[0] = asName.As.Alias
			}
		}

		if del.Where != nil {
			err = b.replaceAST(del.Where.Expr, inScope, tnName, false, ds)
			if err != nil {
				panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
			}
		}

		// check delete privilege for view owner
		b.checkPrivilegeForUser(tab.Name(), tab, nil, privilege.DELETE, view.GetOwner())

		// check select privilege for view owner on select clause
		var viewSelectCols tree.NameList
		for i := range columnAST {
			if name, ok := columnAST[i].(*tree.UnresolvedName); ok {
				viewSelectCols = append(viewSelectCols, tree.Name(name.Parts[0]))
			}
		}
		b.checkPrivilegeForUser(tab.Name(), tab, viewSelectCols, privilege.SELECT, view.GetOwner())

		if stat, ok := stmt.AST.(*tree.Select).Select.(*tree.SelectClause); ok {
			if stat.Where != nil {
				b.addCheckType(stat.Where.Expr, inScope)
				if del.Where == nil {
					del.Where = stat.Where
				} else {
					del.Where.Expr = rebuildWhere(stat.Where.Expr, del.Where.Expr)
				}
			}
		}
		if del.Returning != nil {
			switch p := del.Returning.(type) {
			case *tree.ReturningExprs:
				selectExprRet := tree.SelectExprs(*p)
				for i := range selectExprRet {
					switch returnType := selectExprRet[i].Expr.(type) {
					case *tree.UnresolvedName:
						name, ok, err := b.changedAST(returnType, columnAST, tnName, false, view)
						if err != nil {
							panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
						}
						if ok {
							addChangedCol(inScope, name, CheckOwner, true)
							selectExprRet[i].Expr = columnAST[name]
						}
					case tree.UnqualifiedStar:
						panic(pgerror.NewErrorf(pgcode.Syntax, "feature unsupported *"))
					default:
						err = b.replaceAST(returnType, inScope, tnName, false, ds)
						if err != nil {
							panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
						}
					}
				}
			default:
			}
		}
		if del.OrderBy != nil {
			for i := range del.OrderBy {
				if orderByColumnName, ok := del.OrderBy[i].Expr.(*tree.UnresolvedName); ok {
					name, ok, err := b.changedAST(orderByColumnName, columnAST, tnName, false, view)
					if err != nil {
						panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
					}
					if ok {
						addChangedCol(inScope, name, CheckOwner, true)
						del.OrderBy[i].Expr = columnAST[name]
					}
				} else {
					err = b.replaceAST(del.OrderBy[i].Expr, inScope, tnName, false, ds)
					if err != nil {
						panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
					}
				}
			}
		}

		if inScope.UpdatableViewDepends.TableName != nil {
			alias.TableName = inScope.UpdatableViewDepends.TableName[0]
			alias.SchemaName = inScope.UpdatableViewDepends.TableName[1]
			alias.CatalogName = inScope.UpdatableViewDepends.TableName[2]
			alias.ExplicitCatalog = true
			alias.ExplicitSchema = true
		}
	} else {
		b.checkPrivilegeForUser(tn, tab, nil, privilege.DELETE, "")

		// Check Select permission as well, since existing values must be read.
		b.checkPrivilegeForUser(tn, tab, nil, privilege.SELECT, "")
	}

	var mb mutationBuilder
	mb.init(b, opt.DeleteOp, tab, tabXQ, *alias)

	// Build the input expression that selects the rows that will be deleted:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the delete table will be projected.
	mb.buildInputForDelete(inScope, del.Where, del.Limit, del.OrderBy)

	// Build the final delete statement, including any returned expressions.
	if resultsNeeded(del.Returning) {
		mb.buildDelete(*del.Returning.(*tree.ReturningExprs))
	} else {
		mb.buildDelete(nil /* returning */)
	}

	mb.outScope.expr = b.wrapWithCTEs(mb.outScope.expr, ctes)

	return mb.outScope
}

// buildDelete constructs a Delete operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildDelete(returning tree.ReturningExprs) {
	private := mb.makeMutationPrivate(returning != nil)
	mb.outScope.expr = mb.b.factory.ConstructDelete(mb.outScope.expr, private)

	mb.buildReturning(returning)
}
