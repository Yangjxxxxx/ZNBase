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
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// buildUpdate builds a memo group for an UpdateOp expression. First, an input
// expression is constructed that outputs the existing values for all rows from
// the target table that match the WHERE clause. Additional column(s) that
// provide updated values are projected for each of the SET expressions, as well
// as for any computed columns. For example:
//
//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//   UPDATE abc SET b=1 WHERE a=2
//
// This would create an input expression similar to this SQL:
//
//   SELECT a AS oa, b AS ob, c AS oc, 1 AS nb FROM abc WHERE a=2
//
// The execution engine evaluates this relational expression and uses the
// resulting values to form the KV keys and values.
//
// Tuple SET expressions are decomposed into individual columns:
//
//   UPDATE abc SET (b, c)=(1, 2) WHERE a=3
//   =>
//   SELECT a AS oa, b AS ob, c AS oc, 1 AS nb, 2 AS nc FROM abc WHERE a=3
//
// Subqueries become correlated left outer joins:
//
//   UPDATE abc SET b=(SELECT y FROM xyz WHERE x=a)
//   =>
//   SELECT a AS oa, b AS ob, c AS oc, y AS nb
//   FROM abc
//   LEFT JOIN LATERAL (SELECT y FROM xyz WHERE x=a)
//   ON True
//
// Computed columns result in an additional wrapper projection that can depend
// on input columns.
//
// Note that the ORDER BY clause can only be used if the LIMIT clause is also
// present. In that case, the ordering determines which rows are included by the
// limit. The ORDER BY makes no additional guarantees about the order in which
// mutations are applied, or the order of any returned rows (i.e. it won't
// become a physical property required of the Update operator).
type colType int64

// add column type in updatable view to support privilege check
const (
	_          colType = 1 << iota //1<<0 which is 01
	CheckOwner                     //1<<1 which is 10
)

//changedAST 返回where条件中的列名，是否是unresolvedname类型
func (b *Builder) changedAST(
	t tree.Expr,
	columnast map[string]tree.Expr,
	tblname []tree.Name,
	fromOriginTable bool,
	ds cat.DataSource,
) (string, bool, error) {
	switch p := t.(type) {
	case *tree.UnresolvedName:
		switch p.NumParts {
		case 1:
			if columnast[p.Parts[0]] != nil {
				b.checkPrivilegeForUser(ds.Name(), ds, tree.NameList{tree.Name(p.Parts[0])}, privilege.SELECT, "")
				return p.Parts[0], true, nil
			} else if fromOriginTable {
				// 判断是否是from源表
				return "", false, nil
			} else {
				// 判断是否引用了视图中不存在的列
				return p.Parts[0], false, fmt.Errorf("ERROR: column name %s is not recognized", p.Parts[0])
			}
		case 2:
			if columnast[p.Parts[0]] != nil && p.Parts[1] == string(tblname[0]) {
				b.checkPrivilegeForUser(ds.Name(), ds, tree.NameList{tree.Name(p.Parts[0])}, privilege.SELECT, "")
				return p.Parts[0], true, nil
			} else if p.Parts[1] == string(tblname[0]) {
				return p.Parts[0], false, fmt.Errorf("ERROR: column name %s is not recognized", p.Parts[0])
			} else if fromOriginTable == false {
				// 没有from源表不能使用源表的其他列,否则报错
				if p.Parts[1] == string(tblname[3]) {
					return p.Parts[0], false, fmt.Errorf("ERROR: missing FROM-clause entry for table %s", p.Parts[1])
				}
			} else {
				return "", false, nil
			}
		case 3:
			if columnast[p.Parts[0]] != nil && p.Parts[1] == string(tblname[0]) && p.Parts[2] == string(tblname[1]) {
				b.checkPrivilegeForUser(ds.Name(), ds, tree.NameList{tree.Name(p.Parts[0])}, privilege.SELECT, "")
				return p.Parts[0], true, nil
			} else if p.Parts[1] == string(tblname[0]) && p.Parts[2] == string(tblname[1]) {
				return p.Parts[0], false, fmt.Errorf("ERROR: column name %s is not recognized", p.Parts[0])
			} else if fromOriginTable == false {
				if p.Parts[1] == string(tblname[3]) {
					return p.Parts[0], false, fmt.Errorf("ERROR: missing FROM-clause entry for table %s", p.Parts[1])
				}
			} else {
				return "", false, nil
			}
		case 4:
			if columnast[p.Parts[0]] != nil && p.Parts[1] == string(tblname[0]) && p.Parts[2] == string(tblname[1]) && p.Parts[3] == string(tblname[2]) {
				b.checkPrivilegeForUser(ds.Name(), ds, tree.NameList{tree.Name(p.Parts[0])}, privilege.SELECT, "")
				return p.Parts[0], true, nil
			} else if p.Parts[1] == string(tblname[0]) && p.Parts[2] == string(tblname[1]) && p.Parts[3] == string(tblname[2]) {
				return p.Parts[0], false, fmt.Errorf("ERROR: column name %s is not recognized", p.Parts[0])
			} else if fromOriginTable == false {
				if p.Parts[1] == string(tblname[3]) {
					return p.Parts[0], false, fmt.Errorf("ERROR: missing FROM-clause entry for table %s", p.Parts[1])
				}
			} else {
				return "", false, nil
			}
		default:
		}
	default:
		return "", false, nil
	}
	return "", false, nil
}

// replaceAST 递归修改where条件中的语法树
func (b *Builder) replaceAST(
	t tree.Expr, inscope *scope, tblname []tree.Name, fromOriginTable bool, ds cat.DataSource,
) error {
	// TODO: 把视图中不存的列报错 changeAST
	var name, ok = "", false
	var err error
	switch p := t.(type) {
	case *tree.AndExpr:
		name, ok, err = b.changedAST(p.Left, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}
		if ok {
			addChangedCol(inscope, name, CheckOwner, true)
			p.Left = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			name, ok, err = b.changedAST(p.Right, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
			if err != nil {
				return err
			}
			if ok {
				addChangedCol(inscope, name, CheckOwner, true)
				p.Right = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			}
			err = b.replaceAST(p.Right, inscope, tblname, fromOriginTable, ds)
			if err != nil {
				return err
			}
			return nil
		}

		name, ok, err = b.changedAST(p.Right, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}
		if ok {
			addChangedCol(inscope, name, CheckOwner, true)
			p.Right = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			return nil
		}

		err = b.replaceAST(p.Left, inscope, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}
		err = b.replaceAST(p.Right, inscope, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}

	case *tree.ComparisonExpr:
		name, ok, err = b.changedAST(p.Left, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}
		if ok {
			addChangedCol(inscope, name, CheckOwner, true)
			p.Left = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			name, ok, err = b.changedAST(p.Right, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
			if err != nil {
				return err
			}
			if ok {
				addChangedCol(inscope, name, CheckOwner, true)
				p.Right = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			}
			err = b.replaceAST(p.Right, inscope, tblname, fromOriginTable, ds)
			if err != nil {
				return err
			}
			return nil
		}
		// 当ComparisonExpr两端都是unresolvedname时，要依次先判断左右子树的类型
		name, ok, err = b.changedAST(p.Right, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}
		if ok {
			addChangedCol(inscope, name, CheckOwner, true)
			p.Right = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			return nil
		}

		err = b.replaceAST(p.Left, inscope, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}

		err = b.replaceAST(p.Right, inscope, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}

	case *tree.BinaryExpr:
		name, ok, err = b.changedAST(p.Left, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}
		if ok {
			addChangedCol(inscope, name, CheckOwner, true)
			p.Left = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			name, ok, err = b.changedAST(p.Right, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
			if err != nil {
				return err
			}
			if ok {
				addChangedCol(inscope, name, CheckOwner, true)
				p.Right = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			} else {
				err = b.replaceAST(p.Right, inscope, tblname, fromOriginTable, ds)
				if err != nil {
					return err
				}
			}
			return nil
		}

		name, ok, err = b.changedAST(p.Right, inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}
		if ok {
			addChangedCol(inscope, name, CheckOwner, true)
			p.Right = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			return nil
		}

		err = b.replaceAST(p.Left, inscope, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}
		err = b.replaceAST(p.Right, inscope, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}

	case *tree.ParenExpr:

		err = b.replaceAST(p.Expr, inscope, tblname, fromOriginTable, ds)
		if err != nil {
			return err
		}

	case *tree.IndirectionExpr:
		if t, ok := p.Expr.(*tree.UnresolvedName); ok {
			p.Expr = inscope.UpdatableViewDepends.ViewDependsColExpr[t.Parts[0]]
			addChangedCol(inscope, t.Parts[0], CheckOwner, true)
		} else {
			err = b.replaceAST(p.Expr, inscope, tblname, fromOriginTable, ds)
			if err != nil {
				return err
			}
		}
		return nil

	case *tree.FuncExpr:
		for i := range p.Exprs {
			name, ok, err = b.changedAST(p.Exprs[i], inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
			if err != nil {
				return err
			}
			if ok {
				addChangedCol(inscope, name, CheckOwner, true)
				p.Exprs[i] = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			} else {
				err = b.replaceAST(p.Exprs[i], inscope, tblname, fromOriginTable, ds)
				if err != nil {
					return err
				}
			}
		}
	case *tree.Tuple:
		for i := range p.Exprs {
			name, ok, err = b.changedAST(p.Exprs[i], inscope.UpdatableViewDepends.ViewDependsColExpr, tblname, fromOriginTable, ds)
			if err != nil {
				return err
			}
			if ok {
				addChangedCol(inscope, name, CheckOwner, true)
				p.Exprs[i] = inscope.UpdatableViewDepends.ViewDependsColExpr[name]
			} else {
				err = b.replaceAST(p.Exprs[i], inscope, tblname, fromOriginTable, ds)
				if err != nil {
					return err
				}
			}
		}
	default:
	}
	return err
}

// nameAST and recuseName 递归去为源表的列名添加前缀
func (b *Builder) nameAST(t *tree.SelectClause, tblname []tree.Name) {
	for i := range t.Exprs {
		switch p := t.Exprs[i].Expr.(type) {
		case *tree.BinaryExpr:
			recuseName(p, tblname)
		case *tree.UnresolvedName:
			recuseName(p, tblname)
		default:
		}
	}
	//为where中的列名增加前缀
	if t.Where != nil {
		recuseName(t.Where.Expr, tblname)
	}

}

func recuseName(t tree.Expr, tblname []tree.Name) {
	switch p := t.(type) {
	case *tree.BinaryExpr:
		recuseName(p.Left, tblname)
		recuseName(p.Right, tblname)
	case *tree.UnresolvedName:
		for i := range tblname {
			p.Parts[i+1] = string(tblname[i])
		}
		p.NumParts = 4
	case *tree.ParenExpr:
		recuseName(p.Expr, tblname)
	case *tree.AndExpr:
		recuseName(p.Left, tblname)
		recuseName(p.Right, tblname)
	case *tree.ComparisonExpr:
		recuseName(p.Left, tblname)
		recuseName(p.Right, tblname)
	default:
	}
}

//重构update条件中的where结构，增加创建视图时的限定条件
func rebuildWhere(tableWhere tree.Expr, updateWhere tree.Expr) (finalWhere *tree.AndExpr) {
	var updateWhereBL tree.Expr
	switch p := updateWhere.(type) {
	case *tree.AndExpr:
		if _, ok := p.Left.(*tree.AndExpr); !ok {
			updateWhereBL = p.Left
			p.Left = tableWhere
			finalWhere := &tree.AndExpr{
				Left:  updateWhere,
				Right: updateWhereBL,
			}
			return finalWhere
		}
		finalWhere = rebuildWhere(tableWhere, p.Left)
		return finalWhere

	case *tree.ParenExpr:
		finalWhere = rebuildWhere(tableWhere, p.Expr)
		return finalWhere
	default:
		//update只有一个限定条件，创视图的where中有一个/多个限定条件
		//保持左树一直tree.AndExpr
		return &tree.AndExpr{
			Left:  tableWhere,
			Right: updateWhere,
		}
	}
}

func (b *Builder) buildUpdate(upd *tree.Update, inScope *scope) (outScope *scope) {
	if upd.OrderBy != nil && upd.Limit == nil {
		panic(pgerror.NewErrorf(pgcode.Syntax,
			"UPDATE statement requires LIMIT when ORDER BY is used"))
	}

	// UX friendliness safeguard.

	if upd.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(pgerror.NewDangerousStatementErrorf("UPDATE without WHERE clause"))
	}

	var ctes []cteSource
	if upd.With != nil {
		inScope, ctes = b.buildCTEs(upd.With, inScope, nil)
	}

	// UPDATE xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias := getAliasedTableName(upd.Table)

	var updcols tree.NameList
	for i := range upd.Exprs {
		updcols = append(updcols, upd.Exprs[i].Names...)
	}

	// Find which table we're working on, check the permissions.
	// todo(xz): privilege here not used yet
	tab, tabXQ, resName, viewFlag, ViewDependsColName, NotMutilView := b.resolveTable(tn, privilege.UPDATE)

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
		var fromOriginTable bool
		columnAST := make(map[string]tree.Expr)
		ds, _ := b.resolveDataSource(tn, privilege.UPDATE)
		b.checkPrivilegeForUser(tn, ds, updcols, privilege.UPDATE, "")
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
		// 在tnName[0]中存放视图的名字，在tnName[3]中存放源表的名字
		tnName := make([]tree.Name, 4)
		copy(tnName, inScope.UpdatableViewDepends.TableName)
		if hasAlias {
			tnName[0] = alias.TableName
		} else {
			tnName[0] = tn.TableName
		}
		tnName[3] = inScope.UpdatableViewDepends.TableName[0]
		for i := range upd.From {
			if fromExpr, ok := upd.From[i].(*tree.AliasedTableExpr); ok {
				if fromName, ok := fromExpr.Expr.(*tree.TableName); ok {
					//as中的别名为原表名 报错
					if fromExpr.As.Alias == inScope.UpdatableViewDepends.TableName[0] {
						panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: cannot use origin table name %s as "+
							"alias of view %s", string(inScope.UpdatableViewDepends.TableName[0]), tn.TableName.String()))
					}
					//判断是否from原表
					if fromName.TableName == inScope.UpdatableViewDepends.TableName[0] {
						fromOriginTable = true
					}
				}
			}
		}
		for i := range upd.Exprs {
			// change colName of set vl1 -> set l1
			for j := range upd.Exprs[i].Names {
				if inScope.UpdatableViewDepends.ViewDependsColExpr[string(upd.Exprs[i].Names[j])] != nil {
					if ViewDependsColName[string(upd.Exprs[i].Names[j])] != "" {
						upd.Exprs[i].Names[j] = tree.Name(ViewDependsColName[string(upd.Exprs[i].Names[j])])
					} else {
						panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: cannot update into column %s of view %s "+
							"\nDETAIL:  View columns that are not columns of their base relation are not updatable", string(upd.Exprs[i].Names[j]), tn.TableName.String()))
					}
				} else {
					panic(pgerror.NewErrorf(pgcode.Syntax, "column %s does not exist", upd.Exprs[i].Names[j]))
				}
			}
			// change colName like set vl1 = array_append(vl1, 41) to set vl1 = array_append(l1, 41)
			err = b.replaceAST(upd.Exprs[i].Expr, inScope, tnName, fromOriginTable, ds)
			if err != nil {
				panic(pgerror.NewError(pgcode.Syntax, err.Error()))
			}
		}

		if upd.Where != nil {
			err = b.replaceAST(upd.Where.Expr, inScope, tnName, fromOriginTable, view)
			if err != nil {
				panic(pgerror.NewError(pgcode.Syntax, err.Error()))
			}
		}

		// check update privilege for view owner
		var tabUpdcols tree.NameList
		for i := range upd.Exprs {
			tabUpdcols = append(tabUpdcols, upd.Exprs[i].Names...)
		}
		b.checkPrivilegeForUser(tab.Name(), tab, tabUpdcols, privilege.UPDATE, view.GetOwner())

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
				whereStr := stat.Where.Expr.String()
				if strings.Contains(whereStr, "row_num") {
					panic(pgerror.NewErrorf(pgcode.Syntax, "data manipulation operation not legal on this view"))
				}
				if upd.Where == nil {
					upd.Where = stat.Where
				} else {
					upd.Where.Expr = rebuildWhere(stat.Where.Expr, upd.Where.Expr)
				}
			}
		}

		if upd.Returning != nil {
			switch p := upd.Returning.(type) {
			case *tree.ReturningExprs:
				selectExprRet := tree.SelectExprs(*p)
				for i := range selectExprRet {
					switch returnType := selectExprRet[i].Expr.(type) {
					case *tree.UnresolvedName:
						name, ok, err := b.changedAST(returnType, columnAST, tnName, fromOriginTable, view)
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
						err = b.replaceAST(returnType, inScope, tnName, fromOriginTable, ds)
						if err != nil {
							panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
						}
					}
				}
			default:
			}
		}
		if upd.OrderBy != nil {
			for i := range upd.OrderBy {
				if orderByColumnName, ok := upd.OrderBy[i].Expr.(*tree.UnresolvedName); ok {
					name, ok, err := b.changedAST(orderByColumnName, columnAST, tnName, fromOriginTable, view)
					if err != nil {
						panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
					}
					if ok {
						addChangedCol(inScope, name, CheckOwner, true)
						upd.OrderBy[i].Expr = columnAST[name]
					}
				} else {
					err = b.replaceAST(upd.OrderBy[i].Expr, inScope, tnName, fromOriginTable, ds)
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
		b.checkPrivilegeForUser(tn, tab, updcols, privilege.UPDATE, "")
	}

	var mb mutationBuilder
	mb.init(b, opt.UpdateOp, tab, tabXQ, *alias)

	// Build the input expression that selects the rows that will be updated:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the update table will be projected and checked with privilege.
	mb.buildInputForUpdate(inScope, upd.From, upd.Where, upd.Limit, upd.OrderBy)

	// when table created with some column defined on update current_timestamp,
	// it should reconsturct updateExpr such as update ... set starttime = current_timestamp
	upd.Exprs, _ = mb.buildAdditionalUpdateExprs(upd.Exprs)

	// Derive the columns that will be updated from the SET expressions.
	mb.addTargetColsForUpdate(upd.Exprs)

	// Build each of the SET expressions.
	mb.addUpdateCols(upd.Exprs)

	// Add additional columns for computed expressions that may depend on the
	// updated columns.
	mb.addComputedColsForUpdate()

	// Build the final update statement, including any returned expressions.
	if resultsNeeded(upd.Returning) {
		mb.buildUpdate(*upd.Returning.(*tree.ReturningExprs))
	} else {
		mb.buildUpdate(nil /* returning */)
	}

	mb.outScope.expr = b.wrapWithCTEs(mb.outScope.expr, ctes)
	return mb.outScope
}

// addTargetColsForUpdate compiles the given SET expressions and adds the user-
// specified column names to the list of table columns that will be updated by
// the Update operation. Verify that the RHS of the SET expression provides
// exactly as many columns as are expected by the named SET columns.
func (mb *mutationBuilder) addTargetColsForUpdate(exprs tree.UpdateExprs) {
	if len(mb.targetColList) != 0 {
		panic(pgerror.NewAssertionErrorf("addTargetColsForUpdate cannot be called more than once"))
	}

	for _, expr := range exprs {
		mb.addTargetColsByName(expr.Names)

		if expr.Tuple {
			n := -1
			switch t := expr.Expr.(type) {
			case *tree.Subquery:
				// Build the subquery in order to determine how many columns it
				// projects, and store it for later use in the addUpdateCols method.
				// Use the data types of the target columns to resolve expressions
				// with ambiguous types (e.g. should 1 be interpreted as an INT or
				// as a FLOAT).
				desiredTypes := make([]types.T, len(expr.Names))
				targetIdx := len(mb.targetColList) - len(expr.Names)
				for i := range desiredTypes {
					desiredTypes[i] = mb.md.ColumnMeta(mb.targetColList[targetIdx+i]).Type
				}
				outScope := mb.b.buildSelectStmt(t.Select, noRowLocking, desiredTypes, mb.outScope, mb)
				mb.subqueries = append(mb.subqueries, outScope)
				n = len(outScope.cols)

			case *tree.Tuple:
				n = len(t.Exprs)
			}
			if n < 0 {
				panic(unimplementedWithIssueDetailf(35713, fmt.Sprintf("%T", expr.Expr),
					"source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression; not supported: %T", expr.Expr))
			}
			if len(expr.Names) != n {
				panic(pgerror.NewErrorf(pgcode.Syntax,
					"number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n))
			}
		}
	}
}

// addUpdateCols builds nested Project and LeftOuterJoin expressions that
// correspond to the given SET expressions:
//
//   SET a=1 (single-column SET)
//     Add as synthesized Project column:
//       SELECT <fetch-cols>, 1 FROM <input>
//
//   SET (a, b)=(1, 2) (tuple SET)
//     Add as multiple Project columns:
//       SELECT <fetch-cols>, 1, 2 FROM <input>
//
//   SET (a, b)=(SELECT 1, 2) (subquery)
//     Wrap input in Max1Row + LeftJoinApply expressions:
//       SELECT * FROM <fetch-cols> LEFT JOIN LATERAL (SELECT 1, 2) ON True
//
// Multiple subqueries result in multiple left joins successively wrapping the
// input. A final Project operator is built if any single-column or tuple SET
// expressions are present.
func (mb *mutationBuilder) addUpdateCols(exprs tree.UpdateExprs) {
	// SET expressions should reject aggregates, generators, etc.
	scalarProps := &mb.b.semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	mb.b.semaCtx.Properties.Require("UPDATE SET", tree.RejectSpecial)

	// UPDATE input columns are accessible to SET expressions.
	inScope := mb.outScope

	// Project additional column(s) for each update expression (can be multiple
	// columns in case of tuple assignment).
	projectionsScope := mb.outScope.replace()
	if mb.outScope.UpdatableViewDepends.ChangedCol != nil {
		projectionsScope.UpdatableViewDepends.ChangedCol = mb.outScope.UpdatableViewDepends.ChangedCol
	}
	projectionsScope.appendColumnsFromScope(mb.outScope)

	checkCol := func(sourceCol *scopeColumn, scopeOrd scopeOrdinal, targetColID opt.ColumnID) {
		// Type check the input expression against the corresponding table column.
		ord := mb.tabID.ColumnOrdinal(targetColID)
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), sourceCol.typ)

		// Add ordinal of new scope column to the list of columns to update.
		mb.updateOrds[ord] = scopeOrd

		// Rename the column to match the target column being updated.
		sourceCol.name = mb.tab.Column(ord).ColName()
	}

	addCol := func(expr tree.Expr, targetColID opt.ColumnID) {
		// Allow right side of SET to be DEFAULT.
		if _, ok := expr.(tree.DefaultVal); ok {
			expr = mb.parseDefaultOrComputedExpr(targetColID)
		}

		if inScope.builder.semaCtx.IVarContainer == nil {
			inScope.builder.semaCtx.IVarContainer = &tree.ForInsertHelper{}
		}
		inScope.builder.semaCtx.IVarContainer.SetForInsertOrUpdate(true)

		// Add new column to the projections scope.
		desiredType := mb.md.ColumnMeta(targetColID).Type
		texpr := inScope.resolveType(expr, desiredType)

		typ := texpr.ResolvedType()
		if desiredType != types.Any && typ != desiredType && !tree.IsNullExpr(texpr) {
			if ok, c := tree.IsCastDeepValid(typ, desiredType); ok {
				telemetry.Inc(c)
				var tableDesc *sqlbase.TableDescriptor
				if mb.b.catalog != nil && mb != nil && mb.tab != nil {
					tb, err := mb.b.catalog.ResolveTableDesc(mb.tab.Name())
					if err != nil {
						panic(pgerror.Wrap(err, "", ""))
					}
					tbDesc, ok := tb.(*sqlbase.TableDescriptor)
					if !ok {
						panic(pgerror.NewAssertionErrorf("table descriptor does not exist"))
					}
					tableDesc = tbDesc
				}
				width := 0
				width = tableDesc.Columns[targetColID-1].ColTypeWidth()
				CastExpr := tree.ChangeToCastExpr(texpr, desiredType, width)
				texpr = &CastExpr
				typ = desiredType
			}
		}

		scopeCol := mb.b.addColumn(projectionsScope, "" /* alias */, texpr)
		scopeColOrd := scopeOrdinal(len(projectionsScope.cols) - 1)
		mb.b.buildScalar(texpr, inScope, projectionsScope, scopeCol, nil)

		checkCol(scopeCol, scopeColOrd, targetColID)
	}

	n := 0
	subQuery := 0
	for _, set := range exprs {
		if set.Tuple {
			switch t := set.Expr.(type) {
			case *tree.Subquery:
				// Get the subquery scope that was built by addTargetColsForUpdate.
				subQueryScope := mb.subqueries[subQuery]
				subQuery++

				// Type check and rename columns.
				for i := range subQueryScope.cols {
					scopeColOrd := scopeOrdinal(len(projectionsScope.cols) + i)
					checkCol(&subQueryScope.cols[i], scopeColOrd, mb.targetColList[n])
					n++
				}

				// Lazily create new scope to hold results of join.
				if mb.outScope == inScope {
					mb.outScope = inScope.replace()
					mb.outScope.appendColumnsFromScope(inScope)
					mb.outScope.expr = inScope.expr
				}

				// Wrap input with Max1Row + LOJ.
				mb.outScope.appendColumnsFromScope(subQueryScope)
				mb.outScope.expr = mb.b.factory.ConstructLeftJoinApply(
					mb.outScope.expr,
					mb.b.factory.ConstructMax1Row(subQueryScope.expr),
					memo.TrueFilter,
					memo.EmptyJoinPrivate,
				)

				// Project all subquery output columns.
				projectionsScope.appendColumnsFromScope(subQueryScope)

			case *tree.Tuple:
				for _, expr := range t.Exprs {
					addCol(expr, mb.targetColList[n])
					n++
				}
			}
		} else {
			addCol(set.Expr, mb.targetColList[n])
			n++
		}
	}

	mb.b.constructProjectForScope(mb.outScope, projectionsScope)
	mb.outScope = projectionsScope

	// Possibly round DECIMAL-related columns that were updated. Do this
	// before evaluating computed expressions, since those may depend on the
	// inserted columns.
	mb.roundDecimalValues(mb.updateOrds, false /* roundComputedCols */)

	// Add additional columns for computed expressions that may depend on any
	// updated columns.
	mb.addComputedColsForUpdate()
}

// addComputedColsForUpdate wraps an Update input expression with a Project
// operator containing any computed columns that need to be updated. This
// includes write-only mutation columns that are computed.
func (mb *mutationBuilder) addComputedColsForUpdate() {
	// Allow mutation columns to be referenced by other computed mutation
	// columns (otherwise the scope will raise an error if a mutation column
	// is referenced). These do not need to be set back to true again because
	// mutation columns are not projected by the Update operator.
	for i := range mb.outScope.cols {
		mb.outScope.cols[i].mutation = false
	}

	// Disambiguate names so that references in the computed expression refer to
	// the correct columns.
	mb.disambiguateColumns()

	mb.addSynthesizedCols(
		mb.updateOrds,
		func(tabCol cat.Column) bool { return tabCol.IsComputed() },
	)

	// Possibly round DECIMAL-related computed columns.
	mb.roundDecimalValues(mb.updateOrds, true /* roundComputedCols */)
}

// buildUpdate constructs an Update operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildUpdate(returning tree.ReturningExprs) {
	mb.addCheckConstraintCols()

	mb.projectPartialIndexPutCols(mb.outScope)

	private := mb.makeMutationPrivate(returning != nil)
	for _, col := range mb.extraAccessibleCols {
		if col.id != 0 {
			private.PassthroughCols = append(private.PassthroughCols, col.id)
		}
	}
	mb.outScope.expr = mb.b.factory.ConstructUpdate(mb.outScope.expr, private)
	mb.buildReturning(returning)
}

func (mb *mutationBuilder) addTargetColsForMergeUpdate(
	ins *tree.Insert, targetColListForNotMatch *opt.ColList,
) {
	if len(mb.targetColList) != 0 {
		panic(pgerror.NewAssertionErrorf("addTargetColsForMergeUpdate cannot be called more than once"))
	}

	// deal with match
	unionCols, errMatch := ins.OnConflict.GetUnionColListOfMatch()
	if errMatch != nil {
		panic(errMatch)
	}
	mb.addTargetColsByName(unionCols)

	// deal with no match
	colNames := make([]tree.Name, 0)
	for p := 0; p < mb.tab.ColumnCount(); p++ {
		colNames = append(colNames, mb.tab.Column(p).ColName())
	}
	// ensure that whether table has the unique column
	rowidName, hasPrimaryKey := mb.checkUniqueOrNot()

	unionNmCols, err := ins.OnConflict.GetUnionColsOfNotMatch(colNames, rowidName, hasPrimaryKey)
	if err != nil {
		panic(err)
	}
	// 根据 unionNmCols 中的colName 获得 id 并存入某结构
	for _, name := range unionNmCols {
		if ord := cat.FindTableColumnByName(mb.tab, name); ord != -1 {
			colID := mb.tabID.ColumnID(ord)
			*targetColListForNotMatch = append(*targetColListForNotMatch, colID)
		}
	}
}

func (mb *mutationBuilder) addMergeUpdateCols(
	ins *tree.Insert, targetColListForNotMatch opt.ColList,
) {
	// SET expressions should reject aggregates, generators, etc.
	scalarProps := &mb.b.semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	mb.b.semaCtx.Properties.Require("UPDATE SET", tree.RejectSpecial)

	// UPDATE input columns are accessible to SET expressions.
	inScope := mb.outScope

	// Project additional column(s) for each update expression (can be multiple
	// columns in case of tuple assignment).
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)

	checkCol := func(sourceCol *scopeColumn, scopeOrd scopeOrdinal, targetColID opt.ColumnID, isUpdate bool) {
		// Type check the input expression against the corresponding table column.
		ord := mb.tabID.ColumnOrdinal(targetColID)
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), sourceCol.typ)

		// Add ordinal of new scope column to the list of columns to update.
		if isUpdate {
			mb.updateOrds[ord] = scopeOrd
			// Rename the column to match the target column being updated.
			sourceCol.name = mb.tab.Column(ord).ColName()
		} else {
			mb.insertOrds[ord] = scopeOrd
		}
	}

	addCol := func(expr tree.Expr, targetColID opt.ColumnID, isUpdate bool, isNewColumn bool) {
		// Allow right side of SET to be DEFAULT.
		if _, ok := expr.(tree.DefaultVal); ok {
			expr = mb.parseDefaultOrComputedExpr(targetColID)
		}

		// Add new column to the projections scope.
		if isNewColumn {
			texpr := inScope.resolveType(expr, types.Int)
			scopeCol := mb.b.addColumn(projectionsScope, "" /* alias */, texpr)
			// TODO：可能存在问题， 需要测试
			mb.b.AllowUnsupportedExpr = true
			mb.b.buildScalar(texpr, inScope, projectionsScope, scopeCol, nil)

			if mb.additionalColID == 0 {
				mb.additionalColID = projectionsScope.cols[len(projectionsScope.cols)-1].id
			}
		} else {
			desiredType := mb.md.ColumnMeta(targetColID).Type
			texpr := inScope.resolveType(expr, desiredType)
			scopeCol := mb.b.addColumn(projectionsScope, "" /* alias */, texpr)
			scopeColOrd := scopeOrdinal(len(projectionsScope.cols) - 1)
			mb.b.buildScalar(texpr, inScope, projectionsScope, scopeCol, nil)
			checkCol(scopeCol, scopeColOrd, targetColID, isUpdate)
		}
	}

	var tblName string
	if tableName, ok := ins.Table.(*tree.TableName); ok {
		tblName = tableName.Table()
	} else if tableName, ok := ins.Table.(*tree.AliasedTableExpr); ok {
		tblName = tableName.As.Alias.String()
	}

	//为match语句构建caseExpr
	for i, col := range mb.targetColList {

		ord := mb.tabID.ColumnOrdinal(col)
		colName := inScope.cols[ord+mb.tab.ColumnCount()].name
		if expr, err := ins.OnConflict.ConstructCaseExprByColName(colName, ord, tblName, true, false, nil); err == nil {
			if caseExpr, ok := expr.(*tree.CaseExpr); ok {
				addCol(caseExpr, mb.targetColList[i], true, false)
			}
		}
	}

	for k := range mb.insertOrds {
		if inScope.cols[mb.insertOrds[k]].exprStr == "unique_rowid()" {
			continue
		}
		mb.insertOrds[k] = signMergeNum(mb.insertOrds[k])
	}

	//为notMatch构建caseExpr
	for i, col := range targetColListForNotMatch {

		ord := mb.tabID.ColumnOrdinal(col)
		colName := inScope.cols[ord+mb.tab.ColumnCount()].name
		if expr, err := ins.OnConflict.ConstructCaseExprByColName(colName, ord, tblName, false, false, nil); err == nil {
			if caseExpr, ok := expr.(*tree.CaseExpr); ok {
				caseExpr.Else = inScope.cols[ord].expr
				addCol(caseExpr, targetColListForNotMatch[i], false, false)
			}
		}

	}

	//to get table name and primary column for ConstructCaseExprOfAdditionalCol
	primaryKeyName := mb.tab.Index(cat.PrimaryIndex).Column(0).ColName()
	tableName := mb.alias.TableName.String()
	if tableName == "" {
		tableName = mb.tab.Name().TableName.String()
	}

	// construct caseExpr for extra column
	if expr, err := ins.OnConflict.ConstructCaseExprOfAdditionalCol(tableName, string(primaryKeyName)); err == nil {
		if caseExpr, ok := expr.(*tree.CaseExpr); ok {
			addCol(caseExpr, 0, false, true)
		}
	}

	// set null expr
	// has no done

	mb.b.constructProjectForScope(mb.outScope, projectionsScope)
	mb.outScope = projectionsScope

	// Possibly round DECIMAL-related columns that were updated. Do this
	// before evaluating computed expressions, since those may depend on the
	// inserted columns.
	mb.roundDecimalValues(mb.updateOrds, false /* roundComputedCols */)

	// Add additional columns for computed expressions that may depend on any
	// updated columns.
	mb.addComputedColsForUpdate()
}

func (mb *mutationBuilder) checkUniqueOrNot() (string, bool) {
	// ensure that if table has primary key
	tmpCol := mb.tab.Column(mb.tab.ColumnCount() - 1)
	if tmpCol.IsRowIDColumn() {
		return string(tmpCol.ColName()), false
	}
	return "", true
}

const secrecMergeNum scopeOrdinal = 3

// signMergeNum 将列序号转换为小于-2的值,用于标记该列在merge语法中需要插入空值
func signMergeNum(source scopeOrdinal) scopeOrdinal {
	source += secrecMergeNum
	source = 0 - source
	return source
}

// restoreMergeNum 还原被标记的列序号为原来的值
func restoreMergeNum(source scopeOrdinal) scopeOrdinal {
	source = 0 - source
	source -= secrecMergeNum
	return source
}

// buildAdditionalUpdateExprs build addtionnal updateExpr if this table have on update checkInvoker
func (mb *mutationBuilder) buildAdditionalUpdateExprs(
	updateExprs tree.UpdateExprs,
) (up tree.UpdateExprs, err error) {
	colLen := mb.tab.ColumnCount()
	for i := 0; i < colLen; i++ {
		col := mb.tab.Column(i)
		if col.IsOnUpdateCurrentTimeStamp() {
			// construct updateExprs
			addUpdExpr := &tree.UpdateExpr{
				Tuple: false,
				Names: []tree.Name{
					col.ColName(),
				},
				Expr: &tree.FuncExpr{
					Func: tree.WrapFunction("current_timestamp"),
				},
			}
			updateExprs = append(updateExprs, addUpdExpr)
		}
	}
	return updateExprs, nil
}

// addCheckType is used to handle the select clause of view definition.
func (b *Builder) addCheckType(t tree.Expr, inScope *scope) {
	switch p := t.(type) {
	case *tree.OrExpr:
		b.addCheckType(p.Left, inScope)
		b.addCheckType(p.Right, inScope)
	case *tree.AndExpr:
		b.addCheckType(p.Left, inScope)
		b.addCheckType(p.Right, inScope)
	case *tree.ComparisonExpr:
		b.addCheckType(p.Left, inScope)
		b.addCheckType(p.Right, inScope)
	case *tree.BinaryExpr:
		b.addCheckType(p.Left, inScope)
		b.addCheckType(p.Right, inScope)
	case *tree.IndirectionExpr:
		b.addCheckType(p.Expr, inScope)
	case *tree.ParenExpr:
		b.addCheckType(p.Expr, inScope)
	case *tree.FuncExpr:
		for i := range p.Exprs {
			b.addCheckType(p.Exprs[i], inScope)
		}
	case *tree.Tuple:
		for i := range p.Exprs {
			b.addCheckType(p.Exprs[i], inScope)
		}
	case *tree.UnresolvedName:
		// add column used in view create
		addChangedCol(inScope, p.Parts[0], CheckOwner, false)
	default:
	}
}

// addChangedCol is used to add view based cols to ChangedCol. support privilege check
func addChangedCol(inScope *scope, viewColName string, checkType colType, useMap bool) {
	if inScope.UpdatableViewDepends.ChangedCol == nil {
		inScope.UpdatableViewDepends.ChangedCol = make(map[string]colType)
	}
	convertedColName := make([]string, 0)
	if useMap {
		if inScope.UpdatableViewDepends.ViewDependsColExpr == nil {
			panic(pgerror.NewErrorf(pgcode.Syntax,
				"ERROR: can not get column reflect"))
		}
		colExpr := inScope.UpdatableViewDepends.ViewDependsColExpr[viewColName]
		getColName(colExpr, &convertedColName)
		for _, colName := range convertedColName {
			//增加修改列权限标记
			inScope.UpdatableViewDepends.ChangedCol[colName] |= checkType
		}
	} else {
		inScope.UpdatableViewDepends.ChangedCol[viewColName] |= checkType
	}
}

func getColName(expr tree.Expr, convertedColName *[]string) {
	switch t := expr.(type) {
	case *tree.OrExpr:
		getColName(t.Left, convertedColName)
		getColName(t.Right, convertedColName)
	case *tree.AndExpr:
		getColName(t.Left, convertedColName)
		getColName(t.Right, convertedColName)
	case *tree.ComparisonExpr:
		getColName(t.Left, convertedColName)
		getColName(t.Right, convertedColName)
	case *tree.ParenExpr:
		getColName(t.Expr, convertedColName)
	case *tree.UnresolvedName:
		*convertedColName = append(*convertedColName, t.Parts[0])
	default:

	}
}
