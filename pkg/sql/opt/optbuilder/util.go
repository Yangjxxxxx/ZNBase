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

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// CheckVirtualSchema get the func from package sql
var CheckVirtualSchema = func(sc string) bool { return false }

func checkFrom(expr tree.Expr, inScope *scope) {
	if len(inScope.cols) == 0 {
		panic(pgerror.NewErrorf(pgcode.InvalidName,
			"cannot use %q without a FROM clause", tree.ErrString(expr)))
	}
}

// expandStar expands expr into a list of columns if expr
// corresponds to a "*", "<table>.*" or "(Expr).*".
func (b *Builder) expandStar(
	expr tree.Expr, inScope *scope,
) (aliases []string, exprs []tree.TypedExpr) {
	if b.insideViewDef {
		b.viewHasStar = true
		//panic(unimplemented.NewWithIssue(10028, "views do not currently support * expressions"))
		//panic(builderError{fmt.Errorf("views do not currently support * expressions")})
	}
	switch t := expr.(type) {
	case *tree.TupleStar:
		texpr := inScope.resolveType(t.Expr, types.Any)
		typ := texpr.ResolvedType()
		tType, ok := typ.(types.TTuple)
		if !ok || tType.Labels == nil {
			panic(builderError{tree.NewTypeIsNotCompositeError(typ)})
		}

		// If the sub-expression is a tuple constructor, we'll de-tuplify below.
		// Otherwise we'll re-evaluate the expression multiple times.
		//
		// The following query generates a tuple constructor:
		//     SELECT (kv.*).* FROM kv
		//     -- the inner star expansion (scope.VisitPre) first expands to
		//     SELECT (((kv.k, kv.v) as k,v)).* FROM kv
		//     -- then the inner tuple constructor detuplifies here to:
		//     SELECT kv.k, kv.v FROM kv
		//
		// The following query generates a scalar var with tuple type that
		// is not a tuple constructor:
		//
		//     SELECT (SELECT pg_get_keywords() AS x LIMIT 1).*
		//     -- does not detuplify, one gets instead:
		//     SELECT (SELECT pg_get_keywords() AS x LIMIT 1).word,
		//            (SELECT pg_get_keywords() AS x LIMIT 1).catcode,
		//            (SELECT pg_get_keywords() AS x LIMIT 1).catdesc
		//     -- (and we hope a later opt will merge the subqueries)
		tTuple, isTuple := texpr.(*tree.Tuple)

		aliases = tType.Labels
		exprs = make([]tree.TypedExpr, len(tType.Types))
		for i := range tType.Types {
			if isTuple {
				// De-tuplify: ((a,b,c)).* -> a, b, c
				exprs[i] = tTuple.Exprs[i].(tree.TypedExpr)
			} else {
				// Can't de-tuplify: (Expr).* -> (Expr).a, (Expr).b, (Expr).c
				exprs[i] = tree.NewTypedColumnAccessExpr(texpr, tType.Labels[i], i)
			}
		}

	case *tree.AllColumnsSelector:
		checkFrom(expr, inScope)
		src, _, err := t.Resolve(b.ctx, inScope)
		if err != nil {
			panic(builderError{err})
		}
		exprs = make([]tree.TypedExpr, 0, len(inScope.cols))
		aliases = make([]string, 0, len(inScope.cols))
		for i := range inScope.cols {
			col := &inScope.cols[i]
			if col.table == *src && !col.hidden {
				exprs = append(exprs, col)
				aliases = append(aliases, string(col.name))
			}
		}

	case tree.UnqualifiedStar:
		checkFrom(expr, inScope)
		exprs = make([]tree.TypedExpr, 0, len(inScope.cols))
		aliases = make([]string, 0, len(inScope.cols))
		for i := range inScope.cols {
			col := &inScope.cols[i]
			if !col.hidden {
				exprs = append(exprs, col)
				aliases = append(aliases, string(col.name))
			}
		}

	default:
		panic(fmt.Sprintf("unhandled type: %T", expr))
	}

	return aliases, exprs
}

// expandStarAndResolveType expands expr into a list of columns if
// expr corresponds to a "*", "<table>.*" or "(Expr).*". Otherwise,
// expandStarAndResolveType resolves the type of expr and returns it
// as a []TypedExpr.
func (b *Builder) expandStarAndResolveType(
	expr tree.Expr, inScope *scope,
) (exprs []tree.TypedExpr) {
	switch t := expr.(type) {
	case *tree.AllColumnsSelector, tree.UnqualifiedStar, *tree.TupleStar:
		_, exprs = b.expandStar(expr, inScope)

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}
		return b.expandStarAndResolveType(vn, inScope)

	default:
		texpr := inScope.resolveType(t, types.Any)
		exprs = []tree.TypedExpr{texpr}
	}

	return exprs
}

// synthesizeColumn is used to synthesize new columns. This is needed for
// operations such as projection of scalar expressions and aggregations. For
// example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a projection with
// a synthesized column "x_incr".
//
// scope  The scope is passed in so it can can be updated with the newly bound
//        variable.
// alias  This is an optional alias for the new column (e.g., if specified with
//        the AS keyword).
// typ    The type of the column.
// expr   The expression this column refers to (if any).
// group  The memo group ID of this column/expression (if any). This parameter
//        is optional and can be set later in the returned scopeColumn.
//
// The new column is returned as a scopeColumn object.
func (b *Builder) synthesizeColumn(
	scope *scope, alias string, typ types.T, expr tree.TypedExpr, scalar opt.ScalarExpr,
) *scopeColumn {
	name := tree.Name(alias)
	colID := b.factory.Metadata().AddColumn(alias, typ, expr)
	if name == "row_num" && scope.cols != nil {
		scope.cols = append(scope.cols, scopeColumn{
			name:        name,
			table:       scope.cols[0].table,
			typ:         typ,
			id:          colID,
			expr:        expr,
			scalar:      scalar,
			visibleType: "row_num",
		})
	} else {
		scope.cols = append(scope.cols, scopeColumn{
			name:   name,
			typ:    typ,
			id:     colID,
			expr:   expr,
			scalar: scalar,
		})
	}

	return &scope.cols[len(scope.cols)-1]
}

// populateSynthesizedColumn is similar to synthesizeColumn, but it fills in
// the given existing column rather than allocating a new one.
func (b *Builder) populateSynthesizedColumn(col *scopeColumn, scalar opt.ScalarExpr) {
	colID := b.factory.Metadata().AddColumn(string(col.name), col.typ, col.expr)
	col.id = colID
	col.scalar = scalar
}

// projectColumn projects src by copying its column ID to dst. projectColumn
// also copies src.name to dst if an alias is not already set in dst. No other
// fields are copied, for the following reasons:
// - We don't copy group, as dst becomes a pass-through column in the new
//   scope. dst already has group=0, so keep it as-is.
// - We don't copy hidden, because projecting a column makes it visible.
//   dst already has hidden=false, so keep it as-is.
// - We don't copy table, since the table becomes anonymous in the new scope.
// - We don't copy descending, since we don't want to overwrite dst.descending
//   if dst is an ORDER BY column.
// - expr, exprStr and typ in dst already correspond to the expression and type
//   of the src column.
func (b *Builder) projectColumn(dst *scopeColumn, src *scopeColumn) {
	if dst.name == "" {
		dst.name = src.name
	}
	dst.id = src.id
}

// addColumn adds a column to scope with the given alias, type, and
// expression. It returns a pointer to the new column. The column ID and group
// are left empty so they can be filled in later.
func (b *Builder) addColumn(scope *scope, alias string, expr tree.TypedExpr) *scopeColumn {
	name := tree.Name(alias)
	var visbleType string
	if inscope, ok := expr.(*scopeColumn); ok {
		visbleType = inscope.visibleType
	}
	view := sqlbase.InvalidID
	if column, ok := expr.(*scopeColumn); ok {
		view = column.view
	}
	colTyp := expr.ResolvedType()
	if plvar, ok := expr.(*tree.PlpgsqlVar); ok {
		tempVar := b.semaCtx.UDRParms[plvar.Index]
		if tuple, ok := tempVar.VarDatum.(*tree.DTuple); ok {
			colTyp = tuple.ResolvedType()
		}
	}
	scope.cols = append(scope.cols, scopeColumn{
		name:        name,
		typ:         colTyp,
		expr:        expr,
		visibleType: visbleType,
		view:        view,
	})
	return &scope.cols[len(scope.cols)-1]
}

func (b *Builder) synthesizeResultColumns(scope *scope, cols sqlbase.ResultColumns) {
	for i := range cols {
		c := b.synthesizeColumn(scope, cols[i].Name, cols[i].Typ, nil /* expr */, nil /* scalar */)
		if cols[i].Hidden {
			c.hidden = true
		}
	}
}

// colIndex takes an expression that refers to a column using an integer,
// verifies it refers to a valid target in the SELECT list, and returns the
// corresponding column index. For example:
//    SELECT a from T ORDER by 1
// Here "1" refers to the first item in the SELECT list, "a". The returned
// index is 0.
func colIndex(numOriginalCols int, expr tree.Expr, context string) int {
	ord := int64(-1)
	switch i := expr.(type) {
	case *tree.NumVal:
		if i.ShouldBeInt64() {
			val, err := i.AsInt64()
			if err != nil {
				panic(builderError{err})
			}
			ord = val
		} else {
			panic(pgerror.NewErrorf(
				pgcode.Syntax,
				"non-integer constant in %s: %s", context, expr,
			))
		}
	case *tree.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *tree.StrVal:
		panic(pgerror.NewErrorf(
			pgcode.Syntax, "non-integer constant in %s: %s", context, expr,
		))
	case tree.Datum:
		panic(pgerror.NewErrorf(
			pgcode.Syntax, "non-integer constant in %s: %s", context, expr,
		))
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			panic(pgerror.NewErrorf(
				pgcode.InvalidColumnReference,
				"%s position %s is not in select list", context, expr,
			))
		}
		ord--
	}
	return int(ord)
}

// colIdxByProjectionAlias returns the corresponding index in columns of an expression
// that may refer to a column alias.
// If there are no aliases in columns that expr refers to, then -1 is returned.
// This method is pertinent to ORDER BY and DISTINCT ON clauses that may refer
// to a column alias.
func colIdxByProjectionAlias(expr tree.Expr, op string, scope *scope) int {
	index := -1

	if vBase, ok := expr.(tree.VarName); ok {
		v, err := vBase.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}

		if c, ok := v.(*tree.ColumnItem); ok && c.TableName == nil {
			// Look for an output column that matches the name. This
			// handles cases like:
			//
			//   SELECT a AS b FROM t ORDER BY b
			//   SELECT DISTINCT ON (b) a AS b FROM t
			target := c.ColumnName
			for j := range scope.cols {
				col := &scope.cols[j]
				if col.name != target {
					continue
				}

				if col.mutation {
					panic(builderError{makeBackfillError(col.name)})
				}

				if index != -1 {
					// There is more than one projection alias that matches the clause.
					// Here, SQL92 is specific as to what should be done: if the
					// underlying expression is known and it is equivalent, then just
					// accept that and ignore the ambiguity. This plays nice with
					// `SELECT b, * FROM t ORDER BY b`. Otherwise, reject with an
					// ambiguity error.
					if scope.cols[j].getExprStr() != scope.cols[index].getExprStr() {
						panic(pgerror.NewErrorf(pgcode.AmbiguousAlias,
							"%s \"%s\" is ambiguous", op, target))
					}
					// Use the index of the first matching column.
					continue
				}
				index = j
			}
		}
	}

	return index
}

// makeBackfillError returns an error indicating that the column of the given
// name is currently being backfilled and cannot be referenced.
func makeBackfillError(name tree.Name) error {
	return pgerror.NewErrorf(pgcode.InvalidColumnReference,
		"column %q is being backfilled", tree.ErrString(&name))
}

// flattenTuples extracts the members of tuples into a list of columns.
func flattenTuples(exprs []tree.TypedExpr) []tree.TypedExpr {
	// We want to avoid allocating new slices unless strictly necessary.
	var newExprs []tree.TypedExpr
	for i, e := range exprs {
		if t, ok := e.(*tree.Tuple); ok {
			if newExprs == nil {
				// All right, it was necessary to allocate the slices after all.
				newExprs = make([]tree.TypedExpr, i, len(exprs))
				copy(newExprs, exprs[:i])
			}

			newExprs = flattenTuple(t, newExprs)
		} else if newExprs != nil {
			newExprs = append(newExprs, e)
		}
	}
	if newExprs != nil {
		return newExprs
	}
	return exprs
}

// flattenTuple recursively extracts the members of a tuple into a list of
// expressions.
func flattenTuple(t *tree.Tuple, exprs []tree.TypedExpr) []tree.TypedExpr {
	for _, e := range t.Exprs {
		if eT, ok := e.(*tree.Tuple); ok {
			exprs = flattenTuple(eT, exprs)
		} else {
			expr := e.(tree.TypedExpr)
			exprs = append(exprs, expr)
		}
	}
	return exprs
}

// symbolicExprStr returns a string representation of the expression using
// symbolic notation. Because the symbolic notation disambiguates columns, this
// string can be used to determine if two expressions are equivalent.
func symbolicExprStr(expr tree.Expr) string {
	return tree.AsStringWithFlags(expr, tree.FmtCheckEquivalence)
}

func colsToColList(cols []scopeColumn) opt.ColList {
	colList := make(opt.ColList, len(cols))
	for i := range cols {
		colList[i] = cols[i].id
	}
	return colList
}

func (b *Builder) assertNoAggregationOrWindowing(expr tree.Expr, op string) {
	if b.exprTransformCtx.AggregateInExpr(expr, b.semaCtx.SearchPath) {
		panic(builderError{
			pgerror.NewErrorf(pgcode.Grouping, "aggregate functions are not allowed in %s", op),
		})
	}
	if b.exprTransformCtx.WindowFuncInExpr(expr) {
		panic(builderError{
			pgerror.NewErrorf(pgcode.Windowing, "window functions are not allowed in %s", op),
		})
	}
}

// In Postgres, qualifying an object name with pg_temp is equivalent to explicitly
// specifying TEMP/TEMPORARY in the CREATE syntax. resolveTemporaryStatus returns
// true if either(or both) of these conditions are true.
func resolveTemporaryStatus(name *tree.TableName, explicitTemp bool) bool {
	// An explicit schema can only be provided in the CREATE TEMP TABLE statement
	// if it is pg_temp.
	if explicitTemp && name.ExplicitSchema && name.SchemaName != sessiondata.PgTempSchemaName {
		panic(pgerror.NewErrorf(pgcode.InvalidTableDefinition, "cannot create temporary relation in non-temporary schema"))
	}
	return name.SchemaName == sessiondata.PgTempSchemaName || explicitTemp
}

// resolveSchemaForCreate returns the schema that will contain a newly created
// catalog object with the given name. If the current user does not have the
// CREATE privilege, then resolveSchemaForCreate raises an error.
func (b *Builder) resolveSchemaForCreate(name *tree.TableName) (cat.Schema, cat.SchemaName) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	sch, resName, err := b.catalog.ResolveSchema(b.ctx, flags, &name.TableNamePrefix)
	if err != nil {
		// Remap invalid schema name error text so that it references the catalog
		// object that could not be created.
		if pgerr, ok := err.(*pgerror.Error); ok && pgerr.Code == pgcode.InvalidSchemaName {
			panic(pgerror.NewErrorf(pgcode.InvalidSchemaName,
				"cannot create %q because the target database or schema does not exist",
				tree.ErrString(name)).
				SetHintf("verify that the current database and search_path are valid and/or the target database exists"))
		}
		panic(builderError{err})
	}
	sc := sch.ReplaceDesc(resName.SchemaName.String())
	if err := b.catalog.CheckPrivilege(b.ctx, sc, privilege.CREATE); err != nil {
		panic(builderError{err})
	}

	return sch, resName
}

// resolveTableOrderBy is to support orderby
func (b *Builder) resolveTableOrderBy(
	tn *tree.TableName, priv privilege.Kind,
) (cat.Table, cat.TableXQ, tree.TableName) {
	ds, resName := b.resolveDataSource(tn, priv)
	tab, ok := ds.(cat.Table)
	var tabXQ cat.TableXQ
	if !ok {
		// maybe tableXQ
		tabXQ, ok = ds.(cat.TableXQ)
		if !ok {
			panic(builderError{sqlbase.NewWrongObjectTypeError(tn, "table")})
		}

		tab = tabXQ.Table()
	}

	/*if !ok {
		panic(builderError{sqlbase.NewWrongObjectTypeError(tn, "table")})
	}*/
	return tab, tabXQ, resName
}

// resolveTable returns the data source in the catalog with the given name. If
// the name does not resolve to a table, or if the current user does not have
// the given privilege, then resolveTable raises an error.
func (b *Builder) resolveTable(
	tn *tree.TableName, priv privilege.Kind,
) (cat.Table, cat.TableXQ, tree.TableName, bool, map[string]string, bool) {
	ds, resName := b.resolveDataSource(tn, priv)
	tab, ok := ds.(cat.Table)
	var tabXQ cat.TableXQ
	var viewFlag bool
	var notMultiView = true
	ViewDependsColName := make(map[string]string)
	if !ok {
		// maybe tableXQ or updatableView
		switch t := ds.(type) {
		case cat.TableXQ:
			tabXQ = t
			tab = tabXQ.Table()
			ok = true
		case cat.View:
			// TODO(cms): 是否需要多层循环嵌套, 当一个视图引用于另一个视图, baseID标记源表还是视图
			var err error
			var baseID int
			ViewDependsColName, viewFlag, err = t.GetViewDependsColName()
			if err != nil {
				panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
			}
			baseID, err = t.GetBaseTableID()
			if err != nil {
				panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
			}
			for notView := false; notView == false; {
				parentDS, err := b.catalog.ResolveDataSourceByID(b.ctx, cat.Flags{}, cat.StableID(baseID))
				if parentDS == nil {
					panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: The dependent table of view %s cannot be resolved", tn.TableName.String()))
				}
				if err != nil {
					panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
				}
				switch p := parentDS.(type) {
				case cat.Table:
					notView = true
				case cat.View:
					baseID, err = p.GetBaseTableID()
					if err != nil {
						panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
					}
					notMultiView = false
				default:
					notView = true
				}
			}
			ds, err = b.catalog.ResolveDataSourceByID(b.ctx, cat.Flags{}, cat.StableID(baseID))
			if ds == nil {
				panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: The dependent table of view %s cannot be resolved", tn.TableName.String()))
			}
			if err != nil {
				panic(pgerror.NewErrorf(pgcode.Syntax, err.Error()))
			}

			tab, ok = ds.(cat.Table)
			if !ok {
				tabXQ, ok = ds.(cat.TableXQ)
				if !ok {
					panic(builderError{sqlbase.NewWrongObjectTypeError(tn, "table")})
				}
				tab = tabXQ.Table()
			}
		default:
		}
		if !ok {
			panic(builderError{sqlbase.NewWrongObjectTypeError(tn, "table")})

		}
	}

	// We can't mutate materialized views.
	if tab.IsMaterializedView() {
		panic(pgerror.NewErrorf(pgcode.WrongObjectType, "cannot mutate materialized view %q", tab.Name().TableName))
	}
	return tab, tabXQ, resName, viewFlag, ViewDependsColName, notMultiView
}

// resolveDataSource returns the data source in the catalog with the given name.
// If the name does not resolve to a table, or if the current user does not have
// the given privilege, then resolveDataSource raises an error.
func (b *Builder) resolveDataSource(
	tn *tree.TableName, priv privilege.Kind,
) (cat.DataSource, cat.DataSourceName) {
	var flags cat.Flags
	if b.insideViewDef {
		// Avoid taking table leases when we're creating a view.
		flags.AvoidDescriptorCaches = true
	}
	ds, resName, err := b.catalog.ResolveDataSource(b.ctx, flags, tn)
	if err != nil {
		panic(builderError{err})
	}

	// Check relation level privilege iff sequence.
	switch ds.(type) {
	case cat.Sequence:
		b.checkPrivilege(tn, ds, nil, priv, false)
	default:
		b.factory.Metadata().AddDataSourceDependency(tn, ds, nil, priv, "")
	}
	return ds, resName
}

// resolveDataSourceFromRef returns the data source in the catalog that matches
// the given TableRef spec. If no data source matches, or if the current user
// does not have the given privilege, then resolveDataSourceFromRef raises an
// error.
func (b *Builder) resolveDataSourceRef(ref *tree.TableRef, priv privilege.Kind) cat.DataSource {
	var flags cat.Flags
	if b.insideViewDef {
		// Avoid taking table leases when we're creating a view.
		flags.AvoidDescriptorCaches = true
	}
	ds, err := b.catalog.ResolveDataSourceByID(b.ctx, flags, cat.StableID(ref.TableID))
	if err != nil {
		panic(builderError{errors.Wrapf(err, "%s", tree.ErrString(ref))})
	}
	b.checkPrivilegeForUser(ds.Name(), ds, nil, priv, b.upperOwner)
	return ds
}

func (b *Builder) checkPrivilege(
	origName *cat.DataSourceName,
	ds cat.DataSource,
	cols tree.NameList,
	priv privilege.Kind,
	checkOwner bool,
) {
	if b.writeOnView && !checkOwner {
		b.checkPrivilegeForUser(origName, ds, cols, priv, "")
	} else {
		b.checkPrivilegeForUser(origName, ds, cols, priv, b.upperOwner)
	}
}

// checkPrivilegeForUser ensures that user has the privilege needed to
// access the given object in the catalog.
// If not, then checkPrivilegeForUser raises an error.
//
// It also adds the object and it's original unresolved name as a
// dependency to the metadata, so that the privileges can be re-checked on reuse
// of the memo.
//
// checkPrivilegeForUser handle cols as column privilege to check. If cols is nil,
// it will be treated as all the columns of object.
//
// If user is empty, check current user.
// If not, check specified user. Typically used on relations underlying with the upper view.
func (b *Builder) checkPrivilegeForUser(
	origName *cat.DataSourceName,
	ds cat.DataSource,
	cols tree.NameList,
	priv privilege.Kind,
	user string,
) {
	if err := b.catalog.CheckDMLPrivilege(b.ctx, ds, cols, priv, user); err != nil {
		panic(pgerror.NewError(pgcode.InsufficientPrivilege, err.Error()))
	}

	// Add dependency on this object to the metadata, so that the metadata can be
	// cached and later checked for freshness.
	b.factory.Metadata().AddDataSourceDependency(origName, ds, cols, priv, user)
}
