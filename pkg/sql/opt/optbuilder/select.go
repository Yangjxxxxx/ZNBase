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

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/keys"
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
	"github.com/znbasedb/znbase/pkg/sql/sqltelemetry"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util"
)

const (
	excludeMutations = false
	includeMutations = true
)

// buildDataSource builds a set of memo groups that represent the given table
// expression. For example, if the tree.TableExpr consists of a single table,
// the resulting set of memo groups will consist of a single group with a
// scanOp operator. Joins will result in the construction of several groups,
// including two for the left and right table scans, at least one for the join
// condition, and one for the join itself.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildDataSource(
	texpr tree.TableExpr, indexFlags *tree.IndexFlags, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	offset := b.Blockoffset
	defer func() {
		if outScope != nil {
			outScope.expr.SetBlockOffset(offset)
		}
	}()
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		if source.IndexFlags != nil {
			telemetry.Inc(sqltelemetry.IndexHintUseCounter)
			indexFlags = source.IndexFlags
		}

		if source.As.Alias != "" {
			locking = locking.filter(source.As.Alias)
		}

		outScope = b.buildDataSource(source.Expr, indexFlags, locking, inScope)

		if source.Ordinality {
			outScope = b.buildWithOrdinality("ordinality", outScope)
		}

		// Overwrite output properties with any alias information.
		b.renameSource(source.As, outScope)

		return outScope

	case *tree.JoinTableExpr:
		return b.buildJoin(source, locking, inScope)

	case *tree.TableName:
		tn := source

		// CTEs take precedence over other data sources.
		if cte := inScope.resolveCTE(tn); cte != nil {
			locking.ignoreLockingForCTE()
			outScope = inScope.push()

			inCols := make(opt.ColList, len(cte.cols))
			outCols := make(opt.ColList, len(cte.cols))
			outScope.cols = nil
			i := 0
			for _, col := range cte.cols {
				c := b.factory.Metadata().ColumnMeta(col.ID)
				newCol := b.synthesizeColumn(outScope, col.Alias, c.Type, nil, nil)
				newCol.table = *tn
				inCols[i] = col.ID
				outCols[i] = newCol.id
				i++
			}

			outScope.expr = b.factory.ConstructWithScan(&memo.WithScanPrivate{
				ID:           cte.id,
				Name:         string(cte.name.Alias),
				InCols:       inCols,
				OutCols:      outCols,
				BindingProps: cte.bindingProps,
			})
			return outScope
		}

		ds, resName := b.resolveDataSource(tn, privilege.SELECT)

		locking = locking.filter(tn.TableName)
		if locking.isSet() {
			// SELECT ... FOR [KEY] UPDATE/SHARE requires UPDATE privileges.
			b.checkPrivilegeForUser(tn, ds, nil, privilege.UPDATE, "")
		}

		if b.insideViewDef {
			// Overwrite the table name in the AST to the fully resolved version.
			// TODO(radu): modifying the AST in-place is hacky; we will need to switch
			// to using AST annotations.
			*tn = resName
			tn.ExplicitCatalog = true
			tn.ExplicitSchema = true
		}

		switch t := ds.(type) {
		case cat.Table:
			tabID := b.factory.Metadata().AddTableWithAlias(t, &resName)
			inheritsBys := make([]sqlbase.ID, 0)
			inheritsBy := b.factory.Metadata().Table(tabID).GetInhertisBy()
			if tn.IsOnly || inheritsBy == nil {
				return b.buildScan(tabID, nil /* ordinals */, indexFlags, locking, excludeMutations, inScope)
			}
			// convert []uint32 to []sqlbase.ID
			for _, u := range inheritsBy {
				inheritsBys = append(inheritsBys, sqlbase.ID(u))
			}
			return b.buildUnionOfInhertis(uint64(ds.ID()), inheritsBys, inScope, nil)
		case cat.TableXQ:
			tabID := b.factory.Metadata().AddTableWithAliasXQ(t, &resName)
			inheritsBys := make([]sqlbase.ID, 0)
			inheritsBy := b.factory.Metadata().Table(tabID).GetInhertisBy()
			if tn.IsOnly || inheritsBy == nil {
				return b.buildScan(tabID, nil /* ordinals */, indexFlags, locking, excludeMutations, inScope)
			}
			// convert []uint32 to []sqlbase.ID
			for _, u := range inheritsBy {
				inheritsBys = append(inheritsBys, sqlbase.ID(u))
			}
			return b.buildUnionOfInhertis(uint64(ds.ID()), inheritsBys, inScope, nil)
		case cat.View:
			return b.buildView(t, locking, inScope)
		case cat.Sequence:
			return b.buildSequenceSelect(t, inScope)
		default:
			panic(pgerror.NewAssertionErrorf("unknown DataSource type %T", ds))
		}

	case *tree.ParenTableExpr:
		return b.buildDataSource(source.Expr, indexFlags, locking, inScope)

	case *tree.RowsFromExpr:
		return b.buildZip(source.Items, inScope)

	case *tree.Subquery:
		// Remove any target relations from the current scope's locking spec, as
		// those only apply to relations in this statements. Interestingly, this
		// would not be necessary if we required all subqueries to have aliases
		// like Postgres does.
		// 从当前作用域的锁定规范中删除任何目标关系，因为这些关系只适用于此语句中的关系。
		// 有趣的是，如果我们像Postgres那样要求所有子查询都有别名，那么就没有必要这样做了。
		locking = locking.withoutTargets()
		b.Blockoffset++
		outScope = b.buildSelectStmt(source.Select, locking, nil, inScope, nil)

		// Treat the subquery result as an anonymous data source (i.e. column names
		// are not qualified). Remove hidden columns, as they are not accessible
		// outside the subquery.
		outScope.setTableAlias("")
		outScope.removeHiddenCols()

		return outScope

	case *tree.StatementSource:
		outScope = b.buildStmt(source.Statement, nil, inScope)
		if len(outScope.cols) == 0 {
			panic(pgerror.NewErrorf(pgcode.UndefinedColumn,
				"statement source \"%v\" does not return any columns", source.Statement))
		}
		locking.ignoreLockingForCTE()
		return outScope

	case *tree.TableRef:
		priv := privilege.SELECT
		locking = locking.filter(source.As.Alias)
		if locking.isSet() {
			// SELECT ... FOR [KEY] UPDATE/SHARE requires UPDATE privileges.
			// SELECT ... FOR [KEY]更新/共享需要UPDATE特权。
			priv = privilege.UPDATE
		}

		ds := b.resolveDataSourceRef(source, priv)
		switch t := ds.(type) {
		case cat.Table:
			outScope = b.buildScanFromTableRef(t, source, indexFlags, locking, inScope)
		case cat.TableXQ:
			outScope = b.buildScanFromTableXQRef(t, source, indexFlags, locking, inScope)
		default:
			panic(unimplementedWithIssueDetailf(35708, fmt.Sprintf("%T", t), "view and sequence numeric refs are not supported"))
		}
		b.renameSource(source.As, outScope)
		return outScope

	case *tree.PartitionExpr:
		tn := &source.TableName

		ds, resName := b.resolveDataSource(tn, privilege.SELECT)
		switch t := ds.(type) {
		case cat.Table:
			tabID := b.factory.Metadata().AddTableWithAlias(t, &resName)
			return b.buildScan(tabID, nil /* ordinals */, indexFlags, locking, excludeMutations, inScope)
		case cat.TableXQ:
			tabID := b.factory.Metadata().AddTableWithAliasXQ(t, &resName)
			return b.buildScan(tabID, nil /* ordinals */, indexFlags, locking, excludeMutations, inScope)
		case cat.View:
			return b.buildView(t, locking, inScope)
		case cat.Sequence:
			return b.buildSequenceSelect(t, inScope)
		default:
			panic(pgerror.NewAssertionErrorf("unknown DataSource type %T", ds))
		}

	default:
		panic(pgerror.NewAssertionErrorf("unknown table expr: %T", texpr))
	}
}

// buildView parses the view query text and builds it as a Select expression.
func (b *Builder) buildView(view cat.View, locking lockingSpec, inScope *scope) (outScope *scope) {
	// Cache the AST so that multiple references won't need to reparse.
	if b.views == nil {
		b.views = make(map[cat.View]*tree.Select)
	}

	// Check whether view has already been parsed, and if not, parse now.
	sel, ok := b.views[view]
	if !ok {
		var vstmt parser.Statement
		var verr error
		var query string

		if strings.Contains(view.Query(), "rownum") {
			query = sqlutil.SolveRownumInCreateView(view.Query())
			vstmt, verr = parser.ParseOne(query, b.CaseSensitive)
		} else {
			vstmt, verr = parser.ParseOne(view.Query(), b.CaseSensitive)
		}
		if strings.Contains(strings.ToUpper(view.Query()), "ORDER BY") {
			if ast, ok := vstmt.AST.(*tree.Select); ok {
				b.evalCtx.ViewOrderBy = ast.OrderBy
			}
		} else {
			b.evalCtx.ViewOrderBy = nil
		}
		if verr != nil {
			wrapped := errors.Wrapf(verr, "failed to parse underlying query from view %q", view.Name())
			panic(builderError{wrapped})
		}

		sel, ok = vstmt.AST.(*tree.Select)
		if !ok {
			panic(pgerror.NewAssertionErrorf("expected SELECT statement"))
		}

		b.views[view] = sel

		// Keep track of referenced views for EXPLAIN (opt, env).
		b.factory.Metadata().AddView(view)
	}

	// When building the view, we don't want to check for the SELECT privilege
	// on the underlying tables, just on the view itself. Checking on the
	// underlying tables as well would defeat the purpose of having separate
	// SELECT privileges on the view, which is intended to allow for exposing
	// some subset of a restricted table's data to less privileged users.
	//if !b.skipSelectPrivilegeChecks {
	//	b.skipSelectPrivilegeChecks = true
	//	defer func() { b.skipSelectPrivilegeChecks = false }()
	//}

	temp := b.upperOwner
	b.upperOwner = view.GetOwner()
	defer func() { b.upperOwner = temp }()

	if b.writeOnView {
		b.writeOnView = false
		defer func() { b.writeOnView = true }()
	}

	trackDeps := b.trackViewDeps
	if trackDeps {
		// We are only interested in the direct dependency on this view descriptor.
		// Any further dependency by the view's query should not be tracked.
		b.trackViewDeps = false
		defer func() { b.trackViewDeps = true }()
	}

	outScope = b.buildSelect(sel, locking, nil, &scope{builder: b}, nil)

	// Update data source name to be the name of the view. And if view columns
	// are specified, then update names of output columns.
	hasCols := view.ColumnNameCount() > 0
	for i := range outScope.cols {
		outScope.cols[i].table = *view.Name()
		outScope.cols[i].view = sqlbase.ID(view.ID())
		if hasCols {
			outScope.cols[i].name = view.ColumnName(i)
		}
	}

	if trackDeps && !view.IsSystemView() {
		dep := opt.ViewDep{DataSource: view}
		for i := range outScope.cols {
			dep.ColumnOrdinals.Add(i)
		}
		b.viewDeps = append(b.viewDeps, dep)
	}

	if m, ok := b.TableHint[tree.GenerateQBName(b.QueryType, 1)]; ok && b.TableHint != nil {
		if hints, ok := m[view.Name().Table()]; ok {
			switch typ := outScope.expr.(type) {
			case *memo.ScanExpr:
				// 支持普通视图(单表)指定join方式, 且当前view只有第一个join hint 生效.
			loopForScan:
				for _, hint := range hints {
					switch hint.HintName {
					case "merge_join":
						typ.Flags.ForceJoinType = keys.ForceMergeJoin
						break loopForScan
					case "hash_join":
						typ.Flags.ForceJoinType = keys.ForceHashJoin
						break loopForScan
					case "lookup_join":
						typ.Flags.ForceJoinType = keys.ForceLookUpJoin
						break loopForScan
					}
				}
			case *memo.InnerJoinExpr:
				// 支持多表视图指定join方式, 且当前view只有第一个join hint 生效.
			loopForInner:
				for _, hint := range hints {
					switch hint.HintName {
					case "merge_join":
						typ.Flags.JoinForView = keys.ForceMergeJoin
						break loopForInner
					case "hash_join":
						typ.Flags.JoinForView = keys.ForceHashJoin
						break loopForInner
					case "lookup_join":
						typ.Flags.JoinForView = keys.ForceLookUpJoin
						break loopForInner
					}
				}
			}
		}
	}
	return outScope
}

// renameSource applies an AS clause to the columns in scope.
func (b *Builder) renameSource(as tree.AliasClause, scope *scope) {
	if as.Alias != "" {
		colAlias := as.Cols

		// Special case for Postgres compatibility: if a data source does not
		// currently have a name, and it is a set-generating function or a scalar
		// function with just one column, and the AS clause doesn't specify column
		// names, then use the specified table name both as the column name and
		// table name.
		noColNameSpecified := len(colAlias) == 0
		if scope.isAnonymousTable() && noColNameSpecified && scope.singleSRFColumn {
			colAlias = tree.NameList{as.Alias}
		}

		// If an alias was specified, use that to qualify the column names.
		tableAlias := tree.MakeUnqualifiedTableName(as.Alias)
		scope.setTableAlias(as.Alias)

		// If input expression is a ScanExpr, then override metadata aliases for
		// pretty-printing.
		scan, isScan := scope.expr.(*memo.ScanExpr)
		if isScan {
			tabMeta := b.factory.Metadata().TableMeta(scan.ScanPrivate.Table)
			tabMeta.Alias = tree.MakeUnqualifiedTableName(as.Alias)
		}

		if len(colAlias) > 0 {
			// The column aliases can only refer to explicit columns.
			for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
				if colIdx >= len(scope.cols) {
					srcName := tree.ErrString(&tableAlias)
					panic(pgerror.NewErrorf(
						pgcode.InvalidColumnReference,
						"source %q has %d columns available but %d columns specified",
						srcName, aliasIdx, len(colAlias),
					))
				}
				col := &scope.cols[colIdx]
				if col.hidden {
					continue
				}
				col.name = colAlias[aliasIdx]
				if isScan {
					// Override column metadata alias.
					colMeta := b.factory.Metadata().ColumnMeta(col.id)
					colMeta.Alias = string(colAlias[aliasIdx])
				}
				aliasIdx++
			}
		}
	}
}

// buildScanFromTableRef adds support for numeric references in queries.
// For example:
// SELECT * FROM [53 as t]; (table reference)
// SELECT * FROM [53(1) as t]; (+columnar reference)
// SELECT * FROM [53(1) as t]@1; (+index reference)
// Note, the query SELECT * FROM [53() as t] is unsupported. Column lists must
// be non-empty
func (b *Builder) buildScanFromTableRef(
	tab cat.Table,
	ref *tree.TableRef,
	indexFlags *tree.IndexFlags,
	locking lockingSpec,
	inScope *scope,
) (outScope *scope) {
	if ref.Columns != nil && len(ref.Columns) == 0 {
		panic(pgerror.NewErrorf(pgcode.Syntax,
			"an explicit list of column IDs must include at least one column"))
	}

	// See tree.TableRef: "Note that a nil [Columns] array means 'unspecified'
	// (all columns). whereas an array of length 0 means 'zero columns'.
	// Lists of zero columns are not supported and will throw an error."
	// The error for lists of zero columns is thrown in the caller function
	// for buildScanFromTableRef.
	var ordinals []int
	if ref.Columns != nil {
		ordinals = make([]int, len(ref.Columns))
		for i, c := range ref.Columns {
			ord := 0
			cnt := tab.ColumnCount()
			for ord < cnt {
				if tab.Column(ord).ColID() == cat.StableID(c) {
					break
				}
				ord++
			}
			if ord >= cnt {
				panic(pgerror.NewErrorf(pgcode.UndefinedColumn,
					"column [%d] does not exist", c))
			}
			ordinals[i] = ord
		}
	}

	tabID := b.factory.Metadata().AddTable(tab)
	return b.buildScan(tabID, ordinals, indexFlags, locking, excludeMutations, inScope)
}

// buildScanFromTableRef adds support for numeric references in queries.
// For example:
// SELECT * FROM [53 as t]; (table reference)
// SELECT * FROM [53(1) as t]; (+columnar reference)
// SELECT * FROM [53(1) as t]@1; (+index reference)
// Note, the query SELECT * FROM [53() as t] is unsupported. Column lists must
// be non-empty
func (b *Builder) buildScanFromTableXQRef(
	tabXQ cat.TableXQ,
	ref *tree.TableRef,
	indexFlags *tree.IndexFlags,
	locking lockingSpec,
	inScope *scope,
) (outScope *scope) {
	tab := tabXQ.Table()
	if ref.Columns != nil && len(ref.Columns) == 0 {
		panic(pgerror.NewErrorf(pgcode.Syntax,
			"an explicit list of column IDs must include at least one column"))
	}

	// See tree.TableRef: "Note that a nil [Columns] array means 'unspecified'
	// (all columns). whereas an array of length 0 means 'zero columns'.
	// Lists of zero columns are not supported and will throw an error."
	// The error for lists of zero columns is thrown in the caller function
	// for buildScanFromTableRef.
	var ordinals []int
	if ref.Columns != nil {
		ordinals = make([]int, len(ref.Columns))
		for i, c := range ref.Columns {
			ord := 0
			cnt := tab.ColumnCount()
			for ord < cnt {
				if tab.Column(ord).ColID() == cat.StableID(c) {
					break
				}
				ord++
			}
			if ord >= cnt {
				panic(pgerror.NewErrorf(pgcode.UndefinedColumn,
					"column [%d] does not exist", c))
			}
			ordinals[i] = ord
		}
	}

	tabID := b.factory.Metadata().AddTableXQ(tabXQ)
	return b.buildScan(tabID, ordinals, indexFlags, locking, excludeMutations, inScope)
}

// buildScan builds a memo group for a ScanOp or VirtualScanOp expression on the
// given table with the given table name. Note that the table name is passed
// separately in order to preserve knowledge of whether the catalog and schema
// names were explicitly specified.
//
// If the ordinals slice is not nil, then only columns with ordinals in that
// list are projected by the scan. Otherwise, all columns from the table are
// projected.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildScan(
	tabID opt.TableID,
	ordinals []int,
	indexFlags *tree.IndexFlags,
	locking lockingSpec,
	scanMutationCols bool,
	inScope *scope,
) (outScope *scope) {
	offset := b.Blockoffset
	defer func() {
		if outScope.expr != nil {
			outScope.expr.SetBlockOffset(offset)
		}
	}()

	md := b.factory.Metadata()
	tabMeta := md.TableMeta(tabID)
	tab := tabMeta.Table

	colCount := len(ordinals)
	if colCount == 0 {
		// If scanning mutation columns, then include writable and deletable
		// columns in the output, in addition to public columns.
		if scanMutationCols {
			colCount = tab.DeletableColumnCount()
		} else {
			colCount = tab.ColumnCount()
		}
	}

	getOrdinal := func(i int) int {
		if ordinals == nil {
			return i
		}
		return ordinals[i]
	}

	var tabColIDs opt.ColSet
	outScope = inScope.push()
	outScope.cols = make([]scopeColumn, 0, colCount)
	for i := 0; i < colCount; i++ {
		ord := i
		if ordinals != nil {
			ord = ordinals[i]
		}

		col := tab.Column(ord)
		colID := tabID.ColumnID(ord)
		tabColIDs.Add(int(colID))
		name := col.ColName()
		isMutation := cat.IsMutationColumn(tab, ord)
		outScope.cols = append(outScope.cols, scopeColumn{
			id:          colID,
			name:        name,
			table:       tabMeta.Alias,
			typ:         col.DatumType(),
			hidden:      col.IsHidden() || isMutation,
			mutation:    isMutation,
			visibleType: col.VisibleType(),
		})
	}

	if tab.IsVirtualTable() {
		if indexFlags != nil {
			panic(pgerror.NewErrorf(pgcode.Syntax,
				"index flags not allowed with virtual tables"))
		}
		if locking.isSet() {
			panic(pgerror.NewErrorWithDepthf(1, pgcode.Syntax,
				"%s not allowed with virtual tables", locking.get().Strength))
		}
		private := memo.VirtualScanPrivate{Table: tabID, Cols: tabColIDs}
		outScope.expr = b.factory.ConstructVirtualScan(&private)
	} else {
		private := memo.ScanPrivate{Table: tabID, Cols: tabColIDs, EngineTypeSet: tab.DataStoreEngineInfo()}
		if indexFlags != nil {
			private.Flags.NoIndexJoin = indexFlags.NoIndexJoin
			// select * from a@index_name或者select * from a@第几个index. yacc解析成indexFlags.
			if indexFlags.Index != "" || indexFlags.IndexID != 0 {
				idx := -1
				var name tree.Name
				for i := 0; i < tab.IndexCount(); i++ {
					if tab.Index(i).Name() == tree.Name(indexFlags.Index) ||
						tab.Index(i).ID() == cat.StableID(indexFlags.IndexID) {
						idx = i
						name = tab.Index(i).Name()
						break
					}
				}
				if idx == -1 {
					var err error
					if indexFlags.Index != "" {
						err = errors.Errorf("index %q not found", tree.ErrString(&indexFlags.Index))
					} else {
						err = errors.Errorf("index [%d] not found", indexFlags.IndexID)
					}
					panic(builderError{err})
				}
				private.Flags.IndexHintType = keys.IndexHintForce
				private.Flags.HintIndexs = append(private.Flags.HintIndexs, idx)
				private.Flags.IndexName = append(private.Flags.IndexName, name)
				private.Flags.Direction = indexFlags.Direction
			}
		}
		if QueryBlockHint, ok := b.TableHint[tree.GenerateQBName(b.QueryType, offset)]; ok {
			if tableHint, ok := QueryBlockHint[tab.Name().TableName.String()]; ok {
				private.Flags.AcceptTableHints(tableHint)
			}
		}
		var indexHints []*tree.IndexHint
		if QueryBlockHint, ok := b.IndexHint["default"]; ok {
			if indexHint, ok := QueryBlockHint[tab.Name().TableName.String()]; ok {
				indexHints = append(indexHints, indexHint...)
			}
		}

		if QueryBlockHint, ok := b.IndexHint[tree.GenerateQBName(b.QueryType, offset)]; ok {
			if indexHint, ok := QueryBlockHint[tab.Name().TableName.String()]; ok {
				indexHints = append(indexHints, indexHint...)
			}
		}

		// 找当前table的index hint
		var hint *tree.IndexHint
		idx := []int{}
		idxName := []tree.Name{}
		for _, h := range indexHints {
			if h.TableName.String() != tab.Name().Table() {
				continue
			}
			hint = h
			break
		}

		// 将当前index hint加入ScanPrivate
		for i := 0; i < tab.IndexCount() && hint != nil; i++ {
			for _, indexName := range hint.IndexNames {
				if tab.Index(i).Name() == tree.Name(indexName.Index) {
					idx = append(idx, i)
					idxName = append(idxName, tree.Name(indexName.Index))
				}
			}
		}
		// 当我们得到第一个indexHint时, 我们直接忽略后面的index hint
		// USE表示可以使用的索引, 当有这个标志但是没有设置索引名的时候, 代表使用主键索引(全表扫描)
		if hint != nil {
			private.Flags.IndexHintType = hint.IndexHintType

			// 如果没找到索引那么不强制使用hint
			// TODO：增加warning报错
			if len(idx) == 0 {
				private.Flags.IndexHintType = keys.IndexNoHint
			}
			private.Flags.HintIndexs = idx
			private.Flags.IndexName = idxName
			private.Flags.TableName = *tab.Name()
		}
		b.addPartialIndexPredicatesForTable(tabMeta)
		if locking.isSet() {
			private.Locking = locking.get()
		}
		outScope.expr = b.factory.ConstructScan(&private)

		if b.trackViewDeps {
			dep := opt.ViewDep{DataSource: tab}
			for i := 0; i < colCount; i++ {
				dep.ColumnOrdinals.Add(getOrdinal(i))
			}
			if private.Flags.IndexHintType == keys.IndexHintForce {
				dep.SpecificIndex = true
				dep.Index = private.Flags.HintIndexs[0]
			}
			if private.Flags.IndexHintType == keys.IndexHintForce {
				dep.SpecificIndex = true
				dep.Index = private.Flags.HintIndexs[0]
			}
			b.viewDeps = append(b.viewDeps, dep)
		}
	}
	return outScope
}

func (b *Builder) buildSequenceSelect(seq cat.Sequence, inScope *scope) (outScope *scope) {
	tn := seq.SequenceName()
	md := b.factory.Metadata()
	outScope = inScope.push()

	cols := opt.ColList{
		md.AddColumn("last_value", types.Int, nil),
		md.AddColumn("log_cnt", types.Int, nil),
		md.AddColumn("is_called", types.Bool, nil),
	}

	outScope.cols = make([]scopeColumn, 3)
	for i, c := range cols {
		col := md.ColumnMeta(c)
		outScope.cols[i] = scopeColumn{
			id:    c,
			name:  tree.Name(col.Alias),
			table: *tn,
			typ:   col.Type,
		}
	}

	private := memo.SequenceSelectPrivate{
		Sequence: md.AddSequence(seq),
		Cols:     cols,
	}
	outScope.expr = b.factory.ConstructSequenceSelect(&private)

	if b.trackViewDeps {
		b.viewDeps = append(b.viewDeps, opt.ViewDep{DataSource: seq})
	}
	return outScope
}

// buildWithOrdinality builds a group which appends an increasing integer column to
// the output. colName optionally denotes the name this column is given, or can
// be blank for none.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildWithOrdinality(colName string, inScope *scope) (outScope *scope) {
	col := b.synthesizeColumn(inScope, colName, types.Int, nil, nil /* scalar */)

	// See https://www.znbaselabs.com/docs/stable/query-order.html#order-preservation
	// for the semantics around WITH ORDINALITY and ordering.

	input := inScope.expr.(memo.RelExpr)
	inScope.expr = b.factory.ConstructOrdinality(input, &memo.OrdinalityPrivate{
		Ordering: inScope.makeOrderingChoice(),
		ColID:    col.id,
	})

	return inScope
}

func (b *Builder) buildCTEs(
	with *tree.With, inScope *scope, stmt *tree.Select,
) (outScope *scope, addedCTEs []cteSource) {
	outScope = inScope.push()
	addedCTEs = make([]cteSource, len(with.CTEList))
	outScope.ctes = make(map[string]*cteSource)
	for i, cte := range with.CTEList {
		cteExpr, cteCols := b.buildCTE(cte, outScope, with.Recursive, stmt)

		// TODO(justin): lift this restriction when possible. WITH should be hoistable.
		if b.subquery != nil && !b.subquery.outerCols.Empty() {
			panic(pgerror.NewErrorf(pgcode.FeatureNotSupported, "CTEs may not be correlated"))
		}

		aliasStr := cte.Name.Alias.String()
		if _, ok := outScope.ctes[aliasStr]; ok {
			panic(pgerror.NewErrorf(pgcode.DuplicateAlias, "WITH query name %q specified more than once", aliasStr))
		}

		id := b.factory.Memo().NextWithID()

		addedCTEs[i] = cteSource{
			name:         cte.Name,
			cols:         cteCols,
			originalExpr: cte.Stmt,
			expr:         cteExpr,
			bindingProps: cteExpr.Relational(),
			id:           id,
		}
		outScope.ctes[aliasStr] = &addedCTEs[i]
	}

	telemetry.Inc(sqltelemetry.CteUseCounter)
	return outScope, addedCTEs
}

// buildSelectStmt builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectStmt(
	stmt tree.SelectStatement,
	locking lockingSpec,
	desiredTypes []types.T,
	inScope *scope,
	mb *mutationBuilder,
) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, locking, desiredTypes, inScope, mb)

	case *tree.SelectClause:
		return b.buildSelectClause(stmt, nil /* orderBy */, locking, desiredTypes, inScope, mb)

	case *tree.UnionClause:
		b.rejectIfLocking(locking, "UNION/INTERSECT/EXCEPT")
		return b.buildUnion(stmt, desiredTypes, inScope, mb)

	case *tree.ValuesClause:
		b.rejectIfLocking(locking, "VALUES")
		return b.buildValuesClause(stmt, desiredTypes, inScope, mb)

	default:
		panic(pgerror.NewAssertionErrorf("unknown select statement type: %T", stmt))
	}
}

// buildSelect builds a set of memo groups that represent the given select
// expression.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(
	stmt *tree.Select,
	locking lockingSpec,
	desiredTypes []types.T,
	inScope *scope,
	mb *mutationBuilder,
) (outScope *scope) {
	b.QueryType = "select"
	offset := b.Blockoffset
	defer func() {
		if outScope != nil {
			outScope.expr.SetBlockOffset(offset)
		}
	}()

	wrapped := stmt.Select
	orderBy := stmt.OrderBy
	limit := stmt.Limit
	with := stmt.With

	locking.apply(stmt.ForLocked)

	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		stmt = s.Select
		if stmt.With != nil {
			if with != nil {
				// (WITH ... (WITH ...))
				// Currently we are unable to nest the scopes inside ParenSelect so we
				// must refuse the syntax so that the query does not get invalid results.
				panic(pgerror.UnimplementedWithIssueError(24303, "multiple WITH clauses in parentheses"))
			}
			with = s.Select.With
		}
		wrapped = stmt.Select
		if stmt.OrderBy != nil {
			if orderBy != nil {
				panic(pgerror.NewErrorf(
					pgcode.Syntax, "multiple ORDER BY clauses not allowed",
				))
			}
			orderBy = stmt.OrderBy
		}
		if stmt.Limit != nil {
			if limit != nil {
				panic(pgerror.NewErrorf(
					pgcode.Syntax, "multiple LIMIT clauses not allowed",
				))
			}
			limit = stmt.Limit
		}
		if stmt.ForLocked != nil {
			locking.apply(stmt.ForLocked)
		}
	}

	var ctes []cteSource
	if with != nil {
		inScope, ctes = b.buildCTEs(with, inScope, stmt)
		orderBy = stmt.OrderBy

	}

	// NB: The case statements are sorted lexicographically.
	switch t := stmt.Select.(type) {
	case *tree.ParenSelect:
		panic(errors.AssertionFailedf(
			"%T in buildSelectStmtWithoutParens", wrapped))

	case *tree.SelectClause:
		outScope = b.buildSelectClause(t, orderBy, locking, desiredTypes, inScope, mb)

	case *tree.UnionClause:
		b.rejectIfLocking(locking, "UNION/INTERSECT/EXCEPT")
		outScope = b.buildUnion(t, desiredTypes, inScope, mb)

	case *tree.ValuesClause:
		b.rejectIfLocking(locking, "VALUES")
		outScope = b.buildValuesClause(t, desiredTypes, inScope, mb)

	default:
		panic(pgerror.NewErrorf(pgcode.FeatureNotSupported,
			"unknown select statement: %T", stmt.Select))
	}

	if outScope.ordering.Empty() && orderBy != nil {
		projectionsScope := outScope.replace()
		projectionsScope.cols = make([]scopeColumn, 0, len(outScope.cols))
		for i := range outScope.cols {
			expr := &outScope.cols[i]
			col := b.addColumn(projectionsScope, "" /* alias */, expr)
			b.buildScalar(expr, outScope, projectionsScope, col, nil)
		}
		orderByScope := b.analyzeOrderBy(orderBy, outScope, projectionsScope)
		b.buildOrderBy(outScope, projectionsScope, orderByScope)
		b.constructProjectForScope(outScope, projectionsScope)
		outScope = projectionsScope
	}

	if limit != nil {
		b.buildLimit(limit, inScope, outScope)
	}

	outScope.expr = b.wrapWithCTEs(outScope.expr, ctes)
	// TODO(rytaft): Support FILTER expression.
	return outScope
}

// wrapWithCTEs adds With expressions on top of an expression.
func (b *Builder) wrapWithCTEs(expr memo.RelExpr, ctes []cteSource) memo.RelExpr {
	// Since later CTEs can refer to earlier ones, we want to add these in
	// reverse order.
	for i := len(ctes) - 1; i >= 0; i-- {
		expr = b.factory.ConstructWith(
			ctes[i].expr,
			expr,
			&memo.WithPrivate{
				ID:           ctes[i].id,
				Name:         string(ctes[i].name.Alias),
				OriginalExpr: ctes[i].originalExpr,
			},
		)
	}
	return expr
}

//changeOrderByExprCase change the case of the name for consistency
func (b *Builder) changeOrderByExprCase(orderBy tree.OrderBy, sel *tree.SelectClause) {
	if orderBy != nil && !tree.CaseSensitiveG && !b.GetFactoty().SessionData.CaseSensitive {
		for _, value := range orderBy {
			switch v := value.Expr.(type) {
			case *tree.UnresolvedName:
				for key, val := range sel.Exprs {
					switch v.IsDoublequotes {
					case true:
						if !val.AsNameIsDoublequotes {
							switch v.Parts[0] == string(val.As) {
							case true:
								switch t := val.Expr.(type) {
								case *tree.UnresolvedName:
									if !t.IsDoublequotes || v.Parts[0] != t.Parts[0] {
										sel.Exprs[key].As = tree.UnrestrictedName(strings.ToLower(string(val.As)))
									}
								default:
									sel.Exprs[key].As = tree.UnrestrictedName(strings.ToLower(string(val.As)))
								}
							case false:
								switch t := val.Expr.(type) {
								case *tree.UnresolvedName:
									if !t.IsDoublequotes && v.Parts[0] == t.Parts[0] {
										v.Parts[0] = t.Parts[0]
									}
								}
							}
						}

					case false:
						if !val.AsNameIsDoublequotes {
							switch v.Parts[0] == strings.ToLower(string(val.As)) {
							case true:
								v.Parts[0] = string(val.As)
							case false:
								switch t := val.Expr.(type) {
								case *tree.UnresolvedName:
									if !t.IsDoublequotes && v.Parts[0] == strings.ToLower(t.Parts[0]) {
										v.Parts[0] = t.Parts[0]
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

// buildSelectClause builds a set of memo groups that represent the given
// select clause. We pass the entire select statement rather than just the
// select clause in order to handle ORDER BY scoping rules. ORDER BY can sort
// results using columns from the FROM/GROUP BY clause and/or from the
// projection list.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSelectClause(
	sel *tree.SelectClause,
	orderBy tree.OrderBy,
	locking lockingSpec,
	desiredTypes []types.T,
	inScope *scope,
	mb *mutationBuilder,
) (outScope *scope) {
	offset := b.Blockoffset

	// 每次buildSelectClause时构建hint, 将不是本block中的hint也保存到Builder的hint map中.
	b.TableHint, b.IndexHint = sel.HintSet.GetHintInfo(tree.GenerateQBName(b.QueryType, offset), b.TableHint, b.IndexHint, sel.From.Tables)

	defer func() {
		if outScope != nil {
			outScope.expr.SetBlockOffset(offset)
		}
	}()
	var isRownum, isStar bool
	var rownumCount, rownumIndex, tableRownumIndex int
	rownumIndex = -1
	tableRownumIndex = -1
	for i, expr := range sel.Exprs {
		if urName, ok := expr.Expr.(*tree.UnresolvedName); ok {
			if urName.Parts[0] == "rownum" {
				tableRownumIndex = i
				rownumCount++
			}
			if urName.Parts[0] == "row_num" {
				rownumIndex = i
				rownumCount++
			}
		}
	}

	if rownumCount >= 2 {
		if rownumIndex < tableRownumIndex {
			if sel.Exprs[tableRownumIndex].As == "" {
				sel.Exprs[tableRownumIndex].As = "rownum1"
			}
			if sel.Exprs[rownumIndex].As == "" || sel.Exprs[tableRownumIndex].As == "row_num" {
				sel.Exprs[rownumIndex].As = "ROWNUM"
			}
		} else {
			if sel.Exprs[rownumIndex].As == "" || sel.Exprs[tableRownumIndex].As == "row_num" {
				sel.Exprs[rownumIndex].As = "ROWNUM1"
			}
		}
	} else {
		if rownumIndex != -1 && tableRownumIndex == -1 {
			if sel.Exprs[rownumIndex].As == "" || sel.Exprs[rownumIndex].As == "row_num" {
				sel.Exprs[rownumIndex].As = "ROWNUM"
			}
		}
	}
	ShouldBeHidden := true
	// Determine whether the select statement contains rownum
	selectStr := sel.String()
	selectList := strings.Split(selectStr, "FROM")
	if len(selectList) > 2 {
		for i := 1; i < len(selectList)-1; i++ {
			if find := strings.Contains(selectList[i], "row_num"); find {
				ShouldBeHidden = false
			}
		}
	}
	//if len(selectList) == 1 && strings.Contains(strings.Split(selectStr, "WHERE")[0], "lnnvl") {
	//	panic(pgerror.NewErrorf(
	//		pgcode.Syntax, "Syntax error, it is not allowed to be used alone,only allowed in the where clause;",
	//	))
	//}

	if strings.Contains(selectList[0], "row_num") {
		for _, value := range sel.Exprs {
			switch value.Expr.(type) {
			case *tree.UnresolvedName:
				str := value.Expr.String()
				if find := strings.Contains(str, "row_num"); find {
					ShouldBeHidden = false
				} else if find := strings.Contains(str, "*"); find {
					isStar = true
				}
			case tree.UnqualifiedStar:
				isStar = true
			}
		}

	} else {
		b.evalCtx.SessionData.Rownum.InSelect = false
	}
	if strings.Contains(sel.String(), "row_num") {
		isRownum = true
	}
	if orderBy != nil {
		for _, orderBy := range orderBy {
			if orderBy.Expr != nil {
				if orderBy.Expr.String() == "row_num" {
					isRownum = true
				}
			}
		}
	}
	if !ShouldBeHidden && isStar {
		ShouldBeHidden = true
	}

	if isRownum {
		b.evalCtx.SessionData.Rownum.IsRownum = true
	} else {
		b.evalCtx.SessionData.Rownum.IsRownum = false
	}
	//var hasOrderby bool
	//if orderBy != nil {
	//	hasOrderby = true
	//}

	fromScope := b.buildFrom(sel.From, locking, inScope, isRownum)

	//if hasOrderby {
	//	b.evalCtx.ViewOrderBy = nil
	//}

	if orderBy == nil && b.evalCtx.ViewOrderBy != nil {
		if !strings.Contains(sel.String(), "create_statement") {
			if fromScope.cols != nil {
				for _, order := range b.evalCtx.ViewOrderBy {
					if expr, ok := order.Expr.(*tree.UnresolvedName); ok {
						if expr.NumParts > 1 {
							for idx := 1; idx < expr.NumParts; idx++ {
								expr.Parts[idx] = ""
							}
						}
						expr.NumParts = 1
					}
				}
			}
			orderBy = b.evalCtx.ViewOrderBy
			b.evalCtx.ViewOrderBy = nil
		} else if orderBy != nil {
			if expr, ok := orderBy[0].Expr.(*tree.UnresolvedName); ok {
				if expr.Parts[0] == "created" {
					orderBy = b.evalCtx.ViewOrderBy
					b.evalCtx.ViewOrderBy = nil
				}
			}
		}
	}

	b.processWindowDefs(sel, fromScope)
	if isRownum {
		for key, value := range fromScope.cols {
			if value.name == "row_num" {
				if ShouldBeHidden && key < len(fromScope.cols) {
					fromScope.cols[key].hidden = true
				}
				if value.name == "row_num" && value.expr == nil {
					fromScope.cols[key].hidden = true
				}
			}
		}
	}
	b.changeOrderByExprCase(orderBy, sel)

	getTableDesc := func(name tree.TableName) (*sqlbase.TableDescriptor, error) {
		tb, err := b.catalog.ResolveTableDesc(&name)
		if err != nil {
			return nil, err
		}
		desc, ok := tb.(*sqlbase.TableDescriptor)
		if !ok {
			return nil, errors.New("table descriptor does not exist")
		}
		return desc, nil
	}

	if sel.StartWithIsJoin {
		for key, value := range fromScope.cols {
			if value.table.TableName != "" {
				fromScope.cols[key].name = value.table.TableName + "." + value.name
			} else if value.expr != nil {
				switch t := value.expr.(type) {
				case *scopeColumn:
					fromScope.cols[key].name = t.table.TableName + "." + t.name
				}
			}
		}
	}
	andList, err := BuildPartitionExprs(sel.From.Tables, getTableDesc)
	if err != nil {
		panic(builderError{err})
	}
	partitionExprs := sqlbase.TwoExprs(andList, sqlbase.AndExpr)

	if b.insideViewDef {
		for _, table := range sel.From.Tables {
			if aliasedTable, ok := table.(*tree.AliasedTableExpr); ok {
				tn, ok := aliasedTable.Expr.(*tree.TableName)
				if ok {
					_, err = b.catalog.ResolveTableDesc(tn)
					if err != nil {
						continue
					}
				}
			}
		}
	}

	// We need to process the WHERE clause before initTargets below because
	// it must not see any column generated by SRFs.
	if sel.Where != nil {
		if partitionExprs != nil {
			sel.Where.Expr = sqlbase.AndExpr(partitionExprs, sel.Where.Expr)
		}
	} else if partitionExprs != nil {
		sel.Where = &tree.Where{
			Type: tree.AstWhere,
			Expr: partitionExprs,
		}
	}

	b.buildWhere(sel.Where, fromScope)

	projectionsScope := fromScope.replace()

	// This is where the magic happens. When this call reaches an aggregate
	// function that refers to variables in fromScope or an ancestor scope,
	// buildAggregateFunction is called which adds columns to the appropriate
	// aggInScope and aggOutScope.
	b.analyzeProjectionList(sel.Exprs, desiredTypes, fromScope, projectionsScope, mb)

	// Any aggregates in the HAVING, ORDER BY and DISTINCT ON clauses (if they
	// exist) will be added here.
	havingExpr := b.analyzeHaving(sel.Having, fromScope)
	orderByScope := b.analyzeOrderBy(orderBy, fromScope, projectionsScope)
	distinctOnScope := b.analyzeDistinctOnArgs(sel.DistinctOn, fromScope, projectionsScope)

	if b.needsAggregation(sel, fromScope) {
		outScope = b.buildAggregation(
			sel, havingExpr, fromScope, projectionsScope, orderByScope, distinctOnScope,
		)
	} else {
		b.buildProjectionList(fromScope, projectionsScope)
		b.buildOrderBy(fromScope, projectionsScope, orderByScope)
		b.buildDistinctOnArgs(fromScope, projectionsScope, distinctOnScope)
		if len(fromScope.srfs) > 0 {
			fromScope.expr = b.constructProjectSet(fromScope.expr, fromScope.srfs)
		}
		outScope = fromScope
	}

	b.validateLockingInFrom(sel, locking, fromScope)

	b.buildWindow(outScope, fromScope)

	// Construct the projection.
	b.constructProjectForScope(outScope, projectionsScope)
	outScope = projectionsScope

	if sel.Distinct {
		if projectionsScope.distinctOnCols.Empty() {
			outScope.expr = b.constructDistinct(outScope)
		} else {
			outScope = b.buildDistinctOn(projectionsScope.distinctOnCols, outScope)
		}
	}

	// lnnvl is only allowed to be used after "WHERE" in a select stmt
	if sel.Exprs != nil {
		for i := 0; i < len(sel.Exprs); i++ {
			if strings.Contains(sel.Exprs[i].Expr.String(), "lnnvl") {
				panic(pgerror.NewErrorf(
					pgcode.Syntax, "Syntax error, lnnvl() is not allowed to be used alone,only allowed in the where clause;",
				))
			}
		}
	}

	setHidenColumn(outScope)
	return outScope
}

// buildFrom builds a set of memo groups that represent the given FROM clause.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildFrom(
	from *tree.From, locking lockingSpec, inScope *scope, isRownum bool,
) (outScope *scope) {
	offset := b.Blockoffset
	defer func() {
		if outScope != nil {
			outScope.expr.SetBlockOffset(offset)
		}
	}()
	// The root AS OF clause is recognized and handled by the executor. The only
	// thing that must be done at this point is to ensure that if any timestamps
	// are specified, the root SELECT was an AS OF SYSTEM TIME and that the time
	// specified matches the one found at the root.
	if from.AsSnapshot.Snapshot != "" {
		b.validateAsSnapshot(from.AsSnapshot)
	}
	if from.AsOf.Expr != nil {
		b.validateAsOf(from.AsOf)
	}

	if len(from.Tables) > 0 {
		outScope = b.buildFromTables(from.Tables, locking, inScope)
	} else {
		outScope = inScope.push()
		outScope.expr = b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   b.factory.Metadata().NextValuesID(),
		})
	}
	if isRownum {
		outScope = b.buildWithOrdinality("row_num", outScope)
	}
	return outScope
}

// processWindowDefs validates that any window defs have unique names and adds
// them to the given scope.
func (b *Builder) processWindowDefs(sel *tree.SelectClause, fromScope *scope) {
	// Just do an O(n^2) loop since the number of window defs is likely small.
	for i := range sel.Window {
		for j := i + 1; j < len(sel.Window); j++ {
			if sel.Window[i].Name == sel.Window[j].Name {
				panic(builderError{
					pgerror.Newf(
						pgcode.Windowing,
						"window %q is already defined",
						sel.Window[i].Name,
					),
				})
			}
		}
	}

	// Pass down the set of window definitions so that they can be referenced
	// elsewhere in the SELECT.
	fromScope.windowDefs = sel.Window
}

// buildWhere builds a set of memo groups that represent the given WHERE clause.
// And check column privilege as well.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildWhere(where *tree.Where, inScope *scope) {
	if where == nil {
		return
	}
	b.semaCtx.IsWhere = true
	defer func() {
		b.semaCtx.IsWhere = false
	}()

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require("WHERE", tree.RejectSpecial|tree.RejectWindowApplications)
	inScope.context = "WHERE"

	// All "from" columns are visible to the filter expression.
	texpr := inScope.resolveAndRequireType(where.Expr, types.Bool)

	filter := b.buildScalar(texpr, inScope, nil, nil, nil)

	// Wrap the filter in a FiltersOp.
	inScope.expr = b.factory.ConstructSelect(
		inScope.expr.(memo.RelExpr),
		memo.FiltersExpr{{Condition: filter}},
	)
}

// buildFromTables recursively builds a series of InnerJoin expressions that
// join together the given FROM tables. The tables are joined in the reverse
// order that they appear in the list, with the innermost join involving the
// tables at the end of the list. For example:
//
//   SELECT * FROM a,b,c
//
// is joined like:
//
//   SELECT * FROM a JOIN (b JOIN c ON true) ON true
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFromTables(
	tables tree.TableExprs, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	offset := b.Blockoffset
	defer func() {
		if outScope != nil {
			outScope.expr.SetBlockOffset(offset)
		}
	}()
	outScope = b.buildDataSource(tables[0], nil /* indexFlags */, locking, inScope)

	// Recursively build table join.
	tables = tables[1:]
	if len(tables) == 0 {
		return outScope
	}
	tableScope := b.buildFromTables(tables, locking, inScope)

	// Check that the same table name is not used multiple times.
	b.validateJoinTableNames(outScope, tableScope)

	outScope.appendColumnsFromScope(tableScope)

	left := outScope.expr.(memo.RelExpr)
	right := tableScope.expr.(memo.RelExpr)
	joinPrivate := buildPrivateJoinForHint(left, right)
	outScope.expr = b.factory.ConstructInnerJoin(left, right, memo.TrueFilter, joinPrivate)
	return outScope
}

// validateAsOf ensures that any AS OF SYSTEM TIME timestamp is consistent with
// that of the root statement.
func (b *Builder) validateAsOf(asOf tree.AsOfClause) {
	ts, err := tree.EvalAsOfTimestamp(asOf, b.semaCtx, b.evalCtx)
	if err != nil {
		panic(builderError{err})
	}

	if b.semaCtx.AsOfTimestamp == nil {
		panic(pgerror.NewErrorf(pgcode.Syntax,
			"AS OF SYSTEM TIME must be provided on a top-level statement"))
	}

	if *b.semaCtx.AsOfTimestamp != ts {
		panic(unimplementedWithIssueDetailf(35712, "",
			"cannot specify AS OF SYSTEM TIME with different timestamps"))
	}
}

func (b *Builder) validateAsSnapshot(asSnapshot tree.AsSnapshotClause) {
	if b.semaCtx.AsOfTimestamp == nil {
		panic(pgerror.NewErrorf(pgcode.Syntax,
			"AS OF SNAPSHOT must be provided on a top-level statement"))
	}
}

// validateLockingInFrom checks for operations that are not supported with FOR
// [KEY] UPDATE/SHARE. If a locking clause was specified with the select and an
// incompatible operation is in use, a locking error is raised.
// validateLockingInFrom检查FOR [KEY]更新/共享不支持的操作。如果在select中指定了锁定子句，
// 并且使用了不兼容的操作，则会引发锁定错误。
func (b *Builder) validateLockingInFrom(
	sel *tree.SelectClause, locking lockingSpec, fromScope *scope,
) {
	if !locking.isSet() {
		// No FOR [KEY] UPDATE/SHARE locking modes in scope.
		// 范围内无FOR [KEY]更新/共享锁定模式。
		return
	}
	switch {
	case sel.Distinct:
		b.raiseLockingContextError(locking, "DISTINCT clause")

	case sel.GroupBy != nil:
		b.raiseLockingContextError(locking, "GROUP BY clause")

	case sel.Having != nil:
		b.raiseLockingContextError(locking, "HAVING clause")

	case fromScope.hasAggregates():
		b.raiseLockingContextError(locking, "aggregate functions")

	case len(fromScope.srfs) != 0:
		b.raiseLockingContextError(locking, "set-returning functions in the target list")
	}
	for _, li := range locking {
		// Validate locking strength.
		// 验证锁定强度。
		switch li.Strength {
		case tree.ForNone:
			// AST nodes should not be created with this locking strength.
			// AST节点不应以此锁定强度创建。
			panic(errors.AssertionFailedf("locking item without strength"))
		case tree.ForUpdate, tree.ForNoKeyUpdate, tree.ForShare, tree.ForKeyShare:
			// ZNBaseDB treats all of the FOR LOCKED modes as no-ops. Since all
			// transactions are serializable in CockroachDB, clients can't observe
			// whether or not FOR UPDATE (or any of the other weaker modes) actually
			// created a lock. This behavior may improve as the transaction model gains
			// more capabilities.
			// ZNBaseDB将所有“锁定”模式都视为无操作。 由于所有事务都可以在CockroachDB中进行
			// 序列化，因此客户端无法观察到FOR UPDATE（或其他任何较弱的模式）是否实际创建了锁。
			// 随着事务模型获得更多功能，此行为可能会改善。
		default:
			panic(errors.AssertionFailedf("unknown locking strength: %s", li.Strength))
		}

		// Validating locking wait policy.
		// 验证锁定等待策略。
		switch li.WaitPolicy.LockingType {
		case tree.LockWaitBlock:
			// Default. Block on conflicting locks.
			// 默认。 阻止冲突的锁。
		case tree.LockWaitSkip:
			panic(pgerror.NewError("40476",
				"unimplemented: SKIP LOCKED lock wait policy is not supported"))
		case tree.LockWaitError:
			// Raise an error on conflicting locks.
			// 在冲突的锁上引发错误。
		case tree.LockWait:
			// todo gzq: wait
			// 待办事项：等待
		default:
			panic(errors.AssertionFailedf("unknown locking wait policy: %s", li.WaitPolicy))
		}

		// Validate locking targets by checking that all targets are well-formed
		// and all point to real relations present in the FROM clause.
		// 通过检查所有目标的格式是否正确以及所有指向FROM子句中存在的实际关系，来验证锁定目标。
		for _, target := range li.Targets {
			// Insist on unqualified alias names here. We could probably do
			// something smarter, but it's better to just mirror Postgres
			// exactly. See transformLockingClause in Postgres' source.
			// 在这里坚持使用不合格的化名。我们也许可以做一些更聪明的事情，但最好还是照搬postgre。
			// 请参阅Postgres源代码中的transformLockingClause。
			if target.CatalogName != "" || target.SchemaName != "" {
				panic(pgerror.NewErrorWithDepthf(1, pgcode.Syntax,
					"%s must specify unqualified relation names", li.Strength))
			}
			// Search for the target in fromScope. If a target is missing from
			// the scope then raise an error. This will end up looping over all
			// columns in scope for each of the locking targets. We could use a
			// more efficient data structure (e.g. a hash map of relation names)
			// to improve the complexity here, but we don't expect the number of
			// columns to be small enough that doing so is likely not worth it.
			// 在fromScope中搜索目标。如果作用域中缺少目标，则引发错误。这将在每个锁定目标
			// 的作用域中的所有列上循环。我们可以使用更有效的数据结构（例如，关系名的哈希映射）
			// 来提高复杂性，但我们不希望列的数量足够小，这样做可能不值得。
			found := false
			for _, col := range fromScope.cols {
				if target.TableName == col.table.TableName {
					found = true
					break
				}
			}
			if !found {
				panic(pgerror.NewErrorf(
					pgcode.UndefinedTable,
					"relation %q in %s clause not found in FROM clause",
					target.TableName, li.Strength,
				))
			}
		}
	}
}

// rejectIfLocking raises a locking error if a locking clause was specified.
func (b *Builder) rejectIfLocking(locking lockingSpec, context string) {
	if !locking.isSet() {
		// No FOR [KEY] UPDATE/SHARE locking modes in scope.
		return
	}
	b.raiseLockingContextError(locking, context)
}

// raiseLockingContextError raises an error indicating that a row-level locking
// clause is not permitted in the specified context. locking.set must be true.
// raiseLockingContextError引发错误，指示在指定的上下文中不允许行级锁定子句。
// lock.set必须为true。
func (b *Builder) raiseLockingContextError(locking lockingSpec, context string) {
	panic(pgerror.NewErrorf(pgcode.LockNotAvailable,
		"%s is not allowed with %s", locking.get().Strength, context))
}

func (b *Builder) buildCreateView(cv *tree.CreateView, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	if isInternal := CheckVirtualSchema(cv.Name.Schema()); isInternal {
		panic(builderError{fmt.Errorf("cannot create view in virtual schema: %q", cv.Name.Schema())})
	}
	sch, resName := b.resolveSchemaForCreate(&cv.Name)
	cv.Name.TableNamePrefix = resName
	schID := b.factory.Metadata().AddSchema(sch)

	// We build the select statement to:
	//  - check the statement semantically,
	//  - get the fully resolved names into the AST, and
	//  - collect the view dependencies in b.viewDeps.
	// The result is not otherwise used.
	b.insideViewDef = true
	b.trackViewDeps = true
	defer func() {
		b.insideViewDef = false
		b.trackViewDeps = false
		b.viewDeps = nil
	}()

	defScope := b.buildStmt(cv.AsSource, nil, inScope)

	{
		f := tree.NewFmtCtx(tree.FmtParsable)
		f.SetReformatTableNames(
			func(_ *tree.FmtCtx, tn *tree.TableName) {
				// Persist the database prefix expansion.
				if tn.CatalogName != "" {
					tn.ExplicitCatalog = true
				}
				if tn.SchemaName != "" {
					// All CTE or table aliases have no schema
					// information. Those do not turn into explicit.
					tn.ExplicitSchema = true
				}
			},
		)
		f.FormatNode(cv.AsSource)
		f.Close() // We don't need the string.
	}

	p := defScope.makePhysicalProps().Presentation
	if len(cv.ColumnNames) != 0 {
		if len(p) != len(cv.ColumnNames) {
			panic(sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE VIEW specifies %d column name%s, but data source has %d column%s",
				len(cv.ColumnNames), util.Pluralize(int64(len(cv.ColumnNames))),
				len(p), util.Pluralize(int64(len(p)))),
			))
		}
		// Override the columns.
		for i := range p {
			p[i].Alias = string(cv.ColumnNames[i])
		}
	}

	expr := b.factory.ConstructCreateView(
		&memo.CreateViewPrivate{
			Schema:  schID,
			Syntax:  cv,
			Columns: p,
			Deps:    b.viewDeps,
			HasStar: b.viewHasStar,
		},
	)
	return &scope{builder: b, expr: expr}
}

// addPartialIndexPredicatesForTable finds all partial indexes in the table and
// adds their predicates to the table metadata (see
// TableMeta.PartialIndexPredicates). The predicates are converted from strings
// to ScalarExprs here.
//
// The predicates are used as "known truths" about table data. Any predicates
// containing non-immutable operators are omitted.
func (b *Builder) addPartialIndexPredicatesForTable(tabMeta *opt.TableMeta) {
	tab := tabMeta.Table

	// Find the first partial index.
	numIndexes := tab.IndexCount()
	indexOrd := 0
	for ; indexOrd < numIndexes; indexOrd++ {
		if _, ok := tab.Index(indexOrd).Predicate(); ok {
			break
		}
	}

	// Return early if there are no partial indexes. Only partial indexes have
	// predicates.
	if indexOrd == numIndexes {
		return
	}

	// Create a scope that can be used for building the scalar expressions.
	tableScope := b.allocScope()
	tableScope.appendOrdinaryColumnsFromTable(tabMeta, &tabMeta.Alias)

	// Skip to the first partial index we found above.
	for ; indexOrd < numIndexes; indexOrd++ {
		index := tab.Index(indexOrd)
		pred, ok := index.Predicate()

		// If the index is not a partial index, do nothing.
		if !ok {
			continue
		}

		expr, err := parser.ParseExpr(pred)
		if err != nil {
			panic(err)
		}

		texpr := tableScope.resolveAndRequireType(expr, types.Bool)
		var scalar = b.buildScalar(texpr, tableScope, nil, nil, nil)

		// Wrap the scalar in a FiltersItem.
		filter := memo.FiltersItem{Condition: scalar}

		// Wrap the predicate filter expression in a FiltersExpr and normalize
		// it.
		filters := memo.FiltersExpr{filter}

		// Add the filters to the table metadata.
		tabMeta.AddPartialIndexPredicate(indexOrd, &filters)
	}
}

func buildPrivateJoinForHint(
	left memo.RelExpr, right memo.RelExpr,
) (joinPrivate *memo.JoinPrivate) {

	joinPrivate = &memo.JoinPrivate{}
	leftType := keys.NormalJoin
	rightType := keys.NormalJoin
	switch typ := left.(type) {
	case *memo.ScanExpr:
		leftType = typ.Flags.ForceJoinType
	case *memo.InnerJoinExpr:
		if typ.Flags.JoinForView != keys.NormalJoin {
			switch typ.Flags.JoinForView {
			case keys.ForceMergeJoin:
				joinPrivate.Flags.DisallowLookupJoin = true
				joinPrivate.Flags.DisallowHashJoin = true
			case keys.ForceHashJoin:
				joinPrivate.Flags.DisallowMergeJoin = true
				joinPrivate.Flags.DisallowLookupJoin = true
			case keys.ForceLookUpJoin:
				joinPrivate.Flags.DisallowMergeJoin = true
				joinPrivate.Flags.DisallowHashJoin = true
			}
		}
	}
	switch typ := right.(type) {
	case *memo.ScanExpr:
		rightType = typ.Flags.ForceJoinType
	case *memo.InnerJoinExpr:
		if typ.Flags.JoinForView != keys.NormalJoin {
			switch typ.Flags.JoinForView {
			case keys.ForceMergeJoin:
				joinPrivate.Flags.DisallowLookupJoin = true
				joinPrivate.Flags.DisallowHashJoin = true
			case keys.ForceHashJoin:
				joinPrivate.Flags.DisallowMergeJoin = true
				joinPrivate.Flags.DisallowLookupJoin = true
			case keys.ForceLookUpJoin:
				joinPrivate.Flags.DisallowMergeJoin = true
				joinPrivate.Flags.DisallowHashJoin = true
			}
		}
	}

	bothType := rightType | leftType

	// HashJoin最高优先级
	if bothType&keys.ForceHashJoin == keys.ForceHashJoin {
		joinPrivate.Flags.DisallowLookupJoin = true
		joinPrivate.Flags.DisallowMergeJoin = true
		return
	}

	// MergeJoin优先级第二
	if bothType&keys.ForceMergeJoin == keys.ForceMergeJoin {
		joinPrivate.Flags.DisallowHashJoin = true
		joinPrivate.Flags.DisallowLookupJoin = true
		return
	}

	// LookUpJoin优先级最低
	if bothType&keys.ForceLookUpJoin == keys.ForceLookUpJoin {
		joinPrivate.Flags.DisallowMergeJoin = true
		joinPrivate.Flags.DisallowHashJoin = true
		return
	}
	return
}
