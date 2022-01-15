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
	// "bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util"
)

// excludedTableName is the name of a special Upsert data source. When a row
// cannot be inserted due to a conflict, the "excluded" data source contains
// that row, so that its columns can be referenced in the conflict clause:
//
//   INSERT INTO ab VALUES (1, 2) ON CONFLICT (a) DO UPDATE b=excluded.b+1
//
// It is located in the special zbdb_internal schema so that it never overlaps
// with user data sources.
var excludedTableName tree.TableName

func init() {
	// Clear explicit schema and catalog so that they're not printed in error
	// messages.
	excludedTableName = tree.MakeTableNameWithSchema("", "zbdb_internal", "excluded")
	excludedTableName.ExplicitSchema = false
	excludedTableName.ExplicitCatalog = false
}

// buildInsert builds a memo group for an InsertOp or UpsertOp expression. To
// begin, an input expression is constructed which outputs these columns to
// insert into the target table:
//
//   1. Columns explicitly specified by the user in SELECT or VALUES expression.
//
//   2. Columns not specified by the user, but having a default value declared
//      in schema (or being nullable).
//
//   3. Computed columns.
//
//   4. Mutation columns which are being added or dropped by an online schema
//      change.
//
// buildInsert starts by constructing the input expression, and then wraps it
// with Project operators which add default, computed, and mutation columns. The
// final input expression will project values for all columns in the target
// table. For example, if this is the schema and INSERT statement:
//
//   CREATE TABLE abcd (
//     a INT PRIMARY KEY,
//     b INT,
//     c INT DEFAULT(10),
//     d INT AS (b+c) STORED
//   )
//   INSERT INTO abcd (a) VALUES (1)
//
// Then an input expression equivalent to this would be built:
//
//   SELECT ins_a, ins_b, ins_c, ins_b + ins_c AS ins_d
//   FROM (VALUES (1, NULL, 10)) AS t(ins_a, ins_b, ins_c)
//
// If an ON CONFLICT clause is present (or if it was an UPSERT statement), then
// additional columns are added to the input expression:
//
//   1. Columns containing existing values fetched from the target table and
//      used to detect conflicts and to formulate the key/value update commands.
//
//   2. Columns containing updated values to set when a conflict is detected, as
//      specified by the user.
//
//   3. Computed columns which will be updated when a conflict is detected and
//      that are dependent on one or more updated columns.
//
// In addition, the insert and update column expressions are merged into a
// single set of upsert column expressions that toggle between the insert and
// update values depending on whether the canary column is null.
//
// For example, if this is the schema and INSERT..ON CONFLICT statement:
//
//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//   INSERT INTO abc VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b=10
//
// Then an input expression equivalent to this would be built:
//
//   SELECT
//     fetch_a,
//     fetch_b,
//     fetch_c,
//     CASE WHEN fetch_a IS NULL ins_a ELSE fetch_a END AS ups_a,
//     CASE WHEN fetch_a IS NULL ins_b ELSE 10 END AS ups_b,
//     CASE WHEN fetch_a IS NULL ins_c ELSE fetch_c END AS ups_c,
//   FROM (VALUES (1, 2, NULL)) AS ins(ins_a, ins_b, ins_c)
//   LEFT OUTER JOIN abc AS fetch(fetch_a, fetch_b, fetch_c)
//   ON ins_a = fetch_a
//
// The CASE expressions will often prevent the unnecessary evaluation of the
// update expression in the case where an insertion needs to occur. In addition,
// it simplifies logical property calculation, since a 1:1 mapping to each
// target table column from a corresponding input column is maintained.
//
// If the ON CONFLICT clause contains a DO NOTHING clause, then each UNIQUE
// index on the target table requires its own LEFT OUTER JOIN to check whether a
// conflict exists. For example:
//
//   CREATE TABLE ab (a INT PRIMARY KEY, b INT)
//   INSERT INTO ab (a, b) VALUES (1, 2) ON CONFLICT DO NOTHING
//
// Then an input expression equivalent to this would be built:
//
//   SELECT x, y
//   FROM (VALUES (1, 2)) AS input(x, y)
//   LEFT OUTER JOIN ab
//   ON input.x = ab.a
//   WHERE ab.a IS NULL
//
// Note that an ordered input to the INSERT does not provide any guarantee about
// the order in which mutations are applied, or the order of any returned rows
// (i.e. it won't become a physical property required of the Insert or Upsert
// operator). Not propagating input orderings avoids an extra sort when the
// ON CONFLICT clause is present, since it joins a new set of rows to the input
// and thereby scrambles the input ordering.
func (b *Builder) buildInsert(ins *tree.Insert, inScope *scope) (outScope *scope) {
	var ctes []cteSource
	if ins.With != nil {
		inScope, ctes = b.buildCTEs(ins.With, inScope, nil)
	}

	// INSERT INTO xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias := getAliasedTableName(ins.Table)

	// Find which table we're working on, check the permissions.
	tab, tabXQ, resName, viewFlag, ViewDependsColName, NotMutilView := b.resolveTable(tn, privilege.INSERT)
	inScope.UpdatableViewDepends.ViewDependsColName = ViewDependsColName

	// upsert view feature unsupported
	if viewFlag && (ins.OnConflict != nil || ins.OnConflict.IsMergeStmt()) {
		panic(pgerror.NewErrorf(pgcode.Syntax, "feature unsupported"))
	}

	ds, _ := b.resolveDataSource(tn, privilege.INSERT)

	if alias == nil {
		alias = &resName
	}
	values, ok := ins.Rows.Select.(*tree.ValuesClause)
	if ok {
		if ins.OnConflict == nil && len(values.Rows) >= 10 && !tab.IsHashPartition() {
			panic(builderError{pgerror.NewErrorf(
				pgcode.FeatureNotSupported, "fall back to opt insert")})
		}
	}

	if ins.OnConflict != nil {
		// UPSERT and INDEX ON CONFLICT will read from the table to check for
		// duplicates.

		if ins.OnConflict.MergeOrNot {
			tmp, ok := ins.Rows.Select.(*tree.SelectClause).From.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.TableName)
			if !ok {
				tmp = ins.Rows.Select.(*tree.SelectClause).From.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.Subquery).Select.(*tree.ParenSelect).Select.Select.(*tree.SelectClause).From.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.TableName)
			}

			isHashA := tab.IsHashPartition()
			isHashB := false

			ds, _ := b.resolveDataSource(tmp, privilege.SELECT)
			switch t := ds.(type) {
			case cat.Table:
				isHashB = t.IsHashPartition()
			case cat.View:
				isHashB = false
			}

			switch {
			case !isHashA && !isHashB:
				ins.OnConflict.MergeHashCondition = 0
			case isHashA && isHashB:
				ins.OnConflict.MergeHashCondition = 1
			case isHashA && !isHashB:
				ins.OnConflict.MergeHashCondition = 2
			case !isHashA && isHashB:
				ins.OnConflict.MergeHashCondition = 3
			}
		}
	}

	var mb mutationBuilder
	if ins.OnConflict != nil && ins.OnConflict.IsUpsertAlias() {
		mb.init(b, opt.UpsertOp, tab, tabXQ, *alias)
	} else {
		mb.init(b, opt.InsertOp, tab, tabXQ, *alias)
	}

	// Compute target columns in two cases:
	//
	//   1. When explicitly specified by name:
	//
	//        INSERT INTO <table> (<col1>, <col2>, ...) ...
	//
	//   2. When implicitly targeted by VALUES expression:
	//
	//        INSERT INTO <table> VALUES (...)
	//
	// Target columns for other cases can't be derived until the input expression
	// is built, at which time the number of input columns is known. At the same
	// time, the input expression cannot be built until DEFAULT expressions are
	// replaced and named target columns are known. So this step must come first.
	if viewFlag {
		if !NotMutilView {
			panic(pgerror.NewErrorf(pgcode.Syntax, "feature unsupported"))
		}
		if ins.OnConflict != nil || ins.OnConflict.IsMergeStmt() {
			panic(pgerror.NewErrorf(pgcode.Syntax, "feature unsupported"))
		}
		b.checkPrivilegeForUser(tn, ds, ins.Columns, privilege.INSERT, "")
		b.upperOwner = ds.GetOwner()
		b.writeOnView = true
		ColName := make([]string, 0)
		if len(ins.Columns) != 0 {
			viewColumnName := make(map[string]bool)
			for i := 0; i < ds.(cat.View).ColumnNameCount(); i++ {
				viewColumnName[string(ds.(cat.View).ColumnName(i))] = true
			}
			for i := range ins.Columns {
				if !viewColumnName[string(ins.Columns[i])] {
					panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: column %q does not exist", string(ins.Columns[i])))
				}
				if ViewDependsColName[string(ins.Columns[i])] == "" {
					panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: cannot insert into column %s of view %s \nDETAIL:"+
						"  View columns that are not columns of their base relation are not updatable", string(ds.(cat.View).ColumnName(i)), string(tn.TableName)))
				}
				ins.Columns[i] = tree.Name(ViewDependsColName[string(ins.Columns[i])])
			}
			b.checkPrivilegeForUser(tab.Name(), tab, ins.Columns, privilege.INSERT, ds.GetOwner())
			mb.addTargetNamedColsForInsert(ins.Columns)
		} else {
			values := mb.extractValuesInput(ins.Rows)
			if values != nil {
				// Ensure that the number of input columns does not exceed the number of
				// target columns.
				if len(values.Rows[0]) > ds.(cat.View).ColumnNameCount() {
					panic(pgerror.NewErrorf(pgcode.Syntax, "INSERT has more expressions than target columns, %d expressions for %d targets",
						len(values.Rows[0]), ds.(cat.View).ColumnNameCount()))
				}
				for i := range values.Rows[0] {
					if ViewDependsColName[string(ds.(cat.View).ColumnName(i))] == "" {
						panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: cannot insert into column %s of view %s \nDETAIL:"+
							"  View columns that are not columns of their base relation are not updatable", string(ds.(cat.View).ColumnName(i)), string(tn.TableName)))
					}
					ColName = append(ColName, ViewDependsColName[string(ds.(cat.View).ColumnName(i))])
				}
				b.checkPrivilegeForUser(tab.Name(), tab, tree.NewNameList(ColName), privilege.INSERT, ds.GetOwner())
				mb.addTargetNamedColsForInsert(tree.NewNameList(ColName))
			}
		}
		switch p := ins.Returning.(type) {
		case *tree.ReturningExprs:
			selectExprRet := tree.SelectExprs(*p)
			for i := range selectExprRet {
				switch returnType := selectExprRet[i].Expr.(type) {
				case *tree.UnresolvedName:
					if ViewDependsColName[returnType.Parts[0]] == "" {
						panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: invalid returning column name %s", returnType.Parts[0]))
					}
					b.checkPrivilegeForUser(tn, ds, tree.NameList{tree.Name(returnType.Parts[0])}, privilege.SELECT, "")
					returnType.Parts[0] = ViewDependsColName[returnType.Parts[0]]
					addChangedCol(inScope, returnType.Parts[0], CheckOwner, false)
				case tree.UnqualifiedStar:
					// TODO(xz): privilege support.
					panic(pgerror.NewErrorf(pgcode.Syntax, "feature unsupported *"))
				default:
				}
			}
		default:
		}
	} else {
		if len(ins.Columns) != 0 {
			// Target columns are explicitly specified by name.
			b.checkPrivilegeForUser(tn, tab, ins.Columns, privilege.INSERT, "")
			mb.addTargetNamedColsForInsert(ins.Columns)
		} else {
			values := mb.extractValuesInput(ins.Rows)
			if values != nil {
				// Target columns are implicitly targeted by VALUES expression in the
				// same order they appear in the target table schema.
				mb.addTargetTableColsForInsert(len(values.Rows[0]))
				cols := tree.NameList{}
				for i := range mb.targetColList {
					cols = append(cols, tab.Column(i).ColName())
				}
				b.checkPrivilegeForUser(tn, tab, cols, privilege.INSERT, "")
			}
		}
	}

	// Build the input rows expression if one was specified:
	//
	//   INSERT INTO <table> VALUES ...
	//   INSERT INTO <table> SELECT ... FROM ...
	//
	// or initialize an empty input if inserting default values (default values
	// will be added later):
	//
	//   INSERT INTO <table> DEFAULT VALUES
	//
	if !ins.DefaultValues() {
		// Replace any DEFAULT expressions in the VALUES clause, if a VALUES clause
		// exists:
		//
		//   INSERT INTO <table> VALUES (..., DEFAULT, ...)
		//
		rows := mb.replaceDefaultExprs(ins.Rows)

		mb.buildInputForInsert(inScope, rows, ins.OnConflict.IsMergeStmt(), ds)
		if !viewFlag && ins.Columns == nil {
			cols := tree.NameList{}
			for i := range mb.targetColList {
				cols = append(cols, tab.Column(i).ColName())
			}
			b.checkPrivilegeForUser(tn, tab, cols, privilege.INSERT, "")
		}
	} else {
		mb.buildInputForInsert(inScope, nil, ins.OnConflict.IsMergeStmt(), nil)
	}

	// Add default columns that were not explicitly specified by name or
	// implicitly targeted by input columns. This includes columns undergoing
	// write mutations, if they have a default value.
	mb.addDefaultColsForInsert()

	// Possibly round DECIMAL-related columns containing insertion values. Do
	// this before evaluating computed expressions, since those may depend on
	// the inserted columns.
	mb.roundDecimalValues(mb.insertOrds, false /* roundComputedCols */)

	// Add any computed columns. This includes columns undergoing write mutations,
	// if they have a computed value.
	mb.addComputedColsForInsert()

	var returning tree.ReturningExprs
	if resultsNeeded(ins.Returning) {
		returning = *ins.Returning.(*tree.ReturningExprs)
	}

	switch {
	// Case 1: Simple INSERT statement.
	case ins.OnConflict == nil:
		// Build the final insert statement, including any returned expressions.
		mb.buildInsert(returning)

	// Case 2: INSERT..ON CONFLICT DO NOTHING.
	case ins.OnConflict.DoNothing:
		// Wrap the input in one LEFT OUTER JOIN per UNIQUE index, and filter out
		// rows that have conflicts. See the buildInputForDoNothing comment for
		// more details.
		mb.buildInputForDoNothing(inScope, ins)

		// Since buildInputForDoNothing filters out rows with conflicts, always
		// insert rows that are not filtered.
		mb.buildInsert(returning)

	// Case 3: UPSERT statement.
	case ins.OnConflict.IsUpsertAlias():
		// Add columns which will be updated by the Upsert when a conflict occurs.
		// These are derived from the insert columns.
		mb.setUpsertCols(ins.Columns)

		// Check whether the existing rows need to be fetched in order to detect
		// conflicts.
		if mb.needExistingRows() {
			// Left-join each input row to the target table, using conflict columns
			// derived from the primary index as the join condition.
			vecCols := make([]string, 0)
			mb.buildInputForUpsert(inScope, mb.getPrimaryKeyColumnNames(), nil /* whereClause */, ins, vecCols)

			// Add additional columns for computed expressions that may depend on any
			// updated columns.
			mb.addComputedColsForUpdate()
		}

		// Build the final upsert statement, including any returned expressions.
		mb.buildUpsert(returning, false, false, false, false)

	// Case 4: INSERT..ON CONFLICT..DO UPDATE statement.
	default:
		// Left-join each input row to the target table, using the conflict columns
		// as the join condition.
		vecCols := make([]string, 0)
		// On Duplicate Key : Build Expr
		if ins.OnConflict.IsDuplicate {
			mapAlias := make(map[tree.Name]tree.Name, 0)
			var aliasNames tree.AliasClause // vlaues使用行列别名
			if valuesClause, ok := ins.Rows.Select.(*tree.ValuesClause); ok {
				aliasNames = valuesClause.Alias
			}
			var originNames tree.NameList
			if len(ins.Columns) != 0 {
				originNames = ins.Columns
			} else {
				nColNum := tab.ColumnCount()
				for i := 0; i < nColNum; i++ {
					originNames = append(originNames, tab.Column(i).ColName())
				}
			}
			if aliasNames.Alias != "" {
				mapAlias[aliasNames.Alias] = "excluded"
			}
			if len(aliasNames.Cols) > 0 {
				for i := range originNames {
					mapAlias[aliasNames.Cols[i]] = originNames[i]
				}
			}
			updateAliasNames(ins.OnConflict.Exprs, mapAlias)

			if !b.IsExecPortal {
				checkColNames := parseExpr(ins.OnConflict.Exprs, tree.Name(tn.Table()), 1, true)
				b.checkPrivilege(tn, tab, RemoveRepeatedElementForName(checkColNames), privilege.UPDATE, true)
				parseExpr(ins.OnConflict.Exprs, tree.Name(tn.Table()), 2, false)
			}
			vecCols = findIndexs(tab)
		}
		mb.buildInputForUpsert(inScope, ins.OnConflict.Columns, ins.OnConflict.Where, ins, vecCols)
		if !ins.OnConflict.IsMergeStmt() {
			// Derive the columns that will be updated from the SET expressions.
			mb.addTargetColsForUpdate(ins.OnConflict.Exprs)

			// Build each of the SET expressions.
			mb.addUpdateCols(ins.OnConflict.Exprs)
		} else {
			// merge into
			var targetColListForNotMatch opt.ColList
			mb.addTargetColsForMergeUpdate(ins, &targetColListForNotMatch)
			mb.addMergeUpdateCols(ins, targetColListForNotMatch)
		}
		// 为merge功能的多种情况进行区分, insert...on conflict功能三个变量均为false
		// 1. 仅有match部分 ==> onlyMatch
		// 2. match部分仅有delete情况 ==> onlyDelete
		// 3. notMatch部分仅有signal情况 ==> notMatchOnlySignal
		onlyMatch := ins.OnConflict.IsMergeStmt() && ins.OnConflict.MatchList != nil && ins.OnConflict.NotMatchList == nil

		hasDelete := true
		for _, match := range ins.OnConflict.MatchList {
			if match.IsDelete == false {
				hasDelete = false
				break
			}
		}
		onlyDelete := ins.OnConflict.IsMergeStmt() && ins.OnConflict.MatchList != nil && hasDelete

		signal := true
		for _, nMatch := range ins.OnConflict.NotMatchList {
			if nMatch.Sig == nil {
				signal = false
				break
			}
		}
		notMatchOnlySignal := ins.OnConflict.IsMergeStmt() && ins.OnConflict.NotMatchList != nil && signal
		isMerge := ins.OnConflict.IsMergeStmt()
		mb.buildUpsert(returning, onlyMatch, onlyDelete, notMatchOnlySignal, isMerge)
	}

	mb.outScope.expr = b.wrapWithCTEs(mb.outScope.expr, ctes)
	return mb.outScope
}

// RemoveRepeatedElementForName 切片去重
// Input: 	arr(初始切片)
// Output: 	newArr(去重后的切片)
func RemoveRepeatedElementForName(arr []tree.Name) []tree.Name {
	newArr := make([]tree.Name, 0)
	for i := 0; i < len(arr); i++ {
		repeat := false
		for j := i + 1; j < len(arr); j++ {
			if arr[i] == arr[j] {
				repeat = true
				break
			}
		}
		if !repeat {
			newArr = append(newArr, arr[i])
		}
	}
	return newArr
}

func updateAliasNames(inputExprs tree.UpdateExprs, mapAlias map[tree.Name]tree.Name) {
	for _, value := range inputExprs {
		updateAilasNamesForExpr(value, mapAlias)
	}
}

func updateAilasNamesForExpr(inExpr *tree.UpdateExpr, mapAlias map[tree.Name]tree.Name) {
	switch v := inExpr.Expr.(type) {
	case *tree.BinaryExpr:
		updateAilasBinaryExpr(v, mapAlias)
	case *tree.FuncExpr:
		updateAilasFuncExpr(v, mapAlias)
	}
}

func updateAilasBinaryExpr(binary *tree.BinaryExpr, mapAlias map[tree.Name]tree.Name) {
	switch r := binary.Right.(type) {
	case *tree.BinaryExpr:
		updateAilasBinaryExpr(r, mapAlias)
	case *tree.FuncExpr:
		updateAilasFuncExpr(r, mapAlias)
	case *tree.UnresolvedName:
		if value, ok := mapAlias[tree.Name(r.Parts[0])]; ok {
			r.Parts[0] = string(value)
			r.Parts[1] = "excluded"
			r.NumParts = 2
		} else {
			if len(r.Parts[1]) > 0 {
				if value, ok := mapAlias[tree.Name(r.Parts[1])]; ok {
					r.Parts[1] = string(value)
				}
			}
		}

	}
	switch l := binary.Left.(type) {
	case *tree.BinaryExpr:
		updateAilasBinaryExpr(l, mapAlias)
	case *tree.FuncExpr:
		updateAilasFuncExpr(l, mapAlias)
	case *tree.UnresolvedName:
		if value, ok := mapAlias[tree.Name(l.Parts[0])]; ok {
			l.Parts[0] = string(value)
			l.Parts[1] = "excluded"
			l.NumParts = 2
		} else {
			if len(l.Parts[1]) > 0 {
				if value, ok := mapAlias[tree.Name(l.Parts[1])]; ok {
					l.Parts[1] = string(value)
				}
			}
		}
	}
}

func updateAilasFuncExpr(fun *tree.FuncExpr, mapAlias map[tree.Name]tree.Name) {
	for _, value := range fun.Exprs {
		switch v := value.(type) {
		case *tree.BinaryExpr:
			updateAilasBinaryExpr(v, mapAlias)
		case *tree.FuncExpr:
			updateAilasFuncExpr(v, mapAlias)
		case *tree.UnresolvedName:
			if value, ok := mapAlias[tree.Name(v.Parts[0])]; ok {
				v.Parts[0] = string(value)
				v.Parts[1] = "excluded"
				v.NumParts = 2
			} else {
				if len(v.Parts[1]) > 0 {
					if value, ok := mapAlias[tree.Name(v.Parts[1])]; ok {
						v.Parts[1] = string(value)
					}
				}
			}
		}
	}
}

// parseExpr 解析表达式(处理表达式列表的表达式关系，合并相同表达式)
// Input: 	inputExprs(表达式列表)
// 		  	tblName(表名称)
// 			iType(处理类型，1：两个表达式右项不同；2：两个表达式右项相同)
// 			bIsCheck(是否校验权限)
// Output: 	checkColNames(需要检验权限的列，在需要检验权限时返回)
func parseExpr(
	inputExprs tree.UpdateExprs, tblName tree.Name, iType int, bIsCheck bool,
) tree.NameList {
	var checkColNames tree.NameList
	for key, value := range inputExprs {
		if bIsCheck {
			checkColNames = append(checkColNames, value.Names[0])
		}
		for i := key + 1; i < len(inputExprs); i++ {
			if checkExprOnDuplicate(value.Names[0], inputExprs[i].Expr, tblName) {
				if iType == 1 && value.Names[0] != inputExprs[i].Names[0] {
					combineExpr(inputExprs[i].Expr, value, tblName)
				} else if iType == 2 && value.Names[0] == inputExprs[i].Names[0] {
					combineExpr(inputExprs[i].Expr, value, tblName)
					break
				}
			}
		}
	}
	return checkColNames
}

// combineExpr 合并表达式(将符合条件的表达式合并)
// Input: 	outExpr(输出表达式)
// 			inExpr(被合并表达式)
// 			tblName(表名称)
func combineExpr(outExpr tree.Expr, inExpr *tree.UpdateExpr, tblName tree.Name) {
	switch t := outExpr.(type) {
	case *tree.BinaryExpr:
		switch v := inExpr.Expr.(type) {
		case *tree.BinaryExpr:
			res := &tree.BinaryExpr{Operator: 0, Left: nil, Right: nil}
			deepCopyBinaryExpr(res, v)
			t = reconstructBinaryExpr(res, t, inExpr.Names[0], tblName)
		case *tree.FuncExpr:
			res := &tree.FuncExpr{Func: tree.ResolvableFunctionReference{FunctionReference: nil}, Type: 0, Exprs: nil}
			deepCopyFuncExpr(res, v)
			t = reconstructBinaryExpr(res, t, inExpr.Names[0], tblName)
		default:
			t = reconstructBinaryExpr(v, t, inExpr.Names[0], tblName)
		}
	case *tree.FuncExpr:
		switch v := inExpr.Expr.(type) {
		case *tree.BinaryExpr:
			res := &tree.BinaryExpr{Operator: 0, Left: nil, Right: nil}
			deepCopyBinaryExpr(res, v)
			t = reconstructFuncExpr(res, t, inExpr.Names[0], tblName)
		case *tree.FuncExpr:
			res := &tree.FuncExpr{Func: tree.ResolvableFunctionReference{FunctionReference: nil}, Type: 0, Exprs: nil}
			deepCopyFuncExpr(res, v)
			t = reconstructFuncExpr(res, t, inExpr.Names[0], tblName)
		default:
			t = reconstructFuncExpr(v, t, inExpr.Names[0], tblName)
		}
	}
}

// reconstructFuncExpr 改造Function表达式(合并符合条件的Function表达式)
// Input: 	expr(被合并表达式)
// 			fun(输出表达式)
// 			name(表达式右侧字段名)
// 			tblName(表名称)
func reconstructFuncExpr(
	expr tree.Expr, fun *tree.FuncExpr, name tree.Name, tblName tree.Name,
) *tree.FuncExpr {
	for key, value := range fun.Exprs {
		switch v := value.(type) {
		case *tree.BinaryExpr:
			v = reconstructBinaryExpr(expr, v, name, tblName)
		case *tree.FuncExpr:
			v = reconstructFuncExpr(expr, v, name, tblName)
		case *tree.UnresolvedName:
			if tree.Name(v.Parts[0]) == name && tree.Name(v.Parts[1]) == tblName {
				fun.Exprs[key] = expr
			}
		}
	}
	return fun
}

// reconstructBinaryExpr 改造Binary表达式(合并符合条件的Binary表达式)
// Input: 	expr(被合并表达式)
// 			binary(输出表达式)
// 			name(表达式右侧字段名)
// 			tblName(表名称)
func reconstructBinaryExpr(
	expr tree.Expr, binary *tree.BinaryExpr, name tree.Name, tblName tree.Name,
) *tree.BinaryExpr {
	switch r := binary.Right.(type) {
	case *tree.BinaryExpr:
		r = reconstructBinaryExpr(expr, r, name, tblName)
	case *tree.FuncExpr:
		r = reconstructFuncExpr(expr, r, name, tblName)
	case *tree.UnresolvedName:
		if tree.Name(r.Parts[0]) == name && tree.Name(r.Parts[1]) == tblName {
			binary.Left = expr
		}
	}
	switch l := binary.Left.(type) {
	case *tree.BinaryExpr:
		l = reconstructBinaryExpr(expr, l, name, tblName)
	case *tree.FuncExpr:
		l = reconstructFuncExpr(expr, l, name, tblName)
	case *tree.UnresolvedName:
		if tree.Name(l.Parts[0]) == name && tree.Name(l.Parts[1]) == tblName {
			binary.Left = expr
		}
	}
	return binary
}

// deepCopyFuncExpr 深拷贝Function表达式
// Input: 	res(输出表达式)
// 			fun(被拷贝表达式)
func deepCopyFuncExpr(res, fun *tree.FuncExpr) {
	res.Func = tree.ResolvableFunctionReference{FunctionReference: fun.Func.FunctionReference}
	for _, value := range fun.Exprs {
		switch v := value.(type) {
		case *tree.BinaryExpr:
			b := &tree.BinaryExpr{Operator: 0, Left: nil, Right: nil}
			deepCopyBinaryExpr(b, v)
			res.Exprs = append(res.Exprs, b)
		case *tree.FuncExpr:
			f := &tree.FuncExpr{Func: tree.ResolvableFunctionReference{FunctionReference: nil}, Type: 0, Exprs: nil}
			deepCopyFuncExpr(f, v)
			res.Exprs = append(res.Exprs, f)
		case *tree.UnresolvedName:
			parts := tree.NameParts{v.Parts[0], v.Parts[1], v.Parts[2], v.Parts[3]}
			res.Exprs = append(res.Exprs, &tree.UnresolvedName{NumParts: v.NumParts, Parts: parts})
		case *tree.NumVal:
			res.Exprs = append(res.Exprs, &tree.NumVal{Value: v.Value, OrigString: v.OrigString})
		}
	}
}

// deepCopyBinaryExpr 深拷贝Binary表达式
// Input: 	res(输出表达式)
// 			fun(被拷贝表达式)
func deepCopyBinaryExpr(res, binary *tree.BinaryExpr) {
	res.Operator = binary.Operator

	switch r := binary.Right.(type) {
	case *tree.BinaryExpr:
		res.Right = &tree.BinaryExpr{Operator: 0, Left: nil, Right: nil}
		switch rr := res.Right.(type) {
		case *tree.BinaryExpr:
			deepCopyBinaryExpr(rr, r)
		}
	case *tree.FuncExpr:
		res.Right = &tree.FuncExpr{Func: tree.ResolvableFunctionReference{FunctionReference: nil}, Type: 0, Exprs: nil}
		switch rr := res.Right.(type) {
		case *tree.FuncExpr:
			deepCopyFuncExpr(rr, r)
		}
	case *tree.UnresolvedName:
		parts := tree.NameParts{r.Parts[0], r.Parts[1], r.Parts[2], r.Parts[3]}
		res.Right = &tree.UnresolvedName{NumParts: r.NumParts, Parts: parts}
	case *tree.NumVal:
		res.Right = &tree.NumVal{Value: r.Value, OrigString: r.OrigString}
	}

	switch l := binary.Left.(type) {
	case *tree.BinaryExpr:
		res.Left = &tree.BinaryExpr{Operator: 0, Left: nil, Right: nil}
		switch ll := res.Left.(type) {
		case *tree.BinaryExpr:
			deepCopyBinaryExpr(ll, l)
		}
	case *tree.FuncExpr:
		res.Left = &tree.FuncExpr{Func: tree.ResolvableFunctionReference{FunctionReference: nil}, Type: 0, Exprs: nil}
		switch ll := res.Left.(type) {
		case *tree.FuncExpr:
			deepCopyFuncExpr(ll, l)
		}
	case *tree.UnresolvedName:
		parts := tree.NameParts{l.Parts[0], l.Parts[1], l.Parts[2], l.Parts[3]}
		res.Left = &tree.UnresolvedName{NumParts: l.NumParts, Parts: parts}
	case *tree.NumVal:
		res.Left = &tree.NumVal{Value: l.Value, OrigString: l.OrigString}
	}
}

// checkExprOnDuplicate 校验OnDuplicate表达式(检查表达式是否符合合并条件)
// Input: 	name(表达式右侧字段名)
// 			expr(表达式)
// 			tblName(表名称)
// Output: 	bool(是否符合条件)
func checkExprOnDuplicate(name tree.Name, expr tree.Expr, tblName tree.Name) bool {
	switch e := expr.(type) {
	case *tree.BinaryExpr:
		return checkBinaryExpr(name, e, tblName, false)
	case *tree.FuncExpr:
		return checkFuncExpr(name, e, tblName, false)
	}
	return false
}

// checkBinaryExpr 校验Binary表达式(检查Binary表达式是否符合合并条件)
// Input: 	name(表达式右侧字段名)
// 			binary(Binary表达式)
// 			tblName(表名称)
// 			flag(递归调用时返回上一层是否有符合条件的表达式)
// Output: 	bool(是否符合条件)
func checkBinaryExpr(name tree.Name, binary *tree.BinaryExpr, tblName tree.Name, flag bool) bool {
	if flag {
		return flag
	}
	switch r := binary.Right.(type) {
	case *tree.BinaryExpr:
		if checkBinaryExpr(name, r, tblName, flag) {
			return true
		}
	case *tree.FuncExpr:
		if checkFuncExpr(name, r, tblName, flag) {
			return true
		}
	case *tree.UnresolvedName:
		if tree.Name(r.Parts[0]) == name && tree.Name(r.Parts[1]) == tblName {
			return true
		}
	}
	switch l := binary.Left.(type) {
	case *tree.BinaryExpr:
		if checkBinaryExpr(name, l, tblName, flag) {
			return true
		}
	case *tree.FuncExpr:
		if checkFuncExpr(name, l, tblName, flag) {
			return true
		}
	case *tree.UnresolvedName:
		if tree.Name(l.Parts[0]) == name && tree.Name(l.Parts[1]) == tblName {
			return true
		}
	}
	return flag
}

// checkFuncExpr 校验Function表达式(检查Function表达式是否符合合并条件)
// Input: 	name(表达式右侧字段名)
// 			fun(Fun表达式)
// 			tblName(表名称)
// 			flag(递归调用时返回上一层是否有符合条件的表达式)
// Output: 	bool(是否符合条件)
func checkFuncExpr(name tree.Name, fun *tree.FuncExpr, tblName tree.Name, flag bool) bool {
	if flag {
		return flag
	}
	for _, value := range fun.Exprs {
		switch v := value.(type) {
		case *tree.BinaryExpr:
			if checkBinaryExpr(name, v, tblName, false) {
				return true
			}
		case *tree.FuncExpr:
			if checkFuncExpr(name, v, tblName, false) {
				return true
			}
		case *tree.UnresolvedName:
			if tree.Name(v.Parts[0]) == name && tree.Name(v.Parts[1]) == tblName {
				return true
			}
		}
	}
	return flag
}

// needExistingRows returns true if an Upsert statement needs to fetch existing
// rows in order to detect conflicts. In some cases, it is not necessary to
// fetch existing rows, and then the KV Put operation can be used to blindly
// insert a new record or overwrite an existing record. This is possible when:
//
//   1. There are no secondary indexes. Existing values are needed to delete
//      secondary index rows when the update causes them to move.
//   2. All non-key columns (including mutation columns) have insert and update
//      values specified for them.
//   3. Each update value is the same as the corresponding insert value.
//
// TODO(andyk): The fast path is currently only enabled when the UPSERT alias
// is explicitly selected by the user. It's possible to fast path some queries
// of the form INSERT ... ON CONFLICT, but the utility is low and there are lots
// of edge cases (that caused real correctness bugs #13437 #13962). As a result,
// this support was removed and needs to re-enabled. See #14482.
func (mb *mutationBuilder) needExistingRows() bool {
	if mb.tab.DeletableIndexCount() > 1 {
		return true
	}

	// Key columns are never updated and are assumed to be the same as the insert
	// values.
	// TODO(andyk): This is not true in the case of composite key encodings. See
	// issue #34518.
	primary := mb.tab.Index(cat.PrimaryIndex)
	var keyOrds util.FastIntSet
	for i, n := 0, primary.LaxKeyColumnCount(); i < n; i++ {
		keyOrds.Add(primary.Column(i).Ordinal)
	}

	for i, n := 0, mb.tab.DeletableColumnCount(); i < n; i++ {
		if keyOrds.Contains(i) {
			// #1: Don't consider key columns.
			continue
		}
		insertColID := mb.insertColID(i)
		if insertColID == 0 {
			// #2: Non-key column does not have insert value specified.
			return true
		}
		if insertColID != mb.scopeOrdToColID(mb.updateOrds[i]) {
			// #3: Update value is not same as corresponding insert value.
			return true
		}
	}
	return false
}

// addTargetNamedColsForInsert adds a list of user-specified column names to the
// list of table columns that are the target of the Insert operation.
func (mb *mutationBuilder) addTargetNamedColsForInsert(names tree.NameList) {
	if len(mb.targetColList) != 0 {
		panic(pgerror.NewAssertionErrorf("addTargetNamedColsForInsert cannot be called more than once"))
	}

	// Add target table columns by the names specified in the Insert statement.
	mb.addTargetColsByName(names)

	// Ensure that primary key columns are in the target column list, or that
	// they have default values.
	mb.checkPrimaryKeyForInsert()

	// Ensure that foreign keys columns are in the target column list, or that
	// they have default values.
	mb.checkForeignKeysForInsert()
}

// checkPrimaryKeyForInsert ensures that the columns of the primary key are
// either assigned values by the INSERT statement, or else have default/computed
// values. If neither condition is true, checkPrimaryKeyForInsert raises an
// error.
func (mb *mutationBuilder) checkPrimaryKeyForInsert() {
	primary := mb.tab.Index(cat.PrimaryIndex)
	for i, n := 0, primary.KeyColumnCount(); i < n; i++ {
		col := primary.Column(i)
		if col.HasDefault() || col.IsComputed() {
			// The column has a default or computed value.
			continue
		}

		colID := mb.tabID.ColumnID(col.Ordinal)
		if mb.targetColSet.Contains(int(colID)) {
			// The column is explicitly specified in the target name list.
			continue
		}

		if col.ColName() == "hashnum" && col.Column.IsHidden() {
			continue
		}

		panic(pgerror.NewErrorf(pgcode.InvalidForeignKey,
			"missing %q primary key column", col.ColName()))
	}
}

// checkForeignKeysForInsert ensures that all composite foreign keys that
// specify the matching method as MATCH FULL have all of their columns assigned
// values by the INSERT statement, or else have default/computed values.
// Alternatively, all columns can be unspecified. If neither condition is true,
// checkForeignKeys raises an error. Here is an example:
//
//   CREATE TABLE orders (
//     id INT,
//     cust_id INT,
//     state STRING,
//     FOREIGN KEY (cust_id, state) REFERENCES customers (id, state) MATCH FULL
//   )
//
//   INSERT INTO orders (cust_id) VALUES (1)
//
// This INSERT statement would trigger a static error, because only cust_id is
// specified in the INSERT statement. Either the state column must be specified
// as well, or else neither column can be specified.
func (mb *mutationBuilder) checkForeignKeysForInsert() {
	for i, n := 0, mb.tab.IndexCount(); i < n; i++ {
		idx := mb.tab.Index(i)
		fkey, ok := idx.ForeignKey()
		if !ok {
			continue
		}

		// This check should only be performed on composite foreign keys that use
		// the MATCH FULL method.
		if fkey.Match != tree.MatchFull {
			continue
		}

		var missingCols []string
		allMissing := true
		for j := 0; j < int(fkey.PrefixLen); j++ {
			indexCol := idx.Column(j)
			if indexCol.HasDefault() || indexCol.IsComputed() {
				// The column has a default value.
				allMissing = false
				continue
			}

			colID := mb.tabID.ColumnID(indexCol.Ordinal)
			if mb.targetColSet.Contains(int(colID)) {
				// The column is explicitly specified in the target name list.
				allMissing = false
				continue
			}

			missingCols = append(missingCols, string(indexCol.ColName()))
		}
		if allMissing {
			continue
		}

		switch len(missingCols) {
		case 0:
			// Do nothing.
		case 1:
			panic(pgerror.NewErrorf(pgcode.ForeignKeyViolation,
				"missing value for column %q in multi-part foreign key", missingCols[0]))
		default:
			sort.Strings(missingCols)
			panic(pgerror.NewErrorf(pgcode.ForeignKeyViolation,
				"missing values for columns %q in multi-part foreign key", missingCols))
		}
	}
}

// addTargetTableColsForInsert adds up to maxCols columns to the list of columns
// that will be set by an INSERT operation. Non-mutation columns are added from
// the target table in the same order they appear in its schema. This method is
// used when the target columns are not explicitly specified in the INSERT
// statement:
//
//   INSERT INTO t VALUES (1, 2, 3)
//
// In this example, the first three columns of table t would be added as target
// columns.
func (mb *mutationBuilder) addTargetTableColsForInsert(maxCols int) {
	if len(mb.targetColList) != 0 {
		panic(pgerror.NewAssertionErrorf("addTargetTableColsForInsert cannot be called more than once"))
	}

	// Only consider non-mutation columns, since mutation columns are hidden from
	// the SQL user.
	numCols := 0
	for i, n := 0, mb.tab.ColumnCount(); i < n && numCols < maxCols; i++ {
		// Skip hidden columns.
		if mb.tab.Column(i).IsHidden() {
			continue
		}

		mb.addTargetCol(i)
		numCols++
	}

	// Ensure that the number of input columns does not exceed the number of
	// target columns.
	mb.checkNumCols(len(mb.targetColList), maxCols)
}

// buildInputForInsert constructs the memo group for the input expression and
// constructs a new output scope containing that expression's output columns.
func (mb *mutationBuilder) buildInputForInsert(
	inScope *scope, inputRows *tree.Select, isMerge bool, ds cat.DataSource,
) { // Handle DEFAULT VALUES case by creating a single empty row as input.
	if inputRows == nil {
		mb.outScope = inScope.push()
		mb.outScope.expr = mb.b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   mb.md.NextValuesID(),
		})
		return
	}

	// If there are already required target columns, then those will provide
	// desired input types. Otherwise, input columns are mapped to the table's
	// non-hidden columns by corresponding ordinal position. Exclude hidden
	// columns to prevent this statement from writing hidden columns:
	//
	//   INSERT INTO <table> VALUES (...)
	//
	// However, hidden columns can be written if the target columns were
	// explicitly specified:
	//
	//   INSERT INTO <table> (...) VALUES (...)
	//

	var desiredTypes []types.T
	if len(mb.targetColList) != 0 {
		desiredTypes = make([]types.T, len(mb.targetColList))
		for i, colID := range mb.targetColList {
			cm := mb.md.ColumnMeta(colID)
			desiredTypes[i] = cm.Type
			if desiredTypes[i] == types.Int {
				if mb.md.TableMeta(cm.Table).Table.ColumnByIDAndName(int(colID), cm.Alias).ColTypeWidth() == 16 {
					desiredTypes[i] = types.Int2
				}
				if mb.md.TableMeta(cm.Table).Table.ColumnByIDAndName(int(colID), cm.Alias).ColTypeWidth() == 32 {
					desiredTypes[i] = types.Int4
				}
			}
			if desiredTypes[i] == types.Float || desiredTypes[i] == types.Float4 {
				if mb.md.TableMeta(cm.Table).Table.Column(i).ColTypePrecision() == 16 {
					desiredTypes[i] = types.Float4
				}
			}
		}
	} else {
		// Do not target mutation columns.
		desiredTypes = make([]types.T, 0, mb.tab.ColumnCount())
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			tabCol := mb.tab.Column(i)
			if !tabCol.IsHidden() {
				desiredTypes = append(desiredTypes, tabCol.DatumType())
			}
		}
	}

	if inScope.builder.semaCtx.IVarContainer == nil {
		inScope.builder.semaCtx.IVarContainer = &tree.ForInsertHelper{}
	}
	inScope.builder.semaCtx.IVarContainer.SetForInsertOrUpdate(true)
	mb.outScope = mb.b.buildSelect(inputRows, noRowLocking, desiredTypes, inScope, mb)

	if len(mb.targetColList) != 0 {
		// Target columns already exist, so ensure that the number of input
		// columns exactly matches the number of target columns.
		mb.checkNumCols(len(mb.targetColList), len(mb.outScope.cols))

	} else {

		// add view column name according to select expression
		if viewName, ok := ds.(cat.View); ok {
			// Ensure that the number of input columns does not exceed the number of
			// target columns.
			if len(mb.outScope.cols) > ds.(cat.View).ColumnNameCount() {
				panic(pgerror.NewErrorf(pgcode.Syntax,
					"INSERT has more expressions than target columns, %d expressions for %d targets",
					len(mb.outScope.cols), ds.(cat.View).ColumnNameCount()))
			}
			ColName := make([]string, 0)
			for i := range mb.outScope.cols {
				if inScope.UpdatableViewDepends.ViewDependsColName[string(viewName.(cat.View).ColumnName(i))] == "" {
					panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: cannot insert into column %s of view %s \nDETAIL:"+
						"  View columns that are not columns of their base relation are not updatable", string(ds.(cat.View).ColumnName(i)), viewName.Name()))
				}
				ColName = append(ColName, inScope.UpdatableViewDepends.ViewDependsColName[string(viewName.(cat.View).ColumnName(i))])
			}
			mb.addTargetNamedColsForInsert(tree.NewNameList(ColName))
		} else {
			// No target columns have been added by previous steps, so add columns
			// that are implicitly targeted by the input expression.
			mb.addTargetTableColsForInsert(len(mb.outScope.cols))
		}
	}

	// Loop over input columns and:
	//   1. Type check each column
	//   2. Assign name to each column
	//   3. Add scope column ordinal to the insertOrds list.
	for i := range mb.outScope.cols {
		inCol := &mb.outScope.cols[i]
		ord := mb.tabID.ColumnOrdinal(mb.targetColList[i])

		// Type check the input column against the corresponding table column.
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), inCol.typ)

		// Assign name of input column. Computed columns can refer to this column
		// by its name.
		inCol.table = *mb.tab.Name()
		if !isMerge {
			inCol.name = tree.Name(mb.md.ColumnMeta(mb.targetColList[i]).Alias)
		}
		// Record the ordinal position of the scope column that contains the
		// value to be inserted into the corresponding target table column.
		mb.insertOrds[ord] = scopeOrdinal(i)
	}
	if inScope.UpdatableViewDepends.ChangedCol != nil {
		mb.outScope.UpdatableViewDepends.ChangedCol = inScope.UpdatableViewDepends.ChangedCol
	}
}

// addDefaultColsForInsert wraps an Insert input expression with a Project
// operator containing any default (or nullable) columns that are not yet part
// of the target column list. This includes mutation columns, since they must
// always have default or computed values.
func (mb *mutationBuilder) addDefaultColsForInsert() {
	mb.addSynthesizedCols(
		mb.insertOrds,
		func(tabCol cat.Column) bool { return !tabCol.IsComputed() },
	)
}

// addComputedColsForInsert wraps an Insert input expression with a Project
// operator containing computed columns that are not yet part of the target
// column list. This includes mutation columns, since they must always have
// default or computed values. This must be done after calling
// addDefaultColsForInsert, because computed columns can depend on default
// columns.
func (mb *mutationBuilder) addComputedColsForInsert() {
	mb.addSynthesizedCols(
		mb.insertOrds,
		func(tabCol cat.Column) bool { return tabCol.IsComputed() },
	)

	// Possibly round DECIMAL-related computed columns.
	mb.roundDecimalValues(mb.insertOrds, true /* roundComputedCols */)
}

func (mb *mutationBuilder) projectPartialIndexCols(colIDs []scopeOrdinal, predScope *scope) {
	if partialIndexCount(mb.tab) > 0 {
		projectionScope := mb.outScope.replace()
		projectionScope.appendColumnsFromScope(mb.outScope)

		ord := 0
		for i, n := 0, mb.tab.DeletableIndexCount(); i < n; i++ {
			index := mb.tab.Index(i)
			predicate, ok := index.Predicate()
			if !ok {
				continue
			}

			expr, err := parser.ParseExpr(predicate)
			if err != nil {
				panic(err)
			}

			texpr := predScope.resolveAndRequireType(expr, types.Bool)
			scopeCol := mb.b.addColumn(projectionScope, "", texpr)

			mb.b.buildScalar(texpr, predScope, projectionScope, scopeCol, nil)
			colIDs[ord] = scopeOrdinal(scopeCol.id)

			ord++
		}

		mb.b.constructProjectForScope(mb.outScope, projectionScope)
		mb.outScope = projectionScope
	}
}

// add del pred cols
func (mb *mutationBuilder) projectPartialIndexDelCols(predScope *scope) {
	mb.projectPartialIndexCols(mb.partialIndexDelColIDs, predScope)
}

// add pred col
func (mb *mutationBuilder) projectPartialIndexPutCols(predScope *scope) {
	mb.projectPartialIndexCols(mb.partialIndexPutColIDs, predScope)
}

// buildInsert constructs an Insert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildInsert(returning tree.ReturningExprs) {
	// Add any check constraint boolean columns to the input.
	mb.addCheckConstraintCols()

	preCheckScope := mb.outScope
	mb.projectPartialIndexPutCols(preCheckScope)

	private := mb.makeMutationPrivate(returning != nil)
	mb.outScope.expr = mb.b.factory.ConstructInsert(mb.outScope.expr, private)

	mb.buildReturning(returning)
}

// buildInputForDoNothing wraps the input expression in LEFT OUTER JOIN
// expressions, one for each UNIQUE index on the target table. It then adds a
// filter that discards rows that have a conflict (by checking a not-null table
// column to see if it was null-extended by the left join). See the comment
// header for Builder.buildInsert for an example.
func (mb *mutationBuilder) buildInputForDoNothing(inScope *scope, ins *tree.Insert) {
	//Do nothing privilege check
	tn := mb.tab.Name()
	tab := mb.tab
	mb.b.checkPrivilegeForUser(tn, tab, ins.OnConflict.Columns, privilege.SELECT, "")

	// DO NOTHING clause does not require ON CONFLICT columns.
	var conflictIndex cat.Index
	if len(ins.OnConflict.Columns) != 0 {
		// Check that the ON CONFLICT columns reference at most one target row by
		// ensuring they match columns of a UNIQUE index. Using LEFT OUTER JOIN
		// to detect conflicts relies upon this being true (otherwise result
		// cardinality could increase). This is also a Postgres requirement.
		conflictIndex = mb.ensureUniqueConflictCols(ins.OnConflict.Columns)
	}

	insertColSet := mb.outScope.expr.Relational().OutputCols
	insertColScope := mb.outScope.replace()
	insertColScope.appendColumnsFromScope(mb.outScope)

	// Loop over each UNIQUE index, potentially creating a left join + filter for
	// each one.
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
		index := mb.tab.Index(idx)
		if !index.IsUnique() {
			continue
		}

		// If conflict columns were explicitly specified, then only check for a
		// conflict on a single index. Otherwise, check on all indexes.
		if conflictIndex != nil && conflictIndex != index {
			continue
		}

		// Build the right side of the left outer join. Use a new metadata instance
		// of the mutation table so that a different set of column IDs are used for
		// the two tables in the self-join.
		tn := mb.tab.Name().TableName
		alias := tree.MakeUnqualifiedTableName(tree.Name(fmt.Sprintf("%s_%d", tn, idx+1)))
		tabID := mb.md.AddTableWithAliasAll(mb.tab, mb.tabXQ, &alias)
		scanScope := mb.b.buildScan(
			tabID,
			nil, /* ordinals */
			nil, /* indexFlags */
			noRowLocking,
			excludeMutations,
			inScope,
		)

		// Remember the column ID of a scan column that is not null. This will be
		// used to detect whether a conflict was detected for a row. Such a column
		// must always exist, since the index always contains the primary key
		// columns, either explicitly or implicitly.
		notNullColID := scanScope.cols[findNotNullIndexCol(index)].id

		// Build the join condition by creating a conjunction of equality conditions
		// that test each conflict column:
		//
		//   ON ins.x = scan.a AND ins.y = scan.b
		//
		var on memo.FiltersExpr
		for i, n := 0, index.LaxKeyColumnCount(); i < n; i++ {
			indexCol := index.Column(i)
			scanColID := scanScope.cols[indexCol.Ordinal].id

			condition := mb.b.factory.ConstructEq(
				mb.b.factory.ConstructVariable(mb.insertColID(indexCol.Ordinal)),
				mb.b.factory.ConstructVariable(scanColID),
			)
			on = append(on, memo.FiltersItem{Condition: condition})
		}

		// Construct the left join + filter.
		// TODO(andyk): Convert this to use anti-join once we have support for
		// lookup anti-joins.
		mb.outScope.expr = mb.b.factory.ConstructProject(
			mb.b.factory.ConstructSelect(
				mb.b.factory.ConstructLeftJoin(
					mb.outScope.expr,
					scanScope.expr,
					on,
					memo.EmptyJoinPrivate,
				),
				memo.FiltersExpr{memo.FiltersItem{
					Condition: mb.b.factory.ConstructIs(
						mb.b.factory.ConstructVariable(notNullColID),
						memo.NullSingleton,
					),
				}},
			),
			memo.EmptyProjectionsExpr,
			insertColSet,
		)
	}

	// Loop over each arbiter index, creating an upsert-distinct-on for each one.
	// This must happen after all conflicting rows are removed with the left-join
	// filters created above, to avoid removing valid rows (see #59125).
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {

		index := mb.tab.Index(idx)
		_, isPartial := index.Predicate()

		// If the index is a partial index, project a new column that allows the
		// UpsertDistinctOn to only de-duplicate insert rows that satisfy the
		// partial index predicate. See projectPartialIndexDistinctColumn for more
		// details.
		var partialIndexDistinctCol *scopeColumn
		if isPartial {
			partialIndexDistinctCol = mb.projectPartialIndexDistinctColumn(insertColScope, idx)
		}

		// Add an UpsertDistinctOn operator to ensure there are no duplicate input
		// rows for this unique index. Duplicate rows can trigger conflict errors
		// at runtime, which DO NOTHING is not supposed to do. See issue #37880.
		var conflictCols opt.ColSet
		for i, n := 0, index.LaxKeyColumnCount(); i < n; i++ {
			indexCol := index.Column(i)
			conflictCols.Add(int(mb.insertColID(indexCol.Ordinal)))
		}
		if partialIndexDistinctCol != nil {
			conflictCols.Add(int(partialIndexDistinctCol.id))
		}

		// Treat NULL values as distinct from one another. And if duplicates are
		// detected, remove them rather than raising an error.
		mb.outScope = mb.b.buildDistinctOn(
			conflictCols, mb.outScope)

		// Remove the partialIndexDistinctCol from the output.
		if isPartial {
			projectionScope := mb.outScope.replace()
			projectionScope.appendColumnsFromScope(insertColScope)
			mb.b.constructProjectForScope(mb.outScope, projectionScope)
			mb.outScope = projectionScope
		}
	}

	mb.targetColList = make(opt.ColList, 0, mb.tab.DeletableColumnCount())
	mb.targetColSet = opt.ColSet{}
}

func (mb *mutationBuilder) projectPartialIndexDistinctColumn(
	insertScope *scope, idx cat.IndexOrdinal,
) *scopeColumn {
	projectionScope := mb.outScope.replace()
	projectionScope.appendColumnsFromScope(insertScope)

	predExpr := mb.parsePartialIndexPredicateExpr(idx)
	expr := &tree.OrExpr{
		Left:  predExpr,
		Right: tree.DNull,
	}
	texpr := insertScope.resolveAndRequireType(expr, types.Bool)

	alias := fmt.Sprintf("upsert_partial_index_distinct%d", idx)
	scopeCol := mb.b.addColumn(projectionScope, alias, texpr)
	mb.b.buildScalar(texpr, mb.outScope, projectionScope, scopeCol, nil)

	mb.b.constructProjectForScope(mb.outScope, projectionScope)
	mb.outScope = projectionScope

	return scopeCol
}

// parsePartialIndexPredicateExpr parses the partial index predicate for the
// given index and caches it for reuse. This function panics if the index at the
// given ordinal is not a partial index.
func (mb *mutationBuilder) parsePartialIndexPredicateExpr(idx cat.IndexOrdinal) tree.Expr {
	index := mb.tab.Index(idx)

	predStr, isPartial := index.Predicate()
	if !isPartial {
		panic(errors.AssertionFailedf("index at ordinal %d is not a partial index", idx))
	}

	if mb.parsedIndexExprs == nil {
		mb.parsedIndexExprs = make([]tree.Expr, mb.tab.DeletableIndexCount())
	}

	// Return expression from the cache, if it was already parsed previously.
	if mb.parsedIndexExprs[idx] != nil {
		return mb.parsedIndexExprs[idx]
	}

	expr, err := parser.ParseExpr(predStr)
	if err != nil {
		panic(err)
	}

	mb.parsedIndexExprs[idx] = expr
	return expr
}

// buildInputForUpsert assumes that the output scope already contains the insert
// columns. It left-joins each insert row to the target table, using the given
// conflict columns as the join condition. It also selects one of the table
// columns to be a "canary column" that can be tested to determine whether a
// given insert row conflicts with an existing row in the table. If it is null,
// then there is no conflict.
func (mb *mutationBuilder) buildInputForUpsert(
	inScope *scope,
	conflictCols tree.NameList,
	whereClause *tree.Where,
	ins *tree.Insert,
	vecCols []string,
) {
	// Check that the ON CONFLICT columns reference at most one target row.
	// Using LEFT OUTER JOIN to detect conflicts relies upon this being true
	// (otherwise result cardinality could increase). This is also a Postgres
	// requirement.
	tn := mb.tab.Name()
	tab := mb.tab
	if !ins.OnConflict.IsUpsertAlias() {
		//Insert on conflict Do Update privilege check
		var updateCols tree.NameList
		for i := range ins.OnConflict.Exprs {
			for j := range ins.OnConflict.Exprs[i].Names {
				updateCols = append(updateCols, ins.OnConflict.Exprs[i].Names[j])
			}
		}
		mb.b.checkPrivilegeForUser(tn, tab, updateCols, privilege.UPDATE, "")

		for i := range ins.OnConflict.Columns {
			updateCols = append(updateCols, ins.OnConflict.Columns[i])
		}
		mb.b.checkPrivilegeForUser(tn, tab, updateCols, privilege.SELECT, "")
	}

	// TODO: add by cms , if there is not merge into, run the previous method
	// if it is merge into, change the table name and do not check the unique constraint
	if !ins.OnConflict.IsMergeStmt() {
		if len(vecCols) == 0 {
			mb.ensureUniqueConflictCols(conflictCols)
		}

		// Re-alias all INSERT columns so that they are accessible as if they were
		// part of a special data source named "zbdb_internal.excluded".
		for i := range mb.outScope.cols {
			mb.outScope.cols[i].table = excludedTableName
		}
	} else {
		// TODO: add by cms , if it's merge sql, check the assertion, change table's name
		// if assert failed, table name will be 'excluded'
		table := excludedTableName
		// first , check if table has alias
		// if so , use alias, if not, use tableName
		if selectclause, ok := ins.Rows.Select.(*tree.SelectClause); ok {
			if atexpr, ok := selectclause.From.Tables[0].(*tree.AliasedTableExpr); ok {
				if atexpr.As.Alias == "" {
					// no alias
					if tablename, ok := atexpr.Expr.(*tree.TableName); ok {
						table = *(tablename)
					}
				} else {
					// has alias
					table.TableName = atexpr.As.Alias
				}
			}
		}
		for i := range mb.outScope.cols {
			mb.outScope.cols[i].table = table
		}
	}

	// Build the right side of the left outer join. Include mutation columns
	// because they can be used by computed update expressions. Use a different
	// instance of table metadata so that col IDs do not overlap.
	inputTabID := mb.md.AddTableWithAliasAll(mb.tab, mb.tabXQ, &mb.alias)
	fetchScope := mb.b.buildScan(
		inputTabID,
		nil, /* ordinals */
		nil, /* indexFlags */
		noRowLocking,
		includeMutations,
		inScope,
	)

	// Record a not-null "canary" column. After the left-join, this will be null
	// if no conflict has been detected, or not null otherwise. At least one not-
	// null column must exist, since primary key columns are not-null.
	canaryScopeCol := &fetchScope.cols[findNotNullIndexCol(mb.tab.Index(cat.PrimaryIndex))]
	mb.canaryColID = canaryScopeCol.id

	// Set fetchOrds to point to the scope columns created for the fetch values.
	for i := range fetchScope.cols {
		// Fetch columns come after insert columns.
		mb.fetchOrds[i] = scopeOrdinal(len(mb.outScope.cols) + i)
	}

	// Add the fetch columns to the current scope. It's OK to modify the current
	// scope because it contains only INSERT columns that were added by the
	// mutationBuilder, and which aren't needed for any other purpose.
	mb.outScope.appendColumnsFromScope(fetchScope)

	// Build the join condition by creating a conjunction of equality conditions
	// that test each conflict column:
	//
	//   ON ins.x = scan.a AND ins.y = scan.b
	//
	var on memo.FiltersExpr
	// TODO: add by cms, try to construct a condition struct
	if !ins.OnConflict.IsMergeStmt() {
		if len(vecCols) != 0 {
			var condition opt.ScalarExpr
			for _, col := range vecCols {
				var cond opt.ScalarExpr
				for i := range fetchScope.cols {
					sCols := strings.Split(col, ";")
					if len(sCols) > 1 {
						cond = mb.doUnionExpr(sCols, fetchScope)
					} else {
						fetchCol := &fetchScope.cols[i]
						if fetchCol.name == tree.Name(col) {
							cond = mb.buildConstruct(mb.b.factory.ConstructVariable(mb.insertColID(i)), mb.b.factory.ConstructVariable(fetchCol.id), 1)
						}
					}
					if cond != nil {
						if condition == nil {
							condition = cond
						} else {
							condition = mb.buildConstruct(condition, cond, 2)
						}
						break
					}
				}
			}
			on = append(on, memo.FiltersItem{Condition: condition})
		} else {
			for _, name := range conflictCols {
				for i := range fetchScope.cols {
					fetchCol := &fetchScope.cols[i]
					if fetchCol.name == name {
						condition := mb.buildConstruct(mb.b.factory.ConstructVariable(mb.insertColID(i)), mb.b.factory.ConstructVariable(fetchCol.id), 1)
						on = append(on, memo.FiltersItem{Condition: condition})
						break
					}
				}
			}
		}
	} else {
		filter := mb.b.buildScalar(
			mb.outScope.resolveAndRequireType(ins.OnConflict.Condition.Expr, types.Bool), mb.outScope, nil, nil, nil,
		)
		on = memo.FiltersExpr{{Condition: filter}}
	}

	// Construct the left join.
	mb.outScope.expr = mb.b.factory.ConstructLeftJoin(
		mb.outScope.expr,
		fetchScope.expr,
		on,
		memo.EmptyJoinPrivate,
	)

	// Add a filter from the WHERE clause if one exists.
	if whereClause != nil {
		where := &tree.Where{
			Type: whereClause.Type,
			Expr: &tree.OrExpr{
				Left: &tree.ComparisonExpr{
					Operator:    tree.IsNotDistinctFrom,
					Left:        canaryScopeCol,
					Right:       tree.DNull,
					IsCanaryCol: true,
				},
				Right: whereClause.Expr,
			},
		}

		mb.b.buildWhere(where, mb.outScope)
	}

	mb.targetColList = make(opt.ColList, 0, mb.tab.DeletableColumnCount())
	mb.targetColSet = opt.ColSet{}
}

// buildConstruct 构建各种操作的表达式
// Input: 	left(左表达式)
// 			right(右表达式)
// 			iType(表达式类型, 1:Eq;2:Or)
// Output: 	结果表达式
func (mb *mutationBuilder) buildConstruct(left, right opt.ScalarExpr, iType int) opt.ScalarExpr {
	switch iType {
	case 1:
		return mb.b.factory.ConstructEq(left, right)
	case 2:
		return mb.b.factory.ConstructOr(left, right)
	case 3:
		return mb.b.factory.ConstructAnd(left, right)
	default:
		return nil
	}
}

// doUnionExpr 处理联合索引和联合主键
// Input: 	sCols(联合键列表)
// 			fetchScope(表列的列表)
// Output: 	condition(And表达式)
func (mb *mutationBuilder) doUnionExpr(
	sCols []string, fetchScope *scope,
) (condition opt.ScalarExpr) {
	for _, col := range sCols {
		var cond opt.ScalarExpr
		for i := range fetchScope.cols {
			fetchCol := &fetchScope.cols[i]
			if fetchCol.name == tree.Name(col) {
				cond = mb.buildConstruct(mb.b.factory.ConstructVariable(mb.insertColID(i)), mb.b.factory.ConstructVariable(fetchCol.id), 1)
				if condition == nil {
					condition = cond
				} else {
					condition = mb.buildConstruct(condition, cond, 3)
				}
				break
			}
		}
	}
	return
}

// setUpsertCols sets the list of columns to be updated in case of conflict.
// There are two cases to handle:
//
//   1. Target columns are explicitly specified:
//        UPSERT INTO abc (col1, col2, ...) <input-expr>
//
//   2. Target columns are implicitly derived:
//        UPSERT INTO abc <input-expr>
//
// In case #1, only the columns that were specified by the user will be updated.
// In case #2, all non-mutation columns in the table will be updated.
//
// Note that primary key columns (i.e. the conflict detection columns) are never
// updated. This can have an impact in unusual cases where equal SQL values have
// different representations. For example:
//
//   CREATE TABLE abc (a DECIMAL PRIMARY KEY, b DECIMAL)
//   INSERT INTO abc VALUES (1, 2.0)
//   UPSERT INTO abc VALUES (1.0, 2)
//
// The UPSERT statement will update the value of column "b" from 2 => 2.0, but
// will not modify column "a".
func (mb *mutationBuilder) setUpsertCols(insertCols tree.NameList) {
	//Upsert privilege check
	tn := mb.tab.Name()
	tab := mb.tab

	cols := tree.NameList{}
	if len(insertCols) != 0 {
		cols = insertCols
	} else {
		for i := range mb.targetColList {
			cols = append(cols, tab.Column(i).ColName())
		}
	}
	mb.b.checkPrivilegeForUser(tn, tab, cols, privilege.UPDATE, "")
	mb.b.checkPrivilegeForUser(tn, tab, cols, privilege.SELECT, "")

	if len(insertCols) != 0 {
		for _, name := range insertCols {
			// Table column must exist, since existence of insertCols has already
			// been checked previously.
			ord := cat.FindTableColumnByName(mb.tab, name)
			mb.updateOrds[ord] = mb.insertOrds[ord]
		}
	} else {
		copy(mb.updateOrds, mb.insertOrds)
	}

	// Never update mutation columns.
	for i, n := mb.tab.ColumnCount(), mb.tab.DeletableColumnCount(); i < n; i++ {
		mb.updateOrds[i] = -1
	}

	// Never update primary key columns.
	conflictIndex := mb.tab.Index(cat.PrimaryIndex)
	for i, n := 0, conflictIndex.KeyColumnCount(); i < n; i++ {
		mb.updateOrds[conflictIndex.Column(i).Ordinal] = -1
	}
}

// buildUpsert constructs an Upsert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildUpsert(
	returning tree.ReturningExprs,
	onlyMatch bool,
	onlyDelete bool,
	notMatchOnlySignal bool,
	isMerge bool,
) {
	// Merge input insert and update columns using CASE expressions.
	mb.projectUpsertColumns(onlyMatch, onlyDelete, notMatchOnlySignal)

	// Add any check constraint boolean columns to the input.
	mb.addCheckConstraintCols()

	mb.projectPartialIndexPutCols(mb.outScope)

	private := mb.makeMutationPrivate(returning != nil)
	private.OnlyMatch = onlyMatch
	private.IsMerge = isMerge
	mb.outScope.expr = mb.b.factory.ConstructUpsert(mb.outScope.expr, private)

	mb.buildReturning(returning)
}

// projectUpsertColumns projects a set of merged columns that will be either
// inserted into the target table, or else used to update an existing row,
// depending on whether the canary column is null. For example:
//
//   UPSERT INTO ab VALUES (ins_a, ins_b) ON CONFLICT (a) DO UPDATE SET b=upd_b
//
// will cause the columns to be projected:
//
//   SELECT
//     fetch_a,
//     fetch_b,
//     CASE WHEN fetch_a IS NULL ins_a ELSE fetch_a END AS ups_a,
//     CASE WHEN fetch_b IS NULL ins_b ELSE upd_b END AS ups_b,
//   FROM (SELECT ins_a, ins_b, upd_b, fetch_a, fetch_b FROM ...)
//
// For each column, a CASE expression is created that toggles between the insert
// and update values depending on whether the canary column is null. These
// columns can then feed into any constraint checking expressions, which operate
// on the final result values.
func (mb *mutationBuilder) projectUpsertColumns(
	onlyMatch bool, onlyDelete bool, notMatchOnlySignal bool,
) {
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)

	// Add a new column for each target table column that needs to be upserted.
	// This can include mutation columns.
	for i, n := 0, mb.tab.DeletableColumnCount(); i < n; i++ {
		insertScopeOrd := mb.insertOrds[i]
		// if it is merge and onlyMatch, need to restore the true insertOrds to avoid insert NULL
		// because null may make conflict to some column should be not null
		if onlyMatch || notMatchOnlySignal {
			if mb.insertOrds[i] <= 1-secrecMergeNum {
				// row_id column need'n to be change
				insertScopeOrd = restoreMergeNum(mb.insertOrds[i])
				mb.insertOrds[i] = insertScopeOrd
			}
		}
		updateScopeOrd := mb.updateOrds[i]
		if onlyDelete {
			updateScopeOrd = mb.fetchOrds[i]
			mb.updateOrds[i] = updateScopeOrd
		}
		if updateScopeOrd == -1 {
			updateScopeOrd = mb.fetchOrds[i]
		}

		// Skip columns that will only be inserted or only updated.
		if insertScopeOrd == -1 || updateScopeOrd == -1 {
			continue
		}

		if insertScopeOrd <= 1-secrecMergeNum {
			insertScopeOrd = -1
			mb.insertOrds[i] = -1
		}

		// Skip columns where the insert value and update value are the same.
		if !onlyDelete {
			if mb.scopeOrdToColID(insertScopeOrd) == mb.scopeOrdToColID(updateScopeOrd) {
				continue
			}
		}

		var insertOrdStmt opt.ScalarExpr
		if insertScopeOrd == -1 {
			insertOrdStmt = memo.NullSingleton
		} else {
			insertOrdStmt = mb.b.factory.ConstructVariable(mb.outScope.cols[insertScopeOrd].id)
		}
		// Generate CASE that toggles between insert and update column.
		caseExpr := mb.b.factory.ConstructCase(
			memo.TrueSingleton,
			memo.ScalarListExpr{
				mb.b.factory.ConstructWhen(
					mb.b.factory.ConstructIs(
						mb.b.factory.ConstructVariable(mb.canaryColID),
						memo.NullSingleton,
					),
					insertOrdStmt,
				),
			},
			mb.b.factory.ConstructVariable(mb.outScope.cols[updateScopeOrd].id),
		)

		alias := fmt.Sprintf("upsert_%s", mb.tab.Column(i).ColName())
		var typ types.T
		if insertScopeOrd != -1 {
			typ = mb.outScope.cols[insertScopeOrd].typ
		} else {
			typ = types.Unknown
		}
		scopeCol := mb.b.synthesizeColumn(projectionsScope, alias, typ, nil /* expr */, caseExpr)
		scopeColOrd := scopeOrdinal(len(projectionsScope.cols) - 1)

		// Assign name to synthesized column. Check constraint columns may refer
		// to columns in the table by name.
		scopeCol.table = *mb.tab.Name()
		scopeCol.name = mb.tab.Column(i).ColName()

		// Update the scope ordinals for the update columns that are involved in
		// the Upsert. The new columns will be used by the Upsert operator in place
		// of the original columns. Also set the scope ordinals for the upsert
		// columns, as those columns can be used by RETURNING columns.
		if mb.updateOrds[i] != -1 {
			mb.updateOrds[i] = scopeColOrd
		}
		mb.upsertOrds[i] = scopeColOrd
	}

	mb.b.constructProjectForScope(mb.outScope, projectionsScope)
	mb.outScope = projectionsScope
}

// ensureUniqueConflictCols tries to prove that the given list of column names
// correspond to the columns of at least one UNIQUE index on the target table.
// If true, then ensureUniqueConflictCols returns the matching index. Otherwise,
// it reports an error.
func (mb *mutationBuilder) ensureUniqueConflictCols(cols tree.NameList) cat.Index {
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
		index := mb.tab.Index(idx)

		// Skip non-unique indexes. Use lax key columns, which always contain
		// the minimum columns that ensure uniqueness. Null values are considered
		// to be *not* equal, but that's OK because the join condition rejects
		// nulls anyway.
		if !index.IsUnique() || index.LaxKeyColumnCount() != len(cols) {
			continue
		}

		found := true
		for col, colCount := 0, index.LaxKeyColumnCount(); col < colCount; col++ {
			if cols[col] != index.Column(col).ColName() {
				found = false
				break
			}
		}

		if found {
			return index
		}
	}
	panic(pgerror.NewErrorf(pgcode.InvalidColumnReference,
		"there is no unique or exclusion constraint matching the ON CONFLICT specification"))
}

// getPrimaryKeyColumnNames returns the names of all primary key columns in the
// target table.
func (mb *mutationBuilder) getPrimaryKeyColumnNames() tree.NameList {
	pkIndex := mb.tab.Index(cat.PrimaryIndex)
	names := make(tree.NameList, pkIndex.KeyColumnCount())
	for i, n := 0, pkIndex.KeyColumnCount(); i < n; i++ {
		names[i] = pkIndex.Column(i).ColName()
	}
	return names
}

func partialIndexCount(tab cat.Table) int {
	count := 0
	for i, n := 0, tab.DeletableIndexCount(); i < n; i++ {
		if _, ok := tab.Index(i).Predicate(); ok {
			count++
		}
	}
	return count
}
