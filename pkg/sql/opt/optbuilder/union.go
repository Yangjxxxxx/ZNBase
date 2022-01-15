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
	"math"

	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// buildUnion builds a set of memo groups that represent the given union
// clause.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildUnion(
	clause *tree.UnionClause, desiredTypes []types.T, inScope *scope, mb *mutationBuilder,
) (outScope *scope) {

	leftScope := b.buildSelect(clause.Left, noRowLocking, desiredTypes, inScope, nil)
	rightScope := b.buildSelect(clause.Right, noRowLocking, desiredTypes, inScope, nil)
	setHidenColumn(leftScope)
	setHidenColumn(rightScope)

	// Remove any hidden columns, as they are not included in the Union.
	leftScope.removeHiddenCols()
	rightScope.removeHiddenCols()

	// Check that the number of columns matches.
	if len(leftScope.cols) != len(rightScope.cols) {
		panic(pgerror.NewErrorf(
			pgcode.Syntax,
			"each %v query must have the same number of columns: %d vs %d",
			clause.Type, len(leftScope.cols), len(rightScope.cols),
		))
	}

	outScope = inScope.push()

	// newColsNeeded indicates whether or not we need to synthesize output
	// columns. This is always required for a UNION, because the output columns
	// of the union contain values from the left and right relations, and we must
	// synthesize new columns to contain these values. This is not necessary for
	// INTERSECT or EXCEPT, since these operations are basically filters on the
	// left relation.
	newColsNeeded := clause.Type == tree.UnionOp
	if newColsNeeded {
		outScope.cols = make([]scopeColumn, 0, len(leftScope.cols))
	}

	// propagateTypesLeft/propagateTypesRight indicate whether we need to wrap
	// the left/right side in a projection to cast some of the columns to the
	// correct type.
	// For example:
	//   SELECT NULL UNION SELECT 1
	// The type of NULL is unknown, and the type of 1 is int. We need to
	// wrap the left side in a project operation with a Cast expression so the
	// output column will have the correct type.
	var propagateTypesLeft, propagateTypesRight bool

	desired := make([]types.T, 0, len(leftScope.cols))

	needCast := false
	// Build map from left columns to right columns.
	for i := range leftScope.cols {
		l := &leftScope.cols[i]
		r := &rightScope.cols[i]

		// TODO(dan): This currently checks whether the types are exactly the same,
		// but Postgres is more lenient:
		// http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
		canConvert := false
		var convertType types.T
		if desiredTypes == nil {
			convertType, canConvert = tree.GetUnionType(l.typ, r.typ)
		}
		if !needCast && !l.typ.Equivalent(r.typ) && canConvert {
			needCast = true
		}
		if !(l.typ.Equivalent(r.typ) || l.typ == types.Unknown || r.typ == types.Unknown || canConvert) {
			panic(pgerror.NewErrorf(pgcode.DatatypeMismatch,
				"%v types %s and %s cannot be matched", clause.Type, l.typ, r.typ))
		}
		if l.hidden != r.hidden {
			// This should never happen.
			panic(pgerror.NewAssertionErrorf("%v types cannot be matched", clause.Type))
		}

		var typ types.T
		if desiredTypes != nil {
			if l.typ != types.Unknown {
				typ = l.typ
				if r.typ == types.Unknown {
					propagateTypesRight = true
				}
			} else {
				typ = r.typ
				if r.typ != types.Unknown {
					propagateTypesLeft = true
				}
			}
		} else {
			if l.typ != types.Unknown && r.typ == types.Unknown {
				typ = l.typ
				propagateTypesRight = true
			}
			if r.typ != types.Unknown && l.typ == types.Unknown {
				typ = r.typ
				propagateTypesLeft = true
			}
			if canConvert {
				typ = convertType
			} else {
				typ = r.typ
				if typ == nil || typ == types.Unknown {
					typ = types.Any
				}
			}
			desired = append(desired, typ)
		}
		if newColsNeeded {
			b.synthesizeColumn(outScope, string(l.name), typ, nil, nil /* scalar */)
		}
	}

	if desiredTypes == nil && needCast {
		if !newColsNeeded {
			b.factory.Metadata().ClearMetadate()
		}
		leftScope = b.buildSelect(clause.Left, noRowLocking, desired, inScope, mb)
		rightScope = b.buildSelect(clause.Right, noRowLocking, desired, inScope, mb)
		leftScope.removeHiddenCols()
		rightScope.removeHiddenCols()
	}

	if propagateTypesLeft {
		leftScope = b.propagateTypes(leftScope, rightScope)
	}
	if propagateTypesRight {
		rightScope = b.propagateTypes(rightScope, leftScope)
	}

	// Create the mapping between the left-side columns, right-side columns and
	// new columns (if needed).
	leftCols := colsToColList(leftScope.cols)
	rightCols := colsToColList(rightScope.cols)
	var newCols opt.ColList
	if newColsNeeded {
		newCols = colsToColList(outScope.cols)
	} else {
		outScope.appendColumnsFromScope(leftScope)
		newCols = leftCols
	}

	left := leftScope.expr.(memo.RelExpr)
	right := rightScope.expr.(memo.RelExpr)
	private := memo.SetPrivate{LeftCols: leftCols, RightCols: rightCols, OutCols: newCols}

	if clause.All {
		switch clause.Type {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnionAll(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersectAll(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExceptAll(left, right, &private)
		}
	} else {
		switch clause.Type {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnion(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersect(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExcept(left, right, &private)
		}
	}

	return outScope
}

func setHidenColumn(outScope *scope) {
	for key, value := range outScope.cols {
		if string(value.name) == "startwithvirtualpathcolumn" {
			outScope.cols[key].hidden = true
		}
	}
}

// buildUnionOfInhertis builds a set of memo group that represent the union of given table
// with its son tables (which inherits this table, and its tableID stored in slice 'Inherits' at descriptor
// of this table).
// 函数返回继承表和其子表select的union结果
func (b *Builder) buildUnionOfInhertis(
	descID uint64, inheritsBy []sqlbase.ID, inscope *scope, hasUnion map[uint64]struct{},
) (leftScope *scope) {

	// 该函数通过描述符ID返回对于该表的 buildScan结果
	getSelectScopeByDescID := func(id uint64) *scope {
		tableRef := &tree.TableRef{TableID: int64(id)}
		ds := b.resolveDataSourceRef(tableRef, privilege.SELECT)
		switch t := ds.(type) {
		case cat.Table:
			return b.buildScanFromTableRef(t, tableRef, nil, nil, inscope)
		case cat.TableXQ:
			return b.buildScanFromTableXQRef(t, tableRef, nil, nil, inscope)
		default:
			panic(pgerror.NewAssertionErrorf("unsupport table type for inherit table"))
		}
	}

	// 该函数返回第一个参数中与第二个参数列名相同的columns的序号列表
	colsToColListByNames := func(colsTarget []scopeColumn, colsSource []scopeColumn) opt.ColList {
		colList := make(opt.ColList, len(colsSource))
		for i := range colsSource {
			for _, j := range colsTarget {
				if colsSource[i].name == j.name {
					colList[i] = j.id
				}
			}
		}
		return colList
	}

	// 由于在该函数中会进行从 uint64 到 uint32 的转换, 为避免由于精度丢失导致的问题, 需要对数字范围进行判断
	if descID > math.MaxUint32 {
		panic(pgerror.NewError(pgcode.InheritSelect, "some error happens when select from an inherit table since its id is out of range"))
	}
	// 当select from 父表时, 转换 out scope为父表union子表的结果
	if hasUnion == nil {
		hasUnion = make(map[uint64]struct{})
	}
	hasUnion[descID] = struct{}{}
	leftScope = getSelectScopeByDescID(descID)
	for _, sonDescID := range inheritsBy {
		if _, ok := hasUnion[uint64(sonDescID)]; ok {
			continue
		}
		sonDesc, err := sqlbase.GetTableDescFromID(b.ctx, b.evalCtx.Txn, sonDescID)
		if err != nil {
			panic(err)
		}
		right := b.buildUnionOfInhertis(uint64(sonDesc.ID), sonDesc.GetInheritsBy(), inscope, hasUnion)

		leftScope.removeHiddenCols()
		right.removeHiddenCols()

		private := memo.SetPrivate{
			LeftCols:  colsToColList(leftScope.cols),
			RightCols: colsToColListByNames(right.cols, leftScope.cols),
			OutCols:   colsToColList(leftScope.cols),
		}
		if len(private.LeftCols) != len(private.RightCols) { //that is impossible normally
			panic(pgerror.NewError(pgcode.InheritSelect,
				"some error happens when select from an inherit table, please try again"))
		}
		leftScope.expr = b.factory.ConstructUnionAll(leftScope.expr, right.expr, &private)
	}
	return leftScope
}

func (b *Builder) buildSetOp(
	typ tree.UnionType, all bool, inScope, leftScope, rightScope *scope,
) (outScope *scope) {
	// Remove any hidden columns, as they are not included in the Union.
	leftScope.removeHiddenCols()
	rightScope.removeHiddenCols()

	outScope = inScope.push()

	// propagateTypesLeft/propagateTypesRight indicate whether we need to wrap
	// the left/right side in a projection to cast some of the columns to the
	// correct type.
	// For example:
	//   SELECT NULL UNION SELECT 1
	// The type of NULL is unknown, and the type of 1 is int. We need to
	// wrap the left side in a project operation with a Cast expression so the
	// output column will have the correct type.
	propagateTypesLeft, propagateTypesRight := b.checkTypesMatch(
		leftScope, rightScope,
		true, /* tolerateUnknownLeft */
		true, /* tolerateUnknownRight */
		typ.String(),
	)

	if propagateTypesLeft {
		leftScope = b.propagateTypes(leftScope, rightScope)
	}
	if propagateTypesRight {
		rightScope = b.propagateTypes(rightScope, leftScope)
	}

	// For UNION, we have to synthesize new output columns (because they contain
	// values from both the left and right relations). This is not necessary for
	// INTERSECT or EXCEPT, since these operations are basically filters on the
	// left relation.
	if typ == tree.UnionOp {
		outScope.cols = make([]scopeColumn, 0, len(leftScope.cols))
		for i := range leftScope.cols {
			c := &leftScope.cols[i]
			b.synthesizeColumn(outScope, string(c.name), c.typ, nil, nil /* scalar */)
		}
	} else {
		outScope.appendColumnsFromScope(leftScope)
	}

	// Create the mapping between the left-side columns, right-side columns and
	// new columns (if needed).
	leftCols := colsToColList(leftScope.cols)
	rightCols := colsToColList(rightScope.cols)
	newCols := colsToColList(outScope.cols)

	left := leftScope.expr.(memo.RelExpr)
	right := rightScope.expr.(memo.RelExpr)
	private := memo.SetPrivate{LeftCols: leftCols, RightCols: rightCols, OutCols: newCols}

	if all {
		switch typ {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnionAll(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersectAll(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExceptAll(left, right, &private)
		}
	} else {
		switch typ {
		case tree.UnionOp:
			outScope.expr = b.factory.ConstructUnion(left, right, &private)
		case tree.IntersectOp:
			outScope.expr = b.factory.ConstructIntersect(left, right, &private)
		case tree.ExceptOp:
			outScope.expr = b.factory.ConstructExcept(left, right, &private)
		}
	}

	return outScope
}

// checkTypesMatch is used when the columns must match between two scopes (e.g.
// for a UNION). Throws an error if the scopes don't have the same number of
// columns, or when column types don't match 1-1, except:
//  - if tolerateUnknownLeft is set and the left column has Unknown type while
//    the right has a known type (in this case it returns propagateToLeft=true).
//  - if tolerateUnknownRight is set and the right column has Unknown type while
//    the right has a known type (in this case it returns propagateToRight=true).
//
// clauseTag is used only in error messages.
//
// TODO(dan): This currently checks whether the types are exactly the same,
// but Postgres is more lenient:
// http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
func (b *Builder) checkTypesMatch(
	leftScope, rightScope *scope,
	tolerateUnknownLeft bool,
	tolerateUnknownRight bool,
	clauseTag string,
) (propagateToLeft, propagateToRight bool) {
	// Check that the number of columns matches.
	if len(leftScope.cols) != len(rightScope.cols) {
		panic(pgerror.NewErrorf(
			pgcode.Syntax,
			"each %v query must have the same number of columns: %d vs %d",
			clauseTag, len(leftScope.cols), len(rightScope.cols),
		))
	}

	// Build map from left columns to right columns.
	for i := range leftScope.cols {
		l := &leftScope.cols[i]
		r := &rightScope.cols[i]

		if l.typ.Equivalent(r.typ) {
			continue
		}
		// Note that Unknown types are equivalent so at this point at most one of
		// the types can be Unknown.
		if l.typ == types.Unknown && tolerateUnknownLeft {
			propagateToLeft = true
			continue
		}
		if r.typ == types.Unknown && tolerateUnknownRight {
			propagateToRight = true
			continue
		}

		panic(builderError{fmt.Errorf("%v types %s and %s cannot be matched", clauseTag, l.typ, r.typ)})
	}
	return propagateToLeft, propagateToRight
}

// propagateTypes propagates the types of the source columns to the destination
// columns by wrapping the destination in a Project operation. The Project
// operation passes through columns that already have the correct type, and
// creates cast expressions for those that don't.
func (b *Builder) propagateTypes(dst, src *scope) *scope {
	expr := dst.expr.(memo.RelExpr)
	dstCols := dst.cols

	dst = dst.push()
	dst.cols = make([]scopeColumn, 0, len(dstCols))

	for i := 0; i < len(dstCols); i++ {
		dstType := dstCols[i].typ
		srcType := src.cols[i].typ
		if dstType == types.Unknown && srcType != types.Unknown {
			// Create a new column which casts the old column to the correct type.
			colType, _ := coltypes.DatumTypeToColumnType(srcType)
			castExpr := b.factory.ConstructCast(b.factory.ConstructVariable(dstCols[i].id), colType, false)
			b.synthesizeColumn(dst, string(dstCols[i].name), srcType, nil /* expr */, castExpr)
		} else {
			// The column is already the correct type, so add it as a passthrough
			// column.
			dst.appendColumn(&dstCols[i])
		}
	}
	dst.expr = b.constructProject(expr, dst.cols)
	return dst
}
