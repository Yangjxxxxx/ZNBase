// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"context"

	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
)

// IndexPredicateValidator validates that an expression is a valid partial index
// predicate. See Validate for more details.
type IndexPredicateValidator struct {
	ctx       context.Context
	tableName tree.TableName
	desc      sqlbase.TableDescriptor
	semaCtx   *tree.SemaContext
}

// MakeIndexPredicateValidator returns an IndexPredicateValidator struct that
// can be used to validate partial index predicates. See Validate for more
// details.
func MakeIndexPredicateValidator(
	ctx context.Context,
	tableName tree.TableName,
	desc sqlbase.TableDescriptor,
	semaCtx *tree.SemaContext,
) IndexPredicateValidator {
	return IndexPredicateValidator{
		ctx:       ctx,
		tableName: tableName,
		desc:      desc,
		semaCtx:   semaCtx,
	}
}

// Validate verifies that an expression is a valid partial index predicate. If
// the expression is valid, it returns the serialized expression with the
// columns dequalified.
//
// A predicate expression is valid if all of the following are true:
//
//   - It results in a boolean.
//   - It refers only to columns in the table.
//   - It does not include subqueries.
//   - It does not include non-immutable, aggregate, window, or set returning
//     functions.
//
func (v *IndexPredicateValidator) Validate(e tree.Expr) (string, error) {
	expr, _, err := DequalifyAndValidateExpr(
		v.ctx,
		v.desc,
		e,
		&types.Bool,
		"index predicate",
		v.semaCtx,
		&v.tableName,
	)
	if err != nil {
		return "", err
	}

	return expr, nil
}

// FormatExprForDisplay formats a schema expression string for display by adding
// type annotations and resolving user defined types.
func FormatExprForDisplay(
	desc sqlbase.TableDescriptor, exprStr string, semaCtx *tree.SemaContext,
) (string, error) {
	expr, err := DeserializeExprForFormatting(desc, exprStr, semaCtx)
	if err != nil {
		return "", err
	}
	return tree.SerializeForDisplay(expr), nil
}

// FormatIndexForDisplay formats a column descriptor as a SQL string. It
// converts user defined types in partial index predicate expressions to a
// human-readable form.
//
// If tableName is anonymous then no table name is included in the formatted
// string. For example:
//
//   INDEX i (a) WHERE b > 0
//
// If tableName is not anonymous, then "ON" and the name is included:
//
//   INDEX i ON t (a) WHERE b > 0
//
func FormatIndexForDisplay(
	table sqlbase.TableDescriptor,
	tableName *tree.TableName,
	index *sqlbase.IndexDescriptor,
	semaCtx *tree.SemaContext,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	if index.Unique {
		f.WriteString("UNIQUE ")
	}
	if index.Type == sqlbase.IndexDescriptor_INVERTED {
		f.WriteString("INVERTED ")
	}
	f.WriteString("INDEX ")
	f.FormatNameP(&index.Name)
	if *tableName != sqlbase.AnonymousTable {
		f.WriteString(" ON ")
		f.FormatNode(tableName)
	}
	f.WriteString(" (")
	index.ColNamesFormat(f)
	f.WriteByte(')')

	if len(index.StoreColumnNames) > 0 {
		f.WriteString(" STORING (")
		for i := range index.StoreColumnNames {
			if i > 0 {
				f.WriteString(", ")
			}
			f.FormatNameP(&index.StoreColumnNames[i])
		}
		f.WriteByte(')')
	}

	if index.IsPartial() {
		f.WriteString(" WHERE ")
		pred, err := FormatExprForDisplay(table, index.PredExpr, semaCtx)
		if err != nil {
			return "", err
		}
		f.WriteString(pred)
	}

	return f.CloseAndGetString(), nil
}

// ExtractColumnIDs returns the set of column IDs within the given expression.
func ExtractColumnIDs(desc sqlbase.TableDescriptor, rootExpr tree.Expr) (opt.ColSet, error) {
	var colIDs opt.ColSet

	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			return nil, true, expr
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err, false, nil
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return nil, true, expr
		}

		col, _, err := desc.FindColumnByName(c.ColumnName)
		if err != nil {
			return err, false, nil
		}

		colIDs.Add(int(col.ID))
		return nil, false, expr
	})

	return colIDs, err
}

// replaceColumnVars replaces the occurrences of column names in an expression with
// dummyColumns containing their type, so that they may be type-checked. It
// returns this new expression tree alongside a set containing the ColumnID of
// each column seen in the expression.
//
// If the expression references a column that does not exist in the table
// descriptor, replaceColumnVars errs with pgcode.UndefinedColumn.
func replaceColumnVars(
	desc sqlbase.TableDescriptor, rootExpr tree.Expr,
) (tree.Expr, opt.ColSet, error) {
	var colIDs opt.ColSet

	newExpr, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return nil, true, expr
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err, false, nil
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return nil, true, expr
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return pgerror.Newf(pgcode.UndefinedColumn,
					"column %q does not exist, referenced in %q", c.ColumnName, rootExpr.String()),
				false, nil
		}
		colIDs.Add(int(col.ID))

		// Convert to a dummyColumn of the correct type.
		return nil, false, &dummyColumn{typ: col.Type.ToDatumType(), name: c.ColumnName}
	})

	return newExpr, colIDs, err
}

// DequalifyAndValidateExpr validates that an expression has the given type
// and contains no functions with a volatility greater than maxVolatility. The
// type-checked and constant-folded expression and the set of column IDs within
// the expression are returned, if valid.
//
// The serialized expression is returned because returning the created
// tree.TypedExpr would be dangerous. It contains dummyColumns which do not
// support evaluation and are not useful outside the context of type-checking
// the expression.
func DequalifyAndValidateExpr(
	ctx context.Context,
	desc sqlbase.TableDescriptor,
	expr tree.Expr,
	typ *types.T,
	op string,
	semaCtx *tree.SemaContext,
	tn *tree.TableName,
) (string, opt.ColSet, error) {
	var colIDs opt.ColSet
	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		*tn, sqlbase.ResultColumnsFromColDescs(
			desc.AllNonDropColumns(),
		),
	)
	sources := sqlbase.MultiSourceInfo{sourceInfo}
	expr, err := DequalifyColumnRefs(ctx, sources, expr)
	if err != nil {
		return "", colIDs, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, colIDs, err := replaceColumnVars(desc, expr)
	if err != nil {
		return "", colIDs, err
	}

	typedExpr, err := sqlbase.SanitizeVarFreeExpr(
		replacedExpr,
		*typ,
		op,
		semaCtx,
		false,
		true,
	)

	if err != nil {
		return "", colIDs, err
	}

	return tree.Serialize(typedExpr), colIDs, nil
}

//DeserializeExprForFormatting is for DeserializeExpr
func DeserializeExprForFormatting(
	desc sqlbase.TableDescriptor, exprStr string, semaCtx *tree.SemaContext,
) (tree.Expr, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return nil, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	expr, _, err = replaceColumnVars(desc, expr)
	if err != nil {
		return nil, err
	}

	// Type-check the expression to resolve user defined types.
	typedExpr, err := expr.TypeCheck(semaCtx, types.Any, false)
	if err != nil {
		return nil, err
	}

	return typedExpr, nil
}

// dummyColumn represents a variable column that can type-checked. It is used
// in validating check constraint and partial index predicate expressions. This
// validation requires that the expression can be both both typed-checked and
// examined for variable expressions.
type dummyColumn struct {
	typ  types.T
	name tree.Name
}

// String implements the Stringer interface.
func (d *dummyColumn) String() string {
	return tree.AsString(d)
}

// Format implements the NodeFormatter interface.
func (d *dummyColumn) Format(ctx *tree.FmtCtx) {
	d.name.Format(ctx)
}

// Walk implements the Expr interface.
func (d *dummyColumn) Walk(_ tree.Visitor) tree.Expr {
	return d
}

// TypeCheck implements the Expr interface.
func (d *dummyColumn) TypeCheck(
	ctx *tree.SemaContext, desired types.T, useOrigin bool,
) (tree.TypedExpr, error) {
	return d, nil
}

// Eval implements the TypedExpr interface.
func (*dummyColumn) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("dummyColumnItem.Eval() is undefined")
}

// ResolvedType implements the TypedExpr interface.
func (d *dummyColumn) ResolvedType() types.T {
	return d.typ
}

//DequalifyColumnRefs is Dequalify Columns
func DequalifyColumnRefs(
	ctx context.Context, sources sqlbase.MultiSourceInfo, expr tree.Expr,
) (tree.Expr, error) {
	resolver := sqlbase.ColumnResolver{Sources: sources}
	return tree.SimpleVisit(
		expr,
		func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
			if vBase, ok := expr.(tree.VarName); ok {
				v, err := vBase.NormalizeVarName()
				if err != nil {
					return err, false, nil
				}
				if c, ok := v.(*tree.ColumnItem); ok {
					_, err := c.Resolve(ctx, &resolver)
					if err != nil {
						return err, false, nil
					}
					srcIdx := resolver.ResolverState.SrcIdx
					colIdx := resolver.ResolverState.ColIdx
					col := sources[srcIdx].SourceColumns[colIdx]
					return nil, false, &tree.ColumnItem{ColumnName: tree.Name(col.Name)}
				}
			}
			return nil, true, expr
		},
	)
}

// PartialIndexUpdateHelper keeps track of partial indexes that should be not
// be updated during a mutation. When a newly inserted or updated row does not
// satisfy a partial index predicate, it should not be added to the index.
// Likewise, deleting a row from a partial index should not be attempted during
// an update or a delete when the row did not already exist in the partial
// index.
type PartialIndexUpdateHelper struct {
	// IgnoreForPut is a set of index IDs to ignore for Put operations.
	IgnoreForPut util.FastIntSet

	// IgnoreForDel is a set of index IDs to ignore for Del operations.
	IgnoreForDel util.FastIntSet
}

// Init initializes a PartialIndexUpdateHelper to track partial index IDs that
// should be ignored for Put and Del operations. The partialIndexPutVals and
// partialIndexDelVals arguments must be lists of boolean expressions where the
// i-th element corresponds to the i-th partial index defined on the table. If
// the expression evaluates to false, the index should be ignored.
//
// For example, partialIndexPutVals[2] evaluating to true indicates that the
// second partial index of the table should not be ignored for Put operations.
// Meanwhile, partialIndexPutVals[3] evaluating to false indicates that the
// third partial index should be ignored.
func (pm *PartialIndexUpdateHelper) Init(
	partialIndexPutVals tree.Datums,
	partialIndexDelVals tree.Datums,
	tabDesc *sqlbase.ImmutableTableDescriptor,
) error {
	colIdx := 0
	partialIndexOrds := tabDesc.PartialIndexOrds()
	indexes := tabDesc.DeletableIndexes()

	for i, ok := partialIndexOrds.Next(0); ok; i, ok = partialIndexOrds.Next(i + 1) {
		index := &indexes[i]
		if index.IsPartial() {

			// Check the boolean partial index put column, if it exists.
			if colIdx < len(partialIndexPutVals) {
				val, err := tree.GetBool(partialIndexPutVals[colIdx])
				if err != nil {
					return err
				}
				if !val {
					// If the value of the column for the index predicate
					// expression is false, the row should not be added to the
					// partial index.
					pm.IgnoreForPut.Add(int(index.ID))
				}
			}

			// Check the boolean partial index del column, if it exists.
			if colIdx < len(partialIndexDelVals) {
				val, err := tree.GetBool(partialIndexDelVals[colIdx])
				if err != nil {
					return err
				}
				if !val {
					// If the value of the column for the index predicate
					// expression is false, the row should not be removed from
					// the partial index.
					pm.IgnoreForDel.Add(int(index.ID))
				}
			}

			colIdx++
		}
	}

	return nil
}

// RenameColumn replaces any occurrence of the column from in expr with to, and
// returns a string representation of the new expression.
func RenameColumn(expr string, from tree.Name, to tree.Name) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	replaceFn := func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return err, false, nil
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				if string(c.ColumnName) == string(from) {
					c.ColumnName = to
				}
			}
			return nil, false, v
		}
		return nil, true, expr
	}

	renamed, err := tree.SimpleVisit(parsed, replaceFn)
	if err != nil {
		return "", err
	}

	return renamed.String(), nil
}
