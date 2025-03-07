// Copyright 2015  The Cockroach Authors.
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

package sqlbase

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	expr tree.Expr,
	expectedType types.T,
	context string,
	semaCtx *tree.SemaContext,
	allowImpure bool,
	typeChange bool,
) (tree.TypedExpr, error) {
	if tree.ContainsVars(expr) {
		return nil, pgerror.NewErrorf(pgcode.Syntax,
			"variable sub-expressions are not allowed in %s", context)
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called from another context
	// which uses the properties field.
	defer semaCtx.Properties.Restore(semaCtx.Properties)

	// Ensure that the expression doesn't contain special functions.
	flags := tree.RejectSpecial
	if !allowImpure {
		flags |= tree.RejectImpureFunctions
	}
	semaCtx.Properties.Require(context, flags)

	typedExpr, err := tree.TypeCheck(expr, semaCtx, expectedType, typeChange)
	if err != nil {
		return nil, err
	}

	actualType := typedExpr.ResolvedType()
	if !expectedType.Equivalent(actualType) && typedExpr != tree.DNull {
		// The expression must match the column type exactly unless it is a constant
		// NULL value.
		return nil, fmt.Errorf("expected %s expression to have type %s, but '%s' has type %s",
			context, expectedType, expr, actualType)
	}
	return typedExpr, nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
//
// If the column type *may* be SERIAL (or SERIAL-like), it is the
// caller's responsibility to call sql.processSerialInColumnDef() and
// sql.doCreateSequence() before MakeColumnDefDescs() to remove the
// SERIAL type and replace it with a suitable integer type and default
// expression.
//
// semaCtx can be nil if no default expression is used for the
// column.
//
// The DEFAULT expression is returned in TypedExpr form for analysis (e.g. recording
// sequence dependencies).
func MakeColumnDefDescs(
	d *tree.ColumnTableDef, semaCtx *tree.SemaContext, privileges *PrivilegeDescriptor, parentid ID,
) (*ColumnDescriptor, *IndexDescriptor, tree.TypedExpr, error) {
	if _, ok := d.Type.(*coltypes.TSerial); ok {
		// To the reader of this code: if control arrives here, this means
		// the caller has not suitably called processSerialInColumnDef()
		// prior to calling MakeColumnDefDescs. The dependent sequences
		// must be created, and the SERIAL type eliminated, prior to this
		// point.
		return nil, nil, nil, pgerror.NewError(pgcode.FeatureNotSupported,
			"SERIAL cannot be used in this context")
	}

	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column CHECK constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column REFERENCED constraint")
	}

	col := &ColumnDescriptor{
		Name:       string(d.Name),
		Nullable:   d.Nullable.Nullability != tree.NotNull && !d.PrimaryKey,
		InhCount:   1,
		IsInherits: false,
		Privileges: privileges,
		ParentID:   parentid,
	}
	if d.Name == "hashnum" && d.IsHash {
		col.Hidden = true
	}

	// Set Type.SemanticType and Type.Locale.
	colDatumType := coltypes.CastTargetToDatumType(d.Type)
	colTyp, err := DatumTypeToColumnType(colDatumType)
	if err != nil {
		return nil, nil, nil, err
	}

	col.Type, err = PopulateTypeAttrs(colTyp, d.Type)
	if err != nil {
		return nil, nil, nil, err
	}

	var typedExpr tree.TypedExpr
	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		if typedExpr, err = SanitizeVarFreeExpr(
			d.DefaultExpr.Expr, colDatumType, "DEFAULT", semaCtx, true /* allowImpure */, false,
		); err != nil {
			return nil, nil, nil, err
		}
		if enumVal, ok := d.Type.(*coltypes.TEnum); ok {
			if defaultVal, ok := typedExpr.(*tree.DString); ok {
				newVal, valid := IsContain(enumVal.Bounds, string(*defaultVal))
				if !valid {
					return nil, nil, nil, errors.Errorf("Invalid default value for '%s'", d.Name.String())
				}
				typedExpr = tree.NewDString(newVal)
			}
		}
		if setVal, ok := d.Type.(*coltypes.TSet); ok {
			if defaultVal, ok := typedExpr.(*tree.DString); ok && string(*defaultVal) != "" {
				defaultSli := strings.Split(string(*defaultVal), ",")
				newVal, valid := tree.IsContainSet(setVal.Bounds, defaultSli)
				if !valid {
					return nil, nil, nil, errors.Errorf("Invalid default value for '%s'", d.Name.String())
				}
				typedExpr = tree.NewDString(newVal)
			}
		}
		// We keep the type checked expression so that the type annotation
		// gets properly stored.
		d.DefaultExpr.Expr = typedExpr

		s := tree.SerializeWithoutSuffix(d.DefaultExpr.Expr)
		col.DefaultExpr = &s
	}

	if d.IsComputed() {
		s := tree.Serialize(d.Computed.Expr)
		col.ComputeExpr = &s
	}

	var idx *IndexDescriptor
	if d.PrimaryKey && d.IsHash {
		idx = &IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{"hashnum", string(d.Name)},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_ASC},
		}
		if d.UniqueConstraintName != "" {
			idx.Name = string(d.UniqueConstraintName)
		}
	} else if d.PrimaryKey || d.Unique {
		idx = &IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{string(d.Name)},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
		if d.UniqueConstraintName != "" {
			idx.Name = string(d.UniqueConstraintName)
		}
	}

	return col, idx, typedExpr, nil
}

// MakeHashColumnDefDescs specifically construct tableDesc for hash partition table
func MakeHashColumnDefDescs(
	d *tree.ColumnTableDef, semaCtx *tree.SemaContext,
) (*ColumnDescriptor, *IndexDescriptor, tree.TypedExpr, error) {
	if _, ok := d.Type.(*coltypes.TSerial); ok {
		// To the reader of this code: if control arrives here, this means
		// the caller has not suitably called processSerialInColumnDef()
		// prior to calling MakeColumnDefDescs. The dependent sequences
		// must be created, and the SERIAL type eliminated, prior to this
		// point.
		return nil, nil, nil, pgerror.NewError(pgcode.FeatureNotSupported,
			"SERIAL cannot be used in this context")
	}

	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column CHECK constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column REFERENCED constraint")
	}

	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable.Nullability != tree.NotNull && !d.PrimaryKey,
	}
	if d.Name == "hashnum" && d.IsHash {
		col.Hidden = true
	}

	// Set Type.SemanticType and Type.Locale.
	colDatumType := coltypes.CastTargetToDatumType(d.Type)
	colTyp, err := DatumTypeToColumnType(colDatumType)
	if err != nil {
		return nil, nil, nil, err
	}

	col.Type, err = PopulateTypeAttrs(colTyp, d.Type)
	if err != nil {
		return nil, nil, nil, err
	}

	var typedExpr tree.TypedExpr
	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		if typedExpr, err = SanitizeVarFreeExpr(
			d.DefaultExpr.Expr, colDatumType, "DEFAULT", semaCtx, true /* allowImpure */, false,
		); err != nil {
			return nil, nil, nil, err
		}
		// We keep the type checked expression so that the type annotation
		// gets properly stored.
		d.DefaultExpr.Expr = typedExpr

		s := tree.SerializeWithoutSuffix(d.DefaultExpr.Expr)
		col.DefaultExpr = &s
	}

	if d.IsComputed() {
		s := tree.Serialize(d.Computed.Expr)
		col.ComputeExpr = &s
	}

	var idx *IndexDescriptor
	if d.IsHash {
		idx = &IndexDescriptor{
			//ID:               1,
			Name:             "hashpartitionidx",
			Unique:           false,
			ColumnNames:      []string{"hashnum"},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
			//ColumnIDs:        []ColumnID{3, 1},
		}
		if d.UniqueConstraintName != "" {
			idx.Name = string(d.UniqueConstraintName)
		}
	} else if d.PrimaryKey || d.Unique {
		idx = &IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{string(d.Name)},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
		if d.UniqueConstraintName != "" {
			idx.Name = string(d.UniqueConstraintName)
		}
	}
	col.IsHashPartitionCol = true

	return col, idx, typedExpr, nil
}

// MakeFuncArgDefDescs creates the funcArg descriptor for a function arg
//
// If the column type *may* be SERIAL (or SERIAL-like), it is the
// caller's responsibility to call sql.processSerialInColumnDef() and
// sql.doCreateSequence() before MakeColumnDefDescs() to remove the
// SERIAL type and replace it with a suitable integer type and default
// expression.
//
// semaCtx can be nil if no default expression is used for the
// column.
//
// The DEFAULT expression is returned in TypedExpr form for analysis (e.g. recording
// sequence dependencies).
func MakeFuncArgDefDescs(
	d *tree.FunctionParameter, semaCtx *tree.SemaContext,
) (*FuncArgDescriptor, error) {
	typ := d.Typ
	serialTypStr := ""
	if dSerial, ok := d.Typ.(*coltypes.TSerial); ok {
		serialTypStr = strings.ToUpper(dSerial.ColumnType())
		tempTyp := &coltypes.TInt{Width: dSerial.Width}
		switch dSerial.VisibleType {
		case coltypes.VisSERIAL:
			tempTyp.VisibleType = coltypes.VisINT
		case coltypes.VisSERIAL2:
			tempTyp.VisibleType = coltypes.VisINT2
		case coltypes.VisSERIAL4:
			tempTyp.VisibleType = coltypes.VisINT4
		case coltypes.VisSERIAL8:
			tempTyp.VisibleType = coltypes.VisINT8
		case coltypes.VisSMALLSERIAL:
			tempTyp.VisibleType = coltypes.VisSMALLINT
		case coltypes.VisBIGSERIAL:
			tempTyp.VisibleType = coltypes.VisBIGINT
		}
		typ = tempTyp
		// return nil, pgerror.NewError(pgcode.FeatureNotSupported,
		// 	"SERIAL cannot be used in this context")
	}

	arg := &FuncArgDescriptor{
		Name: d.Name,
	}

	// Set Type.SemanticType and Type.Locale.
	argDatumType := coltypes.CastTargetToDatumType(typ)
	argTyp, err := DatumTypeToColumnType(argDatumType)
	if err != nil {
		return nil, err
	}

	arg.Type, err = PopulateTypeAttrs(argTyp, typ)
	if err != nil {
		return nil, err
	}

	// get type oid and store in arg.Oid
	dataumTypStr := strings.ToUpper(typ.TypeName())
	typStr := strings.ToUpper(typ.ColumnType())

	if typeOid, ok := tree.TypeNameGetOID[dataumTypStr]; ok {
		// 特殊处理几种无法区分的类型
		if _, okk := typ.(*coltypes.TString); okk {
			typStr = typ.ColumnType()
			arg.Oid = oid.T_text
		}
		arg.Oid = typeOid
	} else {
		strErr := fmt.Sprintf("%s is not an invalid type.", typStr)
		return nil, errors.New(strErr)
	}
	arg.ColumnTypeString = typStr
	if len(serialTypStr) > 0 {
		arg.ColumnTypeString = serialTypStr
	}

	if d.DefExpr != nil {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported,
			"currently plsql does not support default value in function/procedure inout parameters.")
		//s := tree.Serialize(d.DefExpr)
		//arg.ComputeExpr = s
	}
	switch d.Mode {
	case tree.FuncParamIN:
		arg.InOutMode = "in"
	case tree.FuncParamOUT:
		arg.InOutMode = "out"
	case tree.FuncParamINOUT:
		arg.InOutMode = "inout"
	case tree.FuncParamTable:
		arg.InOutMode = "table"
	case tree.FuncParamVariadic:
		arg.InOutMode = "variadic"
	}

	return arg, nil
}

// EncodeColumns is a version of EncodePartialIndexKey that takes ColumnIDs and
// directions explicitly. WARNING: unlike EncodePartialIndexKey, EncodeColumns
// appends directly to keyPrefix.
func EncodeColumns(
	columnIDs []ColumnID,
	directions directions,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	for colIdx, id := range columnIDs {
		val := findColumnValue(id, colMap, values)
		if val == tree.DNull {
			containsNull = true
		}

		dir, err := directions.get(colIdx)
		if err != nil {
			return nil, containsNull, err
		}

		if key, err = EncodeTableKey(key, val, dir); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
}

// EncodeIndexColumns is a version of EncodePartialIndexKey that takes ColumnIDs and
// directions explicitly. WARNING: unlike EncodePartialIndexKey, EncodeColumns
// appends directly to keyPrefix.
func EncodeIndexColumns(
	columnIDs []ColumnID,
	desc *TableDescriptor,
	index *IndexDescriptor,
	directions directions,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	for colIdx, id := range columnIDs {
		val := findIndexColumnValue(desc, index, id, colMap, values)
		if val == tree.DNull {
			containsNull = true
		}

		dir, err := directions.get(colIdx)
		if err != nil {
			return nil, containsNull, err
		}

		if key, err = EncodeTableKey(key, val, dir); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
}

// GetColumnTypes returns the types of the columns with the given IDs.
func GetColumnTypes(desc *TableDescriptor, columnIDs []ColumnID) ([]ColumnType, error) {
	types := make([]ColumnType, len(columnIDs))
	for i, id := range columnIDs {
		col, err := desc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}
		types[i] = col.Type
	}
	return types, nil
}

// GetIndexColumnTypes returns the types of the columns with the given IDs.
func GetIndexColumnTypes(
	desc *TableDescriptor, columnIDs []ColumnID, index *IndexDescriptor,
) ([]ColumnType, error) {
	columntype := make([]ColumnType, len(columnIDs))
	for i, id := range columnIDs {
		if int(id) > len(desc.Columns) {
			for i, column := range index.ColumnIDs {
				if index.IsFunc != nil {
					if column == id && index.IsFunc[i] {
						expr, err := parser.ParseExpr(index.ColumnNames[i])
						if err != nil {
							return nil, err
						}
						iv := &descContainer{desc.Columns, false}
						ivarHelper := tree.MakeIndexedVarHelper(iv, len(desc.Columns))

						sources := []*DataSourceInfo{NewSourceInfoForSingleTable(
							*tree.NewUnqualifiedTableName(tree.Name(desc.Name)), ResultColumnsFromColDescs(desc.Columns),
						)}

						semaCtx := tree.MakeSemaContext()
						semaCtx.IVarContainer = iv

						newexprs, _, _, err := ResolveNames(expr,
							MakeMultiSourceInfo(sources...),
							ivarHelper, sessiondata.SearchPath{})
						if err != nil {
							return nil, err
						}
						typedExpr, err := tree.TypeCheck(newexprs, &semaCtx, types.Any, false)
						if err != nil {
							return nil, err
						}
						switch t := typedExpr.(type) {
						case *tree.FuncExpr:
							columntype[i] = TypeConversion(t.Typ)
						case *tree.IndexedVar:
							columntype[i] = TypeConversion(t.Typ)
						}
						break
					}
				}
				if i == len(index.ColumnIDs)-1 {
					col, err := desc.FindActiveColumnByID(id)
					if err != nil {
						return nil, err
					}
					columntype[i] = col.Type
				}
			}
		} else {
			col, err := desc.FindActiveColumnByID(id)
			if err != nil {
				return nil, err
			}
			columntype[i] = col.Type
		}
	}
	return columntype, nil
}

// ConstraintType is used to identify the type of a constraint.
type ConstraintType string

const (
	// ConstraintTypePK identifies a PRIMARY KEY constraint.
	ConstraintTypePK ConstraintType = "PRIMARY KEY"
	// ConstraintTypeFK identifies a FOREIGN KEY constraint.
	ConstraintTypeFK ConstraintType = "FOREIGN KEY"
	// ConstraintTypeUnique identifies a UNIQUE constraint.
	ConstraintTypeUnique ConstraintType = "UNIQUE"
	// ConstraintTypeCheck identifies a CHECK constraint.
	ConstraintTypeCheck ConstraintType = "CHECK"
)

// ConstraintDetail describes a constraint.
type ConstraintDetail struct {
	Kind        ConstraintType
	Columns     []string
	Details     string
	Unvalidated bool

	// Only populated for FK, PK, and Unique Constraints.
	Index *IndexDescriptor

	// Only populated for FK Constraints.
	FK              *ForeignKeyReference
	ReferencedTable *TableDescriptor
	ReferencedIndex *IndexDescriptor

	// Only populated for Check Constraints.
	CheckConstraint *TableDescriptor_CheckConstraint
}

type tableLookupFn func(ID) (*TableDescriptor, error)

// GetConstraintInfo returns a summary of all constraints on the table.
func (desc *TableDescriptor) GetConstraintInfo(
	ctx context.Context, txn *client.Txn,
) (map[string]ConstraintDetail, error) {
	var tableLookup tableLookupFn
	if txn != nil {
		tableLookup = func(id ID) (*TableDescriptor, error) {
			return GetTableDescFromID(ctx, txn, id)
		}
	}
	return desc.collectConstraintInfo(tableLookup)
}

// GetConstraintInfoWithLookup returns a summary of all constraints on the
// table using the provided function to fetch a TableDescriptor from an ID.
func (desc *TableDescriptor) GetConstraintInfoWithLookup(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	return desc.collectConstraintInfo(tableLookup)
}

// CheckUniqueConstraints returns a non-nil error if a descriptor contains two
// constraints with the same name.
func (desc *TableDescriptor) CheckUniqueConstraints() error {
	_, err := desc.collectConstraintInfo(nil)
	return err
}

// if `tableLookup` is non-nil, provide a full summary of constraints, otherwise just
// check that constraints have unique names.
func (desc *TableDescriptor) collectConstraintInfo(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	info := make(map[string]ConstraintDetail)

	// Indexes provide PK, Unique and FK constraints.
	indexes := desc.AllNonDropIndexes()
	for _, index := range indexes {
		if index.ID == desc.PrimaryIndex.ID {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			colHiddenMap := make(map[ColumnID]bool, len(desc.Columns))
			for i, column := range desc.Columns {
				colHiddenMap[column.ID] = desc.Columns[i].Hidden
			}
			// Don't include constraints against only hidden columns.
			// This prevents the auto-created rowid primary key index from showing up
			// in show constraints.
			hidden := true
			for _, id := range index.ColumnIDs {
				if !colHiddenMap[id] {
					hidden = false
					break
				}
			}
			if hidden {
				continue
			}
			detail := ConstraintDetail{Kind: ConstraintTypePK}
			detail.Columns = index.ColumnNames
			detail.Index = index
			info[index.Name] = detail
		} else if index.Unique {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			detail := ConstraintDetail{Kind: ConstraintTypeUnique}
			detail.Columns = index.ColumnNames
			detail.Index = index
			info[index.Name] = detail
		}
		if index.ForeignKey.IsSet() {
			if v, ok := info[index.ForeignKey.Name]; ok {
				if reflect.DeepEqual(v.Columns, index.ColumnNames) {
					continue
				}
				return nil, errors.Errorf("duplicate constraint name: %q", index.ForeignKey.Name)
			}
			detail := ConstraintDetail{Kind: ConstraintTypeFK}
			detail.Unvalidated = index.ForeignKey.Validity == ConstraintValidity_Unvalidated
			numCols := len(index.ColumnIDs)
			if index.ForeignKey.SharedPrefixLen > 0 {
				numCols = int(index.ForeignKey.SharedPrefixLen)
			}
			detail.Columns = index.ColumnNames[:numCols]
			detail.Index = index
			detail.FK = &index.ForeignKey

			if tableLookup != nil {
				other, err := tableLookup(index.ForeignKey.Table)
				if err != nil {
					return nil, errors.Wrapf(err, "error resolving table %d referenced in foreign key",
						index.ForeignKey.Table)
				}
				otherIdx, err := other.FindIndexByID(index.ForeignKey.Index)
				if err != nil {
					return nil, errors.Wrapf(err, "error resolving index %d in table %s referenced "+
						"in foreign key", index.ForeignKey.Index, other.Name)
				}
				detail.Details = fmt.Sprintf("%s.%v", other.Name, otherIdx.ColumnNames)
				detail.ReferencedTable = other
				detail.ReferencedIndex = otherIdx
			}
			info[index.ForeignKey.Name] = detail
		}
	}

	for _, c := range desc.AllActiveAndInactiveChecks() {
		if _, ok := info[c.Name]; ok {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate constraint name: %q", c.Name)
		}
		detail := ConstraintDetail{Kind: ConstraintTypeCheck}
		// Constraints in the Validating state are considered Unvalidated for this purpose
		detail.Unvalidated = c.Validity != ConstraintValidity_Validated
		detail.CheckConstraint = c
		detail.Details = c.Expr
		if tableLookup != nil {
			colsUsed, err := c.ColumnsUsed(desc)
			if err != nil {
				return nil, errors.Wrapf(err,
					"error computing columns used in check constraint %q", c.Name)
			}
			for _, colID := range colsUsed {
				col, err := desc.FindColumnByID(colID)
				if err != nil {
					return nil, errors.Wrapf(err,
						"error finding column %d in table %s", log.Safe(colID), desc.Name)
				}
				detail.Columns = append(detail.Columns, col.Name)
			}
		}
		info[c.Name] = detail
	}
	return info, nil
}

//TypeConversion conversion type.T to ColumnType
func TypeConversion(oldtype types.T) ColumnType {
	newType := ColumnType{
		VisibleTypeName: oldtype.String(),
	}
	switch newType.VisibleTypeName {
	case "bool":
		newType.SemanticType = ColumnType_BOOL
	case "int":
		newType.SemanticType = ColumnType_INT
	case "float":
		newType.SemanticType = ColumnType_FLOAT
	case "decimal":
		newType.SemanticType = ColumnType_DECIMAL
	case "data":
		newType.SemanticType = ColumnType_DATE
	case "timestamp":
		newType.SemanticType = ColumnType_TIMESTAMP
	case "interval":
		newType.SemanticType = ColumnType_INTERVAL
	case "string":
		newType.SemanticType = ColumnType_STRING
	case "bytes":
		newType.SemanticType = ColumnType_BYTES
	case "timestampz":
		newType.SemanticType = ColumnType_TIMESTAMPTZ
	case "collatedstring":
		newType.SemanticType = ColumnType_COLLATEDSTRING
		newType.SemanticType = ColumnType_NAME
	case "oid":
		newType.SemanticType = ColumnType_OID
	case "null":
		newType.SemanticType = ColumnType_NULL
	case "uuid":
		newType.SemanticType = ColumnType_UUID
	case "array":
		newType.SemanticType = ColumnType_ARRAY
	case "inet":
		newType.SemanticType = ColumnType_INET
	case "time":
		newType.SemanticType = ColumnType_TIME
	case "jsonb":
		newType.SemanticType = ColumnType_JSONB
	case "tuple":
		newType.SemanticType = ColumnType_TUPLE
	case "bit":
		newType.SemanticType = ColumnType_BIT
	case "int2vector":
		newType.SemanticType = ColumnType_INT2VECTOR
	case "oidvector":
		newType.SemanticType = ColumnType_OIDVECTOR
	}
	return newType
}
