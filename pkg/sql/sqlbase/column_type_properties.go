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

package sqlbase

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/apd"
	coltypes2 "github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/util/encoding"
)

// This file provides facilities to support the correspondence
// between:
//
// - types.T, also called "datum types", for in-memory representations
//   via tree.Datum.
//
// - coltypes.T, also called "cast target types", which are specified
//   types in CREATE TABLE, ALTER TABLE and the CAST/:: syntax.
//
// - ColumnType, used in table descriptors.
//
// - the string representations thereof, for use in SHOW CREATE,
//   information_schema.columns and other introspection facilities.
//
// As a general rule of thumb, we are aiming for a 1-1 mapping between
// coltypes.T and ColumnType. Eventually we should even consider having
// just one implementation for both.
//
// Some notional complexity arises from the fact there are fewer
// different types.T than different coltypes.T/ColumnTypes. This is
// because some distinctions which are important at the time of data
// persistence (or casts) are not useful to keep for in-flight values;
// for example the final required precision for DECIMAL values.
//

// DatumTypeToColumnType converts a types.T (datum type) to a
// ColumnType.
//
// When working from a coltypes.T (i.e. a type in CREATE/ALTER TABLE)
// this must be used in combination with PopulateTypeAttrs() below.
// For example:
//
//	coltyp := <coltypes.T>
//	colDatumType := coltypes.CastTargetToDatumType(coltyp)
//	columnTyp, _ := DatumTypeToColumnType(colDatumType)
//	columnTyp, _ = PopulateTypeAttrs(columnTyp, coltyp)
//
func DatumTypeToColumnType(ptyp types.T) (ColumnType, error) {
	var ctyp ColumnType
	switch t := ptyp.(type) {
	case types.TCollatedString:
		ctyp.SemanticType = ColumnType_COLLATEDSTRING
		ctyp.Locale = &t.Locale
	case types.TArray:
		ctyp.SemanticType = ColumnType_ARRAY
		contents, err := datumTypeToColumnSemanticType(t.Typ)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.ArrayContents = &contents
		if t.Typ.FamilyEqual(types.FamCollatedString) {
			cs := t.Typ.(types.TCollatedString)
			ctyp.Locale = &cs.Locale
		}
	case types.TEnum:
		ctyp.SemanticType = ColumnType_STRING
		contents, err := datumTypeToColumnSemanticType(t.Typ)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.ArrayContents = &contents
		if t.Typ.FamilyEqual(types.FamCollatedString) {
			cs := t.Typ.(types.TCollatedString)
			ctyp.Locale = &cs.Locale
		}
	case types.TtSet:
		ctyp.SemanticType = ColumnType_SET
		ctyp.SetContents = t.Bounds
	case types.TTuple:
		ctyp.SemanticType = ColumnType_TUPLE
		ctyp.TupleContents = make([]ColumnType, len(t.Types))
		for i, tc := range t.Types {
			var err error
			ctyp.TupleContents[i], err = DatumTypeToColumnType(tc)
			if err != nil {
				return ColumnType{}, err
			}
		}
		ctyp.TupleLabels = t.Labels
		return ctyp, nil
	default:
		semanticType, err := datumTypeToColumnSemanticType(ptyp)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.SemanticType = semanticType
		if semanticType == ColumnType_FLOAT {
			ctyp.Precision = 32
		}
		if ptyp.Oid() == oid.T_int4 {
			ctyp.Width = 32
		} else if ptyp.Oid() == oid.T_int8 {
			ctyp.Width = 64
		} else if ptyp.Oid() == oid.T_int2 {
			ctyp.Width = 16
		}
	}
	return ctyp, nil
}

// PopulateTypeAttrs set other attributes of the ColumnType from a
// coltypes.T and performs type-specific verifications.
//
// This must be used on ColumnTypes produced from
// DatumTypeToColumnType if the origin of the type was a coltypes.T
// (e.g. via CastTargetToDatumType).
func PopulateTypeAttrs(base ColumnType, typ coltypes.T) (ColumnType, error) {
	switch t := typ.(type) {
	case *coltypes.TBitArray:
		if t.Width > math.MaxInt32 {
			return ColumnType{}, fmt.Errorf("bit width too large: %d", t.Width)
		}
		base.Width = int32(t.Width)
		if t.Variable {
			base.VisibleType = ColumnType_VARBIT
		}

	case *coltypes.TInt:
		// Ensure that "naked" INT types are promoted to INT8 to preserve
		// compatibility with previous versions.
		if t.Width == 0 {
			base.Width = 64
		} else {
			base.Width = int32(t.Width)
		}

		// For 2.1 nodes only Width is sufficient, but we also populate
		// VisibleType for compatibility with pre-2.1 nodes.
		switch t.Width {
		case 16:
			base.VisibleType = ColumnType_SMALLINT
		case 64:
			base.VisibleType = ColumnType_BIGINT
		case 32:
			base.VisibleType = ColumnType_INTEGER
		}

	case *coltypes.TFloat:
		base.VisibleType = ColumnType_NONE
		base.Precision = 32
		if t.Short {
			base.VisibleType = ColumnType_REAL
			base.Precision = 16
		}

	case *coltypes.TDecimal:
		base.Width = int32(t.Scale)
		base.Precision = int32(t.Prec)
		switch {
		case base.Precision == 0 && base.Width > 0:
			// TODO (seif): Find right range for error message.
			return ColumnType{}, errors.New("invalid NUMERIC precision 0")
		case base.Precision < base.Width:
			return ColumnType{}, fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				base.Width, base.Precision)
		}

	case *coltypes.TString:
		base.Width = int32(t.N)
		base.VisibleType = coltypeStringVariantToVisibleType(t.Variant)

	case *coltypes.TCollatedString:
		base.Width = int32(t.N)
		base.VisibleType = coltypeStringVariantToVisibleType(t.Variant)

	case *coltypes.TArray:
		base.ArrayDimensions = t.Bounds
		var err error
		base, err = PopulateTypeAttrs(base, t.ParamType)
		if err != nil {
			return ColumnType{}, err
		}
	case *coltypes.TEnum:
		base.EnumContents = t.Bounds
		var err error
		base, err = PopulateTypeAttrs(base, t.ParamType)
		if err != nil {
			return ColumnType{}, err
		}
		for i, val1 := range base.EnumContents {
			for _, val2 := range base.EnumContents[i+1:] {
				if strings.ToUpper(val1) == strings.ToUpper(val2) {
					return ColumnType{}, fmt.Errorf("Column has duplicated value '%s' in ENUM", val1)
				}
			}
		}
	case *coltypes.TSet:
		base.SetContents = t.Bounds
		for i, val1 := range base.SetContents {
			for _, val2 := range base.SetContents[i+1:] {
				if strings.ToUpper(val1) == strings.ToUpper(val2) {
					return ColumnType{}, fmt.Errorf("Column has duplicated value '%s' in SET", val1)
				}
			}
		}
		base.SemanticType = ColumnType_SET

	case *coltypes.TVector:
		switch t.ParamType.(type) {
		case *coltypes.TInt, *coltypes.TOid:
		default:
			return ColumnType{}, errors.Errorf("vectors of type %s are unsupported", t.ParamType)
		}

	case *coltypes.TBool:
	case *coltypes.TBytes:
	case *coltypes.TDate:
	case *coltypes.TIPAddr:
	case *coltypes.TInterval:
	case *coltypes.TJSON:
	case *coltypes.TName:
	case *coltypes.TOid:
	case *coltypes.TTime:
	case *coltypes.TTimestamp:
		base.Precision = int32(t.Precision)
		if t.PrecisionSet {
			base.VisibleType = ColumnType_TIMESTAMPP
		}

	case *coltypes.TTimestampTZ:
		base.Precision = int32(t.Precision)
		if t.PrecisionSet {
			base.VisibleType = ColumnType_TIMESTAMPP
		}

	case *coltypes.TUUID:
	case coltypes.TTuple:
	default:
		return ColumnType{}, errors.Errorf("unexpected type %T", t)
	}
	base.VisibleTypeName = typ.ColumnType()
	return base, nil
}

// coltypeStringVariantToVisibleType encodes the visible type of a
// coltypes.TString/TCollatedString variant.
func coltypeStringVariantToVisibleType(c coltypes.TStringVariant) ColumnType_VisibleType {
	switch c {
	case coltypes.TStringVariantVARCHAR:
		return ColumnType_VARCHAR
	case coltypes.TStringVariantCHAR:
		return ColumnType_CHAR
	case coltypes.TStringVariantQCHAR:
		return ColumnType_QCHAR
	default:
		return ColumnType_NONE
	}
}

// stringTypeName returns the visible type name for the given
// STRING/COLLATEDSTRING column type.
func (c *ColumnType) stringTypeName() string {
	typName := "STRING"
	switch c.VisibleType {
	case ColumnType_VARCHAR:
		typName = "VARCHAR"
	case ColumnType_CHAR:
		typName = "CHAR"
	case ColumnType_QCHAR:
		// Yes, that's the name. The ways of PostgreSQL are inscrutable.
		typName = `"char"`
	}
	return typName
}

// SQLString returns the ZNBaseDB native SQL string that can be
// used to reproduce the ColumnType (via parsing -> coltypes.T ->
// CastTargetToColumnType -> PopulateAttrs).
//
// Is is used in error messages and also to produce the output
// of SHOW CREATE.
//
// See also InformationSchemaVisibleType() below.
func (c *ColumnType) SQLString() string {
	if len(c.EnumContents) != 0 {
		enumContents := strings.Join(c.EnumContents, "','")
		return fmt.Sprintf("ENUM('%s')", enumContents)
	}
	if len(c.SetContents) != 0 {
		setContents := strings.Join(c.SetContents, "','")
		return fmt.Sprintf("SET('%s')", setContents)
	}
	if c.VisibleTypeName != "" {
		return c.VisibleTypeName
	}
	switch c.SemanticType {
	case ColumnType_BIT:
		typName := "BIT"
		if c.VisibleType == ColumnType_VARBIT {
			typName = "VARBIT"
		}
		if (c.VisibleType != ColumnType_VARBIT && c.Width > 1) ||
			(c.VisibleType == ColumnType_VARBIT && c.Width > 0) {
			typName = fmt.Sprintf("%s(%d)", typName, c.Width)
		}
		return typName
	case ColumnType_INT:
		// Pre-2.1 BIT was using column type INT with arbitrary width We
		// map this to INT now. See #34161.
		width := c.Width
		if width != 0 && width != 64 && width != 32 && width != 16 {
			width = 64
		}
		if name, ok := coltypes.IntegerTypeNames[int(width)]; ok {
			return name
		}
	case ColumnType_STRING, ColumnType_COLLATEDSTRING:
		typName := c.stringTypeName()
		// In general, if there is a specified width we want to print it next
		// to the type. However, in the specific case of CHAR, the default
		// is 1 and the width should be omitted in that case.
		if c.Width > 0 && !(c.VisibleType == ColumnType_CHAR && c.Width == 1) {
			typName = fmt.Sprintf("%s(%d)", typName, c.Width)
		}
		if c.SemanticType == ColumnType_COLLATEDSTRING {
			if c.Locale == nil {
				panic("locale is required for COLLATEDSTRING")
			}
			typName = fmt.Sprintf("%s COLLATE %s", typName, *c.Locale)
		}
		return typName
	case ColumnType_FLOAT:
		const realName = "FLOAT4"
		const doubleName = "FLOAT8"

		switch c.VisibleType {
		case ColumnType_REAL:
			return realName
		default:
			// NONE now means double precision.
			// Pre-2.1 there were 3 cases:
			// - VisibleType = DOUBLE PRECISION, Width = 0 -> now clearly FLOAT8
			// - VisibleType = NONE, Width = 0 -> now clearly FLOAT8
			// - VisibleType = NONE, Width > 0 -> we need to derive the precision.
			if c.Precision >= 1 && c.Precision <= 24 {
				return realName
			}
			return doubleName
		}
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			if c.Width > 0 {
				return fmt.Sprintf("%s(%d,%d)", c.SemanticType.String(), c.Precision, c.Width)
			}
			return fmt.Sprintf("%s(%d)", c.SemanticType.String(), c.Precision)
		}
	case ColumnType_ARRAY:
		return c.elementColumnType().SQLString() + "[]"
	}
	if c.VisibleType != ColumnType_NONE {
		return c.VisibleType.String()
	}
	return c.SemanticType.String()
}

// InformationSchemaVisibleType returns the string suitable to
// populate the data_type column of information_schema.columns.
//
// This is different from SQLString() in that it must report SQL
// standard names that are compatible with PostgreSQL client
// expectations.
func (c *ColumnType) InformationSchemaVisibleType() string {
	return c.VisibleTypeName
}

// MaxCharacterLength returns the declared maximum length of
// characters if the ColumnType is a character or bit string data
// type. Returns false if the data type is not a character or bit
// string, or if the string's length is not bounded.
//
// This is used to populate information_schema.columns.character_maximum_length;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) MaxCharacterLength() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_STRING, ColumnType_COLLATEDSTRING, ColumnType_BIT:
		if c.Width > 0 {
			return c.Width, true
		}
	}
	return 0, false
}

// MaxOctetLength returns the maximum possible length in
// octets of a datum if the ColumnType is a character string. Returns
// false if the data type is not a character string, or if the
// string's length is not bounded.
//
// This is used to populate information_schema.columns.character_octet_length;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) MaxOctetLength() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_STRING, ColumnType_COLLATEDSTRING:
		if c.Width > 0 {
			return c.Width * utf8.UTFMax, true
		}
	}
	return 0, false
}

// NumericPrecision returns the declared or implicit precision of numeric
// data types. Returns false if the data type is not numeric, or if the precision
// of the numeric type is not bounded.
//
// This is used to populate information_schema.columns.numeric_precision;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) NumericPrecision() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_INT:
		width := c.Width
		// Pre-2.1 BIT was using column type INT with arbitrary
		// widths. Clamp them to fixed/known widths. See #34161.
		if width != 64 && width != 32 && width != 16 {
			width = 64
		}
		return width, true
	case ColumnType_FLOAT:
		_, prec := c.FloatProperties()
		return prec, true
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			return c.Precision, true
		}
	}
	return 0, false
}

// NumericPrecisionRadix returns the implicit precision radix of
// numeric data types. Returns false if the data type is not numeric.
//
// This is used to populate information_schema.columns.numeric_precision_radix;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) NumericPrecisionRadix() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_INT:
		return 2, true
	case ColumnType_FLOAT:
		return 2, true
	case ColumnType_DECIMAL:
		return 10, true
	}
	return 0, false
}

// NumericScale returns the declared or implicit precision of exact numeric
// data types. Returns false if the data type is not an exact numeric, or if the
// scale of the exact numeric type is not bounded.
//
// This is used to populate information_schema.columns.numeric_scale;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) NumericScale() (int32, bool) {
	switch c.SemanticType {
	case ColumnType_INT:
		return 0, true
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			return c.Width, true
		}
	}
	return 0, false
}

// FloatProperties returns the width and precision for a FLOAT column type.
func (c *ColumnType) FloatProperties() (int32, int32) {
	switch c.VisibleType {
	case ColumnType_REAL:
		return 32, 24
	default:
		// NONE now means double precision.
		// Pre-2.1 there were 3 cases:
		// - VisibleType = DOUBLE PRECISION, Width = 0 -> now clearly FLOAT8
		// - VisibleType = NONE, Width = 0 -> now clearly FLOAT8
		// - VisibleType = NONE, Width > 0 -> we need to derive the precision.
		if c.Precision >= 1 && c.Precision <= 24 {
			return 32, 24
		}
		return 64, 53
	}
}

// datumTypeToColumnSemanticType converts a types.T to a SemanticType.
//
// This is mainly used by DatumTypeToColumnType() above; it is also
// used to derive the semantic type of array elements and the
// determination of DatumTypeHasCompositeKeyEncoding().
func datumTypeToColumnSemanticType(ptyp types.T) (ColumnType_SemanticType, error) {
	switch ptyp {
	case types.BitArray:
		return ColumnType_BIT, nil
	case types.Bool:
		return ColumnType_BOOL, nil
	case types.Int, types.Int2, types.Int4:
		return ColumnType_INT, nil
	// case types.Int8:
	// 	return ColumnType_INT, nil
	case types.Float, types.Float4:
		return ColumnType_FLOAT, nil
	case types.Decimal:
		return ColumnType_DECIMAL, nil
	case types.Bytes:
		return ColumnType_BYTES, nil
	case types.String:
		return ColumnType_STRING, nil
	case types.Name:
		return ColumnType_NAME, nil
	case types.Date:
		return ColumnType_DATE, nil
	case types.Time:
		return ColumnType_TIME, nil
	case types.Timestamp:
		return ColumnType_TIMESTAMP, nil
	case types.TimestampTZ:
		return ColumnType_TIMESTAMPTZ, nil
	case types.Interval:
		return ColumnType_INTERVAL, nil
	case types.UUID:
		return ColumnType_UUID, nil
	case types.INet:
		return ColumnType_INET, nil
	case types.Oid, types.RegClass, types.RegNamespace, types.RegProc, types.RegType, types.RegProcedure:
		return ColumnType_OID, nil
	case types.Unknown:
		return ColumnType_NULL, nil
	case types.IntVector:
		return ColumnType_INT2VECTOR, nil
	case types.OidVector:
		return ColumnType_OIDVECTOR, nil
	case types.JSON:
		return ColumnType_JSONB, nil
	case types.Void:
		return ColumnType_STRING, nil
	default:
		if ptyp.FamilyEqual(types.FamCollatedString) {
			return ColumnType_COLLATEDSTRING, nil
		}
		if ptyp.FamilyEqual(types.FamTuple) {
			return ColumnType_TUPLE, nil
		}
		if wrapper, ok := ptyp.(types.TOidWrapper); ok {
			return datumTypeToColumnSemanticType(wrapper.T)
		}
		if _, ok := ptyp.(types.TtSet); ok {
			return ColumnType_SET, nil
		}
		return -1, pgerror.NewErrorf(pgcode.FeatureNotSupported, "unsupported result type: %s, %T, %+v", ptyp, ptyp, ptyp)
	}
}

// columnSemanticTypeToDatumType determines a types.T that can be used
// to instantiate an in-memory representation of values for the given
// column type.
func columnSemanticTypeToDatumType(c *ColumnType, k ColumnType_SemanticType) types.T {
	switch k {
	case ColumnType_BIT:
		return types.BitArray
	case ColumnType_BOOL:
		return types.Bool
	case ColumnType_INT:
		return types.Int
	case ColumnType_FLOAT:
		return types.Float
	case ColumnType_DECIMAL:
		return types.Decimal
	case ColumnType_STRING:
		return types.String
	case ColumnType_BYTES:
		return types.Bytes
	case ColumnType_DATE:
		return types.Date
	case ColumnType_TIME:
		return types.Time
	case ColumnType_TIMESTAMP:
		return types.Timestamp
	case ColumnType_TIMESTAMPTZ:
		return types.TimestampTZ
	case ColumnType_INTERVAL:
		return types.Interval
	case ColumnType_UUID:
		return types.UUID
	case ColumnType_INET:
		return types.INet
	case ColumnType_JSONB:
		return types.JSON
	case ColumnType_TUPLE:
		return types.FamTuple
	case ColumnType_COLLATEDSTRING:
		if c.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		return types.TCollatedString{Locale: *c.Locale}
	case ColumnType_NAME:
		return types.Name
	case ColumnType_OID:
		return types.Oid
	case ColumnType_NULL:
		return types.Unknown
	case ColumnType_INT2VECTOR:
		return types.IntVector
	case ColumnType_OIDVECTOR:
		return types.OidVector
	case ColumnType_SET:
		return types.String
	}
	return nil
}

// ToDatumType converts the ColumnType to a types.T (type of in-memory
// representations). It returns nil if there is no such type.
//
// This is a lossy conversion: some type attributes are not preserved.
func (c *ColumnType) ToDatumType() types.T {
	switch c.SemanticType {
	case ColumnType_ARRAY:
		return types.TArray{Typ: columnSemanticTypeToDatumType(c, *c.ArrayContents)}
	case ColumnType_ENUM:
		return types.TEnum{Typ: columnSemanticTypeToDatumType(c, ColumnType_STRING)}
	case ColumnType_SET:
		return types.TtSet{Typ: types.String, Bounds: c.SetContents}
	case ColumnType_TUPLE:
		datums := types.TTuple{
			Types:  make([]types.T, len(c.TupleContents)),
			Labels: c.TupleLabels,
		}
		for i := range c.TupleContents {
			datums.Types[i] = c.TupleContents[i].ToDatumType()
		}
		return datums
	default:
		return columnSemanticTypeToDatumType(c, c.SemanticType)
	}
}

// ColumnTypesToDatumTypes converts a slice of ColumnTypes to a slice of
// datum types.
func ColumnTypesToDatumTypes(colTypes []ColumnType) []types.T {
	res := make([]types.T, len(colTypes))
	for i, t := range colTypes {
		res[i] = t.ToDatumType()
	}
	return res
}

// IsContain checks whether a item is listed in a slice
func IsContain(items []string, item string) (string, bool) {
	for _, eachItem := range items {
		if strings.ToLower(eachItem) == strings.ToLower(item) {
			return eachItem, true
		}
	}
	return "", false
}

// LimitValueWidth checks that the width (for strings, byte arrays, and bit
// strings) and scale (for decimals) of the value fits the specified column
// type. In case of decimals, it can truncate fractional digits in the input
// value in order to fit the target column. If the input value fits the target
// column, it is returned unchanged. If the input value can be truncated to fit,
// then a truncated copy is returned. Otherwise, an error is returned. This
// method is used by INSERT and UPDATE.
func LimitValueWidth(
	typ ColumnType, inVal tree.Datum, name *string, inUDR bool,
) (outVal tree.Datum, err error) {
	switch typ.SemanticType {
	case ColumnType_STRING, ColumnType_COLLATEDSTRING:
		var sv string
		if v, ok := tree.AsDString(inVal); ok {
			sv = string(v)
		} else if v, ok := inVal.(*tree.DCollatedString); ok {
			sv = v.Contents
		}

		if typ.Width > 0 && utf8.RuneCountInString(sv) > int(typ.Width) {
			return nil, pgerror.NewErrorf(pgcode.StringDataRightTruncation,
				"value too long for type %s (column %q)",
				typ.SQLString(), tree.ErrNameStringP(name))
		}
		if len(typ.EnumContents) != 0 {
			switch inVal.ResolvedType() {
			case types.String:
				input := string(tree.MustBeDString(inVal))
				input = strings.Trim(input, "'")
				sortVal, valid := IsContain(typ.EnumContents, input)
				if !valid {
					return nil, pgerror.NewErrorf(pgcode.InvalidColumnReference,
						"ENUM value not valid, column %q ENUM value can only be one of %s",
						tree.ErrNameStringP(name), typ.EnumContents)
				}
				inVal = tree.NewDString(sortVal)
				//case types.Unknown:
				//default:
			}
		}
	case ColumnType_SET:
		switch inVal.ResolvedType() {
		case types.String:
			inputVal := string(tree.MustBeDString(inVal))
			if inputVal == "" {
				break
			}
			input := strings.Split(inputVal, ",")
			sortVal, valid := tree.IsContainSet(typ.SetContents, input)
			if !valid {
				return nil, pgerror.NewErrorf(pgcode.InvalidColumnReference,
					"SET value is invalid, column SET value must be a subset of \"%s\"",
					tree.ErrNameString(strings.Join(typ.SetContents, ",")))
			}
			inVal = tree.NewDString(sortVal)
		}
	case ColumnType_INT:
		if v, ok := tree.AsDInt(inVal); ok {
			if typ.Width == 32 || typ.Width == 64 || typ.Width == 16 {
				// Width is defined in bits.
				width := uint(typ.Width - 1)

				// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
				shifted := v >> width
				if (v >= 0 && shifted > 0) || (v < 0 && shifted < -1) {
					if inUDR == false {
						return nil, pgerror.NewErrorf(pgcode.NumericValueOutOfRange,
							"integer out of range for type %s (column %q)",
							typ.VisibleTypeName, tree.ErrNameStringP(name))
					}
					return nil, pgerror.NewErrorf(pgcode.NumericValueOutOfRange,
						"integer out of range for type %s (variable %q)",
						typ.VisibleTypeName, tree.ErrNameStringP(name))

				}
			}
		}
	case ColumnType_BIT:
		if v, ok := tree.AsDBitArray(inVal); ok {
			if typ.Width > 0 {
				bitLen := v.BitLen()
				switch typ.VisibleType {
				case ColumnType_VARBIT:
					if bitLen > uint(typ.Width) {
						return nil, pgerror.NewErrorf(pgcode.StringDataRightTruncation,
							"bit string length %d too large for type %s", bitLen, typ.SQLString())
					}
				default:
					if bitLen != uint(typ.Width) {
						//if inUDR {
						//	return inVal, nil
						//}
						return nil, pgerror.NewErrorf(pgcode.StringDataLengthMismatch,
							"bit string length %d does not match type %s", bitLen, typ.SQLString())
					}
				}
			}
		}
	case ColumnType_DECIMAL:
		if inDec, ok := inVal.(*tree.DDecimal); ok {
			if inDec.Form != apd.Finite || typ.Precision == 0 {
				// Non-finite form or unlimited target precision, so no need to limit.
				break
			}
			if int64(typ.Precision) >= inDec.NumDigits() && typ.Width == inDec.Exponent {
				// Precision and scale of target column are sufficient.
				break
			}

			var outDec tree.DDecimal
			outDec.Set(&inDec.Decimal)
			err := tree.LimitDecimalWidth(&outDec.Decimal, int(typ.Precision), int(typ.Width))
			if err != nil {
				return nil, errors.Wrapf(err, "type %s (column %q)",
					typ.SQLString(), tree.ErrNameStringP(name))
			}
			return &outDec, nil
		}
	case ColumnType_ARRAY:
		if inArr, ok := inVal.(*tree.DArray); ok {
			var outArr *tree.DArray
			elementType := *typ.elementColumnType()
			for i, inElem := range inArr.Array {
				outElem, err := LimitValueWidth(elementType, inElem, name, false)
				if err != nil {
					return nil, err
				}
				if outElem != inElem {
					if outArr == nil {
						outArr = &tree.DArray{
							ParamTyp: inArr.ParamTyp,
							Array:    make(tree.Datums, len(inArr.Array)),
							HasNulls: inArr.HasNulls,
						}
						copy(outArr.Array, inArr.Array[:i])
					}
				}
				if outArr != nil {
					outArr.Array[i] = inElem
				}
			}
			if outArr != nil {
				return outArr, nil
			}
		}
	case ColumnType_FLOAT:
		if v, ok := tree.AsDFloat(inVal); ok {
			if typ.Precision >= 1 && typ.Precision <= 24 {
				if (v > math.MaxFloat32 || v < -math.MaxFloat32) && (v != tree.PosInfFloat && v != tree.NegInfFloat && v != tree.NaNFloat) {
					return nil, pgerror.NewErrorf(pgcode.NumericValueOutOfRange,
						"float out of range for type %s (column %q)",
						typ.VisibleTypeName, tree.ErrNameStringP(name))
				}
			}
		}
	case ColumnType_TIMESTAMP:
		if v, ok := tree.AsDTimestamp(inVal); ok {
			//ColumnType_TIMESTAMPP just for type timestamp(p) and timestamptz(p).
			if typ.VisibleType == ColumnType_TIMESTAMPP {
				if typ.Precision >= 0 && typ.Precision < 6 {
					//Truncation occurs when the precision ranges from 0 to 5
					//precision=6 is same as timestamp
					var val time.Time
					preVal := math.Round(float64(v.Nanosecond()) / math.Pow(10, 9-float64(typ.Precision)))
					tempVal := v.Round(time.Second)
					if tempVal.Second() != v.Second() {
						val = tempVal.Add(time.Duration(-1) * time.Second)
					} else {
						val = tempVal
					}
					res := val.Add(time.Nanosecond * time.Duration(preVal*math.Pow(10, 9-float64(typ.Precision))))
					return tree.MakeDTimestamp(res, time.Nanosecond), nil
				}
			}
		}
	case ColumnType_TIMESTAMPTZ:
		if v, ok := tree.AsDTimestampTZ(inVal); ok {
			//ColumnType_TIMESTAMPP just for type timestamp(p) and timestamptz(p).
			if typ.VisibleType == ColumnType_TIMESTAMPP {
				if typ.Precision >= 0 && typ.Precision < 6 {
					//Truncation occurs when the precision ranges from 0 to 5
					//precision=6 is same as timestamptz
					var val time.Time
					preVal := math.Round(float64(v.Nanosecond()) / math.Pow(10, 9-float64(typ.Precision)))
					tempVal := v.Round(time.Second)
					if tempVal.Second() != v.Second() {
						val = tempVal.Add(time.Duration(-1) * time.Second)
					} else {
						val = tempVal
					}
					res := val.Add(time.Nanosecond * time.Duration(preVal*math.Pow(10, 9-float64(typ.Precision))))
					return tree.MakeDTimestampTZ(res, time.Nanosecond), nil
				}
			}
		}

	}
	return inVal, nil
}

// elementColumnType works on a ColumnType with semantic type ARRAY
// and retrieves the ColumnType of the elements of the array.
//
// This is used by LimitValueWidth() and SQLType().
//
// TODO(knz): make this return a bool and avoid a heap allocation.
func (c *ColumnType) elementColumnType() *ColumnType {
	if c.SemanticType != ColumnType_ARRAY {
		return nil
	}
	result := *c
	result.SemanticType = *c.ArrayContents
	result.ArrayContents = nil
	return &result
}

// CheckDatumTypeFitsColumnType verifies that a given scalar value
// type is valid to be stored in a column of the given column type. If
// the scalar value is a placeholder, the type of the placeholder gets
// populated. NULL values are considered to fit every target type.
//
// For the purpose of this analysis, column type aliases are not
// considered to be different (eg. TEXT and VARCHAR will fit the same
// scalar type String).
//
// This is used by the UPDATE, INSERT and UPSERT code.
func CheckDatumTypeFitsColumnType(
	col ColumnDescriptor, typ types.T, pmap *tree.PlaceholderInfo,
) error {
	if typ == types.Unknown {
		return nil
	}
	// If the value is a placeholder, then the column check above has
	// populated 'colTyp' with a type to assign to it.
	colTyp := col.Type.ToDatumType()
	if p, pok := typ.(types.TPlaceholder); pok {
		if err := pmap.SetType(p.Idx, colTyp); err != nil {
			return pgerror.NewErrorf(pgcode.IndeterminateDatatype,
				"cannot infer type for placeholder %s from column %q: %s",
				p.Idx, tree.ErrNameString(col.Name), err)
		}
	} else if !typ.Equivalent(colTyp) {
		castTyp := col.DatumType()
		if castTyp == nil || castTyp == types.Any {
			castTyp = types.TypeStrGetdatumType(col.Type.SQLString())
		}
		if castTyp != nil {
			if ok, c := tree.IsCastDeepValid(typ, castTyp); ok {
				telemetry.Inc(c)
				return nil
			}
		}
		// Not a placeholder; check that the value cast has succeeded.
		return pgerror.NewErrorf(pgcode.DatatypeMismatch,
			"value type %s doesn't match type %s of column %q",
			typ, col.Type.SQLString(), tree.ErrNameString(col.Name))
	}
	return nil
}

// datumType1ToColumnSemanticType converts a types1.T to a SemanticType.
//
// This is mainly used by DatumTypeToColumnType() above; it is also
// used to derive the semantic type of array elements and the
// determination of DatumTypeHasCompositeKeyEncoding().
func datumType1ToColumnSemanticType(ptyp types1.T) (ColumnType_SemanticType, error) {
	switch ptyp.Family() {
	case types1.BitFamily:
		return ColumnType_BIT, nil
	case types1.BoolFamily:
		return ColumnType_BOOL, nil
	case types1.IntFamily:
		return ColumnType_INT, nil
	case types1.FloatFamily:
		return ColumnType_FLOAT, nil
	case types1.DecimalFamily:
		return ColumnType_DECIMAL, nil
	case types1.BytesFamily:
		return ColumnType_BYTES, nil
	case types1.StringFamily:
		return ColumnType_STRING, nil
	case types1.DateFamily:
		return ColumnType_DATE, nil
	case types1.TimeFamily:
		return ColumnType_TIME, nil
	case types1.TimestampTZFamily:
		if ptyp.Oid() == oid.T_timestamp {
			return ColumnType_TIMESTAMP, nil
		}
		return ColumnType_TIMESTAMPTZ, nil
	case types1.IntervalFamily:
		return ColumnType_INTERVAL, nil
	case types1.UuidFamily:
		return ColumnType_UUID, nil
	case types1.INetFamily:
		return ColumnType_INET, nil
	case types1.OidFamily:
		return ColumnType_OID, nil
	case types1.UnknownFamily:
		return ColumnType_NULL, nil
	case types1.ArrayFamily:
		return ColumnType_OIDVECTOR, nil
	case types1.JsonFamily:
		return ColumnType_JSONB, nil
	default:
		if ptyp.Family() == types1.CollatedStringFamily {
			return ColumnType_COLLATEDSTRING, nil
		}
		if ptyp.Family() == types1.TupleFamily {
			return ColumnType_TUPLE, nil
		}
		return -1, pgerror.NewErrorf(pgcode.FeatureNotSupported, "unsupported result type: %s, %T, %+v", ptyp.Name(), ptyp, ptyp)
	}
}

//DatumType1ToColumnType convert types1.T to ColumnType
func DatumType1ToColumnType(ptyp types1.T) (ColumnType, error) {
	var ctyp ColumnType
	switch ptyp.Family() {
	case types1.CollatedStringFamily:
		ctyp.SemanticType = ColumnType_COLLATEDSTRING
		ctyp.Locale = ptyp.InternalType.Locale
	case types1.ArrayFamily:
		ctyp.SemanticType = ColumnType_ARRAY
		contents, err := datumType1ToColumnSemanticType(ptyp)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.ArrayContents = &contents
		if ptyp.Family() == types1.CollatedStringFamily {
			ctyp.Locale = ptyp.InternalType.Locale
		}
	case types1.TupleFamily:
		ctyp.SemanticType = ColumnType_TUPLE
		ctyp.TupleContents = make([]ColumnType, len(ptyp.InternalType.TupleContents))
		for i, tc := range ptyp.InternalType.TupleContents {
			var err error
			ctyp.TupleContents[i], err = DatumType1ToColumnType(tc)
			if err != nil {
				return ColumnType{}, err
			}
		}
		ctyp.TupleLabels = ptyp.InternalType.TupleLabels
		return ctyp, nil
	default:
		semanticType, err := datumType1ToColumnSemanticType(ptyp)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.SemanticType = semanticType
		ctyp.Width = ptyp.Width()
	}
	return ctyp, nil
}

//DatumType1sToColumnTypes convert types1.T to ColumnType
func DatumType1sToColumnTypes(ptyps []types1.T) ([]ColumnType, error) {
	ctyp := make([]ColumnType, len(ptyps))
	for i, ptyp := range ptyps {
		ct, err := DatumType1ToColumnType(ptyp)
		if err != nil {
			return nil, err
		}
		ctyp[i] = ct
	}

	return ctyp, nil
}

//DatumTypesToColumnTypes convert types.T to ColumnType
func DatumTypesToColumnTypes(ptyps []types.T) ([]ColumnType, error) {
	ctyp := make([]ColumnType, len(ptyps))
	for i, ptyp := range ptyps {
		ct, err := DatumTypeToColumnType(ptyp)
		if err != nil {
			return nil, err
		}
		ctyp[i] = ct
	}

	return ctyp, nil
}

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type.
func GetDatumToPhysicalFn(ct ColumnType) func(tree.Datum) (interface{}, error) {
	switch ct.SemanticType {
	case ColumnType_BOOL:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBool)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBool, found %s", reflect.TypeOf(datum))
			}
			return bool(*d), nil
		}
	case ColumnType_BYTES:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBytes)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBytes, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case ColumnType_INT:
		switch ct.Width {
		case 8:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int8(*d), nil
			}
		case 16:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int16(*d), nil
			}
		case 32:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int32(*d), nil
			}
		case 0, 64:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int64(*d), nil
			}
		}
		panic(fmt.Sprintf("unhandled INT width %d", ct.Width))
	case ColumnType_DATE:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDate)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDate, found %s", reflect.TypeOf(datum))
			}
			return int64(*d), nil
		}
	case ColumnType_FLOAT:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DFloat)
			if !ok {
				return nil, errors.Errorf("expected *tree.DFloat, found %s", reflect.TypeOf(datum))
			}
			return float64(*d), nil
		}
	case ColumnType_OID:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DOid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOid, found %s", reflect.TypeOf(datum))
			}
			return int64(d.DInt), nil
		}
	case ColumnType_STRING:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DString, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case ColumnType_NAME:
		return func(datum tree.Datum) (interface{}, error) {
			wrapper, ok := datum.(*tree.DOidWrapper)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOidWrapper, found %s", reflect.TypeOf(datum))
			}
			d, ok := wrapper.Wrapped.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DString, found %s", reflect.TypeOf(wrapper))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case ColumnType_DECIMAL:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDecimal)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDecimal, found %s", reflect.TypeOf(datum))
			}
			return d.Decimal, nil
		}
	}
	panic(fmt.Sprintf("unhandled ColumnType %s", ct.String()))
}

// ToColType returns the T that corresponds to the input ColumnType.
// Note: if you're adding a new type here, add it to
// vecexec.AllSupportedSQLTypes as well.
func ToColType(ct *ColumnType) coltypes2.T {
	switch ct.SemanticType {
	case ColumnType_BOOL:
		return coltypes2.Bool
	case ColumnType_BYTES, ColumnType_STRING, ColumnType_UUID:
		return coltypes2.Bytes
	case ColumnType_DATE, ColumnType_OID:
		return coltypes2.Int64
	case ColumnType_DECIMAL:
		return coltypes2.Decimal
	case ColumnType_INT:
		switch ct.Width {
		case 16:
			return coltypes2.Int16
		case 32:
			return coltypes2.Int32
		case 0, 64:
			return coltypes2.Int64
		}
	case ColumnType_FLOAT:
		return coltypes2.Float64
	case ColumnType_TIMESTAMP:
		return coltypes2.Timestamp
	}
	return coltypes2.Unhandled
}

//ToColTypes convert ColumnType to coltypes2.T
func ToColTypes(cts []ColumnType) []coltypes2.T {
	coltyps := make([]coltypes2.T, len(cts))
	for i := range cts {
		coltyps[i] = ToColType(&cts[i])
	}

	return coltyps
}

//ToColTypeFromType convert types.T to coltypes2.T
func ToColTypeFromType(typ *types.T) coltypes2.T {
	ct, _ := DatumTypeToColumnType(*typ)
	return ToColType(&ct)
}

// ToColtysFormtypes calls FromColumnType on each element of cts, returning the
// resulting slice.
func ToColtysFormtypes(cts []types.T) ([]coltypes2.T, error) {
	typs := make([]coltypes2.T, len(cts))
	for i := range typs {
		typs[i] = ToColTypeFromType(&cts[i])
		if typs[i] == coltypes2.Unhandled {
			return nil, errors.Errorf("unsupported type %s", cts[i].String())
		}
	}
	return typs, nil
}

//ToColTypeFromType1 convert types1.T to coltypes2.T
func ToColTypeFromType1(typ *types1.T) coltypes2.T {
	ct, _ := DatumType1ToColumnType(*typ)
	return ToColType(&ct)
}

// ToColtysFormtype1s calls FromColumnType on each element of cts, returning the
// resulting slice.
func ToColtysFormtype1s(cts []types1.T) ([]coltypes2.T, error) {
	typs := make([]coltypes2.T, len(cts))
	for i := range typs {
		typs[i] = ToColTypeFromType1(&cts[i])
		if typs[i] == coltypes2.Unhandled {
			return nil, errors.Errorf("unsupported type %s", cts[i].String())
		}
	}
	return typs, nil
}
