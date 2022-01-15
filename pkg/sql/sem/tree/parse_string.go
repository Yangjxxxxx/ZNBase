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

package tree

import (
	"strconv"
	"strings"
	"time"

	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

// ParseStringAs reads s as type t. If t is Bytes or String, s is returned
// unchanged. Otherwise s is parsed with the given type's Parse func.
func ParseStringAs(t types.T, s string, evalCtx *EvalContext, useOrigin bool) (Datum, error) {
	var d Datum
	var err error
	switch t {
	case types.Bytes:
		d = NewDBytes(DBytes(s))
	default:
		switch t := t.(type) {
		case types.TArray:
			typ, err := coltypes.DatumTypeToColumnType(t.Typ)
			if err != nil {
				return nil, err
			}
			//因该方法由SQL以及导入导出共同使用，因此目前根据useOrigin进行区分 useOrigin=true 目前表示导入导出模块使用
			if useOrigin {
				if _, ok := typ.(*coltypes.TString); ok {
					d, err = ParseDArrayForLoad(evalCtx, s, typ)
				} else {
					d, err = ParseDArrayFromStringForDump(evalCtx, s, typ)
				}
			} else {
				d, err = ParseDArrayFromString(evalCtx, s, typ)
			}
			if err != nil {
				return nil, err
			}
		case types.TCollatedString:
			d = NewDCollatedString(s, t.Locale, &evalCtx.CollationEnv)
		default:
			d, err = parseStringAs(t, s, evalCtx, useOrigin)
			if d == nil && err == nil {
				return nil, pgerror.NewAssertionErrorf("unknown type %s (%T)", t, t)
			}
		}
	}
	return d, err
}

// ParseDatumStringAs parses s as type t. This function is guaranteed to
// round-trip when printing a Datum with FmtExport.
func ParseDatumStringAs(t types.T, s string, evalCtx *EvalContext, useOrigin bool) (Datum, error) {
	switch t {
	case types.Bytes:
		return ParseDByte(s)
	default:
		return ParseStringAs(t, s, evalCtx, useOrigin)
	}
}

// parseStringAs parses s as type t for simple types. Bytes, arrays, collated
// strings are not handled. nil, nil is returned if t is not a supported type.
func parseStringAs(t types.T, s string, ctx ParseTimeContext, useOrigin bool) (Datum, error) {
	switch t {
	case types.BitArray:
		return ParseDBitArray(s)
	case types.Bool:
		bb, err := ParseDBool(s)
		if err != nil && len(s) != 0 && useOrigin {
			dec, err := ParseDDecimal(s)
			if err != nil {
				return nil, makeParseError(s, types.Bool, pgerror.NewError(pgcode.InvalidTextRepresentation, "invalid bool value"))
			}
			if dec.IsZero() {
				return DBoolFalse, nil
			}
			return DBoolTrue, nil
		}
		return bb, err
	case types.Date:
		return ParseDDate(ctx, s)
	case types.Decimal:
		dd, err := ParseDDecimal(s)
		if err == nil {
			return dd, nil
		}
		if useOrigin {
			return dd, err
		}
		return ParseDDDecimal(s)
		// f, _ := ParseDFloat(s)
		// d, _ := dd.SetFloat64(float64(*f))
		// return &DDecimal{*d}, nil
	case types.Float, types.Float4:
		if useOrigin {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, makeParseError(s, types.Float, err)
			}
			return NewDFloat(DFloat(f)), nil
		}
		return ParseDFloat(s, false, 64)
	case types.INet:
		return ParseDIPAddrFromINetString(s)
	case types.Int, types.Int2, types.Int4:
		return ParseDInt(s, useOrigin)
	case types.Interval:
		return ParseDInterval(s)
	case types.JSON:
		return ParseDJSON(s)
	case types.String:
		return NewDString(s), nil
	case types.Time:
		return ParseDTime(ctx, s)
	case types.Timestamp:
		return ParseDTimestamp(ctx, s, time.Microsecond)
	case types.TimestampTZ:
		return ParseDTimestampTZ(ctx, s, time.Microsecond)
	case types.UUID:
		return ParseDUuidFromString(s)
	case types.TArray{Typ: types.String}:
		return ParseDStringArray(s)
	case types.TArray{Typ: types.Int}:
		return ParseDIntArray(s)
	case types.TArray{Typ: types.Decimal}:
		return ParseDFloatArray(s)
	default:
		if tset, ok := t.(types.TtSet); ok {
			if s == "" {
				return NewDString(s), nil
			}
			input := strings.Split(s, ",")
			sortVal, valid := IsContainSet(tset.Bounds, input)
			if !valid {
				return nil, pgerror.NewErrorf(pgcode.InvalidColumnReference,
					"SET value is invalid, column SET value must be a subset of \"%s\"",
					ErrNameString(strings.Join(tset.Bounds, ",")))
			}
			return NewDString(sortVal), nil
		}
		return nil, nil
	}
}

//IsContainSet Determine the value of type SET
func IsContainSet(items, input []string) (string, bool) {
	var sortVal []string
	sortInput := make(map[string]bool)

	for _, eachInput := range input {
		sortInput[strings.ToLower(eachInput)] = false
	}
	for _, eachItem := range items {
		if _, ok := sortInput[strings.ToLower(eachItem)]; ok {
			sortInput[strings.ToLower(eachItem)] = true
			sortVal = append(sortVal, eachItem)
		}
	}
	for _, exist := range sortInput {
		if exist == false {
			return "", false
		}
	}
	return strings.Join(sortVal, ","), true
}
