// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sem

import (
	"fmt"
	"reflect"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	coltypes2 "github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/util/encoding"
)

// FromColumnType returns the T that corresponds to the input ColumnType.
// Note: if you're adding a new type here, add it to
// vecexec.AllSupportedSQLTypes as well.
func FromColumnType(ct *types1.T) coltypes.T {
	switch ct.Family() {
	case types1.BoolFamily:
		return coltypes.Bool
	case types1.BytesFamily, types1.StringFamily, types1.UuidFamily:
		return coltypes.Bytes
	case types1.DateFamily, types1.OidFamily:
		return coltypes.Int64
	case types1.DecimalFamily:
		return coltypes.Decimal
	case types1.IntFamily:
		switch ct.Width() {
		case 16:
			return coltypes.Int16
		case 32:
			return coltypes.Int32
		case 0, 64:
			return coltypes.Int64
		}
		execerror.VectorizedInternalPanic(fmt.Sprintf("integer with unknown width %d", ct.Width()))
	case types1.FloatFamily:
		return coltypes.Float64
	case types1.TimestampFamily:
		return coltypes.Timestamp
	}
	return coltypes.Unhandled
}

// FromColumnTypes calls FromType1ToType on each element of cts, returning the
// resulting slice.
func FromColumnTypes(cts []types1.T) ([]coltypes.T, error) {
	typs := make([]coltypes.T, len(cts))
	for i := range typs {
		typs[i] = FromColumnType(&cts[i])
		if typs[i] == coltypes.Unhandled {
			return nil, errors.Errorf("unsupported type %s", cts[i].String())
		}
	}
	return typs, nil
}

//ToNewType 转换到新的数据类型
func ToNewType(typ types.T) *types1.T {
	switch typ {
	case types.Unknown:
		return types1.Unknown
	case types.Bool:
		return types1.Bool
	case types.BitArray:
		return types1.VarBit
	case types.Int:
		return types1.Int
	case types.Float:
		return types1.Float
	case types.Decimal:
		return types1.Decimal
	case types.String:
		return types1.String
	case types.Bytes:
		return types1.Bytes
	case types.Date:
		return types1.Date
	case types.Time:
		return types1.Time
	case types.Timestamp:
		return types1.Timestamp
	case types.TimestampTZ:
		return types1.TimestampTZ
	case types.Interval:
		return types1.Interval
	case types.JSON:
		return types1.Jsonb
	case types.UUID:
		return types1.UUID
	case types.INet:
		return types1.INet
	case types.AnyArray:
		return types1.AnyArray
	case types.Any:
		return types1.Any
	case types.Name:
		return types1.Name
	case types.Oid:
		return types1.Oid
	}

	return types1.Unknown
}

//ToOldType 转换到旧的数据类型
func ToOldType(typ *types1.T) types.T {
	switch typ.Family() {
	case types1.UnknownFamily:
		return types.Unknown
	case types1.BoolFamily:
		return types.Bool
	case types1.BitFamily:
		return types.BitArray
	case types1.IntFamily, types1.OidFamily:
		return types.Int
	case types1.FloatFamily:
		return types.Float
	case types1.DecimalFamily:
		return types.Decimal
	case types1.StringFamily:
		return types.String
	case types1.BytesFamily:
		return types.Bytes
	case types1.DateFamily:
		return types.Date
	case types1.TimeFamily:
		return types.Time
	case types1.TimestampTZFamily:
		if typ.Oid() == oid.T_timestamp {
			return types.Timestamp
		}
		return types.TimestampTZ
	case types1.IntervalFamily:
		return types.Interval
	case types1.JsonFamily:
		return types.JSON
	case types1.UuidFamily:
		return types.UUID
	case types1.INetFamily:
		return types.INet
	case types1.ArrayFamily:
		return types.AnyArray
	case types1.AnyFamily:
		return types.Any
	}

	return types.Unknown
}

//ToOldTypes return types.T
func ToOldTypes(typs []types1.T) []types.T {
	oldtys := make([]types.T, len(typs))
	for i, typ := range typs {
		oldtys[i] = ToOldType(&typ)
	}
	return oldtys
}

//ToNewTypes return types1.T
func ToNewTypes(typs []types.T) []types1.T {
	newtys := make([]types1.T, len(typs))
	for i, typ := range typs {
		newtys[i] = *ToNewType(typ)
	}
	return newtys
}

// ToColumnType converts a types1.T that corresponds to the column type. Note
// that due to the fact that multiple types1.T's are represented by a single
// column type, this conversion might return the type that is unexpected.
// NOTE: this should only be used in tests.
func ToColumnType(t coltypes.T) *types1.T {
	switch t {
	case coltypes.Bool:
		return types1.Bool
	case coltypes.Bytes:
		return types1.Bytes
	case coltypes.Decimal:
		return types1.Decimal
	case coltypes.Int16:
		return types1.Int2
	case coltypes.Int32:
		return types1.Int4
	case coltypes.Int64:
		return types1.Int
	case coltypes.Float64:
		return types1.Float
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected coltype %s", t.String()))
	return nil
}

// ToColumnTypes calls ToColumnType on each element of typs returning the
// resulting slice.
func ToColumnTypes(typs []coltypes.T) []types1.T {
	cts := make([]types1.T, len(typs))
	for i := range cts {
		t := ToColumnType(typs[i])
		cts[i] = *t
	}
	return cts
}

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type.
func GetDatumToPhysicalFn(ct *types1.T) func(tree.Datum) (interface{}, error) {
	switch ct.Family() {
	case types1.BoolFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBool)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBool, found %s", reflect.TypeOf(datum))
			}
			return bool(*d), nil
		}
	case types1.BytesFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBytes)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBytes, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case types1.IntFamily:
		switch ct.Width() {
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
					c, ok := datum.(*tree.DOid)
					if !ok {
						return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
					}
					return int64(c.DInt), nil
				}
				return int64(*d), nil
			}
		}
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled INT width %d", ct.Width()))
	case types1.DateFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDate)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDate, found %s", reflect.TypeOf(datum))
			}
			return int64(*d), nil
		}
	case types1.FloatFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DFloat)
			if !ok {
				return nil, errors.Errorf("expected *tree.DFloat, found %s", reflect.TypeOf(datum))
			}
			return float64(*d), nil
		}
	case types1.OidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DOid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOid, found %s", reflect.TypeOf(datum))
			}
			return int64(d.DInt), nil
		}
	case types1.StringFamily:
		return func(datum tree.Datum) (interface{}, error) {
			// Handle other STRING-related OID types, like oid.T_name.
			wrapper, ok := datum.(*tree.DOidWrapper)
			if ok {
				datum = wrapper.Wrapped
			}

			d, ok := datum.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DString, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case types1.DecimalFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDecimal)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDecimal, found %s", reflect.TypeOf(datum))
			}
			return d.Decimal, nil
		}
	case types1.UuidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DUuid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DUuid, found %s", reflect.TypeOf(datum))
			}
			return d.UUID.GetBytesMut(), nil
		}
	case types1.TimestampFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTimestamp)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTimestamp, found %s", reflect.TypeOf(datum))
			}
			return d.Time, nil
		}
	}
	// It would probably be more correct to return an error here, rather than a
	// function which always returns an error. But since the function tends to be
	// invoked immediately after GetDatumToPhysicalFn is called, this works just
	// as well and makes the error handling less messy for the caller.
	return func(datum tree.Datum) (interface{}, error) {
		return nil, errors.Errorf("unhandled type %s", ct.DebugString())
	}
}

//CastTypeToType1 return types1.T
func CastTypeToType1(t coltypes2.CastTargetType) *types1.T {
	typ := coltypes2.CastTargetToDatumType(t)
	return ToNewType(typ)
}

//CastTypeToType return types.T
func CastTypeToType(t coltypes2.CastTargetType) *types.T {
	typ := coltypes2.CastTargetToDatumType(t)
	return &typ
}
