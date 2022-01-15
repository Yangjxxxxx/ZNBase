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

package conv

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// FromColumnType1s calls FromType1ToType on each element of cts, returning the
// resulting slice.
func FromColumnType1s(cts []sqlbase.ColumnType) ([]coltypes.T, error) {
	typs := make([]coltypes.T, len(cts))
	for i := range typs {
		typs[i] = FromType1ToType(sqlbase.FromColumnTypeToT(cts[i]))
		if typs[i] == coltypes.Unhandled {
			return nil, errors.Errorf("unsupported type %s", cts[i].String())
		}
	}
	return typs, nil
}

// FromType1ToType returns the T that corresponds to the input ColumnType.
// Note: if you're adding a new type here, add it to
// vecexec.AllSupportedSQLTypes as well.
func FromType1ToType(ct *types1.T) coltypes.T {
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
	case types1.TimestampFamily, types1.TimestampTZFamily:
		return coltypes.Timestamp
	}
	return coltypes.Unhandled
}
