// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"fmt"

	"github.com/lib/pq/oid"
	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// PhysicalTypeColElemToDatum converts an element in a colvec to a datum of
// semtype ct. Note that this function handles nulls as well, so there is no
// need for a separate null check.
func PhysicalTypeColElemToDatum(
	col coldata.Vec, rowIdx uint16, da sqlbase.DatumAlloc, ct *types1.T,
) tree.Datum {
	if col.MaybeHasNulls() {
		if col.Nulls().NullAt(rowIdx) {
			return tree.DNull
		}
	}
	switch ct.Family() {
	case types1.BoolFamily:
		if col.Bool()[rowIdx] {
			return tree.DBoolTrue
		}
		return tree.DBoolFalse
	case types1.IntFamily:
		switch ct.Width() {
		case 16:
			return da.NewDInt(tree.DInt(col.Int16()[rowIdx]))
		case 32:
			return da.NewDInt(tree.DInt(col.Int32()[rowIdx]))
		default:
			return da.NewDInt(tree.DInt(col.Int64()[rowIdx]))
		}
	case types1.FloatFamily:
		return da.NewDFloat(tree.DFloat(col.Float64()[rowIdx]))
	case types1.DecimalFamily:
		return da.NewDDecimal(tree.DDecimal{Decimal: col.Decimal()[rowIdx]})
	case types1.DateFamily:
		return tree.NewDDate(tree.DDate(col.Int64()[rowIdx]))
	case types1.StringFamily:
		b := col.Bytes().Get(int(rowIdx))
		if ct.Oid() == oid.T_name {
			return da.NewDName(tree.DString(string(b)))
		}
		return da.NewDString(tree.DString(string(b)))
	case types1.BytesFamily:
		return da.NewDBytes(tree.DBytes(col.Bytes().Get(int(rowIdx))))
	case types1.OidFamily:
		return da.NewDOid(tree.MakeDOid(tree.DInt(col.Int64()[rowIdx])))
	case types1.UuidFamily:
		id, err := uuid.FromBytes(col.Bytes().Get(int(rowIdx)))
		if err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return da.NewDUuid(tree.DUuid{UUID: id})
	case types1.TimestampFamily:
		return da.NewDTimestamp(tree.DTimestamp{Time: col.Timestamp()[rowIdx]})
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("Unsupported column type %s", ct.String()))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}
