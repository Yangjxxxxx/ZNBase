// Copyright 2019  The Cockroach Authors.

package coldata

import (
	"time"

	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
)

// unknown is a Vec that represents an unhandled type. Used when a batch needs a placeholder Vec.
type unknown struct{}

var _ Vec = &unknown{}

func (u unknown) Type() coltypes.T {
	return coltypes.Unhandled
}

func (u unknown) Bool() []bool {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Int16() []int16 {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Int32() []int32 {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Int64() []int64 {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Float64() []float64 {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Bytes() *Bytes {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Decimal() []apd.Decimal {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Timestamp() []time.Time {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Col() interface{} {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) SetCol(interface{}) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) TemplateType() []interface{} {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Append(SliceArgs) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Copy(CopySliceArgs) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Window(colType coltypes.T, start uint64, end uint64) Vec {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) PrettyValueAt(idx uint16, colType coltypes.T) string {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) MaybeHasNulls() bool {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Nulls() *Nulls {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) SetNulls(*Nulls) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Length() int {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) SetLength(int) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Capacity() int {
	return 0
}
