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

package tree

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unsafe"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util/bitarray"
	"github.com/znbasedb/znbase/pkg/util/duration"
	"github.com/znbasedb/znbase/pkg/util/ipaddr"
	"github.com/znbasedb/znbase/pkg/util/json"
	"github.com/znbasedb/znbase/pkg/util/stringencoding"
	"github.com/znbasedb/znbase/pkg/util/timeofday"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil/pgdate"
	"github.com/znbasedb/znbase/pkg/util/uint128"
	"github.com/znbasedb/znbase/pkg/util/uuid"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

var (
	constDBoolTrue  DBool = true
	constDBoolFalse DBool = false

	// DBoolTrue is a pointer to the DBool(true) value and can be used in
	// comparisons against Datum types.
	DBoolTrue = &constDBoolTrue
	// DBoolFalse is a pointer to the DBool(false) value and can be used in
	// comparisons against Datum types.
	DBoolFalse = &constDBoolFalse

	// DNull is the NULL Datum.
	DNull Datum = dNull{}

	// DZero is the zero-valued integer Datum.
	DZero = NewDInt(0)
)

// Datum represents a SQL value.
type Datum interface {
	TypedExpr

	// AmbiguousFormat indicates whether the result of formatting this Datum can
	// be interpreted into more than one type. Used with
	// fmtFlags.disambiguateDatumTypes.
	AmbiguousFormat() bool

	// Compare returns -1 if the receiver is less than other, 0 if receiver is
	// equal to other and +1 if receiver is greater than other.
	Compare(ctx *EvalContext, other Datum) int

	// Prev returns the previous datum and true, if one exists, or nil and false.
	// The previous datum satisfies the following definition: if the receiver is
	// "b" and the returned datum is "a", then for every compatible datum "x", it
	// holds that "x < b" is true if and only if "x <= a" is true.
	//
	// The return value is undefined if IsMin(_ *EvalContext) returns true.
	//
	// TODO(#12022): for DTuple, the contract is actually that "x < b" (SQL order,
	// where NULL < x is unknown for all x) is true only if "x <= a"
	// (.Compare/encoding order, where NULL <= x is true for all x) is true. This
	// is okay for now: the returned datum is used only to construct a span, which
	// uses .Compare/encoding order and is guaranteed to be large enough by this
	// weaker contract. The original filter expression is left in place to catch
	// false positives.
	Prev(ctx *EvalContext) (Datum, bool)

	// IsMin returns true if the datum is equal to the minimum value the datum
	// type can hold.
	IsMin(ctx *EvalContext) bool

	// Next returns the next datum and true, if one exists, or nil and false
	// otherwise. The next datum satisfies the following definition: if the
	// receiver is "a" and the returned datum is "b", then for every compatible
	// datum "x", it holds that "x > a" is true if and only if "x >= b" is true.
	//
	// The return value is undefined if IsMax(_ *EvalContext) returns true.
	//
	// TODO(#12022): for DTuple, the contract is actually that "x > a" (SQL order,
	// where x > NULL is unknown for all x) is true only if "x >= b"
	// (.Compare/encoding order, where x >= NULL is true for all x) is true. This
	// is okay for now: the returned datum is used only to construct a span, which
	// uses .Compare/encoding order and is guaranteed to be large enough by this
	// weaker contract. The original filter expression is left in place to catch
	// false positives.
	Next(ctx *EvalContext) (Datum, bool)

	// IsMax returns true if the datum is equal to the maximum value the datum
	// type can hold.
	IsMax(ctx *EvalContext) bool

	// Max returns the upper value and true, if one exists, otherwise
	// nil and false. Used By Prev().
	Max(ctx *EvalContext) (Datum, bool)

	// Min returns the lower value, if one exists, otherwise nil and
	// false. Used by Next().
	Min(ctx *EvalContext) (Datum, bool)

	// Size returns a lower bound on the total size of the receiver in bytes,
	// including memory that is pointed at (even if shared between Datum
	// instances) but excluding allocation overhead.
	//
	// It holds for every Datum d that d.Size().
	Size() uintptr
}

// Datums is a slice of Datum values.
type Datums []Datum

// Len returns the number of Datum values.
func (d Datums) Len() int { return len(d) }

// Format implements the NodeFormatter interface.
func (d *Datums) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	for i, v := range *d {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(v)
	}
	ctx.WriteByte(')')
}

// IsDistinctFrom checks to see if two datums are distinct from each other. Any
// change in value is considered distinct, however, a NULL value is NOT
// considered disctinct from another NULL value.
func (d Datums) IsDistinctFrom(evalCtx *EvalContext, other Datums) bool {
	if len(d) != len(other) {
		return true
	}
	for i, val := range d {
		if val == DNull {
			if other[i] != DNull {
				return true
			}
		} else {
			if val.Compare(evalCtx, other[i]) != 0 {
				return true
			}
		}
	}
	return false
}

// CompositeDatum is a Datum that may require composite encoding in
// indexes. Any Datum implementing this interface must also add itself to
// sqlbase/HasCompositeKeyEncoding.
type CompositeDatum interface {
	Datum
	// IsComposite returns true if this datum is not round-tripable in a key
	// encoding.
	IsComposite() bool
}

// DBool is the boolean Datum.
type DBool bool

// MakeDBool converts its argument to a *DBool, returning either DBoolTrue or
// DBoolFalse.
func MakeDBool(d DBool) *DBool {
	if d {
		return DBoolTrue
	}
	return DBoolFalse
}

// MustBeDBool attempts to retrieve a DBool from an Expr, panicking if the
// assertion fails.
func MustBeDBool(e Expr) DBool {
	b, ok := AsDBool(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DBool, found %T", e))
	}
	return b
}

// AsDBool attempts to retrieve a *DBool from an Expr, returning a *DBool and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions.
func AsDBool(e Expr) (DBool, bool) {
	switch t := e.(type) {
	case *DBool:
		return *t, true
	}
	return false, false
}

// makeParseError returns a parse error using the provided string and type. An
// optional error can be provided, which will be appended to the end of the
// error string.
func makeParseError(s string, typ types.T, err error) error {
	var suffix string
	if err != nil {
		suffix = fmt.Sprintf(": %v", err)
	}
	return pgerror.NewErrorf(
		pgcode.InvalidTextRepresentation, "could not parse %q as type %s%s", s, typ, suffix)
}

func makeUnsupportedComparisonMessage(d1, d2 Datum) string {
	return fmt.Sprintf("unsupported comparison: %s to %s", d1.ResolvedType(), d2.ResolvedType())
}

func isCaseInsensitivePrefix(prefix, s string) bool {
	if len(prefix) > len(s) {
		return false
	}
	return strings.EqualFold(prefix, s[:len(prefix)])
}

// ParseDBool parses and returns the *DBool Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
// See https://github.com/postgres/postgres/blob/90627cf98a8e7d0531789391fd798c9bfcc3bc1a/src/backend/utils/adt/bool.c#L36
func ParseDBool(s string) (*DBool, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return nil, makeParseError(s, types.Bool, pgerror.NewError(pgcode.InvalidTextRepresentation, "invalid bool value"))
	}
	if len(s) >= 1 {
		switch s[0] {
		case 't', 'T':
			if isCaseInsensitivePrefix(s, "true") {
				return DBoolTrue, nil
			}
		case 'f', 'F':
			if isCaseInsensitivePrefix(s, "false") {
				return DBoolFalse, nil
			}
		case 'y', 'Y':
			if isCaseInsensitivePrefix(s, "yes") {
				return DBoolTrue, nil
			}
		case 'n', 'N':
			if isCaseInsensitivePrefix(s, "no") {
				return DBoolFalse, nil
			}
		case '1':
			if s == "1" {
				return DBoolTrue, nil
			}
		case '0':
			if s == "0" {
				return DBoolFalse, nil
			}
		case 'o', 'O':
			// Just 'o' is ambiguous between 'on' and 'off'.
			if len(s) > 1 {
				if isCaseInsensitivePrefix(s, "on") {
					return DBoolTrue, nil
				}
				if isCaseInsensitivePrefix(s, "ok") {
					return DBoolTrue, nil
				}
				if isCaseInsensitivePrefix(s, "off") {
					return DBoolFalse, nil
				}
			}
		}
	}

	dec, err := ParseDDecimal(s)
	if err != nil {
		return nil, makeParseError(s, types.Bool, pgerror.NewError(pgcode.InvalidTextRepresentation, "invalid bool value"))
	}
	decimalOne := apd.New(1, 0)
	if dec.Decimal.Cmp(decimalOne) == 0 {
		return DBoolTrue, nil
	}
	if dec.IsZero() {
		return DBoolFalse, nil
	}
	return nil, makeParseError(s, types.Bool, pgerror.NewError(pgcode.InvalidTextRepresentation, "invalid bool value"))
}

// ParseDByte parses a string representation of hex encoded binary
// data. It supports both the hex format, with "\x" followed by a
// string of hexadecimal digits (the "\x" prefix occurs just once at
// the beginning), and the escaped format, which supports "\\" and
// octal escapes.
func ParseDByte(s string) (*DBytes, error) {
	res, err := lex.DecodeRawBytesToByteArrayAuto([]byte(s))
	if err != nil {
		return nil, makeParseError(s, types.Bytes, err)
	}
	return NewDBytes(DBytes(res)), nil
}

// ParseDUuidFromString parses and returns the *DUuid Datum value represented
// by the provided input string, or an error.
func ParseDUuidFromString(s string) (*DUuid, error) {
	uv, err := uuid.FromString(s)
	if err != nil {
		return nil, makeParseError(s, types.UUID, err)
	}
	return NewDUuid(DUuid{uv}), nil
}

// ParseDUuidFromBytes parses and returns the *DUuid Datum value represented
// by the provided input bytes, or an error.
func ParseDUuidFromBytes(b []byte) (*DUuid, error) {
	uv, err := uuid.FromBytes(b)
	if err != nil {
		return nil, makeParseError(string(b), types.UUID, err)
	}
	return NewDUuid(DUuid{uv}), nil
}

// ParseDIPAddrFromINetString parses and returns the *DIPAddr Datum value
// represented by the provided input INet string, or an error.
func ParseDIPAddrFromINetString(s string) (*DIPAddr, error) {
	var d DIPAddr
	err := ipaddr.ParseINet(s, &d.IPAddr)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// GetBool gets DBool or an error (also treats NULL as false, not an error).
func GetBool(d Datum) (DBool, error) {
	if v, ok := d.(*DBool); ok {
		return *v, nil
	}
	if d == DNull {
		return DBool(false), nil
	}
	return false, pgerror.NewAssertionErrorf("cannot convert %s to type %s", d.ResolvedType(), types.Bool)
}

// ResolvedType implements the TypedExpr interface.
func (*DBool) ResolvedType() types.T {
	return types.Bool
}

// Compare implements the Datum interface.
func (d *DBool) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DBool)
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DDecimal:
		return -t.Compare(ctx, d)
	}
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return CompareBools(bool(*d), bool(*v))
}

// CompareBools compares the input bools according to the SQL comparison rules.
func CompareBools(d, v bool) int {
	if !d && v {
		return -1
	}
	if d && !v {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (*DBool) Prev(_ *EvalContext) (Datum, bool) {
	return DBoolFalse, true
}

// Next implements the Datum interface.
func (*DBool) Next(_ *EvalContext) (Datum, bool) {
	return DBoolTrue, true
}

// IsMax implements the Datum interface.
func (d *DBool) IsMax(_ *EvalContext) bool {
	return bool(*d)
}

// IsMin implements the Datum interface.
func (d *DBool) IsMin(_ *EvalContext) bool {
	return !bool(*d)
}

// Min implements the Datum interface.
func (d *DBool) Min(_ *EvalContext) (Datum, bool) {
	return DBoolFalse, true
}

// Max implements the Datum interface.
func (d *DBool) Max(_ *EvalContext) (Datum, bool) {
	return DBoolTrue, true
}

// AmbiguousFormat implements the Datum interface.
func (*DBool) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (d *DBool) Format(ctx *FmtCtx) {
	if ctx.HasFlags(fmtPgwireFormat) {
		if bool(*d) {
			ctx.WriteByte('t')
		} else {
			ctx.WriteByte('f')
		}
		return
	}
	ctx.WriteString(strconv.FormatBool(bool(*d)))
}

// Size implements the Datum interface.
func (d *DBool) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DBitArray is the BIT/VARBIT Datum.
type DBitArray struct {
	bitarray.BitArray
}

// ParseDBitArray parses a string representation of binary digits.
func ParseDBitArray(s string) (*DBitArray, error) {
	var a DBitArray
	var err error
	a.BitArray, err = bitarray.Parse(s)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// NewDBitArray returns a DBitArray.
func NewDBitArray(bitLen uint) *DBitArray {
	a := MakeDBitArray(bitLen)
	return &a
}

// MakeDBitArray returns a DBitArray.
func MakeDBitArray(bitLen uint) DBitArray {
	return DBitArray{BitArray: bitarray.MakeZeroBitArray(bitLen)}
}

// MustBeDBitArray attempts to retrieve a DBitArray from an Expr, panicking if the
// assertion fails.
func MustBeDBitArray(e Expr) *DBitArray {
	b, ok := AsDBitArray(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DBitArray, found %T", e))
	}
	return b
}

// AsDBitArray attempts to retrieve a *DBitArray from an Expr, returning a *DBitArray and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions.
func AsDBitArray(e Expr) (*DBitArray, bool) {
	switch t := e.(type) {
	case *DBitArray:
		return t, true
	}
	return nil, false
}

var errCannotCastNegativeIntToBitArray = pgerror.NewErrorf(pgcode.CannotCoerce,
	"cannot cast negative integer to bit varying with unbounded width")

// NewDBitArrayFromInt creates a bit array from the specified integer
// at the specified width.
// If the width is zero, only positive integers can be converted.
// If the width is nonzero, the value is truncated to that width.
// Negative values are encoded using two's complement.
func NewDBitArrayFromInt(i int64, width uint) (*DBitArray, error) {
	if width == 0 && i < 0 {
		return nil, errCannotCastNegativeIntToBitArray
	}
	return &DBitArray{
		BitArray: bitarray.MakeBitArrayFromInt64(width, i, 64),
	}, nil
}

// AsDInt computes the integer value of the given bit array.
// The value is assumed to be encoded using two's complement.
// The result is truncated to the given integer number of bits,
// if specified.
// The given width must be 64 or smaller. The results are undefined
// if n is greater than 64.
func (d *DBitArray) AsDInt(n uint) *DInt {
	if n == 0 {
		n = 64
	}
	return NewDInt(DInt(d.BitArray.AsInt64(n)))
}

// ResolvedType implements the TypedExpr interface.
func (*DBitArray) ResolvedType() types.T {
	return types.BitArray
}

// Compare implements the Datum interface.
func (d *DBitArray) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	useDec := false
	var v *DBitArray
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DBitArray:
		v = t
	case *DString:
		vFromStr, err := ParseDBitArray(string(*t))
		if err != nil {
			panic(err)
		}
		v = vFromStr
	case *DInt, *DFloat, *DDecimal, *DBool, *DDate, *DTime, *DTimestamp, *DTimestampTZ:
		useDec = true
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	// v, ok := UnwrapDatum(ctx, other).(*DBitArray)
	// if !ok {
	// 	panic(makeUnsupportedComparisonMessage(d, other))
	// }

	vDec := &DDecimal{}
	if useDec {
		if d == nil {
			return -1
		}
		vDec.SetFinite(d.AsInt64(64), 0)
		return vDec.Compare(ctx, other)
	}
	return bitarray.Compare(d.BitArray, v.BitArray)
}

// Prev implements the Datum interface.
func (d *DBitArray) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DBitArray) Next(_ *EvalContext) (Datum, bool) {
	a := bitarray.Next(d.BitArray)
	return &DBitArray{BitArray: a}, true
}

// IsMax implements the Datum interface.
func (d *DBitArray) IsMax(_ *EvalContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DBitArray) IsMin(_ *EvalContext) bool {
	return d.BitArray.IsEmpty()
}

var bitArrayZero = NewDBitArray(0)

// Min implements the Datum interface.
func (d *DBitArray) Min(_ *EvalContext) (Datum, bool) {
	return bitArrayZero, true
}

// Max implements the Datum interface.
func (d *DBitArray) Max(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DBitArray) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (d *DBitArray) Format(ctx *FmtCtx) {
	f := ctx.flags
	if f.HasFlags(fmtPgwireFormat) {
		d.BitArray.Format(&ctx.Buffer)
	} else {
		withQuotes := !f.HasFlags(FmtFlags(lex.EncBareStrings))
		if withQuotes {
			ctx.WriteString("B'")
		}
		d.BitArray.Format(&ctx.Buffer)
		if withQuotes {
			ctx.WriteByte('\'')
		}
	}
}

// Size implements the Datum interface.
func (d *DBitArray) Size() uintptr {
	return d.BitArray.Sizeof()
}

// DInt is the int Datum.
type DInt int64

// NewDInt is a helper routine to create a *DInt initialized from its argument.
func NewDInt(d DInt) *DInt {
	return &d
}

// NewDInt16 same as NewDInt
func NewDInt16(d DInt) *DInt {
	DInt16 := DInt(int(d) % (1 << 16))
	if DInt16 > 32767 || d < 0 {
		DInt16 = DInt16 - (1 << 15)
	}
	return &DInt16
}

// NewDInt32 same as NewDInt
func NewDInt32(d DInt) *DInt {
	DInt32 := DInt(int(d) % (1 << 32))
	if DInt32 >= 2147483647 || d < 0 {
		DInt32 = DInt32 - (1 << 31)
	}
	return &DInt32
}

//ParseDStringArray parses string to stringarray
func ParseDStringArray(s string) (*DArray, error) {
	if s == "" {
		return nil, fmt.Errorf("array must be enclosed in { and }")
	}
	s = strings.TrimSpace(s)
	//对于字符串“{}”,返回空数组
	if s == "{}" {
		return &DArray{ParamTyp: types.String}, nil
	}
	ret := NewDArray(types.String)

	//将入参字符串按','分割
	str := strings.Split(s, ",")

	//切割后的第一个元素,可能包含'{'
	left := []byte(str[0])

	//切割后的最后一个元素, 可能包含'}'
	right := []byte(str[len(str)-1])
	if len(left) == 0 || len(right) == 0 {
		return nil, fmt.Errorf("invalid array:%s", s)
	}
	//尝试对以{}包裹的字符串转换为string数组
	if left[0] == '{' && right[len(right)-1] == '}' {
		for i, v := range str {
			//不含‘,’的入参, 即只有一个元素, v形如"{a}"
			if v == s {
				str := strings.TrimSpace(string(left[1 : len(left)-1]))
				_ = ret.Append(NewDString(str))
				continue
			}
			//对切割后的第一个元素转换
			if i == 0 {
				str := strings.TrimSpace(string(left[1:]))
				if str == "" {
					return nil, fmt.Errorf("invalid array:%s", s)
				}
				_ = ret.Append(NewDString(str))
				continue
			}
			//对切割后的最后一个元素转换
			if i == len(str)-1 {
				str := strings.TrimSpace(string(right[0 : len(right)-1]))
				if str == "" {
					return nil, fmt.Errorf("invalid array:%s", s)
				}
				_ = ret.Append(NewDString(str))
				continue
			}
			//对其余的元素转换
			str := strings.TrimSpace(v)
			if str == "" {
				return nil, fmt.Errorf("invalid array:%s", s)
			}
			_ = ret.Append(NewDString(str))
		}
	} else {
		return nil, fmt.Errorf("array must be enclosed in { and }")
	}
	return ret, nil
}

//ParseDIntArray parses intarray of string to intarray
func ParseDIntArray(s string) (*DArray, error) {
	if s == "" {
		return nil, fmt.Errorf("array must be enclosed in { and }")
	}
	s = strings.TrimSpace(s)
	//对于字符串“{}”,返回空数组
	if s == "{}" {
		return &DArray{ParamTyp: types.Int}, nil
	}
	ret := NewDArray(types.Int)

	//将入参字符串按','分割
	str := strings.Split(s, ",")

	//切割后的第一个元素,可能包含'{'
	left := []byte(str[0])

	//切割后的最后一个元素, 可能包含'}'
	right := []byte(str[len(str)-1])
	if len(left) == 0 || len(right) == 0 {
		return nil, fmt.Errorf("invalid array:%s", s)
	}
	//尝试对以{}包裹的字符串转换为数字数组
	if left[0] == '{' && right[len(right)-1] == '}' {
		for i, v := range str {
			//不含‘,’的入参, 即只有一个元素, v形如"{num}"
			if v == s {
				str := strings.TrimSpace(string(left[1 : len(left)-1]))
				//尝试将第一个元素转换为int8,截取num
				num, err := strconv.ParseInt(str, 10, 64)
				if err != nil {
					f, err := strconv.ParseFloat(str, 10)
					if err != nil {
						if str == "" {
							return nil, fmt.Errorf("invalid array:%s", s)
						}
						if strings.ToLower(str) == "null" {
							_ = ret.Append(NewDInt(DInt(0)))
							continue
						}
						return nil, fmt.Errorf("can't convert %s to int", str)
					}
					if f-math.Trunc(f) == 0.0 {
						_ = ret.Append(NewDInt(DInt(f)))
						continue
					} else {
						_ = ret.Append(NewDFloat(DFloat(f)))
						continue
					}
				}
				_ = ret.Append(NewDInt(DInt(num)))
				continue
			}
			//对切割后的第一个元素转换
			if i == 0 {
				str := strings.TrimSpace(string(left[1:]))
				num, err := strconv.ParseInt(str, 10, 64)
				if err != nil {
					f, err := strconv.ParseFloat(str, 10)
					if err != nil {
						if str == "" {
							return nil, fmt.Errorf("invalid array:%s", s)
						}
						if strings.ToLower(str) == "null" {
							_ = ret.Append(NewDInt(DInt(0)))
							continue
						}
						return nil, fmt.Errorf("can't convert %s to int", str)
					}
					if f-math.Trunc(f) == 0.0 {
						_ = ret.Append(NewDInt(DInt(f)))
						continue
					} else {
						_ = ret.Append(NewDFloat(DFloat(f)))
						continue
					}
				}
				_ = ret.Append(NewDInt(DInt(num)))
				continue
			}
			//对切割后的最后一个元素转换
			if i == len(str)-1 {
				str := strings.TrimSpace(string(right[:len(right)-1]))
				num, err := strconv.ParseInt(str, 10, 64)
				if err != nil {
					f, err := strconv.ParseFloat(str, 10)
					if err != nil {
						if str == "" {
							return nil, fmt.Errorf("invalid array:%s", s)
						}
						if strings.ToLower(str) == "null" {
							_ = ret.Append(NewDInt(DInt(0)))
							continue
						}
						return nil, fmt.Errorf("can't convert %s to int", str)
					}
					if f-math.Trunc(f) == 0.0 {
						_ = ret.Append(NewDInt(DInt(f)))
						continue
					} else {
						_ = ret.Append(NewDFloat(DFloat(f)))
						continue
					}
				}
				_ = ret.Append(NewDInt(DInt(num)))
				continue
			}
			//对其余的元素转换
			num, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				f, err := strconv.ParseFloat(strings.TrimSpace(v), 10)
				if err != nil {
					if strings.TrimSpace(v) == "" {
						return nil, fmt.Errorf("invalid array:%s", s)
					}
					if strings.ToLower(strings.TrimSpace(v)) == "null" {
						_ = ret.Append(NewDInt(DInt(0)))
						continue
					}
					return nil, fmt.Errorf("can't convert %s to int", strings.TrimSpace(v))
				}
				if f-math.Trunc(f) == 0.0 {
					_ = ret.Append(NewDInt(DInt(f)))
					continue
				} else {
					_ = ret.Append(NewDFloat(DFloat(f)))
					continue
				}
			}
			_ = ret.Append(NewDInt(DInt(num)))
		}
	} else {
		return nil, fmt.Errorf("array must be enclosed in { and }")
	}
	return ret, nil
}

//ParseDFloatArray parses Floatarray of string to Floatarray
func ParseDFloatArray(s string) (*DArray, error) {
	if s == "" {
		return nil, fmt.Errorf("array must be enclosed in { and }")
	}
	s = strings.TrimSpace(s)
	//对于字符串“{}”,返回空数组
	if s == "" || s == "{}" {
		return &DArray{ParamTyp: types.Float}, nil
	}
	ret := NewDArray(types.Float)

	//将入参字符串按','分割
	str := strings.Split(s, ",")

	//切割后的第一个元素,可能包含'{'
	left := []byte(str[0])

	//切割后的最后一个元素, 可能包含'}'
	right := []byte(str[len(str)-1])
	if len(left) == 0 || len(right) == 0 {
		return nil, fmt.Errorf("invalid array:%s", s)
	}
	//尝试对以{}包裹的字符串转换为数字数组
	if left[0] == '{' && right[len(right)-1] == '}' {
		for i, v := range str {
			//不含‘,’的入参, 即只有一个元素, v形如"{num}"
			if v == s {
				str := strings.TrimSpace(string(left[1 : len(left)-1]))
				f, err := strconv.ParseFloat(str, 10)
				if err != nil {
					if str == "" {
						return nil, fmt.Errorf("invalid array:%s", s)
					}
					if strings.ToLower(str) == "null" {
						_ = ret.Append(NewDFloat(DFloat(0.0)))
						continue
					}
					return nil, fmt.Errorf("can't convert %s to float", str)
				}
				_ = ret.Append(NewDFloat(DFloat(f)))
				continue

			}
			//对切割后的第一个元素转换
			if i == 0 {
				str := strings.TrimSpace(string(left[1:]))
				f, err := strconv.ParseFloat(str, 10)
				if err != nil {
					if str == "" {
						return nil, fmt.Errorf("invalid array:%s", s)
					}
					if strings.ToLower(str) == "null" {
						_ = ret.Append(NewDFloat(DFloat(0.0)))
						continue
					}
					return nil, fmt.Errorf("can't convert %s to float", str)
				}
				_ = ret.Append(NewDFloat(DFloat(f)))
				continue

			}
			//对切割后的最后一个元素转换
			if i == len(str)-1 {
				str := strings.TrimSpace(string(right[:len(right)-1]))
				f, err := strconv.ParseFloat(str, 10)
				if err != nil {
					if str == "" {
						return nil, fmt.Errorf("invalid array:%s", s)
					}
					if strings.ToLower(str) == "null" {
						_ = ret.Append(NewDFloat(DFloat(0.0)))
						continue
					}
					return nil, fmt.Errorf("can't convert %s to float", str)
				}
				_ = ret.Append(NewDFloat(DFloat(f)))
				continue
			}
			//对其余的元素转换
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 10)
			if err != nil {
				if strings.TrimSpace(v) == "" {
					return nil, fmt.Errorf("invalid array:%s", s)
				}
				if strings.ToLower(strings.TrimSpace(v)) == "null" {
					_ = ret.Append(NewDFloat(DFloat(0.0)))
					continue
				}
				return nil, fmt.Errorf("can't convert %s to float", strings.TrimSpace(v))
			}
			_ = ret.Append(NewDFloat(DFloat(f)))
			continue

		}
	} else {
		return nil, fmt.Errorf("array must be enclosed in { and }")
	}
	return ret, nil
}

// ParseDInt parses and returns the *DInt Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDInt(s string, useOrigin bool) (*DInt, error) {
	d, err := strconv.ParseInt(s, 0, 64)
	if err == nil {
		return NewDInt(DInt(d)), nil
	}
	dd := &DDecimal{}
	newErr := dd.SetString(s)
	if newErr == nil {
		tempDD := &apd.Decimal{}
		_, err := DecimalCtx.RoundToIntegralValue(tempDD, &dd.Decimal)
		if err != nil {
			return nil, err
		}
		i, err := tempDD.Int64()
		if err != nil {
			return nil, ErrIntOutOfRange
		}
		return NewDInt(DInt(i)), nil
	}
	if !useOrigin {
		// f, err := ParseDFloat(s)
		dec, err := ParseDDDecimal(s)
		if err != nil {
			return nil, err
		}
		VFloat, errf := dec.Float64()
		if errf != nil {
			return nil, errf
		}
		return NewDInt(DInt(math.Round(VFloat))), nil
	}
	return nil, makeParseError(s, types.Int, err)
}

// AsDInt attempts to retrieve a DInt from an Expr, returning a DInt and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DInt wrapped by a
// *DOidWrapper is possible.
func AsDInt(e Expr) (DInt, bool) {
	switch t := e.(type) {
	case *DInt:
		return *t, true
	case *DOidWrapper:
		return AsDInt(t.Wrapped)
	case *DFloat:
		asint := math.Round(float64(*t))
		if math.Abs(float64(*t)-asint) < 1e-8 {
			return DInt(asint), true
		}
	case *DString:
		if sfloat, err := strconv.ParseFloat(string(*t), 64); err == nil {
			asint := math.Round(sfloat)
			if math.Abs(sfloat-asint) < 1e-8 {
				return DInt(asint), true
			}
		}
	case *DDecimal:
		asfloat, _ := t.Float64()
		asint := math.Round(asfloat)
		if math.Abs(asfloat-asint) < 1e-8 {
			return DInt(asint), true
		}
	}

	return 0, false
}

// AsDFloat attempts to retrieve a DFloat from an Expr, returning a DFloat and
// a flag signifying whether the assertion was successful.
func AsDFloat(e Expr) (DFloat, bool) {
	switch t := e.(type) {
	case *DFloat:
		return *t, true
	case *DOidWrapper:
		return AsDFloat(t.Wrapped)
	}
	return 0, false
}

// MustBeDInt attempts to retrieve a DInt from an Expr, panicking if the
// assertion fails.
func MustBeDInt(e Expr) DInt {
	i, ok := AsDInt(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DInt, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (*DInt) ResolvedType() types.T {
	return types.Int
}

// Compare implements the Datum interface.
func (d *DInt) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	var v DInt
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DInt:
		v = *t
	case *DBitArray:
		v = *t.AsDInt(64)
	case *DFloat, *DDecimal:
		return -t.Compare(ctx, d)
	case *DString:
		vStr, _ := ParseDInt(string(*t), false)
		if vStr == nil {
			return 0
		}
		v = *vStr
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return -1
	}
	if *d > v {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (d *DInt) Prev(_ *EvalContext) (Datum, bool) {
	return NewDInt(*d - 1), true
}

// Next implements the Datum interface.
func (d *DInt) Next(_ *EvalContext) (Datum, bool) {
	return NewDInt(*d + 1), true
}

// IsMax implements the Datum interface.
func (d *DInt) IsMax(_ *EvalContext) bool {
	return *d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DInt) IsMin(_ *EvalContext) bool {
	return *d == math.MinInt64
}

var dMaxInt = NewDInt(math.MaxInt64)
var dMinInt = NewDInt(math.MinInt64)

// Max implements the Datum interface.
func (d *DInt) Max(_ *EvalContext) (Datum, bool) {
	return dMaxInt, true
}

// Min implements the Datum interface.
func (d *DInt) Min(_ *EvalContext) (Datum, bool) {
	return dMinInt, true
}

// AmbiguousFormat implements the Datum interface.
func (*DInt) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DInt) Format(ctx *FmtCtx) {
	// If the number is negative, we need to use parens or the `:::INT` type hint
	// will take precedence over the negation sign.
	disambiguate := ctx.flags.HasFlags(fmtDisambiguateDatumTypes)
	parsable := ctx.flags.HasFlags(FmtParsableNumerics)
	needParens := (disambiguate || parsable) && *d < 0
	if needParens {
		ctx.WriteByte('(')
	}
	ctx.WriteString(strconv.FormatInt(int64(*d), 10))
	if needParens {
		ctx.WriteByte(')')
	}
}

// Size implements the Datum interface.
func (d *DInt) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DFloat is the float Datum.
type DFloat float64

// MustBeDFloat attempts to retrieve a DFloat from an Expr, panicking if the
// assertion fails.
func MustBeDFloat(e Expr) DFloat {
	switch t := e.(type) {
	case *DFloat:
		return *t
	}
	panic(pgerror.NewAssertionErrorf("expected *DFloat, found %T", e))
}

// NewDFloat is a helper routine to create a *DFloat initialized from its
// argument.
func NewDFloat(d DFloat) *DFloat {
	return &d
}

// ParseDFloat parses and returns the *DFloat Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDFloat(s string, useOrigin bool, precision int) (*DFloat, error) {
	ff, errf := strconv.ParseFloat(s, precision)
	if errf == nil {
		return NewDFloat(DFloat(ff)), nil
	}
	if useOrigin {
		return nil, errf
	}
	if len(s) <= 0 {
		return nil, errf
	}
	sign := s[0]
	signNum := 1
	dotNum := 0
	powSign := 1
	var powNum float64 = 1
	var scp string
	var powStr string
	if sign == '+' {
		s = s[1:]
	} else if sign == '-' {
		s = s[1:]
		signNum = -1
	}
OuterFor:
	for i := 0; i < len(s); i++ {
		// 对于非数字字符的判断条件
		if !unicode.IsDigit(rune(s[i])) {
			if s[i] == '.' && dotNum == 0 { // 小数点有且最多有一个
				dotNum++
			} else if (s[i] == 'e') || (s[i] == 'E') { // 科学计数法，‘e'的个数至多为1
				// 数值字符串记录
				scp = s
				s = s[:i]
				// 记录乘方值
				if i < len(scp)-1 {
					if scp[i+1] == '+' {
						powSign = 1
						i++
					} else if scp[i+1] == '-' {
						powSign = -1
						i++
					}
				}
				for j := i + 1; j < len(scp)+1; j++ {
					if j == len(scp) || (!unicode.IsDigit(rune(scp[j]))) {
						powStr = scp[i+1 : j]
						break OuterFor
					}
				}
			} else { // 其他情况说明数值阶段已经结束
				s = s[:i]
			}
		}
	}
	var f float64
	var err error
	if s != "" {
		f, err = strconv.ParseFloat(s, 64)
	} else {
		f, err = 0, nil
	}

	if powStr != "" {
		if tempPowNum, err := strconv.ParseInt(powStr, 0, 64); err == nil {
			if powSign == 1 {
				powNum = math.Pow(10, float64(tempPowNum))
			} else {
				powNum = math.Pow(0.1, float64(tempPowNum))
			}
		}
	} else if powStr == "" {
		powNum = 1
	}

	f = f * float64(signNum) * float64(powNum)
	return NewDFloat(DFloat(f)), err
}

// ParseDDDecimal parses and returns the *DDecimal Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDDDecimal(s string) (*DDecimal, error) {
	s = strings.TrimSpace(s)
	var dd DDecimal
	var err error
	if err = dd.SetString(s); err == nil {
		return &dd, nil
	}
	if len(s) <= 0 {
		return nil, errors.Errorf("cannot parse \"%s\" as decimal", s)
	}
	dotNum := 0
	powSign := 1
	// var powNum float64 = 1
	var scp string
	var powStr string
OuterFor:
	for i := 0; i < len(s); i++ {
		if i == 0 && (s[i] == '-' || s[i] == '+') {
			continue
		}
		// 对于非数字字符的判断条件
		if !unicode.IsDigit(rune(s[i])) {
			if s[i] == '.' && dotNum == 0 { // 小数点有且最多有一个
				dotNum++
			} else if (s[i] == 'e') || (s[i] == 'E') { // 科学计数法，‘e'的个数至多为1
				// 数值字符串记录
				scp = s
				if len(s) > i {
					s = s[:i+1]
				}

				// 记录乘方值
				if i < len(scp)-1 {
					if scp[i+1] == '+' {
						powSign = 1
						i++
					} else if scp[i+1] == '-' {
						powSign = -1
						i++
					}
				}
				for j := i + 1; j < len(scp)+1; j++ {
					if j == len(scp) || (!unicode.IsDigit(rune(scp[j]))) {
						powStr = scp[i+1 : j]
						break OuterFor
					}
				}
			} else { // 其他情况说明数值阶段已经结束
				s = s[:i]
			}
		}
	}
	if powStr != "" {
		if powSign == 1 {
			powStr = "+" + powStr
		} else {
			powStr = "-" + powStr
		}
	}
	if s != "" && s != "e" && s != "+" && s != "-" {
		err = dd.SetString(s + powStr)
	} else {
		err = dd.SetString("0")
	}
	return &dd, err
}

// ResolvedType implements the TypedExpr interface.
func (*DFloat) ResolvedType() types.T {
	return types.Float
}

// Compare implements the Datum interface.
func (d *DFloat) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	var v DFloat
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DFloat:
		v = *t
	case *DInt:
		v = DFloat(MustBeDInt(t))
	case *DBitArray:
		v = DFloat(MustBeDInt(t.AsDInt(64)))
	case *DDecimal:
		return -t.Compare(ctx, d)
	case *DString:
		vStr, err := ParseDFloat(string(*t), false, 64)
		if err != nil {
			panic(err)
		}
		v = *vStr
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return -1
	}
	if *d > v {
		return 1
	}
	// NaN sorts before non-NaN (#10109).
	if *d == v {
		return 0
	}
	if math.IsNaN(float64(*d)) {
		if math.IsNaN(float64(v)) {
			return 0
		}
		return -1
	}
	return 1
}

// Prev implements the Datum interface.
func (d *DFloat) Prev(_ *EvalContext) (Datum, bool) {
	f := float64(*d)
	if math.IsNaN(f) {
		return nil, false
	}
	if f == math.Inf(-1) {
		return dNaNFloat, true
	}
	return NewDFloat(DFloat(math.Nextafter(f, math.Inf(-1)))), true
}

// Next implements the Datum interface.
func (d *DFloat) Next(_ *EvalContext) (Datum, bool) {
	f := float64(*d)
	if math.IsNaN(f) {
		return dNegInfFloat, true
	}
	if f == math.Inf(+1) {
		return nil, false
	}
	return NewDFloat(DFloat(math.Nextafter(f, math.Inf(+1)))), true
}

var dPosInfFloat = NewDFloat(DFloat(math.Inf(+1)))
var dNegInfFloat = NewDFloat(DFloat(math.Inf(-1)))
var dNaNFloat = NewDFloat(DFloat(math.NaN()))

// PosInfFloat means a Positive infinity float value
var PosInfFloat = DFloat(math.Inf(+1))

// NegInfFloat means a negative infinity float value
var NegInfFloat = DFloat(math.Inf(-1))

// NaNFloat means a NaN float value
var NaNFloat = DFloat(math.NaN())

// IsMax implements the Datum interface.
func (d *DFloat) IsMax(_ *EvalContext) bool {
	return *d == *dPosInfFloat
}

// IsMin implements the Datum interface.
func (d *DFloat) IsMin(_ *EvalContext) bool {
	return math.IsNaN(float64(*d))
}

// Max implements the Datum interface.
func (d *DFloat) Max(_ *EvalContext) (Datum, bool) {
	return dPosInfFloat, true
}

// Min implements the Datum interface.
func (d *DFloat) Min(_ *EvalContext) (Datum, bool) {
	return dNaNFloat, true
}

// AmbiguousFormat implements the Datum interface.
func (*DFloat) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DFloat) Format(ctx *FmtCtx) {
	fl := float64(*d)

	disambiguate := ctx.flags.HasFlags(fmtDisambiguateDatumTypes)
	parsable := ctx.flags.HasFlags(FmtParsableNumerics)
	quote := parsable && (math.IsNaN(fl) || math.IsInf(fl, 0))
	// We need to use Signbit here and not just fl < 0 because of -0.
	needParens := !quote && (disambiguate || parsable) && math.Signbit(fl)
	// If the number is negative, we need to use parens or the `:::INT` type hint
	// will take precedence over the negation sign.
	if quote {
		ctx.WriteByte('\'')
	} else if needParens {
		ctx.WriteByte('(')
	}
	if _, frac := math.Modf(fl); frac == 0 && -1000000 < *d && *d < 1000000 {
		// d is a small whole number. Ensure it is printed using a decimal point.
		ctx.Printf("%.1f", fl)
	} else {
		ctx.Printf("%g", fl)
	}
	if quote {
		ctx.WriteByte('\'')
	} else if needParens {
		ctx.WriteByte(')')
	}
}

// Size implements the Datum interface.
func (d *DFloat) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// IsComposite implements the CompositeDatum interface.
func (d *DFloat) IsComposite() bool {
	// -0 is composite.
	return math.Float64bits(float64(*d)) == 1<<63
}

// DDecimal is the decimal Datum.
type DDecimal struct {
	apd.Decimal
}

// AsDDecimal attempts to retrieve a DDecimal from an Expr, returning a DDecimal and
// a flag signifying whether the assertion was successful.
func AsDDecimal(e Expr) (*DDecimal, bool) {
	switch t := e.(type) {
	case *DDecimal:
		return t, true
	case *DOidWrapper:
		return AsDDecimal(t.Wrapped)
	}
	return nil, false
}

// MustBeDDecimal attempts to retrieve a DDecimal from an Expr, panicking if the
// assertion fails.
func MustBeDDecimal(e Expr) DDecimal {
	switch t := e.(type) {
	case *DDecimal:
		return *t
	}
	panic(pgerror.NewAssertionErrorf("expected *DDecimal, found %T", e))
}

// ParseDDecimal parses and returns the *DDecimal Datum value represented by the
// provided string, or an error if parsing is unsuccessful.
func ParseDDecimal(s string) (*DDecimal, error) {
	dd := &DDecimal{}
	err := dd.SetString(s)
	return dd, err
}

// SetString sets d to s. Any non-standard NaN values are converted to a
// normal NaN. Any negative zero is converted to positive.
func (d *DDecimal) SetString(s string) error {
	// Using HighPrecisionCtx here restricts the max and min exponents to 2000,
	// and the precision to 2000 places. Any rounding or other inexact conversion
	// will result in an error.
	_, res, err := HighPrecisionCtx.SetString(&d.Decimal, s)
	if res != 0 || err != nil {
		return makeParseError(s, types.Decimal, nil)
	}
	switch d.Form {
	case apd.NaNSignaling:
		d.Form = apd.NaN
		d.Negative = false
	case apd.NaN:
		d.Negative = false
	case apd.Finite:
		if d.IsZero() && d.Negative {
			d.Negative = false
		}
	}
	return nil
}

// ResolvedType implements the TypedExpr interface.
func (*DDecimal) ResolvedType() types.T {
	return types.Decimal
}

// Compare implements the Datum interface.
func (d *DDecimal) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v := ctx.getTmpDec()
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DDecimal:
		v = &t.Decimal
	case *DInt:
		v.SetFinite(int64(*t), 0)
	case *DBitArray:
		v.SetFinite(t.AsInt64(64), 0)
	case *DFloat:
		if _, err := v.SetFloat64(float64(*t)); err != nil {
			panic(err)
		}
	case *DString:
		vStr, err := ParseDDDecimal(string(*t))
		if err != nil {
			panic(err)
		}
		v = &vStr.Decimal
	case *DBool:
		if *t {
			v.SetFinite(1, 0)
		} else {
			v.SetFinite(0, 0)
		}
	case *DTime:
		res, err := ParseDTimeToDType(t, "decimal")
		if err != nil {
			panic(err)
		}
		resD, _ := res.(*DDecimal)
		v = &resD.Decimal
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return CompareDecimals(&d.Decimal, v)
}

// CompareDecimals compares 2 apd.Decimals according to the SQL comparison
// rules, making sure that NaNs sort first.
func CompareDecimals(d *apd.Decimal, v *apd.Decimal) int {
	// NaNs sort first in SQL.
	if dn, vn := d.Form == apd.NaN, v.Form == apd.NaN; dn && !vn {
		return -1
	} else if !dn && vn {
		return 1
	} else if dn && vn {
		return 0
	}
	return d.Cmp(v)
}

// Prev implements the Datum interface.
func (d *DDecimal) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DDecimal) Next(_ *EvalContext) (Datum, bool) {
	return nil, false
}

var dPosInfDecimal = &DDecimal{Decimal: apd.Decimal{Form: apd.Infinite, Negative: false}}
var dNaNDecimal = &DDecimal{Decimal: apd.Decimal{Form: apd.NaN}}

// IsMax implements the Datum interface.
func (d *DDecimal) IsMax(_ *EvalContext) bool {
	return d.Form == apd.Infinite && !d.Negative
}

// IsMin implements the Datum interface.
func (d *DDecimal) IsMin(_ *EvalContext) bool {
	return d.Form == apd.NaN
}

// Max implements the Datum interface.
func (d *DDecimal) Max(_ *EvalContext) (Datum, bool) {
	return dPosInfDecimal, true
}

// Min implements the Datum interface.
func (d *DDecimal) Min(_ *EvalContext) (Datum, bool) {
	return dNaNDecimal, true
}

// AmbiguousFormat implements the Datum interface.
func (*DDecimal) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DDecimal) Format(ctx *FmtCtx) {
	// If the number is negative, we need to use parens or the `:::INT` type hint
	// will take precedence over the negation sign.
	disambiguate := ctx.flags.HasFlags(fmtDisambiguateDatumTypes)
	parsable := ctx.flags.HasFlags(FmtParsableNumerics)
	quote := parsable && d.Decimal.Form != apd.Finite
	needParens := !quote && (disambiguate || parsable) && d.Negative
	if needParens {
		ctx.WriteByte('(')
	}
	if quote {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.Decimal.String())
	if quote {
		ctx.WriteByte('\'')
	}
	if needParens {
		ctx.WriteByte(')')
	}
}

// SizeOfDecimal returns the size in bytes of an apd.Decimal.
func SizeOfDecimal(d *apd.Decimal) uintptr {
	return uintptr(cap(d.Coeff.Bits())) * unsafe.Sizeof(big.Word(0))
}

// Size implements the Datum interface.
func (d *DDecimal) Size() uintptr {
	return unsafe.Sizeof(*d) + SizeOfDecimal(&d.Decimal)
}

var (
	decimalNegativeZero = &apd.Decimal{Negative: true}
	bigTen              = big.NewInt(10)
)

// IsComposite implements the CompositeDatum interface.
func (d *DDecimal) IsComposite() bool {
	// -0 is composite.
	if d.Decimal.CmpTotal(decimalNegativeZero) == 0 {
		return true
	}

	// Check if d is divisible by 10.
	var r big.Int
	r.Rem(&d.Decimal.Coeff, bigTen)
	return r.Sign() == 0
}

// DString is the string Datum.
type DString string

// NewDString is a helper routine to create a *DString initialized from its
// argument.
func NewDString(d string) *DString {
	r := DString(d)
	return &r
}

// AsDString attempts to retrieve a DString from an Expr, returning a DString and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DString wrapped by a
// *DOidWrapper is possible.
func AsDString(e Expr) (DString, bool) {
	switch t := e.(type) {
	case *DString:
		return *t, true
	case *DOidWrapper:
		return AsDString(t.Wrapped)
	}
	return "", false
}

// MustBeDString attempts to retrieve a DString from an Expr, panicking if the
// assertion fails.
func MustBeDString(e Expr) DString {
	i, ok := AsDString(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DString, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (*DString) ResolvedType() types.T {
	return types.String
}

// Compare implements the Datum interface.
func (d *DString) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DInt, *DFloat, *DDecimal:
		dec, err := parseStringAs(types.Decimal, string(*d), ctx, false)
		if err != nil {
			panic(err)
		}
		return dec.Compare(ctx, t)
	default:
		v, ok := UnwrapDatum(ctx, other).(*DString)
		if !ok {
			panic(makeUnsupportedComparisonMessage(d, other))
		}
		if *d < *v {
			return -1
		}
		if *d > *v {
			return 1
		}
		return 0
	}
}

// Prev implements the Datum interface.
func (d *DString) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DString) Next(_ *EvalContext) (Datum, bool) {
	return NewDString(string(roachpb.Key(*d).Next())), true
}

// IsMax implements the Datum interface.
func (*DString) IsMax(_ *EvalContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DString) IsMin(_ *EvalContext) bool {
	return len(*d) == 0
}

var dEmptyString = NewDString("")

// Min implements the Datum interface.
func (d *DString) Min(_ *EvalContext) (Datum, bool) {
	return dEmptyString, true
}

// Max implements the Datum interface.
func (d *DString) Max(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DString) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DString) Format(ctx *FmtCtx) {
	buf, f := &ctx.Buffer, ctx.flags
	if f.HasFlags(fmtRawStrings) {
		buf.WriteString(string(*d))
	} else if f.HasFlags(FmtRawStrings) {
		buf.WriteByte('\'')
		buf.WriteString(string(*d))
		buf.WriteByte('\'')
	} else {
		lex.EncodeSQLStringWithFlags(buf, string(*d), f.EncodeFlags())
	}
}

// Size implements the Datum interface.
func (d *DString) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(*d))
}

// DCollatedString is the Datum for strings with a locale. The struct members
// are intended to be immutable.
type DCollatedString struct {
	Contents string
	Locale   string
	// Key is the collation key.
	Key []byte
}

// CollationEnvironment stores the state needed by NewDCollatedString to
// construct collation keys efficiently.
type CollationEnvironment struct {
	cache  map[string]collationEnvironmentCacheEntry
	buffer *collate.Buffer
}

type collationEnvironmentCacheEntry struct {
	// locale is interned.
	locale string
	// collator is an expensive factory.
	collator *collate.Collator
}

func (env *CollationEnvironment) getCacheEntry(locale string) collationEnvironmentCacheEntry {
	entry, ok := env.cache[locale]
	if !ok {
		if env.cache == nil {
			env.cache = make(map[string]collationEnvironmentCacheEntry)
		}
		entry = collationEnvironmentCacheEntry{locale, collate.New(language.MustParse(locale))}
		env.cache[locale] = entry
	}
	return entry
}

// NewDCollatedString is a helper routine to create a *DCollatedString. Panics
// if locale is invalid. Not safe for concurrent use.
func NewDCollatedString(
	contents string, locale string, env *CollationEnvironment,
) *DCollatedString {
	entry := env.getCacheEntry(locale)
	if env.buffer == nil {
		env.buffer = &collate.Buffer{}
	}
	key := entry.collator.KeyFromString(env.buffer, contents)
	d := DCollatedString{contents, entry.locale, make([]byte, len(key))}
	copy(d.Key, key)
	env.buffer.Reset()
	return &d
}

// AmbiguousFormat implements the Datum interface.
func (*DCollatedString) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (d *DCollatedString) Format(ctx *FmtCtx) {
	lex.EncodeSQLString(&ctx.Buffer, d.Contents)
	ctx.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(&ctx.Buffer, d.Locale, lex.EncNoFlags)
}

// ResolvedType implements the TypedExpr interface.
func (d *DCollatedString) ResolvedType() types.T {
	return types.TCollatedString{Locale: d.Locale}
}

// Compare implements the Datum interface.
func (d *DCollatedString) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DCollatedString)
	if !ok || d.Locale != v.Locale {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return bytes.Compare(d.Key, v.Key)
}

// Prev implements the Datum interface.
func (d *DCollatedString) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DCollatedString) Next(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (*DCollatedString) IsMax(_ *EvalContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DCollatedString) IsMin(_ *EvalContext) bool {
	return d.Contents == ""
}

// Min implements the Datum interface.
func (d *DCollatedString) Min(_ *EvalContext) (Datum, bool) {
	return &DCollatedString{"", d.Locale, nil}, true
}

// Max implements the Datum interface.
func (d *DCollatedString) Max(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Size implements the Datum interface.
func (d *DCollatedString) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(d.Contents)) + uintptr(len(d.Locale)) + uintptr(len(d.Key))
}

// IsComposite implements the CompositeDatum interface.
func (d *DCollatedString) IsComposite() bool {
	return true
}

// DBytes is the bytes Datum. The underlying type is a string because we want
// the immutability, but this may contain arbitrary bytes.
type DBytes string

// NewDBytes is a helper routine to create a *DBytes initialized from its
// argument.
func NewDBytes(d DBytes) *DBytes {
	return &d
}

// MustBeDBytes attempts to convert an Expr into a DBytes, panicking if unsuccessful.
func MustBeDBytes(e Expr) DBytes {
	i, ok := AsDBytes(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DBytes, found %T", e))
	}
	return i
}

// AsDBytes attempts to convert an Expr into a DBytes, returning a flag indicating
// whether it was successful.
func AsDBytes(e Expr) (DBytes, bool) {
	switch t := e.(type) {
	case *DBytes:
		return *t, true
	}
	return "", false
}

// ResolvedType implements the TypedExpr interface.
func (*DBytes) ResolvedType() types.T {
	return types.Bytes
}

// Compare implements the Datum interface.
func (d *DBytes) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DBytes)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < *v {
		return -1
	}
	if *d > *v {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (d *DBytes) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DBytes) Next(_ *EvalContext) (Datum, bool) {
	return NewDBytes(DBytes(roachpb.Key(*d).Next())), true
}

// IsMax implements the Datum interface.
func (*DBytes) IsMax(_ *EvalContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DBytes) IsMin(_ *EvalContext) bool {
	return len(*d) == 0
}

var dEmptyBytes = NewDBytes(DBytes(""))

// Min implements the Datum interface.
func (d *DBytes) Min(_ *EvalContext) (Datum, bool) {
	return dEmptyBytes, true
}

// Max implements the Datum interface.
func (d *DBytes) Max(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DBytes) AmbiguousFormat() bool { return true }

func writeAsHexString(ctx *FmtCtx, d *DBytes) {
	b := string(*d)
	for i := 0; i < len(b); i++ {
		ctx.Write(stringencoding.RawHexMap[b[i]])
	}
}

// Format implements the NodeFormatter interface.
func (d *DBytes) Format(ctx *FmtCtx) {
	f := ctx.flags
	if f.HasFlags(fmtPgwireFormat) {
		ctx.WriteString(`"\\x`)
		writeAsHexString(ctx, d)
		ctx.WriteString(`"`)
	} else {
		withQuotes := !f.HasFlags(FmtFlags(lex.EncBareStrings))
		if withQuotes {
			ctx.WriteByte('\'')
		}
		ctx.WriteString("\\x")
		writeAsHexString(ctx, d)
		if withQuotes {
			ctx.WriteByte('\'')
		}
	}
}

// Size implements the Datum interface.
func (d *DBytes) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(*d))
}

// DUuid is the UUID Datum.
type DUuid struct {
	uuid.UUID
}

// NewDUuid is a helper routine to create a *DUuid initialized from its
// argument.
func NewDUuid(d DUuid) *DUuid {
	return &d
}

// ResolvedType implements the TypedExpr interface.
func (*DUuid) ResolvedType() types.T {
	return types.UUID
}

// Compare implements the Datum interface.
func (d *DUuid) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DUuid)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return bytes.Compare(d.GetBytes(), v.GetBytes())
}

func (d DUuid) equal(other *DUuid) bool {
	return bytes.Equal(d.GetBytes(), other.GetBytes())
}

// Prev implements the Datum interface.
func (d *DUuid) Prev(_ *EvalContext) (Datum, bool) {
	i := d.ToUint128()
	u := uuid.FromUint128(i.Sub(1))
	return NewDUuid(DUuid{u}), true
}

// Next implements the Datum interface.
func (d *DUuid) Next(_ *EvalContext) (Datum, bool) {
	i := d.ToUint128()
	u := uuid.FromUint128(i.Add(1))
	return NewDUuid(DUuid{u}), true
}

// IsMax implements the Datum interface.
func (d *DUuid) IsMax(_ *EvalContext) bool {
	return d.equal(dMaxUUID)
}

// IsMin implements the Datum interface.
func (d *DUuid) IsMin(_ *EvalContext) bool {
	return d.equal(dMinUUID)
}

var dMinUUID = NewDUuid(DUuid{uuid.UUID{}})
var dMaxUUID = NewDUuid(DUuid{uuid.UUID{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}})

// Min implements the Datum interface.
func (*DUuid) Min(_ *EvalContext) (Datum, bool) {
	return dMinUUID, true
}

// Max implements the Datum interface.
func (*DUuid) Max(_ *EvalContext) (Datum, bool) {
	return dMaxUUID, true
}

// AmbiguousFormat implements the Datum interface.
func (*DUuid) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DUuid) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lex.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.UUID.String())
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DUuid) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DIPAddr is the IPAddr Datum.
type DIPAddr struct {
	ipaddr.IPAddr
}

// NewDIPAddr is a helper routine to create a *DIPAddr initialized from its
// argument.
func NewDIPAddr(d DIPAddr) *DIPAddr {
	return &d
}

// AsDIPAddr attempts to retrieve a *DIPAddr from an Expr, returning a *DIPAddr and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DIPAddr wrapped by a
// *DOidWrapper is possible.
func AsDIPAddr(e Expr) (DIPAddr, bool) {
	switch t := e.(type) {
	case *DIPAddr:
		return *t, true
	case *DOidWrapper:
		return AsDIPAddr(t.Wrapped)
	}
	return DIPAddr{}, false
}

// MustBeDIPAddr attempts to retrieve a DIPAddr from an Expr, panicking if the
// assertion fails.
func MustBeDIPAddr(e Expr) DIPAddr {
	i, ok := AsDIPAddr(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DIPAddr, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (*DIPAddr) ResolvedType() types.T {
	return types.INet
}

// Compare implements the Datum interface.
func (d *DIPAddr) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DIPAddr)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}

	return d.IPAddr.Compare(&v.IPAddr)
}

func (d DIPAddr) equal(other *DIPAddr) bool {
	return d.IPAddr.Equal(&other.IPAddr)
}

// Prev implements the Datum interface.
func (d *DIPAddr) Prev(_ *EvalContext) (Datum, bool) {
	// We will do one of the following to get the Prev IPAddr:
	//	- Decrement IP address if we won't underflow the IP.
	//	- Decrement mask and set the IP to max in family if we will underflow.
	//	- Jump down from IPv6 to IPv4 if we will underflow both IP and mask.
	if d.Family == ipaddr.IPv6family && d.Addr.Equal(dIPv6min) {
		if d.Mask == 0 {
			// Jump down IP family.
			return dMaxIPv4Addr, true
		}
		// Decrease mask size, wrap IPv6 IP address.
		return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv6family, Addr: dIPv6max, Mask: d.Mask - 1}}), true
	} else if d.Family == ipaddr.IPv4family && d.Addr.Equal(dIPv4min) {
		// Decrease mask size, wrap IPv4 IP address.
		return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv4family, Addr: dIPv4max, Mask: d.Mask - 1}}), true
	}
	// Decrement IP address.
	return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: d.Family, Addr: d.Addr.Sub(1), Mask: d.Mask}}), true
}

// Next implements the Datum interface.
func (d *DIPAddr) Next(_ *EvalContext) (Datum, bool) {
	// We will do one of a few things to get the Next IP address:
	//	- Increment IP address if we won't overflow the IP.
	//	- Increment mask and set the IP to min in family if we will overflow.
	//	- Jump up from IPv4 to IPv6 if we will overflow both IP and mask.
	if d.Family == ipaddr.IPv4family && d.Addr.Equal(dIPv4max) {
		if d.Mask == 32 {
			// Jump up IP family.
			return dMinIPv6Addr, true
		}
		// Increase mask size, wrap IPv4 IP address.
		return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv4family, Addr: dIPv4min, Mask: d.Mask + 1}}), true
	} else if d.Family == ipaddr.IPv6family && d.Addr.Equal(dIPv6max) {
		// Increase mask size, wrap IPv6 IP address.
		return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv6family, Addr: dIPv6min, Mask: d.Mask + 1}}), true
	}
	// Increment IP address.
	return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: d.Family, Addr: d.Addr.Add(1), Mask: d.Mask}}), true
}

// IsMax implements the Datum interface.
func (d *DIPAddr) IsMax(_ *EvalContext) bool {
	return d.equal(dMaxIPAddr)
}

// IsMin implements the Datum interface.
func (d *DIPAddr) IsMin(_ *EvalContext) bool {
	return d.equal(dMinIPAddr)
}

// dIPv4 and dIPv6 min and maxes use ParseIP because the actual byte constant is
// no equal to solely zeros or ones. For IPv4 there is a 0xffff prefix. Without
// this prefix this makes IP arithmetic invalid.
var dIPv4min = ipaddr.Addr(uint128.FromBytes([]byte(net.ParseIP("0.0.0.0"))))
var dIPv4max = ipaddr.Addr(uint128.FromBytes([]byte(net.ParseIP("255.255.255.255"))))
var dIPv6min = ipaddr.Addr(uint128.FromBytes([]byte(net.ParseIP("::"))))
var dIPv6max = ipaddr.Addr(uint128.FromBytes([]byte(net.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"))))

// dMaxIPv4Addr and dMinIPv6Addr are used as global constants to prevent extra
// heap extra allocation
var dMaxIPv4Addr = NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv4family, Addr: dIPv4max, Mask: 32}})
var dMinIPv6Addr = NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv6family, Addr: dIPv6min, Mask: 0}})

// dMinIPAddr and dMaxIPAddr are used as the DIPAddr global min and max.
var dMinIPAddr = NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv4family, Addr: dIPv4min, Mask: 0}})
var dMaxIPAddr = NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv6family, Addr: dIPv6max, Mask: 128}})

// Min implements the Datum interface.
func (*DIPAddr) Min(_ *EvalContext) (Datum, bool) {
	return dMinIPAddr, true
}

// Max implements the Datum interface.
func (*DIPAddr) Max(_ *EvalContext) (Datum, bool) {
	return dMaxIPAddr, true
}

// AmbiguousFormat implements the Datum interface.
func (*DIPAddr) AmbiguousFormat() bool {
	return true
}

// Format implements the NodeFormatter interface.
func (d *DIPAddr) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lex.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.IPAddr.String())
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DIPAddr) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DDate is the date Datum represented as the number of days after
// the Unix epoch.
type DDate int64

// NewDDate is a helper routine to create a *DDate initialized from its
// argument.
func NewDDate(d DDate) *DDate {
	return &d
}

//IsFinite return ture if date is finite
func (d DDate) IsFinite() bool {
	return d != math.MinInt64 && d != math.MaxInt64
}

// NewDDateFromTime constructs a *DDate from a time.Time in the provided time zone.
func NewDDateFromTime(t time.Time, loc *time.Location) *DDate {
	Year, Month, Day := t.In(loc).Date()
	secs := time.Date(Year, Month, Day, 0, 0, 0, 0, time.UTC).Unix()
	return NewDDate(DDate(secs / SecondsInDay))
}

// ParseTimeContext provides the information necessary for
// parsing dates, times, and timestamps. A nil value is generally
// acceptable and will result in reasonable defaults being applied.
type ParseTimeContext interface {
	duration.Context
	// GetRelativeParseTime returns the transaction time in the session's
	// timezone (i.e. now()). This is used to calculate relative dates,
	// like "tomorrow", and also provides a default time.Location for
	// parsed times.
	GetRelativeParseTime() time.Time
}

var _ ParseTimeContext = &EvalContext{}
var _ ParseTimeContext = &SemaContext{}
var _ ParseTimeContext = &simpleParseTimeContext{}

// NewParseTimeContext constructs a ParseTimeContext that returns
// the given values.
func NewParseTimeContext(mode duration.AdditionMode, relativeParseTime time.Time) ParseTimeContext {
	return &simpleParseTimeContext{
		AdditionMode:      mode,
		RelativeParseTime: relativeParseTime,
	}
}

type simpleParseTimeContext struct {
	AdditionMode      duration.AdditionMode
	RelativeParseTime time.Time
}

// GetAdditionMode implements ParseTimeContext.
func (ctx simpleParseTimeContext) GetAdditionMode() duration.AdditionMode {
	return ctx.AdditionMode
}

// GetRelativeParseTime implements ParseTimeContext.
func (ctx simpleParseTimeContext) GetRelativeParseTime() time.Time {
	return ctx.RelativeParseTime
}

// relativeParseTime chooses a reasonable "now" value for
// performing date parsing.
func relativeParseTime(ctx ParseTimeContext) time.Time {
	if ctx == nil {
		return timeutil.Now()
	}
	return ctx.GetRelativeParseTime()
}

// ParseDDate parses and returns the *DDate Datum value represented by the provided
// string in the provided location, or an error if parsing is unsuccessful.
func ParseDDate(ctx ParseTimeContext, s string) (*DDate, error) {
	now := relativeParseTime(ctx)
	t, olderr := pgdate.ParseDate(now, 0 /* mode */, s)
	if olderr == nil {
		return NewDDateFromTime(t, time.UTC), nil
	}
	if len(s) == 0 {
		return nil, pgerror.NewErrorf(pgcode.InvalidDatetimeFormat,
			`could not parse "" as type %s`, "date")
	}
	// 不支持浮点类型数值转换为date类型，在对小数点进行判断的同时，也把科学计数法进行处理
	// 以errFlag标记出错类型，方便统一输出错误信息
	errFlag := 0
	if strings.Contains(s, ".") {
		if strings.Count(s, ".") > 1 {
			errFlag = 1
			goto ErrProcess
		}
		// 有可能的科学计数法
		if strings.Contains(s, "e") || strings.Contains(s, "E") {
			EIndex := int(math.Max(float64(strings.IndexRune(s, 'e')), float64(strings.IndexRune(s, 'E'))))
			if strings.IndexRune(s, '.') > EIndex {
				errFlag = 1
				goto ErrProcess
			}
			if ENum := strings.Count(s, "e") + strings.Count(s, "E"); ENum != 1 {
				errFlag = 1
			} else {
				PowerNumStr := s[EIndex+1:]
				if PowerNum, err := strconv.ParseInt(PowerNumStr, 0, 64); err == nil {
					tempFloat, _ := strconv.ParseFloat(s[:EIndex], 64)
					tempFloat = tempFloat * math.Pow(10, float64(PowerNum))
					if math.IsNaN(tempFloat) || tempFloat <= float64(-99991231) || tempFloat >= float64(99991231) {
						errFlag = 3
					}
					ss := strconv.FormatInt(int64(tempFloat), 10)
					if strings.Contains(ss, ".") {
						errFlag = 1
					} else {
						s = ss
					}
				}
			}
		} else {

			if strings.Count(s, ".") == 1 {
				decimalPart := strings.Split(s, ".")[1]
				if len(decimalPart) == 3 {
					decimalVal, _ := strconv.ParseInt(decimalPart, 10, 64)
					if 1 <= decimalVal && decimalVal <= 366 {
						goto ErrProcess
					}
				}
			}
			errFlag = 2
		}
	}
ErrProcess:
	if errFlag != 0 {
		if olderr != nil {
			return nil, olderr
		}
		switch errFlag {
		case 1:
			{
				return nil, errors.Errorf("please check the input format of %s", s)
			}
		case 2:
			{
				return nil, errors.Errorf("can not convert a float number to type date")
			}
		}
	}
	//•	输入2位以下整数：报错。
	//•	输入3到4位的整数：右端对齐，月份高位补零，默认年份是2000年。
	//•	输入5位整数：第一位表示年份，即200X年，后四位表示月日。
	//•	输入6位整数：根据前两位的数值确定默认年份，后四位为月日。前两位大于等于70视为19XX年，小于70视为20XX年。
	//•	输入7位整数：前三位为年份，即0XXX年，后四位为月日。
	//•	输入8位整数：完整的年月日。
	//•	输入8位以上的整数，需求定为报错。
	//•	以上规则从第一个不是0的数字算起
	//•	TODO 支持科学记数法
	if s[0] == '-' && len(s) > 1 {
		s = s[1:]
	}

	if _, err := ParseDDecimal(s); err != nil {
		return nil, olderr
	}

	switch len(s) {
	case 3, 4:
		{
			if len(s) == 3 {
				s = "0" + s
			}
			s = "2000" + s
		}
	case 5:
		{
			s = "200" + s
		}
	case 6:
		{
			tempYear := s[:2]
			if year, err := strconv.ParseInt(tempYear, 0, 64); err == nil {
				if year >= 70 {
					s = "19" + s
				} else {
					s = "20" + s
				}
			}
		}
	case 7:
		{
			s = "0" + s
		}
	}
	t, err := pgdate.ParseDate(now, 0 /* mode */, s)
	if err != nil {
		return nil, err
	}

	return NewDDateFromTime(t, time.UTC), nil
}

// ResolvedType implements the TypedExpr interface.
func (*DDate) ResolvedType() types.T {
	return types.Date
}

// Compare implements the Datum interface.
func (d *DDate) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	var v DDate
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DDate:
		v = *t
	case *DTimestamp, *DTimestampTZ:
		return compareTimestamps(ctx, d, other)
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return -1
	}
	if v < *d {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (d *DDate) Prev(_ *EvalContext) (Datum, bool) {
	return NewDDate(*d - 1), true
}

// Next implements the Datum interface.
func (d *DDate) Next(_ *EvalContext) (Datum, bool) {
	return NewDDate(*d + 1), true
}

// IsMax implements the Datum interface.
func (d *DDate) IsMax(_ *EvalContext) bool {
	return *d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DDate) IsMin(_ *EvalContext) bool {
	return *d == math.MinInt64
}

// Max implements the Datum interface.
func (d *DDate) Max(_ *EvalContext) (Datum, bool) {
	// TODO(knz): figure a good way to find a maximum.
	return nil, false
}

// Min implements the Datum interface.
func (d *DDate) Min(_ *EvalContext) (Datum, bool) {
	// TODO(knz): figure a good way to find a minimum.
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DDate) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DDate) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lex.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(timeutil.Unix(int64(*d)*SecondsInDay, 0).Format(DateFormat))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DDate) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTime is the time Datum.
type DTime timeofday.TimeOfDay

// MakeDTime creates a DTime from a TimeOfDay.
func MakeDTime(t timeofday.TimeOfDay) *DTime {
	d := DTime(t)
	return &d
}

// ParseDTime parses and returns the *DTime Datum value represented by the
// provided string, or an error if parsing is unsuccessful.
func ParseDTime(ctx ParseTimeContext, s string) (*DTime, error) {
	now := relativeParseTime(ctx)
	t, err := pgdate.ParseTime(now, 0 /* mode */, s)
	if err != nil {
		// Build our own error message to avoid exposing the dummy date.
		return nil, makeParseError(s, types.Time, nil)
	}
	return MakeDTime(timeofday.FromTime(t)), nil
}

// ResolvedType implements the TypedExpr interface.
func (*DTime) ResolvedType() types.T {
	return types.Time
}

// Compare implements the Datum interface.
func (d *DTime) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DDecimal:
		return -t.Compare(ctx, d)
	}
	return compareTimestamps(ctx, d, other)
}

// Prev implements the Datum interface.
func (d *DTime) Prev(_ *EvalContext) (Datum, bool) {
	prev := *d - 1
	return &prev, true
}

// Next implements the Datum interface.
func (d *DTime) Next(_ *EvalContext) (Datum, bool) {
	next := *d + 1
	return &next, true
}

var dTimeMin = MakeDTime(timeofday.Min)
var dTimeMax = MakeDTime(timeofday.Max)

// IsMax implements the Datum interface.
func (d *DTime) IsMax(_ *EvalContext) bool {
	return *d == *dTimeMax
}

// IsMin implements the Datum interface.
func (d *DTime) IsMin(_ *EvalContext) bool {
	return *d == *dTimeMin
}

// Max implements the Datum interface.
func (d *DTime) Max(_ *EvalContext) (Datum, bool) {
	return dTimeMax, true
}

// Min implements the Datum interface.
func (d *DTime) Min(_ *EvalContext) (Datum, bool) {
	return dTimeMin, true
}

// AmbiguousFormat implements the Datum interface.
func (*DTime) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DTime) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lex.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(timeofday.TimeOfDay(*d).String())
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DTime) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTimestamp is the timestamp Datum.
type DTimestamp struct {
	time.Time
}

// MakeDTimestamp creates a DTimestamp with specified precision.
func MakeDTimestamp(t time.Time, precision time.Duration) *DTimestamp {
	return &DTimestamp{Time: t.Round(precision)}
}

// time.Time formats.
const (
	DateFormat = "2006-01-02"

	TimeFormat = "15:04:05.999999"

	TimestampFormat = "2006-01-02 15:04:05.999999"

	// TimestampOutputFormat is used to output all timestamps.
	TimestampOutputFormat = "2006-01-02 15:04:05.999999-07:00"
)

// ParseDTimestamp parses and returns the *DTimestamp Datum value represented by
// the provided string in UTC, or an error if parsing is unsuccessful.
func ParseDTimestamp(ctx ParseTimeContext, s string, precision time.Duration) (*DTimestamp, error) {
	now := relativeParseTime(ctx)
	t, err := pgdate.ParseTimestamp(now, 0 /* mode */, s)
	if err != nil {
		return nil, err
	}
	// Truncate the timezone. DTimestamp doesn't carry its timezone around.
	_, offset := t.Zone()
	t = duration.Add(ctx, t, duration.FromInt64(int64(offset))).UTC()
	return MakeDTimestamp(t, precision), nil
}

// AsDTimestamp attempts to retrieve a DTimestamp from an Expr, returning a DTimestamp and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DTimestamp wrapped by a
// *DOidWrapper is possible.
func AsDTimestamp(e Expr) (DTimestamp, bool) {
	switch t := e.(type) {
	case *DTimestamp:
		return *t, true
	case *DOidWrapper:
		return AsDTimestamp(t.Wrapped)
	}
	return DTimestamp{}, false
}

// MustBeDTimestamp attempts to retrieve a DTimestamp from an Expr, panicking if the
// assertion fails.
func MustBeDTimestamp(e Expr) DTimestamp {
	t, ok := AsDTimestamp(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DTimestamp, found %T", e))
	}
	return t
}

// ResolvedType implements the TypedExpr interface.
func (*DTimestamp) ResolvedType() types.T {
	return types.Timestamp
}

func timeFromDatum(ctx *EvalContext, d Datum) (time.Time, bool) {
	d = UnwrapDatum(ctx, d)
	switch t := d.(type) {
	case *DDate:
		return MakeDTimestampTZFromDate(ctx.GetLocation(), t).Time, true
	case *DTimestampTZ:
		return t.stripTimeZone(ctx).Time, true
	case *DTimestamp:
		return t.Time, true
	case *DTime:
		//return timeofday.TimeOfDay(*t).ToTime(), true
		castTime, _ := PerformCast(ctx, t, coltypes.Timestamp)
		return MustBeDTimestamp(castTime).Time, true
	default:
		return time.Time{}, false
	}
}

func compareTimestamps(ctx *EvalContext, l Datum, r Datum) int {
	lTime, lOk := timeFromDatum(ctx, l)
	rTime, rOk := timeFromDatum(ctx, r)
	if !lOk || !rOk {
		panic(makeUnsupportedComparisonMessage(l, r))
	}
	if lTime.Before(rTime) {
		return -1
	}
	if rTime.Before(lTime) {
		return 1
	}
	return 0
}

// Compare implements the Datum interface.
func (d *DTimestamp) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	return compareTimestamps(ctx, d, other)
}

// Prev implements the Datum interface.
func (d *DTimestamp) Prev(_ *EvalContext) (Datum, bool) {
	return &DTimestamp{Time: d.Add(-time.Microsecond)}, true
}

// Next implements the Datum interface.
func (d *DTimestamp) Next(_ *EvalContext) (Datum, bool) {
	return &DTimestamp{Time: d.Add(time.Microsecond)}, true
}

// IsMax implements the Datum interface.
func (d *DTimestamp) IsMax(_ *EvalContext) bool {
	// Adding 1 overflows to a smaller value
	tNext := d.Time.Add(time.Microsecond)
	return d.After(tNext)
}

// IsMin implements the Datum interface.
func (d *DTimestamp) IsMin(_ *EvalContext) bool {
	// Subtracting 1 underflows to a larger value.
	tPrev := d.Time.Add(-time.Microsecond)
	return d.Before(tPrev)
}

// Min implements the Datum interface.
func (d *DTimestamp) Min(_ *EvalContext) (Datum, bool) {
	// TODO(knz): figure a good way to find a minimum.
	return nil, false
}

// Max implements the Datum interface.
func (d *DTimestamp) Max(_ *EvalContext) (Datum, bool) {
	// TODO(knz): figure a good way to find a minimum.
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DTimestamp) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DTimestamp) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lex.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.UTC().Format(TimestampFormat))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DTimestamp) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTimestampTZ is the timestamp Datum that is rendered with session offset.
type DTimestampTZ struct {
	time.Time
}

// MakeDTimestampTZ creates a DTimestampTZ with specified precision.
func MakeDTimestampTZ(t time.Time, precision time.Duration) *DTimestampTZ {
	return &DTimestampTZ{Time: t.Round(precision)}
}

// MakeDTimestampTZFromDate creates a DTimestampTZ from a DDate.
func MakeDTimestampTZFromDate(loc *time.Location, d *DDate) *DTimestampTZ {
	year, month, day := timeutil.Unix(int64(*d)*SecondsInDay, 0).Date()
	return MakeDTimestampTZ(time.Date(year, month, day, 0, 0, 0, 0, loc), time.Microsecond)
}

// ParseDTimestampTZ parses and returns the *DTimestampTZ Datum value represented by
// the provided string in the provided location, or an error if parsing is unsuccessful.
func ParseDTimestampTZ(
	ctx ParseTimeContext, s string, precision time.Duration,
) (*DTimestampTZ, error) {
	now := relativeParseTime(ctx)
	t, err := pgdate.ParseTimestamp(now, 0 /* mode */, s)
	if err != nil {
		return nil, err
	}
	return MakeDTimestampTZ(t, precision), nil
}

//var dMinTimestampTZ = &DTimestampTZ{}  //not use

// AsDTimestampTZ attempts to retrieve a DTimestampTZ from an Expr, returning a
// DTimestampTZ and a flag signifying whether the assertion was successful. The
// function should be used instead of direct type assertions wherever a
// *DTimestamp wrapped by a *DOidWrapper is possible.
func AsDTimestampTZ(e Expr) (DTimestampTZ, bool) {
	switch t := e.(type) {
	case *DTimestampTZ:
		return *t, true
	case *DOidWrapper:
		return AsDTimestampTZ(t.Wrapped)
	}
	return DTimestampTZ{}, false
}

// MustBeDTimestampTZ attempts to retrieve a DTimestampTZ from an Expr,
// panicking if the assertion fails.
func MustBeDTimestampTZ(e Expr) DTimestampTZ {
	t, ok := AsDTimestampTZ(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DTimestampTZ, found %T", e))
	}
	return t
}

// ResolvedType implements the TypedExpr interface.
func (*DTimestampTZ) ResolvedType() types.T {
	return types.TimestampTZ
}

// Compare implements the Datum interface.
func (d *DTimestampTZ) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	return compareTimestamps(ctx, d, other)
}

// Prev implements the Datum interface.
func (d *DTimestampTZ) Prev(_ *EvalContext) (Datum, bool) {
	return &DTimestampTZ{Time: d.Add(-time.Microsecond)}, true
}

// Next implements the Datum interface.
func (d *DTimestampTZ) Next(_ *EvalContext) (Datum, bool) {
	return &DTimestampTZ{Time: d.Add(time.Microsecond)}, true
}

// IsMax implements the Datum interface.
func (d *DTimestampTZ) IsMax(_ *EvalContext) bool {
	// Adding 1 overflows to a smaller value
	tNext := d.Time.Add(time.Microsecond)
	return d.After(tNext)
}

// IsMin implements the Datum interface.
func (d *DTimestampTZ) IsMin(_ *EvalContext) bool {
	// Subtracting 1 underflows to a larger value.
	tPrev := d.Time.Add(-time.Microsecond)
	return d.Before(tPrev)
}

// Min implements the Datum interface.
func (d *DTimestampTZ) Min(_ *EvalContext) (Datum, bool) {
	// TODO(knz): figure a good way to find a minimum.
	return nil, false
}

// Max implements the Datum interface.
func (d *DTimestampTZ) Max(_ *EvalContext) (Datum, bool) {
	// TODO(knz): figure a good way to find a minimum.
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DTimestampTZ) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DTimestampTZ) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lex.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.Time.Format(TimestampOutputFormat))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DTimestampTZ) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// stripTimeZone removes the time zone from this TimestampTZ. For example, a
// TimestampTZ '2012-01-01 12:00:00 +02:00' would become
//             '2012-01-01 12:00:00'.
func (d *DTimestampTZ) stripTimeZone(ctx *EvalContext) *DTimestamp {
	_, locOffset := d.Time.In(ctx.GetLocation()).Zone()
	newTime := duration.Add(ctx, d.Time.UTC(), duration.FromInt64(int64(locOffset)))
	return MakeDTimestamp(newTime, time.Microsecond)
}

// EvalAtTimeZone evaluates this TimestampTZ as if it were in the supplied
// location, returning a timestamp without a timezone.
func (d *DTimestampTZ) EvalAtTimeZone(ctx *EvalContext, loc *time.Location) *DTimestamp {
	_, locOffset := d.Time.In(loc).Zone()
	newTime := duration.Add(ctx, d.Time.UTC(), duration.FromInt64(int64(locOffset)))
	return MakeDTimestamp(newTime, time.Microsecond)
}

// DInterval is the interval Datum.
type DInterval struct {
	duration.Duration
}

// DurationField is the type of a postgres duration field.
// https://www.postgresql.org/docs/9.6/static/datatype-datetime.html
type DurationField int

// These constants designate the various time parts of an interval.
const (
	_ DurationField = iota
	Year
	Month
	Day
	Hour
	Minute
	Second

	// While not technically part of the SQL standard for intervals, we provide
	// Millisecond as a field to allow code to parse intervals with a default unit
	// of milliseconds, which is useful for some internal use cases like
	// statement_timeout.
	Millisecond
)

// ParseDInterval parses and returns the *DInterval Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDInterval(s string) (*DInterval, error) {
	return ParseDIntervalWithField(s, Second)
}

// truncateDInterval truncates the input DInterval downward to the nearest
// interval quantity specified by the DurationField input.
func truncateDInterval(d *DInterval, field DurationField) {
	switch field {
	case Year:
		d.Duration.Months = d.Duration.Months - d.Duration.Months%12
		d.Duration.Days = 0
		d.Duration.SetNanos(0)
	case Month:
		d.Duration.Days = 0
		d.Duration.SetNanos(0)
	case Day:
		d.Duration.SetNanos(0)
	case Hour:
		d.Duration.SetNanos(d.Duration.Nanos() - d.Duration.Nanos()%time.Hour.Nanoseconds())
	case Minute:
		d.Duration.SetNanos(d.Duration.Nanos() - d.Duration.Nanos()%time.Minute.Nanoseconds())
	case Second:
		// Postgres doesn't truncate to whole seconds.
	}
}

// ParseDIntervalWithField is like ParseDInterval, but it also takes a
// DurationField that both specifies the units for unitless, numeric intervals
// and also specifies the precision of the interval. Any precision in the input
// interval that's higher than the DurationField value will be truncated
// downward.
func ParseDIntervalWithField(s string, field DurationField) (*DInterval, error) {
	d, err := parseDInterval(s, field)
	if err != nil {
		return nil, err
	}
	truncateDInterval(d, field)
	return d, nil
}

func parseDInterval(s string, field DurationField) (*DInterval, error) {
	// At this time the only supported interval formats are:
	// - SQL standard.
	// - Postgres compatible.
	// - iso8601 format (with designators only), see interval.go for
	//   sources of documentation.
	// - Golang time.parseDuration compatible.

	// If it's a blank string, exit early.
	if len(s) == 0 {
		return nil, makeParseError(s, types.Interval, nil)
	}
	if s[0] == 'P' {
		// If it has a leading P we're most likely working with an iso8601
		// interval.
		dur, err := iso8601ToDuration(s)
		if err != nil {
			return nil, makeParseError(s, types.Interval, err)
		}
		return &DInterval{Duration: dur}, nil
	} else if f, err := strconv.ParseFloat(s, 64); err == nil {
		// An interval that's just a number uses the field as its unit.
		// All numbers are rounded down unless the precision is SECOND.
		ret := &DInterval{Duration: duration.Duration{}}
		switch field {
		case Year:
			ret.Months = int64(f) * 12
		case Month:
			ret.Months = int64(f)
		case Day:
			ret.Days = int64(f)
		case Hour:
			ret.SetNanos(time.Hour.Nanoseconds() * int64(f))
		case Minute:
			ret.SetNanos(time.Minute.Nanoseconds() * int64(f))
		case Second:
			ret.SetNanos(int64(float64(time.Second.Nanoseconds()) * f))
		case Millisecond:
			ret.SetNanos(int64(float64(time.Millisecond.Nanoseconds()) * f))
		default:
			panic(fmt.Sprintf("unhandled DurationField constant %d", field))
		}
		return ret, nil
	} else if strings.IndexFunc(s, unicode.IsLetter) == -1 {
		// If it has no letter, then we're most likely working with a SQL standard
		// interval, as both postgres and golang have letter(s) and iso8601 has been tested.
		dur, err := sqlStdToDuration(s)
		if err != nil {
			return nil, makeParseError(s, types.Interval, err)
		}
		return &DInterval{Duration: dur}, nil
	}

	// We're either a postgres string or a Go duration.
	// Our postgres syntax parser also supports golang, so just use that for both.
	dur, err := parseDuration(s)
	if err != nil {
		return nil, makeParseError(s, types.Interval, err)
	}
	return &DInterval{Duration: dur}, nil
}

// ResolvedType implements the TypedExpr interface.
func (*DInterval) ResolvedType() types.T {
	return types.Interval
}

// Compare implements the Datum interface.
func (d *DInterval) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DDecimal:
		return -t.Compare(ctx, d)
	}
	v, ok := UnwrapDatum(ctx, other).(*DInterval)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return d.Duration.Compare(v.Duration)
}

// Prev implements the Datum interface.
func (d *DInterval) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DInterval) Next(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DInterval) IsMax(_ *EvalContext) bool {
	return d.Duration == dMaxInterval.Duration
}

// IsMin implements the Datum interface.
func (d *DInterval) IsMin(_ *EvalContext) bool {
	return d.Duration == dMinInterval.Duration
}

var (
	dMaxInterval = &DInterval{duration.MakeDuration(math.MaxInt64, math.MaxInt64, math.MaxInt64)}
	dMinInterval = &DInterval{duration.MakeDuration(math.MinInt64, math.MinInt64, math.MinInt64)}
)

// Max implements the Datum interface.
func (d *DInterval) Max(_ *EvalContext) (Datum, bool) {
	return dMaxInterval, true
}

// Min implements the Datum interface.
func (d *DInterval) Min(_ *EvalContext) (Datum, bool) {
	return dMinInterval, true
}

// ValueAsString returns the interval as a string (e.g. "1h2m").
func (d *DInterval) ValueAsString() string {
	return d.Duration.String()
}

// AmbiguousFormat implements the Datum interface.
func (*DInterval) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DInterval) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lex.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	d.Duration.Format(&ctx.Buffer)
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DInterval) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DJSON is the JSON Datum.
type DJSON struct{ json.JSON }

// NewDJSON is a helper routine to create a DJSON initialized from its argument.
func NewDJSON(j json.JSON) *DJSON {
	return &DJSON{j}
}

// ParseDJSON takes a string of JSON and returns a DJSON value.
func ParseDJSON(s string) (Datum, error) {
	j, err := json.ParseJSON(s)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse JSON")
	}
	return NewDJSON(j), nil
}

// MakeDJSON returns a JSON value given a Go-style representation of JSON.
// * JSON null is Go `nil`,
// * JSON true is Go `true`,
// * JSON false is Go `false`,
// * JSON numbers are json.Number | int | int64 | float64,
// * JSON string is a Go string,
// * JSON array is a Go []interface{},
// * JSON object is a Go map[string]interface{}.
func MakeDJSON(d interface{}) (Datum, error) {
	j, err := json.MakeJSON(d)
	if err != nil {
		return nil, err
	}
	return &DJSON{j}, nil
}

// AsDJSON attempts to retrieve a *DJSON from an Expr, returning a *DJSON and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DJSON wrapped by a
// *DOidWrapper is possible.
func AsDJSON(e Expr) (*DJSON, bool) {
	switch t := e.(type) {
	case *DJSON:
		return t, true
	case *DOidWrapper:
		return AsDJSON(t.Wrapped)
	}
	return nil, false
}

// MustBeDJSON attempts to retrieve a DJSON from an Expr, panicking if the
// assertion fails.
func MustBeDJSON(e Expr) DJSON {
	i, ok := AsDJSON(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DJSON, found %T", e))
	}
	return *i
}

// AsJSON converts a datum into our standard json representation.
func AsJSON(d Datum) (json.JSON, error) {
	switch t := d.(type) {
	case *DBool:
		return json.FromBool(bool(*t)), nil
	case *DInt:
		return json.FromInt(int(*t)), nil
	case *DFloat:
		return json.FromFloat64(float64(*t))
	case *DDecimal:
		return json.FromDecimal(t.Decimal), nil
	case *DString:
		return json.FromString(string(*t)), nil
	case *DCollatedString:
		return json.FromString(t.Contents), nil
	case *DJSON:
		return t.JSON, nil
	case *DArray:
		builder := json.NewArrayBuilder(t.Len())
		for _, e := range t.Array {
			j, err := AsJSON(e)
			if err != nil {
				return nil, err
			}
			builder.Add(j)
		}
		return builder.Build(), nil
	case *DTuple:
		builder := json.NewObjectBuilder(len(t.D))
		for i, e := range t.D {
			j, err := AsJSON(e)
			if err != nil {
				return nil, err
			}
			builder.Add(fmt.Sprintf("f%d", i+1), j)
		}
		return builder.Build(), nil
	case *DTimestampTZ:
		// Our normal timestamp-formatting code uses a variation on RFC 3339,
		// without the T separator. This causes some compatibility problems
		// with certain JSON consumers, so we'll use an alternate formatting
		// path here to maintain consistency with PostgreSQL.
		return json.FromString(t.Time.Format(time.RFC3339Nano)), nil
	case *DTimestamp:
		// This is RFC3339Nano, but without the TZ fields.
		return json.FromString(t.UTC().Format("2006-01-02T15:04:05.999999999")), nil
	case *DDate, *DUuid, *DOid, *DInterval, *DBytes, *DIPAddr, *DTime, *DBitArray:
		return json.FromString(AsStringWithFlags(t, FmtBareStrings)), nil
	default:
		if d == DNull {
			return json.NullJSONValue, nil
		}

		return nil, pgerror.NewAssertionErrorf("unexpected type %T for AsJSON", d)
	}
}

// ResolvedType implements the TypedExpr interface.
func (*DJSON) ResolvedType() types.T {
	return types.JSON
}

// Compare implements the Datum interface.
func (d *DJSON) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DJSON)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	// No avenue for us to pass up this error here at the moment, but Compare
	// only errors for invalid encoded data.
	// TODO(justin): modify Compare to allow passing up errors.
	c, err := d.JSON.Compare(v.JSON)
	if err != nil {
		panic(err)
	}
	return c
}

// Prev implements the Datum interface.
func (d *DJSON) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DJSON) Next(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DJSON) IsMax(_ *EvalContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DJSON) IsMin(_ *EvalContext) bool {
	return d.JSON == json.NullJSONValue
}

// Max implements the Datum interface.
func (d *DJSON) Max(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DJSON) Min(_ *EvalContext) (Datum, bool) {
	return &DJSON{json.NullJSONValue}, true
}

// AmbiguousFormat implements the Datum interface.
func (*DJSON) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DJSON) Format(ctx *FmtCtx) {
	// TODO(justin): ideally the JSON string encoder should know it needs to
	// escape things to be inside SQL strings in order to avoid this allocation.
	s := d.JSON.String()
	if ctx.flags.HasFlags(fmtRawStrings) {
		ctx.WriteString(s)
	} else if ctx.flags.HasFlags(FmtRawStrings) {
		ctx.WriteByte('\'')
		ctx.WriteString(s)
		ctx.WriteByte('\'')
	} else {
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, s, ctx.flags.EncodeFlags())
	}
}

// Size implements the Datum interface.
// TODO(justin): is this a frequently-called method? Should we be caching the computed size?
func (d *DJSON) Size() uintptr {
	return unsafe.Sizeof(*d) + d.JSON.Size()
}

// DTuple is the tuple Datum.
type DTuple struct {
	D Datums

	// sorted indicates that the values in D are pre-sorted.
	// This is used to accelerate IN comparisons.
	sorted bool

	// typ is the tuple's type.
	//
	// The Types sub-field can be initially uninitialized, and is then
	// populated upon first invocation of ResolvedTypes(). If
	// initialized it must have the same arity as D.
	//
	// The Labels sub-field can be left nil. If populated, it must have
	// the same arity as D.
	//
	// TODO(knz): this ought to be a pointer when #26680 is resolved.
	// Also when it becomes a pointer a nil value should be valid and
	// the tuple type auto-computed when ResolvedType() is called the
	// first time.
	typ types.TTuple
}

// NewDTuple creates a *DTuple with the provided datums. When creating a new
// DTuple with Datums that are known to be sorted in ascending order, chain
// this call with DTuple.SetSorted.
func NewDTuple(typ types.TTuple, d ...Datum) *DTuple {
	return &DTuple{D: d, typ: typ}
}

// NewDTupleWithLen creates a *DTuple with the provided length.
func NewDTupleWithLen(typ types.TTuple, l int) *DTuple {
	return &DTuple{D: make(Datums, l), typ: typ}
}

// AsDTuple attempts to retrieve a *DTuple from an Expr, returning a *DTuple and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DTuple wrapped by a
// *DOidWrapper is possible.
func AsDTuple(e Expr) (*DTuple, bool) {
	switch t := e.(type) {
	case *DTuple:
		return t, true
	case *DOidWrapper:
		return AsDTuple(t.Wrapped)
	}
	return nil, false
}

// ResolvedType implements the TypedExpr interface.
func (d *DTuple) ResolvedType() types.T {
	if d.typ.Types == nil {
		d.typ.Types = make([]types.T, len(d.D))
		for i, v := range d.D {
			d.typ.Types[i] = v.ResolvedType()
		}
	}
	return d.typ
}

// Compare implements the Datum interface.
func (d *DTuple) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DTuple)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	n := len(d.D)
	if n > len(v.D) {
		n = len(v.D)
	}
	for i := 0; i < n; i++ {
		c := d.D[i].Compare(ctx, v.D[i])
		if c != 0 {
			return c
		}
	}
	if len(d.D) < len(v.D) {
		return -1
	}
	if len(d.D) > len(v.D) {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (d *DTuple) Prev(ctx *EvalContext) (Datum, bool) {
	// Note: (a:decimal, b:int, c:int) has a prev value; that's (a, b,
	// c-1). With an exception if c is MinInt64, in which case the prev
	// value is (a, b-1, max(_ *EvalContext)). However, (a:int, b:decimal) does not
	// have a prev value, because decimal doesn't have one.
	//
	// In general, a tuple has a prev value if and only if it ends with
	// zero or more values that are a minimum and a maximum value of the
	// same type exists, and the first element before that has a prev
	// value.
	res := NewDTupleWithLen(d.typ, len(d.D))
	copy(res.D, d.D)
	for i := len(res.D) - 1; i >= 0; i-- {
		if !res.D[i].IsMin(ctx) {
			prevVal, ok := res.D[i].Prev(ctx)
			if !ok {
				return nil, false
			}
			res.D[i] = prevVal
			break
		}
		maxVal, ok := res.D[i].Max(ctx)
		if !ok {
			return nil, false
		}
		res.D[i] = maxVal
	}
	return res, true
}

// Next implements the Datum interface.
func (d *DTuple) Next(ctx *EvalContext) (Datum, bool) {
	// Note: (a:decimal, b:int, c:int) has a next value; that's (a, b,
	// c+1). With an exception if c is MaxInt64, in which case the next
	// value is (a, b+1, min(_ *EvalContext)). However, (a:int, b:decimal) does not
	// have a next value, because decimal doesn't have one.
	//
	// In general, a tuple has a next value if and only if it ends with
	// zero or more values that are a maximum and a minimum value of the
	// same type exists, and the first element before that has a next
	// value.
	res := NewDTupleWithLen(d.typ, len(d.D))
	copy(res.D, d.D)
	for i := len(res.D) - 1; i >= 0; i-- {
		if !res.D[i].IsMax(ctx) {
			nextVal, ok := res.D[i].Next(ctx)
			if !ok {
				return nil, false
			}
			res.D[i] = nextVal
			break
		}
		// TODO(#12022): temporary workaround; see the interface comment.
		res.D[i] = DNull
	}
	return res, true
}

// Max implements the Datum interface.
func (d *DTuple) Max(ctx *EvalContext) (Datum, bool) {
	res := NewDTupleWithLen(d.typ, len(d.D))
	for i, v := range d.D {
		m, ok := v.Max(ctx)
		if !ok {
			return nil, false
		}
		res.D[i] = m
	}
	return res, true
}

// Min implements the Datum interface.
func (d *DTuple) Min(ctx *EvalContext) (Datum, bool) {
	res := NewDTupleWithLen(d.typ, len(d.D))
	for i, v := range d.D {
		m, ok := v.Min(ctx)
		if !ok {
			return nil, false
		}
		res.D[i] = m
	}
	return res, true
}

// IsMax implements the Datum interface.
func (d *DTuple) IsMax(ctx *EvalContext) bool {
	for _, v := range d.D {
		if !v.IsMax(ctx) {
			return false
		}
	}
	return true
}

// IsMin implements the Datum interface.
func (d *DTuple) IsMin(ctx *EvalContext) bool {
	for _, v := range d.D {
		if !v.IsMin(ctx) {
			return false
		}
	}
	return true
}

// AmbiguousFormat implements the Datum interface.
func (*DTuple) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (d *DTuple) Format(ctx *FmtCtx) {
	if ctx.HasFlags(fmtPgwireFormat) {
		d.pgwireFormat(ctx)
		return
	}

	showLabels := len(d.typ.Labels) > 0
	if showLabels {
		ctx.WriteByte('(')
	}
	ctx.WriteByte('(')
	comma := ""
	parsable := ctx.HasFlags(FmtParsable)
	for i, v := range d.D {
		ctx.WriteString(comma)
		ctx.FormatNode(v)
		if parsable && (v == DNull) && len(d.typ.Types) > i {
			coltype, err := coltypes.DatumTypeToColumnType(d.typ.Types[i])
			// If err != nil, we can't determine the column type to write this
			// annotation. This will primarily happen if the Tuple has types.Unknown
			// for this slot. Somebody else will provide an error message in this
			// case, if necessary, so just skip the annotation and continue.
			if err == nil {
				ctx.WriteString("::")
				if ctx.flags.HasFlags(FmtVisableType) {
					ctx.WriteString(coltype.ColumnType())
				} else {
					coltype.Format(&ctx.Buffer, ctx.flags.EncodeFlags())
				}
			}
		}
		comma = ", "
	}
	if len(d.D) == 1 {
		// Ensure the pretty-printed 1-value tuple is not ambiguous with
		// the equivalent value enclosed in grouping parentheses.
		ctx.WriteByte(',')
	}
	ctx.WriteByte(')')
	if showLabels {
		ctx.WriteString(" AS ")
		comma := ""
		for i := range d.typ.Labels {
			ctx.WriteString(comma)
			ctx.FormatNode((*Name)(&d.typ.Labels[i]))
			comma = ", "
		}
		ctx.WriteByte(')')
	}
}

// Sorted returns true if the tuple is known to be sorted (and contains no
// NULLs).
func (d *DTuple) Sorted() bool {
	return d.sorted
}

// SetSorted sets the sorted flag on the DTuple. This should be used when a
// DTuple is known to be sorted based on the datums added to it.
func (d *DTuple) SetSorted() *DTuple {
	if d.ContainsNull() {
		// A DTuple that contains a NULL (see ContainsNull) cannot be marked as sorted.
		return d
	}
	d.sorted = true
	return d
}

// AssertSorted asserts that the DTuple is sorted.
func (d *DTuple) AssertSorted() {
	if !d.sorted {
		panic(fmt.Sprintf("expected sorted tuple, found %#v", d))
	}
}

// SearchSorted searches the tuple for the target Datum, returning an int with
// the same contract as sort.Search and a boolean flag signifying whether the datum
// was found. It assumes that the DTuple is sorted and panics if it is not.
//
// The target Datum cannot be NULL or a DTuple that contains NULLs (we cannot
// binary search in this case; for example `(1, NULL) IN ((1, 2), ..)` needs to
// be
func (d *DTuple) SearchSorted(ctx *EvalContext, target Datum) (int, bool) {
	d.AssertSorted()
	if target == DNull {
		panic(fmt.Sprintf("NULL target (d: %s)", d))
	}
	if t, ok := target.(*DTuple); ok && t.ContainsNull() {
		panic(fmt.Sprintf("target containing NULLs: %#v (d: %s)", target, d))
	}
	i := sort.Search(len(d.D), func(i int) bool {
		return d.D[i].Compare(ctx, target) >= 0
	})
	found := i < len(d.D) && d.D[i].Compare(ctx, target) == 0
	return i, found
}

// Normalize sorts and uniques the datum tuple.
func (d *DTuple) Normalize(ctx *EvalContext) {
	d.sort(ctx)
	d.makeUnique(ctx)
}

func (d *DTuple) sort(ctx *EvalContext) {
	if !d.sorted {
		sort.Slice(d.D, func(i, j int) bool {
			return d.D[i].Compare(ctx, d.D[j]) < 0
		})
		d.SetSorted()
	}
}

func (d *DTuple) makeUnique(ctx *EvalContext) {
	n := 0
	for i := 0; i < len(d.D); i++ {
		if n == 0 || d.D[n-1].Compare(ctx, d.D[i]) < 0 {
			d.D[n] = d.D[i]
			n++
		}
	}
	d.D = d.D[:n]
}

// Size implements the Datum interface.
func (d *DTuple) Size() uintptr {
	sz := unsafe.Sizeof(*d)
	for _, e := range d.D {
		dsz := e.Size()
		sz += dsz
	}
	return sz
}

// ContainsNull returns true if the tuple contains NULL, possibly nested inside
// other tuples. For example, all the following tuples contain NULL:
//  (1, 2, NULL)
//  ((1, 1), (2, NULL))
//  (((1, 1), (2, 2)), ((3, 3), (4, NULL)))
func (d *DTuple) ContainsNull() bool {
	for _, r := range d.D {
		if r == DNull {
			return true
		}
		if t, ok := r.(*DTuple); ok {
			if t.ContainsNull() {
				return true
			}
		}
	}
	return false
}

//DNullExtern set to dNull
type DNullExtern = dNull

type dNull struct{}

// ResolvedType implements the TypedExpr interface.
func (dNull) ResolvedType() types.T {
	return types.Unknown
}

// Compare implements the Datum interface.
func (d dNull) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		return 0
	}
	return -1
}

// Prev implements the Datum interface.
func (d dNull) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d dNull) Next(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (dNull) IsMax(_ *EvalContext) bool {
	return true
}

// IsMin implements the Datum interface.
func (dNull) IsMin(_ *EvalContext) bool {
	return true
}

// Max implements the Datum interface.
func (dNull) Max(_ *EvalContext) (Datum, bool) {
	return DNull, true
}

// Min implements the Datum interface.
func (dNull) Min(_ *EvalContext) (Datum, bool) {
	return DNull, true
}

// AmbiguousFormat implements the Datum interface.
func (dNull) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (dNull) Format(ctx *FmtCtx) {
	if ctx.HasFlags(fmtPgwireFormat) {
		// NULL sub-expressions in pgwire text values are represented with
		// the empty string.
		return
	}
	ctx.WriteString("NULL")
}

// Size implements the Datum interface.
func (d dNull) Size() uintptr {
	return unsafe.Sizeof(d)
}

// DArray is the array Datum. Any Datum inserted into a DArray are treated as
// text during serialization.
type DArray struct {
	ParamTyp types.T
	Array    Datums
	// HasNulls is set to true if any of the datums within the array are null.
	// This is used in the binary array serialization format.
	HasNulls bool
}

// NewDArray returns a DArray containing elements of the specified type.
func NewDArray(paramTyp types.T) *DArray {
	return &DArray{ParamTyp: paramTyp}
}

// AsDArray attempts to retrieve a *DArray from an Expr, returning a *DArray and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DArray wrapped by a
// *DOidWrapper is possible.
func AsDArray(e Expr) (*DArray, bool) {
	switch t := e.(type) {
	case *DArray:
		return t, true
	case *DOidWrapper:
		return AsDArray(t.Wrapped)
	}
	return nil, false
}

// MustBeDArray attempts to retrieve a *DArray from an Expr, panicking if the
// assertion fails.
func MustBeDArray(e Expr) *DArray {
	i, ok := AsDArray(e)
	if !ok {
		panic(pgerror.NewAssertionErrorf("expected *DArray, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (d *DArray) ResolvedType() types.T {
	return types.TArray{Typ: d.ParamTyp}
}

// Compare implements the Datum interface.
func (d *DArray) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DArray)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	n := d.Len()
	if n > v.Len() {
		n = v.Len()
	}
	for i := 0; i < n; i++ {
		c := d.Array[i].Compare(ctx, v.Array[i])
		if c != 0 {
			return c
		}
	}
	if d.Len() < v.Len() {
		return -1
	}
	if d.Len() > v.Len() {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (d *DArray) Prev(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DArray) Next(_ *EvalContext) (Datum, bool) {
	a := DArray{ParamTyp: d.ParamTyp, Array: make(Datums, d.Len()+1)}
	copy(a.Array, d.Array)
	a.Array[len(a.Array)-1] = DNull
	return &a, true
}

// Max implements the Datum interface.
func (d *DArray) Max(_ *EvalContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DArray) Min(_ *EvalContext) (Datum, bool) {
	return &DArray{ParamTyp: d.ParamTyp}, true
}

// IsMax implements the Datum interface.
func (d *DArray) IsMax(_ *EvalContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DArray) IsMin(_ *EvalContext) bool {
	return d.Len() == 0
}

// AmbiguousFormat implements the Datum interface.
func (d *DArray) AmbiguousFormat() bool {
	// The type of the array is ambiguous if it is empty; when serializing we need
	// to annotate it with the type.
	return len(d.Array) == 0
}

// Format implements the NodeFormatter interface.
func (d *DArray) Format(ctx *FmtCtx) {
	if ctx.HasFlags(fmtPgwireFormat) {
		d.pgwireFormat(ctx)
		return
	}

	ctx.WriteString("ARRAY[")
	comma := ""
	for _, v := range d.Array {
		ctx.WriteString(comma)
		ctx.FormatNode(v)
		comma = ","
	}
	ctx.WriteByte(']')
}

const maxArrayLength = math.MaxInt32

var arrayTooLongError = pgerror.NewErrorf(
	pgcode.DataException, "ARRAYs can be at most 2^31-1 elements long")

// Validate checks that the given array is valid,
// for example, that it's not too big.
func (d *DArray) Validate() error {
	if d.Len() > maxArrayLength {
		return arrayTooLongError
	}
	return nil
}

// Len returns the length of the Datum array.
func (d *DArray) Len() int {
	return len(d.Array)
}

// Size implements the Datum interface.
func (d *DArray) Size() uintptr {
	sz := unsafe.Sizeof(*d)
	for _, e := range d.Array {
		dsz := e.Size()
		sz += dsz
	}
	return sz
}

var errNonHomogeneousArray = pgerror.NewError(pgcode.ArraySubscript, "multidimensional arrays must have array expressions with matching dimensions")

// Append appends a Datum to the array, whose parameterized type must be
// consistent with the type of the Datum.
func (d *DArray) Append(v Datum) error {
	if v != DNull && !d.ParamTyp.Equivalent(v.ResolvedType()) {
		return pgerror.NewAssertionErrorf("cannot append %s to array containing %s", d.ParamTyp,
			v.ResolvedType())
	}
	if d.Len() >= maxArrayLength {
		return arrayTooLongError
	}
	if _, ok := d.ParamTyp.(types.TArray); ok {
		if v == DNull {
			return errNonHomogeneousArray
		}
		if d.Len() > 0 {
			prevItem := d.Array[d.Len()-1]
			if prevItem == DNull {
				return errNonHomogeneousArray
			}
			expectedLen := MustBeDArray(prevItem).Len()
			if MustBeDArray(v).Len() != expectedLen {
				return errNonHomogeneousArray
			}
		}
	}
	if v == DNull {
		d.HasNulls = true
	}
	d.Array = append(d.Array, v)
	return d.Validate()
}

// DOid is the Postgres OID datum. It can represent either an OID type or any
// of the reg* types, such as regproc or regclass.
type DOid struct {
	// A DOid embeds a DInt, the underlying integer OID for this OID datum.
	DInt
	// semanticType indicates the particular variety of OID this datum is, whether raw
	// oid or a reg* type.
	semanticType *coltypes.TOid
	// name is set to the resolved name of this OID, if available.
	name string
}

// MakeDOid is a helper routine to create a DOid initialized from a DInt.
func MakeDOid(d DInt) DOid {
	return DOid{DInt: d, semanticType: coltypes.Oid, name: ""}
}

// NewDOid is a helper routine to create a *DOid initialized from a DInt.
func NewDOid(d DInt) *DOid {
	oid := MakeDOid(d)
	return &oid
}

// NewDOidWithName is a helper routine to create a *DOid initialized from a DInt
// and a string.
func NewDOidWithName(d DInt, typ *coltypes.TOid, name string) *DOid {
	return &DOid{
		DInt:         d,
		semanticType: typ,
		name:         name,
	}
}

// AsRegProc changes the input DOid into a regproc with the given name and
// returns it.
func (d *DOid) AsRegProc(name string) *DOid {
	d.name = name
	d.semanticType = coltypes.RegProc
	return d
}

// AmbiguousFormat implements the Datum interface.
func (*DOid) AmbiguousFormat() bool { return true }

// Compare implements the Datum interface.
func (d *DOid) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := UnwrapDatum(ctx, other).(*DOid)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if d.DInt < v.DInt {
		return -1
	}
	if d.DInt > v.DInt {
		return 1
	}
	return 0
}

// Format implements the Datum interface.
func (d *DOid) Format(ctx *FmtCtx) {
	if d.semanticType == coltypes.Oid || d.name == "" {
		// If we call FormatNode directly when the disambiguateDatumTypes flag
		// is set, then we get something like 123:::INT:::OID. This is the
		// important flag set by FmtParsable which is supposed to be
		// roundtrippable. Since in this branch, a DOid is a thin wrapper around
		// a DInt, I _think_ it's correct to just delegate to the DInt's Format.
		d.DInt.Format(ctx)
	} else if ctx.HasFlags(fmtDisambiguateDatumTypes) {
		ctx.WriteString("zbdb_internal.create_")
		ctx.WriteString(d.semanticType.Name)
		ctx.WriteByte('(')
		d.DInt.Format(ctx)
		ctx.WriteByte(',')
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, d.name, lex.EncNoFlags)
		ctx.WriteByte(')')
	} else {
		// This is used to print the name of pseudo-procedures in e.g.
		// pg_catalog.pg_type.typinput
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, d.name, lex.EncBareStrings)
	}
}

// IsMax implements the Datum interface.
func (d *DOid) IsMax(ctx *EvalContext) bool { return d.DInt.IsMax(ctx) }

// IsMin implements the Datum interface.
func (d *DOid) IsMin(ctx *EvalContext) bool { return d.DInt.IsMin(ctx) }

// Next implements the Datum interface.
func (d *DOid) Next(ctx *EvalContext) (Datum, bool) {
	next, ok := d.DInt.Next(ctx)
	return &DOid{*next.(*DInt), d.semanticType, ""}, ok
}

// Prev implements the Datum interface.
func (d *DOid) Prev(ctx *EvalContext) (Datum, bool) {
	prev, ok := d.DInt.Prev(ctx)
	return &DOid{*prev.(*DInt), d.semanticType, ""}, ok
}

// ResolvedType implements the Datum interface.
func (d *DOid) ResolvedType() types.T {
	return coltypes.TOidToType(d.semanticType)
}

// Size implements the Datum interface.
func (d *DOid) Size() uintptr { return unsafe.Sizeof(*d) }

// Max implements the Datum interface.
func (d *DOid) Max(ctx *EvalContext) (Datum, bool) {
	max, ok := d.DInt.Max(ctx)
	return &DOid{*max.(*DInt), d.semanticType, ""}, ok
}

// Min implements the Datum interface.
func (d *DOid) Min(ctx *EvalContext) (Datum, bool) {
	min, ok := d.DInt.Min(ctx)
	return &DOid{*min.(*DInt), d.semanticType, ""}, ok
}

// DOidWrapper is a Datum implementation which is a wrapper around a Datum, allowing
// custom Oid values to be attached to the Datum and its types.T (see tOidWrapper).
// The reason the Datum type was introduced was to permit the introduction of Datum
// types with new Object IDs while maintaining identical behavior to current Datum
// types. Specifically, it obviates the need to:
// - define a new tree.Datum type.
// - define a new types.T type.
// - support operations and functions for the new types.T.
// - support mixed-type operations between the new types.T and the old types.T.
//
// Instead, DOidWrapper allows a standard Datum to be wrapped with a new Oid.
// This approach provides two major advantages:
// - performance of the existing Datum types are not affected because they
//   do not need to have custom oid.Oids added to their structure.
// - the introduction of new Datum aliases is straightforward and does not require
//   additions to typing rules or type-dependent evaluation behavior.
//
// Types that currently benefit from DOidWrapper are:
// - DName => DOidWrapper(*DString, oid.T_name)
//
type DOidWrapper struct {
	Wrapped Datum
	Oid     oid.Oid
}

// wrapWithOid wraps a Datum with a custom Oid.
func wrapWithOid(d Datum, oid oid.Oid) Datum {
	switch v := d.(type) {
	case nil:
		return nil
	case *DInt:
	case *DString:
	case *DArray:
	case dNull, *DOidWrapper:
		panic(pgerror.NewAssertionErrorf("cannot wrap %T with an Oid", v))
	default:
		// Currently only *DInt, *DString, *DArray are hooked up to work with
		// *DOidWrapper. To support another base Datum type, replace all type
		// assertions to that type with calls to functions like AsDInt and
		// MustBeDInt.
		panic(pgerror.NewAssertionErrorf("unsupported Datum type passed to wrapWithOid: %T", d))
	}
	return &DOidWrapper{
		Wrapped: d,
		Oid:     oid,
	}
}

// UnwrapDatum returns the base Datum type for a provided datum, stripping
// an *DOidWrapper if present. This is useful for cases like type switches,
// where type aliases should be ignored.
func UnwrapDatum(evalCtx *EvalContext, d Datum) Datum {
	if w, ok := d.(*DOidWrapper); ok {
		return w.Wrapped
	}
	if p, ok := d.(*Placeholder); ok && evalCtx != nil && evalCtx.HasPlaceholders() {
		ret, err := p.Eval(evalCtx)
		if err != nil {
			// If we fail to evaluate the placeholder, it's because we don't have
			// a placeholder available. Just return the placeholder and someone else
			// will handle this problem.
			return d
		}
		return ret
	}
	return d
}

// ResolvedType implements the TypedExpr interface.
func (d *DOidWrapper) ResolvedType() types.T {
	return types.WrapTypeWithOid(d.Wrapped.ResolvedType(), d.Oid)
}

// Compare implements the Datum interface.
func (d *DOidWrapper) Compare(ctx *EvalContext, other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	if v, ok := other.(*DOidWrapper); ok {
		return d.Wrapped.Compare(ctx, v.Wrapped)
	}
	return d.Wrapped.Compare(ctx, other)
}

// Prev implements the Datum interface.
func (d *DOidWrapper) Prev(ctx *EvalContext) (Datum, bool) {
	prev, ok := d.Wrapped.Prev(ctx)
	return wrapWithOid(prev, d.Oid), ok
}

// Next implements the Datum interface.
func (d *DOidWrapper) Next(ctx *EvalContext) (Datum, bool) {
	next, ok := d.Wrapped.Next(ctx)
	return wrapWithOid(next, d.Oid), ok
}

// IsMax implements the Datum interface.
func (d *DOidWrapper) IsMax(ctx *EvalContext) bool {
	return d.Wrapped.IsMax(ctx)
}

// IsMin implements the Datum interface.
func (d *DOidWrapper) IsMin(ctx *EvalContext) bool {
	return d.Wrapped.IsMin(ctx)
}

// Max implements the Datum interface.
func (d *DOidWrapper) Max(ctx *EvalContext) (Datum, bool) {
	max, ok := d.Wrapped.Max(ctx)
	return wrapWithOid(max, d.Oid), ok
}

// Min implements the Datum interface.
func (d *DOidWrapper) Min(ctx *EvalContext) (Datum, bool) {
	min, ok := d.Wrapped.Min(ctx)
	return wrapWithOid(min, d.Oid), ok
}

// AmbiguousFormat implements the Datum interface.
func (d *DOidWrapper) AmbiguousFormat() bool {
	return d.Wrapped.AmbiguousFormat()
}

// Format implements the NodeFormatter interface.
func (d *DOidWrapper) Format(ctx *FmtCtx) {
	// Custom formatting based on d.OID could go here.
	ctx.FormatNode(d.Wrapped)
}

// Size implements the Datum interface.
func (d *DOidWrapper) Size() uintptr {
	return unsafe.Sizeof(*d) + d.Wrapped.Size()
}

// AmbiguousFormat implements the Datum interface.
func (d *Placeholder) AmbiguousFormat() bool {
	return true
}

func (d *Placeholder) mustGetValue(ctx *EvalContext) Datum {
	e, ok := ctx.Placeholders.Value(d.Idx)
	if !ok {
		panic("fail")
	}
	out, err := e.Eval(ctx)
	if err != nil {
		panic(fmt.Sprintf("fail %s", err))
	}
	return out
}

// Compare implements the Datum interface.
func (d *Placeholder) Compare(ctx *EvalContext, other Datum) int {
	return d.mustGetValue(ctx).Compare(ctx, other)
}

// Prev implements the Datum interface.
func (d *Placeholder) Prev(ctx *EvalContext) (Datum, bool) {
	return d.mustGetValue(ctx).Prev(ctx)
}

// IsMin implements the Datum interface.
func (d *Placeholder) IsMin(ctx *EvalContext) bool {
	return d.mustGetValue(ctx).IsMin(ctx)
}

// Next implements the Datum interface.
func (d *Placeholder) Next(ctx *EvalContext) (Datum, bool) {
	return d.mustGetValue(ctx).Next(ctx)
}

// IsMax implements the Datum interface.
func (d *Placeholder) IsMax(ctx *EvalContext) bool {
	return d.mustGetValue(ctx).IsMax(ctx)
}

// Max implements the Datum interface.
func (d *Placeholder) Max(ctx *EvalContext) (Datum, bool) {
	return d.mustGetValue(ctx).Max(ctx)
}

// Min implements the Datum interface.
func (d *Placeholder) Min(ctx *EvalContext) (Datum, bool) {
	return d.mustGetValue(ctx).Min(ctx)
}

// Size implements the Datum interface.
func (d *Placeholder) Size() uintptr {
	panic("shouldn't get called")
}

// NewDNameFromDString is a helper routine to create a *DName (implemented as
// a *DOidWrapper) initialized from an existing *DString.
func NewDNameFromDString(d *DString) Datum {
	return wrapWithOid(d, oid.T_name)
}

// NewDName is a helper routine to create a *DName (implemented as a *DOidWrapper)
// initialized from a string.
func NewDName(d string) Datum {
	return NewDNameFromDString(NewDString(d))
}

// NewDIntVectorFromDArray is a helper routine to create a *DIntVector
// (implemented as a *DOidWrapper) initialized from an existing *DArray.
func NewDIntVectorFromDArray(d *DArray) Datum {
	return wrapWithOid(d, oid.T_int2vector)
}

// NewDOidVectorFromDArray is a helper routine to create a *DOidVector
// (implemented as a *DOidWrapper) initialized from an existing *DArray.
func NewDOidVectorFromDArray(d *DArray) Datum {
	return wrapWithOid(d, oid.T_oidvector)
}

// DatumTypeSize returns a lower bound on the total size of a Datum
// of the given type in bytes, including memory that is
// pointed at (even if shared between Datum instances) but excluding
// allocation overhead.
//
// The second argument indicates whether data of this type have different
// sizes.
//
// It holds for every Datum d that d.Size() >= DatumSize(d.ResolvedType())
func DatumTypeSize(t types.T) (uintptr, bool) {
	// The following are composite types.
	switch ty := t.(type) {
	case types.TOid:
		// Note: we have multiple Type instances of tOid (TypeOid,
		// TypeRegClass, etc). Instead of listing all of them in
		// baseDatumTypeSizes below, we use a single case here.
		return unsafe.Sizeof(DInt(0)), fixedSize

	case types.TOidWrapper:
		return DatumTypeSize(ty.T)

	case types.TCollatedString:
		return unsafe.Sizeof(DCollatedString{"", "", nil}), variableSize

	case types.TTuple:
		sz := uintptr(0)
		variable := false
		for _, typ := range ty.Types {
			typsz, typvariable := DatumTypeSize(typ)
			sz += typsz
			variable = variable || typvariable
		}
		return sz, variable

	case types.TArray, types.TEnum, types.TtSet:
		// TODO(jordan,justin): This seems suspicious.
		return unsafe.Sizeof(DString("")), variableSize
	}

	// All the primary types have fixed size information.
	if bSzInfo, ok := baseDatumTypeSizes[t]; ok {
		return bSzInfo.sz, bSzInfo.variable
	}

	panic(fmt.Sprintf("unknown type: %T", t))
}

const (
	fixedSize    = false
	variableSize = true
)

var baseDatumTypeSizes = map[types.T]struct {
	sz       uintptr
	variable bool
}{
	types.Unknown:     {unsafe.Sizeof(dNull{}), fixedSize},
	types.Bool:        {unsafe.Sizeof(DBool(false)), fixedSize},
	types.BitArray:    {unsafe.Sizeof(DBitArray{}), variableSize},
	types.Int:         {unsafe.Sizeof(DInt(0)), fixedSize},
	types.Int2:        {unsafe.Sizeof(DInt(0)), fixedSize},
	types.Int4:        {unsafe.Sizeof(DInt(0)), fixedSize},
	types.Float:       {unsafe.Sizeof(DFloat(0.0)), fixedSize},
	types.Float4:      {unsafe.Sizeof(DFloat(0.0)), fixedSize},
	types.Decimal:     {unsafe.Sizeof(DDecimal{}), variableSize},
	types.String:      {unsafe.Sizeof(DString("")), variableSize},
	types.Bytes:       {unsafe.Sizeof(DBytes("")), variableSize},
	types.Date:        {unsafe.Sizeof(DDate(0)), fixedSize},
	types.Time:        {unsafe.Sizeof(DTime(0)), fixedSize},
	types.Timestamp:   {unsafe.Sizeof(DTimestamp{}), fixedSize},
	types.TimestampTZ: {unsafe.Sizeof(DTimestampTZ{}), fixedSize},
	types.Interval:    {unsafe.Sizeof(DInterval{}), fixedSize},
	types.JSON:        {unsafe.Sizeof(DJSON{}), variableSize},
	types.UUID:        {unsafe.Sizeof(DUuid{}), fixedSize},
	types.INet:        {unsafe.Sizeof(DIPAddr{}), fixedSize},
	// TODO(jordan,justin): This seems suspicious.
	types.Any:    {unsafe.Sizeof(DString("")), variableSize},
	types.Void:   {unsafe.Sizeof(DString("")), variableSize},
	types.Cursor: {unsafe.Sizeof(DString("")), variableSize},
}
