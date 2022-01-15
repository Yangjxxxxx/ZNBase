// Copyright 2016  The Cockroach Authors.
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
	"fmt"
	"go/constant"
	"go/token"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

// Constant is an constant literal expression which may be resolved to more than one type.
type Constant interface {
	Expr
	// AvailableTypes returns the ordered set of types that the Constant is able to
	// be resolved into. The order of the type slice provides a notion of precedence,
	// with the first element in the ordering being the Constant's "natural type".
	AvailableTypes(useOrigin bool) []types.T
	// DesirableTypes returns the ordered set of types that the constant would
	// prefer to be resolved into. As in AvailableTypes, the order of the returned
	// type slice provides a notion of precedence, with the first element in the
	// ordering being the Constant's "natural type." The function is meant to be
	// differentiated from AvailableTypes in that it will exclude certain types
	// that are possible, but not desirable.
	//
	// An example of this is a floating point numeric constant without a value
	// past the decimal point. It is possible to resolve this constant as a
	// decimal, but it is not desirable.
	DesirableTypes(useOrigin bool) []types.T
	// ResolveAsType resolves the Constant as the Datum type specified, or returns an
	// error if the Constant could not be resolved as that type. The method should only
	// be passed a type returned from AvailableTypes and should never be called more than
	// once for a given Constant.
	ResolveAsType(*SemaContext, types.T, bool) (Datum, error)
}

// NoColumnIdx is a special value that can be used as a "column index" to
// indicate that the column is not present.
const NoColumnIdx = -1

var _ Constant = &NumVal{}
var _ Constant = &StrVal{}

func isConstant(expr Expr) bool {
	_, ok := expr.(Constant)
	return ok
}

func typeCheckConstant(
	c Constant, ctx *SemaContext, desired types.T, useOrigin bool,
) (ret TypedExpr, err error) {
	avail := c.AvailableTypes(useOrigin)
	if desired != types.Any {
		for _, typ := range avail {
			if desired.Equivalent(typ) {
				return c.ResolveAsType(ctx, desired, useOrigin)
			}
		}
	}

	// If a numeric constant will be promoted to a DECIMAL because it was out
	// of range of an INT, but an INT is desired, throw an error here so that
	// the error message specifically mentions the overflow.
	if desired.FamilyEqual(types.Int) || desired.FamilyEqual(types.Int2) || desired.FamilyEqual(types.Int4) {
		if n, ok := c.(*NumVal); ok {
			_, err := n.AsInt64()
			if err == nil {
				panic(fmt.Sprintf("unexpected error %v", err))
			} else {
				switch err.Error() {
				case "numeric constant out of int64 range":
					return nil, err
				case "cannot represent numeric constant as an int":
				default:
					panic(fmt.Sprintf("unexpected error %v", err))
				}
			}
		}
	}

	natural := avail[0]
	return c.ResolveAsType(ctx, natural, useOrigin)
}

func naturalConstantType(c Constant) types.T {
	return c.AvailableTypes(false)[0]
}

// canConstantBecome returns whether the provided Constant can become resolved
// as the provided type.
func canConstantBecome(c Constant, typ types.T, useOrigin bool) bool {
	avail := c.AvailableTypes(useOrigin)
	for _, availTyp := range avail {
		if availTyp.Equivalent(typ) {
			return true
		}
	}
	return false
}

var (
	dateLikeTypes = []types.T{types.Date, types.Timestamp, types.Decimal, types.Float, types.Int, types.String}
)

// AvailableTypes implements the Constant interface.
func (expr *DDate) AvailableTypes(useOrigin bool) []types.T {
	return dateLikeTypes
}

// DesirableTypes implements the Constant interface.
func (expr *DDate) DesirableTypes(useOrigin bool) []types.T {
	return dateLikeTypes
}

// ResolveAsType implements the Constant interface.
func (expr *DDate) ResolveAsType(ctx *SemaContext, typ types.T, useOrigin bool) (Datum, error) {
	str := expr.String()
	temp := strings.Split(str, "-")
	stri := strings.Join(temp, "")
	switch typ {
	case types.Date:
		return expr, nil
	case types.String:
		res := DString(str)
		return &res, nil
	case types.Decimal:
		var dd DDecimal
		err := dd.SetString(stri)
		if err != nil {
			return nil, err
		}
		return &dd, nil
	case types.Float, types.Float4:
		f, _ := strconv.ParseFloat(stri, 64)
		res := DFloat(f)
		return &res, nil
	case types.Int, types.Int2, types.Int4:
		I, _ := strconv.ParseInt(stri, 0, 64)
		res := DInt(I)
		return &res, nil
	default:
		if _, ok := typ.(types.TtSet); ok {
			return NewDString(str), nil
		}
		return nil, pgerror.NewAssertionErrorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

var (
	boolLikeTypes = []types.T{types.Bool, types.Int, types.Decimal, types.Float, types.BitArray, types.String}
)

// AvailableTypes implements the Constant interface.
func (expr *DBool) AvailableTypes(useOrigin bool) []types.T {
	return boolLikeTypes
}

// DesirableTypes implements the Constant interface.
func (expr *DBool) DesirableTypes(useOrigin bool) []types.T {
	return boolLikeTypes
}

// ResolveAsType implements the Constant interface.
func (expr *DBool) ResolveAsType(ctx *SemaContext, typ types.T, useOrigin bool) (Datum, error) {
	switch typ {
	case types.Bool:
		return expr, nil
	case types.String:
		var s string
		if *expr == true {
			s = "true"
		} else {
			s = "false"
		}
		DS := DString(s)
		return &DS, nil
	case types.Int, types.Int2, types.Int4:
		var i int
		if *expr == true {
			i = 1
		} else {
			i = 0
		}
		DI := DInt(i)
		return &DI, nil
	case types.Float, types.Float4:
		var f float64
		if *expr == true {
			f = 1
		} else {
			f = 0
		}
		DF := DFloat(f)
		return &DF, nil
	case types.Decimal:
		var dd DDecimal
		if *expr == true {
			_ = dd.SetString("1")
			return &dd, nil
		}
		_ = dd.SetString("0")
		return &dd, nil
	case types.BitArray:
		return expr, nil
		// if *expr {
		// 	return NewDBitArrayFromInt(1, 1)
		// }
		// return NewDBitArrayFromInt(0, 1)

	default:
		if _, ok := typ.(types.TtSet); ok {
			var s string
			if *expr == true {
				s = "true"
			} else {
				s = "false"
			}
			return NewDString(s), nil
		}
		return nil, pgerror.NewAssertionErrorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

// NumVal represents a constant numeric value.
type NumVal struct {
	constant.Value
	// Negative is the sign bit to add to any interpretation of the
	// Value or OrigString fields.
	Negative bool

	// We preserve the "original" string representation (before
	// folding). This should remain sign-less.
	OrigString string

	// The following fields are used to avoid allocating Datums on type resolution.
	resInt     DInt
	resFloat   DFloat
	resDecimal DDecimal
	resString  DString
}

var _ Constant = &NumVal{}

// Format implements the NodeFormatter interface.
func (expr *NumVal) Format(ctx *FmtCtx) {
	s := expr.OrigString
	if s == "" {
		s = expr.Value.String()
	}
	if expr.Negative {
		ctx.WriteByte('-')
	}
	ctx.WriteString(s)
}

// canBeInt64 checks if it's possible for the value to become an int64:
//  1   = yes
//  1.0 = yes
//  1.1 = no
//  123...overflow...456 = no
func (expr *NumVal) canBeInt64() bool {
	_, err := expr.AsInt64()
	return err == nil
}

// ShouldBeInt64 checks if the value naturally is an int64:
//  1   = yes
//  1.0 = no
//  1.1 = no
//  123...overflow...456 = no
func (expr *NumVal) ShouldBeInt64() bool {
	return expr.Kind() == constant.Int && expr.canBeInt64()
}

// These errors are statically allocated, because they are returned in the
// common path of AsInt64.
var errConstNotInt = pgerror.NewError(pgcode.NumericValueOutOfRange, "cannot represent numeric constant as an int")
var errConstOutOfRange64 = pgerror.NewError(pgcode.NumericValueOutOfRange, "numeric constant out of int64 range")
var errConstOutOfRange32 = pgerror.NewError(pgcode.NumericValueOutOfRange, "numeric constant out of int32 range")

// AsInt64 returns the value as a 64-bit integer if possible, or returns an
// error if not possible. The method will set expr.resInt to the value of
// this int64 if it is successful, avoiding the need to call the method again.
func (expr *NumVal) AsInt64() (int64, error) {
	intVal, ok := expr.asConstantInt()
	if !ok {
		return 0, pgerror.NewError(errConstNotInt.Code, errConstNotInt.Message)
	}
	i, exact := constant.Int64Val(intVal)
	if !exact {
		return 0, pgerror.NewError(errConstOutOfRange64.Code, errConstOutOfRange64.Message)
	}
	expr.resInt = DInt(i)
	return i, nil
}

// AsInt32 returns the value as 32-bit integer if possible, or returns
// an error if not possible. The method will set expr.resInt to the
// value of this int32 if it is successful, avoiding the need to call
// the method again.
func (expr *NumVal) AsInt32() (int32, error) {
	intVal, ok := expr.asConstantInt()
	if !ok {
		return 0, pgerror.NewError(errConstNotInt.Code, errConstNotInt.Message)
	}
	i, exact := constant.Int64Val(intVal)
	if !exact {
		return 0, pgerror.NewError(errConstOutOfRange32.Code, errConstOutOfRange32.Message)
	}
	if i > math.MaxInt32 || i < math.MinInt32 {
		return 0, pgerror.NewError(errConstOutOfRange32.Code, errConstOutOfRange32.Message)
	}
	expr.resInt = DInt(i)
	return int32(i), nil
}

// asValue returns the value as a constant numerical value, with the proper sign
// as given by expr.Negative.
func (expr *NumVal) asValue() constant.Value {
	v := expr.Value
	if expr.Negative {
		v = constant.UnaryOp(token.SUB, v, 0)
	}
	return v
}

// asConstantInt returns the value as an constant.Int if possible, along
// with a flag indicating whether the conversion was possible.
// The result contains the proper sign as per expr.Negative.
func (expr *NumVal) asConstantInt() (constant.Value, bool) {
	v := expr.asValue()
	intVal := constant.ToInt(v)
	if intVal.Kind() == constant.Int {
		return intVal, true
	}
	return nil, false
}

var (
	intLikeTypes     = []types.T{types.Int, types.Oid}
	decimalLikeTypes = []types.T{types.Decimal, types.Float}

	// NumValAvailIntegerOrigin is the set of Origin available integer types.
	NumValAvailIntegerOrigin = append(intLikeTypes, decimalLikeTypes...)
	// NumValAvailDecimalNoFractionOrigin is the set of Origin available integral numeric types.
	NumValAvailDecimalNoFractionOrigin = append(decimalLikeTypes, intLikeTypes...)
	// NumValAvailDecimalWithFractionOrigin is the set of Origin available fractional numeric types.
	NumValAvailDecimalWithFractionOrigin = decimalLikeTypes

	// NumValAvailInteger is the set of available integer types.
	NumValAvailInteger = []types.T{types.Int, types.Decimal, types.Float, types.Oid, types.Date, types.Time, types.Timestamp, types.TimestampTZ, types.Bool, types.BitArray, types.String}
	// NumValAvailDecimalNoFraction is the set of available integral numeric types.
	NumValAvailDecimalNoFraction = []types.T{types.Decimal, types.Float, types.Int, types.Oid, types.Date, types.Time, types.Timestamp, types.TimestampTZ, types.Bool, types.BitArray, types.String}
	// NumValAvailDecimalWithFraction is the set of available fractional numeric types
	NumValAvailDecimalWithFraction = NumValAvailDecimalNoFraction
)

// AvailableTypes implements the Constant interface.
func (expr *NumVal) AvailableTypes(useOrigin bool) []types.T {
	if useOrigin {
		switch {
		case expr.canBeInt64():
			if expr.Kind() == constant.Int {
				return NumValAvailIntegerOrigin
			}
			return NumValAvailDecimalNoFractionOrigin
		default:
			return NumValAvailDecimalWithFractionOrigin
		}
	}
	switch {
	case expr.canBeInt64():
		if expr.Kind() == constant.Int {
			return NumValAvailInteger
		}
		return NumValAvailDecimalNoFraction
	default:
		return NumValAvailDecimalWithFraction
	}

}

// DesirableTypes implements the Constant interface.
func (expr *NumVal) DesirableTypes(useOrigin bool) []types.T {
	if useOrigin {
		if expr.ShouldBeInt64() {
			return NumValAvailIntegerOrigin
		}
		return NumValAvailDecimalWithFractionOrigin
	}
	if expr.ShouldBeInt64() {
		return NumValAvailInteger
	}
	return NumValAvailDecimalWithFraction
}

// ResolveAsType implements the Constant interface.
func (expr *NumVal) ResolveAsType(ctx *SemaContext, typ types.T, useOrigin bool) (Datum, error) {
	switch typ {
	case types.Date:
		//data, ok := constant.CanBeTime(expr.Value)
		//if !ok {
		//	return nil,  pgerror.NewAssertionErrorf("could not resolve %T %v into a Date", expr, expr)
		//}

		// data, ok, isInt := JudgeInt(expr.Value)
		// if !ok || !isInt {
		// 	return nil, errors.Errorf("could not resolve %T %v into a Date", expr, expr)
		// }
		// s := strconv.FormatInt(data, 10)
		s := expr.OrigString
		if s == "" {
			s = expr.ExactString()
		}
		if strings.Contains(s, ".") {
			tempStr := strings.Split(s, ".")
			if len(tempStr) == 2 {
				if decimalPart, err := strconv.ParseInt(tempStr[1], 10, 64); err == nil {
					if decimalPart == 0 {
						s = tempStr[0]
					}
				}
			}
		}
		expr.resString = DString(s)
		if len(s) > 8 {
			s = s[:8] + " " + s[8:]
		}
		datum, err := ParseDDate(ctx, s)
		return datum, err
	case types.Time:
		s := expr.OrigString
		if s == "" {
			s = expr.ExactString()
		}
		return ParseDTime(ctx, s)
	case types.Bool:
		var DB *DBool
		f, _ := constant.Float64Val(expr.Value)
		if f == 0 {
			DB = DBoolFalse
		} else {
			DB = DBoolTrue
		}
		return DB, nil
	case types.Int, types.Int2, types.Int4:
		return numResolveAsType(expr, "int", useOrigin)
		// We may have already set expr.resInt in AsInt64.
		// if expr.resInt == 0 {
		// 	if _, err := expr.AsInt64(); err != nil {
		// 		return nil, err
		// 	}
		// }
		// return &expr.resInt, nil
	case types.Float, types.Float4:
		return numResolveAsType(expr, "float", useOrigin)
		// f, _ := constant.Float64Val(expr.Value)
		// if expr.Negative {
		// 	f = -f
		// }
		// expr.resFloat = DFloat(f)
		// return &expr.resFloat, nil
	case types.Decimal, types.String, types.Timestamp, types.TimestampTZ:
		dd := &expr.resDecimal
		s := expr.OrigString
		if s == "" {
			// TODO(nvanbenschoten): We should propagate width through constant folding so that we
			// can control precision on folded values as well.
			s = expr.ExactString()
		}
		if idx := strings.IndexRune(s, '/'); idx != -1 {
			// Handle constant.ratVal, which will return a rational string
			// like 6/7. If only we could call big.Rat.FloatString() on it...
			num, den := s[:idx], s[idx+1:]
			if err := dd.SetString(num); err != nil {
				return nil, errors.Wrapf(err, "could not evaluate numerator of %v as Datum type DDecimal "+
					"from string %q", expr, num)
			}
			// TODO(nvanbenschoten): Should we try to avoid this allocation?
			denDec, err := ParseDDecimal(den)
			if err != nil {
				return nil, errors.Wrapf(err, "could not evaluate denominator %v as Datum type DDecimal "+
					"from string %q", expr, den)
			}
			if cond, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, &denDec.Decimal); err != nil {
				if cond.DivisionByZero() {
					return nil, ErrDivByZero
				}
				return nil, err
			}
		} else {
			if err := dd.SetString(s); err != nil {
				err = dd.SetString(expr.Value.String())
				if err != nil {
					return nil, errors.Wrapf(err, "could not evaluate %v as Datum type DDecimal from "+
						"string %q", expr, s)
				}
			}
		}
		if !dd.IsZero() {
			// Negative zero does not exist for DECIMAL, in that case we ignore the
			// sign. Otherwise XOR the signs of the expr and the decimal value
			// contained in the expr, since the negative may have been folded into the
			// inner decimal.
			dd.Negative = dd.Negative != expr.Negative
		}
		if typ == types.Decimal {
			return dd, nil
		}
		numStr := dd.String()
		if typ == types.TimestampTZ || typ == types.Timestamp {
			if len(numStr) > 8 {
				numStr = numStr[:8] + " " + numStr[8:]
			}
			if typ == types.Timestamp {
				return ParseDTimestamp(ctx, numStr, time.Microsecond)
			}
			if typ == types.TimestampTZ {
				return ParseDTimestampTZ(ctx, numStr, time.Microsecond)
			}
		}
		expr.resString = DString(numStr)
		return &expr.resString, nil
	case types.Oid,
		types.RegClass,
		types.RegNamespace,
		types.RegProc,
		types.RegProcedure,
		types.RegType:

		d, err := expr.ResolveAsType(ctx, types.Int, useOrigin)
		if err != nil {
			return nil, err
		}
		oid := NewDOid(*d.(*DInt))
		oid.semanticType = coltypes.OidTypeToColType(typ)
		return oid, nil
	case types.BitArray:
		// we can not get the width info, so just make the conversion through cast after,
		// here simple return a int value
		return numResolveAsType(expr, "int", useOrigin)
	default:
		if _, ok := typ.(types.TtSet); ok {
			return NewDString(expr.OrigString), nil
		}
		return nil, errors.Errorf("could not resolve %T %v into a %T", expr, expr, typ)
		// return nil, pgerror.NewAssertionErrorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

//GetBitCount Get Bit Count
func GetBitCount(intVal int64) int {
	bitCount := 0
	intValTemp := intVal
	if intValTemp < 0 {
		bitCount = 64
	} else {
		for ; intValTemp != 0; intValTemp = intValTemp >> 1 {
			bitCount++
		}
		if bitCount == 0 {
			bitCount++
		}
	}
	return bitCount
}

func numResolveAsType(expr *NumVal, flagStr string, useOrigin bool) (Datum, error) {
	if flagStr == "int" {
		resI, err := expr.AsInt64()
		if err == nil {
			return NewDInt(DInt(resI)), nil
		}
		if useOrigin {
			return nil, err
		}
	}
	if flagStr == "float" && useOrigin {
		f, _ := constant.Float64Val(expr.Value)
		if expr.Negative {
			f = -f
		}
		expr.resFloat = DFloat(f)
		return &expr.resFloat, nil
	}

	dd := DDecimal{}
	s := expr.OrigString
	if s == "" {
		s = expr.ExactString()
	}
	if err := dd.SetString(s); err == nil {
		if !dd.IsZero() {
			dd.Negative = expr.Negative
		}
		switch flagStr {
		case "int":
			var limitDecimal = &apd.Decimal{}
			var negativeLimitDecimal = &apd.Decimal{}
			limitDecimal.SetInt64(math.MaxInt64)
			negativeLimitDecimal.SetInt64(math.MinInt64)
			if dd.Decimal.Cmp(limitDecimal) > 0 || dd.Decimal.Cmp(negativeLimitDecimal) < 0 {
				return nil, pgerror.NewError(errConstOutOfRange64.Code, errConstOutOfRange64.Message)
			}
			var integeDecimal = &apd.Decimal{}
			_, err := DecimalCtx.RoundToIntegralValue(integeDecimal, &dd.Decimal)
			if err != nil {
				return nil, err
			}
			resI, err := integeDecimal.Int64()
			if err != nil {
				return nil, err
			}
			return NewDInt(DInt(resI)), nil
		case "float":
			dtof, _ := dd.Float64()
			if expr.Negative && dd.IsZero() {
				dtof = -dtof
			}
			resF := DFloat(dtof)
			return &resF, nil
		}
	}
	f, _ := constant.Float64Val(expr.Value)
	if expr.Negative {
		f = -f
	}
	switch flagStr {
	case "int":
		expr.resInt = DInt(math.Round(f))
		return &expr.resInt, nil
	case "float":
		expr.resFloat = DFloat(f)
		return &expr.resFloat, nil
	default:
		return nil, pgerror.NewAssertionErrorf("could not resolve %T %v into a %T", expr, expr, flagStr)
	}
}

func intersectTypeSlices(xs, ys []types.T) (out []types.T) {
	for _, x := range xs {
		for _, y := range ys {
			if x == y {
				out = append(out, x)
			}
		}
	}
	return out
}

// commonConstantType returns the most constrained type which is mutually
// resolvable between a set of provided constants.
//
// The function takes a slice of Exprs and indexes, but expects all the indexed
// Exprs to wrap a Constant. The reason it does no take a slice of Constants
// instead is to avoid forcing callers to allocate separate slices of Constant.
func commonConstantType(vals []Expr, idxs []int) (types.T, bool) {
	var candidates []types.T
	// due to we enlarge the scope of constants, so the original commonConstantType not work well
	// as before, so add a slice to help judge commonType about constant
	firstTypeRecord := make([]types.T, 0)
	for _, i := range idxs {
		availableTypes := vals[i].(Constant).DesirableTypes(false)
		had := false
		for _, t := range firstTypeRecord {
			if t == availableTypes[0] {
				had = true
				break
			}
		}
		if !had {
			firstTypeRecord = append(firstTypeRecord, availableTypes[0])
		}
		if candidates == nil {
			candidates = availableTypes
		} else {
			candidates = intersectTypeSlices(candidates, availableTypes)
		}
	}

	if len(candidates) > 0 {
		if candidates[0] == types.Int || candidates[0] == types.Int2 || candidates[0] == types.Int4 {
			for _, t := range firstTypeRecord {
				if t == types.Decimal || t == types.String {
					return types.Decimal, true
				}
			}
		}
		return candidates[0], true
	}
	return nil, false
}

func commonConstantTypeAgain(
	overloads []OverloadImpl, exprs []Expr, cidxs []int,
) ([]types.T, bool) {
	ok := false
	res := make([]types.T, len(cidxs))
	restemp := make([]types.T, len(cidxs))
	typnum := make([]int, len(cidxs))
	typnumtemp := make([]int, len(cidxs))
	for ovn, overload := range overloads {
		if overload.params().Length() != len(cidxs) {
			break
		}
		numpar := len(cidxs)
		for _, i := range cidxs {
			tlist := exprs[i].(Constant).DesirableTypes(false)
			if i >= len(cidxs) {
				break
			}
			for k, t := range tlist {
				if overload.params().GetAt(i).Equivalent(t) {
					if ovn == 0 || k < typnum[i] {
						restemp[i] = t
						typnumtemp[i] = k
						numpar--
						break
					}
				}
			}
		}
		if numpar == 0 {
			for _, i := range cidxs {
				res[i] = restemp[i]
				typnum[i] = typnumtemp[i]
				ok = true
			}
		}
	}
	if ok {
		return res, ok
	}
	return nil, ok
}

// StrVal represents a constant string value.
type StrVal struct {
	// We could embed a constant.Value here (like NumVal) and use the stringVal implementation,
	// but that would have extra overhead without much of a benefit. However, it would make
	// constant folding (below) a little more straightforward.
	s string

	// scannedAsBytes is true iff the input syntax was using b'...' or
	// x'....'. If false, the string is guaranteed to be a valid UTF-8
	// sequence.
	scannedAsBytes bool

	// The following fields are used to avoid allocating Datums on type resolution.
	resString DString
	resBytes  DBytes
}

// NewStrVal constructs a StrVal instance. This is used during
// parsing when interpreting a token of type SCONST, i.e. *not* using
// the b'...' or x'...' syntax.
func NewStrVal(s string) *StrVal {
	return &StrVal{s: s}
}

// NewBytesStrVal constructs a StrVal instance suitable as byte array.
// This is used during parsing when interpreting a token of type BCONST,
// i.e. using the b'...' or x'...' syntax.
func NewBytesStrVal(s string) *StrVal {
	return &StrVal{s: s, scannedAsBytes: true}
}

// RawString retrieves the underlying string of the StrVal.
func (expr *StrVal) RawString() string {
	return expr.s
}

// Format implements the NodeFormatter interface.
func (expr *StrVal) Format(ctx *FmtCtx) {
	buf, f := &ctx.Buffer, ctx.flags
	if expr.scannedAsBytes {
		lex.EncodeSQLBytes(buf, expr.s)
	} else {
		lex.EncodeSQLStringWithFlags(buf, expr.s, f.EncodeFlags())
	}
}

var (
	// StrValAvailAllParsable is the set of parsable string types.
	StrValAvailAllParsable = []types.T{
		types.String,
		types.Bytes,
		types.Date,
		types.Time,
		types.Timestamp,
		types.TimestampTZ,
		types.Interval,
		types.UUID,
		types.Bool,
		types.Decimal,
		types.Float,
		types.Int,
		types.INet,
		types.JSON,
		types.BitArray,
		types.FamCollatedString,
	}
	// StrValAvailBytes is the set of types convertible to byte array.
	StrValAvailBytes = []types.T{types.Bytes, types.UUID, types.String}
)

// AvailableTypes implements the Constant interface.
//
// To fully take advantage of literal type inference, this method would
// determine exactly which types are available for a given string. This would
// entail attempting to parse the literal string as a date, a timestamp, an
// interval, etc. and having more fine-grained results than StrValAvailAllParsable.
// However, this is not feasible in practice because of the associated parsing
// overhead.
//
// Conservative approaches like checking the string's length have been investigated
// to reduce ambiguity and improve type inference in some cases. When doing so, the
// length of the string literal was compared against all valid date and timestamp
// formats to quickly gain limited insight into whether parsing the string as the
// respective datum types could succeed. The hope was to eliminate impossibilities
// and constrain the returned type sets as much as possible. Unfortunately, two issues
// were found with this approach:
// - date and timestamp formats do not always imply a fixed-length valid input. For
//   instance, timestamp formats that take fractional seconds can successfully parse
//   inputs of varied length.
// - the set of date and timestamp formats are not disjoint, which means that ambiguity
//   can not be eliminated when inferring the type of string literals that use these
//   shared formats.
// While these limitations still permitted improved type inference in many cases, they
// resulted in behavior that was ultimately incomplete, resulted in unpredictable levels
// of inference, and occasionally failed to eliminate ambiguity. Further heuristics could
// have been applied to improve the accuracy of the inference, like checking that all
// or some characters were digits, but it would not have circumvented the fundamental
// issues here. Fully parsing the literal into each type would be the only way to
// concretely avoid the issue of unpredictable inference behavior.
func (expr *StrVal) AvailableTypes(useOrigin bool) []types.T {
	if expr.scannedAsBytes {
		return StrValAvailBytes
	}
	return StrValAvailAllParsable
}

// DesirableTypes implements the Constant interface.
func (expr *StrVal) DesirableTypes(useOrigin bool) []types.T {
	return expr.AvailableTypes(useOrigin)
}

// ResolveAsType implements the Constant interface.
func (expr *StrVal) ResolveAsType(ctx *SemaContext, typ types.T, useOrigin bool) (Datum, error) {
	if expr.scannedAsBytes {
		// We're looking at typing a byte literal constant into some value type.
		switch typ {
		case types.Bytes:
			expr.resBytes = DBytes(expr.s)
			return &expr.resBytes, nil
		case types.UUID:
			return ParseDUuidFromBytes([]byte(expr.s))
		case types.String:
			expr.resString = DString(expr.s)
			return &expr.resString, nil
		}
		return nil, pgerror.NewAssertionErrorf("attempt to type byte array literal to %T", typ)
	}

	// Typing a string literal constant into some value type.
	switch typ {
	case types.String:
		expr.resString = DString(expr.s)
		return &expr.resString, nil
	case types.Name:
		expr.resString = DString(expr.s)
		return NewDNameFromDString(&expr.resString), nil
	case types.Bytes:
		return ParseDByte(expr.s)
	}

	if _, ok := typ.(types.TCollatedString); ok {
		return &DCollatedString{Contents: expr.s, Locale: typ.(types.TCollatedString).Locale, Key: make([]byte, 0)}, nil
	}

	if ctx != nil && ctx.IVarContainer != nil {
		if tempScopeIdentify, ok := ctx.IVarContainer.(ScopeIdentify); ok {
			if _, isInsert := tempScopeIdentify.GetScopeStmt().(*Insert); isInsert {
				useOrigin = true
			}
			if _, isUpdate := tempScopeIdentify.GetScopeStmt().(*Update); isUpdate {
				useOrigin = true
			}
		} else if ctx.IVarContainer.GetForInsertOrUpdate() {
			useOrigin = true
		}
	}
	datum, err := parseStringAs(types.UnwrapType(typ), expr.s, ctx, useOrigin)
	if datum == nil && err == nil {
		return nil, pgerror.NewAssertionErrorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
	return datum, err
}

type constantFolderVisitor struct{}

var _ Visitor = constantFolderVisitor{}

func (constantFolderVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	return true, expr
}

var unaryOpToToken = map[UnaryOperator]token.Token{
	UnaryMinus: token.SUB,
}
var unaryOpToTokenIntOnly = map[UnaryOperator]token.Token{
	UnaryComplement: token.XOR,
}
var binaryOpToToken = map[BinaryOperator]token.Token{
	Plus:  token.ADD,
	Minus: token.SUB,
	Mult:  token.MUL,
	Div:   token.QUO,
}
var binaryOpToTokenIntOnly = map[BinaryOperator]token.Token{
	FloorDiv: token.QUO_ASSIGN,
	Mod:      token.REM,
	Bitand:   token.AND,
	Bitor:    token.OR,
	Bitxor:   token.XOR,
}
var comparisonOpToToken = map[ComparisonOperator]token.Token{
	EQ: token.EQL,
	NE: token.NEQ,
	LT: token.LSS,
	LE: token.LEQ,
	GT: token.GTR,
	GE: token.GEQ,
}

func (constantFolderVisitor) VisitPost(expr Expr) (retExpr Expr) {
	defer func() {
		// go/constant operations can panic for a number of reasons (like division
		// by zero), but it's difficult to preemptively detect when they will. It's
		// safest to just recover here without folding the expression and let
		// normalization or evaluation deal with error handling.
		if r := recover(); r != nil {
			retExpr = expr
		}
	}()
	switch t := expr.(type) {
	case *ParenExpr:
		switch cv := t.Expr.(type) {
		case *NumVal, *StrVal:
			return cv
		}
	case *UnaryExpr:
		switch cv := t.Expr.(type) {
		case *NumVal:
			if tok, ok := unaryOpToToken[t.Operator]; ok {
				return &NumVal{Value: constant.UnaryOp(tok, cv.asValue(), 0)}
			}
			if token, ok := unaryOpToTokenIntOnly[t.Operator]; ok {
				if intVal, ok := cv.asConstantInt(); ok {
					return &NumVal{Value: constant.UnaryOp(token, intVal, 0)}
				}
			}
		}
	case *BinaryExpr:
		switch l := t.Left.(type) {
		case *NumVal:
			if r, ok := t.Right.(*NumVal); ok {
				if token, ok := binaryOpToToken[t.Operator]; ok {
					return &NumVal{Value: constant.BinaryOp(l.asValue(), token, r.asValue())}
				}
				if token, ok := binaryOpToTokenIntOnly[t.Operator]; ok {
					if lInt, ok := l.asConstantInt(); ok {
						if rInt, ok := r.asConstantInt(); ok {
							return &NumVal{Value: constant.BinaryOp(lInt, token, rInt)}
						}
					}
				}
				// Explicitly ignore shift operators so the expression is evaluated as a
				// non-const. This is because 1 << 63 as a 64-bit int (which is a negative
				// number due to 2s complement) is different than 1 << 63 as constant,
				// which is positive.
			}
		case *StrVal:
			if r, ok := t.Right.(*StrVal); ok {
				switch t.Operator {
				case Concat:
					// When folding string-like constants, if either was a byte
					// array literal, the result is also a byte literal.
					return &StrVal{s: l.s + r.s, scannedAsBytes: l.scannedAsBytes || r.scannedAsBytes}
				}
			}
		}
	case *ComparisonExpr:
		switch l := t.Left.(type) {
		case *NumVal:
			if r, ok := t.Right.(*NumVal); ok {
				if token, ok := comparisonOpToToken[t.Operator]; ok {
					return MakeDBool(DBool(constant.Compare(l.asValue(), token, r.asValue())))
				}
			}
		case *StrVal:
			// ComparisonExpr folding for String-like constants is not significantly different
			// from constant evalutation during normalization (because both should be exact,
			// unlike numeric comparisons). Still, folding these comparisons when possible here
			// can reduce the amount of work performed during type checking, can reduce necessary
			// allocations, and maintains symmetry with numeric constants.
			if r, ok := t.Right.(*StrVal); ok {
				switch t.Operator {
				case EQ:
					return MakeDBool(DBool(l.s == r.s))
				case NE:
					return MakeDBool(DBool(l.s != r.s))
				case LT:
					return MakeDBool(DBool(l.s < r.s))
				case LE:
					return MakeDBool(DBool(l.s <= r.s))
				case GT:
					return MakeDBool(DBool(l.s > r.s))
				case GE:
					return MakeDBool(DBool(l.s >= r.s))
				}
			}
		}
	}
	return expr
}

// FoldConstantLiterals folds all constant literals using exact arithmetic.
//
// TODO(nvanbenschoten): Can this visitor be preallocated (like normalizeVisitor)?
// TODO(nvanbenschoten): Investigate normalizing associative operations to group
//     constants together and permit further numeric constant folding.
func FoldConstantLiterals(expr Expr) (Expr, error) {
	v := constantFolderVisitor{}
	expr, _ = WalkExpr(v, expr)
	return expr, nil
}
