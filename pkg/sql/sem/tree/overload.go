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
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util/mon"
)

// SpecializedVectorizedBuiltin is used to map overloads
// to the vectorized operator that is specific to
// that implementation of the builtin function.
type SpecializedVectorizedBuiltin int

// TODO (rohany): What is the best place to put this list?
// I want to put it in builtins or exec, but those create an import
// cycle with exec. tree is imported by both of them, so
// this package seems like a good place to do it.

// Keep this list alphabetized so that it is easy to manage.
const (
	_ SpecializedVectorizedBuiltin = iota
	SubstringStringIntInt
)

// Overload is one of the overloads of a built-in function.
// Each FunctionDefinition may contain one or more overloads.
type Overload struct {
	Types      TypeList
	ReturnType ReturnTyper

	// PreferredOverload determines overload resolution as follows.
	// When multiple overloads are eligible based on types even after all of of
	// the heuristics to pick one have been used, if one of the overloads is a
	// Overload with the `PreferredOverload` flag set to true it can be selected
	// rather than returning a no-such-method error.
	// This should generally be avoided -- avoiding introducing ambiguous
	// overloads in the first place is a much better solution -- and only done
	// after consultation with @knz @nvanbenschoten.
	PreferredOverload bool

	// Info is a description of the function, which is surfaced on the ZNBaseDB
	// docs site on the "Functions and Operators" page. Descriptions typically use
	// third-person with the function as an implicit subject (e.g. "Calculates
	// infinity"), but should focus more on ease of understanding so other structures
	// might be more appropriate.
	Info string

	AggregateFunc func([]types.T, *EvalContext, Datums, *mon.BoundAccount) AggregateFunc
	WindowFunc    func([]types.T, *EvalContext, *mon.BoundAccount) WindowFunc
	Fn            func(*EvalContext, Datums) (Datum, error)
	Generator     GeneratorFactory
	Name          string
	// counter, if non-nil, should be incremented upon successful
	// type check of expressions using this overload.
	counter telemetry.Counter

	// SpecializedVecBuiltin is used to let the vectorized engine
	// know when an Overload has a specialized vectorized operator.
	SpecializedVecBuiltin SpecializedVectorizedBuiltin

	// RunPriv tag that current user has privilege on the function or not.
	RunPriv error
}

// params implements the OverloadImpl interface.
func (b Overload) params() TypeList { return b.Types }

// returnType implements the OverloadImpl interface.
func (b Overload) returnType() ReturnTyper { return b.ReturnType }

// preferred implements the OverloadImpl interface.
func (b Overload) preferred() bool { return b.PreferredOverload }

// FixedReturnType returns a fixed type that the function returns, returning Any
// if the return type is based on the function's arguments.
func (b Overload) FixedReturnType() types.T {
	if b.ReturnType == nil {
		return nil
	}
	return returnTypeToFixedType(b.ReturnType)
}

// Signature returns a human-readable signature.
// If simplify is bool, tuple-returning functions with just
// 1 tuple element unwrap the return type in the signature.
func (b Overload) Signature(simplify bool) string {
	retType := b.FixedReturnType()
	if simplify {
		if t, ok := retType.(types.TTuple); ok && len(t.Types) == 1 {
			retType = t.Types[0]
		}
	}
	return fmt.Sprintf("(%s) -> %s", b.Types.String(), retType)
}

// OverloadImpl is an implementation of an overloaded function. It provides
// access to the parameter type list  and the return type of the implementation.
//
// This is a more general type than Overload defined above, because it also
// works with the built-in binary and unary operators.
type OverloadImpl interface {
	params() TypeList
	returnType() ReturnTyper
	// allows manually resolving preference between multiple compatible overloads
	preferred() bool
}

var _ OverloadImpl = &Overload{}
var _ OverloadImpl = &UnaryOp{}
var _ OverloadImpl = &BinOp{}

// GetParamsAndReturnType gets the parameters and return type of an
// OverloadImpl.
func GetParamsAndReturnType(impl OverloadImpl) (TypeList, ReturnTyper) {
	return impl.params(), impl.returnType()
}

// TypeList is a list of types representing a function parameter list.
type TypeList interface {
	// Match checks if all types in the TypeList match the corresponding elements in types.
	Match(types []types.T) bool
	// MatchAt checks if the parameter type at index i of the TypeList matches type typ.
	// In all implementations, types.Null will match with each parameter type, allowing
	// NULL values to be used as arguments.
	MatchAt(typ types.T, i int) bool
	// matchLen checks that the TypeList can support l parameters.
	MatchLen(l int) bool
	// getAt returns the type at the given index in the TypeList, or nil if the TypeList
	// cannot have a parameter at index i.
	GetAt(i int) types.T
	// Length returns the number of types in the list
	Length() int
	// Types returns a realized copy of the list. variadic lists return a list of size one.
	Types() []types.T
	// String returns a human readable signature
	String() string
}

var _ TypeList = ArgTypes{}
var _ TypeList = HomogeneousType{}
var _ TypeList = VariadicType{}

// ArgType is element of ArgTypes
type ArgType struct {
	Name string
	Typ  types.T
}

// ArgTypes is very similar to ArgTypes except it allows keeping a string
// name for each argument as well and using those when printing the
// human-readable signature.
type ArgTypes []ArgType

// Match is part of the TypeList interface.
func (a ArgTypes) Match(types []types.T) bool {
	if len(types) != len(a) {
		return false
	}
	for i := range types {
		if !a.MatchAt(types[i], i) {
			return false
		}
	}
	return true
}

// MatchAt is part of the TypeList interface.
func (a ArgTypes) MatchAt(typ types.T, i int) bool {
	// The parameterized types for Tuples are checked in the type checking
	// routines before getting here, so we only need to check if the argument
	// type is a types.FamTuple below. This allows us to avoid defining overloads
	// for types.FamTuple{}, types.FamTuple{types.Any}, types.FamTuple{types.Any, types.Any}, etc.
	// for Tuple operators.
	if typ.FamilyEqual(types.FamTuple) {
		typ = types.FamTuple
	}
	return i < len(a) && (typ == types.Unknown || a[i].Typ.Equivalent(typ))
}

// MatchLen is part of the TypeList interface.
func (a ArgTypes) MatchLen(l int) bool {
	return len(a) == l
}

// GetAt is part of the TypeList interface.
func (a ArgTypes) GetAt(i int) types.T {
	return a[i].Typ
}

// Length is part of the TypeList interface.
func (a ArgTypes) Length() int {
	return len(a)
}

// Types is part of the TypeList interface.
func (a ArgTypes) Types() []types.T {
	n := len(a)
	ret := make([]types.T, n)
	for i, s := range a {
		ret[i] = s.Typ
	}
	return ret
}

func (a ArgTypes) String() string {
	var s bytes.Buffer
	for i, arg := range a {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(arg.Name)
		s.WriteString(": ")
		s.WriteString(arg.Typ.String())
	}
	return s.String()
}

// HomogeneousType is a TypeList implementation that accepts any arguments, as
// long as all are the same type or NULL. The homogeneous constraint is enforced
// in typeCheckOverloadedExprs.
type HomogeneousType struct{}

// Match is part of the TypeList interface.
func (HomogeneousType) Match(types []types.T) bool {
	return true
}

// MatchAt is part of the TypeList interface.
func (HomogeneousType) MatchAt(typ types.T, i int) bool {
	return true
}

// MatchLen is part of the TypeList interface.
func (HomogeneousType) MatchLen(l int) bool {
	return true
}

// GetAt is part of the TypeList interface.
func (HomogeneousType) GetAt(i int) types.T {
	return types.Any
}

// Length is part of the TypeList interface.
func (HomogeneousType) Length() int {
	return 1
}

// Types is part of the TypeList interface.
func (HomogeneousType) Types() []types.T {
	return []types.T{types.Any}
}

func (HomogeneousType) String() string {
	return "anyelement..."
}

// VariadicType is a TypeList implementation which accepts a fixed number of
// arguments at the beginning and an arbitrary number of homogenous arguments
// at the end.
type VariadicType struct {
	FixedTypes []types.T
	VarType    types.T
}

// Match is part of the TypeList interface.
func (v VariadicType) Match(types []types.T) bool {
	for i := range types {
		if !v.MatchAt(types[i], i) {
			return false
		}
	}
	return true
}

// MatchAt is part of the TypeList interface.
func (v VariadicType) MatchAt(typ types.T, i int) bool {
	if i < len(v.FixedTypes) {
		return typ == types.Unknown || v.FixedTypes[i].Equivalent(typ)
	}
	return typ == types.Unknown || v.VarType.Equivalent(typ)
}

// MatchLen is part of the TypeList interface.
func (v VariadicType) MatchLen(l int) bool {
	return l >= len(v.FixedTypes)
}

// GetAt is part of the TypeList interface.
func (v VariadicType) GetAt(i int) types.T {
	if i < len(v.FixedTypes) {
		return v.FixedTypes[i]
	}
	return v.VarType
}

// Length is part of the TypeList interface.
func (v VariadicType) Length() int {
	return len(v.FixedTypes) + 1
}

// Types is part of the TypeList interface.
func (v VariadicType) Types() []types.T {
	result := make([]types.T, len(v.FixedTypes)+1)
	for i := range v.FixedTypes {
		result[i] = v.FixedTypes[i]
	}
	result[len(result)-1] = v.VarType
	return result
}

func (v VariadicType) String() string {
	var s bytes.Buffer
	for i, t := range v.FixedTypes {
		if i != 0 {
			s.WriteString(", ")
		}
		s.WriteString(t.String())
	}
	if len(v.FixedTypes) > 0 {
		s.WriteString(", ")
	}
	fmt.Fprintf(&s, "%s...", v.VarType)
	return s.String()
}

// UnknownReturnType is returned from ReturnTypers when the arguments provided are
// not sufficient to determine a return type. This is necessary for cases like overload
// resolution, where the argument types are not resolved yet so the type-level function
// will be called without argument types. If a ReturnTyper returns unknownReturnType,
// then the candidate function set cannot be refined. This means that only ReturnTypers
// that never return unknownReturnType, like those created with FixedReturnType, can
// help reduce overload ambiguity.
var UnknownReturnType types.T

// ReturnTyper defines the type-level function in which a builtin function's return type
// is determined. ReturnTypers should make sure to return unknownReturnType when necessary.
type ReturnTyper func(args []TypedExpr) types.T

// FixedReturnType functions simply return a fixed type, independent of argument types.
func FixedReturnType(typ types.T) ReturnTyper {
	return func(args []TypedExpr) types.T { return typ }
}

// IdentityReturnType creates a returnType that is a projection of the idx'th
// argument type.
func IdentityReturnType(idx int) ReturnTyper {
	return func(args []TypedExpr) types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		return args[idx].ResolvedType()
	}
}

// FirstNonNullReturnType returns the type of the first non-null argument, or
// types.Unknown if all arguments are null. There must be at least one argument,
// or else FirstNonNullReturnType returns UnknownReturnType. This method is used
// with HomogeneousType functions, in which all arguments have been checked to
// have the same type (or be null).
func FirstNonNullReturnType() ReturnTyper {
	return func(args []TypedExpr) types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		for _, arg := range args {
			if t := arg.ResolvedType(); t != types.Unknown {
				return t
			}
		}
		return types.Unknown
	}
}

func returnTypeToFixedType(s ReturnTyper) types.T {
	if t := s(nil); t != UnknownReturnType {
		return t
	}
	return types.Any
}

type typeCheckOverloadState struct {
	overloads       []OverloadImpl
	overloadIdxs    []uint8 // index into overloads
	exprs           []Expr
	typedExprs      []TypedExpr
	resolvableIdxs  []int // index into exprs/typedExprs
	constIdxs       []int // index into exprs/typedExprs
	placeholderIdxs []int // index into exprs/typedExprs
}

// typeCheckOverloadedExprs determines the correct overload to use for the given set of
// expression parameters, along with an optional desired return type. It returns the expression
// parameters after being type checked, along with a slice of candidate overloadImpls. The
// slice may have length:
//   0: overload resolution failed because no compatible overloads were found
//   1: overload resolution succeeded
//  2+: overload resolution failed because of ambiguity
// The inBinOp parameter denotes whether this type check is occurring within a binary operator,
// in which case we may need to make a guess that the two parameters are of the same type if one
// of them is NULL.

func typeCheckOverloadedExprs(
	ctx *SemaContext,
	desired types.T,
	overloads []OverloadImpl,
	inBinOp bool,
	useOrigin bool,
	exprs ...Expr,
) ([]TypedExpr, []OverloadImpl, []uint8, error) {
	if len(overloads) > math.MaxUint8 {
		return nil, nil, nil, pgerror.NewAssertionErrorf("too many overloads (%d > 255)", len(overloads))
	}

	var s typeCheckOverloadState
	s.exprs = exprs
	s.overloads = overloads

	// Special-case the HomogeneousType overload. We determine its return type by checking that
	// all parameters have the same type.
	for i, overload := range overloads {
		// Only one overload can be provided if it has parameters with HomogeneousType.
		if _, ok := overload.params().(HomogeneousType); ok {
			if len(overloads) > 1 {
				if overload.(*Overload).Info != "Returns the result of decode condition expression" {
					panic("only one overload can have HomogeneousType parameters")
				}
			}
			var typedExprs []TypedExpr
			var err error
			if overload.(*Overload).Info == "Returns the result of decode condition expression" {
				for i := 0; i < len(exprs); i++ {
					e, err := exprs[i].TypeCheck(ctx, types.Any, false)
					if err != nil {
						return nil, nil, nil, err
					}
					typedExprs = append(typedExprs, e)
				}
			} else {
				typedExprs, _, err = TypeCheckSameTypedExprs(ctx, desired, false, useOrigin, exprs...)
				if err != nil {
					return nil, nil, nil, err
				}
			}
			return typedExprs, overloads[i : i+1], []uint8{uint8(i)}, nil
		}
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	s.typedExprs = make([]TypedExpr, len(exprs))
	s.constIdxs, s.placeholderIdxs, s.resolvableIdxs = typeCheckSplitExprs(ctx, exprs)

	// If no overloads are provided, just type check parameters and return.
	if len(overloads) == 0 {
		for _, i := range s.resolvableIdxs {
			typ, err := exprs[i].TypeCheck(ctx, types.Any, useOrigin)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "error type checking resolved expression:")
			}
			s.typedExprs[i] = typ
		}
		if err := defaultTypeCheck(ctx, &s, false); err != nil {
			return nil, nil, nil, err
		}
		return s.typedExprs, nil, nil, nil
	}

	s.overloadIdxs = make([]uint8, len(overloads))
	for i := 0; i < len(overloads); i++ {
		s.overloadIdxs[i] = uint8(i)
	}

	// Filter out incorrect parameter length overloads.
	s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
		func(o OverloadImpl) bool {
			return o.params().MatchLen(len(exprs))
		})

	// Filter out overloads which constants cannot become.
	for _, i := range s.constIdxs {
		constExpr := exprs[i].(Constant)
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o OverloadImpl) bool {
				return canConstantBecome(constExpr, o.params().GetAt(i), useOrigin)
			})
	}

	// TODO(nvanbenschoten): We should add a filtering step here to filter
	// out impossible candidates based on identical parameters. For instance,
	// f(int, float) is not a possible candidate for the expression f($1, $1).

	// Filter out overloads on resolved types.
	for _, i := range s.resolvableIdxs {
		paramDesired := types.Any
		if len(s.overloadIdxs) == 1 {
			// Once we get down to a single overload candidate, begin desiring its
			// parameter types for the corresponding argument expressions.
			paramDesired = s.overloads[s.overloadIdxs[0]].params().GetAt(i)
		}
		typ, err := exprs[i].TypeCheck(ctx, paramDesired, useOrigin)
		if err != nil {
			return nil, nil, nil, err
		}
		s.typedExprs[i] = typ
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o OverloadImpl) bool {
				return o.params().MatchAt(typ.ResolvedType(), i)
			})
	}

	// At this point, all remaining overload candidates accept the argument list,
	// so we begin checking for a single remaining candidate implementation to choose.
	// In case there is more than one candidate remaining, the following code uses
	// heuristics to find a most preferable candidate.
	if ok, typedExprs, fns, err := checkReturn(ctx, &s, nil, useOrigin); ok {
		return typedExprs, fns, s.overloadIdxs, err
	}

	// The first heuristic is to prefer candidates that return the desired type.
	if desired != types.Any {
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o OverloadImpl) bool {
				// For now, we only filter on the return type for overloads with
				// fixed return types. This could be improved, but is not currently
				// critical because we have no cases of functions with multiple
				// overloads that do not all expose FixedReturnTypes.
				if t := o.returnType()(nil); t != UnknownReturnType {
					return t.Equivalent(desired)
				}
				return true
			})
		if ok, typedExprs, fns, err := checkReturn(ctx, &s, nil, useOrigin); ok {
			return typedExprs, fns, s.overloadIdxs, err
		}
	}

	var homogeneousTyp types.T
	if len(s.resolvableIdxs) > 0 {
		homogeneousTyp = s.typedExprs[s.resolvableIdxs[0]].ResolvedType()
		for _, i := range s.resolvableIdxs[1:] {
			if !homogeneousTyp.Equivalent(s.typedExprs[i].ResolvedType()) {
				homogeneousTyp = nil
				break
			}
		}
	}

	if len(s.constIdxs) > 0 {
		if ok, typedExprs, fns, err := filterAttempt(ctx, &s, nil, useOrigin, func() {
			// The second heuristic is to prefer candidates where all constants can
			// become a homogeneous type, if all resolvable expressions became one.
			// This is only possible resolvable expressions were resolved
			// homogeneously up to this point.
			if homogeneousTyp != nil {
				all := true
				for _, i := range s.constIdxs {
					if !canConstantBecome(exprs[i].(Constant), homogeneousTyp, useOrigin) {
						all = false
						break
					}
				}
				if all {
					for _, i := range s.constIdxs {
						s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
							func(o OverloadImpl) bool {
								return o.params().GetAt(i).Equivalent(homogeneousTyp)
							})
					}
				}
			}
		}); ok {
			return typedExprs, fns, s.overloadIdxs, err
		}

		if ok, typedExprs, fns, err := filterAttempt(ctx, &s, nil, useOrigin, func() {
			// The third heuristic is to prefer candidates where all constants can
			// become their "natural" types.
			for _, i := range s.constIdxs {
				natural := naturalConstantType(exprs[i].(Constant))
				if natural != nil {
					s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
						func(o OverloadImpl) bool {
							return o.params().GetAt(i).Equivalent(natural)
						})
				}
			}
		}); ok {
			return typedExprs, fns, s.overloadIdxs, err
		}

		// At this point, it's worth seeing if we have constants that can't actually
		// parse as the type that canConstantBecome claims they can. For example,
		// every string literal will report that it can become an interval, but most
		// string literals do not encode valid intervals. This may uncover some
		// overloads with invalid type signatures.
		//
		// This parsing is sufficiently expensive (see the comment on
		// StrVal.AvailableTypes) that we wait until now, when we've eliminated most
		// overloads from consideration, so that we only need to check each constant
		// against a limited set of types. We can't hold off on this parsing any
		// longer, though: the remaining heuristics are overly aggressive and will
		// falsely reject the only valid overload in some cases.
		for _, i := range s.constIdxs {
			constExpr := exprs[i].(Constant)
			s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
				func(o OverloadImpl) bool {
					_, err := constExpr.ResolveAsType(&SemaContext{}, o.params().GetAt(i), useOrigin)
					return err == nil
				})
		}
		if ok, typedExprs, fn, err := checkReturn(ctx, &s, nil, useOrigin); ok {
			return typedExprs, fn, s.overloadIdxs, err
		}

		if realBestConstType, ok := commonConstantTypeAgain(s.overloads, s.exprs, s.constIdxs); ok {
			for _, i := range s.constIdxs {
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o OverloadImpl) bool {
						return o.params().GetAt(i).Equivalent(realBestConstType[i])
					})
			}
		}

		if ok, typedExprs, fn, err := checkReturn(ctx, &s, nil, useOrigin); ok {
			return typedExprs, fn, s.overloadIdxs, err
		}

		// The fourth heuristic is to prefer candidates that accepts the "best"
		// mutual type in the resolvable type set of all constants.
		if bestConstType, ok := commonConstantType(s.exprs, s.constIdxs); ok {
			for _, i := range s.constIdxs {
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o OverloadImpl) bool {
						return o.params().GetAt(i).Equivalent(bestConstType)
					})
			}
			if ok, typedExprs, fns, err := checkReturn(ctx, &s, nil, useOrigin); ok {
				return typedExprs, fns, s.overloadIdxs, err
			}
			if homogeneousTyp != nil {
				if !homogeneousTyp.Equivalent(bestConstType) {
					homogeneousTyp = nil
				}
			} else {
				homogeneousTyp = bestConstType
			}
		}
	}

	// The fifth heuristic is to prefer candidates where all placeholders can be
	// given the same type as all constants and resolvable expressions. This is
	// only possible if all constants and resolvable expressions were resolved
	// homogeneously up to this point.
	if homogeneousTyp != nil && len(s.placeholderIdxs) > 0 {
		// Before we continue, try to propagate the homogeneous type to the
		// placeholders. This might not have happened yet, if the overloads'
		// parameter types are ambiguous (like in the case of tuple-tuple binary
		// operators).
		for _, i := range s.placeholderIdxs {
			if _, err := exprs[i].TypeCheck(ctx, homogeneousTyp, useOrigin); err != nil {
				return nil, nil, nil, err
			}
			s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
				func(o OverloadImpl) bool {
					return o.params().GetAt(i).Equivalent(homogeneousTyp)
				})
		}
		if ok, typedExprs, fns, err := checkReturn(ctx, &s, nil, useOrigin); ok {
			return typedExprs, fns, s.overloadIdxs, err
		}
	}

	// In a binary expression, in the case of one of the arguments being untyped NULL,
	// we prefer overloads where we infer the type of the NULL to be the same as the
	// other argument. This is used to differentiate the behavior of
	// STRING[] || NULL and STRING || NULL.
	if inBinOp && len(s.exprs) == 2 {
		if ok, typedExprs, fns, err := filterAttempt(ctx, &s, nil, useOrigin, func() {
			var err error
			left := s.typedExprs[0]
			if left == nil {
				left, err = s.exprs[0].TypeCheck(ctx, types.Any, useOrigin)
				if err != nil {
					return
				}
			}
			right := s.typedExprs[1]
			if right == nil {
				right, err = s.exprs[1].TypeCheck(ctx, types.Any, useOrigin)
				if err != nil {
					return
				}
			}
			leftType := left.ResolvedType()
			rightType := right.ResolvedType()
			leftIsNull := leftType == types.Unknown
			rightIsNull := rightType == types.Unknown
			oneIsNull := (leftIsNull || rightIsNull) && !(leftIsNull && rightIsNull)
			if oneIsNull {
				if leftIsNull {
					leftType = rightType
				}
				if rightIsNull {
					rightType = leftType
				}
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o OverloadImpl) bool {
						return o.params().GetAt(0).Equivalent(leftType) &&
							o.params().GetAt(1).Equivalent(rightType)
					})
			}
		}); ok {
			return typedExprs, fns, s.overloadIdxs, err
		}
	}

	// The final heuristic is to defer to preferred candidates, if available.
	if ok, typedExprs, fns, err := filterAttempt(ctx, &s, nil, useOrigin, func() {
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs, func(o OverloadImpl) bool {
			return o.preferred()
		})
	}); ok {
		return typedExprs, fns, s.overloadIdxs, err
	}

	if err := defaultTypeCheck(ctx, &s, len(s.overloads) > 0); err != nil {
		return nil, nil, nil, err
	}

	possibleOverloads := make([]OverloadImpl, len(s.overloadIdxs))
	for i, o := range s.overloadIdxs {
		possibleOverloads[i] = s.overloads[o]
	}
	return s.typedExprs, possibleOverloads, s.overloadIdxs, nil
}

// isDateTimeType is to judge a type if is a type in [date, time, interval, timestamp, timestampTZ]
func isDateTimeType(t types.T) bool {
	dateTime := []types.T{types.Date, types.Time, types.Interval, types.Timestamp, types.TimestampTZ}
	for _, v := range dateTime {
		if v == t {
			return true
		}
	}
	return false
}

// toCastTypeCheckOverloadedExprs determines the correct overload to use for the given set of
// expression parameters, along with an optional desired return type. It returns the expression
// parameters after being type checked, along with a slice of candidate overloadImpls. The
// slice may have length:
//   0: overload resolution failed because no compatible overloads were found
//   1: overload resolution succeeded
//  2+: overload resolution failed because of ambiguity
// The inBinOp parameter denotes whether this type check is occurring within a binary operator,
// in which case we may need to make a guess that the two parameters are of the same type if one
// of them is NULL.
func toCastTypeCheckOverloadedExprs(
	ctx *SemaContext,
	opkind string,
	operator Operator,
	desired types.T,
	overloads []OverloadImpl,
	inBinOp bool,
	useOrigin bool,
	exprs ...Expr,
) ([]TypedExpr, []OverloadImpl, error) {
	if len(overloads) > math.MaxUint8 {
		return nil, nil, pgerror.NewAssertionErrorf("too many overloads (%d > 255)", len(overloads))
	}

	var s typeCheckOverloadState
	s.exprs = exprs
	s.overloads = overloads

	// Special-case the HomogeneousType overload. We determine its return type by checking that
	// all parameters have the same type.
	for i, overload := range overloads {
		// Only one overload can be provided if it has parameters with HomogeneousType.
		if _, ok := overload.params().(HomogeneousType); ok {
			if len(overloads) > 1 {
				panic("only one overload can have HomogeneousType parameters")
			}
			typedExprs, _, err := TypeCheckSameTypedExprs(ctx, desired, false, useOrigin, exprs...)
			if err != nil {
				return nil, nil, err
			}
			return typedExprs, overloads[i : i+1], nil
		}
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	s.typedExprs = make([]TypedExpr, len(exprs))
	s.constIdxs, s.placeholderIdxs, s.resolvableIdxs = typeCheckSplitExprs(ctx, exprs)

	// If no overloads are provided, just type check parameters and return.
	if len(overloads) == 0 {
		for _, i := range s.resolvableIdxs {
			typ, err := exprs[i].TypeCheck(ctx, types.Any, false)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error type checking resolved expression:")
			}
			s.typedExprs[i] = typ
		}
		if err := defaultTypeCheck(ctx, &s, false); err != nil {
			return nil, nil, err
		}
		return s.typedExprs, nil, nil
	}

	s.overloadIdxs = make([]uint8, len(overloads))
	for i := 0; i < len(overloads); i++ {
		s.overloadIdxs[i] = uint8(i)
	}
	overloadsIdxsCopy := make([]uint8, len(s.overloadIdxs))
	copy(overloadsIdxsCopy, s.overloadIdxs)

	// Filter out incorrect parameter length overloads.
	s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
		func(o OverloadImpl) bool {
			return o.params().MatchLen(len(exprs))
		})

	// Filter out overloads which constants cannot become.
	for _, i := range s.constIdxs {
		constExpr := exprs[i].(Constant)
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o OverloadImpl) bool {
				return canConstantBecome(constExpr, o.params().GetAt(i), useOrigin)
			})
	}

	// Filter out overloads on resolved types.
	for _, i := range s.resolvableIdxs {
		paramDesired := types.Any
		leftOneOverload := false
		if len(s.overloadIdxs) == 1 {
			// Once we get down to a single overload candidate, begin desiring its
			// parameter types for the corresponding argument expressions.
			paramDesired = s.overloads[s.overloadIdxs[0]].params().GetAt(i)

			if opkind == "B" {
				leftOneOverload = true
			}
		}
		typ, err := exprs[i].TypeCheck(ctx, paramDesired, leftOneOverload)
		if err != nil {
			return nil, nil, err
		}
		if i == 1 && opkind == "B" && len(s.resolvableIdxs) == 2 && operator == Minus && typ.ResolvedType() == types.Time {
			if s.typedExprs[0].ResolvedType() == types.Timestamp || s.typedExprs[0].ResolvedType() == types.TimestampTZ {
				CastExpr := ChangeToCastExpr(typ, types.Interval, 0)
				typ = &CastExpr
			}
		}
		s.typedExprs[i] = typ
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o OverloadImpl) bool {
				return o.params().MatchAt(typ.ResolvedType(), i)
			})
	}
	if len(s.overloadIdxs) == 1 {
		needJumpNext := false
		if opkind == "C" && !s.overloads[s.overloadIdxs[0]].params().MatchAt(types.FamCollatedString, 0) {
			needJumpNext = true
			if exprTypeLeft, err1 := exprs[0].TypeCheck(ctx, types.Any, false); err1 == nil {
				if exprTypeRight, err2 := exprs[1].TypeCheck(ctx, types.Any, false); err2 == nil {
					leftType := exprTypeLeft.ResolvedType()
					rightType := exprTypeRight.ResolvedType()
					if s.overloads[s.overloadIdxs[0]].params().MatchAt(leftType, 0) && s.overloads[s.overloadIdxs[0]].params().MatchAt(rightType, 1) {
						needJumpNext = false
					}
				}
			}
			if operator == JSONExists || operator == JSONSomeExists || operator == JSONAllExists {
				needJumpNext = false
			}
		}
		if ok, typedExprs, fns, err := checkReturn(ctx, &s, operator, useOrigin); ok && !needJumpNext {
			tl := typedExprs[0].ResolvedType()
			tr := typedExprs[1].ResolvedType()
			if csl, okl := tl.(types.TCollatedString); okl {
				if csr, okr := tr.(types.TCollatedString); okr {
					if csl.Locale == "" && csr.Locale != "" {
						if _, okd := typedExprs[0].(*DCollatedString); okd {
							typedExprs[0].(*DCollatedString).Locale = csr.Locale
						}
						if _, okd := typedExprs[0].(*CollateExpr); okd {
							typedExprs[0].(*CollateExpr).Locale = csr.Locale
							typedExprs[0].(*CollateExpr).Typ = types.TCollatedString{Locale: csr.Locale}
						}
					}
					if csr.Locale == "" && csl.Locale != "" {
						if _, okd := typedExprs[1].(*DCollatedString); okd {
							typedExprs[1].(*DCollatedString).Locale = csl.Locale
						}
						if _, okd := typedExprs[1].(*CollateExpr); okd {
							typedExprs[1].(*CollateExpr).Locale = csl.Locale
							typedExprs[1].(*CollateExpr).Typ = types.TCollatedString{Locale: csl.Locale}
						}
					}
				}
			}
			return typedExprs, fns, err
		}
	}

	var TypeLeft, TypeRight, TypeCast types.T
	GetTypeCastOk := false
	if colType, err := exprs[0].TypeCheck(ctx, types.Any, false); err == nil {
		TypeLeft = colType.ResolvedType()
	}
	if colType, err := exprs[1].TypeCheck(ctx, types.Any, false); err == nil {
		TypeRight = colType.ResolvedType()
	}
	if opkind == "C" {
		TypeCast, GetTypeCastOk = GetCmpType(TypeLeft, TypeRight, operator)
	}
	if opkind == "B" {
		TypeCast, GetTypeCastOk = GetBinType(TypeLeft, TypeRight, operator)
	}
	if useOrigin {
		GetTypeCastOk = false
	}
	// for type string
	if (isDateTimeType(TypeLeft) && TypeRight == types.String) || (TypeLeft == types.String && isDateTimeType(TypeRight)) {
		cmpUseCast := false
		if opkind == "C" {
			if ((TypeLeft == types.Timestamp || TypeLeft == types.TimestampTZ) && TypeRight == types.String) ||
				(TypeLeft == types.String && (TypeRight == types.Timestamp || TypeRight == types.TimestampTZ)) {
				cmpUseCast = true
			}
		}
		if !cmpUseCast {
			useOrigin = true
			GetTypeCastOk = false
		}
	}

	// TODO: 值得商榷，这里如果直接typecheck，则固定了const的类型，对于string来说，有可能是其它类型，比如interval '6 month'
	if GetTypeCastOk && opkind == "C" {
		err := castBeforeTypeCheck(ctx, &s, overloadsIdxsCopy, TypeCast, exprs...)
		if err != nil {
			return nil, nil, err
		}
		if len(s.overloadIdxs) == 1 {
			return s.typedExprs, s.overloads[s.overloadIdxs[0] : s.overloadIdxs[0]+1], err
		}
	}

	// At this point, all remaining overload candidates accept the argument list,
	// so we begin checking for a single remaining candidate implementation to choose.
	// In case there is more than one candidate remaining, the following code uses
	// heuristics to find a most preferable candidate.
	if GetTypeCastOk && (len(s.overloadIdxs) == 0 || (len(s.overloadIdxs) == 1 &&
		returnTypeToFixedType(s.overloads[s.overloadIdxs[0]].returnType()) != TypeCast)) {
		// TODO: maybe should judge if the decimal is the constant expr
		if len(s.overloadIdxs) == 1 && (returnTypeToFixedType(s.overloads[s.overloadIdxs[0]].returnType()) == types.Float || returnTypeToFixedType(s.overloads[s.overloadIdxs[0]].returnType()) == types.Float) && TypeCast == types.Decimal {
			TypeCast = types.Float
		} else {
			err := castBeforeTypeCheck(ctx, &s, overloadsIdxsCopy, TypeCast, exprs...)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if ok, typedExprs, fns, err := checkReturn(ctx, &s, operator, useOrigin); ok {
		return typedExprs, fns, err
	}

	// The first heuristic is to prefer candidates that return the desired type.
	if desired != types.Any {
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o OverloadImpl) bool {
				// For now, we only filter on the return type for overloads with
				// fixed return types. This could be improved, but is not currently
				// critical because we have no cases of functions with multiple
				// overloads that do not all expose FixedReturnTypes.
				if t := o.returnType()(nil); t != UnknownReturnType {
					return t.Equivalent(desired)
				}
				return true
			})
		if len(s.overloadIdxs) == 1 && returnTypeToFixedType(s.overloads[s.overloadIdxs[0]].returnType()) != desired {
			if GetTypeCastOk && (len(s.overloadIdxs) == 0 || (len(s.overloadIdxs) == 1 &&
				returnTypeToFixedType(s.overloads[s.overloadIdxs[0]].returnType()) != TypeCast)) {
				err := castBeforeTypeCheck(ctx, &s, overloadsIdxsCopy, TypeCast, exprs...)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		// when desired type not nil and overloads >= 2, try to convert constants to desired type
		if len(s.overloadIdxs) >= 2 {
			jumpOut := false
			for _, i := range s.constIdxs {
				typ, err := exprs[i].TypeCheck(ctx, desired, false)
				// if err != nil, continue next steps, don't do typecheck.
				// For some instances of a type, it may can not convert to a available type
				// Example: 'a' is string type, json is one of available type of string, but 'a' cannot convert to json type
				if err != nil {
					jumpOut = true
					break
				}
				s.resolvableIdxs = append(s.resolvableIdxs, i)
				s.typedExprs[i] = typ
			}
			if !jumpOut {
				s.constIdxs = nil
				// Filter out overloads on resolved types.
				for _, i := range s.resolvableIdxs {
					typ := s.typedExprs[i]
					s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
						func(o OverloadImpl) bool {
							return o.params().MatchAt(typ.ResolvedType(), i)
						})
				}
			}
		}
		if ok, typedExprs, fns, err := checkReturn(ctx, &s, operator, useOrigin); ok {
			return typedExprs, fns, err
		}
	}

	var homogeneousTyp types.T
	if len(s.resolvableIdxs) > 0 {
		homogeneousTyp = s.typedExprs[s.resolvableIdxs[0]].ResolvedType()
		for _, i := range s.resolvableIdxs[1:] {
			if !homogeneousTyp.Equivalent(s.typedExprs[i].ResolvedType()) {
				homogeneousTyp = nil
				break
			}
		}
	}

	if len(s.constIdxs) > 0 {
		if ok, typedExprs, fns, err := filterAttempt(ctx, &s, operator, useOrigin, func() {
			// The second heuristic is to prefer candidates where all constants can
			// become a homogeneous type, if all resolvable expressions became one.
			// This is only possible resolvable expressions were resolved
			// homogeneously up to this point.
			if homogeneousTyp != nil && useOrigin { // don't need this function
				all := true
				for _, i := range s.constIdxs {
					if !canConstantBecome(exprs[i].(Constant), homogeneousTyp, useOrigin) {
						all = false
						break
					}
				}
				if all {
					for _, i := range s.constIdxs {
						s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
							func(o OverloadImpl) bool {
								return o.params().GetAt(i).Equivalent(homogeneousTyp)
							})
					}
				}
			}
		}); ok {
			return typedExprs, fns, err
		}

		if ok, typedExprs, fns, err := filterAttempt(ctx, &s, operator, useOrigin, func() {
			// The third heuristic is to prefer candidates where all constants can
			// become their "natural" types.
			for _, i := range s.constIdxs {
				natural := naturalConstantType(exprs[i].(Constant))
				if natural != nil {
					s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
						func(o OverloadImpl) bool {
							return o.params().GetAt(i).Equivalent(natural)
						})
				}
			}
		}); ok {
			return typedExprs, fns, err
		}

		// At this point, it's worth seeing if we have constants that can't actually
		// parse as the type that canConstantBecome claims they can. For example,
		// every string literal will report that it can become an interval, but most
		// string literals do not encode valid intervals. This may uncover some
		// overloads with invalid type signatures.
		//
		// This parsing is sufficiently expensive (see the comment on
		// StrVal.AvailableTypes) that we wait until now, when we've eliminated most
		// overloads from consideration, so that we only need to check each constant
		// against a limited set of types. We can't hold off on this parsing any
		// longer, though: the remaining heuristics are overly aggressive and will
		// falsely reject the only valid overload in some cases.
		for _, i := range s.constIdxs {
			constExpr := exprs[i].(Constant)
			s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
				func(o OverloadImpl) bool {
					_, err := constExpr.ResolveAsType(&SemaContext{}, o.params().GetAt(i), useOrigin)
					return err == nil
				})
		}
		if GetTypeCastOk && (len(s.overloadIdxs) == 0 || (len(s.overloadIdxs) == 1 &&
			returnTypeToFixedType(s.overloads[s.overloadIdxs[0]].returnType()) != TypeCast)) {
			err := castBeforeTypeCheck(ctx, &s, overloadsIdxsCopy, TypeCast, exprs...)
			if err != nil {
				return nil, nil, err
			}
		}
		if ok, typedExprs, fn, err := checkReturn(ctx, &s, operator, useOrigin); ok {
			return typedExprs, fn, err
		}

		// The fourth heuristic is to prefer candidates that accepts the "best"
		// mutual type in the resolvable type set of all constants.
		if bestConstType, ok := commonConstantType(s.exprs, s.constIdxs); ok {
			// if bestConstType, ok := TypeCast, GetTypeCastOk; ok {
			for _, i := range s.constIdxs {
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o OverloadImpl) bool {
						return o.params().GetAt(i).Equivalent(bestConstType)
					})
			}
			if GetTypeCastOk && (len(s.overloadIdxs) == 0 || (len(s.overloadIdxs) == 1 &&
				returnTypeToFixedType(s.overloads[s.overloadIdxs[0]].returnType()) != TypeCast)) {
				err := castBeforeTypeCheck(ctx, &s, overloadsIdxsCopy, TypeCast, exprs...)
				if err != nil {
					return nil, nil, err
				}
			}
			if ok, typedExprs, fns, err := checkReturn(ctx, &s, operator, useOrigin); ok {
				return typedExprs, fns, err
			}
			if homogeneousTyp != nil {
				if !homogeneousTyp.Equivalent(bestConstType) {
					homogeneousTyp = nil
				}
			} else {
				homogeneousTyp = bestConstType
			}
		}
	}

	// The fifth heuristic is to prefer candidates where all placeholders can be
	// given the same type as all constants and resolvable expressions. This is
	// only possible if all constants and resolvable expressions were resolved
	// homogeneously up to this point.
	if homogeneousTyp != nil && len(s.placeholderIdxs) > 0 {
		// Before we continue, try to propagate the homogeneous type to the
		// placeholders. This might not have happened yet, if the overloads'
		// parameter types are ambiguous (like in the case of tuple-tuple binary
		// operators).
		for _, i := range s.placeholderIdxs {
			if _, err := exprs[i].TypeCheck(ctx, homogeneousTyp, false); err != nil {
				return nil, nil, err
			}
			s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
				func(o OverloadImpl) bool {
					return o.params().GetAt(i).Equivalent(homogeneousTyp)
				})
		}
		if GetTypeCastOk && (len(s.overloadIdxs) == 0 || (len(s.overloadIdxs) == 1 &&
			returnTypeToFixedType(s.overloads[s.overloadIdxs[0]].returnType()) != TypeCast)) {
			err := castBeforeTypeCheck(ctx, &s, overloadsIdxsCopy, TypeCast, exprs...)
			if err != nil {
				return nil, nil, err
			}
		}
		if ok, typedExprs, fns, err := checkReturn(ctx, &s, operator, useOrigin); ok {
			return typedExprs, fns, err
		}
	}

	// In a binary expression, in the case of one of the arguments being untyped NULL,
	// we prefer overloads where we infer the type of the NULL to be the same as the
	// other argument. This is used to differentiate the behavior of
	// STRING[] || NULL and STRING || NULL.
	if inBinOp && len(s.exprs) == 2 {
		if ok, typedExprs, fns, err := filterAttempt(ctx, &s, operator, useOrigin, func() {
			var err error
			left := s.typedExprs[0]
			if left == nil {
				left, err = s.exprs[0].TypeCheck(ctx, types.Any, false)
				if err != nil {
					return
				}
			}
			right := s.typedExprs[1]
			if right == nil {
				right, err = s.exprs[1].TypeCheck(ctx, types.Any, false)
				if err != nil {
					return
				}
			}
			leftType := left.ResolvedType()
			rightType := right.ResolvedType()
			leftIsNull := leftType == types.Unknown
			rightIsNull := rightType == types.Unknown
			oneIsNull := (leftIsNull || rightIsNull) && !(leftIsNull && rightIsNull)
			if oneIsNull {
				if leftIsNull {
					leftType = rightType
				}
				if rightIsNull {
					rightType = leftType
				}
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o OverloadImpl) bool {
						return o.params().GetAt(0).Equivalent(leftType) &&
							o.params().GetAt(1).Equivalent(rightType)
					})
			}
		}); ok {
			return typedExprs, fns, err
		}
	}

	// The final heuristic is to defer to preferred candidates, if available.
	if ok, typedExprs, fns, err := filterAttempt(ctx, &s, operator, useOrigin, func() {
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs, func(o OverloadImpl) bool {
			return o.preferred()
		})
	}); ok {
		return typedExprs, fns, err
	}

	if len(s.overloadIdxs) > 0 {
		for i, v := range s.overloadIdxs {
			if v > s.overloadIdxs[0] {
				s.overloadIdxs[i], s.overloadIdxs[0] = s.overloadIdxs[0], s.overloadIdxs[i]
			}
		}
		s.overloadIdxs = s.overloadIdxs[:1]
		if ok, typedExprs, fns, err := checkReturn(ctx, &s, operator, useOrigin); ok {
			return typedExprs, fns, err
		}
	}

	if GetTypeCastOk {
		err := castBeforeTypeCheck(ctx, &s, overloadsIdxsCopy, TypeCast, exprs...)
		if err != nil {
			return nil, nil, err
		}
	} else {
		if err := defaultTypeCheck(ctx, &s, len(s.overloads) > 0); err != nil {
			return nil, nil, err
		}
	}

	possibleOverloads := make([]OverloadImpl, len(s.overloadIdxs))
	for i, o := range s.overloadIdxs {
		possibleOverloads[i] = s.overloads[o]
	}
	return s.typedExprs, possibleOverloads, nil
}

// filterAttempt attempts to filter the overloads down to a single candidate.
// If it succeeds, it will return true, along with the overload (in a slice for
// convenience) and a possible error. If it fails, it will return false and
// undo any filtering performed during the attempt.
func filterAttempt(
	ctx *SemaContext, s *typeCheckOverloadState, operate Operator, useOrigin bool, attempt func(),
) (ok bool, _ []TypedExpr, _ []OverloadImpl, _ error) {
	before := s.overloadIdxs
	attempt()
	if len(s.overloadIdxs) == 1 {
		ok, typedExprs, fns, err := checkReturn(ctx, s, operate, useOrigin)
		if err != nil {
			return false, nil, nil, err
		}
		if ok {
			return true, typedExprs, fns, err
		}
	}
	s.overloadIdxs = before
	return false, nil, nil, nil
}

// filterOverloads filters overloads which do not satisfy the predicate.
func filterOverloads(
	overloads []OverloadImpl, overloadIdxs []uint8, fn func(OverloadImpl) bool,
) []uint8 {
	for i := 0; i < len(overloadIdxs); {
		if fn(overloads[overloadIdxs[i]]) {
			i++
		} else {
			overloadIdxs[i], overloadIdxs[len(overloadIdxs)-1] = overloadIdxs[len(overloadIdxs)-1], overloadIdxs[i]
			overloadIdxs = overloadIdxs[:len(overloadIdxs)-1]
		}
	}
	return overloadIdxs
}

// defaultTypeCheck type checks the constant and placeholder expressions without a preference
// and adds them to the type checked slice.
func defaultTypeCheck(ctx *SemaContext, s *typeCheckOverloadState, errorOnPlaceholders bool) error {
	for _, i := range s.constIdxs {
		typ, err := s.exprs[i].TypeCheck(ctx, types.Any, false)
		if err != nil {
			return errors.Wrap(err, "error type checking constant value")
		}
		s.typedExprs[i] = typ
	}
	for _, i := range s.placeholderIdxs {
		if errorOnPlaceholders {
			_, err := s.exprs[i].TypeCheck(ctx, types.Any, false)
			return err
		}
		// If we dont want to error on args, avoid type checking them without a desired type.
		s.typedExprs[i] = StripParens(s.exprs[i]).(*Placeholder)
	}
	return nil
}

// checkReturn checks the number of remaining overloaded function
// implementations.
// Returns ok=true if we should stop overload resolution, and returning either
// 1. the chosen overload in a slice, or
// 2. nil,
// along with the typed arguments.
// This modifies values within s as scratch slices, but only in the case where
// it returns true, which signals to the calling function that it should
// immediately return, so any mutations to s are irrelevant.
func checkReturn(
	ctx *SemaContext, s *typeCheckOverloadState, opkind Operator, useOrigin bool,
) (ok bool, _ []TypedExpr, _ []OverloadImpl, _ error) {
	switch len(s.overloadIdxs) {
	case 0:
		if err := defaultTypeCheck(ctx, s, false); err != nil {
			return false, nil, nil, err
		}
		// for CONCAT operator, when overloadIdxs == nil, convert exprs to string type
		if opkind == Concat {
			LCollated := CollatedStrORCollatedStrArr(s.typedExprs[0])
			RCollated := CollatedStrORCollatedStrArr(s.typedExprs[1])
			if !LCollated && !RCollated {
				for i, t := range s.typedExprs {
					typ := ChangeToCastExpr(t, types.String, 0)
					s.typedExprs[i] = &typ
				}
				return true, s.typedExprs, s.overloads[3:4], nil
			}
		}
		return true, s.typedExprs, nil, nil

	case 1:
		idx := s.overloadIdxs[0]
		o := s.overloads[idx]
		p := o.params()
		for _, i := range s.constIdxs {
			des := p.GetAt(i)
			_, before := s.exprs[i].(*CollateExpr)

			// when concat about bitArray and const string, when string can convert to bitArray, the return type is bitArray before
			// add this condition to avoid this case, let the concat return type about bitArray and const string is string
			if des == types.BitArray {
				if _, isStrVal := s.exprs[i].(*StrVal); opkind == Concat && isStrVal {
					return false, nil, nil, errors.Errorf("const string's desired type in concat operator should not be %s", des.String())
				}
			}
			if des == types.Time {
				if _, isNumVal := s.exprs[i].(*NumVal); opkind == Minus && isNumVal {
					return false, nil, nil, errors.Errorf("const number's desired type in minus operator should not be %s", des.String())
				}
			}
			typ, err := s.exprs[i].TypeCheck(ctx, des, useOrigin)
			_, after := typ.(*CollateExpr)
			if err != nil {
				return false, s.typedExprs, nil, pgerror.Wrapf(
					err, pgcode.InvalidParameterValue,
					"error type checking constant value",
				)
			}
			if before != false || after != true {
				if des != nil && !typ.ResolvedType().Equivalent(des) {
					if opkind == Concat {
						return false, nil, nil, nil
					}
					return true, nil, nil, errors.Errorf("desired constant value type %s but set type %s", des, typ.ResolvedType())
				}
			}
			s.typedExprs[i] = typ
		}

		for _, i := range s.placeholderIdxs {
			des := p.GetAt(i)
			typ, err := s.exprs[i].TypeCheck(ctx, des, false)
			if err != nil {
				if des.IsAmbiguous() {
					return false, nil, nil, nil
				}
				return false, nil, nil, err
			}
			s.typedExprs[i] = typ
		}
		return true, s.typedExprs, s.overloads[idx : idx+1], nil

	default:
		return false, nil, nil, nil
	}
}

func formatCandidates(prefix string, candidates []OverloadImpl, noReturn bool) string {
	var buf bytes.Buffer
	sort.Slice(candidates, func(i, j int) bool {
		typeStr := func(argTypes TypeList) string {
			s := ""
			tLen := argTypes.Length()
			for i := 0; i < tLen; i++ {
				t := argTypes.GetAt(i)
				if i > 0 {
					s += ","
				}
				s += t.String()
			}
			return s
		}
		return typeStr(candidates[i].params()) <= typeStr(candidates[j].params())
	})
	for _, candidate := range candidates {
		buf.WriteString(prefix)
		buf.WriteByte('(')
		params := candidate.params()
		tLen := params.Length()
		for i := 0; i < tLen; i++ {
			t := params.GetAt(i)
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(t.String())
		}
		buf.WriteString(") -> ")
		if !noReturn {
			buf.WriteString(returnTypeToFixedType(candidate.returnType()).String())
		}
		if candidate.preferred() {
			buf.WriteString(" [preferred]")
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

func castBeforeTypeCheck(
	ctx *SemaContext,
	s *typeCheckOverloadState,
	overloadsIdxsCopy []uint8,
	TypeCast types.T,
	exprs ...Expr,
) error {
	s.overloadIdxs = s.overloadIdxs[:0]
	s.overloadIdxs = append(s.overloadIdxs, overloadsIdxsCopy...)
	// convert constants to castExpr
	for _, i := range s.constIdxs {
		typ, err := exprs[i].TypeCheck(ctx, TypeCast, false)
		if err != nil {
			return err
		}
		// TODO:这个地方确定还需要进行cast转换?
		// cexpr := ChangeToCastExpr(typ, TypeCast)
		// s.typedExprs[i] = &cexpr
		// exprs[i] = &cexpr
		s.resolvableIdxs = append(s.resolvableIdxs, i)
		s.typedExprs[i] = typ
	}
	s.constIdxs = nil

	for _, i := range s.resolvableIdxs {
		if s.typedExprs[i].ResolvedType() != TypeCast && !IsNullExpr(s.typedExprs[i]) {
			tempTypedExpr, err := exprs[i].TypeCheck(ctx, types.Any, false)
			if err != nil {
				return err
			}
			cexpr := ChangeToCastExpr(tempTypedExpr, TypeCast, 0)
			s.typedExprs[i] = &cexpr
		}
		typ := s.typedExprs[i]
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o OverloadImpl) bool {
				return o.params().MatchAt(typ.ResolvedType(), i)
			})
	}
	return nil
}

// CollatedStrORCollatedStrArr CollatedStrORCollatedStrArr
func CollatedStrORCollatedStrArr(texpr TypedExpr) bool {
	typ := texpr.ResolvedType()
	switch v := typ.(type) {
	case types.TArray:
		if _, ok := v.Typ.(types.TCollatedString); ok {
			return true
		}
	case types.TCollatedString:
		return true
	}
	return false
}

// IsNullExpr IsNullExpr
func IsNullExpr(expr Expr) bool {
	_, nullOK := expr.(dNull)
	return nullOK
}

// FormatCandidates explore function formatCandidates to other package
var FormatCandidates = formatCandidates
