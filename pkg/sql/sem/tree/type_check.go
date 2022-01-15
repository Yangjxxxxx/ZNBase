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
	"fmt"
	"go/constant"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqltelemetry"
	"github.com/znbasedb/znbase/pkg/util/duration"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"golang.org/x/text/language"
)

// SemaContext defines the context in which to perform semantic analysis on an
// expression syntax tree.
type SemaContext struct {
	// Placeholders relates placeholder names to their type and, later, value.
	Placeholders PlaceholderInfo

	// IVarContainer is used to resolve the types of IndexedVars.
	IVarContainer IndexedVarContainer

	// Location references the *Location on the current Session.
	Location **time.Location

	// SearchPath indicates where to search for unqualified function
	// names. The path elements must be normalized via Name.Normalize()
	// already.
	SearchPath sessiondata.SearchPath

	// AsOfTimestamp denotes the explicit AS OF SYSTEM TIME timestamp for the
	// query, if any. If the query is not an AS OF SYSTEM TIME query,
	// AsOfTimestamp is nil.
	// TODO(knz): we may want to support table readers at arbitrary
	// timestamps, so that each FROM clause can have its own
	// timestamp. In that case, the timestamp would not be set
	// globally for the entire txn and this field would not be needed.
	AsOfTimestamp *hlc.Timestamp

	Properties SemaProperties

	InUDR    bool
	IsWhere  bool
	UDRParms UDRVars

	// TODO: to use udr function for table column in future version
	// Resolver UDRFunctionResolver
}

// SemaProperties is a holder for required and derived properties
// during semantic analysis. It provides scoping semantics via its
// Restore() method, see below.
type SemaProperties struct {
	// required constraints type checking to only accept certain kinds
	// of expressions. See SetConstraint
	required semaRequirements

	// Derived is populated during semantic analysis with properties
	// from the expression being analyzed.  The caller is responsible
	// for re-initializing this when needed.
	Derived ScalarProperties
}

type semaRequirements struct {
	// context is the name of the semantic anlysis context, for use in
	// error messages.
	context string

	// The various reject flags reject specific forms of scalar
	// expressions. The default for this struct with false everywhere
	// ensures that anything is allowed.
	rejectFlags SemaRejectFlags
}

// Clear resets the property requirements and derived properties.
func (s *SemaProperties) Clear() {
	s.Require("", AllowAll)
}

// Require resets the derived properties and sets required constraints.
func (s *SemaProperties) Require(context string, rejectFlags SemaRejectFlags) {
	s.required.context = context
	s.required.rejectFlags = rejectFlags
	s.Derived.Clear()
}

// IsSet checks if the given rejectFlag is set as a required property.
func (s *SemaProperties) IsSet(rejectFlags SemaRejectFlags) bool {
	return s.required.rejectFlags&rejectFlags != 0
}

// Restore restores a copy of a SemaProperties. Use with:
// defer semaCtx.Properties.Restore(semaCtx.Properties)
func (s *SemaProperties) Restore(orig SemaProperties) {
	*s = orig
}

// SemaRejectFlags contains flags to filter out certain kinds of
// expressions.
type SemaRejectFlags int

// Valid values for SemaRejectFlags.
const (
	AllowAll SemaRejectFlags = 0

	// RejectAggregates rejects min(), max(), etc.
	RejectAggregates SemaRejectFlags = 1 << iota

	// RejectNestedAggregates rejects any use of aggregates inside the
	// argument list of another function call, which can itself be an aggregate
	// (RejectAggregates notwithstanding).
	RejectNestedAggregates

	// RejectNestedWindows rejects any use of window functions inside the
	// argument list of another window function.

	RejectNestedWindowFunctions

	// RejectWindowApplications rejects "x() over y", etc.
	RejectWindowApplications

	// RejectGenerators rejects any use of SRFs, e.g "generate_series()".
	RejectGenerators

	// RejectNestedGenerators rejects any use of SRFs inside the
	// argument list of another function call, which can itself be a SRF
	// (RejectGenerators notwithstanding).
	// This is used e.g. when processing the calls inside ROWS FROM.
	RejectNestedGenerators

	// RejectImpureFunctions rejects any non-const functions like now().
	RejectImpureFunctions

	// RejectSubqueries rejects subqueries in scalar contexts.
	RejectSubqueries

	// RejectSpecial is used in common places like the LIMIT clause.
	RejectSpecial SemaRejectFlags = RejectAggregates | RejectGenerators | RejectWindowApplications
)

// ScalarProperties contains the properties of the current scalar
// expression discovered during semantic analysis. The properties
// are collected prior to simplification, so some of the properties
// may not hold anymore by the time semantic analysis completes.
type ScalarProperties struct {
	// SeenAggregate is set to true if the expression originally
	// contained an aggregation.
	SeenAggregate bool

	// SeenWindowApplication is set to true if the expression originally
	// contained a window function.
	SeenWindowApplication bool

	// SeenGenerator is set to true if the expression originally
	// contained a SRF.
	SeenGenerator bool

	// SeenImpureFunctions is set to true if the expression originally
	// contained an impure function.
	SeenImpure bool

	// inFuncExpr is temporarily set to true while type checking the
	// parameters of a function. Used to process RejectNestedGenerators
	// properly.
	inFuncExpr bool

	// inWindowFunc is temporarily set to true while type checking the
	// parameters of a window function in order to reject nested window
	// functions.
	inWindowFunc bool
}

// Clear resets the scalar properties to defaults.
func (sp *ScalarProperties) Clear() {
	*sp = ScalarProperties{}
}

// MakeSemaContext initializes a simple SemaContext suitable
// for "lightweight" type checking such as the one performed for default
// expressions.
// Note: if queries with placeholders are going to be used,
// SemaContext.Placeholders.Init must be called separately.
func MakeSemaContext() SemaContext {
	return SemaContext{}
}

// isUnresolvedPlaceholder provides a nil-safe method to determine whether expr is an
// unresolved placeholder.
func (sc *SemaContext) isUnresolvedPlaceholder(expr Expr) bool {
	if sc == nil {
		return false
	}
	return sc.Placeholders.IsUnresolvedPlaceholder(expr)
}

// GetLocation returns the session timezone.
func (sc *SemaContext) GetLocation() *time.Location {
	if sc == nil || sc.Location == nil || *sc.Location == nil {
		return time.UTC
	}
	return *sc.Location
}

// GetAdditionMode implements ParseTimeContext.
func (sc *SemaContext) GetAdditionMode() duration.AdditionMode {
	return duration.AdditionModeCompatible
}

// GetRelativeParseTime implements ParseTimeContext.
func (sc *SemaContext) GetRelativeParseTime() time.Time {
	return timeutil.Now().In(sc.GetLocation())
}

// SetInUDR set set inUDR flag.
func (sc *SemaContext) SetInUDR(inUDR bool) {
	sc.InUDR = inUDR
}

type placeholderTypeAmbiguityError struct {
	idx types.PlaceholderIdx
}

func (err placeholderTypeAmbiguityError) Error() string {
	return fmt.Sprintf("could not determine data type of placeholder %s", err.idx)
}

type unexpectedTypeError struct {
	expr      Expr
	want, got types.T
}

func (err unexpectedTypeError) Error() string {
	return fmt.Sprintf("expected %s to be of type %s, found type %s", err.expr, err.want, err.got)
}

func decorateTypeCheckError(err error, format string, a ...interface{}) error {
	if _, ok := err.(placeholderTypeAmbiguityError); ok {
		return err
	}
	return errors.Wrapf(err, format, a...)
}

// TypeCheck performs type checking on the provided expression tree, returning
// the new typed expression tree, which additionally permits evaluation and type
// introspection globally and on each sub-tree.
//
// While doing so, it will fold numeric constants and bind placeholder names to
// their inferred types in the provided context. The optional desired parameter can
// be used to hint the desired type for the root of the resulting typed expression
// tree. Like with Expr.TypeCheck, it is not valid to provide a nil desired
// type. Instead, call it with the wildcard type types.Any if no specific type is
// desired.
func TypeCheck(expr Expr, ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	if desired == nil {
		panic("the desired type for tree.TypeCheck cannot be nil, use types.Any instead")
	}

	expr, err := FoldConstantLiterals(expr)
	if err != nil {
		return nil, err
	}

	if Iexpr, ok := expr.(*IndexedVar); ok && !IsNullExpr(expr) {
		if desired != types.Any && desired != Iexpr.Typ {
			cexpr := ChangeToCastExpr(Iexpr, desired, 0)
			expr = &cexpr
		}
	}

	if Iexpr, ok := expr.(*FuncExpr); ok && !IsNullExpr(expr) {
		if desired != types.Any && desired != Iexpr.Typ {
			if tempExpr, ok := Iexpr.Func.FunctionReference.(*FunctionDefinition); ok && tempExpr.Name == "isnull" {
				cexpr := ChangeToCastExpr(Iexpr, desired, 0)
				expr = &cexpr
			}
		}
	}

	return expr.TypeCheck(ctx, desired, useOrigin)
}

// TypeCheckAndRequire performs type checking on the provided expression tree in
// an identical manner to TypeCheck. It then asserts that the resulting TypedExpr
// has the provided return type, returning both the typed expression and an error
// if it does not.
func TypeCheckAndRequire(
	expr Expr, ctx *SemaContext, required types.T, op string, useOrigin bool,
) (TypedExpr, error) {
	if op == "SET CLUSTER SETTING server.sql_session_timeout" {
		e, _ := FoldConstantLiterals(expr)
		if val, ok := e.(*NumVal); ok {
			f, _ := constant.Float64Val(val.Value)
			find := strings.Contains(fmt.Sprint(f), ".")
			if find {
				return nil, errors.Errorf("parameter \"server.sql_session_timeout\" requires an integer value \nDETAIL: '%v' is a decimal", f)
			}
		}
	}
	typedExpr, err := TypeCheck(expr, ctx, required, useOrigin)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ.Equivalent(required) || typ == types.Unknown) {
		if t, ok := typedExpr.(*BinaryExpr); ok {
			if t.Operator == Xor {
				res := ChangeToCastExpr(typedExpr, required, 0)
				return &res, nil
			}
		}
		return typedExpr, pgerror.NewErrorf(
			pgcode.DatatypeMismatch, "argument of %s must be type %s, not type %s", op, required, typ)
	}
	return typedExpr, nil
}

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(ctx, expr.Left, "AND argument", useOrigin)
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(ctx, expr.Right, "AND argument", useOrigin)
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.Typ = types.Bool
	return expr, nil
}

var (
	jsonBinOP = []BinaryOperator{JSONFetchText, JSONFetchTextPath, JSONFetchVal, JSONFetchValPath}
)

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	ops := BinOps[expr.Operator]
	// var exprop *ExprOpType

	opkind := "B"
	// useOrigin := false
	for _, v := range jsonBinOP {
		if expr.Operator == v {
			useOrigin = true
		}
	}
	typedSubExprs, fns, err := toCastTypeCheckOverloadedExprs(ctx, opkind, expr.Operator, desired, ops, true, useOrigin, expr.Left, expr.Right)
	if err != nil {
		return nil, err
	}

	leftTyped, rightTyped := typedSubExprs[0], typedSubExprs[1]
	leftReturn := leftTyped.ResolvedType()
	rightReturn := rightTyped.ResolvedType()

	// Return NULL if at least one overload is possible, NULL is an argument,
	// and none of the overloads accept NULL.
	if leftReturn == types.Unknown || rightReturn == types.Unknown {
		if len(fns) > 0 {
			noneAcceptNull := true
			for _, e := range fns {
				if e.(*BinOp).NullableArgs {
					noneAcceptNull = false
					break
				}
			}
			if noneAcceptNull {
				return DNull, nil
			}
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	if len(fns) != 1 {
		var desStr string
		if desired != types.Any {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("<%s> %s <%s>%s", leftReturn, expr.Operator, rightReturn, desStr)
		if len(fns) == 0 {
			return nil,
				pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedBinaryOpErrFmt, sig)
		}
		fnsStr := formatCandidates(expr.Operator.String(), fns, false)
		return nil,
			pgerror.NewErrorf(pgcode.AmbiguousFunction,
				ambiguousBinaryOpErrFmt, sig).SetHintf(candidatesHintFmt, fnsStr)
	}

	binOp := fns[0].(*BinOp)

	// Register operator usage in telemetry.
	if binOp.counter != nil {
		telemetry.Inc(binOp.counter)
	}

	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = binOp
	if t, ok := binOp.returnType()(typedSubExprs).(types.TArray); ok {
		switch t.Typ.(type) {
		case types.TCollatedString:
			if expr.Operator == Concat {
				if _, Lok := leftReturn.(types.TArray); Lok {
					expr.Typ = leftReturn
				} else {
					expr.Typ = rightReturn
				}
			}
		}
	}
	if expr.Typ == nil {
		expr.Typ = binOp.returnType()(typedSubExprs)
	}
	return expr, nil
}

// // ExprEquals ExprEquals
// func ExprEquals(ctx *SemaContext, expr1, expr2 Expr) (bool, int) {
// 	commonTypeExpr1, _ := expr1.TypeCheck(ctx, types.Any)
// 	commonTypeExpr2, _ := expr2.TypeCheck(ctx, types.Any)
// 	ei := commonTypeExpr1.ResolvedType()
// 	switch ei {
// 	case types.Int:
// 		expr1Int, ok := commonTypeExpr1.(*DInt)
// 		if ok {
// 			expr2Int, ok2 := commonTypeExpr2.(*DInt)
// 			if ok2 {
// 				if *expr1Int == *expr2Int {
// 					return true, 0
// 				}
// 				return false, 0
//
// 			}
// 			return false, 1
//
// 		}
// 		return false, 1
//
// 	case types.Decimal:
// 		expr1Decimal, ok := commonTypeExpr1.(*DDecimal)
// 		if ok {
// 			expr2Decimal, ok2 := commonTypeExpr2.(*DDecimal)
// 			if ok2 {
// 				if expr1Decimal.Decimal.Cmp(&expr2Decimal.Decimal) == 0 {
// 					return true, 0
// 				}
// 				return false, 0
//
// 			}
// 			return false, 1
//
// 		}
// 		return false, 1
//
// 	case types.Float:
// 		expr1Float, ok := commonTypeExpr1.(*DFloat)
// 		if ok {
// 			expr2Float, ok2 := commonTypeExpr2.(*DFloat)
// 			if ok2 {
// 				if *expr1Float == *expr2Float {
// 					return true, 0
// 				}
// 				return false, 0
// 			}
// 			return false, 1
// 		}
// 		return false, 1
//
// 	case types.String:
// 		expr1String, ok := commonTypeExpr1.(*DString)
// 		if ok {
// 			expr2String, ok2 := commonTypeExpr2.(*DString)
// 			if ok2 {
// 				if *expr1String == *expr2String {
// 					return true, 0
// 				}
// 				return false, 0
//
// 			}
// 			return false, 1
// 		}
// 		return false, 1
//
// 	}
// 	return false, 0
// }

// 如果类型全部未知，则返回类型定为 string
func allIsUnknowType(ts []types.T) (types.T, bool) {
	for _, t := range ts {
		if t != types.Unknown {
			return nil, false
		}
	}
	return types.String, true
}

// 除了一个确定的类型之外，其他全是unknow，则返回那个确定的类型
func oneTypeWithUnknow(ts []types.T) (types.T, bool) {
	var temp types.T
	for _, t := range ts {
		if t != types.Unknown {
			if temp == nil {
				temp = t
			} else if t != temp {
				return nil, false
			}
		}
	}
	return temp, true
}

// 如果所有的类型都是数字类型，则返回结果也应该为数字类型，且返回精度更高的那一个
// 比如 int 和 float，都是数字类型，返回精度更高的float
func allIsNumType(ts []types.T) (types.T, bool) {
	highPriorityType := types.Int
	numPriorityMap := make(map[types.T]int)
	numPriorityMap[types.Int] = 1
	numPriorityMap[types.Decimal] = 2
	numPriorityMap[types.Float] = 3
	for _, t := range ts {
		if _, ok := numPriorityMap[t]; !ok {
			return nil, false
		}
		if numPriorityMap[highPriorityType] < numPriorityMap[t] {
			highPriorityType = t
		}
	}
	return highPriorityType, true
}

// 若类型不相同，但都是表示时间的类型，则返回类型应为timestampTZ
func allIsTimeType(ts []types.T) (types.T, bool) {
	var currentType = ts[0]
	change := false
	for _, t := range ts {
		if !types.IsDateTimeType(t) {
			return nil, false
		}
		if currentType != t {
			change = true
		}
	}
	if change {
		return types.Timestamp, true
	}
	return ts[0], true

}

// 类型不全相同情况下，如果全部类型属于string、 bytes 或者 unknow ，则返回类型为 string
func allIsStrType(ts []types.T) (types.T, bool) {
	for _, t := range ts {
		if t != types.String && t != types.Bytes || t != types.Unknown {
			return nil, false
		}
	}
	return types.String, true
}

// judge if a set of types are the same type
func checkSameType(ts []types.T) bool {
	for _, t := range ts[1:] {
		if t != ts[0] {
			return false
		}
	}
	return true
}

// choose a fit type for case expr from a set of types
func determinCaseRetType(ts []types.T) (types.T, error) {
	// 类型不全相同要进行额外的判断
	if !checkSameType(ts) {
		if t, ok := allIsUnknowType(ts); ok {
			return t, nil
		} else if t, ok := oneTypeWithUnknow(ts); ok {
			return t, nil
		} else if t, ok := allIsNumType(ts); ok {
			return t, nil
		} else if t, ok := allIsTimeType(ts); ok {
			return t, nil
		} else if t, ok := allIsStrType(ts); ok {
			return t, nil
		}
		return nil, nil

	}
	// 类型相同返回类型也是这个类型
	return ts[0], nil

}

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	var retType types.T
	tmpExprs := make([]Expr, 0, len(expr.Whens)+1)
	if expr.Expr != nil {
		tmpExprs = tmpExprs[:0]
		tmpExprs = append(tmpExprs, expr.Expr)
		for _, when := range expr.Whens {
			tmpExprs = append(tmpExprs, when.Cond)
		}
		typedSubExprs, _, err := TypeCheckSameTypedExprs(ctx, types.Any, false, useOrigin, tmpExprs...)
		if err != nil {
			return nil, decorateTypeCheckError(err, "incompatible condition type:")
		}
		expr.Expr = typedSubExprs[0]
		for i, whenCond := range typedSubExprs[1:] {
			expr.Whens[i].Cond = whenCond
		}
	} else {
		// If expr.Expr is nil, the WHEN clauses contain boolean expressions.
		for i, when := range expr.Whens {
			typedCond, err := typeCheckAndRequireBoolean(ctx, when.Cond, "condition", useOrigin)
			if err != nil {
				return nil, err
			}
			expr.Whens[i].Cond = typedCond
		}
	}
	tmpExprs = tmpExprs[:0]
	for _, when := range expr.Whens {
		tmpExprs = append(tmpExprs, when.Val)
	}
	if expr.Else != nil {
		tmpExprs = append(tmpExprs, expr.Else)
	}

	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, true, useOrigin, tmpExprs...)
	if err != nil {
		if typedSubExprs != nil {
			if tmp, ok := err.(unexpectedTypeError); ok {
				if _, ok := tmp.want.(types.TTuple); ok {
					return nil, errors.Errorf("please check your return type or value in case expression!")
				}
			}
			allReturnType := make([]types.T, 0)
			for _, typedExprNode := range typedSubExprs {
				allReturnType = append(allReturnType, typedExprNode.ResolvedType())
			}
			retType, _ = determinCaseRetType(allReturnType)
		} else {
			return nil, errors.Errorf("please check your return type or value in case expression!")
		}
		if retType == nil {
			return nil, err
		}
		for i, t := range typedSubExprs {
			if retType != t.ResolvedType() {
				ok, c := isCastDeepValid(t.ResolvedType(), retType)
				if ok && !IsNullExpr(t) {
					telemetry.Inc(c)
					tempCastExpr := ChangeToCastExpr(t, retType, 0)
					typedSubExprs[i] = &tempCastExpr
				}
			}
		}
	}
	if expr.Else != nil {
		expr.Else = typedSubExprs[len(typedSubExprs)-1]
		typedSubExprs = typedSubExprs[:len(typedSubExprs)-1]
	}
	for i, whenVal := range typedSubExprs {
		expr.Whens[i].Val = whenVal
	}
	expr.Typ = retType
	return expr, nil
}

// IsCastDeepValid IsCastDeepValid
var IsCastDeepValid = isCastDeepValid

func isCastDeepValid(castFrom, castTo types.T) (bool, telemetry.Counter) {
	if castFrom == types.Int2 || castFrom == types.Int4 {
		castFrom = types.Int
	}
	if castTo == types.Int2 || castTo == types.Int4 {
		castTo = types.Int
	}
	if castFrom == types.Float4 {
		castFrom = types.Float
	}
	if castTo == types.Float4 {
		castTo = types.Float
	}

	castFrom = types.UnwrapType(castFrom)
	castTo = types.UnwrapType(castTo)
	if castTo.FamilyEqual(types.FamArray) && castFrom.FamilyEqual(types.FamArray) {
		ok, c := isCastDeepValid(castFrom.(types.TArray).Typ, castTo.(types.TArray).Typ)
		if ok {
			telemetry.Inc(sqltelemetry.ArrayCastCounter)
		}
		return ok, c
	}
	for _, t := range validCastTypes(castTo) {
		if castFrom.FamilyEqual(t.fromT) {
			return true, t.counter
		}
	}
	return false, nil
}

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck(ctx *SemaContext, typ types.T, useOrigin bool) (TypedExpr, error) {
	returnType := expr.castType()
	ifChange := false
	isSpecialCase := returnType == types.Time && typ == types.Interval
	_, isArrayTyp := typ.(types.TArray)
	if _, isArray := returnType.(types.TArray); (!useOrigin || isSpecialCase) && !isArray && !isArrayTyp && typ != nil {
		if types.Any != typ && returnType != typ && typ != types.BitArray && !IsNullExpr(expr) {
			if ok, c := isCastDeepValid(returnType, typ); ok {
				telemetry.Inc(c)
				cexpr := ChangeToCastExpr(expr, typ, 0)
				ifChange = true
				expr = &cexpr
			}
		}
	}
	// The desired type provided to a CastExpr is ignored. Instead,
	// types.Any is passed to the child of the cast. There are two
	// exceptions, described below.
	desired := types.Any
	//desired := returnType
	switch {
	case isConstant(expr.Expr):
		if canConstantBecome(expr.Expr.(Constant), returnType, useOrigin) {
			// If a Constant is subject to a cast which it can naturally become (which
			// is in its resolvable type set), we desire the cast's type for the Constant,
			// which will result in the CastExpr becoming an identity cast.
			desired = returnType

			// If the type doesn't have any possible parameters (like length,
			// precision), the CastExpr becomes a no-op and can be elided.
			switch expr.Type.(type) {
			case *coltypes.TBool, *coltypes.TDate, *coltypes.TTime, *coltypes.TTimestamp, *coltypes.TTimestampTZ,
				*coltypes.TInterval, *coltypes.TBytes:
				return expr.Expr.TypeCheck(ctx, returnType, useOrigin)
			}
		}
	case ctx.isUnresolvedPlaceholder(expr.Expr):
		// This case will be triggered if ProcessPlaceholderAnnotations found
		// the same placeholder in another location where it was either not
		// the child of a cast, or was the child of a cast to a different type.
		// In this case, we default to inferring a STRING for the placeholder.
		desired = types.String
	}
	// if castExpr.Expr is a funcExpr, set desired type=types.Any
	if _, ok := expr.Expr.(*FuncExpr); ok {
		desired = types.Any
	} else if _, ok := expr.Expr.(*NumVal); ok && returnType == types.BitArray {
		desired = types.Any
	}
	typedSubExpr, err := expr.Expr.TypeCheck(ctx, desired, useOrigin)
	if err != nil {
		return nil, err
	}

	castFrom := typedSubExpr.ResolvedType()

	if ok, c := isCastDeepValid(castFrom, returnType); ok {
		telemetry.Inc(c)
		expr.Expr = typedSubExpr
		if ifChange {
			expr.Typ = typ
		} else {
			expr.Typ = returnType
		}
		return expr, nil
	}

	return nil, pgerror.NewErrorf(pgcode.CannotCoerce, "invalid cast: %s -> %s", castFrom, expr.Type)
}

// TypeCheck implements the Expr interface.
func (expr *IndirectionExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	for i, t := range expr.Indirection {
		if t.Slice {
			return nil, pgerror.UnimplementedWithIssueErrorf(32551, "ARRAY slicing in %s", expr)
		}
		if i > 0 {
			return nil, pgerror.UnimplementedWithIssueDetailErrorf(32552, "ind", "multidimensional indexing: %s", expr)
		}

		beginExpr, err := typeCheckAndRequire(ctx, t.Begin, types.Int, "ARRAY subscript", useOrigin)
		if err != nil {
			return nil, err
		}
		t.Begin = beginExpr
	}

	subExpr, err := expr.Expr.TypeCheck(ctx, types.TArray{Typ: desired}, useOrigin)
	if err != nil {
		return nil, err
	}
	typ := types.UnwrapType(subExpr.ResolvedType())
	arrType, ok := typ.(types.TArray)
	if !ok {
		return nil, pgerror.NewErrorf(pgcode.DatatypeMismatch, "cannot subscript type %s because it is not an array", typ)
	}
	expr.Expr = subExpr
	expr.Typ = arrType.Typ

	telemetry.Inc(sqltelemetry.ArraySubscriptCounter)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *AnnotateTypeExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	annotType := expr.annotationType()
	subExpr, err := typeCheckAndRequire(ctx, expr.Expr, annotType, fmt.Sprintf("type annotation for %v as %s, found", expr.Expr, annotType), useOrigin)
	if err != nil {
		return nil, err
	}
	return subExpr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CollateExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	_, err := language.Parse(expr.Locale)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid locale %s", expr.Locale)
	}
	subExpr, err := expr.Expr.TypeCheck(ctx, types.String, useOrigin)
	if err != nil {
		return nil, err
	}
	t := subExpr.ResolvedType()
	if types.IsStringType(t) || t == types.Unknown {
		expr.Expr = subExpr
		expr.Typ = types.TCollatedString{Locale: expr.Locale}
		return expr, nil
	}
	return nil, pgerror.NewErrorf(pgcode.DatatypeMismatch, "incompatible type for COLLATE: %s", t)
}

// NewTypeIsNotCompositeError generates an error suitable to report
// when a ColumnAccessExpr or TupleStar is applied to a non-composite
// type.
func NewTypeIsNotCompositeError(resolvedType types.T) error {
	return pgerror.NewErrorf(pgcode.WrongObjectType,
		"type %s is not composite", resolvedType,
	)
}

// TypeCheck implements the Expr interface.
func (expr *TupleStar) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	subExpr, err := expr.Expr.TypeCheck(ctx, desired, useOrigin)
	if err != nil {
		return nil, err
	}
	expr.Expr = subExpr
	resolvedType := subExpr.ResolvedType()

	// Alghough we're going to elide the tuple star, we need to ensure
	// the expression is indeed a labeled tuple first.
	if tType, ok := resolvedType.(types.TTuple); !ok || len(tType.Labels) == 0 {
		return nil, NewTypeIsNotCompositeError(resolvedType)
	}

	return subExpr, err
}

// ResolvedType implements the TypedExpr interface.
func (expr *TupleStar) ResolvedType() types.T {
	return expr.Expr.(TypedExpr).ResolvedType()
}

// TypeCheck implements the Expr interface.
func (expr *ColumnAccessExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	// If the context requires types T, we need to ask "Any tuple with
	// at least this label and the element type T for this label" from
	// the sub-expression. Of course, our type system does not support
	// this. So drop the type constraint instead.
	subExpr, err := expr.Expr.TypeCheck(ctx, types.Any, useOrigin)
	if err != nil {
		return nil, err
	}

	expr.Expr = subExpr
	resolvedType := types.UnwrapType(subExpr.ResolvedType())

	tType, ok := resolvedType.(types.TTuple)
	if !ok || len(tType.Labels) == 0 {
		return nil, NewTypeIsNotCompositeError(resolvedType)
	}

	// Go through all of the labels to find a match.
	expr.ColIndex = -1
	for i, label := range tType.Labels {
		if label == expr.ColName {
			expr.ColIndex = i
			break
		}
	}
	if expr.ColIndex < 0 {
		return nil, pgerror.NewErrorf(pgcode.DatatypeMismatch,
			"could not identify column %q in %s",
			ErrNameStringP(&expr.ColName), resolvedType,
		)
	}

	// Optimization: if the expression is actually a tuple, then
	// simplify the tuple straight away.
	if tExpr, ok := expr.Expr.(*Tuple); ok {
		return tExpr.Exprs[expr.ColIndex].(TypedExpr), nil
	}

	// Otherwise, let the expression be, it's probably more complex.
	// Just annotate the type of the result properly.
	expr.Typ = tType.Types[expr.ColIndex]
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CoalesceExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, false, useOrigin, expr.Exprs...)
	if err != nil {
		return nil, decorateTypeCheckError(err, fmt.Sprintf("incompatible %s expressions", expr.Name))
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.Typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CharExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	typedSubExprs, _, err := TypeCheckNotSameTypedExprs(ctx, desired, false, expr.Exprs...)
	if err != nil {
		return nil, decorateTypeCheckError(err, fmt.Sprintf("incompatible %s expressions", expr.Name))
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.Typ = types.String
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NvlExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	typedSubExprs, retType, isNotSame, err := TypeCheckSameOrNotTypedExprs(ctx, desired, false, expr.Exprs...)
	if err != nil {
		return nil, err
	}
	if isNotSame {
		var Priority = make([]int, len(typedSubExprs))
		for i, e := range typedSubExprs {
			Priority[i] = matchPriorityType[e.ResolvedType()]
		}
		highPriority := Priority[0]
		for i := 1; i < len(Priority); i++ {
			if highPriority > Priority[i] {
				highPriority = Priority[i]
			}
		}

		var NewExpr Exprs
		NewExpr = make(Exprs, len(expr.Exprs))
		for i, expr1 := range expr.Exprs {
			cexpr := ChangeToCastExpr(expr1, matchType[highPriority], 0)
			NewExpr[i] = &cexpr
		}
		typedSubExprs, retType, err = TypeCheckSameTypedExprs(ctx, desired, false, useOrigin, NewExpr...)
		if err != nil {
			return nil, decorateTypeCheckError(err, fmt.Sprintf("incompatible %s expressions", expr.Name))
		}
	}
	expr.Typ = retType
	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}

	return expr, nil

}

// TypeCheck implements the Expr interface.
func (expr *ComparisonExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	var leftTyped, rightTyped TypedExpr
	var fn *CmpOp
	var alwaysNull bool
	var err error
	if expr.Operator.hasSubOperator() {
		leftTyped, rightTyped, fn, alwaysNull, err = typeCheckComparisonOpWithSubOperator(ctx,
			expr.Operator,
			expr.SubOperator,
			expr.Left,
			expr.Right,
			useOrigin,
		)
	} else {
		leftTyped, rightTyped, fn, alwaysNull, err = typeCheckComparisonOp(ctx,
			expr.Operator,
			expr.Left,
			expr.Right,
			useOrigin,
		)
	}
	if err != nil {
		return nil, err
	}

	if alwaysNull {
		return DNull, nil
	}

	// Register operator usage in telemetry.
	if fn.counter != nil {
		telemetry.Inc(fn.counter)
	}

	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn
	expr.Typ = types.Bool
	return expr, nil
}

var (
	errOrderByIndexInWindow = pgerror.NewError(pgcode.FeatureNotSupported, "ORDER BY INDEX in window definition is not supported")
	errStarNotAllowed       = pgerror.NewError(pgcode.Syntax, "cannot use \"*\" in this context")
	errInvalidDefaultUsage  = pgerror.NewError(pgcode.Syntax, "DEFAULT can only appear in a VALUES list within INSERT or on the right side of a SET")
	errInvalidMaxUsage      = pgerror.NewError(pgcode.Syntax, "MAXVALUE can only appear within a range partition expression")
	errInvalidMinUsage      = pgerror.NewError(pgcode.Syntax, "MINVALUE can only appear within a range partition expression")
	errPrivateFunction      = pgerror.NewError(pgcode.FeatureNotSupported, "function reserved for internal use")
)

// NewAggInAggError creates an error for the case when an aggregate function is
// contained within another aggregate function.
func NewAggInAggError() error {
	return pgerror.NewErrorf(pgcode.Grouping, "aggregate function calls cannot be nested")
}

// NewInvalidNestedSRFError creates a rejection for a nested SRF.
func NewInvalidNestedSRFError(context string) error {
	return pgerror.NewErrorf(pgcode.FeatureNotSupported,
		"set-returning functions must appear at the top level of %s", context)
}

// NewInvalidFunctionUsageError creates a rejection for a special function.
func NewInvalidFunctionUsageError(class FunctionClass, context string) error {
	var cat string
	var code pgcode.Code
	switch class {
	case AggregateClass:
		cat = "aggregate"
		code = pgcode.Grouping
	case WindowClass:
		cat = "window"
		code = pgcode.Windowing
	case GeneratorClass:
		cat = "generator"
		code = pgcode.FeatureNotSupported
	}
	return pgerror.NewErrorf(code, "%s functions are not allowed in %s", cat, context)
}

// CheckFunctionUsage checks whether a given built-in function is
// allowed in the current context.
func (sc *SemaContext) CheckFunctionUsage(expr *FuncExpr, def *FunctionDefinition) error {
	if def.UnsupportedWithIssue != 0 {
		// Note: no need to embed the function name in the message; the
		// caller will add the function name as prefix.
		const msg = "this function is not supported"
		if def.UnsupportedWithIssue < 0 {
			return pgerror.Unimplemented(def.Name+"()", msg)
		}
		return pgerror.UnimplementedWithIssueDetailError(def.UnsupportedWithIssue, def.Name, msg)
	}
	if def.Private {
		return errors.Wrapf(errPrivateFunction, "%s()", def.Name)
	}
	if sc == nil {
		// We can't check anything further. Give up.
		return nil
	}

	if expr.IsWindowFunctionApplication() {
		if sc.Properties.required.rejectFlags&RejectWindowApplications != 0 {
			return NewInvalidFunctionUsageError(WindowClass, sc.Properties.required.context)
		}
		if sc.Properties.Derived.inWindowFunc &&
			sc.Properties.required.rejectFlags&RejectNestedWindowFunctions != 0 {
			return pgerror.Newf(pgcode.Windowing, "window function calls cannot be nested")
		}
		sc.Properties.Derived.SeenWindowApplication = true
	} else {
		// If it is an aggregate function *not used OVER a window*, then
		// we have an aggregation.
		if def.Class == AggregateClass {
			if sc.Properties.Derived.inFuncExpr &&
				sc.Properties.required.rejectFlags&RejectNestedAggregates != 0 {
				return NewAggInAggError()
			}
			if sc.Properties.required.rejectFlags&RejectAggregates != 0 {
				return NewInvalidFunctionUsageError(AggregateClass, sc.Properties.required.context)
			}
			sc.Properties.Derived.SeenAggregate = true
		}
	}
	if def.Class == GeneratorClass {
		if sc.Properties.Derived.inFuncExpr &&
			sc.Properties.required.rejectFlags&RejectNestedGenerators != 0 {
			return NewInvalidNestedSRFError(sc.Properties.required.context)
		}
		if sc.Properties.required.rejectFlags&RejectGenerators != 0 {
			return NewInvalidFunctionUsageError(GeneratorClass, sc.Properties.required.context)
		}
		sc.Properties.Derived.SeenGenerator = true
	}
	if def.Impure {
		if sc.Properties.required.rejectFlags&RejectImpureFunctions != 0 {
			// The code FeatureNotSupported is a bit misleading here,
			// because we probably can't support the feature at all. However
			// this error code matches PostgreSQL's in the same conditions.
			return pgerror.NewErrorf(pgcode.FeatureNotSupported,
				"impure functions are not allowed in %s", sc.Properties.required.context)
		}
		sc.Properties.Derived.SeenImpure = true
	}
	return nil
}

// CheckIsWindowOrAgg returns an error if the function definition is not a
// window function or an aggregate.
func CheckIsWindowOrAgg(def *FunctionDefinition) error {
	switch def.Class {
	case AggregateClass:
	case WindowClass:
	default:
		return pgerror.Newf(pgcode.WrongObjectType,
			"OVER specified, but %s() is neither a window function nor an aggregate function",
			def.Name)
	}
	return nil
}

// cancel type_conversion support int function
// var (
// 	// UseOriginTypeFuncs UseOriginTypeFuncs
// 	UseOriginTypeFuncs = []string{"has_any_column_privilege", "has_column_privilege", "has_database_privilege",
// 		"has_foreign_data_wrapper_privilege", "has_function_privilege", "has_language_privilege", "has_schema_privilege",
// 		"has_sequence_privilege", "has_server_privilege", "has_table_privilege", "has_tablespace_privilege", "has_type_privilege"}
// )

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	var searchPath sessiondata.SearchPath
	if ctx != nil {
		searchPath = ctx.SearchPath
	}
	def, err := expr.Func.Resolve(searchPath, false)
	if err != nil {
		return nil, err
	}

	if err := ctx.CheckFunctionUsage(expr, def); err != nil {
		return nil, errors.Wrapf(err, "%s()", def.Name)
	}
	if ctx != nil {
		// We'll need to remember we are in a function application to
		// generate suitable errors in checkFunctionUsage().  We cannot
		// set ctx.inFuncExpr earlier (in particular not before the call
		// to checkFunctionUsage() above) because the top-level FuncExpr
		// must be acceptable even if it is a SRF and
		// RejectNestedGenerators is set.
		defer func(ctx *SemaContext, prevFunc bool, prevWindow bool) {
			ctx.Properties.Derived.inFuncExpr = prevFunc
			ctx.Properties.Derived.inWindowFunc = prevWindow
		}(
			ctx,
			ctx.Properties.Derived.inFuncExpr,
			ctx.Properties.Derived.inWindowFunc,
		)
		ctx.Properties.Derived.inFuncExpr = true
		if expr.WindowDef != nil {
			ctx.Properties.Derived.inWindowFunc = true
		}
	}

	typedSubExprs, fns, _, err := typeCheckOverloadedExprs(ctx, desired, def.Definition, false, true, expr.Exprs...)
	if err != nil {
		return nil, errors.Wrapf(err, "%s()", def.Name)
	}

	// Return NULL if at least one overload is possible, no overload accepts
	// NULL arguments, the function isn't a generator builtin, and NULL is given
	// as an argument.
	if !def.NullableArgs && def.FunctionProperties.Class != GeneratorClass {
		for _, expr := range typedSubExprs {
			if expr.ResolvedType() == types.Unknown {
				return DNull, nil
			}
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	// TODO(nvanbenschoten): now that we can distinguish these, we can improve the
	//   error message the two report (e.g. "add casts please")
	if len(fns) != 1 {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, expr := range typedSubExprs {
			typeNames = append(typeNames, expr.ResolvedType().String())
		}
		var desStr string
		if desired != types.Any {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("%s(%s)%s", &expr.Func, strings.Join(typeNames, ", "), desStr)
		if len(fns) == 0 {
			if def.Category == "the user define func" {
				return nil, pgerror.NewErrorf(pgcode.UndefinedFunction, "No function/procedure matches the given name and argument types.")
			}
			if strings.ToLower(def.Name) == "any_value" {
				return nil, pgerror.NewErrorf(pgcode.UndefinedFunction, "Incorrect parameter count in the call to native function 'any_value'")
			}
			return nil, pgerror.NewErrorf(pgcode.UndefinedFunction, "unknown signature: %s", sig)
		}
		if strings.ToLower(def.Name) == "any_value" && len(typedSubExprs) == 1 && typedSubExprs[0].ResolvedType() == types.Unknown {
			return DNull, nil
		}
		fnsStr := formatCandidates(expr.Func.String(), fns, false)
		return nil, pgerror.NewErrorf(pgcode.AmbiguousFunction, "ambiguous call: %s, candidates are:\n%s", sig, fnsStr)
	}

	overloadImpl := fns[0].(*Overload)
	if overloadImpl.RunPriv != nil {
		// check overloadImpl's run priv
		return nil, overloadImpl.RunPriv
	}
	if expr.IsWindowFunctionApplication() {
		// Make sure the window function application is of either a built-in window
		// function or of a builtin aggregate function.
		switch def.Class {
		case AggregateClass:
		case WindowClass:
		default:
			return nil, pgerror.NewErrorf(pgcode.WrongObjectType,
				"OVER specified, but %s() is neither a window function nor an aggregate function",
				&expr.Func)
		}

		for i, partition := range expr.WindowDef.Partitions {
			typedPartition, err := partition.TypeCheck(ctx, types.Any, useOrigin)
			if err != nil {
				return nil, err
			}
			expr.WindowDef.Partitions[i] = typedPartition
		}
		for i, orderBy := range expr.WindowDef.OrderBy {
			if orderBy.OrderType != OrderByColumn {
				return nil, errOrderByIndexInWindow
			}
			typedOrderBy, err := orderBy.Expr.TypeCheck(ctx, types.Any, useOrigin)
			if err != nil {
				return nil, err
			}
			expr.WindowDef.OrderBy[i].Expr = typedOrderBy
		}
		if expr.WindowDef.Frame != nil {
			if err := expr.WindowDef.Frame.TypeCheck(ctx, expr.WindowDef); err != nil {
				return nil, err
			}
		}
	} else {
		// Make sure the window function builtins are used as window function applications.
		if def.Class == WindowClass {
			return nil, pgerror.NewErrorf(pgcode.WrongObjectType,
				"window function %s() requires an OVER clause", &expr.Func)
		}
	}

	if expr.Filter != nil {
		if def.Class != AggregateClass {
			// Same error message as Postgres. If we have a window function, only
			// aggregates accept a FILTER clause.
			return nil, pgerror.NewErrorf(pgcode.WrongObjectType,
				"FILTER specified but %s() is not an aggregate function", &expr.Func)
		}

		typedFilter, err := typeCheckAndRequireBoolean(ctx, expr.Filter, "FILTER expression", useOrigin)
		if err != nil {
			return nil, err
		}
		expr.Filter = typedFilter
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.fn = overloadImpl
	expr.fnProps = &def.FunctionProperties
	expr.Typ = overloadImpl.returnType()(typedSubExprs)
	if expr.Typ == UnknownReturnType {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, expr := range typedSubExprs {
			typeNames = append(typeNames, expr.ResolvedType().String())
		}
		return nil, pgerror.NewErrorf(
			pgcode.DatatypeMismatch,
			"could not determine polymorphic type: %s(%s)",
			&expr.Func,
			strings.Join(typeNames, ", "),
		)
	}
	if overloadImpl.counter != nil {
		telemetry.Inc(overloadImpl.counter)
	}
	return expr, nil
}

// TypeCheck checks that offsets of the window frame (if present) are of the
// appropriate type.
func (f *WindowFrame) TypeCheck(ctx *SemaContext, windowDef *WindowDef) error {
	bounds := f.Bounds
	startBound, endBound := bounds.StartBound, bounds.EndBound
	var requiredType types.T
	switch f.Mode {
	case ROWS:
		// In ROWS mode, offsets must be non-null, non-negative integers. Non-nullity
		// and non-negativity will be checked later.
		requiredType = types.Int
	case RANGE:
		// In RANGE mode, offsets must be non-null and non-negative datums of a type
		// dependent on the type of the ordering column. Non-nullity and
		// non-negativity will be checked later.
		if bounds.HasOffset() {
			// At least one of the bounds is of type 'value' PRECEDING or 'value' FOLLOWING.
			// We require ordering on a single column that supports addition/subtraction.
			if len(windowDef.OrderBy) != 1 {
				return pgerror.NewErrorf(pgcode.Windowing, "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column")
			}
			requiredType = windowDef.OrderBy[0].Expr.(TypedExpr).ResolvedType()
			if !types.IsAdditiveType(requiredType) {
				return pgerror.NewErrorf(pgcode.Windowing, fmt.Sprintf("RANGE with offset PRECEDING/FOLLOWING is not supported for column type %s", requiredType))
			}
			if types.IsDateTimeType(requiredType) {
				// Spec: for datetime ordering columns, the required type is an 'interval'.
				requiredType = types.Interval
			}
		}
	case GROUPS:
		// In GROUPS mode, offsets must be non-null, non-negative integers.
		// Non-nullity and non-negativity will be checked later.
		requiredType = types.Int
	default:
		panic("unexpected WindowFrameMode")
	}
	if startBound.HasOffset() {
		typedStartOffsetExpr, err := typeCheckAndRequire(ctx, startBound.OffsetExpr, requiredType, "window frame start", false)
		if err != nil {
			return err
		}
		startBound.OffsetExpr = typedStartOffsetExpr
	}
	if endBound != nil && endBound.HasOffset() {
		typedEndOffsetExpr, err := typeCheckAndRequire(ctx, endBound.OffsetExpr, requiredType, "window frame end", false)
		if err != nil {
			return err
		}
		endBound.OffsetExpr = typedEndOffsetExpr
	}
	return nil
}

// TypeCheck implements the Expr interface.
func (expr *IfErrExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	var typedCond, typedElse TypedExpr
	var retType types.T
	var err error
	if expr.Else == nil {
		typedCond, err = expr.Cond.TypeCheck(ctx, types.Any, useOrigin)
		if err != nil {
			return nil, err
		}
		retType = types.Bool
	} else {
		var typedSubExprs []TypedExpr
		typedSubExprs, retType, err = TypeCheckSameTypedExprs(ctx, desired, false, useOrigin, expr.Cond, expr.Else)
		if err != nil {
			return nil, decorateTypeCheckError(err, "incompatible IFERROR expressions")
		}
		typedCond, typedElse = typedSubExprs[0], typedSubExprs[1]
	}

	var typedErrCode TypedExpr
	if expr.ErrCode != nil {
		typedErrCode, err = typeCheckAndRequire(ctx, expr.ErrCode, types.String, "IFERROR", useOrigin)
		if err != nil {
			return nil, err
		}
	}

	expr.Cond = typedCond
	expr.Else = typedElse
	expr.ErrCode = typedErrCode
	expr.Typ = retType

	telemetry.Inc(sqltelemetry.IfErrCounter)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	typedCond, err := typeCheckAndRequireBoolean(ctx, expr.Cond, "IF condition", useOrigin)
	if err != nil {
		return nil, err
	}

	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, false, useOrigin, expr.True, expr.Else)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible IF expressions")
	}

	expr.Cond = typedCond
	expr.True, expr.Else = typedSubExprs[0], typedSubExprs[1]
	expr.Typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, types.Any, useOrigin)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.Typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	exprTyped, err := typeCheckAndRequireBoolean(ctx, expr.Expr, "NOT argument", useOrigin)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.Typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, false, useOrigin, expr.Expr1, expr.Expr2)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible NULLIF expressions")
	}

	expr.Expr1, expr.Expr2 = typedSubExprs[0], typedSubExprs[1]
	expr.Typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(ctx, expr.Left, "OR argument", useOrigin)
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(ctx, expr.Right, "OR argument", useOrigin)
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.Typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ParenExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, desired, useOrigin)
	if err != nil {
		return nil, err
	}
	// Parentheses are semantically unimportant and can be removed/replaced
	// with its nested expression in our plan. This makes type checking cleaner.
	return exprTyped, nil
}

// TypeCheck implements the Expr interface.  This function has a valid
// implementation only for testing within this package. During query
// execution, ColumnItems are replaced to IndexedVars prior to type
// checking.
func (expr *ColumnItem) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	name := expr.String()
	if _, ok := presetTypesForTesting[name]; ok {
		return expr, nil
	}
	return nil, pgerror.NewErrorf(pgcode.UndefinedColumn,
		"column %q does not exist", ErrString(expr))
}

// TypeCheck implements the Expr interface.
func (expr UnqualifiedStar) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return nil, errStarNotAllowed
}

// TypeCheck implements the Expr interface.
func (expr *UnresolvedName) TypeCheck(
	s *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	v, err := expr.NormalizeVarName()
	if err != nil {
		return nil, err
	}
	return v.TypeCheck(s, desired, useOrigin)
}

// TypeCheck implements the Expr interface.
func (expr *AllColumnsSelector) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return nil, pgerror.NewErrorf(pgcode.Syntax, "cannot use %q in this context", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	leftTyped, fromTyped, _, _, err := typeCheckComparisonOp(ctx, GT, expr.Left, expr.From, useOrigin)
	if err != nil {
		return nil, err
	}
	_, toTyped, _, _, err := typeCheckComparisonOp(ctx, LT, expr.Left, expr.To, useOrigin)
	if err != nil {
		return nil, err
	}

	expr.Left, expr.From, expr.To = leftTyped, fromTyped, toTyped
	expr.Typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Subquery) TypeCheck(
	sc *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	if sc != nil && sc.Properties.required.rejectFlags&RejectSubqueries != 0 {
		return nil, pgerror.NewErrorf(pgcode.FeatureNotSupported,
			"subqueries are not allowed in %s", sc.Properties.required.context)
	}
	expr.assertTyped()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	ops := UnaryOps[expr.Operator]

	typedSubExprs, fns, _, err := typeCheckOverloadedExprs(ctx, desired, ops, false, useOrigin, expr.Expr)
	if err != nil {
		return nil, err
	}

	exprTyped := typedSubExprs[0]
	exprReturn := exprTyped.ResolvedType()

	// Return NULL if at least one overload is possible and NULL is an argument.
	if len(fns) > 0 {
		if exprReturn == types.Unknown {
			return DNull, nil
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	if len(fns) != 1 {
		var desStr string
		if desired != types.Any {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("%s <%s>%s", expr.Operator, exprReturn, desStr)
		if len(fns) == 0 {
			return nil,
				pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedUnaryOpErrFmt, sig)
		}
		fnsStr := formatCandidates(expr.Operator.String(), fns, false)
		return nil, pgerror.NewErrorf(pgcode.AmbiguousFunction,
			ambiguousUnaryOpErrFmt, sig).SetHintf(candidatesHintFmt, fnsStr)
	}

	unaryOp := fns[0].(*UnaryOp)

	// Register operator usage in telemetry.
	if unaryOp.counter != nil {
		telemetry.Inc(unaryOp.counter)
	}

	expr.Expr = exprTyped
	expr.fn = unaryOp
	expr.Typ = unaryOp.returnType()(typedSubExprs)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return nil, errInvalidDefaultUsage
}

// TypeCheck implements the Expr interface.
func (expr PartitionMinVal) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return nil, errInvalidMinUsage
}

// TypeCheck implements the Expr interface.
func (expr PartitionMaxVal) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return nil, errInvalidMaxUsage
}

// TypeCheck implements the Expr interface.
func (expr *NumVal) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return typeCheckConstant(expr, ctx, desired, useOrigin)
}

// TypeCheck implements the Expr interface.
func (expr *StrVal) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	if cs, ok := desired.(types.TCollatedString); ok {
		var res CollateExpr
		subexpr, err := expr.TypeCheck(ctx, types.String, useOrigin)
		res.Expr = subexpr
		res.Typ = desired
		res.Locale = cs.Locale
		return &res, err
	}
	return typeCheckConstant(expr, ctx, desired, useOrigin)
}

// TypeCheck implements the Expr interface.
func (expr AdditionalExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return &expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	// Ensure the number of labels matches the number of expressions.
	if len(expr.Labels) > 0 && len(expr.Labels) != len(expr.Exprs) {
		return nil, pgerror.NewErrorf(pgcode.Syntax,
			"mismatch in tuple definition: %d expressions, %d labels",
			len(expr.Exprs), len(expr.Labels),
		)
	}

	expr.typ = types.TTuple{Types: make([]types.T, len(expr.Exprs))}
	for i, subExpr := range expr.Exprs {
		desiredElem := types.Any
		if t, ok := desired.(types.TTuple); ok && len(t.Types) > i {
			desiredElem = t.Types[i]
		}
		typedExpr, err := subExpr.TypeCheck(ctx, desiredElem, useOrigin)
		if err != nil {
			return nil, err
		}
		expr.Exprs[i] = typedExpr
		expr.typ.Types[i] = typedExpr.ResolvedType()
	}
	// Copy the labels if there are any.
	if len(expr.Labels) > 0 {
		// Ensure that there are no repeat labels.
		for i := range expr.Labels {
			for j := 0; j < i; j++ {
				if expr.Labels[i] == expr.Labels[j] {
					return nil, pgerror.NewErrorf(pgcode.Syntax,
						"found duplicate tuple label: %q", ErrNameStringP(&expr.Labels[i]),
					)
				}
			}
		}

		expr.typ.Labels = make([]string, len(expr.Labels))
		for i := range expr.Labels {
			expr.typ.Labels[i] = lex.NormalizeName(expr.Labels[i])
		}
	}
	return expr, nil
}

var errAmbiguousArrayType = pgerror.NewErrorf(pgcode.IndeterminateDatatype, "cannot determine type of empty array. "+
	"Consider annotating with the desired type, for example ARRAY[]:::int[]")

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	desiredParam := types.Any
	if arr, ok := desired.(types.TArray); ok {
		desiredParam = arr.Typ
	}

	if len(expr.Exprs) == 0 {
		if desiredParam == types.Any {
			return nil, errAmbiguousArrayType
		}
		expr.Typ = types.TArray{Typ: desiredParam}
		return expr, nil
	}

	typedSubExprs, typ, err := TypeCheckSameTypedExprs(ctx, desiredParam, false, useOrigin, expr.Exprs...)
	if err != nil {
		return nil, err
	}

	expr.Typ = types.TArray{Typ: typ}
	for i := range typedSubExprs {
		expr.Exprs[i] = typedSubExprs[i]
	}

	telemetry.Inc(sqltelemetry.ArrayConstructorCounter)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ArrayFlatten) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	desiredParam := types.Any
	if arr, ok := desired.(types.TArray); ok {
		desiredParam = arr.Typ
	}

	subqueryTyped, err := expr.Subquery.TypeCheck(ctx, desiredParam, useOrigin)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	expr.Typ = types.TArray{Typ: subqueryTyped.ResolvedType()}

	telemetry.Inc(sqltelemetry.ArrayFlattenCounter)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Placeholder) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	// Perform placeholder typing. This function is only called during Prepare,
	// when there are no available values for the placeholders yet, because
	// during Execute all placeholders are replaced from the AST before type
	// checking.
	if !ctx.InUDR {
		if typ, ok := ctx.Placeholders.Type(expr.Idx); ok {
			if !desired.Equivalent(typ) {
				// This indicates there's a conflict between what the type system thinks
				// the type for this position should be, and the actual type of the
				// placeholder. This actual placeholder type could be either a type hint
				// (from pgwire or from a SQL PREPARE), or the actual value type.
				//
				// To resolve this situation, we *override* the placeholder type with what
				// the type system expects. Then, when the value is actually sent to us
				// later, we cast the input value (whose type is the expected type) to the
				// desired type here.
				typ = desired
			}
			// We call SetType regardless of the above condition to inform the
			// placeholder struct that this placeholder is locked to its type and cannot
			// be overridden again.
			if err := ctx.Placeholders.SetType(expr.Idx, typ); err != nil {
				return nil, err
			}
			expr.Typ = typ
			return expr, nil
		}
		if desired.IsAmbiguous() {
			return nil, placeholderTypeAmbiguityError{expr.Idx}
		}
		if err := ctx.Placeholders.SetType(expr.Idx, desired); err != nil {
			return nil, err
		}
		expr.Typ = desired
		return expr, nil
	}
	// if desired.IsAmbiguous() {
	// 	return nil, placeholderTypeAmbiguityError{expr.Idx}
	// }
	if err := ctx.Placeholders.SetType(expr.Idx, types.OidToType[ctx.UDRParms[int(expr.Idx)].VarOid]); err != nil {
		return nil, err
	}
	typOid := ctx.UDRParms[int(expr.Idx)].VarOid
	return &PlpgsqlVar{Index: int(expr.Idx), VarType: types.OidToType[typOid], DesiredTyp: desired, IsPlaceHolder: true}, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBitArray) TypeCheck(ctx *SemaContext, typ types.T, useOrigin bool) (TypedExpr, error) {
	if useOrigin {
		return d, nil
	}
	if ok, c := isCastDeepValid(d.ResolvedType(), typ); ok && d.ResolvedType() != typ && !IsNullExpr(d) {
		telemetry.Inc(c)
		castExpr := ChangeToCastExpr(d, typ, 0)
		return &castExpr, nil
	}
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
//func (d *DBool) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }
func (d *DBool) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	if desired == types.Any {
		desired = types.Bool
	}
	return typeCheckConstant(d, ctx, desired, useOrigin)
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInt) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DFloat) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDecimal) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DString) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DCollatedString) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBytes) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DUuid) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DIPAddr) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
//func (d *DDate) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }
func (d *DDate) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return typeCheckConstant(d, ctx, desired, useOrigin)
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTime) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestamp) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestampTZ) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInterval) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DJSON) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTuple) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DArray) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOid) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOidWrapper) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d dNull) TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error) {
	return d, nil
}

// typeCheckAndRequireTupleElems asserts that all elements in the Tuple are
// comparable to the input Expr given the input comparison operator.
func typeCheckAndRequireTupleElems(
	ctx *SemaContext, expr TypedExpr, tuple *Tuple, op ComparisonOperator, useOrigin bool,
) (TypedExpr, error) {
	tuple.typ = types.TTuple{Types: make([]types.T, len(tuple.Exprs))}
	for i, subExpr := range tuple.Exprs {
		// Require that the sub expression is comparable to the required type.
		_, rightTyped, _, _, err := typeCheckComparisonOp(ctx, op, expr, subExpr, useOrigin)
		if err != nil {
			return nil, err
		}
		tuple.Exprs[i] = rightTyped
		tuple.typ.Types[i] = rightTyped.ResolvedType()
	}
	return tuple, nil
}

func typeCheckAndRequireBoolean(
	ctx *SemaContext, expr Expr, op string, useOrigin bool,
) (TypedExpr, error) {
	return typeCheckAndRequire(ctx, expr, types.Bool, op, useOrigin)
}

func typeCheckAndRequire(
	ctx *SemaContext, expr Expr, required types.T, op string, useOrigin bool,
) (TypedExpr, error) {
	typedExpr, err := expr.TypeCheck(ctx, required, useOrigin)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ == types.Unknown || typ.Equivalent(required)) {
		if funcExpr, ok := typedExpr.(*FuncExpr); ok && required == types.Bool {
			resExpr := ChangeToCastExpr(funcExpr, required, 0)
			return &resExpr, nil
		}
		temp := typedExpr.ResolvedType()
		if isValid(temp) && required == types.Bool {
			resExpr := ChangeToCastExpr(typedExpr, required, 0)
			return &resExpr, nil
		}
		return nil, pgerror.NewErrorf(pgcode.DatatypeMismatch, "incompatible %s type: %s", op, typ)
	}
	return typedExpr, nil
}

func isValid(t types.T) bool {
	switch t {
	case types.Int2, types.Int4, types.Int, types.Decimal:
		return true
	}
	return false
}

const (
	compSignatureFmt          = "<%s> %s <%s>"
	compSignatureWithSubOpFmt = "<%s> %s %s <%s>"
	compExprsFmt              = "%s %s %s: %v"
	compExprsWithSubOpFmt     = "%s %s %s %s: %v"
	unsupportedCompErrFmt     = "unsupported comparison operator: %s"
	unsupportedUnaryOpErrFmt  = "unsupported unary operator: %s"
	unsupportedBinaryOpErrFmt = "unsupported binary operator: %s"
	ambiguousCompErrFmt       = "ambiguous comparison operator: %s"
	ambiguousUnaryOpErrFmt    = "ambiguous unary operator: %s"
	ambiguousBinaryOpErrFmt   = "ambiguous binary operator: %s"
	candidatesHintFmt         = "candidates are:\n%s"
)

func typeCheckComparisonOpWithSubOperator(
	ctx *SemaContext, op, subOp ComparisonOperator, left, right Expr, useOrigin bool,
) (_ TypedExpr, _ TypedExpr, _ *CmpOp, alwaysNull bool, _ error) {
	// Parentheses are semantically unimportant and can be removed/replaced
	// with its nested expression in our plan. This makes type checking cleaner.
	left = StripParens(left)
	right = StripParens(right)

	// Determine the set of comparisons are possible for the sub-operation,
	// which will be memoized.
	foldedOp, _, _, _, _ := foldComparisonExpr(subOp, nil, nil)
	ops := CmpOps[foldedOp]

	var cmpTypeLeft, cmpTypeRight types.T
	var leftTyped, rightTyped TypedExpr
	if array, isConstructor := right.(*Array); isConstructor {
		// If the right expression is an (optionally nested) array constructor, we
		// perform type inference on the array elements and the left expression.
		sameTypeExprs := make([]Expr, len(array.Exprs)+1)
		sameTypeExprs[0] = left
		copy(sameTypeExprs[1:], array.Exprs)

		typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, types.Any, false, useOrigin, sameTypeExprs...)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsWithSubOpFmt, left, subOp, op, right, err)
			return nil, nil, nil, false,
				pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		// Determine TypedExpr and comparison type for left operand.
		leftTyped = typedSubExprs[0]
		cmpTypeLeft = retType

		// Determine TypedExpr and comparison type for right operand, making sure
		// all ParenExprs on the right are properly type checked.
		for i, typedExpr := range typedSubExprs[1:] {
			array.Exprs[i] = typedExpr
		}
		array.Typ = types.TArray{Typ: retType}

		rightTyped = array
		cmpTypeRight = retType

		// Return early without looking up a CmpOp if the comparison type is types.Null.
		if leftTyped.ResolvedType() == types.Unknown || retType == types.Unknown {
			return leftTyped, rightTyped, nil, true /* alwaysNull */, nil
		}
	} else {
		// If the right expression is not an array constructor, we type the left
		// expression in isolation.
		var err error
		leftTyped, err = left.TypeCheck(ctx, types.Any, false)
		if err != nil {
			return nil, nil, nil, false, err
		}
		cmpTypeLeft = leftTyped.ResolvedType()

		if tuple, ok := right.(*Tuple); ok {
			// If right expression is a tuple, we require that all elements' inferred
			// type is equivalent to the left's type.
			rightTyped, err = typeCheckAndRequireTupleElems(ctx, leftTyped, tuple, subOp, useOrigin)
			if err != nil {
				return nil, nil, nil, false, err
			}
		} else {
			// Try to type the right expression as an array of the left's type.
			// If right is an sql.subquery Expr, it should already be typed.
			// TODO(richardwu): If right is a subquery, we should really
			// propagate the left type as a desired type for the result column.
			rightTyped, err = right.TypeCheck(ctx, types.TArray{Typ: cmpTypeLeft}, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if op == Any || op == Some {
				if str, ok := rightTyped.(*DString); ok {
					if cmpTypeLeft == types.Unknown {
						rightTyped, err = parseStringAs(types.TArray{Typ: types.String}, string(*str), ctx, false)
						if err != nil {
							return nil, nil, nil, false, err
						}
						return leftTyped, rightTyped, nil, true /* alwaysNull */, nil
					}
					if cmpTypeLeft != types.String && cmpTypeLeft != types.Decimal && cmpTypeLeft != types.Int {
						return nil, nil, nil, false /* alwaysNull */, fmt.Errorf("not supported convert %s to array", cmpTypeLeft.String())
					}
					rightTyped, err = parseStringAs(types.TArray{Typ: cmpTypeLeft}, string(*str), ctx, false)
					if err != nil {
						return nil, nil, nil, false, err
					}
				}
			}
		}

		rightReturn := rightTyped.ResolvedType()
		if cmpTypeLeft == types.Unknown || rightReturn == types.Unknown {
			return leftTyped, rightTyped, nil, true /* alwaysNull */, nil
		}

		switch rightUnwrapped := types.UnwrapType(rightReturn).(type) {
		case types.TArray:
			cmpTypeRight = rightUnwrapped.Typ
		case types.TTuple:
			if len(rightUnwrapped.Types) == 0 {
				// Literal tuple contains no elements, or subquery tuple returns 0 rows.
				cmpTypeRight = cmpTypeLeft
			} else {
				// Literal tuples and subqueries were type checked such that all
				// elements have comparable types with the left. Once that's true, we
				// can safely grab the first element's type as the right type for the
				// purposes of computing the correct comparison function below, since
				// if two datum types are comparable, it's legal to call .Compare on
				// one with the other.
				cmpTypeRight = rightUnwrapped.Types[0]
			}
		default:
			sigWithErr := fmt.Sprintf(compExprsWithSubOpFmt, left, subOp, op, right,
				fmt.Sprintf("op %s <right> requires array, tuple or subquery on right side", op))
			return nil, nil, nil, false, pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}
	}
	fn, ok := ops.lookupImpl(cmpTypeLeft, cmpTypeRight)
	if !ok {
		return nil, nil, nil, false, subOpCompError(cmpTypeLeft, rightTyped.ResolvedType(), subOp, op)
	}
	return leftTyped, rightTyped, fn, false, nil
}

func subOpCompError(leftType, rightType types.T, subOp, op ComparisonOperator) *pgerror.Error {
	sig := fmt.Sprintf(compSignatureWithSubOpFmt, leftType, subOp, op, rightType)
	return pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
}

// typeCheckSubqueryWithIn checks the case where the right side of an IN
// expression is a subquery.
func typeCheckSubqueryWithIn(left, right types.T) error {
	if rTuple, ok := right.(types.TTuple); ok {
		// Subqueries come through as a tuple{T}, so T IN tuple{T} should be
		// accepted.
		if len(rTuple.Types) != 1 {
			return pgerror.NewErrorf(pgcode.InvalidParameterValue,
				unsupportedCompErrFmt, fmt.Sprintf(compSignatureFmt, left, In, right))
		}
		if !left.Equivalent(rTuple.Types[0]) {
			return pgerror.NewErrorf(pgcode.InvalidParameterValue,
				unsupportedCompErrFmt, fmt.Sprintf(compSignatureFmt, left, In, right))
		}
	}
	return nil
}

func typeCheckComparisonOp(
	ctx *SemaContext, op ComparisonOperator, left, right Expr, useOrigin bool,
) (_ TypedExpr, _ TypedExpr, _ *CmpOp, alwaysNull bool, _ error) {
	foldedOp, foldedLeft, foldedRight, switched, _ := foldComparisonExpr(op, left, right)
	ops := CmpOps[foldedOp]

	_, leftIsTuple := foldedLeft.(*Tuple)
	rightTuple, rightIsTuple := foldedRight.(*Tuple)
	switch {
	case foldedOp == In && rightIsTuple:
		sameTypeExprs := make([]Expr, len(rightTuple.Exprs)+1)
		sameTypeExprs[0] = foldedLeft
		copy(sameTypeExprs[1:], rightTuple.Exprs)

		typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, types.Any, false, useOrigin, sameTypeExprs...)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		fn, ok := ops.lookupImpl(retType, types.FamTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, retType, op, types.FamTuple)
			return nil, nil, nil, false,
				pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}

		typedLeft := typedSubExprs[0]
		typedSubExprs = typedSubExprs[1:]

		rightTuple.typ = types.TTuple{Types: make([]types.T, len(typedSubExprs))}
		for i, typedExpr := range typedSubExprs {
			rightTuple.Exprs[i] = typedExpr
			rightTuple.typ.Types[i] = retType
		}
		if switched {
			return rightTuple, typedLeft, fn, false, nil
		}
		return typedLeft, rightTuple, fn, false, nil

	case leftIsTuple && rightIsTuple:
		fn, ok := ops.lookupImpl(types.FamTuple, types.FamTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, types.FamTuple, op, types.FamTuple)
			return nil, nil, nil, false,
				pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}
		// Using non-folded left and right to avoid having to swap later.
		typedLeft, typedRight, err := typeCheckTupleComparison(ctx, op, left.(*Tuple), right.(*Tuple), useOrigin)
		if err != nil {
			return nil, nil, nil, false, err
		}
		return typedLeft, typedRight, fn, false, nil
	}

	// For comparisons, we do not stimulate the typing of untyped NULL with the
	// other side's type, because comparisons of NULL with anything else are
	// defined to return NULL anyways. Should the SQL dialect ever be extended with
	// comparisons that can return non-NULL on NULL input, the `inBinOp` parameter
	// may need altering.
	typedSubExprs, fns, err := toCastTypeCheckOverloadedExprs(
		ctx, "C", foldedOp, types.Any, ops, true, false, foldedLeft, foldedRight,
	)
	if err != nil {
		return nil, nil, nil, false, err
	}

	leftExpr, rightExpr := typedSubExprs[0], typedSubExprs[1]
	if switched {
		leftExpr, rightExpr = rightExpr, leftExpr
	}
	leftReturn := leftExpr.ResolvedType()
	rightReturn := rightExpr.ResolvedType()

	/*
	 * 占位符应该保证和比较字符的类型完全一致，  与 collatedstring 类型进行比较的 placeholder
	 * 经过上述检查，会变成 collatedstring{}, Locale没有正确赋值
	 */
	if placeHolder, ok := left.(*Placeholder); ok {
		placeHolder.Typ = rightReturn
		ctx.Placeholders.Types[placeHolder.Idx] = rightReturn
	}

	if placeHolder, ok := right.(*Placeholder); ok {
		placeHolder.Typ = leftReturn
		ctx.Placeholders.Types[placeHolder.Idx] = leftReturn
	}

	if foldedOp == In {
		if err := typeCheckSubqueryWithIn(leftReturn, rightReturn); err != nil {
			return nil, nil, nil, false, err
		}
	}

	// Return early if at least one overload is possible, NULL is an argument,
	// and none of the overloads accept NULL.
	if leftReturn == types.Unknown || rightReturn == types.Unknown {
		if len(fns) > 0 {
			noneAcceptNull := true
			for _, e := range fns {
				if e.(*CmpOp).NullableArgs {
					noneAcceptNull = false
					break
				}
			}
			if noneAcceptNull {
				return leftExpr, rightExpr, nil, true /* alwaysNull */, err
			}
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	collationMismatch := leftReturn.FamilyEqual(types.FamCollatedString) && !leftReturn.Equivalent(rightReturn)
	if len(fns) != 1 || collationMismatch {
		sig := fmt.Sprintf(compSignatureFmt, leftReturn, op, rightReturn)
		if len(fns) == 0 || collationMismatch {
			return nil, nil, nil, false,
				pgerror.NewErrorf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}
		fnsStr := formatCandidates(op.String(), fns, false)
		return nil, nil, nil, false,
			pgerror.NewErrorf(pgcode.AmbiguousFunction,
				ambiguousCompErrFmt, sig).SetHintf(candidatesHintFmt, fnsStr)
	}

	return leftExpr, rightExpr, fns[0].(*CmpOp), false, nil
}

type typeCheckExprsState struct {
	ctx *SemaContext

	exprs           []Expr
	typedExprs      []TypedExpr
	constIdxs       []int // index into exprs/typedExprs
	placeholderIdxs []int // index into exprs/typedExprs
	resolvableIdxs  []int // index into exprs/typedExprs
}

// TypeCheckSameTypedExprs type checks a list of expressions, asserting that all
// resolved TypeExprs have the same type. An optional desired type can be provided,
// which will hint that type which the expressions should resolve to, if possible.
func TypeCheckSameTypedExprs(
	ctx *SemaContext, desired types.T, isCase bool, useOrigin bool, exprs ...Expr,
) ([]TypedExpr, types.T, error) {
	switch len(exprs) {
	case 0:
		return nil, nil, nil
	case 1:
		typedExpr, err := exprs[0].TypeCheck(ctx, desired, useOrigin)
		if err != nil {
			return nil, nil, err
		}
		return []TypedExpr{typedExpr}, typedExpr.ResolvedType(), nil
	}

	// Handle tuples, which will in turn call into this function recursively for each element.
	if _, ok := exprs[0].(*Tuple); ok {
		return typeCheckSameTypedTupleExprs(ctx, desired, isCase, useOrigin, exprs...)
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten): Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	constIdxs, placeholderIdxs, resolvableIdxs := typeCheckSplitExprs(ctx, exprs)

	s := typeCheckExprsState{
		ctx:             ctx,
		exprs:           exprs,
		typedExprs:      typedExprs,
		constIdxs:       constIdxs,
		placeholderIdxs: placeholderIdxs,
		resolvableIdxs:  resolvableIdxs,
	}

	switch {
	case len(resolvableIdxs) == 0 && len(constIdxs) == 0:
		if err := typeCheckSameTypedPlaceholders(s, desired, useOrigin); err != nil {
			return nil, nil, err
		}
		return typedExprs, desired, nil
	case len(resolvableIdxs) == 0:
		return typeCheckConstsAndPlaceholdersWithDesired(s, desired, useOrigin)
	default:
		firstValidIdx := -1
		firstValidType := types.Unknown
		for i, j := range resolvableIdxs {
			typedExpr, err := exprs[j].TypeCheck(ctx, desired, useOrigin)
			if err != nil {
				return nil, nil, err
			}
			typedExprs[j] = typedExpr
			if returnType := typedExpr.ResolvedType(); returnType != types.Unknown {
				firstValidType = returnType
				firstValidIdx = i
				break
			}
		}

		if firstValidType == types.Unknown {
			switch {
			case len(constIdxs) > 0:
				return typeCheckConstsAndPlaceholdersWithDesired(s, desired, useOrigin)
			case len(placeholderIdxs) > 0:
				p := s.exprs[placeholderIdxs[0]].(*Placeholder)
				return nil, nil, placeholderTypeAmbiguityError{p.Idx}
			default:
				return typedExprs, types.Unknown, nil
			}
		}
		var unExpectedTypeErr error
		for _, i := range resolvableIdxs[firstValidIdx+1:] {
			typedExpr, err := exprs[i].TypeCheck(ctx, firstValidType, useOrigin)
			if err != nil {
				return nil, nil, err
			}
			if isCase {
				if typ := typedExpr.ResolvedType(); !(typ.Equivalent(firstValidType) || typ == types.Unknown) && unExpectedTypeErr == nil {
					// return nil, nil, unexpectedTypeError{exprs[i], firstValidType, typ}
					unExpectedTypeErr = unexpectedTypeError{exprs[i], firstValidType, typ}
				}
			} else {
				if typ := typedExpr.ResolvedType(); !(typ.Equivalent(firstValidType) || typ == types.Unknown) && unExpectedTypeErr == nil {
					return nil, nil, unexpectedTypeError{exprs[i], firstValidType, typ}
				}
			}
			typedExprs[i] = typedExpr
		}
		// 对于case的情况，用types.Unknow进行TypeCheck，得出所有的类型，对于有类型不相同的情况，记录错误，使得后续可以处理
		if isCase {
			typ := types.Unknown
			if len(constIdxs) > 0 {
				for _, i := range s.constIdxs {
					typedExpr, err := s.exprs[i].TypeCheck(s.ctx, typ, useOrigin)
					currentTyp := typedExpr.ResolvedType()
					if currentTyp != firstValidType && unExpectedTypeErr == nil {
						unExpectedTypeErr = unexpectedTypeError{exprs[i], firstValidType, currentTyp}
					}
					if err != nil {
						// In this case, even though the constExpr has been shown to be
						// upcastable to typ based on canConstantBecome, it can't actually be
						// parsed as typ.
						return nil, nil, err
					}
					s.typedExprs[i] = typedExpr
				}
			}
			if len(placeholderIdxs) > 0 {
				for _, i := range s.placeholderIdxs {
					typedExpr, err := s.exprs[i].TypeCheck(s.ctx, typ, useOrigin)
					currentTyp := typedExpr.ResolvedType()
					if currentTyp != firstValidType && unExpectedTypeErr == nil {
						unExpectedTypeErr = unexpectedTypeError{exprs[i], firstValidType, currentTyp}
					}
					if err != nil {
						return nil, nil, err
					}
					s.typedExprs[i] = typedExpr
				}
			}
		} else {
			if len(constIdxs) > 0 {
				if _, err := typeCheckSameTypedConsts(s, firstValidType, true, useOrigin); err != nil {
					return nil, nil, err
				}
			}
			if len(placeholderIdxs) > 0 {
				if err := typeCheckSameTypedPlaceholders(s, firstValidType, useOrigin); err != nil {
					return nil, nil, err
				}
			}
		}
		return typedExprs, firstValidType, unExpectedTypeErr
	}
}

//TypeCheckSameOrNotTypedExprs just for nvlExpr to typeCheck
func TypeCheckSameOrNotTypedExprs(
	ctx *SemaContext, desired types.T, isCase bool, exprs ...Expr,
) ([]TypedExpr, types.T, bool, error) {
	switch len(exprs) {
	case 0:
		return nil, nil, false, nil
	case 1:
		typedExpr, err := exprs[0].TypeCheck(ctx, desired, false)
		if err != nil {
			return nil, nil, false, err
		}
		return []TypedExpr{typedExpr}, typedExpr.ResolvedType(), false, nil
	}

	// Handle tuples, which will in turn call into this function recursively for each element.
	if _, ok := exprs[0].(*Tuple); ok {
		typeExpr, t, err := typeCheckSameTypedTupleExprs(ctx, desired, isCase, false, exprs...)
		return typeExpr, t, false, err
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten): Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	firstValidType := types.Unknown
	for i, j := range exprs {
		typedExpr, err := j.TypeCheck(ctx, desired, false)
		if err != nil {
			return nil, nil, false, err
		}
		typedExprs[i] = typedExpr
	}

	for i := 0; i < len(typedExprs)-1; i++ {
		if typedExprs[i].ResolvedType() != types.Unknown {
			if typedExprs[i+1].ResolvedType() != types.Unknown {
				if typedExprs[i].ResolvedType() != typedExprs[i+1].ResolvedType() {
					return typedExprs, typedExprs[i].ResolvedType(), true, nil
				}
			}
			firstValidType = typedExprs[i].ResolvedType()
		} else if typedExprs[i+1].ResolvedType() != types.Unknown {
			return typedExprs, typedExprs[i+1].ResolvedType(), false, nil
		}

	}
	return typedExprs, firstValidType, false, nil
}

//TypeCheckNotSameTypedExprs just for CharExpr to typeCheck
func TypeCheckNotSameTypedExprs(
	ctx *SemaContext, desired types.T, isCase bool, exprs ...Expr,
) ([]TypedExpr, types.T, error) {
	switch len(exprs) {
	case 0:
		return nil, nil, nil
	case 1:
		typedExpr, err := exprs[0].TypeCheck(ctx, desired, false)
		if err != nil {
			return nil, nil, err
		}
		return []TypedExpr{typedExpr}, typedExpr.ResolvedType(), nil
	}
	// Handle tuples, which will in turn call into this function recursively for each element.
	if _, ok := exprs[0].(*Tuple); ok {
		return typeCheckSameTypedTupleExprs(ctx, desired, isCase, false, exprs...)
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten): Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	firstValidType := types.String
	for i, j := range exprs {
		typedExpr, err := j.TypeCheck(ctx, desired, false)
		if err != nil {
			return nil, nil, err
		}
		typedExprs[i] = typedExpr
	}
	return typedExprs, firstValidType, nil
}

var matchPriorityType = map[types.T]int{
	types.Decimal: 3,
	types.Float:   4,
	types.Float4:  5,
	types.Int:     6,
	types.Int4:    7,
	types.Int2:    8,
	types.Unknown: 12,
}

var matchType = map[int]types.T{
	0:  types.String,
	3:  types.Decimal,
	4:  types.Decimal,
	5:  types.Decimal,
	6:  types.Int,
	7:  types.Int,
	8:  types.Int,
	12: types.Unknown,
}

// Used to set placeholders to the desired typ.
func typeCheckSameTypedPlaceholders(s typeCheckExprsState, typ types.T, useOrigin bool) error {
	for _, i := range s.placeholderIdxs {
		typedExpr, err := typeCheckAndRequire(s.ctx, s.exprs[i], typ, "placeholder", useOrigin)
		if err != nil {
			return err
		}
		s.typedExprs[i] = typedExpr
	}
	return nil
}

// Used to type check constants to the same type. An optional typ can be
// provided to signify the desired shared type, which can be set to the
// required shared type using the second parameter.
func typeCheckSameTypedConsts(
	s typeCheckExprsState, typ types.T, required bool, useOrigin bool,
) (types.T, error) {
	setTypeForConsts := func(typ types.T) (types.T, error) {
		for _, i := range s.constIdxs {
			typedExpr, err := typeCheckAndRequire(s.ctx, s.exprs[i], typ, "constant", useOrigin)
			if err != nil {
				// In this case, even though the constExpr has been shown to be
				// upcastable to typ based on canConstantBecome, it can't actually be
				// parsed as typ.
				return nil, err
			}
			s.typedExprs[i] = typedExpr
		}
		return typ, nil
	}

	// If typ is not a wildcard, all consts try to become typ.
	if typ != types.Any {
		all := true
		for _, i := range s.constIdxs {
			if !canConstantBecome(s.exprs[i].(Constant), typ, useOrigin) {
				if required {
					typedExpr, err := s.exprs[i].TypeCheck(s.ctx, types.Any, useOrigin)
					if err != nil {
						return nil, err
					}
					return nil, unexpectedTypeError{s.exprs[i], typ, typedExpr.ResolvedType()}
				}
				all = false
				break
			}
		}
		if all {
			return setTypeForConsts(typ)
		}
	}

	// If not all constIdxs could become typ but they have a mutual
	// resolvable type, use this common type.
	if bestType, ok := commonConstantType(s.exprs, s.constIdxs); ok {
		return setTypeForConsts(bestType)
	}

	// If not, we want to force an error because the constants cannot all
	// become the same type.
	reqTyp := typ
	for _, i := range s.constIdxs {
		typedExpr, err := s.exprs[i].TypeCheck(s.ctx, reqTyp, useOrigin)
		if err != nil {
			return nil, err
		}
		if typ := typedExpr.ResolvedType(); !typ.Equivalent(reqTyp) {
			return nil, unexpectedTypeError{s.exprs[i], reqTyp, typ}
		}
		if reqTyp == types.Any {
			reqTyp = typedExpr.ResolvedType()
		}
	}
	panic("should throw error above")
}

// Used to type check all constants with the optional desired type. The
// type that is chosen here will then be set to any placeholders.
func typeCheckConstsAndPlaceholdersWithDesired(
	s typeCheckExprsState, desired types.T, useOrigin bool,
) ([]TypedExpr, types.T, error) {
	typ, err := typeCheckSameTypedConsts(s, desired, false, useOrigin)
	if err != nil {
		return nil, nil, err
	}
	if len(s.placeholderIdxs) > 0 {
		if err := typeCheckSameTypedPlaceholders(s, typ, useOrigin); err != nil {
			return nil, nil, err
		}
	}
	return s.typedExprs, typ, nil
}

// typeCheckSplitExprs splits the expressions into three groups of indexes:
// - Constants
// - Placeholders
// - All other Exprs
func typeCheckSplitExprs(
	ctx *SemaContext, exprs []Expr,
) (constIdxs []int, placeholderIdxs []int, resolvableIdxs []int) {
	for i, expr := range exprs {
		switch {
		case isConstant(expr):
			constIdxs = append(constIdxs, i)
		case ctx.isUnresolvedPlaceholder(expr):
			placeholderIdxs = append(placeholderIdxs, i)
		default:
			resolvableIdxs = append(resolvableIdxs, i)
		}
	}
	return constIdxs, placeholderIdxs, resolvableIdxs
}

// typeCheckTupleComparison type checks a comparison between two tuples,
// asserting that the elements of the two tuples are comparable at each index.
func typeCheckTupleComparison(
	ctx *SemaContext, op ComparisonOperator, left *Tuple, right *Tuple, useOrigin bool,
) (TypedExpr, TypedExpr, error) {
	// All tuples must have the same length.
	tupLen := len(left.Exprs)
	if err := checkTupleHasLength(right, tupLen); err != nil {
		return nil, nil, err
	}
	left.typ = types.TTuple{Types: make([]types.T, tupLen)}
	right.typ = types.TTuple{Types: make([]types.T, tupLen)}
	for elemIdx := range left.Exprs {
		leftSubExpr := left.Exprs[elemIdx]
		rightSubExpr := right.Exprs[elemIdx]
		leftSubExprTyped, rightSubExprTyped, _, _, err := typeCheckComparisonOp(ctx, op, leftSubExpr, rightSubExpr, useOrigin)
		if err != nil {
			exps := Exprs([]Expr{left, right})
			return nil, nil, pgerror.NewErrorf(pgcode.DatatypeMismatch, "tuples %s are not comparable at index %d: %s",
				&exps, elemIdx+1, err)
		}
		left.Exprs[elemIdx] = leftSubExprTyped
		left.typ.Types[elemIdx] = leftSubExprTyped.ResolvedType()
		right.Exprs[elemIdx] = rightSubExprTyped
		right.typ.Types[elemIdx] = rightSubExprTyped.ResolvedType()
	}
	return left, right, nil
}

// typeCheckSameTypedTupleExprs type checks a list of expressions, asserting that all
// are tuples which have the same type. The function expects the first provided expression
// to be a tuple, and will panic if it is not. However, it does not expect all other
// expressions are tuples, and will return a sane error if they are not. An optional
// desired type can be provided, which will hint that type which the expressions should
// resolve to, if possible.
func typeCheckSameTypedTupleExprs(
	ctx *SemaContext, desired types.T, isCase bool, useOrigin bool, exprs ...Expr,
) ([]TypedExpr, types.T, error) {
	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten): Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	// All other exprs must be tuples.
	first := exprs[0].(*Tuple)
	if err := checkAllExprsAreTuples(ctx, exprs[1:], useOrigin); err != nil {
		return nil, nil, err
	}

	// All tuples must have the same length.
	firstLen := len(first.Exprs)
	if err := checkAllTuplesHaveLength(exprs[1:], firstLen); err != nil {
		return nil, nil, err
	}

	// Pull out desired types.
	var desiredTuple types.TTuple
	if t, ok := desired.(types.TTuple); ok {
		desiredTuple = t
	}

	// All expressions at the same indexes must be the same type.
	resTypes := types.TTuple{Types: make([]types.T, firstLen)}
	sameTypeExprs := make([]Expr, len(exprs))
	for elemIdx := range first.Exprs {
		for tupleIdx, expr := range exprs {
			sameTypeExprs[tupleIdx] = expr.(*Tuple).Exprs[elemIdx]
		}
		desiredElem := types.Any
		if len(desiredTuple.Types) > elemIdx {
			desiredElem = desiredTuple.Types[elemIdx]
		}
		typedSubExprs, resType, err := TypeCheckSameTypedExprs(ctx, desiredElem, isCase, useOrigin, sameTypeExprs...)
		if err != nil {
			return nil, nil, pgerror.NewErrorf(pgcode.DatatypeMismatch, "tuples %s are not the same type: %v", Exprs(exprs), err)
		}
		for j, typedExpr := range typedSubExprs {
			exprs[j].(*Tuple).Exprs[elemIdx] = typedExpr
		}
		resTypes.Types[elemIdx] = resType
	}
	for tupleIdx, expr := range exprs {
		expr.(*Tuple).typ = resTypes
		typedExprs[tupleIdx] = expr.(TypedExpr)
	}
	return typedExprs, resTypes, nil
}

func checkAllExprsAreTuples(ctx *SemaContext, exprs []Expr, useOrigin bool) error {
	for _, expr := range exprs {
		if _, ok := expr.(*Tuple); !ok {
			typedExpr, err := expr.TypeCheck(ctx, types.Any, useOrigin)
			if err != nil {
				return err
			}
			return unexpectedTypeError{expr, types.FamTuple, typedExpr.ResolvedType()}
		}
	}
	return nil
}

func checkAllTuplesHaveLength(exprs []Expr, expectedLen int) error {
	for _, expr := range exprs {
		if err := checkTupleHasLength(expr.(*Tuple), expectedLen); err != nil {
			return err
		}
	}
	return nil
}

func checkTupleHasLength(t *Tuple, expectedLen int) error {
	if len(t.Exprs) != expectedLen {
		return pgerror.NewErrorf(pgcode.DatatypeMismatch, "expected tuple %v to have a length of %d", t, expectedLen)
	}
	return nil
}

type placeholderAnnotationVisitor struct {
	types PlaceholderTypes
	state []annotationState
	err   error
	// errIdx stores the placeholder to which err applies. Used to select the
	// error for the smallest index.
	errIdx types.PlaceholderIdx
}

// annotationState keeps extra information relating to the type of a
// placeholder.
type annotationState uint8

const (
	noType annotationState = iota

	// typeFromHint indicates that the type for the placeholder is set from a
	// provided type hint (from pgwire, or PREPARE AS arguments).
	typeFromHint

	// typeFromAnnotation indicates that the type for this placeholder is set from
	// a type annotation.
	typeFromAnnotation

	// typeFromCast indicates that the type for this placeholder is set from
	// a cast. This type can be replaced if an annotation is found, or discarded
	// if conflicting casts are found.
	typeFromCast

	// conflictingCasts indicates that we haven't found an annotation for the
	// placeholder, and we cannot determine the type from casts: either we found
	// conflicting casts, or we found an appearance of the placeholder without a
	// cast.
	conflictingCasts
)

func (v *placeholderAnnotationVisitor) setErr(idx types.PlaceholderIdx, err error) {
	if v.err == nil || v.errIdx >= idx {
		v.err = err
		v.errIdx = idx
	}
}

func (v *placeholderAnnotationVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *AnnotateTypeExpr:
		if arg, ok := t.Expr.(*Placeholder); ok {
			annotationType := t.annotationType()
			switch v.state[arg.Idx] {
			case noType, typeFromCast, conflictingCasts:
				// An annotation overrides casts.
				v.types[arg.Idx] = annotationType
				v.state[arg.Idx] = typeFromAnnotation

			case typeFromAnnotation:
				// Verify that the annotations are consistent.
				if !annotationType.Equivalent(v.types[arg.Idx]) {
					v.setErr(arg.Idx, pgerror.NewErrorf(
						pgcode.DatatypeMismatch,
						"multiple conflicting type annotations around %s",
						arg.Idx,
					))
				}

			case typeFromHint:
				// Verify that the annotation is consistent with the type hint.
				if prevType := v.types[arg.Idx]; !annotationType.Equivalent(prevType) {
					v.setErr(arg.Idx, pgerror.NewErrorf(
						pgcode.DatatypeMismatch,
						"type annotation around %s conflicts with specified type %s",
						arg.Idx, v.types[arg.Idx],
					))
				}

			default:
				panic("unhandled state")
			}
			return false, expr
		}

	case *CastExpr:
		if arg, ok := t.Expr.(*Placeholder); ok {
			castType := t.castType()
			switch v.state[arg.Idx] {
			case noType:
				v.types[arg.Idx] = castType
				v.state[arg.Idx] = typeFromCast

			case typeFromCast:
				// Verify that the casts are consistent.
				if !castType.Equivalent(v.types[arg.Idx]) {
					v.state[arg.Idx] = conflictingCasts
					v.types[arg.Idx] = nil
				}

			case typeFromHint, typeFromAnnotation:
				// A cast never overrides a hinted or annotated type.

			case conflictingCasts:
				// We already saw inconsistent casts, or a "bare" placeholder; ignore
				// this cast.

			default:
				panic("unhandled state")
			}
			return false, expr
		}

	case *Placeholder:
		switch v.state[t.Idx] {
		case noType, typeFromCast:
			// A "bare" placeholder prevents type determination from casts.
			v.state[t.Idx] = conflictingCasts
			v.types[t.Idx] = nil

		case typeFromHint, typeFromAnnotation:
			// We are not relying on casts to determine the type, nothing to do.

		case conflictingCasts:
			// We already decided not to use casts, nothing to do.

		default:
			panic("unhandled state")
		}
	}
	return true, expr
}

func (*placeholderAnnotationVisitor) VisitPost(expr Expr) Expr { return expr }

// ProcessPlaceholderAnnotations performs an order-independent global traversal of the
// provided Statement, annotating all placeholders with a type in either of the following
// situations:
//
//  - the placeholder is the subject of an explicit type annotation in at least one
//    of its occurrences. If it is subject to multiple explicit type annotations
//    where the types are not all in agreement, or if the placeholder already has
//    a type hint in the placeholder map which conflicts with the explicit type
//    annotation type, an error will be thrown.
//
//  - the placeholder is the subject to a cast of the same type in all
//    occurrences of the placeholder. If the placeholder is subject to casts of
//    multiple types, or if it has occurrences without a cast, no error will be
//    thrown but the type will not be inferred. If the placeholder already has a
//    type hint, that type will be kept regardless of any casts.
//
// See docs/RFCS/20160203_typing.md for more details on placeholder typing (in
// particular section "First pass: placeholder annotations").
//
// The typeHints slice contains the client-provided hints and is populated with
// any newly assigned types. It is assumed to be pre-sized to the number of
// placeholders in the statement and is populated accordingly.
//
// TODO(nvanbenschoten): Can this visitor and map be preallocated (like normalizeVisitor)?
func ProcessPlaceholderAnnotations(stmt Statement, typeHints PlaceholderTypes) error {
	v := placeholderAnnotationVisitor{
		types: typeHints,
		state: make([]annotationState, len(typeHints)),
	}

	for placeholder := range typeHints {
		if typeHints[placeholder] != nil {
			v.state[placeholder] = typeFromHint
		}
	}

	walkStmt(&v, stmt)
	return v.err
}

// StripMemoizedFuncs strips memoized function references from expression trees.
// This is necessary to permit equality checks using reflect.DeepEqual.
// Used in testing.
func StripMemoizedFuncs(expr Expr) Expr {
	expr, _ = WalkExpr(stripFuncsVisitor{}, expr)
	return expr
}

type stripFuncsVisitor struct{}

func (v stripFuncsVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *UnaryExpr:
		t.fn = nil
	case *BinaryExpr:
		t.fn = nil
	case *ComparisonExpr:
		t.fn = nil
	case *FuncExpr:
		t.fn = nil
		t.fnProps = nil
	}
	return true, expr
}

func (stripFuncsVisitor) VisitPost(expr Expr) Expr { return expr }

// ChooseFunctionInOverload get a correct overload from overloads for a determine funcName and given args
func (callStmt *CallStmt) ChooseFunctionInOverload(
	ctx *SemaContext, desired types.T, ops []OverloadImpl,
) (TypedExprs, []OverloadImpl, []uint8, error) {

	typedExprs, fns, overloadIdxs, err := typeCheckOverloadedExprs(ctx, desired, ops, false, true, callStmt.Args...)
	if err != nil {
		return nil, nil, nil, err
	}
	return typedExprs, []OverloadImpl(fns), overloadIdxs, nil
}

//ChooseFunctionInOverload used to choose function in over load
func ChooseFunctionInOverload(
	ctx *SemaContext, desired types.T, trig *CreateTrigger, ops []OverloadImpl,
) (TypedExprs, []OverloadImpl, []uint8, error) {

	typedExprs, fns, s, err := typeCheckOverloadedExprs(ctx, desired, ops, false, true, trig.Args...)
	if err != nil {
		return typedExprs, nil, s, err
	}
	return typedExprs, fns, s, nil
}
