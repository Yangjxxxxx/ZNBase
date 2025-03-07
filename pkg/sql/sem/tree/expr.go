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
	"context"
	"fmt"
	"strconv"

	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/mon"
)

// Expr represents an expression.
type Expr interface {
	fmt.Stringer
	NodeFormatter
	// Walk recursively walks all children using WalkExpr. If any children are changed, it returns a
	// copy of this node updated to point to the new children. Otherwise the receiver is returned.
	// For childless (leaf) Exprs, its implementation is empty.
	Walk(Visitor) Expr
	// TypeCheck transforms the Expr into a well-typed TypedExpr, which further permits
	// evaluation and type introspection, or an error if the expression cannot be well-typed.
	// When type checking is complete, if no error was reported, the expression and all
	// sub-expressions will be guaranteed to be well-typed, meaning that the method effectively
	// maps the Expr tree into a TypedExpr tree.
	//
	// The ctx parameter defines the context in which to perform type checking.
	// The desired parameter hints the desired type that the method's caller wants from
	// the resulting TypedExpr. It is not valid to call TypeCheck with a nil desired
	// type. Instead, call it with wildcard type types.Any if no specific type is
	// desired. This restriction is also true of most methods and functions related
	// to type checking.
	TypeCheck(ctx *SemaContext, desired types.T, useOrigin bool) (TypedExpr, error)
}

// TypedExpr represents a well-typed expression.
type TypedExpr interface {
	Expr
	// Eval evaluates an SQL expression. Expression evaluation is a
	// mostly straightforward walk over the parse tree. The only
	// significant complexity is the handling of types and implicit
	// conversions. See binOps and cmpOps for more details. Note that
	// expression evaluation returns an error if certain node types are
	// encountered: Placeholder, VarName (and related UnqualifiedStar,
	// UnresolvedName and AllColumnsSelector) or Subquery. These nodes
	// should be replaced prior to expression evaluation by an
	// appropriate WalkExpr. For example, Placeholder should be replace
	// by the argument passed from the client.
	Eval(*EvalContext) (Datum, error)
	// ResolvedType provides the type of the TypedExpr, which is the type of Datum
	// that the TypedExpr will return when evaluated.
	ResolvedType() types.T
}

// VariableExpr is an Expr that may change per row. It is used to
// signal the evaluation/simplification machinery that the underlying
// Expr is not constant.
type VariableExpr interface {
	Expr
	Variable()
}

var _ VariableExpr = &IndexedVar{}
var _ VariableExpr = &Subquery{}
var _ VariableExpr = UnqualifiedStar{}
var _ VariableExpr = &UnresolvedName{}
var _ VariableExpr = &AllColumnsSelector{}
var _ VariableExpr = &ColumnItem{}

// operatorExpr is used to identify expression types that involve operators;
// used by exprStrWithParen.
type operatorExpr interface {
	Expr
	operatorExpr()
}

var _ operatorExpr = &AndExpr{}
var _ operatorExpr = &OrExpr{}
var _ operatorExpr = &NotExpr{}
var _ operatorExpr = &BinaryExpr{}
var _ operatorExpr = &UnaryExpr{}
var _ operatorExpr = &ComparisonExpr{}
var _ operatorExpr = &RangeCond{}
var _ operatorExpr = &IsOfTypeExpr{}

// Operator is used to identify Operators; used in sql.y.
type Operator interface {
	operator()
}

var _ Operator = UnaryOperator(0)
var _ Operator = BinaryOperator(0)
var _ Operator = ComparisonOperator(0)

// exprFmtWithParen is a variant of Format() which adds a set of outer parens
// if the expression involves an operator. It is used internally when the
// expression is part of another expression and we know it is preceded or
// followed by an operator.
func exprFmtWithParen(ctx *FmtCtx, e Expr) {
	if _, ok := e.(operatorExpr); ok {
		ctx.WriteByte('(')
		ctx.FormatNode(e)
		ctx.WriteByte(')')
	} else {
		ctx.FormatNode(e)
	}
}

// typeAnnotation is an embeddable struct to provide a TypedExpr with a dynamic
// type annotation.
type typeAnnotation struct {
	Typ types.T
}

func (ta typeAnnotation) ResolvedType() types.T {
	ta.assertTyped()
	return ta.Typ
}

func (ta typeAnnotation) assertTyped() {
	if ta.Typ == nil {
		panic("ReturnType called on TypedExpr with empty typeAnnotation. " +
			"Was the underlying Expr type-checked before asserting a type of TypedExpr?")
	}
}

// AndExpr represents an AND expression.
type AndExpr struct {
	Left, Right Expr

	typeAnnotation
}

func (*AndExpr) operatorExpr() {}

// Return andexpr copy.
//func (node AndExpr) ResloveRownumAnd() AndExpr {
//	var result AndExpr
//	result = node
//	return result
//}

// Solve rownum problems when rownum in where conditions.
//func (node *AndExpr) ResolveWhereAnd(isChange int) {
//	switch expr := node.Left.(type) {
//	case *AndExpr:
//		expr.ResolveWhereAnd(isChange)
//	case *OrExpr:
//		expr.ResolveWhereOr(isChange)
//	case *ComparisonExpr:
//		if left, ok := expr.Left.(*UnresolvedName); ok {
//			if left.Parts[0] == "rownum" && isChange == 1 {
//				left.Parts[0] = "rownum1"
//			}
//			if left.Parts[0] == "row_num" && isChange == 2 {
//				left.Parts[0] = "ROWNUM1"
//			}
//		}
//		if right, ok := expr.Right.(*UnresolvedName); ok {
//			if right.Parts[0] == "rownum" && isChange == 1 {
//				right.Parts[0] = "rownum1"
//			}
//			if right.Parts[0] == "row_num" && isChange == 2 {
//				right.Parts[0] = "ROWNUM1"
//			}
//		}
//	default:
//	}
//}

func binExprFmtWithParen(ctx *FmtCtx, e1 Expr, op string, e2 Expr, pad bool) {
	exprFmtWithParen(ctx, e1)
	if pad {
		ctx.WriteByte(' ')
	}
	ctx.WriteString(op)
	if pad {
		ctx.WriteByte(' ')
	}
	exprFmtWithParen(ctx, e2)
}

func binExprFmtWithParenAndSubOp(ctx *FmtCtx, e1 Expr, subOp, op string, e2 Expr) {
	exprFmtWithParen(ctx, e1)
	ctx.WriteByte(' ')
	if subOp != "" {
		ctx.WriteString(subOp)
		ctx.WriteByte(' ')
	}
	ctx.WriteString(op)
	ctx.WriteByte(' ')
	exprFmtWithParen(ctx, e2)
}

// Format implements the NodeFormatter interface.
func (node *AndExpr) Format(ctx *FmtCtx) {
	binExprFmtWithParen(ctx, node.Left, "AND", node.Right, true)
}

// NewTypedAndExpr returns a new AndExpr that is verified to be well-typed.
func NewTypedAndExpr(left, right TypedExpr) *AndExpr {
	node := &AndExpr{Left: left, Right: right}
	node.Typ = types.Bool
	return node
}

// TypedLeft returns the AndExpr's left expression as a TypedExpr.
func (node *AndExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the AndExpr's right expression as a TypedExpr.
func (node *AndExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// OrExpr represents an OR expression.
type OrExpr struct {
	Left, Right Expr

	typeAnnotation
}

func (*OrExpr) operatorExpr() {}

// Solve rownum problems when rownum in where conditions.
//func (node *OrExpr) ResolveWhereOr(isChange int) {
//	switch expr := node.Left.(type) {
//	case *AndExpr:
//		expr.ResolveWhereAnd(isChange)
//	case *OrExpr:
//		expr.ResolveWhereOr(isChange)
//	case *ComparisonExpr:
//		if left, ok := expr.Left.(*UnresolvedName); ok {
//			if left.Parts[0] == "rownum" && isChange == 1 {
//				left.Parts[0] = "rownum1"
//			}
//			if left.Parts[0] == "row_num" && isChange == 2 {
//				left.Parts[0] = "ROWNUM1"
//			}
//		}
//		if right, ok := expr.Right.(*UnresolvedName); ok {
//			if right.Parts[0] == "rownum" && isChange == 1 {
//				right.Parts[0] = "rownum1"
//			}
//			if right.Parts[0] == "row_num" && isChange == 2 {
//				right.Parts[0] = "ROWNUM1"
//			}
//		}
//	default:
//	}
//}

// Format implements the NodeFormatter interface.
func (node *OrExpr) Format(ctx *FmtCtx) {
	binExprFmtWithParen(ctx, node.Left, "OR", node.Right, true)
}

// NewTypedOrExpr returns a new OrExpr that is verified to be well-typed.
func NewTypedOrExpr(left, right TypedExpr) *OrExpr {
	node := &OrExpr{Left: left, Right: right}
	node.Typ = types.Bool
	return node
}

// TypedLeft returns the OrExpr's left expression as a TypedExpr.
func (node *OrExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the OrExpr's right expression as a TypedExpr.
func (node *OrExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// NotExpr represents a NOT expression.
type NotExpr struct {
	Expr Expr

	typeAnnotation
}

func (*NotExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *NotExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("NOT ")
	exprFmtWithParen(ctx, node.Expr)
}

// NewTypedNotExpr returns a new NotExpr that is verified to be well-typed.
func NewTypedNotExpr(expr TypedExpr) *NotExpr {
	node := &NotExpr{Expr: expr}
	node.Typ = types.Bool
	return node
}

// TypedInnerExpr returns the NotExpr's inner expression as a TypedExpr.
func (node *NotExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *ParenExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteByte(')')
}

// TypedInnerExpr returns the ParenExpr's inner expression as a TypedExpr.
func (node *ParenExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// StripParens strips any parentheses surrounding an expression and
// returns the inner expression. For instance:
//   1   -> 1
//  (1)  -> 1
// ((1)) -> 1
func StripParens(expr Expr) Expr {
	if p, ok := expr.(*ParenExpr); ok {
		return StripParens(p.Expr)
	}
	return expr
}

// ComparisonOperator represents a binary operator.
type ComparisonOperator int

func (ComparisonOperator) operator() {}

// ComparisonExpr.Operator
const (
	EQ ComparisonOperator = iota
	LT
	GT
	LE
	GE
	NE
	In
	NotIn
	Like
	NotLike
	ILike
	NotILike
	SimilarTo
	NotSimilarTo
	RegMatch
	NotRegMatch
	RegIMatch
	NotRegIMatch
	IsDistinctFrom
	IsNotDistinctFrom
	Contains
	ContainedBy
	JSONExists
	JSONSomeExists
	JSONAllExists

	// The following operators will always be used with an associated SubOperator.
	// If Go had algebraic data types they would be defined in a self-contained
	// manner like:
	//
	// Any(ComparisonOperator)
	// Some(ComparisonOperator)
	// ...
	//
	// where the internal ComparisonOperator qualifies the behavior of the primary
	// operator. Instead, a secondary ComparisonOperator is optionally included in
	// ComparisonExpr for the cases where these operators are the primary op.
	//
	// ComparisonOperator.hasSubOperator returns true for ops in this group.
	Any
	Some
	All

	EQNU

	NumComparisonOperators
)

var _ = NumComparisonOperators

var comparisonOpName = [...]string{
	EQ:                "=",
	LT:                "<",
	GT:                ">",
	LE:                "<=",
	GE:                ">=",
	NE:                "!=",
	In:                "IN",
	NotIn:             "NOT IN",
	Like:              "LIKE",
	NotLike:           "NOT LIKE",
	ILike:             "ILIKE",
	NotILike:          "NOT ILIKE",
	SimilarTo:         "SIMILAR TO",
	NotSimilarTo:      "NOT SIMILAR TO",
	RegMatch:          "~",
	NotRegMatch:       "!~",
	RegIMatch:         "~*",
	NotRegIMatch:      "!~*",
	IsDistinctFrom:    "IS DISTINCT FROM",
	IsNotDistinctFrom: "IS NOT DISTINCT FROM",
	Contains:          "@>",
	ContainedBy:       "<@",
	JSONExists:        "?",
	JSONSomeExists:    "?|",
	JSONAllExists:     "?&",
	Any:               "ANY",
	Some:              "SOME",
	All:               "ALL",
	EQNU:              "<=>",
}

func (i ComparisonOperator) String() string {
	if i < 0 || i > ComparisonOperator(len(comparisonOpName)-1) {
		return fmt.Sprintf("ComparisonOp(%d)", i)
	}
	return comparisonOpName[i]
}

// Inverse returns the inverse of this comparison operator if it exists. The
// second return value is true if it exists, and false otherwise.
func (i ComparisonOperator) Inverse() (ComparisonOperator, bool) {
	inverse, ok := cmpOpsInverse[i]
	return inverse, ok
}

// hasSubOperator returns if the ComparisonOperator is used with a sub-operator.
func (i ComparisonOperator) hasSubOperator() bool {
	switch i {
	case Any:
	case Some:
	case All:
	default:
		return false
	}
	return true
}

// ComparisonExpr represents a two-value comparison expression.
type ComparisonExpr struct {
	Operator    ComparisonOperator
	SubOperator ComparisonOperator // used for array operators (when Operator is Any, Some, or All)
	Left, Right Expr

	typeAnnotation
	fn      *CmpOp
	Nocycle bool

	IsCanaryCol bool //IsCanaryCol stores a flag (true if there's a canary column, vice versa)
}

func (*ComparisonExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *ComparisonExpr) Format(ctx *FmtCtx) {
	opStr := node.Operator.String()
	if node.Operator == IsDistinctFrom && (node.Right == DNull || node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS NOT"
	} else if node.Operator == IsNotDistinctFrom && (node.Right == DNull || node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS"
	}
	if node.Operator.hasSubOperator() {
		binExprFmtWithParenAndSubOp(ctx, node.Left, node.SubOperator.String(), opStr, node.Right)
	} else {
		binExprFmtWithParen(ctx, node.Left, opStr, node.Right, true)
	}
}

// NewTypedComparisonExpr returns a new ComparisonExpr that is verified to be well-typed.
func NewTypedComparisonExpr(op ComparisonOperator, left, right TypedExpr) *ComparisonExpr {
	node := &ComparisonExpr{Operator: op, Left: left, Right: right}
	node.Typ = types.Bool
	node.memoizeFn()
	return node
}

// NewTypedComparisonExprWithSubOp returns a new ComparisonExpr that is verified to be well-typed.
func NewTypedComparisonExprWithSubOp(
	op, subOp ComparisonOperator, left, right TypedExpr,
) *ComparisonExpr {
	node := &ComparisonExpr{Operator: op, SubOperator: subOp, Left: left, Right: right}
	node.Typ = types.Bool
	node.memoizeFn()
	return node
}

// NewTypedIndirectionExpr returns a new IndirectionExpr that is verified to be well-typed.
func NewTypedIndirectionExpr(expr, index TypedExpr, typ types.T) *IndirectionExpr {
	node := &IndirectionExpr{
		Expr:        expr,
		Indirection: ArraySubscripts{&ArraySubscript{Begin: index}},
	}
	node.Typ = typ
	//node.Typ = types.UnwrapType(expr.(TypedExpr).ResolvedType()).(types.TArray).Typ
	return node
}

// NewTypedCollateExpr returns a new CollateExpr that is verified to be well-typed.
func NewTypedCollateExpr(expr TypedExpr, locale string) *CollateExpr {
	node := &CollateExpr{
		Expr:   expr,
		Locale: locale,
	}
	node.Typ = types.TCollatedString{Locale: locale}
	return node
}

// NewTypedArrayFlattenExpr returns a new ArrayFlattenExpr that is verified to be well-typed.
func NewTypedArrayFlattenExpr(input Expr) *ArrayFlatten {
	inputTyp := input.(TypedExpr).ResolvedType()
	node := &ArrayFlatten{
		Subquery: input,
	}
	node.Typ = types.TArray{Typ: inputTyp}
	return node
}

// NewTypedIfErrExpr returns a new IfErrExpr that is verified to be well-typed.
func NewTypedIfErrExpr(cond, orElse, errCode TypedExpr) *IfErrExpr {
	node := &IfErrExpr{
		Cond:    cond,
		Else:    orElse,
		ErrCode: errCode,
	}
	if orElse == nil {
		node.Typ = types.Bool
	} else {
		node.Typ = cond.ResolvedType()
	}
	return node
}

// NewTypedAddtionalExpr will consturct  addtionalExpr with int type
func NewTypedAddtionalExpr(op int, signal *Signal) *AdditionalExpr {
	expr := &AdditionalExpr{
		OperationType: op,
		Sig:           signal,
	}
	expr.Typ = types.Int
	return expr
}

func (node *ComparisonExpr) memoizeFn() {
	fOp, fLeft, fRight, _, _ := foldComparisonExpr(node.Operator, node.Left, node.Right)
	leftRet, rightRet := fLeft.(TypedExpr).ResolvedType(), fRight.(TypedExpr).ResolvedType()
	switch node.Operator {
	case Any, Some, All:
		// Array operators memoize the SubOperator's CmpOp.
		fOp, _, _, _, _ = foldComparisonExpr(node.SubOperator, nil, nil)
		// The right operand is either an array or a tuple/subquery.
		switch t := types.UnwrapType(rightRet).(type) {
		case types.TArray:
			// For example:
			//   x = ANY(ARRAY[1,2])
			rightRet = t.Typ
		case types.TTuple:
			// For example:
			//   x = ANY(SELECT y FROM t)
			//   x = ANY(1,2)
			if len(t.Types) > 0 {
				rightRet = t.Types[0]
			} else {
				rightRet = leftRet
			}
		}
	}

	fn, ok := CmpOps[fOp].lookupImpl(leftRet, rightRet)
	if !ok {
		panic(fmt.Sprintf("lookup for ComparisonExpr %s's CmpOp failed",
			AsStringWithFlags(node, FmtShowTypes)))
	}
	node.fn = fn
}

// TypedLeft returns the ComparisonExpr's left expression as a TypedExpr.
func (node *ComparisonExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the ComparisonExpr's right expression as a TypedExpr.
func (node *ComparisonExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// RangeCond represents a BETWEEN [SYMMETRIC] or a NOT BETWEEN [SYMMETRIC]
// expression.
type RangeCond struct {
	Not       bool
	Symmetric bool
	Left      Expr
	From, To  Expr

	typeAnnotation
}

func (*RangeCond) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *RangeCond) Format(ctx *FmtCtx) {
	notStr := " BETWEEN "
	if node.Not {
		notStr = " NOT BETWEEN "
	}
	exprFmtWithParen(ctx, node.Left)
	ctx.WriteString(notStr)
	if node.Symmetric {
		ctx.WriteString("SYMMETRIC ")
	}
	binExprFmtWithParen(ctx, node.From, "AND", node.To, true)
}

// TypedLeft returns the RangeCond's left expression as a TypedExpr.
func (node *RangeCond) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedFrom returns the RangeCond's from expression as a TypedExpr.
func (node *RangeCond) TypedFrom() TypedExpr {
	return node.From.(TypedExpr)
}

// TypedTo returns the RangeCond's to expression as a TypedExpr.
func (node *RangeCond) TypedTo() TypedExpr {
	return node.To.(TypedExpr)
}

// IsOfTypeExpr represents an IS {,NOT} OF (type_list) expression.
type IsOfTypeExpr struct {
	Not   bool
	Expr  Expr
	Types []coltypes.T

	typeAnnotation
}

func (*IsOfTypeExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *IsOfTypeExpr) Format(ctx *FmtCtx) {
	exprFmtWithParen(ctx, node.Expr)
	ctx.WriteString(" IS")
	if node.Not {
		ctx.WriteString(" NOT")
	}
	ctx.WriteString(" OF (")
	for i, t := range node.Types {
		if i > 0 {
			ctx.WriteString(", ")
		}
		if ctx.flags.HasFlags(FmtVisableType) {
			ctx.WriteString(t.ColumnType())
		} else {
			t.Format(&ctx.Buffer, ctx.flags.EncodeFlags())
		}
	}
	ctx.WriteByte(')')
}

// IfErrExpr represents an IFERROR expression.
type IfErrExpr struct {
	Cond    Expr
	Else    Expr
	ErrCode Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *IfErrExpr) Format(ctx *FmtCtx) {
	if node.Else != nil {
		ctx.WriteString("IFERROR(")
	} else {
		ctx.WriteString("ISERROR(")
	}
	ctx.FormatNode(node.Cond)
	if node.Else != nil {
		ctx.WriteString(", ")
		ctx.FormatNode(node.Else)
	}
	if node.ErrCode != nil {
		ctx.WriteString(", ")
		ctx.FormatNode(node.ErrCode)
	}
	ctx.WriteByte(')')
}

// IfExpr represents an IF expression.
type IfExpr struct {
	Cond Expr
	True Expr
	Else Expr

	typeAnnotation
}

// TypedTrueExpr returns the IfExpr's True expression as a TypedExpr.
func (node *IfExpr) TypedTrueExpr() TypedExpr {
	return node.True.(TypedExpr)
}

// TypedCondExpr returns the IfExpr's Cond expression as a TypedExpr.
func (node *IfExpr) TypedCondExpr() TypedExpr {
	return node.Cond.(TypedExpr)
}

// TypedElseExpr returns the IfExpr's Else expression as a TypedExpr.
func (node *IfExpr) TypedElseExpr() TypedExpr {
	return node.Else.(TypedExpr)
}

// Format implements the NodeFormatter interface.
func (node *IfExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("IF(")
	ctx.FormatNode(node.Cond)
	ctx.WriteString(", ")
	ctx.FormatNode(node.True)
	ctx.WriteString(", ")
	ctx.FormatNode(node.Else)
	ctx.WriteByte(')')
}

// NullIfExpr represents a NULLIF expression.
type NullIfExpr struct {
	Expr1 Expr
	Expr2 Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *NullIfExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("NULLIF(")
	ctx.FormatNode(node.Expr1)
	ctx.WriteString(", ")
	ctx.FormatNode(node.Expr2)
	ctx.WriteByte(')')
}

// NvlExpr represents a NVL or IFNULL expression.
type NvlExpr struct {
	Name  string
	Exprs Exprs

	typeAnnotation
}

// NewTypedNvlExpr returns a NvlExpr that is well-typed.
func NewTypedNvlExpr(typedExprs TypedExprs, typ types.T) *NvlExpr {
	c := &NvlExpr{
		Name:  "NVL",
		Exprs: make(Exprs, len(typedExprs)),
	}
	for i := range typedExprs {
		c.Exprs[i] = typedExprs[i]
	}
	c.Typ = typ
	return c
}

// CoalesceExpr represents a COALESCE expression.
type CoalesceExpr struct {
	Name  string
	Exprs Exprs
	typeAnnotation
}

// NewTypedCoalesceExpr returns a CoalesceExpr that is well-typed.
func NewTypedCoalesceExpr(typedExprs TypedExprs, typ types.T) *CoalesceExpr {
	c := &CoalesceExpr{
		Name:  "COALESCE",
		Exprs: make(Exprs, len(typedExprs)),
	}
	for i := range typedExprs {
		c.Exprs[i] = typedExprs[i]
	}
	c.Typ = typ
	return c
}

// NewTypedArray returns an Array that is well-typed.
func NewTypedArray(typedExprs TypedExprs, typ types.T) *Array {
	c := &Array{
		Exprs: make(Exprs, len(typedExprs)),
	}
	for i := range typedExprs {
		c.Exprs[i] = typedExprs[i]
	}
	c.Typ = typ
	return c
}

// TypedExprAt returns the expression at the specified index as a TypedExpr.
func (node *CoalesceExpr) TypedExprAt(idx int) TypedExpr {
	return node.Exprs[idx].(TypedExpr)
}

// Format implements the NodeFormatter interface.
func (node *CoalesceExpr) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Name)
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
}

// CharExpr represents a Char expression.
type CharExpr struct {
	Name  string
	Exprs Exprs
	typeAnnotation
}

// NewTypedCharExpr returns a CharExpr that is well-typed.
func NewTypedCharExpr(typedExprs TypedExprs, typ types.T) *CharExpr {
	c := &CharExpr{
		Name:  "CHAR",
		Exprs: make(Exprs, len(typedExprs)),
	}
	for i := range typedExprs {
		c.Exprs[i] = typedExprs[i]
	}
	c.Typ = typ
	return c
}

// TypedExprAt returns the expression at the specified index as a TypedExpr.
func (node *CharExpr) TypedExprAt(idx int) TypedExpr {
	return node.Exprs[idx].(TypedExpr)
}

// Format implements the NodeFormatter interface.
func (node *CharExpr) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Name)
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
}

// TypedExprAt returns the expression at the specified index as a TypedExpr.
func (node *NvlExpr) TypedExprAt(idx int) TypedExpr {
	return node.Exprs[idx].(TypedExpr)
}

// Format implements the NodeFormatter interface.
func (node *NvlExpr) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Name)
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
}

// DefaultVal represents the DEFAULT expression.
type DefaultVal struct{}

// Format implements the NodeFormatter interface.
func (node DefaultVal) Format(ctx *FmtCtx) {
	ctx.WriteString("DEFAULT")
}

// ResolvedType implements the TypedExpr interface.
func (DefaultVal) ResolvedType() types.T { return nil }

// PartitionMaxVal represents the MAXVALUE expression.
type PartitionMaxVal struct{}

// Format implements the NodeFormatter interface.
func (node PartitionMaxVal) Format(ctx *FmtCtx) {
	ctx.WriteString("MAXVALUE")
}

// PartitionMinVal represents the MINVALUE expression.
type PartitionMinVal struct{}

// Format implements the NodeFormatter interface.
func (node PartitionMinVal) Format(ctx *FmtCtx) {
	ctx.WriteString("MINVALUE")
}

// Placeholder represents a named placeholder.
type Placeholder struct {
	Idx types.PlaceholderIdx

	typeAnnotation
}

// NewPlaceholder allocates a Placeholder.
func NewPlaceholder(name string) (*Placeholder, error) {
	uval, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return nil, err
	}
	// The string is the number that follows $ which is a 1-based index ($1, $2,
	// etc), while PlaceholderIdx is 0-based.
	if uval == 0 || uval > types.MaxPlaceholderIdx+1 {
		return nil, pgerror.NewErrorf(
			pgcode.NumericValueOutOfRange,
			"placeholder index must be between 1 and %d", types.MaxPlaceholderIdx+1,
		)
	}
	return &Placeholder{Idx: types.PlaceholderIdx(uval - 1)}, nil
}

// Format implements the NodeFormatter interface.
func (node *Placeholder) Format(ctx *FmtCtx) {
	if ctx.placeholderFormat != nil {
		ctx.placeholderFormat(ctx, node)
		return
	}
	ctx.Printf("$%d", node.Idx+1)
}

// ResolvedType implements the TypedExpr interface.
func (node *Placeholder) ResolvedType() types.T {
	if node.Typ == nil {
		node.Typ = &types.TPlaceholder{Idx: node.Idx}
	}
	return node.Typ
}

// Tuple represents a parenthesized list of expressions.
type Tuple struct {
	Exprs  Exprs
	Labels []string

	// Row indicates whether `ROW` was used in the input syntax. This is
	// used solely to generate column names automatically, see
	// col_name.go.
	Row bool

	typ types.TTuple
}

// NewTypedTuple returns a new Tuple that is verified to be well-typed.
func NewTypedTuple(typ types.TTuple, typedExprs Exprs) *Tuple {
	return &Tuple{
		Exprs:  typedExprs,
		Labels: typ.Labels,
		typ:    typ,
	}
}

// Format implements the NodeFormatter interface.
func (node *Tuple) Format(ctx *FmtCtx) {
	// If there are labels, extra parentheses are required surrounding the
	// expression.
	if len(node.Labels) > 0 {
		ctx.WriteByte('(')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Exprs)
	if len(node.Exprs) == 1 {
		// Ensure the pretty-printed 1-value tuple is not ambiguous with
		// the equivalent value enclosed in grouping parentheses.
		ctx.WriteByte(',')
	}
	ctx.WriteByte(')')
	if len(node.Labels) > 0 {
		ctx.WriteString(" AS ")
		comma := ""
		for i := range node.Labels {
			ctx.WriteString(comma)
			ctx.FormatNode((*Name)(&node.Labels[i]))
			comma = ", "
		}
		ctx.WriteByte(')')
	}
}

// ResolvedType implements the TypedExpr interface.
func (node *Tuple) ResolvedType() types.T {
	return node.typ
}

// Truncate returns a new Tuple that contains only a prefix of the original
// expressions. E.g.
//   Tuple:       (1, 2, 3)
//   Truncate(2): (1, 2)
func (node *Tuple) Truncate(prefix int) *Tuple {
	return &Tuple{
		Exprs: append(Exprs(nil), node.Exprs[:prefix]...),
		Row:   node.Row,
		typ:   types.TTuple{Types: append([]types.T(nil), node.typ.Types[:prefix]...)},
	}
}

// Project returns a new Tuple that contains a subset of the original
// expressions. E.g.
//  Tuple:           (1, 2, 3)
//  Project({0, 2}): (1, 3)
func (node *Tuple) Project(set util.FastIntSet) *Tuple {
	t := &Tuple{
		Exprs: make(Exprs, 0, set.Len()),
		Row:   node.Row,
		typ:   types.TTuple{Types: make([]types.T, 0, set.Len())},
	}
	for i, ok := set.Next(0); ok; i, ok = set.Next(i + 1) {
		t.Exprs = append(t.Exprs, node.Exprs[i])
		t.typ.Types = append(t.typ.Types, node.typ.Types[i])
	}
	return t
}

// Array represents an array constructor.
type Array struct {
	Exprs Exprs

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *Array) Format(ctx *FmtCtx) {
	ctx.WriteString("ARRAY[")
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(']')
	// If the array has a type, add an annotation. Don't add it if the type is
	// UNKNOWN[], since that's not a valid annotation.
	if ctx.HasFlags(FmtParsable) {
		if colTyp, err := coltypes.DatumTypeToColumnType(node.Typ); err == nil {
			ctx.WriteString(":::")
			colTyp.Format(&ctx.Buffer, ctx.flags.EncodeFlags())
		}
	}
}

// ArrayFlatten represents a subquery array constructor.
type ArrayFlatten struct {
	Subquery Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *ArrayFlatten) Format(ctx *FmtCtx) {
	ctx.WriteString("ARRAY ")
	exprFmtWithParen(ctx, node.Subquery)
	if ctx.HasFlags(FmtParsable) {
		if t, ok := node.Subquery.(*DTuple); ok {
			if len(t.D) == 0 {
				if colTyp, err := coltypes.DatumTypeToColumnType(node.Typ); err == nil {
					ctx.WriteString(":::")
					if ctx.flags.HasFlags(FmtVisableType) {
						ctx.WriteString(colTyp.ColumnType())
					} else {
						colTyp.Format(&ctx.Buffer, ctx.flags.EncodeFlags())
					}
				}
			}
		}
	}
}

// Exprs represents a list of value expressions. It's not a valid expression
// because it's not parenthesized.
type Exprs []Expr

// Format implements the NodeFormatter interface.
func (node *Exprs) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(n)
	}
}

// TypedExprs represents a list of well-typed value expressions. It's not a valid expression
// because it's not parenthesized.
type TypedExprs []TypedExpr

func (node *TypedExprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range *node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// Subquery represents a subquery.
type Subquery struct {
	Select SelectStatement
	Exists bool

	// Idx is a query-unique index for the subquery.
	// Subqueries are 1-indexed to ensure that the default
	// value 0 can be used to detect uninitialized subqueries.
	Idx int

	typeAnnotation
}

// SetType forces the type annotation on the Subquery node.
func (node *Subquery) SetType(t types.T) {
	node.Typ = t
}

// Variable implements the VariableExpr interface.
func (*Subquery) Variable() {}

// Format implements the NodeFormatter interface.
func (node *Subquery) Format(ctx *FmtCtx) {
	if ctx.HasFlags(FmtSymbolicSubqueries) {
		ctx.Printf("@S%d", node.Idx)
	} else {
		// Ensure that type printing is disabled during the recursion, as
		// the type annotations are not available in subqueries.
		ctx.WithFlags(ctx.flags & ^FmtShowTypes, func() {
			if node.Exists {
				ctx.WriteString("EXISTS ")
			}
			if node.Select == nil {
				// If the subquery is generated by the optimizer, we
				// don't have an actual statement.
				ctx.WriteString("<unknown>")
			} else {
				ctx.FormatNode(node.Select)
			}
		})
	}
}

// BinaryOperator represents a binary operator.
type BinaryOperator int

func (BinaryOperator) operator() {}

// BinaryExpr.Operator
const (
	Bitand BinaryOperator = iota
	Bitor
	Bitxor
	Xor
	Plus
	Minus
	Mult
	Div
	FloorDiv
	Mod
	Pow
	Concat
	LShift
	RShift
	JSONFetchVal
	JSONFetchText
	JSONFetchValPath
	JSONFetchTextPath

	NumBinaryOperators
)

var _ = NumBinaryOperators

var binaryOpName = [...]string{
	Bitand:            "&",
	Bitor:             "|",
	Bitxor:            "#",
	Xor:               "XOR",
	Plus:              "+",
	Minus:             "-",
	Mult:              "*",
	Div:               "/",
	FloorDiv:          "//",
	Mod:               "%",
	Pow:               "^",
	Concat:            "||",
	LShift:            "<<",
	RShift:            ">>",
	JSONFetchVal:      "->",
	JSONFetchText:     "->>",
	JSONFetchValPath:  "#>",
	JSONFetchTextPath: "#>>",
}

// binaryOpPrio follows the precedence order in the grammar. Used for pretty-printing.
var binaryOpPrio = [...]int{
	Pow:  1,
	Mult: 2, Div: 2, FloorDiv: 2, Mod: 2,
	Plus: 3, Minus: 3,
	LShift: 4, RShift: 4,
	Bitand: 5,
	Bitxor: 6,
	Bitor:  7,
	Concat: 8, JSONFetchVal: 8, JSONFetchText: 8, JSONFetchValPath: 8, JSONFetchTextPath: 8,
	Xor: 9,
}

// binaryOpFullyAssoc indicates whether an operator is fully associative.
// Reminder: an op R is fully associative if (a R b) R c == a R (b R c)
var binaryOpFullyAssoc = [...]bool{
	Pow:  false,
	Mult: true, Div: false, FloorDiv: false, Mod: false,
	Plus: true, Minus: false,
	LShift: false, RShift: false,
	Bitand: true,
	Bitxor: true,
	Bitor:  true,
	Xor:    true,
	Concat: true, JSONFetchVal: false, JSONFetchText: false, JSONFetchValPath: false, JSONFetchTextPath: false,
}

func (i BinaryOperator) isPadded() bool {
	return !(i == JSONFetchVal || i == JSONFetchText || i == JSONFetchValPath || i == JSONFetchTextPath)
}

func (i BinaryOperator) String() string {
	if i < 0 || i > BinaryOperator(len(binaryOpName)-1) {
		return fmt.Sprintf("BinaryOp(%d)", i)
	}
	return binaryOpName[i]
}

// BinaryExpr represents a binary value expression.
type BinaryExpr struct {
	Operator    BinaryOperator
	Left, Right Expr

	typeAnnotation
	fn *BinOp
}

//ReconstructBinary use for StartWith to contruct Binary expr
func (node BinaryExpr) ReconstructBinary(TableName string, levelBinary *BinaryExpr) BinaryExpr {
	var binary BinaryExpr
	binary.Operator = node.Operator
	if right, ok := node.Right.(*UnresolvedName); ok {
		switch right.Parts[0] {
		case "level":
			binary.Right = levelBinary
		default:
			rightExpr := ConstructCoalesceExpr(right, TableName)
			binary.Right = rightExpr
		}
	} else {
		binary.Right = node.Right
	}
	if left, ok := node.Left.(*BinaryExpr); ok {
		result := left.ReconstructBinary(TableName, levelBinary)
		binary.Left = &result
	} else if left, ok := node.Left.(*UnresolvedName); ok {
		switch left.Parts[0] {
		case "level":
			binary.Left = levelBinary
		default:
			leftExpr := ConstructCoalesceExpr(left, TableName)
			binary.Left = leftExpr
		}
	} else {
		binary.Left = node.Left
	}

	return binary
}

//ConstructCoalesceExpr use for StartWith to construct Coalesce() expr
func ConstructCoalesceExpr(name *UnresolvedName, TableName string) *CoalesceExpr {
	var result UnresolvedName
	switch name {
	case nil:
		return &CoalesceExpr{
			Name: "COALESCE",
			Exprs: []Expr{
				NewStrVal(""),
				NewStrVal(""),
			},
		}
	default:
		if TableName != "" {
			result = name.ConstructUnresolvedName(2, false, TableName)
		} else if name.NumParts != 2 {
			result = name.ConstructUnresolvedName(1, false, "")
		} else {
			result = *name
		}
		return &CoalesceExpr{
			Name: "COALESCE",
			Exprs: []Expr{
				&CastExpr{
					Expr: &result,
					Type: &coltypes.TString{
						Variant:     coltypes.TStringVariantSTRING,
						N:           0,
						VisibleType: coltypes.VisSTRING,
					},
					SyntaxMode: CastShort,
				},
				NewStrVal(""),
			},
		}
	}
}

// TypedLeft returns the BinaryExpr's left expression as a TypedExpr.
func (node *BinaryExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the BinaryExpr's right expression as a TypedExpr.
func (node *BinaryExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// ResolvedBinOp returns the resolved binary op overload; can only be called
// after Resolve (which happens during TypeCheck).
func (node *BinaryExpr) ResolvedBinOp() *BinOp {
	return node.fn
}

// NewTypedBinaryExpr returns a new BinaryExpr that is well-typed.
func NewTypedBinaryExpr(op BinaryOperator, left, right TypedExpr, typ types.T) *BinaryExpr {
	node := &BinaryExpr{Operator: op, Left: left, Right: right}
	node.Typ = typ
	node.memoizeFn()
	return node
}

func (*BinaryExpr) operatorExpr() {}

func (node *BinaryExpr) memoizeFn() {
	leftRet, rightRet := node.Left.(TypedExpr).ResolvedType(), node.Right.(TypedExpr).ResolvedType()
	fn, ok := BinOps[node.Operator].lookupImpl(leftRet, rightRet)
	if !ok {
		panic(fmt.Sprintf("lookup for BinaryExpr %s's BinOp failed",
			AsStringWithFlags(node, FmtShowTypes)))
	}
	node.fn = fn
}

// newBinExprIfValidOverload constructs a new BinaryExpr if and only
// if the pair of arguments have a valid implementation for the given
// BinaryOperator.
func newBinExprIfValidOverload(op BinaryOperator, left TypedExpr, right TypedExpr) *BinaryExpr {
	leftRet, rightRet := left.ResolvedType(), right.ResolvedType()
	fn, ok := BinOps[op].lookupImpl(leftRet, rightRet)
	if ok {
		expr := &BinaryExpr{
			Operator: op,
			Left:     left,
			Right:    right,
			fn:       fn,
		}
		expr.Typ = returnTypeToFixedType(fn.returnType())
		return expr
	}
	return nil
}

// Format implements the NodeFormatter interface.
func (node *BinaryExpr) Format(ctx *FmtCtx) {
	binExprFmtWithParen(ctx, node.Left, node.Operator.String(), node.Right, node.Operator.isPadded())
}

// UnaryOperator represents a unary operator.
type UnaryOperator int

func (UnaryOperator) operator() {}

// UnaryExpr.Operator
const (
	UnaryMinus UnaryOperator = iota
	UnaryComplement

	NumUnaryOperators
)

var _ = NumUnaryOperators

var unaryOpName = [...]string{
	UnaryMinus:      "-",
	UnaryComplement: "~",
}

func (i UnaryOperator) String() string {
	if i < 0 || i > UnaryOperator(len(unaryOpName)-1) {
		return fmt.Sprintf("UnaryOp(%d)", i)
	}
	return unaryOpName[i]
}

// UnaryExpr represents a unary value expression.
type UnaryExpr struct {
	Operator UnaryOperator
	Expr     Expr

	typeAnnotation
	fn *UnaryOp
}

func (*UnaryExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *UnaryExpr) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Operator.String())
	e := node.Expr
	_, isOp := e.(operatorExpr)
	_, isDatum := e.(Datum)
	_, isConstant := e.(Constant)
	if isOp || (node.Operator == UnaryMinus && (isDatum || isConstant)) {
		ctx.WriteByte('(')
		ctx.FormatNode(e)
		ctx.WriteByte(')')
	} else {
		ctx.FormatNode(e)
	}
}

// TypedInnerExpr returns the UnaryExpr's inner expression as a TypedExpr.
func (node *UnaryExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// NewTypedUnaryExpr returns a new UnaryExpr that is well-typed.
func NewTypedUnaryExpr(op UnaryOperator, expr TypedExpr, typ types.T) *UnaryExpr {
	node := &UnaryExpr{Operator: op, Expr: expr}
	node.Typ = typ
	innerType := expr.ResolvedType()
	for _, o := range UnaryOps[op] {
		o := o.(*UnaryOp)
		if innerType.Equivalent(o.Typ) && node.Typ.Equivalent(o.ReturnType) {
			node.fn = o
			return node
		}
	}
	panic(fmt.Sprintf("invalid TypedExpr with unary op %d: %s", op, expr))
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Func  ResolvableFunctionReference
	Type  funcType
	Exprs Exprs
	// Filter is used for filters on aggregates: SUM(k) FILTER (WHERE k > 0)
	Filter      Expr
	WindowDef   *WindowDef
	IsUDF       bool
	FromPrepare bool
	typeAnnotation
	fnProps *FunctionProperties
	fn      *Overload
}

// NewTypedFuncExpr returns a FuncExpr that is already well-typed and resolved.
func NewTypedFuncExpr(
	ref ResolvableFunctionReference,
	aggQualifier funcType,
	exprs TypedExprs,
	filter TypedExpr,
	windowDef *WindowDef,
	typ types.T,
	props *FunctionProperties,
	overload *Overload,
) *FuncExpr {
	f := &FuncExpr{
		Func:           ref,
		Type:           aggQualifier,
		Exprs:          make(Exprs, len(exprs)),
		Filter:         filter,
		WindowDef:      windowDef,
		typeAnnotation: typeAnnotation{Typ: typ},
		fn:             overload,
		fnProps:        props,
	}
	for i, e := range exprs {
		f.Exprs[i] = e
	}
	return f
}

// ResolveUDRFunction will resolve user define function and return function def.
func (node *FuncExpr) ResolveUDRFunction(
	ctx context.Context, searchPath sessiondata.SearchPath, r UDRFunctionResolver,
) (*FunctionDefinition, error) {
	// as function has been resolve with build-in process, so we can Get function name from node directly
	def, ok := node.Func.FunctionReference.(*FunctionDefinition)
	if !ok {
		return nil, pgerror.NewAssertionErrorf("unknown function name type: %+v (%T)",
			node.Func.FunctionReference, node.Func.FunctionReference)
	}
	return r.ResolveUDRFunction(ctx, def)
}

// ResolvedOverload returns the builtin definition; can only be called after
// Resolve (which happens during TypeCheck).
func (node *FuncExpr) ResolvedOverload() *Overload {
	return node.fn
}

// GetAggregateConstructor exposes the AggregateFunc field for use by
// the group node in package sql.
func (node *FuncExpr) GetAggregateConstructor() func(*EvalContext, Datums, *mon.BoundAccount) AggregateFunc {
	if node.fn == nil || node.fn.AggregateFunc == nil {
		return nil
	}
	return func(evalCtx *EvalContext, arguments Datums, account *mon.BoundAccount) AggregateFunc {
		types := typesOfExprs(node.Exprs)
		return node.fn.AggregateFunc(types, evalCtx, arguments, account)
	}
}

func typesOfExprs(exprs Exprs) []types.T {
	types := make([]types.T, len(exprs))
	for i, expr := range exprs {
		types[i] = expr.(TypedExpr).ResolvedType()
	}
	return types
}

// IsGeneratorApplication returns true iff the function applied is a generator (SRF).
func (node *FuncExpr) IsGeneratorApplication() bool {
	return node.fn != nil && node.fn.Generator != nil
}

// IsWindowFunctionApplication returns true iff the function is being applied as a window function.
func (node *FuncExpr) IsWindowFunctionApplication() bool {
	return node.WindowDef != nil
}

// IsImpure returns whether the function application is impure, meaning that it
// potentially returns a different value when called in the same statement with
// the same parameters.
func (node *FuncExpr) IsImpure() bool {
	return node.fnProps != nil && node.fnProps.Impure
}

// IsDistSQLBlacklist returns whether the function is not supported by DistSQL.
func (node *FuncExpr) IsDistSQLBlacklist() bool {
	return node.fnProps != nil && node.fnProps.DistsqlBlacklist
}

// CanHandleNulls 返回函数是否能处理NULL
func (node *FuncExpr) CanHandleNulls() bool {
	return node.fnProps != nil && node.fnProps.NullableArgs
}

type funcType int

// FuncExpr.Type
const (
	_ funcType = iota
	DistinctFuncType
	AllFuncType
	UDRFuncType
)

var funcTypeName = [...]string{
	DistinctFuncType: "DISTINCT",
	AllFuncType:      "ALL",
	UDRFuncType:      "UDR",
}

// Format implements the NodeFormatter interface.
func (node *FuncExpr) Format(ctx *FmtCtx) {
	var typ string
	if node.Type != 0 {
		typ = funcTypeName[node.Type] + " "
	}

	// We need to remove name anonymization for the function name in
	// particular. Do this by overriding the flags.
	ctx.WithFlags(ctx.flags&^FmtAnonymize, func() {
		ctx.FormatNode(&node.Func)
	})

	ctx.WriteByte('(')
	ctx.WriteString(typ)
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
	if ctx.HasFlags(FmtParsable) && node.Typ != nil {
		if node.fnProps.AmbiguousReturnType {
			if typ, err := coltypes.DatumTypeToColumnType(node.Typ); err == nil {
				// There's no type annotation available for tuples.
				// TODO(jordan,knz): clean this up. AmbiguousReturnType should be set only
				// when we should and can put an annotation here. #28579
				if _, ok := typ.(coltypes.TTuple); !ok {
					ctx.WriteString(":::")
					ctx.WriteString(typ.TypeName())
				}
			}
		}
	}
	if node.Filter != nil {
		ctx.WriteString(" FILTER (WHERE ")
		ctx.FormatNode(node.Filter)
		ctx.WriteString(")")
	}
	if window := node.WindowDef; window != nil {
		ctx.WriteString(" OVER ")
		if window.Name != "" {
			ctx.FormatNode(&window.Name)
		} else {
			ctx.FormatNode(window)
		}
	}
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr  Expr
	Whens []*When
	Else  Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *CaseExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("CASE ")
	if node.Expr != nil {
		ctx.FormatNode(node.Expr)
		ctx.WriteByte(' ')
	}
	for _, when := range node.Whens {
		ctx.FormatNode(when)
		ctx.WriteByte(' ')
	}
	if node.Else != nil {
		ctx.WriteString("ELSE ")
		ctx.FormatNode(node.Else)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("END")
}

// NewTypedCaseExpr returns a new CaseExpr that is verified to be well-typed.
func NewTypedCaseExpr(
	expr TypedExpr, whens []*When, elseStmt TypedExpr, typ types.T,
) (*CaseExpr, error) {
	node := &CaseExpr{Expr: expr, Whens: whens, Else: elseStmt}
	node.Typ = typ
	return node, nil
}

// When represents a WHEN sub-expression.
type When struct {
	Cond Expr
	Val  Expr
}

// Format implements the NodeFormatter interface.
func (node *When) Format(ctx *FmtCtx) {
	ctx.WriteString("WHEN ")
	ctx.FormatNode(node.Cond)
	ctx.WriteString(" THEN ")
	ctx.FormatNode(node.Val)
}

type castSyntaxMode int

// These constants separate the syntax X::Y from CAST(X AS Y).
const (
	CastExplicit castSyntaxMode = iota
	CastShort
	CastPrepend
	CastManual
)

// CastExpr represents a CAST(expr AS type) expression.
type CastExpr struct {
	Expr Expr
	Type coltypes.CastTargetType

	typeAnnotation
	SyntaxMode castSyntaxMode
}

// Format implements the NodeFormatter interface.
func (node *CastExpr) Format(ctx *FmtCtx) {
	buf := &ctx.Buffer
	switch node.SyntaxMode {
	case CastPrepend:
		// This is a special case for things like INTERVAL '1s'. These only work
		// with string constats; if the underlying expression was changed, we fall
		// back to the short syntax.
		if _, ok := node.Expr.(*StrVal); ok {
			node.Type.Format(buf, ctx.flags.EncodeFlags())
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Expr)
			break
		}
		fallthrough
	case CastShort:
		exprFmtWithParen(ctx, node.Expr)
		ctx.WriteString("::")
		node.Type.Format(buf, ctx.flags.EncodeFlags())
	case CastManual:
		// This is added for function ChangeToCastExpr to Identify if the expr is changed to castExpr manually
		ctx.WriteString("MANUAL")
		fallthrough
	default:
		ctx.WriteString("CAST(")
		ctx.FormatNode(node.Expr)
		ctx.WriteString(" AS ")
		t, isCollatedString := node.Type.(*coltypes.TCollatedString)
		typ := node.Type
		if isCollatedString {
			typ = coltypes.String
		}
		typ.Format(buf, ctx.flags.EncodeFlags())
		ctx.WriteByte(')')
		if isCollatedString {
			ctx.WriteString(" COLLATE ")
			lex.EncodeUnrestrictedSQLIdent(&ctx.Buffer, t.Locale, lex.EncNoFlags)
		}
	}
}

// NewTypedCastExpr returns a new CastExpr that is verified to be well-typed.
func NewTypedCastExpr(expr TypedExpr, colType coltypes.T) (*CastExpr, error) {
	node := &CastExpr{Expr: expr, Type: colType, SyntaxMode: CastShort}
	node.Typ = coltypes.CastTargetToDatumType(colType)
	return node, nil
}

func (node *CastExpr) castType() types.T {
	return coltypes.CastTargetToDatumType(node.Type)
}

type castInfo struct {
	fromT   types.T
	counter telemetry.Counter
}

var (
	bitArrayCastTypes = annotateCast(types.BitArray, []types.T{types.Unknown, types.BitArray, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString})
	boolCastTypes     = annotateCast(types.Bool, []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString, types.BitArray, types.Date, types.Time, types.Timestamp, types.TimestampTZ, types.Set})
	intCastTypes      = annotateCast(types.Int, []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Time, types.Interval, types.Oid, types.BitArray, types.Set})
	floatCastTypes = annotateCast(types.Float, []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Time, types.Interval, types.BitArray, types.Set})
	decimalCastTypes = annotateCast(types.Decimal, []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Time, types.Interval, types.BitArray, types.Set})
	stringCastTypes = annotateCast(types.String, []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString,
		types.BitArray,
		types.FamArray, types.FamTuple,
		types.Bytes, types.Timestamp, types.TimestampTZ, types.Interval, types.UUID, types.Date, types.Time, types.Oid, types.INet, types.JSON, types.Set})
	bytesCastTypes = annotateCast(types.Bytes, []types.T{types.Unknown, types.String, types.FamCollatedString, types.Bytes, types.UUID, types.Set})
	dateCastTypes  = annotateCast(types.Date, []types.T{types.Unknown, types.String, types.FamCollatedString, types.Date, types.Timestamp, types.TimestampTZ, types.Int, types.Float, types.Decimal, types.TTuple{}})
	timeCastTypes  = annotateCast(types.Time, []types.T{types.Unknown, types.String, types.FamCollatedString, types.Time,
		types.Timestamp, types.TimestampTZ, types.Interval, types.Int, types.Float, types.Decimal, types.TTuple{}})
	timestampCastTypes = annotateCast(types.Timestamp, []types.T{types.Unknown, types.String, types.FamCollatedString, types.Date, types.Time, types.Timestamp, types.TimestampTZ, types.Int, types.Float, types.Decimal})
	intervalCastTypes  = annotateCast(types.Interval, []types.T{types.Unknown, types.String, types.FamCollatedString, types.Int, types.Time, types.Interval, types.Float, types.Decimal})
	oidCastTypes       = annotateCast(types.Oid, []types.T{types.Unknown, types.String, types.FamCollatedString, types.Int, types.Oid})
	uuidCastTypes      = annotateCast(types.UUID, []types.T{types.Unknown, types.String, types.FamCollatedString, types.Bytes, types.UUID})
	inetCastTypes      = annotateCast(types.INet, []types.T{types.Unknown, types.String, types.FamCollatedString, types.INet})
	arrayCastTypes     = annotateCast(types.FamArray, []types.T{types.Unknown, types.String})
	jsonCastTypes      = annotateCast(types.JSON, []types.T{types.Unknown, types.String, types.JSON})
)

// validCastTypes returns a set of types that can be cast into the provided type.
func validCastTypes(t types.T) []castInfo {
	switch types.UnwrapType(t) {
	case types.BitArray:
		return bitArrayCastTypes
	case types.Bool:
		return boolCastTypes
	case types.Int, types.Int2, types.Int4:
		return intCastTypes
	case types.Float, types.Float4:
		return floatCastTypes
	case types.Decimal:
		return decimalCastTypes
	case types.String:
		return stringCastTypes
	case types.Bytes:
		return bytesCastTypes
	case types.Date:
		return dateCastTypes
	case types.Time:
		return timeCastTypes
	case types.Timestamp, types.TimestampTZ:
		return timestampCastTypes
	case types.Interval:
		return intervalCastTypes
	case types.JSON:
		return jsonCastTypes
	case types.UUID:
		return uuidCastTypes
	case types.INet:
		return inetCastTypes
	case types.Oid, types.RegClass, types.RegNamespace, types.RegProc, types.RegProcedure, types.RegType:
		return oidCastTypes
	default:
		// TODO(eisen): currently dead -- there is no syntax yet for casting
		// directly to collated string.
		if t.FamilyEqual(types.FamCollatedString) {
			return stringCastTypes
		} else if t.FamilyEqual(types.FamArray) {
			ret := make([]castInfo, len(arrayCastTypes))
			copy(ret, arrayCastTypes)
			return ret
		}
		return nil
	}
}

// ArraySubscripts represents a sequence of one or more array subscripts.
type ArraySubscripts []*ArraySubscript

// Format implements the NodeFormatter interface.
func (a *ArraySubscripts) Format(ctx *FmtCtx) {
	for _, s := range *a {
		ctx.FormatNode(s)
	}
}

// IndirectionExpr represents a subscript expression.
type IndirectionExpr struct {
	Expr        Expr
	Indirection ArraySubscripts

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *IndirectionExpr) Format(ctx *FmtCtx) {
	exprFmtWithParen(ctx, node.Expr)
	ctx.FormatNode(&node.Indirection)
}

type annotateSyntaxMode int

// These constants separate the syntax X:::Y from ANNOTATE_TYPE(X, Y)
const (
	AnnotateExplicit annotateSyntaxMode = iota
	AnnotateShort
)

// AnnotateTypeExpr represents a ANNOTATE_TYPE(expr, type) expression.
type AnnotateTypeExpr struct {
	Expr Expr
	Type coltypes.CastTargetType

	SyntaxMode annotateSyntaxMode
}

// Format implements the NodeFormatter interface.
func (node *AnnotateTypeExpr) Format(ctx *FmtCtx) {
	buf := &ctx.Buffer
	switch node.SyntaxMode {
	case AnnotateShort:
		exprFmtWithParen(ctx, node.Expr)
		ctx.WriteString(":::")
		node.Type.Format(buf, ctx.flags.EncodeFlags())

	default:
		ctx.WriteString("ANNOTATE_TYPE(")
		ctx.FormatNode(node.Expr)
		ctx.WriteString(", ")
		node.Type.Format(buf, ctx.flags.EncodeFlags())
		ctx.WriteByte(')')
	}
}

// TypedInnerExpr returns the AnnotateTypeExpr's inner expression as a TypedExpr.
func (node *AnnotateTypeExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

func (node *AnnotateTypeExpr) annotationType() types.T {
	return coltypes.CastTargetToDatumType(node.Type)
}

// CollateExpr represents an (expr COLLATE locale) expression.
type CollateExpr struct {
	Expr   Expr
	Locale string

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *CollateExpr) Format(ctx *FmtCtx) {
	exprFmtWithParen(ctx, node.Expr)
	ctx.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(&ctx.Buffer, node.Locale, lex.EncNoFlags)
}

// TupleStar represents (E).* expressions.
// It is meant to evaporate during star expansion.
type TupleStar struct {
	Expr Expr
}

// NormalizeVarName implements the VarName interface.
func (node *TupleStar) NormalizeVarName() (VarName, error) { return node, nil }

// Format implements the NodeFormatter interface.
func (node *TupleStar) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteString(").*")
}

// ColumnAccessExpr represents (E).x expressions. Specifically, it
// allows accessing the column(s) from a Set Retruning Function.
type ColumnAccessExpr struct {
	Expr    Expr
	ColName string

	// ColIndex indicates the index of the column in the tuple. This is
	// set during type checking based on the label in ColName.
	ColIndex int

	typeAnnotation
}

// ChangeToCastExpr change a typedExpr to a CastExpr with castType = desired
func ChangeToCastExpr(expr Expr, desired types.T, width int) CastExpr {
	var cexpr CastExpr
	cexpr.SyntaxMode = CastManual
	cexpr.Expr = expr
	cexpr.Type, _ = coltypes.DatumTypeToColumnType(desired)
	if desired == types.BitArray && width != 0 {
		cexpr.Type = &coltypes.TBitArray{Width: uint(width), Variable: true, VisibleType: coltypes.VisVARBIT}
	}
	cexpr.Typ = desired
	return cexpr
}

// NewTypedColumnAccessExpr creates a pre-typed ColumnAccessExpr.
func NewTypedColumnAccessExpr(expr TypedExpr, colName string, colIdx int) *ColumnAccessExpr {
	return &ColumnAccessExpr{
		Expr:           expr,
		ColName:        colName,
		ColIndex:       colIdx,
		typeAnnotation: typeAnnotation{Typ: expr.ResolvedType().(types.TTuple).Types[colIdx]},
	}
}

// Format implements the NodeFormatter interface.
func (node *ColumnAccessExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteString(").")
	ctx.WriteString(node.ColName)
}

func (node *AliasedTableExpr) String() string { return AsString(node) }
func (node *ParenTableExpr) String() string   { return AsString(node) }
func (node *JoinTableExpr) String() string    { return AsString(node) }
func (node *AndExpr) String() string          { return AsString(node) }
func (node *Array) String() string            { return AsString(node) }
func (node *BinaryExpr) String() string       { return AsString(node) }
func (node *CaseExpr) String() string         { return AsString(node) }
func (node *CastExpr) String() string         { return AsString(node) }
func (node *CoalesceExpr) String() string     { return AsString(node) }
func (node *CharExpr) String() string         { return AsString(node) }
func (node *NvlExpr) String() string          { return AsString(node) }
func (node *ColumnAccessExpr) String() string { return AsString(node) }
func (node *CollateExpr) String() string      { return AsString(node) }
func (node *ComparisonExpr) String() string   { return AsString(node) }
func (node *Datums) String() string           { return AsString(node) }
func (node *DBitArray) String() string        { return AsString(node) }
func (node *DBool) String() string            { return AsString(node) }
func (node *DBytes) String() string           { return AsString(node) }
func (node *DDate) String() string            { return AsString(node) }
func (node *DTime) String() string            { return AsString(node) }
func (node *DDecimal) String() string         { return AsString(node) }
func (node *DFloat) String() string           { return AsString(node) }
func (node *DInt) String() string             { return AsString(node) }
func (node *DInterval) String() string        { return AsString(node) }
func (node *DJSON) String() string            { return AsString(node) }
func (node *DUuid) String() string            { return AsString(node) }
func (node *DIPAddr) String() string          { return AsString(node) }
func (node *DString) String() string          { return AsString(node) }
func (node *DCollatedString) String() string  { return AsString(node) }
func (node *DTimestamp) String() string       { return AsString(node) }
func (node *DTimestampTZ) String() string     { return AsString(node) }
func (node *DTuple) String() string           { return AsString(node) }
func (node *DArray) String() string           { return AsString(node) }
func (node *DOid) String() string             { return AsString(node) }
func (node *DOidWrapper) String() string      { return AsString(node) }
func (node *Exprs) String() string            { return AsString(node) }
func (node *ArrayFlatten) String() string     { return AsString(node) }
func (node *FuncExpr) String() string         { return AsString(node) }
func (node *IfExpr) String() string           { return AsString(node) }
func (node *IfErrExpr) String() string        { return AsString(node) }
func (node *IndexedVar) String() string       { return AsString(node) }
func (node *IndirectionExpr) String() string  { return AsString(node) }
func (node *IsOfTypeExpr) String() string     { return AsString(node) }
func (node *Name) String() string             { return AsString(node) }
func (node *UnrestrictedName) String() string { return AsString(node) }
func (node *NotExpr) String() string          { return AsString(node) }
func (node *NullIfExpr) String() string       { return AsString(node) }
func (node *NumVal) String() string           { return AsString(node) }
func (node *OrExpr) String() string           { return AsString(node) }
func (node *ParenExpr) String() string        { return AsString(node) }
func (node *RangeCond) String() string        { return AsString(node) }
func (node *StrVal) String() string           { return AsString(node) }
func (node *Subquery) String() string         { return AsString(node) }
func (node *Tuple) String() string            { return AsString(node) }
func (node *TupleStar) String() string        { return AsString(node) }
func (node *AnnotateTypeExpr) String() string { return AsString(node) }
func (node *UnaryExpr) String() string        { return AsString(node) }
func (node DefaultVal) String() string        { return AsString(node) }
func (node PartitionMaxVal) String() string   { return AsString(node) }
func (node PartitionMinVal) String() string   { return AsString(node) }
func (node *Placeholder) String() string      { return AsString(node) }
func (node dNull) String() string             { return AsString(node) }
func (list *NameList) String() string         { return AsString(list) }

// GetBinType get the return type from a binary expr's left type and right type
func GetBinType(tl, tr types.T, operator Operator) (types.T, bool) {
	_, ok1 := tl.(types.TArray)
	_, ok2 := tr.(types.TArray)
	if operator == Concat && (!ok1 && !ok2) {
		return types.String, true
	}
	if operator == Bitand || operator == Bitor || operator == Bitxor {
		return tl, false
	}
	if (operator == LShift || operator == RShift) && tl != types.BitArray {
		return types.Int, true
	}
	switch tl {
	case types.Bool:
		switch tr {
		case types.BitArray, types.Bool:
			return types.Int, true
		case types.String, types.Date, types.Time, types.Timestamp, types.TimestampTZ:
			return types.Decimal, true
		case types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal:
			return tr, true
		default:
			return tl, false
		}
	case types.Decimal:
		return types.Decimal, true
	case types.String:
		if operator == Concat && tr == types.JSON {
			return types.JSON, true
		} else if operator == Plus || operator == Minus || operator == Mult || operator == Div || operator == Mod || operator == FloorDiv || operator == Pow {
			if tr == types.Float || tr == types.Float4 {
				return types.Float, true
			}
			return types.Decimal, true
		}
		return types.String, true
	case types.Int, types.Int2, types.Int4:
		switch tr {
		case types.Float, types.Float4:
			return types.Float, true
		case types.String:
			return types.Decimal, true
		case types.Decimal:
			return types.Decimal, true
		case types.Int, types.Int2, types.Int4, types.Bool, types.BitArray:
			switch operator {
			case Div, Mult, Pow:
				if tr == types.Int || tr == types.Int2 || tr == types.Int4 || tr == types.BitArray {
					return types.Decimal, true
				}
			case Concat:
				return types.String, true
			}
			return types.Int, true
		case types.Date:
			if operator == Mult || operator == Div || operator == FloorDiv || operator == Mod || operator == Pow {
				return types.Decimal, true
			} else if operator == Minus {
				return types.Int, true
			}
			return tl, false
		case types.Time, types.Timestamp, types.TimestampTZ:
			return types.Decimal, true
		default:
			return tl, false
		}
	case types.Float, types.Float4:
		switch tr {
		case types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Time, types.Timestamp, types.TimestampTZ, types.Date, types.Bool, types.BitArray, types.String:
			return types.Float, true
		case types.Decimal:
			return types.Decimal, true
		default:
			return tl, false
		}
	case types.BitArray:
		switch tr {
		case types.Int, types.Int2, types.Int4, types.Bool:
			if operator == RShift || operator == LShift {
				if tr == types.Int || tr == types.Int2 || tr == types.Int4 {
					return types.BitArray, true
				}
				return tr, false

			}
			if (tr == types.Int || tr == types.Int2 || tr == types.Int4) && (operator == Mult || operator == Div || operator == Pow) {
				return types.Decimal, true
			}
			return types.Int, true
		case types.Float, types.Float4:
			return types.Float, true
		case types.String, types.Decimal, types.Time, types.Timestamp, types.TimestampTZ:
			return types.Decimal, true
		case types.Date:
			return types.Int, true
		default:
			return tl, false
		}
	case types.Date:
		switch tr {
		case types.Int, types.Int2, types.Int4:
			if operator == Mult || operator == Div || operator == FloorDiv || operator == Mod || operator == Pow {
				return types.Decimal, true
			}
			return tl, false
		case types.Float, types.Float4:
			return types.Float, true
		case types.Date, types.BitArray:
			return types.Int, true
		case types.Bool, types.String, types.Timestamp, types.TimestampTZ, types.Decimal:
			return types.Decimal, true
		case types.Time:
			if operator == Mult || operator == Div || operator == FloorDiv || operator == Mod || operator == Pow {
				return types.Decimal, true
			}
			fallthrough
		default:
			return tl, false
		}
	case types.Time:
		switch tr {
		case types.Bool, types.Int, types.Int2, types.Int4, types.Decimal, types.BitArray:
			return types.Decimal, true
		case types.Float4, types.Float:
			return types.Float, true
		case types.Date, types.Time, types.Timestamp, types.TimestampTZ:
			if operator == Mult || operator == Div || operator == FloorDiv || operator == Mod || operator == Pow {
				return types.Decimal, true
			}
			if operator == Minus && (tr == types.Date || tr == types.Timestamp || tr == types.TimestampTZ) {
				return types.Decimal, true
			}
			if operator == Plus && (tr == types.Date || tr == types.Timestamp || tr == types.TimestampTZ) {
				if tr == types.TimestampTZ {
					return types.TimestampTZ, true
				}
				return types.Timestamp, true
			}
			fallthrough
		default:
			return tl, false
		}
	case types.Timestamp:
		switch tr {
		case types.Bool, types.Int, types.Int2, types.Int4, types.Decimal:
			return types.Decimal, true
		case types.Float, types.Float4:
			return types.Float, true
		case types.Date, types.Timestamp, types.TimestampTZ, types.BitArray:
			if (tr == types.Timestamp || tr == types.TimestampTZ) && operator == Minus {
				return types.Interval, true
			}
			return types.Decimal, true
		case types.Time:
			if operator == Plus || operator == Minus {
				return types.Timestamp, true
			}
			return types.Decimal, true
		case types.String:
			return types.Decimal, true
		default:
			return tl, false
		}
	case types.TimestampTZ:
		switch tr {
		case types.Bool, types.Int, types.Int2, types.Int4, types.Decimal:
			return types.Decimal, true
		case types.Float, types.Float4:
			return types.Float, true
		case types.Date, types.Timestamp, types.TimestampTZ, types.BitArray:
			if (tr == types.Timestamp || tr == types.TimestampTZ) && operator == Minus {
				return types.Interval, true
			}
			return types.Decimal, true
		case types.Time:
			if operator == Plus || operator == Minus {
				return types.TimestampTZ, true
			}
			return types.Decimal, true
		case types.String:
			return types.Decimal, true
		default:
			return tl, false
		}
	default:
		return tl, false
	}
}

// GetCmpType Get Cmp Type
func GetCmpType(tl types.T, tr types.T, operator Operator) (types.T, bool) {
	if tl == tr {
		return tl, true
	}
	switch tl {
	case types.Bool:
		if tr == types.String {
			return types.Bool, true
		}
		return types.Decimal, true
	case types.String:
		switch tr {
		case types.String, types.Date, types.Timestamp, types.TimestampTZ, types.Bool, types.BitArray:
			return tr, true
		case types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal:
			return types.Decimal, true
		default:
			return tl, false
		}
	case types.Int, types.Int2, types.Int4:
		switch tr {
		case types.Float, types.Float4:
			return types.Decimal, true
		case types.String:
			return types.Decimal, true
		case types.BitArray:
			return types.Int, true
		case types.Int, types.Int2, types.Int4, types.Decimal, types.Bool, types.Date, types.Time, types.Timestamp, types.TimestampTZ:
			return types.Decimal, true
		default:
			return tl, false
		}
	case types.Float, types.Float4:
		switch tr {
		case types.Float, types.Float4:
			return types.Float, true
		case types.String, types.Int, types.Int2, types.Int4, types.BitArray:
			return types.Decimal, true
		case types.Decimal, types.Bool, types.Date, types.Time, types.Timestamp, types.TimestampTZ:
			return types.Decimal, true
		default:
			return tl, false
		}
	case types.Decimal:
		switch tr {
		case types.String, types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.BitArray, types.Bool, types.Date, types.Time, types.Timestamp, types.TimestampTZ:
			return types.Decimal, true
		default:
			return tl, false
		}
	case types.Time:
		switch tr {
		case types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal:
			return types.Decimal, true
		case types.BitArray, types.Date, types.Bool, types.Timestamp, types.TimestampTZ:
			return types.Decimal, true
		case types.String:
			return types.Time, true
		default:
			return tl, false
		}
	case types.Date:
		switch tr {
		case types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal:
			return types.Decimal, true
		case types.BitArray, types.Time, types.Bool, types.Timestamp, types.TimestampTZ:
			return types.Decimal, true
		case types.String:
			return types.Date, true
		default:
			return tl, false
		}
	case types.Timestamp:
		switch tr {
		case types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.Date, types.Time, types.TimestampTZ, types.BitArray, types.Bool:
			return types.Decimal, true
		case types.String:
			return types.Timestamp, true
		default:
			return tl, false
		}
	case types.TimestampTZ:
		switch tr {
		case types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.Date, types.Time, types.Timestamp, types.BitArray, types.Bool:
			return types.Decimal, true
		case types.String:
			return types.TimestampTZ, true
		default:
			return tl, false
		}
	case types.BitArray:
		switch tr {
		case types.Int, types.Int2, types.Int4:
			return types.Int, true
		case types.Decimal, types.Float, types.Float4, types.Date, types.Time, types.Timestamp, types.TimestampTZ, types.Bool:
			return types.Decimal, true
		case types.String:
			return tl, true
		default:
			return tl, false
		}
	default:
		return tl, false
	}

}

// GetUnionType Get Union Type
func GetUnionType(tl types.T, tr types.T) (types.T, bool) {
	if _, ok := tl.(types.TtSet); ok {
		return types.String, true
	}
	if _, ok := tr.(types.TtSet); ok {
		return types.String, true
	}
	if tl == tr {
		return tl, true
	}
	switch tl {
	case types.Bool:
		switch tr {
		case types.Int, types.Int2, types.Int4:
			return types.Int, true
		case types.Float, types.Float4, types.Decimal, types.BitArray:
			if tr == types.Float4 {
				return types.Float, true
			}
			return tr, true
		case types.String, types.Date, types.Time, types.Interval, types.Timestamp, types.TimestampTZ:
			return types.String, true
		}
	case types.Int, types.Int2, types.Int4:
		switch tr {
		case types.Int, types.Int2, types.Int4, types.Bool, types.BitArray:
			return types.Int, true
		case types.Float, types.Float4, types.Decimal:
			if tr == types.Float4 {
				return types.Float, true
			}
			return tr, true
		case types.String, types.Date, types.Time, types.Interval, types.Timestamp, types.TimestampTZ:
			return types.String, true
		}
	case types.Float, types.Float4:
		switch tr {
		case types.Bool, types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.BitArray:
			return types.Float, true
		case types.Decimal:
			return types.Decimal, true
		case types.String, types.Date, types.Time, types.Interval, types.Timestamp, types.TimestampTZ:
			return types.String, true
		}
	case types.Decimal:
		switch tr {
		case types.Bool, types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.BitArray:
			return types.Decimal, true
		case types.String, types.Date, types.Time, types.Interval, types.Timestamp, types.TimestampTZ:
			return types.String, true
		}
	case types.String:
		return types.String, true
	case types.Date:
		switch tr {
		case types.Bool, types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.BitArray, types.String, types.Interval:
			return types.String, true
		case types.Time, types.Timestamp:
			return types.Timestamp, true
		case types.TimestampTZ:
			return types.TimestampTZ, true
		}
	case types.Time:
		switch tr {
		case types.Time:
			return types.Time, true
		case types.Bool, types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.BitArray, types.String, types.Interval:
			return types.String, true
		case types.Date, types.Timestamp:
			return types.Timestamp, true
		case types.TimestampTZ:
			return types.TimestampTZ, true
		}
	case types.Interval:
		switch tr {
		case types.Interval:
			return types.Interval, true
		case types.Bool, types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.BitArray, types.String, types.Date, types.Time, types.Timestamp, types.TimestampTZ:
			return types.String, true
		}
	case types.Timestamp:
		switch tr {
		case types.Bool, types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.String, types.Interval, types.BitArray:
			return types.String, true
		case types.Date, types.Time, types.TimestampTZ:
			return types.Timestamp, true
		}
	case types.TimestampTZ:
		switch tr {
		case types.Bool, types.Int, types.Int2, types.Int4, types.Float, types.Float4, types.Decimal, types.String, types.Interval, types.BitArray:
			return types.String, true
		case types.Date, types.Time:
			return types.TimestampTZ, true
		case types.Timestamp:
			return types.Timestamp, true
		}
	case types.BitArray:
		switch tr {
		case types.String, types.Date, types.Time, types.Interval, types.Timestamp, types.TimestampTZ:
			return types.String, true
		case types.Int2, types.Int4, types.Int:
			return types.Int, true
		case types.Float, types.Float4:
			return types.Float, true
		case types.Decimal:
			return types.Decimal, true
		case types.Bool:
			return tl, true
		}
	}
	return tl, false
}
