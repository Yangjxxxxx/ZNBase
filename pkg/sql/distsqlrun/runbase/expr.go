// Copyright 2016 The Cockroach Authors.
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

package runbase

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/transform"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// ivarBinder is a tree.Visitor that binds ordinal references
// (IndexedVars represented by @1, @2, ...) to an IndexedVarContainer.
type ivarBinder struct {
	h   *tree.IndexedVarHelper
	err error
}

func (v *ivarBinder) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	if ivar, ok := expr.(*tree.IndexedVar); ok {
		newVar, err := v.h.BindIfUnbound(ivar)
		if err != nil {
			v.err = err
			return false, expr
		}
		return false, newVar
	}
	return true, expr
}

func (*ivarBinder) VisitPost(expr tree.Expr) tree.Expr { return expr }

// FunctionCollectVisitor is a visitor of tree which collection the functions used this tree expressions
type FunctionCollectVisitor struct {
	// funcCollections is function set
	funcCollections []sqlbase.FunctionDescriptor
	// err store for check error
	err error
}

//NewFunctionCollectVisitor use to collect new visitor
func NewFunctionCollectVisitor(functions []sqlbase.FunctionDescriptor) *FunctionCollectVisitor {
	return &FunctionCollectVisitor{
		funcCollections: functions,
	}
}

//VisitPre use to collect function visitor
func (v *FunctionCollectVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.Type == tree.UDRFuncType {
			v.err = errors.Errorf("function %s cannot be executed with distsql", t.String())
			return false, expr
		}
		var funcDef tree.FunctionDefinition
		var funcName string
		// extract function name from funcExrp
		if name, ok := t.Func.FunctionReference.(*tree.UnresolvedName); ok {
			funcName = name.Parts[0]
		}
		funcDef.Definition = []tree.OverloadImpl{}
		funcDef.Name = funcName
		for _, funcDesc := range v.funcCollections {
			if funcDesc.Name != funcName {
				continue
			}
			// add opt function into optCatalog for query optimizer
			typesVal := tree.ArgTypes{}
			for _, arg := range funcDesc.Args {
				if arg.InOutMode == "in" || arg.InOutMode == "inout" {
					typesVal = append(typesVal, tree.ArgType{Name: arg.Name, Typ: types.OidToType[arg.Oid]})
				}
			}
			bytes, err := protoutil.Marshal(&funcDesc)
			if err != nil {
				v.err = err
				return false, nil
			}
			overload := &tree.Overload{
				Types:      typesVal,
				ReturnType: tree.FixedReturnType(types.OidToType[funcDesc.RetOid]),
				Name:       funcDesc.FullFuncName,
				Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
					var execargs tree.ExecUdrArgs
					c := make(chan tree.ResultData)
					defer close(c)
					execargs.Ret = c
					execargs.Args = args
					execargs.FuncDescBytes = bytes
					execargs.Name = ctx.UdrFunctionFullName
					execargs.Ctx = ctx
					execargs.ArgTypes = typesVal
					m := ctx.Mon
					tree.ExecChan <- execargs
					ret := <-c
					ctx.Mon = m
					ctx.UDFCTX = true
					return ret.Result, ret.Err
				},
				Info: "Calculates the absolute value of `val`.",
			}
			funcDef.Definition = append(funcDef.Definition, overload)
		}
		funcDef.AmbiguousReturnType = false
		funcDef.Category = "the user define func"
		funcDef.Class = tree.NormalClass
		funcDef.DistsqlBlacklist = false
		funcDef.Impure = false
		funcDef.NullableArgs = false
		funcDef.Private = false
		funcDef.NeedsRepeatedEvaluation = false
		funcDef.UnsupportedWithIssue = 0
		funcDef.ReturnLabels = nil
		t.Func.FunctionReference = &funcDef
	}
	return true, expr
}

//VisitPost use to collect function vistpost
func (v *FunctionCollectVisitor) VisitPost(expr tree.Expr) (newNode tree.Expr) {
	return expr
}

// processExpression parses the string expression inside an Expression,
// and associates ordinal references (@1, @2, etc) with the given helper.
func processExpression(
	exprSpec distsqlpb.Expression,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	h *tree.IndexedVarHelper,
	functions []sqlbase.FunctionDescriptor,
) (tree.TypedExpr, error) {
	if exprSpec.Expr == "" {
		return nil, nil
	}
	expr, err := parser.ParseExpr(exprSpec.Expr)
	if err != nil {
		return nil, err
	}
	// Check function Descriptor
	if functions != nil {
		visitor := NewFunctionCollectVisitor(functions)
		tree.WalkExprConst(visitor, expr)
	}
	// Bind IndexedVars to our eh.vars.
	v := ivarBinder{h: h, err: nil}
	expr, _ = tree.WalkExpr(&v, expr)
	if v.err != nil {
		return nil, v.err
	}
	var typedExpr tree.TypedExpr
	semaCtx.IVarContainer = h.Container()
	e, ok := expr.(*tree.FuncExpr)
	if ok && exprSpec.UdrFuncDef != nil {
		e.Func.FunctionReference = exprSpec.UdrFuncDef
		typedExpr, err = e.TypeCheck(semaCtx, types.Any, false)

	} else {
		// Convert to a fully typed expression.
		typedExpr, err = tree.TypeCheck(expr, semaCtx, types.Any, false)
		if err != nil {
			return nil, errors.Wrap(err, expr.String())
		}
	}

	// Pre-evaluate constant expressions. This is necessary to avoid repeatedly
	// re-evaluating constant values every time the expression is applied.
	//
	// TODO(solon): It would be preferable to enhance our expression serialization
	// format so this wouldn't be necessary.
	c := tree.MakeConstantEvalVisitor(evalCtx)
	expr, _ = tree.WalkExpr(&c, typedExpr)
	if err := c.Err(); err != nil {
		return nil, err
	}

	return expr.(tree.TypedExpr), nil
}

// FuncResolver use to help resolve user define function
type FuncResolver interface {
	ResolveFunc() tree.TypedExpr
}

// ExprHelper implements the common logic around evaluating an expression that
// depends on a set of values.
type ExprHelper struct {
	FuncResolver

	_ util.NoCopy

	Expr tree.TypedExpr
	// vars is used to generate IndexedVars that are "backed" by the values in
	// `row`.
	Vars tree.IndexedVarHelper

	EvalCtx *tree.EvalContext

	Types            []sqlbase.ColumnType
	Row              sqlbase.EncDatumRow
	datumAlloc       sqlbase.DatumAlloc
	IsInsertOrUpdate bool

	FunctionDescCollection []sqlbase.FunctionDescriptor
}

//NewExprHelper use to help NewExpr
func NewExprHelper(function []sqlbase.FunctionDescriptor) *ExprHelper {
	return &ExprHelper{
		FunctionDescCollection: function,
	}
}

func (eh *ExprHelper) String() string {
	if eh.Expr == nil {
		return "none"
	}
	return eh.Expr.String()
}

// ExprHelper implements tree.IndexedVarContainer.
var _ tree.IndexedVarContainer = &ExprHelper{}

// IndexedVarResolvedType is part of the tree.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarResolvedType(idx int) types.T {
	return eh.Types[idx].ToDatumType()
}

// IndexedVarEval is part of the tree.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	err := eh.Row[idx].EnsureDecoded(&eh.Types[idx], &eh.datumAlloc)
	if err != nil {
		return nil, err
	}
	return eh.Row[idx].Datum.Eval(ctx)
}

// IndexedVarNodeFormatter is part of the parser.IndexedVarContainer interface.
func (eh *ExprHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

// GetVisibleType implements the parser.IndexedVarContainer interface.
func (eh *ExprHelper) GetVisibleType(idx int) string {
	return ""
}

// SetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (eh *ExprHelper) SetForInsertOrUpdate(b bool) {
	eh.IsInsertOrUpdate = b
}

// GetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (eh *ExprHelper) GetForInsertOrUpdate() bool {
	return eh.IsInsertOrUpdate
}

// Init initialize ExprHelper
func (eh *ExprHelper) Init(
	Expr distsqlpb.Expression, types []sqlbase.ColumnType, evalCtx *tree.EvalContext,
) error {
	if Expr.Empty() {
		return nil
	}
	eh.EvalCtx = evalCtx
	eh.Types = types
	eh.Vars = tree.MakeIndexedVarHelper(eh, len(types))

	if Expr.LocalExpr != nil {
		eh.Expr = Expr.LocalExpr
		// Bind IndexedVars to our eh.vars.
		eh.Vars.Rebind(eh.Expr, true /* alsoReset */, false /* normalizeToNonNil */)
		return nil
	}
	var err error
	semaContext := tree.MakeSemaContext()
	eh.Expr, err = processExpression(Expr, evalCtx, &semaContext, &eh.Vars, eh.FunctionDescCollection)
	if err != nil {
		return err
	}
	var t transform.ExprTransformContext
	if t.AggregateInExpr(eh.Expr, evalCtx.SessionData.SearchPath) {
		return errors.Errorf("Expression '%s' has aggregate", eh.Expr)
	}
	return nil
}

// EvalFilter is used for filter Expressions; it evaluates the Expression and
// returns whether the filter passes.
func (eh *ExprHelper) EvalFilter(row sqlbase.EncDatumRow) (bool, error) {
	eh.Row = row
	eh.EvalCtx.PushIVarContainer(eh)
	pass, err := sqlbase.RunFilter(eh.Expr, eh.EvalCtx)
	eh.EvalCtx.PopIVarContainer()
	return pass, err
}

// Eval Given a row, eval evaluates the wrapped expression and returns the
// resulting datum. For example, given a row (1, 2, 3, 4, 5):
//  '@2' would return '2'
//  '@2 + @5' would return '7'
//  '@1' would return '1'
//  '@2 + 10' would return '12'
func (eh *ExprHelper) Eval(row sqlbase.EncDatumRow) (tree.Datum, error) {
	eh.Row = row

	eh.EvalCtx.PushIVarContainer(eh)
	d, err := eh.Expr.Eval(eh.EvalCtx)
	eh.EvalCtx.PopIVarContainer()
	return d, err
}

// InitWithRemapping 初始化ExprHelper。
// indexVarMap指定了一个可选的(也就是说，它可以为空)映射
// 用于在将IndexedVars的索引绑定到容器之前重新映射索引。
func (eh *ExprHelper) InitWithRemapping(
	expr distsqlpb.Expression, types []types1.T, evalCtx *tree.EvalContext, indexVarMap []int,
) error {
	if expr.Empty() {
		return nil
	}
	eh.EvalCtx = evalCtx
	tys, _ := sqlbase.DatumType1sToColumnTypes(types)
	eh.Types = tys
	eh.Vars = tree.MakeIndexedVarHelperWithRemapping(eh, len(types), indexVarMap)

	if expr.LocalExpr != nil {
		eh.Expr = expr.LocalExpr
		// Bind IndexedVars to our eh.Vars.
		eh.Vars.Rebind(eh.Expr, true /* alsoReset */, false /* normalizeToNonNil */)
		return nil
	}
	var err error
	semaContext := tree.MakeSemaContext()
	eh.Expr, err = processExpression(expr, evalCtx, &semaContext, &eh.Vars, eh.FunctionDescCollection)
	if err != nil {
		return err
	}
	var t transform.ExprTransformContext
	if t.AggregateInExpr(eh.Expr, evalCtx.SessionData.SearchPath) {
		return errors.Errorf("expression '%s' has aggregate", eh.Expr)
	}
	return nil
}
