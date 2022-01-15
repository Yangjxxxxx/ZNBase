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

	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

// IndexedVarContainer provides the implementation of TypeCheck, Eval, and
// String for IndexedVars.
type IndexedVarContainer interface {
	IndexedVarEval(idx int, ctx *EvalContext) (Datum, error)
	IndexedVarResolvedType(idx int) types.T
	// IndexedVarNodeFormatter returns a NodeFormatter; if an object that
	// wishes to implement this interface has lost the textual name that an
	// IndexedVar originates from, this function can return nil (and the
	// ordinal syntax "@1, @2, .." will be used).
	IndexedVarNodeFormatter(idx int) NodeFormatter

	SetForInsertOrUpdate(b bool)
	GetForInsertOrUpdate() bool
	// GetVisibleType 接收用户定义类型
	GetVisibleType(idx int) string
}

// ScopeIdentify provides the implementation of GetScopeStmt for scope.
type ScopeIdentify interface {
	GetScopeStmt() Statement
}

// IndexedVar is a VariableExpr that can be used as a leaf in expressions; it
// represents a dynamic value. It defers calls to TypeCheck, Eval, String to an
// IndexedVarContainer.
type IndexedVar struct {
	Idx         int
	Used        bool
	bindInPlace bool

	// VisibleType 表示用户自定义类型
	VisibleType string

	col NodeFormatter

	typeAnnotation
}

var _ TypedExpr = &IndexedVar{}

// Variable is a dummy function part of the VariableExpr interface.
func (*IndexedVar) Variable() {}

// Walk is part of the Expr interface.
func (v *IndexedVar) Walk(_ Visitor) Expr {
	return v
}

// TypeCheck is part of the Expr interface.
func (v *IndexedVar) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	_, isForInsertHelper := ctx.IVarContainer.(*ForInsertHelper)
	if ctx.IVarContainer == nil || ctx.IVarContainer == unboundContainer || isForInsertHelper {
		// A more technically correct message would be to say that the
		// reference is unbound and thus cannot be typed. However this is
		// a tad bit too technical for the average SQL use case and
		// instead we acknowledge that we only get here if someone has
		// used a column reference in a place where it's not allowed by
		// the docs, so just say that instead.
		return nil, pgerror.NewErrorf(
			pgcode.UndefinedColumn, "column reference @%d not allowed in this context", v.Idx+1)
	}
	v.Typ = ctx.IVarContainer.IndexedVarResolvedType(v.Idx)
	return v, nil
}

// Eval is part of the TypedExpr interface.
func (v *IndexedVar) Eval(ctx *EvalContext) (Datum, error) {
	_, isForInsertHelper := ctx.IVarContainer.(*ForInsertHelper)
	if ctx.IVarContainer == nil || ctx.IVarContainer == unboundContainer || isForInsertHelper {
		panic("indexed var must be bound to a container before evaluation")
	}
	return ctx.IVarContainer.IndexedVarEval(v.Idx, ctx)
}

// SetIntType set type INT for IndexedVar
func (v *IndexedVar) SetIntType() {
	v.Typ = types.Int
}

// ResolvedType is part of the TypedExpr interface.
func (v *IndexedVar) ResolvedType() types.T {
	if v.Typ == nil {
		panic("indexed var must be type checked first")
	}
	return v.Typ
}

// Format implements the NodeFormatter interface.
func (v *IndexedVar) Format(ctx *FmtCtx) {
	f := ctx.flags
	if ctx.indexedVarFormat != nil {
		ctx.indexedVarFormat(ctx, v.Idx)
	} else if f.HasFlags(fmtSymbolicVars) || v.col == nil {
		ctx.Printf("@%d", v.Idx+1)
	} else {
		v.col.Format(ctx)
	}
}

// NewOrdinalReference is a helper routine to create a standalone
// IndexedVar with the given index value. This needs to undergo
// BindIfUnbound() below before it can be fully used.
func NewOrdinalReference(r int) *IndexedVar {
	return &IndexedVar{Idx: r}
}

// NewTypedOrdinalReference returns a new IndexedVar with the given index value
// that is verified to be well-typed.
func NewTypedOrdinalReference(r int, typ types.T) *IndexedVar {
	return &IndexedVar{Idx: r, typeAnnotation: typeAnnotation{Typ: typ}}
}

// NewIndexedVar is a helper routine to create a standalone Indexedvar
// with the given index value. This needs to undergo BindIfUnbound()
// below before it can be fully used. The difference with ordinal
// references is that vars returned by this constructor are modified
// in-place by BindIfUnbound.
//
// Do not use NewIndexedVar for AST nodes that can undergo binding two
// or more times.
func NewIndexedVar(r int) *IndexedVar {
	return &IndexedVar{Idx: r, bindInPlace: true}
}

// IndexedVarHelper wraps an IndexedVarContainer (an interface) and creates
// IndexedVars bound to that container.
//
// It also keeps track of which indexes from the container are used by
// expressions.
type IndexedVarHelper struct {
	vars      []IndexedVar
	container IndexedVarContainer

	// indexVarMap是IndexedVars索引的可选映射。如果它是
	//非空，它将被用来确定一个IndexedVar的“实际索引”
	//绑定之前——而不是使用ivar。Idx，一个新的IndexedVar将有
	// indexVarMap [ivar]。作为它的索引。
	indexVarMap []int
}

// Container returns the container associated with the helper.
func (h *IndexedVarHelper) Container() IndexedVarContainer {
	return h.container
}

func (h *IndexedVarHelper) getIndex(ivar *IndexedVar) int {
	if ivar.Used {
		// ivar has already been bound, so the remapping step (if it was needed)
		// has already occurred.
		return ivar.Idx
	}
	if h.indexVarMap != nil {
		// indexVarMap is non-nil, so we need to remap the index.
		return h.indexVarMap[ivar.Idx]
	}
	// indexVarMap is nil, so we return the index as is.
	return ivar.Idx
}

// BindIfUnbound ensures the IndexedVar is attached to this helper's container.
// - for freshly created IndexedVars (with a nil container) this will bind in-place.
// - for already bound IndexedVar, bound to this container, this will return the same ivar unchanged.
// - for ordinal references (with an explicit unboundContainer) this will return a new var.
// - for already bound IndexedVars, bound to another container, this will error out.
func (h *IndexedVarHelper) BindIfUnbound(ivar *IndexedVar) (*IndexedVar, error) {
	// We perform the range check always, even if the ivar is already
	// bound, as a form of safety assertion against misreuse of ivars
	// across containers.
	ivarIdx := h.getIndex(ivar)
	if ivarIdx < 0 || ivarIdx >= len(h.vars) {
		return ivar, pgerror.NewErrorf(
			pgcode.UndefinedColumn, "invalid column ordinal: @%d", ivar.Idx+1)
	}

	if !ivar.Used {
		if ivar.bindInPlace {
			// This container must also remember it has "seen" the variable
			// so that IndexedVarUsed() below returns the right results.
			// The IndexedVar() method ensures this.
			*ivar = *h.IndexedVar(ivarIdx)
			return ivar, nil
		}
		return h.IndexedVar(ivarIdx), nil
	}
	return ivar, nil
}

// MakeIndexedVarHelper initializes an IndexedVarHelper structure.
func MakeIndexedVarHelper(container IndexedVarContainer, numVars int) IndexedVarHelper {
	return IndexedVarHelper{vars: make([]IndexedVar, numVars), container: container}
}

// AppendSlot expands the capacity of this IndexedVarHelper by one and returns
// the index of the new slot.
func (h *IndexedVarHelper) AppendSlot() int {
	h.vars = append(h.vars, IndexedVar{})
	return len(h.vars) - 1
}

func (h *IndexedVarHelper) checkIndex(idx int) {
	if idx < 0 || idx >= len(h.vars) {
		panic(fmt.Sprintf("invalid var index %d (columns: %d)", idx, len(h.vars)))
	}
}

// NumVars returns the number of variables the IndexedVarHelper was initialized
// for.
func (h *IndexedVarHelper) NumVars() int {
	return len(h.vars)
}

// IndexedVar returns an IndexedVar for the given index. The index must be
// valid.
func (h *IndexedVarHelper) IndexedVar(idx int) *IndexedVar {
	h.checkIndex(idx)
	v := &h.vars[idx]
	v.Idx = idx
	v.Used = true
	v.Typ = h.container.IndexedVarResolvedType(idx)
	v.col = h.container.IndexedVarNodeFormatter(idx)
	v.VisibleType = h.container.GetVisibleType(idx)
	return v
}

// IndexedVarWithType returns an IndexedVar for the given index, with the given
// type. The index must be valid. This should be used in the case where an
// indexed var is being added before its container has a corresponding entry
// for it.
func (h *IndexedVarHelper) IndexedVarWithType(idx int, typ types.T) *IndexedVar {
	h.checkIndex(idx)
	v := &h.vars[idx]
	v.Idx = idx
	v.Used = true
	v.Typ = typ
	return v
}

// IndexedVarUsed returns true if IndexedVar() was called for the given index.
// The index must be valid.
func (h *IndexedVarHelper) IndexedVarUsed(idx int) bool {
	h.checkIndex(idx)
	return h.vars[idx].Used
}

// GetIndexedVars returns the indexed var array of this helper.
// IndexedVars to the caller; unused vars are guaranteed to have
// a false Used field.
func (h *IndexedVarHelper) GetIndexedVars() []IndexedVar {
	return h.vars
}

// Reset re-initializes an IndexedVarHelper structure with the same
// number of slots. After a helper has been reset, all the expressions
// that were linked to the helper before it was reset must be
// re-bound, e.g. using Rebind(). Resetting is useful to ensure that
// the helper's knowledge of which IndexedVars are actually used by
// linked expressions is up to date, especially after
// optimizations/transforms which eliminate sub-expressions. The
// optimizations performed by setNeededColumns() work then best.
//
// TODO(knz): groupNode and windowNode hold on to IndexedVar's after a Reset().
func (h *IndexedVarHelper) Reset() {
	h.vars = make([]IndexedVar, len(h.vars))
}

// Rebind collects all the IndexedVars in the given expression
// and re-binds them to this helper.
func (h *IndexedVarHelper) Rebind(expr TypedExpr, alsoReset, normalizeToNonNil bool) TypedExpr {
	if alsoReset {
		h.Reset()
	}
	if expr == nil || expr == DBoolTrue {
		if normalizeToNonNil {
			return DBoolTrue
		}
		return nil
	}
	ret, _ := WalkExpr(h, expr)
	return ret.(TypedExpr)
}

var _ Visitor = &IndexedVarHelper{}

// VisitPre implements the Visitor interface.
func (h *IndexedVarHelper) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if iv, ok := expr.(*IndexedVar); ok {
		return false, h.IndexedVar(iv.Idx)
	}
	return true, expr
}

// VisitPost implements the Visitor interface.
func (*IndexedVarHelper) VisitPost(expr Expr) Expr { return expr }

type unboundContainerType struct {
	IsInsertOrUpdate bool
}

// unboundContainer is the marker used by ordinal references (@N) in
// the input syntax. It differs from `nil` in that calling
// BindIfUnbound on an indexed var that uses unboundContainer will
// cause a new IndexedVar object to be returned, to ensure that the
// original is left unchanged (to preserve the invariant that the AST
// is constant after parse).
var unboundContainer = &unboundContainerType{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (*unboundContainerType) IndexedVarEval(idx int, _ *EvalContext) (Datum, error) {
	panic(fmt.Sprintf("unbound ordinal reference @%d", idx+1))
}

// GetVisibleType implements the parser.IndexedVarContainer interface.
func (*unboundContainerType) GetVisibleType(idx int) string {
	return ""
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (*unboundContainerType) IndexedVarResolvedType(idx int) types.T {
	panic(fmt.Sprintf("unbound ordinal reference @%d", idx+1))
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (*unboundContainerType) IndexedVarNodeFormatter(idx int) NodeFormatter {
	panic(fmt.Sprintf("unbound ordinal reference @%d", idx+1))
}

// SetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (u *unboundContainerType) SetForInsertOrUpdate(b bool) {
	u.IsInsertOrUpdate = b
}

// GetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (u *unboundContainerType) GetForInsertOrUpdate() bool {
	return u.IsInsertOrUpdate
}

type typeContainer struct {
	types            []types.T
	IsInsertOrUpdate bool
}

var _ IndexedVarContainer = &typeContainer{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (tc *typeContainer) IndexedVarEval(idx int, ctx *EvalContext) (Datum, error) {
	panic("no eval allowed in typeContainer")
}

// GetVisibleType implements the parser.IndexedVarContainer interface.
func (tc *typeContainer) GetVisibleType(idx int) string {
	return ""
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (tc *typeContainer) IndexedVarResolvedType(idx int) types.T {
	return tc.types[idx]
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (tc *typeContainer) IndexedVarNodeFormatter(idx int) NodeFormatter {
	return nil
}

// SetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (tc *typeContainer) SetForInsertOrUpdate(b bool) {
	tc.IsInsertOrUpdate = b
}

// GetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (tc *typeContainer) GetForInsertOrUpdate() bool {
	return tc.IsInsertOrUpdate
}

// MakeTypesOnlyIndexedVarHelper creates an IndexedVarHelper which provides
// the given types for indexed vars. It does not support evaluation, unless
// Rebind is used with another container which supports evaluation.
func MakeTypesOnlyIndexedVarHelper(types []types.T) IndexedVarHelper {
	c := &typeContainer{types: types}
	return MakeIndexedVarHelper(c, len(types))
}

// ForInsertHelper implements the IndexedVarContainer interface to help
// judge if an expr use in an insertStmt or a updateStmt
type ForInsertHelper struct {
	IsInsertOrUpdate bool
}

// GetVisibleType implements the parser.IndexedVarContainer interface.
func (f *ForInsertHelper) GetVisibleType(idx int) string {
	return ""
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (f *ForInsertHelper) IndexedVarResolvedType(idx int) types.T {
	return types.Int
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (f *ForInsertHelper) IndexedVarEval(idx int, ctx *EvalContext) (Datum, error) {
	return nil, nil
}

// IndexedVarNodeFormatter implements the parser.IndexedVarContainer interface.
func (f *ForInsertHelper) IndexedVarNodeFormatter(idx int) NodeFormatter {
	n := Name(fmt.Sprintf("var%d", idx))
	return &n
}

// SetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (f *ForInsertHelper) SetForInsertOrUpdate(b bool) {
	f.IsInsertOrUpdate = b
}

// GetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (f *ForInsertHelper) GetForInsertOrUpdate() bool {
	return f.IsInsertOrUpdate
}

// MakeIndexedVarHelperWithRemapping initializes an IndexedVarHelper structure.
// An optional (it can be left nil) indexVarMap argument determines a mapping
// for indices of IndexedVars (see comment above for more details).
func MakeIndexedVarHelperWithRemapping(
	container IndexedVarContainer, numVars int, indexVarMap []int,
) IndexedVarHelper {
	return IndexedVarHelper{
		vars:        make([]IndexedVar, numVars),
		container:   container,
		indexVarMap: indexVarMap,
	}
}
