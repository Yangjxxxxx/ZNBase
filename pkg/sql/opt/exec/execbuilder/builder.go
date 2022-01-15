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

package execbuilder

import (
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/exec"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util/errorutil"
)

// Builder constructs a tree of execution nodes (exec.Node) from an optimized
// expression tree (opt.Expr).
type Builder struct {
	factory            exec.Factory
	mem                *memo.Memo
	e                  opt.Expr
	disableTelemetry   bool
	evalCtx            *tree.EvalContext
	fastIsConstVisitor fastIsConstVisitor

	// subqueries accumulates information about subqueries that are part of scalar
	// expressions we built. Each entry is associated with a tree.Subquery
	// expression node.
	subqueries []exec.Subquery

	// nullifyMissingVarExprs, if greater than 0, tells the builder to replace
	// VariableExprs that have no bindings with DNull. This is useful for apply
	// join, which needs to be able to create a plan that has outer columns.
	// The number indicates the depth of apply joins.
	nullifyMissingVarExprs int

	// -- output --

	// IsDDL is set to true if the statement contains DDL.
	IsDDL bool

	// withExprs is the set of With expressions which may be referenced elsewhere
	// in the query.
	// TODO(justin): set this up so that we can look them up by index lookups
	// rather than scans.
	withExprs []builtWithExpr

	// forceForUpdateLocking is conditionally passed through to factory methods
	// for scan operators that serve as the input for mutation operators. When
	// set to true, it ensures that a FOR UPDATE row-level locking mode is used
	// by scans. See forUpdateLocking.
	forceForUpdateLocking bool
}

// builtWithExpr is metadata regarding a With expression which has already been
// added to the set of subqueries for the query.
type builtWithExpr struct {
	id opt.WithID
	// outputCols maps the output ColumnIDs of the With expression to the ordinal
	// positions they are output to. See execPlan.outputCols for more details.
	outputCols opt.ColMap
	bufferNode exec.Node
}

// New constructs an instance of the execution node builder using the
// given factory to construct nodes. The Build method will build the execution
// node tree from the given optimized expression tree.
func New(factory exec.Factory, mem *memo.Memo, e opt.Expr, evalCtx *tree.EvalContext) *Builder {
	return &Builder{factory: factory, mem: mem, e: e, evalCtx: evalCtx}
}

// New constructs an instance of the execution node builder using the
// given factory to construct nodes. The Build method will build the execution
// node tree from the given optimized expression tree.
//
// catalog is only needed if the statement contains an EXPLAIN (OPT, CATALOG).

//func NewBuilder(
//	factory exec.Factory, mem *memo.Memo, catalog cat.Catalog, e opt.Expr, evalCtx *tree.EvalContext,
//) *Builder {
//	b := &Builder{
//		factory: factory,
//		mem:     mem,
//		catalog: catalog,
//		e:       e,
//		evalCtx: evalCtx,
//	}
//	if evalCtx != nil {
//		if evalCtx.SessionData.SaveTablesPrefix != "" {
//			b.nameGen = memo.NewExprNameGenerator(evalCtx.SessionData.SaveTablesPrefix)
//		}
//		b.allowInsertFastPath = evalCtx.SessionData.InsertFastPath
//	}
//	return b
//}

// DisableTelemetry prevents the execbuilder from updating telemetry counters.
func (b *Builder) DisableTelemetry() {
	b.disableTelemetry = true
}

// Build constructs the execution node tree and returns its root node if no
// error occurred.
func (b *Builder) Build() (_ exec.Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal errors without having to add
			// error checks everywhere throughout the code. This is only possible
			// because the code does not update shared state and does not manipulate
			// locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else if pgErr, ok := r.(*pgerror.Error); ok {
				err = pgErr
			} else {
				panic(r)
			}
		}
	}()

	plan, err := b.build(b.e)
	if err != nil {
		return nil, err
	}
	return b.factory.ConstructPlan(plan.root, b.subqueries)
}

func (b *Builder) build(e opt.Expr) (execPlan, error) {
	rel, ok := e.(memo.RelExpr)
	if !ok {
		return execPlan{}, errors.Errorf("building execution for non-relational operator %s", e.Op())
	}
	return b.buildRelational(rel)
}

// BuildScalar converts a scalar expression to a TypedExpr. Variables are mapped
// according to the IndexedVarHelper.
func (b *Builder) BuildScalar(ivh *tree.IndexedVarHelper) (tree.TypedExpr, error) {
	scalar, ok := b.e.(opt.ScalarExpr)
	if !ok {
		return nil, errors.Errorf("BuildScalar cannot be called for non-scalar operator %s", b.e.Op())
	}
	ctx := buildScalarCtx{ivh: *ivh}
	for i := 0; i < ivh.NumVars(); i++ {
		ctx.ivarMap.Set(i+1, i)
	}
	return b.buildScalar(&ctx, scalar)
}

func (b *Builder) decorrelationError() error {
	return errors.Errorf("could not decorrelate subquery")
}

func (b *Builder) addBuiltWithExpr(id opt.WithID, outputCols opt.ColMap, bufferNode exec.Node) {
	b.withExprs = append(b.withExprs, builtWithExpr{
		id:         id,
		outputCols: outputCols,
		bufferNode: bufferNode,
	})
}
