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

package optbuilder

import (
	"context"
	"fmt"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/sql/delegate"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/norm"
	"github.com/znbasedb/znbase/pkg/sql/opt/optgen/exprgen"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/transform"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

// Builder holds the context needed for building a memo structure from a SQL
// statement. Builder.Build() is the top-level function to perform this build
// process. As part of the build process, it performs name resolution and
// type checking on the expressions within Builder.stmt.
//
// The memo structure is the primary data structure used for query optimization,
// so building the memo is the first step required to optimize a query. The memo
// is maintained inside Builder.factory, which exposes methods to construct
// expression groups inside the memo. Once the expression tree has been built,
// the builder calls SetRoot on the memo to indicate the root memo group, as
// well as the set of physical properties (e.g., row and column ordering) that
// at least one expression in the root group must satisfy.
//
// A memo is essentially a compact representation of a forest of logically-
// equivalent query trees. Each tree is either a logical or a physical plan
// for executing the SQL query. After the build process is complete, the memo
// forest will contain exactly one tree: the logical query plan corresponding
// to the AST of the original SQL statement with some number of "normalization"
// transformations applied. Normalization transformations include heuristics
// such as predicate push-down that should always be applied. They do not
// include "exploration" transformations whose benefit must be evaluated with
// the optimizer's cost model (e.g., join reordering).
//
// See factory.go and memo.go inside the opt/xform package for more details
// about the memo structure.
type Builder struct {
	// AllowUnsupportedExpr is a control knob: if set, when building a scalar, the
	// builder takes any TypedExpr node that it doesn't recognize and wraps that
	// expression in an UnsupportedExpr node. This is temporary; it is used for
	// interfacing with the old planning code.
	AllowUnsupportedExpr bool

	// KeepPlaceholders is a control knob: if set, optbuilder will never replace
	// a placeholder operator with its assigned value, even when it is available.
	// This is used when re-preparing invalidated queries.
	KeepPlaceholders bool

	// FmtFlags controls the way column names are formatted in test output. For
	// example, if set to FmtAlwaysQualifyTableNames, the builder fully qualifies
	// the table name in all column aliases before adding them to the metadata.
	// This flag allows us to test that name resolution works correctly, and
	// avoids cluttering test output with schema and catalog names in the general
	// case.
	FmtFlags tree.FmtFlags

	// IsCorrelated is set to true during semantic analysis if a scalar variable was
	// pulled from an outer scope, that is, if the query was found to be correlated.
	IsCorrelated bool

	// HadPlaceholders is set to true if we replaced any placeholders with their
	// values.
	HadPlaceholders bool

	// DisableMemoReuse is set to true if we encountered a statement that is not
	// safe to cache the memo for. This is the case for various DDL and SHOW
	// statements.
	DisableMemoReuse bool

	factory *norm.Factory
	stmt    tree.Statement

	ctx              context.Context
	semaCtx          *tree.SemaContext
	evalCtx          *tree.EvalContext
	catalog          cat.Catalog
	exprTransformCtx transform.ExprTransformContext
	scopeAlloc       []scope

	// upperOwner represent owner of upper object if necessary.
	// It helps to check privilege level by level.
	// checkPrivilege works for current user if upperOwner is empty.
	upperOwner string

	// views contains a cache of views that have already been parsed, in case they
	// are referenced multiple times in the same query.
	views map[cat.View]*tree.Select

	// subquery contains a pointer to the subquery which is currently being built
	// (if any).
	subquery *subquery

	// If set, we are processing a view definition; in this case, catalog caches
	// are disabled and certain statements (like mutations) are disallowed.
	insideViewDef bool

	// If set, we are collecting view dependencies in viewDeps. This can only
	// happen inside view definitions.
	//
	// When a view depends on another view, we only want to track the dependency
	// on the inner view itself, and not the transitive dependencies (so
	// trackViewDeps would be false inside that inner view).
	trackViewDeps bool
	viewDeps      opt.ViewDeps
	viewHasStar   bool
	CaseSensitive bool

	// Select Update Insert
	QueryType string
	// 为了标记整个路径中的block offset
	Blockoffset int
	// map[query block]map[table name]hint
	TableHint map[string]map[string][]*tree.TableHint
	// map[query block]map[table name]hint, 暂时只支持全局index
	IndexHint map[string]map[string][]*tree.IndexHint
	// writeOnView label only when DML operation on VIEW, which will set false when buildView inside subqueries.
	writeOnView bool

	// IsCanaryCol stores a flag (true if there's a canary column, vice versa)
	IsCanaryCol bool

	// IsPortal: Whether Type of The Executor is ExecPortal
	IsExecPortal bool
}

// New creates a new Builder structure initialized with the given
// parsed SQL statement.
func New(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	catalog cat.Catalog,
	factory *norm.Factory,
	stmt tree.Statement,
) *Builder {
	return &Builder{
		factory: factory,
		stmt:    stmt,
		ctx:     ctx,
		semaCtx: semaCtx,
		evalCtx: evalCtx,
		catalog: catalog,
	}
}

//GetFactoty in order to access evalCtx simply
func (b *Builder) GetFactoty() *tree.EvalContext {
	return b.evalCtx
}

// Build is the top-level function to build the memo structure inside
// Builder.factory from the parsed SQL statement in Builder.stmt. See the
// comment above the Builder type declaration for details.
//
// If any subroutines panic with a builderError or pgerror.Error as part of the
// build process, the panic is caught here and returned as an error.
func (b *Builder) Build() (err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate semantic and internal errors without
			// adding lots of checks for `if err != nil` throughout the code. This is
			// only possible because the code does not update shared state and does
			// not manipulate locks.
			switch e := r.(type) {
			case TryOldPath:
				err = e.ERR
			case builderError:
				err = e.error
			case *pgerror.Error:
				err = e
			default:
				panic(r)
			}
		}
	}()

	// Special case for CannedOptPlan.
	if canned, ok := b.stmt.(*tree.CannedOptPlan); ok {
		b.factory.DisableOptimizations()
		_, err := exprgen.Build(b.catalog, b.factory, canned.Plan)
		return err
	}
	//初始化block offset
	switch b.stmt.(type) {
	case *tree.Select:
		b.Blockoffset = 0
	case *tree.Update, *tree.Delete:
		b.Blockoffset = 0
	}
	// Build the memo, and call SetRoot on the memo to indicate the root group
	// and physical properties.
	outScope := b.buildStmt(b.stmt, nil, b.allocScope())
	b.evalCtx.ViewOrderBy = nil
	physical := outScope.makePhysicalProps()
	b.factory.Memo().SetRoot(outScope.expr, physical)
	return nil
}

// builderError is used to wrap errors returned by various external APIs that
// occur during the build process. It exists for us to be able to panic on these
// errors and then catch them inside Builder.Build even if they are not
// pgerror.Error.
type builderError struct {
	error
}

// TryOldPath 实现暂时部分sql不支持memo整合或者和权限相关的sql，返回到上层走回非优化过程
type TryOldPath struct {
	ERR error
}

func (t TryOldPath) Error() string {
	return t.ERR.Error()
}

// unimplementedWithIssueDetailf formats according to a format
// specifier and returns a Postgres error with the
// pgcode.FeatureNotSupported code, wrapped in a
// builderError.
func unimplementedWithIssueDetailf(
	issue int, detail, format string, args ...interface{},
) *pgerror.Error {
	return pgerror.UnimplementedWithIssueDetailErrorf(issue, detail, format, args...)
}

// buildStmt builds a set of memo groups that represent the given SQL
// statement.
//
// NOTE: The following descriptions of the inScope parameter and outScope
//       return value apply for all buildXXX() functions in this directory.
//       Note that some buildXXX() functions pass outScope as a parameter
//       rather than a return value so its scopeColumns can be built up
//       incrementally across several function calls.
//
// inScope   This parameter contains the name bindings that are visible for this
//           statement/expression (e.g., passed in from an enclosing statement).
//
// outScope  This return value contains the newly bound variables that will be
//           visible to enclosing statements, as well as a pointer to any
//           "parent" scope that is still visible. The top-level memo group ID
//           for the built statement/expression is returned in outScope.group.
func (b *Builder) buildStmt(
	stmt tree.Statement, desiredTypes []types.T, inScope *scope,
) (outScope *scope) {
	if b.insideViewDef {
		// A black list of statements that can't be used from inside a view.
		switch stmt := stmt.(type) {
		case *tree.Delete /**tree.Insert,*/, *tree.Update, *tree.CreateTable, *tree.CreateView,
			*tree.Split, *tree.Relocate,
			*tree.ControlJobs, *tree.CancelQueries, *tree.CancelSessions:
			panic(builderError{fmt.Errorf(
				"%s cannot be used inside a view definition", stmt.StatementTag())})
		}
	}

	// NB: The case statements are sorted lexicographically.
	switch stmt := stmt.(type) {
	case *tree.CreateTable:
		if len(stmt.InhRelations) > 0 {
			if outScope := b.tryBuildOpaque(stmt, inScope); outScope != nil {
				// The opaque handler may resolve objects; we don't care about caching
				// plans for these statements.
				b.DisableMemoReuse = true
				return outScope
			}
		}
		return b.buildCreateTable(stmt, inScope)

	case *tree.Delete:
		return b.buildDelete(stmt, inScope)

	case *tree.Explain:
		return b.buildExplain(stmt, inScope)

	case *tree.Insert:
		return b.buildInsert(stmt, inScope)

	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, noRowLocking, desiredTypes, inScope, nil)

	case *tree.Select:
		b.Blockoffset++
		return b.buildSelect(stmt, noRowLocking, desiredTypes, inScope, nil)

	case *tree.ShowTraceForSession:
		return b.buildShowTrace(stmt, inScope)

	case *tree.Update:
		return b.buildUpdate(stmt, inScope)

	case *tree.CreateView:
		return b.buildCreateView(stmt, inScope)

	case *tree.Split:
		return b.buildAlterTableSplit(stmt, inScope)

	case *tree.Relocate:
		return b.buildAlterTableRelocate(stmt, inScope)

	case *tree.ControlJobs:
		return b.buildControlJobs(stmt, inScope)

	case *tree.CancelQueries:
		return b.buildCancelQueries(stmt, inScope)

	case *tree.CancelSessions:
		return b.buildCancelSessions(stmt, inScope)

	default:
		// See if this statement can be rewritten to another statement using the
		// delegate functionality.
		newStmt, err := delegate.TryDelegate(b.ctx, b.catalog, b.evalCtx, stmt)
		if err != nil {
			panic(builderError{err})
		}
		if newStmt != nil {
			// Many delegate implementations resolve objects. It would be tedious to
			// register all those dependencies with the metadata (for cache
			// invalidation). We don't care about caching plans for these statements.
			b.DisableMemoReuse = true
			return b.buildStmt(newStmt, desiredTypes, inScope)
		}

		// See if we have an opaque handler registered for this statement type.
		if outScope := b.tryBuildOpaque(stmt, inScope); outScope != nil {
			// The opaque handler may resolve objects; we don't care about caching
			// plans for these statements.
			b.DisableMemoReuse = true
			return outScope
		}
		panic(errors.AssertionFailedf("unexpected statement: %T", stmt))
	}
}

// BuildStmtAndReturnScalarExpr use to Build Stmt And Return ScalarExpr and typedExpr
func (b *Builder) BuildStmtAndReturnScalarExpr(
	stmt tree.Statement,
) (opt.ScalarExpr, tree.TypedExpr) {
	outScope := b.buildStmt(stmt, nil, b.allocScope())
	return outScope.cols[0].scalar, outScope.cols[0].expr
}

func (b *Builder) allocScope() *scope {
	if len(b.scopeAlloc) == 0 {
		// scope is relatively large (~250 bytes), so only allocate in small
		// chunks.
		b.scopeAlloc = make([]scope, 4)
	}
	r := &b.scopeAlloc[0]
	b.scopeAlloc = b.scopeAlloc[1:]
	r.builder = b
	return r
}

//SetInUDR use to set in udr
func (b *Builder) SetInUDR(inUDR bool) {
	b.semaCtx.InUDR = inUDR
}

//SetUDRParam use to set udr param
func (b *Builder) SetUDRParam(params tree.UDRVars) {
	b.semaCtx.UDRParms = params
}
