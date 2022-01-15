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

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/exec"
	"github.com/znbasedb/znbase/pkg/sql/opt/exec/execbuilder"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/opt/optbuilder"
	"github.com/znbasedb/znbase/pkg/sql/opt/xform"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/querycache"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

var queryCacheEnabled = settings.RegisterBoolSetting(
	"sql.query_cache.enabled", "enable the query cache", true,
)

// prepareUsingOptimizer builds a memo for a prepared statement and populates
// the following stmt.Prepared fields:
//  - Columns
//  - Types
//  - AnonymizedStmt
//  - Memo (for reuse during exec, if appropriate).
//
// On success, the returned flags always have planFlagOptpkg/sql/opt/partialidx/implicator.goUsed set.
//
// isCorrelated is set in error cases if we detect a correlated subquery; it is
// used in the fallback case to create a better error.
func (p *planner) prepareUsingOptimizer(
	ctx context.Context,
) (_ planFlags, isCorrelated bool, _ error) {
	stmt := p.stmt
	p.curPlan = planTop{AST: stmt.AST}

	opc := &p.optPlanningCtx
	opc.reset()
	stmt.Prepared.AnonymizedStr = anonymizeStmt(stmt.AST)

	switch stmt.AST.(type) {
	case *tree.AlterIndex, *tree.AlterTable, *tree.AlterSequence,
		*tree.BeginTransaction,
		*tree.CommentOnColumn, *tree.CommentOnDatabase, *tree.CommentOnIndex, *tree.CommentOnTable, *tree.CommentOnConstraint,
		*tree.CommitTransaction,
		*tree.CopyFrom, *tree.CreateDatabase, *tree.CreateIndex, *tree.CreateView,
		*tree.CreateSequence,
		*tree.CreateStats,
		*tree.Deallocate, *tree.Discard, *tree.DropDatabase, *tree.DropIndex,
		*tree.DropTable, *tree.DropView, *tree.DropSequence,
		*tree.Execute,
		*tree.Grant, *tree.GrantRole,
		*tree.Prepare,
		*tree.ReleaseSavepoint, *tree.RenameColumn, *tree.RenameDatabase,
		*tree.RenameIndex, *tree.RenameTable, *tree.Revoke, *tree.RevokeRole,
		*tree.RollbackToSavepoint, *tree.RollbackTransaction,
		*tree.Savepoint, *tree.SetTransaction, *tree.SetTracing,
		*tree.SetSessionCharacteristics:
		// These statements do not have result columns and do not support placeholders
		// so there is no need to do anything during prepare.
		//
		// Some of these statements (like BeginTransaction) aren't supported by the
		// optbuilder so they would error out. Others (like CreateIndex) have planning
		// code that can introduce unnecessary txn retries (because of looking up
		// descriptors and such).
		return opc.flags, false, nil
	}

	if opc.useCache {
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, stmt.SQL)
		if ok && cachedData.PrepareMetadata != nil {
			pm := cachedData.PrepareMetadata
			// Check that the type hints match (the type hints affect type checking).
			if !pm.TypeHints.Equals(p.semaCtx.Placeholders.TypeHints) {
				opc.log(ctx, "query cache hit but type hints don't match")
			} else {
				isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog)
				if err != nil {
					return 0, false, err
				}
				if !isStale {
					opc.log(ctx, "query cache hit (prepare)")
					opc.flags.Set(planFlagOptCacheHit)
					stmt.Prepared.AnonymizedStr = pm.AnonymizedStr
					stmt.Prepared.Columns = pm.Columns
					stmt.Prepared.Types = pm.Types
					stmt.Prepared.Memo = cachedData.Memo
					stmt.Prepared.IsCorrelated = cachedData.IsCorrelated
					return opc.flags, cachedData.IsCorrelated, nil
				}
				opc.log(ctx, "query cache hit but memo is stale (prepare)")
			}
		} else if ok {
			opc.log(ctx, "query cache hit but there is no prepare metadata")
		} else {
			opc.log(ctx, "query cache miss")
		}
		opc.flags.Set(planFlagOptCacheMiss)
	}

	memo, isCorrelated, err := opc.buildReusableMemo(ctx)
	if err != nil {
		//Solve the problem that JDBC connects to the database and uses the cursor to obtain data
		if n, ok := stmt.AST.(*tree.CursorAccess); ok {
			if find := strings.Contains(stmt.SQL, "fetch"); find {
				cursorName := n.CursorName.String()
				_, desc, err := FindCursor(p.ExecCfg(), p.ExtendedEvalContext(), cursorName)
				if desc != nil {
					stmt.Prepared.Columns = sqlbase.ResultColumnsFromColDescs(desc.Columns)
					for key, value := range desc.Columns {
						if value.Name == "rowid" {
							stmt.Prepared.Columns = stmt.Prepared.Columns[:key]
						}
					}
				}
				if err != nil {
					return 0, isCorrelated, err
				}
			}
		}
		return 0, isCorrelated, err
	}

	md := memo.Metadata()
	physical := memo.RootProps()
	resultCols := make(sqlbase.ResultColumns, len(physical.Presentation))
	for i, col := range physical.Presentation {
		resultCols[i].Name = col.Alias
		resultCols[i].Typ = md.ColumnMeta(col.ID).Type
		if err := checkResultType(resultCols[i].Typ); err != nil {
			return 0, isCorrelated, err
		}
	}

	// Verify that all placeholder types have been set.
	if err := p.semaCtx.Placeholders.Types.AssertAllSet(); err != nil {
		return 0, isCorrelated, err
	}

	stmt.Prepared.Columns = resultCols
	stmt.Prepared.Types = p.semaCtx.Placeholders.Types
	if opc.allowMemoReuse {
		stmt.Prepared.Memo = memo
		stmt.Prepared.IsCorrelated = isCorrelated
		if opc.useCache {
			// execPrepare sets the PrepareMetadata.InferredTypes field after this
			// point. However, once the PrepareMetadata goes into the cache, it
			// can't be modified without causing race conditions. So make a copy of
			// it now.
			// TODO(radu): Determine if the extra object allocation is really
			// necessary.
			pm := stmt.Prepared.PrepareMetadata
			cachedData := querycache.CachedData{
				SQL:             stmt.SQL,
				Memo:            memo,
				PrepareMetadata: &pm,
				IsCorrelated:    isCorrelated,
			}
			p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		}
	}
	return opc.flags, isCorrelated, nil
}

// makeOptimizerPlan is an alternative to makePlan which uses the cost-based
// optimizer. On success, the returned flags always have planFlagOptUsed set.
//
// isCorrelated is set in error cases if we detect a correlated subquery; it is
// used in the fallback case to create a better error.
func (p *planner) makeOptimizerPlan(ctx context.Context) error {
	stmt := p.stmt
	// Cursor optimizer mode check.
	if err := OptimizerModeCheck(stmt.AST); err != nil {
		return err
	}

	opc := &p.optPlanningCtx
	opc.reset()

	execMemo, err := opc.buildExecMemo(ctx)
	if err != nil {
		return err
	}
	if mode := p.SessionData().ExperimentalDistSQLPlanningMode; mode != sessiondata.ExperimentalDistSQLPlanningOff {
		planningMode := distSQLDefaultPlanning
		err := opc.runExecBuilder(
			&p.curPlan,
			p.stmt,
			newDistSQLSpecExecFactory(p, planningMode),
			execMemo,
			p.EvalContext(),
			p.autoCommit,
		)
		if err != nil {
			if mode == sessiondata.ExperimentalDistSQLPlanningAlways &&
				!strings.Contains(p.stmt.AST.StatementTag(), "SET") {
				return err
			}
			// We will fallback to the old path.
		} else {
			err = opc.runExecBuilder(
				&p.curPlan,
				p.stmt,
				newDistSQLSpecExecFactory(p, distSQLLocalOnlyPlanning),
				execMemo,
				p.EvalContext(),
				p.autoCommit,
			)

			if err == nil {
				return nil
			}
		}
		// TODO(yuzefovich): make the logging conditional on the verbosity
		// level once new DistSQL planning is no longer experimental.
		log.Infof(
			ctx, "distSQLSpecExecFactory failed planning with %v, falling back to the old path", err,
		)
	}

	return opc.runExecBuilder(
		&p.curPlan,
		p.stmt,
		newExecFactory(p),
		execMemo,
		p.EvalContext(),
		p.autoCommit,
	)
}

func checkOptSupportForTopStatement(AST tree.Statement) error {
	// Start with fast check to see if top-level statement is supported.
	switch t := AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause,
		*tree.UnionClause, *tree.ValuesClause, *tree.Explain,
		*tree.Insert, *tree.Update, *tree.Delete, *tree.CreateView,
		*tree.CannedOptPlan:
		return nil
	case *tree.CreateTable:
		if len(t.InhRelations) == 0 {
			return nil
		}
		return pgerror.Unimplemented("statement", fmt.Sprintf("unsupported statement: %T", AST))

	default:
		return pgerror.Unimplemented("statement", fmt.Sprintf("unsupported statement: %T", AST))
	}
}

type optPlanningCtx struct {
	p *planner

	// catalog is initialized once, and reset for each query. This allows the
	// catalog objects to be reused across queries in the same session.
	catalog optCatalog

	// -- Fields below are reinitialized for each query ---

	optimizer xform.Optimizer

	// When set, we are allowed to reuse a memo, or store a memo for later reuse.
	allowMemoReuse bool

	// When set, we consult and update the query cache. Never set if
	// allowMemoReuse is false.
	useCache bool

	flags planFlags

	// a flag to point that if in UDR progress
	InUDR bool
}

// init performs one-time initialization of the planning context; reset() must
// also be called before each use.
func (opc *optPlanningCtx) init(p *planner) {
	opc.p = p
	opc.catalog.init(p)
}

// reset initializes the planning context for the statement in the planner.
func (opc *optPlanningCtx) reset() {
	p := opc.p
	opc.catalog.reset()

	opc.optimizer.Init(p.EvalContext())
	if p.execCfg.LocationName.Names != nil {
		opc.optimizer.InitXQ(p.execCfg.LocationName)

	}

	if err := checkOptSupportForTopStatement(p.stmt.AST); err != nil {
		opc.flags = 0
	} else {
		opc.flags = planFlagOptUsed
	}

	// We only allow memo caching for SELECT/INSERT/UPDATE/DELETE. We could
	// support it for all statements in principle, but it would increase the
	// surface of potential issues (conditions we need to detect to invalidate a
	// cached memo).
	switch p.stmt.AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause, *tree.UnionClause, *tree.ValuesClause,
		*tree.Insert, *tree.Update, *tree.Delete, *tree.CannedOptPlan:
		// If the current transaction has uncommitted DDL statements, we cannot rely
		// on descriptor versions for detecting a "stale" memo. This is because
		// descriptor versions are bumped at most once per transaction, even if there
		// are multiple DDL operations; and transactions can be aborted leading to
		// potential reuse of versions. To avoid these issues, we prevent saving a
		// memo (for prepare) or reusing a saved memo (for execute).
		opc.allowMemoReuse = !p.Tables().hasUncommittedTables()
		opc.useCache = opc.allowMemoReuse && queryCacheEnabled.Get(&p.execCfg.Settings.SV)
		if _, isCanned := p.stmt.AST.(*tree.CannedOptPlan); isCanned {
			// It's unsafe to use the cache, since PREPARE AS OPT PLAN doesn't track
			// dependencies and check permissions.
			opc.useCache = false
		}

	default:
		opc.allowMemoReuse = false
		opc.useCache = false
	}
}

func (opc *optPlanningCtx) log(ctx context.Context, msg string) {
	if log.VDepth(1, 1) {
		log.InfofDepth(ctx, 1, "%s: %s", msg, opc.p.stmt)
	} else {
		log.Event(ctx, msg)
	}
}

// buildReusableMemo builds the statement into a memo that can be stored for
// prepared statements and can later be used as a starting point for
// optimization. The returned memo is fully detached from the planner and can be
// used with reuseMemo independently and concurrently by multiple threads.
//
// isCorrelated is set in error cases if we detect a correlated subquery; it is
// used in the fallback case to create a better error.
func (opc *optPlanningCtx) buildReusableMemo(
	ctx context.Context,
) (_ *memo.Memo, isCorrelated bool, _ error) {
	p := opc.p

	_, isCanned := opc.p.stmt.AST.(*tree.CannedOptPlan)
	if isCanned {
		if !p.EvalContext().SessionData.AllowPrepareAsOptPlan {
			return nil, false, errors.Errorf(
				"PREPARE AS OPT PLAN is a testing facility that should not be used directly",
			)
		}

		if p.SessionData().User != security.RootUser {
			return nil, false, errors.Errorf(
				"PREPARE AS OPT PLAN may only be used by root",
			)
		}
	}

	// Build the Memo (optbuild) and apply normalization rules to it. If the
	// query contains placeholders, values are not assigned during this phase,
	// as that only happens during the EXECUTE phase. If the query does not
	// contain placeholders, then also apply exploration rules to the Memo so
	// that there's even less to do during the EXECUTE phase.
	//
	f := opc.optimizer.Factory()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	bld.KeepPlaceholders = true
	bld.IsExecPortal = p.stmt.IsExecPortal
	bld.SetInUDR(opc.InUDR)
	defer func() {
		bld.SetInUDR(false)
	}()
	bld.CaseSensitive = opc.p.SessionData().CaseSensitive
	if err := bld.Build(); err != nil {
		return nil, bld.IsCorrelated, err
	}

	if bld.DisableMemoReuse {
		// The builder encountered a statement that prevents safe reuse of the memo.
		opc.allowMemoReuse = false
		opc.useCache = false
	}

	if isCanned {
		if f.Memo().HasPlaceholders() {
			// We don't support placeholders inside the canned plan. The main reason
			// is that they would be invisible to the parser (which is reports the
			// number of placeholders, used to initialize the relevant structures).
			return nil, false, errors.Errorf("placeholders are not supported with PREPARE AS OPT PLAN")
		}
		// With a canned plan, the memo is already optimized.
	} else {
		// If the memo doesn't have placeholders, then fully optimize it, since
		// it can be reused without further changes to build the execution tree.
		if !f.Memo().HasPlaceholders() {
			if _, err := opc.optimizer.Optimize(); err != nil {
				return nil, bld.IsCorrelated, err
			}
		}
	}

	// Detach the prepared memo from the factory and transfer its ownership
	// to the prepared statement. DetachMemo will re-initialize the optimizer
	// to an empty memo.
	return opc.optimizer.DetachMemo(), false, nil
}

// reuseMemo returns an optimized memo using a cached memo as a starting point.
//
// The cached memo is not modified; it is safe to call reuseMemo on the same
// cachedMemo from multiple threads concurrently.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
func (opc *optPlanningCtx) reuseMemo(cachedMemo *memo.Memo) (*memo.Memo, error) {
	if !cachedMemo.HasPlaceholders() {
		// If there are no placeholders, the query was already fully optimized
		// (see buildReusableMemo).
		return cachedMemo, nil
	}
	f := opc.optimizer.Factory()
	// Finish optimization by assigning any remaining placeholders and
	// applying exploration rules. Reinitialize the optimizer and construct a
	// new memo that is copied from the prepared memo, but with placeholders
	// assigned.
	if err := f.AssignPlaceholders(cachedMemo); err != nil {
		return nil, err
	}
	if _, err := opc.optimizer.Optimize(); err != nil {
		return nil, err
	}
	return f.Memo(), nil
}

// buildExecMemo creates a fully optimized memo, possibly reusing a previously
// cached memo as a starting point.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.

func (opc *optPlanningCtx) buildExecMemo(ctx context.Context) (_ *memo.Memo, _ error) {
	var unuseFlag = true
	prepared := opc.p.stmt.Prepared
	p := opc.p
	if sel, ok := p.stmt.AST.(*tree.Select); ok {
		//StartWith recursive unuse reuseMemo
		unuseFlag = !(sel.With != nil && sel.With.StartWith)
	}
	if opc.allowMemoReuse && prepared != nil && prepared.Memo != nil {
		// We are executing a previously prepared statement and a reusable memo is
		// available.

		// If the prepared memo has been invalidated by schema or other changes,
		// re-prepare it.
		if isStale, err := prepared.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
			return nil, err
		} else if isStale {
			prepared.Memo, _, err = opc.buildReusableMemo(ctx)
			opc.log(ctx, "rebuilding cached memo")
			if err != nil {
				return nil, err
			}
		}
		opc.log(ctx, "reusing cached memo")
		memo, err := opc.reuseMemo(prepared.Memo)
		return memo, err
	}

	if opc.useCache && unuseFlag {
		// Consult the query cache.
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, opc.p.stmt.SQL)
		if ok {
			// 暂时只检查metadata里面的列
			udrVarIsStable := false
			for i := 0; i < cachedData.Memo.Metadata().NumColumns(); i++ {
				colMeta := cachedData.Memo.Metadata().ColumnMeta(opt.ColumnID(i + 1))
				if checkMemoColIsStaleUDRVar(colMeta, p) {
					udrVarIsStable = true
					break
				}
			}

			if isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
				return nil, err
			} else if isStale || udrVarIsStable {
				cachedData.Memo, _, err = opc.buildReusableMemo(ctx)
				if err != nil {
					return nil, err
				}
				// Update the plan in the cache. If the cache entry had PrepareMetadata
				// populated, it may no longer be valid.
				cachedData.PrepareMetadata = nil
				p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
				opc.log(ctx, "query cache hit but needed update")
				opc.flags.Set(planFlagOptCacheMiss)
			} else {
				opc.log(ctx, "query cache hit")
				opc.flags.Set(planFlagOptCacheHit)
			}
			memo, err := opc.reuseMemo(cachedData.Memo)
			return memo, err
		}
		opc.flags.Set(planFlagOptCacheMiss)
		opc.log(ctx, "query cache miss")
	} else {
		opc.log(ctx, "not using query cache")
	}
	// We are executing a statement for which there is no reusable memo
	// available.
	f := opc.optimizer.Factory()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	bld.SetInUDR(opc.InUDR)
	bld.IsExecPortal = p.stmt.IsExecPortal
	defer func() {
		bld.SetInUDR(false)
	}()
	//从hint中拿出在优化过程中需要的信息 TODO(zyk):有冲突的hint要做warning提示
	opc.handleStmtHints(bld, opc.p.stmt.AST)
	bld.CaseSensitive = opc.p.SessionData().CaseSensitive

	bld.GetFactoty().SessionData.SQL = opc.p.stmt.SQL
	if err := bld.Build(); err != nil {
		return nil, err
	}
	if _, isCanned := opc.p.stmt.AST.(*tree.CannedOptPlan); !isCanned {
		if _, err := opc.optimizer.Optimize(); err != nil {
			return nil, err
		}
	}

	// If this statement doesn't have placeholders, add it to the cache. Note
	// that non-prepared statements from pgwire clients cannot have
	// placeholders.
	if opc.useCache && !bld.HadPlaceholders && !bld.DisableMemoReuse {
		memo := opc.optimizer.DetachMemo()
		cachedData := querycache.CachedData{
			SQL:  opc.p.stmt.SQL,
			Memo: memo,
		}
		p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		opc.log(ctx, "query cache add")
		return memo, nil
	}
	return f.Memo(), nil
}

// runExecBuilder execbuilds a plan using the given factory and stores the
// result in planTop. If required, also captures explain data using the explain
// factory.
func (opc *optPlanningCtx) runExecBuilder(
	planTop *planTop,
	stmt *Statement,
	f exec.Factory,
	mem *memo.Memo,
	evalCtx *tree.EvalContext,
	allowAutoCommit bool,
) error {
	var result *planComponents
	var isDDL bool

	// No instrumentation.
	bld := execbuilder.New(f, mem, mem.RootExpr(), evalCtx)
	plan, err := bld.Build()
	if err != nil {
		return err
	}

	result = plan.(*planComponents)
	isDDL = bld.IsDDL

	if stmt.ExpectedTypes != nil {
		cols := result.main.planColumns()
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			return pgerror.NewError(pgcode.FeatureNotSupported, "cached plan must not change result type")
		}
	}

	planTop.planComponents = *result
	planTop.flags = opc.flags
	if isDDL {
		planTop.flags.Set(planFlagIsDDL)
	}
	return nil
}

// 将在优化过程中需要的hint加到ctx中
func (opc *optPlanningCtx) handleStmtHints(bld *optbuilder.Builder, stmt tree.Statement) {

	if explain, ok := stmt.(*tree.Explain); ok {
		stmt = explain.Statement
	}
	tableHints := tree.HintSet{}
	switch x := stmt.(type) {
	case *tree.Select:
		if selClause, ok := x.Select.(*tree.SelectClause); ok {
			tableHints = selClause.HintSet
		}
	default:
		return
	}
	for i := 0; i < len(tableHints); i++ {
		hint := tableHints[i]
		if hint.GetHintType() == keys.TableLevelHint {
			tableHint := hint.GetTableHint()
			switch tableHint.HintName {
			case "nth_plan":
				opc.optimizer.ForcePlanTh = true
				opc.optimizer.PlanTh = tableHint.HintData.(int) & 1023
			}
		}
	}
}

// return true represent the memo is invalid and meed to rebuild, return false represent the memo is valid
func checkMemoColIsStaleUDRVar(colMeta *opt.ColumnMeta, p *planner) bool {
	udrParams := p.semaCtx.UDRParms
	if colMeta.Expr != nil {
		if plVar, ok := colMeta.Expr.(*tree.PlpgsqlVar); ok {
			if !p.semaCtx.InUDR {
				return true
			}
			if plVar.IsPlaceHolder {
				if coltypes.CastTargetToDatumType(udrParams[plVar.Index].ColType) == plVar.VarType {
					return false
				}
				return true
			}
			i := len(udrParams) - 1
			for ; i >= 0; i-- {
				if udrParams[i].VarName == colMeta.Alias || tree.FindNameInNames(colMeta.Alias, udrParams[i].AliasNames) {
					v := udrParams[i]
					if coltypes.CastTargetToDatumType(v.ColType) == plVar.VarType {
						return false
					}
					return true
				}
			}
		}
	}
	return false
}
