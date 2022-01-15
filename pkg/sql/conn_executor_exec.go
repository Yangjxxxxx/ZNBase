// Copyright 2018  The Cockroach Authors.D
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
	"runtime/pprof"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/opt/optbuilder"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgnotice"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/errorutil/unimplemented"
	"github.com/znbasedb/znbase/pkg/util/fsm"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/retry"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// AutoCommitDDL is to enable DDL statements to be committed immediately within a transaction.
var AutoCommitDDL = settings.RegisterBoolSetting(
	"kv.transaction.auto_commit_ddl",
	"if set, DDL transaction can commit implicitly with begin commit",
	false,
)

// NoticesEnabled is the cluster setting that allows users
// to enable notices.
var NoticesEnabled = settings.RegisterBoolSetting(
	"sql.notices.enabled",
	"enable notices in the server/client protocol being sent",
	true,
)

// gatedNoticeSender is a noticeSender which can be gated by a cluster setting.
type gatedNoticeSender struct {
	noticeSender
	sv *settings.Values
}

// BufferNotice implements the noticeSender interface.
func (s *gatedNoticeSender) BufferNotice(notice pgnotice.Notice) {
	if !NoticesEnabled.Get(s.sv) {
		return
	}
	s.noticeSender.BufferNotice(notice)
}

// execStmt executes one statement by dispatching according to the current
// state. Returns an Event to be passed to the state machine, or nil if no
// transition is needed. If nil is returned, then the cursor is supposed to
// advance to the next statement.
//
// If an error is returned, the session is supposed to be considered done. Query
// execution errors are not returned explicitly and they're also not
// communicated to the client. Instead they're incorporated in the returned
// event (the returned payload will implement payloadWithError). It is the
// caller's responsibility to deliver execution errors to the client.
//
// Args:
// stmt: The statement to execute.
// res: Used to produce query results.
// pinfo: The values to use for the statement's placeholders. If nil is passed,
// 	 then the statement cannot have any placeholder.
func (ex *connExecutor) execStmt(
	ctx context.Context, stmt Statement, res RestrictedCommandResult, pinfo *tree.PlaceholderInfo,
) (fsm.Event, fsm.EventPayload, error) {
	if log.V(2) || logStatementsExecuteEnabled.Get(&ex.server.cfg.Settings.SV) ||
		log.HasSpanOrEvent(ctx) {
		log.VEventf(ctx, 2, "executing: %s in state: %s", stmt, ex.machine.CurState())
	}

	// Run observer statements in a separate code path; their execution does not
	// depend on the current transaction state.
	if _, ok := stmt.AST.(tree.ObserverStatement); ok {
		err := ex.runObserverStatement(ctx, stmt, res)
		return nil, nil, err
	}

	queryID := ex.generateID()
	stmt.queryID = queryID

	// Dispatch the statement for execution based on the current state.
	var ev fsm.Event
	var payload fsm.EventPayload
	var err error

	switch ex.machine.CurState().(type) {
	case stateNoTxn:
		ev, payload = ex.execStmtInNoTxnState(ctx, stmt)
	case stateOpen:
		if ex.server.cfg.Settings.IsCPUProfiling() {
			labels := pprof.Labels(
				"stmt.tag", stmt.AST.StatementTag(),
				"stmt.anonymized", stmt.AnonymizedStr,
			)
			pprof.Do(ctx, labels, func(ctx context.Context) {
				ev, payload, err = ex.execStmtInOpenState(ctx, stmt, pinfo, res)
			})
		} else {
			ev, payload, err = ex.execStmtInOpenState(ctx, stmt, pinfo, res)
		}
		switch ev.(type) {
		case eventNonRetriableErr:
			ex.recordFailure()
		}
	case stateAborted:
		ev, payload = ex.execStmtInAbortedState(ctx, stmt, res)
	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(stmt, res)
	default:
		panic(fmt.Sprintf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	return ev, payload, err
}

func (ex *connExecutor) recordFailure() {
	ex.metrics.EngineMetrics.FailureCount.Inc(1)
}

// execStmtInOpenState executes one statement in the context of the session's
// current transaction.
// It handles statements that affect the transaction state (BEGIN, COMMIT)
// directly and delegates everything else to the execution engines.
// Results and query execution errors are written to res.
//
// This method also handles "auto commit" - committing of implicit transactions.
//
// If an error is returned, the connection is supposed to be consider done.
// Query execution errors are not returned explicitly; they're incorporated in
// the returned Event.
//
// The returned event can be nil if no state transition is required.
func (ex *connExecutor) execStmtInOpenState(
	ctx context.Context, stmt Statement, pinfo *tree.PlaceholderInfo, res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {

	ex.incrementStmtCounter(stmt)
	os := ex.machine.CurState().(stateOpen)
	if ex.state.mu.txn.IsolationLevel() == util.ReadCommittedIsolation {
		ex.state.mu.txn.SetTimeForRC(ex.server.cfg.Clock.Now())
	}
	var timeoutTicker *time.Timer
	queryTimedOut := false
	doneAfterFunc := make(chan struct{}, 1)

	// Canceling a query cancels its transaction's context so we take a reference
	// to the cancelation function here.
	unregisterFn := ex.addActiveQuery(stmt.queryID, stmt.AST, ex.state.cancel)

	// queryDone is a cleanup function dealing with unregistering a query.
	// It also deals with overwriting res.Error to a more user-friendly message in
	// case of query cancelation. res can be nil to opt out of this.
	queryDone := func(ctx context.Context, res RestrictedCommandResult) {
		if timeoutTicker != nil {
			if !timeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// queryTimedOut.
				<-doneAfterFunc
			}
		}
		unregisterFn()

		// Detect context cancelation and overwrite whatever error might have been
		// set on the result before. The idea is that once the query's context is
		// canceled, all sorts of actors can detect the cancelation and set all
		// sorts of errors on the result. Rather than trying to impose discipline
		// in that jungle, we just overwrite them all here with an error that's
		// nicer to look at for the client.
		if res != nil && ctx.Err() != nil && res.Err() != nil {
			if queryTimedOut {
				res.SetError(sqlbase.QueryTimeoutError)
			} else {
				res.SetError(sqlbase.QueryCanceledError)
			}
		}
	}
	// Generally we want to unregister after the auto-commit below. However, in
	// case we'll execute the statement through the parallel execution queue,
	// we'll pass the responsibility for unregistering to the queue.
	defer func() {
		if queryDone != nil {
			queryDone(ctx, res)
		}
	}()

	if ex.sessionData.StmtTimeout > 0 {
		timeoutTicker = time.AfterFunc(
			ex.sessionData.StmtTimeout-timeutil.Since(ex.phaseTimes[sessionQueryReceived]),
			func() {
				ex.cancelQuery(stmt.queryID)
				queryTimedOut = true
				doneAfterFunc <- struct{}{}
			})
	}

	defer func() {
		if filter := ex.server.cfg.TestingKnobs.StatementFilter; retErr == nil && filter != nil {
			var execErr error
			if perr, ok := retPayload.(payloadWithError); ok {
				execErr = perr.errorCause()
			}
			filter(ctx, stmt.String(), execErr)
		}

		// Do the auto-commit, if necessary.
		if retEv != nil || retErr != nil {
			return
		}
		if os.ImplicitTxn.Get() {
			retEv, retPayload = ex.handleAutoCommit(ctx, stmt.AST)
			if retEvent, ok := retEv.(eventRetriableErr); ok {
				if retEvent.CanAutoRetry.Get() {
					reast, _ := parser.ParseOne(stmt.SQL, ex.getCaseSensitive())
					DeepCopyAST(reast, stmt)
				}
			}
			return
		}
	}()

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, stmt.AST)
		if event, ok := ev.(eventRetriableErr); ok {
			if event.CanAutoRetry.Get() {
				reast, _ := parser.ParseOne(stmt.SQL, ex.getCaseSensitive())
				DeepCopyAST(reast, stmt)
			}
		}
		return ev, payload, nil
	}

	var discardRows bool
	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		// BEGIN is always an error when in the Open state. It's legitimate only in
		// the NoTxn state.
		return makeErrEvent(errTransactionInProgress)

	case *tree.CommitTransaction, *tree.BeginCommit:
		// for execute prepared stmt, We need to determine whether auto commit is enabled.
		// if auto commit enabled, execute ddl need BeginCommit stmt.
		ok, err := ex.checkPrepareDDL(s)
		if err != nil {
			return makeErrEvent(err)
		}
		if !ok {
			return nil, nil, nil
		}

		// CommitTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.commitSQLTransaction(ctx, stmt.AST)
		return ev, payload, nil

	case *tree.RollbackTransaction:
		// RollbackTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.rollbackSQLTransaction(ctx)
		return ev, payload, nil

	case *tree.Savepoint:
		return ex.execSavepointInOpenState(ctx, s, res)

	case *tree.ReleaseSavepoint:
		ev, payload := ex.execRelease(ctx, s, res)
		return ev, payload, nil

	case *tree.RollbackToSavepoint:
		ev, payload := ex.execRollbackToSavepointInOpenState(ctx, s, res)
		return ev, payload, nil

	case *tree.Prepare:
		// This is handling the SQL statement "PREPARE". See execPrepare for
		// handling of the protocol-level command for preparing statements.
		name := s.Name.String()
		if _, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]; ok {
			err := pgerror.NewErrorf(
				pgcode.DuplicatePreparedStatement,
				"prepared statement %q already exists", name,
			)
			return makeErrEvent(err)
		}
		var typeHints tree.PlaceholderTypes
		if len(s.Types) > 0 {
			if len(s.Types) > stmt.NumPlaceholders {
				err := pgerror.NewErrorf(pgcode.Syntax, "too many types provided")
				return makeErrEvent(err)
			}
			typeHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
			for i, t := range s.Types {
				typeHints[i] = coltypes.CastTargetToDatumType(t)
			}
		}
		if _, err := ex.addPreparedStmt(
			ctx, name,
			Statement{
				Statement: parser.Statement{
					// We need the SQL string just for the part that comes after
					// "PREPARE ... AS",
					// TODO(radu): it would be nice if the parser would figure out this
					// string and store it in tree.Prepare.
					SQL:             tree.AsStringWithFlags(s.Statement, tree.FmtParsable),
					AST:             s.Statement,
					NumPlaceholders: stmt.NumPlaceholders,
				},
			},
			typeHints,
		); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil

	case *tree.Execute:
		// Replace the `EXECUTE foo` statement with the prepared statement, and
		// continue execution below.
		name := s.Name.String()
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
		if !ok {
			err := pgerror.NewErrorf(
				pgcode.InvalidSQLStatementName,
				"prepared statement %q does not exist", name,
			)
			return makeErrEvent(err)
		}
		var err error
		pinfo, err = fillInPlaceholders(ps, name, s.Params, ex.sessionData.SearchPath)
		if err != nil {
			return makeErrEvent(err)
		}

		stmt.Statement = ps.Statement
		stmt.Prepared = ps
		stmt.ExpectedTypes = ps.Columns
		stmt.AnonymizedStr = ps.AnonymizedStr
		ctx = withStatement(ctx, stmt.AST)
		res.ResetStmtType(ps.AST)

		discardRows = s.DiscardRows
	}

	// For regular statements (the ones that get to this point), we don't return
	// any event unless an an error happens.

	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)

	p.noticeSender = &gatedNoticeSender{noticeSender: res, sv: &ex.server.cfg.Settings.SV}
	// For Cursor
	// init ex.planner CurrentSessionID,这里如果不赋值，事务提交时候不能拿到sessionId
	ex.planner.extendedEvalCtx.CurrentSessionID = ex.sessionID
	// cursor transaction map
	p.extendedEvalCtx.curTxnMap = ex.planner.extendedEvalCtx.curTxnMap

	if os.ImplicitTxn.Get() {
		asOfTs, err := p.asOfSnapshot(ctx, stmt.AST)
		if err != nil {
			return makeErrEvent(err)
		}
		if asOfTs != nil {
			p.semaCtx.AsOfTimestamp = asOfTs
			p.extendedEvalCtx.SetTxnTimestamp(asOfTs.GoTime())
			ex.state.setHistoricalTimestamp(ctx, *asOfTs)
		}
	} else {
		// If we're in an explicit txn, we allow AOST but only if it matches with
		// the transaction's timestamp. This is useful for running AOST statements
		// using the InternalExecutor inside an external transaction; one might want
		// to do that to force p.avoidCachedDescriptors to be set below.
		ts, err := p.asOfSnapshot(ctx, stmt.AST)
		if err != nil {
			return makeErrEvent(err)
		}
		if ts != nil {
			if origTs := ex.state.getReadTimestamp(); *ts != origTs {
				return makeErrEvent(errors.Errorf("inconsistent AS OF SYSTEM TIME timestamp. Expected: %s. "+
					"Generally AS OF SYSTEM TIME cannot be used inside a transaction.",
					origTs))
			}
			p.semaCtx.AsOfTimestamp = ts
		}
	}

	prevSteppingMode := ex.state.mu.txn.ConfigureStepping(ctx, client.SteppingEnabled)
	defer func() { _ = ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode) }()

	if err := ex.state.mu.txn.Step(ctx); err != nil {
		return makeErrEvent(err)
	}

	if err := p.semaCtx.Placeholders.Assign(pinfo, stmt.NumPlaceholders); err != nil {
		return makeErrEvent(err)
	}
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	ex.phaseTimes[plannerStartExecStmt] = timeutil.Now()
	p.stmt = &stmt
	p.discardRows = discardRows

	// TODO(andrei): Ideally we'd like to fork off a context for each individual
	// statement. But the heartbeat loop in TxnCoordSender currently assumes that
	// the context of the first operation in a txn batch lasts at least as long as
	// the transaction itself. Once that sender is able to distinguish between
	// statement and transaction contexts, we should move to per-statement
	// contexts.
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)

	p.autoCommit = os.ImplicitTxn.Get() && !ex.server.cfg.TestingKnobs.DisableAutoCommit
	if err := ex.dispatchToExecutionEngine(ctx, p, res); err != nil {
		return nil, nil, err
	}
	if err := res.Err(); err != nil {
		return makeErrEvent(err)
	}

	ex.phaseTimes[plannerEndExecStmt] = timeutil.Now()
	txn := ex.state.mu.txn
	if !os.ImplicitTxn.Get() && txn.IsSerializablePushAndRefreshNotPossible() {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		if canAutoRetry {
			ev := eventRetriableErr{
				IsCommit:     fsm.FromBool(isCommit(stmt.AST)),
				CanAutoRetry: fsm.FromBool(canAutoRetry),
			}
			txn.ManualRestart(ctx, ex.server.cfg.Clock.Now())
			payload := eventRetriableErrPayload{
				err: roachpb.NewTransactionRetryWithProtoRefreshError(
					"serializable transaction timestamp pushed (detected by connExecutor)",
					txn.ID(),
					// No updated transaction required; we've already manually updated our
					// client.Txn.
					roachpb.Transaction{},
				),
				rewCap: rc,
			}
			return ev, payload, nil
		}
	}
	// No event was generated.
	return nil, nil, nil
}

// checkTableTwoVersionInvariant checks whether any new table schema being
// modified written at a version V has only valid leases at version = V - 1.
// A transaction retry error is returned whenever the invariant is violated.
// Before returning the retry error the current transaction is
// rolled-back and the function waits until there are only outstanding
// leases on the current version. This affords the retry to succeed in the
// event that there are no other schema changes simultaneously contending with
// this txn.
//
// checkTableTwoVersionInvariant blocks until it's legal for the modified
// table descriptors (if any) to be committed.
// Reminder: a descriptor version v can only be written at a timestamp
// that's not covered by a lease on version v-2. So, if the current
// txn wants to write some updated descriptors, it needs
// to wait until all incompatible leases are revoked or expire. If
// incompatible leases exist, we'll block waiting for these leases to
// go away. Then, the transaction is restarted by generating a retriable error.
// Note that we're relying on the fact that the number of conflicting
// leases will only go down over time: no new conflicting leases can be
// created as of the time of this call because v-2 can't be leased once
// v-1 exists.
func (ex *connExecutor) checkTableTwoVersionInvariant(ctx context.Context) error {
	tables := ex.extraTxnState.tables.getTablesWithNewVersion()
	if tables == nil {
		return nil
	}
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		panic("transaction has already committed")
	}

	// Release leases here for two reasons:
	// 1. If there are existing leases at version V-2 for a descriptor
	// being modified to version V being held the wait loop below that
	// waits on a cluster wide release of old version leases will hang
	// until these leases expire.
	// 2. Once this transaction commits, the schema changers run and
	// increment the version of the modified descriptors. If one of the
	// descriptors being modified has a lease being held the schema
	// changers will stall until the leases expire.
	//
	// The above two cases can be satified by releasing leases for both
	// cases explicitly, but we prefer to call it here and kill two birds
	// with one stone.
	//
	// It is safe to release leases even though the transaction hasn't yet
	// committed only because the transaction timestamp has been fixed using
	// CommitTimestamp().
	//
	// releaseLeases can fail to release a lease if the server is shutting
	// down. This is okay because it will result in the two cases mentioned
	// above simply hanging until the expiration time for the leases.
	ex.extraTxnState.tables.releaseTableLeases(ex.Ctx(), tables)

	count, err := CountLeases(ctx, ex.server.cfg.InternalExecutor, tables, txn.Serialize().WriteTimestamp)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	// Restart the transaction so that it is able to replay itself at a newer timestamp
	// with the hope that the next time around there will be leases only at the current
	// version.
	retryErr := roachpb.NewTransactionRetryWithProtoRefreshError(
		fmt.Sprintf(
			`cannot publish new versions for tables: %v, old versions still in use`,
			tables),
		txn.ID(),
		*txn.Serialize(),
	)
	// We cleanup the transaction and create a new transaction after
	// waiting for the invariant to be satisfied because the wait time
	// might be extensive and intents can block out leases being created
	// on a descriptor.
	//
	// TODO(vivek): Change this to restart a txn while fixing #20526 . All the
	// table descriptor intents can be laid down here after the invariant
	// has been checked.
	userPriority := txn.UserPriority()
	// We cleanup the transaction and create a new transaction wait time
	// might be extensive and so we'd better get rid of all the intents.
	txn.CleanupOnError(ctx, retryErr)
	ex.extraTxnState.tables.releaseLeases(ctx)
	// Release the rest of our leases on unmodified tables so we don't hold up
	// schema changes there and potentially create a deadlock.
	ex.extraTxnState.tables.releaseLeases(ctx)

	// Wait until all older version leases have been released or expired.
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		// Use the current clock time.
		now := ex.server.cfg.Clock.Now()
		count, err := CountLeases(ctx, ex.server.cfg.InternalExecutor, tables, now)
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		if ex.server.cfg.SchemaChangerTestingKnobs.TwoVersionLeaseViolation != nil {
			ex.server.cfg.SchemaChangerTestingKnobs.TwoVersionLeaseViolation()
		}
	}

	// Create a new transaction to retry with a higher timestamp than the
	// timestamps used in the retry loop above.
	ex.state.mu.txn = client.NewTxnWithSteppingEnabled(ctx, ex.transitionCtx.db, ex.transitionCtx.nodeID, client.RootTxn)
	if err := ex.state.mu.txn.SetUserPriority(userPriority); err != nil {
		return err
	}
	return retryErr
}

// commitSQLTransaction executes a commit after the execution of a stmt,
// which can be any statement when executing a statement with an implicit
// transaction, or a COMMIT or RELEASE SAVEPOINT statement when using
// an explicit transaction.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	err := ex.commitSQLTransactionInternal(ctx, stmt)
	if err != nil {
		return ex.makeErrEvent(err, stmt)
	}

	if len(ex.planner.ExtendedEvalContext().curTxnMap) != 0 {
		// CommitCursors
		if err := ex.planner.CommitCursors(); err != nil {
			return ex.makeErrEvent(err, stmt)
		}
		// CommitVariables
		if err := ex.planner.CommitVariables(); err != nil {
			return ex.makeErrEvent(err, stmt)
		}
	}
	return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
}

func (ex *connExecutor) commitSQLTransactionInternal(
	ctx context.Context, stmt tree.Statement,
) error {
	if err := validatePrimaryKeys(&ex.extraTxnState.tables); err != nil {
		return err
	}

	if err := ex.checkTableTwoVersionInvariant(ctx); err != nil {
		return err
	}

	if err := ex.state.mu.txn.Commit(ctx); err != nil {
		return err
	}
	// Now that we've committed, if we modified any table we need to make sure
	// to release the leases for them so that the schema change can proceed and
	// we don't block the client.
	if tables := ex.extraTxnState.tables.getTablesWithNewVersion(); tables != nil {
		ex.extraTxnState.tables.releaseLeases(ctx)
	}
	return nil
}

// validatePrimaryKeys verifies that all tables modified in the transaction have
// an enabled primary key after potentially undergoing DROP PRIMARY KEY, which
// is required to be followed by ADD PRIMARY KEY.
func validatePrimaryKeys(tc *TableCollection) error {
	tables := tc.GetUncommittedTables()
	for _, table := range tables {
		if !table.HasPrimaryKey() {
			return unimplemented.NewWithIssuef(48026,
				"primary key of table %s dropped without subsequent addition of new primary key",
				table.Name,
			)
		}
	}
	return nil
}

// rollbackSQLTransaction executes a ROLLBACK statement: the KV transaction is
// rolled-back and an event is produced.
func (ex *connExecutor) rollbackSQLTransaction(ctx context.Context) (fsm.Event, fsm.EventPayload) {
	if err := ex.state.mu.txn.Rollback(ctx); err != nil {
		log.Warningf(ctx, "txn rollback failed: %s", err)
	}

	if len(ex.planner.ExtendedEvalContext().curTxnMap) != 0 {
		// RollBackCursors
		if err := ex.planner.RollBackCursors(); err != nil {
			log.Warningf(ctx, "txn rollback failed: %s", err)
		}
		// RollBackVariables
		if err := ex.planner.RollBackVariables(); err != nil {
			log.Warningf(ctx, "txn rollback failed: %s", err)
		}
	}
	// We're done with this txn.
	return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
}

func enhanceErrWithCorrelation(err error, isCorrelated bool) {
	if err == nil || !isCorrelated {
		return
	}

	// If the query was found to be correlated by the new-gen
	// optimizer, but the optimizer decided to give up (e.g. because
	// of some feature it does not support), in most cases the
	// heuristic planner will choke on the correlation with an
	// unhelpful "table/column not defined" error.
	//
	// ("In most cases" because the heuristic planner does support
	// *some* correlation, specifically that of SRFs in projections.)
	//
	// To help the user understand what is going on, we enhance these
	// error message here when correlation has been found.
	//
	// We cannot be more assertive/definite in the text of the hint
	// (e.g. by replacing the error entirely by "correlated queries are
	// not supported") because perhaps there was an actual mistake in
	// the query in addition to the unsupported correlation, and we also
	// want to give a chance to the user to fix mistakes.
	if pqErr, ok := err.(*pgerror.Error); ok {
		if pqErr.Code == pgcode.UndefinedColumn ||
			pqErr.Code == pgcode.UndefinedTable {
			_ = pqErr.SetHintf("some correlated subqueries are not supported yet.")
		}
	}
}

// dispatchToExecutionEngine executes the statement, writes the result to res
// and returns an event for the connection's state machine.
//
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned; it is
// expected that the caller will inspect res and react to query errors by
// producing an appropriate state machine event.
func (ex *connExecutor) dispatchToExecutionEngine(
	ctx context.Context, planner *planner, res RestrictedCommandResult,
) error {
	stmt := planner.stmt
	ex.sessionTracing.TracePlanStart(ctx, stmt.AST.StatementTag())
	planner.statsCollector.PhaseTimes()[plannerStartLogicalPlan] = timeutil.Now()

	// Prepare the plan. Note, the error is processed below. Everything
	// between here and there needs to happen even if there's an error.
	err := ex.makeExecPlan(ctx, planner)
	// We'll be closing the plan manually below after execution; this
	// defer is a catch-all in case some other return path is taken.
	defer planner.curPlan.close(ctx)

	// Certain statements want their results to go to the client
	// directly. Configure this here.
	if planner.curPlan.avoidBuffering {
		res.DisableBuffering()
	}

	// Ensure that the plan is collected just before closing.
	if sampleLogicalPlans.Get(&ex.appStats.st.SV) {
		planner.curPlan.maybeSavePlan = func(ctx context.Context) *roachpb.ExplainTreePlanNode {
			return ex.maybeSavePlan(ctx, planner)
		}
	}

	defer func() { planner.maybeLogStatement(ctx, "exec", res.RowsAffected(), res.Err()) }()

	planner.statsCollector.PhaseTimes()[plannerEndLogicalPlan] = timeutil.Now()
	ex.sessionTracing.TracePlanEnd(ctx, err)

	if err == nil && !ex.checkReplication() {
		err = errors.New("PLEASE SET \"set session replicate_tables_in_sync='on';\" , WHEN OPERATING REPLICATION TABLE")
	}
	// Finally, process the planning error from above.
	if err != nil {
		res.SetError(err)
		return nil
	}

	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planner.curPlan.main.planColumns()
	}
	if err := ex.initStatementResult(ctx, res, stmt, cols); err != nil {
		res.SetError(err)
		return nil
	}

	ex.sessionTracing.TracePlanCheckStart(ctx)
	distributePlan := false
	distributePlan = shouldDistributePlan(
		ctx, ex.sessionData.DistSQLMode, ex.server.cfg.DistSQLPlanner, planner.curPlan.main.planNode)
	ex.sessionTracing.TracePlanCheckEnd(ctx, nil, distributePlan)

	if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), false /* isParallel */)
	}

	planner.statsCollector.PhaseTimes()[plannerStartExecStmt] = timeutil.Now()

	ex.mu.Lock()
	queryMeta, ok := ex.mu.ActiveQueries[stmt.queryID]
	if !ok {
		ex.mu.Unlock()
		panic(fmt.Sprintf("query %d not in registry", stmt.queryID))
	}
	queryMeta.phase = executing
	queryMeta.isDistributed = distributePlan
	ex.mu.Unlock()

	// We need to set the "exec done" flag early because
	// curPlan.close(), which will need to observe it, may be closed
	// during execution (distsqlrun.PlanAndRun).
	//
	// TODO(knz): This is a mis-design. Andrei says "it's OK if
	// execution closes the plan" but it transfers responsibility to
	// run any "finalizers" on the plan (including plan sampling for
	// stats) to the execution engine. That's a lot of responsibility
	// to transfer! It would be better if this responsibility remained
	// around here.
	planner.curPlan.flags.Set(planFlagExecDone)

	if distributePlan {
		planner.curPlan.flags.Set(planFlagDistributed)
	} else {
		planner.curPlan.flags.Set(planFlagDistSQLLocal)
	}
	ex.sessionTracing.TraceExecStart(ctx, "distributed")

	err = ex.execWithDistSQLEngine(ctx, planner, stmt.AST.StatementType(), res, distributePlan)
	ex.sessionTracing.TraceExecEnd(ctx, res.Err(), res.RowsAffected())
	planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()

	// Record the statement summary. This also closes the plan if the
	// plan has not been closed earlier.
	ex.recordStatementSummary(
		ctx, planner,
		ex.extraTxnState.autoRetryCounter, res.RowsAffected(), res.Err(),
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
	}

	return err
}

func (ex *connExecutor) checkReplication() bool {
	if ex.planner.stmt != nil {
		if ex.planner.tableTrigger != nil &&
			ex.planner.tableTrigger.TableDesc() != nil &&
			ex.planner.tableTrigger.TableDesc().ReplicationTable {
			if !ex.sessionData.ReplicaionSync {
				switch ex.planner.stmt.AST.(type) {
				case *tree.Select:
					return true
				case *tree.Delete, *tree.Insert, *tree.Update, *tree.AlterTable, *tree.Truncate:
					return false
				default:
					return true
				}
			}
		}
	}
	return true
}

// makeExecPlan creates an execution plan and populates planner.curPlan, using
// either the optimizer or the heuristic planner.
func (ex *connExecutor) makeExecPlan(ctx context.Context, planner *planner) error {
	stmt := planner.stmt
	// Initialize planner.curPlan.AST early; it might be used by maybeLogStatement
	// in error cases.
	planner.curPlan = planTop{AST: stmt.AST}

	var err error
	var isCorrelated bool

	_, ok := stmt.AST.(*tree.CreateView)
	if ok {
		// Avoid taking table leases when we're creating a view.
		planner.avoidCachedDescriptors = true
	}

	if err = planner.makeOptimizerPlan(ctx); err == nil {
		flags := planner.curPlan.flags

		// TODO(knz): Remove this accounting if/when savepoint rollbacks
		// support rolling back over DDL.
		if flags.IsSet(planFlagIsDDL) {
			ex.extraTxnState.numDDL++
		}
		return nil
	}

	if !canFallbackFromOpt1(err, stmt) {
		return err
	}
	planner.curPlan.flags.Set(planFlagOptFallback)

	log.VEventf(ctx, 1, "optimizer plan failed: %v", err)

	optFlags := planner.curPlan.flags
	err = planner.makePlan(ctx)
	planner.curPlan.flags |= optFlags
	enhanceErrWithCorrelation(err, isCorrelated)
	return err
}

// saveLogicalPlanDescription returns whether we should save this as a sample logical plan
// for its corresponding fingerprint. We use `logicalPlanCollectionPeriod`
// to assess how frequently to sample logical plans.
func (ex *connExecutor) saveLogicalPlanDescription(
	stmt *Statement, useDistSQL bool, optimizerUsed bool, err error,
) bool {
	stats := ex.appStats.getStatsForStmt(
		stmt, useDistSQL, optimizerUsed, err, false /* createIfNonexistent */)
	if stats == nil {
		// Save logical plan the first time we see new statement fingerprint.
		return true
	}
	now := timeutil.Now()
	period := logicalPlanCollectionPeriod.Get(&ex.appStats.st.SV)
	stats.Lock()
	defer stats.Unlock()
	timeLastSampled := stats.data.SensitiveInfo.MostRecentPlanTimestamp
	return now.Sub(timeLastSampled) >= period
}

// canFallbackFromOpt returns whether we can fallback on the heuristic planner
// when the optimizer hits an error.
func canFallbackFromOpt(err error, optMode sessiondata.OptimizerMode, stmt *Statement) bool {
	_, ok := err.(optbuilder.TryOldPath)
	if ok {
		return true
	}
	pgerr, ok := err.(*pgerror.Error)
	if !ok || pgerr.Code != pgcode.FeatureNotSupported {
		// We only fallback on "feature not supported" errors.
		return false
	}

	if optMode == sessiondata.OptimizerAlways {
		// In Always mode we never fallback, with one exception: SET commands (or
		// else we can't switch to another mode).
		_, isSetVar := stmt.AST.(*tree.SetVar)
		return isSetVar
	}

	// If the statement is EXPLAIN (OPT), then don't fallback (we want to return
	// the error, not show a plan from the heuristic planner).
	// TODO(radu): this is hacky and doesn't handle an EXPLAIN (OPT) inside
	// a larger query.
	if e, ok := stmt.AST.(*tree.Explain); ok {
		if opts, err := e.ParseOptions(); err == nil && opts.Mode == tree.ExplainOpt {
			return false
		}
	}

	// Never fall back on PREPARE AS OPT PLAN.
	if _, ok := stmt.AST.(*tree.CannedOptPlan); ok {
		return false
	}
	return true
}

func canFallbackFromOpt1(err error, stmt *Statement) bool {
	_, ok := err.(optbuilder.TryOldPath)
	if ok {
		return true
	}
	if strings.Contains(err.Error(), `ENUM value not valid`) ||
		strings.Contains(err.Error(), `SET value is invalid`) {
		return false
	}
	if strings.Contains(err.Error(), `schema change statement cannot follow a statement that has written in the same transaction`) {
		return false
	}
	if strings.Contains(err.Error(), `cannot change the user priority of a running transaction`) {
		return false
	}
	pgerr, ok := err.(*pgerror.Error)
	if !ok {
		return true
	} else if ok && pgerr.Code == pgcode.CCLRequired {
		// We only fallback on "feature not supported" errors.
		return true
	} else if pgerr.Code != pgcode.FeatureNotSupported {
		return false
	}

	// If the statement is EXPLAIN (OPT), then don't fallback (we want to return
	// the error, not show a plan from the heuristic planner).
	// TODO(radu): this is hacky and doesn't handle an EXPLAIN (OPT) inside
	// a larger query.
	if e, ok := stmt.AST.(*tree.Explain); ok {
		if opts, err := e.ParseOptions(); err == nil && opts.Mode == tree.ExplainOpt {
			return false
		}
	}

	// Never fall back on PREPARE AS OPT PLAN.
	if _, ok := stmt.AST.(*tree.CannedOptPlan); ok {
		return false
	}
	return true
}

// execWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned.
func (ex *connExecutor) execWithDistSQLEngine(
	ctx context.Context,
	planner *planner,
	stmtType tree.StatementType,
	res RestrictedCommandResult,
	distribute bool,
) error {
	var recv *DistSQLReceiver
	if DistSQLRecOpt.Get(&planner.execCfg.Settings.SV) && !planner.discardRows && stmtType == tree.Rows {
		recv = MakeDistSQLReceiverOpt(
			ctx, res, stmtType,
			ex.server.cfg.RangeDescriptorCache, ex.server.cfg.LeaseHolderCache,
			planner.txn,
			func(ts hlc.Timestamp) {
				_ = ex.server.cfg.Clock.Update(ts)
			},
			&ex.sessionTracing,
		)
	} else {
		recv = MakeDistSQLReceiver(
			ctx, res, stmtType,
			ex.server.cfg.RangeDescriptorCache, ex.server.cfg.LeaseHolderCache,
			planner.txn,
			func(ts hlc.Timestamp) {
				_ = ex.server.cfg.Clock.Update(ts)
			},
			&ex.sessionTracing,
		)
	}

	defer recv.Release()

	evalCtx := planner.ExtendedEvalContext()
	var planCtx *PlanningCtx
	if distribute {
		planCtx = ex.server.cfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, planner.txn)
	} else {
		planCtx = ex.server.cfg.DistSQLPlanner.newLocalPlanningCtx(ctx, evalCtx)
	}
	planCtx.isLocal = !distribute
	planCtx.planner = planner
	planCtx.stmtType = recv.stmtType

	if len(planner.curPlan.subqueryPlans) != 0 {
		var evalCtx extendedEvalContext
		ex.initEvalCtx(ctx, &evalCtx, planner)
		evalCtxFactory := func() *extendedEvalContext {
			ex.resetEvalCtx(&evalCtx, planner.txn, planner.ExtendedEvalContext().StmtTimestamp)
			evalCtx.Placeholders = &planner.semaCtx.Placeholders
			return &evalCtx
		}
		if !ex.server.cfg.DistSQLPlanner.PlanAndRunSubqueries(
			ctx, planner, evalCtxFactory, planner.curPlan.subqueryPlans, recv, distribute,
		) {
			return recv.commErr
		}
	}
	if len(planner.curPlan.planComponents.subqueryPlans) != 0 {
		var evalCtx extendedEvalContext
		ex.initEvalCtx(ctx, &evalCtx, planner)
		evalCtxFactory := func() *extendedEvalContext {
			ex.resetEvalCtx(&evalCtx, planner.txn, planner.ExtendedEvalContext().StmtTimestamp)
			evalCtx.Placeholders = &planner.semaCtx.Placeholders
			return &evalCtx
		}
		if !ex.server.cfg.DistSQLPlanner.PlanAndRunSubqueries(
			ctx, planner, evalCtxFactory, planner.curPlan.planComponents.subqueryPlans, recv, distribute,
		) {
			return recv.commErr
		}
	}
	recv.discardRows = planner.discardRows
	// We pass in whether or not we wanted to distribute this plan, which tells
	// the planner whether or not to plan remote table readers.
	ExecInUDR = false
	planner.semaCtx.SetInUDR(false)
	ex.server.cfg.DistSQLPlanner.PlanAndRun(
		ctx, evalCtx, planCtx, planner.txn, planner.curPlan.main, recv)
	return recv.commErr
}

// beginTransactionTimestampsAndReadMode computes the timestamps and
// ReadWriteMode to be used for the associated transaction state based on the
// values of the statement's Modes. Note that this method may reset the
// connExecutor's planner in order to compute the timestamp for the AsOf clause
// if it exists. The timestamps correspond to the timestamps passed to
// makeEventTxnStartPayload; txnSQLTimestamp propagates to become the
// TxnTimestamp while historicalTimestamp populated with a non-nil value only
// if the BeginTransaction statement has a non-nil AsOf clause expression. A
// non-nil historicalTimestamp implies a ReadOnly rwMode.
func (ex *connExecutor) beginTransactionTimestampsAndReadMode(
	ctx context.Context, s *tree.BeginTransaction,
) (
	isolationLevel tree.IsolationLevel,
	rwMode tree.ReadWriteMode,
	txnSQLTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	debugName tree.Name,
	supDDL tree.SUPDDL,
	err error,
) {
	now := ex.server.cfg.Clock.Now()
	debugName = s.Modes.Name
	supDDL = s.Modes.TxnDDL
	if s.Modes.Isolation == tree.UnspecifiedIsolation {
		if level := tree.IsolationLevel(ex.sessionData.DefaultIsolationLevel); level == tree.UnspecifiedIsolation {
			isolationLevel = tree.DefaultIsolation
		} else {
			isolationLevel = level
		}
	} else {
		isolationLevel = s.Modes.Isolation
	}
	if s.Modes.AsOf.Expr == nil {
		rwMode = ex.readWriteModeWithSessionDefault(s.Modes.ReadWriteMode)
		return isolationLevel, rwMode, now.GoTime(), nil, debugName, supDDL, nil
	}
	p := &ex.planner
	ex.resetPlanner(ctx, p, nil /* txn */, now.GoTime())
	ts, err := p.EvalAsOfTimestamp(s.Modes.AsOf)
	if err != nil {
		return 0, 0, time.Time{}, nil, "", tree.NotSupportDDL, err
	}
	// NB: This check should never return an error because the parser should
	// disallow the creation of a TransactionModes struct which both has an
	// AOST clause and is ReadWrite but performing a check decouples this code
	// from that and hopefully adds clarity that the returning of ReadOnly with
	// a historical timestamp is intended.
	if s.Modes.ReadWriteMode == tree.ReadWrite {
		return 0, 0, time.Time{}, nil, "", 0, tree.ErrAsOfSpecifiedWithReadWrite
	}
	return tree.DefaultIsolation, tree.ReadOnly, ts.GoTime(), &ts, debugName, supDDL, nil
}

// execStmtInNoTxnState "executes" a statement when no transaction is in scope.
// For anything but BEGIN, this method doesn't actually execute the statement;
// it just returns an Event that will generate a transaction. The statement will
// then be executed again, but this time in the Open state (implicit txn).
//
// Note that eventTxnStart, which is generally returned by this method, causes
// the state to change and previous results to be flushed, but for implicit txns
// the cursor is not advanced. This means that the statement will run again in
// stateOpen, at each point its results will also be flushed.
func (ex *connExecutor) execStmtInNoTxnState(
	ctx context.Context, stmt Statement,
) (fsm.Event, fsm.EventPayload) {
	switch s := stmt.AST.(type) {
	case *tree.BeginCommit, *tree.BeginTransaction:
		// for execute prepared stmt, We need to determine whether auto commit is enabled.
		// if auto commit enabled, execute ddl need BeginCommit stmt.
		ddl, err := ex.checkPrepareDDL(s)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		if !ddl {
			return nil, nil
		}

		var begin *tree.BeginTransaction
		var ok bool
		if begin, ok = s.(*tree.BeginTransaction); !ok {
			begin = &tree.BeginTransaction{}
		}

		ex.incrementStmtCounter(stmt)
		pri, err := priorityToProto(begin.Modes.UserPriority)
		if err != nil {
			return ex.makeErrEvent(err, begin)
		}
		isolationLevel, mode, sqlTs, historicalTs, debugName, supDDL, err := ex.beginTransactionTimestampsAndReadMode(ctx, begin)
		if err != nil {
			return ex.makeErrEvent(err, begin)
		}
		return eventTxnStart{ImplicitTxn: fsm.False},
			makeEventTxnStartPayload(
				isolationLevel,
				pri, mode, sqlTs,
				historicalTs,
				debugName,
				supDDL,
				ex.transitionCtx)
	case *tree.CommitTransaction, *tree.ReleaseSavepoint,
		*tree.RollbackTransaction, *tree.SetTransaction, *tree.Savepoint:
		return ex.makeErrEvent(errNoTransactionInProgress, stmt.AST)
	default:
		switch t := stmt.AST.(type) {
		case *tree.AlterTable:
			switch t.Cmds[0].(type) {
			case *tree.AlterTableSetNotNull:
				t.IsAuto = true
			}
		}
		var level = tree.IsolationLevel(ex.sessionData.DefaultIsolationLevel)
		if level == tree.UnspecifiedIsolation {
			level = tree.DefaultIsolation
		}
		mode := tree.ReadWrite
		if ex.sessionData.DefaultReadOnly {
			mode = tree.ReadOnly
		}
		// NB: Implicit transactions are created without a historical timestamp even
		// though the statement might contain an AOST clause. In these cases the
		// clause is evaluated and applied execStmtInOpenState.
		return eventTxnStart{ImplicitTxn: fsm.True},
			makeEventTxnStartPayload(
				level,
				roachpb.NormalUserPriority,
				mode,
				ex.server.cfg.Clock.PhysicalTime(),
				nil, /* historicalTimestamp */
				"",
				0,
				ex.transitionCtx)
	}
}

// execStmtInAbortedState executes a statement in a txn that's in state
// Aborted or RestartWait. All statements result in error events except:
// - COMMIT / ROLLBACK: aborts the current transaction.
// - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//   allowing it to be retried.
func (ex *connExecutor) execStmtInAbortedState(
	ctx context.Context, stmt Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {

	reject := func() (fsm.Event, fsm.EventPayload) {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionAbortedError("" /* customMsg */),
		}
		return ev, payload
	}

	switch s := stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		if _, ok := s.(*tree.CommitTransaction); ok {
			// Note: Postgres replies to COMMIT of failed txn with "ROLLBACK" too.
			res.ResetStmtType((*tree.RollbackTransaction)(nil))
		}
		return ex.rollbackSQLTransaction(ctx)

	case *tree.RollbackToSavepoint:
		return ex.execRollbackToSavepointInAbortedState(ctx, s)

	case *tree.Savepoint:
		if ex.isCommitOnReleaseSavepoint(s.Name) {
			// We allow SAVEPOINT znbase_restart as an alternative to ROLLBACK TO
			// SAVEPOINT znbase_restart in the Aborted state. This is needed
			// because any client driver (that we know of) which links subtransaction
			// `ROLLBACK/RELEASE` to an object's lifetime will fail to `ROLLBACK` on a
			// failed `RELEASE`. Instead, we now can use the creation of another
			// subtransaction object (which will issue another `SAVEPOINT` statement)
			// to indicate retry intent. Specifically, this change was prompted by
			// subtransaction handling in `libpqxx` (C++ driver) and `rust-postgres`
			// (Rust driver).
			res.ResetStmtType((*tree.RollbackToSavepoint)(nil))
			return ex.execRollbackToSavepointInAbortedState(
				ctx, &tree.RollbackToSavepoint{Savepoint: s.Name})
		}
		return reject()

	default:
		return reject()
	}
}

// execStmtInCommitWaitState executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (ex *connExecutor) execStmtInCommitWaitState(
	stmt Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	ex.incrementStmtCounter(stmt)
	switch stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction, *tree.BeginCommit:
		// for execute prepared stmt, We need to determine whether auto commit is enabled.
		// if auto commit enabled, execute ddl need BeginCommit stmt.
		ok, err := ex.checkPrepareDDL(stmt.AST)
		if err != nil {
			return ex.makeErrEvent(err, stmt.AST)
		}
		if !ok {
			return nil, nil
		}

		// Reply to a rollback with the COMMIT tag, by analogy to what we do when we
		// get a COMMIT in state Aborted.
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
	default:
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionCommittedError(),
		}
		return ev, payload
	}
}

// runObserverStatement executes the given observer statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runObserverStatement(
	ctx context.Context, stmt Statement, res RestrictedCommandResult,
) error {
	switch sqlStmt := stmt.AST.(type) {
	case *tree.ShowTransactionStatus:
		return ex.runShowTransactionState(ctx, res)
	case *tree.ShowSavepointStatus:
		return ex.runShowSavepointState(ctx, res)
	case *tree.ShowSyntax:
		return ex.runShowSyntax(ctx, sqlStmt.Statement, res)
	case *tree.SetTracing:
		ex.runSetTracing(ctx, sqlStmt, res)
		return nil
	default:
		res.SetError(pgerror.NewAssertionErrorf("unrecognized observer statement type %T", stmt.AST))
		return nil
	}
}

// runShowSyntax executes a SHOW SYNTAX <stmt> query.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowSyntax(
	ctx context.Context, stmt string, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{
		{Name: "field", Typ: types.String},
		{Name: "message", Typ: types.String},
	})
	var commErr error
	if err := runShowSyntax(ctx, stmt, ex.getCaseSensitive(),
		func(ctx context.Context, field, msg string) error {
			commErr = res.AddRow(ctx, tree.Datums{tree.NewDString(field), tree.NewDString(msg)})
			return nil
		},
		ex.recordError, /* reportErr */
	); err != nil {
		res.SetError(err)
	}
	return commErr
}

// runShowTransactionState executes a SHOW TRANSACTION STATUS statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowTransactionState(
	ctx context.Context, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}})

	state := fmt.Sprintf("%s", ex.machine.CurState())
	return res.AddRow(ctx, tree.Datums{tree.NewDString(state)})
}

func (ex *connExecutor) runSetTracing(
	ctx context.Context, n *tree.SetTracing, res RestrictedCommandResult,
) {
	if len(n.Values) == 0 {
		res.SetError(fmt.Errorf("set tracing missing argument"))
		return
	}

	modes := make([]string, len(n.Values))
	for i, v := range n.Values {
		v = unresolvedNameToStrVal(v)
		strVal, ok := v.(*tree.StrVal)
		if !ok {
			res.SetError(fmt.Errorf("expected string for set tracing argument, not %T", v))
			return
		}
		modes[i] = strVal.RawString()
	}

	if err := ex.enableTracing(modes); err != nil {
		res.SetError(err)
	}
}

func (ex *connExecutor) enableTracing(modes []string) error {
	traceKV := false
	recordingType := tracing.SnowballRecording
	enableMode := true
	showResults := false

	for _, s := range modes {
		switch strings.ToLower(s) {
		case "results":
			showResults = true
		case "on":
			enableMode = true
		case "off":
			enableMode = false
		case "kv":
			traceKV = true
		case "local":
			recordingType = tracing.SingleNodeRecording
		case "cluster":
			recordingType = tracing.SnowballRecording
		default:
			return errors.Errorf("set tracing: unknown mode %q", s)
		}
	}
	if !enableMode {
		return ex.sessionTracing.StopTracing()
	}
	return ex.sessionTracing.StartTracing(recordingType, traceKV, showResults)
}

// addActiveQuery adds a running query to the list of running queries.
//
// It returns a cleanup function that needs to be run when the query is no
// longer executing. NOTE(andrei): As of Feb 2018, "executing" does not imply
// that the results have been delivered to the client.
func (ex *connExecutor) addActiveQuery(
	queryID ClusterWideID, stmt tree.Statement, cancelFun context.CancelFunc,
) func() {

	_, hidden := stmt.(tree.HiddenFromShowQueries)
	qm := &queryMeta{
		start:         ex.phaseTimes[sessionQueryReceived],
		stmt:          stmt,
		phase:         preparing,
		isDistributed: false,
		ctxCancel:     cancelFun,
		hidden:        hidden,
	}
	ex.mu.Lock()
	ex.mu.ActiveQueries[queryID] = qm
	ex.mu.Unlock()
	return func() {
		ex.mu.Lock()
		_, ok := ex.mu.ActiveQueries[queryID]
		if !ok {
			ex.mu.Unlock()
			panic(fmt.Sprintf("query %d missing from ActiveQueries", queryID))
		}
		delete(ex.mu.ActiveQueries, queryID)
		ex.mu.LastActiveQuery = qm.stmt

		ex.mu.Unlock()
	}
}

// handleAutoCommit commits the KV transaction if it hasn't been committed
// already.
//
// It's possible that the statement constituting the implicit txn has already
// committed it (in case it tried to run as a 1PC). This method detects that
// case.
// NOTE(andrei): It bothers me some that we're peeking at txn to figure out
// whether we committed or not, where SQL could already know that - individual
// statements could report this back through the Event.
//
// Args:
// stmt: The statement that we just ran.
func (ex *connExecutor) handleAutoCommit(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	}

	if knob := ex.server.cfg.TestingKnobs.BeforeAutoCommit; knob != nil {
		if err := knob(ctx, stmt.String()); err != nil {
			return ex.makeErrEvent(err, stmt)
		}
	}

	ev, payload := ex.commitSQLTransaction(ctx, stmt)
	var err error
	if perr, ok := payload.(payloadWithError); ok {
		err = perr.errorCause()
	}
	log.VEventf(ctx, 2, "AutoCommit. err: %v", err)
	return ev, payload
}

func (ex *connExecutor) incrementStmtCounter(stmt Statement) {
	ex.metrics.StatementCounters.incrementCount(ex, stmt.AST)
}

//DeepCopyAST function to deep copy data
//If the transaction is retried, the newly parsed statement is assigned to the original
func DeepCopyAST(pstmt parser.Statement, stmt Statement) {
	switch t := stmt.AST.(type) {
	case *tree.AlterIndex:
		*t = *(pstmt.AST.(*tree.AlterIndex))
	case *tree.AlterTable:
		*t = *(pstmt.AST.(*tree.AlterTable))
	case *tree.AlterSequence:
		*t = *(pstmt.AST.(*tree.AlterSequence))
	case *tree.AlterSchema:
		*t = *(pstmt.AST.(*tree.AlterSchema))
	case *tree.AlterViewAS:
		*t = *(pstmt.AST.(*tree.AlterViewAS))
	case *tree.CancelQueries:
		*t = *(pstmt.AST.(*tree.CancelQueries))
	case *tree.CancelSessions:
		*t = *(pstmt.AST.(*tree.CancelSessions))
	case *tree.CommentOnColumn:
		*t = *(pstmt.AST.(*tree.CommentOnColumn))
	case *tree.CommentOnDatabase:
		*t = *(pstmt.AST.(*tree.CommentOnDatabase))
	case *tree.CommentOnTable:
		*t = *(pstmt.AST.(*tree.CommentOnTable))
	case *tree.CommentOnIndex:
		*t = *(pstmt.AST.(*tree.CommentOnIndex))
	case *tree.CommentOnConstraint:
		*t = *(pstmt.AST.(*tree.CommentOnConstraint))
	case *tree.ControlJobs:
		*t = *(pstmt.AST.(*tree.ControlJobs))
	case *tree.Scrub:
		*t = *(pstmt.AST.(*tree.Scrub))
	case *tree.CreateDatabase:
		*t = *(pstmt.AST.(*tree.CreateDatabase))
	case *tree.CreateIndex:
		*t = *(pstmt.AST.(*tree.CreateIndex))
	case *tree.CreateTable:
		// 解决临时表事务重试卡死问题 NEWSQL-9091
		if !strings.Contains(string(t.Table.TableNamePrefix.SchemaName), "pg_temp") {
			*t = *(pstmt.AST.(*tree.CreateTable))
		}
	case *tree.CreateView:
		if !strings.Contains(string(t.Name.TableNamePrefix.SchemaName), "pg_temp") {
			*t = *(pstmt.AST.(*tree.CreateView))
		}
	case *tree.CreateSequence:
		if !strings.Contains(string(t.Name.TableNamePrefix.SchemaName), "pg_temp") {
			*t = *(pstmt.AST.(*tree.CreateSequence))
		}
	case *tree.CreateStats:
		*t = *(pstmt.AST.(*tree.CreateStats))
	case *tree.CreateSchema:
		*t = *(pstmt.AST.(*tree.CreateSchema))
	case *tree.Deallocate:
		*t = *(pstmt.AST.(*tree.Deallocate))
	case *tree.DeclareVariable:
		*t = *(pstmt.AST.(*tree.DeclareVariable))
	case *tree.VariableAccess:
		*t = *(pstmt.AST.(*tree.VariableAccess))
	case *tree.DeclareCursor:
		*t = *(pstmt.AST.(*tree.DeclareCursor))
	case *tree.CursorAccess:
		*t = *(pstmt.AST.(*tree.CursorAccess))
	case *tree.Delete:
		*t = *(pstmt.AST.(*tree.Delete))
	case *tree.Discard:
		*t = *(pstmt.AST.(*tree.Discard))
	case *tree.DropDatabase:
		*t = *(pstmt.AST.(*tree.DropDatabase))
	case *tree.DropIndex:
		*t = *(pstmt.AST.(*tree.DropIndex))
	case *tree.DropTable:
		*t = *(pstmt.AST.(*tree.DropTable))
	case *tree.DropView:
		*t = *(pstmt.AST.(*tree.DropView))
	case *tree.DropSequence:
		*t = *(pstmt.AST.(*tree.DropSequence))
	case *tree.DropUser:
		*t = *(pstmt.AST.(*tree.DropUser))
	case *tree.DropSchema:
		*t = *(pstmt.AST.(*tree.DropSchema))
	case *tree.Explain:
		*t = *(pstmt.AST.(*tree.Explain))
	case *tree.Grant:
		*t = *(pstmt.AST.(*tree.Grant))
	case *tree.Insert:
		*t = *(pstmt.AST.(*tree.Insert))
	case *tree.ParenSelect:
		*t = *(pstmt.AST.(*tree.ParenSelect))
	case *tree.QueryLockstat:
		*t = *(pstmt.AST).(*tree.QueryLockstat)
	case *tree.Relocate:
		*t = *(pstmt.AST.(*tree.Relocate))
	case *tree.RenameColumn:
		*t = *(pstmt.AST.(*tree.RenameColumn))
	case *tree.RenameDatabase:
		*t = *(pstmt.AST.(*tree.RenameDatabase))
	case *tree.RenameIndex:
		*t = *(pstmt.AST.(*tree.RenameIndex))
	case *tree.RenameTable:
		*t = *(pstmt.AST.(*tree.RenameTable))
	case *tree.Revoke:
		*t = *(pstmt.AST.(*tree.Revoke))
	case *tree.Scatter:
		*t = *(pstmt.AST.(*tree.Scatter))
	case *tree.SetClusterSetting:
		*t = *(pstmt.AST.(*tree.SetClusterSetting))
	case *tree.SetZoneConfig:
		*t = *(pstmt.AST.(*tree.SetZoneConfig))
	case *tree.SetVar:
		*t = *(pstmt.AST.(*tree.SetVar))
	case *tree.SetTransaction:
		*t = *(pstmt.AST.(*tree.SetTransaction))
	case *tree.SetSessionCharacteristics:
		*t = *(pstmt.AST.(*tree.SetSessionCharacteristics))
	case *tree.ShowCursor:
		*t = *(pstmt.AST.(*tree.ShowCursor))
	case *tree.ShowClusterSetting:
		*t = *(pstmt.AST.(*tree.ShowClusterSetting))
	case *tree.ShowVar:
		*t = *(pstmt.AST.(*tree.ShowVar))
	case *tree.ShowColumns:
		*t = *(pstmt.AST.(*tree.ShowColumns))
	case *tree.ShowConstraints:
		*t = *(pstmt.AST.(*tree.ShowConstraints))
	case *tree.ShowCreate:
		*t = *(pstmt.AST.(*tree.ShowCreate))
	case *tree.ShowDatabases:
		*t = *(pstmt.AST.(*tree.ShowDatabases))
	case *tree.ShowGrants:
		*t = *(pstmt.AST.(*tree.ShowGrants))
	case *tree.ShowHistogram:
		*t = *(pstmt.AST.(*tree.ShowHistogram))
	case *tree.ShowIndexes:
		*t = *(pstmt.AST.(*tree.ShowIndexes))
	case *tree.ShowQueries:
		*t = *(pstmt.AST.(*tree.ShowQueries))
	case *tree.ShowJobs:
		*t = *(pstmt.AST.(*tree.ShowJobs))
	case *tree.ShowRoleGrants:
		*t = *(pstmt.AST.(*tree.ShowRoleGrants))
	case *tree.ShowRoles:
		*t = *(pstmt.AST.(*tree.ShowRoles))
	case *tree.ShowSpaces:
		*t = *(pstmt.AST.(*tree.ShowSpaces))
	case *tree.ShowSessions:
		*t = *(pstmt.AST.(*tree.ShowSessions))
	case *tree.ShowTableStats:
		*t = *(pstmt.AST.(*tree.ShowTableStats))
	case *tree.ShowSyntax:
		*t = *(pstmt.AST.(*tree.ShowSyntax))
	case *tree.ShowTables:
		*t = *(pstmt.AST.(*tree.ShowTables))
	case *tree.ShowSchemas:
		*t = *(pstmt.AST.(*tree.ShowSchemas))
	case *tree.ShowSequences:
		*t = *(pstmt.AST.(*tree.ShowSequences))
	case *tree.ShowTraceForSession:
		*t = *(pstmt.AST.(*tree.ShowTraceForSession))
	case *tree.ShowTransactionStatus:
		*t = *(pstmt.AST.(*tree.ShowTransactionStatus))
	case *tree.ShowUsers:
		*t = *(pstmt.AST.(*tree.ShowUsers))
	case *tree.ShowZoneConfig:
		*t = *(pstmt.AST.(*tree.ShowZoneConfig))
	case *tree.ShowRanges:
		*t = *(pstmt.AST.(*tree.ShowRanges))
	case *tree.ShowFingerprints:
		*t = *(pstmt.AST.(*tree.ShowFingerprints))
	case *tree.Split:
		*t = *(pstmt.AST.(*tree.Split))
	case *tree.Truncate:
		*t = *(pstmt.AST.(*tree.Truncate))
	case *tree.UnionClause:
		*t = *(pstmt.AST.(*tree.UnionClause))
	case *tree.Update:
		*t = *(pstmt.AST.(*tree.Update))
	case *tree.ValuesClause:
		*t = *(pstmt.AST.(*tree.ValuesClause))
	case *tree.ValuesClauseWithNames:
		*t = *(pstmt.AST.(*tree.ValuesClauseWithNames))
	default:
		return
	}
}

// CheckDDLorDCL checks the ddl/dcl stmt, these stmts are not allowed in an explicit txn.
func CheckDDLorDCL(AST tree.Statement) bool {
	switch AST.(type) {
	case *tree.CreateTable, *tree.CreateDatabase, *tree.CreateSchema,
		*tree.CreateChangefeed, *tree.CreateIndex, *tree.CreateSequence, *tree.CreateView,
		*tree.CreateUser, *tree.CreateRole, *tree.CreateStats, /*create stmt*/
		*tree.AlterTable, *tree.AlterSchema, *tree.AlterIndex, *tree.AlterViewAS,
		*tree.AlterUser, *tree.AlterSequence,
		*tree.RenameTable, *tree.RenameDatabase, *tree.RenameIndex, *tree.RenameColumn, /*alter stmt*/
		*tree.DropDatabase, *tree.DropSchema, *tree.DropTable, *tree.Truncate,
		*tree.DropSequence, *tree.DropView, *tree.DropIndex,
		*tree.DropUser, *tree.DropRole, /*drop stmt*/
		*tree.CreateSnapshot, *tree.RevertSnapshot, *tree.DropSnapshot, /*SnapShot*/
		*tree.RevertFlashback, *tree.RevertDropFlashback, /*FlashBack*/
		*tree.ClearIntent,
		*tree.CommentOnDatabase, *tree.CommentOnTable, *tree.CommentOnIndex, *tree.CommentOnColumn, *tree.CommentOnConstraint, /*CommentOn schema missing*/
		*tree.Grant, *tree.GrantRole, *tree.Revoke, *tree.RevokeRole, /*DCL stmt*/
		*tree.Execute: /*Exucute stmt need to determine if stmt is DDL*/
		return true
	default:
		return false
	}
}

// checkPrepareDDL check if the AST is BeginCommit for Execute DDL stmt, if return true, execute BeginCommit
func (ex *connExecutor) checkPrepareDDL(AST tree.Statement) (b bool, err error) {
	if st, ok := AST.(*tree.BeginCommit); ok {
		name := st.Name.String()
		if st.Prepared {
			ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
			if !ok {
				err := pgerror.NewErrorf(
					pgcode.InvalidSQLStatementName,
					"prepared statement %q does not exist", name,
				)
				return false, err
			}
			if !CheckDDLorDCL(ps.AST) {
				return false, nil
			}
		}
	}
	return true, nil
}
