// Copyright 2019  The Cockroach Authors.
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

package storage

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/closedts/ctpb"
	"github.com/znbasedb/znbase/pkg/storage/concurrency"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/storage/storagebase"
	"github.com/znbasedb/znbase/pkg/storage/storagepb"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// executeWriteBatch is the entry point for client requests which may mutate the
// range's replicated state. Requests taking this path are evaluated and ultimately
// serialized through Raft, but pass through additional machinery whose goal is
// to allow commands which commute to be proposed in parallel. The naive
// alternative, submitting requests to Raft one after another, paying massive
// latency, is only taken for commands whose effects may overlap.
//
// Concretely,
//
// - Latches for the keys affected by the command are acquired (i.e.
//   tracked as in-flight mutations).
// - In doing so, we wait until no overlapping mutations are in flight.
// - The timestamp cache is checked to determine if the command's affected keys
//   were accessed with a timestamp exceeding that of the command; if so, the
//   command's timestamp is incremented accordingly.
// - A RaftCommand is constructed. If proposer-evaluated KV is active,
//   the request is evaluated and the Result is placed in the
//   RaftCommand. If not, the request itself is added to the command.
// - The proposal is inserted into the Replica's in-flight proposals map,
//   a lease index is assigned to it, and it is submitted to Raft, returning
//   a channel.
// - The result of the Raft proposal is read from the channel and the command
//   registered with the timestamp cache, its latches are released, and
//   its result (which could be an error) is returned to the client.
//
// Returns exactly one of a response, an error or re-evaluation reason.
//
// NB: changing BatchRequest to a pointer here would have to be done cautiously
// as this method makes the assumption that it operates on a shallow copy (see
// call to applyTimestampCache).
func (r *Replica) executeWriteBatch(
	ctx context.Context, ba *roachpb.BatchRequest, st storagepb.LeaseStatus, g *concurrency.Guard,
) (br *roachpb.BatchResponse, _ *concurrency.Guard, pErr *roachpb.Error) {
	startTime := timeutil.Now()

	// Verify that the batch can be executed.
	// NB: we only need to check that the request is in the Range's key bounds
	// at proposal time, not at application time, because the spanlatch manager
	// will synchronize all requests (notably EndTransaction with SplitTrigger)
	// that may cause this condition to change.
	if err := r.checkExecutionCanProceed(ba, g, &st); err != nil {
		return nil, g, roachpb.NewError(err)
	}

	minTS, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
	defer untrack(ctx, 0, 0) // covers all error returns below

	// Examine the read and write timestamp caches for preceding
	// commands which require this command to move its timestamp
	// forward. Or, in the case of a transactional write, the txn
	// timestamp and possible write-too-old bool.
	if bumped := r.applyTimestampCache(ctx, ba, minTS); pErr != nil {
		return nil, g, pErr
	} else if bumped {
		// If we bump the transaction's timestamp, we must absolutely
		// tell the client in a response transaction (for otherwise it
		// doesn't know about the incremented timestamp). Response
		// transactions are set far away from this code, but at the time
		// of writing, they always seem to be set. Since that is a
		// likely target of future micro-optimization, this assertion is
		// meant to protect against future correctness anomalies.
		defer func() {
			if br != nil && ba.Txn != nil && br.Txn == nil {
				log.Fatalf(ctx, "assertion failed: transaction updated by "+
					"timestamp cache, but transaction returned in response; "+
					"updated timestamp would have been lost (recovered): "+
					"%s in batch %s", ba.Txn, ba,
				)
			}
		}()
	}

	log.Event(ctx, "applied timestamp cache")
	// Checking the context just before proposing can help avoid ambiguous errors.
	if err := ctx.Err(); err != nil {
		log.VEventf(ctx, 2, "%s before proposing: %s", err, ba.Summary())
		return nil, g, roachpb.NewError(errors.Wrap(err, "aborted before proposing"))
	}

	// Check that the lease is still valid before proposing to avoid discovering
	// this after replication and potentially missing out on the chance to retry
	// if the request is using AsyncConsensus. This is best-effort, but can help
	// in cases where the request waited arbitrarily long for locks acquired by
	// other transactions to be released while sequencing in the concurrency
	// manager.
	if curLease, _ := r.GetLease(); curLease.Sequence > st.Lease.Sequence {
		curLeaseCpy := curLease // avoid letting curLease escape
		err := newNotLeaseHolderError(&curLeaseCpy, r.store.StoreID(), r.Desc())
		log.VEventf(ctx, 2, "%s before proposing: %s", err, ba.Summary())
		return nil, g, roachpb.NewError(err)
	}

	// If the command is proposed to Raft, ownership of and responsibility for
	// the concurrency guard will be assumed by Raft, so provide the guard to
	// evalAndPropose.
	ch, abandon, maxLeaseIndex, pErr := r.evalAndPropose(ctx, ba, g, &st.Lease)
	if pErr != nil {
		if maxLeaseIndex != 0 {
			log.Fatalf(
				ctx, "unexpected max lease index %d assigned to failed proposal: %s, error %s",
				maxLeaseIndex, ba, pErr,
			)
		}
		return nil, g, pErr
	}

	g = nil // ownership passed to Raft, prevent misuse

	// A max lease index of zero is returned when no proposal was made or a lease was proposed.
	// In both cases, we don't need to communicate a MLAI.
	if maxLeaseIndex != 0 {
		untrack(ctx, r.RangeID, ctpb.LAI(maxLeaseIndex))
	}

	// If the command was accepted by raft, wait for the range to apply it.
	ctxDone := ctx.Done()
	shouldQuiesce := r.store.stopper.ShouldQuiesce()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	slowTimer.Reset(base.SlowRequestThreshold)
	tBegin := timeutil.Now()
	log.Eventf(ctx, "Start raft loop.")
	for {
		select {
		case propResult := <-ch:
			// Semi-synchronously process any intents that need resolving here in
			// order to apply back pressure on the client which generated them. The
			// resolution is semi-synchronous in that there is a limited number of
			// outstanding asynchronous resolution tasks allowed after which
			// further calls will block.
			if len(propResult.EncounteredIntents) > 0 {
				// TODO(peter): Re-proposed and canceled (but executed) commands can
				// both leave intents to GC that don't hit this code path. No good
				// solution presents itself at the moment and such intents will be
				// resolved on reads.
				if err := r.store.intentResolver.CleanupIntentsAsync(
					ctx, propResult.EncounteredIntents, true /* allowSync */, propResult.BackFill,
				); err != nil {
					log.Warning(ctx, err)
				}
			}
			if len(propResult.EndTxns) > 0 {
				if err := r.store.intentResolver.CleanupTxnIntentsAsync(
					ctx, r.RangeID, propResult.EndTxns, true /* allowSync */, propResult.BackFill,
				); err != nil {
					log.Warning(ctx, err)
				}
			}
			log.Eventf(ctx, "Start raft loop END.")
			return propResult.Reply, nil, propResult.Err
		case <-slowTimer.C:
			slowTimer.Read = true
			log.Warningf(ctx, `have been waiting %.2fs for proposing command %s.
This range is likely unavailable.
Please contact us with this message to ZNBaseDB technical support 

along with

	https://yourhost:8080/#/reports/range/%d

and the following Raft status: %+v`,
				timeutil.Since(tBegin).Seconds(),
				ba,
				r.RangeID,
				r.RaftStatus(),
			)
			r.store.metrics.SlowRaftRequests.Inc(1)
			defer func() {
				r.store.metrics.SlowRaftRequests.Dec(1)
				log.Infof(
					ctx,
					"slow command %s finished after %.2fs with error %v",
					ba,
					timeutil.Since(tBegin).Seconds(),
					pErr,
				)
			}()

		case <-ctxDone:

			// If our context was canceled, return an AmbiguousResultError,
			// which indicates to the caller that the command may have executed.
			abandon()
			log.VEventf(ctx, 2, "context cancellation after %0.1fs of attempting command %s",
				timeutil.Since(startTime).Seconds(), ba)
			return nil, nil, roachpb.NewError(roachpb.NewAmbiguousResultError(ctx.Err().Error()))
		case <-shouldQuiesce:

			// If shutting down, return an AmbiguousResultError, which indicates
			// to the caller that the command may have executed.
			abandon()
			log.VEventf(ctx, 2, "shutdown cancellation after %0.1fs of attempting command %s",
				timeutil.Since(startTime).Seconds(), ba)
			return nil, nil, roachpb.NewError(roachpb.NewAmbiguousResultError("server shutdown"))
		}
	}
}

// evaluateWriteBatch evaluates the supplied batch.
//
// If the batch is transactional and has all the hallmarks of a 1PC
// commit (i.e. includes all intent writes & EndTransaction, and
// there's nothing to suggest that the transaction will require retry
// or restart), the batch's txn is stripped and it's executed as an
// atomic batch write. If the writes cannot all be completed at the
// intended timestamp, the batch's txn is restored and it's
// re-executed in full. This allows it to lay down intents and return
// an appropriate retryable error.
func (r *Replica) evaluateWriteBatch(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
) (engine.Batch, enginepb.MVCCStats, *roachpb.BatchResponse, result.Result, *roachpb.Error) {

	// If the transaction has been pushed but it can commit at the higher
	// timestamp, let's evaluate the batch at the bumped timestamp. This will
	// allow it commit, and also it'll allow us to attempt the 1PC code path.
	maybeBumpReadTimestampToWriteTimestamp(ctx, ba, latchSpans)

	// Attempt 1PC execution, if applicable. If not transactional or there are
	// indications that the batch's txn will require retry, execute as normal.
	if isOnePhaseCommit(ba) {
		res := r.evaluate1PC(ctx, idKey, ba, latchSpans)
		switch res.success {
		case onePCSucceeded:
			return res.batch, res.stats, res.br, res.res, nil
		case onePCFailed:
			if res.pErr == nil {
				log.Fatalf(ctx, "1PC failed but no err. ba: %s", ba.String())
			}
			return nil, enginepb.MVCCStats{}, nil, result.Result{}, res.pErr
		case onePCFallbackToTransactionalEvaluation:
		}
	}

	ms := new(enginepb.MVCCStats)
	rec := NewReplicaEvalContext(r, latchSpans)
	batch, br, res, pErr := r.evaluateWriteBatchWithServersideRefreshes(
		ctx, idKey, rec, ms, ba, latchSpans, nil /* deadline */)
	return batch, *ms, br, res, pErr
}

type onePCSuccess int

const (
	// onePCSucceeded means that the 1PC evaluation succeeded and the results should be
	// returned to the client.
	onePCSucceeded onePCSuccess = iota
	// onePCFailed means that the 1PC evaluation failed and the attached error should be
	// returned to the client.
	onePCFailed
	// onePCFallbackToTransactionalEvaluation means that 1PC evaluation failed, but
	// regular transactional evaluation should be attempted.
	onePCFallbackToTransactionalEvaluation
)

type onePCResult struct {
	success onePCSuccess
	// pErr is set if success == onePCFailed. This is the error that should be
	// returned to the client for this request.
	pErr *roachpb.Error

	// The fields below are only set when success == onePCSucceeded.
	stats enginepb.MVCCStats
	br    *roachpb.BatchResponse
	res   result.Result
	batch engine.Batch
}

// evaluate1PC attempts to evaluate the batch as a 1PC transaction - meaning it
// attempts to evaluate the batch as a non-transactional request. This is only
// possible if the batch contains all of the transaction's writes, which the
// caller needs to ensure. If successful, evaluating the batch this way is more
// efficient - we're avoiding writing the transaction record and writing and the
// immediately deleting intents.
func (r *Replica) evaluate1PC(
	ctx context.Context, idKey storagebase.CmdIDKey, ba *roachpb.BatchRequest, spans *spanset.SpanSet,
) (onePCRes onePCResult) {
	log.VEventf(ctx, 2, "attempting 1PC execution")

	var batch engine.Batch
	defer func() {
		// Close the batch unless it's passed to the caller (when the evaluation
		// succeeds).
		if onePCRes.success != onePCSucceeded {
			batch.Close()
		}
	}()

	// Try executing with transaction stripped.
	strippedBa := *ba
	strippedBa.Txn = nil
	strippedBa.Requests = ba.Requests[:len(ba.Requests)-1] // strip end txn req

	rec := NewReplicaEvalContext(r, spans)
	var br *roachpb.BatchResponse
	var res result.Result
	var pErr *roachpb.Error

	arg, _ := ba.GetArg(roachpb.EndTransaction)
	etArg := arg.(*roachpb.EndTransactionRequest)
	// Evaluate strippedBa. If the transaction allows, permit refreshes.
	ms := new(enginepb.MVCCStats)
	if ba.CanForwardRead {
		batch, br, res, pErr = r.evaluateWriteBatchWithServersideRefreshes(
			ctx, idKey, rec, ms, &strippedBa, spans, etArg.Deadline)
	} else {
		batch, br, res, pErr = r.evaluateWriteBatchWrapper(
			ctx, idKey, rec, ms, &strippedBa, spans)
	}

	if pErr != nil || (!ba.CanForwardRead && ba.Timestamp != br.Timestamp) {
		if etArg.Require1PC {
			if pErr == nil {
				pErr = roachpb.NewError(roachpb.NewTransactionRetryError(
					roachpb.RETRY_SERIALIZABLE, "Require1PC batch pushed"))
			}
			return onePCResult{
				success: onePCFailed,
				pErr:    pErr,
			}
		}

		if pErr != nil {
			log.VEventf(ctx, 2,
				"1PC execution failed, falling back to transactional execution. pErr: %v", pErr.String())
		} else {
			log.VEventf(ctx, 2,
				"1PC execution failed, falling back to transactional execution; the batch was pushed")
		}
		return onePCResult{success: onePCFallbackToTransactionalEvaluation}
	}

	// 1PC execution was successful, let's synthesize an EndTxnResponse.

	clonedTxn := ba.Txn.Clone()
	clonedTxn.Status = roachpb.COMMITTED
	// Make sure the returned txn has the actual commit timestamp. This can be
	// different from ba.Txn's if the stripped batch was evaluated at a bumped
	// timestamp.
	clonedTxn.ReadTimestamp = br.Timestamp
	clonedTxn.WriteTimestamp = br.Timestamp

	// If the end transaction is not committed, clear the batch and mark the status aborted.
	if !etArg.Commit {
		clonedTxn.Status = roachpb.ABORTED
		batch.Close()
		batch = r.store.Engine().NewBatch()
		ms = new(enginepb.MVCCStats)
	} else {
		// Run commit trigger manually.
		innerResult, err := batcheval.RunCommitTrigger(ctx, rec, batch, ms, etArg, clonedTxn)
		if err != nil {
			return onePCResult{
				success: onePCFailed,
				pErr:    roachpb.NewErrorf("failed to run commit trigger: %s", err),
			}
		}
		if err := res.MergeAndDestroy(innerResult); err != nil {
			return onePCResult{
				success: onePCFailed,
				pErr:    roachpb.NewError(err),
			}
		}
	}
	res.Local.ResolvedLocks = make([]roachpb.LockUpdate, len(etArg.LockSpans))
	for i, sp := range etArg.LockSpans {
		res.Local.ResolvedLocks[i] = roachpb.LockUpdate{
			Span:           sp,
			Txn:            clonedTxn.TxnMeta,
			Status:         clonedTxn.Status,
			IgnoredSeqNums: clonedTxn.IgnoredSeqNums,
		}
	}
	// Add placeholder responses for end transaction requests.
	br.Add(&roachpb.EndTransactionResponse{OnePhaseCommit: true})
	br.Txn = clonedTxn
	return onePCResult{
		success: onePCSucceeded,
		stats:   *ms,
		br:      br,
		res:     res,
		batch:   batch,
	}
}

// evaluateWriteBatchWithServersideRefreshes invokes evaluateBatch and retries
// at a higher timestamp in the event of some retriable errors if allowed by the
// batch/txn.
//
// deadline, if not nil, specifies the highest timestamp (exclusive) at which
// the request can be evaluated. If ba is a transactional request, then dealine
// cannot be specified; a transaction's deadline comes from it's EndTxn request.
func (r *Replica) evaluateWriteBatchWithServersideRefreshes(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
	deadline *hlc.Timestamp,
) (batch engine.Batch, br *roachpb.BatchResponse, res result.Result, pErr *roachpb.Error) {
	goldenMS := *ms
	for retries := 0; ; retries++ {
		if retries > 0 {
			log.VEventf(ctx, 2, "server-side retry of batch")
		}
		if batch != nil {
			// Reset the stats.
			*ms = goldenMS
			batch.Close()
		}

		batch, br, res, pErr = r.evaluateWriteBatchWrapper(ctx, idKey, rec, ms, ba, latchSpans)

		var success bool
		if pErr == nil {
			wto := br.Txn != nil && br.Txn.WriteTooOld
			success = !wto
		} else {
			success = false
		}

		// If we can retry, set a higher batch timestamp and continue.
		// Allow one retry only; a non-txn batch containing overlapping
		// latchSpans will always experience WriteTooOldError.
		if success || retries > 0 || !canDoServersideRetry(ctx, pErr, ba, br, latchSpans, deadline) {
			break
		}
	}
	return batch, br, res, pErr
}

// evaluateWriteBatchWrapper is a wrapper on top of evaluateBatch() which deals
// with filling out result.LogicalOpLog.
func (r *Replica) evaluateWriteBatchWrapper(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba *roachpb.BatchRequest,
	spans *spanset.SpanSet,
) (engine.Batch, *roachpb.BatchResponse, result.Result, *roachpb.Error) {
	batch, opLogger := r.newBatchedEngine(spans)
	backFill := ba.BackFill
	br, res, pErr := evaluateBatch(ctx, idKey, batch, rec, ms, ba, false /* readOnly */)
	if pErr == nil {
		if opLogger != nil {
			opts := opLogger.LogicalOps()
			for _, opt := range opts {
				switch t := opt.GetValue().(type) {
				case *enginepb.MVCCWriteValueOp:
					t.BackFill = backFill
				case *enginepb.MVCCCommitIntentOp:
					t.BackFill = backFill
				case *enginepb.MVCCWriteIntentOp:
					t.BackFill = backFill
				case *enginepb.MVCCUpdateIntentOp:
					t.BackFill = backFill
				case *enginepb.MVCCAbortIntentOp:
					t.BackFill = backFill
				default:
					panic(fmt.Sprintf("unknown logical op %T", t))
				}
			}
			res.LogicalOpLog = &storagepb.LogicalOpLog{
				Ops: opLogger.LogicalOps(),
			}
		}
	}
	return batch, br, res, pErr
}

// canDoServersideRetry looks at the error produced by evaluating ba and decides
// if it's possible to retry the batch evaluation at a higher timestamp.
// Retrying is sometimes possible in case of some retriable errors which ask for
// higher timestamps: for transactional requests, retrying is possible if the
// transaction had not performed any prior reads that need refreshing.
//
// deadline, if not nil, specifies the highest timestamp (exclusive) at which
// the request can be evaluated. If ba is a transactional request, then dealine
// cannot be specified; a transaction's deadline comes from it's EndTxn request.
//
// If true is returned, ba and ba.Txn will have been updated with the new
// timestamp.
func canDoServersideRetry(
	ctx context.Context,
	pErr *roachpb.Error,
	ba *roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	latchSpans *spanset.SpanSet,
	deadline *hlc.Timestamp,
) bool {
	if ba.Txn != nil {
		if !ba.CanForwardRead {
			return false
		}
		if deadline != nil {
			log.Fatal(ctx, "deadline passed for transactional request")
		}
		if etArg, ok := ba.GetArg(roachpb.EndTransaction); ok {
			et := etArg.(*roachpb.EndTransactionRequest)
			deadline = et.Deadline
		}
	}

	var newTimestamp hlc.Timestamp
	if pErr != nil {
		switch tErr := pErr.GetDetail().(type) {
		case *roachpb.WriteTooOldError:
			txn := pErr.GetTxn()
			if txn != nil && txn.IsolationLevel == util.ReadCommittedIsolation {
				return false
			}
			// Locking scans hit WriteTooOld errors if they encounter values at
			// timestamps higher than their read timestamps. The encountered
			// timestamps are guaranteed to be greater than the txn's read
			// timestamp, but not its write timestamp. So, when determining what
			// the new timestamp should be, we make sure to not regress the
			// txn's write timestamp.
			newTimestamp = tErr.ActualTimestamp
			if ba.Txn != nil {
				newTimestamp.Forward(pErr.GetTxn().WriteTimestamp)
			}
		case *roachpb.TransactionRetryError:
			if ba.Txn == nil {
				// TODO(andrei): I don't know if TransactionRetryError is possible for
				// non-transactional batches, but some tests inject them for 1PC
				// transactions. I'm not sure how to deal with them, so let's not retry.
				return false
			}
			newTimestamp = pErr.GetTxn().WriteTimestamp
		default:
			// TODO(andrei): Handle other retriable errors too.
			return false
		}
	} else {
		if !br.Txn.WriteTooOld {
			log.Fatalf(ctx, "programming error: expected the WriteTooOld flag to be set")
		}
		newTimestamp = br.Txn.WriteTimestamp
	}

	if deadline != nil && deadline.LessEq(newTimestamp) {
		return false
	}
	return tryBumpBatchTimestamp(ctx, ba, newTimestamp, latchSpans)
}

// newBatchedEngine creates an engine.Batch. Depending on whether rangefeeds
// are enabled, it also returns an engine.OpLoggerBatch. If non-nil, then this
// OpLogger is attached to the returned engine.Batch, recording all operations.
// Its recording should be attached to the Result of request evaluation.
func (r *Replica) newBatchedEngine(spans *spanset.SpanSet) (engine.Batch, *engine.OpLoggerBatch) {
	batch := r.store.Engine().NewBatch()
	var opLogger *engine.OpLoggerBatch
	if RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
		// TODO(nvanbenschoten): once we get rid of the RangefeedEnabled
		// cluster setting we'll need a way to turn this on when any
		// replica (not just the leaseholder) wants it and off when no
		// replicas want it. This turns out to be pretty involved.
		//
		// The current plan is to:
		// - create a range-id local key that stores all replicas that are
		//   subscribed to logical operations, along with their corresponding
		//   liveness epoch.
		// - create a new command that adds or subtracts replicas from this
		//   structure. The command will be a write across the entire replica
		//   span so that it is serialized with all writes.
		// - each replica will add itself to this set when it first needs
		//   logical ops. It will then wait until it sees the replicated command
		//   that added itself pop out through Raft so that it knows all
		//   commands that are missing logical ops are gone.
		// - It will then proceed as normal, relying on the logical ops to
		//   always be included on the raft commands. When its no longer
		//   needs logical ops, it will remove itself from the set.
		// - The leaseholder will have a new queue to detect registered
		//   replicas that are no longer live and remove them from the
		//   set to prevent "leaking" subscriptions.
		// - The condition here to add logical logging will be:
		//     if len(replicaState.logicalOpsSubs) > 0 { ... }
		//
		// An alternative to this is the reduce the cost of the including
		// the logical op log to a negligible amount such that it can be
		// included on all raft commands, regardless of whether any replica
		// has a rangefeed running or not.
		//
		// Another alternative is to make the setting table/zone-scoped
		// instead of a fine-grained per-replica state.
		opLogger = engine.NewOpLoggerBatch(batch)
		batch = opLogger
	}
	if util.RaceEnabled {
		// During writes we may encounter a versioned value newer than the request
		// timestamp, and may have to retry at a higher timestamp. This is still
		// safe as we're only ever writing at timestamps higher than the timestamp
		// any write latch would be declared at. But because of this, we don't
		// assert on access timestamps using spanset.NewBatchAt.
		batch = spanset.NewBatch(batch, spans)
	}
	return batch, opLogger
}

// isOnePhaseCommit returns true iff the BatchRequest contains all commands in
// the transaction, starting with BeginTransaction and ending with
// EndTransaction. One phase commits are disallowed if (1) the transaction has
// already been flagged with a write too old error, or (2) if isolation is
// serializable and the commit timestamp has been forwarded, or (3) the
// transaction exceeded its deadline, or (4) the testing knobs disallow optional
// one phase commits and the BatchRequest does not require one phase commit.
func isOnePhaseCommit(ba *roachpb.BatchRequest) bool {
	if ba.Txn == nil {
		return false
	}
	if !ba.IsCompleteTransaction() {
		return false
	}
	arg, _ := ba.GetArg(roachpb.EndTransaction)
	etArg := arg.(*roachpb.EndTransactionRequest)
	if retry, _, _ := batcheval.IsEndTransactionTriggeringRetryError(ba.Txn, etArg); retry {
		return false
	}
	// If the transaction has already restarted at least once then it may have
	// left intents at prior epochs that need to be cleaned up during the
	// process of committing the transaction. Even if the current epoch could
	// perform a one phase commit, we don't allow it to because that could
	// prevent it from properly resolving intents from prior epochs and cause
	// it to abandon them instead.
	//
	// The exception to this rule is transactions that require a one phase
	// commit. We know that if they also required a one phase commit in past
	// epochs then they couldn't have left any intents that they now need to
	// clean up.
	return ba.Txn.Epoch == 0 || etArg.Require1PC
}
