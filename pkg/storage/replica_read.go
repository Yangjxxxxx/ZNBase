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

	"github.com/kr/pretty"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/concurrency"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/storage/storagebase"
	"github.com/znbasedb/znbase/pkg/storage/storagepb"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// executeReadOnlyBatch is the execution logic for client requests which do not
// mutate the range's replicated state. The method uses a single RocksDB
// iterator to evaluate the batch and then updates the read timestamp cache to
// reflect the key spans that it read.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba *roachpb.BatchRequest, st storagepb.LeaseStatus, g *concurrency.Guard,
) (br *roachpb.BatchResponse, _ *concurrency.Guard, pErr *roachpb.Error) {
	log.Event(ctx, "waiting for read lock")
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()
	// Verify that the batch can be executed.
	if err := r.checkExecutionCanProceed(ba, g, &st); err != nil {
		return nil, g, roachpb.NewError(err)
	}

	// Evaluate read-only batch command. It checks for matching key range; note
	// that holding readOnlyCmdMu throughout is important to avoid reads from the
	// "wrong" key range being served after the range has been split.
	spans := g.LatchSpans()
	rec := NewReplicaEvalContext(r, spans)
	readOnly := r.store.Engine().NewReadOnly()
	if util.RaceEnabled {
		readOnly = spanset.NewReadWriter(readOnly, spans)
	}
	defer readOnly.Close()
	// TODO(nvanbenschoten): once all replicated intents are pulled into the
	// concurrency manager's lock-table, we can be sure that if we reached this
	// point, we will not conflict with any of them during evaluation. This in
	// turn means that we can bump the timestamp cache *before* evaluation
	// without risk of starving writes. Once we start doing that, we're free to
	// release latches immediately after we acquire an engine iterator as long
	// as we're performing a non-locking read.

	var result result.Result
	br, result, pErr = r.executeReadOnlyBatchWithServersideRefreshes(ctx, readOnly, rec, ba, spans)

	// If the request hit a server-side concurrency retry error, immediately
	// proagate the error. Don't assume ownership of the concurrency guard.
	if isConcurrencyRetryError(pErr) {
		return nil, g, pErr
	}

	// Handle any local (leaseholder-only) side-effects of the request.
	intents := result.Local.DetachEncounteredIntents()
	if pErr == nil {
		pErr = r.handleReadOnlyLocalEvalResult(ctx, ba, result.Local)
	}
	// Otherwise, update the timestamp cache and release the concurrency guard.
	ec, g := endCmds{repl: r, g: g}, nil
	ec.done(ba, br, pErr)

	// Semi-synchronously process any intents that need resolving here in
	// order to apply back pressure on the client which generated them. The
	// resolution is semi-synchronous in that there is a limited number of
	// outstanding asynchronous resolution tasks allowed after which
	// further calls will block.
	if len(intents) > 0 {
		log.Eventf(ctx, "submitting %d intents to asynchronous processing", len(intents))
		// We only allow synchronous intent resolution for consistent requests.
		// Intent resolution is async/best-effort for inconsistent requests.
		//
		// An important case where this logic is necessary is for RangeLookup
		// requests. In their case, synchronous intent resolution can deadlock
		// if the request originated from the local node which means the local
		// range descriptor cache has an in-flight RangeLookup request which
		// prohibits any concurrent requests for the same range. See #17760.
		allowSyncProcessing := ba.ReadConsistency == roachpb.CONSISTENT
		if err := r.store.intentResolver.CleanupIntentsAsync(ctx, intents, allowSyncProcessing, false); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}
	if pErr != nil {
		log.VErrEventf(ctx, 3, "%v", pErr.String())
	} else {
		log.Event(ctx, "read completed")
	}
	return br, nil, pErr
}

// executeReadOnlyBatchWithServersideRefreshes invokes evaluateBatch and retries
// at a higher timestamp in the event of some retriable errors if allowed by the
// batch/txn.
func (r *Replica) executeReadOnlyBatchWithServersideRefreshes(
	ctx context.Context,
	rw engine.ReadWriter,
	rec batcheval.EvalContext,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
) (br *roachpb.BatchResponse, res result.Result, pErr *roachpb.Error) {
	log.Event(ctx, "executing read-only batch")

	for retries := 0; ; retries++ {
		if retries > 0 {
			log.VEventf(ctx, 2, "server-side retry of batch")
		}
		br, res, pErr = evaluateBatch(ctx, storagebase.CmdIDKey(""), rw, rec, nil, ba, true /* readOnly */)
		// If we can retry, set a higher batch timestamp and continue.
		// Allow one retry only.
		if pErr == nil || retries > 0 || !canDoServersideRetry(ctx, pErr, ba, br, latchSpans, nil /* deadline */) {
			break
		}
	}

	if pErr != nil {
		// Failed read-only batches can't have any Result except for what's
		// allowlisted here.
		res.Local = result.LocalResult{
			EncounteredIntents: res.Local.DetachEncounteredIntents(),
			Metrics:            res.Local.Metrics,
		}
		return nil, res, pErr
	}
	return br, res, nil
}

func (r *Replica) handleReadOnlyLocalEvalResult(
	ctx context.Context, ba *roachpb.BatchRequest, lResult result.LocalResult,
) *roachpb.Error {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		lResult.Reply = nil
	}

	if lResult.AcquiredLocks != nil {
		// These will all be unreplicated locks.
		for i := range lResult.AcquiredLocks {
			r.concMgr.OnLockAcquired(ctx, &lResult.AcquiredLocks[i])
		}
		lResult.AcquiredLocks = nil
	}

	if lResult.MaybeWatchForMerge {
		// A merge is (likely) about to be carried out, and this replica needs
		// to block all traffic until the merge either commits or aborts. See
		// docs/tech-notes/range-merges.md.
		if err := r.maybeWatchForMerge(ctx); err != nil {
			return roachpb.NewError(err)
		}
		lResult.MaybeWatchForMerge = false
	}

	if !lResult.IsZero() {
		log.Fatalf(ctx, "unhandled field in LocalEvalResult: %s", pretty.Diff(lResult, result.LocalResult{}))
	}
	return nil
}

func backwardTimeStamp(ba *roachpb.BatchRequest, duration int64) {
	ba.Timestamp.WallTime -= duration * 1000 * 1000 * 1000
	ba.Txn.ReadTimestamp.WallTime -= duration * 1000 * 1000 * 1000
	ba.Txn.MaxTimestamp.WallTime -= duration * 1000 * 1000 * 1000
}
func forwardTimeStamp(br *roachpb.BatchResponse, duration int64) {
	br.Timestamp.WallTime += duration * 1000 * 1000 * 1000
	br.Txn.ReadTimestamp.WallTime += duration * 1000 * 1000 * 1000
	br.Txn.MaxTimestamp.WallTime += duration * 1000 * 1000 * 1000
}
