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

package kv

import (
	"context"
	"fmt"
	"sort"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// RefreshInvalidReason 记录refresh失效的原因
type RefreshInvalidReason int32

const (
	// ExceedRefreshMemoryLimit 表示超出refresh的内存缓存限制
	ExceedRefreshMemoryLimit RefreshInvalidReason = 1
)

// MaxTxnRefreshSpansBytes is a threshold in bytes for refresh spans stored
// on the coordinator during the lifetime of a transaction. Refresh spans
// are used for SERIALIZABLE transactions to avoid client restarts.
var MaxTxnRefreshSpansBytes = settings.RegisterIntSetting(
	"kv.transaction.max_refresh_spans_bytes",
	"maximum number of bytes used to track refresh spans in serializable transactions",
	256*1000,
)

// txnSpanRefresher is a txnInterceptor that collects the read spans of a
// serializable transaction in the event it gets a serializable retry error. It
// can then use the set of read spans to avoid retrying the transaction if all
// the spans can be updated to the current transaction timestamp.
//
// Serializable isolation mandates that transactions appear to have occurred in
// some total order, where none of their component sub-operations appear to have
// interleaved with sub-operations from other transactions. ZNBaseDB enforces
// this isolation level by ensuring that all of a transaction's reads and writes
// are performed at the same HLC timestamp. This timestamp is referred to as the
// transaction's commit timestamp.
//
// As a transaction in ZNBaseDB executes at a certain provisional commit
// timestamp, it lays down intents at this timestamp for any write operations
// and ratchets various timestamp cache entries to this timestamp for any read
// operations. If a transaction performs all of its reads and writes and is able
// to commit at its original provisional commit timestamp then it may go ahead
// and do so. However, for a number of reasons including conflicting reads and
// writes, a transaction may discover that its provisional commit timestamp is
// too low and that it needs to move this timestamp forward to commit.
//
// This poses a problem for operations that the transaction has already
// completed at lower timestamps. Are the effects of these operations still
// valid? The transaction is always free to perform a full restart at a higher
// epoch, but this often requires iterating in a client-side retry loop and
// performing all of the transaction's operations again. Intents are maintained
// across retries to improve the chance that later epochs succeed, but it is
// vastly preferable to avoid re-issuing these operations. Instead, it would be
// ideal if the transaction could "move" each of its operations to its new
// provisional commit timestamp without redoing them entirely.
//
// Only a single write intent can exist on a key and no reads are allowed above
// the intent's timestamp until the intent is resolved, so a transaction is free
// to move any of its intent to a higher timestamp. In fact, a synchronous
// rewrite of these intents isn't even necessary because intent resolution will
// already rewrite the intents at higher timestamp if necessary. So, moving
// write intents to a higher timestamp can be performed implicitly by committing
// their transaction at a higher timestamp. However, unlike intents created by
// writes, timestamp cache entries created by reads only prevent writes on
// overlapping keys from being written at or below their timestamp; they do
// nothing to prevent writes on overlapping keys from being written above their
// timestamp. This means that a transaction is not free to blindly move its
// reads to a higher timestamp because writes from other transaction may have
// already invalidated them. In effect, this means that transactions acquire
// pessimistic write locks and optimistic read locks.
//
// The txnSpanRefresher is in charge of detecting when a transaction may want to
// move its provisional commit timestamp forward and determining whether doing
// so is safe given the reads that it has performed (i.e. its "optimistic read
// locks"). When the interceptor decides to attempt to move a transaction's
// timestamp forward, it first "refreshes" each of its reads. This refreshing
// step revisits all of the key spans that the transaction has read and checks
// whether any writes have occurred between the original time that these span
// were read and the timestamp that the transaction now wants to commit at that
// change the result of these reads. If any read would produce a different
// result at the newer commit timestamp, the refresh fails and the transaction
// is forced to fall back to a full transaction restart. However, if all of the
// reads would produce exactly the same result at the newer commit timestamp,
// the timestamp cache entries for these reads are updated and the transaction
// is free to update its provisional commit timestamp without needing to
// restart.
//
// TODO(nvanbenschoten): Unit test this file.
type txnSpanRefresher struct {
	st      *cluster.Settings
	knobs   *ClientTestingKnobs
	wrapped lockedSender
	// Optional; used to condense lock  spans, if provided. If not provided,
	// a transaction's lock refreshSpans may grow without bound.
	ri *RangeIterator
	// refreshSpans contains key spans which were read during the  transaction. In
	// case the transaction's timestamp needs to be pushed, we can avoid a
	// retriable error by "refreshing" these spans: verifying that there have been
	// no changes to their data in between the timestamp at which they were read
	// and the higher timestamp we want to move to.
	refreshSpans condensableSpanSet
	// See TxnCoordMeta.RefreshInvalid.
	refreshInvalid bool
	// refreshInvalidReason refresh机制无效的原因
	refreshInvalidReason RefreshInvalidReason
	// refreshedTimestamp keeps track of the largest timestamp that refreshed
	// don't fail on (i.e. if we'll refresh, we'll refreshFrom timestamp onwards).
	// After every epoch bump, it is initialized to the timestamp of the first
	// batch. It is then bumped after every successful refresh.
	refreshedTimestamp hlc.Timestamp

	// canAutoRetry is set if the txnSpanRefresher is allowed to auto-retry.
	canAutoRetry bool
	// autoRetryCounter counts the number of auto retries which avoid
	// client-side restarts.
	autoRetryCounter *metric.Counter
}

// SendLocked implements the lockedSender interface.
func (sr *txnSpanRefresher) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if ba.Txn.IsolationLevel == util.ReadCommittedIsolation {
		sr.refreshedTimestamp = ba.Txn.ReadTimestamp
		// Send through wrapped lockedSender. Unlocks while sending then re-locks.
		br, pErr := sr.sendLockedWithRefreshAttempts(ctx, ba,
			int(util.MaxTxnRefreshAttempts.Get(&sr.st.SV)))
		if pErr != nil {
			return nil, pErr
		}
		return br, nil
	}
	batchReadTimestamp := ba.Txn.ReadTimestamp
	if sr.refreshedTimestamp.IsEmpty() {
		// This must be the first batch we're sending for this epoch. Future
		// refreshes shouldn't check values below batchReadTimestamp, so initialize
		// sr.refreshedTimestamp.
		sr.refreshedTimestamp = batchReadTimestamp
	} else if batchReadTimestamp.Less(sr.refreshedTimestamp) {
		// sr.refreshedTimestamp might be ahead of batchReadTimestamp. We want to
		// read at the latest refreshed timestamp, so bump the batch.
		// batchReadTimestamp can be behind after a successful refresh, if the
		// TxnCoordSender hasn't actually heard about the updated read timestamp.
		// This can happen if a refresh succeeds, but then the retry of the batch
		// that produced the timestamp fails without returning the update txn (for
		// example, through a canceled ctx). The client should only be sending
		// rollbacks in such cases.
		ba.Txn.ReadTimestamp.Forward(sr.refreshedTimestamp)
		ba.Txn.WriteTimestamp.Forward(sr.refreshedTimestamp)
	} else if sr.refreshedTimestamp != batchReadTimestamp {
		// Sanity check: we're supposed to control the read timestamp. What we're
		// tracking in sr.refreshedTimestamp is not supposed to get out of sync
		// with what batches use (which comes from tc.mu.txn).
		return nil, roachpb.NewError(errors.Errorf(
			"unexpected batch read timestamp: %s. Expected refreshed timestamp: %s. ba: %s",
			batchReadTimestamp, sr.refreshedTimestamp, ba))
	}

	ba.CanForwardRead = sr.canAutoRetry && !sr.refreshInvalid && sr.refreshSpans.empty() && !ba.Txn.CommitTimestampFixed
	if rArgs, hasET := ba.GetArg(roachpb.EndTransaction); hasET {
		et := rArgs.(*roachpb.EndTransactionRequest)
		if et.CanCommitAtHigherTimestamp != ba.CanForwardRead {
			isReissue := et.CanCommitAtHigherTimestamp
			if isReissue {
				etCpy := *et
				ba.Requests[len(ba.Requests)-1].SetInner(&etCpy)
				et = &etCpy
			}
			et.CanCommitAtHigherTimestamp = ba.CanForwardRead
		}
	}

	maxAttempts := int(util.MaxTxnRefreshAttempts.Get(&sr.st.SV))
	if knob := sr.knobs.MaxTxnRefreshAttempts; knob != 0 {
		if knob == -1 {
			maxAttempts = 0
		} else {
			maxAttempts = knob
		}
	}

	// Attempt a refresh before sending the batch.
	ba, pErr := sr.maybeRefreshPreemptively(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	br, pErr := sr.sendLockedWithRefreshAttempts(ctx, ba, maxAttempts)
	if pErr != nil {
		return nil, pErr
	}

	// If the transaction is no longer pending, just return without
	// attempting to record its refresh spans.
	if br.Txn.Status != roachpb.PENDING {
		return br, nil
	}

	// Iterate over and aggregate refresh spans in the requests, qualified by
	// possible resume spans in the responses.
	if !sr.refreshInvalid {
		if err := sr.appendRefreshSpans(ctx, ba, br); err != nil {
			return nil, roachpb.NewError(err)
		}
		// Check whether we should condense the refresh spans.
		maxBytes := MaxTxnRefreshSpansBytes.Get(&sr.st.SV)
		if sr.refreshSpans.bytes >= maxBytes {
			condensedBefore := sr.refreshSpans.condensed
			condensedSufficient := sr.tryCondenseRefreshSpans(ctx, maxBytes)
			if condensedSufficient {
				log.VEventf(ctx, 2, "condensed refresh spans for txn %s to %d bytes",
					br.Txn, sr.refreshSpans.bytes)
			} else {
				// Condensing was not enough. Giving up on tracking reads. Refreshed
				// will not be possible.
				log.VEventf(ctx, 2, "condensed refresh spans didn't save enough memory. txn %s. "+
					"refresh spans after condense: %d bytes",
					br.Txn, sr.refreshSpans.bytes)
				sr.refreshInvalid = true
				sr.refreshInvalidReason = ExceedRefreshMemoryLimit
				sr.refreshSpans.clear()
			}

			if sr.refreshSpans.condensed && !condensedBefore {
				log.Warningf(ctx, "refresh memory limit exceeded")
			}
		}
	}
	return br, nil
}

// maybeRefreshPreemptively attempts to refresh a transaction's read timestamp
// eagerly. Doing so can take advantage of opportunities where the refresh is
// free or can avoid wasting work issuing a batch containing an EndTxn that will
// necessarily throw a serializable error. The method returns a batch with an
// updated transaction if the refresh is successful, or a retry error if not.
func (sr *txnSpanRefresher) maybeRefreshPreemptively(
	ctx context.Context, ba roachpb.BatchRequest,
) (roachpb.BatchRequest, *roachpb.Error) {
	// If we know that the transaction will need a refresh at some point because
	// its write timestamp has diverged from its read timestamp, consider doing
	// so preemptively. We perform a preemptive refresh if either a) doing so
	// would be free because we have not yet accumulated any refresh spans, or
	// b) the batch contains a committing EndTxn request that we know will be
	// rejected if issued.
	//
	// The first case is straightforward. If the transaction has yet to perform
	// any reads but has had its write timestamp bumped, refreshing is a trivial
	// no-op. In this case, refreshing eagerly prevents the transaction for
	// performing any future reads at its current read timestamp. Not doing so
	// preemptively guarantees that we will need to perform a real refresh in
	// the future if the transaction ever performs a read. At best, this would
	// be wasted work. At worst, this could result in the future refresh
	// failing. So we might as well refresh preemptively while doing so is free.
	//
	// Note that this first case here does NOT obviate the need for server-side
	// refreshes. Notably, a transaction's write timestamp might be bumped in
	// the same batch in which it performs its first read. In such cases, a
	// preemptive refresh would not be needed but a reactive refresh would not
	// be a trivial no-op. These situations are common for one-phase commit
	// transactions.
	//
	// The second case is more complex. If the batch contains a committing
	// EndTxn request that we know will need a refresh, we don't want to bother
	// issuing it just for it to be rejected. Instead, preemptively refresh
	// before issuing the EndTxn batch. If we view reads as acquiring a form of
	// optimistic read locks under an optimistic concurrency control scheme (as
	// is discussed in the comment on txnSpanRefresher) then this preemptive
	// refresh immediately before the EndTxn is synonymous with the "validation"
	// phase of a standard OCC transaction model. However, as an optimization
	// compared to standard OCC, the validation phase is only performed when
	// necessary in CockroachDB (i.e. if the transaction's writes have been
	// pushed to higher timestamps).
	//
	// TODO(andrei): whether or not we can still auto-retry at the SQL level
	// should also play a role in deciding whether we want to refresh eagerly or
	// not.

	// If the transaction has yet to be pushed, no refresh is necessary.
	if ba.Txn.ReadTimestamp == ba.Txn.WriteTimestamp {
		return ba, nil
	}

	// If true, this batch is guaranteed to fail without a refresh.
	args, hasET := ba.GetArg(roachpb.EndTransaction)
	refreshInevitable := hasET && args.(*roachpb.EndTransactionRequest).Commit

	// If neither condition is true, defer the refresh.
	if !ba.CanForwardRead && !refreshInevitable {
		return ba, nil
	}

	canRefreshTxn, refreshTxn := roachpb.PrepareTransactionForRefresh(ba.Txn, ba.Txn.WriteTimestamp)
	if !canRefreshTxn || !sr.canAutoRetry {
		return roachpb.BatchRequest{}, newRetryErrorOnFailedPreemptiveRefresh(ba.Txn)
	}
	log.VEventf(ctx, 2, "preemptively refreshing to timestamp %s before issuing %s",
		refreshTxn.ReadTimestamp, ba)

	if sr.refreshInvalid && sr.refreshInvalidReason == ExceedRefreshMemoryLimit {
		maxBytes := MaxTxnRefreshSpansBytes.Get(&sr.st.SV)
		return roachpb.BatchRequest{}, newAbortErrorOnExceedRefreshLimitMemory(ba.Txn, maxBytes)
	}

	// Try updating the txn spans at a timestamp that will allow us to commit.
	if ok := sr.tryUpdatingTxnSpans(ctx, refreshTxn); !ok {
		log.Eventf(ctx, "preemptive refresh failed; propagating retry error")
		return roachpb.BatchRequest{}, newRetryErrorOnFailedPreemptiveRefresh(ba.Txn)
	}

	log.Eventf(ctx, "preemptive refresh succeeded")
	ba.UpdateTxn(refreshTxn)
	return ba, nil
}

func newAbortErrorOnExceedRefreshLimitMemory(
	txn *roachpb.Transaction, maxBytes int64,
) *roachpb.Error {
	msg := fmt.Sprintf("exceed refresh limit memory:%v bytes, increase refresh limit memory or reduce the number of operating records", maxBytes)
	e := errors.Errorf(msg)
	err := roachpb.NewErrorWithTxn(e, txn)
	err.TransactionRestart = roachpb.TransactionRestart_NONE
	return err
}

func newRetryErrorOnFailedPreemptiveRefresh(txn *roachpb.Transaction) *roachpb.Error {
	reason := roachpb.RETRY_SERIALIZABLE
	if txn.WriteTooOld {
		reason = roachpb.RETRY_WRITE_TOO_OLD
	}
	err := roachpb.NewTransactionRetryError(reason, "failed preemptive refresh")
	return roachpb.NewErrorWithTxn(err, txn)
}

// sendLockedWithRefreshAttempts sends the batch through the wrapped sender. It
// catches serializable errors and attempts to avoid them by refreshing the txn
// at a larger timestamp. It returns the response, an error, and the largest
// timestamp that the request was able to refresh at.
func (sr *txnSpanRefresher) sendLockedWithRefreshAttempts(
	ctx context.Context, ba roachpb.BatchRequest, maxRefreshAttempts int,
) (_ *roachpb.BatchResponse, _ *roachpb.Error) {
	if ba.Txn.WriteTooOld && sr.canAutoRetry {
		// The WriteTooOld flag is not supposed to be set on requests. It's only set
		// by the server and it's terminated by this interceptor on the client.
		log.Fatalf(ctx, "unexpected WriteTooOld request. ba: %s (txn: %s)",
			ba.String(), ba.Txn.String())
	}
	br, pErr := sr.wrapped.SendLocked(ctx, ba)
	// 19.2 servers might give us an error with the WriteTooOld flag set. This
	// interceptor wants to always terminate that flag. In the case of an error,
	// we can just ignore it.
	if pErr != nil && pErr.GetTxn() != nil {
		pErr.GetTxn().WriteTooOld = false
	}
	if pErr == nil && br.Txn.WriteTooOld {
		// If we got a response with the WriteTooOld flag set, then we pretend that
		// we got a WriteTooOldError, which will cause us to attempt to refresh and
		// propagate the error if we failed. When it can, the server prefers to
		// return the WriteTooOld flag, rather than a WriteTooOldError because, in
		// the former case, it can leave intents behind. We like refreshing eagerly
		// when the WriteTooOld flag is set because it's likely that the refresh
		// will fail (if we previously read the key that's now causing a WTO, then
		// the refresh will surely fail).
		// TODO(andrei): Implement a more discerning policy based on whether we've
		// read that key before.
		//
		// If the refresh fails, we could continue running the transaction even
		// though it will not be able to commit, in order for it to lay down more
		// intents. Not doing so, though, gives the SQL a chance to auto-retry.
		// TODO(andrei): Implement a more discerning policy based on whether
		// auto-retries are still possible.
		//
		// For the refresh, we have two options: either refresh everything read
		// *before* this batch, and then retry this batch, or refresh the current
		// batch's reads too and then, if successful, there'd be nothing to refresh.
		// We take the former option by setting br = nil below to minimized the
		// chances that the refresh fails.
		bumpedTxn := br.Txn.Clone()
		bumpedTxn.WriteTooOld = false
		// bumpedTxn.ReadTimestamp = bumpedTxn.WriteTimestamp
		pErr = roachpb.NewErrorWithTxn(
			roachpb.NewTransactionRetryError(roachpb.RETRY_WRITE_TOO_OLD,
				"WriteTooOld flag converted to WriteTooOldError"),
			bumpedTxn)
		br = nil
	}

	if pErr != nil && maxRefreshAttempts > 0 {
		br, pErr = sr.maybeRetrySend(ctx, ba, br, pErr, maxRefreshAttempts)
	}
	sr.forwardRefreshTimestampOnResponse(br, pErr)
	return br, pErr
}

// forwardRefreshTimestampOnResponse updates the refresher's tracked
// refreshedTimestamp to stay in sync with "server-side refreshes", where the
// transaction's read timestamp is updated during the evaluation of a batch.
func (sr *txnSpanRefresher) forwardRefreshTimestampOnResponse(
	br *roachpb.BatchResponse, pErr *roachpb.Error,
) {
	var txn *roachpb.Transaction
	if pErr != nil {
		txn = pErr.GetTxn()
	} else {
		txn = br.Txn
	}
	if txn != nil {
		sr.refreshedTimestamp.Forward(txn.ReadTimestamp)
	}
}

// maybeRetrySend attempts to catch serializable errors and avoid them by
// refreshing the txn at a larger timestamp. If it succeeds at refreshing the
// txn timestamp, it recurses into sendLockedWithRefreshAttempts and retries the
// suffix of the original batch that has not yet completed successfully.
func (sr *txnSpanRefresher) maybeRetrySend(
	ctx context.Context,
	ba roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
	maxRefreshAttempts int,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Check for an error which can be retried after updating spans.
	canRetryTxn, retryTxn := roachpb.CanTransactionRetryAtRefreshedTimestamp(ctx, pErr)
	if !canRetryTxn || !sr.canAutoRetry {
		return nil, pErr
	}
	// 获取EndTransactionRequest，并给其SplitEt标志设置成True
	lastReq := ba.Requests[len(ba.Requests)-1].GetInner()
	if et, ok := lastReq.(*roachpb.EndTransactionRequest); ok && canRetryTxn {
		et.Retrytxn = true
	}
	// If a prefix of the batch was executed, collect refresh spans for
	// that executed portion, and retry the remainder. The canonical
	// case is a batch split between everything up to but not including
	// the EndTransaction. Requests up to the EndTransaction succeed,
	// but the EndTransaction fails with a retryable error. We want to
	// retry only the EndTransaction.
	ba.UpdateTxn(retryTxn)
	retryBa := ba
	if br != nil {
		doneBa := ba
		doneBa.Requests = ba.Requests[:len(br.Responses)]
		log.VEventf(ctx, 2, "collecting refresh spans after partial batch execution of %s", doneBa)
		if err := sr.appendRefreshSpans(ctx, doneBa, br); err != nil {
			return nil, roachpb.NewError(err)
		}
		retryBa.Requests = ba.Requests[len(br.Responses):]
	}

	log.VEventf(ctx, 2, "retrying %s at refreshed timestamp %s because of %s",
		retryBa, retryTxn.ReadTimestamp, pErr)

	// Try updating the txn spans so we can retry.
	if ok := sr.tryUpdatingTxnSpans(ctx, retryTxn); !ok {
		return nil, pErr
	}

	// We've refreshed all of the read spans successfully and set
	// newBa.Txn.ReadTimestamp to the current timestamp. Submit the
	// batch again.
	retryBr, retryErr := sr.sendLockedWithRefreshAttempts(
		ctx, retryBa, maxRefreshAttempts-1,
	)
	if retryErr != nil {
		log.VEventf(ctx, 2, "retry failed with %s", retryErr)
		return nil, retryErr
	}

	log.VEventf(ctx, 2, "retry successful @%s", retryBa.Txn.WriteTimestamp)
	sr.autoRetryCounter.Inc(1)

	// On success, combine responses if applicable and set error to nil.
	if br != nil {
		br.Responses = append(br.Responses, retryBr.Responses...)
		retryBr.CollectedSpans = append(br.CollectedSpans, retryBr.CollectedSpans...)
		br.BatchResponse_Header = retryBr.BatchResponse_Header
	} else {
		br = retryBr
	}
	return br, nil
}

// tryUpdatingTxnSpans sends Refresh and RefreshRange commands to all spans read
// during the transaction to ensure that no writes were written more recently
// than sr.refreshedTimestamp. All implicated timestamp caches are updated with
// the final transaction timestamp. Returns whether the refresh was successful
// or not.
func (sr *txnSpanRefresher) tryUpdatingTxnSpans(
	ctx context.Context, refreshTxn *roachpb.Transaction,
) bool {
	if sr.refreshInvalid {
		log.VEvent(ctx, 2, "can't refresh txn spans; not valid")
		return false
	} else if sr.refreshSpans.empty() {
		log.VEvent(ctx, 2, "there are no txn spans to refresh")
		sr.refreshedTimestamp.Forward(refreshTxn.ReadTimestamp)
		return true
	}

	// Refresh all spans (merge first).
	refreshSpanBa := roachpb.BatchRequest{}
	refreshSpanBa.Txn = refreshTxn
	addRefreshes := func(refreshes *condensableSpanSet) {
		// We're going to check writes between the previous refreshed timestamp, if
		// any, and the timestamp we want to bump the transaction to. Note that if
		// we've already refreshed the transaction before, we don't need to check
		// the (key ranges x timestamp range) that we've already checked - there's
		// no values there for sure.
		// More importantly, reads that have happened since we've previously
		// refreshed don't need to be checked below below the timestamp at which
		// they've been read (which is the timestamp to which we've previously
		// refreshed). Checking below that timestamp (like we would, for example, if
		// we simply used txn.OrigTimestamp here), could cause false-positives that
		// would fail the refresh.
		for _, u := range refreshes.asSlice() {
			var req roachpb.Request
			if len(u.EndKey) == 0 {
				req = &roachpb.RefreshRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(u),
					RefreshFrom:   sr.refreshedTimestamp,
				}
			} else {
				req = &roachpb.RefreshRangeRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(u),
					RefreshFrom:   sr.refreshedTimestamp,
				}
			}
			refreshSpanBa.Add(req)
			log.VEventf(ctx, 2, "updating span %s @%s - @%s to avoid serializable restart",
				req.Header().Span(), sr.refreshedTimestamp, refreshTxn.WriteTimestamp)
		}
	}

	addRefreshes(&sr.refreshSpans)

	if _, batchErr := sr.wrapped.SendLocked(ctx, refreshSpanBa); batchErr != nil {
		log.VEventf(ctx, 2, "failed to refresh txn spans (%s); propagating original retry error", batchErr)
		return false
	}

	sr.refreshedTimestamp.Forward(refreshTxn.ReadTimestamp)
	return true
}

// appendRefreshSpans appends refresh spans from the supplied batch request,
// qualified by the batch response where appropriate.
func (sr *txnSpanRefresher) appendRefreshSpans(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) error {
	batchTimestamp := br.Txn.ReadTimestamp
	if batchTimestamp.Less(sr.refreshedTimestamp) {
		// This can happen with (illegal) concurrent txn use, but that's supposed to
		// be detected by the gatekeeper interceptor.
		return errors.Errorf("attempting to append refresh spans after the tracked"+
			" timestamp has moved forward. batchTimestamp: %s refreshedTimestamp: %s ba: %s",
			batchTimestamp, sr.refreshedTimestamp, ba)
	}

	ba.RefreshSpanIterate(br, func(span roachpb.Span) {
		log.VEventf(ctx, 3, "recording span to refresh: %s", span)
		sr.refreshSpans.insert(span)
	})

	return nil
}

// setWrapped implements the txnInterceptor interface.
func (sr *txnSpanRefresher) setWrapped(wrapped lockedSender) { sr.wrapped = wrapped }

// populateMetaLocked implements the txnInterceptor interface.
func (sr *txnSpanRefresher) populateMetaLocked(meta *roachpb.TxnCoordMeta) {
	meta.RefreshInvalid = sr.refreshInvalid
	if !sr.refreshInvalid {
		// Copy mutable state so access is safe for the caller.
		meta.RefreshSpans = append([]roachpb.Span(nil), sr.refreshSpans.asSlice()...)
	}
}

// augmentMetaLocked implements the txnInterceptor interface.
func (sr *txnSpanRefresher) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
	// Do not modify existing span slices when copying.
	if meta.RefreshInvalid {
		sr.refreshInvalid = true
		sr.refreshSpans.clear()
	} else if !sr.refreshInvalid {
		sr.refreshSpans.insert(meta.RefreshSpans...)
	}
}

// epochBumpedLocked implements the txnInterceptor interface.
func (sr *txnSpanRefresher) epochBumpedLocked() {
	sr.refreshSpans.clear()
	sr.refreshInvalid = false
	sr.refreshedTimestamp.Reset()
}

// createSavepointLocked is part of the txnReqInterceptor interface.
func (sr *txnSpanRefresher) createSavepointLocked(ctx context.Context, s *savepoint) {
	s.refreshSpans = make([]roachpb.Span, len(sr.refreshSpans.asSlice()))
	copy(s.refreshSpans, sr.refreshSpans.asSlice())
	s.refreshInvalid = sr.refreshInvalid
}

// rollbackToSavepointLocked is part of the txnReqInterceptor interface.
func (sr *txnSpanRefresher) rollbackToSavepointLocked(ctx context.Context, s savepoint) {
	sr.refreshSpans.clear()
	sr.refreshSpans.insert(s.refreshSpans...)
	sr.refreshInvalid = s.refreshInvalid
}

// closeLocked implements the txnInterceptor interface.
func (*txnSpanRefresher) closeLocked() {}

// tryCondenseRefreshSpans attempts to condense the refresh spans in order to
// save memory. Returns true if we managed to condense them below maxBytes.
func (sr *txnSpanRefresher) tryCondenseRefreshSpans(ctx context.Context, maxBytes int64) bool {
	if sr.knobs.CondenseRefreshSpansFilter != nil && !sr.knobs.CondenseRefreshSpansFilter() {
		return false
	}
	sr.refreshSpans.maybeCondense(ctx, sr.ri, maxBytes)
	return sr.refreshSpans.bytes < maxBytes
}

// getRefresherSpans get interceptor Refresher span
func (sr *txnSpanRefresher) getRefresherSpans() []roachpb.Span {
	spans := sr.refreshSpans.s
	combinedSpan := combineSpan(spans)
	return combinedSpan
}

//combineSpan 区间合并
func combineSpan(spanSet []roachpb.Span) []roachpb.Span {
	if len(spanSet) <= 1 {
		return spanSet
	}
	sort.SliceStable(spanSet, func(i, j int) bool {
		if spanSet[i].Key.Compare(spanSet[j].Key) <= 0 {
			return true
		}
		return false
	})
	res := make([]roachpb.Span, 0)
	swap := roachpb.Span{}
	for k, v := range spanSet {
		if k == 0 {
			swap = v
			continue
		}
		if v.Key.Compare(swap.EndKey) <= 0 {
			if v.EndKey.Compare(swap.EndKey) > 0 {
				swap.EndKey = v.EndKey
			}
		} else {
			res = append(res, swap)
			swap = v
		}
	}
	res = append(res, swap)
	return res
}
