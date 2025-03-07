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

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/storage/closedts"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/intentresolver"
	"github.com/znbasedb/znbase/pkg/storage/rangefeed"
	"github.com/znbasedb/znbase/pkg/storage/storagepb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// RangefeedEnabled is a cluster setting that enables rangefeed requests.
var RangefeedEnabled = settings.RegisterBoolSetting(
	"kv.rangefeed.enabled",
	"if set, rangefeed registration is enabled",
	false,
)

// lockedRangefeedStream is an implementation of rangefeed.Stream which provides
// support for concurrent calls to Send. Note that the default implementation of
// grpc.Stream is not safe for concurrent calls to Send.
type lockedRangefeedStream struct {
	wrapped roachpb.Internal_RangeFeedServer
	sendMu  syncutil.Mutex
}

func (s *lockedRangefeedStream) Context() context.Context {
	return s.wrapped.Context()
}

func (s *lockedRangefeedStream) Send(e *roachpb.RangeFeedEvent) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(e)
}

// rangefeedTxnPusher is a shim around intentResolver that implements the
// rangefeed.TxnPusher interface.
type rangefeedTxnPusher struct {
	ir *intentresolver.IntentResolver
	r  *Replica
}

// PushTxns is part of the rangefeed.TxnPusher interface. It performs a
// high-priority push at the specified timestamp to each of the specified
// transactions.
func (tp *rangefeedTxnPusher) PushTxns(
	ctx context.Context, txns []enginepb.TxnMeta, ts hlc.Timestamp,
) ([]*roachpb.Transaction, error) {
	pushTxnMap := make(map[uuid.UUID]*enginepb.TxnMeta, len(txns))
	for i := range txns {
		txn := &txns[i]
		pushTxnMap[txn.ID] = txn
	}

	h := roachpb.Header{
		Timestamp: ts,
		Txn: &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Priority: enginepb.MaxTxnPriority,
			},
		},
	}

	pushedTxnMap, pErr := tp.ir.MaybePushTransactions(
		ctx, pushTxnMap, h, roachpb.PUSH_TIMESTAMP, false, /* skipIfInFlight */
	)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	pushedTxns := make([]*roachpb.Transaction, 0, len(pushedTxnMap))
	for _, txn := range pushedTxnMap {
		pushedTxns = append(pushedTxns, txn)
	}
	return pushedTxns, nil
}

// ResolveIntents is part of the rangefeed.TxnPusher interface.
func (tp *rangefeedTxnPusher) ResolveIntents(
	ctx context.Context, intents []roachpb.LockUpdate, backFill bool,
) error {
	return tp.ir.ResolveIntents(ctx, intents,
		// NB: Poison is ignored for non-ABORTED intents.
		intentresolver.ResolveOptions{Poison: true, BackFill: backFill},
	).GoError()
}

type iteratorWithCloser struct {
	engine.SimpleIterator
	close func()
}

func (i iteratorWithCloser) Close() {
	i.SimpleIterator.Close()
	i.close()
}

// RangeFeed registers a rangefeed over the specified span. It sends updates to
// the provided stream and returns with an optional error when the rangefeed is
// complete. The provided ConcurrentRequestLimiter is used to limit the number
// of rangefeeds using catchup iterators at the same time.
func (r *Replica) RangeFeed(
	args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) *roachpb.Error {
	if !RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
		return roachpb.NewErrorf("rangefeeds require the kv.rangefeed.enabled setting. See " +
			base.DocsURL(`change-data-capture.html#enable-rangefeeds-to-reduce-latency`))
	}
	ctx := r.AnnotateCtx(stream.Context())

	var rSpan roachpb.RSpan
	var err error
	var filter hlc.Timestamp
	if args.Filter != nil {
		filter = *args.Filter
	}
	rSpan.Key, err = keys.Addr(args.Span.Key)
	if err != nil {
		return roachpb.NewError(err)
	}
	rSpan.EndKey, err = keys.Addr(args.Span.EndKey)
	if err != nil {
		return roachpb.NewError(err)
	}

	if err := r.ensureClosedTimestampStarted(ctx); err != nil {
		return err
	}

	// If the RangeFeed is performing a catch-up scan then it will observe all
	// values above args.Timestamp. If the RangeFeed is requesting previous
	// values for every update then it will also need to look for the version
	// proceeding each value observed during the catch-up scan timestamp. This
	// means that the earliest value observed by the catch-up scan will be
	// args.Timestamp.Next and the earliest timestamp used to retrieve the
	// previous version of a value will be args.Timestamp, so this is the
	// timestamp we must check against the GCThreshold.
	checkTS := args.Timestamp
	if checkTS.IsEmpty() {
		// If no timestamp was provided then we're not going to run a catch-up
		// scan, so make sure the GCThreshold in requestCanProceed succeeds.
		checkTS = r.Clock().Now()
	}

	lockedStream := &lockedRangefeedStream{wrapped: stream}
	errC := make(chan *roachpb.Error, 1)

	// If we will be using a catch-up iterator, wait for the limiter here before
	// locking raftMu.
	usingCatchupIter := false
	var iterSemRelease func()
	if !args.Timestamp.IsEmpty() {
		usingCatchupIter = true
		lim := &r.store.limiters.ConcurrentRangefeedIters
		if err := lim.Begin(ctx); err != nil {
			return roachpb.NewError(err)
		}
		// Finish the iterator limit, but only if we exit before
		// creating the iterator itself.
		iterSemRelease = lim.Finish
		defer func() {
			if iterSemRelease != nil {
				iterSemRelease()
			}
		}()
	}

	// Lock the raftMu, then register the stream as a new rangefeed registration.
	// raftMu is held so that the catch-up iterator is captured in the same
	// critical-section as the registration is established. This ensures that
	// the registration doesn't miss any events.
	r.raftMu.Lock()
	if err := r.checkExecutionCanProceedForRangeFeed(rSpan, checkTS); err != nil {
		r.raftMu.Unlock()
		return roachpb.NewError(err)
	}

	// Ensure that the rangefeed processor is running.
	//开启CDC feed Event
	p := r.maybeInitRangefeedRaftMuLocked(ctx, filter)
	// Register the stream with a catch-up iterator.
	var catchUpIter engine.SimpleIterator
	if usingCatchupIter {
		innerIter := r.Engine().NewIterator(engine.IterOptions{
			UpperBound: args.Span.EndKey,
			// RangeFeed originally intended to use the time-bound iterator
			// performance optimization. However, they've had correctness issues in
			// the past (#28358, #34819) and no-one has the time for the due-diligence
			// necessary to be confidant in their correctness going forward. Not using
			// them causes the total time spent in RangeFeed catchup on changefeed
			// over tpcc-1000 to go from 40s -> 4853s, which is quite large but still
			// workable. See #35122 for details.
			// MinTimestampHint: args.Timestamp,
		})
		catchUpIter = iteratorWithCloser{
			SimpleIterator: innerIter,
			close:          iterSemRelease,
		}
		// Responsibility for releasing the semaphore now passes to the iterator.
		iterSemRelease = nil
	}
	p.Register(rSpan, args.Timestamp, catchUpIter, args.WithDiff, lockedStream, errC)
	r.raftMu.Unlock()

	// When this function returns, attempt to clean up the rangefeed.
	defer func() {
		r.raftMu.Lock()
		r.maybeDestroyRangefeedRaftMuLocked(p)
		r.raftMu.Unlock()
	}()

	// Block on the registration's error channel. Note that the registration
	// observes stream.Context().Done.
	return <-errC
}

// The size of an event is 112 bytes, so this will result in an allocation on
// the order of ~512KB per RangeFeed. That's probably ok given the number of
// ranges on a node that we'd like to support with active rangefeeds, but it's
// certainly on the upper end of the range.
//
// TODO(dan): Everyone seems to agree that this memory limit would be better set
// at a store-wide level, but there doesn't seem to be an easy way to accomplish
// that.
const defaultEventChanCap = 4096

// maybeInitRangefeedRaftMuLocked initializes a rangefeed for the Replica if one
// is not already running. Requires raftMu be locked.
func (r *Replica) maybeInitRangefeedRaftMuLocked(
	ctx context.Context, filter hlc.Timestamp,
) *rangefeed.Processor {
	if r.raftMu.rangefeed != nil {
		return r.raftMu.rangefeed
	}

	// Create a new rangefeed.
	desc := r.mu.state.Desc
	tp := rangefeedTxnPusher{ir: r.store.intentResolver, r: r}
	cfg := rangefeed.Config{
		AmbientContext:   r.AmbientContext,
		Clock:            r.Clock(),
		Span:             desc.RSpan(),
		TxnPusher:        &tp,
		EventChanCap:     defaultEventChanCap,
		EventChanTimeout: 50 * time.Millisecond,
		Metrics:          r.store.metrics.RangeFeedMetrics,
	}
	r.raftMu.rangefeed = rangefeed.NewProcessor(cfg)
	r.raftMu.rangefeed.FilterEvents = make([]rangefeed.BackfillEvent, 1)
	r.raftMu.rangefeed.FilterEvents[0] = rangefeed.BackfillEvent{
		Ts:       filter,
		Backfill: true,
	}
	r.store.addReplicaWithRangefeed(r.RangeID)

	// Start it with an iterator to initialize the resolved timestamp.
	rtsIter := r.Engine().NewIterator(engine.IterOptions{
		UpperBound: desc.EndKey.AsRawKey(),
		// TODO(nvanbenschoten): To facilitate fast restarts of rangefeed
		// we should periodically persist the resolved timestamp so that we
		// can initialize the rangefeed using an iterator that only needs to
		// observe timestamps back to the last recorded resolved timestamp.
		// This is safe because we know that there are no unresolved intents
		// at times before a resolved timestamp.
		// MinTimestampHint: r.ResolvedTimestamp,
	})
	r.raftMu.rangefeed.Start(r.store.Stopper(), rtsIter)

	// Check for an initial closed timestamp update immediately to help
	// initialize the rangefeed's resolved timestamp as soon as possible.
	r.handleClosedTimestampUpdateRaftMuLocked(ctx)

	return r.raftMu.rangefeed
}

func (r *Replica) resetRangefeedRaftMuLocked() {
	r.raftMu.rangefeed = nil
	r.store.removeReplicaWithRangefeed(r.RangeID)
}

// maybeDestroyRangefeedRaftMuLocked tears down the provided Processor if it is
// still active and if it no longer has any registrations. Requires raftMu to be
// locked.
func (r *Replica) maybeDestroyRangefeedRaftMuLocked(p *rangefeed.Processor) {
	if r.raftMu.rangefeed != p {
		return
	}
	if r.raftMu.rangefeed.Len() == 0 {
		r.raftMu.rangefeed.Stop()
		r.resetRangefeedRaftMuLocked()
	}
}

// disconnectRangefeedWithErrRaftMuLocked broadcasts the provided error to all
// rangefeed registrations and tears down the active rangefeed Processor. No-op
// if a rangefeed is not active. Requires raftMu to be locked.
func (r *Replica) disconnectRangefeedWithErrRaftMuLocked(pErr *roachpb.Error) {
	if r.raftMu.rangefeed == nil {
		return
	}
	r.raftMu.rangefeed.StopWithErr(pErr)
	r.resetRangefeedRaftMuLocked()
}

// disconnectRangefeedWithReasonRaftMuLocked broadcasts the provided rangefeed
// retry reason to all rangefeed registrations and tears down the active
// rangefeed Processor. No-op if a rangefeed is not active. Requires raftMu to
// be locked.
func (r *Replica) disconnectRangefeedWithReasonRaftMuLocked(
	reason roachpb.RangeFeedRetryError_Reason,
) {
	if r.raftMu.rangefeed == nil {
		return
	}
	filter := r.raftMu.rangefeed.FilterEvents
	var backfills []*roachpb.RangeFeedRetryError_BackFill
	for _, event := range filter {
		backfills = append(backfills, &roachpb.RangeFeedRetryError_BackFill{TxnID: event.TxnID, Timestamp: event.Ts, BackFill: &event.Backfill})
	}
	pErr := roachpb.NewError(roachpb.NewRangeFeedRetryError(reason, backfills))
	r.disconnectRangefeedWithErrRaftMuLocked(pErr)
}

// populatePrevValsInLogicalOpLogRaftMuLocked updates the provided logical op
// log with previous values read from the reader, which is expected to reflect
// the state of the Replica before the operations in the logical op log are
// applied. No-op if a rangefeed is not active. Requires raftMu to be locked.
func (r *Replica) populatePrevValsInLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *storagepb.LogicalOpLog, prevReader engine.Reader,
) {
	if r.raftMu.rangefeed == nil {
		return
	}
	if ops == nil {
		return
	}
	// Read from the Reader to populate the PrevValue fields.
	for _, op := range ops.Ops {
		var key []byte
		var ts hlc.Timestamp
		var prevValPtr *[]byte
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			key, ts, prevValPtr = t.Key, t.Timestamp, &t.PrevValue
		case *enginepb.MVCCCommitIntentOp:
			key, ts, prevValPtr = t.Key, t.Timestamp, &t.PrevValue
		case *enginepb.MVCCWriteIntentOp,
			*enginepb.MVCCUpdateIntentOp,
			*enginepb.MVCCAbortIntentOp,
			*enginepb.MVCCABackFillOp,
			*enginepb.MVCCAbortTxnOp:
			// Nothing to do.
			continue
		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		//// Don't read previous values from the reader for operations that are
		//// not needed by any rangefeed registration.
		//if !filter.NeedPrevVal(roachpb.Span{Key: key}) {
		//	continue
		//}

		// Read the previous value from the prev Reader. Unlike the new value
		// (see handleLogicalOpLogRaftMuLocked), this one may be missing.
		prevVal, _, err := engine.MVCCGet(
			ctx, prevReader, key, ts, engine.MVCCGetOptions{Tombstones: true, Inconsistent: true},
		)
		if err != nil {
			r.disconnectRangefeedWithErrRaftMuLocked(roachpb.NewErrorf(
				"error consuming %T for key %v @ ts %v: %v", op, key, ts, err,
			))
			return
		}
		if prevVal != nil {
			*prevValPtr = prevVal.RawBytes
		} else {
			*prevValPtr = nil
		}
	}
}

// handleLogicalOpLogRaftMuLocked passes the logical op log to the active
// rangefeed, if one is running. The method accepts a reader, which is used to
// look up the values associated with key-value writes in the log before handing
// them to the rangefeed processor. No-op if a rangefeed is not active. Requires
func (r *Replica) handleLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *storagepb.LogicalOpLog, reader engine.Reader,
) {
	if r.raftMu.rangefeed == nil {
		return
	}
	if ops == nil {
		// Rangefeeds can't be turned on unless RangefeedEnabled is set to true,
		// after which point new Raft proposals will include logical op logs.
		// However, there's a race present where old Raft commands without a
		// logical op log might be passed to a rangefeed. Since the effect of
		// these commands was not included in the catch-up scan of current
		// registrations, we're forced to throw an error. The rangefeed clients
		// can reconnect at a later time, at which point all new Raft commands
		// should have logical op logs.
		r.disconnectRangefeedWithReasonRaftMuLocked(
			roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING,
		)
		return
	}
	if len(ops.Ops) == 0 {
		return
	}

	// When reading straight from the Raft log, some logical ops will not be
	// fully populated. Read from the Reader to populate all fields.
	for _, op := range ops.Ops {
		var key []byte
		var ts hlc.Timestamp
		var valPtr *[]byte
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCCommitIntentOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCWriteIntentOp,
			*enginepb.MVCCUpdateIntentOp,
			*enginepb.MVCCAbortIntentOp,
			*enginepb.MVCCABackFillOp,
			*enginepb.MVCCAbortTxnOp:
			// Nothing to do.
			continue
		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Read the value directly from the Reader. This is performed in the
		// same raftMu critical section that the logical op's corresponding
		// WriteBatch is applied, so the value should exist.
		val, _, err := engine.MVCCGet(ctx, reader, key, ts, engine.MVCCGetOptions{Tombstones: true})
		if val == nil && err == nil {
			err = errors.New("value missing in reader")
		}
		if err != nil {
			r.disconnectRangefeedWithErrRaftMuLocked(roachpb.NewErrorf(
				"error consuming %T for key %v @ ts %v: %v", op, key, ts, err,
			))
			return
		}
		*valPtr = val.RawBytes
	}

	// Pass the ops to the rangefeed processor.
	if !r.raftMu.rangefeed.ConsumeLogicalOps(ops.Ops...) {
		// Consumption failed and the rangefeed was stopped.
		r.resetRangefeedRaftMuLocked()
	}
}

// handleClosedTimestampUpdate determines the current maximum closed timestamp
// for the replica and informs the rangefeed, if one is running. No-op if a
// rangefeed is not active.
func (r *Replica) handleClosedTimestampUpdate(ctx context.Context) {
	ctx = r.AnnotateCtx(ctx)
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.handleClosedTimestampUpdateRaftMuLocked(ctx)
}

// handleClosedTimestampUpdateRaftMuLocked is like handleClosedTimestampUpdate,
// but it requires raftMu to be locked.
func (r *Replica) handleClosedTimestampUpdateRaftMuLocked(ctx context.Context) {
	if r.raftMu.rangefeed == nil {
		return
	}

	// Determine what the maximum closed timestamp is for this replica.
	closedTS := r.maxClosed(ctx)

	// If the closed timestamp is sufficiently stale, signal that we want an
	// update to the leaseholder so that it will eventually begin to progress
	// again.
	slowClosedTSThresh := 5 * closedts.TargetDuration.Get(&r.store.cfg.Settings.SV)
	if d := timeutil.Since(closedTS.GoTime()); d > slowClosedTSThresh {
		m := r.store.metrics.RangeFeedMetrics
		if m.RangeFeedSlowClosedTimestampLogN.ShouldLog() {
			if closedTS.IsEmpty() {
				log.Infof(ctx, "RangeFeed closed timestamp is empty")
			} else {
				log.Infof(ctx, "RangeFeed closed timestamp %s is behind by %s", closedTS, d)
			}
		}

		// Asynchronously attempt to nudge the closed timestamp in case it's stuck.
		key := fmt.Sprintf(`rangefeed-slow-closed-timestamp-nudge-r%d`, r.RangeID)
		// Ignore the result of DoChan since, to keep this all async, it always
		// returns nil and any errors are logged by the closure passed to the
		// `DoChan` call.
		_, _ = m.RangeFeedSlowClosedTimestampNudge.DoChan(key, func() (interface{}, error) {
			// Also ignore the result of RunTask, since it only returns errors when
			// the task didn't start because we're shutting down.
			_ = r.store.stopper.RunTask(ctx, key, func(context.Context) {
				// Limit the amount of work this can suddenly spin up. In particular,
				// this is to protect against the case of a system-wide slowdown on
				// closed timestamps, which would otherwise potentially launch a huge
				// number of lease acquisitions all at once.
				select {
				case <-ctx.Done():
					// Don't need to do this anymore.
					return
				case m.RangeFeedSlowClosedTimestampNudgeSem <- struct{}{}:
				}
				defer func() { <-m.RangeFeedSlowClosedTimestampNudgeSem }()
				if err := r.ensureClosedTimestampStarted(ctx); err != nil {
					log.Infof(ctx, `RangeFeed failed to nudge: %s`, err)
				}
			})
			return nil, nil
		})
	}

	// If the closed timestamp is not empty, inform the Processor.
	if closedTS.IsEmpty() {
		return
	}
	if !r.raftMu.rangefeed.ForwardClosedTS(closedTS) {
		// Consumption failed and the rangefeed was stopped.
		r.resetRangefeedRaftMuLocked()
	}
}

// ensureClosedTimestampStarted does its best to make sure that this node is
// receiving closed timestamp updated for this replica's range. Note that this
// forces a lease to exist somewhere and so is reasonably expensive.
func (r *Replica) ensureClosedTimestampStarted(ctx context.Context) *roachpb.Error {
	// Make sure there's a leaseholder. If there's no leaseholder, there's no
	// closed timestamp updates.
	var leaseholderNodeID roachpb.NodeID
	_, err := r.redirectOnOrAcquireLease(ctx)
	if err == nil {
		// We have the lease. Request is essentially a wrapper for calling EmitMLAI
		// on a remote node, so cut out the middleman.
		r.EmitMLAI()
		return nil
	} else if lErr, ok := err.GetDetail().(*roachpb.NotLeaseHolderError); ok {
		if lErr.LeaseHolder == nil {
			// It's possible for redirectOnOrAcquireLease to return
			// NotLeaseHolderErrors with LeaseHolder unset, but these should be
			// transient conditions. If this method is being called by RangeFeed to
			// nudge a stuck closedts, then essentially all we can do here is nothing
			// and assume that redirectOnOrAcquireLease will do something different
			// the next time it's called.
			return nil
		}
		leaseholderNodeID = lErr.LeaseHolder.NodeID
	} else {
		return err
	}
	// Request fixes any issues where we've missed a closed timestamp update or
	// where we're not connected to receive them from this node in the first
	// place.
	r.store.cfg.ClosedTimestamp.Clients.Request(leaseholderNodeID, r.RangeID)
	return nil
}
