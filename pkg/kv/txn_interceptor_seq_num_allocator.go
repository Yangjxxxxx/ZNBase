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

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
)

// txnSeqNumAllocator is a txnInterceptor in charge of allocating sequence
// numbers to all the individual requests in batches.
//
// Sequence numbers serve a few roles in the transaction model:
//
// 1. they are used to enforce an ordering between read and write operations in a
//    single transaction that go to the same key. Each read request that travels
//    through the interceptor is assigned the sequence number of the most recent
//    write. Each write request that travels through the interceptor is assigned
//    a sequence number larger than any previously allocated.
//
//    This is true even for leaf transaction coordinators. In their case, they are
//    provided the sequence number of the most recent write during construction.
//    Because they only perform read operations and never issue writes, they assign
//    each read this sequence number without ever incrementing their own counter.
//    In this way, sequence numbers are maintained correctly across a distributed
//    tree of transaction coordinators.
//
// 2. they are used to uniquely identify write operations. Because every write
//    request is given a new sequence number, the tuple (txn_id, txn_epoch, seq)
//    uniquely identifies a write operation across an entire cluster. This property
//    is exploited when determining the status of an individual write by looking
//    for its intent. We perform such an operation using the QueryIntent request
//    type when pipelining transactional writes. We will do something similar
//    during the recovery stage of implicitly committed transactions.
//
// 3. they are used to determine whether a batch contains the entire write set
//    for a transaction. See BatchRequest.IsCompleteTransaction.
//
// 4. they are used to provide idempotency for replays and re-issues. The MVCC
//    layer is sequence number-aware and ensures that reads at a given sequence
//    number ignore writes in the same transaction at larger sequence numbers.
//    Likewise, writes at a sequence number become no-ops if an intent with the
//    same sequence is already present. If an intent with the same sequence is not
//    already present but an intent with a larger sequence number is, an error is
//    returned. Likewise, if an intent with the same sequence is present but its
//    value is different than what we recompute, an error is returned.
//
type txnSeqNumAllocator struct {
	wrapped lockedSender
	//seqGen  enginepb.TxnSeq

	// commandCount indicates how many requests have been sent through
	// this transaction. Reset on retryable txn errors.
	// TODO(andrei): let's get rid of this. It should be maintained
	// in the SQL level.
	commandCount int32

	// writeSeq is the current write seqnum, i.e. the value last assigned
	// to a write operation in a batch. It remains at 0 until the first
	// write operation is encountered.
	writeSeq enginepb.TxnSeq

	// readSeq is the sequence number at which to perform read-only
	// operations when steppingModeEnabled is set.
	readSeq enginepb.TxnSeq

	// steppingModeEnabled indicates whether to operate in stepping mode
	// or read-own-writes:
	// - in read-own-writes, read-only operations read at the latest
	//   write seqnum.
	// - when stepping, read-only operations read at a
	//   fixed readSeq.
	steppingModeEnabled bool
}

// SendLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	for _, ru := range ba.Requests {
		req := ru.GetInner()
		// Only increment the sequence number generator for requests that
		// will leave intents or requests that will commit the transaction.
		// This enables ba.IsCompleteTransaction to work properly.
		if roachpb.IsIntentWrite(req) || req.Method() == roachpb.EndTransaction {
			s.writeSeq++
		}

		// Note: only read-only requests can operate at a past seqnum.
		// Combined read/write requests (e.g. CPut) always read at the
		// latest write seqnum.
		oldHeader := req.Header()
		oldHeader.Sequence = s.writeSeq
		if s.steppingModeEnabled && roachpb.IsReadOnly(req) {
			oldHeader.Sequence = s.readSeq
		}
		req.SetHeader(oldHeader)
	}

	s.commandCount += int32(len(ba.Requests))

	return s.wrapped.SendLocked(ctx, ba)
}

// setWrapped is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) setWrapped(wrapped lockedSender) { s.wrapped = wrapped }

// populateMetaLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) populateMetaLocked(meta *roachpb.TxnCoordMeta) {
	meta.CommandCount = s.commandCount
	meta.Txn.Sequence = s.writeSeq
}

// augmentMetaLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
	s.commandCount += meta.CommandCount
	if meta.Txn.Sequence > s.writeSeq {
		s.writeSeq = meta.Txn.Sequence
	}
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) epochBumpedLocked() {
	s.writeSeq = 0
	s.readSeq = 0
	s.commandCount = 0
}

// createSavepointLocked is part of the txnReqInterceptor interface.
func (s *txnSeqNumAllocator) createSavepointLocked(ctx context.Context, sp *savepoint) {
	sp.seqNum = s.writeSeq
}

// rollbackToSavepointLocked is part of the txnReqInterceptor interface.
func (*txnSeqNumAllocator) rollbackToSavepointLocked(context.Context, savepoint) {
	// Nothing to restore. The seq nums keep increasing. The TxnCoordSender has
	// added a range of sequence numbers to the ignored list.
}

// closeLocked is part of the txnInterceptor interface.
func (*txnSeqNumAllocator) closeLocked() {}

// stepLocked bumps the read seqnum to the current write seqnum.
// Used by the TxnCoordSender's Step() method.
func (s *txnSeqNumAllocator) stepLocked(ctx context.Context) error {
	if !s.steppingModeEnabled {
		return errors.AssertionFailedf("stepping mode is not enabled")
	}
	if s.readSeq > s.writeSeq {
		return errors.AssertionFailedf(
			"cannot step() after mistaken initialization (%d,%d)", s.writeSeq, s.readSeq)
	}
	s.readSeq = s.writeSeq
	return nil
}

// configureSteppingLocked configures the stepping mode.
//
// When enabling stepping from the non-enabled state, the read seqnum
// is set to the current write seqnum, as if a snapshot was taken at
// the point stepping was enabled.
//
// The read seqnum is otherwise not modified when trying to enable
// stepping when it was previously enabled already. This is the
// behavior needed to provide the documented API semantics of
// sender.ConfigureStepping() (see client/sender.go).
func (s *txnSeqNumAllocator) configureSteppingLocked(
	newMode client.SteppingMode,
) (prevMode client.SteppingMode) {
	prevEnabled := s.steppingModeEnabled
	enabled := newMode == client.SteppingEnabled
	s.steppingModeEnabled = enabled
	if !prevEnabled && enabled {
		s.readSeq = s.writeSeq
	}
	prevMode = client.SteppingDisabled
	if prevEnabled {
		prevMode = client.SteppingEnabled
	}
	return prevMode
}
