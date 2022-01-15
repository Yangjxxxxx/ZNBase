// Copyright 2014  The Cockroach Authors.
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

package batcheval

import (
	"context"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
)

// DefaultDeclareKeys is the default implementation of Command.DeclareKeys.
func DefaultDeclareKeys(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	access := spanset.SpanReadWrite
	if roachpb.IsReadOnly(req) && !roachpb.IsLocking(req) {
		access = spanset.SpanReadOnly
	}
	latchSpans.AddMVCC(access, req.Header().Span(), header.Timestamp)
}

// DefaultDeclareIsolatedKeys is similar to DefaultDeclareKeys, but it declares
// both lock spans in addition to latch spans. When used, commands will wait on
// locks and wait-queues owned by other transactions before evaluating. This
// ensures that the commands are fully isolated from conflicting transactions
// when it evaluated.
func DefaultDeclareIsolatedKeys(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	access := spanset.SpanReadWrite
	timestamp := header.Timestamp
	if roachpb.IsReadOnly(req) && !roachpb.IsLocking(req) {
		access = spanset.SpanReadOnly
		if header.Txn != nil {
			// For transactional reads, acquire read latches all the way up to
			// the transaction's MaxTimestamp, because reads may observe locks
			// all the way up to this timestamp.
			//
			// TODO(nvanbenschoten): this parallels similar logic in
			// concurrency.Request.readConflictTimestamp, which indicates that
			// there is almost certainly a better way to structure this. There
			// are actually two issues here that lead to this duplication:
			//
			// 1. latch spans and lock spans are declared separately. While these
			//    concepts are not identical, it appears that lock spans are always
			//    a subset of latch spans, which means that we can probably unify
			//    the concepts more closely than we have thus far. This would
			//    probably also have positive performance implications, as the
			//    duplication mandates extra memory allocations.
			//
			// 2. latch spans can each be assigned unique MVCC timestamps but lock
			//    spans inherit the timestamp of their request's transaction (see
			//    lockTable and concurrency.Request.{read,write}ConflictTimestamp).
			//    This difference is strange and confusing. It's not clear that the
			//    generality of latches each being declared at their own timestamp
			//    is useful. There may be an emergent pattern that arises here when
			//    we unify latch and lock spans (see part 1) where latches that are
			//    in the lock span subset can inherit their request's transaction's
			//    timestamp and latches that are not are non-MVCC latches.
			//
			// Note that addressing these issues does not necessarily need to
			// lead to the timestamp that MVCC spans are interpretted at being
			// the same for the purposes of the latch manager and lock-table.
			// For instance, once the lock-table is segregated and all logic
			// relating to "lock discovery" is removed, we no longer need to
			// acquire read latches up to a txn's max timestamp, just to its
			// read timestamp. However, we will still need to search the
			// lock-table up to a txn's max timestamp.
			timestamp.Forward(header.Txn.MaxTimestamp)
		}
	}
	latchSpans.AddMVCC(access, req.Header().Span(), timestamp)
	lockSpans.AddNonMVCC(access, req.Header().Span())
}

// DeclareKeysForBatch adds all keys that the batch with the provided header
// touches to the given SpanSet. This does not include keys touched during the
// processing of the batch's individual commands.
func DeclareKeysForBatch(
	desc *roachpb.RangeDescriptor, header roachpb.Header, latchSpans *spanset.SpanSet,
) {
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.AbortSpanKey(header.RangeID, header.Txn.ID),
		})
	}
}

// CommandArgs contains all the arguments to a command.
// TODO(bdarnell): consider merging with storagebase.FilterArgs (which
// would probably require removing the EvalCtx field due to import order
// constraints).
type CommandArgs struct {
	EvalCtx EvalContext
	Header  roachpb.Header
	Args    roachpb.Request
	// If MaxKeys is non-zero, span requests should limit themselves to
	// that many keys. Commands using this feature should also set
	// NumKeys and ResumeSpan in their responses.
	MaxKeys int64

	// *Stats should be mutated to reflect any writes made by the command.
	Stats *enginepb.MVCCStats
}
