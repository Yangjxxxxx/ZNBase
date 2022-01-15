// Copyright 2017 The Cockroach Authors.
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

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/log"
)

func init() {
	RegisterCommand(roachpb.Refresh, DefaultDeclareKeys, Refresh)
}

// Refresh checks whether the key has any values written in the interval
// [args.RefreshFrom, header.Timestamp].
// Refresh会检验在[args.RefreshFrom, header.Timestamp]区间中是否有针对于key的写操作发生
func Refresh(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RefreshRequest)
	h := cArgs.Header

	if h.Txn == nil {
		return result.Result{}, errors.Errorf("no transaction specified to %s", args.Method())
	}

	// We're going to refresh up to the transaction's read timestamp.
	if h.Timestamp != h.Txn.WriteTimestamp {
		// We're expecting the read and write timestamp to have converged before the
		// Refresh request was sent.
		log.Fatalf(ctx, "expected provisional commit ts %s == read ts %s. txn: %s", h.Timestamp,
			h.Txn.WriteTimestamp, h.Txn)
	}
	refreshTo := h.Timestamp

	refreshFrom := args.RefreshFrom
	if refreshFrom.IsEmpty() {
		return result.Result{}, errors.Errorf("empty RefreshFrom: %s", args)
	}

	// Get the most recent committed value and return any intent by
	// specifying consistent=false. Note that we include tombstones,
	// which must be considered as updates on refresh.
	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Span(), refreshFrom, refreshTo)
	val, intent, err := engine.MVCCGet(ctx, batch, args.Key, refreshTo, engine.MVCCGetOptions{
		Inconsistent: true,
		Tombstones:   true,
	})

	if err != nil {
		return result.Result{}, err
	} else if val != nil {
		// TODO(nvanbenschoten): This is pessimistic. We only need to check
		//   !ts.Less(h.Txn.PrevRefreshTimestamp)
		// This could avoid failed refreshes due to requests performed after
		// earlier refreshes (which read at the refresh ts) that already
		// observed writes between the orig ts and the refresh ts.For example:
		//		// - OrigTimestamp is 10
		//		// - attempt to read k1@10. The read fails and we have to refresh to 20.
		//		// - succeed in refreshing
		//		// - read k1@20, succeeding this time. Let's say that the latest value is @15.
		//		// - attempt to read k2@20. Need to refresh to 30.
		//		// - the refresh checks k1@[10-30]. The value @15 is found, and it causes
		//		//   the refresh to fail. But it shouldn't have, since we had already read
		//		//   that value. We should have only verified [20-30].
		if ts := val.Timestamp; !ts.Less(refreshFrom) {
			return result.Result{}, errors.Errorf("encountered recently written key %s @%s", args.Key, ts)
		}
	}

	// Check if an intent which is not owned by this transaction was written
	// at or beneath the refresh timestamp.
	if intent != nil && intent.Txn.ID != h.Txn.ID {
		return result.Result{}, errors.Errorf("encountered recently written intent %s @%s",
			intent.Key, intent.Txn.WriteTimestamp)
	}

	return result.Result{}, nil
}
