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
	RegisterCommand(roachpb.RefreshRange, DefaultDeclareKeys, RefreshRange)
}

// RefreshRange checks whether the key range specified has any values written in
// the interval [args.RefreshFrom, header.Timestamp].
func RefreshRange(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RefreshRangeRequest)
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

	// Iterate over values until we discover any value written at or after the
	// original timestamp, but before or at the current timestamp. Note that we
	// iterate inconsistently without using the txn. This reads only committed
	// values and returns all intents, including those from the txn itself. Note
	// that we include tombstones, which must be considered as updates on refresh.
	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Span(), refreshFrom, refreshTo)
	intents, err := engine.MVCCIterate(ctx, batch, args.Key, args.EndKey, refreshTo, engine.MVCCScanOptions{
		Inconsistent: true,
		Tombstones:   true,
	}, func(kv roachpb.KeyValue) (bool, error) {
		// TODO(nvanbenschoten): This is pessimistic. We only need to check
		//   !ts.Less(h.Txn.PrevRefreshTimestamp)
		// This could avoid failed refreshes due to requests performed after
		// earlier refreshes (which read at the refresh ts) that already
		// observed writes between the orig ts and the refresh ts.
		// See more info in similar comment in cmd_refresh.go.
		if ts := kv.Value.Timestamp; !ts.Less(refreshFrom) {
			return true, errors.Errorf("encountered recently written key %s @%s", kv.Key, ts)
		}
		return false, nil
	})
	if err != nil {
		return result.Result{}, err
	}

	// Check if any intents which are not owned by this transaction were written
	// at or beneath the refresh timestamp.
	for _, i := range intents {
		// Ignore our own intents.
		if i.Txn.ID == h.Txn.ID {
			continue
		}
		// Return an error if an intent was written to the span.
		return result.Result{}, errors.Errorf("encountered recently written intent %s @%s",
			i.Key, i.Txn.WriteTimestamp)
	}

	return result.Result{}, nil
}
