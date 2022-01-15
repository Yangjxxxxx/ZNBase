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
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

func init() {
	RegisterCommand(roachpb.ResolveIntent, declareKeysResolveIntent, ResolveIntent)
}

func declareKeysResolveIntentCombined(
	header roachpb.Header, req roachpb.Request, latchSpans *spanset.SpanSet,
) {
	var status roachpb.TransactionStatus
	var txnID uuid.UUID
	var minTxnTS hlc.Timestamp
	switch t := req.(type) {
	case *roachpb.ResolveIntentRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
		minTxnTS = t.IntentTxn.MinTimestamp
	case *roachpb.ResolveIntentRangeRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
		minTxnTS = t.IntentTxn.MinTimestamp
	}
	latchSpans.AddMVCC(spanset.SpanReadWrite, req.Header().Span(), minTxnTS)
	if status == roachpb.ABORTED {
		// We don't always write to the abort span when resolving an ABORTED
		// intent, but we can't tell whether we will or not ahead of time.
		latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, txnID)})
	}
}

func declareKeysResolveIntent(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	declareKeysResolveIntentCombined(header, req, latchSpans)
}

func resolveToMetricType(status roachpb.TransactionStatus, poison bool) *result.Metrics {
	var typ result.Metrics
	if status == roachpb.ABORTED {
		if poison {
			typ.ResolvePoison = 1
		} else {
			typ.ResolveAbort = 1
		}
	} else {
		typ.ResolveCommit = 1
	}
	return &typ
}

// ResolveIntent resolves a write intent from the specified key
// according to the status of the transaction which created it.
func ResolveIntent(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ResolveIntentRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}

	update := args.AsLockUpdate()
	ok, err := engine.MVCCResolveWriteIntent(ctx, batch, ms, update)
	if err != nil {
		return result.Result{}, err
	}

	var res result.Result
	res.Local.ResolvedLocks = []roachpb.LockUpdate{update}
	res.Local.Metrics = resolveToMetricType(args.Status, args.Poison)

	if WriteAbortSpanOnResolve(args.Status, args.Poison, ok) {
		if err := SetAbortSpan(ctx, cArgs.EvalCtx, batch, ms, args.IntentTxn, args.Poison); err != nil {
			return result.Result{}, err
		}
	}
	return res, nil
}
