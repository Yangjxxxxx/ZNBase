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
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
)

func init() {
	RegisterCommand(roachpb.Revert, declareKeysRevert, Revert)
}

var enableFlashbackRevertConsistency = settings.RegisterBoolSetting(
	"flashback.revert.consistency",
	"set to true to make the recovery process is consistent, but the performance is worse;"+
		"Setting to false recovery interrupts have intermediate states, but better performance",
	true,
)

func declareKeysRevert(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	DefaultDeclareIsolatedKeys(desc, header, req, latchSpans, lockSpans)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(desc.RangeID)})
}

// Revert snapshot.
func Revert(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RevertRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.RevertResponse)

	var timestamp = h.Timestamp

	var clearIntents []roachpb.Intent
	if args.ClearIntents != nil {
		clearIntents = args.ClearIntents
	}

	eFRC := enableFlashbackRevertConsistency.Get(&cArgs.EvalCtx.ClusterSettings().SV)

	reverted, resumeSpan, num, rIntents, err := engine.MVCCRevertRange(
		ctx, batch, cArgs.Stats, args.Key, args.EndKey, cArgs.MaxKeys, timestamp, args.SnapshotTime, h.Txn,
		args.ReturnKeys, eFRC, clearIntents,
	)
	if err == nil {
		reply.Keys = reverted
		reply.UnknownIntents = *rIntents
	}
	reply.Reverted = num
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
		reply.NumKeys = cArgs.Header.MaxSpanRequestKeys
	}

	return result.Result{}, err
}
