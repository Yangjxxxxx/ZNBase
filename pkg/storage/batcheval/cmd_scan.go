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
	"fmt"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
)

func init() {
	RegisterCommand(roachpb.Scan, DefaultDeclareIsolatedKeys, Scan)
}

// Scan scans the key range specified by start key through end key
// in ascending order up to some maximum number of results. maxKeys
// stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func Scan(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ScanResponse)

	var res result.Result
	var scanRes engine.MVCCScanResult
	var err error

	failOnMoreRecent := args.KeyLocking != lock.None
	forUpdate := false
	if h.Txn != nil && h.Txn.IsolationLevel == util.ReadCommittedIsolation {
		failOnMoreRecent = false
		forUpdate = args.KeyLocking != lock.None
	}

	opts := engine.MVCCScanOptions{
		Inconsistent:     h.ReadConsistency != roachpb.CONSISTENT,
		Txn:              h.Txn,
		MaxKeys:          h.MaxSpanRequestKeys,
		FailOnMoreRecent: failOnMoreRecent,
		Reverse:          false,
	}

	switch args.ScanFormat {
	case roachpb.COUNT_ONLY:
		opts.CountOnly = true
		scanRes, err = engine.MVCCScanToBytes(
			ctx, batch, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.NumKeys = scanRes.NumKeys
	case roachpb.BATCH_RESPONSE:
		scanRes, err = engine.MVCCScanToBytes(
			ctx, batch, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.BatchResponses = scanRes.KVData
	case roachpb.KEY_VALUES:
		scanRes, err = engine.MVCCScan(
			ctx, batch, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.Rows = scanRes.KVs
	default:
		panic(fmt.Sprintf("Unknown scanFormat %d", args.ScanFormat))
	}
	reply.NumKeys = scanRes.NumKeys

	if scanRes.ResumeSpan != nil {
		reply.ResumeSpan = scanRes.ResumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		// NOTE: MVCCScan doesn't use a Prefix iterator, so we don't want to use
		// one in CollectIntentRows either so that we're guaranteed to use the
		// same cached iterator and observe a consistent snapshot of the engine.
		const usePrefixIter = false
		reply.IntentRows, err = CollectIntentRows(ctx, batch, usePrefixIter, scanRes.Intents)
		if err != nil {
			return result.Result{}, err
		}
	}

	if forUpdate {
		if len(scanRes.Intents) == 0 {
			if err := forUpdateWithGet(ctx, batch, cArgs, reply.NumKeys, reply.BatchResponses); err != nil {
				return result.Result{}, err
			}
		}
	}
	if args.KeyLocking != lock.None && h.Txn != nil {
		err = acquireUnreplicatedLocksOnKeys(&res, h.Txn, args.ScanFormat, &scanRes)
		if err != nil {
			return result.Result{}, err
		}
	}
	res.Local.EncounteredIntents = scanRes.Intents
	return res, nil
}

// forUpdateWithGet implements for update by putting a null value.
func forUpdateWithGet(
	ctx context.Context,
	batch engine.ReadWriter,
	cArgs CommandArgs,
	NumKeys int64,
	BatchResponses [][]byte,
) error {
	h := cArgs.Header
	timestamp := h.Txn.ReadTimestamp
	opts := engine.MVCCGetOptions{
		Inconsistent:     false,
		Txn:              h.Txn,
		FailOnMoreRecent: true,
	}
	if NumKeys > 0 {
		for _, br := range BatchResponses {
			for {
				k, _, r, err := enginepb.ScanDecodeKeyValueNoTS(br)
				if err != nil {
					return err
				}
				// For Debug need remove
				//if err := cArgs.EvalCtx.IsEndKey(k); err != nil {
				//	log.Errorf(ctx, "splitKey Error: must fatal")
				//	break
				//}
				_, intent, err := engine.MVCCGet(ctx, batch, k, timestamp, opts)
				if err != nil {
					if log.V(2) {
						log.Errorf(ctx, "can't for update: %s\n%v", err.Error(), h)
					}
					return err
				}
				if intent != nil {
					return &roachpb.WriteIntentError{Intents: []roachpb.Intent{*intent}}
				}
				if len(r) == 0 || len(br) == len(r) {
					break
				}
				br = r
			}
		}
	}
	return nil
}
