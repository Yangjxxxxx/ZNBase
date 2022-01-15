// Copyright 2014 The Drdb Authors.
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

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util"
)

func init() {
	RegisterCommand(roachpb.VecScan, DefaultDeclareKeys, VecScan)
}

//VecScan 向量读取
func VecScan(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.VecScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.VecScanResponse)

	//fmt.Printf("cmd_vecscan VecScan cArgs.MaxKeys=%d\n", cArgs.MaxKeys)
	//fmt.Printf("cmd_vecscan VecScan pushDown=%s\n", args.PushDown.String())
	//fmt.Printf("cmd_vecscan VecScan ColIDs=%v\n", args.ColIDs)
	//fmt.Printf("cmd_vecscan VecScan cArgs.MaxKeys=%d, MaxSpanRequestKeys=%d\n", cArgs.MaxKeys, h.MaxSpanRequestKeys)
	var err error
	var res result.Result

	failOnMoreRecent := false //args.KeyLocking != lock.None
	if h.Txn != nil && h.Txn.IsolationLevel == util.ReadCommittedIsolation {
		failOnMoreRecent = false
		//forUpdate = args.KeyLocking != lock.None
	}

	colBatch, resumeSpan, intents, err := engine.MVCCVecScan(
		ctx, batch,
		args.Key, args.EndKey,
		args.PushDown,
		cArgs.MaxKeys,
		h.Timestamp,
		engine.MVCCScanOptions{
			Inconsistent:     h.ReadConsistency != roachpb.CONSISTENT,
			Txn:              h.Txn,
			MaxKeys:          h.MaxSpanRequestKeys,
			FailOnMoreRecent: failOnMoreRecent,
			Reverse:          false,
		})
	if err != nil {
		return result.Result{}, err
	}
	reply.NumKeys = colBatch.Count
	//reply.Keys = keys
	reply.Batch = colBatch

	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		reply.IntentRows, err = VecScanCollectIntentRows(ctx, batch, cArgs, intents)
	}
	res.Local.EncounteredIntents = intents
	return res, nil
}

//VecScanCollectIntentRows 写意图返回
func VecScanCollectIntentRows(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, intents []roachpb.Intent,
) ([]roachpb.KeyValue, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	res := make([]roachpb.KeyValue, 0, len(intents))
	for _, intent := range intents {
		val, _, err := engine.MVCCGetAsTxn(
			ctx, batch, intent.Key, intent.Txn.WriteTimestamp, intent.Txn,
		)
		if err != nil {
			return nil, err
		}
		if val == nil {
			// Intent is a deletion.
			continue
		}
		res = append(res, roachpb.KeyValue{
			Key:   intent.Key,
			Value: *val,
		})
	}
	return res, nil
}
