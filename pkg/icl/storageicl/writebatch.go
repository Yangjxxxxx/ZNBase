// Copyright 2016  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package storageicl

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl/engineicl"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

func init() {
	batcheval.RegisterCommand(roachpb.WriteBatch, batcheval.DefaultDeclareKeys, evalWriteBatch)
}

// evalWriteBatch applies the operations encoded in a BatchRepr. Any existing
// data in the affected keyrange is first cleared (not tombstoned), which makes
// this command idempotent.
func evalWriteBatch(
	ctx context.Context, batch engine.ReadWriter, cArgs batcheval.CommandArgs, _ roachpb.Response,
) (result.Result, error) {

	args := cArgs.Args.(*roachpb.WriteBatchRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	_, span := tracing.ChildSpan(ctx, fmt.Sprintf("WriteBatch [%s,%s)", args.Key, args.EndKey))
	defer tracing.FinishSpan(span)
	if log.V(1) {
		log.Infof(ctx, "writebatch [%s,%s)", args.Key, args.EndKey)
	}

	// We can't use the normal RangeKeyMismatchError mechanism for dealing with
	// splits because args.Data should stay an opaque blob to DistSender.
	if args.DataSpan.Key.Compare(args.Key) < 0 || args.DataSpan.EndKey.Compare(args.EndKey) > 0 {
		// TODO(dan): Add a new field in roachpb.Error, so the client can catch
		// this and retry.
		return result.Result{}, errors.New("data spans multiple ranges")
	}

	mvccStartKey := engine.MVCCKey{Key: args.Key}
	mvccEndKey := engine.MVCCKey{Key: args.EndKey}

	// Verify that the keys in the batch are within the range specified by the
	// request header.
	msBatch, err := engineicl.VerifyBatchRepr(args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, err
	}
	ms.Add(msBatch)

	// Check if there was data in the affected keyrange. If so, delete it (and
	// adjust the MVCCStats) before applying the WriteBatch data.
	existingStats, err := clearExistingData(ctx, batch, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "clearing existing data")
	}
	ms.Subtract(existingStats)

	if err := batch.ApplyBatchRepr(args.Data, false /* sync */); err != nil {
		return result.Result{}, err
	}
	return result.Result{}, nil
}

func clearExistingData(
	ctx context.Context, batch engine.ReadWriter, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	{
		isEmpty := true
		if err := batch.Iterate(start, end, func(_ engine.MVCCKeyValue) (bool, error) {
			isEmpty = false
			return true, nil // stop right away
		}); err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "while checking for empty key space")
		}

		if isEmpty {
			return enginepb.MVCCStats{}, nil
		}
	}

	iter := batch.NewIterator(engine.IterOptions{UpperBound: end.Key})
	defer iter.Close()

	iter.Seek(start)
	if ok, err := iter.Valid(); err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok && !iter.UnsafeKey().Less(end) {
		return enginepb.MVCCStats{}, nil
	}

	existingStats, err := iter.ComputeStats(start, end, nowNanos)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	log.Eventf(ctx, "target key range not empty, will clear existing data: %+v", existingStats)
	// If this is a Iterator, we have to unwrap it because
	// ClearIterRange needs a plain rocksdb iterator (and can't unwrap
	// it itself because of import cycles).
	if ssi, ok := iter.(*spanset.Iterator); ok {
		iter = ssi.Iterator()
	}
	// TODO(dan): Ideally, this would use `batch.ClearRange` but it doesn't
	// yet work with read-write batches (or IngestExternalData).
	if err := batch.ClearIterRange(iter, start, end); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return existingStats, nil
}
