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

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// CollectIntentRows collects the provisional key-value pairs for each intent
// provided.
//
// The method accepts a reader and flag indicating whether a prefix iterator
// should be used when creating an iterator from the reader. This flexibility
// works around a limitation of the Engine.NewReadOnly interface where prefix
// iterators and non-prefix iterators pulled from the same read-only engine are
// not guaranteed to provide a consistent snapshot of the underlying engine.
// This function expects to be able to retrieve the corresponding provisional
// value for each of the provided intents. As such, it is critical that it
// observes the engine in the same state that it was in when the intent keys
// were originally collected. Because of this, callers are tasked with
// indicating whether the intents were originally collected using a prefix
// iterator or not.
//
// TODO(nvanbenschoten): remove the usePrefixIter complexity when we're fully on
// Pebble and can guarantee that all iterators created from a read-only engine
// are consistent.
//
// TODO(nvanbenschoten): mvccGetInternal should return the intent values
// directly when reading at the READ_UNCOMMITTED consistency level. Since this
// is only currently used for range lookups and when watching for a merge (both
// of which are off the hot path), this is ok for now.
func CollectIntentRows(
	ctx context.Context, reader engine.Reader, usePrefixIter bool, intents []roachpb.Intent,
) ([]roachpb.KeyValue, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	res := make([]roachpb.KeyValue, 0, len(intents))
	for i := range intents {
		kv, err := readProvisionalVal(ctx, reader, usePrefixIter, &intents[i])
		if err != nil {
			switch t := err.(type) {
			case *roachpb.WriteIntentError:
				log.Fatalf(ctx, "unexpected %T in CollectIntentRows: %+v", t, t)
			case *roachpb.ReadWithinUncertaintyIntervalError:
				log.Fatalf(ctx, "unexpected %T in CollectIntentRows: %+v", t, t)
			}
			return nil, err
		}
		if kv.Value.IsPresent() {
			res = append(res, kv)
		}
	}
	return res, nil
}

// readProvisionalVal retrieves the provisional value for the provided intent
// using the reader and the specified access method (i.e. with or without the
// use of a prefix iterator). The function returns an empty KeyValue if the
// intent is found to contain a deletion tombstone as its provisional value.
func readProvisionalVal(
	ctx context.Context, reader engine.Reader, usePrefixIter bool, intent *roachpb.Intent,
) (roachpb.KeyValue, error) {
	if usePrefixIter {
		val, _, err := engine.MVCCGetAsTxn(
			ctx, reader, intent.Key, intent.Txn.WriteTimestamp, intent.Txn,
		)
		if err != nil {
			return roachpb.KeyValue{}, err
		}
		if val == nil {
			// Intent is a deletion.
			return roachpb.KeyValue{}, nil
		}
		return roachpb.KeyValue{Key: intent.Key, Value: *val}, nil
	}
	res, err := engine.MVCCScanAsTxn(
		ctx, reader, intent.Key, intent.Key.Next(), intent.Txn.WriteTimestamp, intent.Txn,
	)
	if err != nil {
		return roachpb.KeyValue{}, err
	}
	if len(res.KVs) > 1 {
		log.Fatalf(ctx, "multiple key-values returned from single-key scan: %+v", res.KVs)
	} else if len(res.KVs) == 0 {
		// Intent is a deletion.
		return roachpb.KeyValue{}, nil
	}
	return res.KVs[0], nil
}

// acquireUnreplicatedLocksOnKeys adds an unreplicated lock acquisition by the
// transaction to the provided result.Result for each key in the scan result.
func acquireUnreplicatedLocksOnKeys(
	res *result.Result,
	txn *roachpb.Transaction,
	scanFmt roachpb.ScanFormat,
	scanRes *engine.MVCCScanResult,
) error {
	res.Local.AcquiredLocks = make([]roachpb.LockAcquisition, scanRes.NumKeys)
	switch scanFmt {
	case roachpb.BATCH_RESPONSE:
		var i int
		return engine.MVCCScanDecodeKeyValues(scanRes.KVData, func(key engine.MVCCKey, _ []byte) error {
			res.Local.AcquiredLocks[i] = roachpb.MakeLockAcquisition(txn, key.Key, lock.Unreplicated)
			i++
			return nil
		})
	case roachpb.KEY_VALUES:
		for i, row := range scanRes.KVs {
			res.Local.AcquiredLocks[i] = roachpb.MakeLockAcquisition(txn, row.Key, lock.Unreplicated)
		}
		return nil
	default:
		panic("unexpected scanFormat")
	}
}
