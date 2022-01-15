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

package row

import (
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
)

// KVFetcher wraps kvBatchFetcher, providing a NextKV interface that returns the
// next kv from its input.
type KVFetcher struct {
	kvBatchFetcher

	blockRespon roachpb.BlockStruct

	kvs []roachpb.KeyValue

	batchResponse []byte
	bytesRead     int64
	Span          roachpb.Span
	newSpan       bool
}

// NewKVFetcher creates a new KVFetcher.
func NewKVFetcher(
	txn *client.Txn,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	returnRangeInfo bool,
	lockStrength sqlbase.ScanLockingStrength,
	lockWaitPolicy sqlbase.ScanLockingWaitPolicy,
) (*KVFetcher, error) {
	kvBatchFetcher, err := makeKVBatchFetcher(
		txn, spans, reverse, useBatchLimit, firstBatchLimit, returnRangeInfo, lockStrength, lockWaitPolicy)
	return newKVFetcher(&kvBatchFetcher), err
}

func newKVFetcher(batchFetcher kvBatchFetcher) *KVFetcher {
	return &KVFetcher{
		kvBatchFetcher: batchFetcher,
	}
}

// NextKV returns the next kv from this fetcher. Returns false if there are no
// more kvs to fetch, the kv that was fetched, and any errors that may have
// occurred.
func (f *KVFetcher) NextKV(
	ctx context.Context,
) (ok bool, kv roachpb.KeyValue, newSpan bool, err error) {
	newSpan = f.newSpan
	f.newSpan = false
	if len(f.kvs) != 0 {
		kv = f.kvs[0]
		f.kvs = f.kvs[1:]
		return true, kv, newSpan, nil
	}
	if len(f.batchResponse) > 0 {
		var key []byte
		var rawBytes []byte
		var err error
		key, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValueNoTS(f.batchResponse)
		if err != nil {
			return false, kv, false, err
		}
		return true, roachpb.KeyValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes: rawBytes,
			},
		}, newSpan, nil
	}

	ok, f.kvs, f.batchResponse, f.Span, err = f.nextBatch(ctx)
	if err != nil {
		return ok, kv, false, err
	}
	if !ok {
		return false, kv, false, nil
	}
	f.newSpan = true
	return f.NextKV(ctx)
}

func (f *KVFetcher) nextKV1(
	ctx context.Context, res chan roachpb.BlockStruct,
) (first bool, ok bool, kv roachpb.KeyValue, err error) {
	if len(f.blockRespon.Kvs) != 0 {
		kv = f.blockRespon.Kvs[0]
		f.blockRespon.Kvs = f.blockRespon.Kvs[1:]
		return false, true, kv, nil
	}
	if len(f.blockRespon.BatchResp) > 0 {
		var key []byte
		var rawBytes []byte
		var err error
		key, rawBytes, f.blockRespon.BatchResp, err = enginepb.ScanDecodeKeyValueNoTS(f.blockRespon.BatchResp)
		if err != nil {
			return false, false, kv, err
		}
		return false, true, roachpb.KeyValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes: rawBytes,
			},
		}, nil
	}

	f.blockRespon, ok = <-res

	if ok {
		if len(f.blockRespon.Kvs) != 0 {
			kv = f.blockRespon.Kvs[0]
			f.blockRespon.Kvs = f.blockRespon.Kvs[1:]
			return true, true, kv, nil
		}
		if len(f.blockRespon.BatchResp) > 0 {
			var key []byte
			var rawBytes []byte
			var err error
			key, rawBytes, f.blockRespon.BatchResp, err = enginepb.ScanDecodeKeyValueNoTS(f.blockRespon.BatchResp)
			if err != nil {
				return false, false, kv, err
			}
			return true, true, roachpb.KeyValue{
				Key: key,
				Value: roachpb.Value{
					RawBytes: rawBytes,
				},
			}, nil
		}
	}

	if (f.blockRespon.BatchResp == nil && f.blockRespon.Kvs == nil) && !ok {
		return false, false, kv, nil
	}

	return f.nextKV1(ctx, res)
}

// BlockedNextKV for tablereader parallel
func (f *KVFetcher) BlockedNextKV(
	ctx context.Context, block *roachpb.BlockStruct,
) (ok bool, kv roachpb.KeyValue, newSpan bool, err error) {
	newSpan = true
	if len(block.Kvs) != 0 {
		kv = block.Kvs[0]
		block.Kvs = block.Kvs[1:]
		return true, kv, newSpan, nil
	}
	if len(block.BatchResp) > 0 {
		var key []byte
		var rawBytes []byte
		var err error
		key, rawBytes, block.BatchResp, err = enginepb.ScanDecodeKeyValueNoTS(block.BatchResp)
		if err != nil {
			return false, kv, false, err
		}
		return true, roachpb.KeyValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes: rawBytes,
			},
		}, newSpan, nil
	}
	return false, kv, false, nil
}
