// Copyright 2016  The Cockroach Authors.
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
	"bytes"
	"context"
	"math"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// kvBatchSize is the number of keys we request at a time.
// On a single node, 1000 was enough to avoid any performance degradation. On
// multi-node clusters, we want bigger chunks to make up for the higher latency.
// TODO(radu): parameters like this should be configurable
var kvBatchSize int64 = 10000

// SetKVBatchSize changes the kvBatchFetcher batch size, and returns a function that restores it.
func SetKVBatchSize(val int64) func() {
	oldVal := kvBatchSize
	kvBatchSize = val
	return func() { kvBatchSize = oldVal }
}

// sendFunc是执行KV批处理的函数;正常情况下
// 包装(* client.Txn) .Send
type sendFunc func(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error)

// txnKVFetcher handles retrieval of key/values.
type txnKVFetcher struct {
	// "Constant" fields, provided by the caller.
	txn       *client.Txn
	sendFn    sendFunc
	spans     roachpb.Spans
	count     chan int
	justcount bool
	// If useBatchLimit is true, batches are limited to kvBatchSize. If
	// firstBatchLimit is also set, the first batch is limited to that value.
	// Subsequent batches are larger, up to kvBatchSize.
	firstBatchLimit int64
	useBatchLimit   bool
	reverse         bool
	// lockStrength represents the locking mode to use when fetching KVs.
	// lockStrength表示在获取KVs时要使用的锁定模式。
	lockStrength sqlbase.ScanLockingStrength
	// lockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	// lockWaitPolicy表示用于处理由其他活动事务持有的冲突锁的策略。
	lockWaitPolicy sqlbase.ScanLockingWaitPolicy
	// returnRangeInfo, if set, causes the kvBatchFetcher to populate rangeInfos.
	// See also rowFetcher.returnRangeInfo.
	returnRangeInfo bool

	fetchEnd bool
	batchIdx int

	// requestSpans contains the spans that were requested in the last request,
	// and is one to one with responses. This field is kept separately from spans
	// so that the fetcher can keep track of which response was produced for each
	// input Span.
	requestSpans roachpb.Spans
	responses    []roachpb.ResponseUnion

	// As the kvBatchFetcher fetches batches of kvs, it accumulates information on the
	// replicas where the batches came from. This info can be retrieved through
	// getRangeInfo(), to be used for updating caches.
	// rangeInfos are deduped, so they're not ordered in any particular way and
	// they don't map to kvBatchFetcher.spans in any particular way.
	rangeInfos       []roachpb.RangeInfo
	origSpan         roachpb.Span
	remainingBatches [][]byte
	// ReplicationTable indicates whether the fetch operation is for the replicated table.
	ReplicationTable bool
}

var _ kvBatchFetcher = &txnKVFetcher{}

func (f *txnKVFetcher) GetRangesInfo() []roachpb.RangeInfo {
	if !f.returnRangeInfo {
		panic("GetRangeInfo() called on kvBatchFetcher that wasn't configured with returnRangeInfo")
	}
	return f.rangeInfos
}

// getBatchSize returns the max size of the next batch.
func (f *txnKVFetcher) getBatchSize() int64 {
	return f.getBatchSizeForIdx(f.batchIdx)
}

func (f *txnKVFetcher) getBatchSizeForIdx(batchIdx int) int64 {
	if !f.useBatchLimit {
		return 0
	}
	if f.firstBatchLimit == 0 || f.firstBatchLimit >= kvBatchSize {
		return kvBatchSize
	}

	// We grab the first batch according to the limit. If it turns out that we
	// need another batch, we grab a bigger batch. If that's still not enough,
	// we revert to the default batch size.
	switch batchIdx {
	case 0:
		return f.firstBatchLimit

	case 1:
		// Make the second batch 10 times larger (but at most the default batch
		// size and at least 1/10 of the default batch size). Sample
		// progressions of batch sizes:
		//
		//  First batch | Second batch | Subsequent batches
		//  -----------------------------------------------
		//         1    |     1,000     |     10,000
		//       100    |     1,000     |     10,000
		//       500    |     5,000     |     10,000
		//      1000    |    10,000     |     10,000
		secondBatch := f.firstBatchLimit * 10
		switch {
		case secondBatch < kvBatchSize/10:
			return kvBatchSize / 10
		case secondBatch > kvBatchSize:
			return kvBatchSize
		default:
			return secondBatch
		}

	default:
		return kvBatchSize
	}
}

// getKeyLockingStrength returns the configured per-key locking strength to use
// for key-value scans.
// getKeyLockingStrength返回配置的每个键的锁定强度，以用于键值扫描。
func (f *txnKVFetcher) getKeyLockingStrength() lock.Strength {
	switch f.lockStrength {
	case sqlbase.ScanLockingStrength_FOR_NONE:
		return lock.None

	case sqlbase.ScanLockingStrength_FOR_KEY_SHARE:
		// Promote to FOR_SHARE.
		// 升级为FOR_SHARE。
		fallthrough
	case sqlbase.ScanLockingStrength_FOR_SHARE:
		// We currently perform no per-key locking when FOR_SHARE is used
		// because Shared locks have not yet been implemented.
		// 当前，由于尚未实现共享锁，因此在使用FOR_SHARE时我们不执行每个键的锁定。
		return lock.None

	case sqlbase.ScanLockingStrength_FOR_NO_KEY_UPDATE:
		// Promote to FOR_UPDATE.
		// 升级到FOR_UPDATE。
		fallthrough
	case sqlbase.ScanLockingStrength_FOR_UPDATE:
		// We currently perform exclusive per-key locking when FOR_UPDATE is
		// used because Upgrade locks have not yet been implemented.
		// 当前，由于尚未实现升级锁，因此在使用FOR_UPDATE时我们将按排它执行每个键的锁定。
		return lock.Exclusive

	default:
		panic(errors.AssertionFailedf("unknown locking strength %s", f.lockStrength))
	}
}

// getWaitPolicy returns the configured lock wait policy to use for key-value
// scans.
// getWaitPolicy返回配置的锁定等待策略，以用于键值扫描。
func (f *txnKVFetcher) getWaitPolicy() lock.WaitPolicy {
	switch f.lockWaitPolicy.LockLevel {
	case sqlbase.ScanLockingWaitLevel_BLOCK:
		return lock.WaitPolicy_Block

	case sqlbase.ScanLockingWaitLevel_SKIP:
		// Should not get here. Query should be rejected during planning.
		// 不应该到这里。 规划期间应拒绝查询。
		panic(errors.AssertionFailedf("unsupported wait policy %s", f.lockWaitPolicy.LockLevel))

	case sqlbase.ScanLockingWaitLevel_WAIT:
		return lock.WaitPolicy_Time

	case sqlbase.ScanLockingWaitLevel_ERROR:
		return lock.WaitPolicy_Error

	default:
		panic(errors.AssertionFailedf("unknown wait policy %s", f.lockWaitPolicy.LockLevel))
	}
}

// makeKVBatchFetcher initializes a kvBatchFetcher for the given spans.
//
// If useBatchLimit is true, batches are limited to kvBatchSize. If
// firstBatchLimit is also set, the first batch is limited to that value.
// Subsequent batches are larger, up to kvBatchSize.
//
// Batch limits can only be used if the spans are ordered.
func makeKVBatchFetcher(
	txn *client.Txn,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	returnRangeInfo bool,
	lockStrength sqlbase.ScanLockingStrength,
	lockWaitPolicy sqlbase.ScanLockingWaitPolicy,
) (txnKVFetcher, error) {
	if firstBatchLimit < 0 || (!useBatchLimit && firstBatchLimit != 0) {
		return txnKVFetcher{}, errors.Errorf("invalid batch limit %d (useBatchLimit: %t)",
			firstBatchLimit, useBatchLimit)
	}

	if useBatchLimit {
		// Verify the spans are ordered if a batch limit is used.
		for i := 1; i < len(spans); i++ {
			if spans[i].Key.Compare(spans[i-1].EndKey) < 0 {
				return txnKVFetcher{}, errors.Errorf("unordered spans (%s %s)", spans[i-1], spans[i])
			}
		}
	} else if util.RaceEnabled {
		// Otherwise, just verify the spans don't contain consecutive overlapping
		// spans.
		for i := 1; i < len(spans); i++ {
			if spans[i].Key.Compare(spans[i-1].EndKey) >= 0 {
				// Current Span's start key is greater than or equal to the last Span's
				// end key - we're good.
				continue
			} else if spans[i].EndKey.Compare(spans[i-1].Key) < 0 {
				// Current Span's end key is less than or equal to the last Span's start
				// key - also good.
				continue
			}
			// Otherwise, the two spans overlap, which isn't allowed - it leaves us at
			// risk of incorrect results, since the row fetcher can't distinguish
			// between identical rows in two different batches.
			return txnKVFetcher{}, errors.Errorf("overlapping neighbor spans (%s %s)", spans[i-1], spans[i])
		}
	}

	// Make a copy of the spans because we update them.
	copySpans := make(roachpb.Spans, len(spans))
	for i := range spans {
		if reverse {
			// Reverse scans receive the spans in decreasing order.
			copySpans[len(spans)-i-1] = spans[i]
		} else {
			copySpans[i] = spans[i]
		}
	}

	return txnKVFetcher{
		txn:             txn,
		spans:           copySpans,
		reverse:         reverse,
		lockStrength:    lockStrength,
		lockWaitPolicy:  lockWaitPolicy,
		useBatchLimit:   useBatchLimit,
		firstBatchLimit: firstBatchLimit,
		returnRangeInfo: returnRangeInfo,
	}, nil
}

// makeKVBatchFetcherWithSendFunc与makeKVBatchFetcher类似，但使用自定义
// 发送功能。
func makeKVBatchFetcherWithSendFunc(
	sendFn sendFunc,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	returnRangeInfo bool,
	lockStrength sqlbase.ScanLockingStrength,
	lockWaitPolicy sqlbase.ScanLockingWaitPolicy,
) (txnKVFetcher, error) {
	if firstBatchLimit < 0 || (!useBatchLimit && firstBatchLimit != 0) {
		return txnKVFetcher{}, errors.Errorf("invalid batch limit %d (useBatchLimit: %t)",
			firstBatchLimit, useBatchLimit)
	}

	if useBatchLimit {
		// Verify the spans are ordered if a batch limit is used.
		for i := 1; i < len(spans); i++ {
			if spans[i].Key.Compare(spans[i-1].EndKey) < 0 {
				return txnKVFetcher{}, errors.Errorf("unordered spans (%s %s)", spans[i-1], spans[i])
			}
		}
	} else if util.RaceEnabled {
		// Otherwise, just verify the spans don't contain consecutive overlapping
		// spans.
		for i := 1; i < len(spans); i++ {
			if spans[i].Key.Compare(spans[i-1].EndKey) >= 0 {
				// Current Span's start key is greater than or equal to the last Span's
				// end key - we're good.
				continue
			} else if spans[i].EndKey.Compare(spans[i-1].Key) < 0 {
				// Current Span's end key is less than or equal to the last Span's start
				// key - also good.
				continue
			}
			// Otherwise, the two spans overlap, which isn't allowed - it leaves us at
			// risk of incorrect results, since the row fetcher can't distinguish
			// between identical rows in two different batches.
			return txnKVFetcher{}, errors.Errorf("overlapping neighbor spans (%s %s)", spans[i-1], spans[i])
		}
	}

	// Make a copy of the spans because we update them.
	copySpans := make(roachpb.Spans, len(spans))
	for i := range spans {
		if reverse {
			// Reverse scans receive the spans in decreasing order.
			copySpans[len(spans)-i-1] = spans[i]
		} else {
			copySpans[i] = spans[i]
		}
	}

	return txnKVFetcher{
		sendFn:          sendFn,
		spans:           copySpans,
		reverse:         reverse,
		lockStrength:    lockStrength,
		lockWaitPolicy:  lockWaitPolicy,
		useBatchLimit:   useBatchLimit,
		firstBatchLimit: firstBatchLimit,
		returnRangeInfo: returnRangeInfo,
	}, nil
}

// fetch retrieves spans from the kv
func (f *txnKVFetcher) fetch(ctx context.Context) error {
	var ba roachpb.BatchRequest
	ba.Header.MaxSpanRequestKeys = f.getBatchSize()
	//add by zyk to change max span request key
	if f.justcount {
		ba.Header.MaxSpanRequestKeys = math.MaxInt64 - 1
		ba.DontNeedMeta = true
	}
	ba.Header.ReturnRangeInfo = f.returnRangeInfo
	ba.Header.WaitPolicy = f.getWaitPolicy()
	if ba.Header.WaitPolicy == lock.WaitPolicy_Time {
		ba.WaitTime.WallTime = f.lockWaitPolicy.WaitTime
	}
	ba.Requests = make([]roachpb.RequestUnion, len(f.spans))
	keyLocking := f.getKeyLockingStrength()
	if f.reverse {
		scans := make([]roachpb.ReverseScanRequest, len(f.spans))
		for i := range f.spans {
			if f.justcount {
				scans[i].ScanFormat = roachpb.COUNT_ONLY
			} else {
				scans[i].ScanFormat = roachpb.BATCH_RESPONSE
			}
			if f.ReplicationTable {
				scans[i].ReplicationTable = true
			}
			scans[i].SetSpan(f.spans[i])
			ba.Requests[i].MustSetInner(&scans[i])
			scans[i].KeyLocking = keyLocking
		}
	} else {
		scans := make([]roachpb.ScanRequest, len(f.spans))
		for i := range f.spans {
			if f.justcount {
				scans[i].ScanFormat = roachpb.COUNT_ONLY
			} else {
				scans[i].ScanFormat = roachpb.BATCH_RESPONSE
			}
			if f.ReplicationTable {
				scans[i].ReplicationTable = true
			}
			scans[i].SetSpan(f.spans[i])
			ba.Requests[i].MustSetInner(&scans[i])
			scans[i].KeyLocking = keyLocking
		}
	}
	if cap(f.requestSpans) < len(f.spans) {
		f.requestSpans = make(roachpb.Spans, len(f.spans))
	} else {
		f.requestSpans = f.requestSpans[:len(f.spans)]
	}
	copy(f.requestSpans, f.spans)

	if log.ExpensiveLogEnabled(ctx, 2) {
		buf := bytes.NewBufferString("Scan ")
		for i, span := range f.spans {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(span.String())
		}
		log.VEvent(ctx, 2, buf.String())
	}

	// Reset spans in preparation for adding resume-spans below.
	f.spans = f.spans[:0]
	br, err := f.txn.Send(ctx, ba)
	if err != nil {
		return err.GoError()
	}
	if br != nil {
		f.responses = br.Responses
	} else {
		f.responses = nil
	}

	// Set end to true until disproved.
	f.fetchEnd = true
	var sawResumeSpan bool
	for _, resp := range f.responses {
		reply := resp.GetInner()
		header := reply.Header()

		if header.NumKeys > 0 && sawResumeSpan {
			return errors.Errorf(
				"Span with results after resume Span; it shouldn't happen given that "+
					"we're only scanning non-overlapping spans. New spans: %s",
				sqlbase.PrettySpans(nil, f.spans, 0 /* skip */))
		}

		if resumeSpan := header.ResumeSpan; resumeSpan != nil {
			// A Span needs to be resumed.
			f.fetchEnd = false
			f.spans = append(f.spans, *resumeSpan)
			// Verify we don't receive results for any remaining spans.
			sawResumeSpan = true
		}

		// Fill up the RangeInfos, in case we got any.
		if f.returnRangeInfo {
			for _, ri := range header.RangeInfos {
				f.rangeInfos = roachpb.InsertRangeInfo(f.rangeInfos, ri)
			}
		}
	}

	f.batchIdx++

	// TODO(radu): We should fetch the next chunk in the background instead of waiting for the next
	// call to fetch(). We can use a pool of workers to issue the KV ops which will also limit the
	// total number of fetches that happen in parallel (and thus the amount of resources we use).
	return nil
}

// nextBatch returns the next batch of key/value pairs. If there are none
// available, a fetch is initiated. When there are no more keys, ok is false.
// origSpan returns the Span that batch was fetched from, and bounds all of the
// keys returned.
func (f *txnKVFetcher) nextBatch(
	ctx context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, origSpan roachpb.Span, err error) {
	if len(f.remainingBatches) > 0 {
		batch := f.remainingBatches[0]
		f.remainingBatches = f.remainingBatches[1:]
		return true, nil, batch, f.origSpan, nil
	}
	if len(f.responses) > 0 {
		reply := f.responses[0].GetInner()
		f.responses = f.responses[1:]
		origSpan := f.requestSpans[0]
		f.requestSpans = f.requestSpans[1:]
		var batchResp []byte
		switch t := reply.(type) {
		case *roachpb.ScanResponse:
			if len(t.BatchResponses) > 0 {
				batchResp = t.BatchResponses[0]
				f.remainingBatches = t.BatchResponses[1:]
			}
			return true, t.Rows, batchResp, origSpan, nil
		case *roachpb.ReverseScanResponse:
			if len(t.BatchResponses) > 0 {
				batchResp = t.BatchResponses[0]
				f.remainingBatches = t.BatchResponses[1:]
			}
			return true, t.Rows, batchResp, origSpan, nil
		}
	}
	if f.fetchEnd {
		return false, nil, nil, roachpb.Span{}, nil
	}
	if err := f.fetch(ctx); err != nil {
		return false, nil, nil, roachpb.Span{}, err
	}
	return f.nextBatch(ctx)
}

func (f *txnKVFetcher) NextBatch(
	ctx context.Context, respons chan roachpb.BlockStruct, errCh chan error, rf *Fetcher,
) {
	numKeyCount := 0
	firstBa := true
	if f.justcount {
		for {
			if len(f.responses) > 0 {
				for _, re := range f.responses {
					reply := re.GetInner()
					if re, ok := reply.(*roachpb.ScanResponse); ok {
						if re.NumKeys > 0 {
							numKeyCount += int(re.NumKeys) //delete the last element nil
						}
					}
					f.responses = nil
				}
			}
			if f.fetchEnd {
				rf.Count <- numKeyCount
				close(errCh)
				close(respons)
				close(f.count)
				return
			}
			if err := f.fetch(ctx); err != nil {
				err1 := ConvertFetchError(ctx, rf, err)
				errCh <- err1
				close(respons)
				return
			}
			if firstBa {
				firstBa = false
				errCh <- nil
			}
		}
	}

	for {
		for len(f.remainingBatches) > 0 {
			batch := f.remainingBatches[0]
			f.remainingBatches = f.remainingBatches[1:]
			respons <- roachpb.BlockStruct{
				Kvs:       nil,
				BatchResp: batch,
			}
		}

		if len(f.responses) > 0 {
			reply := f.responses[0].GetInner()
			f.responses = f.responses[1:]
			f.requestSpans = f.requestSpans[1:]
			var batchResp []byte
			switch t := reply.(type) {
			case *roachpb.ScanResponse:
				if len(t.BatchResponses) > 0 {
					batchResp = t.BatchResponses[0]
					f.remainingBatches = t.BatchResponses[1:]
				}
				respons <- roachpb.BlockStruct{
					Kvs:       t.Rows,
					BatchResp: batchResp,
				}
				continue

			case *roachpb.ReverseScanResponse:
				if len(t.BatchResponses) > 0 {
					batchResp = t.BatchResponses[0]
					f.remainingBatches = t.BatchResponses[1:]
				}
				respons <- roachpb.BlockStruct{
					Kvs:       t.Rows,
					BatchResp: batchResp,
				}
				continue
			}
		}

		if f.fetchEnd {
			close(errCh)
			close(respons)
			return
		}
		if err := f.fetch(ctx); err != nil {
			err1 := ConvertFetchError(ctx, rf, err)
			errCh <- err1
			close(errCh)
			close(respons)
			return
		}
		if firstBa {
			firstBa = false
			errCh <- nil
		}
	}
}
