package batcheval

import (
	"context"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
)

func init() {
	RegisterCommand(roachpb.QueryLock, declareKeysQueryLock, QueryLock)
}

func declareKeysQueryLock(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	DefaultDeclareKeys(desc, header, req, latchSpans, lockSpans)
}

// QueryLock 通过事务id在存储层查找事务writeintent和在locktable中查找span范围
func QueryLock(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	req := cArgs.Args.(*roachpb.QueryLockRequest)
	h := cArgs.Header
	response := resp.(*roachpb.QueryLockResponse)
	if req.ScanIntent {
		_, err := engine.MVCCScan(ctx, batch, req.Key, req.EndKey, cArgs.MaxKeys, h.Timestamp, engine.MVCCScanOptions{})
		if err != nil {
			writeIntentError := err.(*roachpb.WriteIntentError)
			for _, intent := range writeIntentError.Intents {
				if intent.Txn.ID.String() == req.TransactionId {
					response.IntentKeys = append(response.IntentKeys, intent.Key)
				}
			}
		}
	} else {
		kvSpan := roachpb.Span{Key: req.Key, EndKey: req.EndKey}
		lockDurabilityStatus := cArgs.EvalCtx.GetConcurrencyManager().GetSpanLockStatus(kvSpan, req.TransactionId)
		response.KVspan = &kvSpan
		response.DurabilityReplicated = lockDurabilityStatus[lock.Replicated]
		response.DurabilityUnreplicated = lockDurabilityStatus[lock.Unreplicated]
	}
	return result.FromAcquiredLocks(h.Txn), nil
}
