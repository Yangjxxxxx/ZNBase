package batcheval

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

var (
	testKey1 = roachpb.Key("/db1")
	value1   = roachpb.MakeValueFromString("testValue1")
)

func TestQueryLock(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	id := uuid.FromStringOrNil("f90b99de-6bd2-48a3-873c-12fdb9867a3c")
	if id == uuid.Nil {
		t.Fatalf("id = %v", uuid.Nil)
	}
	falseID := "f90b99de-6bd2-48a3-873c-12fdb9867a3a"
	txnMeta := enginepb.TxnMeta{
		ID:             id,
		Key:            keys.LocalMax.Next(),
		WriteTimestamp: hlc.Timestamp{WallTime: 1},
	}

	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	transaction := &roachpb.Transaction{
		TxnMeta:       txnMeta,
		ReadTimestamp: hlc.Timestamp{WallTime: 1},
	}
	if err := engine.MVCCPut(ctx, e, nil, testKey1, hlc.Timestamp{WallTime: 1}, value1, transaction); err != nil {
		t.Fatal(err)
	}

	mec := mockEvalCtx{
		clock: hlc.NewClock(hlc.UnixNano, time.Nanosecond),
	}

	var h roachpb.Header
	h.Timestamp = mec.Clock().Now()

	cArgs := CommandArgs{Header: h}
	cArgs.MaxKeys = math.MaxInt64
	cArgs.EvalCtx = &mockEvalCtx{txnMeta: txnMeta}

	_, _ = engine.MVCCScan(ctx, e, keys.MinKey, keys.MaxKey, cArgs.MaxKeys, hlc.Timestamp{WallTime: 1}, engine.MVCCScanOptions{})

	type testResult struct {
		kvSpan                 *roachpb.Span
		intentKeys             [][]byte
		durabilityReplicated   bool
		durabilityUnreplicated bool
	}

	tests := []struct {
		transactionID string
		scanIntent    bool
		KVspan        roachpb.Span
		result        testResult
	}{
		{
			transactionID: id.String(),
			scanIntent:    false,
			KVspan:        roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
			result: testResult{
				kvSpan:               &roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
				durabilityReplicated: true,
			},
		},
		{
			transactionID: id.String(),
			scanIntent:    false,
			KVspan:        roachpb.Span{Key: keys.MinKey, EndKey: keys.LocalMax},
			result: testResult{
				kvSpan: &roachpb.Span{Key: keys.MinKey, EndKey: keys.LocalMax},
			},
		},
		{
			transactionID: falseID,
			scanIntent:    false,
			KVspan:        roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
			result: testResult{
				kvSpan: &roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
			},
		},
		{
			transactionID: id.String(),
			scanIntent:    true,
			KVspan:        roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
			result: testResult{
				nil,
				[][]byte{testKey1},
				false,
				false,
			},
		},
		{
			transactionID: falseID,
			scanIntent:    true,
			KVspan:        roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
			result: testResult{
				nil,
				nil,
				false,
				false,
			},
		},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			queryLockRequest := roachpb.QueryLockRequest{
				TransactionId: test.transactionID,
				ScanIntent:    test.scanIntent,
				RequestHeader: roachpb.RequestHeader{
					Key:    test.KVspan.Key,
					EndKey: test.KVspan.EndKey,
				},
			}
			cArgs.Args = &queryLockRequest

			resp := &roachpb.QueryLockResponse{}

			_, _ = QueryLock(ctx, e, cArgs, resp)

			if resp.DurabilityReplicated != test.result.durabilityReplicated {
				t.Errorf("QueryLock() got = %v, want %v", resp.DurabilityReplicated, test.result.durabilityReplicated)
				return
			}
			if resp.DurabilityUnreplicated != test.result.durabilityUnreplicated {
				t.Errorf("QueryLock() got = %v, want %v", resp.DurabilityUnreplicated, test.result.durabilityUnreplicated)
				return
			}

			if !test.scanIntent {
				if !resp.KVspan.EqualValue(*test.result.kvSpan) {
					t.Errorf("QueryLock() got = %v, want %v", resp.KVspan.String(), test.KVspan.String())
					return
				}
			}

			if !reflect.DeepEqual(resp.IntentKeys, test.result.intentKeys) {
				t.Errorf("QueryLock() got = %v, want %v", resp.IntentKeys, test.result.intentKeys)
				return
			}
		})
	}
}
