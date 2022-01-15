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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package intentresolver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// TestCleanupTxnIntentsOnGCAsync exercises the code which is used to
// asynchronously clean up transaction intents and then transaction records.
// This method is invoked from the storage GC queue.
func TestCleanupTxnIntentsOnGCAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	cfg := Config{
		Stopper: stopper,
		Clock:   clock,
	}
	type testCase struct {
		txn *roachpb.Transaction
		// intentSpans, if set, are appended to txn.LockSpans. They'll result in
		// ResolveIntent requests.
		intentSpans   []roachpb.Span
		sendFuncs     *sendFuncs
		expectPushed  bool
		expectSucceed bool
	}

	// This test creates 3 transaction for use in the below test cases.
	// A new intent resolver is created for each test case so they operate
	// completely independently.
	key := roachpb.Key("a")
	// Txn0 is in the pending state and is not old enough to have expired so the
	// code ought to send nothing.
	txn0 := newTransaction("txn0", key, 1, clock)
	// Txn1 is in the pending state but is expired.
	txn1 := newTransaction("txn1", key, 1, clock)
	txn1.ReadTimestamp.WallTime -= int64(100 * time.Second)
	txn1.LastHeartbeat = txn1.ReadTimestamp
	// Txn2 is in the staging state and is not old enough to have expired so the
	// code ought to send nothing.
	txn2 := newTransaction("txn2", key, 1, clock)
	txn2.Status = roachpb.STAGING
	// Txn3 is in the staging state but is expired.
	txn3 := newTransaction("txn3", key, 1, clock)
	txn3.Status = roachpb.STAGING
	txn3.ReadTimestamp.WallTime -= int64(100 * time.Second)
	txn3.LastHeartbeat = txn3.ReadTimestamp
	// Txn4 is in the committed state.
	txn4 := newTransaction("txn4", key, 1, clock)
	txn4.Status = roachpb.COMMITTED
	cases := []*testCase{
		// This one has an unexpired pending transaction so it's skipped.
		{
			txn:       txn0,
			sendFuncs: newSendFuncs(t),
		},
		// Txn1 is pending and expired so the code should attempt to push the txn.
		// The provided sender will fail on the first request. The callback should
		// indicate that the transaction was pushed but that the resolution was not
		// successful.
		{
			txn:          txn1,
			sendFuncs:    newSendFuncs(t, failSendFunc),
			expectPushed: true,
		},
		// Txn1 is pending and expired so the code should attempt to push the txn
		// and then work to resolve its intents. The intent resolution happens in
		// different requests for individual keys and then for spans. This case will
		// allow the individual key intent to be resolved completely but will fail
		// to resolve the span. The callback should indicate that the transaction
		// has been pushed but that the garbage collection was not successful.
		{
			txn: txn1,
			intentSpans: []roachpb.Span{
				{Key: key},
				{Key: key, EndKey: roachpb.Key("b")},
			},
			sendFuncs: newSendFuncs(t,
				singlePushTxnSendFunc(t),
				resolveIntentsSendFuncEx(t, checkTxnAborted),
				failSendFunc,
			),
			expectPushed: true,
		},
		// Txn1 is pending and expired so the code should attempt to push the txn
		// and then work to resolve its intents. The intent resolution happens in
		// different requests for individual keys and then for spans. This case will
		// allow the individual key intents to be resolved in one request and for
		// the span request to be resolved in another. Finally it will succeed on
		// the GCRequest. This is a positive case and the callback should indicate
		// that the txn has both been pushed and successfully resolved.
		{
			txn: txn1,
			intentSpans: []roachpb.Span{
				{Key: key},
				{Key: roachpb.Key("aa")},
				{Key: key, EndKey: roachpb.Key("b")},
			},
			sendFuncs: func() *sendFuncs {
				s := newSendFuncs(t)
				s.pushFrontLocked(
					singlePushTxnSendFunc(t),
					resolveIntentsSendFuncs(s, 3, 2),
					gcSendFunc(t),
				)
				return s
			}(),
			expectPushed:  true,
			expectSucceed: true,
		},
		// This one has an unexpired staging transaction so it's skipped.
		{
			txn:       txn2,
			sendFuncs: newSendFuncs(t),
		},
		// Txn3 is staging and expired so the code should attempt to push the txn.
		// The provided sender will fail on the first request. The callback should
		// indicate that the transaction was pushed but that the resolution was not
		// successful.
		{
			txn:          txn3,
			sendFuncs:    newSendFuncs(t, failSendFunc),
			expectPushed: true,
		},
		// Txn3 is staging and expired so the code should attempt to push the txn
		// and then work to resolve its intents. The intent resolution happens in
		// different requests for individual keys and then for spans. This case will
		// allow the individual key intent to be resolved completely but will fail
		// to resolve the span. The callback should indicate that the transaction
		// has been pushed but that the garbage collection was not successful.
		{
			txn: txn3,
			intentSpans: []roachpb.Span{
				{Key: key},
				{Key: key, EndKey: roachpb.Key("b")},
			},
			sendFuncs: newSendFuncs(t,
				singlePushTxnSendFunc(t),
				resolveIntentsSendFunc(t),
				failSendFunc,
			),
			expectPushed: true,
		},
		// Txn3 is staging and expired so the code should attempt to push the txn
		// and then work to resolve its intents. The intent resolution happens in
		// different requests for individual keys and then for spans. This case will
		// allow the individual key intents to be resolved in one request and for
		// the span request to be resolved in another. Finally it will succeed on
		// the GCRequest. This is a positive case and the callback should indicate
		// that the txn has both been pushed and successfully resolved.
		{
			txn: txn3,
			intentSpans: []roachpb.Span{
				{Key: key},
				{Key: roachpb.Key("aa")},
				{Key: key, EndKey: roachpb.Key("b")},
			},
			sendFuncs: func() *sendFuncs {
				s := newSendFuncs(t)
				s.pushFrontLocked(
					singlePushTxnSendFunc(t),
					resolveIntentsSendFuncs(s, 3, 2),
					gcSendFunc(t),
				)
				return s
			}(),
			expectPushed:  true,
			expectSucceed: true,
		},
		// Txn4 is committed so it should not be pushed. Also it has no intents so
		// it should only send a GCRequest. The callback should indicate that there
		// is no push but that the gc has occurred successfully.
		{
			txn:           txn4,
			intentSpans:   []roachpb.Span{},
			sendFuncs:     newSendFuncs(t, gcSendFunc(t)),
			expectSucceed: true,
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			ir := newIntentResolverWithSendFuncs(cfg, c.sendFuncs, stopper)
			var didPush, didSucceed bool
			done := make(chan struct{})
			onComplete := func(pushed, succeeded bool) {
				didPush, didSucceed = pushed, succeeded
				close(done)
			}
			txn := c.txn.Clone()
			txn.LockSpans = append([]roachpb.Span{}, c.intentSpans...)
			err := ir.CleanupTxnIntentsOnGCAsync(ctx, 1, txn, clock.Now(), onComplete)
			if err != nil {
				t.Fatalf("unexpected error sending async transaction")
			}
			<-done
			if c.sendFuncs.len() != 0 {
				t.Errorf("Not all send funcs called")
			}
			if didSucceed != c.expectSucceed {
				t.Fatalf("unexpected success value: got %v, expected %v", didSucceed, c.expectSucceed)
			}
			if didPush != c.expectPushed {
				t.Fatalf("unexpected pushed value: got %v, expected %v", didPush, c.expectPushed)
			}
		})
	}
}

// TestCleanupIntentsAsync verifies that CleanupIntentsAsync either runs
// synchronously or returns an error when there are too many concurrently
// running tasks.
func TestCleanupIntentsAsyncThrottled(t *testing.T) {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	cfg := Config{
		Stopper: stopper,
		Clock:   clock,
	}
	txn := newTransaction("txn", roachpb.Key("a"), 1, clock)
	sf := newSendFuncs(t,
		pushTxnSendFunc(t, 1),
		resolveIntentsSendFunc(t),
	)
	ir := newIntentResolverWithSendFuncs(cfg, sf, stopper)
	// Run defaultTaskLimit tasks which will block until blocker is closed.
	blocker := make(chan struct{})
	defer close(blocker)
	var wg sync.WaitGroup
	wg.Add(defaultTaskLimit)
	for i := 0; i < defaultTaskLimit; i++ {
		if err := ir.runAsyncTask(context.Background(), false, func(context.Context) {
			wg.Done()
			<-blocker
		}); err != nil {
			t.Fatalf("Failed to run blocking async task: %+v", err)
		}
	}
	wg.Wait()
	testIntents := []roachpb.Intent{
		roachpb.MakeIntent(&txn.TxnMeta, roachpb.Key("a")),
	}
	// Running with allowSyncProcessing = false should result in an error and no
	// requests being sent.
	err := ir.CleanupIntentsAsync(context.Background(), testIntents, false, false)
	assert.True(t, errors.Is(err, stop.ErrThrottled))
	// Running with allowSyncProcessing = true should result in the synchronous
	// processing of the intents resulting in no error and the consumption of the
	// sendFuncs.
	err = ir.CleanupIntentsAsync(context.Background(), testIntents, true, false)
	assert.Nil(t, err)
	assert.Equal(t, sf.len(), 0)
}

// TestCleanupIntentsAsync verifies that CleanupIntentsAsync sends the expected
// requests.
func TestCleanupIntentsAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		intents   []roachpb.Intent
		sendFuncs []sendFunc
	}
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn := newTransaction("txn", roachpb.Key("a"), 1, clock)
	testIntents := []roachpb.Intent{
		roachpb.MakeIntent(&txn.TxnMeta, roachpb.Key("a")),
	}
	cases := []testCase{
		{
			intents: testIntents,
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc(t),
				resolveIntentsSendFunc(t),
			},
		},
		{
			intents: testIntents,
			sendFuncs: []sendFunc{
				singlePushTxnSendFunc(t),
				failSendFunc,
			},
		},
		{
			intents: testIntents,
			sendFuncs: []sendFunc{
				failSendFunc,
			},
		},
	}
	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			sf := newSendFuncs(t, c.sendFuncs...)
			cfg := Config{
				Stopper: stopper,
				Clock:   clock,
			}
			ir := newIntentResolverWithSendFuncs(cfg, sf, stopper)
			err := ir.CleanupIntentsAsync(context.Background(), c.intents, true, false)
			testutils.SucceedsSoon(t, func() error {
				if l := sf.len(); l > 0 {
					return fmt.Errorf("Still have %d funcs to send", l)
				}
				return nil
			})
			stopper.Stop(context.Background())
			assert.Nil(t, err, "error from CleanupIntentsAsync")
		})
	}
}

func newSendFuncs(t *testing.T, sf ...sendFunc) *sendFuncs {
	return &sendFuncs{t: t, sendFuncs: sf}
}

type sendFuncs struct {
	t         *testing.T
	mu        syncutil.Mutex
	sendFuncs []sendFunc
}

func (sf *sendFuncs) len() int {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return len(sf.sendFuncs)
}

func (sf *sendFuncs) pushFrontLocked(f ...sendFunc) {
	sf.sendFuncs = append(f, sf.sendFuncs...)
}

func (sf *sendFuncs) popLocked() sendFunc {
	if len(sf.sendFuncs) == 0 {
		sf.t.Errorf("No send funcs left!")
	}
	ret := sf.sendFuncs[0]
	sf.sendFuncs = sf.sendFuncs[1:]
	return ret
}

// TestCleanupTxnIntentsAsync verifies that CleanupTxnIntentsAsync sends the
// expected requests.
func TestCleanupTxnIntentsAsync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		intents   []result.EndTxnIntents
		before    func(*testCase, *IntentResolver) func()
		sendFuncs *sendFuncs
	}
	testEndTxnIntents := []result.EndTxnIntents{
		{
			Txn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					ID:           uuid.MakeV4(),
					MinTimestamp: hlc.Timestamp{WallTime: 123},
				},
				LockSpans: []roachpb.Span{
					{Key: roachpb.Key("a")},
					{Key: roachpb.Key("b")},
					{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")},
				},
			},
		},
	}

	cases := []testCase{
		{
			intents:   testEndTxnIntents,
			sendFuncs: newSendFuncs(t),
			before: func(tc *testCase, ir *IntentResolver) func() {
				_, f := ir.lockInFlightTxnCleanup(context.Background(), tc.intents[0].Txn.ID)
				return f
			},
		},
		{
			intents: testEndTxnIntents,
			sendFuncs: func() *sendFuncs {
				s := newSendFuncs(t)
				s.pushFrontLocked(
					resolveIntentsSendFuncs(s, 4, 2),
					gcSendFunc(t),
				)
				return s
			}(),
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			cfg := Config{
				Stopper: stopper,
				Clock:   clock,
			}
			ir := newIntentResolverWithSendFuncs(cfg, c.sendFuncs, stopper)
			if c.before != nil {
				defer c.before(&c, ir)()
			}
			err := ir.CleanupTxnIntentsAsync(context.Background(), 1, c.intents, false, false)
			testutils.SucceedsSoon(t, func() error {
				if left := c.sendFuncs.len(); left != 0 {
					return fmt.Errorf("still waiting for %d calls", left)
				}
				return nil
			})
			stopper.Stop(context.Background())
			assert.Nil(t, err)
		})
	}
}

// TestCleanupIntents verifies that CleanupIntents sends the expected requests
// and returns the appropriate errors.
func TestCleanupIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	txn := newTransaction("txn", roachpb.Key("a"), roachpb.MinUserPriority, clock)
	// Set txn.ID to a very small value so it's sorted deterministically first.
	txn.ID = uuid.UUID{15: 0x01}
	testIntents := []roachpb.Intent{
		roachpb.MakeIntent(&txn.TxnMeta, roachpb.Key("a")),
	}
	type testCase struct {
		intents     []roachpb.Intent
		sendFuncs   *sendFuncs
		expectedErr bool
		expectedNum int
		cfg         Config
	}
	cases := []testCase{
		{
			intents: testIntents,
			sendFuncs: newSendFuncs(t,
				singlePushTxnSendFunc(t),
				resolveIntentsSendFunc(t),
			),
			expectedNum: 1,
		},
		{
			intents: testIntents,
			sendFuncs: newSendFuncs(t,
				failSendFunc,
			),
			expectedErr: true,
		},
		{
			intents: append(makeTxnIntents(t, clock, 3*intentResolverBatchSize),
				// Three intents with the same transaction will only attempt to push the
				// txn 1 time. Hence 3 full batches plus 1 extra.
				testIntents[0], testIntents[0], testIntents[0]),
			sendFuncs: func() *sendFuncs {
				sf := newSendFuncs(t)
				sf.pushFrontLocked( // don't need to lock
					pushTxnSendFuncs(sf, intentResolverBatchSize),
					resolveIntentsSendFuncs(sf, 102 /* numIntents */, 2 /* minNumReqs */),
					pushTxnSendFuncs(sf, intentResolverBatchSize),
					resolveIntentsSendFuncs(sf, 100 /* numIntents */, 1 /* minNumReqs */),
					pushTxnSendFuncs(sf, intentResolverBatchSize),
					resolveIntentsSendFuncs(sf, 100 /* numIntents */, 1 /* minNumReqs */),
					pushTxnSendFuncs(sf, 1),
					resolveIntentsSendFuncs(sf, 1 /* numIntents */, 1 /* minNumReqs */),
				)
				return sf
			}(),
			expectedNum: 3*intentResolverBatchSize + 3,
			cfg: Config{
				MaxIntentResolutionBatchWait: -1, // disabled
				MaxIntentResolutionBatchIdle: 1 * time.Microsecond,
			},
		},
	}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			c.cfg.Stopper = stopper
			c.cfg.Clock = clock
			ir := newIntentResolverWithSendFuncs(c.cfg, c.sendFuncs, stopper)
			num, err := ir.CleanupIntents(context.Background(), c.intents, clock.Now(), roachpb.PUSH_ABORT, false)
			assert.Equal(t, num, c.expectedNum, "number of resolved intents")
			assert.Equal(t, err != nil, c.expectedErr, "error during CleanupIntents: %v", err)
		})
	}
}

func newTransaction(
	name string, baseKey roachpb.Key, userPriority roachpb.UserPriority, clock *hlc.Clock,
) *roachpb.Transaction {
	var offset int64
	var now hlc.Timestamp
	if clock != nil {
		offset = clock.MaxOffset().Nanoseconds()
		now = clock.Now()
	}
	txn := roachpb.MakeTransaction(name, baseKey, userPriority, now, offset)
	return &txn
}

// makeTxnIntents creates a slice of Intent which each have a unique txn.
func makeTxnIntents(t *testing.T, clock *hlc.Clock, numIntents int) []roachpb.Intent {
	ret := make([]roachpb.Intent, 0, numIntents)
	for i := 0; i < numIntents; i++ {
		txn := newTransaction("test", roachpb.Key("a"), 1, clock)
		ret = append(ret,
			roachpb.MakeIntent(&txn.TxnMeta, txn.Key))
	}
	return ret
}

// sendFunc is a function used to control behavior for a specific request that
// the IntentResolver tries to send. They are used in conjunction with the below
// function to create an IntentResolver with a slice of sendFuncs.
// A library of useful sendFuncs are defined below.
type sendFunc func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

func newIntentResolverWithSendFuncs(
	c Config, sf *sendFuncs, stopper *stop.Stopper,
) *IntentResolver {
	txnSenderFactory := client.NonTransactionalFactoryFunc(
		func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			sf.mu.Lock()
			defer sf.mu.Unlock()
			f := sf.popLocked()
			return f(ba)
		})
	db := client.NewDB(log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}, txnSenderFactory, c.Clock)
	c.DB = db
	c.MaxGCBatchWait = time.Nanosecond
	return New(c)
}

// pushTxnSendFuncs allows the pushing of N txns across several invocations.
func pushTxnSendFuncs(sf *sendFuncs, N int) sendFunc {
	toPush := int64(N)
	var f sendFunc
	f = func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if remaining := atomic.LoadInt64(&toPush); len(ba.Requests) > int(remaining) {
			sf.t.Errorf("expected at most %d PushTxnRequests in batch, got %d",
				remaining, len(ba.Requests))
		}
		nowRemaining := atomic.AddInt64(&toPush, -1*int64(len(ba.Requests)))
		if nowRemaining > 0 {
			sf.pushFrontLocked(f)
		}
		return respForPushTxnBatch(sf.t, ba), nil
	}
	return f
}

func pushTxnSendFunc(t *testing.T, numPushes int) sendFunc {
	return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if len(ba.Requests) != numPushes {
			t.Errorf("expected %d PushTxnRequests in batch, got %d",
				numPushes, len(ba.Requests))
		}
		return respForPushTxnBatch(t, ba), nil
	}
}

func singlePushTxnSendFunc(t *testing.T) sendFunc {
	return pushTxnSendFunc(t, 1)
}

// checkTxnStatusOpt specifies whether some mock handlers for ResolveIntent(s)
// request should assert the intent's status before resolving it, or not.
type checkTxnStatusOpt bool

const (
	// checkTxnAborted makes the mock ResolveIntent check that the intent's txn is
	// aborted (and so the intent would be discarded by the production code).
	checkTxnAborted checkTxnStatusOpt = true

	// NOTE: There should be a checkTxnCommitted option, but no test currently
	// uses it.

	// A bunch of tests use dontCheckTxnStatus because they take shortcuts that
	// causes intents to not be cleaned with a txn that was properly finalized.
	dontCheckTxnStatus checkTxnStatusOpt = false
)

func resolveIntentsSendFuncsEx(
	sf *sendFuncs, numIntents int, minRequests int, opt checkTxnStatusOpt,
) sendFunc {
	toResolve := int64(numIntents)
	reqsSeen := int64(0)
	var f sendFunc
	f = func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if remaining := atomic.LoadInt64(&toResolve); len(ba.Requests) > int(remaining) {
			sf.t.Errorf("expected at most %d ResolveIntentRequests in batch, got %d",
				remaining, len(ba.Requests))
		}
		nowRemaining := atomic.AddInt64(&toResolve, -1*int64(len(ba.Requests)))
		seen := atomic.AddInt64(&reqsSeen, 1)
		if nowRemaining > 0 {
			sf.pushFrontLocked(f)
		} else if seen < int64(minRequests) {
			sf.t.Errorf("expected at least %d requests to resolve %d intents, only saw %d",
				minRequests, numIntents, seen)
		}
		return respForResolveIntentBatch(sf.t, ba, opt), nil
	}
	return f
}

func resolveIntentsSendFuncEx(t *testing.T, checkTxnStatusOpt checkTxnStatusOpt) sendFunc {
	return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return respForResolveIntentBatch(t, ba, checkTxnStatusOpt), nil
	}
}

// resolveIntentsSendFuncs is like resolveIntentsSendFuncsEx, except it never checks
// the intents' txn status.
func resolveIntentsSendFuncs(sf *sendFuncs, numIntents int, minRequests int) sendFunc {
	return resolveIntentsSendFuncsEx(sf, numIntents, minRequests, dontCheckTxnStatus)
}

// resolveIntentsSendFunc is like resolveIntentsSendFuncEx, but it never checks
// the intents' txn status.
func resolveIntentsSendFunc(t *testing.T) sendFunc {
	return resolveIntentsSendFuncEx(t, dontCheckTxnStatus)
}

func failSendFunc(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	return nil, roachpb.NewError(fmt.Errorf("boom"))
}

func gcSendFunc(t *testing.T) sendFunc {
	return func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		resp := &roachpb.BatchResponse{}
		for _, r := range ba.Requests {
			if _, ok := r.GetInner().(*roachpb.GCRequest); !ok {
				t.Errorf("Unexpected request type %T, expected GCRequest", r.GetInner())
			}
			resp.Add(&roachpb.GCResponse{})
		}
		return resp, nil
	}
}

func respForPushTxnBatch(t *testing.T, ba roachpb.BatchRequest) *roachpb.BatchResponse {
	resp := &roachpb.BatchResponse{}
	for _, r := range ba.Requests {
		var txn enginepb.TxnMeta
		if req, ok := r.GetInner().(*roachpb.PushTxnRequest); ok {
			txn = req.PusheeTxn
		} else {
			t.Errorf("Unexpected request type %T, expected PushTxnRequest", r.GetInner())
		}
		resp.Add(&roachpb.PushTxnResponse{
			PusheeTxn: roachpb.Transaction{
				Status:  roachpb.ABORTED,
				TxnMeta: txn,
			},
		})
	}
	return resp
}

func respForResolveIntentBatch(
	t *testing.T, ba roachpb.BatchRequest, checkTxnStatusOpt checkTxnStatusOpt,
) *roachpb.BatchResponse {
	resp := &roachpb.BatchResponse{}
	var status roachpb.TransactionStatus
	for _, r := range ba.Requests {
		if rir, ok := r.GetInner().(*roachpb.ResolveIntentRequest); ok {
			status = rir.AsLockUpdate().Status
			resp.Add(&roachpb.ResolveIntentResponse{})
		} else if rirr, ok := r.GetInner().(*roachpb.ResolveIntentRangeRequest); ok {
			status = rirr.AsLockUpdate().Status
			resp.Add(&roachpb.ResolveIntentRangeResponse{})
		} else {
			t.Errorf("Unexpected request in batch for intent resolution: %T", r.GetInner())
		}
	}
	if checkTxnStatusOpt == checkTxnAborted && status != roachpb.ABORTED {
		t.Errorf("expected txn to be finalized, got status: %s", status)
	}
	return resp
}
