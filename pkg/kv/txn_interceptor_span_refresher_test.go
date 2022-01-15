package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

func makeMockTxnSpanRefresher() (txnSpanRefresher, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	return txnSpanRefresher{
		st:               cluster.MakeTestingClusterSettings(),
		knobs:            new(ClientTestingKnobs),
		wrapped:          mockSender,
		canAutoRetry:     true,
		autoRetryCounter: metric.NewCounter(metaAutoRetriesRates),
	}, mockSender
}

// TestTxnSpanRefresherSavepoint checks that the span refresher can savepoint
// its state and restore it.
func TestTxnSpanRefresherSavepoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tsr, mockSender := makeMockTxnSpanRefresher()

	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	txn := makeTxnProto()

	read := func(key roachpb.Key) {
		var ba roachpb.BatchRequest
		ba.Header = roachpb.Header{Txn: &txn}
		getArgs := roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: key}}
		ba.Add(&getArgs)
		mockSender.MockSend(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			require.Len(t, ba.Requests, 1)
			require.IsType(t, &roachpb.GetRequest{}, ba.Requests[0].GetInner())

			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})
		br, pErr := tsr.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	}
	read(keyA)
	require.Equal(t, []roachpb.Span{{Key: keyA}}, tsr.refreshSpans.asSlice())

	s := savepoint{}
	tsr.createSavepointLocked(ctx, &s)

	// Another read after the savepoint was created.
	read(keyB)
	require.Equal(t, []roachpb.Span{{Key: keyA}, {Key: keyB}}, tsr.refreshSpans.asSlice())

	require.Equal(t, []roachpb.Span{{Key: keyA}}, s.refreshSpans)
	require.False(t, s.refreshInvalid)

	// Rollback the savepoint and check that refresh spans were overwritten.
	tsr.rollbackToSavepointLocked(ctx, s)
	require.Equal(t, []roachpb.Span{{Key: keyA}}, tsr.refreshSpans.asSlice())

	// Set MaxTxnRefreshSpansBytes limit low and then exceed it.
	MaxTxnRefreshSpansBytes.Override(&tsr.st.SV, 1)
	read(keyB)
	require.True(t, tsr.refreshInvalid)

	// Check that rolling back to the savepoint resets refreshInvalid.
	tsr.rollbackToSavepointLocked(ctx, s)
	require.False(t, tsr.refreshInvalid)

	// Exceed the limit again and then create a savepoint.
	read(keyB)
	require.True(t, tsr.refreshInvalid)
	s = savepoint{}
	tsr.createSavepointLocked(ctx, &s)
	require.True(t, s.refreshInvalid)
	require.Empty(t, s.refreshSpans)
	// Rollback to the savepoint check that refreshes are still invalid.
	tsr.rollbackToSavepointLocked(ctx, s)
	require.Empty(t, tsr.refreshSpans)
	require.True(t, tsr.refreshInvalid)
}

// TestTxnSpanRefresherRefreshesTransactions tests that the txnSpanRefresher
// refreshes the transaction's read and write spans if it observes an error
// that indicates that the transaction's timestamp is being pushed.
func TestTxnSpanRefresherRefreshesTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	txn := makeTxnProto()
	txn.UpdateObservedTimestamp(1, txn.WriteTimestamp.Add(20, 0))
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	cases := []struct {
		// If name is not set, the test will use pErr.String().
		name string
		// OnFirstSend, if set, is invoked to evaluate the batch. If not set, pErr()
		// will be used to provide an error.
		onFirstSend  func(request roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
		pErr         func() *roachpb.Error
		expRefresh   bool
		expRefreshTS hlc.Timestamp
	}{
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewError(
					&roachpb.TransactionRetryError{Reason: roachpb.RETRY_SERIALIZABLE})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp,
		},
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewError(
					&roachpb.TransactionRetryError{Reason: roachpb.RETRY_WRITE_TOO_OLD})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp,
		},
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewError(
					&roachpb.WriteTooOldError{ActualTimestamp: txn.WriteTimestamp.Add(15, 0)})
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(15, 0),
		},
		{
			pErr: func() *roachpb.Error {
				pErr := roachpb.NewError(&roachpb.ReadWithinUncertaintyIntervalError{})
				pErr.OriginNode = 1
				return pErr
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(20, 0), // see UpdateObservedTimestamp
		},
		{
			pErr: func() *roachpb.Error {
				pErr := roachpb.NewError(
					&roachpb.ReadWithinUncertaintyIntervalError{
						ExistingTimestamp: txn.WriteTimestamp.Add(25, 0),
					})
				pErr.OriginNode = 1
				return pErr
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(25, 1), // see ExistingTimestamp
		},
		{
			pErr: func() *roachpb.Error {
				return roachpb.NewErrorf("no refresh")
			},
			expRefresh: false,
		},
		{
			name: "write_too_old flag",
			onFirstSend: func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()
				br.Txn.WriteTooOld = true
				br.Txn.WriteTimestamp = txn.WriteTimestamp.Add(20, 1)
				return br, nil
			},
			expRefresh:   true,
			expRefreshTS: txn.WriteTimestamp.Add(20, 1), // Same as br.Txn.WriteTimestamp.
		},
	}
	for _, tc := range cases {
		name := tc.name
		if name == "" {
			name = tc.pErr().String()
		}
		if (tc.onFirstSend != nil) == (tc.pErr != nil) {
			panic("exactly one tc.onFirstSend and tc.pErr must be set")
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tsr, mockSender := makeMockTxnSpanRefresher()

			// Collect some refresh spans.
			var ba roachpb.BatchRequest
			ba.Header = roachpb.Header{Txn: txn.Clone()} // clone txn since it's shared between subtests
			getArgs := roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
			delRangeArgs := roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB}}
			ba.Add(&getArgs, &delRangeArgs)

			br, pErr := tsr.SendLocked(ctx, ba)
			require.Nil(t, pErr)
			require.NotNil(t, br)

			require.Equal(t, []roachpb.Span{getArgs.Span(), delRangeArgs.Span()}, tsr.refreshSpans.asSlice())
			require.False(t, tsr.refreshInvalid)
			require.Equal(t, br.Txn.ReadTimestamp, tsr.refreshedTimestamp)

			// Hook up a chain of mocking functions.
			onFirstSend := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				require.Len(t, ba.Requests, 1)
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

				// Return a transaction retry error.
				if tc.onFirstSend != nil {
					return tc.onFirstSend(ba)
				}
				pErr = tc.pErr()
				pErr.SetTxn(ba.Txn)
				return nil, pErr
			}
			onSecondSend := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				// Should not be called if !expRefresh.
				require.True(t, tc.expRefresh)

				require.Len(t, ba.Requests, 1)
				require.IsType(t, &roachpb.PutRequest{}, ba.Requests[0].GetInner())

				// Don't return an error.
				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			onRefresh := func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				// Should not be called if !expRefresh.
				require.True(t, tc.expRefresh)

				require.Len(t, ba.Requests, 2)
				require.IsType(t, &roachpb.RefreshRequest{}, ba.Requests[0].GetInner())
				require.IsType(t, &roachpb.RefreshRangeRequest{}, ba.Requests[1].GetInner())

				refReq := ba.Requests[0].GetRefresh()
				require.Equal(t, getArgs.Span(), refReq.Span())

				refRngReq := ba.Requests[1].GetRefreshRange()
				require.Equal(t, delRangeArgs.Span(), refRngReq.Span())

				br = ba.CreateReply()
				br.Txn = ba.Txn
				return br, nil
			}
			mockSender.ChainMockSend(onFirstSend, onRefresh, onSecondSend)

			// Send a request that will hit a retry error. Depending on the
			// error type, we may or may not perform a refresh.
			ba.Requests = nil
			putArgs := roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
			ba.Add(&putArgs)

			br, pErr = tsr.SendLocked(ctx, ba)
			if tc.expRefresh {
				require.Nil(t, pErr)
				require.NotNil(t, br)
				require.Equal(t, tc.expRefreshTS, br.Txn.WriteTimestamp)
				require.Equal(t, tc.expRefreshTS, br.Txn.ReadTimestamp)
				require.Equal(t, tc.expRefreshTS, tsr.refreshedTimestamp)
			} else {
				require.Nil(t, br)
				require.NotNil(t, pErr)
				require.Equal(t, ba.Txn.ReadTimestamp, tsr.refreshedTimestamp)
			}
		})
	}
}
