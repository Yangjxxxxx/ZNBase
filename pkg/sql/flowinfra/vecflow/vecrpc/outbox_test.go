// Copyright 2019  The Cockroach Authors.

package vecrpc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestOutboxCatchesPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var (
		input    = vecexec.NewBatchBuffer()
		typs     = []coltypes.T{coltypes.Int64}
		rpcLayer = makeMockFlowStreamRPCLayer()
	)
	outbox, err := NewOutbox(testAllocator, input, typs, nil)
	require.NoError(t, err)

	// This test relies on the fact that BatchBuffer panics when there are no
	// batches to return. Verify this assumption.
	require.Panics(t, func() { input.Next(ctx) })

	// The actual test verifies that the Outbox handles input execution tree
	// panics by not panicking and returning.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)
		wg.Done()
	}()

	inboxMemAccount := testMemMonitor.MakeBoundAccount()
	defer inboxMemAccount.Close(ctx)
	inbox, err := NewInbox(
		vecexec.NewAllocator(ctx, &inboxMemAccount), typs, distsqlpb.StreamID(0),
	)
	require.NoError(t, err)

	streamHandlerErrCh := handleStream(ctx, inbox, rpcLayer.server, func() { close(rpcLayer.server.csChan) })

	// The outbox will be sending the panic as metadata eagerly. This Next call
	// is valid, but should return a zero-length batch, indicating that the caller
	// should call DrainMeta.
	require.True(t, inbox.Next(ctx).Length() == 0)

	// Expect the panic as an error in DrainMeta.
	meta := inbox.DrainMeta(ctx)

	require.True(t, len(meta) == 1)
	require.True(t, testutils.IsError(meta[0].Err, "runtime error: index out of range"), meta[0])

	require.NoError(t, <-streamHandlerErrCh)
	wg.Wait()
}

func TestOutboxDrainsMetadataSources(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var (
		input = vecexec.NewBatchBuffer()
		typs  = []coltypes.T{coltypes.Int64}
	)

	// Define common function that returns both an Outbox and a pointer to a
	// uint32 that is set atomically when the outbox drains a metadata source.
	newOutboxWithMetaSources := func(allocator *vecexec.Allocator) (*Outbox, *uint32, error) {
		var sourceDrained uint32
		outbox, err := NewOutbox(
			allocator,
			input,
			typs,
			[]distsqlpb.MetadataSource{
				distsqlpb.CallbackMetadataSource{
					DrainMetaCb: func(context.Context) []distsqlpb.ProducerMetadata {
						atomic.StoreUint32(&sourceDrained, 1)
						return nil
					},
				},
			},
		)
		if err != nil {
			return nil, nil, err
		}
		return outbox, &sourceDrained, nil
	}

	t.Run("AfterSuccessfulRun", func(t *testing.T) {
		rpcLayer := makeMockFlowStreamRPCLayer()
		outboxMemAccount := testMemMonitor.MakeBoundAccount()
		defer outboxMemAccount.Close(ctx)
		outbox, sourceDrained, err := newOutboxWithMetaSources(
			vecexec.NewAllocator(ctx, &outboxMemAccount),
		)
		require.NoError(t, err)

		b := testAllocator.NewMemBatch(typs)
		b.SetLength(0)
		input.Add(b)

		// Close the csChan to unblock the Recv goroutine (we don't need it for this
		// test).
		close(rpcLayer.client.csChan)
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)

		require.True(t, atomic.LoadUint32(sourceDrained) == 1)
	})

	// This is similar to TestOutboxCatchesPanics, but focuses on verifying that
	// the Outbox drains its metadata sources even after an error.
	t.Run("AfterOutboxError", func(t *testing.T) {
		// This test, similar to TestOutboxCatchesPanics, relies on the fact that
		// a BatchBuffer panics when there are no batches to return.
		require.Panics(t, func() { input.Next(ctx) })

		rpcLayer := makeMockFlowStreamRPCLayer()
		outbox, sourceDrained, err := newOutboxWithMetaSources(testAllocator)
		require.NoError(t, err)

		close(rpcLayer.client.csChan)
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)

		require.True(t, atomic.LoadUint32(sourceDrained) == 1)
	})
}
