// Copyright 2019  The Cockroach Authors.

package vecflow_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra/vecflow/vecrpc"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/randutil"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

type shutdownScenario struct {
	string
}

var (
	consumerDone      = shutdownScenario{"ConsumerDone"}
	consumerClosed    = shutdownScenario{"ConsumerClosed"}
	shutdownScenarios = []shutdownScenario{consumerDone, consumerClosed}
)

// TestVectorizedFlowShutdown tests that closing the materializer correctly
// closes all the infrastructure corresponding to the flow ending in that
// materializer. Namely:
// - on a remote node, it creates an exec.HashRouter with 3 outputs (with a
// corresponding to each Outbox) as well as 3 standalone Outboxes;
// - on a local node, it creates 6 exec.Inboxes that feed into an unordered
// synchronizer which then outputs all the data into a materializer.
// The resulting scheme looks as follows:
//
//            Remote Node             |                  Local Node
//                                    |
//             -> output -> Outbox -> | -> Inbox -> |
//            |                       |
// Hash Router -> output -> Outbox -> | -> Inbox -> |
//            |                       |
//             -> output -> Outbox -> | -> Inbox -> |
//                                    |              -> Synchronizer -> materializer
//                          Outbox -> | -> Inbox -> |
//                                    |
//                          Outbox -> | -> Inbox -> |
//                                    |
//                          Outbox -> | -> Inbox -> |
//
// Also, with 50% probability, another remote node with the chain of an Outbox
// and Inbox is placed between the synchronizer and materializer. The resulting
// scheme then looks as follows:
//
//            Remote Node             |            Another Remote Node             |         Local Node
//                                    |                                            |
//             -> output -> Outbox -> | -> Inbox ->                                |
//            |                       |             |                              |
// Hash Router -> output -> Outbox -> | -> Inbox ->                                |
//            |                       |             |                              |
//             -> output -> Outbox -> | -> Inbox ->                                |
//                                    |             | -> Synchronizer -> Outbox -> | -> Inbox -> materializer
//                          Outbox -> | -> Inbox ->                                |
//                                    |             |                              |
//                          Outbox -> | -> Inbox ->                                |
//                                    |             |                              |
//                          Outbox -> | -> Inbox ->                                |
//
// Remote nodes are simulated by having separate contexts and separate outbox
// registries.
//
// Additionally, all Outboxes have a single metadata source. In ConsumerDone
// shutdown scenario, we check that the metadata has been successfully
// propagated from all of the metadata sources.
func TestVectorizedFlowShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	_, mockServer, addr, err := distsqlpb.StartMockDistSQLServer(
		hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, runbase.StaticNodeID,
	)
	require.NoError(t, err)
	dialer := &distsqlpb.MockDialer{Addr: addr}
	defer dialer.Close()

	for run := 0; run < 10; run++ {
		for _, shutdownOperation := range shutdownScenarios {
			t.Run(fmt.Sprintf("shutdownScenario=%s", shutdownOperation.string), func(t *testing.T) {
				ctxLocal := context.Background()
				ctxRemote, cancelRemote := context.WithCancel(context.Background())
				// Linter says there is a possibility of "context leak" because
				// cancelRemote variable may not be used, so we defer the call to it.
				// This does not change anything about the test since we're blocking on
				// the wait group and we will call cancelRemote() below, so this defer
				// is actually a noop.
				defer cancelRemote()
				st := cluster.MakeTestingClusterSettings()
				evalCtx := tree.MakeTestingEvalContext(st)
				defer evalCtx.Stop(ctxLocal)
				flowCtx := &runbase.FlowCtx{
					EvalCtx: &evalCtx,
					Cfg:     &runbase.ServerConfig{Settings: st},
				}
				rng, _ := randutil.NewPseudoRand()
				var (
					err             error
					wg              sync.WaitGroup
					typs            = []coltypes.T{coltypes.Int64}
					semtyps         = []types1.T{*types1.Int}
					hashRouterInput = vecexec.NewRandomDataOp(
						testAllocator,
						rng,
						vecexec.RandomDataOpArgs{
							DeterministicTyps: typs,
							// Set a high number of batches to ensure that the HashRouter is
							// very far from being finished when the flow is shut down.
							NumBatches: math.MaxInt64,
							Selection:  true,
						},
					)
					numHashRouterOutputs        = 3
					numInboxes                  = numHashRouterOutputs + 3
					inboxes                     = make([]*vecrpc.Inbox, 0, numInboxes+1)
					handleStreamErrCh           = make([]chan error, numInboxes+1)
					synchronizerInputs          = make([]vecexec.Operator, 0, numInboxes)
					materializerMetadataSources = make([]distsqlpb.MetadataSource, 0, numInboxes+1)
					streamID                    = 0
					addAnotherRemote            = rng.Float64() < 0.5
				)

				// Note that the components of the vectorized flow will run
				// concurrently, so we cannot reuse testAllocator and/or testMemAcc in
				// all of them, and we need to instantiate separate objects.
				hashRouterMemAccount := testMemMonitor.MakeBoundAccount()
				defer hashRouterMemAccount.Close(ctxRemote)
				hashRouter, hashRouterOutputs := vecexec.NewHashRouter(
					vecexec.NewAllocator(ctxRemote, &hashRouterMemAccount), hashRouterInput, typs, []int{0}, numHashRouterOutputs,
				)
				for i := 0; i < numInboxes; i++ {
					inboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer inboxMemAccount.Close(ctxLocal)
					inbox, err := vecrpc.NewInbox(
						vecexec.NewAllocator(ctxLocal, &inboxMemAccount), typs, distsqlpb.StreamID(streamID),
					)
					require.NoError(t, err)
					inboxes = append(inboxes, inbox)
					materializerMetadataSources = append(materializerMetadataSources, inbox)
					synchronizerInputs = append(synchronizerInputs, vecexec.Operator(inbox))
				}
				synchronizer := vecexec.NewParallelUnorderedSynchronizer(synchronizerInputs, typs, &wg)
				flowID := distsqlpb.FlowID{UUID: uuid.MakeV4()}

				runOutboxInbox := func(
					ctx context.Context,
					cancelFn context.CancelFunc,
					outboxMemAcc *mon.BoundAccount,
					outboxInput vecexec.Operator,
					inbox *vecrpc.Inbox,
					id int,
					outboxMetadataSources []distsqlpb.MetadataSource,
				) {
					outbox, err := vecrpc.NewOutbox(
						vecexec.NewAllocator(ctx, outboxMemAcc),
						outboxInput,
						typs,
						append(outboxMetadataSources,
							distsqlpb.CallbackMetadataSource{
								DrainMetaCb: func(ctx context.Context) []distsqlpb.ProducerMetadata {
									return []distsqlpb.ProducerMetadata{{Err: errors.Errorf("%d", id)}}
								},
							},
						),
					)
					require.NoError(t, err)
					wg.Add(1)
					go func(id int) {
						outbox.Run(ctx, dialer, runbase.StaticNodeID, flowID, distsqlpb.StreamID(id), cancelFn)
						wg.Done()
					}(id)

					require.NoError(t, err)
					serverStreamNotification := <-mockServer.InboundStreams
					serverStream := serverStreamNotification.Stream
					handleStreamErrCh[id] = make(chan error, 1)
					doneFn := func() { close(serverStreamNotification.Donec) }
					wg.Add(1)
					go func(id int, stream distsqlpb.DistSQL_FlowStreamServer, doneFn func()) {
						handleStreamErrCh[id] <- inbox.RunWithStream(stream.Context(), stream)
						doneFn()
						wg.Done()
					}(id, serverStream, doneFn)
				}

				wg.Add(1)
				go func() {
					hashRouter.Run(ctxRemote)
					wg.Done()
				}()
				for i := 0; i < numInboxes; i++ {
					var outboxMetadataSources []distsqlpb.MetadataSource
					outboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer outboxMemAccount.Close(ctxRemote)
					if i < numHashRouterOutputs {
						if i == 0 {
							// Only one outbox should drain the hash router.
							outboxMetadataSources = append(outboxMetadataSources, hashRouter)
						}
						runOutboxInbox(ctxRemote, cancelRemote, &outboxMemAccount, hashRouterOutputs[i], inboxes[i], streamID, outboxMetadataSources)
					} else {
						sourceMemAccount := testMemMonitor.MakeBoundAccount()
						defer sourceMemAccount.Close(ctxRemote)
						batch := vecexec.NewAllocator(ctxRemote, &sourceMemAccount).NewMemBatch(typs)
						batch.SetLength(coldata.BatchSize())
						runOutboxInbox(ctxRemote, cancelRemote, &outboxMemAccount, vecexec.NewRepeatableBatchSource(batch), inboxes[i], streamID, outboxMetadataSources)
					}
					streamID++
				}

				var materializerInput vecexec.Operator
				ctxAnotherRemote, cancelAnotherRemote := context.WithCancel(context.Background())
				if addAnotherRemote {
					// Add another "remote" node to the flow.
					inboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer inboxMemAccount.Close(ctxAnotherRemote)
					inbox, err := vecrpc.NewInbox(
						vecexec.NewAllocator(ctxAnotherRemote, &inboxMemAccount),
						typs, distsqlpb.StreamID(streamID),
					)
					require.NoError(t, err)
					inboxes = append(inboxes, inbox)
					outboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer outboxMemAccount.Close(ctxAnotherRemote)
					runOutboxInbox(ctxAnotherRemote, cancelAnotherRemote, &outboxMemAccount, synchronizer, inbox, streamID, materializerMetadataSources)
					streamID++
					// There is now only a single Inbox on the "local" node which is the
					// only metadata source.
					materializerMetadataSources = []distsqlpb.MetadataSource{inbox}
					materializerInput = inbox
				} else {
					materializerInput = synchronizer
				}

				ctxLocal, cancelLocal := context.WithCancel(ctxLocal)
				materializer, err := vecexec.NewMaterializer(
					flowCtx,
					1, /* processorID */
					materializerInput,
					semtyps,
					&distsqlpb.PostProcessSpec{},
					nil, /* output */
					materializerMetadataSources,
					nil, /* outputStatsToTrace */
					func() context.CancelFunc { return cancelLocal },
				)
				require.NoError(t, err)
				materializer.Start(ctxLocal)

				for i := 0; i < 10; i++ {
					row, meta := materializer.Next()
					require.NotNil(t, row)
					require.Nil(t, meta)
				}
				switch shutdownOperation {
				case consumerDone:
					materializer.ConsumerDone()
					receivedMetaFromID := make([]bool, streamID)
					metaCount := 0
					for {
						row, meta := materializer.Next()
						require.Nil(t, row)
						if meta == nil {
							break
						}
						metaCount++
						require.NotNil(t, meta.Err)
						id, err := strconv.Atoi(meta.Err.Error())
						require.NoError(t, err)
						require.False(t, receivedMetaFromID[id])
						receivedMetaFromID[id] = true
					}
					require.Equal(t, streamID, metaCount, fmt.Sprintf("received metadata from Outbox %+v", receivedMetaFromID))
				case consumerClosed:
					materializer.ConsumerClosed()
				}

				// When Outboxes are setup through vectorizedFlowCreator, the latter
				// keeps track of how many outboxes are on the node. When the last one
				// exits (and if there is no materializer on that node),
				// vectorizedFlowCreator will cancel the flow context of the node. To
				// simulate this, we manually cancel contexts of both remote nodes.
				cancelRemote()
				cancelAnotherRemote()

				for i := range inboxes {
					err = <-handleStreamErrCh[i]
					// We either should get no error or a context cancellation error.
					if err != nil {
						require.True(t, testutils.IsError(err, "context canceled"), err)
					}
				}
				wg.Wait()
			})
		}
	}
}
