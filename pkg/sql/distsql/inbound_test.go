// Copyright 2018 The Cockroach Authors.
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

package distsql

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/rpc/nodedialer"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/netutil"
	"github.com/znbasedb/znbase/pkg/util/stop"
)

// staticAddressResolver maps execinfra.StaticNodeID to the given address.
func staticAddressResolver(addr net.Addr) nodedialer.AddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, error) {
		if nodeID == runbase.StaticNodeID {
			return addr, nil
		}
		return nil, errors.Errorf("node %d not found", nodeID)
	}
}

// TestOutboxInboundStreamIntegration verifies that if an inbound stream gets
// a draining status from its consumer, that status is propagated back to the
// outbox and there are no goroutine leaks.
func TestOutboxInboundStreamIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ni := base.NodeIDContainer{}
	ni.Set(ctx, 1)
	st := cluster.MakeTestingClusterSettings()
	mt := runbase.MakeDistSQLMetrics(time.Hour /* histogramWindow */)
	srv := NewServer(
		ctx,
		runbase.ServerConfig{
			Settings: st,
			Stopper:  stopper,
			Metrics:  &mt,
			NodeID:   &ni,
		},
	)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcCtx := rpc.NewInsecureTestingContext(clock, stopper)
	rpcSrv := rpc.NewServer(rpcCtx)
	defer rpcSrv.Stop()

	distsqlpb.RegisterDistSQLServer(rpcSrv, srv)
	ln, err := netutil.ListenAndServeGRPC(stopper, rpcSrv, util.IsolatedTestAddr)
	if err != nil {
		t.Fatal(err)
	}

	// The outbox uses this stopper to run a goroutine.
	outboxStopper := stop.NewStopper()
	flowCtx := runbase.FlowCtx{
		Cfg: &runbase.ServerConfig{
			NodeDialer: nodedialer.New(rpcCtx, staticAddressResolver(ln.Addr())),
			Stopper:    outboxStopper,
		},
	}

	streamID := distsqlpb.StreamID(1)
	outbox := flowinfra.NewOutbox(&flowCtx, runbase.StaticNodeID, distsqlpb.FlowID{}, streamID)
	outbox.Init(sqlbase.OneIntCol)

	// WaitGroup for the outbox and inbound stream. If the WaitGroup is done, no
	// goroutines were leaked. Grab the flow's waitGroup to avoid a copy warning.
	f := &flowinfra.FlowBase{}
	wg := f.GetWaitGroup()

	// Use RegisterFlow to register our consumer, which we will control.
	consumer := runbase.NewRowBuffer(sqlbase.OneIntCol, nil /* rows */, runbase.RowBufferArgs{})
	connectionInfo := map[distsqlpb.StreamID]*flowinfra.InboundStreamInfo{
		streamID: flowinfra.NewInboundStreamInfo(
			flowinfra.RowInboundStreamHandler{RowReceiver: consumer},
			wg,
		),
	}
	// Add to the WaitGroup counter for the inbound stream.
	wg.Add(1)
	require.NoError(
		t,
		srv.flowRegistry.RegisterFlow(ctx, distsqlpb.FlowID{}, f, connectionInfo, time.Hour /* timeout */),
	)

	outbox.Start(ctx, wg, func() {})

	// Put the consumer in draining mode, this should propagate all the way back
	// from the inbound stream to the outbox when it attempts to Push a row
	// below.
	consumer.ConsumerDone()

	row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(sqlbase.IntType, tree.NewDInt(tree.DInt(0)))}

	// Now push a row to the outbox's RowChannel and expect the consumer status
	// returned to be DrainRequested. This is wrapped in a SucceedsSoon because
	// the write to the row channel is asynchronous wrt the outbox sending the
	// row and getting back the updated consumer status.
	testutils.SucceedsSoon(t, func() error {
		if cs := outbox.Push(row, nil /* meta */); cs != runbase.DrainRequested {
			return errors.Errorf("unexpected consumer status %s", cs)
		}
		return nil
	})

	// As a producer, we are now required to call ProducerDone after draining. We
	// do so now to simulate the fact that we have no more rows or metadata to
	// send.
	outbox.ProducerDone()

	// Both the outbox and the inbound stream should exit.
	wg.Wait()

	// Wait for outstanding tasks to complete. Specifically, we are waiting for
	// the outbox's drain signal listener to return.
	outboxStopper.Quiesce(ctx)
}
