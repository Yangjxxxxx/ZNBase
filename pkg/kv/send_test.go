// Copyright 2015  The Cockroach Authors.
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

package kv

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/rpc/nodedialer"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/netutil"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

type Node time.Duration

func (n Node) Batch(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if n > 0 {
		time.Sleep(time.Duration(n))
	}
	return &roachpb.BatchResponse{}, nil
}

func (n Node) RangeFeed(_ *roachpb.RangeFeedRequest, _ roachpb.Internal_RangeFeedServer) error {
	panic("unimplemented")
}

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	// This test uses the testing function sendBatch() which does not
	// support setting the node ID on GRPCDialNode(). Disable Node ID
	// checks to avoid log.Fatal.
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	s := rpc.NewServer(rpcContext)
	roachpb.RegisterInternalServer(s, Node(0))
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	nodeDialer := nodedialer.New(rpcContext, func(roachpb.NodeID) (net.Addr, error) {
		return ln.Addr(), nil
	})

	reply, err := sendBatch(context.Background(), t, nil, []net.Addr{ln.Addr()}, rpcContext, nodeDialer)
	if err != nil {
		t.Fatal(err)
	}
	if reply == nil {
		t.Errorf("expected reply")
	}
}

// firstNErrorTransport is a mock transport that sends an error on
// requests to the first N addresses, then succeeds.
type firstNErrorTransport struct {
	replicas  ReplicaSlice
	numErrors int
	numSent   int
}

func (f *firstNErrorTransport) IsExhausted() bool {
	return f.numSent >= len(f.replicas)
}

func (f *firstNErrorTransport) Release() {}

func (f *firstNErrorTransport) SendNext(
	_ context.Context, _ roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	var err error
	if f.numSent < f.numErrors {
		err = roachpb.NewSendError("test")
	}
	f.numSent++
	return &roachpb.BatchResponse{}, err
}

func (f *firstNErrorTransport) NextInternalClient(
	ctx context.Context,
) (context.Context, roachpb.InternalClient, error) {
	panic("unimplemented")
}

func (f *firstNErrorTransport) NextReplica() roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{}
}

func (f *firstNErrorTransport) SkipReplica() {
	panic("SkipReplica not supported")
}

func (*firstNErrorTransport) MoveToFront(roachpb.ReplicaDescriptor) {
}

// TestComplexScenarios verifies various complex success/failure scenarios by
// mocking sendOne.
func TestComplexScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	nodeContext := rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		testutils.NewNodeTestBaseContext(),
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)
	nodeDialer := nodedialer.New(nodeContext, nil)

	// TODO(bdarnell): the retryable flag is no longer used for RPC errors.
	// Rework this test to incorporate application-level errors carried in
	// the BatchResponse.
	testCases := []struct {
		numServers int
		numErrors  int
		success    bool
	}{
		// --- Success scenarios ---
		{1, 0, true},
		{5, 0, true},
		// There are some errors, but enough RPCs succeed.
		{5, 1, true},
		{5, 4, true},
		{5, 2, true},

		// --- Failure scenarios ---
		// All RPCs fail.
		{5, 5, false},
	}
	for i, test := range testCases {
		var serverAddrs []net.Addr
		for j := 0; j < test.numServers; j++ {
			serverAddrs = append(serverAddrs, util.NewUnresolvedAddr("dummy",
				strconv.Itoa(j)))
		}

		reply, err := sendBatch(
			context.Background(),
			t,
			func(
				_ SendOptions,
				_ *nodedialer.Dialer,
				replicas ReplicaSlice,
			) (Transport, error) {
				return &firstNErrorTransport{
					replicas:  replicas,
					numErrors: test.numErrors,
				}, nil
			},
			serverAddrs,
			nodeContext,
			nodeDialer,
		)
		if test.success {
			if err != nil {
				t.Errorf("%d: unexpected error: %s", i, err)
			}
			if reply == nil {
				t.Errorf("%d: expected reply", i)
			}
		} else {
			if err == nil {
				t.Errorf("%d: unexpected success", i)
			}
		}
	}
}

// TestSplitHealthy tests that the splitHealthy helper function sorts healthy
// nodes before unhealthy nodes.
func TestSplitHealthy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type batchClient struct {
		replica roachpb.ReplicaDescriptor
		healthy bool
	}

	testData := []struct {
		in  []batchClient
		out []roachpb.ReplicaDescriptor
	}{
		{nil, []roachpb.ReplicaDescriptor{}},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]roachpb.ReplicaDescriptor{{NodeID: 3}, {NodeID: 1}, {NodeID: 2}},
		},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]roachpb.ReplicaDescriptor{{NodeID: 1}, {NodeID: 3}, {NodeID: 2}},
		},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]roachpb.ReplicaDescriptor{{NodeID: 1}, {NodeID: 2}, {NodeID: 3}},
		},
	}

	for _, td := range testData {
		t.Run("", func(t *testing.T) {
			replicas := make([]roachpb.ReplicaDescriptor, len(td.in))
			var health util.FastIntMap
			for i, r := range td.in {
				replicas[i] = r.replica
				if r.healthy {
					health.Set(i, healthHealthy)
				} else {
					health.Set(i, healthUnhealthy)
				}
			}
			splitHealthy(replicas, health)
			if !reflect.DeepEqual(replicas, td.out) {
				t.Errorf("splitHealthy(...) = %+v not %+v", replicas, td.out)
			}
		})
	}
}

func makeReplicas(addrs ...net.Addr) ReplicaSlice {
	replicas := make(ReplicaSlice, len(addrs))
	for i, addr := range addrs {
		replicas[i].NodeDesc = &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i + 1),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
	}
	return replicas
}

// sendBatch sends Batch requests to specified addresses using send.
func sendBatch(
	ctx context.Context,
	t *testing.T,
	transportFactory TransportFactory,
	addrs []net.Addr,
	rpcContext *rpc.Context,
	nodeDialer *nodedialer.Dialer,
) (*roachpb.BatchResponse, error) {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	g := makeGossip(t, stopper, rpcContext)

	desc := new(roachpb.RangeDescriptor)
	desc.StartKey = roachpb.RKeyMin
	desc.EndKey = roachpb.RKeyMax
	for i, addr := range addrs {
		nd := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i + 1),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
		err := g.AddInfoProto(gossip.MakeNodeIDKey(nd.NodeID), nd, gossip.NodeDescriptorTTL)
		require.NoError(t, err)

		desc.Replicas = append(desc.Replicas,
			roachpb.ReplicaDescriptor{
				NodeID:    nd.NodeID,
				StoreID:   0,
				ReplicaID: roachpb.ReplicaID(i + 1),
			})
	}
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: transportFactory,
		},
	}, nil)
	if err := ds.rangeCache.InsertRangeDescriptors(ctx, *desc); err != nil {
		return nil, err
	}
	return ds.sendToReplicas(
		ctx,
		roachpb.BatchRequest{},
		SendOptions{metrics: &ds.metrics},
		0, /* rangeID */
		makeReplicas(addrs...),
		nodeDialer,
		roachpb.ReplicaDescriptor{},
		false, /* withCommit */
	)
}
