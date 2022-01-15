// Copyright 2019  The Cockroach Authors.
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

// Package replicastrategy provides functionality for distsqlplan to choose a
// replica for a range.
package replicastrategy

import (
	"context"
	"math"
	"math/rand"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// Policy determines how an Strategy should select a replica.
type Policy byte

var (
	// RandomChoice chooses lease replicas randomly.
	RandomChoice = RegisterPolicy(newRandomOracleFactory)
	// BinPackingChoice bin-packs the choices.
	BinPackingChoice = RegisterPolicy(newBinPackingOracleFactory)
	// ClosestChoice chooses the node closest to the current node.
	ClosestChoice = RegisterPolicy(newClosestOracleFactory)
)

// Config is used to construct an StrategyFactory.
type Config struct {
	NodeDesc         roachpb.NodeDescriptor
	Settings         *cluster.Settings
	Gossip           *gossip.Gossip
	RPCContext       *rpc.Context
	LeaseHolderCache *kv.LeaseHolderCache
}

// Strategy 用于为范围选择租约持有者。这
// 接口被提取出来，这样我们可以进行不同选择的实验
// 政策。
// 请注意，从随机开始的选择可以作为自我实现的预言
// -如果没有活动的租赁，节点将被要求执行的一部分
// 查询(选择的节点)将获得新租约。
type Strategy interface {
	// ChoosePreferredReplica返回一个范围的选择。实现者是
	//免费使用queryState参数，其中有关于范围数量的信息
	//当前SQL查询已经由每个节点处理。国家不是
	//用这种方法的结果更新;打电话的人负责这件事。
	//
	//如果流言中没有信息，可以返回一个RangeUnavailableError
	//关于任何可能尝试的节点。
	ChoosePreferredReplica(
		context.Context, roachpb.RangeDescriptor, QueryState,
	) (kv.ReplicaInfo, error)
}

// StrategyFactory 为Txn创建了一个strategy
type StrategyFactory interface {
	Oracle(*client.Txn) Strategy
}

// StrategyFactoryFunc creates an StrategyFactory from a Config.
type StrategyFactoryFunc func(Config) StrategyFactory

// NewStrategyFactory creates an oracle with the given policy.
func NewStrategyFactory(policy Policy, cfg Config) StrategyFactory {
	ff, ok := strategyFactoryFuncs[policy]
	if !ok {
		panic(errors.Errorf("unknown Policy %v", policy))
	}
	return ff(cfg)
}

// RegisterPolicy creates a new policy given a function which constructs an
// StrategyFactory. RegisterPolicy is intended to be called only during init and
// is not safe for concurrent use.
func RegisterPolicy(f StrategyFactoryFunc) Policy {
	if len(strategyFactoryFuncs) == 255 {
		panic("Can only register 255 Policy instances")
	}
	r := Policy(len(strategyFactoryFuncs))
	strategyFactoryFuncs[r] = f
	return r
}

var strategyFactoryFuncs = map[Policy]StrategyFactoryFunc{}

// QueryState encapsulates the history of assignments of ranges to nodes
// done by an oracle on behalf of one particular query.
type QueryState struct {
	RangesPerNode  map[roachpb.NodeID]int
	AssignedRanges map[roachpb.RangeID]kv.ReplicaInfo
}

// MakeQueryState creates an initialized QueryState.
func MakeQueryState() QueryState {
	return QueryState{
		RangesPerNode:  make(map[roachpb.NodeID]int),
		AssignedRanges: make(map[roachpb.RangeID]kv.ReplicaInfo),
	}
}

// randomOracle is a Strategy that chooses the lease holder randomly
// among the replicas in a range descriptor.
type randomOracle struct {
	gossip *gossip.Gossip
}

var _ StrategyFactory = &randomOracle{}

func newRandomOracleFactory(cfg Config) StrategyFactory {
	return &randomOracle{gossip: cfg.Gossip}
}

func (o *randomOracle) Oracle(_ *client.Txn) Strategy {
	return o
}

func (o *randomOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, _ QueryState,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	return replicas[rand.Intn(len(replicas))], nil
}

type closestOracle struct {
	gossip      *gossip.Gossip
	latencyFunc kv.LatencyFunc
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

func newClosestOracleFactory(cfg Config) StrategyFactory {
	return &closestOracle{
		latencyFunc: latencyFunc(cfg.RPCContext),
		gossip:      cfg.Gossip,
		nodeDesc:    cfg.NodeDesc,
	}
}

func (o *closestOracle) Oracle(_ *client.Txn) Strategy {
	return o
}

func (o *closestOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, queryState QueryState,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc, o.latencyFunc)
	return replicas[0], nil
}

// maxPreferredRangesPerLeaseHolder applies to the binPackingOracle.
// When choosing lease holders, we try to choose the same node for all the
// ranges applicable, until we hit this limit. The rationale is that maybe a
// bunch of those ranges don't have an active lease, so our choice is going to
// be self-fulfilling. If so, we want to collocate the lease holders. But above
// some limit, we prefer to take the parallelism and distribute to multiple
// nodes. The actual number used is based on nothing.
const maxPreferredRangesPerLeaseHolder = 10

// binPackingOracle coalesces choices together, so it gives preference to
// replicas on nodes that are already assumed to be lease holders for some other
// ranges that are going to be part of a single query.
// Secondarily, it gives preference to replicas that are "close" to the current
// node.
// Finally, it tries not to overload any node.
type binPackingOracle struct {
	leaseHolderCache                 *kv.LeaseHolderCache
	maxPreferredRangesPerLeaseHolder int
	gossip                           *gossip.Gossip
	latencyFunc                      kv.LatencyFunc
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

func newBinPackingOracleFactory(cfg Config) StrategyFactory {
	return &binPackingOracle{
		maxPreferredRangesPerLeaseHolder: maxPreferredRangesPerLeaseHolder,
		gossip:                           cfg.Gossip,
		nodeDesc:                         cfg.NodeDesc,
		leaseHolderCache:                 cfg.LeaseHolderCache,
		latencyFunc:                      latencyFunc(cfg.RPCContext),
	}
}

var _ StrategyFactory = &binPackingOracle{}

func (o *binPackingOracle) Oracle(_ *client.Txn) Strategy {
	return o
}

func (o *binPackingOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, queryState QueryState,
) (kv.ReplicaInfo, error) {
	// Attempt to find a cached lease holder and use it if found.
	// If an error occurs, ignore it and proceed to choose a replica below.
	if storeID, ok := o.leaseHolderCache.Lookup(ctx, desc.RangeID); ok {
		var repl kv.ReplicaInfo
		repl.ReplicaDescriptor = roachpb.ReplicaDescriptor{StoreID: storeID}
		// Fill in the node descriptor.
		nodeID, err := o.gossip.GetNodeIDForStoreID(storeID)
		if err != nil {
			log.VEventf(ctx, 2, "failed to lookup store %d: %s", storeID, err)
		} else if nd, err := o.gossip.GetNodeDescriptor(nodeID); err != nil {
			log.VEventf(ctx, 2, "failed to resolve node %d: %s", nodeID, err)
		} else {
			repl.ReplicaDescriptor.NodeID = nodeID
			repl.NodeDesc = nd
			return repl, nil
		}
	}

	// If we've assigned the range before, return that assignment.
	if repl, ok := queryState.AssignedRanges[desc.RangeID]; ok {
		return repl, nil
	}

	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc, o.latencyFunc)

	// Look for a replica that has been assigned some ranges, but it's not yet full.
	minLoad := int(math.MaxInt32)
	var leastLoadedIdx int
	for i, repl := range replicas {
		assignedRanges := queryState.RangesPerNode[repl.NodeID]
		if assignedRanges != 0 && assignedRanges < o.maxPreferredRangesPerLeaseHolder {
			return repl, nil
		}
		if assignedRanges < minLoad {
			leastLoadedIdx = i
			minLoad = assignedRanges
		}
	}
	// Either no replica was assigned any previous ranges, or all replicas are
	// full. Use the least-loaded one (if all the load is 0, then the closest
	// replica is returned).
	return replicas[leastLoadedIdx], nil
}

// replicaSliceOrErr returns a ReplicaSlice for the given range descriptor.
// ReplicaSlices are restricted to replicas on nodes for which a NodeDescriptor
// is available in gossip. If no nodes are available, a RangeUnavailableError is
// returned.
func replicaSliceOrErr(desc roachpb.RangeDescriptor, gsp *gossip.Gossip) (kv.ReplicaSlice, error) {
	replicas := kv.NewReplicaSlice(gsp, &desc)
	if len(replicas) == 0 {
		// We couldn't get node descriptors for any replicas.
		var nodeIDs []roachpb.NodeID
		for _, r := range desc.Replicas {
			nodeIDs = append(nodeIDs, r.NodeID)
		}
		return kv.ReplicaSlice{}, sqlbase.NewRangeUnavailableError(
			desc.RangeID, errors.Errorf("node info not available in gossip"), nodeIDs...)
	}
	return replicas, nil
}

// latencyFunc returns a kv.LatencyFunc for use with
// Replicas.OptimizeReplicaOrder.
func latencyFunc(rpcCtx *rpc.Context) kv.LatencyFunc {
	if rpcCtx != nil {
		return rpcCtx.RemoteClocks.Latency
	}
	return nil
}
