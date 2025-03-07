// Copyright 2018  The Cockroach Authors.
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

package storage

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/util/contextutil"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"go.etcd.io/etcd/raft"
)

const (
	// storeRebalancerTimerDuration is how frequently to check the store-level
	// balance of the cluster.
	storeRebalancerTimerDuration = time.Minute

	// minQPSThresholdDifference is the minimum QPS difference from the cluster
	// mean that this system should care about. In other words, we won't worry
	// about rebalancing for QPS reasons if a store's QPS differs from the mean
	// by less than this amount even if the amount is greater than the percentage
	// threshold. This avoids too many lease transfers in lightly loaded clusters.
	minQPSThresholdDifference = 100
)

var (
	metaStoreRebalancerLeaseTransferCount = metric.Metadata{
		Name:        "rebalancing.lease.transfers",
		Help:        "Number of lease transfers motivated by store-level load imbalances",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
	metaStoreRebalancerRangeRebalanceCount = metric.Metadata{
		Name:        "rebalancing.range.rebalances",
		Help:        "Number of range rebalance operations motivated by store-level load imbalances",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
)

// StoreRebalancerMetrics is the set of metrics for the store-level rebalancer.
type StoreRebalancerMetrics struct {
	LeaseTransferCount  *metric.Counter
	RangeRebalanceCount *metric.Counter
}

func makeStoreRebalancerMetrics() StoreRebalancerMetrics {
	return StoreRebalancerMetrics{
		LeaseTransferCount:  metric.NewCounter(metaStoreRebalancerLeaseTransferCount),
		RangeRebalanceCount: metric.NewCounter(metaStoreRebalancerRangeRebalanceCount),
	}
}

// LoadBasedRebalancingMode controls whether range rebalancing takes
// additional variables such as write load and disk usage into account.
// If disabled, rebalancing is done purely based on replica count.
var LoadBasedRebalancingMode = settings.RegisterEnumSetting(
	"kv.allocator.load_based_rebalancing",
	"whether to rebalance based on the distribution of QPS across stores",
	"leases and replicas",
	map[int64]string{
		int64(LBRebalancingOff):               "off",
		int64(LBRebalancingLeasesOnly):        "leases",
		int64(LBRebalancingLeasesAndReplicas): "leases and replicas",
	},
)

// qpsRebalanceThreshold is much like rangeRebalanceThreshold, but for
// QPS rather than range count. This should be set higher than
// rangeRebalanceThreshold because QPS can naturally vary over time as
// workloads change and clients come and go, so we need to be a little more
// forgiving to avoid thrashing.
var qpsRebalanceThreshold = settings.RegisterNonNegativeFloatSetting(
	"kv.allocator.qps_rebalance_threshold",
	"minimum fraction away from the mean a store's QPS (such as queries per second) can be before it is considered overfull or underfull",
	0.25,
)

// LBRebalancingMode controls if and when we do store-level rebalancing
// based on load.
type LBRebalancingMode int64

const (
	// LBRebalancingOff means that we do not do store-level rebalancing
	// based on load statistics.
	LBRebalancingOff LBRebalancingMode = iota
	// LBRebalancingLeasesOnly means that we rebalance leases based on
	// store-level QPS imbalances.
	LBRebalancingLeasesOnly
	// LBRebalancingLeasesAndReplicas means that we rebalance both leases and
	// replicas based on store-level QPS imbalances.
	LBRebalancingLeasesAndReplicas
)

// StoreRebalancer 负责检查关联商店的负载情况
// 比较集群中其他store的负载和传输租约
// 或者在本地存储过载时复制。
//
// 这不是作为一个队列实现的，因为所有的队列都在一个队列上操作
// 每次复制一个，本地决定每个复制。队列不
// 真正知道他们所看到的副本与其他副本的比较
// 商店。我们的目标是平衡商店，所以这是更好的选择
// 决定每个商店，然后小心地选择副本移动
// 将最好地完成商店级别的目标。
type StoreRebalancer struct {
	log.AmbientContext
	metrics         StoreRebalancerMetrics
	st              *cluster.Settings
	rq              *replicateQueue
	replRankings    *replicaRankings
	getRaftStatusFn func(replica *Replica) *raft.Status
}

// NewStoreRebalancer creates a StoreRebalancer to work in tandem with the
// provided replicateQueue.
func NewStoreRebalancer(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	rq *replicateQueue,
	replRankings *replicaRankings,
) *StoreRebalancer {
	sr := &StoreRebalancer{
		AmbientContext: ambientCtx,
		metrics:        makeStoreRebalancerMetrics(),
		st:             st,
		rq:             rq,
		replRankings:   replRankings,
		getRaftStatusFn: func(replica *Replica) *raft.Status {
			return replica.RaftStatus()
		},
	}
	sr.AddLogTag("store-rebalancer", nil)
	sr.rq.store.metrics.registry.AddMetricStruct(&sr.metrics)
	return sr
}

// Start 在goroutine中运行一个无限循环，定期检查是否
// 存储在任何重要的维度上都是重载的(例如，范围计数，
// QPS，磁盘使用情况)，如果是，则尝试通过移动租约或
// 复制到其他地方。
// 这个worker处理存储级不平衡，而复制队列
// 根据区域配置约束和多样性进行决策
// 个人范围。这意味着有两个不同的工人
// 可能会对给定的范围做出决定，所以他们必须这样做
// 注意不要踩到对方的脚趾头。
//
// TODO(a-robinson): Expose metrics to make this understandable without having
// to dive into logspy.
func (sr *StoreRebalancer) Start(ctx context.Context, stopper *stop.Stopper) {
	ctx = sr.AnnotateCtx(ctx)

	// Start a goroutine that watches and proactively renews certain
	// expiration-based leases.
	stopper.RunWorker(ctx, func(ctx context.Context) {
		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(jitteredInterval(storeRebalancerTimerDuration))
		for {
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some qps/wps stats to
			// accumulate.
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				timer.Reset(jitteredInterval(storeRebalancerTimerDuration))
			}

			if !sr.st.Version.IsActive(cluster.VersionLoadBasedRebalancing) {
				continue
			}

			mode := LBRebalancingMode(LoadBasedRebalancingMode.Get(&sr.st.SV))
			if mode == LBRebalancingOff {
				continue
			}

			storeList, _, _ := sr.rq.allocator.storePool.getStoreList(roachpb.RangeID(0), storeFilterNone)
			sr.rebalanceStore(ctx, mode, storeList)
		}
	})
}

func (sr *StoreRebalancer) rebalanceStore(
	ctx context.Context, mode LBRebalancingMode, storeList StoreList,
) {
	qpsThresholdFraction := qpsRebalanceThreshold.Get(&sr.st.SV)

	// First check if we should transfer leases away to better balance QPS.
	qpsMinThreshold := math.Min(storeList.candidateQueriesPerSecond.mean*(1-qpsThresholdFraction),
		storeList.candidateQueriesPerSecond.mean-minQPSThresholdDifference)
	qpsMaxThreshold := math.Max(storeList.candidateQueriesPerSecond.mean*(1+qpsThresholdFraction),
		storeList.candidateQueriesPerSecond.mean+minQPSThresholdDifference)

	var localDesc *roachpb.StoreDescriptor
	for i := range storeList.stores {
		if storeList.stores[i].StoreID == sr.rq.store.StoreID() {
			localDesc = &storeList.stores[i]
		}
	}
	if localDesc == nil {
		log.Warningf(ctx, "StorePool missing descriptor for local store")
		return
	}

	if !(localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold) {
		log.VEventf(ctx, 1, "local QPS %.2f is below max threshold %.2f (mean=%.2f); no rebalancing needed",
			localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold, storeList.candidateQueriesPerSecond.mean)
		return
	}

	var replicasToMaybeRebalance []replicaWithStats
	storeMap := storeListToMap(storeList)

	log.Infof(ctx,
		"considering load-based lease transfers for s%d with %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, storeList.candidateQueriesPerSecond.mean, qpsMaxThreshold)

	hottestRanges := sr.replRankings.topQPS()
	for localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
		replWithStats, target, considerForRebalance := sr.chooseLeaseToTransfer(
			ctx, &hottestRanges, localDesc, storeList, storeMap, qpsMinThreshold, qpsMaxThreshold)
		replicasToMaybeRebalance = append(replicasToMaybeRebalance, considerForRebalance...)
		if replWithStats.repl == nil {
			break
		}

		log.VEventf(ctx, 1, "transferring r%d (%.2f qps) to s%d to better balance load",
			replWithStats.repl.RangeID, replWithStats.qps, target.StoreID)
		if err := contextutil.RunWithTimeout(ctx, "transfer lease", sr.rq.processTimeout, func(ctx context.Context) error {
			return sr.rq.transferLease(ctx, replWithStats.repl, target, replWithStats.qps)
		}); err != nil {
			log.Errorf(ctx, "unable to transfer lease to s%d: %v", target.StoreID, err)
			continue
		}
		sr.metrics.LeaseTransferCount.Inc(1)

		// Finally, update our local copies of the descriptors so that if
		// additional transfers are needed we'll be making the decisions with more
		// up-to-date info. The StorePool copies are updated by transferLease.
		localDesc.Capacity.LeaseCount--
		localDesc.Capacity.QueriesPerSecond -= replWithStats.qps
		if otherDesc := storeMap[target.StoreID]; otherDesc != nil {
			otherDesc.Capacity.LeaseCount++
			otherDesc.Capacity.QueriesPerSecond += replWithStats.qps
		}
	}

	if !(localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold) {
		log.Infof(ctx,
			"load-based lease transfers successfully brought s%d down to %.2f qps (mean=%.2f, upperThreshold=%.2f)",
			localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, storeList.candidateQueriesPerSecond.mean, qpsMaxThreshold)
		return
	}

	if mode != LBRebalancingLeasesAndReplicas {
		log.Infof(ctx,
			"ran out of leases worth transferring and qps (%.2f) is still above desired threshold (%.2f)",
			localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)
		return
	}
	log.Infof(ctx,
		"ran out of leases worth transferring and qps (%.2f) is still above desired threshold (%.2f); considering load-based replica rebalances",
		localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)

	// Re-combine replicasToMaybeRebalance with what remains of hottestRanges so
	// that we'll reconsider them for replica rebalancing.
	replicasToMaybeRebalance = append(replicasToMaybeRebalance, hottestRanges...)

	for localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
		replWithStats, targets := sr.chooseReplicaToRebalance(
			ctx,
			&replicasToMaybeRebalance,
			localDesc,
			storeList,
			storeMap,
			qpsMinThreshold,
			qpsMaxThreshold)
		if replWithStats.repl == nil {
			log.Infof(ctx,
				"ran out of replicas worth transferring and qps (%.2f) is still above desired threshold (%.2f); will check again soon",
				localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)
			return
		}

		descBeforeRebalance := replWithStats.repl.Desc()
		log.VEventf(ctx, 1, "rebalancing r%d (%.2f qps) from %v to %v to better balance load",
			replWithStats.repl.RangeID, replWithStats.qps, descBeforeRebalance.Replicas, targets)
		if err := contextutil.RunWithTimeout(ctx, "relocate range", sr.rq.processTimeout, func(ctx context.Context) error {
			return sr.rq.store.AdminRelocateRange(ctx, *descBeforeRebalance, targets, false)
		}); err != nil {
			log.Errorf(ctx, "unable to relocate range to %v: %v", targets, err)
			continue
		}
		sr.metrics.RangeRebalanceCount.Inc(1)

		// Finally, update our local copies of the descriptors so that if
		// additional transfers are needed we'll be making the decisions with more
		// up-to-date info.
		//
		// TODO(a-robinson): This just updates the copies used locally by the
		// storeRebalancer. We may also want to update the copies in the StorePool
		// itself.
		for i := range descBeforeRebalance.Replicas {
			if storeDesc := storeMap[descBeforeRebalance.Replicas[i].StoreID]; storeDesc != nil {
				storeDesc.Capacity.RangeCount--
			}
		}
		localDesc.Capacity.LeaseCount--
		localDesc.Capacity.QueriesPerSecond -= replWithStats.qps
		for i := range targets {
			if storeDesc := storeMap[targets[i].StoreID]; storeDesc != nil {
				storeDesc.Capacity.RangeCount++
				if i == 0 {
					storeDesc.Capacity.LeaseCount++
					storeDesc.Capacity.QueriesPerSecond += replWithStats.qps
				}
			}
		}
	}

	log.Infof(ctx,
		"load-based replica transfers successfully brought s%d down to %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, storeList.candidateQueriesPerSecond.mean, qpsMaxThreshold)
}

// TODO(a-robinson): Should we take the number of leases on each store into
// account here or just continue to let that happen in allocator.go?
func (sr *StoreRebalancer) chooseLeaseToTransfer(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	storeList StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	minQPS float64,
	maxQPS float64,
) (replicaWithStats, roachpb.ReplicaDescriptor, []replicaWithStats) {
	var considerForRebalance []replicaWithStats
	now := sr.rq.store.Clock().Now()
	for {
		if len(*hottestRanges) == 0 {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}, considerForRebalance
		}
		replWithStats := (*hottestRanges)[0]
		*hottestRanges = (*hottestRanges)[1:]

		// We're all out of replicas.
		if replWithStats.repl == nil {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}, considerForRebalance
		}

		if shouldNotMoveAway(ctx, replWithStats, localDesc, now, minQPS) {
			continue
		}

		// Don't bother moving leases whose QPS is below some small fraction of the
		// store's QPS (unless the store has extra leases to spare anyway). It's
		// just unnecessary churn with no benefit to move leases responsible for,
		// for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction &&
			float64(localDesc.Capacity.LeaseCount) <= storeList.candidateLeases.mean {
			log.VEventf(ctx, 5, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, localDesc.Capacity.QueriesPerSecond)
			continue
		}

		desc, zone := replWithStats.repl.DescAndZone()
		log.VEventf(ctx, 3, "considering lease transfer for r%d with %.2f qps",
			desc.RangeID, replWithStats.qps)
		ctx = setLocateSpace(ctx, replWithStats.repl.GetLocateSpace())

		// Check all the other replicas in order of increasing qps.
		replicas := make([]roachpb.ReplicaDescriptor, len(desc.Replicas))
		copy(replicas, desc.Replicas)
		sort.Slice(replicas, func(i, j int) bool {
			var iQPS, jQPS float64
			if desc := storeMap[replicas[i].StoreID]; desc != nil {
				iQPS = desc.Capacity.QueriesPerSecond
			}
			if desc := storeMap[replicas[j].StoreID]; desc != nil {
				jQPS = desc.Capacity.QueriesPerSecond
			}
			return iQPS < jQPS
		})

		var raftStatus *raft.Status

		for _, candidate := range replicas {
			if candidate.StoreID == localDesc.StoreID {
				continue
			}

			meanQPS := storeList.candidateQueriesPerSecond.mean
			if shouldNotMoveTo(ctx, storeMap, replWithStats, candidate.StoreID, meanQPS, minQPS, maxQPS) {
				continue
			}

			if raftStatus == nil {
				raftStatus = sr.getRaftStatusFn(replWithStats.repl)
			}
			if replicaIsBehind(raftStatus, candidate.ReplicaID) {
				log.VEventf(ctx, 3, "%v is behind or this store isn't the raft leader for r%d; raftStatus: %v",
					candidate, desc.RangeID, raftStatus)
				continue
			}

			al := allocatorLocation{ctx: ctx}
			preferred := al.preferredLeaseholders(zone, sr.rq.allocator.storePool, desc.Replicas)
			if len(preferred) > 0 && !storeHasReplica(candidate.StoreID, preferred) {
				log.VEventf(ctx, 3, "s%d not a preferred leaseholder for r%d; preferred: %v",
					candidate.StoreID, desc.RangeID, preferred)
				continue
			}

			filteredStoreList := storeList.filter(zone.Constraints)
			if sr.rq.allocator.followTheWorkloadPrefersLocal(
				ctx,
				filteredStoreList,
				*localDesc,
				candidate.StoreID,
				desc.Replicas,
				replWithStats.repl.leaseholderStats,
			) {
				log.VEventf(ctx, 3, "r%d is on s%d due to follow-the-workload; skipping",
					desc.RangeID, localDesc.StoreID)
				continue
			}

			return replWithStats, candidate, considerForRebalance
		}

		// If none of the other replicas are valid lease transfer targets, consider
		// this range for replica rebalancing.
		considerForRebalance = append(considerForRebalance, replWithStats)
	}
}

func (sr *StoreRebalancer) chooseReplicaToRebalance(
	ctx context.Context,
	hottestRanges *[]replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	storeList StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	minQPS float64,
	maxQPS float64,
) (replicaWithStats, []roachpb.ReplicationTarget) {
	now := sr.rq.store.Clock().Now()
	for {
		if len(*hottestRanges) == 0 {
			return replicaWithStats{}, nil
		}
		replWithStats := (*hottestRanges)[0]
		*hottestRanges = (*hottestRanges)[1:]

		if replWithStats.repl == nil {
			return replicaWithStats{}, nil
		}

		if shouldNotMoveAway(ctx, replWithStats, localDesc, now, minQPS) {
			continue
		}

		// Don't bother moving ranges whose QPS is below some small fraction of the
		// store's QPS (unless the store has extra ranges to spare anyway). It's
		// just unnecessary churn with no benefit to move ranges responsible for,
		// for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction &&
			float64(localDesc.Capacity.RangeCount) <= storeList.candidateRanges.mean {
			log.VEventf(ctx, 5, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, localDesc.Capacity.QueriesPerSecond)
			continue
		}

		desc, zone := replWithStats.repl.DescAndZone()
		ctx = setLocateSpace(ctx, replWithStats.repl.GetLocateSpace())
		log.VEventf(ctx, 3, "considering replica rebalance for r%d with %.2f qps",
			desc.RangeID, replWithStats.qps)

		enableClusterNodes := sr.rq.allocator.storePool.ClusterEnableNodeCount()
		desiredReplicas := GetNeededReplicas(*zone.NumReplicas, enableClusterNodes)
		targets := make([]roachpb.ReplicationTarget, 0, desiredReplicas)
		targetReplicas := make([]roachpb.ReplicaDescriptor, 0, desiredReplicas)

		// Check the range's existing diversity score, since we want to ensure we
		// don't hurt locality diversity just to improve QPS.
		curDiversity := rangeDiversityScore(sr.rq.allocator.storePool.getLocalities(desc.Replicas))

		// Check the existing replicas, keeping around those that aren't overloaded.
		for i := range desc.Replicas {
			if desc.Replicas[i].StoreID == localDesc.StoreID {
				continue
			}
			// Keep the replica in the range if we don't know its QPS or if its QPS
			// is below the upper threshold. Punishing stores not in our store map
			// could cause mass evictions if the storePool gets out of sync.
			storeDesc, ok := storeMap[desc.Replicas[i].StoreID]
			if !ok || storeDesc.Capacity.QueriesPerSecond < maxQPS {
				targets = append(targets, roachpb.ReplicationTarget{
					NodeID:  desc.Replicas[i].NodeID,
					StoreID: desc.Replicas[i].StoreID,
				})
				targetReplicas = append(targetReplicas, roachpb.ReplicaDescriptor{
					NodeID:  desc.Replicas[i].NodeID,
					StoreID: desc.Replicas[i].StoreID,
				})
			}
		}

		// Then pick out which new stores to add the remaining replicas to.
		rangeInfo := rangeInfoForRepl(replWithStats.repl, desc)
		// Make sure to use the same qps measurement throughout everything we do.
		rangeInfo.QueriesPerSecond = replWithStats.qps
		options := sr.rq.allocator.scorerOptions()
		options.qpsRebalanceThreshold = qpsRebalanceThreshold.Get(&sr.st.SV)
		for len(targets) < desiredReplicas {
			// Use the preexisting AllocateTarget logic to ensure that considerations
			// such as zone constraints, locality diversity, and full disk come
			// into play.
			target, _ := sr.rq.allocator.allocateTargetFromList(
				ctx,
				storeList,
				zone,
				targetReplicas,
				rangeInfo,
				options,
			)
			if target == nil {
				log.VEventf(ctx, 3, "no rebalance targets found to replace the current store for r%d",
					desc.RangeID)
				break
			}

			meanQPS := storeList.candidateQueriesPerSecond.mean
			if shouldNotMoveTo(ctx, storeMap, replWithStats, target.StoreID, meanQPS, minQPS, maxQPS) {
				break
			}

			targets = append(targets, roachpb.ReplicationTarget{
				NodeID:  target.Node.NodeID,
				StoreID: target.StoreID,
			})
			targetReplicas = append(targetReplicas, roachpb.ReplicaDescriptor{
				NodeID:  target.Node.NodeID,
				StoreID: target.StoreID,
			})
		}

		// If we couldn't find enough valid targets, forget about this range.
		//
		// TODO(a-robinson): Support more incremental improvements -- move what we
		// can if it makes things better even if it isn't great. For example,
		// moving one of the other existing replicas that's on a store with less
		// qps than the max threshold but above the mean would help in certain
		// locality configurations.
		if len(targets) < desiredReplicas {
			log.VEventf(ctx, 3, "couldn't find enough rebalance targets for r%d (%d/%d)",
				desc.RangeID, len(targets), desiredReplicas)
			continue
		}
		newDiversity := rangeDiversityScore(sr.rq.allocator.storePool.getLocalities(targetReplicas))
		if newDiversity < curDiversity {
			log.VEventf(ctx, 3,
				"new diversity %.2f for r%d worse than current diversity %.2f; not rebalancing",
				newDiversity, desc.RangeID, curDiversity)
			continue
		}

		// Pick the replica with the least QPS to be leaseholder;
		// RelocateRange transfers the lease to the first provided target.
		newLeaseIdx := 0
		newLeaseQPS := math.MaxFloat64
		var raftStatus *raft.Status
		for i := 0; i < len(targets); i++ {
			// Ensure we don't transfer the lease to an existing replica that is behind
			// in processing its raft log.
			var replicaID roachpb.ReplicaID
			for _, replica := range desc.Replicas {
				if replica.StoreID == targets[i].StoreID {
					replicaID = replica.ReplicaID
				}
			}
			if replicaID != 0 {
				if raftStatus == nil {
					raftStatus = sr.getRaftStatusFn(replWithStats.repl)
				}
				if replicaIsBehind(raftStatus, replicaID) {
					continue
				}
			}

			storeDesc, ok := storeMap[targets[i].StoreID]
			if ok && storeDesc.Capacity.QueriesPerSecond < newLeaseQPS {
				newLeaseIdx = i
				newLeaseQPS = storeDesc.Capacity.QueriesPerSecond
			}
		}
		targets[0], targets[newLeaseIdx] = targets[newLeaseIdx], targets[0]
		return replWithStats, targets
	}
}

func shouldNotMoveAway(
	ctx context.Context,
	replWithStats replicaWithStats,
	localDesc *roachpb.StoreDescriptor,
	now hlc.Timestamp,
	minQPS float64,
) bool {
	if !replWithStats.repl.OwnsValidLease(now) {
		log.VEventf(ctx, 3, "store doesn't own the lease for r%d", replWithStats.repl.RangeID)
		return true
	}
	if localDesc.Capacity.QueriesPerSecond-replWithStats.qps < minQPS {
		log.VEventf(ctx, 3, "moving r%d's %.2f qps would bring s%d below the min threshold (%.2f)",
			replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, minQPS)
		return true
	}
	return false
}

func shouldNotMoveTo(
	ctx context.Context,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	replWithStats replicaWithStats,
	candidateStore roachpb.StoreID,
	meanQPS float64,
	minQPS float64,
	maxQPS float64,
) bool {
	storeDesc, ok := storeMap[candidateStore]
	if !ok {
		log.VEventf(ctx, 3, "missing store descriptor for s%d", candidateStore)
		return true
	}

	newCandidateQPS := storeDesc.Capacity.QueriesPerSecond + replWithStats.qps
	if storeDesc.Capacity.QueriesPerSecond < minQPS {
		if newCandidateQPS > maxQPS {
			log.VEventf(ctx, 3,
				"r%d's %.2f qps would push s%d over the max threshold (%.2f) with %.2f qps afterwards",
				replWithStats.repl.RangeID, replWithStats.qps, candidateStore, maxQPS, newCandidateQPS)
			return true
		}
	} else if newCandidateQPS > meanQPS {
		log.VEventf(ctx, 3,
			"r%d's %.2f qps would push s%d over the mean (%.2f) with %.2f qps afterwards",
			replWithStats.repl.RangeID, replWithStats.qps, candidateStore, meanQPS, newCandidateQPS)
		return true
	}

	return false
}

func storeListToMap(sl StoreList) map[roachpb.StoreID]*roachpb.StoreDescriptor {
	storeMap := make(map[roachpb.StoreID]*roachpb.StoreDescriptor)
	for i := range sl.stores {
		storeMap[sl.stores[i].StoreID] = &sl.stores[i]
	}
	return storeMap
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from checkInterval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*rand.Float64()))
}
