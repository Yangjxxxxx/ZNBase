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

import React from "react";
import _ from "lodash";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, storeSources, nodeSources } = props;

  return [
    <LineGraph title="Ranges" sources={storeSources}>
      <Axis label="range数目">
        <Metric name="cr.store.ranges" title="Ranges" />
        <Metric name="cr.store.replicas.leaders" title="Leaders" />
        <Metric name="cr.store.replicas.leaseholders" title="Lease Holders" />
        <Metric name="cr.store.replicas.leaders_not_leaseholders" title="Leaders w/o Lease" />
        <Metric name="cr.store.ranges.unavailable" title="Unavailable" />
        <Metric name="cr.store.ranges.underreplicated" title="Under-replicated" />
        <Metric name="cr.store.ranges.overreplicated" title="Over-replicated" />
      </Axis>
    </LineGraph>,

    <LineGraph title="每个Store的副本" tooltip={`每个Store上的副本数目。`}>
      <Axis label="副本数">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.replicas"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="每个Strore的租赁副本"
      tooltip={
          `每个Store的租赁者副本数量。 租赁者副本是接收和协调其Range上所有读取和写入请求的副本。`
      }
    >
      <Axis label="租赁者(leaseholders)">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.replicas.leaseholders"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="每个Store的平均访问次数" tooltip={`每个Store上的租赁者副本每秒处理的KV批量请求的数目的指数加权平均值。 记录大约最后30分钟的请求，用来协助基于负载的再平衡决策。
    (Exponentially weighted moving average of the number of KV batch requests processed by leaseholder replicas on each store per second. Tracks roughly the last 30 minutes of requests. Used for load-based rebalancing decisions.)`}>
      <Axis label="访问数">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.rebalancing.queriespersecond"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="每个Store的逻辑字节数" tooltip={`每个Store的数据逻辑字节数。`}>
      <Axis units={AxisUnits.Bytes} label="逻辑存储大小">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.totalbytes"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="静止副本" sources={storeSources}>
      <Axis label="replicas">
        <Metric name="cr.store.replicas" title="Replicas" />
        <Metric name="cr.store.replicas.quiescent" title="Quiescent" />
      </Axis>
    </LineGraph>,

    <LineGraph title="Range操作" sources={storeSources}>
      <Axis label="ranges">
        <Metric name="cr.store.range.splits" title="分裂（Splits）" nonNegativeRate />
        <Metric name="cr.store.range.merges" title="合并（Merges）" nonNegativeRate />
        <Metric name="cr.store.range.adds" title="增加（Add）" nonNegativeRate />
        <Metric name="cr.store.range.removes" title="移除（Remove）" nonNegativeRate />
        <Metric name="cr.store.leases.transfers.success" title="租赁转让（Lease Transfers）" nonNegativeRate />
        <Metric name="cr.store.rebalancing.lease.transfers" title="Load-based Lease Transfers" nonNegativeRate />
        <Metric name="cr.store.rebalancing.range.rebalances" title="Load-based Range Rebalances" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="RangeId Queue长度" sources={storeSources}>
      <Axis label="ranges">
        <Metric name="cr.store.tick.range.id.queue.length" title="Tick Queue长度"  />
        <Metric name="cr.store.ready.request.range.id.queue.length" title="Ready/Request Queue长度"  />
      </Axis>
    </LineGraph>,

    <LineGraph title="快照" sources={storeSources}>
      <Axis label="snapshots">
        <Metric name="cr.store.range.snapshots.generated" title="Generated" nonNegativeRate />
        <Metric name="cr.store.range.snapshots.normal-applied" title="Applied (Raft-initiated)" nonNegativeRate />
        <Metric name="cr.store.range.snapshots.learner-applied" title="Applied (Learner)" nonNegativeRate />
        <Metric name="cr.store.range.snapshots.preemptive-applied" title="Applied (Preemptive)" nonNegativeRate />
        <Metric name="cr.store.replicas.reserved" title="Reserved" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="Follower Read执行计数" sources={nodeSources}>
      <Axis label="数目">
        {
          _.map(nodeIDs, (nid) => (
              <Metric
                  key={nid}
                  name="cr.node.follower_reads.count"
                  title={nodeDisplayName(nodesSummary, nid)}
                  sources={storeIDsForNode(nodesSummary, nid)}
              />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="Follower Read成功计数" sources={storeSources}>
      <Axis label="数目">
        {
          _.map(nodeIDs, (nid) => (
              <Metric
                  key={nid}
                  name="cr.store.follower_reads.success_count"
                  title={nodeDisplayName(nodesSummary, nid)}
                  sources={storeIDsForNode(nodesSummary, nid)}
              />
          ))
        }
      </Axis>
    </LineGraph>,
  ];
}
