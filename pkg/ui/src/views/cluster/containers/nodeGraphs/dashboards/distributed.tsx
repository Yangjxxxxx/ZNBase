  
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

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources } = props;

  return [
    <LineGraph title="Batche数目" sources={nodeSources}>
      <Axis label="Bathe数目">
        <Metric name="cr.node.distsender.batches" title="Bathes" nonNegativeRate />
        <Metric name="cr.node.distsender.batches.partial" title="partial Batches" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="RPC数目" sources={nodeSources}>
      <Axis label="rpc数目">
        <Metric name="cr.node.distsender.rpc.sent" title="RPCs Sent" nonNegativeRate />
        <Metric name="cr.node.distsender.rpc.sent.local" title="本地快速途径" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="RPC 错误" sources={nodeSources}>
      <Axis label="错误数目">
        <Metric name="cr.node.distsender.rpc.sent.sendnexttimeout" title="RPC 超时" nonNegativeRate />
        <Metric name="cr.node.distsender.rpc.sent.nextreplicaerror" title="备份错误 " nonNegativeRate />
        <Metric name="cr.node.distsender.errors.notleaseholder" title="非租赁者错误" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="KV事务" sources={nodeSources}>
      <Axis label="事务数目">
        <Metric name="cr.node.txn.commits" title="提交" nonNegativeRate />
        <Metric name="cr.node.txn.commits1PC" title="快速提交" nonNegativeRate />
        <Metric name="cr.node.txn.aborts" title="终止" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="KV事务重试" sources={nodeSources}>
      <Axis label="重试次数">
        <Metric name="cr.node.txn.restarts.writetooold" title="Write Too Old" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.writetoooldmulti" title="Write Too Old (multiple)" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.serializable" title="Forwarded Timestamp (iso=serializable)" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.possiblereplay" title="Possible Replay" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.asyncwritefailure" title="Async Consensus Failure" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.readwithinuncertainty" title="Read Within Uncertainty Interval" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.txnaborted" title="Aborted" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.txnpush" title="Push Failure" nonNegativeRate />
        <Metric name="cr.node.txn.restarts.unknown" title="Unknown" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="KV事务持续时间：P99"
      tooltip={`99%的事务可以在一分钟的时间段内完成，每个节点单独显示值。`}>
      <Axis units={AxisUnits.Duration} label="交易持续时间">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.txn.durations-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="KV事务持续时间：P90"
      tooltip={`90%的事务可以在一分钟的时间段内完成，每个节点单独显示值。`}>
      <Axis units={AxisUnits.Duration} label="事务持续时间">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.txn.durations-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="节点心跳延迟：P99"
      tooltip={`每分钟内99%的心跳节点的内部活跃记录延迟都在这个范围内，
                              每个节点单独显示值。`}>
      <Axis units={AxisUnits.Duration} label="心跳延迟">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.liveness.heartbeatlatency-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph title="节点心跳延迟：P90"
      tooltip={`每分钟内90%的心跳节点的内部活跃记录延迟都在这个范围内，
                                     每个节点单独显示值。`}>
      <Axis units={AxisUnits.Duration} label="心跳延迟">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.liveness.heartbeatlatency-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

  ];
}
