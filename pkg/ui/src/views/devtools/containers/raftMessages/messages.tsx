  
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

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="Raft App"
      sources={nodeSources}
      // tooltip={`The number of raft app messages ${tooltipSelection}`}
      tooltip={` ${tooltipSelection}上的Raft App 消息数`}
    >
      <Axis label="消息数">
        <Metric name="cr.store.raft.rcvd.app" title="App" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.appresp" title="AppResp" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft 心跳"
      sources={nodeSources}
      // tooltip={`The number of raft heartbeat messages ${tooltipSelection}`}
      tooltip={` ${tooltipSelection}上的Raft 心跳消息数`}
    >
      <Axis label="心跳">
        <Metric name="cr.store.raft.rcvd.heartbeat" title="Heartbeat" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.heartbeatresp" title="HeartbeatResp" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft 其他"
      sources={nodeSources}
      tooltip={`${tooltipSelection}上的其他Raft消息数`}
    >
      <Axis label="消息数">
        <Metric name="cr.store.raft.rcvd.prop" title="Prop" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.vote" title="Vote" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.voteresp" title="VoteResp" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.snap" title="Snap" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.transferleader" title="TransferLeader" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.timeoutnow" title="TimeoutNow" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.prevote" title="PreVote" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.prevoteresp" title="PreVoteResp" nonNegativeRate />
        <Metric name="cr.store.raft.rcvd.dropped" title="Dropped" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft 时间"
      sources={nodeSources}
      tooltip={`${tooltipSelection}在store.processRaft中花费的时间`}
    >
      <Axis units={AxisUnits.Duration}>
        <Metric name="cr.store.raft.process.workingnanos" title="Working" nonNegativeRate />
        <Metric name="cr.store.raft.process.tickingnanos" title="Ticking" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft Ticks"
      sources={nodeSources}
      tooltip={`排在${tooltipSelection}上的Raft ticks数`}
    >
      <Axis label="ticks">
        <Metric name="cr.store.raft.ticks" title="Ticks" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="挂起的心跳"
      sources={nodeSources}
      tooltip={`${tooltipSelection}上的等待合并的Raft心跳和响应数`}
    >
      <Axis label="心跳">
        <Metric name="cr.store.raft.heartbeats.pending" title="Pending" />
      </Axis>
    </LineGraph>,

  ];
}
