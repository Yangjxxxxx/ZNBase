  
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

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

// TODO(vilterp): tooltips

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, storeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="CPU 使用率"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Percentage} label="CPU">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.cpu.combined.percent-normalized"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="内存使用量"
      sources={nodeSources}
      tooltip={(
        <div>
          {tooltipSelection}中的内存使用量
        </div>
      )}
    >
      <Axis units={AxisUnits.Bytes} label="用量">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.rss"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="硬盘读取"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Bytes} label="字节数">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.read.bytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="硬盘写入"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Bytes} label="字节数">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.write.bytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="硬盘每秒读取次数"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Count} label="数目">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.read.count"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="硬盘每秒写入次数"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Count} label="数目">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.write.count"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="硬盘每秒IO次数"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Count} label="IOPS">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.disk.iopsinprogress"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="可用的硬盘容量"
      sources={storeSources}
    >
      <Axis units={AxisUnits.Bytes} label="容量">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.store.capacity.available"
            sources={storeIDsForNode(nodesSummary, nid)}
            title={nodeDisplayName(nodesSummary, nid)}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="网络接收数据"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Bytes} label="字节数">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.net.recv.bytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="网络发送数据"
      sources={nodeSources}
    >
      <Axis units={AxisUnits.Bytes} label="字节数">
        {nodeIDs.map((nid) => (
          <Metric
            name="cr.node.sys.host.net.send.bytes"
            title={nodeDisplayName(nodesSummary, nid)}
            sources={[nid]}
            nonNegativeRate
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
