  
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
  const { nodeIDs, nodesSummary, nodeSources, tooltipSelection } = props;

  return [
    <LineGraph title="活跃节点数目" tooltip="集群中活跃节点的数目">
      <Axis label="节点数">
        <Metric name="cr.node.liveness.livenodes" title="活跃节点数" aggregateMax />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="内存使用"
      sources={nodeSources}
      tooltip={(
        <div>
          {`${tooltipSelection}中内存的使用情况 `}
          <dl>
            <dt>RSS</dt>
            <dd>inspur使用的总内存</dd>
            <dt>Go占用</dt>
            <dd>Go层分配的内存</dd>
            <dt>Go总计</dt>
            <dd>Go层管理的总内存</dd>
            <dt>C占用</dt>
            <dd>C层分配的内存</dd>
            <dt>C总计</dt>
            <dd>C层管理的总内存</dd>
          </dl>
        </div>
      )}
    >
      <Axis units={AxisUnits.Bytes} label="内存使用">
        <Metric name="cr.node.sys.rss" title="RSS" />
        <Metric name="cr.node.sys.go.allocbytes" title="Go占用的" />
        <Metric name="cr.node.sys.go.totalbytes" title="Go总计" />
        <Metric name="cr.node.sys.cgo.allocbytes" title="CGo占用的" />
        <Metric name="cr.node.sys.cgo.totalbytes" title="CGo总计" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Goroutine数目"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}中Goroutine的数目。
        此数目根据负载的变化而增加或减少。`
      }
    >
      <Axis label="Ｇoroutine">
        <Metric name="cr.node.sys.goroutines" title="Goroutine数目" />
      </Axis>
    </LineGraph>,

    // TODO(mrtracy): The following two graphs are a good first example of a graph with
    // two axes; the two series should be highly correlated, but have different units.
    <LineGraph
      title="GC每秒钟执行次数"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}每秒调用Go的垃圾收集器的次数。`
      }
    >
      <Axis label="运行次数">
        <Metric name="cr.node.sys.gc.count" title="GC 运行次数" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="GC 阻塞时间"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}调用Go垃圾收集器期间程序被阻塞，该数值表示程序被阻塞的时间。`
      }
    >
      <Axis units={AxisUnits.Duration} label="阻塞时间">
        <Metric name="cr.node.sys.gc.pause.ns" title="GC 阻塞时间" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="CPU 时间"
      sources={nodeSources}
      tooltip={
        ` ${tooltipSelection}中用户级和系统级程序使用的CPU时间`
      }
    >
      <Axis units={AxisUnits.Duration} label="CPU 时间">
        <Metric name="cr.node.sys.cpu.user.ns" title="用户级CPU时间" nonNegativeRate />
        <Metric name="cr.node.sys.cpu.sys.ns" title="系统级CPU时间" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="时钟偏移量"
      sources={nodeSources}
      tooltip={`每个节点与集群中其他节点的平均时钟偏移量。`}
    >
      <Axis label="偏移量" units={AxisUnits.Duration}>
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.node.clock-offset.meannanos"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,
  ];
}
