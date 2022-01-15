  
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

import * as docsURL from "src/util/docs";
import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, storeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="SQL"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}每十秒启动的SELECT，INSERT，UPDATE和DELETE语句的平均值`
      }
    >
      <Axis label="数目">
        <Metric name="cr.node.sql.select.count" title="查询" nonNegativeRate />
        <Metric name="cr.node.sql.update.count" title="更新" nonNegativeRate />
        <Metric name="cr.node.sql.insert.count" title="插入" nonNegativeRate />
        <Metric name="cr.node.sql.delete.count" title="删除" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="服务延迟：SQL P99"
      tooltip={(
        <div>
          99％的语句可以在最近的一分钟内执行完成。&nbsp;
            <em>此时间不包括节点和客户端之间的网络延迟。</em>
        </div>
      )}
    >
      <Axis units={AxisUnits.Duration} label="延迟">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.sql.service.latency-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="每个节点的副本数目"
      tooltip={(
        <div>
          存储在此节点上的Range副本数。
          {" "}
          <em>Range是数据的子集，可以复制这些子集来保证其生存能力。</em>
        </div>
      )}
    >
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
      title="存储容量"
      sources={storeSources}
      tooltip={(
        <div>
          <dl>
            <dt>存储容量</dt>
            <dd>
            {tooltipSelection} 中inspur可用的磁盘空间。
              {" "}
              <em>
                每个节点可以通过
                {" "}
                <code>
                  <a href={docsURL.startFlags} target="_blank">
                    --store
                  </a>
                </code>
                {" "}
                标志来控制这个值。
              </em>
            </dd>
            <dt>可用的</dt>
            <dd>{tooltipSelection} 中inspur可用的磁盘空间 。</dd>
            <dt>已用的</dt>
            <dd>{tooltipSelection} 中inspur已使用的磁盘空间 。</dd>
          </dl>
        </div>
      )}
    >
      <Axis units={AxisUnits.Bytes} label="容量">
        <Metric name="cr.store.capacity" title="总容量" />
        <Metric name="cr.store.capacity.available" title="可用的" />
        <Metric name="cr.store.capacity.used" title="已用的" />
      </Axis>
    </LineGraph>,

  ];
}
