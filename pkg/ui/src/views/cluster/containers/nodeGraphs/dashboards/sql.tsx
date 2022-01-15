  
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
  const { nodeIDs, nodesSummary, nodeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="SQL 连接"
      sources={nodeSources}
      tooltip={` ${tooltipSelection}中活跃的SQL连接数目。`}
    >
      <Axis label="连接数">
        <Metric name="cr.node.sql.conns" title="客户端连接数" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL字节流量"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}中SQL客户端网络流量的总量，以每秒字节数为单位 。`
      }
    >
      <Axis units={AxisUnits.Bytes} label="字节流量">
        <Metric name="cr.node.sql.bytesin" title="输入字节" nonNegativeRate />
        <Metric name="cr.node.sql.bytesout" title="输出字节" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL操作"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}中每十秒启动的查询，插入，更新和删除语句的平均值。`
      }
    >
      <Axis label="操作数目">
        <Metric name="cr.node.sql.select.count" title="查询" nonNegativeRate />
        <Metric name="cr.node.sql.update.count" title="更新" nonNegativeRate />
        <Metric name="cr.node.sql.insert.count" title="插入" nonNegativeRate />
        <Metric name="cr.node.sql.delete.count" title="删除" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="SQL 访问错误"
      sources={nodeSources}
      tooltip={"返回计划或运行时错误的语句数。"}
    >
      <Axis label="数目">
        <Metric name="cr.node.sql.failure.count" title="错误数目" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="活跃的分布式SQL操作"
      sources={nodeSources}
      tooltip={`${tooltipSelection}中运行的分布式SQL操作的总数目 。`}
    >
      <Axis label="操作数目">
        <Metric name="cr.node.sql.distsql.queries.active" title="活跃的操作数目" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="活跃的分布式SQL操作的执行流"
      tooltip="协助执行当前分布式SQL操作的各个节点上流的数量。"
    >
      <Axis label="流的数目">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.sql.distsql.flows.active"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="服务延迟: SQL, P99"
      tooltip={(
        <div>
          此节点上99％的操作可以在最近的一分钟内完成。
          {" "}
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
      title="服务延迟: SQL, P90"
      tooltip={(
        <div>
          此节点上90％的操作可以在最近的一分钟内完成。
          {" "}
          <em>此时间不包括节点和客户端之间的网络延迟。</em>
        </div>
      )}
    >
      <Axis units={AxisUnits.Duration} label="延迟">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.sql.service.latency-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="执行延迟: P99"
      tooltip={
        `此节点上99％的操作可以在最近的一分钟内完成。每个节点单独显示值。`
      }
    >
      <Axis units={AxisUnits.Duration} label="延迟">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.exec.latency-p99"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="执行延迟：P90"
      tooltip={
        `此节点上90％的操作可以在最近的一分钟内完成。每个节点单独显示值。`
      }
    >
      <Axis units={AxisUnits.Duration} label="延迟">
        {
          _.map(nodeIDs, (node) => (
            <Metric
              key={node}
              name="cr.node.exec.latency-p90"
              title={nodeDisplayName(nodesSummary, node)}
              sources={[node]}
              downsampleMax
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="事务数目"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}中每秒打开，提交，回滚或中止的事务总数。`
      }
    >
      <Axis label="事务数目">
        <Metric name="cr.node.sql.txn.begin.count" title="开始" nonNegativeRate />
        <Metric name="cr.node.sql.txn.commit.count" title="提交" nonNegativeRate />
        <Metric name="cr.node.sql.txn.rollback.count" title="回滚" nonNegativeRate />
        <Metric name="cr.node.sql.txn.abort.count" title="中止" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="CDC"
      sources={nodeSources}
      tooltip={`${tooltipSelection}中每秒DDL语句的总数。`}
    >
      <Axis label="语句数目">
        <Metric name="cr.node.sql.ddl.count" title="DDL语句数目" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
