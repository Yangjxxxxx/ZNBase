  
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
  const { nodeIDs, nodesSummary, nodeSources, storeSources, tooltipSelection } = props;

  return [
    <LineGraph
      title="存储容量"
      sources={storeSources}
      tooltip={`${tooltipSelection}中总容量和可用容量的总结。`}
    >
      <Axis units={AxisUnits.Bytes} label="存储容量">
        <Metric name="cr.store.capacity" title="总量" />
        <Metric name="cr.store.capacity.available" title="可用" />
        <Metric name="cr.store.capacity.used" title="已用" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="热数据"
      sources={storeSources}
      tooltip={
        `${tooltipSelection}中应用程序和inspur系统使用的实时数据量。
        这不包括历史数据和已删除数据。`
      }
    >
      <Axis units={AxisUnits.Bytes} label="字节数目">
        <Metric name="cr.store.livebytes" title="热数据" />
        <Metric name="cr.store.sysbytes" title="系统" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft日志提交延迟: P99"
      sources={storeSources}
      tooltip={`99%的Raft日志可以在此时间内完成提交。`}
    >
      <Axis units={AxisUnits.Duration} label="延迟">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.raft.process.logcommit.latency-p99"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Raft命令提交延迟: P99"
      sources={storeSources}
      tooltip={`99%的Raft命令可以在此时间内完成提交。`}
    >
      <Axis units={AxisUnits.Duration} label="延迟">
        {
          _.map(nodeIDs, (nid) => (
            <Metric
              key={nid}
              name="cr.store.raft.process.commandcommit.latency-p99"
              title={nodeDisplayName(nodesSummary, nid)}
              sources={storeIDsForNode(nodesSummary, nid)}
            />
          ))
        }
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RocksDB读放大"
      sources={storeSources}
      tooltip={
        `RocksDB读放大统计; 用来衡量${tooltipSelection}中每个逻辑读操作的实际读操作的平均值。`
      }
    >
      <Axis label="比值">
        <Metric name="cr.store.rocksdb.read-amplification" title="读放大" aggregateAvg />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RocksDB SSTables"
      sources={storeSources}
      tooltip={`${tooltipSelection}中在用的RocksDB SSTable的数目。`}
    >
      <Axis label="sstable数目">
        <Metric name="cr.store.rocksdb.num-sstables" title="SSTable数目" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="文件描述符"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}中开放的文件描述符的数目与文件描述符上限的比对。`
      }
    >
      <Axis label="文件描述符">
        <Metric name="cr.node.sys.fd.open" title="Open" />
        <Metric name="cr.node.sys.fd.softlimit" title="Limit" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="RocksDB压缩/写入硬盘的数目"
      sources={storeSources}
      tooltip={
        `${tooltipSelection}中每秒钟RocksDB压缩和写入硬盘的数目 。`
      }
    >
      <Axis label="数目">
        <Metric name="cr.store.rocksdb.compactions" title="压缩" nonNegativeRate />
        <Metric name="cr.store.rocksdb.flushes" title="写入硬盘" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Time Series 写"
      sources={nodeSources}
      tooltip={
        `${tooltipSelection}中每秒写入time series样本成功的数量和错误的数目。`
      }
    >
      <Axis label="数目">
        <Metric name="cr.node.timeseries.write.samples" title="已写样本数目" nonNegativeRate />
        <Metric name="cr.node.timeseries.write.errors" title="错误数数目" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="已写的Time Series占用的大小"
      sources={nodeSources}
      tooltip={
        <div>
          {tooltipSelection}中每秒time series写入的字节数。
          <br />
          请注意由于数据在磁盘上高度压缩，所以这个值反映的并不是time series写占用磁盘的速率。
           此速率表示的是time series写产生的网络流量和硬盘活动量。
          <br />
          请参阅“数据库”选项卡以查找时间序列数据的当前硬盘使用情况。
        </div>
      }
    >
      <Axis units={AxisUnits.Bytes}>
        <Metric name="cr.node.timeseries.write.bytes" title="已写字节数" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
