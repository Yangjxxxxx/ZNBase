  
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

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources } = props;

  return [
    <LineGraph title="操作处理失败" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="失败数目">
        <Metric name="cr.store.queue.gc.process.failure" title="GC" nonNegativeRate />
        <Metric name="cr.store.queue.replicagc.process.failure" title=" GC 副本" nonNegativeRate />
        <Metric name="cr.store.queue.replicate.process.failure" title="副本" nonNegativeRate />
        <Metric name="cr.store.queue.split.process.failure" title="分割" nonNegativeRate />
        <Metric name="cr.store.queue.consistency.process.failure" title="一致性" nonNegativeRate />
        <Metric name="cr.store.queue.raftlog.process.failure" title="Raft 日志" nonNegativeRate />
        <Metric name="cr.store.queue.raftsnapshot.process.failure" title="Raft 快照" nonNegativeRate />
        <Metric name="cr.store.queue.tsmaintenance.process.failure" title="时间序列维护" nonNegativeRate />
        <Metric name="cr.store.compactor.compactions.failure" title="压缩" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="队列处理时间" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="处理时间">
        <Metric name="cr.store.queue.gc.processingnanos" title="GC" nonNegativeRate />
        <Metric name="cr.store.queue.replicagc.processingnanos" title="GC 副本" nonNegativeRate />
        <Metric name="cr.store.queue.replicate.processingnanos" title="副本" nonNegativeRate />
        <Metric name="cr.store.queue.split.processingnanos" title="分裂" nonNegativeRate />
        <Metric name="cr.store.queue.consistency.processingnanos" title="一致性" nonNegativeRate />
        <Metric name="cr.store.queue.raftlog.processingnanos" title="Raft 日志" nonNegativeRate />
        <Metric name="cr.store.queue.raftsnapshot.processingnanos" title="Raft 快照" nonNegativeRate />
        <Metric name="cr.store.queue.tsmaintenance.processingnanos" title="time series 维护" nonNegativeRate />
        <Metric name="cr.store.compactor.compactingnanos" title="压缩" nonNegativeRate />
      </Axis>
    </LineGraph>,

    // TODO(mrtracy): The queues below should also have "processing
    // nanos" on the graph, but that has a time unit instead of a count
    // unit, and thus we need support for multi-axis graphs.
    <LineGraph title="GC 副本队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.replicagc.process.success" title="成功的操作数目 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.replicagc.pending" title="挂起的操作数目" downsampleMax />
        <Metric name="cr.store.queue.replicagc.removereplica" title="副本移除 / 秒" nonNegativeRate />
      </Axis>
    </LineGraph>,

    <LineGraph title="副本队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.replicate.process.success" title="成功操作数 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.replicate.pending" title="挂起的操作数" />
        <Metric name="cr.store.queue.replicate.addreplica" title="副本增加 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.replicate.removereplica" title="副本移除 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.replicate.removedeadreplica" title="失效副本移除 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.replicate.rebalancereplica" title="副本再平衡 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.replicate.transferlease" title="Leases Transferred / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.replicate.purgatory" title="有问题的副本(Replicas in Purgatory)" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="分裂队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.split.process.success" title="成功操作数 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.split.pending" title="挂起操作数" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="合并队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.merge.process.success" title="成功 操作数 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.merge.pending" title="挂起操作数" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="GC 队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.gc.process.success" title="成功操作数 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.gc.pending" title="挂起操作数" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="Raft 日志队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.raftlog.process.success" title="成功操作数 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.raftlog.pending" title="挂起操作数" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="Raft 快照队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.raftsnapshot.process.success" title="成功操作数 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.raftsnapshot.pending" title="挂起操作数" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="一致性检查器队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.consistency.process.success" title="成功操作数 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.consistency.pending" title="挂起操作数" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="Time Series 维护队列" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.store.queue.tsmaintenance.process.success" title="成功操作数 / 秒" nonNegativeRate />
        <Metric name="cr.store.queue.tsmaintenance.pending" title="挂起操作数" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="压缩队列"
      sources={storeSources}
      tooltip={`通过强制RocksDB压缩回收（或可能）回收的已完成（或估计）的存储字节。`}
    >
      <Axis units={AxisUnits.Bytes} label="字节">
        <Metric name="cr.store.compactor.suggestionbytes.compacted" title="字节压缩 / 秒" nonNegativeRate />
        <Metric name="cr.store.compactor.suggestionbytes.queued" title="序列化的字节数(Queued bytes)" downsampleMax />
      </Axis>
    </LineGraph>,
  ];
}
