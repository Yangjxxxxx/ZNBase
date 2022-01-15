// Copyright 2019  The Cockroach Authors.
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
    <LineGraph title="读取记录数目" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.node.load.read.record.total" title="Counts" aggregateMax />
      </Axis>
    </LineGraph>,
    <LineGraph title="转换协程并发数" sources={storeSources}>
      <Axis units={AxisUnits.Count} label="数目">
        <Metric name="cr.node.load.convert.goroutines.total" title="Counts" aggregateMax />
      </Axis>
    </LineGraph>,
    <LineGraph title="读协程等待时间" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="时间">
        <Metric name="cr.node.load.read.wait.time-p99" title="readWait" nonNegativeRate />
      </Axis>
    </LineGraph>,
    <LineGraph title="转换协程等待时间" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="时间">
        <Metric name="cr.node.load.conversion.wait.time-p99" title="convertWait" nonNegativeRate />
      </Axis>
    </LineGraph>,
    <LineGraph title="转换kv写入KvCh等待时间" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="时间">
        <Metric name="cr.node.load.kv.write.channel.wait.time-p99" title="kvWriteCh" nonNegativeRate />
      </Axis>
    </LineGraph>,
    <LineGraph title="KvCh flush的等待时间" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="时间">
        <Metric name="cr.node.load.kv.write.sst.wait.time-p99" title="kvChFlush" nonNegativeRate />
      </Axis>
    </LineGraph>,
    <LineGraph title="addSSTable消耗的时间" sources={storeSources}>
      <Axis units={AxisUnits.Duration} label="时间">
        <Metric name="cr.node.load.add.sstable.time-p99" title="addsstable" nonNegativeRate />
      </Axis>
    </LineGraph>,
    <LineGraph title="数据导入速度" sources={storeSources}>
      <Axis units={AxisUnits.Bytes} label="字节">
        <Metric name="cr.node.load.speed-p99" title="loadSpeed" nonNegativeRate />
      </Axis>
    </LineGraph>,
  ];
}
