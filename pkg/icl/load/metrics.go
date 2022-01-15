// Copyright 2017 The Cockroach Authors.
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

package load

import (
	"time"

	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// Metrics contains pointers to the metrics for
// monitoring DistSQL processing.
type Metrics struct {
	LoadReadRecordTotal      *metric.Gauge
	LoadConvertedRecordTotal *metric.Gauge
	ConvertGoroutines        *metric.Gauge
	ReadWaitTime             *metric.Histogram
	ConversionWaitTime       *metric.Histogram
	KvWriteChannelWaitTime   *metric.Histogram
	KvWriteSSTWaitTime       *metric.Histogram
	AddSSTableTime           *metric.Histogram
	LoadSpeed                *metric.Histogram
}

// MetricStruct implements the metrics.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

var (
	metaLoadReadRecordTotal = metric.Metadata{
		Name:        "load.read.record.total",
		Help:        "Import the total number of read records",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaLoadConvertedRecordTotal = metric.Metadata{
		Name:        "load.convert.record.total",
		Help:        "Import the number of converted KV records",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaReadWaitTime = metric.Metadata{
		Name:        "load.read.wait.time",
		Help:        "Read goroutine wait time",
		Measurement: "Flows",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaConvertGoroutines = metric.Metadata{
		Name:        "load.convert.goroutines.total",
		Help:        "Number of concurrent conversion coroutines",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaConversionWaitTime = metric.Metadata{
		Name:        "load.conversion.wait.time",
		Help:        "Conversion wait time",
		Measurement: "Flows",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaKvWriteChannelWaitTime = metric.Metadata{
		Name:        "load.kv.write.channel.wait.time",
		Help:        "kv write channel wait time",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaKvWriteSSTWaitTime = metric.Metadata{
		Name:        "load.kv.write.sst.wait.time",
		Help:        "kv write sst wait time",
		Measurement: "Memory",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaAddSSTableTime = metric.Metadata{
		Name:        "load.add.sstable.time",
		Help:        "Add SSTable time",
		Measurement: "Memory",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaLoadSpeed = metric.Metadata{
		Name:        "load.speed",
		Help:        "load speed",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 10000000000

// MakeLoadMetrics instantiates the metrics holder for DistSQL monitoring.
func MakeLoadMetrics(histogramWindow time.Duration) metric.Struct {
	return &Metrics{
		LoadReadRecordTotal:      metric.NewGauge(metaLoadReadRecordTotal),
		LoadConvertedRecordTotal: metric.NewGauge(metaLoadConvertedRecordTotal),
		//ReadWaitTime:   metric.NewHistogram(metaReadWaitTime, histogramWindow, math.MaxInt64, 3),
		ReadWaitTime:           metric.NewHistogram(metaReadWaitTime, histogramWindow, log10int64times1000, 3),
		ConversionWaitTime:     metric.NewHistogram(metaConversionWaitTime, histogramWindow, log10int64times1000, 3),
		KvWriteChannelWaitTime: metric.NewHistogram(metaKvWriteChannelWaitTime, histogramWindow, log10int64times1000, 3),
		KvWriteSSTWaitTime:     metric.NewHistogram(metaKvWriteSSTWaitTime, histogramWindow, log10int64times1000, 3),
		AddSSTableTime:         metric.NewHistogram(metaAddSSTableTime, histogramWindow, log10int64times1000, 3),
		ConvertGoroutines:      metric.NewGauge(metaConvertGoroutines),
		LoadSpeed:              metric.NewHistogram(metaLoadSpeed, histogramWindow, log10int64times1000, 3),
	}
}

// LoadStart 导入结束后对相应的Metrics进行清空操作
func (m *Metrics) LoadStart() {
	m.LoadReadRecordTotal.Inc(0)
	m.LoadConvertedRecordTotal.Inc(0)
	m.ReadWaitTime.RecordValue(0)
	m.LoadSpeed.RecordValue(0)
}

// LoadStop 导入结束后对相应的Metrics进行清空操作
func (m *Metrics) LoadStop() {
	m.LoadReadRecordTotal.Update(0)
	m.LoadConvertedRecordTotal.Update(0)
	m.ConvertGoroutines.Update(0)
	m.ReadWaitTime.RecordValue(0)
	m.AddSSTableTime.RecordValue(0)
	m.ConversionWaitTime.RecordValue(0)
	m.KvWriteChannelWaitTime.RecordValue(0)
	m.KvWriteSSTWaitTime.RecordValue(0)
	m.LoadSpeed.RecordValue(0)
}

func init() {
	jobs.MakeLoadMetricsHook = MakeLoadMetrics
}
