// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	"math"
	"time"

	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

var (
	metaCDCEmittedMessages = metric.Metadata{
		Name:        "changefeed.emitted_messages",
		Help:        "Messages emitted by all feeds",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaCDCEmittedBytes = metric.Metadata{
		Name:        "changefeed.emitted_bytes",
		Help:        "Bytes emitted by all feeds",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaCDCFlushes = metric.Metadata{
		Name:        "changefeed.flushes",
		Help:        "Total flushes across all feeds",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaCDCSinkErrorRetries = metric.Metadata{
		Name:        "changefeed.sink_error_retries",
		Help:        "Total retryable errors encountered while emitting to sinks",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaCDCBufferEntriesIn = metric.Metadata{
		Name:        "changefeed.buffer_entries.in",
		Help:        "Total entries entering the buffer between raft and cdc sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaCDCBufferEntriesOut = metric.Metadata{
		Name:        "changefeed.buffer_entries.out",
		Help:        "Total entries leaving the buffer between raft and cdc sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}

	metaCDCPollRequestNanos = metric.Metadata{
		Name:        "changefeed.poll_request_nanos",
		Help:        "Time spent fetching changes",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCDCProcessingNanos = metric.Metadata{
		Name:        "changefeed.processing_nanos",
		Help:        "Time spent processing KV changes into SQL rows",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCDCTableMetadataNanos = metric.Metadata{
		Name:        "changefeed.table_metadata_nanos",
		Help:        "Time blocked while verifying table metadata histories",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCDCEmitNanos = metric.Metadata{
		Name:        "changefeed.emit_nanos",
		Help:        "Total time spent emitting all feeds",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCDCFlushNanos = metric.Metadata{
		Name:        "changefeed.flush_nanos",
		Help:        "Total time spent flushing all feeds",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// TODO(dan): This was intended to be a measure of the minimum distance of
	// any CDC ahead of its gc ttl threshold, but keeping that correct in
	// the face of changing zone configs is much harder, so this will have to do
	// for now.
	metaCDCMaxBehindNanos = metric.Metadata{
		Name:        "changefeed.max_behind_nanos",
		Help:        "Largest commit-to-emit duration of any running feed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// Deprecated.
	metaCDCMinHighWater = metric.Metadata{
		Name:        "changefeed.min_high_water",
		Help:        "Latest high-water timestamp of most behind feed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_TIMESTAMP_NS,
	}
)

const noMinHighWaterSentinel = int64(math.MaxInt64)

const pollRequestNanosHistMaxLatency = time.Hour

// Metrics are for production monitoring of cdc.
type Metrics struct {
	EmittedMessages  *metric.Counter
	EmittedBytes     *metric.Counter
	Flushes          *metric.Counter
	SinkErrorRetries *metric.Counter
	BufferEntriesIn  *metric.Counter
	BufferEntriesOut *metric.Counter

	PollRequestNanosHist *metric.Histogram
	ProcessingNanos      *metric.Counter
	TableMetadataNanos   *metric.Counter
	EmitNanos            *metric.Counter
	FlushNanos           *metric.Counter

	mu struct {
		syncutil.Mutex
		id       int
		resolved map[int]hlc.Timestamp
	}
	MaxBehindNanos *metric.Gauge
	MinHighWater   *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for cdc monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	m := &Metrics{
		EmittedMessages:  metric.NewCounter(metaCDCEmittedMessages),
		EmittedBytes:     metric.NewCounter(metaCDCEmittedBytes),
		Flushes:          metric.NewCounter(metaCDCFlushes),
		SinkErrorRetries: metric.NewCounter(metaCDCSinkErrorRetries),
		BufferEntriesIn:  metric.NewCounter(metaCDCBufferEntriesIn),
		BufferEntriesOut: metric.NewCounter(metaCDCBufferEntriesOut),

		// Metrics for cdc performance debugging: - PollRequestNanos and
		// PollRequestNanosHist, things are first
		//   fetched with some limited concurrency. We're interested in both the
		//   total amount of time fetching as well as outliers, so we need both
		//   the counter and the histogram.
		// - N/A. Each change is put into a buffer. Right now nothing measures
		//   this since the buffer doesn't actually buffer and so it just tracks
		//   the poll sleep time.
		// - ProcessingNanos. Everything from the buffer until the SQL row is
		//   about to be emitted. This includes TableMetadataNanos, which is
		//   dependent on network calls, so also tracked in case it's ever the
		//   cause of a ProcessingNanos blowup.
		// - EmitNanos and FlushNanos. All of our interactions with the sink.
		PollRequestNanosHist: metric.NewHistogram(
			metaCDCPollRequestNanos, histogramWindow,
			pollRequestNanosHistMaxLatency.Nanoseconds(), 1),
		ProcessingNanos:    metric.NewCounter(metaCDCProcessingNanos),
		TableMetadataNanos: metric.NewCounter(metaCDCTableMetadataNanos),
		EmitNanos:          metric.NewCounter(metaCDCEmitNanos),
		FlushNanos:         metric.NewCounter(metaCDCFlushNanos),
	}
	m.mu.resolved = make(map[int]hlc.Timestamp)

	m.MaxBehindNanos = metric.NewFunctionalGauge(metaCDCMaxBehindNanos, func() int64 {
		now := timeutil.Now()
		var maxBehind time.Duration
		m.mu.Lock()
		for _, resolved := range m.mu.resolved {
			if behind := now.Sub(resolved.GoTime()); behind > maxBehind {
				maxBehind = behind
			}
		}
		m.mu.Unlock()
		return maxBehind.Nanoseconds()
	})
	m.MinHighWater = metric.NewFunctionalGauge(metaCDCMinHighWater, func() int64 {
		minHighWater := noMinHighWaterSentinel
		m.mu.Lock()
		for _, resolved := range m.mu.resolved {
			if minHighWater == noMinHighWaterSentinel || resolved.WallTime < minHighWater {
				minHighWater = resolved.WallTime
			}
		}
		m.mu.Unlock()
		return minHighWater
	})
	return m
}

func init() {
	jobs.MakeCDCMetricsHook = MakeMetrics
}
