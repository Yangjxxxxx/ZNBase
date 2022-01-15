// Copyright 2018 The Cockroach Authors.
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

package rowexec

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// InputStatCollector wraps a RowSource and collects stats from it.
type InputStatCollector struct {
	runbase.RowSource
	InputStats
}

var _ runbase.RowSource = &InputStatCollector{}

// NewInputStatCollector creates a new InputStatCollector that wraps the given
// input.
func NewInputStatCollector(input runbase.RowSource) *InputStatCollector {
	return &InputStatCollector{RowSource: input}
}

// Next implements the RowSource interface. It calls Next on the embedded
// RowSource and collects stats.
func (isc *InputStatCollector) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	start := timeutil.Now()
	row, meta := isc.RowSource.Next()
	if row != nil {
		isc.NumRows++
	}
	isc.StallTime += timeutil.Since(start)
	return row, meta
}

//Count is fix count(*) to use a special function
func (isc *InputStatCollector) Count() (int, *distsqlpb.ProducerMetadata) {
	t, ret := isc.RowSource.(*tableReader).Count()
	isc.StallTime += time.Microsecond
	isc.NumRows = int64(t)
	return t, ret
}

const (
	rowsReadTagSuffix  = "input.rows"
	stallTimeTagSuffix = "stalltime"
	//MaxMemoryTagSuffix is the tag suffix for the bytes mem.max.
	MaxMemoryTagSuffix = "mem.max"
	//MaxDiskTagSuffix is the tag suffix for the bytes disk.max.
	MaxDiskTagSuffix = "disk.max"
	// bytesReadTagSuffix is the tag suffix for the bytes read stat. unused
	//bytesReadTagSuffix = "bytes.read"
)

// Stats is a utility method that returns a map of the InputStats` stats to
// output to a trace as tags. The given prefix is prefixed to the keys.
func (is InputStats) Stats(prefix string) map[string]string {
	return map[string]string{
		prefix + rowsReadTagSuffix:  fmt.Sprintf("%d", is.NumRows),
		prefix + stallTimeTagSuffix: fmt.Sprintf("%v", is.RoundStallTime()),
	}
}

const (
	rowsReadQueryPlanSuffix  = "rows read"
	stallTimeQueryPlanSuffix = "stall time"
	//MaxMemoryQueryPlanSuffix is the tag suffix for the bytes max memory used.
	MaxMemoryQueryPlanSuffix = "max memory used"
	//MaxDiskQueryPlanSuffix is the tag suffix for the bytes max disk used.
	MaxDiskQueryPlanSuffix = "max disk used"
)

// StatsForQueryPlan is a utility method that returns a list of the InputStats'
// stats to output on a query plan. The given prefix is prefixed to each element
// in the returned list.
func (is InputStats) StatsForQueryPlan(prefix string) []string {
	return []string{
		fmt.Sprintf("%s%s: %d", prefix, rowsReadQueryPlanSuffix, is.NumRows),
		fmt.Sprintf("%s%s: %v", prefix, stallTimeQueryPlanSuffix, is.RoundStallTime()),
	}
}

// RoundStallTime returns the InputStats' StallTime rounded to the nearest
// time.Millisecond.
func (is InputStats) RoundStallTime() time.Duration {
	return is.StallTime.Round(time.Microsecond)
}

// rowFetcherStatCollector is a wrapper on top of a row.Fetcher that collects stats.
//
// Only row.Fetcher methods that collect stats are overridden.
type rowFetcherStatCollector struct {
	*row.Fetcher
	// stats contains the collected stats.
	stats              InputStats
	startScanStallTime time.Duration
}

var _ rowFetcher = &rowFetcherStatCollector{}

// newRowFetcherStatCollector returns a new rowFetcherStatCollector.
func newRowFetcherStatCollector(f *row.Fetcher) *rowFetcherStatCollector {
	return &rowFetcherStatCollector{Fetcher: f}
}

// NextRow is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) NextRow(
	ctx context.Context,
) (sqlbase.EncDatumRow, *sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, error) {
	start := timeutil.Now()
	row, t, i, err := c.Fetcher.NextRow(ctx)
	if row != nil {
		c.stats.NumRows++
	}
	c.stats.StallTime += timeutil.Since(start)
	return row, t, i, err
}

// StartScan is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) StartScan(
	ctx context.Context,
	txn *client.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	start := timeutil.Now()
	err := c.Fetcher.StartScan(ctx, txn, spans, limitBatches, limitHint, traceKV)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

// StartInconsistentScan is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) StartInconsistentScan(
	ctx context.Context,
	db *client.DB,
	initialTimestamp hlc.Timestamp,
	maxTimestampAge time.Duration,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	start := timeutil.Now()
	err := c.Fetcher.StartInconsistentScan(
		ctx, db, initialTimestamp, maxTimestampAge, spans, limitBatches, limitHint, traceKV,
	)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

// getInputStats is a utility function to check whether the given input is
// collecting stats, returning true and the stats if so. If false is returned,
// the input is not collecting stats.
func getInputStats(flowCtx *runbase.FlowCtx, input runbase.RowSource) (InputStats, bool) {
	isc, ok := input.(*InputStatCollector)
	if !ok {
		return InputStats{}, false
	}
	return getStatsInner(flowCtx, isc.InputStats), true
}

func getStatsInner(flowCtx *runbase.FlowCtx, stats InputStats) InputStats {
	if flowCtx.Cfg.TestingKnobs.DeterministicStats {
		stats.StallTime = 0
	}
	return stats
}

//unused
// getFetcherInputStats is a utility function to check whether the given input
// is collecting row fetcher stats, returning true and the stats if so. If
// false is returned, the input is not collecting row fetcher stats.
//func getFetcherInputStats(flowCtx *runbase.FlowCtx, f rowFetcher) (InputStats, bool) {
//	rfsc, ok := f.(*rowFetcherStatCollector)
//	if !ok {
//		return InputStats{}, false
//	}
//	// Add row fetcher start scan stall time to Next() stall time.
//	if !flowCtx.Cfg.TestingKnobs.DeterministicStats {
//		rfsc.stats.StallTime += rfsc.startScanStallTime
//	}
//	return getStatsInner(flowCtx, rfsc.stats), true
//}

const outboxTagPrefix = "Outbox."

// Stats implements the SpanStats interface.
func (os *OutboxStats) Stats() map[string]string {
	statsMap := make(map[string]string)
	statsMap[outboxTagPrefix+"bytes_sent"] = humanizeutil.IBytes(os.BytesSent)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (os *OutboxStats) StatsForQueryPlan() []string {
	return []string{fmt.Sprintf("bytes sent: %d", os.BytesSent)}
}

const routerOutputTagPrefix = "routeroutput."

// Stats implements the SpanStats interface.
func (ros *RouterOutputStats) Stats() map[string]string {
	statsMap := make(map[string]string)
	statsMap[routerOutputTagPrefix+"rows_routed"] = strconv.FormatInt(ros.NumRows, 10)
	statsMap[routerOutputTagPrefix+MaxMemoryTagSuffix] = strconv.FormatInt(ros.MaxAllocatedMem, 10)
	statsMap[routerOutputTagPrefix+MaxDiskTagSuffix] = strconv.FormatInt(ros.MaxAllocatedDisk, 10)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ros *RouterOutputStats) StatsForQueryPlan() []string {
	return []string{
		fmt.Sprintf("rows routed: %d", ros.NumRows),
		fmt.Sprintf("%s: %d", MaxMemoryQueryPlanSuffix, ros.MaxAllocatedMem),
		fmt.Sprintf("%s: %d", MaxDiskQueryPlanSuffix, ros.MaxAllocatedDisk),
	}
}

// getFetcherInputStats is a utility function to check whether the given input
// is collecting row fetcher stats, returning true and the stats if so. If
// false is returned, the input is not collecting row fetcher stats.
func getFetcherInputStats(flowCtx *runbase.FlowCtx, f rowFetcher) (InputStats, bool) {
	rfsc, ok := f.(*rowFetcherStatCollector)
	if !ok {
		return InputStats{}, false
	}
	// Add row fetcher start scan stall time to Next() stall time.
	if !flowCtx.Cfg.TestingKnobs.DeterministicStats {
		rfsc.stats.StallTime += rfsc.startScanStallTime
	}
	return getStatsInner(flowCtx, rfsc.stats), true
}
