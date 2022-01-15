// Copyright 2019  The Cockroach Authors.

package execpb

import (
	"fmt"
	"time"

	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

var _ tracing.SpanStats = &VectorizedStats{}
var _ distsqlpb.DistSQLSpanStats = &VectorizedStats{}

const (
	batchesOutputTagSuffix = "output.batches"
	tuplesOutputTagSuffix  = "output.tuples"
	selectivityTagSuffix   = "selectivity"
	stallTimeTagSuffix     = "time.stall"
	executionTimeTagSuffix = "time.execution"
)

// Stats is part of SpanStats interface.
func (vs *VectorizedStats) Stats() map[string]string {
	var timeSuffix string
	if vs.Stall {
		timeSuffix = stallTimeTagSuffix
	} else {
		timeSuffix = executionTimeTagSuffix
	}
	selectivity := float64(0)
	if vs.NumBatches > 0 {
		selectivity = float64(vs.NumTuples) / float64(int64(coldata.BatchSize())*vs.NumBatches)
	}
	return map[string]string{
		batchesOutputTagSuffix: fmt.Sprintf("%d", vs.NumBatches),
		tuplesOutputTagSuffix:  fmt.Sprintf("%d", vs.NumTuples),
		selectivityTagSuffix:   fmt.Sprintf("%.2f", selectivity),
		timeSuffix:             fmt.Sprintf("%v", vs.Time.Round(time.Microsecond)),
	}
}

const (
	batchesOutputQueryPlanSuffix = "batches output"
	tuplesOutputQueryPlanSuffix  = "tuples output"
	selectivityQueryPlanSuffix   = "selectivity"
	stallTimeQueryPlanSuffix     = "stall time"
	executionTimeQueryPlanSuffix = "execution time"
)

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (vs *VectorizedStats) StatsForQueryPlan() []string {
	var timeSuffix string
	if vs.Stall {
		timeSuffix = stallTimeQueryPlanSuffix
	} else {
		timeSuffix = executionTimeQueryPlanSuffix
	}
	selectivity := float64(0)
	if vs.NumBatches > 0 {
		selectivity = float64(vs.NumTuples) / float64(int64(coldata.BatchSize())*vs.NumBatches)
	}
	return []string{
		fmt.Sprintf("%s: %d", batchesOutputQueryPlanSuffix, vs.NumBatches),
		fmt.Sprintf("%s: %d", tuplesOutputQueryPlanSuffix, vs.NumTuples),
		fmt.Sprintf("%s: %.2f", selectivityQueryPlanSuffix, selectivity),
		fmt.Sprintf("%s: %v", timeSuffix, vs.Time.Round(time.Microsecond)),
	}
}
