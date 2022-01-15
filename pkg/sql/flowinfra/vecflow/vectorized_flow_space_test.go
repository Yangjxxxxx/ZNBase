// Copyright 2019  The Cockroach Authors.

package vecflow_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/mon"
)

func TestVectorizeInternalMemorySpaceError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &runbase.FlowCtx{
		Cfg:     &runbase.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	oneInput := []distsqlpb.InputSyncSpec{
		{ColumnTypes: []sqlbase.ColumnType{sqlbase.IntType}},
	}
	twoInputs := []distsqlpb.InputSyncSpec{
		{ColumnTypes: []sqlbase.ColumnType{sqlbase.IntType}},
		{ColumnTypes: []sqlbase.ColumnType{sqlbase.IntType}},
	}

	testCases := []struct {
		desc string
		spec *distsqlpb.ProcessorSpec
	}{
		{
			desc: "CASE",
			spec: &distsqlpb.ProcessorSpec{
				Input: oneInput,
				Core: distsqlpb.ProcessorCoreUnion{
					Noop: &distsqlpb.NoopCoreSpec{},
				},
				Post: distsqlpb.PostProcessSpec{
					RenderExprs: []distsqlpb.Expression{{Expr: "CASE WHEN @1 = 1 THEN 1 ELSE 2 END"}},
				},
			},
		},
		{
			desc: "MERGE JOIN",
			spec: &distsqlpb.ProcessorSpec{
				Input: twoInputs,
				Core: distsqlpb.ProcessorCoreUnion{
					MergeJoiner: &distsqlpb.MergeJoinerSpec{},
				},
			},
		},
	}

	for _, tc := range testCases {
		for _, success := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, success), func(t *testing.T) {
				inputs := []vecexec.Operator{vecexec.NewZeroOp(nil)}
				if len(tc.spec.Input) > 1 {
					inputs = append(inputs, vecexec.NewZeroOp(nil))
				}
				memMon := mon.MakeMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				if success {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
				}
				defer memMon.Stop(ctx)
				acc := memMon.MakeBoundAccount()
				defer acc.Close(ctx)
				args := vecexec.NewColOperatorArgs{
					Spec:                               tc.spec,
					Inputs:                             inputs,
					StreamingMemAccount:                &acc,
					UseStreamingMemAccountForBuffering: true,
				}
				result, err := vecexec.NewColOperator(ctx, flowCtx, args)
				if err != nil {
					t.Fatal(err)
				}
				err = acc.Grow(ctx, int64(result.InternalMemUsage))
				if success {
					require.NoError(t, err, "expected success, found: ", err)
				} else {
					require.Error(t, err, "expected memory error, found nothing")
				}
			})
		}
	}
}

func TestVectorizeAllocatorSpaceError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &runbase.FlowCtx{
		Cfg:     &runbase.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	oneInput := []distsqlpb.InputSyncSpec{
		{ColumnTypes: []sqlbase.ColumnType{sqlbase.IntType}},
	}
	twoInputs := []distsqlpb.InputSyncSpec{
		{ColumnTypes: []sqlbase.ColumnType{sqlbase.IntType}},
		{ColumnTypes: []sqlbase.ColumnType{sqlbase.IntType}},
	}

	testCases := []struct {
		desc string
		spec *distsqlpb.ProcessorSpec
	}{
		{
			desc: "SORTER",
			spec: &distsqlpb.ProcessorSpec{
				Input: oneInput,
				Core: distsqlpb.ProcessorCoreUnion{
					Sorter: &distsqlpb.SorterSpec{
						OutputOrdering: distsqlpb.Ordering{
							Columns: []distsqlpb.Ordering_Column{
								{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC},
							},
						},
					},
				},
			},
		},
		{
			desc: "HASH AGGREGATOR",
			spec: &distsqlpb.ProcessorSpec{
				Input: oneInput,
				Core: distsqlpb.ProcessorCoreUnion{
					Aggregator: &distsqlpb.AggregatorSpec{
						Type: distsqlpb.AggregatorSpec_SCALAR,
						Aggregations: []distsqlpb.AggregatorSpec_Aggregation{
							{
								Func:   distsqlpb.AggregatorSpec_MAX,
								ColIdx: []uint32{0},
							},
						},
					},
				},
			},
		},
		{
			desc: "HASH JOINER",
			spec: &distsqlpb.ProcessorSpec{
				Input: twoInputs,
				Core: distsqlpb.ProcessorCoreUnion{
					HashJoiner: &distsqlpb.HashJoinerSpec{
						LeftEqColumns:  []uint32{0},
						RightEqColumns: []uint32{0},
					},
				},
			},
		},
	}

	batch := testAllocator.NewMemBatchWithSize(
		[]coltypes.T{coltypes.Int64}, 1, /* size */
	)
	for _, tc := range testCases {
		for _, success := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, success), func(t *testing.T) {
				inputs := []vecexec.Operator{vecexec.NewRepeatableBatchSource(batch)}
				if len(tc.spec.Input) > 1 {
					inputs = append(inputs, vecexec.NewRepeatableBatchSource(batch))
				}
				memMon := mon.MakeMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				if success {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
				}
				defer memMon.Stop(ctx)
				acc := memMon.MakeBoundAccount()
				defer acc.Close(ctx)
				args := vecexec.NewColOperatorArgs{
					Spec:                               tc.spec,
					Inputs:                             inputs,
					StreamingMemAccount:                &acc,
					UseStreamingMemAccountForBuffering: true,
				}
				result, err := vecexec.NewColOperator(ctx, flowCtx, args)
				require.NoError(t, err)
				err = execerror.CatchVecRuntimeError(func() {
					result.Op.Init()
					result.Op.Next(ctx)
				})
				if success {
					require.NoError(t, err, "expected success, found: ", err)
				} else {
					require.Error(t, err, "expected memory error, found nothing")
				}
			})
		}
	}
}
