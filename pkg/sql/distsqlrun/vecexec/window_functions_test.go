// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"context"
	"testing"

	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

type windowFnTestCase struct {
	tuples       []tuple
	expected     []tuple
	windowerSpec distsqlpb.WindowerSpec
}

func TestRank(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &runbase.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &runbase.ServerConfig{
			Settings: st,
		},
	}

	rankFn := distsqlpb.WindowerSpec_RANK
	denseRankFn := distsqlpb.WindowerSpec_DENSE_RANK
	for _, tc := range []windowFnTestCase{
		// With PARTITION BY, no ORDER BY.
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
			windowerSpec: distsqlpb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []distsqlpb.WindowerSpec_WindowFn{
					{
						Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &rankFn},
						OutputColIdx: 1,
					},
				},
			},
		},
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
			windowerSpec: distsqlpb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []distsqlpb.WindowerSpec_WindowFn{
					{
						Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &denseRankFn},
						OutputColIdx: 1,
					},
				},
			},
		},
		// No PARTITION BY, with ORDER BY.
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 1}, {1, 3}, {1, 3}, {2, 5}, {3, 6}, {3, 6}},
			windowerSpec: distsqlpb.WindowerSpec{
				WindowFns: []distsqlpb.WindowerSpec_WindowFn{
					{
						Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &rankFn},
						Ordering:     distsqlpb.Ordering{Columns: []distsqlpb.Ordering_Column{{ColIdx: 0}}},
						OutputColIdx: 1,
					},
				},
			},
		},
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 1}, {1, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 4}},
			windowerSpec: distsqlpb.WindowerSpec{
				WindowFns: []distsqlpb.WindowerSpec_WindowFn{
					{
						Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &denseRankFn},
						Ordering:     distsqlpb.Ordering{Columns: []distsqlpb.Ordering_Column{{ColIdx: 0}}},
						OutputColIdx: 1,
					},
				},
			},
		},
		// With both PARTITION BY and ORDER BY.
		{
			tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
			expected: tuples{{nil, nil, 1}, {nil, nil, 1}, {nil, 1, 3}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
			windowerSpec: distsqlpb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []distsqlpb.WindowerSpec_WindowFn{
					{
						Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &rankFn},
						Ordering:     distsqlpb.Ordering{Columns: []distsqlpb.Ordering_Column{{ColIdx: 1}}},
						OutputColIdx: 2,
					},
				},
			},
		},
		{
			tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
			expected: tuples{{nil, nil, 1}, {nil, nil, 1}, {nil, 1, 2}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
			windowerSpec: distsqlpb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []distsqlpb.WindowerSpec_WindowFn{
					{
						Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &denseRankFn},
						Ordering:     distsqlpb.Ordering{Columns: []distsqlpb.Ordering_Column{{ColIdx: 1}}},
						OutputColIdx: 2,
					},
				},
			},
		},
	} {
		runTests(t, []tuples{tc.tuples}, tc.expected, unorderedVerifier, func(inputs []Operator) (Operator, error) {
			ct := make([]types.T, len(tc.tuples[0]))
			for i := range ct {
				ct[i] = types.Int
			}
			cts, _ := sqlbase.DatumTypesToColumnTypes(ct)
			spec := &distsqlpb.ProcessorSpec{
				Input: []distsqlpb.InputSyncSpec{{ColumnTypes: cts}},
				Core: distsqlpb.ProcessorCoreUnion{
					Windower: &tc.windowerSpec,
				},
			}
			args := NewColOperatorArgs{
				Spec:                               spec,
				Inputs:                             inputs,
				StreamingMemAccount:                testMemAcc,
				UseStreamingMemAccountForBuffering: true,
			}
			result, err := NewColOperator(ctx, flowCtx, args)
			if err != nil {
				return nil, err
			}
			return result.Op, nil
		})
	}
}

func TestRowNumber(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &runbase.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &runbase.ServerConfig{
			Settings: st,
		},
	}

	rowNumberFn := distsqlpb.WindowerSpec_ROW_NUMBER
	for _, tc := range []windowFnTestCase{
		// Without ORDER BY, the output of row_number is non-deterministic, so we
		// skip such a case.
		//
		// No PARTITION BY, with ORDER BY.
		{
			tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
			expected: tuples{{nil, 1}, {nil, 2}, {1, 3}, {1, 4}, {2, 5}, {3, 6}, {3, 7}},
			windowerSpec: distsqlpb.WindowerSpec{
				WindowFns: []distsqlpb.WindowerSpec_WindowFn{
					{
						Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &rowNumberFn},
						Ordering:     distsqlpb.Ordering{Columns: []distsqlpb.Ordering_Column{{ColIdx: 0}}},
						OutputColIdx: 1,
					},
				},
			},
		},
		// With both PARTITION BY and ORDER BY.
		{
			tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
			expected: tuples{{nil, nil, 1}, {nil, nil, 2}, {nil, 1, 3}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
			windowerSpec: distsqlpb.WindowerSpec{
				PartitionBy: []uint32{0},
				WindowFns: []distsqlpb.WindowerSpec_WindowFn{
					{
						Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &rowNumberFn},
						Ordering:     distsqlpb.Ordering{Columns: []distsqlpb.Ordering_Column{{ColIdx: 1}}},
						OutputColIdx: 2,
					},
				},
			},
		},
	} {
		runTests(t, []tuples{tc.tuples}, tc.expected, unorderedVerifier, func(inputs []Operator) (Operator, error) {
			ct := make([]types.T, len(tc.tuples[0]))
			for i := range ct {
				ct[i] = types.Int
			}
			cts, _ := sqlbase.DatumTypesToColumnTypes(ct)
			spec := &distsqlpb.ProcessorSpec{
				Input: []distsqlpb.InputSyncSpec{{ColumnTypes: cts}},
				Core: distsqlpb.ProcessorCoreUnion{
					Windower: &tc.windowerSpec,
				},
			}
			args := NewColOperatorArgs{
				Spec:                               spec,
				Inputs:                             inputs,
				StreamingMemAccount:                testMemAcc,
				UseStreamingMemAccountForBuffering: true,
			}
			result, err := NewColOperator(ctx, flowCtx, args)
			if err != nil {
				return nil, err
			}
			return result.Op, nil
		})
	}
}
