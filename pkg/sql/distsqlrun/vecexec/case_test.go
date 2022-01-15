// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"context"
	"testing"

	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestCaseOp(t *testing.T) {
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
	spec := &distsqlpb.ProcessorSpec{
		Input: []distsqlpb.InputSyncSpec{{}},
		Core: distsqlpb.ProcessorCoreUnion{
			Noop: &distsqlpb.NoopCoreSpec{},
		},
		Post: distsqlpb.PostProcessSpec{
			RenderExprs: []distsqlpb.Expression{{}},
		},
	}

	decs := make([]apd.Decimal, 2)
	decs[0].SetInt64(0)
	decs[1].SetInt64(1)
	zero := decs[0]
	one := decs[1]

	for _, tc := range []struct {
		tuples     tuples
		renderExpr string
		expected   tuples
		inputTypes []types1.T
	}{
		{
			// Basic test.
			tuples:     tuples{{1}, {2}, {nil}, {3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 1 ELSE 0 END",
			expected:   tuples{{0}, {1}, {0}, {0}},
			inputTypes: []types1.T{*types1.Int},
		},
		{
			// Test "reordered when's."
			tuples:     tuples{{1, 1}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 + @2 > 3 THEN 0 WHEN @1 = 2 THEN 1 ELSE 2 END",
			expected:   tuples{{2}, {1}, {2}, {0}},
			inputTypes: []types1.T{*types1.Int, *types1.Int},
		},
		{
			// Test the short-circuiting behavior.
			tuples:     tuples{{1, 2}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 0::DECIMAL WHEN @1 / @2 = 1 THEN 1::DECIMAL END",
			expected:   tuples{{nil}, {zero}, {nil}, {one}},
			inputTypes: []types1.T{*types1.Int, *types1.Int},
		},
	} {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(inputs []Operator) (Operator, error) {
			cts, err := sqlbase.DatumType1sToColumnTypes(tc.inputTypes)
			if err != nil {
				return nil, nil
			}
			spec.Input[0].ColumnTypes = cts
			spec.Post.RenderExprs[0].Expr = tc.renderExpr
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
