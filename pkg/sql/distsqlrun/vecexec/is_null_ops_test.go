// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestIsNullProjOp(t *testing.T) {
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

	testCases := []struct {
		desc         string
		inputTuples  tuples
		outputTuples tuples
		negate       bool
	}{
		{
			desc:         "SELECT c, c IS NULL FROM t -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0, false}, {nil, true}, {1, false}, {2, false}, {nil, true}},
			negate:       false,
		},
		{
			desc:         "SELECT c, c IS NULL FROM t -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0, false}, {1, false}, {2, false}},
			negate:       false,
		},
		{
			desc:         "SELECT c, c IS NULL FROM t -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil, true}, {nil, true}},
			negate:       false,
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0, true}, {nil, false}, {1, true}, {2, true}, {nil, false}},
			negate:       true,
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0, true}, {1, true}, {2, true}},
			negate:       true,
		},
		{
			desc:         "SELECT c, c IS NOT NULL FROM t -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil, false}, {nil, false}},
			negate:       true,
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			opConstructor := func(input []Operator) (Operator, error) {
				projExpr := "IS NULL"
				if c.negate {
					projExpr = "IS NOT NULL"
				}
				spec := &distsqlpb.ProcessorSpec{
					Input: []distsqlpb.InputSyncSpec{{ColumnTypes: []sqlbase.ColumnType{sqlbase.IntType}}},
					Core: distsqlpb.ProcessorCoreUnion{
						Noop: &distsqlpb.NoopCoreSpec{},
					},
					Post: distsqlpb.PostProcessSpec{
						RenderExprs: []distsqlpb.Expression{
							{Expr: "@1"},
							{Expr: fmt.Sprintf("@1 %s", projExpr)},
						},
					},
				}
				args := NewColOperatorArgs{
					Spec:                               spec,
					Inputs:                             input,
					StreamingMemAccount:                testMemAcc,
					UseStreamingMemAccountForBuffering: true,
				}
				result, err := NewColOperator(ctx, flowCtx, args)
				if err != nil {
					return nil, err
				}
				return result.Op, nil
			}
			runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier, opConstructor)
		})
	}
}

func TestIsNullSelOp(t *testing.T) {
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

	testCases := []struct {
		desc         string
		inputTuples  tuples
		outputTuples tuples
		negate       bool
	}{
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{nil}, {nil}},
			negate:       false,
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{},
			negate:       false,
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NULL -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{{nil}, {nil}},
			negate:       false,
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- both",
			inputTuples:  tuples{{0}, {nil}, {1}, {2}, {nil}},
			outputTuples: tuples{{0}, {1}, {2}},
			negate:       true,
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- no NULLs",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0}, {1}, {2}},
			negate:       true,
		},
		{
			desc:         "SELECT c FROM t WHERE c IS NOT NULL -- only NULLs",
			inputTuples:  tuples{{nil}, {nil}},
			outputTuples: tuples{},
			negate:       true,
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			opConstructor := func(input []Operator) (Operator, error) {
				selExpr := "IS NULL"
				if c.negate {
					selExpr = "IS NOT NULL"
				}
				spec := &distsqlpb.ProcessorSpec{
					Input: []distsqlpb.InputSyncSpec{{ColumnTypes: []sqlbase.ColumnType{sqlbase.IntType}}},
					Core: distsqlpb.ProcessorCoreUnion{
						Noop: &distsqlpb.NoopCoreSpec{},
					},
					Post: distsqlpb.PostProcessSpec{
						Filter: distsqlpb.Expression{Expr: fmt.Sprintf("@1 %s", selExpr)},
					},
				}
				args := NewColOperatorArgs{
					Spec:                               spec,
					Inputs:                             input,
					StreamingMemAccount:                testMemAcc,
					UseStreamingMemAccountForBuffering: true,
				}
				result, err := NewColOperator(ctx, flowCtx, args)
				if err != nil {
					return nil, err
				}
				return result.Op, nil
			}
			runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier, opConstructor)
		})
	}
}
