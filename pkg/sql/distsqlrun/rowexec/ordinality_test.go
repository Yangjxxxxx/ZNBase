// Copyright 2019  The Cockroach Authors.

package rowexec

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

func TestOrdinality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(sqlbase.IntType, tree.NewDInt(tree.DInt(i)))
	}

	testCases := []struct {
		spec     distsqlpb.OrdinalitySpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			input: sqlbase.EncDatumRows{
				{v[2]},
				{v[5]},
				{v[2]},
				{v[5]},
				{v[2]},
				{v[3]},
				{v[2]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[1]},
				{v[5], v[2]},
				{v[2], v[3]},
				{v[5], v[4]},
				{v[2], v[5]},
				{v[3], v[6]},
				{v[2], v[7]},
			},
		},
		{
			input: sqlbase.EncDatumRows{
				{},
				{},
				{},
				{},
				{},
				{},
				{},
			},
			expected: sqlbase.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
				{v[4]},
				{v[5]},
				{v[6]},
				{v[7]},
			},
		},
		{
			input: sqlbase.EncDatumRows{
				{v[2], v[1]},
				{v[5], v[2]},
				{v[2], v[3]},
				{v[5], v[4]},
				{v[2], v[5]},
				{v[3], v[6]},
				{v[2], v[7]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[1], v[1]},
				{v[5], v[2], v[2]},
				{v[2], v[3], v[3]},
				{v[5], v[4], v[4]},
				{v[2], v[5], v[5]},
				{v[3], v[6], v[6]},
				{v[2], v[7], v[7]},
			},
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			os := c.spec

			in := runbase.NewRowBuffer(sqlbase.TwoIntCols, c.input, runbase.RowBufferArgs{})
			out := &runbase.RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := runbase.FlowCtx{
				Cfg:     &runbase.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}

			d, err := newOrdinalityProcessor(&flowCtx, 0 /* processorID */, &os, in, &distsqlpb.PostProcessSpec{}, out)
			if err != nil {
				t.Fatal(err)
			}

			d.Run(context.Background())
			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t).Copy()
				if row == nil {
					break
				}
				res = append(res, row)
			}

			var typs []sqlbase.ColumnType
			switch len(res[0]) {
			case 1:
				typs = sqlbase.OneIntCol
			case 2:
				typs = sqlbase.TwoIntCols
			case 3:
				typs = sqlbase.ThreeIntCols
			}
			if result := res.String(typs); result != c.expected.String(typs) {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected.String(sqlbase.TwoIntCols))
			}
		})
	}
}

func BenchmarkOrdinality(b *testing.B) {
	const numCols = 2

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &runbase.FlowCtx{
		Cfg:     &runbase.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	spec := &distsqlpb.OrdinalitySpec{}

	post := &distsqlpb.PostProcessSpec{}
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		input := runbase.NewRepeatableRowSource(sqlbase.ColumnTypesToDatumTypes(sqlbase.TwoIntCols), sqlbase.MakeIntRows(numRows, numCols))
		b.SetBytes(int64(8 * numRows * numCols))
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				o, err := newOrdinalityProcessor(flowCtx, 0 /* processorID */, spec, input, post, &runbase.RowDisposer{})
				if err != nil {
					b.Fatal(err)
				}
				o.Run(ctx)
				input.Reset()
			}
		})
	}
}
