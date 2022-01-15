// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/col/colserde"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/coltypes/conv"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
	"github.com/znbasedb/znbase/pkg/sql/sem"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/randutil"
)

// TestSupportedSQLTypesIntegration tests that all SQL types supported by the
// vectorized engine are "actually supported." For each type, it creates a bunch
// of rows consisting of a single datum (possibly null), converts them into
// column batches, serializes and then deserializes these batches, and finally
// converts the deserialized batches back to rows which are compared with the
// original rows.
func TestSupportedSQLTypesIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := runbase.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &runbase.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &runbase.ServerConfig{
			Settings:    st,
			DiskMonitor: diskMonitor,
		},
	}

	var da sqlbase.DatumAlloc
	rng, _ := randutil.NewPseudoRand()

	for _, typ := range allSupportedSQLTypes {
		if typ.Equal(*types1.Decimal) || typ.Equal(*types1.Timestamp) {
			// Serialization of Decimals is currently not supported.
			// TODO(yuzefovich): remove this once it is supported.
			continue
		}
		for _, numRows := range []uint16{
			// A few interesting sizes.
			1,
			coldata.BatchSize() - 1,
			coldata.BatchSize(),
			coldata.BatchSize() + 1,
		} {
			rows := make(sqlbase.EncDatumRows, numRows)
			for i := uint16(0); i < numRows; i++ {
				rows[i] = make(sqlbase.EncDatumRow, 1)
				ctp, _ := sqlbase.DatumType1ToColumnType(typ)
				rows[i][0] = sqlbase.DatumToEncDatum(ctp, sqlbase.RandDatum(rng, ctp, true /* nullOk */))
			}
			typs := []types1.T{typ}
			source := runbase.NewRepeatableRowSource(sem.ToOldTypes(typs), rows)

			columnarizer, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0 /* processorID */, source)
			require.NoError(t, err)

			cts, err := sqlbase.DatumType1sToColumnTypes(typs)
			coltyps, err := conv.FromColumnType1s(cts)
			require.NoError(t, err)
			c, err := colserde.NewArrowBatchConverter(coltyps)
			require.NoError(t, err)
			r, err := colserde.NewRecordBatchSerializer(coltyps)
			require.NoError(t, err)
			arrowOp := newArrowTestOperator(columnarizer, c, r)

			output := runbase.NewRowBuffer(cts, nil /* rows */, runbase.RowBufferArgs{})
			materializer, err := NewMaterializer(
				flowCtx,
				1, /* processorID */
				arrowOp,
				typs,
				&distsqlpb.PostProcessSpec{},
				output,
				nil, /* metadataSourcesQueue */
				nil, /* outputStatsToTrace */
				nil, /* cancelFlow */
			)
			require.NoError(t, err)

			materializer.Start(ctx)
			materializer.Run(ctx)
			actualRows := output.GetRowsNoMeta(t)
			require.Equal(t, len(rows), len(actualRows))
			ct, _ := sqlbase.DatumType1ToColumnType(typ)
			for rowIdx, expectedRow := range rows {
				require.Equal(t, len(expectedRow), len(actualRows[rowIdx]))
				cmp, err := expectedRow[0].Compare(&ct, &da, &evalCtx, &actualRows[rowIdx][0])
				require.NoError(t, err)
				require.Equal(t, 0, cmp)
			}
		}
	}
}

// arrowTestOperator is an Operator that takes in a coldata.Batch from its
// input, passes it through a chain of
// - converting to Arrow format
// - serializing
// - deserializing
// - converting from Arrow format
// and returns the resulting batch.
type arrowTestOperator struct {
	OneInputNode

	c *colserde.ArrowBatchConverter
	r *colserde.RecordBatchSerializer
}

var _ Operator = &arrowTestOperator{}

func newArrowTestOperator(
	input Operator, c *colserde.ArrowBatchConverter, r *colserde.RecordBatchSerializer,
) Operator {
	return &arrowTestOperator{
		OneInputNode: NewOneInputNode(input),
		c:            c,
		r:            r,
	}
}

func (a *arrowTestOperator) Init() {
	a.input.Init()
}

func (a *arrowTestOperator) Next(ctx context.Context) coldata.Batch {
	batchIn := a.input.Next(ctx)
	// Note that we don't need to handle zero-length batches in a special way.
	var buf bytes.Buffer
	arrowDataIn, err := a.c.BatchToArrow(batchIn)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	_, _, err = a.r.Serialize(&buf, arrowDataIn)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	var arrowDataOut []*array.Data
	if err := a.r.Deserialize(&arrowDataOut, buf.Bytes()); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	batchOut := testAllocator.NewMemBatchWithSize(nil, 0)
	if err := a.c.ArrowToBatch(arrowDataOut, batchOut); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return batchOut
}
