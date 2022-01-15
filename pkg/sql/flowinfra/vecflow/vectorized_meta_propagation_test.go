// Copyright 2019  The Cockroach Authors.

package vecflow_test

import (
	"context"
	"testing"

	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// TestVectorizedMetaPropagation tests whether metadata is correctly propagated
// alongside columnar operators. It sets up the following "flow":
// RowSource -> metadataTestSender -> columnarizer -> noopOperator ->
// -> materializer -> metadataTestReceiver. Metadata propagation is hooked up
// manually from the columnarizer into the materializer similar to how it is
// done in setupVectorizedFlow.
func TestVectorizedMetaPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := runbase.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &runbase.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows := 10
	nCols := 1
	types := sqlbase.OneIntCol

	input := runbase.NewRowBuffer(types, sqlbase.MakeIntRows(nRows, nCols), runbase.RowBufferArgs{})
	mtsSpec := distsqlpb.ProcessorCoreUnion{
		MetadataTestSender: &distsqlpb.MetadataTestSenderSpec{
			ID: uuid.MakeV4().String(),
		},
	}
	mts, err := runbase.NewMetadataTestSender(
		&flowCtx,
		0,
		input,
		&distsqlpb.PostProcessSpec{},
		nil,
		uuid.MakeV4().String(),
	)
	if err != nil {
		t.Fatal(err)
	}

	col, err := vecexec.NewColumnarizer(ctx, testAllocator, &flowCtx, 1, mts)
	if err != nil {
		t.Fatal(err)
	}

	noop := vecexec.NewNoop(col)
	cts, _ := sqlbase.ToType1s(types)
	mat, err := vecexec.NewMaterializer(
		&flowCtx,
		2, /* processorID */
		noop,
		cts,
		&distsqlpb.PostProcessSpec{},
		nil, /* output */
		[]distsqlpb.MetadataSource{col},
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}

	mtr, err := runbase.NewMetadataTestReceiver(
		&flowCtx,
		3,
		mat,
		&distsqlpb.PostProcessSpec{},
		nil,
		[]string{mtsSpec.MetadataTestSender.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	mtr.Start(ctx)

	rowCount, metaCount := 0, 0
	for {
		row, meta := mtr.Next()
		if row == nil && meta == nil {
			break
		}
		if row != nil {
			rowCount++
		} else if meta.Err != nil {
			t.Fatal(meta.Err)
		} else {
			metaCount++
		}
	}
	if rowCount != nRows {
		t.Fatalf("expected %d rows but %d received", nRows, rowCount)
	}
	if metaCount != nRows+1 {
		// metadataTestSender sends a meta after each row plus an additional one to
		// indicate the last meta.
		t.Fatalf("expected %d meta but %d received", nRows+1, metaCount)
	}
}
