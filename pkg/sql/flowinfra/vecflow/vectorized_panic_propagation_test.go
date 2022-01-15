// Copyright 2019  The Cockroach Authors.

package vecflow_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// TestVectorizedInternalPanic verifies that materializers successfully
// handle panics coming from exec package. It sets up the following chain:
// RowSource -> columnarizer -> test panic emitter -> materializer,
// and makes sure that a panic doesn't occur yet the error is propagated.
func TestVectorizedInternalPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := runbase.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &runbase.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows, nCols := 1, 1
	typs := sqlbase.OneIntCol
	input := runbase.NewRepeatableRowSource(sqlbase.ToTypes(typs), sqlbase.MakeIntRows(nRows, nCols))

	col, err := vecexec.NewColumnarizer(ctx, testAllocator, &flowCtx, 0 /* processorID */, input)
	if err != nil {
		t.Fatal(err)
	}

	vee := newTestVectorizedInternalPanicEmitter(col)
	mat, err := vecexec.NewMaterializer(
		&flowCtx,
		1, /* processorID */
		vee,
		sqlbase.ToType1sWithoutErr(typs),
		&distsqlpb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourceQueue */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}
	mat.Start(ctx)

	var meta *distsqlpb.ProducerMetadata
	require.NotPanics(t, func() { _, meta = mat.Next() }, "VectorizedInternalPanic was not caught")
	require.NotNil(t, meta.Err, "VectorizedInternalPanic was not propagated as metadata")
}

// TestNonVectorizedPanicPropagation verifies that materializers do not handle
// panics coming not from exec package. It sets up the following chain:
// RowSource -> columnarizer -> test panic emitter -> materializer,
// and makes sure that a panic is emitted all the way through the chain.
func TestNonVectorizedPanicPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := runbase.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &runbase.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows, nCols := 1, 1
	typs := sqlbase.OneIntCol
	input := runbase.NewRepeatableRowSource(sqlbase.ToTypes(typs), sqlbase.MakeIntRows(nRows, nCols))

	col, err := vecexec.NewColumnarizer(ctx, testAllocator, &flowCtx, 0 /* processorID */, input)
	if err != nil {
		t.Fatal(err)
	}

	nvee := newTestNonVectorizedPanicEmitter(col)
	mat, err := vecexec.NewMaterializer(
		&flowCtx,
		1, /* processorID */
		nvee,
		sqlbase.ToType1sWithoutErr(typs),
		&distsqlpb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourceQueue */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}
	mat.Start(ctx)

	require.Panics(t, func() { mat.Next() }, "NonVectorizedPanic was caught by the operators")
}

// testVectorizedInternalPanicEmitter is an vecexec.Operator that panics with
// execerror.VectorizedInternalPanic on every odd-numbered invocation of Next()
// and returns the next batch from the input on every even-numbered (i.e. it
// becomes a noop for those iterations). Used for tests only.
type testVectorizedInternalPanicEmitter struct {
	vecexec.OneInputNode
	emitBatch bool
}

var _ vecexec.Operator = &testVectorizedInternalPanicEmitter{}

func newTestVectorizedInternalPanicEmitter(input vecexec.Operator) vecexec.Operator {
	return &testVectorizedInternalPanicEmitter{
		OneInputNode: vecexec.NewOneInputNode(input),
	}
}

// Init is part of exec.Operator interface.
func (e *testVectorizedInternalPanicEmitter) Init() {
	e.Input().Init()
}

// Next is part of exec.Operator interface.
func (e *testVectorizedInternalPanicEmitter) Next(ctx context.Context) coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		execerror.VectorizedInternalPanic("")
	}

	e.emitBatch = false
	return e.Input().Next(ctx)
}

// testNonVectorizedPanicEmitter is the same as
// testVectorizedInternalPanicEmitter but it panics with the builtin panic
// function. Used for tests only. It is the only vecexec.Operator panics from
// which are not caught.
type testNonVectorizedPanicEmitter struct {
	vecexec.OneInputNode
	emitBatch bool
}

var _ vecexec.Operator = &testVectorizedInternalPanicEmitter{}

func newTestNonVectorizedPanicEmitter(input vecexec.Operator) vecexec.Operator {
	return &testNonVectorizedPanicEmitter{
		OneInputNode: vecexec.NewOneInputNode(input),
	}
}

// Init is part of exec.Operator interface.
func (e *testNonVectorizedPanicEmitter) Init() {
	e.Input().Init()
}

// Next is part of exec.Operator interface.
func (e *testNonVectorizedPanicEmitter) Next(ctx context.Context) coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		panic("")
	}

	e.emitBatch = false
	return e.Input().Next(ctx)
}
