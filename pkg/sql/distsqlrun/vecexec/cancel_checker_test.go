// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// TestCancelChecker verifies that CancelChecker panics with appropriate error
// when the context is canceled.
func TestCancelChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
	op := NewCancelChecker(NewNoop(NewRepeatableBatchSource(batch)))
	cancel()
	err := execerror.CatchVecRuntimeError(func() {
		op.Next(ctx)
	})
	require.Equal(t, sqlbase.QueryCanceledError, err)
}
