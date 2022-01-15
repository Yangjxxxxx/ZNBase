// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/randutil"
)

func TestSerialUnorderedSynchronizer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()
	const numInputs = 3
	const numBatches = 4

	typs := []coltypes.T{coltypes.Int64}
	inputs := make([]Operator, numInputs)
	for i := range inputs {
		batch := RandomBatch(testAllocator, rng, typs, int(coldata.BatchSize()), 0 /* length */, rng.Float64())
		source := NewRepeatableBatchSource(batch)
		source.ResetBatchesToReturn(numBatches)
		inputs[i] = source
	}
	s := NewSerialUnorderedSynchronizer(inputs, typs)
	resultBatches := 0
	for {
		b := s.Next(ctx)
		if b.Length() == 0 {
			break
		}
		resultBatches++
	}
	require.Equal(t, numInputs*numBatches, resultBatches)
}
