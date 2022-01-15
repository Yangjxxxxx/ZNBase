// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"context"
	"testing"

	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestOrdinality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		tuples   []tuple
		expected []tuple
	}{
		{
			tuples:   tuples{{1}},
			expected: tuples{{1, 1}},
		},
		{
			tuples:   tuples{{}, {}, {}, {}, {}},
			expected: tuples{{1}, {2}, {3}, {4}, {5}},
		},
		{
			tuples:   tuples{{5}, {6}, {7}, {8}},
			expected: tuples{{5, 1}, {6, 2}, {7, 3}, {8, 4}},
		},
		{
			tuples:   tuples{{5, 'a'}, {6, 'b'}, {7, 'c'}, {8, 'd'}},
			expected: tuples{{5, 'a', 1}, {6, 'b', 2}, {7, 'c', 3}, {8, 'd', 4}},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier,
			func(input []Operator) (Operator, error) {
				return NewOrdinalityOp(testAllocator, input[0]), nil
			})
	}
}

func BenchmarkOrdinality(b *testing.B) {
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64})
	batch.SetLength(coldata.BatchSize())
	source := NewRepeatableBatchSource(batch)
	source.Init()

	ordinality := NewOrdinalityOp(testAllocator, source)

	b.SetBytes(int64(8 * int(coldata.BatchSize()) * batch.Width()))
	for i := 0; i < b.N; i++ {
		ordinality.Next(ctx)
	}
}
