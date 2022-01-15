// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"testing"

	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestConst(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{1, 9}, {1, 9}},
		},
		{
			tuples:   tuples{},
			expected: tuples{},
		},
	}
	for _, tc := range tcs {
		runTestsWithTyps(t, []tuples{tc.tuples}, [][]coltypes.T{{coltypes.Int64}}, tc.expected, orderedVerifier,
			func(input []Operator) (Operator, error) {
				return NewConstOp(testAllocator, input[0], coltypes.Int64, int64(9), 1)
			})
	}
}

func TestConstNull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{1, nil}, {1, nil}},
		},
		{
			tuples:   tuples{},
			expected: tuples{},
		},
	}
	for _, tc := range tcs {
		runTestsWithTyps(t, []tuples{tc.tuples}, [][]coltypes.T{{coltypes.Int64}}, tc.expected, orderedVerifier,
			func(input []Operator) (Operator, error) {
				return NewConstNullOp(testAllocator, input[0], 1, coltypes.Int64), nil
			})
	}
}
