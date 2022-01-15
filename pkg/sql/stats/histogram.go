// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package stats

import (
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
)

// HistogramClusterMode controls the cluster setting for enabling
// histogram collection.
var HistogramClusterMode = settings.RegisterBoolSetting(
	"sql.stats.histogram_collection.enabled",
	"histogram collection mode",
	false,
)

// EquiDepthHistogram creates a histogram where each bucket contains roughly the
// same number of samples (though it can vary when a boundary value has high
// frequency).
//
// numRows is the total number of rows from which values were sampled.
func EquiDepthHistogram(
	evalCtx *tree.EvalContext,
	samples tree.Datums,
	numRows int64,
	distinctCount int64,
	maxBuckets int,
) (HistogramData, error) {
	numSamples := len(samples)
	if numSamples == 0 {
		return HistogramData{}, nil
	}
	if numRows < int64(numSamples) {
		return HistogramData{}, errors.Errorf("more samples than rows")
	}
	for _, d := range samples {
		if d == tree.DNull {
			return HistogramData{}, errors.Errorf("NULL values not allowed in histogram")
		}
	}
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Compare(evalCtx, samples[j]) < 0
	})
	numBuckets := maxBuckets
	if maxBuckets > numSamples {
		numBuckets = numSamples
	}
	h := HistogramData{
		Buckets: make([]HistogramData_Bucket, 0, numBuckets),
	}
	var err error
	lowerBound := samples[0]
	h.ColumnType, err = sqlbase.DatumTypeToColumnType(lowerBound.ResolvedType())
	if err != nil {
		return HistogramData{}, err
	}
	var distinctCountRange, distinctCountEq float64
	// i keeps track of the current sample and advances as we form buckets.
	for i, b := 0, 0; b < numBuckets && i < numSamples; b++ {
		// num is the number of samples in this bucket.
		num := (numSamples - i) / (numBuckets - b)
		if num < 1 || i == 0 {
			num = 1
		}
		upper := samples[i+num-1]
		// numLess is the number of samples less than upper (in this bucket).
		numLess := 0
		for ; numLess < num-1; numLess++ {
			if c := samples[i+numLess].Compare(evalCtx, upper); c == 0 {
				break
			} else if c > 0 {
				panic("samples not sorted")
			}
		}
		// Advance the boundary of the bucket to cover all samples equal to upper.
		for ; i+num < numSamples; num++ {
			if samples[i+num].Compare(evalCtx, upper) != 0 {
				break
			}
		}
		numEq := int64(num-numLess) * numRows / int64(numSamples)
		numRange := int64(numLess) * numRows / int64(numSamples)
		distinctRange := estimatedDistinctValuesInRange(float64(numRange), lowerBound, upper)
		upperEncoded, err := sqlbase.EncodeTableKey(nil, upper, encoding.Ascending)
		lowerEncoded, err := sqlbase.EncodeTableKey(nil, lowerBound, encoding.Ascending)
		if err != nil {
			return HistogramData{}, err
		}
		i += num
		h.Buckets = append(h.Buckets, HistogramData_Bucket{
			NumEq:         numEq,
			NumRange:      numRange,
			DistinctRange: distinctRange,
			UpperBound:    upperEncoded,
			LowerBound:    lowerEncoded,
		})
		// Keep track of the total number of estimated distinct values. This will
		// be used to adjust the distinct count below.
		distinctCountRange += distinctRange
		if numEq > 0 {
			distinctCountEq++
		}
		lowerBound = getNextLowerBound(evalCtx, upper)
	}
	h.adjustDistinctCount(float64(distinctCount), distinctCountRange, distinctCountEq)
	return h, nil
}

// maxDistinctValuesInRange returns the maximum number of distinct values in
// the range [lowerBound, upperBound). It returns ok=false when it is not
// possible to determine a finite value (which is the case for all types other
// than integers and dates).
func maxDistinctValuesInRange(lowerBound, upperBound tree.Datum) (_ float64, ok bool) {
	switch lowerBound.ResolvedType() {
	case types.Int:
		return float64(*upperBound.(*tree.DInt)) - float64(*lowerBound.(*tree.DInt)), true

	case types.Date:
		lower := lowerBound.(*tree.DDate)
		upper := upperBound.(*tree.DDate)
		if lower.IsFinite() && upper.IsFinite() {
			return float64(*upper) - float64(*lower), true
		}
		return 0, false

	default:
		return 0, false
	}
}

func getNextLowerBound(evalCtx *tree.EvalContext, currentUpperBound tree.Datum) tree.Datum {
	nextLowerBound, ok := currentUpperBound.Next(evalCtx)
	if !ok {
		nextLowerBound = currentUpperBound
	}
	return nextLowerBound
}

// estimatedDistinctValuesInRange returns the estimated number of distinct
// values in the range [lowerBound, upperBound), given that the total number
// of values is numRange.
func estimatedDistinctValuesInRange(numRange float64, lowerBound, upperBound tree.Datum) float64 {
	if maxDistinct, ok := maxDistinctValuesInRange(lowerBound, upperBound); ok {
		return expectedDistinctCount(numRange, maxDistinct)
	}
	return numRange
}

// adjustDistinctCount adjusts the number of distinct values per bucket based
// on the total number of distinct values.
func (h *HistogramData) adjustDistinctCount(
	distinctCountTotal, distinctCountRange, distinctCountEq float64,
) {
	if distinctCountRange == 0 {
		return
	}

	adjustmentFactor := (distinctCountTotal - distinctCountEq) / distinctCountRange
	if adjustmentFactor < 0 {
		adjustmentFactor = 0
	}
	for i := range h.Buckets {
		h.Buckets[i].DistinctRange *= adjustmentFactor
	}
}

// expectedDistinctCount returns the expected number of distinct values
// among k random numbers selected from n possible values. We assume the
// values are chosen using uniform random sampling with replacement.
func expectedDistinctCount(k, n float64) float64 {
	if n == 0 || k == 0 {
		return 0
	}
	// The probability that one specific value (out of the n possible values)
	// does not appear in any of the k selections is:
	//
	//         ⎛ n-1 ⎞ k
	//     p = ⎜-----⎟
	//         ⎝  n  ⎠
	//
	// Therefore, the probability that a specific value appears at least once is
	// 1-p. Over all n values, the expected number that appear at least once is
	// n * (1-p). In other words, the expected distinct count is:
	//
	//                             ⎛     ⎛ n-1 ⎞ k ⎞
	//     E[distinct count] = n * ⎜ 1 - ⎜-----⎟   ⎟
	//                             ⎝     ⎝  n  ⎠   ⎠
	//
	// See https://math.stackexchange.com/questions/72223/finding-expected-
	//   number-of-distinct-values-selected-from-a-set-of-integers for more info.
	count := n * (1 - math.Pow((n-1)/n, k))

	// It's possible that if n is very large, floating point precision errors
	// will cause count to be 0. In that case, just return min(n, k).
	if count == 0 {
		count = k
		if n < k {
			count = n
		}
	}
	return count
}
