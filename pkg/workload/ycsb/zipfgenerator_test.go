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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package ycsb

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type params struct {
	iMin, iMax uint64
	theta      float64
}

var gens = []params{
	{0, 100, 0.99},
	{0, 100, 1.01},
}

func TestCreateZipfGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, gen := range gens {
		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		_, err := NewZipfGenerator(rng, gen.iMin, gen.iMax, gen.theta, false)
		if err != nil {
			t.Fatal(err)
		}
	}
}

var tests = []struct {
	n        uint64
	theta    float64
	expected float64
}{
	{20, 0.99, 3.64309060779367},
	{200, 0.99, 6.02031118558},
	{1000, 0.99, 7.72895321728},
	{2000, 0.99, 8.47398788329},
	{10000, 0.99, 10.2243614596},
	{100000, 0.99, 12.7783380626},
	{1000000, 0.99, 15.391849746},
	{10000000, 0.99, 18.066242575},
	{100000000, 0.99, 20.80293049},
}

func TestZetaFromScratch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("short")
	}
	for _, test := range tests {
		computedZeta, err := computeZetaFromScratch(test.n, test.theta)
		if err != nil {
			t.Fatalf("Failed to compute zeta(%d,%f): %s", test.n, test.theta, err)
		}
		if math.Abs(computedZeta-test.expected) > 0.000000001 {
			t.Fatalf("expected %6.4f, got %6.4f", test.expected, computedZeta)
		}
	}
}

func TestZetaIncrementally(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("short")
	}
	// Theta cannot be 1 by definition, so this is a safe initial value.
	oldTheta := 1.0
	var oldZetaN float64
	var oldN uint64
	for _, test := range tests {
		// If theta has changed, recompute from scratch
		if test.theta != oldTheta {
			var err error
			oldZetaN, err = computeZetaFromScratch(test.n, test.theta)
			if err != nil {
				t.Fatalf("Failed to compute zeta(%d,%f): %s", test.n, test.theta, err)
			}
			oldN = test.n
			continue
		}

		computedZeta, err := computeZetaIncrementally(oldN, test.n, test.theta, oldZetaN)
		if err != nil {
			t.Fatalf("Failed to compute zeta(%d,%f) incrementally: %s", test.n, test.theta, err)
		}
		if math.Abs(computedZeta-test.expected) > 0.000000001 {
			t.Fatalf("expected %6.4f, got %6.4f", test.expected, computedZeta)
		}

		oldZetaN = computedZeta
		oldN = test.n
	}
}

func runZipfGenerators(t *testing.T, withIncrements bool) {
	gen := gens[0]
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	z, err := NewZipfGenerator(rng, gen.iMin, gen.iMax, gen.theta, false)
	if err != nil {
		t.Fatal(err)
	}

	const ROLLS = 10000
	x := make([]int, ROLLS)

	for i := 0; i < ROLLS; i++ {
		x[i] = int(z.Uint64())
		z.zipfGenMu.mu.Lock()
		if x[i] < int(z.iMin) || x[i] > int(z.zipfGenMu.iMax) {
			t.Fatalf("zipf(%d,%d,%f) rolled %d at index %d", z.iMin, z.zipfGenMu.iMax, z.theta, x[i], i)
			z.zipfGenMu.mu.Unlock()
			if withIncrements {
				if err := z.IncrementIMax(); err != nil {
					t.Fatalf("could not increment iMax: %s", err)
				}
			}
		}
		z.zipfGenMu.mu.Unlock()
	}

	if withIncrements {
		return
	}

	sort.Ints(x)

	max := x[ROLLS-1]
	step := max / 20
	index := 0
	count := 0
	for i := 0; i < max; i += step {
		count = 0
		for {
			if x[index] >= i {
				break
			}
			index++
			count++
		}
		fmt.Printf("[%10d-%10d)        ", i, i+step)
		for j := 0; j < count; j++ {
			if j%50 == 0 {
				fmt.Printf("%c", '∎')
			}
		}
		fmt.Println()
	}
}

func TestZipfGenerator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runZipfGenerators(t, false)
	runZipfGenerators(t, true)
}
