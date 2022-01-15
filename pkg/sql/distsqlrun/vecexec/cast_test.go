// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"fmt"
	"testing"

	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/sem"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types1"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/randutil"
)

func TestRandomizedCast(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datumAsBool := func(d tree.Datum) interface{} {
		return bool(tree.MustBeDBool(d))
	}
	datumAsInt := func(d tree.Datum) interface{} {
		return int(tree.MustBeDInt(d))
	}
	datumAsFloat := func(d tree.Datum) interface{} {
		return float64(tree.MustBeDFloat(d))
	}
	datumAsDecimal := func(d tree.Datum) interface{} {
		return tree.MustBeDDecimal(d).Decimal
	}

	tc := []struct {
		fromTyp      *types1.T
		fromPhysType func(tree.Datum) interface{}
		toTyp        *types1.T
		toPhysType   func(tree.Datum) interface{}
		// Some types1 casting can fail, so retry if we
		// generate a datum that is unable to be casted.
		retryGeneration bool
	}{
		//bool -> t tests
		{types1.Bool, datumAsBool, types1.Bool, datumAsBool, false},
		{types1.Bool, datumAsBool, types1.Int, datumAsInt, false},
		{types1.Bool, datumAsBool, types1.Float, datumAsFloat, false},
		// decimal -> t tests
		{types1.Decimal, datumAsDecimal, types1.Bool, datumAsBool, false},
		// int -> t tests
		{types1.Int, datumAsInt, types1.Bool, datumAsBool, false},
		{types1.Int, datumAsInt, types1.Float, datumAsFloat, false},
		{types1.Int, datumAsInt, types1.Decimal, datumAsDecimal, false},
		// float -> t tests
		{types1.Float, datumAsFloat, types1.Bool, datumAsBool, false},
		// We can sometimes generate a float outside of the range of the integers,
		// so we want to retry with generation if that occurs.
		{types1.Float, datumAsFloat, types1.Int, datumAsInt, true},
		{types1.Float, datumAsFloat, types1.Decimal, datumAsDecimal, false},
	}

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	rng, _ := randutil.NewPseudoRand()

	for _, c := range tc {
		t.Run(fmt.Sprintf("%sTo%s", c.fromTyp.String(), c.toTyp.String()), func(t *testing.T) {
			n := 100
			// Make an input vector of length n.
			input := tuples{}
			output := tuples{}
			for i := 0; i < n; i++ {
				// We don't allow any NULL datums to be generated, so disable
				// this ability in the RandDatum function.
				cty, errcon := sqlbase.DatumType1ToColumnType(*c.fromTyp)
				if errcon != nil {
					return
				}
				fromDatum := sqlbase.RandDatum(rng, cty, false)
				var (
					toDatum tree.Datum
					err     error
				)
				clt, _ := coltypes.DatumTypeToColumnType(sem.ToOldType(c.toTyp))
				toDatum, err = tree.PerformCast(evalCtx, fromDatum, clt)
				if c.retryGeneration {
					for err != nil {
						// If we are allowed to retry, make a new datum and cast it on error.
						fromDatum = sqlbase.RandDatum(rng, cty, false)

						toDatum, err = tree.PerformCast(evalCtx, fromDatum, clt)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
				}
				input = append(input, tuple{c.fromPhysType(fromDatum)})
				output = append(output, tuple{c.fromPhysType(fromDatum), c.toPhysType(toDatum)})
			}
			runTests(t, []tuples{input}, output, orderedVerifier,
				func(input []Operator) (Operator, error) {
					return GetCastOperator(testAllocator, input[0], 0 /* inputIdx*/, 1 /* resultIdx */, c.fromTyp, c.toTyp)
				})
		})
	}
}
