// Copyright 2015  The Cockroach Authors.
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

package builtins

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util/arith"
	"github.com/znbasedb/znbase/pkg/util/duration"
	"github.com/znbasedb/znbase/pkg/util/json"
	"github.com/znbasedb/znbase/pkg/util/mon"
)

func initAggregateBuiltins() {
	// Add all aggregates to the Builtins map after a few sanity checks.
	for k, v := range aggregates {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}

		if !v.props.Impure {
			panic(fmt.Sprintf("%s: aggregate functions should all be impure, found %v", k, v))
		}
		if v.props.Class != tree.AggregateClass {
			panic(fmt.Sprintf("%s: aggregate functions should be marked with the tree.AggregateClass "+
				"function class, found %v", k, v))
		}
		for _, a := range v.overloads {
			if a.AggregateFunc == nil {
				panic(fmt.Sprintf("%s: aggregate functions should have tree.AggregateFunc constructors, "+
					"found %v", k, a))
			}
			if a.WindowFunc == nil {
				panic(fmt.Sprintf("%s: aggregate functions should have tree.WindowFunc constructors, "+
					"found %v", k, a))
			}
		}

		// The aggregate functions are considered "row dependent". This is
		// because each aggregate function application receives the set of
		// grouped rows as implicit parameter. It may have a different
		// value in every group, so it cannot be considered constant in
		// the context of a data source.
		v.props.NeedsRepeatedEvaluation = true

		builtins[k] = v
	}
}

func aggProps() tree.FunctionProperties {
	return tree.FunctionProperties{Class: tree.AggregateClass, Impure: true}
}

func aggPropsNullableArgs() tree.FunctionProperties {
	f := aggProps()
	f.NullableArgs = true
	return f
}

// aggregates are a special class of builtin functions that are wrapped
// at execution in a bucketing layer to combine (aggregate) the result
// of the function being run over many rows.
//
// See `aggregateFuncHolder` in the sql package.
//
// In particular they must not be simplified during normalization
// (and thus must be marked as impure), even when they are given a
// constant argument (e.g. SUM(1)). This is because aggregate
// functions must return NULL when they are no rows in the source
// table, so their evaluation must always be delayed until query
// execution.
//
// Some aggregate functions must handle nullable arguments, since normalizing
// an aggregate function call to NULL in the presence of a NULL argument may
// not be correct. There are two cases where an aggregate function must handle
// nullable arguments:
// 1) the aggregate function does not skip NULLs (e.g., ARRAY_AGG); and
// 2) the aggregate function does not return NULL when it aggregates no rows
//		(e.g., COUNT).
//
// For use in other packages, see AllAggregateBuiltinNames and
// GetBuiltinProperties().
// These functions are also identified with Class == tree.AggregateClass.
// The properties are reachable via tree.FunctionDefinition.
var aggregates = map[string]builtinDefinition{
	"array_agg": setProps(aggPropsNullableArgs(),
		arrayBuiltin(func(t types.T) tree.Overload {
			return makeAggOverloadWithReturnType(
				[]types.T{t},
				func(args []tree.TypedExpr) types.T {
					if len(args) == 0 {
						return types.TArray{Typ: t}
					}
					// Whenever possible, use the expression's type, so we can properly
					// handle aliased types that don't explicitly have overloads.
					return types.TArray{Typ: args[0].ResolvedType()}
				},
				newArrayAggregate,
				"Aggregates the selected values into an array.",
			)
		})),

	"avg": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Int}, types.Decimal, newIntAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggOverload([]types.T{types.Float}, types.Float, newFloatAvgAggregate,
			"Calculates the average of the selected values."),
		makeAggOverload([]types.T{types.Decimal}, types.Decimal, newDecimalAvgAggregate,
			"Calculates the average of the selected values."),
	),

	"bool_and": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Bool}, types.Bool, newBoolAndAggregate,
			"Calculates the boolean value of `AND`ing all selected values."),
	),

	"bool_or": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Bool}, types.Bool, newBoolOrAggregate,
			"Calculates the boolean value of `OR`ing all selected values."),
	),

	"concat_agg": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values."),
		makeAggOverload([]types.T{types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values."),
		// TODO(eisen): support collated strings when the type system properly
		// supports parametric types.
	),

	"group_concat": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.String}, types.String, newStringGroupConcat,
			"Concatenates all selected values using the provided delimiter','."),
		makeAggOverload([]types.T{types.Bytes}, types.Bytes, newBytesGroupConcat,
			"Concatenates all selected values using the provided delimiter','."),
		makeAggOverload([]types.T{types.String, types.String}, types.String, newStringGroupConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
		makeAggOverload([]types.T{types.Bytes, types.Bytes}, types.Bytes, newBytesGroupConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
	),

	"count": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]types.T{types.Any}, types.Int, newCountAggregate,
			"Calculates the number of selected elements."),
	),

	"count_rows": makeBuiltin(aggProps(),
		tree.Overload{
			Types:         tree.ArgTypes{},
			ReturnType:    tree.FixedReturnType(types.Int),
			AggregateFunc: newCountRowsAggregate,
			WindowFunc: func(params []types.T, evalCtx *tree.EvalContext, account *mon.BoundAccount) tree.WindowFunc {
				return newFramableAggregateWindow(
					newCountRowsAggregate(params, evalCtx, nil /* arguments */, account),
					func(evalCtx *tree.EvalContext, arguments tree.Datums, boundAccount *mon.BoundAccount) tree.AggregateFunc {
						return newCountRowsAggregate(params, evalCtx, arguments, boundAccount)
					},
				)
			},
			Info: "Calculates the number of rows.",
		},
	),

	"first_value": collectOverloads(aggProps(), types.AnyNonArray,
		func(t types.T) tree.Overload {
			return makeAggOverload([]types.T{t}, t, newFirstValueAggregate,
				"Identifies the first selected value.")
		}),

	"last_value": collectOverloads(aggProps(), types.AnyNonArray,
		func(t types.T) tree.Overload {
			return makeAggOverload([]types.T{t}, t, newLastValueAggregate,
				"Identifies the last selected value.")
		}),

	"max": collectOverloads(aggProps(), types.AnyNonArray,
		func(t types.T) tree.Overload {
			return makeAggOverload([]types.T{t}, t, newMaxAggregate,
				"Identifies the maximum selected value.")
		}),

	"min": collectOverloads(aggProps(), types.AnyNonArray,
		func(t types.T) tree.Overload {
			return makeAggOverload([]types.T{t}, t, newMinAggregate,
				"Identifies the minimum selected value.")
		}),

	"string_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]types.T{types.String, types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
		makeAggOverload([]types.T{types.Bytes, types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
	),

	"listagg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]types.T{types.String, types.String}, types.String, newStringConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
		makeAggOverload([]types.T{types.Bytes, types.Bytes}, types.Bytes, newBytesConcatAggregate,
			"Concatenates all selected values using the provided delimiter."),
	),

	"stats_mode": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.String}, types.String, newStatsModeAggregate,
			"Returns the value that occurs with the greatest frequency."),
		makeAggOverload([]types.T{types.Int}, types.Int, newStatsModeAggregate,
			"Returns the value that occurs with the greatest frequency."),
		makeAggOverload([]types.T{types.Float}, types.Float, newStatsModeAggregate,
			"Returns the value that occurs with the greatest frequency."),
		makeAggOverload([]types.T{types.Decimal}, types.Decimal, newStatsModeAggregate,
			"Returns the value that occurs with the greatest frequency."),
	),

	"any_value": collectOverloads(aggPropsNullableArgs(), types.AnyNonArray,
		func(t types.T) tree.Overload {
			return makeAggOverload([]types.T{t}, t, newAnyValueAggregate,
				"Returns a value of an expression in a group. It is optimized to return the first value.")
		}),

	"median": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Float}, types.Float, newFloatMedianAggregate,
			"Calculates the median of the selected values."),
		makeAggOverload([]types.T{types.Int}, types.Float, newIntMedianAggregate,
			"Calculates the median of the selected values."),
	),

	"sum_int": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Int}, types.Int, newSmallIntSumAggregate,
			"Calculates the sum of the selected values."),
	),

	"sum": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Int}, types.Decimal, newIntSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]types.T{types.Float}, types.Float, newFloatSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]types.T{types.Decimal}, types.Decimal, newDecimalSumAggregate,
			"Calculates the sum of the selected values."),
		makeAggOverload([]types.T{types.Interval}, types.Interval, newIntervalSumAggregate,
			"Calculates the sum of the selected values."),
	),

	"sqrdiff": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Int}, types.Decimal, newIntSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
		makeAggOverload([]types.T{types.Decimal}, types.Decimal, newDecimalSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
		makeAggOverload([]types.T{types.Float}, types.Float, newFloatSqrDiffAggregate,
			"Calculates the sum of squared differences from the mean of the selected values."),
	),

	// final_(variance|stddev) computes the global (variance|standard deviation)
	// from an arbitrary collection of local sums of squared difference from the mean.
	// Adapted from https://www.johndcook.com/blog/skewness_kurtosis and
	// https://github.com/znbasedb/znbase/pull/17728.

	// TODO(knz): The 3-argument final_variance and final_stddev are
	// only defined for internal use by distributed aggregations. They
	// are marked as "private" so as to not trigger panics from issue
	// #10495.

	// The input signature is: SQDIFF, SUM, COUNT
	"final_variance": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]types.T{types.Decimal, types.Decimal, types.Int},
			types.Decimal,
			newDecimalFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalVarianceAggregate,
			"Calculates the variance from the selected locally-computed squared difference values.",
		),
	)),

	"final_stddev": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]types.T{types.Decimal,
				types.Decimal, types.Int},
			types.Decimal,
			newDecimalFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalStdDevAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
	)),

	"final_stddev_samp": makePrivate(makeBuiltin(aggProps(),
		makeAggOverload(
			[]types.T{types.Decimal, types.Decimal, types.Int},
			types.Decimal,
			newDecimalFinalStddevSampAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
		makeAggOverload(
			[]types.T{types.Float, types.Float, types.Int},
			types.Float,
			newFloatFinalStddevSampAggregate,
			"Calculates the standard deviation from the selected locally-computed squared difference values.",
		),
	)),

	"variance": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Int}, types.Decimal, newIntVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggOverload([]types.T{types.Decimal}, types.Decimal, newDecimalVarianceAggregate,
			"Calculates the variance of the selected values."),
		makeAggOverload([]types.T{types.Float}, types.Float, newFloatVarianceAggregate,
			"Calculates the variance of the selected values."),
	),

	"stddev": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Int}, types.Decimal, newIntStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggOverload([]types.T{types.Decimal}, types.Decimal, newDecimalStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggOverload([]types.T{types.Float}, types.Float, newFloatStdDevAggregate,
			"Calculates the standard deviation of the selected values."),
	),

	"stddev_samp": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Int}, types.Decimal, newIntStddevSampAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggOverload([]types.T{types.Decimal}, types.Decimal, newDecimalStddevSampAggregate,
			"Calculates the standard deviation of the selected values."),
		makeAggOverload([]types.T{types.Float}, types.Float, newFloatStddevSampAggregate,
			"Calculates the standard deviation of the selected values."),
	),

	"xor_agg": makeBuiltin(aggProps(),
		makeAggOverload([]types.T{types.Bytes}, types.Bytes, newBytesXorAggregate,
			"Calculates the bitwise XOR of the selected values."),
		makeAggOverload([]types.T{types.Int}, types.Int, newIntXorAggregate,
			"Calculates the bitwise XOR of the selected values."),
	),

	"json_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]types.T{types.Any}, types.JSON, newJSONAggregate,
			"Aggregates values as a JSON or JSONB array."),
	),

	"jsonb_agg": makeBuiltin(aggPropsNullableArgs(),
		makeAggOverload([]types.T{types.Any}, types.JSON, newJSONAggregate,
			"Aggregates values as a JSON or JSONB array."),
	),

	"json_object_agg":  makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Class: tree.AggregateClass, Impure: true}),
	"jsonb_object_agg": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Class: tree.AggregateClass, Impure: true}),

	AnyNotNull: makePrivate(makeBuiltin(aggProps(),
		makeAggOverloadWithReturnType(
			[]types.T{types.Any},
			tree.IdentityReturnType(0),
			newAnyNotNullAggregate,
			"Returns an arbitrary not-NULL value, or NULL if none exists.",
		))),
}

// AnyNotNull is the name of the aggregate returned by NewAnyNotNullAggregate.
const AnyNotNull = "any_not_null"

func makePrivate(b builtinDefinition) builtinDefinition {
	b.props.Private = true
	return b
}

func makeAggOverload(
	in []types.T,
	ret types.T,
	f func([]types.T, *tree.EvalContext, tree.Datums, *mon.BoundAccount) tree.AggregateFunc,
	info string,
) tree.Overload {
	return makeAggOverloadWithReturnType(
		in,
		tree.FixedReturnType(ret),
		f,
		info,
	)
}

func makeAggOverloadWithReturnType(
	in []types.T,
	retType tree.ReturnTyper,
	f func([]types.T, *tree.EvalContext, tree.Datums, *mon.BoundAccount) tree.AggregateFunc,
	info string,
) tree.Overload {
	argTypes := make(tree.ArgTypes, len(in))
	for i, typ := range in {
		argTypes[i].Name = fmt.Sprintf("arg%d", i+1)
		argTypes[i].Typ = typ
	}

	return tree.Overload{
		// See the comment about aggregate functions in the definitions
		// of the Builtins array above.
		Types:         argTypes,
		ReturnType:    retType,
		AggregateFunc: f,
		WindowFunc: func(params []types.T, evalCtx *tree.EvalContext, account *mon.BoundAccount) tree.WindowFunc {
			aggWindowFunc := f(params, evalCtx, nil /* arguments */, account)
			switch w := aggWindowFunc.(type) {
			case *MinAggregate:
				min := &slidingWindowFunc{}
				min.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return -a.Compare(evalCtx, b)
				})
				return min
			case *MaxAggregate:
				max := &slidingWindowFunc{}
				max.sw = makeSlidingWindow(evalCtx, func(evalCtx *tree.EvalContext, a, b tree.Datum) int {
					return a.Compare(evalCtx, b)
				})
				return max
			case *IntSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *DecimalSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *FloatSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *IntervalSumAggregate:
				return newSlidingWindowSumFunc(aggWindowFunc)
			case *AvgAggregate:
				// w.agg is a sum aggregate.
				return &avgWindowFunc{sum: newSlidingWindowSumFunc(w.agg)}
			}

			return newFramableAggregateWindow(
				aggWindowFunc,
				func(evalCtx *tree.EvalContext, arguments tree.Datums, boundAccount *mon.BoundAccount) tree.AggregateFunc {
					return f(params, evalCtx, arguments, boundAccount)
				},
			)
		},
		Info: info,
	}
}

var _ tree.AggregateFunc = &ArrayAggregate{}
var _ tree.AggregateFunc = &AvgAggregate{}
var _ tree.AggregateFunc = &CountAggregate{}
var _ tree.AggregateFunc = &CountRowsAggregate{}
var _ tree.AggregateFunc = &FirstValueAggregate{}
var _ tree.AggregateFunc = &LastValueAggregate{}
var _ tree.AggregateFunc = &FloatStddevSampAggregate{}
var _ tree.AggregateFunc = &DecimalStddevSampAggregate{}
var _ tree.AggregateFunc = &MaxAggregate{}
var _ tree.AggregateFunc = &MinAggregate{}
var _ tree.AggregateFunc = &FloatMedianAggregate{}
var _ tree.AggregateFunc = &IntMedianAggregate{}
var _ tree.AggregateFunc = &SmallIntSumAggregate{}
var _ tree.AggregateFunc = &IntSumAggregate{}
var _ tree.AggregateFunc = &DecimalSumAggregate{}
var _ tree.AggregateFunc = &FloatSumAggregate{}
var _ tree.AggregateFunc = &IntervalSumAggregate{}
var _ tree.AggregateFunc = &IntSqrDiffAggregate{}
var _ tree.AggregateFunc = &FloatSqrDiffAggregate{}
var _ tree.AggregateFunc = &DecimalSqrDiffAggregate{}
var _ tree.AggregateFunc = &FloatSumSqrDiffsAggregate{}
var _ tree.AggregateFunc = &DecimalSumSqrDiffsAggregate{}
var _ tree.AggregateFunc = &FloatVarianceAggregate{}
var _ tree.AggregateFunc = &DecimalVarianceAggregate{}
var _ tree.AggregateFunc = &FloatStdDevAggregate{}
var _ tree.AggregateFunc = &DecimalStdDevAggregate{}
var _ tree.AggregateFunc = &AnyNotNullAggregate{}
var _ tree.AggregateFunc = &ConcatAggregate{}
var _ tree.AggregateFunc = &StatsModeAggregate{}
var _ tree.AggregateFunc = &AnyValueAggregate{}
var _ tree.AggregateFunc = &BoolAndAggregate{}
var _ tree.AggregateFunc = &BoolOrAggregate{}
var _ tree.AggregateFunc = &BytesXorAggregate{}
var _ tree.AggregateFunc = &IntXorAggregate{}
var _ tree.AggregateFunc = &JSONAggregate{}

const sizeOfArrayAggregate = int64(unsafe.Sizeof(ArrayAggregate{}))
const sizeOfAvgAggregate = int64(unsafe.Sizeof(AvgAggregate{}))
const sizeOfCountAggregate = int64(unsafe.Sizeof(CountAggregate{}))
const sizeOfCountRowsAggregate = int64(unsafe.Sizeof(CountRowsAggregate{}))
const sizeOfFirstValueAggregate = int64(unsafe.Sizeof(FirstValueAggregate{}))
const sizeOfLastValueAggregate = int64(unsafe.Sizeof(LastValueAggregate{}))
const sizeOfFloatStddevSampAggregate = int64(unsafe.Sizeof(FloatStddevSampAggregate{}))
const sizeOfDecimalStddevSampAggregate = int64(unsafe.Sizeof(DecimalStddevSampAggregate{}))
const sizeOfMaxAggregate = int64(unsafe.Sizeof(MaxAggregate{}))
const sizeOfMinAggregate = int64(unsafe.Sizeof(MinAggregate{}))
const sizeOfFloatMedianAggregate = int64(unsafe.Sizeof(FloatMedianAggregate{}))
const sizeOfIntMedianAggregate = int64(unsafe.Sizeof(IntMedianAggregate{}))
const sizeOfSmallIntSumAggregate = int64(unsafe.Sizeof(SmallIntSumAggregate{}))
const sizeOfIntSumAggregate = int64(unsafe.Sizeof(IntSumAggregate{}))
const sizeOfDecimalSumAggregate = int64(unsafe.Sizeof(DecimalSumAggregate{}))
const sizeOfFloatSumAggregate = int64(unsafe.Sizeof(FloatSumAggregate{}))
const sizeOfIntervalSumAggregate = int64(unsafe.Sizeof(IntervalSumAggregate{}))
const sizeOfIntSqrDiffAggregate = int64(unsafe.Sizeof(IntSqrDiffAggregate{}))
const sizeOfFloatSqrDiffAggregate = int64(unsafe.Sizeof(FloatSqrDiffAggregate{}))
const sizeOfDecimalSqrDiffAggregate = int64(unsafe.Sizeof(DecimalSqrDiffAggregate{}))
const sizeOfFloatSumSqrDiffsAggregate = int64(unsafe.Sizeof(FloatSumSqrDiffsAggregate{}))
const sizeOfDecimalSumSqrDiffsAggregate = int64(unsafe.Sizeof(DecimalSumSqrDiffsAggregate{}))
const sizeOfFloatVarianceAggregate = int64(unsafe.Sizeof(FloatVarianceAggregate{}))
const sizeOfDecimalVarianceAggregate = int64(unsafe.Sizeof(DecimalVarianceAggregate{}))
const sizeOfFloatStdDevAggregate = int64(unsafe.Sizeof(FloatStdDevAggregate{}))
const sizeOfDecimalStdDevAggregate = int64(unsafe.Sizeof(DecimalStdDevAggregate{}))
const sizeOfAnyNotNullAggregate = int64(unsafe.Sizeof(AnyNotNullAggregate{}))
const sizeOfConcatAggregate = int64(unsafe.Sizeof(ConcatAggregate{}))
const sizeOfStatsModeAggregate = int64(unsafe.Sizeof(StatsModeAggregate{}))
const sizeOfAnyValueAggregate = int64(unsafe.Sizeof(AnyValueAggregate{}))
const sizeOfBoolAndAggregate = int64(unsafe.Sizeof(BoolAndAggregate{}))
const sizeOfBoolOrAggregate = int64(unsafe.Sizeof(BoolOrAggregate{}))
const sizeOfBytesXorAggregate = int64(unsafe.Sizeof(BytesXorAggregate{}))
const sizeOfIntXorAggregate = int64(unsafe.Sizeof(IntXorAggregate{}))
const sizeOfJSONAggregate = int64(unsafe.Sizeof(JSONAggregate{}))

// SingleAggBase for each aggregator BoundAccount
type SingleAggBase struct {
	sharedAcc *mon.BoundAccount
}

//AnyNotNullAggregate See NewAnyNotNullAggregate.
type AnyNotNullAggregate struct {
	val tree.Datum
}

// NewAnyNotNullAggregate returns an aggregate function that returns an
// arbitrary not-NULL value passed to Add (or NULL if no such value). This is
// particularly useful for "passing through" values for columns which we know
// are constant within any aggregation group (for example, the grouping columns
// themselves).
//
// Note that NULL values do not affect the result of the aggregation; this is
// important in a few different contexts:
//
//  - in distributed multi-stage aggregations, we can have a local stage with
//    multiple (parallel) instances feeding into a final stage. If some of the
//    instances see no rows, they emit a NULL into the final stage which needs
//    to be ignored.
//
//  - for query optimization, when moving aggregations across left joins (which
//    add NULL values).
func NewAnyNotNullAggregate(*tree.EvalContext, tree.Datums, *mon.BoundAccount) tree.AggregateFunc {
	return &AnyNotNullAggregate{val: tree.DNull}
}

func newAnyNotNullAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &AnyNotNullAggregate{val: tree.DNull}
}

// Add sets the value to the passed datum.
func (a *AnyNotNullAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if a.val == tree.DNull && datum != tree.DNull {
		a.val = datum
	}
	return nil
}

// OptAdd sets the value to the passed datum.
func (a *AnyNotNullAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*AnyNotNullAggregate)
	if a.val == tree.DNull && t.val != tree.DNull {
		a.val = t.val
	}
	return nil
}

// Result returns the value most recently passed to Add.
func (a *AnyNotNullAggregate) Result() (tree.Datum, error) {
	return a.val, nil
}

// Close is no-op in aggregates using constant space.
func (a *AnyNotNullAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *AnyNotNullAggregate) Size() int64 {
	return sizeOfAnyNotNullAggregate
}

//ArrayAggregate is for ArrayAggregate
type ArrayAggregate struct {
	SingleAggBase
	arr *tree.DArray
	acc mon.BoundAccount
}

func newArrayAggregate(
	params []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	if account != nil {
		return &ArrayAggregate{
			arr: tree.NewDArray(params[0]),
			acc: evalCtx.Mon.MakeBoundAccount(),
			SingleAggBase: SingleAggBase{
				sharedAcc: account,
			},
		}
	}
	return &ArrayAggregate{
		arr: tree.NewDArray(params[0]),
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
}

// Add accumulates the passed datum into the array.
func (a *ArrayAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if a.sharedAcc == nil {
		if err := a.acc.Grow(ctx, int64(datum.Size())); err != nil {
			return err
		}
	} else {
		if err := a.sharedAcc.Grow(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return a.arr.Append(datum)
}

// OptAdd accumulates the passed datum into the array.
func (a *ArrayAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*ArrayAggregate)
	if err := a.acc.Grow(ctx, int64(t.arr.Size())); err != nil {
		return err
	}
	return a.arr.Append(t.arr)
}

// Result returns a copy of the array of all datums passed to Add.
func (a *ArrayAggregate) Result() (tree.Datum, error) {
	if len(a.arr.Array) > 0 {
		arrCopy := *a.arr
		return &arrCopy, nil
	}
	return tree.DNull, nil
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *ArrayAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *ArrayAggregate) Size() int64 {
	return sizeOfArrayAggregate
}

//AvgAggregate is for AvgAggregate
type AvgAggregate struct {
	agg   tree.AggregateFunc
	count int
}

func newIntAvgAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &AvgAggregate{agg: newIntSumAggregate(params, evalCtx, arguments, account)}
}
func newFloatAvgAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &AvgAggregate{agg: newFloatSumAggregate(params, evalCtx, arguments, account)}
}
func newDecimalAvgAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &AvgAggregate{agg: newDecimalSumAggregate(params, evalCtx, arguments, account)}
}

// Add accumulates the passed datum into the average.
func (a *AvgAggregate) Add(ctx context.Context, datum tree.Datum, other ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if err := a.agg.Add(ctx, datum); err != nil {
		return err
	}
	a.count++
	return nil
}

// OptAdd accumulates the passed datum into the average.
func (a *AvgAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	b := b2.(*AvgAggregate)
	if err := a.agg.OptAdd(ctx, b.agg); err != nil {
		return err
	}
	a.count = a.count + b.count
	return nil
}

// OptAdd accumulates the passed datum into the intsum.
func (a *IntSumAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	b := b2.(*IntSumAggregate)
	if b.intSum != 0 {
		// The sum can be computed using a single int64 as long as the
		// result of the addition does not overflow.  However since Go
		// does not provide checked addition, we have to check for the
		// overflow explicitly.
		if !a.large {
			r, ok := arith.AddWithOverflow(a.intSum, b.intSum)
			if ok {
				a.intSum = r
			} else {
				// And overflow was detected; go to large integers, but keep the
				// sum computed so far.
				a.large = true
				a.decSum.SetFinite(a.intSum, 0)
			}
		}

		if a.large {
			if !b.large {
				a.tmpDec.SetFinite(b.intSum, 0)
			} else {
				a.tmpDec = b.decSum
			}
			_, err := tree.ExactCtx.Add(&a.decSum, &a.decSum, &a.tmpDec)
			if err != nil {
				return err
			}
			if a.sharedAcc == nil {
				if err := a.acc.ResizeTo(ctx, int64(tree.SizeOfDecimal(&a.decSum))); err != nil {
					return err
				}
			} else {
				if err := a.sharedAcc.ResizeTo(ctx, int64(tree.SizeOfDecimal(&a.decSum))); err != nil {
					return err
				}
			}
		}
	}
	a.seenNonNull = true
	return nil
}

// OptAdd accumulates the passed datum into the count.
func (a *CountAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	b := b2.(*CountAggregate)
	a.count = a.count + b.count
	return nil
}

//Result returns the average of all datums passed to Add.
func (a *AvgAggregate) Result() (tree.Datum, error) {
	sum, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if sum == tree.DNull {
		return sum, nil
	}
	switch t := sum.(type) {
	case *tree.DFloat:
		return tree.NewDFloat(*t / tree.DFloat(a.count)), nil
	case *tree.DDecimal:
		count := apd.New(int64(a.count), 0)
		_, err := tree.DecimalCtx.Quo(&t.Decimal, &t.Decimal, count)
		return t, err
	default:
		return nil, pgerror.NewAssertionErrorf("unexpected SUM result type: %s", t)
	}
}

// Close is part of the tree.AggregateFunc interface.
func (a *AvgAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *AvgAggregate) Size() int64 {
	return sizeOfAvgAggregate
}

//ConcatAggregate is for ConcatAggregate
type ConcatAggregate struct {
	SingleAggBase
	forBytes   bool
	sawNonNull bool
	delimiter  string // used for non window functions
	result     bytes.Buffer
	acc        mon.BoundAccount
}

func newBytesConcatAggregate(
	_ []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	concatAgg := &ConcatAggregate{
		forBytes: true,
	}
	if account != nil {
		concatAgg.sharedAcc = account
	} else {
		concatAgg.acc = evalCtx.Mon.MakeBoundAccount()
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDBytes(arguments[0]))
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func newStringConcatAggregate(
	_ []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	concatAgg := &ConcatAggregate{}
	if account != nil {
		concatAgg.sharedAcc = account
	} else {
		concatAgg.acc = evalCtx.Mon.MakeBoundAccount()
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDString(arguments[0]))
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func newStringGroupConcatAggregate(
	_ []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	concatAgg := &ConcatAggregate{
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDString(arguments[0]))
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func newBytesGroupConcatAggregate(
	_ []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	concatAgg := &ConcatAggregate{
		forBytes: true,
		acc:      evalCtx.Mon.MakeBoundAccount(),
	}
	if len(arguments) == 1 && arguments[0] != tree.DNull {
		concatAgg.delimiter = string(tree.MustBeDBytes(arguments[0]))
	} else if len(arguments) > 1 {
		panic(fmt.Sprintf("too many arguments passed in, expected < 2, got %d", len(arguments)))
	}
	return concatAgg
}

func newBytesGroupConcat(
	_ []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	concatAgg := &ConcatAggregate{
		forBytes: true,
	}
	if account != nil {
		concatAgg.sharedAcc = account
	} else {
		concatAgg.acc = evalCtx.Mon.MakeBoundAccount()
	}
	concatAgg.delimiter = ","
	return concatAgg
}

func newStringGroupConcat(
	_ []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	concatAgg := &ConcatAggregate{}
	if account != nil {
		concatAgg.sharedAcc = account
	} else {
		concatAgg.acc = evalCtx.Mon.MakeBoundAccount()
	}
	concatAgg.delimiter = ","
	return concatAgg
}

// Add accumulates the passed datum into the ConcatAggregate.
func (a *ConcatAggregate) Add(ctx context.Context, datum tree.Datum, others ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		a.sawNonNull = true
	} else {
		delimiter := a.delimiter
		// If this is called as part of a window function, the delimiter is passed in
		// via the first element in others.
		if len(others) == 1 && others[0] != tree.DNull {
			if a.forBytes {
				delimiter = string(tree.MustBeDBytes(others[0]))
			} else {
				delimiter = string(tree.MustBeDString(others[0]))
			}
		} else if len(others) > 1 {
			panic(fmt.Sprintf("too many other datums passed in, expected < 2, got %d", len(others)))
		}
		if len(delimiter) > 0 {
			if a.sharedAcc == nil {
				if err := a.acc.Grow(ctx, int64(len(delimiter))); err != nil {
					return err
				}
			} else {
				if err := a.sharedAcc.Grow(ctx, int64(len(delimiter))); err != nil {
					return err
				}
			}
			a.result.WriteString(delimiter)
		}
	}
	var arg string
	if a.forBytes {
		arg = string(tree.MustBeDBytes(datum))
	} else {
		arg = string(tree.MustBeDString(datum))
	}
	if a.sharedAcc == nil {
		if err := a.acc.Grow(ctx, int64(len(arg))); err != nil {
			return err
		}
	} else {
		if err := a.sharedAcc.Grow(ctx, int64(len(arg))); err != nil {
			return err
		}
	}
	a.result.WriteString(arg)
	return nil
}

//OptAdd accumulates the passed datum into the ConcatAggregate.
func (a *ConcatAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*ConcatAggregate)

	arg := tree.DString(t.result.String())
	if a.sharedAcc != nil {
		if err := a.sharedAcc.Grow(ctx, int64(len(arg))); err != nil {
			return err
		}
	} else {
		if err := a.acc.Grow(ctx, int64(len(arg))); err != nil {
			return err
		}
	}
	a.result.WriteString(string(arg))
	return nil
}

//Result returns the Concat of all datums passed to Concat.
func (a *ConcatAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	if a.forBytes {
		res := tree.DBytes(a.result.String())
		return &res, nil
	}
	res := tree.DString(a.result.String())
	return &res, nil
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *ConcatAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *ConcatAggregate) Size() int64 {
	return sizeOfConcatAggregate
}

//StatsModeAggregate is for StatsModeAggregate
type StatsModeAggregate struct {
	forBytes   bool
	sawNonNull bool
	delimiter  string // used for non window functions
	dataType   bytes.Buffer
	result     bytes.Buffer
	acc        mon.BoundAccount
}

// OptAdd for agg opt
func (a *StatsModeAggregate) OptAdd(_ context.Context, _ interface{}) error {
	panic("implement me")
}

func newStatsModeAggregate(
	_ []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, acc *mon.BoundAccount,
) tree.AggregateFunc {
	concatAgg := &StatsModeAggregate{
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
	concatAgg.delimiter = "{}="
	return concatAgg
}

// Add accumulates the passed datum into the StatsModeAggregate.
func (a *StatsModeAggregate) Add(
	ctx context.Context, datum tree.Datum, others ...tree.Datum,
) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		a.sawNonNull = true
	} else {
		delimiter := a.delimiter
		// If this is called as part of a window function, the delimiter is passed in
		// via the first element in others.
		if len(others) == 1 && others[0] != tree.DNull {
			if a.forBytes {
				delimiter = string(tree.MustBeDBytes(others[0]))
			} else {
				delimiter = string(tree.MustBeDString(others[0]))
			}
		} else if len(others) > 1 {
			panic(fmt.Sprintf("too many other datums passed in, expected < 2, got %d", len(others)))
		}
		if len(delimiter) > 0 {
			if err := a.acc.Grow(ctx, int64(len(delimiter))); err != nil {
				return err
			}
			a.result.WriteString(delimiter)
		}
	}
	dataTypelen := len(a.dataType.String())
	if dataTypelen < 3 {
		switch datum.ResolvedType() {
		case types.Int:
			a.dataType.WriteString("INT")
		case types.Float:
			a.dataType.WriteString("FLOAT")
		case types.Decimal:
			a.dataType.WriteString("DECIMAL")
		default:
			a.dataType.WriteString("STRING")
		}
	}
	var arg string
	if a.forBytes {
		arg = string(tree.MustBeDBytes(datum))
	} else {
		switch a.dataType.String() {
		case "INT":
			intArg := int(tree.MustBeDInt(datum))
			arg = strconv.Itoa(intArg)
		case "FLOAT":
			floatArg := float64(tree.MustBeDFloat(datum))
			arg = strconv.FormatFloat(floatArg, 'f', -1, 64)
		case "DECIMAL":
			floatNum, _ := datum.(*tree.DDecimal).Float64()
			arg = strconv.FormatFloat(floatNum, 'f', -1, 64)
		default:
			arg = string(tree.MustBeDString(datum))
		}
	}
	if err := a.acc.Grow(ctx, int64(len(arg))); err != nil {
		return err
	}
	a.result.WriteString(arg)
	return nil
}

//Result returns the value that occurs with the greatest frequency.
func (a *StatsModeAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	if a.forBytes {
		res := tree.DBytes(a.result.String())
		return &res, nil
	}

	resStrArr := strings.Split(a.result.String(), "{}=")
	numMap := make(map[string]int)
	newStrArr := make([]string, 0)
	for i := 0; i < len(resStrArr); i++ {
		repeat := false
		for j := i + 1; j < len(resStrArr); j++ {
			if resStrArr[i] == resStrArr[j] {
				repeat = true
				numMap[resStrArr[j]]++
				break
			}
		}
		if !repeat {
			newStrArr = append(newStrArr, resStrArr[i])
			numMap[resStrArr[i]]++
		}
	}
	var max int
	max = numMap[resStrArr[0]]
	var statsRes string
	statsRes = resStrArr[0]

	changeKey := true
	for i, v := range numMap {
		if v > max && i != statsRes && changeKey {
			max = v
			statsRes = i
			changeKey = false
		}
	}

	switch a.dataType.String() {
	case "INT":
		intNum, _ := strconv.Atoi(statsRes)
		res := tree.DInt(intNum)
		return &res, nil
	case "FLOAT":
		floatNum, _ := strconv.ParseFloat(statsRes, 64)
		res := tree.DFloat(floatNum)
		return &res, nil
	case "DECIMAL":
		res, _ := tree.ParseDDecimal(statsRes)
		return res, nil
	case "STRING":
		res := tree.DString(statsRes)
		return &res, nil
	}
	return nil, fmt.Errorf(" The argument should be of float, decimal, string or int type")
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *StatsModeAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *StatsModeAggregate) Size() int64 {
	return sizeOfStatsModeAggregate
}

// AnyValueAggregate keeps track of the first value of an expression in a group passed to Add.
type AnyValueAggregate struct {
	any     tree.Datum
	evalCtx *tree.EvalContext

	acc               mon.BoundAccount
	datumSize         uintptr
	variableDatumSize bool

	firstTag bool
}

// OptAdd for agg opt
func (a *AnyValueAggregate) OptAdd(_ context.Context, _ interface{}) error {
	panic("implement me")
}

func newAnyValueAggregate(
	params []types.T, evalCtx *tree.EvalContext, _ tree.Datums, acc *mon.BoundAccount,
) tree.AggregateFunc {
	sz, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a fixed size, it will be included in the size
	// reported by Size(). Otherwise it will be accounted for in Add(). This
	// avoids doing unnecessary memory accounting work for fixed-size datums.
	if variable {
		sz = 0
	}
	return &AnyValueAggregate{
		evalCtx:           evalCtx,
		acc:               evalCtx.Mon.MakeBoundAccount(),
		datumSize:         sz,
		variableDatumSize: variable,
		firstTag:          true,
	}
}

// Add sets first value of an expression in a group.
func (a *AnyValueAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if a.firstTag {
		a.any = datum
		a.firstTag = false
	}

	if a.variableDatumSize {
		if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the first value  of an expression in a group passed to Add.
func (a *AnyValueAggregate) Result() (tree.Datum, error) {
	if a.any == nil {
		return tree.DNull, nil
	}
	return a.any, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *AnyValueAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *AnyValueAggregate) Size() int64 {
	return sizeOfAnyValueAggregate + int64(a.datumSize)
}

//BoolAndAggregate is a agg func
type BoolAndAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolAndAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &BoolAndAggregate{}
}

//Add accumulates the passed datum into the BoolAndAggregate.
func (a *BoolAndAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if !a.sawNonNull {
		a.sawNonNull = true
		a.result = true
	}
	a.result = a.result && bool(*datum.(*tree.DBool))
	return nil
}

//OptAdd accumulates the passed datum into the BoolAndAggregate.
func (a *BoolAndAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*BoolAndAggregate)
	if !a.sawNonNull {
		a.sawNonNull = true
		a.result = true
	}
	a.result = a.result && t.result
	return nil
}

//Result returns all datums passed to Concat.
func (a *BoolAndAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.MakeDBool(tree.DBool(a.result)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *BoolAndAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *BoolAndAggregate) Size() int64 {
	return sizeOfBoolAndAggregate
}

//BoolOrAggregate is a agg func
type BoolOrAggregate struct {
	sawNonNull bool
	result     bool
}

func newBoolOrAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &BoolOrAggregate{}
}

//Add accumulates the passed datum into the BoolOrAggregate.
func (a *BoolOrAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.sawNonNull = true
	a.result = a.result || bool(*datum.(*tree.DBool))
	return nil
}

//OptAdd accumulates the passed datum into the BoolOrAggregate.
func (a *BoolOrAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*BoolOrAggregate)
	a.sawNonNull = true
	a.result = a.result || t.result
	return nil
}

//Result returns all datums passed to Result.
func (a *BoolOrAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.MakeDBool(tree.DBool(a.result)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *BoolOrAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *BoolOrAggregate) Size() int64 {
	return sizeOfBoolOrAggregate
}

//CountAggregate is a agg func
type CountAggregate struct {
	count int
}

func newCountAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &CountAggregate{}
}

//Add accumulates the passed datum into the CountAggregate.
func (a *CountAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	a.count++
	return nil
}

// Result returns all datums passed to CountAggregate.
func (a *CountAggregate) Result() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(a.count)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *CountAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *CountAggregate) Size() int64 {
	return sizeOfCountAggregate
}

//CountRowsAggregate is a agg func
type CountRowsAggregate struct {
	count int
}

func newCountRowsAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &CountRowsAggregate{}
}

//Add accumulates the passed datum into the CountRowsAggregate.
func (a *CountRowsAggregate) Add(_ context.Context, _ tree.Datum, _ ...tree.Datum) error {
	a.count++
	return nil
}

//OptAdd accumulates the passed datum into the CountRowsAggregate.
func (a *CountRowsAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*CountRowsAggregate)
	a.count = a.count + t.count
	return nil
}

//Result returns all datums passed to CountRowsAggregate.
func (a *CountRowsAggregate) Result() (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(a.count)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *CountRowsAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *CountRowsAggregate) Size() int64 {
	return sizeOfCountRowsAggregate
}

// FirstValueAggregate keeps track of the first value passed to Add.
type FirstValueAggregate struct {
	first   tree.Datum
	evalCtx *tree.EvalContext

	acc               mon.BoundAccount
	datumSize         uintptr
	variableDatumSize bool

	firstTag bool
}

// OptAdd for hashagg opt
func (a *FirstValueAggregate) OptAdd(_ context.Context, _ interface{}) error {
	panic("implement me")
}

func newFirstValueAggregate(
	params []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	sz, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a fixed size, it will be included in the size
	// reported by Size(). Otherwise it will be accounted for in Add(). This
	// avoids doing unnecessary memory accounting work for fixed-size datums.
	if variable {
		sz = 0
	}
	return &FirstValueAggregate{
		evalCtx:           evalCtx,
		acc:               evalCtx.Mon.MakeBoundAccount(),
		datumSize:         sz,
		variableDatumSize: variable,
		firstTag:          true,
	}
}

// Add sets the first to the larger of the current first or the passed datum.
func (a *FirstValueAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if a.firstTag {
		a.first = datum
		a.firstTag = false
	}

	if a.variableDatumSize {
		if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the first value passed to Add.
func (a *FirstValueAggregate) Result() (tree.Datum, error) {
	if a.first == nil {
		return tree.DNull, nil
	}
	return a.first, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *FirstValueAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *FirstValueAggregate) Size() int64 {
	return sizeOfFirstValueAggregate + int64(a.datumSize)
}

// LastValueAggregate keeps track of the last value passed to Add.
type LastValueAggregate struct {
	last    tree.Datum
	evalCtx *tree.EvalContext

	acc               mon.BoundAccount
	datumSize         uintptr
	variableDatumSize bool
}

// OptAdd for hashagg opt
func (a *LastValueAggregate) OptAdd(_ context.Context, _ interface{}) error {
	panic("implement me")
}

func newLastValueAggregate(
	params []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	sz, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a fixed size, it will be included in the size
	// reported by Size(). Otherwise it will be accounted for in Add(). This
	// avoids doing unnecessary memory accounting work for fixed-size datums.
	if variable {
		sz = 0
	}
	return &LastValueAggregate{
		evalCtx:           evalCtx,
		acc:               evalCtx.Mon.MakeBoundAccount(),
		datumSize:         sz,
		variableDatumSize: variable,
	}
}

// Add sets the last to the larger of the current last or the passed datum.
func (a *LastValueAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	a.last = datum

	if a.variableDatumSize {
		if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
			return err
		}
	}
	return nil
}

// Result returns the last value passed to Add.
func (a *LastValueAggregate) Result() (tree.Datum, error) {
	if a.last == nil {
		return tree.DNull, nil
	}
	return a.last, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *LastValueAggregate) Close(ctx context.Context) {
	a.acc.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *LastValueAggregate) Size() int64 {
	return sizeOfLastValueAggregate + int64(a.datumSize)
}

// MaxAggregate keeps track of the largest value passed to Add.
type MaxAggregate struct {
	SingleAggBase
	max     tree.Datum
	evalCtx *tree.EvalContext

	acc               mon.BoundAccount
	datumSize         uintptr
	variableDatumSize bool
}

func newMaxAggregate(
	params []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	sz, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a fixed size, it will be included in the size
	// reported by Size(). Otherwise it will be accounted for in Add(). This
	// avoids doing unnecessary memory accounting work for fixed-size datums.
	if variable {
		sz = 0
	}
	if account != nil {
		return &MaxAggregate{
			evalCtx:           evalCtx,
			datumSize:         sz,
			variableDatumSize: variable,
			SingleAggBase: SingleAggBase{
				sharedAcc: account,
			},
		}
	}
	return &MaxAggregate{
		evalCtx:           evalCtx,
		acc:               evalCtx.Mon.MakeBoundAccount(),
		datumSize:         sz,
		variableDatumSize: variable,
	}
}

// Add sets the max to the larger of the current max or the passed datum.
func (a *MaxAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.max == nil {
		a.max = datum
		return nil
	}
	c := a.max.Compare(a.evalCtx, datum)
	if c < 0 {
		a.max = datum
		if a.variableDatumSize {
			if a.sharedAcc == nil {
				if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
					return err
				}
			} else {
				if err := a.sharedAcc.ResizeTo(ctx, int64(datum.Size())); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// OptAdd sets the max to the larger of the current max or the passed datum.
func (a *MaxAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*MaxAggregate)
	if t.max == tree.DNull {
		return nil
	}
	if a.max == nil {
		a.max = t.max
		return nil
	}
	c := a.max.Compare(a.evalCtx, t.max)
	if c < 0 {
		a.max = t.max
		if a.variableDatumSize {
			if a.sharedAcc != nil {
				if err := a.sharedAcc.ResizeTo(ctx, int64(t.max.Size())); err != nil {
					return err
				}
			} else {
				if err := a.acc.ResizeTo(ctx, int64(t.max.Size())); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Result returns the largest value passed to Add.
func (a *MaxAggregate) Result() (tree.Datum, error) {
	if a.max == nil {
		return tree.DNull, nil
	}
	return a.max, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *MaxAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *MaxAggregate) Size() int64 {
	return sizeOfMaxAggregate + int64(a.datumSize)
}

// MinAggregate keeps track of the smallest value passed to Add.
type MinAggregate struct {
	SingleAggBase
	min     tree.Datum
	evalCtx *tree.EvalContext

	acc               mon.BoundAccount
	datumSize         uintptr
	variableDatumSize bool
}

func newMinAggregate(
	params []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	sz, variable := tree.DatumTypeSize(params[0])
	// If the datum type has a fixed size, it will be included in the size
	// reported by Size(). Otherwise it will be accounted for in Add(). This
	// avoids doing unnecessary memory accounting work for fixed-size datums.
	if variable {
		// Datum size will be accounted for in the Add method.
		sz = 0
	}
	if account != nil {
		return &MinAggregate{
			evalCtx:           evalCtx,
			datumSize:         sz,
			variableDatumSize: variable,
			SingleAggBase: SingleAggBase{
				sharedAcc: account,
			},
		}
	}
	return &MinAggregate{
		evalCtx:           evalCtx,
		acc:               evalCtx.Mon.MakeBoundAccount(),
		datumSize:         sz,
		variableDatumSize: variable,
	}
}

// Add sets the min to the smaller of the current min or the passed datum.
func (a *MinAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if a.min == nil {
		a.min = datum
		return nil
	}
	c := a.min.Compare(a.evalCtx, datum)
	if c > 0 {
		a.min = datum
		if a.variableDatumSize {
			if a.sharedAcc == nil {
				if err := a.acc.ResizeTo(ctx, int64(datum.Size())); err != nil {
					return err
				}
			} else {
				if err := a.sharedAcc.ResizeTo(ctx, int64(datum.Size())); err != nil {
					return err
				}
			}

		}
	}
	return nil
}

// OptAdd sets the min to the smaller of the current min or the passed datum.
func (a *MinAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*MinAggregate)
	if t.min == tree.DNull {
		return nil
	}
	if a.min == nil {
		a.min = t.min
		return nil
	}
	c := a.min.Compare(a.evalCtx, t.min)
	if c > 0 {
		a.min = t.min
		if a.variableDatumSize {
			if a.sharedAcc != nil {
				if err := a.sharedAcc.ResizeTo(ctx, int64(t.min.Size())); err != nil {
					return err
				}
			} else {
				if err := a.acc.ResizeTo(ctx, int64(t.min.Size())); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Result returns the smallest value passed to Add.
func (a *MinAggregate) Result() (tree.Datum, error) {
	if a.min == nil {
		return tree.DNull, nil
	}
	return a.min, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *MinAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *MinAggregate) Size() int64 {
	return sizeOfMinAggregate + int64(a.datumSize)
}

//FloatMedianAggregate is a agg func
type FloatMedianAggregate struct {
	med float64
	arg []float64
}

func newFloatMedianAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &FloatMedianAggregate{}
}

//Add adds the value of the passed datum to the arg.
func (a *FloatMedianAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DFloat)
	a.arg = append(a.arg, float64(*t))
	return nil

}

//OptAdd is part of the tree.AggregateFunc interface.
func (a *FloatMedianAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	return nil
}

// Result returns the med.
func (a *FloatMedianAggregate) Result() (tree.Datum, error) {
	if len(a.arg) == 0 {
		return tree.DNull, nil
	}
	length := len(a.arg)
	for i := 0; i < length-1; i++ {
		//sort:min to max
		for j := i + 1; j < length; j++ {
			if a.arg[j] < a.arg[i] {
				a.arg[i], a.arg[j] = a.arg[j], a.arg[i]
			}
		}
	}
	if length%2 == 0 {
		x := a.arg[(length/2)-1]
		y := a.arg[(length / 2)]
		a.med = ((x + y) / 2)
	} else {
		n := (length - 1) / 2
		a.med = (a.arg[n])
	}
	return tree.NewDFloat(tree.DFloat(a.med)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *FloatMedianAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *FloatMedianAggregate) Size() int64 {
	return sizeOfFloatMedianAggregate
}

//IntMedianAggregate is a agg func
type IntMedianAggregate struct {
	med float64
	arg []float64
}

func newIntMedianAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &IntMedianAggregate{}
}

//Add adds the value of the passed datum to the arg.
func (a *IntMedianAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DInt)
	a.arg = append(a.arg, float64(*t))
	return nil
}

//OptAdd is part of the tree.AggregateFunc interface.
func (a *IntMedianAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	return nil
}

// Result returns the med.
func (a *IntMedianAggregate) Result() (tree.Datum, error) {
	if len(a.arg) == 0 {
		return tree.DNull, nil
	}
	length := len(a.arg)
	for i := 0; i < length-1; i++ {
		//sort:from min to max
		for j := i + 1; j < length; j++ {
			if a.arg[j] < a.arg[i] {
				a.arg[i], a.arg[j] = a.arg[j], a.arg[i]
			}
		}
	}
	if length%2 == 0 {
		x := a.arg[(length/2)-1]
		y := a.arg[(length / 2)]
		a.med = ((x + y) / 2)
	} else {
		n := (length - 1) / 2
		a.med = (a.arg[n])
	}
	return tree.NewDFloat(tree.DFloat(a.med)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *IntMedianAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *IntMedianAggregate) Size() int64 {
	return sizeOfIntMedianAggregate
}

//SmallIntSumAggregate is a agg func
type SmallIntSumAggregate struct {
	sum         int64
	seenNonNull bool
}

func newSmallIntSumAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &SmallIntSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *SmallIntSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	a.sum += int64(tree.MustBeDInt(datum))
	a.seenNonNull = true
	return nil
}

// OptAdd adds the value of the passed datum to the sum.
func (a *SmallIntSumAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*SmallIntSumAggregate)
	if t.sum == 0 {
		return nil
	}

	a.sum += t.sum
	a.seenNonNull = true
	return nil
}

// Result returns the sum.
func (a *SmallIntSumAggregate) Result() (tree.Datum, error) {
	if !a.seenNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.sum)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *SmallIntSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *SmallIntSumAggregate) Size() int64 {
	return sizeOfSmallIntSumAggregate
}

//IntSumAggregate is a agg func
type IntSumAggregate struct {
	// Either the `intSum` and `decSum` fields contains the
	// result. Which one is used is determined by the `large` field
	// below.
	SingleAggBase
	intSum      int64
	decSum      apd.Decimal
	tmpDec      apd.Decimal
	large       bool
	seenNonNull bool
	acc         mon.BoundAccount
}

func newIntSumAggregate(
	_ []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	if account != nil {
		return &IntSumAggregate{
			SingleAggBase: SingleAggBase{
				sharedAcc: account,
			},
		}
	}
	return &IntSumAggregate{acc: evalCtx.Mon.MakeBoundAccount()}
}

// Add adds the value of the passed datum to the sum.
func (a *IntSumAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	t := int64(tree.MustBeDInt(datum))
	if t != 0 {
		// The sum can be computed using a single int64 as long as the
		// result of the addition does not overflow.  However since Go
		// does not provide checked addition, we have to check for the
		// overflow explicitly.
		if !a.large {
			r, ok := arith.AddWithOverflow(a.intSum, t)
			if ok {
				a.intSum = r
			} else {
				// And overflow was detected; go to large integers, but keep the
				// sum computed so far.
				a.large = true
				a.decSum.SetFinite(a.intSum, 0)
			}
		}

		if a.large {
			a.tmpDec.SetFinite(t, 0)
			_, err := tree.ExactCtx.Add(&a.decSum, &a.decSum, &a.tmpDec)
			if err != nil {
				return err
			}
			if a.sharedAcc == nil {
				if err := a.acc.ResizeTo(ctx, int64(tree.SizeOfDecimal(&a.decSum))); err != nil {
					return err
				}
			} else {
				if err := a.sharedAcc.ResizeTo(ctx, int64(tree.SizeOfDecimal(&a.decSum))); err != nil {
					return err
				}
			}

		}
	}
	a.seenNonNull = true
	return nil
}

// Result returns the sum.
func (a *IntSumAggregate) Result() (tree.Datum, error) {
	if !a.seenNonNull {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{}
	if a.large {
		dd.Set(&a.decSum)
	} else {
		dd.SetFinite(a.intSum, 0)
	}
	return dd, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *IntSumAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *IntSumAggregate) Size() int64 {
	return sizeOfIntSumAggregate
}

//DecimalSumAggregate is a agg func
type DecimalSumAggregate struct {
	SingleAggBase
	sum        apd.Decimal
	sawNonNull bool
	acc        mon.BoundAccount
}

func newDecimalSumAggregate(
	_ []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	if account != nil {
		return &DecimalSumAggregate{
			SingleAggBase: SingleAggBase{
				sharedAcc: account,
			},
		}
	}
	return &DecimalSumAggregate{acc: evalCtx.Mon.MakeBoundAccount()}
}

// Add adds the value of the passed datum to the sum.
func (a *DecimalSumAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DDecimal)
	_, err := tree.ExactCtx.Add(&a.sum, &a.sum, &t.Decimal)
	if err != nil {
		return err
	}
	if a.sharedAcc == nil {
		if err := a.acc.ResizeTo(ctx, int64(tree.SizeOfDecimal(&a.sum))); err != nil {
			return err
		}
	} else {
		if err := a.sharedAcc.ResizeTo(ctx, int64(tree.SizeOfDecimal(&a.sum))); err != nil {
			return err
		}
	}

	a.sawNonNull = true
	return nil
}

// OptAdd adds the value of the passed datum to the sum.
func (a *DecimalSumAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*DecimalSumAggregate)
	_, err := tree.ExactCtx.Add(&a.sum, &a.sum, &t.sum)
	if err != nil {
		return err
	}
	if a.sharedAcc != nil {
		if err := a.sharedAcc.ResizeTo(ctx, int64(tree.SizeOfDecimal(&a.sum))); err != nil {
			return err
		}
	} else {
		if err := a.acc.ResizeTo(ctx, int64(tree.SizeOfDecimal(&a.sum))); err != nil {
			return err
		}
	}

	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *DecimalSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{}
	dd.Set(&a.sum)
	return dd, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *DecimalSumAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *DecimalSumAggregate) Size() int64 {
	return sizeOfDecimalSumAggregate
}

//FloatSumAggregate is a agg func
type FloatSumAggregate struct {
	sum        float64
	sawNonNull bool
}

func newFloatSumAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &FloatSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *FloatSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DFloat)
	a.sum += float64(*t)
	a.sawNonNull = true
	return nil
}

// OptAdd adds the value of the passed datum to the sum.
func (a *FloatSumAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*FloatSumAggregate)
	if t.sum == 0 {
		return nil
	}
	a.sum += t.sum
	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *FloatSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sum)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *FloatSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *FloatSumAggregate) Size() int64 {
	return sizeOfFloatSumAggregate
}

//IntervalSumAggregate is a agg func
type IntervalSumAggregate struct {
	sum        duration.Duration
	sawNonNull bool
}

func newIntervalSumAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &IntervalSumAggregate{}
}

// Add adds the value of the passed datum to the sum.
func (a *IntervalSumAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := datum.(*tree.DInterval).Duration
	a.sum = a.sum.Add(t)
	a.sawNonNull = true
	return nil
}

// OptAdd adds the value of the passed datum to the sum.
func (a *IntervalSumAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*IntervalSumAggregate)
	a.sum = a.sum.Add(t.sum)
	a.sawNonNull = true
	return nil
}

// Result returns the sum.
func (a *IntervalSumAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return &tree.DInterval{Duration: a.sum}, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *IntervalSumAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *IntervalSumAggregate) Size() int64 {
	return sizeOfIntervalSumAggregate
}

// Read-only constants used for square difference computations.
var (
	decimalOne = apd.New(1, 0)
	decimalTwo = apd.New(2, 0)
)

//IntSqrDiffAggregate is a agg func
type IntSqrDiffAggregate struct {
	agg decimalSqrDiff
	// Used for passing int64s as *apd.Decimal values.
	tmpDec tree.DDecimal
}

func newIntSqrDiff(evalCtx *tree.EvalContext, account *mon.BoundAccount) decimalSqrDiff {
	return &IntSqrDiffAggregate{agg: newDecimalSqrDiff(evalCtx, account)}
}

func newIntSqrDiffAggregate(
	_ []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return newIntSqrDiff(evalCtx, account)
}

// Count is part of the decimalSqrDiff interface.
func (a *IntSqrDiffAggregate) Count() *apd.Decimal {
	return a.agg.Count()
}

// Tmp is part of the decimalSqrDiff interface.
func (a *IntSqrDiffAggregate) Tmp() *apd.Decimal {
	return a.agg.Tmp()
}

//Add accumulates the passed datum into the IntSqrDiffAggregate.
func (a *IntSqrDiffAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}

	a.tmpDec.SetFinite(int64(tree.MustBeDInt(datum)), 0)
	return a.agg.Add(ctx, &a.tmpDec)
}

//OptAdd accumulates the passed datum into the IntSqrDiffAggregate.
func (a *IntSqrDiffAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*IntSqrDiffAggregate)
	a.tmpDec.SetFinite(int64(t.tmpDec.Coeff.Uint64()), 0)
	return a.agg.OptAdd(ctx, t.agg)
}

//Result returns all datums passed to IntSqrDiffAggregate.
func (a *IntSqrDiffAggregate) Result() (tree.Datum, error) {
	return a.agg.Result()
}

// Close is part of the tree.AggregateFunc interface.
func (a *IntSqrDiffAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *IntSqrDiffAggregate) Size() int64 {
	return sizeOfIntSqrDiffAggregate
}

//FloatSqrDiffAggregate is a agg func
type FloatSqrDiffAggregate struct {
	count   int64
	mean    float64
	sqrDiff float64
}

func newFloatSqrDiff() floatSqrDiff {
	return &FloatSqrDiffAggregate{}
}

func newFloatSqrDiffAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return newFloatSqrDiff()
}

// Count is part of the floatSqrDiff interface.
func (a *FloatSqrDiffAggregate) Count() int64 {
	return a.count
}

//Add accumulates the passed datum into the FloatSqrDiffAggregate.
func (a *FloatSqrDiffAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	f := float64(*datum.(*tree.DFloat))

	// Uses the Knuth/Welford method for accurately computing squared difference online in a
	// single pass. Refer to squared difference calculations
	// in http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count++
	delta := f - a.mean
	// We are converting an int64 number (with 63-bit precision)
	// to a float64 (with 52-bit precision), thus in the worst cases,
	// we may lose up to 11 bits of precision. This was deemed acceptable
	// considering that we are losing 11 bits on a 52+-bit operation and
	// that users dealing with floating points should be aware
	// of floating-point imprecision.
	a.mean += delta / float64(a.count)
	a.sqrDiff += delta * (f - a.mean)
	return nil
}

//OptAdd accumulates the passed datum into the FloatSqrDiffAggregate.
func (a *FloatSqrDiffAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*FloatSqrDiffAggregate)
	f := t.mean

	// Uses the Knuth/Welford method for accurately computing squared difference online in a
	// single pass. Refer to squared difference calculations
	// in http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count = a.count + t.count
	delta := f - a.mean
	// We are converting an int64 number (with 63-bit precision)
	// to a float64 (with 52-bit precision), thus in the worst cases,
	// we may lose up to 11 bits of precision. This was deemed acceptable
	// considering that we are losing 11 bits on a 52+-bit operation and
	// that users dealing with floating points should be aware
	// of floating-point imprecision.
	a.mean += delta / float64(a.count)
	a.sqrDiff += delta * (f - a.mean)
	return nil
}

//Result returns all datums passed to FloatSqrDiffAggregate.
func (a *FloatSqrDiffAggregate) Result() (tree.Datum, error) {
	if a.count < 1 {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sqrDiff)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *FloatSqrDiffAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *FloatSqrDiffAggregate) Size() int64 {
	return sizeOfFloatSqrDiffAggregate
}

//DecimalSqrDiffAggregate is a agg func
type DecimalSqrDiffAggregate struct {
	SingleAggBase
	// Variables used across iterations.
	ed      *apd.ErrDecimal
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	delta apd.Decimal
	tmp   apd.Decimal
	tmp1  apd.Decimal
	tmp2  apd.Decimal
	tmp3  apd.Decimal
	tmp4  apd.Decimal
	tmp5  apd.Decimal

	acc mon.BoundAccount
}

func newDecimalSqrDiff(evalCtx *tree.EvalContext, account *mon.BoundAccount) decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	if account != nil {
		return &DecimalSqrDiffAggregate{
			ed: &ed,
			SingleAggBase: SingleAggBase{
				sharedAcc: account,
			},
		}
	}
	return &DecimalSqrDiffAggregate{
		ed:  &ed,
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
}

func newDecimalSqrDiffAggregate(
	_ []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return newDecimalSqrDiff(evalCtx, account)
}

// Count is part of the decimalSqrDiff interface.
func (a *DecimalSqrDiffAggregate) Count() *apd.Decimal {
	return &a.count
}

// Tmp is part of the decimalSqrDiff interface.
func (a *DecimalSqrDiffAggregate) Tmp() *apd.Decimal {
	return &a.tmp
}

//Add accumulates the passed datum into the DecimalSqrDiffAggregate.
func (a *DecimalSqrDiffAggregate) Add(
	ctx context.Context, datum tree.Datum, _ ...tree.Datum,
) error {
	if datum == tree.DNull {
		return nil
	}
	d := &datum.(*tree.DDecimal).Decimal

	// Uses the Knuth/Welford method for accurately computing squared difference online in a
	// single pass. Refer to squared difference calculations
	// in http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.ed.Add(&a.count, &a.count, decimalOne)
	a.ed.Sub(&a.delta, d, &a.mean)
	a.ed.Quo(&a.tmp, &a.delta, &a.count)
	a.ed.Add(&a.mean, &a.mean, &a.tmp)
	a.ed.Sub(&a.tmp, d, &a.mean)
	a.ed.Add(&a.sqrDiff, &a.sqrDiff, a.ed.Mul(&a.delta, &a.delta, &a.tmp))

	size := tree.SizeOfDecimal(&a.count) +
		tree.SizeOfDecimal(&a.mean) +
		tree.SizeOfDecimal(&a.sqrDiff) +
		tree.SizeOfDecimal(&a.delta) +
		tree.SizeOfDecimal(&a.tmp)
	if a.sharedAcc == nil {
		if err := a.acc.ResizeTo(ctx, int64(size)); err != nil {
			return err
		}
	} else {
		if err := a.sharedAcc.ResizeTo(ctx, int64(size)); err != nil {
			return err
		}
	}

	return a.ed.Err()
}

//OptAdd accumulates the passed datum into the DecimalSqrDiffAggregate.
func (a *DecimalSqrDiffAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*DecimalSqrDiffAggregate)

	// Uses the Knuth/Welford method for accurately computing squared difference online in a
	// single pass. Refer to squared difference calculations
	// in http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.

	a.ed.Mul(&a.tmp2, &a.count, &t.count)
	a.ed.Add(&t.tmp2, &a.count, &t.count)
	a.ed.Quo(&a.tmp3, &a.tmp2, &t.tmp2)
	a.ed.Sub(&t.tmp3, &a.mean, &t.mean)
	a.ed.Mul(&t.tmp3, &t.tmp3, &t.tmp3)
	a.ed.Mul(&a.tmp4, &a.tmp3, &t.tmp3)
	a.ed.Mul(&a.tmp5, &a.count, &a.mean)
	a.ed.Mul(&t.tmp5, &t.count, &t.mean)
	a.ed.Add(&a.tmp5, &a.tmp5, &t.tmp5)
	a.ed.Add(&a.tmp1, &a.sqrDiff, &t.sqrDiff)
	a.ed.Add(&a.sqrDiff, &a.tmp1, &a.tmp4)
	a.ed.Add(&a.count, &a.count, &t.count)
	a.ed.Quo(&a.mean, &a.tmp5, &a.count)

	//TODO: CMS
	//There are certain errors due to different implementation methods
	//a.ed.Add(&a.sqrDiff, &a.sqrDiff, &t.sqrDiff)

	size := tree.SizeOfDecimal(&a.count) +
		tree.SizeOfDecimal(&a.mean) +
		tree.SizeOfDecimal(&a.sqrDiff) +
		tree.SizeOfDecimal(&a.delta) +
		tree.SizeOfDecimal(&a.tmp)
	if a.sharedAcc == nil {
		if err := a.acc.ResizeTo(ctx, int64(size)); err != nil {
			return err
		}
	} else {
		if err := a.sharedAcc.ResizeTo(ctx, int64(size)); err != nil {
			return err
		}
	}

	return a.ed.Err()
}

//Result returns all datums passed to DecimalSqrDiffAggregate.
func (a *DecimalSqrDiffAggregate) Result() (tree.Datum, error) {
	if a.count.Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{Decimal: a.sqrDiff}
	// Remove trailing zeros. Depending on the order in which the input
	// is processed, some number of trailing zeros could be added to the
	// output. Remove them so that the results are the same regardless of order.
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *DecimalSqrDiffAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *DecimalSqrDiffAggregate) Size() int64 {
	return sizeOfDecimalSqrDiffAggregate
}

//FloatSumSqrDiffsAggregate is a agg func
type FloatSumSqrDiffsAggregate struct {
	count   int64
	mean    float64
	sqrDiff float64
}

func newFloatSumSqrDiffs() floatSqrDiff {
	return &FloatSumSqrDiffsAggregate{}
}

// Count is part of the FloatSumSqrDiffsAggregate interface.
func (a *FloatSumSqrDiffsAggregate) Count() int64 {
	return a.count
}

//Add The signature for the datums is:SQRDIFF (float), SUM (float), COUNT(int)
func (a *FloatSumSqrDiffsAggregate) Add(
	_ context.Context, sqrDiffD tree.Datum, otherArgs ...tree.Datum,
) error {
	sumD := otherArgs[0]
	countD := otherArgs[1]
	if sqrDiffD == tree.DNull || sumD == tree.DNull || countD == tree.DNull {
		return nil
	}

	sqrDiff := float64(*sqrDiffD.(*tree.DFloat))
	sum := float64(*sumD.(*tree.DFloat))
	count := int64(*countD.(*tree.DInt))

	mean := sum / float64(count)
	delta := mean - a.mean

	// Compute the sum of Knuth/Welford sum of squared differences from the
	// mean in a single pass. Adapted from sum of RunningStats in
	// https://www.johndcook.com/blog/skewness_kurtosis and our
	// implementation of NumericStats
	// https://github.com/znbasedb/znbase/pull/17728.
	totalCount, ok := arith.AddWithOverflow(a.count, count)
	if !ok {
		return pgerror.NewErrorf(pgcode.NumericValueOutOfRange,
			"number of values in aggregate exceed max count of %d", math.MaxInt64,
		)
	}
	// We are converting an int64 number (with 63-bit precision)
	// to a float64 (with 52-bit precision), thus in the worst cases,
	// we may lose up to 11 bits of precision. This was deemed acceptable
	// considering that we are losing 11 bits on a 52+-bit operation and
	// that users dealing with floating points should be aware
	// of floating-point imprecision.
	a.sqrDiff += sqrDiff + delta*delta*float64(count)*float64(a.count)/float64(totalCount)
	a.count = totalCount
	a.mean += delta * float64(count) / float64(a.count)
	return nil
}

//OptAdd The signature for the datums is:SQRDIFF (float), SUM (float), COUNT(int)TODO: CMS
func (a *FloatSumSqrDiffsAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*FloatSumSqrDiffsAggregate)
	if t.count == 0 || t.mean == 0 || t.sqrDiff == 0 {
		return nil
	}

	sqrDiff := t.sqrDiff
	sum := t.sqrDiff
	count := t.count

	mean := sum / float64(count)
	delta := mean - a.mean

	// Compute the sum of Knuth/Welford sum of squared differences from the
	// mean in a single pass. Adapted from sum of RunningStats in
	// https://www.johndcook.com/blog/skewness_kurtosis and our
	// implementation of NumericStats
	// https://github.com/znbasedb/znbase/pull/17728.
	totalCount, ok := arith.AddWithOverflow(a.count, count)
	if !ok {
		return pgerror.NewErrorf(pgcode.NumericValueOutOfRange,
			"number of values in aggregate exceed max count of %d", math.MaxInt64,
		)
	}
	// We are converting an int64 number (with 63-bit precision)
	// to a float64 (with 52-bit precision), thus in the worst cases,
	// we may lose up to 11 bits of precision. This was deemed acceptable
	// considering that we are losing 11 bits on a 52+-bit operation and
	// that users dealing with floating points should be aware
	// of floating-point imprecision.
	a.sqrDiff += sqrDiff + delta*delta*float64(count)*float64(a.count)/float64(totalCount)
	a.count = totalCount
	a.mean += delta * float64(count) / float64(a.count)
	return nil
}

//Result returns all datums passed to FloatSumSqrDiffsAggregate.
func (a *FloatSumSqrDiffsAggregate) Result() (tree.Datum, error) {
	if a.count < 1 {
		return tree.DNull, nil
	}
	return tree.NewDFloat(tree.DFloat(a.sqrDiff)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *FloatSumSqrDiffsAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *FloatSumSqrDiffsAggregate) Size() int64 {
	return sizeOfFloatSumSqrDiffsAggregate
}

//DecimalSumSqrDiffsAggregate is a agg func
type DecimalSumSqrDiffsAggregate struct {
	SingleAggBase
	// Variables used across iterations.
	ed      *apd.ErrDecimal
	count   apd.Decimal
	mean    apd.Decimal
	sqrDiff apd.Decimal

	// Variables used as scratch space within iterations.
	tmpCount apd.Decimal
	tmpMean  apd.Decimal
	delta    apd.Decimal
	tmp      apd.Decimal

	acc mon.BoundAccount
}

func newDecimalSumSqrDiffs(evalCtx *tree.EvalContext, account *mon.BoundAccount) decimalSqrDiff {
	ed := apd.MakeErrDecimal(tree.IntermediateCtx)
	if account != nil {
		return &DecimalSumSqrDiffsAggregate{
			ed: &ed,
			SingleAggBase: SingleAggBase{
				sharedAcc: account,
			},
		}
	}
	return &DecimalSumSqrDiffsAggregate{
		ed:  &ed,
		acc: evalCtx.Mon.MakeBoundAccount(),
	}
}

// Count is part of the decimalSqrDiff interface.
func (a *DecimalSumSqrDiffsAggregate) Count() *apd.Decimal {
	return &a.count
}

// Tmp is part of the decimalSumSqrDiffs interface.
func (a *DecimalSumSqrDiffsAggregate) Tmp() *apd.Decimal {
	return &a.tmp
}

//Add accumulates the passed datum into the DecimalSumSqrDiffsAggregate.
func (a *DecimalSumSqrDiffsAggregate) Add(
	ctx context.Context, sqrDiffD tree.Datum, otherArgs ...tree.Datum,
) error {
	sumD := otherArgs[0]
	countD := otherArgs[1]
	if sqrDiffD == tree.DNull || sumD == tree.DNull || countD == tree.DNull {
		return nil
	}
	sqrDiff := &sqrDiffD.(*tree.DDecimal).Decimal
	sum := &sumD.(*tree.DDecimal).Decimal
	a.tmpCount.SetInt64(int64(*countD.(*tree.DInt)))

	a.ed.Quo(&a.tmpMean, sum, &a.tmpCount)
	a.ed.Sub(&a.delta, &a.tmpMean, &a.mean)

	// Compute the sum of Knuth/Welford sum of squared differences from the
	// mean in a single pass. Adapted from sum of RunningStats in
	// https://www.johndcook.com/blog/skewness_kurtosis and our
	// implementation of NumericStats
	// https://github.com/znbasedb/znbase/pull/17728.

	// This is logically equivalent to
	//   sqrDiff + delta * delta * tmpCount * a.count / (tmpCount + a.count)
	// where the expression is computed from RIGHT to LEFT.
	a.ed.Add(&a.tmp, &a.tmpCount, &a.count)
	a.ed.Quo(&a.tmp, &a.count, &a.tmp)
	a.ed.Mul(&a.tmp, &a.tmpCount, &a.tmp)
	a.ed.Mul(&a.tmp, &a.delta, &a.tmp)
	a.ed.Mul(&a.tmp, &a.delta, &a.tmp)
	a.ed.Add(&a.tmp, sqrDiff, &a.tmp)
	// Update running squared difference.
	a.ed.Add(&a.sqrDiff, &a.sqrDiff, &a.tmp)

	// Update total count.
	a.ed.Add(&a.count, &a.count, &a.tmpCount)

	// This is logically equivalent to
	//   delta * tmpCount / a.count
	// where the expression is computed from LEFT to RIGHT.
	// Note `a.count` is now the total count (includes tmpCount).
	a.ed.Mul(&a.tmp, &a.delta, &a.tmpCount)
	a.ed.Quo(&a.tmp, &a.tmp, &a.count)
	// Update running mean.
	a.ed.Add(&a.mean, &a.mean, &a.tmp)

	size := tree.SizeOfDecimal(&a.count) +
		tree.SizeOfDecimal(&a.mean) +
		tree.SizeOfDecimal(&a.sqrDiff) +
		tree.SizeOfDecimal(&a.tmpCount) +
		tree.SizeOfDecimal(&a.tmpMean) +
		tree.SizeOfDecimal(&a.delta) +
		tree.SizeOfDecimal(&a.tmp)
	if a.sharedAcc == nil {
		if err := a.acc.ResizeTo(ctx, int64(size)); err != nil {
			return err
		}
	} else {
		if err := a.sharedAcc.ResizeTo(ctx, int64(size)); err != nil {
			return err
		}
	}

	return a.ed.Err()
}

//OptAdd accumulates the passed datum into the DecimalSumSqrDiffsAggregate.TODO: CMS
func (a *DecimalSumSqrDiffsAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*DecimalSumSqrDiffsAggregate)
	sum := t.delta
	countD := t.count
	sqrDiff := t.sqrDiff
	a.tmpCount.SetInt64(int64(countD.Coeff.Uint64()))

	a.ed.Quo(&a.tmpMean, &sum, &a.tmpCount)
	a.ed.Sub(&a.delta, &a.tmpMean, &a.mean)

	// Compute the sum of Knuth/Welford sum of squared differences from the
	// mean in a single pass. Adapted from sum of RunningStats in
	// https://www.johndcook.com/blog/skewness_kurtosis and our
	// implementation of NumericStats
	// https://github.com/znbasedb/znbase/pull/17728.

	// This is logically equivalent to
	//   sqrDiff + delta * delta * tmpCount * a.count / (tmpCount + a.count)
	// where the expression is computed from RIGHT to LEFT.
	a.ed.Add(&a.tmp, &a.tmpCount, &a.count)
	a.ed.Quo(&a.tmp, &a.count, &a.tmp)
	a.ed.Mul(&a.tmp, &a.tmpCount, &a.tmp)
	a.ed.Mul(&a.tmp, &a.delta, &a.tmp)
	a.ed.Mul(&a.tmp, &a.delta, &a.tmp)
	a.ed.Add(&a.tmp, &sqrDiff, &a.tmp)
	// Update running squared difference.
	a.ed.Add(&a.sqrDiff, &a.sqrDiff, &a.tmp)

	// Update total count.
	a.ed.Add(&a.count, &a.count, &a.tmpCount)

	// This is logically equivalent to
	//   delta * tmpCount / a.count
	// where the expression is computed from LEFT to RIGHT.
	// Note `a.count` is now the total count (includes tmpCount).
	a.ed.Mul(&a.tmp, &a.delta, &a.tmpCount)
	a.ed.Quo(&a.tmp, &a.tmp, &a.count)
	// Update running mean.
	a.ed.Add(&a.mean, &a.mean, &a.tmp)

	size := tree.SizeOfDecimal(&a.count) +
		tree.SizeOfDecimal(&a.mean) +
		tree.SizeOfDecimal(&a.sqrDiff) +
		tree.SizeOfDecimal(&a.tmpCount) +
		tree.SizeOfDecimal(&a.tmpMean) +
		tree.SizeOfDecimal(&a.delta) +
		tree.SizeOfDecimal(&a.tmp)
	if a.sharedAcc != nil {
		if err := a.sharedAcc.ResizeTo(ctx, int64(size)); err != nil {
			return err
		}
	} else {
		if err := a.acc.ResizeTo(ctx, int64(size)); err != nil {
			return err
		}
	}

	return a.ed.Err()
}

//Result returns all datums passed to DecimalSumSqrDiffsAggregate.
func (a *DecimalSumSqrDiffsAggregate) Result() (tree.Datum, error) {
	if a.count.Cmp(decimalOne) < 0 {
		return tree.DNull, nil
	}
	dd := &tree.DDecimal{Decimal: a.sqrDiff}
	return dd, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *DecimalSumSqrDiffsAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *DecimalSumSqrDiffsAggregate) Size() int64 {
	return sizeOfDecimalSumSqrDiffsAggregate
}

type floatSqrDiff interface {
	tree.AggregateFunc
	Count() int64
}

type decimalSqrDiff interface {
	tree.AggregateFunc
	Count() *apd.Decimal
	Tmp() *apd.Decimal
}

//FloatVarianceAggregate is a agg func
type FloatVarianceAggregate struct {
	agg floatSqrDiff
}

//DecimalVarianceAggregate is a agg func
type DecimalVarianceAggregate struct {
	agg decimalSqrDiff
}

// Both Variance and FinalVariance aggregators have the same codepath for
// their tree.AggregateFunc interface.
// The key difference is that Variance employs SqrDiffAggregate which
// has one input: VALUE; whereas FinalVariance employs SumSqrDiffsAggregate
// which takes in three inputs: (local) SQRDIFF, SUM, COUNT.
// FinalVariance is used for local/final aggregation in distsql.
func newIntVarianceAggregate(
	params []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalVarianceAggregate{agg: newIntSqrDiff(evalCtx, account)}
}

func newFloatVarianceAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &FloatVarianceAggregate{agg: newFloatSqrDiff()}
}

func newDecimalVarianceAggregate(
	_ []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalVarianceAggregate{agg: newDecimalSqrDiff(evalCtx, account)}
}

func newFloatFinalVarianceAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &FloatVarianceAggregate{agg: newFloatSumSqrDiffs()}
}

func newDecimalFinalVarianceAggregate(
	_ []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalVarianceAggregate{agg: newDecimalSumSqrDiffs(evalCtx, account)}
}

// Add is part of the tree.AggregateFunc interface.
//  Variance: VALUE(float)
//  FinalVariance: SQRDIFF(float), SUM(float), COUNT(int)
func (a *FloatVarianceAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// OptAdd is part of the tree.AggregateFunc interface.
//  Variance: VALUE(float)
//  FinalVariance: SQRDIFF(float), SUM(float), COUNT(int) TODO: CMS
func (a *FloatVarianceAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*FloatVarianceAggregate)
	return a.agg.OptAdd(ctx, t.agg)
}

// Add is part of the tree.AggregateFunc interface.
//  Variance: VALUE(int|decimal)
//  FinalVariance: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *DecimalVarianceAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// OptAdd is part of the tree.AggregateFunc interface.
//  Variance: VALUE(int|decimal)
//  FinalVariance: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *DecimalVarianceAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*DecimalVarianceAggregate)
	return a.agg.OptAdd(ctx, t.agg)
}

// Result calculates the variance from the member square difference aggregator.
func (a *FloatVarianceAggregate) Result() (tree.Datum, error) {
	if a.agg.Count() < 2 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	return tree.NewDFloat(tree.DFloat(float64(*sqrDiff.(*tree.DFloat)) / (float64(a.agg.Count()) - 1))), nil
}

// Result calculates the variance from the member square difference aggregator.
func (a *DecimalVarianceAggregate) Result() (tree.Datum, error) {
	if a.agg.Count().Cmp(decimalTwo) < 0 {
		return tree.DNull, nil
	}
	sqrDiff, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if _, err = tree.IntermediateCtx.Sub(a.agg.Tmp(), a.agg.Count(), decimalOne); err != nil {
		return nil, err
	}
	dd := &tree.DDecimal{}
	if _, err = tree.DecimalCtx.Quo(&dd.Decimal, &sqrDiff.(*tree.DDecimal).Decimal, a.agg.Tmp()); err != nil {
		return nil, err
	}
	// Remove trailing zeros. Depending on the order in which the input is
	// processed, some number of trailing zeros could be added to the
	// output. Remove them so that the results are the same regardless of
	// order.
	dd.Decimal.Reduce(&dd.Decimal)
	return dd, nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *FloatVarianceAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *FloatVarianceAggregate) Size() int64 {
	return sizeOfFloatVarianceAggregate
}

// Close is part of the tree.AggregateFunc interface.
func (a *DecimalVarianceAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *DecimalVarianceAggregate) Size() int64 {
	return sizeOfDecimalVarianceAggregate
}

//FloatStdDevAggregate is a agg func
type FloatStdDevAggregate struct {
	agg tree.AggregateFunc
}

//DecimalStdDevAggregate is a agg func
type DecimalStdDevAggregate struct {
	agg tree.AggregateFunc
}

// Both StdDev and FinalStdDev aggregators have the same codepath for
// their tree.AggregateFunc interface.
// The key difference is that StdDev employs SqrDiffAggregate which
// has one input: VALUE; whereas FinalStdDev employs SumSqrDiffsAggregate
// which takes in three inputs: (local) SQRDIFF, SUM, COUNT.
// FinalStdDev is used for local/final aggregation in distsql.
func newIntStdDevAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalStdDevAggregate{agg: newIntVarianceAggregate(params, evalCtx, arguments, account)}
}

func newFloatStdDevAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &FloatStdDevAggregate{agg: newFloatVarianceAggregate(params, evalCtx, arguments, account)}
}

func newDecimalStdDevAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalStdDevAggregate{agg: newDecimalVarianceAggregate(params, evalCtx, arguments, account)}
}

func newFloatFinalStdDevAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &FloatStdDevAggregate{agg: newFloatFinalVarianceAggregate(params, evalCtx, arguments, account)}
}

func newDecimalFinalStdDevAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalStdDevAggregate{agg: newDecimalFinalVarianceAggregate(params, evalCtx, arguments, account)}
}

// Add implements the tree.AggregateFunc interface.
// The signature of the datums is:
//  StdDev: VALUE(float)
//  FinalStdDev: SQRDIFF(float), SUM(float), COUNT(int)
func (a *FloatStdDevAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// OptAdd implements the tree.AggregateFunc interface.
// The signature of the datums is:
//  StdDev: VALUE(float)
//  FinalStdDev: SQRDIFF(float), SUM(float), COUNT(int) TODO: CMS
func (a *FloatStdDevAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*FloatStdDevAggregate)
	return a.agg.OptAdd(ctx, t.agg)
}

// Add is part of the tree.AggregateFunc interface.
// The signature of the datums is:
//  StdDev: VALUE(int|decimal)
//  FinalStdDev: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *DecimalStdDevAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// OptAdd is part of the tree.AggregateFunc interface.
// The signature of the datums is:
//  StdDev: VALUE(int|decimal)
//  FinalStdDev: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *DecimalStdDevAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*DecimalStdDevAggregate)
	return a.agg.OptAdd(ctx, t.agg)
}

// Result computes the square root of the variance aggregator.
func (a *FloatStdDevAggregate) Result() (tree.Datum, error) {
	variance, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if variance == tree.DNull {
		return variance, nil
	}
	return tree.NewDFloat(tree.DFloat(math.Sqrt(float64(*variance.(*tree.DFloat))))), nil
}

// Result computes the square root of the variance aggregator.
func (a *DecimalStdDevAggregate) Result() (tree.Datum, error) {
	// TODO(richardwu): both decimalVarianceAggregate and
	// finalDecimalVarianceAggregate return a decimal result with
	// default tree.DecimalCtx precision. We want to be able to specify that the
	// varianceAggregate use tree.IntermediateCtx (with the extra precision)
	// since it is returning an intermediate value for stdDevAggregate (of
	// which we take the Sqrt).
	variance, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if variance == tree.DNull {
		return variance, nil
	}
	varianceDec := variance.(*tree.DDecimal)
	_, err = tree.DecimalCtx.Sqrt(&varianceDec.Decimal, &varianceDec.Decimal)
	return varianceDec, err
}

// Close is part of the tree.AggregateFunc interface.
func (a *FloatStdDevAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *FloatStdDevAggregate) Size() int64 {
	return sizeOfFloatStdDevAggregate
}

// Close is part of the tree.AggregateFunc interface.
func (a *DecimalStdDevAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *DecimalStdDevAggregate) Size() int64 {
	return sizeOfDecimalStdDevAggregate
}

//FloatStddevSampAggregate is a agg func
type FloatStddevSampAggregate struct {
	agg tree.AggregateFunc
}

// OptAdd for agg opt
func (a *FloatStddevSampAggregate) OptAdd(_ context.Context, _ interface{}) error {
	panic("implement me")
}

//DecimalStddevSampAggregate is a agg func
type DecimalStddevSampAggregate struct {
	agg tree.AggregateFunc
}

// OptAdd for agg opt
func (a *DecimalStddevSampAggregate) OptAdd(_ context.Context, _ interface{}) error {
	panic("implement me")
}

// Both Stddevsamp and FinalStddevsamp aggregators have the same codepath for
// their tree.AggregateFunc interface.
// The key difference is that Stddevsamp employs SqrDiffAggregate which
// has one input: VALUE; whereas FinalStddevsamp employs SumSqrDiffsAggregate
// which takes in three inputs: (local) SQRDIFF, SUM, COUNT.
// FinalStddevsamp is used for local/final aggregation in distsql.
func newIntStddevSampAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, acc *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalStddevSampAggregate{agg: newIntVarianceAggregate(params, evalCtx, arguments, acc)}
}

func newFloatStddevSampAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, acc *mon.BoundAccount,
) tree.AggregateFunc {
	return &FloatStddevSampAggregate{agg: newFloatVarianceAggregate(params, evalCtx, arguments, acc)}
}

func newDecimalStddevSampAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, acc *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalStddevSampAggregate{agg: newDecimalVarianceAggregate(params, evalCtx, arguments, acc)}
}

func newFloatFinalStddevSampAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, acc *mon.BoundAccount,
) tree.AggregateFunc {
	return &FloatStddevSampAggregate{agg: newFloatFinalVarianceAggregate(params, evalCtx, arguments, acc)}
}

func newDecimalFinalStddevSampAggregate(
	params []types.T, evalCtx *tree.EvalContext, arguments tree.Datums, acc *mon.BoundAccount,
) tree.AggregateFunc {
	return &DecimalStddevSampAggregate{agg: newDecimalFinalVarianceAggregate(params, evalCtx, arguments, acc)}
}

// Add implements the tree.AggregateFunc interface.
// The signature of the datums is:
//  Stddevsamp: VALUE(float)
//  FinalStddevsamp: SQRDIFF(float), SUM(float), COUNT(int)
func (a *FloatStddevSampAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Add is part of the tree.AggregateFunc interface.
// The signature of the datums is:
//  Stddevsamp: VALUE(int|decimal)
//  FinalStddevsamp: SQRDIFF(decimal), SUM(decimal), COUNT(int)
func (a *DecimalStddevSampAggregate) Add(
	ctx context.Context, firstArg tree.Datum, otherArgs ...tree.Datum,
) error {
	return a.agg.Add(ctx, firstArg, otherArgs...)
}

// Result computes the square root of the variance aggregator.
func (a *FloatStddevSampAggregate) Result() (tree.Datum, error) {
	variance, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if variance == tree.DNull {
		return variance, nil
	}
	return tree.NewDFloat(tree.DFloat(math.Sqrt(float64(*variance.(*tree.DFloat))))), nil
}

// Result computes the square root of the variance aggregator.
func (a *DecimalStddevSampAggregate) Result() (tree.Datum, error) {
	// TODO(richardwu): both decimalVarianceAggregate and
	// finalDecimalVarianceAggregate return a decimal result with
	// default tree.DecimalCtx precision. We want to be able to specify that the
	// varianceAggregate use tree.IntermediateCtx (with the extra precision)
	// since it is returning an intermediate value for StddevsampAggregate (of
	// which we take the Sqrt).
	variance, err := a.agg.Result()
	if err != nil {
		return nil, err
	}
	if variance == tree.DNull {
		return variance, nil
	}
	varianceDec := variance.(*tree.DDecimal)
	_, err = tree.DecimalCtx.Sqrt(&varianceDec.Decimal, &varianceDec.Decimal)
	return varianceDec, err
}

// Close is part of the tree.AggregateFunc interface.
func (a *FloatStddevSampAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *FloatStddevSampAggregate) Size() int64 {
	return sizeOfFloatStddevSampAggregate
}

// Close is part of the tree.AggregateFunc interface.
func (a *DecimalStddevSampAggregate) Close(ctx context.Context) {
	a.agg.Close(ctx)
}

// Size is part of the tree.AggregateFunc interface.
func (a *DecimalStddevSampAggregate) Size() int64 {
	return sizeOfDecimalStddevSampAggregate
}

//BytesXorAggregate is a agg func
type BytesXorAggregate struct {
	sum        []byte
	sawNonNull bool
}

func newBytesXorAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &BytesXorAggregate{}
}

// Add inserts one value into the running xor.
func (a *BytesXorAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	t := []byte(*datum.(*tree.DBytes))
	if !a.sawNonNull {
		a.sum = append([]byte(nil), t...)
	} else if len(a.sum) != len(t) {
		return pgerror.NewErrorf(pgcode.InvalidParameterValue,
			"arguments to xor must all be the same length %d vs %d", len(a.sum), len(t),
		)
	} else {
		for i := range t {
			a.sum[i] = a.sum[i] ^ t[i]
		}
	}
	a.sawNonNull = true
	return nil
}

// OptAdd inserts one value into the running xor.TODO: CMS
func (a *BytesXorAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t1 := b2.(*BytesXorAggregate)
	t := t1.sum
	if !a.sawNonNull {
		a.sum = append([]byte(nil), t...)
	} else if len(a.sum) != len(t) {
		return pgerror.NewErrorf(pgcode.InvalidParameterValue,
			"arguments to xor must all be the same length %d vs %d", len(a.sum), len(t),
		)
	} else {
		for i := range t {
			a.sum[i] = a.sum[i] ^ t[i]
		}
	}
	a.sawNonNull = true
	return nil
}

// Result returns the xor.
func (a *BytesXorAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDBytes(tree.DBytes(a.sum)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *BytesXorAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *BytesXorAggregate) Size() int64 {
	return sizeOfBytesXorAggregate
}

//IntXorAggregate is a agg func
type IntXorAggregate struct {
	sum        int64
	sawNonNull bool
}

func newIntXorAggregate(
	_ []types.T, _ *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	return &IntXorAggregate{}
}

// Add inserts one value into the running xor.
func (a *IntXorAggregate) Add(_ context.Context, datum tree.Datum, _ ...tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	x := int64(*datum.(*tree.DInt))
	a.sum = a.sum ^ x
	a.sawNonNull = true
	return nil
}

// OptAdd inserts one value into the running xor.
func (a *IntXorAggregate) OptAdd(_ context.Context, b2 interface{}) error {
	t := b2.(*IntXorAggregate)
	x := t.sum
	a.sum = a.sum ^ x
	a.sawNonNull = true
	return nil
}

// Result returns the xor.
func (a *IntXorAggregate) Result() (tree.Datum, error) {
	if !a.sawNonNull {
		return tree.DNull, nil
	}
	return tree.NewDInt(tree.DInt(a.sum)), nil
}

// Close is part of the tree.AggregateFunc interface.
func (a *IntXorAggregate) Close(context.Context) {}

// Size is part of the tree.AggregateFunc interface.
func (a *IntXorAggregate) Size() int64 {
	return sizeOfIntXorAggregate
}

//JSONAggregate is a agg func
type JSONAggregate struct {
	SingleAggBase
	builder    *json.ArrayBuilderWithCounter
	acc        mon.BoundAccount
	sawNonNull bool
}

func newJSONAggregate(
	_ []types.T, evalCtx *tree.EvalContext, _ tree.Datums, account *mon.BoundAccount,
) tree.AggregateFunc {
	if account != nil {
		return &JSONAggregate{
			builder:    json.NewArrayBuilderWithCounter(),
			sawNonNull: false,
			SingleAggBase: SingleAggBase{
				sharedAcc: account,
			},
		}
	}
	return &JSONAggregate{
		builder:    json.NewArrayBuilderWithCounter(),
		acc:        evalCtx.Mon.MakeBoundAccount(),
		sawNonNull: false,
	}
}

// Add accumulates the transformed json into the JSON array.
func (a *JSONAggregate) Add(ctx context.Context, datum tree.Datum, _ ...tree.Datum) error {
	j, err := tree.AsJSON(datum)
	if err != nil {
		return err
	}
	oldSize := a.builder.Size()
	a.builder.Add(j)
	if a.sharedAcc == nil {
		if err = a.acc.Grow(ctx, int64(a.builder.Size()-oldSize)); err != nil {
			return err
		}
	} else {
		if err = a.sharedAcc.Grow(ctx, int64(a.builder.Size()-oldSize)); err != nil {
			return err
		}
	}

	a.sawNonNull = true
	return nil
}

// OptAdd accumulates the transformed json into the JSON array.
func (a *JSONAggregate) OptAdd(ctx context.Context, b2 interface{}) error {
	t := b2.(*JSONAggregate)
	oldSize := a.builder.Size()
	jsons := t.builder.GetArrayBuilder().GetJsons()
	for _, json := range jsons {
		a.builder.Add(json)
	}
	if a.sharedAcc != nil {
		if err := a.sharedAcc.Grow(ctx, int64(a.builder.Size()-oldSize)); err != nil {
			return err
		}
	} else {
		if err := a.acc.Grow(ctx, int64(a.builder.Size()-oldSize)); err != nil {
			return err
		}
	}

	a.sawNonNull = true
	return nil
}

// Result returns an DJSON from the array of JSON.
func (a *JSONAggregate) Result() (tree.Datum, error) {
	if a.sawNonNull {
		return tree.NewDJSON(a.builder.Build()), nil
	}
	return tree.DNull, nil
}

// Close allows the aggregate to release the memory it requested during
// operation.
func (a *JSONAggregate) Close(ctx context.Context) {
	if a.sharedAcc == nil {
		a.acc.Close(ctx)
	}
}

// Size is part of the tree.AggregateFunc interface.
func (a *JSONAggregate) Size() int64 {
	return sizeOfJSONAggregate
}
