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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	gojson "encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/fnv"
	"math"
	"math/big"
	"math/rand"
	"net"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/knz/strtime"
	"github.com/pkg/errors"
	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/build"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/duration"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
	"github.com/znbasedb/znbase/pkg/util/ipaddr"
	"github.com/znbasedb/znbase/pkg/util/json"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeofday"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

var (
	errEmptyInputString = pgerror.NewError(pgcode.InvalidParameterValue, "the input string must not be empty")
	errAbsOfMinInt64    = pgerror.NewError(pgcode.NumericValueOutOfRange, "abs of min integer value (-9223372036854775808) not defined")
	//	errSqrtOfNegNumber  = pgerror.NewError(pgcode.InvalidArgumentForPowerFunction, "cannot take square root of a negative number")
	errLogOfNegNumber   = pgerror.NewError(pgcode.InvalidArgumentForLogarithm, "cannot take logarithm of a negative number")
	errLogOfZero        = pgerror.NewError(pgcode.InvalidArgumentForLogarithm, "cannot take logarithm of zero")
	errZeroIP           = pgerror.NewError(pgcode.InvalidParameterValue, "zero length IP")
	errChrValueTooSmall = pgerror.NewError(pgcode.InvalidParameterValue, "input value must be >= 0")
	errChrValueTooLarge = pgerror.NewErrorf(pgcode.InvalidParameterValue,
		"input value must be <= %d (maximum Unicode code point)", utf8.MaxRune)
	errStringTooLarge = pgerror.NewErrorf(pgcode.ProgramLimitExceeded,
		fmt.Sprintf("requested length too large, exceeds %s", humanizeutil.IBytes(maxAllocatedStringSize)))
)

const maxAllocatedStringSize = 128 * 1024 * 1024

const errInsufficientArgsFmtString = "unknown signature: %s()"

const (
	nsPerDay     = 86400 * 1000000000
	daysPerMonth = 30
	nsPerSecond  = 1000000000
)

const (
	categoryComparison    = "Comparison"
	categoryCompatibility = "Compatibility"
	categoryDateAndTime   = "Date and time"
	categoryIDGeneration  = "ID generation"
	categorySequences     = "Sequence"
	categoryMath          = "Math and numeric"
	categoryString        = "String and byte"
	categoryArray         = "Array"
	categorySystemInfo    = "System info"
	categoryGenerator     = "Set-returning"
	categoryJSON          = "JSONB"
)

func nextVal(
	fInt func(tree.DInt) *tree.DInt,
) func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	return func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		name := tree.MustBeDString(args[0])
		qualifiedName, err := evalCtx.Sequence.ParseQualifiedTableName(evalCtx.Ctx(), string(name))
		if err != nil {
			return nil, err
		}
		res, err := evalCtx.Sequence.IncrementSequence(evalCtx.Ctx(), qualifiedName)
		if err != nil {
			return nil, err
		}
		return fInt(tree.DInt(res)), nil
	}
}

func categorizeType(t types.T) string {
	switch t {
	case types.Date, types.Interval, types.Timestamp, types.TimestampTZ:
		return categoryDateAndTime
	case types.Int, types.Int2, types.Int4, types.Decimal, types.Float, types.Float4:
		return categoryMath
	case types.String, types.Bytes:
		return categoryString
	default:
		return strings.ToUpper(t.String())
	}
}

var digitNames = []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}

// builtinDefinition represents a built-in function before it becomes
type builtinDefinition struct {
	props     tree.FunctionProperties
	overloads []tree.Overload
}

// GetBuiltinProperties provides low-level access to a built-in function's properties.
// For a better, semantic-rich interface consider using tree.FunctionDefinition
// instead, and resolve function names via ResolvableFunctionReference.Resolve().
func GetBuiltinProperties(name string) (*tree.FunctionProperties, []tree.Overload) {
	def, ok := builtins[name]
	if !ok {
		return nil, nil
	}
	return &def.props, def.overloads
}

// defProps is used below to define built-in functions with default properties.
func defProps() tree.FunctionProperties { return tree.FunctionProperties{} }

// arrayProps is used below for array functions.
func arrayProps() tree.FunctionProperties { return tree.FunctionProperties{Category: categoryArray} }

// arrayPropsNullableArgs is used below for array functions that accept NULLs as arguments.
func arrayPropsNullableArgs() tree.FunctionProperties {
	p := arrayProps()
	p.NullableArgs = true
	return p
}

// 函数makeBuiltin用于创建内建函数，props用于标记内建函数的类别，overloads不定长参数，是内建函数的重载实现。
func makeBuiltin(props tree.FunctionProperties, overloads ...tree.Overload) builtinDefinition {
	return builtinDefinition{
		props:     props,     // 用于在生成文档时标记内建函数的类别，不影响逻辑实现
		overloads: overloads, // 内建函数重载
	}
}

func newDecodeError(enc string) error {
	return pgerror.NewErrorf(pgcode.CharacterNotInRepertoire,
		"invalid byte sequence for encoding %q", enc)
}

func newEncodeError(c rune, enc string) error {
	return pgerror.NewErrorf(pgcode.UntranslatableCharacter,
		"character %q has no representation in encoding %q", c, enc)
}

// builtins contains the built-in functions indexed by name.
// For use in other packages, see AllBuiltinNames and GetBuiltinProperties().
var builtins = map[string]builtinDefinition{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length":           lengthImpls,
	"char_length":      lengthImpls,
	"character_length": lengthImpls,
	"instr": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.String}, {Name: "substr", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDString(args[0]))
				substr := string(tree.MustBeDString(args[1]))
				if substr == "" {
					return tree.NewDString(""), nil
				}
				if str == "" {
					return tree.NewDString(""), nil
				}
				return tree.NewDString(strconv.Itoa(strings.Index(str, substr) + 1)), nil
			},
			Info: "Find the index of the first instance of substr in s",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.String}, {Name: "substr", Typ: types.String}, {Name: "startPos", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDString(args[0]))
				substr := string(tree.MustBeDString(args[1]))
				startPos := int(tree.MustBeDInt(args[2]))
				if substr == "" {
					return tree.NewDString(""), nil
				}
				if str == "" {
					return tree.NewDString(""), nil
				}
				if startPos == 0 {
					return tree.NewDString(strconv.Itoa(0)), nil
				}
				var poss []int
				for i := 0; i < len(str)-len(substr); {
					pos := strings.Index(str[i:], substr)
					if pos > 0 {
						poss = append(poss, pos+i)
						i += pos + 1
					} else if pos < 0 {
						break
					} else {
						poss = append(poss, pos+i)
						i++
					}
				}
				if len(poss) == 0 {
					return tree.NewDString("0"), nil
				}
				if startPos > 0 {
					for _, pos := range poss {
						if pos+1 >= startPos {
							return tree.NewDString(strconv.Itoa(pos + 1)), nil
						}
					}
					return tree.NewDString("0"), nil
				} else if startPos < 0 {
					for i := len(poss) - 1; i >= 0; i-- {
						if poss[i]+len(substr) <= len(str)+startPos+1 {
							return tree.NewDString(strconv.Itoa(poss[i] + 1)), nil
						}
					}
					return tree.NewDString("0"), nil

				}
				return tree.NewDString("0"), nil
			},
			Info: "find the index of the instance of substr in s from the `startPos` of s",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.String}, {Name: "substr", Typ: types.String}, {Name: "startPos", Typ: types.Int}, {Name: "nth", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDString(args[0]))
				substr := string(tree.MustBeDString(args[1]))
				startPos := int(tree.MustBeDInt(args[2]))
				nth := int(tree.MustBeDInt(args[3]))
				if substr == "" {
					return tree.NewDString(""), nil
				}
				if str == "" {
					return tree.NewDString(""), nil
				}
				if startPos == 0 {
					return tree.NewDString("0"), nil
				}
				var poss []int
				for i := 0; i < len(str)-len(substr); {
					pos := strings.Index(str[i:], substr)
					if pos > 0 {
						poss = append(poss, pos+i)
						i += pos + 1
					} else if pos < 0 {
						break
					} else {
						poss = append(poss, pos+i)
						i++
					}
				}
				if len(poss) == 0 {
					return tree.NewDString(""), nil
				}
				appearTimes := 0
				if startPos > 0 {
					if len(poss) < nth {
						return tree.NewDString("0"), nil
					}
					for _, pos := range poss {
						if pos+1 >= startPos {
							appearTimes++
							if appearTimes == nth {
								return tree.NewDString(strconv.Itoa(pos + 1)), nil
							}
						}
					}
				}
				if startPos < 0 {
					for i := len(poss) - 1; i >= 0; i-- {
						if poss[i]+len(substr) <= len(str)+startPos+1 {
							appearTimes++
							if appearTimes == nth {
								return tree.NewDString(strconv.Itoa(poss[i] + 1)), nil
							}
						}
					}
				}
				return tree.NewDString("0"), nil
			},
			Info: "find the index of the `nth` instance of substr in s from the `startPos` of s",
		},
	),

	"insert": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.String}, {Name: "Pos", Typ: types.Int}, {Name: "length", Typ: types.Int}, {Name: "newstr", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDString(args[0]))
				pos := int(tree.MustBeDInt(args[1]))
				length := int(tree.MustBeDInt(args[2]))
				newstr := string(tree.MustBeDString(args[3]))
				return tree.NewDString(strInsertSubstr(str, pos, length, newstr)), nil
			},
			Info: "Insert substring at specified position.",
		},
	),

	"sessiontimezone": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := strings.Split(fmt.Sprint(timeutil.LocalNow()), " ")[2]
				return tree.NewDString(s[0:len(s)-2] + ":" + s[len(s)-2:]), nil
			},
			Info: "Show the SESSIONTIMEZONE",
		},
	),

	// Returns true iff the current user has admin role.
	// Note: it would be a privacy leak to extend this to check arbitrary usernames.
	"zbdb_internal.is_admin": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			//DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(evalCtx *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				if evalCtx.SessionAccessor == nil {
					return nil, errors.New("session accessor not set")
				}
				ctx := evalCtx.Ctx()
				isAdmin, err := evalCtx.SessionAccessor.HasAdminRole(ctx)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(isAdmin)), nil
			},
			Info: "Retrieves the current user's admin status.",
			//Volatility: tree.VolatilityStable,
		},
	),

	"bit_length": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s) * 8)), nil
		}, types.Int, "Calculates the number of bits used to represent `val`."),
		bytesOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s) * 8)), nil
		}, types.Int, "Calculates the number of bits in `val`."),
	),

	"octet_length": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes used to represent `val`."),
		bytesOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes in `val`."),
	),

	// TODO(pmattis): What string functions should also support types.Bytes?

	"vsize": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes used to represent `val`."),
		bytesOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes in `val`."),
	),

	"bin_to_num": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Int},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var numStr string
				var numInt int
				for i, b := range args {
					if b == tree.DNull {
						//continue
						return nil, fmt.Errorf("illegal argument for function")
					}
					bInt := int(tree.MustBeDInt(args[i]))
					bStr := strconv.Itoa(int(bInt))
					if bStr != "1" && bStr != "0" {
						return nil, fmt.Errorf("argument '%s' is out of range", bStr)
					}
					numStr += bStr
				}
				l := len(numStr)
				for i := l - 1; i >= 0; i-- {
					numInt += (int(numStr[l-i-1]) & 0xf) << uint8(i)
				}
				return tree.NewDInt(tree.DInt(numInt)), nil
			},
			Info: "Convert the bit vector to its equivalent number.",
		},
	),

	"quarter": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[0].(*tree.DTimestamp)
				timeSpan := strings.ToLower("quarter")
				return extractStringFromTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `quarter` from `input`.\n\n",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower("quarter")
				date := args[0].(*tree.DDate)
				fromTSTZ := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `quarter` from `input`.\n\n",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[0].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower("quarter")
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `quarter` from `input`.\n\n",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				result, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					return tree.DNull, nil
				}
				fromTS := result.(*tree.DTimestamp)
				timeSpan := strings.ToLower("quarter")
				return extractStringFromTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `quarter` from `input`.\n\n",
		},
	),

	"date": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				result, err := tree.PerformCast(ctx, args[0], coltypes.Date)
				if err != nil {
					if strings.Contains(err.Error(), "cannot interpret field:") ||
						strings.Contains(err.Error(), "time out of range:") ||
						strings.Contains(err.Error(), "unexpected separator") {
						return nil, fmt.Errorf("field value %s cannot convert to a date", args[0])
					}
					return nil, err
				}
				return result, nil
			},
			Info: "Extracts `date` from `input`.\n\n",
		},
	),

	"lower": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.ToLower(s)), nil
		}, types.String, "Converts all characters in `val` to their lower-case equivalents.")),

	"ucase": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.ToUpper(s)), nil
		}, types.String, "Converts all characters in `val` to their to their upper-case equivalents.")),
	"lcase": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.ToLower(s)), nil
		}, types.String, "Converts all characters in `val` to their lower-case equivalents.")),

	"upper": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.ToUpper(s)), nil
		}, types.String, "Converts all characters in `val` to their to their upper-case equivalents.")),

	"space": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "number", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var res string
				num, err := tree.PerformCast(ctx, args[0], coltypes.Float)
				if err != nil {
					return nil, nil
				}
				Fnum := math.Floor(float64(tree.MustBeDFloat(num)) + 0.5)
				for i := float64(0); i < Fnum; i++ {
					res = res + " "
				}
				return tree.NewDString(res), nil
			},
			Info: "Return a string of the specified number of spaces",
		},
	),

	"substr":    substringImpls,
	"substring": substringImpls,

	//to_char is for compatibility with the  to_char in Postgresql
	"to_char": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "fromTime", Typ: types.Timestamp}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timestamp := args[0].(*tree.DTimestamp).Time
				format := string(tree.MustBeDString(args[1]))
				ret, err := tree.StrFormatOfTime(timestamp, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Converts timestamp to string for compatibility with the  to_char in Postgresql",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "Interval", Typ: types.Interval}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				t := args[0].(*tree.DInterval).String()
				i := strings.Index(t, ".")
				nanoseconds := 0.0
				format := string(tree.MustBeDString(args[1]))
				seconds, ok := args[0].(*tree.DInterval).AsInt64()
				if !ok {
					return nil, fmt.Errorf("argument convert to int64 failed")
				}

				if i != -1 {
					var err error
					nanoseconds, err = strconv.ParseFloat("0"+t[i:len(t)-1], 64)
					if err != nil {
						return nil, err
					}
				}
				ret, err := tree.StrFormatOfInterval(seconds, int64(nanoseconds*nsPerSecond), format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Converts interval to string for compatibility with the  to_char in Postgresql",
		},
		tree.Overload{

			Types:      tree.ArgTypes{{Name: "timestamptz", Typ: types.TimestampTZ}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timestamptz := args[0].(*tree.DTimestampTZ).UTC()
				format := string(tree.MustBeDString(args[1]))
				ret, err := tree.StrFormatOfTime(timestamptz, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Converts timestamptz to string for compatibility with the  to_char in Postgresql",
		},
		tree.Overload{

			Types:      tree.ArgTypes{{Name: "time", Typ: types.Time}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				time := timeofday.TimeOfDay(*args[0].(*tree.DTime))
				format := string(tree.MustBeDString(args[1]))
				ret, err := tree.StrFormatOfTime(time.ToTime(), format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Converts time to string",
		},
		tree.Overload{

			Types:      tree.ArgTypes{{Name: "date", Typ: types.Date}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date := timeutil.Unix(int64(*args[0].(*tree.DDate))*tree.SecondsInDay, 0)
				format := string(tree.MustBeDString(args[1]))
				ret, err := tree.StrFormatOfTime(date, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Converts date to string",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "intNum", Typ: types.Int}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				intNum := int(*args[0].(*tree.DInt))
				format := string(tree.MustBeDString(args[1]))
				dmarks := tree.FindDMark(format)
				if strings.Contains(format, "x") {
					return tree.NewDString(strings.ToLower(fmt.Sprintf("%x", intNum))), nil
				} else if strings.Contains(format, "X") {
					return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", intNum))), nil
				}
				if strings.Contains(format, "\"") {
					for i := 0; i < len(dmarks); i++ {
						format = strings.Replace(format, dmarks[i].Cont, "'", 1)
					}
				}
				if (strings.Contains(format, ".") ||
					strings.Contains(format, "D")) &&
					strings.Contains(format, "V") {
					return nil, errors.New("cannot use \"V\" and decimal point together")
				}
				if strings.Contains(format, "RN") {
					if strings.Contains(format, "EEEE") {
						return nil, errors.New("\"EEEE\" is incompatible with other formats")
					}
					format = tree.RN(intNum, format)
					if strings.Contains(format, "FM") {
						format = strings.ReplaceAll(strings.ReplaceAll(format, "FM", ""), " ", "")
					}
					if strings.Contains(format, "\"") {
						for i := 0; i < len(dmarks); i++ {
							format = strings.Replace(format, "\"'\"", dmarks[i].Cont, 1)
						}
					}
					return tree.NewDString(format), nil
				}
				intNum, format = tree.FillV(intNum, format)
				markFM := strings.Contains(format, "FM")
				format = tree.FillNum(intNum, format)
				if strings.Contains(format, "PR") {
					var err error
					format, err = tree.FillPR(intNum, format)
					if err != nil {
						return nil, err
					}
					format = strings.ReplaceAll(format, "PR", "")
				} else {
					head := 0
					for i := 0; i < len(format); i++ {
						if '0' <= format[i] && format[i] <= '9' {
							head = i
							break
						}
					}
					if intNum < 0 {
						if strings.Contains(format, "S") ||
							strings.Contains(format, "MI") ||
							strings.Contains(format, "SG") {
							format = strings.ReplaceAll(strings.ReplaceAll(format, "SG", "-"), "MI", "-")
							format = strings.ReplaceAll(format, "S", "-")
						} else {
							format = format[:head] + "-" + format[head:]
						}
					}
					if markFM {
						format = strings.ReplaceAll(format, "MI", "")
					}
					if !markFM && intNum >= 0 {
						if strings.Contains(format, "S") ||
							strings.Contains(format, "MI") ||
							strings.Contains(format, "SG") {
							format = strings.ReplaceAll(strings.ReplaceAll(format, "SG", "+"), "MI", " ")
							format = strings.ReplaceAll(format, "S", "+")
						} else {
							format = format[:head] + " " + format[head:]
						}
					}
				}
				if strings.Contains(format, "EEEE") {
					if strings.Contains(format, "PR") ||
						strings.Contains(format, "SG") ||
						strings.Contains(format, "S") ||
						strings.Contains(format, "L") ||
						strings.Contains(format, "D") ||
						strings.Contains(format, "FM") ||
						strings.Contains(format, "MI") ||
						strings.Contains(format, "RN") ||
						strings.Contains(format, strings.ToLower("TH")) ||
						strings.Contains(format, "V") {
						return nil, fmt.Errorf("%s may only be used together with digit and decimal point patterns", "EEEE")
					}
				}
				format, err := tree.ReplaceChar(intNum, format)
				if err != nil {
					return nil, err
				}
				if strings.Contains(format, "EEEE") {
					ret, err := tree.IntToEEEE(intNum, format)
					if err != nil {
						return nil, err
					}
					if strings.Contains(ret, "\"") {
						for i := 0; i < len(dmarks); i++ {
							ret = strings.Replace(ret, "\"'\"", dmarks[i].Cont, 1)
						}
					}
					return tree.NewDString(ret), nil
				}
				if strings.Contains(format, "\"") {
					for i := 0; i < len(dmarks); i++ {
						format = strings.Replace(format, "\"'\"", dmarks[i].Cont, 1)
					}
				}
				return tree.NewDString(format), nil
			},
			Info: "Converts int to string for compatibility with the  to_char in Postgresql",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "fNum", Typ: types.Float}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				floatNum := float64(*args[0].(*tree.DFloat))
				format := string(tree.MustBeDString(args[1]))
				dmarks := tree.FindDMark(format)
				if strings.Contains(format, "x") {
					hexRes := tree.ConvertDecToHex(floatNum, 16)
					return tree.NewDString(strings.ToLower(hexRes)), nil
				} else if strings.Contains(format, "X") {
					hexRes := tree.ConvertDecToHex(floatNum, 16)
					return tree.NewDString(strings.ToUpper(hexRes)), nil
				}
				if strings.Contains(format, "\"") {
					for i := 0; i < len(dmarks); i++ {
						format = strings.Replace(format, dmarks[i].Cont, "'", 1)
					}
				}
				if (strings.Contains(format, ".") ||
					strings.Contains(format, "D")) &&
					strings.Contains(format, "V") {
					return nil, errors.New("cannot use \"V\" and decimal point together")
				}
				tmpFloat := floatNum
				tmp := strconv.FormatFloat(math.Abs(tmpFloat), 'f', -1, 64)
				if tmp[len(tmp)-1] == '5' && len(tmp)-strings.Index(tmp, ".")-2 == 0 {
					if tmpFloat >= 0 {
						tmpFloat += 1 / math.Pow10(1)
					} else {
						tmpFloat -= 1 / math.Pow10(1)
					}
				}
				intNum, _ := strconv.Atoi(fmt.Sprintf("%.[2]*[1]f", tmpFloat, 0))
				if strings.Contains(format, "RN") {
					if strings.Contains(format, "EEEE") {
						return nil, errors.New("\"EEEE\" is incompatible with other formats")
					}
					format = tree.RN(intNum, format)
					if strings.Contains(format, "FM") {
						format = strings.ReplaceAll(strings.ReplaceAll(format, "FM", ""), " ", "")
					}
					if strings.Contains(format, "\"") {
						for i := 0; i < len(dmarks); i++ {
							format = strings.Replace(format, "\"'\"", dmarks[i].Cont, 1)
						}
					}
					return tree.NewDString(format), nil
				}
				floatNum, format = tree.FillFloatV(floatNum, format)
				markFM := strings.Contains(format, "FM")
				format, err := tree.FillFloat(floatNum, format)
				if err != nil {
					return nil, err
				}
				if strings.Contains(format, "PR") {
					var err error
					intNum := 1
					if floatNum < 0 {
						intNum = -1
					}
					format, err = tree.FillPR(intNum, format)
					if err != nil {
						return nil, err
					}
					format = strings.ReplaceAll(format, "PR", "")
				} else {
					head := 0
					for i := 0; i < len(format); i++ {
						if '0' <= format[i] && format[i] <= '9' {
							head = i
							break
						}
					}
					if floatNum < 0 {
						if strings.Contains(format, "S") ||
							strings.Contains(format, "MI") ||
							strings.Contains(format, "SG") {
							format = strings.ReplaceAll(strings.ReplaceAll(format, "SG", "-"), "MI", "-")
							format = strings.ReplaceAll(format, "S", "-")
						} else {
							format = format[:head] + "-" + format[head:]
						}
					}
					if markFM {
						format = strings.ReplaceAll(format, "MI", "")
					}
					if !markFM && floatNum >= 0 {
						if strings.Contains(format, "S") ||
							strings.Contains(format, "MI") ||
							strings.Contains(format, "SG") {
							format = strings.ReplaceAll(strings.ReplaceAll(format, "SG", "+"), "MI", " ")
							format = strings.ReplaceAll(format, "S", "+")
						} else {
							format = format[:head] + " " + format[head:]
						}
					}
				}
				if strings.Contains(format, "EEEE") {
					if strings.Contains(format, "PR") ||
						strings.Contains(format, "SG") ||
						strings.Contains(format, "S") ||
						strings.Contains(format, "L") ||
						strings.Contains(format, "D") ||
						strings.Contains(format, "FM") ||
						strings.Contains(format, "MI") ||
						strings.Contains(format, "RN") ||
						strings.Contains(format, strings.ToLower("TH")) ||
						strings.Contains(format, "V") {
						return nil, fmt.Errorf("%s may only be used together with digit and decimal point patterns", "EEEE")
					}
				}
				var er error
				format, er = tree.ReplaceFloatChar(floatNum, format)
				if er != nil {
					return nil, er
				}
				if strings.Contains(format, "EEEE") {
					ret, err := tree.FloatToEEEE(floatNum, format)
					if err != nil {
						return nil, err
					}
					if strings.Contains(ret, "\"") {
						for i := 0; i < len(dmarks); i++ {
							ret = strings.Replace(ret, "\"'\"", dmarks[i].Cont, 1)
						}
					}
					return tree.NewDString(ret), nil
				}
				if strings.Contains(format, "\"") {
					for i := 0; i < len(dmarks); i++ {
						format = strings.Replace(format, "\"'\"", dmarks[i].Cont, 1)
					}
				}
				return tree.NewDString(format), nil
			},
			Info: "Converts double to string for compatibility with the  to_char in Postgresql",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "fNum", Typ: types.Decimal}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				strNum := args[0].(*tree.DDecimal).String()
				format := string(tree.MustBeDString(args[1]))
				dmarks := tree.FindDMark(format)
				floatNum, _ := args[0].(*tree.DDecimal).Float64()
				if strings.Contains(format, "x") {
					hexRes := tree.ConvertDecToHex(floatNum, 16)
					return tree.NewDString(strings.ToLower(hexRes)), nil
				} else if strings.Contains(format, "X") {
					hexRes := tree.ConvertDecToHex(floatNum, 16)
					return tree.NewDString(strings.ToUpper(hexRes)), nil
				}
				if strings.Contains(format, "\"") {
					for i := 0; i < len(dmarks); i++ {
						format = strings.Replace(format, dmarks[i].Cont, "'", 1)
					}
				}
				if strings.Contains(strNum, ".") {
					floatNum, _ := args[0].(*tree.DDecimal).Float64()
					if (strings.Contains(format, ".") ||
						strings.Contains(format, "D")) &&
						strings.Contains(format, "V") {
						return nil, errors.New("cannot use \"V\" and decimal point together")
					}
					tmpFloat := floatNum
					tmp := strconv.FormatFloat(math.Abs(tmpFloat), 'f', -1, 64)
					if tmp[len(tmp)-1] == '5' && len(tmp)-strings.Index(tmp, ".")-2 == 0 {
						if tmpFloat >= 0 {
							tmpFloat += 1 / math.Pow10(1)
						} else {
							tmpFloat -= 1 / math.Pow10(1)
						}
					}
					intNum, _ := strconv.Atoi(fmt.Sprintf("%.[2]*[1]f", tmpFloat, 0))
					if strings.Contains(format, "RN") {
						if strings.Contains(format, "EEEE") {
							return nil, errors.New("\"EEEE\" is incompatible with other formats")
						}

						format = tree.RN(intNum, format)
						if strings.Contains(format, "FM") {
							format = strings.ReplaceAll(strings.ReplaceAll(format, "FM", ""), " ", "")
						}
						if strings.Contains(format, "\"") {
							for i := 0; i < len(dmarks); i++ {
								format = strings.Replace(format, "\"'\"", dmarks[i].Cont, 1)
							}
						}
						return tree.NewDString(format), nil
					}
					floatNum, format = tree.FillFloatV(floatNum, format)
					markFM := strings.Contains(format, "FM")
					if !strings.Contains(format, "EEEE") {
						var err error
						format, err = tree.FillFloat(floatNum, format)
						if err != nil {
							return nil, err
						}
					}
					if strings.Contains(format, "PR") {
						var err error
						intNum := 1
						if floatNum < 0 {
							intNum = -1
						}
						format, err = tree.FillPR(intNum, format)
						if err != nil {
							return nil, err
						}
						format = strings.ReplaceAll(format, "PR", "")
					} else {
						head := 0
						for i := 0; i < len(format); i++ {
							if '0' <= format[i] && format[i] <= '9' || format[i] == '.' {
								head = i
								break
							}
						}
						if floatNum < 0 {
							if strings.Contains(format, "S") ||
								strings.Contains(format, "MI") ||
								strings.Contains(format, "SG") {
								format = strings.ReplaceAll(strings.ReplaceAll(format, "SG", "-"), "MI", "-")
								format = strings.ReplaceAll(format, "S", "-")
							} else {
								format = format[:head] + "-" + format[head:]
							}
						}
						if markFM {
							format = strings.ReplaceAll(format, "MI", "")
						}
						if !markFM && floatNum >= 0 {
							if strings.Contains(format, "S") ||
								strings.Contains(format, "MI") ||
								strings.Contains(format, "SG") {
								format = strings.ReplaceAll(strings.ReplaceAll(format, "SG", "+"), "MI", " ")
								format = strings.ReplaceAll(format, "S", "+")
							} else {
								format = format[:head] + " " + format[head:]
							}
						}
					}
					if strings.Contains(format, "EEEE") {
						if strings.Contains(format, "PR") ||
							strings.Contains(format, "SG") ||
							strings.Contains(format, "S") ||
							strings.Contains(format, "L") ||
							strings.Contains(format, "D") ||
							strings.Contains(format, "FM") ||
							strings.Contains(format, "MI") ||
							strings.Contains(format, "RN") ||
							strings.Contains(format, strings.ToLower("TH")) ||
							strings.Contains(format, "V") {
							return nil, fmt.Errorf("%s may only be used together with digit and decimal point patterns", "EEEE")
						}
					}
					var er error
					format, er = tree.ReplaceFloatChar(floatNum, format)
					if er != nil {
						return nil, er
					}
					if strings.Contains(format, "EEEE") {
						ret, err := tree.FloatToEEEE(floatNum, format)
						if err != nil {
							return nil, err
						}
						if strings.Contains(ret, "\"") {
							for i := 0; i < len(dmarks); i++ {
								ret = strings.Replace(ret, "\"'\"", dmarks[i].Cont, 1)
							}
						}
						return tree.NewDString(ret), nil
					}
					if strings.Contains(format, "\"") {
						for i := 0; i < len(dmarks); i++ {
							format = strings.Replace(format, "\"'\"", dmarks[i].Cont, 1)
						}
					}
					return tree.NewDString(format), nil
				}
				intNum64, _ := args[0].(*tree.DDecimal).Int64()
				intNum := int(intNum64)
				if (strings.Contains(format, ".") ||
					strings.Contains(format, "D")) &&
					strings.Contains(format, "V") {
					return nil, errors.New("cannot use \"V\" and decimal point together")
				}
				if strings.Contains(format, "RN") {
					if strings.Contains(format, "EEEE") {
						return nil, errors.New("\"EEEE\" is incompatible with other formats")
					}
					format = tree.RN(intNum, format)
					if strings.Contains(format, "FM") {
						format = strings.ReplaceAll(strings.ReplaceAll(format, "FM", ""), " ", "")
					}
					if strings.Contains(format, "\"") {
						for i := 0; i < len(dmarks); i++ {
							format = strings.Replace(format, "\"'\"", dmarks[i].Cont, 1)
						}
					}
					return tree.NewDString(format), nil
				}
				intNum, format = tree.FillV(intNum, format)
				markFM := strings.Contains(format, "FM")
				format = tree.FillNum(intNum, format)
				if strings.Contains(format, "PR") {
					var err error
					format, err = tree.FillPR(intNum, format)
					if err != nil {
						return nil, err
					}
					format = strings.ReplaceAll(format, "PR", "")
				} else {
					head := 0
					for i := 0; i < len(format); i++ {
						if '0' <= format[i] && format[i] <= '9' {
							head = i
							break
						}
					}
					if intNum < 0 {
						if strings.Contains(format, "S") ||
							strings.Contains(format, "MI") ||
							strings.Contains(format, "SG") {
							format = strings.ReplaceAll(strings.ReplaceAll(format, "SG", "-"), "MI", "-")
							format = strings.ReplaceAll(format, "S", "-")
						} else {
							format = format[:head] + "-" + format[head:]
						}
					}
					if markFM {
						format = strings.ReplaceAll(format, "MI", "")
					}
					if !markFM && intNum >= 0 {
						if strings.Contains(format, "S") ||
							strings.Contains(format, "MI") ||
							strings.Contains(format, "SG") {
							format = strings.ReplaceAll(strings.ReplaceAll(format, "SG", "+"), "MI", " ")
							format = strings.ReplaceAll(format, "S", "+")
						} else {
							format = format[:head] + " " + format[head:]
						}
					}
				}
				if strings.Contains(format, "EEEE") {
					if strings.Contains(format, "PR") ||
						strings.Contains(format, "SG") ||
						strings.Contains(format, "S") ||
						strings.Contains(format, "L") ||
						strings.Contains(format, "D") ||
						strings.Contains(format, "FM") ||
						strings.Contains(format, "MI") ||
						strings.Contains(format, "RN") ||
						strings.Contains(format, strings.ToLower("TH")) ||
						strings.Contains(format, "V") {
						return nil, fmt.Errorf("%s may only be used together with digit and decimal point patterns", "EEEE")
					}
				}
				format, err := tree.ReplaceChar(intNum, format)
				if err != nil {
					return nil, err
				}
				if strings.Contains(format, "EEEE") {
					ret, err := tree.IntToEEEE(intNum, format)
					if err != nil {
						return nil, err
					}
					if strings.Contains(ret, "\"") {
						for i := 0; i < len(dmarks); i++ {
							ret = strings.Replace(ret, "\"'\"", dmarks[i].Cont, 1)
						}
					}
					return tree.NewDString(ret), nil
				}
				if strings.Contains(format, "\"") {
					for i := 0; i < len(dmarks); i++ {
						format = strings.Replace(format, "\"'\"", dmarks[i].Cont, 1)
					}
				}
				return tree.NewDString(format), nil

			},
			Info: "Converts numeric to string for compatibility with the  to_char in Postgresql",
		},
	),
	"starts_with": makeBuiltin(tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.String}, {Name: "prefix", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := []rune(string(tree.MustBeDString(args[0])))
				prefix := []rune(tree.MustBeDString(args[1]))
				if len(prefix) > len(str) {
					return tree.MakeDBool(false), nil
				}
				for i, v := range prefix {
					if v != str[i] {
						return tree.MakeDBool(false), nil
					}
				}
				return tree.MakeDBool(true), nil
			},
			Info: "判断字符串是否以prefix开头, 是则返回真, 否则为假.",
		},
	),
	"local_time": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Time),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				now := timeutil.Now()
				current := ctx.SessionData.DataConversion.Location
				timeTZ := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), current)
				_, offset := timeTZ.Zone()
				return tree.MakeDTime(timeofday.FromTime(timeTZ.Add(time.Duration(offset) * time.Second))), nil
			},
			Info: "当前时间（一天中的时间）",
		},
	),

	"local_timestamp": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				now := timeutil.Now()
				current := ctx.SessionData.DataConversion.Location
				timeTZ := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), current)
				_, offset := timeTZ.Zone()
				return tree.MakeDTimestamp(timeTZ.Add(time.Duration(offset)*time.Second), time.Microsecond), nil
			},
			Info: "当前事务的日期和时间",
		},
	),

	"make_time": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "hour", Typ: types.Int},
				{Name: "min", Typ: types.Int},
				{Name: "sec", Typ: types.Decimal},
			},
			ReturnType: tree.FixedReturnType(types.Time),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				hour := int(tree.MustBeDInt(args[0]))
				min := int(tree.MustBeDInt(args[1]))
				tmpSec := tree.MustBeDDecimal(args[2])
				sec, err := tmpSec.Float64()
				if err != nil {
					return nil, err
				}
				t, err := tree.ParseNumToTime(1, 1, 1, hour, min, sec, "time", "")
				if err != nil {
					return nil, err
				}
				return tree.MakeDTime(timeofday.FromTime(t)), nil
			},
			Info: "从时,分,秒域创建时间",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "hour", Typ: types.Int},
				{Name: "min", Typ: types.Int},
				{Name: "sec", Typ: types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Time),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				hour := int(tree.MustBeDInt(args[0]))
				min := int(tree.MustBeDInt(args[1]))
				sec := float64(tree.MustBeDFloat(args[2]))
				t, err := tree.ParseNumToTime(1, 1, 1, hour, min, sec, "time", "")
				if err != nil {
					return nil, err
				}
				return tree.MakeDTime(timeofday.FromTime(t)), nil
			},
			Info: "从时,分,秒域创建时间",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "hour", Typ: types.Int},
				{Name: "min", Typ: types.Int},
				{Name: "sec", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Time),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				hour := int(tree.MustBeDInt(args[0]))
				min := int(tree.MustBeDInt(args[1]))
				sec := int(tree.MustBeDInt(args[2]))
				t, err := tree.ParseNumToTime(1, 1, 1, hour, min, float64(sec), "time", "")
				if err != nil {
					return nil, err
				}
				return tree.MakeDTime(timeofday.FromTime(t)), nil
			},
			Info: "从时,分,秒域创建时间",
		},
	),

	"make_timestamp": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "year", Typ: types.Int},
				{Name: "mon", Typ: types.Int},
				{Name: "day", Typ: types.Int},
				{Name: "hour", Typ: types.Int},
				{Name: "min", Typ: types.Int},
				{Name: "sec", Typ: types.Decimal},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				year, mon, day, hour, min, sec, _, err := parseTimestamp(args)
				if err != nil {
					return nil, err
				}
				t, err := tree.ParseNumToTime(year, mon, day, hour, min, sec, "utc", "")
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(t, time.Microsecond), nil
			},
			Info: "将年,月,日,时,分,秒创建为时间戳: year-mon-day hour:min:sec",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "year", Typ: types.Int},
				{Name: "mon", Typ: types.Int},
				{Name: "day", Typ: types.Int},
				{Name: "hour", Typ: types.Int},
				{Name: "min", Typ: types.Int},
				{Name: "sec", Typ: types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				year, mon, day, hour, min, sec, _, err := parseTimestamp(args)
				if err != nil {
					return nil, err
				}
				t, err := tree.ParseNumToTime(year, mon, day, hour, min, sec, "utc", "")
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(t, time.Microsecond), nil
			},
			Info: "将年,月,日,时,分,秒创建为时间戳: year-mon-day hour:min:sec",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "year", Typ: types.Int},
				{Name: "mon", Typ: types.Int},
				{Name: "day", Typ: types.Int},
				{Name: "hour", Typ: types.Int},
				{Name: "min", Typ: types.Int},
				{Name: "sec", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				year, mon, day, hour, min, sec, _, err := parseTimestamp(args)
				if err != nil {
					return nil, err
				}
				t, err := tree.ParseNumToTime(year, mon, day, hour, min, float64(sec), "utc", "")
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(t, time.Microsecond), nil
			},
			Info: "将年,月,日,时,分,秒创建为时间戳: year-mon-day hour:min:sec",
		},
	),
	"make_timestamptz": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types: tree.VariadicType{
				FixedTypes: []types.T{types.Int, types.Int, types.Int, types.Int, types.Int, types.Decimal},
				VarType:    types.String,
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				year, mon, day, hour, min, sec, loc, err := parseTimestamp(args)
				if err != nil {
					return nil, err
				}
				current := ctx.SessionData.DataConversion.Location.String()
				t, err := tree.ParseNumToTime(year, mon, day, hour, min, sec, loc, current)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestampTZ(t, time.Microsecond), nil
			},
			Info: " 将年,月,日,时,分,秒创建带时区的时间戳: year-mon-day hour:min:sec+xx,如果没有指定timezone, 则使用当前时区",
		},
		tree.Overload{
			Types: tree.VariadicType{
				FixedTypes: []types.T{types.Int, types.Int, types.Int, types.Int, types.Int, types.Float},
				VarType:    types.String,
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				year, mon, day, hour, min, sec, loc, err := parseTimestamp(args)
				current := ctx.SessionData.DataConversion.Location.String()
				t, err := tree.ParseNumToTime(year, mon, day, hour, min, sec, loc, current)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestampTZ(t, time.Microsecond), nil
			},
			Info: " 将年,月,日,时,分,秒创建带时区的时间戳: year-mon-day hour:min:sec+xx,如果没有指定timezone, 则使用当前时区",
		},
		tree.Overload{
			Types: tree.VariadicType{
				FixedTypes: []types.T{types.Int, types.Int, types.Int, types.Int, types.Int, types.Int},
				VarType:    types.String,
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				year, mon, day, hour, min, sec, loc, err := parseTimestamp(args)
				current := ctx.SessionData.DataConversion.Location.String()
				t, err := tree.ParseNumToTime(year, mon, day, hour, min, sec, loc, current)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestampTZ(t, time.Microsecond), nil
			},
			Info: " 将年,月,日,时,分,秒创建带时区的时间戳: year-mon-day hour:min:sec+xx,如果没有指定timezone, 则使用当前时区",
		},
	),

	"make_interval": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Any},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(args) > 7 {
					return tree.DNull, errors.New("the args should year month week day hour mins secs")
				}
				mon, day, nano, err := tree.ParseArg(args)
				if err != nil {
					return nil, err
				}
				ret := newDInterval(int64(mon), int64(day), int64(nano))
				return &ret, nil
			},
			Info: "将年,月,日,时,分,秒创建为时间戳: year-mon-day hour:min:sec",
		},
	),

	"num_nonnulls": makeBuiltin(
		tree.FunctionProperties{
			Category:     categorySystemInfo,
			Impure:       true,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Any},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				NullNumber := 0
				for _, v := range args {
					if v == tree.DNull {
						NullNumber++
					}
				}
				return tree.NewDInt(tree.DInt(len(args) - NullNumber)), nil
			},
			Info: "return nonull numbers",
		},
	),
	"num_nulls": makeBuiltin(
		tree.FunctionProperties{
			Category:     categorySystemInfo,
			Impure:       true,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Any},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				NullNumber := 0
				for _, v := range args {
					if v == tree.DNull {
						NullNumber++
					}
				}
				return tree.NewDInt(tree.DInt(NullNumber)), nil
			},
			Info: "return null numbers",
		},
	),

	"date_part": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "format", Typ: types.String}, {Name: "timestamp", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				format := string(tree.MustBeDString(args[0]))
				timestamp := args[1].(*tree.DTimestamp).Time
				ret, err := tree.DatepartTimestamp(format, timestamp)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(ret)), nil
			},
			Info: "apply timestamp type to date_part for Postgresql",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "format", Typ: types.String}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				res := args[1].String()
				res = res[1 : len(res)-1]
				format := string(tree.MustBeDString(args[0]))
				ret, err := tree.DatePartInterval(format, res)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(ret)), nil
			},
			Info: "apply (interval:timestamp-timestamp) type to date_part for Postgresql",
		},

		tree.Overload{
			Types:      tree.ArgTypes{{Name: "format", Typ: types.String}, {Name: "timestamptz", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				format := string(tree.MustBeDString(args[0]))
				timestamptz := args[1].(*tree.DTimestampTZ).Time
				ret, err := tree.DatepartTimestamp(format, timestamptz)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(ret)), nil
			},
			Info: "apply timestamptz type to date_part for Postgresql",
		},
	),

	// date_format 根据字符串格式格式化日期值
	"date_format": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamp", Typ: types.Timestamp}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestamp).Time
				format := string(tree.MustBeDString(args[1]))
				ret, err := tree.ParseDDateToFormat(timeStamp, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamp", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeDatum, err := tree.PerformCast(evalCtx, args[0], coltypes.TimestampTZ)
				if err != nil {
					return nil, err
				}
				timeStamp := tree.MustBeDTimestampTZ(timeDatum).Time
				format := string(tree.MustBeDString(args[1]))
				_, offset := evalCtx.StmtTimestamp.In(evalCtx.GetLocation()).Zone()
				offsetNS := offset * int(math.Pow(10, 9))
				timeStamp = timeStamp.Add(time.Duration(offsetNS))
				ret, err := tree.ParseDDateToFormat(timeStamp, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamp", Typ: types.TimestampTZ}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestampTZ).Time
				format := string(tree.MustBeDString(args[1]))
				_, offset := evalCtx.StmtTimestamp.In(evalCtx.GetLocation()).Zone()
				_, offset1 := timeStamp.Zone()
				offset = offset - offset1
				offsetNS := offset * int(math.Pow(10, 9))
				timeStamp = timeStamp.Add(time.Duration(offsetNS))
				ret, err := tree.ParseDDateToFormat(timeStamp, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamp", Typ: types.Date}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeDatum, _ := tree.PerformCast(evalCtx, args[0], coltypes.Timestamp)
				timeStamp := tree.MustBeDTimestamp(timeDatum).Time
				format := string(tree.MustBeDString(args[1]))
				ret, err := tree.ParseDDateToFormat(timeStamp, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
		},
	),

	"justify_hours": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				argInterval, ok := args[0].(*tree.DInterval)
				if !ok {
					return nil, fmt.Errorf("error happens in function 'justify_hours'")
				}
				var wholeDay int64
				nano := argInterval.Nanos()
				days := argInterval.Days
				months := argInterval.Months

				nano, wholeDay = justifyTime(nano, nsPerDay)
				days += wholeDay

				if days > 0 && nano < 0 {
					nano += nsPerDay
					days--
				} else if days < 0 && nano > 0 {
					nano -= nsPerDay
					days++
				}

				ret := newDInterval(months, days, nano)
				return &ret, nil
			},
			Info: "apply justify_hours for Postgresql",
		},
	),

	"justify_days": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				argInterval, ok := args[0].(*tree.DInterval)
				if !ok {
					return nil, fmt.Errorf("error happens in function 'justify_days'")
				}
				var wholeMonth int64
				nano := argInterval.Nanos()
				days := argInterval.Days
				months := argInterval.Months

				days, wholeMonth = justifyTime(days, daysPerMonth)
				months += wholeMonth

				if months > 0 && days < 0 {
					days += daysPerMonth
					months--
				} else if months < 0 && days > 0 {
					days -= daysPerMonth
					months++
				}

				ret := newDInterval(months, days, nano)
				return &ret, nil
			},
			Info: "apply justify_hours for Postgresql",
		},
	),

	"justify_interval": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				argInterval, ok := args[0].(*tree.DInterval)
				if !ok {
					return nil, fmt.Errorf("error happens in function 'justify_interval'")
				}
				var wholeDay, wholeMonth int64
				nano := argInterval.Nanos()
				days := argInterval.Days
				months := argInterval.Months

				nano, wholeDay = justifyTime(nano, nsPerDay)
				days += wholeDay

				days, wholeMonth = justifyTime(days, daysPerMonth)
				months += wholeMonth

				if months > 0 && (days < 0 || (days == 0 && nano < 0)) {
					days += daysPerMonth
					months--
				} else if months < 0 && (days > 0 || (days == 0 && nano > 0)) {
					days -= daysPerMonth
					months++
				}
				if days > 0 && nano < 0 {
					nano += nsPerDay
					days--
				} else if days < 0 && nano > 0 {
					nano -= nsPerDay
					days++
				}

				ret := newDInterval(months, days, nano)
				return &ret, nil
			},
			Info: "apply justify_hours for Postgresql",
		},
	),

	"make_date": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "yeas", Typ: types.Int}, {Name: "mons", Typ: types.Int}, {Name: "days", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				years := int(tree.MustBeDInt(args[0]))
				mons := int(tree.MustBeDInt(args[1]))
				days := int(tree.MustBeDInt(args[2]))
				ret, err := tree.MakeDate(years, mons, days)
				if err != nil {
					return nil, err
				}
				return ret, nil
			},
			Info: "apply make_date for Postgresql",
		},
	),

	"regexp_split_to_array": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "string", Typ: types.String},
				{Name: "pattern", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.String}),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return regexpSplitToArray(ctx, args, false /* hasFlags */)
			},
			Info: "Split string using a POSIX regular expression as the delimiter.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "string", Typ: types.String},
				{Name: "pattern", Typ: types.String},
				{Name: "flags", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.String}),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return regexpSplitToArray(ctx, args, true /* hasFlags */)
			},
			Info: "Split string using a POSIX regular expression as the delimiter with flags.",
		},
	),

	"regexp_match": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}, {Name: "regex", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.String}),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				return regexpMatch(ctx, s, pattern, `\`)
			},
			Info: "Returns all the  matches for the Regular Expression `regex` in `input`.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "flags", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.String}),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				sqlFlags := string(tree.MustBeDString(args[2]))
				result, err := regexpMatchFlag(evalCtx, s, pattern, sqlFlags)
				if err != nil {
					return nil, err
				}
				return result, nil
			},
			Info: "print all the matches for the regular expression `regex` in `input` with the regular " +
				"expression `regexp_match` using `flags`.",
		},
	),

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var buffer bytes.Buffer
				length := 0
				for _, d := range args {
					if d == tree.DNull {
						continue
					}
					length += len(string(tree.MustBeDString(d)))
					if length > maxAllocatedStringSize {
						return nil, errStringTooLarge
					}
					buffer.WriteString(string(tree.MustBeDString(d)))
				}
				return tree.NewDString(buffer.String()), nil
			},
			Info: "Concatenates a comma-separated list of strings.",
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var buffer bytes.Buffer
				length := 0
				for _, d := range args {
					if d == tree.DNull {
						continue
					}
					length += len(string(tree.MustBeDBytes(d)))
					if length > maxAllocatedStringSize {
						return nil, errStringTooLarge
					}
					buffer.WriteString(string(tree.MustBeDBytes(d)))
				}
				return tree.NewDString(buffer.String()), nil
			},
			Info: "Concatenates a comma-separated list of strings.",
		},
	),
	"client_encoding": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Name),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(evalCtx.SessionData.ClientEncoding), nil
			},
			Info: "output client encoding name.",
		},
	),

	"isfinite": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamp", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timestamp := args[0].(*tree.DTimestamp).Time
				str := timestamp.String()
				if str == "-292277022365-05-08 08:17:07.854776 +0000 UTC" || str == "292277026304-08-26 15:42:51.145224 +0000 UTC" {
					return tree.MakeDBool(false), nil
				}
				return tree.MakeDBool(true), nil
			},
			Info: "judge timestamp if is infinity",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {

				return tree.MakeDBool(true), nil
			},
			Info: "judge interval if is infinity",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				infiniteDate := args[0].(*tree.DDate).String()
				if infiniteDate == "'292277026304-08-26'" || infiniteDate == "'-292277022365-05-08'" {
					return tree.MakeDBool(false), nil
				}
				return tree.MakeDBool(true), nil
			},
			Info: "judge date if is infinity",
		},
	),

	"format": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Any},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var buffer bytes.Buffer
				if len(args) == 0 {
					return nil, errors.New("you should input one argument at least")
				}
				if _, ok := args[0].(*tree.DString); !ok { //第一个参数不是字符串报错
					return nil, errors.New("the 1st argument isn't string type")
				}

				words, err := tree.CutString(string(tree.MustBeDString(args[0]))) //处理format字符串
				if err != nil {
					return nil, err
				}
				for _, v := range words {
					if !v.IsPlaceholder {
						buffer.WriteString(v.Name)
						continue
					}
					result, err := tree.JudgeType(ctx, words, v, args) //将占位符替换为相应的格式化参数
					if err != nil {
						return nil, err
					}
					buffer.WriteString(result)
				}

				return tree.NewDString(buffer.String()), nil
			},
			Info: "Format output.",
		},
	),
	"concat_ws": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(args) == 0 {
					return nil, pgerror.NewErrorf(pgcode.UndefinedFunction, errInsufficientArgsFmtString, "concat_ws")
				}
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				sep := string(tree.MustBeDString(args[0]))
				var buf bytes.Buffer
				prefix := ""
				length := 0
				for _, d := range args[1:] {
					if d == tree.DNull {
						continue
					}
					length += len(prefix) + len(string(tree.MustBeDString(d)))
					if length > maxAllocatedStringSize {
						return nil, errStringTooLarge
					}
					// Note: we can't use the range index here because that
					// would break when the 2nd argument is NULL.
					buf.WriteString(prefix)
					prefix = sep
					buf.WriteString(string(tree.MustBeDString(d)))
				}
				return tree.NewDString(buf.String()), nil
			},
			Info: "Uses the first argument as a separator between the concatenation of the " +
				"subsequent arguments. \n\nFor example `concat_ws('!','wow','great')` " +
				"returns `wow!great`.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-string.html#FUNCTIONS-STRING-OTHER
	"convert_from": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.Bytes}, {Name: "enc", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := []byte(tree.MustBeDBytes(args[0]))
				enc := CleanEncodingName(string(tree.MustBeDString(args[1])))
				switch enc {
				// All the following are aliases to each other in PostgreSQL.
				case "utf8", "unicode", "cp65001":
					if !utf8.Valid(str) {
						return nil, newDecodeError("UTF8")
					}
					return tree.NewDString(string(str)), nil

					// All the following are aliases to each other in PostgreSQL.
				case "latin1", "iso88591", "cp28591":
					var buf strings.Builder
					for _, c := range str {
						buf.WriteRune(rune(c))
					}
					return tree.NewDString(buf.String()), nil
				}
				return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue,
					"invalid source encoding name %q", enc)
			},
			Info: "Decode the bytes in `str` into a string using encoding `enc`. " +
				"Supports encodings 'UTF8' and 'LATIN1'.",
		}),

	// https://www.postgresql.org/docs/10/static/functions-string.html#FUNCTIONS-STRING-OTHER
	"convert_to": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.String}, {Name: "enc", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDString(args[0]))
				enc := CleanEncodingName(string(tree.MustBeDString(args[1])))
				switch enc {
				// All the following are aliases to each other in PostgreSQL.
				case "utf8", "unicode", "cp65001":
					return tree.NewDBytes(tree.DBytes([]byte(str))), nil

					// All the following are aliases to each other in PostgreSQL.
				case "latin1", "iso88591", "cp28591":
					res := make([]byte, 0, len(str))
					for _, c := range str {
						if c > 255 {
							return nil, newEncodeError(c, "LATIN1")
						}
						res = append(res, byte(c))
					}
					return tree.NewDBytes(tree.DBytes(res)), nil
				}
				return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue,
					"invalid destination encoding name %q", enc)
			},
			Info: "Encode the string `str` as a byte array using encoding `enc`. " +
				"Supports encodings 'UTF8' and 'LATIN1'.",
		}),

	"gen_random_uuid": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryIDGeneration,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.UUID),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				uv := uuid.MakeV4()
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info: "Generates a random UUID and returns it as a value of UUID type.",
		},
	),

	"to_uuid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				uv, err := uuid.FromString(s)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(uv.GetBytes())), nil
			},
			Info: "Converts the character string representation of a UUID to its byte string " +
				"representation.",
		},
	),

	"from_uuid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := []byte(*args[0].(*tree.DBytes))
				uv, err := uuid.FromBytes(b)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(uv.String()), nil
			},
			Info: "Converts the byte string representation of a UUID to its character string " +
				"representation.",
		},
	),

	"to_number": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				res, err := tree.ParseDDecimal(s)
				if err != nil {
					return nil, err
				}
				return res, nil
			},
			Info: "Converts a string representation to a numeric value,",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "fNum", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				strNum := strings.ToUpper(string(tree.MustBeDString(args[0])))
				format := strings.ToUpper(string(tree.MustBeDString(args[1])))
				if strings.Contains(format, "X") {
					parseRes, err := strconv.ParseInt(strings.ToLower(strNum), 16, 64)
					if err == nil {
						return tree.PerformCast(ctx, tree.NewDInt(tree.DInt(parseRes)), coltypes.Decimal)
					}
					convertRes, err := tree.ConvertHexTODeci(strings.ToLower(strNum), 16)
					if err != nil {
						return nil, err
					}
					convertDres, err := tree.PerformCast(ctx, tree.NewDFloat(tree.DFloat(convertRes)), coltypes.Decimal)
					if err == nil {
						return convertDres, nil
					}
				}
				res, err := tree.StrToNum(strNum, format)
				if err != nil {
					return nil, err
				}
				if res == "" || res == "-" || res == "+" || res == "." {
					return nil, fmt.Errorf("invalid input syntax for type decimal")
				}
				DDres, err := tree.ParseDDecimal(res)
				if err != nil {
					return nil, err
				}
				if strings.Contains(format, "V") {
					index := strings.Index(format, "V")
					var n float64
					for _, val := range format[index:] {
						d, _ := strconv.Atoi(string(val))
						if d == 9 {
							n++
						}
					}
					Sres := fmt.Sprint(DDres)
					Fres, err := strconv.ParseFloat(Sres, 64)
					if err != nil {
						return nil, err
					}
					FFres := Fres / math.Pow(10, n)
					Dfres, err := tree.PerformCast(ctx, tree.NewDFloat(tree.DFloat(FFres)), coltypes.Decimal)
					if err != nil {
						return nil, err
					}
					return Dfres, nil
				}
				return DDres, nil
			},
			Info: "Converts string to a number ",
		},
	),

	// The following functions are all part of the NET address functions. They can
	// be found in the postgres reference at https://www.postgresql.org/docs/9.6/static/functions-net.html#CIDR-INET-FUNCTIONS-TABLE
	// This includes:
	// - abbrev
	// - broadcast
	// - family
	// - host
	// - hostmask
	// - masklen
	// - netmask
	// - set_masklen
	// - text(inet)
	// - inet_same_family

	"abbrev": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				return tree.NewDString(dIPAddr.IPAddr.String()), nil
			},
			Info: "Converts the combined IP address and prefix length to an abbreviated display format as text." +
				"For INET types, this will omit the prefix length if it's not the default (32 or IPv4, 128 for IPv6)" +
				"\n\nFor example, `abbrev('192.168.1.2/24')` returns `'192.168.1.2/24'`",
		},
	),
	"aes_encrypt": makeBuiltin(tree.FunctionProperties{NullableArgs: false},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "plaintext", Typ: types.Any}, {Name: "key", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var data []byte
				if args[0].ResolvedType() == types.Bytes {
					data = []byte(tree.MustBeDBytes(args[0]))
				} else {
					plaintext, err := tree.PerformCast(evalCtx, args[0], coltypes.String)
					if err != nil {
						return nil, fmt.Errorf("Please enter the correct plaintext type")
					}
					data = tree.Str2bytes(string(tree.MustBeDString(plaintext)))
				}
				inKey, err := tree.PerformCast(evalCtx, args[1], coltypes.String)
				if err != nil {
					return nil, fmt.Errorf("Please enter the correct key type")
				}
				key := tree.Str2bytes(string(tree.MustBeDString(inKey)))
				res := tree.Encrypt(data, key)
				return tree.NewDString(fmt.Sprintf("%x", res)), nil
			},
			Info: "Encrypt data using the official AES-128-ECB algorithm.",
		},
	),

	"aes_decrypt": makeBuiltin(tree.FunctionProperties{NullableArgs: false},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "ciphertext", Typ: types.String}, {Name: "key", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				data, be := string(tree.MustBeDString(args[0])), sessiondata.BytesEncodeHex
				unhexMi, err := lex.DecodeRawBytesToByteArray(data, be)
				if err != nil || len(unhexMi)%16 != 0 || len(unhexMi) == 0 {
					return tree.DNull, nil
				}
				inKey, err := tree.PerformCast(evalCtx, args[1], coltypes.String)
				if err != nil {
					return tree.DNull, fmt.Errorf("Please enter the correct key type")
				}
				key := tree.Str2bytes(string(tree.MustBeDString(inKey)))
				res := string(tree.Decrypt(unhexMi, key))
				return tree.NewDString(res), nil
			},
			Info: "Decrypt data using the official AES-128-ECB algorithm.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "ciphertext", Typ: types.Bytes}, {Name: "key", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				cipher := string(tree.MustBeDBytes(args[0]))
				be := sessiondata.BytesEncodeHex
				unhexCip, err := lex.DecodeRawBytesToByteArray(cipher, be)
				if err != nil || len(unhexCip)%16 != 0 || len(unhexCip) == 0 {
					return tree.DNull, nil
				}
				inKey, err := tree.PerformCast(evalCtx, args[1], coltypes.String)
				if err != nil {
					return tree.DNull, fmt.Errorf("Please enter the correct key type")
				}
				key := tree.Str2bytes(string(tree.MustBeDString(inKey)))
				res := string(tree.Decrypt(unhexCip, key))
				return tree.NewDString(res), nil
			},
			Info: "Decrypt data using the official AES-128-ECB algorithm.",
		},
	),

	"broadcast": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				broadcastIPAddr := dIPAddr.IPAddr.Broadcast()
				return &tree.DIPAddr{IPAddr: broadcastIPAddr}, nil
			},
			Info: "Gets the broadcast address for the network address represented by the value." +
				"\n\nFor example, `broadcast('192.168.1.2/24')` returns `'192.168.1.255/24'`",
		},
	),

	"family": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				if dIPAddr.Family == ipaddr.IPv4family {
					return tree.NewDInt(tree.DInt(4)), nil
				}
				return tree.NewDInt(tree.DInt(6)), nil
			},
			Info: "Extracts the IP family of the value; 4 for IPv4, 6 for IPv6." +
				"\n\nFor example, `family('::1')` returns `6`",
		},
	),

	"host": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				s := dIPAddr.IPAddr.String()
				if i := strings.IndexByte(s, '/'); i != -1 {
					return tree.NewDString(s[:i]), nil
				}
				return tree.NewDString(s), nil
			},
			Info: "Extracts the address part of the combined address/prefixlen value as text." +
				"\n\nFor example, `host('192.168.1.2/16')` returns `'192.168.1.2'`",
		},
	),

	"hostmask": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				ipAddr := dIPAddr.IPAddr.Hostmask()
				return &tree.DIPAddr{IPAddr: ipAddr}, nil
			},
			Info: "Creates an IP host mask corresponding to the prefix length in the value." +
				"\n\nFor example, `hostmask('192.168.1.2/16')` returns `'0.0.255.255'`",
		},
	),

	"masklen": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				return tree.NewDInt(tree.DInt(dIPAddr.Mask)), nil
			},
			Info: "Retrieves the prefix length stored in the value." +
				"\n\nFor example, `masklen('192.168.1.2/16')` returns `16`",
		},
	),

	"netmask": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				ipAddr := dIPAddr.IPAddr.Netmask()
				return &tree.DIPAddr{IPAddr: ipAddr}, nil
			},
			Info: "Creates an IP network mask corresponding to the prefix length in the value." +
				"\n\nFor example, `netmask('192.168.1.2/16')` returns `'255.255.0.0'`",
		},
	),

	"set_masklen": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "val", Typ: types.INet},
				{Name: "prefixlen", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				mask := int(tree.MustBeDInt(args[1]))

				if !(dIPAddr.Family == ipaddr.IPv4family && mask >= 0 && mask <= 32) && !(dIPAddr.Family == ipaddr.IPv6family && mask >= 0 && mask <= 128) {
					return nil, pgerror.NewErrorf(
						pgcode.InvalidParameterValue, "invalid mask length: %d", mask)
				}
				return &tree.DIPAddr{IPAddr: ipaddr.IPAddr{Family: dIPAddr.Family, Addr: dIPAddr.Addr, Mask: byte(mask)}}, nil
			},
			Info: "Sets the prefix length of `val` to `prefixlen`.\n\n" +
				"For example, `set_masklen('192.168.1.2', 16)` returns `'192.168.1.2/16'`.",
		},
	),

	"text": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				s := dIPAddr.IPAddr.String()
				// Ensure the string has a "/mask" suffix.
				if strings.IndexByte(s, '/') == -1 {
					s += "/" + strconv.Itoa(int(dIPAddr.Mask))
				}
				return tree.NewDString(s), nil
			},
			Info: "Converts the IP address and prefix length to text.",
		},
	),

	"inet_same_family": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "val", Typ: types.INet},
				{Name: "val", Typ: types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				first := tree.MustBeDIPAddr(args[0])
				other := tree.MustBeDIPAddr(args[1])
				return tree.MakeDBool(tree.DBool(first.Family == other.Family)), nil
			},
			Info: "Checks if two IP addresses are of the same IP family.",
		},
	),

	"inet_contained_by_or_equals": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "val", Typ: types.INet},
				{Name: "container", Typ: types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipAddr := tree.MustBeDIPAddr(args[0]).IPAddr
				other := tree.MustBeDIPAddr(args[1]).IPAddr
				return tree.MakeDBool(tree.DBool(ipAddr.ContainedByOrEquals(&other))), nil
			},
			Info: "Test for subnet inclusion or equality, using only the network parts of the addresses. " +
				"The host part of the addresses is ignored.",
		},
	),

	"inet_contains_or_contained_by": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "val", Typ: types.INet},
				{Name: "val", Typ: types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipAddr := tree.MustBeDIPAddr(args[0]).IPAddr
				other := tree.MustBeDIPAddr(args[1]).IPAddr
				return tree.MakeDBool(tree.DBool(ipAddr.ContainsOrContainedBy(&other))), nil
			},
			Info: "Test for subnet inclusion, using only the network parts of the addresses. " +
				"The host part of the addresses is ignored.",
		},
	),

	"inet_contains_or_equals": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "container", Typ: types.INet},
				{Name: "val", Typ: types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipAddr := tree.MustBeDIPAddr(args[0]).IPAddr
				other := tree.MustBeDIPAddr(args[1]).IPAddr
				return tree.MakeDBool(tree.DBool(ipAddr.ContainsOrEquals(&other))), nil
			},
			Info: "Test for subnet inclusion or equality, using only the network parts of the addresses. " +
				"The host part of the addresses is ignored.",
		},
	),

	"from_ip": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipstr := args[0].(*tree.DBytes)
				nboip := net.IP(*ipstr)
				sv := nboip.String()
				// if nboip has a length of 0, sv will be "<nil>"
				if sv == "<nil>" {
					return nil, errZeroIP
				}
				return tree.NewDString(sv), nil
			},
			Info: "Converts the byte string representation of an IP to its character string " +
				"representation.",
		},
	),

	"to_ip": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipdstr := tree.MustBeDString(args[0])
				ip := net.ParseIP(string(ipdstr))
				// If ipdstr could not be parsed to a valid IP,
				// ip will be nil.
				if ip == nil {
					return nil, pgerror.NewErrorf(
						pgcode.InvalidParameterValue, "invalid IP format: %s", ipdstr.String())
				}
				return tree.NewDBytes(tree.DBytes(ip)), nil
			},
			Info: "Converts the character string representation of an IP to its byte string " +
				"representation.",
		},
	),

	"split_part": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "input", Typ: types.String},
				{Name: "delimiter", Typ: types.String},
				{Name: "return_index_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				text := string(tree.MustBeDString(args[0]))
				sep := string(tree.MustBeDString(args[1]))
				field := int(tree.MustBeDInt(args[2]))

				if field <= 0 {
					return nil, pgerror.NewErrorf(
						pgcode.InvalidParameterValue, "field position %d must be greater than zero", field)
				}

				splits := strings.Split(text, sep)
				if field > len(splits) {
					return tree.NewDString(""), nil
				}
				return tree.NewDString(splits[field-1]), nil
			},
			Info: "Splits `input` on `delimiter` and return the value in the `return_index_pos`  " +
				"position (starting at 1). \n\nFor example, `split_part('123.456.789.0','.',3)`" +
				"returns `789`.",
		},
	),

	"repeat": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}, {Name: "repeat_counter", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				s := string(tree.MustBeDString(args[0]))
				count := int(tree.MustBeDInt(args[1]))

				ln := len(s) * count
				// Use <= here instead of < to prevent a possible divide-by-zero in the next
				// if block.
				if count <= 0 {
					count = 0
				} else if ln/count != len(s) {
					// Detect overflow and trigger an error.
					return nil, errStringTooLarge
				} else if ln > maxAllocatedStringSize {
					return nil, errStringTooLarge
				}

				return tree.NewDString(strings.Repeat(s, count)), nil
			},
			Info: "Concatenates `input` `repeat_counter` number of times.\n\nFor example, " +
				"`repeat('dog', 2)` returns `dogdog`.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-binarystring.html
	"encode": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "data", Typ: types.Bytes}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, format := *args[0].(*tree.DBytes), string(tree.MustBeDString(args[1]))
				be, ok := sessiondata.BytesEncodeFormatFromString(format)
				if !ok {
					return nil, pgerror.NewError(pgcode.InvalidParameterValue,
						"only 'hex', 'escape', and 'base64' formats are supported for encode()")
				}
				return tree.NewDString(lex.EncodeByteArrayToRawBytes(
					string(data), be, true /* skipHexPrefix */)), nil
			},
			Info: "Encodes `data` using `format` (`hex` / `escape` / `base64`).",
		},
	),

	"decode": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "text", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, format := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				be, ok := sessiondata.BytesEncodeFormatFromString(format)
				if !ok {
					return nil, pgerror.NewError(pgcode.InvalidParameterValue,
						"only 'hex', 'escape', and 'base64' formats are supported for decode()")
				}
				res, err := lex.DecodeRawBytesToByteArray(data, be)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(res)), nil
			},
			Info: "Decodes `data` using `format` (`hex` / `escape` / `base64`).",
		},
	),
	"switch": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.HomogeneousType{},
			ReturnType: tree.IdentityReturnType(2),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (datum tree.Datum, err error) {
				if len(args)%2 != 0 {
					// return nil, errors.New("use format: decode(expr, condition1,value1, condition2, value2 .....condition n, value n, default)")
					e := args[0]
					desiredType := args[2].ResolvedType()
					var whens []*tree.When
					for i := 1; i < len(args); i = i + 2 {
						if !args[i+1].ResolvedType().Equivalent(desiredType) {
							if ok, _ := tree.IsCastDeepValid(args[i+1].ResolvedType(), desiredType); ok {
								desired, err := coltypes.DatumTypeToColumnType(desiredType)
								if err != nil {
									return nil, err
								}
								args[i+1], err = tree.PerformCast(ctx, args[i+1], desired)
								if err != nil {
									return nil, err
								}
							} else {
								return nil, errors.New("use format: switch(expr, condition1,value1, condition2, value2 .....condition n, value n)")
							}
						}
						when := &tree.When{Cond: args[i], Val: args[i+1]}
						whens = append(whens, when)
					}
					expr, _ := tree.NewTypedCaseExpr(e, whens, nil, types.Unknown)
					a, err := expr.Eval(ctx)
					if err != nil {
						return nil, err
					}
					return a.Eval(ctx)
				}
				e := args[0]
				desiredType := args[2].ResolvedType()
				var whens []*tree.When
				for i := 1; i < len(args)-1; i = i + 2 {
					if !args[i+1].ResolvedType().Equivalent(desiredType) {
						if ok, _ := tree.IsCastDeepValid(args[i+1].ResolvedType(), desiredType); ok {
							desired, err := coltypes.DatumTypeToColumnType(desiredType)
							if err != nil {
								return nil, err
							}
							args[i+1], err = tree.PerformCast(ctx, args[i+1], desired)
							if err != nil {
								return nil, err
							}
						} else {
							return nil, errors.New("use format: switch(expr, condition1,value1, condition2, value2 .....condition n, value n)")
						}
					}
					when := &tree.When{Cond: args[i], Val: args[i+1]}
					whens = append(whens, when)
				}
				defaultexpr := args[len(args)-1]
				if !defaultexpr.ResolvedType().Equivalent(desiredType) {
					if ok, _ := tree.IsCastDeepValid(defaultexpr.ResolvedType(), desiredType); ok {
						desired, err := coltypes.DatumTypeToColumnType(desiredType)
						if err != nil {
							return nil, err
						}
						defaultexpr, err = tree.PerformCast(ctx, defaultexpr, desired)
						if err != nil {
							return nil, err
						}
					} else {
						return nil, errors.New("use format: switch(expr, condition1,value1, condition2, value2 .....condition n, value n)")
					}
				}
				expr, _ := tree.NewTypedCaseExpr(e, whens, defaultexpr, types.Unknown)
				a, err := expr.Eval(ctx)
				if err != nil {
					return nil, err
				}
				return a.Eval(ctx)
			},
			Info: "Returns the result of decode condition expression",
		},
	),
	"ascii": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			for _, ch := range s {
				return tree.NewDInt(tree.DInt(ch)), nil
			}
			return nil, errEmptyInputString
		}, types.Int, "Returns the character code of the first character in `val`. Despite the name, the function supports Unicode too.")),

	"chr": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := tree.MustBeDInt(args[0])
				var answer string
				switch {
				case x < 0:
					return nil, errChrValueTooSmall
				case x > utf8.MaxRune:
					return nil, errChrValueTooLarge
				default:
					answer = string(rune(x))
				}
				return tree.NewDString(answer), nil
			},
			Info: "Returns the character with the code given in `val`. Inverse function of `ascii()`.",
		},
	),
	//"chr": makeBuiltin(tree.FunctionProperties{Category: categoryString},
	//	tree.Overload{
	//		Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
	//		ReturnType: tree.FixedReturnType(types.String),
	//		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	//			x := tree.MustBeDInt(args[0])
	//			var answer string
	//			switch {
	//			case x < 0:
	//				return nil, errChrValueTooSmall
	//			case x > 256:
	//				x = x % 256
	//				answer = string(rune(x))
	//			default:
	//				answer = string(rune(x))
	//			}
	//			return tree.NewDString(answer), nil
	//		},
	//		Info: "Returns the character with the code given in `val`. Inverse function of `ascii()`.",
	//	},
	//),

	"md5": hashBuiltin(
		func() hash.Hash { return md5.New() },
		"Calculates the MD5 hash value of a set of values.",
	),

	"sha1": hashBuiltin(
		func() hash.Hash { return sha1.New() },
		"Calculates the SHA1 hash value of a set of values.",
	),

	"sha256": hashBuiltin(
		func() hash.Hash { return sha256.New() },
		"Calculates the SHA256 hash value of a set of values.",
	),

	"sha512": hashBuiltin(
		func() hash.Hash { return sha512.New() },
		"Calculates the SHA512 hash value of a set of values.",
	),

	"fnv32": hash32Builtin(
		func() hash.Hash32 { return fnv.New32() },
		"Calculates the 32-bit FNV-1 hash value of a set of values.",
	),

	"fnv32a": hash32Builtin(
		func() hash.Hash32 { return fnv.New32a() },
		"Calculates the 32-bit FNV-1a hash value of a set of values.",
	),

	"fnv64": hash64Builtin(
		func() hash.Hash64 { return fnv.New64() },
		"Calculates the 64-bit FNV-1 hash value of a set of values.",
	),

	"fnv64a": hash64Builtin(
		func() hash.Hash64 { return fnv.New64a() },
		"Calculates the 64-bit FNV-1a hash value of a set of values.",
	),

	"crc32ieee": hash32Builtin(
		func() hash.Hash32 { return crc32.New(crc32.IEEETable) },
		"Calculates the CRC-32 hash using the IEEE polynomial.",
	),

	"crc32": hash32Builtin(
		func() hash.Hash32 { return crc32.New(crc32.IEEETable) },
		"Calculates the CRC-32 hash using the IEEE polynomial.",
	),

	"crc32c": hash32Builtin(
		func() hash.Hash32 { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
		"Calculates the CRC-32 hash using the Castagnoli polynomial.",
	),

	"hextoraw": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				hexStr := string(tree.MustBeDString(args[0]))
				for i := 0; i < len(hexStr); i++ {
					if !(47 < hexStr[i] && hexStr[i] < 58) && !(64 < hexStr[i] && hexStr[i] < 71) && !(96 < hexStr[i] && hexStr[i] < 103) {
						return nil, fmt.Errorf("invalid hex number")
					}
				}
				if len(hexStr)%2 == 1 {
					hexStr = "0" + hexStr
				}
				return tree.NewDString(strings.ToUpper(hexStr)), nil
			},
			Info: "Converts `val` to its raw representation.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				hexStr := int(tree.MustBeDInt(args[0]))
				str := strconv.Itoa(hexStr)
				if len(str)%2 == 1 {
					str = "0" + str
				}

				return tree.NewDString(str), nil
			},
			Info: "Converts `val` to its raw representation.",
		},
	),

	"rawtohex": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := *(args[0].(*tree.DBytes))
				return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", []byte(bytes)))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", string(tree.MustBeDString(args[0]))))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
	),

	"to_hex": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("%x", int64(tree.MustBeDInt(args[0])))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := *(args[0].(*tree.DBytes))
				return tree.NewDString(fmt.Sprintf("%x", []byte(bytes))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("%x", string(tree.MustBeDString(args[0])))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
	),

	"hex": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", int64(tree.MustBeDInt(args[0]))))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := *(args[0].(*tree.DBytes))
				return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", []byte(bytes)))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", string(tree.MustBeDString(args[0]))))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
	),

	"to_english": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				val := int(*args[0].(*tree.DInt))
				var buf bytes.Buffer
				if val < 0 {
					buf.WriteString("minus-")
					val = -val
				}
				var digits []string
				digits = append(digits, digitNames[val%10])
				for val > 9 {
					val /= 10
					digits = append(digits, digitNames[val%10])
				}
				for i := len(digits) - 1; i >= 0; i-- {
					if i < len(digits)-1 {
						buf.WriteByte('-')
					}
					buf.WriteString(digits[i])
				}
				return tree.NewDString(buf.String()), nil
			},
			Info: "This function enunciates the value of its argument using English cardinals.",
		},
	),
	"asciistr": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(*args[0].(*tree.DString))
				var result string
				var s = []rune(str)
				encodeContent := utf16.Encode(s)
				for i := 0; i < len(encodeContent); i++ {
					if encodeContent[i] > 0 && encodeContent[i] < 255 {
						result = result + string(rune(encodeContent[i]))
					} else {
						result = result + "\\" + strconv.FormatInt(int64(encodeContent[i]), 16)
					}
				}
				return tree.NewDString(string(result)), nil

			},
			Info: "ASCIISTR takes as its argument a string, or an expression that resolves to a string, in any character set and returns an ASCII version of the string in the database character set. Non-ASCII characters are converted to the form \\xxxx, where xxxx represents a UTF-16 code unit.",
		},
	),

	// The SQL parser coerces POSITION to b=b[c+1:].
	"strpos": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		stringOverload2("input", "find", func(_ *tree.EvalContext, s, substring string) (tree.Datum, error) {
			index := strings.Index(s, substring)
			if index < 0 {
				return tree.DZero, nil
			}

			return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s[:index]) + 1)), nil
		}, types.Int, "Calculates the position where the string `find` begins in `input`. \n\nFor"+
			" example, `strpos('doggie', 'gie')` returns `4`."),
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int},
				{Name: "occ", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				occ := int(tree.MustBeDInt(args[3]))
				if inputpattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				c := strings.Index(str, inputpattern)
				if c < 0 {
					return tree.NewDInt(0), nil
				}
				pos := index + c
				if occ == 1 {
					return tree.NewDInt(tree.DInt(pos)), nil
				} else if occ > 1 {
					for i := 2; i <= occ; i++ {
						str = str[c+1:]
						c = strings.Index(str, inputpattern)
						if c < 0 {
							return tree.NewDInt(0), nil
						}
						pos = pos + c + 1
					}
					return tree.NewDInt(tree.DInt(pos)), nil
				} else {
					return tree.NewDInt(0), nil
				}
			},
			Info: "Return the position of the matching string",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				if inputpattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				c := strings.Index(str, inputpattern)
				if c < 0 {
					return tree.NewDInt(0), nil
				}
				pos := index + c
				return tree.NewDInt(tree.DInt(pos)), nil
			},
			Info: "Return the position of the matching string",
		},
	),

	"overlay": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "input", Typ: types.String},
				{Name: "overlay_val", Typ: types.String},
				{Name: "start_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				to := string(tree.MustBeDString(args[1]))
				pos := int(tree.MustBeDInt(args[2]))
				size := utf8.RuneCountInString(to)
				return overlay(s, to, pos, size)
			},
			Info: "Replaces characters in `input` with `overlay_val` starting at `start_pos` " +
				"(begins at 1). \n\nFor example, `overlay('doggie', 'CAT', 2)` returns " +
				"`dCATie`.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "input", Typ: types.String},
				{Name: "overlay_val", Typ: types.String},
				{Name: "start_pos", Typ: types.Int},
				{Name: "end_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				to := string(tree.MustBeDString(args[1]))
				pos := int(tree.MustBeDInt(args[2]))
				size := int(tree.MustBeDInt(args[3]))
				return overlay(s, to, pos, size)
			},
			Info: "Deletes the characters in `input` between `start_pos` and `end_pos` (count " +
				"starts at 1), and then insert `overlay_val` at `start_pos`.",
		},
	),

	"lpad": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "string", Typ: types.String}, {Name: "length", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				length := int(tree.MustBeDInt(args[1]))
				ret, err := lpad(s, length, " ")
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Pads `string` to `length` by adding ' ' to the left of `string`." +
				"If `string` is longer than `length` it is truncated.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "string", Typ: types.String}, {Name: "length", Typ: types.Int}, {Name: "fill", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				length := int(tree.MustBeDInt(args[1]))
				fill := string(tree.MustBeDString(args[2]))
				ret, err := lpad(s, length, fill)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Pads `string` by adding `fill` to the left of `string` to make it `length`. " +
				"If `string` is longer than `length` it is truncated.",
		},
	),

	"rpad": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "string", Typ: types.String}, {Name: "length", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				length := int(tree.MustBeDInt(args[1]))
				ret, err := rpad(s, length, " ")
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Pads `string` to `length` by adding ' ' to the right of string. " +
				"If `string` is longer than `length` it is truncated.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "string", Typ: types.String}, {Name: "length", Typ: types.Int}, {Name: "fill", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				length := int(tree.MustBeDInt(args[1]))
				fill := string(tree.MustBeDString(args[2]))
				ret, err := rpad(s, length, fill)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Pads `string` to `length` by adding `fill` to the right of `string`. " +
				"If `string` is longer than `length` it is truncated.",
		},
	),

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": makeBuiltin(defProps(),
		stringOverload2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.Trim(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning or end"+
			" of `input` (applies recursively). \n\nFor example, `btrim('doggie', 'eod')` "+
			"returns `ggi`."),
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimSpace(s)), nil
		}, types.String, "Removes all spaces from the beginning and end of `val`."),
	),

	"trim": makeBuiltin(defProps(),
		stringOverload2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.Trim(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning or end"+
			" of `input` (applies recursively). \n\nFor example, `trim('doggie', 'eod')` "+
			"returns `ggi`."),
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimSpace(s)), nil
		}, types.String, "Removes all spaces from the beginning and end of `val`."),
	),

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": makeBuiltin(defProps(),
		stringOverload2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimLeft(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning "+
			"(left-hand side) of `input` (applies recursively). \n\nFor example, "+
			"`ltrim('doggie', 'od')` returns `ggie`."),
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
		}, types.String, "Removes all spaces from the beginning (left-hand side) of `val`."),
	),

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": makeBuiltin(defProps(),
		stringOverload2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimRight(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the end (right-hand "+
			"side) of `input` (applies recursively). \n\nFor example, `rtrim('doggie', 'ei')` "+
			"returns `dogg`."),
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
		}, types.String, "Removes all spaces from the end (right-hand side) of `val`."),
	),

	"reverse": makeBuiltin(defProps(),
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			if len(s) > maxAllocatedStringSize {
				return nil, errStringTooLarge
			}
			runes := []rune(s)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return tree.NewDString(string(runes)), nil
		}, types.String, "Reverses the order of the string's characters.")),

	"replace": makeBuiltin(defProps(),
		stringOverload3("input", "find", "replace",
			func(evalCtx *tree.EvalContext, input, from, to string) (tree.Datum, error) {
				// Reserve memory for the largest possible result.
				var maxResultLen int64
				if len(from) == 0 {
					// Replacing the empty string causes len(input)+1 insertions.
					maxResultLen = int64(len(input) + (len(input)+1)*len(to))
				} else if len(from) < len(to) {
					// Largest result is if input is [from] repeated over and over.
					maxResultLen = int64(len(input) / len(from) * len(to))
				} else {
					// Largest result is if there are no replacements.
					maxResultLen = int64(len(input))
				}
				if maxResultLen > maxAllocatedStringSize {
					return nil, errStringTooLarge
				}
				result := strings.Replace(input, from, to, -1)
				return tree.NewDString(result), nil
			},
			types.String,
			"Replaces all occurrences of `find` with `replace` in `input`",
		)),

	"translate": makeBuiltin(defProps(),
		stringOverload3("input", "find", "replace",
			func(evalCtx *tree.EvalContext, s, from, to string) (tree.Datum, error) {
				const deletionRune = utf8.MaxRune + 1
				translation := make(map[rune]rune, len(from))
				for _, fromRune := range from {
					toRune, size := utf8.DecodeRuneInString(to)
					if toRune == utf8.RuneError {
						toRune = deletionRune
					} else {
						to = to[size:]
					}
					translation[fromRune] = toRune
				}

				runes := make([]rune, 0, len(s))
				for _, c := range s {
					if t, ok := translation[c]; ok {
						if t != deletionRune {
							runes = append(runes, t)
						}
					} else {
						runes = append(runes, c)
					}
				}
				return tree.NewDString(string(runes)), nil
			}, types.String, "In `input`, replaces the first character from `find` with the first "+
				"character in `replace`; repeat for each character in `find`. \n\nFor example, "+
				"`translate('doggie', 'dog', '123');` returns `1233ie`.")),

	"regexp_extract": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}, {Name: "regex", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				return regexpExtract(ctx, s, pattern, `\`)
			},
			Info: "Returns the first match for the Regular Expression `regex` in `input`.",
		},
	),

	"regexp_replace": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "replace", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				to := string(tree.MustBeDString(args[2]))
				result, err := regexpReplace(evalCtx, s, pattern, to, "")
				if err != nil {
					return nil, err
				}
				return result, nil
			},
			Info: "Replaces matches for the Regular Expression `regex` in `input` with the " +
				"Regular Expression `replace`.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "replace", Typ: types.String},
				{Name: "flags", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				to := string(tree.MustBeDString(args[2]))
				sqlFlags := string(tree.MustBeDString(args[3]))
				result, err := regexpReplace(evalCtx, s, pattern, to, sqlFlags)
				if err != nil {
					return nil, err
				}
				return result, nil
			},
			Info: "Replaces matches for the regular expression `regex` in `input` with the regular " +
				"expression `replace` using `flags`." + `

ZNBaseDB supports the following flags:

| Flag           | Description                                                       |
|----------------|-------------------------------------------------------------------|
| **c**          | Case-sensitive matching                                           |
| **g**          | Global matching (match each substring instead of only the first)  |
| **i**          | Case-insensitive matching                                         |
| **m** or **n** | Newline-sensitive (see below)                                     |
| **p**          | Partial newline-sensitive matching (see below)                    |
| **s**          | Newline-insensitive (default)                                     |
| **w**          | Inverse partial newline-sensitive matching (see below)            |

| Mode | ` + "`.` and `[^...]` match newlines | `^` and `$` match line boundaries" + `|
|------|----------------------------------|--------------------------------------|
| s    | yes                              | no                                   |
| w    | yes                              | yes                                  |
| p    | no                               | no                                   |
| m/n  | no                               | yes                                  |`,
		},
	),

	"regexp_count": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int},
				{Name: "flag", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				reflag := string(tree.MustBeDString(args[3]))
				var pattern, flag string
				if len(reflag) > 1 {
					flag = reflag[len(reflag)-1:]
				} else if len(reflag) == 1 {
					flag = reflag
				}
				switch flag {
				case "i":
					pattern = `((?i)` + `(` + inputpattern + `))`
				case "c":
					pattern = inputpattern
				case "m":
					pattern = `((?m)` + `(` + inputpattern + `))`
				case "n":
					pattern = `(?s:` + `(` + inputpattern + `))`
				case "x":
					pattern = strings.Replace(inputpattern, " ", "", -1)
				default:
					err := fmt.Errorf("未识别的输入match_param")
					return nil, err
				}
				if pattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				re, err := regexp.Compile(pattern)
				if err != nil {
					return nil, err
				}
				new := re.FindAllString(str, -1)
				count := len(new)
				return tree.NewDInt(tree.DInt(count)), nil

			},
			Info: "Returns the sum of all matches of the regular expression regex in the input",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				var pattern string
				pattern = strings.Replace(inputpattern, "(", "", -1)
				pattern = strings.Replace(pattern, ")", "", -1)
				if pattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				re, err := regexp.Compile(pattern)
				if err != nil {
					return nil, err
				}
				new := re.FindAllString(str, -1)
				count := len(new)
				return tree.NewDInt(tree.DInt(count)), nil

			},
			Info: "Returns the sum of all matches of the regular expression regex in the input",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				if inputpattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				re, err := regexp.Compile(inputpattern)
				if err != nil {
					return nil, err
				}
				new := re.FindAllString(inputstr, -1)
				count := len(new)
				return tree.NewDInt(tree.DInt(count)), nil

			},
			Info: "Returns the sum of all matches of the regular expression regex in the input",
		},
	),
	"regexp_instr": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int},
				{Name: "occurrence", Typ: types.Int},
				{Name: "return_opt", Typ: types.Int},
				{Name: "flag", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				occur := int(tree.MustBeDInt(args[3]))
				opt := int(tree.MustBeDInt(args[4]))
				reflag := string(tree.MustBeDString(args[5]))
				if len(inputpattern) > 512 {
					return nil, errors.New("Exceeds the maximum capacity of the inputpattern")
				}
				var pattern, flag string
				if len(reflag) > 1 {
					flag = reflag[len(reflag)-1:]
				} else if len(reflag) == 1 {
					flag = reflag
				}
				switch flag {
				case "i":
					pattern = `((?i)` + `(` + inputpattern + `))`
				case "c":
					pattern = inputpattern
				case "m":
					pattern = `((?m)` + `(` + inputpattern + `))`
				case "n":
					pattern = `(?s:` + `(` + inputpattern + `))`
				case "x":
					pattern = strings.Replace(inputpattern, " ", "", -1)
				default:
					err := fmt.Errorf("未识别的输入match_param")
					return nil, err
				}
				if pattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				re, err := regexp.Compile(pattern)
				if err != nil {
					return nil, err
				}
				loc := re.FindAllStringIndex(str, -1)
				if loc == nil || occur > len(loc) || opt < 0 || opt > 1 {
					return tree.NewDInt(0), nil
				}
				temp := loc[occur-1]
				location := temp[opt] + index
				return tree.NewDInt(tree.DInt(location)), nil
			},
			Info: "Returns the position of matches of the regular expression regex in the input",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int},
				{Name: "occurrence", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				occur := int(tree.MustBeDInt(args[3]))
				if len(inputpattern) > 512 {
					return nil, errors.New("Exceeds the maximum capacity of the inputpattern")
				}
				if inputpattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				re, err := regexp.Compile(inputpattern)
				if err != nil {
					return nil, err
				}
				loc := re.FindAllStringIndex(str, -1)
				if loc == nil || occur > len(loc) {
					return tree.NewDInt(0), nil
				}
				temp := loc[occur-1]
				location := temp[0] + index
				return tree.NewDInt(tree.DInt(location)), nil
			},
			Info: "Returns the position of matches of the regular expression regex in the input",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				if len(inputpattern) > 512 {
					return nil, errors.New("Exceeds the maximum capacity of the inputpattern")
				}
				if inputpattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				re, err := regexp.Compile(inputpattern)
				if err != nil {
					return nil, err
				}
				loc := re.FindStringIndex(str)
				if loc == nil {
					return tree.NewDInt(0), nil
				}
				location := loc[0] + index
				return tree.NewDInt(tree.DInt(location)), nil
			},
			Info: "Returns the position of matches of the regular expression regex in the input",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int},
				{Name: "occurrence", Typ: types.Int},
				{Name: "return_opt", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				occur := int(tree.MustBeDInt(args[3]))
				opt := int(tree.MustBeDInt(args[4]))
				if len(inputpattern) > 512 {
					return nil, errors.New("Exceeds the maximum capacity of the inputpattern")
				}
				if inputpattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				re, err := regexp.Compile(inputpattern)
				if err != nil {
					return nil, err
				}
				loc := re.FindAllStringIndex(str, -1)
				if loc == nil || occur > len(loc) || opt < 0 || opt > 1 {
					return tree.NewDInt(0), nil
				}
				temp := loc[occur-1]
				location := temp[opt] + index
				return tree.NewDInt(tree.DInt(location)), nil
			},
			Info: "Returns the position of matches of the regular expression regex in the input",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "index", Typ: types.Int},
				{Name: "occurrence", Typ: types.Int},
				{Name: "return_opt", Typ: types.Int},
				{Name: "flag", Typ: types.String},
				{Name: "subexpr", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				index := int(tree.MustBeDInt(args[2]))
				occur := int(tree.MustBeDInt(args[3]))
				opt := int(tree.MustBeDInt(args[4]))
				reflag := string(tree.MustBeDString(args[5]))
				subxprnum := int(tree.MustBeDInt(args[6]))
				if len(inputpattern) > 512 {
					return nil, errors.New("Exceeds the maximum capacity of the inputpattern")
				}
				var pattern, flag string
				if len(reflag) > 1 {
					flag = reflag[len(reflag)-1:]
				} else if len(reflag) == 1 {
					flag = reflag
				}
				switch flag {
				case "i":
					pattern = `((?i)` + `(` + inputpattern + `))`
				case "c":
					pattern = inputpattern
				case "m":
					pattern = `((?m)` + `(` + inputpattern + `))`
				case "n":
					pattern = `(?s:` + `(` + inputpattern + `))`
				case "x":
					pattern = strings.Replace(inputpattern, " ", "", -1)
				default:
					err := fmt.Errorf("未识别的输入match_param")
					return nil, err
				}
				if pattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr[index-1:]
				re, err := regexp.Compile(pattern)
				if err != nil {
					return nil, err
				}
				loc := re.FindAllStringSubmatchIndex(str, -1)
				if loc == nil || occur > len(loc) || opt < 0 || opt > 1 || subxprnum < 0 || subxprnum > 9 {
					return tree.NewDInt(0), nil
				}
				loctemp := loc[occur-1]
				var temp []int
				if reflag == "i" || reflag == "m" {
					temp = append(loctemp[:2], loctemp[6:]...)
				} else if reflag == "c" || reflag == "x" {
					temp = loctemp
				} else if reflag == "n" {
					temp = append(loctemp[:2], loctemp[4:]...)
				}
				if subxprnum > len(temp)/2-1 || len(temp) <= 2 {
					return tree.NewDInt(0), nil
				}
				location := temp[2*subxprnum+opt] + index
				return tree.NewDInt(tree.DInt(location)), nil
			},
			Info: "Returns the position of matches of the regular expression regex in the input",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputstr := string(tree.MustBeDString(args[0]))
				inputpattern := string(tree.MustBeDString(args[1]))
				if len(inputpattern) > 512 {
					return nil, errors.New("Exceeds the maximum capacity of the inputpattern")
				}
				if inputpattern == "" && inputstr != "" {
					return tree.NewDInt(0), nil
				}
				str := inputstr
				re, err := regexp.Compile(inputpattern)
				if err != nil {
					return nil, err
				}
				loc := re.FindStringIndex(str)
				if loc == nil {
					return tree.NewDInt(0), nil
				}
				return tree.NewDInt(tree.DInt(1)), nil
			},
			Info: "Returns the position of matches of the regular expression regex in the input",
		},
	),

	"like_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				return tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, false)
			},
			types.Bool,
			"Matches `unescaped` with `pattern` using 'escape' as an escape token.",
		)),

	"not_like_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, false)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches with `pattern` using 'escape' as an escape token.",
		)),

	"ilike_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				return tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, true)
			},
			types.Bool,
			"Matches case insensetively `unescaped` with `pattern` using 'escape' as an escape token.",
		)),

	"not_ilike_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, true)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches case insensetively with `pattern` using 'escape' as an escape token.",
		)),

	"similar_to_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				return tree.SimilarToEscape(evalCtx, unescaped, pattern, escape)
			},
			types.Bool,
			"Matches `unescaped` with `pattern` using 'escape' as an escape token.",
		)),

	"not_similar_to_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := tree.SimilarToEscape(evalCtx, unescaped, pattern, escape)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches with `pattern` using 'escape' as an escape token.",
		)),

	"initcap": makeBuiltin(defProps(),
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.Title(strings.ToLower(s))), nil
		}, types.String, "Capitalizes the first letter of `val`.")),

	"quote_ident": makeBuiltin(defProps(),
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			var buf bytes.Buffer
			lex.EncodeRestrictedSQLIdent(&buf, s, lex.EncNoFlags)
			return tree.NewDString(buf.String()), nil
		}, types.String, "Return `val` suitably quoted to serve as identifier in a SQL statement.")),

	"quote_literal": makeBuiltin(defProps(),
		tree.Overload{
			Types:             tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType:        tree.FixedReturnType(types.String),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := tree.MustBeDString(args[0])
				return tree.NewDString(lex.EscapeSQLString(string(s))), nil
			},
			Info: "Return `val` suitably quoted to serve as string literal in a SQL statement.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// PostgreSQL specifies that this variant first casts to the SQL string type,
				// and only then quotes. We can't use (Datum).String() directly.
				d := tree.UnwrapDatum(ctx, args[0])
				strD, err := tree.PerformCast(ctx, d, coltypes.String)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(strD.String()), nil
			},
			Info: "Coerce `val` to a string and then quote it as a literal.",
		},
	),

	// quote_nullable is the same as quote_literal but accepts NULL arguments.
	"quote_nullable": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryString,
			NullableArgs: true,
		},
		tree.Overload{
			Types:             tree.ArgTypes{{Name: "val", Typ: types.String}},
			ReturnType:        tree.FixedReturnType(types.String),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.NewDString("NULL"), nil
				}
				s := tree.MustBeDString(args[0])
				return tree.NewDString(lex.EscapeSQLString(string(s))), nil
			},
			Info: "Coerce `val` to a string and then quote it as a literal. If `val` is NULL, returns 'NULL'.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.NewDString("NULL"), nil
				}
				// PostgreSQL specifies that this variant first casts to the SQL string type,
				// and only then quotes. We can't use (Datum).String() directly.
				d := tree.UnwrapDatum(ctx, args[0])
				strD, err := tree.PerformCast(ctx, d, coltypes.String)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(strD.String()), nil
			},
			Info: "Coerce `val` to a string and then quote it as a literal. If `val` is NULL, returns 'NULL'.",
		},
	),

	"left": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Bytes}, {Name: "return_set", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := []byte(*args[0].(*tree.DBytes))
				n := int(tree.MustBeDInt(args[1]))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return tree.NewDBytes(tree.DBytes(bytes[:n])), nil
			},
			Info: "Returns the first `return_set` bytes from `input`.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}, {Name: "return_set", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				runes := []rune(string(tree.MustBeDString(args[0])))
				n := int(tree.MustBeDInt(args[1]))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return tree.NewDString(string(runes[:n])), nil
			},
			Info: "Returns the first `return_set` characters from `input`.",
		},
	),

	"right": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Bytes}, {Name: "return_set", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := []byte(*args[0].(*tree.DBytes))
				n := int(tree.MustBeDInt(args[1]))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return tree.NewDBytes(tree.DBytes(bytes[len(bytes)-n:])), nil
			},
			Info: "Returns the last `return_set` bytes from `input`.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}, {Name: "return_set", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				runes := []rune(string(tree.MustBeDString(args[0])))
				n := int(tree.MustBeDInt(args[1]))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return tree.NewDString(string(runes[len(runes)-n:])), nil
			},
			Info: "Returns the last `return_set` characters from `input`.",
		},
	),

	"random": makeBuiltin(
		tree.FunctionProperties{
			Impure:                  true,
			NeedsRepeatedEvaluation: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(rand.Float64())), nil
			},
			Info: "Returns a random float between 0 and 1.",
		},
	),

	"unhex": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, be := string(tree.MustBeDString(args[0])), sessiondata.BytesEncodeHex
				res, err := lex.DecodeRawBytesToByteArray(data, be)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(string(res)), nil
			},
			Info: "Converts hexadecimal encoding to a value.",
		},
	),

	"unique_rowid": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryIDGeneration,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDInt(GenerateUniqueInt(ctx.NodeID)), nil
			},
			Info: "Returns a unique ID used by ZNBaseDB to generate unique row IDs if a " +
				"Primary Key isn't defined for the table. The value is a combination of the " +
				"insert timestamp and the ID of the node executing the statement, which " +
				"guarantees this combination is globally unique. However, there can be " +
				"gaps and the order is not completely guaranteed.",
		},
	),

	"unique_rowid16": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryIDGeneration,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDInt16(GenerateUniqueInt2(ctx.NodeID)), nil
			},
			Info: "Returns a unique ID used by ZNBaseDB to generate unique row IDs if a " +
				"Primary Key isn't defined for the table. The value is a combination of the " +
				"insert timestamp and the ID of the node executing the statement, which " +
				"guarantees this combination is globally unique. However, there can be " +
				"gaps and the order is not completely guaranteed.",
		},
	),

	"unique_rowid32": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryIDGeneration,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDInt32(GenerateUniqueInt(ctx.NodeID)), nil
			},
			Info: "Returns a unique ID used by ZNBaseDB to generate unique row IDs if a " +
				"Primary Key isn't defined for the table. The value is a combination of the " +
				"insert timestamp and the ID of the node executing the statement, which " +
				"guarantees this combination is globally unique. However, there can be " +
				"gaps and the order is not completely guaranteed.",
		},
	),

	// Sequence functions.

	"nextval": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySequences,
			DistsqlBlacklist: true,
			Impure:           true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "sequence_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn:         nextVal(tree.NewDInt),
			Info:       "Advances the given sequence and returns its new value.",
		},
	),

	"nextval16": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySequences,
			DistsqlBlacklist: true,
			Impure:           true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "sequence_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn:         nextVal(tree.NewDInt16),
			Info:       "Advances the given sequence and returns its new value.",
		},
	),

	"nextval32": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySequences,
			DistsqlBlacklist: true,
			Impure:           true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "sequence_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn:         nextVal(tree.NewDInt32),
			Info:       "Advances the given sequence and returns its new value.",
		},
	),

	"currval": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySequences,
			DistsqlBlacklist: true,
			Impure:           true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "sequence_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := evalCtx.Sequence.ParseQualifiedTableName(evalCtx.Ctx(), string(name))
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.GetLatestValueInSessionForSequence(evalCtx.Ctx(), qualifiedName)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info: "Returns the latest value obtained with nextval for this sequence in this session.",
		},
	),

	"lastval": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySequences,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				val, err := evalCtx.SessionData.SequenceState.GetLastValue()
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(val)), nil
			},
			Info: "Return value most recently obtained with nextval in this session.",
		},
	),

	// Note: behavior is slightly different than Postgres for performance reasons.
	// See https://github.com/znbasedb/znbase/issues/21564
	"setval": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySequences,
			DistsqlBlacklist: true,
			Impure:           true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "sequence_name", Typ: types.String}, {Name: "value", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := evalCtx.Sequence.ParseQualifiedTableName(evalCtx.Ctx(), string(name))
				if err != nil {
					return nil, err
				}

				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValue(
					evalCtx.Ctx(), qualifiedName, int64(newVal), true); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. The next call to nextval will return " +
				"`value + Increment`",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "sequence_name", Typ: types.String}, {Name: "value", Typ: types.Int}, {Name: "is_called", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := evalCtx.Sequence.ParseQualifiedTableName(evalCtx.Ctx(), string(name))
				if err != nil {
					return nil, err
				}

				isCalled := bool(tree.MustBeDBool(args[2]))

				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValue(
					evalCtx.Ctx(), qualifiedName, int64(newVal), isCalled); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. If is_called is false, the next call to " +
				"nextval will return `value`; otherwise `value + Increment`.",
		},
	),

	"experimental_uuid_v4": uuidV4Impl,
	"uuid_v4":              uuidV4Impl,

	"greatest": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryComparison,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.HomogeneousType{},
			ReturnType: tree.FirstNonNullReturnType(),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PickFromTuple(ctx, true /* greatest */, args)
			},
			Info: "Returns the element with the greatest value.",
		},
	),

	"least": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryComparison,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.HomogeneousType{},
			ReturnType: tree.FirstNonNullReturnType(),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PickFromTuple(ctx, false /* greatest */, args)
			},
			Info: "Returns the element with the lowest value.",
		},
	),

	// Timestamp/Date functions.

	"experimental_strftime": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Timestamp}, {Name: "extract_format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[0].(*tree.DTimestamp).Time
				format := string(tree.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Date}, {Name: "extract_format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := timeutil.Unix(int64(*args[0].(*tree.DDate))*tree.SecondsInDay, 0)
				format := string(tree.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.TimestampTZ}, {Name: "extract_format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[0].(*tree.DTimestampTZ).Time
				format := string(tree.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
	),

	//unix_timestamp 返回该时间对应时间戳秒数
	"unix_timestamp": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.Int),
			PreferredOverload: true,
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				_, offset := evalCtx.StmtTimestamp.In(evalCtx.GetLocation()).Zone()
				ret := evalCtx.StmtTimestamp.Unix() - int64(offset)
				return tree.NewDInt(tree.DInt(ret)), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamp", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				d := args[0].(*tree.DTimestamp)
				_, offset := d.Time.In(evalCtx.GetLocation()).Zone()
				timeStamp := args[0].(*tree.DTimestamp).Time
				ret := timeStamp.Unix() - int64(offset)
				return tree.NewDInt(tree.DInt(ret)), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestampTZ", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStampTZ := args[0].(*tree.DTimestampTZ).Time
				ret := timeStampTZ.Unix()
				return tree.NewDInt(tree.DInt(ret)), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timeDate", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStampTZ, err := tree.PerformCast(evalCtx, args[0], coltypes.TimestampTZ)
				if err != nil {
					return nil, err
				}
				timeStamp := tree.MustBeDTimestampTZ(timeStampTZ).Time
				ret := timeStamp.Unix()
				return tree.NewDInt(tree.DInt(ret)), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timeString", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeDatum, err := tree.PerformCast(evalCtx, args[0], coltypes.TimestampTZ)
				if err != nil {
					return nil, err
				}
				timeStamp := tree.MustBeDTimestampTZ(timeDatum).Time
				ret := timeStamp.Unix()
				return tree.NewDInt(tree.DInt(ret)), nil
			},
		},
	),

	"experimental_strptime": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				toParse := string(tree.MustBeDString(args[0]))
				format := string(tree.MustBeDString(args[1]))
				t, err := strtime.Strptime(toParse, format)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestampTZ(t.UTC(), time.Microsecond), nil
			},
			Info: "Returns `input` as a timestamptz using `format` (which uses standard " +
				"`strptime` formatting).",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-datetime.html
	"age": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				Dtime2, err := tree.PerformCast(ctx, args[0], coltypes.TimestampTZ)
				if err != nil {
					return nil, err
				}
				time1 := ctx.GetTxnTimestamp(time.Microsecond).UTC()
				time2 := tree.MustBeDTimestampTZ(Dtime2).Time.UTC()
				res, err := tree.SubInterval(ctx, time1, time2)
				if err != nil {
					return nil, err
				}
				return tree.ParseDInterval(res)
			},
			Info: "Calculates the interval between `val` and the current time.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "end", Typ: types.Any}, {Name: "begin", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				Dtime1, err := tree.PerformCast(ctx, args[0], coltypes.TimestampTZ)
				if err != nil {
					return nil, err
				}
				Dtime2, err := tree.PerformCast(ctx, args[1], coltypes.TimestampTZ)
				if err != nil {
					return nil, err
				}
				time1 := tree.MustBeDTimestampTZ(Dtime1).Time.UTC()
				time2 := tree.MustBeDTimestampTZ(Dtime2).Time.UTC()
				res, err := tree.SubInterval(ctx, time1, time2)
				if err != nil {
					return nil, err
				}
				return tree.ParseDInterval(res)
			},
			Info: "Calculates the interval between `begin` and `end`.",
		},
	),

	"datediff": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "firstdate", Typ: types.Any}, {Name: "seconddate", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				datum1, err := tree.PerformCast(ctx, args[0], coltypes.Date)
				if err != nil {
					return tree.DNull, nil
				}
				datum2, err := tree.PerformCast(ctx, args[1], coltypes.Date)
				if err != nil {
					return tree.DNull, nil
				}
				daydiff := int64(*(datum1.(*tree.DDate))) - int64(*(datum2.(*tree.DDate)))

				return tree.NewDInt(tree.DInt(daydiff)), nil
			},
			Info: "Calculates the date interval between firstdate and seconddate",
		},
	),

	"dayofmonth": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date1, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					return tree.DNull, nil
				}
				_, _, day := date1.(*tree.DTimestamp).Date()
				return tree.NewDInt(tree.DInt(day)), nil
			},
			Info: "return the day of month",
		},
	),

	"sysdate": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.Timestamp),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(timeutil.LocalNow(), time.Second), nil
			},
			Info: "Return the time at which the function executes",
		},
		tree.Overload{
			Types:             tree.ArgTypes{{Name: "input", Typ: types.Int}},
			ReturnType:        tree.FixedReturnType(types.Timestamp),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				num := args[0].(*tree.DInt)
				n := int(*num)
				return tree.MakeDTimestamp(timeutil.LocalNow(), time.Second/time.Duration(math.Pow10(n))), nil
			},
			Info: "Return the time at which the function executes",
		},
	),

	"timestampadd": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "unit", Typ: types.String}, {Name: "Interval", Typ: types.Int}, {Name: "Timestamp", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var sarg = [22]string{"year", "years", "quarter", "quarters", "month", "months", "week", "weeks", "day", "days", "hour", "hours", "minute", "minutes", "second", "seconds", "frac_second", "frac_seconds", "millisecond", "milliseconds", "microsecond", "microseconds"}
				var daySumOfMonth = [12]int{31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30}
				var flag bool
				var n int
				str := strings.Trim(strings.ToLower(args[0].(*tree.DString).String()), "'")
				for _, ss := range sarg {
					if str == ss {
						flag = true
						break
					}
				}
				if !flag {
					err := fmt.Errorf("Input of type string is not recognized")
					return nil, err
				}
				tinterval, _ := args[1].(*tree.DInt)
				num := int(*tinterval)
				ttime := args[2].(*tree.DTimestamp)
				sst := strings.Split(strings.Trim(args[2].String(), "'"), ".")
				if len(sst) > 1 {
					n = len(sst[1])
				}
				var ti time.Time
				year, month, day := ttime.Date()
				hour, min, sec := ttime.Clock()
				nano := ttime.Nanosecond()
				switch str {
				case "year", "years":
					year = year + num
					mon := int(month)
					monindex := mon % 12
					if monindex != 2 {
						if day > daySumOfMonth[monindex] {
							day = daySumOfMonth[monindex]
						}
					} else {
						if (year+mon/12)%4 == 0 {
							if day > daySumOfMonth[monindex]+1 {
								day = daySumOfMonth[monindex] + 1
							}
						} else {
							if day > daySumOfMonth[monindex] {
								day = daySumOfMonth[monindex]
							}
						}
					}
					ti = time.Date(year, time.Month(mon), day, hour, min, sec, nano, ttime.Location())
				case "quarter", "quarters":
					mon := int(month) + num*3
					monindex := mon % 12
					if monindex != 2 {
						if day > daySumOfMonth[monindex] {
							day = daySumOfMonth[monindex]
						}
					} else {
						if (year+mon/12)%4 == 0 {
							if day > daySumOfMonth[monindex]+1 {
								day = daySumOfMonth[monindex] + 1
							}
						} else {
							if day > daySumOfMonth[monindex] {
								day = daySumOfMonth[monindex]
							}
						}
					}
					ti = time.Date(year, time.Month(mon), day, hour, min, sec, nano, ttime.Location())
				case "month", "months":
					mon := int(month) + num
					monindex := mon % 12
					if monindex != 2 {
						if day > daySumOfMonth[monindex] {
							day = daySumOfMonth[monindex]
						}
					} else {
						if (year+mon/12)%4 == 0 {
							if day > daySumOfMonth[monindex]+1 {
								day = daySumOfMonth[monindex] + 1
							}
						} else {
							if day > daySumOfMonth[monindex] {
								day = daySumOfMonth[monindex]
							}
						}
					}
					ti = time.Date(year, time.Month(mon), day, hour, min, sec, nano, ttime.Location())
				case "week", "weeks":
					ti = ttime.AddDate(0, 0, num*7)
				case "day", "days":
					ti = ttime.AddDate(0, 0, num)
				case "hour", "hours":
					ti = ttime.Add(time.Duration(num) * time.Hour)
				case "minute", "minutes":
					ti = ttime.Add(time.Duration(num) * time.Minute)
				case "second", "seconds":
					ti = ttime.Add(time.Duration(num) * time.Second)
				case "frac_second", "frac_seconds", "millisecond", "milliseconds":
					ti = ttime.Add(time.Duration(num) * time.Millisecond)
					n = 3
				case "microsecond", "microseconds":
					ti = ttime.Add(time.Duration(num) * time.Microsecond)
					n = 6
				}
				t := tree.MakeDTimestamp(ti, time.Second/time.Duration(math.Pow10(n)))
				return t, nil
			},
			Info: "Add an interval on a timestamp",
		},
	),
	"timestampdiff": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "unit", Typ: types.String}, {Name: "Timestamp1", Typ: types.Any}, {Name: "Timestamp2", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var timestamp1, timestamp2, ttime1, ttime2 tree.Datum
				var errD1, errD2, errT1, errT2 error
				var mark1, mark2 bool
				str := strings.ToLower(string(tree.MustBeDString(args[0])))
				timestamp1, errD1 = tree.PerformCast(ctx, args[1], coltypes.Timestamp)
				if errD1 != nil {
					ttime1, errT1 = tree.PerformCast(ctx, args[1], coltypes.Time)
					if errT1 != nil {
						return tree.DNull, nil
					}
					mark1 = false

				} else {
					mark1 = true
				}
				timestamp2, errD2 = tree.PerformCast(ctx, args[2], coltypes.Timestamp)
				if errD2 != nil {
					ttime2, errT2 = tree.PerformCast(ctx, args[2], coltypes.Time)
					if errT2 != nil {
						return tree.DNull, nil
					}
					mark2 = false

				} else {
					mark2 = true
				}
				if mark1 && mark2 {
					time1 := tree.MustBeDTimestamp(timestamp1)
					time2 := tree.MustBeDTimestamp(timestamp2)
					time1str := strings.Split(strings.Trim(time1.String(), "'"), " ")
					time2str := strings.Split(strings.Trim(time2.String(), "'"), " ")
					date1, err3 := tree.ParseDDate(ctx, time1str[0])
					date2, err4 := tree.ParseDDate(ctx, time2str[0])
					dtime1, err5 := tree.ParseDTime(ctx, time1str[1])
					dtime2, err6 := tree.ParseDTime(ctx, time2str[1])

					if err3 != nil {
						return nil, err3
					}
					if err4 != nil {
						return nil, err4
					}
					if err5 != nil {
						return nil, err5
					}
					if err6 != nil {
						return nil, err6
					}
					var num, flag int64
					dateinter := int64(*date2 - *date1)
					timeinter := timeofday.Difference(timeofday.TimeOfDay(*dtime2), timeofday.TimeOfDay(*dtime1))
					nanodiff := time2.Nanosecond() - time1.Nanosecond()
					switch str {
					case "year", "years":
						num = int64(time2.Year() - time1.Year())
						if num < 0 {
							t := time1
							time1 = time2
							time2 = t
							num = num * (-1)
							flag = 1
						}
						if num > 0 {
							if int(time2.Month()) < int(time1.Month()) {
								num--
							} else if int(time2.Month()) == int(time1.Month()) {
								if time2.Day() < time1.Day() {
									num--
								} else if time2.Day() == time1.Day() {
									h2, m2, s2 := time2.Clock()
									h1, m1, s1 := time1.Clock()
									intervalHour := h2 - h1
									intervalMinute := m2 - m1
									inters := s2 - s1
									if intervalHour < 0 {
										num--
									} else if intervalHour == 0 {
										if intervalMinute < 0 {
											num--
										} else if intervalMinute == 0 {
											if inters < 0 {
												num--
											} else if inters == 0 {
												if nanodiff < 0 {
													num--
												}
											}
										}
									}
								}
							}
						}
						if flag == 1 {
							num = num * (-1)
						}
					case "quarter", "quarters":
						num = int64((time2.Year()-time1.Year())*12 + int(time2.Month()) - int(time1.Month()))
						if num < 0 {
							t := time1
							time1 = time2
							time2 = t
							num = num * (-1)
							flag = 1
						}
						if num > 0 {
							if time2.Day() < time1.Day() {
								num--
							} else if time2.Day() == time1.Day() {
								h2, m2, s2 := time2.Clock()
								h1, m1, s1 := time1.Clock()
								intervalHour := h2 - h1
								intervalMinute := m2 - m1
								inters := s2 - s1
								if intervalHour < 0 {
									num--
								} else if intervalHour == 0 {
									if intervalMinute < 0 {
										num--
									} else if intervalMinute == 0 {
										if inters < 0 {
											num--
										} else if inters == 0 {
											if nanodiff < 0 {
												num--
											}
										}
									}
								}
							}
						}
						if flag == 1 {
							num = num * (-1)
						}
						num = num / 3
					case "month", "mon", "months":
						num = int64((time2.Year()-time1.Year())*12 + int(time2.Month()) - int(time1.Month()))
						if num < 0 {
							t := time1
							time1 = time2
							time2 = t
							num = num * (-1)
							flag = 1
						}
						if num > 0 {
							if time2.Day() < time1.Day() {
								num--
							} else if time2.Day() == time1.Day() {
								h2, m2, s2 := time2.Clock()
								h1, m1, s1 := time1.Clock()
								intervalHour := h2 - h1
								intervalMinute := m2 - m1
								inters := s2 - s1
								if intervalHour < 0 {
									num--
								} else if intervalHour == 0 {
									if intervalMinute < 0 {
										num--
									} else if intervalMinute == 0 {
										if inters < 0 {
											num--
										} else if inters == 0 {
											if nanodiff < 0 {
												num--
											}
										}
									}
								}
							}
						}
						if flag == 1 {
							num = num * (-1)
						}
					case "week", "weeks":
						if dateinter > 0 {
							if timeinter.Nanos() < 0 {
								num = (dateinter - 1) / 7
							} else {
								num = dateinter / 7
							}
						}
						if dateinter < 0 {
							if timeinter.Nanos() > 0 {
								num = (dateinter + 1) / 7
							} else {
								num = dateinter / 7
							}
						}
					case "day", "days":
						if dateinter > 0 {
							if timeinter.Nanos() < 0 {
								num = dateinter - 1
							} else {
								num = dateinter
							}
						}
						if dateinter < 0 {
							if timeinter.Nanos() > 0 {
								num = dateinter + 1
							} else {
								num = dateinter
							}
						}
					case "hour", "hours", "h":
						temp := float64(dateinter*24) + (float64(timeinter.Nanos()) / float64(3600000000000))
						num = int64(temp)
					case "minute", "minutes", "m", "min":
						temp := float64(dateinter*24*60) + (float64(timeinter.Nanos()) / float64(60000000000))
						num = int64(temp)
					case "second", "seconds", "sec":
						temp := float64(dateinter*24*60*60) + (float64(timeinter.Nanos()) / float64(1000000000))
						num = int64(temp)
					case "frac_second", "frac_seconds", "millisecond", "milliseconds":
						temp := float64(dateinter*24*60*60*1000) + (float64(timeinter.Nanos()) / float64(1000000))
						num = int64(temp)
					case "microsecond", "microseconds":
						temp := float64(dateinter*24*60*60*1000*1000) + (float64(timeinter.Nanos()) / float64(1000))
						num = int64(temp)
					default:
						return nil, pgerror.NewError(pgcode.InvalidDatetimeFormat, "invalid input")
					}
					return tree.NewDInt(tree.DInt(num)), nil
				} else if !mark1 && !mark2 {
					time1 := ttime1.(*tree.DTime)
					time2 := ttime2.(*tree.DTime)
					var num int
					inter := timeofday.Difference(timeofday.TimeOfDay(*time2), timeofday.TimeOfDay(*time1))
					k := inter.Nanos()
					switch str {
					case "hour", "hours", "h":
						num, _ = tree.DatePartIntervalsec("hours", k/1000000000)
					case "minute", "minutes", "m", "min":
						num, _ = tree.DatePartIntervalsec("minutes", k/1000000000)
					case "second", "seconds", "sec":
						num, _ = tree.DatePartIntervalsec("second", k/1000000000)
					case "frac_second", "frac_seconds", "millisecond", "milliseconds":
						num = int(k / 1000000)
					case "microsecond", "microseconds":
						num = int(k / 1000)
					default:
						return nil, pgerror.NewError(pgcode.InvalidDatetimeFormat, "invalid input")
					}
					return tree.NewDInt(tree.DInt(num)), nil
				}
				return tree.DNull, nil
			},
			Info: "Caculate the interval between timestamp2 and timestamp1 according to the unit",
		},
	),

	"current_time": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Time),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				_, offset := ctx.StmtTimestamp.In(ctx.GetLocation()).Zone()
				res := ctx.GetStmtTimestamp().Add(time.Duration(offset) * time.Second)
				tRes := tree.MakeDTimestamp(res, time.Microsecond)
				Dres, _ := tree.PerformCast(ctx, tRes, coltypes.Time)
				return Dres, nil
			},
			Info: txnTSDoc,
		},
	),

	"current_date": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				t := ctx.GetTxnTimestamp(time.Microsecond).Time
				return tree.NewDDateFromTime(t, ctx.GetLocation()), nil
			},
			Info: "Returns the date of the current transaction." + txnTSContextDoc,
		},
	),

	"date_add": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime, Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "Date", Typ: types.Date}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date := args[0].(*tree.DDate)
				timeStamp := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date).Time
				inter := strings.Trim(args[1].String(), "'")
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				if strings.Contains(inter, "year") {
					addYear, _ := strconv.Atoi(strings.Split(inter, " ")[0])
					if year%4 == 0 && month == 2 && day == 29 {
						if (year+addYear)%4 != 0 {
							day--
						}
					}
					year += addYear
				}

				if strings.Contains(inter, "mon") {
					interSlice := strings.Split(inter, " ")
					var monIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "mon") {
							monIndex = i - 1
							break
						}
					}
					addMonth, _ := strconv.Atoi(interSlice[monIndex])
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
					switch day {
					case 29:
						if day == 29 && month == 2 {
							if year%4 != 0 {
								day = 28
							}
						}
					case 30:
						if day == 30 && month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					case 31:
						if day == 31 {
							if month == 4 || month == 6 || month == 9 || month == 11 {
								day = 30
							} else if month == 2 {
								if year%4 == 0 {
									day = 29
								} else {
									day = 28
								}
							}
						}
					}
				}
				hour, minute, second := timeStamp.Clock()
				nanosecond := timeStamp.Nanosecond()
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day) + " " + strconv.Itoa(hour) + ":" + strconv.Itoa(minute) + ":" + strconv.Itoa(second) + "." + strconv.Itoa(nanosecond)
				dTimeStamp, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				timeStamp = dTimeStamp.Time

				if strings.Contains(inter, "day") {
					interSlice := strings.Split(inter, " ")
					var dayIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "day") {
							dayIndex = i - 1
							break
						}
					}
					day, _ := strconv.Atoi(interSlice[dayIndex])
					timeStamp = timeStamp.AddDate(0, 0, day)
				}

				if strings.Contains(inter, ":") {
					interSlice := strings.Split(inter, " ")
					var timeIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], ":") {
							timeIndex = i
							break
						}
					}
					timeSlice := strings.Split(interSlice[timeIndex], ":")
					h, _ := strconv.Atoi(timeSlice[0])
					m, _ := strconv.Atoi(timeSlice[1])
					s, _ := strconv.Atoi(timeSlice[2][0:2])
					interToSecond := h*3600 + m*60 + s
					timeStamp = timeStamp.Add(time.Duration(interToSecond) * time.Second)
				}

				if strings.Contains(inter, ".") {
					interSlice := strings.Split(inter, ".")
					var interMicro int
					var interMilli int
					if len(interSlice[1]) > 3 {
						interMilli, _ = strconv.Atoi(interSlice[1][0:3])
						timeStamp = timeStamp.Add(time.Duration(interMilli) * time.Millisecond)
						switch len(interSlice[1][3:]) {
						case 1:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 100
						case 2:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 10
						case 3:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
						}
						timeStamp = timeStamp.Add(time.Duration(interMicro) * time.Microsecond)
					} else {
						switch len(interSlice[1]) {
						case 1:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 100
						case 2:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 10
						case 3:
							interMilli, _ = strconv.Atoi(interSlice[1])
						}
						timeStamp = timeStamp.Add(time.Duration(interMilli) * time.Millisecond)
					}
				}
				return tree.MakeDTimestamp(timeStamp, time.Microsecond), nil
			},
			Info: "Returns the timestamp of the given date plus the given time interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "TimeStr", Typ: types.String}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var timeStr string
				var dTimestamp tree.DTimestamp
				DatumeTime, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					return tree.DNull, nil
				}
				dTimestamp = tree.MustBeDTimestamp(DatumeTime)
				timeStamp := dTimestamp.Time
				inter := strings.Trim(args[1].String(), "'")

				timestStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timestStr[:4])
				month, _ := strconv.Atoi(timestStr[5:7])
				day, _ := strconv.Atoi(timestStr[8:])
				if strings.Contains(inter, "year") {
					addYear, _ := strconv.Atoi(strings.Split(inter, " ")[0])
					if year%4 == 0 && month == 2 && day == 29 {
						if (year+addYear)%4 != 0 {
							day--
						}
					}
					year += addYear
				}

				if strings.Contains(inter, "mon") {
					interSlice := strings.Split(inter, " ")
					var monIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "mon") {
							monIndex = i - 1
							break
						}
					}
					addMonth, _ := strconv.Atoi(interSlice[monIndex])
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
					switch day {
					case 29:
						if day == 29 && month == 2 {
							if year%4 != 0 {
								day = 28
							}
						}
					case 30:
						if day == 30 && month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					case 31:
						if day == 31 {
							if month == 4 || month == 6 || month == 9 || month == 11 {
								day = 30
							} else if month == 2 {
								if year%4 == 0 {
									day = 29
								} else {
									day = 28
								}
							}
						}
					}
				}
				hour, minute, second := timeStamp.Clock()
				nanosecond := timeStamp.Nanosecond()
				if year >= 10000 {
					return tree.DNull, nil
				}
				timeStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day) + " " + strconv.Itoa(hour) + ":" + strconv.Itoa(minute) + ":" + strconv.Itoa(second) + "." + strconv.Itoa(nanosecond)
				DatumeTime2, err2 := tree.PerformCast(ctx, tree.NewDString(timeStr), coltypes.Timestamp)
				if err2 != nil {
					return tree.DNull, nil
				}
				dTimestamp = tree.MustBeDTimestamp(DatumeTime2)
				timeStamp = dTimestamp.Time

				if strings.Contains(inter, "day") {
					interSlice := strings.Split(inter, " ")
					var dayIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "day") {
							dayIndex = i - 1
							break
						}
					}
					day, _ := strconv.Atoi(interSlice[dayIndex])
					timeStamp = timeStamp.AddDate(0, 0, day)
				}

				if strings.Contains(inter, ":") {
					interSlice := strings.Split(inter, " ")
					var timeIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], ":") {
							timeIndex = i
							break
						}
					}
					timeSlice := strings.Split(interSlice[timeIndex], ":")
					h, _ := strconv.Atoi(timeSlice[0])
					m, _ := strconv.Atoi(timeSlice[1])
					s, _ := strconv.Atoi(timeSlice[2][0:2])
					interToSecond := h*3600 + m*60 + s
					timeStamp = timeStamp.Add(time.Duration(interToSecond) * time.Second)
				}

				if strings.Contains(inter, ".") {
					interSlice := strings.Split(inter, ".")
					var interMicro int
					var interMilli int
					if len(interSlice[1]) > 3 {
						interMilli, _ = strconv.Atoi(interSlice[1][0:3])
						timeStamp = timeStamp.Add(time.Duration(interMilli) * time.Millisecond)
						switch len(interSlice[1][3:]) {
						case 1:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 100
						case 2:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 10
						case 3:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
						}
						timeStamp = timeStamp.Add(time.Duration(interMicro) * time.Microsecond)
					} else {
						switch len(interSlice[1]) {
						case 1:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 100
						case 2:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 10
						case 3:
							interMilli, _ = strconv.Atoi(interSlice[1])
						}
						timeStamp = timeStamp.Add(time.Duration(interMilli) * time.Millisecond)
					}
				}
				return tree.MakeDTimestamp(timeStamp, time.Microsecond), nil
			},
			Info: "Returns the timestamp of the given date plus the given time interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "TimeStamp", Typ: types.Timestamp}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestamp).Time
				inter := strings.Trim(args[1].String(), "'")
				res, err := tree.AddDate(ctx, timeStamp, inter)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(res, time.Microsecond), nil
			},
			Info: "Returns the timestamp of the given date plus the given time interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "TimeStamp", Typ: types.TimestampTZ}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestampTZ).Time
				inter := strings.Trim(args[1].String(), "'")
				res, err := tree.AddDate(ctx, timeStamp, inter)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(res, time.Microsecond), nil
			},
			Info: "Returns the timestamp of the given date plus the given time interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "Time", Typ: types.Time}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Time),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var timeStr string
				var dTimestamp tree.DTimestamp
				timeStr = strings.Trim(args[0].String(), "'")
				timeStr = "0001-01-01 " + timeStr
				DatumeTime, err := tree.PerformCast(ctx, tree.NewDString(timeStr), coltypes.Timestamp)
				if err != nil {
					return tree.DNull, nil
				}
				dTimestamp = tree.MustBeDTimestamp(DatumeTime)
				timeStamp := dTimestamp.Time
				inter := strings.Trim(args[1].String(), "'")

				timestStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timestStr[:4])
				month, _ := strconv.Atoi(timestStr[5:7])
				day, _ := strconv.Atoi(timestStr[8:])
				if strings.Contains(inter, "year") {
					return tree.DNull, nil
				}

				if strings.Contains(inter, "mon") {
					return tree.DNull, nil
				}
				hour, minute, second := timeStamp.Clock()
				nanosecond := timeStamp.Nanosecond()
				if year >= 10000 {
					return tree.DNull, nil
				}
				timeStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day) + " " + strconv.Itoa(hour) + ":" + strconv.Itoa(minute) + ":" + strconv.Itoa(second) + "." + strconv.Itoa(nanosecond)
				DatumeTime2, err2 := tree.PerformCast(ctx, tree.NewDString(timeStr), coltypes.Timestamp)
				if err2 != nil {
					return tree.DNull, nil
				}
				dTimestamp = tree.MustBeDTimestamp(DatumeTime2)
				timeStamp = dTimestamp.Time

				if strings.Contains(inter, "day") {
					interSlice := strings.Split(inter, " ")
					var dayIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "day") {
							dayIndex = i - 1
							break
						}
					}
					day, _ := strconv.Atoi(interSlice[dayIndex])
					timeStamp = timeStamp.AddDate(0, 0, day)
				}

				if strings.Contains(inter, ":") {
					interSlice := strings.Split(inter, " ")
					var timeIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], ":") {
							timeIndex = i
							break
						}
					}
					timeSlice := strings.Split(interSlice[timeIndex], ":")
					h, _ := strconv.Atoi(timeSlice[0])
					m, _ := strconv.Atoi(timeSlice[1])
					s, _ := strconv.Atoi(timeSlice[2][0:2])
					interToSecond := h*3600 + m*60 + s
					timeStamp = timeStamp.Add(time.Duration(interToSecond) * time.Second)
				}

				if strings.Contains(inter, ".") {
					interSlice := strings.Split(inter, ".")
					var interMicro int
					var interMilli int
					if len(interSlice[1]) > 3 {
						interMilli, _ = strconv.Atoi(interSlice[1][0:3])
						timeStamp = timeStamp.Add(time.Duration(interMilli) * time.Millisecond)
						switch len(interSlice[1][3:]) {
						case 1:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 100
						case 2:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 10
						case 3:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
						}
						timeStamp = timeStamp.Add(time.Duration(interMicro) * time.Microsecond)
					} else {
						switch len(interSlice[1]) {
						case 1:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 100
						case 2:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 10
						case 3:
							interMilli, _ = strconv.Atoi(interSlice[1])
						}
						timeStamp = timeStamp.Add(time.Duration(interMilli) * time.Millisecond)
					}
				}
				timeStr = strings.Trim(timeStamp.String(), " ")
				dayStr := string(timeStr[8:10])
				if dayStr != "01" {
					return tree.DNull, nil
				}
				timeStr = string(timeStr[11:19])
				Dres, err := tree.PerformCast(ctx, tree.NewDString(timeStr), coltypes.Time)
				if err != nil {
					return tree.DNull, nil
				}
				return Dres, nil
			},
			Info: "Returns the time of the given time plus the given time interval",
		},
	),

	"add_months": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime, Impure: true},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "Date", Typ: types.Date},
				{Name: "Interval", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date := args[0].(*tree.DDate)
				timeStamp := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date).Time
				inter := tree.MustBeDInt(args[1])
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				var addMonth int = int(inter)
				if addMonth > 0 {
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
				} else if addMonth < 0 {
					addMonth = int(math.Abs(float64(addMonth)))
					for addMonth > 0 {
						if month > 1 {
							month--
							addMonth--
						} else {
							addMonth--
							year--
							month = 12
						}
					}
				}
				switch day {
				case 29:
					if day == 29 && month == 2 {
						if year%4 != 0 {
							day = 28
						}
					}
				case 30:
					if day == 30 && month == 2 {
						if year%4 == 0 {
							day = 29
						} else {
							day = 28
						}
					}
				case 31:
					if day == 31 {
						if month == 4 || month == 6 || month == 9 || month == 11 {
							day = 30
						} else if month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					}
				}
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day)
				dTime, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				return tree.NewDDateFromTime(dTime.Time, time.Local), nil
			},
			Info: "Return the result of the month operation",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "Date", Typ: types.String},
				{Name: "Interval", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStr := string(tree.MustBeDString(args[0]))
				dTimestamp, err := tree.ParseDTimestamp(ctx, timeStr, time.Microsecond)
				if err != nil {
					return tree.DNull, nil
				}
				timeStamp := dTimestamp.Time
				inter := tree.MustBeDInt(args[1])
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				var addMonth int = int(inter)
				if addMonth > 0 {
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
				} else if addMonth < 0 {
					addMonth = int(math.Abs(float64(addMonth)))
					for addMonth > 0 {
						if month > 1 {
							month--
							addMonth--
						} else {
							addMonth--
							year--
							month = 12
						}
					}
				}
				switch day {
				case 29:
					if day == 29 && month == 2 {
						if year%4 != 0 {
							day = 28
						}
					}
				case 30:
					if day == 30 && month == 2 {
						if year%4 == 0 {
							day = 29
						} else {
							day = 28
						}
					}
				case 31:
					if day == 31 {
						if month == 4 || month == 6 || month == 9 || month == 11 {
							day = 30
						} else if month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					}
				}
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day)
				dTime, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				return tree.NewDDateFromTime(dTime.Time, time.Local), nil
			},
			Info: "Return the result of the month operation",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "Date", Typ: types.Timestamp},
				{Name: "Interval", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestamp).Time
				inter := tree.MustBeDInt(args[1])
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				var addMonth int = int(inter)
				if addMonth > 0 {
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
				} else if addMonth < 0 {
					addMonth = int(math.Abs(float64(addMonth)))
					for addMonth > 0 {
						if month > 1 {
							month--
							addMonth--
						} else {
							addMonth--
							year--
							month = 12
						}
					}
				}
				switch day {
				case 29:
					if day == 29 && month == 2 {
						if year%4 != 0 {
							day = 28
						}
					}
				case 30:
					if day == 30 && month == 2 {
						if year%4 == 0 {
							day = 29
						} else {
							day = 28
						}
					}
				case 31:
					if day == 31 {
						if month == 4 || month == 6 || month == 9 || month == 11 {
							day = 30
						} else if month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					}
				}
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day)
				dTime, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				return tree.NewDDateFromTime(dTime.Time, time.Local), nil
			},
			Info: "Return the result of the month operation",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "Date", Typ: types.Timestamp},
				{Name: "Interval", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestamp).Time
				inter := int(tree.MustBeDFloat(args[1]))
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				var addMonth int = int(inter)
				if addMonth > 0 {
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
				} else if addMonth < 0 {
					addMonth = int(math.Abs(float64(addMonth)))
					for addMonth > 0 {
						if month > 1 {
							month--
							addMonth--
						} else {
							addMonth--
							year--
							month = 12
						}
					}
				}
				switch day {
				case 29:
					if day == 29 && month == 2 {
						if year%4 != 0 {
							day = 28
						}
					}
				case 30:
					if day == 30 && month == 2 {
						if year%4 == 0 {
							day = 29
						} else {
							day = 28
						}
					}
				case 31:
					if day == 31 {
						if month == 4 || month == 6 || month == 9 || month == 11 {
							day = 30
						} else if month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					}
				}
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day)
				dTime, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				return tree.NewDDateFromTime(dTime.Time, time.Local), nil
			},
			Info: "Return the result of the month operation",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "Date", Typ: types.TimestampTZ},
				{Name: "Interval", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestampTZ).Time
				inter := tree.MustBeDInt(args[1])
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				var addMonth int = int(inter)
				if addMonth > 0 {
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
				} else if addMonth < 0 {
					addMonth = int(math.Abs(float64(addMonth)))
					for addMonth > 0 {
						if month > 1 {
							month--
							addMonth--
						} else {
							addMonth--
							year--
							month = 12
						}
					}
				}
				switch day {
				case 29:
					if day == 29 && month == 2 {
						if year%4 != 0 {
							day = 28
						}
					}
				case 30:
					if day == 30 && month == 2 {
						if year%4 == 0 {
							day = 29
						} else {
							day = 28
						}
					}
				case 31:
					if day == 31 {
						if month == 4 || month == 6 || month == 9 || month == 11 {
							day = 30
						} else if month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					}
				}
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day)
				dTime, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				return tree.NewDDateFromTime(dTime.Time, time.Local), nil
			},
			Info: "Return the result of the month operation",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "Date", Typ: types.TimestampTZ},
				{Name: "Interval", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestampTZ).Time
				inter := int(tree.MustBeDFloat(args[1]))
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				var addMonth int = int(inter)
				if addMonth > 0 {
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
				} else if addMonth < 0 {
					addMonth = int(math.Abs(float64(addMonth)))
					for addMonth > 0 {
						if month > 1 {
							month--
							addMonth--
						} else {
							addMonth--
							year--
							month = 12
						}
					}
				}
				switch day {
				case 29:
					if day == 29 && month == 2 {
						if year%4 != 0 {
							day = 28
						}
					}
				case 30:
					if day == 30 && month == 2 {
						if year%4 == 0 {
							day = 29
						} else {
							day = 28
						}
					}
				case 31:
					if day == 31 {
						if month == 4 || month == 6 || month == 9 || month == 11 {
							day = 30
						} else if month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					}
				}
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day)
				dTime, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				return tree.NewDDateFromTime(dTime.Time, time.Local), nil
			},
			Info: "Return the result of the month operation",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "Date", Typ: types.String},
				{Name: "Interval", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStr := string(tree.MustBeDString(args[0]))
				dTimestamp, err := tree.ParseDTimestamp(ctx, timeStr, time.Microsecond)
				if err != nil {
					return tree.DNull, nil
				}
				timeStamp := dTimestamp.Time
				inter := int(tree.MustBeDFloat(args[1]))
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				var addMonth int = int(inter)
				if addMonth > 0 {
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
				} else if addMonth < 0 {
					addMonth = int(math.Abs(float64(addMonth)))
					for addMonth > 0 {
						if month > 1 {
							month--
							addMonth--
						} else {
							addMonth--
							year--
							month = 12
						}
					}
				}
				switch day {
				case 29:
					if day == 29 && month == 2 {
						if year%4 != 0 {
							day = 28
						}
					}
				case 30:
					if day == 30 && month == 2 {
						if year%4 == 0 {
							day = 29
						} else {
							day = 28
						}
					}
				case 31:
					if day == 31 {
						if month == 4 || month == 6 || month == 9 || month == 11 {
							day = 30
						} else if month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					}
				}
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day)
				dTime, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				return tree.NewDDateFromTime(dTime.Time, time.Local), nil
			},
			Info: "Return the result of the month operation",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "Date", Typ: types.Date},
				{Name: "Interval", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date := args[0].(*tree.DDate)
				timeStamp := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date).Time
				inter := int(tree.MustBeDFloat(args[1]))
				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				var addMonth int = int(inter)
				if addMonth > 0 {
					for addMonth > 0 {
						if month < 12 {
							month++
							addMonth--
						} else {
							addMonth--
							year++
							month = 1
						}
					}
				} else if addMonth < 0 {
					addMonth = int(math.Abs(float64(addMonth)))
					for addMonth > 0 {
						if month > 1 {
							month--
							addMonth--
						} else {
							addMonth--
							year--
							month = 12
						}
					}
				}
				switch day {
				case 29:
					if day == 29 && month == 2 {
						if year%4 != 0 {
							day = 28
						}
					}
				case 30:
					if day == 30 && month == 2 {
						if year%4 == 0 {
							day = 29
						} else {
							day = 28
						}
					}
				case 31:
					if day == 31 {
						if month == 4 || month == 6 || month == 9 || month == 11 {
							day = 30
						} else if month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					}
				}
				timeStStr = fmt.Sprintf("%04d", year) + "-" + fmt.Sprintf("%02d", month) + "-" + fmt.Sprintf("%02d", day)
				dTime, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				return tree.NewDDateFromTime(dTime.Time, time.Local), nil
			},
			Info: "Return the result of the month operation",
		},
	),

	"date_sub": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime, Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "TimeStamp", Typ: types.Timestamp}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestamp).Time
				inter := strings.Trim(args[1].String(), "'")
				res, err := tree.SubDate(ctx, timeStamp, inter)
				if err != nil {
					return nil, err
				}
				if res.Year() < 0 {
					return tree.DNull, nil
				}
				return tree.MakeDTimestamp(res, time.Microsecond), nil
			},
			Info: "Returns the timestamp of the given date minus the given time interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "TimeStamp", Typ: types.TimestampTZ}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStamp := args[0].(*tree.DTimestampTZ).Time
				inter := strings.Trim(args[1].String(), "'")
				res, err := tree.SubDate(ctx, timeStamp, inter)
				if err != nil {
					return nil, err
				}
				if res.Year() < 0 {
					return tree.DNull, nil
				}
				return tree.MakeDTimestamp(res, time.Microsecond), nil
			},
			Info: "Returns the timestamp of the given date minus the given time interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "Date", Typ: types.Date}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date := args[0].(*tree.DDate)
				timeStamp := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date).Time
				inter := strings.Trim(args[1].String(), "'")

				timeStStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:])
				if strings.Contains(inter, "year") {
					subYear, _ := strconv.Atoi(strings.Split(inter, " ")[0])
					if year%4 == 0 && month == 2 && day == 29 {
						if (year-subYear)%4 != 0 {
							day--
						}
					}
					year -= subYear
				}

				if strings.Contains(inter, "mon") {
					interSlice := strings.Split(inter, " ")
					var monIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "mon") {
							monIndex = i - 1
							break
						}
					}
					subMonth, _ := strconv.Atoi(interSlice[monIndex])
					for subMonth > 0 {
						if month > 1 {
							month--
							subMonth--
						} else {
							subMonth--
							year--
							month = 12
						}
					}
					switch day {
					case 29:
						if day == 29 && month == 2 {
							if year%4 != 0 {
								day = 28
							}
						}
					case 30:
						if day == 30 && month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					case 31:
						if day == 31 {
							if month == 4 || month == 6 || month == 9 || month == 11 {
								day = 30
							} else if month == 2 {
								if year%4 == 0 {
									day = 29
								} else {
									day = 28
								}
							}
						}
					}
				}
				hour, minute, second := timeStamp.Clock()
				nanosecond := timeStamp.Nanosecond()
				timeStStr = strconv.Itoa(year) + "-" + strconv.Itoa(month) + "-" + strconv.Itoa(day) + " " + strconv.Itoa(hour) + ":" + strconv.Itoa(minute) + ":" + strconv.Itoa(second) + "." + strconv.Itoa(nanosecond)
				dTimeStamp, _ := tree.ParseDTimestamp(ctx, timeStStr, time.Microsecond)
				timeStamp = dTimeStamp.Time
				if strings.Contains(inter, "day") {
					interSlice := strings.Split(inter, " ")
					var dayIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "day") {
							dayIndex = i - 1
							break
						}
					}
					day, _ := strconv.Atoi(interSlice[dayIndex])
					timeStamp = timeStamp.AddDate(0, 0, -day)
				}

				if strings.Contains(inter, ":") {
					interSlice := strings.Split(inter, " ")
					var timeIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], ":") {
							timeIndex = i
							break
						}
					}
					timeSlice := strings.Split(interSlice[timeIndex], ":")
					h, _ := strconv.Atoi(timeSlice[0])
					m, _ := strconv.Atoi(timeSlice[1])
					s, _ := strconv.Atoi(timeSlice[2][0:2])
					interToSecond := h*3600 + m*60 + s
					timeStamp = timeStamp.Add(-(time.Duration(interToSecond) * time.Second))
				}

				if strings.Contains(inter, ".") {
					interSlice := strings.Split(inter, ".")
					var interMicro int
					var interMilli int
					if len(interSlice[1]) > 3 {
						interMilli, _ = strconv.Atoi(interSlice[1][0:3])
						timeStamp = timeStamp.Add(-(time.Duration(interMilli) * time.Millisecond))
						switch len(interSlice[1][3:]) {
						case 1:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 100
						case 2:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 10
						case 3:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
						}
						timeStamp = timeStamp.Add(-(time.Duration(interMicro) * time.Microsecond))
					} else {
						switch len(interSlice[1]) {
						case 1:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 100
						case 2:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 10
						case 3:
							interMilli, _ = strconv.Atoi(interSlice[1])
						}
						timeStamp = timeStamp.Add(-(time.Duration(interMilli) * time.Millisecond))
					}
				}
				if timeStamp.Year() < 0 {
					return tree.DNull, nil
				}
				return tree.MakeDTimestamp(timeStamp, time.Microsecond), nil
			},
			Info: "Returns the timestamp of the given date minus the given time interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "TimeStr", Typ: types.String}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeStr := string(tree.MustBeDString(args[0]))
				dTimestamp, err := tree.ParseDTimestamp(ctx, timeStr, time.Microsecond)
				if err != nil {
					return tree.DNull, nil
				}
				timeStamp := dTimestamp.Time
				inter := strings.Trim(args[1].String(), "'")

				timestStr := timeStamp.Format("2006-01-02")
				year, _ := strconv.Atoi(timestStr[:4])
				month, _ := strconv.Atoi(timestStr[5:7])
				day, _ := strconv.Atoi(timestStr[8:])
				if strings.Contains(inter, "year") {
					subYear, _ := strconv.Atoi(strings.Split(inter, " ")[0])
					if year%4 == 0 && month == 2 && day == 29 {
						if (year-subYear)%4 != 0 {
							day--
						}
					}
					year -= subYear
				}

				if strings.Contains(inter, "mon") {
					interSlice := strings.Split(inter, " ")
					var monIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "mon") {
							monIndex = i - 1
							break
						}
					}
					subMonth, _ := strconv.Atoi(interSlice[monIndex])
					for subMonth > 0 {
						if month > 1 {
							month--
							subMonth--
						} else {
							subMonth--
							year--
							month = 12
						}
					}
					switch day {
					case 29:
						if day == 29 && month == 2 {
							if year%4 != 0 {
								day = 28
							}
						}
					case 30:
						if day == 30 && month == 2 {
							if year%4 == 0 {
								day = 29
							} else {
								day = 28
							}
						}
					case 31:
						if day == 31 {
							if month == 4 || month == 6 || month == 9 || month == 11 {
								day = 30
							} else if month == 2 {
								if year%4 == 0 {
									day = 29
								} else {
									day = 28
								}
							}
						}
					}
				}
				hour, minute, second := timeStamp.Clock()
				nanosecond := timeStamp.Nanosecond()
				timeStr = strconv.Itoa(year) + "-" + strconv.Itoa(month) + "-" + strconv.Itoa(day) + " " + strconv.Itoa(hour) + ":" + strconv.Itoa(minute) + ":" + strconv.Itoa(second) + "." + strconv.Itoa(nanosecond)
				dTimestamp, _ = tree.ParseDTimestamp(ctx, timeStr, time.Microsecond)
				timeStamp = dTimestamp.Time
				if strings.Contains(inter, "day") {
					interSlice := strings.Split(inter, " ")
					var dayIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], "day") {
							dayIndex = i - 1
							break
						}
					}
					day, _ := strconv.Atoi(interSlice[dayIndex])
					timeStamp = timeStamp.AddDate(0, 0, -day)
				}

				if strings.Contains(inter, ":") {
					interSlice := strings.Split(inter, " ")
					var timeIndex int
					for i := 0; i < len(interSlice); i++ {
						if strings.Contains(interSlice[i], ":") {
							timeIndex = i
							break
						}
					}
					timeSlice := strings.Split(interSlice[timeIndex], ":")
					h, _ := strconv.Atoi(timeSlice[0])
					m, _ := strconv.Atoi(timeSlice[1])
					s, _ := strconv.Atoi(timeSlice[2][0:2])
					interToSecond := h*3600 + m*60 + s
					timeStamp = timeStamp.Add(-(time.Duration(interToSecond) * time.Second))
				}

				if strings.Contains(inter, ".") {
					interSlice := strings.Split(inter, ".")
					var interMicro int
					var interMilli int
					if len(interSlice[1]) > 3 {
						interMilli, _ = strconv.Atoi(interSlice[1][0:3])
						timeStamp = timeStamp.Add(-(time.Duration(interMilli) * time.Millisecond))
						switch len(interSlice[1][3:]) {
						case 1:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 100
						case 2:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
							interMicro *= 10
						case 3:
							interMicro, _ = strconv.Atoi(interSlice[1][3:])
						}
						timeStamp = timeStamp.Add(-(time.Duration(interMicro) * time.Microsecond))
					} else {
						switch len(interSlice[1]) {
						case 1:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 100
						case 2:
							interMilli, _ = strconv.Atoi(interSlice[1])
							interMilli *= 10
						case 3:
							interMilli, _ = strconv.Atoi(interSlice[1])
						}
						timeStamp = timeStamp.Add(-(time.Duration(interMilli) * time.Millisecond))
					}
				}
				if timeStamp.Year() < 0 {
					return tree.DNull, nil
				}
				return tree.MakeDTimestamp(timeStamp, time.Microsecond), nil
			},
			Info: "Returns the timestamp of the given date minus the given time interval",
		},
	),

	"systimestamp": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Return the time at which the function executes",
		},
	),

	"to_single_byte": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryString,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "String", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDString(args[0]))
				retstr := ""
				for _, i := range str {
					InsideCode := i
					if InsideCode == 12288 {
						InsideCode = 32
					} else {
						InsideCode -= 65248
					}
					if InsideCode < 32 || InsideCode > 126 {
						retstr += string(i)
					} else {
						retstr += string(InsideCode)
					}
				}
				return tree.NewDString(retstr), nil
			},
			Info: "Return char and convert all its multibyte characters to corresponding single-byte characters",
		},
	),

	"sys_extract_utc": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "time", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var Time time.Time
				Time = args[0].(*tree.DTimestampTZ).Time
				return tree.MakeDTimestampTZ(Time.UTC(), time.Microsecond), nil
			},
			Info: "Returns the standard UTC time corresponding to the specified time",
		},
	),

	"nvl2": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryCompatibility,
			Impure:       true,
			NullableArgs: true,
		},
		tree.Overload{
			Types:             tree.ArgTypes{{Name: "value1", Typ: types.Any}, {Name: "value2", Typ: types.Any}, {Name: "value3", Typ: types.Any}},
			ReturnType:        tree.FixedReturnType(types.String),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					value2 := args[2].String()
					return tree.NewDString(strings.Trim(value2, "'")), nil
				}
				value1 := args[1].String()
				return tree.NewDString(strings.Trim(value1, "'")), nil

			},
			Info: "If the first parameter is not empty, return the second parameter, otherwise return the third parameter",
		},
	),

	"lnnvl": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryCompatibility,
			Impure:       true,
			NullableArgs: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "val", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				first := args[0]
				if first.String() == "true" {
					return tree.MakeDBool(false), nil
				}
				return tree.MakeDBool(true), nil
			},
			Info: "Lnnvl is used for the condition in the where clause of a statement. " +
				"If the condition is true, it returns false; if the condition is unknown or false, it returns true.",
		},
	),

	"nanvl": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryCompatibility,
			Impure:       true,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val1", Typ: types.Any}, {Name: "val2", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var val [2]float64
				var err error
				var res [2]types.T
				for i := 0; i < 2; i++ {
					if args[i] == tree.DNull {
						return tree.DNull, nil
					}
					res[i] = args[i].ResolvedType()
					switch res[i] {
					case types.Int:
						val[i] = float64(tree.MustBeDInt(args[i]))
					case types.Float:
						val[i] = float64(tree.MustBeDFloat(args[i]))
					case types.Decimal:
						val[i], err = args[i].(*tree.DDecimal).Float64()
						if err != nil {
							return nil, errors.New("Invalid input parameter")
						}
					case types.String:
						val[i], err = strconv.ParseFloat(string(tree.MustBeDString(args[i])), 64)
						if err != nil {
							return nil, fmt.Errorf("invalid number")
						}
					default:
						return nil, fmt.Errorf("invalid number")
					}
				}
				isNaN := math.IsNaN(val[0])
				if isNaN {
					return tree.NewDFloat(tree.DFloat(val[1])), nil
				}
				return tree.NewDFloat(tree.DFloat(val[0])), nil
			},
			Info: "Returns an alternative value val1 if the input value val2 is NaN (not a number). If val2 is not NaN, then returns val2.",
		},
	),

	"round_ties_to_even": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryMath,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "value", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				value := float64(tree.MustBeDFloat(args[0]))
				value = math.RoundToEven(value)
				return tree.NewDInt(tree.DInt(value)), nil
			},
			Info: "nil",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "value", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDString(args[0]))
				if !strings.Contains(str, ".") {
					Value, _ := strconv.Atoi(str)
					EvenValue := math.RoundToEven(float64(Value))
					return tree.NewDInt(tree.DInt(EvenValue)), nil
				}
				StrSplice := strings.Split(str, ".")
				InterGer, _ := strconv.Atoi(StrSplice[0])
				decimal, _ := strconv.Atoi(StrSplice[1])
				LenthDecimal := len(string(strconv.Itoa(decimal)))
				EvenValue := float64(InterGer) + float64(decimal)*math.Pow(0.1, float64(LenthDecimal))
				EvenValue = math.RoundToEven(EvenValue)
				return tree.NewDInt(tree.DInt(EvenValue)), nil
			},
			Info: "nil",
		},
	),

	"tz_offset": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "zoneName", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				zoneName := string(tree.MustBeDString(args[0]))

				l, err := time.LoadLocation(zoneName)
				if err != nil {
					zoneName = strings.ToLower(zoneName)
					switch zoneName {
					case "sessiontimezone":
						location := ctx.GetLocation()
						strTime := timeutil.Now()
						t := strTime.In(location).Format(time.RFC822Z)
						result := t[16:17] + t[17:19] + ":" + t[19:]
						return tree.NewDString(result), nil
					case "dbtimezone":
						return tree.NewDString(tree.ServerTimeZone), nil
					}

					if strings.Contains(zoneName, ":") {
						zoneNameSplite := strings.Split(zoneName, ":")
						hour, err := strconv.Atoi(zoneNameSplite[0])
						if err != nil {
							return tree.DNull, errors.New("Invalid input parameter")
						}
						if strings.Contains(zoneNameSplite[1], ".") {
							zoneNameSplite[1] = strings.Split(zoneNameSplite[1], ".")[0]
						}
						minute, err := strconv.Atoi(zoneNameSplite[1])
						if err != nil {
							return tree.DNull, errors.New("Invalid input parameter")
						}
						if hour >= -12 && hour <= 14 && -59 <= minute && 59 >= minute {
							var result string
							if hour > 0 {
								result = "+"
							} else {
								result = "-"
							}
							if math.Abs(float64(hour)) < 10 && math.Abs(float64(hour)) >= 0 {
								if hour < 0 {
									result = result + "0" + strconv.Itoa(-hour) + ":"
								} else {
									result = result + "0" + strconv.Itoa(hour) + ":"
								}
							} else {
								if hour < 0 {
									result = result + strconv.Itoa(-hour) + ":"
								} else {
									result = result + strconv.Itoa(hour) + ":"
								}
							}
							if minute < 10 {
								result = result + "0" + strconv.Itoa(minute)
							} else {
								result = result + strconv.Itoa(minute)
							}
							return tree.NewDString(result), nil
						}
					}
					return nil, err
				}
				strTime := timeutil.Now()
				t := strTime.In(l).Format(time.RFC822Z)
				result := t[16:17] + t[17:19] + ":" + t[19:]
				return tree.NewDString(result), nil
			},
			Info: "Returns the time zone offset corresponding to the argument based on the date the statement is executed",
		},
	),

	"bitand": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryMath,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input1", Typ: types.Decimal}, {Name: "input2", Typ: types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputSlice1 := strings.Split(args[0].(*tree.DDecimal).String(), ".")
				inputSlice2 := strings.Split(args[1].(*tree.DDecimal).String(), ".")
				input1, err1 := strconv.Atoi(inputSlice1[0])
				if err1 != nil {
					return nil, errors.New("value out of range")
				}
				input2, err2 := strconv.Atoi(inputSlice2[0])
				if err2 != nil {
					return nil, errors.New("value out of range")
				}
				return tree.NewDFloat(tree.DFloat(int64(input1) & int64(input2))), nil
			},
			Info: "The BITAND function treats its inputs and its output as vectors of bits; the output is the bitwise AND of the inputs." +
				"The arguments must be in the range -(2(n-1)) .. ((2(n-1))-1). (n=64)If an argument is out of this range, the result is uncertain.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input1", Typ: types.String}, {Name: "input2", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				input3 := string(tree.MustBeDString(args[0]))
				input4 := string(tree.MustBeDString(args[1]))
				var inputOfInt1 int64
				var inputOfInt2 int64
				if strings.Contains(input3, ".") {
					inputSlice := strings.Split(input3, ".")
					strInput := inputSlice[0]
					_, err := strconv.Atoi(inputSlice[1])
					if err != nil {
						return nil, errors.New("value out of range or invalid input parameter")
					}
					value, err := strconv.Atoi(strInput)
					if err != nil {
						return nil, errors.New("value out of range or invalid input parameter")
					}
					inputOfInt1 = int64(value)
				} else {
					value, err := strconv.Atoi(input3)
					if err != nil {
						return nil, errors.New("value out of range or invalid input parameter")
					}
					inputOfInt1 = int64(value)
				}
				if strings.Contains(input4, ".") {
					inputSlice := strings.Split(input4, ".")
					strInput := inputSlice[0]
					_, err := strconv.Atoi(inputSlice[1])
					if err != nil {
						return nil, errors.New("value out of range or invalid input parameter")
					}
					value, err := strconv.Atoi(strInput)
					if err != nil {
						return nil, errors.New("value out of range or invalid input parameter")
					}
					inputOfInt2 = int64(value)
				} else {
					value, err := strconv.Atoi(input4)
					if err != nil {
						return nil, errors.New("value out of range or invalid input parameter")
					}
					inputOfInt2 = int64(value)
				}
				return tree.NewDFloat(tree.DFloat(inputOfInt1 & inputOfInt2)), nil
			},
			Info: "The BITAND function treats its inputs and its output as vectors of bits; the output is the bitwise AND of the inputs." +
				"The arguments must be in the range -(2(n-1)) .. ((2(n-1))-1). (n=64)If an argument is out of this range, the result is uncertain.",
		},
	),

	"remainder": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryMath,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input1", Typ: types.Int}, {Name: "input2", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				input1 := float64(tree.MustBeDInt(args[0]))
				input2 := float64(tree.MustBeDInt(args[1]))
				return tree.NewDFloat(tree.DFloat(math.Remainder(input1, input2))), nil
			},
			Info: "REMAINDER returns the remainder of n2 divided by n1.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input1", Typ: types.Decimal}, {Name: "input2", Typ: types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				input1, err := args[0].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				input1Num := 0
				if len(strings.Split(fmt.Sprintf("%v", input1), ".")) == 1 {
					input1Num = 0
				} else {
					input1Num = len(strings.Split(fmt.Sprintf("%v", input1), ".")[1])
				}
				input2, err := args[1].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				input2Num := 0
				if len(strings.Split(fmt.Sprintf("%v", input2), ".")) == 1 {
					input2Num = 0
				} else {
					input2Num = len(strings.Split(fmt.Sprintf("%v", input2), ".")[1])
				}

				if input1Num < input2Num {
					input1Num = input2Num
				}

				result := math.Remainder(input1, input2)
				floatStr := fmt.Sprintf("%."+strconv.Itoa(input1Num)+"f", result)
				result, _ = strconv.ParseFloat(floatStr, 64)
				return tree.NewDFloat(tree.DFloat(result)), nil
			},
			Info: "REMAINDER returns the remainder of n2 divided by n1.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input1", Typ: types.String}, {Name: "input2", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				valOne, err := strconv.ParseFloat(string(tree.MustBeDString(args[0])), 64)
				if err != nil {
					return nil, errors.New("Invalid input parameter")
				}
				valTwo, err := strconv.ParseFloat(string(tree.MustBeDString(args[1])), 64)
				if err != nil {
					return nil, errors.New("Invalid input parameter")
				}
				return tree.NewDFloat(tree.DFloat(math.Remainder(valOne, valTwo))), nil
			},
			Info: "REMAINDER returns the remainder of n2 divided by n1.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input1", Typ: types.Decimal}, {Name: "input2", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				valOne, err := args[0].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, errors.New("Invalid input parameter")
				}
				valTwo, err := strconv.ParseFloat(string(tree.MustBeDString(args[1])), 64)
				if err != nil {
					return nil, errors.New("Invalid input parameter")
				}
				return tree.NewDFloat(tree.DFloat(math.Remainder(valOne, valTwo))), nil
			},
			Info: "REMAINDER returns the remainder of n2 divided by n1.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input1", Typ: types.String}, {Name: "input2", Typ: types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				valOne, err := strconv.ParseFloat(string(tree.MustBeDString(args[0])), 64)
				if err != nil {
					return nil, errors.New("Invalid input parameter")
				}
				valTwo, err := args[1].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, errors.New("Invalid input parameter")
				}
				return tree.NewDFloat(tree.DFloat(math.Remainder(valOne, valTwo))), nil
			},
			Info: "REMAINDER returns the remainder of n2 divided by n1.",
		},
	),

	"local_now": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.Timestamp),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(timeutil.LocalNow(), time.Microsecond), nil
			},
			Info: "Return the local time. ",
		},
	),
	"hour": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				result, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					res, err2 := tree.PerformCast(ctx, args[0], coltypes.Time)
					if err2 != nil {
						if strings.Contains(err.Error(), "separator") {
							return nil, err2
						}
						if strings.Contains(err.Error(), "cannot interpret field:") ||
							strings.Contains(err.Error(), "time out of range:") {
							return nil, fmt.Errorf("field value %s cannot convert to a time/timestamp", args[0])
						}
						return nil, err

					}
					fromtime := res.(*tree.DTime)
					time := timeofday.TimeOfDay(*fromtime)
					return tree.NewDInt(tree.DInt(time.Hour())), nil
				}
				date := result.(*tree.DTimestamp)
				return tree.NewDInt(tree.DInt(date.Hour())), nil
			},
			Info: "Returns an integer value between 0 and 23 that represents the corresponding hour of the day",
		},
	),

	"minute": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				result, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					res, err2 := tree.PerformCast(ctx, args[0], coltypes.Time)
					if err2 != nil {
						if strings.Contains(err.Error(), "separator") {
							return nil, err2
						}
						if strings.Contains(err.Error(), "cannot interpret field:") ||
							strings.Contains(err.Error(), "time out of range:") {
							return nil, fmt.Errorf("field value %s cannot convert to a time/timestamp", args[0])
						}
						return nil, err
					}
					fromtime := res.(*tree.DTime)
					time := timeofday.TimeOfDay(*fromtime)
					return tree.NewDInt(tree.DInt(time.Minute())), nil
				}
				date := result.(*tree.DTimestamp)
				return tree.NewDInt(tree.DInt(date.Minute())), nil
			},
			Info: "Returns an integer value between 0 and 59 that represents the corresponding minutes of an hour.",
		},
	),

	"second": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				result, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					res, err2 := tree.PerformCast(ctx, args[0], coltypes.Time)
					if err2 != nil {
						if strings.Contains(err.Error(), "separator") {
							return nil, err2
						}
						if strings.Contains(err.Error(), "cannot interpret field:") ||
							strings.Contains(err.Error(), "time out of range:") {
							return nil, fmt.Errorf("field value %s cannot convert to a time/timestamp", args[0])
						}
						return nil, err
					}
					fromtime := res.(*tree.DTime)
					time := timeofday.TimeOfDay(*fromtime)
					return tree.NewDInt(tree.DInt(time.Second())), nil
				}
				date := result.(*tree.DTimestamp)
				return tree.NewDInt(tree.DInt(date.Second())), nil
			},
			Info: "Returns an integer value between 0 and 59, representing the corresponding number of seconds per minute.",
		},
	),

	"year": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				result, err := tree.PerformCast(ctx, args[0], coltypes.Date)
				if err != nil {
					if strings.Contains(err.Error(), "cannot interpret field:") ||
						strings.Contains(err.Error(), "time out of range:") ||
						strings.Contains(err.Error(), "unexpected separator") {
						return nil, fmt.Errorf("field value %s cannot convert to a date", args[0])
					}
					return nil, err
				}
				Dresult := result.(*tree.DDate)
				time := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), Dresult)
				return tree.NewDInt(tree.DInt(time.Year())), nil

			},
			Info: "Returns an integer value representing the year.",
		},
	),

	"month": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				result, err := tree.PerformCast(ctx, args[0], coltypes.TimestampTZ)
				if err != nil {
					if strings.Contains(err.Error(), "cannot interpret field:") ||
						strings.Contains(err.Error(), "time out of range:") ||
						strings.Contains(err.Error(), "unexpected separator") {
						return nil, fmt.Errorf("field value %s cannot convert to a date", args[0])
					}
					return nil, err
				}
				time := result.(*tree.DTimestampTZ)
				return tree.NewDInt(tree.DInt(time.Month())), nil
			},
			Info: "Returns an integer value between 1 and 12 that represents the corresponding month of the year",
		},
	),

	"last_day": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTZ := tree.MakeDTimestampTZFromDate(time.Local, args[0].(*tree.DDate))
				firsttime := time.Date(fromTZ.Year(), fromTZ.Month(), 1, fromTZ.Hour(), fromTZ.Minute(), fromTZ.Second(), fromTZ.Nanosecond(), fromTZ.Location())
				lasttime := firsttime.AddDate(0, 1, -1)
				return tree.NewDDateFromTime(lasttime, lasttime.Location()), nil
			},
			Info: "Returns the date of the last day of the month in which the date was",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTZ := tree.MustBeDTimestampTZ(args[0])
				firsttime := time.Date(fromTZ.Year(), fromTZ.Month(), 1, fromTZ.Hour(), fromTZ.Minute(), fromTZ.Second(), fromTZ.Nanosecond(), fromTZ.Location())
				lasttime := firsttime.AddDate(0, 1, -1)
				return tree.NewDDateFromTime(lasttime, lasttime.Location()), nil
			},
			Info: "Returns the date of the last day of the month in which the date was",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTZ := tree.MustBeDTimestamp(args[0])
				firsttime := time.Date(fromTZ.Year(), fromTZ.Month(), 1, fromTZ.Hour(), fromTZ.Minute(), fromTZ.Second(), fromTZ.Nanosecond(), fromTZ.Location())
				lasttime := firsttime.AddDate(0, 1, -1)
				return tree.NewDDateFromTime(lasttime, lasttime.Location()), nil
			},
			Info: "Returns the date of the last day of the month in which the date was",
		},
	),

	"now":                   txnTSImpl,
	"current_timestamp":     txnTSImpl,
	"transaction_timestamp": txnTSImpl,

	"statement_timestamp": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the start time of the current statement.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the start time of the current statement.",
		},
	),

	tree.FollowerReadTimestampFunctionName: makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ts, err := recentTimestamp(ctx)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestampTZ(ts, time.Microsecond), nil
			},
			Info: `Returns a timestamp which is very likely to be safe to perform
against a follower replica.

This function is intended to be used with an AS OF SYSTEM TIME clause to perform
historical reads against a time which is recent but sufficiently old for reads
to be performed against the closest replica as opposed to the currently
leaseholder for a given range.

Note that this function requires an enterprise license on a ICL distribution to
return without an error.`,
		},
	),

	"cluster_logical_timestamp": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return ctx.GetClusterTimestamp(), nil
			},
			Info: `Returns the logical time of the current transaction.

This function is reserved for testing purposes by ZNBaseDB
developers and its definition may change without prior notice.

Note that uses of this function disable server-side optimizations and
may increase either contention or retry errors, or both.`,
		},
	),

	"clock_timestamp": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current system time on one of the cluster nodes.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current system time on one of the cluster nodes.",
		},
	),

	"timeofday": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(timeutil.Now().Format("Mon Jan 2 15:04:05 2006 MST")), nil
			},
			Info: "Returns the current system time on one of the cluster nodes.",
		},
	),

	//"timeofday": makeBuiltin(
	//	tree.FunctionProperties{
	//		Category: categoryDateAndTime,
	//		Impure:   true,
	//	},
	//	tree.Overload{
	//		Types:             tree.ArgTypes{},
	//		ReturnType:        tree.FixedReturnType(types.String),
	//		PreferredOverload: true,
	//		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	//			//d:=ctx.GetTxnTimestamp(time.Microsecond).UTC()
	//			d:=ctx.TxnTimestamp
	//			_=d
	//			return tree.NewDString(ctx.GetTxnTimestamp(time.Microsecond).Time.Format("Mon Jan 2 15:04:05 2006 MST")), nil
	//		},
	//		Info: "Returns the current system time on one of the cluster nodes.",
	//	},
	//),
	"extract": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[1].(*tree.DTimestamp)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "Interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				res := args[1].String()
				res = res[1 : len(res)-1]
				format := string(tree.MustBeDString(args[0]))
				ret, err := tree.DatePartInterval(format, res)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(ret)), nil
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				fromTSTZ := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Time}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[1].(*tree.DTime)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTime(fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond, epoch",
		},
	),

	"extract_duration": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromInterval := *args[1].(*tree.DInterval)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				switch timeSpan {
				case "hour", "hours":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Hour))), nil

				case "minute", "minutes":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Minute))), nil

				case "second", "seconds":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Second))), nil

				case "millisecond", "milliseconds":
					// This a PG extension not supported in MySQL.
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Millisecond))), nil

				case "microsecond", "microseconds":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Microsecond))), nil

				default:
					return nil, pgerror.NewErrorf(
						pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
				}
			},
			Info: "Extracts `element` from `input`.\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
	//
	// PostgreSQL documents date_trunc for timestamp, timestamptz and
	// interval. It will also handle date and time inputs by casting them,
	// so we support those for compatibility. This gives us the following
	// function signatures:
	//
	//  date_trunc(string, time)        -> interval
	//  date_trunc(string, date)        -> timestamptz
	//  date_trunc(string, timestamp)   -> timestamp
	//  date_trunc(string, timestamptz) -> timestamptz
	//
	// See the following snippet from running the functions in PostgreSQL:
	//
	// 		postgres=# select pg_typeof(date_trunc('month', '2017-04-11 00:00:00'::timestamp));
	// 							pg_typeof
	// 		-----------------------------
	// 		timestamp without time zone
	//
	// 		postgres=# select pg_typeof(date_trunc('month', '2017-04-11 00:00:00'::date));
	// 						pg_typeof
	// 		--------------------------
	// 		timestamp with time zone
	//
	// 		postgres=# select pg_typeof(date_trunc('month', '2017-04-11 00:00:00'::time));
	// 		pg_typeof
	// 		-----------
	// 		interval
	//
	// This implicit casting behavior is mentioned in the PostgreSQL documentation:
	// https://www.postgresql.org/docs/10/static/functions-datetime.html
	// > source is a value expression of type timestamp or interval. (Values
	// > of type date and time are cast automatically to timestamp or interval,
	// > respectively.)
	//
	"date_trunc": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTS := args[1].(*tree.DTimestamp)
				tsTZ, err := truncateTimestamp(ctx, fromTS.Time, timeSpan)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(tsTZ.Time.In(ctx.GetLocation()), time.Microsecond), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				fromTSTZ := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				return truncateTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Time}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTime := args[1].(*tree.DTime)
				time, err := truncateTime(fromTime, timeSpan)
				if err != nil {
					return nil, err
				}
				return &tree.DInterval{Duration: duration.MakeDuration(int64(*time)*1000, 0, 0)}, nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return truncateTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},

		tree.Overload{
			Types: tree.ArgTypes{{Name: "text", Typ: types.String},
				{Name: "interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args.Len() == 0 {
					return nil, errors.New("invalid number of arguments")
				}
				sText := string(*args[0].(*tree.DString))
				dDuration := args[1].(*tree.DInterval).Duration
				sResult, err := duration.DateTruncExtract(sText, dDuration)
				if err != nil {
					return nil, err
				}

				return tree.NewDString(sResult), nil
			},
			Info: "从日期/时间值中抽取子域，例如年或者小时等",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "text", Typ: types.String},
				{Name: "date", Typ: types.TimestampTZ},
				{Name: "zone", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args.Len() == 0 {
					return nil, errors.New("invalid number of arguments")
				}
				var tResult time.Time
				sText := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				tsTZ, err := truncateTimestamp(ctx, fromTSTZ.Time, sText)
				if err != nil {
					return nil, err
				}
				if sText == "hour" || sText == "hours" || sText == "minute" || sText == "minutes" || sText == "second" ||
					sText == "seconds" || sText == "microsecond" || sText == "microseconds" || sText == "millisecond" || sText == "milliseconds" {
					return tsTZ, nil
				}
				sZone := string(*args[2].(*tree.DString))
				lZone, ParErr := strconv.ParseInt(sZone, 16, 64)
				if ParErr != nil {
					lLocation, err := time.LoadLocation(sZone)
					if err != nil {
						return nil, err
					}
					tResult = time.Date(tsTZ.Year(), tsTZ.Month(), tsTZ.Day(), tsTZ.Hour(), tsTZ.Minute(), tsTZ.Second(), tsTZ.Nanosecond(), lLocation)
				} else {
					utc, err := time.LoadLocation("UTC")
					if err != nil {
						return nil, err
					}
					tResult = time.Date(tsTZ.Year(), tsTZ.Month(), tsTZ.Day(), tsTZ.Hour()+int(lZone), tsTZ.Minute(), tsTZ.Second(), tsTZ.Nanosecond(), utc)
				}

				return tree.MakeDTimestampTZ(tResult, time.Microsecond), nil
			},
			Info: "从日期/时间值中抽取子域，例如年或者小时等，并按照时区做转换",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "text", Typ: types.String},
				{Name: "date", Typ: types.Timestamp},
				{Name: "zone", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args.Len() == 0 {
					return nil, errors.New("invalid number of arguments")
				}
				var tResult time.Time
				sText := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTSTZ := args[1].(*tree.DTimestamp)
				tsTZ, err := truncateTimestamp(ctx, fromTSTZ.Time, sText)
				if err != nil {
					return nil, err
				}
				if sText == "hour" || sText == "hours" || sText == "minute" || sText == "minutes" || sText == "second" ||
					sText == "seconds" || sText == "microsecond" || sText == "microseconds" || sText == "millisecond" || sText == "milliseconds" {
					utc, err := time.LoadLocation("UTC")
					if err != nil {
						return nil, err
					}
					tResult = time.Date(tsTZ.Year(), tsTZ.Month(), tsTZ.Day(), tsTZ.Hour(), tsTZ.Minute(), tsTZ.Second(), tsTZ.Nanosecond(), utc)
				}
				sZone := string(*args[2].(*tree.DString))
				lZone, ParErr := strconv.ParseInt(sZone, 16, 64)
				if ParErr != nil {
					lLocation, err := time.LoadLocation(sZone)
					if err != nil {
						return nil, err
					}
					tResult = time.Date(tsTZ.Year(), tsTZ.Month(), tsTZ.Day(), tsTZ.Hour(), tsTZ.Minute(), tsTZ.Second(), tsTZ.Nanosecond(), lLocation)
				} else {
					utc, err := time.LoadLocation("UTC")
					if err != nil {
						return nil, err
					}
					tResult = time.Date(tsTZ.Year(), tsTZ.Month(), tsTZ.Day(), tsTZ.Hour()+int(lZone), tsTZ.Minute(), tsTZ.Second(), tsTZ.Nanosecond(), utc)
				}

				return tree.MakeDTimestampTZ(tResult, time.Microsecond), nil
			},
			Info: "从日期/时间值中抽取子域，例如年或者小时等，并按照时区做转换",
		},
	),
	"to_timestamp_tz": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "fromTime", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				datestring := string(tree.MustBeDString(args[0]))
				format := string(tree.MustBeDString(args[1]))
				format1 := strings.ToLower(format)
				tizone := ""
				if strings.Contains(format1, "tzh:tzm") {
					format = format[0 : len(format)-7]
					tizone = datestring[len(datestring)-5:]
					datestring = datestring[0:len(format)]
				}
				res, err := tree.StrToDate(datestring, format, true)
				if err != nil {
					return nil, err
				}
				res = res + " " + tizone
				Dres := tree.NewDString(res)
				result, err := tree.PerformCast(ctx, Dres, coltypes.Timestamp)
				if err != nil {
					return nil, fmt.Errorf("Can't convert to a TimestampTZ Type")
				}
				result1 := result.(*tree.DTimestamp)
				return tree.MakeDTimestampTZ(result1.Time, time.Microsecond), nil

			},
			Info: "Converts the string to a fixed format timestamptz",
		},
	),
	"to_utc_timestamp_tz": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "fromTime", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				datestring := string(tree.MustBeDString(args[0]))
				format := string(tree.MustBeDString(args[1]))
				format1 := strings.ToLower(format)
				tizone := ""
				if strings.Contains(format1, "tzh:tzm") {
					format = format[0 : len(format)-7]
					tizone = datestring[len(format):]
					datestring = datestring[0:len(format)]
				}
				res, err := tree.StrToDate(datestring, format, true)
				if err != nil {
					return nil, err
				}
				res = res + " " + tizone
				Dres := tree.NewDString(res)
				result, _ := tree.PerformCast(ctx, Dres, coltypes.TimestampTZ)
				if err != nil {
					return nil, fmt.Errorf("Can't convert to a TimestampTZ Type")
				}
				return result, nil
			},
			Info: "Converts a CHAR to a value of the TIMESTAMP WITH TIME ZONE data type, standardizing the input as a UTC TIME",
		},
	),

	"to_dsinterval": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:             tree.ArgTypes{{Name: "input", Typ: types.String}},
			ReturnType:        tree.FixedReturnType(types.Interval),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputStr := string(tree.MustBeDString(args[0]))
				format := strings.ToUpper(strings.Trim(inputStr, " "))
				if strings.Contains(format, " ") {
					resStr := strings.Split(strings.Trim(format, " "), " ")
					var newStr string
					for _, v := range resStr {
						if v != "" {
							newStr += " " + v
						}
					}
					resStr = strings.Split(strings.Trim(newStr, " "), " ")
					if len(resStr) > 2 {
						return nil, fmt.Errorf("the interval is invalid")
					}
					day, err := strconv.Atoi(resStr[0])
					if err != nil {
						return nil, fmt.Errorf("the interval is invalid")
					}
					if day > 999999999 || day < -999999999 {
						return nil, fmt.Errorf("interval out of range")
					}
					hhmmssStr := strings.Split(resStr[len(resStr)-1], ":")
					hour, err := strconv.Atoi(hhmmssStr[0])
					if err != nil {
						return nil, fmt.Errorf("the interval is invalid")
					}
					min, err := strconv.Atoi(hhmmssStr[1])
					if err != nil {
						return nil, fmt.Errorf("the interval is invalid")
					}
					secstr := hhmmssStr[2]
					if strings.Contains(secstr, ".") {
						secnum := strings.Split(secstr, ".")
						fracSecs, err := strconv.Atoi(secnum[1])
						if err != nil {
							return nil, fmt.Errorf("the interval is invalid")
						}
						if fracSecs > 999999999 || fracSecs < -999999999 {
							return nil, fmt.Errorf("interval out of range")
						}
					}
					sec, err := strconv.Atoi(hhmmssStr[2])
					if err != nil {
						return nil, fmt.Errorf("the interval is invalid")
					}
					if hour > 23 {
						return nil, fmt.Errorf("hours must be between 0 and 23")
					}
					if min > 59 {
						return nil, fmt.Errorf("minutes must be between 0 and 59")
					}
					if sec > 59 {
						return nil, fmt.Errorf("seconds must be between 0 and 59")
					}
					sqlRes, err := tree.PerformCast(ctx, tree.NewDString(format), coltypes.Interval)
					if err != nil {
						return nil, fmt.Errorf("the interval is invalid")
					}
					return sqlRes, nil
				}
				isoRes, err := tree.PerformCast(ctx, args[0], coltypes.Interval)
				if err != nil {
					return nil, err
				}
				argInterval, ok := isoRes.(*tree.DInterval)
				if !ok {
					return nil, fmt.Errorf("the interval is invalid")
				}
				rex := regexp.MustCompile("[0-9]+")
				str := rex.FindAllString(format, -1)
				fmt.Println(str)
				var isotime int
				for _, v := range str {
					if strings.Contains(v, ".") {
						secnum := strings.Split(v, ".")
						fracSecs, err := strconv.Atoi(secnum[1])
						if err != nil {
							return nil, fmt.Errorf("the interval is invalid")
						}
						if fracSecs > 99999999 || fracSecs < -999999999 {
							return nil, fmt.Errorf("interval out of range")
						}
					}
					isotime, err = strconv.Atoi(v)
					if err != nil {
						return nil, fmt.Errorf("the interval is invalid")
					}
					if isotime > 999999999 || isotime < -999999999 {
						return nil, fmt.Errorf("interval out of range")
					}
				}
				nano := argInterval.Nanos()
				days := argInterval.Days
				months := argInterval.Months
				if months != 0 {
					return nil, fmt.Errorf("the interval is invalid")
				}
				if days > 999999999 || days < -999999999 {
					return nil, fmt.Errorf("interval out of range")
				}
				return &tree.DInterval{Duration: duration.MakeDuration(nano, days, int64(0))}, nil
			},
			Info: "Converts its parameters to values of the data type by second to day",
		},
	),

	"to_yminterval": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
			Impure:   true,
		},
		tree.Overload{
			Types:             tree.ArgTypes{{Name: "input", Typ: types.String}},
			ReturnType:        tree.FixedReturnType(types.Interval),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				inputStr := string(tree.MustBeDString(args[0]))
				str := strings.ToUpper(strings.Trim(inputStr, " "))
				if strings.Contains(str, "-") {
					if strings.HasPrefix(str, "-") {
						negArr := strings.Trim(strings.Trim(str, "-"), " ")
						arr := strings.Split(negArr, "-")
						if len(arr) > 2 {
							return nil, fmt.Errorf("the interval is invalid")
						}
						y, err1 := strconv.Atoi(strings.Trim(arr[0], " "))
						m, err2 := strconv.Atoi(strings.Trim(arr[1], " "))
						if err1 != nil || err2 != nil {
							return nil, fmt.Errorf("the interval is invalid")
						}
						m = y*12 + m
						return &tree.DInterval{Duration: duration.MakeDuration(0, 0, int64(-m))}, nil
					}
					arr := strings.Split(str, "-")
					if len(arr) > 2 {
						return nil, fmt.Errorf("the interval is invalid")
					}
					y, err1 := strconv.Atoi(strings.Trim(arr[0], " "))
					m, err2 := strconv.Atoi(strings.Trim(arr[1], " "))
					if err1 != nil || err2 != nil {
						return nil, fmt.Errorf("the interval is invalid")
					}
					m = y*12 + m
					return &tree.DInterval{Duration: duration.MakeDuration(0, 0, int64(m))}, nil
				}
				isoRes, err := tree.PerformCast(ctx, tree.NewDString(str), coltypes.Interval)
				if err != nil {
					return nil, err
				}
				argInterval, ok := isoRes.(*tree.DInterval)
				if !ok {
					return nil, fmt.Errorf("the interval is invalid")
				}
				months := argInterval.Months
				return &tree.DInterval{Duration: duration.MakeDuration(0, 0, int64(months))}, nil
			},
			Info: "Converts its parameters to values of the data type by month to year",
		},
	),

	"round_date": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Date}, {Name: "fmt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := tree.MakeDTimestampTZFromDate(time.Local, args[0].(*tree.DDate))
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					if month >= 7 {
						year++
					}
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					if day >= 16 {
						month++
					}
					day = 1
				case "day", "dy", "d":
					if week >= 4 {
						day += 7 - week
					} else {
						day -= week
					}
				case "j":
					break
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Timestamp}, {Name: "fmt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := args[0].(*tree.DTimestamp)
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					if month >= 7 {
						year++
					}
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					if day >= 16 {
						month++
					}
					day = 1
				case "day", "dy", "d":
					if week >= 4 {
						day += 7 - week
					} else {
						day -= week
					}
				case "j":
					break
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.TimestampTZ}, {Name: "fmt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := args[0].(*tree.DTimestampTZ)
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					if month >= 7 {
						year++
					}
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					if day >= 16 {
						month++
					}
					day = 1
				case "day", "dy", "d":
					if week >= 4 {
						day += 7 - week
					} else {
						day -= week
					}
				case "j":
					break
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
		},
	),

	"to_time": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "fromTime", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Time),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				Stime, err := tree.PerformCast(Ctx, args[0], coltypes.String)
				if err != nil {
					return tree.DNull, nil
				}
				res, _ := tree.StrToTime(Ctx, Stime)
				return res, nil
			},
			Info: "Converts the string to a time.time",
		},
	),

	"sleep": makeBuiltin(
		tree.FunctionProperties{
			// sleep is marked as impure so it doesn't get executed during normalization.
			Impure: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "seconds", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				durationNanos := int64(float64(*args[0].(*tree.DFloat)) * float64(1000000000))
				dur := time.Duration(durationNanos)
				select {
				case <-ctx.Ctx().Done():
					return nil, ctx.Ctx().Err()
				case <-time.After(dur):
					return tree.DBoolTrue, nil
				}
			},
			Info: "sleep makes the current session's process sleep until " +
				"seconds seconds have elapsed. seconds is a value of type " +
				"double precision, so fractional-second delays can be specified.",
		},
	),

	// Math functions
	"abs": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Abs(x))), nil
		}, "Calculates the absolute value of `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			dd.Abs(x)
			return dd, nil
		}, "Calculates the absolute value of `val`."),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := tree.MustBeDInt(args[0])
				switch {
				case x == math.MinInt64:
					return nil, errAbsOfMinInt64
				case x < 0:
					return tree.NewDInt(-x), nil
				}
				return args[0], nil
			},
			Info: "Calculates the absolute value of `val`.",
		},
	),

	"acos": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val`."),
	),

	"asin": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Asin(x))), nil
		}, "Calculates the inverse sine of `val`."),
	),

	"atan2": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y`."),
	),

	"atan": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val`."),
	),

	"cbrt": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cbrt(x))), nil
		}, "Calculates the cube root (∛) of `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Cbrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the cube root (∛) of `val`."),
	),

	"ceil":    ceilImpl,
	"ceiling": ceilImpl,

	"cos": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cos(x))), nil
		}, "Calculates the cosine of `val`."),
	),

	"cot": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(1 / math.Tan(x))), nil
		}, "Calculates the cotangent of `val`."),
	),

	"degrees": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(180.0 * x / math.Pi)), nil
		}, "Converts `val` as a radian value to a degree value."),
	),

	"div": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x / y))), nil
		}, "Calculates the integer quotient of `x`/`y`."),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrDivByZero
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.QuoInteger(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates the integer quotient of `x`/`y`."),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "x", Typ: types.Int}, {Name: "y", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				y := tree.MustBeDInt(args[1])
				if y == 0 {
					return nil, tree.ErrDivByZero
				}
				x := tree.MustBeDInt(args[0])
				return tree.NewDInt(x / y), nil
			},
			Info: "Calculates the integer quotient of `x`/`y`.",
		},
	),

	"exp": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Exp(x))), nil
		}, "Calculates *e* ^ `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Exp(&dd.Decimal, x)
			return dd, err
		}, "Calculates *e* ^ `val`."),
		intOverload1(func(x int64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Exp(float64(x)))), nil
		}, "Calculates *e* ^ `val`."),
	),

	"floor": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Floor(x))), nil
		}, "Calculates the largest integer not greater than `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.ExactCtx.Floor(&dd.Decimal, x)
			return dd, err
		}, "Calculates the largest integer not greater than `val`."),
		intOverload(func(x int64) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(int64(math.Floor(float64(x))))), nil
		}, "Calculates the largest integer not greater than `val`."),
	),

	"isnan": makeBuiltin(defProps(),
		tree.Overload{
			// Can't use floatBuiltin1 here because this one returns
			// a boolean.
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDBool(tree.DBool(math.IsNaN(float64(*args[0].(*tree.DFloat))))), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				isNaN := args[0].(*tree.DDecimal).Decimal.Form == apd.NaN
				return tree.MakeDBool(tree.DBool(isNaN)), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
	),

	"ln": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log(x))), nil
		}, "Calculates the natural log of `val`."),
		decimalLogFn(tree.DecimalCtx.Ln, "Calculates the natural log of `val`."),
	),

	"log": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log10(x))), nil
		}, "Calculates the base 10 log of `val`."),
		decimalLogFn(tree.DecimalCtx.Log10, "Calculates the base 10 log of `val`."),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "a", Typ: types.Any}, {Name: "b", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var a, b float64
				switch args[0].(type) {
				case *tree.DFloat:
					a = float64(*args[0].(*tree.DFloat))
					break
				case *tree.DInt:
					a = float64(*args[0].(*tree.DInt))
					break
				case *tree.DDecimal:
					a, _ = args[0].(*tree.DDecimal).Float64()
					break
				default:
					return nil, errors.New("Type Of First Param Not Support (int or float or decimal)")
				}

				switch args[1].(type) {
				case *tree.DFloat:
					b = float64(*args[1].(*tree.DFloat))
					break
				case *tree.DInt:
					b = float64(*args[1].(*tree.DInt))
					break
				case *tree.DDecimal:
					b, _ = args[1].(*tree.DDecimal).Float64()
					break
				default:
					return nil, errors.New("Type Of Second Param Not Support (int or float or decimal)")
				}
				if a == float64(0) || b == float64(0) {
					return nil, errors.New("cannot take logarithm of zero")
				}
				if a == float64(1) {
					return nil, errors.New("division by zero")
				}
				if a < float64(0) || b < float64(0) {
					return nil, errors.New("cannot take logarithm of a negative number")
				}
				return tree.NewDFloat(tree.DFloat((math.Log(b) / math.Log(a)))), nil
			},
			Info: "以b为底的对数",
		},
	),

	"mod": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Mod(x, y))), nil
		}, "Calculates `x`%`y`."),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrZeroModulus
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.Rem(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates `x`%`y`."),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "x", Typ: types.Int}, {Name: "y", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				y := tree.MustBeDInt(args[1])
				if y == 0 {
					return nil, tree.ErrZeroModulus
				}
				x := tree.MustBeDInt(args[0])
				return tree.NewDInt(x % y), nil
			},
			Info: "Calculates `x`%`y`.",
		},
	),

	"pi": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(math.Pi), nil
			},
			Info: "Returns the value for pi (3.141592653589793).",
		},
	),

	"pow":   powImpls,
	"power": powImpls,

	"radians": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(x * math.Pi / 180.0)), nil
		}, "Converts `val` as a degree value to a radians value."),
	),

	"round": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.RoundToEven(x))), nil
		}, "Rounds `val` to the nearest integer using half to even (banker's) rounding."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return roundDecimal(x, 0)
		}, "Rounds `val` to the nearest integer, half away from zero: "+
			"round(+/-2.4) = +/-2, round(+/-2.5) = +/-3."),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Float}, {Name: "decimal_accuracy", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				f := float64(*args[0].(*tree.DFloat))
				if math.IsInf(f, 0) || math.IsNaN(f) {
					return args[0], nil
				}
				var x apd.Decimal
				if _, err := x.SetFloat64(f); err != nil {
					return nil, err
				}

				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(tree.MustBeDInt(args[1]))

				var d apd.Decimal
				if _, err := tree.RoundCtx.Quantize(&d, &x, -scale); err != nil {
					return nil, err
				}

				f, err := d.Float64()
				if err != nil {
					return nil, err
				}

				return tree.NewDFloat(tree.DFloat(f)), nil
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				" in `input` using half to even (banker's) rounding.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Decimal}, {Name: "decimal_accuracy", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(tree.MustBeDInt(args[1]))
				return roundDecimal(&args[0].(*tree.DDecimal).Decimal, scale)
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				"in `input` using half away from zero rounding. If `decimal_accuracy` " +
				"is not in the range -2^31...(2^31-1), the results are undefined.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dateStr := args[0].String()
				res := strings.Trim(dateStr, "'")
				result, err := tree.PerformCast(ctx, tree.NewDString(res), coltypes.Date)
				if err != nil {
					return nil, fmt.Errorf("Can't convert to a Date Type")
				}
				return result, nil
			},
			Info: "Returns date rounded to the nearest day",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamp", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dateStr := args[0].String()
				timeStStr := strings.Trim(dateStr, "'")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:10])
				hour, _ := strconv.Atoi(timeStStr[11:13])
				if hour > 11 {
					result, err := tree.MakeDate(year, month, day)
					fromTSTZ := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), result)
					t := fromTSTZ.Time.AddDate(0, 0, 1)
					if err != nil {
						return nil, fmt.Errorf("field value %s cannot convert to a date", args[0])
					}
					return tree.NewDDateFromTime(t, ctx.GetLocation()), nil
				}
				result, err := tree.PerformCast(ctx, tree.NewDString(timeStStr), coltypes.Date)
				if err != nil {
					return nil, fmt.Errorf("field value %s cannot convert to a date", args[0])
				}
				return result, nil
			},
			Info: "Returns date rounded to the nearest day",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamptz", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dateStr := args[0].String()
				timeStStr := strings.Trim(dateStr, "'")
				year, _ := strconv.Atoi(timeStStr[:4])
				month, _ := strconv.Atoi(timeStStr[5:7])
				day, _ := strconv.Atoi(timeStStr[8:10])
				hour, _ := strconv.Atoi(timeStStr[11:13])
				if hour > 11 {
					result, err := tree.MakeDate(year, month, day)
					fromTSTZ := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), result)
					t := fromTSTZ.Time.AddDate(0, 0, 1)
					if err != nil {
						return nil, fmt.Errorf("field value %s cannot convert to a date", args[0])
					}
					return tree.NewDDateFromTime(t, ctx.GetLocation()), nil
				}
				result, err := tree.PerformCast(ctx, tree.NewDString(timeStStr), coltypes.Date)
				if err != nil {
					return nil, fmt.Errorf("field value %s cannot convert to a date", args[0])
				}
				return result, nil
			},
			Info: "Returns date rounded to the nearest day",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Date}, {Name: "fmt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := tree.MakeDTimestampTZFromDate(time.Local, args[0].(*tree.DDate))
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					if month >= 7 {
						year++
					}
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					if day >= 16 {
						month++
					}
					day = 1
				case "day", "dy", "d":
					if week >= 4 {
						day += 7 - week
					} else {
						day -= week
					}
				case "j":
					break
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Returns date rounded to the unit specified by the format model fmt",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Timestamp}, {Name: "fmt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := args[0].(*tree.DTimestamp)
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					if month >= 7 {
						year++
					}
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					if day >= 16 {
						month++
					}
					day = 1
				case "day", "dy", "d":
					if week >= 4 {
						day += 7 - week
					} else {
						day -= week
					}
				case "j":
					break
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Returns date rounded to the unit specified by the format model fmt",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.TimestampTZ}, {Name: "fmt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := args[0].(*tree.DTimestampTZ)
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					if month >= 7 {
						year++
					}
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					if day >= 16 {
						month++
					}
					day = 1
				case "day", "dy", "d":
					if week >= 4 {
						day += 7 - week
					} else {
						day -= week
					}
				case "j":
					break
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Returns date rounded to the unit specified by the format model fmt",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.String}, {Name: "fmt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt1 := strings.ToLower(string(tree.MustBeDString(args[1])))
				DatumeTime, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					return nil, fmt.Errorf("invalid number")
				}
				fromTZ := DatumeTime.(*tree.DTimestamp)
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt1 {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					if month >= 7 {
						year++
					}
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					if day >= 16 {
						month++
					}
					day = 1
				case "day", "dy", "d":
					if week >= 4 {
						day += 7 - week
					} else {
						day -= week
					}
				case "j":
					break
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt1)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Returns date rounded to the unit specified by the format model fmt",
		},
	),

	"sin": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sin(x))), nil
		}, "Calculates the sine of `val`."),
	),

	"sign": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			switch {
			case x < 0:
				return tree.NewDFloat(-1), nil
			case x == 0:
				return tree.NewDFloat(0), nil
			}
			return tree.NewDFloat(1), nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			d := &tree.DDecimal{}
			d.Decimal.SetFinite(int64(x.Sign()), 0)
			return d, nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := tree.MustBeDInt(args[0])
				switch {
				case x < 0:
					return tree.NewDInt(-1), nil
				case x == 0:
					return tree.DZero, nil
				}
				return tree.NewDInt(1), nil
			},
			Info: "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** " +
				"for negative.",
		},
	),

	"sqrt": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			// TODO(mjibson): see #13642
			if x < 0 {
				//return nil, errSqrtOfNegNumber
				return tree.DNull, nil
			}
			return tree.NewDFloat(tree.DFloat(math.Sqrt(x))), nil
		}, "Calculates the square root of `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			if x.Sign() < 0 {
				//return nil, errSqrtOfNegNumber
				return tree.DNull, nil
			}
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Sqrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the square root of `val`."),
		intOverload1(func(x int64) (tree.Datum, error) {
			if x < 0 {
				//return nil, errSqrtOfNegNumber
				return tree.DNull, nil
			}
			return tree.NewDFloat(tree.DFloat(math.Sqrt(float64(x)))), nil
		}, "Calculates the square root of `val`."),
	),

	// https://www.postgresql.org/docs/9.6/functions-datetime.html
	"timezone": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "timezone", Typ: types.String},
				{Name: "timestamptz_string", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tzArg := string(tree.MustBeDString(args[0]))
				tsArg := string(tree.MustBeDString(args[1]))
				ts, err := tree.ParseDTimestampTZ(ctx, tsArg, time.Microsecond)
				if err != nil {
					return nil, err
				}
				loc, err := timeutil.TimeZoneStringToLocation(
					tzArg,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				return ts.EvalAtTimeZone(ctx, loc), nil
			},
			Info: "Convert given time stamp with time zone to the new time zone, with no time zone designation.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "timezone", Typ: types.String},
				{Name: "timestamp", Typ: types.Timestamp},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tzStr := string(tree.MustBeDString(args[0]))
				ts := tree.MustBeDTimestamp(args[1])
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				_, beforeOffsetSecs := ts.Time.Zone()
				_, afterOffsetSecs := ts.Time.In(loc).Zone()
				durationDelta := time.Duration(beforeOffsetSecs-afterOffsetSecs) * time.Second
				return tree.MakeDTimestampTZ(ts.Time.Add(durationDelta), time.Microsecond), nil
			},
			Info: "Treat given time stamp without time zone as located in the specified time zone.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "timezone", Typ: types.String},
				{Name: "timestamptz", Typ: types.TimestampTZ},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tzStr := string(tree.MustBeDString(args[0]))
				ts := tree.MustBeDTimestampTZ(args[1])
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				return ts.EvalAtTimeZone(ctx, loc), nil
			},
			Info: "Convert given time stamp with time zone to the new time zone, with no time zone designation.",
		},
		// older version
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "timestamp", Typ: types.Timestamp},
				{Name: "timezone", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDTimestamp(args[0])
				tzStr := string(tree.MustBeDString(args[1]))
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				_, before := ts.Time.Zone()
				_, after := ts.Time.In(loc).Zone()
				return tree.MakeDTimestampTZ(ts.Time.Add(time.Duration(before-after)*time.Second), time.Microsecond), nil
			},
			Info: "Treat given time stamp without time zone as located in the specified time zone",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "timestamptz", Typ: types.TimestampTZ},
				{Name: "timezone", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDTimestampTZ(args[0])
				tzStr := string(tree.MustBeDString(args[1]))
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				return ts.EvalAtTimeZone(ctx, loc), nil
			},
			Info: "Convert given time stamp with time zone to the new time zone, with no time zone designation",
		},
	),

	"tan": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tan(x))), nil
		}, "Calculates the tangent of `val`."),
	),

	"to_date": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "fromTime", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dateString := string(tree.MustBeDString(args[0]))
				format := strings.ToLower(string(tree.MustBeDString(args[1])))
				res, err := tree.StrToDate(dateString, format, false)
				if err != nil {
					return nil, err
				}
				result, err := tree.PerformCast(Ctx, tree.NewDString(res), coltypes.Date)
				if err != nil {
					return nil, fmt.Errorf("Can't convert to a Date Type")
				}
				return result, nil
			},
			Info: "Converts the string to a fixed format date",
		},
	),

	"numtoyminterval": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Decimal}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				floatNum, _ := args[0].(*tree.DDecimal).Float64()
				format := strings.ToLower(string(tree.MustBeDString(args[1])))
				strNum := args[0].(*tree.DDecimal).String()
				if strings.Contains(strNum, ".") {
					datenum := strings.Split(strNum, ".")
					_, err := strconv.ParseInt(datenum[0], 10, 32)
					if err != nil {
						return nil, fmt.Errorf("interval out of range")
					}
				} else {
					_, err := strconv.ParseInt(strNum, 10, 32)
					if err != nil {
						return nil, fmt.Errorf("interval out of range")
					}
				}
				switch format {
				case "year", "years":
					intNum := math.Floor(floatNum*12-0.5) + 1
					if floatNum < 0.125 && floatNum > (0.01/12) {
						intNum = 1
					}
					return &tree.DInterval{Duration: duration.MakeDuration(0, 0, int64(intNum))}, nil
				case "month", "months":
					intNum := math.Floor(floatNum-0.5) + 1
					if floatNum < 1.0 && floatNum >= 0.01 {
						intNum = 1
					}
					return &tree.DInterval{Duration: duration.MakeDuration(0, 0, int64(intNum))}, nil
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", format)
				}
			},
			Info: "Converts the number to a fixed format interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dateInt := int(tree.MustBeDInt(args[0]))
				format := strings.ToLower(string(tree.MustBeDString(args[1])))
				s := strconv.Itoa(dateInt)
				dateInt64, err := strconv.ParseInt(s, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("interval out of range")
				}
				switch format {
				case "year", "years":
					m := dateInt64*12 + 0
					return &tree.DInterval{Duration: duration.MakeDuration(0, 0, m)}, nil
				case "month", "months":
					m := 0*12 + dateInt64
					return &tree.DInterval{Duration: duration.MakeDuration(0, 0, m)}, nil
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", format)
				}
			},
			Info: "Converts the number to a fixed format interval",
		},
	),

	"numtodsinterval": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Decimal}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				strNum := args[0].(*tree.DDecimal).String()
				valNum, err := args[0].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				var datepre int64
				var datesuf float64
				var err1 error
				if strings.Contains(strNum, ".") {
					datenum := strings.Split(strNum, ".")
					datepre, err1 = strconv.ParseInt(datenum[0], 10, 32)
					if err1 != nil {
						return nil, fmt.Errorf("interval out of range")
					}
					datenum[1] = "0." + datenum[1]
					if valNum < 0 {
						datenum[1] = "-" + datenum[1]
					}
					datesuf, _ = strconv.ParseFloat(datenum[1], 64)
				} else {
					datepre, err1 = strconv.ParseInt(strNum, 10, 32)
					_, err1 = strconv.ParseInt(strNum, 10, 32)
					if err1 != nil {
						return nil, fmt.Errorf("interval out of range")
					}
					datepre = int64(valNum)
				}
				format := strings.ToLower(string(tree.MustBeDString(args[1])))
				switch format {
				case "day", "days":
					return &tree.DInterval{Duration: duration.MakeDuration(int64(datesuf*1000000000*3600*24), datepre, 0)}, nil
				case "hour", "hours":
					return &tree.DInterval{Duration: duration.MakeDuration(int64(valNum*1000000000*3600), 0, 0)}, nil
				case "minute", "minutes":
					return &tree.DInterval{Duration: duration.MakeDuration(int64(valNum*1000000000*60), 0, 0)}, nil
				case "second", "seconds":
					return &tree.DInterval{Duration: duration.MakeDuration(int64(valNum*1000000000), 0, 0)}, nil
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", format)
				}

			},
			Info: "Converts the number to a fixed format interval",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dateInt := int(tree.MustBeDInt(args[0]))
				s := strconv.Itoa(dateInt)
				format := strings.ToLower(string(tree.MustBeDString(args[1])))
				dateInt64, err := strconv.ParseInt(s, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("interval out of range")
				}
				switch format {
				case "day", "days":
					return &tree.DInterval{Duration: duration.MakeDuration(0, dateInt64, 0)}, nil
				case "hour", "hours":
					second := dateInt64 * 3600
					if dateInt64 > 24 {
						me := second % (24 * 60 * 60)
						day := (second - me) / (24 * 60 * 60)
						return &tree.DInterval{Duration: duration.MakeDuration(me*1000000000, day, 0)}, nil
					}
					return &tree.DInterval{Duration: duration.MakeDuration(second*1000000000, 0, 0)}, nil
				case "minute", "minutes":
					second := dateInt64 * 60
					if dateInt64 > 24*60-1 {
						me := second % (24 * 60 * 60)
						day := (dateInt64 - me/60) / (24 * 60)
						return &tree.DInterval{Duration: duration.MakeDuration(me*1000000000, day, 0)}, nil
					}
					return &tree.DInterval{Duration: duration.MakeDuration(second*1000000000, 0, 0)}, nil
				case "second", "seconds":
					if dateInt > 24*60*60-1 {
						second := dateInt64 % (24 * 60 * 60)
						day := (dateInt64 - second) / (24 * 60 * 60)
						return &tree.DInterval{Duration: duration.MakeDuration(int64(second)*1000000000, day, 0)}, nil
					}
					return &tree.DInterval{Duration: duration.MakeDuration(dateInt64*1000000000, 0, 0)}, nil
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", format)
				}
			},
			Info: "Converts the number to a fixed format interval",
		},
	),

	"to_timestamp": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "chr", Typ: types.String}, {Name: "fmt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dateString := string(tree.MustBeDString(args[0]))
				fmtString := strings.ToLower(string(tree.MustBeDString(args[1])))
				res, err := tree.StrToDate(dateString, fmtString, true)
				if err != nil {
					if err.Error() == "time field value out of range" {
						return nil, fmt.Errorf("time field value out of range: %s", args[0])
					}
					return nil, err
				}
				Dr := tree.NewDString(res)
				r, err := tree.PerformCast(Ctx, Dr, coltypes.Timestamp)
				if err != nil {
					return nil, fmt.Errorf("Can't convert to a Timestamp Type")
				}
				return r, nil
			},
			Info: "Converts the string to a fixed format timestamp",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "unixtime", Typ: types.Decimal}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				unixTime, err := args[0].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				sec, dec := math.Modf(unixTime)
				t := timeutil.Unix(int64(sec), int64(dec*(1e9)))
				return tree.MakeDTimestampTZ(t, time.Microsecond), nil
			},
			Info: "把 Unix 时间（从 1970-01-01 00:00:00+00 开始的秒）转换成 timestamp with time zone",
		},
	),

	"from_tz": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "timestamp", Typ: types.Timestamp}, {Name: "timezone", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(Ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timestamp := tree.MustBeDTimestamp(args[0])
				fromTimeStr, err := tree.TimestampToFormatStr(timestamp)
				if err != nil {
					return nil, err
				}
				timezone := tree.MustBeDString(args[1])
				timezoneStr := strings.Split(timezone.String(), "'")[1]
				var result string
				if strings.ToLower(timezoneStr) == "utc" {
					result = fromTimeStr + "-UTC"
				} else if matched, _ := regexp.MatchString("^\\+?([1-9]|1[01234])$", timezoneStr); matched {
					if timezoneStr[0] == '+' {
						result = fromTimeStr + timezoneStr + ":00"
					} else {
						result = fromTimeStr + "+" + timezoneStr + ":00"
					}
				} else if matched, _ := regexp.MatchString("^\\+?([1-9]|1[01234]):[0-5][0-9]$", timezoneStr); matched {
					if timezoneStr[0] == '+' {
						result = fromTimeStr + timezoneStr
					} else {
						result = fromTimeStr + "+" + timezoneStr
					}
				} else if matched, _ := regexp.MatchString("^-([1-9]|1[012])$", timezoneStr); matched {
					result = fromTimeStr + timezoneStr + ":00"
				} else if matched, _ := regexp.MatchString("^-([1-9]|1[012]):[0-5][0-9]$", timezoneStr); matched {
					result = fromTimeStr + timezoneStr
				} else {
					return nil, errors.New("timezone region not supported")
				}
				Dr := tree.NewDString(result)
				r, _ := tree.PerformCast(Ctx, Dr, coltypes.TimestampTZ)
				return r, nil
			},
			Info: "Convert Timestamp type data to TimestampTZ type",
		},
	),

	"months_between": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "firstDate", Typ: types.Date}, {Name: "secondDate", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				firstDate := fmt.Sprintf("%s", args[0])
				secondDate := fmt.Sprintf("%s", args[1])
				ret, err := tree.SubOfDate(firstDate, secondDate)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.MustBeDFloat(ret)), nil
			},
			Info: "Returns the number of months between two dates",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "firstDate", Typ: types.String}, {Name: "secondDate", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				firstDate := string(tree.MustBeDString(args[0]))
				secondDate := string(tree.MustBeDString(args[1]))
				ret, err := tree.SubOfDate(firstDate, secondDate)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.MustBeDFloat(ret)), nil
			},
			Info: "Returns the number of months between two dates",
		},
	),

	"next_day": makeBuiltin(tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "inputDate", Typ: types.Date}, {Name: "nextWeekday", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				days := args[0].(*tree.DDate)
				inputDate := fmt.Sprintf("%s", (args[0]))
				nextweekday := string(tree.MustBeDString(args[1]))
				ret, err := tree.NextDate(inputDate, nextweekday, days)
				if err != nil {
					return nil, err
				}
				return tree.NewDDate(ret), nil
			},
			Info: "Returns the number of months between two dates",
		},
	),
	"sinh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sinh(x))), nil
		}, "Calculates the sine of `val`."),
	),

	"sind": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			sign := 1.0
			if !tree.CheckInf(x) {
				return tree.DNull, errors.New("input is out of range")
			}
			arg1 := math.Mod(x, 360.0)
			if arg1 < 0.0 {
				/* sind(-x) = -sind(x) */
				arg1 = -arg1
				sign = -sign
			}
			if arg1 > 180.0 {
				/* sind(360-x) = -sind(x) */
				arg1 = 360.0 - arg1
				sign = -sign
			}
			if arg1 > 90.0 {
				/* sind(180-x) = sind(x) */
				arg1 = 180.0 - arg1
			}
			result := sign * tree.Sind(arg1)
			return tree.NewDFloat(tree.DFloat(result)), nil
		}, "Calculates the sine of `val`."),
	),

	"cosh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cosh(x))), nil
		}, "Calculates the sine of `val`."),
	),

	"cosd": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			if !tree.CheckInf(x) {
				return tree.DNull, errors.New("input is out of range")
			}
			arg1 := math.Mod(x, 360.0)
			sign := 1.0
			if arg1 < 0.0 {
				/* cosd(-x) = cosd(x) */
				arg1 = -arg1
			}
			if arg1 > 180.0 {
				/* cosd(360-x) = cosd(x) */
				arg1 = 360.0 - arg1
			}

			if arg1 > 90.0 {
				/* cosd(180-x) = -cosd(x) */
				arg1 = 180.0 - arg1
				sign = -sign
			}
			result := sign * tree.Cosd(x)
			return tree.NewDFloat(tree.DFloat(result)), nil
		}, "Calculates the sine of `val`."),
	),

	"tanh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tanh(x))), nil
		}, "Calculates the sine of `val`."),
	),

	"tand": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			if !tree.CheckInf(x) {
				return tree.DNull, errors.New("input is out of range")
			}
			arg1 := math.Mod(x, 360.0)
			sign := 1.0
			if arg1 < 0.0 {
				/* tand(-x) = -tand(x) */
				arg1 = -arg1
				sign = -sign
			}
			if arg1 > 180.0 {
				/* tand(360-x) = -tand(x) */
				arg1 = 360.0 - arg1
				sign = -sign
			}
			if arg1 > 90.0 {
				/* tand(180-x) = -tand(x) */
				arg1 = 180.0 - arg1
				sign = -sign
			}
			tanArg1 := tree.Sind(arg1) / tree.Cosd(arg1)
			result := sign * tanArg1
			return tree.NewDFloat(tree.DFloat(result)), nil
		}, "Calculates the sine of `val`."),
	),

	"asinh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Asinh(x))), nil
		}, "Calculates the sine of `val`."),
	),

	"asind": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			var res float64
			if x < -1 || x > 1 {
				return tree.DNull, errors.New("input out of range")
			}
			if x >= 0.0 {
				res = tree.Asind(x)
			} else {
				res = -tree.Asind(-x)
			}
			return tree.NewDFloat(tree.DFloat(res)), nil
		}, "Calculates the sine of `val`."),
	),

	"acosh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			if x < 1.0 {
				return tree.DNull, errors.New("input out of range")
			}
			return tree.NewDFloat(tree.DFloat(math.Acosh(x))), nil
		}, "Calculates the sine of `val`."),
	),

	"acosd": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			var res float64
			if x < -1 || x > 1 {
				return tree.DNull, errors.New("input out of range")
			}
			if x >= 0.0 {
				res = tree.Acosd(x)
			} else {
				res = 90.0 + tree.Asind(-x)
			}

			return tree.NewDFloat(tree.DFloat(res)), nil
		}, "Calculates the sine of `val`."),
	),

	"atanh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			if x < -1 || x > 1 {
				return tree.DNull, errors.New("input out of range")
			}
			return tree.NewDFloat(tree.DFloat(math.Atanh(x))), nil
		}, "Calculates the tan of `val`."),
	),

	"atand": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(tree.Atand(x))), nil
		}, "Calculates the tan of `val`."),
	),

	"atan2d": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(tree.Atan2d(x, y))), nil
		}, "Calculates the sine of `val`."),
	),

	"scale": makeBuiltin(defProps(),
		decimalIntOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(-x.Exponent)), nil
		}, "Calculates the sine of `val`."),
	),

	"log10": makeBuiltin(defProps(),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			res := &tree.DDecimal{}
			argFloat, err := tree.MathType(x) //参数类型转换
			if err != nil {
				return nil, err
			}
			if argFloat <= 0 {
				return tree.DNull, errors.New("Cannot take the logarithm of 0 or a negative number")
			}
			_, err1 := res.SetFloat64(math.Log10(argFloat))
			if err1 != nil {
				return nil, err1
			}
			return res, nil
		}, "Calculates the sine of `val`."),
		floatOverload3(func(x float64) (tree.Datum, error) {
			res := &tree.DDecimal{}
			//argFloat, err := tree.MathType(x) //参数类型转换
			//if err != nil {
			//	return nil, err
			//}
			if x <= 0 {
				return tree.DNull, errors.New("Cannot take the logarithm of 0 or a negative number")
			}
			_, err1 := res.SetFloat64(math.Log10(x))
			if err1 != nil {
				return nil, err1
			}
			return res, nil
		}, "Calculates the sine of `val`."),
		intOverload2(func(x int64) (tree.Datum, error) {
			res := &tree.DDecimal{}

			numStr := strconv.Itoa(int(x))
			num, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return nil, err
			}
			if x <= 0 {
				return tree.DNull, errors.New("Cannot take the logarithm of 0 or a negative number")
			}
			_, err1 := res.SetFloat64(math.Log10(num))
			if err1 != nil {
				return nil, err1
			}
			return res, nil
		}, "Calculates the sine of `val`."),
	),

	"trunc": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x))), nil
		}, "Truncates the decimal values of `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			x.Modf(&dd.Decimal, nil)
			return dd, nil
		}, "Truncates the decimal values of `val`."),
		decimalOverload3(func(data *apd.Decimal, p int32) (tree.Datum, error) {
			var daVal apd.Decimal
			var da *apd.Decimal
			var sVal []big.Word
			//Make a copy of the source data
			//to prevent the original value from being changed during table query
			da = &daVal
			tempS := data.Coeff.Bits()
			val := tempS[0]
			sVal = append(sVal, val)
			da.Coeff.SetBits(sVal)
			da.Exponent = data.Exponent
			s := da.Coeff.Bits() //获取参数绝对值切片
			num := s[0:1]        //参数绝对值切片
			dd := &tree.DDecimal{}
			getPow := func(sur int32) (float64, error) { //获取10的n次方函数
				pow := math.Pow10(int(sur))
				return pow, nil
			}
			outRes := func(exp int32) (*tree.DDecimal, error) { //输出赋值函数
				da.Coeff.SetBits(num) //改变绝对值大小
				da.Exponent = exp     //调整精度
				da.Negative = data.Negative
				resErr := dd.SetString(da.String()) //转换为DDcimal类型
				if resErr != nil {
					return nil, resErr
				}
				return dd, nil
			}
			if da.Exponent < 0 { //如果参数精度不为零
				expAbs := -da.Exponent
				if p >= expAbs { //如果需要保留的精度大于数字精度
					pow, err := getPow(p - expAbs) //获取10的n次方值，并转换类型
					if err != nil {
						return nil, err
					}
					d := float64(num[0]) * float64(pow)
					num[0] = big.Word(d)
					resDci, err2 := outRes(-p)
					if err2 != nil {
						return nil, err2
					}
					return resDci, nil
				}
				if 0 < p && p < expAbs { //如果截断精度小于本身精度但大于0
					pow, err := getPow(expAbs - p) //获取10的n次方值，并转换类型
					if err != nil {
						return nil, err
					}
					num[0] = num[0] / big.Word(pow)
					resDci, err2 := outRes(-p)
					if err2 != nil {
						return nil, err2
					}
					data.Coeff.SetBits(tempS[0:1])
					return resDci, nil
				}
				if p <= 0 { //如果需要的精度为负数
					pow, err := getPow(expAbs) //获取10的n次方值，并转换类型，计算10的expAbs次方
					if err != nil {
						return nil, err
					}
					num[0] = num[0] / big.Word(pow) //将数值除去本身精度
					pow2, err2 := getPow(-p)        //获取10的-p次方值，并转换类型
					if err2 != nil {
						return nil, err2
					}
					num[0] = (num[0] / big.Word(pow2)) * big.Word(pow2)
					resDci, err3 := outRes(0)
					if err3 != nil {
						return nil, err3
					}
					data.Coeff.SetBits(tempS[0:1])
					return resDci, nil
				}
			}
			if da.Exponent == 0 {
				if p > 0 { //如果需求精度大于本身精度
					pow, err := getPow(p) //获取10的n次方值，并转换类型
					if err != nil {
						return nil, err
					}
					num[0] = num[0] * big.Word(pow)
					resDci, err2 := outRes(-p)
					if err2 != nil {
						return nil, err2
					}
					data.Coeff.SetBits(tempS[0:1])
					return resDci, nil
				}
				if p <= 0 { //如果需求精度小于本身
					pow, err := getPow(-p) //获取10的n次方值，并转换类型
					if err != nil {
						return nil, err
					}
					num[0] = (num[0] / big.Word(pow)) * big.Word(pow)
					da.Coeff.SetBits(num)
					resErr := dd.SetString(da.String())
					if resErr != nil {
						return nil, err
					}
					data.Coeff.SetBits(tempS[0:1])
					return dd, nil
				}
			}
			return tree.NewDString("null"), nil
		}, "Truncates the decimal values of `val`."),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Date}, {Name: "element", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := tree.MakeDTimestampTZFromDate(time.Local, args[0].(*tree.DDate))
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					day = 1
				case "day", "dy", "d":
					if week != 0 {
						day -= week
					}
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "DD", "j", "DDD":
					break
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				case "hh", "hh24", "hh12", "mi":
					break
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Timestamp}, {Name: "element", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := args[0].(*tree.DTimestamp)
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					day = 1
				case "day", "dy", "d":
					if week != 0 {
						day -= week
					}
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "DD", "j", "DDD":
					break
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				case "hh", "hh24", "hh12", "mi":
					break
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.String}, {Name: "element", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				format := strings.ToLower(string(tree.MustBeDString(args[1])))
				DatumeTime, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					return nil, fmt.Errorf("invalid number")
				}
				fromTZ := DatumeTime.(*tree.DTimestamp)
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch format {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					day = 1
				case "day", "dy", "d":
					if week != 0 {
						day -= week
					}
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "DD", "j", "DDD":
					break
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				case "hh", "hh24", "hh12", "mi":
					break
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", format)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.TimestampTZ}, {Name: "element", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fmt := strings.ToLower(string(tree.MustBeDString(args[1])))
				fromTZ := args[0].(*tree.DTimestampTZ)
				year := fromTZ.Year()
				month := int(fromTZ.Month())
				day := fromTZ.Day()
				week := int(fromTZ.Weekday())
				switch fmt {
				case "year", "yyyy", "yyy", "yy", "y", "syear":
					month = 1
					day = 1
				case "month", "mon", "mm", "rm":
					day = 1
				case "day", "dy", "d":
					if week != 0 {
						day -= week
					}
				case "q":
					month = ((month-1)/3)*3 + 1
					day = 1
				case "DD", "j", "DDD":
					break
				case "cc", "scc":
					year = year / 100 * 100
					month = 1
					day = 1
				case "hh", "hh24", "hh12", "mi":
					break
				default:
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported fmt: %s", fmt)
				}
				toTime := time.Date(year, time.Month(month), day, 0, 0, 0, 0, fromTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower("day")
				fromTS := args[0].(*tree.DTimestamp)
				tsTZ, err := truncateTimestamp(ctx, fromTS.Time, timeSpan)
				if err != nil {
					return nil, err
				}
				toTime := time.Date(tsTZ.Year(), tsTZ.Month(), tsTZ.Day(), 0, 0, 0, 0, tsTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower("day")
				fromTS := tree.MakeDTimestampTZFromDate(time.Local, args[0].(*tree.DDate))
				tsTZ, err := truncateTimestamp(ctx, fromTS.Time, timeSpan)
				if err != nil {
					return nil, err
				}
				toTime := time.Date(tsTZ.Year(), tsTZ.Month(), tsTZ.Day(), 0, 0, 0, 0, tsTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower("day")
				fromTS := args[0].(*tree.DTimestampTZ)
				tsTZ, err := truncateTimestamp(ctx, fromTS.Time, timeSpan)
				if err != nil {
					return nil, err
				}
				toTime := time.Date(tsTZ.Year(), tsTZ.Month(), tsTZ.Day(), 0, 0, 0, 0, tsTZ.Location())
				return tree.NewDDateFromTime(toTime, toTime.Location()), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
	),

	// Array functions.
	"string_to_array": makeBuiltin(arrayPropsNullableArgs(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.String}, {Name: "delimiter", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.String}),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				str := string(tree.MustBeDString(args[0]))
				delimOrNil := stringOrNil(args[1])
				return stringToArray(str, delimOrNil, nil)
			},
			Info: "Split a string into components on a delimiter.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "str", Typ: types.String}, {Name: "delimiter", Typ: types.String}, {Name: "null", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.String}),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				str := string(tree.MustBeDString(args[0]))
				delimOrNil := stringOrNil(args[1])
				nullStr := stringOrNil(args[2])
				return stringToArray(str, delimOrNil, nullStr)
			},
			Info: "Split a string into components on a delimiter with a specified string to consider NULL.",
		},
	),

	"array_to_string": makeBuiltin(arrayPropsNullableArgs(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.AnyArray}, {Name: "delim", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, nil
				}
				arr := tree.MustBeDArray(args[0])
				delim := string(tree.MustBeDString(args[1]))
				return arrayToString(arr, delim, nil)
			},
			Info: "Join an array into a string with a delimiter.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.AnyArray}, {Name: "delimiter", Typ: types.String}, {Name: "null", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, nil
				}
				arr := tree.MustBeDArray(args[0])
				delim := string(tree.MustBeDString(args[1]))
				nullStr := stringOrNil(args[2])
				return arrayToString(arr, delim, nullStr)
			},
			Info: "Join an array into a string with a delimiter, replacing NULLs with a null string.",
		},
	),

	"array_length": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.AnyArray}, {Name: "array_dimension", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the length of `input` on the provided `array_dimension`. However, " +
				"because ZNBaseDB doesn't yet support multi-dimensional arrays, the only supported" +
				" `array_dimension` is **1**.",
		},
	),

	"array_lower": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.AnyArray}, {Name: "array_dimension", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLower(arr, dimen), nil
			},
			Info: "Calculates the minimum value of `input` on the provided `array_dimension`. " +
				"However, because ZNBaseDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	),

	"array_upper": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.AnyArray}, {Name: "array_dimension", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the maximum value of `input` on the provided `array_dimension`. " +
				"However, because ZNBaseDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	),

	"array_append": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{Name: "array", Typ: types.TArray{Typ: typ}}, {Name: "elem", Typ: typ}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: typ}),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.AppendToMaybeNullArray(typ, args[0], args[1])
			},
			Info: "Appends `elem` to `array`, returning the result.",
		}
	})),

	"array_prepend": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{Name: "elem", Typ: typ}, {Name: "array", Typ: types.TArray{Typ: typ}}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: typ}),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PrependToMaybeNullArray(typ, args[0], args[1])
			},
			Info: "Prepends `elem` to `array`, returning the result.",
		}
	})),

	"array_cat": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{Name: "left", Typ: types.TArray{Typ: typ}}, {Name: "right", Typ: types.TArray{Typ: typ}}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: typ}),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.ConcatArrays(typ, args[0], args[1])
			},
			Info: "Appends two arrays.",
		}
	})),

	"array_remove": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{Name: "array", Typ: types.TArray{Typ: typ}}, {Name: "elem", Typ: typ}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: typ}),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(typ)
				for _, e := range tree.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) != 0 {
						if err := result.Append(e); err != nil {
							return nil, err
						}
					}
				}
				return result, nil
			},
			Info: "Remove from `array` all elements equal to `elem`.",
		}
	})),

	"array_replace": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{Name: "array", Typ: types.TArray{Typ: typ}}, {Name: "toreplace", Typ: typ}, {Name: "replacewith", Typ: typ}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: typ}),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(typ)
				for _, e := range tree.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) == 0 {
						if err := result.Append(args[2]); err != nil {
							return nil, err
						}
					} else {
						if err := result.Append(e); err != nil {
							return nil, err
						}
					}
				}
				return result, nil
			},
			Info: "Replace all occurrences of `toreplace` in `array` with `replacewith`.",
		}
	})),

	"array_position": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{Name: "array", Typ: types.TArray{Typ: typ}}, {Name: "elem", Typ: typ}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				for i, e := range tree.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) == 0 {
						return tree.NewDInt(tree.DInt(i + 1)), nil
					}
				}
				return tree.DNull, nil
			},
			Info: "Return the index of the first occurrence of `elem` in `array`.",
		}
	})),

	"array_positions": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{Name: "array", Typ: types.TArray{Typ: typ}}, {Name: "elem", Typ: typ}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.Int}),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(types.Int)
				for i, e := range tree.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) == 0 {
						if err := result.Append(tree.NewDInt(tree.DInt(i + 1))); err != nil {
							return nil, err
						}
					}
				}
				return result, nil
			},
			Info: "Returns and array of indexes of all occurrences of `elem` in `array`.",
		}
	})),

	// JSON functions.

	"json_to_recordset":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"jsonb_to_recordset":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"json_populate_recordset":  makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"jsonb_populate_recordset": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),

	"json_remove_path": makeBuiltin(jsonProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.JSON}, {Name: "path", Typ: types.TArray{Typ: types.String}}},
			ReturnType: tree.FixedReturnType(types.JSON),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ary := *tree.MustBeDArray(args[1])
				if err := checkHasNulls(ary); err != nil {
					return nil, err
				}
				path, _ := darrayToStringSlice(ary)
				s, _, err := tree.MustBeDJSON(args[0]).JSON.RemovePath(path)
				if err != nil {
					return nil, err
				}
				return &tree.DJSON{JSON: s}, nil
			},
			Info: "Remove the specified path from the JSON object.",
		},
	),

	"json_extract_path": makeBuiltin(jsonProps(), jsonExtractPathImpl),

	"jsonb_extract_path": makeBuiltin(jsonProps(), jsonExtractPathImpl),

	"json_set": makeBuiltin(jsonProps(), jsonSetImpl, jsonSetWithCreateMissingImpl),

	"jsonb_set": makeBuiltin(jsonProps(), jsonSetImpl, jsonSetWithCreateMissingImpl),

	"jsonb_insert": makeBuiltin(jsonProps(), jsonInsertImpl, jsonInsertWithInsertAfterImpl),

	"jsonb_pretty": makeBuiltin(jsonProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.JSON}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s, err := json.Pretty(tree.MustBeDJSON(args[0]).JSON)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(s), nil
			},
			Info: "Returns the given JSON value as a STRING indented and with newlines.",
		},
	),

	"json_typeof": makeBuiltin(jsonProps(), jsonTypeOfImpl),

	"jsonb_typeof": makeBuiltin(jsonProps(), jsonTypeOfImpl),

	"array_to_json": arrayToJSONImpls,

	"to_json": makeBuiltin(jsonProps(), toJSONImpl),

	"to_jsonb": makeBuiltin(jsonProps(), toJSONImpl),

	"json_build_array": makeBuiltin(jsonPropsNullableArgs(), jsonBuildArrayImpl),

	"jsonb_build_array": makeBuiltin(jsonPropsNullableArgs(), jsonBuildArrayImpl),

	"json_build_object": makeBuiltin(jsonPropsNullableArgs(), jsonBuildObjectImpl),

	"jsonb_build_object": makeBuiltin(jsonPropsNullableArgs(), jsonBuildObjectImpl),

	"json_object": jsonObjectImpls,

	"jsonb_object": jsonObjectImpls,

	"json_strip_nulls": makeBuiltin(jsonProps(), jsonStripNullsImpl),

	"jsonb_strip_nulls": makeBuiltin(jsonProps(), jsonStripNullsImpl),

	"json_array_length": makeBuiltin(jsonProps(), jsonArrayLengthImpl),

	"jsonb_array_length": makeBuiltin(jsonProps(), jsonArrayLengthImpl),

	// Metadata functions.

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"version": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(build.GetInfo().Short()), nil
			},
			Info: "Returns the node's version of ZNBaseDB.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"current_database": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(ctx.SessionData.Database) == 0 {
					return tree.DNull, nil
				}
				return tree.NewDString(ctx.SessionData.Database), nil
			},
			Info: "Returns the current database.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	//
	// Note that in addition to what the pg doc says ("current_schema =
	// first item in search path"), the pg server actually skips over
	// non-existent schemas in the search path to determine
	// current_schema. This is not documented but can be verified by a
	// SQL client against a pg server.
	"current_schema": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlacklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ctx := evalCtx.Ctx()
				curDb := evalCtx.SessionData.Database
				iter := evalCtx.SessionData.SearchPath.IterWithoutImplicitPGCatalog()
				for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
					if found, _, err := evalCtx.Planner.LookupSchema(ctx, curDb, scName); found || err != nil {
						if err != nil {
							return nil, err
						}
						return tree.NewDString(scName), nil
					}
				}
				return tree.DNull, nil
			},
			Info: "Returns the current schema.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	//
	// Note that in addition to what the pg doc says ("current_schemas =
	// items in search path with or without pg_catalog depending on
	// argument"), the pg server actually skips over non-existent
	// schemas in the search path to compute current_schemas. This is
	// not documented but can be verified by a SQL client against a pg
	// server.
	"current_schemas": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlacklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "include_pg_catalog", Typ: types.Bool}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.String}),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ctx := evalCtx.Ctx()
				curDb := evalCtx.SessionData.Database
				includePgCatalog := *(args[0].(*tree.DBool))
				schemas := tree.NewDArray(types.String)
				var iter sessiondata.SearchPathIter
				if includePgCatalog {
					iter = evalCtx.SessionData.SearchPath.Iter()
				} else {
					iter = evalCtx.SessionData.SearchPath.IterWithoutImplicitPGCatalog()
				}
				for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
					if found, _, err := evalCtx.Planner.LookupSchema(ctx, curDb, scName); found || err != nil {
						if err != nil {
							return nil, err
						}
						if err := schemas.Append(tree.NewDString(scName)); err != nil {
							return nil, err
						}
					}
				}
				return schemas, nil
			},
			Info: "Returns the valid schemas in the search path.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"current_user": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(ctx.SessionData.User) == 0 {
					return tree.DNull, nil
				}
				return tree.NewDString(ctx.SessionData.User), nil
			},
			Info: "Returns the current user. This function is provided for " +
				"compatibility with PostgreSQL.",
		},
	),

	"user": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(ctx.SessionData.User) == 0 {
					return tree.DNull, nil
				}
				return tree.NewDString(ctx.SessionData.User), nil
			},
			Info: "Returns the current user. This function is provided for " +
				"compatibility with Oracle.",
		},
	),

	"userenv": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "opt_parameter", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args.Len() == 0 {
					return nil, errors.New("invalid number of arguments")
				}
				parameter := strings.ToLower(string(tree.MustBeDString(args[0])))
				switch parameter {
				case "client_info":
					return tree.NewDString(""), nil
				case "entryid":
					return tree.NewDString("0"), nil
				case "isdba":
					currentUser := ctx.SessionData.User
					if currentUser == "root" {
						return tree.NewDString("true"), nil
					}
					return tree.NewDString("false"), nil
				case "lang":
					return tree.NewDString("US"), nil
				case "language":
					return tree.NewDString("AMERICAN_AMERICA.UTF8"), nil
				case "sessionid":
					return tree.DNull, nil
				case "sid":
					return tree.DNull, nil
				case "terminal":
					return tree.DNull, nil
				default:
					return nil, errors.New("Invalid parameter")
				}
			},
			Info: "Return the information about the current session. This function is" +
				"provided for compatibility with Oracle.",
		},
	),

	"zbdb_internal.node_executable_version": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				v := "unknown"
				// TODO(tschottdorf): we should always have a Settings, but there
				// are many random places that create an ad-hoc EvalContext that
				// they only partially populate.
				if st := ctx.Settings; st != nil {
					v = st.Version.ServerVersion.String()
				}
				return tree.NewDString(v), nil
			},
			Info: "Returns the version of ZNBaseDB this node is running.",
		},
	),

	"zbdb_internal.cluster_id": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.UUID),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDUuid(tree.DUuid{UUID: ctx.ClusterID}), nil
			},
			Info: "Returns the cluster ID.",
		},
	),

	"zbdb_internal.force_error": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "errorCode", Typ: types.String}, {Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				errCode := string(*args[0].(*tree.DString))
				msg := string(*args[1].(*tree.DString))
				if errCode == "" {
					return nil, errors.New(msg)
				}
				return nil, pgerror.NewError(pgcode.MakeCode(errCode), msg)
			},
			Info: "This function is used only by ZNBaseDB's developers for testing purposes.",
		},
	),

	"zbdb_internal.force_assertion_error": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				msg := string(*args[0].(*tree.DString))
				return nil, pgerror.NewAssertionErrorf("%s", msg)
			},
			Info: "This function is used only by ZNBaseDB's developers for testing purposes.",
		},
	),

	"zbdb_internal.force_panic": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				msg := string(*args[0].(*tree.DString))
				panic(msg)
			},
			Info: "This function is used only by ZNBaseDB's developers for testing purposes.",
		},
	),

	"zbdb_internal.force_log_fatal": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				msg := string(*args[0].(*tree.DString))
				log.Fatal(ctx.Ctx(), msg)
				return nil, nil
			},
			Info: "This function is used only by ZNBaseDB's developers for testing purposes.",
		},
	),

	// If force_retry is called during the specified interval from the beginning
	// of the transaction it returns a retryable error. If not, 0 is returned
	// instead of an error.
	// The second version allows one to create an error intended for a transaction
	// different than the current statement's transaction.
	"zbdb_internal.force_retry": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				minDuration := args[0].(*tree.DInterval).Duration
				elapsed := duration.MakeDuration(int64(ctx.StmtTimestamp.Sub(ctx.TxnTimestamp)), 0, 0)
				if elapsed.Compare(minDuration) < 0 {
					return nil, ctx.Txn.GenerateForcedRetryableError(
						ctx.Ctx(), "forced by zbdb_internal.force_retry()")
				}
				return tree.DZero, nil
			},
			Info: "This function is used only by ZNBaseDB's developers for testing purposes.",
		},
	),

	// Fetches the corresponding lease_holder for the request key.
	"zbdb_internal.lease_holder": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "key", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				key := []byte(tree.MustBeDBytes(args[0]))
				b := &client.Batch{}
				b.AddRawRequest(&roachpb.LeaseInfoRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: key,
					},
				})
				if err := ctx.Txn.Run(ctx.Context, b); err != nil {
					return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "message: %s", err)
				}
				resp := b.RawResponse().Responses[0].GetInner().(*roachpb.LeaseInfoResponse)

				return tree.NewDInt(tree.DInt(resp.Lease.Replica.StoreID)), nil
			},
			Info: "This function is used to fetch the leaseholder corresponding to a request key",
		},
	),

	// Return statistics about a range.
	"zbdb_internal.range_stats": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "key", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.JSON),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				key := []byte(tree.MustBeDBytes(args[0]))
				b := &client.Batch{}
				b.AddRawRequest(&roachpb.RangeStatsRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: key,
					},
				})
				if err := ctx.Txn.Run(ctx.Context, b); err != nil {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "message: %s", err)
				}
				resp := b.RawResponse().Responses[0].GetInner().(*roachpb.RangeStatsResponse).MVCCStats
				jsonStr, err := gojson.Marshal(&resp)
				if err != nil {
					return nil, err
				}
				jsonDatum, err := tree.ParseDJSON(string(jsonStr))
				if err != nil {
					return nil, err
				}
				return jsonDatum, nil
			},
			Info: "This function is used to retrieve range statistics information as a JSON object.",
		},
	),

	// Identity function which is marked as impure to avoid constant folding.
	"zbdb_internal.no_constant_folding": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "input", Typ: types.Any}},
			ReturnType: tree.IdentityReturnType(0),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info: "This function is used only by ZNBaseDB's developers for testing purposes.",
		},
	),

	// Return a pretty key for a given raw key, skipping the specified number of
	// fields.
	"zbdb_internal.pretty_key": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "raw_key", Typ: types.Bytes},
				{Name: "skip_fields", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(sqlbase.PrettyKey(
					nil, /* valDirs */
					roachpb.Key(tree.MustBeDBytes(args[0])),
					int(tree.MustBeDInt(args[1])))), nil
			},
			Info: "This function is used only by ZNBaseDB's developers for testing purposes.",
		},
	),

	"zbdb_internal.set_vmodule": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "vmodule_string", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				return tree.DZero, log.SetVModule(string(*args[0].(*tree.DString)))
			},
			Info: "Set the equivalent of the `--vmodule` flag on the gateway node processing this request; " +
				"it affords control over the logging verbosity of different files. " +
				"Example syntax: `zbdb_internal.set_vmodule('recordio=2,file=1,gfs*=3')`. " +
				"Reset with: `zbdb_internal.set_vmodule('')`. " +
				"Raising the verbosity can severely affect performance.",
		},
	),

	// Returns the number of distinct inverted index entries that would be generated for a JSON value.
	"zbdb_internal.json_num_index_entries": makeBuiltin(
		tree.FunctionProperties{
			Category:     categorySystemInfo,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "val", Typ: types.JSON}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arg := args[0]
				if arg == tree.DNull {
					return tree.NewDInt(tree.DInt(1)), nil
				}
				n, err := json.NumInvertedIndexEntries(tree.MustBeDJSON(arg).JSON)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(n)), nil
			},
			Info: "This function is used only by ZNBaseDB's developers for testing purposes.",
		},
	),

	"zbdb_internal.round_decimal_values": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "val", Typ: types.Decimal},
				{Name: "scale", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				value := args[0].(*tree.DDecimal)
				scale := int32(tree.MustBeDInt(args[1]))
				return roundDDecimal(value, scale)
			},
			Info: "This function is used internally to round decimal values during mutations.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "val", Typ: types.TArray{Typ: types.Decimal}},
				{Name: "scale", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.Decimal}),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				value := args[0].(*tree.DArray)
				scale := int32(tree.MustBeDInt(args[1]))

				// Lazily allocate a new array only if/when one of its elements
				// is rounded.
				var newArr tree.Datums
				for i, elem := range value.Array {
					// Skip NULL values.
					if elem == tree.DNull {
						continue
					}

					rounded, err := roundDDecimal(elem.(*tree.DDecimal), scale)
					if err != nil {
						return nil, err
					}
					if rounded != elem {
						if newArr == nil {
							newArr = make(tree.Datums, len(value.Array))
							copy(newArr, value.Array)
						}
						newArr[i] = rounded
					}
				}
				if newArr != nil {
					return &tree.DArray{
						ParamTyp: value.ParamTyp,
						Array:    newArr,
						HasNulls: value.HasNulls,
					}, nil
				}
				return value, nil
			},
			Info: "This function is used internally to round decimal array values during mutations.",
		},
	),

	"weekday": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {

				tarTime := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), args[0].(*tree.DDate))
				weekday := tarTime.Time.Weekday()
				if weekday == 0 {
					weekday = 7
				}
				return tree.NewDInt(tree.DInt(weekday - 1)), nil
			},
			Info: "Returns an integer value representing the corresponding day of the week in the range of 1 to 7",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tarTime := args[0].(*tree.DTimestamp)
				weekday := tarTime.Time.Weekday()
				if weekday == 0 {
					weekday = 7
				}
				return tree.NewDInt(tree.DInt(weekday - 1)), nil
			},
			Info: "Returns an integer value representing the corresponding day of the week in the range of 1 to 7",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tarTime := args[0].(*tree.DTimestampTZ)
				weekday := tarTime.Time.Weekday()
				if weekday == 0 {
					weekday = 7
				}
				return tree.NewDInt(tree.DInt(weekday - 1)), nil
			},
			Info: "Returns an integer value representing the corresponding day of the week in the range of 1 to 7",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date, err := tree.PerformCast(ctx, args[0], coltypes.Timestamp)
				if err != nil {
					return tree.DNull, nil
				}
				tarTime := date.(*tree.DTimestamp)
				weekday := tarTime.Time.Weekday()
				if weekday == 0 {
					weekday = 7
				}
				return tree.NewDInt(tree.DInt(weekday - 1)), nil
			},
			Info: "Returns an integer value representing the corresponding day of the week in the range of 1 to 7",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date, err := tree.PerformCast(ctx, args[0], coltypes.Date)
				if err != nil {
					return tree.DNull, nil
				}
				tarTime := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date.(*tree.DDate))
				weekday := tarTime.Time.Weekday()
				if weekday == 0 {
					weekday = 7
				}
				return tree.NewDInt(tree.DInt(weekday - 1)), nil
			},
			Info: "Returns an integer value representing the corresponding day of the week in the range of 1 to 7",
		},
	),

	"dayofweek": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date, err := tree.PerformCast(ctx, args[0], coltypes.Date)
				if err != nil {
					return tree.DNull, nil
				}
				tarTime := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date.(*tree.DDate))
				weekday := tarTime.Time.Weekday()
				if weekday == 0 {
					weekday = 7
				}
				return tree.NewDInt(tree.DInt(weekday)), nil
			},
			Info: "Returns an integer value representing the corresponding day of the week in the range of 1 to 7",
		},
	),

	"dayofyear": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date, err := tree.PerformCast(ctx, args[0], coltypes.Date)
				if err != nil {
					return tree.DNull, nil
				}
				tarTime := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date.(*tree.DDate))
				return tree.NewDInt(tree.DInt(tarTime.Time.YearDay())), nil
			},
			Info: "Returns an integer value representing the day of the year from 1 to 366",
		},
	),

	"monthname": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "date", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				date, err := tree.PerformCast(ctx, args[0], coltypes.Date)
				if err != nil {
					return tree.DNull, nil
				}
				tarTime := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date.(*tree.DDate))
				return tree.NewDString(tarTime.Month().String()), nil
			},
			Info: "Returns the name of the month of the year",
		},
	),

	"isnull": makeBuiltin(
		tree.FunctionProperties{
			Category:     categorySystemInfo,
			Impure:       true,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "expre", Typ: types.Any}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				exper := args[0]
				if exper == tree.DNull {
					return tree.NewDInt(1), nil
				}
				return tree.NewDInt(0), nil
			},
			Info: "If expr is NULL, it returns 1, otherwise it returns 0.",
		},
	),

	"width_bucket": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "op", Typ: types.Decimal},
				{Name: "b1", Typ: types.Decimal},
				{Name: "b2", Typ: types.Decimal},
				{Name: "count", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args.Len() == 0 {
					return nil, errors.New("invalid number of arguments")
				}
				op, err := args[0].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				b1, err := args[1].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				b2, err := args[2].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				count := int32(tree.MustBeDInt(args[3]))

				if b1 == b2 {
					return nil, errors.New("lower bound cannot equal upper bound")
				}

				var nResult int32 // 结果

				if b2 < b1 {
					if op > b1 {
						nResult = 0
					} else if op < b2 {
						nResult = count
					} else {
						nResult = count + 1
					}
				} else {
					fDistance := b2 - b1                // 范围b1到b2的距离
					fStep := fDistance / float64(count) // 宽度
					if op < b1 {
						nResult = 0
					} else if op >= b2 {
						nResult = count + 1
					} else {
						nResult = int32((op-b1)/fStep + 1)
					}
				}

				return tree.NewDInt(tree.DInt(nResult)), nil
			},
			Info: "返回一个桶号，这个桶是在一个柱状图中operand将被分配的那个桶，该柱状图有count个散布在范围b1到b2上的等宽桶。对于超过该范围的输入，将返回0或者count+1",
		},
		tree.Overload{
			Types: tree.ArgTypes{{Name: "op", Typ: types.Float},
				{Name: "b1", Typ: types.Float},
				{Name: "b2", Typ: types.Float},
				{Name: "count", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args.Len() == 0 {
					return nil, errors.New("invalid number of arguments")
				}
				op := float32(*args[0].(*tree.DFloat))
				b1 := float32(*args[1].(*tree.DFloat))
				b2 := float32(*args[2].(*tree.DFloat))
				count := int32(tree.MustBeDInt(args[3]))

				if b1 == b2 {
					return nil, errors.New("lower bound cannot equal upper bound")
				}

				var nResult int32 // 结果

				if b2 < b1 {
					if op > b1 {
						nResult = 0
					} else if op < b2 {
						nResult = count
					} else {
						nResult = count + 1
					}
				} else {
					fDistance := b2 - b1                // 范围b1到b2的距离
					fStep := fDistance / float32(count) // 宽度
					if op < b1 {
						nResult = 0
					} else if op >= b2 {
						nResult = count + 1
					} else {
						nResult = int32((op-b1)/fStep + 1)
					}
				}

				return tree.NewDInt(tree.DInt(nResult)), nil
			},
			Info: "返回一个桶号，这个桶是在一个柱状图中operand将被分配的那个桶，该柱状图有count个散布在范围b1到b2上的等宽桶。对于超过该范围的输入，将返回0或者count+1",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "element", Typ: types.Any},
				{Name: "array", Typ: types.AnyArray}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				argArray := tree.MustBeDArray(args[1]).Array
				elementArg := args[0]
				var type1, type2 int
				var err error
				typeJudge := func(ele interface{}) (int, error) { //两个参数类型匹配检查,不同数字代表不同类型
					for _, v := range argArray {
						if v == tree.DNull {
							return -1, errors.New("thresholds array must not contain NULLs")
						}
					}
					switch ele.(type) {
					case *tree.DInt:
						return 1, nil
					case *tree.DString:
						return 2, nil
					case *tree.DDecimal:
						return 3, nil
					case *tree.DTimestamp:
						return 4, nil
					case *tree.DTimestampTZ:
						return 5, nil
					default:
						return -1, errors.New("not support type of arg")
					}
				}
				type1, err = typeJudge(args[0])
				type2, err = typeJudge(argArray[0])
				if err != nil {
					return nil, err
				}
				if type1 != type2 { //类型匹配检查
					return nil, errors.New("arg type mismatch")
				}
				res := 0
				for k, v := range argArray { //所属区间判断
					if elementArg.Compare(ctx, v) < 0 {
						res = k
						return tree.NewDInt(tree.DInt(res)), nil
					}
					if elementArg.Compare(ctx, v) == 0 {
						res = k + 1
						return tree.NewDInt(tree.DInt(res)), nil
					}

				}
				res = len(argArray)
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info: "返回一个桶号，这个桶是在给定数组中operand 将被分配的桶，该数组列出了桶的下界。对于一个低于第一个下界的输入返回 0。thresholds数组必须被排好序， 最小的排在最前面，否则将会得到意想不到的结果",
		},
	),
}

var lengthImpls = makeBuiltin(tree.FunctionProperties{Category: categoryString},
	stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
		return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s))), nil
	}, types.Int, "Calculates the number of characters in `val`."),
	bytesOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
		return tree.NewDInt(tree.DInt(len(s))), nil
	}, types.Int, "Calculates the number of bytes in `val`."),
)

var substringImpls = makeBuiltin(tree.FunctionProperties{Category: categoryString},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "input", Typ: types.String},
			{Name: "substr_pos", Typ: types.Int},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			runes := []rune(string(tree.MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(tree.MustBeDInt(args[1])) - 1

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return tree.NewDString(string(runes[start:])), nil
		},
		Info: "Returns a substring of `input` starting at `substr_pos` (count starts at 1).",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "input", Typ: types.String},
			{Name: "start_pos", Typ: types.Int},
			{Name: "end_pos", Typ: types.Int},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			runes := []rune(string(tree.MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(tree.MustBeDInt(args[1])) - 1
			length := int(tree.MustBeDInt(args[2]))

			if length < 0 {
				return nil, pgerror.NewErrorf(
					pgcode.InvalidParameterValue, "negative substring length %d not allowed", length)
			}

			end := start + length
			// Check for integer overflow.
			if end < start {
				end = len(runes)
			} else if end < 0 {
				end = 0
			} else if end > len(runes) {
				end = len(runes)
			}

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return tree.NewDString(string(runes[start:end])), nil
		},
		Info: "Returns a substring of `input` between `start_pos` and `end_pos` (count starts at 1).",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "input", Typ: types.String},
			{Name: "regex", Typ: types.String},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDString(args[0]))
			pattern := string(tree.MustBeDString(args[1]))
			return regexpExtract(ctx, s, pattern, `\`)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex`.",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "input", Typ: types.String},
			{Name: "regex", Typ: types.String},
			{Name: "escape_char", Typ: types.String},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDString(args[0]))
			pattern := string(tree.MustBeDString(args[1]))
			escape := string(tree.MustBeDString(args[2]))
			return regexpExtract(ctx, s, pattern, escape)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex` using " +
			"`escape_char` as your escape character instead of `\\`.",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "input", Typ: types.Bytes},
			{Name: "substr_pos", Typ: types.Int},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			runes := []rune(string(tree.MustBeDBytes(args[0])))
			// SQL strings are 1-indexed.
			start := int(tree.MustBeDInt(args[1])) - 1

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}
			return tree.NewDString(string(runes[start:])), nil
		},
		Info: "Returns a substring of `input` starting at `substr_pos` (count starts at 1).",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "input", Typ: types.Bytes},
			{Name: "start_pos", Typ: types.Int},
			{Name: "end_pos", Typ: types.Int},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			runes := []rune(string(tree.MustBeDBytes(args[0])))
			// SQL strings are 1-indexed.
			start := int(tree.MustBeDInt(args[1])) - 1
			length := int(tree.MustBeDInt(args[2]))

			if length < 0 {
				return nil, pgerror.NewErrorf(
					pgcode.InvalidParameterValue, "negative substring length %d not allowed", length)
			}

			end := start + length
			// Check for integer overflow.
			if end < start {
				end = len(runes)
			} else if end < 0 {
				end = 0
			} else if end > len(runes) {
				end = len(runes)
			}

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return tree.NewDString(string(runes[start:end])), nil
		},
		Info: "Returns a substring of `input` between `start_pos` and `end_pos` (count starts at 1).",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "input", Typ: types.Bytes},
			{Name: "regex", Typ: types.Bytes},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDBytes(args[0]))
			pattern := string(tree.MustBeDBytes(args[1]))
			return regexpExtract(ctx, s, pattern, `\`)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex`.",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "input", Typ: types.Bytes},
			{Name: "regex", Typ: types.Bytes},
			{Name: "escape_char", Typ: types.Bytes},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDBytes(args[0]))
			pattern := string(tree.MustBeDBytes(args[1]))
			escape := string(tree.MustBeDBytes(args[2]))
			return regexpExtract(ctx, s, pattern, escape)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex` using " +
			"`escape_char` as your escape character instead of `\\`.",
	},
)

var uuidV4Impl = makeBuiltin(
	tree.FunctionProperties{
		Category: categoryIDGeneration,
		Impure:   true,
	},
	tree.Overload{
		Types:      tree.ArgTypes{},
		ReturnType: tree.FixedReturnType(types.Bytes),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return tree.NewDBytes(tree.DBytes(uuid.MakeV4().GetBytes())), nil
		},
		Info: "Returns a UUID.",
	},
)

var ceilImpl = makeBuiltin(defProps(),
	floatOverload1(func(x float64) (tree.Datum, error) {
		return tree.NewDFloat(tree.DFloat(math.Ceil(x))), nil
	}, "Calculates the smallest integer greater than `val`."),
	decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
		dd := &tree.DDecimal{}
		_, err := tree.ExactCtx.Ceil(&dd.Decimal, x)
		if dd.IsZero() {
			dd.Negative = false
		}
		return dd, err
	}, "Calculates the smallest integer greater than `val`."),
)

var txnTSContextDoc = `

The value is based on a timestamp picked when the transaction starts
and which stays constant throughout the transaction. This timestamp
has no relationship with the commit order of concurrent transactions.`

var txnTSDoc = `Returns the time of the current transaction.` + txnTSContextDoc

var txnTSImpl = makeBuiltin(
	tree.FunctionProperties{
		Category: categoryDateAndTime,
		Impure:   true,
	},
	tree.Overload{
		Types:             tree.ArgTypes{},
		ReturnType:        tree.FixedReturnType(types.TimestampTZ),
		PreferredOverload: true,
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return ctx.GetTxnTimestamp(time.Microsecond), nil
		},
		Info: txnTSDoc,
	},
	tree.Overload{
		Types:      tree.ArgTypes{},
		ReturnType: tree.FixedReturnType(types.Timestamp),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return ctx.GetTxnTimestampNoZone(time.Microsecond), nil
		},
		Info: txnTSDoc,
	},
)

var powImpls = makeBuiltin(defProps(),
	floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
		return tree.NewDFloat(tree.DFloat(math.Pow(x, y))), nil
	}, "Calculates `x`^`y`."),
	decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
		dd := &tree.DDecimal{}
		_, err := tree.DecimalCtx.Pow(&dd.Decimal, x, y)
		return dd, err
	}, "Calculates `x`^`y`."),
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "x", Typ: types.Int},
			{Name: "y", Typ: types.Int},
		},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			xd := apd.New(int64(tree.MustBeDInt(args[0])), 0)
			yd := apd.New(int64(tree.MustBeDInt(args[1])), 0)
			_, err := tree.DecimalCtx.Pow(&dd.Decimal, xd, yd)
			if err != nil {
				return nil, err
			}
			return dd, nil
		},
		Info: "Calculates `x`^`y`.",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{Name: "x", Typ: types.Int},
			{Name: "y", Typ: types.Decimal},
		},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			xd := apd.New(int64(tree.MustBeDInt(args[0])), 0)
			yd := &args[1].(*tree.DDecimal).Decimal
			_, err := tree.DecimalCtx.Pow(&dd.Decimal, xd, yd)
			if err != nil {
				return nil, err
			}
			return dd, nil
		},
		Info: "Calculates `x`^`y`.",
	},
)

var (
	jsonNullDString    = tree.NewDString("null")
	jsonStringDString  = tree.NewDString("string")
	jsonNumberDString  = tree.NewDString("number")
	jsonBooleanDString = tree.NewDString("boolean")
	jsonArrayDString   = tree.NewDString("array")
	jsonObjectDString  = tree.NewDString("object")
)

var (
	errJSONObjectNotEvenNumberOfElements = pgerror.NewError(pgcode.InvalidParameterValue,
		"array must have even number of elements")
	errJSONObjectNullValueForKey = pgerror.NewError(pgcode.InvalidParameterValue,
		"null value not allowed for object key")
	errJSONObjectMismatchedArrayDim = pgerror.NewError(pgcode.InvalidParameterValue,
		"mismatched array dimensions")
)

var jsonExtractPathImpl = tree.Overload{
	Types:      tree.VariadicType{FixedTypes: []types.T{types.JSON}, VarType: types.String},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j := tree.MustBeDJSON(args[0])
		path := make([]string, len(args)-1)
		for i, v := range args {
			if i == 0 {
				continue
			}
			if v == tree.DNull {
				return tree.DNull, nil
			}
			path[i-1] = string(tree.MustBeDString(v))
		}
		result, err := json.FetchPath(j.JSON, path)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return tree.DNull, nil
		}
		return &tree.DJSON{JSON: result}, nil
	},
	Info: "Returns the JSON value pointed to by the variadic arguments.",
}

// darrayToStringSlice converts an array of string datums to a Go array of
// strings. If any of the elements are NULL, then ok will be returned as false.
func darrayToStringSlice(d tree.DArray) (result []string, ok bool) {
	result = make([]string, len(d.Array))
	for i, s := range d.Array {
		if s == tree.DNull {
			return nil, false
		}
		result[i] = string(tree.MustBeDString(s))
	}
	return result, true
}

// checkHasNulls returns an appropriate error if the array contains a NULL.
func checkHasNulls(ary tree.DArray) error {
	if ary.HasNulls {
		for i := range ary.Array {
			if ary.Array[i] == tree.DNull {
				return pgerror.NewErrorf(pgcode.NullValueNotAllowed, "path element at position %d is null", i+1)
			}
		}
	}
	return nil
}

var jsonSetImpl = tree.Overload{
	Types: tree.ArgTypes{
		{Name: "val", Typ: types.JSON},
		{Name: "path", Typ: types.TArray{Typ: types.String}},
		{Name: "to", Typ: types.JSON},
	},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return jsonDatumSet(args[0], args[1], args[2], tree.DBoolTrue)
	},
	Info: "Returns the JSON value pointed to by the variadic arguments.",
}

var jsonSetWithCreateMissingImpl = tree.Overload{
	Types: tree.ArgTypes{
		{Name: "val", Typ: types.JSON},
		{Name: "path", Typ: types.TArray{Typ: types.String}},
		{Name: "to", Typ: types.JSON},
		{Name: "create_missing", Typ: types.Bool},
	},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return jsonDatumSet(args[0], args[1], args[2], args[3])
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. " +
		"If `create_missing` is false, new keys will not be inserted to objects " +
		"and values will not be prepended or appended to arrays.",
}

func jsonDatumSet(
	targetD tree.Datum, pathD tree.Datum, toD tree.Datum, createMissingD tree.Datum,
) (tree.Datum, error) {
	ary := *tree.MustBeDArray(pathD)
	// jsonb_set only errors if there is a null at the first position, but not
	// at any other positions.
	if err := checkHasNulls(ary); err != nil {
		return nil, err
	}
	path, ok := darrayToStringSlice(ary)
	if !ok {
		return targetD, nil
	}
	j, err := json.DeepSet(tree.MustBeDJSON(targetD).JSON, path, tree.MustBeDJSON(toD).JSON, bool(tree.MustBeDBool(createMissingD)))
	if err != nil {
		return nil, err
	}
	return &tree.DJSON{JSON: j}, nil
}

var jsonInsertImpl = tree.Overload{
	Types: tree.ArgTypes{
		{Name: "target", Typ: types.JSON},
		{Name: "path", Typ: types.TArray{Typ: types.String}},
		{Name: "new_val", Typ: types.JSON},
	},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return insertToJSONDatum(args[0], args[1], args[2], tree.DBoolFalse)
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. `new_val` will be inserted before path target.",
}

var jsonInsertWithInsertAfterImpl = tree.Overload{
	Types: tree.ArgTypes{
		{Name: "target", Typ: types.JSON},
		{Name: "path", Typ: types.TArray{Typ: types.String}},
		{Name: "new_val", Typ: types.JSON},
		{Name: "insert_after", Typ: types.Bool},
	},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return insertToJSONDatum(args[0], args[1], args[2], args[3])
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. " +
		"If `insert_after` is true (default is false), `new_val` will be inserted after path target.",
}

func insertToJSONDatum(
	targetD tree.Datum, pathD tree.Datum, newValD tree.Datum, insertAfterD tree.Datum,
) (tree.Datum, error) {
	ary := *tree.MustBeDArray(pathD)

	// jsonb_insert only errors if there is a null at the first position, but not
	// at any other positions.
	if err := checkHasNulls(ary); err != nil {
		return nil, err
	}
	path, ok := darrayToStringSlice(ary)
	if !ok {
		return targetD, nil
	}
	j, err := json.DeepInsert(tree.MustBeDJSON(targetD).JSON, path, tree.MustBeDJSON(newValD).JSON, bool(tree.MustBeDBool(insertAfterD)))
	if err != nil {
		return nil, err
	}
	return &tree.DJSON{JSON: j}, nil
}

var jsonTypeOfImpl = tree.Overload{
	Types:      tree.ArgTypes{{Name: "val", Typ: types.JSON}},
	ReturnType: tree.FixedReturnType(types.String),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		t := tree.MustBeDJSON(args[0]).JSON.Type()
		switch t {
		case json.NullJSONType:
			return jsonNullDString, nil
		case json.StringJSONType:
			return jsonStringDString, nil
		case json.NumberJSONType:
			return jsonNumberDString, nil
		case json.FalseJSONType, json.TrueJSONType:
			return jsonBooleanDString, nil
		case json.ArrayJSONType:
			return jsonArrayDString, nil
		case json.ObjectJSONType:
			return jsonObjectDString, nil
		}
		return nil, pgerror.NewAssertionErrorf("unexpected JSON type %d", t)
	},
	Info: "Returns the type of the outermost JSON value as a text string.",
}

func jsonProps() tree.FunctionProperties {
	return tree.FunctionProperties{
		Category: categoryJSON,
	}
}

func jsonPropsNullableArgs() tree.FunctionProperties {
	d := jsonProps()
	d.NullableArgs = true
	return d
}

var jsonBuildObjectImpl = tree.Overload{
	Types:      tree.VariadicType{VarType: types.Any},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		if len(args)%2 != 0 {
			return nil, pgerror.NewError(pgcode.InvalidParameterValue,
				"argument list must have even number of elements")
		}

		builder := json.NewObjectBuilder(len(args) / 2)
		for i := 0; i < len(args); i += 2 {
			if args[i] == tree.DNull {
				return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue,
					"argument %d cannot be null", i+1)
			}

			key, err := asJSONBuildObjectKey(args[i])
			if err != nil {
				return nil, err
			}

			val, err := tree.AsJSON(args[i+1])
			if err != nil {
				return nil, err
			}

			builder.Add(key, val)
		}

		return tree.NewDJSON(builder.Build()), nil
	},
	Info: "Builds a JSON object out of a variadic argument list.",
}

var toJSONImpl = tree.Overload{
	Types:      tree.ArgTypes{{Name: "val", Typ: types.Any}},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return toJSONObject(args[0])
	},
	Info: "Returns the value as JSON or JSONB.",
}

var prettyPrintNotSupportedError = pgerror.NewErrorf(pgcode.FeatureNotSupported, "pretty printing is not supported")

var arrayToJSONImpls = makeBuiltin(jsonProps(),
	tree.Overload{
		Types:      tree.ArgTypes{{Name: "array", Typ: types.AnyArray}},
		ReturnType: tree.FixedReturnType(types.JSON),
		Fn:         toJSONImpl.Fn,
		Info:       "Returns the array as JSON or JSONB.",
	},
	tree.Overload{
		Types:      tree.ArgTypes{{Name: "array", Typ: types.AnyArray}, {Name: "pretty_bool", Typ: types.Bool}},
		ReturnType: tree.FixedReturnType(types.JSON),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			prettyPrint := bool(tree.MustBeDBool(args[1]))
			if prettyPrint {
				return nil, prettyPrintNotSupportedError
			}
			return toJSONObject(args[0])
		},
		Info: "Returns the array as JSON or JSONB.",
	},
)

var jsonBuildArrayImpl = tree.Overload{
	Types:      tree.VariadicType{VarType: types.Any},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		builder := json.NewArrayBuilder(len(args))
		for _, arg := range args {
			j, err := tree.AsJSON(arg)
			if err != nil {
				return nil, err
			}
			builder.Add(j)
		}
		return tree.NewDJSON(builder.Build()), nil
	},
	Info: "Builds a possibly-heterogeneously-typed JSON or JSONB array out of a variadic argument list.",
}

var jsonObjectImpls = makeBuiltin(jsonProps(),
	tree.Overload{
		Types:      tree.ArgTypes{{Name: "texts", Typ: types.TArray{Typ: types.String}}},
		ReturnType: tree.FixedReturnType(types.JSON),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			arr := tree.MustBeDArray(args[0])
			if arr.Len()%2 != 0 {
				return nil, errJSONObjectNotEvenNumberOfElements
			}
			builder := json.NewObjectBuilder(arr.Len() / 2)
			for i := 0; i < arr.Len(); i += 2 {
				if arr.Array[i] == tree.DNull {
					return nil, errJSONObjectNullValueForKey
				}
				key, err := asJSONObjectKey(arr.Array[i])
				if err != nil {
					return nil, err
				}
				val, err := tree.AsJSON(arr.Array[i+1])
				if err != nil {
					return nil, err
				}
				builder.Add(key, val)
			}
			return tree.NewDJSON(builder.Build()), nil
		},
		Info: "Builds a JSON or JSONB object out of a text array. The array must have " +
			"exactly one dimension with an even number of members, in which case " +
			"they are taken as alternating key/value pairs.",
	},
	tree.Overload{
		Types: tree.ArgTypes{{Name: "keys", Typ: types.TArray{Typ: types.String}},
			{Name: "values", Typ: types.TArray{Typ: types.String}}},
		ReturnType: tree.FixedReturnType(types.JSON),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			keys := tree.MustBeDArray(args[0])
			values := tree.MustBeDArray(args[1])
			if keys.Len() != values.Len() {
				return nil, errJSONObjectMismatchedArrayDim
			}
			builder := json.NewObjectBuilder(keys.Len())
			for i := 0; i < keys.Len(); i++ {
				if keys.Array[i] == tree.DNull {
					return nil, errJSONObjectNullValueForKey
				}
				key, err := asJSONObjectKey(keys.Array[i])
				if err != nil {
					return nil, err
				}
				val, err := tree.AsJSON(values.Array[i])
				if err != nil {
					return nil, err
				}
				builder.Add(key, val)
			}
			return tree.NewDJSON(builder.Build()), nil
		},
		Info: "This form of json_object takes keys and values pairwise from two " +
			"separate arrays. In all other respects it is identical to the " +
			"one-argument form.",
	},
)

var jsonStripNullsImpl = tree.Overload{
	Types:      tree.ArgTypes{{Name: "from_json", Typ: types.JSON}},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j, _, err := tree.MustBeDJSON(args[0]).StripNulls()
		return tree.NewDJSON(j), err
	},
	Info: "Returns from_json with all object fields that have null values omitted. Other null values are untouched.",
}

var jsonArrayLengthImpl = tree.Overload{
	Types:      tree.ArgTypes{{Name: "json", Typ: types.JSON}},
	ReturnType: tree.FixedReturnType(types.Int),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j := tree.MustBeDJSON(args[0])
		switch j.Type() {
		case json.ArrayJSONType:
			return tree.NewDInt(tree.DInt(j.Len())), nil
		case json.ObjectJSONType:
			return nil, pgerror.NewError(pgcode.InvalidParameterValue,
				"cannot get array length of a non-array")
		default:
			return nil, pgerror.NewError(pgcode.InvalidParameterValue,
				"cannot get array length of a scalar")
		}
	},
	Info: "Returns the number of elements in the outermost JSON or JSONB array.",
}

func arrayBuiltin(impl func(types.T) tree.Overload) builtinDefinition {
	overloads := make([]tree.Overload, 0, len(types.AnyNonArray))
	for _, typ := range types.AnyNonArray {
		if ok, _ := types.IsValidArrayElementType(typ); ok {
			overloads = append(overloads, impl(typ))
		}
	}
	return builtinDefinition{
		props:     tree.FunctionProperties{Category: categoryArray},
		overloads: overloads,
	}
}

func setProps(props tree.FunctionProperties, d builtinDefinition) builtinDefinition {
	d.props = props
	return d
}

func decimalLogFn(
	logFn func(*apd.Decimal, *apd.Decimal) (apd.Condition, error), info string,
) tree.Overload {
	return decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
		// TODO(mjibson): see #13642
		switch x.Sign() {
		case -1:
			return nil, errLogOfNegNumber
		case 0:
			return nil, errLogOfZero
		}
		dd := &tree.DDecimal{}
		_, err := logFn(&dd.Decimal, x)
		return dd, err
	}, info)
}

func intOverload(f func(int64) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
		ReturnType: tree.FixedReturnType(types.Int),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(int64(*args[0].(*tree.DInt)))
		},
		Info: info,
	}
}

func intOverload1(f func(int64) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(int64(*args[0].(*tree.DInt)))
		},
		Info: info,
	}
}
func intOverload2(f func(int64) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Int}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(int64(*args[0].(*tree.DInt)))
		},
		Info: info,
	}
}

func floatOverload1(f func(float64) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)))
		},
		Info: info,
	}
}
func floatOverload3(f func(float64) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Float}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)))
		},
		Info: info,
	}
}

func floatOverload2(
	a, b string, f func(float64, float64) (tree.Datum, error), info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: a, Typ: types.Float}, {Name: b, Typ: types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)),
				float64(*args[1].(*tree.DFloat)))
		},
		Info: info,
	}
}

func decimalOverload1(f func(*apd.Decimal) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec := &args[0].(*tree.DDecimal).Decimal
			return f(dec)
		},
		Info: info,
	}
}
func decimalIntOverload1(f func(*apd.Decimal) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Int),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec := &args[0].(*tree.DDecimal).Decimal
			return f(dec)
		},
		Info: info,
	}
}
func decimalOverload3(f func(*apd.Decimal, int32) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Decimal}, {Name: "val2", Typ: types.Int}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec1 := &args[0].(*tree.DDecimal).Decimal
			dec2 := int(*args[1].(*tree.DInt))
			return f(dec1, int32(dec2))
		},
		Info: info,
	}
}

func decimalOverload2(
	a, b string, f func(*apd.Decimal, *apd.Decimal) (tree.Datum, error), info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: a, Typ: types.Decimal}, {Name: b, Typ: types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec1 := &args[0].(*tree.DDecimal).Decimal
			dec2 := &args[1].(*tree.DDecimal).Decimal
			return f(dec1, dec2)
		},
		Info: info,
	}
}

func stringOverload1(
	f func(*tree.EvalContext, string) (tree.Datum, error), returnType types.T, info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])))
		},
		Info: info,
	}
}

func stringOverload2(
	a, b string,
	f func(*tree.EvalContext, string, string) (tree.Datum, error),
	returnType types.T,
	info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: a, Typ: types.String}, {Name: b, Typ: types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])))
		},
		Info: info,
	}
}

func stringOverload3(
	a, b, c string,
	f func(*tree.EvalContext, string, string, string) (tree.Datum, error),
	returnType types.T,
	info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: a, Typ: types.String}, {Name: b, Typ: types.String}, {Name: c, Typ: types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])), string(tree.MustBeDString(args[2])))
		},
		Info: info,
	}
}

func bytesOverload1(
	f func(*tree.EvalContext, string) (tree.Datum, error), returnType types.T, info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{Name: "val", Typ: types.Bytes}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(*args[0].(*tree.DBytes)))
		},
		Info: info,
	}
}

// feedHash returns true if it encounters any non-Null datum.
func feedHash(h hash.Hash, args tree.Datums) bool {
	var nonNullSeen bool
	for _, datum := range args {
		if datum == tree.DNull {
			continue
		} else {
			nonNullSeen = true
		}
		var buf string
		if d, ok := datum.(*tree.DBytes); ok {
			buf = string(*d)
		} else {
			buf = string(tree.MustBeDString(datum))
		}
		_, err := h.Write([]byte(buf))
		if err != nil {
			panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
		}
	}
	return nonNullSeen
}

func hashBuiltin(newHash func() hash.Hash, info string) builtinDefinition {
	return makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if !feedHash(h, args) {
					return tree.DNull, nil
				}
				return tree.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info: info,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if !feedHash(h, args) {
					return tree.DNull, nil
				}
				return tree.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info: info,
		},
	)
}

func hash32Builtin(newHash func() hash.Hash32, info string) builtinDefinition {
	return makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if !feedHash(h, args) {
					return tree.DNull, nil
				}
				return tree.NewDInt(tree.DInt(h.Sum32())), nil
			},
			Info: info,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if !feedHash(h, args) {
					return tree.DNull, nil
				}
				return tree.NewDInt(tree.DInt(h.Sum32())), nil
			},
			Info: info,
		},
	)
}

func hash64Builtin(newHash func() hash.Hash64, info string) builtinDefinition {
	return makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if !feedHash(h, args) {
					return tree.DNull, nil
				}
				return tree.NewDInt(tree.DInt(h.Sum64())), nil
			},
			Info: info,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if !feedHash(h, args) {
					return tree.DNull, nil
				}
				return tree.NewDInt(tree.DInt(h.Sum64())), nil
			},
			Info: info,
		},
	)
}

type regexpEscapeKey struct {
	sqlPattern string
	sqlEscape  string
}

// Pattern implements the RegexpCacheKey interface.
func (k regexpEscapeKey) Pattern() (string, error) {
	pattern := k.sqlPattern
	if k.sqlEscape != `\` {
		pattern = strings.Replace(pattern, `\`, `\\`, -1)
		pattern = strings.Replace(pattern, k.sqlEscape, `\`, -1)
	}
	return pattern, nil
}

func regexpExtract(ctx *tree.EvalContext, s, pattern, escape string) (tree.Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpEscapeKey{pattern, escape})
	if err != nil {
		return nil, err
	}

	match := patternRe.FindStringSubmatch(s)
	if match == nil {
		return tree.DNull, nil
	}

	if len(match) > 1 {
		return tree.NewDString(match[1]), nil
	}
	return tree.NewDString(match[0]), nil
}

type regexpFlagKey struct {
	sqlPattern string
	sqlFlags   string
}

// Pattern implements the RegexpCacheKey interface.
func (k regexpFlagKey) Pattern() (string, error) {
	return regexpEvalFlags(k.sqlPattern, k.sqlFlags)
}

func regexpReplace(ctx *tree.EvalContext, s, pattern, to, sqlFlags string) (tree.Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpFlagKey{pattern, sqlFlags})
	if err != nil {
		return nil, err
	}

	matchCount := 1
	if strings.ContainsRune(sqlFlags, 'g') {
		matchCount = -1
	}

	finalIndex := 0
	var newString bytes.Buffer

	// regexp.ReplaceAllStringFunc cannot be used here because it does not provide
	// access to regexp submatches for expansion in the replacement string.
	// regexp.ReplaceAllString cannot be used here because it does not allow
	// replacement of a specific number of matches, and does not expose the full
	// match for expansion in the replacement string.
	//
	// regexp.FindAllStringSubmatchIndex must therefore be used, which returns a 2D
	// int array. The outer array is iterated over with this for-range loop, and corresponds
	// to each match of the pattern in the string s. Inside each outer array is an int
	// array with index pairs. The first pair in a given match n ([n][0] & [n][1]) represents
	// the start and end index in s of the matched pattern. Subsequent pairs ([n][2] & [n][3],
	// and so on) represent the start and end index in s of matched subexpressions within the
	// pattern.
	for _, matchIndex := range patternRe.FindAllStringSubmatchIndex(s, matchCount) {
		// matchStart and matchEnd are the boundaries of the current regexp match
		// in the searched text.
		matchStart := matchIndex[0]
		matchEnd := matchIndex[1]

		// Add sections of s either before the first match or between matches.
		preMatch := s[finalIndex:matchStart]
		newString.WriteString(preMatch)

		// We write out `to` into `newString` in chunks, flushing out the next chunk
		// when we hit a `\\` or a backreference.
		// chunkStart is the start of the next chunk we will flush out.
		chunkStart := 0
		// i is the current position in the replacement text that we are scanning
		// through.
		i := 0
		for i < len(to) {
			if to[i] == '\\' && i+1 < len(to) {
				i++
				if to[i] == '\\' {
					// `\\` is special in regexpReplace to insert a literal backslash.
					newString.WriteString(to[chunkStart:i])
					chunkStart = i + 1
				} else if ('0' <= to[i] && to[i] <= '9') || to[i] == '&' {
					newString.WriteString(to[chunkStart : i-1])
					chunkStart = i + 1
					if to[i] == '&' {
						// & refers to the entire match.
						newString.WriteString(s[matchStart:matchEnd])
					} else {
						idx := int(to[i] - '0')
						// regexpReplace expects references to "out-of-bounds" capture groups
						// to be ignored.
						if 2*idx < len(matchIndex) {
							newString.WriteString(s[matchIndex[2*idx]:matchIndex[2*idx+1]])
						}
					}
				}
			}
			i++
		}
		newString.WriteString(to[chunkStart:])

		finalIndex = matchEnd
	}

	// Add the section of s past the final match.
	newString.WriteString(s[finalIndex:])

	return tree.NewDString(newString.String()), nil
}

var flagToByte = map[syntax.Flags]byte{
	syntax.FoldCase: 'i',
	syntax.DotNL:    's',
}

var flagToNotByte = map[syntax.Flags]byte{
	syntax.OneLine: 'm',
}

// regexpEvalFlags evaluates the provided Postgres regexp flags in
// accordance with their definitions provided at
// http://www.postgresql.org/docs/9.0/static/functions-matching.html#POSIX-EMBEDDED-OPTIONS-TABLE.
// It then returns an adjusted regexp pattern.
func regexpEvalFlags(pattern, sqlFlags string) (string, error) {
	flags := syntax.DotNL | syntax.OneLine

	for _, sqlFlag := range sqlFlags {
		switch sqlFlag {
		case 'g':
			// Handled in `regexpReplace`.
		case 'i':
			flags |= syntax.FoldCase
		case 'c':
			flags &^= syntax.FoldCase
		case 's':
			flags &^= syntax.DotNL
		case 'm', 'n':
			flags &^= syntax.DotNL
			flags &^= syntax.OneLine
		case 'p':
			flags &^= syntax.DotNL
			flags |= syntax.OneLine
		case 'w':
			flags |= syntax.DotNL
			flags &^= syntax.OneLine
		default:
			return "", pgerror.NewErrorf(
				pgcode.InvalidRegularExpression, "invalid regexp flag: %q", sqlFlag)
		}
	}

	var goFlags bytes.Buffer
	for flag, b := range flagToByte {
		if flags&flag != 0 {
			goFlags.WriteByte(b)
		}
	}
	for flag, b := range flagToNotByte {
		if flags&flag == 0 {
			goFlags.WriteByte(b)
		}
	}
	// Bytes() instead of String() to save an allocation.
	bs := goFlags.Bytes()
	if len(bs) == 0 {
		return pattern, nil
	}
	return fmt.Sprintf("(?%s:%s)", bs, pattern), nil
}

func overlay(s, to string, pos, size int) (tree.Datum, error) {
	if pos < 1 {
		return nil, pgerror.NewErrorf(
			pgcode.InvalidParameterValue, "non-positive substring length not allowed: %d", pos)
	}
	pos--

	runes := []rune(s)
	if pos > len(runes) {
		pos = len(runes)
	}
	after := pos + size
	if after < 0 {
		after = 0
	} else if after > len(runes) {
		after = len(runes)
	}
	return tree.NewDString(string(runes[:pos]) + to + string(runes[after:])), nil
}

// roundDDecimal avoids creation of a new DDecimal in common case where no
// rounding is necessary.
func roundDDecimal(d *tree.DDecimal, scale int32) (tree.Datum, error) {
	// Fast path: check if number of digits after decimal point is already low
	// enough.
	if -d.Exponent <= scale {
		return d, nil
	}
	return roundDecimal(&d.Decimal, scale)
}

func roundDecimal(x *apd.Decimal, scale int32) (tree.Datum, error) {
	dd := &tree.DDecimal{}
	_, err := tree.HighPrecisionCtx.Quantize(&dd.Decimal, x, -scale)
	return dd, err
}

var uniqueIntState struct {
	syncutil.Mutex
	timestamp uint64
}

var uniqueIntEpoch = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()

// NodeIDBits is the number of bits stored in the lower portion of
// GenerateUniqueInt.
const NodeIDBits = 15

// GenerateUniqueInt creates a unique int composed of the current time at a
// 10-microsecond granularity and the node-id. The node-id is stored in the
// lower 15 bits of the returned value and the timestamp is stored in the upper
// 48 bits. The top-bit is left empty so that negative values are not returned.
// The 48-bit timestamp field provides for 89 years of timestamps. We use a
// custom epoch (Jan 1, 2015) in order to utilize the entire timestamp range.
//
// Note that GenerateUniqueInt() imposes a limit on node IDs while
// generateUniqueBytes() does not.
//
// TODO(pmattis): Do we have to worry about persisting the milliseconds value
// periodically to avoid the clock ever going backwards (e.g. due to NTP
// adjustment)?
func GenerateUniqueInt(nodeID roachpb.NodeID) tree.DInt {
	const precision = uint64(10 * time.Microsecond)

	nowNanos := timeutil.Now().UnixNano()
	// Paranoia: nowNanos should never be less than uniqueIntEpoch.
	if nowNanos < uniqueIntEpoch {
		nowNanos = uniqueIntEpoch
	}
	timestamp := uint64(nowNanos-uniqueIntEpoch) / precision

	uniqueIntState.Lock()
	if timestamp <= uniqueIntState.timestamp {
		timestamp = uniqueIntState.timestamp + 1
	}
	uniqueIntState.timestamp = timestamp
	uniqueIntState.Unlock()

	return GenerateUniqueID(int32(nodeID), timestamp)
}

// GenerateUniqueID encapsulates the logic to generate a unique number from
// a nodeID and timestamp.
func GenerateUniqueID(nodeID int32, timestamp uint64) tree.DInt {
	// We xor in the nodeID so that nodeIDs larger than 32K will flip bits in the
	// timestamp portion of the final value instead of always setting them.
	id := (timestamp << NodeIDBits) ^ uint64(nodeID)
	return tree.DInt(id)
}

// GenerateUniqueInt2 creates a unique int composed of the current time at a
// 10-microsecond granularity and the node-id(for int16).
func GenerateUniqueInt2(nodeID roachpb.NodeID) tree.DInt {
	const precision = uint64(10 * time.Microsecond)

	nowNanos := timeutil.Now().UnixNano()
	// Paranoia: nowNanos should never be less than uniqueIntEpoch.
	if nowNanos < uniqueIntEpoch {
		nowNanos = uniqueIntEpoch
	}
	timestamp := uint64(nowNanos-uniqueIntEpoch) / precision

	uniqueIntState.Lock()
	if timestamp <= uniqueIntState.timestamp {
		timestamp = uniqueIntState.timestamp + 1
	}
	uniqueIntState.timestamp = timestamp
	uniqueIntState.Unlock()

	id := timestamp ^ uint64(nodeID)
	return tree.DInt(id)
}

func arrayLength(arr *tree.DArray, dim int64) tree.Datum {
	if arr.Len() == 0 || dim < 1 {
		return tree.DNull
	}
	if dim == 1 {
		return tree.NewDInt(tree.DInt(arr.Len()))
	}
	a, ok := tree.AsDArray(arr.Array[0])
	if !ok {
		return tree.DNull
	}
	return arrayLength(a, dim-1)
}

var intOne = tree.NewDInt(tree.DInt(1))

func arrayLower(arr *tree.DArray, dim int64) tree.Datum {
	if arr.Len() == 0 || dim < 1 {
		return tree.DNull
	}
	if dim == 1 {
		return intOne
	}
	a, ok := tree.AsDArray(arr.Array[0])
	if !ok {
		return tree.DNull
	}
	return arrayLower(a, dim-1)
}

const microsPerMilli = 1000

func extractStringFromTime(fromTime *tree.DTime, timeSpan string) (tree.Datum, error) {
	t := timeofday.TimeOfDay(*fromTime)
	switch timeSpan {
	case "hour", "hours":
		return tree.NewDInt(tree.DInt(t.Hour())), nil
	case "minute", "minutes":
		return tree.NewDInt(tree.DInt(t.Minute())), nil
	case "second", "seconds":
		return tree.NewDInt(tree.DInt(t.Second())), nil
	case "millisecond", "milliseconds":
		return tree.NewDInt(tree.DInt(t.Microsecond() / microsPerMilli)), nil
	case "microsecond", "microseconds":
		return tree.NewDInt(tree.DInt(t.Microsecond())), nil
	case "epoch":
		seconds := time.Duration(t) * time.Microsecond / time.Second
		return tree.NewDInt(tree.DInt(int64(seconds))), nil
	default:
		return nil, pgerror.NewErrorf(
			pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
	}
}

func extractStringFromTimestamp(
	_ *tree.EvalContext, fromTime time.Time, timeSpan string,
) (tree.Datum, error) {
	switch timeSpan {
	case "CENTURY", "C", "century", "centuries", "c":
		return tree.NewDInt(tree.DInt(fromTime.Year()/100 + 1)), nil

	case "DECADE", "decade", "DECADES", "decades":
		return tree.NewDInt(tree.DInt(fromTime.Year() / 10)), nil

	case "MILLENNIUM", "millennium", "MILLENNIUMS", "millenniums":
		return tree.NewDInt(tree.DInt(fromTime.Year()/1000 + 1)), nil

	case "microsecond", "microseconds":
		//return tree.NewDInt(tree.DInt(fromTime.Nanosecond() / int(time.Microsecond))), nil
		return tree.NewDInt(tree.DInt(fromTime.Second()*1000000 + fromTime.Nanosecond()/1000)), nil

	case "year", "years":
		return tree.NewDInt(tree.DInt(fromTime.Year())), nil

	case "quarter":
		return tree.NewDInt(tree.DInt((fromTime.Month()-1)/3 + 1)), nil

	case "month", "months":
		return tree.NewDInt(tree.DInt(fromTime.Month())), nil

	case "week", "weeks":
		_, week := fromTime.ISOWeek()
		return tree.NewDInt(tree.DInt(week)), nil

	case "day", "days":
		return tree.NewDInt(tree.DInt(fromTime.Day())), nil

	case "dayofweek", "dow":
		return tree.NewDInt(tree.DInt(fromTime.Weekday())), nil

	case "dayofyear", "doy":
		return tree.NewDInt(tree.DInt(fromTime.YearDay())), nil

	case "hour", "hours":
		return tree.NewDInt(tree.DInt(fromTime.Hour())), nil

	case "minute", "minutes":
		return tree.NewDInt(tree.DInt(fromTime.Minute())), nil

	case "second", "seconds":
		return tree.NewDInt(tree.DInt(fromTime.Second())), nil

	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		return tree.NewDInt(tree.DInt(fromTime.Second()*1000 + fromTime.Nanosecond()/1000000)), nil

	case "epoch":
		return tree.NewDInt(tree.DInt(fromTime.Unix())), nil

	default:
		return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
	}
}

func truncateTime(fromTime *tree.DTime, timeSpan string) (*tree.DTime, error) {
	t := timeofday.TimeOfDay(*fromTime)
	hour := t.Hour()
	min := t.Minute()
	sec := t.Second()
	micro := t.Microsecond()

	hourTrunc := 0
	minTrunc := 0
	secTrunc := 0
	microTrunc := 0

	switch timeSpan {
	case "day", "days", "week", "weeks", "month", "months", "quarter", "year", "years", "decade", "century", "millennium":
		hour, min, sec, micro = hourTrunc, minTrunc, secTrunc, microTrunc
	case "hour", "hours":
		min, sec, micro = minTrunc, secTrunc, microTrunc
	case "minute", "minutes":
		sec, micro = secTrunc, microTrunc
	case "second", "seconds":
		micro = microTrunc
	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		micro = (micro / microsPerMilli) * microsPerMilli
	case "microsecond", "microseconds":
	default:
		return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
	}

	return tree.MakeDTime(timeofday.New(hour, min, sec, micro)), nil
}

func stringOrNil(d tree.Datum) *string {
	if d == tree.DNull {
		return nil
	}
	s := string(tree.MustBeDString(d))
	return &s
}

// strInsertSubstr returns the string str, with the substring beginning at position
// pos and len characters long replaced by the string newstr.
// Returns the original string if pos is not within the length of the string.
// Replaces the rest of the string from position pos if len is not within
// the length of the rest of the string. Returns NULL if any argument is NULL.
func strInsertSubstr(str string, pos int, length int, newstr string) string {
	lenStr := len(str) // , len(newstr), len_new
	var res string
	if str == "" {
		return ""
	}
	if pos <= 0 || pos > lenStr {
		return str
	}
	if length > lenStr-pos || length < 0 {
		return str[:pos-1] + newstr
	}
	res = str[:pos-1] + newstr + str[pos+length-1:]
	return res
}

// stringToArray implements the string_to_array builtin - str is split on delim to form an array of strings.
// If nullStr is set, any elements equal to it will be NULL.
func stringToArray(str string, delimPtr *string, nullStr *string) (tree.Datum, error) {
	var split []string

	if delimPtr != nil {
		delim := *delimPtr
		if str == "" {
			split = nil
		} else if delim == "" {
			split = []string{str}
		} else {
			split = strings.Split(str, delim)
		}
	} else {
		// When given a NULL delimiter, string_to_array splits into each character.
		split = make([]string, len(str))
		for i, c := range str {
			split[i] = string(c)
		}
	}

	result := tree.NewDArray(types.String)
	for _, s := range split {
		var next tree.Datum = tree.NewDString(s)
		if nullStr != nil && s == *nullStr {
			next = tree.DNull
		}
		if err := result.Append(next); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// arrayToString implements the array_to_string builtin - arr is joined using
// delim. If nullStr is non-nil, NULL values in the array will be replaced by
// it.
func arrayToString(arr *tree.DArray, delim string, nullStr *string) (tree.Datum, error) {
	f := tree.NewFmtCtx(tree.FmtArrayToString)

	for i := range arr.Array {
		if arr.Array[i] == tree.DNull {
			if nullStr == nil {
				continue
			}
			f.WriteString(*nullStr)
		} else {
			f.FormatNode(arr.Array[i])
		}
		if i < len(arr.Array)-1 {
			f.WriteString(delim)
		}
	}
	return tree.NewDString(f.CloseAndGetString()), nil
}

// encodeEscape implements the encode(..., 'escape') Postgres builtin. It's
// described "escape converts zero bytes and high-bit-set bytes to octal
// sequences (\nnn) and doubles backslashes."
func encodeEscape(input []byte) string {
	var result bytes.Buffer
	start := 0
	for i := range input {
		if input[i] == 0 || input[i]&128 != 0 {
			result.Write(input[start:i])
			start = i + 1
			result.WriteString(fmt.Sprintf(`\%03o`, input[i]))
		} else if input[i] == '\\' {
			result.Write(input[start:i])
			start = i + 1
			result.WriteString(`\\`)
		}
	}
	result.Write(input[start:])
	return result.String()
}

var errInvalidSyntaxForDecode = pgerror.NewError(pgcode.InvalidParameterValue, "invalid syntax for decode(..., 'escape')")

func isOctalDigit(c byte) bool {
	return '0' <= c && c <= '7'
}

func decodeOctalTriplet(input string) byte {
	return (input[0]-'0')*64 + (input[1]-'0')*8 + (input[2] - '0')
}

// decodeEscape implements the decode(..., 'escape') Postgres builtin. The
// escape format is described as "escape converts zero bytes and high-bit-set
// bytes to octal sequences (\nnn) and doubles backslashes."
func decodeEscape(input string) ([]byte, error) {
	result := make([]byte, 0, len(input))
	for i := 0; i < len(input); i++ {
		if input[i] == '\\' {
			if i+1 < len(input) && input[i+1] == '\\' {
				result = append(result, '\\')
				i++
			} else if i+3 < len(input) &&
				isOctalDigit(input[i+1]) &&
				isOctalDigit(input[i+2]) &&
				isOctalDigit(input[i+3]) {
				result = append(result, decodeOctalTriplet(input[i+1:i+4]))
				i += 3
			} else {
				return nil, errInvalidSyntaxForDecode
			}
		} else {
			result = append(result, input[i])
		}
	}
	return result, nil
}

func truncateTimestamp(
	_ *tree.EvalContext, fromTime time.Time, timeSpan string,
) (*tree.DTimestampTZ, error) {
	year := fromTime.Year()
	month := fromTime.Month()
	day := fromTime.Day()
	hour := fromTime.Hour()
	min := fromTime.Minute()
	sec := fromTime.Second()
	nsec := fromTime.Nanosecond()
	loc := fromTime.Location()

	monthTrunc := time.January
	dayTrunc := 1
	hourTrunc := 0
	minTrunc := 0
	secTrunc := 0
	nsecTrunc := 0

	switch timeSpan {
	case "millennium":
		firstYear := (year/1000)*1000 + 1
		year, month, day, hour, min, sec, nsec = firstYear, monthTrunc, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc
	case "century":
		firstYear := (year/100)*100 + 1
		year, month, day, hour, min, sec, nsec = firstYear, monthTrunc, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc
	case "decade":
		firstYear := (year / 10) * 10
		year, month, day, hour, min, sec, nsec = firstYear, monthTrunc, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc
	case "year", "years":
		month, day, hour, min, sec, nsec = monthTrunc, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "quarter":
		firstMonthInQuarter := ((month-1)/3)*3 + 1
		month, day, hour, min, sec, nsec = firstMonthInQuarter, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "month", "months":
		day, hour, min, sec, nsec = dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "week", "weeks":
		// Subtract (day of week * nanoseconds per day) to get date as of previous Sunday.
		previousSunday := fromTime.Add(time.Duration(-1 * int64(fromTime.Weekday()) * int64(time.Hour) * 24))
		year, month, day = previousSunday.Year(), previousSunday.Month(), previousSunday.Day()
		hour, min, sec, nsec = hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "day", "days":
		hour, min, sec, nsec = hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "hour", "hours":
		min, sec, nsec = minTrunc, secTrunc, nsecTrunc

	case "minute", "minutes":
		sec, nsec = secTrunc, nsecTrunc

	case "second", "seconds":
		nsec = nsecTrunc

	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		milliseconds := (nsec / int(time.Millisecond)) * int(time.Millisecond)
		nsec = milliseconds

	case "microsecond", "microseconds":
		microseconds := (nsec / int(time.Microsecond)) * int(time.Microsecond)
		nsec = microseconds

	default:
		return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
	}

	toTime := time.Date(year, month, day, hour, min, sec, nsec, loc)
	return tree.MakeDTimestampTZ(toTime, time.Microsecond), nil
}

// Converts a scalar Datum to its string representation
func asJSONBuildObjectKey(d tree.Datum) (string, error) {
	switch t := d.(type) {
	case *tree.DJSON, *tree.DArray, *tree.DTuple:
		return "", pgerror.NewError(pgcode.InvalidParameterValue,
			"key value must be scalar, not array, tuple, or json")
	case *tree.DString:
		return string(*t), nil
	case *tree.DCollatedString:
		return t.Contents, nil
	case *tree.DBool, *tree.DInt, *tree.DFloat, *tree.DDecimal, *tree.DTimestamp, *tree.DTimestampTZ, *tree.DDate, *tree.DUuid, *tree.DInterval, *tree.DBytes, *tree.DIPAddr, *tree.DOid, *tree.DTime:
		return tree.AsStringWithFlags(d, tree.FmtBareStrings), nil
	default:
		return "", pgerror.NewAssertionErrorf("unexpected type %T for key value", d)
	}
}

func asJSONObjectKey(d tree.Datum) (string, error) {
	switch t := d.(type) {
	case *tree.DString:
		return string(*t), nil
	default:
		return "", pgerror.NewAssertionErrorf("unexpected type %T for asJSONObjectKey", d)
	}
}

func toJSONObject(d tree.Datum) (tree.Datum, error) {
	j, err := tree.AsJSON(d)
	if err != nil {
		return nil, err
	}
	return tree.NewDJSON(j), nil
}

// padMaybeTruncate truncates the input string to length if the string is
// longer or equal in size to length. If truncated, the first return value
// will be true, and the last return value will be the truncated string.
// The second return value is set to the length of the input string in runes.
func padMaybeTruncate(s string, length int, fill string) (ok bool, slen int, ret string) {
	if length < 0 {
		// lpad and rpad both return an empty string if the input length is
		// negative.
		length = 0
	}
	slen = utf8.RuneCountInString(s)
	if length == slen {
		return true, slen, s
	}

	// If string is longer then length truncate it to the requested number
	// of characters.
	if length < slen {
		return true, slen, string([]rune(s)[:length])
	}

	// If the input fill is the empty string, return the original string.
	if len(fill) == 0 {
		return true, slen, s
	}

	return false, slen, s
}

func lpad(s string, length int, fill string) (string, error) {
	if length > maxAllocatedStringSize {
		return "", errStringTooLarge
	}
	ok, slen, ret := padMaybeTruncate(s, length, fill)
	if ok {
		return ret, nil
	}
	var buf strings.Builder
	fillRunes := []rune(fill)
	for i := 0; i < length-slen; i++ {
		buf.WriteRune(fillRunes[i%len(fillRunes)])
	}
	buf.WriteString(s)

	return buf.String(), nil
}

func rpad(s string, length int, fill string) (string, error) {
	if length > maxAllocatedStringSize {
		return "", errStringTooLarge
	}
	ok, slen, ret := padMaybeTruncate(s, length, fill)
	if ok {
		return ret, nil
	}
	var buf strings.Builder
	buf.WriteString(s)
	fillRunes := []rune(fill)
	for i := 0; i < length-slen; i++ {
		buf.WriteRune(fillRunes[i%len(fillRunes)])
	}

	return buf.String(), nil
}

// CleanEncodingName sanitizes the string meant to represent a
// recognized encoding. This ignores any non-alphanumeric character.
//
// See function clean_encoding_name() in postgres' sources
// in backend/utils/mb/encnames.c.
func CleanEncodingName(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			b = append(b, c-'A'+'a')
		} else if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b = append(b, c)
		}
	}
	return string(b)
}

var errInsufficientPriv = pgerror.NewError(
	pgcode.InsufficientPrivilege, "insufficient privilege",
)

func checkPrivilegedUser(ctx *tree.EvalContext) error {
	if ctx.SessionData.User != security.RootUser {
		return errInsufficientPriv
	}
	return nil
}

// EvalFollowerReadOffset is a function used often with AS OF SYSTEM TIME queries
// to determine the appropriate offset from now which is likely to be safe for
// follower reads. It is injected by followerreadsccl. An error may be returned
// if an enterprise license is not installed.
var EvalFollowerReadOffset func(clusterID uuid.UUID, _ *cluster.Settings) (time.Duration, error)

func recentTimestamp(ctx *tree.EvalContext) (time.Time, error) {
	if EvalFollowerReadOffset == nil {
		return time.Time{}, pgerror.NewError(pgcode.FeatureNotSupported,
			tree.FollowerReadTimestampFunctionName+
				" is only available in icl distribution")
	}
	offset, err := EvalFollowerReadOffset(ctx.ClusterID, ctx.Settings)
	if err != nil {
		return time.Time{}, err
	}
	return ctx.StmtTimestamp.Add(offset), nil
}

func regexpSplit(ctx *tree.EvalContext, args tree.Datums, hasFlags bool) ([]string, error) {
	s := string(tree.MustBeDString(args[0]))
	pattern := string(tree.MustBeDString(args[1]))
	sqlFlags := ""
	if hasFlags {
		sqlFlags = string(tree.MustBeDString(args[2]))
	}
	patternRe, err := ctx.ReCache.GetRegexp(regexpFlagKey{pattern, sqlFlags})
	if err != nil {
		return nil, err
	}
	return patternRe.Split(s, -1), nil
}

func regexpSplitToArray(
	ctx *tree.EvalContext, args tree.Datums, hasFlags bool,
) (tree.Datum, error) {
	words, err := regexpSplit(ctx, args, hasFlags /* hasFlags */)
	if err != nil {
		return nil, err
	}
	result := tree.NewDArray(types.String)
	for _, word := range words {
		if err := result.Append(tree.NewDString(word)); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func regexpMatch(ctx *tree.EvalContext, s, pattern, escape string) (tree.Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpEscapeKey{pattern, escape})
	if err != nil {
		return nil, err
	}
	match := patternRe.FindStringSubmatch(s)
	if match == nil {
		return tree.DNull, nil
	}
	result := tree.NewDArray(types.String)
	if len(match) == 1 {
		if err := result.Append(tree.NewDString(match[0])); err != nil {
			return nil, err
		}
	} else if len(match) > 1 {
		var i int
		for i = 1; i < len(match); i++ {
			if err := result.Append(tree.NewDString(match[i])); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func regexpMatchFlag(ctx *tree.EvalContext, s, pattern, sqlFlags string) (tree.Datum, error) {
	// patternRe, err := ctx.ReCache.GetRegexp(regexpEscapeKey{pattern, escape})
	patternRe, err := ctx.ReCache.GetRegexp(regexpFlagKey{pattern, sqlFlags})
	if err != nil {
		return nil, err
	}
	match := patternRe.FindStringSubmatch(s)
	if match == nil {
		return tree.DNull, nil
	}
	result := tree.NewDArray(types.String)
	if len(match) == 1 {
		if err := result.Append(tree.NewDString(match[0])); err != nil {
			return nil, err
		}
	} else if len(match) > 1 {
		var i int
		for i = 1; i < len(match); i++ {
			if err := result.Append(tree.NewDString(match[i])); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// CheckBuiltinFunctionWithName will find builtins function exist or not with specific funcName
func CheckBuiltinFunctionWithName(funcName string) bool {
	if _, ok := builtins[funcName]; ok {
		return true
	}
	return false
}

// justifyTime 将time按照cell的大小划分成n部分,返回剩余大小和n
func justifyTime(time int64, cell int64) (int64, int64) {
	t := time / cell
	if t != 0 {
		time -= cell * t
	}
	return time, t
}

// newDInterval make a new tree.DInterval by its month, day and nanos
func newDInterval(months int64, days int64, nanos int64) tree.DInterval {
	ret := tree.DInterval{}
	ret.SetNanos(nanos)
	ret.Months = months
	ret.Days = days
	return ret
}

// parseTimestamp parses year, mon, day, hour, min, sec, local and error from arguments
func parseTimestamp(args tree.Datums) (int, int, int, int, int, float64, string, error) {
	if len(args) > 7 {
		errTimezone := make([]string, 0)
		for i := 6; i < len(args); i++ {
			errTimezone = append(errTimezone, args[i].String())
		}
		return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("invalid time zone:%s", strings.Join(errTimezone, ","))
	}
	if len(args) < 6 {
		return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("args for function parseTimestamp() are not enough")
	}
	year := int(tree.MustBeDInt(args[0]))
	mon := int(tree.MustBeDInt(args[1]))
	day := int(tree.MustBeDInt(args[2]))
	hour := int(tree.MustBeDInt(args[3]))
	min := int(tree.MustBeDInt(args[4]))
	var sec float64
	var errSec error
	switch t := args[5].(type) {
	case *tree.DDecimal:
		sec, errSec = t.Float64()
	case *tree.DInt:
		sec = float64(*t)
	case *tree.DFloat:
		sec = float64(*t)
	default:
		return 0, 0, 0, 0, 0, 0, "", fmt.Errorf("unexpect arg type in function parseTimestamp()")
	}
	if errSec != nil {
		return 0, 0, 0, 0, 0, 0, "", errSec
	}
	loc := "local"
	if len(args) == 7 {
		loc = string(tree.MustBeDString(args[6]))
	}
	return year, mon, day, hour, min, sec, loc, nil
}
