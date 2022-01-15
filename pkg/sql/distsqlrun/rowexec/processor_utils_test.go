// Copyright 2018 The Cockroach Authors.
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

package rowexec

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

type ProcessorTestConfig struct {
	// ProcessorTest takes ownership of the evalCtx passed through this FlowCtx.
	FlowCtx *runbase.FlowCtx

	BeforeTestCase func(p runbase.Processor, inputs []runbase.RowSource, output runbase.RowReceiver)
	AfterTestCase  func(p runbase.Processor, inputs []runbase.RowSource, output runbase.RowReceiver)
}

func DefaultProcessorTestConfig() ProcessorTestConfig {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	return ProcessorTestConfig{
		FlowCtx: &runbase.FlowCtx{
			Cfg:     &runbase.ServerConfig{Settings: st},
			EvalCtx: &evalCtx,
		},
	}
}

// ProcessorTestCaseRows is a number of rows of go values with an associated
// schema that can be converted to sqlbase.EncDatumRows.
type ProcessorTestCaseRows struct {
	Rows  [][]interface{}
	Types []sqlbase.ColumnType
}

// toEncDatum converts a go value to an EncDatum.
func toEncDatum(datumType sqlbase.ColumnType, v interface{}) sqlbase.EncDatum {
	d := func() tree.Datum {
		switch concreteType := v.(type) {
		case int:
			if datumType.SemanticType == sqlbase.ColumnType_DECIMAL {
				dd := &tree.DDecimal{}
				dd.SetInt64(int64(v.(int)))
				return dd
			}
			return tree.NewDInt(tree.DInt(v.(int)))
		case bool:
			return tree.MakeDBool(tree.DBool(v.(bool)))
		case nil:
			return tree.DNull
		default:
			panic(fmt.Sprintf("type %T not supported yet", concreteType))
		}
	}()
	return sqlbase.DatumToEncDatum(datumType, d)
}

func (r ProcessorTestCaseRows) toEncDatumRows() sqlbase.EncDatumRows {
	result := make(sqlbase.EncDatumRows, len(r.Rows))
	for i, row := range r.Rows {
		if len(row) != len(r.Types) {
			panic("mismatched number of columns and number of types")
		}
		result[i] = make(sqlbase.EncDatumRow, len(row))
		for j, col := range row {
			result[i][j] = toEncDatum(r.Types[j], col)
		}
	}
	return result
}

// ProcessorTestCase is the specification for a test that creates a processor
// given the struct fields, runs it with the given input, and verifies that
// the output is expected.
type ProcessorTestCase struct {
	Name   string
	Input  ProcessorTestCaseRows
	Output ProcessorTestCaseRows

	// SecondInput can be optionally set by processors that take in two inputs.
	SecondInput *ProcessorTestCaseRows

	// DisableSort disables the sorting of the output produced by the processor
	// before checking for expected output.
	DisableSort bool

	// ProcessorCoreUnion is the spec to be passed in to newProcessor when
	// creating the processor to run this test case.
	ProcessorCore distsqlpb.ProcessorCoreUnion

	// Post is the PostProcessSpec to be used when creating the processor.
	Post distsqlpb.PostProcessSpec
}

// ProcessorTest runs one or more ProcessorTestCases.
type ProcessorTest struct {
	config ProcessorTestConfig
}

// MakeProcessorTest makes a ProcessorTest with the given config.
func MakeProcessorTest(config ProcessorTestConfig) ProcessorTest {
	return ProcessorTest{
		config: config,
	}
}

// RunTestCases runs the given ProcessorTestCases.
func (p *ProcessorTest) RunTestCases(
	ctx context.Context, t *testing.T, testCases []ProcessorTestCase,
) {
	var processorID int32
	for _, tc := range testCases {
		inputs := make([]runbase.RowSource, 1, 2)
		inputs[0] = runbase.NewRowBuffer(
			tc.Input.Types, tc.Input.toEncDatumRows(), runbase.RowBufferArgs{},
		)
		if tc.SecondInput != nil {
			inputs[1] = runbase.NewRowBuffer(
				tc.SecondInput.Types, tc.SecondInput.toEncDatumRows(), runbase.RowBufferArgs{},
			)
		}
		output := runbase.NewRowBuffer(
			tc.Output.Types, nil, runbase.RowBufferArgs{},
		)

		processor, err := NewProcessor(
			ctx,
			p.config.FlowCtx,
			processorID,
			&tc.ProcessorCore,
			&tc.Post,
			inputs,
			[]runbase.RowReceiver{output},
			nil, /* localProcessors */
		)
		if err != nil {
			t.Fatalf("test case %s processor creation failed %s", tc.Name, err)
		}
		processorID++

		if p.config.BeforeTestCase != nil {
			p.config.BeforeTestCase(processor, inputs, output)
		}

		processor.Run(ctx)

		if p.config.AfterTestCase != nil {
			p.config.AfterTestCase(processor, inputs, output)
		}

		expectedRows := tc.Output.toEncDatumRows()
		expected := make([]string, len(expectedRows))
		for i, row := range expectedRows {
			expected[i] = row.String(tc.Output.Types)
		}
		if !tc.DisableSort {
			sort.Strings(expected)
		}

		var returned []string
		for {
			row := output.NextNoMeta(t)
			if row == nil {
				break
			}
			returned = append(returned, row.String(tc.Output.Types))
		}
		if !tc.DisableSort {
			sort.Strings(returned)
		}

		expStr := strings.Join(expected, "")
		retStr := strings.Join(returned, "")
		if expStr != retStr {
			t.Errorf(
				"test case %s (DisableSort=%t) invalid results; expected\n%s\ngot:\n%s",
				tc.Name,
				tc.DisableSort,
				expStr,
				retStr,
			)
		}
	}
}

func (p ProcessorTest) Close(ctx context.Context) {
	p.config.FlowCtx.EvalCtx.Stop(ctx)
}
