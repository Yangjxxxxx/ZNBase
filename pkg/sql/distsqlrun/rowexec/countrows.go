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

	"github.com/opentracing/opentracing-go"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// countAggregator is a simple processor that counts the number of rows it
// receives. It's a specialized aggregator that can be used for COUNT(*).
type countAggregator struct {
	runbase.ProcessorBase
	ret   bool
	input runbase.RowSource
	count int
	done  bool
}

var _ runbase.Processor = &countAggregator{}
var _ runbase.RowSource = &countAggregator{}

const countRowsProcName = "count rows"

var outputTypes = []sqlbase.ColumnType{
	{
		SemanticType: sqlbase.ColumnType_INT,
	},
}

func newCountAggregator(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (*countAggregator, error) {
	ag := &countAggregator{}
	ag.input = input

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		ag.input = NewInputStatCollector(input)
		ag.FinishTrace = ag.outputStatsToTrace
	}

	if err := ag.Init(
		ag,
		post,
		outputTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{ag.input},
		},
	); err != nil {
		return nil, err
	}

	return ag, nil
}

func (ag *countAggregator) Start(ctx context.Context) context.Context {
	ag.input.Start(ctx)
	return ag.StartInternal(ctx, countRowsProcName)
}

//GetBatch is part of the RowSource interface.
func (ag *countAggregator) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (ag *countAggregator) SetChan() {
	ag.input.SetChan()
}

func (ag *countAggregator) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {

	if ag.FlowCtx.EvalCtx.SessionData.PC[sessiondata.TableReaderSet].Parallel && ag.FlowCtx.CanParallel {
		if c, ok := ag.input.(*InputStatCollector); ok {
			if t, ok := c.RowSource.(*tableReader); ok && t.InternalOpt {
				if t.Out.GetFilter() == nil {
					if ag.done {
						if ag.State == runbase.StateRunning {
							ret := make(sqlbase.EncDatumRow, 1)
							ret[0] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(ag.count))}
							rendered, _, err := ag.Out.ProcessRow(ag.Ctx, ret)
							ag.MoveToDraining(err)
							return rendered, nil
						}
						return nil, ag.DrainHelper()
					}
					ag.done = true
					var retMeta *distsqlpb.ProducerMetadata
					ag.count, retMeta = c.Count()
					if retMeta != nil {
						if retMeta.Err != nil {
							ag.MoveToDraining(nil)
							return nil, ag.DrainHelper()
						}
					}
					return nil, retMeta
				}
			}
		}
		if ag.done {
			return nil, nil
		}
		if c, ok := ag.input.(*tableReader); ok && c.InternalOpt {
			if c.Out.GetFilter() == nil {
				if ag.ret {
					return nil, ag.DrainHelper()
				}
				ag.ret = true
				ag.count, _ = c.Count()
				ag.done = true
				ret := make(sqlbase.EncDatumRow, 1)
				ret[0] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(ag.count))}
				rendered, _, err := ag.Out.ProcessRow(ag.Ctx, ret)
				ag.MoveToDraining(err)
				return rendered, nil
			}
		}
	}
	for ag.State == runbase.StateRunning {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(meta.Err)
				break
			}
			return nil, meta
		}
		if row == nil {
			ret := make(sqlbase.EncDatumRow, 1)
			ret[0] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(ag.count))}
			rendered, _, err := ag.Out.ProcessRow(ag.Ctx, ret)
			// We're done as soon as we process our one output row.
			ag.MoveToDraining(err)
			return rendered, nil
		}
		ag.count++
	}
	return nil, ag.DrainHelper()
}

func (ag *countAggregator) ConsumerDone() {
	ag.MoveToDraining(nil /* err */)
}

func (ag *countAggregator) ConsumerClosed() {
	ag.InternalClose()
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (ag *countAggregator) outputStatsToTrace() {
	is, ok := getInputStats(ag.FlowCtx, ag.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(ag.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp, &AggregatorStats{InputStats: is},
		)
	}
}
