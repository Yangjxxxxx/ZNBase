// Copyright 2019  The Cockroach Authors.

package rowexec

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// ordinalityProcessor is the processor of the WITH ORDINALITY operator, which
// adds an additional ordinal column to the result.
type ordinalityProcessor struct {
	runbase.ProcessorBase

	input  runbase.RowSource
	curCnt int64
}

var _ runbase.Processor = &ordinalityProcessor{}
var _ runbase.RowSource = &ordinalityProcessor{}

const ordinalityProcName = "ordinality"

func newOrdinalityProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.OrdinalitySpec,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (runbase.RowSourcedProcessor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	o := &ordinalityProcessor{input: input, curCnt: 1}

	colTypes := make([]sqlbase.ColumnType, len(input.OutputTypes())+1)
	copy(colTypes, input.OutputTypes())
	colTypes[len(colTypes)-1] = sqlbase.IntType
	if err := o.Init(
		o,
		post,
		colTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{o.input},
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				o.ConsumerClosed()
				return nil
			}},
	); err != nil {
		return nil, err
	}

	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		o.input = NewInputStatCollector(o.input)
		o.FinishTrace = o.outputStatsToTrace
	}

	return o, nil
}

// Start is part of the RowSource interface.
func (o *ordinalityProcessor) Start(ctx context.Context) context.Context {
	o.input.Start(ctx)
	return o.StartInternal(ctx, ordinalityProcName)
}

// Next is part of the RowSource interface.
func (o *ordinalityProcessor) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for o.State == runbase.StateRunning {
		row, meta := o.input.Next()

		if meta != nil {
			if meta.Err != nil {
				o.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			o.MoveToDraining(nil /* err */)
			break
		}

		// The ordinality should increment even if the row gets filtered out.
		row = append(row, sqlbase.DatumToEncDatum(sqlbase.IntType, tree.NewDInt(tree.DInt(o.curCnt))))
		o.curCnt++
		if outRow := o.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, o.DrainHelper()

}

// ConsumerClosed is part of the RowSource interface.
func (o *ordinalityProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	o.InternalClose()
}

//GetBatch
func (o *ordinalityProcessor) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (o *ordinalityProcessor) SetChan() {}

const ordinalityTagPrefix = "ordinality."

// Stats implements the SpanStats interface.
func (os *OrdinalityStats) Stats() map[string]string {
	return os.InputStats.Stats(ordinalityTagPrefix)
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (os *OrdinalityStats) StatsForQueryPlan() []string {
	return os.InputStats.StatsForQueryPlan("")
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (o *ordinalityProcessor) outputStatsToTrace() {
	is, ok := getInputStats(o.FlowCtx, o.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(o.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp, &OrdinalityStats{InputStats: is},
		)
	}
}
