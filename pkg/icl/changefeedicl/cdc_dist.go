// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

func init() {
	rowexec.NewChangeAggregatorProcessor = newChangeAggregatorProcessor
	rowexec.NewChangeFrontierProcessor = newChangeFrontierProcessor
}

const (
	changeAggregatorProcName = `changeagg`
	changeFrontierProcName   = `changefntr`
)

var cdcResultTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},  // resolved span
	{SemanticType: sqlbase.ColumnType_STRING}, // topic
	{SemanticType: sqlbase.ColumnType_BYTES},  // key
	{SemanticType: sqlbase.ColumnType_BYTES},  // value
}

// distCDCFlow plans and runs a distributed cdc.
//
// One or more ChangeAggregator processors watch table data for changes. These
// transform the changed kvs into changed rows and either emit them to a sink
// (such as kafka) or, if there is no sink, forward them in columns 1,2,3 (where
// they will be eventually returned directly via pgwire). In either case,
// periodically a span will become resolved as of some timestamp, meaning that
// no new rows will ever be emitted at or below that timestamp. These span-level
// resolved timestamps are emitted as a marshaled `jobspb.ResolvedSpan` proto in
// column 0.
//
// The flow will always have exactly one ChangeFrontier processor which all the
// ChangeAggregators feed into. It collects all span-level resolved timestamps
// and aggregates them into a cdc-level resolved timestamp, which is the
// minimum of the span-level resolved timestamps. This cdc-level resolved
// timestamp is emitted into the cdc sink (or returned to the gateway if
// there is no sink) whenever it advances. ChangeFrontier also updates the
// progress of the cdc's corresponding system job.
func distCDCFlow(
	ctx context.Context,
	phs sql.PlanHookState,
	jobID int64,
	details jobspb.ChangefeedDetails,
	progress jobspb.Progress,
	resultsCh chan<- tree.Datums,
) error {
	var err error
	details, err = validateDetails(details)
	if err != nil {
		return err
	}

	execCfg := phs.ExecCfg()
	if PushEnabled.Get(&execCfg.Settings.SV) {
		telemetry.Count(`changefeed.run.push.enabled`)
	} else {
		telemetry.Count(`changefeed.run.push.disabled`)
	}

	spansTS := details.StatementTime
	var initialHighWater hlc.Timestamp
	if h := progress.GetHighWater(); h != nil && *h != (hlc.Timestamp{}) {
		initialHighWater = *h
		// If we have a high-water set, use it to compute the spans, since the
		// ones at the statement time may have been garbage collected by now.
		spansTS = initialHighWater
	}

	trackedSpans, err := fetchSpansForTargets(ctx, execCfg.DB, details.Targets, spansTS)
	if err != nil {
		return err
	}

	// cdc flows handle transactional consistency themselves.
	var noTxn *client.Txn
	gatewayNodeID := execCfg.NodeID.Get()
	dsp := phs.DistSQLPlanner()
	evalCtx := phs.ExtendedEvalContext()
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, noTxn)
	_, withDDL := details.Opts[optWithDdl]

	var spanPartitions []sql.SpanPartition
	if details.SinkURI == `` || withDDL {
		// Sinkless feeds get one ChangeAggregator on the gateway.
		spanPartitions = []sql.SpanPartition{{Node: gatewayNodeID, Spans: trackedSpans}}
	} else {
		// All other feeds get a ChangeAggregator local on the leaseholder.
		spanPartitions, err = dsp.PartitionSpans(planCtx, trackedSpans)
		if err != nil {
			return err
		}
	}

	changeAggregatorProcs := make([]distsqlplan.Processor, 0, len(spanPartitions))
	for _, sp := range spanPartitions {
		// TODO(dan): Merge these watches with the span-level resolved
		// timestamps from the job progress.
		watches := make([]distsqlpb.ChangeAggregatorSpec_Watch, len(sp.Spans))
		for i, nodeSpan := range sp.Spans {
			watches[i] = distsqlpb.ChangeAggregatorSpec_Watch{
				Span:            nodeSpan,
				InitialResolved: initialHighWater,
			}
		}

		changeAggregatorProcs = append(changeAggregatorProcs, distsqlplan.Processor{
			Node: sp.Node,
			Spec: distsqlpb.ProcessorSpec{
				Core: distsqlpb.ProcessorCoreUnion{
					ChangeAggregator: &distsqlpb.ChangeAggregatorSpec{
						Watches: watches,
						Feed:    details,
						JobId:   jobID,
					},
				},
				Output: []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
			},
		})
	}
	// NB: This SpanFrontier processor depends on the set of tracked spans being
	// static. Currently there is no way for them to change after the cdc
	// is created, even if it is paused and unpaused, but #28982 describes some
	// ways that this might happen in the future.
	changeFrontierSpec := distsqlpb.ChangeFrontierSpec{
		TrackedSpans: trackedSpans,
		Feed:         details,
		JobID:        jobID,
	}

	var p sql.PhysicalPlan

	stageID := p.NewStageID()
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(changeAggregatorProcs))
	for i, proc := range changeAggregatorProcs {
		proc.Spec.StageID = stageID
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	p.AddSingleGroupStage(
		gatewayNodeID,
		distsqlpb.ProcessorCoreUnion{ChangeFrontier: &changeFrontierSpec},
		distsqlpb.PostProcessSpec{},
		cdcResultTypes,
	)

	p.ResultTypes = cdcResultTypes
	p.PlanToStreamColMap = []int{1, 2, 3}
	dsp.FinalizePlan(planCtx, &p)

	resultRows := makeCDCResultWriter(resultsCh)
	recv := sql.MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		execCfg.LeaseHolderCache,
		noTxn,
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)
	defer recv.Release()

	var finishedSetupFn func()
	if details.SinkURI != `` {
		// We abuse the job's results channel to make CREATE CHANGEFEED wait for
		// this before returning to the user to ensure the setup went okay. Job
		// resumption doesn't have the same hack, but at the moment ignores
		// results and so is currently okay. Return nil instead of anything
		// meaningful so that if we start doing anything with the results
		// returned by resumed jobs, then it breaks instead of returning
		// nonsense.
		finishedSetupFn = func() { resultsCh <- tree.Datums(nil) }
	}

	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, noTxn, &p, recv, &evalCtxCopy, finishedSetupFn)
	return resultRows.Err()
}

// cdcResultWriter implements the `distsqlrun.resultWriter` that sends
// the received rows back over the given channel.
type cdcResultWriter struct {
	rowsCh       chan<- tree.Datums
	rowsAffected int
	err          error
}

func makeCDCResultWriter(rowsCh chan<- tree.Datums) *cdcResultWriter {
	return &cdcResultWriter{rowsCh: rowsCh}
}

func (w *cdcResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	// Copy the row because it's not guaranteed to exist after this function
	// returns.
	row = append(tree.Datums(nil), row...)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.rowsCh <- row:
		return nil
	}
}
func (w *cdcResultWriter) IncrementRowsAffected(n int) {
	w.rowsAffected += n
}
func (w *cdcResultWriter) SetError(err error) {
	w.err = err
}
func (w *cdcResultWriter) Err() error {
	return w.err
}
