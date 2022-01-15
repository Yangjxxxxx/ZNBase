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
	"fmt"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/closedts"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type changeAggregator struct {
	runbase.ProcessorBase

	flowCtx *runbase.FlowCtx
	spec    distsqlpb.ChangeAggregatorSpec
	memAcc  mon.BoundAccount

	// cancel shuts down the processor, both the `Next()` flow and the poller.
	cancel func()
	// errCh contains the return values of the poller.
	errCh chan error
	// poller runs in the background and puts kv changes and resolved spans into
	// a buffer, which is used by `Next()`.
	poller *poller
	// pollerDoneCh is closed when the poller exits.
	pollerDoneCh chan struct{}
	pollerMemMon *mon.BytesMonitor

	// encoder is the Encoder to use for key and value serialization.
	encoder Encoder
	// sink is the Sink to write rows to. Resolved timestamps are never written
	// by changeAggregator.
	sink Sink
	// tickFn is the workhorse behind Next(). It pulls kv changes from the
	// buffer that poller fills, handles table leasing, converts them to rows,
	// and writes them to the sink.
	tickFn func(context.Context) ([]jobspb.ResolvedSpan, error)
	// changedRowBuf, if non-nil, contains changed rows to be emitted. Anything
	// queued in `resolvedSpanBuf` is dependent on these having been emitted, so
	// this one must be empty before moving on to that one.
	changedRowBuf *encDatumRowBuffer
	// resolvedSpanBuf contains resolved span updates to send to changeFrontier.
	// If sink is a bufferSink, it must be emptied before these are sent.
	resolvedSpanBuf encDatumRowBuffer
}

var _ runbase.Processor = &changeAggregator{}
var _ runbase.RowSource = &changeAggregator{}

func newChangeAggregatorProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec distsqlpb.ChangeAggregatorSpec,
	output runbase.RowReceiver,
) (runbase.Processor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "changeagg-mem")
	ca := &changeAggregator{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
	}
	if err := ca.Init(
		ca,
		&distsqlpb.PostProcessSpec{},
		nil, /* types */
		flowCtx,
		processorID,
		output,
		memMonitor,
		runbase.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				ca.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	var err error
	if ca.encoder, err = getEncoder(ca.spec.Feed.Opts); err != nil {
		return nil, err
	}

	return ca, nil
}

func (ca *changeAggregator) OutputTypes() []sqlbase.ColumnType {
	return cdcResultTypes
}

// Start is part of the RowSource interface.
func (ca *changeAggregator) Start(ctx context.Context) context.Context {
	ctx, ca.cancel = context.WithCancel(ctx)
	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = ca.StartInternal(ctx, changeAggregatorProcName)

	nodeID := ca.flowCtx.EvalCtx.NodeID
	var err error
	if ca.sink, err = getSink(
		ca.spec.Feed.SinkURI, nodeID, ca.spec.Feed.Opts, ca.spec.Feed.Targets, ca.flowCtx.Cfg.Settings,
		ca.flowCtx.Cfg.DumpSinkFromURI, ca.spec.JobId,
	); err != nil {
		// Early abort in the case that there is an error creating the sink.
		ca.MoveToDraining(err)
		ca.cancel()
		return ctx
	}

	// This is the correct point to set up certain hooks depending on the sink
	// type.
	if b, ok := ca.sink.(*bufferSink); ok {
		ca.changedRowBuf = &b.buf
	}

	var initialHighWater hlc.Timestamp
	spans := make([]roachpb.Span, 0, len(ca.spec.Watches))
	for _, watch := range ca.spec.Watches {
		spans = append(spans, watch.Span)
		if initialHighWater.IsEmpty() || watch.InitialResolved.Less(initialHighWater) {
			initialHighWater = watch.InitialResolved
		}
	}
	// This SpanFrontier only tracks the spans being watched on this node.
	// There is a different SpanFrontier elsewhere for the entire changefeed.
	// This object is used to filter out some previously emitted rows, and
	// by the cloudStorageSink to name its output files in lexicographically
	// monotonic fashion.
	sf := makeSpanFrontier(spans...)
	for _, watch := range ca.spec.Watches {
		sf.Forward(watch.Span, watch.InitialResolved)
	}

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	metrics := ca.flowCtx.Cfg.JobRegistry.MetricsStruct().CDC.(*Metrics)
	ca.sink = makeMetricsSink(metrics, ca.sink)

	var knobs TestingKnobs
	if cfKnobs, ok := ca.flowCtx.Cfg.TestingKnobs.CDC.(*TestingKnobs); ok {
		knobs = *cfKnobs
	}

	// It seems like we should also be able to use `ca.ProcessorBase.MemMonitor`
	// for the poller, but there is a race between the flow's MemoryMonitor
	// getting Stopped and `changeAggregator.Close`, which causes panics. Not sure
	// what to do about this yet.
	pollerMemMonCapacity := memBufferDefaultCapacity
	if knobs.MemBufferCapacity != 0 {
		pollerMemMonCapacity = knobs.MemBufferCapacity
	}
	pollerMemMon := mon.MakeMonitorInheritWithLimit("poller", math.MaxInt64, ca.ProcessorBase.MemMonitor)
	pollerMemMon.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(pollerMemMonCapacity))
	ca.pollerMemMon = &pollerMemMon

	buf := makeBuffer()
	leaseMgr := ca.flowCtx.Cfg.LeaseManager.(*sql.LeaseManager)
	ca.poller = makePoller(
		ca.flowCtx.Cfg.Settings, ca.flowCtx.Cfg.DB, ca.flowCtx.Cfg.DB.Clock(), ca.flowCtx.Cfg.Gossip,
		spans, ca.spec.Feed, initialHighWater, buf, leaseMgr, metrics, ca.pollerMemMon, SchemaChangeEventClass(ca.spec.Feed.Opts[OptSchemaChangeEvents]),
		SchemaChangePolicy(ca.spec.Feed.Opts[OptSchemaChangePolicy]), ca.sink, ca.flowCtx.Cfg.Executor, ca, sf,
	)
	if _, ok := ca.spec.Feed.Opts[optWithDdl]; ok {
		filepath := dumpsink.MakeSpecifyLocalSinkURI("", ca.flowCtx.NodeID)
		conf, err := dumpsink.ConfFromURI(ctx, filepath)
		if err != nil {
			// Early abort in the case that there is an error creating the sink.
			ca.MoveToDraining(err)
			ca.cancel()
			return ctx
		}
		tempSink, err := ca.flowCtx.Cfg.DumpSink(ctx, conf)
		if err != nil {
			// Early abort in the case that there is an error creating the sink.
			ca.MoveToDraining(err)
			ca.cancel()
			return ctx
		}
		ca.poller.dumpSink = tempSink
	}
	rowsFn := kvsToRows(leaseMgr, ca.spec.Feed, buf.Get)
	ca.tickFn = emitEntrySlice(
		ca.flowCtx.Cfg.Settings, ca.spec.Feed, sf, ca.encoder, ca.sink, rowsFn, knobs, metrics, ca.flowCtx.Cfg.DB, ca.flowCtx.Cfg.Executor, ca.poller)

	// Give errCh enough buffer both possible errors from supporting goroutines,
	// but only the first one is ever used.
	ca.errCh = make(chan error, 2)

	if err := ca.flowCtx.Stopper().RunAsyncTask(ctx, "changefeed-poller", func(ctx context.Context) {
		ca.pollerDoneCh = make(chan struct{})
		defer close(ca.pollerDoneCh)
		var err error
		if PushEnabled.Get(&ca.flowCtx.Cfg.Settings.SV) {
			err = ca.poller.RunUsingRangefeeds(ctx)
		} else {
			err = ca.poller.Run(ctx)
		}

		// Trying to call MoveToDraining here is racy (`MoveToDraining called in
		// state stateTrailingMeta`), so return the error via a channel.
		ca.errCh <- err
		ca.cancel()
	}); err != nil {
		ca.errCh <- err
		ca.cancel()
	}

	return ctx
}

//GetBatch is part of the RowSource interface.
func (ca *changeAggregator) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (ca *changeAggregator) SetChan() {}

// close has two purposes: to synchronize on the completion of the helper
// goroutines created by the Start method, and to clean up any resources used by
// the processor. Due to the fact that this method may be called even if the
// processor did not finish completion, there is an excessive amount of nil
// checking.
func (ca *changeAggregator) close() {
	if ca.InternalClose() {
		// Shut down the poller if it wasn't already.
		if ca.cancel != nil {
			ca.cancel()
		}
		// Wait for the poller to finish shutting down.
		if ca.pollerDoneCh != nil {
			<-ca.pollerDoneCh
		}
		if ca.sink != nil {
			if err := ca.sink.Close(); err != nil {
				log.Warningf(ca.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
			}
		}
		ca.memAcc.Close(ca.Ctx)
		if ca.poller != nil && ca.poller.dumpSink != nil {
			err := ca.poller.dumpSink.Close()
			if err != nil {
				log.Warningf(ca.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
			}
		}
		if ca.pollerMemMon != nil {
			ca.pollerMemMon.Stop(ca.Ctx)
		}
		ca.MemMonitor.Stop(ca.Ctx)
	}
}

// Next is part of the RowSource interface.
func (ca *changeAggregator) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for ca.State == runbase.StateRunning {
		if !ca.changedRowBuf.IsEmpty() {
			return ca.changedRowBuf.Pop(), nil
		} else if !ca.resolvedSpanBuf.IsEmpty() {
			return ca.resolvedSpanBuf.Pop(), nil
		}

		if err := ca.tick(); err != nil {
			select {
			// If the poller errored first, that's the
			// interesting one, so overwrite `err`.
			case err = <-ca.errCh:
			default:
			}
			// Shut down the poller if it wasn't already.
			ca.cancel()

			ca.MoveToDraining(err)
			break
		}
	}
	return nil, ca.DrainHelper()
}

func (ca *changeAggregator) tick() error {
	resolvedSpans, err := ca.tickFn(ca.Ctx)
	if err != nil {
		return err
	}

	for _, resolvedSpan := range resolvedSpans {
		resolvedBytes, err := protoutil.Marshal(&resolvedSpan)
		if err != nil {
			return err
		}
		// EnqueueTxn a row to be returned that indicates some span-level resolved
		// timestamp has advanced. If any rows were queued in `sink`, they must
		// be emitted first.
		ca.resolvedSpanBuf.Push(sqlbase.EncDatumRow{
			sqlbase.EncDatum{Datum: tree.NewDBytes(tree.DBytes(resolvedBytes))},
			sqlbase.EncDatum{Datum: tree.DNull}, // topic
			sqlbase.EncDatum{Datum: tree.DNull}, // key
			sqlbase.EncDatum{Datum: tree.DNull}, // value
		})
	}
	return nil
}

// ConsumerDone is part of the RowSource interface.
func (ca *changeAggregator) ConsumerDone() {
	ca.MoveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (ca *changeAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ca.InternalClose()
}

const (
	emitAllResolved = 0
	emitNoResolved  = -1
)

type changeFrontier struct {
	runbase.ProcessorBase

	flowCtx *runbase.FlowCtx
	spec    distsqlpb.ChangeFrontierSpec
	memAcc  mon.BoundAccount
	a       sqlbase.DatumAlloc

	// input returns rows from one or more changeAggregator processors
	input runbase.RowSource

	// sf contains the current resolved timestamp high-water for the tracked
	// span set.
	sf *spanFrontier
	// encoder is the Encoder to use for resolved timestamp serialization.
	encoder Encoder
	// sink is the Sink to write resolved timestamps to. Rows are never written
	// by changeFrontier.
	sink Sink
	// schemaChangeBoundary represents an hlc timestamp at which a schema change
	// event occurred to a target watched by this frontier. If the changefeed is
	// configured to stop on schema change then the changeFrontier will wait for
	// the span frontier to reach the schemaChangeBoundary, will drain, and then
	// will exit. If the changefeed is configured to backfill on schema changes,
	// the changeFrontier will protect the scan timestamp in order to ensure that
	// the scan complete. The protected timestamp will be released when a new scan
	// schemaChangeBoundary is created or the changefeed reaches a timestamp that
	// is near the present.
	//
	// schemaChangeBoundary values are communicated to the changeFrontier via
	// Resolved messages send from the changeAggregators. The policy regarding
	// which schema change events lead to a schemaChangeBoundary is controlled
	// by the KV feed based on OptSchemaChangeEvents and OptSchemaChangePolicy.
	schemaChangeBoundary hlc.Timestamp

	// freqEmitResolved, if >= 0, is a lower bound on the duration between
	// resolved timestamp emits.
	freqEmitResolved time.Duration
	// lastEmitResolved is the last time a resolved timestamp was emitted.
	lastEmitResolved time.Time
	// lastSlowSpanLog is the last time a slow span from `sf` was logged.
	lastSlowSpanLog time.Time

	// jobProgressedFn, if non-nil, is called to checkpoint the changefeed's
	// progress in the corresponding system job entry.
	jobProgressedFn func(context.Context, jobs.HighWaterProgressedFn) error
	// highWaterAtStart is the greater of the job high-water and the timestamp the
	// CHANGEFEED statement was run at. It's used in an assertion that we never
	// regress the job high-water.
	highWaterAtStart hlc.Timestamp

	// passthroughBuf, in some but not all flows, contains changed row data to
	// pass through unchanged to the gateway node.
	passthroughBuf encDatumRowBuffer
	// resolvedBuf, if non-nil, contains rows indicating a changefeed-level
	// resolved timestamp to be returned. It depends on everything in
	// `passthroughBuf` being sent, so that one needs to be emptied first.
	resolvedBuf *encDatumRowBuffer
	// metrics are monitoring counters shared between all changefeeds.
	metrics *Metrics
	// metricsID is used as the unique id of this changefeed in the
	// metrics.MinHighWater map.
	metricsID int
}

var _ runbase.Processor = &changeFrontier{}
var _ runbase.RowSource = &changeFrontier{}

func newChangeFrontierProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec distsqlpb.ChangeFrontierSpec,
	input runbase.RowSource,
	output runbase.RowReceiver,
) (runbase.Processor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := runbase.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "changefntr-mem")
	cf := &changeFrontier{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
		input:   input,
		sf:      makeSpanFrontier(spec.TrackedSpans...),
	}
	if err := cf.Init(
		cf, &distsqlpb.PostProcessSpec{},
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		memMonitor,
		runbase.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []distsqlpb.ProducerMetadata {
				cf.close()
				return nil
			},
			InputsToDrain: []runbase.RowSource{cf.input},
		},
	); err != nil {
		return nil, err
	}

	if r, ok := cf.spec.Feed.Opts[optResolvedTimestamps]; ok {
		var err error
		if r == `` {
			// Empty means emit them as often as we have them.
			cf.freqEmitResolved = emitAllResolved
		} else if cf.freqEmitResolved, err = time.ParseDuration(r); err != nil {
			return nil, err
		}
	} else {
		cf.freqEmitResolved = emitNoResolved
	}

	var err error
	if cf.encoder, err = getEncoder(spec.Feed.Opts); err != nil {
		return nil, err
	}

	return cf, nil
}

func (cf *changeFrontier) OutputTypes() []sqlbase.ColumnType {
	return cdcResultTypes
}

// Start is part of the RowSource interface.
func (cf *changeFrontier) Start(ctx context.Context) context.Context {
	cf.input.Start(ctx)

	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = cf.StartInternal(ctx, changeFrontierProcName)

	nodeID := cf.flowCtx.EvalCtx.NodeID
	var err error
	if cf.sink, err = getSink(
		cf.spec.Feed.SinkURI, nodeID, cf.spec.Feed.Opts, cf.spec.Feed.Targets, cf.flowCtx.Cfg.Settings,
		cf.flowCtx.Cfg.DumpSinkFromURI, cf.spec.JobID,
	); err != nil {
		cf.MoveToDraining(err)
		return ctx
	}

	if b, ok := cf.sink.(*bufferSink); ok {
		cf.resolvedBuf = &b.buf
	}

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	cf.metrics = cf.flowCtx.Cfg.JobRegistry.MetricsStruct().CDC.(*Metrics)
	cf.sink = makeMetricsSink(cf.metrics, cf.sink)

	if cf.spec.JobID != 0 {
		job, err := cf.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cf.spec.JobID)
		if err != nil {
			cf.MoveToDraining(err)
			return ctx
		}
		cf.jobProgressedFn = job.HighWaterProgressed
	}

	cf.metrics.mu.Lock()
	cf.metricsID = cf.metrics.mu.id
	cf.metrics.mu.id++
	cf.metrics.mu.Unlock()
	go func() {
		// Delete this feed from the MinHighwater metric so it's no longer
		// considered by the gauge.
		//
		// TODO(dan): Ideally this would be done in something like `close` but
		// there's nothing that's guaranteed to be called when a processor shuts
		// down.
		<-ctx.Done()
		cf.metrics.mu.Lock()
		delete(cf.metrics.mu.resolved, cf.metricsID)
		cf.metricsID = -1
		cf.metrics.mu.Unlock()
	}()

	return ctx
}

//GetBatch is part of the RowSource interface.
func (cf *changeFrontier) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (cf *changeFrontier) SetChan() {}

func (cf *changeFrontier) close() {
	if cf.InternalClose() {
		if cf.sink != nil {
			if err := cf.sink.Close(); err != nil {
				log.Warningf(cf.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
			}
		}
		cf.memAcc.Close(cf.Ctx)
		cf.MemMonitor.Stop(cf.Ctx)
	}
}

// shouldFailOnSchemaChange checks the job's spec to determine whether it should
// failed on schema change events after all spans have been resolved.
func (cf *changeFrontier) shouldFailOnSchemaChange() bool {
	policy := SchemaChangePolicy(cf.spec.Feed.Opts[OptSchemaChangePolicy])
	return policy == OptSchemaChangePolicyStop
}

// Next is part of the RowSource interface.
func (cf *changeFrontier) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for cf.State == runbase.StateRunning {
		if !cf.passthroughBuf.IsEmpty() {
			return cf.passthroughBuf.Pop(), nil
		} else if !cf.resolvedBuf.IsEmpty() {
			return cf.resolvedBuf.Pop(), nil
		}

		if cf.schemaChangeBoundaryReached() && cf.shouldFailOnSchemaChange() {
			// TODO(ajwerner): make this more useful by at least informing the client
			// of which tables changed.
			cf.MoveToDraining(pgerror.Newf(pgcode.Code(pgcode.SchemaChangeOccurred),
				"schema change occurred at %v", cf.schemaChangeBoundary.Next().AsOfSystemTime()))
			break
		}
		row, meta := cf.input.Next()
		if meta != nil {
			if meta.Err != nil {
				cf.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			cf.MoveToDraining(nil /* err */)
			break
		}

		if row[0].IsNull() {
			// In changefeeds with a sink, this will never happen. But in the
			// core changefeed, which returns changed rows directly via pgwire,
			// a row with a null resolved_span field is a changed row that needs
			// to be forwarded to the gateway.
			cf.passthroughBuf.Push(row)
			continue
		}

		if err := cf.noteResolvedSpan(row[0]); err != nil {
			cf.MoveToDraining(err)
			break
		}
	}
	return nil, cf.DrainHelper()
}

func (cf *changeFrontier) noteResolvedSpan(d sqlbase.EncDatum) error {
	if err := d.EnsureDecoded(&cdcResultTypes[0], &cf.a); err != nil {
		return err
	}
	raw, ok := d.Datum.(*tree.DBytes)
	if !ok {
		return errors.Errorf(`unexpected datum type %T: %s`, d.Datum, d.Datum)
	}
	var resolved jobspb.ResolvedSpan
	if err := protoutil.Unmarshal([]byte(*raw), &resolved); err != nil {
		return errors.Wrapf(err, `unmarshalling resolved span: %x`, raw)
	}

	// Inserting a timestamp less than the one the changefeed flow started at
	// could potentially regress the job progress. This is not expected, but it
	// was a bug at one point, so assert to prevent regressions.
	//
	// TODO(dan): This is much more naturally expressed as an assert inside the
	// job progress update closure, but it currently doesn't pass along the info
	// we'd need to do it that way.
	if !resolved.Timestamp.IsEmpty() && resolved.Timestamp.Less(cf.highWaterAtStart) {
		log.ReportOrPanic(cf.Ctx, &cf.flowCtx.Cfg.Settings.SV,
			`got a span level timestamp %s for %s that is less than the initial high-water %s`,
			log.Safe(resolved.Timestamp), resolved.Span, log.Safe(cf.highWaterAtStart))
		return nil
	}

	frontierChanged := cf.sf.Forward(resolved.Span, resolved.Timestamp)
	if frontierChanged {
		if err := cf.handleFrontierChanged(); err != nil {
			return err
		}
	}
	cf.maybeLogBehindSpan(frontierChanged)
	return nil
}
func (cf *changeFrontier) handleFrontierChanged() error {
	newResolved := cf.sf.Frontier()
	cf.metrics.mu.Lock()
	if cf.metricsID != -1 {
		cf.metrics.mu.resolved[cf.metricsID] = newResolved
	}
	cf.metrics.mu.Unlock()
	if err := checkpointResolvedTimestamp(cf.Ctx, cf.jobProgressedFn, cf.sf); err != nil {
		return err
	}
	err := cf.maybeEmitResolved(newResolved)
	return err
}
func (cf *changeFrontier) maybeEmitResolved(newResolved hlc.Timestamp) error {
	if cf.freqEmitResolved == emitNoResolved {
		return nil
	}
	sinceEmitted := newResolved.GoTime().Sub(cf.lastEmitResolved)
	shouldEmit := sinceEmitted >= cf.freqEmitResolved
	if !shouldEmit {
		return nil
	}
	// Keeping this after the checkpointResolvedTimestamp call will avoid
	// some duplicates if a restart happens.
	if err := emitResolvedTimestamp(cf.Ctx, cf.encoder, cf.sink, newResolved); err != nil {
		return err
	}
	cf.lastEmitResolved = newResolved.GoTime()
	return nil
}

// Potentially log the most behind span in the frontier for debugging.
func (cf *changeFrontier) maybeLogBehindSpan(frontierChanged bool) {
	// These two cluster setting values represent the target responsiveness of
	// poller and range feed. The cluster setting for switching between poller and
	// rangefeed is only checked at changefeed start/resume, so instead of
	// switching on it here, just add them. Also add 1 second in case both these
	// settings are set really low (as they are in unit tests).
	pollInterval := cdcPollInterval.Get(&cf.flowCtx.Cfg.Settings.SV)
	closedtsInterval := closedts.TargetDuration.Get(&cf.flowCtx.Cfg.Settings.SV)
	slownessThreshold := time.Second + 10*(pollInterval+closedtsInterval)
	frontier := cf.sf.Frontier()
	now := timeutil.Now()
	resolvedBehind := now.Sub(frontier.GoTime())
	if resolvedBehind <= slownessThreshold {
		return
	}

	description := `sinkless feed`
	if cf.spec.JobID != 0 {
		description = fmt.Sprintf("job %d", cf.spec.JobID)
	}
	if frontierChanged {
		log.Infof(cf.Ctx, "%s new resolved timestamp %s is behind by %s",
			description, frontier, resolvedBehind)
	}
	const slowSpanMaxFrequency = 10 * time.Second
	if now.Sub(cf.lastSlowSpanLog) > slowSpanMaxFrequency {
		cf.lastSlowSpanLog = now
		s := cf.sf.PeekFrontierSpan()
		log.Infof(cf.Ctx, "%s span %s is behind by %s", description, s, resolvedBehind)
	}
}

// schemaChangeBoundaryReached returns true if the spanFrontier is at the
// current schemaChangeBoundary.
func (cf *changeFrontier) schemaChangeBoundaryReached() (r bool) {
	return !cf.schemaChangeBoundary.IsEmpty() && cf.schemaChangeBoundary.Equal(cf.sf.Frontier())
}

// ConsumerDone is part of the RowSource interface.
func (cf *changeFrontier) ConsumerDone() {
	cf.MoveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (cf *changeFrontier) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	cf.InternalClose()
}
