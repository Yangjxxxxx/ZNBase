// Copyright 2016 The Cockroach Authors.
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

package distsql

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra/rowflow"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra/vecflow"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/contextutil"
	"github.com/znbasedb/znbase/pkg/util/duration"
	"github.com/znbasedb/znbase/pkg/util/envutil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/log/logtags"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// Version identifies the distsqlrun protocol version.
//
// This version is separate from the main CockroachDB version numbering; it is
// only changed when the distsqlrun API changes.
//
// The planner populates the version in SetupFlowRequest.
// A server only accepts requests with versions in the range MinAcceptedVersion
// to Version.
//
// Is is possible used to provide a "window" of compatibility when new features are
// added. Example:
//  - we start with Version=1; distsqlrun servers with version 1 only accept
//    requests with version 1.
//  - a new distsqlrun feature is added; Version is bumped to 2. The
//    planner does not yet use this feature by default; it still issues
//    requests with version 1.
//  - MinAcceptedVersion is still 1, i.e. servers with version 2
//    accept both versions 1 and 2.
//  - after an upgrade cycle, we can enable the feature in the planner,
//    requiring version 2.
//  - at some later point, we can choose to deprecate version 1 and have
//    servers only accept versions >= 2 (by setting
//    MinAcceptedVersion to 2).
//
// ATTENTION: When updating these fields, add to version_history.txt explaining
// what changed.
const Version distsqlpb.DistSQLVersion = 24

// MinAcceptedVersion is the oldest version that the server is
// compatible with; see above.
const MinAcceptedVersion distsqlpb.DistSQLVersion = 21

// minFlowDrainWait is the minimum amount of time a draining server allows for
// any incoming flows to be registered. It acts as a grace period in which the
// draining server waits for its gossiped draining state to be received by other
// nodes.
const minFlowDrainWait = 1 * time.Second

var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("ZNBASE_NOTEWORTHY_DISTSQL_MEMORY_USAGE", 1024*1024 /* 1MB */)

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	runbase.ServerConfig
	flowRegistry  *flowinfra.FlowRegistry
	flowScheduler *flowinfra.FlowScheduler
	memMonitor    mon.BytesMonitor
	regexpCache   *tree.RegexpCache
}

var _ distsqlpb.DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(ctx context.Context, cfg runbase.ServerConfig) *ServerImpl {
	ds := &ServerImpl{
		ServerConfig:  cfg,
		regexpCache:   tree.NewRegexpCache(512),
		flowRegistry:  flowinfra.NewFlowRegistry(cfg.NodeID.Get()),
		flowScheduler: flowinfra.NewFlowScheduler(cfg.AmbientContext, cfg.Stopper, cfg.Settings, cfg.Metrics),
		memMonitor: mon.MakeMonitor(
			"distsql",
			mon.MemoryResource,
			cfg.Metrics.CurBytesCount,
			cfg.Metrics.MaxBytesHist,
			-1, /* increment: use default block size */
			noteworthyMemoryUsageBytes,
			cfg.Settings,
		),
	}
	ds.memMonitor.Start(ctx, cfg.ParentMemoryMonitor, mon.BoundAccount{})
	return ds
}

// Start launches workers for the server.
func (ds *ServerImpl) Start() {
	// Gossip the version info so that other nodes don't plan incompatible flows
	// for us.
	if err := ds.ServerConfig.Gossip.AddInfoProto(
		gossip.MakeDistSQLNodeVersionKey(ds.ServerConfig.NodeID.Get()),
		&distsqlpb.DistSQLVersionGossipInfo{
			Version:            Version,
			MinAcceptedVersion: MinAcceptedVersion,
		},
		0, // ttl - no expiration
	); err != nil {
		panic(err)
	}

	if err := ds.setDraining(false); err != nil {
		panic(err)
	}

	ds.flowScheduler.Start()
}

// Drain changes the node's draining state through gossip and drains the
// server's flowRegistry. See flowRegistry.Drain for more details.
func (ds *ServerImpl) Drain(ctx context.Context, flowDrainWait time.Duration) {
	if err := ds.setDraining(true); err != nil {
		log.Warningf(ctx, "unable to gossip distsql draining state: %s", err)
	}

	flowWait := flowDrainWait
	minWait := minFlowDrainWait
	if ds.ServerConfig.TestingKnobs.DrainFast {
		flowWait = 0
		minWait = 0
	} else if len(ds.Gossip.Outgoing()) == 0 {
		// If there is only one node in the cluster (us), there's no need to
		// wait a minimum time for the draining state to be gossiped.
		minWait = 0
	}
	ds.flowRegistry.Drain(flowWait, minWait)
}

// Undrain changes the node's draining state through gossip and undrains the
// server's flowRegistry. See flowRegistry.Undrain for more details.
func (ds *ServerImpl) Undrain(ctx context.Context) {
	ds.flowRegistry.Undrain()
	if err := ds.setDraining(false); err != nil {
		log.Warningf(ctx, "unable to gossip distsql draining state: %s", err)
	}
}

// setDraining changes the node's draining state through gossip to the provided
// state.
func (ds *ServerImpl) setDraining(drain bool) error {
	return ds.ServerConfig.Gossip.AddInfoProto(
		gossip.MakeDistSQLDrainingKey(ds.ServerConfig.NodeID.Get()),
		&distsqlpb.DistSQLDrainingInfo{
			Draining: drain,
		},
		0, // ttl - no expiration
	)
}

// FlowVerIsCompatible checks a flow's version is compatible with this node's
// DistSQL version.
func FlowVerIsCompatible(flowVer, minAcceptedVersion, serverVersion distsqlpb.DistSQLVersion) bool {
	return flowVer >= minAcceptedVersion && flowVer <= serverVersion
}

func newFlow(
	flowCtx runbase.FlowCtx,
	flowReg *flowinfra.FlowRegistry,
	syncFlowConsumer runbase.RowReceiver,
	localProcessors []runbase.LocalProcessor,
	isVectorized bool,
) flowinfra.Flow {
	base := flowinfra.NewFlowBase(flowCtx, flowReg, syncFlowConsumer, localProcessors)
	if isVectorized {
		return vecflow.NewVectorizedFlow(base)
	}
	return rowflow.NewRowBasedFlow(base)
}

// setupFlow creates a Flow.
//
// Args:
// localState: Specifies if the flow runs entirely on this node and, if it does,
//   specifies the txn and other attributes.
//
// Note: unless an error is returned, the returned context contains a span that
// must be finished through Flow.Cleanup.
func (ds *ServerImpl) setupFlow(
	ctx context.Context,
	parentSpan opentracing.Span,
	parentMonitor *mon.BytesMonitor,
	req *distsqlpb.SetupFlowRequest,
	syncFlowConsumer runbase.RowReceiver,
	localState LocalState,
) (context.Context, flowinfra.Flow, error) {
	if !FlowVerIsCompatible(req.Version, MinAcceptedVersion, Version) {
		err := errors.Errorf(
			"version mismatch in flow request: %d; this node accepts %d through %d",
			req.Version, MinAcceptedVersion, Version,
		)
		log.Warning(ctx, err)
		return ctx, nil, err
	}

	//log.Infof(ctx, "setflow %v", localState)

	nodeID := ds.ServerConfig.NodeID.Get()
	if nodeID == 0 {
		return nil, nil, errors.Errorf("setupFlow called before the NodeID was resolved")
	}

	const opName = "flow"
	var sp opentracing.Span
	if parentSpan == nil {
		sp = ds.Tracer.(*tracing.Tracer).StartRootSpan(
			opName, logtags.FromContext(ctx), tracing.NonRecordableSpan)
	} else if localState.IsLocal {
		// If we're a local flow, we don't need a "follows from" relationship: we're
		// going to run this flow synchronously.
		// TODO(andrei): localState.IsLocal is not quite the right thing to use.
		//  If that field is unset, we might still want to create a child span if
		//  this flow is run synchronously.
		sp = tracing.StartChildSpan(opName, parentSpan, logtags.FromContext(ctx), false /* separateRecording */)
	} else {
		// We use FollowsFrom because the flow's span outlives the SetupFlow request.
		// TODO(andrei): We should use something more efficient than StartSpan; we
		// should use AmbientContext.AnnotateCtxWithSpan() but that interface
		// doesn't currently support FollowsFrom relationships.
		sp = ds.Tracer.StartSpan(
			opName,
			opentracing.FollowsFrom(parentSpan.Context()),
			tracing.LogTagsFromCtx(ctx),
		)
	}
	// sp will be Finish()ed by Flow.Cleanup().
	ctx = opentracing.ContextWithSpan(ctx, sp)

	// The monitor opened here are closed in Flow.Cleanup().
	monitor := mon.MakeMonitor(
		"flow",
		mon.MemoryResource,
		ds.Metrics.CurBytesCount,
		ds.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		ds.Settings,
	)
	monitor.Start(ctx, parentMonitor, mon.BoundAccount{})

	makeLeaf := func(req *distsqlpb.SetupFlowRequest) (*client.Txn, error) {
		meta := req.TxnCoordMeta
		if meta == nil {
			// This must be a flow running for some bulk-io operation that doesn't use
			// a txn.
			return nil, nil
		}
		if meta.Txn.Status != roachpb.PENDING {
			return nil, errors.Errorf("cannot create flow in non-PENDING txn: %s",
				meta.Txn)
		}
		// The flow will run in a LeafTxn because we do not want each distributed
		// Txn to heartbeat the transaction.
		txn := client.NewTxnWithCoordMeta(ctx, ds.FlowDB, req.Flow.Gateway, client.LeafTxn, *meta)
		txn.SetIsolationLevel(util.IsolationLevel(req.EvalContext.IsolationLevel))
		return txn, nil
	}

	var evalCtx *tree.EvalContext
	var leafTxn *client.Txn
	if localState.EvalContext != nil {
		evalCtx = localState.EvalContext
		evalCtx.Mon = &monitor
	} else {
		if localState.IsLocal {
			return nil, nil, errors.Errorf(
				"EvalContext expected to be populated when IsLocal is set")
		}

		location, err := timeutil.TimeZoneStringToLocation(
			req.EvalContext.Location,
			timeutil.TimeZoneStringToLocationISO8601Standard,
		)
		if err != nil {
			tracing.FinishSpan(sp)
			return ctx, nil, err
		}

		var be sessiondata.BytesEncodeFormat
		switch req.EvalContext.BytesEncodeFormat {
		case distsqlpb.BytesEncodeFormat_HEX:
			be = sessiondata.BytesEncodeHex
		case distsqlpb.BytesEncodeFormat_ESCAPE:
			be = sessiondata.BytesEncodeEscape
		case distsqlpb.BytesEncodeFormat_BASE64:
			be = sessiondata.BytesEncodeBase64
		default:
			return nil, nil, errors.Errorf("unknown byte encode format: %s",
				req.EvalContext.BytesEncodeFormat.String())
		}
		sd := &sessiondata.SessionData{
			ApplicationName: req.EvalContext.ApplicationName,
			Database:        req.EvalContext.Database,
			User:            req.EvalContext.User,
			SearchPath:      sessiondata.MakeSearchPath(req.EvalContext.SearchPath),
			SequenceState:   sessiondata.NewSequenceState(),
			DataConversion: sessiondata.DataConversionConfig{
				Location:          location,
				BytesEncodeFormat: be,
				ExtraFloatDigits:  int(req.EvalContext.ExtraFloatDigits),
			},
		}
		for i := range req.EvalContext.Ps {
			sd.PC[i].Parallel = req.EvalContext.Ps[i].Parallel
			sd.PC[i].ParallelNum = req.EvalContext.Ps[i].ParallelNum
			sd.PC[i].ValString = req.EvalContext.Ps[i].ValStr
		}
		// Enable better compatibility with PostgreSQL date math.
		if req.Version >= 22 {
			sd.DurationAdditionMode = duration.AdditionModeCompatible
		} else {
			sd.DurationAdditionMode = duration.AdditionModeLegacy
		}
		ie := &lazyInternalExecutor{
			newInternalExecutor: func() tree.SessionBoundInternalExecutor {
				return ds.SessionBoundInternalExecutorFactory(ctx, sd)
			},
		}

		// It's important to populate evalCtx.Txn early. We'll write it again in the
		// f.SetTxn() call below, but by then it will already have been captured by
		// processors.
		leafTxn, err = makeLeaf(req)
		if err != nil {
			return nil, nil, err
		}
		evalCtx = &tree.EvalContext{
			Settings:    ds.ServerConfig.Settings,
			SessionData: sd,
			ClusterID:   ds.ServerConfig.ClusterID.Get(),
			NodeID:      nodeID,
			ReCache:     ds.regexpCache,
			Mon:         &monitor,
			// TODO(andrei): This is wrong. Each processor should override Ctx with its
			// own context.
			Context:          ctx,
			Planner:          &sqlbase.DummyEvalPlanner{},
			SessionAccessor:  &sqlbase.DummySessionAccessor{},
			Sequence:         &sqlbase.DummySequenceOperators{},
			InternalExecutor: ie,
			Txn:              leafTxn,
			ShouldOrdered:    req.EvalContext.ShouldOrdered,
		}
		evalCtx.SetStmtTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.StmtTimestampNanos))
		evalCtx.SetTxnTimestamp(timeutil.Unix(0 /* sec */, req.EvalContext.TxnTimestampNanos))
		var haveSequences bool
		for _, seq := range req.EvalContext.SeqState.Seqs {
			evalCtx.SessionData.SequenceState.RecordValue(seq.SeqID, seq.LatestVal)
			haveSequences = true
		}
		if haveSequences {
			evalCtx.SessionData.SequenceState.SetLastSequenceIncremented(
				*req.EvalContext.SeqState.LastSeqIncremented)
		}
	}
	// TODO(radu): we should sanity check some of these fields.
	flowCtx := runbase.FlowCtx{
		AmbientContext: ds.AmbientContext,
		Cfg:            &ds.ServerConfig,
		Txn:            leafTxn,
		ID:             req.Flow.FlowID,
		EvalCtx:        evalCtx,
		NodeID:         nodeID,
		TraceKV:        req.TraceKV,
		Local:          localState.IsLocal,
		CanParallel:    req.EvalContext.CanParallel,
	}

	// req总是包含所需的向量化模式，不管我们是否
	//非nil localstate。evalcontext。我们不想更新EvalContext
	//当向量子化模式需要改变时，因为我们需要
	//恢复在压力下可能出现数据竞争的原始值。
	isVectorized := sessiondata.VectorizeExecMode(req.EvalContext.Vectorize) != sessiondata.VectorizeOff
	f := newFlow(flowCtx, ds.flowRegistry, syncFlowConsumer, localState.LocalProcs, isVectorized)
	opt := flowinfra.FuseNormally
	if localState.IsLocal {
		// If there's no remote flows, fuse everything. This is needed in order for
		// us to be able to use the RootTxn for the flow 's execution; the RootTxn
		// doesn't allow for concurrent operations. Local flows with mutations need
		// to use the RootTxn.
		opt = flowinfra.FuseAggressively
	}
	if err := f.Setup(ctx, &req.Flow, opt); err != nil {
		log.Errorf(ctx, "error setting up flow: %s", err)
		// Flow.Cleanup will not be called, so we have to close the memory monitor
		// and finish the span manually.
		monitor.Stop(ctx)
		tracing.FinishSpan(sp)
		ctx = opentracing.ContextWithSpan(ctx, nil)
		return ctx, nil, err
	}
	if !f.IsLocal() {
		flowCtx.AddLogTag("f", f.GetFlowCtx().ID.Short())
		flowCtx.AnnotateCtx(ctx)
	}

	// Figure out what txn the flow needs to run in, if any. For gateway flows
	// that have no remote flows and also no concurrency, the txn comes from
	// localState.Txn. Otherwise, we create a txn based on the request's
	// TxnCoordMeta.
	var txn *client.Txn
	//sv := &flowCtx.Cfg.Settings.SV
	var err error
	if localState.IsLocal && !f.ConcurrentExecution() {
		txn = localState.Txn
	} else {
		// If I haven't created the leaf already, do it now.
		if leafTxn == nil {
			leafTxn, err = makeLeaf(req)
			if err != nil {
				return nil, nil, err
			}
		}
		txn = leafTxn
	}
	// TODO(andrei): We're about to overwrite f.EvalCtx.Txn, but the existing
	// field has already been captured by various processors and operators that
	// have already made a copy of the EvalCtx. In case this is not the gateway,
	// we had already set the LeafTxn on the EvalCtx above, so it's OK. In case
	// this is the gateway, if we're running with the RootTxn, then again it was
	// set above so it's fine. If we're using a LeafTxn on the gateway, though,
	// then the processors have erroneously captured the Root. See #41992.
	f.SetTxn(txn)

	return ctx, f, nil
}

// SetupSyncFlow sets up a synchronous flow, connecting the sync response
// output stream to the given RowReceiver. The flow is not started. The flow
// will be associated with the given context.
// Note: the returned context contains a span that must be finished through
// Flow.Cleanup.
func (ds *ServerImpl) SetupSyncFlow(
	ctx context.Context,
	parentMonitor *mon.BytesMonitor,
	req *distsqlpb.SetupFlowRequest,
	output runbase.RowReceiver,
) (context.Context, flowinfra.Flow, error) {
	ctx, f, err := ds.setupFlow(ds.AnnotateCtx(ctx), opentracing.SpanFromContext(ctx), parentMonitor,
		req, output, LocalState{})
	if err != nil {
		return nil, nil, err
	}
	return ctx, f, err
}

// LocalState carries information that is required to set up a flow with wrapped
// planNodes.
type LocalState struct {
	EvalContext *tree.EvalContext

	// IsLocal is true if the flow is being run locally in the first place.
	IsLocal bool

	/////////////////////////////////////////////
	// Fields below are empty if IsLocal == false
	/////////////////////////////////////////////

	// LocalProcs is an array of planNodeToRowSource processors. It's in order and
	// will be indexed into by the RowSourceIdx field in LocalPlanNodeSpec.
	LocalProcs       []runbase.LocalProcessor
	Txn              *client.Txn
	ReplicationTable bool
}

// SetupLocalSyncFlow sets up a synchronous flow on the current (planning) node.
// It's used by the gateway node to set up the flows local to it. Otherwise,
// the same as SetupSyncFlow.
func (ds *ServerImpl) SetupLocalSyncFlow(
	ctx context.Context,
	parentMonitor *mon.BytesMonitor,
	req *distsqlpb.SetupFlowRequest,
	output runbase.RowReceiver,
	localState LocalState,
) (context.Context, flowinfra.Flow, error) {
	ctx, f, err := ds.setupFlow(ctx, opentracing.SpanFromContext(ctx), parentMonitor, req, output,
		localState)
	if err != nil {
		return nil, nil, err
	}
	return ctx, f, err
}

// RunSyncFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) RunSyncFlow(stream distsqlpb.DistSQL_RunSyncFlowServer) error {
	// Set up the outgoing mailbox for the stream.
	mbox := flowinfra.NewOutboxSyncFlowStream(stream)

	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	if firstMsg.SetupFlowRequest == nil {
		return errors.Errorf("first message in RunSyncFlow doesn't contain SetupFlowRequest")
	}
	req := firstMsg.SetupFlowRequest
	ctx, f, err := ds.SetupSyncFlow(stream.Context(), &ds.memMonitor, req, mbox)
	if err != nil {
		return err
	}
	mbox.SetFlowCtx(f.GetFlowCtx())

	if err := ds.Stopper.RunTask(ctx, "distsqlrun.ServerImpl: sync flow", func(ctx context.Context) {
		ctx, ctxCancel := contextutil.WithCancel(ctx)
		defer ctxCancel()
		f.AddStartable(mbox)
		ds.Metrics.FlowStart()
		if err := f.Run(ctx, func() {}); err != nil {
			log.Fatalf(ctx, "unexpected error from syncFlow.Start(): %s "+
				"The error should have gone to the consumer.", err)
		}
		f.Cleanup(ctx, false)
		ds.Metrics.FlowStop()
	}); err != nil {
		return err
	}
	return mbox.Err()
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) SetupFlow(
	ctx context.Context, req *distsqlpb.SetupFlowRequest,
) (*distsqlpb.SimpleResponse, error) {
	log.VEventf(ctx, 1, "received SetupFlow request from n%v for flow %v", req.Flow.Gateway, req.Flow.FlowID)
	parentSpan := opentracing.SpanFromContext(ctx)

	// Note: the passed context will be canceled when this RPC completes, so we
	// can't associate it with the flow.
	ctx = ds.AnnotateCtx(context.Background())
	ctx, f, err := ds.setupFlow(ctx, parentSpan, &ds.memMonitor, req, nil /* syncFlowConsumer */, LocalState{})
	if err == nil {
		err = ds.flowScheduler.ScheduleFlow(ctx, f)
	}
	if err != nil {
		// We return flow deployment errors in the response so that they are
		// packaged correctly over the wire. If we return them directly to this
		// function, they become part of an rpc error.
		return &distsqlpb.SimpleResponse{Error: distsqlpb.NewError(err)}, nil
	}
	return &distsqlpb.SimpleResponse{}, nil
}

func (ds *ServerImpl) flowStreamInt(
	ctx context.Context, stream distsqlpb.DistSQL_FlowStreamServer,
) error {
	// Receive the first message.
	msg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return errors.Errorf("missing header message")
		}
		return err
	}
	if msg.Header == nil {
		return errors.Errorf("no header in first message")
	}
	flowID := msg.Header.FlowID
	streamID := msg.Header.StreamID
	if log.V(1) {
		log.Infof(ctx, "connecting inbound stream %s/%d", flowID.Short(), streamID)
	}
	f, receiver, cleanup, err := ds.flowRegistry.ConnectInboundStream(
		ctx, flowID, streamID, stream, flowinfra.SettingFlowStreamTimeout.Get(&ds.Settings.SV),
	)
	if err != nil {
		return err
	}
	defer cleanup()
	log.VEventf(ctx, 1, "connected inbound stream %s/%d", flowID.Short(), streamID)
	// return ProcessInboundStream(f.AnnotateCtx(ctx), stream, msg, receiver, f)
	return receiver.Run(f.AnnotateCtx(ctx), stream, msg, f)
}

// FlowStream is part of the DistSQLServer interface.
func (ds *ServerImpl) FlowStream(stream distsqlpb.DistSQL_FlowStreamServer) error {
	ctx := ds.AnnotateCtx(stream.Context())
	err := ds.flowStreamInt(ctx, stream)
	if err != nil {
		log.Error(ctx, err)
	}
	return err
}

// TestingKnobs are the testing knobs.
type TestingKnobs struct {
	// RunBeforeBackfillChunk is called before executing each chunk of a
	// backfill during a schema change operation. It is called with the
	// current span and returns an error which eventually is returned to the
	// caller of SchemaChanger.exec(). It is called at the start of the
	// backfill function passed into the transaction executing the chunk.
	RunBeforeBackfillChunk func(sp roachpb.Span) error

	// RunAfterBackfillChunk is called after executing each chunk of a
	// backfill during a schema change operation. It is called just before
	// returning from the backfill function passed into the transaction
	// executing the chunk. It is always called even when the backfill
	// function returns an error, or if the table has already been dropped.
	RunAfterBackfillChunk func()

	// MemoryLimitBytes specifies a maximum amount of working memory that a
	// processor that supports falling back to disk can use. Must be >= 1 to
	// enable. Once this limit is hit, processors employ their on-disk
	// implementation regardless of applicable cluster settings.
	MemoryLimitBytes int64

	// DrainFast, if enabled, causes the server to not wait for any currently
	// running flows to complete or give a grace period of minFlowDrainWait
	// to incoming flows to register.
	DrainFast bool

	// MetadataTestLevel controls whether or not additional metadata test
	// processors are planned, which send additional "RowNum" metadata that is
	// checked by a test receiver on the gateway.
	MetadataTestLevel MetadataTestLevel

	// DeterministicStats overrides stats which don't have reliable values, like
	// stall time and bytes sent. It replaces them with a zero value.
	DeterministicStats bool

	// CDC contains testing knobs specific to the CDC system.
	CDC base.ModuleTestingKnobs

	// EnableVectorizedInvariantsChecker, if enabled, will allow for planning
	// the invariant checkers between all columnar operators.
	EnableVectorizedInvariantsChecker bool
}

// MetadataTestLevel represents the types of queries where metadata test
// processors are planned.
type MetadataTestLevel int

const (
	// Off represents that no metadata test processors are planned.
	Off MetadataTestLevel = iota
	// NoExplain represents that metadata test processors are planned for all
	// queries except EXPLAIN (DISTSQL) statements.

	//NoExplain unused

	// On represents that metadata test processors are planned for all queries.
	On
)

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

// lazyInternalExecutor is a tree.SessionBoundInternalExecutor that initializes
// itself only on the first call to QueryRow.
type lazyInternalExecutor struct {
	// Set when an internal executor has been initialized.
	tree.SessionBoundInternalExecutor

	// Used for initializing the internal executor exactly once.
	once sync.Once

	// newInternalExecutor must be set when instantiating a lazyInternalExecutor,
	// it provides an internal executor to use when necessary.
	newInternalExecutor func() tree.SessionBoundInternalExecutor
}

var _ tree.SessionBoundInternalExecutor = &lazyInternalExecutor{}

func (ie *lazyInternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	ie.once.Do(func() {
		ie.SessionBoundInternalExecutor = ie.newInternalExecutor()
	})
	return ie.SessionBoundInternalExecutor.QueryRow(ctx, opName, txn, stmt, qargs...)
}
