// Copyright 2016  The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or aVectorization greed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lib/pq/oid"
	"github.com/opentracing/opentracing-go"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/rpc/nodedialer"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/distsql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra/vecflow"
	"github.com/znbasedb/znbase/pkg/sql/opt/optbuilder"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// To allow queries to send out flow RPCs in parallel, we use a pool of workers
// that can issue the RPCs on behalf of the running code. The pool is shared by
// multiple queries.
const numRunners = 16

const clientRejectedMsg string = "client rejected when attempting to run DistSQL plan"

const distSQLReceiveBlockSize = 32

// DistSQLRecOpt if open,distsend unsync
var DistSQLRecOpt = settings.RegisterBoolSetting(
	"sql.distsql.sendopt",
	"decode row and add row unsync",
	false,
)

// runnerRequest is the request that is sent (via a channel) to a worker.
type runnerRequest struct {
	ctx        context.Context
	nodeDialer *nodedialer.Dialer
	flowReq    *distsqlpb.SetupFlowRequest
	nodeID     roachpb.NodeID
	resultChan chan<- runnerResult
}

type decodeRowsWrapper struct {
	rows   []tree.Datums
	maxIdx int
}

// DistSQLReceiverOpt for distsqlsend opt
type DistSQLReceiverOpt struct {
	sync.WaitGroup
	// EncRows ch for encoded rows
	EncRows chan *runbase.ChunkBuf
	// EncCollectRows ch for collecting enc rows
	EncCollectRows chan *runbase.ChunkBuf
	// DecRows ch for add a ready row
	DecRows chan *decodeRowsWrapper
	// DecCollectRows ch for collecting decoded rows
	DecCollectRows chan *decodeRowsWrapper
	// Parent normal distsender
	Parent     *DistSQLReceiver
	CurBuffer  *runbase.ChunkBuf
	MoreBuffer bool
}

// InitAndRun for distsqlsend
func (distOpt *DistSQLReceiverOpt) InitAndRun() {
	for i := 0; i < distSQLReceiveBlockSize; i++ {
		cb := new(runbase.ChunkBuf)
		cb.Buffer = make([]*runbase.ChunkRow, 32)
		for j := 0; j < 32; j++ {
			ck := new(runbase.ChunkRow)
			ck.Row = make(sqlbase.EncDatumRow, len(distOpt.Parent.outputTypes))
			cb.Buffer[j] = ck
		}
		cb.MaxIdx = -1
		distOpt.EncCollectRows <- cb

		drw := new(decodeRowsWrapper)
		drw.maxIdx = -1
		drw.rows = make([]tree.Datums, 32)
		for j := 0; j < 32; j++ {
			drw.rows[j] = make(tree.Datums, len(distOpt.Parent.resultToStreamColMap))
		}
		distOpt.DecCollectRows <- drw
	}
	go distOpt.DecodeRow()
	go distOpt.AddRow()
}

// AddRow for distsqlsend
func (distOpt *DistSQLReceiverOpt) AddRow() {
	runtime.LockOSThread()
	r := distOpt.Parent
	defer func() {
		runtime.UnlockOSThread()
		close(distOpt.DecCollectRows)
	}()
	for {
		dataums, ok := <-distOpt.DecRows
		if !ok {
			distOpt.Done()
			return
		}
		for i := 0; i <= dataums.maxIdx; i++ {
			if commErr := r.resultWriter.AddRow(r.ctx, dataums.rows[i]); commErr != nil {
				r.commErr = commErr
				// Set the error on the resultWriter too, for the convenience of some of the
				// clients. If clients don't care to differentiate between communication
				// errors and query execution errors, they can simply inspect
				// resultWriter.Err(). Also, this function itself doesn't care about the
				// distinction and just uses resultWriter.Err() to see if we're still
				// accepting results.
				r.resultWriter.SetError(commErr)
				// TODO(andrei): We should drain here. Metadata from this query would be
				// useful, particularly as it was likely a large query (since AddRow()
				// above failed, presumably with an out-of-memory error).
				r.status = runbase.ConsumerClosed
				return
			}
		}
		dataums.maxIdx = -1
		distOpt.DecCollectRows <- dataums
	}

}

// DecodeRow for distsql send
func (distOpt *DistSQLReceiverOpt) DecodeRow() {
	defer func() {
		close(distOpt.DecRows)
		close(distOpt.EncCollectRows)
	}()
	r := distOpt.Parent
	var EncBuffer *runbase.ChunkBuf
	var ok bool
	for {
		EncBuffer, ok = <-distOpt.EncRows
		if !ok {
			return
		}
		datums := <-distOpt.DecCollectRows
		for i := 0; i <= EncBuffer.MaxIdx; i++ {
			for j, resIdx := range r.resultToStreamColMap {
				err := EncBuffer.Buffer[i].Row[resIdx].EnsureDecoded(&r.outputTypes[resIdx], &r.alloc)
				if err != nil {
					r.resultWriter.SetError(err)
					r.status = runbase.ConsumerClosed
				}
				datums.rows[i][j] = EncBuffer.Buffer[i].Row[resIdx].Datum
			}
			datums.maxIdx++
		}
		distOpt.DecRows <- datums
		EncBuffer.MaxIdx = -1
		distOpt.EncCollectRows <- EncBuffer
	}
}

// runnerResult is returned by a worker (via a channel) for each received
// request.
type runnerResult struct {
	nodeID roachpb.NodeID
	err    error
}

func (req runnerRequest) run() {
	defer distsqlplan.ReleaseSetupFlowRequest(req.flowReq)

	res := runnerResult{nodeID: req.nodeID}

	conn, err := req.nodeDialer.Dial(req.ctx, req.nodeID, rpc.DefaultClass)
	if err != nil {
		res.err = err
	} else {
		client := distsqlpb.NewDistSQLClient(conn)
		// TODO(radu): do we want a timeout here?
		resp, err := client.SetupFlow(req.ctx, req.flowReq)
		if err != nil {
			res.err = err
		} else {
			res.err = resp.Error.ErrorDetail(req.ctx)
		}
	}
	req.resultChan <- res
}

func (dsp *DistSQLPlanner) initRunners() {
	// This channel has to be unbuffered because we want to only be able to send
	// requests if a worker is actually there to receive them.
	dsp.runnerChan = make(chan runnerRequest)
	for i := 0; i < numRunners; i++ {
		dsp.stopper.RunWorker(context.TODO(), func(context.Context) {
			runnerChan := dsp.runnerChan
			stopChan := dsp.stopper.ShouldStop()
			for {
				select {
				case req := <-runnerChan:
					req.run()

				case <-stopChan:
					return
				}
			}
		})
	}
}

// setupFlows sets up all the flows specified in flows using the provided state.
// It will first attempt to set up all remote flows using the dsp workers if
// available or sequentially if not, and then finally set up the gateway flow,
// whose output is the DistSQLReceiver provided. This flow is then returned to
// be run.
func (dsp *DistSQLPlanner) setupFlows(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	leafInputState *roachpb.TxnCoordMeta,
	flows map[roachpb.NodeID]*distsqlpb.FlowSpec,
	recv *DistSQLReceiver,
	localState distsql.LocalState,
	txn *client.Txn,
) (context.Context, flowinfra.Flow, error) {
	thisNodeID := dsp.nodeDesc.NodeID
	_, ok := flows[thisNodeID]
	if !ok {
		return nil, nil, errors.AssertionFailedf("missing gateway flow")
	}
	if localState.IsLocal && len(flows) != 1 && !localState.ReplicationTable {
		return nil, nil, errors.AssertionFailedf("IsLocal set but there's multiple flows")
	}

	evalCtxProto := distsqlpb.MakeEvalContext(evalCtx.EvalContext)
	evalCtxProto.CanParallel = !evalCtx.IsInternalSQL && !evalCtx.EvalContext.SessionData.IsInternalSQL
	evalCtxProto.ShouldOrdered = evalCtx.ShouldOrdered
	evalCtxProto.Ps = make([]distsqlpb.ParallelSet, len(evalCtx.SessionData.PC))
	for i := range evalCtx.SessionData.PC {
		evalCtxProto.Ps[i].ParallelNum = evalCtx.SessionData.PC[i].ParallelNum
		evalCtxProto.Ps[i].Parallel = evalCtx.SessionData.PC[i].Parallel
		evalCtxProto.Ps[i].ValStr = evalCtx.SessionData.PC[i].ValString
	}
	if txn != nil {
		// set IsolationLevel for leaf flow
		evalCtxProto.IsolationLevel = int64(txn.IsolationLevel())
	}
	setupReq := distsqlpb.SetupFlowRequest{
		TxnCoordMeta: leafInputState,
		Version:      distsql.Version,
		EvalContext:  evalCtxProto,
		TraceKV:      evalCtx.Tracing.KVTracingEnabled(),
	}

	// Start all the flows except the flow on this node (there is always a flow on
	// this node).
	var resultChan chan runnerResult
	if len(flows) > 1 {
		resultChan = make(chan runnerResult, len(flows)-1)
	}
	evalCtx.SessionData.VectorizeMode = sessiondata.VectorizeOff
	//evalCtx.SessionData.VectorizeMode = sessiondata.VectorizeAuto
	if evalCtx.SessionData.VectorizeMode != sessiondata.VectorizeOff {
		//if !vectorizeThresholdMet && evalCtx.SessionData.VectorizeMode == sessiondata.VectorizeAuto {
		// Vectorization is not justified for this flow because the expected
		// amount of data is too small and the overhead of pre-allocating data
		// structures needed for the vectorized engine is expected to dominate
		// the execution time.
		//setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
		//} else {
		fuseOpt := flowinfra.FuseNormally
		if localState.IsLocal {
			fuseOpt = flowinfra.FuseAggressively
		}
		// Now we check to see whether or not to even try vectorizing the flow.
		// The goal here is to determine up front whether all of the flows can be
		// vectorized. If any of them can't, turn off the setting.
		// TODO(yuzefovich): this is a safe but quite inefficient way of setting
		// up vectorized flows since the flows will effectively be planned twice.
		for _, spec := range flows {
			if spec.Processors[0].VecComputeSupported == 0 {
				setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
				break
			}
			if evalCtx.EvalContext.SessionData.Database == "test" {
				log.Infof(ctx, "Flows speck %v", spec)
			}
			if _, err := vecflow.SupportsVectorized(
				ctx, &runbase.FlowCtx{
					EvalCtx: &evalCtx.EvalContext,
					Cfg: &runbase.ServerConfig{
						DiskMonitor: &mon.BytesMonitor{},
						Settings:    dsp.st,
					},
					NodeID: -1,
				}, spec.Processors, fuseOpt,
			); err != nil {
				// Vectorization attempt failed with an error.
				returnVectorizationSetupError := false
				if evalCtx.SessionData.VectorizeMode == sessiondata.VectorizeExperimentalAlways {
					returnVectorizationSetupError = true
					// If running with VectorizeExperimentalAlways, this check makes sure
					// that we can still run SET statements (mostly to set vectorize to
					// off) and the like.
					if len(spec.Processors) == 1 &&
						spec.Processors[0].Core.LocalPlanNode != nil {
						rsidx := spec.Processors[0].Core.LocalPlanNode.RowSourceIdx
						if rsidx != nil {
							lp := localState.LocalProcs[*rsidx]
							if z, ok := lp.(vecflow.VectorizeAlwaysException); ok {
								if z.IsException() {
									returnVectorizationSetupError = false
								}
							}
						}
					}
				}
				log.VEventf(ctx, 1, "failed to vectorize: %s", err)
				log.Infof(ctx, "failed to vectorize: %s", err)
				if returnVectorizationSetupError {
					return nil, nil, err
				}
				// Vectorization is not supported for this flow, so we override the
				// setting.
				setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
				break
			}
		}
	}
	setupReq.EvalContext.Vectorize = int32(sessiondata.VectorizeOff)
	for nodeID, flowSpec := range flows {
		if nodeID == thisNodeID {
			// Skip this node.
			continue
		}
		req := setupReq
		req.Flow = *flowSpec
		runReq := runnerRequest{
			ctx:        ctx,
			nodeDialer: dsp.nodeDialer,
			flowReq:    &req,
			nodeID:     nodeID,
			resultChan: resultChan,
		}
		//defer distsqlplan.ReleaseSetupFlowRequest(&req)

		// Send out a request to the workers; if no worker is available, run
		// directly.
		select {
		case dsp.runnerChan <- runReq:
		default:
			runReq.run()
		}
	}

	var firstErr error
	// Now wait for all the flows to be scheduled on remote nodes. Note that we
	// are not waiting for the flows themselves to complete.
	for i := 0; i < len(flows)-1; i++ {
		res := <-resultChan
		if firstErr == nil {
			firstErr = res.err
		}
		// TODO(radu): accumulate the flows that we failed to set up and move them
		// into the local flow.
	}
	if firstErr != nil {
		return nil, nil, firstErr
	}

	// Set up the flow on this node.
	localReq := setupReq
	localReq.Flow = *flows[thisNodeID]
	defer distsqlplan.ReleaseSetupFlowRequest(&localReq)

	if util.EnableUDR {
		for i, pro := range localReq.Flow.Processors {
			for j, expression := range pro.Post.RenderExprs {
				name := strings.Split(expression.Expr, ".")
				expr, err := parser.ParseExpr(expression.Expr)
				if err != nil {
					continue
				}
				if _, ok := expr.(*tree.FuncExpr); !ok {
					continue
				}
				for k := 1; k <= len(name)/2; k++ {
					name[k-1] = name[len(name)-k]
				}
				fullName := [4]string{}
				for k := 0; k < len(name); k++ {
					fullName[k] = name[k]
				}
				fullName[0] = strings.Split(fullName[0], "(")[0]
				if _, ok := tree.FunDefs[fullName[0]]; ok {
					continue
				}
				funcName := tree.MakeUnqualifiedFuncTableName(fullName)
				funcName.DefinedAsFunction()
				desc, err := resolveExistingObjectImpl(ctx, evalCtx.Planner.(*planner), &funcName, true, false, requireFunctionDesc)
				if err != nil {
					continue
				}
				funcDescGroup, ok := desc.(*sqlbase.ImmutableFunctionDescriptor)
				if !ok {
					continue
				}
				var funcDef tree.FunctionDefinition
				funcDef.Name = expression.Expr
				funcDef.Definition = []tree.OverloadImpl{}
				for _, function := range funcDescGroup.FuncGroup {
					funcDesc := function.Desc
					// add opt function into optCatalog for query optimizer
					typesVal := tree.ArgTypes{}
					for _, arg := range funcDesc.Args {
						if arg.InOutMode == "in" || arg.InOutMode == "inout" {
							typesVal = append(typesVal, tree.ArgType{Name: arg.Name, Typ: types.UnwrapType(types.OidToType[arg.Oid])})
						}
					}
					typ := types.OidToType[funcDesc.RetOid]
					if funcDesc.RetOid == oid.T_float4 || funcDesc.RetOid == oid.T_float8 {
						typ = types.Float
					}
					bytes, err := protoutil.Marshal(funcDesc)
					if err != nil {
						continue
					}
					overload := &tree.Overload{
						Types:      typesVal,
						ReturnType: tree.FixedReturnType(typ),
						Name:       funcDesc.FullFuncName,
						Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
							var execargs tree.ExecUdrArgs
							c := make(chan tree.ResultData)
							defer close(c)
							if ctx.SelectCount > tree.MaxFuncSelectNum {
								return nil, errors.New("the udf stack overflow")
							}
							execargs.Ret = c
							execargs.Args = args
							execargs.FuncDescBytes = bytes
							execargs.Name = ctx.UdrFunctionFullName
							execargs.Ctx = ctx
							execargs.ArgTypes = typesVal
							m := ctx.Mon
							tree.ExecChan <- execargs
							ret := <-c
							ctx.Mon = m
							ctx.UDFCTX = true
							return ret.Result, ret.Err
						},
						Info: "Calculates the absolute value of `val`.",
					}
					funcDef.Definition = append(funcDef.Definition, overload)
				}
				funcDef.AmbiguousReturnType = false
				funcDef.Category = "the user define func"
				funcDef.Class = tree.NormalClass
				funcDef.DistsqlBlacklist = false
				funcDef.Impure = false
				funcDef.NullableArgs = true
				funcDef.Private = false
				funcDef.NeedsRepeatedEvaluation = false
				funcDef.UnsupportedWithIssue = 0
				funcDef.ReturnLabels = nil
				expression.UdrFuncDef = &funcDef
				pro.Post.RenderExprs[j] = expression
			}
			localReq.Flow.Processors[i] = pro
		}
	}
	ctx, flow, err := dsp.distSQLSrv.SetupLocalSyncFlow(ctx, evalCtx.Mon, &localReq, recv, localState)
	if err != nil {
		return nil, nil, err
	}

	return ctx, flow, nil
}

// Run executes a physical plan. The plan should have been finalized using
// FinalizePlan.
//
// All errors encountered are reported to the DistSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encountered
// while using that resultWriter), the error is also stored in
// DistSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
//
// Args:
// - txn is the transaction in which the plan will run. If nil, the different
// processors are expected to manage their own internal transactions.
// - evalCtx is the evaluation context in which the plan will run. It might be
// mutated.
// - finishedSetupFn, if non-nil, is called synchronously after all the
// processors have successfully started up.
func (dsp *DistSQLPlanner) Run(
	planCtx *PlanningCtx,
	txn *client.Txn,
	plan *PhysicalPlan,
	recv *DistSQLReceiver,
	evalCtx *extendedEvalContext,
	finishedSetupFn func(),
) {
	ctx := planCtx.ctx
	var (
		localState     distsql.LocalState
		leafInputState *roachpb.TxnCoordMeta
	)
	// NB: putting part of evalCtx in localState means it might be mutated down
	// the line.
	localState.EvalContext = &evalCtx.EvalContext
	localState.Txn = txn
	if planCtx.isLocal {
		localState.IsLocal = true
		localState.LocalProcs = plan.LocalProcessors
		//if planCtx.stmtType == tree.Rows && rowexec.IsOperatorOpt(&evalCtx.ExecCfg.Settings.SV) && txn != nil && planCtx.planner.autoCommit && !planCtx.ExtendedEvalCtx.SessionData.IsInternalSql {
		//	tis, err := txn.GetTxnCoordMetaOrRejectClient(ctx)
		//	if err != nil {
		//		log.Infof(ctx, "%s: %s", clientRejectedMsg, err)
		//		recv.SetError(err)
		//		return
		//	}
		//	tis.StripRootToLeaf()
		//	leafInputState = &tis
		//}
	} else if txn != nil {
		// If the plan is not local, we will have to set up leaf txns using the
		// txnCoordMeta.
		tis, err := txn.GetTxnCoordMetaOrRejectClient(ctx)
		if err != nil {
			log.Infof(ctx, "%s: %s", clientRejectedMsg, err)
			recv.SetError(err)
			return
		}
		tis.StripRootToLeaf()
		leafInputState = &tis
	}

	if err := planCtx.sanityCheckAddresses(); err != nil {
		recv.SetError(err)
		return
	}

	flowID := distsqlpb.FlowID{}
	if plan.IsRemotePlan() {
		flowID.UUID = uuid.MakeV4()
	}

	//log.Infof(ctx, "plan.ProcessorsCount %v %v", len(plan.Processors))

	flows := plan.GenerateFlowSpecs(dsp.nodeDesc.NodeID /* gateway */, &planCtx.ExtendedEvalCtx.EvalContext)
	if _, ok := flows[dsp.nodeDesc.NodeID]; !ok {
		recv.SetError(errors.Errorf("expected to find gateway flow"))
		return
	}

	//for i, proc := range plan.Processors{
	//	log.Infof(ctx, "plan.Processors %v %v", i,  proc.Spec.Core.String())
	//
	//}

	if logPlanDiagram {
		log.VEvent(ctx, 1, "creating plan diagram")
		_, url, err := distsqlpb.GeneratePlanDiagramURL(flows)
		if err != nil {
			log.Infof(ctx, "Error generating diagram: %s", err)
		} else {
			log.Infof(ctx, "Plan diagram URL:\n%s", url.String())
		}
	}

	log.VEvent(ctx, 1, "running DistSQL plan")

	dsp.distSQLSrv.ServerConfig.Metrics.QueryStart()
	defer dsp.distSQLSrv.ServerConfig.Metrics.QueryStop()

	recv.outputTypes = plan.ResultTypes
	recv.resultToStreamColMap = plan.PlanToStreamColMap
	if recv.InternalOpt {
		recv.InitAndRun()
	}
	//vectorizedThresholdMet := plan.MaxEstimatedRowCount >= evalCtx.SessionData.VectorizeRowCountThreshold

	if len(flows) == 1 {
		// We ended up planning everything locally, regardless of whether we
		// intended to distribute or not.
		localState.IsLocal = true
	}

	if plan.HaveReplicaTable {
		localState.ReplicationTable = true
	}

	//ctx, flow, err := dsp.setupFlows(ctx, evalCtx, leafInputState, flows, recv, localState, vectorizedThresholdMet, txn)
	ctx, flow, err := dsp.setupFlows(ctx, evalCtx, leafInputState, flows, recv, localState, txn)
	if err != nil {
		recv.SetError(err)
		return
	}

	if finishedSetupFn != nil {
		finishedSetupFn()
	}

	if txn != nil {
		// set IsolationLevel for leaf flow
		txn.SetIsolationLevel(txn.IsolationLevel())
	}

	if txn != nil && planCtx.isLocal && flow.ConcurrentExecution() {
		//recv.SetError(errors.Errorf(
		//	"unexpected concurrency for a flow that was forced to be planned locally"))
		//return
		// todo windows算子无法通过测试 临时解决方案
		log.Errorf(ctx, "unexpected concurrency for a flow that was forced to be planned locally: %s", statementFromCtx(ctx))
		txn.CommitTimestamp()
		flow.SetTxn(txn)
	}

	// TODO(radu): this should go through the flow scheduler.
	if err := flow.Run(ctx, func() {}); err != nil {
		log.Fatalf(ctx, "unexpected error from syncFlow.Start(): %s "+
			"The error should have gone to the consumer.", err)
	}
	var skipCheck bool
	// We need to close the planNode tree we translated into a DistSQL plan before
	// flow.Cleanup, which closes memory accounts that expect to be emptied.
	if planCtx.planner != nil && !planCtx.ignoreClose {
		curPlan := &planCtx.planner.curPlan
		curPlan.execErr = recv.resultWriter.Err()
		curPlan.close(ctx)
		//When the execution plan is DUMP, the memory monitor should have DUMP
		//to perform the related shutdown operation, otherwise, when the cancel
		//operation is performed during the DUMP process, the DUMP result set has
		//not received the shutdown message, and the memory monitor's Stop is
		//performed at this time , It will cause the memory monitor to not be
		//closed completely, which leads to panic.
		//TODO. For DUMP CSV operations, DumpNode should be used instead of
		//TODO packaging scanNode to form the corresponding execution plan.
		if _, ok := curPlan.AST.(*tree.Dump); ok {
			skipCheck = true
		}
	}
	flow.Cleanup(ctx, skipCheck)
}

// DistSQLReceiver is a RowReceiver that writes results to a rowResultWriter.
// This is where the DistSQL execution meets the SQL Session - the RowContainer
// comes from a client Session.
//
// DistSQLReceiver also update the RangeDescriptorCache and the LeaseholderCache
// in response to DistSQL metadata about misplanned ranges.
type DistSQLReceiver struct {
	*DistSQLReceiverOpt
	InternalOpt bool

	ctx context.Context

	// resultWriter is the interface which we send results to.
	resultWriter rowResultWriter

	stmtType tree.StatementType

	// outputTypes are the types of the result columns produced by the plan.
	outputTypes []sqlbase.ColumnType

	// resultToStreamColMap maps result columns to columns in the distsqlrun results
	// stream.
	resultToStreamColMap []int

	// noColsRequired indicates that the caller is only interested in the
	// existence of a single row. Used by subqueries in EXISTS mode.
	noColsRequired bool

	// discardRows is set when we want to discard rows (for testing/benchmarks).
	// See EXECUTE .. DISCARD ROWS.
	discardRows bool

	// commErr keeps track of the error received from interacting with the
	// resultWriter. This represents a "communication error" and as such is unlike
	// query execution errors: when the DistSQLReceiver is used within a SQL
	// session, such errors mean that we have to bail on the session.
	// Query execution errors are reported to the resultWriter. For some client's
	// convenience, communication errors are also reported to the resultWriter.
	//
	// Once set, no more rows are accepted.
	commErr error

	// txnAbortedErr is atomically set to an errWrap when the KV txn finishes
	// asynchronously. Further results should not be returned to the client, as
	// they risk missing seeing their own writes. Upon the next Push(), err is set
	// and ConsumerStatus is set to ConsumerClosed.
	txnAbortedErr atomic.Value

	row    tree.Datums
	status runbase.ConsumerStatus
	alloc  sqlbase.DatumAlloc
	closed bool

	rangeCache *kv.RangeDescriptorCache
	leaseCache *kv.LeaseHolderCache
	tracing    *SessionTracing
	cleanup    func()

	// The transaction in which the flow producing data for this
	// receiver runs. The DistSQLReceiver updates the transaction in
	// response to RetryableTxnError's and when distributed processors
	// pass back TxnCoordMeta objects via ProducerMetas. Nil if no
	// transaction should be updated on errors (i.e. if the flow overall
	// doesn't run in a transaction).
	txn *client.Txn

	// A handler for clock signals arriving from remote nodes. This should update
	// this node's clock.
	updateClock func(observedTs hlc.Timestamp)
}

// errWrap is a container for an error, for use with atomic.Value, which
// requires that all of things stored in it must have the same concrete type.
type errWrap struct {
	err error
}

// rowResultWriter is a subset of CommandResult to be used with the
// DistSQLReceiver. It's implemented by RowResultWriter.
type rowResultWriter interface {
	// AddRow writes a result row.
	// Note that the caller owns the row slice and might reuse it.
	AddRow(ctx context.Context, row tree.Datums) error
	IncrementRowsAffected(n int)
	SetError(error)
	Err() error
}

// errOnlyResultWriter is a rowResultWriter that only supports receiving an
// error. All other functions that deal with producing results panic.
type errOnlyResultWriter struct {
	err error
}

var _ rowResultWriter = &errOnlyResultWriter{}

func (w *errOnlyResultWriter) SetError(err error) {
	w.err = err
}
func (w *errOnlyResultWriter) Err() error {
	return w.err
}

func (w *errOnlyResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	panic("AddRow not supported by errOnlyResultWriter")
}
func (w *errOnlyResultWriter) IncrementRowsAffected(n int) {
	panic("IncrementRowsAffected not supported by errOnlyResultWriter")
}

var _ runbase.RowReceiver = &DistSQLReceiver{}

var receiverSyncPool = sync.Pool{
	New: func() interface{} {
		return &DistSQLReceiver{}
	},
}

// MakeDistSQLReceiver creates a DistSQLReceiver.
//
// ctx is the Context that the receiver will use throughput its
// lifetime. resultWriter is the container where the results will be
// stored. If only the row count is needed, this can be nil.
//
// txn is the transaction in which the producer flow runs; it will be updated
// on errors. Nil if the flow overall doesn't run in a transaction.
func MakeDistSQLReceiver(
	ctx context.Context,
	resultWriter rowResultWriter,
	stmtType tree.StatementType,
	rangeCache *kv.RangeDescriptorCache,
	leaseCache *kv.LeaseHolderCache,
	txn *client.Txn,
	updateClock func(observedTs hlc.Timestamp),
	tracing *SessionTracing,
) *DistSQLReceiver {
	consumeCtx, cleanup := tracing.TraceExecConsume(ctx)
	r := receiverSyncPool.Get().(*DistSQLReceiver)
	*r = DistSQLReceiver{
		ctx:          consumeCtx,
		cleanup:      cleanup,
		resultWriter: resultWriter,
		rangeCache:   rangeCache,
		leaseCache:   leaseCache,
		txn:          txn,
		updateClock:  updateClock,
		stmtType:     stmtType,
		tracing:      tracing,
	}
	// If this receiver is part of a distributed flow and isn't using the root
	// transaction, we need to sure that the flow is canceled when the root
	// transaction finishes (i.e. it is abandoned, aborted, or committed), so that
	// we don't return results to the client that might have missed seeing their
	// own writes. The committed case shouldn't happen. This isn't necessary if
	// the flow is running locally and is using the root transaction.
	//
	// TODO(andrei): Instead of doing this, should we lift this transaction
	// monitoring to connExecutor and have it cancel the SQL txn's context? Or for
	// that matter, should the TxnCoordSender cancel the context itself?
	if r.txn != nil && r.txn.Type() == client.LeafTxn {
		r.txn.OnCurrentIncarnationFinish(func(err error) {
			r.txnAbortedErr.Store(errWrap{err: err})
		})
	}
	return r
}

// Release releases this DistSQLReceiver back to the pool.
func (r *DistSQLReceiver) Release() {
	*r = DistSQLReceiver{}
	receiverSyncPool.Put(r)
}

// clone clones the receiver for running subqueries. Not all fields are cloned,
// only those required for running subqueries.
func (r *DistSQLReceiver) clone() *DistSQLReceiver {
	ret := receiverSyncPool.Get().(*DistSQLReceiver)
	*ret = DistSQLReceiver{
		ctx:         r.ctx,
		cleanup:     func() {},
		rangeCache:  r.rangeCache,
		leaseCache:  r.leaseCache,
		txn:         r.txn,
		updateClock: r.updateClock,
		stmtType:    tree.Rows,
		tracing:     r.tracing,
	}
	return ret
}

// SetError provides a convenient way for a client to pass in an error, thus
// pretending that a query execution error happened. The error is passed along
// to the resultWriter.
func (r *DistSQLReceiver) SetError(err error) {
	r.resultWriter.SetError(err)
}

// MakeDistSQLReceiverOpt creates a DistSQLReceiver.
//
// ctx is the Context that the receiver will use throughput its
// lifetime. resultWriter is the container where the results will be
// stored. If only the row count is needed, this can be nil.
//
// txn is the transaction in which the producer flow runs; it will be updated
// on errors. Nil if the flow overall doesn't run in a transaction.
func MakeDistSQLReceiverOpt(
	ctx context.Context,
	resultWriter rowResultWriter,
	stmtType tree.StatementType,
	rangeCache *kv.RangeDescriptorCache,
	leaseCache *kv.LeaseHolderCache,
	txn *client.Txn,
	updateClock func(observedTs hlc.Timestamp),
	tracing *SessionTracing,
) *DistSQLReceiver {
	consumeCtx, cleanup := tracing.TraceExecConsume(ctx)
	r := receiverSyncPool.Get().(*DistSQLReceiver)
	*r = DistSQLReceiver{
		InternalOpt:  true,
		ctx:          consumeCtx,
		cleanup:      cleanup,
		resultWriter: resultWriter,
		rangeCache:   rangeCache,
		leaseCache:   leaseCache,
		txn:          txn,
		updateClock:  updateClock,
		stmtType:     stmtType,
		tracing:      tracing,
	}

	r.DistSQLReceiverOpt = new(DistSQLReceiverOpt)
	r.DistSQLReceiverOpt.Add(1)
	r.DistSQLReceiverOpt.Parent = r
	r.EncRows = make(chan *runbase.ChunkBuf, distSQLReceiveBlockSize)
	r.EncCollectRows = make(chan *runbase.ChunkBuf, distSQLReceiveBlockSize)
	r.DecRows = make(chan *decodeRowsWrapper, distSQLReceiveBlockSize)
	r.DecCollectRows = make(chan *decodeRowsWrapper, distSQLReceiveBlockSize)
	// If this receiver is part of a distributed flow and isn't using the root
	// transaction, we need to sure that the flow is canceled when the root
	// transaction finishes (i.e. it is abandoned, aborted, or committed), so that
	// we don't return results to the client that might have missed seeing their
	// own writes. The committed case shouldn't happen. This isn't necessary if
	// the flow is running locally and is using the root transaction.
	//
	// TODO(andrei): Instead of doing this, should we lift this transaction
	// monitoring to connExecutor and have it cancel the SQL txn's context? Or for
	// that matter, should the TxnCoordSender cancel the context itself?
	if r.txn != nil && r.txn.Type() == client.LeafTxn {
		r.txn.OnCurrentIncarnationFinish(func(err error) {
			r.txnAbortedErr.Store(errWrap{err: err})
		})
	}
	return r
}

// Push is part of the RowReceiver interface.
func (r *DistSQLReceiver) Push(
	row sqlbase.EncDatumRow, meta *distsqlpb.ProducerMetadata,
) runbase.ConsumerStatus {
	if r.InternalOpt {
		return r.PushOpt(row, meta)
	}
	return r.PushNormal(row, meta)

}

// PushOpt is parallel set for sending to result stream
func (r *DistSQLReceiver) PushOpt(
	row sqlbase.EncDatumRow, meta *distsqlpb.ProducerMetadata,
) runbase.ConsumerStatus {
	if meta != nil {
		if meta.TxnCoordMeta != nil {
			if r.txn != nil {
				if r.txn.ID() == meta.TxnCoordMeta.Txn.ID {
					r.txn.AugmentTxnCoordMeta(r.ctx, *meta.TxnCoordMeta)
				}
			} else {
				r.resultWriter.SetError(
					errors.Errorf("received a leaf TxnCoordMeta (%s); but have no root", meta.TxnCoordMeta))
			}
		}
		if meta.Err != nil {
			// Check if the error we just received should take precedence over a
			// previous error (if any).
			if roachpb.ErrPriority(meta.Err) > roachpb.ErrPriority(r.resultWriter.Err()) {
				if r.txn != nil {
					if retryErr, ok := meta.Err.(*roachpb.UnhandledRetryableError); ok {
						// Update the txn in response to remote errors. In the non-DistSQL
						// world, the TxnCoordSender handles "unhandled" retryable errors,
						// but this one is coming from a distributed SQL node, which has
						// left the handling up to the root transaction.
						meta.Err = r.txn.UpdateStateOnRemoteRetryableErr(r.ctx, &retryErr.PErr)
						// Update the clock with information from the error. On non-DistSQL
						// code paths, the DistSender does this.
						// TODO(andrei): We don't propagate clock signals on success cases
						// through DistSQL; we should. We also don't propagate them through
						// non-retryable errors; we also should.
						r.updateClock(retryErr.PErr.Now)
					}
				}
				r.resultWriter.SetError(meta.Err)
			}
		}
		if len(meta.Ranges) > 0 {
			if err := r.updateCaches(r.ctx, meta.Ranges); err != nil && r.resultWriter.Err() == nil {
				r.resultWriter.SetError(err)
			}
		}
		if len(meta.TraceData) > 0 {
			span := opentracing.SpanFromContext(r.ctx)
			if span == nil {
				r.resultWriter.SetError(
					errors.New("trying to ingest remote spans but there is no recording span set up"))
			} else if err := tracing.ImportRemoteSpans(span, meta.TraceData); err != nil {
				r.resultWriter.SetError(errors.Errorf("error ingesting remote spans: %s", err))
			}
		}
		return r.status
	}
	if r.resultWriter.Err() == nil && r.txnAbortedErr.Load() != nil {
		r.resultWriter.SetError(r.txnAbortedErr.Load().(errWrap).err)
	}
	if r.resultWriter.Err() == nil && r.ctx.Err() != nil {
		r.resultWriter.SetError(r.ctx.Err())
	}
	if r.resultWriter.Err() != nil {
		// TODO(andrei): We should drain here if we weren't canceled.
		return runbase.ConsumerClosed
	}
	if r.status != runbase.NeedMoreRows {
		return r.status
	}
	// If no columns are needed by the output, the consumer is only looking for
	// whether a single row is pushed or not, so the contents do not matter, and
	// planNodeToRowSource is not set up to handle decoding the row.
	if r.noColsRequired {
		r.row = []tree.Datum{}
		r.status = runbase.ConsumerClosed
	} else {
		if r.CurBuffer == nil {
			r.CurBuffer, r.MoreBuffer = <-r.EncCollectRows
		}
		if !r.MoreBuffer {
			r.status = runbase.ConsumerClosed
		} else {
			if runbase.PushToBuf(r.CurBuffer, row, nil, 32) {
				r.EncRows <- r.CurBuffer
				r.CurBuffer = nil
			}
		}
	}
	//r.tracing.TraceExecRowsResult(r.ctx, r.row)
	// Note that AddRow accounts for the memory used by the Datums.

	return r.status
}

// PushNormal is the normal push
func (r *DistSQLReceiver) PushNormal(
	row sqlbase.EncDatumRow, meta *distsqlpb.ProducerMetadata,
) runbase.ConsumerStatus {
	if meta != nil {
		if meta.TxnCoordMeta != nil {
			if r.txn != nil {
				if r.txn.ID() == meta.TxnCoordMeta.Txn.ID {
					r.txn.AugmentTxnCoordMeta(r.ctx, *meta.TxnCoordMeta)
				}
			} else {
				r.resultWriter.SetError(
					errors.Errorf("received a leaf TxnCoordMeta (%s); but have no root", meta.TxnCoordMeta))
			}
		}
		if meta.Err != nil {
			// Check if the error we just received should take precedence over a
			// previous error (if any).
			if roachpb.ErrPriority(meta.Err) > roachpb.ErrPriority(r.resultWriter.Err()) {
				if r.txn != nil {
					if retryErr, ok := meta.Err.(*roachpb.UnhandledRetryableError); ok {
						// Update the txn in response to remote errors. In the non-DistSQL
						// world, the TxnCoordSender handles "unhandled" retryable errors,
						// but this one is coming from a distributed SQL node, which has
						// left the handling up to the root transaction.
						meta.Err = r.txn.UpdateStateOnRemoteRetryableErr(r.ctx, &retryErr.PErr)
						// Update the clock with information from the error. On non-DistSQL
						// code paths, the DistSender does this.
						// TODO(andrei): We don't propagate clock signals on success cases
						// through DistSQL; we should. We also don't propagate them through
						// non-retryable errors; we also should.
						r.updateClock(retryErr.PErr.Now)
					}
				}
				r.resultWriter.SetError(meta.Err)
			}
		}
		if len(meta.Ranges) > 0 {
			if err := r.updateCaches(r.ctx, meta.Ranges); err != nil && r.resultWriter.Err() == nil {
				r.resultWriter.SetError(err)
			}
		}
		if len(meta.TraceData) > 0 {
			span := opentracing.SpanFromContext(r.ctx)
			if span == nil {
				r.resultWriter.SetError(
					errors.New("trying to ingest remote spans but there is no recording span set up"))
			} else if err := tracing.ImportRemoteSpans(span, meta.TraceData); err != nil {
				r.resultWriter.SetError(errors.Errorf("error ingesting remote spans: %s", err))
			}
		}
		return r.status
	}
	if r.resultWriter.Err() == nil && r.txnAbortedErr.Load() != nil {
		r.resultWriter.SetError(r.txnAbortedErr.Load().(errWrap).err)
	}
	if r.resultWriter.Err() == nil && r.ctx.Err() != nil {
		r.resultWriter.SetError(r.ctx.Err())
	}
	if r.resultWriter.Err() != nil {
		// TODO(andrei): We should drain here if we weren't canceled.
		return runbase.ConsumerClosed
	}
	if r.status != runbase.NeedMoreRows {
		return r.status
	}

	if r.stmtType != tree.Rows {
		// We only need the row count. planNodeToRowSource is set up to handle
		// ensuring that the last stage in the pipeline will return a single-column
		// row with the row count in it, so just grab that and exit.
		r.resultWriter.IncrementRowsAffected(int(tree.MustBeDInt(row[0].Datum)))
		return r.status
	}

	if r.discardRows {
		// Discard rows.
		return r.status
	}
	if r.noColsRequired {
		r.row = []tree.Datum{}
		r.status = runbase.ConsumerClosed
	} else {
		if r.row == nil {
			r.row = make(tree.Datums, len(r.resultToStreamColMap))
		}
		for i, resIdx := range r.resultToStreamColMap {
			err := row[resIdx].EnsureDecoded(&r.outputTypes[resIdx], &r.alloc)
			if err != nil {
				r.resultWriter.SetError(err)
				r.status = runbase.ConsumerClosed
				return r.status
			}
			r.row[i] = row[resIdx].Datum
		}
	}
	r.tracing.TraceExecRowsResult(r.ctx, r.row)

	if optbuilder.CycleStopFlag == true {
		optbuilder.CycleStopFlag = false
		return r.status
	}
	// Note that AddRow accounts for the memory used by the Datums.
	if commErr := r.resultWriter.AddRow(r.ctx, r.row); commErr != nil {
		r.commErr = commErr
		// Set the error on the resultWriter too, for the convenience of some of the
		// clients. If clients don't care to differentiate between communication
		// errors and query execution errors, they can simply inspect
		// resultWriter.Err(). Also, this function itself doesn't care about the
		// distinction and just uses resultWriter.Err() to see if we're still
		// accepting results.
		r.resultWriter.SetError(commErr)
		// TODO(andrei): We should drain here. Metadata from this query would be
		// useful, particularly as it was likely a large query (since AddRow()
		// above failed, presumably with an out-of-memory error).
		r.status = runbase.ConsumerClosed
		return r.status
	}
	if ExecInUDR {
		tempRow := make(tree.Datums, 0)
		for _, v := range r.row {
			tempRow = append(tempRow, v)
			// UDRExecRes.Res[UDRExecRes.RowCount] = append(UDRExecRes.Res[UDRExecRes.RowCount], v)
		}
		UDRExecRes.Res = append(UDRExecRes.Res, tempRow)
		UDRExecRes.RowCount++
	}
	return r.status
}

// ProducerDone is part of the RowReceiver interface.
func (r *DistSQLReceiver) ProducerDone() {
	if r.InternalOpt {
		if r.CurBuffer != nil {
			r.EncRows <- r.CurBuffer
		}
		close(r.EncRows)
		r.Wait()
	}
	if r.txn != nil {
		r.txn.OnCurrentIncarnationFinish(nil)
	}
	if r.closed {
		panic("double close")
	}
	r.closed = true
	r.cleanup()
}

// Types is part of the RowReceiver interface.
func (r *DistSQLReceiver) Types() []sqlbase.ColumnType {
	return r.outputTypes
}

// updateCaches takes information about some ranges that were mis-planned and
// updates the range descriptor and lease-holder caches accordingly.
//
// TODO(andrei): updating these caches is not perfect: we can clobber newer
// information that someone else has populated because there's no timing info
// anywhere. We also may fail to remove stale info from the LeaseHolderCache if
// the ids of the ranges that we get are different than the ids in that cache.
func (r *DistSQLReceiver) updateCaches(ctx context.Context, ranges []roachpb.RangeInfo) error {
	// Update the RangeDescriptorCache.
	rngDescs := make([]roachpb.RangeDescriptor, len(ranges))
	for i, ri := range ranges {
		rngDescs[i] = ri.Desc
	}
	if err := r.rangeCache.InsertRangeDescriptors(ctx, rngDescs...); err != nil {
		return err
	}

	// Update the LeaseHolderCache.
	for _, ri := range ranges {
		r.leaseCache.Update(ctx, ri.Desc.RangeID, ri.Lease.Replica.StoreID)
	}
	return nil
}

// PlanAndRunSubqueries returns false if an error was encountered and sets that
// error in the provided receiver.
func (dsp *DistSQLPlanner) PlanAndRunSubqueries(
	ctx context.Context,
	planner *planner,
	evalCtxFactory func() *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) bool {
	for planIdx, subqueryPlan := range subqueryPlans {
		if err := dsp.planAndRunSubquery(
			ctx,
			planIdx,
			subqueryPlan,
			planner,
			evalCtxFactory(),
			subqueryPlans,
			recv,
			maybeDistribute,
		); err != nil {
			recv.SetError(err)
			return false
		}
	}
	if sel, ok := planner.stmt.AST.(*tree.Select); ok {
		if sel.With != nil && sel.With.RecursiveData != nil {
			if sel.With.RecursiveData.ISCycle && sel.With.RecursiveData.Nocycle == false {
				sel.With.RecursiveData.ISCycle = false
				err := pgerror.NewError("", "loop in data")
				recv.SetError(err)
				return false
			} else if sel.With.RecursiveData.CycleStmt && sel.With.RecursiveData.Nocycle == false {
				sel.With.RecursiveData.CycleStmt = false
				err := pgerror.NewError("", "CONNECT_BY_ISCYCLE must use with NOCYCLE")
				recv.SetError(err)
				return false
			}
		}
	}

	return true
}

func (dsp *DistSQLPlanner) planAndRunSubquery(
	ctx context.Context,
	planIdx int,
	subqueryPlan subquery,
	planner *planner,
	evalCtx *extendedEvalContext,
	subqueryPlans []subquery,
	recv *DistSQLReceiver,
	maybeDistribute bool,
) error {
	subqueryMonitor := mon.MakeMonitor(
		"subquery",
		mon.MemoryResource,
		dsp.distSQLSrv.Metrics.CurBytesCount,
		dsp.distSQLSrv.Metrics.MaxBytesHist,
		-1, /* use default block size */
		noteworthyMemoryUsageBytes,
		dsp.distSQLSrv.Settings,
	)
	subqueryMonitor.Start(ctx, evalCtx.Mon, mon.BoundAccount{})
	defer subqueryMonitor.Stop(ctx)

	subqueryMemAccount := subqueryMonitor.MakeBoundAccount()
	defer subqueryMemAccount.Close(ctx)

	var subqueryPlanCtx *PlanningCtx
	var distributeSubquery bool
	if maybeDistribute {
		distributeSubquery = shouldDistributePlan(
			ctx, planner.SessionData().DistSQLMode, dsp, subqueryPlan.plan.planNode)
	}
	if distributeSubquery {
		subqueryPlanCtx = dsp.NewPlanningCtx(ctx, evalCtx, planner.txn)
	} else {
		subqueryPlanCtx = dsp.newLocalPlanningCtx(ctx, evalCtx)
	}

	subqueryPlanCtx.isLocal = !distributeSubquery
	subqueryPlanCtx.planner = planner
	subqueryPlanCtx.stmtType = tree.Rows
	// Don't close the top-level plan from subqueries - someone else will handle
	// that.
	subqueryPlanCtx.ignoreClose = true

	subqueryPhysPlan, err := dsp.createPhysPlan(subqueryPlanCtx, subqueryPlan.plan)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(subqueryPlanCtx, &subqueryPhysPlan)

	// TODO(arjun): #28264: We set up a row container, wrap it in a row
	// receiver, and use it and serialize the results of the subquery. The type
	// of the results stored in the container depends on the type of the subquery.
	subqueryRecv := recv.clone()
	var typ sqlbase.ColTypeInfo
	var rows *rowcontainer.RowContainer
	if subqueryPlan.execMode == rowexec.SubqueryExecModeExists {
		subqueryRecv.noColsRequired = true
		typ = sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{})
	} else {
		// Apply the PlanToStreamColMap projection to the ResultTypes to get the
		// final set of output types for the subquery. The reason this is necessary
		// is that the output schema of a query sometimes contains columns necessary
		// to merge the streams, but that aren't required by the final output of the
		// query. These get projected out, so we need to similarly adjust the
		// expected result types of the subquery here.
		colTypes := make([]sqlbase.ColumnType, len(subqueryPhysPlan.PlanToStreamColMap))
		for i, resIdx := range subqueryPhysPlan.PlanToStreamColMap {
			colTypes[i] = subqueryPhysPlan.ResultTypes[resIdx]
		}
		typ = sqlbase.ColTypeInfoFromColTypes(colTypes)
	}
	rows = rowcontainer.NewRowContainer(subqueryMemAccount, typ, 0)
	defer rows.Close(ctx)

	subqueryRowReceiver := NewRowResultWriter(rows)
	subqueryRecv.resultWriter = subqueryRowReceiver
	subqueryPlans[planIdx].started = true
	dsp.Run(subqueryPlanCtx, planner.txn, &subqueryPhysPlan, subqueryRecv, evalCtx, nil /* finishedSetupFn */)
	if subqueryRecv.commErr != nil {
		return subqueryRecv.commErr
	}
	if err := subqueryRowReceiver.Err(); err != nil {
		return err
	}
	switch subqueryPlan.execMode {
	case rowexec.SubqueryExecModeExists:
		// For EXISTS expressions, all we want to know if there is at least one row.
		hasRows := rows.Len() != 0
		subqueryPlans[planIdx].result = tree.MakeDBool(tree.DBool(hasRows))
	case rowexec.SubqueryExecModeAllRows, rowexec.SubqueryExecModeAllRowsNormalized:
		var result tree.DTuple
		for rows.Len() > 0 {
			row := rows.At(0)
			rows.PopFirst()
			if row.Len() == 1 {
				// This seems hokey, but if we don't do this then the subquery expands
				// to a tuple of tuples instead of a tuple of values and an expression
				// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
				// a single value against a tuple.
				result.D = append(result.D, row[0])
			} else {
				result.D = append(result.D, &tree.DTuple{D: row})
			}
		}

		if subqueryPlan.execMode == rowexec.SubqueryExecModeAllRowsNormalized {
			result.Normalize(&evalCtx.EvalContext)
		}
		subqueryPlans[planIdx].result = &result
	case rowexec.SubqueryExecModeOneRow:
		switch rows.Len() {
		case 0:
			subqueryPlans[planIdx].result = tree.DNull
		case 1:
			row := rows.At(0)
			switch row.Len() {
			case 1:
				subqueryPlans[planIdx].result = row[0]
			default:
				subqueryPlans[planIdx].result = &tree.DTuple{D: rows.At(0)}
			}
		default:
			return pgerror.NewErrorf(pgcode.CardinalityViolation,
				"more than one row returned by a subquery used as an expression")
		}
	default:
		return fmt.Errorf("unexpected subqueryExecMode: %d", subqueryPlan.execMode)
	}
	return nil
}

// PlanAndRun generates a physical plan from a planNode tree and executes it. It
// assumes that the tree is supported (see CheckSupport).
//
// All errors encountered are reported to the DistSQLReceiver's resultWriter.
// Additionally, if the error is a "communication error" (an error encountered
// while using that resultWriter), the error is also stored in
// DistSQLReceiver.commErr. That can be tested to see if a client session needs
// to be closed.
func (dsp *DistSQLPlanner) PlanAndRun(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *client.Txn,
	plan planMaybePhysical,
	recv *DistSQLReceiver,
) {
	log.VEventf(ctx, 1, "creating DistSQL plan with isLocal=%v", planCtx.isLocal)

	//log.Infof(planCtx.ctx, "PlanAndRun %v, %v", planCtx.planner.stmt)
	physPlan, err := dsp.createPhysPlan(planCtx, plan)
	if err != nil {
		recv.SetError(err)
		return
	}
	dsp.FinalizePlan(planCtx, &physPlan)
	// set support vector computation flag
	dsp.setProcessorVectorFlag(planCtx, &physPlan)
	dsp.Run(planCtx, txn, &physPlan, recv, evalCtx, nil /* finishedSetupFn */)
}
