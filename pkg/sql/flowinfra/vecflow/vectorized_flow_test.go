// Copyright 2019  The Cockroach Authors.

package vecflow

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra/vecflow/vecrpc"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

type callbackRemoteComponentCreator struct {
	newOutboxFn func(*vecexec.Allocator, vecexec.Operator, []coltypes.T, []distsqlpb.MetadataSource) (*vecrpc.Outbox, error)
	newInboxFn  func(allocator *vecexec.Allocator, typs []coltypes.T, streamID distsqlpb.StreamID) (*vecrpc.Inbox, error)
}

func (c callbackRemoteComponentCreator) newOutbox(
	allocator *vecexec.Allocator,
	input vecexec.Operator,
	typs []coltypes.T,
	metadataSources []distsqlpb.MetadataSource,
) (*vecrpc.Outbox, error) {
	return c.newOutboxFn(allocator, input, typs, metadataSources)
}

func (c callbackRemoteComponentCreator) newInbox(
	allocator *vecexec.Allocator, typs []coltypes.T, streamID distsqlpb.StreamID,
) (*vecrpc.Inbox, error) {
	return c.newInboxFn(allocator, typs, streamID)
}

func intCols(numCols int) []sqlbase.ColumnType {
	cols := make([]sqlbase.ColumnType, numCols)
	for i := range cols {
		cols[i] = sqlbase.IntType
	}
	return cols
}

// TestDrainOnlyInputDAG is a regression test for #39137 to ensure
// that queries don't hang using the following scenario:
// Consider two nodes n1 and n2, an outbox (o1) and inbox (i1) on n1, and an
// arbitrary flow on n2.
// At the end of the query, o1 will drain its metadata sources when it
// encounters a zero-length batch from its input. If one of these metadata
// sources is i1, there is a possibility that a cycle is unknowingly created
// since i1 (as an example) could be pulling from a remote operator that itself
// is pulling from o1, which is at this moment attempting to drain i1.
// This test verifies that no metadata sources are added to an outbox that are
// not explicitly known to be in its input DAG. The diagram below outlines
// the main point of this test. The outbox's input ends up being some inbox
// pulling from somewhere upstream (in this diagram, node 3, but this detail is
// not important). If it drains the depicted inbox, that is pulling from node 2
// which is in turn pulling from an outbox, a cycle is created and the flow is
// blocked.
//          +------------+
//          |  Node 3    |
//          +-----+------+
//                ^
//      Node 1    |           Node 2
// +------------------------+-----------------+
//          +------------+  |
//     Spec C +--------+ |  |
//          | |  noop  | |  |
//          | +---+----+ |  |
//          |     ^      |  |
//          |  +--+---+  |  |
//          |  |outbox|  +<----------+
//          |  +------+  |  |        |
//          +------------+  |        |
// Drain cycle!---+         |   +----+-----------------+
//                v         |   |Any group of operators|
//          +------------+  |   +----+-----------------+
//          |  +------+  |  |        ^
//     Spec A  |inbox +--------------+
//          |  +------+  |  |
//          +------------+  |
//                ^         |
//                |         |
//          +-----+------+  |
//     Spec B    noop    |  |
//          |materializer|  +
//          +------------+
func TestDrainOnlyInputDAG(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		numInputTypesToOutbox       = 3
		numInputTypesToMaterializer = 1
	)
	// procs are the ProcessorSpecs that we pass in to create the flow. Note that
	// we order the inbox first so that the flow creator instantiates it before
	// anything else.
	procs := []distsqlpb.ProcessorSpec{
		{
			// This is i1, the inbox which should be drained by the materializer, not
			// o1.
			// Spec A in the diagram.
			Input: []distsqlpb.InputSyncSpec{
				{
					Streams:     []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_REMOTE, StreamID: 1}},
					ColumnTypes: intCols(numInputTypesToMaterializer),
				},
			},
			Core: distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			Output: []distsqlpb.OutputRouterSpec{
				{
					Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
					// We set up a local output so that the inbox is created independently.
					Streams: []distsqlpb.StreamEndpointSpec{
						{Type: distsqlpb.StreamEndpointSpec_LOCAL, StreamID: 2},
					},
				},
			},
		},
		// This is the root of the flow. The noop operator that will read from i1
		// and the materializer.
		// Spec B in the diagram.
		{
			Input: []distsqlpb.InputSyncSpec{
				{
					Streams:     []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_LOCAL, StreamID: 2}},
					ColumnTypes: intCols(numInputTypesToMaterializer),
				},
			},
			Core: distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			Output: []distsqlpb.OutputRouterSpec{
				{
					Type:    distsqlpb.OutputRouterSpec_PASS_THROUGH,
					Streams: []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_SYNC_RESPONSE}},
				},
			},
		},
		{
			// Because creating a table reader is too complex (you need to create a
			// bunch of other state) we simulate this by creating a noop operator with
			// a remote input, which is treated as having no local edges during
			// topological processing.
			// Spec C in the diagram.
			Input: []distsqlpb.InputSyncSpec{
				{
					Streams: []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_REMOTE}},
					// Use three Int columns as the types to be able to distinguish
					// between input DAGs when creating the inbox.
					ColumnTypes: intCols(numInputTypesToOutbox),
				},
			},
			Core: distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			// This is o1, the outbox that will drain metadata.
			Output: []distsqlpb.OutputRouterSpec{
				{
					Type:    distsqlpb.OutputRouterSpec_PASS_THROUGH,
					Streams: []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_REMOTE}},
				},
			},
		},
	}

	inboxToNumInputTypes := make(map[*vecrpc.Inbox][]coltypes.T)
	outboxCreated := false
	componentCreator := callbackRemoteComponentCreator{
		newOutboxFn: func(
			allocator *vecexec.Allocator,
			op vecexec.Operator,
			typs []coltypes.T,
			sources []distsqlpb.MetadataSource,
		) (*vecrpc.Outbox, error) {
			require.False(t, outboxCreated)
			outboxCreated = true
			// Verify that there is only one metadata source: the inbox that is the
			// input to the noop operator. This is verified by first checking the
			// number of metadata sources and then that the input types are what we
			// expect from the input DAG.
			require.Len(t, sources, 1)
			require.Len(t, inboxToNumInputTypes[sources[0].(*vecrpc.Inbox)], numInputTypesToOutbox)
			return vecrpc.NewOutbox(allocator, op, typs, sources)
		},
		newInboxFn: func(allocator *vecexec.Allocator, typs []coltypes.T, streamID distsqlpb.StreamID) (*vecrpc.Inbox, error) {
			inbox, err := vecrpc.NewInbox(allocator, typs, streamID)
			inboxToNumInputTypes[inbox] = typs
			return inbox, err
		},
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)
	f := &flowinfra.FlowBase{FlowCtx: runbase.FlowCtx{EvalCtx: &evalCtx, NodeID: roachpb.NodeID(1)}}
	var wg sync.WaitGroup
	vfc := newVectorizedFlowCreator(
		&vectorizedFlowCreatorHelper{f: f},
		componentCreator,
		false, /* recordingStats */
		&wg,
		&runbase.RowChannel{},
		nil, /* nodeDialer */
		distsqlpb.FlowID{},
	)

	_, err := vfc.setupFlow(ctx, &f.FlowCtx, procs, flowinfra.FuseNormally)
	defer func() {
		for _, memAcc := range vfc.streamingMemAccounts {
			memAcc.Close(ctx)
		}
	}()
	require.NoError(t, err)

	// Verify that an outbox was actually created.
	require.True(t, outboxCreated)
}
