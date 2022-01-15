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

package flowinfra

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/contextutil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
	"google.golang.org/grpc"
)

const outboxBufRows = 16
const optoutboxBufRows = 64
const outboxFlushPeriod = 100 * time.Microsecond

// PreferredEncoding is the encoding used for EncDatums that don't already have
// an encoding available.
const PreferredEncoding = sqlbase.DatumEncoding_ASCENDING_KEY

type flowStream interface {
	Send(*distsqlpb.ProducerMessage) error
	Recv() (*distsqlpb.ConsumerSignal, error)
}

// Outbox implements an outgoing mailbox as a RowReceiver that receives rows and
// sends them to a gRPC stream. Its core logic runs in a goroutine. We send rows
// when we accumulate outboxBufRows or every outboxFlushPeriod (whichever comes
// first).
type Outbox struct {
	// RowChannel implements the RowReceiver interface.
	runbase.RowChannel

	flowCtx  *runbase.FlowCtx
	streamID distsqlpb.StreamID
	nodeID   roachpb.NodeID
	// The rows received from the RowChannel will be forwarded on this stream once
	// it is established.
	stream flowStream

	encoder StreamEncoder
	// numRows is the number of rows that have been accumulated in the encoder.
	numRows int

	// flowCtxCancel is the cancellation function for this flow's ctx; context
	// cancellation is used to stop processors on this flow. It is invoked
	// whenever the consumer returns an error on the stream above. Set
	// to a non-null value in start().
	flowCtxCancel context.CancelFunc

	err error

	statsCollectionEnabled bool
	stats                  rowexec.OutboxStats
}

var _ runbase.RowReceiver = &Outbox{}
var _ Startable = &Outbox{}

//NewOutbox create a Outbox
func NewOutbox(
	flowCtx *runbase.FlowCtx,
	nodeID roachpb.NodeID,
	flowID distsqlpb.FlowID,
	streamID distsqlpb.StreamID,
) *Outbox {
	m := &Outbox{flowCtx: flowCtx, nodeID: nodeID}
	m.encoder.SetHeaderFields(flowID, streamID)
	m.streamID = streamID
	return m
}

// NewOutboxSyncFlowStream sets up an Outbox for the special "sync flow"
// stream. The flow context should be provided via setFlowCtx when it is
// available.
func NewOutboxSyncFlowStream(stream distsqlpb.DistSQL_RunSyncFlowServer) *Outbox {
	return &Outbox{stream: stream}
}

//SetFlowCtx set flowCtx
func (m *Outbox) SetFlowCtx(flowCtx *runbase.FlowCtx) {
	m.flowCtx = flowCtx
}

//Init init types
func (m *Outbox) Init(types []sqlbase.ColumnType) {
	if types == nil {
		// We check for nil to detect uninitialized cases; but we support 0-length
		// rows.
		types = make([]sqlbase.ColumnType, 0)
	}
	m.RowChannel.InitWithNumSenders(types, 1)
	m.encoder.init(types)
}

// addRow encodes a row into rowBuf. If enough rows were accumulated, flush() is
// called.
//
// If an error is returned, the Outbox's stream might or might not be usable; if
// it's not usable, it will have been set to nil. The error might be a
// communication error, in which case the other side of the stream should get it
// too, or it might be an encoding error, in which case we've forwarded it on
// the stream.
func (m *Outbox) addRow(
	ctx context.Context, row sqlbase.EncDatumRow, meta *distsqlpb.ProducerMetadata,
) error {
	mustFlush := false
	var encodingErr error
	if meta != nil {
		m.encoder.AddMetadata(ctx, *meta)
		// If we hit an error, let's forward it ASAP. The consumer will probably
		// close.
		mustFlush = meta.Err != nil
	} else {
		encodingErr = m.encoder.AddRow(row)
		if encodingErr != nil {
			m.encoder.AddMetadata(ctx, distsqlpb.ProducerMetadata{Err: encodingErr})
			mustFlush = true
		}
	}
	m.numRows++
	var flushErr error
	if !runbase.OptTableReader {
		if m.numRows >= outboxBufRows || mustFlush {
			flushErr = m.flush(ctx)
		}
	} else {
		if m.numRows >= optoutboxBufRows || mustFlush {
			flushErr = m.flush(ctx)
		}
	}

	if encodingErr != nil {
		return encodingErr
	}
	return flushErr
}

// flush sends the rows accumulated so far in a ProducerMessage. Any error
// returned indicates that sending a message on the Outbox's stream failed, and
// thus the stream can't be used any more. The stream is also set to nil if
// an error is returned.
func (m *Outbox) flush(ctx context.Context) error {
	if m.numRows == 0 && m.encoder.headerSent {
		return nil
	}
	msg := m.encoder.FormMessage(ctx)
	if m.statsCollectionEnabled {
		m.stats.BytesSent += int64(msg.Size())
	}

	if log.V(3) {
		log.Infof(ctx, "flushing Outbox")
	}
	sendErr := m.stream.Send(msg)
	if sendErr != nil {
		// Make sure the stream is not used any more.
		m.stream = nil
		if log.V(1) {
			log.Errorf(ctx, "Outbox flush error: %s", sendErr)
		}
	} else if log.V(3) {
		log.Infof(ctx, "Outbox flushed")
	}
	if sendErr != nil {
		return sendErr
	}

	m.numRows = 0
	return nil
}

// mainLoop reads from m.RowChannel and writes to the output stream through
// addRow()/flush() until the producer doesn't have any more data to send or an
// error happened.
//
// If the consumer asks the producer to drain, mainLoop() will relay this
// information and, again, wait until the producer doesn't have any more data to
// send (the producer is supposed to only send trailing metadata once it
// receives this signal).
//
// If an error is returned, it's either a communication error from the Outbox's
// stream, or otherwise the error has already been forwarded on the stream.
// Depending on the specific error, the stream might or might not need to be
// closed. In case it doesn't, m.stream has been set to nil.
func (m *Outbox) mainLoop(ctx context.Context) error {
	// No matter what happens, we need to make sure we close our RowChannel, since
	// writers could be writing to it as soon as we are started.
	defer m.RowChannel.ConsumerClosed()

	var span opentracing.Span
	ctx, span = runbase.ProcessorSpan(ctx, "Outbox")
	if span != nil && tracing.IsRecording(span) {
		m.statsCollectionEnabled = true
		span.SetTag(distsqlpb.StreamIDTagKey, m.streamID)
	}
	// spanFinished specifies whether we called tracing.FinishSpan on the span.
	// Some code paths (e.g. stats collection) need to prematurely call
	// FinishSpan to get trace data.
	spanFinished := false
	defer func() {
		if !spanFinished {
			tracing.FinishSpan(span)
		}
	}()

	if m.stream == nil {
		var conn *grpc.ClientConn
		var err error
		conn, err = m.flowCtx.Cfg.NodeDialer.Dial(ctx, m.nodeID, rpc.DefaultClass)
		if err != nil {
			return err
		}
		client := distsqlpb.NewDistSQLClient(conn)
		if log.V(2) {
			log.Infof(ctx, "Outbox: calling FlowStream")
		}
		// The context used here escapes, so it has to be a background context.
		m.stream, err = client.FlowStream(context.TODO())
		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "FlowStream error: %s", err)
			}
			return err
		}
		if log.V(2) {
			log.Infof(ctx, "Outbox: FlowStream returned")
		}
	}

	var flushTimer timeutil.Timer
	defer flushTimer.Stop()

	draining := false

	// TODO(andrei): It's unfortunate that we're spawning a goroutine for every
	// outgoing stream, but I'm not sure what to do instead. The streams don't
	// have a non-blocking API. We could start this goroutine only after a
	// timeout, but that timeout would affect queries that use flows with
	// LimitHint's (so, queries where the consumer is expected to quickly ask the
	// producer to drain). Perhaps what we want is a way to tell when all the rows
	// corresponding to the first KV batch have been sent and only start the
	// goroutine if more batches are needed to satisfy the query.
	listenToConsumerCtx, cancel := contextutil.WithCancel(ctx)
	drainCh, err := m.listenForDrainSignalFromConsumer(listenToConsumerCtx)
	defer cancel()
	if err != nil {
		return err
	}

	// Send a first message that will contain the header (i.e. the StreamID), so
	// that the stream is properly initialized on the consumer. The consumer has
	// a timeout in which inbound streams must be established.
	if err := m.flush(ctx); err != nil {
		return err
	}

	for {
		select {
		case msg, ok := <-m.RowChannel.C:
			if !ok {
				// No more data.
				if m.statsCollectionEnabled {
					err := m.flush(ctx)
					if err != nil {
						return nil
					}
					if m.flowCtx.Cfg.TestingKnobs.DeterministicStats {
						m.stats.BytesSent = 0
					}
					tracing.SetSpanStats(span, &m.stats)
					tracing.FinishSpan(span)
					spanFinished = true
					if trace := runbase.GetTraceData(ctx); trace != nil {
						err := m.addRow(ctx, nil, &distsqlpb.ProducerMetadata{TraceData: trace})
						if err != nil {
							return err
						}
					}
				}
				return m.flush(ctx)
			}
			if !draining || msg.Meta != nil {
				// If we're draining, we ignore all the rows and just send metadata.
				err := m.addRow(ctx, msg.Row, msg.Meta)
				if err != nil {
					return err
				}
				// If the message to add was metadata, a flush was already forced. If
				// this is our first row, restart the flushTimer.
				if m.numRows == 1 {
					flushTimer.Reset(outboxFlushPeriod)
				}
			}
		case <-flushTimer.C:
			flushTimer.Read = true
			err := m.flush(ctx)
			if err != nil {
				return err
			}
		case drainSignal := <-drainCh:
			if drainSignal.err != nil {
				// Stop work from proceeding in this flow. This also causes FlowStream
				// RPCs that have this node as consumer to return errors.
				m.flowCtxCancel()
				// The consumer either doesn't care any more (it returned from the
				// FlowStream RPC with an error if the Outbox established the stream or
				// it canceled the client context if the consumer established the
				// stream through a RunSyncFlow RPC), or there was a communication error
				// and the stream is dead. In any case, the stream has been closed and
				// the consumer will not consume more rows from this Outbox. Make sure
				// the stream is not used any more.
				m.stream = nil
				return drainSignal.err
			}
			drainCh = nil
			if drainSignal.drainRequested {
				// Enter draining mode.
				draining = true
				m.RowChannel.ConsumerDone()
			} else {
				// No draining required. We're done; no need to consume any more.
				// m.RowChannel.ConsumerClosed() is called in a defer above.
				return nil
			}
		}
	}
}

// drainSignal is a signal received from the consumer telling the producer that
// it doesn't need any more rows and optionally asking the producer to drain.
type drainSignal struct {
	// drainRequested, if set, means that the consumer is interested in the
	// trailing metadata that the producer might have. If not set, the producer
	// should close immediately (the consumer is probably gone by now).
	drainRequested bool
	// err, if set, is either the error that the consumer returned when closing
	// the FlowStream RPC or a communication error.
	err error
}

// listenForDrainSignalFromConsumer returns a channel that will be pinged once the
// consumer has closed its send-side of the stream, or has sent a drain signal.
//
// This method runs a task that will run until either the consumer closes the
// stream or until the caller cancels the context. The caller has to cancel the
// context once it no longer reads from the channel, otherwise this method might
// deadlock when attempting to write to the channel.
func (m *Outbox) listenForDrainSignalFromConsumer(ctx context.Context) (<-chan drainSignal, error) {
	ch := make(chan drainSignal, 1)

	stream := m.stream
	if err := m.flowCtx.Cfg.Stopper.RunAsyncTask(ctx, "drain", func(ctx context.Context) {
		sendDrainSignal := func(drainRequested bool, err error) bool {
			select {
			case ch <- drainSignal{drainRequested: drainRequested, err: err}:
				return true
			case <-ctx.Done():
				// Listening for consumer signals has been canceled. This generally
				// means that the main Outbox routine is no longer listening to these
				// signals but, in the RunSyncFlow case, it may also mean that the
				// client (the consumer) has canceled the RPC. In that case, the main
				// routine is still listening (and this branch of the select has been
				// randomly selected; the other was also available), so we have to
				// notify it. Thus, we attempt sending again.
				select {
				case ch <- drainSignal{drainRequested: drainRequested, err: err}:
					return true
				default:
					return false
				}
			}
		}

		for {
			signal, err := stream.Recv()
			if err == io.EOF {
				sendDrainSignal(false, nil)
				return
			}
			if err != nil {
				sendDrainSignal(false, err)
				return
			}
			switch {
			case signal.DrainRequest != nil:
				if !sendDrainSignal(true, nil) {
					return
				}
			case signal.SetupFlowRequest != nil:
				log.Fatalf(ctx, "Unexpected SetupFlowRequest. "+
					"This SyncFlow specific message should have been handled in RunSyncFlow.")
			case signal.Handshake != nil:
				log.Eventf(ctx, "Consumer sent handshake. Consuming flow scheduled: %t",
					signal.Handshake.ConsumerScheduled)
			}
		}
	}); err != nil {
		return nil, err
	}
	return ch, nil
}

func (m *Outbox) run(ctx context.Context, wg *sync.WaitGroup) {
	err := m.mainLoop(ctx)
	if stream, ok := m.stream.(distsqlpb.DistSQL_FlowStreamClient); ok {
		closeErr := stream.CloseSend()
		if err == nil {
			err = closeErr
		}
	}
	m.err = err
	if wg != nil {
		wg.Done()
	}
}

// Err returns the error (if any occurred) while Outbox was running.
func (m *Outbox) Err() error {
	return m.err
}

//Start Starts the Outbox.
func (m *Outbox) Start(ctx context.Context, wg *sync.WaitGroup, flowCtxCancel context.CancelFunc) {
	if m.Types() == nil {
		panic("Outbox not initialized")
	}
	if wg != nil {
		wg.Add(1)
	}
	m.flowCtxCancel = flowCtxCancel
	go m.run(ctx, wg)
}
