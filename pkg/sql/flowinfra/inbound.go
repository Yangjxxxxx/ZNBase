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

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// ProcessInboundStream receives rows from a DistSQL_FlowStreamServer and sends
// them to a RowReceiver. Optionally processes an initial StreamMessage that was
// already received (because the first message contains the flow and stream IDs,
// it needs to be received before we can get here).
func ProcessInboundStream(
	ctx context.Context,
	stream distsqlpb.DistSQL_FlowStreamServer,
	firstMsg *distsqlpb.ProducerMessage,
	dst runbase.RowReceiver,
	f *FlowBase,
) error {

	err := processInboundStreamHelper(ctx, stream, firstMsg, dst, f)

	// err, if set, will also be propagated to the producer
	// as the last record that the producer gets.
	if err != nil {
		log.VEventf(ctx, 1, "inbound stream error: %s", err)
		return err
	}
	log.VEventf(ctx, 1, "inbound stream done")
	// We are now done. The producer, if it's still around, will receive an EOF
	// error over its side of the stream.
	return nil
}

func processInboundStreamHelper(
	ctx context.Context,
	stream distsqlpb.DistSQL_FlowStreamServer,
	firstMsg *distsqlpb.ProducerMessage,
	dst runbase.RowReceiver,
	f *FlowBase,
) error {
	draining := false
	var sd StreamDecoder

	sendErrToConsumer := func(err error) {
		if err != nil {
			dst.Push(nil, &distsqlpb.ProducerMetadata{Err: err})
		}
		dst.ProducerDone()
	}

	if firstMsg != nil {
		if res := processProducerMessage(
			ctx, stream, dst, &sd, &draining, firstMsg,
		); res.err != nil || res.consumerClosed {
			sendErrToConsumer(res.err)
			return res.err
		}
	}

	// There's two goroutines involved in handling the RPC - the current one (the
	// "parent"), which is watching for context cancellation, and a "reader" one
	// that receives messages from the stream. This is all because a stream.Recv()
	// call doesn't react to context cancellation. The idea is that, if the parent
	// detects a canceled context, it will return from this RPC handler, which
	// will cause the stream to be closed. Because the parent cannot wait for the
	// reader to finish (that being the whole point of the different goroutines),
	// the reader sending an error to the parent might race with the parent
	// finishing. In that case, nobody cares about the reader anymore and so its
	// result channel is buffered.
	errChan := make(chan error, 1)

	f.GetWaitGroup().Add(1)
	go func() {
		defer f.GetWaitGroup().Done()
		if runbase.OptTableReader && f.CanParallel {
			MsgChan := make(chan *distsqlpb.ProducerMessage, 64)
			var wg sync.WaitGroup
			for i := 0; i < runbase.OptTableReaderParallelSetting; i++ {
				wg.Add(1)
				//var sd1 StreamDecoder
				//sd1.headerReceived = sd.headerReceived
				var sd1 StreamDecoder
				sd1 = sd
				draining1 := draining
				go OptProcessProducerMessage(ctx, stream, dst, &sd1, &draining1, &MsgChan, &wg)
			}
			go RecvRPCRows(stream, &MsgChan, &errChan)
			wg.Wait()
			sendErrToConsumer(nil)
		} else {
			for {
				msg, err := stream.Recv()
				if err != nil {
					if err != io.EOF {
						// Communication error.
						err = pgerror.NewErrorf(pgcode.ConnectionFailure, "communication error: %s", err)
						sendErrToConsumer(err)
						errChan <- err
						return
					}
					// End of the stream.
					sendErrToConsumer(nil)
					errChan <- nil
					return
				}
				if res := processProducerMessage(
					ctx, stream, dst, &sd, &draining, msg,
				); res.err != nil || res.consumerClosed {
					sendErrToConsumer(res.err)
					errChan <- res.err
					return
				}
			}
		}
	}()

	// Check for context cancellation while reading from the stream on another
	// goroutine.
	select {
	case <-f.GetCtxDone():
		return sqlbase.QueryCanceledError
	case err := <-errChan:
		return err
	}
}

// RecvRPCRows for async recv form rpc data
func RecvRPCRows(
	stream distsqlpb.DistSQL_FlowStreamServer,
	MsgChan *chan *distsqlpb.ProducerMessage,
	errChan *chan error,
) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				// Communication error.
				err = pgerror.NewErrorf(pgcode.ConnectionDoesNotExist, "communication error: %s", err)
				close(*MsgChan)
				*errChan <- err
				return
			}
			// End of the stream.
			close(*MsgChan)
			*errChan <- nil
			return
		}
		*MsgChan <- msg
	}
}

// sendDrainSignalToProducer is called when the consumer wants to signal the
// producer that it doesn't need any more rows and the producer should drain. A
// signal is sent on stream to the producer to ask it to send metadata.
func sendDrainSignalToStreamProducer(
	ctx context.Context, stream distsqlpb.DistSQL_FlowStreamServer,
) error {
	log.VEvent(ctx, 1, "sending drain signal to producer")
	sig := distsqlpb.ConsumerSignal{DrainRequest: &distsqlpb.DrainRequest{}}
	return stream.Send(&sig)
}

// processProducerMessage is a helper function to process data from the producer
// and send it along to the consumer. It keeps track of whether or not it's
// draining between calls. If err in the result is set (or if the consumer is
// closed), the caller must return the error to the producer.
func processProducerMessage(
	ctx context.Context,
	stream distsqlpb.DistSQL_FlowStreamServer,
	dst runbase.RowReceiver,
	sd *StreamDecoder,
	draining *bool,
	msg *distsqlpb.ProducerMessage,
) processMessageResult {
	err := sd.AddMessage(ctx, msg)
	if err != nil {
		return processMessageResult{
			err:            errors.Wrap(err, log.MakeMessage(ctx, "decoding error", nil /* args */)),
			consumerClosed: false,
		}
	}
	var types []sqlbase.ColumnType
	for {
		row, meta, err := sd.GetRow(nil /* rowBuf */)
		if err != nil {
			return processMessageResult{err: err, consumerClosed: false}
		}
		if row == nil && meta == nil {
			// No more rows in the last message.
			return processMessageResult{err: nil, consumerClosed: false}
		}

		if log.V(3) && row != nil {
			if types == nil {
				cts, _ := sqlbase.DatumTypesToColumnTypes(sd.Types())
				types = cts
			}
			log.Infof(ctx, "inbound stream pushing row %s", row.String(types))
		}
		if *draining && meta == nil {
			// Don't forward data rows when we're draining.
			continue
		}
		switch dst.Push(row, meta) {
		case runbase.NeedMoreRows:
			continue
		case runbase.DrainRequested:
			// The rest of rows are not needed by the consumer. We'll send a drain
			// signal to the producer and expect it to quickly send trailing
			// metadata and close its side of the stream, at which point we also
			// close the consuming side of the stream and call dst.ProducerDone().
			if !*draining {
				*draining = true
				if err := sendDrainSignalToStreamProducer(ctx, stream); err != nil {
					log.Errorf(ctx, "draining error: %s", err)
				}
			}
		case runbase.ConsumerClosed:
			return processMessageResult{err: nil, consumerClosed: true}
		}
	}
	//}
}

// OptProcessProducerMessage is a helper function to process data from the producer
// and send it along to the consumer. It keeps track of whether or not it's
// draining between calls. If err in the result is set (or if the consumer is
// closed), the caller must return the error to the producer.
func OptProcessProducerMessage(
	ctx context.Context,
	stream distsqlpb.DistSQL_FlowStreamServer,
	dst runbase.RowReceiver,
	sd *StreamDecoder,
	draining *bool,
	MsgsChan *chan *distsqlpb.ProducerMessage,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {
		msg, ok := <-*MsgsChan
		if !ok {
			return
		}
		err := sd.AddMessage(ctx, msg)
		if err != nil {
			//ResultChan <- processMessageResult{
			//	err:            errors.Wrap(err, log.MakeMessage(ctx, "decoding error", nil /* args */)),
			//	consumerClosed: false,
			//}
			//continue
			return
		}
		done, _ := ParallelGetRow(ctx, stream, dst, sd, draining)
		if done {
			return
		}
	}
}

// ParallelGetRow is used for sending rows to output channel
func ParallelGetRow(
	ctx context.Context,
	stream distsqlpb.DistSQL_FlowStreamServer,
	dst runbase.RowReceiver,
	sd *StreamDecoder,
	draining *bool,
) (bool, error) {
	var types []sqlbase.ColumnType
	for {
		row, meta, err := sd.GetRow(nil /* rowBuf */)
		if err != nil {
			//Scchan <- processMessageResult{err: err, consumerClosed: false}
			//atomic.StoreInt32(parallelDone, 0)
			return true, err
		}
		if row == nil && meta == nil {
			// No more rows in the last message.
			//Scchan <- processMessageResult{err: nil, consumerClosed: false}
			//atomic.StoreInt32(parallelDone, 0)
			return false, nil
		}

		if log.V(3) && row != nil {
			if types == nil {
				cts, _ := sqlbase.DatumTypesToColumnTypes(sd.Types())
				types = cts
			}
			log.Infof(ctx, "inbound stream pushing row %s", row.String(types))
		}
		if *draining && meta == nil {
			// Don't forward data rows when we're draining.
			continue
		}
		switch dst.Push(row, meta) {
		case runbase.NeedMoreRows:
			continue
		case runbase.DrainRequested:
			// The rest of rows are not needed by the consumer. We'll send a drain
			// signal to the producer and expect it to quickly send trailing
			// metadata and close its side of the stream, at which point we also
			// close the consuming side of the stream and call dst.ProducerDone().
			if !*draining {
				*draining = true
				if err := sendDrainSignalToStreamProducer(ctx, stream); err != nil {
					log.Errorf(ctx, "draining error: %s", err)
				}
			}
		case runbase.ConsumerClosed:
			//atomic.StoreInt32(parallelDone, 0)
			//Scchan <- processMessageResult{err: nil, consumerClosed: true}
			return true, nil
		}
	}
}

type processMessageResult struct {
	err            error
	consumerClosed bool
}

// InboundStreamHandler is a handler of an inbound stream.
type InboundStreamHandler interface {
	// run is called once a FlowStream RPC is handled and a stream is obtained to
	// make this stream accessible to the rest of the flow.
	Run(
		ctx context.Context, stream distsqlpb.DistSQL_FlowStreamServer, firstMsg *distsqlpb.ProducerMessage, f *FlowBase,
	) error
	// timeout is called with an error, which results in the teardown of the
	// stream strategy with the given error.
	// WARNING: timeout may block.
	Timeout(err error)
}

// RowInboundStreamHandler is an InboundStreamHandler for the row based flow.
// It is exported since it is the default for the flow infrastructure.
type RowInboundStreamHandler struct {
	runbase.RowReceiver
}

var _ InboundStreamHandler = RowInboundStreamHandler{}

// Run is part of the InboundStreamHandler interface.
func (s RowInboundStreamHandler) Run(
	ctx context.Context,
	stream distsqlpb.DistSQL_FlowStreamServer,
	firstMsg *distsqlpb.ProducerMessage,
	f *FlowBase,
) error {
	return ProcessInboundStream(ctx, stream, firstMsg, s.RowReceiver, f)
}

// Timeout is part of the InboundStreamHandler interface.
func (s RowInboundStreamHandler) Timeout(err error) {
	s.Push(
		nil, /* row */
		&distsqlpb.ProducerMetadata{Err: err},
	)
	s.ProducerDone()
}
