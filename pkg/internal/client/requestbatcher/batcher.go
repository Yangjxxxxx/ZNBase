// Copyright 2019  The Cockroach Authors.
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

// Package requestbatcher is a library to enable easy batching of roachpb
// requests.
//
// Batching in general represents a tradeoff between throughput and latency. The
// underlying assumption being that batched operations are cheaper than an
// individual operation. If this is not the case for your workload, don't use
// this library.
//
// Batching assumes that data with the same key can be sent in a single batch.
// The initial implementation uses rangeID as the key explicitly to avoid
// creating an overly general solution without motivation but interested readers
// should recognize that it would be easy to extend this package to accept an
// arbitrary comparable key.
package requestbatcher

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// The motivating use case for this package are opportunities to perform cleanup
// operations in a single raft transaction rather than several. Three main
// opportunities are known:
//
//   1) Intent resolution
//   2) Txn heartbeating
//   3) Txn record garbage collection
//
// The first two have a relatively tight time bound expectations. In other words
// it would be surprising and negative for a client if operations were not sent
// soon after they were queued. The transaction record GC workload can be rather
// asynchronous. This motivates the need for some knobs to control the maximum
// acceptable amount of time to buffer operations before sending.
// Another wrinkle is dealing with the different ways in which sending a batch
// may fail. A batch may fail in an ambiguous way (RPC/network errors), it may
// fail completely (which is likely indistinguishable from the ambiguous
// failure) and lastly it may fail partially. Today's Sender contract is fairly
// ambiguous about the contract between BatchResponse inner responses and errors
// returned from a batch request.

// TODO(ajwerner): Do we need to consider ordering dependencies between
// operations? For the initial motivating use cases for this library there are
// no data dependencies between operations and the only key will be the guess
// for where an operation should go.

// TODO(ajwerner): Consider a more sophisticated mechanism to limit on maximum
// number of requests in flight at a time. This may ultimately lead to a need
// for queuing. Furthermore consider using batch time to dynamically tune the
// amount of time we wait.

// TODO(ajwerner): Consider filtering requests which might have been canceled
// before sending a batch.

// TODO(ajwerner): Consider more dynamic policies with regards to deadlines.
// Perhaps we want to wait no more than some percentile of the duration of
// historical operations and stay idle only some other percentile. For example
// imagine if the max delay was the 50th and the max idle was the 10th? This
// has a problem when much of the prior workload was say local operations and
// happened very rapidly. Perhaps we need to provide some bounding envelope?

// TODO(ajwerner): Consider a more general purpose interface for this package.
// While several interface-oriented interfaces have been explored they all felt
// heavy and allocation intensive

// TODO(ajwerner): Consider providing an interface which enables a single
// goroutine to dispatch a number of requests destined for different ranges to
// the RequestBatcher which may then wait for completion of all of the requests.
// What is the right contract for error handling? Imagine a situation where a
// client has dispatched N requests and one has been sent and returns with an
// error while others are queued. What should happen? Should the client receive
// the error rapidly? Should the other requests be sent at all? Should they be
// filtered before sending?

// Config contains the dependencies and configuration for a Batcher.
type Config struct {

	// Name of the batcher, used for logging and stopper.
	Name string

	// Sender can round-trip a batch. Sender must not be nil.
	Sender client.Sender

	// Stopper controls the lifecycle of the Batcher. Stopper must not be nil.
	Stopper *stop.Stopper

	// MaxSizePerBatch is the maximum number of bytes in individual requests in a
	// batch. If MaxSizePerBatch <= 0 then no limit is enforced.
	MaxSizePerBatch int

	// MaxMsgsPerBatch is the maximum number of messages.
	// If MaxMsgsPerBatch <= 0 then no limit is enforced.
	MaxMsgsPerBatch int

	// MaxWait is the maximum amount of time a message should wait in a batch
	// before being sent. If MaxWait is <= 0 then no wait timeout is enforced.
	// It is inadvisable to disable both MaxIdle and MaxWait.
	MaxWait time.Duration

	// MaxIdle is the amount of time a batch should wait between message additions
	// before being sent. The idle timer allows clients to observe low latencies
	// when throughput is low. If MaxWait is <= 0 then no wait timeout is
	// enforced. It is inadvisable to disable both MaxIdle and MaxWait.
	MaxIdle time.Duration

	// InFlightBackpressureLimit 是飞行中超过派遣客户应经历背压。
	// 如果配料器有更多在飞行中它将不接受新的请求，直到飞行中批次的数量再次低于此阈值。
	// 这个值可以不限制最终可能在飞行中的批数排队等待发送但尚未飞行的批仍将发送。
	// 注意，小于或等于零的值将导致使用默认DefaultInFlightBackpressureLimit。
	InFlightBackpressureLimit int

	// Capacity 并发chan容量
	Capacity int

	// NowFunc is used to determine the current time. It defaults to timeutil.Now.
	NowFunc func() time.Time
}

const (
	// DefaultInFlightBackpressureLimit is the InFlightBackpressureLimit used if
	// a zero value for that setting is passed in a Config to New.
	// TODO(ajwerner): Justify this number.
	DefaultInFlightBackpressureLimit = 1000

	// DefaultChanCapacity 默认并发容量
	DefaultChanCapacity = 65536

	// BackpressureRecoveryFraction is the fraction of InFlightBackpressureLimit
	// used to detect when enough in flight requests have completed such that more
	// requests should now be accepted. A value less than 1 is chosen in order to
	// avoid thrashing on backpressure which might ultimately defeat the purpose
	// of the RequestBatcher.
	backpressureRecoveryFraction = .8
)

func backpressureRecoveryThreshold(limit int) int {
	if l := int(float64(limit) * backpressureRecoveryFraction); l > 0 {
		return l
	}
	return 1 // don't allow the recovery threshold to be 0
}

// RequestBatcher batches requests destined for a single range based on
// a configured batching policy.
type RequestBatcher struct {
	pool pool
	cfg  Config

	batches batchQueue

	requestChan  chan *request
	sendDoneChan chan struct{}
}

// Response is exported for use with the channel-oriented SendWithChan method.
// At least one of Resp or Err will be populated for every sent Response.
type Response struct {
	Resp roachpb.Response
	Err  error
}

// New creates a new RequestBatcher.
func New(cfg Config) *RequestBatcher {
	validateConfig(&cfg)
	b := &RequestBatcher{
		cfg:          cfg,
		pool:         makePool(),
		batches:      makeBatchQueue(),
		requestChan:  make(chan *request, cfg.Capacity),
		sendDoneChan: make(chan struct{}),
	}
	if err := cfg.Stopper.RunAsyncTask(context.Background(), b.cfg.Name, b.run); err != nil {
		panic(err)
	}
	return b
}

func validateConfig(cfg *Config) {
	if cfg.Stopper == nil {
		panic("cannot construct a Batcher with a nil Stopper")
	} else if cfg.Sender == nil {
		panic("cannot construct a Batcher with a nil Sender")
	}
	if cfg.InFlightBackpressureLimit <= 0 {
		cfg.InFlightBackpressureLimit = DefaultInFlightBackpressureLimit
	}
	if cfg.NowFunc == nil {
		cfg.NowFunc = timeutil.Now
	}
}

// SendWithChan sends a request with a client provided response channel. The
// client is responsible for ensuring that the passed respChan has a buffer at
// least as large as the number of responses it expects to receive. Using an
// insufficiently buffered channel can lead to deadlocks and unintended delays
// processing requests inside the RequestBatcher.
func (b *RequestBatcher) SendWithChan(
	ctx context.Context, respChan chan<- Response, rangeID roachpb.RangeID, req roachpb.Request,
) error {
	select {
	case b.requestChan <- b.pool.newRequest(ctx, rangeID, req, respChan):
		return nil
	case <-b.cfg.Stopper.ShouldQuiesce():
		return stop.ErrUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Send sends req as a part of a batch. An error is returned if the context
// is canceled before the sending of the request completes.
func (b *RequestBatcher) Send(
	ctx context.Context, rangeID roachpb.RangeID, req roachpb.Request,
) (roachpb.Response, error) {
	responseChan := b.pool.getResponseChan()
	if err := b.SendWithChan(ctx, responseChan, rangeID, req); err != nil {
		return nil, err
	}
	select {
	case resp := <-responseChan:
		// It's only safe to put responseChan back in the pool if it has been
		// received from.
		b.pool.putResponseChan(responseChan)
		return resp.Resp, resp.Err
	case <-b.cfg.Stopper.ShouldQuiesce():
		return nil, stop.ErrUnavailable
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SendAsync 异步发送
func (b *RequestBatcher) SendAsync(
	ctx context.Context, rangeID roachpb.RangeID, req roachpb.Request,
) error {
	return b.SendWithChan(ctx, nil, rangeID, req)
}

func (b *RequestBatcher) sendDone(ba *batch) {
	b.pool.putBatch(ba)
	select {
	case b.sendDoneChan <- struct{}{}:
	case <-b.cfg.Stopper.ShouldQuiesce():
	}
}

func (b *RequestBatcher) sendBatch(ctx context.Context, ba *batch) {
	b.cfg.Stopper.RunWorker(ctx, func(ctx context.Context) {
		defer b.sendDone(ba)
		resp, pErr := b.cfg.Sender.Send(ctx, ba.batchRequest())
		for i, r := range ba.reqs {
			res := Response{}
			if resp != nil && i < len(resp.Responses) {
				res.Resp = resp.Responses[i].GetInner()
			}
			if pErr != nil {
				res.Err = pErr.GoError()
			}
			b.sendResponse(r, res)
		}
	})
}

func (b *RequestBatcher) sendResponse(req *request, resp Response) {
	// This send should never block because responseChan is buffered.
	if req.responseChan != nil {
		req.responseChan <- resp
	}
	b.pool.putRequest(req)
}

func addRequestToBatch(cfg *Config, now time.Time, ba *batch, r *request) (shouldSend bool) {
	ba.reqs = append(ba.reqs, r)
	ba.size += r.req.Size()
	ba.lastUpdated = now
	if cfg.MaxIdle > 0 {
		ba.deadline = ba.lastUpdated.Add(cfg.MaxIdle)
	}
	if cfg.MaxWait > 0 {
		waitDeadline := ba.startTime.Add(cfg.MaxWait)
		if cfg.MaxIdle <= 0 || waitDeadline.Before(ba.deadline) {
			ba.deadline = waitDeadline
		}
	}
	return (cfg.MaxMsgsPerBatch > 0 && len(ba.reqs) >= cfg.MaxMsgsPerBatch) ||
		(cfg.MaxSizePerBatch > 0 && ba.size >= cfg.MaxSizePerBatch)
}

func (b *RequestBatcher) cleanup(err error) {
	for ba := b.batches.popFront(); ba != nil; ba = b.batches.popFront() {
		for _, r := range ba.reqs {
			b.sendResponse(r, Response{Err: err})
		}
	}
}

func (b *RequestBatcher) run(ctx context.Context) {
	var (
		// inFlight tracks the number of batches currently being sent.
		// true.
		inFlight = 0
		// inBackPressure indicates whether the reqChan is enabled.
		// It becomes true when inFlight exceeds b.cfg.InFlightBackpressureLimit.
		inBackPressure = false
		// recoveryThreshold is the number of in flight requests below which the
		// the inBackPressure state should exit.
		recoveryThreshold = backpressureRecoveryThreshold(b.cfg.InFlightBackpressureLimit)
		// reqChan consults inBackPressure to determine whether the goroutine is
		// accepting new requests.
		reqChan = func() <-chan *request {
			if inBackPressure {
				return nil
			}
			return b.requestChan
		}
		sendBatch = func(ba *batch) {
			inFlight++
			if inFlight >= b.cfg.InFlightBackpressureLimit {
				inBackPressure = true
			}
			b.sendBatch(ctx, ba)
		}
		handleSendDone = func() {
			inFlight--
			if inFlight < recoveryThreshold {
				inBackPressure = false
			}
		}
		handleRequest = func(req *request) {
			now := b.cfg.NowFunc()
			ba, existsInQueue := b.batches.get(req.rangeID)
			if !existsInQueue {
				ba = b.pool.newBatch(now)
			}
			if resolveIntentRequest, ok := req.req.(*roachpb.ResolveIntentRequest); ok {
				ba.backFill = resolveIntentRequest.BackFill
			}
			if shouldSend := addRequestToBatch(&b.cfg, now, ba, req); shouldSend {
				if existsInQueue {
					b.batches.remove(ba)
				}
				sendBatch(ba)
			} else {
				b.batches.upsert(ba)
			}
		}
		deadline      time.Time
		timer         = timeutil.NewTimer()
		maybeSetTimer = func() {
			var nextDeadline time.Time
			if next := b.batches.peekFront(); next != nil {
				nextDeadline = next.deadline
			}
			if !deadline.Equal(nextDeadline) || timer.Read {
				deadline = nextDeadline
				if !deadline.IsZero() {
					timer.Reset(time.Until(deadline))
				} else {
					// Clear the current timer due to a sole batch already sent before
					// the timer fired.
					timer.Stop()
					timer = timeutil.NewTimer()
				}
			}
		}
	)
	for {
		select {
		case req := <-reqChan():
			handleRequest(req)
			maybeSetTimer()
		case <-timer.C:
			timer.Read = true
			sendBatch(b.batches.popFront())
			maybeSetTimer()
		case <-b.sendDoneChan:
			handleSendDone()
		case <-b.cfg.Stopper.ShouldQuiesce():
			b.cleanup(stop.ErrUnavailable)
			return
		case <-ctx.Done():
			b.cleanup(ctx.Err())
			return
		}
	}
}

type request struct {
	ctx          context.Context
	req          roachpb.Request
	rangeID      roachpb.RangeID
	responseChan chan<- Response
}

type batch struct {
	reqs []*request
	size int // bytes

	// idx is the batch's index in the batchQueue.
	idx int

	deadline    time.Time
	startTime   time.Time
	lastUpdated time.Time
	backFill    bool
}

func (b *batch) rangeID() roachpb.RangeID {
	if len(b.reqs) == 0 {
		panic("rangeID cannot be called on an empty batch")
	}
	return b.reqs[0].rangeID
}

func (b *batch) batchRequest() roachpb.BatchRequest {
	req := roachpb.BatchRequest{
		// Preallocate the Requests slice.
		Requests: make([]roachpb.RequestUnion, 0, len(b.reqs)),
	}
	for _, r := range b.reqs {
		req.Add(r.req)
	}
	if b.backFill {
		req.Header.BackFill = b.backFill
	}
	return req
}

// pool stores object pools for the various commonly reused objects of the
// batcher
type pool struct {
	responseChanPool sync.Pool
	batchPool        sync.Pool
	requestPool      sync.Pool
}

func makePool() pool {
	return pool{
		responseChanPool: sync.Pool{
			New: func() interface{} { return make(chan Response, 1) },
		},
		batchPool: sync.Pool{
			New: func() interface{} { return &batch{} },
		},
		requestPool: sync.Pool{
			New: func() interface{} { return &request{} },
		},
	}
}

func (p *pool) getResponseChan() chan Response {
	return p.responseChanPool.Get().(chan Response)
}

func (p *pool) putResponseChan(r chan Response) {
	p.responseChanPool.Put(r)
}

func (p *pool) newRequest(
	ctx context.Context, rangeID roachpb.RangeID, req roachpb.Request, responseChan chan<- Response,
) *request {
	r := p.requestPool.Get().(*request)
	*r = request{
		ctx:          ctx,
		rangeID:      rangeID,
		req:          req,
		responseChan: responseChan,
	}
	return r
}

func (p *pool) putRequest(r *request) {
	*r = request{}
	p.requestPool.Put(r)
}

func (p *pool) newBatch(now time.Time) *batch {
	ba := p.batchPool.Get().(*batch)
	*ba = batch{
		startTime: now,
		idx:       -1,
	}
	return ba
}

func (p *pool) putBatch(b *batch) {
	*b = batch{}
	p.batchPool.Put(b)
}

// batchQueue is a container for batch objects which offers O(1) get based on
// rangeID and peekFront as well as O(log(n)) upsert, removal, popFront.
// Batch structs are heap ordered inside of the batches slice based on their
// deadline with the earliest deadline at the front.
//
// Note that the batch struct stores its index in the batches slice and is -1
// when not part of the queue. The heap methods update the batch indices when
// updating the heap. Take care not to ever put a batch in to multiple
// batchQueues. At time of writing this package only ever used one batchQueue
// per RequestBatcher.
type batchQueue struct {
	batches []*batch
	byRange map[roachpb.RangeID]*batch
}

var _ heap.Interface = (*batchQueue)(nil)

func makeBatchQueue() batchQueue {
	return batchQueue{
		byRange: map[roachpb.RangeID]*batch{},
	}
}

func (q *batchQueue) peekFront() *batch {
	if q.Len() == 0 {
		return nil
	}
	return q.batches[0]
}

func (q *batchQueue) popFront() *batch {
	if q.Len() == 0 {
		return nil
	}
	return heap.Pop(q).(*batch)
}

func (q *batchQueue) get(id roachpb.RangeID) (*batch, bool) {
	b, exists := q.byRange[id]
	return b, exists
}

func (q *batchQueue) remove(ba *batch) {
	delete(q.byRange, ba.rangeID())
	heap.Remove(q, ba.idx)
}

func (q *batchQueue) upsert(ba *batch) {
	if ba.idx >= 0 {
		heap.Fix(q, ba.idx)
	} else {
		heap.Push(q, ba)
	}
}

func (q *batchQueue) Len() int {
	return len(q.batches)
}

func (q *batchQueue) Swap(i, j int) {
	q.batches[i], q.batches[j] = q.batches[j], q.batches[i]
	q.batches[i].idx = i
	q.batches[j].idx = j
}

func (q *batchQueue) Less(i, j int) bool {
	idl, jdl := q.batches[i].deadline, q.batches[j].deadline
	if before := idl.Before(jdl); before || !idl.Equal(jdl) {
		return before
	}
	return q.batches[i].rangeID() < q.batches[j].rangeID()
}

func (q *batchQueue) Push(v interface{}) {
	ba := v.(*batch)
	ba.idx = len(q.batches)
	q.byRange[ba.rangeID()] = ba
	q.batches = append(q.batches, ba)
}

func (q *batchQueue) Pop() interface{} {
	ba := q.batches[len(q.batches)-1]
	q.batches = q.batches[:len(q.batches)-1]
	delete(q.byRange, ba.rangeID())
	ba.idx = -1
	return ba
}
