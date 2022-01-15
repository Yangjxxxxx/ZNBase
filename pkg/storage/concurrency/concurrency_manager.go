// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"context"
	"sync"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/spanlatch"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/storage/storagepb"
	"github.com/znbasedb/znbase/pkg/storage/txnwait"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// DefaultBalanceIntentLength 用来解决Bug9878.详细查看使用地方
const DefaultBalanceIntentLength int = 200

// managerImpl implements the Manager interface. 实现管理器接口。
type managerImpl struct {
	// Synchronizes conflicting in-flight requests.同步冲突的动态请求。
	lm latchManager
	// Synchronizes conflicting in-progress transactions.同步正在进行的冲突事务。
	lt lockTable
	// Waits for locks that conflict with a request to be released.等待与请求冲突的锁被释放。
	ltw lockTableWaiter
	// Waits for transaction completion and detects deadlocks.等待事务完成并检测死锁。
	twq txnWaitQueue
}

// Config contains the dependencies to construct a Manager.包含构造管理器的依赖项。
type Config struct {
	// Identification.识别
	NodeDesc  *roachpb.NodeDescriptor
	RangeDesc *roachpb.RangeDescriptor
	// Components. 组件。
	Settings       *cluster.Settings
	DB             *client.DB
	Clock          *hlc.Clock
	Stopper        *stop.Stopper
	IntentResolver IntentResolver
	// Metrics.。指标
	TxnWaitMetrics *txnwait.Metrics
	SlowLatchGauge *metric.Gauge
	// Configs + Knobs.配置+旋钮
	MaxLockTableSize  int64
	DisableTxnPushing bool
	TxnWaitKnobs      txnwait.TestingKnobs
}

func (c *Config) initDefaults() {
	if c.MaxLockTableSize == 0 {
		c.MaxLockTableSize = defaultLockTableSize
	}
}

// NewManager creates a new concurrency Manager structure.创建一个新的并发管理器结构。
func NewManager(cfg Config) Manager {
	cfg.initDefaults()
	m := new(managerImpl)
	*m = managerImpl{
		// TODO(nvanbenschoten): move pkg/storage/spanlatch to a new
		// pkg/storage/concurrency/latch package. Make it implement the
		// latchMani
		lm: &latchManagerImpl{
			m: spanlatch.Make(
				cfg.Stopper,
				cfg.SlowLatchGauge,
			),
		},
		lt: &lockTableImpl{
			maxLocks: cfg.MaxLockTableSize,
		},
		ltw: &lockTableWaiterImpl{
			st:                cfg.Settings,
			stopper:           cfg.Stopper,
			ir:                cfg.IntentResolver,
			lm:                m,
			disableTxnPushing: cfg.DisableTxnPushing,
		},
		// TODO(nvanbenschoten): move pkg/storage/txnwait to a new
		// pkg/storage/concurrency/txnwait package.
		twq: txnwait.NewQueue(txnwait.Config{
			RangeDesc: cfg.RangeDesc,
			DB:        cfg.DB,
			Clock:     cfg.Clock,
			Stopper:   cfg.Stopper,
			Metrics:   cfg.TxnWaitMetrics,
			Knobs:     cfg.TxnWaitKnobs,
		}),
	}
	return m
}

// SequenceReq implements the RequestSequencer interface.
func (m *managerImpl) SequenceReq(
	ctx context.Context, prev *Guard, req Request,
) (*Guard, Response, *Error) {
	var g *Guard
	if prev == nil {
		g = newGuard(req)
		log.Event(ctx, "sequencing request")
	} else {
		g = prev
		g.AssertNoLatches()
		log.Event(ctx, "re-sequencing request")
	}

	resp, err := m.sequenceReqWithGuard(ctx, g, req)
	if resp != nil || err != nil {
		// Ensure that we release the guard if we return a response or an error.
		m.FinishReq(g)
		return nil, resp, err
	}
	return g, nil, nil
}

func (m *managerImpl) sequenceReqWithGuard(
	ctx context.Context, g *Guard, req Request,
) (Response, *Error) {
	has := req.Requests[0].GetDumpOnline()
	// Some requests don't need to acquire latches at all.
	if !shouldAcquireLatches(req) && has == nil {
		log.Event(ctx, "not acquiring latches")
		return nil, nil
	}
	g.Req.BackFill = req.BackFill

	// Provide the manager with an opportunity to intercept the request. It
	// may be able to serve the request directly, and even if not, it may be
	// able to update its internal state based on the request.
	// 为管理人员提供拦截请求的机会。它可以直接为请求提供服务，
	// 即使不能，它也可以根据请求更新其内部状态。
	resp, err := m.maybeInterceptReq(ctx, req)
	if resp != nil || err != nil {
		return resp, err
	}

	for {
		// Acquire latches for the request. This synchronizes the request
		// with all conflicting in-flight requests.
		// 获取请求的锁存。这将使请求与所有冲突的动态请求同步。
		log.Event(ctx, "acquiring latches")
		g.lg, err = m.lm.Acquire(ctx, req)
		if err != nil {
			return nil, err
		}
		// Some requests don't want the wait on locks.
		// 有些请求不需要锁上的等待。
		if req.LockSpans.Empty() {
			return nil, nil
		}

		// Scan for conflicting locks.
		// 扫描冲突的锁。
		log.Event(ctx, "scanning lock table for conflicting locks")
		g.ltg = m.lt.ScanAndEnqueue(g.Req, g.ltg)
		// Wait on conflicting locks, if necessary.
		// 等待冲突的锁。
		if g.ltg.ShouldWait() {
			m.lm.Release(g.moveLatchGuard())

			log.Event(ctx, "waiting in lock wait-queues")
			if req.WaitPolicy == lock.WaitPolicy_Time {
				if err := m.ltw.WaitOnTime(ctx, g.Req, g.ltg); err != nil {
					return nil, err
				}
			} else {
				if err := m.ltw.WaitOn(ctx, g.Req, g.ltg); err != nil {
					return nil, err
				}
			}
			continue
		}
		return nil, nil
	}
}

// maybeInterceptReq allows the concurrency manager to intercept requests before
// sequencing and evaluation so that it can immediately act on them. This allows
// the concurrency manager to route certain concurrency control-related requests
// into queues and optionally update its internal state based on the requests.
// maybeInterceptreq允许并发管理器在排序和评估请求之前拦截请求，
// 以便立即对它们采取行动。这允许并发管理器将某些与并发控制相关的请求路由到队列中，并可根据请求更新其内部状态。
func (m *managerImpl) maybeInterceptReq(ctx context.Context, req Request) (Response, *Error) {
	switch {
	case req.isSingle(roachpb.PushTxn):
		// If necessary, wait in the txnWaitQueue for the pushee transaction to
		// expire or to move to a finalized state.
		// 如果有必要，在txnWaitQueue中等待pushee事务过期或转移到最后状态。
		t := req.Requests[0].GetPushTxn()
		resp, err := m.twq.MaybeWaitForPush(ctx, t)
		if err != nil {
			return nil, err
		} else if resp != nil {
			return makeSingleResponse(resp), nil
		}
	case req.isSingle(roachpb.QueryTxn):
		// If necessary, wait in the txnWaitQueue for a transaction state update
		// or for a dependent transaction to change.
		// 如果有必要，在txnWaitQueue中等待事务状态更新或依赖事务更改。
		t := req.Requests[0].GetQueryTxn()
		return nil, m.twq.MaybeWaitForQuery(ctx, t)
	default:
		// TODO(nvanbenschoten): in the future, use this hook to update the lock
		// table to allow contending transactions to proceed.
		// for _, arg := range req.Requests {
		// 	switch t := arg.GetInner().(type) {
		// 	case *roachpb.ResolveIntentRequest:
		// 		_ = t
		// 	case *roachpb.ResolveIntentRangeRequest:
		// 		_ = t
		// 	}
		// }
	}
	return nil, nil
}

//GetSpanLockStatus 获取span持有锁的状态
func (m *managerImpl) GetSpanLockStatus(
	span roachpb.Span, transactionID string,
) map[lock.Durability]bool {
	return m.lt.GetSpanLockMsg(span, transactionID)
}

// shouldAcquireLatches determines whether the request should acquire latches
// before proceeding to evaluate. Latches are used to synchronize with other
// conflicting requests, based on the Spans collected for the request. Most
// request types will want to acquire latches.
// 在进行评估之前，shouldAcquireLatches决定请求是否应该获取锁存。
// 锁用于根据为请求收集的跨度与其他冲突请求同步。大多数请求类型都希望获得锁存。
func shouldAcquireLatches(req Request) bool {
	switch {
	case req.ReadConsistency != roachpb.CONSISTENT:
		// Only acquire latches for consistent operations.
		return false
	case req.isSingle(roachpb.RequestLease):
		// Do not acquire latches for lease requests. These requests are run on
		// replicas that do not hold the lease, so acquiring latches wouldn't
		// help synchronize with other requests.
		// 不要为租约请求获取锁。这些请求在不持有租约的副本上运行，因此获取锁存将无助于与其他请求同步
		return false
	}
	return true
}

// FinishReq implements the RequestSequencer interface.
func (m *managerImpl) FinishReq(g *Guard) {
	if ltg := g.moveLockTableGuard(); ltg != nil {
		m.lt.Dequeue(ltg)
	}
	if lg := g.moveLatchGuard(); lg != nil {
		m.lm.Release(lg)
	}
	releaseGuard(g)
}

// HandleWriterIntentError implements the ContentionHandler interface.
func (m *managerImpl) HandleWriterIntentError(
	ctx context.Context, g *Guard, seq roachpb.LeaseSequence, t *roachpb.WriteIntentError,
) (*Guard, *Error) {
	if g.ltg == nil {
		log.Fatalf(ctx, "cannot handle WriteIntentError %v for request without "+
			"lockTableGuard; were lock spans declared for this request?", t)
	}

	// Add a discovered lock to lock-table for each intent and enter each lock's
	// wait-queue. If the lock-table is disabled and one or more of the intents
	// are ignored then we immediately wait on all intents.
	// 为每个意图添加一个已发现的锁到锁表中，并输入每个锁的等待队列。
	// 如果锁表被禁用，并且一个或多个意图被忽略，那么我们将立即等待所有意图。
	wait := false
	for i := range t.Intents {
		intent := &t.Intents[i]
		added, err := m.lt.AddDiscoveredLock(intent, seq, g.ltg)
		if err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		if !added {
			wait = true
		}
	}

	// Release the Guard's latches but continue to remain in lock wait-queues by
	// not releasing lockWaitQueueGuards. We expect the caller of this method to
	// then re-sequence the Request by calling SequenceReq with the un-latched
	// Guard. This is analogous to iterating through the loop in SequenceReq.
	// 释放保护的锁，但通过不释放lockwaitqueueguard继续保持在锁定等待队列中。
	// 我们期望这个方法的调用者然后通过调用SequenceReq来使用未锁住的保护来重新排序请求。
	// 这类似于在SequenceReq中遍历循环。
	m.lm.Release(g.moveLatchGuard())

	// If the lockTable was disabled then we need to immediately wait on the
	// intents to ensure that they are resolved and moved out of the request's
	// way.
	// 如果禁用了lockTable，那么我们需要立即等待意图，以确保它们被解析并移出请求的路径。
	if wait {
		for i := range t.Intents {
			intent := &t.Intents[i]
			// 用来解决Bug9878.当上一个事务做了500万行的inert语句后，下一个事务立马执行select。
			// 这样在select时候会发现大量的WriteIntent，本用例是30万左右个。这样select会想解决掉
			// 所有的 writeintent,导致for循环执行时间过长，有可能后面的writeintent早就已经被清理
			// 掉了，所以目前的策略是，解决一部分以后出去竞争一下，看是否还会有writeintent。
			if i >= DefaultBalanceIntentLength {
				return g, nil
			}
			if err := m.ltw.WaitOnLock(ctx, g.Req, intent); err != nil {
				m.FinishReq(g)
				return nil, err
			}
		}
	}

	return g, nil
}

// HandleTransactionPushError implements the ContentionHandler interface.
func (m *managerImpl) HandleTransactionPushError(
	ctx context.Context, g *Guard, t *roachpb.TransactionPushError,
) *Guard {
	m.twq.EnqueueTxn(&t.PusheeTxn)

	// Release the Guard's latches. The PushTxn request should not be in any
	// lock wait-queues because it does not scan the lockTable. We expect the
	// caller of this method to then re-sequence the Request by calling
	// SequenceReq with the un-latched Guard. This is analogous to iterating
	// through the loop in SequenceReq.
	// 释放保护的锁。PushTxn请求不应该在任何锁等待队列中，因为它不扫描lockTable。
	// 我们期望这个方法的调用者然后通过调用SequenceReq来使用未锁住的保护来重新排序请求。
	// 这类似于在SequenceReq中遍历循环。
	m.lm.Release(g.moveLatchGuard())
	return g
}

/// OnLockAcquired implements the LockManager interface.
//  OnLockAcquired实现了LockManager接口。
func (m *managerImpl) OnLockAcquired(ctx context.Context, acq *roachpb.LockAcquisition) {
	if err := m.lt.AcquireLock(&acq.Txn, acq.Key, lock.Exclusive, acq.Durability); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
}

// OnLockUpdated implements the LockManager interface.
// OnLockUpdated实现了LockManager接口。
func (m *managerImpl) OnLockUpdated(ctx context.Context, up *roachpb.LockUpdate) {
	if err := m.lt.UpdateLocks(up); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
}

// OnTransactionUpdated implements the TransactionManager interface.
// OnTransactionUpdated实现了TransactionManager接口。
func (m *managerImpl) OnTransactionUpdated(ctx context.Context, txn *roachpb.Transaction) {
	m.twq.UpdateTxn(ctx, txn)
}

// GetDependents implements the TransactionManager interface.
func (m *managerImpl) GetDependents(txnID uuid.UUID) []uuid.UUID {
	return m.twq.GetDependents(txnID)
}

// OnRangeDescUpdated implements the RangeStateListener interface.
func (m *managerImpl) OnRangeDescUpdated(desc *roachpb.RangeDescriptor) {
	m.twq.OnRangeDescUpdated(desc)
}

// OnRangeLeaseUpdated implements the RangeStateListener interface.
func (m *managerImpl) OnRangeLeaseUpdated(seq roachpb.LeaseSequence, isLeaseholder bool) {
	if isLeaseholder {
		m.lt.Enable(seq)
		m.twq.Enable(seq)
	} else {
		// Disable all queues - the concurrency manager will no longer be
		// informed about all state transitions to locks and transactions.
		// 禁用所有队列——并发管理器将不再被告知所有状态转换到锁和事务
		const disable = true
		m.lt.Clear(disable)
		m.twq.Clear(disable)
		// Also clear caches, since they won't be needed any time soon and
		// consume memory.
		// 还要清除缓存，因为它们很快就不需要了，而且会消耗内存。
		m.ltw.ClearCaches()
	}
}

// OnRangeSplit implements the RangeStateListener interface.
func (m *managerImpl) OnRangeSplit() {
	// TODO(nvanbenschoten): it only essential that we clear the half of the
	// lockTable which contains locks in the key range that is being split off
	// from the current range. For now though, we clear it all.
	// lockTable包含从当前range中分离出的键range的锁。不过现在，我们把它清理干净了。
	const disable = false
	m.lt.Clear(disable)
	m.twq.Clear(disable)
}

// OnRangeMerge implements the RangeStateListener interface.
func (m *managerImpl) OnRangeMerge() {
	// Disable all queues - the range is being merged into its LHS neighbor.
	// It will no longer be informed about all state transitions to locks and
	// transactions.
	// 禁用所有队列——范围将合并到它的LHS邻居中。它将不再被告知向锁和事务的所有状态转换。
	const disable = true
	m.lt.Clear(disable)
	m.twq.Clear(disable)
}

// OnReplicaSnapshotApplied implements the RangeStateListener interface.
func (m *managerImpl) OnReplicaSnapshotApplied() {
	// A snapshot can cause discontinuities in raft entry application. The
	// lockTable expects to observe all lock state transitions on the range
	// through LockManager listener methods. If there's a chance it missed a
	// state transition, it is safer to simply clear the lockTable and rebuild
	// it from persistent intent state by allowing requests to discover locks
	// and inform the manager through calls to HandleWriterIntentError.
	//
	// A range only maintains locks in the lockTable of its leaseholder replica
	// even thought it runs a concurrency manager on all replicas. Because of
	// this, we expect it to be very rare that this actually clears any locks.
	// Still, it is possible for the leaseholder replica to receive a snapshot
	// when it is not also the raft leader.
	// 在raft进入应用中，快照会引起不连续。lockTable期望通过LockManager
	// 侦听器方法观察范围内的所有锁状态转换。如果它有可能错过了状态转换，那
	// 么更安全的方法是简单地清除lockTable，并通过允许请求发现锁并通过
	// 调用HandleWriterIntentError通知管理器，从持久的意图状态重新构建它。
	//
	//range只维护其租赁者副本的lockTable中的锁，即使它在所有副本上运行并发管理器。
	const disable = false
	m.lt.Clear(disable)
}

// LatchMetrics implements the MetricExporter interface.
func (m *managerImpl) LatchMetrics() (global, local storagepb.LatchManagerInfo) {
	return m.lm.Info()
}

// LockTableDebug implements the MetricExporter interface.
func (m *managerImpl) LockTableDebug() string {
	return m.lt.String()
}

// TxnWaitQueue implements the MetricExporter interface.
func (m *managerImpl) TxnWaitQueue() *txnwait.Queue {
	return m.twq.(*txnwait.Queue)
}

func (r *Request) txnMeta() *enginepb.TxnMeta {
	if r.Txn == nil {
		return nil
	}
	return &r.Txn.TxnMeta
}

// readConflictTimestamp returns the maximum timestamp at which the request
// conflicts with locks acquired by other transaction. The request must wait
// for all locks acquired by other transactions at or below this timestamp
// to be released. All locks acquired by other transactions above this
// timestamp are ignored.
// readConflictTimestamp返回请求与其他事务获得的锁冲突的最大时间戳。
// 请求必须等待其他事务在此时间戳或低于此时间戳获得的所有锁被释放。
// 其他事务在此时间戳以上获取的所有锁都将被忽略。
func (r *Request) readConflictTimestamp() hlc.Timestamp {
	ts := r.Timestamp
	if r.Txn != nil {
		ts = r.Txn.ReadTimestamp
		ts.Forward(r.Txn.MaxTimestamp)
	}
	return ts
}

// writeConflictTimestamp returns the minimum timestamp at which the request
// acquires locks when performing mutations. All writes performed by the
// requests must take place at or above this timestamp.
func (r *Request) writeConflictTimestamp() hlc.Timestamp {
	ts := r.Timestamp
	if r.Txn != nil {
		ts = r.Txn.WriteTimestamp
	}
	return ts
}

func (r *Request) isSingle(m roachpb.Method) bool {
	if len(r.Requests) != 1 {
		return false
	}
	return r.Requests[0].GetInner().Method() == m
}

// Used to avoid allocations.
var guardPool = sync.Pool{
	New: func() interface{} { return new(Guard) },
}

func newGuard(req Request) *Guard {
	g := guardPool.Get().(*Guard)
	g.Req = req
	return g
}

func releaseGuard(g *Guard) {
	*g = Guard{}
	guardPool.Put(g)
}

// LatchSpans returns the maximal set of spans that the request will access.
func (g *Guard) LatchSpans() *spanset.SpanSet {
	return g.Req.LatchSpans
}

// HoldingLatches returned whether the guard is holding latches or not.
func (g *Guard) HoldingLatches() bool {
	return g != nil && g.lg != nil
}

// AssertLatches asserts that the guard is non-nil and holding latches.
func (g *Guard) AssertLatches() {
	if !g.HoldingLatches() {
		panic("expected latches held, found none")
	}
}

// AssertNoLatches asserts that the guard is non-nil and not holding latches.
func (g *Guard) AssertNoLatches() {
	if g.HoldingLatches() {
		panic("unexpected latches held")
	}
}

func (g *Guard) moveLatchGuard() latchGuard {
	lg := g.lg
	g.lg = nil
	return lg
}

func (g *Guard) moveLockTableGuard() lockTableGuard {
	ltg := g.ltg
	g.ltg = nil
	return ltg
}

func makeSingleResponse(r roachpb.Response) Response {
	ru := make(Response, 1)
	ru[0].MustSetInner(r)
	return ru
}

// MockManager is for test
func MockManager(cfg Config, txnMeta enginepb.TxnMeta) Manager {
	cfg.initDefaults()
	m := new(managerImpl)
	*m = managerImpl{
		// TODO(nvanbenschoten): move pkg/storage/spanlatch to a new
		// pkg/storage/concurrency/latch package. Make it implement the
		// latchMani
		lm: &latchManagerImpl{
			m: spanlatch.Make(
				cfg.Stopper,
				cfg.SlowLatchGauge,
			),
		},
		lt: &lockTableImpl{
			maxLocks:   cfg.MaxLockTableSize,
			enabled:    true,
			enabledSeq: 1,
		},
		ltw: &lockTableWaiterImpl{
			st:                cfg.Settings,
			stopper:           cfg.Stopper,
			ir:                cfg.IntentResolver,
			lm:                m,
			disableTxnPushing: cfg.DisableTxnPushing,
		},
		// TODO(nvanbenschoten): move pkg/storage/txnwait to a new
		// pkg/storage/concurrency/txnwait package.
		twq: txnwait.NewQueue(txnwait.Config{
			RangeDesc: cfg.RangeDesc,
			DB:        cfg.DB,
			Clock:     cfg.Clock,
			Stopper:   cfg.Stopper,
			Metrics:   cfg.TxnWaitMetrics,
			Knobs:     cfg.TxnWaitKnobs,
		}),
	}
	ltGuard := &lockTableGuardImpl{
		txn: &txnMeta,
	}
	spans := &spanset.SpanSet{}
	spans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.MinKey, EndKey: keys.UserTableDataMin})
	spans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.UserTableDataMin, EndKey: keys.MaxKey})
	ltGuard.spans = spans
	intent := &roachpb.Intent{
		Intent_SingleKeySpan: roachpb.Intent_SingleKeySpan{Key: keys.SystemMax},
		Txn:                  txnMeta,
	}
	_, _ = m.lt.AddDiscoveredLock(intent, roachpb.LeaseSequence(1), ltGuard)
	return m
}
