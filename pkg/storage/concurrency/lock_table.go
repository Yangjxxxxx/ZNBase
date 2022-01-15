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
	"container/list"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// Default upper bound on the number of locks in a lockTable.
// lockTable中锁数的默认上限。
const defaultLockTableSize = 10000

// The kind of waiting that the request is subject to.
// 请求所服从的等待方式。
type waitKind int

const (
	_ waitKind = iota

	// waitFor indicates that the request is waiting on another transaction to
	// to release its locks or complete its own request. waitingStates with this
	// waitKind will provide information on who the request is waiting on. The
	// request will likely want to eventually push the conflicting transaction.
	// waitFor表示请求正在等待另一个事务释放它的锁或完成它自己的请求。
	// 具有这种等待类型的等待状态将提供请求正在等待谁的信息。请求可能希望最终推入冲突的事务。
	waitFor

	// waitForDistinguished is a sub-case of waitFor. It implies everything that
	// waitFor does and additionally indicates that the request is currently the
	// "distinguished waiter". A distinguished waiter is responsible for taking
	// extra actions, e.g. immediately pushing the transaction it is waiting
	// for. If there are multiple requests in the waitFor state waiting on the
	// same transaction, at least one will be a distinguished waiter.
	// waitfordistingushed是waitFor的一个子案例。它意味着waitFor所做的一切，
	// 并且还表明请求当前是“distinguished waiter”。distinguished waiter负责采取额外的行动，
	// 例如，立即推动正在等待的事务。如果有多个处于waitFor状态的请求在同一个事务上等待，
	// 那么至少有一个请求将是distinguished waiter。
	waitForDistinguished

	// waitElsewhere is used when the lockTable is under memory pressure and is
	// clearing its internal queue state. Like the waitFor* states, it informs
	// the request who it is waiting for so that deadlock detection works.
	// However, sequencing information inside the lockTable is mostly discarded.
	// 当lockTable处于内存压力下并正在清除其内部队列状态时，将使用waitElsewhere。
	// 与waitFor*状态一样，它通知请求正在等待的对象，以便死锁检测工作。
	// 然而，lockTable内部的排序信息大多被丢弃。
	waitElsewhere

	// waitSelf indicates that a different requests from the same transaction
	// has a conflicting reservation. See the comment about "Reservations" in
	// lockState. This request should sit tight and wait for a new notification
	// without pushing anyone.
	// waSelf 表示来自同一事务的不同请求有一个冲突的保留。见lockState中关于“reservation”的提交。
	// 这个请求应该等待一个新的通知，而不是推给任何人。
	waitSelf

	// doneWaiting indicates that the request is done waiting on this pass
	// through the lockTable and should make another call to ScanAndEnqueue.
	// doneWaiting表示该请求已通过lockTable完成等待，并应再次调用ScanAndEnqueue。
	doneWaiting
)

// The current waiting state of the request.
//
// See the detailed comment about "Waiting logic" on lockTableGuardImpl
//.请求的当前等待状态。

////查看lockTableGuardImpl上关于“waiting logic”的详细注释。
type waitingState struct {
	kind waitKind

	// Fields below are populated for waitFor* and waitElsewhere kinds.

	// Represents who the request is waiting for. The conflicting
	// transaction may be a lock holder of a conflicting lock or a
	// conflicting request being sequenced through the same lockTable.
	//下面的字段被填充为waitFor*和waitElsewhere类型。
	//表示请求正在等待谁。冲突事务可以是冲突锁的持有者，也可以是排序通过同一个锁表的冲突请求。
	txn  *enginepb.TxnMeta // always non-nil
	key  roachpb.Key       // the key of the conflict
	held bool              // is the conflict a held lock?

	// Represents the action that the request was trying to perform when
	// it hit the conflict. E.g. was it trying to read or write?
	//表示请求在遇到冲突时试图执行的操作。它是在试着读还是写?
	guardAccess spanset.SpanAccess
}

// Implementation
// TODO(sbhola):
// - metrics about lockTable state to export to observability debug pages:
//   number of locks, number of waiting requests, wait time?, ...
// - test cases where guard.readTS != guard.writeTS.

// The btree for a particular SpanScope.
type treeMu struct {
	mu syncutil.RWMutex // Protects everything in this struct.

	// For assigning sequence numbers to the lockState objects as required by
	// the util/interval/generic type contract.
	// 用于根据util/interval/generic类型协定的要求为lockState对象分配序列号。
	lockIDSeqNum uint64

	// Container for lockState structs. Locks that are not held or reserved and
	// have no waiting requests are garbage collected. Additionally, locks that
	// are only held with Replicated durability and have no waiting requests may
	// also be garbage collected since their state can be recovered from
	// persistent storage.
	// 锁定状态结构体的容器。未被持有或保留且没有等待请求的锁将被垃圾回收。
	// 此外，只有具有复制持久性且没有等待请求的锁也可能被垃圾回收，因为它们的状态可以从持久存储中恢复。
	btree

	// For constraining memory consumption. We need better memory accounting
	// than this.
	// 用于限制内存消耗。我们需要更好的存储计算方法。
	numLocks int64
}

// lockTableImpl is an implementation of lockTable.
//
// Concurrency: in addition to holding latches, we require for a particular
// request ScanAndEnqueue() and CurState() must be called by the same
// thread.
//
// Mutex ordering:   lockTableImpl.enabledMu
//                 > treeMu.mu
//                 > lockState.mu
//                 > lockTableGuardImpl.mu
// lockTableImpl是lockTable的一个实现。
// 并发性:除了保持锁存之外，我们还要求对于特定的请求，同一个线程必须调用ScanAndEnqueue()和CurState()。
// 互斥锁排序:lockTableImpl.enabledMu
//						> treeMu.mu
//                 	 	> lockState.mu
//                 		> lockTableGuardImpl.mu
type lockTableImpl struct {
	// Is the lockTable enabled? When enabled, the lockTable tracks locks and
	// allows requests to queue in wait-queues on these locks. When disabled,
	// no locks or wait-queues are maintained.
	//
	// enabledMu is held in read-mode when determining whether the lockTable
	// is enabled and when acting on that information (e.g. adding new locks).
	// It is held in write-mode when enabling or disabling the lockTable.
	//
	// enabledSeq holds the lease sequence for which the lockTable is enabled
	// under. Discovered locks from prior lease sequences are ignored, as they
	// may no longer be accurate.
	// 锁表启用了吗？启用锁队列时，允许在表队列中锁定这些请求。禁用时，不会维护锁或等待队列。
	//
	// 当确定是否启用锁表以及对该信息执行操作（例如添加新锁）时，enabledMu处于读取模式。
	//
	// 当启用或禁用锁定表时，它将保持在写入模式下。enabledSeq保存为其启用锁表的租约序列。
	// 先前租约序列中发现的锁将被忽略，因为它们可能不再准确。
	enabled    bool
	enabledMu  syncutil.RWMutex
	enabledSeq roachpb.LeaseSequence

	// A sequence number is assigned to each request seen by the lockTable. This
	// is to preserve fairness despite the design choice of allowing
	// out-of-order evaluation of requests with overlapping spans where the
	// latter request does not encounter contention. This out-of-order
	// evaluation happens because requests do not reserve spans that are
	// uncontended while they wait for on contended locks after releasing their
	// latches.
	//为lockTable看到的每个请求分配一个序列号。这是为了保持公平性，
	//尽管在设计上选择了允许对跨重叠的请求进行无序评估(后者不会发生争用)。
	//发生这种无序求值的原因是，请求在释放锁存之后等待争用的锁时，不会保留未争用的spans
	//Consider the following examples:
	//
	// Example 1:
	// - req1 wants to write to A, B
	// - req2 wants to write to B
	// - lock at A is held by some other txn.
	// - Even though req2 arrives later, req1 will wait only in the queue for A
	//   and allow req2 to proceed to evaluation.
	//
	// Example 2:
	// - Same as example 1 but lock at A is held by txn3 and lock at B is held
	//   by txn4.
	// - Lock at A is released so req1 acquires the reservation at A and starts
	//   waiting at B.
	// - It is unfair for req1 to wait behind req2 at B. The sequence number
	//   assigned to req1 and req2 will restore the fairness by making req1
	//   wait before req2.
	//
	// Example 3: Deadlock in lock table if it did not use sequence numbers.
	// - Lock at B is acquired by txn0.
	// - req1 (from txn1) arrives at lockTable and wants to write to A and B.
	//   It queues at B.
	// - req2 (from txn2) arrives at lockTable and only wants to write A.
	//   It proceeds to evaluation and acquires the lock at A for txn2 and then
	//   the request is done. The lock is still held.
	// - req3 (from txn3) wants to write to A and B. It queues at A.
	// - txn2 releases A. req3 is in the front of the queue at A and gets the
	//   reservation and starts waiting at B behind req1.
	// - txn0 releases B. req1 gets the reservation at B and does another scan
	//   and adds itself to the queue at A, behind req3 which holds the
	//   reservation at A.
	// Now in the queues for A and B req1 is behind req3 and vice versa and
	// this deadlock has been created entirely due to the lock table's behavior.
	seqNum uint64

	locks [spanset.NumSpanScope]treeMu

	maxLocks int64
}

var _ lockTable = &lockTableImpl{}

// lockTableGuardImpl is an implementation of lockTableGuard.
//
// The struct is a guard that is returned to the request the first time it calls
// lockTable.ScanAndEnqueue() and used in later calls to ScanAndEnqueue() and
// done(). After a call to ScanAndEnqueue() (which is made while holding
// latches), the caller must first call lockTableGuard.StartWaiting() and if it
// returns true release the latches and continue interacting with the
// lockTableGuard. If StartWaiting() returns false, the request can proceed to
// evaluation.
//
//结构体是一个保护，它在第一次调用lockTable.ScanAndEnqueue()时返回给请求，
//并在以后调用ScanAndEnqueue()和done()时使用。在调用ScanAndEnqueue()(这是在持有锁存器时进行的)之后，
//调用者必须首先调用lockTableGuard. startwaiting()，
//如果它返回true，则释放锁存器并继续与lockTableGuard交互。如果StartWaiting()返回false，则请求可以继续计算。
// Waiting logic: The interface hides the queues that the request is waiting on,
// and the request's position in the queue. One of the reasons for this hiding
// is that queues are not FIFO since a request that did not wait on a queue for
// key k in a preceding call to ScanAndEnqueue() (because k was not locked and
// there was no queue) may need to wait on the queue in a later call to
// ScanAndEnqueue(). So sequencing of requests arriving at the lockTable is
// partially decided by a sequence number assigned to a request when it first
// called ScanAndEnqueue() and queues are ordered by this sequence number.
// However the sequencing is not fully described by the sequence numbers -- a
// request R1 encountering contention over some keys in its span does not
// prevent a request R2 that has a higher sequence number and overlapping span
// to proceed if R2 does not encounter contention. This concurrency (that is not
// completely fair) is deemed desirable.
// 等待逻辑：接口隐藏请求正在等待的队列，以及请求在队列中的位置。
// 这种隐藏的原因之一是队列不是FIFO，因为在对ScanAndEnqueue（）的前一次调用中，
// 没有在队列上等待键k的请求（因为k没有被锁定并且没有队列），
// 可能需要在以后调用ScanAndEnqueue（）时等待队列。
// 因此，到达锁表的请求的顺序部分取决于第一次调用ScanAndEnqueue（）时分配给请求的序列号，
// 队列按此顺序排序号码。但是序列号并没有完全描述序列号——请求R1在其范围内遇到了一些键的争用，
// 而不是这样如果R2没有遇到争用，则阻止具有更高序列号和重叠跨度的请求R2继续。
// 这种一致性（不完全公平）被认为是可取的。
//
// The interface exposes an abstracted version of the waiting logic in a way
// that the request that starts waiting is considered waiting for at most one
// other request or transaction. This is exposed as a series of state
// transitions where the transitions are notified via newState() and the current
// state can be read using CurState().
//
// - The waitFor* states provide information on who the request is waiting for.
//   The waitForDistinguished state is a sub-case -- a distinguished waiter is
//   responsible for taking extra actions e.g. immediately pushing the transaction
//   it is waiting for. The implementation ensures that if there are multiple
//   requests in waitFor state waiting on the same transaction at least one will
//   be a distinguished waiter.
//
//   TODO(sbhola): investigate removing the waitForDistinguished state which
//   will simplify the code here. All waitFor requests would wait (currently
//   50ms) before pushing the transaction (for deadlock detection) they are
//   waiting on, say T. Typically T will be done before 50ms which is considered
//   ok: the one exception we will need to make is if T has the min priority or
//   the waiting transaction has max priority -- in both cases it will push
//   immediately. The bad case is if T is ABORTED: the push will succeed after,
//   and if T left N intents, each push would wait for 50ms, incurring a latency
//   of 50*N ms. A cache of recently encountered ABORTED transactions on each
//   Store should mitigate this latency increase. Whenever a transaction sees a
//   waitFor state, it will consult this cache and if T is found, push
//   immediately (if there isn't already a push in-flight) -- even if T is not
//   initially in the cache, the first push will place it in the cache, so the
//   maximum latency increase is 50ms.
//
// - The waitElsewhere state is a rare state that is used when the lockTable is
//   under memory pressure and is clearing its internal queue state. Like the
//   waitFor* states, it informs the request who it is waiting for so that
//   deadlock detection works. However, sequencing information inside the
//   lockTable is mostly discarded.
//
// - The waitSelf state is a rare state when a different request from the same
//   transaction has a reservation. See the comment about "Reservations" in
//   lockState.
//
// - The doneWaiting state is used to indicate that the request should make
//   another call to ScanAndEnqueue() (that next call is more likely to return a
//   lockTableGuard that returns false from StartWaiting()).
type lockTableGuardImpl struct {
	seqNum uint64

	// Information about this request.
	txn     *enginepb.TxnMeta
	spans   *spanset.SpanSet
	readTS  hlc.Timestamp
	writeTS hlc.Timestamp

	// Snapshots of the trees for which this request has some spans. Note that
	// the lockStates in these snapshots may have been removed from
	// lockTableImpl. Additionally, it is possible that there is a new lockState
	// for the same key. This can result in various harmless anomalies:
	// - the request may hold a reservation on a lockState that is no longer
	//   in the tree. When it next does a scan, it will either find a new
	//   lockState where it will compete or none. Both lockStates can be in
	//   the mu.locks map, which is harmless.
	// - the request may wait behind a reservation holder that is not the
	//   lock holder. This could cause a delay in pushing the lock holder.
	//   This is not a correctness issue (the whole system is not deadlocked)
	//   and we expect will not be a real performance issue.
	//
	// TODO(sbhola): experimentally evaluate the lazy queueing of the current
	// implementation, in comparison with eager queueing. If eager queueing
	// is comparable in system throughput, one can eliminate the above anomalies.
	//
	tableSnapshot [spanset.NumSpanScope]btree

	// A request whose startWait is set to true in ScanAndEnqueue is actively
	// waiting at a particular key. This is the first key encountered when
	// iterating through spans that it needs to wait at. A future event (lock
	// release etc.) may cause the request to no longer need to wait at this
	// key. It then needs to continue iterating through spans to find the next
	// key to wait at (we don't want to wastefully start at the beginning since
	// this request probably has a reservation at the contended keys there): sa,
	// ss, index, key collectively track the current position to allow it to
	// continue iterating.

	// The key for the lockState.
	key roachpb.Key
	// The key for the lockState is contained in the Span specified by
	// spans[sa][ss][index].
	ss    spanset.SpanScope
	sa    spanset.SpanAccess // Iterates from stronger to weaker strength
	index int

	mu struct {
		syncutil.Mutex
		startWait bool

		state  waitingState
		signal chan struct{}

		// locks for which this request has a reservation or is in the queue of
		// writers (active or inactive) or actively waiting as a reader.
		//
		// TODO(sbhola): investigate whether the logic to maintain this locks map
		// can be simplified so it doesn't need to be adjusted by various
		// lockState methods. It adds additional bookkeeping burden that means it
		// is more prone to inconsistencies. There are two main uses: (a) removing
		// from various lockStates when done() is called, (b) tryActiveWait() uses
		// it as an optimization to know that this request is not known to the
		// lockState. (b) can be handled by other means -- the first scan the
		// request won't be in the lockState and the second scan it likely will.
		// (a) doesn't necessarily require this map to be consistent -- the
		// request could track the places where it is has enqueued as places where
		// it could be present and then do the search.

		locks map[*lockState]struct{}

		// If this is true, the state has changed and the channel has been
		// signaled, but what the state should be has not been computed. The call
		// to CurState() needs to compute that current state. Deferring the
		// computation makes the waiters do this work themselves instead of making
		// the call to release/update locks or release reservations do this work
		// (proportional to number of waiters).
		mustFindNextLockAfter bool
	}
}

var _ lockTableGuard = &lockTableGuardImpl{}

// Used to avoid allocations.
var lockTableGuardImplPool = sync.Pool{
	New: func() interface{} {
		g := new(lockTableGuardImpl)
		g.mu.signal = make(chan struct{}, 1)
		g.mu.locks = make(map[*lockState]struct{})
		return g
	},
}

// newLockTableGuardImpl returns a new lockTableGuardImpl. The struct will
// contain pre-allocated mu.signal and mu.locks fields, so it shouldn't be
// overwritten blindly.
func newLockTableGuardImpl() *lockTableGuardImpl {
	return lockTableGuardImplPool.Get().(*lockTableGuardImpl)
}

// releaseLockTableGuardImpl releases the guard back into the object pool.
func releaseLockTableGuardImpl(g *lockTableGuardImpl) {
	// Preserve the signal channel and locks map fields in the pooled
	// object. Drain the signal channel and assert that the map is empty.
	// The map should have been cleared by lockState.requestDone.
	signal, locks := g.mu.signal, g.mu.locks
	select {
	case <-signal:
	default:
	}
	if len(locks) != 0 {
		panic("lockTableGuardImpl.mu.locks not empty after Dequeue")
	}

	*g = lockTableGuardImpl{}
	g.mu.signal = signal
	g.mu.locks = locks
	lockTableGuardImplPool.Put(g)
}

func (g *lockTableGuardImpl) ShouldWait() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.startWait
}

func (g *lockTableGuardImpl) NewStateChan() chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.signal
}

func (g *lockTableGuardImpl) CurState() waitingState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.mu.mustFindNextLockAfter {
		return g.mu.state
	}
	// Not actively waiting anywhere so no one else can set
	// mustFindNextLockAfter to true while this method executes.
	g.mu.mustFindNextLockAfter = false
	g.mu.Unlock()
	g.findNextLockAfter(false /* notify */)
	g.mu.Lock() // Unlock deferred
	return g.mu.state
}

func (g *lockTableGuardImpl) notify() {
	select {
	case g.mu.signal <- struct{}{}:
	default:
	}
}

// Called when the request is no longer actively waiting at lock l, and should
// look for the next lock to wait at. hasReservation is true iff the request
// acquired the reservation at l. Note that it will be false for requests that
// were doing a read at the key, or non-transactional writes at the key.
func (g *lockTableGuardImpl) doneWaitingAtLock(hasReservation bool, l *lockState) {
	g.mu.Lock()
	if !hasReservation {
		delete(g.mu.locks, l)
	}
	g.mu.mustFindNextLockAfter = true
	g.notify()
	g.mu.Unlock()
}

func (g *lockTableGuardImpl) isSameTxn(txn *enginepb.TxnMeta) bool {
	return g.txn != nil && g.txn.ID == txn.ID
}

func (g *lockTableGuardImpl) isSameTxnAsReservation(ws waitingState) bool {
	return !ws.held && g.isSameTxn(ws.txn)
}

// Finds the next lock, after the current one, to actively wait at. If it
// finds the next lock the request starts actively waiting there, else it is
// told that it is done waiting.
//找到当前锁之后的下一个锁，以主动等待。
//如果它找到了下一个锁，请求就开始在那里积极地等待，否则就被告知它已完成等待。
// Acquires g.mu.
func (g *lockTableGuardImpl) findNextLockAfter(notify bool) {
	spans := g.spans.GetSpans(g.sa, g.ss)
	var span *spanset.Span
	resumingInSameSpan := false
	if g.index == -1 || len(spans[g.index].EndKey) == 0 {
		span = stepToNextSpan(g)
	} else {
		span = &spans[g.index]
		resumingInSameSpan = true
	}
	for span != nil {
		startKey := span.Key
		if resumingInSameSpan {
			startKey = g.key
		}
		tree := g.tableSnapshot[g.ss]
		iter := tree.MakeIter()

		// From here on, the use of resumingInSameSpan is just a performance
		// optimization to deal with the interface limitation of btree that
		// prevents us from specifying an exclusive start key. We need to check
		// that the lock is not the same as our exclusive start key and only need
		// to do that check once -- for the first lock.
		// 从这里开始，使用resumingInSameSpan只是一个性能优化，用来处理btree的接口限制，
		// 这个限制阻止我们指定一个独占的开始键。我们需要检查锁是否与我们的独占启动键不一样，
		// 并且只需要做一次检查——对于第一个锁。
		ltRange := &lockState{key: startKey, endKey: span.EndKey}
		for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
			l := iter.Cur()
			if resumingInSameSpan {
				resumingInSameSpan = false
				if l.key.Equal(startKey) {
					// This lock is where it stopped waiting.
					continue
				}
				// Else, past the lock where it stopped waiting. We may not
				// encounter that lock since it may have been garbage collected.
			}
			if l.tryActiveWait(g, g.sa, notify) {
				return
			}
		}
		resumingInSameSpan = false
		span = stepToNextSpan(g)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.state = waitingState{kind: doneWaiting}
	if notify {
		g.notify()
	}
}

// Waiting writers in a lockState are wrapped in a queuedGuard. A waiting
// writer is typically waiting in an active state, i.e., the
// lockTableGuardImpl.key refers to this lockState. However, breaking of
// reservations (see the comment on reservations below, in lockState) can
// cause a writer to be an inactive waiter.
//处于锁定状态的等待写入器被包装在queuedGuard中。等待写入器通常处于活动状态，即lockTableGuardImpl。
//key指的是这个锁定状态。然而，打破reservations(见下面关于reservations的评论，在lockState)会
//让writer变成不活跃的waiter。
type queuedGuard struct {
	guard  *lockTableGuardImpl
	active bool // protected by lockState.mu
}

// Information about a lock holder.
type lockHolderInfo struct {
	// nil if there is no holder. Else this is the TxnMeta of the latest call to
	// acquire/update the lock by this transaction. For a given transaction if
	// the lock is continuously held by a succession of different TxnMetas, the
	// epoch must be monotonic and the ts (derived from txn.WriteTimestamp for
	// some calls, and request.ts for other calls) must be monotonic. After ts
	// is intialized, the timestamps inside txn are not used.
	txn *enginepb.TxnMeta

	// All the TxnSeqs in the current epoch at which this lock has been
	// acquired. In increasing order. We track these so that if a lock is
	// acquired at both seq 5 and seq 7, rollback of 7 does not cause the lock
	// to be released. This is also consistent with PostgreSQL semantics
	// https://www.postgresql.org/docs/12/sql-select.html#SQL-FOR-UPDATE-SHARE
	seqs []enginepb.TxnSeq

	// The timestamp at which the lock is held.
	ts hlc.Timestamp
}

func (lh *lockHolderInfo) isEmpty() bool {
	return lh.txn == nil && lh.seqs == nil && lh.ts.IsEmpty()
}

// Per lock state in lockTableImpl.
//
// NOTE: we can't easily pool lockState objects without some form of reference
// counting because they are used as elements in a copy-on-write btree and may
// still be referenced by clones of the tree even when deleted from the primary.
// However, other objects referenced by lockState can be pooled as long as they
// are removed from all lockStates that reference them first.
type lockState struct {
	id     uint64 // needed for implementing util/interval/generic type contract
	endKey []byte // used in btree iteration and tests

	// The key being locked and the scope of that key. This state is never
	// mutated.
	key roachpb.Key
	ss  spanset.SpanScope

	mu syncutil.Mutex // Protects everything below.

	// Invariant summary (see detailed comments below):
	// - both holder.locked and waitQ.reservation != nil cannot be true.
	// - if holder.locked and multiple holderInfos have txn != nil: all the
	//   txns must have the same txn.ID.
	// - !holder.locked => waitingReaders.Len() == 0. That is, readers wait
	//   only if the lock is held. They do not wait for a reservation.
	// - If reservation != nil, that request is not in queuedWriters.

	// Information about whether the lock is held and the holder. We track
	// information for each durability level separately since a transaction can
	// go through multiple epochs and TxnSeq and may acquire the same lock in
	// replicated and unreplicated mode at different stages.
	holder struct {
		locked bool
		// LockStrength is always Exclusive
		holder [lock.MaxDurability + 1]lockHolderInfo
	}

	// Information about the requests waiting on the lock.
	lockWaitQueue
}

type lockWaitQueue struct {
	// Reservations:
	//
	// A not-held lock can be "reserved". A reservation is just a claim that
	// prevents multiple requests from racing when the lock is released. A
	// reservation by req2 can be broken by req1 is req1 has a smaller seqNum
	// than req2. Only requests that specify SpanReadWrite for a key can make
	// reservations. This means a reservation can only be made when the lock is
	// not held, since the reservation (which can acquire an Exclusive lock) and
	// the lock holder (which is an Exclusive lock) conflict.
	//未被持有的锁可以被“保留”。预约只是在锁被释放时防止多个请求竞争的声明。req1可以打破req2的预订，
	//因为req1的seqNum小于req2。只有为键指定SpanReadWrite的请求才能进行预约。
	//这意味着只有在锁未被持有时才能进行预约，因为预约(可以获得排他锁)和锁持有者(排他锁)发生冲突。

	// Read reservations are not permitted due to the complexities discussed in
	// the review for #43740. Additionally, reads do not queue for their turn at
	// all -- they are held in the waitingReaders list while the lock is held
	// and removed when the lock is not released, so they race with
	// reservations. Let us consider scenarios where reads did wait in the same
	// queue: the lock could be held or reserved by a write at ts=20, followed
	// by a waiting writer at ts=18, writer at ts=10, reader at ts=12. That
	// reader is waiting not because of a conflict with the holder, or reserver,
	// or the first waiter, but because there is a waiter ahead of it which it
	// conflicts with. This introduces more complexity in tracking who this
	// reader should push. Also consider a scenario where a reader did not wait
	// in the queue and waited on the side like in waitingReaders but acquired a
	// read reservation (together with other readers) when the lock was
	// released. Ignoring the unfairness of this, we can construct a deadlock
	// scenario with request req1 with seqnum 1 and req2 with seqnum 2 where
	// req1 and req2 both want to write at one key and so get ordered by their
	// seqnums but at another key req2 wants to read and req1 wants to write and
	// since req2 does not wait in the queue it acquires a read reservation
	// before req1. See the discussion at the end of this comment section on how
	// the behavior will extend when we start supporting Shared and Upgrade
	// locks.
	//
	// Non-transactional requests can do both reads and writes but cannot be
	// depended on since they don't have a transaction that can be pushed.
	// Therefore they not only do not acquire locks, but cannot make reservations.
	// The non-reservation for reads is already covered in the previous
	// paragraph. For non-transactional writes, the request waits in the queue
	// with other writers. The difference occurs:
	// - when it gets to the front of the queue and there is no lock holder
	//   or reservation: instead of acquiring the reservation it removes
	//   itself from the lockState and proceeds to the next lock. If it
	//   does not need to wait for any more locks and manages to acquire
	//   latches before those locks are acquired by some other request, it
	//   will evaluate.
	// - when deciding to wait at a lock: if the lock has a reservation with
	//   a sequence num higher than this non-transactional request it will
	//   ignore that reservation. Note that ignoring such reservations is
	//   safe since when this non-transactional request is holding latches
	//   those reservation holders cannot be holding latches, so they cannot
	//   conflict.
	//
	// Multiple requests from the same transaction wait independently, including
	// the situation where one of the requests has a reservation and the other
	// is waiting (currently this can only happen if both requests are doing
	// SpanReadWrite). Making multiple requests from the same transaction
	// jointly hold the reservation introduces code complexity since joint
	// reservations can be partially broken (see deadlock example below), and is
	// not necessarily fair to other requests. Additionally, if req1 from txn1
	// is holding a a reservation and req2 from txn1 is waiting, they must
	// conflict wrt latches and cannot evaluate concurrently so there isn't a
	// benefit to joint reservations. However, if one of the requests acquires
	// the lock the other request no longer needs to wait on this lock. This
	// situation motivates the waitSelf state.
	//
	// Deadlock example if joint reservations were supported and we did not
	// allow partial breaking of such reservations:
	//
	// - Keys are A, B, C, D.
	// - Key D is locked by some random txn.
	// - req1 from txn1 writes A, B, D. It waits at D.
	// - Some other request from some random txn that writes C arrives,
	//   evaluates, and locks C.
	// - req2 from txn2 that writes A, C. It waits at C.
	// - Some other request from some random txn that writes A arrives,
	//   evaluates, and locks A.
	// - req3 from txn1 that writes A, C. It waits at A. Note that req1 and req3
	//   are from the same txn.
	// - A is unlocked. req3 reserves A and waits at C behind req2.
	// - B is locked by some random txn.
	// - D is unlocked. req1 reserves D and proceeds to scan again and finds A
	//   is reserved by req3 which is the same txn so becomes a joint
	//   reservation holder at A.
	// - Since B is locked, req1 waits at B.
	// - C is unlocked. req2 reserves C. It scans and finds req1+req3 holding
	//   the joint reservation at A. If it queues behind this joint reservation
	//   we have the following situation:
	//        reservation   waiter
	//   A     req1+req3     req2
	//   C       req2        req3
	//   This is a deadlock caused by the lock table unless req2 partially
	//   breaks the reservation at A.
	//
	// Extension for Shared and Upgrade locks:
	// There are 3 aspects to consider: holders; reservers; the dependencies
	// that need to be captured when waiting.
	//
	// - Holders: only shared locks are compatible with themselves, so there can
	//   be one of (a) no holder (b) multiple shared lock holders, (c) one
	//   exclusive holder, (d) one upgrade holder. Non-locking reads will
	//   wait in waitingReaders for only an incompatible exclusive holder.
	//
	// - Reservers: This follows the same pattern as holders. Non-locking reads
	//   do not wait on reservers.
	//
	// - Queueing and dependencies: All potential lockers and non-transactional
	//   writers will wait in the same queue. A sequence of consecutive requests
	//   that have the potential to acquire a shared lock will jointly reserve
	//   that shared lock. Such requests cannot jump ahead of requests with a
	//   lower seqnum just because there is currently a shared lock reservation
	//   (this can cause lockTable induced deadlocks). Such joint reservations
	//   can be partially broken by a waiter desiring an exclusive or upgrade
	//   lock. Like the current code, non-transactional writes will wait for
	//   reservations that have a lower sequence num, but not make their own
	//   reservation. Additionally, they can partially break joint reservations.
	//
	//   Reservations that are (partially or fully) broken cause requests to
	//   reenter the queue as inactive waiters. This is no different than the
	//   current behavior. Each request can specify the same key in spans for
	//   ReadOnly, ReadShared, ReadUpgrade, ReadWrite. The spans will be
	//   iterated over in decreasing order of strength, to only wait at a lock
	//   at the highest strength (this is similar to the current behavior using
	//   accessDecreasingStrength).
	//
	//   For dependencies, a waiter desiring an exclusive or upgrade lock always
	//   conflicts with the holder(s) or reserver(s) so that is the dependency
	//   that will be captured. A waiter desiring a shared lock may encounter a
	//   situation where it does not conflict with the holder(s) or reserver(s)
	//   since those are also shared lockers. In that case it will depend on the
	//   first waiter since that waiter must be desiring a lock that is
	//   incompatible with a shared lock.

	reservation *lockTableGuardImpl

	// TODO(sbhola): There are a number of places where we iterate over these
	// lists looking for something, as described below. If some of these turn
	// out to be inefficient, consider better data-structures. One idea is that
	// for cases that find a particular guard the lockTableGuardImpl.locks can be
	// a map instead of a set to point directly to the *list.Element.
	//
	// queuedWriters:
	// - to find all active queuedWriters.
	// - to find the first active writer to make it distinguished.
	// - to find a particular guard.
	// - to find the position, based on seqNum, for inserting a particular guard.
	// - to find all waiting writers with a particular txn ID.
	//
	// waitingReaders:
	// - readers with a higher timestamp than some timestamp.
	// - to find a particular guard.

	// Waiters: An active waiter needs to be notified about changes in who it is
	// waiting for.

	// List of *queuedGuard. A subset of these are actively waiting. If
	// non-empty, either the lock is held or there is a reservation.
	queuedWriters list.List

	// List of *lockTableGuardImpl. All of these are actively waiting. If
	// non-empty, the lock must be held. By definition these cannot be in
	// waitSelf state since that state is only used when there is a reservation.
	waitingReaders list.List

	// If there is a non-empty set of active waiters that are not waitSelf, then
	// at least one must be distinguished.
	distinguishedWaiter *lockTableGuardImpl
}

//go:generate ../../util/interval/generic/gen.sh *lockState concurrency

// Methods required by util/interval/generic type contract.
func (l *lockState) ID() uint64         { return l.id }
func (l *lockState) Key() []byte        { return l.key }
func (l *lockState) EndKey() []byte     { return l.endKey }
func (l *lockState) New() *lockState    { return new(lockState) }
func (l *lockState) SetID(v uint64)     { l.id = v }
func (l *lockState) SetKey(v []byte)    { l.key = v }
func (l *lockState) SetEndKey(v []byte) { l.endKey = v }

// REQUIRES: l.mu is locked.
func (l *lockState) String() string {
	var buf strings.Builder
	l.Format(&buf)
	return buf.String()
}

// REQUIRES: l.mu is locked.
func (l *lockState) Format(buf *strings.Builder) {
	fmt.Fprintf(buf, " lock: %s\n", l.key)
	if l.isEmptyLock() {
		fmt.Fprintln(buf, "  empty")
		return
	}
	writeResInfo := func(b *strings.Builder, txn *enginepb.TxnMeta, ts hlc.Timestamp) {
		// TODO(sbhola): strip the leading 0 bytes from the UUID string since tests are assigning
		// UUIDs using a counter and makes this output more readable.
		fmt.Fprintf(b, "txn: %v, ts: %v, seq: %v\n", txn.ID, ts, txn.Sequence)
	}
	writeHolderInfo := func(b *strings.Builder, txn *enginepb.TxnMeta, ts hlc.Timestamp) {
		fmt.Fprintf(b, "  holder: txn: %v, ts: %v, info: ", txn.ID, ts)
		first := true
		for i := range l.holder.holder {
			h := &l.holder.holder[i]
			if h.txn == nil {
				continue
			}
			if !first {
				fmt.Fprintf(b, ", ")
			}
			first = false
			if lock.Durability(i) == lock.Replicated {
				fmt.Fprintf(b, "repl ")
			} else {
				fmt.Fprintf(b, "unrepl ")
			}
			fmt.Fprintf(b, "epoch: %d, seqs: [%d", h.txn.Epoch, h.seqs[0])
			for j := 1; j < len(h.seqs); j++ {
				fmt.Fprintf(b, ", %d", h.seqs[j])
			}
			fmt.Fprintf(b, "]")
		}
		fmt.Fprintln(b, "")
	}
	txn, ts := l.getLockHolder()
	if txn == nil {
		fmt.Fprintf(buf, "  res: req: %d, ", l.reservation.seqNum)
		writeResInfo(buf, l.reservation.txn, l.reservation.writeTS)
	} else {
		writeHolderInfo(buf, txn, ts)
	}
	// TODO(sumeer): Add an optional `description string` field to Request and
	// lockTableGuardImpl that tests can set to avoid relying on the seqNum to
	// identify requests.
	if l.waitingReaders.Len() > 0 {
		fmt.Fprintln(buf, "   waiting readers:")
		for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
			g := e.Value.(*lockTableGuardImpl)
			fmt.Fprintf(buf, "    req: %d, txn: ", g.seqNum)
			if g.txn == nil {
				fmt.Fprintln(buf, "none")
			} else {
				fmt.Fprintf(buf, "%v\n", g.txn.ID)
			}
		}
	}
	if l.queuedWriters.Len() > 0 {
		fmt.Fprintln(buf, "   queued writers:")
		for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
			qg := e.Value.(*queuedGuard)
			g := qg.guard
			fmt.Fprintf(buf, "    active: %t req: %d, txn: ",
				qg.active, qg.guard.seqNum)
			if g.txn == nil {
				fmt.Fprintln(buf, "none")
			} else {
				fmt.Fprintf(buf, "%v\n", g.txn.ID)
			}
		}
	}
	if l.distinguishedWaiter != nil {
		fmt.Fprintf(buf, "   distinguished req: %d\n", l.distinguishedWaiter.seqNum)
	}
}

//LockStateInfo 检查该lockstate是否被目标事务持有，及该lockstate持有的锁的durability
func (l *lockState) LockStateInfo(span roachpb.Span, txnID string) (bool, lock.Durability) {
	if l.isEmptyLock() {
		return false, -1
	}
	if l.key.Compare(span.Key) < 0 || l.key.Compare(span.EndKey) > 0 {
		return false, -1
	}
	index := lock.Replicated
	if l.holder.holder[index].txn == nil || (l.holder.holder[lock.Unreplicated].txn != nil &&
		l.holder.holder[lock.Unreplicated].ts.Less(l.holder.holder[lock.Replicated].ts)) {
		index = lock.Unreplicated
	}
	holder := l.holder.holder[index].txn

	if holder.ID.String() != txnID {
		return false, -1
	}
	return true, index
}

// Called for a write request when there is a reservation. Returns true iff it
// succeeds.
// REQUIRES: l.mu is locked.
func (l *lockState) tryBreakReservation(seqNum uint64) bool {
	if l.reservation.seqNum > seqNum {
		qg := &queuedGuard{
			guard:  l.reservation,
			active: false,
		}
		l.queuedWriters.PushFront(qg)
		l.reservation = nil
		return true
	}
	return false
}

// Informs active waiters about reservation or lock holder. The reservation
// may have changed so this needs to fix any inconsistencies wrt waitSelf and
// waitForDistinguished states.
// REQUIRES: l.mu is locked.
func (l *lockState) informActiveWaiters() {
	waitForState := waitingState{kind: waitFor, key: l.key}
	findDistinguished := l.distinguishedWaiter == nil
	if lockHolderTxn, _ := l.getLockHolder(); lockHolderTxn != nil {
		waitForState.txn = lockHolderTxn
		waitForState.held = true
	} else {
		waitForState.txn = l.reservation.txn
		if !findDistinguished && l.distinguishedWaiter.isSameTxnAsReservation(waitForState) {
			findDistinguished = true
			l.distinguishedWaiter = nil
		}
	}

	for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
		state := waitForState
		state.guardAccess = spanset.SpanReadOnly
		// Since there are waiting readers we could not have transitioned out of
		// or into a state with a reservation, since readers do not wait for
		// reservations.
		g := e.Value.(*lockTableGuardImpl)
		if findDistinguished {
			l.distinguishedWaiter = g
			findDistinguished = false
		}
		g.mu.Lock()
		g.mu.state = state
		if l.distinguishedWaiter == g {
			g.mu.state.kind = waitForDistinguished
		}
		g.notify()
		g.mu.Unlock()
	}
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		if !qg.active {
			continue
		}
		g := qg.guard
		var state waitingState
		if g.isSameTxnAsReservation(waitForState) {
			state = waitingState{kind: waitSelf}
		} else {
			state = waitForState
			state.guardAccess = spanset.SpanReadWrite
			if findDistinguished {
				l.distinguishedWaiter = g
				findDistinguished = false
			}
			if l.distinguishedWaiter == g {
				state.kind = waitForDistinguished
			}
		}
		g.mu.Lock()
		g.mu.state = state
		g.notify()
		g.mu.Unlock()
	}
}

// releaseWritersFromTxn removes all waiting writers for the lockState that are
// part of the specified transaction.
// REQUIRES: l.mu is locked.
func (l *lockState) releaseWritersFromTxn(txn *enginepb.TxnMeta) {
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		curr := e
		e = e.Next()
		g := qg.guard
		if g.isSameTxn(txn) {
			if qg.active {
				if g == l.distinguishedWaiter {
					l.distinguishedWaiter = nil
				}
				g.doneWaitingAtLock(false, l)
			} else {
				g.mu.Lock()
				delete(g.mu.locks, l)
				g.mu.Unlock()
			}
			l.queuedWriters.Remove(curr)
		}
	}
}

// When the active waiters have shrunk and the distinguished waiter has gone,
// try to make a new distinguished waiter if there is at least 1 active
// waiter.
// REQUIRES: l.mu is locked.
func (l *lockState) tryMakeNewDistinguished() {
	var g *lockTableGuardImpl
	if l.waitingReaders.Len() > 0 {
		g = l.waitingReaders.Front().Value.(*lockTableGuardImpl)
	} else if l.queuedWriters.Len() > 0 {
		for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
			qg := e.Value.(*queuedGuard)
			if qg.active && (l.reservation == nil || !qg.guard.isSameTxn(l.reservation.txn)) {
				g = qg.guard
				break
			}
		}
	}
	if g != nil {
		l.distinguishedWaiter = g
		g.mu.Lock()
		g.mu.state.kind = waitForDistinguished
		// The rest of g.state is already up-to-date.
		g.notify()
		g.mu.Unlock()
	}
}

// Returns true iff the lockState is empty, i.e., there is no lock holder or
// reservation.
// REQUIRES: l.mu is locked.
func (l *lockState) isEmptyLock() bool {
	if !l.holder.locked && l.reservation == nil {
		for i := range l.holder.holder {
			if !l.holder.holder[i].isEmpty() {
				panic("lockState with !locked but non-zero lockHolderInfo")
			}
		}
		if l.waitingReaders.Len() > 0 || l.queuedWriters.Len() > 0 {
			panic("lockState with waiters but no holder or reservation")
		}
		return true
	}
	return false
}

// Returns true iff the lock is currently held by the transaction with the
// given id.
// REQUIRES: l.mu is locked.
func (l *lockState) isLockedBy(id uuid.UUID) bool {
	if l.holder.locked {
		var holderID uuid.UUID
		if l.holder.holder[lock.Unreplicated].txn != nil {
			holderID = l.holder.holder[lock.Unreplicated].txn.ID
		} else {
			holderID = l.holder.holder[lock.Replicated].txn.ID
		}
		return id == holderID
	}
	return false
}

// Returns information about the current lock holder if the lock is held, else
// returns nil.
// REQUIRES: l.mu is locked.
func (l *lockState) getLockHolder() (*enginepb.TxnMeta, hlc.Timestamp) {
	if !l.holder.locked {
		return nil, hlc.Timestamp{}
	}

	// If the lock is held as both replicated and unreplicated we want to
	// provide the lower of the two timestamps, since the lower timestamp
	// contends with more transactions. Else we provide whichever one it is held
	// at.

	// Start with the assumption that it is held as replicated.
	index := lock.Replicated
	// Condition under which we prefer the unreplicated holder.
	if l.holder.holder[index].txn == nil || (l.holder.holder[lock.Unreplicated].txn != nil &&
		// If we are evaluating the following clause we are sure that it is held
		// as both replicated and unreplicated.
		l.holder.holder[lock.Unreplicated].ts.Less(l.holder.holder[lock.Replicated].ts)) {
		index = lock.Unreplicated
	}
	return l.holder.holder[index].txn, l.holder.holder[index].ts
}

// Removes the current lock holder from the lock.
// REQUIRES: l.mu is locked.
func (l *lockState) clearLockHolder() {
	l.holder.locked = false
	for i := range l.holder.holder {
		l.holder.holder[i] = lockHolderInfo{}
	}
}

// Decides whether the request g with access sa should actively wait at this
// lock and if yes, adjusts the data-structures appropriately. The notify
// parameter is true iff the request's new state channel should be notified --
// it is set to false when the call to tryActiveWait is happening due to an
// event for a different request or transaction (like a lock release) since in
// that case the channel is notified first and the call to tryActiveWait()
// happens later in lockTableGuard.CurState(). The return value is true iff
// it is actively waiting.
// 决定具有访问权限sa的请求g是否应主动等待此锁定，如果是，则适当地调整数据结构。
// 如果应通知请求的新状态通道，则notify参数为true －由于不同请求或事务（例如锁定释放）
// 的事件而发生tryActiveActiveWait调用时，将其设置为false，因为在这种情况下，
// 通道会先通知，然后在lockTableGuard.CurState（）中调用tryActiveWait（）。
// 如果正在积极等待，则返回值为true。
// Acquires l.mu, g.mu.
func (l *lockState) tryActiveWait(g *lockTableGuardImpl, sa spanset.SpanAccess, notify bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// It is possible that this lock is empty and has not yet been deleted.
	if l.isEmptyLock() {
		return false
	}

	// Lock is not empty.
	lockHolderTxn, lockHolderTS := l.getLockHolder()
	if lockHolderTxn != nil && g.isSameTxn(lockHolderTxn) {
		// Already locked by this txn.
		return false
	}

	if sa == spanset.SpanReadOnly {
		if lockHolderTxn == nil {
			// Reads only care about locker, not a reservation.
			// 读只关心锁，不关心保留。
			return false
		}
		// Locked by some other txn.
		if g.readTS.Less(lockHolderTS) {
			return false
		}
		g.mu.Lock()
		_, alsoHasStrongerAccess := g.mu.locks[l]
		g.mu.Unlock()

		// If the request already has this lock in its locks map, it must also be
		// writing to this key and must be either a reservation holder or inactive
		// waiter at this lock. The former has already been handled above. For the
		// latter, it must have had its reservation broken. Since this is a weaker
		// access we defer to the stronger access and don't wait here.

		//
		// For non-transactional requests that have the key specified as both
		// SpanReadOnly and SpanReadWrite, the request never acquires a
		// reservation, so using the locks map to detect this duplication of the
		// key is not possible. In the rare case, the lock is now held at a
		// timestamp that is not compatible with this request and it will wait
		// here -- there is no correctness issue with doing that.
		// 如果请求的locks映射中已经有这个锁，那么它也必须写入这个键，
		// 并且必须是这个锁上的保留持有者或不活动的waiter。前者已经在上面处理过了。
		// 对于后者，它的保留肯定被打破了。由于这是一个较弱的途径，我们遵从较强的途径，
		// 而不是在这里等待。对于同时将密钥指定为SpanReadOnly和SpanReadWrite的非事务性请求，
		// 请求永远不会获得保留，因此不可能使用locks映射来检测密钥的复制。
		// 在极少数情况下，锁现在持有的时间戳与此请求不兼容，它将在这里等待——这样做没有正确性问题。
		if alsoHasStrongerAccess {
			return false
		}
	}

	waitForState := waitingState{kind: waitFor, key: l.key}
	if lockHolderTxn != nil {
		waitForState.txn = lockHolderTxn
		waitForState.held = true
	} else {
		if l.reservation == g {
			// Already reserved by this request.
			// 已被此请求保留。
			return false
		}
		// A non-transactional write request never makes or breaks reservations,
		// and only waits for a reservation if the reservation has a lower
		// seqNum. Note that `sa == spanset.SpanRead && lockHolderTxn == nil`
		// was already checked above.
		// 注意‘sa == spanset’。上面已经检查了SpanRead && lockHolderTxn == nil。
		if g.txn == nil && l.reservation.seqNum > g.seqNum {
			// Reservation is held by a request with a higher seqNum and g is a
			// non-transactional request. Ignore the reservation.
			return false
		}
		waitForState.txn = l.reservation.txn
	}

	// Incompatible with whoever is holding lock or reservation.

	if l.reservation != nil && sa == spanset.SpanReadWrite && l.tryBreakReservation(g.seqNum) {
		l.reservation = g
		g.mu.Lock()
		g.mu.locks[l] = struct{}{}
		g.mu.Unlock()
		// There cannot be waitingReaders, since they do not wait for
		// reservations. And the set of active queuedWriters has not changed, but
		// they do need to be told about the change in who they are waiting for.
		l.informActiveWaiters()
		return false
	}

	// Need to wait.

	g.mu.Lock()
	defer g.mu.Unlock()
	if sa == spanset.SpanReadWrite {
		if _, inQueue := g.mu.locks[l]; inQueue {
			// Already in queue and must be in the right position, so mark as active
			// waiter there. We expect this to be rare.
			var qg *queuedGuard
			for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
				qqg := e.Value.(*queuedGuard)
				if qqg.guard == g {
					qg = qqg
					break
				}
			}
			if qg == nil {
				panic("lockTable bug")
			}
			qg.active = true
		} else {
			// Not in queue so insert as active waiter.
			qg := &queuedGuard{
				guard:  g,
				active: true,
			}
			if l.queuedWriters.Len() == 0 {
				l.queuedWriters.PushFront(qg)
			} else {
				var e *list.Element
				for e = l.queuedWriters.Back(); e != nil; e = e.Prev() {
					qqg := e.Value.(*queuedGuard)
					if qqg.guard.seqNum < qg.guard.seqNum {
						break
					}
				}
				if e == nil {
					l.queuedWriters.PushFront(qg)
				} else {
					l.queuedWriters.InsertAfter(qg, e)
				}
			}
			g.mu.locks[l] = struct{}{}
		}
	} else {
		l.waitingReaders.PushFront(g)
		g.mu.locks[l] = struct{}{}
	}
	// Make it an active waiter.
	g.key = l.key
	g.mu.startWait = true
	if g.isSameTxnAsReservation(waitForState) {
		g.mu.state = waitingState{kind: waitSelf}
	} else {
		state := waitForState
		state.guardAccess = sa
		if l.distinguishedWaiter == nil {
			l.distinguishedWaiter = g
			state.kind = waitForDistinguished
		}
		g.mu.state = state
	}
	if notify {
		g.notify()
	}
	return true
}

// Acquires this lock. Returns the list of guards that are done actively
// waiting at this key -- these will be requests from the same transaction
// that is acquiring the lock
//.获取此锁。返回在该键处主动等待的guard的列表——这些将是来自获取锁的同一事务的请求。
// Acquires l.mu.
func (l *lockState) acquireLock(
	_ lock.Strength, durability lock.Durability, txn *enginepb.TxnMeta, ts hlc.Timestamp,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder.locked {
		// Already held.
		beforeTxn, beforeTs := l.getLockHolder()
		if txn.ID != beforeTxn.ID {
			return errors.AssertionFailedf("existing lock cannot be acquired by different transaction")
		}
		seqs := l.holder.holder[durability].seqs
		if l.holder.holder[durability].txn != nil && l.holder.holder[durability].txn.Epoch < txn.Epoch {
			// Clear the sequences for the older epoch.
			seqs = seqs[:0]
		}
		if len(seqs) > 0 && seqs[len(seqs)-1] >= txn.Sequence {
			// Idempotent lock acquisition. In this case, we simply ignore the lock
			// acquisition as long as it corresponds to an existing sequence number.
			// If the sequence number is not being tracked yet, insert it into the
			// sequence history. The validity of such a lock re-acquisition should
			// have already been determined at the MVCC level.
			// 在本例中，只要锁获取对应于一个现有序列号，我们就简单地忽略它。
			// 如果还没有跟踪序列号，则将其插入到序列历史记录中。
			// 这种锁重新获取的有效性应该已经在MVCC级别上确定了。
			if i := sort.Search(len(seqs), func(i int) bool {
				return seqs[i] >= txn.Sequence
			}); i == len(seqs) {
				panic("lockTable bug - search value <= last element")
			} else if seqs[i] != txn.Sequence {
				seqs = append(seqs, 0)
				copy(seqs[i+1:], seqs[i:])
				seqs[i] = txn.Sequence
				l.holder.holder[durability].seqs = seqs
			}
			return nil
		}
		l.holder.holder[durability].txn = txn
		// Forward the lock's timestamp instead of assigning to it blindly.
		// While lock acquisition uses monotonically increasing timestamps
		// from the perspective of the transaction's coordinator, this does
		// not guarantee that a lock will never be acquired at a higher
		// epoch and/or sequence number but with a lower timestamp when in
		// the presence of transaction pushes. Consider the following
		// sequence of events:
		//
		//  - txn A acquires lock at sequence 1, ts 10
		//  - txn B pushes txn A to ts 20
		//  - txn B updates lock to ts 20
		//  - txn A's coordinator does not immediately learn of the push
		//  - txn A re-acquires lock at sequence 2, ts 15
		//
		// A lock's timestamp at a given durability level is not allowed to
		// regress, so by forwarding its timestamp during the second acquisition
		// instead if assigning to it blindly, it remains at 20.
		//
		// However, a lock's timestamp as reported by getLockHolder can regress
		// if it is acquired at a lower timestamp and a different durability
		// than it was previously held with. This is necessary to support
		// because the hard constraint which we must uphold here that the
		// lockHolderInfo for a replicated lock cannot diverge from the
		// replicated state machine in such a way that its timestamp in the
		// lockTable exceeds that in the replicated keyspace. If this invariant
		// were to be violated, we'd risk infinite lock-discovery loops for
		// requests that conflict with the lock as is written in the replicated
		// state machine but not as is reflected in the lockTable.
		//
		// Lock timestamp regressions are safe from the perspective of other
		// transactions because the request which re-acquired the lock at the
		// lower timestamp must have been holding a write latch at or below the
		// new lock's timestamp. This means that no conflicting requests could
		// be evaluating concurrently. Instead, all will need to re-scan the
		// lockTable once they acquire latches and will notice the reduced
		// timestamp at that point, which may cause them to conflict with the
		// lock even if they had not conflicted before. In a sense, it is no
		// different than the first time a lock is added to the lockTable.
		// 转发锁的时间戳，而不是盲目地分配给它。从事务协调器的角度来看，锁获取使用单调递增的时间戳，
		// 但这并不能保证锁永远不会在更高的历元和/或序列号上获得，
		// 而是在存在事务推送时使用更低的时间戳。考虑以下事件序列:
		//- txn A在序列1，ts10获取锁
		//- txn b将txn a 推到 ts20
		//- txn b更新锁定到 ts20
		//- txn A的协调器不会立即获知推送
		//- txn A在序列2，ts15重新获得锁

		// 给定持久性级别的锁的时间戳不允许返回，因此如果盲目地分配给它，
		// 通过在第二次获取期间转发它的时间戳，它仍然是20。

		// 但是，getLockHolder报告的锁的时间戳可以返回，如果它是以较低的时间戳获得的，
		// 并且持久性与之前持有的不同。这是必须支持的，因为我们在这里必须坚持的硬约束是，
		// 复制锁的lockHolderInfo不能以这样一种方式偏离复制状态机，
		// 即它在lockTable中的时间戳超过了在复制密钥空间中的时间戳。
		// 如果违反了这个不变量，对于与锁冲突的请求，我们将冒无限次的锁发现循环的风险，
		// 这种冲突是在复制状态机中写入的，而不是在lockTable中反映的。
		//
		// 锁时间戳回归从其他事务的角度来看是安全的，因为在较低的时间戳重新获得锁的请求必须在新锁的时间戳或以下持有写锁。
		// 这意味着不能同时评估冲突的请求。相反，一旦获得锁存，所有人都需要重新扫描lockTable，
		// 并注意到此时时间戳的减少，这可能导致它们与锁冲突，即使它们之前没有冲突。
		// 从某种意义上说，这与第一次将锁添加到lockTable没有什么不同。
		l.holder.holder[durability].ts.Forward(ts)
		l.holder.holder[durability].seqs = append(seqs, txn.Sequence)

		_, afterTs := l.getLockHolder()
		if beforeTs.Less(afterTs) {
			l.increasedLockTs(afterTs)
		}
		return nil
	}
	// Not already held, so may be reserved by this request. There is also the
	// possibility that some other request has broken this reservation because
	// of a concurrent release but that is harmless since this request is
	// holding latches and has proceeded to evaluation.\
	// 尚未held，因此可由此请求保留。还有一种可能是，
	// 其他一些请求由于并发释放而违反了这项保留，但这是无害的，
	// 因为该请求已被锁住并已开始评估。
	if l.reservation != nil {
		if l.reservation.txn.ID != txn.ID {
			// Reservation is broken.
			qg := &queuedGuard{
				guard:  l.reservation,
				active: false,
			}
			l.queuedWriters.PushFront(qg)
		} else {
			// Else, reservation is not broken, or broken by a different request
			// from the same transaction. In the latter case, both requests are not
			// actively waiting at this lock. We don't know which is in the queue
			// and which is holding the reservation but it does not matter. Both
			// will have their requestGuardImpl.mu.locks updated and neither will be
			// in the queue at the end of this method.
			// 否则，保留不会被破坏，或者被来自同一事务的不同请求破坏。在后一种情况下，
			// 这两个请求都没有在这个锁处主动等待。
			// 我们不知道哪个在排队，哪个在保留，但这无关紧要。
			// 双方都会有自己的requestGuardImpl.mu.locks已更新，并且在该方法结束时，两者都不在队列中。
			l.reservation.mu.Lock()
			delete(l.reservation.mu.locks, l)
			l.reservation.mu.Unlock()
		}
		if l.waitingReaders.Len() > 0 {
			panic("lockTable bug")
		}
	} else {
		if l.queuedWriters.Len() > 0 || l.waitingReaders.Len() > 0 {
			panic("lockTable bug")
		}
	}
	l.reservation = nil
	l.holder.locked = true
	l.holder.holder[durability].txn = txn
	l.holder.holder[durability].ts = ts
	l.holder.holder[durability].seqs = append([]enginepb.TxnSeq(nil), txn.Sequence)

	// If there are waiting requests from the same txn, they no longer need to wait.
	l.releaseWritersFromTxn(txn)

	// Inform active waiters since lock has transitioned to held.
	l.informActiveWaiters()
	return nil
}

// A replicated lock held by txn with timestamp ts was discovered by guard g
// where g is trying to access this key with access sa.
// Acquires l.mu.
func (l *lockState) discoveredLock(
	txn *enginepb.TxnMeta, ts hlc.Timestamp, g *lockTableGuardImpl, sa spanset.SpanAccess,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.holder.locked {
		if !l.isLockedBy(txn.ID) {
			return errors.AssertionFailedf(
				"discovered lock by different transaction than existing lock on %s",
				keys.PrettyPrint(nil, l.key),
			)
		}
	} else {
		l.holder.locked = true
	}
	holder := &l.holder.holder[lock.Replicated]
	if holder.txn == nil {
		holder.txn = txn
		holder.ts = ts
		holder.seqs = append(holder.seqs, txn.Sequence)
	}

	// Queue the existing reservation holder. Note that this reservation
	// holder may not be equal to g due to two reasons (a) the reservation
	// of g could have been broken even though g is holding latches (see
	// the comment in acquireLock()), (b) g may be a non-transactional
	// request (read or write) that can ignore the reservation.
	// 对现有保留持有者进行排队。请注意，由于两个原因，此保留持有者可能不等于g
	// (a) 即使g持有锁存，g的保留也可能被打破(见acquireLock()中的评论)，
	// (b) g可以是非事务性请求(读或写)，可以忽略保留。
	if l.reservation != nil {
		qg := &queuedGuard{
			guard:  l.reservation,
			active: false,
		}
		l.queuedWriters.PushFront(qg)
		l.reservation = nil
	}

	switch sa {
	case spanset.SpanReadOnly:
		// Don't enter the lock's queuedReaders list, because all queued readers
		// are expected to be active. Instead, wait until the next scan.

		// Confirm that the guard will wait on the lock the next time it scans
		// the lock table. If not then it shouldn't have discovered the lock in
		// the first place. Bugs here would cause infinite loops where the same
		// lock is repeatedly re-discovered.
		// 不要进入锁的queuedReaders列表，因为预期所有排队的读取器都是活动的。
		// 相反，等待下一次扫描。确认guard将在下一次扫描锁表时等待锁。
		// 如果不是，那么它就不应该首先发现锁。这里的bug会导致无限循环，重复发现相同的锁。
		if g.readTS.Less(ts) {
			if g.txn != nil && g.txn.IsolationLevel == util.ReadCommittedIsolation {
				g.readTS.Forward(ts.Next())
				g.writeTS.Forward(g.readTS)
			} else {
				return errors.AssertionFailedf("discovered non-conflicting lock")
			}
		}

	case spanset.SpanReadWrite:
		// Immediately enter the lock's queuedWriters list.
		g.mu.Lock()
		_, presentHere := g.mu.locks[l]
		if !presentHere {
			// Since g will place itself in queue as inactive waiter below.
			g.mu.locks[l] = struct{}{}
		}
		g.mu.Unlock()

		if !presentHere {
			// Put self in queue as inactive waiter.
			qg := &queuedGuard{
				guard:  g,
				active: false,
			}
			// g is not necessarily first in the queue in the (rare) case (a) above.
			var e *list.Element
			for e = l.queuedWriters.Front(); e != nil; e = e.Next() {
				qqg := e.Value.(*queuedGuard)
				if qqg.guard.seqNum > g.seqNum {
					break
				}
			}
			if e == nil {
				l.queuedWriters.PushBack(qg)
			} else {
				l.queuedWriters.InsertBefore(qg, e)
			}
		}
	}

	// If there are waiting requests from the same txn, they no longer need to wait.
	l.releaseWritersFromTxn(txn)

	// Active waiters need to be told about who they are waiting for.
	l.informActiveWaiters()
	return nil
}

// Acquires l.mu.
func (l *lockState) tryClearLock(force bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	replicatedHeld := l.holder.locked && l.holder.holder[lock.Replicated].txn != nil
	if replicatedHeld && l.distinguishedWaiter == nil && !force {
		// Replicated lock is held and has no distinguished waiter.
		return false
	}

	// Remove unreplicated holder.
	l.holder.holder[lock.Unreplicated] = lockHolderInfo{}
	var waitState waitingState
	if replicatedHeld && !force {
		lockHolderTxn, _ := l.getLockHolder()
		// Note that none of the current waiters can be requests
		// from lockHolderTxn.
		waitState = waitingState{
			kind:        waitElsewhere,
			txn:         lockHolderTxn,
			key:         l.key,
			held:        true,
			guardAccess: spanset.SpanReadOnly,
		}
	} else {
		l.clearLockHolder()
		waitState = waitingState{kind: doneWaiting}
	}

	l.distinguishedWaiter = nil
	if l.reservation != nil {
		g := l.reservation
		g.mu.Lock()
		delete(g.mu.locks, l)
		g.mu.Unlock()
		l.reservation = nil
	}
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*lockTableGuardImpl)
		curr := e
		e = e.Next()
		l.waitingReaders.Remove(curr)

		g.mu.Lock()
		g.mu.state = waitState
		g.notify()
		delete(g.mu.locks, l)
		g.mu.Unlock()
	}

	waitState.guardAccess = spanset.SpanReadWrite
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		curr := e
		e = e.Next()
		l.queuedWriters.Remove(curr)

		g := qg.guard
		g.mu.Lock()
		if qg.active {
			g.mu.state = waitState
			g.notify()
		}
		delete(g.mu.locks, l)
		g.mu.Unlock()
	}
	return true
}

// Removes the TxnSeqs in heldSeqNums that are contained in ignoredSeqNums.
// REQUIRES: ignoredSeqNums contains non-overlapping ranges and sorted in
// increasing seq order.
// 删除heldSeqNums中包含在ignoredSeqNums中的TxnSeqs。
// 要求: ignoredSeqNums包含不重叠的范围，并按递增的seq顺序排序。
func removeIgnored(
	heldSeqNums []enginepb.TxnSeq, ignoredSeqNums []enginepb.IgnoredSeqNumRange,
) []enginepb.TxnSeq {
	if len(ignoredSeqNums) == 0 {
		return heldSeqNums
	}
	held := heldSeqNums[:0]
	for _, n := range heldSeqNums {
		i := sort.Search(len(ignoredSeqNums), func(i int) bool { return ignoredSeqNums[i].End >= n })
		if i == len(ignoredSeqNums) || ignoredSeqNums[i].Start > n {
			held = append(held, n)
		}
	}
	return held
}

// Tries to update the lock: noop if this lock is held by a different
// transaction, else the lock is updated. Returns whether the lockState can be
// garbage collected
// .尝试更新锁:noop，如果这个锁被一个不同的事务持有，否则锁被更新。返回锁定状态是否可以被垃圾回收。
// Acquires l.mu.
func (l *lockState) tryUpdateLock(up *roachpb.LockUpdate) (gc bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isLockedBy(up.Txn.ID) {
		return false, nil
	}
	if up.Status.IsFinalized() {
		l.clearLockHolder()
		gc = l.lockIsFree()
		return gc, nil
	}

	txn := &up.Txn
	ts := up.Txn.WriteTimestamp
	_, beforeTs := l.getLockHolder()
	advancedTs := beforeTs.Less(ts)
	isLocked := false
	for i := range l.holder.holder {
		holder := &l.holder.holder[i]
		if holder.txn == nil {
			continue
		}
		// Note that mvccResolveWriteIntent() has special handling of the case
		// where the pusher is using an epoch lower than the epoch of the intent
		// (replicated lock), but is trying to push to a higher timestamp. The
		// replicated lock gets written with the newer epoch (not the epoch known
		// to the pusher) but a higher timestamp. Then the pusher will call into
		// this function with that lower epoch. Instead of trying to be consistent
		// with mvccResolveWriteIntent() in the current state of the replicated
		// lock we simply forget the replicated lock since it is no longer in the
		// way of this request. Eventually, once we have segregated locks, the
		// lock table will be the source of truth for replicated locks too, and
		// this forgetting behavior will go away.
		//
		// For unreplicated locks the lock table is the source of truth, so we
		// best-effort mirror the behavior of mvccResolveWriteIntent() by updating
		// the timestamp.
		//
		// 请注意，mvccResolveWriteIntent（）具有特殊的处理方式，
		// 即pusher使用的epoch低于意图的epoch（副本锁），但试图推送到更高的时间戳。
		// 副本锁使用更新的epoch（不是pusher已知的epoch）写入，而是使用更高的时间戳。
		// 然后推进器将用较低的epoch调用这个函数。
		// 我们不想在副本锁的当前状态下与mvccresolverewriteintent（）保持一致，
		// 我们只需遗忘副本锁，因为它不再是此请求的方式。最终，一旦我们分离了锁，
		// 锁表也将成为副本锁的真实来源，这种遗忘行为也将消失离开。
		// 为了unreplicatelocks锁表是单一数据源，
		// 因此我们尽量通过更新时间戳来镜像mvccresolverewriteintent（）的行为。
		if lock.Durability(i) == lock.Replicated || txn.Epoch > holder.txn.Epoch {
			*holder = lockHolderInfo{}
			continue
		}
		// Unreplicated lock held in same epoch or a higher epoch.
		if advancedTs {
			// We may advance ts here but not update the holder.txn object below
			// for the reason stated in the comment about mvccResolveWriteIntent().
			// The lockHolderInfo.ts is the source of truth regarding the timestamp
			// of the lock, and not TxnMeta.WriteTimestamp.
			holder.ts = ts
		}
		if txn.Epoch == holder.txn.Epoch {
			holder.seqs = removeIgnored(holder.seqs, up.IgnoredSeqNums)
			if len(holder.seqs) == 0 {
				*holder = lockHolderInfo{}
				continue
			}
			if advancedTs {
				holder.txn = txn
			}
		}
		// Else txn.Epoch < lockHolderTxn.Epoch, so only the timestamp has been
		// potentially updated.
		isLocked = true
	}

	if !isLocked {
		l.clearLockHolder()
		gc = l.lockIsFree()
		return gc, nil
	}

	if advancedTs {
		l.increasedLockTs(ts)
	}
	// Else no change for waiters. This can happen due to a race between different
	// callers of UpdateLocks().

	return false, nil
}

// The lock holder timestamp has increased. Some of the waiters may no longer
// need to wait.
// 锁持有者的时间戳增加了。一些waiter可能不再需要等待。
// REQUIRES: l.mu is locked.
func (l *lockState) increasedLockTs(newTs hlc.Timestamp) {
	distinguishedRemoved := false
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*lockTableGuardImpl)
		curr := e
		e = e.Next()
		if g.readTS.Less(newTs) {
			// Stop waiting.
			l.waitingReaders.Remove(curr)
			if g == l.distinguishedWaiter {
				distinguishedRemoved = true
				l.distinguishedWaiter = nil
			}
			g.doneWaitingAtLock(false, l)
		}
		// Else don't inform an active waiter which continues to be an active waiter
		// despite the timestamp increase.
		// 否则不要通知活动的waiter，尽管时间戳增加了，它仍然是活动的waiter。
	}
	if distinguishedRemoved {
		l.tryMakeNewDistinguished()
	}
}

// A request known to this lockState is done. The request could be a reserver,
// or waiting reader or writer. Acquires l.mu. Note that there is the
// possibility of a race and the g may no longer be known to l, which we treat
// as a noop (this race is allowed since we order l.mu > g.mu). Returns whether
// the lockState can be garbage collected.
// Acquires l.mu.
func (l *lockState) requestDone(g *lockTableGuardImpl) (gc bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	g.mu.Lock()
	if _, present := g.mu.locks[l]; !present {
		g.mu.Unlock()
		return false
	}
	delete(g.mu.locks, l)
	g.mu.Unlock()

	if l.reservation == g {
		l.reservation = nil
		return l.lockIsFree()
	}
	// May be in queuedWriters or waitingReaders.
	distinguishedRemoved := false
	doneRemoval := false
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		if qg.guard == g {
			l.queuedWriters.Remove(e)
			if qg.guard == l.distinguishedWaiter {
				distinguishedRemoved = true
				l.distinguishedWaiter = nil
			}
			doneRemoval = true
			break
		}
	}
	if !doneRemoval {
		for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
			gg := e.Value.(*lockTableGuardImpl)
			if gg == g {
				l.waitingReaders.Remove(e)
				if g == l.distinguishedWaiter {
					distinguishedRemoved = true
					l.distinguishedWaiter = nil
				}
				doneRemoval = true
				break
			}
		}
	}
	if !doneRemoval {
		panic("lockTable bug")
	}
	if distinguishedRemoved {
		l.tryMakeNewDistinguished()
	}
	return false
}

// tryFreeLockOnReplicatedAcquire attempts to free a write-uncontended lock
// during the state transition from the Unreplicated durability to the
// Replicated durability. This is possible because a Replicated lock is also
// stored as an MVCC intent, so it does not need to also be stored in the
// lockTable if writers are not queuing on it. This is beneficial because it
// serves as a mitigation for #49973. Since we aren't currently great at
// avoiding excessive contention on limited scans when locks are in the
// lockTable, it's better the keep locks out of the lockTable when possible.
//
// If any of the readers do truly contend with this lock even after their limit
// has been applied, they will notice during their MVCC scan and re-enter the
// queue (possibly recreating the lock through AddDiscoveredLock). Still, in
// practice this seems to work well in avoiding most of the artificial
// concurrency discussed in #49973.
// tryFreeLockOnReplicatedAcquire尝试在从未复制持久性到复制持久性的状态转换期间释放写非争用锁。
// 这是可能的，因为复制锁也是作为MVCC意图存储的，所以如果写入器不在lockTable上排队，
// 则不需要将其存储在lockTable中。这是有益的，因为它可以作为#49973的缓解。当锁在lockTable中时，我们目前还不能很好地避免有限扫描上的过度争用，因此最好在可能的情况下将锁放在lockTable之外。
// 如果任何读取器在其限制被应用之后仍然真正地与这个锁竞争，
// 它们将在其MVCC扫描期间注意到并重新进入队列(可能通过AddDiscoveredLock重新创建锁)。
// 不过，在实践中，这似乎可以很好地避免#49973中讨论的大多数人工并发。
// Acquires l.mu.
func (l *lockState) tryFreeLockOnReplicatedAcquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Bail if not locked with only the Unreplicated durability.
	if !l.holder.locked || l.holder.holder[lock.Replicated].txn != nil {
		return false
	}

	// rc事务需要把读锁留在内存中
	if txn := l.holder.holder[lock.Unreplicated].txn; txn != nil && txn.IsolationLevel == util.ReadCommittedIsolation {
		return false
	}

	// Bail if the lock has waiting writers. It is not uncontended.
	if l.queuedWriters.Len() != 0 {
		return false
	}

	// The lock is uncontended by other writers, so we're safe to drop it.
	// This may release readers who were waiting on the lock.
	l.clearLockHolder()
	gc := l.lockIsFree()
	if !gc {
		panic("expected lockIsFree to return true")
	}
	return true
}

// The lock has transitioned from locked/reserved to unlocked. There could be
// waiters, but there cannot be a reservation.
// REQUIRES: l.mu is locked.
func (l *lockState) lockIsFree() (gc bool) {
	if l.holder.locked {
		panic("called lockIsFree on lock with holder")
	}
	if l.reservation != nil {
		panic("called lockIsFree on lock with reservation")
	}

	// All waiting readers don't need to wait here anymore.
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*lockTableGuardImpl)
		curr := e
		e = e.Next()
		l.waitingReaders.Remove(curr)
		if g == l.distinguishedWaiter {
			l.distinguishedWaiter = nil
		}
		g.doneWaitingAtLock(false, l)
	}

	// The prefix of the queue that is non-transactional writers is done
	// waiting.
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		g := qg.guard
		if g.txn == nil {
			curr := e
			e = e.Next()
			l.queuedWriters.Remove(curr)
			if g == l.distinguishedWaiter {
				l.distinguishedWaiter = nil
			}
			g.doneWaitingAtLock(false, l)
		} else {
			break
		}
	}

	if l.queuedWriters.Len() == 0 {
		return true
	}

	// First waiting writer (it must be transactional) gets the reservation.
	e := l.queuedWriters.Front()
	qg := e.Value.(*queuedGuard)
	g := qg.guard
	l.reservation = g
	l.queuedWriters.Remove(e)
	if qg.active {
		if g == l.distinguishedWaiter {
			l.distinguishedWaiter = nil
		}
		g.doneWaitingAtLock(true, l)
	}
	// Else inactive waiter and is waiting elsewhere.

	// Tell the active waiters who they are waiting for.
	l.informActiveWaiters()
	return false
}

func (t *treeMu) nextLockSeqNum() uint64 {
	t.lockIDSeqNum++
	return t.lockIDSeqNum
}

// ScanAndEnqueue implements the lockTable interface.
func (t *lockTableImpl) ScanAndEnqueue(req Request, guard lockTableGuard) lockTableGuard {
	// NOTE: there is no need to synchronize with enabledMu here. ScanAndEnqueue
	// scans the lockTable and enters any conflicting lock wait-queues, but a
	// disabled lockTable will be empty. If the scan's btree snapshot races with
	// a concurrent call to clear/disable then it might enter some wait-queues,
	// but it will quickly be released from them.
	//注意:这里不需要与enabledMu同步。ScanAndEnqueue扫描锁表并进入任何冲突的锁等待队列，
	//但是禁用的锁表将为空。如果扫描的btree快照与要清除/禁用的并发调用竞争，
	//那么它可能会进入一些等待队列，但它会很快从这些队列中释放出来。

	var g *lockTableGuardImpl
	if guard == nil {
		g = newLockTableGuardImpl()
		g.seqNum = atomic.AddUint64(&t.seqNum, 1)
		g.txn = req.txnMeta()
		g.spans = req.LockSpans
		g.readTS = req.readConflictTimestamp()
		g.writeTS = req.writeConflictTimestamp()
		g.sa = spanset.NumSpanAccess - 1
		g.index = -1
	} else {
		g = guard.(*lockTableGuardImpl)
		g.key = nil
		g.sa = spanset.NumSpanAccess - 1
		g.ss = spanset.SpanScope(0)
		g.index = -1
		g.mu.Lock()
		g.mu.startWait = false
		g.mu.mustFindNextLockAfter = false
		g.mu.Unlock()
	}
	for ss := spanset.SpanScope(0); ss < spanset.NumSpanScope; ss++ {
		for sa := spanset.SpanAccess(0); sa < spanset.NumSpanAccess; sa++ {
			if len(g.spans.GetSpans(sa, ss)) > 0 {
				// Since the spans are constant for a request, every call to
				// ScanAndEnqueue for that request will execute the following code
				// for the same SpanScope(s). Any SpanScope for which this code does
				// not execute will always have an empty snapshot.
				// 由于spans对于一个请求是恒定的，
				// 因此每个针对该请求的ScanAndEnqueue调用都将针对相同的SpanScope执行以下代码。
				// 未执行此代码的任何SpanScope都将始终具有一个空快照。
				t.locks[ss].mu.RLock()
				g.tableSnapshot[ss].Reset()
				g.tableSnapshot[ss] = t.locks[ss].Clone()
				t.locks[ss].mu.RUnlock()
				break
			}
		}
	}
	g.findNextLockAfter(true /* notify */)
	return g
}

// Dequeue implements the lockTable interface.
func (t *lockTableImpl) Dequeue(guard lockTableGuard) {
	// NOTE: there is no need to synchronize with enabledMu here. Dequeue only
	// accesses state already held by the guard and does not add anything to the
	// lockTable.
	// 注意:这里不需要与enabledMu同步。Dequeue只访问guard已经持有的状态，不向lockTable添加任何内容。

	g := guard.(*lockTableGuardImpl)
	defer releaseLockTableGuardImpl(g)

	var candidateLocks []*lockState
	g.mu.Lock()
	for l := range g.mu.locks {
		candidateLocks = append(candidateLocks, l)
	}
	g.mu.Unlock()
	var locksToGC [spanset.NumSpanScope][]*lockState
	for _, l := range candidateLocks {
		if gc := l.requestDone(g); gc {
			locksToGC[l.ss] = append(locksToGC[l.ss], l)
		}
	}

	for i := 0; i < len(locksToGC); i++ {
		if len(locksToGC[i]) > 0 {
			t.tryGCLocks(&t.locks[i], locksToGC[i])
		}
	}
}

// AddDiscoveredLock implements the lockTable interface.
func (t *lockTableImpl) AddDiscoveredLock(
	intent *roachpb.Intent, seq roachpb.LeaseSequence, guard lockTableGuard,
) (added bool, _ error) {
	t.enabledMu.RLock()
	defer t.enabledMu.RUnlock()
	if !t.enabled {
		// If not enabled, don't track any locks.
		return false, nil
	}
	if seq < t.enabledSeq {
		// If the lease sequence is too low, this discovered lock may no longer
		// be accurate, so we ignore it. However, we still return true so that
		// the request immediately retries, this time under a newer lease.
		// 如果租约序列太低，这个发现的锁可能不再准确，所以我们忽略它。
		// 但是，我们仍然返回true，以便请求立即重试，这次是在较新的租约下。
		return true, nil
	} else if seq > t.enabledSeq {
		// The enableSeq is set synchronously with the application of a new
		// lease, so it should not be possible for a request to evaluate at a
		// higher lease sequence than the current value of enabledSeq.
		// enableSeq与新租约的应用程序同步设置，因此请求不可能以高于enabledSeq的当前值的租约序列进行计算。
		return false, errors.AssertionFailedf("unexpected lease sequence: %d > %d", seq, t.enabledSeq)
	}
	g := guard.(*lockTableGuardImpl)
	key := intent.Key
	sa, ss, err := findAccessInSpans(key, g.spans)
	if err != nil {
		return false, err
	}
	var l *lockState
	tree := &t.locks[ss]
	tree.mu.Lock()
	// Can't release tree.mu until call l.discoveredLock() since someone may
	// find an empty lock and remove it from the tree.
	defer tree.mu.Unlock()
	iter := tree.MakeIter()
	iter.FirstOverlap(&lockState{key: key})
	if !iter.Valid() {
		l = &lockState{id: tree.nextLockSeqNum(), key: key, ss: ss}
		l.queuedWriters.Init()
		l.waitingReaders.Init()
		tree.Set(l)
		atomic.AddInt64(&tree.numLocks, 1)
	} else {
		l = iter.Cur()
	}
	return true, l.discoveredLock(&intent.Txn, intent.Txn.WriteTimestamp, g, sa)
}

// AcquireLock implements the lockTable interface.
func (t *lockTableImpl) AcquireLock(
	txn *enginepb.TxnMeta, key roachpb.Key, strength lock.Strength, durability lock.Durability,
) error {
	t.enabledMu.RLock()
	defer t.enabledMu.RUnlock()
	if !t.enabled {
		// If not enabled, don't track any locks.
		return nil
	}
	if strength != lock.Exclusive {
		return errors.AssertionFailedf("lock strength not Exclusive")
	}
	ss := spanset.SpanGlobal
	if keys.IsLocal(key) {
		ss = spanset.SpanLocal
	}
	var l *lockState
	tree := &t.locks[ss]
	tree.mu.Lock()
	// Can't release tree.mu until call l.acquireLock() since someone may find
	// an empty lock and remove it from the tree. If we expect that lockState
	// will already be in tree we can optimize this by first trying with a
	// tree.mu.RLock().
	// 无法释放“树.mu“直到调用”l.acquireLock（）”，因为有人可能会找到一个空锁并将其从树中移除。
	// 如果我们预期lockState已经在树中，我们可以通过首先尝试使用树.mu.RLock().”
	iter := tree.MakeIter()
	iter.FirstOverlap(&lockState{key: key})
	if !iter.Valid() {
		if durability == lock.Replicated {
			// Don't remember uncontended replicated locks.
			tree.mu.Unlock()
			return nil
		}
		l = &lockState{id: tree.nextLockSeqNum(), key: key, ss: ss}
		tree.lockIDSeqNum++
		l.queuedWriters.Init()
		l.waitingReaders.Init()
		tree.Set(l)
		atomic.AddInt64(&tree.numLocks, 1)
	} else {
		l = iter.Cur()
		if durability == lock.Replicated && l.tryFreeLockOnReplicatedAcquire() {
			// Don't remember uncontended replicated locks. Just like in the
			// case where the lock is initially added as replicated, we drop
			// replicated locks from the lockTable when being upgraded from
			// Unreplicated to Replicated, whenever possible.
			// 就像锁最初作为复制添加的情况一样，当从未复制升级到复制时，
			// 我们会尽可能从锁表中删除复制的锁。
			tree.Delete(l)
			tree.mu.Unlock()
			atomic.AddInt64(&tree.numLocks, -1)
			return nil
		}
	}
	err := l.acquireLock(strength, durability, txn, txn.WriteTimestamp)
	tree.mu.Unlock()

	var totalLocks int64
	for i := 0; i < len(t.locks); i++ {
		totalLocks += atomic.LoadInt64(&t.locks[i].numLocks)
	}
	if totalLocks > t.maxLocks {
		t.tryClearLocks(false /* force */)
	}
	return err
}

// If force is false, removes all locks, except for those that are held with
// replicated durability and have no distinguished waiter, and tells those
// waiters to wait elsewhere or that they are done waiting. A replicated lock
// which has been discovered by a request but no request is actively waiting on
// it will be preserved since we need to tell that request who it is waiting for
// when it next calls ScanAndEnqueue(). If we aggressively removed even these
// locks, the next ScanAndEnqueue() would not find the lock, the request would
// evaluate again, again discover that lock and if tryClearLocks() keeps getting
// called would be stuck in this loop without pushing.
//
// If force is true, removes all locks and marks all guards as doneWaiting.
// 如果force为false，除去所有的锁，除了那些持久性相同且没有distinguished waiter的锁，
// 并告诉那些waiter在其他地方等待，或者他们已经结束等待。
// 一个已被请求发现但没有请求正在积极等待它的复制锁将被保留，因为我们需要告诉该请求，
// 当它下一次调用ScanAndEnqueue()时，它正在等待谁。
// 即使我们主动删除这些锁，下一个ScanAndEnqueue()也不会找到锁，请求会再次计算，
// 再次发现那个锁，如果tryClearLocks()一直被调用，就会在这个循环中被卡在没有推的情况下。
// 如果force为真，移除所有锁并标记所有guard变为doneWaiting。
func (t *lockTableImpl) tryClearLocks(force bool) {
	for i := 0; i < int(spanset.NumSpanScope); i++ {
		tree := &t.locks[i]
		tree.mu.Lock()
		var locksToClear []*lockState
		iter := tree.MakeIter()
		for iter.First(); iter.Valid(); iter.Next() {
			l := iter.Cur()
			if l.tryClearLock(force) {
				locksToClear = append(locksToClear, l)
			}
		}
		atomic.AddInt64(&tree.numLocks, int64(-len(locksToClear)))
		if tree.Len() == len(locksToClear) {
			// Fast-path full clear.
			tree.Reset()
		} else {
			for _, l := range locksToClear {
				tree.Delete(l)
			}
		}
		tree.mu.Unlock()
	}
}

// Given the key must be in spans, returns the strongest access
// specified in the spans, along with the scope of the key.
// 给定键必须位于span中，则返回span中指定的最强访问权限以及键的范围。
func findAccessInSpans(
	key roachpb.Key, spans *spanset.SpanSet,
) (spanset.SpanAccess, spanset.SpanScope, error) {
	ss := spanset.SpanGlobal
	if keys.IsLocal(key) {
		ss = spanset.SpanLocal
	}
	for sa := spanset.NumSpanAccess - 1; sa >= 0; sa-- {
		s := spans.GetSpans(sa, ss)
		// First span that starts after key
		i := sort.Search(len(s), func(i int) bool {
			return key.Compare(s[i].Key) < 0
		})
		if i > 0 &&
			((len(s[i-1].EndKey) > 0 && key.Compare(s[i-1].EndKey) < 0) || key.Equal(s[i-1].Key)) {
			return sa, ss, nil
		}
	}
	return 0, 0, errors.AssertionFailedf("could not find access in spans")
}

// Tries to GC locks that were previously known to have become empty.
func (t *lockTableImpl) tryGCLocks(tree *treeMu, locks []*lockState) {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	for _, l := range locks {
		iter := tree.MakeIter()
		iter.FirstOverlap(l)
		// Since the same lockState can go from non-empty to empty multiple times
		// it is possible that multiple threads are racing to delete it and
		// multiple find it empty and one wins. If a concurrent thread made the
		// lockState non-empty we do not want to delete it accidentally.
		// 由于同一个锁状态可以多次从非空变为空，因此可能有多个线程争着删除它，多个线程发现它为空，
		// 其中一个线程获胜。如果并发线程使锁状态非空，我们不想失误删除它。
		if !iter.Valid() {
			continue
		}
		l = iter.Cur()
		l.mu.Lock()
		empty := l.isEmptyLock()
		l.mu.Unlock()
		if empty {
			tree.Delete(l)
			atomic.AddInt64(&tree.numLocks, -1)
		}
	}
}

// UpdateLocks implements the lockTable interface.
func (t *lockTableImpl) UpdateLocks(up *roachpb.LockUpdate) error {
	// NOTE: there is no need to synchronize with enabledMu here. Update only
	// accesses locks already in the lockTable, but a disabled lockTable will be
	// empty. If the lock-table scan below races with a concurrent call to clear
	// then it might update a few locks, but they will quickly be cleared.
	// 注意:这里不需要与enabledMu同步。Update只访问锁表中已经存在的锁，
	// 但是禁用的锁表将为空。如果下面的锁表扫描与要清除的并发调用竞争，
	// 那么它可能会更新一些锁，但它们将很快被清除。

	span := up.Span
	ss := spanset.SpanGlobal
	if keys.IsLocal(span.Key) {
		ss = spanset.SpanLocal
	}
	tree := &t.locks[ss]
	var err error
	var locksToGC []*lockState
	changeFunc := func(l *lockState) {
		gc, err2 := l.tryUpdateLock(up)
		if err2 != nil {
			err = err2
			return
		}
		if gc {
			locksToGC = append(locksToGC, l)
		}
	}
	tree.mu.RLock()
	iter := tree.MakeIter()
	ltRange := &lockState{key: span.Key, endKey: span.EndKey}
	for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
		changeFunc(iter.Cur())
		// Optimization to avoid a second key comparison (not for correctness).
		if len(span.EndKey) == 0 {
			break
		}
	}
	tree.mu.RUnlock()

	if len(locksToGC) > 0 {
		t.tryGCLocks(tree, locksToGC)
	}
	return err
}

// Iteration helper for findNextLockAfter. Returns the next span to search
// over, or nil if the iteration is done.
// REQUIRES: g.mu is locked.
// findNextLockAfter的迭代助手。返回要搜索的下一个span，如果迭代完成，则返回nil。要求:g.mu被锁定。
func stepToNextSpan(g *lockTableGuardImpl) *spanset.Span {
	g.index++
	for ; g.ss < spanset.NumSpanScope; g.ss++ {
		for ; g.sa >= 0; g.sa-- {
			spans := g.spans.GetSpans(g.sa, g.ss)
			if g.index < len(spans) {
				span := &spans[g.index]
				g.key = span.Key
				return span
			}
			g.index = 0
		}
		g.sa = spanset.NumSpanAccess - 1
	}
	return nil
}

// Enable implements the lockTable interface.
func (t *lockTableImpl) Enable(seq roachpb.LeaseSequence) {
	// Avoid disrupting other requests if the lockTable is already enabled.
	// NOTE: This may be a premature optimization, but it can't hurt.
	// 如果lockTable已经启用，避免中断其他请求。
	// 注意:这可能是一个过早的优化，但也无伤大雅。
	t.enabledMu.RLock()
	enabled, enabledSeq := t.enabled, t.enabledSeq
	t.enabledMu.RUnlock()
	if enabled && enabledSeq == seq {
		return
	}
	t.enabledMu.Lock()
	t.enabled = true
	t.enabledSeq = seq
	t.enabledMu.Unlock()
}

// Clear implements the lockTable interface.
func (t *lockTableImpl) Clear(disable bool) {
	// If disabling, lock the entire table to prevent concurrent accesses
	// from adding state to the table as we clear it. If not, there's no
	// need to synchronize with enabledMu because we're only removing state.
	// 如果禁用，则锁定整个表，以防止在清除表时并发访问向表添加状态。
	// 如果没有，就不需要与enabledMu同步，因为我们只删除状态。
	if disable {
		t.enabledMu.Lock()
		defer t.enabledMu.Unlock()
		t.enabled = false
	}
	t.tryClearLocks(true /* force */)
}

// For tests.
func (t *lockTableImpl) String() string {
	var buf strings.Builder
	for i := 0; i < len(t.locks); i++ {
		tree := &t.locks[i]
		scope := spanset.SpanScope(i).String()
		tree.mu.RLock()
		fmt.Fprintf(&buf, "%s: num=%d\n", scope, atomic.LoadInt64(&tree.numLocks))
		iter := tree.MakeIter()
		for iter.First(); iter.Valid(); iter.Next() {
			l := iter.Cur()
			l.mu.Lock()
			l.Format(&buf)
			l.mu.Unlock()
		}
		tree.mu.RUnlock()
	}
	return buf.String()
}

//GetSpanLcokMsg 返回传入span持有的锁的durability信息
func (t *lockTableImpl) GetSpanLockMsg(
	span roachpb.Span, transactionID string,
) map[lock.Durability]bool {
	durabilityStatus := make(map[lock.Durability]bool)
	for i := 0; i < len(t.locks); i++ {
		tree := &t.locks[i]
		tree.mu.RLock()
		iter := tree.MakeIter()

		for iter.First(); iter.Valid(); iter.Next() {
			l := iter.Cur()
			l.mu.Lock()
			isThisTxn, lockDurability := l.LockStateInfo(span, transactionID)
			if isThisTxn {
				durabilityStatus[lockDurability] = true
			}
			l.mu.Unlock()
		}
		tree.mu.RUnlock()
	}
	return durabilityStatus
}
