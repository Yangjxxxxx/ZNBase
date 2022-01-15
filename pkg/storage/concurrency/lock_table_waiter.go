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
	"math"
	"time"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/intentresolver"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// LockTableLivenessPushDelay sets the delay before pushing in order to detect
// coordinator failures of conflicting transactions.
// LockTableLivenessPushDelay设置推送前的延迟，以检测冲突事务的协调器故障。
var LockTableLivenessPushDelay = settings.RegisterDurationSetting(
	"kv.lock_table.coordinator_liveness_push_delay",
	"the delay before pushing in order to detect coordinator failures of conflicting transactions",
	// This is set to a short duration to ensure that we quickly detect failed
	// transaction coordinators that have abandoned one or many locks. We don't
	// want to wait out a long timeout on each of these locks to detect that
	// they are abandoned. However, we also don't want to push immediately in
	// cases where the lock is going to be resolved shortly.
	//
	// We could increase this default to somewhere on the order of the
	// transaction heartbeat timeout (5s) if we had a better way to avoid paying
	// the cost on each of a transaction's abandoned locks and instead only pay
	// it once per abandoned transaction per range or per node. This could come
	// in a few different forms, including:
	// - a per-store cache of recently detected abandoned transaction IDs
	// - a per-range reverse index from transaction ID to locked keys
	//
	// EDIT: The finalizedTxnCache gets us part of the way here. It allows us to
	// pay the liveness push delay cost once per abandoned transaction per range
	// instead of once per each of an abandoned transaction's locks. This helped
	// us to feel comfortable increasing the default delay from the original
	// 10ms to the current 50ms. Still, to feel comfortable increasing this
	// further, we'd want to improve this cache (e.g. lifting it to the store
	// level) to reduce the cost to once per abandoned transaction per store.
	//设置为一个较短的持续时间，以确保我们快速检测出放弃了一个或多个锁的失败事务协调器。
	//我们不希望在每个锁上等待长时间的超时来检测它们是否被放弃。
	//但是，我们也不想立即推送，如果我们有更好的方法来避免为每个事务的放弃锁支付成本，
	//而是在每个range或每个节点只为每个放弃的事务支付一次，
	//我们可以将这个默认值增加到事务心跳超时（5s）的某个位置。这可能有几种不同的形式，包括：
	//-最近检测到的被放弃的事务id的每个存储缓存
	//-从事务ID到锁定密钥的每个range反向索引
	//编辑:finalizedTxnCache让我们在这里完成了一部分。
	//它允许我们为每个范围内每个被放弃的事务支付一次活性推送延迟成本，
	//而不是为每个被放弃的事务的锁支付一次。这有助于我们放心地将默认延迟从最初的10ms增加到当前的50ms。
	//尽管如此，为了更舒适地进一步增加缓存，我们还是希望改进缓存(例如，将其提升到存储级别)，
	//以将成本降低到每个存储每放弃一次事务。
	// TODO(nvanbenschoten): continue increasing this default value.
	50*time.Millisecond,
)

// LockTableDeadlockDetectionPushDelay sets the delay before pushing in order to
// detect dependency cycles between transactions.
// LockTableDeadlockDetectionPushDelay设置推送前的延迟，以便检测事务之间的依赖周期。
var LockTableDeadlockDetectionPushDelay = settings.RegisterDurationSetting(
	"kv.lock_table.deadlock_detection_push_delay",
	"the delay before pushing in order to detect dependency cycles between transactions",
	// 为了检测事务之间的依赖周期而在推入之前的延迟
	// This is set to a medium duration to ensure that deadlock caused by
	// dependency cycles between transactions are eventually detected, but that
	// the deadlock detection does not impose any overhead in the vastly common
	// case where there are no dependency cycles. We optimistically assume that
	// deadlocks are not common in production applications and wait locally on
	// locks for a while before checking for a deadlock. Increasing this value
	// reduces the amount of time wasted in needless deadlock checks, but slows
	// down reporting of real deadlock scenarios.
	//
	// The value is analogous to Postgres' deadlock_timeout setting, which has a
	// default value of 1s:
	//  https://www.postgresql.org/docs/current/runtime-config-locks.html#GUC-DEADLOCK-TIMEOUT.
	//
	// We could increase this default to somewhere around 250ms - 1000ms if we
	// confirmed that we do not observe deadlocks in any of the workloads that
	// we care about. When doing so, we should be conscious that even once
	// distributed deadlock detection begins, there is some latency proportional
	// to the length of the dependency cycle before the deadlock is detected.
	//这设置为中等持续时间，以确保最终检测到事务之间依赖周期引起的死锁，但是
	//在不存在依赖周期的常见情况下，死锁检测不会带来任何开销。
	//我们乐观地假设死锁在生产应用程序中并不常见，并且在检查死锁之前在本地等待一段时间。
	//增加这个值可以减少在不必要的死锁检查中浪费的时间，但是会降低实际死锁场景的报告速度。
	//这个值类似于Postgres的deadlock_timeout设置，它的adefault值为1:
	//https://www.postgresql.org/docs/current/runtime-config-locks.html GUC-DEADLOCK-TIMEOUT。
	//如果我们确认在我们关心的任何工作负载中没有观察到死锁，
	//我们可以将这个默认值提高到大约250ms - 1000ms。 在这样做时，我们应该意识到，
	//即使开始了分布式死锁检测，在检测到死锁之前， 仍然存在一些与依赖周期长度成比例的延迟。
	// TODO(nvanbenschoten): increasing this default value.
	100*time.Millisecond,
)

// lockTableWaiterImpl is an implementation of lockTableWaiter.
type lockTableWaiterImpl struct {
	st      *cluster.Settings
	stopper *stop.Stopper
	ir      IntentResolver
	lm      LockManager

	// finalizedTxnCache is a small LRU cache that tracks transactions that
	// were pushed and found to be finalized (COMMITTED or ABORTED). It is
	// used as an optimization to avoid repeatedly pushing the transaction
	// record when cleaning up the intents of an abandoned transaction.
	//
	// NOTE: it probably makes sense to maintain a single finalizedTxnCache
	// across all Ranges on a Store instead of an individual cache per
	// Range. For now, we don't do this because we don't share any state
	// between separate concurrency.Manager instances.
	//finalizedTxnCache是一个小型LRU缓存(最近最少使用)，用于跟踪已推入并发现已完成(提交或中止)的事务。
	//它被用作一种优化，以避免在清理废弃事务的意图时重复推入事务记录。
	//注意:跨存储中的所有range维护一个finalizedTxnCache，而不是每个range维护一个单独的缓存，这可能是有意义的。
	//目前，我们没有这样做，因为我们没有在单独的concurrencyManager实例之间共享任何状态。
	//分布式死锁检测开始时，在检测到死锁之前，存在与依赖周期长度成比例的延迟。
	finalizedTxnCache txnCache

	// When set, WriteIntentError are propagated instead of pushing
	// conflicting transactions.
	//当设置时，WriteIntentError将被传播，而不是推送冲突的事务。
	disableTxnPushing bool
}

// IntentResolver is an interface used by lockTableWaiterImpl to push
// transactions and to resolve intents. It contains only the subset of the
// intentresolver.IntentResolver interface that lockTableWaiterImpl needs.
//IntentResolver是lockTableWaiterImpl用来推送事务和解析意图的接口。
//它只包含intentresolver的子集。lockTableWaiterImpl需要的IntentResolver接口。
type IntentResolver interface {
	// PushTransaction pushes the provided transaction. The method will push the
	// provided pushee transaction immediately, if possible. Otherwise, it will
	// block until the pushee transaction is finalized or eventually can be
	// pushed successfully.
	//PushTransaction推送所提供的事务。如果可能的话，该方法将立即推送所提供的pushee事务。
	//否则，它将阻塞，直到pushee事务完成或最终成功推入。
	PushTransaction(
		context.Context, *enginepb.TxnMeta, roachpb.Header, roachpb.PushTxnType,
	) (*roachpb.Transaction, *Error)

	// ResolveIntent synchronously resolves the provided intent.
	// ResolveIntent同步解析提供的意图。
	ResolveIntent(context.Context, roachpb.LockUpdate, intentresolver.ResolveOptions) *Error

	// ResolveIntents synchronously resolves the provided batch of intents.
	//ResolveIntents同步解析提供的一批意图。
	ResolveIntents(context.Context, []roachpb.LockUpdate, intentresolver.ResolveOptions) *Error
}

// WaitOn implements the lockTableWaiter interface.
// WaitOn实现lockTableWaiter接口
func (w *lockTableWaiterImpl) WaitOn(
	ctx context.Context, req Request, guard lockTableGuard,
) (err *Error) {
	newStateC := guard.NewStateChan()
	ctxDoneC := ctx.Done()
	shouldQuiesceC := w.stopper.ShouldQuiesce()
	// Used to delay liveness and deadlock detection pushes.
	// 用于延迟活动和死锁检测推送。
	var timer *timeutil.Timer
	var timerC <-chan time.Time
	var timerWaitingState waitingState
	rcRead := hasReadCommitted(req.Txn) && req.ReadOnly
	// Used to defer the resolution of duplicate intents. Intended to allow
	// batching of intent resolution while cleaning up after abandoned txns. A
	// request may begin deferring intent resolution and then be forced to wait
	// again on other locks. This is ok, as the request that deferred intent
	// resolution will often be the new reservation holder for those intents'
	// keys. Even when this is not the case (e.g. the request is read-only so it
	// can't hold reservations), any other requests that slip ahead will simply
	// re-discover the intent(s) during evaluation and resolve them themselves.
	//用来推迟重复意图的解决。旨在在清理废弃TXN后，允许批量处理意向解决方案。
	//请求可能开始推迟意向解析，然后被迫再次等待其他锁。 这是可以的，
	//因为延迟意向解析的请求通常是那些意图的密钥的新reservation holder。
	//即使不是这样，任何其他提前的请求都会在评估期间重新发现意图并自行解决。
	var deferredResolution []roachpb.LockUpdate
	defer w.resolveDeferredIntents(ctx, &err, &deferredResolution, req.BackFill)
	for {
		select {
		case <-newStateC:
			timerC = nil
			state := guard.CurState()
			switch state.kind {
			case waitFor, waitForDistinguished:
				if req.WaitPolicy == lock.WaitPolicy_Error {
					// If the waiter has an Error wait policy, resolve the conflict
					// immediately without waiting. If the conflict is a lock then
					// push the lock holder's transaction using a PUSH_TOUCH to
					// determine whether the lock is abandoned or whether its holder
					// is still active. If the conflict is a reservation holder,
					// raise an error immediately, we know the reservation holder is
					// active.
					//如果waiter有错误等待策略，立即解决冲突，而不必等待。
					//如果冲突是一个锁，那么使用push_TOUCH来推动锁持有者的事务，以确定锁是否被放弃或其持有者是否仍处于活动状态。
					//如果冲突是reservation holder，立即提出错误，我们知道reservation holder是活动的。
					if state.held {
						err = w.pushLockTxn(ctx, req, state)
					} else {
						err = newWriteIntentErr(state)
					}
					if err != nil {
						return err
					}
					continue
				}

				if rcRead {
					// 需要立刻push
					if !state.held {
						// reservation holder暂时不处理
						return nil
					}
					return w.pushLockTxn(ctx, req, state)
				}
				// waitFor indicates that the request is waiting on another
				// transaction. This transaction may be the lock holder of a
				// conflicting lock or the head of a lock-wait queue that the
				// request is a part of.
				//
				// waitForDistinguished is like waitFor, except it instructs the
				// waiter to quickly push the conflicting transaction after a short
				// liveness push delay instead of waiting out the full deadlock
				// detection push delay. The lockTable guarantees that there is
				// always at least one request in the waitForDistinguished state for
				// each lock that has any waiters.
				//
				// The purpose of the waitForDistinguished state is to avoid waiting
				// out the longer deadlock detection delay before recognizing and
				// recovering from the failure of a transaction coordinator for
				// *each* of that transaction's previously written intents.
				//waitFor表示请求正在等待另一个事务。此事务可能是冲突锁的锁持有者，
				//也可能是请求所属的锁等待队列的头。
				//waitForDistinguished与waitFor类似，只是它指示waiter在短暂的活动
				//推送延迟之后快速推送冲突事务，而不是等待完全死锁检测推送延迟。
				//锁表保证对于具有任何waiter的每个锁，始终至少有一个处于waitForDistinguished状态的请求。
				//waitForDistinguished状态的目的是为了避免在事务协调器失败之前等待较长的死锁检测延迟，
				//并从事务协调器的故障中恢复该事务先前写入的意图。
				livenessPush := state.kind == waitForDistinguished
				deadlockPush := true

				// If the conflict is a reservation holder and not a held lock then
				// there's no need to perform a liveness push - the request must be
				// alive or its context would have been canceled and it would have
				// exited its lock wait-queues.
				//如果冲突是一个reservation holder，而不是一个被持有的锁，
				//那么就不需要执行活动推送——请求必须是活动的，否则它的上下文将被取消，它将退出它的锁等待队列。
				if !state.held {
					livenessPush = false
				}

				// For non-transactional requests, there's no need to perform
				// deadlock detection because a non-transactional request can
				// not be part of a dependency cycle. Non-transactional requests
				// cannot hold locks or reservations.
				// 对于非事务性请求，不需要执行死锁检测，
				// 因为非事务性请求不能成为依赖关系周期的一部分。非事务性请求不能持有锁或保留。
				if req.Txn == nil {
					deadlockPush = false
				}

				// If the request doesn't want to perform a push for either
				// reason, continue waiting.
				// 如果请求由于这两个原因都不想执行推送，继续等待。
				if !livenessPush && !deadlockPush {
					continue
				}

				// If we know that a lock holder is already finalized (COMMITTED
				// or ABORTED), there's no reason to push it again. Instead, we
				// can skip directly to intent resolution.
				//
				// As an optimization, we defer the intent resolution until the
				// we're done waiting on all conflicting locks in this function.
				// This allows us to accumulate a group of intents to resolve
				// and send them together as a batch.
				//
				// Remember that if the lock is held, there will be at least one
				// waiter with livenessPush = true (the distinguished waiter),
				// so at least one request will enter this branch and perform
				// the cleanup on behalf of all other waiters.
				//如果我们知道锁持有者已经完成（提交或中止），就没有理由再推它了。
				//相反，我们可以直接跳到意图解决。
				//作为一个优化， 我们将意图解析推迟到我们完成了对这个函数中所有冲突锁的等待。
				//这使我们可以积累一组意图来解决问题，并将它们作为一个batch发送到一起。
				//请记住，如果锁被持有，那么至少会有一个livenessPush=true的waiter（the distinguished waiter），
				//因此至少有一个请求将进入这个分支，并代表所有其他waiter执行清理。
				if livenessPush {
					if pusheeTxn, ok := w.finalizedTxnCache.get(state.txn.ID); ok {
						resolve := roachpb.MakeLockUpdate(pusheeTxn, roachpb.Span{Key: state.key})
						deferredResolution = append(deferredResolution, resolve)

						// Inform the LockManager that the lock has been updated with a
						// finalized status so that it gets removed from the lockTable
						// and we are allowed to proceed.
						//
						// For unreplicated locks, this is all that is needed - the
						// lockTable is the source of truth so, once removed, the
						// unreplicated lock is gone. It is perfectly valid for us to
						// instruct the lock to be released because we know that the
						// lock's owner is finalized.
						//
						// For replicated locks, this is a bit of a lie. The lock hasn't
						// actually been updated yet, but we will be conducting intent
						// resolution in the future (before we observe the corresponding
						// MVCC state). This is safe because we already handle cases
						// where locks exist only in the MVCC keyspace and not in the
						// lockTable.
						//
						// In the future, we'd like to make this more explicit.
						// Specifically, we'd like to augment the lockTable with an
						// understanding of finalized but not yet resolved locks. These
						// locks will allow conflicting transactions to proceed with
						// evaluation without the need to first remove all traces of
						// them via a round of replication. This is discussed in more
						// detail in #41720. Specifically, see mention of "contention
						// footprint" and COMMITTED_BUT_NOT_REMOVABLE.
						//通知LockManager锁已经更新为最终状态，这样它就可以从lockTable中删除，我们就可以继续操作了。
						//对于未复制的锁，lockTable是数据源，因此，一旦删除，未复制的锁就消失了。
						//对于我们来说，指示释放锁是完全有效的，因为我们知道锁的所有者已经最终确定了。
						//对于复制锁来说， 锁实际上还没有被更新，但是我们将在将来进行意图解析(在我们观察到相应的MVCC状态之前)。
						//这是安全的，因为我们已经处理了锁只存在于MVCC密钥空间而不在lockTable中的情况。
						//在未来，我们将使这一点更加明确。 具体地说，我们希望通过理解已完成但尚未解决的锁来扩充lockTable。
						//这些锁将允许冲突的事务继续进行评估，而不需要首先通过一轮复制删除它们的所有踪迹。
						//这在#41720中有更详细的讨论。
						//具体来说，请参见“contention footprint" and COMMITTED_BUT_NOT_REMOVABLE.
						w.lm.OnLockUpdated(ctx, &deferredResolution[len(deferredResolution)-1])
						continue
					}
				}

				// The request should push to detect abandoned locks due to
				// failed transaction coordinators, detect deadlocks between
				// transactions, or both, but only after delay. This delay
				// avoids unnecessary push traffic when the conflicting
				// transaction is continuing to make forward progress.
				//请求应该推送以检测由于事务协调器失败而放弃的锁，检测事务之间的死锁，
				//或者两者都检测，但只能在延迟之后。
				//当冲突事务继续向前推进时，这种延迟避免了不必要的推送流量。
				delay := time.Duration(math.MaxInt64)
				if livenessPush {
					delay = minDuration(delay, LockTableLivenessPushDelay.Get(&w.st.SV))
				}
				if deadlockPush {
					delay = minDuration(delay, LockTableDeadlockDetectionPushDelay.Get(&w.st.SV))
				}

				// However, if the pushee has the minimum priority or if the
				// pusher has the maximum priority, push immediately.
				// 但是，如果pushee有最小的优先级，或者如果pusher有最大的优先级，立即推动。
				// TODO(nvanbenschoten): flesh these interactions out more and
				// add some testing.
				if hasMinPriority(state.txn) || hasMaxPriority(req.Txn) {
					delay = 0
				}

				if delay > 0 {
					if timer == nil {
						timer = timeutil.NewTimer()
						defer timer.Stop()
					}
					timer.Reset(delay)
					timerC = timer.C
				} else {
					// If we don't want to delay the push, don't use a real timer.
					// Doing so is both a waste of resources and, more importantly,
					// makes TestConcurrencyManagerBasic flaky because there's no
					// guarantee that the timer will fire before the goroutine enters
					// a "select" waiting state on the next iteration of this loop.
					//如果我们不想延迟推送，就不要使用真正的计时器。这样做既浪费资源，
					//更重要的是，会使TestConcurrencyManagerBasic变得不稳定，
					//因为无法保证计时器在goroutine进入“选择”等待状态之前触发该循环的下一次迭代。
					timerC = closedTimerC
				}
				timerWaitingState = state

			case waitElsewhere:
				// The lockTable has hit a memory limit and is no longer maintaining
				// proper lock wait-queues.
				// lockTable已达到内存限制，不再维护适当的锁等待队列。
				if !state.held {
					// If the lock is not held, exit immediately. Requests will
					// be ordered when acquiring latches.
					// 如果没有锁，立即退出。 获取锁时将对请求进行排序。
					return nil
				}
				// The waiting request is still not safe to proceed with
				// evaluation because there is still a transaction holding the
				// lock. It should push the transaction it is blocked on
				// immediately to wait in that transaction's txnWaitQueue. Once
				// this completes, the request should stop waiting on this
				// lockTableGuard, as it will no longer observe lock-table state
				// transitions.
				// 等待的请求仍然不安全，无法继续评估，因为仍有一个事务持有锁。
				// 它应该立即将被阻塞的事务推送到该事务的txnWaitQueue中等待。
				// 一旦完成，请求应该停止等待这个lockTableGuard，因为它将不再观察锁表状态转换。
				return w.pushLockTxn(ctx, req, state)

			case waitSelf:
				// Another request from the same transaction is the reservation
				// holder of this lock wait-queue. This can only happen when the
				// request's transaction is sending multiple requests concurrently.
				// Proceed with waiting without pushing anyone.
				// 来自同一事务的另一个请求是这个锁等待队列的reservation holder。
				// 只有当请求的事务并发发送多个请求时才会发生这种情况。继续等待，不要推任何请求。

			case doneWaiting:
				// The request has waited for all conflicting locks to be released
				// and is at the front of any lock wait-queues. It can now stop
				// waiting, re-acquire latches, and check the lockTable again for
				// any new conflicts. If it find none, it can proceed with
				// evaluation.
				// 请求已等待所有冲突锁释放，并且位于任何锁等待队列的前面。
				// 它现在可以停止等待，重新获取锁存，并再次检查锁表是否有任何新的冲突。
				// 如果没有找到，可以继续计算。
				return nil

			default:
				panic("unexpected waiting state")
			}

		case <-timerC:
			// If the request was in the waitFor or waitForDistinguished states
			// and did not observe any update to its state for the entire delay,
			// it should push. It may be the case that the transaction is part
			// of a dependency cycle or that the lock holder's coordinator node
			// has crashed.
			//如果请求处于waitFor或waitForDistinguished状态，
			//并且在整个延迟期间没有观察到对其状态的任何更新，那么它应该push。
			//可能出现这样的情况:事务是依赖周期的一部分，或者锁持有者的协调节点崩溃。
			timerC = nil
			if timer != nil {
				timer.Read = true
			}
			// If the request is conflicting with a held lock then it pushes its
			// holder synchronously - there is no way it will be able to proceed
			// until the lock's transaction undergoes a state transition (either
			// completing or being pushed) and then updates the lock's state
			// through intent resolution. The request has a dependency on the
			// entire conflicting transaction.
			//
			// However, if the request is conflicting with another request (a
			// reservation holder) then it pushes the reservation holder
			// asynchronously while continuing to listen to state transition in
			// the lockTable. This allows the request to cancel its push if the
			// conflicting reservation exits the lock wait-queue without leaving
			// behind a lock. In this case, the request has a dependency on the
			// conflicting request but not necessarily the entire conflicting
			// transaction.
			// 如果请求与一个被持有的锁冲突，
			// 那么它会同步推送它的持有者——在锁的事务经历状态转换(完成或被推送)之后，它无法继续进行，
			// 然后通过意图解析更新锁的状态。请求依赖于整个冲突的事务。
			//
			//但是，如果请求与另一个请求(一个reservation holder)发生冲突，
			// 那么它将异步地推送reservation holder，同时继续侦听lockTable中的状态转换。
			// 这允许请求在冲突保留退出锁等待队列而不留下锁的情况下取消推送。
			// 在这种情况下，请求依赖于冲突请求，但不一定依赖于整个冲突事务。
			if timerWaitingState.held {
				err = w.pushLockTxn(ctx, req, timerWaitingState)
			} else {
				// It would be more natural to launch an async task for the push
				// and continue listening on this goroutine for lockTable state
				// transitions, but doing so is harder to test against. Instead,
				// we launch an async task to listen to lockTable state and
				// synchronously push. If the watcher goroutine detects a
				// lockTable change, it cancels the context on the push.
				// 为push启动一个异步任务并继续监听这个goroutine以获得可锁状态转换将更自然，
				// 但这样做更难测试。相反，我们启动一个异步任务来监听锁表状态并同步推送。
				// 如果watcher goroutine检测到锁表更改，它将取消推送时的上下文。
				pushCtx, pushCancel := context.WithCancel(ctx)
				go w.watchForNotifications(pushCtx, pushCancel, newStateC)
				err = w.pushRequestTxn(pushCtx, req, timerWaitingState)
				if errors.Is(pushCtx.Err(), context.Canceled) {
					// Ignore the context canceled error. If this was for the
					// parent context then we'll notice on the next select.
					// 忽略上下文已取消错误。如果这是为父上下文，那么我们会注意到下一个选择。
					err = nil
				}
				pushCancel()
			}
			if err != nil {
				return err
			}

		case <-ctxDoneC:
			return roachpb.NewError(ctx.Err())

		case <-shouldQuiesceC:
			return roachpb.NewError(&roachpb.NodeUnavailableError{})
		}
	}
}

//WaitOnTime
func (w *lockTableWaiterImpl) WaitOnTime(
	ctx context.Context, req Request, guard lockTableGuard,
) (err *Error) {
	req.WaitPolicy = lock.WaitPolicy_Block
	sem := make(chan struct{}, 1)
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	w.stopper.RunWorker(ctx, func(_ context.Context) {
		err = w.WaitOn(ctx1, req, guard)
		sem <- struct{}{}
	})
	ctxDoneW := ctx.Done()
	shouldQuiesceW := w.stopper.ShouldQuiesce()
	timer := timeutil.NewTimer()
	defer timer.Stop()
	timer.Reset(time.Duration(req.WaitTime) * time.Second)
	timerC := timer.C
	for {
		select {
		case <-sem:
			return err
		case <-timerC:
			cancel1()
			// wait for cancel
			select {
			case <-sem:
			}
			req.WaitPolicy = lock.WaitPolicy_Error
			guard.(*lockTableGuardImpl).notify()
			err = w.WaitOn(ctx, req, guard)
			return err
		case <-ctxDoneW:
			return roachpb.NewError(ctx.Err())

		case <-shouldQuiesceW:
			return roachpb.NewError(&roachpb.NodeUnavailableError{})
		}
	}
}

// WaitOnLock implements the lockTableWaiter interface.
// WaitOnLock实现lockTableWaiter接口。
func (w *lockTableWaiterImpl) WaitOnLock(
	ctx context.Context, req Request, intent *roachpb.Intent,
) *Error {
	sa, _, err := findAccessInSpans(intent.Key, req.LockSpans)
	if err != nil {
		return roachpb.NewError(err)
	}
	return w.pushLockTxn(ctx, req, waitingState{
		kind:        waitFor,
		txn:         &intent.Txn,
		key:         intent.Key,
		held:        true,
		guardAccess: sa,
	})
}

// ClearCaches implements the lockTableWaiter interface.
// ClearCaches实现了lockTableWaiter接口。
func (w *lockTableWaiterImpl) ClearCaches() {
	w.finalizedTxnCache.clear()
}

// pushLockTxn pushes the holder of the provided lock.
//
// The method blocks until the lock holder transaction experiences a state
// transition such that it no longer conflicts with the pusher's request. The
// method then synchronously updates the lock to trigger a state transition in
// the lockTable that will free up the request to proceed. If the method returns
// successfully then the caller can expect to have an updated waitingState.
// pushLockTxn推入所提供的锁的持有者。
// 该方法将一直阻塞，直到锁持有者事务经历状态转换，使其不再与推送者的请求冲突。
// 然后，该方法同步更新锁，以触发锁表中的状态转换，从而释放请求继续进行。
// 如果该方法成功返回，那么调用者可以期待有一个更新的等待状态。
func (w *lockTableWaiterImpl) pushLockTxn(
	ctx context.Context, req Request, ws waitingState,
) *Error {
	if w.disableTxnPushing {
		return newWriteIntentErr(ws)
	}

	// Construct the request header and determine which form of push to use.
	// 构造请求头并确定使用哪种推送形式。
	h := w.pushHeader(req)
	var pushType roachpb.PushTxnType
	switch req.WaitPolicy {
	case lock.WaitPolicy_Block:
		// This wait policy signifies that the request wants to wait until the
		// conflicting lock is released. For read-write conflicts, try to push
		// the lock holder's timestamp forward so the read request can read
		// under the lock. For write-write conflicts, try to abort the lock
		// holder entirely so the write request can revoke and replace the lock
		// with its own lock.
		// 此等待策略表示请求希望等待冲突锁释放。
		// 对于读-写冲突，请尝试向前推锁持有者的时间戳，以便读取请求可以在锁下读取。
		// 对于写-写冲突，请尝试完全中止锁持有者， 以便写入请求可以撤消锁并用自己的锁替换锁。
		switch ws.guardAccess {
		case spanset.SpanReadOnly:
			pushType = roachpb.PUSH_TIMESTAMP
			log.VEventf(ctx, 2, "pushing timestamp of txn %s above %s", ws.txn.ID.Short(), h.Timestamp)

		case spanset.SpanReadWrite:
			pushType = roachpb.PUSH_ABORT
			if req.ReadOnly && req.Txn != nil && req.Txn.IsolationLevel == util.ReadCommittedIsolation {
				pushType = roachpb.PUSH_TIMESTAMP
			}
			log.VEventf(ctx, 2, "pushing txn %s to abort", ws.txn.ID.Short())
		}

	case lock.WaitPolicy_Error:
		// This wait policy signifies that the request wants to raise an error
		// upon encountering a conflicting lock. We still need to push the lock
		// holder to ensure that it is active and that this isn't an abandoned
		// lock, but we push using a PUSH_TOUCH to immediately return an error
		// if the lock hold is still active.
		// 此等待策略表示请求希望在遇到冲突的锁时引发错误。
		// 我们仍然需要push锁持有者，以确保它是活动的，不是一个废弃的锁，但我们使用PUSH_TOUCH push，
		// 以立即返回一个错误，如果锁持有仍然活动。
		pushType = roachpb.PUSH_TOUCH
		log.VEventf(ctx, 2, "pushing txn %s to check if abandoned", ws.txn.ID.Short())

	default:
		log.Fatalf(ctx, "unexpected WaitPolicy: %v", req.WaitPolicy)
	}

	pusheeTxn, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		// If pushing with an Error WaitPolicy and the push fails, then the lock
		// holder is still active. Transform the error into a WriteIntentError.
		// 如果使用错误的WaitPolicy进行推送并且推送失败，则锁保持器仍处于活动状态。将错误转化为WriteIntentError。
		if _, ok := err.GetDetail().(*roachpb.TransactionPushError); ok && req.WaitPolicy == lock.WaitPolicy_Error {
			err = newWriteIntentErr(ws)
		}
		return err
	}

	// If the transaction is finalized, add it to the finalizedTxnCache. This
	// avoids needing to push it again if we find another one of its locks and
	// allows for batching of intent resolution.
	// 如果事务完成，则将其添加到finalizedTxnCache中。
	// 这样，如果我们找到另一个锁，就不必再推它，并且允许批量处理意图解析。
	if pusheeTxn.Status.IsFinalized() {
		w.finalizedTxnCache.add(pusheeTxn)
	}

	// If the push succeeded then the lock holder transaction must have
	// experienced a state transition such that it no longer conflicts with
	// the pusher's request. This state transition could have been any of the
	// following, each of which would be captured in the pusheeTxn proto:
	// 1. the pushee was committed
	// 2. the pushee was aborted
	// 3. the pushee was pushed to a higher provisional commit timestamp such
	//    that once its locks are updated to reflect this, they will no longer
	//    conflict with the pusher request. This is only applicable if pushType
	//    is PUSH_TIMESTAMP.
	// 4. the pushee rolled back all sequence numbers that it held the
	//    conflicting lock . This allows the lock to be revoked entirely.
	// 如果推送成功，那么锁持有者事务一定经历了一个状态转换，使得它不再与推送者的请求冲突。
	// 这种状态转换可以是以下任何一种，每一种都可以在pusheeTxn proto中捕获：
	// 1. pushee提交
	// 2. pushee被中止
	// 3. pushee被推到更高的临时提交时间戳，这样一旦它的锁被更新以反映这一点，它们将不再与pusher请求冲突。这只适用于pushType为PUSH_TIMESTAMP的情况。
	// 4. pushee回滚了它持有冲突锁的所有序列号。这允许完全撤销锁。
	//    TODO(nvanbenschoten): we do not currently detect this case. Doing so
	//    would not be useful until we begin eagerly updating a transaction's
	//    record upon rollbacks to savepoints.
	//
	// Update the conflicting lock to trigger the desired state transition in
	// the lockTable itself, which will allow the request to proceed.
	//
	// We always poison due to limitations of the API: not poisoning equals
	// clearing the AbortSpan, and if our pushee transaction first got pushed
	// for timestamp (by us), then (by someone else) aborted and poisoned, and
	// then we run the below code, we're clearing the AbortSpan illegaly.
	// Furthermore, even if our pushType is not PUSH_ABORT, we may have ended up
	// with the responsibility to abort the intents (for example if we find the
	// transaction aborted). To do better here, we need per-intent information
	// on whether we need to poison.
	// /我们总是发生poison由于API的限制:不poison等于清除AbortSpan，
	// 如果我们的pushee事务首先被推送的时间戳(由我们)，然后(由其他人)中止和poisoned，
	// 然后我们运行下面的代码，我们正在非法清除AbortSpan。
	// 此外，即使我们的pushType不是PUSH_ABORT，我们也可能最终要负责中止意图(例如，如果我们发现事务中止)。
	// 为了做得更好，我们需要每个意图的信息，关于我们是否需要poison.
	resolve := roachpb.MakeLockUpdate(pusheeTxn, roachpb.Span{Key: ws.key})
	opts := intentresolver.ResolveOptions{Poison: true}
	err = w.ir.ResolveIntent(ctx, resolve, opts)
	if err != nil {
		return err
	}

	if h.Txn != nil && h.Txn.IsolationLevel == util.ReadCommittedIsolation && pushType == roachpb.PUSH_ABORT {
		// 读为了写需要sql重试
		if h.Txn.Status == roachpb.ABORTED {
			log.Errorf(ctx, "need FATAL")
		}
		return newWriteTooOld(ws)
	}
	return nil
}

// pushRequestTxn pushes the owner of the provided request.
//
// The method blocks until either the pusher's transaction is aborted or the
// pushee's transaction is finalized (committed or aborted). If the pusher's
// transaction is aborted then the method will send an error on the channel and
// the pusher should exit its lock wait-queues. If the pushee's transaction is
// finalized then the method will send no error on the channel. The pushee is
// expected to notice that it has been aborted during its next attempt to push
// another transaction and will exit its lock wait-queues.
//
// However, the method responds to context cancelation and will terminate the
// push attempt if its context is canceled. This allows the caller to revoke a
// push if it determines that the pushee is no longer blocking the request. The
// caller is expected to terminate the push if it observes any state transitions
// in the lockTable. As such, the push is only expected to be allowed to run to
// completion in cases where requests are truly deadlocked.
// pushRequestTxn推送所提供请求的所有者。
// 该方法将一直阻塞，直到推送者的事务被中止或pushee的事务最终完成（提交或中止）。
// 如果pusher的事务被中止，则该方法将在通道上发送错误，并且pusher应退出其锁等待队列。
// 如果pushee的事务已完成，那么该方法将不会在通道上发送错误。
// pushee将注意到它在下一次尝试推送另一个事务时已中止，并将退出其锁等待队列。
// 但是，如果上下文取消，则尝试终止其上下文。
// 这允许调用者在确定pushee不再阻止请求时撤销push。
// 如果调用者观察到锁表中的任何状态转换，则它将终止推送。
// 因此，只有在请求真正陷入死锁的情况下，推送才被允许运行到完成。
func (w *lockTableWaiterImpl) pushRequestTxn(
	ctx context.Context, req Request, ws waitingState,
) *Error {
	// Regardless of whether the waiting request is reading from or writing to a
	// key, it always performs a PUSH_ABORT when pushing a conflicting request
	// because it wants to block until either a) the pushee or the pusher is
	// aborted due to a deadlock or b) the request exits the lock wait-queue and
	// the caller of this function cancels the push.
	// 无论等待的请求是读取key还是写入key，它总是在推送冲突的请求时执行PUSH_ABORT，
	// 因为它希望阻塞直到任何一个1.推送程序因死锁而中止2.请求退出锁等待队列，此函数的调用者取消push操作。
	h := w.pushHeader(req)
	pushType := roachpb.PUSH_ABORT
	log.VEventf(ctx, 3, "pushing txn %s to detect request deadlock", ws.txn.ID.Short())

	_, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		return err
	}

	// Even if the push succeeded and aborted the other transaction to break a
	// deadlock, there's nothing for the pusher to clean up. The conflicting
	// request will quickly exit the lock wait-queue and release its reservation
	// once it notices that it is aborted and the pusher will be free to proceed
	// because it was not waiting on any locks. If the pusher's request does end
	// up hitting a lock which the pushee fails to clean up, it will perform the
	// cleanup itself using pushLockTxn.
	//
	// It may appear that there is a bug here in the handling of request-only
	// dependency cycles. If such a cycle was broken by simultaneously aborting
	// the transactions responsible for each of the request, there would be no
	// guarantee that an aborted pusher would notice that its own transaction
	// was aborted before it notices that its pushee's transaction was aborted.
	// For example, in the simplest case, imagine two requests deadlocked on
	// each other. If their transactions are both aborted and each push notices
	// the pushee is aborted first, they will both return here triumphantly and
	// wait for the other to exit its lock wait-queues, leading to deadlock.
	// Even if they eventually pushed each other again, there would be no
	// guarantee that the same thing wouldn't happen.
	//
	// However, such a situation is not possible in practice because such a
	// dependency cycle is never constructed by the lockTable. The lockTable
	// assigns each request a monotonically increasing sequence number upon its
	// initial entrance to the lockTable. This sequence number is used to
	// straighten out dependency chains of requests such that a request only
	// waits on conflicting requests with lower sequence numbers than its own
	// sequence number. This behavior guarantees that request-only dependency
	// cycles are never constructed by the lockTable. Put differently, all
	// dependency cycles must include at least one dependency on a lock and,
	// therefore, one call to pushLockTxn. Unlike pushRequestTxn, pushLockTxn
	// actively removes the conflicting lock and removes the dependency when it
	// determines that its pushee transaction is aborted. This means that the
	// call to pushLockTxn will continue to make forward progress in the case of
	// a simultaneous abort of all transactions behind the members of the cycle,
	// preventing such a hypothesized deadlock from ever materializing.
	//
	// Example:
	//
	//  req(1, txn1), req(1, txn2) are both waiting on a lock held by txn3, and
	//  they respectively hold a reservation on key "a" and key "b". req(2, txn2)
	//  queues up behind the reservation on key "a" and req(2, txn1) queues up
	//  behind the reservation on key "b". Now the dependency cycle between txn1
	//  and txn2 only involves requests, but some of the requests here also
	//  depend on a lock. So when both txn1, txn2 are aborted, the req(1, txn1),
	//  req(1, txn2) are guaranteed to eventually notice through self-directed
	//  QueryTxn requests and will exit the lockTable, allowing req(2, txn1) and
	//  req(2, txn2) to get the reservation and now they no longer depend on each
	//  other.
	//
	// 即使推送成功并中止了另一个事务以打破死锁，也没有什么可以让pusher清理的。
	// 冲突的请求将很快退出锁等待队列，一旦它发现它被中止，并且pusher将可以自由地继续，因为它没有等待任何锁。
	// 如果pusher的请求最终碰到了一个被pushee无法清理的锁，它将使用pushLockTxn执行清理
	// 在处理仅请求依赖循环时，可能会出现一个bug。如果这样一个循环被同时中止负责每个请求的事务而被打破，
	// 则不能保证中止的pusher在通知其pushee的事务被中止之前会注意到自己的事务被中止。
	// 例如:在最简单的情况下，假设两个请求彼此死锁。如果它们的事务都被中止，并且每个push都注意到
	// pushee首先被中止，那么它们都将成功地返回，并等待另一个退出其锁等待队列，从而导致死锁
	// 即使他们最终再次互相推动，也不能保证同样的事情不会发生。
	// 但是，这种情况在实践中是不可能的，因为lockTable从来没有构造过这样的依赖循环。
	// lockTable为每个请求分配一个单调递增的序号，当它初始进入lockTable时。
	// 这个序列号用于清理依赖请求链，以便请求仅等待序列号低于其自身序列号的冲突请求。
	// 这种行为保证了锁表永远不会构造只请求的依赖循环
	// 换句话说，所有依赖循环必须至少包含一个对锁的依赖，因此，必须包含一个对pushLockTxn的调用。
	// 与pushRequestTxn不同，pushLockTxn在确定其pushee事务被中止时，会主动删除冲突锁并删除依赖关系
	// 这意味着，在周期成员背后的所有事务同时中止的情况下，
	// 对pushLockTxn的调用将继续向前推进，从而防止这种假设的死锁发生。
	// eg.
	// req（1，txn1），req（1，txn2）都在等待由txn3持有的锁，它们分别持有对键“a”和“b”的保留。
	// req（2，txn2）在键“a”上的预约后面排队，req（2，txn1）在键“b”上的预约后面排队。
	// 现在txn1和txn2之间的依赖循环只涉及请求，但是这里的一些请求也依赖于锁。
	// 因此，当txn1和txn2都被中止时，
	// req（1，txn1），req（1，txn2）保证最终通过自我定向的QueryTxn请求进行通知，
	// 并将退出锁表，从而允许req（2，txn1）和req（2，txn2）获得保留，现在它们不再相互依赖。
	return nil
}

func (w *lockTableWaiterImpl) pushHeader(req Request) roachpb.Header {
	h := roachpb.Header{
		Timestamp:    req.readConflictTimestamp(),
		UserPriority: req.Priority,
	}
	if req.Txn != nil {
		// We are going to hand the header (and thus the transaction proto) to
		// the RPC framework, after which it must not be changed (since that
		// could race). Since the subsequent execution of the original request
		// might mutate the transaction, make a copy here. See #9130.
		// 我们将把头（因此事务协议）交给RPC框架，之后它不能更改（因为这可能会发生竞争）。
		// 由于原始请求的后续执行可能会使事务发生变化，所以在此处复制。参见#9130。
		h.Txn = req.Txn.Clone()
	}
	return h
}

// resolveDeferredIntents resolves the batch of intents if the provided error is
// nil. The batch of intents may be resolved more efficiently than if they were
// resolved individually.
// 如果提供的错误为nil, resolveDeferredIntents将解析这批意图。这批意图的解决可能比单独解决更有效。
func (w *lockTableWaiterImpl) resolveDeferredIntents(
	ctx context.Context, err **Error, deferredResolution *[]roachpb.LockUpdate, backFill bool,
) {
	if (*err != nil) || (len(*deferredResolution) == 0) {
		return
	}
	// See pushLockTxn for an explanation of these options.
	opts := intentresolver.ResolveOptions{Poison: true, BackFill: backFill}
	*err = w.ir.ResolveIntents(ctx, *deferredResolution, opts)
}

// watchForNotifications selects on the provided channel and watches for any
// updates. If the channel is ever notified, it calls the provided context
// cancelation function and exits.
// watchForNotifications在提供的通道上选择并监视任何更新。
// 如果通道收到通知，它将调用提供的上下文取消函数并退出。
func (w *lockTableWaiterImpl) watchForNotifications(
	ctx context.Context, cancel func(), newStateC chan struct{},
) {
	select {
	case <-newStateC:
		// Re-signal the channel.
		select {
		case newStateC <- struct{}{}:
		default:
		}
		// Cancel the context of the async task.
		cancel()
	case <-ctx.Done():
	}
}

// txnCache is a small LRU cache that holds Transaction objects.
// txnCache是一个小型LRU缓存，用于保存事务对象。
//
// The zero value of this struct is ready for use.
type txnCache struct {
	mu   syncutil.Mutex
	txns [8]*roachpb.Transaction // [MRU, ..., LRU]
}

func (c *txnCache) get(id uuid.UUID) (*roachpb.Transaction, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx := c.getIdxLocked(id); idx >= 0 {
		txn := c.txns[idx]
		c.moveFrontLocked(txn, idx)
		return txn, true
	}
	return nil, false
}

func (c *txnCache) add(txn *roachpb.Transaction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx := c.getIdxLocked(txn.ID); idx >= 0 {
		c.moveFrontLocked(txn, idx)
	} else {
		c.insertFrontLocked(txn)
	}
}

func (c *txnCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.txns {
		c.txns[i] = nil
	}
}

func (c *txnCache) getIdxLocked(id uuid.UUID) int {
	for i, txn := range c.txns {
		if txn != nil && txn.ID == id {
			return i
		}
	}
	return -1
}

func (c *txnCache) moveFrontLocked(txn *roachpb.Transaction, cur int) {
	copy(c.txns[1:cur+1], c.txns[:cur])
	c.txns[0] = txn
}

func (c *txnCache) insertFrontLocked(txn *roachpb.Transaction) {
	copy(c.txns[1:], c.txns[:])
	c.txns[0] = txn
}

func newWriteIntentErr(ws waitingState) *Error {
	return roachpb.NewError(&roachpb.WriteIntentError{
		Intents: []roachpb.Intent{roachpb.MakeIntent(ws.txn, ws.key)},
	})
}

func newWriteTooOld(ws waitingState) *Error {
	return roachpb.NewError(&roachpb.WriteTooOldError{
		Timestamp:       ws.txn.WriteTimestamp,
		ActualTimestamp: ws.txn.WriteTimestamp.Next(),
	})
}

func hasMinPriority(txn *enginepb.TxnMeta) bool {
	return txn != nil && txn.Priority == enginepb.MinTxnPriority
}

func hasMaxPriority(txn *roachpb.Transaction) bool {
	return txn != nil && txn.Priority == enginepb.MaxTxnPriority
}

func hasReadCommitted(txn *roachpb.Transaction) bool {
	return txn != nil && txn.IsolationLevel == util.ReadCommittedIsolation
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

var closedTimerC chan time.Time

func init() {
	closedTimerC = make(chan time.Time)
	close(closedTimerC)
}
