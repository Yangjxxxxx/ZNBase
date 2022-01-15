// Copyright 2014  The Cockroach Authors.
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

package batcheval

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/storage/txnwait"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
)

func init() {
	RegisterCommand(roachpb.PushTxn, declareKeysPushTransaction, PushTxn)
}

func declareKeysPushTransaction(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	pr := req.(*roachpb.PushTxnRequest)
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(pr.PusheeTxn.Key, pr.PusheeTxn.ID)})
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, pr.PusheeTxn.ID)})
}

// PushTxn resolves conflicts between concurrent txns (or between
// a non-transactional reader or writer and a txn) in several ways,
// depending on the statuses and priorities of the conflicting
// transactions. The PushTxn operation is invoked by a "pusher"
// (args.PusherTxn -- the writer trying to abort a conflicting txn
// or the reader trying to push a conflicting txn's commit timestamp
// forward), who attempts to resolve a conflict with a "pushee"
// (args.PusheeTxn -- the pushee txn whose intent(s) caused the
// conflict). A pusher is either transactional, in which case
// PusherTxn is completely initialized, or not, in which case the
// PusherTxn has only the priority set.
// PusherTxn如果是事务性的，在这种情况下PusherTxn被完全初始化；如果不是事务性的，在这种情况下PusherTxn就只设置了优先级。
//
// The request arrives and immediately tries to determine the current
// disposition of the pushee transaction by reading its transaction
// record.
// 该请求到达，并立即尝试通过读取其事务记录来确定请求处理的当前处置。
// If it finds one, it continues with the push.
// 如果找到了位置，就继续push事务
// If not, it
// uses knowledge from the existence of the conflicting intent to
// determine the current state of the pushee.
// 如果没有的话，则使用存在冲突意图的知识来确定pushee的当前状态。
// It's possible that the
// transaction record is missing either because it hasn't been written
// yet or because it has already been GCed after being finalized.
// 可能是由于尚未写入事务记录，或者因为在完成后已经对其进行了GC处理，所以缺少事务记录。
// Once
// the request determines which case its in, it decides whether to
// continue with the push.
// 一旦请求确定了哪种情况，它就会决定是否继续进行push
// There are a number of different outcomes
// that a push can result in, based on the state that the pushee's
// transaction record is found in:
// 根据在以下位置找到pushee事务的交易记录的状态，push操作可能会导致许多不同的结果：
//
// Txn already committed/aborted: If the pushee txn is committed or
// aborted return success.
// 事务已经被commit或者abort：如果pushee事务被提交或者abort返回success了
//
// Txn record expired: If the pushee txn is pending, its last
// heartbeat timestamp is observed to determine the latest client
// activity.
// Txn记录已过期：如果pushee事务处于pending状态，则将观察其上一个心跳时间戳以确定最新的客户端活动。
// This heartbeat is forwarded by the conflicting intent's
// timestamp because that timestamp also indicates definitive client
// activity.
// 该心跳由冲突的意图的时间戳转发，因为该时间戳也指示确定的客户端活动。
// This time of "last activity" is compared against the
// current time to determine whether the transaction has expired.
// 将“last activity”的时间与当前时间进行比较，以确定交易是否已过期。
// If so, it is aborted. NOTE: the intent timestamp used is not
// updated on intent pushes.
// 如果过期了的话，就abort掉
// This is important because it allows us
// to use its timestamp as an indication of recent activity.
// 这很重要，因为它允许我们将其时间戳记用作最近活动的指示。
// If this
// is ever changed, we don't run the risk of any correctness violations,
// but we do make it possible for intent pushes to look like client
// activity and extend the waiting period until a transaction is
// considered expired.
// 如果这已经改变，我们就不会冒任何违反正确性的风险，但是我们确实使intent推送看起来像客户端活动，并延长了等待时间，直到事务被视为过期为止。
// This waiting period is a "courtesy" - if we
// simply aborted txns right away then we would see worse performance
// under contention, but everything would still be correct.
// 这个等待期是一个“courtesy”-如果我们只是立即中止txns，那么在争用中我们将看到较差的性能，但是一切仍然正确。
// Txn record not expired: If the pushee txn is not expired, its
// priority is compared against the pusher's (see CanPushWithPriority).
// Txn记录未到期：如果推送对象txn尚未到期，则将其优先级与推送程序的优先级进行比较（请参见CanPushWithPriority）。
// Push cannot proceed: a TransactionPushError is returned.
// push无法继续：返回TransactionPushError。
//
// Push can proceed but txn record staging: if the transaction record
// is STAGING then it can't be changed by a pusher without going through
// the transaction recovery process. An IndeterminateCommitError is returned
// to kick off recovery.
// 可以继续进行push，但可以执行事务记录暂存：如果事务记录是STAGING，则在不经过事务恢复过程的情况下，
// 推送程序将无法更改它。返回IndeterminateCommitError以启动恢复。
// Push can proceed: the pushee's transaction record is modified and
// rewritten, based on the value of args.PushType. If args.PushType
// is PUSH_ABORT, txn.Status is set to ABORTED. If args.PushType is
// PUSH_TIMESTAMP, txn.Timestamp is set to just after args.PushTo.
// 推送可以继续进行：根据args.PushType的值，修改并重写pushee事务的交易记录。如果args.PushType为PUSH_ABORT，
// 则txn.Status设置为ABORTED。如果args.PushType为PUSH_TIMESTAMP，则将txn.Timestamp设置为仅在args.PushTo之后。
// If the pushee is aborted, its timestamp will be forwarded to match
// its last client activity timestamp (i.e. last heartbeat), if available.
// 如果pushee事务被abort掉了，pushee事务将转发其时间戳以匹配其上次客户端活动时间戳（即上一次心跳）（如果有）。
// This is done so that the updated timestamp populates the AbortSpan when
// the pusher proceeds to resolve intents, allowing the GC queue to purge
// records for which the transaction coordinator must have found out via
// its heartbeats that the transaction has failed.
// 这样做是为了使当pusher事务继续解析intent时，更新的时间戳会填充AbortSpan，从而允许GC队列清除
// 必须由transaction coordinator通过其心跳为其发现事务失败的记录。
func PushTxn(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.PushTxnRequest) //获取到pushTxn请求的参数信息
	h := cArgs.Header                            //获取到参数头
	reply := resp.(*roachpb.PushTxnResponse)

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}

	if h.Timestamp.Less(args.PushTo) {
		// Verify that the PushTxn's timestamp is not less than the timestamp that
		// the request intends to push the transaction to. Transactions should not
		// be pushed into the future or their effect may not be fully reflected in
		// a future leaseholder's timestamp cache. This is analogous to how reads
		// should not be performed at a timestamp in the future.
		return result.Result{}, errors.Errorf("request timestamp %s less than PushTo timestamp %s", h.Timestamp, args.PushTo)
	}
	if h.Timestamp.Less(args.PusheeTxn.WriteTimestamp) {
		// This condition must hold for the timestamp cache access/update to be safe.
		return result.Result{}, errors.Errorf("request timestamp %s less than pushee txn timestamp %s", h.Timestamp, args.PusheeTxn.WriteTimestamp)
	}
	now := cArgs.EvalCtx.Clock().Now()
	if now.Less(h.Timestamp) {
		// The batch's timestamp should have been used to update the clock.
		return result.Result{}, errors.Errorf("request timestamp %s less than current clock time %s", h.Timestamp, now)
	}
	if !bytes.Equal(args.Key, args.PusheeTxn.Key) {
		return result.Result{}, errors.Errorf("request key %s should match pushee txn key %s", args.Key, args.PusheeTxn.Key)
	}
	key := keys.TransactionKey(args.PusheeTxn.Key, args.PusheeTxn.ID)
	//pushee事务对象是对应了事务记录对象，pusher事务对象对应了事务对象

	// Fetch existing transaction; if missing, we're allowed to abort.
	// 获取存在的事务对象；如果没有获取到的话，我们就可以abort
	var existTxn roachpb.Transaction
	ok, err := engine.MVCCGetProto(ctx, batch, key, hlc.Timestamp{}, &existTxn, engine.MVCCGetOptions{}) //去获取事务记录信息
	//如果是通过jdbc连接ZNBase的情况下，ok值一直为false；如果是通过终端SQL语句操作的话，ok值一直为true
	if err != nil {
		return result.Result{}, err
	} else if !ok {
		// 如果没有获取到指定事务信息的话，就会涉及到事务记录也没有获取到的问题，下面列举事务记录没有获取到的可能原因:
		// There are three cases in which there is no transaction record:
		//
		// * the pushee is still active but its transaction record has not
		//   been written yet. This is fairly common because transactions
		//   do not eagerly write their transaction record before writing
		//   intents, which another reader or writer might stumble upon and
		//   be forced to push.
		// * the pushee resolved its intents synchronously on successful commit;
		//   in this case, the transaction record of the pushee is also removed.
		//   Note that in this case, the intent which prompted this PushTxn
		//   doesn't exist any more.
		// * the pushee timed out or was aborted and the intent not cleaned up,
		//   but the transaction record was garbage collected.
		//
		// To determine which case we're in, we check whether the transaction could
		// ever write a transaction record. We do this by using the metadata from
		// the intent and attempting to synthesize a transaction record while
		// verifying that it would be possible for the transaction record to ever be
		// written. If a transaction record for the transaction could be written in
		// the future then we must be in the first case. If one could not be written
		// then we know we're in either the second or the third case.
		//
		// Performing this detection could have false positives where we determine
		// that a record could still be written and conclude that we're in the first
		// case. However, it cannot have false negatives where we determine that a
		// record can not be written and conclude that we're in the second or third
		// case. This is important, because it means that we may end up failing to
		// push a finalized transaction but will never determine that a transaction
		// is finalized when it still could end up committing.
		reply.PusheeTxn = SynthesizeTxnFromMeta(cArgs.EvalCtx, args.PusheeTxn)
		//通过pushee事务的事务记录元数据信息合成一个新的事务记录对象
		if reply.PusheeTxn.Status == roachpb.ABORTED {
			//如果事务状态是aborted的话
			// If the transaction is uncommittable, we don't even need to
			// persist an ABORTED transaction record, we can just consider it
			// aborted. This is good because it allows us to obey the invariant
			// that only the transaction's own coordinator can create its
			// transaction record.
			// 如果事务没有被提交的话，我们甚至不需要保留abort状态的事务记录，我们可以认为它已中止。
			// 这很好，因为它使我们服从不变，即只有交易本身的coordinator才能创建其交易记录。
			result := result.Result{}
			result.Local.UpdatedTxns = []*roachpb.Transaction{&reply.PusheeTxn}
			return result, nil
		}

		// 过载不一致问题的一种可能修复
		ok = args.PusheeTxn.IsolationLevel == util.ReadCommittedIsolation //如果pushee事务的隔离级别为读提交的话
	} else {
		//正常的代码逻辑
		// Start with the persisted transaction record.
		reply.PusheeTxn = existTxn

		// Forward the last heartbeat time of the transaction record by
		// the timestamp of the intent. This is another indication of
		// client activity.
		reply.PusheeTxn.LastHeartbeat.Forward(args.PusheeTxn.WriteTimestamp)
	}

	// If already committed or aborted, return success.
	if reply.PusheeTxn.Status.IsFinalized() {
		// Trivial noop.
		return result.Result{}, nil
	}

	// If we're trying to move the timestamp forward, and it's already
	// far enough forward, return success.
	if args.PushType == roachpb.PUSH_TIMESTAMP && !reply.PusheeTxn.WriteTimestamp.Less(args.PushTo) {
		// Trivial noop.
		return result.Result{}, nil
	}

	// The pusher might be aware of a newer version of the pushee.
	increasedEpochOrTimestamp := false
	if reply.PusheeTxn.WriteTimestamp.Less(args.PusheeTxn.WriteTimestamp) {
		reply.PusheeTxn.WriteTimestamp = args.PusheeTxn.WriteTimestamp
		increasedEpochOrTimestamp = true
	}
	if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
		reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
		increasedEpochOrTimestamp = true
	}
	reply.PusheeTxn.UpgradePriority(args.PusheeTxn.Priority)

	// If the pusher is aware that the pushee's currently recorded attempt at a
	// parallel commit failed, either because it found intents at a higher
	// timestamp than the parallel commit attempt or because it found intents at
	// a higher epoch than the parallel commit attempt, it should not consider
	// the pushee to be performing a parallel commit. Its commit status is not
	// indeterminate.
	if increasedEpochOrTimestamp && reply.PusheeTxn.Status == roachpb.STAGING {
		reply.PusheeTxn.Status = roachpb.PENDING
		reply.PusheeTxn.InFlightWrites = nil
	}

	pushType := args.PushType
	var pusherWins bool
	var reason string

	switch {
	case txnwait.IsExpired(now, &reply.PusheeTxn):
		reason = "pushee is expired"
		// When cleaning up, actually clean up (as opposed to simply pushing
		// the garbage in the path of future writers).
		pushType = roachpb.PUSH_ABORT
		pusherWins = true
	case pushType == roachpb.PUSH_TOUCH:
		// If just attempting to cleanup old or already-committed txns,
		// pusher always fails.
		pusherWins = false
	case CanPushWithPriority(&args.PusherTxn, &reply.PusheeTxn):
		reason = "pusher has priority"
		pusherWins = true
	case args.Force: //如果是强制推事务的话
		reason = "forced push"
		pusherWins = true
	case args.PusherTxn.IsolationLevel == util.ReadCommittedIsolation && pushType == roachpb.PUSH_TIMESTAMP:
		reason = "rc read push"
		pusherWins = true
	}

	if log.V(1) && reason != "" {
		s := "pushed"
		if !pusherWins {
			s = "failed to push"
		}
		log.Infof(ctx, "%s "+s+" (push type=%s) %s: %s (pushee last active: %s)",
			args.PusherTxn.Short(), pushType, args.PusheeTxn.Short(),
			reason, reply.PusheeTxn.LastActive())
	}

	// If the pushed transaction is in the staging state, we can't change its
	// record without first going through the transaction recovery process and
	// attempting to finalize it.
	recoverOnFailedPush := cArgs.EvalCtx.EvalKnobs().RecoverIndeterminateCommitsOnFailedPushes
	if reply.PusheeTxn.Status == roachpb.STAGING && (pusherWins || recoverOnFailedPush) {
		err := roachpb.NewIndeterminateCommitError(reply.PusheeTxn)
		if log.V(1) {
			log.Infof(ctx, "%v", err)
		}
		return result.Result{}, err
	}

	if !pusherWins {
		err := roachpb.NewTransactionPushError(reply.PusheeTxn)
		if log.V(1) {
			log.Infof(ctx, "%v", err)
		}
		return result.Result{}, err
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(args.PusherTxn.Priority - 1)

	// Determine what to do with the pushee, based on the push type.
	// 基于push请求的类型，来决定给pushee事务怎样的操作
	switch pushType {
	case roachpb.PUSH_ABORT:
		// If aborting the transaction, set the new status.
		if ok {
			reply.PusheeTxn.WriteTimestamp.Forward(reply.PusheeTxn.LastActive())
		}
		if args.Force {
			reply.PusheeTxn.IsDeadlock = true
			ok = true
		}
		reply.PusheeTxn.Status = roachpb.ABORTED
		// If the transaction record was already present, forward the timestamp
		// to accommodate AbortSpan GC. See method comment for details.
	case roachpb.PUSH_TIMESTAMP:
		// Otherwise, update timestamp to be one greater than the request's
		// timestamp. This new timestamp will be use to update the read
		// timestamp cache. If the transaction record was not already present
		// then we rely on the read timestamp cache to prevent the record from
		// ever being written with a timestamp beneath this timestamp.
		reply.PusheeTxn.WriteTimestamp.Forward(args.PushTo)
	default:
		return result.Result{}, errors.Errorf("unexpected push type: %v", pushType)
	}

	// If the transaction record was already present, persist the updates to it.
	// If not, then we don't want to create it.
	// 如果事务记录已经存在，请对其进行持久化更新。如果没有，那么我们不想创建它。
	// This could allow for finalized
	// transactions to be revived.
	// 这可以使已完成的交易得以恢复。
	// Instead, we obey the invariant that only the
	// transaction's own coordinator can issue requests that create its
	// transaction record.
	// 相反，我们服从不变的原则，即只有交易本身的协调器才能发出创建其交易记录的请求。
	// To ensure that a timestamp push or an abort is
	// respected for transactions without transaction records, we rely on the
	// read and write timestamp cache, respectively.
	//
	if ok {
		txnRecord := reply.PusheeTxn.AsRecord()
		if err := engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord); err != nil {
			return result.Result{}, err
		}
	}

	result := result.Result{}
	result.Local.UpdatedTxns = []*roachpb.Transaction{&reply.PusheeTxn}
	return result, nil
}
