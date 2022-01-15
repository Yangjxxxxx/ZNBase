package followerreadsicl

import (
	"fmt"
	"time"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/icl/utilicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan/replicastrategy"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/closedts"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// followerReadMultiple是kv.closed_timestmap.target_duration的倍数
//跟随者阅读复制策略的实现应该
//用来确定一个请求是否可以用于读取。
// FollowerReadMultiple是一个隐藏的设置。
var followerReadMultiple = func() *settings.FloatSetting {
	s := settings.RegisterValidatedFloatSetting(
		"kv.follower_read.target_multiple",
		"if above 1, encourages the distsender to perform a read against the "+
			"closest replica if a request is older than kv.closed_timestamp.target_duration"+
			" * (1 + kv.closed_timestamp.close_fraction * this) less a clock uncertainty "+
			"interval. This value also is used to create follower_timestamp().",
		3,
		func(v float64) error {
			if v < 1 {
				return fmt.Errorf("%v is not >= 1", v)
			}
			return nil
		},
	)
	s.SetSensitive()
	return s
}()

// getFollowerReadDuration返回应该用作的偏移持续时间
////从现在开始的偏移请求一个关注者读。相同的值减去时钟
////不确定度，则在kv层上确定一个查询是否可以使用一个
/// /追随者阅读。
func getReadDurationWithFollower(st *cluster.Settings) time.Duration {
	targetMultiple := followerReadMultiple.Get(&st.SV)
	targetDuration := closedts.TargetDuration.Get(&st.SV)
	closeFraction := closedts.CloseFraction.Get(&st.SV)
	return -1 * time.Duration(float64(targetDuration)*
		(1+closeFraction*targetMultiple))
}

func checkCommercialFeatureEnabled(clusterID uuid.UUID, st *cluster.Settings) error {
	org := sql.ClusterOrganization.Get(&st.SV)
	return utilicl.CheckCommercialFeatureEnabled(st, clusterID, org, "follower reads")
}

func evalReadOffsetWithFollower(clusterID uuid.UUID, st *cluster.Settings) (time.Duration, error) {
	if err := checkCommercialFeatureEnabled(clusterID, st); err != nil {
		return 0, err
	}
	return getReadDurationWithFollower(st), nil
}

// canBeEvaluatedOnFollowerWithBatch 确定一个批处理是否只包含
// 可以在关注者副本上评估的请求。
func canBeEvaluatedOnFollowerWithBatch(ba roachpb.BatchRequest) bool {
	return ba.IsReadOnly() && ba.IsAllTransactional()
}

// 确定提供的事务是否可以执行
func isTxnFollowerReadCanPerform(txn *roachpb.Transaction) bool {
	return txn != nil && !txn.IsLocking()
}

// isUseFollowerRead 确定是否可以将查询发送给关注者。
func isUseFollowerRead(clusterID uuid.UUID, st *cluster.Settings, ts hlc.Timestamp) bool {
	if !storage.FollowerReadsEnabled.Get(&st.SV) {
		return false
	}
	threshold := (-1 * getReadDurationWithFollower(st)) - 1*base.DefaultMaxClockOffset
	if timeutil.Since(ts.GoTime()) < threshold {
		return false
	}
	return checkCommercialFeatureEnabled(clusterID, st) == nil
}

// isSendToFollower 实现检查批处理请求是否存在的逻辑
// 可能被发送给一个追随者。
func isSendToFollower(clusterID uuid.UUID, st *cluster.Settings, ba roachpb.BatchRequest) bool {
	return canBeEvaluatedOnFollowerWithBatch(ba) &&
		isTxnFollowerReadCanPerform(ba.Txn) &&
		isUseFollowerRead(clusterID, st, forward(ba.Txn.ReadTimestamp, ba.Txn.MaxTimestamp))
}

func forward(ts hlc.Timestamp, to hlc.Timestamp) hlc.Timestamp {
	ts.Forward(to)
	return ts
}

type strategyFactory struct {
	clusterID *base.ClusterIDContainer
	st        *cluster.Settings

	binPacking replicastrategy.StrategyFactory
	closest    replicastrategy.StrategyFactory
}

func newStrategyFactory(cfg replicastrategy.Config) replicastrategy.StrategyFactory {
	return &strategyFactory{
		clusterID:  &cfg.RPCContext.ClusterID,
		st:         cfg.Settings,
		binPacking: replicastrategy.NewStrategyFactory(replicastrategy.BinPackingChoice, cfg),
		closest:    replicastrategy.NewStrategyFactory(replicastrategy.ClosestChoice, cfg),
	}
}

func (f strategyFactory) Oracle(txn *client.Txn) replicastrategy.Strategy {
	if txn != nil && isUseFollowerRead(f.clusterID.Get(), f.st, txn.ReadTimestamp()) {
		return f.closest.Oracle(txn)
	}
	return f.binPacking.Oracle(txn)
}

// followerReadAwareChoice 承租人是否正在选择检测的保单
// 查询是否可以与关注者一起使用。
var followerReadAwareChoice = replicastrategy.RegisterPolicy(newStrategyFactory)

func init() {
	sql.ReplicaOraclePolicy = followerReadAwareChoice
	builtins.EvalFollowerReadOffset = evalReadOffsetWithFollower
	kv.CanSendToFollower = isSendToFollower
}
