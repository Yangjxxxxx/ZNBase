package quotapool

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// Option 用于配置 QuotaPool.
type Option interface {
	apply(*config)
}

// AcquisitionFunc 用于配置quotapool以在发生采集后调用函数
type AcquisitionFunc func(
	ctx context.Context, poolName string, r Request, start time.Time,
)

// OnAcquisition creates 创建在获取时配置回调的选项。它通常用于记录度量。
func OnAcquisition(f AcquisitionFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.onAcquisition = f
	})
}

// OnSlowAcquisition 创建一个选项，用于在慢速采集时配置回调。
// 只能使用一个OnSlowAcquisition。如果指定了多个，则只使用最后一个。
func OnSlowAcquisition(threshold time.Duration, f SlowAcquisitionFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.slowAcquisitionThreshold = threshold
		cfg.onSlowAcquisition = f
	})
}

// LogSlowAcquisition 是SlowAcquisitionFunc.
func LogSlowAcquisition(ctx context.Context, poolName string, r Request, start time.Time) func() {
	log.Warningf(ctx, "have been waiting %s attempting to acquire %s quota",
		timeutil.Since(start), poolName)
	return func() {
		log.Infof(ctx, "acquired %s quota after %s",
			poolName, timeutil.Since(start))
	}
}

// SlowAcquisitionFunc 用于配置quotapool以在配额获取缓慢时调用函数。
// 当发生获取时调用返回的回调
type SlowAcquisitionFunc func(
	ctx context.Context, poolName string, r Request, start time.Time,
) (onAcquire func())

type optionFunc func(cfg *config)

func (f optionFunc) apply(cfg *config) { f(cfg) }

type config struct {
	onAcquisition            AcquisitionFunc
	onSlowAcquisition        SlowAcquisitionFunc
	slowAcquisitionThreshold time.Duration
}
