package quotapool_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/quotapool"
)

// logSlowAcquisition 是记录慢速采集的选项。
var logSlowAcquisition = quotapool.OnSlowAcquisition(2*time.Second, quotapool.LogSlowAcquisition)

// TestQuotaPoolBasic 使用不同大小的配额池和不同数量的goroutine测试配额池的最小预期行为，
// 每个goroutine都获取一个单位配额并在之后立即释放它。
func TestQuotaPoolBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	quotas := []uint64{1, 10, 100, 1000}
	goroutineCounts := []int{1, 10, 100}

	for _, quota := range quotas {
		for _, numGoroutines := range goroutineCounts {
			qp := quotapool.NewIntPool("test", quota)
			ctx := context.Background()
			resCh := make(chan error, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func() {
					alloc, err := qp.Acquire(ctx, 1)
					if err != nil {
						resCh <- err
						return
					}
					alloc.Release()
					resCh <- nil
				}()
			}

			for i := 0; i < numGoroutines; i++ {
				select {
				case <-time.After(5 * time.Second):
					t.Fatal("did not complete acquisitions within 5s")
				case err := <-resCh:
					if err != nil {
						t.Fatal(err)
					}
				}
			}

			if q := qp.ApproximateQuota(); q != quota {
				t.Fatalf("expected quota: %d, got: %d", quota, q)
			}
		}
	}
}

// TestQuotaPoolContextCancellation 测试正在进行的被阻止的获取的行为，
// 如果传入的上下文被取消，则获取也将被取消，并显示错误。这不应影响池中的可用配额
func TestQuotaPoolContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	qp := quotapool.NewIntPool("test", 1)
	alloc, err := qp.Acquire(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, canceledErr := qp.Acquire(ctx, 1)
		errCh <- canceledErr
	}()

	cancel()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellation did not unblock acquisitions within 5s")
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context cancellation error, got %v", err)
		}
	}

	alloc.Release()

	if q := qp.ApproximateQuota(); q != 1 {
		t.Fatalf("expected quota: 1, got: %d", q)
	}
}

// TestQuotaPoolClose 测试以下行为：对于正在进行的被阻止的收购，
// 如果配额池关闭，则所有正在进行的收购和后续收购都返回*ErrClosed。
func TestQuotaPoolClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := quotapool.NewIntPool("test", 1)
	if _, err := qp.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	const numGoroutines = 5
	resCh := make(chan error, numGoroutines)

	tryAcquire := func() {
		_, err := qp.Acquire(ctx, 1)
		resCh <- err
	}
	for i := 0; i < numGoroutines; i++ {
		go tryAcquire()
	}

	qp.Close("")

	// Second call should be a no-op.
	qp.Close("")

	for i := 0; i < numGoroutines; i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("quota pool closing did not unblock acquisitions within 5s")
		case err := <-resCh:
			if _, isErrClosed := err.(*quotapool.ErrClosed); !isErrClosed {
				t.Fatal(err)
			}
		}
	}

	go tryAcquire()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("quota pool closing did not unblock acquisitions within 5s")
	case err := <-resCh:
		if _, isErrClosed := err.(*quotapool.ErrClosed); !isErrClosed {
			t.Fatal(err)
		}
	}
}

// TestQuotaPoolCanceledAcquisitions 测试使用已取消的上下文将多个获取排队的行为，
// 并期望使用有效上下文的任何后续获取无误地继续进行。
func TestQuotaPoolCanceledAcquisitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	qp := quotapool.NewIntPool("test", 1)
	alloc, err := qp.Acquire(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	cancel()
	const numGoroutines = 5

	errCh := make(chan error)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			_, err := qp.Acquire(ctx, 1)
			errCh <- err
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("context cancellations did not unblock acquisitions within 5s")
		case err := <-errCh:
			if err != context.Canceled {
				t.Fatalf("expected context cancellation error, got %v", err)
			}
		}
	}

	alloc.Release()
	go func() {
		_, err := qp.Acquire(context.Background(), 1)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition didn't go through within 5s")
	}
}

// TestQuotaPoolNoops 测试应该是noop的配额池操作是这样的，例如。收购报价单（0）和报价单发布（0）。
func TestQuotaPoolNoops(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qp := quotapool.NewIntPool("test", 1)
	ctx := context.Background()
	initialAlloc, err := qp.Acquire(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	// 在释放initialAlloc之前，将阻止对blockedAlloc的获取。
	errCh := make(chan error)
	var blockedAlloc *quotapool.IntAlloc
	go func() {
		blockedAlloc, err = qp.Acquire(ctx, 1)
		errCh <- err
	}()

	// 不应阻止零的分配。
	emptyAlloc, err := qp.Acquire(ctx, 0)
	if err != nil {
		t.Fatalf("failed to acquire 0 quota: %v", err)
	}
	emptyAlloc.Release() // 0的释放不应执行任何操作

	initialAlloc.Release()
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellations did not unblock acquisitions within 5s")
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	}
	if q := qp.ApproximateQuota(); q != 0 {
		t.Fatalf("expected quota: 0, got: %d", q)
	}
	blockedAlloc.Release()
	if q := qp.ApproximateQuota(); q != 1 {
		t.Fatalf("expected quota: 1, got: %d", q)
	}
}

// TestQuotaPoolMaxQuota 获取的测试不能获取超过池初始化时的最大数量。
func TestQuotaPoolMaxQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 100
	qp := quotapool.NewIntPool("test", quota)
	ctx := context.Background()
	alloc, err := qp.Acquire(ctx, 2*quota)
	if err != nil {
		t.Fatal(err)
	}
	if got := alloc.Acquired(); got != quota {
		t.Fatalf("expected to acquire the capacity quota %d, instead got %d", quota, got)
	}
	alloc.Release()
	if q := qp.ApproximateQuota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

// TestQuotaPoolCappedAcquisition 验证当发出的获取请求大于最大配额时，我们仍然允许获取继续进行，但在获取最大配额量之后。
func TestQuotaPoolCappedAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 1
	qp := quotapool.NewIntPool("test", quota)
	alloc, err := qp.Acquire(context.Background(), quota*100)
	if err != nil {
		t.Fatal(err)
	}

	if q := qp.ApproximateQuota(); q != 0 {
		t.Fatalf("expected quota: %d, got: %d", 0, q)
	}

	alloc.Release()
	if q := qp.ApproximateQuota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

func TestOnAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 100
	var called bool
	qp := quotapool.NewIntPool("test", quota,
		quotapool.OnAcquisition(func(ctx context.Context, poolName string, _ quotapool.Request, start time.Time,
		) {
			assert.Equal(t, poolName, "test")
			called = true
		}))
	ctx := context.Background()
	alloc, err := qp.Acquire(ctx, 1)
	assert.Nil(t, err)
	assert.True(t, called)
	alloc.Release()
}

// TestSlowAcquisition 确保在获取调用花费的时间超过配置的超时时调用SlowAcquisition回调。
func TestSlowAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// 测试将设置一个具有1个配额的IntPool和一个SlowAcquisition回调，当第二个goroutine调用时，该回调将关闭通道。
	// 一个初始的获取调用将占用所有配额。然后，应阻止另一个带有go的调用，从而触发回调。
	// 为了防止第一次调用Acquire触发回调，我们用一个值标记它的上下文。
	ctx := context.Background()
	type ctxKey int
	firstKey := ctxKey(1)
	firstCtx := context.WithValue(ctx, firstKey, "foo")
	slowCalled, acquiredCalled := make(chan struct{}), make(chan struct{})
	const poolName = "test"
	f := func(ctx context.Context, name string, _ quotapool.Request, _ time.Time) func() {
		assert.Equal(t, poolName, name)
		if ctx.Value(firstKey) != nil {
			return func() {}
		}
		close(slowCalled)
		return func() {
			close(acquiredCalled)
		}
	}
	qp := quotapool.NewIntPool(poolName, 1, quotapool.OnSlowAcquisition(time.Microsecond, f))
	alloc, err := qp.Acquire(firstCtx, 1)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = qp.Acquire(ctx, 1)
	}()
	select {
	case <-slowCalled:
	case <-time.After(time.Second):
		t.Fatalf("OnSlowAcquisition not called long after timeout")
	}
	select {
	case <-acquiredCalled:
		t.Fatalf("acquired callback called when insufficient quota was available")
	default:
	}
	alloc.Release()
	select {
	case <-slowCalled:
	case <-time.After(time.Second):
		t.Fatalf("OnSlowAcquisition acquired callback not called long after timeout")
	}
}

// 之后调用AcquireFunc（）的测试内冻结（）被称为-以便正在进行的收购有机会观察到其请求没有容量。
func TestQuotaPoolCapacityDecrease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qp := quotapool.NewIntPool("test", 100)
	ctx := context.Background()

	alloc50, err := qp.Acquire(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}

	first := true
	firstCh := make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		_, err = qp.AcquireFunc(ctx, func(_ context.Context, pi quotapool.PoolInfo) (took uint64, err error) {
			if first {
				first = false
				close(firstCh)
			}
			if pi.Capacity < 100 {
				return 0, fmt.Errorf("hopeless")
			}
			return 0, quotapool.ErrNotEnoughQuota
		})
		close(doneCh)
	}()

	// 等待第一次调用回调。它应该返回ErrNotEnoughQuota
	<-firstCh
	// 现在泄露配额。这将调用回调以再次调用
	alloc50.Freeze()
	<-doneCh
	if !testutils.IsError(err, "hopeless") {
		t.Fatalf("expected hopeless error, got: %v", err)
	}
}

func TestIntpoolNoWait(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := quotapool.NewIntPool("test", 2)

	acq1, err := qp.TryAcquire(ctx, 1)
	require.NoError(t, err)

	acq2, err := qp.TryAcquire(ctx, 1)
	require.NoError(t, err)

	failed, err := qp.TryAcquire(ctx, 1)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	require.Nil(t, failed)

	acq1.Release()

	failed, err = qp.TryAcquire(ctx, 2)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	require.Nil(t, failed)

	acq2.Release()

	acq5, err := qp.TryAcquire(ctx, 3)
	require.NoError(t, err)
	require.NotNil(t, acq5)

	failed, err = qp.TryAcquireFunc(ctx, func(ctx context.Context, p quotapool.PoolInfo) (took uint64, err error) {
		require.Equal(t, uint64(0), p.Available)
		return 0, quotapool.ErrNotEnoughQuota
	})
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	require.Nil(t, failed)

	acq5.Release()

	acq6, err := qp.TryAcquireFunc(ctx, func(ctx context.Context, p quotapool.PoolInfo) (took uint64, err error) {
		return 1, nil
	})
	require.NoError(t, err)

	acq6.Release()
}

// TestIntpoolRelease 测试intpool的释放方法，以确保它释放预期的内容并按照文档的方式运行。
func TestIntpoolRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	const numPools = 3
	const capacity = 3
	// 填充为full，因为它对于返回所有配额的情况很方便。
	var full [numPools]uint64
	for i := 0; i < numPools; i++ {
		full[i] = capacity
	}
	makePools := func() (pools [numPools]*quotapool.IntPool) {
		for i := 0; i < numPools; i++ {
			pools[i] = quotapool.NewIntPool(strconv.Itoa(i), capacity)
		}
		return pools
	}

	type acquisition struct {
		pool int
		q    uint64
	}
	type testCase struct {
		toAcquire   []*acquisition
		exclude     int
		releasePool int
		expQuota    [numPools]uint64
	}
	// 首先获取所有配额，然后将除后面的exclude alloc外的所有alloc释放到releasePool中，
	// 并确保池具有expQuota。最后释放其余的alloc并确保池已满。
	runTest := func(c *testCase) func(t *testing.T) {
		return func(t *testing.T) {
			pools := makePools()
			allocs := make([]*quotapool.IntAlloc, len(c.toAcquire))
			for i, acq := range c.toAcquire {
				if acq == nil {
					continue
				}
				require.Condition(t, func() (success bool) {
					return acq.q <= uint64(capacity)
				})
				alloc, err := pools[acq.pool].Acquire(ctx, acq.q)
				require.NoError(t, err)
				allocs[i] = alloc
			}
			prefix := len(allocs) - c.exclude
			pools[c.releasePool].Release(allocs[:prefix]...)
			for i, p := range pools {
				require.Equal(t, c.expQuota[i], p.ApproximateQuota())
			}
			pools[c.releasePool].Release(allocs[prefix:]...)
			for i, p := range pools {
				require.Equal(t, full[i], p.ApproximateQuota())
			}
		}
	}
	for i, c := range []testCase{
		{
			toAcquire: []*acquisition{
				{0, 1},
				{1, 2},
				{1, 1},
				nil,
			},
			expQuota: full,
		},
		{
			releasePool: 1,
			toAcquire: []*acquisition{
				{0, 1},
				{1, 2},
				{1, 1},
				nil,
			},
			expQuota: full,
		},
		{
			toAcquire: []*acquisition{
				nil,
				{0, capacity},
				{1, capacity},
				{2, capacity},
			},
			exclude:  1,
			expQuota: [numPools]uint64{0: capacity, 1: capacity},
		},
		{
			toAcquire: []*acquisition{
				nil,
				{0, capacity},
				{1, capacity},
				{2, capacity},
			},
			exclude:  3,
			expQuota: [numPools]uint64{},
		},
	} {
		t.Run(strconv.Itoa(i), runTest(&c))
	}
}

// TestLen 验证IntPool的Len（）方法是否按预期工作。
func TestLen(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qp := quotapool.NewIntPool("test", 1, logSlowAcquisition)
	ctx := context.Background()
	allocCh := make(chan *quotapool.IntAlloc)
	doAcquire := func(ctx context.Context) {
		alloc, err := qp.Acquire(ctx, 1)
		if ctx.Err() == nil && assert.Nil(t, err) {
			allocCh <- alloc
		}
	}
	assertLenSoon := func(exp int) {
		testutils.SucceedsSoon(t, func() error {
			if got := qp.Len(); got != exp {
				return errors.Errorf("expected queue len to be %d, got %d", got, exp)
			}
			return nil
		})
	}
	// 最初qp的长度应该为0
	assert.Equal(t, 0, qp.Len())
	// 从池中获取所有配额
	alloc, err := qp.Acquire(ctx, 1)
	assert.Nil(t, err)
	// 长度仍应为0
	assert.Equal(t, 0, qp.Len())
	// 启动goroutine以获取配额，确保长度增加.
	go doAcquire(ctx)
	assertLenSoon(1)
	// 创建更多的goroutines，这些goroutines稍后将被取消，以确保取消从长度中扣除
	const numToCancel = 12 // 任意数
	ctxToCancel, cancel := context.WithCancel(ctx)
	for i := 0; i < numToCancel; i++ {
		go doAcquire(ctxToCancel)
	}
	// 确保所有新的goroutine都反映在长度中
	assertLenSoon(numToCancel + 1)
	// 使用默认上下文启动另一个goroutine
	go doAcquire(ctx)
	assertLenSoon(numToCancel + 2)
	// 取消一些goroutine
	cancel()
	// 确保它们不会很快反映在长度上
	assertLenSoon(2)
	// 打开第一个goroutine
	alloc.Release()
	alloc = <-allocCh
	assert.Equal(t, 1, qp.Len())
	// 打开第二个goroutine
	alloc.Release()
	<-allocCh
	assert.Equal(t, 0, qp.Len())
}

// TestIntpoolIllegalCapacity 确保构建容量超过数学.MaxInt64会惊慌失措的
func TestIntpoolWithExcessCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, c := range []uint64{
		1, math.MaxInt64 - 1, math.MaxInt64,
	} {
		require.NotPanics(t, func() {
			quotapool.NewIntPool("test", c)
		})
	}
	for _, c := range []uint64{
		math.MaxUint64, math.MaxUint64 - 1, math.MaxInt64 + 1,
	} {
		require.Panics(t, func() {
			quotapool.NewIntPool("test", c)
		})
	}
}

// TestUpdateCapacityFluctuationsPermitExcessAllocs 练习IntPool容量波动的情况。
// 我们希望确保未完成配额的数量永远不会超过任何未完成分配获得时可用的最大容量
func TestUpdateCapacityFluctuationsPreventsExcessAllocs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := quotapool.NewIntPool("test", 1)
	var allocs []*quotapool.IntAlloc
	allocCh := make(chan *quotapool.IntAlloc)
	defer close(allocCh)
	go func() {
		for a := range allocCh {
			if a == nil {
				continue //	允许nil通道发送同步
			}
			allocs = append(allocs, a)
		}
	}()
	acquireN := func(n int) {
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				alloc, err := qp.Acquire(ctx, 1)
				assert.NoError(t, err)
				allocCh <- alloc
			}()
		}
		wg.Wait()
	}

	acquireN(1)
	qp.UpdateCapacity(100)
	acquireN(99)
	require.Equal(t, uint64(0), qp.ApproximateQuota())
	qp.UpdateCapacity(1)
	// 在内部，配额的表示应该是-99，但我们不会向用户公开负配额。
	require.Equal(t, uint64(0), qp.ApproximateQuota())

	// 更新容量，现在实际上是0。
	qp.UpdateCapacity(100)
	require.Equal(t, uint64(0), qp.ApproximateQuota())
	_, err := qp.TryAcquire(ctx, 1)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)

	// 现在更新容量，使其为1。
	qp.UpdateCapacity(101)
	require.Equal(t, uint64(1), qp.ApproximateQuota())
	_, err = qp.TryAcquire(ctx, 2)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	acquireN(1)
	allocCh <- nil // 使同步
	// 将所有配额释放回池中
	for _, a := range allocs {
		a.Release()
	}
	allocs = nil
	require.Equal(t, uint64(101), qp.ApproximateQuota())
}

func TestQuotaPoolUpdateCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	ch := make(chan *quotapool.IntAlloc, 1)
	qp := quotapool.NewIntPool("test", 1)
	alloc, err := qp.Acquire(ctx, 1)
	require.NoError(t, err)
	go func() {
		blocked, err := qp.Acquire(ctx, 2)
		assert.NoError(t, err)
		ch <- blocked
	}()
	ensureBlocked := func() {
		t.Helper()
		select {
		case <-time.After(10 * time.Millisecond): // 确保收购暂时失败
		case got := <-ch:
			got.Release()
			t.Fatal("expected acquisition to fail")
		}
	}
	ensureBlocked()
	// 将容量更新为2，请求仍应被阻止
	qp.UpdateCapacity(2)
	ensureBlocked()
	qp.UpdateCapacity(3)
	got := <-ch
	require.Equal(t, uint64(2), got.Acquired())
	require.Equal(t, uint64(3), qp.Capacity())
	alloc.Release()
	require.Equal(t, uint64(1), qp.ApproximateQuota())
}

func TestConcurrentUpdatesAndAcquisitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var wg sync.WaitGroup
	const maxCap = 100
	qp := quotapool.NewIntPool("test", maxCap, logSlowAcquisition)
	const N = 100
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			newCap := uint64(rand.Intn(maxCap-1)) + 1
			qp.UpdateCapacity(newCap)
		}()
	}
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			acq, err := qp.Acquire(ctx, uint64(rand.Intn(maxCap)))
			assert.NoError(t, err)
			runtime.Gosched()
			acq.Release()
		}()
	}
	wg.Wait()
	qp.UpdateCapacity(maxCap)
	assert.Equal(t, uint64(maxCap), qp.ApproximateQuota())
}

// 此测试确保如果您尝试冻结alloc，这将使IntPool具有负容量，则会发生死机。
func TestFreezeUnavailableCapacityPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := quotapool.NewIntPool("test", 10)
	acq, err := qp.Acquire(ctx, 10)
	require.NoError(t, err)
	qp.UpdateCapacity(9)
	require.Panics(t, func() {
		acq.Freeze()
	})
}
