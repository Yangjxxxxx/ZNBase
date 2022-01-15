package quotapool

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
)

// IntPool 管理向客户端分配整数配额单位。客户机可以通过两种方式获取配额，
// 一种是acquire，它要求客户机指定调用时的配额数量；
// 另一种是acquire func，它允许客户机提供一个函数，用于确定配额数量在可用时是否足够。
type IntPool struct {
	qp *QuotaPool

	// 容量保持有多少总配额（不一定可用）。
	// 原子访问！容量最初是在构建IntPool时设置的，然后可以通过内冻结().
	capacity uint64

	// updateCapacityMu同步对容量的访问。
	updateCapacityMu syncutil.Mutex
}

// IntAlloc 是应发布的已分配数量。
type IntAlloc struct {
	// 当用作IntPool的当前配额或降低容量时，alloc可能为负（请参阅UpdateCapacity）。
	// 此包的客户端获取的alloc永远不会为负
	alloc int64
	p     *IntPool
}

// Release 将IntAlloc释放回IntPool。放进一个封闭的池子里是安全的。
func (ia *IntAlloc) Release() {
	ia.p.qp.Add((*intAlloc)(ia))
}

// Acquired 返回此分配已获取的数量。
func (ia *IntAlloc) Acquired() uint64 {
	return uint64(ia.alloc)
}

// Merge 将“其他”中获取的资源添加到IntAlloc。
// 其他在合并后不能使用。合并来自不同池的alloc是非法的，这样做会导致恐慌。
func (ia *IntAlloc) Merge(other *IntAlloc) {
	if ia.p != other.p {
		panic("cannot merge IntAllocs from two different pools")
	}
	ia.alloc = min(int64(ia.p.Capacity()), ia.alloc+other.alloc)
	ia.p.putIntAlloc(other)
}

// Freeze 通知配额池此分配永远不会被释放（）。以后释放它是非法的，将导致恐慌。
//
// 在同一个池上使用Freeze和UpdateCapacity可能需要显式协调。
// 冻结不再可用的已分配容量是非法的—特别是将IntPool的容量设为负值是非法的。
// 想象一下，一个IntPool的容量最初是10。获得10的分配。然后，在保留时，池的容量将更新为9。
// 然后冻结未分配的款项。这将使IntPool的总容量为负值，这是不允许的，并将导致恐慌。
// 一般来说，冻结池中的已分配配额是一个坏主意，因为它的容量将永远下降。
//
// AcquireFunc（）请求将以更新的容量唤醒，Alloc（）请求将相应地被修剪。
func (ia *IntAlloc) Freeze() {
	ia.p.decCapacity(uint64(ia.alloc))
	ia.p = nil // ensure that future uses of this alloc will panic
}

// String 将IntAlloc格式化为字符串.
func (ia *IntAlloc) String() string {
	if ia == nil {
		return strconv.Itoa(0)
	}
	return strconv.FormatInt(ia.alloc, 10)
}

// from 如果此IntAlloc来自p，则返回true.
func (ia *IntAlloc) from(p *IntPool) bool {
	return ia.p == p
}

// intAlloc 用于使IntAlloc实现资源而不干扰其导出接口。
type intAlloc IntAlloc

// Merge 合并intAlloc资源.
func (ia *intAlloc) Merge(other Resource) {
	(*IntAlloc)(ia).Merge((*IntAlloc)(other.(*intAlloc)))
}

// NewIntPool 创建一个新的名为IntPool。
// 容量是最初可用的配额量。最大容量为数学.MaxInt64. 如果capacity参数超过该值，则此函数将panic。
func NewIntPool(name string, capacity uint64, options ...Option) *IntPool {
	assertCapacityIsValid(capacity)
	p := IntPool{
		capacity: capacity,
	}
	p.qp = New(name, (*intAlloc)(p.newIntAlloc(int64(capacity))), options...)
	return &p
}

// Acquire 获取从池中获取指定数量的配额。成功时，将返回非nil alloc，必须对其调用Release（）才能将配额返回池。
// 如果“v”大于池的总容量，则尝试获取等于最大容量的配额。如果在执行此请求时降低了最大容量，则请求将再次截断为最大容量。
// 即使关闭了IntPool，0的获取也会立即无错误地返回。
// 可并发使用。
func (p *IntPool) Acquire(ctx context.Context, v uint64) (*IntAlloc, error) {
	return p.acquireMaybeWait(ctx, v, true /* wait */)
}

// TryAcquire 类似于Acquire，但如果配额不足，无法立即获取，则方法将返回ErrNotEnoughQuota。
func (p *IntPool) TryAcquire(ctx context.Context, v uint64) (*IntAlloc, error) {
	return p.acquireMaybeWait(ctx, v, false /* wait */)
}

func (p *IntPool) acquireMaybeWait(ctx context.Context, v uint64, wait bool) (*IntAlloc, error) {
	// 大小为0的特例采集。
	if v == 0 {
		return p.newIntAlloc(0), nil
	}
	// 最大容量为数学.MaxInt64所以我们总是可以将请求截断为该值。
	if v > math.MaxInt64 {
		v = math.MaxInt64
	}
	r := p.newIntRequest(v)
	defer p.putIntRequest(r)
	var req Request
	if wait {
		req = r
	} else {
		req = (*intRequestNoWait)(r)
	}
	if err := p.qp.Acquire(ctx, req); err != nil {
		return nil, err
	}
	return p.newIntAlloc(int64(r.want)), nil
}

// Release 将释放所有回他们的池。来自p的alloc在添加之前合并为单个alloc，以避免在p上多次同步。
// 不是来自p的Allocs一次释放一个。在alloc中传递nil值是合法的。
func (p *IntPool) Release(allocs ...*IntAlloc) {
	var toRelease *IntAlloc
	for _, alloc := range allocs {
		switch {
		case alloc == nil:
			continue
		case !alloc.from(p):
			// 如果alloc不是来自p，则直接对其调用Release（）.
			alloc.Release()
			continue
		case toRelease == nil:
			toRelease = alloc
		default:
			toRelease.Merge(alloc)
		}
	}
	if toRelease != nil {
		toRelease.Release()
	}
}

// IntRequestFunc 用于请求配额为 而不是在请求之前。
// 如果请求得到满足，则函数返回配额的数量消耗，没有错误。如果因为没有当前可用的配额足够多，返回ErrNotEnoughQuota以导致
//
// 在有更多配额可用时再次调用的函数。不得不为0（即不允许请求为以后保存一些配额使用）。
// 如果返回任何其他错误，则Take必须为0。功能将不再调用，错误将从IntPool.AcquireFunc().
type IntRequestFunc func(ctx context.Context, p PoolInfo) (took uint64, err error)

// ErrNotEnoughQuota 当intrequestfunc希望在有新资源时再次调用它们时返回。
var ErrNotEnoughQuota = fmt.Errorf("not enough quota available")

// HasErrClosed 如果此错误是或包含ErrClosed错误，则返回true。
func HasErrClosed(err error) bool {
	_, hasErrClosed := errors.If(err, func(err error) (unwrapped interface{}, ok bool) {
		if _, hasErrClosed := err.(*ErrClosed); hasErrClosed {
			return err, hasErrClosed
		}
		return nil, false
	})
	return hasErrClosed
}

// PoolInfo 表示IntRequestFunc获取的有关当前配额池条件的信息。
type PoolInfo struct {
	// Available是可使用的配额量。这是IntRequestFunc的“taked”返回值可以设置的最大值。
	// 注意Available（）可以是0。当由于池的容量减少而调用IntRequestFunc（）时会发生这种情况。
	Available uint64

	// Capacity返回池中可用的最大容量。这会随着时间的推移而减少。它可用于确定请求所需的资源永远不可用。
	Capacity uint64
}

// AcquireFunc 获取由函数确定的配额数量，该函数与可用配额数量一起调用
func (p *IntPool) AcquireFunc(ctx context.Context, f IntRequestFunc) (*IntAlloc, error) {
	return p.acquireFuncMaybeWait(ctx, f, true /* wait */)
}

// TryAcquireFunc 类似于AcquireFunc，但如果存在不足的配额，则方法将返回ErrNotEnoughQuota，而不是等待配额变为可用。
func (p *IntPool) TryAcquireFunc(ctx context.Context, f IntRequestFunc) (*IntAlloc, error) {
	return p.acquireFuncMaybeWait(ctx, f, false /* wait */)
}

func (p *IntPool) acquireFuncMaybeWait(
	ctx context.Context, f IntRequestFunc, wait bool,
) (*IntAlloc, error) {
	r := p.newIntFuncRequest(f)
	defer p.putIntFuncRequest(r)
	var req Request
	if wait {
		req = r
	} else {
		req = (*intFuncRequestNoWait)(r)
	}
	err := p.qp.Acquire(ctx, req)
	if err != nil {
		return nil, err
	}
	if r.err != nil {
		if r.took != 0 {
			panic(fmt.Sprintf("both took set (%d) and err (%s)", r.took, r.err))
		}
		return nil, r.err
	}
	// NB: 我们知道r.take必须小于数学.MaxInt64因为容量不能超过那个值，而take不能超过容量。
	return p.newIntAlloc(int64(r.took)), nil
}

// Len 返回此IntPool的队列的当前长度
func (p *IntPool) Len() int {
	return p.qp.Len()
}

// ApproximateQuota 将报告池中可用配额的大致数量。
func (p *IntPool) ApproximateQuota() (q uint64) {
	p.qp.ApproximateQuota(func(r Resource) {
		if ia, ok := r.(*intAlloc); ok {
			q = uint64(max(0, ia.alloc))
		}
	})
	return q
}

// Close 向所有正在进行和随后进行的收购发出信号，表明池已关闭，应返回错误。
// 是线程安全的.
func (p *IntPool) Close(reason string) {
	p.qp.Close(reason)
}

// IntPoolCloser implements stop.Closer.
type IntPoolCloser struct {
	reason string
	p      *IntPool
}

// Close makes the IntPoolCloser a stop.Closer.
func (ipc IntPoolCloser) Close() {
	ipc.p.Close(ipc.reason)
}

// Closer returns a struct which implements stop.Closer.
func (p *IntPool) Closer(reason string) IntPoolCloser {
	return IntPoolCloser{p: p, reason: reason}
}

var intAllocSyncPool = sync.Pool{
	New: func() interface{} { return new(IntAlloc) },
}

func (p *IntPool) newIntAlloc(v int64) *IntAlloc {
	ia := intAllocSyncPool.Get().(*IntAlloc)
	*ia = IntAlloc{p: p, alloc: v}
	return ia
}

func (p *IntPool) putIntAlloc(ia *IntAlloc) {
	*ia = IntAlloc{}
	intAllocSyncPool.Put(ia)
}

var intRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(intRequest) },
}

// newIntRequest 从同步池获取. 它应该与intRequest一起返回。.
func (p *IntPool) newIntRequest(v uint64) *intRequest {
	r := intRequestSyncPool.Get().(*intRequest)
	*r = intRequest{want: v}
	return r
}

func (p *IntPool) putIntRequest(r *intRequest) {
	*r = intRequest{}
	intRequestSyncPool.Put(r)
}

var intFuncRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(intFuncRequest) },
}

// newIntRequest 从同步池获取. 它应该与intFuncRequest一起返回。
func (p *IntPool) newIntFuncRequest(f IntRequestFunc) *intFuncRequest {
	r := intFuncRequestSyncPool.Get().(*intFuncRequest)
	*r = intFuncRequest{f: f, p: p}
	return r
}

func (p *IntPool) putIntFuncRequest(r *intFuncRequest) {
	*r = intFuncRequest{}
	intFuncRequestSyncPool.Put(r)
}

// Capacity 返回此池管理的配额量
func (p *IntPool) Capacity() uint64 {
	return atomic.LoadUint64(&p.capacity)
}

// UpdateCapacity 将容量设置为newCapacity。如果当前容量高于新容量，则当前运行的请求将不受影响。
// 当容量增加时，将增加新的配额。未完成配额的总数量永远不会超过获得任何未完成配额时存在的容量的最大值。
func (p *IntPool) UpdateCapacity(newCapacity uint64) {
	assertCapacityIsValid(newCapacity)

	// 同步更新，这样我们就不会丢失任何配额。如果不同步，则快速连续的增加和减少可能会重新排序与当前配额相关的更新。
	//
	// 想象一下容量的变化:
	//   100, 1, 100, 1, 100
	// 它将导致以下的intAllocs被释放:
	//   -99, 99, -99, 99
	// 如果重新排序为:
	//   99, 99, -99, -99
	// 然后我们实际上忽略了一开始的加法，因为它们会把alloc推到容量之上，
	// 然后我们做相应的减法运算，最终得到-98的状态，即使我们希望它是1。
	p.updateCapacityMu.Lock()
	defer p.updateCapacityMu.Unlock()

	// 注意：如果要降低容量，我们需要在更新容量值之前从池中删除配额。
	// 想象一下这样一种情况：配额同时释放到池中。如果它看到的是较新的容量而不是旧的配额值，则会将该值推到新容量之上，
	// 从而被截断。这是个倒霉蛋。我们通过在改变产能之前从配额池中减去（可能会将其推入负区域）来防止这种情况。
	oldCapacity := atomic.LoadUint64(&p.capacity)
	delta := int64(newCapacity) - int64(oldCapacity)
	if delta < 0 {
		p.newIntAlloc(delta).Release()
		delta = 0 // 我们仍然想在更新后发布唤醒goroutines
	}
	atomic.SwapUint64(&p.capacity, newCapacity)
	p.newIntAlloc(delta).Release()
}

// decCapacity 使容量减少c.
func (p *IntPool) decCapacity(c uint64) {
	p.updateCapacityMu.Lock()
	defer p.updateCapacityMu.Unlock()

	oldCapacity := p.Capacity()
	if int64(oldCapacity)-int64(c) < 0 {
		panic("cannot freeze quota which is no longer part of the pool")
	}
	newCapacity := oldCapacity - c
	atomic.SwapUint64(&p.capacity, newCapacity)

	// 在队列前面唤醒请求。上面的减量可能会与正在进行的请求（这就是为什么它是一个原子访问）竞争，
	// 但无论如何，该请求都会被再次评估
	p.newIntAlloc(0).Release()
}

// intRequest 用于从预先知道的配额中获取数量.
type intRequest struct {
	want uint64
}

func (r *intRequest) Acquire(ctx context.Context, v Resource) (fulfilled bool, extra Resource) {
	ia := v.(*intAlloc)
	want := min(int64(r.want), int64(ia.p.Capacity()))
	if ia.alloc < want {
		return false, nil
	}
	r.want = uint64(want)
	ia.alloc -= want
	return true, ia
}

func (r *intRequest) ShouldWait() bool { return true }

type intRequestNoWait intRequest

func (r *intRequestNoWait) Acquire(
	ctx context.Context, v Resource,
) (fulfilled bool, extra Resource) {
	return (*intRequest)(r).Acquire(ctx, v)
}

func (r *intRequestNoWait) ShouldWait() bool { return false }

// intFuncRequest 用于从池中获取未知的数量。
type intFuncRequest struct {
	p    *IntPool
	f    IntRequestFunc
	took uint64
	// err保存r.f返回的错误（如果不是ErrNotEnoughQuota）。
	err error
}

func (r *intFuncRequest) Acquire(ctx context.Context, v Resource) (fulfilled bool, extra Resource) {
	ia := v.(*intAlloc)
	pi := PoolInfo{
		Available: uint64(max(0, ia.alloc)),
		Capacity:  ia.p.Capacity(),
	}
	took, err := r.f(ctx, pi)
	if err != nil {
		if took != 0 {
			panic(fmt.Sprintf("IntRequestFunc returned both took: %d and err: %s", took, err))
		}
		if err == ErrNotEnoughQuota {
			return false, nil
		}
		r.err = err
		// 将请求从队列中取出，并将所有配额放回。
		return true, ia
	}
	if took > math.MaxInt64 || int64(took) > ia.alloc {
		panic(errors.Errorf("took %d quota > %d allocated", took, ia.alloc))
	}
	r.took = took
	ia.alloc -= int64(took)
	return true, ia
}

func min(a, b int64) (v int64) {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) (v int64) {
	if a > b {
		return a
	}
	return b
}

func (r *intFuncRequest) ShouldWait() bool { return true }

type intFuncRequestNoWait intFuncRequest

func (r *intFuncRequestNoWait) Acquire(
	ctx context.Context, v Resource,
) (fulfilled bool, extra Resource) {
	return (*intFuncRequest)(r).Acquire(ctx, v)
}

func (r *intFuncRequestNoWait) ShouldWait() bool { return false }

// assertCapacityIsValid 如果容量超过math.MaxInt64则panic
func assertCapacityIsValid(capacity uint64) {
	if capacity > math.MaxInt64 {
		panic(errors.Errorf("capacity %d exceeds max capacity %d", capacity, math.MaxInt64))
	}
}
