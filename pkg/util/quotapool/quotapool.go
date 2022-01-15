package quotapool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// Resource 是一个接口，它表示正在进行池和分配的数量。它是可以细分和组合的任何数量。
//
// 此库不提供任何具体的资源实现，但内部使用*IntAlloc作为资源。
type Resource interface {

	// 合并将其他资源合并到当前资源中。
	//
	// 将资源（其他）传递到合并后，QuotaPool将不再使用该资源。
	// 此行为允许客户端通过在获取过程中创建资源并在合并中销毁资源来共享资源实例。
	Merge(other Resource)
}

// Request 是用于从池中获取配额的接口。
// 请求负责将资源细分为在请求完成时保留的部分和其余部分。
type Request interface {

	// Acquire决定一个请求是否可以通过给定数量的资源。
	// 如果未实现，则不得修改或保留传递的alloc。
	// 如果它被实现了，它应该返回它所做的Alloc的任何部分不打算使用。
	// 由实现者决定返回获取所有传递的资源。如果返回nil并且有获得池中的零值资源单元，
	// 则这些收购可能需要请等到池非空后再继续。那些零价值的收购仍需等待，才能排在最前面。
	// 它可能对特殊情况下的零值收购的实施者有意义完全像IntPool一样。
	Acquire(context.Context, Resource) (fulfilled bool, unused Resource)

	// ShouldWait指示此请求是否应排队。如果这个方法返回false，
	// 并且当请求已排队，则将从调用返回ErrNotEnoughQuota获得。
	ShouldWait() bool
}

// ErrClosed 在调用Close后从Acquire返回
type ErrClosed struct {
	poolName string
	reason   string
}

// Error implements error.
func (ec *ErrClosed) Error() string {
	return fmt.Sprintf("%s pool closed: %s", ec.poolName, ec.reason)
}

// QuotaPool 是存储某些资源单元的池的抽象实现。其基本思想是，
// 它允许请求以FIFO顺序从池中获取大量资源，这种方式与上下文取消交互良好
type QuotaPool struct {
	config

	// name 用于日志记录，并传递给用于报告采集或慢速采集的函数
	name string

	// 当配额池关闭时，正在进行的收购将侦听已完成的交易（请参阅QuotaPool.Close).
	done chan struct{}

	// closeErr 调用Close时填充非nil错误。
	closeErr *ErrClosed

	mu struct {
		syncutil.Mutex

		// quota 存储池中可用配额的当前数量。
		quota Resource

		// 我们以先到先得的方式获取服务配额。这个这样做是为了防止连续不断的小的。
		// 收购本身“注册”通知他们现在排在第一位。这是通过在队列中附加他们将等待的通道来完成打开。
		// 如果不再需要通知goroutine，即获取上下文已被取消，goroutine负责通过填充通道缓冲器。
		q notifyQueue

		// numCanceled 是已取消的q的成员数。
		// 它用于确定队列中当前活动等待者的数量，即q.len（）减去此值。
		numCanceled int

		// closed 当配额池关闭时设置为true（请参阅QuotaPool.Close).
		closed bool
	}
}

// New 返回用给定配额初始化的新配额池。配额的上限是这个数量，
// 这意味着调用方返回的配额可能比获得的配额多，而不会提供超过配额容量的可用容量。
func New(name string, initialResource Resource, options ...Option) *QuotaPool {
	qp := &QuotaPool{
		name: name,
		done: make(chan struct{}),
	}
	for _, o := range options {
		o.apply(&qp.config)
	}
	qp.mu.quota = initialResource
	initializeNotifyQueue(&qp.mu.q)
	return qp
}

// ApproximateQuota 将向f报告池中可用配额的大致数量。所提供的资源不得更改。
func (qp *QuotaPool) ApproximateQuota(f func(Resource)) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	f(qp.mu.quota)
}

// Len 返回此QuotaPool的队列的当前长度。
func (qp *QuotaPool) Len() int {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	return int(qp.mu.q.len) - qp.mu.numCanceled
}

// Close 向所有正在进行和随后进行的收购发出信号，表明它们可以自由地返回给呼叫者。他们将收到包含此原因的*ErrClosed。
// 是线程安全的
func (qp *QuotaPool) Close(reason string) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	if qp.mu.closed {
		return
	}
	qp.mu.closed = true
	qp.closeErr = &ErrClosed{
		poolName: qp.name,
		reason:   reason,
	}
	close(qp.done)
}

// Add 将提供的Alloc添加回池中。如果存在，该值将与QuotaPool中的现有资源合并。
// 是线程安全的
func (qp *QuotaPool) Add(v Resource) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	qp.addLocked(v)
}

func (qp *QuotaPool) addLocked(r Resource) {
	if qp.mu.quota != nil {
		r.Merge(qp.mu.quota)
	}
	qp.mu.quota = r
	// 如果有人在等待，通知队列的头。
	if n := qp.mu.q.peek(); n != nil && n.c != nil {
		select {
		case n.c <- struct{}{}:
		default:
		}
	}
}

// chanSyncPool 用于将用于通知在获取中等待的goroutine的通道分配到池中。
var chanSyncPool = sync.Pool{
	New: func() interface{} { return make(chan struct{}, 1) },
}

// Acquire 尝试使用来自qp的资源来完成请求。请求按FIFO顺序提供服务；
// 一次只提供一个请求的资源。请求将被提供给池的当前资源量，直到它被满足或其上下文被取消。
// 是线程安全的
func (qp *QuotaPool) Acquire(ctx context.Context, r Request) (err error) {
	// Set up onAcquisition if we have one.
	start := timeutil.Now()
	if qp.config.onAcquisition != nil {
		defer func() {
			if err == nil {
				qp.config.onAcquisition(ctx, qp.name, r, start)
			}
		}()
	}
	// 尝试在快速路径上获取配额。
	fulfilled, n, err := qp.acquireFastPath(ctx, r)
	if fulfilled || err != nil {
		return err
	}
	// 设置基础结构以报告慢请求。
	var slowTimer *timeutil.Timer
	var slowTimerC <-chan time.Time
	if qp.onSlowAcquisition != nil {
		slowTimer = timeutil.NewTimer()
		defer slowTimer.Stop()
		// 故意只重置一次，因为我们更关心goroutine配置文件中的select duration，而不是定期日志记录。
		slowTimer.Reset(qp.slowAcquisitionThreshold)
		slowTimerC = slowTimer.C
	}
	for {
		select {
		case <-slowTimerC:
			slowTimer.Read = true
			slowTimerC = nil
			defer qp.onSlowAcquisition(ctx, qp.name, r, start)()
			continue
		case <-n.c:
			if fulfilled := qp.tryAcquireOnNotify(ctx, r, n); fulfilled {
				return nil
			}
		case <-ctx.Done():
			qp.cleanupOnCancel(n)
			return ctx.Err()
		case <-qp.done:
			// 我们不需要像取消上下文时那样“注销”自己。事实上，
			// 我们希望其他服务员只接受qp.完成向他们发出信号会起到反作用。
			return qp.closeErr // 当qp.done已关闭
		}
	}
}

// acquireFastPath 如果无人等待，则尝试获取配额；如果请求未立即完成，则返回notifyee。
func (qp *QuotaPool) acquireFastPath(
	ctx context.Context, r Request,
) (fulfilled bool, _ *notifyee, _ error) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	if qp.mu.closed {
		return false, nil, qp.closeErr
	}
	if qp.mu.q.len == 0 {
		if fulfilled, unused := r.Acquire(ctx, qp.mu.quota); fulfilled {
			qp.mu.quota = unused
			return true, nil, nil
		}
	}
	if !r.ShouldWait() {
		return false, nil, ErrNotEnoughQuota
	}
	c := chanSyncPool.Get().(chan struct{})
	return false, qp.mu.q.enqueue(c), nil
}

func (qp *QuotaPool) tryAcquireOnNotify(
	ctx context.Context, r Request, n *notifyee,
) (fulfilled bool) {
	// 如果完成了，请将通知通道释放回同步池。捕获nc的值，因为在它被释放回notifyQueue之后，避免race访问n是不安全的
	defer func(nc chan struct{}) {
		if fulfilled {
			chanSyncPool.Put(nc)
		}
	}(n.c)

	qp.mu.Lock()
	defer qp.mu.Unlock()
	// 确保在最后一次接收和获取互斥锁之间没有人再次通知我们。
	if len(n.c) > 0 {
		<-n.c
	}
	var unused Resource
	if fulfilled, unused = r.Acquire(ctx, qp.mu.quota); fulfilled {
		n.c = nil
		qp.mu.quota = unused
		qp.notifyNextLocked()
	}
	return fulfilled
}

func (qp *QuotaPool) cleanupOnCancel(n *notifyee) {
	// 不管怎样，我们都要把notify频道放回同步池。注意，这个延迟调用在这里计算n.c，不受后面将n.c设置为nil的代码的影响
	defer chanSyncPool.Put(n.c)

	qp.mu.Lock()
	defer qp.mu.Unlock()

	// 如果我们不是HEAD，阻止自己被通知，继续前进
	if n != qp.mu.q.peek() {
		n.c = nil
		qp.mu.numCanceled++
		return
	}

	// 如果我们是负责人，确保在我们通知下一个等待通知的人之前没有人通知我们。
	if len(n.c) > 0 {
		<-n.c
	}
	qp.notifyNextLocked()
}

// notifyNextLocked 通知下一行中等待的获取goroutine（如果有）。它需要qp.mu.互斥被扣留。
func (qp *QuotaPool) notifyNextLocked() {
	// 把我们自己从队伍的前面pop下来。
	qp.mu.q.dequeue()
	// 我们遍历直到找到等待通知的goroutine，通知goroutine并截断队列，以确保所述goroutine位于队列的头部。
	// 通常下一个排队的侍者是等待的那个通知，但是如果我们后面的其他人也取消了他们的上下文，
	// 他们将留下通知者，没有我们下面跳过的频道。
	//
	// 如果确定没有goroutines在等待，我们只需截断队列来反映这一点。
	for n := qp.mu.q.peek(); n != nil; n = qp.mu.q.peek() {
		if n.c == nil {
			qp.mu.numCanceled--
			qp.mu.q.dequeue()
			continue
		}
		n.c <- struct{}{}
		break
	}
}
