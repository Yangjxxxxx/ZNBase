package quotapool

import "sync"

// bufferSize是从notifyQueueNodePool提供的ringBuf buf的大小。
// 每个节点都是32+8*bufferSize字节，所以在28时，一个节点是256字节，这是一个不错的数字。
const bufferSize = 28

// notifyQueue为chan struct{}提供一个高效的FIFO队列。
//
// notifyQueue 不是线程安全的.
type notifyQueue struct {
	len  int64
	head *node

	pool *notifyQueueNodePool
}

// initializeNotifyQueue 初始化notifyQueue.notifyQueue值在初始化。
// 它多次初始化notifyQueue是非法的，如果使用已初始化的notifyQueue调用此函数，则会死机。
func initializeNotifyQueue(q *notifyQueue) {
	if q.pool != nil {
		panic("cannot re-initialize a notifyQueue")
	}
	defaultNotifyQueueNodePool.initialize(q)
}

var defaultNotifyQueueNodePool = newNotifyQueueNodePool()

// enqueue 将c添加到队列的末尾并返回添加的notifyee的地址
func (q *notifyQueue) enqueue(c chan struct{}) (n *notifyee) {
	if q.head == nil {
		q.head = q.pool.pool.Get().(*node)
		q.head.prev = q.head
		q.head.next = q.head
	}
	tail := q.head.prev
	if n = tail.enqueue(c); n == nil {
		newTail := q.pool.pool.Get().(*node)
		tail.next = newTail
		q.head.prev = newTail
		newTail.prev = tail
		newTail.next = q.head
		if n = newTail.enqueue(c); n == nil {
			panic("failed to enqueue into a fresh buffer")
		}
	}
	q.len++
	return n
}

// dequeue 删除可使用peek（）访问的队列的当前头
func (q *notifyQueue) dequeue() {
	if q.head == nil {
		return
	}
	q.head.dequeue()
	if q.head.len == 0 {
		oldHead := q.head
		if oldHead.next == oldHead {
			q.head = nil
		} else {
			q.head = oldHead.next
			q.head.prev = oldHead.prev
			q.head.prev.next = q.head
		}
		*oldHead = node{}
		q.pool.pool.Put(oldHead)
	}
	q.len--
}

// peek 返回队列的当前头，如果队列为空，则返回nil。它不会修改队列。在下一次调用出列之后使用返回的指针是非法的
func (q *notifyQueue) peek() *notifyee {
	if q.head == nil {
		return nil
	}
	return &q.head.buf[q.head.head]
}

// notifyQueueNodePool 构造notifyQueue对象，这些对象在内部共享其缓冲区。
type notifyQueueNodePool struct {
	pool sync.Pool
}

// newNotifyQueueNodePool 返回一个新的notifyQueueNodePool，
// 该nodepool可用于构造notifyQueues，该notifyQueues在内部池化其缓冲区。
func newNotifyQueueNodePool() *notifyQueueNodePool {
	return &notifyQueueNodePool{
		pool: sync.Pool{
			New: func() interface{} { return &node{} },
		},
	}
}

// initialize 初始化将共享同步池具有使用此池初始化的其他notifyQueue实例的节点
func (p *notifyQueueNodePool) initialize(q *notifyQueue) {
	*q = notifyQueue{pool: p}
}

type node struct {
	ringBuf
	prev, next *node
}

type notifyee struct {
	c chan struct{}
}

type ringBuf struct {
	buf  [bufferSize]notifyee
	head int64
	len  int64
}

func (rb *ringBuf) enqueue(c chan struct{}) *notifyee {
	if rb.len == bufferSize {
		return nil
	}
	i := (rb.head + rb.len) % bufferSize
	rb.buf[i] = notifyee{c: c}
	rb.len++
	return &rb.buf[i]
}

func (rb *ringBuf) dequeue() {
	// 注意：notifyQueue从不包含空的ringBuf。
	if rb.len == 0 {
		panic("cannot dequeue from an empty buffer")
	}
	rb.buf[rb.head] = notifyee{}
	rb.head++
	rb.head %= bufferSize
	rb.len--
}
