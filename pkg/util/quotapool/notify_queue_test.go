package quotapool

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkNotifyQueue(b *testing.B) {
	testNotifyQueue(b, b.N)
}

func TestNotifyQueue(t *testing.T) {
	testNotifyQueue(t, 10000)
}

type op bool

const (
	enqueue op = true
	dequeue op = false
)

func testNotifyQueue(t testing.TB, N int) {
	b, _ := t.(*testing.B)
	var q notifyQueue
	initializeNotifyQueue(&q)
	n := q.peek()
	assert.Nil(t, n)
	q.dequeue()
	assert.Equal(t, 0, int(q.len))
	chans := make([]chan struct{}, N)
	for i := 0; i < N; i++ {
		chans[i] = make(chan struct{})
	}
	in := chans
	out := make([]chan struct{}, 0, N)
	ops := make([]op, (4*N)/3)
	for i := 0; i < N; i++ {
		ops[i] = enqueue
	}
	rand.Shuffle(len(ops), func(i, j int) {
		ops[i], ops[j] = ops[j], ops[i]
	})
	if b != nil {
		b.ResetTimer()
	}
	l := 0 // 仅在b==nil时使用
	for _, op := range ops {
		switch op {
		case enqueue:
			q.enqueue(in[0])
			in = in[1:]
			if b == nil {
				l++
			}
		case dequeue:
			// 只有在我们不进行基准测试的情况下才能进行测试。
			if b == nil {
				if n := q.peek(); n != nil {
					out = append(out, n.c)
					q.dequeue()
					l--
					assert.Equal(t, l, int(q.len))
				}
			} else {
				if n := q.peek(); n != nil {
					out = append(out, n.c)
					q.dequeue()
				}
			}
		}
	}
	for n := q.peek(); n != nil; n = q.peek() {
		out = append(out, n.c)
		q.dequeue()
	}
	if b != nil {
		b.StopTimer()
	}
	assert.EqualValues(t, chans, out)
}
