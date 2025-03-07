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

package stop

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/util/caller"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/quotapool"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

const asyncTaskNamePrefix = "[async] "

// ErrThrottled is returned from RunLimitedAsyncTask in the event that there
// is no more capacity for async tasks, as limited by the semaphore.
var ErrThrottled = errors.New("throttled on async limiting semaphore")

// ErrUnavailable indicates that the server is quiescing and is unable to
// process new work.
var ErrUnavailable = &roachpb.NodeUnavailableError{}

func register(s *Stopper) {
	trackedStoppers.Lock()
	trackedStoppers.stoppers = append(trackedStoppers.stoppers, s)
	trackedStoppers.Unlock()
}

func unregister(s *Stopper) {
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	sl := trackedStoppers.stoppers
	for i, tracked := range sl {
		if tracked == s {
			trackedStoppers.stoppers = sl[:i+copy(sl[i:], sl[i+1:])]
			return
		}
	}
	panic("attempt to unregister untracked stopper")
}

var trackedStoppers struct {
	syncutil.Mutex
	stoppers []*Stopper
}

// HandleDebug responds with the list of stopper tasks actively running.
func HandleDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	for _, s := range trackedStoppers.stoppers {
		s.mu.Lock()
		fmt.Fprintf(w, "%p: %d tasks\n%s", s, s.mu.numTasks, s.runningTasksLocked())
		s.mu.Unlock()
	}
}

// Closer is an interface for objects to attach to the stopper to
// be closed once the stopper completes.
type Closer interface {
	Close()
}

// CloserFn is type that allows any function to be a Closer.
type CloserFn func()

// Close implements the Closer interface.
func (f CloserFn) Close() {
	f()
}

// A Stopper provides a channel-based mechanism to stop an arbitrary
// array of workers. Each worker is registered with the stopper via
// the RunWorker() method. The system further allows execution of functions
// through RunTask() and RunAsyncTask().
//
// Stopping occurs in two phases: the first is the request to stop, which moves
// the stopper into a quiescing phase. While quiescing, calls to RunTask() &
// RunAsyncTask() don't execute the function passed in and return ErrUnavailable.
// When all outstanding tasks have been completed, the stopper
// closes its stopper channel, which signals all live workers that it's safe to
// shut down. When all workers have shutdown, the stopper is complete.
//
// An arbitrary list of objects implementing the Closer interface may
// be added to the stopper via AddCloser(), to be closed after the
// stopper has stopped.
type Stopper struct {
	quiescer chan struct{}     // Closed when quiescing
	stopper  chan struct{}     // Closed when stopping
	stopped  chan struct{}     // Closed when stopped completely
	onPanic  func(interface{}) // called with recover() on panic on any goroutine
	stop     sync.WaitGroup    // Incremented for outstanding workers
	mu       struct {
		syncutil.Mutex
		quiesce   *sync.Cond // Conditional variable to wait for outstanding tasks
		quiescing bool       // true when Stop() has been called
		numTasks  int        // number of outstanding tasks
		tasks     TaskMap
		closers   []Closer
		idAlloc   int
		qCancels  map[int]func()
		sCancels  map[int]func()

		stopCalled bool // turns all but first call to Stop into noop
	}
}

// An Option can be passed to NewStopper.
type Option interface {
	apply(*Stopper)
}

type optionPanicHandler func(interface{})

func (oph optionPanicHandler) apply(stopper *Stopper) {
	stopper.onPanic = oph
}

// OnPanic is an option which lets the Stopper recover from all panics using
// the provided panic handler.
//
// When Stop() is invoked during stack unwinding, OnPanic is also invoked, but
// Stop() may not have carried out its duties.
func OnPanic(handler func(interface{})) Option {
	return optionPanicHandler(handler)
}

// NewStopper returns an instance of Stopper.
func NewStopper(options ...Option) *Stopper {
	s := &Stopper{
		quiescer: make(chan struct{}),
		stopper:  make(chan struct{}),
		stopped:  make(chan struct{}),
	}

	s.mu.tasks = TaskMap{}
	s.mu.qCancels = map[int]func(){}
	s.mu.sCancels = map[int]func(){}

	for _, opt := range options {
		opt.apply(s)
	}

	s.mu.quiesce = sync.NewCond(&s.mu)
	register(s)
	return s
}

// Recover is used internally by Stopper to provide a hook for recovery of
// panics on goroutines started by the Stopper. It can also be invoked
// explicitly (via "defer s.Recover()") on goroutines that are created outside
// of Stopper.
func (s *Stopper) Recover(ctx context.Context) {
	if r := recover(); r != nil {
		if s.onPanic != nil {
			s.onPanic(r)
			return
		}
		if sv := settings.TODO(); sv != nil {
			log.ReportPanic(ctx, sv, r, 1)
		}
		panic(r)
	}
}

// RunWorker runs the supplied function as a "worker" to be stopped
// by the stopper. The function <f> is run in a goroutine.
func (s *Stopper) RunWorker(ctx context.Context, f func(context.Context)) {
	s.stop.Add(1)
	go func() {
		// Remove any associated span; we need to ensure this because the
		// worker may run longer than the caller which presumably closes
		// any spans it has created.
		ctx = opentracing.ContextWithSpan(ctx, nil)
		defer s.Recover(ctx)
		defer s.stop.Done()
		f(ctx)
	}()
}

// AddCloser adds an object to close after the stopper has been stopped.
//
// WARNING: memory resources acquired by this method will stay around for
// the lifetime of the Stopper. Use with care to avoid leaking memory.
func (s *Stopper) AddCloser(c Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.stopper:
		// Close immediately.
		c.Close()
	default:
		s.mu.closers = append(s.mu.closers, c)
	}
}

// WithCancelOnQuiesce returns a child context which is canceled when the
// returned cancel function is called or when the Stopper begins to quiesce,
// whichever happens first.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func (s *Stopper) WithCancelOnQuiesce(ctx context.Context) (context.Context, func()) {
	return s.withCancel(ctx, s.mu.qCancels, s.quiescer)
}

// WithCancelOnStop returns a child context which is canceled when the
// returned cancel function is called or when the Stopper begins to stop,
// whichever happens first.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func (s *Stopper) WithCancelOnStop(ctx context.Context) (context.Context, func()) {
	return s.withCancel(ctx, s.mu.sCancels, s.stopper)
}

func (s *Stopper) withCancel(
	ctx context.Context, cancels map[int]func(), cancelCh chan struct{},
) (context.Context, func()) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-cancelCh:
		// Cancel immediately.
		cancel()
		return ctx, func() {}
	default:
		id := s.mu.idAlloc
		s.mu.idAlloc++
		cancels[id] = cancel
		return ctx, func() {
			cancel()
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(cancels, id)
		}
	}
}

// RunTask adds one to the count of tasks left to quiesce in the system.
// Any worker which is a "first mover" when starting tasks must call this method
// before starting work on a new task. First movers include goroutines launched
// to do periodic work and the kv/db.go gateway which accepts external client
// requests.
//
// taskName is used as the "operation" field of the span opened for this task
// and is visible in traces. It's also part of reports printed by stoppers
// waiting to stop. The convention is
// <package name>.<struct name>: <succinct description of the task's action>
//
// Returns an error to indicate that the system is currently quiescing and
// function f was not called.
func (s *Stopper) RunTask(ctx context.Context, taskName string, f func(context.Context)) error {
	if !s.runPrelude(taskName) {
		return ErrUnavailable
	}

	// Call f.
	defer s.Recover(ctx)
	defer s.runPostlude(taskName)

	f(ctx)
	return nil
}

// RunTaskWithErr is like RunTask(), but takes in a callback that can return an
// error. The error is returned to the caller.
func (s *Stopper) RunTaskWithErr(
	ctx context.Context, taskName string, f func(context.Context) error,
) error {
	if !s.runPrelude(taskName) {
		return ErrUnavailable
	}

	// Call f.
	defer s.Recover(ctx)
	defer s.runPostlude(taskName)

	return f(ctx)
}

// RunAsyncTask is like RunTask, except the callback is run in a goroutine. The
// method doesn't block for the callback to finish execution.
func (s *Stopper) RunAsyncTask(
	ctx context.Context, taskName string, f func(context.Context),
) error {
	taskName = asyncTaskNamePrefix + taskName
	if !s.runPrelude(taskName) {
		return ErrUnavailable
	}

	ctx, span := tracing.ForkCtxSpan(ctx, taskName)

	// Call f.
	go func() {
		defer s.Recover(ctx)
		defer s.runPostlude(taskName)
		defer tracing.FinishSpan(span)

		f(ctx)
	}()
	return nil
}

// RunLimitedAsyncTask 使用给定的通道作为信号量来限制正在运行的任务数同时达到信道容量。
// 如果等待是真的直到信号量可用以推后呼叫者这可能会试图创建许多任务。
// 如果wait为false，则返回如果信号量不是可用。呼叫方的责任是确保sem停止时关闭。
// 对于那些为塞子的使用寿命，通常最好将扫描电镜与使用AddCloser阻止。
func (s *Stopper) RunLimitedAsyncTask(
	ctx context.Context, taskName string, sem *quotapool.IntPool, wait bool, f func(context.Context),
) (err error) {
	// Wait for permission to run from the semaphore.
	var alloc *quotapool.IntAlloc
	if wait {
		alloc, err = sem.Acquire(ctx, 1)
	} else {
		alloc, err = sem.TryAcquire(ctx, 1)
	}
	if err == quotapool.ErrNotEnoughQuota {
		err = ErrThrottled
	} else if quotapool.HasErrClosed(err) {
		err = ErrUnavailable
	}
	if err != nil {
		return err
	}
	defer func() {
		// If the err is non-nil then we know that we did not start the async task
		// and thus we need to release the acquired quota. If it is nil then we
		// did start the task and it will release the quota.
		if err != nil {
			alloc.Release()
		}
	}()

	// Check for canceled context: it's possible to get the semaphore even
	// if the context is canceled.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !s.runPrelude(taskName) {
		return ErrUnavailable
	}

	ctx, span := tracing.ForkCtxSpan(ctx, taskName)

	go func() {
		defer s.Recover(ctx)
		defer s.runPostlude(taskName)
		defer alloc.Release()
		defer tracing.FinishSpan(span)

		f(ctx)
	}()
	return nil
}

func (s *Stopper) runPrelude(taskName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.quiescing {
		return false
	}
	s.mu.numTasks++
	s.mu.tasks[taskName]++
	return true
}

func (s *Stopper) runPostlude(taskName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.numTasks--
	s.mu.tasks[taskName]--
	s.mu.quiesce.Broadcast()
}

// NumTasks returns the number of active tasks.
func (s *Stopper) NumTasks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.numTasks
}

// A TaskMap is returned by RunningTasks().
type TaskMap map[string]int

// String implements fmt.Stringer and returns a sorted multi-line listing of
// the TaskMap.
func (tm TaskMap) String() string {
	var lines []string
	for location, num := range tm {
		lines = append(lines, fmt.Sprintf("%-6d %s", num, location))
	}
	sort.Sort(sort.Reverse(sort.StringSlice(lines)))
	return strings.Join(lines, "\n")
}

// RunningTasks returns a map containing the count of running tasks keyed by
// call site.
func (s *Stopper) RunningTasks() TaskMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runningTasksLocked()
}

func (s *Stopper) runningTasksLocked() TaskMap {
	m := TaskMap{}
	for k := range s.mu.tasks {
		if s.mu.tasks[k] == 0 {
			continue
		}
		m[k] = s.mu.tasks[k]
	}
	return m
}

// Stop signals all live workers to stop and then waits for each to
// confirm it has stopped.
func (s *Stopper) Stop(ctx context.Context) {
	s.mu.Lock()
	stopCalled := s.mu.stopCalled
	s.mu.stopCalled = true
	s.mu.Unlock()

	if stopCalled {
		return
	}

	defer s.Recover(ctx)
	defer unregister(s)

	if log.V(1) {
		file, line, _ := caller.Lookup(1)
		log.Infof(ctx,
			"stop has been called from %s:%d, stopping or quiescing all running tasks", file, line)
	}
	// Don't bother doing stuff cleanly if we're panicking, that would likely
	// block. Instead, best effort only. This cleans up the stack traces,
	// avoids stalls and helps some tests in `./cli` finish cleanly (where
	// panics happen on purpose).
	if r := recover(); r != nil {
		go s.Quiesce(ctx)
		close(s.stopper)
		close(s.stopped)
		s.mu.Lock()
		for _, c := range s.mu.closers {
			go c.Close()
		}
		s.mu.Unlock()
		panic(r)
	}

	s.Quiesce(ctx)
	s.mu.Lock()
	for _, cancel := range s.mu.sCancels {
		cancel()
	}
	close(s.stopper)
	s.mu.Unlock()

	s.stop.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.mu.closers {
		c.Close()
	}
	close(s.stopped)
}

// ShouldQuiesce returns a channel which will be closed when Stop() has been
// invoked and outstanding tasks should begin to quiesce.
func (s *Stopper) ShouldQuiesce() <-chan struct{} {
	if s == nil {
		// A nil stopper will never signal ShouldQuiesce, but will also never panic.
		return nil
	}
	return s.quiescer
}

// ShouldStop returns a channel which will be closed when Stop() has been
// invoked and outstanding tasks have quiesced.
func (s *Stopper) ShouldStop() <-chan struct{} {
	if s == nil {
		// A nil stopper will never signal ShouldStop, but will also never panic.
		return nil
	}
	return s.stopper
}

// IsStopped returns a channel which will be closed after Stop() has
// been invoked to full completion, meaning all workers have completed
// and all closers have been closed.
func (s *Stopper) IsStopped() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.stopped
}

// Quiesce moves the stopper to state quiescing and waits until all
// tasks complete. This is used from Stop() and unittests.
func (s *Stopper) Quiesce(ctx context.Context) {
	defer s.Recover(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cancel := range s.mu.qCancels {
		cancel()
	}
	if !s.mu.quiescing {
		s.mu.quiescing = true
		close(s.quiescer)
	}
	for s.mu.numTasks > 0 {
		log.Infof(ctx, "quiescing; tasks left:\n%s", s.runningTasksLocked())
		// Unlock s.mu, wait for the signal, and lock s.mu.
		s.mu.quiesce.Wait()
	}
}
