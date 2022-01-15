package task

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/znbasedb/znbase/pkg/security/audit/util"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/retry"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// State used for runner fsm
type State int

const (
	stateNew State = 1 << iota
	stateInit
	stateRun
	stateRebalance
	stateStop
)

// Runner maintain workers, can auto scale worker according to load of data channel
type Runner struct {
	ctx     context.Context // runner context
	id      string          // runner identifier
	name    string          // runner name
	state   State           // runner current state, no need to lock because all state update within single fsm goroutine
	channel struct {
		data      chan interface{} // channel where data in and out
		fsm       chan State       // used to update runner state
		rebalance chan int         // used to rebalance worker
	}
	workerMu struct { // all running workers, <worker name, worker>
		syncutil.Mutex
		workers map[string]*Worker
	}
	workerCfg struct { // config for worker
		wid        int                     // max id of last worker, increment by 1 for new worker
		wgid       int                     // max group id of last workers, increment by 1 for new group
		minW       int                     // min workers running at the same time
		maxW       int                     // max workers running at the same time
		multiplier float64                 // used to auto scale workers
		f          func(interface{}) error // function used by workers
	}
	retryCfg struct { // retry config
		writeRetry int // retry for put data to data channel, default 1
	}
	timeoutCfg struct { // timeout config
		stopRunner int // timeout for stop runner, default 500ms
		execFunc   int // timeout for work exec f, default 500ms
		writeData  int // timeout for put data to data channel, default 10ms
	}
	stopMu struct { // used to stop worker when runner is stop
		sync.Once
		syncutil.RWMutex //
		sync.WaitGroup   // used to stop worker
		c                chan struct{}
	}
	workerGauge   *RunnerMetrics //Worker Gauge metrics
	solvedCounter *WorkerMetrics //Worker solved audit data counter
}

// NewRunner create new runner with inited state
func NewRunner(
	ctx context.Context,
	name string,
	capacity int,
	parallel int,
	stopTimeout int,
	workTimeout int,
	f func(interface{}) error,
	mr *metric.Registry,
) *Runner {
	// get runner identifier
	rid := getID()
	//create runner worker counter metrics
	metricswc := &RunnerMetrics{
		WorkerGauge: metric.NewGauge(

			metric.Metadata{
				Name:        "audit.runner.worker.count",
				Help:        "runner worker number count",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//create worker solved audit data counter metrics
	metricssd := &WorkerMetrics{
		SolvedCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.runner.worker.solved.count",
				Help:        "count the audit data solved by workers",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// new runner
	r := &Runner{
		ctx:           ctx,
		id:            rid,
		name:          name + "_" + rid,
		state:         stateNew,
		workerGauge:   metricswc,
		solvedCounter: metricssd,
	}

	// init channel for data/fsm/rebalance
	r.channel = struct {
		data      chan interface{}
		fsm       chan State
		rebalance chan int
	}{
		data:      make(chan interface{}, capacity),
		fsm:       make(chan State, DefaultFSMChannelSize),
		rebalance: make(chan int, DefaultRebChannelSize),
	}

	// init worker config
	r.workerCfg = struct {
		wid        int
		wgid       int
		minW       int
		maxW       int
		multiplier float64
		f          func(interface{}) error
	}{
		wid:        DefaultInitID,
		wgid:       DefaultInitGroupID,
		minW:       parallel,
		maxW:       DefaultMax,
		multiplier: DefaultMultiplier,
		f:          f,
	}

	// init retry and timeout
	r.retryCfg.writeRetry = DefaultRetry
	r.timeoutCfg = struct {
		stopRunner int
		execFunc   int
		writeData  int
	}{
		stopRunner: stopTimeout,
		execFunc:   workTimeout,
		writeData:  DefaultWriteTimeout,
	}

	//registry metrics
	if mr != nil {
		mr.AddMetricStruct(metricswc)
		mr.AddMetricStruct(metricssd)
	}
	// init channel for stop
	r.stopMu.c = make(chan struct{})

	// update runner state
	r.fsm(stateInit)
	return r
}

// Start runner
// 1. process runner fsm
// 2. monitor worker, output worker metric
func (r *Runner) Start() {
	// fsm goroutine
	r.stopMu.Add(1)
	go func() {
		defer r.stopMu.Done()
		for {
			select {
			case s := <-r.channel.fsm:
				switch s {
				case stateInit:
					r.state = stateInit
					r.init()
				case stateRebalance:
					r.state = stateRebalance
					// merge rebalance delta
					delta := 0
					for done := false; !done; {
						select {
						case d := <-r.channel.rebalance:
							delta += d
						default:
							done = true
						}
					}
					r.rebalance(delta)
				case stateRun:
					r.state = stateRun
				case stateStop:
					r.state = stateStop
					return
				}
			case <-r.stopMu.c:
				log.Infof(r.ctx, "%s stop fsm, because runner is going to stop", r.name)
				return
			}
		}
	}()

	// monitor goroutine used to output workers metric and rebalance workers
	r.stopMu.Add(1)
	go func() {
		defer r.stopMu.Done()
		log.Infof(r.ctx, "%s monitor workers every %d seconds", r.name, DefaultWorkerMonitorInterval)
		refresh := time.NewTicker(DefaultWorkerMonitorInterval * time.Second)
		defer refresh.Stop()
		capacity := cap(r.channel.data) / 2
		for {
			select {
			case <-refresh.C:
				// output workers metric
				//r.workerMu.Lock()
				//for k, v := range r.workerMu.workers {
				//	log.Infof(r.ctx, "%s worker:%s counter:%d, last update:%s", r.name, k, v.counter, v.last)
				//}
				//r.workerMu.Unlock()
				// release workers when high load has bee gone.
				if len(r.channel.data) < capacity && len(r.workerMu.workers) > r.workerCfg.minW {
					r.channel.rebalance <- r.workerCfg.maxW * -1
					r.fsm(stateRebalance)
				}
			case <-r.stopMu.c:
				log.Infof(r.ctx, "%s stop monitor, because runner is going to stop", r.name)
				return
			}
		}
	}()
}

// PutData put data to data channel
// if write times out, will create more workers using multiplier and rewrite data again
// return error when runner state is stop
// TODO lock timeout
func (r *Runner) PutData(data interface{}) error {
	r.stopMu.RLock()
	defer r.stopMu.RUnlock()
	if r.state&stateStop == 0 {
		scaleOut := true
		return retry.WithMaxAttempts(context.TODO(), retry.Options{}, r.retryCfg.writeRetry, func() error {
			select {
			case r.channel.data <- data:
				return nil
			case <-time.After(time.Duration(r.timeoutCfg.writeData) * time.Microsecond):
				log.Warningf(r.ctx, "%s channel maybe full", r.name)
				// how many workers need to be created this time
				// if workers is already up to max, just log
				if scaleOut && (len(r.workerMu.workers) < r.workerCfg.maxW) {
					math.Float64bits(float64(len(r.workerMu.workers)) * r.workerCfg.multiplier)
					//log.Infof(r.ctx, "%s more worker need to create later", r.name)
					r.channel.rebalance <- util.Max(int(float64(len(r.workerMu.workers))*r.workerCfg.multiplier), DefaultDelta)
					r.fsm(stateRebalance)
					scaleOut = false
				} else {
					log.Warningf(r.ctx, "%s workers is up to max, will retry again", r.name)
				}
				return pgerror.NewErrorf(pgcode.RunnerWriteTimeout, "%s take too much time to write data, maybe lost", r.name)
			}
		})
	}
	return pgerror.NewErrorf(pgcode.RunnerWriteAfterStop, "%s write is not allowed when runner state is %d", r.name, stateStop)
}

// UpdateRetry update write retry
func (r *Runner) UpdateRetry(retry int) {
	if retry > 0 {
		r.retryCfg.writeRetry = retry
	}
}

// init workers and create minw workers
func (r *Runner) init() {
	log.Infof(r.ctx, "%s init, min:%d, max:%d", r.name, r.workerCfg.minW, r.workerCfg.maxW)
	r.workerMu.Lock()
	r.workerMu.workers = make(map[string]*Worker)
	r.workerMu.Unlock()
	r.channel.rebalance <- r.workerCfg.minW
	r.fsm(stateRebalance)
}

// rebalance workers. based on total count of workers running now, create more if too less or destroy some if too many
// delta > 0, create more workers
// delta < 0, release some workers
func (r *Runner) rebalance(d int) {
	//log.Infof(r.ctx, "%s start to rebalance workers", r.name)
	runW := len(r.workerMu.workers)
	delta := 0
	// make sure delta always between r.parallel and r.max
	if runW+d >= r.workerCfg.maxW {
		delta = r.workerCfg.maxW - runW
	} else if runW+d <= r.workerCfg.minW {
		delta = r.workerCfg.minW - runW
	} else {
		delta = d
	}
	if delta != 0 {
		if delta > 0 {
			//log.Infof(r.ctx, "%s create workers, delta:%d", r.name, delta)
			for i := 0; i < delta; i++ {
				r.runWorker()
				r.workerGauge.WorkerGauge.Inc(1)
			}
			r.workerCfg.wgid++
		} else {
			//log.Infof(r.ctx, "%s release workers, delta:%d", r.name, delta)
			r.workerMu.Lock()
			defer r.workerMu.Unlock()
			for _, w := range r.workerMu.workers {
				if delta < 0 {
					w.close()
					r.workerGauge.WorkerGauge.Dec(1)
					delta++
				} else {
					break
				}
			}
		}
	}
	//else {
	//	log.Infof(r.ctx, "%s no need to rebalance, worker:%d, max:%d", r.name, len(r.workerMu.workers), r.workerCfg.maxW)
	//}
	// update fsm state
	r.fsm(stateRun)
}

// Stop will destroy workers and stop runner
// step 1. update runner state
// step 2. worker who get this data will process all data left in channel
// step 3. close stop channel to stop workers
// step 4. wait until all workers done
func (r *Runner) Stop() {
	r.fsm(stateStop)
	//log.Infof(r.ctx, "%s make sure all data left in channel is handled", r.name)
	r.stopMu.Add(1)
	go func() {
		defer r.stopMu.Done()
		defer r.close()
		refresh := time.NewTicker(DefaultTickerTime * time.Millisecond)
		deadline := timeutil.Now().Add(time.Duration(r.timeoutCfg.stopRunner) * time.Millisecond)
		defer refresh.Stop()
		for {
			select {
			case <-refresh.C:
				if timeutil.Now().Before(deadline) {
					ld := len(r.channel.data)
					if ld == 0 {
						//log.Infof(r.ctx, "%s no data left in channel, trigger all goroutine include worker to stop", r.name)
						return
					}
					//log.Infof(r.ctx, "%s still have %d data left in channel", r.name, ld)
				} else {
					log.Warningf(r.ctx, "%s take too much time(%d ms), will force to stop, maybe lose data in channel", r.name, r.timeoutCfg.stopRunner)
					return
				}
			}
		}
	}()

	//log.Infof(r.ctx, "%s waiting until all workers done", r.name)
	r.stopMu.Wait()

	log.Infof(r.ctx, "%s runner has been stopped", r.name)
}

// fsm update finite state machine state
func (r *Runner) fsm(state State) {
	if r.state&stateStop == 0 {
		select {
		case r.channel.fsm <- state:
		case <-time.After(InternalChannelWriteTimeout * time.Millisecond):
			log.Errorf(r.ctx, "%s take too much time to write state, will lose state:%d", r.name, state)
		}
	} else {
		log.Warningf(r.ctx, "%s state is stop, update state is not allowed", r.name)
	}
}

// close destroy channel
func (r *Runner) close() {
	r.stopMu.Do(func() {
		close(r.stopMu.c)
	})
}

// runWorker create new worker, use function to process data in channel and update metric
func (r *Runner) runWorker() {
	// new worker
	w := &Worker{
		groupID: r.workerCfg.wgid,
		id:      r.workerCfg.wid,
		name:    util.StringConcat("_", r.name, strconv.Itoa(r.workerCfg.wgid), strconv.Itoa(r.workerCfg.wid)),
		counter: r.solvedCounter,
	}
	w.destroy.c = make(chan struct{})
	r.workerCfg.wid++
	// TODO(xz): logic test got nil workers map sometimes
	if r.workerMu.workers == nil {
		log.Warningf(r.ctx, "runner %s has not init workers, but add a new worker to it", r.name)
		r.workerMu.Lock()
		r.workerMu.workers = make(map[string]*Worker)
		r.workerMu.Unlock()
	}

	r.workerMu.Lock()
	r.workerMu.workers[w.name] = w
	r.workerMu.Unlock()

	// do work
	r.stopMu.Add(1)
	go func() {
		defer r.stopMu.Done()
		defer func() {
			r.workerMu.Lock()
			delete(r.workerMu.workers, w.name)
			r.workerMu.Unlock()
			//log.Infof(r.ctx, "removed worker:%s from workers map", w.name)
		}()
		//log.Infof(r.ctx, "run worker:%s", w.name)
		tmpC := make(chan error, 1)
		for {
			select {
			case d := <-r.channel.data:
				// process data with timeout
				select {
				case tmpC <- r.workerCfg.f(d):
					if err := <-tmpC; err != nil {
						log.Errorf(r.ctx, "%s data:%s, err:%s", w.name, d, err)
					} else {
						w.last = timeutil.Now()
						w.counter.SolvedCount.Inc(1)
					}
				case <-time.After(time.Duration(r.timeoutCfg.execFunc) * time.Millisecond):
					log.Warningf(r.ctx, "worker:%s function take too much time, data:%s", w.name, d)
				}
			case <-w.destroy.c:
				//log.Infof(r.ctx, "worker:%s receive destroy signal, will destroy itself", w.name)
				return
			case <-r.stopMu.c:
				//log.Infof(r.ctx, "stop worker:%s, because runner is going to stop", w.name)
				return
			}
		}
	}()
}

// Worker run function to process data in channel
type Worker struct {
	id      int            // worker identifier id
	groupID int            // all workers started by new batch with same group id
	name    string         // worker name: groupId_id
	counter *WorkerMetrics // used to get metrics which count the solved audit data by workers
	last    time.Time      // last update time to handle data
	destroy struct {
		sync.Once
		c chan struct{}
	}
}

// close destroy channel
func (w *Worker) close() {
	w.destroy.Do(func() {
		close(w.destroy.c)
	})
}

// getID generate a runner id with timestamp
func getID() string {
	if uid, err := uuid.NewV1(); err == nil {
		return uid.String()
	}
	return strconv.FormatInt(rand.Int63(), 10)
}

// TestConfig config for test, must be run before start
func (r *Runner) TestConfig() {
	r.workerCfg.minW = 1
	r.timeoutCfg.stopRunner = 500
}

//RunnerMetrics define runner worker Gauge metrics
type RunnerMetrics struct {
	WorkerGauge *metric.Gauge
}

//MetricStruct realize interface MetricStruct() for AddMetricStruct()
func (RunnerMetrics) MetricStruct() {}

//WorkerMetrics define worker solved audit data counter metrics
type WorkerMetrics struct {
	SolvedCount *metric.Counter
}

//MetricStruct realize interface MetricStruct() for AddMetricStruct()
func (WorkerMetrics) MetricStruct() {}
