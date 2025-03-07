// Copyright 2017 The Cockroach Authors.
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

package jobs

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/storage/storagepb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/quotapool"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

const defaultLeniencySetting = 60 * time.Second

var (
	nodeLivenessLogLimiter = log.Every(5 * time.Second)
	// LeniencySetting is the amount of time to defer any attempts to
	// reschedule a job.  Visible for testing.
	LeniencySetting = settings.RegisterDurationSetting(
		"jobs.registry.leniency",
		"the amount of time to defer any attempts to reschedule a job",
		defaultLeniencySetting)
	gcSetting = settings.RegisterDurationSetting(
		"jobs.retention_time",
		"the amount of time to retain records for completed jobs before",
		time.Hour*24*14)
)

// NodeLiveness is the subset of storage.NodeLiveness's interface needed
// by Registry.
type NodeLiveness interface {
	Self() (*storagepb.Liveness, error)
	GetLivenesses() []storagepb.Liveness
}

// Registry creates Jobs and manages their leases and cancelation.
//
// Job information is stored in the `system.jobs` table.  Each node will
// poll this table and establish a lease on any claimed job. Registry
// calculates its own liveness for a node based on the expiration time
// of the underlying node-liveness lease.  This is because we want to
// allow jobs assigned to temporarily non-live (i.e. saturated) nodes to
// continue without being canceled.
//
// When a lease has been determined to be stale, a node may attempt to
// claim the relevant job. Thus, a Registry must occasionally
// re-validate its own leases to ensure that another node has not stolen
// the work and cancel the local job if so.
//
// Prior versions of Registry used the node's epoch value to determine
// whether or not a job should be stolen.  The current implementation
// uses a time-based approach, where a node's last reported expiration
// timestamp is used to calculate a liveness value for the purpose
// of job scheduling.
//
// Mixed-version operation between epoch- and time-based nodes works
// since we still publish epoch information in the leases for time-based
// nodes.  From the perspective of a time-based node, an epoch-based
// node simply behaves as though its leniency period is 0. Epoch-based
// nodes will see time-based nodes delay the act of stealing a job.
type Registry struct {
	ac       log.AmbientContext
	stopper  *stop.Stopper
	sem      *quotapool.IntPool
	db       *client.DB
	ex       sqlutil.InternalExecutor
	clock    *hlc.Clock
	nodeID   *base.NodeIDContainer
	settings *cluster.Settings
	planFn   planHookMaker
	metrics  Metrics

	mu struct {
		syncutil.Mutex
		// epoch is present to support older nodes that are not using
		// the timestamp-based approach to determine when to steal jobs.
		// TODO: Remove this and deprecate Lease.Epoch proto field
		epoch int64
		// jobs holds a map from job id to its context cancel func. This should
		// be populated with jobs that are currently being run (and owned) by
		// this registry. Calling the func will cancel the context the job was
		// started/resumed with. This should only be called by the registry when
		// it is attempting to halt its own jobs due to liveness problems. Jobs
		// are normally canceled on any node by the CANCEL JOB statement, which is
		// propagated to jobs via the .Progressed call. This function should not be
		// used to cancel a job in that way.
		jobs map[int64]context.CancelFunc
	}
	TestingResumerCreationKnobs map[jobspb.Type]func(Resumer) Resumer
}

// planHookMaker is a wrapper around sql.NewInternalPlanner. It returns an
// *sql.planner as an interface{} due to package dependency cycles. It should
// be cast to that type in the sql package when it is used. Returns a cleanup
// function that must be called once the caller is done with the planner.
//
// TODO(mjibson): Can we do something to avoid passing an interface{} here
// that must be type casted in a Resumer? It cannot be done here because
// PlanHookState lives in the sql package, which would create a dependency
// cycle if listed here. Furthermore, moving PlanHookState into a common
// subpackage like sqlbase is difficult because of the amount of sql-only
// stuff that PlanHookState exports. One other choice is to merge this package
// back into the sql package. There's maybe a better way that I'm unaware of.
type planHookMaker func(opName, user string) (interface{}, func())

// MakeRegistry creates a new Registry. planFn is a wrapper around
// sql.newInternalPlanner. It returns a sql.PlanHookState, but must be
// coerced into that in the Resumer functions.
func MakeRegistry(
	ac log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *client.DB,
	ex sqlutil.InternalExecutor,
	nodeID *base.NodeIDContainer,
	settings *cluster.Settings,
	histogramWindowInterval time.Duration,
	planFn planHookMaker,
) *Registry {
	r := &Registry{
		ac:       ac,
		stopper:  stopper,
		sem:      quotapool.NewIntPool("Job Registry", uint64(runtime.NumCPU())),
		clock:    clock,
		db:       db,
		ex:       ex,
		nodeID:   nodeID,
		settings: settings,
		planFn:   planFn,
	}
	r.mu.epoch = 1
	r.mu.jobs = make(map[int64]context.CancelFunc)
	r.metrics.InitHooks(histogramWindowInterval)
	return r
}

// MetricsStruct returns the metrics for production monitoring of each job type.
// They're all stored as the `metric.Struct` interface because of dependency
// cycles.
func (r *Registry) MetricsStruct() *Metrics {
	return &r.metrics
}

// CurrentlyRunningJobs returns a slice of the ids of all jobs running on this node.
func (r *Registry) CurrentlyRunningJobs() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	jobs := make([]int64, len(r.mu.jobs))
	i := 0
	for jID := range r.mu.jobs {
		jobs[i] = jID
		i++
	}
	return jobs
}

// CurrentStatus returns the current job status from the jobs table or error.
func (j *Job) CurrentStatus(ctx context.Context) (Status, error) {
	if j.id == nil {
		return "", errors.New("job has not been created")
	}
	var statusString tree.DString
	if err := j.runInTxn(ctx, func(ctx context.Context, txn *client.Txn) error {
		const selectStmt = "SELECT status FROM system.jobs WHERE id = $1"
		row, err := j.registry.ex.QueryRow(ctx, "job-status", txn, selectStmt, *j.ID())
		if err != nil {
			return errors.Wrapf(err, "job %d: can't query system.jobs", *j.ID())
		}
		if row == nil {
			return errors.Errorf("job %d: not found in system.jobs", *j.ID())
		}

		statusString = tree.MustBeDString(row[0])
		return nil
	}); err != nil {
		return "", err
	}
	return Status(statusString), nil
}

// lenientNow returns the timestamp after which we should attempt
// to steal a job from a node whose liveness is failing.  This allows
// jobs coordinated by a node which is temporarily saturated to continue.
func (r *Registry) lenientNow() hlc.Timestamp {
	// We see this in tests.
	var offset time.Duration
	if r.settings == cluster.NoSettings {
		offset = defaultLeniencySetting
	} else {
		offset = LeniencySetting.Get(&r.settings.SV)
	}

	return r.clock.Now().Add(-offset.Nanoseconds(), 0)
}

// makeCtx returns a new context from r's ambient context and an associated
// cancel func.
func (r *Registry) makeCtx() (context.Context, func()) {
	return context.WithCancel(r.ac.AnnotateCtx(context.Background()))
}

func (r *Registry) makeJobID() int64 {
	return int64(builtins.GenerateUniqueInt(r.nodeID.Get()))
}

// StartJob creates and asynchronously starts a job from record. An error is
// returned if the job type has not been registered with AddResumeHook. The
// ctx passed to this function is not the context the job will be started
// with (canceling ctx will not causing the job to cancel).
func (r *Registry) StartJob(
	ctx context.Context, resultsCh chan<- tree.Datums, record Record,
) (*Job, <-chan error, error) {
	resumer, err := r.getResumeHook(jobspb.DetailsType(jobspb.WrapPayloadDetails(record.Details)), r.settings)
	if err != nil {
		return nil, nil, err
	}
	j := r.NewJob(record)
	// We create the job with a known ID, rather than relying on the column's
	// DEFAULT unique_rowid(), to avoid a race condition where the job exists
	// in the jobs table but is not yet present in our registry, which would
	// allow another node to adopt it.
	id := r.makeJobID()
	resumeCtx, cancel := r.makeCtx()
	r.register(id, cancel)
	if err := j.insert(ctx, id, r.newLease()); err != nil {
		r.unregister(id)
		return nil, nil, err
	}
	if err := j.Started(ctx); err != nil {
		r.unregister(id)
		return nil, nil, err
	}
	errCh, err := r.resume(resumeCtx, resumer, resultsCh, j)
	if err != nil {
		return nil, nil, err
	}
	return j, errCh, nil
}

// NewJob creates a new Job.
func (r *Registry) NewJob(record Record) *Job {
	job := &Job{
		registry:  r,
		createdBy: record.CreatedBy,
	}
	job.mu.payload = jobspb.Payload{
		Description:   record.Description,
		Statement:     record.Statement,
		Username:      record.Username,
		DescriptorIDs: record.DescriptorIDs,
		Details:       jobspb.WrapPayloadDetails(record.Details),
		Noncancelable: record.NonCancelable,
	}
	job.mu.progress = jobspb.Progress{
		Details:       jobspb.WrapProgressDetails(record.Progress),
		RunningStatus: string(record.RunningStatus),
	}
	return job
}

// LoadJob loads an existing job with the given jobID from the system.jobs
// table.
func (r *Registry) LoadJob(ctx context.Context, jobID int64) (*Job, error) {
	return r.LoadJobWithTxn(ctx, jobID, nil)
}

// CreateJobWithTxn creates a job to be started later with StartJob.
// It stores the job in the jobs table, marks it pending and gives the
// current node a lease.
func (r *Registry) CreateJobWithTxn(
	ctx context.Context, record Record, txn *client.Txn,
) (*Job, error) {
	j := r.NewJob(record)
	if err := j.WithTxn(txn).insert(ctx, r.makeJobID(), r.newLease()); err != nil {
		return nil, err
	}
	return j, nil
}

// CreateStartableJobWithTxn creates a job to be started later, after the
// creating txn commits. The method uses the passed txn to write the job in the
// jobs table, marks it pending and gives the current node a lease. It
// additionally registers the job with the Registry which will prevent the
// Registry from adopting the job after the transaction commits. The resultsCh
// will be connected to the output of the job and written to after the returned
// StartableJob is started.
//
// The returned job is not associated with the user transaction. The intention
// is that the job will not be modified again in txn. If the transaction is
// committed, the caller must explicitly Start it. If the transaction is rolled
// back then the caller must call CleanupOnRollback to unregister the job from
// the Registry.
func (r *Registry) CreateStartableJobWithTxn(
	ctx context.Context, record Record, txn *client.Txn, resultsCh chan<- tree.Datums,
) (*StartableJob, error) {
	j, err := r.CreateJobWithTxn(ctx, record, txn)
	if err != nil {
		return nil, err
	}
	// The job itself must not hold on to this transaction. We ensure in Start()
	// that the transaction used to create the job is committed. When jobs hold
	// onto transactions they use the transaction in methods which modify the job.
	// On the whole this pattern is bug-prone and hard to reason about.
	j.WithTxn(nil)
	payload := j.Payload()
	resumer, err := r.getResumeHook(payload.Type(), r.settings)
	if err != nil {
		return nil, err
	}
	resumerCtx, cancel := r.makeCtx()
	r.register(*j.ID(), cancel)
	return &StartableJob{
		Job:        j,
		txn:        txn,
		resumer:    resumer,
		resumerCtx: resumerCtx,
		cancel:     cancel,
		resultsCh:  resultsCh,
	}, nil
}

// LoadJobWithTxn does the same as above, but using the transaction passed in
// the txn argument. Passing a nil transaction is equivalent to calling LoadJob
// in that a transaction will be automatically created.
func (r *Registry) LoadJobWithTxn(ctx context.Context, jobID int64, txn *client.Txn) (*Job, error) {
	j := &Job{
		id:       &jobID,
		registry: r,
	}
	if err := j.WithTxn(txn).load(ctx); err != nil {
		return nil, err
	}
	return j, nil
}

// DefaultCancelInterval is a reasonable interval at which to poll this node
// for liveness failures and cancel running jobs.
var DefaultCancelInterval = base.DefaultHeartbeatInterval

// DefaultAdoptInterval is a reasonable interval at which to poll system.jobs
// for jobs with expired leases.
//
// DefaultAdoptInterval is mutable for testing. NB: Updates to this value after
// Registry.Start has been called will not have any effect.
var DefaultAdoptInterval = 30 * time.Second

// gcInterval is how often we check for and delete job records older than the
// retention limit.
const gcInterval = 1 * time.Hour

// Start polls the current node for liveness failures and cancels all registered
// jobs if it observes a failure.
func (r *Registry) Start(
	ctx context.Context,
	stopper *stop.Stopper,
	nl NodeLiveness,
	cancelInterval, adoptInterval time.Duration,
) error {
	// Calling maybeCancelJobs once at the start ensures we have an up-to-date
	// liveness epoch before we wait out the first cancelInterval.
	r.maybeCancelJobs(ctx, nl)

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-time.After(cancelInterval):
				r.maybeCancelJobs(ctx, nl)
			case <-stopper.ShouldStop():
				return
			}
		}
	})

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-time.After(gcInterval):
				old := timeutil.Now().Add(-1 * gcSetting.Get(&r.settings.SV))
				if err := r.cleanupOldJobs(ctx, old); err != nil {
					log.Warningf(ctx, "error cleaning up old job records: %v", err)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})

	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-time.After(adoptInterval):
				if err := r.maybeAdoptJob(ctx, nl); err != nil {
					log.Errorf(ctx, "error while adopting jobs: %s", err)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	return nil
}

func (r *Registry) maybeCancelJobs(ctx context.Context, nl NodeLiveness) {
	liveness, err := nl.Self()
	if err != nil {
		if nodeLivenessLogLimiter.ShouldLog() {
			log.Warningf(ctx, "unable to get node liveness: %s", err)
		}
		// Conservatively assume our lease has expired. Abort all jobs.
		r.mu.Lock()
		defer r.mu.Unlock()
		r.cancelAll(ctx)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// If we haven't persisted a liveness record within the leniency
	// interval, we'll cancel all of our jobs.
	if !liveness.IsLive(r.lenientNow(), r.clock.MaxOffset()) {
		r.cancelAll(ctx)
		r.mu.epoch = liveness.Epoch
		return
	}

	// Finally, we cancel all jobs if the stopper is quiescing.
	select {
	case <-r.stopper.ShouldQuiesce():
		r.cancelAll(ctx)
	default:
	}
}

func (r *Registry) cleanupOldJobs(ctx context.Context, olderThan time.Time) error {
	const stmt = `SELECT id, payload FROM system.jobs WHERE status IN ($1, $2, $3) AND created < $4 ORDER BY created LIMIT 1000`
	rows, err := r.ex.Query(
		ctx, "gc-jobs", nil /* txn */, stmt, StatusFailed, StatusSucceeded, StatusCanceled, olderThan,
	)
	if err != nil {
		return err
	}

	toDelete := tree.NewDArray(types.Int)
	toDelete.Array = make(tree.Datums, 0, len(rows))
	oldMicros := timeutil.ToUnixMicros(olderThan)
	for _, row := range rows {
		payload, err := UnmarshalPayload(row[1])
		if err != nil {
			return err
		}
		if payload.FinishedMicros < oldMicros {
			toDelete.Array = append(toDelete.Array, row[0])
		}
	}
	if len(toDelete.Array) > 0 {

		log.Infof(ctx, "cleaning up %d expired job records", len(toDelete.Array))
		const stmt = `DELETE FROM system.jobs WHERE id = ANY($1)`
		if _ /* cols */, err := r.ex.Exec(
			ctx, "gc-jobs", nil /* txn */, stmt, toDelete,
		); err != nil {
			return errors.Wrap(err, "deleting old jobs")
		}
	}
	return nil
}

// getJobFn attempts to get a resumer from the given job id. If the job id
// does not have a resumer then it returns an error message suitable for users.
func (r *Registry) getJobFn(ctx context.Context, txn *client.Txn, id int64) (*Job, Resumer, error) {
	job, err := r.LoadJobWithTxn(ctx, id, txn)
	if err != nil {
		return nil, nil, err
	}
	payload := job.Payload()
	resumer, err := r.getResumeHook(payload.Type(), r.settings)
	if err != nil {
		return job, nil, errors.Errorf("job %d is not controllable", id)
	}
	return job, resumer, nil
}

// Cancel marks the job with id as canceled using the specified txn (may be nil).
func (r *Registry) Cancel(ctx context.Context, txn *client.Txn, id int64) error {
	job, resumer, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		// Special case schema change jobs to mark the job as canceled.
		if job != nil {
			payload := job.Payload()
			// TODO(mjibson): Use an unfortunate workaround to enable canceling of
			// schema change jobs by comparing the string description. When a schema
			// change job fails or is canceled, a new job is created with the ROLL BACK
			// prefix. These rollback jobs cannot be canceled. We could add a field to
			// the payload proto to indicate if this job is cancelable or not, but in
			// a split version cluster an older node could pick up the schema change
			// and fail to clear/set that field appropriately. Thus it seems that the
			// safest way for now (i.e., without a larger jobs/schema change refactor)
			// is to hack this up with a string comparison.
			if payload.Type() == jobspb.TypeSchemaChange && !strings.HasPrefix(payload.Description, "ROLL BACK") {
				return job.WithTxn(txn).canceled(ctx, NoopFn)
			}
		}
		return err
	}
	return job.WithTxn(txn).canceled(ctx, resumer.OnFailOrCancel)
}

// Pause marks the job with id as paused using the specified txn (may be nil).
func (r *Registry) Pause(ctx context.Context, txn *client.Txn, id int64) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).paused(ctx)
}

// Resume resumes the paused job with id using the specified txn (may be nil).
func (r *Registry) Resume(ctx context.Context, txn *client.Txn, id int64) error {
	job, _, err := r.getJobFn(ctx, txn, id)
	if err != nil {
		return err
	}
	return job.WithTxn(txn).resumed(ctx)
}

// TestingSetAdoptAndCancelIntervals can be used to accelerate job adoption and
// state changes.
func TestingSetAdoptAndCancelIntervals(adopt, cancel time.Duration) (cleanup func()) {
	prevAdopt := DefaultAdoptInterval
	prevCancel := DefaultCancelInterval
	DefaultAdoptInterval = adopt
	DefaultCancelInterval = cancel
	return func() {
		DefaultAdoptInterval = prevAdopt
		DefaultCancelInterval = prevCancel
	}
}

// Resumer is a resumable Job. Jobs can be paused or canceled at any time. Jobs
// should call their .Progressed method, which will return an error if the
// job has been paused or canceled. Functions that take a client.Txn argument
// must ensure that any work they do is still correct if the txn is aborted
// at a later time.
type Resumer interface {
	// Resume is called when a job is started or resumed. Sending results on the
	// chan will return them to a user, if a user's session is connected. phs
	// is a sql.PlanHookState.
	Resume(ctx context.Context, job *Job, phs interface{}, resultsCh chan<- tree.Datums) error
	// OnSuccess is called when a job has completed successfully, and is called
	// with the same txn that will mark the job as successful. The txn will
	// only be committed if this doesn't return an error and the job state was
	// successfully changed to successful. If OnSuccess returns an error, the
	// job will be marked as failed.
	OnSuccess(context.Context, *client.Txn, *Job) error
	// OnTerminal is called after a job has successfully been marked as
	// terminal. It should be used to perform optional cleanup and return final
	// results to the user. There is no guarantee that this function is ever run
	// (for example, if a node died immediately after Success commits).
	OnTerminal(context.Context, *Job, Status, chan<- tree.Datums)

	// OnFailOrCancel is called when a job fails or is canceled, and is called
	// with the same txn that will mark the job as failed or canceled. The txn
	// will only be committed if this doesn't return an error and the job state
	// was successfully changed to failed or canceled. This is done so that
	// transactional cleanup can be guaranteed to have happened.
	//
	// Since this method can be called during cancellation, which is not
	// guaranteed to run on the node where the job is running, it is not safe
	// for it to use state from the Resumer.
	OnFailOrCancel(context.Context, *client.Txn, *Job) error
}

// ResumeHookFn returns a resumable job based on the Type, or nil if the
// hook cannot serve the Type. The Resumer is created on the coordinator
// each time the job is started/resumed, so it can hold state. The Resume
// method is always ran, and can set state on the Resumer that can be used
// by the other methods.
type ResumeHookFn func(jobspb.Type, *cluster.Settings) Resumer

var resumeHooks []ResumeHookFn

func (r *Registry) getResumeHook(typ jobspb.Type, settings *cluster.Settings) (Resumer, error) {
	for _, hook := range resumeHooks {
		if resumer := hook(typ, settings); resumer != nil {
			if fn, ok := r.TestingResumerCreationKnobs[typ]; ok {
				return fn(resumer), nil
			}
			return resumer, nil
		}
	}
	return nil, errors.Errorf("no resumer are available for %s", typ)
}

type retryJobError string

// NewRetryJobError creates a new error that, if returned by a Resumer,
// indicates to the jobs registry that the job should be restarted in the
// background.
func NewRetryJobError(s string) error {
	return retryJobError(s)
}

func (r retryJobError) Error() string {
	return string(r)
}

// resume starts or resumes a job. If no error is returned then the job was
// asynchronously executed. The job is executed with the ctx, so ctx must
// only by canceled if the job should also be canceled. resultsCh is passed
// to the resumable func and should be closed by the caller after errCh sends
// a value. errCh returns an error if the job was not completed with success.
func (r *Registry) resume(
	ctx context.Context, resumer Resumer, resultsCh chan<- tree.Datums, job *Job,
) (<-chan error, error) {
	errCh := make(chan error, 1)
	taskName := fmt.Sprintf(`job-%d`, *job.ID())
	if err := r.stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
		payload := job.Payload()
		phs, cleanup := r.planFn("resume-"+taskName, payload.Username)
		defer cleanup()
		spanName := fmt.Sprintf(`%s-%d`, payload.Type(), *job.ID())
		var span opentracing.Span
		ctx, span = r.ac.AnnotateCtxWithSpan(ctx, spanName)
		defer span.Finish()
		resumeErr := resumer.Resume(ctx, job, phs, resultsCh)
		if resumeErr != nil && ctx.Err() != nil {
			// The context was canceled. Tell the user, but don't attempt to mark the
			// job as failed because it can be resumed by another node.
			resumeErr = NewRetryJobError("node liveness error")
		}
		if e, ok := resumeErr.(retryJobError); ok {
			r.unregister(*job.id)
			errCh <- errors.Errorf("job %d: %s: restarting in background", *job.id, e)
			return
		}
		terminal := true
		var status Status
		defer r.unregister(*job.id)
		if err, ok := errors.Cause(resumeErr).(*InvalidStatusError); ok &&
			(err.status == StatusPaused || err.status == StatusCanceled) {
			if err.status == StatusPaused {
				terminal = false
			}
			// If we couldn't operate on the job because it was paused or canceled, send
			// the more understandable "job paused" or "job canceled" error message to
			// the user.
			resumeErr = errors.Errorf("job %s", err.status)
			status = err.status
		} else {
			// Attempt to mark the job as succeeded.
			if resumeErr == nil {
				status = StatusSucceeded
				if err := job.Succeeded(ctx, resumer.OnSuccess); err != nil {
					// If it didn't succeed, mark it failed.
					resumeErr = errors.Wrapf(err, "could not mark job %d as succeeded", *job.id)
				}
			}
			if resumeErr != nil {
				status = StatusFailed
				if err := job.Failed(ctx, resumeErr, resumer.OnFailOrCancel); err != nil {
					// If we can't transactionally mark the job as failed then it will be
					// restarted during the next adopt loop and retried.
					resumeErr = errors.Wrapf(err, "could not mark job %d as failed: %s", *job.id, resumeErr)
					terminal = false
				}
			}
		}

		if terminal {
			resumer.OnTerminal(ctx, job, status, resultsCh)
		}
		errCh <- resumeErr
	}); err != nil {
		return nil, err
	}
	return errCh, nil
}

// AddResumeHook adds a resume hook.
func AddResumeHook(fn ResumeHookFn) {
	resumeHooks = append(resumeHooks, fn)
}

func (r *Registry) maybeAdoptJob(ctx context.Context, nl NodeLiveness) error {
	const stmt = `SELECT id, payload, progress IS NULL FROM system.jobs WHERE status IN ($1, $2) ORDER BY created DESC`
	rows, err := r.ex.Query(
		ctx, "adopt-job", nil /* txn */, stmt, StatusPending, StatusRunning,
	)
	if err != nil {
		return err
	}

	type nodeStatus struct {
		isLive bool
	}
	nodeStatusMap := map[roachpb.NodeID]*nodeStatus{
		// 0 is not a valid node ID, but we treat it as an always-dead node so that
		// the empty lease (Lease{}) is always considered expired.
		0: {isLive: false},
	}
	{
		// We subtract the leniency interval here to artificially
		// widen the range of times over which the job registry will
		// consider the node to be alive.  We rely on the fact that
		// only a live node updates its own expiration.  Thus, the
		// expiration time can be used as a reasonable measure of
		// when the node was last seen.
		now, maxOffset := r.lenientNow(), r.clock.MaxOffset()
		for _, liveness := range nl.GetLivenesses() {
			nodeStatusMap[liveness.NodeID] = &nodeStatus{
				isLive: liveness.IsLive(now, maxOffset),
			}

			// Don't try to start any more jobs unless we're really live,
			// otherwise we'd just immediately cancel them.
			if liveness.NodeID == r.nodeID.Get() {
				if !liveness.IsLive(r.clock.Now(), maxOffset) {
					return nil
				}
			}
		}
	}

	for _, row := range rows {
		id := (*int64)(row[0].(*tree.DInt))

		payload, err := UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		if log.V(2) {
			log.Infof(ctx, "evaluating job %d with lease %#v", *id, payload.Lease)
		}

		if payload.Lease == nil {
			// If the lease is missing, it simply means the job does not yet support
			// resumability.
			if log.V(2) {
				log.Infof(ctx, "job %d: skipping: nil lease", *id)
			}
			continue
		}

		// If the job has no progress it is from a 2.0 cluster. If the entire cluster
		// has been upgraded to 2.1 then we know nothing is running the job and it
		// can be safely failed.
		if nullProgress, ok := row[2].(*tree.DBool); ok && bool(*nullProgress) {
			payload.Error = "job predates cluster upgrade and must be re-run"
			payloadBytes, err := protoutil.Marshal(payload)
			if err != nil {
				return err
			}

			// We can't use job.update here because it fails while attempting to unmarshal
			// the progress. Setting the status to failed is idempotent so we don't care
			// if multiple nodes execute this.
			const updateStmt = `UPDATE system.jobs SET status = $1, payload = $2 WHERE id = $3`
			updateArgs := []interface{}{StatusFailed, payloadBytes, *id}
			err = r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				_, err := r.ex.Exec(ctx, "job-update", txn, updateStmt, updateArgs...)
				return err
			})
			if err != nil {
				log.Warningf(ctx, "job %d: has no progress but unable to mark failed: %s", id, err)
			}
			continue
		}

		r.mu.Lock()
		_, running := r.mu.jobs[*id]
		r.mu.Unlock()

		var needsResume bool
		if payload.Lease.NodeID == r.nodeID.Get() {
			// If we hold the lease for a job, check to see if we're actually running
			// that job, and resume it if we're not. Otherwise, the job will be stuck
			// until this node is restarted, as the other nodes in the cluster see
			// that we hold a valid lease and assume we're running the job.
			//
			// We end up in this state—a valid lease for a canceled job—when we
			// overcautiously cancel all jobs due to e.g. a slow heartbeat response.
			// If that heartbeat managed to successfully extend the liveness lease,
			// we'll have stopped running jobs on which we still had valid leases.
			//
			// This path also takes care of the case where a node adopts a job
			// and is then restarted; the node will attempt to restart
			// any previously-leased job.
			needsResume = !running
		} else {
			// If we are currently running a job that another node has the lease on,
			// stop running it.
			if running {
				log.Warningf(ctx, "job %d: node %d owns lease; canceling", *id, payload.Lease.NodeID)
				r.unregister(*id)
				continue
			}
			nodeStatus, ok := nodeStatusMap[payload.Lease.NodeID]
			if !ok {
				log.Warningf(ctx, "job %d: skipping: no liveness record for node %d",
					*id, payload.Lease.NodeID)
				continue
			}
			needsResume = !nodeStatus.isLive
		}

		if !needsResume {
			continue
		}

		job := Job{id: id, registry: r}
		resumeCtx, cancel := r.makeCtx()
		if err := job.adopt(ctx, payload.Lease); err != nil {
			return errors.Wrap(err, "unable to acquire lease")
		}
		r.register(*id, cancel)

		resultsCh := make(chan tree.Datums)
		resumer, err := r.getResumeHook(payload.Type(), r.settings)
		if err != nil {
			return err
		}
		errCh, err := r.resume(resumeCtx, resumer, resultsCh, &job)
		if err != nil {
			return err
		}
		go func() {
			// Drain and ignore results.
			for range resultsCh {
			}
		}()
		go func() {
			// Wait for the job to finish. No need to print the error because if there
			// was one it's been set in the job status already.
			<-errCh
			close(resultsCh)
		}()

		// Only adopt one job per turn to allow other nodes their fair share.
		break
	}

	return nil
}

func (r *Registry) newLease() *jobspb.Lease {
	nodeID := r.nodeID.Get()
	if nodeID == 0 {
		panic("jobs.Registry has empty node ID")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return &jobspb.Lease{NodeID: nodeID, Epoch: r.mu.epoch}
}

func (r *Registry) cancelAll(ctx context.Context) {
	r.mu.AssertHeld()
	for jobID, cancel := range r.mu.jobs {
		log.Warningf(ctx, "job %d: canceling due to liveness failure", jobID)
		cancel()
	}
	r.mu.jobs = make(map[int64]context.CancelFunc)
}

func (r *Registry) register(jobID int64, cancel func()) {
	r.mu.Lock()
	r.mu.jobs[jobID] = cancel
	r.mu.Unlock()
}

func (r *Registry) unregister(jobID int64) {
	r.mu.Lock()
	cancel, ok := r.mu.jobs[jobID]
	// It is possible for a job to be double unregistered. unregister is always
	// called at the end of resume. But it can also be called during cancelAll
	// and in the adopt loop under certain circumstances.
	if ok {
		cancel()
		delete(r.mu.jobs, jobID)
	}
	r.mu.Unlock()
}
