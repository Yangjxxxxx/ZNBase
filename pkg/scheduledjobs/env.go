package scheduledjobs

import (
	"time"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// JobSchedulerEnv is an environment for running scheduled jobs.
// This environment facilitates dependency injection mechanism for tests.
type JobSchedulerEnv interface {
	// ScheduledJobsTableName returns the name of the scheduled_jobs table.
	ScheduledJobsTableName() string
	// SystemJobsTableName returns the name of the system jobs table.
	SystemJobsTableName() string
	// Now returns current time.
	Now() time.Time
	// NowExpr returns expression representing current time when
	// used in the database queries.
	NowExpr() string
}

// JobExecutionConfig encapsulates external components needed for scheduled job execution.
type JobExecutionConfig struct {
	Settings         *cluster.Settings
	InternalExecutor sqlutil.InternalExecutor
	DB               *client.DB
	// TestingKnobs is *jobs.TestingKnobs; however we cannot depend
	// on jobs package due to circular dependencies.
	TestingKnobs base.ModuleTestingKnobs
	// PlanHookMaker is responsible for creating sql.NewInternalPlanner. It returns an
	// *sql.planner as an interface{} due to package dependency cycles. It should
	// be cast to that type in the sql package when it is used. Returns a cleanup
	// function that must be called once the caller is done with the planner.
	// This is the same mechanism used in jobs.Registry.
	PlanHookMaker func(opName string, tnx *client.Txn, user string) (interface{}, func())
}

// production JobSchedulerEnv implementation.
type prodJobSchedulerEnvImpl struct{}

// ProdJobSchedulerEnv is a JobSchedulerEnv implementation suitable for production.
var ProdJobSchedulerEnv JobSchedulerEnv = &prodJobSchedulerEnvImpl{}

func (e *prodJobSchedulerEnvImpl) ScheduledJobsTableName() string {
	return "system.scheduled_jobs"
}

func (e *prodJobSchedulerEnvImpl) SystemJobsTableName() string {
	return "system.jobs"
}

func (e *prodJobSchedulerEnvImpl) Now() time.Time {
	return timeutil.Now()
}

func (e *prodJobSchedulerEnvImpl) NowExpr() string {
	return "current_timestamp()"
}
