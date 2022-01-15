package dump

import (
	"context"
	"fmt"
	"strings"
	"time"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/gorhill/cronexpr"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/scheduledjobs"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

const (
	optFirstRun          = "first_run"
	optOnExecFailure     = "on_execution_failure"
	optOnPreviousRunning = "on_previous_running"
)

var scheduledBackupOptionExpectValues = map[string]sql.KVStringOptValidate{
	optFirstRun:          sql.KVStringOptRequireValue,
	optOnExecFailure:     sql.KVStringOptRequireValue,
	optOnPreviousRunning: sql.KVStringOptRequireValue,
}

// scheduledBackupEval is a representation of tree.ScheduledBackup, prepared
// for evaluation
type scheduledBackupEval struct {
	*tree.ScheduledBackup

	// Schedule specific properties that get evaluated.
	scheduleName         func() (string, error)
	recurrence           func() (string, error)
	fullBackupRecurrence func() (string, error)
	scheduleOpts         func() (map[string]string, error)

	// Backup specific properties that get evaluated.
	// We need to evaluate anything in the tree.Backup node that allows
	// placeholders to be specified so that we store evaluated
	// backup statement in the schedule.
	destination func() ([]string, error)
}

func parseOnError(onError string, details *jobspb.ScheduleDetails) error {
	switch strings.ToLower(onError) {
	case "retry":
		details.OnError = jobspb.ScheduleDetails_RETRY_SOON
	case "reschedule":
		details.OnError = jobspb.ScheduleDetails_RETRY_SCHED
	case "pause":
		details.OnError = jobspb.ScheduleDetails_PAUSE_SCHED
	default:
		return errors.Newf(
			"%q is not a valid on_execution_error; valid values are [retry|reschedule|pause]",
			onError)
	}
	return nil
}

func parseWaitBehavior(wait string, details *jobspb.ScheduleDetails) error {
	switch strings.ToLower(wait) {
	case "start":
		details.Wait = jobspb.ScheduleDetails_NO_WAIT
	case "skip":
		details.Wait = jobspb.ScheduleDetails_SKIP
	case "wait":
		details.Wait = jobspb.ScheduleDetails_WAIT
	default:
		return errors.Newf(
			"%q is not a valid on_previous_running; valid values are [start|skip|wait]",
			wait)
	}
	return nil
}

func setScheduleOptions(
	evalCtx *tree.EvalContext, opts map[string]string, sj *jobs.ScheduledJob,
) error {
	if v, ok := opts[optFirstRun]; ok {
		firstRun, err := tree.ParseDTimestampTZ(evalCtx, v, time.Microsecond)
		if err != nil {
			return err
		}
		sj.SetNextRun(firstRun.Time)
	}

	var details jobspb.ScheduleDetails
	if v, ok := opts[optOnExecFailure]; ok {
		if err := parseOnError(v, &details); err != nil {
			return err
		}
	}
	if v, ok := opts[optOnPreviousRunning]; ok {
		if err := parseWaitBehavior(v, &details); err != nil {
			return err
		}
	}

	var defaultDetails jobspb.ScheduleDetails
	if details != defaultDetails {
		sj.SetScheduleDetails(details)
	}

	return nil
}

type scheduleRecurrence struct {
	cron      string
	frequency time.Duration
}

var neverRecurs *scheduleRecurrence

func computeScheduleRecurrence(
	now time.Time, evalFn func() (string, error),
) (*scheduleRecurrence, error) {
	if evalFn == nil {
		return neverRecurs, nil
	}
	cron, err := evalFn()
	if err != nil {
		return nil, err
	}
	expr, err := cronexpr.Parse(cron)
	if err != nil {
		return nil, err
	}
	nextRun := expr.Next(now)
	frequency := expr.Next(nextRun).Sub(nextRun)
	return &scheduleRecurrence{cron, frequency}, nil
}

var humanDurations = map[time.Duration]string{
	time.Hour:          "hour",
	24 * time.Hour:     "day",
	7 * 24 * time.Hour: "week",
}

func (r *scheduleRecurrence) Humanize() string {
	if d, ok := humanDurations[r.frequency]; ok {
		return "every " + d
	}
	return "every " + r.frequency.String()
}

var forceFullBackup *scheduleRecurrence

func pickFullRecurrenceFromIncremental(inc *scheduleRecurrence) *scheduleRecurrence {
	if inc.frequency <= time.Hour {
		// If incremental is faster than once an hour, take fulls every day,
		// some time between midnight and 1 am.
		return &scheduleRecurrence{
			cron:      "@daily",
			frequency: 24 * time.Hour,
		}
	}

	if inc.frequency <= 24*time.Hour {
		// If incremental is less than a day, take full weekly;  some day
		// between 0 and 1 am.
		return &scheduleRecurrence{
			cron:      "@weekly",
			frequency: 7 * 24 * time.Hour,
		}
	}

	// Incremental period too large.
	return forceFullBackup
}

func scheduleFirstRun(evalCtx *tree.EvalContext, opts map[string]string) (*time.Time, error) {
	if v, ok := opts[optFirstRun]; ok {
		firstRun, err := tree.ParseDTimestampTZ(evalCtx, v, time.Microsecond)
		if err != nil {
			return nil, err
		}
		return &firstRun.Time, nil
	}
	return nil, nil
}

// doCreateBackupSchedule creates requested schedule (or schedules).
// It is a plan hook implementation responsible for the creating of scheduled backup.
func doCreateBackupSchedules(
	ctx context.Context, p sql.PlanHookState, eval *scheduledBackupEval, resultsCh chan<- tree.Datums,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}
	// Check if the target is correct
	// Check the database
	if eval.Targets.Databases != nil {
		for _, database := range eval.Targets.Databases {
			_, err := p.ResolveUncachedDatabaseByName(ctx, database.String(), true)
			if err != nil {
				return err
			}
		}
	}
	// Check the schema
	if eval.Targets.Schemas != nil {
		for _, schema := range eval.Targets.Schemas {
			found, _, err := p.LookupSchema(ctx, p.CurrentDatabase(), schema.String())
			if err != nil {
				return err
			}
			if schema.Catalog() != "" && schema.Catalog() != p.CurrentDatabase() {
				return errors.Errorf("You can only backup the schema of your current database")
			}
			if !found {
				return errors.Errorf("schema %v doesn't exist", schema.String())
			}
		}
	}
	// Check the table
	allDescs, err := loadAllDescs(ctx, p.ExecCfg().DB, p.ExecCfg().Clock.Now())
	if err != nil {
		return err
	}
	resolver := &sqlDescriptorResolver{
		allDescs:   allDescs,
		descByID:   make(map[sqlbase.ID]sqlbase.Descriptor),
		dbsByName:  make(map[string]sqlbase.ID),
		schsByName: make(map[string]sqlbase.ID),
		scsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
		tbsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
		tbsByID:    make(map[sqlbase.ID]map[sqlbase.ID]string),
	}
	err = resolver.initResolver(false)
	if err != nil {
		return err
	}

	if eval.Targets.Tables != nil {
		for _, pattern := range eval.Targets.Tables {
			var err error
			pattern, err = pattern.NormalizeTablePattern()
			if err != nil {
				return err
			}

			switch pat := pattern.(type) {
			case *tree.TableName:
				found, _, err := pat.ResolveExisting(ctx, resolver,
					false, /*requireMutable*/
					p.CurrentDatabase(), p.CurrentSearchPath())
				if err != nil {
					return err
				}
				if !found {
					return errors.Errorf(`table %q does not exist`, tree.ErrString(pat))
				}
			case *tree.AllTablesSelector:
				found, _, err := pat.TableNamePrefix.Resolve(ctx, resolver,
					p.CurrentDatabase(), p.CurrentSearchPath())
				if err != nil {
					return err
				}
				if !found {
					return sqlbase.NewInvalidWildcardError(tree.ErrString(pat))
				}
			}
		}
	}

	// Evaluate incremental and full recurrence.
	incRecurrence, err := computeScheduleRecurrence(env.Now(), eval.recurrence)
	if err != nil {
		return err
	}
	fullRecurrence, err := computeScheduleRecurrence(env.Now(), eval.fullBackupRecurrence)
	if err != nil {
		return err
	}

	fullRecurrencePicked := false
	if incRecurrence != nil && fullRecurrence == nil {
		// It's an enterprise user; let's see if we can pick a reasonable
		// full  backup recurrence based on requested incremental recurrence.
		fullRecurrence = pickFullRecurrenceFromIncremental(incRecurrence)
		fullRecurrencePicked = true

		if fullRecurrence == forceFullBackup {
			fullRecurrence = incRecurrence
			incRecurrence = nil
		}
	}

	if fullRecurrence == nil {
		return errors.AssertionFailedf(" full backup recurrence should be set")
	}

	// Prepare backup statement (full).
	backupNode := &tree.Dump{
		Nested:         true,
		AppendToLatest: false,
		Options:        eval.Options,
		Targets:        eval.Targets,
		FileFormat:     eval.FileFormat,
		CurrentPath:    &tree.CurrentSession{Database: p.CurrentDatabase(), SearchPath: p.CurrentSearchPath()},
	}

	// Evaluate required backup destinations.
	destinations, err := eval.destination()
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate backup destination paths")
	}
	_, err = p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, destinations[0])
	if err != nil {
		return err
	}

	for _, dest := range destinations {
		backupNode.To = append(backupNode.To, tree.NewDString(dest))
		backupNode.File = tree.NewDString(dest)
	}

	var fullScheduleName string
	if eval.scheduleName != nil {
		scheduleName, err := eval.scheduleName()
		if err != nil {
			return err
		}
		fullScheduleName = scheduleName
	} else {
		fullScheduleName = fmt.Sprintf("BACKUP %d", env.Now().Unix())
	}

	scheduleOptions, err := eval.scheduleOpts()
	if err != nil {
		return err
	}

	firstRun, err := scheduleFirstRun(&p.ExtendedEvalContext().EvalContext, scheduleOptions)
	if err != nil {
		return err
	}

	return p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Create FULL backup schedule.
		var fullSchedule *jobs.ScheduledJob
		if fullSchedule, err = makeBackupSchedule(
			ctx, p, env, fullScheduleName, fullRecurrence,
			scheduleOptions, backupNode, resultsCh, txn,
		); err != nil {
			return err
		}
		if firstRun != nil {
			fullSchedule.SetNextRun(*firstRun)
		} else if fullRecurrencePicked {
			// The enterprise user did not indicate preference when to run full backups,
			// and we picked the schedule ourselves.
			// Run full backup immediately so that we do not wind up waiting for a long
			// time before the first full backup runs.  Without full backup, we can't
			// execute incremental.
			fullSchedule.SetNextRun(env.Now())
		}
		// Create the schedule (we need its ID to create incremental below).
		if err := fullSchedule.Create(ctx, p.ExecCfg().InternalExecutor, txn); err != nil {
			return err
		}
		backupNode.ScheduleID = fullSchedule.ScheduleID()
		err = emitSchedule(fullSchedule, fullScheduleName, fullRecurrence, resultsCh, backupNode)
		if err != nil {
			return err
		}
		// If needed, create incremental.
		if incRecurrence != nil {
			backupNode.AppendToLatest = true
			var incSchedule *jobs.ScheduledJob
			if incSchedule, err = makeBackupSchedule(
				ctx, p, env, fullScheduleName+": CHANGES", incRecurrence,
				scheduleOptions, backupNode, resultsCh, txn,
			); err != nil {
				return err
			}
			if err := incSchedule.Create(ctx, p.ExecCfg().InternalExecutor, txn); err != nil {
				return err
			}
			fullJob, err := GetScheduleJob(ctx, p.ExecCfg().InternalExecutor, env, txn, fullSchedule.ScheduleID())
			if err != nil {
				return err
			}
			err = emitSchedule(incSchedule, fullScheduleName+": CHANGES", incRecurrence, resultsCh, backupNode)
			if err != nil {
				return err
			}
			fullArgs := &ScheduledBackupExecutionArgs{}
			if err := pbtypes.UnmarshalAny(fullJob.ExecutionArgs().Args, fullArgs); err != nil {
				return err
			}
			fullArgs.ParentScheduleId = incSchedule.ScheduleID()
			any, err := pbtypes.MarshalAny(fullArgs)
			exArg := jobspb.ExecutionArguments{Args: any}
			fullJob.SetExecutionArgs(exArg)
			err = fullJob.Update(ctx, p.ExecCfg().InternalExecutor, txn)
			if err != nil {
				return err
			}

		}

		return nil
	})

}

func makeBackupSchedule(
	ctx context.Context,
	p sql.PlanHookState,
	env scheduledjobs.JobSchedulerEnv,
	name string,
	recurrence *scheduleRecurrence,
	scheduleOpts map[string]string,
	backupNode *tree.Dump,
	resultsCh chan<- tree.Datums,
	txn *client.Txn,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleName(name)

	// Prepare arguments for scheduled backup execution.
	args := &ScheduledBackupExecutionArgs{}
	if backupNode.AppendToLatest {
		args.BackupType = ScheduledBackupExecutionArgs_INCREMENTAL
		args.ParentScheduleId = backupNode.ScheduleID
	} else {
		args.BackupType = ScheduledBackupExecutionArgs_FULL
	}

	if err := sj.SetSchedule(recurrence.cron); err != nil {
		return nil, err
	}

	if err := setScheduleOptions(&p.ExtendedEvalContext().EvalContext, scheduleOpts, sj); err != nil {
		return nil, err
	}

	// TODO(yevgeniy): Validate backup schedule:
	//  * Verify targets exist.  Provide a way for user to override this via option.
	//  * Verify destination paths sane (i.e. valid schema://, etc)

	// We do not set backupNode.AsOf: this is done when the scheduler kicks off the backup.
	// Serialize backup statement and set schedule executor and its args.
	args.BackupStatement = tree.AsString(backupNode)
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	sj.SetDefaultDatabase(backupNode.CurrentPath.Database)
	sj.SetDefaultSearchPath(backupNode.CurrentPath.SearchPath.String())
	sj.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)
	return sj, nil
}

const scheduleBackupOp = "CREATE SCHEDULE FOR DUMP"

// emitSchedule emits schedule details to resultsCh
func emitSchedule(
	sj *jobs.ScheduledJob,
	name string,
	recurrence *scheduleRecurrence,
	resultsCh chan<- tree.Datums,
	backupNode *tree.Dump,
) error {
	var nextRun tree.Datum
	if sj.IsPaused() {
		nextRun = tree.DNull
	} else {
		next := tree.MakeDTimestampTZ(sj.NextRun(), time.Microsecond)
		nextRun = next
	}
	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(sj.ScheduleID())),
		tree.NewDString(name),
		nextRun,
		tree.NewDString(recurrence.Humanize()),
		tree.NewDString(tree.AsString(backupNode)),
	}
	return nil
}

// makeScheduleBackupEval prepares helper scheduledBackupEval struct to assist in evaluation
// of various schedule and backup specific components.
func makeScheduledBackupEval(
	ctx context.Context, p sql.PlanHookState, schedule *tree.ScheduledBackup,
) (*scheduledBackupEval, error) {
	eval := &scheduledBackupEval{ScheduledBackup: schedule}
	var err error
	if schedule.FileFormat != "SST" {
		return nil, errors.Errorf("file format must be sst")
	}
	if schedule.ScheduleName != nil {
		eval.scheduleName, err = p.TypeAsString(schedule.ScheduleName, scheduleBackupOp)
		if err != nil {
			return nil, err
		}
	}

	if schedule.Recurrence == nil {
		// Sanity check: recurrence must be specified.
		return nil, errors.New("RECURRING clause required")
	}

	eval.recurrence, err = p.TypeAsString(schedule.Recurrence, scheduleBackupOp)
	if err != nil {
		return nil, err
	}

	if schedule.FullBackup != nil {
		if schedule.FullBackup.AlwaysFull {
			eval.fullBackupRecurrence = eval.recurrence
			eval.recurrence = nil
		} else {
			eval.fullBackupRecurrence, err = p.TypeAsString(
				schedule.FullBackup.Recurrence, scheduleBackupOp)
			if err != nil {
				return nil, err
			}
		}
	}

	eval.scheduleOpts, err = p.TypeAsStringOpts(
		schedule.ScheduleOptions, scheduledBackupOptionExpectValues)
	if err != nil {
		return nil, err
	}

	eval.destination, err = p.TypeAsStringArray(tree.Exprs(schedule.To), scheduleBackupOp)
	if err != nil {
		return nil, err
	}
	return eval, nil
}

// scheduledBackupHeader is the header for "CREATE SCHEDULE..." statements results.
var scheduledBackupHeader = sqlbase.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "name", Typ: types.String},
	{Name: "next_run", Typ: types.TimestampTZ},
	{Name: "frequency", Typ: types.String},
	{Name: "backup_stmt", Typ: types.String},
}

func createBackupScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	schedule, ok := stmt.(*tree.ScheduledBackup)
	if !ok {
		return nil, nil, nil, false, nil
	}
	eval, err := makeScheduledBackupEval(ctx, p, schedule)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		return doCreateBackupSchedules(ctx, p, eval, resultsCh)
	}
	return fn, scheduledBackupHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook(createBackupScheduleHook)
}
