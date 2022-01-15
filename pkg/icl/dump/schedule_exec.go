package dump

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/scheduledjobs"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
)

const scheduledBackupExecutorName = "scheduled-backup-executor"

type scheduledBackupExecutor struct{}

var _ jobs.ScheduledJobExecutor = &scheduledBackupExecutor{}

// ExecuteJob implements jobs.ScheduledJobExecutor interface.
func (se *scheduledBackupExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *client.Txn,
) error {
	backupStmt, err := extractBackupStatement(sj)
	if err != nil {
		return err
	}
	args := &ScheduledBackupExecutionArgs{}
	err = pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args)
	if err != nil {
		return err
	}
	// Sanity check: make sure the schedule is not paused so that
	// we don't set end time to 0 (this shouldn't happen since job scheduler
	// ignores paused schedules).
	if sj.IsPaused() {
		return errors.New("scheduled unexpectedly paused")
	}

	// Set endTime (AsOf) to be the time this schedule was supposed to have run.
	endTime := tree.MakeDTimestampTZ(sj.ScheduledRunTime(), time.Microsecond)
	backupStmt.AsOf = tree.AsOfClause{Expr: endTime}

	if knobs, ok := cfg.TestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.OverrideAsOfClause != nil {
			knobs.OverrideAsOfClause(&backupStmt.AsOf)
		}
	}

	// Invoke backup plan hook.
	// TODO(yevgeniy): Invoke backup as the owner of the schedule.
	hook, cleanup := cfg.PlanHookMaker("exec-backup", txn, security.RootUser)
	planhook := hook.(sql.PlanHookState)
	planhook.SessionData().Database = sj.ScheduleDefaultDatabase()
	planhook.SessionData().SearchPath = sessiondata.SetSearchPath(planhook.SessionData().SearchPath, []string{sj.ScheduleDefaultSearchPath()})
	defer cleanup()
	newFile := backupStmt.File.(*tree.StrVal).RawString()
	newDest := strings.Join([]string{newFile, endTime.String()}, string(os.PathSeparator))

	//当前为全量备份时，应该更新当前最新的全量备份路径
	{
		if !backupStmt.AppendToLatest {
			args.PathList = append(args.PathList, newDest)
			if args.ParentScheduleId != 0 {
				incScheduleJob, err := GetScheduleJob(ctx, cfg.InternalExecutor, env, txn, args.ParentScheduleId)
				if err != nil {
					return err
				}
				incArgs := &ScheduledBackupExecutionArgs{}
				if err := pbtypes.UnmarshalAny(incScheduleJob.ExecutionArgs().Args, incArgs); err != nil {
					return err
				}
				incArgs.PathList = make([]string, 0)
				any, err := pbtypes.MarshalAny(incArgs)
				if err != nil {
					return err
				}
				exArg := jobspb.ExecutionArguments{Args: any}
				incScheduleJob.SetExecutionArgs(exArg)
				err = incScheduleJob.Update(ctx, cfg.InternalExecutor, txn)
				if err != nil {
					return err
				}
			}
		}
	}
	if backupStmt.AppendToLatest {
		//createdTime := sj.ScheduleCreatedTime()
		//tz := tree.DTimestampTZ{Time: createdTime}
		//scheduleName := strings.TrimRight(sj.ScheduleName(), ": CHANGES")
		fullBackup, err := GetScheduleJob(ctx, cfg.InternalExecutor, env, txn, args.ParentScheduleId)
		if err != nil {
			return err
		}
		argsFull, err := UnmarshalScheduledBackupExecutionArgs(fullBackup)
		if err != nil {
			return err
		}
		//因为当全量更新增量JOB的信息时，处于同步，保证全量与增量JOB的一致性
		incBackup, err := GetScheduleJob(ctx, cfg.InternalExecutor, env, txn, sj.ScheduleID())
		if err != nil {
			return err
		}
		argsInc, err := UnmarshalScheduledBackupExecutionArgs(incBackup)
		if err != nil {
			return err
		}
		//该增量备份的全量备份无路径，返回error
		if len(argsFull.PathList) == 0 {
			return errors.Errorf("full backup path err")
		}
		destBase := argsFull.PathList[len(argsFull.PathList)-1]
		var incPaths tree.Exprs
		incPaths = append(incPaths, tree.NewDString(destBase))
		for _, path := range argsInc.PathList {
			incPaths = append(incPaths, tree.NewDString(path))
		}
		backupStmt.IncrementalFrom = incPaths
		newDest = strings.Join([]string{destBase, "incremental", endTime.String()}, string(os.PathSeparator))
		args.PathList = append(argsInc.PathList, newDest)
	}
	backupStmt.File = tree.NewDString(newDest)
	planBackup, _, _, _, err := dumpPlanHook(ctx, backupStmt, planhook)
	if err != nil {
		return errors.Wrapf(err, "backup eval: %q", tree.AsString(backupStmt))
	}
	if planBackup == nil {
		return errors.Newf("backup eval: %q", tree.AsString(backupStmt))
	}

	//if len(cols) != len(utilicl.DetachedJobExecutionResultHeader) {
	//	return errors.Newf("unexpected result columns")
	//}

	resultCh := make(chan tree.Datums) // No need to close
	g := ctxgroup.WithContext(ctx)

	g.GoCtx(func(ctx context.Context) error {
		select {
		case <-resultCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		if err := planBackup(ctx, nil, resultCh); err != nil {
			return errors.Wrapf(err, "backup planning error: %q", tree.AsString(backupStmt))
		}
		any, err := pbtypes.MarshalAny(args)
		if err != nil {
			return err
		}
		arguments := jobspb.ExecutionArguments{Args: any}
		sj.SetExecutionArgs(arguments)
		err = sj.Update(ctx, cfg.InternalExecutor, txn)
		if err != nil {
			return err
		}
		return nil
	})

	return g.Wait()
}

// UnmarshalScheduledBackupExecutionArgs unmarshals Scheduled Backup Execution Args
func UnmarshalScheduledBackupExecutionArgs(
	fullBackup *jobs.ScheduledJob,
) (*ScheduledBackupExecutionArgs, error) {
	argsFull := &ScheduledBackupExecutionArgs{}
	err := pbtypes.UnmarshalAny(fullBackup.ExecutionArgs().Args, argsFull)
	if err != nil {
		return nil, err
	}
	return argsFull, nil
}

// GetScheduleJob gets schedule job
func GetScheduleJob(
	ctx context.Context,
	ex sqlutil.InternalExecutor,
	env scheduledjobs.JobSchedulerEnv,
	txn *client.Txn,
	scheduleID int64,
) (*jobs.ScheduledJob, error) {
	row, cols, err := ex.QueryWithCols(ctx, "sched-query", txn,
		//fmt.Sprintf("SELECT * FROM %s WHERE created = %v AND schedule_name = '%s'",
		fmt.Sprintf("SELECT * FROM %s WHERE schedule_id = %d ",
			env.ScheduledJobsTableName(), scheduleID))
	if err != nil || len(row) == 0 {
		return nil, errors.Errorf("can't find schedule job")
	}
	fullBackup := jobs.NewScheduledJob(env)
	if err := fullBackup.InitFromDatums(row[0], cols[:]); err != nil {
		return nil, err
	}
	if fullBackup == nil {
		return nil, errors.Errorf("find schedule job error")
	}
	return fullBackup, nil
}

// NotifyJobTermination implements jobs.ScheduledJobExecutor interface.
func (se *scheduledBackupExecutor) NotifyJobTermination(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	md *jobs.JobMetadata,
	sj *jobs.ScheduledJob,
	txn *client.Txn,
) error {
	return errors.New("unimplemented yet")
}

// extractBackupStatement returns tree.Backup node encoded inside scheduled job.
func extractBackupStatement(sj *jobs.ScheduledJob) (*annotatedBackupStatement, error) {
	args := &ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.BackupStatement, false)
	if err != nil {
		return nil, errors.Wrap(err, "parsing backup statement")
	}

	if backupStmt, ok := node.AST.(*tree.Dump); ok {
		if args.BackupType == 1 {
			backupStmt.AppendToLatest = true
		}
		return &annotatedBackupStatement{
			Dump: backupStmt,
			CreatedByInfo: &jobs.CreatedByInfo{
				Name: jobs.CreatedByScheduledJobs,
				ID:   sj.ScheduleID(),
			},
		}, nil
	}

	return nil, errors.Newf("unexpect node type %T", node)
}

func init() {
	jobs.RegisterScheduledJobExecutorFactory(
		scheduledBackupExecutorName,
		func() (jobs.ScheduledJobExecutor, error) {
			return &scheduledBackupExecutor{}, nil
		})
}
