package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/scheduledjobs"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

type controlSchedulesNode struct {
	rows    planNode
	command tree.ScheduleCommand
	numRows int
}

func (p *planner) ControlScheduleJobs(
	ctx context.Context, n *tree.ControlSchedules,
) (planNode, error) {
	rows, err := p.newPlan(ctx, n.Schedules, []types.T{types.Int})
	if err != nil {
		return nil, err
	}
	cols := planColumns(rows)
	if len(cols) != 1 {
		return nil, errors.Errorf("%s JOBS expects a single column source, got %d columns",
			n.Command.String(), len(cols))
	}
	if !(cols[0].Typ.Equivalent(types.Int) || cols[0].Typ.Equivalent(types.Int2) || cols[0].Typ.Equivalent(types.Int4)) {
		return nil, errors.Errorf("%s JOBS requires int values, not type %s",
			n.Command.String(), cols[0].Typ)
	}

	return &controlSchedulesNode{
		rows:    rows,
		command: n.Command,
	}, nil
}

// FastPathResults implements the planNodeFastPath interface.
func (n *controlSchedulesNode) FastPathResults() (int, bool) {
	return n.numRows, true
}

// jobSchedulerEnv returns JobSchedulerEnv.
func jobSchedulerEnv(params runParams) scheduledjobs.JobSchedulerEnv {
	if knobs, ok := params.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			return knobs.JobSchedulerEnv
		}
	}
	return scheduledjobs.ProdJobSchedulerEnv
}

// loadSchedule loads schedule information.
func loadSchedule(params runParams, scheduleID tree.Datum) (*jobs.ScheduledJob, error) {
	env := jobSchedulerEnv(params)
	schedule := jobs.NewScheduledJob(env)

	// Load schedule expression.  This is needed for resume command, but we
	// also use this query to check for the schedule existence.
	datums, cols, err := params.ExecCfg().InternalExecutor.QueryWithCols(
		params.ctx,
		"load-schedule",
		params.EvalContext().Txn,
		fmt.Sprintf(
			"SELECT schedule_id, schedule_expr FROM %s WHERE schedule_id = $1",
			env.ScheduledJobsTableName(),
		),
		scheduleID)
	if err != nil {
		return nil, err
	}

	// Not an error if schedule does not exist.
	if len(datums) != 1 {
		return nil, nil
	}

	if err := schedule.InitFromDatums(datums[0], cols); err != nil {
		return nil, err
	}
	return schedule, nil
}

// updateSchedule executes update for the schedule.
func updateSchedule(params runParams, schedule *jobs.ScheduledJob) error {
	return schedule.Update(
		params.ctx,
		params.ExecCfg().InternalExecutor,
		params.EvalContext().Txn,
	)
}

// deleteSchedule deletes specified schedule.
func deleteSchedule(params runParams, scheduleID int64) error {
	env := jobSchedulerEnv(params)
	_, err := params.ExecCfg().InternalExecutor.Exec(
		params.ctx,
		"delete-schedule",
		params.EvalContext().Txn,
		fmt.Sprintf(
			"DELETE FROM %s WHERE schedule_id = $1",
			env.ScheduledJobsTableName(),
		),
		scheduleID,
	)
	return err
}

// startExec implements planNode interface.
func (n *controlSchedulesNode) startExec(params runParams) error {
	for {
		ok, err := n.rows.Next(params)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		schedule, err := loadSchedule(params, n.rows.Values()[0])
		if err != nil {
			return err
		}

		if schedule == nil {
			continue // not an error if schedule does not exist
		}

		switch n.command {
		case tree.PauseSchedule:
			schedule.Pause("operator paused")
			err = updateSchedule(params, schedule)
		case tree.ResumeSchedule:
			err = schedule.ScheduleNextRun()
			if err == nil {
				err = updateSchedule(params, schedule)
			}
		case tree.DropSchedule:
			err = deleteSchedule(params, schedule.ScheduleID())
		default:
			err = errors.AssertionFailedf("unhandled command %s", n.command)
		}

		if err != nil {
			return err
		}
		n.numRows++
	}

	return nil
}

// Next implements planNode interface.
func (*controlSchedulesNode) Next(runParams) (bool, error) { return false, nil }

// Values implements planNode interface.
func (*controlSchedulesNode) Values() tree.Datums { return nil }

// Close implements planNode interface.
func (n *controlSchedulesNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
