package jobs

import (
	"context"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/scheduledjobs"
)

// InlineExecutorName is the name associated with scheduled job executor which
// runs jobs "inline" -- that is, it doesn't spawn external system.job to do its work.
const InlineExecutorName = "inline"

// inlineScheduledJobExecutor implements ScheduledJobExecutor interface.
// This executor runs SQL statement "inline" -- that is, it executes statement
// directly under transaction.
type inlineScheduledJobExecutor struct{}

var _ ScheduledJobExecutor = &inlineScheduledJobExecutor{}

const retryFailedJobAfter = time.Minute

// ExecuteJob implements ScheduledJobExecutor interface.
func (e *inlineScheduledJobExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	_ scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	txn *client.Txn,
) error {
	sqlArgs := &jobspb.SqlStatementExecutionArg{}

	if err := types.UnmarshalAny(schedule.ExecutionArgs().Args, sqlArgs); err != nil {
		return errors.Wrapf(err, "expected SqlStatementExecutionArg")
	}

	// TODO(yevgeniy): this is too simplistic.  It would be nice
	// to capture execution traces, or some similar debug information and save that.
	// Also, performing this under the same transaction as the scan loop is not ideal
	// since a single failure would result in rollback for all of the changes.
	_, err := cfg.InternalExecutor.Exec(ctx, "inline-exec", txn,
		sqlArgs.Statement,
	)

	if err != nil {
		return err
	}

	// TODO(yevgeniy): Log execution result
	return nil
}

// NotifyJobTermination implements ScheduledJobExecutor interface.
func (e *inlineScheduledJobExecutor) NotifyJobTermination(
	_ context.Context,
	_ *scheduledjobs.JobExecutionConfig,
	_ scheduledjobs.JobSchedulerEnv,
	md *JobMetadata,
	schedule *ScheduledJob,
	_ *client.Txn,
) error {
	// For now, only interested in failed status.
	if md.Status == StatusFailed {
		DefaultHandleFailedRun(schedule, md.ID, nil)
	}
	return nil
}

func init() {
	RegisterScheduledJobExecutorFactory(
		InlineExecutorName,
		func() (ScheduledJobExecutor, error) {
			return &inlineScheduledJobExecutor{}, nil
		})
}
