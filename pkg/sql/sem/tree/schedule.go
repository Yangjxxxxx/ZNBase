package tree

// FullBackupClause describes the frequency of full backups.
type FullBackupClause struct {
	AlwaysFull bool
	Recurrence Expr
}

// ScheduledBackup represents scheduled backup job.
type ScheduledBackup struct {
	FileFormat      string
	ScheduleName    Expr
	Recurrence      Expr
	Options         KVOptions
	FullBackup      *FullBackupClause /* nil implies choose default */
	Targets         TargetList        /* nil implies tree.AllDescriptors coverage */
	To              PartitionedBackup
	ScheduleOptions KVOptions
}

var _ Statement = &ScheduledBackup{}

// Format implements the NodeFormatter interface.
func (node *ScheduledBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SCHEDULE")

	if node.ScheduleName != nil {
		ctx.WriteString(" ")
		node.ScheduleName.Format(ctx)
	}

	ctx.WriteString(" FOR DUMP")
	if &node.Targets != nil {
		ctx.WriteString(" ")
		node.Targets.Format(ctx)
	}

	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.To)

	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}

	ctx.WriteString(" RECURRING ")
	if node.Recurrence == nil {
		ctx.WriteString("NEVER")
	} else {
		node.Recurrence.Format(ctx)
	}

	if node.FullBackup != nil {
		if node.FullBackup.Recurrence != nil {
			ctx.WriteString(" FULL BACKUP ")
			node.FullBackup.Recurrence.Format(ctx)
		} else if node.FullBackup.AlwaysFull {
			ctx.WriteString(" FULL BACKUP ALWAYS")
		}
	}

	if node.ScheduleOptions != nil {
		ctx.WriteString(" WITH EXPERIMENTAL SCHEDULE OPTIONS ")
		node.ScheduleOptions.Format(ctx)
	}
}

// ScheduledJobExecutorType is a type identifying the names of
// the supported scheduled job executors.
type ScheduledJobExecutorType int

const (
	// InvalidExecutor is a placeholder for an invalid executor type.
	InvalidExecutor ScheduledJobExecutorType = iota

	// ScheduledBackupExecutor is an executor responsible for
	// the execution of the scheduled backups.
	ScheduledBackupExecutor
)

var scheduleExecutorInternalNames = map[ScheduledJobExecutorType]string{
	InvalidExecutor:         "unknown-executor",
	ScheduledBackupExecutor: "scheduled-backup-executor",
}

// InternalName returns an internal executor name.
// This name can be used to filter matching schedules.
func (t ScheduledJobExecutorType) InternalName() string {
	return scheduleExecutorInternalNames[t]
}
