package event

import (
	"context"
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/events"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// AuditEvent map event to metric and with logger for table write
type AuditEvent struct {
	ctx         context.Context                     // used by log
	db          *client.DB                          // used to wrap operation without transaction
	auditLogger *log.SecondaryLogger                // used to log data to audit log file
	eventLogger sql.EventLogger                     // used to log events to table system.eventlog
	eventMetric map[sql.EventLogType][]AuditMetrics // map for event and metrics for it
	auditEnable map[sql.EventLogType]bool           // map for event and audit status, if value is true, log audit and metric it
}

// InitEvents init audit event
func InitEvents(
	ctx context.Context, mail *mail.Server, execCfg *sql.ExecutorConfig, registry *metric.Registry,
) *AuditEvent {

	// get new instance
	auditEvent := &AuditEvent{
		ctx:         ctx,
		db:          execCfg.DB,
		auditLogger: execCfg.AuditLogger,
		eventLogger: sql.MakeEventLogger(execCfg),
		eventMetric: map[sql.EventLogType][]AuditMetrics{
			sql.EventLogCreateDatabase: {
				events.NewCreateDatabaseMetric(ctx, mail, execCfg.InternalExecutor),
			},
			/*sql.EventLogRenameIndex:       nil,
			sql.EventLogCommentOnColumn:   nil,
			sql.EventLogSetClusterSetting: nil,*/
			sql.EventLogAlterSequence: {
				events.NewAlterSequenceMetric(ctx, mail),
			},
			sql.EventLogAlterTable: {
				events.NewAlterTableMetric(ctx, mail),
			},
			sql.EventLogCreateIndex: {
				events.NewCreateIndexMetric(ctx, mail, execCfg.InternalExecutor),
			},
			sql.EventLogCreateSequence: {
				events.NewCreateSequenceMetric(ctx, mail, execCfg.InternalExecutor),
			},
			sql.EventLogCreateView: {
				events.NewCreateViewMetric(ctx, mail, execCfg.InternalExecutor),
			},
			sql.EventLogDropDatabase: {
				events.NewDropDatabaseMetric(ctx, mail),
			},
			sql.EventLogDropIndex: {
				events.NewDropIndexMetric(ctx, mail),
			},
			sql.EventLogDropSequence: {
				events.NewDropSequenceMetric(ctx, mail),
			},
			sql.EventLogRestore: {
				events.NewEventRestoreMetric(ctx, mail),
			},
			sql.EventLogDropTable: {
				events.NewDropTableMetric(ctx, mail),
			},
			sql.EventLogDropView: {
				events.NewDropViewMetric(ctx, mail),
			},
			sql.EventLogNodeDecommissioned: {
				events.NewNodeDecommissionedMetric(ctx, mail),
			},
			sql.EventLogNodeJoin: {
				events.NewNodeJoinMetric(ctx, mail),
			},
			sql.EventLogNodeRestart: {
				events.NewNodeRestartMetric(ctx, mail),
			},
			sql.EventLogRenameDatabase: {
				events.NewRenameDatabaseMetric(ctx, mail),
			},
			sql.EventLogRenameIndex: {
				events.NewRenameIndexMetric(ctx, mail),
			},
			sql.EventLogRenameTable: {
				events.NewRenameTableMetric(ctx, mail),
			},
			sql.EventLogReverseSchemaChange: {
				events.NewReverseSchemaChangeMetric(ctx, mail),
			},
			sql.EventLogCreateTable: {
				events.NewCreateTableMetric(ctx, mail, execCfg.InternalExecutor),
			},
			sql.EventLogSetClusterSetting: {
				events.NewSetClusterSettingMetric(ctx, mail),
			},
			sql.EventLogSetZoneConfig: {
				events.NewSetZoneConfigMetric(ctx, mail),
			},
			sql.EventLogRemoveZoneConfig: {
				events.NewRmZoneCfgMetric(ctx, mail),
			},
			sql.EventSQLLenOverflow: {
				events.NewSQLLenOverflowMetric(ctx, mail, execCfg.InternalExecutor),
			},
			sql.EventSQLInjection: {
				events.NewSQLInjectMetric(ctx, mail),
			},
			sql.EventLogUserLogin: {
				events.NewUserLoginMetric(ctx, mail),
			},
			sql.EventLogUserLogout: {
				events.NewUserLogoutMetric(ctx, mail),
			},
			sql.EventLogCreateFunction: {
				events.NewCreateFunctionMetric(ctx, mail),
			},
			sql.EventLogCreateProcedure: {
				events.NewCreateProcedureMetric(ctx, mail),
			},
			sql.EventLogDropFunction: {
				events.NewDropFunctionMetric(ctx, mail),
			},
			sql.EventLogDropProcedure: {
				events.NewDropProcedureMetric(ctx, mail),
			},
			sql.EventLogRenameFunction: {
				events.NewRenameFunctionMetric(ctx, mail),
			},
			sql.EventLogRenameProcedure: {
				events.NewRenameProcedureMetric(ctx, mail),
			},
			sql.EventLogCallFunction: {
				events.NewCallFunctionMetric(ctx, mail),
			},
			sql.EventLogTruncateTable: {
				events.NewTruncateTableMetric(ctx, mail),
			},
			sql.EventLogSplit: {
				events.NewSplitMetric(ctx, mail),
			},
			sql.EventLogClusterInit: {
				events.NewClusterInitMetric(ctx, mail),
			},
			sql.EventLogCreateUser: {
				events.NewCreateUserMetric(ctx, mail, execCfg.InternalExecutor),
			},
			sql.EventLogAlterUser: {
				events.NewAlterUserMetric(ctx, mail),
			},
			sql.EventLogDropUser: {
				events.NewDropUserMetric(ctx, mail),
			},
			sql.EventLogCancelSession: {
				events.NewCancelSessionMetric(ctx, mail),
			},
			sql.EventLogCancelQueries: {
				events.NewCancelQueriesMetric(ctx, mail),
			},
			sql.EventLogControlJobs: {
				events.NewControlJobsMetric(ctx, mail),
			},
			sql.EventLogSetVar: {
				events.NewSetSessionVarMetric(ctx, mail),
			},
			sql.EventLogGrantPrivileges: {
				events.NewGrantPrivilegesMetric(ctx, mail),
			},
			sql.EventLogRevokePrivileges: {
				events.NewRevokePrivilegesMetric(ctx, mail),
			},
			sql.EventLogCreateRole: {
				events.NewCreateRoleMetric(ctx, mail, execCfg.InternalExecutor),
			},
			sql.EventLogDropRole: {
				events.NewDropRoleMetric(ctx, mail),
			},
			sql.EventLogNodeRecommissioned: {
				events.NewNodeRecommissionedMetric(ctx, mail),
			},
			sql.EventLogCreateSchema: {
				events.NewCreateSchemaMetric(ctx, mail, execCfg.InternalExecutor),
			},
			sql.EventLogDropSchema: {
				events.NewDropSchemaMetric(ctx, mail),
			},
			sql.EventLogAlterSchema: {
				events.NewAlterSchemaMetric(ctx, mail),
			},
			sql.EventLogDump: {
				events.NewEventBackupMetric(ctx, mail),
			},
		},
	}

	//	init audit enable map
	auditEvent.auditEnable = make(map[sql.EventLogType]bool, len(auditEvent.eventMetric))
	for k := range auditEvent.eventMetric {
		auditEvent.auditEnable[k] = true
	}

	// register all event metric to metric.Registry
	if registry != nil {
		for _, ms := range auditEvent.eventMetric {
			for _, m := range ms {
				registry.AddMetricStruct(m.RegistMetric())
			}
		}
	}

	return auditEvent
}

//LogAudit : HandleEvent check event, log and metric it
func (ae *AuditEvent) LogAudit(auditInfo *server.AuditInfo) error {

	//	log audit event
	ae.auditLogger.Logf(ae.ctx, "[%s][%s][%s][%s][%d][%s][%s][%s][%d][%d][%s]",
		strings.ToLower(fmt.Sprintf("%+v", auditInfo.ClientInfo)),
		auditInfo.AppName,
		auditInfo.EventTime.Format(TimeFormat),
		auditInfo.EventType,
		auditInfo.TargetInfo.TargetID,
		strings.ToLower(fmt.Sprintf("%+v", auditInfo.TargetInfo.Desc)),
		auditInfo.Opt,
		auditInfo.OptParam,
		auditInfo.OptTime,
		auditInfo.AffectedSize,
		auditInfo.Result,
	)

	//	insert audit info to table
	if err := ae.db.Txn(ae.ctx, func(ctx context.Context, txn *client.Txn) error {
		return ae.eventLogger.InsertEventRecord(
			ctx,
			auditInfo.EventTime,
			txn,
			auditInfo.EventType,
			auditInfo.TargetInfo.TargetID,
			auditInfo.ReportID,
			auditInfo.Info,
		)
	}); err != nil {
		log.Errorf(ae.ctx, "log to db, err:%s", err)
		return err
	}

	// metric event
	if ms, exist := ae.eventMetric[sql.EventLogType(auditInfo.EventType)]; exist && len(ms) > 0 {
		for _, m := range ms {
			m.Metric(auditInfo.Info)
		}
	}

	return nil
}

// IsAudit check if event need to be audit
func (ae *AuditEvent) IsAudit(eventType string) bool {
	return ae.auditEnable[sql.EventLogType(eventType)]
}

// Disable disable audit for all event in list
func (ae *AuditEvent) Disable(eventTypeList string) {

	if len(eventTypeList) > 0 {
		// reset audit enable status
		for k := range ae.eventMetric {
			ae.auditEnable[k] = true
		}

		// disable audit for all event in list
		for _, event := range strings.Split(eventTypeList, ",") {
			if _, ok := ae.eventMetric[sql.EventLogType(event)]; ok {
				ae.auditEnable[sql.EventLogType(event)] = false
				log.Infof(ae.ctx, "disable audit for event:%s", event)
			} else {
				log.Warningf(ae.ctx, "wrong event type, please check. type:%s", event)
			}
		}
	}
}

// GetMetric return metrics for specified event type
func (ae *AuditEvent) GetMetric(eventType sql.EventLogType) []AuditMetrics {
	return ae.eventMetric[eventType]
}
