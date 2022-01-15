// Copyright 2016  The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package sql

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// EventLogType represents an event type that can be recorded in the event log.
type EventLogType string

// NOTE: When you add a new event type here. Please manually add it to
// pkg/ui/src/util/eventTypes.ts so that it will be recognized in the UI.
const (
	// EventLogCreateDatabase is recorded when a database is created.
	EventLogCreateDatabase EventLogType = "create_database"
	// EventLogDropDatabase is recorded when a database is dropped.
	EventLogDropDatabase EventLogType = "drop_database"
	// EventLogCreateSchema is recorded when a schema is created.
	EventLogCreateSchema EventLogType = "create_schema"
	// EventLogAlterSchema is recorded when a schema is altered.
	EventLogAlterSchema EventLogType = "alter_schema"
	// EventLogDropSchema is recorded when a schema is dropped.
	EventLogDropSchema EventLogType = "drop_schema"

	EventLogRenameDatabase EventLogType = "rename_database"

	// EventLogCreateTable is recorded when a table is created.
	EventLogCreateTable EventLogType = "create_table"
	EventLogRenameTable EventLogType = "rename_table"
	// EventLogDropTable is recorded when a table is dropped.
	EventLogDropTable EventLogType = "drop_table"
	// EventLogTruncateTable is recorded when a table is truncated.
	EventLogTruncateTable EventLogType = "truncate"
	// EventLogAlterTable is recorded when a table is altered.
	EventLogAlterTable EventLogType = "alter_table"
	// EventLogCommentOnColumn is recorded when a column is commented.
	EventLogCommentOnColumn EventLogType = "comment_on_column"
	// EventLogCommentOnTable is recorded when a table is commented.
	EventLogCommentOnDatabase EventLogType = "comment_on_database"
	// EventLogCommentOnTable is recorded when a table is commented.
	EventLogCommentOnTable EventLogType = "comment_on_table"
	// EventLogCommentOnIndex is recorded when a index is commented.
	EventLogCommentOnIndex EventLogType = "comment_on_index"
	// EventLogCommentOnConstraint is recorded when a constraint is commented.
	EventLogCommentOnConstraint EventLogType = "comment_on_constraint"
	// EventLogCreateIndex is recorded when an index is created.
	EventLogCreateIndex EventLogType = "create_index"
	EventLogRenameIndex EventLogType = "rename_index"
	// EventLogDropIndex is recorded when an index is dropped.
	EventLogDropIndex EventLogType = "drop_index"
	EventLogJoinIndex EventLogType = "join_index"
	// EventLogAlterIndex is recorded when an index is altered.
	EventLogAlterIndex EventLogType = "alter_index"

	// EventLogCreateView is recorded when a view is created.
	EventLogCreateView EventLogType = "create_view"
	// EventLogDropView is recorded when a view is dropped.
	EventLogDropView EventLogType = "drop_view"
	// EventLogAlterView is recorded when a view is altered.
	EventLogAlterView EventLogType = "alter_view"

	// EventLogCreateSequence is recorded when a sequence is created.
	EventLogCreateSequence EventLogType = "create_sequence"
	// EventLogDropSequence is recorded when a sequence is dropped.
	EventLogDropSequence   EventLogType = "drop_sequence"
	EventLogSelectSequence EventLogType = "select_sequence"
	// EventLogAlterSequence is recorded when a sequence is altered.
	EventLogAlterSequence EventLogType = "alter_sequence"

	// EventLogReverseSchemaChange is recorded when an in-progress schema change
	// encounters a problem and is reversed.
	EventLogReverseSchemaChange EventLogType = "reverse_schema_change"
	// EventLogFinishSchemaChange is recorded when a previously initiated schema
	// change has completed.
	EventLogFinishSchemaChange EventLogType = "finish_schema_change"
	// EventLogFinishSchemaRollback is recorded when a previously
	// initiated schema change rollback has completed.
	EventLogFinishSchemaRollback EventLogType = "finish_schema_change_rollback"

	// EventLogNodeJoin is recorded when a node joins the cluster.
	EventLogNodeJoin EventLogType = "node_join"
	// EventLogNodeRestart is recorded when an existing node rejoins the cluster
	// after being offline.
	EventLogNodeRestart EventLogType = "node_restart"
	// EventLogNodeDecommissioned is recorded when a node is marked as
	// decommissioning.
	EventLogNodeDecommissioned EventLogType = "node_decommissioned"
	// EventLogNodeRecommissioned is recorded when a decommissioned node is
	// recommissioned.
	EventLogNodeRecommissioned EventLogType = "node_recommissioned"
	EventLogNodeQuit           EventLogType = "node_quit"

	// EventLogSetClusterSetting is recorded when a cluster setting is changed.
	EventLogSetClusterSetting EventLogType = "set_cluster_setting"

	// EventLogSetZoneConfig is recorded when a zone config is changed.
	EventLogSetZoneConfig EventLogType = "set_zone_config"

	// EventLogRemoveZoneConfig is recorded when a zone config is removed.
	EventLogRemoveZoneConfig EventLogType = "remove_zone_config"

	// EventLogCreateStatistics is recorded when statistics are collected for a
	// table.
	EventLogCreateStatistics EventLogType = "create_statistics"

	//	EventSQLLenOverflow is recorded when length of sql is overflow
	EventSQLLenOverflow EventLogType = "sql_length_overflow"
	//	EventSQLInjection is recorded when sql injections is detected
	EventSQLInjection EventLogType = "sql_inject"

	//	EventUserLogin is recorded when new client connected successfully
	EventLogUserLogin  EventLogType = "user_login"
	EventLogUserLogout EventLogType = "user_logout"

	EventLogCancelQueries EventLogType = "cancel_queries"
	EventLogCancelSession EventLogType = "cancel_sessions"
	EventLogCreateUser    EventLogType = "create_user"
	EventLogAlterUser     EventLogType = "alter_user_set_password"
	EventLogDropUser      EventLogType = "drop_user"
	EventLogDelay         EventLogType = "delay"
	EventLogDistinct      EventLogType = "distinct"
	EventLogDelete        EventLogType = "delete"
	EventLogDeleteRange   EventLogType = "delete_range"
	EventLogInsert        EventLogType = "insert"
	EventLogJoin          EventLogType = "join"
	EventLogLimit         EventLogType = "limit"
	EventLogUpdate        EventLogType = "update"
	EventLogSort          EventLogType = "sort"
	EventLogTruncate      EventLogType = "truncate"
	EventLogScan          EventLogType = "scan"
	EventLogExplainPlan   EventLogType = "explain_plan"
	EventLogProjectSet    EventLogType = "project_set"
	EventLogHookFn        EventLogType = "hook_fn"
	EventLogRender        EventLogType = "render"
	EventLogUnion         EventLogType = "union"
	EventLogValue         EventLogType = "value"
	EventLogGroup         EventLogType = "group"
	EventLogRenameColumn  EventLogType = "rename_column"
	EventLogRowCount      EventLogType = "row_count"
	EventLogUpsertHelper  EventLogType = "upsert_helper"
	EventLogUpsert        EventLogType = "upsert"

	EventLogWindow     EventLogType = "window"
	EventLogScatter    EventLogType = "scatter"
	EventLogOrdinality EventLogType = "ordinality"
	EventLogSerialize  EventLogType = "serialize"
	EventLogMax1Row    EventLogType = "max_1_row"
	EventLogRelocate   EventLogType = "relocate"
	EventLogTrace      EventLogType = "trace"
	EventLogSplit      EventLogType = "split"

	EventLogShowTrace            EventLogType = "show_trace"
	EventLogFingerprints         EventLogType = "finger_prints"
	EventLogUnary                EventLogType = "unary"
	EventLogFilter               EventLogType = "filter"
	EventLogExplainDistSQL       EventLogType = "explain_dist_sql"
	EventLogVirtualTable         EventLogType = "virtual_table"
	EventLogZero                 EventLogType = "zero"
	EventLogAlterUserSetPassword EventLogType = "alter_user_set_password"
	EventLogApplyJoin            EventLogType = "apply_join"
	EventLogControlJobs          EventLogType = "control_jobs"
	EventLogLookupJoin           EventLogType = "lookup_join"
	EventLogZigzagJoin           EventLogType = "zigzag_join"
	EventLogSetVar               EventLogType = "set_var"
	EventLogScrub                EventLogType = "scrub"
	EventLogShowZoneConfig       EventLogType = "show_zone_config"
	EventLogSpool                EventLogType = "spool"
	EventLogShowTraceReplica     EventLogType = "show_trace_replica"
	EventLogRowSourceToPlan      EventLogType = "row_source_to_plan"
	EventLogClusterInit          EventLogType = "cluster_init"
	EventLogGrantPrivileges      EventLogType = "grant"
	EventLogRevokePrivileges     EventLogType = "revoke"
	EventLogCreateRole           EventLogType = "create_role"
	EventLogDropRole             EventLogType = "drop_role"
	EventLogDump                 EventLogType = "dump"
	EventLogRestore              EventLogType = "load_restore"
	EventLogCreateFunction       EventLogType = "create_function"
	EventLogCreateProcedure      EventLogType = "create_procedure"
	EventLogDropFunction         EventLogType = "drop_function"
	EventLogDropProcedure        EventLogType = "drop_procedure"
	EventLogRenameFunction       EventLogType = "alter_function_name"
	EventLogRenameProcedure      EventLogType = "alter_procedure_name"
	EventLogCallFunction         EventLogType = "call_function"
)

// EventLogSetClusterSettingDetail is the json details for a settings change.
type EventLogSetClusterSettingDetail struct {
	SettingName string
	Value       string
	User        string
}

// An EventLogger exposes methods used to record events to the event table.
type EventLogger struct {
	*InternalExecutor
}

// MakeEventLogger constructs a new EventLogger.
func MakeEventLogger(execCfg *ExecutorConfig) EventLogger {
	return EventLogger{InternalExecutor: execCfg.InternalExecutor}
}

// InsertEventRecord inserts a single event into the event log as part of the
// provided transaction.
func (ev EventLogger) InsertEventRecord(
	ctx context.Context,
	time time.Time,
	txn *client.Txn,
	eventType string,
	targetID, reportingID int32,
	info interface{},
) error {
	// Record event record insertion in local log output.
	txn.AddCommitTrigger(func(ctx context.Context) {
		log.Infof(
			ctx, "Event: %q, target: %d, info: %+v",
			eventType,
			targetID,
			info,
		)
	})

	const insertEventTableStmt = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "targetID", "reportingID", info
)
VALUES(
  $1, $2, $3, $4, $5
)
`
	args := []interface{}{
		time,
		eventType,
		targetID,
		reportingID,
		nil, // info
	}

	if info != nil {
		infoBytes, err := json.Marshal(info)
		if err != nil {
			return err
		}
		args[4] = string(infoBytes)
	}

	rows, err := ev.Exec(ctx, "log-event", txn, insertEventTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}
