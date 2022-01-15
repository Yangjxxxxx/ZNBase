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

package sql

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/build"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/delegate"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgnotice"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqltelemetry"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "9.5.0"
	// PgServerVersionNum is the latest version of postgres that we claim to support in the numeric format of "server_version_num".
	PgServerVersionNum = "90500"
)

var casesensitiveEnabledClusterMode = settings.RegisterBoolSetting(
	"sql.defaults.casesensitive.enabled",
	"default value for CaseSensitive mode; CaseInsensitive by default",
	false,
)

type getStringValFn = func(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (string, error)

// sessionVar provides a unified interface for performing operations on
// variables such as the selected database, or desired syntax.
type sessionVar struct {
	// Hidden indicates that the variable should not show up in the output of SHOW ALL.
	Hidden bool

	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(evalCtx *extendedEvalContext) string

	// GetStringVal converts the provided Expr to a string suitable
	// for Set() or RuntimeSet().
	// If this method is not provided,
	//   `getStringVal(evalCtx, varName, values)`
	// will be used instead.
	//
	// The reason why variable sets work in two phases like this is that
	// the Set() method has to operate on strings, because it can be
	// invoked at a point where there is no evalContext yet (e.g.
	// upon session initialization in pgwire).
	GetStringVal getStringValFn

	// Set performs mutations to effect the change desired by SET commands.
	// This method should be provided for variables that can be overridden
	// in pgwire.
	Set func(ctx context.Context, m *sessionDataMutator, val string) error

	// RuntimeSet is like Set except it can only be used in sessions
	// that are already running (i.e. not during session
	// initialization).  Currently only used for transaction_isolation.
	RuntimeSet func(_ context.Context, evalCtx *extendedEvalContext, s string) error

	// GlobalDefault is the string value to use as default for RESET or
	// during session initialization when no default value was provided
	// by the client.
	GlobalDefault func(sv *settings.Values) string
}

func formatBoolAsPostgresSetting(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func parsePostgresBool(s string) (bool, error) {
	s = strings.ToLower(s)
	switch s {
	case "on":
		s = "true"
	case "off":
		s = "false"
	}
	return strconv.ParseBool(s)
}

// varGen is the main definition array for all session variables.
// Note to maintainers: try to keep this sorted in the source code.
var varGen = map[string]sessionVar{
	`parallel_scan`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			v, ok := sessiondata.BoolFormatFromString(s)
			if !ok {
				return newVarValueError(`parallel_scan`, s, "on", "off")
			}
			m.SetParallel(sessiondata.TableReaderSet, v)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.PC[sessiondata.TableReaderSet].ValString
		},
		GlobalDefault: func(st *settings.Values) string {
			if rowexec.TableReaderOpt.Get(st) {
				return "on"
			}
			return "off"
		},
	},
	`parallel_scan_num`: {
		GetStringVal: makeIntGetStringValFn(`parallel_scan_num`),
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(evalCtx.SessionData.PC[sessiondata.TableReaderSet].ParallelNum, 10)
		},
		Set: func(ctx context.Context, m *sessionDataMutator, val string) error {
			i, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return wrapSetVarError("parallel_scan_num", val, "%v", err)
			}
			if i < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set parallel_scan_num to a negative value: %d", i)
			}
			m.SetParallelNum(sessiondata.TableReaderSet, i)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(rowexec.TableReaderParallelNums.Get(sv), 10)
		},
	},
	`parallel_hashjoin`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			v, ok := sessiondata.BoolFormatFromString(s)
			if !ok {
				return newVarValueError(`parallel_hashjoin`, s, "on", "off")
			}
			m.SetParallel(sessiondata.HashJoinerSet, v)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.PC[sessiondata.HashJoinerSet].ValString
		},
		GlobalDefault: func(st *settings.Values) string {
			if rowexec.HashJoinerOpt.Get(st) {
				return "on"
			}
			return "off"
		},
	},
	`parallel_hashjoin_num`: {
		GetStringVal: makeIntGetStringValFn(`parallel_hashjoin_num`),
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(evalCtx.SessionData.PC[sessiondata.HashJoinerSet].ParallelNum, 10)
		},
		Set: func(ctx context.Context, m *sessionDataMutator, val string) error {
			i, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return wrapSetVarError("parallel_hashjoin_num", val, "%v", err)
			}
			if i < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set parallel_hashjoin_num to a negative value: %d", i)
			}
			m.SetParallelNum(sessiondata.HashJoinerSet, i)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(rowexec.HashJoinerParallelNums.Get(sv), 10)
		},
	},
	`parallel_hashagg`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			v, ok := sessiondata.BoolFormatFromString(s)
			if !ok {
				return newVarValueError(`parallel_hashagg`, s, "on", "off")
			}
			m.SetParallel(sessiondata.HashAggSet, v)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.PC[sessiondata.HashAggSet].ValString
		},
		GlobalDefault: func(st *settings.Values) string {
			if rowexec.AggregatorOpt.Get(st) {
				return "on"
			}
			return "off"
		},
	},
	`parallel_hashagg_num`: {
		GetStringVal: makeIntGetStringValFn(`parallel_hashagg_num`),
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(evalCtx.SessionData.PC[sessiondata.HashAggSet].ParallelNum, 10)
		},
		Set: func(ctx context.Context, m *sessionDataMutator, val string) error {
			i, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return wrapSetVarError("parallel_hashagg_num", val, "%v", err)
			}
			if i < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set parallel_hashagg_num to a negative value: %d", i)
			}
			m.SetParallelNum(sessiondata.HashAggSet, i)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(rowexec.AggregatorParallelNums.Get(sv), 10)
		},
	},
	`parallel_sort`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			v, ok := sessiondata.BoolFormatFromString(s)
			if !ok {
				return newVarValueError(`parallel_sort`, s, "on", "off")
			}
			m.SetParallel(sessiondata.SortSet, v)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.PC[sessiondata.SortSet].ValString
		},
		GlobalDefault: func(st *settings.Values) string {
			if rowexec.SorterOpt.Get(st) {
				return "on"
			}
			return "off"
		},
	},
	`parallel_sort_num`: {
		GetStringVal: makeIntGetStringValFn(`parallel_sort_num`),
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(evalCtx.SessionData.PC[sessiondata.SortSet].ParallelNum, 10)
		},
		Set: func(ctx context.Context, m *sessionDataMutator, val string) error {
			i, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return wrapSetVarError("parallel_sort_num", val, "%v", err)
			}
			if i < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set parallel_sort_num to a negative value: %d", i)
			}
			m.SetParallelNum(sessiondata.SortSet, i)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(rowexec.SorterParallelNums.Get(sv), 10)
		},
	},
	// Set by clients to improve query logging.
	// See https://www.postgresql.org/docs/10/static/runtime-config-logging.html#GUC-APPLICATION-NAME
	`application_name`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			m.SetApplicationName(s)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.ApplicationName
		},
		GlobalDefault: func(_ *settings.Values) string { return "" },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	// and https://www.postgresql.org/docs/10/static/datatype-binary.html
	`bytea_output`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			mode, ok := sessiondata.BytesEncodeFormatFromString(s)
			if !ok {
				return newVarValueError(`bytea_output`, s, "hex", "escape", "base64")
			}
			m.SetBytesEncodeFormat(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DataConversion.BytesEncodeFormat.String()
		},
		GlobalDefault: func(sv *settings.Values) string { return sessiondata.BytesEncodeHex.String() },
	},

	`client_min_messages`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			severity, ok := pgnotice.ParseDisplaySeverity(s)
			if !ok {
				return errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidParameterValue,
						"%s is not supported",
						severity,
					),
					"Valid severities are: %s.",
					strings.Join(pgnotice.ValidDisplaySeverities(), ", "),
				)
			}
			m.SetNoticeDisplaySeverity(severity)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.NoticeDisplaySeverity.String()
		},
		GlobalDefault: func(_ *settings.Values) string { return "notice" },
	},

	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	// Also aliased to SET NAMES.
	`client_encoding`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			encoding := builtins.CleanEncodingName(s)
			switch encoding {
			case "utf8", "unicode", "cp65001":
				m.SetClientEncoding(encoding)
				return nil
			case "gbk", "gb18030":
				m.SetClientEncoding(encoding)
				return nil
			case "big5":
				m.SetClientEncoding(encoding)
				return nil
			default:
				return pgerror.Unimplemented("client_encoding "+encoding,
					"unimplemented client encoding: %q", encoding)
			}
		},
		Get:           func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.ClientEncoding },
		GlobalDefault: func(_ *settings.Values) string { return "UTF8" },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	`server_encoding`: makeReadOnlyVar("UTF8"),

	// ZNBaseDB extension.
	`database`: {
		GetStringVal: func(
			ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) (string, error) {
			dbName, err := getStringVal(&evalCtx.EvalContext, `database`, values)
			if err != nil {
				return "", err
			}
			if len(dbName) == 0 && evalCtx.SessionData.SafeUpdates {
				return "", pgerror.NewDangerousStatementErrorf("SET database to empty string")
			}

			var dbDesc *DatabaseDescriptor
			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if dbDesc, err = evalCtx.schemaAccessors.logical.GetDatabaseDesc(ctx, evalCtx.Txn, dbName,
					DatabaseLookupFlags{required: true}); err != nil {
					return "", err
				}
				// check the privilege to visit the database
				user := evalCtx.SessionData.User
				if err := CheckAccessPrivilege(evalCtx.Context, evalCtx.Txn, user, dbDesc); err != nil {
					return "", err
				}
			}
			return dbName, nil
		},
		Set: func(
			ctx context.Context, m *sessionDataMutator, dbName string,
		) error {
			m.SetDatabase(dbName)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			dbName := evalCtx.SessionData.Database
			var dbDesc *DatabaseDescriptor
			var err error
			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if dbDesc, err = evalCtx.schemaAccessors.logical.GetDatabaseDesc(evalCtx.Context,
					evalCtx.Txn, dbName, DatabaseLookupFlags{required: true}); err != nil {
					evalCtx.SessionData.Database = ""
					return ""
				}
				// check the privilege to visit the database
				user := evalCtx.SessionData.User
				if err := CheckAccessPrivilege(evalCtx.Context, evalCtx.Txn, user, dbDesc); err != nil {
					evalCtx.SessionData.Database = ""
					return ""
				}
			}
			return dbName
		},
		GlobalDefault: func(_ *settings.Values) string {
			// The "defaultdb" value is set as session default in the pgwire
			// connection code. The global default is the empty string,
			// which is what internal connections should pick up.
			return ""
		},
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DATESTYLE
	`datestyle`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			s = strings.ToLower(s)
			parts := strings.Split(s, ",")
			if strings.TrimSpace(parts[0]) != "iso" ||
				(len(parts) == 2 && strings.TrimSpace(parts[1]) != "mdy") ||
				len(parts) > 2 {
				return newVarValueError("DateStyle", s, "ISO", "ISO, MDY").SetDetailf(
					"this parameter is currently recognized only for compatibility and has no effect in ZNBaseDB.")
			}
			return nil
		},
		Get:           func(evalCtx *extendedEvalContext) string { return "ISO, MDY" },
		GlobalDefault: func(_ *settings.Values) string { return "ISO, MDY" },
	},
	// Controls the subsequent parsing of a "naked" INT type.
	// TODO(bob): Remove or no-op this in v2.4: https://github.com/znbasedb/znbase/issues/32844
	`default_int_size`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(int64(evalCtx.SessionData.DefaultIntSize), 10)
		},
		GetStringVal: makeIntGetStringValFn("default_int_size"),
		Set: func(ctx context.Context, m *sessionDataMutator, val string) error {
			i, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return wrapSetVarError("default_int_size", val, "%v", err)
			}
			if i != 4 && i != 8 {
				return pgerror.NewError(pgcode.InvalidParameterValue,
					`only 4 or 8 are supported by default_int_size`)
			}
			// Only record when the value has been changed to a non-default
			// value, since we really just want to know how useful int4-mode
			// is. If we were to record counts for size.4 and size.8
			// variables, we'd have to distinguish cases in which a session
			// was opened in int8 mode and switched to int4 mode, versus ones
			// set to int4 by a connection string.
			// TODO(bob): Change to 8 in v2.3: https://github.com/znbasedb/znbase/issues/32534
			if i == 4 {
				telemetry.Inc(sqltelemetry.DefaultIntSize4Counter)
			}
			m.SetDefaultIntSize(int(i))
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(defaultIntSize.Get(sv), 10)
		},
	},
	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION
	`default_transaction_isolation`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			if isolationLevel, ok := util.IsolationLevelMap[strings.ToLower(s)]; ok {
				m.SetDefaultIsolationLevel(tree.IsolationLevel(isolationLevel))

			} else if isolationLevel, ok := util.NoneIsolationLevelMap[strings.ToLower(s)]; ok {
				m.SetDefaultIsolationLevel(tree.IsolationLevel(isolationLevel))
			} else {
				return newVarValueError(`default_transaction_isolation`, s, strings.ToLower(util.IsolationLevelNames[util.UnspecifiedIsolation]))
			}
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			defaultIsolationLevel := evalCtx.SessionData.DefaultIsolationLevel
			return util.IsolationLevelNames[defaultIsolationLevel]
		},
		GlobalDefault: func(sv *settings.Values) string {
			return clusterIsolationLevel.Get(sv)
		},
	},
	// See https://www.postgresql.org/docs/9.3/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-READ-ONLY
	`default_transaction_read_only`: {
		GetStringVal: makeBoolGetStringValFn("default_transaction_read_only"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetDefaultReadOnly(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.DefaultReadOnly)
		},
		GlobalDefault: globalFalse,
	},

	// ZNBaseDB extension.
	`distsql`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.DistSQLExecModeFromString(s)
			if !ok {
				return newVarValueError(`distsql`, s, "on", "off", "auto", "always", "2.0-auto", "2.0-off")
			}
			m.SetDistSQLMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DistSQLMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.DistSQLExecMode(DistSQLClusterExecMode.Get(sv)).String()
		},
	},

	// ZNBaseDB extension.
	`experimental_force_split_at`: {
		GetStringVal: makeBoolGetStringValFn(`experimental_force_split_at`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetForceSplitAt(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ForceSplitAt)
		},
		GlobalDefault: globalFalse,
	},

	`experimental_distsql_planning`: {
		GetStringVal: makeBoolGetStringValFn(`experimental_distsql_planning`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.ExperimentalDistSQLPlanningModeFromString(s)
			if !ok {
				return newVarValueError(`experimental_distsql_planning`, s,
					"off", "on", "always")
			}
			m.SetExperimentalDistSQLPlanning(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.ExperimentalDistSQLPlanningMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.ExperimentalDistSQLPlanningMode(experimentalDistSQLPlanningClusterMode.Get(sv)).String()
		},
	},

	// ZNBaseDB extension.
	`enable_primary_key_changes`: {
		GetStringVal: makeBoolGetStringValFn(`enable_primary_key_changes`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetPrimaryKeyChangesEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.PrimaryKeyChangesEnabled)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(primaryKeyChangesEnabledClusterMode.Get(sv))
		},
	},

	// ZNBaseDB extension.
	`experimental_enable_zigzag_join`: {
		GetStringVal: makeBoolGetStringValFn(`experimental_enable_zigzag_join`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetZigzagJoinEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ZigzagJoinEnabled)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(zigzagJoinClusterMode.Get(sv))
		},
	},

	// ZNBaseDB extension.
	`reorder_joins_limit`: {
		GetStringVal: makeIntGetStringValFn(`reorder_joins_limit`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.NewErrorf(pgcode.InvalidParameterValue,
					"cannot set reorder_joins_limit to a negative value: %d", b)
			}
			m.SetReorderJoinsLimit(int(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(int64(evalCtx.SessionData.ReorderJoinsLimit), 10)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(ReorderJoinsLimitClusterValue.Get(sv), 10)
		},
	},

	// ZNBaseDB extension.
	`experimental_vectorize`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.VectorizeExecModeFromString(s)
			if !ok {
				return newVarValueError(`experimental_vectorize`, s, "off", "on", "always")
			}
			m.SetVectorize(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.VectorizeMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.VectorizeExecMode(
				VectorizeClusterMode.Get(sv)).String()
		},
	},

	// ZNBaseDB extension.
	`vectorize_row_count_threshold`: {
		GetStringVal: makeIntGetStringValFn(`vectorize_row_count_threshold`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set vectorize_row_count_threshold to a negative value: %d", b)
			}
			m.SetVectorizeRowCountThreshold(uint64(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(int64(evalCtx.SessionData.VectorizeRowCountThreshold), 10)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(VectorizeRowCountThresholdClusterValue.Get(sv), 10)
		},
	},

	// ZNBaseDB extension.
	`optimizer`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.OptimizerModeFromString(s)
			if !ok {
				return newVarValueError(`optimizer`, s, "on", "off", "local", "always")
			}
			m.SetOptimizerMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.OptimizerMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.OptimizerMode(
				OptimizerClusterMode.Get(sv)).String()
		},
	},

	// ZNBaseDB extension.
	`enable_implicit_select_for_update`: {
		GetStringVal: makeBoolGetStringValFn(`enable_implicit_select_for_update`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetImplicitSelectForUpdate(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ImplicitSelectForUpdate)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(implicitSelectForUpdateClusterMode.Get(sv))
		},
	},

	`casesensitive`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := sessiondata.ParseBoolVar("casesensitive", s)
			if err != nil {
				return newVarValueError(`casesensitive`, s, "on", "off", "local", "always")
			}
			m.SetCaseSensitive(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.CaseSensitive)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(casesensitiveEnabledClusterMode.Get(sv))
		},
	},

	// ZNBaseDB extension.
	`experimental_serial_normalization`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.SerialNormalizationModeFromString(s)
			if !ok {
				return newVarValueError(`experimental_serial_normalization`, s,
					"rowid", "virtual_sequence", "sql_sequence")
			}
			m.SetSerialNormalizationMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.SerialNormalizationMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.SerialNormalizationMode(
				SerialNormalizationMode.Get(sv)).String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	`extra_float_digits`: {
		GetStringVal: makeIntGetStringValFn(`extra_float_digits`),
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return wrapSetVarError("extra_float_digits", s, "%v", err)
			}
			// Note: this is the range allowed by PostgreSQL.
			// See also the documentation around (DataConversionConfig).GetFloatPrec()
			// in session_data.go.
			if i < -15 || i > 3 {
				return pgerror.NewErrorf(pgcode.InvalidParameterValue,
					`%d is outside the valid range for parameter "extra_float_digits" (-15 .. 3)`, i)
			}
			m.SetExtraFloatDigits(int(i))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return fmt.Sprintf("%d", evalCtx.SessionData.DataConversion.ExtraFloatDigits)
		},
		GlobalDefault: func(sv *settings.Values) string { return "0" },
	},
	// ZNBaseDB extension. See docs on SessionData.ForceSavepointRestart.
	// https://github.com/znbasedb/znbase/issues/30588
	`force_savepoint_restart`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ForceSavepointRestart)
		},
		GetStringVal: makeBoolGetStringValFn("force_savepoint_restart"),
		Set: func(_ context.Context, m *sessionDataMutator, val string) error {
			b, err := parsePostgresBool(val)
			if err != nil {
				return err
			}
			if b {
				telemetry.Inc(sqltelemetry.ForceSavepointRestartCounter)
			}
			m.SetForceSavepointRestart(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},
	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html
	`integer_datetimes`: makeReadOnlyVar("on"),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-INTERVALSTYLE
	`intervalstyle`: makeCompatStringVar(`IntervalStyle`, "postgres"),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-LOC-TIMEOUT
	`lock_timeout`: makeCompatIntVar(`lock_timeout`, 0),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT
	// See also issue #5924.
	`idle_in_transaction_session_timeout`: makeCompatIntVar(`idle_in_transaction_session_timeout`, 0),

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-MAX-INDEX-KEYS
	`max_index_keys`: makeReadOnlyVar("32"),

	// ZNBaseDB extension.
	`node_id`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return fmt.Sprintf("%d", evalCtx.NodeID)
		},
	},

	// ZNBaseDB extension.
	`replicate_tables_in_sync`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetReplicaionSync(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ReplicaionSync)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(replicationSyncClusterMode.Get(sv))
		},
	},

	// TODO(dan): This should also work with SET.
	`results_buffer_size`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(evalCtx.SessionData.ResultsBufferSize, 10)
		},
	},

	// ZNBaseDB extension (inspired by MySQL).
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_sql_safe_updates
	`sql_safe_updates`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.SafeUpdates)
		},
		GetStringVal: makeBoolGetStringValFn("sql_safe_updates"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetSafeUpdates(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// See https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
	// https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	`search_path`: {
		GetStringVal: func(
			_ context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) (string, error) {
			comma := ""
			var buf bytes.Buffer
			for _, v := range values {
				s, err := datumAsString(&evalCtx.EvalContext, "search_path", v)
				if err != nil {
					return "", err
				}
				buf.WriteString(comma)
				buf.WriteString(base64.StdEncoding.EncodeToString([]byte(s)))
				comma = ","
			}
			return buf.String(), nil
		},
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			paths := strings.Split(s, ",")
			for i, p := range paths {
				decodeBytes, err := base64.StdEncoding.DecodeString(p)
				if err != nil {
					return err
				}
				paths[i] = string(decodeBytes)
				err = m.data.SearchPath.CheckTemporarySchema(paths[i])
				if err != nil {
					return err
				}
			}
			m.SetSearchPath(m.data.SearchPath.UpdatePaths(paths))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.SearchPath.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sqlbase.DefaultSearchPath.Base64String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION
	`server_version`: makeReadOnlyVar(PgServerVersion),

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION-NUM
	`server_version_num`: makeReadOnlyVar(PgServerVersionNum),

	// See https://www.postgresql.org/docs/9.4/runtime-config-connection.html
	`ssl_renegotiation_limit`: {
		Hidden:        true,
		GetStringVal:  makeIntGetStringValFn(`ssl_renegotiation_limit`),
		Get:           func(_ *extendedEvalContext) string { return "0" },
		GlobalDefault: func(_ *settings.Values) string { return "0" },
		Set: func(_ context.Context, _ *sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return wrapSetVarError("ssl_renegotiation_limit", s, "%v", err)
			}
			if i != 0 {
				// See pg src/backend/utils/misc/guc.c: non-zero values are not to be supported.
				return newVarValueError("ssl_renegotiation_limit", s, "0")
			}
			return nil
		},
	},

	// ZNBaseDB extension.
	`znbase_version`: makeReadOnlyVar(build.GetInfo().Short()),

	// ZNBaseDB extension.
	// In PG this is a pseudo-function used with SELECT, not SHOW.
	// See https://www.postgresql.org/docs/10/static/functions-info.html
	`session_user`: {
		Get: func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.User },
	},

	// See pg sources src/backend/utils/misc/guc.c. The variable is defined
	// but is hidden from SHOW ALL.
	`session_authorization`: {
		Hidden: true,
		Get:    func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.User },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
	`standard_conforming_strings`: makeCompatBoolVar(`standard_conforming_strings`, true, false /* anyAllowed */),

	// See https://www.postgresql.org/docs/10/static/runtime-config-compatible.html#GUC-SYNCHRONIZE-SEQSCANS
	// The default in pg is "on" but the behavior in ZNBaseDB is "off". As this does not affect
	// results received by clients, we accept both values.
	`synchronize_seqscans`: makeCompatBoolVar(`synchronize_seqscans`, true, true /* anyAllowed */),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-ROW-SECURITY
	// The default in pg is "on" but row security is not supported in ZNBaseDB.
	// We blindly accept both values because as long as there are now row security policies defined,
	// either value produces the same query results in PostgreSQL. That is, as long as ZNBaseDB
	// does not support row security, accepting either "on" and "off" but ignoring the result
	// is postgres-compatible.
	// If/when ZNBaseDB is extended to support row security, the default and allowed values
	// should be modified accordingly.
	`row_security`: makeCompatBoolVar(`row_security`, false, true /* anyAllowed */),

	`statement_timeout`: {
		GetStringVal: stmtTimeoutVarGetStringVal,
		Set:          stmtTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext) string {
			ms := evalCtx.SessionData.StmtTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10)
		},
		GlobalDefault: func(sv *settings.Values) string { return "0" },
	},

	`sql_session_timeout`: {
		GetStringVal: makeIntGetStringValFn("sql_session_timeout"),
		Set: func(ctx context.Context, m *sessionDataMutator, val string) error {
			i, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return wrapSetVarError("sql_session_timeout", val, "%v", err)
			}
			if i == math.MinInt64 {
				return pgerror.Newf(pgcode.InvalidParameterValue, "numeric constant out of int64 range")
			}
			if i > 270 {
				return errors.Errorf("parameter \"server.sql_session_timeout\" requires the maximum value is 270")
			}
			if find := strings.Contains(val, "-"); find {
				i = 0
			}
			m.SetCloseSessionTimeout(i)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(evalCtx.SessionData.CloseSessionTimeout, 10)
		},
		//GlobalDefault: func(sv *settings.Values) string { return SQLSessionTimeout.String(sv) },
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(SQLSessionTimeout.Get(sv), 10)
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-TIMEZONE
	`timezone`: {
		Get: func(evalCtx *extendedEvalContext) string {
			// If the time zone is a "fixed offset" one, initialized from an offset
			// and not a standard name, then we use a magic format in the Location's
			// name. We attempt to parse that here and retrieve the original offset
			// specified by the user.
			locStr := evalCtx.SessionData.DataConversion.Location.String()
			_, origRepr, parsed := timeutil.ParseFixedOffsetTimeZone(locStr)
			if parsed {
				return origRepr
			}
			return locStr
		},
		GetStringVal:  timeZoneVarGetStringVal,
		Set:           timeZoneVarSet,
		GlobalDefault: func(_ *settings.Values) string { return "UTC" },
	},

	// This is not directly documented in PG's docs but does indeed behave this way.
	// See https://github.com/postgres/postgres/blob/REL_10_STABLE/src/backend/utils/misc/guc.c#L3401-L3409
	`transaction_isolation`: {
		Get: func(evalCtx *extendedEvalContext) string {
			isolationLevel := evalCtx.TxnModesSetter.(*connExecutor).state.isolationLevel
			if isolationLevel == tree.UnspecifiedIsolation {
				isolationLevel = tree.IsolationLevel(evalCtx.SessionData.DefaultIsolationLevel)
			}
			return util.IsolationLevelNames[isolationLevel]
		},
		RuntimeSet: func(_ context.Context, evalCtx *extendedEvalContext, s string) error {
			isolationLevel, ok := util.IsolationLevelMap[s]
			if !ok {
				return newVarValueError(`transaction_isolation`, s, "serializable", "read committed")
			}
			evalCtx.TxnModesSetter.(*connExecutor).state.setIsolationLevel(tree.IsolationLevel(isolationLevel))
			return nil
		},
		GlobalDefault: func(_ *settings.Values) string { return "serializable" },
	},

	// ZNBaseDB extension.
	`transaction_priority`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.Txn.UserPriority().String()
		},
	},

	// ZNBaseDB extension.
	`transaction_status`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.TxnState
		},
	},

	// See https://www.postgresql.org/docs/10/static/hot-standby.html#HOT-STANDBY-USERS
	`transaction_read_only`: {
		GetStringVal: makeBoolGetStringValFn("transaction_read_only"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetReadOnly(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.TxnReadOnly)
		},
		GlobalDefault: globalFalse,
	},

	// ZNBaseDB extension.
	`tracing`: {
		Get: func(evalCtx *extendedEvalContext) string {
			sessTracing := evalCtx.Tracing
			if sessTracing.Enabled() {
				val := "on"
				if sessTracing.RecordingType() == tracing.SingleNodeRecording {
					val += ", local"
				}
				if sessTracing.KVTracingEnabled() {
					val += ", kv"
				}
				return val
			}
			return "off"
		},
		// Setting is done by the SetTracing statement.
	},

	// ZNBaseDB extension.
	`allow_prepare_as_opt_plan`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.AllowPrepareAsOptPlan)
		},
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetAllowPrepareAsOptPlan(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},
}

func init() {
	// Initialize delegate.ValidVars.
	for v := range varGen {
		delegate.ValidVars[v] = struct{}{}
	}
	// Initialize varNames.
	varNames = func() []string {
		res := make([]string, 0, len(varGen))
		for vName := range varGen {
			res = append(res, vName)
		}
		sort.Strings(res)
		return res
	}()
}

func makeBoolGetStringValFn(varName string) getStringValFn {
	return func(
		ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
	) (string, error) {
		s, err := getSingleBool(varName, evalCtx, values)
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(bool(*s)), nil
	}
}

func makeReadOnlyVar(value string) sessionVar {
	return sessionVar{
		Get:           func(_ *extendedEvalContext) string { return value },
		GlobalDefault: func(_ *settings.Values) string { return value },
	}
}

func displayPgBool(val bool) func(_ *settings.Values) string {
	strVal := formatBoolAsPostgresSetting(val)
	return func(_ *settings.Values) string { return strVal }
}

var globalFalse = displayPgBool(false)

func makeCompatBoolVar(varName string, displayValue, anyValAllowed bool) sessionVar {
	displayValStr := formatBoolAsPostgresSetting(displayValue)
	return sessionVar{
		Get: func(_ *extendedEvalContext) string { return displayValStr },
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return pgerror.NewErrorf(pgcode.InvalidParameterValue, "%v", err)
			}
			if anyValAllowed || b == displayValue {
				return nil
			}
			telemetry.Inc(sqltelemetry.UnimplementedSessionVarValueCounter(varName, s))
			allowedVals := []string{displayValStr}
			if anyValAllowed {
				allowedVals = append(allowedVals, formatBoolAsPostgresSetting(!displayValue))
			}
			return newVarValueError(varName, s, allowedVals...).SetDetailf(
				"this parameter is currently recognized only for compatibility and has no effect in ZNBaseDB.")
		},
		GlobalDefault: func(sv *settings.Values) string { return displayValStr },
	}
}

func makeCompatIntVar(varName string, displayValue int, extraAllowed ...int) sessionVar {
	displayValueStr := strconv.Itoa(displayValue)
	extraAllowedStr := make([]string, len(extraAllowed))
	for i, v := range extraAllowed {
		extraAllowedStr[i] = strconv.Itoa(v)
	}
	varObj := makeCompatStringVar(varName, displayValueStr, extraAllowedStr...)
	varObj.GetStringVal = makeIntGetStringValFn(varName)
	return varObj
}

func makeCompatStringVar(varName, displayValue string, extraAllowed ...string) sessionVar {
	allowedVals := append(extraAllowed, strings.ToLower(displayValue))
	return sessionVar{
		Get: func(_ *extendedEvalContext) string {
			return displayValue
		},
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			enc := strings.ToLower(s)
			for _, a := range allowedVals {
				if enc == a {
					return nil
				}
			}
			telemetry.Inc(sqltelemetry.UnimplementedSessionVarValueCounter(varName, s))
			return newVarValueError(varName, s, allowedVals...).SetDetailf(
				"this parameter is currently recognized only for compatibility and has no effect in ZNBaseDB.")
		},
		GlobalDefault: func(sv *settings.Values) string { return displayValue },
	}
}

// makeIntGetStringValFn returns a getStringValFn which allows
// the user to provide plain integer values to a SET variable.
func makeIntGetStringValFn(name string) getStringValFn {
	return func(ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr) (string, error) {
		s, err := getIntVal(&evalCtx.EvalContext, name, values)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(s, 10), nil
	}
}

// IsSessionVariableConfigurable returns true iff there is a session
// variable with the given name and it is settable by a client
// (e.g. in pgwire).
func IsSessionVariableConfigurable(varName string) (exists, configurable bool) {
	v, exists := varGen[varName]
	return exists, v.Set != nil
}

var varNames = func() []string {
	res := make([]string, 0, len(varGen))
	for vName := range varGen {
		res = append(res, vName)
	}
	sort.Strings(res)
	return res
}()

func getSingleBool(
	name string, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (*tree.DBool, error) {
	if len(values) != 1 {
		return nil, newSingleArgVarError(name)
	}
	val, err := values[0].Eval(&evalCtx.EvalContext)
	if err != nil {
		return nil, err
	}
	b, ok := val.(*tree.DBool)
	if !ok {
		return nil, pgerror.NewErrorf(pgcode.InvalidParameterValue,
			"parameter %q requires a Boolean value", name).SetDetailf(
			"%s is a %s", values[0], val.ResolvedType())
	}
	return b, nil
}

func getSessionVar(name string, missingOk bool) (bool, sessionVar, error) {
	if _, ok := UnsupportedVars[name]; ok {
		return false, sessionVar{}, pgerror.Unimplemented("set."+name,
			"the configuration setting %q is not supported", name)
	}

	v, ok := varGen[name]
	if !ok {
		if missingOk {
			return false, sessionVar{}, nil
		}
		return false, sessionVar{}, pgerror.NewErrorf(pgcode.UndefinedObject,
			"unrecognized configuration parameter %q", name)
	}

	return true, v, nil
}

// GetSessionVar implements the EvalSessionAccessor interface.
func (p *planner) GetSessionVar(
	_ context.Context, varName string, missingOk bool,
) (bool, string, error) {
	name := strings.ToLower(varName)
	ok, v, err := getSessionVar(name, missingOk)
	if err != nil || !ok {
		return ok, "", err
	}

	return true, v.Get(&p.extendedEvalCtx), nil
}

// SetSessionVar implements the EvalSessionAccessor interface.
func (p *planner) SetSessionVar(ctx context.Context, varName, newVal string) error {
	name := strings.ToLower(varName)
	_, v, err := getSessionVar(name, false /* missingOk */)
	if err != nil {
		return err
	}

	if v.Set == nil && v.RuntimeSet == nil {
		return newCannotChangeParameterError(name)
	}
	if v.RuntimeSet != nil {
		return v.RuntimeSet(ctx, &p.extendedEvalCtx, newVal)
	}
	return v.Set(ctx, p.sessionDataMutator, newVal)
}
