// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	"context"
	"encoding/json"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/icl/dump"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/retry"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

func init() {
	sql.AddPlanHook(cdcPlanHook)
	jobs.AddResumeHook(cdcResumeHook)
}

type envelopeType string
type formatType string

// SchemaChangeEventClass defines a set of schema change event types which
// trigger the action defined by the SchemaChangeEventPolicy.
type SchemaChangeEventClass string

// SchemaChangePolicy defines the behavior of a changefeed when a schema
// change event which is a member of the changefeed's schema change events.
type SchemaChangePolicy string

const (
	optSchemaRegistry                     = `confluent_schema_registry`
	optCursor                             = `cursor`
	optEnvelope                           = `envelope`
	optFormat                             = `format`
	optResolvedTimestamps                 = `resolved`
	optUpdatedTimestamps                  = `updated`
	optDiff                               = `diff`
	optWithDdl                            = `have_ddl`
	optType                               = `type`
	optKeyInValue                         = `key_in_value`
	optEnvelopeKeyOnly       envelopeType = `key_only`
	optEnvelopeRow           envelopeType = `row`
	optEnvelopeDeprecatedRow envelopeType = `deprecated_row`
	optEnvelopeWrapped       envelopeType = `wrapped`

	optFormatJSON formatType = `json`
	optFormatAvro formatType = `experimental_avro`
	// OptSchemaChangeEvents  Schema Change Events
	OptSchemaChangeEvents = `schema_change_events`
	//OptSchemaChangePolicy Schema Change Policy
	OptSchemaChangePolicy = `schema_change_policy`

	// OptSchemaChangeEventClassColumnChange corresponds to all schema change
	// events which add or remove any column.
	OptSchemaChangeEventClassColumnChange SchemaChangeEventClass = `column_changes`
	// OptSchemaChangeEventClassDefault corresponds to all schema change
	// events which add a column with a default value or remove any column.
	OptSchemaChangeEventClassDefault SchemaChangeEventClass = `default`

	// OptSchemaChangePolicyBackfill indicates that when a schema change event
	// occurs, a full table backfill should occur.
	OptSchemaChangePolicyBackfill SchemaChangePolicy = `backfill`
	// OptSchemaChangePolicyNoBackfill indicates that when a schema change event occurs
	// no backfill should occur and the changefeed should continue.
	OptSchemaChangePolicyNoBackfill SchemaChangePolicy = `nobackfill`
	// OptSchemaChangePolicyStop indicates that when a schema change event occurs
	// the changefeed should resolve all data up to when it occurred and then
	// exit with an error indicating the HLC timestamp of the change from which
	// the user could continue.
	OptSchemaChangePolicyStop SchemaChangePolicy = `stop`

	sinkParamCACert           = `ca_cert`
	sinkParamFileSize         = `file_size`
	sinkParamSchemaTopic      = `schema_topic`
	sinkParamTLSEnabled       = `tls_enabled`
	sinkParamTopicPrefix      = `topic_prefix`
	sinkSchemeBuffer          = ``
	sinkSchemeExperimentalSQL = `experimental-sql`
	sinkSchemeKafka           = `kafka`
	sinkParamSASLEnabled      = `sasl_enabled`
	sinkParamSASLHandshake    = `sasl_handshake`
	sinkParamSASLUser         = `sasl_user`
	sinkParamSASLPassword     = `sasl_password`
	descriptor                = "descriptor4E07408562BEDB8B60CE05C1DECFE3AD16B72230967DE01F640B7E4729B49FCE"
)

var changefeedOptionExpectValues = map[string]sql.KVStringOptValidate{
	optSchemaRegistry:     sql.KVStringOptRequireValue,
	optCursor:             sql.KVStringOptRequireValue,
	optEnvelope:           sql.KVStringOptRequireValue,
	optFormat:             sql.KVStringOptRequireValue,
	optKeyInValue:         sql.KVStringOptRequireNoValue,
	optResolvedTimestamps: sql.KVStringOptAny,
	optUpdatedTimestamps:  sql.KVStringOptRequireNoValue,
	optDiff:               sql.KVStringOptRequireNoValue,
	optWithDdl:            sql.KVStringOptRequireNoValue,
	optType:               sql.KVStringOptRequireNoValue,
	OptSchemaChangeEvents: sql.KVStringOptRequireValue,
	OptSchemaChangePolicy: sql.KVStringOptRequireValue,
}

// cdcPlanHook implements sql.PlanHookFn.
func cdcPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	changefeedStmt, ok := stmt.(*tree.CreateChangefeed)
	if !ok {
		return nil, nil, nil, false, nil
	}

	//isDDL := false
	var sinkURIFn func() (string, error)
	var header sqlbase.ResultColumns
	unspecifiedSink := changefeedStmt.SinkURI == nil
	avoidBuffering := false
	if unspecifiedSink {
		// An unspecified sink triggers a fairly radical change in behavior.
		// Instead of setting up a system.job to emit to a sink in the
		// background and returning immediately with the job ID, the `CREATE
		// CHANGEFEED` blocks forever and returns all changes as rows directly
		// over pgwire. The types of these rows are `(topic STRING, key BYTES,
		// value BYTES)` and they correspond exactly to what would be emitted to
		// a sink.
		sinkURIFn = func() (string, error) { return ``, nil }
		header = sqlbase.ResultColumns{
			{Name: "table", Typ: types.String},
			{Name: "key", Typ: types.Bytes},
			{Name: "value", Typ: types.Bytes},
		}
		avoidBuffering = true
	} else {
		var err error
		sinkURIFn, err = p.TypeAsString(changefeedStmt.SinkURI, `CREATE CHANGEFEED`)
		if err != nil {
			return nil, nil, nil, false, err
		}
		header = sqlbase.ResultColumns{
			{Name: "job_id", Typ: types.Int},
			{Name: "topic", Typ: types.String},
			{Name: "table_name", Typ: types.String},
		}
	}

	optsFn, err := p.TypeAsStringOpts(changefeedStmt.Options, changefeedOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionCreateChangefeed) {
			return errors.Errorf(`CREATE CHANGEFEED requires all nodes to be upgraded to %s`,
				cluster.VersionByKey(cluster.VersionCreateChangefeed),
			)
		}

		sinkURI, err := sinkURIFn()
		if err != nil {
			return err
		}
		if !unspecifiedSink && sinkURI == `` {
			// Error if someone specifies an INTO with the empty string. We've
			// already sent the wrong result column headers.
			return errors.New(`omit the SINK clause for inline results`)
		}

		u, err := url.Parse(sinkURI)
		if err != nil {
			return err
		}
		q := u.Query()
		prefix := q.Get(sinkParamTopicPrefix)
		opts, err := optsFn()
		if err != nil {
			return err
		}
		jobDescription, err := cdcJobDescription(changefeedStmt, sinkURI, opts)
		if err != nil {
			return err
		}

		statementTime := hlc.Timestamp{
			WallTime: p.ExtendedEvalContext().GetStmtTimestamp().UnixNano(),
		}
		var initialHighWater hlc.Timestamp
		if cursor, ok := opts[optCursor]; ok {
			asOf := tree.AsOfClause{Expr: tree.NewStrVal(cursor)}
			var err error
			if initialHighWater, err = p.EvalAsOfTimestamp(asOf); err != nil {
				return err
			}
			statementTime = initialHighWater
		}

		// For now, disallow targeting a database or wildcard table selection.
		// Getting it right as tables enter and leave the set over time is
		// tricky.
		if len(changefeedStmt.Targets.Databases) > 0 {
			return errors.Errorf(`CHANGEFEED cannot target %s`,
				tree.AsString(&changefeedStmt.Targets))
		}
		for _, t := range changefeedStmt.Targets.Tables {
			p, err := t.NormalizeTablePattern()
			if err != nil {
				return err
			}
			if _, ok := p.(*tree.TableName); !ok {
				return errors.Errorf(`CHANGEFEED cannot target %s`, tree.AsString(t))
			}
		}

		// This grabs table descriptors once to get their ids.
		targetDescs, _, err := dump.ResolveTargetsToDescriptors(
			ctx, p, statementTime, changefeedStmt.Targets, tree.RequestedDescriptors, false)
		if err != nil {
			return err
		}
		IDPrefix := make(map[sqlbase.ID]string, len(targetDescs))
		IDTableName := make(map[sqlbase.ID]string, len(targetDescs))
		targets := make(jobspb.ChangefeedTargets, len(targetDescs))
		tables := make(map[uint32]*sqlbase.TableDescriptor, len(targetDescs))
		for _, desc := range targetDescs {
			if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
				tableName, err := getFullTableName(ctx, tableDesc, p)
				if err != nil {
					return err
				}

				targets[tableDesc.ID] = jobspb.ChangefeedTarget{
					StatementTimeName: tableName,
				}
				IDPrefix[tableDesc.ID] = prefix
				IDTableName[tableDesc.ID] = tableName
				tables[uint32(tableDesc.ID)] = tableDesc
				if err := validateCDCTable(ctx, targets, tableDesc, p); err != nil {
					return err
				}
				if tableDesc.ID == keys.DescriptorTableID {
					//isDDL = true
				}
			}
		}

		details := jobspb.ChangefeedDetails{
			Targets:       targets,
			Opts:          opts,
			SinkURI:       sinkURI,
			StatementTime: statementTime,
			Tables:        tables,
		}
		progress := jobspb.Progress{
			Progress: &jobspb.Progress_HighWater{HighWater: &initialHighWater},
			Details: &jobspb.Progress_Changefeed{
				Changefeed: &jobspb.ChangefeedProgress{},
			},
		}

		if details, err = validateDetails(details); err != nil {
			return err
		}

		// Feature telemetry
		parsedSink, err := url.Parse(sinkURI)
		if err != nil {
			return err
		}
		telemetrySink := parsedSink.Scheme
		if telemetrySink == `` {
			telemetrySink = `sinkless`
		}
		telemetry.Count(`changefeed.create.sink.` + telemetrySink)
		telemetry.Count(`changefeed.create.format.` + details.Opts[optFormat])
		telemetry.CountBucketed(`changefeed.create.num_tables`, int64(len(targets)))

		if details.SinkURI == `` {
			err := distCDCFlow(ctx, p, 0 /* jobID */, details, progress, resultsCh)
			return MaybeStripTerminalErrorMarker(err)
		}

		settings := p.ExecCfg().Settings

		// In the case where a user is executing a CREATE CHANGEFEED and is still
		// waiting for the statement to return, we take the opportunity to ensure
		// that the user has not made any obvious errors when specifying the sink in
		// the CREATE CHANGEFEED statement. To do this, we create a "canary" sink,
		// which will be immediately closed, only to check for errors.Also return to the user's corresponding topic
		var topics map[string]string
		{
			nodeID := p.ExtendedEvalContext().NodeID
			canarySink, err := getSink(details.SinkURI, nodeID, details.Opts, details.Targets,
				settings, p.ExecCfg().DistSQLSrv.DumpSinkFromURI, 0)
			if err != nil {
				return MaybeStripTerminalErrorMarker(err)
			}
			topics, err = canarySink.GetTopics()
			if err != nil {
				return err
			}
			if err := canarySink.Close(); err != nil {
				return err
			}
		}
		idPrefix, _ := json.Marshal(IDPrefix)
		//todo make jobDescription more reasonable
		idTableName, _ := json.Marshal(IDTableName)
		jobDescription = jobDescription + "||" + string(idPrefix) + "||" + string(idTableName)
		// Make a channel for runChangefeedFlow to signal once everything has
		// been setup okay. This intentionally abuses what would normally be
		// hooked up to resultsCh to avoid a bunch of extra plumbing.
		startedCh := make(chan tree.Datums)
		job, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, startedCh, jobs.Record{
			Description: jobDescription,
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
				for _, desc := range targetDescs {
					sqlDescIDs = append(sqlDescIDs, desc.GetID())
				}
				return sqlDescIDs
			}(),
			Details:  details,
			Progress: *progress.GetChangefeed(),
		})
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case <-startedCh:
			// The feed set up without error, return control to the user.
		}
		if len(topics) == 0 {
			resultsCh <- tree.Datums{
				tree.NewDInt(tree.DInt(*job.ID())),
				tree.NewDString(""),
				tree.NewDString(""),
			}
		} else {
			for topic, tableName := range topics {
				if strings.HasSuffix(topic, descriptor) {
					continue
				}
				//区分当前的sink类型，从而按照不同的规则组装topic名称
				resultsCh <- tree.Datums{
					tree.NewDInt(tree.DInt(*job.ID())),
					tree.NewDString(topic),
					tree.NewDString(tableName),
				}
			}
		}
		return nil
	}
	return fn, header, nil, avoidBuffering, nil
}

//
func getFullTableName(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, p sql.PlanHookState,
) (string, error) {
	var tableName string
	tableName = tableDesc.Name
	if tableDesc.ID > keys.PostgresSchemaID {
		//区分当前的sink类型，从而按照不同的规则组装topic名称
		schema, err := sqlbase.GetSchemaDescFromID(ctx, p.ExtendedEvalContext().DB.NewTxn(ctx, "cdc-dml-findSchemaName"), tableDesc.ParentID)
		if err != nil {
			return tableName, err
		}
		database, err := sqlbase.GetDatabaseDescFromID(ctx, p.ExtendedEvalContext().DB.NewTxn(ctx, "cdc-dml-findDatabaseName"), schema.ParentID)
		if err != nil {
			return tableName, err
		}
		tableName = strings.Join([]string{database.Name, schema.Name, tableDesc.Name}, ".")
	}
	return tableName, nil
}

func cdcJobDescription(
	cdc *tree.CreateChangefeed, sinkURI string, opts map[string]string,
) (string, error) {
	cleanedSinkURI, err := dumpsink.SanitizeDumpSinkURI(sinkURI)
	if err != nil {
		return "", err
	}
	c := &tree.CreateChangefeed{
		Targets: cdc.Targets,
		SinkURI: tree.NewDString(cleanedSinkURI),
	}
	for k, v := range opts {
		opt := tree.KVOption{Key: tree.Name(k)}
		if len(v) > 0 {
			opt.Value = tree.NewDString(v)
		}
		c.Options = append(c.Options, opt)
	}
	sort.Slice(c.Options, func(i, j int) bool { return c.Options[i].Key < c.Options[j].Key })
	return tree.AsStringWithFlags(c, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType), nil
}

func validateDetails(details jobspb.ChangefeedDetails) (jobspb.ChangefeedDetails, error) {
	if details.Opts == nil {
		// The proto MarshalTo method omits the Opts field if the map is empty.
		// So, if no options were specified by the user, Opts will be nil when
		// the job gets restarted.
		details.Opts = map[string]string{}
	}

	{
		const opt = optResolvedTimestamps
		if o, ok := details.Opts[opt]; ok && o != `` {
			if d, err := time.ParseDuration(o); err != nil {
				return jobspb.ChangefeedDetails{}, err
			} else if d < 0 {
				return jobspb.ChangefeedDetails{}, errors.Errorf(
					`negative durations are not accepted: %s='%s'`, opt, o)
			}
		}
	}
	{
		const opt = OptSchemaChangeEvents
		switch v := SchemaChangeEventClass(details.Opts[opt]); v {
		case ``, OptSchemaChangeEventClassDefault:
			details.Opts[opt] = string(OptSchemaChangeEventClassDefault)
		case OptSchemaChangeEventClassColumnChange:
			// No-op
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`negative durations are not accepted: %s='%s'`,
				optResolvedTimestamps, details.Opts[optResolvedTimestamps])
		}
	}

	{
		const opt = OptSchemaChangePolicy
		switch v := SchemaChangePolicy(details.Opts[opt]); v {
		case ``, OptSchemaChangePolicyBackfill:
			details.Opts[opt] = string(OptSchemaChangePolicyBackfill)
		case OptSchemaChangePolicyNoBackfill:
			// No-op
		case OptSchemaChangePolicyStop:
			// No-op
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
	}
	{
		const opt = optEnvelope
		switch v := envelopeType(details.Opts[opt]); v {
		case optEnvelopeRow, optEnvelopeDeprecatedRow:
			details.Opts[opt] = string(optEnvelopeRow)
		case optEnvelopeKeyOnly:
			details.Opts[opt] = string(optEnvelopeKeyOnly)
		case ``, optEnvelopeWrapped:
			details.Opts[opt] = string(optEnvelopeWrapped)
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
	}
	{
		const opt = optFormat
		switch v := formatType(details.Opts[opt]); v {
		case ``, optFormatJSON:
			details.Opts[opt] = string(optFormatJSON)
		case optFormatAvro:
			if _, ok := details.Opts[optWithDdl]; ok {
				return details, errors.Errorf("format avro does not support enabling DDL monitoring")
			}
			// No-op.
		default:
			return jobspb.ChangefeedDetails{}, errors.Errorf(
				`unknown %s: %s`, opt, v)
		}
	}

	return details, nil
}

// validate weather the table satisfy the cdc
func validateCDCTable(
	ctx context.Context,
	targets jobspb.ChangefeedTargets,
	tableDesc *sqlbase.TableDescriptor,
	p sql.PlanHookState,
) error {
	t, ok := targets[tableDesc.ID]
	if !ok {
		return errors.Errorf(`unwatched table: %s`, tableDesc.Name)
	}
	//进行权限检查，只要用户在该表上具有SELECT权限，则可以创建该表上的CDC
	if p != nil {
		if err := p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil && tableDesc.ID != keys.DescriptorTableID {
			return errors.Wrapf(err, `CHANGEFEEDs failed :`)
		}
	}

	// Technically, the only non-user table known not to work is system.jobs
	// (which creates a cycle since the resolved timestamp high-water mark is
	// saved in it), but there are subtle differences in the way many of them
	// work and this will be under-tested, so disallow them all until demand
	// dictates.
	if tableDesc.ID < keys.MinUserDescID {
		return errors.Errorf(`CHANGEFEEDs are not supported on system tables`)
	}
	if tableDesc.IsView() {
		return errors.Errorf(`CHANGEFEED cannot target views: %s`, tableDesc.Name)
	}
	if tableDesc.IsVirtualTable() {
		return errors.Errorf(`CHANGEFEED cannot target virtual tables: %s`, tableDesc.Name)
	}
	if tableDesc.IsSequence() {
		return errors.Errorf(`CHANGEFEED cannot target sequences: %s`, tableDesc.Name)
	}
	if len(tableDesc.Families) != 1 {
		return errors.Errorf(
			`CHANGEFEEDs are currently supported on tables with exactly 1 column family: %s has %d`,
			tableDesc.Name, len(tableDesc.Families))
	}

	if tableDesc.State == sqlbase.TableDescriptor_DROP {
		return errors.Errorf(`"%s" was dropped or truncated`, t.StatementTimeName)
	}

	// TODO(mrtracy): re-enable this when allow-backfill option is added.
	// if tableDesc.HasColumnBackfillMutation() {
	// 	return errors.Errorf(`CHANGEFEEDs cannot operate on tables being backfilled`)
	// }

	return nil
}

// cdcResumer implements the Resumer of job
type cdcResumer struct{}

func (b *cdcResumer) Resume(
	ctx context.Context, job *jobs.Job, planHookState interface{}, startedCh chan<- tree.Datums,
) error {
	phs := planHookState.(sql.PlanHookState)
	execCfg := phs.ExecCfg()
	jobID := *job.ID()
	details := job.Details().(jobspb.ChangefeedDetails)
	progress := job.Progress()

	// TODO(dan): This is a workaround for not being able to set an initial
	// progress high-water when creating a job (currently only the progress
	// details can be set). I didn't want to pick off the refactor to get this
	// fix in, but it'd be nice to remove this hack.
	if _, ok := details.Opts[optCursor]; ok {
		if h := progress.GetHighWater(); h == nil || *h == (hlc.Timestamp{}) {
			progress.Progress = &jobspb.Progress_HighWater{HighWater: &details.StatementTime}
		}
	}

	// We'd like to avoid failing a changefeed unnecessarily, so when an error
	// bubbles up to this level, we'd like to "retry" the flow if possible. This
	// could be because the sink is down or because a cockroach node has crashed
	// or for many other reasons. We initially tried to whitelist which errors
	// should cause the changefeed to retry, but this turns out to be brittle, so
	// we switched to a blacklist. Any error that is expected to be permanent is
	// now marked with `MarkTerminalError` by the time it comes out of
	// `distChangefeedFlow`. Everything else should be logged loudly and retried.
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
	}
	const consecutiveIdenticalErrorsWindow = time.Minute
	const consecutiveIdenticalErrorsDefault = int(5)
	var consecutiveErrors struct {
		message   string
		count     int
		firstSeen time.Time
	}
	var err error
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		if err = distCDCFlow(ctx, phs, jobID, details, progress, startedCh); err == nil {
			return nil
		}
		if IsTerminalError(err) {
			err = MaybeStripTerminalErrorMarker(err)
			log.Warningf(ctx, `CHANGEFEED job %d returning with error: %v`, jobID, err)
			return err
		}

		// If we receive an identical error message more than some number of times
		// in a time period, bail. This is a safety net in case we miss marking
		// something as terminal.
		{
			m, d := err.Error(), timeutil.Since(consecutiveErrors.firstSeen)
			if d > consecutiveIdenticalErrorsWindow || m != consecutiveErrors.message {
				consecutiveErrors.message = m
				consecutiveErrors.count = 0
				consecutiveErrors.firstSeen = timeutil.Now()
			}
			consecutiveErrors.count++
			consecutiveIdenticalErrorsThreshold := consecutiveIdenticalErrorsDefault
			if cfKnobs, ok := execCfg.DistSQLRunTestingKnobs.CDC.(*TestingKnobs); ok {
				if c := cfKnobs.ConsecutiveIdenticalErrorBailoutCount; c != 0 {
					consecutiveIdenticalErrorsThreshold = c
				}
			}
			if consecutiveErrors.count >= consecutiveIdenticalErrorsThreshold {
				log.Warningf(ctx, `CHANGEFEED job %d saw the same non-terminal error %d times in %s: %+v`,
					jobID, consecutiveErrors.count, d, err)
				return err
			}
		}
		log.Warningf(ctx, `CHANGEFEED job %d encountered retryable error: %+v`, jobID, err)
		if metrics, ok := execCfg.JobRegistry.MetricsStruct().CDC.(*Metrics); ok {
			metrics.SinkErrorRetries.Inc(1)
		}
		reloadedJob, reloadErr := execCfg.JobRegistry.LoadJob(ctx, jobID)
		if reloadErr != nil {
			log.Warningf(ctx, `CHANGEFEED job %d could not reload job progress; `+
				`continuing from last known high-water of %s: %v`,
				jobID, progress.GetHighWater(), reloadErr)
		} else {
			progress = reloadedJob.Progress()
		}
		// startedCh is normally used to signal back to the creator of the job that
		// the job has started; however, in this case nothing will ever receive
		// on the channel, causing the changefeed flow to block. Replace it with
		// a dummy channel.
		startedCh = make(chan tree.Datums, 1)
	}
	// We only hit this if `r.Next()` returns false, which right now only happens
	// on context cancellation.
	return errors.Wrap(err, `ran out of retries`)
}

func (b *cdcResumer) OnFailOrCancel(context.Context, *client.Txn, *jobs.Job) error { return nil }
func (b *cdcResumer) OnSuccess(context.Context, *client.Txn, *jobs.Job) error      { return nil }
func (b *cdcResumer) OnTerminal(context.Context, *jobs.Job, jobs.Status, chan<- tree.Datums) {
}

// register cdc to job registry
func cdcResumeHook(typ jobspb.Type, _ *cluster.Settings) jobs.Resumer {
	if typ != jobspb.TypeChangefeed {
		return nil
	}
	return &cdcResumer{}
}
