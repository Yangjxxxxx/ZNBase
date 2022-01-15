package events

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

//CreateDatabaseMetric is the metric of Create Database
type CreateDatabaseMetric struct {
	name    string                // metric name
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context       // accept context
	metrics *CreateDBMetrics      // get metrics information
	rules   []CreateDBRules       // realize 1 metric to n rules
}

//CreateDBMetrics contain CatchCount
type CreateDBMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewCreateDatabaseMetric init a new metric
func NewCreateDatabaseMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *CreateDatabaseMetric {
	// get metric information
	metrics := &CreateDBMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.database.create",
				Help:        "create a new database",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &CreateDatabaseMetric{
		ie:      ie,
		name:    "db_count",
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateDBRules{
			NewCreateDatabaseRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (cdbm *CreateDatabaseMetric) RegistMetric() interface{} {
	return cdbm.metrics
}

//Metric check rules
func (cdbm *CreateDatabaseMetric) Metric(info interface{}) {

	// 	increment by 1
	cdbm.metrics.CatchCount.Inc(1)

	// 	check all rules
	for _, rule := range cdbm.rules {
		switch info.(type) {
		case *infos.CreateDatabaseInfo:
			rule.rule(cdbm.ctx, cdbm, info.(*infos.CreateDatabaseInfo))
		}
	}
}

//EventLog define time to exec to do count
func (cdbm *CreateDatabaseMetric) EventLog(ctx context.Context) (int, bool) {
	row, err := cdbm.ie.QueryRow(
		ctx,
		"Get Create Database Count In 5 Seconds",
		nil,
		"SELECT count(1) FROM eventlog WHERE \"eventType\" = $1 AND \"timestamp\" > $2",
		string(sql.EventLogCreateDatabase),
		timeutil.Now().Add(-createDBTime),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventLogCreateDatabase, err)
	}
	if row != nil {
		return int(*row[0].(*tree.DInt)), true
	}
	return 0, false
}

//CreateDBRules define Rules
type CreateDBRules interface {
	rule(ctx context.Context, metric *CreateDatabaseMetric, info *infos.CreateDatabaseInfo)
}

//CreateDatabaseRule get actions
type CreateDatabaseRule struct {
	actions []CreateDBActions // realize 1 rule to n actions
}

//NewCreateDatabaseRule use action to send mails
func NewCreateDatabaseRule(mail *mail.Server) *CreateDatabaseRule {
	return &CreateDatabaseRule{
		actions: []CreateDBActions{
			NewCreateDatabaseAction(mail),
		},
	}
}

var createDBTime = 5 * time.Second
var createDBFrequency = 10

// rule defines a rule, if frequency of create database reaches createDBFrequency in createDBTime, do actions
func (cdbr *CreateDatabaseRule) rule(
	ctx context.Context, metric *CreateDatabaseMetric, info *infos.CreateDatabaseInfo,
) {
	frequency, ok := metric.EventLog(ctx)
	if ok {
		if frequency > createDBFrequency {
			for _, action := range cdbr.actions {
				action.action(ctx, info)
			}
		}
	}
}

//CreateDBActions define Actions
type CreateDBActions interface {
	action(ctx context.Context, info *infos.CreateDatabaseInfo)
}

//CreateDatabaseAction define mail
type CreateDatabaseAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewCreateDatabaseAction return action.mail
func NewCreateDatabaseAction(mail *mail.Server) *CreateDatabaseAction {
	return &CreateDatabaseAction{
		mail: mail,
	}
}

// send mails
func (cdba *CreateDatabaseAction) action(ctx context.Context, info *infos.CreateDatabaseInfo) {
	if err := cdba.mail.Send(ctx, "Audit: Create Database", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Create Database action, err:%s", err)
	}
}
