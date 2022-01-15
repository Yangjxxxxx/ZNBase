package events

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

//CreateSchemaMetric is the metric of Create Schema
type CreateSchemaMetric struct {
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context       //accept context
	metrics *CreateSHMMetrics     //get metrics information
	rules   []CreateSHMRules      //realize 1 metric to n rules
}

//CreateSHMMetrics contain CatchCount
type CreateSHMMetrics struct {
	CatchCount *metric.Counter
}

//NewCreateSchemaMetric init a a new metric
func NewCreateSchemaMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *CreateSchemaMetric {
	//get metric information
	metrics := &CreateSHMMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.schema.create",
				Help:        "create a new schema",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &CreateSchemaMetric{
		ie:      ie,
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateSHMRules{
			NewCreateSchemaRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csqm *CreateSchemaMetric) RegistMetric() interface{} {
	return csqm.metrics
}

//Metric check rules
func (csqm *CreateSchemaMetric) Metric(info interface{}) {

	csqm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range csqm.rules {
		switch info.(type) {
		case string:
			rule.rule(csqm.ctx, csqm, info.(string))
		}
	}
}

//EventLog define time to exec to do count
func (csqm *CreateSchemaMetric) EventLog(ctx context.Context) (int, bool) {
	row, err := csqm.ie.QueryRow(
		ctx,
		"Get Create Sequence Count in 5 Seconds",
		nil,
		"SELECT count(1) FROM eventlog WHERE \"eventType\" = $1 AND \"timestamp\" > $2",
		string(sql.EventLogCreateSchema),
		timeutil.Now().Add(-createSHMTime),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventLogCreateSchema, err)
	}
	if row != nil {
		return int(*row[0].(*tree.DInt)), true
	}
	return 0, false
}

//CreateSHMRules define Rules
type CreateSHMRules interface {
	rule(ctx context.Context, metric *CreateSchemaMetric, stmt string)
}

//CreateSchemaRule get actions
type CreateSchemaRule struct {
	actions []CreateSHMActions // realize 1 rule to n actions
}

//NewCreateSchemaRule use action to send mails
func NewCreateSchemaRule(mail *mail.Server) *CreateSchemaRule {
	return &CreateSchemaRule{
		actions: []CreateSHMActions{
			NewCreateSchemaAction(mail),
		},
	}
}

var createSHMTime = 5 * time.Second
var createSHMFrequency = 10

// rules define
func (csqr *CreateSchemaRule) rule(ctx context.Context, metric *CreateSchemaMetric, stmt string) {
	frequency, ok := metric.EventLog(ctx)
	if ok {
		if frequency > createSHMFrequency {
			for _, action := range csqr.actions {
				action.action(ctx, stmt)
			}
		}
	}
}

//CreateSHMActions define Actions
type CreateSHMActions interface {
	action(ctx context.Context, stmt string)
}

//CreateSchemaAction define mail
type CreateSchemaAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewCreateSchemaAction return action.mail
func NewCreateSchemaAction(mail *mail.Server) *CreateSchemaAction {
	return &CreateSchemaAction{
		mail: mail,
	}
}

// send mails
func (csqa *CreateSchemaAction) action(ctx context.Context, stmt string) {
	if err := csqa.mail.Send(ctx, "Audit: Create schema", stmt); err != nil {
		log.Errorf(ctx, "got error when do Create schema action, err:%s", err)
	}
}
