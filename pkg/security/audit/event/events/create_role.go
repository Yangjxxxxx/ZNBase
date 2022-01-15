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

//CreateRoleMetric is the metric of Create Role
type CreateRoleMetric struct {
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context       // accept context
	metrics *CreateRLMetrics      // get metrics information
	rules   []CreateRLRules       // realize 1 metric to n rules
}

//CreateRLMetrics contain CatchCount
type CreateRLMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewCreateRoleMetric init a new metric
func NewCreateRoleMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *CreateRoleMetric {
	// get metric information
	metrics := &CreateRLMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.role.create",
				Help:        "create a new Role",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &CreateRoleMetric{
		ie:      ie,
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateRLRules{
			NewCreateRoleRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (cidm *CreateRoleMetric) RegistMetric() interface{} {
	return cidm.metrics
}

//Metric check rules
func (cidm *CreateRoleMetric) Metric(info interface{}) {

	cidm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range cidm.rules {
		switch info.(type) {
		case string:
			rule.rule(cidm.ctx, cidm, info.(string))
		}
	}
}

//EventLog define time to exec to do count
func (cidm *CreateRoleMetric) EventLog(ctx context.Context) (int, bool) {
	row, err := cidm.ie.QueryRow(
		ctx,
		"Get Create Role Count In 5 Seconds",
		nil,
		"SELECT count(1) FROM eventlog WHERE \"eventType\" = $1 AND \"timestamp\" > $2",
		string(sql.EventLogCreateRole),
		timeutil.Now().Add(-createRoleTime),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventLogCreateDatabase, err)
	}
	if row != nil {
		return int(*row[0].(*tree.DInt)), true
	}
	return 0, false
}

//CreateRLRules define Rules
type CreateRLRules interface {
	rule(ctx context.Context, metric *CreateRoleMetric, stmt string)
}

//CreateRoleRule get actions
type CreateRoleRule struct {
	actions []CreateRLActions // realize 1 rule to n actions
}

//NewCreateRoleRule use action to send mails
func NewCreateRoleRule(mail *mail.Server) *CreateRoleRule {
	return &CreateRoleRule{
		actions: []CreateRLActions{
			NewCreateRoleAction(mail),
		},
	}
}

var createRoleTime = 5 * time.Second
var createRoleFrequency = 10

// rules define
func (cidr *CreateRoleRule) rule(ctx context.Context, metric *CreateRoleMetric, stmt string) {
	frequency, ok := metric.EventLog(ctx)
	if ok {
		if frequency > createRoleFrequency {
			for _, action := range cidr.actions {
				action.action(ctx, stmt)
			}
		}
	}
}

//CreateRLActions define Actions
type CreateRLActions interface {
	action(ctx context.Context, stmt string)
}

//CreateRoleAction define mail
type CreateRoleAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewCreateRoleAction return action.mail
func NewCreateRoleAction(mail *mail.Server) *CreateRoleAction {
	return &CreateRoleAction{
		mail: mail,
	}
}

// send mails
func (cida *CreateRoleAction) action(ctx context.Context, stmt string) {
	if err := cida.mail.Send(ctx, "Audit: Create role", stmt); err != nil {
		log.Errorf(ctx, "got error when do Create role action, err:%s", err)
	}
}
