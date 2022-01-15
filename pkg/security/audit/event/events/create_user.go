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

// CreateUserMetric is the metric when create statistics event occurs.
type CreateUserMetric struct {
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context
	metrics *CreateUserMetrics
	rules   []CreateUserRules
}

//CreateUserMetrics contain CatchCount
type CreateUserMetrics struct {
	CatchCounter *metric.Counter
}

//NewCreateUserMetric init a new metric
func NewCreateUserMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *CreateUserMetric {
	metrics := &CreateUserMetrics{
		CatchCounter: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.user.create",
				Help:        "Create user",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &CreateUserMetric{
		ie:      ie,
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateUserRules{
			NewCreateUserRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (cum *CreateUserMetric) RegistMetric() interface{} {
	return cum.metrics
}

//Metric check rules
func (cum *CreateUserMetric) Metric(info interface{}) {

	cum.metrics.CatchCounter.Inc(1)

	//	check all rules
	for _, rule := range cum.rules {
		switch info.(type) {
		case string:
			rule.rule(cum.ctx, cum, info.(string))
		}
	}
}

//EventLog define time to exec to do count
func (cum *CreateUserMetric) EventLog(ctx context.Context) (int, bool) {
	row, err := cum.ie.QueryRow(
		ctx,
		"Get Create Users Count In 5 Seconds",
		nil,
		"SELECT count(1) FROM eventlog WHERE \"eventType\" = $1 AND \"timestamp\" > $2",
		string(sql.EventLogCreateUser),
		timeutil.Now().Add(-createUserTime),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventLogCreateUser, err)
	}
	if row != nil {
		return int(*row[0].(*tree.DInt)), true
	}
	return 0, false
}

// CreateUserRules define rules for metrics from CreateUserMetric.
type CreateUserRules interface {
	rule(ctx context.Context, metric *CreateUserMetric, stmt string)
}

//CreateUserRule get actions
type CreateUserRule struct {
	actions []CreateUserActions
}

//NewCreateUserRule use action to send mails
// TODO(xz): NewCreateUserRule should be changed with change of rule()
func NewCreateUserRule(mail *mail.Server) *CreateUserRule {
	return &CreateUserRule{
		actions: []CreateUserActions{
			NewCreateUserAction(mail),
		},
	}
}

var createUserTime = 5 * time.Second
var createUserFrequency = 10

func (cur *CreateUserRule) rule(ctx context.Context, metric *CreateUserMetric, stmt string) {
	frequency, ok := metric.EventLog(ctx)
	if ok {
		if frequency > createUserFrequency {
			for _, action := range cur.actions {
				action.action(ctx, stmt)
			}
		}
	}
}

// CreateUserActions define actions to face rules before.
type CreateUserActions interface {
	action(ctx context.Context, stmt string)
}

//CreateUserAction define mail
type CreateUserAction struct {
	mail *mail.Server
}

//NewCreateUserAction return action.mail
// TODO(XZ): NewCreateUserAction should be changed with change of action()
func NewCreateUserAction(mail *mail.Server) *CreateUserAction {
	return &CreateUserAction{
		mail: mail,
	}
}

func (cua *CreateUserAction) action(ctx context.Context, stmt string) {
	if err := cua.mail.Send(ctx, "Audit: Create user", stmt); err != nil {
		log.Errorf(ctx, "got error when do Create user action, err:%s", err)
	}
}
