package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// SQLInjectMetric is the metric when create statistics event occurs.
type SQLInjectMetric struct {
	ctx     context.Context
	metrics *SQLInjectMetrics
	rules   []SQLInjectRules
}

//SQLInjectMetrics contain CatchCount
type SQLInjectMetrics struct {
	CatchCount *metric.Counter
}

//NewSQLInjectMetric init a new metric
func NewSQLInjectMetric(ctx context.Context, mail *mail.Server) *SQLInjectMetric {
	metrics := &SQLInjectMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.sql.inject",
				Help:        "SQL injection happened",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &SQLInjectMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []SQLInjectRules{
			NewSQLInjectRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *SQLInjectMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *SQLInjectMetric) Metric(info interface{}) {

	//	increment by 1
	csm.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range csm.rules {
		switch info.(type) {
		case string:
			rule.rule(csm.ctx, csm, info.(string))
		}
	}
}

// SQLInjectRules define rules for metrics from SQLInjectMetric.
type SQLInjectRules interface {
	rule(ctx context.Context, metric *SQLInjectMetric, stmt string)
}

//SQLInjectRule get actions
type SQLInjectRule struct {
	actions []SQLInjectActions
}

//NewSQLInjectRule use action to send mails
// TODO(xz): NewSQLInjectRule should be changed with change of rule()
func NewSQLInjectRule(mail *mail.Server) *SQLInjectRule {
	return &SQLInjectRule{
		actions: []SQLInjectActions{
			NewSQLInjectAction(mail),
		},
	}
}

func (csr *SQLInjectRule) rule(ctx context.Context, metric *SQLInjectMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// SQLInjectActions define actions to face rules before.
type SQLInjectActions interface {
	action(ctx context.Context, stmt string)
}

//SQLInjectAction define mail
type SQLInjectAction struct {
	mail *mail.Server
}

//NewSQLInjectAction return action.mail
// TODO(XZ): NewSQLInjectAction should be changed with change of action()
func NewSQLInjectAction(mail *mail.Server) *SQLInjectAction {
	return &SQLInjectAction{
		mail: mail,
	}
}

func (csa *SQLInjectAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: SQL Inject", stmt); err != nil {
		log.Errorf(ctx, "got error when do SQL Inject action, err:%s", err)
	}
}
