package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//CallFunctionMetric is the metric when call statistics event occurs.
type CallFunctionMetric struct {
	ctx     context.Context
	metrics *CallFunctionMetrics
	rules   []CallFunctionRules
}

//CallFunctionMetrics contain CatchCount
type CallFunctionMetrics struct {
	CatchCount *metric.Counter
}

//NewCallFunctionMetric init a new metric
func NewCallFunctionMetric(ctx context.Context, mail *mail.Server) *CallFunctionMetric {
	metrics := &CallFunctionMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.function.call",
				Help:        "A function call",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &CallFunctionMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []CallFunctionRules{
			NewCallFunctionRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *CallFunctionMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *CallFunctionMetric) Metric(info interface{}) {

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

//CallFunctionRules define rules for metrics from CallFunctionMetric.
type CallFunctionRules interface {
	rule(ctx context.Context, metric *CallFunctionMetric, stmt string)
}

//CallFunctionRule get actions
type CallFunctionRule struct {
	actions []CallFunctionActions
}

//NewCallFunctionRule use action to send mails
// TODO(xz): NewCallFunctionRule should be changed with change of rule()
func NewCallFunctionRule(mail *mail.Server) *CallFunctionRule {
	return &CallFunctionRule{
		actions: []CallFunctionActions{
			NewCallFunctionAction(mail),
		},
	}
}

func (csr *CallFunctionRule) rule(ctx context.Context, metric *CallFunctionMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

//CallFunctionActions define actions to face rules before.
type CallFunctionActions interface {
	action(ctx context.Context, stmt string)
}

//CallFunctionAction define mail
type CallFunctionAction struct {
	mail *mail.Server
}

//NewCallFunctionAction return action.mail
// TODO(XZ): NewCallFunctionAction should be changed with change of action()
func NewCallFunctionAction(mail *mail.Server) *CallFunctionAction {
	return &CallFunctionAction{
		mail: mail,
	}
}

func (csa *CallFunctionAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: Function call", stmt); err != nil {
		log.Errorf(ctx, "got error when do Function call action, err:%s", err)
	}
}
