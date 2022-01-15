package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropFunctionMetric is the metric when drop statistics event occurs.
type DropFunctionMetric struct {
	ctx     context.Context
	metrics *DropFunctionMetrics
	rules   []DropFunctionRules
}

//DropFunctionMetrics contain CatchCount
type DropFunctionMetrics struct {
	CatchCount *metric.Counter
}

//NewDropFunctionMetric init a new metric
func NewDropFunctionMetric(ctx context.Context, mail *mail.Server) *DropFunctionMetric {
	metrics := &DropFunctionMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.function.drop",
				Help:        "A function drop",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &DropFunctionMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropFunctionRules{
			NewDropFunctionRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *DropFunctionMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *DropFunctionMetric) Metric(info interface{}) {

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

//DropFunctionRules define rules for metrics from dropfunctionMetric.
type DropFunctionRules interface {
	rule(ctx context.Context, metric *DropFunctionMetric, stmt string)
}

//DropFunctionRule get actions
type DropFunctionRule struct {
	actions []DropFunctionActions
}

//NewDropFunctionRule use action to send mails
// TODO(xz): NewDropFunctionRule should be changed with change of rule()
func NewDropFunctionRule(mail *mail.Server) *DropFunctionRule {
	return &DropFunctionRule{
		actions: []DropFunctionActions{
			NewDropFunctionAction(mail),
		},
	}
}

func (csr *DropFunctionRule) rule(ctx context.Context, metric *DropFunctionMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

//DropFunctionActions define actions to face rules before.
type DropFunctionActions interface {
	action(ctx context.Context, stmt string)
}

//DropFunctionAction define mail
type DropFunctionAction struct {
	mail *mail.Server
}

//NewDropFunctionAction return action.mail
// TODO(XZ): NewDropFunctionAction should be changed with change of action()
func NewDropFunctionAction(mail *mail.Server) *DropFunctionAction {
	return &DropFunctionAction{
		mail: mail,
	}
}

func (csa *DropFunctionAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: Function drop", stmt); err != nil {
		log.Errorf(ctx, "got error when do Function drop action, err:%s", err)
	}
}
