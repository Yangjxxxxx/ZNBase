package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//CreateFunctionMetric is the metric when create statistics event occurs.
type CreateFunctionMetric struct {
	ctx     context.Context
	metrics *CreateFunctionMetrics
	rules   []CreateFunctionRules
}

//CreateFunctionMetrics contain CatchCount
type CreateFunctionMetrics struct {
	CatchCount *metric.Counter
}

//NewCreateFunctionMetric init a new metric
func NewCreateFunctionMetric(ctx context.Context, mail *mail.Server) *CreateFunctionMetric {
	metrics := &CreateFunctionMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.function.create",
				Help:        "A function create",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &CreateFunctionMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateFunctionRules{
			NewCreateFunctionRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *CreateFunctionMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *CreateFunctionMetric) Metric(info interface{}) {

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

//CreateFunctionRules define rules for metrics from createfunctionMetric.
type CreateFunctionRules interface {
	rule(ctx context.Context, metric *CreateFunctionMetric, stmt string)
}

//CreateFunctionRule get actions
type CreateFunctionRule struct {
	actions []CreateFunctionActions
}

//NewCreateFunctionRule use action to send mails
// TODO(xz): NewCreateFunctionRule should be changed with change of rule()
func NewCreateFunctionRule(mail *mail.Server) *CreateFunctionRule {
	return &CreateFunctionRule{
		actions: []CreateFunctionActions{
			NewCreateFunctionAction(mail),
		},
	}
}

func (csr *CreateFunctionRule) rule(
	ctx context.Context, metric *CreateFunctionMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

//CreateFunctionActions define actions to face rules before.
type CreateFunctionActions interface {
	action(ctx context.Context, stmt string)
}

//CreateFunctionAction define mail
type CreateFunctionAction struct {
	mail *mail.Server
}

//NewCreateFunctionAction return action.mail
// TODO(XZ): NewCreateFunctionAction should be changed with change of action()
func NewCreateFunctionAction(mail *mail.Server) *CreateFunctionAction {
	return &CreateFunctionAction{
		mail: mail,
	}
}

func (csa *CreateFunctionAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: Function create", stmt); err != nil {
		log.Errorf(ctx, "got error when do Function create action, err:%s", err)
	}
}
