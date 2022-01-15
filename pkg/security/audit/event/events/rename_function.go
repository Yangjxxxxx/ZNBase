package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//RenameFunctionMetric is the metric when rename statistics event occurs.
type RenameFunctionMetric struct {
	ctx     context.Context
	metrics *RenameFunctionMetrics
	rules   []RenameFunctionRules
}

//RenameFunctionMetrics contain CatchCount
type RenameFunctionMetrics struct {
	CatchCount *metric.Counter
}

//NewRenameFunctionMetric init a new metric
func NewRenameFunctionMetric(ctx context.Context, mail *mail.Server) *RenameFunctionMetric {
	metrics := &RenameFunctionMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.function.rename",
				Help:        "A function rename",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &RenameFunctionMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []RenameFunctionRules{
			NewRenameFunctionRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *RenameFunctionMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *RenameFunctionMetric) Metric(info interface{}) {

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

//RenameFunctionRules define rules for metrics from renamefunctionMetric.
type RenameFunctionRules interface {
	rule(ctx context.Context, metric *RenameFunctionMetric, stmt string)
}

//RenameFunctionRule get actions
type RenameFunctionRule struct {
	actions []RenameFunctionActions
}

//NewRenameFunctionRule use action to send mails
// TODO(xz): NewRenameFunctionRule should be changed with change of rule()
func NewRenameFunctionRule(mail *mail.Server) *RenameFunctionRule {
	return &RenameFunctionRule{
		actions: []RenameFunctionActions{
			NewRenameFunctionAction(mail),
		},
	}
}

func (csr *RenameFunctionRule) rule(
	ctx context.Context, metric *RenameFunctionMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

//RenameFunctionActions define actions to face rules before.
type RenameFunctionActions interface {
	action(ctx context.Context, stmt string)
}

//RenameFunctionAction define mail
type RenameFunctionAction struct {
	mail *mail.Server
}

//NewRenameFunctionAction return action.mail
// TODO(XZ): NewRenameFunctionAction should be changed with change of action()
func NewRenameFunctionAction(mail *mail.Server) *RenameFunctionAction {
	return &RenameFunctionAction{
		mail: mail,
	}
}

func (csa *RenameFunctionAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: Function rename", stmt); err != nil {
		log.Errorf(ctx, "got error when do Function rename action, err:%s", err)
	}
}
