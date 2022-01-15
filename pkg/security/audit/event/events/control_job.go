package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// ControlJobsMetric is the metric when create statistics event occurs.
type ControlJobsMetric struct {
	ctx     context.Context
	metrics *ControlJobsMetrics
	rules   []ControlJobsRules
}

//ControlJobsMetrics contain CatchCount
type ControlJobsMetrics struct {
	CatchCount *metric.Counter
}

//NewControlJobsMetric init a new metric
func NewControlJobsMetric(ctx context.Context, mail *mail.Server) *ControlJobsMetric {
	metrics := &ControlJobsMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.job.control",
				Help:        "Control Job",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &ControlJobsMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []ControlJobsRules{
			NewControlJobsRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *ControlJobsMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *ControlJobsMetric) Metric(info interface{}) {

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

// ControlJobsRules define rules for metrics from ControlJobsMetric.
type ControlJobsRules interface {
	rule(ctx context.Context, metric *ControlJobsMetric, stmt string)
}

//ControlJobsRule get actions
type ControlJobsRule struct {
	actions []ControlJobsActions
}

//NewControlJobsRule use action to send mails
// TODO(xz): NewControlJobsRule should be changed with change of rule()
func NewControlJobsRule(mail *mail.Server) *ControlJobsRule {
	return &ControlJobsRule{
		actions: []ControlJobsActions{
			NewControlJobsAction(mail),
		},
	}
}

func (csr *ControlJobsRule) rule(ctx context.Context, metric *ControlJobsMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// ControlJobsActions define actions to face rules before.
type ControlJobsActions interface {
	action(ctx context.Context, stmt string)
}

//ControlJobsAction define mail
type ControlJobsAction struct {
	mail *mail.Server
}

//NewControlJobsAction return action.mail
// TODO(XZ): NewControlJobsAction should be changed with change of action()
func NewControlJobsAction(mail *mail.Server) *ControlJobsAction {
	return &ControlJobsAction{
		mail: mail,
	}
}

func (csa *ControlJobsAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: Control job", stmt); err != nil {
		log.Errorf(ctx, "got error when do Control job action, err:%s", err)
	}
}
