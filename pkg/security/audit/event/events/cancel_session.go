package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// CancelSessionMetric is the metric when create statistics event occurs.
type CancelSessionMetric struct {
	ctx     context.Context
	metrics *CancelSessionMetrics
	rules   []CancelSessionRules
}

//CancelSessionMetrics contain CatchCount
type CancelSessionMetrics struct {
	CatchCount *metric.Counter
}

//NewCancelSessionMetric init a new metric
func NewCancelSessionMetric(ctx context.Context, mail *mail.Server) *CancelSessionMetric {
	metrics := &CancelSessionMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.session.cancel",
				Help:        "session canceled",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &CancelSessionMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []CancelSessionRules{
			NewCancelSessionRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *CancelSessionMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *CancelSessionMetric) Metric(info interface{}) {

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

// CancelSessionRules define rules for metrics from CancelSessionMetric.
type CancelSessionRules interface {
	rule(ctx context.Context, metric *CancelSessionMetric, stmt string)
}

//CancelSessionRule get actions
type CancelSessionRule struct {
	actions []CancelSessionActions
}

//NewCancelSessionRule use action to send mails
// TODO(xz): NewCancelSessionRule should be changed with change of rule()
func NewCancelSessionRule(mail *mail.Server) *CancelSessionRule {
	return &CancelSessionRule{
		actions: []CancelSessionActions{
			NewCancelSessionAction(mail),
		},
	}
}

func (csr *CancelSessionRule) rule(ctx context.Context, metric *CancelSessionMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// CancelSessionActions define actions to face rules before.
type CancelSessionActions interface {
	action(ctx context.Context, stmt string)
}

//CancelSessionAction define mail
type CancelSessionAction struct {
	mail *mail.Server
}

//NewCancelSessionAction use action to send mails
// TODO(XZ): NewCancelSessionAction should be changed with change of action()
func NewCancelSessionAction(mail *mail.Server) *CancelSessionAction {
	return &CancelSessionAction{
		mail: mail,
	}
}

func (csa *CancelSessionAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: Cancel sessions", stmt); err != nil {
		log.Errorf(ctx, "got error when do Cancel sessions action, err:%s", err)
	}
}
