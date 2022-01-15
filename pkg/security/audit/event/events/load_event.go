package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// EventRestoreMetric is the metric when create statistics event occurs.
type EventRestoreMetric struct {
	ctx     context.Context
	metrics *EventRestoreMetrics
	rules   []EventRestoreRules
}

//EventRestoreMetrics contain CatchCount
type EventRestoreMetrics struct {
	CatchCount *metric.Counter
}

//NewEventRestoreMetric init a new metric
func NewEventRestoreMetric(ctx context.Context, mail *mail.Server) *EventRestoreMetric {
	metrics := &EventRestoreMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.event.Restore",
				Help:        "A event Restore",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &EventRestoreMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []EventRestoreRules{
			NewEventRestoreRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *EventRestoreMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *EventRestoreMetric) Metric(info interface{}) {

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

// EventRestoreRules define rules for metrics from EventRestoreMetric.
type EventRestoreRules interface {
	rule(ctx context.Context, metric *EventRestoreMetric, stmt string)
}

//EventRestoreRule get actions
type EventRestoreRule struct {
	actions []EventRestoreActions
}

//NewEventRestoreRule use action to send mails
// TODO(xz): NewEventRestoreRule should be changed with change of rule()
func NewEventRestoreRule(mail *mail.Server) *EventRestoreRule {
	return &EventRestoreRule{
		actions: []EventRestoreActions{
			NewEventRestoreAction(mail),
		},
	}
}

func (csr *EventRestoreRule) rule(ctx context.Context, metric *EventRestoreMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// EventRestoreActions define actions to face rules before.
type EventRestoreActions interface {
	action(ctx context.Context, stmt string)
}

//EventRestoreAction define mail
type EventRestoreAction struct {
	mail *mail.Server
}

//NewEventRestoreAction return action.mail
// TODO(XZ): NewEventRestoreAction should be changed with change of action()
func NewEventRestoreAction(mail *mail.Server) *EventRestoreAction {
	return &EventRestoreAction{
		mail: mail,
	}
}

func (csa *EventRestoreAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: User Logout", stmt); err != nil {
		log.Errorf(ctx, "got error when do User Logout action, err:%s", err)
	}
}
