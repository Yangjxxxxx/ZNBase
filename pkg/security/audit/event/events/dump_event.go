package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// EventBackupMetric is the metric when create statistics event occurs.
type EventBackupMetric struct {
	ctx     context.Context
	metrics *EventBackupMetrics
	rules   []EventBackupRules
}

//EventBackupMetrics contain CatchCount
type EventBackupMetrics struct {
	CatchCount *metric.Counter
}

//NewEventBackupMetric init a new metric
func NewEventBackupMetric(ctx context.Context, mail *mail.Server) *EventBackupMetric {
	metrics := &EventBackupMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.event.backup",
				Help:        "A event backup",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &EventBackupMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []EventBackupRules{
			NewEventBackupRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *EventBackupMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *EventBackupMetric) Metric(info interface{}) {

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

// EventBackupRules define rules for metrics from EventBackupMetric.
type EventBackupRules interface {
	rule(ctx context.Context, metric *EventBackupMetric, stmt string)
}

//EventBackupRule get actions
type EventBackupRule struct {
	actions []EventBackupActions
}

//NewEventBackupRule use action to send mails
// TODO(xz): NewEventBackupRule should be changed with change of rule()
func NewEventBackupRule(mail *mail.Server) *EventBackupRule {
	return &EventBackupRule{
		actions: []EventBackupActions{
			NewEventBackupAction(mail),
		},
	}
}

func (csr *EventBackupRule) rule(ctx context.Context, metric *EventBackupMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// EventBackupActions define actions to face rules before.
type EventBackupActions interface {
	action(ctx context.Context, stmt string)
}

//EventBackupAction define mail
type EventBackupAction struct {
	mail *mail.Server
}

//NewEventBackupAction return action.mail
// TODO(XZ): NewEventBackupAction should be changed with change of action()
func NewEventBackupAction(mail *mail.Server) *EventBackupAction {
	return &EventBackupAction{
		mail: mail,
	}
}

func (csa *EventBackupAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: User Logout", stmt); err != nil {
		log.Errorf(ctx, "got error when do User Logout action, err:%s", err)
	}
}
