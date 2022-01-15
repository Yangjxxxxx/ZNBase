package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// DropUserMetric is the metric when create statistics event occurs.
type DropUserMetric struct {
	ctx     context.Context
	metrics *DropUserMetrics
	rules   []DropUserRules
}

//DropUserMetrics  contain CatchCount
type DropUserMetrics struct {
	CatchCount *metric.Counter
}

//NewDropUserMetric init a new metric
func NewDropUserMetric(ctx context.Context, mail *mail.Server) *DropUserMetric {
	metrics := &DropUserMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.user.drop",
				Help:        "Drop user",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &DropUserMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropUserRules{
			NewDropUserRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (dum *DropUserMetric) RegistMetric() interface{} {
	return dum.metrics
}

//Metric check rules
func (dum *DropUserMetric) Metric(info interface{}) {

	//	increment by 1
	dum.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range dum.rules {
		switch info.(type) {
		case string:
			rule.rule(dum.ctx, dum, info.(string))
		}
	}
}

// DropUserRules define rules for metrics from DropUserMetric.
type DropUserRules interface {
	rule(ctx context.Context, metric *DropUserMetric, stmt string)
}

//DropUserRule  get actions
type DropUserRule struct {
	actions []DropUserActions
}

//NewDropUserRule use action to send mails
// TODO(xz): NewDropUserRule should be changed with change of rule()
func NewDropUserRule(mail *mail.Server) *DropUserRule {
	return &DropUserRule{
		actions: []DropUserActions{
			NewDropUserAction(mail),
		},
	}
}

func (dur *DropUserRule) rule(ctx context.Context, metric *DropUserMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range dur.actions {
			action.action(ctx, stmt)
		}
	}
}

// DropUserActions define actions to face rules before.
type DropUserActions interface {
	action(ctx context.Context, stmt string)
}

//DropUserAction define mail
type DropUserAction struct {
	mail *mail.Server
}

//NewDropUserAction return action.mail
// TODO(XZ): NewDropUserAction should be changed with change of action()
func NewDropUserAction(mail *mail.Server) *DropUserAction {
	return &DropUserAction{
		mail: mail,
	}
}

func (dua *DropUserAction) action(ctx context.Context, stmt string) {
	if err := dua.mail.Send(ctx, "Audit: Drop user", stmt); err != nil {
		log.Errorf(ctx, "got error when do drop user action, err:%s", err)
	}
}
