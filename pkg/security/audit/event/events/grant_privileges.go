package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// GrantPrivilegesMetric is the metric when create statistics event occurs.
type GrantPrivilegesMetric struct {
	ctx     context.Context
	metrics *GrantPrivilegesMetrics
	rules   []GrantPrivilegesRules
}

//GrantPrivilegesMetrics contain CatchCount
type GrantPrivilegesMetrics struct {
	CatchCount *metric.Counter
}

//NewGrantPrivilegesMetric init a new metric
func NewGrantPrivilegesMetric(ctx context.Context, mail *mail.Server) *GrantPrivilegesMetric {
	metrics := &GrantPrivilegesMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.privileges.grant",
				Help:        "grant privileges",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &GrantPrivilegesMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []GrantPrivilegesRules{
			NewGrantPrivilegesRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (GP *GrantPrivilegesMetric) RegistMetric() interface{} {
	return GP.metrics
}

//Metric check rules
func (GP *GrantPrivilegesMetric) Metric(info interface{}) {

	//	increment by 1
	GP.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range GP.rules {
		switch info.(type) {
		case string:
			rule.rule(GP.ctx, GP, info.(string))
		}
	}
}

// GrantPrivilegesRules define rules for metrics from GrantPrivilegesMetric.
type GrantPrivilegesRules interface {
	rule(ctx context.Context, metric *GrantPrivilegesMetric, stmt string)
}

//GrantPrivilegesRule get actions
type GrantPrivilegesRule struct {
	actions []GrantPrivilegesActions
}

//NewGrantPrivilegesRule use action to send mails
// TODO(xz): NewGrantPrivilegesRule should be changed with change of rule()
func NewGrantPrivilegesRule(mail *mail.Server) *GrantPrivilegesRule {
	return &GrantPrivilegesRule{
		actions: []GrantPrivilegesActions{
			NewGrantPrivilegesAction(mail),
		},
	}
}

func (GP *GrantPrivilegesRule) rule(
	ctx context.Context, metric *GrantPrivilegesMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range GP.actions {
			action.action(ctx, stmt)
		}
	}
}

// GrantPrivilegesActions define actions to face rules before.
type GrantPrivilegesActions interface {
	action(ctx context.Context, stmt string)
}

//GrantPrivilegesAction define mail
type GrantPrivilegesAction struct {
	mail *mail.Server
}

//NewGrantPrivilegesAction return action.mail
// TODO(XZ): NewGrantPrivilegesAction should be changed with change of action()
func NewGrantPrivilegesAction(mail *mail.Server) *GrantPrivilegesAction {
	return &GrantPrivilegesAction{
		mail: mail,
	}
}

func (dua *GrantPrivilegesAction) action(ctx context.Context, stmt string) {
	if err := dua.mail.Send(ctx, "Audit: Grant privileges", stmt); err != nil {
		log.Errorf(ctx, "got error when do Grant privileges action, err:%s", err)
	}
}
