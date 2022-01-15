package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// RevokePrivilegesMetric is the metric when create statistics event occurs.
type RevokePrivilegesMetric struct {
	ctx     context.Context
	metrics *RevokePrivilegesMetrics
	rules   []RevokePrivilegesRules
}

//RevokePrivilegesMetrics contain CatchCount
type RevokePrivilegesMetrics struct {
	CatchCount *metric.Counter
}

//NewRevokePrivilegesMetric init a new metric
func NewRevokePrivilegesMetric(ctx context.Context, mail *mail.Server) *RevokePrivilegesMetric {
	metrics := &RevokePrivilegesMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.privileges.Revoke",
				Help:        "Revoke privileges",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &RevokePrivilegesMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []RevokePrivilegesRules{
			NewRevokePrivilegesRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (GP *RevokePrivilegesMetric) RegistMetric() interface{} {
	return GP.metrics
}

//Metric check rules
func (GP *RevokePrivilegesMetric) Metric(info interface{}) {

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

// RevokePrivilegesRules define rules for metrics from RevokePrivilegesMetric.
type RevokePrivilegesRules interface {
	rule(ctx context.Context, metric *RevokePrivilegesMetric, stmt string)
}

//RevokePrivilegesRule get actions
type RevokePrivilegesRule struct {
	actions []RevokePrivilegesActions
}

//NewRevokePrivilegesRule use action to send mails
// TODO(xz): NewRevokePrivilegesRule should be changed with change of rule()
func NewRevokePrivilegesRule(mail *mail.Server) *RevokePrivilegesRule {
	return &RevokePrivilegesRule{
		actions: []RevokePrivilegesActions{
			NewRevokePrivilegesAction(mail),
		},
	}
}

func (GP *RevokePrivilegesRule) rule(
	ctx context.Context, metric *RevokePrivilegesMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range GP.actions {
			action.action(ctx, stmt)
		}
	}
}

// RevokePrivilegesActions define actions to face rules before.
type RevokePrivilegesActions interface {
	action(ctx context.Context, stmt string)
}

//RevokePrivilegesAction define mail
type RevokePrivilegesAction struct {
	mail *mail.Server
}

//NewRevokePrivilegesAction return action.mail
// TODO(XZ): NewRevokePrivilegesAction should be changed with change of action()
func NewRevokePrivilegesAction(mail *mail.Server) *RevokePrivilegesAction {
	return &RevokePrivilegesAction{
		mail: mail,
	}
}

func (dua *RevokePrivilegesAction) action(ctx context.Context, stmt string) {
	if err := dua.mail.Send(ctx, "Audit: Revoke Privileges", stmt); err != nil {
		log.Errorf(ctx, "got error when do Revoke Privileges action, err:%s", err)
	}
}
