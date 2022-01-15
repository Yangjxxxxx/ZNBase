package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// UserLogoutMetric is the metric when create statistics event occurs.
type UserLogoutMetric struct {
	ctx     context.Context
	metrics *UserLogoutMetrics
	rules   []UserLogoutRules
}

//UserLogoutMetrics contain CatchCount
type UserLogoutMetrics struct {
	CatchCount *metric.Counter
}

//NewUserLogoutMetric init a new metric
func NewUserLogoutMetric(ctx context.Context, mail *mail.Server) *UserLogoutMetric {
	metrics := &UserLogoutMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.user.logout",
				Help:        "A user logout",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &UserLogoutMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []UserLogoutRules{
			NewUserLogoutRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *UserLogoutMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *UserLogoutMetric) Metric(info interface{}) {

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

// UserLogoutRules define rules for metrics from UserLogoutMetric.
type UserLogoutRules interface {
	rule(ctx context.Context, metric *UserLogoutMetric, stmt string)
}

//UserLogoutRule get actions
type UserLogoutRule struct {
	actions []UserLogoutActions
}

//NewUserLogoutRule use action to send mails
// TODO(xz): NewUserLogoutRule should be changed with change of rule()
func NewUserLogoutRule(mail *mail.Server) *UserLogoutRule {
	return &UserLogoutRule{
		actions: []UserLogoutActions{
			NewUserLogoutAction(mail),
		},
	}
}

func (csr *UserLogoutRule) rule(ctx context.Context, metric *UserLogoutMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// UserLogoutActions define actions to face rules before.
type UserLogoutActions interface {
	action(ctx context.Context, stmt string)
}

//UserLogoutAction define mail
type UserLogoutAction struct {
	mail *mail.Server
}

//NewUserLogoutAction return action.mail
// TODO(XZ): NewUserLogoutAction should be changed with change of action()
func NewUserLogoutAction(mail *mail.Server) *UserLogoutAction {
	return &UserLogoutAction{
		mail: mail,
	}
}

func (csa *UserLogoutAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: User Logout", stmt); err != nil {
		log.Errorf(ctx, "got error when do User Logout action, err:%s", err)
	}
}
