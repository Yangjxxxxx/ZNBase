package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// UserLoginMetric is the metric when create statistics event occurs.
type UserLoginMetric struct {
	ctx     context.Context
	metrics *UserLoginMetrics
	rules   []UserLoginRules
}

//UserLoginMetrics contain CatchCount
type UserLoginMetrics struct {
	CatchCount *metric.Counter
}

//NewUserLoginMetric init a new metric
func NewUserLoginMetric(ctx context.Context, mail *mail.Server) *UserLoginMetric {
	metrics := &UserLoginMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.user.login",
				Help:        "A user login",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &UserLoginMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []UserLoginRules{
			NewUserLoginRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *UserLoginMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *UserLoginMetric) Metric(info interface{}) {

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

// UserLoginRules define rules for metrics from UserLoginMetric.
type UserLoginRules interface {
	rule(ctx context.Context, metric *UserLoginMetric, stmt string)
}

//UserLoginRule get actions
type UserLoginRule struct {
	actions []UserLoginActions
}

//NewUserLoginRule use action to send mails
// TODO(xz): NewUserLoginRule should be changed with change of rule()
func NewUserLoginRule(mail *mail.Server) *UserLoginRule {
	return &UserLoginRule{
		actions: []UserLoginActions{
			NewUserLoginAction(mail),
		},
	}
}

func (csr *UserLoginRule) rule(ctx context.Context, metric *UserLoginMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// UserLoginActions define actions to face rules before.
type UserLoginActions interface {
	action(ctx context.Context, stmt string)
}

//UserLoginAction define mail
type UserLoginAction struct {
	mail *mail.Server
}

//NewUserLoginAction return action.mail
// TODO(XZ): NewUserLoginAction should be changed with change of action()
func NewUserLoginAction(mail *mail.Server) *UserLoginAction {
	return &UserLoginAction{
		mail: mail,
	}
}

func (csa *UserLoginAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: User Login", stmt); err != nil {
		log.Errorf(ctx, "got error when do User Login action, err:%s", err)
	}
}
