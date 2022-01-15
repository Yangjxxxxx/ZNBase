package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// AlterUserMetric is the metric when create statistics event ocaurs.
type AlterUserMetric struct {
	ctx     context.Context
	metrics *AlterUserMetrics
	rules   []AlterUserRules
}

//AlterUserMetrics contain CatchCount
type AlterUserMetrics struct {
	CatchCount *metric.Counter
}

//NewAlterUserMetric init a new metric
func NewAlterUserMetric(ctx context.Context, mail *mail.Server) *AlterUserMetric {
	metrics := &AlterUserMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.user.alter",
				Help:        "Alter user",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &AlterUserMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []AlterUserRules{
			NewAlterUserRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (aum *AlterUserMetric) RegistMetric() interface{} {
	return aum.metrics
}

//Metric check rules
func (aum *AlterUserMetric) Metric(info interface{}) {

	//	increment by 1
	aum.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range aum.rules {
		switch info.(type) {
		case string:
			rule.rule(aum.ctx, aum, info.(string))
		}
	}
}

// AlterUserRules define rules for metrics from AlterUserMetric.
type AlterUserRules interface {
	rule(ctx context.Context, metric *AlterUserMetric, stmt string)
}

//AlterUserRule get actions
type AlterUserRule struct {
	actions []AlterUserActions
}

// NewAlterUserRule get actions
// TODO(xz): NewAlterUserRule should be changed with change of rule()
func NewAlterUserRule(mail *mail.Server) *AlterUserRule {
	return &AlterUserRule{
		actions: []AlterUserActions{
			NewAlterUserAction(mail),
		},
	}
}

func (aur *AlterUserRule) rule(ctx context.Context, metric *AlterUserMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range aur.actions {
			action.action(ctx, stmt)
		}
	}
}

// AlterUserActions define actions to face rules before.
type AlterUserActions interface {
	action(ctx context.Context, stmt string)
}

//AlterUserAction define mail
type AlterUserAction struct {
	mail *mail.Server
}

// NewAlterUserAction return action.mail
// TODO(XZ): NewAlterUserAction should be changed with change of action()
func NewAlterUserAction(mail *mail.Server) *AlterUserAction {
	return &AlterUserAction{
		mail: mail,
	}
}

func (aua *AlterUserAction) action(ctx context.Context, stmt string) {
	if err := aua.mail.Send(ctx, "Audit: Alter User", stmt); err != nil {
		log.Errorf(ctx, "got error when do Alter User action, err:%s", err)
	}
}
