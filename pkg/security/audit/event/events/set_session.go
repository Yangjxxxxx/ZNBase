package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// SetSessionVarMetric is the metric when create statistics event occurs.
type SetSessionVarMetric struct {
	ctx     context.Context
	metrics *SetSessionVarMetrics
	rules   []SetSessionVarRules
}

//SetSessionVarMetrics contain CatchCount
type SetSessionVarMetrics struct {
	CatchCount *metric.Counter
}

//NewSetSessionVarMetric init a new metric
func NewSetSessionVarMetric(ctx context.Context, mail *mail.Server) *SetSessionVarMetric {
	metrics := &SetSessionVarMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.session.set",
				Help:        "Set Session",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &SetSessionVarMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []SetSessionVarRules{
			NewSetSessionVarRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *SetSessionVarMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *SetSessionVarMetric) Metric(info interface{}) {

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

// SetSessionVarRules define rules for metrics from SetSessionVarMetric.
type SetSessionVarRules interface {
	rule(ctx context.Context, metric *SetSessionVarMetric, stmt string)
}

//SetSessionVarRule get actions
type SetSessionVarRule struct {
	actions []SetSessionVarActions
}

//NewSetSessionVarRule use action to send mails
// TODO(xz): NewSetSessionVarRule should be changed with change of rule()
func NewSetSessionVarRule(mail *mail.Server) *SetSessionVarRule {
	return &SetSessionVarRule{
		actions: []SetSessionVarActions{
			NewSetSessionVarAction(mail),
		},
	}
}

func (csr *SetSessionVarRule) rule(ctx context.Context, metric *SetSessionVarMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 10 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// SetSessionVarActions define actions to face rules before.
type SetSessionVarActions interface {
	action(ctx context.Context, stmt string)
}

//SetSessionVarAction define mail
type SetSessionVarAction struct {
	mail *mail.Server
}

//NewSetSessionVarAction return action.mail
// TODO(XZ): NewSetSessionVarAction should be changed with change of action()
func NewSetSessionVarAction(mail *mail.Server) *SetSessionVarAction {
	return &SetSessionVarAction{
		mail: mail,
	}
}

func (csa *SetSessionVarAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: Set Session", stmt); err != nil {
		log.Errorf(ctx, "got error when do Set Session action, err:%s", err)
	}
}
