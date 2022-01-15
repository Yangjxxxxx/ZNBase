package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// CancelQueriesMetric is the metric when create statistics event occurs.
type CancelQueriesMetric struct {
	ctx     context.Context
	metrics *CancelQueriesMetrics
	rules   []CancelQueriesRules
}

//CancelQueriesMetrics contain CatchCount
type CancelQueriesMetrics struct {
	CatchCount *metric.Counter
}

//NewCancelQueriesMetric init a new metric
func NewCancelQueriesMetric(ctx context.Context, mail *mail.Server) *CancelQueriesMetric {
	metrics := &CancelQueriesMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.queries.cancel",
				Help:        "Queries canceled",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &CancelQueriesMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []CancelQueriesRules{
			NewCancelQueriesRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *CancelQueriesMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *CancelQueriesMetric) Metric(info interface{}) {

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

// CancelQueriesRules define rules for metrics from CancelQueriesMetric.
type CancelQueriesRules interface {
	rule(ctx context.Context, metric *CancelQueriesMetric, stmt string)
}

//CancelQueriesRule get actions
type CancelQueriesRule struct {
	actions []CancelQueriesActions
}

//NewCancelQueriesRule return action.mail
// TODO(xz): NewCancelQueriesRule should be changed with change of rule()
func NewCancelQueriesRule(mail *mail.Server) *CancelQueriesRule {
	return &CancelQueriesRule{
		actions: []CancelQueriesActions{
			NewCancelQueriesAction(mail),
		},
	}
}

func (csr *CancelQueriesRule) rule(ctx context.Context, metric *CancelQueriesMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

// CancelQueriesActions define actions to face rules before.
type CancelQueriesActions interface {
	action(ctx context.Context, stmt string)
}

//CancelQueriesAction define mail
type CancelQueriesAction struct {
	mail *mail.Server
}

//NewCancelQueriesAction return action.mail
// TODO(XZ): NewCancelQueriesAction should be changed with change of action()
func NewCancelQueriesAction(mail *mail.Server) *CancelQueriesAction {
	return &CancelQueriesAction{
		mail: mail,
	}
}

func (csa *CancelQueriesAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: Cancel queries", stmt); err != nil {
		log.Errorf(ctx, "got error when do Cancel queries action, err:%s", err)
	}
}
