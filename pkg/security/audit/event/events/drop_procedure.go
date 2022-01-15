package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropProcedureMetric is the metric when drop statistics event occurs.
type DropProcedureMetric struct {
	ctx     context.Context
	metrics *DropProcedureMetrics
	rules   []DropProcedureRules
}

//DropProcedureMetrics contain CatchCount
type DropProcedureMetrics struct {
	CatchCount *metric.Counter
}

//NewDropProcedureMetric init a new metric
func NewDropProcedureMetric(ctx context.Context, mail *mail.Server) *DropProcedureMetric {
	metrics := &DropProcedureMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.procedure.drop",
				Help:        "A procedure drop",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &DropProcedureMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropProcedureRules{
			NewDropProcedureRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *DropProcedureMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *DropProcedureMetric) Metric(info interface{}) {

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

//DropProcedureRules define rules for metrics from dropprocedureMetric.
type DropProcedureRules interface {
	rule(ctx context.Context, metric *DropProcedureMetric, stmt string)
}

//DropProcedureRule get actions
type DropProcedureRule struct {
	actions []DropProcedureActions
}

//NewDropProcedureRule use action to send mails
// TODO(xz): NewDropProcedureRule should be changed with change of rule()
func NewDropProcedureRule(mail *mail.Server) *DropProcedureRule {
	return &DropProcedureRule{
		actions: []DropProcedureActions{
			NewDropProcedureAction(mail),
		},
	}
}

func (csr *DropProcedureRule) rule(ctx context.Context, metric *DropProcedureMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

//DropProcedureActions define actions to face rules before.
type DropProcedureActions interface {
	action(ctx context.Context, stmt string)
}

//DropProcedureAction define mail
type DropProcedureAction struct {
	mail *mail.Server
}

//NewDropProcedureAction return action.mail
// TODO(XZ): NewDropProcedureAction should be changed with change of action()
func NewDropProcedureAction(mail *mail.Server) *DropProcedureAction {
	return &DropProcedureAction{
		mail: mail,
	}
}

func (csa *DropProcedureAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: procedure drop", stmt); err != nil {
		log.Errorf(ctx, "got error when do procedure drop action, err:%s", err)
	}
}
