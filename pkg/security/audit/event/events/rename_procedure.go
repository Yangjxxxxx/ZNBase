package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//RenameProcedureMetric is the metric when rename statistics event occurs.
type RenameProcedureMetric struct {
	ctx     context.Context
	metrics *RenameProcedureMetrics
	rules   []RenameProcedureRules
}

//RenameProcedureMetrics contain CatchCount
type RenameProcedureMetrics struct {
	CatchCount *metric.Counter
}

//NewRenameProcedureMetric init a new metric
func NewRenameProcedureMetric(ctx context.Context, mail *mail.Server) *RenameProcedureMetric {
	metrics := &RenameProcedureMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.procedure.rename",
				Help:        "A procedure rename",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &RenameProcedureMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []RenameProcedureRules{
			NewRenameProcedureRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *RenameProcedureMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *RenameProcedureMetric) Metric(info interface{}) {

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

//RenameProcedureRules define rules for metrics from renameprocedureMetric.
type RenameProcedureRules interface {
	rule(ctx context.Context, metric *RenameProcedureMetric, stmt string)
}

//RenameProcedureRule get actions
type RenameProcedureRule struct {
	actions []RenameProcedureActions
}

//NewRenameProcedureRule use action to send mails
// TODO(xz): NewRenameProcedureRule should be changed with change of rule()
func NewRenameProcedureRule(mail *mail.Server) *RenameProcedureRule {
	return &RenameProcedureRule{
		actions: []RenameProcedureActions{
			NewRenameProcedureAction(mail),
		},
	}
}

func (csr *RenameProcedureRule) rule(
	ctx context.Context, metric *RenameProcedureMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

//RenameProcedureActions define actions to face rules before.
type RenameProcedureActions interface {
	action(ctx context.Context, stmt string)
}

//RenameProcedureAction define mail
type RenameProcedureAction struct {
	mail *mail.Server
}

//NewRenameProcedureAction return action.mail
// TODO(XZ): NewRenameProcedureAction should be changed with change of action()
func NewRenameProcedureAction(mail *mail.Server) *RenameProcedureAction {
	return &RenameProcedureAction{
		mail: mail,
	}
}

func (csa *RenameProcedureAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: procedure rename", stmt); err != nil {
		log.Errorf(ctx, "got error when do procedure rename action, err:%s", err)
	}
}
