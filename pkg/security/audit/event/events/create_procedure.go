package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//CreateProcedureMetric is the metric when create statistics event occurs.
type CreateProcedureMetric struct {
	ctx     context.Context
	metrics *CreateProcedureMetrics
	rules   []CreateProcedureRules
}

//CreateProcedureMetrics contain CatchCount
type CreateProcedureMetrics struct {
	CatchCount *metric.Counter
}

//NewCreateProcedureMetric init a new metric
func NewCreateProcedureMetric(ctx context.Context, mail *mail.Server) *CreateProcedureMetric {
	metrics := &CreateProcedureMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.procedure.create",
				Help:        "A procedure create",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &CreateProcedureMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateProcedureRules{
			NewCreateProcedureRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csm *CreateProcedureMetric) RegistMetric() interface{} {
	return csm.metrics
}

//Metric check rules
func (csm *CreateProcedureMetric) Metric(info interface{}) {

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

//CreateProcedureRules define rules for metrics from createprocedureMetric.
type CreateProcedureRules interface {
	rule(ctx context.Context, metric *CreateProcedureMetric, stmt string)
}

//CreateProcedureRule get actions
type CreateProcedureRule struct {
	actions []CreateProcedureActions
}

//NewCreateProcedureRule use action to send mails
// TODO(xz): NewCreateProcedureRule should be changed with change of rule()
func NewCreateProcedureRule(mail *mail.Server) *CreateProcedureRule {
	return &CreateProcedureRule{
		actions: []CreateProcedureActions{
			NewCreateProcedureAction(mail),
		},
	}
}

func (csr *CreateProcedureRule) rule(
	ctx context.Context, metric *CreateProcedureMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range csr.actions {
			action.action(ctx, stmt)
		}
	}
}

//CreateProcedureActions define actions to face rules before.
type CreateProcedureActions interface {
	action(ctx context.Context, stmt string)
}

//CreateProcedureAction define mail
type CreateProcedureAction struct {
	mail *mail.Server
}

//NewCreateProcedureAction return action.mail
// TODO(XZ): NewCreateprocedureAction should be changed with change of action()
func NewCreateProcedureAction(mail *mail.Server) *CreateProcedureAction {
	return &CreateProcedureAction{
		mail: mail,
	}
}

func (csa *CreateProcedureAction) action(ctx context.Context, stmt string) {
	if err := csa.mail.Send(ctx, "Audit: procedure create", stmt); err != nil {
		log.Errorf(ctx, "got error when do procedure create action, err:%s", err)
	}
}
