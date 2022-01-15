package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//ReverseSchemaChangeMetric is the metric of Reverse Schema Change
type ReverseSchemaChangeMetric struct {
	ctx     context.Context   //accept context
	metrics *ReverseSCMetrics //get metrics information
	rules   []ReverseSCRules  //realize 1 metric to n rules
}

//ReverseSCMetrics contain CatchCount
type ReverseSCMetrics struct {
	CatchCount *metric.Counter //metric define
}

//NewReverseSchemaChangeMetric init a a new metric
func NewReverseSchemaChangeMetric(
	ctx context.Context, mail *mail.Server,
) *ReverseSchemaChangeMetric {
	//get metric information
	metrics := &ReverseSCMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.schema.reverse.change",
				Help:        "reverse schema change",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//use rules to send mail
	return &ReverseSchemaChangeMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []ReverseSCRules{
			NewReverseSchemaChangeRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (rscm *ReverseSchemaChangeMetric) RegistMetric() interface{} {
	return rscm.metrics
}

//Metric check rules
func (rscm *ReverseSchemaChangeMetric) Metric(info interface{}) {

	//	increment by 1
	rscm.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range rscm.rules {
		switch info.(type) {
		case string:
			rule.rule(rscm.ctx, rscm, info.(string))
		}
	}
}

//ReverseSCRules define Rules
type ReverseSCRules interface {
	rule(ctx context.Context, metric *ReverseSchemaChangeMetric, stmt string)
}

//ReverseSchemaChangeRule get actions
type ReverseSchemaChangeRule struct {
	actions []ReverseSCActions //realize 1 rule to n actions
}

//NewReverseSchemaChangeRule use action to send mails
func NewReverseSchemaChangeRule(mail *mail.Server) *ReverseSchemaChangeRule {
	return &ReverseSchemaChangeRule{
		actions: []ReverseSCActions{
			NewReverseSchemaChangeAction(mail),
		},
	}
}

//rules define
func (rscr *ReverseSchemaChangeRule) rule(
	ctx context.Context, metric *ReverseSchemaChangeMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 5 {
		for _, action := range rscr.actions {
			action.action(ctx, stmt)
		}
	}
}

//ReverseSCActions define Actions
type ReverseSCActions interface {
	action(ctx context.Context, stmt string)
}

//ReverseSchemaChangeAction define mail
type ReverseSchemaChangeAction struct {
	mail *mail.Server //define a Server to send mails
}

//NewReverseSchemaChangeAction return action.mail
func NewReverseSchemaChangeAction(mail *mail.Server) *ReverseSchemaChangeAction {
	return &ReverseSchemaChangeAction{
		mail: mail,
	}
}

//send mails
func (rsca *ReverseSchemaChangeAction) action(ctx context.Context, stmt string) {
	if err := rsca.mail.Send(ctx, "Audit: Reverse Schema Change", stmt); err != nil {
		log.Errorf(ctx, "got error when do Reverse Schema Change action, err:%s", err)
	}
}
