package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//AlterSequenceMetric is the metric of alter sequence
type AlterSequenceMetric struct {
	ctx     context.Context //accept context
	metrics *AlterSQMetrics //get metrics information
	rules   []AlterSQRules  //realize 1 metric to n rules
}

//AlterSQMetrics contain CatchCount
type AlterSQMetrics struct {
	CatchCount *metric.Counter //metric define
}

//NewAlterSequenceMetric init a new metric
func NewAlterSequenceMetric(ctx context.Context, mail *mail.Server) *AlterSequenceMetric {
	//get metric information
	metrics := &AlterSQMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.sequence.alter",
				Help:        "alter sequence",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//use rules to send mail
	return &AlterSequenceMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []AlterSQRules{
			NewAlterSequenceRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (asqm *AlterSequenceMetric) RegistMetric() interface{} {
	return asqm.metrics
}

//Metric check rules
func (asqm *AlterSequenceMetric) Metric(info interface{}) {

	//	increment by 1
	asqm.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range asqm.rules {
		switch info.(type) {
		case *infos.AlterSequenceInfo:
			rule.rule(asqm.ctx, asqm, info.(*infos.AlterSequenceInfo))
		}
	}
}

//AlterSQRules define Rules
type AlterSQRules interface {
	rule(ctx context.Context, metric *AlterSequenceMetric, info *infos.AlterSequenceInfo)
}

//AlterSequenceRule get actions
type AlterSequenceRule struct {
	actions []AlterSQActions //realize 1 rule to n actions
}

//NewAlterSequenceRule use action to send mails
func NewAlterSequenceRule(mail *mail.Server) *AlterSequenceRule {
	return &AlterSequenceRule{
		actions: []AlterSQActions{
			NewAlterSequenceAction(mail),
		},
	}
}

//rules define
func (asqr *AlterSequenceRule) rule(
	ctx context.Context, metric *AlterSequenceMetric, info *infos.AlterSequenceInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range asqr.actions {
			action.action(ctx, info)
		}
	}
}

//AlterSQActions define Actions
type AlterSQActions interface {
	action(ctx context.Context, info *infos.AlterSequenceInfo)
}

//AlterSequenceAction define mail
type AlterSequenceAction struct {
	mail *mail.Server //define a Server to send mails
}

//NewAlterSequenceAction return action.mail
func NewAlterSequenceAction(mail *mail.Server) *AlterSequenceAction {
	return &AlterSequenceAction{
		mail: mail,
	}
}

//send mails
func (asqa *AlterSequenceAction) action(ctx context.Context, info *infos.AlterSequenceInfo) {
	if err := asqa.mail.Send(ctx, "Audit: Alter Sequence", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Alter Sequence action, err:%s", err)
	}
}
