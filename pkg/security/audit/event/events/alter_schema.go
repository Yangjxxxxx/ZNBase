package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//AlterSchemaMetric is the metric of alter schema
type AlterSchemaMetric struct {
	ctx     context.Context //accept context
	metrics *AlterSHMetrics //get metrics information
	rules   []AlterSHRules  //realize 1 metric to n rules
}

//AlterSHMetrics contain CatchCount
type AlterSHMetrics struct {
	CatchCount *metric.Counter //metric define
}

//NewAlterSchemaMetric init a new metric
func NewAlterSchemaMetric(ctx context.Context, mail *mail.Server) *AlterSchemaMetric {
	//get metric information
	metrics := &AlterSHMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.schema.alter",
				Help:        "alter schema",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//use rules to send mail
	return &AlterSchemaMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []AlterSHRules{
			NewAlterSchemaRule(mail),
		},
	}
}

//RegistMetric interface return aSHm.metrics
func (aSHm *AlterSchemaMetric) RegistMetric() interface{} {
	return aSHm.metrics
}

//Metric check rules
func (aSHm *AlterSchemaMetric) Metric(info interface{}) {

	//	increment by 1
	aSHm.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range aSHm.rules {
		switch info.(type) {
		case string:
			rule.rule(aSHm.ctx, aSHm, info.(string))
		}
	}
}

//AlterSHRules define Rules
type AlterSHRules interface {
	rule(ctx context.Context, metric *AlterSchemaMetric, stmt string)
}

//AlterSchemaRule get actions
type AlterSchemaRule struct {
	actions []AlterSHActions //realize 1 rule to n actions
}

//NewAlterSchemaRule use action to send mails
func NewAlterSchemaRule(mail *mail.Server) *AlterSchemaRule {
	return &AlterSchemaRule{
		actions: []AlterSHActions{
			NewAlterSchemaAction(mail),
		},
	}
}

//rules define
func (aSHr *AlterSchemaRule) rule(ctx context.Context, metric *AlterSchemaMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range aSHr.actions {
			action.action(ctx, stmt)
		}
	}
}

//AlterSHActions define Actions
type AlterSHActions interface {
	action(ctx context.Context, stmt string)
}

//AlterSchemaAction define mail
type AlterSchemaAction struct {
	mail *mail.Server //define a Server to send mails
}

//NewAlterSchemaAction return action.mail
func NewAlterSchemaAction(mail *mail.Server) *AlterSchemaAction {
	return &AlterSchemaAction{
		mail: mail,
	}
}

//send mails
func (aSHa *AlterSchemaAction) action(ctx context.Context, stmt string) {
	if err := aSHa.mail.Send(ctx, "Audit: Alter Schema", stmt); err != nil {
		log.Errorf(ctx, "got error when do Alter Schema action, err:%s", err)
	}
}
