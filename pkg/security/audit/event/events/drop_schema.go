package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropSchemaMetric is the metric of Drop Schema
type DropSchemaMetric struct {
	ctx     context.Context // accept context
	metrics *DropSHMetrics  // get metrics information
	rules   []DropSHRules   // realize 1 metric to n rules
}

//DropSHMetrics contain CatchCount
type DropSHMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewDropSchemaMetric init a new metric
func NewDropSchemaMetric(ctx context.Context, mail *mail.Server) *DropSchemaMetric {
	// get metric information
	metrics := &DropSHMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.schema.drop",
				Help:        "drop a schema",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &DropSchemaMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropSHRules{
			NewDropSchemaRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (dSHm *DropSchemaMetric) RegistMetric() interface{} {
	return dSHm.metrics
}

//Metric check rules
func (dSHm *DropSchemaMetric) Metric(info interface{}) {

	// increment by 1
	dSHm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range dSHm.rules {
		switch info.(type) {
		case string:
			rule.rule(dSHm.ctx, dSHm, info.(string))
		}
	}
}

//DropSHRules define Rules
type DropSHRules interface {
	rule(ctx context.Context, metric *DropSchemaMetric, stmt string)
}

//DropSchemaRule get actions
type DropSchemaRule struct {
	actions []DropSHctions // realize 1 rule to n actions
}

//NewDropSchemaRule use action to send mails
func NewDropSchemaRule(mail *mail.Server) *DropSchemaRule {
	return &DropSchemaRule{
		actions: []DropSHctions{
			NewDropSchemaAction(mail),
		},
	}
}

// rules define
func (dSHr *DropSchemaRule) rule(ctx context.Context, metric *DropSchemaMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range dSHr.actions {
			action.action(ctx, stmt)
		}
	}
}

//DropSHctions define Actions
type DropSHctions interface {
	action(ctx context.Context, stmt string)
}

//DropSchemaAction define mail
type DropSchemaAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewDropSchemaAction return action.mail
func NewDropSchemaAction(mail *mail.Server) *DropSchemaAction {
	return &DropSchemaAction{
		mail: mail,
	}
}

// send mails
func (dSH *DropSchemaAction) action(ctx context.Context, stmt string) {
	if err := dSH.mail.Send(ctx, "Audit: Drop schema", stmt); err != nil {
		log.Errorf(ctx, "got error when do Drop schema action, err:%s", err)
	}
}
