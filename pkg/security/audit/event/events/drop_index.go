package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropIndexMetric is the metric of Drop Index
type DropIndexMetric struct {
	ctx     context.Context // accept context
	metrics *DropIDXMetrics // get metrics information
	rules   []DropIDXRules  // realize 1 metric to n rules
}

//DropIDXMetrics contain CatchCount
type DropIDXMetrics struct {
	CatchCount *metric.Counter //metric define
}

//NewDropIndexMetric init a new metric
func NewDropIndexMetric(ctx context.Context, mail *mail.Server) *DropIndexMetric {
	// get metric information
	metrics := &DropIDXMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.index.drop",
				Help:        "drop a index",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//use rules to send mail
	return &DropIndexMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropIDXRules{
			NewDropIndexRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (dim *DropIndexMetric) RegistMetric() interface{} {
	return dim.metrics
}

//Metric check rules
func (dim *DropIndexMetric) Metric(info interface{}) {

	// increment by 1
	dim.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range dim.rules {
		switch info.(type) {
		case *infos.DropIndexInfo:
			rule.rule(dim.ctx, dim, info.(*infos.DropIndexInfo))
		}
	}
}

//DropIDXRules define Rules
type DropIDXRules interface {
	rule(ctx context.Context, metric *DropIndexMetric, info *infos.DropIndexInfo)
}

//DropIndexRule get actions
type DropIndexRule struct {
	actions []DropIDXActions // realize 1 rule to n actions
}

//NewDropIndexRule use action to send mails
func NewDropIndexRule(mail *mail.Server) *DropIndexRule {
	return &DropIndexRule{
		actions: []DropIDXActions{
			NewDropIndexAction(mail),
		},
	}
}

// rules define
func (dir *DropIndexRule) rule(
	ctx context.Context, metric *DropIndexMetric, info *infos.DropIndexInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range dir.actions {
			action.action(ctx, info)
		}
	}
}

//DropIDXActions define Actions
type DropIDXActions interface {
	action(ctx context.Context, info *infos.DropIndexInfo)
}

//DropIndexAction define mail
type DropIndexAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewDropIndexAction return action.mail
func NewDropIndexAction(mail *mail.Server) *DropIndexAction {
	return &DropIndexAction{
		mail: mail,
	}
}

// send mails
func (dia *DropIndexAction) action(ctx context.Context, info *infos.DropIndexInfo) {
	if err := dia.mail.Send(ctx, "Audit: Drop Index", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Drop Index action, err:%s", err)
	}
}
