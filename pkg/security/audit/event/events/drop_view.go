package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropViewMetric is the metric of Drop View
type DropViewMetric struct {
	ctx     context.Context // accept context
	metrics *DropVMetrics   // get metrics information
	rules   []DropVRules    // realize 1 metric to n rules
}

//DropVMetrics contain CatchCount
type DropVMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewDropViewMetric init a new metric
func NewDropViewMetric(ctx context.Context, mail *mail.Server) *DropViewMetric {
	// get metric information
	metrics := &DropVMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.view.drop",
				Help:        "drop view",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &DropViewMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropVRules{
			NewDropViewRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (dvm *DropViewMetric) RegistMetric() interface{} {
	return dvm.metrics
}

//Metric check rules
func (dvm *DropViewMetric) Metric(info interface{}) {

	// increment by 1
	dvm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range dvm.rules {
		switch info.(type) {
		case *infos.DropViewInfo:
			rule.rule(dvm.ctx, dvm, info.(*infos.DropViewInfo))
		}
	}
}

//DropVRules  define Rules
type DropVRules interface {
	rule(ctx context.Context, metric *DropViewMetric, info *infos.DropViewInfo)
}

//DropViewRule get actions
type DropViewRule struct {
	actions []DropVActions // realize 1 rule to n actions
}

//NewDropViewRule use action to send mails
func NewDropViewRule(mail *mail.Server) *DropViewRule {
	return &DropViewRule{
		actions: []DropVActions{
			NewDropViewAction(mail),
		},
	}
}

// rules define
func (dvr *DropViewRule) rule(
	ctx context.Context, metric *DropViewMetric, info *infos.DropViewInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range dvr.actions {
			action.action(ctx, info)
		}
	}
}

//DropVActions define Actions
type DropVActions interface {
	action(ctx context.Context, info *infos.DropViewInfo)
}

//DropViewAction define mail
type DropViewAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewDropViewAction return action.mail
func NewDropViewAction(mail *mail.Server) *DropViewAction {
	return &DropViewAction{
		mail: mail,
	}
}

// send mails
func (dva *DropViewAction) action(ctx context.Context, info *infos.DropViewInfo) {
	if err := dva.mail.Send(ctx, "Audit: Drop View", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Drop View action, err:%s", err)
	}
}
