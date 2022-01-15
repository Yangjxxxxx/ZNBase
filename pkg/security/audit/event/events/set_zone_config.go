package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//SetZoneConfigMetric is the metric of Set Zone Config
type SetZoneConfigMetric struct {
	ctx     context.Context // accept context
	metrics *SetZCMetrics   // get metrics information
	rules   []SetZCRules    // realize 1 metric to n rules
}

//SetZCMetrics contain CatchCount
type SetZCMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewSetZoneConfigMetric init a a new metric
func NewSetZoneConfigMetric(ctx context.Context, mail *mail.Server) *SetZoneConfigMetric {
	//get metric information
	metrics := &SetZCMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.zone.config.set",
				Help:        "set zone config",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &SetZoneConfigMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []SetZCRules{
			NewSetZoneConfigRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (szcm *SetZoneConfigMetric) RegistMetric() interface{} {
	return szcm.metrics
}

//Metric check rules
func (szcm *SetZoneConfigMetric) Metric(info interface{}) {

	//	increment by 1
	szcm.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range szcm.rules {
		switch info.(type) {
		case *infos.SetZoneConfigInfo:
			rule.rule(szcm.ctx, szcm, info.(*infos.SetZoneConfigInfo))
		}
	}
}

//SetZCRules define Rules
type SetZCRules interface {
	rule(ctx context.Context, metric *SetZoneConfigMetric, info *infos.SetZoneConfigInfo)
}

//SetZoneConfigRule get actions
type SetZoneConfigRule struct {
	actions []SetZCActions //realize 1 rule to n actions
}

//NewSetZoneConfigRule use action to send mails
func NewSetZoneConfigRule(mail *mail.Server) *SetZoneConfigRule {
	return &SetZoneConfigRule{
		actions: []SetZCActions{
			NewSetZoneConfigAction(mail),
		},
	}
}

// rules define
func (szcr *SetZoneConfigRule) rule(
	ctx context.Context, metric *SetZoneConfigMetric, info *infos.SetZoneConfigInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range szcr.actions {
			action.action(ctx, info)
		}
	}
}

//SetZCActions define Actions
type SetZCActions interface {
	action(ctx context.Context, info *infos.SetZoneConfigInfo)
}

//SetZoneConfigAction define mail
type SetZoneConfigAction struct {
	mail *mail.Server //define a Server to send mails
}

//NewSetZoneConfigAction return action.mail
func NewSetZoneConfigAction(mail *mail.Server) *SetZoneConfigAction {
	return &SetZoneConfigAction{
		mail: mail,
	}
}

// send mails
func (szca *SetZoneConfigAction) action(ctx context.Context, info *infos.SetZoneConfigInfo) {
	if err := szca.mail.Send(ctx, "Audit: Zone Config", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Zone Config action, err:%s", err)
	}
}
