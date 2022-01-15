package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

// RmZoneCfgMetric is the metric when a zone config removed event occurs.
type RmZoneCfgMetric struct {
	ctx     context.Context
	metrics *RmZoneCfgMetrics
	rules   []RmZoneCfgRules
}

//RmZoneCfgMetrics contain CatchCount
type RmZoneCfgMetrics struct {
	CatchCount *metric.Counter
}

//NewRmZoneCfgMetric init a new metric
func NewRmZoneCfgMetric(ctx context.Context, mail *mail.Server) *RmZoneCfgMetric {
	metrics := &RmZoneCfgMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.zone.config.remove",
				Help:        "remove a zone config",
				Measurement: "audit",
				// TODO(xz): waiting for design for metric
				Unit: metric.Unit_COUNT,
			}),
	}

	return &RmZoneCfgMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []RmZoneCfgRules{
			NewRmZoneCfgRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (rzcm *RmZoneCfgMetric) RegistMetric() interface{} {
	return rzcm.metrics
}

//Metric check rules
func (rzcm *RmZoneCfgMetric) Metric(info interface{}) {

	//	increment by 1
	rzcm.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range rzcm.rules {
		switch info.(type) {
		case string:
			rule.rule(rzcm.ctx, rzcm, info.(string))
		}
	}
}

// RmZoneCfgRules define rules for metrics from RmZoneCfgMetric.
type RmZoneCfgRules interface {
	rule(ctx context.Context, metric *RmZoneCfgMetric, stmt string)
}

//RmZoneCfgRule get actions
type RmZoneCfgRule struct {
	actions []RmZoneCfgActions
}

//NewRmZoneCfgRule use action to send mails
// TODO(xz): NewRmZoneCfgRule should be changed with change of rule()
func NewRmZoneCfgRule(mail *mail.Server) *RmZoneCfgRule {
	return &RmZoneCfgRule{
		actions: []RmZoneCfgActions{
			NewRmZoneCfgAction(mail),
		},
	}
}

func (crzr *RmZoneCfgRule) rule(ctx context.Context, metric *RmZoneCfgMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range crzr.actions {
			action.action(ctx, stmt)
		}
	}
}

// RmZoneCfgActions define actions to face rules before.
type RmZoneCfgActions interface {
	action(ctx context.Context, stmt string)
}

//RmZoneCfgAction define mail
type RmZoneCfgAction struct {
	mail *mail.Server
}

//NewRmZoneCfgAction return action.mail
// TODO(XZ): NewRmZoneCfgAction should be changed with change of action()
func NewRmZoneCfgAction(mail *mail.Server) *RmZoneCfgAction {
	return &RmZoneCfgAction{
		mail: mail,
	}
}

func (rzca *RmZoneCfgAction) action(ctx context.Context, stmt string) {
	if err := rzca.mail.Send(ctx, "Audit: Remove zone config", stmt); err != nil {
		log.Errorf(ctx, "got error when do Remove zone config action, err:%s", err)
	}
}
