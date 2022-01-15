package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//SetClusterSettingMetric is the metric of Set Cluster Setting
type SetClusterSettingMetric struct {
	ctx     context.Context // accept context
	metrics *SetCSMetrics   // get metrics information
	rules   []SetCSRules    // realize 1 metric to n rules
}

//SetCSMetrics contain CatchCount
type SetCSMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewSetClusterSettingMetric init a a new metric
func NewSetClusterSettingMetric(ctx context.Context, mail *mail.Server) *SetClusterSettingMetric {
	// get metric information
	metrics := &SetCSMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.set_cluster_setting",
				Help:        "set cluster setting",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &SetClusterSettingMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []SetCSRules{
			NewSetClusterSettingRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (scsm *SetClusterSettingMetric) RegistMetric() interface{} {
	return scsm.metrics
}

//Metric check rules
func (scsm *SetClusterSettingMetric) Metric(info interface{}) {

	// 	increment by 1
	scsm.metrics.CatchCount.Inc(1)

	// 	check all rules
	for _, rule := range scsm.rules {
		switch info.(type) {
		case *infos.SetClusterSettingInfo:
			rule.rule(scsm.ctx, scsm, info.(*infos.SetClusterSettingInfo))
		}
	}
}

//SetCSRules define Rules
type SetCSRules interface {
	rule(ctx context.Context, metric *SetClusterSettingMetric, info *infos.SetClusterSettingInfo)
}

//SetClusterSettingRule get actions
type SetClusterSettingRule struct {
	actions []SetCSActions // realize 1 rule to n actions
}

//NewSetClusterSettingRule use action to send mails
func NewSetClusterSettingRule(mail *mail.Server) *SetClusterSettingRule {
	return &SetClusterSettingRule{
		actions: []SetCSActions{
			NewSetClusterSettingAction(mail),
		},
	}
}

// rules define
func (scsr *SetClusterSettingRule) rule(
	ctx context.Context, metric *SetClusterSettingMetric, info *infos.SetClusterSettingInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range scsr.actions {
			action.action(ctx, info)
		}
	}
}

//SetCSActions define Actions
type SetCSActions interface {
	action(ctx context.Context, info *infos.SetClusterSettingInfo)
}

//SetClusterSettingAction define mail
type SetClusterSettingAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewSetClusterSettingAction return action.mail
func NewSetClusterSettingAction(mail *mail.Server) *SetClusterSettingAction {
	return &SetClusterSettingAction{
		mail: mail,
	}
}

// send mails
func (scsa *SetClusterSettingAction) action(
	ctx context.Context, info *infos.SetClusterSettingInfo,
) {
	if err := scsa.mail.Send(ctx, "Audit: Set Cluster Setting", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Set Cluster Setting action, err:%s", err)
	}
}
