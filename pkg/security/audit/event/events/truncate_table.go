package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//TruncateTableMetric is the metric of Truncate Table
type TruncateTableMetric struct {
	ctx     context.Context    // accept context
	metrics *TruncateTBMetrics // get metrics information
	rules   []TruncateTBRules  // realize 1 metric to n rules
}

//TruncateTBMetrics contain CatchCount
type TruncateTBMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewTruncateTableMetric init a new metric
func NewTruncateTableMetric(ctx context.Context, mail *mail.Server) *TruncateTableMetric {
	// get metric information
	metrics := &TruncateTBMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.table.truncate",
				Help:        "truncate table",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &TruncateTableMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []TruncateTBRules{
			NewTruncateTableRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (tctm *TruncateTableMetric) RegistMetric() interface{} {
	return tctm.metrics
}

//Metric check rules
func (tctm *TruncateTableMetric) Metric(info interface{}) {

	// 	increment by 1
	tctm.metrics.CatchCount.Inc(1)

	// 	check all rules
	for _, rule := range tctm.rules {
		switch info.(type) {
		case *infos.TruncateInfo:
			rule.rule(tctm.ctx, tctm, info.(*infos.TruncateInfo))
		}
	}
}

//TruncateTBRules define Rules
type TruncateTBRules interface {
	rule(ctx context.Context, metric *TruncateTableMetric, info *infos.TruncateInfo)
}

//TruncateTableRule get actions
type TruncateTableRule struct {
	actions []TruncateTBActions // realize 1 rule to n actions
}

//NewTruncateTableRule use action to send mails
func NewTruncateTableRule(mail *mail.Server) *TruncateTableRule {
	return &TruncateTableRule{
		actions: []TruncateTBActions{
			NewTruncateTableAction(mail),
		},
	}
}

// rules define
func (tctr *TruncateTableRule) rule(
	ctx context.Context, metric *TruncateTableMetric, info *infos.TruncateInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range tctr.actions {
			action.action(ctx, info)
		}
	}
}

//TruncateTBActions define Actions
type TruncateTBActions interface {
	action(ctx context.Context, info *infos.TruncateInfo)
}

//TruncateTableAction define mail
type TruncateTableAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewTruncateTableAction return action.mail
func NewTruncateTableAction(mail *mail.Server) *TruncateTableAction {
	return &TruncateTableAction{
		mail: mail,
	}
}

// send mails
func (tcta *TruncateTableAction) action(ctx context.Context, info *infos.TruncateInfo) {
	if err := tcta.mail.Send(ctx, "Audit: Truncate Table", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Truncate Table action, err:%s", err)
	}
}
