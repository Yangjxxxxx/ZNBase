package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//AlterTableMetric  is the metric of Alter Table
type AlterTableMetric struct {
	ctx     context.Context // accept context
	metrics *AlterTBMetrics // get metrics information
	rules   []AlterTBRules  // realize 1 metric to n rules
}

//AlterTBMetrics contain CatchCount
type AlterTBMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewAlterTableMetric init a new metric
func NewAlterTableMetric(ctx context.Context, mail *mail.Server) *AlterTableMetric {
	// get metric information
	metrics := &AlterTBMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.table.alter",
				Help:        "alter table with options",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &AlterTableMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []AlterTBRules{
			NewAlterTableRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (atm *AlterTableMetric) RegistMetric() interface{} {
	return atm.metrics
}

//Metric check rules
func (atm *AlterTableMetric) Metric(info interface{}) {

	// increment by 1
	atm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range atm.rules {
		switch info.(type) {
		case *infos.AlterTableInfo:
			rule.rule(atm.ctx, atm, info.(*infos.AlterTableInfo))
		}
	}
}

//AlterTBRules define Rules
type AlterTBRules interface {
	rule(ctx context.Context, metric *AlterTableMetric, info *infos.AlterTableInfo)
}

//AlterTableRule get actions
type AlterTableRule struct {
	actions []AlterTBActions // realize 1 rule to n actions
}

//NewAlterTableRule use action to send mails
func NewAlterTableRule(mail *mail.Server) *AlterTableRule {
	return &AlterTableRule{
		actions: []AlterTBActions{
			NewAlterTableAction(mail),
		},
	}
}

// rules define
func (atr *AlterTableRule) rule(
	ctx context.Context, metric *AlterTableMetric, info *infos.AlterTableInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range atr.actions {
			action.action(ctx, info)
		}
	}
}

//AlterTBActions define Actions
type AlterTBActions interface {
	action(ctx context.Context, info *infos.AlterTableInfo)
}

//AlterTableAction define mail
type AlterTableAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewAlterTableAction return action.mail
func NewAlterTableAction(mail *mail.Server) *AlterTableAction {
	return &AlterTableAction{
		mail: mail,
	}
}

// send mails
func (ata *AlterTableAction) action(ctx context.Context, info *infos.AlterTableInfo) {
	if err := ata.mail.Send(ctx, "Audit: Alter Table", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Alter Table action, err:%s", err)
	}
}
