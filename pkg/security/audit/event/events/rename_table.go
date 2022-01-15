package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//RenameTableMetric is the metric of Rename Table
type RenameTableMetric struct {
	ctx     context.Context
	metrics *RenameTBMetrics
	rules   []RenameTBRules
}

//RenameTBMetrics contain CatchCount
type RenameTBMetrics struct {
	CatchCount *metric.Counter
}

//NewRenameTableMetric init a new metric
func NewRenameTableMetric(ctx context.Context, mail *mail.Server) *RenameTableMetric {

	metrics := &RenameTBMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.table.rename",
				Help:        "rename a table",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	return &RenameTableMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []RenameTBRules{
			NewRenameTableRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (rtm *RenameTableMetric) RegistMetric() interface{} {
	return rtm.metrics
}

//Metric check rules
func (rtm *RenameTableMetric) Metric(info interface{}) {

	// 	increment by 1
	rtm.metrics.CatchCount.Inc(1)

	// 	check all rules
	for _, rule := range rtm.rules {
		switch info.(type) {
		case string:
			rule.rule(rtm.ctx, rtm, info.(string))
		}
	}
}

//RenameTBRules define Rules
type RenameTBRules interface {
	rule(ctx context.Context, metric *RenameTableMetric, stmt string)
}

//RenameTableRule get actions
type RenameTableRule struct {
	actions []RenameTBActions
}

//NewRenameTableRule use action to send mails
func NewRenameTableRule(mail *mail.Server) *RenameTableRule {
	return &RenameTableRule{
		actions: []RenameTBActions{
			NewRenameTableAction(mail),
		},
	}
}

func (rtr *RenameTableRule) rule(ctx context.Context, metric *RenameTableMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range rtr.actions {
			action.action(ctx, stmt)
		}
	}
}

//RenameTBActions define Actions
type RenameTBActions interface {
	action(ctx context.Context, stmt string)
}

//RenameTableAction define mail
type RenameTableAction struct {
	mail *mail.Server
}

//NewRenameTableAction return action.mail
func NewRenameTableAction(mail *mail.Server) *RenameTableAction {
	return &RenameTableAction{
		mail: mail,
	}
}

func (rta *RenameTableAction) action(ctx context.Context, stmt string) {
	if err := rta.mail.Send(ctx, "Audit: Rename table", stmt); err != nil {
		log.Errorf(ctx, "got error when do Rename table action, err:%s", err)
	}
}
