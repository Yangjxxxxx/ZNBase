package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//RenameDatabaseMetric is the metric of Rename Database
type RenameDatabaseMetric struct {
	ctx     context.Context
	metrics *RenameDBMetrics
	rules   []RenameDBRules
}

//RenameDBMetrics contain CatchCount
type RenameDBMetrics struct {
	CatchCount *metric.Counter
}

//NewRenameDatabaseMetric init a new metric
func NewRenameDatabaseMetric(ctx context.Context, mail *mail.Server) *RenameDatabaseMetric {

	metrics := &RenameDBMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.database.rename",
				Help:        "rename database",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	return &RenameDatabaseMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []RenameDBRules{
			NewRenameDatabaseRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (rdbm *RenameDatabaseMetric) RegistMetric() interface{} {
	return rdbm.metrics
}

//Metric check rules
func (rdbm *RenameDatabaseMetric) Metric(info interface{}) {

	// 	increment by 1
	rdbm.metrics.CatchCount.Inc(1)

	// 	check all rules
	for _, rule := range rdbm.rules {
		switch info.(type) {
		case string:
			rule.rule(rdbm.ctx, rdbm, info.(string))
		}
	}
}

//RenameDBRules define Rules
type RenameDBRules interface {
	rule(ctx context.Context, metric *RenameDatabaseMetric, stmt string)
}

//RenameDatabaseRule get actions
type RenameDatabaseRule struct {
	actions []RenameDBActions
}

//NewRenameDatabaseRule use action to send mails
func NewRenameDatabaseRule(mail *mail.Server) *RenameDatabaseRule {
	return &RenameDatabaseRule{
		actions: []RenameDBActions{
			NewRenameDatabaseAction(mail),
		},
	}
}

func (rdbr *RenameDatabaseRule) rule(
	ctx context.Context, metric *RenameDatabaseMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range rdbr.actions {
			action.action(ctx, stmt)
		}
	}
}

//RenameDBActions  define Actions
type RenameDBActions interface {
	action(ctx context.Context, stmt string)
}

//RenameDatabaseAction define mail
type RenameDatabaseAction struct {
	mail *mail.Server
}

//NewRenameDatabaseAction return action.mail
func NewRenameDatabaseAction(mail *mail.Server) *RenameDatabaseAction {
	return &RenameDatabaseAction{
		mail: mail,
	}
}

func (rdba *RenameDatabaseAction) action(ctx context.Context, stmt string) {
	if err := rdba.mail.Send(ctx, "Audit: Rename database", stmt); err != nil {
		log.Errorf(ctx, "got error when do Rename database action, err:%s", err)
	}
}
