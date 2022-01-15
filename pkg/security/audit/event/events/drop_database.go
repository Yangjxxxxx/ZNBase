package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropDatabaseMetric is the metric of Drop Database
type DropDatabaseMetric struct {
	ctx     context.Context
	metrics *DropDBMetrics
	rules   []DropDBRules
}

//DropDBMetrics contain CatchCount
type DropDBMetrics struct {
	CatchCount *metric.Counter
}

//NewDropDatabaseMetric init a new metric
func NewDropDatabaseMetric(ctx context.Context, mail *mail.Server) *DropDatabaseMetric {

	metrics := &DropDBMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.database.drop",
				Help:        "drop a database",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	return &DropDatabaseMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropDBRules{
			NewDropDatabaseRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (ddbm *DropDatabaseMetric) RegistMetric() interface{} {
	return ddbm.metrics
}

//Metric check rules
func (ddbm *DropDatabaseMetric) Metric(info interface{}) {

	// 	increment by 1
	ddbm.metrics.CatchCount.Inc(1)

	// 	check all rules
	for _, rule := range ddbm.rules {
		switch info.(type) {
		case *infos.DropDatabaseInfo:
			rule.rule(ddbm.ctx, ddbm, info.(*infos.DropDatabaseInfo))
		}
	}
}

//DropDBRules define Rules
type DropDBRules interface {
	rule(ctx context.Context, metric *DropDatabaseMetric, info *infos.DropDatabaseInfo)
}

//DropDatabaseRule get actions
type DropDatabaseRule struct {
	actions []DropDBActions
}

//NewDropDatabaseRule use action to send mails
func NewDropDatabaseRule(mail *mail.Server) *DropDatabaseRule {
	return &DropDatabaseRule{
		actions: []DropDBActions{
			NewDropDatabaseAction(mail),
		},
	}
}

// rule defines a rule, every time drop database, do actions
func (ddbr *DropDatabaseRule) rule(
	ctx context.Context, metric *DropDatabaseMetric, info *infos.DropDatabaseInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range ddbr.actions {
			action.action(ctx, info)
		}
	}
}

//DropDBActions define Actions
type DropDBActions interface {
	action(ctx context.Context, info *infos.DropDatabaseInfo)
}

//DropDatabaseAction define mail
type DropDatabaseAction struct {
	mail *mail.Server
}

//NewDropDatabaseAction return action.mail
func NewDropDatabaseAction(mail *mail.Server) *DropDatabaseAction {
	return &DropDatabaseAction{
		mail: mail,
	}
}

func (ddba *DropDatabaseAction) action(ctx context.Context, info *infos.DropDatabaseInfo) {
	if err := ddba.mail.Send(ctx, "Audit: Drop Database", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Drop Database action, err:%s", err)
	}
}
