package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropTableMetric  is the metric of Drop Table
type DropTableMetric struct {
	ctx     context.Context // accept context
	metrics *DropTBMetrics  // get metrics information
	rules   []DropTBRules   // realize 1 metric to n rules
}

//DropTBMetrics contain CatchCount
type DropTBMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewDropTableMetric init a new metric
func NewDropTableMetric(ctx context.Context, mail *mail.Server) *DropTableMetric {
	// get metric information
	metrics := &DropTBMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.table.drop",
				Help:        "drop a table",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &DropTableMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropTBRules{
			NewDropTableRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (dtm *DropTableMetric) RegistMetric() interface{} {
	return dtm.metrics
}

//Metric check rules
func (dtm *DropTableMetric) Metric(info interface{}) {

	// 	increment by 1
	dtm.metrics.CatchCount.Inc(1)

	// 	check all rules
	for _, rule := range dtm.rules {
		switch info.(type) {
		case *infos.DropTableInfo:
			rule.rule(dtm.ctx, dtm, info.(*infos.DropTableInfo))
		}
	}
}

//DropTBRules  define Rules
type DropTBRules interface {
	rule(ctx context.Context, metric *DropTableMetric, info *infos.DropTableInfo)
}

//DropTableRule get actions
type DropTableRule struct {
	actions []DropTBActions // realize 1 rule to n actions
}

//NewDropTableRule use action to send mails
func NewDropTableRule(mail *mail.Server) *DropTableRule {
	return &DropTableRule{
		actions: []DropTBActions{
			NewDropTableAction(mail),
		},
	}
}

// rules define
func (dtr *DropTableRule) rule(
	ctx context.Context, metric *DropTableMetric, info *infos.DropTableInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range dtr.actions {
			action.action(ctx, info)
		}
	}
}

//DropTBActions define Actions
type DropTBActions interface {
	action(ctx context.Context, info *infos.DropTableInfo)
}

//DropTableAction define mail
type DropTableAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewDropTableAction return action.mail
func NewDropTableAction(mail *mail.Server) *DropTableAction {
	return &DropTableAction{
		mail: mail,
	}
}

// send mails
func (dta *DropTableAction) action(ctx context.Context, info *infos.DropTableInfo) {
	if err := dta.mail.Send(ctx, "Audit: Drop Table", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Drop Table action, err:%s", err)
	}
}
