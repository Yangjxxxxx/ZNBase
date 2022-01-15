package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//SplitMetric is the metric of Split
type SplitMetric struct {
	ctx     context.Context // accept context
	metrics *SplitAtMetrics // get metrics information
	rules   []SplitAtRules  // realize 1 metric to n rules
}

//SplitAtMetrics contain CatchCount
type SplitAtMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewSplitMetric init a new metric
func NewSplitMetric(ctx context.Context, mail *mail.Server) *SplitMetric {
	// get metric information
	metrics := &SplitAtMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.sql.split",
				Help:        "split at",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &SplitMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []SplitAtRules{
			NewSplitRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (tctm *SplitMetric) RegistMetric() interface{} {
	return tctm.metrics
}

//Metric check rules
func (tctm *SplitMetric) Metric(info interface{}) {

	// 	increment by 1
	tctm.metrics.CatchCount.Inc(1)

	// 	check all rules
	for _, rule := range tctm.rules {
		switch info.(type) {
		case string:
			rule.rule(tctm.ctx, tctm, info.(string))
		}
	}
}

//SplitAtRules define Rules
type SplitAtRules interface {
	rule(ctx context.Context, metric *SplitMetric, stmt string)
}

//SplitRule get actions
type SplitRule struct {
	actions []SplitAtActions // realize 1 rule to n actions
}

//NewSplitRule use action to send mails
func NewSplitRule(mail *mail.Server) SplitRule {
	return SplitRule{
		actions: []SplitAtActions{
			NewSplitTableAction(mail),
		},
	}
}

// rules define
func (tctr SplitRule) rule(ctx context.Context, metric *SplitMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range tctr.actions {
			action.action(ctx, stmt)
		}
	}
}

//SplitAtActions define Actions
type SplitAtActions interface {
	action(ctx context.Context, stmt string)
}

//SplitAction define mail
type SplitAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewSplitTableAction return action.mail
func NewSplitTableAction(mail *mail.Server) *SplitAction {
	return &SplitAction{
		mail: mail,
	}
}

// send mails
func (tcta *SplitAction) action(ctx context.Context, stmt string) {
	if err := tcta.mail.Send(ctx, "Audit: Split at", stmt); err != nil {
		log.Errorf(ctx, "got error when do Split at action, err:%s", err)
	}
}
