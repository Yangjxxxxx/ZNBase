package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//RenameIndexMetric is the metric of Rename Index
type RenameIndexMetric struct {
	ctx     context.Context   // accept context
	metrics *RenameIDXMetrics // get metrics information
	rules   []RenameIDXRules  // realize 1 metric to n rules
}

//RenameIDXMetrics contain CatchCount
type RenameIDXMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewRenameIndexMetric init a a new metric
func NewRenameIndexMetric(ctx context.Context, mail *mail.Server) *RenameIndexMetric {
	// get metric information
	metrics := &RenameIDXMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.index.rename",
				Help:        "rename a index",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &RenameIndexMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []RenameIDXRules{
			NewRenameIndexRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (ridm *RenameIndexMetric) RegistMetric() interface{} {
	return ridm.metrics
}

//Metric check rules
func (ridm *RenameIndexMetric) Metric(info interface{}) {

	// increment by 1
	ridm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range ridm.rules {
		switch info.(type) {
		case string:
			rule.rule(ridm.ctx, ridm, info.(string))
		}
	}
}

//RenameIDXRules define Rules
type RenameIDXRules interface {
	rule(ctx context.Context, metric *RenameIndexMetric, stmt string)
}

//RenameIndexRule get actions
type RenameIndexRule struct {
	actions []RenameIDXActions // realize 1 rule to n actions
}

//NewRenameIndexRule use action to send mails
func NewRenameIndexRule(mail *mail.Server) *RenameIndexRule {
	return &RenameIndexRule{
		actions: []RenameIDXActions{
			NewRenameIndexAction(mail),
		},
	}
}

// rules define
func (ridr *RenameIndexRule) rule(ctx context.Context, metric *RenameIndexMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range ridr.actions {
			action.action(ctx, stmt)
		}
	}
}

//RenameIDXActions define Actions
type RenameIDXActions interface {
	action(ctx context.Context, stmt string)
}

//RenameIndexAction define mail
type RenameIndexAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewRenameIndexAction return action.mail
func NewRenameIndexAction(mail *mail.Server) *RenameIndexAction {
	return &RenameIndexAction{
		mail: mail,
	}
}

// send mails
func (rida *RenameIndexAction) action(ctx context.Context, stmt string) {
	if err := rida.mail.Send(ctx, "Audit: Rename index", stmt); err != nil {
		log.Errorf(ctx, "got error when do Rename index action, err:%s", err)
	}
}
