package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropSequenceMetric is the metric of alter schema
type DropSequenceMetric struct {
	ctx     context.Context // accept context
	metrics *DropSQMetrics  // get metrics information
	rules   []DropSQRules   // realize 1 metric to n rules
}

//DropSQMetrics contain CatchCount
type DropSQMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewDropSequenceMetric init a new metric
func NewDropSequenceMetric(ctx context.Context, mail *mail.Server) *DropSequenceMetric {
	// get metric information
	metrics := &DropSQMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.sequence.drop",
				Help:        "drop a sequence",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &DropSequenceMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropSQRules{
			NewDropSequenceRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (dsqm *DropSequenceMetric) RegistMetric() interface{} {
	return dsqm.metrics
}

//Metric check rules
func (dsqm *DropSequenceMetric) Metric(info interface{}) {

	// increment by 1
	dsqm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range dsqm.rules {
		switch info.(type) {
		case *infos.DropSequenceInfo:
			rule.rule(dsqm.ctx, dsqm, info.(*infos.DropSequenceInfo))
		}
	}
}

//DropSQRules define Rules
type DropSQRules interface {
	rule(ctx context.Context, metric *DropSequenceMetric, info *infos.DropSequenceInfo)
}

//DropSequenceRule get actions
type DropSequenceRule struct {
	actions []DropSQActions // realize 1 rule to n actions
}

//NewDropSequenceRule use action to send mails
func NewDropSequenceRule(mail *mail.Server) *DropSequenceRule {
	return &DropSequenceRule{
		actions: []DropSQActions{
			NewDropSequenceAction(mail),
		},
	}
}

// rules define
func (dsqr *DropSequenceRule) rule(
	ctx context.Context, metric *DropSequenceMetric, info *infos.DropSequenceInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range dsqr.actions {
			action.action(ctx, info)
		}
	}
}

//DropSQActions define Actions
type DropSQActions interface {
	action(ctx context.Context, info *infos.DropSequenceInfo)
}

//DropSequenceAction define mail
type DropSequenceAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewDropSequenceAction return action.mail
func NewDropSequenceAction(mail *mail.Server) *DropSequenceAction {
	return &DropSequenceAction{
		mail: mail,
	}
}

// send mails
func (dsqa *DropSequenceAction) action(ctx context.Context, info *infos.DropSequenceInfo) {
	if err := dsqa.mail.Send(ctx, "Audit: Drop Sequence", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Drop Sequence action, err:%s", err)
	}
}
