package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//DropRoleMetric is the metric of Drop Role
type DropRoleMetric struct {
	ctx     context.Context // accept context
	metrics *DropRLMetrics  // get metrics information
	rules   []DropRLRules   // realize 1 metric to n rules
}

//DropRLMetrics contain CatchCount
type DropRLMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewDropRoleMetric init a new metric
func NewDropRoleMetric(ctx context.Context, mail *mail.Server) *DropRoleMetric {
	// get metric information
	metrics := &DropRLMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.role.drop",
				Help:        "drop a role",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &DropRoleMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []DropRLRules{
			NewDropRoleRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (dsqm *DropRoleMetric) RegistMetric() interface{} {
	return dsqm.metrics
}

//Metric check rules
func (dsqm *DropRoleMetric) Metric(info interface{}) {

	// increment by 1
	dsqm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range dsqm.rules {
		switch info.(type) {
		case string:
			rule.rule(dsqm.ctx, dsqm, info.(string))
		}
	}
}

//DropRLRules define Rules
type DropRLRules interface {
	rule(ctx context.Context, metric *DropRoleMetric, stmt string)
}

//DropRoleRule get actions
type DropRoleRule struct {
	actions []DropRLActions // realize 1 rule to n actions
}

//NewDropRoleRule use action to send mails
func NewDropRoleRule(mail *mail.Server) *DropRoleRule {
	return &DropRoleRule{
		actions: []DropRLActions{
			NewDropRoleAction(mail),
		},
	}
}

// rules define
func (dsqr *DropRoleRule) rule(ctx context.Context, metric *DropRoleMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range dsqr.actions {
			action.action(ctx, stmt)
		}
	}
}

//DropRLActions define Actions
type DropRLActions interface {
	action(ctx context.Context, stmt string)
}

//DropRoleAction define mail
type DropRoleAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewDropRoleAction return action.mail
func NewDropRoleAction(mail *mail.Server) *DropRoleAction {
	return &DropRoleAction{
		mail: mail,
	}
}

// send mails
func (dsqa *DropRoleAction) action(ctx context.Context, stmt string) {
	if err := dsqa.mail.Send(ctx, "Audit: Drop role", stmt); err != nil {
		log.Errorf(ctx, "got error when do Drop role action, err:%s", err)
	}
}
