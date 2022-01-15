package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//NodeDecommissionedMetric is the metric of Node Decommissioned
type NodeDecommissionedMetric struct {
	ctx     context.Context //accept context
	metrics *NDCMMetrics    //get metrics information
	rules   []NDCMRules     //realize 1 metric to n rules
}

//NDCMMetrics contain CatchCount
type NDCMMetrics struct {
	CatchCount *metric.Counter //metric define
}

//NewNodeDecommissionedMetric init a a new metric
func NewNodeDecommissionedMetric(ctx context.Context, mail *mail.Server) *NodeDecommissionedMetric {
	//get metric information
	metrics := &NDCMMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.node.decommissioned",
				Help:        "node decommissioned",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//use rules to send mail
	return &NodeDecommissionedMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []NDCMRules{
			NewNodeDecommissionedRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (ndmm *NodeDecommissionedMetric) RegistMetric() interface{} {
	return ndmm.metrics
}

//Metric check rules
func (ndmm *NodeDecommissionedMetric) Metric(info interface{}) {

	//	increment by 1
	ndmm.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range ndmm.rules {
		switch info.(type) {
		case string:
			rule.rule(ndmm.ctx, ndmm, info.(string))
		}
	}
}

//NDCMRules define Rules
type NDCMRules interface {
	rule(ctx context.Context, metric *NodeDecommissionedMetric, stmt string)
}

//NodeDecommissionedRule get actions
type NodeDecommissionedRule struct {
	actions []NDCMActions //realize 1 rule to n actions
}

//NewNodeDecommissionedRule use action to send mails
func NewNodeDecommissionedRule(mail *mail.Server) *NodeDecommissionedRule {
	return &NodeDecommissionedRule{
		actions: []NDCMActions{
			NewNodeDecommissionedAction(mail),
		},
	}
}

//rules define
func (ndmr *NodeDecommissionedRule) rule(
	ctx context.Context, metric *NodeDecommissionedMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range ndmr.actions {
			action.action(ctx, stmt)
		}
	}
}

//NDCMActions define Actions
type NDCMActions interface {
	action(ctx context.Context, stmt string)
}

//NodeDecommissionedAction define mail
type NodeDecommissionedAction struct {
	mail *mail.Server //define a Server to send mails
}

//NewNodeDecommissionedAction return action.mail
func NewNodeDecommissionedAction(mail *mail.Server) *NodeDecommissionedAction {
	return &NodeDecommissionedAction{
		mail: mail,
	}
}

//send mails
func (ndma *NodeDecommissionedAction) action(ctx context.Context, stmt string) {
	if err := ndma.mail.Send(ctx, "Audit: Node Decommissioned", stmt); err != nil {
		log.Errorf(ctx, "got error when do Node Decommissioned action, err:%s", err)
	}
}
