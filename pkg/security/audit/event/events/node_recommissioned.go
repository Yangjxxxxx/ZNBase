package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//NodeRecommissionedMetric is the metric of Node Recommissioned
type NodeRecommissionedMetric struct {
	ctx     context.Context //accept context
	metrics *NRCMMetrics    //get metrics information
	rules   []NRCMRules     //realize 1 metric to n rules
}

//NRCMMetrics contain CatchCount
type NRCMMetrics struct {
	CatchCount *metric.Counter //metric define
}

//NewNodeRecommissionedMetric init a a new metric
func NewNodeRecommissionedMetric(ctx context.Context, mail *mail.Server) *NodeRecommissionedMetric {
	//get metric information
	metrics := &NRCMMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.node.recommissioned",
				Help:        "node recommissioned",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//use rules to send mail
	return &NodeRecommissionedMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []NRCMRules{
			NewNodeRecommissionedRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (ndmm *NodeRecommissionedMetric) RegistMetric() interface{} {
	return ndmm.metrics
}

//Metric check rules
func (ndmm *NodeRecommissionedMetric) Metric(info interface{}) {

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

//NRCMRules define Rules
type NRCMRules interface {
	rule(ctx context.Context, metric *NodeRecommissionedMetric, stmt string)
}

//NodeRecommissionedRule get actions
type NodeRecommissionedRule struct {
	actions []NRCMActions //realize 1 rule to n actions
}

//NewNodeRecommissionedRule use action to send mails
func NewNodeRecommissionedRule(mail *mail.Server) *NodeRecommissionedRule {
	return &NodeRecommissionedRule{
		actions: []NRCMActions{
			NewNodeRecommissionedAction(mail),
		},
	}
}

//rules define
func (ndmr *NodeRecommissionedRule) rule(
	ctx context.Context, metric *NodeRecommissionedMetric, stmt string,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range ndmr.actions {
			action.action(ctx, stmt)
		}
	}
}

//NRCMActions get actions
type NRCMActions interface {
	action(ctx context.Context, stmt string)
}

//NodeRecommissionedAction define mail
type NodeRecommissionedAction struct {
	mail *mail.Server //define a Server to send mails
}

//NewNodeRecommissionedAction return action.mail
func NewNodeRecommissionedAction(mail *mail.Server) *NodeRecommissionedAction {
	return &NodeRecommissionedAction{
		mail: mail,
	}
}

//send mails
func (ndma *NodeRecommissionedAction) action(ctx context.Context, stmt string) {
	if err := ndma.mail.Send(ctx, "Audit: Node Recommissioned", stmt); err != nil {
		log.Errorf(ctx, "got error when do Node Recommissioned action, err:%s", err)
	}
}
