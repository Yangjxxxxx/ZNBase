package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//NodeJoinMetric is the metric of Node Join
type NodeJoinMetric struct {
	ctx     context.Context //accept context
	metrics *NJoinMetrics   //get metrics information
	rules   []NJoinRules    //realize 1 metric to n rules
}

//NJoinMetrics contain CatchCount
type NJoinMetrics struct {
	CatchCount *metric.Counter //metric define
}

//NewNodeJoinMetric init a a new metric
func NewNodeJoinMetric(ctx context.Context, mail *mail.Server) *NodeJoinMetric {
	//get metric information
	metrics := &NJoinMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.node.Join",
				Help:        "node Join",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//use rules to send mail
	return &NodeJoinMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []NJoinRules{
			NewNodeJoinRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (nrsm *NodeJoinMetric) RegistMetric() interface{} {
	return nrsm.metrics
}

//Metric check rules
func (nrsm *NodeJoinMetric) Metric(info interface{}) {

	//	increment by 1
	nrsm.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range nrsm.rules {
		switch info.(type) {
		case *infos.NodeInfo:
			rule.rule(nrsm.ctx, nrsm, info.(*infos.NodeInfo))
		}
	}
}

//NJoinRules define Rules
type NJoinRules interface {
	rule(ctx context.Context, metric *NodeJoinMetric, info *infos.NodeInfo)
}

//NodeJoinRule get actions
type NodeJoinRule struct {
	actions []NJoinActions //realize 1 rule to n actions
}

//NewNodeJoinRule use action to send mails
func NewNodeJoinRule(mail *mail.Server) *NodeJoinRule {
	return &NodeJoinRule{
		actions: []NJoinActions{
			NewNodeJoinAction(mail),
		},
	}
}

//rules define
func (nrsr *NodeJoinRule) rule(ctx context.Context, metric *NodeJoinMetric, info *infos.NodeInfo) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range nrsr.actions {
			action.action(ctx, info)
		}
	}
}

//NJoinActions define Actions
type NJoinActions interface {
	action(ctx context.Context, info *infos.NodeInfo)
}

//NodeJoinAction define mail
type NodeJoinAction struct {
	mail *mail.Server //define a Server to send mails
}

//NewNodeJoinAction return action.mail
func NewNodeJoinAction(mail *mail.Server) *NodeJoinAction {
	return &NodeJoinAction{
		mail: mail,
	}
}

//send mails
func (nrsa *NodeJoinAction) action(ctx context.Context, info *infos.NodeInfo) {
	if err := nrsa.mail.Send(ctx, "Audit: Node Join", info.String()); err != nil {
		log.Errorf(ctx, "got error when do node join action, err:%s", err)
	}
}
