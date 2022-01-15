package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//NodeRestartMetric is the metric of Node Restart
type NodeRestartMetric struct {
	ctx     context.Context  //accept context
	metrics *NReStartMetrics //get metrics information
	rules   []NReStartRules  //realize 1 metric to n rules
}

//NReStartMetrics contain CatchCount
type NReStartMetrics struct {
	CatchCount *metric.Counter //metric define
}

//NewNodeRestartMetric init a a new metric
func NewNodeRestartMetric(ctx context.Context, mail *mail.Server) *NodeRestartMetric {
	//get metric information
	metrics := &NReStartMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.node.restart",
				Help:        "node restart",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	//use rules to send mail
	return &NodeRestartMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []NReStartRules{
			NewNodeRestartRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (nrsm *NodeRestartMetric) RegistMetric() interface{} {
	return nrsm.metrics
}

//Metric check rules
func (nrsm *NodeRestartMetric) Metric(info interface{}) {

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

//NReStartRules define Rules
type NReStartRules interface {
	rule(ctx context.Context, metric *NodeRestartMetric, info *infos.NodeInfo)
}

//NodeRestartRule get actions
type NodeRestartRule struct {
	actions []NReStartActions //realize 1 rule to n actions
}

//NewNodeRestartRule use action to send mails
func NewNodeRestartRule(mail *mail.Server) *NodeRestartRule {
	return &NodeRestartRule{
		actions: []NReStartActions{
			NewNodeRestartAction(mail),
		},
	}
}

//rules define
func (nrsr *NodeRestartRule) rule(
	ctx context.Context, metric *NodeRestartMetric, info *infos.NodeInfo,
) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range nrsr.actions {
			action.action(ctx, info)
		}
	}
}

//NReStartActions define Actions
type NReStartActions interface {
	action(ctx context.Context, info *infos.NodeInfo)
}

//NodeRestartAction define mail
type NodeRestartAction struct {
	mail *mail.Server //define a Server to send mails
}

//NewNodeRestartAction return action.mail
func NewNodeRestartAction(mail *mail.Server) *NodeRestartAction {
	return &NodeRestartAction{
		mail: mail,
	}
}

//send mails
func (nrsa *NodeRestartAction) action(ctx context.Context, info *infos.NodeInfo) {
	if err := nrsa.mail.Send(ctx, "Audit: Node Restart", info.String()); err != nil {
		log.Errorf(ctx, "got error when do node restart action, err:%s", err)
	}
}
