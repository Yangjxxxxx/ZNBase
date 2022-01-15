package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//ClusterInitMetric is the metric of Cluster Init
type ClusterInitMetric struct {
	ctx     context.Context     // accept context
	metrics *ClusterInitMetrics // get metrics information
	rules   []clusterInitRules  // realize 1 metric to n rules
}

//ClusterInitMetrics contain CatchCount
type ClusterInitMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewClusterInitMetric init a a new metric
func NewClusterInitMetric(ctx context.Context, mail *mail.Server) *ClusterInitMetric {
	// get metric information
	metrics := &ClusterInitMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.cluster.init",
				Help:        "cluster init",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &ClusterInitMetric{
		ctx:     ctx,
		metrics: metrics,
		rules: []clusterInitRules{
			NewClusterInitRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (cim *ClusterInitMetric) RegistMetric() interface{} {
	return cim.metrics
}

//Metric check rules
func (cim *ClusterInitMetric) Metric(info interface{}) {

	//	increment by 1
	cim.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range cim.rules {
		switch info.(type) {
		case string:
			rule.rule(cim.ctx, cim, info.(string))
		}
	}
}

//clusterInitRules define Rules
type clusterInitRules interface {
	rule(ctx context.Context, metric *ClusterInitMetric, stmt string)
}

//ClusterInitRule  get actions
type ClusterInitRule struct {
	actions []clusterInitActions // realize 1 rule to n actions
}

//NewClusterInitRule use action to send mails
func NewClusterInitRule(mail *mail.Server) *ClusterInitRule {
	return &ClusterInitRule{
		actions: []clusterInitActions{
			NewClusterInitAction(mail),
		},
	}
}

// rules define
func (cir *ClusterInitRule) rule(ctx context.Context, metric *ClusterInitMetric, stmt string) {
	if metric.metrics.CatchCount.Count() > 0 {
		for _, action := range cir.actions {
			action.action(ctx, stmt)
		}
	}
}

//clusterInitActions define Actions
type clusterInitActions interface {
	action(ctx context.Context, stmt string)
}

//ClusterInitAction define mail
type ClusterInitAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewClusterInitAction return action.mail
func NewClusterInitAction(mail *mail.Server) *ClusterInitAction {
	return &ClusterInitAction{
		mail: mail,
	}
}

// send mails
func (cia *ClusterInitAction) action(ctx context.Context, stmt string) {
	if err := cia.mail.Send(ctx, "Audit: Cluster init", stmt); err != nil {
		log.Errorf(ctx, "got error when do Cluster init action, err:%s", err)
	}
}
