package events

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

//CreateViewMetric is the metric of Create View
type CreateViewMetric struct {
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context       // accept context
	metrics *CreateVMetrics       // get metrics information
	rules   []CreateVRules        // realize 1 metric to n rules
}

//CreateVMetrics contain CatchCount
type CreateVMetrics struct {
	CatchCount *metric.Counter
}

//NewCreateViewMetric init a new metric
func NewCreateViewMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *CreateViewMetric {
	// get metric information
	metrics := &CreateVMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.view.create",
				Help:        "create a new view",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &CreateViewMetric{
		ie:      ie,
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateVRules{
			NewCreateViewRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (cvm *CreateViewMetric) RegistMetric() interface{} {
	return cvm.metrics
}

//Metric check rules
func (cvm *CreateViewMetric) Metric(info interface{}) {

	//	increment by 1
	cvm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range cvm.rules {
		switch info.(type) {
		case *infos.CreateViewInfo:
			rule.rule(cvm.ctx, cvm, info.(*infos.CreateViewInfo))
		}
	}
}

//EventLog define time to exec to do count
func (cvm *CreateViewMetric) EventLog(ctx context.Context) (int, bool) {
	row, err := cvm.ie.QueryRow(
		ctx,
		"Get Create View Count In 5 Seconds",
		nil,
		"SELECT count(1) FROM eventlog WHERE \"eventType\" = $1 AND \"timestamp\" > $2",
		string(sql.EventLogCreateView),
		timeutil.Now().Add(-createVTime),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventLogCreateView, err)
	}
	if row != nil {
		return int(*row[0].(*tree.DInt)), true
	}
	return 0, false
}

// CreateVRules define Rules
type CreateVRules interface {
	rule(ctx context.Context, metric *CreateViewMetric, info *infos.CreateViewInfo)
}

//CreateViewRule get actions
type CreateViewRule struct {
	actions []CreateVActions // realize 1 rule to n actions
}

//NewCreateViewRule use action to send mails
func NewCreateViewRule(mail *mail.Server) *CreateViewRule {
	return &CreateViewRule{
		actions: []CreateVActions{
			NewCreateViewAction(mail),
		},
	}
}

var createVTime = 5 * time.Second
var createVFrequency = 10

// rules define
func (cvr *CreateViewRule) rule(
	ctx context.Context, metric *CreateViewMetric, info *infos.CreateViewInfo,
) {
	frequency, ok := metric.EventLog(ctx)
	if ok {
		if frequency > createVFrequency {
			for _, action := range cvr.actions {
				action.action(ctx, info)
			}
		}
	}
}

//CreateVActions define Actions
type CreateVActions interface {
	action(ctx context.Context, info *infos.CreateViewInfo)
}

//CreateViewAction define mail
type CreateViewAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewCreateViewAction return action.mail
func NewCreateViewAction(mail *mail.Server) *CreateViewAction {
	return &CreateViewAction{
		mail: mail,
	}
}

// send mails
func (cva *CreateViewAction) action(ctx context.Context, info *infos.CreateViewInfo) {
	if err := cva.mail.Send(ctx, "Audit: Create View", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Create View action, err:%s", err)
	}
}
