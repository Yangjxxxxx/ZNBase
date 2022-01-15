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

//CreateIndexMetric is the metric of Create Index
type CreateIndexMetric struct {
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context       // accept context
	metrics *CreateIDXMetrics     // get metrics information
	rules   []CreateIDXRules      // realize 1 metric to n rules
}

//CreateIDXMetrics contain CatchCount
type CreateIDXMetrics struct {
	CatchCount *metric.Counter // metric define
}

//NewCreateIndexMetric init a new metric
func NewCreateIndexMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *CreateIndexMetric {
	// get metric information
	metrics := &CreateIDXMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.index.create",
				Help:        "create a new index",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &CreateIndexMetric{
		ie:      ie,
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateIDXRules{
			NewCreateIndexRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (cidm *CreateIndexMetric) RegistMetric() interface{} {
	return cidm.metrics
}

//Metric check rules
func (cidm *CreateIndexMetric) Metric(info interface{}) {

	cidm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range cidm.rules {
		switch info.(type) {
		case *infos.CreateIndexInfo:
			rule.rule(cidm.ctx, cidm, info.(*infos.CreateIndexInfo))
		}
	}
}

//EventLog define time to exec to do count
func (cidm *CreateIndexMetric) EventLog(ctx context.Context) (int, bool) {
	row, err := cidm.ie.QueryRow(
		ctx,
		"Get Create Index Count In 5 Seconds",
		nil,
		"SELECT count(1) FROM eventlog WHERE \"eventType\" = $1 AND \"timestamp\" > $2",
		string(sql.EventLogCreateIndex),
		timeutil.Now().Add(-createIndexTime),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventLogCreateIndex, err)
	}
	if row != nil {
		return int(*row[0].(*tree.DInt)), true
	}
	return 0, false
}

//CreateIDXRules  define Rules
type CreateIDXRules interface {
	rule(ctx context.Context, metric *CreateIndexMetric, info *infos.CreateIndexInfo)
}

//CreateIndexRule get actions
type CreateIndexRule struct {
	actions []CreateIDXActions // realize 1 rule to n actions
}

//NewCreateIndexRule use action to send mails
func NewCreateIndexRule(mail *mail.Server) *CreateIndexRule {
	return &CreateIndexRule{
		actions: []CreateIDXActions{
			NewCreateIndexAction(mail),
		},
	}
}

var createIndexTime = 5 * time.Second
var createIndexFrequency = 10

// rules define
func (cidr *CreateIndexRule) rule(
	ctx context.Context, metric *CreateIndexMetric, info *infos.CreateIndexInfo,
) {
	frequency, ok := metric.EventLog(ctx)
	if ok {
		if frequency > createIndexFrequency {
			for _, action := range cidr.actions {
				action.action(ctx, info)
			}
		}
	}
}

//CreateIDXActions define Actions
type CreateIDXActions interface {
	action(ctx context.Context, info *infos.CreateIndexInfo)
}

//CreateIndexAction define mail
type CreateIndexAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewCreateIndexAction return action.mail
func NewCreateIndexAction(mail *mail.Server) *CreateIndexAction {
	return &CreateIndexAction{
		mail: mail,
	}
}

// send mails
func (cida *CreateIndexAction) action(ctx context.Context, info *infos.CreateIndexInfo) {
	if err := cida.mail.Send(ctx, "Audit: Create Index", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Create Index action, err:%s", err)
	}
}
