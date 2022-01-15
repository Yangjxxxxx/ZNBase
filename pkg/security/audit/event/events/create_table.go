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

//CreateTableMetric is the metric of Create Table
type CreateTableMetric struct {
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context
	metrics *CreateTBMetrics
	rules   []CreateTBRules
}

//CreateTBMetrics contain CatchCount
type CreateTBMetrics struct {
	CatchCount *metric.Counter
}

//NewCreateTableMetric init a new metric
func NewCreateTableMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *CreateTableMetric {

	metrics := &CreateTBMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.table.create",
				Help:        "create a new table",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	return &CreateTableMetric{
		ie:      ie,
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateTBRules{
			NewCreateTableRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (ctm *CreateTableMetric) RegistMetric() interface{} {
	return ctm.metrics
}

//Metric check rules
func (ctm *CreateTableMetric) Metric(info interface{}) {

	ctm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range ctm.rules {
		switch info.(type) {
		case *infos.CreateTableInfo:
			rule.rule(ctm.ctx, ctm, info.(*infos.CreateTableInfo))
		}
	}
}

//EventLog define time to exec to do count
func (ctm *CreateTableMetric) EventLog(ctx context.Context) (int, bool) {
	row, err := ctm.ie.QueryRow(
		ctx,
		"Get Create Table Count In 5 Seconds",
		nil,
		"SELECT count(1) FROM eventlog WHERE \"eventType\" = $1 AND \"timestamp\" > $2",
		string(sql.EventLogCreateTable),
		timeutil.Now().Add(-createTableTime),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventLogCreateTable, err)
	}
	if row != nil {
		return int(*row[0].(*tree.DInt)), true
	}
	return 0, false
}

//CreateTBRules define Rules
type CreateTBRules interface {
	rule(ctx context.Context, metric *CreateTableMetric, info *infos.CreateTableInfo)
}

//CreateTableRule get actions
type CreateTableRule struct {
	actions []CreateTBActions
}

//NewCreateTableRule use action to send mails
func NewCreateTableRule(mail *mail.Server) *CreateTableRule {
	return &CreateTableRule{
		actions: []CreateTBActions{
			NewCreateTableAction(mail),
		},
	}
}

var createTableTime = time.Second * 5
var createTableFrequency = 10

func (ctr *CreateTableRule) rule(
	ctx context.Context, metric *CreateTableMetric, info *infos.CreateTableInfo,
) {
	frequency, ok := metric.EventLog(ctx)
	if ok {
		if frequency > createTableFrequency {
			for _, action := range ctr.actions {
				action.action(ctx, info)
			}
		}
	}
}

//CreateTBActions  define Actions
type CreateTBActions interface {
	action(ctx context.Context, info *infos.CreateTableInfo)
}

//CreateTableAction define mail
type CreateTableAction struct {
	mail *mail.Server
}

//NewCreateTableAction return action.mail
func NewCreateTableAction(mail *mail.Server) *CreateTableAction {
	return &CreateTableAction{
		mail: mail,
	}
}

func (cta *CreateTableAction) action(ctx context.Context, info *infos.CreateTableInfo) {
	if err := cta.mail.Send(ctx, "Audit: Create Table", info.String()); err != nil {
		log.Errorf(ctx, "got error when do create Table action, err:%s", err)
	}
}
