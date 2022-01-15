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

//CreateSequenceMetric is the metric of Create Sequence
type CreateSequenceMetric struct {
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context       //accept context
	metrics *CreateSQMetrics      //get metrics information
	rules   []CreateSQRules       //realize 1 metric to n rules
}

//CreateSQMetrics contain CatchCount
type CreateSQMetrics struct {
	CatchCount *metric.Counter
}

//NewCreateSequenceMetric init a a new metric
func NewCreateSequenceMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *CreateSequenceMetric {
	//get metric information
	metrics := &CreateSQMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.sequence.create",
				Help:        "create a new sequence",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	// use rules to send mail
	return &CreateSequenceMetric{
		ie:      ie,
		ctx:     ctx,
		metrics: metrics,
		rules: []CreateSQRules{
			NewCreateSequenceRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (csqm *CreateSequenceMetric) RegistMetric() interface{} {
	return csqm.metrics
}

//Metric check rules
func (csqm *CreateSequenceMetric) Metric(info interface{}) {

	csqm.metrics.CatchCount.Inc(1)

	// check all rules
	for _, rule := range csqm.rules {
		switch info.(type) {
		case *infos.CreateSequenceInfo:
			rule.rule(csqm.ctx, csqm, info.(*infos.CreateSequenceInfo))
		}
	}
}

//EventLog define time to exec to do count
func (csqm *CreateSequenceMetric) EventLog(ctx context.Context) (int, bool) {
	row, err := csqm.ie.QueryRow(
		ctx,
		"Get Create Schema Count in 5 Seconds",
		nil,
		"SELECT count(1) FROM eventlog WHERE \"eventType\" = $1 AND \"timestamp\" > $2",
		string(sql.EventLogCreateSequence),
		timeutil.Now().Add(-createSQTime),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventLogCreateSequence, err)
	}
	if row != nil {
		return int(*row[0].(*tree.DInt)), true
	}
	return 0, false
}

//CreateSQRules define Rules
type CreateSQRules interface {
	rule(ctx context.Context, metric *CreateSequenceMetric, info *infos.CreateSequenceInfo)
}

//CreateSequenceRule get actions
type CreateSequenceRule struct {
	actions []CreateSQActions // realize 1 rule to n actions
}

//NewCreateSequenceRule use action to send mails
func NewCreateSequenceRule(mail *mail.Server) *CreateSequenceRule {
	return &CreateSequenceRule{
		actions: []CreateSQActions{
			NewCreateSequenceAction(mail),
		},
	}
}

var createSQTime = 5 * time.Second
var createSQFrequency = 10

// rules define
func (csqr *CreateSequenceRule) rule(
	ctx context.Context, metric *CreateSequenceMetric, info *infos.CreateSequenceInfo,
) {
	frequency, ok := metric.EventLog(ctx)
	if ok {
		if frequency > createSQFrequency {
			for _, action := range csqr.actions {
				action.action(ctx, info)
			}
		}
	}
}

//CreateSQActions define Actions
type CreateSQActions interface {
	action(ctx context.Context, info *infos.CreateSequenceInfo)
}

//CreateSequenceAction define mail
type CreateSequenceAction struct {
	mail *mail.Server // define a Server to send mails
}

//NewCreateSequenceAction return action.mail
func NewCreateSequenceAction(mail *mail.Server) *CreateSequenceAction {
	return &CreateSequenceAction{
		mail: mail,
	}
}

// send mails
func (csqa *CreateSequenceAction) action(ctx context.Context, info *infos.CreateSequenceInfo) {
	if err := csqa.mail.Send(ctx, "Audit: Create Sequence", info.String()); err != nil {
		log.Errorf(ctx, "got error when do Create Sequence action, err:%s", err)
	}
}
