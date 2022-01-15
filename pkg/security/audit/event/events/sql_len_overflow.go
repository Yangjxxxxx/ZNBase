package events

import (
	"context"

	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
)

//SQLLenOverflowMetric is the metric of SQLLen Over flow
type SQLLenOverflowMetric struct {
	ie      *sql.InternalExecutor // used to wrap operation without transaction
	ctx     context.Context
	metrics *SQLLOMetrics
	rules   []SQLLenRules
}

//SQLLOMetrics contain CatchCount
type SQLLOMetrics struct {
	CatchCount *metric.Counter
}

//NewSQLLenOverflowMetric init a new metric
func NewSQLLenOverflowMetric(
	ctx context.Context, mail *mail.Server, ie *sql.InternalExecutor,
) *SQLLenOverflowMetric {
	metrics := &SQLLOMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        "audit.sql.length",
				Help:        "audit sql statement length",
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
	return &SQLLenOverflowMetric{
		ie:      ie,
		ctx:     ctx,
		metrics: metrics,
		rules: []SQLLenRules{
			NewSQLLenOverflowRule(mail),
		},
	}
}

//RegistMetric interface return metrics
func (slom *SQLLenOverflowMetric) RegistMetric() interface{} {
	return slom.metrics
}

//Metric check rules
func (slom *SQLLenOverflowMetric) Metric(info interface{}) {

	//	increment by 1
	slom.metrics.CatchCount.Inc(1)

	//	check all rules
	for _, rule := range slom.rules {
		switch info.(type) {
		case string:
			rule.rule(slom.ctx, slom, info.(string))
		}
	}
}

//SQLLenRules define Rules
type SQLLenRules interface {
	rule(ctx context.Context, metric *SQLLenOverflowMetric, stmt string)
}

//SQLLenOverflowRule get actions
type SQLLenOverflowRule struct {
	actions []SQLLenActions
}

//NewSQLLenOverflowRule use action to send mails
func NewSQLLenOverflowRule(mail *mail.Server) *SQLLenOverflowRule {
	return &SQLLenOverflowRule{
		actions: []SQLLenActions{
			NewSQLLenOverflowAction(mail),
		},
	}
}

func (slor *SQLLenOverflowRule) rule(
	ctx context.Context, metric *SQLLenOverflowMetric, stmt string,
) {
	row, err := metric.ie.QueryRow(
		ctx,
		"Get SQL Length Overflow Count",
		nil,
		"select count(1) from eventlog where \"eventType\" = $1",
		string(sql.EventSQLLenOverflow),
	)
	if err != nil {
		log.Warningf(ctx, "got error when get total %s type count from eventlog, err:%s", sql.EventSQLLenOverflow, err)
	}
	if row != nil {
		if int(*row[0].(*tree.DInt)) > 0 {
			for _, action := range slor.actions {
				action.action(ctx, stmt)
			}
		}
	}
}

//SQLLenActions define Actions
type SQLLenActions interface {
	action(ctx context.Context, stmt string)
}

//SQLLenOverflowAction define mail
type SQLLenOverflowAction struct {
	mail *mail.Server
}

//NewSQLLenOverflowAction return action.mail
func NewSQLLenOverflowAction(mail *mail.Server) *SQLLenOverflowAction {
	return &SQLLenOverflowAction{
		mail: mail,
	}
}

func (sloa *SQLLenOverflowAction) action(ctx context.Context, stmt string) {
	if err := sloa.mail.Send(ctx, "Audit: SQL Len Overflow", stmt); err != nil {
		log.Errorf(ctx, "got error when do SQL Len Overflow action, err:%s", err)
	}
}
