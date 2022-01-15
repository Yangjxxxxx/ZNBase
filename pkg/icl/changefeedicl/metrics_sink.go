package changefeedicl

import (
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

//
type metricsSink struct {
	metrics *Metrics
	wrapped Sink
}

func (s *metricsSink) GetTopics() (map[string]string, error) {
	return s.wrapped.GetTopics()
}

func makeMetricsSink(metrics *Metrics, s Sink) *metricsSink {
	m := &metricsSink{
		metrics: metrics,
		wrapped: s,
	}
	return m
}

func (s *metricsSink) EmitRow(
	ctx context.Context,
	table *sqlbase.TableDescriptor,
	key, value []byte,
	updated hlc.Timestamp,
	ddl bool,
	desc sqlbase.DescriptorProto,
	databaseName string,
	schemaName string,
	operate TableOperate,
	DB *client.DB,
	Executor sqlutil.InternalExecutor,
	poller *poller,
) (bool, error) {
	start := timeutil.Now()
	succeedEmit, err := s.wrapped.EmitRow(ctx, table, key, value, updated, ddl, desc, databaseName, schemaName, operate, DB, Executor, poller)
	if err == nil {
		s.metrics.EmittedMessages.Inc(1)
		s.metrics.EmittedBytes.Inc(int64(len(key) + len(value)))
		s.metrics.EmitNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return succeedEmit, err
}

func (s *metricsSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	start := timeutil.Now()
	err := s.wrapped.EmitResolvedTimestamp(ctx, encoder, resolved)
	if err == nil {
		s.metrics.EmittedMessages.Inc(1)
		// TODO(exDB): This wasn't correct. The wrapped sink may emit the payload
		// any number of times.
		// s.metrics.EmittedBytes.Inc(int64(len(payload)))
		s.metrics.EmitNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}

func (s *metricsSink) Flush(ctx context.Context) error {
	start := timeutil.Now()
	err := s.wrapped.Flush(ctx)
	if err == nil {
		s.metrics.Flushes.Inc(1)
		s.metrics.FlushNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}
func (s *metricsSink) Close() error {
	return s.wrapped.Close()
}
