// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

var zeroTS hlc.Timestamp

type asyncProducerMock struct {
	inputCh     chan *sarama.ProducerMessage
	successesCh chan *sarama.ProducerMessage
	errorsCh    chan *sarama.ProducerError
}

func (p asyncProducerMock) Input() chan<- *sarama.ProducerMessage     { return p.inputCh }
func (p asyncProducerMock) Successes() <-chan *sarama.ProducerMessage { return p.successesCh }
func (p asyncProducerMock) Errors() <-chan *sarama.ProducerError      { return p.errorsCh }
func (p asyncProducerMock) AsyncClose()                               { panic(`unimplemented`) }
func (p asyncProducerMock) Close() error {
	close(p.inputCh)
	close(p.successesCh)
	close(p.errorsCh)
	return nil
}

func TestKafkaSink(t *testing.T) {
	defer leaktest.AfterTest(t)()

	table := func(name string, id sqlbase.ID) *sqlbase.TableDescriptor {
		return &sqlbase.TableDescriptor{Name: name, ID: id}
	}

	ctx := context.Background()
	p := asyncProducerMock{
		inputCh:     make(chan *sarama.ProducerMessage, 1),
		successesCh: make(chan *sarama.ProducerMessage, 1),
		errorsCh:    make(chan *sarama.ProducerError, 1),
	}
	sink := &kafkaSink{
		producer: p,
	}
	tables := make(map[uint32]*sqlbase.TableDescriptor)
	tables[keys.PostgresSchemaID+1] = table(`t`, keys.PostgresSchemaID+1)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	poller := initPoller(tables, clock)
	h := sha256.New()
	h.Write([]byte(fmt.Sprint(keys.PostgresSchemaID + 1)))
	str := fmt.Sprintf("%X", h.Sum(nil))
	sink.mu.topics = map[string]string{`t` + str: `t`}
	sink.start()
	defer func() {
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// No inflight
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Timeout
	if _, err := sink.EmitRow(ctx, table(`t`, keys.PostgresSchemaID+1), []byte(`1`), nil, zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller); err != nil {
		t.Fatal(err)
	}
	m1 := <-p.inputCh
	for i := 0; i < 2; i++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		if err := sink.Flush(timeoutCtx); !testutils.IsError(
			err, `context deadline exceeded`,
		) {
			t.Fatalf(`expected "context deadline exceeded" error got: %+v`, err)
		}
	}
	go func() { p.successesCh <- m1 }()
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Check no inflight again now that we've sent something
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Mixed success and error.
	if _, err := sink.EmitRow(ctx, table(`t`, keys.PostgresSchemaID+1), []byte(`2`), nil, zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller); err != nil {
		t.Fatal(err)
	}
	m2 := <-p.inputCh
	if _, err := sink.EmitRow(ctx, table(`t`, keys.PostgresSchemaID+1), []byte(`3`), nil, zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller); err != nil {
		t.Fatal(err)
	}
	m3 := <-p.inputCh
	if _, err := sink.EmitRow(ctx, table(`t`, keys.PostgresSchemaID+1), []byte(`4`), nil, zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller); err != nil {
		t.Fatal(err)
	}
	m4 := <-p.inputCh
	go func() { p.successesCh <- m2 }()
	go func() {
		p.errorsCh <- &sarama.ProducerError{
			Msg: m3,
			Err: errors.New("m3"),
		}
	}()
	go func() { p.successesCh <- m4 }()
	if err := sink.Flush(ctx); !testutils.IsError(err, `m3`) {
		t.Fatalf(`expected "m3" error got: %+v`, err)
	}

	// Check simple success again after error
	if _, err := sink.EmitRow(ctx, table(`t`, keys.PostgresSchemaID+1), []byte(`5`), nil, zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller); err != nil {
		t.Fatal(err)
	}
	m5 := <-p.inputCh
	go func() { p.successesCh <- m5 }()
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func initPoller(tables map[uint32]*sqlbase.TableDescriptor, clock *hlc.Clock) *poller {
	details := jobspb.ChangefeedDetails{
		Targets: make(map[sqlbase.ID]jobspb.ChangefeedTarget),
	}
	for id, table := range tables {
		details.Targets[sqlbase.ID(id)] = jobspb.ChangefeedTarget{StatementTimeName: table.Name}
	}
	p := &poller{
		clock:   clock,
		details: details,
	}
	return p
}

func TestKafkaSinkEscaping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	table := func(name string, id sqlbase.ID) *sqlbase.TableDescriptor {
		return &sqlbase.TableDescriptor{Name: name, ID: id}
	}

	ctx := context.Background()
	p := asyncProducerMock{
		inputCh:     make(chan *sarama.ProducerMessage, 1),
		successesCh: make(chan *sarama.ProducerMessage, 1),
		errorsCh:    make(chan *sarama.ProducerError, 1),
	}
	tables := make(map[uint32]*sqlbase.TableDescriptor)
	tables[keys.PostgresSchemaID+1] = table(`t`, keys.PostgresSchemaID+1)
	tables[keys.PostgresSchemaID+2] = table(`☃`, keys.PostgresSchemaID+2)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	poller := initPoller(tables, clock)
	sink := &kafkaSink{
		producer: p,
	}
	h := sha256.New()
	h.Write([]byte(fmt.Sprint(keys.PostgresSchemaID + 2)))
	str := fmt.Sprintf("%X", h.Sum(nil))
	sink.mu.topics = map[string]string{SQLNameToKafkaName(`☃`) + str: `☃`}
	sink.start()
	defer func() { require.NoError(t, sink.Close()) }()
	if _, err := sink.EmitRow(ctx, table(`☃`, keys.PostgresSchemaID+2), []byte(`k☃`), []byte(`v☃`), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller); err != nil {
		t.Fatal(err)
	}
	m := <-p.inputCh
	require.Equal(t, `_u2603_`+str, m.Topic)
	require.Equal(t, sarama.ByteEncoder(`k☃`), m.Key)
	require.Equal(t, sarama.ByteEncoder(`v☃`), m.Value)
}

type testEncoder struct{}

func (testEncoder) EncodeKey(encodeRow) ([]byte, error) { panic(`unimplemented`) }
func (testEncoder) EncodeValue(
	context.Context, encodeRow, *client.DB,
) ([]byte, bool, sqlbase.DescriptorProto, string, string, TableOperate, error) {
	panic(`unimplemented`)
}
func (testEncoder) EncodeResolvedTimestamp(_ string, ts hlc.Timestamp) ([]byte, error) {
	return []byte(ts.String()), nil
}

func TestSQLSink(t *testing.T) {
	defer leaktest.AfterTest(t)()

	table := func(name string, id sqlbase.ID) *sqlbase.TableDescriptor {
		return &sqlbase.TableDescriptor{Name: name, ID: id}
	}

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)

	sinkURL, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()
	sinkURL.Path = `d`

	targets := jobspb.ChangefeedTargets{
		0: jobspb.ChangefeedTarget{StatementTimeName: `foo`},
		1: jobspb.ChangefeedTarget{StatementTimeName: `bar`},
	}
	sink, err := makeSQLSink(sinkURL.String(), `sink`, targets)
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close()) }()
	// Empty
	require.NoError(t, sink.Flush(ctx))
	tables := make(map[uint32]*sqlbase.TableDescriptor)
	tables[keys.PostgresSchemaID+1] = table(`foo`, keys.PostgresSchemaID+1)
	tables[keys.PostgresSchemaID+2] = table(`bar`, keys.PostgresSchemaID+2)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	poller := initPoller(tables, clock)
	// Undeclared topic
	_, err = sink.EmitRow(ctx, table(`nope`, keys.PostgresSchemaID+3), nil, nil, zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
	require.EqualError(t,
		err, `cannot emit to undeclared table: nope`)

	// With one row, nothing flushes until Flush is called.
	_, err = sink.EmitRow(ctx, table(`foo`, keys.PostgresSchemaID+1), []byte(`k1`), []byte(`v0`), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
	require.NoError(t, err)
	sqlDB.CheckQueryResults(t, `SELECT key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{},
	)
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{{`k1`, `v0`}},
	)
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Verify the implicit flushing
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM sink`, [][]string{{`0`}})
	for i := 0; i < sqlSinkRowBatchSize+1; i++ {
		_, err = sink.EmitRow(ctx, table(`foo`, keys.PostgresSchemaID+1), []byte(`k1`), []byte(`v`+strconv.Itoa(i)), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
		require.NoError(t,
			err)
	}
	// Should have auto flushed after sqlSinkRowBatchSize
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM sink`, [][]string{{`3`}})
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM sink`, [][]string{{`4`}})
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Two tables interleaved in time
	_, err1 := sink.EmitRow(ctx, table(`foo`, keys.PostgresSchemaID+1), []byte(`kfoo`), []byte(`v0`), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
	require.NoError(t, err1)
	_, err2 := sink.EmitRow(ctx, table(`bar`, keys.PostgresSchemaID+2), []byte(`kbar`), []byte(`v0`), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
	require.NoError(t, err2)
	_, err3 := sink.EmitRow(ctx, table(`foo`, keys.PostgresSchemaID+1), []byte(`kfoo`), []byte(`v1`), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
	require.NoError(t, err3)
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT topic, key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{{`bar`, `kbar`, `v0`}, {`foo`, `kfoo`, `v0`}, {`foo`, `kfoo`, `v1`}},
	)
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Multiple keys interleaved in time. Use sqlSinkNumPartitions+1 keys to
	// guarantee that at lease two of them end up in the same partition.
	for i := 0; i < sqlSinkNumPartitions+1; i++ {
		_, err = sink.EmitRow(ctx, table(`foo`, keys.PostgresSchemaID+1), []byte(`v`+strconv.Itoa(i)), []byte(`v0`), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
		require.NoError(t,
			err)
	}
	for i := 0; i < sqlSinkNumPartitions+1; i++ {
		_, err = sink.EmitRow(ctx, table(`foo`, keys.PostgresSchemaID+1), []byte(`v`+strconv.Itoa(i)), []byte(`v1`), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
		require.NoError(t,
			err)
	}
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT partition, key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{
			{`0`, `v3`, `v0`},
			{`0`, `v3`, `v1`},
			{`1`, `v1`, `v0`},
			{`1`, `v2`, `v0`},
			{`1`, `v1`, `v1`},
			{`1`, `v2`, `v1`},
			{`2`, `v0`, `v0`},
			{`2`, `v0`, `v1`},
		},
	)
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Emit resolved
	var e testEncoder
	require.NoError(t, sink.EmitResolvedTimestamp(ctx, e, zeroTS))
	_, err = sink.EmitRow(ctx, table(`foo`, keys.PostgresSchemaID+1), []byte(`foo0`), []byte(`v0`), zeroTS, false, nil, "", "", UnknownOperation, nil, nil, poller)
	require.NoError(t, err)
	require.NoError(t, sink.EmitResolvedTimestamp(ctx, e, hlc.Timestamp{WallTime: 1}))
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t,
		`SELECT topic, partition, key, value, resolved FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{
			{`bar`, `0`, ``, ``, `0.000000000,0`},
			{`bar`, `0`, ``, ``, `0.000000001,0`},
			{`bar`, `1`, ``, ``, `0.000000000,0`},
			{`bar`, `1`, ``, ``, `0.000000001,0`},
			{`bar`, `2`, ``, ``, `0.000000000,0`},
			{`bar`, `2`, ``, ``, `0.000000001,0`},
			{`foo`, `0`, ``, ``, `0.000000000,0`},
			{`foo`, `0`, `foo0`, `v0`, ``},
			{`foo`, `0`, ``, ``, `0.000000001,0`},
			{`foo`, `1`, ``, ``, `0.000000000,0`},
			{`foo`, `1`, ``, ``, `0.000000001,0`},
			{`foo`, `2`, ``, ``, `0.000000000,0`},
			{`foo`, `2`, ``, ``, `0.000000001,0`},
		},
	)
}
