/*
Package changefeedicl add by jerry
* @Author       :jerry
* @Version      :1.0.0
* @Date         :下午5:18 19-8-27
* @Description  : insert the details of this file
*/
package changefeedicl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"hash"
	"hash/fnv"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util/bufalloc"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

const (
	sqlSinkCreateTableStmt = `CREATE TABLE IF NOT EXISTS "%s" (
		topic STRING,
		partition INT,
		message_id INT,
		key BYTES, value BYTES,
		resolved BYTES,
		PRIMARY KEY (topic, partition, message_id)
	)`
	sqlSinkEmitStmt = `INSERT INTO "%s" (topic, partition, message_id, key, value, resolved)`
	sqlSinkEmitCols = 6
	// Some amount of batching to mirror a bit how kafkaSink works.
	sqlSinkRowBatchSize = 3
	// While sqlSink is only used for testing, hardcode the number of
	// partitions to something small but greater than 1.
	sqlSinkNumPartitions = 3
)

// sqlSink mirrors the semantics offered by kafkaSink as closely as possible,
// but writes to a SQL table (presumably in ZNBaseDB). Currently only for
// testing.
//
// Each emitted row or resolved timestamp is stored as a row in the table. Each
// table gets 3 partitions. Similar to kafkaSink, the order between two emits is
// only preserved if they are emitted to by the same node and to the same
// partition.
type sqlSink struct {
	db *gosql.DB

	tableName string
	topics    map[string]string
	hasher    hash.Hash32

	rowBuf  []interface{}
	scratch bufalloc.ByteAllocator
}

func (s *sqlSink) GetTopics() (map[string]string, error) {
	return s.topics, nil
}

func makeSQLSink(uri, tableName string, targets jobspb.ChangefeedTargets) (*sqlSink, error) {
	if u, err := url.Parse(uri); err != nil {
		return nil, err
	} else if u.Path == `` {
		return nil, errors.Errorf(`must specify database`)
	}
	db, err := gosql.Open(`postgres`, uri)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(fmt.Sprintf(sqlSinkCreateTableStmt, tableName)); err != nil {
		_ = db.Close()
		return nil, err
	}

	s := &sqlSink{
		db:        db,
		tableName: tableName,
		topics:    make(map[string]string),
		hasher:    fnv.New32a(),
	}
	for _, t := range targets {
		s.topics[t.StatementTimeName] = t.StatementTimeName
	}
	return s, nil
}

// EmitRow implements the Sink interface.
func (s *sqlSink) EmitRow(
	ctx context.Context,
	table *sqlbase.TableDescriptor,
	key, value []byte,
	_ hlc.Timestamp,
	ddl bool,
	desc sqlbase.DescriptorProto,
	databaseName string,
	schemaName string,
	operate TableOperate,
	DB *client.DB,
	Executor sqlutil.InternalExecutor,
	poller *poller,
) (bool, error) {
	target, ok := poller.details.Targets[table.ID]
	if !ok {
		return false, errors.Errorf(`cannot emit to undeclared table: %s`, table.Name)
	}
	topic := target.StatementTimeName
	if _, ok := s.topics[topic]; !ok {
		return false, errors.Errorf(`cannot emit to undeclared topic: %s`, topic)
	}

	// Hashing logic copied from sarama.HashPartitioner.
	s.hasher.Reset()
	if _, err := s.hasher.Write(key); err != nil {
		return false, err
	}
	partition := int32(s.hasher.Sum32()) % sqlSinkNumPartitions
	if partition < 0 {
		partition = -partition
	}

	var noResolved []byte
	return true, s.emit(ctx, topic, partition, key, value, noResolved)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *sqlSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	var noKey, noValue []byte
	for topic := range s.topics {
		payload, err := encoder.EncodeResolvedTimestamp(topic, resolved)
		if err != nil {
			return err
		}
		s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)
		for partition := int32(0); partition < sqlSinkNumPartitions; partition++ {
			if err := s.emit(ctx, topic, partition, noKey, noValue, payload); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sqlSink) emit(
	ctx context.Context, topic string, partition int32, key, value, resolved []byte,
) error {
	// Generate the message id on the client to match the guaranttees of kafka
	// (two messages are only guaranteed to keep their order if emitted from the
	// same producer to the same partition).
	messageID := builtins.GenerateUniqueInt(roachpb.NodeID(partition))
	s.rowBuf = append(s.rowBuf, topic, partition, messageID, key, value, resolved)
	if len(s.rowBuf)/sqlSinkEmitCols >= sqlSinkRowBatchSize {
		return s.Flush(ctx)
	}
	return nil
}

// Flush implements the Sink interface.
func (s *sqlSink) Flush(ctx context.Context) error {
	if len(s.rowBuf) == 0 {
		return nil
	}

	var stmt strings.Builder
	_, _ = fmt.Fprintf(&stmt, sqlSinkEmitStmt, s.tableName)
	for i := 0; i < len(s.rowBuf); i++ {
		if i == 0 {
			stmt.WriteString(` VALUES (`)
		} else if i%sqlSinkEmitCols == 0 {
			stmt.WriteString(`),(`)
		} else {
			stmt.WriteString(`,`)
		}
		_, _ = fmt.Fprintf(&stmt, `$%d`, i+1)
	}
	stmt.WriteString(`)`)
	_, err := s.db.Exec(stmt.String(), s.rowBuf...)
	if err != nil {
		return err
	}
	s.rowBuf = s.rowBuf[:0]
	return nil
}

// Close implements the Sink interface.
func (s *sqlSink) Close() error {
	return s.db.Close()
}
