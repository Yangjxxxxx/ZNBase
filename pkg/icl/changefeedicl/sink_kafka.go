/*
Package changefeedicl add by jerry
* @Author       :jerry
* @Version      :1.0.0
* @Date         :下午5:17 19-8-27
* @Description  : insert the details of this file
*/
package changefeedicl

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util/bufalloc"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/log/logtags"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type kafkaLogAdapter struct {
	ctx context.Context
}

var _ sarama.StdLogger = (*kafkaLogAdapter)(nil)

func (l *kafkaLogAdapter) Print(v ...interface{}) {
	log.InfoDepth(l.ctx, 1, v...)
}
func (l *kafkaLogAdapter) Printf(format string, v ...interface{}) {
	log.InfofDepth(l.ctx, 1, format, v...)
}
func (l *kafkaLogAdapter) Println(v ...interface{}) {
	log.InfoDepth(l.ctx, 1, v...)
}

func init() {
	// We'd much prefer to make one of these per sink, so we can use the real
	// context, but quite unfortunately, sarama only has a global logger hook.
	ctx := context.Background()
	ctx = logtags.AddTag(ctx, "kafka-producer", nil)
	sarama.Logger = &kafkaLogAdapter{ctx: ctx}
}

type kafkaSinkConfig struct {
	kafkaTopicPrefix string
	tlsEnabled       bool
	caCert           []byte
	saslEnabled      bool
	saslHandshake    bool
	saslUser         string
	saslPassword     string
}

// kafkaSink emits to Kafka asynchronously. It is not concurrency-safe; all
// calls to Emit and Flush should be from the same goroutine.
type kafkaSink struct {
	cfg      kafkaSinkConfig
	client   sarama.Client
	producer sarama.AsyncProducer

	targets             jobspb.ChangefeedTargets
	lastMetadataRefresh time.Time
	stopWorkerCh        chan struct{}
	worker              sync.WaitGroup
	scratch             bufalloc.ByteAllocator
	// Only synchronized between the client goroutine and the worker goroutine.
	mu struct {
		topics map[string]string
		syncutil.Mutex
		inflight int64
		flushErr error
		flushCh  chan struct{}
	}
}

func (s *kafkaSink) GetTopics() (map[string]string, error) {
	return s.mu.topics, nil
}

// create and return an kafka sink instance
func makeKafkaSink(
	cfg kafkaSinkConfig, bootstrapServers string, targets jobspb.ChangefeedTargets,
) (Sink, error) {
	sink := &kafkaSink{cfg: cfg}
	sink.mu.topics = make(map[string]string)
	for k, t := range targets {
		h := sha256.New()
		h.Write([]byte(fmt.Sprint(k)))
		str := fmt.Sprintf("%X", h.Sum(nil))
		sink.mu.topics[cfg.kafkaTopicPrefix+SQLNameToKafkaName(t.StatementTimeName)+str] = t.StatementTimeName
	}
	sink.targets = targets
	config := sarama.NewConfig()
	config.ClientID = `ZNBaseDB`
	config.Version = sarama.V0_10_0_0

	config.Producer.Return.Successes = true
	config.Producer.Partitioner = newCDCPartitioner

	if cfg.caCert != nil {
		if !cfg.tlsEnabled {
			return nil, errors.Errorf(`%s requires %s=true`, sinkParamCACert, sinkParamTLSEnabled)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(cfg.caCert)
		config.Net.TLS.Config = &tls.Config{
			RootCAs: caCertPool,
		}
		config.Net.TLS.Enable = true
	} else if cfg.tlsEnabled {
		config.Net.TLS.Enable = true
	}

	if cfg.saslEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = cfg.saslHandshake
		config.Net.SASL.User = cfg.saslUser
		config.Net.SASL.Password = cfg.saslPassword
	}

	// When we emit messages to sarama, they're placed in a queue (as does any
	// reasonable kafka producer client). When our sink's Flush is called, we
	// have to wait for all buffered and inflight requests to be sent and then
	// acknowledged. Quite unfortunately, we have no way to hint to the producer
	// that it should immediately send out whatever is buffered. This
	// configuration can have a dramatic impact on how quickly this happens
	// naturally (and some configurations will block forever!).
	//
	// We can configure the producer to send out its batches based on number of
	// messages and/or total buffered message size and/or time. If none of them
	// are set, it uses some defaults, but if any of the three are set, it does
	// no defaulting. Which means that if `Flush.Messages` is set to 10 and
	// nothing else is set, then 9/10 times `Flush` will block forever. We can
	// work around this by also setting `Flush.Frequency` but a cleaner way is
	// to set `Flush.Messages` to 1. In the steady state, this sends a request
	// with some messages, buffers any messages that come in while it is in
	// flight, then sends those out.
	config.Producer.Flush.Messages = 1

	// This works around what seems to be a bug in sarama where it isn't
	// computing the right value to compare against `Producer.MaxMessageBytes`
	// and the server sends it back with a "Message was too large, server
	// rejected it to avoid allocation" error. The other flush tunings are
	// hints, but this one is a hard limit, so it's useful here as a workaround.
	//
	// This workaround should probably be something like setting
	// `Producer.MaxMessageBytes` to 90% of it's value for some headroom, but
	// this workaround is the one that's been running in roachtests and I'd want
	// to test this one more before changing it.
	config.Producer.Flush.MaxMessages = 1000

	// config.Producer.Flush.Messages is set to 1 so we don't need this, but
	// sarama prints scary things to the logs if we don't.
	config.Producer.Flush.Frequency = time.Hour

	var err error
	sink.client, err = sarama.NewClient(strings.Split(bootstrapServers, `,`), config)
	if err != nil {
		err = errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
		return nil, err
	}
	sink.producer, err = sarama.NewAsyncProducerFromClient(sink.client)
	if err != nil {
		err = errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
		return nil, err
	}

	sink.start()
	return sink, nil
}

// start kafka sink
func (s *kafkaSink) start() {
	s.stopWorkerCh = make(chan struct{})
	s.worker.Add(1)
	go s.workerLoop()
}

// Close implements the Sink interface.
func (s *kafkaSink) Close() error {
	close(s.stopWorkerCh)
	s.worker.Wait()

	// If we're shutting down, we don't care what happens to the outstanding
	// messages, so ignore this error.
	_ = s.producer.Close()
	// s.client is only nil in tests.
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// EmitRow implements the Sink interface.
func (s *kafkaSink) EmitRow(
	ctx context.Context,
	table *sqlbase.TableDescriptor,
	key, value []byte,
	resolved hlc.Timestamp,
	ddl bool,
	desc sqlbase.DescriptorProto,
	databaseName string,
	schemaName string,
	operate TableOperate,
	DB *client.DB,
	Executor sqlutil.InternalExecutor,
	poller *poller,
) (bool, error) {
	succeedEmit := true
	target, ok := poller.details.Targets[table.ID]
	if !ok && table.ID != keys.DescriptorTableID {
		return false, errors.Errorf(`cannot emit to undeclared table: %s`, table)
	}
	h := sha256.New()
	h.Write([]byte(fmt.Sprint(table.ID)))
	str := fmt.Sprintf("%X", h.Sum(nil))
	topic := s.cfg.kafkaTopicPrefix + SQLNameToKafkaName(target.StatementTimeName) + str
	if ddl {
		if operate == UnknownOperation {
			return succeedEmit, nil
		}
		if desc != nil && desc.GetID() > keys.MinNonPredefinedUserDescID-1 {
			if operate == CreateTable || operate == RenameTable || operate == DropTable {
				if len(s.mu.topics) != 1 {
					return succeedEmit, nil
				}
				topic = s.cfg.kafkaTopicPrefix + SQLNameToKafkaName(databaseName)
			} else {
				topicName := strings.Join([]string{databaseName, schemaName, desc.GetName()}, ".")
				topic = SQLNameToKafkaName(topicName)
				topicFilter := false
				Prefixes, err := GetTablePrefix(ctx, DB, Executor)
				if err != nil {
					return succeedEmit, err
				}
				for _, Prefix := range Prefixes {
					for id := range Prefix {
						if id == desc.GetID() {
							topicFilter = true
						}
					}
				}
				if desc.GetID() > keys.PostgresSchemaID {
					if !topicFilter {
						return succeedEmit, err
					}
				}
				tableNames, err := GetTableName(ctx, DB, Executor)
				if err != nil {
					return succeedEmit, err
				}
				var str string
				for _, tableName := range tableNames {
					for id, name := range tableName {
						if id == desc.GetID() {
							h := sha256.New()
							h.Write([]byte(fmt.Sprint(desc.GetID())))
							str = fmt.Sprintf("%X", h.Sum(nil))
							topic = s.cfg.kafkaTopicPrefix + SQLNameToKafkaName(name) + str
						}
					}
				}
				var filter bool
				for topic := range s.mu.topics {
					position := len(topic) - 64
					topic = topic[position:]
					if topic == str {
						filter = true
					}

				}
				if !filter {
					return succeedEmit, nil
				}
			}
		}
	}
	if !ddl {
		if _, ok := s.mu.topics[topic]; !ok {
			return false, errors.Errorf(`cannot emit to undeclared topic: %s`, topic)
		}
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	succeedEmit = true
	return succeedEmit, s.emitMessage(ctx, msg)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *kafkaSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	// Periodically ping sarama to refresh its metadata. This means talking to
	// zookeeper, so it shouldn't be done too often, but beyond that this
	// constant was picked pretty arbitrarily.
	//
	// TODO(exdb): Add a test for this. We can't right now (2018-11-13) because
	// we'd need to bump sarama, but that's a bad idea while we're still
	// actively working on stability. At the same time, revisit this tuning.
	const metadataRefreshMinDuration = time.Minute
	if timeutil.Since(s.lastMetadataRefresh) > metadataRefreshMinDuration {
		topics := make([]string, 0, len(s.mu.topics))
		for topic := range s.mu.topics {
			topics = append(topics, topic)
		}
		if err := s.client.RefreshMetadata(topics...); err != nil {
			return err
		}
		s.lastMetadataRefresh = timeutil.Now()
	}

	for topic := range s.mu.topics {
		payload, err := encoder.EncodeResolvedTimestamp(topic, resolved)
		if err != nil {
			return err
		}
		s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)

		// sarama caches this, which is why we have to periodically refresh the
		// metadata above. Staleness here does not impact correctness. Some new
		// partitions will miss this resolved timestamp, but they'll eventually
		// be picked up and get later ones.
		partitions, err := s.client.Partitions(topic)
		if err != nil {
			return err
		}
		for _, partition := range partitions {
			msg := &sarama.ProducerMessage{
				Topic:     topic,
				Partition: partition,
				Key:       nil,
				Value:     sarama.ByteEncoder(payload),
			}
			if err := s.emitMessage(ctx, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

// Flush implements the Sink interface.
func (s *kafkaSink) Flush(ctx context.Context) error {
	flushCh := make(chan struct{}, 1)

	s.mu.Lock()
	inflight := s.mu.inflight
	flushErr := s.mu.flushErr
	s.mu.flushErr = nil
	immediateFlush := inflight == 0 || flushErr != nil
	if !immediateFlush {
		s.mu.flushCh = flushCh
	}
	s.mu.Unlock()

	if immediateFlush {
		return flushErr
	}

	if log.V(1) {
		log.Infof(ctx, "flush waiting for %d inflight messages", inflight)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flushCh:
		s.mu.Lock()
		flushErr := s.mu.flushErr
		s.mu.flushErr = nil
		s.mu.Unlock()
		return flushErr
	}
}

// emit message
func (s *kafkaSink) emitMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	s.mu.Lock()
	s.mu.inflight++
	inflight := s.mu.inflight
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.producer.Input() <- msg:
	}

	if log.V(2) {
		log.Infof(ctx, "emitted %d inflight records to kafka", inflight)
	}
	return nil
}

func (s *kafkaSink) workerLoop() {
	defer s.worker.Done()

	for {
		select {
		case <-s.stopWorkerCh:
			return
		case <-s.producer.Successes():
		case err := <-s.producer.Errors():
			s.mu.Lock()
			if s.mu.flushErr == nil {
				s.mu.flushErr = err
			}
			s.mu.Unlock()
		}

		s.mu.Lock()
		s.mu.inflight--
		if s.mu.inflight == 0 && s.mu.flushCh != nil {
			s.mu.flushCh <- struct{}{}
			s.mu.flushCh = nil
		}
		s.mu.Unlock()
	}
}

// kafka partitioner
type cdcPartitioner struct {
	hash sarama.Partitioner
}

var _ sarama.Partitioner = &cdcPartitioner{}
var _ sarama.PartitionerConstructor = newCDCPartitioner

func newCDCPartitioner(topic string) sarama.Partitioner {
	return &cdcPartitioner{
		hash: sarama.NewHashPartitioner(topic),
	}
}

func (p *cdcPartitioner) RequiresConsistency() bool { return true }
func (p *cdcPartitioner) Partition(
	message *sarama.ProducerMessage, numPartitions int32,
) (int32, error) {
	if message.Key == nil {
		return message.Partition, nil
	}
	return p.hash.Partition(message, numPartitions)
}

// GetTablePrefix Get Table Prefix
func GetTablePrefix(
	ctx context.Context, DB *client.DB, Executor sqlutil.InternalExecutor,
) ([]map[sqlbase.ID]string, error) {
	txn := DB.NewTxn(ctx, "cdc-ddl")
	queryStmt := `SELECT description FROM [SHOW JOBS] where description != 'A job that captures DDL' and job_type = 'CHANGEFEED' and  status = 'running'`
	queryRows, err := Executor.Query(ctx, "select_perfix_DDL", txn, queryStmt)
	if err != nil {
		return nil, err
	}
	s := make([]map[sqlbase.ID]string, len(queryRows))
	for _, queryRow := range queryRows {
		IDPrefix := make(map[sqlbase.ID]string)
		description := string(tree.MustBeDString(queryRow[0]))
		index := strings.Index(description, "||")
		lastIndex := strings.LastIndex(description, "||")
		description = description[index+2 : lastIndex]
		//todo make descrition more reasonable
		err := json.Unmarshal([]byte(description), &IDPrefix)
		if err != nil {
			return s, err
		}
		s = append(s, IDPrefix)
	}
	return s, nil
}

// GetTableName Get Table Name
func GetTableName(
	ctx context.Context, DB *client.DB, Executor sqlutil.InternalExecutor,
) ([]map[sqlbase.ID]string, error) {
	txn := DB.NewTxn(ctx, "cdc-ddl")
	queryStmt := `SELECT description FROM [SHOW JOBS] where description != 'A job that captures DDL' and job_type = 'CHANGEFEED' and  status = 'running'`
	queryRows, err := Executor.Query(ctx, "select_tablename_DDL", txn, queryStmt)
	if err != nil {
		return nil, err
	}
	s := make([]map[sqlbase.ID]string, len(queryRows))
	for _, queryRow := range queryRows {
		IDTableName := make(map[sqlbase.ID]string)
		description := string(tree.MustBeDString(queryRow[0]))
		index1 := strings.LastIndex(description, "||")
		description = description[index1+2:]
		//todo make descrition more reasonable
		err := json.Unmarshal([]byte(description), &IDTableName)
		if err != nil {
			return s, err
		}
		s = append(s, IDTableName)
	}
	return s, nil
}
