package load

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

const (
	sinkParamSASLEnabled   = `sasl_enabled`
	sinkParamSASLHandshake = `sasl_handshake`
	sinkParamSASLUser      = `sasl_user`
	sinkParamSASLPassword  = `sasl_password`
	sinkParamCACert        = `ca_cert`
	sinkParamTopicPrefix   = `topic_prefix`
	sinkParamSchemaTopic   = `schema_topic`
	sinkParamTLSEnabled    = `tls_enabled`
	sinkParamTopic         = `topic`
)

type kafkaInputReader struct {
	flowCtx      *runbase.FlowCtx
	kvCh         chan KVBatch
	recordCh     chan csvRecord
	batchSize    int
	batch        csvRecord
	opts         roachpb.CSVOptions
	walltime     int64
	tableDesc    *sqlbase.TableDescriptor
	expectedCols int
	metrics      *Metrics
	fileContext  importFileContext
	kafkaSink    *KafkaSink
	consumerCh   chan ConsumerOffset
	jobID        int64
}

func (k *kafkaInputReader) start(group ctxgroup.Group) {
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "convertcsv")
		defer tracing.FinishSpan(span)
		defer close(k.kvCh)
		defer k.metrics.ConvertGoroutines.Update(0)
		//转换限制转换并发处理数,用户自定义并发数，可减缓cpu负载从而减少rpc error
		concurrentNumber := storageicl.LoadConcurrency(k.flowCtx.Cfg.Settings)
		k.metrics.ConvertGoroutines.Update(concurrentNumber)
		kvBatchSize = int(storageicl.LoadKvRecordSize(k.flowCtx.Cfg.Settings))
		defer k.closeRejectCh(ctx)
		return ctxgroup.GroupWorkers(ctx, int(concurrentNumber), func(ctx context.Context) error {
			return k.convertRecordWorker(ctx)
		})
	})
}

func (k *kafkaInputReader) readFiles(
	ctx context.Context,
	cp *readImportDataProcessor,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
	details jobspb.ImportDetails,
) error {
	dataFiles := cp.spec.Uri

	var kafkaURLs []string
	var groupID string

	for _, file := range dataFiles {
		kafkaURLs = append(kafkaURLs, file.Path)
		groupID = strconv.FormatInt(file.EndOffset, 10)
		k.jobID = file.EndOffset
	}
	//用户是否设置groupID
	if details.Format.Kafka.GroupId != nil {
		groupID = *details.Format.Kafka.GroupId
	}
	log.Info(ctx, "当前groupID:", groupID)
	kafkaSink, err := NewKafkaSink(ctx, kafkaURLs, groupID)
	if err != nil {
		return err
	}
	k.kafkaSink = kafkaSink
	defer kafkaSink.Close()
	return k.Consumer(ctx, kafkaSink, kafkaSink.topics, details)
	//for _, dataFile := range dataFiles {
	//	path, err := url.Parse(dataFile.Path)
	//	if err != nil {
	//		return err
	//	}
	//	if path.RawQuery == "" {
	//		return errors.Errorf("the topic %s corresponding to kafka cannot be empty", path)
	//	}
	//	topic := path.RawQuery
	//	kafkaSink.partitions[topic] = append(kafkaSink.partitions[topic], int32(dataFile.Offset))
	//}
	////目前kafka 仅支持单表导入
	//if len(details.Tables) > 1 || len(details.Tables) == 0 {
	//	return errors.Errorf("KAFKA streaming import currently only supports single table import, and then there are currently %d tables", len(details.Tables))
	//}
	//cols := details.Tables[0].Desc.VisibleColumns()
	//signals := make(chan os.Signal, 1)
	//signal.Notify(signals, os.Interrupt)
	//errCh := make(chan struct{}, len(kafkaSink.partitions))
	//defer close(errCh)
	//group := ctxgroup.WithContext(ctx)
	//for topic, partitions := range kafkaSink.partitions {
	//	partitionsTemp := partitions
	//	for _, partition := range partitionsTemp {
	//		partitionTemp := partition
	//		group.GoCtx(func(ctx context.Context) error {
	//			partitionConsume, err := kafkaSink.consumer.ConsumePartition(topic, partitionTemp, sarama.OffsetOldest)
	//			if err != nil {
	//				return err
	//			}
	//			defer partitionConsumeClose(ctx, partitionConsume)
	//			for {
	//				select {
	//				case <-ctx.Done():
	//					return nil
	//				case <-errCh:
	//					return nil
	//				case message := <-partitionConsume.Messages():
	//					//此处对message 进行处理，发送到对应数据处理ch
	//					values := string(message.Value)
	//					//record := strings.Split(values, string(k.opts.Comma))
	//					//record := []string{values}
	//					//转换数据格式，目前支持json
	//					record, err := JSONToRecords(values, cols)
	//					if err != nil {
	//						return err
	//					}
	//					if len(record) == k.expectedCols {
	//						// Expected number of columns.
	//					} else if len(record) == k.expectedCols+1 && record[k.expectedCols] == "" {
	//						// Line has the optional trailing comma, ignore the empty field.
	//						record = record[:k.expectedCols]
	//					} else {
	//						rowError := newImportRowError(
	//							errors.Errorf("row %d: expected %d fields, got %d", 1, k.expectedCols, len(record)),
	//							strRecord(record, k.opts.Comma),
	//							int64(1))
	//						err := handleCorruptRow(ctx, &k.fileContext, rowError)
	//						if err != nil {
	//							errCh <- struct{}{}
	//							return err
	//						}
	//						continue
	//					}
	//					//fmt.Printf("msg offset: %d, partition: %d, timestamp: %s, value: %s\n",
	//					//	message.Offset, message.Partition, message.Timestamp.String(), string(message.Value))
	//					if len(k.batch.r) >= k.batchSize {
	//						err = k.flushBatch(ctx)
	//						if err != nil {
	//							return err
	//						}
	//					}
	//					k.batch.r = append(k.batch.r, record)
	//				case <-time.After(5 * time.Second):
	//					err = k.flushBatch(ctx)
	//					if err != nil {
	//						return err
	//					}
	//				case <-signals:
	//					return nil
	//				case err := <-partitionConsume.Errors():
	//					return err
	//				}
	//			}
	//		})
	//	}
	//}
	//return group.Wait()
}

func (k *kafkaInputReader) Consumer(
	ctx context.Context, kafkaSink *KafkaSink, topics []string, details jobspb.ImportDetails,
) error {
	defer func() { _ = kafkaSink.group.Close() }()
	group := ctxgroup.WithContext(ctx)
	group.GoCtx(func(ctx context.Context) error {
		concurrentNumber := storageicl.LoadKafkaConcurrency(k.flowCtx.Cfg.Settings)
		return ctxgroup.GroupWorkers(ctx, int(concurrentNumber), func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case err := <-kafkaSink.group.Errors():
					return err
				default:
					loadConsumerGroupHandler := NewLoadConsumerGroupHandler(ctx, k, details, kafkaSink)
					err := kafkaSink.group.Consume(ctx, topics, loadConsumerGroupHandler)
					if err != nil {
						return err
					}
				}
			}
		})
	})
	return group.Wait()
}

//func partitionConsumeClose(ctx context.Context, partitionConsume sarama.PartitionConsumer) {
//	err := partitionConsume.Close()
//	log.Warning(ctx, err)
//}

func (k *kafkaInputReader) inputFinished(ctx context.Context) {
	close(k.recordCh)
}

func (k *kafkaInputReader) saveRejectRecord(
	ctx context.Context, cp *readImportDataProcessor, group *ctxgroup.Group,
) error {
	var rejected chan string
	rejected = make(chan string)
	//job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
	//if err != nil {
	//	return err
	//}
	//details := job.Details().(jobspb.ImportDetails)
	k.fileContext = importFileContext{
		skip:     0,
		maxRows:  math.MaxInt64,
		rejected: rejected,
		jobID:    cp.spec.Progress.JobID,
		flowCtx:  cp.flowCtx,
	}
	group.GoCtx(func(ctx context.Context) error {
		//var buf []byte
		job, err := cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
		if job == nil || err != nil {
			return err
		}
		details := job.Details().(jobspb.ImportDetails)
		path := ""
		if details.Format.RejectedAddress != "" {
			path = details.Format.RejectedAddress
		} else {
			return errors.New("file path cannot be empty,please specify with the parameter 'rejectaddress'")
		}
		fileName := path
		rejectedRows := int64(0)
		jobID := strconv.FormatInt(int64(cp.spec.Progress.JobID), 10)
		rejFn, err := rejectedFilename(fileName, jobID)
		conf, err := dumpsink.ConfFromURI(ctx, rejFn)
		if err != nil {
			return err
		}
		var fileBase string
		if !conf.LocalFile.Equal(roachpb.DumpSink_LocalFilePath{}) {
			fileBase = conf.LocalFile.Path
		}
		rejectedStorage, err := cp.flowCtx.Cfg.DumpSink(ctx, conf)
		if err != nil {
			return err
		}
		defer rejectedStorage.Close()
		for s := range rejected {
			//atomic.AddInt64(&details.Format.RejectedRows,1)
			rejectedRows++
			k.fileContext.skip++
			//buf = append(buf, s...)
			err = rejectedStorage.Write(ctx, fileBase, bytes.NewReader([]byte(s)))
			if err != nil {
				return err
			}
		}
		if rejectedRows == 0 {
			// no rejected rows
			return nil
		}

		//cp.spec.Format.RejectedAddress=rejFn
		job, err = cp.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
		if err != nil {
			return errors.New("too many parse errors,please check if there is a problem with the imported data file")
		}
		{
			if err := job.FractionDetailProgressed(ctx,
				func(ctx context.Context, details jobspb.Details, progress jobspb.ProgressDetails) float32 {
					detail := details.(*jobspb.Payload_Import).Import
					detail.Format.RejectedAddress = rejFn
					prog := progress.(*jobspb.Progress_Import).Import
					return prog.Completed()
				},
			); err != nil {
				return err
			}
		}
		return err
	})
	return nil
}

func (k *kafkaInputReader) closeRejectCh(ctx context.Context) {
	if k.fileContext.rejected != nil {
		close(k.fileContext.rejected)
	}
}

var _ inputConverter = &kafkaInputReader{}

type kafkaSinkConfig struct {
	kafkaTopicPrefix string
	tlsEnabled       bool
	caCert           []byte
	saslEnabled      bool
	saslHandshake    bool
	saslUser         string
	saslPassword     string
}

// KafkaSink emits to Kafka asynchronously. It is not concurrency-safe; all
// calls to Emit and Flush should be from the same goroutine.
type KafkaSink struct {
	cfg        kafkaSinkConfig
	client     sarama.Client
	consumer   sarama.Consumer
	topics     []string
	partitions map[string][]int32
	group      sarama.ConsumerGroup
	groupName  string
	brokers    []string
}

//NewKafkaSink 初始化kafka sink
func NewKafkaSink(ctx context.Context, kafkaURLs []string, groupID string) (*KafkaSink, error) {
	//kafkaSink := &KafkaSink{}
	if len(kafkaURLs) == 0 {
		return nil, errors.New("kafka urls cannot be empty")
	}
	kafkaSink, err := initKafkaConfig(kafkaURLs[0])
	if err != nil {
		return nil, err
	}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_2
	cfg := kafkaSink.cfg
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

	//配置消费策略，每次消费最老数据，该参数设置为可配置参数
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	//处理url
	var urls []string
	var topics []string
	for _, kafkaURL := range kafkaURLs {
		urlParse, err := url.Parse(kafkaURL)
		if err != nil {
			return nil, err
		}
		if urlParse.RawQuery == "" {
			return nil, errors.Errorf("the topic %s corresponding to kafka cannot be empty", kafkaURL)
		}
		q := urlParse.Query()
		topics = append(topics, q.Get(sinkParamTopic))
		q.Del(sinkParamTopic)
		urls = append(urls, urlParse.Host)
	}
	kafkaSink.brokers = urls
	kafkaSink.groupName = groupID
	group, err := sarama.NewConsumerGroup(urls, groupID, config)
	if err != nil {
		return nil, errors.Errorf(" create consumer error %s\n", err.Error())
	}
	kafkaSink.group = group
	client, err := sarama.NewClient(urls, config)
	if err != nil {
		return nil, errors.Errorf(" create client error %s\n", err.Error())
	}
	// consumer
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, errors.Errorf(" create consumer error %s\n", err.Error())
	}
	kafkaSink.consumer = consumer
	kafkaSink.client = client
	kafkaSink.topics = topics
	kafkaSink.partitions = make(map[string][]int32)
	return kafkaSink, nil
}

//初始化kafka config
func initKafkaConfig(urlStr string) (*KafkaSink, error) {
	kafkaSink := &KafkaSink{}
	var cfg kafkaSinkConfig
	urlParse, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	q := urlParse.Query()
	cfg.kafkaTopicPrefix = q.Get(sinkParamTopicPrefix)
	q.Del(sinkParamTopicPrefix)
	if schemaTopic := q.Get(sinkParamSchemaTopic); schemaTopic != `` {
		return nil, errors.Errorf(`%s is not yet supported`, sinkParamSchemaTopic)
	}
	q.Del(sinkParamSchemaTopic)
	if tlsBool := q.Get(sinkParamTLSEnabled); tlsBool != `` {
		var err error
		if cfg.tlsEnabled, err = strconv.ParseBool(tlsBool); err != nil {
			return nil, errors.Errorf(`param %s must be a bool: %s`, sinkParamTLSEnabled, err)
		}
	}
	q.Del(sinkParamTLSEnabled)
	if caCertHex := q.Get(sinkParamCACert); caCertHex != `` {
		// TODO(dan): There's a straightforward and unambiguous transformation
		// between the base 64 encoding defined in RFC 4648 and the URL variant
		// defined in the same RFC: simply replace all `+` with `-` and `/` with
		// `_`. Consider always doing this for the user and accepting either
		// variant.
		var err error
		if cfg.caCert, err = base64.StdEncoding.DecodeString(caCertHex); err != nil {
			return nil, errors.Errorf(`param %s must be base 64 encoded: %s`, sinkParamCACert, err)
		}
	}
	q.Del(sinkParamCACert)

	saslParam := q.Get(sinkParamSASLEnabled)
	q.Del(sinkParamSASLEnabled)
	if saslParam != `` {
		b, err := strconv.ParseBool(saslParam)
		if err != nil {
			return nil, errors.Wrapf(err, `param %s must be a bool:`, sinkParamSASLEnabled)
		}
		cfg.saslEnabled = b
	}
	handshakeParam := q.Get(sinkParamSASLHandshake)
	q.Del(sinkParamSASLHandshake)
	if handshakeParam == `` {
		cfg.saslHandshake = true
	} else {
		if !cfg.saslEnabled {
			return nil, errors.Errorf(`%s must be enabled to configure SASL handshake behavior`, sinkParamSASLEnabled)
		}
		b, err := strconv.ParseBool(handshakeParam)
		if err != nil {
			return nil, errors.Wrapf(err, `param %s must be a bool:`, sinkParamSASLHandshake)
		}
		cfg.saslHandshake = b
	}
	cfg.saslUser = q.Get(sinkParamSASLUser)
	q.Del(sinkParamSASLUser)
	cfg.saslPassword = q.Get(sinkParamSASLPassword)
	q.Del(sinkParamSASLPassword)
	if cfg.saslEnabled {
		if cfg.saslUser == `` {
			return nil, errors.Errorf(`%s must be provided when SASL is enabled`, sinkParamSASLUser)
		}
		if cfg.saslPassword == `` {
			return nil, errors.Errorf(`%s must be provided when SASL is enabled`, sinkParamSASLPassword)
		}
	} else {
		if cfg.saslUser != `` {
			return nil, errors.Errorf(`%s must be enabled if a SASL user is provided`, sinkParamSASLEnabled)
		}
		if cfg.saslPassword != `` {
			return nil, errors.Errorf(`%s must be enabled if a SASL password is provided`, sinkParamSASLEnabled)
		}
	}
	kafkaSink.cfg = cfg
	return kafkaSink, nil
}

// convertRecordWorker converts CSV records into KV pairs and sends them on the
// kvCh chan.
func (k *kafkaInputReader) convertRecordWorker(ctx context.Context) error {
	// Create a new evalCtx per converter so each go routine gets its own
	// collationenv, which can't be accessed in parallel.
	conv, err := newRowConverter(k.tableDesc, k.flowCtx.NewEvalCtx(), k.kvCh)
	if err != nil {
		return err
	}
	//配置数据源信息
	conv.kafkaDataSource = true
	if conv.evalCtx.SessionData == nil {
		panic("uninitialized session data")
	}
	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	const precision = uint64(10 * time.Microsecond)
	timestamp := uint64(k.walltime-epoch) / precision
	befor := timeutil.Now()
	after := timeutil.Now()
	//for batch := range k.recordCh {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-k.recordCh:
			if !ok {
				k.metrics.ConversionWaitTime.RecordValue(0)
				return conv.sendBatch(ctx, k.metrics)
			}
			conv.kvBatch.sources = batch.sources
			after = timeutil.Now()
			k.metrics.ConversionWaitTime.RecordValue(after.Sub(befor).Nanoseconds())
			isContinue := false
			for batchIdx, record := range batch.r {
				if record == nil {
					continue
				}
				isContinue = false
				rowNum := int64(batch.rowOffset + batchIdx)
				for i, v := range record {
					col := conv.visibleCols[i]
					if k.opts.NullEncoding != nil && v == *k.opts.NullEncoding {
						conv.datums[i] = tree.DNull
					} else {
						if k.opts.NullEncoding != nil && k.opts.Null == *k.opts.NullEncoding {
							v, err = escapeString(v)
							if err != nil {
								rowError := newImportRowError(
									errors.Wrapf(err, "parse %q as %s", col.Name, col.Type.SQLString()),
									strRecord(record, k.opts.Comma),
									rowNum)
								if err = handleCorruptRow(ctx, &k.fileContext, rowError); err != nil {
									//return err
									return makeRowErr(batch.file, rowNum, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
								}
								isContinue = true

							}
						}
						var err error
						conv.datums[i], err = tree.ParseDatumStringAs(conv.visibleColTypes[i], v, conv.evalCtx, true)
						if err != nil {
							err = newImportRowError(
								errors.Wrapf(err, "parse %q as %s", col.Name, col.Type.SQLString()),
								strRecord(record, k.opts.Comma),
								rowNum)
							if err = handleCorruptRow(ctx, &k.fileContext, err); err != nil {
								//return err
								return makeRowErr(batch.file, rowNum, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
							}
							isContinue = true
							//return makeRowErr(batch.file, rowNum, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
						}
					}
				}
				if isContinue {
					continue
				}
				rowIndex := int64(timestamp) + rowNum
				if err := conv.row(ctx, batch.fileIndex, rowIndex, k.metrics); err != nil {
					rowErr := newImportRowError(
						errors.Wrapf(err, "%q: row %d: ", batch.file, rowNum),
						strRecord(record, k.opts.Comma),
						rowNum)
					if rowErr = handleCorruptRow(ctx, &k.fileContext, rowErr); rowErr != nil {
						//return err
						return makeRowErr(batch.file, rowNum, "%s", err)
					}
					isContinue = true
				}
				if isContinue {
					continue
				}
			}
			befor = timeutil.Now()
		case <-time.After(storageicl.LoadKafkaFlushDuration(k.flowCtx.Cfg.Settings)):
			err := conv.sendBatch(ctx, k.metrics)
			if err != nil {
				return err
			}
		}
	}
	//k.metrics.ConversionWaitTime.RecordValue(0)
	//return conv.sendBatch(ctx, k.metrics)
}

func (k *kafkaInputReader) flushBatch(ctx context.Context) error {
	// if the batch isn't empty, we need to flush it.
	before := timeutil.Now()
	if len(k.batch.r) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.recordCh <- k.batch:
		}
	}
	after := timeutil.Now()
	k.metrics.ReadWaitTime.RecordValue(after.Sub(before).Nanoseconds())
	//暂时不进行进度更新
	//if progressErr := k.progressFn(1); progressErr != nil {
	//	return progressErr
	//}
	err := k.maybeJobStatus(ctx)
	if err != nil {
		return err
	}
	k.batch.r = make([][]string, 0, k.batchSize)
	return nil
}

func newKafkaInputReader(
	kvCh chan KVBatch,
	opts roachpb.CSVOptions,
	walltime int64,
	tableDesc *sqlbase.TableDescriptor,
	flowCtx *runbase.FlowCtx,
	metrics *Metrics,
	fileContext importFileContext,
) *kafkaInputReader {
	//hash分区表列属于隐藏列，非数据列。
	expectedCols := len(tableDesc.VisibleColumns())
	if tableDesc.IsHashPartition {
		expectedCols--
	}
	return &kafkaInputReader{
		flowCtx:      flowCtx,
		opts:         opts,
		walltime:     walltime,
		kvCh:         kvCh,
		expectedCols: expectedCols,
		tableDesc:    tableDesc,
		recordCh:     make(chan csvRecord, storageicl.LoadRecordChCap(flowCtx.Cfg.Settings)),
		batchSize:    int(storageicl.LoadRecordBatchSize(flowCtx.Cfg.Settings)),
		metrics:      metrics,
		consumerCh:   make(chan ConsumerOffset),
	}
}

//Close 关闭kafka 消费者以及client关闭
func (kafkaSink *KafkaSink) Close() {
	_ = kafkaSink.consumer.Close()
	_ = kafkaSink.client.Close()
	//_ = kafkaSink.group.Close()
}
