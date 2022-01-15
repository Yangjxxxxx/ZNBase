package load

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/util/log"
)

//ConsumerGroupHandler consume handler
type ConsumerGroupHandler struct {
	k         *kafkaInputReader
	ctx       context.Context
	details   jobspb.ImportDetails
	kafkaSink *KafkaSink
}

//NewLoadConsumerGroupHandler 返回消费者组consume handler
func NewLoadConsumerGroupHandler(
	ctx context.Context, k *kafkaInputReader, details jobspb.ImportDetails, kafkaSink *KafkaSink,
) *ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		k:         k,
		ctx:       ctx,
		details:   details,
		kafkaSink: kafkaSink,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (handler ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	//session.Claims()
	//broker := sarama.NewBroker(handler.kafkaSink.brokers[0])
	//err := broker.Open(handler.kafkaSink.client.Config())
	//if err != nil {
	//	return err
	//}
	//req := new(sarama.OffsetFetchRequest)
	//req.Version = 1
	//req.ConsumerGroup = handler.kafkaSink.groupName
	//for key, value := range session.Claims() {
	//	for _, index := range value {
	//		req.AddPartition(key, index)
	//	}
	//}
	//response, err := broker.FetchOffset(req)
	//if err != nil {
	//	return err
	//}
	//for topic, partitions := range response.Blocks {
	//	for partition, offset := range partitions {
	//		fmt.Println("topic:", topic, "partition:", partition, "offset:", offset.Offset)
	//	}
	//}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (handler ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (handler ConsumerGroupHandler) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) error {
	details := handler.details
	k := handler.k
	ctx := handler.ctx
	//目前kafka 仅支持单表导入
	if len(details.Tables) > 1 || len(details.Tables) == 0 {
		return errors.Errorf("KAFKA streaming import currently only supports single table import, and then there are currently %d tables", len(details.Tables))
	}
	if len(handler.kafkaSink.topics) == 0 {
		return errors.Errorf("topic cannot be empty")
	}
	//当前仅支持单topic
	topic := handler.kafkaSink.topics[0]
	errCh := make(chan struct{})
	go func() {
		for {
			select {
			case consume := <-k.consumerCh:
				for partition, offset := range consume.partitions {
					if offset != 0 {
						sess.MarkOffset(topic, int32(partition), offset+1, "")
					}
				}
			case <-errCh:
				return
			}
		}
	}()
	cols := details.Tables[0].Desc.VisibleColumns()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		if ctx.Err() != nil {
			errCh <- struct{}{}
			return nil
		}
		select {
		case <-ctx.Done():
			errCh <- struct{}{}
			return ctx.Err()
		case message := <-claim.Messages():
			//此处对message 进行处理，发送到对应数据处理ch
			if message == nil {
				errCh <- struct{}{}
				return nil
			}
			//log.Errorf(ctx, "Message claimed: value = %s, timestamp = %v, topic = %s, offset = %s , partition = %s", string(message.Value), message.Timestamp, message.Topic, message.Offset, message.Partition)
			values := string(message.Value)
			//转换数据格式，目前支持json
			record, err := JSONToRecords(values, cols)
			if err != nil {
				errCh <- struct{}{}
				return err
			}
			if len(record) == k.expectedCols {
				// Expected number of columns.
			} else if len(record) == k.expectedCols+1 && record[k.expectedCols] == "" {
				// Line has the optional trailing comma, ignore the empty field.
				record = record[:k.expectedCols]
			} else {
				rowError := newImportRowError(
					errors.Errorf("row %d: expected %d fields, got %d", 1, k.expectedCols, len(record)),
					strRecord(record, k.opts.Comma),
					int64(1))
				err := handleCorruptRow(ctx, &k.fileContext, rowError)
				if err != nil {
					errCh <- struct{}{}
					return err
				}
				continue
			}
			if len(k.batch.r) >= k.batchSize {
				err = k.flushBatch(ctx)
				if err != nil {
					errCh <- struct{}{}
					return err
				}
				//只有当数据实际入库是才进行消费标记
				//sess.MarkMessage(message, "")
			}
			//sess.MarkMessage(message, "")
			source := message.Partition
			offset := message.Offset
			if k.batch.sources == nil {
				nums, err := handler.kafkaSink.client.Partitions(topic)
				if err != nil {
					return err
				}
				k.batch.sources = make([]int64, len(nums))
			}
			//当前记录消息消费切片容量不足时
			if len(k.batch.sources) <= int(source) {
				nums, err := handler.kafkaSink.client.Partitions(topic)
				if err != nil {
					return err
				}
				if len(nums) < int(source) {
					log.Errorf(ctx, "the current number of partitions does not match the expectation, the expectation is %d, but the actual is %d", len(nums), source)
					sourceTemp := make([]int64, source)
					copy(sourceTemp, k.batch.sources)
					k.batch.sources = sourceTemp
				} else {
					sourceTemp := make([]int64, len(nums))
					copy(sourceTemp, k.batch.sources)
					k.batch.sources = sourceTemp
				}
			}
			if k.batch.sources[source] < offset {
				k.batch.sources[source] = offset
			}
			k.batch.r = append(k.batch.r, record)
		case <-time.After(5 * time.Second):
			if len(k.batch.r) > 0 {
				err := k.flushBatch(ctx)
				if err != nil {
					errCh <- struct{}{}
					return err
				}
			}
			//进行当前的状态检查
			err := k.maybeJobStatus(ctx)
			if err != nil {
				return err
			}
		case <-signals:
			errCh <- struct{}{}
			return nil
		}
	}
}

func (k *kafkaInputReader) maybeJobStatus(ctx context.Context) error {
	jobID := k.jobID
	if jobID == 0 {
		return errors.Errorf("the current job's abnormal state ,job id is %d", jobID)
	}
	job, err := k.flowCtx.Cfg.JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		return err
	}
	status, err := job.CurrentStatus(ctx)
	if err != nil {
		return err
	}
	//如果当前job状态不为running 则返回错误
	if status != jobs.StatusRunning {
		return errors.Errorf("The current job has been %s", status)
	}
	return nil
}

//ConsumerOffset 当前组已消费的消息记录
type ConsumerOffset struct {
	topic      string
	partitions []int64
}

//该部分需要后续进行开发
//func (kafkaSink *KafkaSink) MarkOffset(topic string, partition int32, offset int64) error {
//	broker, err := kafkaSink.client.Controller()
//	if err != nil {
//		return err
//	}
//	//if len(brokers) == 0 {
//	//	return errors.New("brokers is empty")
//	//}
//	//broker := brokers[0]
//	conn, err := broker.Connected()
//	if err != nil {
//		return err
//	}
//	if !conn {
//		err = broker.Open(kafkaSink.client.Config())
//		if err != nil {
//			return err
//		}
//	}
//	//defer func() { _ = broker.Close() }()
//	req := &sarama.OffsetCommitRequest{
//		Version:                 1,
//		ConsumerGroup:           kafkaSink.groupName,
//		ConsumerGroupGeneration: sarama.GroupGenerationUndefined,
//		RetentionTime:           sarama.ReceiveTime,
//	}
//	req.AddBlock(topic, partition, offset, -1, "")
//
//	_, err = broker.CommitOffset(req)
//	if err != nil {
//		log.Error(context.Background(), "commit offset failed")
//		return nil
//	}
//	return nil
//
//	//offsetManager, err := sarama.NewOffsetManagerFromClient(kafkaSink.groupName, kafkaSink.client)
//	//if err != nil {
//	//	return err
//	//}
//	////defer func() {
//	////	err := offsetManager.Close()
//	////	if err != nil {
//	////		log.Error(context.Background(), err)
//	////	}
//	////}()
//	//partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
//	//if err != nil {
//	//	return err
//	//}
//	////defer func() {
//	////	err := partitionOffsetManager.Close()
//	////	if err != nil {
//	////		log.Error(context.Background(), err)
//	////	}
//	////}()
//	//partitionOffsetManager.MarkOffset(offset, "")
//	//return nil
//}
