package load

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// showKafkaPlanHook implements PlanHookFn.
func showKafkaPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	showKafka, ok := stmt.(*tree.ShowKafka)
	if !ok {
		return nil, nil, nil, false, nil
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		//url:=showKafka.URL
		toFn, err := p.TypeAsString(showKafka.URL, "SHOW KAFKA")
		if err != nil {
			return err
		}
		url, err := toFn()
		if err != nil {
			return err
		}
		groupIDFn, err := p.TypeAsString(showKafka.GroupID, "SHOW KAFKA")
		if err != nil {
			return err
		}
		groupID, err := groupIDFn()
		if err != nil {
			return err
		}
		//groupID:=showKafka.GroupID.String()
		sink, err := NewKafkaSink(ctx, []string{url}, groupID)
		if err != nil {
			return err
		}
		defer sink.Close()
		if len(sink.topics) == 0 {
			return errors.New("topic cannot be empty")
		}
		broker := sarama.NewBroker(sink.brokers[0])
		err = broker.Open(sink.client.Config())
		if err != nil {
			return err
		}
		defer func() { _ = broker.Close() }()
		req := new(sarama.OffsetFetchRequest)
		req.Version = 1
		req.ConsumerGroup = sink.groupName
		sink.partitions[sink.topics[0]], err = sink.consumer.Partitions(sink.topics[0])
		if err != nil {
			return err
		}
		for key, value := range sink.partitions {
			for _, index := range value {
				req.AddPartition(key, index)
			}
		}
		response, err := broker.FetchOffset(req)
		if err != nil {
			return err
		}
		for _, partitions := range response.Blocks {
			for partition, offset := range partitions {
				row := tree.Datums{
					tree.NewDInt(tree.DInt(partition)),
					tree.NewDInt(tree.DInt(offset.Offset)),
				}
				resultsCh <- row
			}
		}
		//topic:=sink.topics[0]
		//partitions,err:=sink.client.Partitions(topic)
		//if err!=nil {
		//	return err
		//}
		//for _, partition :=range partitions{
		//	offset, err := sink.client.GetOffset(topic, partition, sarama.OffsetNewest)
		//	if err!=nil {
		//		return err
		//	}
		//	row:=tree.Datums{
		//		tree.NewDInt(tree.DInt(partition)),
		//		tree.NewDInt(tree.DInt(offset)),
		//	}
		//	resultsCh<-row
		//}
		return nil
	}
	return fn, ShowKafkaHeader, nil, false, nil
}

// ShowKafkaHeader is the header for SHOW KAFKA stmt results.
var ShowKafkaHeader = sqlbase.ResultColumns{
	{Name: "partition", Typ: types.Int},
	{Name: "offset", Typ: types.Int},
}

func init() {
	sql.AddPlanHook(showKafkaPlanHook)
}
