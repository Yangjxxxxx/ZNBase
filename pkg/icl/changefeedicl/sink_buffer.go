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

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util/bufalloc"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

// encDatumRowBuffer is a FIFO of `EncDatumRow`s.
//
// TODO(dan): There's some potential allocation savings here by reusing the same
// backing array.
type encDatumRowBuffer []sqlbase.EncDatumRow

func (b *encDatumRowBuffer) IsEmpty() bool {
	return b == nil || len(*b) == 0
}
func (b *encDatumRowBuffer) Push(r sqlbase.EncDatumRow) {
	*b = append(*b, r)
}
func (b *encDatumRowBuffer) Pop() sqlbase.EncDatumRow {
	ret := (*b)[0]
	*b = (*b)[1:]
	return ret
}

type bufferSink struct {
	buf     encDatumRowBuffer
	alloc   sqlbase.DatumAlloc
	scratch bufalloc.ByteAllocator
	closed  bool
}

func (s *bufferSink) GetTopics() (map[string]string, error) {
	return make(map[string]string), nil
}

// EmitRow implements the Sink interface.
func (s *bufferSink) EmitRow(
	_ context.Context,
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
	if s.closed {
		return false, errors.New(`cannot EmitRow on a closed sink`)
	}
	topic := table.Name
	s.buf.Push(sqlbase.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: s.alloc.NewDString(tree.DString(topic))}, // topic
		{Datum: s.alloc.NewDBytes(tree.DBytes(key))},     // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(value))},   //value
	})
	return true, nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *bufferSink) EmitResolvedTimestamp(
	_ context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	if s.closed {
		return errors.New(`cannot EmitResolvedTimestamp on a closed sink`)
	}
	var noTopic string
	payload, err := encoder.EncodeResolvedTimestamp(noTopic, resolved)
	if err != nil {
		return err
	}
	s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)
	s.buf.Push(sqlbase.EncDatumRow{
		{Datum: tree.DNull}, // resolved span
		{Datum: tree.DNull}, // topic
		{Datum: tree.DNull}, // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(payload))}, // value
	})
	return nil
}

// Flush implements the Sink interface.
func (s *bufferSink) Flush(_ context.Context) error {
	return nil
}

// Close implements the Sink interface.
func (s *bufferSink) Close() error {
	s.closed = true
	return nil
}
