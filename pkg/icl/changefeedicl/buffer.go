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
	"encoding/json"
	"time"

	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/envutil"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type bufferEntry struct {
	kv roachpb.KeyValue
	// prevVal is set if the key had a non-tombstone value before the change
	// and the before value of each change was requested (optDiff).
	prevVal  roachpb.Value
	resolved *jobspb.ResolvedSpan
	// Timestamp of the schema that should be used to read this KV.
	// If unset (zero-valued), the value's timestamp will be used instead.
	schemaTimestamp hlc.Timestamp
	// Timestamp of the schema that should be used to read the previous
	// version of this KV.
	// If unset (zero-valued), the previous value will be interpretted with
	// the same timestamp as the current value.
	prevSchemaTimestamp hlc.Timestamp
	// bufferGetTimestamp is the time this entry came out of the buffer.
	bufferGetTimestamp time.Time

	//Record tableOperate
	ddlEvent DDLEvent
	//
	source Source
}

// JSONBufferEntry 序列化Buffer Entry
type JSONBufferEntry struct {
	Kv roachpb.KeyValue
	// prevVal is set if the key had a non-tombstone value before the change
	// and the before value of each change was requested (optDiff).
	PrevVal  roachpb.Value
	Resolved *jobspb.ResolvedSpan
	// Timestamp of the schema that should be used to read this KV.
	// If unset (zero-valued), the value's timestamp will be used instead.
	SchemaTimestamp hlc.Timestamp
	// Timestamp of the schema that should be used to read the previous
	// version of this KV.
	// If unset (zero-valued), the previous value will be interpretted with
	// the same timestamp as the current value.
	PrevSchemaTimestamp hlc.Timestamp
	// bufferGetTimestamp is the time this entry came out of the buffer.
	BufferGetTimestamp time.Time

	Source Source
}

// 在执行json Marshal之前会先执行此处的方法
func (bufEntry *bufferEntry) MarshalJSON() ([]byte, error) {
	jsonEntry := bufEntry.fillJSONBufferEntry()
	return json.Marshal(jsonEntry)
}

func (bufEntry *bufferEntry) fillJSONBufferEntry() JSONBufferEntry {
	return JSONBufferEntry{
		Kv:                  bufEntry.kv,
		PrevVal:             bufEntry.prevVal,
		Resolved:            bufEntry.resolved,
		SchemaTimestamp:     bufEntry.schemaTimestamp,
		PrevSchemaTimestamp: bufEntry.prevSchemaTimestamp,
		BufferGetTimestamp:  bufEntry.bufferGetTimestamp,
		Source:              bufEntry.source,
	}
}

func (bufEntry *bufferEntry) fillBufferEntry(entry JSONBufferEntry) {
	bufEntry.kv = entry.Kv
	bufEntry.prevVal = entry.PrevVal
	bufEntry.resolved = entry.Resolved
	bufEntry.schemaTimestamp = entry.SchemaTimestamp
	bufEntry.prevSchemaTimestamp = entry.PrevSchemaTimestamp
	bufEntry.bufferGetTimestamp = entry.BufferGetTimestamp
	bufEntry.source = entry.Source
}
func (bufEntry *bufferEntry) UnmarshalJSON(buf []byte) error {
	jsonEntry := JSONBufferEntry{}
	err := json.Unmarshal(buf, &jsonEntry)
	if err != nil {
		return err
	}
	bufEntry.fillBufferEntry(jsonEntry)
	return nil
}

// buffer mediates between the changed data poller and the rest of the
// cdc pipeline (which is backpressured all the way to the sink).
//
// TODO(dan): Monitor memory usage and spill to disk when necessary.
type buffer struct {
	entriesCh chan bufferEntry
}

func makeBuffer() *buffer {
	return &buffer{entriesCh: make(chan bufferEntry)}
}

// AddKV inserts a changed kv into the buffer.
//
// TODO(dan): AddKV currently requires that each key is added in increasing mvcc
// timestamp order. This will have to change when we add support for RangeFeed,
// which starts out in a catchup state without this guarantee.
func (b *buffer) AddKV(
	ctx context.Context,
	kv roachpb.KeyValue,
	prevVal roachpb.Value,
	schemaTimestamp hlc.Timestamp,
	prevSchemaTimestamp hlc.Timestamp,
	ddlEvent DDLEvent,
	fromTmpFile Source,
) error {
	return b.addEntry(ctx, bufferEntry{
		kv:                  kv,
		prevVal:             prevVal,
		schemaTimestamp:     schemaTimestamp,
		prevSchemaTimestamp: prevSchemaTimestamp,
		ddlEvent:            ddlEvent,
		source:              fromTmpFile,
	})
}

// AddResolved inserts a resolved timestamp notification in the buffer.
func (b *buffer) AddResolved(
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, boundaryReached bool,
) error {
	return b.addEntry(ctx, bufferEntry{resolved: &jobspb.ResolvedSpan{Span: span, Timestamp: ts, BoundaryReached: boundaryReached}})
}

func (b *buffer) addEntry(ctx context.Context, e bufferEntry) error {
	// TODO(dan): Spill to a temp rocksdb if entriesCh would block.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.entriesCh <- e:
		return nil
	}
}

// Get returns an entry from the buffer. They are handed out in an order that
// (if it is maintained all the way to the sink) meets our external guarantees.
func (b *buffer) Get(ctx context.Context) (bufferEntry, error) {
	select {
	case <-ctx.Done():
		return bufferEntry{}, ctx.Err()
	case e := <-b.entriesCh:
		e.bufferGetTimestamp = timeutil.Now()
		return e, nil
	}
}

// memBufferDefaultCapacity is the default capacity for a memBuffer for a single
// cdc.
//
// TODO(dan): It would be better if all CDCs shared a single capacity
// that was given by the operater at startup, like we do for RocksDB and SQL.
var memBufferDefaultCapacity = envutil.EnvOrDefaultBytes(
	//todo
	"ZNBASE_CHANGEFEED_BUFFER_CAPACITY", 6<<30) // 1GB

var memBufferColTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES}, // kv.Key
	{SemanticType: sqlbase.ColumnType_BYTES}, // kv.Value
	{SemanticType: sqlbase.ColumnType_BYTES}, // kv.PrevValue
	{SemanticType: sqlbase.ColumnType_BYTES}, // span.Key
	{SemanticType: sqlbase.ColumnType_BYTES}, // span.EndKey
	{SemanticType: sqlbase.ColumnType_INT},   // ts.WallTime
	{SemanticType: sqlbase.ColumnType_INT},   // ts.Logical
	{SemanticType: sqlbase.ColumnType_INT},   // schemaTimestamp.WallTime
	{SemanticType: sqlbase.ColumnType_INT},   // schemaTimestamp.Logical
	{SemanticType: sqlbase.ColumnType_INT},   // source
}

// memBuffer is an in-memory buffer for changed KV and resolved timestamp
// events. It's size is limited only by the BoundAccount passed to the
// constructor.
type memBuffer struct {
	metrics *Metrics

	mu struct {
		syncutil.Mutex
		entries rowcontainer.RowContainer
	}
	// signalCh can be selected on to learn when an entry is written to
	// mu.entries.
	signalCh chan struct{}
	allocMu  struct {
		syncutil.Mutex
		a sqlbase.DatumAlloc
	}
}

func makeMemBuffer(acc mon.BoundAccount, metrics *Metrics) *memBuffer {
	b := &memBuffer{
		metrics:  metrics,
		signalCh: make(chan struct{}, 1)}
	b.mu.entries.Init(acc, sqlbase.ColTypeInfoFromColTypes(memBufferColTypes), 0 /* rowCapacity */)
	return b
}

func (b *memBuffer) Close(ctx context.Context) {
	b.mu.Lock()
	b.mu.entries.Close(ctx)
	b.mu.Unlock()
}

// AddKV inserts a changed kv into the buffer.
func (b *memBuffer) AddKV(
	ctx context.Context,
	kv roachpb.KeyValue,
	prevVal roachpb.Value,
	schemaTimestamp hlc.Timestamp,
	source Source,
) error {
	b.allocMu.Lock()
	prevValDatum := tree.DNull
	if prevVal.IsPresent() {
		prevValDatum = b.allocMu.a.NewDBytes(tree.DBytes(prevVal.RawBytes))
	}
	row := tree.Datums{
		b.allocMu.a.NewDBytes(tree.DBytes(kv.Key)),
		b.allocMu.a.NewDBytes(tree.DBytes(kv.Value.RawBytes)),
		prevValDatum,
		tree.DNull,
		tree.DNull,
		b.allocMu.a.NewDInt(tree.DInt(kv.Value.Timestamp.WallTime)),
		b.allocMu.a.NewDInt(tree.DInt(kv.Value.Timestamp.Logical)),
		b.allocMu.a.NewDInt(tree.DInt(schemaTimestamp.WallTime)),
		b.allocMu.a.NewDInt(tree.DInt(schemaTimestamp.Logical)),
		b.allocMu.a.NewDInt(tree.DInt(source)),
	}
	b.allocMu.Unlock()
	return b.addRow(ctx, row)
}

// AddResolved inserts a resolved timestamp notification in the buffer.
func (b *memBuffer) AddResolved(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) error {
	b.allocMu.Lock()
	row := tree.Datums{
		tree.DNull,
		tree.DNull,
		tree.DNull,
		b.allocMu.a.NewDBytes(tree.DBytes(span.Key)),
		b.allocMu.a.NewDBytes(tree.DBytes(span.EndKey)),
		b.allocMu.a.NewDInt(tree.DInt(ts.WallTime)),
		b.allocMu.a.NewDInt(tree.DInt(ts.Logical)),
		tree.DNull,
		tree.DNull,
		tree.DNull,
	}
	b.allocMu.Unlock()
	return b.addRow(ctx, row)
}

// Get returns an entry from the buffer. They are handed out in an order that
// (if it is maintained all the way to the sink) meets our external guarantees.
func (b *memBuffer) Get(ctx context.Context) (bufferEntry, error) {
	row, err := b.getRow(ctx)
	if err != nil {
		return bufferEntry{}, err
	}
	e := bufferEntry{bufferGetTimestamp: timeutil.Now()}
	ts := hlc.Timestamp{
		WallTime: int64(*row[5].(*tree.DInt)),
		Logical:  int32(*row[6].(*tree.DInt)),
	}
	if row[2] != tree.DNull {
		e.prevVal = roachpb.Value{
			RawBytes: []byte(*row[2].(*tree.DBytes)),
		}
	}
	if row[0] != tree.DNull {
		e.kv = roachpb.KeyValue{
			Key: []byte(*row[0].(*tree.DBytes)),
			Value: roachpb.Value{
				RawBytes:  []byte(*row[1].(*tree.DBytes)),
				Timestamp: ts,
			},
		}
		e.schemaTimestamp = hlc.Timestamp{
			WallTime: int64(*row[7].(*tree.DInt)),
			Logical:  int32(*row[8].(*tree.DInt)),
		}
		e.source = Source(int32(*row[9].(*tree.DInt)))
		return e, nil
	}
	e.resolved = &jobspb.ResolvedSpan{
		Span: roachpb.Span{
			Key:    []byte(*row[3].(*tree.DBytes)),
			EndKey: []byte(*row[4].(*tree.DBytes)),
		},
		Timestamp: ts,
	}
	return e, nil
}

func (b *memBuffer) addRow(ctx context.Context, row tree.Datums) error {
	b.mu.Lock()
	_, err := b.mu.entries.AddRow(ctx, row)
	b.mu.Unlock()
	b.metrics.BufferEntriesIn.Inc(1)
	select {
	case b.signalCh <- struct{}{}:
	default:
		// Already signaled, don't need to signal again.
	}
	if e, ok := pgerror.GetPGCause(err); ok && e.Code == pgcode.OutOfMemory {
		err = MarkTerminalError(err)
	}
	return err
}

func (b *memBuffer) getRow(ctx context.Context) (tree.Datums, error) {
	for {
		var row tree.Datums
		b.mu.Lock()
		if b.mu.entries.Len() > 0 {
			row = b.mu.entries.At(0)
			b.mu.entries.PopFirst()
		}
		b.mu.Unlock()
		if row != nil {
			b.metrics.BufferEntriesOut.Inc(1)
			return row, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.signalCh:
		}
	}
}
