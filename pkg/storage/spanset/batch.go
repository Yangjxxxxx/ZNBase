// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package spanset

import (
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// Iterator wraps an engine.Iterator and ensures that it can
// only be used to access spans in a SpanSet.
type Iterator struct {
	i     engine.Iterator
	spans *SpanSet

	// Seeking to an invalid key puts the iterator in an error state.
	err error
	// Reaching an out-of-bounds key with Next/Prev invalidates the
	// iterator but does not set err.
	invalid bool
}

var _ engine.Iterator = &Iterator{}

// NewIterator constructs an iterator that verifies access of the underlying
// iterator against the given spans.
func NewIterator(iter engine.Iterator, spans *SpanSet) *Iterator {
	return &Iterator{
		i:     iter,
		spans: spans,
	}
}

// Stats is part of the engine.Iterator interface.
func (iter *Iterator) Stats() engine.IteratorStats {
	return iter.i.Stats()
}

// Close is part of the engine.Iterator interface.
func (iter *Iterator) Close() {
	iter.i.Close()
}

// Iterator returns the underlying engine.Iterator.
func (iter *Iterator) Iterator() engine.Iterator {
	return iter.i
}

// Seek is part of the engine.Iterator interface.
func (iter *Iterator) Seek(key engine.MVCCKey) {
	iter.err = iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key})
	if iter.err == nil {
		iter.invalid = false
	}
	iter.i.Seek(key)
}

// SeekReverse is part of the engine.Iterator interface.
func (iter *Iterator) SeekReverse(key engine.MVCCKey) {
	iter.err = iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key})
	if iter.err == nil {
		iter.invalid = false
	}
	iter.i.SeekReverse(key)
}

// Valid is part of the engine.Iterator interface.
func (iter *Iterator) Valid() (bool, error) {
	if iter.err != nil {
		return false, iter.err
	}
	ok, err := iter.i.Valid()
	if err != nil {
		return false, iter.err
	}
	return ok && !iter.invalid, nil
}

// Next is part of the engine.Iterator interface.
func (iter *Iterator) Next() {
	iter.i.Next()
	if iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: iter.UnsafeKey().Key}) != nil {
		iter.invalid = true
	}
}

// Prev is part of the engine.Iterator interface.
func (iter *Iterator) Prev() {
	iter.i.Prev()
	if iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: iter.UnsafeKey().Key}) != nil {
		iter.invalid = true
	}
}

// NextKey is part of the engine.Iterator interface.
func (iter *Iterator) NextKey() {
	iter.i.NextKey()
	if iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: iter.UnsafeKey().Key}) != nil {
		iter.invalid = true
	}
}

// PrevKey is part of the engine.Iterator interface.
func (iter *Iterator) PrevKey() {
	iter.i.PrevKey()
	if iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: iter.UnsafeKey().Key}) != nil {
		iter.invalid = true
	}
}

// Key is part of the engine.Iterator interface.
func (iter *Iterator) Key() engine.MVCCKey {
	return iter.i.Key()
}

// Value is part of the engine.Iterator interface.
func (iter *Iterator) Value() []byte {
	return iter.i.Value()
}

// ValueProto is part of the engine.Iterator interface.
func (iter *Iterator) ValueProto(msg protoutil.Message) error {
	return iter.i.ValueProto(msg)
}

// UnsafeKey is part of the engine.Iterator interface.
func (iter *Iterator) UnsafeKey() engine.MVCCKey {
	return iter.i.UnsafeKey()
}

// UnsafeValue is part of the engine.Iterator interface.
func (iter *Iterator) UnsafeValue() []byte {
	return iter.i.UnsafeValue()
}

// ComputeStats is part of the engine.Iterator interface.
func (iter *Iterator) ComputeStats(
	start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	if err := iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return iter.i.ComputeStats(start, end, nowNanos)
}

// FindSplitKey is part of the engine.Iterator interface.
func (iter *Iterator) FindSplitKey(
	start, end, minSplitKey engine.MVCCKey, targetSize int64,
) (engine.MVCCKey, error) {
	if err := iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return engine.MVCCKey{}, err
	}
	return iter.i.FindSplitKey(start, end, minSplitKey, targetSize)
}

// MVCCGet is part of the engine.Iterator interface.
func (iter *Iterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts engine.MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	if err := iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key}); err != nil {
		return nil, nil, err
	}
	return iter.i.MVCCGet(key, timestamp, opts)
}

// MVCCScan is part of the engine.Iterator interface.
func (iter *Iterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts engine.MVCCScanOptions,
) (engine.MVCCScanResult, error) {
	if err := iter.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
		return engine.MVCCScanResult{}, err
	}
	return iter.i.MVCCScan(start, end, max, timestamp, opts)
}

// SetUpperBound is part of the engine.Iterator interface.
func (iter *Iterator) SetUpperBound(key roachpb.Key) {
	iter.i.SetUpperBound(key)
}

// CheckForKeyCollisions is part of the engine.Iterator interface.
func (iter *Iterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return iter.i.CheckForKeyCollisions(sstData, start, end)
}

type spanSetReader struct {
	r     engine.Reader
	spans *SpanSet
}

var _ engine.Reader = spanSetReader{}

func (s spanSetReader) Close() {
	s.r.Close()
}

func (s spanSetReader) Closed() bool {
	return s.r.Closed()
}

func (s spanSetReader) Get(key engine.MVCCKey) ([]byte, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		return nil, err
	}
	//lint:ignore SA1019 implementing deprecated interface function (Get) is OK
	return s.r.Get(key)
}

func (s spanSetReader) GetProto(
	key engine.MVCCKey, msg protoutil.Message,
) (bool, int64, int64, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		return false, 0, 0, err
	}
	//lint:ignore SA1019 implementing deprecated interface function (GetProto) is OK
	return s.r.GetProto(key, msg)
}

func (s spanSetReader) Iterate(
	start, end engine.MVCCKey, f func(engine.MVCCKeyValue) (bool, error),
) error {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.r.Iterate(start, end, f)
}

func (s spanSetReader) NewIterator(opts engine.IterOptions) engine.Iterator {
	return &Iterator{s.r.NewIterator(opts), s.spans, nil, false}
}

type spanSetWriter struct {
	w     engine.Writer
	spans *SpanSet
}

var _ engine.Writer = spanSetWriter{}

func (s spanSetWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	// Assume that the constructor of the batch has bounded it correctly.
	return s.w.ApplyBatchRepr(repr, sync)
}

func (s spanSetWriter) Clear(key engine.MVCCKey) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.Clear(key)
}

func (s spanSetWriter) SingleClear(key engine.MVCCKey) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.SingleClear(key)
}

func (s spanSetWriter) ClearRange(start, end engine.MVCCKey) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.w.ClearRange(start, end)
}

func (s spanSetWriter) ClearIterRange(iter engine.Iterator, start, end engine.MVCCKey) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.w.ClearIterRange(iter, start, end)
}

func (s spanSetWriter) Merge(key engine.MVCCKey, value []byte) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.Merge(key, value)
}

func (s spanSetWriter) Put(key engine.MVCCKey, value []byte) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.Put(key, value)
}

func (s spanSetWriter) LogData(data []byte) error {
	return s.w.LogData(data)
}

func (s spanSetWriter) LogLogicalOp(
	op engine.MVICLogicalOpType, details engine.MVICLogicalOpDetails,
) {
	s.w.LogLogicalOp(op, details)
}

type spanSetReadWriter struct {
	spanSetReader
	spanSetWriter
}

var _ engine.ReadWriter = spanSetReadWriter{}

func makeSpanSetReadWriter(rw engine.ReadWriter, spans *SpanSet) spanSetReadWriter {
	return spanSetReadWriter{
		spanSetReader{
			r:     rw,
			spans: spans,
		},
		spanSetWriter{
			w:     rw,
			spans: spans,
		},
	}
}

// NewReadWriter returns an engine.ReadWriter that asserts access of the
// underlying ReadWriter against the given SpanSet.
func NewReadWriter(rw engine.ReadWriter, spans *SpanSet) engine.ReadWriter {
	return makeSpanSetReadWriter(rw, spans)
}

type spanSetBatch struct {
	spanSetReadWriter
	b     engine.Batch
	spans *SpanSet
}

var _ engine.Batch = spanSetBatch{}

func (s spanSetBatch) Commit(sync bool) error {
	return s.b.Commit(sync)
}

func (s spanSetBatch) Distinct() engine.ReadWriter {
	return makeSpanSetReadWriter(s.b.Distinct(), s.spans)
}

func (s spanSetBatch) Empty() bool {
	return s.b.Empty()
}

func (s spanSetBatch) Len() int {
	return s.b.Len()
}

func (s spanSetBatch) Repr() []byte {
	return s.b.Repr()
}

// NewBatch returns an engine.Batch that asserts access of the underlying
// Batch against the given SpanSet.
func NewBatch(b engine.Batch, spans *SpanSet) engine.Batch {
	return &spanSetBatch{
		makeSpanSetReadWriter(b, spans),
		b,
		spans,
	}
}

// GetDBEngine 查找对应的引擎.
func GetDBEngine(e engine.Reader, span roachpb.Span) engine.Reader {
	switch v := e.(type) {
	case spanSetReadWriter:
		return GetDBEngine(getSpanReader(v, span), span)
	case *spanSetBatch:
		return GetDBEngine(getSpanReader(v.spanSetReadWriter, span), span)
	default:
		return e
	}
}

// getSpanReader is a getter to access the engine.Reader field of the
// spansetReader.
func getSpanReader(r spanSetReadWriter, span roachpb.Span) engine.Reader {
	if err := r.spanSetReader.spans.CheckAllowed(SpanReadOnly, span); err != nil {
		panic("Not in the span")
	}

	return r.spanSetReader.r
}
