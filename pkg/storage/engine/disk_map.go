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
// permissions and limitations under the License.

package engine

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/znbasedb/pebble"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/diskmap"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// defaultBatchCapacityBytes is the default capacity for a
// SortedDiskMapBatchWriter.
const defaultBatchCapacityBytes = 4096

// RocksDBMapBatchWriter batches writes to a RocksDBMap.
type RocksDBMapBatchWriter struct {
	// capacity is the number of bytes to write before a Flush() is triggered.
	capacity int

	// makeKey is a function that transforms a key into an MVCCKey with a prefix
	// to be written to the underlying store.
	makeKey           func(k []byte) MVCCKey
	batch             Batch
	numPutsSinceFlush int
	store             Engine
}

// RocksDBMapIterator iterates over the keys of a RocksDBMap in sorted order.
type RocksDBMapIterator struct {
	iter Iterator
	// makeKey is a function that transforms a key into an MVCCKey with a prefix
	// used to Seek() the underlying iterator.
	makeKey func(k []byte) MVCCKey
	// prefix is the prefix of keys that this iterator iterates over.
	prefix []byte
}

// RocksDBMap is a SortedDiskMap that uses RocksDB as its underlying storage
// engine.
type RocksDBMap struct {
	// TODO(asubiotto): Add memory accounting.
	prefix          []byte
	store           Engine
	allowDuplicates bool
	keyID           int64
}

var _ diskmap.SortedDiskMapBatchWriter = &RocksDBMapBatchWriter{}
var _ diskmap.SortedDiskMapIterator = &RocksDBMapIterator{}
var _ diskmap.SortedDiskMap = &RocksDBMap{}

// tempStorageID is the temp ID generator for a node. It generates unique
// prefixes for NewRocksDBMap. It is a global because NewRocksDBMap needs to
// prefix its writes uniquely, and using a global prevents users from having to
// specify the prefix themselves and correctly guarantee that it is unique.
var tempStorageID uint64

func generateTempStorageID() uint64 {
	return atomic.AddUint64(&tempStorageID, 1)
}

// NewRocksDBMap creates a new RocksDBMap with the passed in Engine as
// the underlying store. The RocksDBMap instance will have a keyspace prefixed
// by a unique prefix.
func NewRocksDBMap(e Engine) *RocksDBMap {
	return newRocksDBMap(e, false)
}

// NewRocksDBMultiMap creates a new RocksDBMap with the passed in Engine
// as the underlying store. The RocksDBMap instance will have a keyspace
// prefixed by a unique prefix. Unlike NewRocksDBMap, Puts with identical
// keys will write multiple entries (instead of overwriting previous entries)
// that will be returned during iteration.
func NewRocksDBMultiMap(e Engine) *RocksDBMap {
	return newRocksDBMap(e, true)
}

func newRocksDBMap(e Engine, allowDuplicates bool) *RocksDBMap {
	prefix := generateTempStorageID()
	return &RocksDBMap{
		prefix:          encoding.EncodeUvarintAscending([]byte(nil), prefix),
		store:           e,
		allowDuplicates: allowDuplicates,
	}
}

// makeKey appends k to the RocksDBMap's prefix to keep the key local to this
// instance and creates an MVCCKey, which is what the underlying storage engine
// expects. The returned key is only valid until the next call to makeKey().
func (r *RocksDBMap) makeKey(k []byte) MVCCKey {
	// TODO(asubiotto): We can make this more performant by bypassing MVCCKey
	// creation (have to generalize storage API). See
	// https://github.com/znbasedb/znbase/issues/16718#issuecomment-311493414
	prefixLen := len(r.prefix)
	r.prefix = append(r.prefix, k...)
	mvccKey := MVCCKey{Key: r.prefix}
	r.prefix = r.prefix[:prefixLen]
	return mvccKey
}

// makeKeyWithTimestamp makes a key appropriate for a Put operation. It is like
// makeKey except it respects allowDuplicates, which uses the MVCC timestamp
// field to assign a unique keyID so duplicate keys don't overwrite each other.
func (r *RocksDBMap) makeKeyWithTimestamp(k []byte) MVCCKey {
	mvccKey := r.makeKey(k)
	if r.allowDuplicates {
		r.keyID++
		mvccKey.Timestamp.WallTime = r.keyID
	}
	return mvccKey
}

// Put implements the SortedDiskMap interface.
func (r *RocksDBMap) Put(k []byte, v []byte) error {
	return r.store.Put(r.makeKeyWithTimestamp(k), v)
}

// Get implements the SortedDiskMap interface.
func (r *RocksDBMap) Get(k []byte) ([]byte, error) {
	if r.allowDuplicates {
		return nil, errors.New("Get not supported if allowDuplicates is true")
	}
	return r.store.Get(r.makeKey(k))
}

// NewIterator implements the SortedDiskMap interface.
func (r *RocksDBMap) NewIterator() diskmap.SortedDiskMapIterator {
	// NOTE: prefix is only false because we can't use the normal prefix
	// extractor. This iterator still only does prefix iteration. See
	// RocksDBMapIterator.Valid().
	return &RocksDBMapIterator{
		iter: r.store.NewIterator(IterOptions{
			UpperBound: roachpb.Key(r.prefix).PrefixEnd(),
		}),
		makeKey: r.makeKey,
		prefix:  r.prefix,
	}
}

// NewBatchWriter implements the SortedDiskMap interface.
func (r *RocksDBMap) NewBatchWriter() diskmap.SortedDiskMapBatchWriter {
	return r.NewBatchWriterCapacity(defaultBatchCapacityBytes)
}

// NewBatchWriterCapacity implements the SortedDiskMap interface.
func (r *RocksDBMap) NewBatchWriterCapacity(capacityBytes int) diskmap.SortedDiskMapBatchWriter {
	makeKey := r.makeKey
	if r.allowDuplicates {
		makeKey = r.makeKeyWithTimestamp
	}
	return &RocksDBMapBatchWriter{
		capacity: capacityBytes,
		makeKey:  makeKey,
		batch:    r.store.NewWriteOnlyBatch(),
		store:    r.store,
	}
}

// Clear implements the SortedDiskMap interface.
func (r *RocksDBMap) Clear() error {
	if err := r.store.ClearRange(
		MVCCKey{Key: r.prefix},
		MVCCKey{Key: roachpb.Key(r.prefix).PrefixEnd()},
	); err != nil {
		return errors.Wrapf(err, "unable to clear range with prefix %v", r.prefix)
	}
	// NB: we manually flush after performing the clear range to ensure that the
	// range tombstone is pushed to disk which will kick off compactions that
	// will eventually free up the deleted space.
	return r.store.Flush()
}

// Close implements the SortedDiskMap interface.
func (r *RocksDBMap) Close(ctx context.Context) {
	if err := r.Clear(); err != nil {
		log.Error(ctx, err)
	}
}

// Seek implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Seek(k []byte) {
	i.iter.Seek(i.makeKey(k))
}

// Rewind implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Rewind() {
	i.iter.Seek(i.makeKey(nil))
}

// Valid implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Valid() (bool, error) {
	ok, err := i.iter.Valid()
	if err != nil {
		return false, err
	}
	if ok && !bytes.HasPrefix(i.iter.UnsafeKey().Key, i.prefix) {
		return false, nil
	}

	return ok, nil
}

// Next implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Next() {
	i.iter.Next()
}

// Key implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Key() []byte {
	return i.iter.Key().Key[len(i.prefix):]
}

// Value implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Value() []byte {
	return i.iter.Value()
}

// UnsafeKey implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) UnsafeKey() []byte {
	return i.iter.UnsafeKey().Key[len(i.prefix):]
}

// UnsafeValue implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) UnsafeValue() []byte {
	return i.iter.UnsafeValue()
}

// Close implements the SortedDiskMapIterator interface.
func (i *RocksDBMapIterator) Close() {
	i.iter.Close()
}

// Put implements the SortedDiskMapBatchWriter interface.
func (b *RocksDBMapBatchWriter) Put(k []byte, v []byte) error {
	if err := b.batch.Put(b.makeKey(k), v); err != nil {
		return err
	}
	b.numPutsSinceFlush++
	if b.batch.Len() >= b.capacity {
		return b.Flush()
	}
	return nil
}

// Flush implements the SortedDiskMapBatchWriter interface.
func (b *RocksDBMapBatchWriter) Flush() error {
	if b.batch.Empty() {
		return nil
	}
	if err := b.batch.Commit(false /* syncCommit */); err != nil {
		return err
	}
	b.numPutsSinceFlush = 0
	b.batch = b.store.NewWriteOnlyBatch()
	return nil
}

// NumPutsSinceFlush implements the SortedDiskMapBatchWriter interface.
func (b *RocksDBMapBatchWriter) NumPutsSinceFlush() int {
	return b.numPutsSinceFlush
}

// Close implements the SortedDiskMapBatchWriter interface.
func (b *RocksDBMapBatchWriter) Close(ctx context.Context) error {
	err := b.Flush()
	b.batch.Close()
	return err
}

// pebbleMapBatchWriter batches writes to a pebbleMap.
type pebbleMapBatchWriter struct {
	// capacity is the number of bytes to write before a Flush() is triggered.
	capacity int

	// makeKey is a function that transforms a key into a byte slice with a prefix
	// to be written to the underlying store.
	makeKey           func(k []byte) []byte
	batch             *pebble.Batch
	numPutsSinceFlush int
	store             *pebble.DB
}

// pebbleMapIterator iterates over the keys of a pebbleMap in sorted order.
type pebbleMapIterator struct {
	allowDuplicates bool
	iter            *pebble.Iterator
	// makeKey is a function that transforms a key into a byte slice with a prefix
	// used to SeekGE() the underlying iterator.
	makeKey func(k []byte) []byte
	// prefix is the prefix of keys that this iterator iterates over.
	prefix []byte
}

// pebbleMap is a SortedDiskMap, similar to rocksDBMap, that uses pebble as its
// underlying storage engine.
type pebbleMap struct {
	prefix          []byte
	store           *pebble.DB
	allowDuplicates bool
	keyID           int64
}

var _ diskmap.SortedDiskMapBatchWriter = &pebbleMapBatchWriter{}
var _ diskmap.SortedDiskMapIterator = &pebbleMapIterator{}
var _ diskmap.SortedDiskMap = &pebbleMap{}

// newPebbleMap creates a new pebbleMap with the passed in Engine as the
// underlying store. The pebbleMap instance will have a keyspace prefixed by a
// unique prefix. The allowDuplicates parameter controls whether Puts with
// identical keys will write multiple entries or overwrite previous entries.
func newPebbleMap(e *pebble.DB, allowDuplicates bool) *pebbleMap {
	prefix := generateTempStorageID()
	return &pebbleMap{
		prefix:          encoding.EncodeUvarintAscending([]byte(nil), prefix),
		store:           e,
		allowDuplicates: allowDuplicates,
	}
}

// makeKey appends k to the pebbleMap's prefix to keep the key local to this
// instance and returns a byte slice containing the user-provided key and the
// prefix. Pebble's operations can take this byte slice as a key. This key is
// only valid until the next call to makeKey.
func (r *pebbleMap) makeKey(k []byte) []byte {
	prefixLen := len(r.prefix)
	r.prefix = append(r.prefix, k...)
	key := r.prefix
	r.prefix = r.prefix[:prefixLen]
	return key
}

// makeKeyWithSequence makes a key appropriate for a Put operation. It is like
// makeKey except it respects allowDuplicates, by appending a sequence number to
// the user-provided key.
func (r *pebbleMap) makeKeyWithSequence(k []byte) []byte {
	byteKey := r.makeKey(k)
	if r.allowDuplicates {
		r.keyID++
		byteKey = encoding.EncodeUint64Ascending(byteKey, uint64(r.keyID))
	}
	return byteKey
}

// NewIterator implements the SortedDiskMap interface.
func (r *pebbleMap) NewIterator() diskmap.SortedDiskMapIterator {
	return &pebbleMapIterator{
		allowDuplicates: r.allowDuplicates,
		iter: r.store.NewIter(&pebble.IterOptions{
			UpperBound: roachpb.Key(r.prefix).PrefixEnd(),
		}),
		makeKey: r.makeKey,
		prefix:  r.prefix,
	}
}

// NewBatchWriter implements the SortedDiskMap interface.
func (r *pebbleMap) NewBatchWriter() diskmap.SortedDiskMapBatchWriter {
	return r.NewBatchWriterCapacity(defaultBatchCapacityBytes)
}

// NewBatchWriterCapacity implements the SortedDiskMap interface.
func (r *pebbleMap) NewBatchWriterCapacity(capacityBytes int) diskmap.SortedDiskMapBatchWriter {
	makeKey := r.makeKey
	if r.allowDuplicates {
		makeKey = r.makeKeyWithSequence
	}
	return &pebbleMapBatchWriter{
		capacity: capacityBytes,
		makeKey:  makeKey,
		batch:    r.store.NewBatch(),
		store:    r.store,
	}
}

// Clear implements the SortedDiskMap interface.
func (r *pebbleMap) Clear() error {
	if err := r.store.DeleteRange(
		r.prefix,
		roachpb.Key(r.prefix).PrefixEnd(),
		pebble.NoSync,
	); err != nil {
		return errors.Wrapf(err, "unable to clear range with prefix %v", r.prefix)
	}
	// NB: we manually flush after performing the clear range to ensure that the
	// range tombstone is pushed to disk which will kick off compactions that
	// will eventually free up the deleted space.
	_, err := r.store.AsyncFlush()
	return err
}

// Close implements the SortedDiskMap interface.
func (r *pebbleMap) Close(ctx context.Context) {
	if err := r.Clear(); err != nil {
		log.Error(ctx, err)
	}
}

// SeekGE implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Seek(k []byte) {
	i.iter.SeekGE(i.makeKey(k))
}

// Rewind implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Rewind() {
	i.iter.SeekGE(i.makeKey(nil))
}

// Valid implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Valid() (bool, error) {
	return i.iter.Valid(), nil
}

// Next implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Next() {
	i.iter.Next()
}

// Key implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Key() []byte {
	unsafeKey := i.iter.Key()
	end := len(unsafeKey)
	if i.allowDuplicates {
		// There are 8 bytes of sequence number at the end of the key, remove them.
		end -= 8
	}
	//(zhangwg):unsafeKey会随着外部iter移动被修改,所以通过append复制出来返回
	var key []byte
	key = append(key, unsafeKey[len(i.prefix):end]...)
	return key
}

// Value implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Value() []byte {
	return i.iter.Value()
}

// UnsafeKey implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) UnsafeKey() []byte {
	unsafeKey := i.iter.Key()
	end := len(unsafeKey)
	if i.allowDuplicates {
		// There are 8 bytes of sequence number at the end of the key, remove them.
		end -= 8
	}
	return unsafeKey[len(i.prefix):end]
}

// UnsafeValue implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) UnsafeValue() []byte {
	return i.iter.Value()
}

// Close implements the SortedDiskMapIterator interface.
func (i *pebbleMapIterator) Close() {
	_ = i.iter.Close()
}

// Put implements the SortedDiskMapBatchWriter interface.
func (b *pebbleMapBatchWriter) Put(k []byte, v []byte) error {
	key := b.makeKey(k)
	if err := b.batch.Set(key, v, nil); err != nil {
		return err
	}
	b.numPutsSinceFlush++
	if len(b.batch.Repr()) >= b.capacity {
		return b.Flush()
	}
	return nil
}

// Flush implements the SortedDiskMapBatchWriter interface.
func (b *pebbleMapBatchWriter) Flush() error {
	if err := b.batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	b.numPutsSinceFlush = 0
	b.batch = b.store.NewBatch()
	return nil
}

// NumPutsSinceFlush implements the SortedDiskMapBatchWriter interface.
func (b *pebbleMapBatchWriter) NumPutsSinceFlush() int {
	return b.numPutsSinceFlush
}

// Close implements the SortedDiskMapBatchWriter interface.
func (b *pebbleMapBatchWriter) Close(ctx context.Context) error {
	err := b.Flush()
	if err != nil {
		return err
	}
	return b.batch.Close()
}
