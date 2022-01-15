/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"time"

	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
	"sync"
	"sync/atomic"
)

// None Badger transaction.
type Txnone struct {
	Txn
	readTs   uint64
	commitTs uint64

	db        *DB
	discarded bool

	size         int64
	count        int64
	numIterators int32
	mutex        sync.RWMutex
}

func (txn *Txnone) checkSize(e *Entry) error {
	count := txn.count + 1
	// Extra bytes for version in key.
	size := txn.size + int64(e.estimateSize(txn.db.opt.ValueThreshold)) + 10
	if count >= txn.db.opt.maxBatchCount || size >= txn.db.opt.maxBatchSize {
		//return ErrTxnTooBig
	}
	txn.count, txn.size = count, size
	return nil
}

// Set adds a key-value pair to the database.
func (txn *Txnone) Set(key, val []byte) error {
	e := &Entry{
		Key:   key,
		Value: val,
	}
	return txn.SetEntry(e)
}

// SetWithMeta adds a key-value pair to the database, along with a metadata
// byte.
func (txn *Txnone) SetWithMeta(key, val []byte, meta byte) error {
	e := &Entry{Key: key, Value: val, UserMeta: meta}
	return txn.SetEntry(e)
}

// SetWithDiscard acts like SetWithMeta, but adds a marker to discard earlier
// versions of the key.
func (txn *Txnone) SetWithDiscard(key, val []byte, meta byte) error {
	e := &Entry{
		Key:      key,
		Value:    val,
		UserMeta: meta,
		meta:     bitDiscardEarlierVersions,
	}
	return txn.SetEntry(e)
}

// SetWithTTL adds a key-value pair to the database, along with a time-to-live
// (TTL) setting. A key stored with a TTL would automatically expire after the
// time has elapsed , and be eligible for garbage collection.
func (txn *Txnone) SetWithTTL(key, val []byte, dur time.Duration) error {
	expire := time.Now().Add(dur).Unix()
	e := &Entry{Key: key, Value: val, ExpiresAt: uint64(expire)}
	return txn.SetEntry(e)
}

func (txn *Txnone) modify(e *Entry) error {
	const maxKeySize = 65000

	switch {
	case txn.discarded:
		return ErrDiscardedTxn
	case len(e.Key) == 0:
		return ErrEmptyKey
	case bytes.HasPrefix(e.Key, badgerPrefix):
		return ErrInvalidKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, e.Key)
	case int64(len(e.Value)) > txn.db.opt.ValueLogFileSize:
		return exceedsSize("Value", txn.db.opt.ValueLogFileSize, e.Value)
	}

	if err := txn.checkSize(e); err != nil {
		return err
	}
	err := txn.commitAndSend(e)
	return err
}

// SetEntry takes an Entry struct and adds the key-value pair in the struct,
// along with other metadata to the database.
func (txn *Txnone) SetEntry(e *Entry) error {
	return txn.modify(e)
}

// Delete deletes a key.
func (txn *Txnone) Delete(key []byte) error {
	e := &Entry{
		Key:  key,
		meta: bitDelete,
	}
	return txn.modify(e)
}

func (txn *Txnone) Get(key []byte) (item *Item, rerr error) {
	return txn.rawGet(key)
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txnone) rawGet(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if txn.discarded {
		return nil, ErrDiscardedTxn
	}

	item = new(Item)

	vs, err := txn.db.get(key)
	if err != nil {
		return nil, errors.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return nil, ErrKeyNotFound
	}

	item.key = key
	item.version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.db = txn.db
	item.vptr = vs.Value // TODO: Do we need to copy this over?
	item.txn = nil
	item.expiresAt = vs.ExpiresAt
	return item, nil
}
func (txn *Txnone) newCommitTs() uint64 {
	//return uint64(time.Now().UnixNano())
	return txn.commitTs
}
func (txn *Txnone) commitAndSend(e *Entry) error {
	commitTs := txn.newCommitTs()

	e.Key = y.KeyWithTs(e.Key, commitTs)
	e.meta |= bitTxn

	entries := []*Entry{e}
	// log.Printf("%s\n", b.String())
	/*
		te := &Entry{ //事务日志?
			Key:   y.KeyWithTs(txnKey, commitTs),
			Value: []byte(strconv.FormatUint(commitTs, 10)),
			meta:  bitFinTxn,
		}*/
	req := requestPool.Get().(*request)
	req.Entries = entries
	req.Wg = sync.WaitGroup{}
	req.Wg.Add(1)
	txn.mutex.Lock()
	defer txn.mutex.Unlock()
	txn.db.writeRequests([]*request{req})
	/*
		req, err := txn.db.sendToWriteCh(entries)
		if err != nil {
			return err
		}
		err = req.Wait()
	*/
	return nil
}

func (txn *Txnone) commitPrecheck() {
	if txn.commitTs == 0 && txn.db.opt.managedTxns {
		panic("Commit cannot be called with managedDB=true. Use CommitAt.")
	}
	if txn.discarded {
		panic("Trying to commit a discarded txn")
	}
}

// ReadTs returns the read timestamp of the transaction.
func (txn *Txnone) ReadTs() uint64 {
	return txn.readTs
}
func (txn *Txnone) NewIterator(opt IteratorOptions) *NIterator {
	// Do not change the order of the next if. We must track the number of running iterators.
	if atomic.AddInt32(&txn.numIterators, 1) > 1 {
		atomic.AddInt32(&txn.numIterators, -1)
		panic("Only one iterator can be active at one time .")
	}

	// TODO: If Prefix is set, only pick those memtables which have keys with the prefix.
	tables, decr := txn.db.getMemTables()
	defer decr()
	txn.db.vlog.incrIteratorCount()
	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].NewUniIterator())
	}
	iters = txn.db.lc.appendIterators(iters, &opt) // This will increment references.
	res := &NIterator{
		txn:    txn,
		iitr:   y.NewMergeIterator(iters),
		opt:    opt,
		readTs: txn.readTs,
	}
	return res
}

// NewKeyIterator is just like NewIterator, but allows the user to iterate over all versions of a
// single key. Internally, it sets the Prefix option in provided opt, and uses that prefix to
// additionally run bloom filter lookups before picking tables from the LSM tree.
func (txn *Txnone) NewKeyIterator(key []byte, opt IteratorOptions) *NIterator {
	if len(opt.Prefix) > 0 {
		panic("opt.Prefix should be nil for NewKeyIterator.")
	}
	opt.Prefix = key // This key must be without the timestamp.
	opt.prefixIsKey = true
	return txn.NewIterator(opt)
}

//  // Call various APIs.
func (db *DB) NewNoneTransaction() *Txnone {
	return db.NewNoneTransactionWith(uint64(time.Now().UnixNano()))
}
func (db *DB) NewNoneTransactionWith(ts uint64) *Txnone {
	txn := &Txnone{
		db:       db,
		count:    1,                       // One extra entry for BitFin.
		size:     int64(len(txnKey) + 10), // Some buffer for the extra entry.
		readTs:   ts,
		commitTs: ts,
		mutex:    sync.RWMutex{},
	}
	return txn

}
