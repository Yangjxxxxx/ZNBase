package badger

import (
	"bytes"
	"github.com/dgraph-io/badger/y"
	"github.com/dgryski/go-farm"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type WriteBatch struct {
	sync.Mutex
	ts uint64

	update        bool              // update is used to conditionally keep track of reads.
	reads         []uint64          // contains fingerprints of keys read.
	writes        []uint64          // contains fingerprints of keys written.
	pendingWrites map[string]*Entry // cache stores any writes done by txn.

	db        *DB
	discarded bool

	throttle *y.Throttle
	err      error

	size         int64
	count        int64
	numIterators int32
}

type WriteBatchIterator struct {
	entries []*Entry
	nextIdx int
	readTs  uint64
	//reversed bool
}

func (d *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		db:            d,
		pendingWrites: make(map[string]*Entry),
		throttle:      y.NewThrottle(16),
	}
}
func (wb *WriteBatch) Cancel() {
	if err := wb.throttle.Finish(); err != nil {
		wb.db.opt.Errorf("WatchBatch.Cancel error while finishing: %v", err)
	}
	wb.Discard()
}
func (wb *WriteBatch) Discard() {
	if wb.discarded { // Avoid a re-run.
		return
	}
	if atomic.LoadInt32(&wb.numIterators) > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	wb.discarded = true
	/*
		if !wb.db.orc.isManaged {
			wb.db.orc.readMark.Done(wb.ts)
		}
		wb.db.orc.decrRef()
	*/
}
func (wb *WriteBatch) Set(key, val []byte) error {
	e := &Entry{
		Key:   key,
		Value: val,
	}
	return wb.SetEntry(e)
}

// SetWithTTL is equivalent of Txn.SetWithTTL.
func (wb *WriteBatch) SetWithTTL(key, val []byte, dur time.Duration) error {
	expire := time.Now().Add(dur).Unix()
	e := &Entry{Key: key, Value: val, ExpiresAt: uint64(expire)}
	return wb.SetEntry(e)
}
func (wb *WriteBatch) SetEntry(e *Entry) error {
	return wb.modify(e)
}

func (wb *WriteBatch) modify(e *Entry) error {
	const maxKeySize = 65000

	switch {
	case !wb.update:
		return ErrReadOnlyTxn
	case len(e.Key) == 0:
		return ErrEmptyKey
	case bytes.HasPrefix(e.Key, badgerPrefix):
		return ErrInvalidKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, e.Key)
	case int64(len(e.Value)) > wb.db.opt.ValueLogFileSize:
		return exceedsSize("Value", wb.db.opt.ValueLogFileSize, e.Value)
	}

	if err := wb.checkSize(e); err != nil {
		return err
	}
	fp := farm.Fingerprint64(e.Key) // Avoid dealing with byte arrays.
	wb.writes = append(wb.writes, fp)
	wb.pendingWrites[string(e.Key)] = e
	return nil
}

func (wb *WriteBatch) checkSize(e *Entry) error {
	count := wb.count + 1
	// Extra bytes for version in key.
	size := wb.size + int64(e.estimateSize(wb.db.opt.ValueThreshold)) + 10
	if count >= wb.db.opt.maxBatchCount || size >= wb.db.opt.maxBatchSize {
		return ErrTxnTooBig
	}
	wb.count, wb.size = count, size
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	wb.Lock()
	defer wb.Unlock()
	e := &Entry{
		Key:  key,
		meta: bitDelete,
	}
	return wb.modify(e)
}
func (wb *WriteBatch) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if wb.discarded {
		return nil, ErrDiscardedTxn
	}

	item = new(Item)
	if e, has := wb.pendingWrites[string(key)]; has && bytes.Equal(key, e.Key) {
		if isDeletedOrExpired(e.meta, e.ExpiresAt) {
			return nil, ErrKeyNotFound
		}
		// Fulfill from cache.
		item.meta = e.meta
		item.val = e.Value
		item.userMeta = e.UserMeta
		item.key = key
		item.status = prefetched
		item.version = wb.ts
		item.expiresAt = e.ExpiresAt
		// We probably don't need to set db on item here.
		return item, nil
	}
	return nil, nil
}

// Caller to commit must hold a write lock.
func (wb *WriteBatch) commit() (func() error, error) {
	if wb.err != nil {
		return nil, wb.err
	}
	txn := wb.db.NewNoneTransaction()
	orc := wb.db.orc
	commitTs := txn.commitTs

	entries := make([]*Entry, 0, len(txn.pendingWrites)+1)
	for _, e := range txn.pendingWrites {
		// fmt.Fprintf(&b, "[%q : %q], ", e.Key, e.Value)

		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = y.KeyWithTs(e.Key, commitTs)
		e.meta |= bitTxn
		entries = append(entries, e)
	}
	// log.Printf("%s\n", b.String())
	e := &Entry{
		Key:   y.KeyWithTs(txnKey, commitTs),
		Value: []byte(strconv.FormatUint(commitTs, 10)),
		meta:  bitFinTxn,
	}
	entries = append(entries, e)

	req, err := txn.db.sendToWriteCh(entries)
	if err != nil {
		//orc.doneCommit(commitTs)
		return nil, err
	}
	ret := func() error {
		err := req.Wait()
		// Wait before marking commitTs as done.
		// We can't defer doneCommit above, because it is being called from a
		// callback here.
		orc.doneCommit(commitTs)
		return err
	}

	if err := wb.throttle.Do(); err != nil {
		return ret, err
	}

	wb.ts = 0 // We're not reading anything.
	return ret, wb.err
}

// Flush must be called at the end to ensure that any pending writes get committed to Badger. Flush
// returns any error stored by WriteBatch.
func (wb *WriteBatch) Flush() error {
	wb.Lock()
	_, _ = wb.commit()
	wb.Discard()
	wb.Unlock()

	if err := wb.throttle.Finish(); err != nil {
		return err
	}

	return wb.err
}
func (wb *WriteBatch) newWriteBatchIterator() *WriteBatchIterator {
	if !wb.update || len(wb.pendingWrites) == 0 {
		return nil
	}
	entries := make([]*Entry, 0, len(wb.pendingWrites))
	for _, e := range wb.pendingWrites {
		entries = append(entries, e)
	}
	// Number of pending writes per transaction shouldn't be too big in general.
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		reversed := false //TODO 反向遍历也是按照“小->大”顺序排？
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &WriteBatchIterator{
		readTs:  wb.ts,
		entries: entries,
		//reversed: reversed,
	}
}
func (pi *WriteBatchIterator) Next() {
	pi.nextIdx++
}

func (pi *WriteBatchIterator) Prev() {
	if pi.nextIdx > 0 {
		pi.nextIdx--
	}
}

func (pi *WriteBatchIterator) SeekToFirst() {
	pi.rewind(false)
}
func (pi *WriteBatchIterator) SeekToLast() {
	pi.rewind(true)
}
func (pi *WriteBatchIterator) Rewind() {
	pi.rewind(false)
}
func (pi *WriteBatchIterator) rewind(reversed bool) {
	pi.nextIdx = 0
}

func (pi *WriteBatchIterator) Seek(key []byte) {
	pi.seek(key)
}

func (pi *WriteBatchIterator) SeekForPrev(key []byte) {
	pi.seek(key)
}

func (pi *WriteBatchIterator) seek(key []byte) {
	key = y.ParseKey(key)
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].Key, key)
		reversed := false //TODO 小->大的正向排序
		if !reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (pi *WriteBatchIterator) Key() []byte {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.KeyWithTs(entry.Key, pi.readTs)
}

func (pi *WriteBatchIterator) Value() y.ValueStruct {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.ValueStruct{
		Value:     entry.Value,
		Meta:      entry.meta,
		UserMeta:  entry.UserMeta,
		ExpiresAt: entry.ExpiresAt,
		Version:   pi.readTs,
	}
}

func (pi *WriteBatchIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

func (pi *WriteBatchIterator) Close() error {
	return nil
}
