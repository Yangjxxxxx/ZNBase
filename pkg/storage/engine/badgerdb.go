package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
)

// BadgerDB is a wrapper around a BadgerDB database instance.
type BadgerDB struct {
	cfg    *badger.Options
	db     *badger.DB
	txnone *badger.Txnone

	commit struct {
		syncutil.Mutex
		cond       sync.Cond
		committing bool
		groupSize  int
		pending    []*badgerDBBatch
	}

	syncer struct {
		syncutil.Mutex
		cond    sync.Cond
		closed  bool
		pending []*badgerDBBatch
	}

	iters struct {
		syncutil.Mutex
		m map[*rocksDBIterator][]byte
	}
}

//var _ Engine = &BadgerDB{}

// String formatter.
func (r *BadgerDB) String() string {
	dir := r.cfg.Dir
	return fmt.Sprintf("Dir=%s", dir)
}

//##################Reader Interface##################

// Close closes the reader
func (r *BadgerDB) Close() {
	if r.db == nil {
		log.Errorf(context.TODO(), "closing unopened rocksdb instance")
		return
	}

	_ = r.db.Close()
	r.db = nil
}

// Closed returns true if the engine is closed.
func (r *BadgerDB) Closed() bool {
	return r.db == nil
}

// Get returns the []bytes with error
func (r *BadgerDB) Get(key MVCCKey) ([]byte, error) {
	return txnGet(r.txnone, key)
}

// GetProto gets the proto
func (r *BadgerDB) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return txnGetProto(r.txnone, key, msg)
}

// Iterate interates MVCCKey
func (r *BadgerDB) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (stop bool, err error)) error {
	return nil
}

// NewIterator inits the interator
func (r *BadgerDB) NewIterator(opts IterOptions) Iterator {
	//opt := badger.DefaultIteratorOptions
	return newBadgerDBIterator(opts, r.db, r)
}

// NewBatch returns a new batch wrapping this rocksdb engine.
func (r *BadgerDB) NewBatch() Batch {
	return nil
}

// NewWriteOnlyBatch inits a write only batch
func (r *BadgerDB) NewWriteOnlyBatch() Batch {
	return nil
}

//##################Writer Interface##################

//##################Engine Interface##################

type badgerDBBatch struct {
	parent *BadgerDB
}

var dbBatchPool = sync.Pool{
	New: func() interface{} {
		return &badgerDBBatch{}
	},
}

//##################BadgerDB Iterator Interface##################
type badgerDBIterator struct {
	parent  *badger.DB
	engine  Reader
	iter    *badger.NIterator
	valid   bool
	reseek  bool
	err     error
	item    *badger.Item
	intents *badger.WriteBatch
}

var _ Iterator = &badgerDBIterator{}

var bIterPool = sync.Pool{
	New: func() interface{} {
		return &badgerDBIterator{}
	},
}

func newBadgerDBIterator(opts IterOptions, parent *badger.DB, engine Reader) Iterator {
	r := bIterPool.Get().(*badgerDBIterator)
	r.init(opts, engine, parent)
	return r
}
func (r *badgerDBIterator) init(opts IterOptions, engine Reader, parent *badger.DB) {
	r.parent = parent
	txnone := parent.NewNoneTransaction()
	opt := badger.DefaultIteratorOptions
	r.iter = txnone.NewIterator(opt)
	r.engine = engine
	r.intents = parent.NewWriteBatch()
}

func (r *badgerDBIterator) getIter() *badger.NIterator {
	return r.iter
}

func (r *badgerDBIterator) Stats() IteratorStats {
	return IteratorStats{0, 0}
}

func (r *badgerDBIterator) Close() {
	r.intents.Cancel()
	r.iter.Close()
}

func (r *badgerDBIterator) Seek(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		// 定位到第一个key
		r.iter.SeekToFirst()
		r.setState(r.iter)
	} else {
		//*/ We can avoid seeking if we're already at the key we seek.
		//badger不需要该缓存?
		if r.valid && !r.reseek && key.Equal(r.UnsafeKey()) {
			return
		}
		r.iter.Seek(toRawKey(key))
		r.setState(r.iter)
	}
}

func (r *badgerDBIterator) SeekReverse(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		r.iter.SeekToLast()
		r.setState(r.iter)
	} else {
		if r.valid && !r.reseek && key.Equal(r.UnsafeKey()) {
			return
		}
		r.iter.Seek(toRawKey(key))
		r.setState(r.iter)
	}
}

func (r *badgerDBIterator) Valid() (bool, error) {
	return r.valid, nil
}

func (r *badgerDBIterator) Next() {
	r.checkEngineOpen()
	//r.setState(C.DBIterNext(r.iter, C.bool(false) /* skip_current_key_versions */))
	r.iter.Next()
	r.setState(r.iter)
}

func (r *badgerDBIterator) Prev() {
	r.checkEngineOpen()
	//skip_current_key_versions = true
	r.iter.Prev()
	r.setState(r.iter)
}

func (r *badgerDBIterator) NextKey() {
	r.checkEngineOpen()
	//skip_current_key_versions = true
	r.iter.Next()
	r.setState(r.iter)
}

func (r *badgerDBIterator) PrevKey() {
	r.checkEngineOpen()
	//skip_current_key_versions = true
	r.iter.Prev()
	r.setState(r.iter)
}

func (r *badgerDBIterator) Key() MVCCKey {
	// should merge variable declaration with assignment on next line (S1021)
	// var keyCopy []byte // = make([]byte,PREPARED_NUM)
	keyCopy := r.item.KeyCopy(nil)
	return fromRawKey(keyCopy)
}

func (r *badgerDBIterator) Value() []byte {
	var valCopy []byte // = make([]byte,PREPARED_NUM)
	valCopy, _ = r.item.ValueCopy(nil)
	return valCopy
}

func (r *badgerDBIterator) ValueProto(msg protoutil.Message) error {
	if len(r.item.UnsafeValue()) <= 0 {
		return nil
	}
	return protoutil.Unmarshal(r.UnsafeValue(), msg)
}

func (r *badgerDBIterator) UnsafeKey() MVCCKey {
	return fromRawKey(r.item.Key())
}

func (r *badgerDBIterator) UnsafeValue() []byte {
	return r.item.UnsafeValue()
}

func (r *badgerDBIterator) ComputeStats(
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return enginepb.MVCCStats{}, errors.Errorf("Unsupported method :ComputeStats ")
}

func (r *badgerDBIterator) FindSplitKey(
	start, end, minSplitKey MVCCKey, targetSize int64,
) (MVCCKey, error) {
	return MVCCKey{}, errors.Errorf("Unsupported method :FindSplitKey ")
}

func (r *badgerDBIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}

	r.clearState()

	/* rocksDB中会查询Intents:
	  intents, err := buildScanIntents(cSliceToGoBytes(state.intents))
	而Intents的内容是C语言的rocksdb::WriteBatch类型，它的值是在mvcc.h中的get和scal是put的

	本实现：不检查
	*/
	mkey := MVCCKey{Key: key, Timestamp: timestamp}
	item, err := r.iter.Get(toRawKey(mkey))
	if err != nil {
		panic(err)
	}
	if item == nil {
		return nil, nil, nil
	}
	value := &roachpb.Value{
		RawBytes:  item.UnsafeValue(),
		Timestamp: timestamp, //TODO
	}

	return value, nil, nil
}

func (r *badgerDBIterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (MVCCScanResult, error) {

	return MVCCScanResult{}, nil
}

func (r *badgerDBIterator) SetUpperBound(key roachpb.Key) {
	//C.DBIterSetUpperBound(r.iter, goToCKey(MakeMVCCMetadataKey(key)))
}

func (r *badgerDBIterator) checkEngineOpen() bool {
	return r.parent != nil
}

func (r *badgerDBIterator) clearState() {
	r.valid = false
	r.reseek = true
	r.item = nil
	r.err = nil
}
func (r *badgerDBIterator) setState(state *badger.NIterator) {
	r.valid = state.Valid()
	r.reseek = false
	if r.valid {
		r.item = state.Item()
	} else {
		r.item = nil
	}
	//r.err = statusToError(state)
}

// CheckForKeyCollisions indicates if the provided SST data collides with this
// iterator in the specified range.
func (r *badgerDBIterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	panic("not implemented")
}

//##################common method##################
func txnGetProto(
	txnone *badger.Txnone, key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		err = emptyKeyError()
		return
	}
	var result []byte
	if result, err = txnGet(txnone, key); err != nil {
		return
	}
	if len(result) <= 0 {
		msg.Reset()
		return
	}
	ok = true
	if msg != nil {
		err = protoutil.Unmarshal(result, msg)
	}
	keyBytes = int64(key.EncodedSize())
	valBytes = int64(len(result))
	return
}

func txnGet(txnone *badger.Txnone, key MVCCKey) ([]byte, error) {
	item, err := txnone.Get(toRawKey(key))
	if err != nil {
		return nil, err
	}
	var valCopy []byte
	valCopy, err = item.ValueCopy(nil)
	return valCopy, err
}

func toRawKey(key MVCCKey) []byte {
	out := make([]byte, len(key.Key)+8+4)
	copy(out, key.Key)
	binary.BigEndian.PutUint64(out[len(key.Key):], uint64(key.Timestamp.WallTime))
	binary.BigEndian.PutUint32(out[len(key.Key)+8:], uint32(key.Timestamp.Logical))
	return out
}

func fromRawKey(key []byte) MVCCKey {
	return MVCCKey{
		Key: key[:len(key)-12],
		Timestamp: hlc.Timestamp{
			WallTime: int64(binary.BigEndian.Uint64(key[len(key)-12 : len(key)-4])),
			Logical:  int32(binary.BigEndian.Uint64(key[len(key)-4:])),
		},
	}
}
