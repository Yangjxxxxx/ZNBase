package badger

import (
	"bytes"
	"encoding/binary"
)

type DBTimestamp struct {
	wall_time int64
	logical   int32
}
type mvccKey struct {
	key []byte
	ts  DBTimestamp
}

var kZeroTimestamp = DBTimestamp{0, 0}
var kMVCCVersionTimestampSize = 12
var kMaxItersBeforeSeek = 10

func (t *DBTimestamp) compare(oth DBTimestamp) int {
	if t.wall_time > oth.wall_time {
		return 1
	} else if t.wall_time < oth.wall_time {
		return -1
	} else if t.logical > oth.logical {
		return 1
	} else if t.logical < oth.logical {
		return -1
	}
	return 0
}

type DBStatus string
type Slice []byte
type DBSlice []byte
type DBChunkedBuffer struct {
	bufs *DBSlice
	// len is the number of DBSlices in bufs.
	len int32
	// count is the number of key/value pairs in bufs.
	count int32
}
type DBTxn struct {
	id            DBSlice
	epoch         uint32
	sequence      int32
	max_timestamp DBTimestamp
}
type DBScanResults struct {
	status                DBStatus
	data                  DBChunkedBuffer
	intents               DBSlice
	uncertainty_timestamp DBTimestamp
	resume_key            DBSlice
}

var iters_count *int64

type DBIterator struct {
	db  *DB
	rep *NIterator
	//kvs chunkedBuffer
	intents *WriteBatch
	//stats IteratorStats
	rev_resume_key string
	//read_opts ReadOptions
	lower_bound_str string
	upper_bound_str string
	lower_bound     []byte
	upper_bound     []byte
}

func NewDBIterator(db DB, opts IteratorOptions) DBIterator {
	txn := db.NewNoneTransaction()
	itr := DBIterator{
		db:      &db,
		rep:     txn.NewIterator(opts),
		intents: db.NewWriteBatch(),
	}
	(*iters_count)++
	return itr
}
func (d *DBIterator) Close() {
	d.intents.Flush()
	d.rep.Close()
}

func (d *DBIterator) SetLowerBound(key []byte) {
	if key == nil {
		d.lower_bound = KeyMin
	} else {
		d.lower_bound = key
	}
	d.lower_bound_str = string(d.lower_bound)
}

func (d *DBIterator) SetUpperBound(key []byte) {
	if key == nil {
		d.upper_bound = KeyMax
	} else {
		d.upper_bound = key
	}
	d.upper_bound_str = string(d.upper_bound)
}

type MvccScanner struct {
	iter_              *DBIterator
	iter_rep_          *NIterator
	start_key_         []byte
	end_key_           []byte
	max_keys_          int64
	timestamp_         DBTimestamp
	txn_id_            []byte
	txn_epoch_         uint32
	txn_sequence_      int32
	txn_max_timestamp_ DBTimestamp
	inconsistent_      bool
	tombstones_        bool
	ignore_sequence_   bool
	check_uncertainty_ bool
	results_           DBScanResults
	//kvs_ chunkedBuffer
	intents_   *WriteBatch
	key_buf_   string
	saved_buf_ []byte
	peeked_    bool
	//meta_ znbase::storage::engine::enginepb::MVCCMetadata
	// cur_raw_key_ holds either iter_rep_->key() or the saved value of
	// iter_rep_->key() if we've peeked at the previous key (and peeked_
	// is true).
	cur_raw_key_ []byte
	// cur_key_ is the decoded MVCC key, separated from the timestamp
	// suffix.
	cur_key_ Slice
	// cur_value_ holds either iter_rep_->value() or the saved value of
	// iter_rep_->value() if we've peeked at the previous key (and
	// peeked_ is true).
	cur_value_ []byte
	// cur_timestamp_ is the timestamp for a decoded MVCC key.
	cur_timestamp_     DBTimestamp
	iters_before_seek_ int
	reverse            bool
}

func NewMvccScanner(iter *DBIterator, start DBSlice, end DBSlice, timestamp DBTimestamp, max_keys int64,
	txn DBTxn, inconsistent bool, tombstones bool, ignore_sequence bool) MvccScanner {
	scanner := MvccScanner{
		iter_:              iter,
		iter_rep_:          iter.rep,
		start_key_:         start,
		end_key_:           end,
		max_keys_:          max_keys,
		txn_id_:            txn.id,
		txn_epoch_:         txn.epoch,
		txn_sequence_:      txn.sequence,
		txn_max_timestamp_: txn.max_timestamp,
		inconsistent_:      inconsistent,
		tombstones_:        tombstones,
		ignore_sequence_:   ignore_sequence,
		//kvs_: {},
		intents_:           iter.db.NewWriteBatch(), //iter.intents,
		peeked_:            false,
		check_uncertainty_: timestamp.compare(txn.max_timestamp) < 0,
		iters_before_seek_: kMaxItersBeforeSeek / 2,
		//memset(&results_, 0, sizeof(results_))
		reverse: false,
	}
	//memset(&results_, 0, sizeof(results_));
	//results_.status = kSuccess;

	return scanner
}

// The MVCC data is sorted by key and descending timestamp. If a key
// has a write intent (i.e. an uncommitted transaction has written
// to the key) a key with a zero timestamp, with an MVCCMetadata
// value, will appear. We arrange for the keys to be sorted such
// that the intent sorts first. For example:
//
//   A @ T3
//   A @ T2
//   A @ T1
//   B <intent @ T2>
//   B @ T2
//
// Here we have 2 keys, A and B. Key A has 3 versions, T3, T2 and
// T1. Key B has 1 version, T1, and an intent. Scanning involves
// looking for values at a particular timestamp. For example, let's
// consider scanning this entire range at T2. We'll first seek to A,
// discover the value @ T3. This value is newer than our read
// timestamp so we'll iterate to find a value newer than our read
// timestamp (the value @ T2). We then iterate to the next key and
// discover the intent at B. What happens with the intent depends on
// the mode we're reading in and the timestamp of the intent. In
// this case, the intent is at our read timestamp. If we're
// performing an inconsistent read we'll return the intent and read
// at the instant of time just before the intent (for only that
// key). If we're reading consistently, we'll either return the
// intent along with an error or read the intent value if we're
// reading transactionally and we own the intent.

func (s *MvccScanner) get() DBScanResults {
	if !s.iterSeek(EncodeKey(s.start_key_, 0, 0)) {
		return s.results_
	}
	//s.getAndAdvance()
	return DBScanResults{} //s.fillResults()
}

func (s *MvccScanner) scan() DBScanResults {
	// TODO(peter): Remove this timing/debugging code.
	// auto pctx = rocksdb::get_perf_context();
	// pctx->Reset();
	// auto start_time = std::chrono::steady_clock::now();
	// auto elapsed = std::chrono::steady_clock::now() - start_time;
	// auto micros = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
	// printf("seek %d: %s\n", int(micros), pctx->ToString(true).c_str());

	if s.reverse {
		if !s.iterSeekReverse(EncodeKey(s.start_key_, 0, 0)) {
			return s.results_
		}
	} else {
		if !s.iterSeek(EncodeKey(s.start_key_, 0, 0)) {
			return s.results_
		}
	}
	/*
		for ; ; s.getAndAdvance() {
		}

		if (kvs_.Count() == max_keys_ && advanceKey()) {
			if (reverse) {
				// It is possible for cur_key_ to be pointing into mvccScanner.saved_buf_
				// instead of iter_rep_'s underlying storage if iterating in reverse (see
				// iterPeekPrev), so copy the key onto the DBIterator struct to ensure it
				// has a lifetime that outlives the DBScanResults.
				iter_.rev_resume_key.assign(cur_key_.data(), cur_key_.size());
				results_.resume_key = ToDBSlice(iter_.rev_resume_key);
			} else {
				results_.resume_key = ToDBSlice(cur_key_);
			}
		}
	*/
	return DBScanResults{} //fillResults()
}
func (s *MvccScanner) iterSeek(key Slice) bool {
	s.clearPeeked()
	s.iter_rep_.Seek(key)
	return s.updateCurrent()
}

// iterSeekReverse positions the iterator at the last key that is
// less than key.
func (s *MvccScanner) iterSeekReverse(key Slice) bool {
	s.clearPeeked()

	// `SeekForPrev` positions the iterator at the last key that is less than or
	// equal to `key` AND strictly less than `ReadOptions::iterate_upper_bound`.
	s.iter_rep_.SeekForPre(key)
	if s.iter_rep_.Valid() && bytes.Compare(key, s.iter_rep_.Item().Key()) == 0 {
		s.iter_rep_.Prev()
	}
	if !s.updateCurrent() {
		return false
	}
	if s.cur_timestamp_ == kZeroTimestamp {
		// We landed on an intent or inline value.
		return true
	}

	// We landed on a versioned value, we need to back up to find the
	// latest version.
	return false //s.backwardLatestVersion(s.cur_key_, 0)
}

/*
// iterPeekPrev "peeks" at the previous key before the current
// iterator position.
func (s *MvccScanner) iterPeekPrev(peeked_key Slice) bool {
	if (!s.peeked_) {
		s.peeked_ = true
		// We need to save a copy of the current iterator key and value
		// and adjust cur_raw_key_, cur_key and cur_value to point to
		// this saved data. We use a single buffer for this purpose:
		// saved_buf_.
		s.saved_buf_ = make([]byte, 0, len(s.cur_raw_key_)+len(s.cur_value_))
		s.saved_buf_ = Append(s.saved_buf_, s.cur_raw_key_)
		s.saved_buf_ = Append(s.saved_buf_, s.cur_value_)
		s.cur_raw_key_ = s.saved_buf_[0:len(s.cur_raw_key_)]
		s.cur_value_ = s.saved_buf_[len(s.cur_raw_key_):]
		var dummy_timestamp DBTimestamp
		if (!SplitKey(s.cur_raw_key_, &s.cur_key_, &dummy_timestamp)) {
			return s.setStatus("failed to split mvcc key")
		}

		// With the current iterator state saved we can move the
		// iterator to the previous entry.
		s.iter_rep_.Prev()
		if (!s.iter_rep_.Valid()) {
			// Peeking at the previous key should never leave the iterator
			// invalid. Instead, we seek back to the first key and set the
			// peeked_key to the empty key. Note that this prevents using
			// reverse scan to scan to the empty key.
			s.peeked_ = false
			*peeked_key = rocksdb::Slice();
			s.iter_rep_.SeekToFirst()
			return s.updateCurrent()
		}
	}

rocksdb::Slice
	dummy_timestamp;
	if (!SplitKey(iter_rep_- > key(), peeked_key, &dummy_timestamp)) {
		return s.setStatus("failed to split mvcc key")
	}
	return true
}

// backwardLatestVersion backs up the iterator to the latest version
// for the specified key. The parameter i is used to maintain the
// iteration count between the loop here and the caller (usually
// prevKey). Returns false if an error occurred.
func (s *MvccScanner) backwardLatestVersion(key Slice, i int) bool {
	s.key_buf_ = string(key)

	for ; i < s.iters_before_seek_; i++ {
		var peeked_key Slice
		if (!s.iterPeekPrev(peeked_key)) {
			return false
		}
		if (peeked_key != s.key_buf_) {
			// The key changed which means the current key is the latest
			// version.
			iters_before_seek_ = std::max < int > (kMaxItersBeforeSeek, iters_before_seek_ + 1);
			return true;
		}
		if (!s.iterPrev()) {
			return false;
		}
	}

	s.iters_before_seek_ = std::max < int > (1, s.iters_before_seek_ - 1);
	s.key_buf_ = s.key_buf_ + "\0"
	return s.iterSeek([]byte(s.key_buf_))
}
*/

func (s *MvccScanner) updateCurrent() bool {
	if !s.iter_rep_.Valid() {
		return false
	}
	item := s.iter_rep_.item
	s.cur_raw_key_ = item.Key()
	s.cur_value_, _ = item.SafeValue()
	s.cur_timestamp_ = kZeroTimestamp
	var success bool
	success = DecodeKey(s.cur_raw_key_, &s.cur_key_, &s.cur_timestamp_)
	if !success {
		return s.setStatus("failed to split mvcc key")
	}
	return true
}

func (s *MvccScanner) setStatus(status string) bool {
	s.results_.status = DBStatus(status)
	return false
}

func (s *MvccScanner) clearPeeked() {
	if s.reverse {
		s.peeked_ = false
	}
}

func Append(dest []byte, src []byte) []byte {
	for i := range src {
		dest = append(dest, src[i])
	}
	return dest
}
func EncodeTimestamp(s *[]byte, wall_time int64, logical int32) {
	binary.BigEndian.PutUint64(*s, uint64(wall_time))
	binary.BigEndian.PutUint32((*s)[8:12], uint32(logical))
}

//组成：(key)+[可选:0+wall_time+logical]+(ts的长度)
func EncodeKey(key []byte, wall_time int64, logical int32) []byte {
	ts := wall_time != 0 || logical != 0
	klen := len(key) + 1
	if ts {
		klen = klen + 1 + kMVCCVersionTimestampSize
	}

	out := make([]byte, 0, klen)
	out = Append(out, key)
	if ts {
		out = append(out, byte(0))
		ts_b := out[len(key)+1 : len(key)+1+kMVCCVersionTimestampSize]
		EncodeTimestamp(&ts_b, wall_time, logical)
		out = out[:len(out)+len(ts_b)]
	}
	out = append(out, byte(len(out)-len(key)))
	return out
}
func SplitKey(buf Slice, key *Slice, timestamp *Slice) bool {
	if len(buf) < 1 {
		return false
	}
	ts_size := int(buf[len(buf)-1])
	if ts_size >= len(buf) {
		return false
	}
	*key = buf[:len(buf)-ts_size-1]
	*timestamp = buf[len(buf)-ts_size : len(buf)-1]
	return true
}

func DecodeKey(buf []byte, key *Slice, ts *DBTimestamp) bool {
	var timestamp Slice
	if !SplitKey(buf, key, &timestamp) {
		return false
	}
	*ts = DBTimestamp{
		wall_time: int64(binary.BigEndian.Uint64(timestamp[:len(timestamp)-4])),
		logical:   int32(binary.BigEndian.Uint32(timestamp[len(timestamp)-4:])),
	}
	return true
}
