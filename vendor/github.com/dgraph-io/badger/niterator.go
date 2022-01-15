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
	"github.com/dgraph-io/badger/y"
	"math"
	"sync/atomic"
)

var KeyMin = []byte{0}
var KeyMax = []byte{0xff, 0xff}

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type NIterator struct {
	iitr   *y.MergeIterator
	txn    *Txnone
	readTs uint64

	opt  IteratorOptions
	item *Item
	//data  list
	//waste list

	lastKey []byte // Used to skip over multiple versions of the same key.

	/*反向遍历时从先搜索到旧版本值，因此需要Prev后预取下一个值，并比较是否同一个Key
	  然后在客户端执行Prev()方法时判断是否已经预取过key值，并返回该key
	*/
	prefetchKey []byte

	closed bool
}

func (it *NIterator) newItem() *Item {
	item := &Item{slice: new(y.Slice), db: it.txn.db, txn: nil}
	return item
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *NIterator) Item() *Item {
	tx := it.txn
	tx.addReadKey(it.item.Key())
	return it.item
}

// Valid returns false when iteration is done.
func (it *NIterator) Valid() bool {
	if it.item == nil {
		return false
	}
	return bytes.HasPrefix(it.item.key, it.opt.Prefix)
}

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *NIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && bytes.HasPrefix(it.item.key, prefix)
}

// Close would close the iterator. It is important to call this when you're done with iteration.
func (it *NIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true

	it.iitr.Close()
	// It is important to wait for the fill goroutines to finish. Otherwise, we might leave zombie
	// goroutines behind, which are waiting to acquire file read locks after DB has been closed.
	/*waitFor := func(l list) {
		item := l.pop()
		for item != nil {
			item.wg.Wait()
			item = l.pop()
		}
	}
	waitFor(it.waste)
	waitFor(it.data)
	*/
	if it.item != nil {
		it.item.wg.Wait()
	}

	// TODO: We could handle this error.
	_ = it.txn.db.vlog.decrIteratorCount()
	atomic.AddInt32(&it.txn.numIterators, -1)
}
func (it *NIterator) Next() {
	it.item.wg.Wait()
	it.item = nil

	it.iitr.Next()
	if it.iitr.Valid() && it.prefetchKey != nil {
		/*假设当前客户端看到的位置=8，如果前一个操作是Prev()，则可能iitr的实际位置=7
		  客户端执行next()期望的位置=9，所以需要再执行一次next()
		*/
		it.iitr.Next()
	}
	for it.iitr.Valid() {
		if it.parseItem(false, func(s *y.MergeIterator) { s.Next() }) {
			break
		}
	}
}
func (it *NIterator) Prev() {
	it.item.wg.Wait()
	it.item = nil

	if it.prefetchKey == nil {
		it.iitr.Prev()
	}
	for it.iitr.Valid() {
		if it.parseItem(true, func(s *y.MergeIterator) { s.Prev() }) {
			break
		}
	}
}

// This function advances the iterator.
func (it *NIterator) parseItem(reversed bool, itmove func(s *y.MergeIterator)) bool {
	it.prefetchKey = nil
	mi := it.iitr
	key := mi.Key()

	setItem := func(item *Item) {
		it.item = item
	}

	// Skip badger keys.
	if !it.opt.internalAccess && bytes.HasPrefix(key, badgerPrefix) {
		itmove(mi)
		return false
	}

	// Skip any versions which are beyond the readTs.
	version := y.ParseTs(key)
	if version > it.readTs {
		itmove(mi)
		return false
	}

	if it.opt.AllVersions {
		// Return deleted or expired values also, otherwise user can't figure out
		// whether the key was deleted.
		item := it.newItem()
		it.fill(item)
		setItem(item)
		return true
	}

	// If iterating in forward direction, then just checking the last key against current key would
	// be sufficient.
	if !reversed {
		if y.SameKey(it.lastKey, key) {
			itmove(mi)
			return false
		}
		// Only track in forward direction.
		// We should update lastKey as soon as we find a different key in our snapshot.
		// Consider keys: a 5, b 7 (del), b 5. When iterating, lastKey = a.
		// Then we see b 7, which is deleted. If we don't store lastKey = b, we'll then return b 5,
		// which is wrong. Therefore, update lastKey here.
		it.lastKey = y.SafeCopy(it.lastKey, mi.Key())
	}

FILL:
	// If deleted, advance and return.
	vs := mi.Value()
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		itmove(mi)
		return false
	}

	item := it.newItem()
	it.fill(item)

	if !reversed { // Forward direction, or invalid.
		setItem(item)
		return true
	}

	mi.Prev() // Advance but no fill item yet.
	if !mi.Valid() {
		setItem(item)
		return true
	}
	it.prefetchKey = mi.Key() //记录预取的Key

	// Reverse direction.
	nextTs := y.ParseTs(mi.Key())
	mik := y.ParseKey(mi.Key())
	if nextTs <= it.readTs && bytes.Equal(mik, item.key) {
		// This is a valid potential candidate.
		goto FILL
	}

	setItem(item)
	return true
}

func (it *NIterator) fill(item *Item) {
	vs := it.iitr.Value()
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.expiresAt = vs.ExpiresAt

	item.version = y.ParseTs(it.iitr.Key())
	item.key = y.SafeCopy(item.key, y.ParseKey(it.iitr.Key()))

	item.vptr = y.SafeCopy(item.vptr, vs.Value)
	item.val = nil
	if it.opt.PrefetchValues {
		item.wg.Add(1)
		go func() {
			// FIXME we are not handling errors here.
			item.prefetchValue()
			item.wg.Done()
		}()
	}
}

// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// greater than the provided key if iterating in the forward direction. Behavior would be reversed if
// iterating backwards.
func (it *NIterator) Seek(key []byte) {
	if it.item != nil {
		it.item.wg.Wait()
		it.item = nil
	}
	if len(key) == 0 {
		key = it.opt.Prefix
	}
	key = y.KeyWithTs(key, it.readTs)

	it.lastKey = it.lastKey[:0]
	it.iitr.Seek(key)

	it.itemForSeek(false)
}
func (it *NIterator) Get(key []byte) (item *Item, rerr error) {
	it.Seek(key)
	if it.Valid() {
		item := it.Item()
		if bytes.Compare(key, item.key) == 0 {
			return item, nil
		} else {
			return nil, nil
		}
	} else {
		return nil, nil
	}
}
func (it *NIterator) SeekForPre(key []byte) {
	if it.item != nil {
		it.item.wg.Wait()
		it.item = nil
	}
	if len(key) == 0 {
		key = it.opt.Prefix
	}
	/*ts=MAX 在底层的skl和block中是从小->大排序，搜索也是需要从前向后搜索
	ts=MAX后构建的y.KeyWithTs构建的值后面全是0，那么搜索时就可以定位到指定key的前一个
	如写入顺序(括号后是ts)：a(1)、b(1)、c(1)、b(2)，则经过KeyWithTs实际存储为（假设MAX=10）：
	a9(1)、b8(2)、b8(1)、c9(1)
	这样在ts=3时查找b值，实际key=b7(3)，在正向遍历时就先搜索到了b8(2)，也就是最新update的值。
	而反向遍历时，底层也是从小->大搜索，因此我们搜索值应该设置为b0(MAX)，这样就会定位到a9(1)这个值
	*/
	key = y.KeyWithTs(key, math.MaxUint64)

	it.lastKey = it.lastKey[:0]
	it.iitr.SeekForPre(key)

	it.itemForSeek(true)
}

func (it *NIterator) itemForSeek(reversed bool) {
	for it.iitr.Valid() {
		if it.parseItem(reversed, func(s *y.MergeIterator) { s.Next() }) {
			break
		}
	}
}

// Rewind would rewind the iterator cursor all the way to zero-th position, which would be the
// smallest key if iterating forward, and largest if iterating backward. It does not keep track of
// whether the cursor started with a Seek().
func (it *NIterator) Rewind() {
	it.SeekToFirst()
}
func (it *NIterator) SeekToFirst() {
	if it.opt.Prefix != nil {
		it.Seek(it.opt.Prefix)
	} else {
		it.iitr.SeekToFirst()
		it.itemForSeek(false)
	}
}
func (it *NIterator) SeekToLast() {
	it.iitr.SeekToLast()
	it.itemForSeek(true)
}
