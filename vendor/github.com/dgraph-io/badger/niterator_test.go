/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"math/rand"
)
import clist "container/list"

var bkey = func(i int) []byte {
	return []byte(fmt.Sprintf("%04d", i))
}
var VAL = "OK"
var bvalue = func(i int) []byte {
	return []byte(fmt.Sprintf("%04d%s", i, VAL))
}
var keyvalue = func(key []byte) []byte {
	return []byte(fmt.Sprintf("%s%s", key, VAL))
}

func prepareData(t *testing.T, opts *Options) string {
	dbPath := "/data/test"
	//产生100个数据，其中0-29顺序写入然后刷盘.
	runBadgerTestWithDB(t, nil, dbPath, false, func(t *testing.T, db *DB) {
		txn := db.NewNoneTransaction()
		for i := 0; i < 30; i++ {
			k := bkey(i)
			v := bvalue(i)
			require.NoError(t, txn.Set(k, v))
		}
	})
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		txn := db.NewNoneTransaction()
		//30-49无序写入后刷盘
		l := clist.New()
		for i := 30; i < 50; i++ {
			if rand.Int31n(100) > 50 {
				l.PushBack(i)
			} else {
				l.PushFront(i)
			}
		}
		for e := l.Front(); e != nil; e = e.Next() {
			k := bkey(e.Value.(int))
			v := bvalue(e.Value.(int))
			require.NoError(t, txn.Set(k, v))
		}
	})
	return dbPath
}
func prepareDataWithDB(t *testing.T, db *DB) {
	//在mem中无序写入50-100
	txn := db.NewNoneTransaction()
	l := clist.New()
	for i := 50; i < 100; i++ {
		if rand.Int31n(100) > 50 {
			l.PushBack(i)
		} else {
			l.PushFront(i)
		}
	}
	for e := l.Front(); e != nil; e = e.Next() {
		require.NoError(t, txn.Set(bkey(e.Value.(int)), bvalue(e.Value.(int))))
	}
}
func testIterateNext(t *testing.T, db *DB) int {
	txn := db.NewNoneTransaction()

	opt := DefaultIteratorOptions
	itr := txn.NewIterator(opt)
	defer itr.Close()
	var minKey []byte
	var count int
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		item := itr.Item()
		t.Logf("key=%s", item.Key())
		if minKey != nil {
			require.True(t, bytes.Compare(minKey, item.Key()) == -1)
		}
		minKey = item.Key()
		count++
	}
	return count
}
func testIteratePrev(t *testing.T, db *DB, show bool) int {
	txn := db.NewNoneTransaction()

	opt := DefaultIteratorOptions
	itr := txn.NewIterator(opt)
	defer itr.Close()
	var maxKey []byte
	var count int
	for itr.SeekToLast(); itr.Valid(); itr.Prev() {
		item := itr.Item()
		if show {
			t.Logf("key=%s", item.Key())
		}
		if maxKey != nil {
			require.True(t, bytes.Compare(maxKey, item.Key()) >= 1)
		}
		maxKey = item.Key()
		count++
	}
	return count
}
func testCurrItem(t *testing.T, itr *NIterator, ki int, value []byte) {
	require.True(t, itr.Valid())
	item := itr.Item()
	require.Equal(t, string(bkey(ki)), string(item.Key()))
	err := item.Value(func(v []byte) error {
		require.Equal(t, string(value), string(v))
		return nil
	})
	require.NoError(t, err)
}
func TestMemIterate(t *testing.T) {
	dbPath := "/data/test"
	runBadgerTestWithDB(t, nil, dbPath, false, func(t *testing.T, db *DB) {
		txn := db.NewNoneTransaction()
		for i := 0; i < 30; i++ {
			k := bkey(i)
			v := bvalue(i)
			require.NoError(t, txn.Set(k, v))
		}
		testIterateNext(t, db)

		opt := DefaultIteratorOptions
		itr := txn.NewIterator(opt)
		defer itr.Close()
		itr.SeekForPre(bkey(10))
		testCurrItem(t, itr, 9, bvalue(9))
	})
	//重新打开数据库，遍历Level文件
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		testIterateNext(t, db)
	})
}
func TestNIterate(t *testing.T) {
	dbPath := prepareData(t, nil)
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		require.Equal(t, 50, testIterateNext(t, db))
		prepareDataWithDB(t, db)

		require.Equal(t, 100, testIterateNext(t, db))
	})
}
func TestNIterateBackward(t *testing.T) {
	dbPath := prepareData(t, nil)
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		//require.Equal(t, 50, testIteratePrev(t, db, false))
		prepareDataWithDB(t, db)
		require.Equal(t, 100, testIteratePrev(t, db, true))
	})
}
func TestNIterateMix(t *testing.T) {
	dbPath := prepareData(t, nil)
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		prepareDataWithDB(t, db)

		txn := db.NewNoneTransaction()
		opt := DefaultIteratorOptions
		itr := txn.NewIterator(opt)
		defer itr.Close()
		var curKey []byte
		var count int
		for itr.SeekToFirst(); itr.Valid(); itr.Next() {
			item := itr.Item()
			if curKey != nil {
				require.True(t, bytes.Compare(curKey, item.Key()) < 0)
			}
			curKey = item.Key()
			count++
		}
		require.Equal(t, 100, count)
		curKey = nil
		for itr.SeekToLast(); itr.Valid(); itr.Prev() {
			item := itr.Item()
			t.Logf("key=%s", item.Key())
			if curKey != nil {
				require.True(t, bytes.Compare(curKey, item.Key()) >= 1)
			}
			curKey = item.Key()
			count++
		}
		require.Equal(t, 200, count)
	})
}
func TestSeek(t *testing.T) {
	dbPath := prepareData(t, nil)
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		prepareDataWithDB(t, db)

		txn := db.NewNoneTransaction()
		opt := DefaultIteratorOptions
		itr := txn.NewIterator(opt)
		defer itr.Close()
		key := bkey(10)
		itr.Seek(key)
		testCurrItem(t, itr, 10, bvalue(10))

		//next
		itr.Next()
		testCurrItem(t, itr, 11, bvalue(11))

		//prev
		itr.Prev()
		testCurrItem(t, itr, 10, bvalue(10))

		//2个it的接缝处-next
		itr.Seek(bkey(29))
		testCurrItem(t, itr, 29, bvalue(29))
		itr.Next()
		testCurrItem(t, itr, 30, bvalue(30))

		//2个it的接缝处-prev
		itr.Seek(bkey(30))
		testCurrItem(t, itr, 30, bvalue(30))
		itr.Prev()
		testCurrItem(t, itr, 29, bvalue(29))
	})
}
func TestSeekPrev(t *testing.T) {
	dbPath := prepareData(t, nil)
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		prepareDataWithDB(t, db)

		txn := db.NewNoneTransaction()
		opt := DefaultIteratorOptions
		itr := txn.NewIterator(opt)
		defer itr.Close()
		key := bkey(10)
		itr.SeekForPre(key)
		testCurrItem(t, itr, 9, bvalue(9))

		//next
		itr.Next()
		testCurrItem(t, itr, 10, bvalue(10))

		//prev
		itr.Prev()
		testCurrItem(t, itr, 9, bvalue(9))

		//2个it的接缝处-next
		itr.SeekForPre(bkey(30))
		testCurrItem(t, itr, 29, bvalue(29))
		itr.Next()
		testCurrItem(t, itr, 30, bvalue(30))

		//2个it的接缝处-prev
		itr.SeekForPre(bkey(31))
		testCurrItem(t, itr, 30, bvalue(30))
		itr.Prev()
		testCurrItem(t, itr, 29, bvalue(29))
	})
}
func TestItrGet(t *testing.T) {
	dbPath := prepareData(t, nil)
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		prepareDataWithDB(t, db)

		txn := db.NewNoneTransaction()
		opt := DefaultIteratorOptions
		itr := txn.NewIterator(opt)
		defer itr.Close()
		key := bkey(10)

		item, _ := itr.Get(key)
		require.Equal(t, string(key), string(item.Key()))
		err := item.Value(func(v []byte) error {
			require.Equal(t, string(bvalue(10)), string(v))
			return nil
		})
		require.NoError(t, err)

		key = []byte("001a")
		item, _ = itr.Get(key)
		require.Nil(t, item)

	})
}

/*
func TestNIteratePrefix(t *testing.T) {
	dbPath := "/data/test"
	val := []byte("OK")
	runBadgerTestWithDB(t, nil, dbPath, false, func(t *testing.T, db *DB) {
		bkey := func(i int) []byte {
			return []byte(fmt.Sprintf("%04d", i))
		}
		n := 10000
		txn := db.NewNoneTransaction()
		//batch := db.NewWriteBatch()
		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				t.Logf("Put i=%d\n", i)
			}
			//require.NoError(t, batch.Set(bkey(i), val, 0))
			txn.Set(bkey(i), val)
		}
		//require.NoError(t, batch.Flush())
	})
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {

		countKeys := func(prefix string) int {
			t.Logf("Testing with prefix: %s", prefix)
			var count int
			opt := DefaultIteratorOptions
			opt.Prefix = []byte(prefix)
			txn := db.NewNoneTransaction()
			itr := txn.NewIterator(opt)
			defer itr.Close()
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				err := item.Value(func(v []byte) error {
					require.Equal(t, val, v)
					return nil
				})
				require.NoError(t, err)
				require.True(t, bytes.HasPrefix(item.Key(), opt.Prefix))
				count++
			}
			return count
		}

		countOneKey := func(key []byte) int {
			var count int
			txn := db.NewNoneTransaction()
			itr := txn.NewKeyIterator(key, DefaultIteratorOptions)
			defer itr.Close()
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				err := item.Value(func(v []byte) error {
					require.Equal(t, val, v)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, key, item.Key())
				count++
			}
			return count
		}

		for i := 0; i <= 9; i++ {
			fmt.Sprintf("i=%d", i)
			require.Equal(t, 1, countKeys(fmt.Sprintf("%d%d%d%d", i, i, i, i)))
			require.Equal(t, 10, countKeys(fmt.Sprintf("%d%d%d", i, i, i)))
			require.Equal(t, 100, countKeys(fmt.Sprintf("%d%d", i, i)))
			require.Equal(t, 1000, countKeys(fmt.Sprintf("%d", i)))
		}

		t.Logf("Testing each key with key iterator")
		for i := 0; i < 100; i++ {
			require.Equal(t, 1, countOneKey(bkey(i)))
		}
	})
}
*/
