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
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTxnoneSimple(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {

		txn := db.NewNoneTransaction()

		for i := 0; i < 10; i++ {
			k := []byte(fmt.Sprintf("key=%d", i))
			v := []byte(fmt.Sprintf("val=%d", i))
			txn.Set(k, v)
		}
		//time.Sleep(time.Second * 10)

		item, err := txn.Get([]byte("key=8"))
		require.NoError(t, err)

		require.NoError(t, item.Value(func(val []byte) error {
			require.Equal(t, []byte("val=8"), val)
			return nil
		}))
	})
}

func TestUpdate(t *testing.T) {
	dbPath := "/data/test"
	runBadgerTestWithDB(t, nil, dbPath, false, func(t *testing.T, db *DB) {
		txn := db.NewNoneTransaction()
		for i := 0; i < 30; i++ {
			k := bkey(i)
			v := bvalue(i)
			require.NoError(t, txn.Set(k, v))
		}
	})

	val_1 := []byte("ok1")
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		txn := db.NewNoneTransaction()
		for i := 0; i < 30; i++ {
			require.NoError(t, txn.Set(bkey(i), val_1))
		}

		//验证修改是否成功
		opt := DefaultIteratorOptions
		itr := txn.NewIterator(opt)
		var count int
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			err := item.Value(func(v []byte) error {
				t.Logf("key=%s,value=%s", item.Key(), v)
				require.Equal(t, val_1, v)
				return nil
			})
			require.NoError(t, err)
			count++
		}
		require.Equal(t, 30, count)

		//Seek
		itr.SeekForPre(bkey(30))
		testCurrItem(t, itr, 29, val_1)

		itr.Prev()
		testCurrItem(t, itr, 28, val_1)

		//SeekPrev
		itr.SeekForPre(bkey(20))
		testCurrItem(t, itr, 19, val_1)

		//反向遍历检查
		count = 0
		for itr.SeekToLast(); itr.Valid(); itr.Prev() {
			item := itr.Item()
			err := item.Value(func(v []byte) error {
				t.Logf("key=%s,value=%s", item.Key(), v)
				require.Equal(t, val_1, v)
				return nil
			})
			require.NoError(t, err)
			count++
		}

		//Seek
		itr.Seek(bkey(10))
		testCurrItem(t, itr, 10, val_1)
		//next
		itr.Next()
		testCurrItem(t, itr, 11, val_1)

		//SeekPrev
		itr.SeekForPre(bkey(20))
		testCurrItem(t, itr, 19, val_1)
		//prev
		itr.Prev()
		testCurrItem(t, itr, 18, val_1)

	})
	//关闭修改，再次打开
	runBadgerTestWithDB(t, nil, dbPath, true, func(t *testing.T, db *DB) {
		txn := db.NewNoneTransaction()
		//验证修改是否成功
		opt := DefaultIteratorOptions
		itr := txn.NewIterator(opt)
		var count int
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			err := item.Value(func(v []byte) error {
				t.Logf("key=%s,value=%s", item.Key(), v)
				require.Equal(t, val_1, v)
				return nil
			})
			require.NoError(t, err)
			count++
		}
		require.Equal(t, 30, count)

		//反向遍历检查
		count = 0
		for itr.SeekToLast(); itr.Valid(); itr.Prev() {
			item := itr.Item()
			err := item.Value(func(v []byte) error {
				t.Logf("key=%s,value=%s", item.Key(), v)
				require.Equal(t, val_1, v)
				return nil
			})
			require.NoError(t, err)
			count++
		}

		//Seek
		itr.Seek(bkey(10))
		testCurrItem(t, itr, 10, val_1)
		//next
		itr.Next()
		testCurrItem(t, itr, 11, val_1)

		//SeekPrev
		itr.Seek(bkey(20))
		testCurrItem(t, itr, 20, val_1)
		//prev
		itr.Prev()
		itr.Prev()
		testCurrItem(t, itr, 18, val_1)

	})

}
func TestUnsafeValue(t *testing.T) {
	runBadgerTestWithDB(t, nil, "/data/test", false, func(t *testing.T, db *DB) {

		txn := db.NewNoneTransaction()

		for i := 0; i < 10; i++ {
			k := []byte(fmt.Sprintf("key=%d", i))
			v := []byte(fmt.Sprintf("val=%d", i))
			txn.Set(k, v)
		}
		//time.Sleep(time.Second * 10)

		item, err := txn.Get([]byte("key=8"))
		require.NoError(t, err)

		require.Equal(t, []byte("val=8"), item.UnsafeValue())
	})
	runBadgerTestWithDB(t, nil, "/data/test", true, func(t *testing.T, db *DB) {

		txn := db.NewNoneTransaction()

		for i := 0; i < 10; i++ {
			k := []byte(fmt.Sprintf("key=%d", i))
			v := []byte(fmt.Sprintf("val=%d", i))
			item, err := txn.Get(k)

			require.NoError(t, err)

			require.Equal(t, v, item.UnsafeValue())
		}
	})
}
