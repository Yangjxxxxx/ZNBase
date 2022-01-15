package snapshot

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/tests"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// Data is a struct
type Data struct {
	key   []byte
	value string
	del   bool
}

// DEBUG is a bool var
const DEBUG = true

var tableKey = roachpb.Key(keys.MakeTablePrefix(52)) // 第一个用户表的key
func setupData(
	t *testing.T, initData bool,
) (serverutils.TestServerInterface, *gosql.DB, *client.DB, []SnapItems) {
	params, _ := tests.CreateTestServerParams()
	if knobs := params.Knobs.Store; knobs != nil {
		if mo := knobs.(*storage.StoreTestingKnobs); mo != nil {
			mo.DisableOptional1PC = true
		}
	}
	s, sqlDB, kdb := serverutils.StartServer(t, params)
	if !initData {
		return s, sqlDB, kdb, nil
	}
	requireTxn := true

	var tableData []SnapItems

	t1s := time.Duration(1e9) // 1秒
	// 时间点1
	exec(t, sqlDB, "CREATE TABLE item (id int primary key,name string);", requireTxn)
	exec(t, sqlDB, "insert into item values(1,'tv'),(2,'tel')", requireTxn)
	// 快照1
	snap1 := s.Clock().Now().WallTime
	fmt.Printf("snap1 Time : %d", snap1)
	exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT item_snap1 ON TABLE item AS OF SYSTEM TIME %d", snap1), requireTxn)
	//快照1数据
	snap1Items := []item{
		{1, "tv"},
		{2, "tel"},
	}
	tableData = append(tableData, SnapItems{name: "item_snap1", ts: snap1, data: snap1Items})

	// 时间点2 - 1秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into item values(101,'book'),(102,'iphone')", requireTxn)
	exec(t, sqlDB, "update item set name='tv-new' where id=1", requireTxn)
	// 建快照2
	snap2 := s.Clock().Now().WallTime
	fmt.Printf("snap2 Time : %d", snap2)
	exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT item_snap2 ON TABLE item AS OF SYSTEM TIME %d", snap2), requireTxn)
	//快照2数据
	snap2Items := []item{
		{1, "tv-new"},
		{2, "tel"},
		{101, "book"},
		{102, "iphone"},
	}
	tableData = append(tableData, SnapItems{name: "item_snap2", ts: snap2, data: snap2Items})

	// 时间点3 - 2秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into item values(201,'computer'),(202,'cpu')", requireTxn)
	exec(t, sqlDB, "update item set name='tel-new' where id=2", requireTxn)
	exec(t, sqlDB, "delete from item where id=102", requireTxn)
	// 快照3
	snap3 := s.Clock().Now().WallTime
	fmt.Printf("snap3 Time : %d", snap3)
	exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT item_snap3 ON TABLE item AS OF SYSTEM TIME %d", snap3), requireTxn)
	snap3Items := []item{
		{1, "tv-new"},
		{2, "tel-new"},
		{101, "book"},
		{201, "computer"},
		{202, "cpu"},
	}
	tableData = append(tableData, SnapItems{name: "item_snap3", ts: snap3, data: snap3Items})

	// 时间点4 - 3秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into item values(301,'notebook'),(302,'paper')", requireTxn)
	exec(t, sqlDB, "update item set name='book-new' where id=101", requireTxn)
	exec(t, sqlDB, "delete from item where id=202", requireTxn)

	//PrintAllStores(t, s.GetStores().(*storage.Stores), snap1)

	//验证当前数据
	items := []item{
		{1, "tv-new"},
		{2, "tel-new"},
		{101, "book-new"},
		{201, "computer"},
		{301, "notebook"},
		{302, "paper"},
	}
	tableData = append(tableData, SnapItems{ts: 0, data: items})
	if err := checkResults(t, sqlDB, "select * from item", items); err != nil {
		t.Fatal(err)
	}

	if err := checkResults(t, sqlDB, "select * from item AS of SNAPSHOT item_snap1", snap1Items); err != nil {
		t.Fatal(err)
	}

	//验证快照2数据
	if err := checkResults(t, sqlDB, "select * from item AS of SNAPSHOT item_snap2", snap2Items); err != nil {
		t.Fatal(err)
	}

	//验证快照3数据
	if err := checkResults(t, sqlDB, "select * from item AS of SNAPSHOT item_snap3", snap3Items); err != nil {
		t.Fatal(err)
	}
	return s, sqlDB, kdb, tableData
}
func txnBatch(ctx context.Context, t *testing.T, s serverutils.TestServerInterface, data []Data) {
	db := s.DB()
	err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		for _, d := range data {
			if d.del {
				b.Del(d.key)
			} else {
				b.Put(d.key, d.value)
			}
		}
		_ = txn.Run(ctx, b)
		//t.Logf("×××××××事务提交前数据×××××")
		//PrintAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		return txn.Commit(ctx)
		//return txn.CommitInBatch(ctx, b)
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
}
func txnRevert(ctx context.Context, t *testing.T, s serverutils.TestServerInterface, snapTs int64) {
	db := s.DB()
	err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		b.RevertRange(tableKey, tableKey.PrefixEnd(), snapTs, false, nil)
		_ = txn.Run(ctx, b)
		return txn.Commit(ctx)
		//return txn.CommitInBatch(ctx, b)
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func checkKvs(
	t *testing.T, s serverutils.TestServerInterface, tableKey roachpb.Key, expKVs []Data, maxTs int64,
) error {
	stores := s.GetStores().(*storage.Stores)
	return stores.VisitStores(func(s *storage.Store) error {
		res, err := engine.MVCCScan(
			context.Background(), s.Engine(), tableKey, tableKey.PrefixEnd(), 1000, hlc.Timestamp{WallTime: maxTs}, engine.MVCCScanOptions{Txn: nil})
		if err != nil {
			return errors.Errorf("MVCCScan error: %v", err)
		}
		kvs := res.KVs
		if len(kvs) != len(expKVs) {
			return errors.Errorf("expected length %d; got %d", len(expKVs), len(kvs))
		}
		for i, kv := range kvs {
			if !kv.Key.Equal(expKVs[i].key) {
				return errors.Errorf("%d: expected key %q; got %q", i, expKVs[i].key, kv.Key)
			}
			if !bytes.Equal(kv.Value.RawBytes[5:], []byte(expKVs[i].value)) {
				return errors.Errorf("%d: expected value=%s; got %s", i, expKVs[i].value, kv.Value.RawBytes[5:])
			}
			if log.V(1) {
				log.Infof(context.Background(), "%d: %s", i, kv.Key)
			}
		}
		return nil
	})
}

// SnapItems is a struct
type SnapItems struct {
	name string
	ts   int64
	data []item
}

type item struct {
	id   int
	name string
}

func checkResults(t *testing.T, sqlDB *gosql.DB, sql string, expected []item) error {
	items, err := queryItems(sqlDB, sql)
	if err != nil {
		return err
	}

	if len(items) != len(expected) {
		return errors.Errorf("expected row count: %d, but got: %d", len(expected), len(items))
	}
	for i := range expected {
		if items[i] != expected[i] {
			return errors.Errorf("%d: expected result: %v, but got: %v", i, expected[i], items[i])
		}
	}
	return nil
}
func queryItems(sqlDB *gosql.DB, sql string) ([]item, error) {
	rows, err := sqlDB.Query(sql)
	if err != nil {
		return nil, err
	}
	var items []item
	for rows.Next() {
		r := item{}
		_ = rows.Scan(&r.id, &r.name)
		items = append(items, r)
	}
	return items, err
}

func exec(t *testing.T, sqlDb *gosql.DB, sql string, requireTxn bool) {
	if requireTxn {
		// 如果在事务内
		txn, err := sqlDb.Begin()
		if err != nil {
			t.Fatal(err)
		}
		_, _ = txn.Exec(sql)
		_ = txn.Commit()
	} else {
		_, err := sqlDb.Exec(sql)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// PrintAllStores is that print all stores
func PrintAllStores(t *testing.T, stores *storage.Stores, prefix *roachpb.Key, snap int64) {
	_ = stores.VisitStores(func(s *storage.Store) error {
		return PrintStoreKV(s, prefix, snap)
	})

	/*s, err := stores.GetStore(roachpb.StoreID(0))
	if err != nil {
		t.Fatal(err)
	}
	*/
}

// PrintStoreKV is that print store kv.
func PrintStoreKV(s *storage.Store, prefix *roachpb.Key, snap int64) error {
	//fmt.Printf("store Count : %d", stores.GetStoreCount())
	fmt.Printf("Store : %s\n", s.String())
	opts := engine.IterOptions{
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	}
	it := s.Engine().NewIterator(opts)
	defer it.Close()
	it.Seek(engine.MakeMVCCMetadataKey(keys.MinKey))
	//tablek := roachpb.Key(keys.MakeTablePrefix(52))
	snapTs := hlc.Timestamp{WallTime: snap}
	preKeys := roachpb.Key{}
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}
		k := it.Key()
		value := it.UnsafeValue()
		if prefix != nil && !bytes.HasPrefix(k.Key, *prefix) { //事务记录
			continue
		}
		if bytes.Equal(k.Key, preKeys) {
			continue // 同一Key，输出第一个
		}
		preKeys = k.Key

		if !k.Timestamp.Less(snapTs) {
			continue
		}
		msg := fmt.Sprintf("KEY %v", k)
		if len(value) == 0 {
			msg = fmt.Sprintf("%s,deleted", msg)
		}
		if !k.IsValue() {
			meta := enginepb.MVCCMetadata{}
			if err := it.ValueProto(&meta); err != nil {
				fmt.Printf("parse meta ERROR :%s\n", err)
			}
			if meta.ValBytes == 0 {
				msg = fmt.Sprintf("%s,deleted", msg)
			}

			if meta.Txn != nil {
				msg = fmt.Sprintf("%s, txn=%s", msg, meta.Txn.String())
			}
		}
		fmt.Println(msg)
		_ = value
	}
	return nil
}
