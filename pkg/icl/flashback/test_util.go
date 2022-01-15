package flashback

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/tests"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/testutils/testcluster"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/workload"
	"github.com/znbasedb/znbase/pkg/workload/bank"
)

// DEBUG is a bool var
const DEBUG = true

var tableKey = roachpb.Key(keys.MakeTablePrefix(52)) // 第一个用户表的key

func startFlashbackCluster(
	t *testing.T, initData bool,
) (serverutils.TestServerInterface, *gosql.DB, *client.DB) {
	params, _ := tests.CreateTestServerParams()
	if knobs := params.Knobs.Store; knobs != nil {
		if mo := knobs.(*storage.StoreTestingKnobs); mo != nil {
			mo.DisableOptional1PC = true
		}
	}
	return serverutils.StartServer(t, params)
}

const (
	multiNode = 4
	//dumpLoadDefaultRanges = 10
	//localFoo              = "nodelocal:///foo"
)

func dumpLoadTestSetup(
	t testing.TB, clusterSize int, numAccounts int, init func(*testcluster.TestCluster),
) (
	ctx context.Context,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	tempDir string,
	cleanup func(),
) {
	return dumpLoadTestSetupWithParams(t, clusterSize, numAccounts, init, base.TestClusterArgs{})
}

func dumpLoadTestSetupWithParams(
	t testing.TB,
	clusterSize int,
	numAccounts int,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (
	ctx context.Context,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	tempDir string,
	cleanup func(),
) {
	ctx = context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	params.ServerArgs.ExternalIODir = dir
	params.ServerArgs.UseDatabase = "data"
	tc = testcluster.StartTestCluster(t, clusterSize, params)
	init(tc)

	const payloadSize = 100
	splits := 10
	if numAccounts == 0 {
		splits = 0
	}
	bankData := bank.FromConfig(numAccounts, payloadSize, splits)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)
	const insertBatchSize = 1000
	const concurrency = 4
	if _, err := workload.Setup(ctx, sqlDB.DB.(*gosql.DB), bankData, insertBatchSize, concurrency); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := workload.Split(ctx, sqlDB.DB.(*gosql.DB), bankData.Tables()[0], 1 /* concurrency */); err != nil {
		// This occasionally flakes, so ignore errors.
		t.Logf("failed to split: %+v", err)
	}

	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}

	cleanupFn := func() {
		tempDirs := []string{dir}
		for _, s := range tc.Servers {
			for _, e := range s.Engines() {
				tempDirs = append(tempDirs, e.GetAuxiliaryDir())
			}
		}
		tc.Stopper().Stop(context.TODO()) // cleans up in memory storage's auxiliary dirs
		dirCleanupFn()                    // cleans up dir, which is the nodelocal:// storage

		for _, temp := range tempDirs {
			testutils.SucceedsSoon(t, func() error {
				items, err := ioutil.ReadDir(temp)
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				for _, leftover := range items {
					return errors.Errorf("found %q remaining in %s", leftover.Name(), temp)
				}
				return nil
			})
		}
	}

	return ctx, tc, sqlDB, dir, cleanupFn
}

func flashData(
	t *testing.T, initData bool,
) (serverutils.TestServerInterface, *gosql.DB, *client.DB, []Items) {
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

	var tableData []Items

	t1s := time.Duration(2e9) // 2秒
	// 时间点1
	exec(t, sqlDB, "CREATE TABLE item (id int primary key,name string);", requireTxn)
	// 开启flashback
	exec(t, sqlDB, "ALTER TABLE item ENABLE FLASHBACK WITH TTLDAYS 2;", requireTxn)
	exec(t, sqlDB, "insert into item values(1,'tv'),(2,'tel')", requireTxn)
	// exec(t, sqlDB, "update item set name='tv-at-time1' where id=1", requireTxn)
	time1 := s.Clock().Now().WallTime
	time.Sleep(t1s)
	// 闪回点后更新的数据
	exec(t, sqlDB, "update item set name='tv-at-time1' where id=1", requireTxn)
	exec(t, sqlDB, "update item set name='tel-at-time1' where id=2", requireTxn)

	// 闪回点time1数据
	flash1Items := []item{
		{1, "tv"},
		{2, "tel"},
	}
	tableData = append(tableData, Items{name: "flashback1", ts: time1, data: flash1Items})

	// 时间点2 - 1秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into item values(101,'book'),(102,'iphone')", requireTxn)
	exec(t, sqlDB, "update item set name='tv-new' where id=1", requireTxn)
	time2 := s.Clock().Now().WallTime
	//闪回时间点后更新的数据
	exec(t, sqlDB, "update item set name='book-at-time2' where id=101", requireTxn)

	//闪回time2数据
	flash2Items := []item{
		{1, "tv-new"},
		{2, "tel-at-time1"},
		{101, "book"},
		{102, "iphone"},
	}
	tableData = append(tableData, Items{name: "flashback2", ts: time2, data: flash2Items})

	// 时间点3 - 2秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into item values(201,'computer'),(202,'cpu')", requireTxn)
	exec(t, sqlDB, "update item set name='tel-new' where id=2", requireTxn)
	exec(t, sqlDB, "delete from item where id=102", requireTxn)
	time3 := s.Clock().Now().WallTime
	exec(t, sqlDB, "update item set name='comp-at-time3' where id=201", requireTxn)
	exec(t, sqlDB, "insert into item values(110,'in-time3'),(210,'in-3')", requireTxn)
	// 快照3
	flash3Items := []item{
		{1, "tv-new"},
		{2, "tel-new"},
		{101, "book-at-time2"},
		{201, "computer"},
		{202, "cpu"},
	}
	tableData = append(tableData, Items{name: "flashback3", ts: time3, data: flash3Items})

	// 时间点4 - 3秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into item values(301,'notebook'),(302,'paper')", requireTxn)
	exec(t, sqlDB, "update item set name='book-new' where id=101", requireTxn)
	exec(t, sqlDB, "delete from item where id=202", requireTxn)

	time4 := s.Clock().Now().WallTime
	exec(t, sqlDB, "delete from item where id=110", requireTxn)
	exec(t, sqlDB, "delete from item where id=210", requireTxn)
	exec(t, sqlDB, "alter table item add test int", requireTxn)
	// exec(t, sqlDB, "drop table item cascade", requireTxn)
	// 验证当前数据
	items := []item{
		{1, "tv-new"},
		{2, "tel-new"},
		{101, "book-new"},
		{110, "in-time3"},
		{201, "comp-at-time3"},
		{210, "in-3"},
		{301, "notebook"},
		{302, "paper"},
	}
	tableData = append(tableData, Items{ts: time4, data: items})
	if err := checkResults(sqlDB, fmt.Sprintf("select * from item AS of SYSTEM TIME %d", time1), flash1Items); err != nil {
		t.Fatal(err)
	}
	return s, sqlDB, kdb, tableData
}

// Items is a struct
type Items struct {
	name string
	ts   int64
	data []item
}

type item struct {
	id   int
	name string
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

func checkResults(sqlDB *gosql.DB, sql string, expected []item) error {
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

// printAllStores is that print all stores
func printAllStores(t *testing.T, stores *storage.Stores, prefix *roachpb.Key, snap int64) {
	_ = stores.VisitStores(func(s *storage.Store) error {
		return printStoreKV(s, prefix, snap)
	})

	/*s, err := stores.GetStore(roachpb.StoreID(0))
	if err != nil {
		t.Fatal(err)
	}
	*/
}

// printStoreKV is that print store kv.
func printStoreKV(s *storage.Store, prefix *roachpb.Key, snap int64) error {
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
