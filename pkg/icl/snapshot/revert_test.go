package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/tests"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// SQL层测试revert
func TestRevertSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, tableData := setupData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false

	for _, d := range tableData {
		if d.ts < 1 {
			continue
		}
		//恢复快照1
		if DEBUG {
			t.Logf("*********************快照[%s] 恢复前 KV*********************", d.name)
			PrintAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		}
		exec(t, sqlDB, fmt.Sprintf("REVERT SNAPSHOT %s FROM TABLE item", d.name), requireTxn)
		if DEBUG {
			t.Logf("*********************快照[%s] 恢复后 KV*********************", d.name)
			PrintAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		}

		if err := checkResults(t, sqlDB, "select * from item", d.data); err != nil {
			t.Fatal(err)
		}
	}
}

// 在多个Range分片上，SQL层测试revert
func TestRevertOnMultiRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, tableData := setupData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false

	//拆分Range之后再REVERT，验证多个RANGE 的revert
	exec(t, sqlDB, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false;", false)
	exec(t, sqlDB, "ALTER TABLE item SPLIT AT VALUES (100),(200),(300),(600);", false)
	// 查看RANGE SPLIT是否生效
	rows, err := sqlDB.Query("SHOW EXPERIMENTAL_RANGES FROM TABLE item;")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	if count != 5 {
		t.Fatalf("expected range count: %d, but got: %d", 5, count)
	}

	for _, d := range tableData {
		if d.ts < 1 {
			continue
		}
		//if i != 1 {
		//	//continue
		//}
		//恢复快照1
		if DEBUG {
			fmt.Printf("*********************快照[%s] 恢复前 KV*********************\n", d.name)
			PrintAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		}
		exec(t, sqlDB, fmt.Sprintf("REVERT SNAPSHOT %s FROM TABLE item", d.name), requireTxn)
		if DEBUG {
			fmt.Printf("*********************快照[%s] 恢复后 KV*********************\n", d.name)
			PrintAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		}

		if err := checkResults(t, sqlDB, "select * from item", d.data); err != nil {
			t.Fatal(err)
		}
	}
}

// 库级快照恢复测试
func TestRevertDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, _ := setupData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := true
	// 创建快照
	exec(t, sqlDB, "create database demodb", requireTxn)
	exec(t, sqlDB, "create table demodb.test1(id int,name string)", requireTxn)
	exec(t, sqlDB, "insert into demodb.test1 values(1,'a')", requireTxn)

	exec(t, sqlDB, "create table demodb.test2(id int,name string)", requireTxn)
	exec(t, sqlDB, "insert into demodb.test2 values(1,'A')", requireTxn)
	snap1 := s.Clock().Now().WallTime
	exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT snap1 ON DATABASE demodb AS OF SYSTEM TIME %d", snap1), requireTxn)

	// 1秒后
	t1s := time.Duration(1e9)
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into demodb.test1 values(2,'b')", requireTxn)

	exec(t, sqlDB, "insert into demodb.test2 values(2,'B')", requireTxn)
	exec(t, sqlDB, "update demodb.test2 set name='B' WHERE id=1", requireTxn)

	// 恢复快照
	exec(t, sqlDB, "REVERT SNAPSHOT snap1 FROM DATABASE demodb", requireTxn)

	// 检查表1的数据
	rows, err := sqlDB.Query("select id,name from demodb.test1")
	if err != nil {
		t.Fatal(err)
	}
	var items []item
	for rows.Next() {
		r := item{}
		_ = rows.Scan(&r.id, &r.name)
		items = append(items, r)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count on table demodb.test1: [1], but got: [%d]", len(items))
	}
	if items[0].name != "a" {
		t.Fatalf("expected name value: [b] of table demodb.test1 , but got: [%s]", items[0].name)
	}

	// 检查表2的数据
	rows, err = sqlDB.Query("select id,name from demodb.test2")
	if err != nil {
		t.Fatal(err)
	}
	items = []item{}
	for rows.Next() {
		r := item{}
		_ = rows.Scan(&r.id, &r.name)
		items = append(items, r)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count on table demodb.test2: [1], but got: [%d]", len(items))
	}
	if items[0].name != "A" {
		t.Fatalf("expected name value: [A] of table [demodb.test2], but got: [%s]", items[0].name)
	}
}

// 开启库级快照，表级恢复测试
func TestRevertDBTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, _ := setupData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := true
	// 创建快照
	exec(t, sqlDB, "create database demodb", requireTxn)
	exec(t, sqlDB, "create table demodb.test1(id int,name string)", requireTxn)
	exec(t, sqlDB, "insert into demodb.test1 values(1,'a')", requireTxn)
	exec(t, sqlDB, "create table test1(id int,name string)", requireTxn)
	exec(t, sqlDB, "create table demodb.test2(id int,name string)", requireTxn)
	exec(t, sqlDB, "create table test2(id int,name string)", requireTxn)
	exec(t, sqlDB, "insert into demodb.test2 values(1,'A')", requireTxn)
	snap1 := s.Clock().Now().WallTime
	exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT snap1 ON DATABASE demodb AS OF SYSTEM TIME %d", snap1), requireTxn)

	// 1秒后
	t1s := time.Duration(1e9)
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into demodb.test1 values(2,'b')", requireTxn)

	exec(t, sqlDB, "insert into demodb.test2 values(2,'B')", requireTxn)
	exec(t, sqlDB, "update demodb.test2 set name='B' WHERE id=1", requireTxn)

	// 恢复表1快照
	exec(t, sqlDB, "REVERT SNAPSHOT snap1 FROM TABLE demodb.test1", requireTxn)

	// 检查表1的数据
	rows, err := sqlDB.Query("select id,name from demodb.test1")
	if err != nil {
		t.Fatal(err)
	}
	var items []item
	for rows.Next() {
		r := item{}
		_ = rows.Scan(&r.id, &r.name)
		items = append(items, r)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count on table demodb.test1: [1], but got: [%d]", len(items))
	}
	if items[0].name != "a" {
		t.Fatalf("expected name value: [b] of table demodb.test1 , but got: [%s]", items[0].name)
	}
	//检查表2数据
	rows2, err := sqlDB.Query("select id,name from demodb.test2")
	if err != nil {
		t.Fatal(err)
	}
	var items2 []item
	for rows2.Next() {
		r := item{}
		_ = rows2.Scan(&r.id, &r.name)
		items2 = append(items2, r)
	}
	if len(items2) != 2 {
		t.Fatalf("expected row count on table demodb.test2: [2], but got: [%d]", len(items2))
	}
	if items2[0].name != "B" {
		t.Fatalf("expected name value: [B] of table demodb.test1 , but got: [%s]", items2[0].name)
	}

	// 恢复表2快照
	exec(t, sqlDB, "REVERT SNAPSHOT snap1 FROM TABLE demodb.test2", requireTxn)

	// 检查表2的数据
	rows, err = sqlDB.Query("select id,name from demodb.test2")
	if err != nil {
		t.Fatal(err)
	}
	items = []item{}
	for rows.Next() {
		r := item{}
		_ = rows.Scan(&r.id, &r.name)
		items = append(items, r)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count on table demodb.test2: [1], but got: [%d]", len(items))
	}
	if items[0].name != "A" {
		t.Fatalf("expected name value: [A] of table [demodb.test2], but got: [%s]", items[0].name)
	}
}

// 表级恢复测试, 重名场景
func TestRevertTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, _ := setupData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := true
	// 创建快照
	exec(t, sqlDB, "create database demodb", requireTxn)
	exec(t, sqlDB, "create table demodb.test1(id int,name string)", requireTxn)
	exec(t, sqlDB, "insert into demodb.test1 values(1,'a')", requireTxn)
	exec(t, sqlDB, "create table test1(id int,name string)", requireTxn)
	snap1 := s.Clock().Now().WallTime
	exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT snap1 ON TABLE demodb.test1 AS OF SYSTEM TIME %d", snap1), requireTxn)

	// 1秒后
	t1s := time.Duration(1e9)
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into demodb.test1 values(2,'b')", requireTxn)
	exec(t, sqlDB, "update demodb.test1 set name='B' WHERE id=1", requireTxn)
	// 恢复表1快照
	exec(t, sqlDB, "REVERT SNAPSHOT snap1 FROM TABLE demodb.test1", requireTxn)

	// 检查表1的数据
	rows, err := sqlDB.Query("select id,name from demodb.test1")
	if err != nil {
		t.Fatal(err)
	}
	var items []item
	for rows.Next() {
		r := item{}
		_ = rows.Scan(&r.id, &r.name)
		items = append(items, r)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count on table demodb.test1: [1], but got: [%d]", len(items))
	}
	if items[0].name != "a" {
		t.Fatalf("expected name value: [b] of table demodb.test1 , but got: [%s]", items[0].name)
	}
}

// 从KV层测试revert
func TestCmdRevert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// todo gzq CI测试概率出错，暂时取消测试
	t.Skip("skip by gzq")
	key1 := bytes.Join([][]byte{tableKey, []byte("a")}, nil)
	key2 := bytes.Join([][]byte{tableKey, []byte("b")}, nil)
	key3 := bytes.Join([][]byte{tableKey, []byte("c")}, nil)
	key4 := bytes.Join([][]byte{tableKey, []byte("d")}, nil)
	key5 := bytes.Join([][]byte{tableKey, []byte("e")}, nil)

	params, _ := tests.CreateTestServerParams()
	if knobs := params.Knobs.Store; knobs != nil {
		if mo := knobs.(*storage.StoreTestingKnobs); mo != nil {
			mo.DisableOptional1PC = true
		}
	}
	s, _, db := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()
	t1s := time.Duration(1e9)

	//时间点1
	d1 := []Data{
		{key1, "value1", false},
		{key2, "value1", false},
		{key3, "value1", false},
	}
	txnBatch(ctx, t, s, d1)
	snap1 := s.Clock().PhysicalNow()
	_ = snap1

	//时间点2
	time.Sleep(t1s)
	d2 := []Data{
		{key2, "", true},
		{key3, "value2", false},
		{key4, "value1", false},
	}
	txnBatch(ctx, t, s, d2)
	snap2 := s.Clock().PhysicalNow()
	_ = snap2

	//时间点3
	time.Sleep(t1s)
	d3 := []Data{
		{key3, "", true},
		{key4, "value2", false},
		{key5, "value1", false},
	}
	txnBatch(ctx, t, s, d3)
	snap3 := s.Clock().PhysicalNow()
	if DEBUG {
		fmt.Println("×××××××时间点3最新数据×××××")
		PrintAllStores(t, s.GetStores().(*storage.Stores), &tableKey, snap3)
	}

	// 最新的数据
	newd := []Data{
		{key1, "value1", false},
		{key4, "value2", false},
		{key5, "value1", false},
	}
	if err := checkKvs(t, s, tableKey, newd, snap3); err != nil {
		t.Fatal(err)
	}

	txnRevert(ctx, t, s, snap1)
	// 恢复快照1数据
	if DEBUG {
		fmt.Println("×××××××恢复快照1后数据×××××")
		PrintAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
	}
	if err := checkKvs(t, s, tableKey, d1, s.Clock().PhysicalNow()); err != nil {
		t.Fatal(err)
	}

	d2 = []Data{
		{key1, "value1", false},
		{key3, "value2", false},
		{key4, "value1", false},
	}
	// 恢复快照2数据
	err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		b.RevertRange(tableKey, tableKey.PrefixEnd(), snap2, false, nil)
		return txn.CommitInBatch(ctx, b)
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	if DEBUG {
		fmt.Println("×××××××恢复快照2-事务已提交×××××")
		PrintAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
	}
	if err := checkKvs(t, s, tableKey, d2, s.Clock().PhysicalNow()); err != nil {
		t.Fatal(err)
	}
}

// 外键约束恢复测试
func TestSnapshotConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, _ := setupData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false

	t1s := time.Duration(2e9) // 2秒
	// 时间点1
	exec(t, sqlDB, "CREATE DATABASE data;", requireTxn)
	exec(t, sqlDB, "CREATE TABLE data.item (id int primary key,s_w_id int);", requireTxn)
	exec(t, sqlDB, "CREATE TABLE data.item2 (id int primary key,s_w_id int);", requireTxn)
	// 开启快照
	time.Sleep(time.Duration(1e8))
	exec(t, sqlDB, "use data;", requireTxn)
	exec(t, sqlDB, "alter table item add constraint s_item_fkey foreign key (s_w_id) references item2 (id);", requireTxn)
	snap1 := s.Clock().Now().WallTime
	exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT snap1 ON DATABASE data AS OF SYSTEM TIME %d", snap1), requireTxn)
	//exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT snap2 ON TABLE data.item2 AS OF SYSTEM TIME %d", snap1), requireTxn)
	//time.Sleep(time.Duration(1e8)) // 100ms
	time.Sleep(t1s)

	// can not revert table
	_, err := sqlDB.Exec("REVERT SNAPSHOT snap1 FROM TABLE data.item")
	if err == nil || !strings.Contains(err.Error(), "table[item] have some constraints ,can not revert.") {
		t.Fatalf("expected error msg {table[item] have some constraints ,can not revert.} ,but got {%s}", err)
	}
	// can not revert table
	_, err = sqlDB.Exec("REVERT SNAPSHOT snap1 FROM TABLE data.item2")
	if err == nil || !strings.Contains(err.Error(), "table[item2] have some constraints ,can not revert.") {
		t.Fatalf("expected error msg {table[item2] have some constraints ,can not revert.} ,but got {%s}", err)
	}
}
