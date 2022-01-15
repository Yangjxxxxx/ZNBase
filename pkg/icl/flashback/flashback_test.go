package flashback

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/tests"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/testutils/testcluster"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// 语法类测试
func TestFlashbackParser(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		sql string
	}{
		{`ALTER TABLE customers ENABLE FLASHBACK WITH TTLDAYS 5`},
		{`ALTER DATABASE demodb ENABLE FLASHBACK WITH TTLDAYS 2`},
		{`FLASHBACK TABLE customers AS OF SYSTEM TIME '-1h'`},
		{`FLASHBACK DATABASE demodb AS OF SYSTEM TIME '-50s'`},
		{`SHOW FLASHBACK ALL`},
		{`ALTER TABLE customers DISABLE FLASHBACK`},
		{`ALTER DATABASE demodb DISABLE FLASHBACK`},
	}

	var p parser.Parser // Verify that the same parser can be reused.
	for _, d := range testData {
		t.Run(d.sql, func(t *testing.T) {
			stmts, err := p.Parse(d.sql)
			if err != nil {
				t.Fatalf("%s: expected success, but found %s", d.sql, err)
			}
			s := stmts.String()
			if d.sql != s {
				t.Errorf("expected \n%q\n, but found \n%q", d.sql, s)
			}
			sqlutils.VerifyStatementPrettyRoundtrip(t, d.sql)
		})
	}
}

// 查询系统表测试
func TestQuerySystemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		sql string
	}{
		{`SELECT ttl_days FROM system.flashback WHERE (type = $1) AND (object_id = $2)`},
	}
	var p parser.Parser // Verify that the same parser can be reused.
	for _, d := range testData {
		t.Run(d.sql, func(t *testing.T) {
			stmts, err := p.Parse(d.sql)
			if err != nil {
				t.Fatalf("%s: expected success, but found %s", d.sql, err)
			}
			s := stmts.String()
			if d.sql != s {
				t.Errorf("expected \n%q\n, but found \n%q", d.sql, s)
			}
			sqlutils.VerifyStatementPrettyRoundtrip(t, d.sql)
		})
	}
}

type flashback struct {
	objectID   int
	TYPE       string
	objectName string
	parentID   int
	dbID       int
	ctTime     time.Time
	ttlDays    int
	status     string
}

// 测试SHOW FLASHBACK ALL语句
func TestShowFlashback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _, _ := flashData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	// 1.测试1-sql语句检查快照个数
	rows, err := sqlDB.Query("SHOW FLASHBACK ALL")
	if err != nil {
		t.Fatal(err)
	}
	var flashbacks []flashback
	for rows.Next() {
		r := flashback{}
		_ = rows.Scan(&r.objectID, &r.TYPE, &r.objectName, &r.parentID, &r.dbID, &r.ctTime, &r.ttlDays, &r.status)
		flashbacks = append(flashbacks, r)
	}
	if len(flashbacks) != 1 {
		t.Fatalf("expected flashback count is %d,but got %d ", 1, len(flashbacks))
	}
	for i, ss := range flashbacks {
		if ss.ttlDays != 2 {
			t.Fatalf("%d: expected flashback ttl_days %d; got %d", i, 2, ss.ttlDays)
		}
		if ss.objectName != "item" {
			t.Fatalf("%d: expected object name 'item'; got %s", i, ss.objectName)
		}
		if ss.TYPE != "Table" {
			t.Fatalf("%d: expected type name 'Table'; got %s", i, ss.TYPE)
		}
	}
}

// SQL层测试闪回
func TestFlashbackWithSql(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, tableData := flashData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := true

	for _, d := range tableData {
		// 恢复快照1
		if DEBUG {
			t.Logf("*********************闪回[%s] 恢复前 KV*********************", d.name)
			printAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		}
		exec(t, sqlDB, fmt.Sprintf("FLASHBACK TABLE item AS OF SYSTEM TIME %d", d.ts), requireTxn)
		if DEBUG {
			t.Logf("*********************闪回[%s] 恢复后 KV*********************", d.name)
			printAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		}

		if err := checkResults(sqlDB, "select * from item", d.data); err != nil {
			t.Fatal(err)
		}
	}
}

func initNone(_ *testcluster.TestCluster) {}

// test for enable & disable table flashback
func TestUpdateDescNamespaceFlashback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000

	_, _, sqlDB, _, cleanupFn := dumpLoadTestSetup(t, multiNode, numAccounts, initNone)
	defer cleanupFn()

	i := 0
	for {
		createSQL := fmt.Sprintf("create table data.test%d(id int, name string,  ct_time TIMESTAMP, visible bool);", i)
		sqlDB.Exec(t, createSQL)
		i++
		if i > 9 {
			i = 0
			break
		}
	}
	for {
		enableSQL := fmt.Sprintf("ALTER database data enable flashback with ttldays %d;", i%7+1)
		sqlDB.Exec(t, enableSQL)
		fmt.Println("ALTER database data enable flashback with ttldays ", i%7+1)
		disableSQL := fmt.Sprintf("ALTER database data disable flashback;")
		sqlDB.Exec(t, disableSQL)
		fmt.Println("ALTER database data disable flashback;", i)
		i++
		if i > 10 {
			break
		}
	}
}

// 表结构变更的闪回测试
func TestFlashbackWithAlter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _ := startFlashbackCluster(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false

	t1s := time.Duration(2e9) // 2秒
	// 时间点1
	exec(t, sqlDB, "CREATE DATABASE data", requireTxn)
	exec(t, sqlDB, "CREATE TABLE data.item (id int primary key,name string);", requireTxn)
	// 开启flashback
	exec(t, sqlDB, "ALTER TABLE data.item ENABLE FLASHBACK WITH TTLDAYS 2;", requireTxn)
	exec(t, sqlDB, "insert into data.item values(1,'tv'),(2,'tel');", requireTxn)
	time1 := s.Clock().Now().WallTime
	//time.Sleep(time.Duration(1e8)) // 100ms
	time.Sleep(t1s)
	//exec(t, sqlDB, "drop table item cascade;", requireTxn)
	exec(t, sqlDB, "alter table data.item add day int;", requireTxn)
	exec(t, sqlDB, "insert into data.item values(3,'tv',5),(4,'tel',6)", requireTxn)
	//exec(t, sqlDB, "insert into item values(3,'xiaoming'),(4,'lilei');", requireTxn)

	exec(t, sqlDB, fmt.Sprintf("flashback table data.item as of system time %d", time1), requireTxn)

	items, err := queryItems(sqlDB, "select * from data.item")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count on table demodb.test1: [2], but got: [%d]", len(items))
	}

}

// Drop database 闪回测试
func TestFlashbackDropDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _ := startFlashbackCluster(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false

	t1s := time.Duration(2e9) // 2秒
	// 时间点1
	exec(t, sqlDB, "create database demodb;", requireTxn)
	exec(t, sqlDB, "CREATE TABLE demodb.item (id int primary key,name string);", requireTxn)
	// exec(t, sqlDB, "CREATE TABLE demodb.item1 (id int primary key,name string);", requireTxn)
	// 开启flashback
	exec(t, sqlDB, "ALTER database demodb ENABLE FLASHBACK WITH TTLDAYS 2;", requireTxn)
	exec(t, sqlDB, "insert into demodb.item values(1,'tv'),(2,'tel')", requireTxn)
	//time1 := s.Clock().Now().WallTime
	time.Sleep(t1s) // 100ms  alter table customer add day int;
	//exec(t, sqlDB, "drop table demodb.item cascade;", requireTxn)
	exec(t, sqlDB, "drop database demodb cascade;", requireTxn)
	exec(t, sqlDB, "create database demodb;", requireTxn)
	exec(t, sqlDB, "CREATE TABLE demodb.item (id int primary key,name string);", requireTxn)
	exec(t, sqlDB, "ALTER database demodb ENABLE FLASHBACK WITH TTLDAYS 2;", requireTxn)
	exec(t, sqlDB, "insert into demodb.item values(1,'tv'),(2,'tel'),(3,'tel')", requireTxn)

	//type idStruct struct {
	//	id int
	//}
	//row, _ := sqlDB.Query("select object_id from system.flashback where parent_id = 0")
	//for row.Next() {
	//	r := idStruct{}
	//	row.Scan(&r.id)
	//	fmt.Println(r.id)
	//}
	time.Sleep(t1s) // 100ms  alter table customer add day int;
	exec(t, sqlDB, "drop database demodb cascade;", requireTxn)
	time.Sleep(t1s)
	exec(t, sqlDB, "flashback database demodb to before drop with object_id 54;", requireTxn) // len(items) == 2
	//exec(t, sqlDB, "flashback database demodb to before drop with object_id 57;", requireTxn) // len(items) == 3

	items, err := queryItems(sqlDB, fmt.Sprintf("select * from demodb.item"))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count on table demodb.item: [2], but got: [%d]", len(items))
	}

}

// 测试表删除闪回
func TestFlashbackDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	_, _ = sqlDB.Exec(`CREATE TABLE item (id int primary key,name string);`)
	_, _ = sqlDB.Exec(`alter table item enable flashback with ttldays 3;`)
	_, _ = sqlDB.Exec(`insert into item values(1,'tv'),(2,'tel')`)
	time.Sleep(time.Duration(2e9))
	_, _ = sqlDB.Exec(`drop table item;`)
	time.Sleep(time.Duration(2e9))
	if _, err := sqlDB.Exec(`flashback table item to before drop with object_id 54;`); err != nil {
		t.Fatal(err)
	}
	items, err := queryItems(sqlDB, fmt.Sprintf("select * from item"))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count on table item: [2], but got: [%d]", len(items))
	}
}

// 在多个Range分片上，SQL层测试闪回
func TestFlashbackOnMultiRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, tableData := flashData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false

	// 拆分Range之后再REVERT，验证多个RANGE 的revert
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

		// 闪回到指定时间点
		if DEBUG {
			t.Logf("*********************闪回[%s] 恢复前 KV*********************", d.name)
			printAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		}
		exec(t, sqlDB, fmt.Sprintf("FLASHBACK TABLE item AS OF SYSTEM TIME %d", d.ts), requireTxn)
		if DEBUG {
			t.Logf("*********************闪回[%s] 恢复后 KV*********************", d.name)
			printAllStores(t, s.GetStores().(*storage.Stores), &tableKey, s.Clock().PhysicalNow())
		}

		if err := checkResults(sqlDB, "select * from item", d.data); err != nil {
			t.Fatal(err)
		}
	}
}

// 库级闪回恢复测试
func TestFlashbackDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, _ := flashData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := true
	exec(t, sqlDB, "create database demodb", requireTxn)
	exec(t, sqlDB, "create table demodb.test1(id int,name string)", requireTxn)
	exec(t, sqlDB, "insert into demodb.test1 values(1,'a')", requireTxn)
	exec(t, sqlDB, "create table demodb.test2(id int,name string)", requireTxn)
	exec(t, sqlDB, "insert into demodb.test2 values(1,'A')", requireTxn)
	// 创建闪回
	exec(t, sqlDB, "ALTER DATABASE demodb ENABLE FLASHBACK WITH TTLDAYS 2", requireTxn)
	t1s := time.Duration(1e9)
	time.Sleep(t1s)
	time1 := s.Clock().Now().WallTime
	// 2秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into demodb.test1 values(2,'b')", requireTxn)

	exec(t, sqlDB, "insert into demodb.test2 values(2,'B')", requireTxn)
	exec(t, sqlDB, "update demodb.test2 set name='B' WHERE id=1", requireTxn)
	time.Sleep(t1s)
	//exec(t, sqlDB, "drop table demodb.test1 cascade", requireTxn)

	// 恢复time1时间点的数据
	//time.Sleep(t1s)
	//exec(t, sqlDB, fmt.Sprintf("flashback table demodb.test1 to before drop with object_id 57"), requireTxn)
	//time.Sleep(t1s)
	exec(t, sqlDB, fmt.Sprintf("flashback database demodb as of system time %d", time1), requireTxn)

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

// 库级闪回测试--测试重名场景
func TestDatabaseFlashback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, _ := flashData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := true
	t1s := time.Duration(1e9)
	exec(t, sqlDB, "create database demodb;", requireTxn)
	exec(t, sqlDB, "create table demodb.test1(id int,name string);", requireTxn)
	exec(t, sqlDB, "insert into demodb.test1 values(1,'a');", requireTxn)
	exec(t, sqlDB, "drop table demodb.test1;", requireTxn)
	time.Sleep(t1s)
	exec(t, sqlDB, "create table demodb.test1(id int,name string);", requireTxn)
	exec(t, sqlDB, "insert into demodb.test1 values(1,'a');", requireTxn)
	time.Sleep(t1s)
	// 创建闪回
	exec(t, sqlDB, "ALTER DATABASE demodb ENABLE FLASHBACK WITH TTLDAYS 2;", requireTxn)
	exec(t, sqlDB, "drop table demodb.test1;", requireTxn)
	exec(t, sqlDB, "create table demodb.test1(id int,name string);", requireTxn)
	exec(t, sqlDB, "insert into demodb.test1 values(1,'a');", requireTxn)
	time.Sleep(t1s)
	exec(t, sqlDB, "drop database demodb cascade;", requireTxn)
	time.Sleep(t1s)

	exec(t, sqlDB, "flashback database demodb to before drop with object_id 54", requireTxn)
	items, err := queryItems(sqlDB, fmt.Sprintf("select * from demodb.test1;"))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count on table item: [1], but got: [%d]", len(items))
	}
}

// 测试truncate
func TestFlashbackTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, _ := flashData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false
	t1s := time.Duration(2e9)

	exec(t, sqlDB, "create table item(id int,name string);", requireTxn)
	exec(t, sqlDB, "alter table item enable flashback with ttldays 3;", requireTxn)
	exec(t, sqlDB, "insert into item values(1,'tv'),(2,'tel');", requireTxn)
	time.Sleep(t1s)
	exec(t, sqlDB, "truncate table item;", requireTxn)
	time.Sleep(t1s)
	exec(t, sqlDB, "drop table item cascade;", requireTxn)
	// 创建闪回
	time.Sleep(t1s)
	exec(t, sqlDB, "flashback table item to before drop with object_id 54;", requireTxn)
	// 2秒后
	items, err := queryItems(sqlDB, fmt.Sprintf("select * from item;"))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count on table item: [2], but got: [%d]", len(items))
	}

}

// 测试drop后闪回主键
func TestFlashbackPrimary(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _, _ := flashData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false
	t1s := time.Duration(2e9)
	exec(t, sqlDB, "create database test10;", requireTxn)
	exec(t, sqlDB, "alter database test10 ENABLE FLASHBACK WITH TTLDAYS 2;", requireTxn)
	exec(t, sqlDB, "create table test10.t1(a int,b int,c int,primary key(a,b));", requireTxn)
	exec(t, sqlDB, "insert into test10.t1 values(3,3,3);", requireTxn)
	time.Sleep(t1s)
	exec(t, sqlDB, "drop table test10.t1;", requireTxn)
	// 创建闪回
	exec(t, sqlDB, "flashback table test10.t1 to before drop with object_id 56;", requireTxn)
	//2秒后
	time.Sleep(t1s)
	time1 := s.Clock().Now().WallTime
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into test10.t1 values(2,2,2);", requireTxn)
	time.Sleep(t1s)
	exec(t, sqlDB, fmt.Sprintf("flashback table test10.t1 as of system time %d", time1), requireTxn)
	items, err := queryItems(sqlDB, fmt.Sprintf("select * from test10.t1;"))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count on table t1: [1], but got: [%d]", len(items))
	}
}

// 外键约束闪回测试
func TestFlashbackConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _ := startFlashbackCluster(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := false

	t1s := time.Duration(2e9) // 2秒
	// 时间点1
	exec(t, sqlDB, "CREATE DATABASE data;", requireTxn)
	exec(t, sqlDB, "CREATE TABLE data.item (id int primary key,s_w_id int);", requireTxn)
	exec(t, sqlDB, "CREATE TABLE data.item2 (id int primary key,s_w_id int);", requireTxn)
	// 开启flashback
	exec(t, sqlDB, "ALTER database data ENABLE FLASHBACK WITH TTLDAYS 2;", requireTxn)

	time1 := s.Clock().Now().WallTime
	//time.Sleep(time.Duration(1e8)) // 100ms
	time.Sleep(t1s)
	exec(t, sqlDB, "use data;", requireTxn)
	exec(t, sqlDB, "alter table item add constraint s_item_fkey foreign key (s_w_id) references item2 (id);", requireTxn)
	// can not flashback table
	_, err := sqlDB.Exec(fmt.Sprintf("flashback table data.item as of system time %d", time1))
	if err == nil || !strings.Contains(err.Error(), "Table[item] have some constraints ,can not revert.") {
		t.Fatalf("expected error msg {Table[item] have some constraints ,can not revert.} ,but got {%s}", err)
	}
	// can not flashback table
	_, err = sqlDB.Exec(fmt.Sprintf("flashback table data.item2 as of system time %d", time1))
	if err == nil || !strings.Contains(err.Error(), "Table[item2] have some constraints ,can not revert.") {
		t.Fatalf("expected error msg {Table[item2] have some constraints ,can not revert.} ,but got {%s}", err)
	}

	exec(t, sqlDB, "drop table item cascade;", requireTxn)
	// can not flashback table
	_, err = sqlDB.Exec("flashback table data.item to before drop with object_id 56;")
	if err == nil || !strings.Contains(err.Error(), "Table[item] have some constraints ,can not revert.") {
		t.Fatalf("expected error msg {Table[item] have some constraints ,can not revert.} ,but got {%s}", err)
	}
}

func TestTableIDFromKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tableKey := MakeTableIDKey(53)
	tableID, err := sql.TableIDFromKey(tableKey)
	if err != nil || tableID != 53 {
		t.Fatalf("%v\n", err)
	}
}
