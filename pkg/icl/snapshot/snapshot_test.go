package snapshot

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// 语法类测试
func TestSnapshotParser1(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		sql string
	}{
		{`CREATE SNAPSHOT customers_snapshot1 ON TABLE customers AS OF SYSTEM TIME '-1h'`},
		{`CREATE SNAPSHOT customers_snapshot2 ON DATABASE defaultdb AS OF SYSTEM TIME '-5h'`},
		{`SHOW SNAPSHOT customers_snapshot1`},
		{`SHOW SNAPSHOT FROM TABLE db1.customers`},
		{`SHOW SNAPSHOT FROM DATABASE defaultdb`},
		{`SHOW SNAPSHOT ALL`},
		{`DROP SNAPSHOT customers_snapshot1`},
		{`REVERT SNAPSHOT customers_snapshot1 FROM TABLE customer`},
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

type snapshot struct {
	id     uuid.UUID
	name   string
	stype  string
	object string
	asof   time.Time
	desc   string
}

// 测试SHOW SNAPSHOT语句
func TestShowSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, db, tableData := setupData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	// 1.测试1-sql语句检查快照个数
	rows, err := sqlDB.Query("SELECT * FROM [SHOW SNAPSHOT FROM TABLE ITEM] ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []snapshot{}
	for rows.Next() {
		r := snapshot{}
		_ = rows.Scan(&r.id, &r.name, &r.stype, &r.object, &r.asof, &r.desc)
		snapshots = append(snapshots, r)
	}
	if len(snapshots) != 3 {
		t.Fatalf("expected snapshot count is %d,but got %d ", 3, len(snapshots))
	}
	for i, ss := range snapshots {
		if ss.name != tableData[i].name {
			t.Fatalf("%d: expected snapshot name %s; got %s", i, tableData[i].name, ss.name)
		}
		if ss.object != "item" {
			t.Fatalf("%d: expected object name 'item'; got %s", i, ss.object)
		}
	}

	// 2.测试2-在replica层检查快照个数
	desc, _ := LookupTableDescriptor(ctx, db, "defaultdb", "item")
	snaps, err := getTableSnapshots(ctx, s, desc)
	if err != nil {
		t.Fatal(err)
	}

	// todo jdx
	//t.Skip("skip by gzq")

	if len(snaps) != 3 {
		t.Fatalf("expected table[defaultdb.item] snapshot count is %d,but got %d ", 3, len(snaps))
	}
}

//
func TestDropSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _, tableData := setupData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	rs, err := sqlDB.Exec("DROP SNAPSHOT " + tableData[2].name)
	if err != nil {
		t.Fatal(err)
	}
	_ = rs

	// 检查是否删除成功
	rows, err := sqlDB.Query("SHOW SNAPSHOT FROM TABLE ITEM")
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []snapshot{}
	for rows.Next() {
		r := snapshot{}
		_ = rows.Scan(&r.id, &r.name, &r.stype, &r.object, &r.asof, &r.desc)
		snapshots = append(snapshots, r)
	}
	if len(snapshots) != 2 {
		t.Fatalf("expected snapshot count is %d,but got %d ", 2, len(snapshots))
	}
}
func TestAlterTableWithSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _, tableData := setupData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec("ALTER TABLE item ADD COLUMN price float")
	if err == nil || !strings.Contains(err.Error(), "cannot alter table") {
		t.Fatalf("expected error msg {cannot alter table} ,but got {%s}", err)
	}

	// delete all snapshots
	for i := range tableData {
		if tableData[i].name == "" {
			continue
		}
		if _, err := sqlDB.Exec("DROP SNAPSHOT " + tableData[i].name); err != nil {
			t.Fatal(err)
		}
	}
	// now , can alter table success
	if _, err := sqlDB.Exec("ALTER TABLE item ADD COLUMN price float"); err != nil {
		t.Fatal(err)
	}
}
func TestDropTableWithSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _, tableData := setupData(t, true)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec("DROP TABLE item")
	if err == nil || !strings.Contains(err.Error(), "cannot drop table") {
		t.Fatalf("expected error msg {cannot drop table} ,but got {%s}", err)
	}

	_, err = sqlDB.Exec("TRUNCATE TABLE item")
	if err == nil || !strings.Contains(err.Error(), "cannot truncate table") {
		t.Fatalf("expected error msg {cannot truncate table} ,but got {%s}", err)
	}

	// delete all snapshots
	for i := range tableData {
		if tableData[i].name == "" {
			continue
		}
		if _, err := sqlDB.Exec("DROP SNAPSHOT " + tableData[i].name); err != nil {
			t.Fatal(err)
		}
	}

	// now , can truncate table success
	if _, err := sqlDB.Exec("TRUNCATE TABLE item"); err != nil {
		t.Fatal(err)
	}
	// now , can alter table success
	if _, err := sqlDB.Exec("DROP TABLE item"); err != nil {
		t.Fatal(err)
	}
}

// 测试库级快照语句
func TestDatabaseSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, db, _ := setupData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	// 创建快照
	exec(t, sqlDB, "create database demodb", false)
	exec(t, sqlDB, "create table demodb.test1(id int,name string)", false)
	exec(t, sqlDB, "create table demodb.test2(id int,name string)", false)
	exec(t, sqlDB, "create snapshot snap1 ON database demodb", false)
	exec(t, sqlDB, "create table defaultdb.test3(id int,name string)", false)

	// 1.测试1-查询快照
	rows, err := sqlDB.Query("SHOW SNAPSHOT FROM DATABASE demodb")
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []snapshot{}
	for rows.Next() {
		r := snapshot{}
		_ = rows.Scan(&r.id, &r.name, &r.stype, &r.object, r.asof, r.desc)
		snapshots = append(snapshots, r)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected snapshot count is %d,but got %d ", 1, len(snapshots))
	}
	if snapshots[0].name != "snap1" {
		t.Fatalf("expected database snapshot name snap1; got %s", snapshots[0].name)
	}
	if snapshots[0].object != "demodb" {
		t.Fatalf("expected object is demodb, but got %s", snapshots[0].object)
	}

	// 2.测试2-检查表级隐含快照
	sql := fmt.Sprintf("SELECT id,name,asof,type FROM system.snapshots where parent_id='%s'", snapshots[0].id)
	rows, err = sqlDB.Query(sql)
	if err != nil {
		t.Fatal(err)
	}
	snapshots = []snapshot{}
	for rows.Next() {
		r := snapshot{}
		var itype int
		_ = rows.Scan(&r.id, &r.name, &r.asof, &itype)
		if itype != tree.TableSnapshotType {
			t.Fatalf("expected TableSnapshotType,but got %d ", itype)
		}
		snapshots = append(snapshots, r)
	}
	if len(snapshots) != 2 {
		t.Fatalf("expected table snapshot count is %d,but got %d ", 2, len(snapshots))
	}

	// todo jdx
	//t.Skip("skip by gzq")

	// 3.测试3-在replica层检查快照个数
	desc, _ := LookupTableDescriptor(ctx, db, "demodb", "test1")
	snaps, err := getTableSnapshots(ctx, s, desc) // 第一个表的key
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected table[demodb.test1] snapshot count is %d,but got %d ", 1, len(snaps))
	}

	desc, _ = LookupTableDescriptor(ctx, db, "demodb", "test2")
	snaps, err = getTableSnapshots(ctx, s, desc)
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected table[demodb.test2] snapshot count is %d,but got %d ", 1, len(snaps))
	}

	desc, _ = LookupTableDescriptor(ctx, db, "defaultdb", "test3")
	snaps, err = getTableSnapshots(ctx, s, desc)
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected table[defaultdb.test3] snapshot count is %d,but got %d ", 0, len(snaps))
	}

	// 4.测试4-删除库级快照
	exec(t, sqlDB, "DROP SNAPSHOT snap1", false)
	// 检查是否删除成功
	sql = fmt.Sprintf("SELECT id,name,asof FROM system.snapshots")
	rows, err = sqlDB.Query(sql)
	if err != nil {
		t.Fatal(err)
	}
	snapshots = []snapshot{}
	for rows.Next() {
		r := snapshot{}
		_ = rows.Scan(&r.id, &r.name, &r.asof)
		snapshots = append(snapshots, r)
	}
	if len(snapshots) != 0 {
		t.Fatalf("expected dropped snapshot,but got %d ", len(snapshots))
	}

}

func TestAlterDatabaseWithSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _, _ := setupData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	// 创建快照
	exec(t, sqlDB, "create database demodb", false)
	exec(t, sqlDB, "create table demodb.test1(id int,name string)", false)
	exec(t, sqlDB, "create table demodb.test2(id int,name string)", false)
	exec(t, sqlDB, "create snapshot snap1 ON database demodb", false)
	exec(t, sqlDB, "create table defaultdb.test3(id int,name string)", false)

	// can not drop table
	_, err := sqlDB.Exec("DROP DATABASE demodb")
	if err == nil || !strings.Contains(err.Error(), "cannot drop database") {
		t.Fatalf("expected error msg {cannot drop database} ,but got {%s}", err)
	}

	// can not alter table
	_, err = sqlDB.Exec("ALTER TABLE demodb.test1 ADD COLUMN day int")
	if err == nil || !strings.Contains(err.Error(), "cannot alter table") {
		t.Fatalf("expected error msg {cannot alter table} ,but got {%s}", err)
	}

	// can not truncate table
	_, err = sqlDB.Exec("TRUNCATE TABLE demodb.test1")
	if err == nil || !strings.Contains(err.Error(), "cannot truncate table") {
		t.Fatalf("expected error msg {cannot truncate table} ,but got {%s}", err)
	}

	// can not drop table
	_, err = sqlDB.Exec("DROP TABLE demodb.test1")
	if err == nil || !strings.Contains(err.Error(), "cannot drop table") {
		t.Fatalf("expected error msg {cannot drop table} ,but got {%s}", err)
	}

	// can not create table
	_, err = sqlDB.Exec("CREATE TABLE demodb.testnew(id int,name string)")
	if err == nil || !strings.Contains(err.Error(), "cannot create table") {
		t.Fatalf("expected error msg {cannot create table} ,but got {%s}", err)
	}

	// drop snapshot
	exec(t, sqlDB, "drop snapshot snap1", false)

	// can alter table
	if _, err := sqlDB.Exec("ALTER TABLE demodb.test1 ADD COLUMN day int"); err != nil {
		t.Fatal(err)
	}

	// can truncate table
	if _, err := sqlDB.Exec("TRUNCATE TABLE demodb.test1"); err != nil {
		t.Fatal(err)
	}

	// can drop table
	if _, err := sqlDB.Exec("DROP TABLE demodb.test1"); err != nil {
		t.Fatal(err)
	}

	// can create table
	if _, err := sqlDB.Exec("CREATE TABLE demodb.testnew(id int,name string)"); err != nil {
		t.Fatal(err)
	}
}

func LookupTableDescriptor(
	ctx context.Context, txn *client.DB, dbName string, tblName string,
) (*sqlbase.TableDescriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	rows, err := txn.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, err
	}
	// 先查询db
	var dbDesc *sql.DatabaseDescriptor
	for _, row := range rows {
		desc := sqlbase.Descriptor{}
		if err := row.ValueProto(&desc); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
		}
		if db := desc.GetDatabase(); db != nil && db.Name == dbName {
			dbDesc = db
			break
		}
	}
	if dbDesc == nil {
		return nil, nil
	}

	// table的schema默认值：public
	parentID := dbDesc.GetSchemaID("public")
	// 本数据库之外的其他数据表
	for _, row := range rows {
		desc := sqlbase.Descriptor{}
		if err := row.ValueProto(&desc); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
		}
		if tbl := desc.GetTable(); tbl != nil {
			if tbl.Dropped() {
				continue
			}
			if tbl.ParentID == parentID && tbl.Name == tblName { // table的ParentID是Schema的ID，Schema的ParentID才是DB
				return tbl, nil
			}
		}
	}
	return nil, nil
}
