package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/tests"
	"github.com/znbasedb/znbase/pkg/testutils"
	_ "github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

func TestInheritTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
USE t;
CREATE SCHEMA t;
CREATE TABLE t.t.parent (a int,b int);
`); err != nil {
		t.Fatal(err)
	}
	dbNameKey := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "t")
	r, err := kvDB.Get(ctx, dbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(r.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(ctx, dbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	dbDesc := desc.GetDatabase()
	var scDesc sqlbase.SchemaDescriptor
	for _, v := range dbDesc.Schemas {
		if v.Name == "t" {
			scDesc = v
		}
	}
	if _, err := sqlDB.Exec(`
CREATE TABLE t.t.child () INHERITS(t.t.parent);
`); err != nil {
		t.Fatal(err)
	}
	parentDesc, err := getTableDesc(ctx, scDesc.ID, "parent", kvDB, desc)
	if err != nil {
		t.Fatal(err)
	}
	childDesc, err := getTableDesc(ctx, scDesc.ID, "child", kvDB, desc)
	if err != nil {
		t.Fatal(err)
	}
	if parentDesc.InheritsBy[0] != childDesc.ID || childDesc.Inherits[0] != parentDesc.ID {
		t.Fatalf("miss Inheritance relationship")
	}
	if _, err := sqlDB.Exec(`
ALTER TABLE t.t.parent add column c int not null default 1 check(c > 0);
`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`
ALTER TABLE t.t.parent rename column a to a1;
`); err != nil {
		t.Fatal(err)
	}
	childDesc, err = getTableDesc(ctx, scDesc.ID, "child", kvDB, desc)
	if err != nil {
		t.Fatal(err)
	}
	if childDesc.Columns[0].Name != "a1" {
		t.Fatal("alter inherit table rename column has error")
	}
	if childDesc.Checks[0] == nil {
		t.Fatal("alter inherit table add check constraint has error")
	}
	if childDesc.Columns[3].Name != "c" || *childDesc.Columns[3].DefaultExpr != "1" || childDesc.Columns[3].Nullable {
		t.Fatal("alter inherit table add column has error")
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.child add column d int;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent add column d string;`); !testutils.IsError(err,
		`pq: child table \"child\" has different type for column \"d\"`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent add column d int default 2 not null;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.child add constraint parent_check_c check(c > 0);`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.child drop column b;`); !testutils.IsError(err,
		`pq: inherit column b can not be dropped`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent drop column b;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent drop column d;`); err != nil {
		t.Fatal(err)
	}
	childDesc, err = getTableDesc(ctx, scDesc.ID, "child", kvDB, desc)
	if err != nil {
		t.Fatal(err)
	}
	if childDesc.Columns[3].Name != "d" || childDesc.Columns[3].Nullable || childDesc.Columns[3].DefaultExpr == nil {
		t.Fatal("inherit table add column has error")
	}
	if childDesc.Columns[3].IsInherits == true || childDesc.Columns[3].InhCount != 1 {
		t.Fatal("inherit table drop column has error")
	}

	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent add check (a1 > 0);`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.child drop constraint parent_check_a1;`); !testutils.IsError(err,
		`pq: inherit check constraint can not be droped`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent drop constraint parent_check_a1;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent disable constraint parent_check_c;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.child no inherit t.t.parent;`); err != nil {
		t.Fatal(err)
	}
	childDesc, err = getTableDesc(ctx, scDesc.ID, "child", kvDB, desc)
	if err != nil {
		t.Fatal(err)
	}
	parentDesc, err = getTableDesc(ctx, scDesc.ID, "parent", kvDB, desc)
	if err != nil {
		t.Fatal(err)
	}
	if len(parentDesc.InheritsBy) != 0 || len(childDesc.Inherits) != 0 {
		t.Fatal("alter table no inherit has error")
	}
	for _, col := range childDesc.Columns {
		if col.Hidden {
			continue
		}
		if col.IsInherits || col.InhCount != 1 {
			t.Fatal("child table contain inherit column after no inherit")
		}
	}
	if !childDesc.Checks[0].Able {
		t.Fatal("inherit able constraint has error")
	}
	if childDesc.Checks[0].IsInherits || childDesc.Checks[0].InhCount != 1 {
		t.Fatal("alter table no inherit has error")
	}
	if _, err := sqlDB.Exec(`drop table t.t.parent, t.t.child;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`
create table t.t.parent(a int check(a > 0), b int, c int not null default 1,constraint check_b check(b>0));
`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`create table t.t.child(d int, e int)inherits(t.t.parent);`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.child rename column a to a1;`); !testutils.IsError(err,
		`pq: cannot rename inherited column \"a\"`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent rename column a to b;`); !testutils.IsError(err,
		`pq: column name \"b\" already exists`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent rename column a to d;`); !testutils.IsError(err,
		`pq: column name \"d\" in inherit table \"child\" already exists`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent rename column a to a1;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.child rename constraint check_b to check_b1;`); !testutils.IsError(err,
		`pq: inherited check constraint can not be rename`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent rename constraint check_b to check_b1;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent alter column a1 set not null;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent alter column b set default 2;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent alter column c drop default;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.t.parent alter column c drop not null;`); err != nil {
		t.Fatal(err)
	}
	childDesc, err = getTableDesc(ctx, scDesc.ID, "child", kvDB, desc)
	if err != nil {
		t.Fatal(err)
	}
	if childDesc.Columns[0].Name != "a1" {
		t.Fatal("rename inherit column has error")
	}
	if *childDesc.Columns[1].DefaultExpr != "2" ||
		childDesc.Columns[2].DefaultExpr != nil ||
		childDesc.Columns[0].Nullable || !childDesc.Columns[2].Nullable {
		t.Fatal("inherit table ColumnMutationCmd has error")
	}
	if _, err := sqlDB.Exec(`drop table t.t.parent cascade;`); err != nil {
		t.Fatal(err)
	}
	res, err := sqlDB.Exec(`show tables;`)
	if err != nil {
		t.Fatal(err)
	}
	tableCount, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if tableCount != 0 {
		t.Fatal("contain inherit table")
	}
}

func getTableDesc(
	ctx context.Context, id sqlbase.ID, s string, db *client.DB, desc *sqlbase.Descriptor,
) (*sqlbase.TableDescriptor, error) {
	tbNameKey := sqlbase.MakeNameMetadataKey(id, s)
	gr, err := db.Get(ctx, tbNameKey)
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, fmt.Errorf(`table "parent" does not exist`)
	}
	tbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	ts, err := db.GetProtoTs(ctx, tbDescKey, desc)
	if err != nil {
		return nil, err
	}
	return desc.Table(ts), nil
}

func TestAlterSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Disable the asynchronous path so that the table data left
			// behind is not cleaned up.
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
USE t;
CREATE SCHEMA t;
CREATE TABLE t.t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
CREATE VIEW Temp AS SELECT v FROM t.t.kv;
`); err != nil {
		t.Fatal(err)
	}

	dbNameKey := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "t")
	r, err := kvDB.Get(ctx, dbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(r.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(ctx, dbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	dbDesc := desc.GetDatabase()
	var scDesc sqlbase.SchemaDescriptor
	for _, v := range dbDesc.Schemas {
		if v.Name == "t" {
			scDesc = v
		}
	}

	tbNameKey := sqlbase.MakeNameMetadataKey(scDesc.ID, "kv")
	gr, err := kvDB.Get(ctx, tbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf(`table "kv" does not exist`)
	}
	tbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	ts, err := kvDB.GetProtoTs(ctx, tbDescKey, desc)
	if err != nil {
		t.Fatal(err)
	}
	tbDesc := desc.Table(ts)

	// Add a zone config for both the table and database.
	cfg := config.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, dbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
	if err := zoneExists(sqlDB, &cfg, dbDesc.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`ALTER SCHEMA t RENAME TO a`); !testutils.IsError(err,
		`pq: cannot rename schema because view t.temp depends on table \"kv\"`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`ALTER SCHEMA t RENAME TO ""`); !testutils.IsError(err,
		`empty Schema name`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`ALTER SCHEMA a RENAME TO zbdb_internal`); !testutils.IsError(err,
		`pq: schema \"zbdb_internal\" collides with builtin schema's name`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`ALTER SCHEMA zbdb_internal RENAME TO test`); !testutils.IsError(err,
		`schema \"zbdb_internal\" collides with builtin schema's name`) {
		t.Fatal(err)
	}
}
func TestAlterSchemaWithoutSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Disable the asynchronous path so that the table data left
			// behind is not cleaned up.
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
USE t;
CREATE SCHEMA t;
CREATE TABLE t.t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
`); err != nil {
		t.Fatal(err)
	}

	dbNameKey := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "t")
	r, err := kvDB.Get(ctx, dbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(r.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(ctx, dbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	dbDesc := desc.GetDatabase()
	var scDesc sqlbase.SchemaDescriptor
	for _, v := range dbDesc.Schemas {
		if v.Name == "t" {
			scDesc = v
		}
	}

	tbNameKey := sqlbase.MakeNameMetadataKey(scDesc.ID, "kv")
	gr, err := kvDB.Get(ctx, tbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf(`table "kv" does not exist`)
	}
	tbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	ts, err := kvDB.GetProtoTs(ctx, tbDescKey, desc)
	if err != nil {
		t.Fatal(err)
	}
	tbDesc := desc.Table(ts)

	// Add a zone config for both the table and database.
	cfg := config.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, dbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
	if err := zoneExists(sqlDB, &cfg, dbDesc.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`ALTER SCHEMA t RENAME TO a`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`ALTER SCHEMA t RENAME TO ""`); !testutils.IsError(err,
		`empty Schema name`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`ALTER SCHEMA a RENAME TO zbdb_internal`); !testutils.IsError(err,
		`pq: schema \"zbdb_internal\" collides with builtin schema's name`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`ALTER SCHEMA zbdb_internal RENAME TO test`); !testutils.IsError(err,
		`schema \"zbdb_internal\" collides with builtin schema's name`) {
		t.Fatal(err)
	}
}

func TestAlterView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = "test"
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE foo (a int, b int, c int)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE view v as select a,b,c from foo`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER view v as select a,b from foo`); err != nil {
		t.Fatal(err)
	}
}
