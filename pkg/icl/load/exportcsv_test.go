// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/CCL.txt

package load_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/testutils/testcluster"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/workload"
	"github.com/znbasedb/znbase/pkg/workload/bank"
)

func setupExportableBank(t *testing.T, nodes, rows int) (*sqlutils.SQLRunner, string, func()) {
	ctx := context.Background()
	dir, cleanupDir := testutils.TempDir(t)

	tc := testcluster.StartTestCluster(t, nodes,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir, UseDatabase: "test"}},
	)
	conn := tc.Conns[0]
	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, "CREATE DATABASE test")

	wk := bank.FromRows(rows)
	if _, err := workload.Setup(ctx, conn, wk, 100, 3); err != nil {
		t.Fatal(err)
	}

	config.TestingSetupZoneConfigHook(tc.Stopper())
	v, err := tc.Servers[0].DB().Get(context.TODO(), keys.DescIDGenerator)
	if err != nil {
		t.Fatal(err)
	}
	last := uint32(v.ValueInt())
	zoneConfig := config.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(5000)
	config.TestingSetZoneConfig(last+1, zoneConfig)
	if err := workload.Split(ctx, conn, wk.Tables()[0], 1 /* concurrency */); err != nil {
		t.Fatal(err)
	}
	db.Exec(t, "ALTER TABLE bank SCATTER")
	db.Exec(t, "SELECT 'force a scan to repopulate range cache' FROM [SELECT count(*) FROM bank]")

	return db, dir, func() {
		tc.Stopper().Stop(ctx)
		cleanupDir()
	}
}

func TestExportImportBank(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, dir, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	// Add some unicode to prove FmtExport works as advertised.
	db.Exec(t, "UPDATE bank SET payload = payload || '✅' WHERE id = 5")
	db.Exec(t, "UPDATE bank SET payload = NULL WHERE id % 2 = 0")

	chunkSize := 13
	for _, null := range []string{"", "NULL", `\N`} {
		nullAs, nullIf := "", ", nullif = ''"
		nullAs = fmt.Sprintf(", nullas = '%s'", null)
		nullIf = fmt.Sprintf(", nullif = '%s'", null)
		t.Run("null="+null, func(t *testing.T) {
			var files []string

			var asOf string
			db.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&asOf)
			for _, row := range db.QueryStr(t,
				fmt.Sprintf(`DUMP TO CSV 'nodelocal:///t'
					FROM SELECT * FROM bank AS OF SYSTEM TIME %s
					WITH chunk_rows = $1, delimiter = '|' %s`, asOf, nullAs), chunkSize,
			) {
				files = append(files, row[1])
				f, err := ioutil.ReadFile(filepath.Join(dir, "t", row[1]))
				if err != nil {
					t.Fatal(err)
				}
				t.Log(string(f))
			}

			schema := bank.FromRows(1).Tables()[0].Schema
			fileList := "'nodelocal:///t/" + strings.Join(files, "', 'nodelocal:///t/") + "'"
			db.Exec(t, fmt.Sprintf(`LOAD TABLE bank3 %s CSV DATA (%s) WITH delimiter = '|'%s`, schema, fileList, nullIf))

			db.CheckQueryResults(t,
				fmt.Sprintf(`SELECT * FROM bank AS OF SYSTEM TIME %s ORDER BY id`, asOf), db.QueryStr(t, `SELECT * FROM bank3 ORDER BY id`),
			)
			//db.CheckQueryResults(t,
			//	`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank3`, db.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank`),
			//)
			db.Exec(t, "DROP TABLE bank3")
		})
	}
}

func TestMultiNodeExportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	nodes := 5
	exportRows := 100
	db, _, cleanup := setupExportableBank(t, nodes, exportRows*2)
	defer cleanup()

	maxTries := 10
	// we might need to retry if our table didn't actually scatter enough.
	for tries := 0; tries < maxTries; tries++ {
		chunkSize := 13
		rows := db.Query(t,
			`DUMP TO CSV 'nodelocal:///t'  FROM SELECT * FROM bank WHERE id >= $1 and id < $2 WITH chunk_rows = $3`,
			10, 10+exportRows, chunkSize,
		)

		files, totalRows, totalBytes := 0, 0, 0
		nodesSeen := make(map[string]bool)
		for rows.Next() {
			queryname, filename, count, bytes := "", "", 0, 0
			if err := rows.Scan(&queryname, &filename, &count, &bytes); err != nil {
				t.Fatal(err)
			}
			files++
			if count > chunkSize {
				t.Fatalf("expected no chunk larger than %d, got %d", chunkSize, count)
			}
			totalRows += count
			totalBytes += bytes
			nodesSeen[strings.SplitN(filename, ".", 2)[0]] = true
		}
		if totalRows != exportRows {
			t.Fatalf("Expected %d rows, got %d", exportRows, totalRows)
		}
		if expected := exportRows / chunkSize; files < expected {
			t.Fatalf("expected at least %d files, got %d", expected, files)
		}
		if len(nodesSeen) < 2 {
			// table isn't as scattered as we expected, but we can try again.
			if tries < maxTries {
				continue
			}
			t.Fatalf("expected files from %d nodes, got %d: %v", 2, len(nodesSeen), nodesSeen)
		}
		break
	}
}

func TestExportJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t AS VALUES (1, 2)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///join' FROM SELECT * FROM t, t as u`)
}

func TestExportOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///order' from select * from foo order by y asc limit 2`)
	content, err := ioutil.ReadFile(filepath.Join(dir, "order", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := "3,32,1,34\n2,22,2,24\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestExportShow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///show' FROM SELECT * FROM [SHOW DATABASES]`)
	content, err := ioutil.ReadFile(filepath.Join(dir, "show", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := "defaultdb\npostgres\nsystem\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestInvaildSymbol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE "‘数据库""]}'#$%^&*(";`)
	sqlDB.Exec(t, `CREATE SCHEMA "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~";`)
	sqlDB.Exec(t, `CREATE TABLE "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~"."sdd!!'"("dsfdasrgg*&'" INT PRIMARY KEY,column1 STRING);`)
	sqlDB.Exec(t, `INSERT INTO "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~"."sdd!!'" VALUES (1,'a'),(2,'b')`)

	t.Run("test dump database to csv using invaild symbol", func(t *testing.T) {
		sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///database_csv" FROM DATABASE "‘数据库""]}'#$%^&*(";`)
	})

	t.Run("test dump database to sql using invaild symbol", func(t *testing.T) {
		sqlDB.Exec(t, `DUMP TO SQL "nodelocal:///database_sql" FROM DATABASE "‘数据库""]}'#$%^&*(";`)
	})

	t.Run("test dump schema to csv using invaild symbol", func(t *testing.T) {
		sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///schema_csv" FROM SCHEMA "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~";`)
	})

	t.Run("test dump schema to sql using invaild symbol", func(t *testing.T) {
		sqlDB.Exec(t, `DUMP TO SQL "nodelocal:///schema_sql" FROM SCHEMA "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~";`)
	})

	t.Run("test dump table to csv using invaild symbol", func(t *testing.T) {
		sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///table_csv" FROM TABLE  "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~"."sdd!!'";`)
		sqlDB.Exec(t, `TRUNCATE TABLE "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~"."sdd!!'"`)
		sqlDB.Query(t, `LOAD INTO "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~"."sdd!!'" CSV DATA ('nodelocal:///table_csv/n1.0.csv')`)
	})

	t.Run("test dump table to sql using invaild symbol", func(t *testing.T) {
		sqlDB.Exec(t, `DUMP TO SQL "nodelocal:///table_sql" FROM TABLE  "‘数据库""]}'#$%^&*("."'sdddd~/*&$#@!~"."sdd!!'";`)
	})
}

func TestExportRowdelimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t(a int,b int)`)
	sqlDB.Exec(t, `insert into t values(1,2),(3,4)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///row' from table t with rowdelimiter = e'\t'`)
	content, err := ioutil.ReadFile(filepath.Join(dir, "row", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := "1,2\t3,4\t", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}

	sqlDB.ExpectErr(
		t, "pq: delimiter and rowdelimiter should be different",
		`DUMP TO CSV 'nodelocal:///escape1' from table t with rowdelimiter = ','`,
	)
}

func TestExportEscaped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t(a int,b string)`)
	sqlDB.Exec(t, `insert into t values(1,'aaa,bbb')`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///escape' from table t with csv_escaped_by = '\'`)

	content, err := ioutil.ReadFile(filepath.Join(dir, "escape", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := "1,aaa\\,bbb\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}

	sqlDB.ExpectErr(
		t, "pq: delimiter and escape should be different",
		`DUMP TO CSV 'nodelocal:///escape1' from table t with csv_escaped_by = ','`,
	)
}
