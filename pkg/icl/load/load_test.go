// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/CCL.txt

package load_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/icl/load"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/testutils/testcluster"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/workload"
	"github.com/znbasedb/znbase/pkg/workload/bank"
)

func bankBuf(numAccounts int) *bytes.Buffer {
	bankData := bank.FromRows(numAccounts).Tables()[0]
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name, bankData.Schema)
	for rowIdx := 0; rowIdx < bankData.InitialRows.NumBatches; rowIdx++ {
		for _, row := range bankData.InitialRows.Batch(rowIdx) {
			rowTuple := strings.Join(workload.StringTuple(row), `,`)
			fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, rowTuple)
		}
	}
	return &buf
}
func TestImportChunking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Generate at least 2 chunks.
	const chunkSize = 1024 * 500
	numAccounts := int(chunkSize / 100 * 2)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	if _, err := tc.Conns[0].Exec("CREATE DATABASE data"); err != nil {
		t.Fatal(err)
	}

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	desc, err := load.Load(ctx, tc.Conns[0], bankBuf(numAccounts), "data", "nodelocal://"+dir, ts, chunkSize, dir, dir)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(desc.Files) < 2 {
		t.Errorf("expected at least 2 ranges")
	}
}

func TestImportOutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	if _, err := tc.Conns[0].Exec("CREATE DATABASE data"); err != nil {
		t.Fatal(err)
	}
	bankData := bank.FromRows(2).Tables()[0]
	row1 := workload.StringTuple(bankData.InitialRows.Batch(0)[0])
	row2 := workload.StringTuple(bankData.InitialRows.Batch(1)[0])

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE %s %s;\n", bankData.Name, bankData.Schema)
	// Intentionally write the rows out of order.
	fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row2, `,`))
	fmt.Fprintf(&buf, "INSERT INTO %s VALUES (%s);\n", bankData.Name, strings.Join(row1, `,`))

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	_, err := load.Load(ctx, tc.Conns[0], &buf, "data", "nodelocal:///foo", ts, 0, dir, dir)
	if !testutils.IsError(err, "out of order row") {
		t.Fatalf("expected out of order row, got: %+v", err)
	}
}

func BenchmarkLoad(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(b)
	defer cleanup()

	tc := testcluster.StartTestCluster(b, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	if _, err := tc.Conns[0].Exec("CREATE DATABASE data"); err != nil {
		b.Fatal(err)
	}

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	buf := bankBuf(b.N)
	b.SetBytes(int64(buf.Len() / b.N))
	b.ResetTimer()
	if _, err := load.Load(ctx, tc.Conns[0], buf, "data", dir, ts, 0, dir, dir); err != nil {
		b.Fatalf("%+v", err)
	}
}

func TestLoadinto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows' FROM SELECT * FROM foo`)
	sqlDB.Exec(t, `drop table foo`)

	content, err := ioutil.ReadFile(filepath.Join(dir, "shows", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}

	sqlDB.Exec(t, `create table test (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `LOAD INTO test CSV DATA ('nodelocal:///shows/n1.0.csv')`)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := string(content), str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestLoadintoNoPrimarykey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int , x int, y int, z int)`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///shows" FROM TABLE foo`)
	sqlDB.Exec(t, `drop table foo`)

	sqlDB.Exec(t, `create table test (i int , x int, y int, z int)`)

	sqlDB.Exec(t, `insert into test  values (1, 112, 3, 14), (2, 222, 2, 24), (5, 2, 1, 0)`)

	sqlDB.Exec(t, `LOAD INTO test CSV DATA ('nodelocal:///shows/n1.0.csv') `)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,3,14\n2,22,2,24\n3,32,1,34\n1,112,3,14\n2,222,2,24\n5,2,1,0\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestLoadintoMultiCsv(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 4, 14), (2, 22, 3, 24), (3, 32, 2, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows' FROM SELECT * FROM foo`)
	sqlDB.Exec(t, `drop table foo`)

	sqlDB.Exec(t, `create table foo1 (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo1 values (4, 12, 4, 14), (5, 22, 3, 24), (6, 32, 2, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows1' FROM SELECT * FROM foo1`)
	sqlDB.Exec(t, `drop table foo1`)

	sqlDB.Exec(t, `create table test (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `LOAD INTO test CSV DATA ('nodelocal:///shows/n1.0.csv','nodelocal:///shows1/n1.0.csv')`)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(a, b, c, d)
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,4,14\n2,22,3,24\n3,32,2,34\n4,12,4,14\n5,22,3,24\n6,32,2,34\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestLoadintodeLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///shows" FROM TABLE foo With delimiter = e'\t';`)
	sqlDB.Exec(t, `drop table foo`)

	content, err := ioutil.ReadFile(filepath.Join(dir, "shows", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(content))

	sqlDB.Exec(t, `create table test (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `LOAD INTO test CSV DATA ('nodelocal:///shows/n1.0.csv') WITH delimiter = e'\t' `)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,3,14\n2,22,2,24\n3,32,1,34\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestLoadintoskip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///shows" FROM TABLE foo;`)
	sqlDB.Exec(t, `drop table foo`)

	content, err := ioutil.ReadFile(filepath.Join(dir, "shows", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(content))

	sqlDB.Exec(t, `create table test (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `LOAD INTO test CSV DATA ('nodelocal:///shows/n1.0.csv')  WITH skip = '1' `)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "2,22,2,24\n3,32,1,34\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestLoadintoNULL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int);`)
	sqlDB.Exec(t, `insert into foo values (1), (2), (3);`)
	sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///shows" FROM TABLE foo WITH NULLAS = ' ' `)
	sqlDB.Exec(t, `drop table foo`)

	content, err := ioutil.ReadFile(filepath.Join(dir, "shows", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(content))

	sqlDB.Exec(t, `create table test (i int primary key, x int)`)
	sqlDB.Exec(t, `LOAD INTO test CSV DATA ('nodelocal:///shows/n1.0.csv')  WITH NULLif = ' ' `)
}

type Foo struct {
	x int
	y int
	z int
	i int
}

func TestLoadintoONLY(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	res := sqlDB.Query(t, `SELECT * FROM foo`)
	columns, error := res.Columns()
	if error != nil {
		t.Fatal(error)
	}
	if a, e := columns, []string{
		"i", "x", "y", "z",
	}; !reflect.DeepEqual(a, e) {
		t.Errorf("got %v, want %v", a, e)
	}

	res1 := sqlDB.Query(t, `SELECT * FROM foo`)
	s := []Foo{}
	for res1.Next() {
		var a int
		var b int
		var c int
		var d int
		error1 := res1.Scan(&a, &b, &c, &d)
		if error1 != nil {
			t.Fatal(error1)
		}
		f := Foo{
			a,
			b,
			c,
			d,
		}
		s = append(s, f)
	}
	fmt.Println(s)
	if a, e := s, []Foo{
		{1, 12, 3, 14}, {2, 22, 2, 24}, {3, 32, 1, 34},
	}; !reflect.DeepEqual(a, e) {
		t.Errorf("got %v, want %v", a, e)
	}
}

func TestLoadwithRejectedType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	const nodes = 3
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `create table foo (i int, x int, y int, z int)`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 4, 14), (2, 22, 3, 24), (3, 32, 2, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows' FROM table foo`)
	sqlDB.Exec(t, `drop table foo`)

	sqlDB.Exec(t, `create table foo1 (i string, x string, y string ,z string)`)
	sqlDB.Exec(t, `insert into foo1 values ('a', 'b', 'c','d')`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows1' FROM table foo1`)
	sqlDB.Exec(t, `drop table foo1`)

	sqlDB.Exec(t, `LOAD table test(i int, x int, y int, z int) CSV DATA ('nodelocal:///shows/n1.0.csv','nodelocal:///shows1/n1.0.csv') with save_rejected`)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(a, b, c, d)
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,4,14\n2,22,3,24\n3,32,2,34\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestLoadwithRejectedNum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	const nodes = 3
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `create table foo (i int, x int, y int, z int)`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 4, 14), (2, 22, 3, 24), (3, 32, 2, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows' FROM table foo`)
	sqlDB.Exec(t, `drop table foo`)

	sqlDB.Exec(t, `create table foo1 (i int, x int, y int)`)
	sqlDB.Exec(t, `insert into foo1 values (1, 2, 3)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows1' FROM table foo1`)
	sqlDB.Exec(t, `drop table foo1`)

	sqlDB.Exec(t, `LOAD table test(i int, x int, y int, z int) CSV DATA ('nodelocal:///shows/n1.0.csv','nodelocal:///shows1/n1.0.csv') with save_rejected`)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(a, b, c, d)
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,4,14\n2,22,3,24\n3,32,2,34\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestLoadwithRejectedRows(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	const nodes = 1
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `create table foo (i int, x int, y int, z int)`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 4, 14), (2, 22, 3, 24), (3, 32, 2, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows' FROM table foo`)
	sqlDB.Exec(t, `drop table foo`)

	sqlDB.Exec(t, `create table foo1 (i int, x int, y int)`)
	sqlDB.Exec(t, `insert into foo1 values (1, 2, 3),(4, 5, 6)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows1' FROM table foo1`)
	sqlDB.Exec(t, `drop table foo1`)

	sqlDB.Exec(t, `LOAD table test(i int, x int, y int, z int) CSV DATA ('nodelocal:///shows/n1.0.csv','nodelocal:///shows1/n1.0.csv') with save_rejected,rejectrows = '2'`)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(a, b, c, d)
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,4,14\n2,22,3,24\n3,32,2,34\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}

	sqlDB.Exec(t, `drop table test`)
	sqlDB.ExpectErr(
		t, "too many parse errors,please check if there is a problem with the imported data file",
		`LOAD table test(i int, x int, y int, z int) CSV DATA ('nodelocal:///shows/n1.0.csv','nodelocal:///shows1/n1.0.csv') with save_rejected,rejectrows = '1'`,
	)
}

func TestLoadwithRejectedAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	const nodes = 3
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `create table foo (i int, x int, y int, z int)`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 4, 14), (2, 22, 3, 24), (3, 32, 2, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows' FROM table foo`)
	sqlDB.Exec(t, `drop table foo`)

	sqlDB.Exec(t, `create table foo1 (i int, x int, y int)`)
	sqlDB.Exec(t, `insert into foo1 values (1, 2, 3),(4, 5, 6)`)
	sqlDB.Exec(t, `DUMP TO CSV 'nodelocal:///shows1' FROM table foo1`)
	sqlDB.Exec(t, `drop table foo1`)

	sqlDB.Exec(t, `LOAD table test(i int, x int, y int, z int) CSV DATA ('nodelocal:///shows/n1.0.csv','nodelocal:///shows1/n1.0.csv') with save_rejected,rejectrows = '2',rejectaddress = 'nodelocal:///reject'`)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(a, b, c, d)
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,4,14\n2,22,3,24\n3,32,2,34\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestLoadWithRowdeLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)
	sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///shows" FROM TABLE foo With rowdelimiter = e'\t';`)
	sqlDB.Exec(t, `drop table foo`)

	content, err := ioutil.ReadFile(filepath.Join(dir, "shows", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(content))

	sqlDB.Exec(t, `LOAD table test(i int primary key, x int, y int, z int, index (y)) CSV DATA ('nodelocal:///shows/n1.0.csv') WITH rowdelimiter = e'\t' `)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d int
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		str += fmt.Sprintf("%d,%d,%d,%d", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,3,14\n2,22,2,24\n3,32,1,34\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}

	sqlDB.Exec(t, `drop table test`)
	sqlDB.ExpectErr(
		t, "pq: nodelocal:///shows/n1.0.csv: delimiter and rowdelimiter should be different",
		`LOAD table test(i int primary key, x int, y int, z int, index (y)) CSV DATA ('nodelocal:///shows/n1.0.csv') with rowdelimiter = ','`,
	)
}

func TestLoadWithEscaped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, s string, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 'a,b'), (2, 22, 2, 'a,b'), (3, 32, 1, 'a,b')`)
	sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///shows" FROM TABLE foo With csv_escaped_by = '\'`)
	sqlDB.Exec(t, `drop table foo`)

	content, err := ioutil.ReadFile(filepath.Join(dir, "shows", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(content))

	sqlDB.Exec(t, `LOAD table test(i int primary key, x int, y int, s string, index (y)) CSV DATA ('nodelocal:///shows/n1.0.csv') WITH csv_escaped_by = '\'`)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d string
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		str += fmt.Sprintf("%d,%d,%d,%v", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,3,a,b\n2,22,2,a,b\n3,32,1,a,b\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}

	sqlDB.Exec(t, `drop table test`)
	sqlDB.ExpectErr(
		t, "pq: nodelocal:///shows/n1.0.csv: delimiter and escape should be different",
		`LOAD table test(i int primary key, x int, y int, z int, index (y)) CSV DATA ('nodelocal:///shows/n1.0.csv') with csv_escaped_by = ','`,
	)
}

func TestLoadWithEnclosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, s string, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 'a,b'), (2, 22, 2, 'a,b'), (3, 32, 1, 'a,b')`)
	sqlDB.Exec(t, `DUMP TO CSV "nodelocal:///shows" FROM TABLE foo`)
	sqlDB.Exec(t, `drop table foo`)

	content, err := ioutil.ReadFile(filepath.Join(dir, "shows", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(content))

	sqlDB.Exec(t, `LOAD table test(i int primary key, x int, y int, s string, index (y)) CSV DATA ('nodelocal:///shows/n1.0.csv') WITH csv_enclosed_by = '"'`)

	res := sqlDB.Query(t, `SELECT * FROM test`)
	var str string
	for res.Next() {
		var a int
		var b int
		var c int
		var d string
		err := res.Scan(&a, &b, &c, &d)
		if err != nil {
			t.Fatal(err)
		}
		str += fmt.Sprintf("%d,%d,%d,%v", a, b, c, d)
		str += "\n"
	}

	if expected, got := "1,12,3,a,b\n2,22,2,a,b\n3,32,1,a,b\n", str; expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}

	sqlDB.Exec(t, `drop table test`)
	sqlDB.ExpectErr(
		t, "pq: nodelocal:///shows/n1.0.csv: delimiter and encloser should be different",
		`LOAD table test(i int primary key, x int, y int, z int, index (y)) CSV DATA ('nodelocal:///shows/n1.0.csv') with csv_enclosed_by = ','`,
	)
}
