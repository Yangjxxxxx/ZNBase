package logictesticl_test

import (
	"context"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/sql/tests"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestAlterPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		create table tt(a INT PRIMARY KEY, b INT)
        PARTITION BY LIST(a)(
        PARTITION p1 VALUES IN(1, 3, 5),
        PARTITION p2 VALUES IN(2, 4, 6)
        );
        ALTER TABLE tt DROP CONSTRAINT "primary";
        insert into tt values(1,2);
	`); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Minute)
	if _, err := db.Exec("select * from [partition p1] of tt;"); err != nil {
		t.Fatal(err)
	}
	row := db.QueryRow("select * from [partition p1] of tt;")
	var a, b string
	if err := row.Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if a != "1" || b != "2" {
		t.Fatal("TestAlterPrimaryKey fail")
	}
}
