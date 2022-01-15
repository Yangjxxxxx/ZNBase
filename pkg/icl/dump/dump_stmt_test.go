package dump

import (
	"fmt"
	"testing"

	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestDumpParser1(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		sql string
	}{
		{`DUMP TABLE customers TO SST 'http://localhost/f1'`},
		{`DUMP TABLE foo TO SST 'http://localhost/f1' AS OF SYSTEM TIME '1' INCREMENTAL FROM 'baz'`},
		{`DUMP TABLE foo TO SST 'http://localhost/f2' INCREMENTAL FROM 'bar'`},
		{`DUMP DATABASE foo TO SST 'http://localhost/f3'`},
		{`DUMP DATABASE foo, baz TO SST 'http://localhost/f4'`},
		{`DUMP DATABASE foo TO SST 'http://localhost/f5' AS OF SYSTEM TIME '1' INCREMENTAL FROM 'baz'`},

		{`DUMP TO CSV 'http://localhost/f6' FROM SELECT * FROM customers`},
		{`DUMP TO CSV 'http://localhost/fa' FROM TABLE a`},
		{`DUMP TO CSV 'http://localhost/fb' FROM SELECT * FROM b`},
		{`DUMP TO CSV 'http://localhost/f7' FROM SELECT * FROM a WITH delimiter = '|'`},
		{`DUMP TO CSV 'http://localhost/f8' FROM SELECT a, sum(b) FROM c WHERE d = 1 ORDER BY sum(b) DESC LIMIT 10 WITH delimiter = '|'`},
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
func TestFormatTableName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		stmt     string
		expected string
	}{
		{`DUMP TO CSV 'http://localhost/customers' FROM select * from customers`,
			`DUMP TO CSV 'http://localhost/customers' FROM SELECT * FROM xoxoxo`},
		//		{`DUMP customers TO CSV 'http://localhost/customers'`,
		//			`DUMP TABLE xoxoxo TO CSV 'http://localhost/customers'`},
		//	{`BACKUP TABLE tpcc.customer TO 'http://localhost/customers'`,
		//		`BACKUP TABLE tpcc.customer TO 'http://localhost/customers'`,
		//	},
	}

	f := tree.NewFmtCtx(tree.FmtSimple)
	f.SetReformatTableNames(func(ctx *tree.FmtCtx, _ *tree.TableName) {
		ctx.WriteString("xoxoxo")
	})

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.stmt, false)
			if err != nil {
				t.Fatal(err)
			}
			f.Reset()
			f.FormatNode(stmt.AST)
			stmtStr := f.String()
			if stmtStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, stmtStr)
			}
		})
	}
}
