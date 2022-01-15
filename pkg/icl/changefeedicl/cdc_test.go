// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase-go/zbdb"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/icl/changefeedicl/cdctest"
	"github.com/znbasedb/znbase/pkg/icl/utilicl"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/server"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql/distsql"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

func TestCDCBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`d.public.foo: [2]->{"after": {"a": 2, "b": "b"}}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [2]->{"after": {"a": 2, "b": "c"}}`,
			`d.public.foo: [3]->{"after": {"a": 3, "b": "d"}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}
func TestCDCDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "updated"}, "before": null}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": {"a": 1, "b": "a"}, "before": null}`,
			`d.public.foo: [2]->{"after": {"a": 2, "b": "b"}, "before": null}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [2]->{"after": {"a": 2, "b": "c"}, "before": {"a": 2, "b": "b"}}`,
			`d.public.foo: [3]->{"after": {"a": 3, "b": "d"}, "before": null}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": null, "before": {"a": 1, "b": "a"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'new a')`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": {"a": 1, "b": "new a"}, "before": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCEnvelope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)

		t.Run(`envelope=row`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope='row'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`d.public.foo: [1]->{"a": 1, "b": "a"}`})
		})
		t.Run(`envelope=deprecated_row`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope='deprecated_row'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`d.public.foo: [1]->{"a": 1, "b": "a"}`})
		})
		t.Run(`envelope=key_only`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope='key_only'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`d.public.foo: [1]->`})
		})
		t.Run(`envelope=wrapped`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope='wrapped'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`d.public.foo: [1]->{"after": {"a": 1, "b": "a"}}`})
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (2, 'b')`)

		fooAndBar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		defer closeFeed(t, fooAndBar)

		assertPayloads(t, fooAndBar, []string{
			`d.public.foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`d.public.bar: [2]->{"after": {"a": 2, "b": "b"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		// To make sure that these timestamps are after 'before' and before
		// 'after', throw a couple sleeps around them. We round timestamps to
		// Microsecond granularity for Postgres compatibility, so make the
		// sleeps 10x that.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before')`)
		time.Sleep(10 * time.Microsecond)

		var tsLogical string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsLogical)
		var tsClock time.Time
		sqlDB.QueryRow(t, `SELECT clock_timestamp()`).Scan(&tsClock)

		time.Sleep(10 * time.Microsecond)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after')`)

		fooLogical := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, tsLogical)
		defer closeFeed(t, fooLogical)
		assertPayloads(t, fooLogical, []string{
			`d.public.foo: [2]->{"after": {"a": 2, "b": "after"}}`,
		})

		nanosStr := strconv.FormatInt(tsClock.UnixNano(), 10)
		fooNanosStr := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, nanosStr)
		defer closeFeed(t, fooNanosStr)
		assertPayloads(t, fooNanosStr, []string{
			`d.public.foo: [2]->{"after": {"a": 2, "b": "after"}}`,
		})

		timeStr := tsClock.Format(`2006-01-02 15:04:05.999999`)
		fooString := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, timeStr)
		defer closeFeed(t, fooString)
		assertPayloads(t, fooString, []string{
			`d.public.foo: [2]->{"after": {"a": 2, "b": "after"}}`,
		})

		// Check that the cursor is properly hooked up to the job statement
		// time. The sinkless tests currently don't have a way to get the
		// statement timestamp, so only verify this for enterprise.
		if e, ok := fooLogical.(*cdctest.TableFeed); ok {
			var bytes []byte
			sqlDB.QueryRow(t, `SELECT payload FROM system.jobs WHERE id=$1`, e.JobID).Scan(&bytes)
			var payload jobspb.Payload
			require.NoError(t, protoutil.Unmarshal(bytes, &payload))
			require.Equal(t, parseTimeToHLC(t, tsLogical), payload.GetChangefeed().StatementTime)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH updated, resolved`)
		defer closeFeed(t, foo)

		// Grab the first non resolved-timestamp row.
		var row0 *cdctest.TestFeedMessage
		for {
			var err error
			row0, err = foo.Next()
			assert.NoError(t, err)
			if len(row0.Value) > 0 {
				break
			}
		}

		// If this CDC uses jobs (and thus stores a ChangefeedDetails), get
		// the statement timestamp from row0 and verify that they match. Otherwise,
		// just skip the row.
		if !strings.Contains(t.Name(), `sinkless`) {
			d, err := foo.(*cdctest.TableFeed).Details()
			assert.NoError(t, err)
			expected := `{"after": {"a": 0}, "updated": "` + d.StatementTime.AsOfSystemTime() + `"}`
			assert.Equal(t, expected, string(row0.Value))
		}

		// Assert the remaining key using assertPayloads, since we know the exact
		// timestamp expected.
		var ts1 string
		if err := zbdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
			return tx.QueryRow(
				`INSERT INTO foo VALUES (1) RETURNING cluster_logical_timestamp()`,
			).Scan(&ts1)
		}); err != nil {
			t.Fatal(err)
		}
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": {"a": 1}, "updated": "` + ts1 + `"}`,
		})

		// Check that we eventually get a resolved timestamp greater than ts1.
		parsed := parseTimeToHLC(t, ts1)
		for {
			if resolved := expectResolvedTimestamp(t, foo); parsed.Less(resolved) {
				break
			}
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	// Most poller tests use sinklessTest, but this one uses enterpriseTest
	// because we get an extra assertion out of it.
	t.Run(`poller`, pollerTest(enterpriseTest, testFn))
}

func TestCDCResolvedFrequency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		const freq = 10 * time.Millisecond
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved=$1`, freq.String())
		defer closeFeed(t, foo)

		// We get each resolved timestamp notification once in each partition.
		// Grab the first `2 * #partitions`, sort because we might get all from
		// one partition first, and compare the first and last.
		resolved := make([]hlc.Timestamp, 2*len(foo.Partitions()))
		for i := range resolved {
			resolved[i] = expectResolvedTimestamp(t, foo)
		}
		sort.Slice(resolved, func(i, j int) bool { return resolved[i].Less(resolved[j]) })
		first, last := resolved[0], resolved[len(resolved)-1]

		if d := last.GoTime().Sub(first.GoTime()); d < freq {
			t.Errorf(`expected %s between resolved timestamps, but got %s`, freq, d)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

// Test how CDCs react to schema changes that do not require a backfill
// operation.
func TestCDCSchemaChangeNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		// Schema changes that predate the CDC.
		t.Run(`historical`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE historical (a INT PRIMARY KEY, b STRING DEFAULT 'before')`)
			var start string
			sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&start)
			sqlDB.Exec(t, `INSERT INTO historical (a, b) VALUES (0, '0')`)
			sqlDB.Exec(t, `INSERT INTO historical (a) VALUES (1)`)
			sqlDB.Exec(t, `ALTER TABLE historical ALTER COLUMN b SET DEFAULT 'after'`)
			sqlDB.Exec(t, `INSERT INTO historical (a) VALUES (2)`)
			sqlDB.Exec(t, `ALTER TABLE historical ADD COLUMN c INT`)
			sqlDB.Exec(t, `INSERT INTO historical (a) VALUES (3)`)
			sqlDB.Exec(t, `INSERT INTO historical (a, c) VALUES (4, 14)`)
			historical := feed(t, f, `CREATE CHANGEFEED FOR historical WITH cursor=$1`, start)
			defer closeFeed(t, historical)
			assertPayloads(t, historical, []string{
				`d.public.historical: [0]->{"after": {"a": 0, "b": "0"}}`,
				`d.public.historical: [1]->{"after": {"a": 1, "b": "before"}}`,
				`d.public.historical: [2]->{"after": {"a": 2, "b": "after"}}`,
				`d.public.historical: [3]->{"after": {"a": 3, "b": "after", "c": null}}`,
				`d.public.historical: [4]->{"after": {"a": 4, "b": "after", "c": 14}}`,
			})
		})

		t.Run(`add column`, func(t *testing.T) {
			// NB: the default is a nullable column
			sqlDB.Exec(t, `CREATE TABLE add_column (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column VALUES (1)`)
			addColumn := feed(t, f, `CREATE CHANGEFEED FOR add_column`)
			defer closeFeed(t, addColumn)
			assertPayloads(t, addColumn, []string{
				`d.public.add_column: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column ADD COLUMN b STRING`)
			sqlDB.Exec(t, `INSERT INTO add_column VALUES (2, '2')`)
			assertPayloads(t, addColumn, []string{
				`d.public.add_column: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
		})

		t.Run(`rename column`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE rename_column (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO rename_column VALUES (1, '1')`)
			renameColumn := feed(t, f, `CREATE CHANGEFEED FOR rename_column`)
			defer closeFeed(t, renameColumn)
			assertPayloads(t, renameColumn, []string{
				`d.public.rename_column: [1]->{"after": {"a": 1, "b": "1"}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE rename_column RENAME COLUMN b TO c`)
			sqlDB.Exec(t, `INSERT INTO rename_column VALUES (2, '2')`)
			assertPayloads(t, renameColumn, []string{
				`d.public.rename_column: [2]->{"after": {"a": 2, "c": "2"}}`,
			})
		})

		t.Run(`add default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_default (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO add_default (a, b) VALUES (1, '1')`)
			addDefault := feed(t, f, `CREATE CHANGEFEED FOR add_default`)
			defer closeFeed(t, addDefault)
			sqlDB.Exec(t, `ALTER TABLE add_default ALTER COLUMN b SET DEFAULT 'd'`)
			sqlDB.Exec(t, `INSERT INTO add_default (a) VALUES (2)`)
			assertPayloads(t, addDefault, []string{
				`d.public.add_default: [1]->{"after": {"a": 1, "b": "1"}}`,
				`d.public.add_default: [2]->{"after": {"a": 2, "b": "d"}}`,
			})
		})

		t.Run(`drop default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_default (a INT PRIMARY KEY, b STRING DEFAULT 'd')`)
			sqlDB.Exec(t, `INSERT INTO drop_default (a) VALUES (1)`)
			dropDefault := feed(t, f, `CREATE CHANGEFEED FOR drop_default`)
			defer closeFeed(t, dropDefault)
			sqlDB.Exec(t, `ALTER TABLE drop_default ALTER COLUMN b DROP DEFAULT`)
			sqlDB.Exec(t, `INSERT INTO drop_default (a) VALUES (2)`)
			assertPayloads(t, dropDefault, []string{
				`d.public.drop_default: [1]->{"after": {"a": 1, "b": "d"}}`,
				`d.public.drop_default: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})

		t.Run(`drop not null`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_notnull (a INT PRIMARY KEY, b STRING NOT NULL)`)
			sqlDB.Exec(t, `INSERT INTO drop_notnull VALUES (1, '1')`)
			dropNotNull := feed(t, f, `CREATE CHANGEFEED FOR drop_notnull`)
			defer closeFeed(t, dropNotNull)
			sqlDB.Exec(t, `ALTER TABLE drop_notnull ALTER b DROP NOT NULL`)
			sqlDB.Exec(t, `INSERT INTO drop_notnull VALUES (2, NULL)`)
			assertPayloads(t, dropNotNull, []string{
				`d.public.drop_notnull: [1]->{"after": {"a": 1, "b": "1"}}`,
				`d.public.drop_notnull: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})

		t.Run(`checks`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE checks (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO checks VALUES (1)`)
			checks := feed(t, f, `CREATE CHANGEFEED FOR checks`)
			defer closeFeed(t, checks)
			sqlDB.Exec(t, `ALTER TABLE checks ADD CONSTRAINT c CHECK (a < 5) NOT VALID`)
			sqlDB.Exec(t, `INSERT INTO checks VALUES (2)`)
			sqlDB.Exec(t, `ALTER TABLE checks VALIDATE CONSTRAINT c`)
			sqlDB.Exec(t, `INSERT INTO checks VALUES (3)`)
			sqlDB.Exec(t, `ALTER TABLE checks DROP CONSTRAINT c`)
			sqlDB.Exec(t, `INSERT INTO checks VALUES (6)`)
			assertPayloads(t, checks, []string{
				`d.public.checks: [1]->{"after": {"a": 1}}`,
				`d.public.checks: [2]->{"after": {"a": 2}}`,
				`d.public.checks: [3]->{"after": {"a": 3}}`,
				`d.public.checks: [6]->{"after": {"a": 6}}`,
			})
		})

		t.Run(`add index`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_index (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (1, '1')`)
			addIndex := feed(t, f, `CREATE CHANGEFEED FOR add_index`)
			defer closeFeed(t, addIndex)
			sqlDB.Exec(t, `CREATE INDEX b_idx ON add_index (b)`)
			sqlDB.Exec(t, `SELECT * FROM add_index@b_idx`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (2, '2')`)
			assertPayloads(t, addIndex, []string{
				`d.public.add_index: [1]->{"after": {"a": 1, "b": "1"}}`,
				`d.public.add_index: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
		})

		t.Run(`unique`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE "unique" (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO "unique" VALUES (1, '1')`)
			unique := feed(t, f, `CREATE CHANGEFEED FOR "unique"`)
			defer closeFeed(t, unique)
			sqlDB.Exec(t, `ALTER TABLE "unique" ADD CONSTRAINT u UNIQUE (b)`)
			sqlDB.Exec(t, `INSERT INTO "unique" VALUES (2, '2')`)
			assertPayloads(t, unique, []string{
				`d.public.unique: [1]->{"after": {"a": 1, "b": "1"}}`,
				`d.public.unique: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
		})

		t.Run(`alter default`, func(t *testing.T) {
			sqlDB.Exec(
				t, `CREATE TABLE alter_default (a INT PRIMARY KEY, b STRING DEFAULT 'before')`)
			sqlDB.Exec(t, `INSERT INTO alter_default (a) VALUES (1)`)
			alterDefault := feed(t, f, `CREATE CHANGEFEED FOR alter_default`)
			defer closeFeed(t, alterDefault)
			sqlDB.Exec(t, `ALTER TABLE alter_default ALTER COLUMN b SET DEFAULT 'after'`)
			sqlDB.Exec(t, `INSERT INTO alter_default (a) VALUES (2)`)
			assertPayloads(t, alterDefault, []string{
				`d.public.alter_default: [1]->{"after": {"a": 1, "b": "before"}}`,
				`d.public.alter_default: [2]->{"after": {"a": 2, "b": "after"}}`,
			})
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCSchemaChangeNoAllowBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO(mrtracy): Re-enable when allow-backfill option is added.")

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		t.Run(`add column not null`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_notnull (a INT PRIMARY KEY)`)
			addColumnNotNull := feed(t, f, `CREATE CHANGEFEED FOR add_column_notnull WITH resolved`)
			defer closeFeed(t, addColumnNotNull)
			sqlDB.Exec(t, `ALTER TABLE add_column_notnull ADD COLUMN b STRING NOT NULL`)
			sqlDB.Exec(t, `INSERT INTO add_column_notnull VALUES (2, '2')`)
			skipResolvedTimestamps(t, addColumnNotNull)
			if _, err := addColumnNotNull.Next(); !testutils.IsError(err, `tables being backfilled`) {
				t.Fatalf(`expected "tables being backfilled" error got: %+v`, err)
			}
		})

		t.Run(`add column with default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_def (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (1)`)
			addColumnDef := feed(t, f, `CREATE CHANGEFEED FOR add_column_def`)
			defer closeFeed(t, addColumnDef)
			assertPayloads(t, addColumnDef, []string{
				`d.public.add_column_def: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_def ADD COLUMN b STRING DEFAULT 'd'`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (2, '2')`)
			if _, err := addColumnDef.Next(); !testutils.IsError(err, `tables being backfilled`) {
				t.Fatalf(`expected "tables being backfilled" error got: %+v`, err)
			}
		})

		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_col_comp (a INT PRIMARY KEY, b INT AS (a + 5) STORED)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp VALUES (1)`)
			addColComp := feed(t, f, `CREATE CHANGEFEED FOR add_col_comp`)
			defer closeFeed(t, addColComp)
			assertPayloads(t, addColComp, []string{
				`d.public.add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_col_comp ADD COLUMN c INT AS (a + 10) STORED`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp (a) VALUES (2)`)
			if _, err := addColComp.Next(); !testutils.IsError(err, `tables being backfilled`) {
				t.Fatalf(`expected "tables being backfilled" error got: %+v`, err)
			}
		})

		t.Run(`drop column`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, '1')`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column`)
			defer closeFeed(t, dropColumn)
			assertPayloads(t, dropColumn, []string{
				`d.public.drop_column: [1]->{"after": {"a": 1, "b": "1"}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2)`)
			if _, err := dropColumn.Next(); !testutils.IsError(err, `tables being backfilled`) {
				t.Fatalf(`expected "tables being backfilled" error got: %+v`, err)
			}
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

// Test schema changes that require a backfill when the backfill option is
// allowed.
func TestCDCSchemaChangeAllowBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("回填逻辑暂时不可用")
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		t.Run(`add column with default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_def (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (2)`)
			addColumnDef := feed(t, f, `CREATE CHANGEFEED FOR add_column_def`)
			defer closeFeed(t, addColumnDef)
			assertPayloads(t, addColumnDef, []string{
				`d.public.add_column_def: [1]->{"after": {"a": 1}}`,
				`d.public.add_column_def: [2]->{"after": {"a": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_def ADD COLUMN b STRING DEFAULT 'd'`)
			assertPayloads(t, addColumnDef, []string{
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `d.public.add_column_def: [1]->{"after": {"a": 1}}`,
				// `d.public.add_column_def: [2]->{"after": {"a": 2}}`,
				`d.public.add_column_def: [1]->{"after": {"a": 1, "b": "d"}}`,
				`d.public.add_column_def: [2]->{"after": {"a": 2, "b": "d"}}`,
			})
		})

		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_col_comp (a INT PRIMARY KEY, b INT AS (a + 5) STORED)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp (a) VALUES (2)`)
			addColComp := feed(t, f, `CREATE CHANGEFEED FOR add_col_comp`)
			defer closeFeed(t, addColComp)
			assertPayloads(t, addColComp, []string{
				`d.public.add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
				`d.public.add_col_comp: [2]->{"after": {"a": 2, "b": 7}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_col_comp ADD COLUMN c INT AS (a + 10) STORED`)
			assertPayloads(t, addColComp, []string{
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `d.public.add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
				// `d.public.add_col_comp: [2]->{"after": {"a": 2, "b": 7}}`,
				`d.public.add_col_comp: [1]->{"after": {"a": 1, "b": 6, "c": 11}}`,
				`d.public.add_col_comp: [2]->{"after": {"a": 2, "b": 7, "c": 12}}`,
			})
		})

		t.Run(`drop column`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, '1')`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2, '2')`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column`)
			defer closeFeed(t, dropColumn)
			assertPayloads(t, dropColumn, []string{
				`d.public.drop_column: [1]->{"after": {"a": 1, "b": "1"}}`,
				`d.public.drop_column: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (3)`)
			// Dropped columns are immediately invisible.
			assertPayloads(t, dropColumn, []string{
				`d.public.drop_column: [1]->{"after": {"a": 1}}`,
				`d.public.drop_column: [2]->{"after": {"a": 2}}`,
				`d.public.drop_column: [3]->{"after": {"a": 3}}`,
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `d.public.drop_column: [1]->{"after": {"a": 1}}`,
				// `d.public.drop_column: [2]->{"after": {"a": 2}}`,
			})
		})

		t.Run(`multiple alters`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE multiple_alters (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO multiple_alters VALUES (1, '1')`)
			sqlDB.Exec(t, `INSERT INTO multiple_alters VALUES (2, '2')`)

			// Set up a hook to pause the changfeed on the next emit.
			var wg sync.WaitGroup
			waitSinkHook := func(_ context.Context) error {
				wg.Wait()
				return nil
			}
			knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
				DistSQL.(*distsql.TestingKnobs).
				CDC.(*TestingKnobs)
			knobs.BeforeEmitRow = waitSinkHook

			multipleAlters := feed(t, f, `CREATE CHANGEFEED FOR multiple_alters`)
			defer closeFeed(t, multipleAlters)
			assertPayloads(t, multipleAlters, []string{
				`d.public.multiple_alters: [1]->{"after": {"a": 1, "b": "1"}}`,
				`d.public.multiple_alters: [2]->{"after": {"a": 2, "b": "2"}}`,
			})

			// Wait on the next emit, queue up three ALTERs. The next poll process
			// will see all of them at once.
			wg.Add(1)
			waitForSchemaChange(t, sqlDB, `ALTER TABLE multiple_alters DROP COLUMN b`)
			waitForSchemaChange(t, sqlDB, `ALTER TABLE multiple_alters ADD COLUMN c STRING DEFAULT 'cee'`)
			waitForSchemaChange(t, sqlDB, `ALTER TABLE multiple_alters ADD COLUMN d STRING DEFAULT 'dee'`)
			wg.Done()

			assertPayloads(t, multipleAlters, []string{
				// Backfill no-ops for DROP. Dropped columns are immediately invisible.
				`d.public.multiple_alters: [1]->{"after": {"a": 1}}`,
				`d.public.multiple_alters: [2]->{"after": {"a": 2}}`,
				// Scan output for DROP
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `d.public.multiple_alters: [1]->{"after": {"a": 1}}`,
				// `d.public.multiple_alters: [2]->{"after": {"a": 2}}`,
				// Backfill no-ops for column C
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `d.public.multiple_alters: [1]->{"after": {"a": 1}}`,
				// `d.public.multiple_alters: [2]->{"after": {"a": 2}}`,
				// Scan output for column C
				`d.public.multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}}`,
				`d.public.multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}}`,
				// Backfill no-ops for column D (C schema change is complete)
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `d.public.multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}}`,
				// `d.public.multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}}`,
				// Scan output for column C
				`d.public.multiple_alters: [1]->{"after": {"a": 1, "c": "cee", "d": "dee"}}`,
				`d.public.multiple_alters: [2]->{"after": {"a": 2, "c": "cee", "d": "dee"}}`,
			})
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

// Regression test for #34314
func TestCDCAfterSchemaChangeBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE after_backfill (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO after_backfill VALUES (0)`)
		sqlDB.Exec(t, `ALTER TABLE after_backfill ADD COLUMN b INT DEFAULT 1`)
		sqlDB.Exec(t, `INSERT INTO after_backfill VALUES (2, 3)`)
		afterBackfill := feed(t, f, `CREATE CHANGEFEED FOR after_backfill`)
		defer closeFeed(t, afterBackfill)
		assertPayloads(t, afterBackfill, []string{
			`d.public.after_backfill: [0]->{"after": {"a": 0, "b": 1}}`,
			`d.public.after_backfill: [2]->{"after": {"a": 2, "b": 3}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		sqlDB.Exec(t, `CREATE TABLE grandparent (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO grandparent VALUES (0, 'grandparent-0')`)
		grandparent := feed(t, f, `CREATE CHANGEFEED FOR grandparent`)
		defer closeFeed(t, grandparent)
		assertPayloads(t, grandparent, []string{
			`d.public.grandparent: [0]->{"after": {"a": 0, "b": "grandparent-0"}}`,
		})

		sqlDB.Exec(t,
			`CREATE TABLE parent (a INT PRIMARY KEY, b STRING) `+
				`INTERLEAVE IN PARENT grandparent (a)`)
		sqlDB.Exec(t, `INSERT INTO grandparent VALUES (1, 'grandparent-1')`)
		sqlDB.Exec(t, `INSERT INTO parent VALUES (1, 'parent-1')`)
		parent := feed(t, f, `CREATE CHANGEFEED FOR parent`)
		defer closeFeed(t, parent)
		assertPayloads(t, grandparent, []string{
			`d.public.grandparent: [1]->{"after": {"a": 1, "b": "grandparent-1"}}`,
		})
		assertPayloads(t, parent, []string{
			`d.public.parent: [1]->{"after": {"a": 1, "b": "parent-1"}}`,
		})

		sqlDB.Exec(t,
			`CREATE TABLE child (a INT PRIMARY KEY, b STRING) INTERLEAVE IN PARENT parent (a)`)
		sqlDB.Exec(t, `INSERT INTO grandparent VALUES (2, 'grandparent-2')`)
		sqlDB.Exec(t, `INSERT INTO parent VALUES (2, 'parent-2')`)
		sqlDB.Exec(t, `INSERT INTO child VALUES (2, 'child-2')`)
		child := feed(t, f, `CREATE CHANGEFEED FOR child`)
		defer closeFeed(t, child)
		assertPayloads(t, grandparent, []string{
			`d.public.grandparent: [2]->{"after": {"a": 2, "b": "grandparent-2"}}`,
		})
		assertPayloads(t, parent, []string{
			`d.public.parent: [2]->{"after": {"a": 2, "b": "parent-2"}}`,
		})
		assertPayloads(t, child, []string{
			`d.public.child: [2]->{"after": {"a": 2, "b": "child-2"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, FAMILY (a), FAMILY (b))`)
		if strings.Contains(t.Name(), `enterprise`) {
			sqlDB.ExpectErr(t, `exactly 1 column family`, `CREATE CHANGEFEED FOR foo`)
		} else {
			sqlDB.ExpectErr(t, `exactly 1 column family`, `EXPERIMENTAL CHANGEFEED FOR foo`)
		}

		// Table with a second column family added after the CDC starts.
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, FAMILY f_a (a))`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0)`)
		bar := feed(t, f, `CREATE CHANGEFEED FOR bar`)
		defer closeFeed(t, bar)
		assertPayloads(t, bar, []string{
			`d.public.bar: [0]->{"after": {"a": 0}}`,
		})
		sqlDB.Exec(t, `ALTER TABLE bar ADD COLUMN b STRING CREATE FAMILY f_b`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)
		if _, err := bar.Next(); !testutils.IsError(err, `exactly 1 column family`) {
			t.Errorf(`expected "exactly 1 column family" error got: %+v`, err)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		// TODO(dan): Also test a non-STORED computed column once we support them.
		sqlDB.Exec(t, `CREATE TABLE cc (
		a INT, b INT AS (a + 1) STORED, c INT AS (a + 2) STORED, PRIMARY KEY (b, a)
	)`)
		sqlDB.Exec(t, `INSERT INTO cc (a) VALUES (1)`)

		cc := feed(t, f, `CREATE CHANGEFEED FOR cc`)
		defer closeFeed(t, cc)

		assertPayloads(t, cc, []string{
			`d.public.cc: [2, 1]->{"after": {"a": 1, "b": 2, "c": 3}}`,
		})

		sqlDB.Exec(t, `INSERT INTO cc (a) VALUES (10)`)
		assertPayloads(t, cc, []string{
			`d.public.cc: [11, 10]->{"after": {"a": 10, "b": 11, "c": 12}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCUpdatePrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		// This NOT NULL column checks a regression when used with UPDATE-ing a
		// primary key column or with DELETE.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'bar')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "bar"}}`,
		})

		sqlDB.Exec(t, `UPDATE foo SET a = 1`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [0]->{"after": null}`,
			`d.public.foo: [1]->{"after": {"a": 1, "b": "bar"}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCTruncateRenameDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		// TODO(dan): TRUNCATE cascades, test for this too.
		sqlDB.Exec(t, `CREATE TABLE truncate (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO truncate VALUES (1)`)
		truncate := feed(t, f, `CREATE CHANGEFEED FOR truncate`)
		defer closeFeed(t, truncate)
		assertPayloads(t, truncate, []string{`d.public.truncate: [1]->{"after": {"a": 1}}`})
		sqlDB.Exec(t, `TRUNCATE TABLE truncate`)
		if _, err := truncate.Next(); !testutils.IsError(err, `"d.public.truncate" was dropped or truncated`) {
			t.Errorf(`expected ""truncate" was dropped or truncated" error got: %+v`, err)
		}

		sqlDB.Exec(t, `CREATE TABLE rename (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO rename VALUES (1)`)
		rename := feed(t, f, `CREATE CHANGEFEED FOR rename`)
		defer closeFeed(t, rename)
		assertPayloads(t, rename, []string{`d.public.rename: [1]->{"after": {"a": 1}}`})
		sqlDB.Exec(t, `ALTER TABLE rename RENAME TO renamed`)
		sqlDB.Exec(t, `INSERT INTO renamed VALUES (2)`)
		sqlDB.Exec(t, `CREATE TABLE drop (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO drop VALUES (1)`)
		drop := feed(t, f, `CREATE CHANGEFEED FOR drop`)
		defer closeFeed(t, drop)
		assertPayloads(t, drop, []string{`d.public.drop: [1]->{"after": {"a": 1}}`})
		sqlDB.Exec(t, `DROP TABLE drop`)
		if _, err := drop.Next(); !testutils.IsError(err, `"d.public.drop" was dropped or truncated`) {
			t.Errorf(`expected ""drop" was dropped or truncated" error got: %+v`, err)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCMonitoring(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// todo(江磊)
	// 修复测试
	t.Skip("todo 江磊")
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		beforeEmitRowCh := make(chan struct{}, 2)
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*distsql.TestingKnobs).
			CDC.(*TestingKnobs)
		knobs.BeforeEmitRow = func(_ context.Context) error {
			<-beforeEmitRowCh
			return nil
		}

		sqlDB := sqlutils.MakeSQLRunner(db)
		var usingRangeFeed bool
		sqlDB.QueryRow(t, `SHOW CLUSTER SETTING changefeed.push.enabled`, &usingRangeFeed)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		start := timeutil.Now()
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		s := f.Server()
		if c := s.MustGetSQLCounter(`changefeed.emitted_messages`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.emit_nanos`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.flushes`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.flush_nanos`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
			t.Errorf(`expected %d got %d`, 0, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.min_high_water`); c != noMinHighWaterSentinel {
			t.Errorf(`expected %d got %d`, noMinHighWaterSentinel, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.buffer_entries.in`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.buffer_entries.out`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}

		beforeEmitRowCh <- struct{}{}
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		_, _ = foo.Next()
		testutils.SucceedsSoon(t, func() error {
			if c := s.MustGetSQLCounter(`changefeed.emitted_messages`); c != 1 {
				return errors.Errorf(`expected 1 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 22 {
				return errors.Errorf(`expected 22 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.emit_nanos`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.flushes`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.flush_nanos`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.min_high_water`); c == noMinHighWaterSentinel {
				return errors.New(`waiting for high-water to not be sentinel`)
			} else if c <= start.UnixNano() {
				return errors.Errorf(`expected > %d got %d`, start.UnixNano(), c)
			}
			if usingRangeFeed {
				// Only RangeFeed-based CDCs use this buffer.
				if c := s.MustGetSQLCounter(`changefeed.buffer_entries.in`); c <= 0 {
					return errors.Errorf(`expected > 0 got %d`, c)
				}
				if c := s.MustGetSQLCounter(`changefeed.buffer_entries.out`); c <= 0 {
					return errors.Errorf(`expected > 0 got %d`, c)
				}
			}
			return nil
		})

		// Not reading from foo will backpressure it. max_behind_nanos will grow and
		// min_high_water will stagnate.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
		const expectedLatency = 100 * time.Millisecond
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = $1`,
			(expectedLatency / 10).String())

		testutils.SucceedsSoon(t, func() error {
			waitForBehindNanos := 2 * expectedLatency.Nanoseconds()
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c < waitForBehindNanos {
				return errors.Errorf(
					`waiting for the feed to be > %d nanos behind got %d`, waitForBehindNanos, c)
			}
			return nil
		})
		stalled := s.MustGetSQLCounter(`changefeed.min_high_water`)
		for i := 0; i < 100; {
			i++
			newMinResolved := s.MustGetSQLCounter(`changefeed.min_high_water`)
			if newMinResolved != stalled {
				stalled = newMinResolved
				i = 0
			}
		}

		// Unblocking the emit should bring the max_behind_nanos back down and
		// should update the min_high_water.
		beforeEmitRowCh <- struct{}{}
		_, _ = foo.Next()
		testutils.SucceedsSoon(t, func() error {
			waitForBehindNanos := expectedLatency.Nanoseconds()
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c > waitForBehindNanos {
				return errors.Errorf(
					`waiting for the feed to be < %d nanos behind got %d`, waitForBehindNanos, c)
			}
			return nil
		})
		testutils.SucceedsSoon(t, func() error {
			if c := s.MustGetSQLCounter(`changefeed.min_high_water`); c <= stalled {
				return errors.Errorf(`expected > %d got %d`, stalled, c)
			}
			return nil
		})

		// Check that two CDCs add correctly.
		beforeEmitRowCh <- struct{}{}
		beforeEmitRowCh <- struct{}{}
		// Set cluster settings back so we don't interfere with schema changes.
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
		fooCopy := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		_, _ = fooCopy.Next()
		_, _ = fooCopy.Next()
		testutils.SucceedsSoon(t, func() error {
			if c := s.MustGetSQLCounter(`changefeed.emitted_messages`); c != 4 {
				return errors.Errorf(`expected 4 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 88 {
				return errors.Errorf(`expected 88 got %d`, c)
			}
			return nil
		})

		// Cancel all the CDCs and check that max_behind_nanos returns to 0
		// and min_high_water returns to the no high-water sentinel.
		close(beforeEmitRowCh)
		require.NoError(t, foo.Close())
		require.NoError(t, fooCopy.Close())
		testutils.SucceedsSoon(t, func() error {
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
				return errors.Errorf(`expected 0 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.min_high_water`); c != noMinHighWaterSentinel {
				return errors.Errorf(`expected %d got %d`, noMinHighWaterSentinel, c)
			}
			return nil
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}
func TestChangefeedRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilicl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*distsql.TestingKnobs).
			CDC.(*TestingKnobs)
		origAfterSinkFlushHook := knobs.AfterSinkFlush
		var failSink int64
		failSinkHook := func() error {
			switch atomic.LoadInt64(&failSink) {
			case 1:
				return fmt.Errorf("unique synthetic retryable error: %s", timeutil.Now())
			case 2:
				return MarkTerminalError(fmt.Errorf("synthetic terminal error"))
			case 3:
				return fmt.Errorf("should be terminal but isn't")
			}
			return origAfterSinkFlushHook()
		}
		knobs.AfterSinkFlush = failSinkHook

		// Set up a new feed and verify that the sink is started up.
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": {"a": 1}}`,
		})

		// Set sink to return unique retryable errors and insert a row. Verify that
		// sink is failing requests.
		atomic.StoreInt64(&failSink, 1)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
		registry := f.Server().JobRegistry().(*jobs.Registry)
		retryCounter := registry.MetricsStruct().CDC.(*Metrics).SinkErrorRetries
		testutils.SucceedsSoon(t, func() error {
			if retryCounter.Counter.Count() < 3 {
				return fmt.Errorf("insufficient error retries detected")
			}
			return nil
		})
		// Fix the sink and insert another row. Check that nothing funky happened.
		atomic.StoreInt64(&failSink, 0)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)
		assertPayloads(t, foo, []string{
			`d.public.foo: [2]->{"after": {"a": 2}}`,
			`d.public.foo: [3]->{"after": {"a": 3}}`,
		})

		// Set sink to return a terminal error and insert a row. Ensure that we
		// eventually get the error message back out.
		atomic.StoreInt64(&failSink, 2)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (4)`)
		for {
			_, err := foo.Next()
			if err == nil {
				continue
			}
			require.EqualError(t, err, `synthetic terminal error`)
			break
		}

		// The above test an error that is correctly _not marked_ as terminal and
		// one that is correctly _marked_ as terminal. We're also concerned a
		// terminal error that is not marked but should be. This would result in an
		// indefinite retry loop, so we have a safety net that bails if we
		// consecutively receive an identical error message some number of times.
		// But default this safety net is disabled in tests, because we want to
		// catch this. The following line enables the safety net (by setting the
		// knob to the 0 value) so that we can test the safety net.
		knobs.ConsecutiveIdenticalErrorBailoutCount = 0

		// Set up a new feed and verify that the sink is started up.
		atomic.StoreInt64(&failSink, 0)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		bar := feed(t, f, `CREATE CHANGEFEED FOR bar`)
		defer closeFeed(t, bar)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)
		assertPayloads(t, bar, []string{
			`d.public.bar: [1]->{"after": {"a": 1}}`,
		})

		// Set sink to return a non-unique non-terminal error and insert a row.
		// This simulates an error we should blacklist but have missed. The feed
		// should eventually fail.
		atomic.StoreInt64(&failSink, 3)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (2)`)
		for {
			_, err := bar.Next()
			if err == nil {
				continue
			}
			require.EqualError(t, err, `should be terminal but isn't`)
			break
		}
	}

	// Only the enterprise version uses jobs.
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(enterpriseTest, testFn))
}

// TestCDCDataTTL ensures that CDCs fail with an error in the case
// where the feed has fallen behind the GC TTL of the table data.
func TestCDCDataTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("https://github.com/cockroachdb/cockroach/issues/37154")
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		// Set a very simple channel-based, wait-and-resume function as the
		// BeforeEmitRow hook.
		var shouldWait int32
		wait := make(chan struct{})
		resume := make(chan struct{})
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*distsql.TestingKnobs).
			CDC.(*TestingKnobs)
		knobs.BeforeEmitRow = func(_ context.Context) error {
			if atomic.LoadInt32(&shouldWait) == 0 {
				return nil
			}
			wait <- struct{}{}
			<-resume
			return nil
		}

		sqlDB := sqlutils.MakeSQLRunner(db)

		// Create the data table; it will only contain a single row with multiple
		// versions.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		counter := 0
		upsertRow := func() {
			counter++
			sqlDB.Exec(t, `UPSERT INTO foo (a, b) VALUES (1, $1)`, fmt.Sprintf("version %d", counter))
		}

		// Create the initial version of the row and the CDC itself. The initial
		// version is necessary to prevent CREATE CHANGEFEED itself from hanging.
		upsertRow()
		dataExpiredRows := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo")
		defer closeFeed(t, dataExpiredRows)

		// Set up our emit trap and update the row, which will allow us to "pause" the
		// CDC in order to force a GC.
		atomic.StoreInt32(&shouldWait, 1)
		upsertRow()
		<-wait

		// Upsert two additional versions. One of these will be deleted by the GC
		// process before CDC polling is resumed.
		upsertRow()
		upsertRow()

		// Force a GC of the table. This should cause both older versions of the
		// table to be deleted, with the middle version being lost to the CDC.
		forceTableGC(t, f.Server(), sqlDB, "d", "foo")

		// Resume our CDC normally.
		atomic.StoreInt32(&shouldWait, 0)
		resume <- struct{}{}

		// Verify that the third call to Next() returns an error (the first is the
		// initial row, the second is the first change. The third should detect the
		// GC interval mismatch).
		_, _ = dataExpiredRows.Next()
		_, _ = dataExpiredRows.Next()
		if _, err := dataExpiredRows.Next(); !testutils.IsError(err, `must be after replica GC threshold`) {
			t.Errorf(`expected "must be after replica GC threshold" error got: %+v`, err)
		}
	}

	// skip by gzq
	// todo jerry
	// t.Run("sinkless", sinklessTest(testFn))
	t.Run("enterprise", enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `CREATE DATABASE d`)

	// CDCs default to rangefeed, but for now, rangefeed defaults to off.
	// Verify that this produces a useful error.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = false`)
	sqlDB.Exec(t, `CREATE TABLE rangefeed_off (a INT PRIMARY KEY)`)
	sqlDB.ExpectErr(
		t, `rangefeeds require the kv.rangefeed.enabled setting`,
		`EXPERIMENTAL CHANGEFEED FOR rangefeed_off`,
	)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled TO DEFAULT`)

	sqlDB.ExpectErr(
		t, `unknown format: nope`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH format=nope`,
	)

	sqlDB.ExpectErr(
		t, `unknown envelope: nope`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH envelope=nope`,
	)
	sqlDB.ExpectErr(
		t, `negative durations are not accepted: resolved='-1s'`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH resolved='-1s'`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify timestamp in the future`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH cursor=$1`, timeutil.Now().Add(time.Hour),
	)

	sqlDB.ExpectErr(
		t, `omit the SINK clause`,
		`CREATE CHANGEFEED FOR foo INTO ''`,
	)
	sqlDB.ExpectErr(
		t, `omit the SINK clause`,
		`CREATE CHANGEFEED FOR foo INTO $1`, ``,
	)

	// Watching system.jobs would create a cycle, since the resolved timestamp
	// high-water mark is saved in it.
	sqlDB.ExpectErr(
		t, `not supported on system tables`,
		`EXPERIMENTAL CHANGEFEED FOR system.jobs`,
	)
	sqlDB.ExpectErr(
		t, `table "bar" does not exist`,
		`EXPERIMENTAL CHANGEFEED FOR bar`,
	)
	sqlDB.Exec(t, `CREATE SEQUENCE seq`)
	sqlDB.ExpectErr(
		t, `CHANGEFEED cannot target sequences: seq`,
		`EXPERIMENTAL CHANGEFEED FOR seq`,
	)
	sqlDB.Exec(t, `CREATE VIEW vw AS SELECT a, b FROM foo`)
	sqlDB.ExpectErr(
		t, `CHANGEFEED cannot target views: vw`,
		`EXPERIMENTAL CHANGEFEED FOR vw`,
	)
	// Backup has the same bad error message #28170.
	sqlDB.ExpectErr(
		t, `"information_schema.tables" does not exist`,
		`EXPERIMENTAL CHANGEFEED FOR information_schema.tables`,
	)

	// TODO(dan): These two tests shouldn't need initial data in the table
	// to pass.
	sqlDB.Exec(t, `CREATE TABLE dec (a DECIMAL PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO dec VALUES (1.0)`)
	sqlDB.ExpectErr(
		t, `pq: column a: decimal with no precision`,
		`EXPERIMENTAL CHANGEFEED FOR dec WITH format=$1, confluent_schema_registry=$2`,
		optFormatAvro, `bar`,
	)
	sqlDB.Exec(t, `CREATE TABLE "oid" (a OID PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO "oid" VALUES (3::OID)`)
	sqlDB.ExpectErr(
		t, `pq: column a: type OID not yet supported with avro`,
		`EXPERIMENTAL CHANGEFEED FOR "oid" WITH format=$1, confluent_schema_registry=$2`,
		optFormatAvro, `bar`,
	)

	// Check that confluent_schema_registry is only accepted if format is avro.
	sqlDB.ExpectErr(
		t, `unknown sink query parameter: confluent_schema_registry`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `experimental-sql://d/?confluent_schema_registry=foo`,
	)

	// Check unavailable kafka.
	sqlDB.ExpectErr(
		t, `client has run out of available brokers`,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope'`,
	)

	// kafka_topic_prefix was referenced by an old version of the RFC, it's
	// "topic_prefix" now.
	sqlDB.ExpectErr(
		t, `unknown sink query parameter: kafka_topic_prefix`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?kafka_topic_prefix=foo`,
	)

	// schema_topic will be implemented but isn't yet.
	sqlDB.ExpectErr(
		t, `schema_topic is not yet supported`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?schema_topic=foo`,
	)

	// Sanity check kafka tls parameters.
	sqlDB.ExpectErr(
		t, `param tls_enabled must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=foo`,
	)
	sqlDB.ExpectErr(
		t, `param ca_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?ca_cert=!`,
	)
	sqlDB.ExpectErr(
		t, `ca_cert requires tls_enabled=true`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?&ca_cert=Zm9v`,
	)

	// Sanity check kafka sasl parameters.
	sqlDB.ExpectErr(
		t, `param sasl_enabled must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=maybe`,
	)

	sqlDB.ExpectErr(
		t, `param sasl_handshake must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_handshake=maybe`,
	)

	sqlDB.ExpectErr(
		t, `sasl_enabled must be enabled to configure SASL handshake behavior`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_handshake=false`,
	)

	sqlDB.ExpectErr(
		t, `sasl_user must be provided when SASL is enabled`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true`,
	)

	sqlDB.ExpectErr(
		t, `sasl_password must be provided when SASL is enabled`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_user=a`,
	)

	sqlDB.ExpectErr(
		t, `sasl_enabled must be enabled if a SASL user is provided`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_user=a`,
	)

	sqlDB.ExpectErr(
		t, `sasl_enabled must be enabled if a SASL password is provided`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_password=a`,
	)

	// The cloudStorageSink is particular about the options it will work with.
	sqlDB.ExpectErr(
		t, `this sink is incompatible with format=experimental_avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='experimental_avro'`,
		`experimental-nodelocal:///bar`,
	)
	sqlDB.ExpectErr(
		t, `this sink is incompatible with envelope=key_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH envelope='key_only'`,
		`experimental-nodelocal:///bar`,
	)
	// WITH diff requires envelope=wrapped
	sqlDB.ExpectErr(
		t, `diff is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo WITH diff, envelope='key_only'`,
	)
	sqlDB.ExpectErr(
		t, `diff is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo WITH diff, envelope='row'`,
	)
}

func TestCDCPermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE USER testuser`)

		s := f.Server()
		pgURL, cleanupFunc := sqlutils.PGUrl(
			t, s.ServingAddr(), "TestCDCPermissions-testuser", url.User("testuser"),
		)
		defer cleanupFunc()
		testuser, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer testuser.Close()

		stmt := `EXPERIMENTAL CHANGEFEED FOR foo`
		if strings.Contains(t.Name(), `enterprise`) {
			stmt = `CREATE CHANGEFEED FOR foo`
		}
		if _, err := testuser.Exec(stmt); !testutils.IsError(err, `pq: table "foo" does not exist`) {
			t.Errorf(`expected 'pq: table "foo" does not exist' error got: %+v`, err)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		// Intentionally don't use the TestFeedFactory because we want to
		// control the placeholders.
		s := f.Server()
		sink, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		sink.Scheme = sinkSchemeExperimentalSQL
		sink.Path = `d`

		var jobID int64
		var topicName string
		var tableName string
		sqlDB.QueryRow(t,
			`CREATE CHANGEFEED FOR foo INTO $1 WITH updated, envelope = $2`, sink.String(), `wrapped`,
		).Scan(&jobID, &topicName, &tableName)

		// Secrets get removed from the sink parameter.
		expectedSink := sink
		expectedSink.RawQuery = ``

		var description string
		sqlDB.QueryRow(t,
			`SELECT description FROM [SHOW JOBS] WHERE job_id = $1`, jobID,
		).Scan(&description)

		// todo make description more reasonable
		expected := `CREATE CHANGEFEED FOR TABLE foo INTO '` + expectedSink.String() +
			`' WITH envelope = 'wrapped', updated` + `||` + `{"56":""}` + `||` + `{"56":"d.public.foo"}`

		if description != expected {
			t.Errorf(`got "%s" expected "%s"`, description, expected)
		}
	}

	// Only the enterprise version uses jobs.
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(enterpriseTest, testFn))
}
func TestCDCPauseUnpause(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(i time.Duration) { jobs.DefaultAdoptInterval = i }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved`).(*cdctest.TableFeed)
		defer closeFeed(t, foo)

		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`d.public.foo: [2]->{"after": {"a": 2, "b": "b"}}`,
			`d.public.foo: [4]->{"after": {"a": 4, "b": "c"}}`,
			`d.public.foo: [7]->{"after": {"a": 7, "b": "d"}}`,
			`d.public.foo: [8]->{"after": {"a": 8, "b": "e"}}`,
		})

		// Wait for the high-water mark on the job to be updated after the initial
		// scan, to make sure we don't get the initial scan data again.
		m, err := foo.Next()
		if err != nil {
			t.Fatal(err)
		} else if m.Key != nil {
			t.Fatalf(`expected a resolved timestamp got %s: %s->%s`, m.Topic, m.Key, m.Value)
		}

		sqlDB.Exec(t, `PAUSE JOB $1`, foo.JobID)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
		sqlDB.Exec(t, `RESUME JOB $1`, foo.JobID)
		assertPayloads(t, foo, []string{
			`d.public.foo: [16]->{"after": {"a": 16, "b": "f"}}`,
		})
	}

	// Only the enterprise version uses jobs.
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(enterpriseTest, testFn))
}

func TestManyCDCsOneTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'init')`)

		foo1 := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, foo1)
		foo2 := feed(t, f, `CREATE CHANGEFEED FOR foo`) // without diff
		defer closeFeed(t, foo2)
		foo3 := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, foo3)

		// Make sure all the CDCs are going.
		assertPayloads(t, foo1, []string{`d.public.foo: [0]->{"after": {"a": 0, "b": "init"}, "before": null}`})
		assertPayloads(t, foo2, []string{`d.public.foo: [0]->{"after": {"a": 0, "b": "init"}}`})
		assertPayloads(t, foo3, []string{`d.public.foo: [0]->{"after": {"a": 0, "b": "init"}, "before": null}`})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'v0')`)
		assertPayloads(t, foo1, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "v0"}, "before": {"a": 0, "b": "init"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'v1')`)
		assertPayloads(t, foo1, []string{
			`d.public.foo: [1]->{"after": {"a": 1, "b": "v1"}, "before": null}`,
		})
		assertPayloads(t, foo2, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "v0"}}`,
			`d.public.foo: [1]->{"after": {"a": 1, "b": "v1"}}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'v2')`)
		assertPayloads(t, foo1, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "v2"}, "before": {"a": 0, "b": "v0"}}`,
		})
		assertPayloads(t, foo2, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "v2"}}`,
		})
		assertPayloads(t, foo3, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "v0"}, "before": {"a": 0, "b": "init"}}`,
			`d.public.foo: [0]->{"after": {"a": 0, "b": "v2"}, "before": {"a": 0, "b": "v0"}}`,
			`d.public.foo: [1]->{"after": {"a": 1, "b": "v1"}, "before": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestUnspecifiedPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT)`)
		var id0 int
		sqlDB.QueryRow(t, `INSERT INTO foo VALUES (0) RETURNING rowid`).Scan(&id0)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		var id1 int
		sqlDB.QueryRow(t, `INSERT INTO foo VALUES (1) RETURNING rowid`).Scan(&id1)

		assertPayloads(t, foo, []string{
			fmt.Sprintf(`d.public.foo: [%d]->{"after": {"a": 0, "rowid": %d}}`, id0, id0),
			fmt.Sprintf(`d.public.foo: [%d]->{"after": {"a": 1, "rowid": %d}}`, id1, id1),
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

// TestCDCNodeShutdown ensures that an enterprise CDC continues
// running after the original job-coordinator node is shut down.
func TestCDCNodeShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#32232")

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	flushCh := make(chan struct{}, 1)
	defer close(flushCh)
	knobs := base.TestingKnobs{DistSQL: &distsql.TestingKnobs{CDC: &TestingKnobs{
		AfterSinkFlush: func() error {
			select {
			case flushCh <- struct{}{}:
			default:
			}
			return nil
		},
	}}}

	tc := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			UseDatabase: "d",
			Knobs:       knobs,
		},
	})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(1)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)

	// Create a factory which uses server 1 as the output of the Sink, but
	// executes the CREATE CHANGEFEED statement on server 0.
	sink, cleanup := sqlutils.PGUrl(
		t, tc.Server(0).ServingAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()
	f := cdctest.MakeTableFeedFactory(tc.Server(1), tc.ServerConn(0), flushCh, sink)
	foo := feed(t, f, "CREATE CHANGEFEED FOR foo")
	defer closeFeed(t, foo)

	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'second')`)
	assertPayloads(t, foo, []string{
		`d.public.foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
		`d.public.foo: [1]->{"after": {"a": 1, "b": "second"}}`,
	})

	// TODO(mrtracy): At this point we need to wait for a resolved timestamp,
	// in order to ensure that there isn't a repeat when the job is picked up
	// again. As an alternative, we could use a verifier instead of assertPayloads.

	// Wait for the high-water mark on the job to be updated after the initial
	// scan, to make sure we don't get the initial scan data again.

	// Stop server 0, which is where the table feed connects.
	tc.StopServer(0)

	sqlDB.Exec(t, `UPSERT INTO foo VALUES(0, 'updated')`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (3, 'third')`)

	assertPayloads(t, foo, []string{
		`d.public.foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
		`d.public.foo: [3]->{"after": {"a": 3, "b": "third"}}`,
	})
}

func TestCDCTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)

		// Reset the counts.
		_ = telemetry.GetAndResetFeatureCounts(false)

		// Low some feeds (and read from them to make sure they've started.
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		fooBar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar WITH format=json`)
		defer closeFeed(t, fooBar)
		assertPayloads(t, foo, []string{
			`d.public.foo: [1]->{"after": {"a": 1}}`,
		})
		assertPayloads(t, fooBar, []string{
			`d.public.bar: [1]->{"after": {"a": 1}}`,
			`d.public.foo: [1]->{"after": {"a": 1}}`,
		})

		var expectedSink string
		if strings.Contains(t.Name(), `sinkless`) || strings.Contains(t.Name(), `poller`) {
			expectedSink = `sinkless`
		} else {
			expectedSink = `experimental-sql`
		}

		//var expectedPushEnabled string
		//if strings.Contains(t.Name(), `sinkless`) {
		//	expectedPushEnabled = `enabled`
		//} else {
		//	expectedPushEnabled = `disabled`
		//}

		counts := telemetry.GetAndResetFeatureCounts(false)
		require.Equal(t, int32(2), counts[`changefeed.create.sink.`+expectedSink])
		require.Equal(t, int32(2), counts[`changefeed.create.format.json`])
		require.Equal(t, int32(1), counts[`changefeed.create.num_tables.1`])
		require.Equal(t, int32(1), counts[`changefeed.create.num_tables.2`])
		//require.Equal(t, int32(2), counts[`changefeed.run.push.`+expectedPushEnabled])
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCDCMemBufferCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*distsql.TestingKnobs).
			CDC.(*TestingKnobs)
		// The RowContainer used internally by the memBuffer seems to request from
		// the budget in 10240 chunks. Set this number high enough for one but not
		// for a second. I'd love to be able to derive this from constants, but I
		// don't see how to do that without a refactor.
		knobs.MemBufferCapacity = 20000
		beforeEmitRowCh := make(chan struct{}, 1)
		knobs.BeforeEmitRow = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-beforeEmitRowCh:
			}
			return nil
		}
		defer close(beforeEmitRowCh)

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'small')`)

		// Force rangefeed, even for the enterprise test.
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.push.enabled = true`)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		// Small amounts of data fit in the buffer.
		beforeEmitRowCh <- struct{}{}
		assertPayloads(t, foo, []string{
			`d.public.foo: [0]->{"after": {"a": 0, "b": "small"}}`,
		})

		// Put enough data in to overflow the buffer and verify that at some point
		// we get the "memory budget exceeded" error.
		sqlDB.Exec(t, `INSERT INTO foo SELECT i, 'foofoofoo' FROM generate_series(1, $1) AS g(i)`, 1000)
		if _, err := foo.Next(); !testutils.IsError(err, `memory budget exceeded`) {
			t.Fatalf(`expected "memory budget exceeded" error got: %v`, err)
		}
	}

	// The mem buffer is only used with RangeFeed.
	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedRestartDuringBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("回填逻辑不可用")
	defer func(i time.Duration) { jobs.DefaultAdoptInterval = i }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*distsql.TestingKnobs).
			CDC.(*TestingKnobs)
		beforeEmitRowCh := make(chan error, 20)
		knobs.BeforeEmitRow = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-beforeEmitRowCh:
				return err
			}
		}
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0), (1), (2), (3)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`).(*cdctest.TableFeed)
		defer closeFeed(t, foo)

		// TODO(dan): At a high level, all we're doing is trying to restart a
		// changefeed in the middle of changefeed backfill after a schema change
		// finishes. It turns out this is pretty hard to do with our current testing
		// knobs and this test ends up being pretty brittle. I'd love it if anyone
		// thought of a better way to do this.
		// Read the initial data in the rows.
		for i := 0; i < 4; i++ {
			beforeEmitRowCh <- nil
		}
		assertPayloads(t, foo, []string{
			`d.public.foo: [0]->{"after": {"a": 0}, "before": null}`,
			`d.public.foo: [1]->{"after": {"a": 1}, "before": null}`,
			`d.public.foo: [2]->{"after": {"a": 2}, "before": null}`,
			`d.public.foo: [3]->{"after": {"a": 3}, "before": null}`,
		})

		// Run a schema change that backfills kvs.
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b STRING DEFAULT 'backfill'`)
		// Unblock emit for each kv written by the schema change's backfill. The
		// changefeed actually emits these, but we lose it to overaggressive
		// duplicate detection in tableFeed.
		// TODO(dan): Track duplicates more precisely in tableFeed.
		for i := 0; i < 4; i++ {
			beforeEmitRowCh <- nil
		}
		// Unblock the emit for *all but one* of the rows emitted by the changefeed
		// backfill (run after the schema change completes and the final table
		// descriptor is written). The reason this test has 4 rows is because the
		// `sqlSink` that powers `tableFeed` only flushes after it has 3 rows, so we
		// need 1 more than that to guarantee that this first one gets flushed.
		for i := 0; i < 3; i++ {
			beforeEmitRowCh <- nil
		}
		assertPayloads(t, foo, []string{
			`d.public.foo: [0]->{"after": {"a": 0}, "before": {"a": 0}}`,
			`d.public.foo: [1]->{"after": {"a": 1}, "before": {"a": 1}}`,
			`d.public.foo: [2]->{"after": {"a": 2}, "before": {"a": 2}}`,
			`d.public.foo: [3]->{"after": {"a": 3}, "before": {"a": 3}}`,
			`d.public.foo: [0]->{"after": {"a": 0, "b": "backfill"}, "before": {"a": 0}}`,
		})

		// Restart the changefeed without allowing the second row to be backfilled.
		sqlDB.Exec(t, `PAUSE JOB $1`, foo.JobID)
		// Make extra sure that the zombie changefeed can't write any more data.
		//beforeEmitRowCh <- MarkRetryableError(errors.New(`nope don't write it`))
		// Insert some data that we should only see out of the changefeed after it
		// re-runs the backfill.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (6, 'bar')`)
		// Unblock all later emits, we don't need this control anymore.
		close(beforeEmitRowCh)
		// Resume the changefeed and the backfill should start up again. Currently
		// this does the entire backfill again, you could imagine in the future that
		// we do some sort of backfill checkpointing and start the backfill up from
		// the last checkpoint.
		sqlDB.Exec(t, `RESUME JOB $1`, foo.JobID)
		assertPayloads(t, foo, []string{
			// The changefeed actually emits this row, but we lose it to
			// overaggressive duplicate detection in tableFeed.
			// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
			// `d.public.foo: [0]->{"after": {"a": 0, "b": "backfill"}}`,
			`d.public.foo: [1]->{"after": {"a": 1, "b": "backfill"}, "before": {"a": 1}}`,
			`d.public.foo: [2]->{"after": {"a": 2, "b": "backfill"}, "before": {"a": 2}}`,
			`d.public.foo: [3]->{"after": {"a": 3, "b": "backfill"}, "before": {"a": 3}}`,
		})

		assertPayloads(t, foo, []string{
			`d.public.foo: [6]->{"after": {"a": 6, "b": "bar"}, "before": null}`,
		})
	}

	// Only the enterprise version uses jobs.
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("回填逻辑不可用")
	if testing.Short() || util.RaceEnabled {
		t.Skip("takes too long with race enabled")
	}
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		// Shorten the intervals so this test doesn't take so long. We need to wait
		// for timestamps to get resolved.
		sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.close_fraction = .99")

		t.Run("add column not null", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_not_null (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_column_not_null`)
			sqlDB.Exec(t, `INSERT INTO add_column_not_null VALUES (0)`)
			addColumnNotNull := feed(t, f, `CREATE CHANGEFEED FOR add_column_not_null `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, addColumnNotNull)
			sqlDB.Exec(t, `INSERT INTO add_column_not_null VALUES (1)`)
			assertPayloads(t, addColumnNotNull, []string{
				`add_column_not_null: [0]->{"after": {"a": 0}}`,
				`add_column_not_null: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_not_null ADD COLUMN b INT NOT NULL DEFAULT 0`)
			sqlDB.Exec(t, "INSERT INTO add_column_not_null VALUES (2, 1)")
			assertPayloads(t, addColumnNotNull, []string{
				`add_column_not_null: [2]->{"after": {"a": 2, "b": 1}}`,
			})
		})
		t.Run("add column null", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_null (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_column_null`)
			sqlDB.Exec(t, `INSERT INTO add_column_null VALUES (0)`)
			addColumnNull := feed(t, f, `CREATE CHANGEFEED FOR add_column_null `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, addColumnNull)
			sqlDB.Exec(t, `INSERT INTO add_column_null VALUES (1)`)
			assertPayloads(t, addColumnNull, []string{
				`add_column_null: [0]->{"after": {"a": 0}}`,
				`add_column_null: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_null ADD COLUMN b INT`)
			sqlDB.Exec(t, "INSERT INTO add_column_null VALUES (2, NULL)")
			assertPayloads(t, addColumnNull, []string{
				`add_column_null: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})
		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_comp_col (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_comp_col`)
			sqlDB.Exec(t, `INSERT INTO add_comp_col VALUES (0)`)
			addCompCol := feed(t, f, `CREATE CHANGEFEED FOR add_comp_col `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, addCompCol)
			sqlDB.Exec(t, `INSERT INTO add_comp_col VALUES (1)`)
			assertPayloads(t, addCompCol, []string{
				`add_comp_col: [0]->{"after": {"a": 0}}`,
				`add_comp_col: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_comp_col ADD COLUMN b INT AS (a + 1) STORED`)
			sqlDB.Exec(t, "INSERT INTO add_comp_col VALUES (2)")
			assertPayloads(t, addCompCol, []string{
				`add_comp_col: [2]->{"after": {"a": 2, "b": 3}}`,
			})
		})
		t.Run("drop column", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b INT)`)
			defer sqlDB.Exec(t, `DROP TABLE drop_column`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (0, NULL)`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, dropColumn)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, 2)`)
			assertPayloads(t, dropColumn, []string{
				`d.public.drop_column: [0]->{"after": {"a": 0, "b": null}}`,
				`d.public.drop_column: [1]->{"after": {"a": 1, "b": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2)`)
			// NB: You might expect to only see the new row here but we'll see them
			// all because we cannot distinguish between the index backfill and
			// foreground writes. See #35738.
			assertPayloads(t, dropColumn, []string{
				`d.public.drop_column: [0]->{"after": {"a": 0}}`,
				`d.public.drop_column: [1]->{"after": {"a": 1}}`,
				`d.public.drop_column: [2]->{"after": {"a": 2}}`,
			})
		})
		t.Run("add index", func(t *testing.T) {
			// This case does not exit
			sqlDB.Exec(t, `CREATE TABLE add_index (a INT PRIMARY KEY, b INT)`)
			defer sqlDB.Exec(t, `DROP TABLE add_index`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (0, NULL)`)
			addIndex := feed(t, f, `CREATE CHANGEFEED FOR add_index `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, addIndex)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (1, 2)`)
			assertPayloads(t, addIndex, []string{
				`d.public.add_index: [0]->{"after": {"a": 0, "b": null}}`,
				`d.public.add_index: [1]->{"after": {"a": 1, "b": 2}}`,
			})
			sqlDB.Exec(t, `CREATE INDEX ON add_index (b)`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (2, NULL)`)
			assertPayloads(t, addIndex, []string{
				`d.public.add_index: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}
