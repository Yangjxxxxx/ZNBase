// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package dump_test

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

func TestShowDump(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 11
	_, tc, sqlDB, _, cleanupFn := dumpLoadTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	full, inc, details := localFoo+"/full", localFoo+"/inc", localFoo+"/details"

	beforeFull := timeutil.Now()
	sqlDB.Exec(t, `DUMP data.bank TO SST $1`, full)

	var unused driver.Value
	var start, end *time.Time
	var dataSize, rows uint64
	sqlDB.QueryRow(t, `SELECT * FROM [SHOW DUMP $1] WHERE table_name = 'bank'`, full).Scan(
		&unused, &unused, &start, &end, &dataSize, &rows,
	)
	if start != nil {
		t.Errorf("expected null start time on full dump, got %v", *start)
	}
	if !(*end).After(beforeFull) {
		t.Errorf("expected now (%s) to be in (%s, %s)", beforeFull, start, end)
	}
	if dataSize <= 0 {
		t.Errorf("expected dataSize to be >0 got : %d", dataSize)
	}
	if rows != numAccounts {
		t.Errorf("expected %d got: %d", numAccounts, rows)
	}

	// Mess with half the rows.
	affectedRows, err := sqlDB.Exec(t,
		`UPDATE data.bank SET id = -1 * id WHERE id > $1`, numAccounts/2,
	).RowsAffected()
	if err != nil {
		t.Fatal(err)
	} else if affectedRows != numAccounts/2 {
		t.Fatalf("expected to update %d rows, got %d", numAccounts/2, affectedRows)
	}

	beforeInc := timeutil.Now()
	sqlDB.Exec(t, `DUMP data.bank TO SST $1 INCREMENTAL FROM $2`, inc, full)

	sqlDB.QueryRow(t, `SELECT * FROM [SHOW DUMP $1] WHERE table_name = 'bank'`, inc).Scan(
		&unused, &unused, &start, &end, &dataSize, &rows,
	)
	if start == nil {
		t.Errorf("expected start time on inc dump, got %v", *start)
	}
	if !(*end).After(beforeInc) {
		t.Errorf("expected now (%s) to be in (%s, %s)", beforeInc, start, end)
	}
	if dataSize <= 0 {
		t.Errorf("expected dataSize to be >0 got : %d", dataSize)
	}
	// We added affectedRows and removed affectedRows, so there should be 2*
	// affectedRows in the dump.
	if expected := affectedRows * 2; rows != uint64(expected) {
		t.Errorf("expected %d got: %d", expected, rows)
	}

	sqlDB.Exec(t, `CREATE TABLE data.details1 (c INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO data.details1 (SELECT generate_series(1, 100))`)
	sqlDB.Exec(t, `ALTER TABLE data.details1 SPLIT AT VALUES (1), (42)`)
	sqlDB.Exec(t, `CREATE TABLE data.details2()`)
	sqlDB.Exec(t, `DUMP data.details1, data.details2 TO SST $1;`, details)

	details1Desc := sqlbase.GetTableDescriptor(tc.Server(0).DB(), "data", "public", "details1")
	details2Desc := sqlbase.GetTableDescriptor(tc.Server(0).DB(), "data", "public", "details2")
	details1Key := roachpb.Key(sqlbase.MakeIndexKeyPrefix(details1Desc, details1Desc.PrimaryIndex.ID))
	details2Key := roachpb.Key(sqlbase.MakeIndexKeyPrefix(details2Desc, details2Desc.PrimaryIndex.ID))

	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SHOW DUMP RANGES '%s'`, details), [][]string{
		{"/Table/57/1", "/Table/57/2", string(details1Key), string(details1Key.PrefixEnd())},
		{"/Table/58/1", "/Table/58/2", string(details2Key), string(details2Key.PrefixEnd())},
	})

	var showFiles = fmt.Sprintf(`SELECT start_pretty, end_pretty, size_bytes, rows
		FROM [SHOW DUMP FILES '%s']`, details)
	sqlDB.CheckQueryResults(t, showFiles, [][]string{
		{"/Table/57/1/1", "/Table/57/1/42", "369", "41"},
		{"/Table/57/1/42", "/Table/57/2", "531", "59"},
	})
	sstMatcher := regexp.MustCompile(`\d+\.sst`)
	pathRows := sqlDB.QueryStr(t, `SELECT path FROM [SHOW DUMP FILES $1]`, details)
	for _, row := range pathRows {
		path := row[0]
		if matched := sstMatcher.MatchString(path); !matched {
			t.Errorf("malformatted path in SHOW DUMP FILES: %s", path)
		}
	}
	if len(pathRows) != 2 {
		t.Fatalf("expected 2 files, but got %d", len(pathRows))
	}
}
