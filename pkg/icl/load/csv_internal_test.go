// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/CCL.txt

package load

import (
	"context"
	"testing"

	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestMakeSimpleTableDescriptorErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		stmt  string
		error string
	}{
		{
			stmt:  "create table if not exists a (i int)",
			error: "unsupported IF NOT EXISTS",
		},
		{
			stmt:  "create table a (i int) interleave in parent b (id)",
			error: "interleaved not supported",
		},
		{
			stmt:  "create table a as select 1",
			error: "CREATE AS not supported",
		},
		{
			stmt:  "create table a (i int references b (id))",
			error: `this IMPORT format does not support foreign keys`,
		},
		{
			stmt:  "create table a (i int, constraint a foreign key (i) references c (id))",
			error: `this IMPORT format does not support foreign keys`,
		},
		{
			stmt: `create table a (
				i int check (i > 0),
				b int default 1,
				c serial,
				constraint a check (i < 0),
				primary key (i),
				unique index (i),
				index (i),
				family (i)
			)`,
		},
	}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	for _, tc := range tests {
		t.Run(tc.stmt, func(t *testing.T) {
			stmt, err := parser.ParseOne(tc.stmt, false)
			if err != nil {
				t.Fatal(err)
			}
			create, ok := stmt.AST.(*tree.CreateTable)
			if !ok {
				t.Fatal("expected CREATE TABLE statement in table file")
			}
			_, err = MakeSimpleTableDescriptor(ctx, st, create, defaultCSVParentID, defaultCSVTableID, NoFKs, 0, nil, sqlbase.AdminRole)
			if !testutils.IsError(err, tc.error) {
				t.Fatalf("expected %v, got %+v", tc.error, err)
			}
		})
	}
}
