// Copyright 2015  The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"testing"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestMakeDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stmt, err := parser.ParseOne("CREATE DATABASE test", false)
	if err != nil {
		t.Fatal(err)
	}
	const id = 7
	desc := NewInitialDatabaseDescriptor(id, string(stmt.AST.(*tree.CreateDatabase).Name), sqlbase.AdminRole)
	if desc.Name != "test" {
		t.Fatalf("expected Name == test, got %s", desc.Name)
	}
	// ID is not set yet.
	if desc.ID != id {
		t.Fatalf("expected ID == 0, got %d", desc.ID)
	}
	if len(desc.GetPrivileges().Users) != 2 {
		t.Fatalf("wrong number of privilege users, expected 2, got: %d", len(desc.GetPrivileges().Users))
	}
}

func TestDatabaseAccessors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	if err := kvDB.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		if _, err := getDatabaseDescByID(ctx, txn, sqlbase.SystemDB.ID); err != nil {
			return err
		}
		if _, err := MustGetDatabaseDescByID(ctx, txn, sqlbase.SystemDB.ID); err != nil {
			return err
		}

		databaseCache := newDatabaseCache(config.NewSystemConfig())
		_, err := databaseCache.getDatabaseDescByID(ctx, txn, sqlbase.SystemDB.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGetCachedDatabaseID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	databaseCache := newDatabaseCache(config.NewSystemConfig())
	databaseCache.setID("test", 2)
	ID, err := databaseCache.getCachedDatabaseID("test")
	if err != nil {
		t.Fatal(err)
	}
	if ID != 2 {
		t.Fatalf("expected ID == 2, got %d", ID)
	}
	ID, err = databaseCache.getCachedDatabaseID("mistake")
	if err != nil {
		t.Fatal(err)
	}
	if ID != 0 {
		t.Fatalf("expected ID == 0, got %d", ID)
	}
	ID, err = databaseCache.getCachedDatabaseID("system")
	if err != nil {
		t.Fatal(err)
	}
	if ID != 1 {
		t.Fatalf("expected ID == 1, got %d", ID)
	}
}
