package sql

import (
	"context"
	"testing"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

func TestMakeSchemaDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stmt, err := parser.ParseOne("CREATE SCHEMA test", false)
	if err != nil {
		t.Fatal(err)
	}
	desc := makeSchemaDesc(stmt.AST.(*tree.CreateSchema), sqlbase.AdminRole)
	if desc.Name != "test" {
		t.Fatalf("expected Schema Name == test, got %s", desc.Name)
	}
	if desc.ID != 0 {
		t.Fatalf("expected ID == 0, got %d", desc.ID)
	}
	if len(desc.GetPrivileges().Users) != 2 {
		t.Fatalf("wrong number of privilege users, expected 2, got: %d", len(desc.GetPrivileges().Users))
	}
}

func TestGetDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stmt, err := parser.ParseOne("CREATE SCHEMA test", false)
	if err != nil {
		t.Fatal(err)
	}
	desc := makeSchemaDesc(stmt.AST.(*tree.CreateSchema), sqlbase.AdminRole)

	//make txn
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	txn := client.NewTxn(ctx, db, s.NodeID(), client.RootTxn)
	defer s.Stopper().Stop(ctx)
	dbName := "defaultdb"
	dbDesc, err := getDatabaseDesc(ctx, txn, dbName)
	if err != nil {
		t.Fatal(err)
	}
	if dbDesc.ID != 50 {
		t.Fatalf("expected ID == 0, got %d", desc.ID)
	}
	if dbDesc.Name != "defaultdb" {
		t.Fatalf("expected Schema Name == test, got %s", desc.Name)
	}
	if len(desc.GetPrivileges().Users) != 2 {
		t.Fatalf("wrong number of privilege users, expected 2, got: %d", len(desc.GetPrivileges().Users))
	}
	if len(dbDesc.Schemas) != 1 {
		t.Fatalf("wrong number of schema, expected 1, got: %d", len(dbDesc.Schemas))
	}
}

func TestPlanner_CreateSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stmt, err := parser.ParseOne("CREATE SCHEMA t", false)
	if err != nil {
		t.Fatal(err)
	}
	desc := makeSchemaDesc(stmt.AST.(*tree.CreateSchema), sqlbase.AdminRole)

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	internalPlanner, _ := NewInternalPlanner(
		"test",
		client.NewTxn(ctx, db, s.NodeID(), client.RootTxn),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	p := internalPlanner.(*planner)
	idCreateSuccess, err := p.createSchema(ctx, &desc, false)
	if err != nil {
		t.Fatal(err)
	}
	if !idCreateSuccess {
		t.Fatalf("create schema should be succeed, but failed")
	}
	defer s.Stopper().Stop(ctx)
}

func TestPlanner_UpdateDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	txn := client.NewTxn(ctx, db, s.NodeID(), client.RootTxn)
	defer s.Stopper().Stop(ctx)
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	internalPlanner, _ := NewInternalPlanner(
		"test",
		client.NewTxn(ctx, db, s.NodeID(), client.RootTxn),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	p := internalPlanner.(*planner)
	dbName := "defaultdb"
	dbDesc, err := getDatabaseDesc(ctx, txn, dbName)
	if err != nil {
		t.Fatal(err)
	}
	b := &client.Batch{}
	err = p.UpdateDescriptor(ctx, b, dbDesc)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetSchemaID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	txn := client.NewTxn(ctx, db, s.NodeID(), client.RootTxn)
	defer s.Stopper().Stop(ctx)
	stmt, err := parser.ParseOne("CREATE SCHEMA test", false)
	if err != nil {
		t.Fatal(err)
	}
	desc := makeSchemaDesc(stmt.AST.(*tree.CreateSchema), sqlbase.AdminRole)
	desc.ID = 2
	desc.ParentID = 10
	config := config.NewSystemConfig()
	nameKey := sqlbase.MakeDescMetadataKey(2)
	data, err := protoutil.Marshal(&desc)
	if err != nil {
		t.Fatal(err)
	}
	var keyValue roachpb.KeyValue
	keyValue.Key = nameKey
	keyValue.Value.RawBytes = data
	config.Values = append(config.Values, keyValue)
	testcases := []struct {
		name     string
		parentID sqlbase.ID
		required bool
	}{
		{
			name:     "system",
			parentID: sqlbase.InvalidID,
			required: false,
		},
		{
			name:     "system",
			parentID: 10,
			required: false,
		},
		{
			name:     "system",
			parentID: 10,
			required: true,
		},
		{
			name:     "test",
			parentID: 10,
			required: true,
		},
	}
	for _, a := range testcases {
		ID, err := getSchemaID(ctx, txn, a.parentID, a.name, a.required)
		if !a.required {
			if ID == sqlbase.InvalidID && err != nil {
				t.Error(err)
			}
		}
		if a.required {
			if ID == sqlbase.InvalidID && err == nil {
				t.Error(sqlbase.NewUndefinedSchemaError(a.name))
			}
			if ID != sqlbase.InvalidID && err != nil {
				t.Error(err)
			}
		}
	}
}
