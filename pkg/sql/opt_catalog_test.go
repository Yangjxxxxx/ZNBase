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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

type fkHandler struct {
	allowed  bool
	skip     bool
	resolver fkResolver
}

type fkResolver map[string]*sqlbase.MutableTableDescriptor

func (fkResolver) LookupObject(
	ctx context.Context, requireMutation bool, requireFunction bool, dbName, scName, obName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	panic("implement me")
}

func (fkResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	panic("implement me")
}

func (fkResolver) Txn() *client.Txn {
	panic("implement me")
}

func (fkResolver) LogicalSchemaAccessor() SchemaAccessor {
	panic("implement me")
}

func (fkResolver) CurrentDatabase() string {
	panic("implement me")
}

func (fkResolver) CurrentSearchPath() sessiondata.SearchPath {
	panic("implement me")
}

func (fkResolver) CommonLookupFlags(required bool) CommonLookupFlags {
	panic("implement me")
}

func (fkResolver) ObjectLookupFlags(required bool, requireMutable bool) ObjectLookupFlags {
	panic("implement me")
}

func (fkResolver) LookupTableByID(ctx context.Context, id sqlbase.ID) (row.TableEntry, error) {
	panic("implement me")
}

func makeSimpleTableDescriptor(
	ctx context.Context,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID, tableID sqlbase.ID,
	fks fkHandler,
	walltime int64,
) (*sqlbase.MutableTableDescriptor, error) {
	create.HoistConstraints()
	if create.IfNotExists {
		return nil, pgerror.Unimplemented("import.if-no-exists", "unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return nil, pgerror.Unimplemented("import.interleave", "interleaved not supported")
	}
	if create.AsSource != nil {
		return nil, pgerror.Unimplemented("import.create-as", "CREATE AS not supported")
	}

	filteredDefs := create.Defs[:0]
	for i := range create.Defs {
		switch def := create.Defs[i].(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if def.Computed.Expr != nil {
				return nil, pgerror.Unimplemented("import.computed", "computed columns not supported: %s", tree.AsString(def))
			}

			if err := SimplifySerialInColumnDefWithRowID(ctx, def, &create.Table); err != nil {
				return nil, err
			}

		case *tree.ForeignKeyConstraintTableDef:
			if !fks.allowed {
				return nil, pgerror.Unimplemented("import.fk", "this IMPORT format does not support foreign keys")
			}
			if fks.skip {
				continue
			}
			// Strip the schema/db prefix.
			def.Table = tree.MakeUnqualifiedTableName(def.Table.TableName)

		default:
			return nil, pgerror.Unimplemented(fmt.Sprintf("import.%T", def), "unsupported table definition: %s", tree.AsString(def))
		}
		// only append this def after we make it past the error checks and continues
		filteredDefs = append(filteredDefs, create.Defs[i])
	}
	create.Defs = filteredDefs

	semaCtx := tree.SemaContext{}
	evalCtx := tree.EvalContext{
		Context:  ctx,
		Sequence: nil,
	}
	affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)

	tableDesc, err := MakeTableDesc(
		ctx,
		nil, /* txn */
		fks.resolver,
		st,
		create,
		parentID,
		tableID,
		hlc.Timestamp{WallTime: walltime},
		sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, sqlbase.AdminRole),
		affected,
		&semaCtx,
		&evalCtx,
		nil,
		nil,
	)
	if err != nil {
		return nil, err
	}
	if err := fixDescriptorFKState(tableDesc.TableDesc()); err != nil {
		return nil, err
	}

	return &tableDesc, nil
}

func fixDescriptorFKState(tableDesc *sqlbase.TableDescriptor) error {
	tableDesc.State = sqlbase.TableDescriptor_PUBLIC
	return tableDesc.ForeachNonDropIndex(func(idx *sqlbase.IndexDescriptor) error {
		if idx.ForeignKey.IsSet() {
			idx.ForeignKey.Validity = sqlbase.ConstraintValidity_Unvalidated
		}
		return nil
	})
}

// Note that zonesAreEqual only tests equality on fields used by the optimizer.
func TestZonesAreEqual(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test replica constraint equality.
	zone1 := &config.ZoneConfig{}
	require.Equal(t, zone1, zone1)

	zone2 := &config.ZoneConfig{}
	require.Equal(t, zone1, zone2)

	constraints1 := []config.Constraints{
		{NumReplicas: 0, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
	}
	zone3 := &config.ZoneConfig{Constraints: constraints1}
	zone4 := &config.ZoneConfig{Constraints: constraints1}
	require.Equal(t, zone3, zone4)

	zone5 := &config.ZoneConfig{Constraints: []config.Constraints{
		{NumReplicas: 3, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
	}}
	require.NotEqual(t, zone4, zone5)

	constraints2 := []config.Constraints{
		{NumReplicas: 3, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
		{NumReplicas: 1, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k2", Value: "v2"},
		}},
	}
	zone6 := &config.ZoneConfig{Constraints: constraints2}
	require.NotEqual(t, zone5, zone6)

	zone7 := &config.ZoneConfig{Constraints: []config.Constraints{
		{NumReplicas: 3, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		}},
		{NumReplicas: 1, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k3", Value: "v3"},
		}},
	}}
	require.NotEqual(t, zone6, zone7)

	zone8 := &config.ZoneConfig{Constraints: []config.Constraints{
		{NumReplicas: 0, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
			{Type: config.Constraint_REQUIRED, Key: "k2", Value: "v2"},
		}},
	}}
	require.NotEqual(t, zone3, zone8)

	zone9 := &config.ZoneConfig{Constraints: []config.Constraints{
		{NumReplicas: 0, Constraints: []config.Constraint{
			{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
			{Type: config.Constraint_REQUIRED, Key: "k2", Value: "v3"},
		}},
	}}
	require.NotEqual(t, zone8, zone9)

	// Test subzone equality.
	subzones1 := []config.Subzone{{IndexID: 0, Config: *zone3}}
	zone20 := &config.ZoneConfig{Constraints: constraints1, Subzones: subzones1}
	require.NotEqual(t, zone3, zone20)

	zone21 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, Config: *zone3},
	}}
	require.NotEqual(t, zone20, zone21)

	zone22 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, PartitionName: "p1", Config: *zone3},
	}}
	require.NotEqual(t, zone21, zone22)

	zone23 := &config.ZoneConfig{Constraints: constraints1, Subzones: []config.Subzone{
		{IndexID: 1, Config: *zone3},
		{IndexID: 2, PartitionName: "p1", Config: *zone3},
	}}
	require.NotEqual(t, zone21, zone23)

	// Test leaseholder preference equality.
	leasePrefs1 := []config.LeasePreference{{Constraints: []config.Constraint{
		{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
	}}}
	zone43 := &config.ZoneConfig{Constraints: constraints1, LeasePreferences: leasePrefs1}
	zone44 := &config.ZoneConfig{Constraints: constraints1, LeasePreferences: leasePrefs1}
	require.Equal(t, zone43, zone44)

	leasePrefs2 := []config.LeasePreference{{Constraints: []config.Constraint{
		{Type: config.Constraint_REQUIRED, Key: "k1", Value: "v1"},
		{Type: config.Constraint_REQUIRED, Key: "k2", Value: "v2"},
	}}}
	zone45 := &config.ZoneConfig{Constraints: constraints1, LeasePreferences: leasePrefs2}
	require.NotEqual(t, zone43, zone45)

	zone46 := &config.ZoneConfig{LeasePreferences: leasePrefs1}
	require.NotEqual(t, zone43, zone46)

	zone47 := &config.ZoneConfig{
		Constraints: constraints1,
		LeasePreferences: []config.LeasePreference{{Constraints: []config.Constraint{
			{Type: config.Constraint_PROHIBITED, Key: "k1", Value: "v1"},
		}}},
	}
	require.NotEqual(t, zone43, zone47)
}

func TestCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var NoFKs = fkHandler{resolver: make(fkResolver)}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	stmt, _ := parser.ParseOne("create table t (a int check (a > 0) disable , b int check (b > 0) enable , c int,constraint check_c check (c > 0) disable)", false)
	create, _ := stmt.AST.(*tree.CreateTable)
	a, _ := makeSimpleTableDescriptor(ctx, st, create, 52, 53, NoFKs, 0)
	b := sqlbase.NewImmutableTableDescriptor(a.TableDescriptor)

	test := optTable{
		desc: b,
	}

	t.Run("test1", func(t *testing.T) {
		got := string(test.Check(0)) //check_c
		want := "true"
		if got != want {
			t.Errorf("got '%s' want '%s'", got, want)
		}
	})
	t.Run("test2", func(t *testing.T) {
		got := string(test.Check(1)) //check_a
		want := "true"
		if got != want {
			t.Errorf("got '%s' want '%s'", got, want)
		}
	})
	t.Run("test3", func(t *testing.T) {
		got := string(test.Check(2)) //check_b
		want := "b > 0"
		if got != want {
			t.Errorf("got '%s' want '%s'", got, want)
		}
	})
}
