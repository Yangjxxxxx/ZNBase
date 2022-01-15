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
	"reflect"
	"testing"

	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestMakeTableDescColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		sqlType  string
		colType  sqlbase.ColumnType
		nullable bool
	}{
		{
			"BIT",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BIT, Width: 1, VisibleTypeName: "BIT"},
			true,
		},
		{
			"BIT(3)",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BIT, Width: 3, VisibleTypeName: "BIT(3)"},
			true,
		},
		{
			"VARBIT",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BIT, Width: 0, VisibleType: sqlbase.ColumnType_VARBIT, VisibleTypeName: "VARBIT"},
			true,
		},
		{
			"VARBIT(3)",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BIT, Width: 3, VisibleType: sqlbase.ColumnType_VARBIT, VisibleTypeName: "VARBIT(3)"},
			true,
		},
		{
			"BOOLEAN",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL, VisibleTypeName: "BOOLEAN"},
			true,
		},
		{
			"INT",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, VisibleType: sqlbase.ColumnType_BIGINT, Width: 64, VisibleTypeName: "INT"},
			true,
		},
		{
			"INT2",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, VisibleType: sqlbase.ColumnType_SMALLINT, Width: 16, VisibleTypeName: "INT2"},
			true,
		},
		{
			"INT4",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, VisibleType: sqlbase.ColumnType_INTEGER, Width: 32, VisibleTypeName: "INT4"},
			true,
		},
		{
			"INT8",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, VisibleType: sqlbase.ColumnType_BIGINT, Width: 64, VisibleTypeName: "INT8"},
			true,
		},
		{
			"INT64",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, VisibleType: sqlbase.ColumnType_BIGINT, Width: 64, VisibleTypeName: "INT64"},
			true,
		},
		{
			"BIGINT",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, VisibleType: sqlbase.ColumnType_BIGINT, Width: 64, VisibleTypeName: "BIGINT"},
			true,
		},
		{
			"FLOAT(3)",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT, Precision: 16, VisibleType: sqlbase.ColumnType_REAL, VisibleTypeName: "FLOAT4"},
			true,
		},
		{
			"DOUBLE PRECISION",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT, Precision: 32, VisibleTypeName: "DOUBLE PRECISION"},
			true,
		},
		{
			"DECIMAL(6,5)",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL, Precision: 6, Width: 5, VisibleTypeName: "DECIMAL(6,5)"},
			true,
		},
		{
			"DATE",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DATE, VisibleTypeName: "DATE"},
			true,
		},
		{
			"TIME",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_TIME, VisibleTypeName: "TIME"},
			true,
		},
		{
			"TIMESTAMP",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_TIMESTAMP, VisibleTypeName: "TIMESTAMP"},
			true,
		},
		{
			"INTERVAL",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INTERVAL, VisibleTypeName: "INTERVAL"},
			true,
		},
		{
			"CHAR",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING, VisibleType: sqlbase.ColumnType_CHAR, Width: 1, VisibleTypeName: "CHAR"},
			true,
		},
		{
			"CHAR(3)",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING, VisibleType: sqlbase.ColumnType_CHAR, Width: 3, VisibleTypeName: "CHAR(3)"},
			true,
		},
		{
			"VARCHAR",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING, VisibleType: sqlbase.ColumnType_VARCHAR, Width: 0, VisibleTypeName: "VARCHAR"},
			true,
		},
		{
			"VARCHAR(3)",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING, VisibleType: sqlbase.ColumnType_VARCHAR, Width: 3, VisibleTypeName: "VARCHAR(3)"},
			true,
		},
		{
			"TEXT",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING, VisibleTypeName: "TEXT"},
			true,
		},
		{
			`"char"`,
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING, VisibleType: sqlbase.ColumnType_QCHAR, VisibleTypeName: "CHAR"},
			true,
		},
		{
			"BLOB",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES, VisibleTypeName: "BLOB"},
			true,
		},
		{
			"INT NOT NULL",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, VisibleType: sqlbase.ColumnType_BIGINT, Width: 64, VisibleTypeName: "INT"},
			false,
		},
		{
			"INT NULL",
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, VisibleType: sqlbase.ColumnType_BIGINT, Width: 64, VisibleTypeName: "INT"},
			true,
		},
	}
	for i, d := range testData {
		s := "CREATE TABLE foo.test (a " + d.sqlType + " PRIMARY KEY, b " + d.sqlType + ")"
		schema, err := CreateTestTableDescriptor(context.TODO(), 1, 100, s, sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, sqlbase.AdminRole))
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		if d.colType.VisibleType != 0 {
			d.colType.VisibleTypeName = d.colType.SQLString()
		}

		if !reflect.DeepEqual(d.colType, schema.Columns[0].Type) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.colType, schema.Columns[0].Type)
		}
		if schema.Columns[0].Nullable {
			t.Fatalf("%d: expected non-nullable primary key, but got %+v", i, schema.Columns[0].Nullable)
		}
		if !reflect.DeepEqual(d.colType, schema.Columns[1].Type) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.colType, schema.Columns[1])
		}
		if d.nullable != schema.Columns[1].Nullable {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.nullable, schema.Columns[1].Nullable)
		}
	}
}

func TestMakeTableDescIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		sql     string
		primary sqlbase.IndexDescriptor
		indexes []sqlbase.IndexDescriptor
	}{
		{
			"a INT PRIMARY KEY",
			sqlbase.IndexDescriptor{
				Name:             sqlbase.PrimaryKeyIndexName,
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"a"},
				ColumnIDs:        []sqlbase.ColumnID{1},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{},
		},
		{
			"a INT UNIQUE, b INT PRIMARY KEY",
			sqlbase.IndexDescriptor{
				Name:             "primary",
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"b"},
				ColumnIDs:        []sqlbase.ColumnID{2},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{
				{
					Name:             "test_a_key",
					ID:               2,
					Unique:           true,
					ColumnNames:      []string{"a"},
					ColumnIDs:        []sqlbase.ColumnID{1},
					ExtraColumnIDs:   []sqlbase.ColumnID{2},
					ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
				},
			},
		},
		{
			"a INT, b INT, CONSTRAINT c PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             "c",
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnIDs:        []sqlbase.ColumnID{1, 2},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{},
		},
		{
			"a INT, b INT, CONSTRAINT c UNIQUE (b), PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             "primary",
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnIDs:        []sqlbase.ColumnID{1, 2},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{
				{
					Name:             "c",
					ID:               2,
					Unique:           true,
					ColumnNames:      []string{"b"},
					ColumnIDs:        []sqlbase.ColumnID{2},
					ExtraColumnIDs:   []sqlbase.ColumnID{1},
					ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
				},
			},
		},
		{
			"a INT, b INT, PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             sqlbase.PrimaryKeyIndexName,
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnIDs:        []sqlbase.ColumnID{1, 2},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{},
		},
	}
	for i, d := range testData {
		s := "CREATE TABLE foo.test (" + d.sql + ")"
		schema, err := CreateTestTableDescriptor(context.TODO(), 1, 100, s, sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, sqlbase.AdminRole))
		if err != nil {
			t.Fatalf("%d (%s): %v", i, d.sql, err)
		}
		if !reflect.DeepEqual(d.primary, schema.PrimaryIndex) {
			t.Fatalf("%d (%s): primary mismatch: expected %+v, but got %+v", i, d.sql, d.primary, schema.PrimaryIndex)
		}
		if !reflect.DeepEqual(d.indexes, append([]sqlbase.IndexDescriptor{}, schema.Indexes...)) {
			t.Fatalf("%d (%s): index mismatch: expected %+v, but got %+v", i, d.sql, d.indexes, schema.Indexes)
		}

	}
}

func TestPrimaryKeyUnspecified(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := "CREATE TABLE foo.test (a INT, b INT, CONSTRAINT c UNIQUE (b))"
	desc, err := CreateTestTableDescriptor(context.TODO(), 1, 100, s, sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, sqlbase.AdminRole))
	if err != nil {
		t.Fatal(err)
	}
	desc.PrimaryIndex = sqlbase.IndexDescriptor{}

	err = desc.ValidateTable(cluster.MakeTestingClusterSettings())
	if !testutils.IsError(err, sqlbase.ErrMissingPrimaryKey.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
}
