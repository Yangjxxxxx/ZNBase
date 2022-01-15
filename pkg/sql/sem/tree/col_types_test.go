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

package tree_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

func TestParseColumnType(t *testing.T) {
	testData := []struct {
		str          string
		expectedType coltypes.T
	}{
		{"BIT", coltypes.Bit},
		{"VARBIT", coltypes.VarBit},
		{"BIT(2)", &coltypes.TBitArray{Width: 2, VisibleType: coltypes.VisBIT}},
		{"VARBIT(2)", &coltypes.TBitArray{Width: 2, Variable: true, VisibleType: coltypes.VisVARBIT}},
		{"BOOL", coltypes.Bool},
		{"INT2", coltypes.Int2},
		{"INT4", coltypes.Int4},
		{"INT8", coltypes.Int8},
		{"FLOAT4", coltypes.Float4},
		{"FLOAT8", coltypes.Float8},
		{"DECIMAL", coltypes.Decimal},
		{"DECIMAL(8)", &coltypes.TDecimal{Prec: 8, VisibleType: coltypes.VisDECIMAL}},
		{"DECIMAL(9,10)", &coltypes.TDecimal{Prec: 9, Scale: 10, VisibleType: coltypes.VisDECIMAL}},
		{"UUID", coltypes.UUID},
		{"INET", coltypes.INet},
		{"DATE", coltypes.Date},
		{"JSONB", coltypes.JSONB},
		{"TIME", coltypes.Time},
		{"TIMESTAMP", coltypes.Timestamp},
		{"TIMESTAMPTZ", coltypes.TimestampTZ},
		{"INTERVAL", coltypes.Interval},
		{"STRING", coltypes.String},
		{"CHAR", coltypes.Char},
		{"CHAR(11)", &coltypes.TString{Variant: coltypes.TStringVariantCHAR, N: 11, VisibleType: coltypes.VisCHAR}},
		{"VARCHAR", coltypes.VarChar},
		{"VARCHAR(2)", &coltypes.TString{Variant: coltypes.TStringVariantVARCHAR, N: 2, VisibleType: coltypes.VisVARCHAR}},
		{`"char"`, coltypes.QChar},
		{"BYTES", coltypes.Bytes},
		{"STRING COLLATE da", &coltypes.TCollatedString{TString: *coltypes.String, Locale: "da"}},
		{"CHAR COLLATE de", &coltypes.TCollatedString{TString: *coltypes.Char, Locale: "de"}},
		{"CHAR(11) COLLATE de", &coltypes.TCollatedString{TString: coltypes.TString{Variant: coltypes.TStringVariantCHAR, N: 11, VisibleType: coltypes.VisCHAR}, Locale: "de"}},
		{"VARCHAR COLLATE en", &coltypes.TCollatedString{TString: *coltypes.VarChar, Locale: "en"}},
		{"VARCHAR(2) COLLATE en", &coltypes.TCollatedString{TString: coltypes.TString{Variant: coltypes.TStringVariantVARCHAR, N: 2, VisibleType: coltypes.VisVARCHAR}, Locale: "en"}},
	}
	for i, d := range testData {
		t.Run(d.str, func(t *testing.T) {
			sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
			stmt, err := parser.ParseOne(sql, false)
			if err != nil {
				t.Fatalf("%d: %s", i, err)
			}
			if sql != stmt.AST.String() {
				t.Errorf("%d: expected %s, but got %s", i, sql, stmt.AST)
			}
			createTable, ok := stmt.AST.(*tree.CreateTable)
			if !ok {
				t.Fatalf("%d: expected tree.CreateTable, but got %T", i, stmt)
			}
			columnDef, ok2 := createTable.Defs[0].(*tree.ColumnTableDef)
			if !ok2 {
				t.Fatalf("%d: expected tree.ColumnTableDef, but got %T", i, createTable.Defs[0])
			}
			if !reflect.DeepEqual(d.expectedType, columnDef.Type) {
				t.Fatalf("%d: expected %s, but got %s", i, d.expectedType, columnDef.Type)
			}
		})
	}
}

func TestParseColumnTypeAliases(t *testing.T) {
	testData := []struct {
		str          string
		expectedStr  string
		expectedType coltypes.T
	}{
		// FLOAT has always been FLOAT8
		{"FLOAT", "CREATE TABLE a (b FLOAT8)", &coltypes.TFloat{Short: false, VisibleType: coltypes.VisFLOAT}},
		// A "naked" INT is 64 bits, for historical compatibility.
		{"INT", "CREATE TABLE a (b INT8)", &coltypes.TInt{Width: 64, VisibleType: coltypes.VisINT}},
		{"INTEGER", "CREATE TABLE a (b INT8)", &coltypes.TInt{Width: 64, VisibleType: coltypes.VisINTEGER}},
	}
	for i, d := range testData {
		t.Run(d.str, func(t *testing.T) {
			sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
			stmt, err := parser.ParseOne(sql, false)
			if err != nil {
				t.Fatalf("%d: %s", i, err)
			}
			if d.expectedStr != stmt.AST.String() {
				t.Errorf("%d: expected %s, but got %s", i, d.expectedStr, stmt.AST)
			}
			createTable, ok := stmt.AST.(*tree.CreateTable)
			if !ok {
				t.Fatalf("%d: expected tree.CreateTable, but got %T", i, stmt.AST)
			}
			columnDef, ok2 := createTable.Defs[0].(*tree.ColumnTableDef)
			if !ok2 {
				t.Fatalf("%d: expected tree.ColumnTableDef, but got %T", i, createTable.Defs[0])
			}
			if !reflect.DeepEqual(d.expectedType, columnDef.Type) {
				t.Fatalf("%d: expected %s, but got %s", i, d.expectedType, columnDef.Type)
			}
		})
	}
}
