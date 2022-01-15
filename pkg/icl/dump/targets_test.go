// Copyright 2016  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package dump

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestDescriptorsMatchingTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()

	descriptors := []sqlbase.Descriptor{
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{ID: 1, Name: "system"}),
		*sqlbase.WrapDescriptor(&sqlbase.TableDescriptor{ID: 2, Name: "foo", ParentID: 1}),
		*sqlbase.WrapDescriptor(&sqlbase.TableDescriptor{ID: 3, Name: "bar", ParentID: 1}),
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{ID: 4, Name: "data"}),
		*sqlbase.WrapDescriptor(&sqlbase.SchemaDescriptor{ID: 5, Name: "public", ParentID: 4}),
		*sqlbase.WrapDescriptor(&sqlbase.TableDescriptor{ID: 6, Name: "baz", ParentID: 5}),
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{ID: 7, Name: "empty"}),
		*sqlbase.WrapDescriptor(&sqlbase.SchemaDescriptor{ID: 8, Name: "public", ParentID: 7}),
	}

	tests := []struct {
		sessionDatabase string
		pattern         string
		expected        []string
		expectedDBs     []string
		err             string
	}{
		{"", "DATABASE system", []string{"system", "foo", "bar"}, []string{"system"}, ``},
		{"", "DATABASE system, noexist", nil, nil, `unknown database "noexist"`},
		{"", "DATABASE system, system", []string{"system", "foo", "bar"}, []string{"system"}, ``},
		{"", "DATABASE data", []string{"data", "public", "baz"}, []string{"data"}, ``},
		{"", "DATABASE system, data", []string{"system", "foo", "bar", "data", "public", "baz"}, []string{"data", "system"}, ``},
		{"", "DATABASE system, data, noexist", nil, nil, `unknown database "noexist"`},
		{"system", "DATABASE system", []string{"system", "foo", "bar"}, []string{"system"}, ``},
		{"system", "DATABASE system, noexist", nil, nil, `unknown database "noexist"`},
		{"system", "DATABASE data", []string{"data", "public", "baz"}, []string{"data"}, ``},
		{"system", "DATABASE system, data", []string{"system", "foo", "bar", "data", "public", "baz"}, []string{"data", "system"}, ``},
		{"system", "DATABASE system, data, noexist", nil, nil, `unknown database "noexist"`},

		{"", "TABLE foo", nil, nil, `table "foo" does not exist`},
		{"system", "TABLE foo", []string{"system", "foo"}, nil, ``},
		{"system", "TABLE foo, foo", []string{"system", "foo"}, nil, ``},
		{"data", "TABLE foo", nil, nil, `table "foo" does not exist`},

		{"", "TABLE *", nil, nil, `"\*" does not match any valid database or schema`},
		{"", "TABLE *, system.public.foo", nil, nil, `"\*" does not match any valid database or schema`},
		{"noexist", "TABLE *", nil, nil, `"\*" does not match any valid database or schema`},
		{"system", "TABLE *", []string{"system", "foo", "bar"}, nil, ``},
		{"data", "TABLE *", []string{"data", "public", "baz"}, nil, ``},
		{"empty", "TABLE *", []string{"empty", "public"}, nil, ``},

		{"", "TABLE foo, baz", nil, nil, `table "(foo|baz)" does not exist`},
		{"system", "TABLE foo, baz", nil, nil, `table "baz" does not exist`},
		{"data", "TABLE foo, baz", nil, nil, `table "foo" does not exist`},

		{"", "TABLE system.foo", []string{"system", "foo"}, nil, ``},
		{"", "TABLE system.foo, foo", []string{"system", "foo"}, nil, `table "foo" does not exist`},
		{"", "TABLE system.public.foo", []string{"system", "foo"}, nil, ``},
		{"", "TABLE system.public.foo, foo", []string{"system", "foo"}, nil, `table "foo" does not exist`},

		{"", "TABLE system.public.foo, bar", []string{"system", "foo"}, nil, `table "bar" does not exist`},
		{"", "TABLE system.foo, bar", []string{"system", "foo"}, nil, `table "bar" does not exist`},
		{"system", "TABLE system.public.foo, bar", []string{"system", "foo", "bar"}, nil, ``},
		{"system", "TABLE system.foo, bar", []string{"system", "foo", "bar"}, nil, ``},

		{"", "TABLE noexist.*", nil, nil, `"noexist\.\*" does not match any valid database or schema`},
		{"", "TABLE empty.*", []string{"empty", "public"}, nil, ``},
		{"", "TABLE system.*", []string{"system", "foo", "bar"}, nil, ``},
		{"", "TABLE system.public.*", []string{"system", "foo", "bar"}, nil, ``},
		{"", "TABLE system.public.*, foo, baz", nil, nil, `table "(foo|baz)" does not exist`},
		{"system", "TABLE system.public.*, foo, baz", nil, nil, `table "baz" does not exist`},
		{"data", "TABLE system.public.*, baz", []string{"system", "foo", "bar", "data", "public", "baz"}, nil, ``},
		{"data", "TABLE system.public.*, foo, baz", nil, nil, `table "(foo|baz)" does not exist`},

		{"", "TABLE SyStEm.FoO", []string{"system", "foo"}, nil, ``},
		{"", "TABLE SyStEm.pUbLic.FoO", []string{"system", "foo"}, nil, ``},

		{"", `TABLE system."foo"`, []string{"system", "foo"}, nil, ``},
		{"", `TABLE system.public."foo"`, []string{"system", "foo"}, nil, ``},
		{"system", `TABLE "foo"`, []string{"system", "foo"}, nil, ``},
		// TODO(dan): Enable these tests once #8862 is fixed.
		// {"", `TABLE system."FOO"`, []string{"system"}},
		// {"system", `TABLE "FOO"`, []string{"system"}},
	}

	searchPath := sessiondata.MakeSearchPath([]string{"public", "pg_catalog"})
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d/%s/%s", i, test.sessionDatabase, test.pattern), func(t *testing.T) {
			sql := fmt.Sprintf(`GRANT ALL ON %s TO ignored`, test.pattern)
			stmt, err := parser.ParseOne(sql, false)
			if err != nil {
				t.Fatal(err)
			}
			targets := stmt.AST.(*tree.Grant).Targets

			resolver := &sqlDescriptorResolver{
				allDescs:   descriptors,
				descByID:   make(map[sqlbase.ID]sqlbase.Descriptor),
				dbsByName:  make(map[string]sqlbase.ID),
				scsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
				schsByName: make(map[string]sqlbase.ID),
				tbsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
				tbsByID:    make(map[sqlbase.ID]map[sqlbase.ID]string),
			}
			err = resolver.initResolver(true)
			if err != nil {
				t.Fatal(err)
			}
			matched, err := resolver.findMatchedDescriptors(context.TODO(),
				test.sessionDatabase, searchPath, descriptors, targets)
			if test.err != "" {
				if !testutils.IsError(err, test.err) {
					t.Fatalf("expected error matching '%v', but got '%v'", test.err, err)
				}
			} else if err != nil {
				t.Fatal(err)
			} else {
				var matchedNames []string
				for _, m := range matched.descs {
					matchedNames = append(matchedNames, m.GetName())
				}
				var matchedDBNames []string
				for _, m := range matched.requestedDBs {
					matchedDBNames = append(matchedDBNames, m.GetName())
				}
				sort.Strings(test.expected)
				sort.Strings(test.expectedDBs)
				sort.Strings(matchedNames)
				sort.Strings(matchedDBNames)
				if !reflect.DeepEqual(test.expected, matchedNames) {
					t.Fatalf("expected %q got %q", test.expected, matchedNames)
				}
				if !reflect.DeepEqual(test.expectedDBs, matchedDBNames) {
					t.Fatalf("expected %q got %q", test.expectedDBs, matchedDBNames)
				}
			}
		})
	}
}
