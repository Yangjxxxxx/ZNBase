// Copyright 2016  The Cockroach Authors.
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

package sessiondata

import (
	"bytes"
	"encoding/base64"
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
)

// PgDatabaseName is the name of the default postgres system database.
const PgDatabaseName = "postgres"

// DefaultDatabaseName is the name of the default ZNBaseDB database used
// for connections without a current db set.
const DefaultDatabaseName = "defaultdb"

// PgCatalogName is the name of the pg_catalog system schema.
const PgCatalogName = "pg_catalog"

// PgTempSchemaName is the alias for temporary schemas across sessions.
const PgTempSchemaName = "pg_temp"

// PgSchemaPrefix is a prefix for Postgres system schemas. Users cannot
// create schemas with this prefix.
const PgSchemaPrefix = "pg_"

// SearchPath represents a list of namespaces to search builtins in.
// The names must be normalized (as per Name.Normalize) already.
type SearchPath struct {
	paths                []string
	containsPgCatalog    bool
	tempSchemaName       string
	containsPgTempSchema bool
}

// IsSchemaNameValid returns whether the input name is valid for a user defined
// schema.
func IsSchemaNameValid(name string) error {
	// Schemas starting with "pg_" are not allowed.
	if strings.HasPrefix(name, PgSchemaPrefix) {
		err := pgerror.Newf(pgcode.ReservedName, "unacceptable schema name %q\nThe prefix \"pg_\" is reserved for system schemas", name)
		return err
	}
	return nil
}

// SetSearchPath replace paths on SearchPath. Keep others no change
func SetSearchPath(sp SearchPath, paths []string) SearchPath {
	newSp := MakeSearchPath(paths)
	newSp.tempSchemaName = sp.tempSchemaName
	newSp.containsPgTempSchema = sp.containsPgTempSchema
	return newSp
}

// GetTemporarySchemaName returns the temporary schema specific to the current
// session.
func (s SearchPath) GetTemporarySchemaName() string {
	return s.tempSchemaName
}

// MakeSearchPath returns a new immutable SearchPath struct. The paths slice
// must not be modified after hand-off to MakeSearchPath.
func MakeSearchPath(paths []string) SearchPath {
	containsPgCatalog := false
	containsPgTempSchema := false
	for _, e := range paths {
		if e == PgCatalogName {
			containsPgCatalog = true
		} else if e == PgTempSchemaName {
			containsPgTempSchema = true
		}
	}
	return SearchPath{
		paths:                paths,
		containsPgCatalog:    containsPgCatalog,
		containsPgTempSchema: containsPgTempSchema,
	}
}

// Iter returns an iterator through the search path. We must include the
// implicit pg_catalog at the beginning of the search path, unless it has been
// explicitly set later by the user.
// "The system catalog schema, pg_catalog, is always searched, whether it is
// mentioned in the path or not. If it is mentioned in the path then it will be
// searched in the specified order. If pg_catalog is not in the path then it
// will be searched before searching any of the path items."
// - https://www.postgresql.org/docs/9.1/static/runtime-config-client.html
func (s SearchPath) Iter() SearchPathIter {
	implicitPgTempSchema := !s.containsPgTempSchema && s.tempSchemaName != ""
	if s.containsPgCatalog {
		return SearchPathIter{
			paths:                s.paths,
			i:                    0,
			tempSchemaName:       s.tempSchemaName,
			implicitPgTempSchema: implicitPgTempSchema,
		}
	}
	return SearchPathIter{
		paths:                s.paths,
		i:                    -1,
		tempSchemaName:       s.tempSchemaName,
		implicitPgTempSchema: implicitPgTempSchema,
	}
}

// IterWithoutImplicitPGCatalog is the same as Iter, but does not include the
// implicit pg_catalog.
func (s SearchPath) IterWithoutImplicitPGCatalog() SearchPathIter {
	return SearchPathIter{
		paths:          s.paths,
		i:              0,
		tempSchemaName: s.tempSchemaName,
	}
}

// WithTemporarySchemaName returns a new immutable SearchPath struct with
// the tempSchemaName supplied and the same paths as before.
// This should be called every time a session creates a temporary schema
// for the first time.
func (s SearchPath) WithTemporarySchemaName(tempSchemaName string) SearchPath {
	return SearchPath{
		paths:                s.paths,
		containsPgCatalog:    s.containsPgCatalog,
		tempSchemaName:       tempSchemaName,
		containsPgTempSchema: s.containsPgTempSchema,
	}
}

//UpdatePaths returns a new immutable SearchPath struct with the paths supplied and
//the same tempSchemaName as before
func (s SearchPath) UpdatePaths(paths []string) SearchPath {
	return MakeSearchPath(paths).WithTemporarySchemaName(s.tempSchemaName)
}

// MaybeResolveTemporarySchema returns the session specific temporary schema
// for the pg_temp alias (only if a temporary schema exists). It acts as a pass
// through for all other schema names.
func (s SearchPath) MaybeResolveTemporarySchema(schemaName string) (string, error) {
	// If the schemaName is pg_temp and the tempSchemaName has been set, pg_temp
	// is an alias the session specific temp schema.
	if schemaName == PgTempSchemaName && s.tempSchemaName != "" {
		return s.tempSchemaName, nil
	}
	return schemaName, nil
}

// CheckTemporarySchema returns error if schemaName is other session's temporary schema
func (s SearchPath) CheckTemporarySchema(schemaName string) error {
	// Only allow access to the session specific temporary schema.
	if strings.HasPrefix(schemaName, PgTempSchemaName) && schemaName != PgTempSchemaName && schemaName != s.tempSchemaName {
		return pgerror.NewError(pgcode.FeatureNotSupported, "cannot access temporary tables of other sessions")
	}
	return nil
}

// GetPathArray returns the underlying path array of this SearchPath. The
// resultant slice is not to be modified.
func (s SearchPath) GetPathArray() []string {
	return s.paths
}

// Equals returns true if two SearchPaths are the same.
func (s SearchPath) Equals(other *SearchPath) bool {
	if s.containsPgCatalog != other.containsPgCatalog {
		return false
	}
	if len(s.paths) != len(other.paths) {
		return false
	}
	// Fast path: skip the check if it is the same slice.
	if &s.paths[0] != &other.paths[0] {
		for i := range s.paths {
			if s.paths[i] != other.paths[i] {
				return false
			}
		}
	}
	return true
}

// Base64String is to base64 encode string
func (s SearchPath) Base64String() string {
	comma := ""
	var buf bytes.Buffer
	for _, s := range s.paths {
		buf.WriteString(comma)
		buf.WriteString(base64.StdEncoding.EncodeToString([]byte(s)))
		comma = ","
	}
	return buf.String()
}

func (s SearchPath) String() string {
	return strings.Join(s.paths, ", ")
}

// SearchPathIter enables iteration over the search paths without triggering an
// allocation. Use one of the SearchPath.Iter methods to get an instance of the
// iterator, and then repeatedly call the Next method in order to iterate over
// each search path.
type SearchPathIter struct {
	paths                []string
	tempSchemaName       string
	implicitPgTempSchema bool
	i                    int
}

// Next returns the next search path, or false if there are no remaining paths.
func (iter *SearchPathIter) Next() (path string, ok bool) {
	if iter.implicitPgTempSchema && iter.tempSchemaName != "" {
		iter.implicitPgTempSchema = false
		return iter.tempSchemaName, true
	}
	if iter.i == -1 {
		iter.i++
		return PgCatalogName, true
	}
	if iter.i < len(iter.paths) {
		iter.i++
		if iter.paths[iter.i-1] == PgTempSchemaName {

		}
		return iter.paths[iter.i-1], true
	}
	return "", false
}
