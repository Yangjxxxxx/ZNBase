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

package tree

// TableName corresponds to the name of a table in a FROM clause,
// INSERT or UPDATE statement, etc.
//
// This is constructed for incoming SQL queries from an UnresolvedObjectName,
//
// Internal uses of this struct should not construct instances of
// TableName directly, and instead use the NewTableName /
// MakeTableName functions underneath.
//
// TableName is the public type for tblName. It exposes the fields
// and can be default-constructed but cannot be instantiated with a
// non-default value; this encourages the use of the constructors below.
type TableName struct {
	tblName
}

type tblName struct {
	// TableName is the unqualified name for the object
	// (table/view/sequence/function/type).
	TableName Name

	// IsOnly means whether this Object is modified by token ONLY
	IsOnly bool

	// TableNamePrefix is the path to the object.  This can be modified
	// further by name resolution, see name_resolution.go.
	TableNamePrefix
	// ResolvedAsFunction is set to true when name is an function
	ResolvedAsFunction bool
}

// TableNamePrefix corresponds to the path prefix of a table name.
type TableNamePrefix struct {
	CatalogName Name
	SchemaName  Name

	// ExplicitCatalog is true if the catalog was explicitly specified
	// or it needs to be rendered during pretty-printing.
	ExplicitCatalog bool
	// ExplicitSchema is true if the schema was explicitly specified
	// or it needs to be rendered during pretty-printing.
	ExplicitSchema bool
}

// Format implements the NodeFormatter interface.
func (tp *TableNamePrefix) Format(ctx *FmtCtx) {
	alwaysFormat := ctx.alwaysFormatTablePrefix()
	if tp.ExplicitSchema || alwaysFormat {
		if tp.ExplicitCatalog || alwaysFormat {
			ctx.FormatNode(&tp.CatalogName)
			ctx.WriteByte('.')
		}
		ctx.FormatNode(&tp.SchemaName)
	}
}

func (tp *TableNamePrefix) String() string { return AsString(tp) }

// Schema retrieves the unqualified schema name.
func (tp *TableNamePrefix) Schema() string {
	return string(tp.SchemaName)
}

// Catalog retrieves the unqualified catalog name.
func (tp *TableNamePrefix) Catalog() string {
	return string(tp.CatalogName)
}

// Equals returns true if the two table name prefixes are identical (including
// the ExplicitSchema/ExplicitCatalog flags).
//func (tp *TableNamePrefix) Equals(other *TableNamePrefix) bool {
//	return *tp == *other
//}

// Format implements the NodeFormatter interface.
func (t *TableName) Format(ctx *FmtCtx) {
	if ctx.tableNameFormatter != nil {
		ctx.tableNameFormatter(ctx, t)
		return
	}
	t.TableNamePrefix.Format(ctx)
	if t.ExplicitSchema || ctx.alwaysFormatTablePrefix() {
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&t.TableName)
}
func (t *TableName) String() string { return AsString(t) }

// FQString renders the table name in full, not omitting the prefix
// schema and catalog names. Suitable for logging, etc.
func (t *TableName) FQString() string {
	return AsStringWithFlags(t, FmtAlwaysQualifyTableNames)
}

// Table retrieves the unqualified table name.
func (t *TableName) Table() string {
	return string(t.TableName)
}

// Equals returns true if the two table names are identical (including
// the ExplicitSchema/ExplicitCatalog flags).
func (t *TableName) Equals(other *TableName) bool {
	return *t == *other
}

// EqualWithPublic returns true if the two table names are identical (including
// the ExplicitSchema/ExplicitCatalog flags).
func (t TableName) EqualWithPublic(other TableName) bool {
	if t.CatalogName == "" {
		t.ExplicitCatalog = true
		t.CatalogName = t.SchemaName
		t.SchemaName = "public"
	}
	if other.CatalogName == "" {
		other.ExplicitCatalog = true
		other.CatalogName = t.SchemaName
		other.SchemaName = "public"
	}
	return t.String() == other.String()
}

// tableExpr implements the TableExpr interface.
func (*TableName) tableExpr() {}

// DefinedAsFunction will set ResolvedAsFunction when object is a real function
func (t *TableName) DefinedAsFunction() {
	t.ResolvedAsFunction = true
}

// MakeTableName creates a new table name qualified with just a schema.
func MakeTableName(db, tbl Name) TableName {
	return TableName{tblName{
		TableName: tbl,
		TableNamePrefix: TableNamePrefix{
			CatalogName:     db,
			SchemaName:      PublicSchemaName,
			ExplicitSchema:  true,
			ExplicitCatalog: true,
		},
	}}
}

// NewTableName creates a new table name qualified with a given
// catalog and the public schema.
func NewTableName(db, tbl Name) *TableName {
	tn := MakeTableName(db, tbl)
	return &tn
}

// MakeTableNameWithSchema creates a new fully qualified table name.
func MakeTableNameWithSchema(db, schema, tbl Name) TableName {
	return TableName{tblName{
		TableName: tbl,
		TableNamePrefix: TableNamePrefix{
			CatalogName:     db,
			SchemaName:      schema,
			ExplicitSchema:  true,
			ExplicitCatalog: true,
		},
	}}
}

// MakeUnqualifiedTableName creates a new base table name.
func MakeUnqualifiedTableName(tbl Name) TableName {
	return TableName{tblName{
		TableName: tbl,
	}}
}

// MakeUnqualifiedFuncTableName return table name
func MakeUnqualifiedFuncTableName(part [4]string) TableName {
	ret := MakeUnqualifiedTableName(Name(part[0]))
	ret.SchemaName = Name(part[1])
	ret.CatalogName = Name(part[2])
	ret.ExplicitSchema = !(part[1] == "")
	ret.ExplicitCatalog = !(part[2] == "")
	return ret
}

// NewUnqualifiedTableName creates a new base table name.
func NewUnqualifiedTableName(tbl Name) *TableName {
	tn := MakeUnqualifiedTableName(tbl)
	return &tn
}

// UDR-TODO: 加注释
func makeTableNameFromUnresolvedName(n *UnresolvedName) TableName {
	return TableName{tblName{
		TableName:       Name(n.Parts[0]),
		TableNamePrefix: makeTableNamePrefixFromUnresolvedName(n),
	}}
}

// MakeTableNameFromUnresolvedName explore to package sql to resolve tableName from UnresolvedName
var MakeTableNameFromUnresolvedName = makeTableNameFromUnresolvedName

// UDR-TODO: 加注释
func makeTableNamePrefixFromUnresolvedName(n *UnresolvedName) TableNamePrefix {
	return TableNamePrefix{
		SchemaName:      Name(n.Parts[1]),
		CatalogName:     Name(n.Parts[2]),
		ExplicitSchema:  n.NumParts >= 2,
		ExplicitCatalog: n.NumParts >= 3,
	}
}

// TableNames represents a comma separated list (see the Format method)
// of table names.
type TableNames []TableName

// Format implements the NodeFormatter interface.
func (ts *TableNames) Format(ctx *FmtCtx) {
	sep := ""
	for i := range *ts {
		ctx.WriteString(sep)
		ctx.FormatNode(&(*ts)[i])
		sep = ", "
	}
}
func (ts *TableNames) String() string { return AsString(ts) }

// TableIndexName is the name of an index, used in statements that
// specifically refer to an index.
//
// The table name is optional. It is possible to specify the schema or catalog
// without specifying a table name; in this case, Table.TableNamePrefix has the
// fields set but Table.TableName is empty.
type TableIndexName struct {
	Table TableName
	Index UnrestrictedName
}

// Format implements the NodeFormatter interface.
func (n *TableIndexName) Format(ctx *FmtCtx) {
	if n.Index == "" {
		// This case is only for ZoneSpecifier.TableOrIndex; normally an empty Index
		// is not valid.
		ctx.FormatNode(&n.Table)
		return
	}

	if n.Table.TableName != "" {
		// The table is specified.
		ctx.FormatNode(&n.Table)
		ctx.WriteByte('@')
		ctx.FormatNode(&n.Index)
		return
	}

	// The table is not specified. The schema/catalog can still be specified.
	if n.Table.ExplicitSchema || ctx.alwaysFormatTablePrefix() {
		ctx.FormatNode(&n.Table.TableNamePrefix)
		ctx.WriteByte('.')
	}
	// In this case, we must format the index name as a restricted name (quotes
	// must be added for reserved keywords).
	ctx.FormatNode((*Name)(&n.Index))
}

func (n *TableIndexName) String() string { return AsString(n) }

// TableIndexNames is a list of indexes.
type TableIndexNames []*TableIndexName

// Format implements the NodeFormatter interface.
func (n *TableIndexNames) Format(ctx *FmtCtx) {
	sep := ""
	for _, tni := range *n {
		ctx.WriteString(sep)
		ctx.FormatNode(tni)
		sep = ", "
	}
}
