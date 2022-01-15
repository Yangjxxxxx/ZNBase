// Copyright 2018  The Cockroach Authors.
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

package testcat

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/stats"
	"github.com/znbasedb/znbase/pkg/util/treeprinter"
)

const (
	// testDB is the default current database for testing purposes.
	testDB = "t"
)

// Catalog implements the cat.Catalog interface for testing purposes.
type Catalog struct {
	testSchema  Schema
	dataSources map[string]cat.DataSource
	counter     int
}

var _ cat.Catalog = &Catalog{}

// New creates a new empty instance of the test catalog.
func New() *Catalog {
	return &Catalog{
		testSchema: Schema{
			SchemaID: 1,
			SchemaName: cat.SchemaName{
				CatalogName:     testDB,
				SchemaName:      tree.PublicSchemaName,
				ExplicitSchema:  true,
				ExplicitCatalog: true,
			},
		},
		dataSources: make(map[string]cat.DataSource),
	}
}

// ResolveTableDesc is for testing.
func (tc *Catalog) ResolveTableDesc(name *tree.TableName) (interface{}, error) {
	return nil, errors.New("Got the wrong ResolveTableDesc")
}

// GetDatabaseDescByName is part of the cat.Catalog interface.
func (tc *Catalog) GetDatabaseDescByName(
	ctx context.Context, txn *client.Txn, dbName string,
) (interface{}, error) {
	p := &sqlbase.PrivilegeDescriptor{
		Users: []sqlbase.UserPrivileges{
			{
				User:       "",
				Privileges: sqlbase.NewPrivileges("", privilege.DatabasePrivileges, true),
			},
		},
	}
	return &sqlbase.DatabaseDescriptor{
		Name:       dbName,
		Privileges: p,
		Schemas: []sqlbase.SchemaDescriptor{
			{
				Name:       "public",
				Privileges: p,
			},
		},
	}, nil
}

// GetSchemaDescByName is part of the cat.Catalog interface.
func (tc *Catalog) GetSchemaDescByName(
	ctx context.Context, txn *client.Txn, scName string,
) (interface{}, error) {
	return nil, nil
}

// GetCurrentDatabase is part of the cat.Catalog interface.
func (tc *Catalog) GetCurrentDatabase(ctx context.Context) string {
	return string(tc.testSchema.SchemaName.CatalogName)
}

// GetCurrentSchema is part of the cat.Catalog interface.
func (tc *Catalog) GetCurrentSchema(ctx context.Context, name tree.Name) (bool, string, error) {
	return true, string(tc.testSchema.SchemaName.SchemaName), nil
}

// ResolveUdrFunction is for testing.
func (tc *Catalog) ResolveUdrFunction(
	ctx context.Context, name *cat.DataSourceName,
) (cat.UdrFunction, error) {
	return nil, errors.New("Got the wrong ResolveTableDesc")
}

// ResolveUDRFunctionDesc is for testing.
func (tc *Catalog) ResolveUDRFunctionDesc(
	name *tree.TableName,
) (interface{}, cat.UdrFunction, error) {
	return nil, nil, errors.New("Got the wrong ResolveTableDesc")
}

// ResolveSchema is part of the cat.Catalog interface.
func (tc *Catalog) ResolveSchema(
	_ context.Context, _ cat.Flags, name *cat.SchemaName,
) (cat.Schema, cat.SchemaName, error) {
	// This is a simplified version of tree.TableName.ResolveTarget() from
	// sql/tree/name_resolution.go.
	toResolve := *name
	if name.ExplicitSchema {
		if name.ExplicitCatalog {
			// Already 2 parts: nothing to do.
			return tc.resolveSchema(&toResolve)
		}

		// Only one part specified; assume it's a schema name and determine
		// whether the current database has that schema.
		toResolve.CatalogName = testDB
		if sch, resName, err := tc.resolveSchema(&toResolve); err == nil {
			return sch, resName, nil
		}

		// No luck so far. Compatibility with ZNBaseDB v1.1: use D.public
		// instead.
		toResolve.CatalogName = name.SchemaName
		toResolve.SchemaName = tree.PublicSchemaName
		toResolve.ExplicitCatalog = true
		return tc.resolveSchema(&toResolve)
	}

	// Neither schema or catalog was specified, so use t.public.
	toResolve.CatalogName = tree.Name(testDB)
	toResolve.SchemaName = tree.PublicSchemaName
	return tc.resolveSchema(&toResolve)
}

// ResolveAllDataSource is part of the cat.Catalog interface.
func (tc *Catalog) ResolveAllDataSource(
	_ context.Context, _ cat.Flags, name *cat.SchemaName,
) ([]cat.DataSourceName, error) {
	return nil, nil
}

// ResolveDataSource is part of the cat.Catalog interface.
func (tc *Catalog) ResolveDataSource(
	_ context.Context, _ cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	// This is a simplified version of tree.TableName.ResolveExisting() from
	// sql/tree/name_resolution.go.
	var ds cat.DataSource
	var err error
	toResolve := *name
	if name.ExplicitSchema && name.ExplicitCatalog {
		// Already 3 parts.
		ds, err = tc.resolveDataSource(&toResolve)
		if err == nil {
			return ds, toResolve, nil
		}
	} else if name.ExplicitSchema {
		// Two parts: Try to use the current database, and be satisfied if it's
		// sufficient to find the object.
		toResolve.CatalogName = testDB
		if tab, err := tc.resolveDataSource(&toResolve); err == nil {
			return tab, toResolve, nil
		}

		// No luck so far. Compatibility with ZNBaseDB v1.1: try D.public.T
		// instead.
		toResolve.CatalogName = name.SchemaName
		toResolve.SchemaName = tree.PublicSchemaName
		toResolve.ExplicitCatalog = true
		ds, err = tc.resolveDataSource(&toResolve)
		if err == nil {
			return ds, toResolve, nil
		}
	} else {
		// This is a naked data source name. Use the current database.
		toResolve.CatalogName = tree.Name(testDB)
		toResolve.SchemaName = tree.PublicSchemaName
		ds, err = tc.resolveDataSource(&toResolve)
		if err == nil {
			return ds, toResolve, nil
		}
	}

	// If we didn't find the table in the catalog, try to lazily resolve it as
	// a virtual table.
	if table, ok := resolveVTable(name); ok {
		// We rely on the check in CreateTable against this table's schema to infer
		// that this is a virtual table.
		return tc.CreateTable(table), *name, nil
	}

	// If this didn't end up being a virtual table, then return the original
	// error returned by resolveDataSource.
	return nil, cat.DataSourceName{}, err
}

// ResolveDataSourceByID is part of the cat.Catalog interface.
func (tc *Catalog) ResolveDataSourceByID(
	ctx context.Context, flags cat.Flags, id cat.StableID,
) (cat.DataSource, error) {
	for _, ds := range tc.dataSources {
		if tab, ok := ds.(*Table); ok && tab.TabID == id {
			return ds, nil
		}
	}
	return nil, pgerror.NewErrorf(pgcode.UndefinedTable,
		"relation [%d] does not exist", id)
}

// CheckPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
	switch t := o.(type) {
	case *Schema:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.SchemaName)
		}
	case *Table:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.TabName)
		}
	case *View:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.ViewName)
		}
	case *Sequence:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.SeqName)
		}
	default:
		panic("invalid Object")
	}
	return nil
}

// CheckDMLPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckDMLPrivilege(
	ctx context.Context, o cat.Object, cols tree.NameList, priv privilege.Kind, user string,
) error {
	switch t := o.(type) {
	case *Table:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.TabName)
		}
	case *View:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.ViewName)
		}
	case *Sequence:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.SeqName)
		}
	default:
		panic("invalid Object")
	}
	return nil
}

// CheckAnyColumnPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckAnyColumnPrivilege(ctx context.Context, o cat.Object) error {
	desc := o.(sqlbase.DescriptorProto)
	return fmt.Errorf("user has no privileges on %s", desc.GetName())
}

func (tc *Catalog) resolveSchema(toResolve *cat.SchemaName) (cat.Schema, cat.SchemaName, error) {
	if string(toResolve.CatalogName) != testDB {
		return nil, cat.SchemaName{}, pgerror.NewErrorf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(&toResolve.CatalogName)).
			SetHintf("verify that the current database and search_path are valid and/or the target database exists")
	}

	if string(toResolve.SchemaName) != tree.PublicSchema {
		return nil, cat.SchemaName{}, pgerror.NewErrorf(pgcode.InvalidName,
			"schema cannot be modified: %q", tree.ErrString(toResolve))
	}

	return &tc.testSchema, *toResolve, nil
}

// resolveDataSource checks if `toResolve` exists among the data sources in this
// Catalog. If it does, returns the corresponding data source. Otherwise, it
// returns an error.
func (tc *Catalog) resolveDataSource(toResolve *cat.DataSourceName) (cat.DataSource, error) {
	if table, ok := tc.dataSources[toResolve.FQString()]; ok {
		return table, nil
	}
	return nil, fmt.Errorf("no data source matches prefix: %q", tree.ErrString(toResolve))
}

// Schema returns the singleton test schema.
func (tc *Catalog) Schema() *Schema {
	return &tc.testSchema
}

// Table returns the test table that was previously added with the given name.
func (tc *Catalog) Table(name *tree.TableName) *Table {
	ds, _, err := tc.ResolveDataSource(context.TODO(), cat.Flags{}, name)
	if err != nil {
		panic(err)
	}
	if tab, ok := ds.(*Table); ok {
		return tab
	}
	panic(fmt.Errorf("\"%q\" is not a table", tree.ErrString(name)))
}

// AddTable adds the given test table to the catalog.
func (tc *Catalog) AddTable(tab *Table) {
	fq := tab.TabName.FQString()
	if _, ok := tc.dataSources[fq]; ok {
		panic(fmt.Errorf("table %q already exists", tree.ErrString(&tab.TabName)))
	}
	tc.dataSources[fq] = tab
}

// View returns the test view that was previously added with the given name.
func (tc *Catalog) View(name *cat.DataSourceName) *View {
	ds, _, err := tc.ResolveDataSource(context.TODO(), cat.Flags{}, name)
	if err != nil {
		panic(err)
	}
	if vw, ok := ds.(*View); ok {
		return vw
	}
	panic(fmt.Errorf("\"%q\" is not a view", tree.ErrString(name)))
}

// AddView adds the given test view to the catalog.
func (tc *Catalog) AddView(view *View) {
	fq := view.ViewName.FQString()
	if _, ok := tc.dataSources[fq]; ok {
		panic(fmt.Errorf("view %q already exists", tree.ErrString(&view.ViewName)))
	}
	tc.dataSources[fq] = view
}

// AddSequence adds the given test sequence to the catalog.
func (tc *Catalog) AddSequence(seq *Sequence) {
	fq := seq.SeqName.FQString()
	if _, ok := tc.dataSources[fq]; ok {
		panic(fmt.Errorf("sequence %q already exists", tree.ErrString(&seq.SeqName)))
	}
	tc.dataSources[fq] = seq
}

// ExecuteDDL parses the given DDL SQL statement and creates objects in the test
// catalog. This is used to test without spinning up a cluster.
func (tc *Catalog) ExecuteDDL(sql string) (string, error) {
	stmt, err := parser.ParseOne(sql, false)
	if err != nil {
		return "", err
	}

	switch stmt.AST.StatementType() {
	case tree.DDL, tree.RowsAffected:
	default:
		return "", fmt.Errorf("statement type is not DDL or RowsAffected: %v", stmt.AST.StatementType())
	}

	switch stmt := stmt.AST.(type) {
	case *tree.CreateTable:
		tab := tc.CreateTable(stmt)
		return tab.String(), nil

	case *tree.CreateView:
		view := tc.CreateView(stmt)
		return view.String(), nil

	case *tree.AlterTable:
		tc.AlterTable(stmt)
		return "", nil

	case *tree.DropTable:
		tc.DropTable(stmt)
		return "", nil

	case *tree.CreateSequence:
		seq := tc.CreateSequence(stmt)
		return seq.String(), nil

	case *tree.SetZoneConfig:
		zone := tc.SetZoneConfig(stmt)
		tp := treeprinter.New()
		cat.FormatZone(zone, tp)
		return tp.String(), nil

	default:
		return "", fmt.Errorf("unsupported statement: %v", stmt)
	}
}

// nextStableID returns a new unique StableID for a data source.
func (tc *Catalog) nextStableID() cat.StableID {
	tc.counter++

	// 53 is a magic number derived from how ZNBase internally stores tables. The
	// first user table is 53. Use this to have the test catalog look more
	// consistent with the real catalog.
	return cat.StableID(53 + tc.counter - 1)
}

// qualifyTableName updates the given table name to include catalog and schema
// if not already included.
func (tc *Catalog) qualifyTableName(name *tree.TableName) {
	hadExplicitSchema := name.ExplicitSchema
	hadExplicitCatalog := name.ExplicitCatalog
	name.ExplicitSchema = true
	name.ExplicitCatalog = true

	if hadExplicitSchema {
		if hadExplicitCatalog {
			// Already 3 parts: nothing to do.
			return
		}

		if name.SchemaName == tree.PublicSchemaName {
			// Use the current database.
			name.CatalogName = testDB
			return
		}

		// Compatibility with ZNBaseDB v1.1: use D.public.T.
		name.CatalogName = name.SchemaName
		name.SchemaName = tree.PublicSchemaName
		return
	}

	// Use the current database.
	name.CatalogName = testDB
	name.SchemaName = tree.PublicSchemaName
}

// Schema implements the cat.Schema interface for testing purposes.
type Schema struct {
	SchemaID   cat.StableID
	SchemaName cat.SchemaName

	// If Revoked is true, then the user has had privileges on the schema revoked.
	Revoked bool
}

var _ cat.Schema = &Schema{}

// ID is part of the cat.Object interface.
func (s *Schema) ID() cat.StableID {
	return s.SchemaID
}

// Equals is part of the cat.Object interface.
func (s *Schema) Equals(other cat.Object) bool {
	otherSchema, ok := other.(*Schema)
	return ok && s.SchemaID == otherSchema.SchemaID
}

// Name is part of the cat.Schema interface.
func (s *Schema) Name() *cat.SchemaName {
	return &s.SchemaName
}

// ReplaceDesc is part of the cat.Schema interface.
func (s *Schema) ReplaceDesc(scName string) cat.Schema {
	return &Schema{}
}

// View implements the cat.View interface for testing purposes.
type View struct {
	ViewID      cat.StableID
	ViewVersion int
	ViewName    cat.DataSourceName
	QueryText   string
	ColumnNames tree.NameList

	// If Revoked is true, then the user has had privileges on the view revoked.
	Revoked bool
}

var _ cat.View = &View{}

func (tv *View) String() string {
	tp := treeprinter.New()
	cat.FormatView(tv, tp)
	return tp.String()
}

// ID is part of the cat.DataSource interface.
func (tv *View) ID() cat.StableID {
	return tv.ViewID
}

// Equals is part of the cat.Object interface.
func (tv *View) Equals(other cat.Object) bool {
	otherView, ok := other.(*View)
	if !ok {
		return false
	}
	return tv.ViewID == otherView.ViewID && tv.ViewVersion == otherView.ViewVersion
}

// Name is part of the cat.DataSource interface.
func (tv *View) Name() *cat.DataSourceName {
	return &tv.ViewName
}

// GetOwner is part of the cat.DataSource interface.
func (tv *View) GetOwner() string {
	return ""
}

// Query is part of the cat.View interface.
func (tv *View) Query() string {
	return tv.QueryText
}

// ColumnNameCount is part of the cat.View interface.
func (tv *View) ColumnNameCount() int {
	return len(tv.ColumnNames)
}

// ColumnName is part of the cat.View interface.
func (tv *View) ColumnName(i int) tree.Name {
	return tv.ColumnNames[i]
}

// IsSystemView is part of the cat.View interface.
func (tv *View) IsSystemView() bool {
	return false
}

// GetBaseTableID is to get tableID from origin table
func (tv *View) GetBaseTableID() (int, error) {
	return -1, fmt.Errorf("it is not an updatable view")
}

// GetViewDependsColName is to get column names from origin table
func (tv *View) GetViewDependsColName() (map[string]string, bool, error) {
	return nil, false, fmt.Errorf("Error:  %s is not an updatable view", tv.ViewName.String())
}

// SysTemTable defines tables which has no data store engine info
var SysTemTable = []string{"lease", "jobs", "settings", "eventlog", "rangelog", "snapshots", "table_statistics", "zones"}

// Table implements the cat.Table interface for testing purposes.
type Table struct {
	TabID      cat.StableID
	TabVersion int
	TabName    tree.TableName
	Columns    []*Column
	Indexes    []*Index
	Stats      TableStats
	Checks     []cat.CheckConstraint
	Families   []*Family
	IsVirtual  bool
	Catalog    cat.Catalog

	// If Revoked is true, then the user has had privileges on the table revoked.
	Revoked bool

	writeOnlyColCount  int
	deleteOnlyColCount int
	writeOnlyIdxCount  int
	deleteOnlyIdxCount int

	// interleaved is true if the table's rows are interleaved with rows from
	// other table(s).
	interleaved bool

	// referenced is set to true when another table has referenced this table
	// via a foreign key.
	referenced bool
}

var _ cat.Table = &Table{}

func (tt *Table) String() string {
	tp := treeprinter.New()
	cat.FormatTable(tt.Catalog, tt, tp)
	return tp.String()
}

// ID is part of the cat.DataSource interface.
func (tt *Table) ID() cat.StableID {
	return tt.TabID
}

// IsHashPartition judges whether the table is hashpartition table or not.
func (tt *Table) IsHashPartition() bool {
	return false
}

// IsMaterializedView is part of the cat.Table interface.
func (tt *Table) IsMaterializedView() bool {
	return false
}

// Equals is part of the cat.Object interface.
func (tt *Table) Equals(other cat.Object) bool {
	otherTable, ok := other.(*Table)
	if !ok {
		return false
	}
	return tt.TabID == otherTable.TabID && tt.TabVersion == otherTable.TabVersion
}

// Name is part of the cat.DataSource interface.
func (tt *Table) Name() *cat.DataSourceName {
	return &tt.TabName
}

// GetOwner is part of the cat.DataSource interface.
func (tt *Table) GetOwner() string {
	return ""
}

// IsVirtualTable is part of the cat.Table interface.
func (tt *Table) IsVirtualTable() bool {
	return tt.IsVirtual
}

// IsInterleaved is part of the cat.Table interface.
func (tt *Table) IsInterleaved() bool {
	return false
}

// IsReferenced is part of the cat.Table interface.
func (tt *Table) IsReferenced() bool {
	return tt.referenced
}

// ColumnCount is part of the cat.Table interface.
func (tt *Table) ColumnCount() int {
	return len(tt.Columns) - tt.writeOnlyColCount - tt.deleteOnlyColCount
}

// WritableColumnCount is part of the cat.Table interface.
func (tt *Table) WritableColumnCount() int {
	return len(tt.Columns) - tt.deleteOnlyColCount
}

// DeletableColumnCount is part of the cat.Table interface.
func (tt *Table) DeletableColumnCount() int {
	return len(tt.Columns)
}

// Column is part of the cat.Table interface.
func (tt *Table) Column(i int) cat.Column {
	return tt.Columns[i]
}

// ColumnByIDAndName return a Column by colID.
func (tt *Table) ColumnByIDAndName(colID int, colName string) cat.Column {
	for _, oCol := range tt.Columns {
		if oCol.Ordinal == colID {
			return oCol
		}
		if oCol.Name == colName {
			return oCol
		}
	}
	return nil
}

// IndexCount is part of the cat.Table interface.
func (tt *Table) IndexCount() int {
	return len(tt.Indexes) - tt.writeOnlyIdxCount - tt.deleteOnlyIdxCount
}

// WritableIndexCount is part of the cat.Table interface.
func (tt *Table) WritableIndexCount() int {
	return len(tt.Indexes) - tt.deleteOnlyIdxCount
}

// DeletableIndexCount is part of the cat.Table interface.
func (tt *Table) DeletableIndexCount() int {
	return len(tt.Indexes)
}

// Index is part of the cat.Table interface.
func (tt *Table) Index(i int) cat.Index {
	return tt.Indexes[i]
}

// Index1 is part of the cat.Table interface.
func (tt *Table) Index1(i int) cat.Index {
	return tt.Indexes[i]
}

// StatisticCount is part of the cat.Table interface.
func (tt *Table) StatisticCount() int {
	return len(tt.Stats)
}

// Statistic is part of the cat.Table interface.
func (tt *Table) Statistic(i int) cat.TableStatistic {
	return tt.Stats[i]
}

// CheckCount is part of the cat.Table interface.
func (tt *Table) CheckCount() int {
	return len(tt.Checks)
}

// Check is part of the cat.Table interface.
func (tt *Table) Check(i int) cat.CheckConstraint {
	return tt.Checks[i]
}

// FamilyCount is part of the cat.Table interface.
func (tt *Table) FamilyCount() int {
	return len(tt.Families)
}

// Family is part of the cat.Table interface.
func (tt *Table) Family(i int) cat.Family {
	return tt.Families[i]
}

// FindOrdinal returns the ordinal of the column with the given name.
func (tt *Table) FindOrdinal(name string) int {
	for i, col := range tt.Columns {
		if col.Name == name {
			return i
		}
	}
	panic(fmt.Sprintf(
		"cannot find column %q in table %q",
		tree.ErrString((*tree.Name)(&name)),
		tree.ErrString(&tt.TabName),
	))
}

func (tt *Table) isSystemTable(name string) bool {
	for _, item := range SysTemTable {
		if item == name {
			return true
		}
	}
	return false
}

// DataStoreEngineInfo implements cat.DataStoreEngineInfo.
func (tt *Table) DataStoreEngineInfo() cat.DataStoreEngine {
	var engineTypeStrSlice []string
	isSysTable := tt.isSystemTable(string(tt.Name().TableName))
	if !isSysTable {
		// fake set by assigned const value;
		engineTypeStrSlice = append(engineTypeStrSlice, "KVStore", "ColumnStore")
	}
	var eType, tmp cat.EngineTypeSet
	for _, item := range engineTypeStrSlice {
		if item == "KVStore" {
			tmp = 1
		}
		if item == "ColumnStore" {
			tmp = 2
		}
		eType = eType | tmp
	}
	return cat.DataStoreEngine{ETypeSet: eType}
}

// GetInhertisBy returns tables inherit by this one if exist
func (tt *Table) GetInhertisBy() []uint32 {
	return nil
}

// Index implements the cat.Index interface for testing purposes.
type Index struct {
	IdxName string

	// Ordinal is the ordinal of this index in the table.
	Ordinal int

	// KeyCount is the number of columns that make up the unique key for the
	// index. See the cat.Index.KeyColumnCount for more details.
	KeyCount int

	// LaxKeyCount is the number of columns that make up a lax key for the
	// index, which allows duplicate rows when at least one of the values is
	// NULL. See the cat.Index.LaxKeyColumnCount for more details.
	LaxKeyCount int

	// Unique is true if this index is declared as UNIQUE in the schema.
	Unique bool

	// Inverted is true when this index is an inverted index.
	Inverted bool

	Columns []cat.IndexColumn

	// IdxZone is the zone associated with the index. This may be inherited from
	// the parent table, database, or even the default zone.
	IdxZone *config.ZoneConfig

	// table is a back reference to the table this index is on.
	table *Table

	// foreignKey is a struct representing an outgoing foreign key
	// reference. If fkSet is true, then foreignKey is a valid
	// index reference.
	foreignKey cat.ForeignKeyReference
	fkSet      bool

	predicate string
}

// ID is part of the cat.Index interface.
func (ti *Index) ID() cat.StableID {
	return 1 + cat.StableID(ti.Ordinal)
}

// Name is part of the cat.Index interface.
func (ti *Index) Name() tree.Name {
	return tree.Name(ti.IdxName)
}

// Table is part of the cat.Index interface.
func (ti *Index) Table() cat.Table {
	return ti.table
}

// GetOrdinal is part of the cat.Index interface.
func (ti *Index) GetOrdinal() int {
	return ti.Ordinal
}

// IsUnique is part of the cat.Index interface.
func (ti *Index) IsUnique() bool {
	return ti.Unique
}

// IsFunc is part of the cat.Index interface.
func (ti *Index) IsFunc() bool {
	return false
}

// ColumnIsFunc is part of the cat.Index interface.
func (ti *Index) ColumnIsFunc(i int) bool {
	return false
}

// GetColumnNames is part of the cat.Index interface.
func (ti *Index) GetColumnNames() []string {
	return nil
}

// GetColumnID is part of the cat.Index interface.
func (ti *Index) GetColumnID() map[int]string {
	return nil
}

// IsInverted is part of the cat.Index interface.
func (ti *Index) IsInverted() bool {
	return ti.Inverted
}

// ColumnCount is part of the cat.Index interface.
func (ti *Index) ColumnCount() int {
	return len(ti.Columns)
}

// KeyColumnCount is part of the cat.Index interface.
func (ti *Index) KeyColumnCount() int {
	return ti.KeyCount
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (ti *Index) LaxKeyColumnCount() int {
	return ti.LaxKeyCount
}

// Column is part of the cat.Index interface.
func (ti *Index) Column(i int) cat.IndexColumn {
	return ti.Columns[i]
}

// ColumnForFunc is part of the cat.Index interface.
func (ti *Index) ColumnForFunc(i int) cat.IndexColumn {
	return ti.Columns[i]
}

// ColumnGetID is part of the cat.Index interface.
func (ti *Index) ColumnGetID(i int, ord int) int {
	return ord
}

// ColumnsForFunc is part of the cat.Index interface.
func (ti *Index) ColumnsForFunc(i int) []cat.IndexColumn {
	IndexColumns := make([]cat.IndexColumn, 0)
	IndexColumns = append(IndexColumns, ti.Columns[i])
	return IndexColumns
}

// ForeignKey is part of the cat.Index interface.
func (ti *Index) ForeignKey() (cat.ForeignKeyReference, bool) {
	return ti.foreignKey, ti.fkSet
}

// Zone is part of the cat.Index interface.
func (ti *Index) Zone() cat.Zone {
	return ti.IdxZone
}

// Predicate is part of the cat.Index interface. It returns the predicate
// expression and true if the index is a partial index. If the index is not
// partial, the empty string and false is returned.
func (ti *Index) Predicate() (string, bool) {
	return ti.predicate, ti.predicate != ""
}

// Column implements the cat.Column interface for testing purposes.
type Column struct {
	Ordinal      int
	Hidden       bool
	Nullable     bool
	Name         string
	Type         types.T
	ColType      sqlbase.ColumnType
	DefaultExpr  *string
	ComputedExpr *string
}

// IsOnUpdateCurrentTimeStamp returns whether this column's currentTimeStamp is open
func (tc *Column) IsOnUpdateCurrentTimeStamp() bool {
	return false
}

var _ cat.Column = &Column{}

// ColID is part of the cat.Index interface.
func (tc *Column) ColID() cat.StableID {
	return 1 + cat.StableID(tc.Ordinal)
}

// IsNullable is part of the cat.Column interface.
func (tc *Column) IsNullable() bool {
	return tc.Nullable
}

// ColName is part of the cat.Column interface.
func (tc *Column) ColName() tree.Name {
	return tree.Name(tc.Name)
}

// DatumType is part of the cat.Column interface.
func (tc *Column) DatumType() types.T {
	return tc.Type
}

// ColTypePrecision is part of the cat.Column interface.
func (tc *Column) ColTypePrecision() int {
	return int(tc.ColType.Precision)
}

// ColTypeWidth is part of the cat.Column interface.
func (tc *Column) ColTypeWidth() int {
	return int(tc.ColType.Width)
}

// ColTypeStr is part of the cat.Column interface.
func (tc *Column) ColTypeStr() string {
	return tc.ColType.SQLString()
}

// IsHidden is part of the cat.Column interface.
func (tc *Column) IsHidden() bool {
	return tc.Hidden
}

// IsRowIDColumn is a function to check whether column is rowid column
func (tc *Column) IsRowIDColumn() bool {
	if tc.IsHidden() && tc.HasDefault() && tc.DefaultExprStr() == "unique_rowid()" {
		return true
	}
	return false
}

// HasDefault is part of the cat.Column interface.
func (tc *Column) HasDefault() bool {
	return tc.DefaultExpr != nil
}

// IsComputed is part of the cat.Column interface.
func (tc *Column) IsComputed() bool {
	return tc.ComputedExpr != nil
}

// DefaultExprStr is part of the cat.Column interface.
func (tc *Column) DefaultExprStr() string {
	return *tc.DefaultExpr
}

// ComputedExprStr is part of the cat.Column interface.
func (tc *Column) ComputedExprStr() string {
	return *tc.ComputedExpr
}

// VisibleType is part of the cat.Column interface.
func (tc *Column) VisibleType() string {
	return tc.ColType.VisibleTypeName
}

// TableStat implements the cat.TableStatistic interface for testing purposes.
type TableStat struct {
	js stats.JSONStatistic
	tt *Table
}

var _ cat.TableStatistic = &TableStat{}

// CreatedAt is part of the cat.TableStatistic interface.
func (ts *TableStat) CreatedAt() time.Time {
	d, err := tree.ParseDTimestamp(nil, ts.js.CreatedAt, time.Microsecond)
	if err != nil {
		panic(err)
	}
	return d.Time
}

// ColumnCount is part of the cat.TableStatistic interface.
func (ts *TableStat) ColumnCount() int {
	return len(ts.js.Columns)
}

// ColumnOrdinal is part of the cat.TableStatistic interface.
func (ts *TableStat) ColumnOrdinal(i int) int {
	return ts.tt.FindOrdinal(ts.js.Columns[i])
}

// RowCount is part of the cat.TableStatistic interface.
func (ts *TableStat) RowCount() uint64 {
	return ts.js.RowCount
}

// DistinctCount is part of the cat.TableStatistic interface.
func (ts *TableStat) DistinctCount() uint64 {
	return ts.js.DistinctCount
}

// NullCount is part of the cat.TableStatistic interface.
func (ts *TableStat) NullCount() uint64 {
	return ts.js.NullCount
}

//Histogram  interface method of table statistics
func (ts *TableStat) Histogram() []cat.HistogramBucket {
	return nil
}

// TableStats is a slice of TableStat pointers.
type TableStats []*TableStat

// Len is part of the Sorter interface.
func (ts TableStats) Len() int { return len(ts) }

// Less is part of the Sorter interface.
func (ts TableStats) Less(i, j int) bool {
	// Sort with most recent first.
	return ts[i].CreatedAt().Unix() > ts[j].CreatedAt().Unix()
}

// Swap is part of the Sorter interface.
func (ts TableStats) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

// Sequence implements the cat.Sequence interface for testing purposes.
type Sequence struct {
	SeqID      cat.StableID
	SeqVersion int
	SeqName    tree.TableName
	Catalog    cat.Catalog

	// If Revoked is true, then the user has had privileges on the sequence revoked.
	Revoked bool
}

var _ cat.Sequence = &Sequence{}

// ID is part of the cat.DataSource interface.
func (ts *Sequence) ID() cat.StableID {
	return ts.SeqID
}

// Equals is part of the cat.Object interface.
func (ts *Sequence) Equals(other cat.Object) bool {
	otherSequence, ok := other.(*Sequence)
	if !ok {
		return false
	}
	return ts.SeqID == otherSequence.SeqID && ts.SeqVersion == otherSequence.SeqVersion
}

// Name is part of the cat.DataSource interface.
func (ts *Sequence) Name() *tree.TableName {
	return &ts.SeqName
}

// GetOwner is part of the cat.DataSource interface.
func (ts *Sequence) GetOwner() string {
	return ""
}

// SequenceName is part of the cat.Sequence interface.
func (ts *Sequence) SequenceName() *tree.TableName {
	return ts.Name()
}

func (ts *Sequence) String() string {
	tp := treeprinter.New()
	cat.FormatSequence(ts.Catalog, ts, tp)
	return tp.String()
}

// Family implements the cat.Family interface for testing purposes.
type Family struct {
	FamName string

	// Ordinal is the ordinal of this family in the table.
	Ordinal int

	Columns []cat.FamilyColumn

	// table is a back reference to the table this index is on.
	table *Table
}

// ID is part of the cat.Family interface.
func (tf *Family) ID() cat.StableID {
	return 1 + cat.StableID(tf.Ordinal)
}

// Name is part of the cat.Family interface.
func (tf *Family) Name() tree.Name {
	return tree.Name(tf.FamName)
}

// Table is part of the cat.Family interface.
func (tf *Family) Table() cat.Table {
	return tf.table
}

// ColumnCount is part of the cat.Family interface.
func (tf *Family) ColumnCount() int {
	return len(tf.Columns)
}

// Column is part of the cat.Family interface.
func (tf *Family) Column(i int) cat.FamilyColumn {
	return tf.Columns[i]
}

// CheckAnyPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckAnyPrivilege(ctx context.Context, o cat.Object) error {
	switch t := o.(type) {
	case *Schema:
		if t.Revoked {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to access %v", t.SchemaName)
		}
	case *Table:
		if t.Revoked {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to access %v", t.TabName)
		}
	case *View:
		if t.Revoked {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to access %v", t.ViewName)
		}
	case *Sequence:
		if t.Revoked {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "user does not have privilege to access %v", t.SeqName)
		}
	default:
		panic("invalid Object")
	}
	return nil
}

// HasRoleOption is part of the cat.Catalog interface.
func (tc *Catalog) HasRoleOption(ctx context.Context, action string) error {
	return nil
}

// CheckFunctionPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckFunctionPrivilege(ctx context.Context, ds cat.DataSource) []error {
	return nil
}
