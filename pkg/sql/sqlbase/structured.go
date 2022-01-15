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

package sqlbase

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/interval"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// ID, ColumnID, FamilyID, and IndexID are all uint32, but are each given a
// type alias to prevent accidental use of one of the types where
// another is expected.

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID tree.ID

// InvalidID is the uninitialised descriptor id.
const InvalidID ID = 0

// IDs is a sortable list of IDs.
type IDs []ID

func (ids IDs) Len() int           { return len(ids) }
func (ids IDs) Less(i, j int) bool { return ids[i] < ids[j] }
func (ids IDs) Swap(i, j int)      { ids[i], ids[j] = ids[j], ids[i] }

// TableDescriptors is a sortable list of *TableDescriptors.
type TableDescriptors []*TableDescriptor

// TablesByID is a shorthand for the common map of tables keyed by ID.
type TablesByID map[ID]*TableDescriptor

func (t TableDescriptors) Len() int           { return len(t) }
func (t TableDescriptors) Less(i, j int) bool { return t[i].ID < t[j].ID }
func (t TableDescriptors) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID tree.ColumnID

// ColumnIDs is a slice of ColumnDescriptor IDs.
type ColumnIDs []ColumnID

func (c ColumnIDs) Len() int           { return len(c) }
func (c ColumnIDs) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c ColumnIDs) Less(i, j int) bool { return c[i] < c[j] }

// FamilyID is a custom type for ColumnFamilyDescriptor IDs.
type FamilyID uint32

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID tree.IndexID

// DescriptorVersion is a custom type for TableDescriptor Versions.
type DescriptorVersion uint32

// FormatVersion is a custom type for TableDescriptor versions of the sql to
// key:value mapping.
//go:generate stringer -type=FormatVersion
type FormatVersion uint32

const (
	_ FormatVersion = iota
	// BaseFormatVersion corresponds to the encoding described in
	// https://www.znbaselabs.com/blog/sql-in-znbasedb-mapping-table-data-to-key-value-storage/.
	BaseFormatVersion
	// FamilyFormatVersion corresponds to the encoding described in
	// https://github.com/znbasedb/znbase/blob/master/docs/RFCS/20151214_sql_column_families.md
	FamilyFormatVersion
	// InterleavedFormatVersion corresponds to the encoding described in
	// https://github.com/znbasedb/znbase/blob/master/docs/RFCS/20160624_sql_interleaved_tables.md
	InterleavedFormatVersion
)

// MutationID is a custom type for TableDescriptor mutations.
type MutationID uint32

// MutableTableDescriptor is a custom type for TableDescriptors
// going through schema mutations.
type MutableTableDescriptor struct {
	TableDescriptor

	// ClusterVersion represents the version of the table descriptor read from the store.
	ClusterVersion TableDescriptor
}

// MutableFunctionDescriptor is a custom type for FunctionDescriptors
// going through schema mutations.
type MutableFunctionDescriptor struct {
	FunctionDescriptor
	// ClusterVersion represents the version of the table descriptor read from the store.
	ClusterVersion FunctionDescriptor
}

// MutableFunctionDescriptorGroup is custom struct about function group with same function name
type MutableFunctionDescriptorGroup struct {
	// funcGroup will represents a group of function descriptor with different args using same funcname
	FuncMutGroup []MutableFunctionDescriptor
	// function name without arg
	FuncGroupName string
	// Function Parent Id
	FunctionParentID ID
}

// FuncDesc is custom function desc for function descriptor
type FuncDesc struct {
	// Desc is descriptor of this function
	Desc *FunctionDescriptor
	// Expiration is expiration time of this desc
	Expiration hlc.Timestamp
	// TableDescriptor will store tabledesc call back
	TableDesc *ImmutableTableDescriptor
}

// ImmutableFunctionDescriptor is custom function desc for immutable function group
type ImmutableFunctionDescriptor struct {
	// FuncGroup stand for a group of function desc with same name
	FuncGroup []FuncDesc
	// same name of this group functions
	FunctionGroupName string
	// Function Parent Id
	FunctionParentID ID
	// Function Group Leader Id
	FunctionLeaderID ID
}

// ImmutableTableDescriptor is a custom type for TableDescriptors
// It holds precomputed values and the underlying TableDescriptor
// should be const.
type ImmutableTableDescriptor struct {
	TableDescriptor

	// FunctionDesc represent a user define function's desc, when treat this object as an function
	// TODO: to consider using a new object struct refact this design.
	FunctionDesc *FunctionDescriptor

	// publicAndNonPublicCols is a list of public and non-public columns.
	// It is partitioned by the state of the column: public, write-only, delete-only
	publicAndNonPublicCols []ColumnDescriptor

	// publicAndNonPublicCols is a list of public and non-public indexes.
	// It is partitioned by the state of the index: public, write-only, delete-only
	publicAndNonPublicIndexes []IndexDescriptor

	writeOnlyColCount   int
	writeOnlyIndexCount int

	allChecks []TableDescriptor_CheckConstraint

	// ReadableColumns is a list of columns (including those undergoing a schema change)
	// which can be scanned. Columns in the process of a schema change
	// are all set to nullable while column backfilling is still in
	// progress, as mutation columns may have NULL values.
	ReadableColumns []ColumnDescriptor

	// partialIndexOrds contains the ordinal of each partial index.
	partialIndexOrds util.FastIntSet
}

// InvalidMutationID is the uninitialised mutation id.
const InvalidMutationID MutationID = 0

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
)

// ErrMissingColumns indicates a table with no columns.
var ErrMissingColumns = errors.New("table must contain at least 1 column")

// ErrMissingPrimaryKey indicates a table with no primary key.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

func validateName(name, typ string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
}

// ToEncodingDirection converts a direction from the proto to an encoding.Direction.
func (dir IndexDescriptor_Direction) ToEncodingDirection() (encoding.Direction, error) {
	switch dir {
	case IndexDescriptor_ASC:
		return encoding.Ascending, nil
	case IndexDescriptor_DESC:
		return encoding.Descending, nil
	default:
		return encoding.Ascending, errors.Errorf("invalid direction: %s", dir)
	}
}

// ErrDescriptorNotFound is returned by GetTableDescFromID to signal that a
// descriptor could not be found with the given id.
var ErrDescriptorNotFound = errors.New("descriptor not found")

// ErrIndexGCMutationsList is returned by FindIndexByID to signal that the
// index with the given ID does not have a descriptor and is in the garbage
// collected mutations list.
var ErrIndexGCMutationsList = errors.New("index in GC mutations list")

// NewMutableCreatedTableDescriptor returns a MutableTableDescriptor from the
// given TableDescriptor with the cluster version being the zero table. This
// is for a table that is created in the transaction.
func NewMutableCreatedTableDescriptor(tbl TableDescriptor) *MutableTableDescriptor {
	return &MutableTableDescriptor{TableDescriptor: tbl}
}

// NewMutableExistingTableDescriptor returns a MutableTableDescriptor from the
// given TableDescriptor with the cluster version also set to the descriptor.
// This is for an existing table.
func NewMutableExistingTableDescriptor(tbl TableDescriptor) *MutableTableDescriptor {
	return &MutableTableDescriptor{TableDescriptor: tbl, ClusterVersion: tbl}
}

// NewMutableCreatedFunctionDescriptor returns a MutableTableDescriptor from the
// given TableDescriptor with the cluster version being the zero table. This
// is for a table that is created in the transaction.
func NewMutableCreatedFunctionDescriptor(fc FunctionDescriptor) *MutableFunctionDescriptor {
	return &MutableFunctionDescriptor{FunctionDescriptor: fc}
}

// NewMutableExistingFunctionDescriptor returns a MutableTableDescriptor from the
// given FunctionDescriptor with the cluster version also set to the descriptor.
// This is for an existing table.
func NewMutableExistingFunctionDescriptor(fc FunctionDescriptor) *MutableFunctionDescriptor {
	return &MutableFunctionDescriptor{FunctionDescriptor: fc, ClusterVersion: fc}
}

// NewImmutableFunctionDescriptor returns a ImmutableFunctionDescriptor from the
// given FunctioinDescriptor
func NewImmutableFunctionDescriptor(function FunctionDescriptor) *ImmutableFunctionDescriptor {
	funcDesc := FuncDesc{
		Desc: &function,
	}
	return &ImmutableFunctionDescriptor{
		FuncGroup:         []FuncDesc{funcDesc},
		FunctionGroupName: function.Name,
		FunctionParentID:  function.ParentID,
	}
}

// NewImmutableTableDescriptor returns a ImmutableTableDescriptor from the
// given TableDescriptor.
func NewImmutableTableDescriptor(tbl TableDescriptor) *ImmutableTableDescriptor {
	publicAndNonPublicCols := tbl.Columns
	publicAndNonPublicIndexes := tbl.Indexes

	readableCols := tbl.Columns

	desc := &ImmutableTableDescriptor{TableDescriptor: tbl}

	if len(tbl.Mutations) > 0 {
		publicAndNonPublicCols = make([]ColumnDescriptor, 0, len(tbl.Columns)+len(tbl.Mutations))
		publicAndNonPublicIndexes = make([]IndexDescriptor, 0, len(tbl.Indexes)+len(tbl.Mutations))
		readableCols = make([]ColumnDescriptor, 0, len(tbl.Columns)+len(tbl.Mutations))

		publicAndNonPublicCols = append(publicAndNonPublicCols, tbl.Columns...)
		publicAndNonPublicIndexes = append(publicAndNonPublicIndexes, tbl.Indexes...)
		readableCols = append(readableCols, tbl.Columns...)

		// Fill up mutations into the column/index lists by placing the writable columns/indexes
		// before the delete only columns/indexes.
		for _, m := range tbl.Mutations {
			switch m.State {
			case DescriptorMutation_DELETE_AND_WRITE_ONLY:
				if idx := m.GetIndex(); idx != nil {
					publicAndNonPublicIndexes = append(publicAndNonPublicIndexes, *idx)
					desc.writeOnlyIndexCount++
				} else if col := m.GetColumn(); col != nil {
					publicAndNonPublicCols = append(publicAndNonPublicCols, *col)
					desc.writeOnlyColCount++
				}
			}
		}

		for _, m := range tbl.Mutations {
			switch m.State {
			case DescriptorMutation_DELETE_ONLY:
				if idx := m.GetIndex(); idx != nil {
					publicAndNonPublicIndexes = append(publicAndNonPublicIndexes, *idx)
				} else if col := m.GetColumn(); col != nil {
					publicAndNonPublicCols = append(publicAndNonPublicCols, *col)
				}
			}
		}

		// Iterate through all mutation columns.
		for _, c := range publicAndNonPublicCols[len(tbl.Columns):] {
			// Mutation column may need to be fetched, but may not be completely backfilled
			// and have be null values (even though they may be configured as NOT NULL).
			c.Nullable = true
			readableCols = append(readableCols, c)
		}
	}

	// Track partial index ordinals.
	for i := range publicAndNonPublicIndexes {
		if publicAndNonPublicIndexes[i].IsPartial() {
			desc.partialIndexOrds.Add(i)
		}
	}

	desc.ReadableColumns = readableCols
	desc.publicAndNonPublicCols = publicAndNonPublicCols
	desc.publicAndNonPublicIndexes = publicAndNonPublicIndexes

	desc.allChecks = make([]TableDescriptor_CheckConstraint, len(tbl.Checks))
	for i, c := range tbl.Checks {
		desc.allChecks[i] = *c
	}

	return desc
}

// PartialIndexOrds returns a set containing the ordinal of each partial index
// defined on the table.
func (desc *ImmutableTableDescriptor) PartialIndexOrds() util.FastIntSet {
	return desc.partialIndexOrds
}

// GetTableName returns the full table name including the database name and schema name by table ID and name
func GetTableName(
	ctx context.Context, txn *client.Txn, id ID, name string,
) (*tree.TableName, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)
	var tbName tree.TableName

	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}
	db := desc.GetDatabase()
	if db == nil {
		sc := desc.GetSchema()
		if sc == nil {
			return nil, ErrDescriptorNotFound
		}
		if err := txn.GetProto(ctx, MakeDescMetadataKey(sc.ParentID), desc); err != nil {
			return nil, err
		}
		db = desc.GetDatabase()
		if db == nil {
			return nil, ErrDescriptorNotFound
		}
		tbName = tree.MakeTableNameWithSchema(tree.Name(db.Name), tree.Name(sc.Name), tree.Name(name))
		return &tbName, nil
	}
	tbName = tree.MakeTableName(tree.Name(db.Name), tree.Name(name))
	return &tbName, nil
}

// GetDatabaseDescFromID retrieves the database descriptor for the database
// ID passed in using an existing txn. Returns an error if the descriptor
// doesn't exist or if it exists and is not a database.
func GetDatabaseDescFromID(
	ctx context.Context, txn *client.Txn, id ID,
) (*DatabaseDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}
	db := desc.GetDatabase()
	if db == nil {
		sc := desc.GetSchema()
		if sc == nil {
			return nil, ErrDescriptorNotFound
		}
		if err := txn.GetProto(ctx, MakeDescMetadataKey(sc.ParentID), desc); err != nil {
			return nil, err
		}
		db = desc.GetDatabase()
		if db == nil {
			return nil, ErrDescriptorNotFound
		}
	}
	return db, nil
}

// GetSchemaDescFromID retrieves the schema descriptor for the schema
// ID passed in using an existing txn. Returns an error if the
// descriptor doesn't exist or if it exists and is not a schema.
func GetSchemaDescFromID(ctx context.Context, txn *client.Txn, id ID) (*SchemaDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}
	schema := desc.GetSchema()
	if schema == nil {
		return nil, ErrDescriptorNotFound
	}
	return schema, nil
}

// GetTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing txn. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
func GetTableDescFromID(ctx context.Context, txn *client.Txn, id ID) (*TableDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)
	ts, err := txn.GetProtoTs(ctx, descKey, desc)
	if err != nil {
		return nil, err
	}
	table := desc.Table(ts)
	if table == nil {
		return nil, ErrDescriptorNotFound
	}
	return table, nil
}

// GetMutableTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing txn. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
// Otherwise a mutable copy of the table is returned.
func GetMutableTableDescFromID(
	ctx context.Context, txn *client.Txn, id ID,
) (*MutableTableDescriptor, error) {
	table, err := GetTableDescFromID(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	return NewMutableExistingTableDescriptor(*table), nil
}

// GetFunctionDescFromID retrieves the function descriptor for the function
// ID passed in using an existing txn. Returns an error if the
// descriptor doesn't exist or if it exists and is not a function.
func GetFunctionDescFromID(
	ctx context.Context, txn *client.Txn, id ID,
) (*FunctionDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}
	function := desc.GetFunction()
	if function == nil {
		return nil, ErrDescriptorNotFound
	}
	return function, nil
}

// GetMutableFunctionDescFromID retrieves the function descriptor for the function
// ID passed in using an existing txn. Returns an error if the
// descriptor doesn't exist or if it exists and is not a function.
// Otherwise a mutable copy of the function is returned.
func GetMutableFunctionDescFromID(
	ctx context.Context, txn *client.Txn, id ID,
) (*MutableFunctionDescriptor, error) {
	function, err := GetFunctionDescFromID(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	return NewMutableExistingFunctionDescriptor(*function), nil
}

// RunOverAllColumns applies its argument fn to each of the column IDs in desc.
// If there is an error, that error is returned immediately.
func (desc *IndexDescriptor) RunOverAllColumns(fn func(id ColumnID) error) error {
	for _, colID := range desc.ColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	for _, colID := range desc.ExtraColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	for _, colID := range desc.StoreColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	return nil
}

// allocateName sets desc.Name to a value that is not EqualName to any
// of tableDesc's indexes. allocateName roughly follows PostgreSQL's
// convention for automatically-named indexes.
func (desc *IndexDescriptor) allocateName(tableDesc *MutableTableDescriptor) {
	segments := make([]string, 0, len(desc.ColumnNames)+2)
	segments = append(segments, tableDesc.Name)
	segments = append(segments, desc.ColumnNames...)
	if desc.Unique {
		segments = append(segments, "key")
	} else {
		segments = append(segments, "idx")
	}

	baseName := strings.Join(segments, "_")
	name := baseName

	exists := func(name string) bool {
		_, _, err := tableDesc.FindIndexByName(name)
		return err == nil
	}
	for i := 1; exists(name); i++ {
		name = fmt.Sprintf("%s%d", baseName, i)
	}

	desc.Name = name
}

// FillFuncColumns is for fill columns when is a func index
func (desc *IndexDescriptor) FillFuncColumns(tab *MutableTableDescriptor) {
	for _, column := range tab.Columns {
		if int(column.ID) == len(tab.Columns) {
			continue
		}
		desc.IsFunc = append(desc.IsFunc, false)
		desc.ColumnNames = append(desc.ColumnNames, column.Name)
		desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_ASC)
		desc.IsRealFunc = append(desc.IsRealFunc, false)
	}
}

//IsFuncIndex is to tell if is a func index
func (desc *IndexDescriptor) IsFuncIndex() bool {
	for _, isfunc := range desc.IsFunc {
		if isfunc {
			return true
		}
	}
	return false
}

// FillColumns sets the column names and directions in desc.
func (desc *IndexDescriptor) FillColumns(elems tree.IndexElemList) error {
	desc.ColumnNames = make([]string, 0, len(elems))
	desc.ColumnDirections = make([]IndexDescriptor_Direction, 0, len(elems))
	for _, c := range elems {
		if desc.IsFuncIndex() {
			if c.IsFuncIndex() {
				desc.IsFunc = append(desc.IsFunc, true)
			} else {
				desc.IsFunc = append(desc.IsFunc, false)
			}
		}
		desc.ColumnNames = append(desc.ColumnNames, string(c.Column))
		switch c.Direction {
		case tree.Ascending, tree.DefaultDirection:
			desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_ASC)
		case tree.Descending:
			desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_DESC)
		default:
			return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
		}
	}
	return nil
}

// FillFunctionColumns sets the column names and directions in desc.
func (desc *IndexDescriptor) FillFunctionColumns(elems tree.IndexElemList) error {
	if desc.ColumnNames == nil {
		desc.ColumnNames = make([]string, 0, len(elems))
	}
	if desc.ColumnDirections == nil {
		desc.ColumnDirections = make([]IndexDescriptor_Direction, 0, len(elems))
	}
loop:
	for _, c := range elems {
		if c.IsFuncIndex() {
			desc.IsRealFunc = append(desc.IsRealFunc, true)
			desc.IsFunc = append(desc.IsFunc, true)
			desc.ColumnNames = append(desc.ColumnNames, string(c.Column))
			switch c.Direction {
			case tree.Ascending, tree.DefaultDirection:
				desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_ASC)
			case tree.Descending:
				desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_DESC)
			default:
				return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
			}
		} else {
			for i, column := range desc.ColumnNames {
				if column == string(c.Column) {
					switch c.Direction {
					case tree.Ascending, tree.DefaultDirection:
						desc.ColumnDirections[i] = IndexDescriptor_ASC
					case tree.Descending:
						desc.ColumnDirections[i] = IndexDescriptor_DESC
					default:
						return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
					}
					desc.IsRealFunc[i] = true
					continue loop
				}
			}
			desc.IsFunc = append(desc.IsFunc, false)
			desc.IsRealFunc = append(desc.IsRealFunc, true)
			desc.ColumnNames = append(desc.ColumnNames, string(c.Column))
			switch c.Direction {
			case tree.Ascending, tree.DefaultDirection:
				desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_ASC)
			case tree.Descending:
				desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_DESC)
			default:
				return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
			}
		}
	}
	return nil
}

type returnTrue struct{}

func (returnTrue) Error() string { panic("unimplemented") }

var returnTruePseudoError error = returnTrue{}

// ContainsColumnID returns true if the index descriptor contains the specified
// column ID either in its explicit column IDs, the extra column IDs, or the
// stored column IDs.
func (desc *IndexDescriptor) ContainsColumnID(colID ColumnID) bool {
	return desc.RunOverAllColumns(func(id ColumnID) error {
		if id == colID {
			return returnTruePseudoError
		}
		return nil
	}) != nil
}

// FullColumnIDs returns the index column IDs including any extra (implicit or
// stored (old STORING encoding)) column IDs for non-unique indexes. It also
// returns the direction with which each column was encoded.
func (desc *IndexDescriptor) FullColumnIDs() ([]ColumnID, []IndexDescriptor_Direction) {
	if desc.Unique {
		return desc.ColumnIDs, desc.ColumnDirections
	}
	// Non-unique indexes have some of the primary-key columns appended to
	// their key.
	columnIDs := append([]ColumnID(nil), desc.ColumnIDs...)
	columnIDs = append(columnIDs, desc.ExtraColumnIDs...)
	dirs := append([]IndexDescriptor_Direction(nil), desc.ColumnDirections...)
	for range desc.ExtraColumnIDs {
		// Extra columns are encoded in ascending order.
		dirs = append(dirs, IndexDescriptor_ASC)
	}
	return columnIDs, dirs
}

// ColNamesFormat writes a string describing the column names and directions
// in this index to the given buffer.
func (desc *IndexDescriptor) ColNamesFormat(ctx *tree.FmtCtx) {
	var org int
	for i := range desc.ColumnNames {
		if desc.IsRealFunc != nil {
			if !desc.IsRealFunc[i] {
				continue
			}
		}
		if i > 0 && org != 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNameP(&desc.ColumnNames[i])
		org++
		if desc.Type != IndexDescriptor_INVERTED {
			ctx.WriteByte(' ')
			ctx.WriteString(desc.ColumnDirections[i].String())
		}
	}
}

// ColNamesString returns a string describing the column names and directions
// in this index.
func (desc *IndexDescriptor) ColNamesString() string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	desc.ColNamesFormat(f)
	return f.CloseAndGetString()
}

// SQLString returns the SQL string describing this index. If non-empty,
// "ON tableName" is included in the output in the correct place.
func (desc *IndexDescriptor) SQLString(
	tb *TableDescriptor, tableName *tree.TableName, semaCtx *tree.SemaContext,
) string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	if desc.Unique {
		f.WriteString("UNIQUE ")
	}
	if desc.Type == IndexDescriptor_INVERTED {
		f.WriteString("INVERTED ")
	}
	f.WriteString("INDEX ")
	if *tableName != AnonymousTable {
		f.WriteString("ON ")
		f.FormatNode(tableName)
	}
	f.FormatNameP(&desc.Name)
	f.WriteString(" (")
	desc.ColNamesFormat(f)
	f.WriteByte(')')

	if desc.IsPartial() {
		f.WriteString(" WHERE ")
		pred, err := FormatExprForDisplay(*tb, desc.PredExpr, semaCtx)
		if err == nil {
			f.WriteString(pred)
		}
	}

	if len(desc.StoreColumnNames) > 0 {
		f.WriteString(" STORING (")
		for i := range desc.StoreColumnNames {
			if i > 0 {
				f.WriteString(", ")
			}
			f.FormatNameP(&desc.StoreColumnNames[i])
		}
		f.WriteByte(')')
	}
	return f.CloseAndGetString()
}

// GetEncodingType returns the encoding type of this index. For backward
// compatibility reasons, this might not match what is stored in
// desc.EncodingType. The primary index's ID must be passed so we can check if
// this index is primary or secondary.
func (desc *IndexDescriptor) GetEncodingType(primaryIndexID IndexID) IndexDescriptorEncodingType {
	if desc.ID == primaryIndexID {
		// Primary indexes always use the PrimaryIndexEncoding, regardless of what
		// desc.EncodingType indicates.
		return PrimaryIndexEncoding
	}
	return desc.EncodingType
}

// IsInterleaved returns whether the index is interleaved or not.
func (desc *IndexDescriptor) IsInterleaved() bool {
	return len(desc.Interleave.Ancestors) > 0 || len(desc.InterleavedBy) > 0
}

//IsPartial is for tell is partialindex or not
func (desc *IndexDescriptor) IsPartial() bool {
	return desc.PredExpr != ""
}

//IsPartitioned is for tell is partition-index or not
func (desc *IndexDescriptor) IsPartitioned() bool {
	return desc.Partitioning.NumColumns != 0
}

// SetID implements the DescriptorProto interface.
func (desc *TableDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *TableDescriptor) TypeName() string {
	return "relation"
}

// SetName implements the DescriptorProto interface.
func (desc *TableDescriptor) SetName(name string) {
	desc.Name = name
}

// IsEmpty checks if the descriptor is uninitialized.
func (desc *TableDescriptor) IsEmpty() bool {
	// Valid tables cannot have an ID of 0.
	return desc.ID == 0
}

// IsTable returns true if the TableDescriptor actually describes a
// Table resource, as opposed to a different resource (like a View).
func (desc *TableDescriptor) IsTable() bool {
	return !desc.IsView() && !desc.IsSequence()
}

// IsView returns true if the TableDescriptor actually describes a
// View resource rather than a Table.
func (desc *TableDescriptor) IsView() bool {
	return desc.ViewQuery != ""
}

// MaterializedView returns whether or not this TableDescriptor is a
// MaterializedView.
func (desc *TableDescriptor) MaterializedView() bool {
	return desc.IsMaterializedView
}

// IsAs returns true if the TableDescriptor actually describes
// a Table resource with an As source.
func (desc *TableDescriptor) IsAs() bool {
	return desc.CreateQuery != ""
}

// IsSequence returns true if the TableDescriptor actually describes a
// Sequence resource rather than a Table.
func (desc *TableDescriptor) IsSequence() bool {
	return desc.SequenceOpts != nil
}

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the information_schema tables) and thus doesn't
// need to be physically stored.
func (desc *TableDescriptor) IsVirtualTable() bool {
	return IsVirtualTable(desc.ID)
}

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the informationgi_schema tables) and thus doesn't
// need to be physically stored.
func IsVirtualTable(id ID) bool {
	return MinVirtualID <= id
}

// IsPhysicalTable returns true if the TableDescriptor actually describes a
// physical Table that needs to be stored in the kv layer, as opposed to a
// different resource like a view or a virtual table. Physical tables have
// primary keys, column families, and indexes (unlike virtual tables).
// Sequences count as physical tables because their values are stored in
// the KV layer.
func (desc *TableDescriptor) IsPhysicalTable() bool {
	return desc.IsSequence() || (desc.IsTable() && !desc.IsVirtualTable()) || desc.MaterializedView()
}

// KeysPerRow returns the maximum number of keys used to encode a row for the
// given index. For secondary indexes, we always only use one, but for primary
// indexes, we can encode up to one kv per column family.
func (desc *TableDescriptor) KeysPerRow(indexID IndexID) int {
	if desc.PrimaryIndex.ID == indexID {
		return len(desc.Families)
	}
	return 1
}

// AllNonDropColumns returns all the columns, including those being added
// in the mutations.
func (desc *TableDescriptor) AllNonDropColumns() []ColumnDescriptor {
	cols := make([]ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
	cols = append(cols, desc.Columns...)
	for _, m := range desc.Mutations {
		if col := m.GetColumn(); col != nil {
			if m.Direction == DescriptorMutation_ADD {
				cols = append(cols, *col)
			}
		}
	}
	return cols
}

// AllNonDropIndexes returns all the indexes, including those being added
// in the mutations.
func (desc *TableDescriptor) AllNonDropIndexes() []*IndexDescriptor {
	indexes := make([]*IndexDescriptor, 0, 1+len(desc.Indexes)+len(desc.Mutations))
	if desc.IsPhysicalTable() || desc.IsMaterializedView {
		indexes = append(indexes, &desc.PrimaryIndex)
	}
	for i := range desc.Indexes {
		indexes = append(indexes, &desc.Indexes[i])
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			if m.Direction == DescriptorMutation_ADD {
				indexes = append(indexes, idx)
			}
		}
	}
	return indexes
}

// AllActiveAndInactiveChecks returns all check constraints, including both
// "active" ones on the table descriptor which are being enforced for all
// writes, and "inactive" ones queued in the mutations list.
func (desc *TableDescriptor) AllActiveAndInactiveChecks() []*TableDescriptor_CheckConstraint {
	// A check constraint could be both on the table descriptor and in the
	// list of mutations while the constraint is validated for existing rows. In
	// that case, the constraint is in the Validating state, and we avoid
	// including it twice. (Note that even though unvalidated check constraints
	// cannot be added as of 19.1, they can still exist if they were created under
	// previous versions.)
	checks := make([]*TableDescriptor_CheckConstraint, 0, len(desc.Checks)+len(desc.Mutations))
	for _, c := range desc.Checks {
		if c.Validity != ConstraintValidity_Validating {
			checks = append(checks, c)
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetConstraint(); c != nil {
			checks = append(checks, &c.Check)
		}
	}
	return checks
}

// ForeachNonDropIndex runs a function on all indexes, including those being
// added in the mutations.
func (desc *TableDescriptor) ForeachNonDropIndex(f func(*IndexDescriptor) error) error {
	if desc.IsPhysicalTable() {
		if err := f(&desc.PrimaryIndex); err != nil {
			return err
		}
	}
	for i := range desc.Indexes {
		if err := f(&desc.Indexes[i]); err != nil {
			return err
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && m.Direction == DescriptorMutation_ADD {
			if err := f(idx); err != nil {
				return err
			}
		}
	}
	return nil
}

func generatedFamilyName(familyID FamilyID, columnNames []string) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "fam_%d", familyID)
	for _, n := range columnNames {
		buf.WriteString(`_`)
		buf.WriteString(n)
	}
	return buf.String()
}

// MaybeFillInDescriptor performs any modifications needed to the table descriptor.
// This includes format upgrades and optional changes that can be handled by all version
// (for example: additional default privileges).
// Returns true if any changes were made.
func (desc *TableDescriptor) MaybeFillInDescriptor() bool {
	changedVersion := desc.maybeUpgradeFormatVersion()
	priv := privilege.TablePrivileges
	if desc.IsSequence() {
		priv = privilege.SequencePrivileges
	}
	changedPrivileges := desc.Privileges.MaybeFixPrivileges(desc.ID, priv)
	return changedVersion || changedPrivileges
}

// maybeUpgradeFormatVersion transforms the TableDescriptor to the latest
// FormatVersion (if it's not already there) and returns true if any changes
// were made.
// This method should be called through MaybeFillInDescriptor, not directly.
func (desc *TableDescriptor) maybeUpgradeFormatVersion() bool {
	if desc.FormatVersion >= InterleavedFormatVersion {
		return false
	}
	desc.maybeUpgradeToFamilyFormatVersion()
	desc.FormatVersion = InterleavedFormatVersion
	return true
}

func (desc *TableDescriptor) maybeUpgradeToFamilyFormatVersion() bool {
	if desc.FormatVersion >= FamilyFormatVersion {
		return false
	}

	primaryIndexColumnIds := make(map[ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColumnIds[colID] = struct{}{}
	}

	desc.Families = []ColumnFamilyDescriptor{
		{ID: 0, Name: "primary"},
	}
	desc.NextFamilyID = desc.Families[0].ID + 1
	addFamilyForCol := func(col ColumnDescriptor) {
		if _, ok := primaryIndexColumnIds[col.ID]; ok {
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		colNames := []string{col.Name}
		family := ColumnFamilyDescriptor{
			ID:              FamilyID(col.ID),
			Name:            generatedFamilyName(FamilyID(col.ID), colNames),
			ColumnNames:     colNames,
			ColumnIDs:       []ColumnID{col.ID},
			DefaultColumnID: col.ID,
		}
		desc.Families = append(desc.Families, family)
		if family.ID >= desc.NextFamilyID {
			desc.NextFamilyID = family.ID + 1
		}
	}

	for _, c := range desc.Columns {
		addFamilyForCol(c)
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			addFamilyForCol(*c)
		}
	}

	desc.FormatVersion = FamilyFormatVersion

	return true
}

// AllocateIDs allocates column, family, and index ids for any column, family,
// or index which has an ID of 0.
func (desc *MutableTableDescriptor) AllocateIDs() error {
	// Only physical tables can have / need a primary key.
	if desc.IsPhysicalTable() {
		if err := desc.ensurePrimaryKey(); err != nil {
			return err
		}
	}

	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
	}
	if desc.Version == 0 {
		desc.Version = 1
	}
	if desc.NextMutationID == InvalidMutationID {
		desc.NextMutationID = 1
	}

	columnNames := map[string]ColumnID{}
	fillColumnID := func(c *ColumnDescriptor) {
		columnID := c.ID
		if columnID == 0 {
			columnID = desc.NextColumnID
			desc.NextColumnID++
		}
		columnNames[c.Name] = columnID
		c.ID = columnID
	}
	for i := range desc.Columns {
		fillColumnID(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			fillColumnID(c)
		}
	}

	// Only physical tables can have / need indexes and column families.
	if desc.IsPhysicalTable() {
		if err := desc.allocateIndexIDs(columnNames); err != nil {
			return err
		}
		desc.allocateColumnFamilyIDs(columnNames)
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.MinUserDescID
	}
	err := desc.ValidateTable(nil)
	desc.ID = savedID
	return err
}

// AllocateInheritsIDs allocates inherit column
func (desc *MutableTableDescriptor) AllocateInheritsIDs(cols []ColumnDescriptor) {

	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
	}
	if desc.Version == 0 {
		desc.Version = 1
	}
	if desc.NextMutationID == InvalidMutationID {
		desc.NextMutationID = 1
	}

	fillColumnID := func(c *ColumnDescriptor) {
		c.ID = desc.NextColumnID
		desc.NextColumnID++
	}
	for i := range cols {
		fillColumnID(&cols[i])
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.MinUserDescID
	}
	//err := desc.ValidateTable(nil)
	desc.ID = savedID
}

func (desc *MutableTableDescriptor) ensurePrimaryKey() error {
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() {
		// Ensure a Primary Key exists.
		nameExists := func(name string) bool {
			_, _, err := desc.FindColumnByName(tree.Name(name))
			return err == nil
		}
		s := "unique_rowid()"
		colPrivs := InheritFromTablePrivileges(desc.Privileges)
		col := ColumnDescriptor{
			Name: GenerateUniqueConstraintName("rowid", nameExists),
			Type: ColumnType{
				SemanticType:    ColumnType_INT,
				VisibleTypeName: coltypes.VisibleTypeName[coltypes.VisINT],
			},
			DefaultExpr: &s,
			Hidden:      true,
			Nullable:    false,
			Privileges:  colPrivs,
			ParentID:    desc.ID,
		}
		desc.AddColumn(col)
		idx := IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{col.Name},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
		if err := desc.AddIndex(idx, true); err != nil {
			return err
		}
	}
	return nil
}

// HasCompositeKeyEncoding returns true if key columns of the given kind can
// have a composite encoding. For such types, it can be decided on a
// case-by-base basis whether a given Datum requires the composite encoding.
func HasCompositeKeyEncoding(semanticType ColumnType_SemanticType) bool {
	switch semanticType {
	case ColumnType_COLLATEDSTRING,
		ColumnType_FLOAT,
		ColumnType_DECIMAL:
		return true
	}
	return false
}

// DatumTypeHasCompositeKeyEncoding is a version of HasCompositeKeyEncoding
// which works on datum types.
func DatumTypeHasCompositeKeyEncoding(typ types.T) bool {
	colType, err := datumTypeToColumnSemanticType(typ)
	return err == nil && HasCompositeKeyEncoding(colType)
}

// MustBeValueEncoded returns true if columns of the given kind can only be value
// encoded.
func MustBeValueEncoded(semanticType ColumnType_SemanticType) bool {
	return semanticType == ColumnType_ARRAY ||
		semanticType == ColumnType_JSONB ||
		semanticType == ColumnType_TUPLE
}

// HasOldStoredColumns returns whether the index has stored columns in the old
// format (data encoded the same way as if they were in an implicit column).
func (desc *IndexDescriptor) HasOldStoredColumns() bool {
	return len(desc.ExtraColumnIDs) > 0 && len(desc.StoreColumnIDs) < len(desc.StoreColumnNames)
}

func (desc *MutableTableDescriptor) allocateIndexIDs(columnNames map[string]ColumnID) error {
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}

	// Keep track of unnamed indexes.
	anonymousIndexes := make([]*IndexDescriptor, 0, len(desc.Indexes)+len(desc.Mutations))

	// Create a slice of modifiable index descriptors.
	indexes := make([]*IndexDescriptor, 0, 1+len(desc.Indexes)+len(desc.Mutations))
	indexes = append(indexes, &desc.PrimaryIndex)
	collectIndexes := func(index *IndexDescriptor) {
		if len(index.Name) == 0 {
			anonymousIndexes = append(anonymousIndexes, index)
		}
		indexes = append(indexes, index)
	}
	for i := range desc.Indexes {
		collectIndexes(&desc.Indexes[i])
	}
	for _, m := range desc.Mutations {
		if index := m.GetIndex(); index != nil {
			collectIndexes(index)
		}
	}

	for _, index := range anonymousIndexes {
		index.allocateName(desc)
	}

	isCompositeColumn := make(map[ColumnID]struct{})
	for _, col := range desc.Columns {
		if HasCompositeKeyEncoding(col.Type.SemanticType) {
			isCompositeColumn[col.ID] = struct{}{}
		}
	}

	// Populate IDs.
	for _, index := range indexes {
		if index.ID != 0 {
			// This index has already been populated. Nothing to do.
			continue
		}
		index.ID = desc.NextIndexID
		desc.NextIndexID++

		for j, colName := range index.ColumnNames {
			if len(index.ColumnIDs) <= j {
				index.ColumnIDs = append(index.ColumnIDs, 0)
			}
			if index.ColumnIDs[j] == 0 {
				if index.IsFunc != nil {
					if index.IsFunc[j] == true {
						//var max int
						//for _,index := range desc.TableDesc().Indexes {
						//	for _,id := range index.ColumnIDs {
						//		if int(id) > max {
						//			max = int(id)
						//		}
						//	}
						//}
						//desc.TableDesc().MaxColumnID = int32(max + 1)
						index.ColumnIDs[j] = ColumnID(desc.TableDesc().MaxColumnID)
					} else {
						index.ColumnIDs[j] = columnNames[colName]
					}
				} else {
					index.ColumnIDs[j] = columnNames[colName]
				}
			}
		}

		if index != &desc.PrimaryIndex {
			indexHasOldStoredColumns := index.HasOldStoredColumns()
			// Need to clear ExtraColumnIDs and StoreColumnIDs because they are used
			// by ContainsColumnID.
			index.ExtraColumnIDs = nil
			index.StoreColumnIDs = nil
			var extraColumnIDs []ColumnID
			for _, primaryColID := range desc.PrimaryIndex.ColumnIDs {
				if !index.ContainsColumnID(primaryColID) {
					extraColumnIDs = append(extraColumnIDs, primaryColID)
				}
			}
			index.ExtraColumnIDs = extraColumnIDs

			for _, colName := range index.StoreColumnNames {
				col, _, err := desc.FindColumnByName(tree.Name(colName))
				if err != nil {
					return err
				}
				if desc.PrimaryIndex.ContainsColumnID(col.ID) {
					// If the primary index contains a stored column, we don't need to
					// store it - it's already part of the index.
					return pgerror.NewErrorf(
						pgcode.DuplicateColumn, "index %q already contains column %q", index.Name, col.Name).
						SetDetailf("column %q is part of the primary index and therefore implicit in all indexes", col.Name)
				}
				if index.ContainsColumnID(col.ID) {
					return pgerror.NewErrorf(
						pgcode.DuplicateColumn,
						"index %q already contains column %q", index.Name, col.Name)
				}
				if indexHasOldStoredColumns {
					index.ExtraColumnIDs = append(index.ExtraColumnIDs, col.ID)
				} else {
					index.StoreColumnIDs = append(index.StoreColumnIDs, col.ID)
				}
			}
		}

		index.CompositeColumnIDs = nil
		for _, colID := range index.ColumnIDs {
			if _, ok := isCompositeColumn[colID]; ok {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
		for _, colID := range index.ExtraColumnIDs {
			if _, ok := isCompositeColumn[colID]; ok {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
	}
	return nil
}

func (desc *MutableTableDescriptor) allocateColumnFamilyIDs(columnNames map[string]ColumnID) {
	if desc.NextFamilyID == 0 {
		if len(desc.Families) == 0 {
			desc.Families = []ColumnFamilyDescriptor{
				{ID: 0, Name: "primary"},
			}
		}
		desc.NextFamilyID = 1
	}

	columnsInFamilies := make(map[ColumnID]struct{}, len(desc.Columns))
	for i, family := range desc.Families {
		if family.ID == 0 && i != 0 {
			family.ID = desc.NextFamilyID
			desc.NextFamilyID++
		}

		for j, colName := range family.ColumnNames {
			if len(family.ColumnIDs) <= j {
				family.ColumnIDs = append(family.ColumnIDs, 0)
			}
			if family.ColumnIDs[j] == 0 {
				family.ColumnIDs[j] = columnNames[colName]
			}
			columnsInFamilies[family.ColumnIDs[j]] = struct{}{}
		}

		desc.Families[i] = family
	}

	primaryIndexColIDs := make(map[ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColIDs[colID] = struct{}{}
	}

	ensureColumnInFamily := func(col *ColumnDescriptor) {
		if _, ok := columnsInFamilies[col.ID]; ok {
			return
		}
		if _, ok := primaryIndexColIDs[col.ID]; ok {
			// Primary index columns are required to be assigned to family 0.
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		var familyID FamilyID
		if desc.ParentID == keys.SystemDatabaseID {
			// TODO(dan): This assigns families such that the encoding is exactly the
			// same as before column families. It's used for all system tables because
			// reads of them don't go through the normal sql layer, which is where the
			// knowledge of families lives. Fix that and remove this workaround.
			familyID = FamilyID(col.ID)
			desc.Families = append(desc.Families, ColumnFamilyDescriptor{
				ID:          familyID,
				ColumnNames: []string{col.Name},
				ColumnIDs:   []ColumnID{col.ID},
			})
		} else {
			idx, ok := fitColumnToFamily(desc, *col)
			if !ok {
				idx = len(desc.Families)
				desc.Families = append(desc.Families, ColumnFamilyDescriptor{
					ID:          desc.NextFamilyID,
					ColumnNames: []string{},
					ColumnIDs:   []ColumnID{},
				})
			}
			familyID = desc.Families[idx].ID
			desc.Families[idx].ColumnNames = append(desc.Families[idx].ColumnNames, col.Name)
			desc.Families[idx].ColumnIDs = append(desc.Families[idx].ColumnIDs, col.ID)
		}
		if familyID >= desc.NextFamilyID {
			desc.NextFamilyID = familyID + 1
		}
	}
	for i := range desc.Columns {
		ensureColumnInFamily(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			ensureColumnInFamily(c)
		}
	}

	for i, family := range desc.Families {
		if len(family.Name) == 0 {
			family.Name = generatedFamilyName(family.ID, family.ColumnNames)
		}

		if family.DefaultColumnID == 0 {
			defaultColumnID := ColumnID(0)
			for _, colID := range family.ColumnIDs {
				if _, ok := primaryIndexColIDs[colID]; !ok {
					if defaultColumnID == 0 {
						defaultColumnID = colID
					} else {
						defaultColumnID = ColumnID(0)
						break
					}
				}
			}
			family.DefaultColumnID = defaultColumnID
		}

		desc.Families[i] = family
	}
}

// RefreshValidate same as Validate with txn step
func (desc *TableDescriptor) RefreshValidate(
	ctx context.Context, txn *client.Txn, st *cluster.Settings,
) error {
	// 需要读到已提交数据
	if err := txn.Step(ctx); err != nil {
		return err
	}
	return desc.Validate(ctx, txn, st)
}

// Validate validates that the table descriptor is well formed. Checks include
// both single table and cross table invariants.
func (desc *TableDescriptor) Validate(
	ctx context.Context, txn *client.Txn, st *cluster.Settings,
) error {
	err := desc.ValidateTable(st)
	if err != nil {
		return err
	}
	if desc.Dropped() {
		return nil
	}
	return desc.validateCrossReferences(ctx, txn)
}

// validateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func (desc *TableDescriptor) validateCrossReferences(ctx context.Context, txn *client.Txn) error {
	// Check that parent DB exists.
	{
		res, err := txn.Get(ctx, MakeDescMetadataKey(desc.ParentID))
		if err != nil {
			return err
		}
		if !res.Exists() {
			return errors.Errorf("parentID %d does not exist", desc.ParentID)
		}
	}

	tablesByID := map[ID]*TableDescriptor{desc.ID: desc}
	getTable := func(id ID) (*TableDescriptor, error) {
		if table, ok := tablesByID[id]; ok {
			return table, nil
		}
		table, err := GetTableDescFromID(ctx, txn, id)
		if err != nil {
			return nil, err
		}
		tablesByID[id] = table
		return table, nil
	}

	findTargetIndex := func(tableID ID, indexID IndexID) (*TableDescriptor, *IndexDescriptor, error) {
		targetTable, err := getTable(tableID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "missing table=%d index=%d", tableID, indexID)
		}
		targetIndex, err := targetTable.FindIndexByID(indexID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "missing table=%s index=%d", targetTable.Name, indexID)
		}
		return targetTable, targetIndex, nil
	}

	for _, index := range desc.AllNonDropIndexes() {
		// Check foreign keys.
		if index.ForeignKey.IsSet() {
			targetTable, targetIndex, err := findTargetIndex(
				index.ForeignKey.Table, index.ForeignKey.Index)
			if err != nil {
				return errors.Wrap(err, "invalid foreign key")
			}
			found := false
			for _, backref := range targetIndex.ReferencedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return errors.Errorf("missing fk back reference to %q@%q from %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
		fkBackrefs := make(map[ForeignKeyReference]struct{})
		for _, backref := range index.ReferencedBy {
			if _, ok := fkBackrefs[backref]; ok {
				return errors.Errorf("duplicated fk backreference %+v", backref)
			}
			fkBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return errors.Wrapf(err, "invalid fk backreference table=%d index=%d",
					backref.Table, backref.Index)
			}
			targetIndex, err := targetTable.FindIndexByID(backref.Index)
			if err != nil {
				return errors.Wrapf(err, "invalid fk backreference table=%s index=%d",
					targetTable.Name, backref.Index)
			}
			if fk := targetIndex.ForeignKey; fk.Table != desc.ID || fk.Index != index.ID {
				return errors.Errorf("broken fk backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}

		// Check interleaves.
		if len(index.Interleave.Ancestors) > 0 {
			// Only check the most recent ancestor, the rest of them don't point
			// back.
			ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
			targetTable, targetIndex, err := findTargetIndex(ancestor.TableID, ancestor.IndexID)
			if err != nil {
				return errors.Wrap(err, "invalid interleave")
			}
			found := false
			for _, backref := range targetIndex.InterleavedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return errors.Errorf(
					"missing interleave back reference to %q@%q from %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
		interleaveBackrefs := make(map[ForeignKeyReference]struct{})
		for _, backref := range index.InterleavedBy {
			if _, ok := interleaveBackrefs[backref]; ok {
				return errors.Errorf("duplicated interleave backreference %+v", backref)
			}
			interleaveBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return errors.Wrapf(err, "invalid interleave backreference table=%d index=%d",
					backref.Table, backref.Index)
			}
			targetIndex, err := targetTable.FindIndexByID(backref.Index)
			if err != nil {
				return errors.Wrapf(err, "invalid interleave backreference table=%s index=%d",
					targetTable.Name, backref.Index)
			}
			if len(targetIndex.Interleave.Ancestors) == 0 {
				return errors.Errorf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
			// The last ancestor is required to be a backreference.
			ancestor := targetIndex.Interleave.Ancestors[len(targetIndex.Interleave.Ancestors)-1]
			if ancestor.TableID != desc.ID || ancestor.IndexID != index.ID {
				return errors.Errorf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
	}
	// TODO(dan): Also validate SharedPrefixLen in the interleaves.
	return nil
}

// ValidateTable validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use Validate to validate that cross-table references are
// correct.
// If version is supplied, the descriptor is checked for version incompatibilities.
func (desc *TableDescriptor) ValidateTable(st *cluster.Settings) error {
	if err := validateName(desc.Name, "table"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid table ID %d", desc.ID)
	}

	// TODO(dt, nathan): virtual descs don't validate (missing privs, PK, etc).
	if desc.IsVirtualTable() {
		return nil
	}

	if desc.IsSequence() {
		return nil
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.ParentID == 0 {
		return fmt.Errorf("invalid parent ID %d", desc.ParentID)
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// MaybeFillInDescriptor missing from some codepath.
	if v := desc.GetFormatVersion(); v != FamilyFormatVersion && v != InterleavedFormatVersion {
		// TODO(dan): We're currently switching from FamilyFormatVersion to
		// InterleavedFormatVersion. After a beta is released with this dual version
		// support, then:
		// - Upgrade the bidirectional reference version to that beta
		// - Start constructing all TableDescriptors with InterleavedFormatVersion
		// - Change maybeUpgradeFormatVersion to output InterleavedFormatVersion
		// - Change this check to only allow InterleavedFormatVersion
		return fmt.Errorf(
			"table %q is encoded using using version %d, but this client only supports version %d and %d",
			desc.Name, desc.GetFormatVersion(), FamilyFormatVersion, InterleavedFormatVersion)
	}

	if len(desc.Columns) == 0 {
		return ErrMissingColumns
	}

	if err := desc.CheckUniqueConstraints(); err != nil {
		return err
	}

	columnNames := make(map[string]ColumnID, len(desc.Columns))
	columnIDs := make(map[ColumnID]string, len(desc.Columns))
	for _, column := range desc.AllNonDropColumns() {
		if err := validateName(column.Name, "column"); err != nil {
			return err
		}
		if column.ID == 0 {
			return fmt.Errorf("invalid column ID %d", column.ID)
		}

		if _, ok := columnNames[column.Name]; ok {
			for _, col := range desc.Columns {
				if col.Name == column.Name {
					if col.Hidden {
						return fmt.Errorf("column name %q already exists but is hidden", column.Name)
					}
					return fmt.Errorf("duplicate column name: %q", column.Name)
				}
			}
			return fmt.Errorf("duplicate: column %q in the middle of being added, not yet public", column.Name)
		}
		columnNames[column.Name] = column.ID

		if other, ok := columnIDs[column.ID]; ok {
			return fmt.Errorf("column %q duplicate ID of column %q: %d",
				column.Name, other, column.ID)
		}
		columnIDs[column.ID] = column.Name

		if column.ID >= desc.NextColumnID {
			return fmt.Errorf("column %q invalid ID (%d) >= next column ID (%d)",
				column.Name, column.ID, desc.NextColumnID)
		}
	}

	if st != nil && st.Version.IsInitialized() {
		if !st.Version.IsActive(cluster.VersionBitArrayColumns) {
			for _, def := range desc.Columns {
				if def.Type.SemanticType == ColumnType_BIT {
					return fmt.Errorf("cluster version does not support BIT (required: %s)",
						cluster.VersionByKey(cluster.VersionBitArrayColumns))
				}
			}
		}
	}

	for _, m := range desc.Mutations {
		unSetEnums := m.State == DescriptorMutation_UNKNOWN || m.Direction == DescriptorMutation_NONE
		switch desc := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			col := desc.Column
			if unSetEnums {
				return errors.Errorf("mutation in state %s, direction %s, col %q, id %v", m.State, m.Direction, col.Name, col.ID)
			}
			columnIDs[col.ID] = col.Name
		case *DescriptorMutation_Index:
			if unSetEnums {
				idx := desc.Index
				return errors.Errorf("mutation in state %s, direction %s, index %s, id %v", m.State, m.Direction, idx.Name, idx.ID)
			}
		case *DescriptorMutation_Constraint:
			if unSetEnums {
				return errors.Errorf("mutation in state %s, direction %s, constraint %v", m.State, m.Direction, desc.Constraint.Name)
			}
		case *DescriptorMutation_PrimaryKeySwap:
			if m.Direction == DescriptorMutation_NONE {
				return errors.Errorf(
					"primary key swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
			}
		case *DescriptorMutation_MaterializedViewRefresh:
			if m.Direction == DescriptorMutation_NONE {
				return errors.Errorf(
					"materialized view refresh mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
			}
		default:
			return errors.Errorf("mutation in state %s, direction %s, and no column/index descriptor", m.State, m.Direction)
		}
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	// Only validate column families and indexes if this is actually a table, not
	// if it's just a view.
	if desc.IsPhysicalTable() {
		colIDToFamilyID, err := desc.validateColumnFamilies(columnIDs)
		if err != nil {
			return err
		}
		if err := desc.validateTableIndexes(columnNames, colIDToFamilyID); err != nil {
			return err
		}
		if err := desc.validatePartitioning(); err != nil {
			return err
		}
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	priv := privilege.TablePrivileges
	if desc.IsSequence() {
		priv = privilege.SequencePrivileges
	}
	desc.Privileges.MaybeFixPrivileges(desc.GetID(), priv)

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID(), priv)
}

func (desc *TableDescriptor) validateColumnFamilies(
	columnIDs map[ColumnID]string,
) (map[ColumnID]FamilyID, error) {
	if len(desc.Families) < 1 {
		return nil, fmt.Errorf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != FamilyID(0) {
		return nil, fmt.Errorf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[FamilyID]string{}
	colIDToFamilyID := map[ColumnID]FamilyID{}
	for _, family := range desc.Families {
		if err := validateName(family.Name, "family"); err != nil {
			return nil, err
		}

		if _, ok := familyNames[family.Name]; ok {
			return nil, fmt.Errorf("duplicate family name: %q", family.Name)
		}
		familyNames[family.Name] = struct{}{}

		if other, ok := familyIDs[family.ID]; ok {
			return nil, fmt.Errorf("family %q duplicate ID of family %q: %d",
				family.Name, other, family.ID)
		}
		familyIDs[family.ID] = family.Name

		if family.ID >= desc.NextFamilyID {
			return nil, fmt.Errorf("family %q invalid family ID (%d) > next family ID (%d)",
				family.Name, family.ID, desc.NextFamilyID)
		}

		if len(family.ColumnIDs) != len(family.ColumnNames) {
			return nil, fmt.Errorf("mismatched column ID size (%d) and name size (%d)",
				len(family.ColumnIDs), len(family.ColumnNames))
		}

		for i, colID := range family.ColumnIDs {
			name, ok := columnIDs[colID]
			if !ok {
				return nil, fmt.Errorf("family %q contains unknown column \"%d\"", family.Name, colID)
			}
			if name != family.ColumnNames[i] {
				return nil, fmt.Errorf("family %q column %d should have name %q, but found name %q",
					family.Name, colID, name, family.ColumnNames[i])
			}
		}

		for _, colID := range family.ColumnIDs {
			if famID, ok := colIDToFamilyID[colID]; ok {
				return nil, fmt.Errorf("column %d is in both family %d and %d", colID, famID, family.ID)
			}
			colIDToFamilyID[colID] = family.ID
		}
	}
	for colID := range columnIDs {
		if _, ok := colIDToFamilyID[colID]; !ok {
			return nil, fmt.Errorf("column %d is not in any column family", colID)
		}
	}
	return colIDToFamilyID, nil
}

// validateTableIndexes validates that indexes are well formed. Checks include
// validating the columns involved in the index, verifying the index names and
// IDs are unique, and the family of the primary key is 0. This does not check
// if indexes are unique (i.e. same set of columns, direction, and uniqueness)
// as there are practical uses for them.
func (desc *TableDescriptor) validateTableIndexes(
	columnNames map[string]ColumnID, colIDToFamilyID map[ColumnID]FamilyID,
) error {
	if len(desc.PrimaryIndex.ColumnIDs) == 0 {
		return ErrMissingPrimaryKey
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[IndexID]string{}
	for _, index := range desc.AllNonDropIndexes() {
		if err := validateName(index.Name, "index"); err != nil {
			return err
		}
		if index.ID == 0 {
			return fmt.Errorf("invalid index ID %d", index.ID)
		}

		if _, ok := indexNames[index.Name]; ok {
			for _, idx := range desc.Indexes {
				if idx.Name == index.Name {
					return fmt.Errorf("duplicate index name: %q", index.Name)
				}
			}
			return fmt.Errorf("duplicate: index %q in the middle of being added, not yet public", index.Name)
		}
		indexNames[index.Name] = struct{}{}

		if other, ok := indexIDs[index.ID]; ok {
			return fmt.Errorf("index %q duplicate ID of index %q: %d",
				index.Name, other, index.ID)
		}
		indexIDs[index.ID] = index.Name

		if index.ID >= desc.NextIndexID {
			return fmt.Errorf("index %q invalid index ID (%d) > next index ID (%d)",
				index.Name, index.ID, desc.NextIndexID)
		}

		if len(index.ColumnIDs) != len(index.ColumnNames) {
			return fmt.Errorf("mismatched column IDs (%d) and names (%d)",
				len(index.ColumnIDs), len(index.ColumnNames))
		}
		if len(index.ColumnIDs) != len(index.ColumnDirections) {
			return fmt.Errorf("mismatched column IDs (%d) and directions (%d)",
				len(index.ColumnIDs), len(index.ColumnDirections))
		}

		if len(index.ColumnIDs) == 0 {
			return fmt.Errorf("index %q must contain at least 1 column", index.Name)
		}

		validateIndexDup := make(map[ColumnID]struct{})
		if index.IsFunc != nil {
			for i, name := range index.ColumnNames {
				if !index.IsFunc[i] {
					colID, ok := columnNames[name]
					if !ok {
						return fmt.Errorf("index %q contains unknown column %q", index.Name, name)
					}
					if colID != index.ColumnIDs[i] {
						return fmt.Errorf("index %q column %q should have ID %d, but found ID %d",
							index.Name, name, colID, index.ColumnIDs[i])
					}
					if _, ok := validateIndexDup[colID]; ok {
						return fmt.Errorf("index %q contains duplicate column %q", index.Name, name)
					}
					validateIndexDup[colID] = struct{}{}
				}
			}
			for i, name := range index.ColumnNames {
				if index.IsFunc[i] {
					expr, err := parser.ParseExpr(name)
					if err != nil {
						return err
					}
					funcexpr, ok := expr.(*tree.FuncExpr)
					if !ok {
						return nil
					}
					for _, exprs := range funcexpr.Exprs {
						sonexpr, exprok := exprs.(*tree.UnresolvedName)
						if exprok {
							for i := 0; i < sonexpr.NumParts; i++ {
								column := sonexpr.Parts[i]
								colID, ok := columnNames[column]
								if !ok {
									return fmt.Errorf("index %q contains unknown column %q", index.Name, name)
								}
								validateIndexDup[colID] = struct{}{}
							}
						}
					}
				}
			}
		} else {
			for i, name := range index.ColumnNames {
				colID, ok := columnNames[name]
				if !ok {
					return fmt.Errorf("index %q contains unknown column %q", index.Name, name)
				}
				if colID != index.ColumnIDs[i] {
					return fmt.Errorf("index %q column %q should have ID %d, but found ID %d",
						index.Name, name, colID, index.ColumnIDs[i])
				}
				if _, ok := validateIndexDup[colID]; ok {
					return fmt.Errorf("index %q contains duplicate column %q", index.Name, name)
				}
				validateIndexDup[colID] = struct{}{}
			}
		}
	}

	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		famID, ok := colIDToFamilyID[colID]
		if !ok || famID != FamilyID(0) {
			return fmt.Errorf("primary key column %d is not in column family 0", colID)
		}
	}

	return nil
}

// PrimaryKeyString returns the pretty-printed primary key declaration for a
// table descriptor.
func (desc *TableDescriptor) PrimaryKeyString() string {
	return fmt.Sprintf("PRIMARY KEY (%s)",
		desc.PrimaryIndex.ColNamesString(),
	)
}

// validatePartitioningDescriptor validates that a PartitioningDescriptor, which
// may represent a subpartition, is well-formed. Checks include validating the
// table-level uniqueness of all partition names, validating that the encoded
// tuples match the corresponding column types, and that range Partitions are
// stored sorted by upper bound. colOffset is non-zero for subpartitions and
// indicates how many index columns to skip over.
func (desc *TableDescriptor) validatePartitioningDescriptor(
	a *DatumAlloc,
	idxDesc *IndexDescriptor,
	partDesc *PartitioningDescriptor,
	colOffset int,
	partitionNames map[string]string,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// TODO(dan): The sqlccl.GenerateSubzoneSpans logic is easier if we disallow
	// setting zone configs on indexes that are interleaved into another index.
	// InterleavedBy is fine, so using the root of the interleave hierarchy will
	// work. It is expected that this is sufficient for real-world use cases.
	// Revisit this restriction if that expectation is wrong.
	if len(idxDesc.Interleave.Ancestors) > 0 {
		return errors.Errorf("cannot set a zone config for interleaved index %s; "+
			"set it on the root of the interleaved hierarchy instead", idxDesc.Name)
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we're
	// only using it to look for collisions and the prefix would be the same for
	// all of them. Faking them out with DNull allows us to make O(list partition)
	// calls to DecodePartitionTuple instead of O(list partition entry).
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	if len(partDesc.List) == 0 && len(partDesc.Range) == 0 {
		return fmt.Errorf("at least one of LIST or RANGE partitioning must be used")
	}
	if len(partDesc.List) > 0 && len(partDesc.Range) > 0 {
		return fmt.Errorf("only one LIST or RANGE partitioning may used")
	}

	checkName := func(name string) error {
		if len(name) == 0 {
			return fmt.Errorf("PARTITION name must be non-empty")
		}
		if indexName, exists := partitionNames[name]; exists {
			if indexName == idxDesc.Name {
				return fmt.Errorf("PARTITION %s: name must be unique (used twice in index %q)",
					name, indexName)
			}
			//return fmt.Errorf("PARTITION %s: name must be unique (used in both index %q and index %q)",
			//	name, indexName, idxDesc.Name)
		}
		partitionNames[name] = idxDesc.Name
		return nil
	}

	if len(partDesc.List) > 0 {
		listValues := make(map[string]struct{}, len(partDesc.List))
		for _, p := range partDesc.List {
			if err := checkName(p.Name); err != nil {
				return err
			}

			if len(p.Values) == 0 {
				return fmt.Errorf("PARTITION %s: must contain values", p.Name)
			}
			// NB: key encoding is used to check uniqueness because it has
			// to match the behavior of the value when indexed.
			for _, valueEncBuf := range p.Values {
				tuple, keyPrefix, err := DecodePartitionTuple(
					a, desc, idxDesc, partDesc, valueEncBuf, fakePrefixDatums)
				if err != nil {
					return fmt.Errorf("PARTITION %s: %v", p.Name, err)
				}
				if _, exists := listValues[string(keyPrefix)]; exists {
					return fmt.Errorf("%s cannot be present in more than one partition", tuple)
				}
				listValues[string(keyPrefix)] = struct{}{}
			}

			newColOffset := colOffset + int(partDesc.NumColumns)
			if err := desc.validatePartitioningDescriptor(
				a, idxDesc, &p.Subpartitioning, newColOffset, partitionNames,
			); err != nil {
				return err
			}
		}
	}

	if len(partDesc.Range) > 0 {
		tree := interval.NewTree(interval.ExclusiveOverlapper)
		for _, p := range partDesc.Range {
			if err := checkName(p.Name); err != nil {
				return err
			}

			// NB: key encoding is used to check uniqueness because it has to match
			// the behavior of the value when indexed.
			fromDatums, fromKey, err := DecodePartitionTuple(
				a, desc, idxDesc, partDesc, p.FromInclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			toDatums, toKey, err := DecodePartitionTuple(
				a, desc, idxDesc, partDesc, p.ToExclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			pi := partitionInterval{p.Name, fromKey, toKey}
			if overlaps := tree.Get(pi.Range()); len(overlaps) > 0 {
				return fmt.Errorf("partitions %s and %s overlap",
					overlaps[0].(partitionInterval).name, p.Name)
			}
			if err := tree.Insert(pi, false /* fast */); err == interval.ErrEmptyRange {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
					p.Name, fromDatums, toDatums)
			} else if err == interval.ErrInvertedRange {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
					p.Name, fromDatums, toDatums)
			} else if err != nil {
				return errors.Wrap(err, fmt.Sprintf("PARTITION %s", p.Name))
			}
		}
	}

	return nil
}

// FindNonDropPartitionByName returns the PartitionDescriptor and the
// IndexDescriptor that the partition with the specified name belongs to. If no
// such partition exists, an error is returned.
func (desc *TableDescriptor) FindNonDropPartitionByName(
	name string,
) (*PartitioningDescriptor, *IndexDescriptor, error) {
	var find func(p PartitioningDescriptor) *PartitioningDescriptor
	find = func(p PartitioningDescriptor) *PartitioningDescriptor {
		for _, l := range p.List {
			if l.Name == name {
				return &p
			}
			if s := find(l.Subpartitioning); s != nil {
				return s
			}
		}
		for _, r := range p.Range {
			if r.Name == name {
				return &p
			}
		}
		return nil
	}
	for _, idx := range desc.AllNonDropIndexes() {
		if p := find(idx.Partitioning); p != nil {
			return p, idx, nil
		}
	}
	return nil, nil, fmt.Errorf("partition %q does not exist", name)
}

// validatePartitioning validates that any PartitioningDescriptors contained in
// table indexes are well-formed. See validatePartitioningDesc for details.
func (desc *TableDescriptor) validatePartitioning() error {
	partitionNames := make(map[string]string)

	a := &DatumAlloc{}
	return desc.ForeachNonDropIndex(func(idxDesc *IndexDescriptor) error {
		return desc.validatePartitioningDescriptor(
			a, idxDesc, &idxDesc.Partitioning, 0 /* colOffset */, partitionNames,
		)
	})
}

// FamilyHeuristicTargetBytes is the target total byte size of columns that the
// current heuristic will assign to a family.
const FamilyHeuristicTargetBytes = 256

// fitColumnToFamily attempts to fit a new column into the existing column
// families. If the heuristics find a fit, true is returned along with the
// index of the selected family. Otherwise, false is returned and the column
// should be put in a new family.
//
// Current heuristics:
// - Put all columns in family 0.
func fitColumnToFamily(desc *MutableTableDescriptor, col ColumnDescriptor) (int, bool) {
	// Fewer column families means fewer kv entries, which is generally faster.
	// On the other hand, an update to any column in a family requires that they
	// all are read and rewritten, so large (or numerous) columns that are not
	// updated at the same time as other columns in the family make things
	// slower.
	//
	// The initial heuristic used for family assignment tried to pack
	// fixed-width columns into families up to a certain size and would put any
	// variable-width column into its own family. This was conservative to
	// guarantee that we avoid the worst-case behavior of a very large immutable
	// blob in the same family as frequently updated columns.
	//
	// However, our initial customers have revealed that this is backward.
	// Repeatedly, they have recreated existing schemas without any tuning and
	// found lackluster performance. Each of these has turned out better as a
	// single family (sometimes 100% faster or more), the most aggressive tuning
	// possible.
	//
	// Further, as the WideTable benchmark shows, even the worst-case isn't that
	// bad (33% slower with an immutable 1MB blob, which is the upper limit of
	// what we'd recommend for column size regardless of families). This
	// situation also appears less frequent than we feared.
	//
	// The result is that we put all columns in one family and require the user
	// to manually specify family assignments when this is incorrect.
	return 0, true
}

// columnTypeIsIndexable returns whether the type t is valid as an indexed column.
func columnTypeIsIndexable(t ColumnType) bool {
	return !MustBeValueEncoded(t.SemanticType)
}

// columnTypeIsInvertedIndexable returns whether the type t is valid to be indexed
// using an inverted index.
func columnTypeIsInvertedIndexable(t ColumnType) bool {
	return t.SemanticType == ColumnType_JSONB
}

func notIndexableError(cols []ColumnDescriptor, inverted bool) error {
	if len(cols) == 0 {
		return nil
	}
	var msg string
	var typInfo string
	if len(cols) == 1 {
		col := cols[0]
		msg = "column %s is of type %s and thus is not indexable"
		if inverted {
			msg += " with an inverted index"
		}
		typInfo = col.Type.String()
		msg = fmt.Sprintf(msg, col.Name, col.Type.SemanticType)
	} else {
		msg = "the following columns are not indexable due to their type: "
		for i, col := range cols {
			msg += fmt.Sprintf("%s (type %s)", col.Name, col.Type.SemanticType)
			typInfo += col.Type.String()
			if i != len(cols)-1 {
				msg += ", "
				typInfo += ","
			}
		}
	}
	return pgerror.UnimplementedWithIssueDetailErrorf(35730, typInfo, msg)
}

func checkColumnsValidForIndex(tableDesc *MutableTableDescriptor, indexColNames []string) error {
	invalidColumns := make([]ColumnDescriptor, 0, len(indexColNames))
	for _, indexCol := range indexColNames {
		for _, col := range tableDesc.AllNonDropColumns() {
			if col.Name == indexCol {
				if !columnTypeIsIndexable(col.Type) {
					invalidColumns = append(invalidColumns, col)
				}
			}
		}
	}
	if len(invalidColumns) > 0 {
		return notIndexableError(invalidColumns, false)
	}
	return nil
}

func checkColumnsValidForInvertedIndex(
	tableDesc *MutableTableDescriptor, indexColNames []string,
) error {
	if len((indexColNames)) > 1 {
		return errors.New("indexing more than one column with an inverted index is not supported")
	}
	invalidColumns := make([]ColumnDescriptor, 0, len(indexColNames))
	for _, indexCol := range indexColNames {
		for _, col := range tableDesc.AllNonDropColumns() {
			if col.Name == indexCol {
				if !columnTypeIsInvertedIndexable(col.Type) {
					invalidColumns = append(invalidColumns, col)
				}
			}
		}
	}
	if len(invalidColumns) > 0 {
		return notIndexableError(invalidColumns, true)
	}
	return nil
}

// AddColumn adds a column to the table.
func (desc *MutableTableDescriptor) AddColumn(col ColumnDescriptor) {
	desc.Columns = append(desc.Columns, col)
}

// AddFamily adds a family to the table.
func (desc *MutableTableDescriptor) AddFamily(fam ColumnFamilyDescriptor) {
	desc.Families = append(desc.Families, fam)
}

// AddIndex adds an index to the table.
func (desc *MutableTableDescriptor) AddIndex(idx IndexDescriptor, primary bool) error {
	if idx.Type == IndexDescriptor_FORWARD {
		if err := checkColumnsValidForIndex(desc, idx.ColumnNames); err != nil {
			return err
		}

		if primary {
			// PrimaryIndex is unset.
			if desc.PrimaryIndex.Name == "" {
				if idx.Name == "" {
					// Only override the index name if it hasn't been set by the user.
					idx.Name = PrimaryKeyIndexName
				}
				desc.PrimaryIndex = idx
			} else {
				return fmt.Errorf("multiple primary keys for table %q are not allowed", desc.Name)
			}
		} else {
			desc.Indexes = append(desc.Indexes, idx)
		}

	} else {
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
		desc.Indexes = append(desc.Indexes, idx)
	}

	return nil
}

// AddColumnToFamilyMaybeCreate adds the specified column to the specified
// family. If it doesn't exist and create is true, creates it. If it does exist
// adds it unless "strict" create (`true` for create but `false` for
// ifNotExists) is specified.
//
// AllocateIDs must be called before the TableDescriptor will be valid.
func (desc *MutableTableDescriptor) AddColumnToFamilyMaybeCreate(
	col string, family string, create bool, ifNotExists bool,
) error {
	idx := int(-1)
	if len(family) > 0 {
		for i := range desc.Families {
			if desc.Families[i].Name == family {
				idx = i
				break
			}
		}
	}

	if idx == -1 {
		if create {
			// NB: When AllocateIDs encounters an empty `Name`, it'll generate one.
			desc.AddFamily(ColumnFamilyDescriptor{Name: family, ColumnNames: []string{col}})
			return nil
		}
		return fmt.Errorf("unknown family %q", family)
	}

	if create && !ifNotExists {
		return fmt.Errorf("family %q already exists", family)
	}
	desc.Families[idx].ColumnNames = append(desc.Families[idx].ColumnNames, col)
	return nil
}

// RemoveColumnFromFamily removes a colID from the family it's assigned to.
func (desc *MutableTableDescriptor) RemoveColumnFromFamily(colID ColumnID) {
	for i, family := range desc.Families {
		for j, c := range family.ColumnIDs {
			if c == colID {
				desc.Families[i].ColumnIDs = append(
					desc.Families[i].ColumnIDs[:j], desc.Families[i].ColumnIDs[j+1:]...)
				desc.Families[i].ColumnNames = append(
					desc.Families[i].ColumnNames[:j], desc.Families[i].ColumnNames[j+1:]...)
				if len(desc.Families[i].ColumnIDs) == 0 {
					desc.Families = append(desc.Families[:i], desc.Families[i+1:]...)
				}
				return
			}
		}
	}
}

// RenameColumnDescriptor updates all references to a column name in
// a table descriptor including indexes and families.
func (desc *MutableTableDescriptor) RenameColumnDescriptor(
	column ColumnDescriptor, newColName string,
) {
	colID := column.ID
	column.Name = newColName
	desc.UpdateColumnDescriptor(column)
	for i := range desc.Families {
		for j := range desc.Families[i].ColumnIDs {
			if desc.Families[i].ColumnIDs[j] == colID {
				desc.Families[i].ColumnNames[j] = newColName
			}
		}
	}

	renameColumnInIndex := func(idx *IndexDescriptor) {
		for i, id := range idx.ColumnIDs {
			if id == colID {
				idx.ColumnNames[i] = newColName
			}
		}
		for i, id := range idx.StoreColumnIDs {
			if id == colID {
				idx.StoreColumnNames[i] = newColName
			}
		}
	}
	renameColumnInIndex(&desc.PrimaryIndex)
	for i := range desc.Indexes {
		renameColumnInIndex(&desc.Indexes[i])
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			renameColumnInIndex(idx)
		}
	}
}

// FindActiveColumnsByNames finds all requested columns (in the requested order)
// or returns an error.
func (desc *TableDescriptor) FindActiveColumnsByNames(
	names tree.NameList,
) ([]ColumnDescriptor, error) {
	cols := make([]ColumnDescriptor, len(names))
	for i := range names {
		c, err := desc.FindActiveColumnByName(string(names[i]))
		if err != nil {
			return nil, err
		}
		cols[i] = c
	}
	return cols, nil
}

// FindColumnByName finds the column with the specified name. It returns
// an active column or a column from the mutation list. It returns true
// if the column is being dropped.
func (desc *TableDescriptor) FindColumnByName(name tree.Name) (ColumnDescriptor, bool, error) {
	for i, c := range desc.Columns {
		if c.Name == string(name) {
			return desc.Columns[i], false, nil
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			if c.Name == string(name) {
				return *c, m.Direction == DescriptorMutation_DROP, nil
			}
		}
	}
	return ColumnDescriptor{}, false, NewUndefinedColumnError(string(name))
}

// ColumnIdxMap returns a map from Column ID to the ordinal position of that
// column.
func (desc *TableDescriptor) ColumnIdxMap() map[ColumnID]int {
	return desc.ColumnIdxMapWithMutations(false)
}

// ColumnIdxMapWithMutations returns a map from Column ID to the ordinal
// position of that column, optionally including mutation columns if the input
// bool is true.
func (desc *TableDescriptor) ColumnIdxMapWithMutations(mutations bool) map[ColumnID]int {
	colIdxMap := make(map[ColumnID]int, len(desc.Columns))
	for i, c := range desc.Columns {
		colIdxMap[c.ID] = i
	}
	if mutations {
		idx := len(desc.Columns)
		for i := range desc.Mutations {
			col := desc.Mutations[i].GetColumn()
			if col != nil {
				colIdxMap[col.ID] = idx
				idx++
			}
		}
	}
	return colIdxMap
}

// UpdateColumnDescriptor updates an existing column descriptor.
func (desc *MutableTableDescriptor) UpdateColumnDescriptor(column ColumnDescriptor) {
	for i := range desc.Columns {
		if desc.Columns[i].ID == column.ID {
			desc.Columns[i] = column
			return
		}
	}
	for i, m := range desc.Mutations {
		if col := m.GetColumn(); col != nil && col.ID == column.ID {
			desc.Mutations[i].Descriptor_ = &DescriptorMutation_Column{Column: &column}
			return
		}
	}

	panic(NewUndefinedColumnError(column.Name))
}

// UpdateColumnDefaultExprByName updates an existing column descriptor.
func (desc *MutableTableDescriptor) UpdateColumnDefaultExprByName(column ColumnDescriptor) {
	for i := range desc.Columns {
		if desc.Columns[i].Name == column.Name {
			desc.Columns[i].DefaultExpr = column.DefaultExpr
			if len(column.UsesSequenceIds) != 0 {
				desc.Columns[i].UsesSequenceIds = column.UsesSequenceIds
			}
			if !column.Nullable {
				desc.Columns[i].Nullable = false
			}
			return
		}
	}

	panic(NewUndefinedColumnError(column.Name))
}

// FindActiveColumnByName finds an active column with the specified name.
func (desc *TableDescriptor) FindActiveColumnByName(name string) (ColumnDescriptor, error) {
	for _, c := range desc.Columns {
		if c.Name == name {
			return c, nil
		}
	}
	return ColumnDescriptor{}, NewUndefinedColumnError(name)
}

// FindColumnByID finds the column with specified ID.
func (desc *TableDescriptor) FindColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			if c.ID == id {
				return c, nil
			}
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindActiveColumnByID finds the active column with specified ID.
func (desc *TableDescriptor) FindActiveColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindReadableColumnByID finds the readable column with specified ID. The
// column may be undergoing a schema change and is marked nullable regardless
// of its configuration. It returns true if the column is undergoing a
// schema change.
func (desc *ImmutableTableDescriptor) FindReadableColumnByID(
	id ColumnID,
) (*ColumnDescriptor, bool, error) {
	for i, c := range desc.ReadableColumns {
		if c.ID == id {
			return &desc.ReadableColumns[i], i >= len(desc.Columns), nil
		}
	}
	return nil, false, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// IsFunction will return true, if this desc is actually a real function not a table.
func (desc *ImmutableTableDescriptor) IsFunction() bool {
	if desc.FunctionDesc != nil && desc.ID == InvalidID {
		return true
	}
	return false
}

// FindFamilyByID finds the family with specified ID.
func (desc *TableDescriptor) FindFamilyByID(id FamilyID) (*ColumnFamilyDescriptor, error) {
	for i, f := range desc.Families {
		if f.ID == id {
			return &desc.Families[i], nil
		}
	}
	return nil, fmt.Errorf("family-id \"%d\" does not exist", id)
}

// FindIndexByName finds the index with the specified name in the active
// list or the mutations list. It returns true if the index is being dropped.
func (desc *TableDescriptor) FindIndexByName(name string) (*IndexDescriptor, bool, error) {
	if desc.IsPhysicalTable() && desc.PrimaryIndex.Name == name {
		return &desc.PrimaryIndex, false, nil
	}
	for i, idx := range desc.Indexes {
		if idx.Name == name {
			return &desc.Indexes[i], false, nil
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			if idx.Name == name {
				return idx, m.Direction == DescriptorMutation_DROP, nil
			}
		}
	}
	return nil, false, fmt.Errorf("index %q does not exist", name)
}

// FindConstraintByName return constraint by name
func (desc *TableDescriptor) FindConstraintByName(
	ctx context.Context, txn *client.Txn, name string,
) (*ConstraintDetail, bool, error) {
	cstmap, err := desc.GetConstraintInfo(ctx, txn)
	if err != nil {
		return nil, false, err
	}

	cst, ok := cstmap[name]
	if ok {
		return &cst, false, nil
	}
	for _, m := range desc.Mutations {
		if cs := m.GetConstraint(); cs != nil {
			if cs.Name == name {
				return &cst, m.Direction == DescriptorMutation_DROP, nil
			}
		}
	}

	return nil, false, fmt.Errorf("constraint %q does not exist", name)
}

// FindCheckByName finds the check constraint with the specified name.
func (desc *TableDescriptor) FindCheckByName(
	name string,
) (*TableDescriptor_CheckConstraint, error) {
	for _, c := range desc.Checks {
		if c.Name == name {
			return c, nil
		}
	}
	return nil, fmt.Errorf("check %q does not exist", name)
}

// NumFamilies returns the number of column families in the descriptor.
func (desc *TableDescriptor) NumFamilies() int {
	return len(desc.Families)
}

// ColumnsWithMutations returns all column descriptors, optionally including
// mutation columns.
func (desc *TableDescriptor) ColumnsWithMutations(includeMutations bool) []ColumnDescriptor {
	n := len(desc.Columns)
	columns := desc.Columns[:n:n] // immutable on append
	if includeMutations {
		for i := range desc.Mutations {
			if col := desc.Mutations[i].GetColumn(); col != nil {
				columns = append(columns, *col)
			}
		}
	}
	return columns
}

// GetPrimaryIndexID returns the ID of the primary index.
func (desc *TableDescriptor) GetPrimaryIndexID() IndexID {
	return desc.PrimaryIndex.ID
}

// ForeachFamily calls f for every column family key in desc until an
// error is returned.
func (desc *TableDescriptor) ForeachFamily(f func(family *ColumnFamilyDescriptor) error) error {
	for i := range desc.Families {
		if err := f(&desc.Families[i]); err != nil {
			return err
		}
	}
	return nil
}

// RenameIndexDescriptor renames an index descriptor.
func (desc *MutableTableDescriptor) RenameIndexDescriptor(
	index *IndexDescriptor, name string,
) error {
	id := index.ID
	if id == desc.PrimaryIndex.ID {
		desc.PrimaryIndex.Name = name
		return nil
	}
	for i := range desc.Indexes {
		if desc.Indexes[i].ID == id {
			desc.Indexes[i].Name = name
			return nil
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && idx.ID == id {
			idx.Name = name
			return nil
		}
	}
	return fmt.Errorf("index with id = %d does not exist", id)
}

// DropConstraint drops a constraint.
func (desc *MutableTableDescriptor) DropConstraint(
	name string,
	detail ConstraintDetail,
	removeFK func(*MutableTableDescriptor, *IndexDescriptor) error,
) error {
	switch detail.Kind {
	case ConstraintTypePK:
		desc.PrimaryIndex.Disabled = true
		return nil

	case ConstraintTypeUnique:
		return pgerror.Unimplemented("drop-constraint-unique",
			"cannot drop UNIQUE constraint %q using ALTER TABLE DROP CONSTRAINT, use DROP INDEX CASCADE instead",
			tree.ErrNameStringP(&detail.Index.Name))

	case ConstraintTypeCheck:
		if !(detail.CheckConstraint.InhCount == 1 && !detail.CheckConstraint.IsInherits) {
			return fmt.Errorf("inherit check constraint can not be droped")
		}
		if detail.CheckConstraint.Validity == ConstraintValidity_Validating {
			return pgerror.Unimplemented("rename-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.CheckConstraint.Name))
		}
		for i, c := range desc.Checks {
			if c.Name == name {
				desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
				break
			}
		}
		return nil

	case ConstraintTypeFK:
		idx, err := desc.FindIndexByID(detail.Index.ID)
		if err != nil {
			return err
		}
		if err := removeFK(desc, idx); err != nil {
			return err
		}
		idx.ForeignKey = ForeignKeyReference{}
		return nil

	default:
		return pgerror.Unimplemented(fmt.Sprintf("drop-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(name))
	}

}

// AbleConstraint ENABLE/DISABLE a check constraint
func (desc *MutableTableDescriptor) AbleConstraint(
	name string, detail ConstraintDetail, Able bool,
) error {
	switch detail.Kind {
	case ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == ConstraintValidity_Validating {
			return pgerror.Unimplemented("rename-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.CheckConstraint.Name))
		}
		for i, c := range desc.Checks {
			if c.Name == name {
				// desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
				desc.Checks[i].Able = Able
				break
			}
		}
		return nil

	default:
		return pgerror.Unimplemented(fmt.Sprintf("drop-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(name))
	}
}

// RenameConstraint renames a constraint.
func (desc *MutableTableDescriptor) RenameConstraint(
	detail ConstraintDetail, oldName, newName string, dependentViewRenameError func(string, ID) error,
) error {
	switch detail.Kind {
	case ConstraintTypePK, ConstraintTypeUnique:
		for _, tableRef := range desc.DependedOnBy {
			if tableRef.IndexID != detail.Index.ID {
				continue
			}
			return dependentViewRenameError("index", tableRef.ID)
		}
		return desc.RenameIndexDescriptor(detail.Index, newName)

	case ConstraintTypeFK:
		idx, err := desc.FindIndexByID(detail.Index.ID)
		if err != nil {
			return err
		}
		if !idx.ForeignKey.IsSet() || idx.ForeignKey.Name != oldName {
			return pgerror.NewAssertionErrorf("constraint %q not found",
				tree.ErrNameString(newName))
		}
		idx.ForeignKey.Name = newName
		return nil

	case ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == ConstraintValidity_Validating {
			return pgerror.Unimplemented("rename-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.CheckConstraint.Name))
		}
		if !(detail.CheckConstraint.InhCount == 1 && !detail.CheckConstraint.IsInherits) {
			return fmt.Errorf("inherited check constraint can not be rename")
		}
		detail.CheckConstraint.Name = newName
		return nil

	default:
		return pgerror.Unimplemented(fmt.Sprintf("rename-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(oldName))
	}
}

// FindIndexByID finds an index (active or inactive) with the specified ID.
// Must return a pointer to the IndexDescriptor in the TableDescriptor, so that
// callers can use returned values to modify the TableDesc.
func (desc *TableDescriptor) FindIndexByID(id IndexID) (*IndexDescriptor, error) {
	if desc.PrimaryIndex.ID == id {
		return &desc.PrimaryIndex, nil
	}
	for i, c := range desc.Indexes {
		if c.ID == id {
			return &desc.Indexes[i], nil
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && idx.ID == id {
			return idx, nil
		}
	}
	for _, m := range desc.GCMutations {
		if m.IndexID == id {
			return nil, ErrIndexGCMutationsList
		}
	}
	return nil, fmt.Errorf("index-id \"%d\" does not exist", id)
}

// FindIndexByIndexIdx returns an active index with the specified
// index's index which has a domain of [0, # of secondary indexes] and whether
// the index is a secondary index.
// The primary index has an index of 0 and the first secondary index (if it exists)
// has an index of 1.
func (desc *TableDescriptor) FindIndexByIndexIdx(
	indexIdx int,
) (index *IndexDescriptor, isSecondary bool, err error) {
	// indexIdx is 0 for the primary index, or 1 to <num-indexes> for a
	// secondary index.
	if indexIdx < 0 || indexIdx > len(desc.Indexes) {
		return nil, false, errors.Errorf("invalid indexIdx %d", indexIdx)
	}

	if indexIdx > 0 {
		return &desc.Indexes[indexIdx-1], true, nil
	}

	return &desc.PrimaryIndex, false, nil
}

// GetIndexMutationCapabilities returns:
// 1. Whether the index is a mutation
// 2. if so, is it in state DELETE_AND_WRITE_ONLY
func (desc *TableDescriptor) GetIndexMutationCapabilities(id IndexID) (bool, bool) {
	for _, mutation := range desc.Mutations {
		if mutationIndex := mutation.GetIndex(); mutationIndex != nil {
			if mutationIndex.ID == id {
				return true,
					mutation.State == DescriptorMutation_DELETE_AND_WRITE_ONLY
			}
		}
	}
	return false, false
}

// IsInterleaved returns true if any part of this this table is interleaved with
// another table's data.
func (desc *TableDescriptor) IsInterleaved() bool {
	for _, index := range desc.AllNonDropIndexes() {
		if index.IsInterleaved() {
			return true
		}
	}
	return false
}

// IsPrimaryIndexDefaultRowID returns whether or not the table's primary
// index is the default primary key on the hidden rowid column.
func (desc *TableDescriptor) IsPrimaryIndexDefaultRowID() bool {
	if len(desc.PrimaryIndex.ColumnIDs) != 1 {
		return false
	}
	col, err := desc.FindColumnByID(desc.PrimaryIndex.ColumnIDs[0])
	if err != nil {
		// Should never be in this case.
		panic(err)
	}
	return col.Hidden
}

// MakeMutationComplete updates the descriptor upon completion of a mutation.
func (desc *MutableTableDescriptor) MakeMutationComplete(m DescriptorMutation) error {
	switch m.Direction {
	case DescriptorMutation_ADD:
		switch t := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			desc.AddColumn(*t.Column)

		case *DescriptorMutation_Index:
			if err := desc.AddIndex(*t.Index, false); err != nil {
				return err
			}

		case *DescriptorMutation_Constraint:
			switch t.Constraint.ConstraintType {
			case ConstraintToUpdate_CHECK:
				for _, c := range desc.Checks {
					if c.Name == t.Constraint.Name {
						c.Validity = ConstraintValidity_Validated
						break
					}
				}
			default:
				return errors.Errorf("unsupported constraint type: %d", t.Constraint.ConstraintType)
			}
		case *DescriptorMutation_PrimaryKeySwap:
			args := t.PrimaryKeySwap
			getIndexIdxByID := func(id IndexID) (int, error) {
				for i, idx := range desc.Indexes {
					if idx.ID == id {
						return i, nil
					}
				}
				return 0, errors.New("it may be executing concurrently, and the primary index was not in list of indexes")
			}

			// Actually write the new primary index into the descriptor, and remove it from the indexes list.
			// Additionally, schedule the old primary index for deletion. Note that if needed, a copy of the primary index
			// that doesn't store any columns was created at the beginning of the primary key change operation.
			primaryIndexCopy := protoutil.Clone(&desc.PrimaryIndex).(*IndexDescriptor)
			primaryIndexCopy.EncodingType = PrimaryIndexEncoding
			for _, col := range desc.Columns {
				containsCol := false
				for _, colID := range primaryIndexCopy.ColumnIDs {
					if colID == col.ID {
						containsCol = true
						break
					}
				}
				if !containsCol {
					primaryIndexCopy.StoreColumnIDs = append(primaryIndexCopy.StoreColumnIDs, col.ID)
					primaryIndexCopy.StoreColumnNames = append(primaryIndexCopy.StoreColumnNames, col.Name)
				}
			}
			if err := desc.AddIndexMutation(primaryIndexCopy, DescriptorMutation_DROP); err != nil {
				return err
			}
			newIndex, err := desc.FindIndexByID(args.NewPrimaryIndexId)
			if err != nil {
				return err
			}
			if args.NewPrimaryIndexName == "" {
				newIndex.Name = PrimaryKeyIndexName
			} else {
				newIndex.Name = args.NewPrimaryIndexName
			}
			newIndex.ExtraColumnIDs = nil
			desc.PrimaryIndex = *protoutil.Clone(newIndex).(*IndexDescriptor)
			// The primary index implicitly stores all columns in the table.
			// Explicitly including them in the stored columns list is incorrect.
			desc.PrimaryIndex.StoreColumnNames, desc.PrimaryIndex.StoreColumnIDs = nil, nil
			idx, err := getIndexIdxByID(newIndex.ID)
			if err != nil {
				return err
			}
			desc.Indexes = append(desc.Indexes[:idx], desc.Indexes[idx+1:]...)

			// Swap out the old indexes with their rewritten versions.
			for j := range args.OldIndexes {
				oldID := args.OldIndexes[j]
				newID := args.NewIndexes[j]
				// All our new indexes have been inserted into the table descriptor by now, since the primary key swap
				// is the last mutation processed in a group of mutations under the same mutation ID.
				newIndex, err := desc.FindIndexByID(newID)
				if err != nil {
					return err
				}
				oldIndexIndex, err := getIndexIdxByID(oldID)
				if err != nil {
					return err
				}
				oldIndex := protoutil.Clone(&desc.Indexes[oldIndexIndex]).(*IndexDescriptor)
				newIndex.Name = oldIndex.Name
				// Splice out old index from the indexes list.
				desc.Indexes = append(desc.Indexes[:oldIndexIndex], desc.Indexes[oldIndexIndex+1:]...)
				// Add a drop mutation for the old index. The code that calls this function will schedule
				// a schema change job to pick up all of these index drop mutations.
				if err := desc.AddIndexMutation(oldIndex, DescriptorMutation_DROP); err != nil {
					return err
				}
			}
		case *DescriptorMutation_MaterializedViewRefresh:
			// Completing a refresh mutation just means overwriting the table's
			// indexes with the new indexes that have been backfilled already.
			desc.PrimaryIndex = t.MaterializedViewRefresh.NewPrimaryIndex
			desc.Indexes = t.MaterializedViewRefresh.NewIndexes
		}

	case DescriptorMutation_DROP:
		switch t := m.Descriptor_.(type) {

		// Nothing else to be done. The column/index was already removed from the
		// set of column/index descriptors at mutation creation time.
		// Constraints to be dropped are dropped before column/index backfills.
		case *DescriptorMutation_Column:
			desc.RemoveColumnFromFamily(t.Column.ID)
		}
	}
	return nil
}

// AddCheckValidationMutation adds a check constraint validation mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddCheckValidationMutation(
	ck *TableDescriptor_CheckConstraint,
) {
	m := DescriptorMutation{
		Descriptor_: &DescriptorMutation_Constraint{
			Constraint: &ConstraintToUpdate{
				ConstraintType: ConstraintToUpdate_CHECK, Name: ck.Name, Check: *ck,
			},
		},
		Direction: DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

// AddColumnMutation adds a column mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddColumnMutation(
	c ColumnDescriptor, direction DescriptorMutation_Direction,
) {
	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Column{Column: &c}, Direction: direction}
	desc.addMutation(m)
}

// AddIndexMutation adds an index mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddIndexMutation(
	idx *IndexDescriptor, direction DescriptorMutation_Direction,
) error {

	switch idx.Type {
	case IndexDescriptor_FORWARD:
		if err := checkColumnsValidForIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	case IndexDescriptor_INVERTED:
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	}

	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Index{Index: idx}, Direction: direction}
	desc.addMutation(m)
	return nil
}

// AddMaterializedViewRefreshMutation adds a MaterializedViewRefreshMutation to
// the table descriptor.
func (desc *MutableTableDescriptor) AddMaterializedViewRefreshMutation(
	refresh *MaterializedViewRefresh,
) {
	m := DescriptorMutation{
		Descriptor_: &DescriptorMutation_MaterializedViewRefresh{MaterializedViewRefresh: refresh},
		Direction:   DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

func (desc *MutableTableDescriptor) addMutation(m DescriptorMutation) {
	switch m.Direction {
	case DescriptorMutation_ADD:
		m.State = DescriptorMutation_DELETE_ONLY

	case DescriptorMutation_DROP:
		m.State = DescriptorMutation_DELETE_AND_WRITE_ONLY
	}
	// For tables created in the same transaction the next mutation ID will
	// not have been allocated and the added mutation will use an invalid ID.
	// This is fine because the mutation will be processed immediately.
	m.MutationID = desc.ClusterVersion.NextMutationID
	desc.NextMutationID = desc.ClusterVersion.NextMutationID + 1
	desc.Mutations = append(desc.Mutations, m)
}

// MakeFirstMutationPublic creates a MutableTableDescriptor from the
// ImmutableTableDescriptor by making the first mutation public.
// This is super valuable when trying to run SQL over data associated
// with a schema mutation that is still not yet public: Data validation,
// error reporting.
func (desc *ImmutableTableDescriptor) MakeFirstMutationPublic() (*MutableTableDescriptor, error) {
	// Clone the ImmutableTable descriptor because we want to create an Immutable one.
	table := NewMutableExistingTableDescriptor(*protoutil.Clone(desc.TableDesc()).(*TableDescriptor))
	mutationID := desc.Mutations[0].MutationID
	i := 0
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		if err := table.MakeMutationComplete(mutation); err != nil {
			return nil, err
		}
		i++
	}
	table.Mutations = table.Mutations[i:]
	table.Version++
	return table, nil
}

// ColumnNeedsBackfill returns true if adding the given column requires a
// backfill (dropping a column always requires a backfill).
func ColumnNeedsBackfill(desc *ColumnDescriptor) bool {
	return desc.DefaultExpr != nil || !desc.Nullable || desc.IsComputed()
}

// HasPrimaryKey returns true if the table has a primary key.
func (desc *TableDescriptor) HasPrimaryKey() bool {
	return !desc.PrimaryIndex.Disabled
}

// HasColumnBackfillMutation returns whether the table has any queued column
// mutations that require a backfill.
func (desc *TableDescriptor) HasColumnBackfillMutation() bool {
	for _, m := range desc.Mutations {
		col := m.GetColumn()
		if col == nil {
			// Index backfills don't affect changefeeds.
			continue
		}
		// It's unfortunate that there's no one method we can call to check if a
		// mutation will be a backfill or not, but this logic was extracted from
		// backfill.go.
		if m.Direction == DescriptorMutation_DROP || ColumnNeedsBackfill(col) {
			return true
		}
	}
	return false
}

// Dropped returns true if the table is being dropped.
func (desc *TableDescriptor) Dropped() bool {
	return desc.State == TableDescriptor_DROP
}

// Adding returns true if the table is being added.
func (desc *TableDescriptor) Adding() bool {
	return desc.State == TableDescriptor_ADD
}

// Dropped returns true if the table is being dropped.
func (desc *FunctionDescriptor) Dropped() bool {
	return desc.State == FunctionDescriptor_DROP
}

// Adding returns true if the table is being added.
func (desc *FunctionDescriptor) Adding() bool {
	return desc.State == FunctionDescriptor_ADD
}

// HasDrainingNames returns true if a draining name exists.
func (desc *FunctionDescriptor) HasDrainingNames() bool {
	return len(desc.DrainingNames) > 0
}

// IsNewTable returns true if the table was created in the current
// transaction.
func (desc *MutableTableDescriptor) IsNewTable() bool {
	return desc.ClusterVersion.ID == InvalidID
}

// IsNewFunction returns true if the function was created in the current
// transaction.
func (desc *MutableFunctionDescriptor) IsNewFunction() bool {
	return desc.ClusterVersion.ID == InvalidID
}

// HasDrainingNames returns true if a draining name exists.
func (desc *TableDescriptor) HasDrainingNames() bool {
	return len(desc.DrainingNames) > 0
}

// VisibleColumns returns all non hidden columns.
func (desc *TableDescriptor) VisibleColumns() []ColumnDescriptor {
	var cols []ColumnDescriptor
	for _, col := range desc.Columns {
		if (desc.IsHashPartition && col.Name == "hashnum") || !col.Hidden {
			cols = append(cols, col)
		}
	}
	return cols
}

// ColumnTypes returns the types of all columns.
func (desc *TableDescriptor) ColumnTypes() []ColumnType {
	return desc.ColumnTypesWithMutations(false)
}

// ColumnTypesWithMutations returns the types of all columns, optionally
// including mutation columns, which will be returned if the input bool is true.
func (desc *TableDescriptor) ColumnTypesWithMutations(mutations bool) []ColumnType {
	nCols := len(desc.Columns)
	if mutations {
		nCols += len(desc.Mutations)
	}
	types := make([]ColumnType, 0, nCols)
	for i := range desc.Columns {
		types = append(types, desc.Columns[i].Type)
	}
	if mutations {
		for i := range desc.Mutations {
			if col := desc.Mutations[i].GetColumn(); col != nil {
				types = append(types, col.Type)
			}
		}
	}
	return types
}

// ColumnsSelectors generates Select expressions for cols.
func ColumnsSelectors(cols []ColumnDescriptor, forUpdateOrDelete bool) tree.SelectExprs {
	exprs := make(tree.SelectExprs, len(cols))
	colItems := make([]tree.ColumnItem, len(cols))
	for i, col := range cols {
		colItems[i].ColumnName = tree.Name(col.Name)
		colItems[i].ForUpdateOrDelete = forUpdateOrDelete
		exprs[i].Expr = &colItems[i]
	}
	return exprs
}

// SetID implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) SetID(id ID) {
	desc.ID = id
}

// AddPrimaryKeySwapMutation adds a PrimaryKeySwap mutation to the table descriptor.
func (desc *MutableTableDescriptor) AddPrimaryKeySwapMutation(swap *PrimaryKeySwap) {
	m := DescriptorMutation{
		Descriptor_: &DescriptorMutation_PrimaryKeySwap{PrimaryKeySwap: swap},
		Direction:   DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

// TypeName returns the plain type of this descriptor.
func (desc *DatabaseDescriptor) TypeName() string {
	return "database"
}

// SetName implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) SetName(name string) {
	desc.Name = name
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *DatabaseDescriptor) Validate() error {
	if err := validateName(desc.Name, "descriptor"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid database ID %d", desc.ID)
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	desc.Privileges.MaybeFixPrivileges(desc.GetID(), privilege.DatabasePrivileges)

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID(), privilege.DatabasePrivileges)
}

// GetID returns the ID of the descriptor.
func (desc *Descriptor) GetID() ID {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		return t.Table.ID
	case *Descriptor_Schema:
		return t.Schema.ID
	case *Descriptor_Database:
		return t.Database.ID
	case *Descriptor_Function:
		return t.Function.ID
	default:
		return 0
	}
}

// GetName returns the Name of the descriptor.
func (desc *Descriptor) GetName() string {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		return t.Table.Name
	case *Descriptor_Schema:
		return t.Schema.Name
	case *Descriptor_Database:
		return t.Database.Name
	case *Descriptor_Function:
		return t.Function.Name
	default:
		return ""
	}
}

// IsSet returns whether or not the foreign key actually references a table.
func (f ForeignKeyReference) IsSet() bool {
	return f.Table != 0
}

// InvalidateFKConstraints sets all FK constraints to un-validated.
func (desc *TableDescriptor) InvalidateFKConstraints() {
	// We don't use GetConstraintInfo because we want to edit the passed desc.
	if desc.PrimaryIndex.ForeignKey.IsSet() {
		desc.PrimaryIndex.ForeignKey.Validity = ConstraintValidity_Unvalidated
	}
	for i := range desc.Indexes {
		if desc.Indexes[i].ForeignKey.IsSet() {
			desc.Indexes[i].ForeignKey.Validity = ConstraintValidity_Unvalidated
		}
	}
}

// AllIndexSpans returns the Spans for each index in the table, including those
// being added in the mutations.
func (desc *TableDescriptor) AllIndexSpans() roachpb.Spans {
	var spans roachpb.Spans
	err := desc.ForeachNonDropIndex(func(index *IndexDescriptor) error {
		spans = append(spans, desc.IndexSpan(index.ID))
		return nil
	})
	if err != nil {
		panic(err)
	}
	return spans
}

// PrimaryIndexSpan returns the Span that corresponds to the entire primary
// index; can be used for a full table scan.
func (desc *TableDescriptor) PrimaryIndexSpan() roachpb.Span {
	return desc.IndexSpan(desc.PrimaryIndex.ID)
}

// IndexSpan returns the Span that corresponds to an entire index; can be used
// for a full index scan.
func (desc *TableDescriptor) IndexSpan(indexID IndexID) roachpb.Span {
	prefix := roachpb.Key(MakeIndexKeyPrefix(desc, indexID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// TableSpan returns the Span that corresponds to the entire table.
func (desc *TableDescriptor) TableSpan() roachpb.Span {
	prefix := roachpb.Key(keys.MakeTablePrefix(uint32(desc.ID)))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// GetDescMetadataKey returns the descriptor key for the table.
func (desc TableDescriptor) GetDescMetadataKey() roachpb.Key {
	return MakeDescMetadataKey(desc.ID)
}

// GetNameMetadataKey returns the namespace key for the table.
func (desc TableDescriptor) GetNameMetadataKey() roachpb.Key {
	return MakeNameMetadataKey(desc.ParentID, desc.Name)
}

// SQLString returns the SQL statement describing the column.
func (desc *ColumnDescriptor) SQLString() string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.FormatNameP(&desc.Name)
	f.WriteByte(' ')
	f.WriteString(desc.Type.SQLString())
	if desc.Nullable {
		f.WriteString(" NULL")
	} else {
		f.WriteString(" NOT NULL")
	}
	if desc.DefaultExpr != nil {
		f.WriteString(" DEFAULT ")
		f.WriteString(*desc.DefaultExpr)
	}
	if desc.IsComputed() {
		f.WriteString(" AS (")
		f.WriteString(*desc.ComputeExpr)
		f.WriteString(") STORED")
	}
	return f.CloseAndGetString()
}

// ColumnsUsed returns the IDs of the columns used in the check constraint's
// expression. v2.0 binaries will populate this during table creation, but older
// binaries will not, in which case this needs to be computed when requested.
//
// TODO(nvanbenschoten): we can remove this in v2.1 and replace it with a sql
// migration to backfill all TableDescriptor_CheckConstraint.ColumnIDs slices.
// See #22322.
func (cc *TableDescriptor_CheckConstraint) ColumnsUsed(desc *TableDescriptor) ([]ColumnID, error) {
	if len(cc.ColumnIDs) > 0 {
		// Already populated.
		return cc.ColumnIDs, nil
	}

	parsed, err := parser.ParseExpr(cc.Expr)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse check constraint %s", cc.Expr)
	}

	colIDsUsed := make(map[ColumnID]struct{})
	visitFn := func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return err, false, nil
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				col, dropped, err := desc.FindColumnByName(c.ColumnName)
				if err != nil || dropped {
					return errors.Errorf("column %q not found for constraint %q",
						c.ColumnName, parsed.String()), false, nil
				}
				colIDsUsed[col.ID] = struct{}{}
			}
			return nil, false, v
		}
		return nil, true, expr
	}
	if _, err := tree.SimpleVisit(parsed, visitFn); err != nil {
		return nil, err
	}

	cc.ColumnIDs = make([]ColumnID, 0, len(colIDsUsed))
	for colID := range colIDsUsed {
		cc.ColumnIDs = append(cc.ColumnIDs, colID)
	}
	sort.Sort(ColumnIDs(cc.ColumnIDs))
	return cc.ColumnIDs, nil
}

// UsesColumn returns whether the check constraint uses the specified column.
func (cc *TableDescriptor_CheckConstraint) UsesColumn(
	desc *TableDescriptor, colID ColumnID,
) (bool, error) {
	colsUsed, err := cc.ColumnsUsed(desc)
	if err != nil {
		return false, err
	}
	i := sort.Search(len(colsUsed), func(i int) bool {
		return colsUsed[i] >= colID
	})
	return i < len(colsUsed) && colsUsed[i] == colID, nil
}

// CompositeKeyMatchMethodValue allows the conversion from a
// tree.ReferenceCompositeKeyMatchMethod to a ForeignKeyReference_Match.
var CompositeKeyMatchMethodValue = [...]ForeignKeyReference_Match{
	tree.MatchSimple:  ForeignKeyReference_SIMPLE,
	tree.MatchFull:    ForeignKeyReference_FULL,
	tree.MatchPartial: ForeignKeyReference_PARTIAL,
}

// ForeignKeyReferenceMatchValue allows the conversion from a
// ForeignKeyReference_Match to a tree.ReferenceCompositeKeyMatchMethod.
// This should match CompositeKeyMatchMethodValue.
var ForeignKeyReferenceMatchValue = [...]tree.CompositeKeyMatchMethod{
	ForeignKeyReference_SIMPLE:  tree.MatchSimple,
	ForeignKeyReference_FULL:    tree.MatchFull,
	ForeignKeyReference_PARTIAL: tree.MatchPartial,
}

// String implements the fmt.Stringer interface.
func (x ForeignKeyReference_Match) String() string {
	switch x {
	case ForeignKeyReference_SIMPLE:
		return "MATCH SIMPLE"
	case ForeignKeyReference_FULL:
		return "MATCH FULL"
	case ForeignKeyReference_PARTIAL:
		return "MATCH PARTIAL"
	default:
		return strconv.Itoa(int(x))
	}
}

// ForeignKeyReferenceActionValue allows the conversion between a
// tree.ReferenceAction and a ForeignKeyReference_Action.
var ForeignKeyReferenceActionValue = [...]ForeignKeyReference_Action{
	tree.NoAction:   ForeignKeyReference_NO_ACTION,
	tree.Restrict:   ForeignKeyReference_RESTRICT,
	tree.SetDefault: ForeignKeyReference_SET_DEFAULT,
	tree.SetNull:    ForeignKeyReference_SET_NULL,
	tree.Cascade:    ForeignKeyReference_CASCADE,
}

// String implements the fmt.Stringer interface.
func (x ForeignKeyReference_Action) String() string {
	switch x {
	case ForeignKeyReference_RESTRICT:
		return "RESTRICT"
	case ForeignKeyReference_SET_DEFAULT:
		return "SET DEFAULT"
	case ForeignKeyReference_SET_NULL:
		return "SET NULL"
	case ForeignKeyReference_CASCADE:
		return "CASCADE"
	default:
		return strconv.Itoa(int(x))
	}
}

var _ cat.Column = &ColumnDescriptor{}

// IsNullable is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsNullable() bool {
	return desc.Nullable
}

// ColID is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColID() cat.StableID {
	return cat.StableID(desc.ID)
}

// ColName is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColName() tree.Name {
	return tree.Name(desc.Name)
}

// DatumType is part of the cat.Column interface.
func (desc *ColumnDescriptor) DatumType() types.T {
	return desc.Type.ToDatumType()
}

// ColTypePrecision is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypePrecision() int {
	return int(desc.Type.Precision)
}

// ColTypeWidth is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypeWidth() int {
	return int(desc.Type.Width)
}

// ColTypeStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypeStr() string {
	return desc.Type.SQLString()
}

// IsHidden is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsHidden() bool {
	return desc.Hidden
}

// IsRowIDColumn is a function to check whether column is rowid column
func (desc *ColumnDescriptor) IsRowIDColumn() bool {
	if desc.IsHidden() && desc.HasDefault() && desc.DefaultExprStr() == "unique_rowid()" {
		return true
	}
	return false
}

// HasDefault is part of the cat.Column interface.
func (desc *ColumnDescriptor) HasDefault() bool {
	return desc.DefaultExpr != nil
}

// VisibleType is part of the cat.Column interface.
func (desc *ColumnDescriptor) VisibleType() string {
	return desc.Type.VisibleTypeName
}

// IsOnUpdateCurrentTimeStamp is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsOnUpdateCurrentTimeStamp() bool {
	return desc.Onupdatecurrenttimestamp
}

// IsComputed is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsComputed() bool {
	return desc.ComputeExpr != nil
}

// DefaultExprStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) DefaultExprStr() string {
	return *desc.DefaultExpr
}

// ComputedExprStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) ComputedExprStr() string {
	return *desc.ComputeExpr
}

// CheckCanBeFKRef returns whether the given column is computed.
func (desc *ColumnDescriptor) CheckCanBeFKRef() error {
	if desc.IsComputed() {
		return pgerror.NewErrorf(
			pgcode.InvalidTableDefinition,
			"computed column %q cannot be a foreign key reference",
			desc.Name,
		)
	}
	return nil
}

// SetAuditMode configures the audit mode on the descriptor.
func (desc *TableDescriptor) SetAuditMode(mode tree.AuditMode) (bool, error) {
	prev := desc.AuditMode
	switch mode {
	case tree.AuditModeDisable:
		desc.AuditMode = TableDescriptor_DISABLED
	case tree.AuditModeReadWrite:
		desc.AuditMode = TableDescriptor_READWRITE
	default:
		return false, pgerror.NewErrorf(pgcode.InvalidParameterValue,
			"unknown audit mode: %s (%d)", mode, mode)
	}
	return prev != desc.AuditMode, nil
}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *DatabaseDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// FindAllReferences returns all the references from a table.
func (desc *TableDescriptor) FindAllReferences() (map[ID]struct{}, error) {
	refs := map[ID]struct{}{}
	if err := desc.ForeachNonDropIndex(func(index *IndexDescriptor) error {
		for _, a := range index.Interleave.Ancestors {
			refs[a.TableID] = struct{}{}
		}
		for _, c := range index.InterleavedBy {
			refs[c.Table] = struct{}{}
		}

		if index.ForeignKey.IsSet() {
			to := index.ForeignKey.Table
			refs[to] = struct{}{}
		}

		for _, c := range index.ReferencedBy {
			refs[c.Table] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	for _, c := range desc.AllNonDropColumns() {
		for _, id := range c.UsesSequenceIds {
			refs[id] = struct{}{}
		}
	}

	for _, dest := range desc.DependsOn {
		refs[dest] = struct{}{}
	}

	for _, c := range desc.DependedOnBy {
		refs[c.ID] = struct{}{}
	}
	return refs, nil
}

// FindReferences returns the references from a table.
func (desc *TableDescriptor) FindReferences() (map[ID]struct{}, error) {
	refs := map[ID]struct{}{}
	if err := desc.ForeachNonDropIndex(func(index *IndexDescriptor) error {
		for _, a := range index.Interleave.Ancestors {
			refs[a.TableID] = struct{}{}
		}
		for _, c := range index.InterleavedBy {
			refs[c.Table] = struct{}{}
		}

		if index.ForeignKey.IsSet() {
			to := index.ForeignKey.Table
			refs[to] = struct{}{}
		}

		for _, c := range index.ReferencedBy {
			refs[c.Table] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	//for _, c := range desc.AllNonDropColumns() {
	//	for _, id := range c.UsesSequenceIds {
	//		refs[id] = struct{}{}
	//	}
	//}

	for _, dest := range desc.DependsOn {
		refs[dest] = struct{}{}
	}

	for _, c := range desc.DependedOnBy {
		refs[c.ID] = struct{}{}
	}
	return refs, nil
}

// ActiveChecks returns a list of all check constraints that should be enforced
// on writes (including constraints being added/validated). The columns
// referenced by the returned checks are writable, but not necessarily public.
func (desc *ImmutableTableDescriptor) ActiveChecks() []TableDescriptor_CheckConstraint {
	return desc.allChecks
}

// WritableColumns returns a list of public and write-only mutation columns.
func (desc *ImmutableTableDescriptor) WritableColumns() []ColumnDescriptor {
	return desc.publicAndNonPublicCols[:len(desc.Columns)+desc.writeOnlyColCount]
}

// DeletableColumns returns a list of public and non-public columns.
func (desc *ImmutableTableDescriptor) DeletableColumns() []ColumnDescriptor {
	return desc.publicAndNonPublicCols
}

// MutationColumns returns a list of mutation columns.
func (desc *ImmutableTableDescriptor) MutationColumns() []ColumnDescriptor {
	return desc.publicAndNonPublicCols[len(desc.Columns):]
}

// WritableIndexes returns a list of public and write-only mutation indexes.
func (desc *ImmutableTableDescriptor) WritableIndexes() []IndexDescriptor {
	return desc.publicAndNonPublicIndexes[:len(desc.Indexes)+desc.writeOnlyIndexCount]
}

// DeletableIndexes returns a list of public and non-public indexes.
func (desc *ImmutableTableDescriptor) DeletableIndexes() []IndexDescriptor {
	return desc.publicAndNonPublicIndexes
}

// MutationIndexes returns a list of mutation indexes.
func (desc *ImmutableTableDescriptor) MutationIndexes() []IndexDescriptor {
	return desc.publicAndNonPublicIndexes[len(desc.Indexes):]
}

// DeleteOnlyIndexes returns a list of delete-only mutation indexes.
func (desc *ImmutableTableDescriptor) DeleteOnlyIndexes() []IndexDescriptor {
	return desc.publicAndNonPublicIndexes[len(desc.Indexes)+desc.writeOnlyIndexCount:]
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *MutableTableDescriptor) TableDesc() *TableDescriptor {
	return &desc.TableDescriptor
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *ImmutableTableDescriptor) TableDesc() *TableDescriptor {
	return &desc.TableDescriptor
}

// FunctionDesc implements the ObjectDescriptor interface.
func (desc *MutableFunctionDescriptor) FunctionDesc() *FunctionDescriptor {
	return &desc.FunctionDescriptor
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *FunctionDescriptor) TableDesc() *TableDescriptor {
	return nil
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *MutableFunctionDescriptorGroup) TableDesc() *TableDescriptor {
	return nil
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *ImmutableFunctionDescriptor) TableDesc() *TableDescriptor {
	return nil
}

// GetSchemaNames returns a slice that includes the names of all the schemas.
func (desc *DatabaseDescriptor) GetSchemaNames() []string {
	var res []string
	for _, schema := range desc.Schemas {
		res = append(res, schema.Name)
	}
	return res
}

// GetSchemaID gets the ID of the schema by the name of the schema.
func (desc *DatabaseDescriptor) GetSchemaID(scName string) ID {
	for _, schema := range desc.Schemas {
		if schema.Name == scName {
			return schema.ID
		}
	}
	return desc.ID
}

// ExistSchema determines if the pattern exists by its name.
func (desc *DatabaseDescriptor) ExistSchema(scName string) bool {
	return desc.GetSchemaID(scName) != desc.ID
}

// ExistSchemaID determines if the pattern exists by its ID.
func (desc *DatabaseDescriptor) ExistSchemaID(schemaID ID) bool {
	for _, schema := range desc.Schemas {
		if schema.ID == schemaID {
			return true
		}
	}
	return false
}

// RemoveSchemas deletes the schema that requires deletion
func (desc *DatabaseDescriptor) RemoveSchemas(scNames []string) {
	s := map[string]struct{}{}
	for _, scName := range scNames {
		s[scName] = struct{}{}
	}
	var res []SchemaDescriptor
	for _, schema := range desc.Schemas {
		if _, ok := s[schema.Name]; !ok {
			res = append(res, schema)
		}
	}
	desc.Schemas = res
}

// GetSchemaDesc gets all the SchemaDescroptor in the current database
func (desc *DatabaseDescriptor) GetSchemaDesc(scNames []string) []SchemaDescriptor {
	s := map[string]struct{}{}
	for _, scName := range scNames {
		s[scName] = struct{}{}
	}
	var res []SchemaDescriptor
	for _, schema := range desc.Schemas {
		if _, ok := s[schema.Name]; ok {
			res = append(res, schema)
		}
	}
	return res
}

// SetID implements the DescriptorProto interface.
func (desc *SchemaDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *SchemaDescriptor) TypeName() string {
	return "schema"
}

// SetName implements the DescriptorProto interface.
func (desc *SchemaDescriptor) SetName(name string) {
	desc.Name = name
}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *SchemaDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *SchemaDescriptor) Validate() error {
	if err := validateName(desc.Name, "descriptor"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid schema ID %d", desc.ID)
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	desc.Privileges.MaybeFixPrivileges(desc.GetID(), privilege.DatabasePrivileges)

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID(), privilege.DatabasePrivileges)
}

// Unused
// GetDescMetadataKey returns the descriptor key for the schema.
//func (desc SchemaDescriptor) GetDescMetadataKey() roachpb.Key {
//	return MakeDescMetadataKey(desc.ID)
//}

// Unused
// GetNameMetadataKey returns the namespace key for the schema.
//func (desc SchemaDescriptor) GetNameMetadataKey() roachpb.Key {
//	return MakeNameMetadataKey(desc.ParentID, desc.Name)
//}

// SetID implements the DescriptorProto interface.
func (desc *FunctionDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *FunctionDescriptor) TypeName() string {
	if desc.IsProcedure {
		return "procedure"
	}
	return "function"
}

// SetName implements the DescriptorProto interface.
func (desc *FunctionDescriptor) SetName(name string) {
	desc.Name = name
}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *FunctionDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *FunctionDescriptor) Validate() error {
	if err := validateName(desc.Name, "descriptor"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid schema ID %d", desc.ID)
	}
	if desc.Dropped() {
		return nil
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	desc.Privileges.MaybeFixPrivileges(desc.GetID(), privilege.FunctionPrivileges)

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID(), privilege.FunctionPrivileges)
}

// LocateAlteredSchema locates the altered schema
func (desc *DatabaseDescriptor) LocateAlteredSchema(n *tree.AlterSchema) int {
	var count int
	for i, v := range desc.Schemas {
		if n.Schema.Schema() == v.Name {
			count = i
			break
		}
	}
	return count
}

// GetSchemaByName gets the schemaDescriptor by name
func (desc *DatabaseDescriptor) GetSchemaByName(schemaName string) (*SchemaDescriptor, error) {
	for _, scDesc := range desc.Schemas {
		if schemaName == scDesc.Name || schemaName == "\""+scDesc.Name+"\"" {
			return &scDesc, nil
		}
	}
	return &SchemaDescriptor{}, pgerror.NewErrorWithDepthf(1, pgcode.UndefinedObject, "no object matched, schema name: %s", schemaName)
}

// Equal judges whether the columnType is equal.
func (columnType *ColumnType) Equal(that interface{}) bool {
	if that == nil {
		return columnType == nil
	}

	that1, ok := that.(*ColumnType)
	if !ok {
		that2, ok := that.(ColumnType)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return columnType == nil
	} else if columnType == nil {
		return false
	}
	if columnType.SemanticType != that1.SemanticType {
		return false
	}
	if columnType.Width != that1.Width {
		return false
	}
	if columnType.Precision != that1.Precision {
		return false
	}
	if len(columnType.ArrayDimensions) != len(that1.ArrayDimensions) {
		return false
	}
	for i := range columnType.ArrayDimensions {
		if columnType.ArrayDimensions[i] != that1.ArrayDimensions[i] {
			return false
		}
	}
	if columnType.Locale != nil && that1.Locale != nil {
		if *columnType.Locale != *that1.Locale {
			return false
		}
	} else if columnType.Locale != nil {
		return false
	} else if that1.Locale != nil {
		return false
	}
	if columnType.VisibleType != that1.VisibleType {
		return false
	}
	if columnType.ArrayContents != nil && that1.ArrayContents != nil {
		if *columnType.ArrayContents != *that1.ArrayContents {
			return false
		}
	} else if columnType.ArrayContents != nil {
		return false
	} else if that1.ArrayContents != nil {
		return false
	}
	if len(columnType.TupleContents) != len(that1.TupleContents) {
		return false
	}
	for i := range columnType.TupleContents {
		if !columnType.TupleContents[i].Equal(&that1.TupleContents[i]) {
			return false
		}
	}
	if len(columnType.TupleLabels) != len(that1.TupleLabels) {
		return false
	}
	for i := range columnType.TupleLabels {
		if columnType.TupleLabels[i] != that1.TupleLabels[i] {
			return false
		}
	}
	if len(columnType.EnumContents) != 0 && len(that1.EnumContents) == 0 {
		return false
	}
	if len(columnType.EnumContents) == 0 && len(that1.EnumContents) != 0 {
		return false
	}
	return true
}

// EqualVisible judges is the visible type equal
func (columnType *ColumnType) EqualVisible(that interface{}) bool {
	if that == nil {
		return columnType == nil
	}

	that1, ok := that.(*ColumnType)
	if !ok {
		that2, ok := that.(ColumnType)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return columnType == nil
	} else if columnType == nil {
		return false
	}
	if columnType.SemanticType != that1.SemanticType {
		return false
	}
	if columnType.Width != that1.Width {
		return false
	}
	if columnType.Precision != that1.Precision {
		return false
	}
	if len(columnType.ArrayDimensions) != len(that1.ArrayDimensions) {
		return false
	}
	for i := range columnType.ArrayDimensions {
		if columnType.ArrayDimensions[i] != that1.ArrayDimensions[i] {
			return false
		}
	}
	if columnType.Locale != nil && that1.Locale != nil {
		if *columnType.Locale != *that1.Locale {
			return false
		}
	} else if columnType.Locale != nil {
		return false
	} else if that1.Locale != nil {
		return false
	}
	if columnType.VisibleType != that1.VisibleType {
		return false
	}
	if columnType.ArrayContents != nil && that1.ArrayContents != nil {
		if *columnType.ArrayContents != *that1.ArrayContents {
			return false
		}
	} else if columnType.ArrayContents != nil {
		return false
	} else if that1.ArrayContents != nil {
		return false
	}
	if len(columnType.TupleContents) != len(that1.TupleContents) {
		return false
	}
	for i := range columnType.TupleContents {
		if !columnType.TupleContents[i].Equal(&that1.TupleContents[i]) {
			return false
		}
	}
	if len(columnType.TupleLabels) != len(that1.TupleLabels) {
		return false
	}
	for i := range columnType.TupleLabels {
		if columnType.TupleLabels[i] != that1.TupleLabels[i] {
			return false
		}
	}
	if len(columnType.EnumContents) != len(that1.EnumContents) {
		return false
	}
	for i := range columnType.EnumContents {
		if columnType.EnumContents[i] != that1.EnumContents[i] {
			return false
		}
	}
	if len(columnType.SetContents) != len(that1.SetContents) {
		return false
	}
	for i := range columnType.SetContents {
		if columnType.SetContents[i] != that1.SetContents[i] {
			return false
		}
	}
	if columnType.VisibleTypeName != that1.VisibleTypeName {
		return false
	}
	return true
}

// GetFullName implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) GetFullName(
	_ func(name string, parentID ID) (tree.NodeFormatter, error),
) (string, error) {
	if desc != nil {
		return desc.Name, nil
	}
	return "", nil
}

// GetFullName implements the DescriptorProto interface.
func (desc *SchemaDescriptor) GetFullName(
	f func(name string, parentID ID) (tree.NodeFormatter, error),
) (string, error) {
	if desc != nil {
		scName, err := f(desc.Name, desc.ParentID)
		if err != nil {
			return "", err
		}
		fmtCtx := tree.NewFmtCtx(tree.FmtAlwaysQualifyTableNames)
		fmtCtx.FormatNode(scName)
		return fmtCtx.String(), nil
	}
	return "", nil
}

// GetFullName implements the DescriptorProto interface.
func (desc *TableDescriptor) GetFullName(
	f func(name string, parentID ID) (tree.NodeFormatter, error),
) (string, error) {
	if desc != nil {
		scName, err := f(desc.Name, desc.ParentID)
		if err != nil {
			return "", err
		}
		fmtCtx := tree.NewFmtCtx(tree.FmtAlwaysQualifyTableNames)
		fmtCtx.FormatNode(scName)
		return fmtCtx.String(), nil
	}
	return "", nil
}

// GetFullName implements the DescriptorProto interface.
func (desc *FunctionDescriptor) GetFullName(
	f func(name string, parentID ID) (tree.NodeFormatter, error),
) (string, error) {
	if desc != nil {
		scName, err := f(desc.FullFuncName, desc.ParentID)
		if err != nil {
			return "", err
		}
		fmtCtx := tree.NewFmtCtx(tree.FmtBareIdentifiers)
		fmtCtx.FormatNode(scName)
		return fmtCtx.String(), nil
	}
	return "", nil
}

// ValidateFunction validates that the function descriptor is well formed. Checks
// include validating the function.
// If version is supplied, the descriptor is checked for version incompatibilities.
func (desc *FunctionDescriptor) ValidateFunction(st *cluster.Settings) error {
	if err := validateName(desc.Name, "function"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid function ID %d", desc.ID)
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.ParentID == 0 {
		return fmt.Errorf("invalid parent ID %d", desc.ParentID)
	}

	argIDs := make(map[ColumnID]int, len(desc.Args))
	for i, arg := range desc.AllNonDropArgs() {
		if arg.ID == 0 {
			return fmt.Errorf("invalid argument oid %d", arg.ID)
		}

		if other, ok := argIDs[arg.ID]; ok {
			return fmt.Errorf("arg %q duplicate ID of arg %q: %d",
				arg.Name, other, arg.ID)
		}
		argIDs[arg.ID] = i
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	priv := privilege.FunctionPrivileges

	desc.Privileges.MaybeFixPrivileges(desc.GetID(), priv)

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID(), priv)
}

// AllNonDropArgs returns all the columns, including those being added
// in the mutations.
func (desc *FunctionDescriptor) AllNonDropArgs() []FuncArgDescriptor {
	cols := make([]FuncArgDescriptor, 0, len(desc.Args))
	cols = append(cols, desc.Args...)
	return cols
}

// GetPrivileges implements the DescriptorProto interface.
func (desc *ColumnDescriptor) GetPrivileges() *PrivilegeDescriptor {
	if desc != nil {
		return desc.Privileges
	}
	return nil
}

// GetID implements the DescriptorProto interface.
func (desc *ColumnDescriptor) GetID() ID {
	if desc != nil {
		return ID(desc.ID)
	}
	return 0
}

// SetID implements the DescriptorProto interface.
func (desc *ColumnDescriptor) SetID(id ID) {
	desc.ID = ColumnID(id)
}

// TypeName returns the plain type of this descriptor.
func (desc *ColumnDescriptor) TypeName() string {
	return "column"
}

// GetName implements the DescriptorProto interface.
func (desc *ColumnDescriptor) GetName() string {
	if desc != nil {
		return desc.Name
	}
	return ""
}

// GetFullName implements the DescriptorProto interface.
func (desc *ColumnDescriptor) GetFullName(
	f func(name string, parentID ID) (tree.NodeFormatter, error),
) (string, error) {
	if desc != nil {
		colName, err := f(desc.Name, desc.ParentID)
		if err != nil {
			return "", err
		}
		fmtCtx := tree.NewFmtCtx(tree.FmtAlwaysQualifyTableNames)
		fmtCtx.FormatNode(colName)
		return fmtCtx.String(), nil
	}
	return "", nil
}

// SetName implements the DescriptorProto interface.
func (desc *ColumnDescriptor) SetName(name string) {
	desc.Name = name
}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *ColumnDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// GetColumnByName get columnDescriptor by name.
func (desc *MutableTableDescriptor) GetColumnByName(columnName string) (*ColumnDescriptor, error) {
	for _, colDesc := range desc.Columns {
		if columnName == colDesc.Name {
			return &colDesc, nil
		}
	}
	return &ColumnDescriptor{}, pgerror.NewErrorWithDepthf(1, pgcode.UndefinedObject, "no object matched, column name: %s", columnName)
}

// Table 对原本的GetTable()方法做一层封装，以给desc设置ModifactionTime
func (desc *Descriptor) Table(ts hlc.Timestamp) *TableDescriptor {
	t := desc.GetTable()
	if t != nil {
		desc.MaybeSetModificationTimeFromMVCCTimestamp(context.TODO(), ts)
	}
	return t
}

// MaybeSetModificationTimeFromMVCCTimestamp 给desc的ModificationTime赋值
func (desc *Descriptor) MaybeSetModificationTimeFromMVCCTimestamp(
	ctx context.Context, ts hlc.Timestamp,
) {
	switch t := desc.Union.(type) {
	case nil:
		// Empty descriptors shouldn't be touched.
		return
	case *Descriptor_Table:
		// CreateAsOfTime is used for CREATE TABLE ... AS ... and was introduced in
		// v19.1. In general it is not critical to set except for tables in the ADD
		// state which were created from CTAS so we should not assert on its not
		// being set. It's not always sensical to set it from the passed MVCC
		// timestamp. However, starting in 19.2 the CreateAsOfTime and
		// ModificationTime fields are both unset for the first Version of a
		// TableDescriptor and the code relies on the value being set based on the
		// MVCC timestamp.
		if !ts.IsEmpty() &&
			t.Table.ModificationTime.IsEmpty() &&
			t.Table.CreateAsOfTime.IsEmpty() &&
			t.Table.Version == 1 {
			t.Table.CreateAsOfTime = ts
		}

		// Ensure that if the table is in the process of being added and relies on
		// CreateAsOfTime that it is now set.
		if t.Table.Adding() && t.Table.IsAs() && t.Table.CreateAsOfTime.IsEmpty() {
			log.Fatalf(context.TODO(), "table descriptor for %q (%d.%d) is in the "+
				"ADD state and was created with CREATE TABLE ... AS but does not have a "+
				"CreateAsOfTime set", t.Table.Name, t.Table.ParentID, t.Table.ID)
		}
	}
	// Set the ModificationTime based on the passed ts if we should.
	// Table descriptors can be updated in place after their version has been
	// incremented (e.g. to include a schema change lease).
	// When this happens we permit the ModificationTime to be written explicitly
	// with the value that lives on the in-memory copy. That value should contain
	// a timestamp set by this method. Thus if the ModificationTime is set it
	// must not be after the MVCC timestamp we just read it at.
	if modTime := desc.GetModificationTime(); modTime.IsEmpty() && ts.IsEmpty() && desc.GetVersion() > 1 {
		// TODO(ajwerner): reconsider the third condition here.It seems that there
		// are some cases where system tables lack this timestamp and then when they
		// are rendered in some other downstream setting we expect the timestamp to
		// be read. This is a hack we shouldn't need to do.
		log.Fatalf(context.TODO(), "read table descriptor for %q (%d) without ModificationTime "+
			"with zero MVCC timestamp", desc.GetName(), desc.GetID())
	} else if modTime.IsEmpty() {
		desc.SetModificationTime(ts)
	} else if !ts.IsEmpty() && ts.Less(modTime) {
		log.Fatalf(context.TODO(), "read table descriptor %q (%d) which has a ModificationTime "+
			"after its MVCC timestamp: has %v, expected %v",
			desc.GetName(), desc.GetID(), modTime, ts)
	}
}

// GetModificationTime returns the ModificationTime of the descriptor.
func (desc *Descriptor) GetModificationTime() hlc.Timestamp {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		return t.Table.ModificationTime
	case *Descriptor_Function:
		return t.Function.ModificationTime
	default:
		debug.PrintStack()
		panic(errors.AssertionFailedf("GetModificationTime: unknown Descriptor type %T", t))
	}
}

// SetModificationTime 给对应的Descriptor赋值
func (desc *Descriptor) SetModificationTime(ts hlc.Timestamp) {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		t.Table.ModificationTime = ts
	case *Descriptor_Function:
		t.Function.ModificationTime = ts
	default:
		panic(errors.AssertionFailedf("setModificationTime: unknown Descriptor type %T", t))
	}
}

// GetVersion returns the Version of the descriptor.
func (desc *Descriptor) GetVersion() DescriptorVersion {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		return t.Table.Version
	case *Descriptor_Function:
		return t.Function.Version
	default:
		panic(errors.AssertionFailedf("GetVersion: unknown Descriptor type %T", t))
	}
}

// SchemaKey implements DescriptorKey interface.
type SchemaKey struct {
	parentID ID
	name     string
}

// NewSchemaKey returns a new SchemaKey
func NewSchemaKey(parentID ID, name string) SchemaKey {
	return SchemaKey{parentID: parentID, name: name}
}

// Key implements DescriptorKey interface.
func (sk SchemaKey) Key() roachpb.Key {
	return MakeNameMetadataKey(sk.parentID, sk.name) //(sk.parentID, keys.RootNamespaceID, sk.name)
}

// Name implements DescriptorKey interface.
func (sk SchemaKey) Name() string {
	return sk.name
}

// DatabaseKey implements DescriptorKey.
type DatabaseKey struct {
	name string
}

// NewDatabaseKey returns a new DatabaseKey.
func NewDatabaseKey(name string) DatabaseKey {
	return DatabaseKey{name: name}
}

// Key implements DescriptorKey interface.
func (dk DatabaseKey) Key() roachpb.Key {
	return MakeNameMetadataKey(keys.RootNamespaceID, dk.name)
}

// Name implements DescriptorKey interface.
func (dk DatabaseKey) Name() string {
	return dk.name
}

// dummyColumn represents a variable column that can type-checked. It is used
// in validating check constraint and partial index predicate expressions. This
// validation requires that the expression can be both both typed-checked and
// examined for variable expressions.
type dummyColumn struct {
	typ  types.T
	name tree.Name
}

// String implements the Stringer interface.
func (d *dummyColumn) String() string {
	return tree.AsString(d)
}

// Format implements the NodeFormatter interface.
func (d *dummyColumn) Format(ctx *tree.FmtCtx) {
	d.name.Format(ctx)
}

// Walk implements the Expr interface.
func (d *dummyColumn) Walk(_ tree.Visitor) tree.Expr {
	return d
}

// TypeCheck implements the Expr interface.
func (d *dummyColumn) TypeCheck(
	ctx *tree.SemaContext, desired types.T, useOrigin bool,
) (tree.TypedExpr, error) {
	return d, nil
}

// Eval implements the TypedExpr interface.
func (*dummyColumn) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("dummyColumnItem.Eval() is undefined")
}

// ResolvedType implements the TypedExpr interface.
func (d *dummyColumn) ResolvedType() types.T {
	return d.typ
}

// ReplaceColumnVars replaces the occurrences of column names in an expression with
// dummyColumns containing their type, so that they may be type-checked. It
// returns this new expression tree alongside a set containing the ColumnID of
// each column seen in the expression.
//
// If the expression references a column that does not exist in the table
// descriptor, replaceColumnVars errs with pgcode.UndefinedColumn.
func ReplaceColumnVars(
	desc TableDescriptor, rootExpr tree.Expr,
) (tree.Expr, util.FastIntSet, error) {
	var colIDs util.FastIntSet

	newExpr, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return nil, true, expr
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err, false, nil
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return nil, true, expr
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return pgerror.Newf(pgcode.UndefinedColumn,
					"column %q does not exist, referenced in %q", c.ColumnName, rootExpr.String()),
				false, nil
		}
		colIDs.Add(int(col.ID))

		// Convert to a dummyColumn of the correct type.
		return nil, false, &dummyColumn{typ: col.Type.ToDatumType(), name: c.ColumnName}
	})

	return newExpr, colIDs, err
}

//DeserializeExprForFormatting is for DeserializeExpr
func DeserializeExprForFormatting(
	desc TableDescriptor, exprStr string, semaCtx *tree.SemaContext,
) (tree.Expr, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return nil, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	expr, _, err = ReplaceColumnVars(desc, expr)
	if err != nil {
		return nil, err
	}

	// Type-check the expression to resolve user defined types.
	typedExpr, err := expr.TypeCheck(semaCtx, types.Any, false)
	if err != nil {
		return nil, err
	}

	return typedExpr, nil
}

// FormatExprForDisplay formats a schema expression string for display by adding
// type annotations and resolving user defined types.
func FormatExprForDisplay(
	desc TableDescriptor, exprStr string, semaCtx *tree.SemaContext,
) (string, error) {
	expr, err := DeserializeExprForFormatting(desc, exprStr, semaCtx)
	if err != nil {
		return "", err
	}
	return tree.SerializeWithoutSuffix(expr), nil
}

// GenerateUniqueConstraintName attempts to generate a unique constraint name
// with the given prefix.
// It will first try prefix by itself, then it will subsequently try
// adding numeric digits at the end, starting from 1.
func GenerateUniqueConstraintName(prefix string, nameExistsFunc func(name string) bool) string {
	name := prefix
	for i := 1; nameExistsFunc(name); i++ {
		name = fmt.Sprintf("%s_%d", prefix, i)
	}
	return name
}

// HasOwner returns true if the sequence options indicate an owner exists.
func (opts *TableDescriptor_SequenceOpts) HasOwner() bool {
	return !opts.SequenceOwner.Equal(TableDescriptor_SequenceOpts_SequenceOwner{})
}

// FindPartitionByName searches this partitioning descriptor for a partition
// whose name is the input and returns it, or nil if no match is found.
func (desc *PartitioningDescriptor) FindPartitionByName(name string) *PartitioningDescriptor {
	for _, l := range desc.List {
		if l.Name == name {
			return desc
		}
		if s := l.Subpartitioning.FindPartitionByName(name); s != nil {
			return s
		}
	}
	for _, r := range desc.Range {
		if r.Name == name {
			return desc
		}
	}
	return nil
}

// FindIndexPartitionByName searches this index descriptor for a partition whose name
// is the input and returns it, or nil if no match is found.
func FindIndexPartitionByName(desc *IndexDescriptor, name string) *PartitioningDescriptor {
	return desc.Partitioning.FindPartitionByName(name)

}
