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

package cat

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util/treeprinter"
)

// EngineType defines storge engine supported by replica;
type EngineType uint32

const (
	// KV store
	KV EngineType = 1 << iota
	// COLUMN store
	COLUMN
)

// EngineTypeSet is the bit set of EngineTypes.
type EngineTypeSet uint32

const (
	// EngineKVOnly is only used to support KV store
	EngineKVOnly = EngineTypeSet(KV)
	// EngineColumnOnly is only used to support COLUMN store
	EngineColumnOnly = EngineTypeSet(COLUMN)
	// EngineAll is used to support KV or COLUMN store
	EngineAll = EngineTypeSet(KV | COLUMN)
)

// DataStoreEngine swrap engine type set info
type DataStoreEngine struct {
	ETypeSet EngineTypeSet
}

// Table is an interface to a database table, exposing only the information
// needed by the query optimizer.
//
// Both columns and indexes are grouped into three sets: public, write-only, and
// delete-only. When a column or index is added or dropped, it proceeds through
// each of the three states as that schema change is incrementally rolled out to
// the cluster without blocking ongoing queries. In the public state, reads,
// writes, and deletes are allowed. In the write-only state, only writes and
// deletes are allowed. Finally, in the delete-only state, only deletes are
// allowed. Further details about "online schema change" can be found in:
//
//   docs/RFCS/20151014_online_schema_change.md
//
// Calling code must take care to use the right collection of columns or
// indexes. Usually this should be the public collections, since most usages are
// read-only, but mutation operators generally need to consider non-public
// columns and indexes.
type Table interface {
	DataSource

	// IsVirtualTable returns true if this table is a special system table that
	// constructs its rows "on the fly" when it's queried. An example is the
	// information_schema tables.
	IsVirtualTable() bool

	// IsMaterializedView returns true if this table is actually a materialized
	// view. Materialized views are the same as tables in all aspects, other than
	// that they cannot be mutated.
	IsMaterializedView() bool

	// IsInterleaved returns true if any of this table's indexes are interleaved
	// with index(es) from other table(s).
	IsInterleaved() bool

	// IsReferenced returns true if this table is referenced by at least one
	// foreign key defined on another table (or this one if self-referential).
	IsReferenced() bool

	// ColumnCount returns the number of public columns in the table. Public
	// columns are not currently being added or dropped from the table. This
	// method should be used when mutation columns can be ignored (the common
	// case).
	ColumnCount() int

	// WritableColumnCount returns the number of public and write-only columns in
	// the table. Although write-only columns are not visible, any inserts and
	// updates must still set them. WritableColumnCount is always >= ColumnCount.
	WritableColumnCount() int

	// DeletableColumnCount returns the number of public, write-only, and
	// delete- only columns in the table. DeletableColumnCount is always >=
	// WritableColumnCount.
	DeletableColumnCount() int

	// Column returns a Column interface to the column at the ith ordinal
	// position within the table, where i < ColumnCount. Note that the Columns
	// collection includes mutation columns, if present. Mutation columns are in
	// the process of being added or dropped from the table, and may need to have
	// default or computed values set when inserting or updating rows. See this
	// RFC for more details:
	//
	//   znbasedb/znbase/docs/RFCS/20151014_online_schema_change.md
	//
	// Writable columns are always situated after public columns, and are followed
	// by deletable columns.
	Column(i int) Column

	// ColumnByIDAndName return a Column by colID or colName.
	ColumnByIDAndName(colID int, colName string) Column

	// IndexCount returns the number of public indexes defined on this table.
	// Public indexes are not currently being added or dropped from the table.
	// This method should be used when mutation columns can be ignored (the common
	// case). The returned indexes include the primary index, so the count is
	// always >= 1.
	IndexCount() int

	// WritableIndexCount returns the number of public and write-only indexes
	// defined on this table. Although write-only indexes are not visible, any
	// table mutation operations must still be applied to them. WritableIndexCount
	// is always >= IndexCount.
	WritableIndexCount() int

	// DeletableIndexCount returns the number of public, write-only, and
	// delete-onlyindexes defined on this table. DeletableIndexCount is always
	// >= WritableIndexCount.
	DeletableIndexCount() int

	// Index returns the ith index, where i < IndexCount. The table's primary
	// index is always the 0th index, and is always present (use cat.PrimaryIndex
	// to select it). The primary index corresponds to the table's primary key.
	// If a primary key was not explicitly specified, then the system implicitly
	// creates one based on a hidden rowid column.
	Index(i int) Index

	// Index1 returns the ith index, where i < IndexCount. The table's primary
	// index is always the 0th index, and is always present (use cat.PrimaryIndex
	// to select it). The primary index corresponds to the table's primary key.
	// If a primary key was not explicitly specified, then the system implicitly
	// creates one based on a hidden rowid column.
	Index1(i int) Index

	// StatisticCount returns the number of statistics available for the table.
	StatisticCount() int

	// Statistic returns the ith statistic, where i < StatisticCount.
	Statistic(i int) TableStatistic

	// CheckCount returns the number of check constraints present on the table.
	CheckCount() int

	// Check returns the ith check constraint, where i < CheckCount.
	Check(i int) CheckConstraint

	// FamilyCount returns the number of column families present on the table.
	// There is always at least one primary family (always family 0) where columns
	// go if they are not explicitly assigned to another family. The primary
	// family is the first family that was explicitly specified by the user, or
	// is a synthesized family if no families were explicitly specified.
	FamilyCount() int

	// Family returns the interface for the ith column family, where
	// i < FamilyCount.
	Family(i int) Family

	// get the engines supported by replicas.
	DataStoreEngineInfo() DataStoreEngine
	IsHashPartition() bool

	// get the inheritsBy list if this is an inherit table
	GetInhertisBy() []uint32
}

// TableXQ is an interface to Table and IndexXQ
type TableXQ interface {
	Table() Table
	Index(i int) IndexXQ
}

// CheckConstraint is the SQL text for a check constraint on a table. Check
// constraints are user-defined restrictions on the content of each row in a
// table. For example, this check constraint ensures that only values greater
// than zero can be inserted into the table:
//
//   CREATE TABLE a (a INT CHECK (a > 0))
//
type CheckConstraint string

// TableStatistic is an interface to a table statistic. Each statistic is
// associated with a set of columns.
type TableStatistic interface {
	// CreatedAt indicates when the statistic was generated.
	CreatedAt() time.Time

	// ColumnCount is the number of columns the statistic pertains to.
	ColumnCount() int

	// ColumnOrdinal returns the column ordinal (see Table.Column) of the ith
	// column in this statistic, with 0 <= i < ColumnCount.
	ColumnOrdinal(i int) int

	// RowCount returns the estimated number of rows in the table.
	RowCount() uint64

	// DistinctCount returns the estimated number of distinct values on the
	// columns of the statistic. If there are multiple columns, each "value" is a
	// tuple with the values on each column. Rows where any statistic column have
	// a NULL don't contribute to this count.
	DistinctCount() uint64

	// NullCount returns the estimated number of rows which have a NULL value on
	// any column in the statistic.
	NullCount() uint64

	// TODO(radu): add Histogram().
	Histogram() []HistogramBucket
}

//HistogramBucket 直方图桶
type HistogramBucket struct {
	// NumEq is the estimated number of values equal to UpperBound.
	NumEq float64

	// NumRange is the estimated number of values between the upper bound of the
	// previous bucket and UpperBound (both boundaries are exclusive).
	// The first bucket should always have NumRange=0.
	NumRange float64

	// DistinctRange is the estimated number of distinct values between the upper
	// bound of the previous bucket and UpperBound (both boundaries are
	// exclusive).
	DistinctRange float64

	// UpperBound is the upper bound of the bucket.
	UpperBound tree.Datum
	LowerBound tree.Datum
	Frequency  float64
}

// ForeignKeyReference is a struct representing an outbound foreign key reference.
// It has accessors for table and index IDs, as well as the prefix length.
type ForeignKeyReference struct {
	// Table contains the referenced table's stable identifier.
	TableID StableID

	// Index contains the stable identifier of the index that represents the
	// destination table's side of the foreign key relation.
	IndexID StableID

	// PrefixLen contains the length of columns that form the foreign key
	// relation in the current and destination indexes.
	PrefixLen int32

	// Validated is true if the reference is validated (i.e. we know that the
	// existing data satisfies the constraint). It is possible to set up a foreign
	// key constraint on existing tables without validating it, in which case we
	// cannot make any assumptions about the data.
	Validated bool

	// Match contains the method used for comparing composite foreign keys.
	Match tree.CompositeKeyMatchMethod
}

// FindTableColumnByName returns the ordinal of the non-mutation column having
// the given name, if one exists in the given table. Otherwise, it returns -1.
func FindTableColumnByName(tab Table, name tree.Name) int {
	for ord, n := 0, tab.ColumnCount(); ord < n; ord++ {
		if tab.Column(ord).ColName() == name {
			return ord
		}
	}
	return -1
}

// FormatTable nicely formats a catalog table using a treeprinter for debugging
// and testing.
func FormatTable(cat Catalog, tab Table, tp treeprinter.Node) {
	child := tp.Childf("TABLE %s", tab.Name().TableName)

	var buf bytes.Buffer
	for i := 0; i < tab.DeletableColumnCount(); i++ {
		buf.Reset()
		formatColumn(tab.Column(i), IsMutationColumn(tab, i), &buf)
		child.Child(buf.String())
	}

	for i := 0; i < tab.DeletableIndexCount(); i++ {
		formatCatalogIndex(tab, i, child)
	}

	for i := 0; i < tab.IndexCount(); i++ {
		fkRef, ok := tab.Index(i).ForeignKey()

		if ok {
			formatCatalogFKRef(cat, tab.Index(i), fkRef, child)
		}
	}

	for i := 0; i < tab.CheckCount(); i++ {
		child.Childf("CHECK (%s)", tab.Check(i))
	}

	// Don't print the primary family, since it's implied.
	if tab.FamilyCount() > 1 || tab.Family(0).Name() != "primary" {
		for i := 0; i < tab.FamilyCount(); i++ {
			buf.Reset()
			formatFamily(tab.Family(i), &buf)
			child.Child(buf.String())
		}
	}
}

// formatCatalogIndex nicely formats a catalog index using a treeprinter for
// debugging and testing.
func formatCatalogIndex(tab Table, ord int, tp treeprinter.Node) {
	idx := tab.Index(ord)
	inverted := ""
	if idx.IsInverted() {
		inverted = "INVERTED "
	}
	mutation := ""
	if IsMutationIndex(tab, ord) {
		mutation = " (mutation)"
	}
	child := tp.Childf("%sINDEX %s%s", inverted, idx.Name(), mutation)

	var buf bytes.Buffer
	colCount := idx.ColumnCount()
	if ord == PrimaryIndex {
		// Omit the "stored" columns from the primary index.
		colCount = idx.KeyColumnCount()
	}

	for i := 0; i < colCount; i++ {
		buf.Reset()

		idxCol := idx.Column(i)
		formatColumn(idxCol.Column, false /* isMutationCol */, &buf)
		if idxCol.Descending {
			fmt.Fprintf(&buf, " desc")
		}

		if i >= idx.LaxKeyColumnCount() {
			fmt.Fprintf(&buf, " (storing)")
		}

		child.Child(buf.String())
	}
}

// formatColPrefix returns a string representation of the first prefixLen columns of idx.
func formatColPrefix(idx Index, prefixLen int) string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	for i := 0; i < prefixLen; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		colName := idx.Column(i).ColName()
		buf.WriteString(colName.String())
	}
	buf.WriteByte(')')

	return buf.String()
}

// formatCatalogFKRef nicely formats a catalog foreign key reference using a
// treeprinter for debugging and testing.
func formatCatalogFKRef(cat Catalog, idx Index, fkRef ForeignKeyReference, tp treeprinter.Node) {
	ds, err := cat.ResolveDataSourceByID(context.TODO(), Flags{}, fkRef.TableID)
	if err != nil {
		panic(err)
	}

	fkTable := ds.(Table)

	var fkIndex Index
	for j, cnt := 0, fkTable.IndexCount(); j < cnt; j++ {
		if fkTable.Index(j).ID() == fkRef.IndexID {
			fkIndex = fkTable.Index(j)
			break
		}
	}

	tp.Childf(
		"FOREIGN KEY %s REFERENCES %v %s",
		formatColPrefix(idx, int(fkRef.PrefixLen)),
		ds.Name(),
		formatColPrefix(fkIndex, int(fkRef.PrefixLen)),
	)
}

func formatColumn(col Column, isMutationCol bool, buf *bytes.Buffer) {
	fmt.Fprintf(buf, "%s %s", col.ColName(), col.DatumType())
	if !col.IsNullable() {
		fmt.Fprintf(buf, " not null")
	}
	if col.IsHidden() {
		fmt.Fprintf(buf, " (hidden)")
	}
	if isMutationCol {
		fmt.Fprintf(buf, " (mutation)")
	}
}

func formatFamily(family Family, buf *bytes.Buffer) {
	fmt.Fprintf(buf, "FAMILY %s (", family.Name())
	for i, n := 0, family.ColumnCount(); i < n; i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		col := family.Column(i)
		buf.WriteString(string(col.ColName()))
	}
	buf.WriteString(")")
}

// ExpandDataSourceGlob is a utility function that expands a tree.TablePattern
// into a list of object names.
func ExpandDataSourceGlob(
	ctx context.Context, catalog Catalog, flags Flags, pattern tree.TablePattern,
) ([]DataSourceName, error) {

	switch p := pattern.(type) {
	case *tree.TableName:
		_, name, err := catalog.ResolveDataSource(ctx, flags, p)
		if err != nil {
			return nil, err
		}
		return []DataSourceName{name}, nil

	case *tree.AllTablesSelector:
		return catalog.ResolveAllDataSource(ctx, flags, &p.TableNamePrefix)

	default:
		return nil, errors.Errorf("invalid TablePattern type %T", p)
	}
}

// ResolveTableIndex resolves a TableIndexName.
func ResolveTableIndex(
	ctx context.Context, catalog Catalog, flags Flags, name *tree.TableIndexName,
) (Index, DataSourceName, error) {
	if name.Table.TableName != "" {
		ds, tn, err := catalog.ResolveDataSource(ctx, flags, &name.Table)
		if err != nil {
			return nil, DataSourceName{}, err
		}
		table, ok := ds.(Table)
		if !ok {
			tabXQ, ok := ds.(TableXQ)
			if !ok {
				return nil, DataSourceName{}, pgerror.Newf(
					pgcode.WrongObjectType, "%q is not a table", name.Table.TableName,
				)
			}
			table = tabXQ.Table()
		}
		if name.Index == "" {
			// Return primary index.
			return table.Index(0), tn, nil
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				return idx, tn, nil
			}
		}
		return nil, DataSourceName{}, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}

	// We have to search for a table that has an index with the given name.
	dsNames, err := catalog.ResolveAllDataSource(ctx, flags, &name.Table.TableNamePrefix)
	if err != nil {
		return nil, DataSourceName{}, err
	}
	var found Index
	var foundTabName DataSourceName
	for i := range dsNames {
		ds, tn, err := catalog.ResolveDataSource(ctx, flags, &dsNames[i])
		if err != nil {
			return nil, DataSourceName{}, err
		}
		table, ok := ds.(Table)
		if !ok {
			tabXQ, ok := ds.(TableXQ)
			if !ok {
				// Not a table, ignore.
				continue
			}
			table = tabXQ.Table()
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				if found != nil {
					return nil, DataSourceName{}, pgerror.Newf(pgcode.AmbiguousParameter,
						"index name %q is ambiguous (found in %s and %s)",
						name.Index, tn.String(), foundTabName.String())
				}
				found = idx
				foundTabName = tn
				break
			}
		}
	}
	if found == nil {
		return nil, DataSourceName{}, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}
	return found, foundTabName, nil
}

//GetRange get range between lower and upper
func GetRange(lowerBound, upperBound tree.Datum) (rng float64, ok bool) {
	switch lowerBound.ResolvedType() {
	case types.Int:
		rng = float64(*upperBound.(*tree.DInt)) - float64(*lowerBound.(*tree.DInt))
		return rng, true

	case types.Date:
		lower := lowerBound.(*tree.DDate)
		upper := upperBound.(*tree.DDate)
		if lower.IsFinite() && upper.IsFinite() {
			rng = float64(*upper) - float64(*lower)
			return rng, true
		}
		return 0, false

	case types.Decimal:
		lower, err := lowerBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, false
		}
		upper, err := upperBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, false
		}
		rng = upper - lower
		return rng, true

	case types.Float:
		rng = float64(*upperBound.(*tree.DFloat)) - float64(*lowerBound.(*tree.DFloat))
		return rng, true

	case types.Timestamp:
		lower := lowerBound.(*tree.DTimestamp).Time
		upper := upperBound.(*tree.DTimestamp).Time
		rng = float64(upper.Sub(lower))
		return rng, true

	case types.TimestampTZ:
		lower := lowerBound.(*tree.DTimestampTZ).Time
		upper := upperBound.(*tree.DTimestampTZ).Time
		rng = float64(upper.Sub(lower))
		return rng, true

	default:
		return 0, false
	}
}

//GetLowerBound get datum of lower
func (b *HistogramBucket) GetLowerBound() tree.Datum {
	return b.LowerBound
}

//GetUpperBound get datum of upper
func (b *HistogramBucket) GetUpperBound() tree.Datum {
	return b.UpperBound
}

//SetDistinct set distinct value
func (b *HistogramBucket) SetDistinct(value float64) {
	b.DistinctRange = value
}

//Contains 查看datum是否在这个桶中
func (b *HistogramBucket) Contains(point tree.Datum, evalCtx *tree.EvalContext) bool {
	if b.UpperBound == point || b.LowerBound == point {
		return true
	}
	if point.Compare(evalCtx, b.LowerBound) == 1 &&
		point.Compare(evalCtx, b.UpperBound) == -1 {
		return true
	}
	return false
}

//IsBefore 查看datum是否在这个桶之前
func (b *HistogramBucket) IsBefore(other *HistogramBucket, evalCtx *tree.EvalContext) bool {
	if b.Intersects(other, evalCtx) {
		return false
	}
	if b.UpperBound.Compare(evalCtx, other.LowerBound) < 1 {
		return true
	}
	return false
}

//IsAfter 查看datum是否在这个桶之后
func (b *HistogramBucket) IsAfter(other *HistogramBucket, evalCtx *tree.EvalContext) bool {
	if b.Intersects(other, evalCtx) {
		return false
	}
	if b.LowerBound.Compare(evalCtx, other.UpperBound) != -1 {
		return true
	}
	return false
}

//IsLessThan 如果upper小于 datum，返回true
func (b *HistogramBucket) IsLessThan(p tree.Datum, evalCtx *tree.EvalContext) bool {
	if b.UpperBound.Compare(evalCtx, p) == -1 {
		return true
	}
	return false
}

//IsGreaterThan 如果lower大于 datum 返回false
func (b *HistogramBucket) IsGreaterThan(p tree.Datum, evalCtx *tree.EvalContext) bool {
	if b.LowerBound.Compare(evalCtx, p) == 1 {
		return true
	}
	return false
}

//CompareUpperBounds Compare upper bounds of the buckets, return 0 if they match, 1 if
// ub of bucket1 is greater than that of bucket2 and -1 otherwise.
func (b *HistogramBucket) CompareUpperBounds(
	other *HistogramBucket, evalCtx *tree.EvalContext,
) int {
	point1 := b.UpperBound
	point2 := other.UpperBound
	cValue := point1.Compare(evalCtx, point2)
	if cValue == 0 {
		return 0
	} else if cValue == -1 {
		return -1
	} else {
		return 1
	}
}

//CompareLowerBounds Compare lower bounds of the buckets, return 0 if they match, 1 if
// ub of bucket1 is greater than that of bucket2 and -1 otherwise.
func (b *HistogramBucket) CompareLowerBounds(
	other *HistogramBucket, evalCtx *tree.EvalContext,
) int {
	point1 := b.LowerBound
	point2 := other.LowerBound
	cValue := point1.Compare(evalCtx, point2)
	if cValue == 0 {
		return 0
	} else if cValue == -1 {
		return -1
	} else {
		return 1
	}
}

//Intersects 如果是两个直方图是相交的，返回true
func (b *HistogramBucket) Intersects(other *HistogramBucket, evalCtx *tree.EvalContext) bool {
	if b.IsSingleton(evalCtx) && other.IsSingleton(evalCtx) {
		if b.LowerBound.Compare(evalCtx, other.LowerBound) == 0 {
			return true
		}
	}
	if b.IsSingleton(evalCtx) {
		return other.Contains(b.LowerBound, evalCtx)
	}
	if other.IsSingleton(evalCtx) {
		return b.Contains(other.LowerBound, evalCtx)
	}
	if b.Subsumes(other, evalCtx) || other.Subsumes(b, evalCtx) {
		return true
	}
	if b.CompareLowerBounds(other, evalCtx) <= 0 {
		if other.CompareLowerBoundToUpperBound(b, evalCtx) <= 0 {
			return true
		}
		return false
	}
	if b.CompareLowerBoundToUpperBound(other, evalCtx) <= 0 {
		return true
	}
	return false
}

//IsSingleton 如果是单桶，返回true
func (b *HistogramBucket) IsSingleton(evalCtx *tree.EvalContext) bool {
	if b.LowerBound.Compare(evalCtx, b.UpperBound) == 0 {
		return true
	}
	return false
}

//Subsumes Does this bucket subsumes/contains another?
func (b *HistogramBucket) Subsumes(other *HistogramBucket, evalCtx *tree.EvalContext) bool {
	if b.IsSingleton(evalCtx) && other.IsSingleton(evalCtx) {
		if b.LowerBound.Compare(evalCtx, other.LowerBound) == 0 {
			return true
		}
	}
	if other.IsSingleton(evalCtx) {
		return b.Contains(other.LowerBound, evalCtx)
	}
	lcValue := b.CompareLowerBounds(other, evalCtx)
	ucValue := b.CompareUpperBounds(other, evalCtx)

	if lcValue <= 0 && ucValue >= 0 {
		return true
	}
	return false
}

//CompareLowerBoundToUpperBound 对比上界和下界
func (b *HistogramBucket) CompareLowerBoundToUpperBound(
	other *HistogramBucket, evalCtx *tree.EvalContext,
) int {
	return b.LowerBound.Compare(evalCtx, other.UpperBound)
}

//Copy 复制一个直方图桶
func (b *HistogramBucket) Copy() HistogramBucket {
	return HistogramBucket{
		NumEq:         b.NumEq,
		NumRange:      b.NumRange,
		DistinctRange: b.DistinctRange,
		UpperBound:    b.UpperBound,
		LowerBound:    b.LowerBound,
	}
}

//GetOverlapPercentage 得到重叠比例
func (b *HistogramBucket) GetOverlapPercentage(p tree.Datum, evalCtx *tree.EvalContext) float64 {
	// special case of upper bound equal to point
	if b.UpperBound.Compare(evalCtx, p) < 1 {
		return 1.0
	}
	// if point is not contained, then no overlap
	if !b.Contains(p, evalCtx) {
		return 0.0
	}
	// special case for singleton bucket
	if b.IsSingleton(evalCtx) {
		// b.LowerBound is equal to p.
		return 1.0
	}
	// general case, compute distance ratio
	distanceUpper, _ := GetRange(b.LowerBound, b.UpperBound)
	distanceMiddle, _ := GetRange(b.LowerBound, p)
	res := 1 / distanceUpper
	if distanceMiddle > 0.0 {
		res = res * distanceMiddle
	}
	return math.Min(res, 1.0)
}

//MakeBucketScaleUpper scaling upper to be same as lower is identical to producing a singleton bucket
func (b *HistogramBucket) MakeBucketScaleUpper(
	p tree.Datum, includeUpper bool, evalCtx *tree.EvalContext,
) *HistogramBucket {
	// scaling upper to be same as lower is identical to producing a singleton bucket
	if b.LowerBound.Compare(evalCtx, p) == 0 {
		if includeUpper == false {
			return nil
		}
		return &HistogramBucket{
			NumEq:         1,
			NumRange:      1,
			DistinctRange: 1,
			UpperBound:    p,
			LowerBound:    p,
		}
	}
	frequencyNew := b.Frequency
	distinctNew := b.DistinctRange
	numRangeNew := b.NumRange
	if b.UpperBound.Compare(evalCtx, p) == 0 {
		overlap := b.GetOverlapPercentage(p, evalCtx)
		frequencyNew = frequencyNew * overlap
		distinctNew = distinctNew * overlap
		numRangeNew = numRangeNew * overlap
	}
	upperBoundNew := p
	if includeUpper == false {
		ub, _ := p.Prev(evalCtx)
		upperBoundNew = ub
	}
	return &HistogramBucket{
		NumEq:         0,
		NumRange:      numRangeNew,
		DistinctRange: distinctNew,
		UpperBound:    upperBoundNew,
		LowerBound:    b.LowerBound,
		Frequency:     frequencyNew,
	}
}

//MakeBucketScaleLower scaling upper to be same as lower is identical to producing a singleton bucket
func (b *HistogramBucket) MakeBucketScaleLower(
	p tree.Datum, includeLower bool, evalCtx *tree.EvalContext,
) *HistogramBucket {
	// scaling upper to be same as lower is identical to producing a singleton bucket
	if b.UpperBound.Compare(evalCtx, p) == 0 {
		return b.MakeBucketSingleton(p)
	}
	frequencyNew := b.Frequency
	distinctNew := b.DistinctRange
	numRangeNew := b.NumRange
	numEqNew := b.NumEq
	if b.LowerBound.Compare(evalCtx, p) != 0 {
		overlap := 1.0 - b.GetOverlapPercentage(p, evalCtx)
		frequencyNew = frequencyNew * overlap
		distinctNew = distinctNew * overlap
		numRangeNew = numRangeNew * overlap
		numEqNew = numEqNew * overlap
	}
	lowerBoundNew := p
	if includeLower == false {
		lb, _ := p.Next(evalCtx)
		lowerBoundNew = lb
	}
	return &HistogramBucket{
		NumEq:         numEqNew,
		NumRange:      numRangeNew,
		DistinctRange: distinctNew,
		UpperBound:    b.UpperBound,
		LowerBound:    lowerBoundNew,
		Frequency:     frequencyNew,
	}
}

//MakeBucketSingleton 做一个单例桶
func (b *HistogramBucket) MakeBucketSingleton(p tree.Datum) *HistogramBucket {
	return &HistogramBucket{
		NumEq:         1,
		NumRange:      1,
		DistinctRange: 1,
		UpperBound:    p,
		LowerBound:    p,
		Frequency:     0.0,
	}
}

//MakeBucketGreaterThan 生成一个比datum的值大的桶
func (b *HistogramBucket) MakeBucketGreaterThan(
	p tree.Datum, evalCtx *tree.EvalContext,
) *HistogramBucket {
	if b.IsSingleton(evalCtx) ||
		b.UpperBound.Compare(evalCtx, p) == 0 {
		return nil
	}
	pNext, _ := p.Next(evalCtx)
	if pNext != nil {
		if b.Contains(pNext, evalCtx) {
			return b.MakeBucketScaleLower(pNext, true, evalCtx)
		}
	} else {
		return b.MakeBucketScaleLower(p, false, evalCtx)
	}
	return nil
}

//IsBeforePoint is the point before the lower bound of the bucket
func (b *HistogramBucket) IsBeforePoint(p tree.Datum, evalCtx *tree.EvalContext) bool {
	return b.LowerBound.Compare(evalCtx, p) >= 0
}

//IsAfterPoint is the point after the upper bound of the bucket
func (b *HistogramBucket) IsAfterPoint(p tree.Datum, evalCtx *tree.EvalContext) bool {
	return b.UpperBound.Compare(evalCtx, p) <= 0
}
