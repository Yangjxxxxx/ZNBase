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

package row

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
)

type checkFKConstraints bool

const (
	// CheckFKs can be passed to row writers to check fk validity.
	CheckFKs checkFKConstraints = true
	// SkipFKs can be passed to row writer to skip fk validity checks.
	SkipFKs checkFKConstraints = false
)

// Inserter abstracts the key/value operations for inserting table rows.
type Inserter struct {
	Helper                rowHelper
	InsertCols            []sqlbase.ColumnDescriptor
	InsertColIDtoRowIndex map[sqlbase.ColumnID]int
	Fks                   fkExistenceCheckForInsert

	// For allocation avoidance.
	marshaled []roachpb.Value
	key       roachpb.Key
	valueBuf  []byte
	value     roachpb.Value
}

// MakeInserter creates a Inserter for the given table.
//
// insertCols must contain every column in the primary key.
func MakeInserter(
	ctx context.Context,
	txn *client.Txn,
	privCheckFn CheckPrivilegeFunction,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	insertCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	alloc *sqlbase.DatumAlloc,
) (Inserter, error) {
	ri := Inserter{
		Helper:                newRowHelper(tableDesc, tableDesc.WritableIndexes()),
		InsertCols:            insertCols,
		InsertColIDtoRowIndex: ColIDtoRowIndexFromCols(insertCols),
		marshaled:             make([]roachpb.Value, len(insertCols)),
	}

	for i, col := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := ri.InsertColIDtoRowIndex[col]; !ok {
			return Inserter{}, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	if checkFKs == CheckFKs {
		var err error
		if ri.Fks, err = makeFkExistenceCheckHelperForInsert(ctx, txn, privCheckFn, tableDesc, fkTables,
			ri.InsertColIDtoRowIndex, alloc); err != nil {
			return ri, err
		}
	}
	return ri, nil
}

// insertCPutFn is used by insertRow when conflicts (i.e. the key already exists)
// should generate errors.
func insertCPutFn(
	ctx context.Context, b putter, key *roachpb.Key, value *roachpb.Value, traceKV bool,
) {
	// TODO(dan): We want do this V(2) log everywhere in sql. Consider making a
	// client.Batch wrapper instead of inlining it everywhere.
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "CPut %s -> %s", *key, value.PrettyPrint())
	}
	b.CPut(key, value, nil /* expValue */)
}

// insertPutFn is used by insertRow when conflicts should be ignored.
func insertPutFn(
	ctx context.Context, b putter, key *roachpb.Key, value *roachpb.Value, traceKV bool,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "Put %s -> %s", *key, value.PrettyPrint())
	}
	b.Put(key, value)
}

// insertDelFn is used by insertRow to delete existing rows.
func insertDelFn(ctx context.Context, b putter, key *roachpb.Key, traceKV bool) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "Del %s", *key)
	}
	b.Del(key)
}

// insertPutFn is used by insertRow when conflicts should be ignored.
func insertInvertedPutFn(
	ctx context.Context, b putter, key *roachpb.Key, value *roachpb.Value, traceKV bool,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "InitPut %s -> %s", *key, value.PrettyPrint())
	}
	b.InitPut(key, value, false)
}

type putter interface {
	CPut(key, value, expValue interface{})
	Put(key, value interface{})
	InitPut(key, value interface{}, failOnTombstones bool)
	Del(key ...interface{})
}

// InsertRow adds to the batch the kv operations necessary to insert a table row
// with the given values.
func (ri *Inserter) InsertRow(
	ctx context.Context,
	b putter,
	values []tree.Datum,
	overwrite bool,
	checkFKs checkFKConstraints,
	traceKV bool,
	pm schemaexpr.PartialIndexUpdateHelper,
) error {
	var funcindex int
	if ri.Helper.Indexes != nil {
		for _, index := range ri.Helper.Indexes {
			if index.IsFunc != nil {
				for _, isfunc := range index.IsFunc {
					if isfunc {
						funcindex++
					}
				}
			}
		}
	}
	if len(values) != len(ri.InsertCols)+funcindex {
		if funcindex == 0 {
			return errors.Errorf("got %d values but expected %d", len(values), len(ri.InsertCols))
		}
		return errors.Errorf("got %d values but expected %d", len(values)-funcindex, len(ri.InsertCols))
	}

	putFn := insertCPutFn
	if overwrite {
		putFn = insertPutFn
	}

	// Encode the values to the expected column type. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for _, family := range ri.Helper.TableDesc.Families {
		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID {
			for i, val := range values[:len(ri.InsertCols)] {
				// Make sure the value can be written to the column before proceeding.
				var err error
				if ri.marshaled[i], err = sqlbase.MarshalColumnValue(&ri.InsertCols[i], val); err != nil {
					return err
				}
			}
			break
		}
	}

	if ri.Fks.checker != nil && checkFKs == CheckFKs {
		if err := ri.Fks.addAllIdxChecks(ctx, values[:len(ri.InsertCols)], traceKV); err != nil {
			return err
		}
		if err := ri.Fks.checker.runCheck(ctx, nil, values[:len(ri.InsertCols)]); err != nil {
			return err
		}
	}

	//var primaryIndexKey []byte
	//var secondaryIndexEntries []sqlbase.IndexEntry
	//var err error
	//
	//if funcindex != 0 {
	//	result := make(tree.Datums,0)
	//	result = append(result, values[len(values)-1])
	//	for i := 0; i < len(values)-1; i++ {
	//		result = append(result, values[i])
	//	}
	//
	//	primaryIndexKey, _, err = ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex,
	//		values, pm.IgnoreForPut)
	//	if err != nil {
	//		return err
	//	}
	//
	//	_, secondaryIndexEntries, err = ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex,
	//		result, pm.IgnoreForPut)
	//	if err != nil {
	//		return err
	//	}
	//}else {
	//	primaryIndexKey, secondaryIndexEntries, err = ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex,
	//		values, pm.IgnoreForPut)
	//	if err != nil {
	//		return err
	//	}

	primaryIndexKey, secondaryIndexEntries, err := ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex,
		values, pm.IgnoreForPut)
	if err != nil {
		return err
	}

	// Add the new values.
	ri.valueBuf, err = prepareInsertOrUpdateBatch(ctx, b,
		&ri.Helper, primaryIndexKey, ri.InsertCols,
		values, ri.InsertColIDtoRowIndex,
		ri.marshaled, ri.InsertColIDtoRowIndex,
		&ri.key, &ri.value, ri.valueBuf, putFn, overwrite, traceKV)
	if err != nil {
		return err
	}

	putFn = insertInvertedPutFn
	for i := range secondaryIndexEntries {
		if secondaryIndexEntries[i].Key != nil {
			e := &secondaryIndexEntries[i]
			putFn(ctx, b, &e.Key, &e.Value, traceKV)
		}
	}

	return nil
}

// prepareInsertOrUpdateBatch constructs a KV batch that inserts or
// updates a row in KV.
// - batch is the KV batch where commands should be appended.
// - putFn is the functions that can append Put/CPut commands to the batch.
//   (must be adapted depending on whether 'overwrite' is set)
// - helper is the rowHelper that knows about the table being modified.
// - primaryIndexKey is the PK prefix for the current row.
// - fetchedCols is the list of schema columns that have been fetched
//   in preparation for this update.
// - values is the SQL-level row values that are being written.
// - marshaledValues contains the pre-encoded KV-level row values.
//   marshaledValues is only used when writing single column families.
//   Regardless of whether there are single column families,
//   pre-encoding must occur prior to calling this function to check whether
//   the encoding is _possible_ (i.e. values fit in the column types, etc).
// - valColIDMapping/marshaledColIDMapping is the mapping from column
//   IDs into positions of the slices values or marshaledValues.
// - kvKey and kvValues must be heap-allocated scratch buffers to write
//   roachpb.Key and roachpb.Value values.
// - rawValueBuf must be a scratch byte array. This must be reinitialized
//   to an empty slice on each call but can be preserved at its current
//   capacity to avoid allocations. The function returns the slice.
// - overwrite must be set to true for UPDATE and UPSERT.
// - traceKV is to be set to log the KV operations added to the batch.
func prepareInsertOrUpdateBatch(
	ctx context.Context,
	batch putter,
	helper *rowHelper,
	primaryIndexKey []byte,
	fetchedCols []sqlbase.ColumnDescriptor,
	values []tree.Datum,
	valColIDMapping map[sqlbase.ColumnID]int,
	marshaledValues []roachpb.Value,
	marshaledColIDMapping map[sqlbase.ColumnID]int,
	kvKey *roachpb.Key,
	kvValue *roachpb.Value,
	rawValueBuf []byte,
	putFn func(ctx context.Context, b putter, key *roachpb.Key, value *roachpb.Value, traceKV bool),
	overwrite, traceKV bool,
) ([]byte, error) {
	for i, family := range helper.TableDesc.Families {
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := marshaledColIDMapping[colID]; ok {
				update = true
				break
			}
		}
		if !update {
			continue
		}

		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}

		*kvKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.

			idx, ok := marshaledColIDMapping[family.DefaultColumnID]
			if !ok {
				continue
			}

			if marshaledValues[idx].RawBytes == nil {
				if overwrite {
					// If the new family contains a NULL value, then we must
					// delete any pre-existing row.
					insertDelFn(ctx, batch, kvKey, traceKV)
				}
			} else {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.
				putFn(ctx, batch, kvKey, &marshaledValues[idx], traceKV)
			}

			continue
		}

		rawValueBuf = rawValueBuf[:0]

		var lastColID sqlbase.ColumnID
		familySortedColumnIDs, ok := helper.sortedColumnFamily(family.ID)
		if !ok {
			return nil, pgerror.NewAssertionErrorf("invalid family sorted column id map")
		}
		for _, colID := range familySortedColumnIDs {
			idx, ok := valColIDMapping[colID]
			if !ok || values[idx] == tree.DNull {
				// Column not being updated or inserted.
				continue
			}

			if skip, err := helper.skipColumnInPK(colID, family.ID, values[idx]); err != nil {
				return nil, err
			} else if skip {
				continue
			}

			col := fetchedCols[idx]

			if lastColID > col.ID {
				return nil, pgerror.NewAssertionErrorf("cannot write column id %d after %d", col.ID, lastColID)
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID
			var err error
			rawValueBuf, err = sqlbase.EncodeTableValue(rawValueBuf, colIDDiff, values[idx], nil)
			if err != nil {
				return nil, err
			}
		}

		if family.ID != 0 && len(rawValueBuf) == 0 {
			if overwrite {
				// The family might have already existed but every column in it is being
				// set to NULL, so delete it.
				insertDelFn(ctx, batch, kvKey, traceKV)
			}
		} else {
			// Copy the contents of rawValueBuf into the roachpb.Value. This is
			// a deep copy so rawValueBuf can be re-used by other calls to the
			// function.
			kvValue.SetTuple(rawValueBuf)
			putFn(ctx, batch, kvKey, kvValue, traceKV)
		}

		// Release reference to roachpb.Key.
		*kvKey = nil
		// Prevent future calls to prepareInsertOrUpdateBatch from mutating
		// the RawBytes in the kvValue we just added to the batch. Remember
		// that we share the kvValue reference across calls to this function.
		*kvValue = roachpb.Value{}
	}

	return rawValueBuf, nil
}

// EncodeIndexesForRow encodes the provided values into their primary and
// secondary index keys. The secondaryIndexEntries are only valid until the next
// call to EncodeIndexesForRow.
func (ri *Inserter) EncodeIndexesForRow(
	ctx context.Context, values []tree.Datum,
) (primaryIndexKey []byte, secondaryIndexEntries []sqlbase.IndexEntry, err error) {
	var ignoreIndexes util.FastIntSet
	return ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex, values, ignoreIndexes)
}

// Updater abstracts the key/value operations for updating table rows.
type Updater struct {
	Helper                rowHelper
	DeleteHelper          *rowHelper
	FetchCols             []sqlbase.ColumnDescriptor
	FetchColIDtoRowIndex  map[sqlbase.ColumnID]int
	UpdateCols            []sqlbase.ColumnDescriptor
	UpdateColIDtoRowIndex map[sqlbase.ColumnID]int
	primaryKeyColChange   bool

	// rd and ri are used when the update this Updater is created for modifies
	// the primary key of the table. In that case, rows must be deleted and
	// re-added instead of merely updated, since the keys are changing.
	rd Deleter
	ri Inserter

	Fks      fkExistenceCheckForUpdate
	cascader *cascader

	// For allocation avoidance.
	marshaled       []roachpb.Value
	newValues       []tree.Datum
	key             roachpb.Key
	indexEntriesBuf []sqlbase.IndexEntry
	valueBuf        []byte
	value           roachpb.Value
	oldIndexEntries [][]sqlbase.IndexEntry
	newIndexEntries [][]sqlbase.IndexEntry
}

// DeleteColumnForMerge func
func (ru *Updater) DeleteColumnForMerge(
	ctx context.Context, b *client.Batch, rows tree.Datums, traceKV bool,
) error {
	ru.rd.Helper.TableDesc = ru.Helper.TableDesc
	ru.rd.FetchCols = ru.FetchCols
	ru.rd.FetchColIDtoRowIndex = ru.FetchColIDtoRowIndex
	return ru.rd.DeleteRow(ctx, b, rows, true, traceKV)
}

type rowUpdaterType int

const (
	// UpdaterDefault indicates that an Updater should update everything
	// about a row, including secondary indexes.
	UpdaterDefault rowUpdaterType = 0
	// UpdaterOnlyColumns indicates that an Updater should only update the
	// columns of a row.
	UpdaterOnlyColumns rowUpdaterType = 1
)

// MakeUpdater creates a Updater for the given table.
//
// UpdateCols are the columns being updated and correspond to the updateValues
// that will be passed to UpdateRow.
//
// The returned Updater contains a FetchCols field that defines the
// expectation of which values are passed as oldValues to UpdateRow. All the columns
// passed in requestedCols will be included in FetchCols at the beginning.
func MakeUpdater(
	txn *client.Txn,
	privCheckFn CheckPrivilegeFunction,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	updateCols []sqlbase.ColumnDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
	updateType rowUpdaterType,
	evalCtx *tree.EvalContext,
	alloc *sqlbase.DatumAlloc,
) (Updater, error) {
	rowUpdater, err := makeUpdaterWithoutCascader(
		evalCtx.Ctx(), txn, privCheckFn, tableDesc, fkTables, updateCols, requestedCols, updateType, alloc,
	)
	if err != nil {
		return Updater{}, err
	}
	rowUpdater.cascader, err = makeUpdateCascader(
		txn, privCheckFn, tableDesc, fkTables, updateCols, evalCtx, alloc,
	)
	if err != nil {
		return Updater{}, err
	}
	return rowUpdater, nil
}

type returnTrue struct{}

func (returnTrue) Error() string { panic("unimplemented") }

var returnTruePseudoError error = returnTrue{}

// makeUpdaterWithoutCascader is the same function as MakeUpdater but does not
// create a cascader.
func makeUpdaterWithoutCascader(
	ctx context.Context,
	txn *client.Txn,
	privCheckFn CheckPrivilegeFunction,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	updateCols []sqlbase.ColumnDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
	updateType rowUpdaterType,
	alloc *sqlbase.DatumAlloc,
) (Updater, error) {
	updateColIDtoRowIndex := ColIDtoRowIndexFromCols(updateCols)

	primaryIndexCols := make(map[sqlbase.ColumnID]struct{}, len(tableDesc.PrimaryIndex.ColumnIDs))
	for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
		primaryIndexCols[colID] = struct{}{}
	}

	var primaryKeyColChange bool
	for _, c := range updateCols {
		if _, ok := primaryIndexCols[c.ID]; ok {
			primaryKeyColChange = true
			break
		}
	}

	// Secondary indexes needing updating.
	needsUpdate := func(index sqlbase.IndexDescriptor) bool {
		if updateType == UpdaterOnlyColumns {
			// Only update columns.
			return false
		}
		// If the primary key changed, we need to update all of them.
		if primaryKeyColChange {
			return true
		}
		if index.PredExpr != "" {
			return true
		}
		return index.RunOverAllColumns(func(id sqlbase.ColumnID) error {
			if _, ok := updateColIDtoRowIndex[id]; ok {
				return returnTruePseudoError
			}
			return nil
		}) != nil
	}

	writableIndexes := tableDesc.WritableIndexes()
	includeIndexes := make([]sqlbase.IndexDescriptor, 0, len(writableIndexes))
	for _, index := range writableIndexes {
		if needsUpdate(index) {
			includeIndexes = append(includeIndexes, index)
		}
	}

	// Columns of the table to update, including those in delete/write-only state
	tableCols := tableDesc.DeletableColumns()

	var deleteOnlyIndexes []sqlbase.IndexDescriptor
	for _, idx := range tableDesc.DeleteOnlyIndexes() {
		if needsUpdate(idx) {
			if deleteOnlyIndexes == nil {
				// Allocate at most once.
				deleteOnlyIndexes = make([]sqlbase.IndexDescriptor, 0, len(tableDesc.DeleteOnlyIndexes()))
			}
			deleteOnlyIndexes = append(deleteOnlyIndexes, idx)
		}
	}

	var deleteOnlyHelper *rowHelper
	if len(deleteOnlyIndexes) > 0 {
		rh := newRowHelper(tableDesc, deleteOnlyIndexes)
		deleteOnlyHelper = &rh
	}

	ru := Updater{
		Helper:                newRowHelper(tableDesc, includeIndexes),
		DeleteHelper:          deleteOnlyHelper,
		UpdateCols:            updateCols,
		UpdateColIDtoRowIndex: updateColIDtoRowIndex,
		primaryKeyColChange:   primaryKeyColChange,
		marshaled:             make([]roachpb.Value, len(updateCols)),
		newValues:             make([]tree.Datum, len(tableCols)),
		oldIndexEntries:       make([][]sqlbase.IndexEntry, len(includeIndexes)),
		newIndexEntries:       make([][]sqlbase.IndexEntry, len(includeIndexes)),
	}

	if primaryKeyColChange {
		// These fields are only used when the primary key is changing.
		// When changing the primary key, we delete the old values and reinsert
		// them, so request them all.
		var err error
		if ru.rd, err = makeRowDeleterWithoutCascader(
			txn, tableDesc, fkTables, tableCols, SkipFKs, alloc,
		); err != nil {
			return Updater{}, err
		}
		ru.FetchCols = ru.rd.FetchCols
		ru.FetchColIDtoRowIndex = ColIDtoRowIndexFromCols(ru.FetchCols)
		// SkipFKs
		if ru.ri, err = MakeInserter(nil, txn, nil, tableDesc, fkTables,
			tableCols, SkipFKs, alloc); err != nil {
			return Updater{}, err
		}
	} else {
		ru.FetchCols = requestedCols[:len(requestedCols):len(requestedCols)]
		ru.FetchColIDtoRowIndex = ColIDtoRowIndexFromCols(ru.FetchCols)

		// maybeAddCol adds the provided column to ru.FetchCols and
		// ru.FetchColIDtoRowIndex if it isn't already present.
		maybeAddCol := func(colID sqlbase.ColumnID) error {
			if _, ok := ru.FetchColIDtoRowIndex[colID]; !ok {
				col, _, err := tableDesc.FindReadableColumnByID(colID)
				if err != nil {
					return err
				}
				ru.FetchColIDtoRowIndex[col.ID] = len(ru.FetchCols)
				ru.FetchCols = append(ru.FetchCols, *col)
			}
			return nil
		}

		// Fetch all columns in the primary key so that we can construct the
		// keys when writing out the new kvs to the primary index.
		for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return Updater{}, err
			}
		}

		// If any part of a column family is being updated, fetch all columns in
		// that column family so that we can reconstruct the column family with
		// the updated columns before writing it.
		for _, fam := range tableDesc.Families {
			familyBeingUpdated := false
			for _, colID := range fam.ColumnIDs {
				if _, ok := ru.UpdateColIDtoRowIndex[colID]; ok {
					familyBeingUpdated = true
					break
				}
			}
			if familyBeingUpdated {
				for _, colID := range fam.ColumnIDs {
					if err := maybeAddCol(colID); err != nil {
						return Updater{}, err
					}
				}
			}
		}

		// Fetch all columns from indices that are being update so that they can
		// be used to create the new kv pairs for those indices.
		for _, index := range includeIndexes {
			if index.IsFuncIndex() {
				continue
			}
			if err := index.RunOverAllColumns(maybeAddCol); err != nil {
				return Updater{}, err
			}
		}
		for _, index := range deleteOnlyIndexes {
			if err := index.RunOverAllColumns(maybeAddCol); err != nil {
				return Updater{}, err
			}
		}
	}

	var err error
	if ru.Fks, err = makeFkExistenceCheckHelperForUpdate(ctx, txn, privCheckFn, tableDesc, fkTables,
		ru.FetchColIDtoRowIndex, alloc); err != nil {
		return Updater{}, err
	}
	return ru, nil
}

// UpdateRow adds to the batch the kv operations necessary to update a table row
// with the given values.
//
// The row corresponding to oldValues is updated with the ones in updateValues.
// Note that updateValues only contains the ones that are changing.
//
// The return value is only good until the next call to UpdateRow.
func (ru *Updater) UpdateRow(
	ctx context.Context,
	b *client.Batch,
	oldValues []tree.Datum,
	updateValues []tree.Datum,
	checkFKs checkFKConstraints,
	traceKV bool,
	pm schemaexpr.PartialIndexUpdateHelper,
) ([]tree.Datum, error) {
	batch := b
	if ru.cascader != nil {
		batch = ru.cascader.txn.NewBatch()
	}

	if len(updateValues) != len(ru.UpdateCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(updateValues), len(ru.UpdateCols))
	}

	var funcIndexNum int
	if ru.Helper.TableDesc.Indexes != nil {
		for _, index := range ru.Helper.TableDesc.Indexes {
			if index.IsFunc != nil {
				for _, isFunc := range index.IsFunc {
					if isFunc {
						funcIndexNum++
					}
				}
			}
		}
	}
	if len(oldValues) != len(ru.FetchCols)+2*funcIndexNum {
		return nil, errors.Errorf("got %d values but expected %d", len(oldValues)-2*funcIndexNum, len(ru.FetchCols))
	}

	primaryIndexKey, oldSecondaryIndexEntries, err := ru.Helper.encodeIndexes(ru.FetchColIDtoRowIndex, oldValues[:(len(ru.FetchCols)+funcIndexNum)], pm.IgnoreForDel)

	if err != nil {
		return nil, err
	}
	var deleteOldSecondaryIndexEntries []sqlbase.IndexEntry
	if ru.DeleteHelper != nil {
		_, deleteOldSecondaryIndexEntries, err = ru.DeleteHelper.encodeIndexes(ru.FetchColIDtoRowIndex, oldValues[:(len(ru.FetchCols)+funcIndexNum)], pm.IgnoreForDel)
		if err != nil {
			return nil, err
		}
	}
	// The secondary index entries returned by rowHelper.encodeIndexes are only
	// valid until the next call to encodeIndexes. We need to copy them so that
	// we can compare against the new secondary index entries.
	oldSecondaryIndexEntries = append(ru.indexEntriesBuf[:0], oldSecondaryIndexEntries...)
	ru.indexEntriesBuf = oldSecondaryIndexEntries

	// Check that the new value types match the column types. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range updateValues {
		if ru.marshaled[i], err = sqlbase.MarshalColumnValue(&ru.UpdateCols[i], val); err != nil {
			return nil, err
		}
	}

	// Update the row values.
	copy(ru.newValues, oldValues[:len(ru.FetchCols)])
	for i := 0; i < funcIndexNum; i++ {
		ru.newValues = append(ru.newValues, oldValues[len(ru.FetchCols)+funcIndexNum+i])
	}
	for i, updateCol := range ru.UpdateCols {
		ru.newValues[ru.FetchColIDtoRowIndex[updateCol.ID]] = updateValues[i]
	}

	rowPrimaryKeyChanged := false
	var newSecondaryIndexEntries []sqlbase.IndexEntry
	if ru.primaryKeyColChange {
		var newPrimaryIndexKey []byte
		newPrimaryIndexKey, newSecondaryIndexEntries, err =
			ru.Helper.encodeIndexes(ru.FetchColIDtoRowIndex, ru.newValues, pm.IgnoreForPut)
		if err != nil {
			return nil, err
		}
		rowPrimaryKeyChanged = !bytes.Equal(primaryIndexKey, newPrimaryIndexKey)
	} else {
		newSecondaryIndexEntries, err =
			ru.Helper.encodeSecondaryIndexes(ru.FetchColIDtoRowIndex, ru.newValues, pm.IgnoreForPut)
		if err != nil {
			return nil, err
		}
	}
	if rowPrimaryKeyChanged {
		if err := ru.rd.DeleteRow(ctx, batch, oldValues[:len(ru.FetchCols)], SkipFKs, traceKV); err != nil {
			return nil, err
		}
		if err := ru.ri.InsertRow(
			ctx, batch, ru.newValues[:len(ru.FetchCols)], false /* ignoreConflicts */, SkipFKs, traceKV, pm,
		); err != nil {
			return nil, err
		}

		ru.Fks.addCheckForIndex(ru.Helper.TableDesc.PrimaryIndex.ID, ru.Helper.TableDesc.PrimaryIndex.Type)
		for i := range ru.Helper.Indexes {
			if !bytes.Equal(newSecondaryIndexEntries[i].Key, oldSecondaryIndexEntries[i].Key) {
				ru.Fks.addCheckForIndex(ru.Helper.Indexes[i].ID, ru.Helper.Indexes[i].Type)
			}
		}

		if ru.cascader != nil {
			if err := ru.cascader.txn.Run(ctx, batch); err != nil {
				return nil, ConvertBatchError(ctx, ru.Helper.TableDesc, batch)
			}
			if err := ru.cascader.cascadeAll(
				ctx,
				ru.Helper.TableDesc,
				tree.Datums(oldValues[:len(ru.FetchCols)]),
				tree.Datums(ru.newValues[:len(ru.FetchCols)]),
				ru.FetchColIDtoRowIndex,
				traceKV,
			); err != nil {
				return nil, err
			}
		}

		if checkFKs == CheckFKs {
			if err := ru.Fks.addIndexChecks(ctx, oldValues[:len(ru.FetchCols)], ru.newValues[:len(ru.FetchCols)], traceKV); err != nil {
				return nil, err
			}
			if !ru.Fks.hasFKs() {
				return ru.newValues, nil
			}
			if err := ru.Fks.checker.runCheck(ctx, oldValues[:len(ru.FetchCols)], ru.newValues[:len(ru.FetchCols)]); err != nil {
				return nil, err
			}
		}

		return ru.newValues[:len(ru.FetchCols)], nil
	}

	// Add the new values.
	ru.valueBuf, err = prepareInsertOrUpdateBatch(ctx, b,
		&ru.Helper, primaryIndexKey, ru.FetchCols,
		ru.newValues[:len(ru.FetchCols)], ru.FetchColIDtoRowIndex,
		ru.marshaled, ru.UpdateColIDtoRowIndex,
		&ru.key, &ru.value, ru.valueBuf, insertPutFn, true /* overwrite */, traceKV)
	if err != nil {
		return nil, err
	}

	// Update secondary indexes.
	// We're iterating through all of the indexes, which should have corresponding entries in both oldSecondaryIndexEntries
	// and newSecondaryIndexEntries. Inverted indexes could potentially have more entries at the end of both and we will
	// update those separately.
	for i, index := range ru.Helper.Indexes {
		oldSecondaryIndexEntry := oldSecondaryIndexEntries[i]
		newSecondaryIndexEntry := newSecondaryIndexEntries[i]

		// We're skipping inverted indexes in this loop, but appending the inverted index entry to the back of
		// newSecondaryIndexEntries to process later. For inverted indexes we need to remove all old entries before adding
		// new ones.
		if index.Type == sqlbase.IndexDescriptor_INVERTED {
			newSecondaryIndexEntries = append(newSecondaryIndexEntries, newSecondaryIndexEntry)
			oldSecondaryIndexEntries = append(oldSecondaryIndexEntries, oldSecondaryIndexEntry)

			continue
		}

		var expValue interface{}
		if newSecondaryIndexEntry.Key != nil && !bytes.Equal(newSecondaryIndexEntry.Key, oldSecondaryIndexEntry.Key) {
			ru.Fks.addCheckForIndex(ru.Helper.Indexes[i].ID, ru.Helper.Indexes[i].Type)
			if traceKV {
				log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(ru.Helper.secIndexValDirs[i], oldSecondaryIndexEntry.Key))
			}
			if oldSecondaryIndexEntry.Key != nil {
				batch.Del(oldSecondaryIndexEntry.Key)
			}
		} else if newSecondaryIndexEntry.Value.RawBytes != nil && oldSecondaryIndexEntry.Value.RawBytes != nil && !newSecondaryIndexEntry.Value.EqualData(oldSecondaryIndexEntry.Value) {
			expValue = &oldSecondaryIndexEntry.Value
		} else {
			continue
		}

		if traceKV {
			k := keys.PrettyPrint(ru.Helper.secIndexValDirs[i], newSecondaryIndexEntry.Key)
			v := newSecondaryIndexEntry.Value.PrettyPrint()
			if expValue != nil {
				log.VEventf(ctx, 2, "CPut %s -> %v (replacing %v, if exists)", k, v, expValue)
			} else {
				log.VEventf(ctx, 2, "CPut %s -> %v (expecting does not exist)", k, v)
			}
		}
		if newSecondaryIndexEntry.Key != nil {
			batch.CPutAllowingIfNotExists(newSecondaryIndexEntry.Key, &newSecondaryIndexEntry.Value, expValue)
		}
	}

	// We're deleting indexes in a delete only state. We're bounding this by the number of indexes because inverted
	// indexed will be handled separately.
	if ru.DeleteHelper != nil {
		for _, deletedSecondaryIndexEntry := range deleteOldSecondaryIndexEntries {
			if traceKV {
				log.VEventf(ctx, 2, "Del %s", deletedSecondaryIndexEntry.Key)
			}
			batch.Del(deletedSecondaryIndexEntry.Key)
		}
	}

	// We're removing all of the inverted index entries from the row being updated.
	for i := len(ru.Helper.Indexes); i < len(oldSecondaryIndexEntries); i++ {
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", oldSecondaryIndexEntries[i].Key)
		}
		if oldSecondaryIndexEntries[i].Key != nil {
			batch.Del(oldSecondaryIndexEntries[i].Key)
		}
	}

	putFn := insertInvertedPutFn
	// We're adding all of the inverted index entries from the row being updated.
	for i := len(ru.Helper.Indexes); i < len(newSecondaryIndexEntries); i++ {
		if newSecondaryIndexEntries[i].Key != nil {
			putFn(ctx, b, &newSecondaryIndexEntries[i].Key, &newSecondaryIndexEntries[i].Value, traceKV)
		}
	}

	if ru.cascader != nil {
		if err := ru.cascader.txn.Run(ctx, batch); err != nil {
			return nil, ConvertBatchError(ctx, ru.Helper.TableDesc, batch)
		}
		if err := ru.cascader.cascadeAll(
			ctx,
			ru.Helper.TableDesc,
			tree.Datums(oldValues[:len(ru.FetchCols)]),
			tree.Datums(ru.newValues[:len(ru.FetchCols)]),
			ru.FetchColIDtoRowIndex,
			traceKV,
		); err != nil {
			return nil, err
		}
	}

	if checkFKs == CheckFKs {
		if err := ru.Fks.addIndexChecks(ctx, oldValues[:len(ru.FetchCols)], ru.newValues[:len(ru.FetchCols)], traceKV); err != nil {
			return nil, err
		}
		if ru.Fks.hasFKs() {
			if err := ru.Fks.checker.runCheck(ctx, oldValues[:len(ru.FetchCols)], ru.newValues[:len(ru.FetchCols)]); err != nil {
				return nil, err
			}
		}
	}

	return ru.newValues[:len(ru.FetchCols)], nil
}

// IsColumnOnlyUpdate returns true if this Updater is only updating column
// data (in contrast to updating the primary key or other indexes).
func (ru *Updater) IsColumnOnlyUpdate() bool {
	// TODO(dan): This is used in the schema change backfill to assert that it was
	// configured correctly and will not be doing things it shouldn't. This is an
	// unfortunate bleeding of responsibility and indicates the abstraction could
	// be improved. Specifically, Updater currently has two responsibilities
	// (computing which indexes need to be updated and mapping sql rows to k/v
	// operations) and these should be split.
	return !ru.primaryKeyColChange && ru.DeleteHelper == nil && len(ru.Helper.Indexes) == 0
}

// Deleter abstracts the key/value operations for deleting table rows.
type Deleter struct {
	Helper               rowHelper
	FetchCols            []sqlbase.ColumnDescriptor
	FetchColIDtoRowIndex map[sqlbase.ColumnID]int
	Fks                  fkExistenceCheckForDelete
	cascader             *cascader
	// For allocation avoidance.
	key roachpb.Key
}

// MakeDeleter creates a Deleter for the given table.
//
// The returned Deleter contains a FetchCols field that defines the
// expectation of which values are passed as values to DeleteRow. Any column
// passed in requestedCols will be included in FetchCols.
func MakeDeleter(
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	requestedCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	evalCtx *tree.EvalContext,
	alloc *sqlbase.DatumAlloc,
) (Deleter, error) {
	rowDeleter, err := makeRowDeleterWithoutCascader(
		txn, tableDesc, fkTables, requestedCols, checkFKs, alloc,
	)
	if err != nil {
		return Deleter{}, err
	}
	if checkFKs == CheckFKs {
		var err error
		rowDeleter.cascader, err = makeDeleteCascader(txn, tableDesc, fkTables, evalCtx, alloc)
		if err != nil {
			return Deleter{}, err
		}
	}
	return rowDeleter, nil
}

// makeRowDeleterWithoutCascader creates a rowDeleter but does not create an
// additional cascader.
func makeRowDeleterWithoutCascader(
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	requestedCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	alloc *sqlbase.DatumAlloc,
) (Deleter, error) {
	indexes := tableDesc.DeletableIndexes()

	fetchCols := requestedCols[:len(requestedCols):len(requestedCols)]
	fetchColIDtoRowIndex := ColIDtoRowIndexFromCols(fetchCols)

	maybeAddCol := func(colID sqlbase.ColumnID) error {
		if _, ok := fetchColIDtoRowIndex[colID]; !ok {
			col, err := tableDesc.FindColumnByID(colID)
			if err != nil {
				return err
			}
			fetchColIDtoRowIndex[col.ID] = len(fetchCols)
			fetchCols = append(fetchCols, *col)
		}
		return nil
	}
	for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
		if err := maybeAddCol(colID); err != nil {
			return Deleter{}, err
		}
	}
	for _, index := range indexes {
		var haveFuncIndex bool
		if index.IsFunc != nil {
			for _, isFunc := range index.IsFunc {
				if isFunc {
					haveFuncIndex = true
					break
				}
			}
		}
		if haveFuncIndex {
			break
		}
		for _, colID := range index.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return Deleter{}, err
			}
		}
		// The extra columns are needed to fix #14601.
		for _, colID := range index.ExtraColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return Deleter{}, err
			}
		}
	}

	rd := Deleter{
		Helper:               newRowHelper(tableDesc, indexes),
		FetchCols:            fetchCols,
		FetchColIDtoRowIndex: fetchColIDtoRowIndex,
	}
	if checkFKs == CheckFKs {
		var err error
		if rd.Fks, err = makeFkExistenceCheckHelperForDelete(txn, tableDesc, fkTables,
			fetchColIDtoRowIndex, alloc); err != nil {
			return Deleter{}, err
		}
	}

	return rd, nil
}

// DeleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values. It also will cascade as required and check for
// orphaned rows. The bytesMonitor is only used if cascading/fk checking and can
// be nil if not.
func (rd *Deleter) DeleteRow(
	ctx context.Context,
	b *client.Batch,
	values []tree.Datum,
	checkFKs checkFKConstraints,
	traceKV bool,
) error {
	var ignoreIndexes util.FastIntSet
	primaryIndexKey, secondaryIndexEntries, err := rd.Helper.encodeIndexes(rd.FetchColIDtoRowIndex,
		values, ignoreIndexes)
	if err != nil {
		return err
	}
	values = values[:len(rd.FetchCols)]

	// Delete the row from any secondary indices.
	for i, secondaryIndexEntry := range secondaryIndexEntries {
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(rd.Helper.secIndexValDirs[i], secondaryIndexEntry.Key))
		}
		b.Del(&secondaryIndexEntry.Key)
	}

	// Delete the row.
	for i, family := range rd.Helper.TableDesc.Families {
		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}
		rd.key = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(rd.Helper.primIndexValDirs, rd.key))
		}
		b.Del(&rd.key)
		rd.key = nil
	}

	if rd.cascader != nil {
		if err := rd.cascader.cascadeAll(
			ctx,
			rd.Helper.TableDesc,
			tree.Datums(values),
			nil, /* updatedValues */
			rd.FetchColIDtoRowIndex,
			traceKV,
		); err != nil {
			return err
		}
	}
	if rd.Fks.checker != nil && checkFKs == CheckFKs {
		if err := rd.Fks.addAllIdxChecks(ctx, values, traceKV); err != nil {
			return err
		}
		return rd.Fks.checker.runCheck(ctx, values, nil)
	}
	return nil
}

// DeleteIndexRow adds to the batch the kv operations necessary to delete a
// table row from the given index.
func (rd *Deleter) DeleteIndexRow(
	ctx context.Context,
	b *client.Batch,
	idx *sqlbase.IndexDescriptor,
	values []tree.Datum,
	traceKV bool,
) error {
	if rd.Fks.checker != nil {
		if err := rd.Fks.addAllIdxChecks(ctx, values, traceKV); err != nil {
			return err
		}
		if err := rd.Fks.checker.runCheck(ctx, values, nil); err != nil {
			return err
		}
	}
	secondaryIndexEntry, err := sqlbase.EncodeSecondaryIndex(
		rd.Helper.TableDesc.TableDesc(), idx, rd.FetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	for _, entry := range secondaryIndexEntry {
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", entry.Key)
		}
		b.Del(entry.Key)
	}
	return nil
}

// ColIDtoRowIndexFromCols groups a slice of ColumnDescriptors by their ID
// field, returning a map from ID to ColumnDescriptor. It assumes there are no
// duplicate descriptors in the input.
func ColIDtoRowIndexFromCols(cols []sqlbase.ColumnDescriptor) map[sqlbase.ColumnID]int {
	colIDtoRowIndex := make(map[sqlbase.ColumnID]int, len(cols))
	for i, col := range cols {
		colIDtoRowIndex[col.ID] = i
	}
	return colIDtoRowIndex
}

//BuildKv1 convert tree.Datums to key-value
func (ri *Inserter) BuildKv1(
	ctx context.Context, i int, j int, values tree.Datums, kvs []sqlbase.KvRedundancy,
) {
	if len(values) != len(ri.InsertCols) {
		kvs[j] = sqlbase.KvRedundancy{
			Err: errors.Errorf("got %d values but expected %d", len(values), len(ri.InsertCols)),
		}
		return
	}

	if ri.Fks.checker != nil {
		if err := ri.Fks.addAllIdxChecks(ctx, values, false); err != nil {
			kvs[j] = sqlbase.KvRedundancy{
				Err: err,
			}
			return
		}
		if err := ri.Fks.checker.runCheck(ctx, nil, values); err != nil {
			kvs[j] = sqlbase.KvRedundancy{
				Err: err,
			}
			return
		}
	}

	var pm schemaexpr.PartialIndexUpdateHelper
	partialIndexOrds := ri.Helper.TableDesc.PartialIndexOrds()
	if !partialIndexOrds.Empty() {
		partialIndexPutVals := values[len(ri.InsertCols):]

		_ = pm.Init(partialIndexPutVals, tree.Datums{}, ri.Helper.TableDesc)

		// Truncate rowVals so that it no longer includes partial index predicate
		// values.
		values = values[:len(ri.InsertCols)]
	}
	primaryIndexKey, secondaryIndexEntries, err := ri.Helper.encodeIndexes1(ri.InsertColIDtoRowIndex, values, pm.IgnoreForPut)
	if err != nil {
		kvs[j] = sqlbase.KvRedundancy{
			Err: err,
		}
		return
	}

	for i, family := range ri.Helper.TableDesc.Families {
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := ri.InsertColIDtoRowIndex[colID]; ok {
				update = true
				break
			}
		}
		if !update {
			continue
		}

		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}

		var kvKey, rawValueBuf []byte

		kvKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))

		var lastColID sqlbase.ColumnID
		familySortedColumnIDs, ok := ri.Helper.sortedColumnFamily(family.ID)
		if !ok {
			kvs[j] = sqlbase.KvRedundancy{
				Err: pgerror.NewAssertionErrorf("invalid family sorted column id map"),
			}
			return
		}
		for _, colID := range familySortedColumnIDs {
			idx, ok := ri.InsertColIDtoRowIndex[colID]
			if !ok || values[idx] == tree.DNull {
				// Column not being updated or inserted.
				continue
			}

			if skip, err := ri.Helper.skipColumnInPK(colID, family.ID, values[idx]); err != nil {
				kvs[j] = sqlbase.KvRedundancy{
					Err: err,
				}
				return
			} else if skip {
				continue
			}

			col := ri.InsertCols[idx]

			if lastColID > col.ID {
				kvs[j] = sqlbase.KvRedundancy{
					Err: pgerror.NewAssertionErrorf("cannot write column id %d after %d", col.ID, lastColID),
				}
				return
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID
			var err error
			rawValueBuf, err = sqlbase.EncodeTableValue(rawValueBuf, colIDDiff, values[idx], nil)
			if err != nil {
				kvs[j] = sqlbase.KvRedundancy{
					Err: err,
				}
				return
			}
		}
		kvs[j] = sqlbase.KvRedundancy{
			KvKey:          kvKey,
			RawValueBuf:    rawValueBuf,
			SecondaryIndex: secondaryIndexEntries,
		}
	}

}

//InsertKv 通过调用CPut直接向batch中插入kv
func (ri *Inserter) InsertKv(ctx context.Context, b putter, kvRed sqlbase.KvRedundancy) error {
	if kvRed.Err != nil {
		return kvRed.Err
	}
	ri.value.SetTuple(kvRed.RawValueBuf)
	insertCPutFn(ctx, b, (*roachpb.Key)(&kvRed.KvKey), &ri.value, false)
	ri.value = roachpb.Value{}
	for i := range kvRed.SecondaryIndex {
		e := &kvRed.SecondaryIndex[i]
		insertInvertedPutFn(ctx, b, &e.Key, &e.Value, false)
	}
	return nil
}
