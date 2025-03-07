// Copyright 2017 The Cockroach Authors.
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
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// cascader is used to handle all referential integrity cascading actions.
type cascader struct {
	txn      *client.Txn
	fkTables FkTableMetadata
	alloc    *sqlbase.DatumAlloc
	evalCtx  *tree.EvalContext

	indexPKRowFetchers map[TableID]map[sqlbase.IndexID]Fetcher // PK RowFetchers by Table ID and Index ID

	// Row Deleters
	rowDeleters        map[TableID]Deleter                    // RowDeleters by Table ID
	deleterRowFetchers map[TableID]Fetcher                    // RowFetchers for rowDeleters by Table ID
	deletedRows        map[TableID]*rowcontainer.RowContainer // Rows that have been deleted by Table ID

	// Row Updaters
	rowUpdaters        map[TableID]Updater                    // RowUpdaters by Table ID
	updaterRowFetchers map[TableID]Fetcher                    // RowFetchers for rowUpdaters by Table ID
	originalRows       map[TableID]*rowcontainer.RowContainer // Original values for rows that have been updated by Table ID
	updatedRows        map[TableID]*rowcontainer.RowContainer // New values for rows that have been updated by Table ID
}

// makeDeleteCascader only creates a cascader if there is a chance that there is
// a possible cascade. It returns a cascader if one is required and nil if not.
func makeDeleteCascader(
	txn *client.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	tablesByID FkTableMetadata,
	evalCtx *tree.EvalContext,
	alloc *sqlbase.DatumAlloc,
) (*cascader, error) {
	if evalCtx == nil {
		return nil, pgerror.NewAssertionErrorf("evalContext is nil")
	}
	var required bool
Outer:
	for _, referencedIndex := range table.AllNonDropIndexes() {
		for _, ref := range referencedIndex.ReferencedBy {
			referencingTable, ok := tablesByID[ref.Table]
			if !ok {
				return nil, pgerror.NewAssertionErrorf("could not find table:%d in table descriptor map", ref.Table)
			}
			if referencingTable.IsAdding {
				// We can assume that a table being added but not yet public is empty,
				// and thus does not need to be checked for cascading.
				continue
			}
			referencingIndex, err := referencingTable.Desc.FindIndexByID(ref.Index)
			if err != nil {
				return nil, err
			}
			if referencingIndex.ForeignKey.OnDelete == sqlbase.ForeignKeyReference_CASCADE ||
				referencingIndex.ForeignKey.OnDelete == sqlbase.ForeignKeyReference_SET_DEFAULT ||
				referencingIndex.ForeignKey.OnDelete == sqlbase.ForeignKeyReference_SET_NULL {
				required = true
				break Outer
			}
		}
	}
	if !required {
		return nil, nil
	}
	return &cascader{
		txn:                txn,
		fkTables:           tablesByID,
		indexPKRowFetchers: make(map[TableID]map[sqlbase.IndexID]Fetcher),
		rowDeleters:        make(map[TableID]Deleter),
		deleterRowFetchers: make(map[TableID]Fetcher),
		deletedRows:        make(map[TableID]*rowcontainer.RowContainer),
		rowUpdaters:        make(map[TableID]Updater),
		updaterRowFetchers: make(map[TableID]Fetcher),
		originalRows:       make(map[TableID]*rowcontainer.RowContainer),
		updatedRows:        make(map[TableID]*rowcontainer.RowContainer),
		evalCtx:            evalCtx,
		alloc:              alloc,
	}, nil
}

// makeUpdateCascader only creates a cascader if there is a chance that there is
// a possible cascade. It returns a cascader if one is required and nil if not.
func makeUpdateCascader(
	txn *client.Txn,
	privCheckFn CheckPrivilegeFunction,
	table *sqlbase.ImmutableTableDescriptor,
	tablesByID FkTableMetadata,
	updateCols []sqlbase.ColumnDescriptor,
	evalCtx *tree.EvalContext,
	alloc *sqlbase.DatumAlloc,
) (*cascader, error) {
	if evalCtx == nil {
		return nil, pgerror.NewAssertionErrorf("evalContext is nil")
	}
	// if ON UPDATE CASCADE, check referenced columns privilege
	for _, referencedIndex := range table.AllNonDropIndexes() {
		for _, ref := range referencedIndex.ReferencedBy {
			referencingTable, ok := tablesByID[ref.Table]
			if !ok {
				return nil, pgerror.NewAssertionErrorf("could not find table:%d in table descriptor map", ref.Table)
			}
			referencingIndex, err := referencingTable.Desc.FindIndexByID(ref.Index)
			if err != nil {
				return nil, err
			}
			if referencingIndex.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_CASCADE {
				td := referencingTable.Desc
				cols := tree.NameList{}
				for _, cid := range referencingIndex.ColumnIDs {
					i := td.ColumnIdxMap()[cid]
					col := tree.Name(td.Columns[i].Name)
					cols = append(cols, col)
				}
				if privCheckFn != nil {
					if err := privCheckFn(evalCtx.Context, td.TableDesc(), cols, privilege.UPDATE); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	var required bool
	colIDs := make(map[sqlbase.ColumnID]struct{})
	for _, col := range updateCols {
		colIDs[col.ID] = struct{}{}
	}
Outer:
	for _, referencedIndex := range table.AllNonDropIndexes() {
		var match bool
		for _, colID := range referencedIndex.ColumnIDs {
			if _, exists := colIDs[colID]; exists {
				match = true
				break
			}
		}
		if !match {
			continue
		}
		for _, ref := range referencedIndex.ReferencedBy {
			referencingTable, ok := tablesByID[ref.Table]
			if !ok {
				return nil, pgerror.NewAssertionErrorf("could not find table:%d in table descriptor map", ref.Table)
			}
			if referencingTable.IsAdding {
				// We can assume that a table being added but not yet public is empty,
				// and thus does not need to be checked for cascading.
				continue
			}
			referencingIndex, err := referencingTable.Desc.FindIndexByID(ref.Index)
			if err != nil {
				return nil, err
			}
			if referencingIndex.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_CASCADE ||
				referencingIndex.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_SET_DEFAULT ||
				referencingIndex.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_SET_NULL {
				required = true
				break Outer
			}
		}
	}
	if !required {
		return nil, nil
	}
	return &cascader{
		txn:                txn,
		fkTables:           tablesByID,
		indexPKRowFetchers: make(map[TableID]map[sqlbase.IndexID]Fetcher),
		rowDeleters:        make(map[TableID]Deleter),
		deleterRowFetchers: make(map[TableID]Fetcher),
		deletedRows:        make(map[TableID]*rowcontainer.RowContainer),
		rowUpdaters:        make(map[TableID]Updater),
		updaterRowFetchers: make(map[TableID]Fetcher),
		originalRows:       make(map[TableID]*rowcontainer.RowContainer),
		updatedRows:        make(map[TableID]*rowcontainer.RowContainer),
		evalCtx:            evalCtx,
		alloc:              alloc,
	}, nil
}

func (c *cascader) clear(ctx context.Context) {
	for _, container := range c.deletedRows {
		container.Clear(ctx)
	}
	for _, container := range c.originalRows {
		container.Clear(ctx)
	}
	for _, container := range c.updatedRows {
		container.Clear(ctx)
	}
}

// spanForIndexValues creates a Span against an index to extract the primary
// keys needed for cascading.
func spanForIndexValues(
	table *sqlbase.ImmutableTableDescriptor,
	index *sqlbase.IndexDescriptor,
	prefixLen int,
	match sqlbase.ForeignKeyReference_Match,
	indexColIDs map[sqlbase.ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (roachpb.Span, error) {
	// See https://github.com/znbasedb/znbase/issues/20305 or
	// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
	// different composite foreign key matching methods.
	switch match {
	case sqlbase.ForeignKeyReference_SIMPLE:
		for _, rowIndex := range indexColIDs {
			if values[rowIndex] == tree.DNull {
				return roachpb.Span{}, nil
			}
		}
	case sqlbase.ForeignKeyReference_FULL:
		var nulls, notNulls bool
		for _, rowIndex := range indexColIDs {
			if values[rowIndex] == tree.DNull {
				nulls = true
			} else {
				notNulls = true
			}
			if nulls && notNulls {
				// TODO(bram): expand this error to show more details.
				return roachpb.Span{}, pgerror.NewErrorf(pgcode.ForeignKeyViolation,
					"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s",
					values,
				)
			}
		}
		// Only if all the values are all null should we skip the FK check for
		// MATCH FULL.
		if nulls {
			return roachpb.Span{}, nil
		}

	case sqlbase.ForeignKeyReference_PARTIAL:
		return roachpb.Span{}, pgerror.UnimplementedWithIssueError(20305, "MATCH PARTIAL not supported")

	default:
		return roachpb.Span{}, pgerror.NewAssertionErrorf("unknown composite key match type: %v", match)
	}
	span, _, err := sqlbase.EncodePartialIndexSpan(table.TableDesc(), index, prefixLen, indexColIDs, values, keyPrefix)
	if err != nil {
		return roachpb.Span{}, err
	}
	return span, nil
}

// batchRequestForIndexValues creates a batch request against an index to
// extract the primary keys needed for cascading. It also returns the
// colIDtoRowIndex that will map the columns that have been retrieved as part of
// the request to the referencing table.
func batchRequestForIndexValues(
	ctx context.Context,
	referencedIndex *sqlbase.IndexDescriptor,
	referencingTable *sqlbase.ImmutableTableDescriptor,
	referencingIndex *sqlbase.IndexDescriptor,
	match sqlbase.ForeignKeyReference_Match,
	values cascadeQueueElement,
	traceKV bool,
) (roachpb.BatchRequest, map[sqlbase.ColumnID]int, error) {

	//TODO(bram): consider caching some of these values
	keyPrefix := sqlbase.MakeIndexKeyPrefix(referencingTable.TableDesc(), referencingIndex.ID)
	prefixLen := len(referencingIndex.ColumnIDs)
	if len(referencedIndex.ColumnIDs) < prefixLen {
		prefixLen = len(referencedIndex.ColumnIDs)
	}

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int, len(referencedIndex.ColumnIDs))
	for i, referencedColID := range referencedIndex.ColumnIDs[:prefixLen] {
		if found, ok := values.colIDtoRowIndex[referencedColID]; ok {
			colIDtoRowIndex[referencingIndex.ColumnIDs[i]] = found
		} else {
			return roachpb.BatchRequest{}, nil, pgerror.NewErrorf(pgcode.ForeignKeyViolation,
				"missing value for column %q in multi-part foreign key", referencedIndex.ColumnNames[i],
			)
		}
	}

	var req roachpb.BatchRequest
	for i := values.startIndex; i < values.endIndex; i++ {
		span, err := spanForIndexValues(
			referencingTable,
			referencingIndex,
			prefixLen,
			match,
			colIDtoRowIndex,
			values.originalValues.At(i),
			keyPrefix,
		)
		if err != nil {
			return roachpb.BatchRequest{}, nil, err
		}
		if span.EndKey != nil {
			req.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeaderFromSpan(span)})
			if traceKV {
				log.VEventf(ctx, 2, "CascadeScan %s", span)
			}
		}
	}
	return req, colIDtoRowIndex, nil
}

// spanForPKValues creates a Span against the primary index of a table and is
// used to fetch rows for cascading.
func spanForPKValues(
	table *sqlbase.ImmutableTableDescriptor,
	fetchColIDtoRowIndex map[sqlbase.ColumnID]int,
	values tree.Datums,
) (roachpb.Span, error) {
	return spanForIndexValues(
		table,
		&table.PrimaryIndex,
		len(table.PrimaryIndex.ColumnIDs),
		sqlbase.ForeignKeyReference_SIMPLE, /* primary key lookup can always use MATCH SIMPLE */
		fetchColIDtoRowIndex,
		values,
		sqlbase.MakeIndexKeyPrefix(table.TableDesc(), table.PrimaryIndex.ID),
	)
}

// batchRequestForPKValues creates a batch request against the primary index of
// a table and is used to fetch rows for cascading.
func batchRequestForPKValues(
	ctx context.Context,
	table *sqlbase.ImmutableTableDescriptor,
	fetchColIDtoRowIndex map[sqlbase.ColumnID]int,
	values *rowcontainer.RowContainer,
	traceKV bool,
) (roachpb.BatchRequest, error) {
	var req roachpb.BatchRequest
	for i := 0; i < values.Len(); i++ {
		span, err := spanForPKValues(table, fetchColIDtoRowIndex, values.At(i))
		if err != nil {
			return roachpb.BatchRequest{}, err
		}
		if span.EndKey != nil {
			if traceKV {
				log.VEventf(ctx, 2, "CascadeScan %s", span)
			}
			req.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeaderFromSpan(span)})
		}
	}
	return req, nil
}

// addIndexPKRowFetch will create or load a cached row fetcher on an index to
// fetch the primary keys of the rows that will be affected by a cascading
// action.
func (c *cascader) addIndexPKRowFetcher(
	table *sqlbase.ImmutableTableDescriptor, index *sqlbase.IndexDescriptor,
) (Fetcher, error) {
	// Is there a cached row fetcher?
	rowFetchersForTable, exists := c.indexPKRowFetchers[table.ID]
	if exists {
		rowFetcher, exists := rowFetchersForTable[index.ID]
		if exists {
			return rowFetcher, nil
		}
	} else {
		c.indexPKRowFetchers[table.ID] = make(map[sqlbase.IndexID]Fetcher)
	}

	// Create a new row fetcher. Only the primary key columns are required.
	var colDesc []sqlbase.ColumnDescriptor
	for _, id := range table.PrimaryIndex.ColumnIDs {
		cDesc, err := table.FindColumnByID(id)
		if err != nil {
			return Fetcher{}, err
		}
		colDesc = append(colDesc, *cDesc)
	}
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(colDesc)-1)
	isSecondary := table.PrimaryIndex.ID != index.ID
	var rowFetcher Fetcher
	if err := rowFetcher.Init(
		false, /* reverse */
		false, /* returnRangeInfo */
		false, /* isCheck */
		c.alloc,
		sqlbase.ScanLockingStrength_FOR_NONE,
		sqlbase.ScanLockingWaitPolicy{LockLevel: sqlbase.ScanLockingWaitLevel_BLOCK},
		FetcherTableArgs{
			Desc:             table,
			Index:            index,
			ColIdxMap:        ColIDtoRowIndexFromCols(colDesc),
			IsSecondaryIndex: isSecondary,
			Cols:             colDesc,
			ValNeededForCol:  valNeededForCol,
		},
	); err != nil {
		return Fetcher{}, err
	}
	// Cache the row fetcher.
	c.indexPKRowFetchers[table.ID][index.ID] = rowFetcher
	return rowFetcher, nil
}

// addRowDeleter creates the row deleter and primary index row fetcher.
func (c *cascader) addRowDeleter(
	table *sqlbase.ImmutableTableDescriptor,
) (Deleter, Fetcher, error) {
	// Is there a cached row fetcher and deleter?
	if rowDeleter, exists := c.rowDeleters[table.ID]; exists {
		rowFetcher, existsFetcher := c.deleterRowFetchers[table.ID]
		if !existsFetcher {
			return Deleter{}, Fetcher{}, pgerror.NewAssertionErrorf("no corresponding row fetcher for the row deleter for table: (%d)%s",
				table.ID, table.Name,
			)
		}
		return rowDeleter, rowFetcher, nil
	}

	// Create the row deleter. The row deleter is needed prior to the row fetcher
	// as it will dictate what columns are required in the row fetcher.
	rowDeleter, err := makeRowDeleterWithoutCascader(
		c.txn,
		table,
		c.fkTables,
		nil, /* requestedCol */
		CheckFKs,
		c.alloc,
	)
	if err != nil {
		return Deleter{}, Fetcher{}, err
	}

	// Create the row fetcher that will retrive the rows and columns needed for
	// deletion.
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(rowDeleter.FetchCols)-1)
	tableArgs := FetcherTableArgs{
		Desc:             table,
		Index:            &table.PrimaryIndex,
		ColIdxMap:        rowDeleter.FetchColIDtoRowIndex,
		IsSecondaryIndex: false,
		Cols:             rowDeleter.FetchCols,
		ValNeededForCol:  valNeededForCol,
	}
	var rowFetcher Fetcher
	if err := rowFetcher.Init(
		false, /* reverse */
		false, /* returnRangeInfo */
		false, /* isCheck */
		c.alloc,
		sqlbase.ScanLockingStrength_FOR_NONE,
		sqlbase.ScanLockingWaitPolicy{LockLevel: sqlbase.ScanLockingWaitLevel_BLOCK},
		tableArgs,
	); err != nil {
		return Deleter{}, Fetcher{}, err
	}

	// Cache both the fetcher and deleter.
	c.rowDeleters[table.ID] = rowDeleter
	c.deleterRowFetchers[table.ID] = rowFetcher
	return rowDeleter, rowFetcher, nil
}

// addRowUpdater creates the row updater and primary index row fetcher.
func (c *cascader) addRowUpdater(
	table *sqlbase.ImmutableTableDescriptor,
) (Updater, Fetcher, error) {
	// Is there a cached updater?
	rowUpdater, existsUpdater := c.rowUpdaters[table.ID]
	if existsUpdater {
		rowFetcher, existsFetcher := c.updaterRowFetchers[table.ID]
		if !existsFetcher {
			return Updater{}, Fetcher{}, pgerror.NewAssertionErrorf("no corresponding row fetcher for the row updater for table: (%d)%s",
				table.ID, table.Name,
			)
		}
		return rowUpdater, rowFetcher, nil
	}

	// Create the row updater. The row updater requires all the columns in the
	// table.
	rowUpdater, err := makeUpdaterWithoutCascader(
		nil,
		c.txn,
		nil,
		table,
		c.fkTables,
		table.Columns,
		nil, /* requestedCol */
		UpdaterDefault,
		c.alloc,
	)
	if err != nil {
		return Updater{}, Fetcher{}, err
	}

	// Create the row fetcher that will retrive the rows and columns needed for
	// deletion.
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(rowUpdater.FetchCols)-1)
	tableArgs := FetcherTableArgs{
		Desc:             table,
		Index:            &table.PrimaryIndex,
		ColIdxMap:        rowUpdater.FetchColIDtoRowIndex,
		IsSecondaryIndex: false,
		Cols:             rowUpdater.FetchCols,
		ValNeededForCol:  valNeededForCol,
	}
	var rowFetcher Fetcher
	if err := rowFetcher.Init(
		false, /* reverse */
		false, /* returnRangeInfo */
		false, /* isCheck */
		c.alloc,
		sqlbase.ScanLockingStrength_FOR_NONE,
		sqlbase.ScanLockingWaitPolicy{LockLevel: sqlbase.ScanLockingWaitLevel_BLOCK},
		tableArgs,
	); err != nil {
		return Updater{}, Fetcher{}, err
	}

	// Cache the updater and the fetcher.
	c.rowUpdaters[table.ID] = rowUpdater
	c.updaterRowFetchers[table.ID] = rowFetcher
	return rowUpdater, rowFetcher, nil
}

// deleteRows performs row deletions on a single table for all rows that match
// the values. Returns the values of the rows that were deleted. This deletion
// happens in a single batch.
func (c *cascader) deleteRows(
	ctx context.Context,
	referencedIndex *sqlbase.IndexDescriptor,
	referencingTable *sqlbase.ImmutableTableDescriptor,
	referencingIndex *sqlbase.IndexDescriptor,
	match sqlbase.ForeignKeyReference_Match,
	values cascadeQueueElement,
	traceKV bool,
) (*rowcontainer.RowContainer, map[sqlbase.ColumnID]int, int, error) {
	// Create the Span to search for index values.
	// TODO(bram): This initial index lookup can be skipped if the index is the
	// primary index.
	if traceKV {
		log.VEventf(ctx, 2, "cascading delete into table: %d using index: %d",
			referencingTable.ID, referencingIndex.ID,
		)
	}

	// 及联删除需要生效
	if err := c.txn.Step(ctx); err != nil {
		return nil, nil, 0, err
	}

	req, _, err := batchRequestForIndexValues(
		ctx, referencedIndex, referencingTable, referencingIndex, match, values, traceKV,
	)
	if err != nil {
		return nil, nil, 0, err
	}
	// If there are no spans to search, there is no need to cascade.
	if len(req.Requests) == 0 {
		return nil, nil, 0, nil
	}
	br, roachErr := c.txn.Send(ctx, req)
	if roachErr != nil {
		return nil, nil, 0, roachErr.GoError()
	}

	// Create or retrieve the index pk row fetcher.
	indexPKRowFetcher, err := c.addIndexPKRowFetcher(referencingTable, referencingIndex)
	if err != nil {
		return nil, nil, 0, err
	}
	indexPKRowFetcherColIDToRowIndex := indexPKRowFetcher.tables[0].colIdxMap

	// Fetch all the primary keys that need to be deleted.
	// TODO(Bram): consider chunking this into n, primary keys, perhaps 100.
	pkColTypeInfo, err := sqlbase.MakeColTypeInfo(referencingTable, indexPKRowFetcherColIDToRowIndex)
	if err != nil {
		return nil, nil, 0, err
	}
	primaryKeysToDelete := rowcontainer.NewRowContainer(
		c.evalCtx.Mon.MakeBoundAccount(), pkColTypeInfo, values.originalValues.Len(),
	)
	defer primaryKeysToDelete.Close(ctx)

	for _, resp := range br.Responses {
		fetcher := SpanKVFetcher{
			KVs: resp.GetInner().(*roachpb.ScanResponse).Rows,
		}
		if err := indexPKRowFetcher.StartScanFrom(ctx, &fetcher); err != nil {
			return nil, nil, 0, err
		}
		for !indexPKRowFetcher.kvEnd {
			primaryKey, _, _, err := indexPKRowFetcher.NextRowDecoded(ctx)
			if err != nil {
				return nil, nil, 0, err
			}
			if _, err := primaryKeysToDelete.AddRow(ctx, primaryKey); err != nil {
				return nil, nil, 0, err
			}
		}
	}

	// Early exit if no rows need to be deleted.
	if primaryKeysToDelete.Len() == 0 {
		return nil, nil, 0, nil
	}

	// Create or retrieve the row deleter and primary index row fetcher.
	rowDeleter, pkRowFetcher, err := c.addRowDeleter(referencingTable)
	if err != nil {
		return nil, nil, 0, err
	}

	// Create a batch request to get all the spans of the primary keys that need
	// to be deleted.
	pkLookupReq, err := batchRequestForPKValues(
		ctx, referencingTable, indexPKRowFetcherColIDToRowIndex, primaryKeysToDelete, traceKV,
	)
	if err != nil {
		return nil, nil, 0, err
	}
	primaryKeysToDelete.Clear(ctx)
	// If there are no spans to search, there is no need to cascade.
	if len(pkLookupReq.Requests) == 0 {
		return nil, nil, 0, nil
	}
	pkResp, roachErr := c.txn.Send(ctx, pkLookupReq)
	if roachErr != nil {
		return nil, nil, 0, roachErr.GoError()
	}

	// Add the values to be checked for constraint violations after all cascading
	// changes have completed. Here either fetch or create the deleted rows
	// rowContainer for the table. This rowContainer for the table is also used by
	// the queue to avoid having to double the memory used.
	if _, exists := c.deletedRows[referencingTable.ID]; !exists {
		// Fetch the rows for deletion and store them in a container.
		colTypeInfo, err := sqlbase.MakeColTypeInfo(referencingTable, rowDeleter.FetchColIDtoRowIndex)
		if err != nil {
			return nil, nil, 0, err
		}
		c.deletedRows[referencingTable.ID] = rowcontainer.NewRowContainer(
			c.evalCtx.Mon.MakeBoundAccount(), colTypeInfo, primaryKeysToDelete.Len(),
		)
	}
	deletedRows := c.deletedRows[referencingTable.ID]
	deletedRowsStartIndex := deletedRows.Len()

	// Delete all the rows in a new batch.
	batch := c.txn.NewBatch()

	for _, resp := range pkResp.Responses {
		fetcher := SpanKVFetcher{
			KVs: resp.GetInner().(*roachpb.ScanResponse).Rows,
		}
		if err := pkRowFetcher.StartScanFrom(ctx, &fetcher); err != nil {
			return nil, nil, 0, err
		}
		for !pkRowFetcher.kvEnd {
			rowToDelete, _, _, err := pkRowFetcher.NextRowDecoded(ctx)
			if err != nil {
				return nil, nil, 0, err
			}

			// Add the row to be checked for consistency changes.
			if _, err := deletedRows.AddRow(ctx, rowToDelete); err != nil {
				return nil, nil, 0, err
			}

			// Delete the row.
			if err := rowDeleter.DeleteRow(ctx, batch, rowToDelete, SkipFKs, traceKV); err != nil {
				return nil, nil, 0, err
			}
		}
	}

	// Run the batch.
	if err := c.txn.Run(ctx, batch); err != nil {
		return nil, nil, 0, ConvertBatchError(ctx, referencingTable, batch)
	}

	return deletedRows, rowDeleter.FetchColIDtoRowIndex, deletedRowsStartIndex, nil
}

// updateRows performs row updates on a single table for all rows that match
// the values. Returns both the values of the rows that were updated and their
// new values. This update happens in a single batch.
func (c *cascader) updateRows(
	ctx context.Context,
	referencedIndex *sqlbase.IndexDescriptor,
	referencingTable *sqlbase.ImmutableTableDescriptor,
	referencingIndex *sqlbase.IndexDescriptor,
	match sqlbase.ForeignKeyReference_Match,
	values cascadeQueueElement,
	action sqlbase.ForeignKeyReference_Action,
	traceKV bool,
) (*rowcontainer.RowContainer, *rowcontainer.RowContainer, map[sqlbase.ColumnID]int, int, error) {
	// Create the Span to search for index values.
	if traceKV {
		log.VEventf(ctx, 2, "cascading update into table: %d using index: %d",
			referencingTable.ID, referencingIndex.ID,
		)
	}

	// 及联更新需要生效
	if err := c.txn.Step(ctx); err != nil {
		return nil, nil, nil, 0, err
	}

	// Create or retrieve the row updater and row fetcher.
	rowUpdater, rowFetcher, err := c.addRowUpdater(referencingTable)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	// Add the values to be checked for constraint violations after all cascading
	// changes have completed. Here either fetch or create the rowContainers for
	// both the original and updated values for the table and index combo. These
	// rowContainers for are also used by the queue to avoid having to double the
	// memory used.
	if _, exists := c.originalRows[referencingTable.ID]; !exists {
		colTypeInfo, err := sqlbase.MakeColTypeInfo(referencingTable, rowUpdater.FetchColIDtoRowIndex)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		c.originalRows[referencingTable.ID] = rowcontainer.NewRowContainer(
			c.evalCtx.Mon.MakeBoundAccount(), colTypeInfo, values.originalValues.Len(),
		)
		c.updatedRows[referencingTable.ID] = rowcontainer.NewRowContainer(
			c.evalCtx.Mon.MakeBoundAccount(), colTypeInfo, values.originalValues.Len(),
		)
	}
	originalRows := c.originalRows[referencingTable.ID]
	updatedRows := c.updatedRows[referencingTable.ID]
	startIndex := originalRows.Len()

	// Update all the rows in a new batch.
	batch := c.txn.NewBatch()

	// Populate a map of all columns that need to be set if the action is not
	// cascade.
	nullable := true
	var nullableColName string
	var referencingIndexValuesByColIDs map[sqlbase.ColumnID]tree.Datum
	switch action {
	case sqlbase.ForeignKeyReference_SET_NULL:
		referencingIndexValuesByColIDs = make(map[sqlbase.ColumnID]tree.Datum)
		for _, columnID := range referencingIndex.ColumnIDs {
			if colDesc, err := referencingTable.FindColumnByID(columnID); err == nil {
				if nullable {
					nullable = colDesc.Nullable
					nullableColName = colDesc.Name
				}
			}
			referencingIndexValuesByColIDs[columnID] = tree.DNull
		}
	case sqlbase.ForeignKeyReference_SET_DEFAULT:
		referencingIndexValuesByColIDs = make(map[sqlbase.ColumnID]tree.Datum)
		for _, columnID := range referencingIndex.ColumnIDs {
			column, err := referencingTable.FindColumnByID(columnID)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			parsedExpr, err := parser.ParseExpr(*column.DefaultExpr)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			typedExpr, err := tree.TypeCheck(parsedExpr, nil, column.Type.ToDatumType(), false)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			normalizedExpr, err := c.evalCtx.NormalizeExpr(typedExpr)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			referencingIndexValuesByColIDs[columnID], err = normalizedExpr.Eval(c.evalCtx)
			if err != nil {
				return nil, nil, nil, 0, err
			}
		}
	}

	// Sadly, this operation cannot be batched the same way as deletes, as the
	// values being updated will change based on both the original and updated
	// values.
	for i := values.startIndex; i < values.endIndex; i++ {
		// Extract a single value to update at a time.
		req, valueColIDtoRowIndex, err := batchRequestForIndexValues(
			ctx, referencedIndex, referencingTable, referencingIndex, match, cascadeQueueElement{
				startIndex:      i,
				endIndex:        i + 1,
				originalValues:  values.originalValues,
				updatedValues:   values.updatedValues,
				table:           values.table,
				colIDtoRowIndex: values.colIDtoRowIndex,
			},
			traceKV,
		)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		// If there are no spans to search, there is no need to cascade.
		if len(req.Requests) == 0 {
			return nil, nil, nil, 0, nil
		}
		br, roachErr := c.txn.Send(ctx, req)
		if roachErr != nil {
			return nil, nil, nil, 0, roachErr.GoError()
		}

		// Create or retrieve the index pk row fetcher.
		indexPKRowFetcher, err := c.addIndexPKRowFetcher(referencingTable, referencingIndex)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		indexPKRowFetcherColIDToRowIndex := indexPKRowFetcher.tables[0].colIdxMap

		// Fetch all the primary keys for rows that will be updated.
		// TODO(Bram): consider chunking this into n, primary keys, perhaps 100.
		pkColTypeInfo, err := sqlbase.MakeColTypeInfo(referencingTable, indexPKRowFetcherColIDToRowIndex)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		primaryKeysToUpdate := rowcontainer.NewRowContainer(
			c.evalCtx.Mon.MakeBoundAccount(), pkColTypeInfo, values.originalValues.Len(),
		)
		defer primaryKeysToUpdate.Close(ctx)

		for _, resp := range br.Responses {
			fetcher := SpanKVFetcher{
				KVs: resp.GetInner().(*roachpb.ScanResponse).Rows,
			}
			if err := indexPKRowFetcher.StartScanFrom(ctx, &fetcher); err != nil {
				return nil, nil, nil, 0, err
			}
			for !indexPKRowFetcher.kvEnd {
				primaryKey, _, _, err := indexPKRowFetcher.NextRowDecoded(ctx)
				if err != nil {
					return nil, nil, nil, 0, err
				}
				if _, err := primaryKeysToUpdate.AddRow(ctx, primaryKey); err != nil {
					return nil, nil, nil, 0, err
				}
			}
		}

		// Early exit if no rows need to be updated.
		if primaryKeysToUpdate.Len() == 0 {
			continue
		}

		if nullable == false {
			return nil, nil, nil, 0, pgerror.NewErrorf(pgcode.NotNullViolation, "null value in column \"%s\" violates not-null constraint", nullableColName)
		}

		// Create a batch request to get all the spans of the primary keys that need
		// to be updated.
		pkLookupReq, err := batchRequestForPKValues(
			ctx, referencingTable, indexPKRowFetcherColIDToRowIndex, primaryKeysToUpdate, traceKV,
		)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		primaryKeysToUpdate.Clear(ctx)
		// If there are no spans to search, there is no need to cascade.
		if len(pkLookupReq.Requests) == 0 {
			return nil, nil, nil, 0, nil
		}
		pkResp, roachErr := c.txn.Send(ctx, pkLookupReq)
		if roachErr != nil {
			return nil, nil, nil, 0, roachErr.GoError()
		}

		for _, resp := range pkResp.Responses {
			fetcher := SpanKVFetcher{
				KVs: resp.GetInner().(*roachpb.ScanResponse).Rows,
			}
			if err := rowFetcher.StartScanFrom(ctx, &fetcher); err != nil {
				return nil, nil, nil, 0, err
			}
			for !rowFetcher.kvEnd {
				rowToUpdate, _, _, err := rowFetcher.NextRowDecoded(ctx)
				if err != nil {
					return nil, nil, nil, 0, err
				}

				updateRow := make(tree.Datums, len(rowUpdater.UpdateColIDtoRowIndex))
				switch action {
				case sqlbase.ForeignKeyReference_CASCADE:
					// Create the updateRow based on the passed in updated values and from
					// the retrieved row as a fallback.
					currentUpdatedValue := values.updatedValues.At(i)
					for colID, rowIndex := range rowUpdater.UpdateColIDtoRowIndex {
						if valueRowIndex, exists := valueColIDtoRowIndex[colID]; exists {
							updateRow[rowIndex] = currentUpdatedValue[valueRowIndex]
							if updateRow[rowIndex] == tree.DNull {
								column, err := referencingTable.FindColumnByID(colID)
								if err != nil {
									return nil, nil, nil, 0, err
								}
								if !column.Nullable {
									tbName, err := sqlbase.GetTableName(ctx, c.txn, referencingTable.ParentID, referencingTable.Name)
									if err != nil {
										return nil, nil, nil, 0, err
									}
									return nil, nil, nil, 0, pgerror.NewErrorf(pgcode.NullValueNotAllowed,
										"cannot cascade a null value into %q as it violates a NOT NULL constraint",
										tree.ErrString(tree.NewUnresolvedName(tbName.Catalog(), tbName.Schema(), referencingTable.Name, column.Name)))
								}
							}
							continue
						}
						if fetchRowIndex, exists := rowUpdater.FetchColIDtoRowIndex[colID]; exists {
							updateRow[rowIndex] = rowToUpdate[fetchRowIndex]
							continue
						}
						return nil, nil, nil, 0, pgerror.NewAssertionErrorf("could find find colID %d in either updated columns or the fetched row",
							colID,
						)
					}
				case sqlbase.ForeignKeyReference_SET_NULL, sqlbase.ForeignKeyReference_SET_DEFAULT:
					// Create the updateRow based on the original values and for all
					// values in the index, either nulls (for SET NULL), or default (for
					// SET DEFAULT).
					for colID, rowIndex := range rowUpdater.UpdateColIDtoRowIndex {
						if value, exists := referencingIndexValuesByColIDs[colID]; exists {
							updateRow[rowIndex] = value
							continue
						}
						if fetchRowIndex, exists := rowUpdater.FetchColIDtoRowIndex[colID]; exists {
							updateRow[rowIndex] = rowToUpdate[fetchRowIndex]
							continue
						}
						return nil, nil, nil, 0, pgerror.NewAssertionErrorf("could find find colID %d in either the index columns or the fetched row",
							colID,
						)
					}
				}

				// Is there something to update?  If not, skip it.
				if !rowToUpdate.IsDistinctFrom(c.evalCtx, updateRow) {
					continue
				}

				var pm schemaexpr.PartialIndexUpdateHelper
				updatedRow, err := rowUpdater.UpdateRow(
					ctx,
					batch,
					rowToUpdate,
					updateRow,
					SkipFKs,
					traceKV,
					pm,
				)
				if err != nil {
					return nil, nil, nil, 0, err
				}
				if _, err := originalRows.AddRow(ctx, rowToUpdate); err != nil {
					return nil, nil, nil, 0, err
				}
				if _, err := updatedRows.AddRow(ctx, updatedRow); err != nil {
					return nil, nil, nil, 0, err
				}
			}
		}
	}
	if err := c.txn.Run(ctx, batch); err != nil {
		return nil, nil, nil, 0, ConvertBatchError(ctx, referencingTable, batch)
	}

	return originalRows, updatedRows, rowUpdater.FetchColIDtoRowIndex, startIndex, nil
}

type cascadeQueueElement struct {
	table *sqlbase.ImmutableTableDescriptor
	// These row containers are defined elsewhere and their memory is not managed
	// by the queue. The updated values can be nil for deleted rows. If it does
	// exist, every row in originalValues must have a corresponding row in
	// updatedValues at the exact same index. They also must have the exact same
	// rank.
	originalValues  *rowcontainer.RowContainer
	updatedValues   *rowcontainer.RowContainer
	colIDtoRowIndex map[sqlbase.ColumnID]int
	startIndex      int // Start of the range of rows in the row container.
	endIndex        int // End of the range of rows (exclusive) in the row container.
}

// cascadeQueue is used for a breadth first walk of the referential integrity
// graph.
type cascadeQueue []cascadeQueueElement

// EnqueueTxn adds a range of values in a row container to the queue. Note that
// it always assumes that all the values start at the startIndex and extend to
// all the rows following that index.
func (q *cascadeQueue) enqueue(
	ctx context.Context,
	table *sqlbase.ImmutableTableDescriptor,
	originalValues *rowcontainer.RowContainer,
	updatedValues *rowcontainer.RowContainer,
	colIDtoRowIndex map[sqlbase.ColumnID]int,
	startIndex int,
) error {
	*q = append(*q, cascadeQueueElement{
		table:           table,
		originalValues:  originalValues,
		updatedValues:   updatedValues,
		colIDtoRowIndex: colIDtoRowIndex,
		startIndex:      startIndex,
		endIndex:        originalValues.Len(),
	})
	return nil
}

func (q *cascadeQueue) dequeue() (cascadeQueueElement, bool) {
	if len(*q) == 0 {
		return cascadeQueueElement{}, false
	}
	elem := (*q)[0]
	*q = (*q)[1:]
	return elem, true
}

// cascadeAll performs all required cascading operations, then checks all the
// remaining indexes to ensure that no orphans were created.
func (c *cascader) cascadeAll(
	ctx context.Context,
	table *sqlbase.ImmutableTableDescriptor,
	originalValues tree.Datums,
	updatedValues tree.Datums,
	colIDtoRowIndex map[sqlbase.ColumnID]int,
	traceKV bool,
) error {
	defer c.clear(ctx)
	var cascadeQ cascadeQueue

	// EnqueueTxn the first values.
	colTypeInfo, err := sqlbase.MakeColTypeInfo(table, colIDtoRowIndex)
	if err != nil {
		return err
	}
	originalRowContainer := rowcontainer.NewRowContainer(
		c.evalCtx.Mon.MakeBoundAccount(), colTypeInfo, len(originalValues),
	)
	defer originalRowContainer.Close(ctx)
	if _, err := originalRowContainer.AddRow(ctx, originalValues); err != nil {
		return err
	}
	var updatedRowContainer *rowcontainer.RowContainer
	if updatedValues != nil {
		updatedRowContainer = rowcontainer.NewRowContainer(
			c.evalCtx.Mon.MakeBoundAccount(), colTypeInfo, len(updatedValues),
		)
		defer updatedRowContainer.Close(ctx)
		if _, err := updatedRowContainer.AddRow(ctx, updatedValues); err != nil {
			return err
		}
	}
	if err := cascadeQ.enqueue(
		ctx, table, originalRowContainer, updatedRowContainer, colIDtoRowIndex, 0,
	); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return sqlbase.QueryCanceledError
		default:
		}
		elem, exists := cascadeQ.dequeue()
		if !exists {
			break
		}
		for _, referencedIndex := range elem.table.AllNonDropIndexes() {
			for _, ref := range referencedIndex.ReferencedBy {
				referencingTable, ok := c.fkTables[ref.Table]
				if !ok {
					return pgerror.NewAssertionErrorf("could not find table:%d in table descriptor map", ref.Table)
				}
				if referencingTable.IsAdding {
					// We can assume that a table being added but not yet public is empty,
					// and thus does not need to be checked for cascading.
					continue
				}
				referencingIndex, err := referencingTable.Desc.FindIndexByID(ref.Index)
				if err != nil {
					return err
				}
				if elem.updatedValues == nil {
					// Deleting a row.
					switch referencingIndex.ForeignKey.OnDelete {
					case sqlbase.ForeignKeyReference_CASCADE:
						deletedRows, colIDtoRowIndex, startIndex, err := c.deleteRows(
							ctx,
							referencedIndex,
							referencingTable.Desc,
							referencingIndex,
							ref.Match,
							elem,
							traceKV,
						)
						if err != nil {
							return err
						}
						if deletedRows != nil && deletedRows.Len() > startIndex {
							// If a row was deleted, add the table to the queue.
							if err := cascadeQ.enqueue(
								ctx,
								referencingTable.Desc,
								deletedRows,
								nil, /* updatedValues */
								colIDtoRowIndex,
								startIndex,
							); err != nil {
								return err
							}
						}
					case sqlbase.ForeignKeyReference_SET_NULL, sqlbase.ForeignKeyReference_SET_DEFAULT:
						originalAffectedRows, updatedAffectedRows, colIDtoRowIndex, startIndex, err := c.updateRows(
							ctx,
							referencedIndex,
							referencingTable.Desc,
							referencingIndex,
							ref.Match,
							elem,
							referencingIndex.ForeignKey.OnDelete,
							traceKV,
						)
						if err != nil {
							return err
						}
						if originalAffectedRows != nil && originalAffectedRows.Len() > startIndex {
							// A row was updated, so let's add it to the queue.
							if err := cascadeQ.enqueue(
								ctx,
								referencingTable.Desc,
								originalAffectedRows,
								updatedAffectedRows,
								colIDtoRowIndex,
								startIndex,
							); err != nil {
								return err
							}
						}
					}
				} else {
					// Updating a row.
					switch referencingIndex.ForeignKey.OnUpdate {
					case sqlbase.ForeignKeyReference_CASCADE, sqlbase.ForeignKeyReference_SET_NULL, sqlbase.ForeignKeyReference_SET_DEFAULT:
						originalAffectedRows, updatedAffectedRows, colIDtoRowIndex, startIndex, err := c.updateRows(
							ctx,
							referencedIndex,
							referencingTable.Desc,
							referencingIndex,
							ref.Match,
							elem,
							referencingIndex.ForeignKey.OnUpdate,
							traceKV,
						)
						if err != nil {
							return err
						}
						if originalAffectedRows != nil && originalAffectedRows.Len() > startIndex {
							// A row was updated, so let's add it to the queue.
							if err := cascadeQ.enqueue(
								ctx,
								referencingTable.Desc,
								originalAffectedRows,
								updatedAffectedRows,
								colIDtoRowIndex,
								startIndex,
							); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}

	// Check all foreign key constraints that have been affected by the cascading
	// operation.

	// Check all deleted rows to ensure there are no orphans.
	for tableID, deletedRows := range c.deletedRows {
		if deletedRows.Len() == 0 {
			continue
		}
		rowDeleter, exists := c.rowDeleters[tableID]
		if !exists {
			return pgerror.NewAssertionErrorf("could not find row deleter for table %d", tableID)
		}
		for deletedRows.Len() > 0 {
			if err := rowDeleter.Fks.addAllIdxChecks(ctx, deletedRows.At(0), traceKV); err != nil {
				return err
			}
			if err := rowDeleter.Fks.checker.runCheck(ctx, deletedRows.At(0), nil); err != nil {
				return err
			}
			deletedRows.PopFirst()
		}
	}

	// Check all updated rows for orphans.
	// TODO(bram): This is running more checks than needed and may be a bit
	// brittle. All of these checks can be done selectively by storing a list of
	// checks to perform for each updated row which can be compiled while
	// cascading and performing them directly without relying on the rowUpdater.
	// There is also an opportunity to batch more of these checks together.
	for tableID, rowUpdater := range c.rowUpdaters {
		// Fetch the original and updated rows for the updater.
		originalRows, originalRowsExists := c.originalRows[tableID]
		if !originalRowsExists {
			return pgerror.NewAssertionErrorf("could not find original rows for table %d", tableID)
		}
		totalRows := originalRows.Len()
		if totalRows == 0 {
			continue
		}

		updatedRows, updatedRowsExists := c.updatedRows[tableID]
		if !updatedRowsExists {
			return pgerror.NewAssertionErrorf("could not find updated rows for table %d", tableID)
		}

		if totalRows != updatedRows.Len() {
			return pgerror.NewAssertionErrorf("original rows length:%d not equal to updated rows length:%d for table %d",
				totalRows, updatedRows.Len(), tableID,
			)
		}

		if totalRows == 1 {
			// If there's only a single change, which is quite often the case, there
			// is no need to worry about intermediate states.  Just run the check and
			// avoid a bunch of allocations.
			if err := rowUpdater.Fks.addIndexChecks(ctx, originalRows.At(0), updatedRows.At(0), traceKV); err != nil {
				return err
			}
			if !rowUpdater.Fks.hasFKs() {
				continue
			}
			if err := rowUpdater.Fks.checker.runCheck(ctx, originalRows.At(0), updatedRows.At(0)); err != nil {
				return err
			}
			// Now check all check constraints for the table.
			checkHelper := c.fkTables[tableID].CheckHelper
			if checkHelper != nil {
				if err := checkHelper.LoadEvalRow(rowUpdater.UpdateColIDtoRowIndex, updatedRows.At(0), false); err != nil {
					return err
				}
				if err := checkHelper.CheckEval(c.evalCtx); err != nil {
					return err
				}
			}
			continue
		}

		skipList := make(map[int]struct{}) // A map of already checked indices.
		for i := 0; i < totalRows; i++ {
			if _, exists := skipList[i]; exists {
				continue
			}

			// Is this the final update for this row? Intermediate states will always
			// fail these checks so only check the original and final update for the
			// row.
			finalRow := updatedRows.At(i)
			for j := i + 1; j < totalRows; j++ {
				if _, exists := skipList[j]; exists {
					continue
				}
				if !originalRows.At(j).IsDistinctFrom(c.evalCtx, finalRow) {
					// The row has been updated again.
					finalRow = updatedRows.At(j)
					skipList[j] = struct{}{}
				}
			}

			if err := rowUpdater.Fks.addIndexChecks(ctx, originalRows.At(i), finalRow, traceKV); err != nil {
				return err
			}
			if !rowUpdater.Fks.hasFKs() {
				continue
			}
			if err := rowUpdater.Fks.checker.runCheck(ctx, originalRows.At(i), finalRow); err != nil {
				return err
			}
			// Now check all check constraints for the table.
			checkHelper := c.fkTables[tableID].CheckHelper
			if checkHelper != nil {
				if err := checkHelper.LoadEvalRow(rowUpdater.UpdateColIDtoRowIndex, finalRow, false); err != nil {
					return err
				}
				if err := checkHelper.CheckEval(c.evalCtx); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
