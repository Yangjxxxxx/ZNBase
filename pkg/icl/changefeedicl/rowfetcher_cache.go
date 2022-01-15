// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	"context"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// rowFetcherCache maintains a cache of single table RowFetchers. Given a key
// with an mvcc timestamp, it retrieves the correct TableDescriptor for that key
// and returns a Fetcher initialized with that table. This Fetcher's
// StartScanFrom can be used to turn that key (or all the keys making up the
// column families of one row) into a row.
type rowFetcherCache struct {
	leaseMgr *sql.LeaseManager
	fetchers map[*sqlbase.ImmutableTableDescriptor]*row.Fetcher

	a sqlbase.DatumAlloc
}

func newRowFetcherCache(leaseMgr *sql.LeaseManager) *rowFetcherCache {
	return &rowFetcherCache{
		leaseMgr: leaseMgr,
		fetchers: make(map[*sqlbase.ImmutableTableDescriptor]*row.Fetcher),
	}
}

func (c *rowFetcherCache) TableDescForKey(
	ctx context.Context, key roachpb.Key, ts hlc.Timestamp,
) (*sqlbase.ImmutableTableDescriptor, error) {
	var tableDesc *sqlbase.ImmutableTableDescriptor
	for skippedCols := 0; ; {
		remaining, tableID, _, err := sqlbase.DecodeTableIDIndexID(key)
		if err != nil {
			return nil, err
		}
		// No caching of these are attempted, since the lease manager does its
		// own caching.
		tableDesc, _, err = c.leaseMgr.Acquire(ctx, ts, tableID)
		if err != nil {
			return nil, err
		}
		if modTime := tableDesc.GetModificationTime(); modTime.IsEmpty() && ts.IsEmpty() && tableDesc.GetVersion() > 1 {
			log.Fatalf(context.TODO(), "read table descriptor for %q (%d) without ModificationTime "+
				"with zero MVCC timestamp", tableDesc.GetName(), tableDesc.GetID())
		} else if modTime.IsEmpty() {
			tableDesc.ModificationTime = ts
		} else if !ts.IsEmpty() && ts.Less(modTime) {
			log.Fatalf(context.TODO(), "read table descriptor %q (%d) which has a ModificationTime "+
				"after its MVCC timestamp: has %v, expected %v",
				tableDesc.GetName(), tableDesc.GetID(), modTime, ts)
		}
		// Immediately release the lease, since we only need it for the exact
		// timestamp requested.
		if err := c.leaseMgr.Release(tableDesc); err != nil {
			return nil, err
		}

		// Skip over the column data.
		for ; skippedCols < len(tableDesc.PrimaryIndex.ColumnIDs); skippedCols++ {
			l, err := encoding.PeekLength(remaining)
			if err != nil {
				return nil, err
			}
			remaining = remaining[l:]
		}
		var interleaved bool
		remaining, interleaved = encoding.DecodeIfInterleavedSentinel(remaining)
		if !interleaved {
			break
		}
		key = remaining
	}

	return tableDesc, nil
}

func (c *rowFetcherCache) RowFetcherForTableDesc(
	tableDesc *sqlbase.ImmutableTableDescriptor,
) (*row.Fetcher, error) {
	if rf, ok := c.fetchers[tableDesc]; ok {
		return rf, nil
	}

	// TODO(dan): Allow for decoding a subset of the columns.
	colIdxMap := make(map[sqlbase.ColumnID]int)
	var valNeededForCol util.FastIntSet
	for colIdx, col := range tableDesc.Columns {
		colIdxMap[col.ID] = colIdx
		valNeededForCol.Add(colIdx)
	}

	var rf row.Fetcher
	if err := rf.Init(
		false /* reverse */, false,
		/* returnRangeInfo */ false /* isCheck */, &c.a,
		sqlbase.ScanLockingStrength_FOR_NONE,
		sqlbase.ScanLockingWaitPolicy{LockLevel: sqlbase.ScanLockingWaitLevel_BLOCK},
		row.FetcherTableArgs{
			Spans:            tableDesc.AllIndexSpans(),
			Desc:             tableDesc,
			Index:            &tableDesc.PrimaryIndex,
			ColIdxMap:        colIdxMap,
			IsSecondaryIndex: false,
			Cols:             tableDesc.Columns,
			ValNeededForCol:  valNeededForCol,
		},
	); err != nil {
		return nil, err
	}
	// TODO(dan): Bound the size of the cache. Resolved notifications will let
	// us evict anything for timestamps entirely before the notification. Then
	// probably an LRU just in case?
	c.fetchers[tableDesc] = &rf
	return &rf, nil
}
