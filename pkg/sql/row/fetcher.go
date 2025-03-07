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
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/scrub"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// DebugRowFetch can be used to turn on some low-level debugging logs. We use
// this to avoid using log.V in the hot path.
const DebugRowFetch = false

type kvBatchFetcher interface {
	// nextBatch returns the next batch of rows. Returns false in the first
	// parameter if there are no more keys in the scan. May return either a slice
	// of KeyValues or a batchResponse, numKvs pair, depending on the server
	// version - both must be handled by calling code.
	nextBatch(ctx context.Context) (ok bool, kvs []roachpb.KeyValue,
		batchResponse []byte, origSpan roachpb.Span, err error)
	GetRangesInfo() []roachpb.RangeInfo
}

//TableInfo is Table information
type TableInfo struct {
	// -- Fields initialized once --

	// Used to determine whether a key retrieved belongs to the Span we
	// want to scan.
	spans            roachpb.Spans
	desc             *sqlbase.ImmutableTableDescriptor
	index            *sqlbase.IndexDescriptor
	isSecondaryIndex bool
	indexColumnDirs  []sqlbase.IndexDescriptor_Direction
	// equivSignature is an equivalence class for each unique table-index
	// pair. It allows us to check if an index key belongs to a given
	// table-index.
	equivSignature []byte

	// The table columns to use for fetching, possibly including ones currently in
	// schema changes.
	cols []sqlbase.ColumnDescriptor

	// The set of ColumnIDs that are required.
	neededCols util.FastIntSet

	// The set of indexes into the cols array that are required for columns
	// in the value part.
	neededValueColsByIdx util.FastIntSet

	// The number of needed columns from the value part of the row. Once we've
	// seen this number of value columns for a particular row, we can stop
	// decoding values in that row.
	neededValueCols int

	// Map used to get the index for columns in cols.
	colIdxMap map[sqlbase.ColumnID]int

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	indexColIdx []int

	// knownPrefixLength is the number of bytes in the index key prefix this
	// Fetcher is configured for. The index key prefix is the table id, index
	// id pair at the start of the key.
	knownPrefixLength int

	// -- Fields updated during a scan --

	keyValTypes []sqlbase.ColumnType
	extraTypes  []sqlbase.ColumnType
	keyVals     []sqlbase.EncDatum
	extraVals   []sqlbase.EncDatum
	row         sqlbase.EncDatumRow
	decodedRow  tree.Datums

	// The following fields contain MVCC metadata for each row and may be
	// returned to users of Fetcher immediately after NextRow returns.
	// They're not important to ordinary consumers of Fetcher that only
	// concern themselves with actual SQL row data.
	//
	// rowLastModified is the timestamp of the last time any family in the row
	// was modified in any way.
	rowLastModified hlc.Timestamp
	// rowIsDeleted is true when the row has been deleted. This is only
	// meaningful when kv deletion tombstones are returned by the kvBatchFetcher,
	// which the one used by `StartScan` (the common case) doesnt. Notably,
	// changefeeds use this by providing raw kvs with tombstones unfiltered via
	// `StartScanFrom`.
	rowIsDeleted bool

	// hasLast indicates whether there was a previously scanned k/v.
	hasLast bool
	// lastDatums is a buffer for the current key. It is only present when
	// doing a physical check in order to verify round-trip encoding.
	// It is required because Fetcher.kv is overwritten before NextRow
	// returns.
	lastKV roachpb.KeyValue
	// lastDatums is a buffer for the previously scanned k/v datums. It is
	// only present when doing a physical check in order to verify
	// ordering.
	lastDatums tree.Datums
}

//Getdesc is to Get Desc
func (tI TableInfo) Getdesc() *sqlbase.ImmutableTableDescriptor {
	return tI.desc
}

//Getindex is to Get index
func (tI TableInfo) Getindex() *sqlbase.IndexDescriptor {
	return tI.index
}

//GetisSecondaryIndex is to Get isSecondaryIndex
func (tI TableInfo) GetisSecondaryIndex() bool {
	return tI.isSecondaryIndex
}

//Getrow is to Get row
func (tI TableInfo) Getrow() sqlbase.EncDatumRow {
	return tI.row
}

//SetlastKV is to Set lastKV
func (tI TableInfo) SetlastKV(kv roachpb.KeyValue) {
	tI.lastKV = kv
}

// FetcherTableArgs are the arguments passed to Fetcher.Init
// for a given table that includes descriptors and row information.
type FetcherTableArgs struct {
	// The spans of keys to return for the given table. Fetcher
	// ignores keys outside these spans.
	// This is irrelevant if Fetcher is initialize with only one
	// table.
	Spans            roachpb.Spans
	Desc             *sqlbase.ImmutableTableDescriptor
	Index            *sqlbase.IndexDescriptor
	ColIdxMap        map[sqlbase.ColumnID]int
	IsSecondaryIndex bool
	Cols             []sqlbase.ColumnDescriptor
	// The indexes (0 to # of columns - 1) of the columns to return.
	ValNeededForCol util.FastIntSet
}

// Fetcher handles fetching kvs and forming table rows for an
// arbitrary number of tables.
// Usage:
//   var rf Fetcher
//   err := rf.Init(..)
//   // Handle err
//   err := rf.StartScan(..)
//   // Handle err
//   for {
//      res, err := rf.NextRow()
//      // Handle err
//      if res.row == nil {
//         // Done
//         break
//      }
//      // Process res.row
//   }
type Fetcher struct {
	// tables is a slice of all the tables and their descriptors for which
	// rows are returned.
	tables []TableInfo

	Count       chan int
	SwitchCount bool
	// allEquivSignatures is a map used for checking if an equivalence
	// signature belongs to any table or table's ancestor. It also maps the
	// string representation of every table's and every table's ancestors'
	// signature to the table's index in 'tables' for lookup during decoding.
	// If 2+ tables share the same ancestor signature, allEquivSignatures
	// will map the signature to the largest 'tables' index.
	// The full signature for a given table in 'tables' will always map to
	// its own index in 'tables'.
	allEquivSignatures map[string]int

	// reverse denotes whether or not the spans should be read in reverse
	// or not when StartScan is invoked.
	reverse bool

	// maxKeysPerRow memoizes the maximum number of keys per row
	// out of all the tables. This is used to calculate the kvBatchFetcher's
	// firstBatchLimit.
	maxKeysPerRow int

	// True if the index key must be decoded.
	// If there is more than one table, the index key must always be decoded.
	// This is only false if there are no needed columns and the (single)
	// table has no interleave children.
	mustDecodeIndexKey bool

	// returnRangeInfo, if set, causes the underlying kvBatchFetcher to return
	// information about the ranges descriptors/leases uses in servicing the
	// requests. This has some cost, so it's only enabled by DistSQL when this
	// info is actually useful for correcting the plan (e.g. not for the PK-side
	// of an index-join).
	// If set, GetRangeInfo() can be used to retrieve the accumulated info.
	returnRangeInfo bool

	// lockStrength represents the row-level locking mode to use when fetching
	// rows.
	// lockStrength表示在获取行时要使用的行级锁定模式。
	lockStrength sqlbase.ScanLockingStrength

	// lockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	// lockWaitPolicy表示用于处理其他活动事务持有的冲突锁的策略。
	lockWaitPolicy sqlbase.ScanLockingWaitPolicy

	// traceKV indicates whether or not session tracing is enabled. It is set
	// when beginning a new scan.
	traceKV bool

	// -- Fields updated during a scan --

	kvFetcher      *KVFetcher
	indexKey       []byte // the index key of the current row
	prettyValueBuf *bytes.Buffer

	valueColsFound int // how many needed cols we've found so far in the value

	rowReadyTable *TableInfo // the table for which a row was fully decoded and ready for output
	currentTable  *TableInfo // the most recent table for which a key was decoded
	keySigBuf     []byte     // buffer for the index key's signature
	keyRestBuf    []byte     // buffer for the rest of the index key that is not part of the signature

	// The current key/value, unless kvEnd is true.
	Kv                roachpb.KeyValue
	keyRemainingBytes []byte
	kvEnd             bool

	// isCheck indicates whether or not we are running checks for k/v
	// correctness. It is set only during SCRUB commands.
	isCheck bool

	// Buffered allocation of decoded datums.
	alloc      *sqlbase.DatumAlloc
	BlockBatch chan roachpb.BlockStruct
	// ReplicationTable indicates whether the fetch operation is for the replicated table.
	ReplicationTable bool
}

//GetmustDecodeIndexKey get mustDecodeIndexKey
func (rf *Fetcher) GetmustDecodeIndexKey() bool {
	return rf.mustDecodeIndexKey
}

//GetTraceKV get TraceKV
func (rf *Fetcher) GetTraceKV() bool {
	return rf.traceKV
}

//GetIndexKey get IndexKey
func (rf *Fetcher) GetIndexKey() []byte {
	return rf.indexKey
}

//SetIndexKey set IndexKey
func (rf *Fetcher) SetIndexKey(b []byte) {
	rf.indexKey = nil
}

//GetRowReadyTable Get RowReadyTable
func (rf *Fetcher) GetRowReadyTable() *TableInfo {
	return rf.rowReadyTable
}

//GetCurrentTable Get CurrentTable
func (rf *Fetcher) GetCurrentTable() *TableInfo {
	return rf.currentTable
}

//SetKeyRemainingBytes Set KeyRemainingBytes
func (rf *Fetcher) SetKeyRemainingBytes(b []byte) {
	rf.keyRemainingBytes = b

}

//GetKvEnd Get KvEnd
func (rf *Fetcher) GetKvEnd() bool {
	return rf.kvEnd
}

//SetKvEnd Set KvEnd
func (rf *Fetcher) SetKvEnd(kv bool) {
	rf.kvEnd = kv
}

//GetIsCheck Get IsCheck
func (rf *Fetcher) GetIsCheck() bool {
	return rf.isCheck
}

// Reset resets this Fetcher, preserving the memory capacity that was used
// for the tables slice, and the slices within each of the tableInfo objects
// within tables. This permits reuse of this objects without forcing total
// reallocation of all of those slice fields.
func (rf *Fetcher) Reset() {
	*rf = Fetcher{
		tables: rf.tables[:0],
	}
}

// Init sets up a Fetcher for a given table and index. If we are using a
// non-primary index, tables.ValNeededForCol can only refer to columns in the
// index.
func (rf *Fetcher) Init(
	reverse, returnRangeInfo bool,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
	lockStrength sqlbase.ScanLockingStrength,
	lockWaitPolicy sqlbase.ScanLockingWaitPolicy,
	tables ...FetcherTableArgs,
) error {
	if len(tables) == 0 {
		panic("no tables to fetch from")
	}

	rf.reverse = reverse
	rf.lockStrength = lockStrength
	rf.lockWaitPolicy = lockWaitPolicy
	rf.returnRangeInfo = returnRangeInfo
	rf.alloc = alloc
	rf.isCheck = isCheck

	// We must always decode the index key if we need to distinguish between
	// rows from more than one table.
	nTables := len(tables)
	multipleTables := nTables >= 2
	rf.mustDecodeIndexKey = multipleTables
	if multipleTables {
		rf.allEquivSignatures = make(map[string]int, len(tables))
	}

	if cap(rf.tables) >= nTables {
		rf.tables = rf.tables[:nTables]
	} else {
		rf.tables = make([]TableInfo, nTables)
	}
	for tableIdx, tableArgs := range tables {
		oldTable := rf.tables[tableIdx]

		table := TableInfo{
			spans:            tableArgs.Spans,
			desc:             tableArgs.Desc,
			colIdxMap:        tableArgs.ColIdxMap,
			index:            tableArgs.Index,
			isSecondaryIndex: tableArgs.IsSecondaryIndex,
			cols:             tableArgs.Cols,
			row:              make(sqlbase.EncDatumRow, len(tableArgs.Cols)),
			decodedRow:       make(tree.Datums, len(tableArgs.Cols)),

			// These slice fields might get re-allocated below, so reslice them from
			// the old table here in case they've got enough capacity already.
			indexColIdx: oldTable.indexColIdx[:0],
			keyVals:     oldTable.keyVals[:0],
			extraVals:   oldTable.extraVals[:0],
		}

		var err error
		if multipleTables {
			// We produce references to every signature's reference.
			equivSignatures, err := sqlbase.TableEquivSignatures(table.desc.TableDesc(), table.index)
			if err != nil {
				return err
			}
			for i, sig := range equivSignatures {
				// We always map the table's equivalence signature (last
				// 'sig' in 'equivSignatures') to its tableIdx.
				// This allows us to overwrite previous "ancestor
				// signatures" (see below).
				if i == len(equivSignatures)-1 {
					rf.allEquivSignatures[string(sig)] = tableIdx
					break
				}
				// Map each table's ancestors' signatures to -1 so
				// we know during ReadIndexKey if the parsed index
				// key belongs to ancestor or one of our tables.
				// We must check if the signature has already been set
				// since it's possible for a later 'table' to have an
				// ancestor that is a previous 'table', and we do not
				// want to overwrite the previous table's tableIdx.
				if _, exists := rf.allEquivSignatures[string(sig)]; !exists {
					rf.allEquivSignatures[string(sig)] = -1
				}
			}
			// The last signature is the given table's equivalence signature.
			table.equivSignature = equivSignatures[len(equivSignatures)-1]
		}

		isfunc := false
		for _, index := range table.desc.Indexes {
			for _, is := range index.IsFunc {
				if is {
					isfunc = true
				}
			}
		}

		// Scan through the entire columns map to see which columns are
		// required.
		for col, idx := range table.colIdxMap {
			if tableArgs.ValNeededForCol.Contains(idx) {
				// The idx-th column is required.
				table.neededCols.Add(int(col))
			} else if isfunc {
				table.neededCols.Add(int(col))
			}
		}

		table.knownPrefixLength = len(sqlbase.MakeIndexKeyPrefix(table.desc.TableDesc(), table.index.ID))

		var indexColumnIDs []sqlbase.ColumnID
		indexColumnIDs, table.indexColumnDirs = table.index.FullColumnIDs()

		table.neededValueColsByIdx = tableArgs.ValNeededForCol.Copy()
		neededIndexCols := 0
		nIndexCols := len(indexColumnIDs)
		if cap(table.indexColIdx) >= nIndexCols {
			table.indexColIdx = table.indexColIdx[:nIndexCols]
		} else {
			table.indexColIdx = make([]int, nIndexCols)
		}
		for i, id := range indexColumnIDs {
			colIdx, ok := table.colIdxMap[id]
			if ok {
				table.indexColIdx[i] = colIdx
				if table.neededCols.Contains(int(id)) {
					neededIndexCols++
					table.neededValueColsByIdx.Remove(colIdx)
				}
			} else {
				table.indexColIdx[i] = -1
				if table.neededCols.Contains(int(id)) {
					panic(fmt.Sprintf("needed column %d not in colIdxMap", id))
				}
			}
		}

		// - If there is more than one table, we have to decode the index key to
		//   figure out which table the row belongs to.
		// - If there are interleaves, we need to read the index key in order to
		//   determine whether this row is actually part of the index we're scanning.
		// - If there are needed columns from the index key, we need to read it.
		//
		// Otherwise, we can completely avoid decoding the index key.
		if !rf.mustDecodeIndexKey && (neededIndexCols > 0 || len(table.index.InterleavedBy) > 0 || len(table.index.Interleave.Ancestors) > 0) {
			rf.mustDecodeIndexKey = true
		}

		// The number of columns we need to read from the value part of the key.
		// It's the total number of needed columns minus the ones we read from the
		// index key, except for composite columns.
		table.neededValueCols = table.neededCols.Len() - neededIndexCols + len(table.index.CompositeColumnIDs)

		if table.isSecondaryIndex {
			for i := range table.cols {
				if table.neededCols.Contains(int(table.cols[i].ID)) && !table.index.ContainsColumnID(table.cols[i].ID) && !isfunc {
					return fmt.Errorf("requested column %s not in index", table.cols[i].Name)
				}
			}
		}

		// Prepare our index key vals slice.

		table.keyValTypes, err = sqlbase.GetIndexColumnTypes(table.desc.TableDesc(), indexColumnIDs, table.index)
		if err != nil {
			return err
		}
		if cap(table.keyVals) >= nIndexCols {
			table.keyVals = table.keyVals[:nIndexCols]
		} else {
			table.keyVals = make([]sqlbase.EncDatum, nIndexCols)
		}

		if hasExtraCols(&table) {
			// Unique secondary indexes have a value that is the
			// primary index key.
			// Primary indexes only contain ascendingly-encoded
			// values. If this ever changes, we'll probably have to
			// figure out the directions here too.
			table.extraTypes, err = sqlbase.GetColumnTypes(table.desc.TableDesc(), table.index.ExtraColumnIDs)
			nExtraColumns := len(table.index.ExtraColumnIDs)
			if cap(table.extraVals) >= nExtraColumns {
				table.extraVals = table.extraVals[:nExtraColumns]
			} else {
				table.extraVals = make([]sqlbase.EncDatum, nExtraColumns)
			}
			if err != nil {
				return err
			}
		}

		// Keep track of the maximum keys per row to accommodate a
		// limitHint when StartScan is invoked.
		if keysPerRow := table.desc.KeysPerRow(table.index.ID); keysPerRow > rf.maxKeysPerRow {
			rf.maxKeysPerRow = keysPerRow
		}

		rf.tables[tableIdx] = table
	}

	if len(tables) == 1 {
		// If there is more than one table, currentTable will be
		// updated every time NextKey is invoked and rowReadyTable
		// will be updated when a row is fully decoded.
		rf.currentTable = &(rf.tables[0])
		rf.rowReadyTable = &(rf.tables[0])
	}

	for i, table := range rf.tables {
		if table.index.IsFunc != nil {
			for j, isfuncindexcol := range table.index.IsFunc {
				if isfuncindexcol {
					rf.tables[i].neededCols.Add(int(table.index.ColumnIDs[j]))
				}
			}
		}
	}

	return nil
}

// MakeCount for table opt count
func (rf *Fetcher) MakeCount() {
	rf.Count = make(chan int, 1)
}

// CountRet for table opt count
func (rf *Fetcher) CountRet() int {
	return <-rf.Count
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
func (rf *Fetcher) StartScan(
	ctx context.Context,
	txn *client.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	if len(spans) == 0 {
		panic("no spans")
	}

	rf.traceKV = traceKV

	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := limitHint
	if firstBatchLimit != 0 {
		// The limitHint is a row limit, but each row could be made up
		// of more than one key. We take the maximum possible keys
		// per row out of all the table rows we could potentially
		// scan over.
		firstBatchLimit = limitHint * int64(rf.maxKeysPerRow)
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	f, err := makeKVBatchFetcher(
		txn,
		spans,
		rf.reverse,
		limitBatches,
		firstBatchLimit,
		rf.returnRangeInfo,
		rf.lockStrength,
		rf.lockWaitPolicy,
	)
	if err != nil {
		return err
	}
	f.ReplicationTable = rf.ReplicationTable
	return rf.StartScanFrom(ctx, &f)
}

// StartInconsistentScan 初始化并启动一个不一致的扫描，其中每个
// KV批次可以在不同的历史时间戳读取。
// 扫描使用初始时间戳，直到它变得比
// maxTimestampAge;此时，时间戳将被时间量取代
// 已经过去了。有关更多信息，请参阅TableReaderSpec的文档
// 细节。
//
//可以多次使用。
func (rf *Fetcher) StartInconsistentScan(
	ctx context.Context,
	db *client.DB,
	initialTimestamp hlc.Timestamp,
	maxTimestampAge time.Duration,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	if len(spans) == 0 {
		return errors.Errorf("no spans")
	}

	txnTimestamp := initialTimestamp
	txnStartTime := timeutil.Now()
	if txnStartTime.Sub(txnTimestamp.GoTime()) >= maxTimestampAge {
		return errors.Errorf(
			"AS OF SYSTEM TIME: cannot specify timestamp older than %s for this operation",
			maxTimestampAge,
		)
	}
	txn := client.NewTxnWithSteppingEnabled(ctx, db, 0 /* gatewayNodeID */, client.RootTxn)
	txn.SetFixedTimestamp(ctx, txnTimestamp)
	if log.V(1) {
		log.Infof(ctx, "starting inconsistent scan at timestamp %v", txnTimestamp)
	}

	sendFn := func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
		if now := timeutil.Now(); now.Sub(txnTimestamp.GoTime()) >= maxTimestampAge {
			// Time to bump the transaction. First commit the old one (should be a no-op).
			if err := txn.Commit(ctx); err != nil {
				return nil, err
			}
			// Advance the timestamp by the time that passed.
			txnTimestamp = txnTimestamp.Add(now.Sub(txnStartTime).Nanoseconds(), 0 /* logical */)
			txnStartTime = now
			txn = client.NewTxnWithSteppingEnabled(ctx, db, 0 /* gatewayNodeID */, client.RootTxn)
			txn.SetFixedTimestamp(ctx, txnTimestamp)

			if log.V(1) {
				log.Infof(ctx, "bumped inconsistent scan timestamp to %v", txnTimestamp)
			}
		}

		res, err := txn.Send(ctx, ba)
		if err != nil {
			return nil, err.GoError()
		}
		return res, nil
	}

	// TODO(radu): we should commit the last txn. Right now the commit is a no-op
	// on read transactions, but perhaps one day it will release some resources.

	rf.traceKV = traceKV
	f, err := makeKVBatchFetcherWithSendFunc(
		sendFunc(sendFn),
		spans,
		rf.reverse,
		limitBatches,
		rf.firstBatchLimit(limitHint),
		rf.returnRangeInfo,
		rf.lockStrength,
		rf.lockWaitPolicy,
	)
	if err != nil {
		return err
	}
	return rf.StartScanFrom(ctx, &f)
}

// KeyToDesc implements the KeyToDescTranslator interface. The implementation is
// used by ConvertFetchError.
// KeyToDesc实现KeyToDescTranslator接口。 该实现由ConvertFetchError使用。
func (rf *Fetcher) KeyToDesc(key roachpb.Key) (*sqlbase.ImmutableTableDescriptor, bool) {
	if rf.currentTable != nil && len(key) < rf.currentTable.knownPrefixLength {
		return nil, false
	}
	if _, ok, err := rf.ReadIndexKey(key); !ok || err != nil {
		return nil, false
	}
	return rf.currentTable.desc, true
}

func (rf *Fetcher) firstBatchLimit(limitHint int64) int64 {
	if limitHint == 0 {
		return 0
	}
	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	// The limitHint is a row limit, but each row could be made up of more than
	// one key. We take the maximum possible keys per row out of all the table
	// rows we could potentially scan over.
	//
	// We add an extra key to make sure we form the last row.
	return limitHint*int64(rf.maxKeysPerRow) + 1
}

// StartScanFrom initializes and starts a scan from the given kvBatchFetcher. Can be
// used multiple times.
func (rf *Fetcher) StartScanFrom(ctx context.Context, f kvBatchFetcher) error {
	rf.indexKey = nil
	rf.kvFetcher = newKVFetcher(f)
	// Retrieve the first key.
	_, err := rf.NextKey(ctx)
	return err
}

// NextKey retrieves the next key/value and sets kv/kvEnd. Returns whether a row
// has been completed.
func (rf *Fetcher) NextKey(ctx context.Context) (rowDone bool, err error) {
	var ok bool

	for {
		ok, rf.Kv, _, err = rf.kvFetcher.NextKV(ctx)
		if err != nil {
			return false, ConvertFetchError(ctx, rf, err)
		}
		rf.kvEnd = !ok
		if rf.kvEnd {
			// No more keys in the scan. We need to transition
			// rf.rowReadyTable to rf.currentTable for the last
			// row.
			rf.rowReadyTable = rf.currentTable
			return true, nil
		}

		// See Init() for a detailed description of when we can get away with not
		// reading the index key.
		if rf.mustDecodeIndexKey || rf.traceKV {
			rf.keyRemainingBytes, ok, err = rf.ReadIndexKey(rf.Kv.Key)
			if err != nil {
				return false, err
			}
			if !ok {
				// The key did not match any of the table
				// descriptors, which means it's interleaved
				// data from some other table or index.
				continue
			}
		} else {
			// We still need to consume the key until the family
			// id, so processKV can know whether we've finished a
			// row or not.
			prefixLen, err := keys.GetRowPrefixLength(rf.Kv.Key)
			if err != nil {
				return false, err
			}
			rf.keyRemainingBytes = rf.Kv.Key[prefixLen:]
		}

		// For unique secondary indexes, the index-key does not distinguish one row
		// from the next if both rows contain identical values along with a NULL.
		// Consider the keys:
		//
		//   /test/unique_idx/NULL/0
		//   /test/unique_idx/NULL/1
		//
		// The index-key extracted from the above keys is /test/unique_idx/NULL. The
		// trailing /0 and /1 are the primary key used to unique-ify the keys when a
		// NULL is present. Currently we don't detect NULLs on decoding. If we did
		// we could detect this case and enlarge the index-key. A simpler fix for
		// this problem is to simply always output a row for each key scanned from a
		// secondary index as secondary indexes have only one key per row.
		// If rf.rowReadyTable differs from rf.currentTable, this denotes
		// a row is ready for output.
		switch {
		case rf.currentTable.isSecondaryIndex:
			// Secondary indexes have only one key per row.
			rowDone = true
		case !bytes.HasPrefix(rf.Kv.Key, rf.indexKey):
			// If the prefix of the key has changed, current key is from a different
			// row than the previous one.
			rowDone = true
		case rf.rowReadyTable != rf.currentTable:
			// For rowFetchers with more than one table, if the table changes the row
			// is done.
			rowDone = true
		default:
			rowDone = false
		}

		if rf.indexKey != nil && rowDone {
			// The current key belongs to a new row. Output the
			// current row.
			rf.indexKey = nil
			return true, nil
		}

		return false, nil
	}
}

//NextKey1 retrieves the next key/value and sets kv/kvEnd. Returns whether a row
//has been completed.
func (rf *Fetcher) NextKey1(ctx context.Context) (rowDone bool, err error) {
	var ok bool
	var first bool
	for {
		first, ok, rf.Kv, err = rf.kvFetcher.nextKV1(ctx, rf.BlockBatch)

		if err != nil {
			return false, ConvertFetchError(ctx, rf, err)
		}
		rf.kvEnd = !ok
		if rf.kvEnd {
			//rf.rowReadyTable = rf.currentTable
			return true, nil
		}

		// See Init() for a detailed description of when we can get away with not
		// reading the index key.
		if rf.mustDecodeIndexKey || rf.traceKV {
			rf.keyRemainingBytes, ok, err = rf.ReadIndexKey(rf.Kv.Key)
			if err != nil {
				return false, err
			}
			if !ok {
				// The key did not match any of the table
				// descriptors, which means it's interleaved
				// data from some other table or index.
				continue
			}
		} else {
			// We still need to consume the key until the family
			// id, so processKV can know whether we've finished a
			// row or not.
			prefixLen, err := keys.GetRowPrefixLength(rf.Kv.Key)
			if err != nil {
				return false, err
			}
			rf.keyRemainingBytes = rf.Kv.Key[prefixLen:]
		}

		if first && rf.rowReadyTable.desc.TableDesc().ParentID == 50 {
			rf.indexKey = nil
			return true, nil
		}

		// For unique secondary indexes, the index-key does not distinguish one row
		// from the next if both rows contain identical values along with a NULL.
		// Consider the keys:
		//
		//   /test/unique_idx/NULL/0
		//   /test/unique_idx/NULL/1
		//
		// The index-key extracted from the above keys is /test/unique_idx/NULL. The
		// trailing /0 and /1 are the primary key used to unique-ify the keys when a
		// NULL is present. Currently we don't detect NULLs on decoding. If we did
		// we could detect this case and enlarge the index-key. A simpler fix for
		// this problem is to simply always output a row for each key scanned from a
		// secondary index as secondary indexes have only one key per row.
		// If rf.rowReadyTable differs from rf.currentTable, this denotes
		// a row is ready for output.
		switch {
		case first && rf.rowReadyTable.desc.TableDesc().Name != "jobs":
			return true, nil
		case !first && rf.rowReadyTable.desc.TableDesc().Name == "jobs":
			return true, nil
		case rf.currentTable.isSecondaryIndex:
			// Secondary indexes have only one key per row.
			rowDone = true
		case !bytes.HasPrefix(rf.Kv.Key, rf.indexKey):
			// If the prefix of the key has changed, current key is from a different
			// row than the previous one.
			rowDone = true
		case rf.rowReadyTable != rf.currentTable:
			// For rowFetchers with more than one table, if the table changes the row
			// is done.
			rowDone = true
		default:
			rowDone = false
		}

		if rf.indexKey != nil && rowDone {
			// The current key belongs to a new row. Output the
			// current row.
			rf.indexKey = nil
			return true, nil
		}

		return false, nil
	}
}

func (rf *Fetcher) prettyEncDatums(types []sqlbase.ColumnType, vals []sqlbase.EncDatum) string {
	var buf bytes.Buffer
	for i, v := range vals {
		if err := v.EnsureDecoded(&types[i], rf.alloc); err != nil {
			_, _ = fmt.Fprintf(&buf, "error decoding: %v", err)
		}
		_, _ = fmt.Fprintf(&buf, "/%v", v.Datum)
	}
	return buf.String()
}

// ReadIndexKey decodes an index key for a given table.
// It returns whether or not the key is for any of the tables initialized
// in Fetcher, and the remaining part of the key if it is.
func (rf *Fetcher) ReadIndexKey(key roachpb.Key) (remaining []byte, ok bool, err error) {
	// If there is only one table to check keys for, there is no need
	// to go through the equivalence signature checks.
	if len(rf.tables) == 1 {
		return sqlbase.DecodeIndexKeyWithoutTableIDIndexIDPrefix(
			rf.currentTable.desc.TableDesc(),
			rf.currentTable.index,
			rf.currentTable.keyValTypes,
			rf.currentTable.keyVals,
			rf.currentTable.indexColumnDirs,
			key[rf.currentTable.knownPrefixLength:],
		)
	}

	// Make a copy of the initial key for validating whether it's within
	// the table's specified spans.
	initialKey := key

	// key now contains the bytes in the key (if match) that are not part
	// of the signature in order.
	tableIdx, key, match, err := sqlbase.IndexKeyEquivSignature(key, rf.allEquivSignatures, rf.keySigBuf, rf.keyRestBuf)
	if err != nil {
		return nil, false, err
	}
	// The index key does not belong to our table because either:
	// !match:	    part of the index key's signature did not match any of
	//		    rf.allEquivSignatures.
	// tableIdx == -1:  index key belongs to an ancestor.
	if !match || tableIdx == -1 {
		return nil, false, nil
	}

	// The index key is not within our specified Span of keys for the
	// particular table.
	// TODO(richardwu): ContainsKey checks every Span within spans. We
	// can check that spans is ordered (or sort it) and memoize
	// the last Span we've checked for each table. We can pass in this
	// information to ContainsKey as a hint for which Span to start
	// checking first.
	if !rf.tables[tableIdx].spans.ContainsKey(initialKey) {
		return nil, false, nil
	}

	// Either a new table is encountered or the rowReadyTable differs from
	// the currentTable (the rowReadyTable was outputted in the previous
	// read). We transition the references.
	if &rf.tables[tableIdx] != rf.currentTable || rf.rowReadyTable != rf.currentTable {
		rf.rowReadyTable = rf.currentTable
		rf.currentTable = &rf.tables[tableIdx]

		// rf.rowReadyTable is nil if this is the very first key.
		// We want to ensure this does not differ from rf.currentTable
		// to prevent another transition.
		if rf.rowReadyTable == nil {
			rf.rowReadyTable = rf.currentTable
		}
	}

	// We can simply decode all the column values we retrieved
	// when processing the index key. The column values are at the
	// front of the key.
	if key, err = sqlbase.DecodeKeyVals(
		rf.currentTable.keyValTypes,
		rf.currentTable.keyVals,
		rf.currentTable.indexColumnDirs,
		key,
	); err != nil {
		return nil, false, err
	}

	return key, true, nil
}

// ProcessKV processes the given key/value, setting values in the row
// accordingly. If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *Fetcher) ProcessKV(
	ctx context.Context, kv roachpb.KeyValue,
) (prettyKey string, prettyValue string, err error) {
	table := rf.currentTable

	if rf.traceKV {
		prettyKey = fmt.Sprintf(
			"/%s/%s%s",
			table.desc.Name,
			table.index.Name,
			rf.prettyEncDatums(table.keyValTypes, table.keyVals),
		)
	}

	// Either this is the first key of the fetch or the first key of a new
	// row.
	if rf.indexKey == nil {
		// This is the first key for the row.
		rf.indexKey = []byte(kv.Key[:len(kv.Key)-len(rf.keyRemainingBytes)])

		// Reset the row to nil; it will get filled in with the column
		// values as we decode the key-value pairs for the row.
		// We only need to reset the needed columns in the value component, because
		// non-needed columns are never set and key columns are unconditionally set
		// below.
		for idx, ok := table.neededValueColsByIdx.Next(0); ok; idx, ok = table.neededValueColsByIdx.Next(idx + 1) {
			table.row[idx].UnsetDatum()
		}

		// Fill in the column values that are part of the index key.
		for i := range table.keyVals {
			if idx := table.indexColIdx[i]; idx != -1 {
				table.row[idx] = table.keyVals[i]
			}
		}

		rf.valueColsFound = 0

		// Reset the MVCC metadata for the next row.

		// set rowLastModified to a sentinel that's before any real timestamp.
		// As kvs are iterated for this row, it keeps track of the greatest
		// timestamp seen.
		table.rowLastModified = hlc.Timestamp{}
		// All row encodings (both before and after column families) have a
		// sentinel kv (column family 0) that is always present when a row is
		// present, even if that row is all NULLs. Thus, a row is deleted if and
		// only if the first kv in it a tombstone (RawBytes is empty).
		table.rowIsDeleted = len(kv.Value.RawBytes) == 0
	}

	if table.rowLastModified.Less(kv.Value.Timestamp) {
		table.rowLastModified = kv.Value.Timestamp
	}

	if table.neededCols.Empty() {
		// We don't need to decode any values.
		if rf.traceKV {
			prettyValue = tree.DNull.String()
		}
		return prettyKey, prettyValue, nil
	}

	// For covering secondary indexes, allow for decoding as a primary key.
	// if !table.isSecondaryIndex && len(rf.keyRemainingBytes) > 0
	if (!table.isSecondaryIndex || table.index.EncodingType == sqlbase.PrimaryIndexEncoding) &&
		len(rf.keyRemainingBytes) > 0 {
		// If familyID is 0, kv.Value contains values for composite key columns.
		// These columns already have a table.row value assigned above, but that value
		// (obtained from the key encoding) might not be correct (e.g. for decimals,
		// it might not contain the right number of trailing 0s; for collated
		// strings, it is one of potentially many strings with the same collation
		// key).
		//
		// In these cases, the correct value will be present in family 0 and the
		// table.row value gets overwritten.

		switch kv.Value.GetTag() {
		case roachpb.ValueType_TUPLE:
			// In this case, we don't need to decode the column family ID, because
			// the ValueType_TUPLE encoding includes the column id with every encoded
			// column value.
			prettyKey, prettyValue, err = rf.processValueTuple(ctx, table, kv, prettyKey)
		default:
			var familyID uint64
			_, familyID, err = encoding.DecodeUvarintAscending(rf.keyRemainingBytes)
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
			}

			var family *sqlbase.ColumnFamilyDescriptor
			family, err = table.desc.FindFamilyByID(sqlbase.FamilyID(familyID))
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
			}

			prettyKey, prettyValue, err = rf.processValueSingle(ctx, table, family, kv, prettyKey)
		}
		if err != nil {
			return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
		}
	} else {
		tag := kv.Value.GetTag()
		var valueBytes []byte
		switch tag {
		case roachpb.ValueType_BYTES:
			valueBytes, err = kv.Value.GetBytes()
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}

			if hasExtraCols(table) {
				// This is a unique secondary index; decode the extra
				// column values from the value.
				var err error
				valueBytes, err = sqlbase.DecodeKeyVals(
					table.extraTypes,
					table.extraVals,
					nil,
					valueBytes,
				)
				if err != nil {
					return "", "", scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
				}
				for i, id := range table.index.ExtraColumnIDs {
					if table.neededCols.Contains(int(id)) {
						table.row[table.colIdxMap[id]] = table.extraVals[i]
					}
				}
				if rf.traceKV {
					prettyValue = rf.prettyEncDatums(table.extraTypes, table.extraVals)
				}
			}
		case roachpb.ValueType_TUPLE:
			valueBytes, err = kv.Value.GetTuple()
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}

		if DebugRowFetch {
			if hasExtraCols(table) {
				log.Infof(ctx, "Scan %s -> %s", kv.Key, rf.prettyEncDatums(table.extraTypes, table.extraVals))
			} else {
				log.Infof(ctx, "Scan %s", kv.Key)
			}
		}

		if len(valueBytes) > 0 {
			prettyKey, prettyValue, err = rf.processValueBytes(
				ctx, table, kv, valueBytes, prettyKey,
			)
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}
	}

	if rf.traceKV && prettyValue == "" {
		prettyValue = tree.DNull.String()
	}

	return prettyKey, prettyValue, nil
}

// processValueSingle processes the given value (of column
// family.DefaultColumnID), setting values in table.row accordingly. The key is
// only used for logging.
func (rf *Fetcher) processValueSingle(
	ctx context.Context,
	table *TableInfo,
	family *sqlbase.ColumnFamilyDescriptor,
	kv roachpb.KeyValue,
	prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix

	// If this is the row sentinel (in the legacy pre-family format),
	// a value is not expected, so we're done.
	if family.ID == 0 {
		return "", "", nil
	}

	colID := family.DefaultColumnID
	if colID == 0 {
		return "", "", errors.Errorf("single entry value with no default column id")
	}

	if rf.traceKV || table.neededCols.Contains(int(colID)) {
		if idx, ok := table.colIdxMap[colID]; ok {
			if rf.traceKV {
				prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.Columns[idx].Name)
			}
			if len(kv.Value.RawBytes) == 0 {
				return prettyKey, "", nil
			}
			typ := table.cols[idx].Type
			// TODO(arjun): The value is a directly marshaled single value, so we
			// unmarshal it eagerly here. This can potentially be optimized out,
			// although that would require changing UnmarshalColumnValue to operate
			// on bytes, and for Encode/DecodeTableValue to operate on marshaled
			// single values.
			value, err := sqlbase.UnmarshalColumnValue(rf.alloc, typ, kv.Value)
			if err != nil {
				return "", "", err
			}
			if rf.traceKV {
				prettyValue = value.String()
			}
			table.row[idx] = sqlbase.DatumToEncDatum(typ, value)
			if DebugRowFetch {
				log.Infof(ctx, "Scan %s -> %v", kv.Key, value)
			}
			return prettyKey, prettyValue, nil
		}
	}

	// No need to unmarshal the column value. Either the column was part of
	// the index key or it isn't needed.
	if DebugRowFetch {
		log.Infof(ctx, "Scan %s -> [%d] (skipped)", kv.Key, colID)
	}
	return prettyKey, prettyValue, nil
}

func (rf *Fetcher) processValueBytes(
	ctx context.Context,
	table *TableInfo,
	kv roachpb.KeyValue,
	valueBytes []byte,
	prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if rf.traceKV {
		if rf.prettyValueBuf == nil {
			rf.prettyValueBuf = &bytes.Buffer{}
		}
		rf.prettyValueBuf.Reset()
	}

	var colIDDiff uint32
	var lastColID sqlbase.ColumnID
	var typeOffset, dataOffset int
	var typ encoding.Type
	for len(valueBytes) > 0 && rf.valueColsFound < table.neededValueCols {
		typeOffset, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(valueBytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + sqlbase.ColumnID(colIDDiff)
		lastColID = colID
		if !table.neededCols.Contains(int(colID)) {
			// This column wasn't requested, so read its length and skip it.
			lenPeek, err := encoding.PeekValueLengthWithOffsetsAndType(valueBytes, dataOffset, typ)
			if err != nil {
				return "", "", err
			}
			valueBytes = valueBytes[lenPeek:]
			if DebugRowFetch {
				log.Infof(ctx, "Scan %s -> [%d] (skipped)", kv.Key, colID)
			}
			continue
		}
		idx := table.colIdxMap[colID]

		if rf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.Columns[idx].Name)
		}

		var encValue sqlbase.EncDatum
		encValue, valueBytes, err = sqlbase.EncDatumValueFromBufferWithOffsetsAndType(valueBytes, typeOffset,
			dataOffset, typ)
		if err != nil {
			return "", "", err
		}
		if util.EnableAQKK && typ == encoding.Float && table.cols[idx].Type.Precision >= 1 && table.cols[idx].Type.Precision <= 24 {
			if err := encValue.EncodeFloat4(); err != nil {
				log.Errorf(ctx, "EncodeFloat4 error: %s", err)
			}
		}
		if rf.traceKV {
			err := encValue.EnsureDecoded(&table.cols[idx].Type, rf.alloc)
			if err != nil {
				return "", "", err
			}
			_, _ = fmt.Fprintf(rf.prettyValueBuf, "/%v", encValue.Datum)
		}
		table.row[idx] = encValue
		rf.valueColsFound++
		if DebugRowFetch {
			log.Infof(ctx, "Scan %d -> %v", idx, encValue)
		}
	}
	if rf.traceKV {
		prettyValue = rf.prettyValueBuf.String()
	}
	return prettyKey, prettyValue, nil
}

// processValueTuple processes the given values (of columns family.ColumnIDs),
// setting values in the rf.row accordingly. The key is only used for logging.
func (rf *Fetcher) processValueTuple(
	ctx context.Context, table *TableInfo, kv roachpb.KeyValue, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	tupleBytes, err := kv.Value.GetTuple()
	if err != nil {
		return "", "", err
	}
	return rf.processValueBytes(ctx, table, kv, tupleBytes, prettyKeyPrefix)
}

// NextRow processes keys until we complete one row, which is returned as an
// EncDatumRow. The row contains one value per table column, regardless of the
// index used; values that are not needed (as per neededCols) are nil. The
// EncDatumRow should not be modified and is only valid until the next call.
// When there are no more rows, the EncDatumRow is nil. The error returned may
// be a scrub.ScrubError, which the caller is responsible for unwrapping.
// It also returns the table and index descriptor associated with the row
// (relevant when more than one table is specified during initialization).
func (rf *Fetcher) NextRow(
	ctx context.Context,
) (
	row sqlbase.EncDatumRow,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	err error,
) {
	if rf.kvEnd {
		return nil, nil, nil, nil
	}

	// All of the columns for a particular row will be grouped together. We
	// loop over the key/value pairs and decode the key to extract the
	// columns encoded within the key and the column ID. We use the column
	// ID to lookup the column and decode the value. All of these values go
	// into a map keyed by column name. When the index key changes we
	// output a row containing the current values.
	for {
		prettyKey, prettyVal, err := rf.ProcessKV(ctx, rf.Kv)
		if err != nil {
			return nil, nil, nil, err
		}
		if rf.traceKV {
			log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
		}

		if rf.isCheck {
			rf.rowReadyTable.lastKV = rf.Kv
		}
		rowDone, err := rf.NextKey(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		if rowDone {
			err := rf.FinalizeRow()
			return rf.rowReadyTable.row, rf.rowReadyTable.desc.TableDesc(), rf.rowReadyTable.index, err
		}
	}
}

// NextRow1 processes keys until we complete one row, which is returned as an
// EncDatumRow. The row contains one value per table column, regardless of the
// index used; values that are not needed (as per neededCols) are nil. The
// EncDatumRow should not be modified and is only valid until the next call.
// When there are no more rows, the EncDatumRow is nil. The error returned may
// be a scrub.ScrubError, which the caller is responsible for unwrapping.
// It also returns the table and index descriptor associated with the row
// (relevant when more than one table is specified during initialization).
func (rf *Fetcher) NextRow1(
	ctx context.Context,
) (
	row sqlbase.EncDatumRow,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	err error,
) {

	for {

		rowDone, err := rf.NextKey1(ctx)
		if err != nil {
			return nil, nil, nil, err
		}

		if rf.kvEnd {
			return nil, nil, nil, nil
		}

		prettyKey, prettyVal, err := rf.ProcessKV(ctx, rf.Kv)
		if err != nil {
			return nil, nil, nil, err
		}
		if rf.traceKV {
			log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
		}
		if rf.isCheck {
			rf.rowReadyTable.lastKV = rf.Kv
		}

		if rowDone {
			err := rf.FinalizeRow()
			return rf.rowReadyTable.row, rf.rowReadyTable.desc.TableDesc(), rf.rowReadyTable.index, err
		}
	}
}

// NextRowDecoded calls NextRow and decodes the EncDatumRow into a Datums.
// The Datums should not be modified and is only valid until the next call.
// When there are no more rows, the Datums is nil.
// It also returns the table and index descriptor associated with the row
// (relevant when more than one table is specified during initialization).
func (rf *Fetcher) NextRowDecoded(
	ctx context.Context,
) (
	datums tree.Datums,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	err error,
) {
	row, table, index, err := rf.NextRow(ctx)
	if err != nil {
		err = scrub.UnwrapScrubError(err)
		return nil, nil, nil, err
	}
	if row == nil {
		return nil, nil, nil, nil
	}

	for i, encDatum := range row {
		if encDatum.IsUnset() {
			rf.rowReadyTable.decodedRow[i] = tree.DNull
			continue
		}
		if err := encDatum.EnsureDecoded(&rf.rowReadyTable.cols[i].Type, rf.alloc); err != nil {
			return nil, nil, nil, err
		}
		rf.rowReadyTable.decodedRow[i] = encDatum.Datum
	}

	return rf.rowReadyTable.decodedRow, table, index, nil
}

// RowLastModified may only be called after NextRow has returned a non-nil row
// and returns the timestamp of the last modification to that row.
func (rf *Fetcher) RowLastModified() hlc.Timestamp {
	return rf.rowReadyTable.rowLastModified
}

// RowIsDeleted may only be called after NextRow has returned a non-nil row and
// returns true if that row was most recently deleted. This method is only
// meaningful when the configured kvBatchFetcher returns deletion tombstones, which
// the normal one (via `StartScan`) does not.
func (rf *Fetcher) RowIsDeleted() bool {
	return rf.rowReadyTable.rowIsDeleted
}

// NextRowWithErrors calls NextRow to fetch the next row and also run
// additional additional logic for physical checks. The Datums should
// not be modified and are only valid until the next call. When there
// are no more rows, the Datums is nil. The checks executed include:
//  - k/v data round-trips, i.e. it decodes and re-encodes to the same
//    value.
//  - There is no extra unexpected or incorrect data encoded in the k/v
//    pair.
//  - Decoded keys follow the same ordering as their encoding.
func (rf *Fetcher) NextRowWithErrors(ctx context.Context) (sqlbase.EncDatumRow, error) {
	row, table, index, err := rf.NextRow(ctx)
	if row == nil {
		return nil, nil
	} else if err != nil {
		// If this is not already a wrapped error, we will consider it to be
		// a generic physical error.
		// FIXME(joey): This may not be needed if we capture all the errors
		// encountered. This is a TBD when this change is polished.
		if !scrub.IsScrubError(err) {
			err = scrub.WrapError(scrub.PhysicalError, err)
		}
		return row, err
	}

	// Decode the row in-place. The following check datum encoding
	// functions require that the table.row datums are decoded.
	for i := range row {
		if row[i].IsUnset() {
			rf.rowReadyTable.decodedRow[i] = tree.DNull
			continue
		}
		if err := row[i].EnsureDecoded(&rf.rowReadyTable.cols[i].Type, rf.alloc); err != nil {
			return nil, err
		}
		rf.rowReadyTable.decodedRow[i] = row[i].Datum
	}

	if index.ID == table.PrimaryIndex.ID {
		err = rf.checkPrimaryIndexDatumEncodings(ctx)
	} else {
		err = rf.checkSecondaryIndexDatumEncodings(ctx)
	}
	if err != nil {
		return row, err
	}

	err = rf.checkKeyOrdering(ctx)

	return row, err
}

// checkPrimaryIndexDatumEncodings will run a round-trip encoding check
// on all values in the buffered row. This check is specific to primary
// index datums.
func (rf *Fetcher) checkPrimaryIndexDatumEncodings(ctx context.Context) error {
	table := rf.rowReadyTable
	scratch := make([]byte, 1024)
	colIDToColumn := make(map[sqlbase.ColumnID]sqlbase.ColumnDescriptor)
	for _, col := range table.desc.Columns {
		colIDToColumn[col.ID] = col
	}

	rh := rowHelper{TableDesc: table.desc, Indexes: table.desc.Indexes}

	for _, family := range table.desc.Families {
		var lastColID sqlbase.ColumnID
		familySortedColumnIDs, ok := rh.sortedColumnFamily(family.ID)
		if !ok {
			panic("invalid family sorted column id map")
		}

		for _, colID := range familySortedColumnIDs {
			rowVal := table.row[table.colIdxMap[colID]]
			if rowVal.IsNull() {
				// Column is not present.
				continue
			}

			if skip, err := rh.skipColumnInPK(colID, family.ID, rowVal.Datum); err != nil {
				log.Errorf(ctx, "unexpected error: %s", err)
				continue
			} else if skip {
				continue
			}

			col := colIDToColumn[colID]

			if lastColID > col.ID {
				panic(fmt.Errorf("cannot write column id %d after %d", col.ID, lastColID))
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID

			if result, err := sqlbase.EncodeTableValue([]byte(nil), colIDDiff, rowVal.Datum,
				scratch); err != nil {
				log.Errorf(ctx, "Could not re-encode column %s, value was %#v. Got error %s",
					col.Name, rowVal.Datum, err)
			} else if !rowVal.BytesEqual(result) {
				return scrub.WrapError(scrub.IndexValueDecodingError, errors.Errorf(
					"value failed to round-trip encode. Column=%s colIDDiff=%d Key=%s expected %#v, got: %#v",
					col.Name, colIDDiff, rf.Kv.Key, rowVal.EncodedString(), result))
			}
		}
	}
	return nil
}

// checkSecondaryIndexDatumEncodings will run a round-trip encoding
// check on all values in the buffered row. This check is specific to
// secondary index datums.
func (rf *Fetcher) checkSecondaryIndexDatumEncodings(ctx context.Context) error {
	table := rf.rowReadyTable
	colToEncDatum := make(map[sqlbase.ColumnID]sqlbase.EncDatum, len(table.row))
	values := make(tree.Datums, len(table.row))
	for i, col := range table.cols {
		colToEncDatum[col.ID] = table.row[i]
		values[i] = table.row[i].Datum
	}

	indexEntries, err := sqlbase.EncodeSecondaryIndex(table.desc.TableDesc(), table.index, table.colIdxMap, values)
	if err != nil {
		return err
	}

	for _, indexEntry := range indexEntries {
		// We ignore the first 4 bytes of the values. These bytes are a
		// checksum which are not set by EncodeSecondaryIndex.
		if !indexEntry.Key.Equal(rf.rowReadyTable.lastKV.Key) {
			return scrub.WrapError(scrub.IndexKeyDecodingError, errors.Errorf(
				"secondary index key failed to round-trip encode. expected %#v, got: %#v",
				rf.rowReadyTable.lastKV.Key, indexEntry.Key))
		} else if !indexEntry.Value.EqualData(table.lastKV.Value) {
			return scrub.WrapError(scrub.IndexValueDecodingError, errors.Errorf(
				"secondary index value failed to round-trip encode. expected %#v, got: %#v",
				rf.rowReadyTable.lastKV.Value, indexEntry.Value))
		}
	}
	return nil
}

// checkKeyOrdering verifies that the datums decoded for the current key
// have the same ordering as the encoded key.
func (rf *Fetcher) checkKeyOrdering(ctx context.Context) error {
	defer func() {
		rf.rowReadyTable.lastDatums = append(tree.Datums(nil), rf.rowReadyTable.decodedRow...)
	}()

	if !rf.rowReadyTable.hasLast {
		rf.rowReadyTable.hasLast = true
		return nil
	}

	evalCtx := tree.EvalContext{}
	// Iterate through columns in order, comparing each value to the value in the
	// previous row in that column. When the first column with a differing value
	// is found, compare the values to ensure the ordering matches the column
	// ordering.
	for i, id := range rf.rowReadyTable.index.ColumnIDs {
		idx := rf.rowReadyTable.colIdxMap[id]
		result := rf.rowReadyTable.decodedRow[idx].Compare(&evalCtx, rf.rowReadyTable.lastDatums[idx])
		expectedDirection := rf.rowReadyTable.index.ColumnDirections[i]
		if rf.reverse && expectedDirection == sqlbase.IndexDescriptor_ASC {
			expectedDirection = sqlbase.IndexDescriptor_DESC
		} else if rf.reverse && expectedDirection == sqlbase.IndexDescriptor_DESC {
			expectedDirection = sqlbase.IndexDescriptor_ASC
		}

		if result != 0 {
			if expectedDirection == sqlbase.IndexDescriptor_ASC && result < 0 ||
				expectedDirection == sqlbase.IndexDescriptor_DESC && result > 0 {
				return scrub.WrapError(scrub.IndexKeyDecodingError,
					errors.Errorf("key ordering did not match datum ordering. IndexDescriptor=%s",
						expectedDirection))
			}
			// After the first column with a differing value is found, the remaining
			// columns are skipped (see #32874).
			break
		}
	}
	return nil
}

//FinalizeRow is to Finalize Rows
func (rf *Fetcher) FinalizeRow() error {
	table := rf.rowReadyTable
	// Fill in any missing values with NULLs
	var issfunc bool
	for _, value := range table.index.IsFunc {
		if value {
			issfunc = true
		}
	}

	for i := range table.cols {
		if rf.valueColsFound == table.neededValueCols {
			// Found all cols - done!
			return nil
		}
		if table.neededCols.Contains(int(table.cols[i].ID)) && table.row[i].IsUnset() && !issfunc {
			// If the row was deleted, we'll be missing any non-primary key
			// columns, including nullable ones, but this is expected.
			if !table.cols[i].Nullable && !table.rowIsDeleted {
				var indexColValues []string
				for _, idx := range table.indexColIdx {
					if idx != -1 {
						indexColValues = append(indexColValues, table.row[idx].String(&table.cols[idx].Type))
					} else {
						indexColValues = append(indexColValues, "?")
					}
				}
				err := pgerror.NewAssertionErrorf(
					"Non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
					table.desc.Name, table.cols[i].Name, table.index.Name,
					strings.Join(table.index.ColumnNames, ","), strings.Join(indexColValues, ","))

				if rf.isCheck {
					return scrub.WrapError(scrub.UnexpectedNullValueError, err)
				}
				return err
			}
			table.row[i] = sqlbase.EncDatum{
				Datum: tree.DNull,
			}
			// We've set valueColsFound to the number of present columns in the row
			// already, in processValueBytes. Now, we're filling in columns that have
			// no encoded values with NULL - so we increment valueColsFound to permit
			// early exit from this loop once all needed columns are filled in.
			rf.valueColsFound++
		}
	}
	return nil
}

// Key returns the next key (the key that follows the last returned row).
// Key returns nil when there are no more rows.
func (rf *Fetcher) Key() roachpb.Key {
	return rf.Kv.Key
}

// PartialKey returns a partial slice of the next key (the key that follows the
// last returned row) containing nCols columns, without the ending column
// family. Returns nil when there are no more rows.
func (rf *Fetcher) PartialKey(nCols int) (roachpb.Key, error) {
	if rf.Kv.Key == nil {
		return nil, nil
	}
	n, err := consumeIndexKeyWithoutTableIDIndexIDPrefix(
		rf.currentTable.index, nCols, rf.Kv.Key[rf.currentTable.knownPrefixLength:])
	if err != nil {
		return nil, err
	}
	return rf.Kv.Key[:n+rf.currentTable.knownPrefixLength], nil
}

// GetRangeInfo returns information about the ranges where the rows came from.
// The RangeInfo's are deduped and not ordered.
func (rf *Fetcher) GetRangeInfo() []roachpb.RangeInfo {
	return rf.kvFetcher.GetRangesInfo()
}

// GetRangesInfo returns information about the ranges where the rows came from.
// The RangeInfo's are deduped and not ordered.
func (rf *Fetcher) GetRangesInfo() []roachpb.RangeInfo {
	f := rf.kvFetcher
	if f == nil {
		// Not yet initialized.
		return nil
	}
	return f.GetRangesInfo()
}

// GetBytesRead returns total number of bytes read by the underlying KVFetcher.
func (rf *Fetcher) GetBytesRead() int64 {
	f := rf.kvFetcher
	if f == nil {
		// Not yet initialized.
		return 0
	}
	return f.bytesRead
}

// Only unique secondary indexes have extra columns to decode (namely the
// primary index columns).
func hasExtraCols(table *TableInfo) bool {
	return table.isSecondaryIndex && table.index.Unique
}

// consumeIndexKeyWithoutTableIDIndexIDPrefix consumes an index key that's
// already pre-stripped of its table ID index ID prefix, up to nCols columns,
// returning the number of bytes consumed. For example, given an input key
// with values (6,7,8,9) such as /Table/60/1/6/7/#/61/1/8/9, stripping 3 columns
// from this key would eat all but the final, 4th column 9 in this example,
// producing /Table/60/1/6/7/#/61/1/8. If nCols was 2, instead, the result
// would include the trailing table ID index ID pair, since that's a more
// precise key: /Table/60/1/6/7/#/61/1.
func consumeIndexKeyWithoutTableIDIndexIDPrefix(
	index *sqlbase.IndexDescriptor, nCols int, key []byte,
) (int, error) {
	origKeyLen := len(key)
	consumedCols := 0
	for _, ancestor := range index.Interleave.Ancestors {
		length := int(ancestor.SharedPrefixLen)
		// Skip up to length values.
		for j := 0; j < length; j++ {
			if consumedCols == nCols {
				// We're done early, in the middle of an interleave.
				return origKeyLen - len(key), nil
			}
			l, err := encoding.PeekLength(key)
			if err != nil {
				return 0, err
			}
			key = key[l:]
			consumedCols++
		}
		var ok bool
		key, ok = encoding.DecodeIfInterleavedSentinel(key)
		if !ok {
			return 0, errors.New("unexpected lack of sentinel key")
		}

		// Skip the TableID/IndexID pair for each ancestor except for the
		// first, which has already been skipped in our input.
		for j := 0; j < 2; j++ {
			idLen, err := encoding.PeekLength(key)
			if err != nil {
				return 0, err
			}
			key = key[idLen:]
		}
	}

	// Decode the remaining values in the key, in the final interleave.
	for ; consumedCols < nCols; consumedCols++ {
		l, err := encoding.PeekLength(key)
		if err != nil {
			return 0, err
		}
		key = key[l:]
	}

	return origKeyLen - len(key), nil
}

//SetBatch make the address of chan to rf.BlockBatch
func (rf *Fetcher) SetBatch(res chan roachpb.BlockStruct) {
	rf.BlockBatch = res
}

//GetDesc return the table's name
func (rf *Fetcher) GetDesc() string {
	return rf.rowReadyTable.desc.Name
}

//GetIfFuncIndex return the index if func
func (rf *Fetcher) GetIfFuncIndex() bool {
	for _, table := range rf.tables {
		for _, isfunc := range table.index.IsFunc {
			if isfunc {
				return true
			}
		}
	}
	return false
}

//StartGetBatch read blocks of kvs until nil
func (rf *Fetcher) StartGetBatch(
	ctx context.Context,
	txn *client.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
	response chan roachpb.BlockStruct,
	errCh chan error,
	switchCount bool,
) error {
	if len(spans) == 0 {
		panic("no spans")
	}

	rf.traceKV = traceKV

	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := limitHint
	if firstBatchLimit != 0 {
		// The limitHint is a row limit, but each row could be made up
		// of more than one key. We take the maximum possible keys
		// per row out of all the table rows we could potentially
		// scan over.
		firstBatchLimit = limitHint * int64(rf.maxKeysPerRow)
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	f, err := makeKVBatchFetcher(txn, spans, rf.reverse, limitBatches, firstBatchLimit,
		rf.returnRangeInfo, rf.lockStrength, rf.lockWaitPolicy)
	if err != nil {
		return err
	}

	rf.indexKey = nil
	rf.kvFetcher = newKVFetcher(&f)

	go rf.ReadBatch(ctx, response, errCh, switchCount)
	err = <-errCh
	return err

}

// BlockedNextRow for tablereader parallel
func (rf *Fetcher) BlockedNextRow(
	ctx context.Context, block *roachpb.BlockStruct,
) (
	row sqlbase.EncDatumRow,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	err error,
) {
	if rf.kvEnd {
		return nil, nil, nil, nil
	}

	// All of the columns for a particular row will be grouped together. We
	// loop over the key/value pairs and decode the key to extract the
	// columns encoded within the key and the column ID. We use the column
	// ID to lookup the column and decode the value. All of these values go
	// into a map keyed by column name. When the index key changes we
	// output a row containing the current values.
	for {
		prettyKey, prettyVal, err := rf.ProcessKV(ctx, rf.Kv)
		if err != nil {
			return nil, nil, nil, err
		}
		if rf.traceKV {
			log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
		}

		if rf.isCheck {
			rf.rowReadyTable.lastKV = rf.Kv
		}
		rowDone, err := rf.BlockedNextKey(ctx, block)
		if err != nil {
			return nil, nil, nil, err
		}
		if rowDone {
			err := rf.FinalizeRow()
			return rf.rowReadyTable.row, rf.rowReadyTable.desc.TableDesc(), rf.rowReadyTable.index, err
		}
	}
}

// BlockedNextKey for tablereader parallel
func (rf *Fetcher) BlockedNextKey(
	ctx context.Context, block *roachpb.BlockStruct,
) (rowDone bool, err error) {
	var ok bool

	for {
		ok, rf.Kv, _, err = rf.kvFetcher.BlockedNextKV(ctx, block)
		if err != nil {
			return false, err
		}
		rf.kvEnd = !ok
		if rf.kvEnd {
			// No more keys in the scan. We need to transition
			// rf.rowReadyTable to rf.currentTable for the last
			// row.
			rf.rowReadyTable = rf.currentTable
			return true, nil
		}

		// See Init() for a detailed description of when we can get away with not
		// reading the index key.
		if rf.mustDecodeIndexKey || rf.traceKV {
			rf.keyRemainingBytes, ok, err = rf.ReadIndexKey(rf.Kv.Key)
			if err != nil {
				return false, err
			}
			if !ok {
				// The key did not match any of the table
				// descriptors, which means it's interleaved
				// data from some other table or index.
				continue
			}
		} else {
			// We still need to consume the key until the family
			// id, so processKV can know whether we've finished a
			// row or not.
			prefixLen, err := keys.GetRowPrefixLength(rf.Kv.Key)
			if err != nil {
				return false, err
			}
			rf.keyRemainingBytes = rf.Kv.Key[prefixLen:]
		}

		// For unique secondary indexes, the index-key does not distinguish one row
		// from the next if both rows contain identical values along with a NULL.
		// Consider the keys:
		//
		//   /test/unique_idx/NULL/0
		//   /test/unique_idx/NULL/1
		//
		// The index-key extracted from the above keys is /test/unique_idx/NULL. The
		// trailing /0 and /1 are the primary key used to unique-ify the keys when a
		// NULL is present. Currently we don't detect NULLs on decoding. If we did
		// we could detect this case and enlarge the index-key. A simpler fix for
		// this problem is to simply always output a row for each key scanned from a
		// secondary index as secondary indexes have only one key per row.
		// If rf.rowReadyTable differs from rf.currentTable, this denotes
		// a row is ready for output.
		switch {
		case rf.currentTable.isSecondaryIndex:
			// Secondary indexes have only one key per row.
			rowDone = true
		case !bytes.HasPrefix(rf.Kv.Key, rf.indexKey):
			// If the prefix of the key has changed, current key is from a different
			// row than the previous one.
			rowDone = true
		case rf.rowReadyTable != rf.currentTable:
			// For rowFetchers with more than one table, if the table changes the row
			// is done.
			rowDone = true
		default:
			rowDone = false
		}

		if rf.indexKey != nil && rowDone {
			// The current key belongs to a new row. Output the
			// current row.
			rf.indexKey = nil
			return true, nil
		}

		return false, nil
	}
}

//ReadBatch call t.NextBatch
func (rf *Fetcher) ReadBatch(
	ctx context.Context, respons chan roachpb.BlockStruct, errCh chan error, switchCount bool,
) {
	switch t := rf.kvFetcher.kvBatchFetcher.(type) {
	case *txnKVFetcher:
		if switchCount {
			t.count = rf.Count
			t.justcount = switchCount
		}
		t.NextBatch(ctx, respons, errCh, rf)
	}
}
