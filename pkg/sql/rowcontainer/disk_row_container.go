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

package rowcontainer

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/diskmap"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/mon"
)

// DiskRowContainer is a SortableRowContainer that stores rows on disk according
// to the ordering specified in DiskRowContainer.ordering. The underlying store
// is a SortedDiskMap so the sorting itself is delegated. Use an iterator
// created through NewIterator() to read the rows in sorted order.
type DiskRowContainer struct {
	diskMap diskmap.SortedDiskMap
	// diskAcc keeps track of disk usage.
	diskAcc mon.BoundAccount
	// bufferedRows buffers writes to the diskMap.
	bufferedRows  diskmap.SortedDiskMapBatchWriter
	scratchKey    []byte
	scratchVal    []byte
	scratchEncRow sqlbase.EncDatumRow

	// For computing mean encoded row bytes.
	totalEncodedRowBytes uint64

	// lastReadKey is used to implement NewFinalIterator. Refer to the method's
	// comment for more information.
	lastReadKey []byte

	// topK is set by callers through InitTopK. Since rows are kept in sorted
	// order, topK will simply limit iterators to read the first k rows.
	topK int

	// rowID is used as a key suffix to prevent duplicate rows from overwriting
	// each other.
	rowID uint64

	// types is the schema of rows in the container.
	types []sqlbase.ColumnType
	// ordering is the order in which rows should be sorted.
	ordering sqlbase.ColumnOrdering
	// encodings keeps around the DatumEncoding equivalents of the encoding
	// directions in ordering to avoid conversions in hot paths.
	encodings []sqlbase.DatumEncoding
	// valueIdxs holds the indexes of the columns that we encode as values. The
	// columns described by ordering will be encoded as keys. See
	// MakeDiskRowContainer() for more encoding specifics.
	valueIdxs []int

	// See comment in DoDeDuplicate().
	deDuplicate bool
	// A mapping from a key to the dense row index assigned to the key. It
	// contains all the key strings that are potentially buffered in bufferedRows.
	// Since we need to de-duplicate for every insert attempt, we don't want to
	// keep flushing bufferedRows after every insert.
	// There is currently no memory-accounting for the deDupCache, just like there
	// is none for the bufferedRows. Both will be approximately the same size.
	deDupCache map[string]int

	diskMonitor *mon.BytesMonitor
	engine      diskmap.Factory

	datumAlloc sqlbase.DatumAlloc
	isFile     bool
}

var _ SortableRowContainer = &DiskRowContainer{}
var _ DeDupingRowContainer = &DiskRowContainer{}

// MakeDiskRowContainer creates a DiskRowContainer with the given engine as the
// underlying store that rows are stored on.
// Arguments:
// 	- diskMonitor is used to monitor this DiskRowContainer's disk usage.
// 	- types is the schema of rows that will be added to this container.
// 	- ordering is the output ordering; the order in which rows should be sorted.
// 	- e is the underlying store that rows are stored on.
func MakeDiskRowContainer(
	diskMonitor *mon.BytesMonitor,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
	e diskmap.Factory,
	isFile bool,
) DiskRowContainer {
	diskMap := e.NewSortedDiskMap()
	d := DiskRowContainer{
		diskMap:       diskMap,
		diskAcc:       diskMonitor.MakeBoundAccount(),
		types:         types,
		ordering:      ordering,
		scratchEncRow: make(sqlbase.EncDatumRow, len(types)),
		diskMonitor:   diskMonitor,
		engine:        e,
		isFile:        isFile,
	}
	d.bufferedRows = d.diskMap.NewBatchWriter()

	// The ordering is specified for a subset of the columns. These will be
	// encoded as a key in the given order according to the given direction so
	// that the sorting can be delegated to the underlying SortedDiskMap. To
	// avoid converting encoding.Direction to sqlbase.DatumEncoding we do this
	// once at initialization and store the conversions in d.encodings.
	// We encode the other columns as values. The indexes of these columns are
	// kept around in d.valueIdxs to have them ready in hot paths.
	// For composite columns that are specified in d.ordering, the Datum is
	// encoded both in the key for comparison and in the value for decoding.
	orderingIdxs := make(map[int]struct{})
	for _, orderInfo := range d.ordering {
		orderingIdxs[orderInfo.ColIdx] = struct{}{}
	}
	d.valueIdxs = make([]int, 0, len(d.types))
	for i := range d.types {
		// TODO(asubiotto): A datum of a type for which HasCompositeKeyEncoding
		// returns true may not necessarily need to be encoded in the value, so
		// make this more fine-grained. See IsComposite() methods in
		// pkg/sql/parser/datum.go.
		if _, ok := orderingIdxs[i]; !ok || (!isFile && sqlbase.HasCompositeKeyEncoding(d.types[i].SemanticType)) {
			d.valueIdxs = append(d.valueIdxs, i)
		}
	}

	d.encodings = make([]sqlbase.DatumEncoding, len(d.ordering))
	for i, orderInfo := range ordering {
		d.encodings[i] = sqlbase.EncodingDirToDatumEncoding(orderInfo.Direction)
	}

	return d
}

// Len is part of the SortableRowContainer interface.
func (d *DiskRowContainer) Len() int {
	return int(d.rowID)
}

// AddRow is part of the SortableRowContainer interface.
//
// It is additionally used in de-duping mode by DiskBackedRowContainer when
// switching from a memory container to this disk container, since it is
// adding rows that are already de-duped. Once it has added all the already
// de-duped rows, it should switch to using AddRowWithDeDup() and never call
// AddRow() again.
//
// Note: if key calculation changes, computeKey() of hashMemRowIterator should
// be changed accordingly.
func (d *DiskRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if err := d.encodeRow(ctx, row); err != nil {
		return err
	}
	if err := d.diskAcc.Grow(ctx, int64(len(d.scratchKey)+len(d.scratchVal))); err != nil {
		return pgerror.Wrapf(err, pgcode.OutOfMemory,
			"this query requires additional disk space")
	}
	if err := d.bufferedRows.Put(d.scratchKey, d.scratchVal); err != nil {
		return err
	}
	// See comment above on when this is used for already de-duplicated
	// rows -- we need to track these in the de-dup cache so that later
	// calls to AddRowWithDeDup() de-duplicate wrt this cache.
	if d.deDuplicate {
		if d.bufferedRows.NumPutsSinceFlush() == 0 {
			d.clearDeDupCache()
		} else {
			d.deDupCache[string(d.scratchKey)] = int(d.rowID)
		}
	}
	d.totalEncodedRowBytes += uint64(len(d.scratchKey) + len(d.scratchVal))
	d.scratchKey = d.scratchKey[:0]
	d.scratchVal = d.scratchVal[:0]
	d.rowID++
	return nil
}

// AddRowWithDeDup is part of the DeDupingRowContainer interface.
func (d *DiskRowContainer) AddRowWithDeDup(
	ctx context.Context, row sqlbase.EncDatumRow,
) (int, error) {
	if err := d.encodeRow(ctx, row); err != nil {
		return 0, err
	}
	defer func() {
		d.scratchKey = d.scratchKey[:0]
		d.scratchVal = d.scratchVal[:0]
	}()
	// First use the cache to de-dup.
	entry, ok := d.deDupCache[string(d.scratchKey)]
	if ok {
		return entry, nil
	}
	// Since not in cache, we need to use an iterator to de-dup.
	// TODO(sumeer): this read is expensive:
	// - if there is a significant  fraction of duplicates, we can do better
	//   with a larger cache
	// - if duplicates are rare, use a bloom filter for all the keys in the
	//   diskMap, since a miss in the bloom filter allows us to write to the
	//   diskMap without reading.
	iter := d.diskMap.NewIterator()
	defer iter.Close()
	iter.Seek(d.scratchKey)
	valid, err := iter.Valid()
	if err != nil {
		return 0, err
	}
	if valid && bytes.Equal(iter.UnsafeKey(), d.scratchKey) {
		// Found the key. Note that as documented in DeDupingRowContainer,
		// this feature is limited to the case where the whole row is
		// encoded into the key. The value only contains the dense RowID
		// assigned to the key.
		_, idx, err := encoding.DecodeUvarintAscending(iter.UnsafeValue())
		if err != nil {
			return 0, err
		}
		return int(idx), nil
	}
	if err := d.diskAcc.Grow(ctx, int64(len(d.scratchKey)+len(d.scratchVal))); err != nil {
		return 0, pgerror.Wrapf(err, pgcode.OutOfMemory,
			"this query requires additional disk space")
	}
	if err := d.bufferedRows.Put(d.scratchKey, d.scratchVal); err != nil {
		return 0, err
	}
	if d.bufferedRows.NumPutsSinceFlush() == 0 {
		d.clearDeDupCache()
	} else {
		d.deDupCache[string(d.scratchKey)] = int(d.rowID)
	}
	d.totalEncodedRowBytes += uint64(len(d.scratchKey) + len(d.scratchVal))
	idx := int(d.rowID)
	d.rowID++
	return idx, nil
}

func (d *DiskRowContainer) clearDeDupCache() {
	for k := range d.deDupCache {
		delete(d.deDupCache, k)
	}
}

func (d *DiskRowContainer) encodeRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if len(row) != len(d.types) {
		log.Fatalf(ctx, "invalid row length %d, expected %d", len(row), len(d.types))
	}

	for i, orderInfo := range d.ordering {
		col := orderInfo.ColIdx
		var err error
		d.scratchKey, err = row[col].Encode(&d.types[col], &d.datumAlloc, d.encodings[i], d.scratchKey)
		if err != nil {
			return err
		}
	}
	if d.isFile {
		d.scratchVal = EncOffsetForReadFile(d.scratchVal, d.valueIdxs, row)
		d.scratchKey = encoding.EncodeUvarintAscending(d.scratchKey, d.rowID)
	} else {
		if !d.deDuplicate {
			for _, i := range d.valueIdxs {
				var err error
				d.scratchVal, err = row[i].Encode(&d.types[i], &d.datumAlloc, sqlbase.DatumEncoding_VALUE, d.scratchVal)
				if err != nil {
					return err
				}
			}
			// Put a unique row to keep track of duplicates. Note that this will not
			// mess with key decoding.
			d.scratchKey = encoding.EncodeUvarintAscending(d.scratchKey, d.rowID)
		} else {
			// Add the row id to the value. Note that in this de-duping case the
			// row id is the only thing in the value since the whole row is encoded
			// into the key. Note that the key could have types for which
			// HasCompositeKeyEncoding() returns true and we do not encode them
			// into the value (only in the key) for this DeDupingRowContainer. This
			// is ok since:
			// - The DeDupingRowContainer never needs to return the original row
			//   (there is no get method).
			// - The columns encoded into the key are the primary key columns
			//   of the original table, so the key encoding represents a unique
			//   row in the original table (the key encoding here is not only
			//   a determinant of sort ordering).
			d.scratchVal = encoding.EncodeUvarintAscending(d.scratchVal, d.rowID)
		}
		// Put a unique row to keep track of duplicates. Note that this will not
		// mess with key decoding.
		d.scratchKey = encoding.EncodeUvarintAscending(d.scratchKey, d.rowID)
	}
	return nil
}

// EncOffsetForReadFile encode offset
func EncOffsetForReadFile(searchVal []byte, idxs []int, row sqlbase.EncDatumRow) []byte {
	for i, idx := range idxs {
		if i == 0 {
			searchVal = []byte(*row[idx].Datum.(*tree.DBytes))
		}
		if i == 1 {
			if *row[idx].Datum.(*tree.DBool) {
				searchVal = append(searchVal, byte(1))
			} else {
				searchVal = append(searchVal, byte(0))
			}
		}
	}
	return searchVal
}

// Sort is a noop because the use of a SortedDiskMap as the underlying store
// keeps the rows in sorted order.
func (d *DiskRowContainer) Sort(context.Context) {}

// Reorder implements ReorderableRowContainer. It creates a new
// DiskRowContainer with the requested ordering and adds a row one by one from
// the current DiskRowContainer, the latter is closed at the end.
func (d *DiskRowContainer) Reorder(ctx context.Context, ordering sqlbase.ColumnOrdering) error {
	// We need to create a new DiskRowContainer since its ordering can only be
	// changed at initialization.
	newContainer := MakeDiskRowContainer(d.diskMonitor, d.types, ordering, d.engine, false)
	i := d.NewFinalIterator(ctx)
	defer i.Close()
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		row, err := i.Row()
		if err != nil {
			return err
		}
		if err := newContainer.AddRow(ctx, row); err != nil {
			return err
		}
	}
	d.Close(ctx)
	*d = newContainer
	return nil
}

// InitTopK limits iterators to read the first k rows.
func (d *DiskRowContainer) InitTopK() {
	d.topK = d.Len()
}

// MaybeReplaceMax adds row to the DiskRowContainer. The SortedDiskMap will
// sort this row into the top k if applicable.
func (d *DiskRowContainer) MaybeReplaceMax(ctx context.Context, row sqlbase.EncDatumRow) error {
	return d.AddRow(ctx, row)
}

// MeanEncodedRowBytes returns the mean bytes consumed by an encoded row stored in
// this container.
func (d *DiskRowContainer) MeanEncodedRowBytes() int {
	if d.rowID == 0 {
		return 0
	}
	return int(d.totalEncodedRowBytes / d.rowID)
}

// UnsafeReset is part of the SortableRowContainer interface.
func (d *DiskRowContainer) UnsafeReset(ctx context.Context) error {
	_ = d.bufferedRows.Close(ctx)
	if err := d.diskMap.Clear(); err != nil {
		return err
	}
	d.diskAcc.Clear(ctx)
	d.bufferedRows = d.diskMap.NewBatchWriter()
	d.clearDeDupCache()
	d.lastReadKey = nil
	d.rowID = 0
	d.totalEncodedRowBytes = 0
	return nil
}

// Close is part of the SortableRowContainer interface.
func (d *DiskRowContainer) Close(ctx context.Context) {
	// We can ignore the error here because the flushed data is immediately cleared
	// in the following Close.
	_ = d.bufferedRows.Close(ctx)
	d.diskMap.Close(ctx)
	d.diskAcc.Close(ctx)
}

// keyValToRow decodes a key and a value byte slice stored with AddRow() into
// a sqlbase.EncDatumRow. The returned EncDatumRow is only valid until the next
// call to keyValToRow().
func (d *DiskRowContainer) keyValToRow(k []byte, v []byte) (sqlbase.EncDatumRow, error) {
	for i, orderInfo := range d.ordering {
		// Types with composite key encodings are decoded from the value.
		if sqlbase.HasCompositeKeyEncoding(d.types[orderInfo.ColIdx].SemanticType) {
			// Skip over the encoded key.
			encLen, err := encoding.PeekLength(k)
			if err != nil {
				return nil, err
			}
			k = k[encLen:]
			continue
		}
		var err error
		col := orderInfo.ColIdx
		d.scratchEncRow[col], k, err = sqlbase.EncDatumFromBuffer(&d.types[col], d.encodings[i], k)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode row")
		}
	}
	if d.isFile {
		if len(d.valueIdxs) == 1 {
			d.scratchEncRow[len(d.types)-1] = sqlbase.EncBytesDatum(sqlbase.DatumEncoding_VALUE, v)
		} else {
			if v[len(v)-1] == byte(1) {
				d.scratchEncRow[len(d.types)-1] = sqlbase.EncBoolDatum(sqlbase.DatumEncoding_VALUE, true)
			} else {
				d.scratchEncRow[len(d.types)-1] = sqlbase.EncBoolDatum(sqlbase.DatumEncoding_VALUE, false)
			}
			d.scratchEncRow[len(d.types)-2] = sqlbase.EncBytesDatum(sqlbase.DatumEncoding_VALUE, v[:len(v)-1])
		}

	} else {
		for _, i := range d.valueIdxs {
			var err error
			d.scratchEncRow[i], v, err = sqlbase.EncDatumFromBuffer(&d.types[i], sqlbase.DatumEncoding_VALUE, v)
			if err != nil {
				return nil, errors.Wrap(err, "unable to decode row")
			}
		}
	}
	for i := range d.scratchEncRow {
		err := d.scratchEncRow[i].EnsureDecoded(&d.types[i], &d.datumAlloc)
		if err != nil {
			return nil, err
		}
	}
	return d.scratchEncRow, nil
}

// diskRowIterator iterates over the rows in a DiskRowContainer.
type diskRowIterator struct {
	rowContainer *DiskRowContainer
	diskmap.SortedDiskMapIterator
}

var _ RowIterator = &diskRowIterator{}

func (d *DiskRowContainer) newIterator(ctx context.Context) diskRowIterator {
	if err := d.bufferedRows.Flush(); err != nil {
		log.Fatal(ctx, err)
	}
	return diskRowIterator{rowContainer: d, SortedDiskMapIterator: d.diskMap.NewIterator()}
}

//NewIterator is part of the SortableRowContainer interface.
func (d *DiskRowContainer) NewIterator(ctx context.Context) RowIterator {
	i := d.newIterator(ctx)
	if d.topK > 0 {
		return &diskRowTopKIterator{RowIterator: &i, k: d.topK}
	}
	return &i
}

// Row returns the current row. The returned sqlbase.EncDatumRow is only valid
// until the next call to Row().
func (r *diskRowIterator) Row() (sqlbase.EncDatumRow, error) {
	if ok, err := r.Valid(); err != nil {
		return nil, errors.Wrap(err, "unable to check row validity")
	} else if !ok {
		return nil, errors.New("invalid row")
	}

	return r.rowContainer.keyValToRow(r.Key(), r.Value())
}

func (r *diskRowIterator) RowOpt(enc *sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	if ok, err := r.Valid(); err != nil {
		return nil, errors.Wrap(err, "unable to check row validity")
	} else if !ok {
		return nil, errors.New("invalid row")
	}

	return r.rowContainer.keyValToRow(r.Key(), r.Value())
}
func (r *diskRowIterator) Close() {
	if r.SortedDiskMapIterator != nil {
		r.SortedDiskMapIterator.Close()
	}
}

// numberedRowIterator is a specialization of diskRowIterator that is
// only for the case where the key is the rowID assigned in AddRow().
type numberedRowIterator struct {
	*diskRowIterator
	scratchKey []byte
}

func (d *DiskRowContainer) newNumberedIterator(ctx context.Context) *numberedRowIterator {
	i := d.newIterator(ctx)
	return &numberedRowIterator{diskRowIterator: &i}
}

func (n numberedRowIterator) seekToIndex(idx int) {
	n.scratchKey = encoding.EncodeUvarintAscending(n.scratchKey, uint64(idx))
	n.Seek(n.scratchKey)
}

type diskRowFinalIterator struct {
	diskRowIterator
}

var _ RowIterator = &diskRowFinalIterator{}

// NewFinalIterator returns an iterator that reads rows exactly once throughout
// the lifetime of a DiskRowContainer. Rows are not actually discarded from the
// DiskRowContainer, but the lastReadKey is kept track of in order to serve as
// the start key for future diskRowFinalIterators.
// NOTE: Don't use NewFinalIterator if you passed in an ordering for the rows
// and will be adding rows between iterations. New rows could sort before the
// current row.
func (d *DiskRowContainer) NewFinalIterator(ctx context.Context) RowIterator {
	i := diskRowFinalIterator{diskRowIterator: d.newIterator(ctx)}
	if d.topK > 0 {
		return &diskRowTopKIterator{RowIterator: &i, k: d.topK}
	}
	return &i
}

func (r *diskRowFinalIterator) Rewind() {
	r.Seek(r.diskRowIterator.rowContainer.lastReadKey)
	if r.diskRowIterator.rowContainer.lastReadKey != nil {
		r.Next()
	}
}

func (r *diskRowFinalIterator) Row() (sqlbase.EncDatumRow, error) {
	row, err := r.diskRowIterator.Row()
	if err != nil {
		return nil, err
	}
	r.diskRowIterator.rowContainer.lastReadKey = r.Key()
	return row, nil
}

type diskRowTopKIterator struct {
	RowIterator
	position int
	// k is the limit of rows to read.
	k int
}

var _ RowIterator = &diskRowTopKIterator{}

func (d *diskRowTopKIterator) Rewind() {
	d.RowIterator.Rewind()
	d.position = 0
}

func (d *diskRowTopKIterator) Valid() (bool, error) {
	if d.position >= d.k {
		return false, nil
	}
	return d.RowIterator.Valid()
}

func (d *diskRowTopKIterator) Next() {
	d.position++
	d.RowIterator.Next()
}
