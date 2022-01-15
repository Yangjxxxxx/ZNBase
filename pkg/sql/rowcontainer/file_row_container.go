package rowcontainer

import (
	"context"
	"runtime"
	"sync"

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

// KeyRowBufferLen key rows buffer
const KeyRowBufferLen = 1024

// FileRowContainer base data struct
type FileRowContainer struct {
	sync.WaitGroup
	FileStore   *Hstore
	drc         DiskBackedRowContainer
	originOrder sqlbase.ColumnOrdering
	originTypes []sqlbase.ColumnType
	valueIdx    []int
	datumAlloc  sqlbase.DatumAlloc
	scratchVal  []byte
	realval     []byte
	cmpRow      sqlbase.EncDatumRow

	offset     uint32
	releaseBuf []byte
	fbid       []byte

	keyRowRecv    chan sqlbase.EncDatumRows
	keyRowCollect chan sqlbase.EncDatumRows
	curBuffer     sqlbase.EncDatumRows
	keyRowIdx     int
}

// Len unimpl
func (f *FileRowContainer) Len() int {
	panic("implement me")
}

func (f *FileRowContainer) dumpKeyRow(ctx context.Context) {
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
		f.Done()
		close(f.keyRowCollect)
	}()

	for {
		buf, ok := <-f.keyRowRecv
		if ok {
			for i := range buf {
				err := f.drc.AddRow(ctx, buf[i])
				if err != nil {
					return
				}
			}
			select {
			case f.keyRowCollect <- buf:
			default:
			}
		} else {
			return
		}
	}
}

// AddRow file row container add encrows
func (f *FileRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	var err error
	f.realval = f.realval[:4]
	for _, i := range f.valueIdx {
		f.scratchVal, err = row[i].Encode(&f.originTypes[i], &f.datumAlloc, sqlbase.DatumEncoding_VALUE, f.scratchVal)
		if err != nil {
			return err
		}
	}
	f.realval[0] = byte(len(f.scratchVal))
	f.realval[1] = byte(len(f.scratchVal) >> 8)
	f.realval[2] = byte(len(f.scratchVal) >> 16)
	f.realval[3] = byte(len(f.scratchVal) >> 24)
	f.realval = append(f.realval, f.scratchVal...)

	if f.offset+uint32(len(f.realval)) > blockSize {
		f.offset = 0
		err = f.FileStore.ReleaseBlock(f.fbid, f.releaseBuf)
		if err != nil {
			return err
		}
		f.fbid, f.releaseBuf, err = f.FileStore.GetBlockForWrite(f.fbid)
	}

	copy(f.releaseBuf[f.offset:int(f.offset)+len(f.realval)], f.realval)
	for i, o := range f.originOrder {
		f.cmpRow[i] = row[o.ColIdx]
	}
	f.fbid = append(f.fbid, byte(f.offset))
	f.fbid = append(f.fbid, byte(f.offset>>8))
	f.fbid = append(f.fbid, byte(f.offset>>16))
	f.fbid = append(f.fbid, byte(f.offset>>24))
	idrow := sqlbase.EncBytesDatum(sqlbase.DatumEncoding_VALUE, f.fbid)
	f.cmpRow[len(f.originOrder)] = idrow
	if f.curBuffer == nil {
		select {
		case f.curBuffer = <-f.keyRowCollect:
		default:
			f.curBuffer = make(sqlbase.EncDatumRows, KeyRowBufferLen)
		}

	}
	if len(f.curBuffer[f.keyRowIdx]) == 0 {
		f.curBuffer[f.keyRowIdx] = make(sqlbase.EncDatumRow, len(f.cmpRow))
	}
	copy(f.curBuffer[f.keyRowIdx], f.cmpRow)
	f.keyRowIdx++
	if f.keyRowIdx == KeyRowBufferLen {
		f.keyRowRecv <- f.curBuffer
		f.keyRowIdx = 0
		f.curBuffer = nil
	}
	f.fbid = f.fbid[:3]
	f.offset = f.offset + uint32(len(f.realval))
	f.scratchVal = f.scratchVal[:0]
	return nil
}

// Sort sort interface impl for tempfile
func (f *FileRowContainer) Sort(ctx context.Context) {
	if f.offset != 0 {
		if err := f.FileStore.ReleaseBlock(f.fbid, f.releaseBuf); err != nil {
			panic(err)
		}
	}
	f.FileStore.WriteDone()
	if f.curBuffer != nil {
		f.keyRowRecv <- f.curBuffer[:f.keyRowIdx]
	}
	close(f.keyRowRecv)
	f.Wait()
	f.drc.Sort(ctx)
}

// NewIterator new iterator
func (f *FileRowContainer) NewIterator(ctx context.Context) RowIterator {
	return f.drc.NewIterator(ctx)
}

// NewFinalIterator new final iterator
func (f *FileRowContainer) NewFinalIterator(ctx context.Context) RowIterator {
	return f.drc.NewFinalIterator(ctx)
}

// UnsafeReset unimpl
func (f *FileRowContainer) UnsafeReset(context.Context) error {
	panic("implement me")
}

// InitTopK init topk
func (f *FileRowContainer) InitTopK() {
	panic("implement me")
}

// MaybeReplaceMax unimpl
func (f *FileRowContainer) MaybeReplaceMax(context.Context, sqlbase.EncDatumRow) error {
	panic("implement me")
}

// Close close tempfile container
func (f *FileRowContainer) Close(ctx context.Context) {
	f.drc.Close(ctx)
}

// Reorder unimpl
func (f *FileRowContainer) Reorder(context.Context, sqlbase.ColumnOrdering) error {
	panic("implement me")
}

// MakeFileRowContainer make file rowcontainer
func (f *FileBackedRowContainer) MakeFileRowContainer(
	monitor *mon.BytesMonitor,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
	engine diskmap.Factory,
) *FileRowContainer {
	var frc FileRowContainer
	frc.fbid = make([]byte, 3)
	frc.realval = make([]byte, 4)
	frc.cmpRow = make(sqlbase.EncDatumRow, len(ordering)+1)
	frc.originOrder = ordering
	frc.originTypes = types
	frc.fbid, frc.releaseBuf, _ = f.FileStore.GetBlockForWrite(frc.fbid)

	frc.keyRowRecv = make(chan sqlbase.EncDatumRows, 2)
	frc.keyRowCollect = make(chan sqlbase.EncDatumRows, 2)
	for i := 0; i < 2; i++ {
		frc.keyRowCollect <- make(sqlbase.EncDatumRows, KeyRowBufferLen)
	}

	orderingIdxs := make(map[int]struct{})
	for _, orderInfo := range ordering {
		orderingIdxs[orderInfo.ColIdx] = struct{}{}
	}
	frc.valueIdx = make([]int, 0, len(types))
	for i := range types {
		// TODO(asubiotto): A datum of a type for which HasCompositeKeyEncoding
		// returns true may not necessarily need to be encoded in the value, so
		// make this more fine-grained. See IsComposite() methods in
		// pkg/sql/parser/datum.go.
		if _, ok := orderingIdxs[i]; !ok || sqlbase.HasCompositeKeyEncoding(types[i].SemanticType) {
			frc.valueIdx = append(frc.valueIdx, i)
		}
	}

	var newTypes = make([]sqlbase.ColumnType, 0, len(ordering)+1)
	var newOrder = make(sqlbase.ColumnOrdering, 0, len(ordering))
	for i := range ordering {
		newTypes = append(newTypes, types[ordering[i].ColIdx])
		newOrder = append(newOrder, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: ordering[i].Direction})
	}
	newTypes = append(newTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES})
	frc.FileStore = f.FileStore
	rc := DiskBackedRowContainer{}
	rc.Init(
		newOrder,
		newTypes,
		f.mrc.evalCtx,
		engine,
		f.fileMemMonitor,
		monitor,
		0, /* rowCapacity */
		true,
	)
	frc.drc = rc
	return &frc
}

// FileBackedRowContainer base data struct
type FileBackedRowContainer struct {
	src ReorderableRowContainer
	mrc *MemRowContainer
	frc *FileRowContainer

	spilled        bool
	engine         diskmap.Factory
	diskMonitor    *mon.BytesMonitor
	fileMemMonitor *mon.BytesMonitor
	FileStore      *Hstore
	scratchEncRow  sqlbase.EncDatumRow
	datumAlloc     sqlbase.DatumAlloc

	appendTo []byte
}

// Len unimpl
func (f *FileBackedRowContainer) Len() int {
	panic("implement me")
}

// AddRow file back container add enc rows
func (f *FileBackedRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if err := f.src.AddRow(ctx, row); err != nil {
		if spilled, spillErr := f.spillIfMemErr(ctx, err); !spilled && spillErr == nil {
			// The error was not an out of memory error.
			return err
		} else if spillErr != nil {
			// A disk spill was attempted but there was an error in doing so.
			return spillErr
		}
		// Add the row that caused the memory error.
		return f.src.AddRow(ctx, row)
	}
	return nil
}

// Sort sort interface impl
func (f *FileBackedRowContainer) Sort(ctx context.Context) {
	f.src.Sort(ctx)
}

// NewIterator unimpl
func (f *FileBackedRowContainer) NewIterator(context.Context) RowIterator {
	panic("implement me")
}

// NewFinalIterator final iterator
func (f *FileBackedRowContainer) NewFinalIterator(ctx context.Context) RowIterator {
	return f.src.NewFinalIterator(ctx)
}

// UnsafeReset unimpl
func (f *FileBackedRowContainer) UnsafeReset(context.Context) error {
	panic("implement me")
}

// InitTopK unimpl
func (f *FileBackedRowContainer) InitTopK() {
	panic("implement me")
}

// MaybeReplaceMax unimpl
func (f *FileBackedRowContainer) MaybeReplaceMax(context.Context, sqlbase.EncDatumRow) error {
	panic("implement me")
}

// Close close the file back rowcontainer
func (f *FileBackedRowContainer) Close(ctx context.Context) {
	f.mrc.Close(ctx)
	if err := f.FileStore.Close(); err != nil {
		panic(err)
	}
	if f.frc != nil {
		f.frc.Close(ctx)
	}
}

var _ SortableRowContainer = &FileBackedRowContainer{}

// GetRow interface for temp file get an enc row
func (f *FileBackedRowContainer) GetRow(keyRow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	err := keyRow[len(keyRow)-1].EnsureDecoded(&sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}, &f.datumAlloc)
	if err != nil {
		return nil, err
	}
	tmp := []byte(*keyRow[len(keyRow)-1].Datum.(*tree.DBytes))
	val, err := f.frc.FileStore.GetBlockForRead(tmp, f.appendTo[:0], false)
	if err != nil {
		return nil, err
	}
	for i, orderInfo := range f.frc.originOrder {
		// Types with composite key encodings are decoded from the value.
		if sqlbase.HasCompositeKeyEncoding(f.frc.originTypes[orderInfo.ColIdx].SemanticType) {
			continue
		}
		col := orderInfo.ColIdx
		f.scratchEncRow[col] = keyRow[i]
	}
	for _, i := range f.frc.valueIdx {
		var err error
		f.scratchEncRow[i], val, err = sqlbase.EncDatumFromBuffer(&f.frc.originTypes[i], sqlbase.DatumEncoding_VALUE, val)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode row")
		}
	}
	for i := range f.scratchEncRow {
		err := f.scratchEncRow[i].EnsureDecoded(&f.frc.originTypes[i], &f.datumAlloc)
		if err != nil {
			return nil, err
		}
	}
	return f.scratchEncRow, nil
}

// Init init file backed row container
func (f *FileBackedRowContainer) Init(
	ordering sqlbase.ColumnOrdering,
	types []sqlbase.ColumnType,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor, fmemoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
	rowCapacity int,
) {
	mrc := MemRowContainer{}
	mrc.InitWithMon(ordering, types, evalCtx, memoryMonitor, rowCapacity)
	f.mrc = &mrc
	f.src = &mrc
	f.engine = engine
	f.fileMemMonitor = memoryMonitor
	f.diskMonitor = diskMonitor
}

func (f *FileBackedRowContainer) spillIfMemErr(ctx context.Context, err error) (bool, error) {
	if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgcode.OutOfMemory) {
		return false, nil
	}
	if spillErr := f.SpillToFile(ctx); spillErr != nil {
		return false, spillErr
	}
	log.VEventf(ctx, 2, "spilled to disk: %v", err)
	return true, nil
}

// SpillToFile when mem overflow
func (f *FileBackedRowContainer) SpillToFile(ctx context.Context) error {
	f.mrc.memAcc.Clear(ctx)
	if f.UsingFile() {
		return errors.New("already using file")
	}
	f.scratchEncRow = make(sqlbase.EncDatumRow, len(f.mrc.types))
	frc := f.MakeFileRowContainer(f.diskMonitor, f.mrc.types, f.mrc.ordering, f.engine)
	err := frc.FileStore.Open()
	if err != nil {
		return err
	}
	frc.Add(1)
	go frc.dumpKeyRow(ctx)
	i := f.mrc.NewIterator(ctx)
	defer i.Close()
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		memRow, err := i.Row()
		if err != nil {
			return err
		}
		if err := frc.AddRow(ctx, memRow); err != nil {
			return err
		}
	}
	f.mrc.Clear(ctx)

	f.src = frc
	f.frc = frc
	f.spilled = true
	return nil
}

// Spilled returns whether or not the DiskBackedRowContainer spilled to disk
// in its lifetime.
func (f *FileBackedRowContainer) Spilled() bool {
	return f.spilled
}

// UsingFile unimpl
func (f *FileBackedRowContainer) UsingFile() bool {
	return f.frc != nil
}

// OriginOrder return origin ordering col
func (f *FileBackedRowContainer) OriginOrder() sqlbase.ColumnOrdering {
	return f.frc.originOrder
}

// OriginTypes return origin ordering col types
func (f *FileBackedRowContainer) OriginTypes() []sqlbase.ColumnType {
	return f.frc.originTypes
}

// ValueIdx value idx
func (f *FileBackedRowContainer) ValueIdx() []int {
	return f.frc.valueIdx
}

// HashFileRowContainer base data struct
type HashFileRowContainer struct {
	sync.WaitGroup
	FileStore   *Hstore
	originOrder sqlbase.ColumnOrdering
	originTypes []sqlbase.ColumnType
	valueIdx    []int
	datumAlloc  sqlbase.DatumAlloc
	scratchVal  []byte
	realval     []byte
	cmpRow      sqlbase.EncDatumRow

	offset     uint32
	releaseBuf []byte
	fbid       []byte

	keyRowRecv    chan sqlbase.EncDatumRows
	keyRowCollect chan sqlbase.EncDatumRows
	curBuffer     sqlbase.EncDatumRows
	keyRowIdx     int

	drc       *HashDiskBackedRowContainer
	newEqCols columns
}

// Init inti hash file container
func (h *HashFileRowContainer) Init(
	ctx context.Context,
	shouldMark bool,
	types []sqlbase.ColumnType,
	storedEqCols columns,
	encodeNull bool,
) error {
	return nil
}

func (h *HashFileRowContainer) dumpKeyRow(ctx context.Context) {
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
		h.Done()
		close(h.keyRowCollect)
	}()

	for {
		buf, ok := <-h.keyRowRecv
		if ok {
			for i := range buf {
				err := h.drc.AddRow(ctx, buf[i])
				if err != nil {
					return
				}
			}
			h.keyRowCollect <- buf
		} else {
			return
		}
	}
}

// AddRow add row to hash file container
func (h *HashFileRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	var err error
	h.realval = h.realval[:4]
	for _, i := range h.valueIdx {
		h.scratchVal, err = row[i].Encode(&h.originTypes[i], &h.datumAlloc, sqlbase.DatumEncoding_VALUE, h.scratchVal)
		if err != nil {
			return err
		}
	}
	h.realval[0] = byte(len(h.scratchVal))
	h.realval[1] = byte(len(h.scratchVal) >> 8)
	h.realval[2] = byte(len(h.scratchVal) >> 16)
	h.realval[3] = byte(len(h.scratchVal) >> 24)
	h.realval = append(h.realval, h.scratchVal...)
	if h.offset+uint32(len(h.realval)) > blockSize {
		h.offset = 0
		err = h.FileStore.ReleaseBlock(h.fbid, h.releaseBuf)
		if err != nil {
			return err
		}
		h.fbid, h.releaseBuf, err = h.FileStore.GetBlockForWrite(h.fbid)
	}
	copy(h.releaseBuf[h.offset:int(h.offset)+len(h.realval)], h.realval)
	for i, o := range h.originOrder {
		h.cmpRow[i] = row[o.ColIdx]
	}
	h.fbid = append(h.fbid, byte(h.offset))
	h.fbid = append(h.fbid, byte(h.offset>>8))
	h.fbid = append(h.fbid, byte(h.offset>>16))
	h.fbid = append(h.fbid, byte(h.offset>>24))
	idrow := sqlbase.EncBytesDatum(sqlbase.DatumEncoding_VALUE, h.fbid)
	h.cmpRow[len(h.originOrder)] = idrow
	if h.curBuffer == nil {
		h.curBuffer = <-h.keyRowCollect
	}
	if len(h.curBuffer[h.keyRowIdx]) == 0 {
		h.curBuffer[h.keyRowIdx] = make(sqlbase.EncDatumRow, len(h.cmpRow))
	}
	copy(h.curBuffer[h.keyRowIdx], h.cmpRow)
	h.keyRowIdx++
	if h.keyRowIdx == KeyRowBufferLen {
		h.keyRowRecv <- h.curBuffer
		h.keyRowIdx = 0
		h.curBuffer = nil
	}
	h.fbid = h.fbid[:3]
	h.offset = h.offset + uint32(len(h.realval))
	h.scratchVal = h.scratchVal[:0]
	return nil
}

// IsDisk unimpl
func (h *HashFileRowContainer) IsDisk() bool {
	panic("implement me")
}

// NewBucketIterator iterator for hash bucket
func (h *HashFileRowContainer) NewBucketIterator(
	ctx context.Context, row sqlbase.EncDatumRow, probeEqCols columns,
) (RowMarkerIterator, error) {
	if h.offset != 0 {
		if err := h.FileStore.ReleaseBlock(h.fbid, h.releaseBuf); err != nil {
			return nil, err
		}
	}
	if h.curBuffer != nil {
		h.keyRowRecv <- h.curBuffer[:h.keyRowIdx]
	}
	close(h.keyRowRecv)
	h.Wait()
	h.FileStore.WriteDone()
	Iterator, err := h.drc.NewBucketIterator(ctx, row, probeEqCols)
	if err != nil {
		return nil, err
	}
	return &hashFileRowBucketIterator{
		row:           Iterator,
		File:          h,
		scratchEncRow: make(sqlbase.EncDatumRow, len(h.originTypes)),
	}, nil
}

// NewBucketIteratorOpt new hash bucket iter for hash parallel
func (h *HashFileRowContainer) NewBucketIteratorOpt(
	ctx context.Context, row sqlbase.EncDatumRow, probeEqCols columns,
) (RowMarkerIterator, error) {
	panic("implement me")
}

// NewUnmarkedIterator iter for non-inner hash
func (h *HashFileRowContainer) NewUnmarkedIterator(ctx context.Context) RowIterator {
	Iterator := h.drc.NewUnmarkedIterator(ctx)
	return &fileRowIterator{
		Iterator:      Iterator,
		File:          h,
		scratchEncRow: make(sqlbase.EncDatumRow, len(h.originTypes)),
	}
}

// Close close hash file container
func (h *HashFileRowContainer) Close(ctx context.Context) {
	h.drc.Close(ctx)
	//h.FileStore.Close()
}

// FlushDir unimpl
func (h *HashFileRowContainer) FlushDir() error {
	panic("implement me")
}

// HashFileBackedRowContainer base data struct
type HashFileBackedRowContainer struct {
	src  HashRowContainer
	hmrc *HashMemRowContainer
	//fdbrc *HashDiskBackedRowContainer
	frc *HashFileRowContainer

	shouldMark   bool
	types        []sqlbase.ColumnType
	storedEqCols columns
	encodeNull   bool

	// mrc is used to build HashMemRowContainer upon.
	mrc *MemRowContainer

	evalCtx        *tree.EvalContext
	memoryMonitor  *mon.BytesMonitor
	diskMonitor    *mon.BytesMonitor
	fmemoryMonitor *mon.BytesMonitor
	engine         diskmap.Factory
	scratchEncRow  sqlbase.EncDatumRow

	// allRowsIterators keeps track of all iterators created via
	// NewAllRowsIterator(). If the container spills to disk, these become
	// invalid, so the container actively recreates the iterators, advances them
	// to appropriate positions, and updates each iterator in-place.
	allRowsIterators []*AllRowsIterator
	FileStore        *Hstore
}

// MayBeCloseDumpKeyRow close key row channel
func (h *HashFileBackedRowContainer) MayBeCloseDumpKeyRow() {
	if h.frc != nil {
		close(h.frc.keyRowRecv)
	}
}

// Init inti hash file backed
func (h *HashFileBackedRowContainer) Init(
	ctx context.Context,
	shouldMark bool,
	types []sqlbase.ColumnType,
	storedEqCols columns,
	encodeNull bool,
) error {
	h.shouldMark = shouldMark
	h.types = types
	h.storedEqCols = storedEqCols
	h.encodeNull = encodeNull
	if shouldMark {
		// We might need to preserve the marks when spilling to disk which requires
		// adding an extra boolean column to the row when read from memory.
		h.scratchEncRow = make(sqlbase.EncDatumRow, len(types)+1)
	}

	// Provide the MemRowContainer with an ordering on the equality columns of
	// the rows that we will store. This will result in rows with the
	// same equality columns occurring contiguously in the keyspace.
	ordering := make(sqlbase.ColumnOrdering, len(storedEqCols))
	for i := range ordering {
		ordering[i] = sqlbase.ColumnOrderInfo{
			ColIdx:    int(storedEqCols[i]),
			Direction: encoding.Ascending,
		}
	}
	if h.mrc == nil {
		h.mrc = &MemRowContainer{}
		h.mrc.InitWithMon(ordering, types, h.evalCtx, h.memoryMonitor, 0 /* rowCapacity */)
	}
	hmrc := MakeHashMemRowContainer(h.mrc)
	h.hmrc = &hmrc
	h.src = h.hmrc
	if err := h.hmrc.Init(ctx, shouldMark, types, storedEqCols, encodeNull); err != nil {
		if spilled, spillErr := h.spillIfMemErr(ctx, err); !spilled && spillErr == nil {
			// The error was not an out of memory error.
			return err
		} else if spillErr != nil {
			// A disk spill was attempted but there was an error in doing so.
			return spillErr
		}
	}

	return nil
}

func (h *HashFileBackedRowContainer) spillIfMemErr(ctx context.Context, err error) (bool, error) {
	if !sqlbase.IsOutOfMemoryError(err) {
		return false, nil
	}
	if spillErr := h.SpillToFile(ctx); spillErr != nil {
		return false, spillErr
	}
	log.VEventf(ctx, 2, "spilled to file: %v", err)
	return true, nil
}

// AddRow add rows
func (h *HashFileBackedRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if err := h.src.AddRow(ctx, row); err != nil {
		if spilled, spillErr := h.spillIfMemErr(ctx, err); !spilled && spillErr == nil {
			// The error was not an out of memory error.
			return err
		} else if spillErr != nil {
			// A disk spill was attempted but there was an error in doing so.
			return spillErr
		}
		// Add the row that caused the memory error.
		return h.src.AddRow(ctx, row)
	}
	return nil
}

// IsDisk unimpl
func (h *HashFileBackedRowContainer) IsDisk() bool {
	panic("implement me")
}

// NewBucketIterator new bucket iter
func (h *HashFileBackedRowContainer) NewBucketIterator(
	ctx context.Context, row sqlbase.EncDatumRow, probeEqCols columns,
) (RowMarkerIterator, error) {
	return h.src.NewBucketIterator(ctx, row, probeEqCols)
}

// NewBucketIteratorOpt unimpl
func (h *HashFileBackedRowContainer) NewBucketIteratorOpt(
	ctx context.Context, row sqlbase.EncDatumRow, probeEqCols columns,
) (RowMarkerIterator, error) {
	panic("implement me")
}

// NewUnmarkedIterator final iter
func (h *HashFileBackedRowContainer) NewUnmarkedIterator(ctx context.Context) RowIterator {
	return h.src.NewUnmarkedIterator(ctx)
}

// Close close hash file backed
func (h *HashFileBackedRowContainer) Close(ctx context.Context) {
	h.hmrc.Close(ctx)
	if err := h.FileStore.Close(); err != nil {
		panic(err)
	}
	if h.frc != nil {
		h.frc.Close(ctx)
	}
}

// FlushDir unimpl
func (h *HashFileBackedRowContainer) FlushDir() error {
	panic("implement me")
}

// SpillToFile when mem overflow
func (h *HashFileBackedRowContainer) SpillToFile(ctx context.Context) error {
	if h.UsingFile() {
		return errors.New("already using disk")
	}
	ordering := make(sqlbase.ColumnOrdering, len(h.storedEqCols))
	for i := range ordering {
		ordering[i] = sqlbase.ColumnOrderInfo{
			ColIdx:    int(h.storedEqCols[i]),
			Direction: encoding.Ascending,
		}
	}
	err := h.FileStore.Open()
	if err != nil {
		return err
	}
	frc, err := h.MakeHashFileRowContainer(ctx, h.diskMonitor, h.mrc.types, ordering, h.engine)
	if err != nil {
		return err
	}
	frc.Add(1)
	go frc.dumpKeyRow(ctx)

	// rowIdx is used to look up the mark on the row and is only updated and used
	// if marks are present.
	//rowIdx := 0
	i := h.hmrc.NewFinalIterator(ctx)
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
		//if h.shouldMark && h.hmrc.marked != nil {
		//	// We need to preserve the mark on this row.
		//	copy(h.scratchEncRow, row)
		//	h.scratchEncRow[len(h.types)] = sqlbase.EncDatum{Datum: tree.MakeDBool(tree.DBool(h.hmrc.marked[rowIdx]))}
		//	row = h.scratchEncRow
		//	rowIdx++
		//}
		if err := frc.AddRow(ctx, row); err != nil {
			return err
		}
	}
	h.hmrc.Clear(ctx)

	h.src = frc
	h.frc = frc

	return nil
}

// MakeHashFileRowContainer make hash file row container
func (h *HashFileBackedRowContainer) MakeHashFileRowContainer(
	ctx context.Context,
	monitor *mon.BytesMonitor,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
	engine diskmap.Factory,
) (*HashFileRowContainer, error) {
	var hfrc HashFileRowContainer
	hfrc.fbid = make([]byte, 3)
	hfrc.realval = make([]byte, 4)
	hfrc.cmpRow = make(sqlbase.EncDatumRow, len(h.storedEqCols)+1)
	hfrc.originOrder = ordering
	hfrc.originTypes = types

	hfrc.keyRowRecv = make(chan sqlbase.EncDatumRows, 2)
	hfrc.keyRowCollect = make(chan sqlbase.EncDatumRows, 2)
	for i := 0; i < 2; i++ {
		hfrc.keyRowCollect <- make(sqlbase.EncDatumRows, KeyRowBufferLen)
	}
	hfrc.fbid, hfrc.releaseBuf, _ = h.FileStore.GetBlockForWrite(hfrc.fbid)
	orderingIdxs := make(map[int]struct{})
	for _, orderInfo := range ordering {
		orderingIdxs[orderInfo.ColIdx] = struct{}{}
	}
	hfrc.valueIdx = make([]int, 0, len(types))
	for i := range types {
		// TODO(asubiotto): A datum of a type for which HasCompositeKeyEncoding
		// returns true may not necessarily need to be encoded in the value, so
		// make this more fine-grained. See IsComposite() methods in
		// pkg/sql/parser/datum.go.
		if _, ok := orderingIdxs[i]; !ok || sqlbase.HasCompositeKeyEncoding(types[i].SemanticType) {
			hfrc.valueIdx = append(hfrc.valueIdx, i)
		}
	}

	var newTypes = make([]sqlbase.ColumnType, 0, len(ordering)+1)
	var newOrder = make(sqlbase.ColumnOrdering, 0, len(ordering))
	var newEqCols = make(columns, 0, len(ordering))
	for i := range ordering {
		newTypes = append(newTypes, types[ordering[i].ColIdx])
		newOrder = append(newOrder, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: ordering[i].Direction})
		newEqCols = append(newEqCols, uint32(i))
	}
	hfrc.newEqCols = newEqCols
	newTypes = append(newTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES})
	hfrc.FileStore = h.FileStore

	mrc := &MemRowContainer{}
	mrc.InitWithMon(newOrder, newTypes, h.evalCtx, h.fmemoryMonitor, 0 /* rowCapacity */)
	hdrc := MakeHashDiskBackedRowContainer(mrc, h.evalCtx, h.fmemoryMonitor, h.diskMonitor, h.engine, true)
	if err := hdrc.Init(ctx, h.shouldMark, newTypes, newEqCols, h.encodeNull); err != nil {
		return &hfrc, err
	}
	hfrc.drc = &hdrc
	return &hfrc, nil
}

// UsingFile using file or not
func (h *HashFileBackedRowContainer) UsingFile() bool {
	return h.frc != nil
}

// ReserveMarkMemoryMaybe alloc mark mem
func (h *HashFileBackedRowContainer) ReserveMarkMemoryMaybe(ctx context.Context) error {
	if h.frc == nil {
		if err := h.hmrc.ReserveMarkMemoryMaybe(ctx); err != nil {
			return err
		}
	} else if !h.frc.drc.UsingDisk() {
		// We're assuming that the disk space is infinite, so we only need to
		// reserve the memory for marks if we're using in-memory container.
		if err := h.frc.drc.ReserveMarkMemoryMaybe(ctx); err != nil {
			return h.frc.drc.SpillToDisk(ctx)
		}
	}
	return nil
}

// MakeHashFileBackedRowContainer make hash file backed
func MakeHashFileBackedRowContainer(
	mrc *MemRowContainer,
	evalCtx *tree.EvalContext,
	memoryMonitor *mon.BytesMonitor,
	fmemoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
	engine diskmap.Factory,
	FileStore *Hstore,
) HashFileBackedRowContainer {
	return HashFileBackedRowContainer{
		mrc:              mrc,
		evalCtx:          evalCtx,
		memoryMonitor:    memoryMonitor,
		fmemoryMonitor:   fmemoryMonitor,
		diskMonitor:      diskMonitor,
		engine:           engine,
		FileStore:        FileStore,
		allRowsIterators: make([]*AllRowsIterator, 0, 1),
	}
}

var _ RowMarkerIterator = &hashFileRowBucketIterator{}

type hashFileRowBucketIterator struct {
	row           RowMarkerIterator
	File          *HashFileRowContainer
	scratchEncRow sqlbase.EncDatumRow
	datumAlloc    sqlbase.DatumAlloc

	appendTo []byte
}

func (h *hashFileRowBucketIterator) Rewind() {
	h.row.Rewind()
}

func (h *hashFileRowBucketIterator) Valid() (bool, error) {
	return h.row.Valid()
}

func (h *hashFileRowBucketIterator) Next() {
	h.row.Next()
}

func (h *hashFileRowBucketIterator) Row() (sqlbase.EncDatumRow, error) {
	keyRow, err := h.row.Row()
	if err != nil {
		return nil, err
	}

	err = keyRow[len(keyRow)-1].EnsureDecoded(&sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}, &h.datumAlloc)
	if err != nil {
		return nil, err
	}
	tmp := []byte(*keyRow[len(keyRow)-1].Datum.(*tree.DBytes))

	h.appendTo = h.appendTo[:0]
	val, err := h.File.FileStore.GetBlockForRead(tmp, h.appendTo, false)
	if err != nil {
		return nil, err
	}
	for i, orderInfo := range h.File.originOrder {
		// Types with composite key encodings are decoded from the value.
		if sqlbase.HasCompositeKeyEncoding(h.File.originTypes[orderInfo.ColIdx].SemanticType) {
			continue
		}
		col := orderInfo.ColIdx
		h.scratchEncRow[col] = keyRow[i]
	}
	for _, i := range h.File.valueIdx {
		var err error
		h.scratchEncRow[i], val, err = sqlbase.EncDatumFromBuffer(&h.File.originTypes[i], sqlbase.DatumEncoding_VALUE, val)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode row")
		}
	}
	for i := range h.scratchEncRow {
		err := h.scratchEncRow[i].EnsureDecoded(&h.File.originTypes[i], &h.datumAlloc)
		if err != nil {
			return nil, err
		}
	}
	return h.scratchEncRow, nil

}

func (h *hashFileRowBucketIterator) RowOpt(enc *sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	panic("implement me")
}

func (h *hashFileRowBucketIterator) Close() {
	h.row.Close()
}

func (h *hashFileRowBucketIterator) Reset(ctx context.Context, row sqlbase.EncDatumRow) error {
	return h.row.Reset(ctx, row)
}

func (h *hashFileRowBucketIterator) ResetOpt(ctx context.Context, row sqlbase.EncDatumRow) error {
	return h.row.ResetOpt(ctx, row)
}

func (h *hashFileRowBucketIterator) Mark(ctx context.Context, mark bool) error {
	return h.row.Mark(ctx, mark)
}

func (h *hashFileRowBucketIterator) IsMarked(ctx context.Context) bool {
	return h.row.IsMarked(ctx)
}

var _ RowIterator = &fileRowIterator{}

type fileRowIterator struct {
	Iterator      RowIterator
	File          *HashFileRowContainer
	scratchEncRow sqlbase.EncDatumRow
	datumAlloc    sqlbase.DatumAlloc

	appendTo []byte
}

func (f *fileRowIterator) Rewind() {
	f.Iterator.Rewind()
}

func (f *fileRowIterator) Valid() (bool, error) {
	return f.Iterator.Valid()
}

func (f *fileRowIterator) Next() {
	f.Iterator.Next()
}

func (f *fileRowIterator) Row() (sqlbase.EncDatumRow, error) {
	keyRow, err := f.Iterator.Row()
	if err != nil {
		return nil, err
	}

	err = keyRow[len(keyRow)-1].EnsureDecoded(&sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}, &f.datumAlloc)
	if err != nil {
		return nil, err
	}
	tmp := []byte(*keyRow[len(keyRow)-1].Datum.(*tree.DBytes))

	f.appendTo = f.appendTo[:0]
	val, err := f.File.FileStore.GetBlockForRead(tmp, f.appendTo, false)
	if err != nil {
		return nil, err
	}
	for i, orderInfo := range f.File.originOrder {
		// Types with composite key encodings are decoded from the value.
		if sqlbase.HasCompositeKeyEncoding(f.File.originTypes[orderInfo.ColIdx].SemanticType) {
			continue
		}
		col := orderInfo.ColIdx
		f.scratchEncRow[col] = keyRow[i]
	}
	for _, i := range f.File.valueIdx {
		var err error
		f.scratchEncRow[i], val, err = sqlbase.EncDatumFromBuffer(&f.File.originTypes[i], sqlbase.DatumEncoding_VALUE, val)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode row")
		}
	}
	for i := range f.scratchEncRow {
		err := f.scratchEncRow[i].EnsureDecoded(&f.File.originTypes[i], &f.datumAlloc)
		if err != nil {
			return nil, err
		}
	}
	return f.scratchEncRow, nil
}

func (f *fileRowIterator) RowOpt(enc *sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	panic("implement me")
}

func (f *fileRowIterator) Close() {
	f.Iterator.Close()
}
