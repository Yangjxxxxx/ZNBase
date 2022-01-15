package rowexec

import (
	"hash/crc32"
	"sync"

	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// HashBlockSize hash opt blocksize
const HashBlockSize = 64
const joinWorkerChannelLen = 4
const resCHLen = 64

// HashJoinerOpt for hashopt switch
var HashJoinerOpt *settings.BoolSetting

// TempFileStorageAble for using tempfile instead of rocksdb
var TempFileStorageAble *settings.BoolSetting

// HashJoinerParallelNums for hashopt parallel num
var HashJoinerParallelNums *settings.IntSetting
var crc32Tb = crc32.MakeTable(crc32.Koopman)

// ReplicaTableHashJoinerOpt for replica table hashjoiner
var ReplicaTableHashJoinerOpt *settings.BoolSetting

func init() {
	HashJoinerOpt = settings.RegisterBoolSetting(
		"sql.opt.operator.hashjoiner",
		"for hashjoin opt",
		false,
	)
	HashJoinerParallelNums = settings.RegisterIntSetting(
		"sql.opt.operator.hashjoiner.parallelnum",
		"for hashjoin woker nums, max 32",
		4,
	)
	TempFileStorageAble = settings.RegisterBoolSetting(
		"sql.distsql.temp_file.experimental",
		"set to true to enable use of temp_file_store for distributed sql sorts or hash. but it's an experimental prod.",
		false,
	)
	ReplicaTableHashJoinerOpt = settings.RegisterBoolSetting(
		"sql.opt.operator.hashjoiner.replicatable.enabled",
		"for replication table hashjoin",
		true,
	)
}

//for row base compute
type joinWorker struct {
	*hashJoiner
	isGetHashRow bool
	joinWorkID   int
	hashOpt      *HashJoinOpt
	// EncRow for inner row
	EncRow sqlbase.EncDatumRow
	// OptRow for outer row
	OptRow    sqlbase.EncDatumRow
	iter      rowcontainer.RowMarkerIterator
	workerCH  chan *runbase.ChunkBuf
	collectCH chan *runbase.ChunkBuf
}

//for row base compute
type outerFetcher struct {
	*HashJoinOpt
	da       sqlbase.DatumAlloc
	bufQueue []*runbase.ChunkBuf
}

// HashJoinOpt for hash parallel
type HashJoinOpt struct {
	*outerFetcher
	sync.WaitGroup
	isClosed    bool
	hashState   uint8
	parallel    int
	isDisk      bool
	parent      *hashJoiner
	joinWorkers []*joinWorker
	resultCh    chan *runbase.ChunkRow
	resCollect  chan *runbase.ChunkRow
	// Res for row res set
	Res              runbase.ChunkRow
	isSplitHashTable bool
	appendTo         []byte
}

func (hashOpt *HashJoinOpt) initHashJoinOpt() {
	hashOpt.Res.Row = make(sqlbase.EncDatumRow, len(hashOpt.parent.combinedRow))
	hashOpt.resultCh = make(chan *runbase.ChunkRow, resCHLen)
	hashOpt.resCollect = make(chan *runbase.ChunkRow, resCHLen)
	for i := 0; i < resCHLen; i++ {
		probeCK := new(runbase.ChunkRow)
		probeCK.Row = make(sqlbase.EncDatumRow, len(hashOpt.parent.combinedRow))
		hashOpt.resCollect <- probeCK
	}
	hashOpt.hashState = 0
	hashOpt.initOuterFetcher()
	hashOpt.initJoinWorkers()
}

func (hashOpt *HashJoinOpt) initOuterFetcher() {
	hashOpt.outerFetcher = new(outerFetcher)
	hashOpt.outerFetcher.HashJoinOpt = hashOpt
	hashOpt.outerFetcher.bufQueue = make([]*runbase.ChunkBuf, hashOpt.parallel)
}

func (hashOpt *HashJoinOpt) initJoinWorkers() {
	for i, jW := range hashOpt.joinWorkers {
		jW.hashOpt = hashOpt
		jW.joinWorkID = i
		jW.workerCH = make(chan *runbase.ChunkBuf, joinWorkerChannelLen)
		jW.collectCH = make(chan *runbase.ChunkBuf, joinWorkerChannelLen)
	}
}

func (hashOpt *HashJoinOpt) startOuterAndWorker() {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	h := hashOpt.parent
	//for disk used,we should set hash parallel 1
	if !hashOpt.isSplitHashTable && h.storedRows.IsDisk() {
		hashOpt.parallel = 1
		hashOpt.joinWorkers = hashOpt.joinWorkers[:1]
		hashOpt.bufQueue = hashOpt.bufQueue[:1]
	}
	//h.Add(hashOpt.parallel)
	for _, jw := range hashOpt.joinWorkers {
		if hashOpt.parent.storedSide == rightSide {
			jw.OptRow = make(sqlbase.EncDatumRow, len(hashOpt.parent.leftSource.OutputTypes()))
			jw.EncRow = make(sqlbase.EncDatumRow, len(hashOpt.parent.rightSource.OutputTypes()))
			for i := 0; i < joinWorkerChannelLen; i++ {
				probeBuf := new(runbase.ChunkBuf)
				probeBuf.MaxIdx = -1
				probeBuf.Buffer = make([]*runbase.ChunkRow, HashBlockSize)
				for j := 0; j < HashBlockSize; j++ {
					probeCK := new(runbase.ChunkRow)
					probeCK.Row = make(sqlbase.EncDatumRow, len(hashOpt.parent.leftSource.OutputTypes()))
					probeBuf.Buffer[j] = probeCK
				}
				jw.collectCH <- probeBuf
			}
		} else {
			jw.OptRow = make(sqlbase.EncDatumRow, len(hashOpt.parent.rightSource.OutputTypes()))
			jw.EncRow = make(sqlbase.EncDatumRow, len(hashOpt.parent.leftSource.OutputTypes()))
			for i := 0; i < joinWorkerChannelLen; i++ {
				probeBuf := new(runbase.ChunkBuf)
				probeBuf.MaxIdx = -1
				probeBuf.Buffer = make([]*runbase.ChunkRow, HashBlockSize)
				for j := 0; j < HashBlockSize; j++ {
					probeCK := new(runbase.ChunkRow)
					probeCK.Row = make(sqlbase.EncDatumRow, len(hashOpt.parent.rightSource.OutputTypes()))
					probeBuf.Buffer[j] = probeCK
				}
				jw.collectCH <- probeBuf
			}
		}
	}
	go hashOpt.startOuterFetcher()
	for _, jw := range hashOpt.joinWorkers {
		h.Add(1)
		go jw.launchHashJoin()
	}
	hashOpt.Wait()
	if shouldEmitUnmatchedRow(h.HashJoinOpt.parent.storedSide, h.HashJoinOpt.parent.joinType) {
		h.HashJoinOpt.emitUnmatchedFromStoredSide()
	}
	hashOpt.Wait()
	close(hashOpt.resultCh)
	return
}

//start reading otherSide rows
func (hashOpt *HashJoinOpt) startOuterFetcher() {
	h := hashOpt.parent
	side := otherSide(h.storedSide)
	var row sqlbase.EncDatumRow
	var meta *distsqlpb.ProducerMetadata
	var emitDirectly bool
	var err error
	var res *runbase.ChunkRow
	var jwIdx = 0
	var ok bool
	defer func() {
		if err := recover(); err != nil {
		}
		for _, jw := range hashOpt.joinWorkers {
			close(jw.workerCH)
		}
	}()
	for {
		if h.isClosed {
			return
		}
		// First process the rows that were already buffered.
		if h.rows[side].Len() > 0 {
			row = h.rows[side].EncRow(0)
			h.rows[side].PopFirst()
		} else {
			row, meta, emitDirectly, err = h.receiveNext(side)
			if err != nil {
				res = <-hashOpt.resCollect
				res.Meta = &distsqlpb.ProducerMetadata{Err: err}
				hashOpt.resultCh <- res
				return
			} else if meta != nil {
				if meta.Err != nil {
					res = <-hashOpt.resCollect
					res.Meta = meta
					hashOpt.resultCh <- res
					return
				}
				res = <-hashOpt.resCollect
				res.Meta = meta
				hashOpt.resultCh <- res
				continue
			} else if emitDirectly {
				res = <-hashOpt.resCollect
				res.Meta = nil
				copy(res.Row, row)
				hashOpt.resultCh <- res
				continue
			}
		}
		if row == nil {
			for i := range hashOpt.outerFetcher.bufQueue {
				if hashOpt.outerFetcher.bufQueue[i] != nil {
					hashOpt.joinWorkers[i].workerCH <- hashOpt.outerFetcher.bufQueue[i]
				}
			}
		} else {
			if hashOpt.parallel > 1 {
				jwIdx = hashOpt.ReHash(row, otherSide(h.storedSide))
			}
			//rehash to each worker queue buf
			if hashOpt.outerFetcher.bufQueue[jwIdx] == nil {
				hashOpt.outerFetcher.bufQueue[jwIdx], ok = <-hashOpt.joinWorkers[jwIdx].collectCH
				if !ok {
					return
				}
			}
			if runbase.PushToBuf(hashOpt.outerFetcher.bufQueue[jwIdx], row, nil, HashBlockSize) {
				hashOpt.joinWorkers[jwIdx].workerCH <- hashOpt.outerFetcher.bufQueue[jwIdx]
				hashOpt.outerFetcher.bufQueue[jwIdx] = nil
			}
			continue
		}
		return
	}

}

func (hashOpt *HashJoinOpt) emitUnmatched() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*distsqlpb.ProducerMetadata,
) {
	h := hashOpt.parent
	i := h.emittingUnmatchedState.iter
	if ok, err := i.Valid(); err != nil {
		return hjStateUnknown, nil, &distsqlpb.ProducerMetadata{Err: err}
	} else if !ok {
		return hjStateUnknown, nil, &distsqlpb.ProducerMetadata{Err: nil}
	}

	if err := h.cancelChecker.Check(); err != nil {
		return hjStateUnknown, nil, &distsqlpb.ProducerMetadata{Err: err}
	}

	row, err := i.Row()
	if err != nil {
		return hjStateUnknown, nil, &distsqlpb.ProducerMetadata{Err: err}
	}
	return hjEmittingUnmatched, h.renderUnmatchedRow(row, h.storedSide), nil
}

//emit stored side rows which is unmatched(when outer join)
func (hashOpt *HashJoinOpt) emitUnmatchedFromStoredSide() {
	h := hashOpt.parent
	if hashOpt.isSplitHashTable {
		for _, jw := range hashOpt.joinWorkers {
			//if joinworker has no hashrow,next
			if !jw.isGetHashRow {
				continue
			}
			hashOpt.Add(1)
			go func(w *joinWorker) {
				defer func() {
					if err := recover(); err != nil {
					}
					hashOpt.Done()
				}()
				i := w.storedRows.NewUnmarkedIterator(h.Ctx)
				w.emittingUnmatchedState.iter = i
				i.Rewind()
				for {
					row := w.emitUnmatchedForJw()
					if row == nil {
						break
					}
					res := <-hashOpt.resCollect
					res.Meta = nil
					copy(res.Row, row)
					hashOpt.resultCh <- res
				}
			}(jw)
		}
	} else {
		i := h.storedRows.NewUnmarkedIterator(h.Ctx)
		i.Rewind()
		h.emittingUnmatchedState.iter = i
		var state hashJoinerState
		var row sqlbase.EncDatumRow
		for {
			state, row, _ = hashOpt.emitUnmatched()
			if state != hjEmittingUnmatched {
				break
			}
			row = hashOpt.parent.ProcessRowHelper(row)
			if row == nil {
				continue
			}
			res := <-hashOpt.resCollect
			res.Meta = nil
			copy(res.Row, row)
			hashOpt.resultCh <- res
			h.emittingUnmatchedState.iter.Next()
		}
	}
}

//begin hashjoin comput
func (jw *joinWorker) launchHashJoin() {
	hashOpt := jw.hashOpt
	h := hashOpt.parent
	var curBuf *runbase.ChunkBuf
	var curProbeSideRowIdx int
	var res *runbase.ChunkRow
	var matched bool
	var nextState hashJoinerState
	var renderedRow sqlbase.EncDatumRow
	var shouldEmit bool
	var ok bool
	var otherRow sqlbase.EncDatumRow
	var err error
	defer func() {
		if err := recover(); err != nil {
		}
		close(jw.collectCH)
		hashOpt.Done()
	}()
	if h.isSplitHashTable {
		jw.AllocMarkMem()
	}
label1:
	if curBuf == nil {
		curBuf, ok = <-jw.workerCH
		if !ok {
			return
		}
	}
	if curProbeSideRowIdx > curBuf.MaxIdx {
		curBuf.MaxIdx = -1
		jw.collectCH <- curBuf
		curBuf = nil
		curProbeSideRowIdx = 0
		goto label1
	}
	copy(jw.OptRow, curBuf.Buffer[curProbeSideRowIdx].Row)
	curProbeSideRowIdx++
	matched = false
	if jw.iter == nil {
		if hashOpt.isSplitHashTable {
			jw.iter, err = jw.storedRows.NewBucketIterator(h.Ctx, jw.OptRow, h.eqCols[otherSide(h.storedSide)])
		} else {
			if h.isDisk {
				jw.iter, err = h.storedRows.NewBucketIterator(h.Ctx, jw.OptRow, h.eqCols[otherSide(h.storedSide)])
			} else {
				jw.iter, err = h.storedRows.NewBucketIteratorOpt(h.Ctx, jw.OptRow, h.eqCols[otherSide(h.storedSide)])
			}
		}
		if err != nil {
			return
		}
	} else {
		if hashOpt.isSplitHashTable {
			err = jw.iter.Reset(h.Ctx, jw.OptRow)
		} else {
			err = jw.iter.ResetOpt(h.Ctx, jw.OptRow)
		}
		if err != nil {
			return
		}
	}
	jw.iter.Rewind()
	i := jw.iter
	for {
		if ok, err := i.Valid(); err != nil {
			return
		} else if !ok {
			// In this case we have reached the end of the matching bucket. Check if any
			// rows passed the ON condition. If they did, move back to
			// hjReadingProbeSide to get the next probe row.
			if matched {
				goto label1
			}
			// If not, this probe row is unmatched. Check if it needs to be emitted.
			if renderedRow, shouldEmit := jw.shouldEmitUnmatched(
				jw.OptRow, otherSide(h.storedSide),
			); shouldEmit {
				res = <-hashOpt.resCollect
				res.Meta = nil
				copy(res.Row, renderedRow)
				hashOpt.resultCh <- res
				goto label1
			}
			goto label1
		}
		if hashOpt.isSplitHashTable {
			otherRow, err = i.Row()
		} else {
			if hashOpt.isDisk {
				otherRow, err = i.Row()
			} else {
				otherRow, err = i.RowOpt(&jw.EncRow)
			}
		}
		if err != nil {
			res = <-hashOpt.resCollect
			res.Meta = &distsqlpb.ProducerMetadata{Err: err}
			hashOpt.resultCh <- res
			return
		}

		if h.storedSide == rightSide {
			renderedRow, err = jw.render(jw.OptRow, otherRow)
		} else {
			renderedRow, err = jw.render(otherRow, jw.OptRow)
		}
		if err != nil {
			res = <-hashOpt.resCollect
			res.Meta = &distsqlpb.ProducerMetadata{Err: err}
			hashOpt.resultCh <- res
			return
		}

		// If the ON condition failed, renderedRow is nil.
		if renderedRow == nil {
			i.Next()
			continue
		}
		matched = true
		shouldEmit = h.joinType != sqlbase.LeftAntiJoin && h.joinType != sqlbase.ExceptAllJoin
		if shouldMark(h.storedSide, h.joinType) {
			// Matched rows are marked on the stored side for 2 reasons.
			// 1: For outer joins, anti joins, and EXCEPT ALL to iterate through
			// the unmarked rows.
			// 2: For semi-joins and INTERSECT ALL where the left-side is stored,
			// multiple rows from the right may match to the same row on the left.
			// The rows on the left should only be emitted the first time
			// a right row matches it, then marked to not be emitted again.
			// (Note: an alternative is to remove the entry from the stored
			// side, but our containers do not support that today).
			if i.IsMarked(h.Ctx) {
				switch h.joinType {
				case sqlbase.LeftSemiJoin:
					shouldEmit = false
				case sqlbase.IntersectAllJoin:
					shouldEmit = false
				case sqlbase.ExceptAllJoin:
					// We want to mark a stored row if possible, so move on to the next
					// match. Reset h.probingRowState.matched in case we don't find any more
					// matches and want to emit this row.
					//h.probingRowState.matched = false
					matched = false
					i.Next()
					continue
				}
			} else if err := i.Mark(h.Ctx, true); err != nil {
				res = <-hashOpt.resCollect
				res.Meta = &distsqlpb.ProducerMetadata{Err: err}
				hashOpt.resultCh <- res
				return
			}
		}
		nextState = hjProbingRow
		if shouldShortCircuit(h.storedSide, h.joinType) {
			nextState = hjReadingProbeSide
		}
		if shouldEmit {
			if h.joinType == sqlbase.IntersectAllJoin {
				// We found a match, so we are done with this row.
				res = <-hashOpt.resCollect
				res.Meta = nil
				copy(res.Row, renderedRow)
				hashOpt.resultCh <- res
				i.Next()
				goto label1
			}
			res = <-hashOpt.resCollect
			res.Meta = nil
			copy(res.Row, renderedRow)
			hashOpt.resultCh <- res
		}
		if nextState == hjProbingRow {
			i.Next()
			continue
		}
		if nextState == hjReadingProbeSide {
			i.Next()
			goto label1
		}
	}
}

// ReHash for hash parallel
func (hashOpt *HashJoinOpt) ReHash(row sqlbase.EncDatumRow, side joinSide) int {
	var err error
	var typ []sqlbase.ColumnType
	if side == leftSide {
		typ = hashOpt.parent.leftSource.OutputTypes()
	} else {
		typ = hashOpt.parent.rightSource.OutputTypes()
	}
	cols := hashOpt.parent.eqCols[side]
	for _, i := range cols {
		hashOpt.appendTo, err = row[i].Encode(&typ[i], &hashOpt.outerFetcher.da, sqlbase.DatumEncoding_ASCENDING_KEY, hashOpt.appendTo)
		if err != nil {
			return 0
		}
	}
	idx := int(crc32.Update(0, crc32Tb, hashOpt.appendTo) % uint32(hashOpt.parallel))
	hashOpt.appendTo = hashOpt.appendTo[:0]
	return idx
}

// ReleaseResource for hashopt
func (hashOpt *HashJoinOpt) ReleaseResource() {
	close(hashOpt.resCollect)
	for _, jw := range hashOpt.joinWorkers {
		if hashOpt.isSplitHashTable {
			jw.jwClose()
		}
		if jw.iter != nil {
			jw.iter.Close()
		}
		if jw.iter == nil {
			if rc, ok := jw.storedRows.(*rowcontainer.HashFileBackedRowContainer); ok {
				rc.MayBeCloseDumpKeyRow()
			}
		}
	}
}

func (h *hashJoiner) jwClose() {
	if h.storedSide == rightSide {
		h.rows[leftSide].Close(h.Ctx)
	} else {
		h.rows[rightSide].Close(h.Ctx)
	}
	if h.storedRows != nil {
		h.storedRows.Close(h.Ctx)
	} else {
		// h.storedRows has not been initialized, so we need to close the stored
		// side container explicitly.
		h.rows[h.storedSide].Close(h.Ctx)
	}
	if h.probingRowState.iter != nil {
		h.probingRowState.iter.Close()
	}
	if h.emittingUnmatchedState.iter != nil {
		h.emittingUnmatchedState.iter.Close()
	}
	h.MemMonitor.Stop(h.Ctx)
	if h.fMemMonitor != nil {
		h.fMemMonitor.Stop(h.Ctx)
	}
	if h.diskMonitor != nil {
		h.diskMonitor.Stop(h.Ctx)
	}
}

func (h *hashJoiner) emitUnmatchedForJw() sqlbase.EncDatumRow {
	i := h.emittingUnmatchedState.iter
	if ok, err := i.Valid(); err != nil {
		return nil
	} else if !ok {
		// Done.
		return nil
	}
	row, err := i.Row()
	if err != nil {
		return nil
	}
	defer i.Next()
	return h.renderUnmatchedRow(row, h.storedSide)
}

// AllocMarkMem for hash opt
func (jw *joinWorker) AllocMarkMem() {
	var err error
	if rc, ok := jw.storedRows.(*rowcontainer.HashMemRowContainer); ok {
		// h.storedRows is hashMemRowContainer and not a disk backed one, so
		// h.useTempStorage is false and we cannot spill to disk, so we simply
		// will return an error if it occurs.
		err = rc.ReserveMarkMemoryMaybe(jw.Ctx)
	} else if hdbrc, ok := jw.storedRows.(*rowcontainer.HashDiskBackedRowContainer); ok {
		err = hdbrc.ReserveMarkMemoryMaybe(jw.Ctx)
	} else if hdbrc, ok := jw.storedRows.(*rowcontainer.HashFileBackedRowContainer); ok {
		err = hdbrc.ReserveMarkMemoryMaybe(jw.Ctx)
	} else {
		panic("unexpected type of storedRows in hashJoiner")
	}
	if err != nil {
		panic("can not make mark memory")
	}
}
