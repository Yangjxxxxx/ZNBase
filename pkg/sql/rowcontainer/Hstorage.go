package rowcontainer

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/sysutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

const blockSize uint32 = 64 << 20
const blockSizeOffset = 28
const readCache = 4
const removeCache = 0
const offsetLength = 7

// HstoreMemMgr tempfile mem mgr
type HstoreMemMgr struct {
	initBlockNum int
	blockPool    sync.Pool
}

// NewHstoreMemMgr New temp file mem mgr
func NewHstoreMemMgr(blockNum int) *HstoreMemMgr {
	return &HstoreMemMgr{
		initBlockNum: blockNum,
		blockPool: sync.Pool{New: func() interface{} {
			return make([]byte, blockSize)
		}},
	}
}

// Start start HstoreMemMgr
func (HsMgr *HstoreMemMgr) Start() {
	for i := 0; i < HsMgr.initBlockNum; i++ {
		HsMgr.blockPool.Put(make([]byte, blockSize))
	}
}

// GetBlock get mem block
func (HsMgr *HstoreMemMgr) GetBlock() []byte {
	return HsMgr.blockPool.Get().([]byte)
}

// Close close the hstore
func (HsMgr *HstoreMemMgr) Close() {

}

// Batch is a base data struct
type Batch struct {
	Data [][]byte
	len  int
}

type batchCopy struct {
	idx    int
	offset []byte
}

type batchCopyArr struct {
	batchArr []batchCopy
	len      int
}
type blockItem struct {
	fbID int
	ts   time.Time
	hits int64
}

type blockItems []blockItem
type blockCache struct {
	syncutil.RWMutex
	timer      *time.Timer
	blockMap   map[int][]byte
	blockQueue blockItems
	blockTsMap map[int]*blockItem
}

// Hstore is the base data struct
type Hstore struct {
	rc              blockCache
	R               chan struct{}
	FileList        []*os.File
	PeerFileBlocks  int
	TempPath        string
	FilePrefix      string
	lastBlockNumber int32
	bca             *batchCopyArr
	appendTo        []byte
	inMemData       [][]byte
	HsMgr           *HstoreMemMgr
}

func (bi blockItems) Less(i, j int) bool {
	return (int64(timeutil.Since(bi[i].ts)) * bi[i].hits) > (int64(timeutil.Since(bi[j].ts)) * bi[j].hits)
}

func (bi blockItems) Swap(i, j int) {
	bi[i], bi[j] = bi[j], bi[i]
}

func (bi blockItems) Len() int {
	return len(bi)
}

func (bca batchCopyArr) Less(i, j int) bool {
	offset1 := int(bca.batchArr[i].offset[0]) | int(bca.batchArr[i].offset[1])<<8 | int(bca.batchArr[i].offset[2])<<16
	offset2 := int(bca.batchArr[j].offset[0]) | int(bca.batchArr[j].offset[1])<<8 | int(bca.batchArr[j].offset[2])<<16
	return offset1 < offset2
}

func (bca batchCopyArr) Swap(i, j int) {
	bca.batchArr[i], bca.batchArr[j] = bca.batchArr[j], bca.batchArr[i]
}

func (bca batchCopyArr) Len() int {
	return bca.len
}

// Init init hstore
func (hs *Hstore) Init(
	blockNum int, initFileNums int, fPrefix string, tempPath string, HsMgr *HstoreMemMgr,
) {
	hs.FileList = make([]*os.File, 0, initFileNums)
	hs.TempPath = tempPath
	hs.FilePrefix = fPrefix
	hs.PeerFileBlocks = blockNum
	hs.lastBlockNumber = -1
	hs.inMemData = make([][]byte, initFileNums)
	hs.rc.blockMap = make(map[int][]byte, readCache)
	hs.rc.blockTsMap = make(map[int]*blockItem, readCache)
	hs.rc.blockQueue = make(blockItems, 0, readCache)
	hs.R = make(chan struct{}, 1)
	hs.HsMgr = HsMgr
}

// Open open hstore
func (hs *Hstore) Open() error {
	for i := range hs.inMemData {
		hs.inMemData[i] = hs.HsMgr.GetBlock()
	}
	return nil
}

// GetBlockForWrite get a mem block
func (hs *Hstore) GetBlockForWrite(fbID []byte) (b []byte, data []byte, err error) {
	hs.lastBlockNumber++
	fileIdx := int(hs.lastBlockNumber) / hs.PeerFileBlocks
	blockIdx := int(hs.lastBlockNumber) % hs.PeerFileBlocks
	if hs.lastBlockNumber < int32(len(hs.inMemData)) {
		fbID[0] = byte(blockIdx)
		fbID[1] = byte(fileIdx)
		fbID[2] = byte(fileIdx>>8) | 0x80
		return fbID, hs.inMemData[blockIdx], nil
	}
	if fileIdx >= len(hs.FileList) {
		s := fmt.Sprintf(hs.TempPath+"/"+hs.FilePrefix+"_%d", fileIdx)
		fd, err := os.OpenFile(s, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, nil, err
		}
		if err := fd.Truncate(int64(hs.PeerFileBlocks << blockSizeOffset)); err != nil {
			return nil, nil, err
		}
		hs.FileList = append(hs.FileList, fd)
	}
	dataMap, err := sysutil.Mmap(int(hs.FileList[fileIdx].Fd()), int64(blockIdx<<blockSizeOffset), int(blockSize), sysutil.ProtWrite, sysutil.MapShared)
	if err != nil {
		return nil, nil, err
	}
	fbID[0] = byte(blockIdx)
	fbID[1] = byte(fileIdx)
	fbID[2] = byte(fileIdx >> 8)
	return fbID, dataMap, nil
}

// GetBlockForRead read from a mem block
func (hs *Hstore) GetBlockForRead(offset, appendTo []byte, isBatch bool) ([]byte, error) {
	blockIdx := int(offset[0])
	fIdx := int(offset[1]) | int(offset[2])<<8
	voff := int(offset[3:][0]) | int(offset[3:][1])<<8 | int(offset[3:][2])<<16 | int(offset[3:][3])<<24
	var readmap []byte
	var vlen int
	if offset[2]&0x80 != 0 {
		readmap = hs.inMemData[blockIdx]
		vlen |= int(readmap[voff : voff+4][0]) << 0
		vlen |= int(readmap[voff : voff+4][1]) << 8
		vlen |= int(readmap[voff : voff+4][2]) << 16
		vlen |= int(readmap[voff : voff+4][3]) << 24
		appendTo = append(appendTo, readmap[voff+4:voff+4+vlen]...)
		return appendTo, nil
	}
	bc := fIdx<<8 | blockIdx
	if fIdx >= len(hs.FileList) {
		return nil, fmt.Errorf("out of file range")
	}
	if !isBatch {
		defer hs.rc.RUnlock()
		hs.rc.RLock()
	}
	if blockMap, ok := hs.rc.blockMap[bc]; ok {
		hs.rc.blockTsMap[bc].ts = timeutil.Now()
		atomic.AddInt64(&hs.rc.blockTsMap[bc].hits, 1)
		readmap = blockMap
	} else {
		b, err := sysutil.Mmap(int(hs.FileList[fIdx].Fd()), int64(blockIdx<<blockSizeOffset), int(blockSize), sysutil.ProtRead, sysutil.MapPrivate)
		if err != nil {
			return nil, err
		}
		hs.rc.blockQueue = append(hs.rc.blockQueue, blockItem{fbID: bc, ts: timeutil.Now(), hits: 1})
		hs.rc.blockTsMap[bc] = &hs.rc.blockQueue[len(hs.rc.blockQueue)-1]
		hs.rc.blockMap[bc] = b
		readmap = b
	}
	vlen |= int(readmap[voff : voff+4][0]) << 0
	vlen |= int(readmap[voff : voff+4][1]) << 8
	vlen |= int(readmap[voff : voff+4][2]) << 16
	vlen |= int(readmap[voff : voff+4][3]) << 24

	appendTo = append(appendTo, readmap[voff+4:voff+4+vlen]...)
	return appendTo, nil
}

// BatchRead Batch read
func (hs *Hstore) BatchRead(batch *Batch) {
	if hs.bca == nil {
		hs.bca = new(batchCopyArr)
		hs.bca.batchArr = make([]batchCopy, batch.len)
	}
	hs.bca.len = batch.len
	for i := 0; i < hs.bca.len; i++ {
		hs.bca.batchArr[i].idx = i
		hs.bca.batchArr[i].offset = batch.Data[i]
	}
	sort.Sort(hs.bca)
	hs.rc.RLock()
	for i := 0; i < hs.bca.len; i++ {
		hs.appendTo = hs.appendTo[:0]
		rs, _ := hs.GetBlockForRead(hs.bca.batchArr[i].offset[:7], hs.appendTo, true)
		batch.Data[hs.bca.batchArr[i].idx] = append(batch.Data[hs.bca.batchArr[i].idx][:0], rs...)
	}
	hs.rc.RUnlock()
}

// GetBatchForRead get a Batch for read
func (hs *Hstore) GetBatchForRead(size int) *Batch {
	ba := new(Batch)
	ba.Data = make([][]byte, size)
	for i := range ba.Data {
		ba.Data[i] = make([]byte, offsetLength)
	}
	return ba
}

// ApplyOffsetToBacth apply Batch to block
func (hs *Hstore) ApplyOffsetToBacth(offset []byte, ba *Batch) {
	ba.Data[ba.len] = append(ba.Data[ba.len][:0], offset...)
	ba.len++
}

// ResetBatch reset Batch
func (hs *Hstore) ResetBatch(ba *Batch) {
	ba.len = 0
}

// WriteDone write end
func (hs *Hstore) WriteDone() {
	if hs.lastBlockNumber >= int32(len(hs.inMemData)) {
		go hs.CollectReadCache()
	}
}

// CollectReadCache collect block  cache
func (hs *Hstore) CollectReadCache() {
	hs.rc.timer = time.NewTimer(time.Millisecond * 1000)
	for {
		select {
		case <-hs.rc.timer.C:
			hs.rc.Lock()
			if len(hs.rc.blockQueue) > readCache {
				rCount := len(hs.rc.blockQueue) - readCache + removeCache
				sort.Sort(hs.rc.blockQueue)
				for i := 0; i < rCount; i++ {
					d := hs.rc.blockQueue[i]
					if err := sysutil.Munmap(hs.rc.blockMap[d.fbID]); err != nil {
						panic(err)
					}
					delete(hs.rc.blockMap, d.fbID)
					delete(hs.rc.blockTsMap, d.fbID)
				}
				hs.rc.blockQueue = hs.rc.blockQueue[rCount:]
			}
			hs.rc.Unlock()
			hs.rc.timer.Reset(1000 * time.Millisecond)
		case <-hs.R:
			hs.rc.timer.Stop()
			return
		}
	}
}

// ReleaseBlock release mem block
func (hs *Hstore) ReleaseBlock(fbid, data []byte) error {
	if fbid[2]&0x80 == 0 {
		if err := sysutil.Munmap(data); err != nil {
			return err
		}
	}
	return nil
}

// Close hstore close
func (hs *Hstore) Close() error {
	hs.R <- struct{}{}
	for _, f := range hs.FileList {
		err := f.Close()
		if err != nil {
			return err
		}
		err = os.Remove(f.Name())
		if err != nil {
			return err
		}
	}
	for r := range hs.rc.blockMap {
		err := sysutil.Munmap(hs.rc.blockMap[r])
		if err != nil {
			return err
		}
	}
	for i := range hs.inMemData {
		hs.HsMgr.blockPool.Put(hs.inMemData[i])
	}
	return nil
}
