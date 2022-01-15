package changefeedicl

import (
	"bytes"
	"context"

	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
)

// DMLMemCacheSize DML 缓存大小
const DMLMemCacheSize = 20 >> 32

// DMLMemCache DML 缓存
type DMLMemCache struct {
	buf *bytes.Buffer
}

// NewDMLMemCache 初始化DML缓存
func NewDMLMemCache() *DMLMemCache {
	buf := bytes.NewBuffer(nil)
	return &DMLMemCache{
		buf: buf,
	}
}

func (cache *DMLMemCache) Write(data []byte) (int, error) {
	return cache.buf.Write(data)
}

func (cache *DMLMemCache) Read() []byte {
	defer cache.buf.Reset()
	return cache.buf.Bytes()
}

//Reset DML 缓存重置
func (cache *DMLMemCache) Reset() {
	cache.buf.Reset()
}
func (cache *DMLMemCache) shouldFlush() bool {
	return cache.buf.Len() > DMLMemCacheSize
}

// FlushToFile flush DML 缓存到文件
func (cache *DMLMemCache) FlushToFile(
	ctx context.Context, sink dumpsink.DumpSink, filename string,
) error {
	return sink.Write(ctx, filename, bytes.NewReader(cache.Read()))
}

// Len DML缓存长度
func (cache *DMLMemCache) Len() int {
	return cache.buf.Len()
}

//// BackFill 回填对象
//type BackFill struct {
//	//回填触发的边界时间
//	Ts      hlc.Timestamp
//	DDLEmit bool
//	//需要在ts 时间进行回填的表
//	tableList map[sqlbase.ID]struct{}
//}

// IsWhiteList 判断是否需要回填操作
//func (bf *BackFill) IsWhiteList(span roachpb.Span) bool {
//	_, tableID, err := keys.DecodeTablePrefix(span.Key)
//	if err != nil {
//		return false
//	}
//	_, ok := bf.tableList[sqlbase.ID(tableID)]
//	return !ok
//}

//func (bf *BackFill) getTableHighWater(span roachpb.Span) hlc.Timestamp {
//	_, tableID, err := keys.DecodeTablePrefix(span.Key)
//	if err != nil {
//		return hlc.Timestamp{}
//	}
//	ts, ok := bf.tableHighWater[sqlbase.ID(tableID)]
//	if !ok {
//		return hlc.Timestamp{}
//	}
//	return ts
//}

////设置各个表的highWater
//func (bf *BackFill) setTableHighWater(span roachpb.Span, ts hlc.Timestamp) {
//	_, tableID, err := keys.DecodeTablePrefix(span.Key)
//	if err != nil {
//		return
//	}
//	bf.tableHighWater[sqlbase.ID(tableID)] = ts
//}
