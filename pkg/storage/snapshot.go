package storage

import (
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

const (
	// SnapshotIdxID is the ID of the snapshot
	SnapshotIdxID = 2
)

// GetSnapShots implements storage.GCer.
func (NoopGCer) GetSnapShots(context.Context) ([]Snapshot, error) { return []Snapshot{}, nil }

func (replGC *replicaGCer) GetSnapShots(ctx context.Context) ([]Snapshot, error) {
	//log.Warningf(ctx, "gc repl ==>%d", replGC.repl.RangeID)
	//SnapDebug("repl startkey:", replGC.repl.Desc().StartKey)
	repl := replGC.repl

	db := repl.DB()
	startKey := repl.Desc().StartKey
	endKey := repl.Desc().EndKey
	return GetRangeSnapShots(ctx, db, startKey.AsRawKey(), endKey.AsRawKey())
}

// GetRangeSnapShots 查询某个Range区间key的快照记录(某个Range只会属于一个Table)
func GetRangeSnapShots(
	ctx context.Context, db *client.DB, startKey, endKey roachpb.Key,
) ([]Snapshot, error) {
	//首先解析该Range所属Table的ID
	_, startTable, startErr := keys.DecodeTablePrefix(startKey)
	//_, end_table, end_err := keys.DecodeTablePrefix(endKey)
	if startErr != nil {
		//如果都无法解析TableId，说明该Range不是用户表
		return nil, startErr
	}
	/*if start_table != end_table { //当1个table只有1个Range时，endKey=startKey+1
		return nil, errors.Wrap(nil, fmt.Sprintf("startKey[%v] and endKey[%v] do not belong to the same table ", startKey, endKey))
	}
	*/
	return GetTableSnapShots(ctx, db, sqlbase.ID(startTable))
}

// GetTableSnapShots get the table snap shots
func GetTableSnapShots(ctx context.Context, db *client.DB, tableID sqlbase.ID) ([]Snapshot, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(uint32(keys.SnapshotsTableID)))
	startKey = append(startKey, encoding.EncodeVarintAscending(nil, SnapshotIdxID)[0])          //index=2
	startKey = append(startKey, encoding.EncodeVarintAscending(nil, tree.TableSnapshotType)[0]) //type=1
	startKey = append(startKey, encoding.EncodeVarintAscending(nil, int64(tableID))[0])         //objectid=tableID
	endKey := startKey.PrefixEnd()
	return GetSnapShotsOf(ctx, db, startKey, endKey)
}

// GetAllSnapShots gets all snap shots
//func GetAllSnapShots(ctx context.Context, db *client.DB) ([]Snapshot, error) {
//	startKey := roachpb.Key(keys.MakeTablePrefix(uint32(keys.SnapshotsTableID)))
//	startKey = append(startKey, encoding.EncodeVarintAscending(nil, SnapshotIdxID)[0]) //index=2
//	endKey := startKey.PrefixEnd()
//	return GetSnapShotsOf(ctx, db, startKey, endKey)
//}

// GetSnapShotsOf gets the snap shot
func GetSnapShotsOf(
	ctx context.Context, db *client.DB, startKey, endKey roachpb.Key,
) ([]Snapshot, error) {
	kvs, err := db.Scan(ctx, startKey, endKey, 1000)
	if err != nil {
		return nil, err
	}
	snapshots := make(SnapshotSlice, len(kvs))
	for i, kv := range kvs {
		//suffix, detail, err := keys.DecodeTablePrefix(kv.Key)
		ss, err := parseSnapshotKey([]byte(kv.Key))
		if err != nil {
			return nil, err
		}

		snapshots[i] = ss
	}

	//按照asof time升序排列
	//sort.Sort(snapshots) //不用排序，key本身就是升序排列
	return snapshots, nil
}

// Snapshot 解析快照表的索引Key，得到的Snapshot定义。
type Snapshot struct {
	ID       uuid.UUID
	ParentID *uuid.UUID
	Type     int
	ObjectID uint32
	Time     int64 //nanoseconds
	Name     string
}

// SnapshotSlice is the slice of the snap shot
type SnapshotSlice []Snapshot

func (s SnapshotSlice) Len() int {
	return len(s)
}

// Less 比较两个元素大小 升序
func (s SnapshotSlice) Less(i, j int) bool {
	return s[i].Time < s[j].Time
}

// Swap 交换数据
func (s SnapshotSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func parseSnapshotKey(br []byte) (Snapshot, error) {
	ss := Snapshot{}
	br, _, err := encoding.DecodeVarintAscending(br)
	if err != nil {
		return ss, err
	}
	//t.Logf("tableid =%d", tableid)
	br, _, err = encoding.DecodeVarintAscending(br)
	if err != nil {
		return ss, err
	}
	//t.Logf("indexid =%d", idxid)

	br, stype, err := encoding.DecodeVarintAscending(br)
	if err != nil {
		return ss, err
	}
	ss.Type = int(stype)
	//t.Logf("snapshot type =%d", stype)

	br, objectid, err := encoding.DecodeVarintAscending(br)
	if err != nil {
		return ss, err
	}
	ss.ObjectID = uint32(objectid)
	//t.Logf("objectid =%d", objectid)

	br, time, err := encoding.DecodeTimeAscending(br)
	if err != nil {
		return ss, err
	}
	ss.Time = time.UnixNano()
	//t.Logf("time =%v", time)

	_, name, err := encoding.DecodeUnsafeStringAscending(br, nil)
	if err != nil {
		return ss, err
	}
	ss.Name = name
	//t.Logf("snapshot name =%s", name)
	return ss, nil
}

// SnapDebug 用于断点调试
//func SnapDebug(msg string, key []byte) {
//	if key != nil {
//		tablek := roachpb.Key(pkeys.MakeTablePrefix(62)) // table
//		if bytes.HasPrefix(key, tablek) {
//			s := fmt.Sprintf("%v", key)
//			fmt.Println(msg, s)
//		}
//	}
//}
