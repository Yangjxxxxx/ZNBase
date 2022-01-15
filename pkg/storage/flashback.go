package storage

import (
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
)

const (
	// FlashbackIdxID is the ID of the Flashback.
	FlashbackIdxID = 2
)

// GetFlashback implements storage.GCer.
func (NoopGCer) GetFlashback(context.Context) (int32, error) { return 0, nil }

func (replGC *replicaGCer) GetFlashback(ctx context.Context) (int32, error) {
	repl := replGC.repl

	db := repl.DB()
	startKey := repl.Desc().StartKey.AsRawKey()

	_, startTable, startErr := keys.DecodeTablePrefix(startKey)
	if startErr != nil {
		//如果都无法解析TableId，说明该Range不是用户表
		return 0, startErr
	}

	return GetTableFlashback(ctx, db, sqlbase.ID(startTable))
}

// GetTableFlashback get the table flashback.
func GetTableFlashback(ctx context.Context, db *client.DB, tableID sqlbase.ID) (int32, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(uint32(keys.FlashbackTableID)))
	startKey = append(startKey, encoding.EncodeVarintAscending(nil, FlashbackIdxID)[0])          //index=2
	startKey = append(startKey, encoding.EncodeVarintAscending(nil, tree.TableFlashbackType)[0]) //type=1
	startKey = append(startKey, encoding.EncodeVarintAscending(nil, int64(tableID))[0])          //objectid=tableID
	endKey := startKey.PrefixEnd()
	return GetFlashbacksOf(ctx, db, startKey, endKey)
}

// GetFlashbacksOf gets the flashback ttl days.
func GetFlashbacksOf(
	ctx context.Context, db *client.DB, startKey, endKey roachpb.Key,
) (int32, error) {
	kv, err := db.Scan(ctx, startKey, endKey, 1)
	if err != nil || len(kv) == 0 {
		return 0, err
	}

	ttlDays, err := parseFlashbackKey([]byte(kv[0].Key))
	TTLDaysSeconds := int32(ttlDays) * 24 * 3600

	if err != nil {
		return 0, err
	}
	return TTLDaysSeconds, nil
}

func parseFlashbackKey(br []byte) (int8, error) {
	br, _, err := encoding.DecodeVarintAscending(br)
	if err != nil {
		return 0, err
	}
	// t.Logf("tableid =%d", tableid)

	br, _, err = encoding.DecodeVarintAscending(br)
	if err != nil {
		return 0, err
	}
	// t.Logf("indexid =%d", idxid)

	br, _, err = encoding.DecodeVarintAscending(br)
	if err != nil {
		return 0, err
	}
	// t.Logf("Flashback type =%d", stype)

	br, _, err = encoding.DecodeVarintAscending(br)
	if err != nil {
		return 0, err
	}
	// t.Logf("objectid =%d", objectid)

	br, ttlDays, err := encoding.DecodeVarintAscending(br)
	if err != nil {
		return 0, err
	}
	// t.Logf("ttlDays =%d", ttldays)
	return int8(ttlDays), nil
}
