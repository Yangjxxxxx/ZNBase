package engine

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
)

//MVCCVecScan 向量读取
func MVCCVecScan(
	ctx context.Context,
	engine Reader,
	key, endKey roachpb.Key,
	pd *roachpb.PushDownExpr,
	maxRows int64,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (roachpb.VecResults, *roachpb.Span, []roachpb.Intent, error) {
	_, tableID, error := keys.DecodeTablePrefix(key)
	if error != nil {
		return roachpb.VecResults{}, nil, nil, error
	}

	if tableID <= keys.PostgresSchemaID {
		return roachpb.VecResults{}, nil, nil, fmt.Errorf("vecscan support tableid must be greater than %d, but it is %d now", keys.PostgresSchemaID, tableID)

	}
	if rocksdb := engine.(*rocksDBReadOnly); rocksdb != nil {
		log.Info(ctx, fmt.Sprintf("MVCC VecScan table[%d] [%s]->[%s]\n", tableID, key.String(), endKey.String()))
		return rocksdb.VecScan(key, endKey, tableID, pd, maxRows, timestamp, opts)

	}
	return roachpb.VecResults{}, nil, nil, fmt.Errorf(("only rocksdb support vecscan() method"))

}
