package loadutils

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// AdminBatchLoadCallback resolve循环引用问题
type AdminBatchLoadCallback func(ctx context.Context, req *serverpb.RawBatchLoadRequest,
	st *cluster.Settings, db *client.DB, rangeCache *kv.RangeDescriptorCache,
	nodeID roachpb.NodeID, tableDesc *sqlbase.TableDescriptor, histogramWindowInterval time.Duration) (*serverpb.RawBatchLoadResponse, error)

// RawBatchLoadCallbackFunc 是为了解决循环引用问题
var RawBatchLoadCallbackFunc AdminBatchLoadCallback
