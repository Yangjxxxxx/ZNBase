package flashback

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// SQL层测试GC
func TestFlashbackGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	//t.Skip("CI有几率不过")
	s, sqlDB, db, _ := flashData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := true

	t1s := time.Duration(2e9) // 2秒
	// 时间点1
	exec(t, sqlDB, "CREATE TABLE item (id int primary key,name string);", requireTxn)
	exec(t, sqlDB, "insert into item values(1,'tv'),(2,'tel'),(3,'book')", requireTxn)
	t1 := s.Clock().Now().WallTime

	// 时间点2 - 100毫秒后
	time.Sleep(time.Duration(1e8)) // 100ms
	exec(t, sqlDB, "delete from item where id=1", requireTxn)
	exec(t, sqlDB, "update item set name='book-new' where id=3", requireTxn)
	// t2 := s.Clock().Now().WallTime
	time.Sleep(time.Duration(1e8)) // 100ms
	// 时间点3 - 2.1秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into item values(1,'kindle'),(4,'iphone')", requireTxn)
	exec(t, sqlDB, "delete from item where id=3", requireTxn)
	exec(t, sqlDB, "delete from item where id=2", requireTxn)
	// 为了测试GC,此处直接赋值的3秒
	exec(t, sqlDB, "ALTER TABLE item ENABLE FLASHBACK WITH ttldays 3;", requireTxn)
	t2 := s.Clock().Now().WallTime
	time.Sleep(time.Duration(1e8)) // 100ms

	// 时间点4 - 2.2s
	time.Sleep(time.Duration(1e8)) // 100ms
	exec(t, sqlDB, "update item set name='huawei' where id=4", requireTxn)
	exec(t, sqlDB, "update item set name='xiaomi' where id=1", requireTxn)
	exec(t, sqlDB, "insert into item values(3,'glass')", requireTxn)
	time.Sleep(t1s)
	// 测试1 ： GC之前，可以查看未GC的数据
	// t1 时间点数据: (1,'tv'),(2,'tel'),(3,'book')
	items, err := queryItems(sqlDB, fmt.Sprintf("select * from item as of system time %d", t1))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("expected row count: %d, but got: %d", 3, len(items))
	}
	if items[0].id != 1 || items[0].name != "tv" {
		t.Fatalf("expected item[0] is (1,'tv') , but got: %v", items[0])
	}
	if items[2].id != 3 || items[2].name != "book" {
		t.Fatalf("expected item[2] is (3,'book') , but got: %v", items[2])
	}

	// t2时间点数据
	items, err = queryItems(sqlDB, fmt.Sprintf("select * from item as of system time %d", t2))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count: %d, but got: %d", 3, len(items))
	}
	if items[0].id != 1 || items[0].name != "kindle" {
		t.Fatalf("expected item[0] is (1,'kindle') , but got: %v", items[0])
	}
	if items[1].id != 4 || items[1].name != "iphone" {
		t.Fatalf("expected item[1] is (4,'iphone') , but got: %v", items[1])
	}
	desc, _ := LookupTableDescriptor(ctx, db, "defaultdb", "item")
	rs, _, err := client.RangeLookup(ctx, s.DistSender().(*kv.DistSender), roachpb.Key(keys.MakeTablePrefix(uint32(desc.GetID()))),
		roachpb.CONSISTENT, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	gcTTLInSeconds := 2 // GC超时时间
	stores := s.GetStores().(*storage.Stores)
	for _, desc := range rs {
		repl, err := stores.GetReplicaForRangeID(desc.RangeID)
		if err != nil {
			t.Fatal(err)
		}
		r := FlashbackGCer{repl: repl}
		info, err := storage.RunGC(
			context.Background(),
			&desc,
			repl.Engine(),
			hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
			config.GCPolicy{TTLSeconds: int32(gcTTLInSeconds)},
			&r,
			func(_ context.Context, _ []roachpb.Intent) error { return nil },
			func(_ context.Context, _ *roachpb.Transaction) error { return nil },
		)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("RangeID: %d [%s, %s):\n", desc.RangeID, desc.StartKey, desc.EndKey)
		_, _ = pretty.Println(info)

		// 重新设置一个较长的GC超时时间， 否则无法使用as of system time查询GC时间前的数据
		// 无法设置更小的时间，cmd_gc中会判断新的值要比旧的Threshold大
	}

	// 测试2 - 检查GC后最新数据
	items, err = queryItems(sqlDB, "select * from item")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("expected row count: %d, but got: %d", 3, len(items))
	}
	if items[0].id != 1 || items[0].name != "xiaomi" {
		t.Fatalf("expected item[0] is {1,xiaomi} , but got: %v", items[0])
	}
	if items[2].id != 4 || items[2].name != "huawei" {
		t.Fatalf("expected item[2] is {4,huawei} , but got: %v", items[2])
	}

	// todo jdx
	// t1时间点数据被GC
	items, err = queryItems(sqlDB, fmt.Sprintf("select * from item as of system time %d", t1))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count: %d, but got: %d", 1, len(items))
	}
	if items[0].id != 2 || items[0].name != "tel" {
		t.Fatalf("expected item[0] is {2,tel} , but got: %v", items[0])
	}
	// t2时间点数据GC时间内，不被GC
	items, err = queryItems(sqlDB, fmt.Sprintf("select * from item as of system time %d", t2))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count: %d, but got: %d", 3, len(items))
	}
	if items[0].id != 1 || items[0].name != "kindle" {
		t.Fatalf("expected item[0] is {1,kindle} , but got: %v", items[0])
	}
	if items[1].id != 4 || items[1].name != "iphone" {
		t.Fatalf("expected item[1] is {4,iphone} , but got: %v", items[1])
	}
}

func LookupTableDescriptor(
	ctx context.Context, txn *client.DB, dbName string, tblName string,
) (*sqlbase.TableDescriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	rows, err := txn.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, err
	}
	// 先查询db
	var dbDesc *sql.DatabaseDescriptor
	for _, row := range rows {
		desc := sqlbase.Descriptor{}
		if err := row.ValueProto(&desc); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
		}
		if db := desc.GetDatabase(); db != nil && db.Name == dbName {
			dbDesc = db
			break
		}
	}
	if dbDesc == nil {
		return nil, nil
	}

	// table的schema默认值：public
	parentID := dbDesc.GetSchemaID("public")
	// 本数据库之外的其他数据表
	for _, row := range rows {
		desc := sqlbase.Descriptor{}
		if err := row.ValueProto(&desc); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
		}
		if tbl := desc.Table(row.Value.Timestamp); tbl != nil {
			if tbl.Dropped() {
				continue
			}
			if tbl.ParentID == parentID && tbl.Name == tblName { // table的ParentID是Schema的ID，Schema的ParentID才是DB
				return tbl, nil
			}
		}
	}
	return nil, nil
}

// SnapshotGCer implements GCer.
type FlashbackGCer struct {
	repl  *storage.Replica
	count int32 // update atomically
}

// SetGCThreshold implements storage.GCer.
func (r *FlashbackGCer) SetGCThreshold(ctx context.Context, thresh storage.GCThreshold) error {
	req := r.template()
	req.Threshold = thresh.Key
	req.TxnSpanGCThreshold = thresh.Txn
	return r.send(ctx, req)
}

// GC implements storage.GCer.
func (r *FlashbackGCer) GC(ctx context.Context, keys []roachpb.GCRequest_GCKey) error {
	if len(keys) == 0 {
		return nil
	}
	req := r.template()
	req.Keys = keys
	return r.send(ctx, req)
}

func (r *FlashbackGCer) GetSnapShots(ctx context.Context) ([]storage.Snapshot, error) {
	repl := r.repl

	db := repl.DB()
	startKey := repl.Desc().StartKey
	endKey := repl.Desc().EndKey
	return storage.GetRangeSnapShots(ctx, db, startKey.AsRawKey(), endKey.AsRawKey())
}
func (r *FlashbackGCer) GetFlashback(ctx context.Context) (int32, error) {
	// 为了测试GC 直接传秒
	return 3, nil
}
func (r *FlashbackGCer) template() roachpb.GCRequest {
	desc := r.repl.Desc()
	var template roachpb.GCRequest
	template.Key = desc.StartKey.AsRawKey()
	template.EndKey = desc.EndKey.AsRawKey()

	return template
}

func (r *FlashbackGCer) send(ctx context.Context, req roachpb.GCRequest) error {
	n := atomic.AddInt32(&r.count, 1)
	log.Eventf(ctx, "sending batch %d (%d keys)", n, len(req.Keys))

	var ba roachpb.BatchRequest

	// Technically not needed since we're talking directly to the Replica.
	ba.RangeID = r.repl.Desc().RangeID
	ba.Timestamp = r.repl.Clock().Now()
	ba.Add(&req)

	if _, pErr := r.repl.Send(ctx, ba); pErr != nil {
		log.VErrEvent(ctx, 2, pErr.String())
		return pErr.GoError()
	}
	return nil
}
