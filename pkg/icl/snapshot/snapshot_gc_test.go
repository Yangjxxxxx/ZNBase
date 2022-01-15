package snapshot

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kr/pretty"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// SQL层测试GC
func TestSnapshotGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, db, _ := setupData(t, false)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)
	requireTxn := true

	t1s := time.Duration(1e9) // 1秒
	// 时间点1
	exec(t, sqlDB, "CREATE TABLE item (id int primary key,name string);", requireTxn)
	exec(t, sqlDB, "insert into item values(1,'tv'),(2,'tel'),(3,'book')", requireTxn)
	t1 := s.Clock().Now().WallTime

	// 时间点2 - 100毫秒后
	time.Sleep(time.Duration(1e8)) // 100ms
	exec(t, sqlDB, "delete from item where id=1", requireTxn)
	exec(t, sqlDB, "update item set name='book-new' where id=3", requireTxn)
	t2 := s.Clock().Now().WallTime
	// 快照1 数据： (2,'tel') ( 3,'book-new')
	exec(t, sqlDB, fmt.Sprintf("CREATE SNAPSHOT item_snap1 ON TABLE item AS OF SYSTEM TIME %d", t2), requireTxn)

	// 时间点3 - 1.1秒后
	time.Sleep(t1s)
	exec(t, sqlDB, "insert into item values(1,'kindle'),(4,'iphone')", requireTxn)
	exec(t, sqlDB, "delete from item where id=3", requireTxn)
	exec(t, sqlDB, "delete from item where id=2", requireTxn)
	t3 := s.Clock().Now().WallTime

	// 时间点4 - 1.2秒后
	time.Sleep(time.Duration(1e8)) // 100ms
	exec(t, sqlDB, "update item set name='huawei' where id=4", requireTxn)
	exec(t, sqlDB, "insert into item values(3,'glass')", requireTxn)
	//t4 := s.Clock().Now().WallTime

	// 时间点5 - 2.2秒后
	time.Sleep(t1s) // 休眠1秒，等待GC超时
	//t5 := s.Clock().Now().WallTime

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

	// t3时间点数据 (1,'kindle'),(4,'iphone')
	items, err = queryItems(sqlDB, fmt.Sprintf("select * from item as of system time %d", t3))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count: %d, but got: %d", 2, len(items))
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
	gcTTLInSeconds := 1 // GC超时时间
	stores := s.GetStores().(*storage.Stores)
	for _, desc := range rs {
		repl, err := stores.GetReplicaForRangeID(desc.RangeID)
		if err != nil {
			t.Fatal(err)
		}
		gcer := SnapshotGCer{repl: repl}
		info, err := storage.RunGC(
			context.Background(),
			&desc,
			repl.Engine(),
			hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
			config.GCPolicy{TTLSeconds: int32(gcTTLInSeconds)},
			&gcer,
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
	//  (1,'kindle'),(3,'glass'),(4,'huawei')
	items, err = queryItems(sqlDB, "select * from item")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("expected row count: %d, but got: %d", 3, len(items))
	}
	if items[0].id != 1 || items[0].name != "kindle" {
		t.Fatalf("expected item[0] is {2,tel} , but got: %v", items[0])
	}
	if items[2].id != 4 || items[2].name != "huawei" {
		t.Fatalf("expected item[2] is {4,huawei} , but got: %v", items[2])
	}

	// 测试3 - 检查GC后快照数据
	//  (2,'tel') ( 3,'book-new')
	items, err = queryItems(sqlDB, "select * from item as of snapshot item_snap1")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count: %d, but got: %d", 2, len(items))
	}
	if items[0].id != 2 || items[0].name != "tel" {
		t.Fatalf("expected item[0] is {2,tel} , but got: %v", items[0])
	}
	if items[1].id != 3 || items[1].name != "book-new" {
		t.Fatalf("expected item[1] is {3,book-new} , but got: %v", items[1])
	}

	// todo jdx
	//t.Skip("skip by gzq")

	// 测试4 - 检查GC之后的t1时间点数据是否已经GC
	// (1,'tv') : 标记del , 在快照之前，GC
	// (3,'book') : 在t2 update，GC
	// t1 时间点可查数据: (2,'tel')
	items, err = queryItems(sqlDB, fmt.Sprintf("select * from item as of system time %d", t1))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected row count: %d, but got: %d", 1, len(items))
	}
	if items[0].id != 2 || items[0].name != "tel" {
		t.Fatalf("expected item[0] is {2,'tel'} , but got: %v", items[0])
	}

	// 测试5 - 检查GC之后t3时间点的数据是否已经GC
	// (1,'kindle'),( 3,'book-new') ,
	// id=2 : 在t3时间delete, 非第一快照前的del数据，未GC
	// id=3 : 在t3时间delete , t4时间insert, 该del记录被GC，因此在t3可以查询到快照1的数据
	// (4,'iphone'), : 在t4时间update， GC
	items, err = queryItems(sqlDB, fmt.Sprintf("select * from item as of system time %d", t3))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected row count: %d, but got: %d", 2, len(items))
	}
	if items[0].id != 1 || items[0].name != "kindle" {
		t.Fatalf("expected item[0] is {1,kindle} , but got: %v", items[0])
	}
	if items[1].id != 3 || items[1].name != "book-new" {
		t.Fatalf("expected item[1] is {3,book-new} , but got: %v", items[0])
	}
}

func getTableSnapshots(
	ctx context.Context, s serverutils.TestServerInterface, desc *sqlbase.TableDescriptor,
) ([]storage.Snapshot, error) {
	tblKey := roachpb.Key(keys.MakeTablePrefix(uint32(desc.GetID())))
	rs, _, err := client.RangeLookup(ctx, s.DistSender().(*kv.DistSender), tblKey,
		roachpb.CONSISTENT, 0, false)
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	stores := s.GetStores().(*storage.Stores)

	desc1 := rs[0]
	repl, err := stores.GetReplicaForRangeID(desc1.RangeID)
	if err != nil {
		return nil, err
	}
	gcer := SnapshotGCer{repl: repl}
	//the surrounding loop is unconditionally terminated (SA4004)
	return gcer.GetSnapShots(ctx)
}

// SnapshotGCer implements GCer.
type SnapshotGCer struct {
	repl  *storage.Replica
	count int32 // update atomically
}

// SetGCThreshold implements storage.GCer.
func (r *SnapshotGCer) SetGCThreshold(ctx context.Context, thresh storage.GCThreshold) error {
	req := r.template()
	req.Threshold = thresh.Key
	req.TxnSpanGCThreshold = thresh.Txn
	return r.send(ctx, req)
}

// GC implements storage.GCer.
func (r *SnapshotGCer) GC(ctx context.Context, keys []roachpb.GCRequest_GCKey) error {
	if len(keys) == 0 {
		return nil
	}
	req := r.template()
	req.Keys = keys
	return r.send(ctx, req)
}

func (r *SnapshotGCer) GetSnapShots(ctx context.Context) ([]storage.Snapshot, error) {
	repl := r.repl

	db := repl.DB()
	startKey := repl.Desc().StartKey
	endKey := repl.Desc().EndKey
	return storage.GetRangeSnapShots(ctx, db, startKey.AsRawKey(), endKey.AsRawKey())
}
func (r *SnapshotGCer) GetFlashback(ctx context.Context) (int32, error) {
	repl := r.repl
	db := repl.DB()
	startKey := repl.Desc().StartKey
	_, startTable, startErr := keys.DecodeTablePrefix(startKey.AsRawKey())
	if startErr != nil {
		//如果都无法解析TableId，说明该Range不是用户表
		return 0, startErr
	}
	return storage.GetTableFlashback(ctx, db, sqlbase.ID(startTable))
}
func (r *SnapshotGCer) template() roachpb.GCRequest {
	desc := r.repl.Desc()
	var template roachpb.GCRequest
	template.Key = desc.StartKey.AsRawKey()
	template.EndKey = desc.EndKey.AsRawKey()

	return template
}

func (r *SnapshotGCer) send(ctx context.Context, req roachpb.GCRequest) error {
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
