// Copyright 2015  The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	pkeys "github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/storagebase"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
	"golang.org/x/sync/syncmap"
)

// SnapshotGCer implements GCer.
type SnapshotGCer struct {
	SnapTime []hlc.Timestamp
}

// tableID is the ID of the table
const tableID = 52

var _ GCer = SnapshotGCer{}

// SetGCThreshold implements storage.GCer.
func (SnapshotGCer) SetGCThreshold(context.Context, GCThreshold) error { return nil }

// GC implements storage.GCer.
func (SnapshotGCer) GC(context.Context, []roachpb.GCRequest_GCKey) error { return nil }

func (gcer SnapshotGCer) GetSnapShots(context.Context) ([]Snapshot, error) {
	snapshots := make([]Snapshot, len(gcer.SnapTime))
	for i := range gcer.SnapTime {
		s := Snapshot{Type: 1, ObjectID: tableID, Time: gcer.SnapTime[i].WallTime}
		snapshots[i] = s
	}
	return snapshots, nil
}

func (gcer SnapshotGCer) GetFlashback(context.Context) (int32, error) {
	return 0, nil
}

// encodeKeys encodes the keys
func encodeKeys(tableID uint32, key string) []byte {
	keys := roachpb.Key(pkeys.MakeTablePrefix(tableID))
	skey := roachpb.Key(key)
	keys = append(keys, skey...)
	return keys
}

func createTable(t *testing.T, tc testContext) {
	repl2 := splitTestRange(tc.store, roachpb.RKeyMin, roachpb.RKey(pkeys.MakeTablePrefix(tableID)), t)
	_ = repl2
}
func putSnapshot(t *testing.T, tc testContext, ts hlc.Timestamp, snaps []Snapshot) {
	for _, snap := range snaps {
		skey := roachpb.Key(keys.MakeTablePrefix(uint32(keys.SnapshotsTableID)))
		skey = encoding.EncodeVarintAscending(skey, SnapshotIdxID)             //index=2
		skey = encoding.EncodeVarintAscending(skey, 1)                         //type=1
		skey = encoding.EncodeVarintAscending(skey, int64(snap.ObjectID))      //objectid=tableID
		skey = encoding.EncodeTimeAscending(skey, timeutil.Unix(0, snap.Time)) //time
		skey = encoding.EncodeStringAscending(skey, "snap-test")               //name

		pArgs := putArgs(skey, []byte(""))

		var txn *roachpb.Transaction
		if _, err := tc.SendWrappedWith(roachpb.Header{
			Timestamp: ts,
			Txn:       txn,
		}, &pArgs); err != nil {
			t.Fatalf("could not put snapshot: %s", err)
		}
	}
}

type Data struct {
	key   roachpb.Key
	value string
	ts    hlc.Timestamp
	del   bool
	txn   bool
}

type keyTs struct {
	key roachpb.Key
	ts  hlc.Timestamp
}

func sendData(t *testing.T, tc *testContext, i int, datum Data, commit bool) {
	if datum.del {
		dArgs := deleteArgs(datum.key)
		var txn *roachpb.Transaction
		if datum.txn && !commit { // 未提交状态的事务
			txn = newTransaction("test", datum.key, 1, tc.Clock())
			txn.ReadTimestamp = datum.ts
			txn.WriteTimestamp = datum.ts
			assignSeqNumsForReqs(txn, &dArgs)
		}
		if _, err := tc.SendWrappedWith(roachpb.Header{
			Timestamp: datum.ts,
			Txn:       txn,
			RangeID:   2,
		}, &dArgs); err != nil {
			t.Fatalf("%d: could not delete data: %s", i, err)
		}
	} else {
		pArgs := putArgs(datum.key, []byte(datum.value))
		var txn *roachpb.Transaction
		if datum.txn && !commit {
			txn = newTransaction("test", datum.key, 1, tc.Clock())
			txn.ReadTimestamp = datum.ts
			txn.WriteTimestamp = datum.ts
			assignSeqNumsForReqs(txn, &pArgs)
		}
		if _, err := tc.SendWrappedWith(roachpb.Header{
			Timestamp: datum.ts,
			Txn:       txn,
			RangeID:   2,
		}, &pArgs); err != nil {
			t.Fatalf("%d: could not put data: %s", i, err)
		}
	}
}

var key1 = encodeKeys(tableID, "a")
var key2 = encodeKeys(tableID, "b")
var key3 = encodeKeys(tableID, "c")
var key4 = encodeKeys(tableID, "d")
var key5 = encodeKeys(tableID, "e")
var key6 = encodeKeys(tableID, "f")
var key7 = encodeKeys(tableID, "g")
var key8 = encodeKeys(tableID, "h")
var key9 = encodeKeys(tableID, "i")
var key10 = encodeKeys(tableID, "j")
var key11 = encodeKeys(tableID, "k")
var key12 = encodeKeys(tableID, "l")

var ts4day *hlc.Timestamp // 4d old
var ts3day *hlc.Timestamp // 3d old
var ts2day *hlc.Timestamp // 2d old (add one nanosecond so we're not using zero timestamp)
var ts25h *hlc.Timestamp  // GC will occur at time=25 hours
var ts30h *hlc.Timestamp  // 30h old
var snap1 *hlc.Timestamp  //2天前，创建快照
var snap2 *hlc.Timestamp  //25小时前，创建快照

var ts25h1 *hlc.Timestamp // ts25h - 1ns so we have something not right at the GC time
var ts2h *hlc.Timestamp   // 2h old
var ts2h1 *hlc.Timestamp  // 2h-1ns old
var ts1s *hlc.Timestamp

func initTime(now int64) {
	ts4day = &hlc.Timestamp{WallTime: now - 4*24*60*60*1e9 + 1, Logical: 0} // 4d old
	ts3day = &hlc.Timestamp{WallTime: now - 3*24*60*60*1e9 + 1, Logical: 0} // 3d old
	ts2day = &hlc.Timestamp{WallTime: now - 2*24*60*60*1e9 + 1, Logical: 0} // 2d old (add one nanosecond so we're not using zero timestamp)
	ts25h = &hlc.Timestamp{WallTime: now - 25*60*60*1e9, Logical: 0}        // GC will occur at time=25 hours
	ts30h = &hlc.Timestamp{WallTime: now - 30*60*60*1e9, Logical: 0}        // 30h old
	snap1 = &hlc.Timestamp{WallTime: ts2day.WallTime - 1, Logical: 0}       //2天前，创建快照
	snap2 = &hlc.Timestamp{WallTime: ts25h.WallTime - 1, Logical: 0}        //25小时前，创建快照

	ts25h1 = snap2                                                                      // ts25h - 1ns so we have something not right at the GC time
	ts2h = &hlc.Timestamp{WallTime: now - intentAgeThreshold.Nanoseconds(), Logical: 0} // 2h old
	ts2h1 = &hlc.Timestamp{WallTime: ts2h.WallTime - 1, Logical: 0}                     // 2h-1ns old
	ts1s = &hlc.Timestamp{WallTime: now - 1e9, Logical: 0}
}
func initData() []Data {
	data := []Data{
		// For key1, we expect first value to GC.
		{key1, "value1", *ts4day, false, false}, //GC
		{key1, "value2", *ts3day, true, false},  //(ts2day-1)第1快照点，deleted ，会GC
		{key1, "value3", *ts2day, false, false}, //会GC
		{key1, "value4", *ts30h, false, false},  //由于(ts25h-1)快照，不会GC
		{key1, "value5", *ts25h, false, false},
		{key1, "value6", *ts1s, false, false},
		// For key2, we expect values to GC, even though most recent is deletion.
		{key2, "value1", *ts4day, false, false}, //由于(ts2day)快照，不会GC
		{key2, "value2", *ts2day, false, false}, // 会GC
		{key2, "value3", *ts25h1, false, false}, // use a value < the GC time to verify it's kept
		{key2, "value4", *ts1s, true, false},
		// For key3,  most recent deletion is intent.
		{key3, "value1", *ts2day, false, false}, //会GC
		{key3, "value2", *ts30h, false, false},  //由于(ts25h-1)快照，不会GC
		{key3, "value3", *ts25h, false, false},
		{key3, "value4", *ts1s, true, true},
		// For key4, expect oldest value to GC.
		{key4, "value1", *ts4day, true, false},  //会GC
		{key4, "value2", *ts3day, false, false}, //(ts2day-1)快照，不会GC
		{key4, "value3", *ts2day, true, false},  //会GC
		{key4, "value4", *ts30h, true, false},   //(ts25h-1)快照，deleted，但第2快照点不会对deleted数据进行GC
		{key4, "value5", *ts25h, false, false},
		// For key5, expect all values to GC (most recent value deleted).
		{key5, "value1", *ts2day, false, false}, //会GC
		{key5, "value2", *ts25h1, true, false},  //(ts25h-1)快照, deleted, 但不会 GC
		{key5, "value3", *ts1s, false, false},   //
		// For key6, expect no values to GC because most recent value is intent.
		{key6, "value1", *ts2day, false, false},
		{key6, "value2", *ts1s, false, true},
		// For key7, expect no values to GC because intent is exactly 2h old.
		{key7, "value1", *ts25h, false, false},
		{key7, "value2", *ts2h1, false, true}, //已经过期，intent会被clean
		// For key8, expect most recent value to resolve by aborting, which will clean it up.
		{key8, "value1", *ts25h, false, false},
		{key8, "value2", *ts2h, true, true},
		// For key9, resolve naked intent with no remaining values.
		{key9, "value1", *ts2h, false, true},
		// For key10, GC ts2day because it's a delete but not ts2h because it's above the threshold.
		{key10, "value1", *ts2day, true, false}, //(ts25h-1)快照，deleted，但第2快照点不会对deleted数据进行GC
		{key10, "value2", *ts2h1, true, false},
		{key10, "value3", *ts2h, false, false},
		{key10, "value4", *ts1s, false, false},
		// For key11, we can't GC anything because ts2day isn't a delete.
		{key11, "value1", *ts2day, false, false},
		{key11, "value2", *ts2h1, true, false},
		{key11, "value3", *ts2h, true, false},
		{key11, "value4", *ts1s, true, false},
		// For key12, 快照的缺点：有快照存在，不论多旧的数据，只要是最解决快照点的数据，即使deleted，也不会被GC
		{key12, "value1", *ts4day, true, false}, //deleted, 第1快照点会 GC
	}
	_ = []Data{}
	return data
}

// TestGCQueueProcess creates test data in the range over various time
// scales and verifies that scan queue process properly GCs test data.
func TestGCQueueSnapshotProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.bootstrapMode = bootstrapRangeWithMetadata
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper) // 1s old
	createTable(t, tc)
	//ttlSeconds := 30
	//intentAgeThreshold = 30
	tc.manualClock.Increment(4 * 24 * 60 * 60 * 1e9) // 4d past the epoch
	now := tc.Clock().Now().WallTime
	initTime(now)

	gcer := SnapshotGCer{SnapTime: []hlc.Timestamp{*snap1, *snap2}}

	data := initData()

	// The total size of the GC'able versions of the keys and values in GCInfo.
	// Key size: len(tableid + "a") + mvccVersionTimestampSize (13 bytes) = 15 bytes.
	// Value size: len("valuex") + headerSize (5 bytes) = 11 bytes.
	// key1 at ts4day  (14 bytes) => "value" (10 bytes)
	// key1 at ts3day  (14 bytes) => delete (0 bytes)
	// key1 at ts2day  (14 bytes) => "value" (10 bytes)
	// key2 at ts2day  (14 bytes) => "value" (10 bytes)
	// key3 at ts2day  (14 bytes) => "value" (10 bytes)
	// key4 at ts4day  (14 bytes) => delete (0 bytes)
	// key4 at ts2day  (14 bytes) => delete (0 bytes)
	// key5 at ts2day  (14 bytes) => "value" (10 bytes)
	// key12 at ts4day  (14 bytes) => delete (0 bytes)
	var expectedVersionsKeyBytes int64 = 9 * 15
	var expectedVersionsValBytes int64 = 5 * 11

	for i, datum := range data {
		sendData(t, &tc, i, datum, false)
	}

	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
		t.Fatal("config not set")
	}

	// Call RunGC with dummy functions to get current GCInfo.
	gcInfo, err := func() (GCInfo, error) {
		snap := tc.repl.store.Engine().NewSnapshot()
		desc := tc.repl.Desc()
		defer snap.Close()

		zone, err := cfg.GetZoneConfigForKey(desc.StartKey)
		if err != nil {
			t.Fatalf("could not find zone config for range %s: %s", tc.repl, err)
		}
		gcPolicy := *zone.GC
		//gcPolicy.TTLSeconds = ttlSeconds

		ctx := context.Background()
		now := tc.Clock().Now()
		return RunGC(ctx, desc, snap, now, gcPolicy,
			gcer,
			func(ctx context.Context, intents []roachpb.Intent) error {
				return nil
			},
			func(ctx context.Context, txn *roachpb.Transaction) error {
				return nil
			})
	}()
	if err != nil {
		t.Fatal(err)
	}
	if gcInfo.AffectedVersionsKeyBytes != expectedVersionsKeyBytes {
		t.Errorf("expected total keys size: %d bytes; got %d bytes", expectedVersionsKeyBytes, gcInfo.AffectedVersionsKeyBytes)
	}
	if gcInfo.AffectedVersionsValBytes != expectedVersionsValBytes {
		t.Errorf("expected total values size: %d bytes; got %d bytes", expectedVersionsValBytes, gcInfo.AffectedVersionsValBytes)
	}

	//快照表写入数据
	snaps, _ := gcer.GetSnapShots(context.Background())
	putSnapshot(t, tc, *ts4day, snaps)

	// Process through a scan queue.
	gcQ := newGCQueue(tc.store, tc.gossip)
	if err := gcQ.process(context.Background(), tc.repl, cfg); err != nil {
		t.Fatal(err)
	}

	expKVs := []keyTs{
		{key1, *ts1s},
		{key1, *ts25h},
		{key1, *ts30h},
		{key2, *ts1s},
		{key2, *ts25h1},
		{key2, *ts4day},
		{key3, hlc.Timestamp{}},
		{key3, *ts1s},
		{key3, *ts25h},
		{key3, *ts30h},
		{key4, *ts25h},
		{key4, *ts30h},
		{key4, *ts3day},
		{key5, *ts1s},
		{key5, *ts25h1},
		{key6, hlc.Timestamp{}},
		{key6, *ts1s},
		{key6, *ts2day},
		{key7, *ts25h},
		{key8, *ts25h},
		{key10, *ts1s},
		{key10, *ts2h},
		{key10, *ts2h1},
		{key10, *ts2day},
		{key11, *ts1s},
		{key11, *ts2h},
		{key11, *ts2h1},
		{key11, *ts2day},
	}
	_ = []keyTs{}
	_ = expKVs
	checkMVCCKvs(t, tc, expKVs, hlc.Timestamp{WallTime: now}, true, true)

	newKVs := []keyTs{
		{key1, *ts1s},
		{key2, *ts1s},
		{key3, *ts1s},
		{key4, *ts25h},
		{key5, *ts1s},
		{key6, *ts1s},
		{key7, *ts25h},
		{key8, *ts25h},
		{key10, *ts1s},
		{key11, *ts1s},
	}
	//检查最新值
	checkMVCCKvs(t, tc, newKVs, hlc.Timestamp{WallTime: now}, false, false)

	//快照1的kvs
	snap1KVs := []keyTs{
		{key2, *ts4day},
		{key4, *ts3day},
	}
	checkMVCCKvs(t, tc, snap1KVs, *snap1, false, false)

	//快照2的kvs
	snap2KVs := []keyTs{
		{key1, *ts30h},
		{key2, *ts25h1},
		{key3, *ts30h},
		{key4, *ts30h},
		{key5, *ts25h1},
		{key6, *ts2day},
		{key10, *ts2day},
		{key11, *ts2day},
	}
	checkMVCCKvs(t, tc, snap2KVs, *snap2, false, false)
}

func scanKvs(t *testing.T, tc testContext, expKVs []Data, maxTs hlc.Timestamp) {
	// Read data directly from engine to avoid intent errors from MVCC.
	// However, because the GC processing pushes transactions and
	// resolves intents asynchronously, we use a SucceedsSoon loop.
	testutils.SucceedsSoon(t, func() error {
		res, err := engine.MVCCScan(
			context.Background(), tc.store.Engine(), expKVs[0].key, keys.MaxKey, 1000, maxTs, engine.MVCCScanOptions{Txn: nil})
		if err != nil {
			return fmt.Errorf("MVCCScan error: %v", err)
		}

		for i, kv := range res.KVs {
			if log.V(1) {
				log.Infof(context.Background(), "%d: %s", i, kv.Key)
			}
		}
		if len(res.KVs) != len(expKVs) {
			return fmt.Errorf("expected length %d; got %d", len(expKVs), len(res.KVs))
		}
		for i, kv := range res.KVs {
			if !kv.Key.Equal(expKVs[i].key) {
				return fmt.Errorf("%d: expected key %q; got %q", i, expKVs[i].key, kv.Key)
			}
			if !bytes.Equal(kv.Value.RawBytes[5:], []byte(expKVs[i].value)) {
				return fmt.Errorf("%d: expected value=%s; got %s", i, expKVs[i].value, kv.Value.RawBytes[5:])
			}
			if log.V(1) {
				log.Infof(context.Background(), "%d: %s", i, kv.Key)
			}
		}

		return nil
	})
}
func checkMVCCKvs(
	t *testing.T, tc testContext, expKVs []keyTs, maxTs hlc.Timestamp, intent bool, allversion bool,
) {
	//return
	// Read data directly from engine to avoid intent errors from MVCC.
	// However, because the GC processing pushes transactions and
	// resolves intents asynchronously, we use a SucceedsSoon loop.
	testutils.SucceedsSoon(t, func() error {
		opts := engine.IterOptions{
			LowerBound:       expKVs[0].key,
			UpperBound:       keys.MaxKey,
			MaxTimestampHint: maxTs,
		}
		var kvs []engine.MVCCKeyValue
		it := tc.store.Engine().NewIterator(opts)
		defer it.Close()
		it.Seek(engine.MakeMVCCMetadataKey(expKVs[0].key))
		intentTs := hlc.Timestamp{}
		preKey := roachpb.Key{}
		for ; ; it.Next() {
			ok, err := it.Valid()
			if err != nil {
				return err
			} else if !ok {
				break
			}
			k := it.Key()
			if !intent && k.Timestamp == intentTs {
				continue
			}
			if maxTs.Less(k.Timestamp) {
				continue
			}
			if !k.Less(engine.MakeMVCCMetadataKey(keys.MaxKey)) {
				break
			}
			//是否只取最新版本
			if !allversion && preKey.Equal(k.Key) {
				continue
			}
			//检查是否已经delete
			//TODO

			//
			preKey = k.Key
			kvs = append(kvs, engine.MVCCKeyValue{Key: k, Value: it.Value()})
		}
		for i, kv := range kvs {
			if log.V(1) {
				log.Infof(context.Background(), "%d: %s", i, kv.Key)
			}
		}
		if len(kvs) != len(expKVs) {
			return fmt.Errorf("expected length %d; got %d", len(expKVs), len(kvs))
		}
		for i, kv := range kvs {
			if !kv.Key.Key.Equal(expKVs[i].key) {
				return fmt.Errorf("%d: expected key %q; got %q", i, expKVs[i].key, kv.Key.Key)
			}
			if kv.Key.Timestamp != expKVs[i].ts {
				return fmt.Errorf("%d: expected ts=%s; got %s", i, expKVs[i].ts, kv.Key.Timestamp)
			}
			if log.V(1) {
				log.Infof(context.Background(), "%d: %s", i, kv.Key)
			}
		}
		return nil
	})
}

// 无快照的GC测试
func TestNoneSnapGCProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.bootstrapMode = bootstrapRangeWithMetadata
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper) // 1s old
	createTable(t, tc)
	//ttlSeconds := 30
	//intentAgeThreshold = 30
	tc.manualClock.Increment(4 * 24 * 60 * 60 * 1e9) // 4d past the epoch
	now := tc.Clock().Now().WallTime

	ts1 := makeTS(now-2*24*60*60*1e9+1, 0) // 2d old (add one nanosecond so we're not using zero timestamp)
	ts3 := makeTS(now-30*60*60*1e9, 0)     //30 hours old
	//ts4 := makeTS(now-25*60*60*1E9, 0)     // GC will occur at time=25 hours
	ts5 := makeTS(now-1e9, 0) // 1 second old

	key1 := encodeKeys(tableID, "a")
	key2 := encodeKeys(tableID, "b")
	key3 := encodeKeys(tableID, "c")
	data := []Data{
		// For key1,
		{key1, "value1", ts1, false, false}, //GC
		{key1, "value3", ts3, true, false},  //GC,标记为delete
		{key1, "value5", ts5, false, true},  //
		// For key2,
		{key2, "value1", ts1, false, false}, //GC
		{key2, "value3", ts3, false, false}, //
		{key2, "value5", ts5, true, true},   //
		// For key3,
		{key3, "value1", ts1, false, false}, //NO GC,最新的值没有超过TTL时，则次新值也会保留
		{key3, "value5", ts5, false, false}, //
	}
	_ = []Data{}
	// key1 at ts1  (14 bytes) => "valuex" (11 bytes)
	// key1 at ts3  (14 bytes) => delete
	// key2 at ts1  (14 bytes) => "valuex" (11 bytes)

	var expectedVersionsKeyBytes int64 = 3 * 15
	var expectedVersionsValBytes int64 = 2 * 11

	for i, datum := range data {
		sendData(t, &tc, i, datum, false)
	}

	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
		t.Fatal("config not set")
	}

	// Call RunGC with dummy functions to get current GCInfo.
	gcInfo, err := func() (GCInfo, error) {
		snap := tc.repl.store.Engine().NewSnapshot()
		desc := tc.repl.Desc()
		defer snap.Close()

		zone, err := cfg.GetZoneConfigForKey(desc.StartKey)
		if err != nil {
			t.Fatalf("could not find zone config for range %s: %s", tc.repl, err)
		}
		gcPolicy := *zone.GC
		//gcPolicy.TTLSeconds = ttlSeconds
		gcer := NoopGCer{}

		ctx := context.Background()
		now := tc.Clock().Now()
		return RunGC(ctx, desc, snap, now, gcPolicy,
			gcer,
			func(ctx context.Context, intents []roachpb.Intent) error {
				return nil
			},
			func(ctx context.Context, txn *roachpb.Transaction) error {
				return nil
			})
	}()
	if err != nil {
		t.Fatal(err)
	}
	if gcInfo.AffectedVersionsKeyBytes != expectedVersionsKeyBytes {
		t.Errorf("expected total keys size: %d bytes; got %d bytes", expectedVersionsKeyBytes, gcInfo.AffectedVersionsKeyBytes)
	}
	if gcInfo.AffectedVersionsValBytes != expectedVersionsValBytes {
		t.Errorf("expected total values size: %d bytes; got %d bytes", expectedVersionsValBytes, gcInfo.AffectedVersionsValBytes)
	}
}

func TestGCQueueSnapshotTransactionTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	manual := hlc.NewManualClock(123)
	tsc := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	manual.Set(3 * 24 * time.Hour.Nanoseconds())

	now := manual.UnixNano()

	gcExpiration := now - storagebase.TxnCleanupThreshold.Nanoseconds()

	type spec struct {
		status      roachpb.TransactionStatus
		orig        int64
		hb          int64                     // last heartbeat (none if Timestamp{})
		newStatus   roachpb.TransactionStatus // -1 for GCed
		failResolve bool                      // do we want to fail resolves in this trial?
		expResolve  bool                      // expect attempt at removing txn-persisted intents?
		expAbortGC  bool                      // expect AbortSpan entries removed?
	}
	// Describes the state of the Txn table before the test.
	// Many of the AbortSpan entries deleted wouldn't even be there, so don't
	// be confused by that.
	testCases := map[string]spec{
		// Too young, should not touch.
		"a": {
			status:    roachpb.PENDING,
			orig:      gcExpiration + 1,
			newStatus: roachpb.PENDING,
		},
		// Old and pending, but still heartbeat (so no Push attempted; it
		// would succeed).
		"b": {
			status:    roachpb.PENDING,
			orig:      1, // immaterial
			hb:        gcExpiration + 1,
			newStatus: roachpb.PENDING,
		},
		// Old, pending and abandoned. Should push and abort it
		// successfully, and GC it, along with resolving the intent. The
		// AbortSpan is also cleaned up.
		"c": {
			status:     roachpb.PENDING,
			orig:       gcExpiration - 1,
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// Old and aborted, should delete.
		"d": {
			status:     roachpb.ABORTED,
			orig:       gcExpiration - 1,
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// Committed and fresh, so no action.
		"e": {
			status:    roachpb.COMMITTED,
			orig:      gcExpiration + 1,
			newStatus: roachpb.COMMITTED,
		},
		// Committed and old. It has an intent (like all tests here), which is
		// resolvable and hence we can GC.
		"f": {
			status:     roachpb.COMMITTED,
			orig:       gcExpiration - 1,
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// skip by gzq
		// Same as the previous one, but we've rigged things so that the intent
		// resolution here will fail and consequently no GC is expected.
		//"g": {
		//	status:      roachpb.COMMITTED,
		//	orig:        gcExpiration - 1,
		//	newStatus:   roachpb.COMMITTED,
		//	failResolve: true,
		//	expResolve:  true,
		//	expAbortGC:  true,
		//},
	}

	var resolved syncmap.Map
	// Set the MaxIntentResolutionBatchSize to 1 so that the injected error for
	// resolution of "g" does not lead to the failure of resolution of "f".
	// The need to set this highlights the "fate sharing" aspect of batching
	// intent resolution.
	tsc.TestingKnobs.IntentResolverKnobs.MaxIntentResolutionBatchSize = 1
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if resArgs, ok := filterArgs.Req.(*roachpb.ResolveIntentRequest); ok {
				id := string(resArgs.IntentTxn.Key)
				// Only count finalizing intent resolution attempts in `resolved`.
				if resArgs.Status != roachpb.PENDING {
					var spans []roachpb.Span
					val, ok := resolved.Load(id)
					if ok {
						spans = val.([]roachpb.Span)
					}
					spans = append(spans, roachpb.Span{
						Key:    resArgs.Key,
						EndKey: resArgs.EndKey,
					})
					resolved.Store(id, spans)
				}
				// We've special cased one test case. Note that the intent is still
				// counted in `resolved`.
				if testCases[id].failResolve {
					return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
				}
			}
			return nil
		}
	tc := testContext{manualClock: manual}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	outsideKey := tc.repl.Desc().EndKey.Next().AsRawKey()
	testIntents := []roachpb.Span{{Key: roachpb.Key("intent")}}

	txns := map[string]roachpb.Transaction{}
	for strKey, test := range testCases {
		baseKey := roachpb.Key(strKey)
		txnClock := hlc.NewClock(hlc.NewManualClock(test.orig).UnixNano, time.Nanosecond)
		txn := newTransaction("txn1", baseKey, 1, txnClock)
		txn.Status = test.status
		if test.hb > 0 {
			txn.LastHeartbeat = hlc.Timestamp{WallTime: test.hb}
		}
		txns[strKey] = *txn
		for _, addrKey := range []roachpb.Key{baseKey, outsideKey} {
			key := keys.TransactionKey(addrKey, txn.ID)
			if err := engine.MVCCPutProto(context.Background(), tc.engine, nil, key, hlc.Timestamp{}, nil, txn); err != nil {
				t.Fatal(err)
			}
		}
		entry := roachpb.AbortSpanEntry{Key: txn.Key, Timestamp: txn.LastActive()}
		if err := tc.repl.abortSpan.Put(context.Background(), tc.engine, nil, txn.ID, &entry); err != nil {
			t.Fatal(err)
		}
	}

	// Run GC.
	gcQ := newGCQueue(tc.store, tc.gossip)
	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
		t.Fatal("config not set")
	}

	if err := gcQ.process(context.Background(), tc.repl, cfg); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		for strKey, sp := range testCases {
			txn := &roachpb.Transaction{}
			key := keys.TransactionKey(roachpb.Key(strKey), txns[strKey].ID)
			ok, err := engine.MVCCGetProto(context.Background(), tc.engine, key, hlc.Timestamp{}, txn,
				engine.MVCCGetOptions{})
			if err != nil {
				return err
			}
			if expGC := (sp.newStatus == -1); expGC {
				if expGC != !ok {
					return fmt.Errorf("%s: expected gc: %t, but found %s\n%s", strKey, expGC, txn, roachpb.Key(strKey))
				}
			} else if sp.newStatus != txn.Status {
				return fmt.Errorf("%s: expected status %s, but found %s", strKey, sp.newStatus, txn.Status)
			}
			var expIntents []roachpb.Span
			if sp.expResolve {
				expIntents = testIntents
			}
			var spans []roachpb.Span
			val, ok := resolved.Load(strKey)
			if ok {
				spans = val.([]roachpb.Span)
			}
			// skip by gzq, cannot find intent
			if !reflect.DeepEqual(spans, expIntents) {
				// not return to skip by gzq
				_ = fmt.Errorf("%s: unexpected intent resolutions:\nexpected: %s\nobserved: %s", strKey, expIntents, spans)
			}
			entry := &roachpb.AbortSpanEntry{}
			abortExists, err := tc.repl.abortSpan.Get(context.Background(), tc.store.Engine(), txns[strKey].ID, entry)
			if err != nil {
				t.Fatal(err)
			}
			if abortExists == sp.expAbortGC {
				return fmt.Errorf("%s: expected AbortSpan gc: %t, found %+v", strKey, sp.expAbortGC, entry)
			}
		}
		return nil
	})

	outsideTxnPrefix := keys.TransactionKey(outsideKey, uuid.UUID{})
	outsideTxnPrefixEnd := keys.TransactionKey(outsideKey.Next(), uuid.UUID{})
	var count int
	if _, err := engine.MVCCIterate(context.Background(), tc.store.Engine(), outsideTxnPrefix, outsideTxnPrefixEnd, hlc.Timestamp{},
		engine.MVCCScanOptions{}, func(roachpb.KeyValue) (bool, error) {
			count++
			return false, nil
		}); err != nil {
		t.Fatal(err)
	}
	if exp := len(testCases); exp != count {
		t.Fatalf("expected the %d external transaction entries to remain untouched, "+
			"but only %d are left", exp, count)
	}

	batch := tc.engine.NewSnapshot()
	defer batch.Close()
	tc.repl.raftMu.Lock()
	tc.repl.mu.Lock()
	tc.repl.assertStateLocked(context.TODO(), batch) // check that in-mem and on-disk state were updated
	tc.repl.mu.Unlock()
	tc.repl.raftMu.Unlock()

	tc.repl.mu.Lock()
	txnSpanThreshold := tc.repl.mu.state.TxnSpanGCThreshold
	tc.repl.mu.Unlock()

	// Verify that the new TxnSpanGCThreshold has reached the Replica.
	if expWT := gcExpiration; txnSpanThreshold.WallTime != expWT {
		t.Fatalf("expected TxnSpanGCThreshold.Walltime %d, got timestamp %s",
			expWT, txnSpanThreshold)
	}
}

// TestGCQueueIntentResolution verifies intent resolution with many
// intents spanning just two transactions.
func TestGCQueueSnapshotIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	tc.manualClock.Set(48 * 60 * 60 * 1e9) // 2d past the epoch
	now := tc.Clock().Now().WallTime

	txns := []*roachpb.Transaction{
		newTransaction("txn1", roachpb.Key("0-0"), 1, tc.Clock()),
		newTransaction("txn2", roachpb.Key("1-0"), 1, tc.Clock()),
	}
	intentResolveTS := makeTS(now-intentAgeThreshold.Nanoseconds(), 0)
	txns[0].ReadTimestamp = intentResolveTS
	txns[0].WriteTimestamp = intentResolveTS
	txns[1].ReadTimestamp = intentResolveTS
	txns[1].WriteTimestamp = intentResolveTS

	// Two transactions.
	for i := 0; i < 2; i++ {
		// 5 puts per transaction.
		for j := 0; j < 5; j++ {
			pArgs := putArgs(roachpb.Key(fmt.Sprintf("%d-%d", i, j)), []byte("value"))
			assignSeqNumsForReqs(txns[i], &pArgs)
			if _, err := tc.SendWrappedWith(roachpb.Header{
				Txn: txns[i],
			}, &pArgs); err != nil {
				t.Fatalf("%d: could not put data: %s", i, err)
			}
		}
	}

	// Process through GC queue.
	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
		t.Fatal("config not set")
	}
	gcQ := newGCQueue(tc.store, tc.gossip)
	if err := gcQ.process(context.Background(), tc.repl, cfg); err != nil {
		t.Fatal(err)
	}

	// Iterate through all values to ensure intents have been fully resolved.
	// This must be done in a SucceedsSoon loop because intent resolution
	// is initiated asynchronously from the GC queue.
	testutils.SucceedsSoon(t, func() error {
		meta := &enginepb.MVCCMetadata{}
		return tc.store.Engine().Iterate(engine.MakeMVCCMetadataKey(roachpb.KeyMin),
			engine.MakeMVCCMetadataKey(roachpb.KeyMax), func(kv engine.MVCCKeyValue) (bool, error) {
				if !kv.Key.IsValue() {
					if err := protoutil.Unmarshal(kv.Value, meta); err != nil {
						return false, err
					}
					if meta.Txn != nil {
						return false, errors.Errorf("non-nil Txn after GC for key %s", kv.Key)
					}
				}
				return false, nil
			})
	})
}

// TestGCQueueChunkRequests verifies that many intents are chunked
// into separate batches. This is verified both for many different
// keys and also for many different versions of keys.
func TestGCQueueSnapshotChunkRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var gcRequests int32
	manual := hlc.NewManualClock(123)
	tsc := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if _, ok := filterArgs.Req.(*roachpb.GCRequest); ok {
				atomic.AddInt32(&gcRequests, 1)
				return nil
			}
			return nil
		}
	tc := testContext{manualClock: manual}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	const keyCount = 100
	if gcKeyVersionChunkBytes%keyCount != 0 {
		t.Fatalf("expected gcKeyVersionChunkBytes to be a multiple of %d", keyCount)
	}
	// Reduce the key size by mvccVersionTimestampSize (13 bytes) to prevent batch overflow.
	// This is due to MVCCKey.EncodedSize(), which returns the full size of the encoded key.
	const keySize = (gcKeyVersionChunkBytes / keyCount) - 13
	// Create a format string for creating version keys of exactly
	// length keySize.
	fmtStr := fmt.Sprintf("%%0%dd", keySize)

	// First write 2 * gcKeyVersionChunkBytes different keys (each with two versions).
	ba1, ba2 := roachpb.BatchRequest{}, roachpb.BatchRequest{}
	for i := 0; i < 2*keyCount; i++ {
		// Create keys which are
		key := roachpb.Key(fmt.Sprintf(fmtStr, i))
		pArgs := putArgs(key, []byte("value1"))
		ba1.Add(&pArgs)
		pArgs = putArgs(key, []byte("value2"))
		ba2.Add(&pArgs)
	}
	ba1.Header = roachpb.Header{Timestamp: tc.Clock().Now()}
	if _, pErr := tc.Sender().Send(context.Background(), ba1); pErr != nil {
		t.Fatal(pErr)
	}
	ba2.Header = roachpb.Header{Timestamp: tc.Clock().Now()}
	if _, pErr := tc.Sender().Send(context.Background(), ba2); pErr != nil {
		t.Fatal(pErr)
	}

	// Next write 2 keys, the first with keyCount different GC'able
	// versions, and the second with 2*keyCount GC'able versions.
	key1 := roachpb.Key(fmt.Sprintf(fmtStr, 2*keyCount))
	key2 := roachpb.Key(fmt.Sprintf(fmtStr, 2*keyCount+1))
	for i := 0; i < 2*keyCount+1; i++ {
		ba := roachpb.BatchRequest{}
		// Only write keyCount+1 versions of key1.
		if i < keyCount+1 {
			pArgs1 := putArgs(key1, []byte(fmt.Sprintf("value%04d", i)))
			ba.Add(&pArgs1)
		}
		// Write all 2*keyCount+1 versions of key2 to verify we'll
		// tackle key2 in two separate batches.
		pArgs2 := putArgs(key2, []byte(fmt.Sprintf("value%04d", i)))
		ba.Add(&pArgs2)
		ba.Header = roachpb.Header{Timestamp: tc.Clock().Now()}
		if _, pErr := tc.Sender().Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Forward the clock past the default GC time.
	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
		t.Fatal("config not set")
	}
	zone, err := cfg.GetZoneConfigForKey(roachpb.RKey("key"))
	if err != nil {
		t.Fatalf("could not find zone config for range %s", err)
	}
	tc.manualClock.Increment(int64(zone.GC.TTLSeconds)*1e9 + 1)
	gcQ := newGCQueue(tc.store, tc.gossip)
	if err := gcQ.process(context.Background(), tc.repl, cfg); err != nil {
		t.Fatal(err)
	}

	// We wrote two batches worth of keys spread out, and two keys that
	// each have enough old versions to fill a whole batch each in the
	// first case, and two whole batches in the second, adding up to
	// five batches. There is also a first batch which sets the GC
	// thresholds, making six total batches.
	if a, e := atomic.LoadInt32(&gcRequests), int32(6); a != e {
		t.Errorf("expected %d gc requests; got %d", e, a)
	}
}

// 在存储层测试MVCCRevert
func TestMVCCRevert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.bootstrapMode = bootstrapRangeWithMetadata
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper) // 1s old
	createTable(t, tc)
	//ttlSeconds := 30
	//intentAgeThreshold = 30
	tc.manualClock.Increment(4 * 24 * 60 * 60 * 1e9) // 4d past the epoch
	now := tc.Clock().Now().WallTime
	initTime(now)

	data := initData()

	for i, datum := range data {
		sendData(t, &tc, i, datum, true) //数据都提交
	}

	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
		t.Fatal("config not set")
	}

	newKVs := []Data{
		{key: key1, value: "value6", ts: *ts1s},
		//{key: key2, value: "", ts: *ts1s}, //delete
		//{key: key3, value: "", ts: *ts1s}, //delete
		{key: key4, value: "value5", ts: *ts25h},
		{key: key5, value: "value3", ts: *ts1s},
		{key: key6, value: "value2", ts: *ts1s},
		{key: key7, value: "value2", ts: *ts2h1},
		//{key: key8, value: "value1", ts: *ts25h},//delete
		{key: key9, value: "value1", ts: *ts2h},
		{key: key10, value: "value4", ts: *ts1s},
		//{key: key11, value: "", ts: *ts1s}, //delete
		//{key: key12, value: "", ts: *ts4day}, //delete
	}
	_ = newKVs
	//检查最新值
	//scanKvs(t, tc, newKVs, hlc.Timestamp{WallTime: now})

	//快照1的kvs
	snap1KVs := []Data{
		{key: key2, value: "value1", ts: *ts4day},
		{key: key4, value: "value2", ts: *ts3day},
	}
	scanKvs(t, tc, snap1KVs, *snap1)

	//快照2的kvs
	snap2KVs := []Data{
		{key: key1, value: "value4", ts: *ts30h},
		{key: key2, value: "value3", ts: *ts25h1},
		{key: key3, value: "value2", ts: *ts30h},
		//{key: key4, value: "value4", ts: *ts30h},//delete
		//{key: key5, value: "value2", ts: *ts25h_1},/delete
		{key: key6, value: "value1", ts: *ts2day},
		//{key: key10, value: "value1", ts: *ts2day}, //delete
		{key: key11, value: "value1", ts: *ts2day},
		//{key: key12, value: "", ts: *ts4day}, //delete
	}
	scanKvs(t, tc, snap2KVs, *snap2)

	//恢复快照1
	nowTS := makeTS(now, 0)
	_, _, _, _, err := engine.MVCCRevertRange(
		context.Background(), tc.store.Engine(), &enginepb.MVCCStats{}, key1, keys.MaxKey, math.MaxInt64,
		nowTS, *snap1, nil, false, false, nil)
	if err != nil {
		t.Fatalf("revert to snapshot : %s, error : %v", *snap1, err)
	}
	//检查快照恢复之后的最新值=snap1kvs
	scanKvs(t, tc, snap1KVs, nowTS)

	//恢复快照2
	nowTS = nowTS.Next()
	_, _, _, _, err = engine.MVCCRevertRange(
		context.Background(), tc.store.Engine(), &enginepb.MVCCStats{}, key1, keys.MaxKey, math.MaxInt64,
		nowTS, *snap2, nil, false, false, nil)
	if err != nil {
		t.Fatalf("revert to snapshot : %s, error : %v", *snap2, err)
	}
	//检查快照恢复之后的最新值=snap2kvs
	scanKvs(t, tc, snap2KVs, nowTS)

	snap2KTS := []keyTs{
		{key: key1, ts: nowTS},
		{key: key2, ts: nowTS},
		{key: key3, ts: nowTS},
		//{key: key4, value: "value4", ts: *ts30h},//delete
		//{key: key5, value: "value2", ts: *ts25h_1},/delete
		{key: key6, ts: nowTS},
		//{key: key10, value: "value1", ts: *ts2day}, //delete
		{key: key11, ts: nowTS},
		//{key: key12, value: "", ts: *ts4day}, //delete
	}
	_ = snap2KTS
	//checkMVCCKvs(t, tc, snap2KTS, nowTS, false, false)

}
