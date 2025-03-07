// Copyright 2017 The Cockroach Authors.
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

package batcheval_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/kr/pretty"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/batcheval"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

func singleKVSSTable(key engine.MVCCKey, value []byte) ([]byte, error) {
	sstFile := &engine.MemFile{}
	sst := engine.MakeBackupSSTWriter(sstFile)
	defer sst.Close()
	if err := sst.Put(key, value); err != nil {
		return nil, err
	}
	if err := sst.Finish(); err != nil {
		return nil, err
	}
	return sstFile.Data(), nil
}

func TestDBAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("store=in-memory", func(t *testing.T) {
		s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
		ctx := context.Background()
		defer s.Stopper().Stop(ctx)
		runTestDBAddSSTable(ctx, t, db, nil)
	})
	t.Run("store=on-disk", func(t *testing.T) {
		dir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()

		storeSpec := base.DefaultTestStoreSpec
		storeSpec.InMemory = false
		storeSpec.Path = dir
		s, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Insecure:   true,
			StoreSpecs: []base.StoreSpec{storeSpec},
		})
		ctx := context.Background()
		defer s.Stopper().Stop(ctx)
		store, err := s.GetStores().(*storage.Stores).GetStore(s.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		runTestDBAddSSTable(ctx, t, db, store)
	})
}

// if store != nil, assume it is on-disk and check ingestion semantics.
func runTestDBAddSSTable(ctx context.Context, t *testing.T, db *client.DB, store *storage.Store) {
	{
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 2}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("1").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		// Key is before the range in the request span.
		if err := db.AddSSTable(
			ctx, "d", "e", data, false, nil, false); !testutils.IsError(err, "not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}
		// Key is after the range in the request span.
		if err := db.AddSSTable(
			ctx, "a", "b", data, false, nil, false); !testutils.IsError(err, "not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}

		// Do an initial ingest.
		ingestCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "test-recording")
		defer cancel()
		if err := db.AddSSTable(ingestCtx, "b", "c", data, false, nil, false); err != nil {
			t.Fatalf("%+v", err)
		}
		formatted := tracing.FormatRecordedSpans(collect())
		if err := testutils.MatchInOrder(formatted,
			"evaluating AddSSTable",
			"sideloadable proposal detected",
			"ingested SSTable at index",
		); err != nil {
			t.Fatal(err)
		}

		if store != nil {
			// Look for the ingested path and verify it still exists.
			re := regexp.MustCompile(`ingested SSTable at index \d+, term \d+: (\S+)`)
			match := re.FindStringSubmatch(formatted)
			if len(match) != 2 {
				t.Fatalf("failed to extract ingested path from message %q,\n got: %v", formatted, match)
			}
			// The on-disk paths have `.ingested` appended unlike in-memory.
			suffix := ".ingested"
			if _, err := os.Stat(strings.TrimSuffix(match[1], suffix)); err != nil {
				t.Fatalf("%q file missing after ingest: %v", match[1], err)
			}
		}
		if r, err := db.Get(ctx, "bb"); err != nil {
			t.Fatalf("%+v", err)
		} else if expected := []byte("1"); !bytes.Equal(expected, r.ValueBytes()) {
			t.Errorf("expected %q, got %q", expected, r.ValueBytes())
		}
	}

	// Check that ingesting a key with an earlier mvcc timestamp doesn't affect
	// the value returned by Get.
	{
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("2").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if err := db.AddSSTable(ctx, "b", "c", data, false, nil, false); err != nil {
			t.Fatalf("%+v", err)
		}
		if r, err := db.Get(ctx, "bb"); err != nil {
			t.Fatalf("%+v", err)
		} else if expected := []byte("1"); !bytes.Equal(expected, r.ValueBytes()) {
			t.Errorf("expected %q, got %q", expected, r.ValueBytes())
		}
		if store != nil {
			metrics := store.Metrics()
			if expected, got := int64(2), metrics.AddSSTableApplications.Count(); expected != got {
				t.Fatalf("expected %d sst ingestions, got %d", expected, got)
			}
		}
	}

	// Key range in request span is not empty. First time through a different
	// key is present. Second time through checks the idempotency.
	{
		key := engine.MVCCKey{Key: []byte("bc"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("3").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		var metrics *storage.StoreMetrics
		var before int64
		if store != nil {
			metrics = store.Metrics()
			before = metrics.AddSSTableApplicationCopies.Count()
		}
		for i := 0; i < 2; i++ {
			ingestCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "test-recording")
			defer cancel()

			if err := db.AddSSTable(ingestCtx, "b", "c", data, false, nil, false); err != nil {
				t.Fatalf("%+v", err)
			}
			if err := testutils.MatchInOrder(tracing.FormatRecordedSpans(collect()),
				"evaluating AddSSTable",
				"sideloadable proposal detected",
				"ingested SSTable at index",
			); err != nil {
				t.Fatal(err)
			}

			if r, err := db.Get(ctx, "bb"); err != nil {
				t.Fatalf("%+v", err)
			} else if expected := []byte("1"); !bytes.Equal(expected, r.ValueBytes()) {
				t.Errorf("expected %q, got %q", expected, r.ValueBytes())
			}
			if r, err := db.Get(ctx, "bc"); err != nil {
				t.Fatalf("%+v", err)
			} else if expected := []byte("3"); !bytes.Equal(expected, r.ValueBytes()) {
				t.Errorf("expected %q, got %q", expected, r.ValueBytes())
			}
		}
		if store != nil {
			if expected, got := int64(4), metrics.AddSSTableApplications.Count(); expected != got {
				t.Fatalf("expected %d sst ingestions, got %d", expected, got)
			}
			// The second time though we had to make a copy of the SST since rocks saw
			// existing data (from the first time), and rejected the no-modification
			// attempt.
			if after := metrics.AddSSTableApplicationCopies.Count(); before != after {
				t.Fatalf("expected sst copies not to increase, %d before %d after", before, after)
			}
		}
	}

	// Invalid key/value entry checksum.
	{
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		value := roachpb.MakeValueFromString("1")
		value.InitChecksum([]byte("foo"))
		data, err := singleKVSSTable(key, value.RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if err := db.AddSSTable(ctx, "b", "c", data, false, nil, false); !testutils.IsError(err, "invalid checksum") {
			t.Fatalf("expected 'invalid checksum' error got: %+v", err)
		}
	}
}

type strKv struct {
	k  string
	ts int64
	v  string
}

func mvccKVsFromStrs(in []strKv) []engine.MVCCKeyValue {
	kvs := make([]engine.MVCCKeyValue, len(in))
	for i := range kvs {
		kvs[i].Key.Key = []byte(in[i].k)
		kvs[i].Key.Timestamp.WallTime = in[i].ts
		if in[i].v != "" {
			kvs[i].Value = roachpb.MakeValueFromBytes([]byte(in[i].v)).RawBytes
		} else {
			kvs[i].Value = nil
		}
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key.Less(kvs[j].Key) })
	return kvs
}

func TestAddSSTableMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	for _, kv := range mvccKVsFromStrs([]strKv{
		{"A", 1, "A"},
		{"a", 1, "a"},
		{"a", 6, ""},
		{"b", 5, "bb"},
		{"c", 6, "ccccccccccccccccccccccccccccccccccccccccccccc"}, // key 4b, 50b, live 64b
		{"d", 1, "d"},
		{"d", 2, ""},
		{"e", 1, "e"},
		{"z", 2, "zzzzzz"},
	}) {
		if err := e.Put(kv.Key, kv.Value); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	sstKVs := mvccKVsFromStrs([]strKv{
		{"a", 2, "aa"},     // mvcc-shadowed within SST.
		{"a", 4, "aaaaaa"}, // mvcc-shadowed by existing delete.
		{"c", 6, "ccc"},    // same TS as existing, LSM-shadows existing.
		{"d", 4, "dddd"},   // mvcc-shadow existing deleted d.
		{"e", 4, "eeee"},   // mvcc-shadow existing 1b.
		{"j", 2, "jj"},     // no colission – via MVCC or LSM – with existing.
	})
	var delta enginepb.MVCCStats
	// the sst will think it added 4 keys here, but a, c, and e shadow or are shadowed.
	delta.LiveCount = -3
	delta.LiveBytes = -109
	// the sst will think it added 5 keys, but only j is new so 4 are over-counted.
	delta.KeyCount = -4
	delta.KeyBytes = -20
	// the sst will think it added 6 values, but since one was a perfect (key+ts)
	// collision, it *replaced* the existing value and is over-counted.
	delta.ValCount = -1
	delta.ValBytes = -50

	// Add in a random metadata key.
	ts := hlc.Timestamp{WallTime: 7}
	txn := roachpb.MakeTransaction(
		"test",
		nil, // baseKey
		roachpb.NormalUserPriority,
		ts,
		base.DefaultMaxClockOffset.Nanoseconds(),
	)
	if err := engine.MVCCPut(
		ctx, e, nil, []byte("i"), ts,
		roachpb.MakeValueFromBytes([]byte("it")),
		&txn,
	); err != nil {
		if _, isWriteIntentErr := err.(*roachpb.WriteIntentError); !isWriteIntentErr {
			t.Fatalf("%+v", err)
		}
	}

	// After evalAddSSTable, cArgs.Stats contains a diff to the existing
	// stats. Make sure recomputing from scratch gets the same answer as
	// applying the diff to the stats
	beforeStats := func() enginepb.MVCCStats {
		iter := e.NewIterator(engine.IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		beforeStats, err := engine.ComputeStatsGo(iter, engine.NilKey, engine.MVCCKeyMax, 10)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		return beforeStats
	}()

	mkSST := func(kvs []engine.MVCCKeyValue) []byte {
		sstFile := &engine.MemFile{}
		sst := engine.MakeIngestionSSTWriter(sstFile)
		defer sst.Close()
		for _, kv := range kvs {
			if err := sst.Put(kv.Key, kv.Value); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		if err := sst.Finish(); err != nil {
			t.Fatalf("%+v", err)
		}
		return sstFile.Data()
	}

	sstBytes := mkSST(sstKVs)

	cArgs := batcheval.CommandArgs{
		Header: roachpb.Header{
			Timestamp: hlc.Timestamp{WallTime: 7},
		},
		Args: &roachpb.AddSSTableRequest{
			RequestHeader: roachpb.RequestHeader{Key: keys.MinKey, EndKey: keys.MaxKey},
			Data:          sstBytes,
		},
		Stats: &enginepb.MVCCStats{},
	}
	_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	evaledStats := beforeStats
	evaledStats.Add(*cArgs.Stats)

	if err := e.WriteFile("sst", sstBytes); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := e.IngestExternalFiles(ctx, []string{"sst"}, true /* skip writing global seqno */, true /* modify the sst */); err != nil {
		t.Fatalf("%+v", err)
	}

	afterStats := func() enginepb.MVCCStats {
		iter := e.NewIterator(engine.IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		afterStats, err := engine.ComputeStatsGo(iter, engine.NilKey, engine.MVCCKeyMax, 10)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		return afterStats
	}()
	evaledStats.Add(delta)
	evaledStats.ContainsEstimates = false
	if !afterStats.Equal(evaledStats) {
		t.Errorf("mvcc stats mismatch: diff(expected, actual): %s", pretty.Diff(afterStats, evaledStats))
	}
}

func TestEvalAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	updater := settings.NewUpdater(&st.SV)
	//updater.Set("kv.range_split.by_load_enabled", settings.EncodeBool(false), "t")
	if err := updater.Set("kv.range.backpressure_range_size_multiplier", settings.EncodeFloat(0), "f"); err != nil {
		t.Fatal(err)
	}
	if err := updater.Set("kv.bulk_io_write.concurrent_addsstable_requests", settings.EncodeInt(2), "i"); err != nil {
		t.Fatal(err)
	}

	//dir, dirCleanupFn := testutils.TempDir(t)
	//defer dirCleanupFn()
	//dir := "/data/sst"

	storeSpec := base.DefaultTestStoreSpec
	storeSpec.InMemory = true
	//storeSpec.Path = dir
	//storeSpec.RocksDBOptions = "disable_auto_compactions=true"
	storeSpec.RocksDBOptions = "level0_slowdown_writes_trigger=2000;level0_stop_writes_trigger=4000"

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Settings: st, Insecure: true, DisableEventLog: true,
		StoreSpecs: []base.StoreSpec{storeSpec}})

	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	stores := s.GetStores().(*storage.Stores)
	storage.BackpressureRangeSizeMultiplier.Override(&st.SV, 0)
	mult := storage.BackpressureRangeSizeMultiplier.Get(&st.SV)
	fmt.Printf("mult:%v\n", mult)
	//预先split range
	ranges := listRanges(stores)
	lastRange := ranges[len(ranges)-1]
	splits := make([][]byte, 100)
	spanKey := lastRange.StartKey
	for i := range splits {
		bkey := []byte(fmt.Sprintf("k-%03d", i))
		bkey = append(lastRange.StartKey, bkey...)
		err := db.AdminSplit(ctx, spanKey, bkey)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		splits[i] = spanKey
		spanKey = bkey
		fmt.Printf("SplitKey : %v\n", spanKey)
	}
	ranges = listRanges(stores)
	fmt.Printf("%d\n", len(ranges))
	concurrency := 1
	mCh := make(chan int, concurrency)
	startT := timeutil.Now()
	makeT := int64(0)
	for i := 0; i < concurrency; i++ {
		total := 0
		for i := 0; i < 100; i++ {
			begin := timeutil.Now().UnixNano()
			data, minKey, maxKey, err := makeSST(splits[rand.Intn(99)], 10000, 256)
			makeT += (timeutil.Now().UnixNano() - begin)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			total += len(data)

			// Do an initial ingest.
			ingestCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "test-recording")
			defer cancel()
			if err := db.AddSSTable(ingestCtx, minKey.Key, maxKey.Key.PrefixEnd(), data, false, nil, false); err != nil {
				t.Fatalf("%+v", err)
			}
			formatted := tracing.FormatRecordedSpans(collect())
			if err := testutils.MatchInOrder(formatted,
				"evaluating AddSSTable",
				"sideloadable proposal detected",
				"ingested SSTable at index",
			); err != nil {
				t.Fatal(err)
			}
		}
		mCh <- total
	}

	total := 0
	for i := 0; i < concurrency; i++ {
		total += <-mCh
	}

	endT := timeutil.Now()
	tt := (endT.UnixNano() - startT.UnixNano()) / 1e9
	band := int64(total) / tt
	t.Logf("Add SST Size = %d , band=%d, time=%d, make time=%d", total/1024/1024, band/1024/1024, tt, makeT/1e9)
}

//num-entry个数, size-字节长度
func makeSST(
	prefix roachpb.RKey, num int, size int,
) ([]byte, engine.MVCCKey, engine.MVCCKey, error) {
	kvs := make([]engine.MVCCKeyValue, num)
	for i := range kvs {
		bkey := []byte(fmt.Sprintf("%d", rand.Int63()))
		bkey = append(prefix, bkey...)

		key := engine.MVCCKey{Key: bkey,
			Timestamp: hlc.Timestamp{WallTime: rand.Int63()}}
		value := roachpb.MakeValueFromBytes(Bytes(size)).RawBytes
		kvs[i] = engine.MVCCKeyValue{Key: key, Value: value}
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key.Less(kvs[j].Key) })
	data, err := kvSSTable(kvs)
	return data, kvs[0].Key, kvs[len(kvs)-1].Key, err
}
func Bytes(n int) []byte {
	d := make([]byte, n)
	rand.Read(d)

	return d
}

//func kvSSTable(kvs []engine.MVCCKeyValue) ([]byte, error) {
//	sst, err := engine.MakeRocksDBSstFileWriter()
//	if err != nil {
//		return nil, err
//	}
//	defer sst.Close()
//	for _,kv := range kvs {
//		if err := sst.Add(kv); err != nil {
//			return nil, err
//		}
//	}
//	return sst.Finish()
//}

func kvSSTable(kvs []engine.MVCCKeyValue) ([]byte, error) {
	sstFile := &engine.MemFile{}
	sst := engine.MakeBackupSSTWriter(sstFile)
	//sst, err := engine.MakeRocksDBSstFileWriter()
	//if err != nil {
	//	return nil, err
	//}

	defer sst.Close()
	for _, kv := range kvs {
		if err := sst.Put(kv.Key, kv.Value); err != nil {
			return nil, err
		}
	}
	if err := sst.Finish(); err != nil {
		return nil, err
	}

	return sstFile.Data(), nil
}

func listRanges(stores *storage.Stores) []*roachpb.RangeDescriptor {
	ranges := make([]*roachpb.RangeDescriptor, 0)
	_ = stores.VisitStores(func(s *storage.Store) error {
		s.VisitReplicas(func(repl *storage.Replica) bool {
			//fmt.Printf("RangeID = %d, start = %v \n",repl.RangeID,repl.Desc().StartKey)
			ranges = append(ranges, repl.Desc())
			return true
		})
		return nil
	})
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].RangeID < ranges[j].RangeID })
	return ranges
}
