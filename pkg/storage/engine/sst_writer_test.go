// Copyright 2019  The Cockroach Authors.
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

package engine

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/randutil"
)

func makeIntTableKVs(numKeys, valueSize, maxRevisions int) []MVCCKeyValue {
	prefix := encoding.EncodeUvarintAscending(keys.MakeTablePrefix(uint32(100)), uint64(1))
	kvs := make([]MVCCKeyValue, numKeys)
	r, _ := randutil.NewPseudoRand()

	var k int
	for i := 0; i < numKeys; {
		k += 1 + rand.Intn(100)
		key := encoding.EncodeVarintAscending(append([]byte{}, prefix...), int64(k))
		buf := make([]byte, valueSize)
		randutil.ReadTestdataBytes(r, buf)
		revisions := 1 + r.Intn(maxRevisions)

		ts := int64(maxRevisions * 100)
		for j := 0; j < revisions && i < numKeys; j++ {
			ts -= 1 + r.Int63n(99)
			kvs[i].Key.Key = key
			kvs[i].Key.Timestamp.WallTime = ts
			kvs[i].Key.Timestamp.Logical = r.Int31()
			kvs[i].Value = roachpb.MakeValueFromString(string(buf)).RawBytes
			i++
		}
	}
	return kvs
}

func makeRocksSST(t testing.TB, kvs []MVCCKeyValue, ingestion bool) []byte {
	var w RocksDBSstFileWriter
	var err error
	if ingestion {
		w, err = MakeRocksDBIngestSstFileWriter()
	} else {
		w, err = MakeRocksDBSstFileWriter()
	}

	require.NoError(t, err)
	defer w.Close()

	for i := range kvs {
		if err := w.Add(kvs[i]); err != nil {
			t.Fatal(err)
		}
	}
	sst, err := w.Finish()
	require.NoError(t, err)
	return sst
}

func makePebbleSST(t testing.TB, kvs []MVCCKeyValue, ingestion bool) []byte {
	f := &MemFile{}
	var w SSTWriter
	if ingestion {
		w = MakeIngestionSSTWriter(f)
	} else {
		w = MakeBackupSSTWriter(f)
	}
	defer w.Close()

	for i := range kvs {
		if err := w.Put(kvs[i].Key, kvs[i].Value); err != nil {
			t.Fatal(err)
		}
	}
	err := w.Finish()
	require.NoError(t, err)
	return f.Data()
}

// TestPebbleWritesSameSSTs tests that using pebble to write some SST produces
// the same file -- byte-for-byte -- as using our Rocks-based writer. This is is
// done not because we don't trust pebble to write the correct SST, but more
// because we otherwise don't have a great way to be sure we've configured it to
// to the same thing we configured RocksDB to do w.r.t. all the block size, key
// filtering, property collecting, etc settings. Getting these settings wrong
// could easily produce an SST with the same K/V content but subtle and hard to
// debug differences in runtime performance (which also could prove elusive when
// it could compacted away at any time and replaced with a Rocks-written one).
//
// This test may need to be removed if/when Pebble's SSTs diverge from Rocks'.
// That is probably OK: it is mostly intended to increase our confidence during
// the transition that we're not introducing a regression. Once pebble-written
// SSTs are the norm, comparing to ones written using the Rocks writer (which
// didn't actually share a configuration with the serving RocksDB, so they were
// already different from actual runtime-written SSTs) will no longer be a
// concen (though we will likely want testing of things like prop collectors).
func TestPebbleWritesSameSSTs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r, _ := randutil.NewPseudoRand()
	const numKeys, valueSize, revisions = 5000, 100, 100

	for _, ingest := range []bool{
		false, true,
	} {
		t.Run(fmt.Sprintf("ingestion=%v", ingest), func(t *testing.T) {
			kvs := makeIntTableKVs(numKeys, valueSize, revisions)
			sstRocks := makeRocksSST(t, kvs, ingest)
			sstPebble := makePebbleSST(t, kvs, ingest)

			//f, err := os.OpenFile("/data/sst/rocksdb.sst", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
			//defer f.Close()
			//if err != nil {
			//	fmt.Println(err.Error())
			//}else{
			//	_,err :=f.Write(sstRocks)
			//	if err != nil {
			//		fmt.Println(err.Error())
			//	}
			//}
			//
			//f2, err := os.OpenFile("/data/sst/pebble.sst", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
			//defer f2.Close()
			//if err != nil {
			//	fmt.Println(err.Error())
			//}else{
			//	_,err=f2.Write(sstPebble)
			//	if err != nil {
			//		fmt.Println(err.Error())
			//	}
			//}
			// ./znbase debug sst_dump --file=/data/sst --command=raw

			itRocks, err := NewMemSSTIterator(sstRocks, false)
			require.NoError(t, err)
			itPebble, err := NewMemSSTIterator(sstPebble, false)
			require.NoError(t, err)

			itPebble.Seek(NilKey)
			for itRocks.Seek(NilKey); ; {
				okRocks, err := itRocks.Valid()
				if err != nil {
					t.Fatal(err)
				}
				okPebble, err := itPebble.Valid()
				if err != nil {
					t.Fatal(err)
				}
				if !okRocks {
					break
				}
				if !okPebble {
					t.Fatal("expected valid")
				}
				require.Equal(t, itRocks.UnsafeKey(), itPebble.UnsafeKey())
				require.Equal(t, itRocks.UnsafeValue(), itPebble.UnsafeValue())

				if r.Intn(5) == 0 {
					itRocks.NextKey()
					itPebble.NextKey()
				} else {
					itRocks.Next()
					itPebble.Next()
				}
			}

			require.Equal(t, string(sstRocks), string(sstPebble))
			itRocks.Close()
			itPebble.Close()
		})
	}

}

func BenchmarkWriteRocksSSTable(b *testing.B) {
	b.StopTimer()
	// Writing the SST 10 times keeps size needed for ~10s benchtime under 1gb.
	const valueSize, revisions, ssts = 100, 100, 10
	kvs := makeIntTableKVs(b.N, valueSize, revisions)
	approxUserDataSizePerKV := kvs[b.N/2].Key.EncodedSize() + valueSize
	b.SetBytes(int64(approxUserDataSizePerKV * ssts))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < ssts; i++ {
		_ = makeRocksSST(b, kvs, false)
	}
	b.StopTimer()
}

func BenchmarkWriteSSTable(b *testing.B) {
	b.StopTimer()
	// Writing the SST 10 times keeps size needed for ~10s benchtime under 1gb.
	const valueSize, revisions, ssts = 100, 100, 10
	kvs := makeIntTableKVs(b.N, valueSize, revisions)
	approxUserDataSizePerKV := kvs[b.N/2].Key.EncodedSize() + valueSize
	b.SetBytes(int64(approxUserDataSizePerKV * ssts))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < ssts; i++ {
		_ = makePebbleSST(b, kvs, true /* ingestion */)
	}
	b.StopTimer()
}
