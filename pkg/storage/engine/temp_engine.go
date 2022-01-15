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

package engine

import (
	"context"
	"fmt"

	"github.com/znbasedb/pebble"
	"github.com/znbasedb/pebble/vfs"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
)

// NewTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory.
func NewTempEngine(
	ctx context.Context,
	engine enginepb.EngineType,
	tempStorage base.TempStorageConfig,
	storeSpec base.StoreSpec,
) (MapProvidingEngine, error) {
	switch engine {
	case enginepb.EngineTypePebble:
		return NewPebbleTempEngine(ctx, tempStorage, storeSpec)

	case enginepb.EngineTypeRocksDB, enginepb.EngineTypeDefault:
		return NewRocksDBTempEngine(tempStorage, storeSpec)
	}

	panic(fmt.Sprintf("unknown engine type: %d", DefaultStorageEngine))
}

// NewRocksDBTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory.
func NewRocksDBTempEngine(
	tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (MapProvidingEngine, error) {
	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to temporary store.
		return newRocksDBInMem(base.StoreSpec{} /* attrs */, 0 /* cacheSize */), nil
	}

	rocksDBCfg := RocksDBConfig{
		Attrs: roachpb.Attributes{},
		Dir:   tempStorage.Path,
		// MaxSizeBytes doesn't matter for temp storage - it's not
		// enforced in any way.
		MaxSizeBytes:    0,
		MaxOpenFiles:    128, // TODO(arjun): Revisit this.
		UseFileRegistry: storeSpec.UseFileRegistry,
		ExtraOptions:    storeSpec.ExtraOptions,
	}
	rocksDBCache := NewRocksDBCache(0)
	rocksdb, err := NewRocksDB(rocksDBCfg, rocksDBCache)
	if err != nil {
		return nil, err
	}

	return rocksdb, nil
}

// NewPebbleTempEngine creates a new Pebble engine for DistSQL processors to use
// when the working set is larger than can be stored in memory.
func NewPebbleTempEngine(
	ctx context.Context, tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (
	MapProvidingEngine, /*diskmap.Factory, fs.FS,*/
	error,
) {

	// Default options as copied over from pebble/cmd/pebble/db.go
	opts := DefaultPebbleOptions()
	// Pebble doesn't currently support 0-size caches, so use a 128MB cache for
	// now.
	opts.Cache = pebble.NewCache(128 << 20)
	defer opts.Cache.Unref()

	// The Pebble temp engine does not use MVCC Encoding. Instead, the
	// caller-provided key is used as-is (with the prefix prepended). See
	// pebbleMap.makeKey and pebbleMap.makeKeyWithSequence on how this works.
	// Use the default bytes.Compare-like comparer.
	opts.Comparer = pebble.DefaultComparer
	opts.DisableWAL = true
	opts.TablePropertyCollectors = nil

	storageConfig := storageConfigFromTempStorageConfigAndStoreSpec(tempStorage, storeSpec)
	if tempStorage.InMemory {
		opts.FS = vfs.NewMem()
		storageConfig.Dir = ""
	}

	p, err := NewPebble(
		ctx,
		PebbleConfig{
			RocksDBConfig: storageConfig,
			Opts:          opts,
		},
	)
	if err != nil {
		return nil, err
	}
	//MapProvidingEngine{Engine: p, diskmap.Factory: &pebbleTempEngine{db: p.db}}
	//return &pebbleTempEngine{db: p.db}, p, nil
	return p, nil
}

// storageConfigFromTempStorageConfigAndStoreSpec creates a base.StorageConfig
// used by both the RocksDB and Pebble temp engines from the given arguments.
func storageConfigFromTempStorageConfigAndStoreSpec(
	config base.TempStorageConfig, spec base.StoreSpec,
) RocksDBConfig {
	return RocksDBConfig{
		Attrs: roachpb.Attributes{},
		Dir:   config.Path,
		// MaxSize doesn't matter for temp storage - it's not enforced in any way.
		MaxSizeBytes:    0,
		UseFileRegistry: spec.UseFileRegistry,
		ExtraOptions:    spec.ExtraOptions,
	}
}

//type pebbleTempEngine struct {
//	db *pebble.DB
//}
//
//// Close implements the diskmap.Factory interface.
//func (r *pebbleTempEngine) Close() {
//	err := r.db.Close()
//	if err != nil {
//		log.Fatal(context.TODO(), err)
//	}
//}
//
//// NewSortedDiskMap implements the diskmap.Factory interface.
//func (r *pebbleTempEngine) NewSortedDiskMap() diskmap.SortedDiskMap {
//	return newPebbleMap(r.db, false /* allowDuplications */)
//}
//
//// NewSortedDiskMultiMap implements the diskmap.Factory interface.
//func (r *pebbleTempEngine) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
//	return newPebbleMap(r.db, true /* allowDuplicates */)
//}
