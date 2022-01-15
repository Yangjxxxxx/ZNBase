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

package engine

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
)

//// InMem wraps RocksDB and configures it for in-memory only storage.
//type InMem struct {
//	*RocksDB
//}

// NewInMemByType allocates and returns a new, opened in-memory engine. The caller
// must call the engine's Close method when the engine is no longer needed.
//
// FIXME(tschottdorf): make the signature similar to NewRocksDB (require a cfg).
func NewInMemByType(
	ctx context.Context, engine enginepb.EngineType, spec base.StoreSpec, cacheSize int64,
) Engine {
	switch engine {
	//case enginepb.EngineTypeTeePebbleRocksDB:
	//	return newTeeInMem(ctx, attrs, cacheSize)
	case enginepb.EngineTypePebble:
		return newPebbleInMem(ctx, spec.Attributes, cacheSize)
	case enginepb.EngineTypeRocksDB, enginepb.EngineTypeDefault:
		return newRocksDBInMem(spec, cacheSize)
	}
	panic(fmt.Sprintf("unknown engine type: %d", engine))
}

// NewInMem allocates and returns a new, opened InMem engine.
// The caller must call the engine's Close method when the engine is no longer
// needed.
//
// FIXME(tschottdorf): make the signature similar to NewRocksDB (require a cfg).
//func NewInMem(attrs roachpb.Attributes, cacheSize int64) InMem {
//	cache := NewRocksDBCache(cacheSize)
//	// The cache starts out with a refcount of one, and creating the engine
//	// from it adds another refcount, at which point we release one of them.
//	defer cache.Release()
//
//	// TODO(bdarnell): The hard-coded 512 MiB is wrong; see
//	// https://github.com/znbasedb/znbase/issues/16750
//	rdb, err := newMemRocksDB(attrs, cache, 512<<20 /* MaxSizeBytes: 512 MiB */)
//	if err != nil {
//		panic(err)
//	}
//	db := InMem{RocksDB: rdb}
//	return db
//}

// NewInMem allocates and returns a new, opened InMem engine.
// The caller must call the engine's Close method when the engine is no longer
// needed.
func NewInMem(attrs roachpb.Attributes, cacheSize int64) Engine {
	return NewInMemByType(context.Background(), enginepb.EngineTypeDefault, base.StoreSpec{Attributes: attrs}, cacheSize)
}

//var _ Engine = InMem{}
