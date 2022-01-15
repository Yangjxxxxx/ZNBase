// Copyright 2018  The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

#pragma once

#include <libroach.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include "encoding.h"

namespace znbase {

static const int kDefaultVerbosityForInfoLogging = 3;

// Make a new rocksdb::Logger that calls back into Go with a translation of
// RocksDB's log level into a corresponding Go log level.
// The message is logged if severity is higher than info, or if severity is
// info and glog verbosity is at least `info_verbosity`.
rocksdb::Logger* NewDBLogger(int info_verbosity);

// DBMakeOptions constructs a rocksdb::Options given a DBOptions.
rocksdb::Options DBMakeOptions(DBOptions db_opts);

class DBPrefixExtractor : public rocksdb::SliceTransform {
 public:
    DBPrefixExtractor() {}

    virtual const char* Name() const { return "bini_comparator"; }

    // MVCC keys are encoded as <user-key>/<timestamp>. Extract the <user-key>
    // prefix which will allow for more efficient iteration over the keys
    // matching a particular <user-key>. Specifically, the <user-key> will be
    // added to the per table bloom filters and will be used to skip tables
    // which do not contain the <user-key>.
    virtual rocksdb::Slice Transform(const rocksdb::Slice& src) const { return KeyPrefix(src); }

    virtual bool InDomain(const rocksdb::Slice& src) const { return true; }
};

}  // namespace znbase
