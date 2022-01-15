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

#include "comparator.h"
#include "encoding.h"

namespace znbase {

int DBComparator::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
  rocksdb::Slice key_a, key_b;
  rocksdb::Slice ts_a, ts_b;
  if (!SplitKey(a, &key_a, &ts_a) || !SplitKey(b, &key_b, &ts_b)) {
    // This should never happen unless there is some sort of corruption of
    // the keys.
    return a.compare(b);
  }

  const int c = key_a.compare(key_b);
  if (c != 0) {
    return c;
  }
  if (ts_a.empty()) {
    if (ts_b.empty()) {
      return 0;
    }
    return -1;
  } else if (ts_b.empty()) {
    return +1;
  }
  return ts_b.compare(ts_a);
}

bool DBComparator::Equal(const rocksdb::Slice& a, const rocksdb::Slice& b) const { return a == b; }

namespace {

  void ShrinkSlice(rocksdb::Slice* a, size_t size) { a->remove_suffix(a->size() - size); }

  int SharedPrefixLen(const rocksdb::Slice& a, const rocksdb::Slice& b) {
    auto n = std::min(a.size(), b.size());
    int i = 0;
    for (; i < n && a[i] == b[i]; ++i) {
    }
    return i;
  }

  bool FindSeparator(rocksdb::Slice* a, std::string* a_backing, const rocksdb::Slice& b) {
    auto prefix = SharedPrefixLen(*a, b);
    auto n = std::min(a->size(), b.size());
    if (prefix >= n) {
      // The > case is not actually possible.
      assert(prefix == n);
      // One slice is a prefix of another.
      return false;
    }
    // prefix < n. So can look at the characters at prefix, where they differed.
    if (static_cast<unsigned char>((*a)[prefix]) >= static_cast<unsigned char>(b[prefix])) {
      // == is not possible since they differed.
      assert((*a)[prefix] != b[prefix]);
      // So b is smaller than a.
      return false;
    }
    if ((prefix < b.size() - 1) ||
        static_cast<unsigned char>((*a)[prefix]) + 1 < static_cast<unsigned char>(b[prefix])) {
      // a and b do not have consecutive characters at prefix.
      (*a_backing)[prefix]++;
      ShrinkSlice(a, prefix + 1);
      return true;
    }
    // They two slices have consecutive characters at prefix, so we leave the
    // character at prefix unchanged for a. Now we are free to increment any
    // subsequent character in a, to make the new a bigger than the old a.
    ++prefix;
    for (int i = prefix; i < a->size() - 1; ++i) {
      if (static_cast<unsigned char>((*a)[i]) != 0xff) {
        (*a_backing)[i]++;
        ShrinkSlice(a, i + 1);
        return true;
      }
    }
    return false;
  }

}  // namespace

  void DBComparator::FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const {
    rocksdb::Slice key_s, key_l;
    rocksdb::Slice ts_s, ts_l;
    if (!SplitKey(*start, &key_s, &ts_s) || !SplitKey(limit, &key_l, &ts_l)) {
      return;
    }
    auto found = FindSeparator(&key_s, start, key_l);
    if (!found)
      return;
    start->resize(key_s.size() + 1);
    (*start)[key_s.size()] = 0x00;
  }

  void DBComparator::FindShortSuccessor(std::string* key) const {
    rocksdb::Slice k, ts;
    if (!SplitKey(*key, &k, &ts)) {
      return;
    }
    for (int i = 0; i < k.size(); ++i) {
      if (static_cast<unsigned char>(k[i]) != 0xff) {
        (*key)[i]++;
        key->resize(i + 2);
        (*key)[i + 1] = 0;
        return;
      }
    }
  }

}  // namespace znbase
