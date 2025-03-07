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

#include "engine.h"
#include "db.h"
#include "encoding.h"
#include "env_manager.h"
#include "fmt.h"
#include "getter.h"
#include "iterator.h"
#include "protos/storage/engine/enginepb/rocksdb.pb.h"
#include "status.h"
#include "strings.h"

using namespace znbase;

DBEngine::~DBEngine() {}

DBStatus DBEngine::AssertPreClose() { return kSuccess; }

DBSSTable* DBEngine::GetSSTables(int* n) {
  std::vector<rocksdb::LiveFileMetaData> metadata;
  rep->GetLiveFilesMetaData(&metadata);
  *n = metadata.size();
  // We malloc the result so it can be deallocated by the caller using free().
  const int size = metadata.size() * sizeof(DBSSTable);
  DBSSTable* tables = reinterpret_cast<DBSSTable*>(malloc(size));
  memset(tables, 0, size);
  for (int i = 0; i < metadata.size(); i++) {
    tables[i].level = metadata[i].level;
    tables[i].size = metadata[i].size;

    rocksdb::Slice tmp;
    if (DecodeKey(metadata[i].smallestkey, &tmp, &tables[i].start_key.wall_time,
                  &tables[i].start_key.logical)) {
      // This is a bit ugly because we want DBKey.key to be copied and
      // not refer to the memory in metadata[i].smallestkey.
      DBString str = ToDBString(tmp);
      tables[i].start_key.key = DBSlice{str.data, str.len};
    }
    if (DecodeKey(metadata[i].largestkey, &tmp, &tables[i].end_key.wall_time,
                  &tables[i].end_key.logical)) {
      DBString str = ToDBString(tmp);
      tables[i].end_key.key = DBSlice{str.data, str.len};
    }
  }
  return tables;
}

DBStatus DBEngine::GetSortedWALFiles(DBWALFile** out_files, int* n) {
  rocksdb::VectorLogPtr files;
  rocksdb::Status s = rep->GetSortedWalFiles(files);
  if (!s.ok()) {
    return ToDBStatus(s);
  }
  *n = files.size();
  // We calloc the result so it can be deallocated by the caller using free().
  *out_files = reinterpret_cast<DBWALFile*>(calloc(files.size(), sizeof(DBWALFile)));
  for (int i = 0; i < files.size(); i++) {
    (*out_files)[i].log_number = files[i]->LogNumber();
    (*out_files)[i].size = files[i]->SizeFileBytes();
  }
  return kSuccess;
}

DBString DBEngine::GetUserProperties() {
  rocksdb::TablePropertiesCollection props;
  rocksdb::Status status = rep->GetPropertiesOfAllTables(&props);

  znbase::storage::engine::enginepb::SSTUserPropertiesCollection all;
  if (!status.ok()) {
    all.set_error(status.ToString());
    return ToDBString(all.SerializeAsString());
  }

  for (auto i = props.begin(); i != props.end(); i++) {
    znbase::storage::engine::enginepb::SSTUserProperties* sst = all.add_sst();
    sst->set_path(i->first);
    auto userprops = i->second->user_collected_properties;

    auto ts_min = userprops.find("znbase.ts.min");
    if (ts_min != userprops.end() && !ts_min->second.empty()) {
      if (!DecodeTimestamp(rocksdb::Slice(ts_min->second), sst->mutable_ts_min())) {
        fmt::SStringPrintf(
            all.mutable_error(), "unable to decode znbase.ts.min value '%s' in table %s",
            rocksdb::Slice(ts_min->second).ToString(true).c_str(), sst->path().c_str());
        break;
      }
    }

    auto ts_max = userprops.find("znbase.ts.max");
    if (ts_max != userprops.end() && !ts_max->second.empty()) {
      if (!DecodeTimestamp(rocksdb::Slice(ts_max->second), sst->mutable_ts_max())) {
        fmt::SStringPrintf(
            all.mutable_error(), "unable to decode znbase.ts.max value '%s' in table %s",
            rocksdb::Slice(ts_max->second).ToString(true).c_str(), sst->path().c_str());
        break;
      }
    }
  }
  return ToDBString(all.SerializeAsString());
}

namespace znbase {

DBImpl::DBImpl(rocksdb::DB* r, std::unique_ptr<EnvManager> e, std::shared_ptr<rocksdb::Cache> bc,
               std::shared_ptr<DBEventListener> event_listener)
    : DBEngine(r, &iters_count),
      env_mgr(std::move(e)),
      rep_deleter(r),
      block_cache(bc),
      event_listener(event_listener),
      iters_count(0) {}

DBImpl::~DBImpl() {
  const rocksdb::Options& opts = rep->GetOptions();
  const std::shared_ptr<rocksdb::Statistics>& s = opts.statistics;
  rocksdb::Info(opts.info_log, "bloom filter utility:    %0.1f%%",
                (100.0 * s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_USEFUL)) /
                    s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_CHECKED));
}

DBStatus DBImpl::AssertPreClose() {
  const int64_t n = iters_count.load();
  if (n == 0) {
    return kSuccess;
  }
  return FmtStatus("%" PRId64 " leaked iterators", n);
}

DBStatus DBImpl::Put(DBKey key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Put(options, EncodeKey(key), ToSlice(value)));
}

DBStatus DBImpl::Merge(DBKey key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Merge(options, EncodeKey(key), ToSlice(value)));
}

DBStatus DBImpl::Get(DBKey key, DBString* value) {
  rocksdb::ReadOptions read_opts;
  DBGetter base(rep, read_opts, EncodeKey(key));
  return base.Get(value);
}

DBStatus DBImpl::Delete(DBKey key) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Delete(options, EncodeKey(key)));
}

DBStatus DBImpl::SingleDelete(DBKey key) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->SingleDelete(options, EncodeKey(key)));
}

DBStatus DBImpl::DeleteRange(DBKey start, DBKey end) {
  rocksdb::WriteOptions options;
  return ToDBStatus(
      rep->DeleteRange(options, rep->DefaultColumnFamily(), EncodeKey(start), EncodeKey(end)));
}

DBStatus DBImpl::CommitBatch(bool sync) { return FmtStatus("unsupported"); }

DBStatus DBImpl::ApplyBatchRepr(DBSlice repr, bool sync) {
  rocksdb::WriteBatch batch(ToString(repr));
  rocksdb::WriteOptions options;
  options.sync = sync;
  return ToDBStatus(rep->Write(options, &batch));
}

DBSlice DBImpl::BatchRepr() { return ToDBSlice("unsupported"); }

DBIterator* DBImpl::NewIter(DBIterOptions iter_opts) {
  DBIterator* iter = new DBIterator(iters, iter_opts);
  iter->rep.reset(rep->NewIterator(iter->read_opts));
  return iter;
}

void SplitString(const std::string &str, std::vector <std::string> &v_str, const char *delim) {
  char *token = NULL;
  char *save = NULL;
  char *buff = (char*)(str.c_str());
  token = strtok_r(buff, delim, &save);
  while (token != NULL) {
    v_str.push_back(token);
    token = strtok_r(NULL, delim, &save);
  }
}

// GetStats retrieves a subset of RocksDB stats that are relevant to
// ZNBaseDB.
DBStatus DBImpl::GetStats(DBStatsResult* stats) {
  const rocksdb::Options& opts = rep->GetOptions();
  const std::shared_ptr<rocksdb::Statistics>& s = opts.statistics;

  uint64_t memtable_total_size;
  rep->GetIntProperty("rocksdb.cur-size-all-mem-tables", &memtable_total_size);

  uint64_t table_readers_mem_estimate;
  rep->GetIntProperty("rocksdb.estimate-table-readers-mem", &table_readers_mem_estimate);

  uint64_t pending_compaction_bytes_estimate;
  rep->GetIntProperty("rocksdb.estimate-pending-compaction-bytes",
                      &pending_compaction_bytes_estimate);

  std::string l0_file_count_str;
  rep->GetProperty("rocksdb.num-files-at-level0", &l0_file_count_str);
  std::string base_level_file_count_str;
  std::string base_level_str;
  rep->GetProperty("rocksdb.base-level", &base_level_str);
  rep->GetProperty("rocksdb.num-files-at-level" + base_level_str, &base_level_file_count_str);

  std::string level_stats;
  rep->GetProperty("rocksdb.levelstats", &level_stats);

  std::vector<std::string> level_infos;
  SplitString(level_stats, level_infos, "\n");

  std::vector<std::string> l0_infos;
  SplitString(level_infos[2], l0_infos, " ");

  int base_level = std::atoi(base_level_str.c_str());
  std::vector<std::string> base_level_infos;
  SplitString(level_infos[base_level + 2], base_level_infos, " ");

  uint64_t l0_bytes = std::atoi(l0_infos[2].c_str());
  uint64_t base_level_bytes = std::atoi(base_level_infos[2].c_str());

  int64_t l0_compaction_score = (int64_t)(std::atof(l0_infos[3].c_str()) * 10);
  int64_t base_level_compaction_score = (int64_t)(std::atof(base_level_infos[3].c_str()) * 10);

  stats->block_cache_hits = (int64_t)s->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
  stats->block_cache_misses = (int64_t)s->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
  stats->block_cache_usage = (int64_t)block_cache->GetUsage();
  stats->block_cache_pinned_usage = (int64_t)block_cache->GetPinnedUsage();
  stats->bloom_filter_prefix_checked =
      (int64_t)s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_CHECKED);
  stats->bloom_filter_prefix_useful =
      (int64_t)s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_USEFUL);
  stats->memtable_total_size = memtable_total_size;
  stats->flushes = (int64_t)event_listener->GetFlushes();
  stats->compactions = (int64_t)event_listener->GetCompactions();
  stats->table_readers_mem_estimate = table_readers_mem_estimate;
  stats->pending_compaction_bytes_estimate = pending_compaction_bytes_estimate >> 20; // kMB
  stats->l0_file_count = std::atoi(l0_file_count_str.c_str());
  stats->base_level_file_count = std::atoi(base_level_file_count_str.c_str());
  stats->l0_bytes = l0_bytes;
  stats->base_level_bytes = base_level_bytes;
  stats->l0_compaction_score = l0_compaction_score;
  stats->base_level_compaction_score = base_level_compaction_score;
  //增加写失速指标
  stats->stall_l0_slowdown_micros = (int64_t)s->getTickerCount(rocksdb::STALL_L0_SLOWDOWN_MICROS);;
  stats->stall_memtable_compaction_micros = (int64_t)s->getTickerCount(rocksdb::STALL_MEMTABLE_COMPACTION_MICROS);;
  stats->stall_l0_num_files_micros = (int64_t)s->getTickerCount(rocksdb::STALL_L0_NUM_FILES_MICROS);;
  stats->stall_micros = (int64_t)s->getTickerCount(rocksdb::STALL_MICROS);;
  stats->stall_l0_slowdown_count = (int64_t)s->getTickerCount(rocksdb::STALL_L0_SLOWDOWN_COUNT);;
  stats->stall_memtable_compaction_count = (int64_t)s->getTickerCount(rocksdb::STALL_MEMTABLE_COMPACTION_COUNT);;
  stats->stall_l0_num_files_count = (int64_t)s->getTickerCount(rocksdb::STALL_L0_NUM_FILES_COUNT);;
  stats->write_stall = (int64_t)s->getTickerCount(rocksdb::WRITE_STALL);;
  return kSuccess;
}

// `GetTickersAndHistograms` retrieves maps of all RocksDB tickers and histograms.
// It differs from `GetStats` by getting _every_ ticker and histogram, and by not
// getting anything else (DB properties, for example).
//
// In addition to freeing the `DBString`s in the result, the caller is also
// responsible for freeing `DBTickersAndHistogramsResult::tickers` and
// `DBTickersAndHistogramsResult::histograms`.
DBStatus DBImpl::GetTickersAndHistograms(DBTickersAndHistogramsResult* stats) {
  const rocksdb::Options& opts = rep->GetOptions();
  const std::shared_ptr<rocksdb::Statistics>& s = opts.statistics;
  stats->tickers_len = rocksdb::TickersNameMap.size();
  // We malloc the result so it can be deallocated by the caller using free().
  stats->tickers = static_cast<TickerInfo*>(
      malloc(stats->tickers_len * sizeof(TickerInfo)));
  if (stats->tickers == nullptr) {
    return FmtStatus("malloc failed");
  }
  for (size_t i = 0; i < stats->tickers_len; ++i) {
    stats->tickers[i].name = ToDBString(rocksdb::TickersNameMap[i].second);
    stats->tickers[i].value = s->getTickerCount(static_cast<uint32_t>(i));
  }

  stats->histograms_len = rocksdb::HistogramsNameMap.size();
  // We malloc the result so it can be deallocated by the caller using free().
  stats->histograms = static_cast<HistogramInfo*>(
      malloc(stats->histograms_len * sizeof(HistogramInfo)));
  if (stats->histograms == nullptr) {
    return FmtStatus("malloc failed");
  }
  for (size_t i = 0; i < stats->histograms_len; ++i) {
    stats->histograms[i].name = ToDBString(rocksdb::HistogramsNameMap[i].second);
    rocksdb::HistogramData data;
    s->histogramData(static_cast<uint32_t>(i), &data);
    stats->histograms[i].mean = data.average;
    stats->histograms[i].p50 = data.median;
    stats->histograms[i].p95 = data.percentile95;
    stats->histograms[i].p99 = data.percentile99;
    stats->histograms[i].max = data.max;
    stats->histograms[i].count = data.count;
    stats->histograms[i].sum = data.sum;
  }
  return kSuccess;
}

DBString DBImpl::GetCompactionStats() {
  std::string tmp;
  rep->GetProperty("rocksdb.cfstats-no-file-histogram", &tmp);
  return ToDBString(tmp);
}

DBStatus DBImpl::GetEnvStats(DBEnvStatsResult* stats) {
  // Always initialize the fields.
  stats->encryption_status = DBString();
  stats->total_files = stats->total_bytes = stats->active_key_files = stats->active_key_bytes = 0;
  stats->encryption_type = 0;

  if (env_mgr->env_stats_handler == nullptr || env_mgr->file_registry == nullptr) {
    // We can't compute these if we don't have a file registry or stats handler.
    // This happens in OSS mode or when encryption has not been turned on.
    return kSuccess;
  }

  // Get encryption algorithm.
  stats->encryption_type = env_mgr->env_stats_handler->GetActiveStoreKeyType();

  // Get encryption status.
  std::string encryption_status;
  auto status = env_mgr->env_stats_handler->GetEncryptionStats(&encryption_status);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  stats->encryption_status = ToDBString(encryption_status);

  // Get file statistics.
  FileStats file_stats(env_mgr.get());
  status = file_stats.GetFiles(rep);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  // Get current active key ID.
  auto active_key_id = env_mgr->env_stats_handler->GetActiveDataKeyID();

  // Request stats for the Data env only.
  status = file_stats.GetStatsForEnvAndKey(enginepb::Data, active_key_id, stats);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  return kSuccess;
}

DBStatus DBImpl::GetEncryptionRegistries(DBEncryptionRegistries* result) {
  // Always initialize the fields.
  result->file_registry = DBString();
  result->key_registry = DBString();

  if (env_mgr->env_stats_handler == nullptr || env_mgr->file_registry == nullptr) {
    // We can't compute these if we don't have a file registry or stats handler.
    // This happens in OSS mode or when encryption has not been turned on.
    return kSuccess;
  }

  auto file_registry = env_mgr->file_registry->GetFileRegistry();
  if (file_registry == nullptr) {
    return ToDBStatus(rocksdb::Status::InvalidArgument("file registry has not been loaded"));
  }

  std::string serialized_file_registry;
  if (!file_registry->SerializeToString(&serialized_file_registry)) {
    return ToDBStatus(rocksdb::Status::InvalidArgument("failed to serialize file registry proto"));
  }

  std::string serialized_key_registry;
  auto status = env_mgr->env_stats_handler->GetEncryptionRegistry(&serialized_key_registry);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  result->file_registry = ToDBString(serialized_file_registry);
  result->key_registry = ToDBString(serialized_key_registry);

  return kSuccess;
}

// EnvWriteFile writes the given data as a new "file" in the given engine.
DBStatus DBImpl::EnvWriteFile(DBSlice path, DBSlice contents) {
  rocksdb::Status s;

  const rocksdb::EnvOptions soptions;
  std::unique_ptr<rocksdb::WritableFile> destfile;
  s = this->rep->GetEnv()->NewWritableFile(ToString(path), &destfile, soptions);
  if (!s.ok()) {
    return ToDBStatus(s);
  }

  s = destfile->Append(ToSlice(contents));
  if (!s.ok()) {
    return ToDBStatus(s);
  }

  return kSuccess;
}

// EnvOpenFile opens a new file in the given engine.
DBStatus DBImpl::EnvOpenFile(DBSlice path, rocksdb::WritableFile** file) {
  rocksdb::Status status;
  const rocksdb::EnvOptions soptions;
  std::unique_ptr<rocksdb::WritableFile> rocksdb_file;

  // Create the file.
  status = this->rep->GetEnv()->NewWritableFile(ToString(path), &rocksdb_file, soptions);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  *file = rocksdb_file.release();
  return kSuccess;
}

// EnvReadFile reads the content of the given filename.
DBStatus DBImpl::EnvReadFile(DBSlice path, DBSlice* contents) {
  rocksdb::Status status;
  std::string data;

  status = ReadFileToString(this->rep->GetEnv(), ToString(path), &data);
  if (!status.ok()) {
    if (status.IsNotFound()) {
      return FmtStatus("No such file or directory");
    }
    return ToDBStatus(status);
  }
  contents->data = static_cast<char*>(malloc(data.size()));
  contents->len = data.size();
  memcpy(contents->data, data.c_str(), data.size());
  return kSuccess;
}

// CloseFile closes the given file in the given engine.
DBStatus DBImpl::EnvCloseFile(rocksdb::WritableFile* file) {
  rocksdb::Status status = file->Close();
  delete file;
  return ToDBStatus(status);
}

// EnvAppendFile appends the given data to the file in the given engine.
DBStatus DBImpl::EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents) {
  rocksdb::Status status = file->Append(ToSlice(contents));
  return ToDBStatus(status);
}

// EnvSyncFile synchronously writes the data of the file to the disk.
DBStatus DBImpl::EnvSyncFile(rocksdb::WritableFile* file) {
  rocksdb::Status status = file->Sync();
  return ToDBStatus(status);
}

// EnvDeleteFile deletes the file with the given filename.
DBStatus DBImpl::EnvDeleteFile(DBSlice path) {
  rocksdb::Status status = this->rep->GetEnv()->DeleteFile(ToString(path));
  if (status.IsNotFound()) {
    return FmtStatus("No such file or directory");
  }
  return ToDBStatus(status);
}

// EnvDeleteDirAndFiles deletes the directory with the given dir name and any
// files it contains but not subdirectories.
DBStatus DBImpl::EnvDeleteDirAndFiles(DBSlice dir) {
  rocksdb::Status status;

  std::vector<std::string> files;
  this->rep->GetEnv()->GetChildren(ToString(dir), &files);
  for (auto& file : files) {
    if (file != "." && file != "..") {
      this->rep->GetEnv()->DeleteFile(ToString(dir) + "/" + file);
    }
  }

  status = this->rep->GetEnv()->DeleteDir(ToString(dir));
  if (status.IsNotFound()) {
    return FmtStatus("No such file or directory");
  }
  return ToDBStatus(status);
}

// EnvLinkFile creates 'newname' as a hard link to 'oldname'.
DBStatus DBImpl::EnvLinkFile(DBSlice oldname, DBSlice newname) {
  return ToDBStatus(this->rep->GetEnv()->LinkFile(ToString(oldname), ToString(newname)));
}

}  // namespace znbase
