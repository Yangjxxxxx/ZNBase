// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include <iostream>

using namespace rocksdb;

std::string walFilePath = "/data/rocksdb/";
std::string kDBPath = "/tmp/rocksdb_simple_example";

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  //options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());


  std::vector<std::string> args;
  args.push_back("dump_wal");
  args.push_back("--walfile="+walFilePath);
  args.push_back("--load_wal");
  args.push_back("--header");
  LDBCommand* command = rocksdb::LDBCommand::InitFromCmdLineArgs(
          args, Options(), LDBOptions(), nullptr);

  command->load_wal_db_ = db;

  command->Run();
  bool is_succeed = command->GetExecuteState().IsSucceed();
  if(is_succeed){
    std::cout<<"succeed!"<<std::endl;
  }else{
    std::cout<<command->GetExecuteState().ToString() <<std::endl;
  }


  delete db;
  return 0;
}

//std::string walFilePath2 = "/data/rocksdb/archive/000141.log";
//int main() {
//  std::vector<std::string> args;
//  args.push_back("dump_wal");
//  args.push_back("--walfile="+walFilePath2);
//  args.push_back("--header");
//  //args.push_back("--print_value");
//  LDBCommand* command = rocksdb::LDBCommand::InitFromCmdLineArgs(
//          args, Options(), LDBOptions(), nullptr);
//  command->Run();
//  bool is_succeed = command->GetExecuteState().IsSucceed();
//  if(is_succeed){
//    std::cout<<"succeed!"<<std::endl;
//  }else{
//    std::cout<<command->GetExecuteState().ToString() <<std::endl;
//  }
//
//
//
//  return 0;
//}
