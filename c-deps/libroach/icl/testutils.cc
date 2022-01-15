// Copyright 2017 The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

#include "testutils.h"
#include <gtest/gtest.h>
#include "icl/baseicl/encryption_options.pb.h"
#include "crypto_utils.h"

namespace testutils {

std::string MakePlaintextExtraOptions() {
  znbase::icl::baseicl::EncryptionOptions opts;
  opts.mutable_key_files()->set_current_key("plain");
  opts.mutable_key_files()->set_old_key("plain");

  std::string ret;
  opts.SerializeToString(&ret);
  return ret;
}

enginepbicl::SecretKey* MakeAES128Key(rocksdb::Env* env) {
  int64_t now;
  env->GetCurrentTime(&now);

  auto key = new enginepbicl::SecretKey();
  // Random key.
  key->set_key(RandomBytes(16));

  auto info = key->mutable_info();
  info->set_encryption_type(enginepbicl::AES128_CTR);
  info->set_creation_time(now);
  // Random key ID.
  info->set_key_id(HexString(RandomBytes(kKeyIDLength)));

  return key;
}

rocksdb::Status WriteAES128KeyFile(rocksdb::Env* env, const std::string& filename) {
  return rocksdb::WriteStringToFile(env, RandomBytes(16 + kKeyIDLength), filename,
                                    true /* should_sync */);
}

MemKeyManager::~MemKeyManager() {}

std::shared_ptr<enginepbicl::SecretKey> MemKeyManager::CurrentKey() {
  if (key_ != nullptr) {
    return std::shared_ptr<enginepbicl::SecretKey>(new enginepbicl::SecretKey(*key_.get()));
  }
  return nullptr;
}

std::shared_ptr<enginepbicl::SecretKey> MemKeyManager::GetKey(const std::string& id) {
  if (key_ != nullptr && key_->info().key_id() == id) {
    return std::shared_ptr<enginepbicl::SecretKey>(new enginepbicl::SecretKey(*key_.get()));
  }
  return nullptr;
}

void MemKeyManager::set_key(enginepbicl::SecretKey* key) { key_.reset(key); }

}  // namespace testutils
