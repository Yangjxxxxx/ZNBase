// Copyright 2017 The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

#pragma once

#include <rocksdb/env.h>
#include <string>
#include "icl/storageicl/engineicl/enginepbicl/key_registry.pb.h"
#include "key_manager.h"

namespace enginepbicl = znbase::icl::storageicl::engineicl::enginepbicl;

namespace testutils {

// Generate the DBOptions.extra_options string for plaintext keys.
std::string MakePlaintextExtraOptions();

// MakeAES<size>Key creates a SecretKeyObject with a key of the specified size.
// It needs an Env for the current time.
enginepbicl::SecretKey* MakeAES128Key(rocksdb::Env* env);

// WriteAES<size>KeyFile writes a AES key of the specified size to 'filename'
// using 'env'.
// The resulting filename can be used in the encryption options.
rocksdb::Status WriteAES128KeyFile(rocksdb::Env* env, const std::string& filename);

// MemKeyManager is a simple key manager useful for tests.
// It holds a single key. ie: there is only an active key, no old keys.
class MemKeyManager : public KeyManager {
 public:
  explicit MemKeyManager(enginepbicl::SecretKey* key) : key_(key) {}
  virtual ~MemKeyManager();

  virtual std::shared_ptr<enginepbicl::SecretKey> CurrentKey() override;
  virtual std::shared_ptr<enginepbicl::SecretKey> GetKey(const std::string& id) override;

  // Replace the key with the passed-in one. Takes ownership.
  void set_key(enginepbicl::SecretKey* key);

 private:
  std::unique_ptr<enginepbicl::SecretKey> key_;
};

}  // namespace testutils
