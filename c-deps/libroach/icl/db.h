// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

#pragma once

#include <libroach.h>
#include <rocksdb/env.h>
#include "../db.h"

namespace znbase {

struct EnvManager;

DBOpenHook DBOpenHookICL;

}  // namespace znbase
