// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/CCL.txt

package allccl

// We import each of the workloads below, so a single import of this package
// enables registration of all workloads.

import (
	// workloads
	_ "github.com/znbasedb/znbase/pkg/icl/workloadicl/roachmarticl"
	_ "github.com/znbasedb/znbase/pkg/workload/bank"
	_ "github.com/znbasedb/znbase/pkg/workload/bulkingest"
	_ "github.com/znbasedb/znbase/pkg/workload/examples"
	_ "github.com/znbasedb/znbase/pkg/workload/indexes"
	_ "github.com/znbasedb/znbase/pkg/workload/interleavedpartitioned"
	_ "github.com/znbasedb/znbase/pkg/workload/jsonload"
	_ "github.com/znbasedb/znbase/pkg/workload/kv"
	_ "github.com/znbasedb/znbase/pkg/workload/ledger"
	_ "github.com/znbasedb/znbase/pkg/workload/querybench"
	_ "github.com/znbasedb/znbase/pkg/workload/queue"
	_ "github.com/znbasedb/znbase/pkg/workload/rand"
	_ "github.com/znbasedb/znbase/pkg/workload/sqlsmith"
	_ "github.com/znbasedb/znbase/pkg/workload/tpcc"
	_ "github.com/znbasedb/znbase/pkg/workload/tpch"
	_ "github.com/znbasedb/znbase/pkg/workload/ycsb"
)
