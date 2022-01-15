// Copyright 2019  The Cockroach Authors.
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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

func runRestart(ctx context.Context, t *test, c *cluster, downDuration time.Duration) {
	znbaseNodes := c.Range(1, c.nodes)
	workloadNode := c.Node(1)
	const restartNode = 3

	t.Status("installing znbase")
	c.Put(ctx, znbase, "./znbase", znbaseNodes)
	c.Start(ctx, t, znbaseNodes, startArgs(`--args=--vmodule=raft_log_queue=3`))

	// We don't really need tpcc, we just need a good amount of traffic and a good
	// amount of data.
	t.Status("importing tpcc fixture")
	c.Run(ctx, workloadNode,
		"./znbase workload fixtures import tpcc --warehouses=100 --fks=false --checks=false")

	// Wait a full scanner cycle (10m) for the raft log queue to truncate the
	// sstable entries from the import. They're huge and are not representative of
	// normal traffic.
	//
	// NB: less would probably do a good enough job, but let's play it safe.
	//
	// TODO(dan/tbg): It's awkward that this is necessary. We should be able to
	// do a better job here, for example by truncating only a smaller prefix of
	// the log instead of all of it (right now there's no notion of per-entry
	// size when we do truncate). Also having quiescing ranges truncate to
	// lastIndex will be helpful because that drives the log size down eagerly
	// when things are healthy.
	t.Status("waiting for addsstable truncations")
	time.Sleep(11 * time.Minute)

	// Stop a node.
	c.Stop(ctx, c.Node(restartNode))

	// Wait for between 10s and `server.time_until_store_dead` while sending
	// traffic to one of the nodes that are not down. This used to cause lots of
	// raft log truncation, which caused node 3 to need lots of snapshots when it
	// came back up.
	c.Run(ctx, workloadNode, "./znbase workload run tpcc --warehouses=100 "+
		fmt.Sprintf("--tolerate-errors --wait=false --duration=%s", downDuration))

	// Bring it back up and make sure it can serve a query within a reasonable
	// time limit. For now, less time than it was down for.
	c.Start(ctx, t, c.Node(restartNode))

	// Dialing the formerly down node may still be prevented by the circuit breaker
	// for a short moment (seconds) after n3 restarts. If it happens, the COUNT(*)
	// can fail with a "no inbound stream connection" error. This is not what we
	// want to catch in this test, so work around it.
	//
	// See
	time.Sleep(15 * time.Second)

	start := timeutil.Now()
	restartNodeDB := c.Conn(ctx, restartNode)
	if _, err := restartNodeDB.Exec(`SELECT count(*) FROM tpcc.order_line`); err != nil {
		t.Fatal(err)
	}
	if took := timeutil.Since(start); took > downDuration {
		t.Fatalf(`expected to recover within %s took %s`, downDuration, took)
	} else {
		c.l.Printf(`connecting and query finished in %s`, took)
	}
}

func registerRestart(r *registry) {
	r.Add(testSpec{
		Name:    fmt.Sprintf("restart/down-for-2m"),
		Cluster: makeClusterSpec(3),
		// "znbase workload is only in 19.1+"
		MinVersion: "v19.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runRestart(ctx, t, c, 2*time.Minute)
		},
	})
}
