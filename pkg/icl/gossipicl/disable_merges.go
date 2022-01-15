// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package gossipicl

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/util/log"
)

const (
	disableMergesInterval = 10 * time.Second
)

// DisableMerges starts a goroutine which periodically gossips keys that
// disable merging for the specified table IDs. The goroutine until the
// associated context is done (usually via cancellation).
func DisableMerges(ctx context.Context, g *gossip.Gossip, tableIDs []uint32) {
	if len(tableIDs) == 0 {
		// Nothing to do.
		return
	}

	disable := func() {
		for _, id := range tableIDs {
			key := gossip.MakeTableDisableMergesKey(id)
			err := g.AddInfo(key, nil /* value */, disableMergesInterval*2 /* ttl */)
			if err != nil {
				log.Infof(ctx, "failed to gossip: %s: %v", key, err)
			}
		}
	}

	// Disable merging synchronously before we start the periodic loop below.
	disable()

	s := g.Stopper()
	// We don't care if this task can't be started as that only occurs if the
	// stopper is stopping.
	_ = s.RunAsyncTask(ctx, "disable-merges", func(ctx context.Context) {
		ticker := time.NewTicker(disableMergesInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				disable()
			case <-ctx.Done():
				return
			case <-s.ShouldQuiesce():
				return
			}
		}
	})
}
