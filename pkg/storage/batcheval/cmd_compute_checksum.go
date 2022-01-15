// Copyright 2014  The Cockroach Authors.
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

package batcheval

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/storage/storagepb"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

func init() {
	RegisterCommand(roachpb.ComputeChecksum, declareKeysComputeChecksum, ComputeChecksum)
}

func declareKeysComputeChecksum(
	desc *roachpb.RangeDescriptor,
	_ roachpb.Header,
	_ roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	// The correctness of range merges depends on the lease applied index of a
	// range not being bumped while the RHS is subsumed. ComputeChecksum bumps a
	// range's LAI and thus needs to be serialized with Subsume requests, in order
	// prevent a rare closed timestamp violation due to writes on the post-merged
	// range that violate a closed timestamp spuriously reported by the pre-merged
	// range. This can, in turn, lead to a serializability violation. See comment
	// at the end of Subsume() in cmd_subsume.go for details. Thus, it must
	// declare access over at least one key. We choose to declare read-only access
	// over the range descriptor key.
	rdKey := keys.RangeDescriptorKey(desc.StartKey)
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: rdKey})
}

// Version numbers for Replica checksum computation. Requests silently no-op
// unless the versions are compatible.
const (
	ReplicaChecksumVersion    = 4
	ReplicaChecksumGCInterval = time.Hour
)

// ComputeChecksum starts the process of computing a checksum on the replica at
// a particular snapshot. The checksum is later verified through a
// CollectChecksumRequest.
func ComputeChecksum(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ComputeChecksumRequest)

	if args.Version != ReplicaChecksumVersion {
		log.Infof(ctx, "incompatible ComputeChecksum versions (server: %d, requested: %d)",
			ReplicaChecksumVersion, args.Version)
		return result.Result{}, nil
	}

	reply := resp.(*roachpb.ComputeChecksumResponse)
	reply.ChecksumID = uuid.MakeV4()

	var pd result.Result
	pd.Replicated.ComputeChecksum = &storagepb.ComputeChecksum{
		ChecksumID:   reply.ChecksumID,
		SaveSnapshot: args.Snapshot,
		Mode:         args.Mode,
		Checkpoint:   args.Checkpoint,
	}
	return pd, nil
}
