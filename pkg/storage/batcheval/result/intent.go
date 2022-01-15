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

package result

import (
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
)

// FromEncounteredIntents creates a Result communicating that the intents were encountered
// by the given request and should be handled.
func FromEncounteredIntents(intents []roachpb.Intent) Result {
	var pd Result
	if len(intents) == 0 {
		return pd
	}
	pd.Local.EncounteredIntents = intents
	return pd
}

// FromAcquiredLocks creates a Result communicating that the locks were
// acquired or re-acquired by the given transaction and should be handled.
func FromAcquiredLocks(txn *roachpb.Transaction, keys ...roachpb.Key) Result {
	var pd Result
	if txn == nil {
		return pd
	}
	pd.Local.AcquiredLocks = make([]roachpb.LockAcquisition, len(keys))
	for i := range pd.Local.AcquiredLocks {
		pd.Local.AcquiredLocks[i] = roachpb.MakeLockAcquisition(txn, keys[i], lock.Replicated)
	}
	return pd
}

// EndTxnIntents contains a finished transaction and a bool (Always),
// which indicates whether the intents should be resolved whether or
// not the command succeeds through Raft.
type EndTxnIntents struct {
	Txn    *roachpb.Transaction
	Always bool
	Poison bool
}

// FromEndTxn creates a Result communicating that a transaction was
// completed and its intents should be resolved.
func FromEndTxn(txn *roachpb.Transaction, alwaysReturn, poison bool) Result {
	var pd Result
	if len(txn.LockSpans) == 0 {
		return pd
	}
	pd.Local.EndTxns = []EndTxnIntents{{Txn: txn, Always: alwaysReturn, Poison: poison}}
	return pd
}
