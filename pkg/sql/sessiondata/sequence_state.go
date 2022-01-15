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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sessiondata

import (
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
)

// SequenceState stores session-scoped state used by sequence builtins.
//
// All public methods of SequenceState are thread-safe, as the structure is
// meant to be shared by statements executing in parallel on a session.
type SequenceState struct {
	mu struct {
		syncutil.Mutex
		// latestValues stores the last value obtained by nextval() in this session
		// by descriptor id.
		latestValues map[uint32]int64

		// lastSequenceIncremented records the descriptor id of the last sequence
		// nextval() was called on in this session.
		lastSequenceIncremented uint32

		//sequenceCache store the cacheVal of the seq.
		sequenceCaches map[uint32]sequenceCache
	}
}

type sequenceCache struct {
	// cacheVals records sequence cache value in this time.
	cacheVals int64

	// cacheCount count the cacheVals in this cache.
	cacheCount int64
}

// NewSequenceState creates a SequenceState.
func NewSequenceState() *SequenceState {
	ss := SequenceState{}
	ss.mu.latestValues = make(map[uint32]int64)
	ss.mu.sequenceCaches = make(map[uint32]sequenceCache)
	return &ss
}

// NextVal ever called returns true if a sequence has ever been incremented on
// this session.
func (ss *SequenceState) nextValEverCalledLocked() bool {
	return len(ss.mu.latestValues) > 0
}

// RecordValue records the latest manipulation of a sequence done by a session.
func (ss *SequenceState) RecordValue(seqID uint32, val int64) {
	ss.mu.Lock()
	ss.mu.lastSequenceIncremented = seqID
	ss.mu.latestValues[seqID] = val
	ss.mu.Unlock()
}

// ResetCacheVals renovate the cacheVals when the value of sequence cache reach the maxval.
func (ss *SequenceState) ResetCacheVals(seqID uint32, startVal int64, cache int64) {
	ss.mu.Lock()
	tempseqcache := sequenceCache{startVal, cache}
	ss.mu.sequenceCaches[seqID] = tempseqcache
	ss.mu.Unlock()
}

// ReturnUpdateCacheVals return the cacheVals and update cacheVals.
func (ss *SequenceState) ReturnUpdateCacheVals(seqID uint32, cache int64, inc int64) int64 {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.mu.sequenceCaches[seqID].cacheCount > 0 {
		tempseqcache := ss.mu.sequenceCaches[seqID]
		tempseqcache.cacheCount--
		tempseqcache.cacheVals += inc
		ss.mu.sequenceCaches[seqID] = tempseqcache
	} else {
		ss.ResetCacheVals(seqID, ss.mu.sequenceCaches[seqID].cacheVals, cache)
	}
	return ss.mu.sequenceCaches[seqID].cacheVals
}

/*// ReturnCacheVals return the val of seqcache
func (ss *SequenceState) ReturnCacheVals(seqID uint32) int64 {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.mu.sequenceCaches[seqID].cacheVals
}*/

// ReturnCacheValsCount return the number of cacheVals left.
func (ss *SequenceState) ReturnCacheValsCount(seqID uint32) int64 {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if _, ok := ss.mu.sequenceCaches[seqID]; ok {
		return ss.mu.sequenceCaches[seqID].cacheCount
	}
	return -1
}

// SetCacheValslast set the cacheVals to the last val in this cache.
func (ss *SequenceState) SetCacheValslast(seqID uint32, inc int64, maxVal int64, minVal int64) {
	ss.mu.Lock()
	count := ss.mu.sequenceCaches[seqID].cacheCount
	tempseqcache := ss.mu.sequenceCaches[seqID]
	tempseqcache.cacheVals += count * inc
	if tempseqcache.cacheVals > maxVal || tempseqcache.cacheVals < minVal {
		for {
			if (tempseqcache.cacheVals > maxVal && inc > 0) || (tempseqcache.cacheVals < minVal && inc < 0) {
				tempseqcache.cacheVals -= inc
				count++
			} else {
				break
			}
		}
	}
	tempseqcache.cacheCount = 1
	ss.mu.sequenceCaches[seqID] = tempseqcache
	ss.mu.Unlock()
}

// RemoveLastAndCacheVal remove the cacheVal and the lastVal of the sequence when drop the sequence.
func (ss *SequenceState) RemoveLastAndCacheVal(seqID uint32) {
	ss.mu.Lock()
	if _, ok := ss.mu.sequenceCaches[seqID]; ok {
		delete(ss.mu.sequenceCaches, seqID)
		delete(ss.mu.latestValues, seqID)
	}
	ss.mu.Unlock()
}

// RemoveCacheval remove the cacheVal of the seq.
func (ss *SequenceState) RemoveCacheval(seqID uint32) {
	ss.mu.Lock()
	if _, ok := ss.mu.sequenceCaches[seqID]; ok {
		delete(ss.mu.sequenceCaches, seqID)
	}
	ss.mu.Unlock()
}

// SetLastSequenceIncremented sets the id of the last incremented sequence.
// Usually this id is set through RecordValue().
func (ss *SequenceState) SetLastSequenceIncremented(seqID uint32) {
	ss.mu.Lock()
	ss.mu.lastSequenceIncremented = seqID
	ss.mu.Unlock()
}

// GetLastValue returns the value most recently obtained by
// nextval() for the last sequence for which RecordLatestVal() was called.
func (ss *SequenceState) GetLastValue() (int64, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if !ss.nextValEverCalledLocked() {
		return 0, pgerror.NewError(
			pgcode.ObjectNotInPrerequisiteState, "lastval is not yet defined in this session")
	}
	if _, ok := ss.mu.latestValues[ss.mu.lastSequenceIncremented]; !ok {
		return 0, pgerror.NewError(
			pgcode.ObjectNotInPrerequisiteState, "lastval is not yet defined in this session")
	}
	return ss.mu.latestValues[ss.mu.lastSequenceIncremented], nil
}

// GetLastValueByID returns the value most recently obtained by nextval() for
// the given sequence in this session.
// The bool retval is false if RecordLatestVal() was never called on the
// requested sequence.
func (ss *SequenceState) GetLastValueByID(seqID uint32) (int64, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	val, ok := ss.mu.latestValues[seqID]
	return val, ok
}

// Export returns a copy of the SequenceState's state - the latestValues and
// lastSequenceIncremented.
// lastSequenceIncremented is only defined if latestValues is non-empty.
func (ss *SequenceState) Export() (map[uint32]int64, uint32) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	res := make(map[uint32]int64, len(ss.mu.latestValues))
	for k, v := range ss.mu.latestValues {
		res[k] = v
	}
	return res, ss.mu.lastSequenceIncremented
}
