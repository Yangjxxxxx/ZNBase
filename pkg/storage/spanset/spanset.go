// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package spanset

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// SpanAccess records the intended mode of access in SpanSet.
type SpanAccess int

// Constants for SpanAccess. Higher-valued accesses imply lower-level ones.
const (
	SpanReadOnly SpanAccess = iota
	SpanReadWrite
	NumSpanAccess
)

// String returns a string representation of the SpanAccess.
func (a SpanAccess) String() string {
	switch a {
	case SpanReadOnly:
		return "read"
	case SpanReadWrite:
		return "write"
	default:
		panic("unreachable")
	}
}

// SpanScope divides access types into local and global keys.
type SpanScope int

// Constants for span scopes.
const (
	SpanGlobal SpanScope = iota
	SpanLocal
	NumSpanScope
)

// String returns a string representation of the SpanScope.
func (a SpanScope) String() string {
	switch a {
	case SpanGlobal:
		return "global"
	case SpanLocal:
		return "local"
	default:
		panic("unreachable")
	}
}

// Span is used to represent a keyspan accessed by a request at a given
// timestamp. A zero timestamp indicates it's a non-MVCC access.
type Span struct {
	roachpb.Span
	Timestamp hlc.Timestamp
}

// SpanSet tracks the set of key spans touched by a command. The set
// is divided into subsets for access type (read-only or read/write)
// and key scope (local or global; used to facilitate use by the
// separate local and global latches).
type SpanSet struct {
	spans [NumSpanAccess][NumSpanScope][]Span
}

// String prints a string representation of the span set.
func (ss *SpanSet) String() string {
	var buf strings.Builder
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for j := SpanScope(0); j < NumSpanScope; j++ {
			for _, span := range ss.GetSpans(i, j) {
				fmt.Fprintf(&buf, "%s %s: %s\n", i, j, span)
			}
		}
	}
	return buf.String()
}

// Len returns the total number of spans tracked across all accesses and scopes.
func (ss *SpanSet) Len() int {
	var count int
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for j := SpanScope(0); j < NumSpanScope; j++ {
			count += len(ss.GetSpans(i, j))
		}
	}
	return count
}

// Empty returns whether the set contains any spans across all accesses and scopes.
func (ss *SpanSet) Empty() bool {
	return ss.Len() == 0
}

// Reserve space for N additional keys.
func (ss *SpanSet) Reserve(access SpanAccess, scope SpanScope, n int) {
	existing := ss.spans[access][scope]
	ss.spans[access][scope] = make([]Span, len(existing), n+cap(existing))
	copy(ss.spans[access][scope], existing)
}

// AddNonMVCC adds a non-MVCC span to the span set. This should typically
// local keys.
func (ss *SpanSet) AddNonMVCC(access SpanAccess, span roachpb.Span) {
	ss.AddMVCC(access, span, hlc.Timestamp{})
}

// AddMVCC adds an MVCC span to the span set to be accessed at the given
// timestamp. This should typically be used for MVCC keys, user keys for e.g.
func (ss *SpanSet) AddMVCC(access SpanAccess, span roachpb.Span, timestamp hlc.Timestamp) {
	scope := SpanGlobal
	if keys.IsLocal(span.Key) {
		scope = SpanLocal
		timestamp = hlc.Timestamp{}
	}

	ss.spans[access][scope] = append(ss.spans[access][scope], Span{Span: span, Timestamp: timestamp})
}

// SortAndDedup sorts the spans in the SpanSet and removes any duplicates.
func (ss *SpanSet) SortAndDedup() {
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for j := SpanScope(0); j < NumSpanScope; j++ {
			ss.spans[i][j], _ /* distinct */ = mergeSpans(ss.spans[i][j])
		}
	}
}

// GetSpans returns a slice of spans with the given parameters.
func (ss *SpanSet) GetSpans(access SpanAccess, scope SpanScope) []Span {
	return ss.spans[access][scope]
}

// BoundarySpan returns a span containing all the spans with the given params.
func (ss *SpanSet) BoundarySpan(scope SpanScope) roachpb.Span {
	var boundary roachpb.Span
	for i := SpanAccess(0); i < NumSpanAccess; i++ {
		for _, span := range ss.spans[i][scope] {
			if !boundary.Valid() {
				boundary = span.Span
				continue
			}
			boundary = boundary.Combine(span.Span)
		}
	}
	return boundary
}

// MaxProtectedTimestamp returns the maximum timestamp that is protected across
// all MVCC spans in the SpanSet. ReadWrite spans are protected from their
// declared timestamp forward, so they have no maximum protect timestamp.
// However, ReadOnly are protected only up to their declared timestamp and
// are not protected at later timestamps.
func (ss *SpanSet) MaxProtectedTimestamp() hlc.Timestamp {
	maxTS := hlc.MaxTimestamp
	for s := SpanScope(0); s < NumSpanScope; s++ {
		for _, cur := range ss.GetSpans(SpanReadOnly, s) {
			curTS := cur.Timestamp
			if !curTS.IsEmpty() {
				maxTS.Backward(curTS)
			}
		}
	}
	return maxTS
}

// AssertAllowed calls checkAllowed and fatals if the access is not allowed.
func (ss *SpanSet) AssertAllowed(access SpanAccess, span roachpb.Span) {
	if err := ss.CheckAllowed(access, span); err != nil {
		log.Fatal(context.TODO(), err)
	}
}

// CheckAllowed returns an error if the access is not allowed.
func (ss *SpanSet) CheckAllowed(access SpanAccess, span roachpb.Span) error {
	scope := SpanGlobal
	if keys.IsLocal(span.Key) {
		scope = SpanLocal
	}
	for ac := access; ac < NumSpanAccess; ac++ {
		for _, s := range ss.spans[ac][scope] {
			if s.Contains(span) {
				return nil
			}
		}
	}

	return errors.Errorf("cannot %s undeclared span %s\ndeclared:\n%s", access, span, ss)
}

// Validate returns an error if any spans that have been added to the set
// are invalid.
func (ss *SpanSet) Validate() error {
	for _, accessSpans := range ss.spans {
		for _, spans := range accessSpans {
			for _, span := range spans {
				if len(span.EndKey) > 0 && span.Key.Compare(span.EndKey) >= 0 {
					return errors.Errorf("inverted span %s %s", span.Key, span.EndKey)
				}
			}
		}
	}
	return nil
}
