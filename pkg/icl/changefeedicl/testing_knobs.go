// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import "context"

// TestingKnobs are the testing knobs for changefeed.
type TestingKnobs struct {
	// BeforeEmitRow is called before every sink emit row operation.
	BeforeEmitRow func(context.Context) error
	// AfterSinkFlush is called after a sink flush operation has returned without
	// error.
	AfterSinkFlush func() error
	// MemBufferCapacity, if non-zero, overrides memBufferDefaultCapacity.
	MemBufferCapacity int64
	// ConsecutiveIdenticalErrorBailoutCount is an override for the top-level
	// safety net in the retry loop for non-terminal errors: if we consecutively
	// receive an identical error message some number of times, we assume it
	// should have been marked as terminal but wasn't. When non-zero, this is an
	// override for how many times. When zero, we fall back to a default.
	ConsecutiveIdenticalErrorBailoutCount int
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
