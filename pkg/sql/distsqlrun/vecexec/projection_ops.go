// Copyright 2019  The Cockroach Authors.

package vecexec

// projConstOpBase contains all of the fields for binary projections with a
// constant, except for the constant itself.
type projConstOpBase struct {
	OneInputNode
	allocator *Allocator
	colIdx    int
	outputIdx int
}

// projOpBase contains all of the fields for non-constant binary projections.
type projOpBase struct {
	OneInputNode
	allocator *Allocator
	col1Idx   int
	col2Idx   int
	outputIdx int
}
