// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"context"

	"github.com/znbasedb/znbase/pkg/col/coldata"
)

// fnOp is an operator that executes an arbitrary function for its side-effects,
// once per input batch, passing the input batch unmodified along.
type fnOp struct {
	OneInputNode
	NonExplainable

	fn func()
}

var _ resettableOperator = fnOp{}

func (f fnOp) Init() {
	f.input.Init()
}

func (f fnOp) Next(ctx context.Context) coldata.Batch {
	batch := f.input.Next(ctx)
	f.fn()
	return batch
}

func (f fnOp) reset() {}
