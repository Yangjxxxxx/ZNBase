// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"context"

	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
)

// TODO(yuzefovich): add actual implementation.

// newExternalSorter returns a disk-backed general sort operator.
func newExternalSorter(
	allocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	orderingCols []distsqlpb.Ordering_Column,
) Operator {
	inMemSorter, err := newSorter(
		allocator, newAllSpooler(allocator, input, inputTypes),
		inputTypes, orderingCols,
	)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return &externalSorter{OneInputNode: NewOneInputNode(inMemSorter)}
}

type externalSorter struct {
	OneInputNode
}

var _ Operator = &externalSorter{}

func (s *externalSorter) Init() {
	s.input.Init()
}

func (s *externalSorter) Next(ctx context.Context) coldata.Batch {
	return s.input.Next(ctx)
}
