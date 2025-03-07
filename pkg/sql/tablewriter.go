// Copyright 2016  The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// expressionCarrier handles visiting sub-expressions.
type expressionCarrier interface {
	// walkExprs explores all sub-expressions held by this object, if
	// any.
	walkExprs(func(desc string, index int, expr tree.TypedExpr))
}

// tableWriter handles writing kvs and forming table rows.
//
// Usage:
//   err := tw.init(txn, evalCtx)
//   // Handle err.
//   for {
//      values := ...
//      row, err := tw.row(values)
//      // Handle err.
//   }
//   err := tw.finalize()
//   // Handle err.
type tableWriter interface {
	expressionCarrier

	// init provides the tableWriter with a Txn and optional monitor to write to
	// and returns an error if it was misconfigured.
	init(*client.Txn, *tree.EvalContext) error

	// row performs a sql row modification (tableInserter performs an insert,
	// etc). It batches up writes to the init'd txn and periodically sends them.
	// The passed Datums is not used after `row` returns.
	// The traceKV parameter determines whether the individual K/V operations
	// should be logged to the context. We use a separate argument here instead
	// of a Value field on the context because Value access in context.Context
	// is rather expensive and the tableWriter interface is used on the
	// inner loop of table accesses.
	row(context.Context, tree.Datums, bool /* traceKV */, schemaexpr.PartialIndexUpdateHelper) error

	// finalize flushes out any remaining writes. It is called after all calls to
	// row.  It returns a slice of all Datums not yet returned by calls to `row`.
	// The traceKV parameter determines whether the individual K/V operations
	// should be logged to the context. See the comment above for why
	// this a separate parameter as opposed to a Value field on the context.
	finalize(ctx context.Context, params runParams, traceKV bool, triggerDesc *sqlbase.TriggerDesc) (*rowcontainer.RowContainer, error)

	// tableDesc returns the TableDescriptor for the table that the tableWriter
	// will modify.
	tableDesc() *sqlbase.ImmutableTableDescriptor

	// fkSpanCollector returns the FkSpanCollector for the tableWriter.
	fkSpanCollector() row.FkSpanCollector

	// close frees all resources held by the tableWriter.
	close(context.Context)

	// desc returns a name suitable for describing the table writer in
	// the output of EXPLAIN.
	desc() string

	// enable auto commit in call to finalize().
	enableAutoCommit()
}

type autoCommitOpt int

const (
	_                 autoCommitOpt = 0
	autoCommitEnabled autoCommitOpt = 1
)

// extendedTableWriter is a temporary interface introduced
// until all the tableWriters implement it. When that is achieved, it will be merged into
// the main tableWriter interface.
type extendedTableWriter interface {
	tableWriter

	// atBatchEnd is called at the end of each batch, just before
	// finalize/flush. It can utilize the current KV batch which is
	// still open at that point. It must not run the batch itself; that
	// task is left to tableWriter.finalize() or flushAndStartNewBatch()
	// below.
	atBatchEnd(context.Context, bool /* traceKV */) error

	// flushAndStartNewBatch is called at the end of each batch but the last.
	// This should flush the current batch.
	flushAndStartNewBatch(context.Context) error

	// curBatchSize returns an upper bound for the amount of KV work
	// needed for the current batch. This cannot reflect the actual KV
	// batch size because the actual KV batch will be constructed only
	// during the call to atBatchEnd().
	curBatchSize() int
}

var _ extendedTableWriter = (*tableUpdater)(nil)
var _ extendedTableWriter = (*tableDeleter)(nil)
var _ extendedTableWriter = (*tableInserter)(nil)

// tableWriterBase is meant to be used to factor common code between
// the other tableWriters.
type tableWriterBase struct {
	// txn is the current KV transaction.
	txn *client.Txn
	// is autoCommit turned on.
	autoCommit autoCommitOpt
	// b is the current batch.
	b *client.Batch
	// batchSize is the current batch size (when known).
	batchSize int
}

func (tb *tableWriterBase) init(txn *client.Txn) {
	tb.txn = txn
	tb.b = txn.NewBatch()
}

// flushAndStartNewBatch shares the common flushAndStartNewBatch()
// code between extendedTableWriters.
func (tb *tableWriterBase) flushAndStartNewBatch(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor,
) error {
	if err := tb.txn.Run(ctx, tb.b); err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	tb.b = tb.txn.NewBatch()
	tb.batchSize = 0
	return nil
}

// curBatchSize shares the common curBatchSize() code between extendedTableWriters().
func (tb *tableWriterBase) curBatchSize() int { return tb.batchSize }

// finalize shares the common finalize code between extendedTableWriters.
func (tb *tableWriterBase) finalize(
	ctx context.Context,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	isAfterStmt bool,
	triggerDesc *sqlbase.TriggerDesc,
) (err error) {
	if tb.autoCommit == autoCommitEnabled && !isAfterStmt {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tb.txn.CommitInBatch(ctx, tb.b)

	} else {
		err = tb.txn.Run(ctx, tb.b)
		if isAfterStmt {
			if tb.autoCommit == autoCommitEnabled {
				err = tb.txn.Commit(ctx)
			}
		}
	}

	if err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	// 写完后需要可读
	return tb.txn.Step(ctx)
}

func (tb *tableWriterBase) finalizeInsert(
	ctx context.Context,
	params runParams,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	isAfterStmt bool,
	triggerDesc *sqlbase.TriggerDesc,
) (err error) {
	if tb.autoCommit == autoCommitEnabled && !isAfterStmt {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tb.txn.CommitInBatch(ctx, tb.b)
	} else {
		err = tb.txn.Run(ctx, tb.b)
		if err != nil {
			return row.ConvertBatchError(ctx, tableDesc, tb.b)
		}
		if isAfterStmt {
			err = ExecuteTriggers(ctx, tb.txn, params, triggerDesc, nil, nil, nil, TriggerTypeInsert, TriggerTypeAfter, TriggerTypeStatement)
			if err != nil {
				return err
			}
			if tb.autoCommit == autoCommitEnabled {
				err = tb.txn.Commit(ctx)
			}
		}
	}

	if err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	// 写完后需要可读
	return tb.txn.Step(ctx)
}

func (tb *tableWriterBase) finalizeDelete(
	ctx context.Context,
	params runParams,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	isAfterStmt bool,
	triggerDesc *sqlbase.TriggerDesc,
) (err error) {
	if tb.autoCommit == autoCommitEnabled && !isAfterStmt {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tb.txn.CommitInBatch(ctx, tb.b)

	} else {
		err = tb.txn.Run(ctx, tb.b)
		if err != nil {
			return row.ConvertBatchError(ctx, tableDesc, tb.b)
		}
		if isAfterStmt {
			err = ExecuteTriggers(ctx, tb.txn, params, triggerDesc, nil, nil, nil, TriggerTypeDelete, TriggerTypeAfter, TriggerTypeStatement)
			if err != nil {
				return err
			}
			if tb.autoCommit == autoCommitEnabled {
				err = tb.txn.Commit(ctx)
			}
		}
	}

	if err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	// 写完后需要可读
	return tb.txn.Step(ctx)
}

func (tb *tableWriterBase) finalizeUpdate(
	ctx context.Context,
	params runParams,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	isAfterStmt bool,
	triggerDesc *sqlbase.TriggerDesc,
) (err error) {
	if tb.autoCommit == autoCommitEnabled && !isAfterStmt {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tb.txn.CommitInBatch(ctx, tb.b)

	} else {
		err = tb.txn.Run(ctx, tb.b)
		if err != nil {
			return row.ConvertBatchError(ctx, tableDesc, tb.b)
		}
		if isAfterStmt {
			err = ExecuteTriggers(ctx, tb.txn, params, triggerDesc, nil, nil, nil, TriggerTypeUpdate, TriggerTypeAfter, TriggerTypeStatement)
			if err != nil {
				return err
			}
			if tb.autoCommit == autoCommitEnabled {
				err = tb.txn.Commit(ctx)
			}
		}
	}

	if err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	// 写完后需要可读
	return tb.txn.Step(ctx)
}

func (tb *tableWriterBase) enableAutoCommit() {
	tb.autoCommit = autoCommitEnabled
}

// batchedTableWriter is used for tableWriters that
// do their work at the end of the current batch, currently
// used for tableUpserter.
type batchedTableWriter interface {
	extendedTableWriter

	// batchedCount returns the number of results in the current batch.
	batchedCount() int

	// batchedValues accesses one row in the current batch.
	batchedValues(rowIdx int) tree.Datums
}

var _ batchedTableWriter = (*tableUpserter)(nil)
var _ batchedTableWriter = (*fastTableUpserter)(nil)
