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

// tableUpdater handles writing kvs and forming table rows for updates.
type tableUpdater struct {
	tableWriterBase
	ru row.Updater

	isAfter bool
}

// desc is part of the tableWriter interface.
func (*tableUpdater) desc() string { return "updater" }

// init is part of the tableWriter interface.
func (tu *tableUpdater) init(txn *client.Txn, _ *tree.EvalContext) error {
	tu.tableWriterBase.init(txn)
	return nil
}

// row is part of the tableWriter interface.
// We don't implement this because tu.ru.UpdateRow wants two slices
// and it would be a shame to split the incoming slice on every call.
// Instead provide a separate rowForUpdate() below.
func (tu *tableUpdater) row(
	context.Context, tree.Datums, bool, schemaexpr.PartialIndexUpdateHelper,
) error {
	panic("unimplemented")
}

// rowForUpdate extends row() from the tableWriter interface.
func (tu *tableUpdater) rowForUpdate(
	ctx context.Context,
	oldValues, updateValues tree.Datums,
	pm schemaexpr.PartialIndexUpdateHelper,
	traceKV bool,
) (tree.Datums, error) {
	tu.batchSize++
	return tu.ru.UpdateRow(ctx, tu.b, oldValues, updateValues, row.CheckFKs, traceKV, pm)
}

// atBatchEnd is part of the extendedTableWriter interface.
func (tu *tableUpdater) atBatchEnd(_ context.Context, _ bool) error { return nil }

// flushAndStartNewBatch is part of the extendedTableWriter interface.
func (tu *tableUpdater) flushAndStartNewBatch(ctx context.Context) error {
	return tu.tableWriterBase.flushAndStartNewBatch(ctx, tu.tableDesc())
}

// finalize is part of the tableWriter interface.
func (tu *tableUpdater) finalize(
	ctx context.Context, params runParams, _ bool, triggerDesc *sqlbase.TriggerDesc,
) (*rowcontainer.RowContainer, error) {
	return nil, tu.tableWriterBase.finalizeUpdate(ctx, params, tu.tableDesc(), tu.isAfter, triggerDesc)
}

// tableDesc is part of the tableWriter interface.
func (tu *tableUpdater) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return tu.ru.Helper.TableDesc
}

// fkSpanCollector is part of the tableWriter interface.
func (tu *tableUpdater) fkSpanCollector() row.FkSpanCollector {
	return tu.ru.Fks
}

// close is part of the tableWriter interface.
func (tu *tableUpdater) close(_ context.Context) {}

// walkExprs is part of the tableWriter interface.
func (tu *tableUpdater) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
