// Copyright 2020 The Bidb Authors.
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

package row

import (
	"context"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/transform"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// KVBatch represents a batch of KVs generated from converted rows.
type KVBatch struct {
	// LastRow is the index of the last converted row in source in this batch.
	LastRow int64
	// Progress represents the fraction of the input that generated this row.
	Progress float32
	// KVs is the actual converted KV data.
	KVs []roachpb.KeyValue
}

// DatumRowConverter converts Datums into kvs and streams it to the destination
// channel.
type DatumRowConverter struct {
	// current row buf
	Datums []tree.Datum

	// kv destination and current batch
	KvCh     chan<- KVBatch
	KvBatch  KVBatch
	BatchCap int

	tableDesc *sqlbase.ImmutableTableDescriptor

	// Tracks which column indices in the set of visible columns are part of the
	// user specified target columns. This can be used before populating Datums
	// to filter out unwanted column data.
	IsTargetCol map[int]struct{}

	// The rest of these are derived from tableDesc, just cached here.
	ri                    Inserter
	EvalCtx               *tree.EvalContext
	cols                  []sqlbase.ColumnDescriptor
	VisibleCols           []sqlbase.ColumnDescriptor
	VisibleColTypes       []types.T
	computedExprs         []tree.TypedExpr
	defaultCache          []tree.TypedExpr
	computedIVarContainer sqlbase.RowIndexedVarContainer

	// FractionFn is used to set the progress header in KVBatches.
	CompletedRowFn func() int64
	FractionFn     func() float32
}

var kvDatumRowConverterBatchSize = 5000

const rowIDBits = 64 - builtins.NodeIDBits

// NewDatumRowConverter returns an instance of a DatumRowConverter.
func NewDatumRowConverter(
	ctx context.Context,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	evalCtx *tree.EvalContext,
	kvCh chan<- KVBatch,
) (*DatumRowConverter, error) {
	c := &DatumRowConverter{
		tableDesc: tableDesc,
		KvCh:      kvCh,
		EvalCtx:   evalCtx.Copy(),
	}

	var targetColDescriptors []sqlbase.ColumnDescriptor
	var err error
	// IMPORT INTO allows specifying target columns which could be a subset of
	// immutDesc.VisibleColumns. If no target columns are specified we assume all
	// columns of the table descriptor are to be inserted into.
	targetColDescriptors = tableDesc.VisibleColumns()

	isTargetColID := make(map[sqlbase.ColumnID]struct{})
	for _, col := range targetColDescriptors {
		isTargetColID[col.ID] = struct{}{}
	}

	c.IsTargetCol = make(map[int]struct{})
	for i, col := range targetColDescriptors {
		if _, ok := isTargetColID[col.ID]; !ok {
			continue
		}
		c.IsTargetCol[i] = struct{}{}
	}

	var txCtx transform.ExprTransformContext
	relevantColumns := func(col *sqlbase.ColumnDescriptor) bool {
		return col.HasDefault() || col.IsComputed()
	}
	cols := ProcessColumnSet(
		targetColDescriptors, tableDesc, relevantColumns)
	defaultExprs, err := sqlbase.MakeDefaultExprs(cols, &txCtx, c.EvalCtx)
	if err != nil {
		return nil, errors.Wrap(err, "process default and computed columns")
	}

	ri, err := MakeInserter(
		ctx,
		nil, /* txn */
		nil,
		tableDesc,
		nil,
		cols,
		SkipFKs,
		&sqlbase.DatumAlloc{},
	)
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}

	c.ri = ri
	c.cols = cols

	c.VisibleCols = targetColDescriptors
	c.VisibleColTypes = make([]types.T, len(c.VisibleCols))
	for i := range c.VisibleCols {
		c.VisibleColTypes[i] = c.VisibleCols[i].DatumType()
	}

	c.Datums = make([]tree.Datum, len(targetColDescriptors), len(cols))
	c.defaultCache = make([]tree.TypedExpr, len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	// In addition, check for non-targeted columns with non-null DEFAULT expressions.
	// If the DEFAULT expression is immutable, we can store it in the cache so that it
	// doesn't have to be reevaluated for every row.
	isTargetCol := func(col *sqlbase.ColumnDescriptor) bool {
		_, ok := isTargetColID[col.ID]
		return ok
	}

	for i := range cols {
		col := &cols[i]
		if col.DefaultExpr != nil {

			c.defaultCache[i] = defaultExprs[i]
			if !isTargetCol(col) {
				c.Datums = append(c.Datums, nil)
			}
		}
		if col.IsComputed() && !isTargetCol(col) {
			c.Datums = append(c.Datums, nil)
		}
	}
	if len(c.Datums) != len(cols) {
		return nil, errors.New("unexpected hidden column")
	}

	padding := 2 * (len(tableDesc.Indexes) + len(tableDesc.Families))
	c.BatchCap = kvDatumRowConverterBatchSize + padding
	c.KvBatch.KVs = make([]roachpb.KeyValue, 0, c.BatchCap)

	colsOrdered := make([]sqlbase.ColumnDescriptor, len(c.tableDesc.Columns))
	for _, col := range c.tableDesc.Columns {
		// We prefer to have the order of columns that will be sent into
		// MakeComputedExprs to map that of Datums.
		colsOrdered[ri.InsertColIDtoRowIndex[col.ID]] = col
	}
	// Here, computeExprs will be nil if there's no computed column, or
	// the list of computed expressions (including nil, for those columns
	// that are not computed) otherwise, according to colsOrdered.
	c.computedExprs, err = sqlbase.MakeComputedExprs(
		colsOrdered,
		c.tableDesc,
		tree.NewUnqualifiedTableName(tree.Name(c.tableDesc.Name)),
		&txCtx,
		c.EvalCtx,
		true)
	if err != nil {
		return nil, errors.Wrapf(err, "error evaluating computed expression for IMPORT INTO")
	}

	c.computedIVarContainer = sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    tableDesc.Columns,
	}
	return c, nil
}

// ProcessColumnSet returns columns in cols, and other writable
// columns in tableDesc that fulfills a given criteria in inSet.
func ProcessColumnSet(
	cols []sqlbase.ColumnDescriptor,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	inSet func(*sqlbase.ColumnDescriptor) bool,
) []sqlbase.ColumnDescriptor {
	colIDSet := make(map[sqlbase.ColumnID]struct{}, len(cols))
	for i := range cols {
		colIDSet[cols[i].ID] = struct{}{}
	}

	// Add all public or columns in DELETE_AND_WRITE_ONLY state
	// that satisfy the condition.
	writable := tableDesc.WritableColumns()
	for i := range writable {
		col := &writable[i]
		if inSet(col) {
			if _, ok := colIDSet[col.ID]; !ok {
				colIDSet[col.ID] = struct{}{}
				cols = append(cols, *col)
			}
		}
	}
	return cols
}

// Row inserts kv operations into the current kv batch, and triggers a SendBatch
// if necessary.
func (c *DatumRowConverter) Row(ctx context.Context, sourceID int32, rowIndex int64) error {
	isTargetCol := func(i int) bool {
		_, ok := c.IsTargetCol[i]
		return ok
	}
	for i := range c.cols {
		col := &c.cols[i]
		if col.DefaultExpr != nil {
			// The importUniqueRowID function is to calculate the rowid
			// in terms of sourceID and rowIndex
			datum, err := importUniqueRowID(sourceID, rowIndex)

			if !isTargetCol(i) {
				if err != nil {
					return errors.Wrapf(
						err, "error evaluating default expression %q", *col.DefaultExpr)
				}
				c.Datums[i] = datum
			}
		}
	}

	var computedColsLookup []sqlbase.ColumnDescriptor
	if len(c.computedExprs) > 0 {
		computedColsLookup = c.tableDesc.Columns
	}

	insertRow, err := GenerateInsertRow(
		c.defaultCache, c.computedExprs, c.cols, computedColsLookup, c.EvalCtx,
		c.tableDesc, c.Datums, &c.computedIVarContainer)
	if err != nil {
		return errors.Wrap(err, "generate insert row")
	}
	// TODO(mgartner): Add partial index IDs to ignoreIndexes that we should
	// not delete entries from.
	var exprstrings []string

	if c.ri.Helper.Indexes != nil {
		for _, index := range c.ri.Helper.TableDesc.Indexes {
			if index.IsFunc != nil {
				for i, isfunc := range index.IsFunc {
					if isfunc {
						exprstrings = append(exprstrings, index.ColumnNames[i])
					}
				}
			}
		}
	}

	if exprstrings != nil {
		var txCtx transform.ExprTransformContext
		typeexprs, err := sqlbase.MakeFuncExprs(exprstrings, c.ri.Helper.TableDesc, tree.NewUnqualifiedTableName(tree.Name(c.ri.Helper.TableDesc.Name)), &txCtx, c.EvalCtx, true /* addingCols */)
		if err != nil {
			return err
		}
		for _, typeexpr := range typeexprs {
			for i := 0; i < len(typeexpr.(*tree.FuncExpr).Exprs); i++ {
				if _, ok := typeexpr.(*tree.FuncExpr).Exprs[i].(*tree.IndexedVar); !ok {
					continue
				}
				typeexpr.(*tree.FuncExpr).Exprs[i] = insertRow[typeexpr.(*tree.FuncExpr).Exprs[i].(*tree.IndexedVar).Idx]
			}
			newrow, err := typeexpr.Eval(c.EvalCtx)
			if err != nil {
				return err
			}
			insertRow = append(insertRow, newrow)
		}
	}
	var pm schemaexpr.PartialIndexUpdateHelper
	if err := c.ri.InsertRow(
		ctx,
		KVInserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.KvBatch.KVs = append(c.KvBatch.KVs, kv)
		}),
		insertRow,
		true, /* ignoreConflicts */
		CheckFKs,
		false, /* traceKV */
		pm,
	); err != nil {
		return errors.Wrap(err, "insert row")
	}
	// If our batch is full, flush it and start a new one.
	if len(c.KvBatch.KVs) >= kvDatumRowConverterBatchSize {
		if err := c.SendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SendBatch streams kv operations from the current KvBatch to the destination
// channel, and resets the KvBatch to empty.
func (c *DatumRowConverter) SendBatch(ctx context.Context) error {
	if len(c.KvBatch.KVs) == 0 {
		return nil
	}
	if c.FractionFn != nil {
		c.KvBatch.Progress = c.FractionFn()
	}
	if c.CompletedRowFn != nil {
		c.KvBatch.LastRow = c.CompletedRowFn()
	}
	select {
	case c.KvCh <- c.KvBatch:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.KvBatch.KVs = make([]roachpb.KeyValue, 0, c.BatchCap)
	return nil
}

// KVInserter implements the putter interface.
type KVInserter func(roachpb.KeyValue)

// CPut is not implmented.
func (i KVInserter) CPut(key, value, expValue interface{}) {
	panic("unimplemented")
}

// Del is not implemented.
func (i KVInserter) Del(key ...interface{}) {
	// This is called when there are multiple column families to ensure that
	// existing data is cleared. With the exception of IMPORT INTO, the entire
	// existing keyspace in any IMPORT is guaranteed to be empty, so we don't have
	// to worry about it.
	//
	// IMPORT INTO disallows overwriting an existing row, so we're also okay here.
	// The reason this works is that row existence is precisely defined as whether
	// column family 0 exists, meaning that we write column family 0 even if all
	// the non-pk columns in it are NULL. It follows that either the row does
	// exist and the imported column family 0 will conflict (and the IMPORT INTO
	// will fail) or the row does not exist (and thus the column families are all
	// empty).
}

// Put method of the putter interface.
func (i KVInserter) Put(key, value interface{}) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// InitPut method of the putter interface.
func (i KVInserter) InitPut(key, value interface{}, failOnTombstones bool) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// GenerateInsertRow prepares a row tuple for insertion. It fills in default
// expressions, verifies non-nullable columns, and checks column widths.
//
// The result is a row tuple providing values for every column in insertCols.
// This results contains:
//
// - the values provided by rowVals, the tuple of source values. The
//   caller ensures this provides values 1-to-1 to the prefix of
//   insertCols that was specified explicitly in the INSERT statement.
// - the default values for any additional columns in insertCols that
//   have default values in defaultExprs.
// - the computed values for any additional columns in insertCols
//   that are computed. The mapping in rowContainerForComputedCols
//   maps the indexes of the comptuedCols/computeExpr slices
//   back into indexes in the result row tuple.
//
func GenerateInsertRow(
	defaultExprs []tree.TypedExpr,
	computeExprs []tree.TypedExpr,
	insertCols []sqlbase.ColumnDescriptor,
	computedColsLookup []sqlbase.ColumnDescriptor,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	rowVals tree.Datums,
	rowContainerForComputedVals *sqlbase.RowIndexedVarContainer,
) (tree.Datums, error) {
	// The values for the row may be shorter than the number of columns being
	// inserted into. Generate default values for those columns using the
	// default expressions. This will not happen if the row tuple was produced
	// by a ValuesClause, because all default expressions will have been populated
	// already by fillDefaults.
	if len(rowVals) < len(insertCols) {
		// It's not cool to append to the slice returned by a node; make a copy.
		oldVals := rowVals
		rowVals = make(tree.Datums, len(insertCols))
		copy(rowVals, oldVals)

		for i := len(oldVals); i < len(insertCols); i++ {
			if defaultExprs == nil {
				rowVals[i] = tree.DNull
				continue
			}
			d, err := defaultExprs[i].Eval(evalCtx)
			if err != nil {
				return nil, err
			}
			rowVals[i] = d
		}
	}

	// Generate the computed values, if needed.
	if len(computeExprs) > 0 {
		rowContainerForComputedVals.CurSourceRow = rowVals
		evalCtx.PushIVarContainer(rowContainerForComputedVals)
		for i := range computedColsLookup {
			// Note that even though the row is not fully constructed at this point,
			// since we disallow computed columns from referencing other computed
			// columns, all the columns which could possibly be referenced *are*
			// available.
			col := computedColsLookup[i]
			computeIdx := rowContainerForComputedVals.Mapping[col.ID]
			if !col.IsComputed() {
				continue
			}
			d, err := computeExprs[computeIdx].Eval(evalCtx)
			if err != nil {
				return nil, errors.Wrapf(err,
					"computed column %s",
					tree.ErrString((*tree.Name)(&col.Name)))
			}
			rowVals[computeIdx] = d
		}
		evalCtx.PopIVarContainer()
	}

	// Verify the column constraints.
	//
	// We would really like to use enforceLocalColumnConstraints() here,
	// but this is not possible because of some brain damage in the
	// Insert() constructor, which causes insertCols to contain
	// duplicate columns descriptors: computed columns are listed twice,
	// one will receive a NULL value and one will receive a comptued
	// value during execution. It "works out in the end" because the
	// latter (non-NULL) value overwrites the earlier, but
	// enforceLocalColumnConstraints() does not know how to reason about
	// this.
	//
	// In the end it does not matter much, this code is going away in
	// favor of the (simpler, correct) code in the CBO.

	// Check to see if NULL is being inserted into any non-nullable column.
	for _, col := range tableDesc.WritableColumns() {
		if !col.Nullable {
			if i, ok := rowContainerForComputedVals.Mapping[col.ID]; !ok || rowVals[i] == tree.DNull {
				return nil, sqlbase.NewNonNullViolationError(col.Name)
			}
		}
	}

	// Ensure that the values honor the specified column widths.
	for i := 0; i < len(insertCols); i++ {
		outVal, err := sqlbase.LimitValueWidth(insertCols[i].Type, rowVals[i], &insertCols[i].Name, false)
		if err != nil {
			return nil, err
		}
		rowVals[i] = outVal
	}

	return rowVals, nil
}

// We don't want to call unique_rowid() for columns with such default expressions
// because it is not idempotent and has unfortunate overlapping of output
// spans since it puts the uniqueness-ensuring per-generator part (nodeID)
// in the low-bits. Instead, make our own IDs that attempt to keep each
// generator (sourceID) writing to its own key-space with sequential
// rowIndexes mapping to sequential unique IDs. This is done by putting the
// following as the lower bits, in order to handle the case where there are
// multiple columns with default as `unique_rowid`:
//
// #default_rowid_cols * rowIndex + colPosition (among those with default unique_rowid)
//
// To avoid collisions with the SQL-genenerated IDs (at least for a
// very long time) we also flip the top bit to 1.
//
// Producing sequential keys in non-overlapping spans for each source yields
// observed improvements in ingestion performance of ~2-3x and even more
// significant reductions in required compactions during IMPORT.
//
// TODO(dt): Note that currently some callers (e.g. CSV IMPORT, which can be
// used on a table more than once) offset their rowIndex by a wall-time at
// which their overall job is run, so that subsequent ingestion jobs pick
// different row IDs for the i'th row and don't collide. However such
// time-offset rowIDs mean each row imported consumes some unit of time that
// must then elapse before the next IMPORT could run without colliding e.g.
// a 100m row file would use 10µs/row or ~17min worth of IDs. For now it is
// likely that IMPORT's write-rate is still the limiting factor, but this
// scheme means rowIndexes are very large (1 yr in 10s of µs is about 2^42).
// Finding an alternative scheme for avoiding collisions (like sourceID *
// fileIndex*desc.Version) could improve on this. For now, if this
// best-effort collision avoidance scheme doesn't work in some cases we can
// just recommend an explicit PK as a workaround.
//
// TODO(anzoteh96): As per the issue in #51004, having too many columns with
// default expression unique_rowid() could cause collisions when IMPORTs are run
// too close to each other. It will therefore be nice to fix this problem.
func importUniqueRowID(sourceID int32, rowIndex int64) (tree.Datum, error) {
	var uniqueRowIDTotal, uniqueRowIDInstance int
	uniqueRowIDTotal++
	avoidCollisionsWithSQLsIDs := uint64(1 << 63)
	shiftedIndex := int64(uniqueRowIDTotal)*rowIndex + int64(uniqueRowIDInstance)
	returnIndex := (uint64(sourceID) << rowIDBits) ^ uint64(shiftedIndex)
	uniqueRowIDInstance++
	return tree.NewDInt(tree.DInt(avoidCollisionsWithSQLsIDs | returnIndex)), nil
}
