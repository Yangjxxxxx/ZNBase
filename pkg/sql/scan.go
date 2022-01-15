// Copyright 2015  The Cockroach Authors.
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
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/exec"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
)

var scanNodePool = sync.Pool{
	New: func() interface{} {
		return &scanNode{}
	},
}

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	desc  *sqlbase.ImmutableTableDescriptor
	index *sqlbase.IndexDescriptor

	// Set if an index was explicitly specified.
	specifiedIndex        *sqlbase.IndexDescriptor
	specifiedIndexReverse bool
	// Set if the NO_INDEX_JOIN hint was given.
	noIndexJoin bool

	colCfg scanColumnsConfig
	// The table columns, possibly including ones currently in schema changes.
	cols []sqlbase.ColumnDescriptor
	// There is a 1-1 correspondence between cols and resultColumns.
	resultColumns sqlbase.ResultColumns

	// For each column in resultColumns, indicates if the value is
	// needed (used as an optimization when the upper layer doesn't need
	// all values).
	// TODO(radu/knz): currently the optimization always loads the
	// entire row from KV and only skips unnecessary decodes to
	// Datum. Investigate whether performance is to be gained (e.g. for
	// tables with wide rows) by reading only certain columns from KV
	// using point lookups instead of a single range lookup for the
	// entire row.
	valNeededForCol util.FastIntSet

	// Map used to get the index for columns in cols.
	colIdxMap map[sqlbase.ColumnID]int

	// The number of backfill columns among cols. These backfill
	// columns are always the last columns within cols.
	numBackfillColumns int

	spans   []roachpb.Span
	reverse bool
	props   physicalProps

	// filter that can be evaluated using only this table/index; it contains
	// tree.IndexedVar leaves generated using filterVars.
	filter     tree.TypedExpr
	filterVars tree.IndexedVarHelper

	// origFilter is the original filtering expression, which might have gotten
	// simplified during index selection. For example "k > 0" is converted to a
	// span and the filter is nil. But we still want to deduce not-null columns
	// from the original filter.
	origFilter tree.TypedExpr

	// if non-zero, hardLimit indicates that the scanNode only needs to provide
	// this many rows (after applying any filter). It is a "hard" guarantee that
	// Next will only be called this many times.
	hardLimit int64
	// if non-zero, softLimit is an estimation that only this many rows (after
	// applying any filter) might be needed. It is a (potentially optimistic)
	// "hint". If hardLimit is set (non-zero), softLimit must be unset (zero).
	softLimit int64

	disableBatchLimits bool

	// Should be set to true if sqlbase.ParallelScans is true.
	parallelScansEnabled bool

	isSecondaryIndex bool

	// Indicates if this scanNode will do a physical data check. This is
	// only true when running SCRUB commands.
	isCheck bool

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy

	// Set when the scanNode is created via the exec factory.
	createdByOpt bool

	// maxResults, if greater than 0, is the maximum number of results that a
	// scan is guaranteed to return.
	maxResults uint64

	// Indicates if this scan is the source for a delete node.
	isDeleteSource bool

	// judge current statement if an insertstmt or a updatestmt
	IsInsertOrUpdate bool

	// estimatedRowCount 此scanNode将包含的估计行数
	// 输出。当没有统计数据来进行估计时，它将会是
	// 设置为0。
	estimatedRowCount uint64
	storeEngine       cat.DataStoreEngine

	// lockingStrength and lockingWaitPolicy represent the row-level locking
	// mode of the Scan.
	lockingStrength   sqlbase.ScanLockingStrength
	lockingWaitPolicy sqlbase.ScanLockingWaitPolicy
}

// scanVisibility represents which table columns should be included in a scan.
type scanVisibility int8

const (
	publicColumns scanVisibility = 0
	// Use this to request mutation columns that are currently being
	// backfilled. These columns are needed to correctly update/delete
	// a row by correctly constructing ColumnFamilies and Indexes.
	publicAndNonPublicColumns scanVisibility = 1
)

// Prettier aliases for distsqlpb.ScanVisibility values.
const (
	ScanVisibilityPublic             = distsqlpb.ScanVisibility_PUBLIC
	ScanVisibilityPublicAndNotPublic = distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
)

func (s scanVisibility) toDistSQLScanVisibility() distsqlpb.ScanVisibility {
	switch s {
	case publicColumns:
		return distsqlpb.ScanVisibility_PUBLIC
	case publicAndNonPublicColumns:
		return distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	default:
		panic(fmt.Sprintf("Unknown visibility %+v", s))
	}
}

// scanColumnsConfig controls the "schema" of a scan node. The zero value is the
// default: all "public" columns.
// Note that not all columns in the schema are read and decoded; that is further
// controlled by scanNode.valNeededForCol.
type scanColumnsConfig struct {
	// If set, only these columns are part of the scan node schema, in this order
	// (with the caveat that the addUnwantedAsHidden flag below can add more
	// columns). Non public columns can only be added if allowed by the visibility
	// flag below.
	// If not set, then all visible columns will be part of the scan node schema,
	// as specified by the visibility flag below. The addUnwantedAsHidden flag
	// is ignored in this case.
	wantedColumns []tree.ColumnID

	// When set, the columns that are not in the wantedColumns list are added to
	// the list of columns as hidden columns. Only useful in conjunction with
	// wantedColumns.
	addUnwantedAsHidden bool

	// If visibility is set to publicAndNonPublicColumns, then mutation columns
	// can be added to the list of columns.
	visibility scanVisibility
}

var publicColumnsCfg = scanColumnsConfig{}

func (p *planner) Scan() *scanNode {
	n := scanNodePool.Get().(*scanNode)
	return n
}

// scanNode implements tree.IndexedVarContainer.
var _ tree.IndexedVarContainer = &scanNode{}

func (n *scanNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("scanNode can't be run in local mode")
}

func (n *scanNode) IndexedVarResolvedType(idx int) types.T {
	return n.resultColumns[idx].Typ
}

// GetVisibleType implements the parser.IndexedVarContainer interface.
func (n *scanNode) GetVisibleType(idx int) string {
	return n.resultColumns[idx].VisibleTypeName
}

func (n *scanNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return (*tree.Name)(&n.resultColumns[idx].Name)
}

// SetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (n *scanNode) SetForInsertOrUpdate(b bool) {
	n.IsInsertOrUpdate = b
}

// GetForInsertOrUpdate implements the parser.IndexedVarContainer interface.
func (n *scanNode) GetForInsertOrUpdate() bool {
	return n.IsInsertOrUpdate
}

func (n *scanNode) startExec(params runParams) error {
	panic("scanNode can't be run in local mode")
}

func (n *scanNode) Close(context.Context) {
	*n = scanNode{}
	scanNodePool.Put(n)
}

func (n *scanNode) Next(params runParams) (bool, error) {
	panic("scanNode can't be run in local mode")
}

func (n *scanNode) Values() tree.Datums {
	panic("scanNode can't be run in local mode")
}

// disableBatchLimit disables the kvfetcher batch limits. Used for index-join,
// where we scan batches of unordered spans.
func (n *scanNode) disableBatchLimit() {
	n.disableBatchLimits = true
	n.hardLimit = 0
	n.softLimit = 0
}

// canParallelize returns true if this scanNode can be parallelized at the
// distSender level safely.
func (n *scanNode) canParallelize() bool {
	// We choose only to parallelize if we are certain that no more than
	// ParallelScanResultThreshold results will be returned, to prevent potential
	// memory blowup.
	// We can't parallelize if we have a non-zero limit hint, since DistSender
	// is limited to running limited batches serially.
	return n.maxResults != 0 &&
		n.maxResults < rowexec.ParallelScanResultThreshold &&
		n.limitHint() == 0 &&
		n.parallelScansEnabled
}

func (n *scanNode) limitHint() int64 {
	var limitHint int64
	if n.hardLimit != 0 {
		limitHint = n.hardLimit
		if !isFilterTrue(n.filter) {
			// The limit is hard, but it applies after the filter; read a multiple of
			// the limit to avoid needing a second batch. The multiple should be an
			// estimate for the selectivity of the filter, but we have no way of
			// calculating that right now.
			limitHint *= 2
		}
	} else {
		// Like above, read a multiple of the limit when the limit is "soft".
		limitHint = n.softLimit * 2
	}
	return limitHint
}

// Initializes a scanNode with a table descriptor.
func (n *scanNode) initTable(
	ctx context.Context,
	p *planner,
	desc *sqlbase.ImmutableTableDescriptor,
	indexFlags *tree.IndexFlags,
	colCfg scanColumnsConfig,
) error {
	n.desc = desc

	if err := p.checkPrivilegeAccessToTable(ctx, desc); err != nil {
		return err
	}

	if !p.skipSelectPrivilegeChecks {
		if err := p.CheckPrivilege(ctx, n.desc, privilege.SELECT); err != nil {
			return err
		}
	}

	if indexFlags != nil {
		if err := n.lookupSpecifiedIndex(indexFlags); err != nil {
			return err
		}
	}

	n.noIndexJoin = (indexFlags != nil && indexFlags.NoIndexJoin)
	return n.initDescDefaults(p.curPlan.deps, colCfg)
}

func (n *scanNode) lookupSpecifiedIndex(indexFlags *tree.IndexFlags) error {
	if indexFlags.Index != "" {
		// Search index by name.
		indexName := string(indexFlags.Index)
		if indexName == n.desc.PrimaryIndex.Name {
			n.specifiedIndex = &n.desc.PrimaryIndex
		} else {
			for i := range n.desc.Indexes {
				if indexName == n.desc.Indexes[i].Name {
					n.specifiedIndex = &n.desc.Indexes[i]
					break
				}
			}
		}
		if n.specifiedIndex == nil {
			return errors.Errorf("index %q not found", tree.ErrString(&indexFlags.Index))
		}
	} else if indexFlags.IndexID != 0 {
		// Search index by ID.
		if n.desc.PrimaryIndex.ID == sqlbase.IndexID(indexFlags.IndexID) {
			n.specifiedIndex = &n.desc.PrimaryIndex
		} else {
			for i := range n.desc.Indexes {
				if n.desc.Indexes[i].ID == sqlbase.IndexID(indexFlags.IndexID) {
					n.specifiedIndex = &n.desc.Indexes[i]
					break
				}
			}
		}
		if n.specifiedIndex == nil {
			return errors.Errorf("index [%d] not found", indexFlags.IndexID)
		}
	}
	if indexFlags.Direction == tree.Descending {
		n.specifiedIndexReverse = true
	}
	return nil
}

// initCols initializes n.cols and n.numBackfillColumns according to n.desc and n.colCfg.
func (n *scanNode) initCols() error {
	n.numBackfillColumns = 0

	if n.colCfg.wantedColumns == nil {
		// Add all active and maybe mutation columns.
		if n.colCfg.visibility == publicColumns {
			n.cols = n.desc.Columns
		} else {
			n.cols = n.desc.ReadableColumns
			n.numBackfillColumns = len(n.desc.ReadableColumns) - len(n.desc.Columns)
		}
		return nil
	}

	n.cols = make([]sqlbase.ColumnDescriptor, 0, len(n.desc.ReadableColumns))
	for _, wc := range n.colCfg.wantedColumns {
		var c *sqlbase.ColumnDescriptor
		var err error
		isBackfillCol := false
		if id := sqlbase.ColumnID(wc); n.colCfg.visibility == publicColumns {
			c, err = n.desc.FindActiveColumnByID(id)
		} else {
			c, isBackfillCol, err = n.desc.FindReadableColumnByID(id)
		}
		if err != nil {
			return err
		}

		n.cols = append(n.cols, *c)
		if isBackfillCol {
			n.numBackfillColumns++
		}
	}

	if n.colCfg.addUnwantedAsHidden {
		for _, c := range n.desc.Columns {
			found := false
			for _, wc := range n.colCfg.wantedColumns {
				if sqlbase.ColumnID(wc) == c.ID {
					found = true
					break
				}
			}
			if !found {
				col := c
				col.Hidden = true
				n.cols = append(n.cols, col)
			}
		}
	}

	return nil
}

// Initializes the column structures.
func (n *scanNode) initDescDefaults(planDeps planDependencies, colCfg scanColumnsConfig) error {
	n.colCfg = colCfg
	n.index = &n.desc.PrimaryIndex

	if err := n.initCols(); err != nil {
		return err
	}

	// Register the dependency to the planner, if requested.
	if planDeps != nil {
		indexID := sqlbase.IndexID(0)
		if n.specifiedIndex != nil {
			indexID = n.specifiedIndex.ID
		}
		usedColumns := make([]sqlbase.ColumnID, len(n.cols))
		for i := range n.cols {
			usedColumns[i] = n.cols[i].ID
		}
		deps := planDeps[n.desc.ID]
		deps.desc = n.desc
		deps.deps = append(deps.deps, sqlbase.TableDescriptor_Reference{
			IndexID:   indexID,
			ColumnIDs: usedColumns,
		})
		planDeps[n.desc.ID] = deps
	}

	// Set up the rest of the scanNode.
	n.resultColumns = sqlbase.ResultColumnsFromColDescs(n.cols)
	n.colIdxMap = make(map[sqlbase.ColumnID]int, len(n.cols))
	for i, c := range n.cols {
		n.colIdxMap[c.ID] = i
	}
	n.valNeededForCol = util.FastIntSet{}
	if len(n.cols) > 0 {
		n.valNeededForCol.AddRange(0, len(n.cols)-1)
	}
	n.filterVars = tree.MakeIndexedVarHelper(n, len(n.cols))
	return nil
}

// initOrdering initializes the ordering info using the selected index. This
// must be called after index selection is performed.
func (n *scanNode) initOrdering(exactPrefix int, evalCtx *tree.EvalContext) {
	if n.index == nil {
		return
	}
	n.props = n.computePhysicalProps(n.index, exactPrefix, n.reverse, evalCtx)
}

// computePhysicalProps calculates ordering information for table columns
// assuming that:
//   - we scan a given index (potentially in reverse order), and
//   - the first `exactPrefix` columns of the index each have a constant value
//     (see physicalProps).
func (n *scanNode) computePhysicalProps(
	index *sqlbase.IndexDescriptor, exactPrefix int, reverse bool, evalCtx *tree.EvalContext,
) physicalProps {
	var pp physicalProps

	columnIDs, dirs := index.FullColumnIDs()

	var keySet util.FastIntSet
	for i, colID := range columnIDs {
		idx, ok := n.colIdxMap[colID]
		if !ok {
			if int(colID) > len(n.colIdxMap) {
				for i, column := range index.ColumnIDs {
					if column == colID && index.IsFunc[i] {
						expr, err := parser.ParseExpr(index.ColumnNames[i])
						if err != nil {
							panic(fmt.Sprintf("index refers to unknown column id %d", colID))
						}
						funcexpr, ok := expr.(*tree.FuncExpr)
						if !ok {
							panic(fmt.Sprintf("index refers to unknown column id %d", colID))
						}
						for _, exprs := range funcexpr.Exprs {
							if _, ok := exprs.(*tree.UnresolvedName); !ok {
								continue
							}
							for i := 0; i < exprs.(*tree.UnresolvedName).NumParts; i++ {
								column := exprs.(*tree.UnresolvedName).Parts[i]
								for _, columns := range n.desc.TableDescriptor.Columns {
									if column == columns.Name {
										colID = columns.ID
										break
									}
								}
								idx, ok = n.colIdxMap[colID]
								if !ok {
									panic(fmt.Sprintf("index refers to unknown column id %d", colID))
								}
								if i < exactPrefix {
									pp.addConstantColumn(idx)
								} else {
									dir, err := dirs[i].ToEncodingDirection()
									if err != nil {
										panic(err)
									}
									if reverse {
										dir = dir.Reverse()
									}
									pp.addOrderColumn(idx, dir)
								}
								if !n.cols[idx].Nullable {
									pp.addNotNullColumn(idx)
								}
								keySet.Add(idx)
							}
						}
						break
					}
					if i == len(index.ColumnIDs)-1 {
						panic(fmt.Sprintf("index refers to unknown column id %d", colID))
					}
				}
			} else {
				panic(fmt.Sprintf("index refers to unknown column id %d", colID))
			}
		} else {
			if i < exactPrefix {
				pp.addConstantColumn(idx)
			} else {
				dir, err := dirs[i].ToEncodingDirection()
				if err != nil {
					panic(err)
				}
				if reverse {
					dir = dir.Reverse()
				}
				pp.addOrderColumn(idx, dir)
			}
			if !n.cols[idx].Nullable {
				pp.addNotNullColumn(idx)
			}
			keySet.Add(idx)
		}
	}

	// We included any implicit columns, so the columns form a (possibly weak)
	// key.
	pp.addWeakKey(keySet)
	pp.applyExpr(evalCtx, n.origFilter)
	return pp
}

func (n *scanNode) SetDataStoreEngine(ds cat.DataStoreEngine) {
	n.storeEngine = ds
}

func (n *scanNode) GetDataStoreEngine() cat.DataStoreEngine {
	return n.storeEngine
}

//unused
//func (n *scanNode) SetEstimatedRowCount(esCount uint64) {
//	n.estimatedRowCount = esCount
//}

// initColsForScan initializes cols according to desc and colCfg.
func initColsForScan(
	desc *ImmutableTableDescriptor, colCfg scanColumnsConfig,
) (cols []sqlbase.ColumnDescriptor, err error) {
	if colCfg.wantedColumns == nil {
		return nil, errors.Errorf("unexpectedly wantedColumns is nil")
	}

	cols = make([]sqlbase.ColumnDescriptor, 0, len(desc.ReadableColumns))
	for _, wc := range colCfg.wantedColumns {
		var c *sqlbase.ColumnDescriptor
		var err error
		// Otherwise, collect the descriptors from the table's columns.
		if id := sqlbase.ColumnID(wc); distsqlpb.ScanVisibility(colCfg.visibility) == ScanVisibilityPublic {
			c, err = desc.FindActiveColumnByID(id)
		} else {
			c, _, err = desc.FindReadableColumnByID(id)
		}
		if err != nil {
			return cols, err
		}

		cols = append(cols, *c)
	}

	if colCfg.addUnwantedAsHidden {
		for i := range desc.Columns {
			c := &desc.Columns[i]
			found := false
			for _, wc := range colCfg.wantedColumns {
				if sqlbase.ColumnID(wc) == c.ID {
					found = true
					break
				}
			}
			if !found {
				// NB: we could amortize this allocation using a second slice,
				// but addUnwantedAsHidden is only used by scrub, so doing so
				// doesn't seem worth it.
				col := *c
				col.Hidden = true
				cols = append(cols, col)
			}
		}
	}

	return cols, nil
}

func (cfg scanColumnsConfig) assertValidReqOrdering(reqOrdering exec.OutputOrdering) error {
	for i := range reqOrdering {
		if reqOrdering[i].ColIdx >= len(cfg.wantedColumns) {
			return errors.Errorf("invalid reqOrdering: %v", reqOrdering)
		}
	}
	return nil
}
