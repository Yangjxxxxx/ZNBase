// Copyright 2019  The Cockroach Authors.

package runbase

import (
	"context"
	"math"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/mon"
)

// StaticNodeID is the default Node ID to be used in tests.
const StaticNodeID = roachpb.NodeID(3)

// RepeatableRowSource is a RowSource used in benchmarks to avoid having to
// reinitialize a new RowSource every time during multiple passes of the input.
// It is intended to be initialized with all rows.
type RepeatableRowSource struct {
	// The index of the next row to emit.
	nextRowIdx int
	rows       sqlbase.EncDatumRows
	// Schema of rows.
	types []types.T
}

var _ RowSource = &RepeatableRowSource{}

// NewRepeatableRowSource creates a RepeatableRowSource with the given schema
// and rows. types is optional if at least one row is provided.
func NewRepeatableRowSource(types []types.T, rows sqlbase.EncDatumRows) *RepeatableRowSource {
	if types == nil {
		panic("types required")
	}
	return &RepeatableRowSource{rows: rows, types: types}
}

// OutputTypes is part of the RowSource interface.
func (r *RepeatableRowSource) OutputTypes() []sqlbase.ColumnType {
	cts, _ := sqlbase.DatumTypesToColumnTypes(r.types)
	return cts
}

//GetRows return r.rows
func (r *RepeatableRowSource) GetRows() sqlbase.EncDatumRows {
	return r.rows
}

// Start is part of the RowSource interface.
func (r *RepeatableRowSource) Start(ctx context.Context) context.Context { return ctx }

// Next is part of the RowSource interface.
func (r *RepeatableRowSource) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	// If we've emitted all rows, signal that we have reached the end.
	if r.nextRowIdx >= len(r.rows) {
		return nil, nil
	}
	nextRow := r.rows[r.nextRowIdx]
	r.nextRowIdx++
	return nextRow, nil
}

// Reset resets the RepeatableRowSource such that a subsequent call to Next()
// returns the first row.
func (r *RepeatableRowSource) Reset() {
	r.nextRowIdx = 0
}

// ConsumerDone is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerDone() {}

// ConsumerClosed is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerClosed() {}

//GetBatch is part of the RowSource interface.
func (r *RepeatableRowSource) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (r *RepeatableRowSource) SetChan() {}

// NewTestMemMonitor creates and starts a new memory monitor to be used in
// tests.
// TODO(yuzefovich): consider reusing this in tree.MakeTestingEvalContext
// (currently it would create an import cycle, so this code will need to be
// moved).
func NewTestMemMonitor(ctx context.Context, st *cluster.Settings) *mon.BytesMonitor {
	memMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	memMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	return &memMonitor
}

// NewTestDiskMonitor creates and starts a new disk monitor to be used in
// tests.
func NewTestDiskMonitor(ctx context.Context, st *cluster.Settings) *mon.BytesMonitor {
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	return &diskMonitor
}

// GenerateValuesSpec generates a ValuesCoreSpec that encodes the given rows.
// We pass the types as well because zero rows are allowed.
func GenerateValuesSpec(
	colTypes []types.T, rows sqlbase.EncDatumRows, rowsPerChunk int,
) (distsqlpb.ValuesCoreSpec, error) {
	var spec distsqlpb.ValuesCoreSpec
	spec.Columns = make([]distsqlpb.DatumInfo, len(colTypes))
	for i := range spec.Columns {
		ct, _ := sqlbase.DatumTypeToColumnType(colTypes[i])
		spec.Columns[i].Type = ct
		spec.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
	}

	var a sqlbase.DatumAlloc
	for i := 0; i < len(rows); {
		var buf []byte
		for end := i + rowsPerChunk; i < len(rows) && i < end; i++ {
			for j, info := range spec.Columns {
				var err error
				ct, _ := sqlbase.DatumTypeToColumnType(colTypes[j])
				buf, err = rows[i][j].Encode(&ct, &a, info.Encoding, buf)
				if err != nil {
					return distsqlpb.ValuesCoreSpec{}, err
				}
			}
		}
		spec.RawBytes = append(spec.RawBytes, buf)
	}
	return spec, nil
}

// RowDisposer is a RowReceiver that discards any rows Push()ed.
type RowDisposer struct{}

var _ RowReceiver = &RowDisposer{}

// Push is part of the distsql.RowReceiver interface.
func (r *RowDisposer) Push(
	row sqlbase.EncDatumRow, meta *distsqlpb.ProducerMetadata,
) ConsumerStatus {
	return NeedMoreRows
}

// ProducerDone is part of the RowReceiver interface.
func (r *RowDisposer) ProducerDone() {}

// Types is part of the RowReceiver interface.
func (r *RowDisposer) Types() []sqlbase.ColumnType {
	return nil
}
