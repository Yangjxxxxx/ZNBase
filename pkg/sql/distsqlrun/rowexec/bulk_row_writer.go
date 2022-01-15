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

package rowexec

import (
	"context"
	"sync/atomic"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/storagebase"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/errorutil"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

//CTASPlanResultTypes is the result types for EXPORT plans.
var CTASPlanResultTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES}, // rows
}

type bulkRowWriter struct {
	runbase.ProcessorBase
	flowCtx        *runbase.FlowCtx
	processorID    int32
	batchIdxAtomic int64
	tableDesc      sqlbase.ImmutableTableDescriptor
	spec           distsqlpb.BulkRowWriterSpec
	input          runbase.RowSource
	output         runbase.RowReceiver
	summary        roachpb.BulkOpSummary
}

var _ runbase.Processor = &bulkRowWriter{}
var _ runbase.RowSource = &bulkRowWriter{}

func newBulkRowWriterProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec distsqlpb.BulkRowWriterSpec,
	input runbase.RowSource,
	output runbase.RowReceiver,
) (runbase.Processor, error) {
	c := &bulkRowWriter{
		flowCtx:        flowCtx,
		processorID:    processorID,
		batchIdxAtomic: 0,
		tableDesc:      *sqlbase.NewImmutableTableDescriptor(spec.Table),
		spec:           spec,
		input:          input,
		output:         output,
	}
	if err := c.Init(
		c, &distsqlpb.PostProcessSpec{}, CTASPlanResultTypes, flowCtx, processorID, output,
		nil /* memMonitor */, runbase.ProcStateOpts{InputsToDrain: []runbase.RowSource{input}},
	); err != nil {
		return nil, err
	}
	return c, nil
}

//Start is part of the RowSource interface.
func (sp *bulkRowWriter) Start(ctx context.Context) context.Context {
	sp.input.Start(ctx)
	ctx = sp.StartInternal(ctx, "bulkRowWriter")
	err := sp.work(ctx)
	sp.MoveToDraining(err)
	return ctx
}

// Next is part of the RowSource interface.
func (sp *bulkRowWriter) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	// If there wasn't an error while processing, output the summary.
	if sp.ProcessorBase.State == runbase.StateRunning {
		columnTypeBytes := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
		countsBytes, marshalErr := protoutil.Marshal(&sp.summary)
		sp.MoveToDraining(marshalErr)
		if marshalErr == nil {
			// Output the summary.
			return sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(columnTypeBytes, tree.NewDBytes(tree.DBytes(countsBytes))),
			}, nil
		}
	}
	return nil, sp.DrainHelper()
}

func (sp *bulkRowWriter) work(ctx context.Context) error {
	kvCh := make(chan row.KVBatch, 10)
	var g ctxgroup.Group

	conv, err := row.NewDatumRowConverter(ctx, &sp.tableDesc, sp.EvalCtx, kvCh)
	if err != nil {
		return err
	}
	if conv.EvalCtx.SessionData == nil {
		panic("uninitialized session data")
	}

	g = ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		return sp.ingestLoop(ctx, kvCh)
	})
	g.GoCtx(func(ctx context.Context) error {
		return sp.convertLoop(ctx, kvCh, conv)
	})
	return g.Wait()
}

func (sp *bulkRowWriter) wrapDupError(ctx context.Context, orig error) error {
	var typed *storagebase.DuplicateKeyError
	if !errorutil.As(orig, &typed) {
		return orig
	}
	v := &roachpb.Value{RawBytes: typed.Value}
	return row.NewUniquenessConstraintViolationError(ctx, &sp.tableDesc, typed.Key, v)
}

func (sp *bulkRowWriter) ingestLoop(ctx context.Context, kvCh chan row.KVBatch) error {
	writeTS := sp.spec.Table.CreateAsOfTime
	flushSize := backillerSSTSize.Get(&sp.flowCtx.Cfg.Settings.SV)
	const bufferSize = 64 << 20
	adder, err := sp.flowCtx.Cfg.BulkAdder(
		ctx, sp.flowCtx.Cfg.DB, bufferSize, flushSize, writeTS, true)
	// We disallow shadowing here to ensure that we report errors when builds
	// of unique indexes fail when there are duplicate values.

	if err != nil {
		return err
	}
	defer adder.Close(ctx)

	// ingestKvs drains kvs from the channel until it closes, ingesting them using
	// the BulkAdder. It handles the required buffering/sorting/etc.
	ingestKvs := func() error {
		for kvBatch := range kvCh {
			for _, kv := range kvBatch.KVs {
				if err := adder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
					return sp.wrapDupError(ctx, err)
				}
			}
		}

		if err := adder.Flush(ctx); err != nil {
			return sp.wrapDupError(ctx, err)
		}
		return nil
	}

	// Drain the kvCh using the BulkAdder until it closes.
	if err := ingestKvs(); err != nil {
		return err
	}

	sp.summary = adder.GetSummary()
	return nil
}

func (sp *bulkRowWriter) convertLoop(
	ctx context.Context, kvCh chan row.KVBatch, conv *row.DatumRowConverter,
) error {
	defer close(kvCh)

	done := false
	alloc := &sqlbase.DatumAlloc{}
	typs := sp.input.OutputTypes()

	for {
		var rows int64
		for {
			row, meta := sp.input.Next()
			if meta != nil {
				if meta.Err != nil {
					return meta.Err
				}
				sp.AppendTrailingMeta(*meta)
				continue
			}
			if row == nil {
				done = true
				break
			}
			rows++

			for i, ed := range row {
				if ed.IsNull() {
					conv.Datums[i] = tree.DNull
					continue
				}
				if err := ed.EnsureDecoded(&typs[i], alloc); err != nil {
					return err
				}
				conv.Datums[i] = ed.Datum
			}

			// `conv.Row` uses these as arguments to GenerateUniqueID to generate
			// hidden primary keys, when necessary. We want them to be ascending per
			// to reduce overlap in the resulting kvs and non-conflicting (because
			// of primary key uniqueness). The ids that come out of GenerateUniqueID
			// are sorted by (fileIndex, rowIndex) and unique as long as the two
			// inputs are a unique combo, so using the processor ID and a
			// monotonically increasing batch index should do what we want.
			if err := conv.Row(ctx, sp.processorID, sp.batchIdxAtomic); err != nil {
				return err
			}
			atomic.AddInt64(&sp.batchIdxAtomic, 1)
		}
		if rows < 1 {
			break
		}

		if err := conv.SendBatch(ctx); err != nil {
			return err
		}

		if done {
			break
		}
	}

	return nil
}

// ConsumerClosed is part of the RowSource interface.
func (sp *bulkRowWriter) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	sp.InternalClose()
}

//GetBatch is part of the RowSource interface.
func (sp *bulkRowWriter) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (sp *bulkRowWriter) SetChan() {}
