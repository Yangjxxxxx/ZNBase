// Copyright 2018 The Cockroach Authors.
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

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/scrub"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

const indexJoinerBatchSize = 100

// indexJoiner performs a join between a secondary index, the `input`, and the
// primary index of the same table, `desc`, to retrieve columns which are not
// stored in the secondary index.
type indexJoiner struct {
	runbase.ProcessorBase

	input runbase.RowSource
	desc  sqlbase.TableDescriptor

	// fetcherInput wraps fetcher in a RowSource implementation and should be used
	// to get rows from the fetcher. This enables the indexJoiner to wrap the
	// fetcherInput with a stat collector when necessary.
	fetcherInput runbase.RowSource
	fetcher      row.Fetcher
	// fetcherReady indicates that we have started an index scan and there are
	// potentially more rows to retrieve.
	fetcherReady bool
	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// keyPrefix is the primary index's key prefix.
	keyPrefix []byte
	// spans is the batch of spans we will next retrieve from the index.
	spans roachpb.Spans

	alloc sqlbase.DatumAlloc
}

var _ runbase.Processor = &indexJoiner{}
var _ runbase.RowSource = &indexJoiner{}

const indexJoinerProcName = "index joiner"

func newIndexJoiner(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec *distsqlpb.JoinReaderSpec,
	input runbase.RowSource,
	post *distsqlpb.PostProcessSpec,
	output runbase.RowReceiver,
) (*indexJoiner, error) {
	if spec.IndexIdx != 0 {
		return nil, errors.Errorf("index join must be against primary index")
	}
	ij := &indexJoiner{
		input:     input,
		desc:      spec.Table,
		keyPrefix: sqlbase.MakeIndexKeyPrefix(&spec.Table, spec.Table.PrimaryIndex.ID),
		batchSize: indexJoinerBatchSize,
	}
	needMutations := spec.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	if err := ij.Init(
		ij,
		post,
		ij.desc.ColumnTypesWithMutations(needMutations),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		runbase.ProcStateOpts{
			InputsToDrain: []runbase.RowSource{ij.input},
			TrailingMetaCallback: func(ctx context.Context) []distsqlpb.ProducerMetadata {
				ij.InternalClose()
				if meta := runbase.GetTxnCoordMeta(ctx, ij.FlowCtx.Txn); meta != nil {
					return []distsqlpb.ProducerMetadata{{TxnCoordMeta: meta}}
				}
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	if _, _, err := initRowFetcher(
		&ij.fetcher,
		&ij.desc,
		0, /* primary index */
		ij.desc.ColumnIdxMapWithMutations(needMutations),
		false, /* reverse */
		ij.Out.NeededColumns(),
		false, /* isCheck */
		&ij.alloc,
		spec.Visibility,
		spec.LockingStrength,
		spec.LockingWaitPolicy,
	); err != nil {
		return nil, err
	}
	ij.fetcherInput = &rowFetcherWrapper{Fetcher: &ij.fetcher}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		// Enable stats collection.
		ij.input = NewInputStatCollector(ij.input)
		ij.fetcherInput = NewInputStatCollector(ij.fetcherInput)
		ij.FinishTrace = ij.outputStatsToTrace
	}

	return ij, nil
}

// Start is part of the RowSource interface.
func (ij *indexJoiner) Start(ctx context.Context) context.Context {
	ij.input.Start(ctx)
	ij.fetcherInput.Start(ctx)
	return ij.StartInternal(ctx, indexJoinerProcName)
}

//GetBatch is part of the RowSource interface.
func (ij *indexJoiner) GetBatch(
	ctx context.Context, respons chan roachpb.BlockStruct,
) context.Context {
	return ctx
}

//SetChan is part of the RowSource interface.
func (ij *indexJoiner) SetChan() {}

// Next is part of the RowSource interface.
func (ij *indexJoiner) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for ij.State == runbase.StateRunning {
		if !ij.fetcherReady {
			// Retrieve a batch of rows from the input.
			for len(ij.spans) < ij.batchSize {
				row, meta := ij.input.Next()
				if meta != nil {
					if meta.Err != nil {
						ij.MoveToDraining(nil /* err */)
					}
					return nil, meta
				}
				if row == nil {
					break
				}
				span, err := ij.generateSpan(row)
				if err != nil {
					ij.MoveToDraining(err)
					return nil, ij.DrainHelper()
				}
				ij.spans = append(ij.spans, span)
			}
			if len(ij.spans) == 0 {
				// All done.
				ij.MoveToDraining(nil /* err */)
				return nil, ij.DrainHelper()
			}
			// Scan the primary index for this batch.
			err := ij.fetcher.StartScan(
				ij.Ctx, ij.FlowCtx.Txn, ij.spans, false /* limitBatches */, 0, /* limitHint */
				ij.FlowCtx.TraceKV)
			if err != nil {
				ij.MoveToDraining(err)
				return nil, ij.DrainHelper()
			}
			ij.fetcherReady = true
			ij.spans = ij.spans[:0]
		}
		row, meta := ij.fetcherInput.Next()
		if meta != nil {
			ij.MoveToDraining(scrub.UnwrapScrubError(meta.Err))
			return nil, ij.DrainHelper()
		}
		if row == nil {
			// Done with this batch.
			ij.fetcherReady = false
		} else if outRow := ij.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, ij.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (ij *indexJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ij.InternalClose()
}

func (ij *indexJoiner) generateSpan(row sqlbase.EncDatumRow) (roachpb.Span, error) {
	numKeyCols := len(ij.desc.PrimaryIndex.ColumnIDs)
	if len(row) < numKeyCols {
		return roachpb.Span{}, errors.Errorf(
			"index join input has %d columns, expected at least %d", len(row), numKeyCols)
	}
	// There may be extra values on the row, e.g. to allow an ordered
	// synchronizer to interleave multiple input streams. Will need at most
	// numKeyCols.
	keyRow := row[:numKeyCols]
	types := ij.input.OutputTypes()[:numKeyCols]
	return sqlbase.MakeSpanFromEncDatums(
		ij.keyPrefix, keyRow, types, ij.desc.PrimaryIndex.ColumnDirections, &ij.desc,
		&ij.desc.PrimaryIndex, &ij.alloc)
}

// outputStatsToTrace outputs the collected indexJoiner stats to the trace. Will
// fail silently if the indexJoiner is not collecting stats.
func (ij *indexJoiner) outputStatsToTrace() {
	is, ok := getInputStats(ij.FlowCtx, ij.input)
	if !ok {
		return
	}
	ils, ok := getInputStats(ij.FlowCtx, ij.fetcherInput)
	if !ok {
		return
	}
	jrs := &JoinReaderStats{
		InputStats:       is,
		IndexLookupStats: ils,
	}
	if sp := opentracing.SpanFromContext(ij.Ctx); sp != nil {
		tracing.SetSpanStats(sp, jrs)
	}
}

// SetBatchSize sets the desired batch size. It should only be used in tests.
func (ij *indexJoiner) SetBatchSize(batchSize int) {
	ij.batchSize = batchSize
}
