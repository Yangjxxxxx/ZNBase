// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util/bufalloc"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

var cdcPollInterval = func() *settings.DurationSetting {
	s := settings.RegisterNonNegativeDurationSetting(
		"changefeed.experimental_poll_interval",
		"polling interval for the prototype changefeed implementation",
		1*time.Second,
	)
	s.SetSensitive()
	return s
}()

// PushEnabled is a cluster setting that triggers all subsequently
// created/unpaused changefeeds to receive kv changes via RangeFeed push
// (instead of ExportRequest polling).
var PushEnabled = settings.RegisterBoolSetting(
	"changefeed.push.enabled",
	"if set, changed are pushed instead of pulled. This requires the "+
		"kv.rangefeed.enabled setting.",
	true,
)

const (
	jsonMetaSentinel = `__exdb__`
)

type emitEntry struct {
	// row, if not the zero value, represents a changed row to be emitted.
	row encodeRow

	// resolved, if non-nil, is a guarantee for the associated
	// span that no previously unseen entries with a lower or equal updated
	// timestamp will be emitted.
	resolved *jobspb.ResolvedSpan

	// bufferGetTimestamp is the time this entry came out of the buffer.
	bufferGetTimestamp time.Time
}

// kvsToRows gets changed kvs from a closure and converts them into sql rows. It
// returns a closure that may be repeatedly called to advance the changefeed.
// The returned closure is not threadsafe.
func kvsToRows(
	leaseMgr *sql.LeaseManager,
	details jobspb.ChangefeedDetails,
	inputFn func(context.Context) (bufferEntry, error),
) func(context.Context) ([]emitEntry, error) {
	_, withDiff := details.Opts[optDiff]
	rfCache := newRowFetcherCache(leaseMgr)

	var kvs row.SpanKVFetcher
	appendEmitEntryForKV := func(
		ctx context.Context,
		output []emitEntry,
		kv roachpb.KeyValue,
		prevVal roachpb.Value,
		ddlEvent DDLEvent,
		schemaTimestamp hlc.Timestamp,
		prevSchemaTimestamp hlc.Timestamp,
		bufferGetTimestamp time.Time,
		source Source,
	) ([]emitEntry, error) {

		desc, err := rfCache.TableDescForKey(ctx, kv.Key, schemaTimestamp)
		if err != nil {
			return nil, err
		}
		if keys.DescriptorTableID == desc.ID {
			withDiff = true
		}
		if _, ok := details.Targets[desc.ID]; !ok {

			if !withDiff {
				// This kv is for an interleaved table that we're not watching.
				if log.V(3) {
					log.Infof(ctx, `skipping key from unwatched table %s: %s`, desc.Name, kv.Key)
				}
				return nil, nil
			}
		}

		rf, err := rfCache.RowFetcherForTableDesc(desc)
		if err != nil {
			return nil, err
		}

		// Get new value.
		var r emitEntry
		r.bufferGetTimestamp = bufferGetTimestamp
		{
			// TODO(dan): Handle tables with multiple column families.
			// Reuse kvs to save allocations.
			kvs.KVs = kvs.KVs[:0]
			kvs.KVs = append(kvs.KVs, kv)
			if err := rf.StartScanFrom(ctx, &kvs); err != nil {
				return nil, err
			}
			r.row.datums, r.row.tableDesc, _, err = rf.NextRow(ctx)
			if err != nil {
				return nil, err
			}
			if r.row.datums == nil {
				return nil, errors.AssertionFailedf("unexpected empty datums")
			}
			r.row.datums = append(sqlbase.EncDatumRow(nil), r.row.datums...)
			r.row.deleted = rf.RowIsDeleted()
			// TODO(mrtracy): This should likely be set to schemaTimestamp instead of
			// the value timestamp, if schema timestamp is set. However, doing so
			// seems to break some of the assumptions of our existing tests in subtle
			// ways, so this should be done as part of a dedicated PR.
			r.row.updated = schemaTimestamp
			//output = append(output, r)
			// Assert that we don't get a second row from the row.Fetcher. We
			// fed it a single KV, so that would be surprising.
			var nextRow emitEntry
			nextRow.row.datums, nextRow.row.tableDesc, _, err = rf.NextRow(ctx)
			if err != nil {
				return nil, err
			}
			if nextRow.row.datums != nil {
				return nil, errors.AssertionFailedf("unexpected non-empty datums")
			}
		}

		// Get prev value, if necessary.
		if withDiff {
			prevRF := rf
			if prevSchemaTimestamp != schemaTimestamp {
				// If the previous value is being interpreted under a different
				// version of the schema, fetch the correct table descriptor and
				// create a new row.Fetcher with it.
				prevDesc, err := rfCache.TableDescForKey(ctx, kv.Key, prevSchemaTimestamp)
				if err != nil {
					return nil, err
				}

				prevRF, err = rfCache.RowFetcherForTableDesc(prevDesc)
				if err != nil {
					return nil, err
				}
			}

			prevKV := roachpb.KeyValue{Key: kv.Key, Value: prevVal}
			// TODO(dan): Handle tables with multiple column families.
			// Reuse kvs to save allocations.
			kvs.KVs = kvs.KVs[:0]
			kvs.KVs = append(kvs.KVs, prevKV)
			if err := prevRF.StartScanFrom(ctx, &kvs); err != nil {
				return nil, err
			}
			r.row.prevDatums, r.row.prevTableDesc, _, err = prevRF.NextRow(ctx)
			if err != nil {
				return nil, err
			}
			if r.row.prevDatums == nil {
				return nil, errors.AssertionFailedf("unexpected empty datums")
			}
			r.row.prevDatums = append(sqlbase.EncDatumRow(nil), r.row.prevDatums...)
			r.row.prevDeleted = prevRF.RowIsDeleted()

			// Assert that we don't get a second row from the row.Fetcher. We
			// fed it a single KV, so that would be surprising.
			var nextRow emitEntry
			nextRow.row.prevDatums, nextRow.row.prevTableDesc, _, err = prevRF.NextRow(ctx)
			if err != nil {
				return nil, err
			}
			if nextRow.row.prevDatums != nil {
				return nil, errors.AssertionFailedf("unexpected non-empty datums")
			}
		}
		r.row.ddlEvent = ddlEvent
		r.row.source = source
		output = append(output, r)
		return output, nil
	}

	var output []emitEntry
	return func(ctx context.Context) ([]emitEntry, error) {
		// Reuse output to save allocations.
		output = output[:0]
		for {
			input, err := inputFn(ctx)
			if err != nil {
				return nil, err
			}
			if input.kv.Key != nil {
				if log.V(3) {
					log.Infof(ctx, "changed key %s %s", input.kv.Key, input.kv.Value.Timestamp)
				}
				schemaTimestamp := input.kv.Value.Timestamp
				if input.schemaTimestamp != (hlc.Timestamp{}) {
					schemaTimestamp = input.schemaTimestamp
				}
				prevSchemaTimestamp := schemaTimestamp
				if input.prevSchemaTimestamp != (hlc.Timestamp{}) {
					prevSchemaTimestamp = input.prevSchemaTimestamp
				}
				output, err = appendEmitEntryForKV(
					ctx, output, input.kv, input.prevVal, input.ddlEvent,
					schemaTimestamp, prevSchemaTimestamp,
					input.bufferGetTimestamp, input.source)
				if err != nil {
					return nil, err
				}
			}
			if input.resolved != nil {
				output = append(output, emitEntry{
					resolved:           input.resolved,
					bufferGetTimestamp: input.bufferGetTimestamp,
				})
			}
			if output != nil {
				return output, nil
			}
		}
	}
}

// emitEntrySlice connects to a sink, receives rows from a closure, and repeatedly
// emits them to the sink. It returns a closure that may be repeatedly called to
// advance the changefeed and which returns span-level resolved timestamp
// updates. The returned closure is not threadsafe.
func emitEntrySlice(
	settings *cluster.Settings,
	details jobspb.ChangefeedDetails,
	sf *spanFrontier,
	//watchedSpans []roachpb.Span,
	encoder Encoder,
	sink Sink,
	inputFn func(context.Context) ([]emitEntry, error),
	knobs TestingKnobs,
	metrics *Metrics,
	DB *client.DB,
	Executor sqlutil.InternalExecutor,
	poller *poller,
) func(context.Context) ([]jobspb.ResolvedSpan, error) {
	var scratch bufalloc.ByteAllocator
	emitRowFn := func(ctx context.Context, row encodeRow) (bool, error) {
		succeedEmit := false
		// Ensure that row updates are strictly newer than the least resolved timestamp
		// being tracked by the local span frontier. The poller should not be forwarding
		// row updates that have timestamps less than or equal to any resolved timestamp
		// it's forwarded before.
		// TODO(aayush): This should be an assertion once we're confident this can never
		// happen under any circumstance.
		if !sf.Frontier().Less(row.updated) && row.ddlEvent.IsEmpty() {
			log.Errorf(ctx, "cdc ux violation: detected timestamp %v that is less than "+
				"or equal to the local frontier %v.", row.updated, sf.Frontier())
			return succeedEmit, nil
		}
		var keyCopy, valueCopy []byte
		encodedKey, err := encoder.EncodeKey(row)
		if err != nil {
			return succeedEmit, err
		}
		scratch, keyCopy = scratch.Copy(encodedKey, 0 /* extraCap */)
		encodedValue, encodedDDL, desc, databaseName, schemaName, operate, err := encoder.EncodeValue(ctx, row, DB)
		if err != nil {
			return succeedEmit, err
		}
		scratch, valueCopy = scratch.Copy(encodedValue, 0 /* extraCap */)
		if knobs.BeforeEmitRow != nil {
			if err := knobs.BeforeEmitRow(ctx); err != nil {
				return succeedEmit, err
			}
		}
		if succeedEmit, err = sink.EmitRow(
			ctx, row.tableDesc, keyCopy, valueCopy, row.updated, encodedDDL, desc, databaseName, schemaName, operate, DB, Executor, poller,
		); err != nil {
			return succeedEmit, err
		}
		//发送完成之后更新DML时间戳
		if row.tableDesc.ID != keys.DescriptorTableID {
			poller.mu.Lock()
			poller.mu.lastDMLTransactionCommitTime[row.tableDesc.ID] = row.updated
			poller.mu.Unlock()
			if row.source == FromTmpFile && poller.mu.resolvedTmpEntry[row.tableDesc.ID] != nil {
				atomic.AddInt32(poller.mu.resolvedTmpEntry[row.tableDesc.ID], -1)
			}
		} else {
			tableDesc, ok := desc.(*sqlbase.TableDescriptor)
			if !ok {
				return succeedEmit, err
			}
			if row.ddlEvent.eventSend == row.ddlEvent.eventAll {
				poller.mu.Lock()
				poller.mu.lastDDLSendTime[desc.GetID()] = tableDesc.ModificationTime
				poller.mu.lastDDLSendLocalTime[desc.GetID()] = poller.db.Clock().Now()
				if row.ddlEvent.Operate != UnknownOperation {
					if poller.mu.resolvedSignal[tableDesc.ID] == ChangeFeedWaitToExecDDL {
						poller.mu.resolvedSignal[tableDesc.ID] = ChangeFeedSyncDML
					} else if poller.mu.resolvedSignal[tableDesc.ID] == ChangeFeedExecDDL {
						poller.mu.resolvedSignal[tableDesc.ID] = ChangeFeedWaitToExecDDL
					}
				}
				poller.mu.Unlock()
			}
		}
		if log.V(3) {
			log.Infof(ctx, `row %s: %s -> %s`, row.tableDesc.Name, keyCopy, valueCopy)
		}
		return succeedEmit, nil
	}

	//// This SpanFrontier only tracks the spans being watched on this node.
	//// (There is a different SpanFrontier elsewhere for the entire changefeed.)
	//watchedSF := makeSpanFrontier(watchedSpans...)

	var lastFlush time.Time
	// TODO(dan): We could keep these in `watchedSF` to eliminate dups.
	var resolvedSpans []jobspb.ResolvedSpan

	return func(ctx context.Context) ([]jobspb.ResolvedSpan, error) {
		inputs, err := inputFn(ctx)
		if err != nil {
			return nil, err
		}
		var boundaryReached bool
		_, withDDL := poller.details.Opts[optWithDdl]
		for _, input := range inputs {
			if input.bufferGetTimestamp == (time.Time{}) {
				// We could gracefully handle this instead of panic'ing, but
				// we'd really like to be able to reason about this data, so
				// instead we're defensive. If this is ever seen in prod without
				// breaking a unit test, then we have a pretty severe test
				// coverage issue.
				panic(`unreachable: bufferGetTimestamp is set by all codepaths`)
			}
			processingNanos := timeutil.Since(input.bufferGetTimestamp).Nanoseconds()
			metrics.ProcessingNanos.Inc(processingNanos)
			if input.row.datums != nil {
				if _, err = emitRowFn(ctx, input.row); err != nil {
					return nil, err
				}
			}
			if input.resolved != nil {
				if withDDL {
					_, tableID, err := keys.DecodeTablePrefix(input.resolved.Span.Key)
					if err != nil {
						return nil, err
					}
					poller.mu.Lock()
					if input.resolved.Timestamp.Less(poller.mu.resolvedTmpMin[sqlbase.ID(tableID)]) {
						boundaryReached = boundaryReached || input.resolved.BoundaryReached
						_ = sf.Forward(input.resolved.Span, input.resolved.Timestamp)
						resolvedSpans = append(resolvedSpans, *input.resolved)
					}
					poller.mu.Unlock()
				} else {
					_ = sf.Forward(input.resolved.Span, input.resolved.Timestamp)
					resolvedSpans = append(resolvedSpans, *input.resolved)
				}
			}
		}

		// If the resolved timestamp frequency is specified, use it as a rough
		// approximation of how latency-sensitive the changefeed user is. If
		// it's not, fall back to the poll interval.
		//
		// The current poller implementation means we emit a changefeed-level
		// resolved timestamps to the user once per cdcPollInterval. This
		// buffering adds on average timeBetweenFlushes/2 to that latency. With
		// timeBetweenFlushes and cdcPollInterval both set to 1s, TPCC
		// was seeing about 100x more time spent emitting than flushing.
		// Dividing by 5 tries to balance these a bit, but ultimately is fairly
		// unprincipled.
		//
		// NB: As long as we periodically get new span-level resolved timestamps
		// from the poller (which should always happen, even if the watched data
		// is not changing), then this is sufficient and we don't have to do
		// anything fancy with timers.
		var timeBetweenFlushes time.Duration
		if r, ok := details.Opts[optResolvedTimestamps]; ok && r != `` {
			var err error
			if timeBetweenFlushes, err = time.ParseDuration(r); err != nil {
				return nil, err
			}
		} else {
			timeBetweenFlushes = cdcPollInterval.Get(&settings.SV) / 5
		}
		if len(resolvedSpans) == 0 ||
			(timeutil.Since(lastFlush) < timeBetweenFlushes && !boundaryReached) {
			return nil, nil
		}

		// Make sure to flush the sink before forwarding resolved spans,
		// otherwise, we could lose buffered messages and violate the
		// at-least-once guarantee. This is also true for checkpointing the
		// resolved spans in the job progress.
		if err := sink.Flush(ctx); err != nil {
			return nil, err
		}
		lastFlush = timeutil.Now()
		if knobs.AfterSinkFlush != nil {
			if err := knobs.AfterSinkFlush(); err != nil {
				return nil, err
			}
		}
		ret := append([]jobspb.ResolvedSpan(nil), resolvedSpans...)
		resolvedSpans = resolvedSpans[:0]
		return ret, nil
	}
}
func appendEmitEntryForKV(
	ctx context.Context,
	kv roachpb.KeyValue,
	prevVal roachpb.Value,
	schemaTimestamp hlc.Timestamp,
	leaseMgr *sql.LeaseManager,
	prevSchemaTimestamp hlc.Timestamp,
) (emitEntry, error) {

	var r emitEntry
	rfCache := newRowFetcherCache(leaseMgr)
	desc, err := rfCache.TableDescForKey(ctx, kv.Key, schemaTimestamp)
	if err != nil {
		return r, err
	}

	rf, err := rfCache.RowFetcherForTableDesc(desc)
	if err != nil {
		return r, err
	}
	var kvs row.SpanKVFetcher
	// Get new value.
	// TODO(dan): Handle tables with multiple column families.
	// Reuse kvs to save allocations.
	kvs.KVs = kvs.KVs[:0]
	kvs.KVs = append(kvs.KVs, kv)
	if err := rf.StartScanFrom(ctx, &kvs); err != nil {
		return r, err
	}
	r.row.datums, r.row.tableDesc, _, err = rf.NextRow(ctx)
	if err != nil {
		return r, err
	}
	if r.row.datums == nil {
		return r, errors.AssertionFailedf("unexpected empty datums")
	}
	r.row.datums = append(sqlbase.EncDatumRow(nil), r.row.datums...)
	r.row.deleted = rf.RowIsDeleted()
	// TODO(mrtracy): This should likely be set to schemaTimestamp instead of
	// the value timestamp, if schema timestamp is set. However, doing so
	// seems to break some of the assumptions of our existing tests in subtle
	// ways, so this should be done as part of a dedicated PR.
	r.row.updated = schemaTimestamp
	//output = append(output, r)
	// Assert that we don't get a second row from the row.Fetcher. We
	// fed it a single KV, so that would be surprising.
	prevRF := rf
	prevSchemaTimestampRecord := schemaTimestamp
	if prevSchemaTimestamp != (hlc.Timestamp{}) {
		prevSchemaTimestampRecord = prevSchemaTimestamp
	}
	if prevSchemaTimestampRecord != schemaTimestamp {
		// If the previous value is being interpreted under a different
		// version of the schema, fetch the correct table descriptor and
		// create a new row.Fetcher with it.
		prevDesc, err := rfCache.TableDescForKey(ctx, kv.Key, prevSchemaTimestampRecord)
		if err != nil {
			return r, err
		}

		prevRF, err = rfCache.RowFetcherForTableDesc(prevDesc)
		if err != nil {
			return r, err
		}
	}

	prevKV := roachpb.KeyValue{Key: kv.Key, Value: prevVal}
	// TODO(dan): Handle tables with multiple column families.
	// Reuse kvs to save allocations.
	kvs.KVs = kvs.KVs[:0]
	kvs.KVs = append(kvs.KVs, prevKV)
	if err := prevRF.StartScanFrom(ctx, &kvs); err != nil {
		return r, err
	}
	r.row.prevDatums, r.row.prevTableDesc, _, err = prevRF.NextRow(ctx)
	if err != nil {
		return r, err
	}
	if r.row.prevDatums == nil {
		return r, errors.AssertionFailedf("unexpected empty datums")
	}
	r.row.prevDatums = append(sqlbase.EncDatumRow(nil), r.row.prevDatums...)
	r.row.prevDeleted = prevRF.RowIsDeleted()

	var nextRow emitEntry
	nextRow.row.datums, nextRow.row.tableDesc, _, err = rf.NextRow(ctx)
	if err != nil {
		return r, err
	}
	if nextRow.row.datums != nil {
		return r, errors.AssertionFailedf("unexpected non-empty datums")
	}
	return r, nil
}

func appendEmitEntryForKVWithBegin(
	ctx context.Context,
	kv roachpb.KeyValue,
	schemaTimestamp hlc.Timestamp,
	leaseMgr *sql.LeaseManager,
) (emitEntry, error) {

	var r emitEntry
	rfCache := newRowFetcherCache(leaseMgr)
	desc, err := rfCache.TableDescForKey(ctx, kv.Key, schemaTimestamp)
	if err != nil {
		return r, err
	}

	rf, err := rfCache.RowFetcherForTableDesc(desc)
	if err != nil {
		return r, err
	}
	var kvs row.SpanKVFetcher
	// Get new value.
	// TODO(dan): Handle tables with multiple column families.
	// Reuse kvs to save allocations.
	kvs.KVs = kvs.KVs[:0]
	kvs.KVs = append(kvs.KVs, kv)
	if err := rf.StartScanFrom(ctx, &kvs); err != nil {
		return r, err
	}
	r.row.datums, r.row.tableDesc, _, err = rf.NextRow(ctx)
	if err != nil {
		return r, err
	}
	if r.row.datums == nil {
		return r, errors.AssertionFailedf("unexpected empty datums")
	}
	r.row.datums = append(sqlbase.EncDatumRow(nil), r.row.datums...)
	r.row.deleted = rf.RowIsDeleted()
	// TODO(mrtracy): This should likely be set to schemaTimestamp instead of
	// the value timestamp, if schema timestamp is set. However, doing so
	// seems to break some of the assumptions of our existing tests in subtle
	// ways, so this should be done as part of a dedicated PR.
	r.row.updated = schemaTimestamp
	//output = append(output, r)
	// Assert that we don't get a second row from the row.Fetcher. We
	// fed it a single KV, so that would be surprising.

	var nextRow emitEntry
	nextRow.row.datums, nextRow.row.tableDesc, _, err = rf.NextRow(ctx)
	if err != nil {
		return r, err
	}
	if nextRow.row.datums != nil {
		return r, errors.AssertionFailedf("unexpected non-empty datums")
	}
	return r, nil
}

// checkpointResolvedTimestamp checkpoints a changefeed-level resolved timestamp
// to the jobs record.
func checkpointResolvedTimestamp(
	ctx context.Context,
	jobProgressedFn func(context.Context, jobs.HighWaterProgressedFn) error,
	sf *spanFrontier,
) error {
	resolved := sf.Frontier()
	var resolvedSpans []jobspb.ResolvedSpan
	sf.Entries(func(span roachpb.Span, ts hlc.Timestamp) {
		resolvedSpans = append(resolvedSpans, jobspb.ResolvedSpan{
			Span: span, Timestamp: ts,
		})
	})

	// Some benchmarks want to skip the job progress update for a bit more
	// isolation.
	//
	// NB: To minimize the chance that a user sees duplicates from below
	// this resolved timestamp, keep this update of the high-water mark
	// before emitting the resolved timestamp to the sink.
	if jobProgressedFn != nil {
		progressedClosure := func(ctx context.Context, d jobspb.ProgressDetails) hlc.Timestamp {
			// TODO(dan): This was making enormous jobs rows, especially in
			// combination with how many mvcc versions there are. Cut down on
			// the amount of data used here dramatically and re-enable.
			//
			// d.(*jobspb.Progress_Changefeed).CDC.ResolvedSpans = resolvedSpans
			return resolved
		}
		if err := jobProgressedFn(ctx, progressedClosure); err != nil {
			if _, ok := err.(*jobs.InvalidStatusError); ok {
				err = MarkTerminalError(err)
			}
			return err
		}
	}
	return nil
}

// emitResolvedTimestamp emits a changefeed-level resolved timestamp to the
// sink.
func emitResolvedTimestamp(
	ctx context.Context, encoder Encoder, sink Sink, resolved hlc.Timestamp,
) error {
	// TODO(dan): Emit more fine-grained (table level) resolved
	// timestamps.
	if err := sink.EmitResolvedTimestamp(ctx, encoder, resolved); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, `resolved %s`, resolved)
	}
	return nil
}
