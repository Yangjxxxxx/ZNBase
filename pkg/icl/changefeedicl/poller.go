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
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/icl/utilicl/intervalicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/bufalloc"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/encoding/csv"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

var (
	errUnknownTableID = errors.New("Cannot get bufEntry from uninitialized TableID")
)

// TODO(ajwerner): Ideally we could have a centralized worker which reads the
// table descriptors instead of polling from each changefeed. This wouldn't be
// too hard. Each registered queue would have a start time. You'd scan from the
// earliest and just ingest the relevant descriptors.

// TableEvent represents a change to a table descriptor.
type TableEvent struct {
	Before, After *sqlbase.TableDescriptor
}

// poller uses ExportRequest with the `ReturnSST` to repeatedly fetch every kv
// that changed between a set of timestamps and insert them into a buffer.
//
// Each poll (ie set of ExportRequests) are rate limited to be no more often
// than the `changefeed.experimental_poll_interval` setting.
type poller struct {
	settings  *cluster.Settings
	db        *client.DB
	clock     *hlc.Clock
	gossip    *gossip.Gossip
	spans     []roachpb.Span
	details   jobspb.ChangefeedDetails
	buf       *buffer
	tableHist *tableHistory
	ca        *changeAggregator
	sf        *spanFrontier

	//lastDDLDescTime store ModificationTime after the last DDL operation
	//ddlcount obtains the correct end time of DDL operation and records it
	//lastTableName record the table name of the last DDL operation
	dumpSink           dumpsink.DumpSink
	leaseMgr           *sql.LeaseManager
	metrics            *Metrics
	mm                 *mon.BytesMonitor
	SchemaChangePolicy SchemaChangePolicy
	sink               Sink
	Executor           sqlutil.InternalExecutor
	mu                 struct {
		syncutil.Mutex
		// highWater timestamp for exports processed by this poller so far.
		highWater hlc.Timestamp
		// scanBoundaries represent timestamps where the cdc output process
		// should pause and output a scan of *all keys* of the watched spans at the
		// given timestamp. There are currently two situations where this occurs:
		// the initial scan of the table when starting a new CDC, and when
		// a backfilling schema change is marked as completed. This collection must
		// be kept in sorted order (by timestamp ascending).
		scanBoundaries []hlc.Timestamp
		// previousTableVersion is a map from tableID to the most recent version
		// of the table descriptor seen by the poller. This is needed to determine
		// when a backilling mutation has successfully completed - this can only
		// be determining by comparing a version to the previous version.
		previousTableVersion map[sqlbase.ID]*sqlbase.TableDescriptor
		//临时文件中最小时间戳
		resolvedTmpMin map[sqlbase.ID]hlc.Timestamp
		//临时文件中最小时间戳
		resolvedTmpEntry map[sqlbase.ID]*int32
		//resolvedSignal map[string]chan int
		resolvedSignal map[sqlbase.ID]ChangeFeedDDLState

		lastDMLTransactionCommitTime map[sqlbase.ID]hlc.Timestamp

		lastDDLSendTime      map[sqlbase.ID]hlc.Timestamp
		lastDDLSendLocalTime map[sqlbase.ID]hlc.Timestamp
		//Record the current blocked DDL timestamp
		blockTime map[sqlbase.ID]hlc.Timestamp
	}
}

// ChangeFeedDDLState is the type for change feed status
type ChangeFeedDDLState int

const (
	// ChangeFeedUnknown stands for all unknown status
	//ChangeFeedUnknown ChangeFeedDDLState = iota

	// ChangeFeedSyncDML means DMLs are being processed
	ChangeFeedSyncDML = iota
	// ChangeFeedWaitToExecDDL means we are waiting to execute a DDL
	ChangeFeedWaitToExecDDL
	// ChangeFeedExecDDL means a DDL is being executed
	ChangeFeedExecDDL
	// ChangeFeedDDLExecuteFailed means that an error occurred when executing a DDL
	//ChangeFeedDDLExecuteFailed
)

// create Poller and return the poller instance
func makePoller(
	settings *cluster.Settings,
	db *client.DB,
	clock *hlc.Clock,
	gossip *gossip.Gossip,
	spans []roachpb.Span,
	details jobspb.ChangefeedDetails,
	highWater hlc.Timestamp,
	buf *buffer,
	leaseMgr *sql.LeaseManager,
	metrics *Metrics,
	mm *mon.BytesMonitor,
	events SchemaChangeEventClass,
	policy SchemaChangePolicy,
	sink Sink,
	Executor sqlutil.InternalExecutor,
	ca *changeAggregator,
	sf *spanFrontier,
) *poller {
	resolvedTmpMin := make(map[sqlbase.ID]hlc.Timestamp, len(details.Tables))
	resolvedSignal := make(map[sqlbase.ID]ChangeFeedDDLState)
	resolvedTmpEntry := make(map[sqlbase.ID]*int32)
	backFillTables := make(map[sqlbase.ID]struct{}, len(details.Tables))
	lastDMLTransactionCommitTime := make(map[sqlbase.ID]hlc.Timestamp, len(details.Tables))
	lastDDLSendTime := make(map[sqlbase.ID]hlc.Timestamp, len(details.Tables))
	lastDDLSendLocalTime := make(map[sqlbase.ID]hlc.Timestamp, len(details.Tables))
	blockTime := make(map[sqlbase.ID]hlc.Timestamp, len(details.Tables))
	for _, desc := range details.Tables {
		_, tableID, err := keys.DecodeTablePrefix(desc.PrimaryIndexSpan().Key)
		if err != nil {
			log.Errorf(context.TODO(), "can not find tableID from %v,err:%v", desc.PrimaryIndexSpan().Key, err)
		}
		var begin int32
		resolvedTmpMin[sqlbase.ID(tableID)] = hlc.MaxTimestamp
		resolvedSignal[sqlbase.ID(tableID)] = ChangeFeedSyncDML
		backFillTables[sqlbase.ID(tableID)] = struct{}{}
		resolvedTmpEntry[sqlbase.ID(tableID)] = &begin
		lastDMLTransactionCommitTime[sqlbase.ID(tableID)] = desc.ModificationTime
		lastDDLSendTime[sqlbase.ID(tableID)] = desc.ModificationTime
		lastDDLSendLocalTime[sqlbase.ID(tableID)] = desc.ModificationTime
		blockTime[sqlbase.ID(tableID)] = hlc.Timestamp{}
	}
	p := &poller{
		settings:           settings,
		db:                 db,
		clock:              clock,
		gossip:             gossip,
		spans:              spans,
		details:            details,
		buf:                buf,
		leaseMgr:           leaseMgr,
		metrics:            metrics,
		mm:                 mm,
		SchemaChangePolicy: policy,
		sink:               sink,
		Executor:           Executor,
		ca:                 ca,
		sf:                 sf,
	}
	p.mu.previousTableVersion = make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	// If no highWater is specified, set the highwater to the statement time
	// and add a scanBoundary at the statement time to trigger an immediate output
	// of the full table.
	if highWater == (hlc.Timestamp{}) {
		p.mu.highWater = details.StatementTime
		p.mu.scanBoundaries = append(p.mu.scanBoundaries, details.StatementTime)
	} else {
		p.mu.highWater = highWater
	}
	p.tableHist = makeTableHistory(p.validateTable, highWater, events)
	p.mu.resolvedTmpMin = resolvedTmpMin
	p.mu.resolvedSignal = resolvedSignal
	p.mu.resolvedTmpEntry = resolvedTmpEntry
	p.mu.lastDMLTransactionCommitTime = lastDMLTransactionCommitTime
	p.mu.lastDDLSendTime = lastDDLSendTime
	p.mu.lastDDLSendLocalTime = lastDDLSendLocalTime
	p.mu.blockTime = blockTime
	p.tableHist = makeTableHistory(p.validateTable, highWater, events)
	return p
}

// Run repeatedly polls and inserts changed kvs and resolved timestamps into a
// buffer. It blocks forever and is intended to be run in a goroutine.
//
// During each poll, a new high-water mark is chosen. The relevant spans for the
// configured tables are broken up by (possibly stale) range boundaries and
// every changed KV between the old and new high-water is fetched via
// ExportRequests. It backpressures sending the requests such that some maximum
// number are inflight or being inserted into the buffer. Finally, after each
// poll completes, a resolved timestamp notification is added to the buffer.
func (p *poller) Run(ctx context.Context) error {
	for {
		// Wait for polling interval
		p.mu.Lock()
		lastHighwater := p.mu.highWater
		p.mu.Unlock()

		pollDuration := cdcPollInterval.Get(&p.settings.SV)
		pollDuration = pollDuration - timeutil.Since(timeutil.Unix(0, lastHighwater.WallTime))
		if pollDuration > 0 {
			log.VEventf(ctx, 1, `sleeping for %s`, pollDuration)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollDuration):
			}
		}

		nextHighWater := p.clock.Now()

		tableMetadataStart := timeutil.Now()
		// Ingest table descriptors up to the next prospective highwater.
		if err := p.updateTableHistory(ctx, nextHighWater); err != nil {
			return err
		}
		p.metrics.TableMetadataNanos.Inc(timeutil.Since(tableMetadataStart).Nanoseconds())

		// Determine if we are at a scanBoundary, and trigger a full scan if needed.
		isFullScan := false
		p.mu.Lock()
		if len(p.mu.scanBoundaries) > 0 {
			if p.mu.scanBoundaries[0].Equal(lastHighwater) {
				// Perform a full scan of the latest value of all keys as of the
				// boundary timestamp and consume the boundary.
				isFullScan = true
				nextHighWater = lastHighwater
				p.mu.scanBoundaries = p.mu.scanBoundaries[1:]
			} else if p.mu.scanBoundaries[0].Less(nextHighWater) {
				// If we aren't currently at a scan boundary, but the next highwater
				// would bring us past the scan boundary, set nextHighWater to the
				// scan boundary. This will cause us to capture all changes up to the
				// scan boundary, then consume the boundary on the next iteration.
				nextHighWater = p.mu.scanBoundaries[0]
			}
		}
		p.mu.Unlock()

		if !isFullScan {
			log.VEventf(ctx, 1, `changefeed poll (%s,%s]: %s`,
				lastHighwater, nextHighWater, time.Duration(nextHighWater.WallTime-lastHighwater.WallTime))
		} else {
			log.VEventf(ctx, 1, `changefeed poll full scan @ %s`, nextHighWater)
		}

		spans, err := getSpansToProcess(ctx, p.db, p.spans)
		if err != nil {
			return err
		}
		_, withDiff := p.details.Opts[optDiff]
		//TODO 过滤用户表
		_, withDDL := p.details.Opts[optWithDdl]
		if withDDL {
			withDiff = true
		}
		if err := p.exportSpansParallel(ctx, spans, lastHighwater, withDiff); err != nil {
			return err
		}
		p.mu.Lock()
		p.mu.highWater = nextHighWater
		p.mu.Unlock()
	}
}

// RunUsingRangeFeeds performs the same task as the normal Run method, but uses
// the experimental Rangefeed system to capture changes rather than the
// poll-and-export method.  Note
func (p *poller) RunUsingRangefeeds(ctx context.Context) error {
	// Start polling tablehistory, which must be done concurrently with
	// the individual rangefeed routines.
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(p.pollTableHistory)
	g.GoCtx(p.rangefeedImpl)
	return g.Wait()
}

var errBoundaryReached = errors.New("scan boundary reached")

func (p *poller) rangefeedImpl(ctx context.Context) error {
	for i := 0; ; i++ {
		if err := p.rangefeedImplIter(ctx, i); err != nil {
			return err
		}
	}
}
func (p *poller) rangefeedImplIter(ctx context.Context, i int) error {
	// Determine whether to request the previous value of each update from
	// RangeFeed based on whether the `diff` option is specified.
	//TODO 过滤用户表
	_, withDiff := p.details.Opts[optDiff]
	_, withDDL := p.details.Opts[optWithDdl]
	if withDDL {
		withDiff = true
	}
	//tableList := make(map[sqlbase.ID]struct{})
	p.mu.Lock()
	lastHighwater := p.mu.highWater
	p.mu.Unlock()
	if err := p.tableHist.WaitForTS(ctx, lastHighwater); err != nil {
		return err
	}
	initialScan := i == 0
	backfillWithDiff := !initialScan && withDiff
	err := p.scanIfShould(ctx, initialScan, backfillWithDiff)
	if err != nil {
		return err
	}
	spans, err := getSpansToProcess(ctx, p.db, p.spans)
	if err != nil {
		return err
	}
	desc, err := sqlbase.GetTableDescFromID(ctx, p.db.NewTxn(ctx, "get descriptor desc"), keys.DescriptorTableID)
	if err != nil {
		return err
	}
	descSpan := desc.PrimaryIndexSpan()
	spansAddDescriptorSpan := p.spans
	if withDDL {
		spansAddDescriptorSpan = append(spansAddDescriptorSpan, descSpan)
	}
	frontier := makeSpanFrontier(spans...)

	// Perform a full scan if necessary - either an initial scan or a backfill
	// Full scans are still performed using an Export operation.
	//initialScan := i == 0
	//backfillWithDiff := !initialScan && withDiff
	//var scanTime hlc.Timestamp
	//p.mu.Lock()
	//if len(p.mu.scanBoundaries) > 0 && p.mu.scanBoundaries[0].Equal(p.mu.highWater) {
	//	// Perform a full scan of the latest value of all keys as of the
	//	// boundary timestamp and consume the boundary.
	//	scanTime = p.mu.scanBoundaries[0]
	//	p.mu.scanBoundaries = p.mu.scanBoundaries[1:]
	//}
	//p.mu.Unlock()
	//当设置为不触发回填操作时
	//if !initialScan && p.SchemaChangePolicy == OptSchemaChangePolicyNoBackfill {
	//	return nil
	//}
	//if scanTime != (hlc.Timestamp{}) {
	//	// TODO(dan): Now that we no longer have the poller, we should stop using
	//	// ExportRequest and start using normal Scans.
	//	if err := p.exportSpansParallel(ctx, spans, scanTime, backfillWithDiff); err != nil {
	//		return err
	//	}
	//}
	// Start rangefeeds, exit polling if we hit a resolved timestamp beyond
	// the next scan boundary.
	// TODO(nvanbenschoten): This is horrible.
	sender := p.db.NonTransactionalSender()
	ds := sender.(*client.CrossRangeTxnWrapperSender).Wrapped().(*kv.DistSender)
	g := ctxgroup.WithContext(ctx)
	//此处将所有span区分开来，之间不相互阻塞
	eventCs := make(map[string]chan *roachpb.RangeFeedEvent)
	for _, span := range spansAddDescriptorSpan {
		key := span.Key.String()
		eventCs[key] = make(chan *roachpb.RangeFeedEvent, 128)
	}
	//eventC := make(chan *roachpb.RangeFeedEvent, 128)

	// To avoid blocking raft, RangeFeed puts all entries in a server side
	// buffer. But to keep things simple, it's a small fixed-sized buffer. This
	// means we need to ingest everything we get back as quickly as possible, so
	// we throw it in a buffer here to pick up the slack between RangeFeed and
	// the sink.
	//
	// TODO(dan): Right now, there are two buffers in the changefeed flow when
	// using RangeFeeds, one here and the usual one between the poller and the
	// rest of the changefeed (he latter of which is implemented with an
	// unbuffered channel, and so doesn't actually buffer). Ideally, we'd have
	// one, but the structure of the poller code right now makes this hard.
	// Specifically, when a schema change happens, we need a barrier where we
	// flush out every change before the schema change timestamp before we start
	// emitting any changes from after the schema change. The poller's
	// `tableHist` is responsible for detecting and enforcing these (they queue
	// up in `p.scanBoundaries`), but the after-poller buffer doesn't have
	// access to any of this state. A cleanup is in order.

	// Maintain a local spanfrontier to tell when all the component rangefeeds
	// being watched have reached the Scan boundary.
	// TODO(mrtracy): The alternative to this would be to maintain two
	// goroutines for each span (the current arrangement is one goroutine per
	// span and one multiplexing goroutine that outputs to the buffer). This
	// alternative would allow us to stop the individual rangefeeds earlier and
	// avoid the need for a span frontier, but would also introduce a different
	// contention pattern and use additional goroutines. it's not clear which
	// solution is best without targeted performance testing, so we're choosing
	// the faster-to-implement solution for now.
	recordFrontiers := make(map[sqlbase.ID]*spanFrontier, len(spans))
	for _, span := range spans {
		_, tableID, err := keys.DecodeTablePrefix(span.Key)
		if err != nil {
			return err
		}
		recordFrontiers[sqlbase.ID(tableID)] = makeSpanFrontier(span)
	}

	rangeFeedStartTS := make(map[sqlbase.ID]hlc.Timestamp, len(p.spans))
	for _, span := range p.spans {
		spanTmp := roachpb.Span{Key: span.Key, EndKey: span.EndKey}

		highWater := lastHighwater
		req := &roachpb.RangeFeedRequest{
			Header: roachpb.Header{
				Timestamp: highWater,
			},
			Span: span,
		}
		frontier.Forward(span, highWater)
		if withDDL {
			_, tableID, err := keys.DecodeTablePrefix(spanTmp.Key)
			if err != nil {
				return err
			}
			recordFrontiers[sqlbase.ID(tableID)].Forward(span, highWater)
			rangeFeedStartTS[sqlbase.ID(tableID)] = highWater
		}
		g.GoCtx(func(ctx context.Context) error {
			err := ds.RangeFeed(ctx, req, withDiff, eventCs[spanTmp.Key.String()])
			if _, ok := err.(*roachpb.BatchTimestampBeforeGCError); ok {
				err = MarkTerminalError(err)
			}
			return err
		})
	}
	if withDDL {
		span := descSpan
		req := &roachpb.RangeFeedRequest{
			Header: roachpb.Header{
				Timestamp: lastHighwater,
			},
			Span: span,
		}
		g.GoCtx(func(ctx context.Context) error {
			return ds.RangeFeed(ctx, req, withDiff, eventCs[descSpan.Key.String()])
		})
	}
	//此处将所有span区分开来，之间不相互阻塞
	memBuffs := make(map[sqlbase.ID]*memBuffer)
	ddlMemBufs := make(map[sqlbase.ID]*memBuffer)
	for _, span := range spansAddDescriptorSpan {
		_, tableID, err := keys.DecodeTablePrefix(span.Key)
		if err != nil {
			return err
		}
		memBuffs[sqlbase.ID(tableID)] = makeMemBuffer(p.mm.MakeBoundAccount(), p.metrics)
		ddlMemBufs[sqlbase.ID(tableID)] = makeMemBuffer(p.mm.MakeBoundAccount(), p.metrics)
	}

	//memBuf := makeMemBuffer(p.mm.MakeBoundAccount(), p.metrics)
	defer memBufClose(ctx, memBuffs)
	defer memBufClose(ctx, ddlMemBufs)

	for _, span := range spansAddDescriptorSpan {
		spanTmp := roachpb.Span{Key: span.Key, EndKey: span.EndKey}
		_, tableID, err := keys.DecodeTablePrefix(spanTmp.Key)
		if err != nil {
			return err
		}
		g.GoCtx(func(ctx context.Context) error {
			for {
				select {
				case e := <-eventCs[spanTmp.Key.String()]:
					if withDDL {
						if !spanTmp.Equal(descSpan) {
							for {
								p.mu.Lock()
								resolvedSignal := p.mu.resolvedSignal[sqlbase.ID(tableID)]
								p.mu.Unlock()
								if resolvedSignal == ChangeFeedSyncDML {
									break
								}
								select {
								case <-ctx.Done():
									return ctx.Err()
								default:
									continue
								}
							}
						}
					}
					switch t := e.GetValue().(type) {
					case *roachpb.RangeFeedValue:
						kv := roachpb.KeyValue{Key: t.Key, Value: t.Value}
						var prevVal roachpb.Value
						if withDiff {
							prevVal = t.PrevValue
						}
						if err := memBuffs[sqlbase.ID(tableID)].AddKV(ctx, kv, prevVal, hlc.Timestamp{}, FromRangeFeed); err != nil {
							return err
						}
					case *roachpb.RangeFeedCheckpoint:
						if !t.ResolvedTS.IsEmpty() && t.ResolvedTS.Less(rangeFeedStartTS[sqlbase.ID(tableID)]) {
							// RangeFeed happily forwards any closed timestamps it receives as
							// soon as there are no outstanding intents under them.
							// Changefeeds don't care about these at all, so throw them out.
							continue
						}
						if err := memBuffs[sqlbase.ID(tableID)].AddResolved(ctx, t.Span, t.ResolvedTS); err != nil {
							return err
						}
					default:
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}
	for _, span := range spansAddDescriptorSpan {
		spanTmp := roachpb.Span{Key: span.Key, EndKey: span.EndKey}
		_, tableID, err := keys.DecodeTablePrefix(spanTmp.Key)
		if err != nil {
			return err
		}
		g.GoCtx(func(ctx context.Context) error {
			dmlCache := NewDMLMemCache()
			defer dmlCache.Reset()
			for {
				var e bufferEntry
				if memBuf, ok := memBuffs[sqlbase.ID(tableID)]; ok {
					e, err = memBuf.Get(ctx)
					if err != nil {
						return err
					}
				} else {
					return errUnknownTableID
				}
				if withDDL {
					if !spanTmp.Equal(descSpan) {
						for {
							p.mu.Lock()
							resolvedSignal := p.mu.resolvedSignal[sqlbase.ID(tableID)]
							p.mu.Unlock()
							if resolvedSignal == ChangeFeedSyncDML {
								break
							}
							select {
							case <-ctx.Done():
								return ctx.Err()
							default:
								continue
							}
						}
					}
				}
				if e.kv.Key != nil {
					//进行区别处理
					var output emitEntry
					if withDDL {
						if spanTmp.Equal(descSpan) {
							output, err = appendEmitEntryForKVWithBegin(ctx, e.kv, e.kv.Value.Timestamp, p.leaseMgr)
							if err != nil {
								return err
							}
							changefeedDescProto, err := parseDescriptorFromValue(output, output.row.datums)
							if err != nil {
								return err
							}
							_, ok := p.details.Tables[uint32(changefeedDescProto.GetID())]
							if ok {
								changefeedDesc := changefeedDescProto.Table(e.kv.Value.Timestamp)
								if ddlMemBuf, ok := ddlMemBufs[changefeedDesc.ID]; ok {
									err = ddlMemBuf.AddKV(ctx, e.kv, e.prevVal, hlc.Timestamp{}, FromTmpFile)
									if err != nil {
										return err
									}
								} else {
									return errUnknownTableID
								}
							}
						} else {
							p.mu.Lock()
							lastDDLSendTime := p.mu.lastDDLSendTime[sqlbase.ID(tableID)]
							p.mu.Unlock()
							rfCache := newRowFetcherCache(p.leaseMgr)
							changefeedDesc, err := rfCache.TableDescForKey(ctx, e.kv.Key, e.kv.Value.Timestamp)
							if err != nil {
								return err
							}
							output, err := appendEmitEntryForKV(ctx, e.kv, e.prevVal, e.kv.Value.Timestamp, p.leaseMgr, e.prevSchemaTimestamp)
							if err != nil {
								return err
							}
							if reflect.DeepEqual(output.row.datums, output.row.prevDatums) && !output.row.prevDeleted && !output.row.deleted {
								continue
							}
							if lastDDLSendTime.Less(changefeedDesc.ModificationTime) {
								if dmlCache.shouldFlush() {
									fileName := strconv.FormatInt(p.ca.spec.JobId, 10) + strconv.FormatInt(int64(tableID), 10)
									err = dmlCache.FlushToFile(ctx, p.dumpSink, fileName)
									if err != nil {
										return err
									}
								}
								buf, err := e.MarshalJSON()
								if err != nil {
									return err
								}
								buf = append(buf, '\n')
								_, err = dmlCache.Write(buf)
								if err != nil {
									return err
								}
								if e.source == FromRangeFeed && p.mu.resolvedTmpEntry[sqlbase.ID(tableID)] != nil {
									atomic.AddInt32(p.mu.resolvedTmpEntry[sqlbase.ID(tableID)], 1)
								}
								p.mu.Lock()
								if e.kv.Value.Timestamp.Less(p.mu.resolvedTmpMin[changefeedDesc.ID]) {
									p.mu.resolvedTmpMin[changefeedDesc.ID] = e.kv.Value.Timestamp
								}
								p.mu.Unlock()
							} else {
								if err := p.buf.AddKV(ctx, e.kv, e.prevVal, e.schemaTimestamp, hlc.Timestamp{}, e.ddlEvent, e.source); err != nil {
									return err
								}
							}
						}
					} else {
						if err := p.tableHist.WaitForTS(ctx, e.kv.Value.Timestamp); err != nil {
							return err
						}
						if err := p.buf.AddKV(ctx, e.kv, e.prevVal, e.schemaTimestamp, hlc.Timestamp{}, e.ddlEvent, e.source); err != nil {
							return err
						}
					}
				} else if e.resolved != nil {
					resolvedTS := e.resolved.Timestamp
					// Make sure scan boundaries less than or equal to `resolvedTS` were
					// added to the `scanBoundaries` list before proceeding.
					if err := p.tableHist.WaitForTS(ctx, resolvedTS); err != nil {
						return err
					}
					if withDDL {
						if tableID == keys.DescriptorTableID {
							continue
						}
						fileName := strconv.FormatInt(p.ca.spec.JobId, 10) + strconv.FormatInt(int64(tableID), 10)
						_, err := p.dumpSink.ReadFile(ctx, fileName)
						if err != nil {
							if _, ok := err.(*os.PathError); !ok {
								return err
							}
							p.mu.Lock()
							//当临时文件中数据已经发完，更新resolvedTmpMin为hlc.MaxTimestamp
							if *p.mu.resolvedTmpEntry[sqlbase.ID(tableID)] == 0 {
								p.mu.resolvedTmpMin[sqlbase.ID(tableID)] = hlc.MaxTimestamp
							}
							p.mu.Unlock()
						}
						{
							if dmlCache.Len() != 0 {
								fileName := strconv.FormatInt(p.ca.spec.JobId, 10) + strconv.FormatInt(int64(tableID), 10)
								err = dmlCache.FlushToFile(ctx, p.dumpSink, fileName)
								if err != nil {
									return err
								}
							}
						}
						p.mu.Lock()
						lastDMLTransactionCommitTime := p.mu.lastDMLTransactionCommitTime[sqlbase.ID(tableID)]
						lastDDLSendLocalTime := p.mu.lastDDLSendLocalTime[sqlbase.ID(tableID)]
						var sfTime hlc.Timestamp
						if recordFrontier, ok := recordFrontiers[sqlbase.ID(tableID)]; ok {
							recordFrontier.Forward(e.resolved.Span, resolvedTS)
							sfTime = recordFrontier.Frontier()
						} else {
							return errUnknownTableID
						}
						if !p.mu.blockTime[sqlbase.ID(tableID)].IsEmpty() && lastDMLTransactionCommitTime.LessEq(resolvedTS) && lastDMLTransactionCommitTime.LessEq(sfTime) && lastDDLSendLocalTime.LessEq(resolvedTS) {
							p.mu.resolvedSignal[sqlbase.ID(tableID)] = ChangeFeedExecDDL
						}
						p.mu.Unlock()
						if err := p.buf.AddResolved(ctx, e.resolved.Span, resolvedTS, false); err != nil {
							return err
						}
					} else {
						if err := p.buf.AddResolved(ctx, e.resolved.Span, resolvedTS, false); err != nil {
							return err
						}
					}
				}
			}
		})
	}

	if withDDL {
		for _, span := range p.spans {
			spanTmp := roachpb.Span{Key: span.Key, EndKey: span.EndKey}
			_, tableID, err := keys.DecodeTablePrefix(spanTmp.Key)
			if err != nil {
				return err
			}
			ddlEventRecord := DDLEvents{}
			g.GoCtx(func(ctx context.Context) error {
				for {
					var e bufferEntry
					if ddlMemBuf, ok := ddlMemBufs[sqlbase.ID(tableID)]; ok {
						e, err = ddlMemBuf.Get(ctx)
						if err != nil {
							return err
						}
					} else {
						return errUnknownTableID
					}
					output, err := appendEmitEntryForKV(ctx, e.kv, e.prevVal, e.kv.Value.Timestamp, p.leaseMgr, e.prevSchemaTimestamp)
					if err != nil {
						return err
					}
					DescProto, err := parseDescriptorFromValue(output, output.row.datums)
					if err != nil {
						return err
					}

					prevDescProto, err := parseDescriptorFromValue(output, output.row.prevDatums)
					if err != nil {
						return err
					}
					descTs := e.kv.Value.Timestamp
					preDescTs := e.prevSchemaTimestamp
					if preDescTs.IsEmpty() {
						preDescTs = descTs
					}
					ddlEvent, err := getDDLEventFromTableDescriptor(ctx, DescProto, prevDescProto, p.db, descTs, preDescTs)
					if err != nil {
						return err
					}
					if len(ddlEvent) == 0 {
						if len(ddlEventRecord) == 0 {
							e.ddlEvent.eventAll, e.ddlEvent.eventSend = 0, 0
							e.ddlEvent.Ts = tree.TimestampToDecimal(DescProto.Table(hlc.Timestamp{}).ModificationTime).Decimal.String()
							if err := p.buf.AddKV(ctx, e.kv, e.prevVal, e.schemaTimestamp, hlc.Timestamp{}, e.ddlEvent, e.source); err != nil {
								return err
							}
							continue
						}
					} else {
						//TODO:ADD CREATE DROP RENAME
						if ddlEvent[0].Operate.NeedContinue() {
							ddlEventRecord = ddlEvent
							continue
						} else {
							ddlEventRecord = ddlEvent
						}
					}

					changefeedDesc := DescProto.Table(hlc.Timestamp{})
					p.mu.Lock()
					p.mu.blockTime[changefeedDesc.ID] = changefeedDesc.ModificationTime
					p.mu.Unlock()
					for {
						p.mu.Lock()
						resolvedSignal := p.mu.resolvedSignal[changefeedDesc.ID]
						p.mu.Unlock()
						if resolvedSignal == ChangeFeedExecDDL {
							break
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						default:
							continue
						}
					}
					//	// Wait for send DDL.
					//	var resolvedTS, lastDMLTransactionCommitTime hlc.Timestamp
					for i := 0; i < len(ddlEventRecord); i++ {
						e.ddlEvent = ddlEventRecord[i]
						e.ddlEvent.eventAll = len(ddlEventRecord)
						e.ddlEvent.eventSend = i + 1
						e.ddlEvent.Ts = tree.TimestampToDecimal(DescProto.Table(hlc.Timestamp{}).ModificationTime).Decimal.String()
						if err := p.buf.AddKV(ctx, e.kv, e.prevVal, e.schemaTimestamp, hlc.Timestamp{}, e.ddlEvent, e.source); err != nil {
							return err
						}
					}

					fileName := strconv.FormatInt(p.ca.spec.JobId, 10) + strconv.FormatInt(int64(changefeedDesc.ID), 10)
					reader, err := p.dumpSink.ReadFile(ctx, fileName)
					if err != nil {
						if _, ok := err.(*os.PathError); !ok {
							return err
						}
					} else {
						csvReader := csv.NewReader(reader)
						for {
							buf, err := csvReader.ReadLine()
							if err == io.EOF {
								err := p.dumpSink.Delete(ctx, fileName)
								if err != nil {
									log.Errorf(ctx, "When deleting temporary files, an error was encountered:%s", err)
								}
								break
							}
							if err != nil {
								return err
							}
							entry := bufferEntry{}
							err = entry.UnmarshalJSON(buf)
							if err != nil {
								return err
							}
							mBuf := memBuffs[sqlbase.ID(tableID)]
							err = mBuf.AddKV(ctx, entry.kv, entry.prevVal, hlc.Timestamp{}, FromTmpFile)
							if err != nil {
								return err
							}
						}
					}
					p.mu.Lock()
					p.mu.blockTime[changefeedDesc.ID] = hlc.Timestamp{}
					if p.mu.resolvedSignal[changefeedDesc.ID] == ChangeFeedWaitToExecDDL {
						p.mu.resolvedSignal[changefeedDesc.ID] = ChangeFeedSyncDML
					} else {
						p.mu.resolvedSignal[changefeedDesc.ID] = ChangeFeedWaitToExecDDL
					}
					p.mu.Unlock()
					ddlEventRecord = DDLEvents{}
				}
			})
		}
	}

	// TODO(mrtracy): We are currently tearing down the entire rangefeed set in
	// order to perform a scan; however, given that we have an intermediate
	// buffer, its seems that we could do this without having to destroy and
	// recreate the rangefeeds.
	if err := g.Wait(); err != nil && err != errBoundaryReached {
		return err
	}
	// Resolve all of the spans as a boundary if the policy indicates that
	// we should do so.
	if p.SchemaChangePolicy != OptSchemaChangePolicyNoBackfill {
		for _, span := range p.spans {
			if err := p.buf.AddResolved(ctx, span, p.mu.highWater, true); err != nil {
				return err
			}
		}
	}
	// Exit if the policy says we should.
	if p.SchemaChangePolicy == OptSchemaChangePolicyStop {
		return schemaChangeDetectedError{p.mu.highWater.Next()}
	}
	return nil
}

//memBufClose关闭
func memBufClose(ctx context.Context, memBuffs map[sqlbase.ID]*memBuffer) {
	for _, memBuf := range memBuffs {
		memBuf.Close(ctx)
	}
}

// schemaChangeDetectedError is a sentinel error to indicate to Run() that the
// schema change is stopping due to a schema change. This is handy to trigger
// the context group to stop; the error is handled entirely in this package.
type schemaChangeDetectedError struct {
	ts hlc.Timestamp
}

func (e schemaChangeDetectedError) Error() string {
	return fmt.Sprintf("schema change deteceted at %v", e.ts)
}

func (p *poller) scanIfShould(ctx context.Context, initialScan bool, backfillWithDiff bool) error {
	var scanTime hlc.Timestamp
	p.mu.Lock()
	if len(p.mu.scanBoundaries) > 0 && p.mu.scanBoundaries[0].Equal(p.mu.highWater) {
		// Perform a full scan of the latest value of all keys as of the
		// boundary timestamp and consume the boundary.
		scanTime = p.mu.scanBoundaries[0]
		p.mu.scanBoundaries = p.mu.scanBoundaries[1:]
	}
	p.mu.Unlock()
	//if !initialScan && p.SchemaChangePolicy == OptSchemaChangePolicyNoBackfill {
	//	return nil
	//}
	if scanTime != (hlc.Timestamp{}) {
		// TODO(dan): Now that we no longer have the poller, we should stop using
		// ExportRequest and start using normal Scans.

		if err := p.exportSpansParallel(ctx, p.spans, scanTime, backfillWithDiff); err != nil {
			return err
		}
	}
	// NB: We don't update the highwater even though we've technically seen all
	// events for all spans at the previous highwater.Next(). We choose not to
	// because doing so would be wrong once we only backfill some tables.
	return nil
}

func getSpansToProcess(
	ctx context.Context, db *client.DB, targetSpans []roachpb.Span,
) ([]roachpb.Span, error) {
	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "fetching range descriptors")
	}

	type spanMarker struct{}
	type rangeMarker struct{}

	var spanCovering intervalicl.Ranges
	for _, span := range targetSpans {
		spanCovering = append(spanCovering, intervalicl.Range{
			Low:     []byte(span.Key),
			High:    []byte(span.EndKey),
			Payload: spanMarker{},
		})
	}

	var rangeCovering intervalicl.Ranges
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, intervalicl.Range{
			Low:     []byte(rangeDesc.StartKey),
			High:    []byte(rangeDesc.EndKey),
			Payload: rangeMarker{},
		})
	}

	chunks := intervalicl.OverlapCoveringMerge(
		[]intervalicl.Ranges{spanCovering, rangeCovering},
	)

	var requests []roachpb.Span
	for _, chunk := range chunks {
		if _, ok := chunk.Payload.([]interface{})[0].(spanMarker); !ok {
			continue
		}
		requests = append(requests, roachpb.Span{Key: chunk.Low, EndKey: chunk.High})
	}
	return requests, nil
}

func (p *poller) exportSpansParallel(
	ctx context.Context, spans []roachpb.Span, ts hlc.Timestamp, withDiff bool,
) error {
	// Export requests for the various watched spans are executed in parallel,
	// with a semaphore-enforced limit based on a cluster setting.
	maxConcurrentExports := clusterNodeCount(p.gossip) *
		int(storage.ExportRequestsLimit.Get(&p.settings.SV))
	exportsSem := make(chan struct{}, maxConcurrentExports)
	g := ctxgroup.WithContext(ctx)

	// atomicFinished is used only to enhance debugging messages.
	var atomicFinished int64

	for _, span := range spans {
		span := span

		// Wait for our semaphore.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case exportsSem <- struct{}{}:
		}

		g.GoCtx(func(ctx context.Context) error {
			defer func() { <-exportsSem }()

			err := p.exportSpan(ctx, span, ts, withDiff)
			finished := atomic.AddInt64(&atomicFinished, 1)
			if log.V(2) {
				log.Infof(ctx, `exported %d of %d`, finished, len(spans))
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func (p *poller) exportSpan(
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, withDiff bool,
) error {
	sender := p.db.NonTransactionalSender()
	if log.V(2) {
		log.Infof(ctx, `sending ExportRequest %s over %s`, span, ts)
	}

	header := roachpb.Header{Timestamp: ts}
	req := &roachpb.DumpRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(span),
		MVCCFilter:    roachpb.MVCCFilter_Latest,
		ReturnSST:     true,
		OmitChecksum:  true,
		//EnableTimeBoundIteratorOptimization: true,
	}
	//if isFullScan {
	//	req.MVCCFilter = roachpb.MVCCFilter_Latest
	//	req.StartTime = hlc.Timestamp{}
	//}

	stopwatchStart := timeutil.Now()
	exported, pErr := client.SendWrappedWith(ctx, sender, header, req)
	exportDuration := timeutil.Since(stopwatchStart)
	if log.V(2) {
		log.Infof(ctx, `finished ExportRequest %s over %s took %s`,
			span, ts.AsOfSystemTime(), exportDuration)
	}
	slowExportThreshold := 10 * cdcPollInterval.Get(&p.settings.SV)
	if exportDuration > slowExportThreshold {
		log.Infof(ctx, "finished ExportRequest %s over %s took %s behind by %s",
			span, ts, exportDuration, timeutil.Since(ts.GoTime()))
	}

	if pErr != nil {
		err := pErr.GoError()
		// TODO(dan): It'd be nice to avoid this string sniffing, if possible, but
		// pErr doesn't seem to have `Details` set, which would rehydrate the
		// BatchTimestampBeforeGCError.
		if strings.Contains(err.Error(), "must be after replica GC threshold") {
			err = MarkTerminalError(err)
		}
		return pgerror.Wrapf(err, pgcode.DataException,
			`fetching changes for %s`, span)
	}
	p.metrics.PollRequestNanosHist.RecordValue(exportDuration.Nanoseconds())

	// When outputting a full scan, we want to use the schema at the scan
	// timestamp, not the schema at the value timestamp.
	//var schemaTimestamp hlc.Timestamp
	//if isFullScan {
	//	schemaTimestamp = end
	//}
	schemaTimestamp := ts
	stopwatchStart = timeutil.Now()
	for _, file := range exported.(*roachpb.DumpResponse).Files {
		if err := p.slurpSST(ctx, file.SST, schemaTimestamp, withDiff); err != nil {
			return err
		}
	}
	if err := p.buf.AddResolved(ctx, span, ts, true); err != nil {
		return err
	}

	if log.V(2) {
		log.Infof(ctx, `finished buffering %s took %s`, span, timeutil.Since(stopwatchStart))
	}
	return nil
}

func (p *poller) updateTableHistory(ctx context.Context, endTS hlc.Timestamp) error {
	startTS := p.tableHist.HighWater()
	if !startTS.Less(endTS) {
		return nil
	}
	descs, err := fetchTableDescriptorVersions(ctx, p.db, startTS, endTS, p.details.Targets)
	if err != nil {
		return err
	}
	return p.tableHist.IngestDescriptors(ctx, startTS, endTS, descs)
}

func (p *poller) pollTableHistory(ctx context.Context) error {
	for {
		if err := p.updateTableHistory(ctx, p.clock.Now()); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(cdcPollInterval.Get(&p.settings.SV)):
		}

	}
}

// slurpSST iterates an encoded sst and inserts the contained kvs into the
// buffer.
func (p *poller) slurpSST(
	ctx context.Context, sst []byte, schemaTimestamp hlc.Timestamp, withDiff bool,
) error {
	var previousKey roachpb.Key
	var kvs []roachpb.KeyValue
	slurpKVs := func() error {
		sort.Sort(byValueTimestamp(kvs))
		for _, kv := range kvs {
			var prevVal roachpb.Value
			var prevSchemaTimestamp hlc.Timestamp
			if withDiff {
				// Include the same value for the "before" and "after" KV, but
				// interpret them at different timestamp. Specifically, interpret
				// the "before" KV at the timestamp immediately before the schema
				// change.
				prevVal = kv.Value
				prevSchemaTimestamp = schemaTimestamp.Prev()
			}
			if err := p.buf.AddKV(ctx, kv, prevVal, schemaTimestamp, prevSchemaTimestamp, DDLEvent{}, FromRangeFeed); err != nil {
				return err
			}
		}
		previousKey = previousKey[:0]
		kvs = kvs[:0]
		return nil
	}

	var scratch bufalloc.ByteAllocator
	it, err := engine.NewMemSSTIterator(sst, false /* verify */)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Seek(engine.NilKey); ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		unsafeKey := it.UnsafeKey()
		var key roachpb.Key
		var value []byte
		scratch, key = scratch.Copy(unsafeKey.Key, 0 /* extraCap */)
		scratch, value = scratch.Copy(it.UnsafeValue(), 0 /* extraCap */)

		// The buffer currently requires that each key's mvcc revisions are
		// added in increasing timestamp order. The sst is guaranteed to be in
		// key order, but decresing timestamp order. So, buffer up kvs until the
		// key changes, then sort by increasing timestamp before handing them
		// all to AddKV.
		if !previousKey.Equal(key) {
			if err := slurpKVs(); err != nil {
				return err
			}
			previousKey = key
		}
		kvs = append(kvs, roachpb.KeyValue{
			Key:   key,
			Value: roachpb.Value{RawBytes: value, Timestamp: unsafeKey.Timestamp},
		})
	}

	return slurpKVs()
}

type byValueTimestamp []roachpb.KeyValue

func (b byValueTimestamp) Len() int      { return len(b) }
func (b byValueTimestamp) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byValueTimestamp) Less(i, j int) bool {
	return b[i].Value.Timestamp.Less(b[j].Value.Timestamp)
}

func allRangeDescriptors(ctx context.Context, txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, err
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(g *gossip.Gossip) int {
	var nodes int
	_ = g.IterateInfos(gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
		nodes++
		return nil
	})
	return nodes
}

func (p *poller) validateTable(ctx context.Context, desc *sqlbase.TableDescriptor) error {
	if err := validateCDCTable(ctx, p.details.Targets, desc, nil); err != nil {
		return MarkTerminalError(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if lastVersion, ok := p.mu.previousTableVersion[desc.ID]; ok {
		if desc.ModificationTime.Less(lastVersion.ModificationTime) {
			return nil
		}
	}
	p.mu.previousTableVersion[desc.ID] = desc
	return nil
}

//func shouldAddScanBoundary(
//	lastVersion *sqlbase.TableDescriptor, desc *sqlbase.TableDescriptor,
//) (res bool) {
//	tableEvent := TableEvent{
//		Before: lastVersion,
//		After:  desc,
//	}
//	return newColumnBackfillComplete(tableEvent) ||
//		hasNewColumnDropBackfillMutation(tableEvent)
//}

//func hasNewColumnDropBackfillMutation(oldDesc, newDesc *sqlbase.TableDescriptor) (res bool) {
//	dropMutationExists := func(desc *sqlbase.TableDescriptor) bool {
//		for _, m := range desc.Mutations {
//			if m.Direction == sqlbase.DescriptorMutation_DROP &&
//				m.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
//				return true
//			}
//		}
//		return false
//	}
//	// Make sure that the old descriptor *doesn't* have the same mutation to avoid adding
//	// the same scan boundary more than once.
//	return !dropMutationExists(oldDesc) && dropMutationExists(newDesc)
//}

//func newColumnBackfillComplete(oldDesc, newDesc *sqlbase.TableDescriptor) (res bool) {
//	return len(oldDesc.Columns) < len(newDesc.Columns) &&
//		oldDesc.HasColumnBackfillMutation() && !newDesc.HasColumnBackfillMutation()
//}

func fetchSpansForTargets(
	ctx context.Context, db *client.DB, targets jobspb.ChangefeedTargets, ts hlc.Timestamp,
) ([]roachpb.Span, error) {
	var spans []roachpb.Span
	err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		spans = nil
		txn.SetFixedTimestamp(ctx, ts)
		// Note that all targets are currently guaranteed to be tables.
		for tableID := range targets {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
			if err != nil {
				return err
			}
			spans = append(spans, tableDesc.PrimaryIndexSpan())
		}
		return nil
	})
	return spans, err
}
