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
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type tableHistoryWaiter struct {
	ts    hlc.Timestamp
	errCh chan error
}

// tableHistory tracks that a some invariants hold over a set of tables as time
// advances.
//
// Internally, two timestamps are tracked. The high-water is the highest
// timestamp such that every version of a TableDescriptor has met a provided
// invariant (via `validateFn`). An error timestamp is also kept, which is the
// lowest timestamp where at least one table doesn't meet the invariant.
//
// The `WaitForTS` method allows a user to block until some given timestamp is
// less (or equal) to either the high-water or the error timestamp. In the
// latter case, it returns the error.
type tableHistory struct {
	validateFn func(context.Context, *sqlbase.TableDescriptor) error
	filter     tableEventFilter
	//// SchemaChangeEvents controls the class of events which are emitted by this
	//// SchemaFeed.
	//SchemaChangeEvents SchemaChangeEventClass
	mu struct {
		syncutil.Mutex

		// the highest known valid timestamp
		highWater hlc.Timestamp

		// the lowest known invalid timestamp
		errTS hlc.Timestamp

		// the error associated with errTS
		err error

		// callers waiting on a timestamp to be resolved as valid or invalid
		waiters []tableHistoryWaiter
		//// events is a sorted list of table events which have not been popped and
		//// are at or below highWater.
		//events []TableEvent
		//
		//// previousTableVersion is a map from tableID to the most recent version
		//// of the table descriptor seen by the poller. This is needed to determine
		//// when a backilling mutation has successfully completed - this can only
		//// be determining by comparing a version to the previous version.
		//previousTableVersion map[sqlbase.ID]*sqlbase.TableDescriptor
	}
}

// makeTableHistory creates tableHistory with the given initial high-water and
// invariant check function. It is expected that `validateFn` is deterministic.
func makeTableHistory(
	validateFn func(context.Context, *sqlbase.TableDescriptor) error,
	initialHighWater hlc.Timestamp,
	SchemaChangeEvents SchemaChangeEventClass,
) *tableHistory {
	m := &tableHistory{validateFn: validateFn, filter: schemaChangeEventFilters[SchemaChangeEvents]}
	m.mu.highWater = initialHighWater
	return m
}

// HighWater returns the current high-water timestamp.
func (th *tableHistory) HighWater() hlc.Timestamp {
	th.mu.Lock()
	highWater := th.mu.highWater
	th.mu.Unlock()
	return highWater
}

// WaitForTS blocks until the given timestamp is less than or equal to the
// high-water or error timestamp. In the latter case, the error is returned.
//
// If called twice with the same timestamp, two different errors may be returned
// (since the error timestamp can recede). However, the return for a given
// timestamp will never switch from nil to an error or vice-versa (assuming that
// `validateFn` is deterministic and the ingested descriptors are read
// transactionally).
func (th *tableHistory) WaitForTS(ctx context.Context, ts hlc.Timestamp) error {
	var errCh chan error

	th.mu.Lock()
	highWater := th.mu.highWater
	var err error
	if th.mu.errTS != (hlc.Timestamp{}) && !ts.Less(th.mu.errTS) {
		err = th.mu.err
	}
	fastPath := err != nil || !highWater.Less(ts)
	if !fastPath {
		errCh = make(chan error, 1)
		th.mu.waiters = append(th.mu.waiters, tableHistoryWaiter{ts: ts, errCh: errCh})
	}
	th.mu.Unlock()
	if fastPath {
		if log.V(1) {
			log.Infof(ctx, "fastpath for %s: %v", ts, err)
		}
		return err
	}

	if log.V(1) {
		log.Infof(ctx, "waiting for %s highwater", ts)
	}
	start := timeutil.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if log.V(1) {
			log.Infof(ctx, "waited %s for %s highwater: %v", timeutil.Since(start), ts, err)
		}
		return err
	}
}

// IngestDescriptors checks the given descriptors against the invariant check
// function and adjusts the high-water or error timestamp appropriately. It is
// required that the descriptors represent a transactional kv read between the
// two given timestamps.
func (th *tableHistory) IngestDescriptors(
	ctx context.Context, startTS, endTS hlc.Timestamp, descs []*sqlbase.TableDescriptor,
) error {
	sort.Slice(descs, func(i, j int) bool {
		return descs[i].ModificationTime.Less(descs[j].ModificationTime)
	})
	var validateErr error
	for _, desc := range descs {
		if err := th.validateFn(ctx, desc); validateErr == nil {
			validateErr = err
		}
	}
	return th.adjustTimestamps(startTS, endTS, validateErr)
}

// adjustTimestamps adjusts the high-water or error timestamp appropriately.
func (th *tableHistory) adjustTimestamps(startTS, endTS hlc.Timestamp, validateErr error) error {
	th.mu.Lock()
	defer th.mu.Unlock()

	if validateErr != nil {
		// don't care about startTS in the invalid case
		if th.mu.errTS == (hlc.Timestamp{}) || endTS.Less(th.mu.errTS) {
			th.mu.errTS = endTS
			th.mu.err = validateErr
			newWaiters := make([]tableHistoryWaiter, 0, len(th.mu.waiters))
			for _, w := range th.mu.waiters {
				if w.ts.Less(th.mu.errTS) {
					newWaiters = append(newWaiters, w)
					continue
				}
				w.errCh <- validateErr
			}
			th.mu.waiters = newWaiters
		}
		return validateErr
	}

	if th.mu.highWater.Less(startTS) {
		return errors.Errorf(`gap between %s and %s`, th.mu.highWater, startTS)
	}
	if th.mu.highWater.Less(endTS) {
		th.mu.highWater = endTS
		newWaiters := make([]tableHistoryWaiter, 0, len(th.mu.waiters))
		for _, w := range th.mu.waiters {
			if th.mu.highWater.Less(w.ts) {
				newWaiters = append(newWaiters, w)
				continue
			}
			w.errCh <- nil
		}
		th.mu.waiters = newWaiters
	}
	return nil
}

type tableHistoryUpdater struct {
	settings *cluster.Settings
	db       *client.DB
	targets  jobspb.ChangefeedTargets
	m        *tableHistory
}

func (u *tableHistoryUpdater) PollTableDescriptors(ctx context.Context) error {
	// TODO(dan): Replace this with a RangeFeed once it stabilizes.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cdcPollInterval.Get(&u.settings.SV)):
		}

		startTS, endTS := u.m.HighWater(), u.db.Clock().Now()
		if !startTS.Less(endTS) {
			continue
		}
		descs, err := fetchTableDescriptorVersions(ctx, u.db, startTS, endTS, u.targets)
		if err != nil {
			return err
		}
		if err := u.m.IngestDescriptors(ctx, startTS, endTS, descs); err != nil {
			return err
		}
	}
}

func fetchTableDescriptorVersions(
	ctx context.Context,
	db *client.DB,
	startTS, endTS hlc.Timestamp,
	targets jobspb.ChangefeedTargets,
) ([]*sqlbase.TableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, `fetching table descs (%s,%s]`, startTS, endTS)
	}
	start := timeutil.Now()
	span := roachpb.Span{Key: keys.MakeTablePrefix(keys.DescriptorTableID)}
	span.EndKey = span.Key.PrefixEnd()
	header := roachpb.Header{Timestamp: endTS}
	req := &roachpb.DumpRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(span),
		StartTime:     startTS,
		MVCCFilter:    roachpb.MVCCFilter_All,
		ReturnSST:     true,
		OmitChecksum:  true,
		// TODO(dan): Remove this in a PR separate from the one that disables
		// time-bound iterators for BACKUP.
		EnableTimeBoundIteratorOptimization: true,
	}
	res, pErr := client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
	if log.V(2) {
		log.Infof(ctx, `fetched table descs (%s,%s] took %s`, startTS, endTS, timeutil.Since(start))
	}
	if pErr != nil {
		err := pErr.GoError()
		// TODO(dan): It'd be nice to avoid this string sniffing, if possible, but
		// pErr doesn't seem to have `Details` set, which would rehydrate the
		// BatchTimestampBeforeGCError.
		if strings.Contains(err.Error(), "must be after replica GC threshold") {
			err = MarkTerminalError(err)
		}
		return nil, pgerror.Wrapf(err, pgcode.DataException, `fetching changes for %s`, span)
	}

	var tableDescs []*sqlbase.TableDescriptor
	for _, file := range res.(*roachpb.DumpResponse).Files {
		if err := func() error {
			it, err := engine.NewMemSSTIterator(file.SST, false /* verify */)
			if err != nil {
				return err
			}
			defer it.Close()
			for it.Seek(engine.NilKey); ; it.Next() {
				if ok, err := it.Valid(); err != nil {
					return err
				} else if !ok {
					return nil
				}
				remaining, _, _, err := sqlbase.DecodeTableIDIndexID(it.UnsafeKey().Key)
				if err != nil {
					return err
				}
				_, tableID, err := encoding.DecodeUvarintAscending(remaining)
				if err != nil {
					return err
				}
				origName, ok := targets[sqlbase.ID(tableID)]
				if !ok {
					// Uninteresting table.
					continue
				}
				unsafeValue := it.UnsafeValue()
				if unsafeValue == nil {
					return errors.Errorf(`"%v" was dropped or truncated`, origName)
				}
				value := roachpb.Value{RawBytes: unsafeValue}
				var desc sqlbase.Descriptor
				if err := value.GetProto(&desc); err != nil {
					return err
				}
				if tableDesc := desc.Table(it.UnsafeKey().Timestamp); tableDesc != nil {
					tableDescs = append(tableDescs, tableDesc)
				}
			}
		}(); err != nil {
			return nil, err
		}
	}
	return tableDescs, nil
}

//// Peek returns all events which have not been popped which happen at or
//// before the passed timestamp.
//func (th *tableHistory) Peek(
//	ctx context.Context, atOrBefore hlc.Timestamp,
//) (events []TableEvent, err error) {
//
//	return th.peekOrPop(ctx, atOrBefore, false /* pop */)
//}
//
//// Pop pops events from the EventQueue.
//func (th *tableHistory) Pop(
//	ctx context.Context, atOrBefore hlc.Timestamp,
//) (events []TableEvent, err error) {
//	return th.peekOrPop(ctx, atOrBefore, true /* pop */)
//}
//
//func (th *tableHistory) peekOrPop(
//	ctx context.Context, atOrBefore hlc.Timestamp, pop bool,
//) (events []TableEvent, err error) {
//	if err = th.WaitForTS(ctx, atOrBefore); err != nil {
//		return nil, err
//	}
//	th.mu.Lock()
//	defer th.mu.Unlock()
//	i := sort.Search(len(th.mu.events), func(i int) bool {
//		return !th.mu.events[i].Timestamp().LessEq(atOrBefore)
//	})
//	if i == -1 {
//		i = 0
//	}
//	events = th.mu.events[:i]
//	if pop {
//		th.mu.events = th.mu.events[i:]
//	}
//	return events, nil
//}
