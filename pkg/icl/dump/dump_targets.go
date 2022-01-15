package dump

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/build"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/icl/utilicl/intervalicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/interval"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// DumpCheckpointInterval is the interval at which dump progress is saved
// to durable storage.
var DumpCheckpointInterval = time.Minute

func startDumpJob(
	ctx context.Context,
	p sql.PlanHookState,
	dumpStmt *annotatedBackupStatement,
	toFile string,
	incrementalFrom []string,
	opts map[string]string,
	endTime hlc.Timestamp,
	resultsCh chan<- tree.Datums,
) error {

	requireVersion2 := false

	mvccFilter := MVCCFilter_Latest
	dumpDropTable := false
	if _, ok := opts[dumpOptRevisionHistory]; ok {
		dumpDropTable = true
		mvccFilter = MVCCFilter_All
		requireVersion2 = true
	}
	var codec roachpb.FileCompression
	if name, ok := opts[dumpOptionCompression]; ok && len(name) != 0 {
		if strings.EqualFold(name, dumpCompressionCodec) {
			codec = roachpb.FileCompression_Gzip
		} else {
			return pgerror.NewError(pgcode.InvalidParameterValue, fmt.Sprintf("unsupported compression codec %s", name))
		}
	}
	var encryptionPassphrase []byte
	if passphrase, ok := opts[dumpOptEncPassphrase]; ok {
		encryptionPassphrase = []byte(passphrase)
	}
	var online bool
	if _, ok := opts[dumpOptionOnline]; ok {
		online = true
	}
	var header string
	if override, ok := opts[dumpOptionHTTPHeader]; ok {
		header = override
	}
	targetDescs, completeDBs, err := ResolveTargetsToDescriptors(ctx, p, endTime, dumpStmt.Targets, dumpStmt.DescriptorCoverage, dumpDropTable)
	if err != nil {
		return err
	}

	//*
	var tables []*sqlbase.TableDescriptor
	schemas := make(map[sqlbase.ID]sqlbase.ID)
	var id int32
	for _, desc := range targetDescs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if err := p.CheckPrivilege(ctx, dbDesc, privilege.USAGE); err != nil {
				return err
			}
			if dumpStmt.Targets.Databases != nil {
				id = int32(dbDesc.ID)
			}
		}
		if scDesc := desc.GetSchema(); scDesc != nil {
			if err := p.CheckPrivilege(ctx, scDesc, privilege.USAGE); err != nil {
				return err
			}
			if dumpStmt.Targets.Schemas != nil {
				id = int32(scDesc.ID)
			}
			schemas[scDesc.ID] = scDesc.ParentID
		}
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			if err := p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
				return err
			}
			if dumpStmt.Targets.Tables != nil {
				id = int32(tableDesc.ID)
			}
			tables = append(tables, tableDesc)
		}
	}

	if err := ensureInterleavesIncluded(tables); err != nil {
		return err
	}
	var encryption *roachpb.FileEncryptionOptions
	var prevDumps []DumpDescriptor
	if len(incrementalFrom) > 0 {
		if encryptionPassphrase != nil {
			exportStore, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, incrementalFrom[0])
			if err != nil {
				return err
			}
			defer exportStore.Close()
			if exportStore.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
				err := exportStore.SetConfig(header)
				if err != nil {
					return err
				}
			}
			opts, err := ReadEncryptionOptions(ctx, exportStore)
			if err != nil {
				return err
			}
			encryption = &roachpb.FileEncryptionOptions{
				Key: storageicl.GenerateKey(encryptionPassphrase, opts.Salt),
			}
		}
		clusterID := p.ExecCfg().ClusterID()
		prevDumps = make([]DumpDescriptor, len(incrementalFrom))
		for i, uri := range incrementalFrom {
			desc, err := ReadDumpDescriptorFromURI(ctx, uri, p.ExecCfg().DistSQLSrv.DumpSinkFromURI, encryption, header)
			if err != nil {
				return errors.Wrapf(err, "failed to read dump from %q", uri)
			}
			// IDs are how we identify tables, and those are only meaningful in the
			// context of their own cluster, so we need to ensure we only allow
			// incremental previous dumps that we created.
			if !desc.ClusterID.Equal(clusterID) {
				return errors.Errorf("previous DUMP %q belongs to cluster %s", uri, desc.ClusterID.String())
			}
			prevDumps[i] = desc
		}
	}

	var startTime hlc.Timestamp
	var newSpans roachpb.Spans
	if len(prevDumps) > 0 {
		startTime = prevDumps[len(prevDumps)-1].EndTime
	}

	var priorIDs map[sqlbase.ID]sqlbase.ID

	var revs []DumpDescriptor_DescriptorRevision
	if mvccFilter == MVCCFilter_All {
		var err error
		priorIDs = make(map[sqlbase.ID]sqlbase.ID)
		revs, err = getRelevantDescChanges(ctx, p.ExecCfg().DB, startTime, endTime, targetDescs, completeDBs, priorIDs)
		if err != nil {
			return err
		}
	}

	spans := spansForAllTableIndexes(tables, revs)

	if len(prevDumps) > 0 {
		tablesInPrev := make(map[sqlbase.ID]struct{})
		dbsInPrev := make(map[sqlbase.ID]struct{})
		for _, d := range prevDumps[len(prevDumps)-1].Descriptors {
			if t := d.Table(hlc.Timestamp{}); t != nil {
				tablesInPrev[t.ID] = struct{}{}
			}
		}
		for _, d := range prevDumps[len(prevDumps)-1].CompleteDbs {
			dbsInPrev[d] = struct{}{}
		}

		for _, d := range targetDescs {
			if t := d.Table(hlc.Timestamp{}); t != nil {
				// If we're trying to use a previous dump for this table, ideally it
				// actually contains this table.
				if _, ok := tablesInPrev[t.ID]; ok {
					continue
				}

				parentID := t.ParentID
				if parentID != sqlbase.SystemDB.ID {
					parentID = schemas[parentID]
					if parentID == sqlbase.InvalidID {
						continue
					}
				}
				// This table isn't in the previous dump... maybe was added to a
				// DB that the previous dump captured?
				if _, ok := dbsInPrev[parentID]; ok {
					continue
				}
				// Maybe this table is missing from the previous dump because it was
				// truncated?
				if t.ReplacementOf.ID != sqlbase.InvalidID {

					// Check if we need to lazy-load the priorIDs (i.e. if this is the first
					// truncate we've encountered in non-MVCC dump).
					if priorIDs == nil {
						priorIDs = make(map[sqlbase.ID]sqlbase.ID)
						_, err := getAllDescChanges(ctx, p.ExecCfg().DB, startTime, endTime, priorIDs)
						if err != nil {
							return err
						}
					}
					found := false
					for was := t.ReplacementOf.ID; was != sqlbase.InvalidID && !found; was = priorIDs[was] {
						_, found = tablesInPrev[was]
					}
					if found {
						continue
					}
				}
				return errors.Errorf("previous dump does not contain table %q", t.Name)
			}
		}

		var err error
		_, coveredTime, err := makeImportSpans(spans, prevDumps, keys.MinKey,
			func(span intervalicl.Range, start, end hlc.Timestamp) error {
				if (start == hlc.Timestamp{}) {
					newSpans = append(newSpans, roachpb.Span{Key: span.Low, EndKey: span.High})
					return nil
				}
				return errOnMissingRange(span, start, end)
			})
		if err != nil {
			return errors.Wrap(err, "invalid previous dumps (a new full dump may be required if a table has been created, dropped or truncated)")
		}
		if coveredTime != startTime {
			return errors.Errorf("expected previous dumps to cover until time %v, got %v", startTime, coveredTime)
		}
	}

	// older nodes don't know about many new fields, e.g. MVCCAll and may
	// incorrectly evaluate either an export RPC, or a resumed dump job.
	if requireVersion2 && !p.ExecCfg().Settings.Version.IsActive(cluster.Version2_0) {
		return errors.Errorf(
			"DUMP features introduced in 2.0 requires cluster version >= %s (",
			cluster.VersionByKey(cluster.Version2_0).String(),
		)
	}
	// if CompleteDbs is lost by a 1.x node, FormatDescriptorTrackingVersion
	// means that a 2.0 node will disallow `RESTORE DATABASE foo`, but `RESTORE
	// foo.table1, foo.table2...` will still work. MVCCFilter would be
	// mis-handled, but is disallowed above. IntroducedSpans may also be lost by
	// a 1.x node, meaning that if 1.1 nodes may resume a dump, the limitation
	// of requiring full dumps after schema changes remains.

	dumpDesc := DumpDescriptor{
		StartTime:          startTime,
		EndTime:            endTime,
		MVCCFilter:         mvccFilter,
		Descriptors:        targetDescs,
		DescriptorChanges:  revs,
		CompleteDbs:        completeDBs,
		Spans:              spans,
		IntroducedSpans:    newSpans,
		FormatVersion:      DumpFormatDescriptorTrackingVersion,
		BuildInfo:          build.GetInfo(),
		NodeID:             p.ExecCfg().NodeID.Get(),
		ClusterID:          p.ExecCfg().ClusterID(),
		DescriptorCoverage: dumpStmt.DescriptorCoverage,
	}

	// Sanity check: re-run the validation that RESTORE will do, but this time
	// including this dump, to ensure that the this dump plus any previous
	// dumps does cover the interval expected.
	if _, coveredEnd, err := makeImportSpans(
		spans, append(prevDumps, dumpDesc), keys.MinKey, errOnMissingRange,
	); err != nil {
		return err
	} else if coveredEnd != endTime {
		return errors.Errorf("expected dump (along with any previous dumps) to cover to %v, not %v", endTime, coveredEnd)
	}

	descBytes, err := protoutil.Marshal(&dumpDesc)
	if err != nil {
		return err
	}
	// If we didn't load any prior backups from which get encryption info, we
	// need to pick a new salt and record it.
	if encryptionPassphrase != nil && encryption == nil {
		salt, err := storageicl.GenerateSalt()
		if err != nil {
			return err
		}
		exportStore, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, toFile)
		if err != nil {
			return err
		}
		defer exportStore.Close()
		if exportStore.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
			err := exportStore.SetConfig(header)
			if err != nil {
				return err
			}
		}
		if err := writeEncryptionOptions(ctx, &EncryptionInfo{Salt: salt}, exportStore); err != nil {
			return err
		}
		encryption = &roachpb.FileEncryptionOptions{Key: storageicl.GenerateKey(encryptionPassphrase, salt)}
	}
	description, err := JobDescription(dumpStmt.Dump, toFile, incrementalFrom, opts)
	if err != nil {
		return err
	}
	dumpDetail := jobspb.DumpDetails{
		StartTime:        startTime,
		EndTime:          endTime,
		URI:              toFile,
		DumpDescriptor:   descBytes,
		Encryption:       encryption,
		CompressionCodec: codec,
		DumpOnline:       online,
		HttpHeader:       header,
	}

	_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
		Description: description,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
			for _, sqlDesc := range dumpDesc.Descriptors {
				sqlDescIDs = append(sqlDescIDs, sqlDesc.GetID())
			}
			return sqlDescIDs
		}(),
		Details:   dumpDetail,
		Progress:  jobspb.DumpProgress{},
		CreatedBy: dumpStmt.CreatedByInfo,
	})
	if err != nil {
		return err
	}
	log.Info(ctx, "Start Backup Job")
	*p.GetAuditInfo() = server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(sql.EventLogDump),
		TargetInfo: &server.TargetInfo{
			TargetID: id,
		},
		Info: "User name:" + p.User() + " Statement:" + description,
	}
	return <-errCh
	//*/return nil
}

//*
func ensureInterleavesIncluded(tables []*sqlbase.TableDescriptor) error {
	inDump := make(map[sqlbase.ID]bool, len(tables))
	for _, t := range tables {
		inDump[t.ID] = true
	}

	for _, table := range tables {
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			for _, a := range index.Interleave.Ancestors {
				if !inDump[a.TableID] {
					return errors.Errorf(
						"cannot dump table %q without interleave parent (ID %d)", table.Name, a.TableID,
					)
				}
			}
			for _, c := range index.InterleavedBy {
				if !inDump[c.Table] {
					return errors.Errorf(
						"cannot dump table %q without interleave child table (ID %d)", table.Name, c.Table,
					)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

type tableAndIndex struct {
	tableID sqlbase.ID
	indexID sqlbase.IndexID
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
func spansForAllTableIndexes(
	tables []*sqlbase.TableDescriptor, revs []DumpDescriptor_DescriptorRevision,
) []roachpb.Span {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	for _, table := range tables {
		for _, index := range table.AllNonDropIndexes() {
			if err := sstIntervalTree.Insert(intervalSpan(table.IndexSpan(index.ID)), false); err != nil {
				panic(errors.Wrap(err, "IndexSpan"))
			}
			added[tableAndIndex{tableID: table.ID, indexID: index.ID}] = true
		}
	}
	// If there are desc revisions, ensure that we also add any index spans
	// in them that we didn't already get above e.g. indexes or tables that are
	// not in latest because they were dropped during the time window in question.
	for _, rev := range revs {
		if tbl := rev.Desc.Table(hlc.Timestamp{}); tbl != nil {
			for _, idx := range tbl.AllNonDropIndexes() {
				key := tableAndIndex{tableID: tbl.ID, indexID: idx.ID}
				if !added[key] {
					if err := sstIntervalTree.Insert(intervalSpan(tbl.IndexSpan(idx.ID)), false); err != nil {
						panic(errors.Wrap(err, "IndexSpan"))
					}
					added[key] = true
				}
			}
		}
	}

	var spans []roachpb.Span
	_ = sstIntervalTree.Do(func(r interval.Interface) bool {
		spans = append(spans, roachpb.Span{
			Key:    roachpb.Key(r.Range().Start),
			EndKey: roachpb.Key(r.Range().End),
		})
		return false
	})
	return spans
}

// getRelevantDescChanges finds the changes between start and end time to the
// SQL descriptors matching `descs` or `expandedDBs`, ordered by time. A
// descriptor revision matches if it is an earlier revision of a descriptor in
// descs (same ID) or has parentID in `expanded`. Deleted descriptors are
// represented as nil. Fills in the `priorIDs` map in the process, which maps
// a descriptor the the ID by which it was previously known (e.g pre-TRUNCATE).
func getRelevantDescChanges(
	ctx context.Context,
	db *client.DB,
	startTime, endTime hlc.Timestamp,
	descs []sqlbase.Descriptor,
	expanded []sqlbase.ID,
	priorIDs map[sqlbase.ID]sqlbase.ID,
) ([]DumpDescriptor_DescriptorRevision, error) {

	allChanges, err := getAllDescChanges(ctx, db, startTime, endTime, priorIDs)
	if err != nil {
		return nil, err
	}

	// If no descriptors changed, we can just stop now and have RESTORE use the
	// normal list of descs (i.e. as of endTime).
	if len(allChanges) == 0 {
		return nil, nil
	}

	// interestingChanges will be every descriptor change relevant to the dump.
	var interestingChanges []DumpDescriptor_DescriptorRevision

	// interestingIDs are the descriptor for which we're interested in capturing
	// changes. This is initially the descriptors matched (as of endTime) by our
	// target spec, plus those that belonged to a DB that our spec expanded at any
	// point in the interval.
	interestingIDs := make(map[sqlbase.ID]struct{}, len(descs))

	// The descriptors that currently (endTime) match the target spec (desc) are
	// obviously interesting to our dump.
	for _, i := range descs {
		interestingIDs[i.GetID()] = struct{}{}
		if t := i.Table(hlc.Timestamp{}); t != nil {
			for j := t.ReplacementOf.ID; j != sqlbase.InvalidID; j = priorIDs[j] {
				interestingIDs[j] = struct{}{}
			}
		}
	}

	// We're also interested in any desc that belonged to a DB we're backing up.
	// We'll start by looking at all descriptors as of the beginning of the
	// interval and add to the set of IDs that we are interested any descriptor that
	// belongs to one of the parents we care about.
	interestingParents := make(map[sqlbase.ID]struct{}, len(expanded))
	for _, i := range expanded {
		interestingParents[i] = struct{}{}
	}

	if !startTime.IsEmpty() {
		starting, err := loadAllDescs(ctx, db, startTime)
		if err != nil {
			return nil, err
		}
		for _, i := range starting {
			if table := i.Table(hlc.Timestamp{}); table != nil {
				// We need to add to interestingIDs so that if we later see a delete for
				// this ID we still know it is interesting to us, even though we will not
				// have a parentID at that point (since the delete is a nil desc).
				if _, ok := interestingParents[table.ParentID]; ok {
					interestingIDs[table.ID] = struct{}{}
				}
			}
			if _, ok := interestingIDs[i.GetID()]; ok {
				desc := i
				// We inject a fake "revision" that captures the starting state for
				// matched descriptor, to allow restoring to times before its first rev
				// actually inside the window. This likely ends up duplicating the last
				// version in the previous DUMP descriptor, but avoids adding more
				// complicated special-cases in RESTORE, so it only needs to look in a
				// single DUMP to restore to a particular time.
				initial := DumpDescriptor_DescriptorRevision{Time: startTime, ID: i.GetID(), Desc: &desc}
				interestingChanges = append(interestingChanges, initial)
			}
		}
	}

	for _, change := range allChanges {
		// A change to an ID that we are interested in is obviously interesting --
		// a change is also interesting if it is to a table that has a parent that
		// we are interested and thereafter it also becomes an ID in which we are
		// interested in changes (since, as mentioned above, to decide if deletes
		// are interesting).
		if _, ok := interestingIDs[change.ID]; ok {
			interestingChanges = append(interestingChanges, change)
		} else if change.Desc != nil {
			if table := change.Desc.Table(hlc.Timestamp{}); table != nil {
				if _, ok := interestingParents[table.ParentID]; ok {
					interestingIDs[table.ID] = struct{}{}
					interestingChanges = append(interestingChanges, change)
				}
			}
		}
	}

	sort.Slice(interestingChanges, func(i, j int) bool {
		return interestingChanges[i].Time.Less(interestingChanges[j].Time)
	})

	return interestingChanges, nil
}

// getAllDescChanges gets every sql descriptor change between start and end time
// returning its ID, content and the change time (with deletions represented as
// nil content).
func getAllDescChanges(
	ctx context.Context,
	db *client.DB,
	startTime, endTime hlc.Timestamp,
	priorIDs map[sqlbase.ID]sqlbase.ID,
) ([]DumpDescriptor_DescriptorRevision, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()

	allRevs, err := getAllRevisions(ctx, db, startKey, endKey, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var res []DumpDescriptor_DescriptorRevision

	for _, revs := range allRevs {
		id, err := keys.DecodeDescMetadataID(revs.Key)
		if err != nil {
			return nil, err
		}
		for _, rev := range revs.Values {
			r := DumpDescriptor_DescriptorRevision{ID: sqlbase.ID(id), Time: rev.Timestamp}
			if len(rev.RawBytes) != 0 {
				var desc sqlbase.Descriptor
				if err := rev.GetProto(&desc); err != nil {
					return nil, err
				}
				r.Desc = &desc
				if t := desc.Table(rev.Timestamp); t != nil && t.ReplacementOf.ID != sqlbase.InvalidID {
					priorIDs[t.ID] = t.ReplacementOf.ID
				}
			}
			res = append(res, r)
		}
	}
	return res, nil
}

type importEntryType int

const (
	dumpSpan importEntryType = iota
	dumpFile
	tableSpan
	completedSpan
	request
)

type importEntry struct {
	roachpb.Span
	entryType importEntryType

	// Only set if entryType is dumpSpan
	start, end hlc.Timestamp

	// Only set if entryType is dumpFile
	dir  roachpb.DumpSink
	file DumpDescriptor_File

	// Only set if entryType is request
	files []roachpb.LoadRequest_File

	// for progress tracking we assign the spans numbers as they can be executed
	// out-of-order based on splitAndScatter's scheduling.
	progressIdx int
}

// makeImportSpans pivots the dumps, which are grouped by time, into
// spans for import, which are grouped by keyrange.
//
// The core logic of this is in OverlapCoveringMerge, which accepts sets of
// non-overlapping key ranges (aka coverings) each with a payload, and returns
// them aligned with the payloads in the same order as in the input.
//
// Example (input):
// - [A, C) dump t0 to t1 -> /file1
// - [C, D) dump t0 to t1 -> /file2
// - [A, B) dump t1 to t2 -> /file3
// - [B, C) dump t1 to t2 -> /file4
// - [C, D) dump t1 to t2 -> /file5
// - [B, D) requested table data to be restored
//
// Example (output):
// - [A, B) -> /file1, /file3
// - [B, C) -> /file1, /file4, requested (note that file1 was split into two ranges)
// - [C, D) -> /file2, /file5, requested
//
// This would be turned into two Import spans, one restoring [B, C) out of
// /file1 and /file3, the other restoring [C, D) out of /file2 and /file5.
// Nothing is restored out of /file3 and only part of /file1 is used.
//
// NB: All grouping operates in the pre-rewrite keyspace, meaning the keyranges
// as they were backed up, not as they're being restored.
//
// If a span is not covered, the onMissing function is called with the span and
// time missing to determine what error, if any, should be returned.
//func makeImportSpans(
//	tableSpans []roachpb.Span,
//	dumps []DumpDescriptor,
//	lowWaterMark roachpb.Key,
//	onMissing func(span intervalccl.Range, start, end hlc.Timestamp) error,
//) ([]importEntry, hlc.Timestamp, error) {
//	// Put the covering for the already-completed spans into the
//	// OverlapCoveringMerge input first. Payloads are returned in the same order
//	// that they appear in the input; putting the completedSpan first means we'll
//	// see it first when iterating over the output of OverlapCoveringMerge and
//	// avoid doing unnecessary work.
//	completedCovering := intervalccl.Covering{
//		{
//			Start:   []byte(keys.MinKey),
//			End:     []byte(lowWaterMark),
//			Payload: importEntry{entryType: completedSpan},
//		},
//	}
//
//	// Put the merged table data covering into the OverlapCoveringMerge input
//	// next.
//	var tableSpanCovering intervalccl.Covering
//	for _, span := range tableSpans {
//		tableSpanCovering = append(tableSpanCovering, intervalccl.Range{
//			Start: span.Key,
//			End:   span.EndKey,
//			Payload: importEntry{
//				Span:      span,
//				entryType: tableSpan,
//			},
//		})
//	}
//
//	dumpCoverings := []intervalccl.Covering{completedCovering, tableSpanCovering}
//
//	// Iterate over dumps creating two coverings for each. First the spans
//	// that were backed up, then the files in the dump. The latter is a subset
//	// when some of the keyranges in the former didn't change since the previous
//	// dump. These alternate (dump1 spans, dump1 files, dump2 spans,
//	// dump2 files) so they will retain that alternation in the output of
//	// OverlapCoveringMerge.
//	var maxEndTime hlc.Timestamp
//	for _, b := range dumps {
//		if maxEndTime.Less(b.EndTime) {
//			maxEndTime = b.EndTime
//		}
//
//		var dumpNewSpanCovering intervalccl.Covering
//		for _, s := range b.IntroducedSpans {
//			dumpNewSpanCovering = append(dumpNewSpanCovering, intervalccl.Range{
//				Start:   s.Key,
//				End:     s.EndKey,
//				Payload: importEntry{Span: s, entryType: dumpSpan, start: hlc.Timestamp{}, end: b.StartTime},
//			})
//		}
//		dumpCoverings = append(dumpCoverings, dumpNewSpanCovering)
//
//		var dumpSpanCovering intervalccl.Covering
//		for _, s := range b.Spans {
//			dumpSpanCovering = append(dumpSpanCovering, intervalccl.Range{
//				Start:   s.Key,
//				End:     s.EndKey,
//				Payload: importEntry{Span: s, entryType: dumpSpan, start: b.StartTime, end: b.EndTime},
//			})
//		}
//		dumpCoverings = append(dumpCoverings, dumpSpanCovering)
//		var dumpFileCovering intervalccl.Covering
//		for _, f := range b.Files {
//			dumpFileCovering = append(dumpFileCovering, intervalccl.Range{
//				Start: f.Span.Key,
//				End:   f.Span.EndKey,
//				Payload: importEntry{
//					Span:      f.Span,
//					entryType: dumpFile,
//					dir:       b.Dir,
//					file:      f,
//				},
//			})
//		}
//		dumpCoverings = append(dumpCoverings, dumpFileCovering)
//	}
//
//	// Group ranges covered by dumps with ones needed to restore the selected
//	// tables. Note that this breaks intervals up as necessary to align them.
//	// See the function godoc for details.
//	importRanges := intervalccl.OverlapCoveringMerge(dumpCoverings)
//
//	// Translate the output of OverlapCoveringMerge into requests.
//	var requestEntries []importEntry
//rangeLoop:
//	for _, importRange := range importRanges {
//		needed := false
//		var ts hlc.Timestamp
//		var files []roachpb.LoadRequest_File
//		payloads := importRange.Payload.([]interface{})
//		for _, p := range payloads {
//			ie := p.(importEntry)
//			switch ie.entryType {
//			case completedSpan:
//				continue rangeLoop
//			case tableSpan:
//				needed = true
//			case dumpSpan:
//				if ts != ie.start {
//					return nil, hlc.Timestamp{}, errors.Errorf(
//						"no dump covers time [%s,%s) for range [%s,%s) or dumps listed out of order (mismatched start time)",
//						ts, ie.start,
//						roachpb.Key(importRange.Start), roachpb.Key(importRange.End))
//				}
//				ts = ie.end
//			case dumpFile:
//				if len(ie.file.Path) > 0 {
//					files = append(files, roachpb.LoadRequest_File{
//						Dir:    ie.dir,
//						Path:   ie.file.Path,
//						Sha512: ie.file.Sha512,
//					})
//				}
//			}
//		}
//		if needed {
//			if ts != maxEndTime {
//				if err := onMissing(importRange, ts, maxEndTime); err != nil {
//					return nil, hlc.Timestamp{}, err
//				}
//			}
//			// If needed is false, we have data backed up that is not necessary
//			// for this restore. Skip it.
//			requestEntries = append(requestEntries, importEntry{
//				Span:      roachpb.Span{Key: importRange.Start, EndKey: importRange.End},
//				entryType: request,
//				files:     files,
//			})
//		}
//	}
//	return requestEntries, maxEndTime, nil
//}

// JobDescription is that dump job description
func JobDescription(
	dump *tree.Dump, toFile string, incrementalFrom []string, opts map[string]string,
) (string, error) {
	b := &tree.Dump{
		AsOf:       dump.AsOf,
		Options:    optsToKVOptions(opts),
		Targets:    dump.Targets,
		FileFormat: dump.FileFormat,
	}

	toFile, err := dumpsink.SanitizeDumpSinkURI(toFile)
	if err != nil {
		return "", err
	}
	b.File = tree.NewDString(toFile)

	for _, from := range incrementalFrom {
		sanitizedFrom, err := dumpsink.SanitizeDumpSinkURI(from)
		if err != nil {
			return "", err
		}
		b.IncrementalFrom = append(b.IncrementalFrom, tree.NewDString(sanitizedFrom))
	}
	return tree.AsStringWithFlags(b, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType), nil
}

var _ jobs.Resumer = &dumpResumer{}

func dumpResumeHook(typ jobspb.Type, settings *cluster.Settings) jobs.Resumer {
	if typ != jobspb.TypeDump {
		return nil
	}

	return &dumpResumer{
		settings: settings,
	}
}

type dumpResumer struct {
	settings     *cluster.Settings
	res          roachpb.BulkOpSummary
	makeDumpSink dumpsink.Factory
}

func (b *dumpResumer) Resume(
	ctx context.Context, job *jobs.Job, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := job.Details().(jobspb.DumpDetails)
	p := phs.(sql.PlanHookState)
	b.makeDumpSink = p.ExecCfg().DistSQLSrv.DumpSink
	if len(details.DumpDescriptor) == 0 {
		return errors.New("missing dump descriptor; cannot resume a dump from an older version")
	}

	var dumpDesc DumpDescriptor
	if err := protoutil.Unmarshal(details.DumpDescriptor, &dumpDesc); err != nil {
		return errors.Wrap(err, "unmarshal dump descriptor")
	}
	conf, err := dumpsink.ConfFromURI(ctx, details.URI)
	if err != nil {
		return err
	}
	exportStore, err := b.makeDumpSink(ctx, conf)
	if err != nil {
		return err
	}
	if exportStore.Conf().Provider == roachpb.ExportStorageProvider_Http && details.HttpHeader != "" {
		err := exportStore.SetConfig(details.HttpHeader)
		if err != nil {
			return err
		}
	}
	var checkpointDesc *DumpDescriptor
	if desc, err := readDumpDescriptor(ctx, exportStore, DumpDescriptorCheckpointName, details.Encryption); err == nil {
		// If the checkpoint is from a different cluster, it's meaningless to us.
		// More likely though are dummy/lock-out checkpoints with no ClusterID.
		if desc.ClusterID.Equal(p.ExecCfg().ClusterID()) {
			checkpointDesc = &desc
		}
	} else {
		// TODO(benesch): distinguish between a missing checkpoint, which simply
		// indicates the prior dump attempt made no progress, and a corrupted
		// checkpoint, which is more troubling. Sadly, storageccl doesn't provide a
		// "not found" error that's consistent across all ExportStorage
		// implementations.
		log.Warningf(ctx, "unable to load dump checkpoint while resuming job %d: %v", *job.ID(), err)
	}
	res, err := dumpTarget(
		ctx,
		p.ExecCfg().DB,
		p.ExecCfg().Gossip,
		p.ExecCfg().Settings,
		exportStore,
		job,
		&dumpDesc,
		checkpointDesc,
		resultsCh,
		details.Encryption,
		details.CompressionCodec,
		details.DumpOnline,
		p.ExecCfg().JobRegistry,
		details.HttpHeader,
	)
	b.res = res
	if &b.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "finish resume backup job :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, status, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "finish resume backup job :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusRunning, b.res.Rows, b.res.IndexEntries, b.res.SystemRecords, b.res.DataSize)
	} else {
		log.Error(ctx, "res or job is a nil Pointer")
	}

	return err
}

func (b *dumpResumer) OnFailOrCancel(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	if &b.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "backup job is onFailOrCancel :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, status, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "backup job is onFailOrCancel :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusFailed, b.res.Rows, b.res.IndexEntries, b.res.SystemRecords, b.res.DataSize)
	} else {
		log.Error(ctx, "res or job is a nil Pointer")
	}
	return nil
}

func (b *dumpResumer) OnSuccess(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	if &b.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "backup job is onSuccess :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, status, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "backup job is onSuccess :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusSucceeded, b.res.Rows, b.res.IndexEntries, b.res.SystemRecords, b.res.DataSize)
	} else {
		log.Error(ctx, "res or job is a nil Pointer")
	}
	return nil
}

func (b *dumpResumer) OnTerminal(
	ctx context.Context, job *jobs.Job, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	// Attempt to delete DUMP-CHECKPOINT.
	if err := func() error {
		details := job.Details().(jobspb.DumpDetails)
		conf, err := dumpsink.ConfFromURI(ctx, details.URI)
		if err != nil {
			return err
		}
		exportStore, err := b.makeDumpSink(ctx, conf)
		if err != nil {
			return err
		}
		return exportStore.Delete(ctx, DumpDescriptorCheckpointName)
	}(); err != nil {
		log.Warningf(ctx, "unable to delete checkpointed dump descriptor: %+v", err)
	}

	if status == jobs.StatusSucceeded {
		// TODO(benesch): emit periodic progress updates.

		// TODO(mjibson): if a restore was resumed, then these counts will only have
		// the current coordinator's counts.

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(b.res.Rows)),
			tree.NewDInt(tree.DInt(b.res.IndexEntries)),
			tree.NewDInt(tree.DInt(b.res.SystemRecords)),
			tree.NewDInt(tree.DInt(b.res.DataSize)),
		}
	}
	if &b.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "backup job is onTerminal :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, status, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "backup job is onTerminal :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusSucceeded, b.res.Rows, b.res.IndexEntries, b.res.SystemRecords, b.res.DataSize)
	} else {
		log.Error(ctx, "res or job is a nil Pointer")
	}
}

type dumpResult struct {
	syncutil.Mutex
	files          []DumpDescriptor_File
	exported       roachpb.BulkOpSummary
	lastCheckpoint time.Time
	minStartTime   hlc.Timestamp
	result         []roachpb.Result
}

func getTableDesc(v roachpb.Value) (*sqlbase.Descriptor, error) {
	desc := sqlbase.Descriptor{}
	err := v.GetProto(&desc)
	return &desc, err
}

func splitSstWhileDumpWal(
	ctx context.Context,
	exportStore dumpsink.DumpSink,
	w *engine.RocksDBSstFileWriter,
	rows *engine.RowCounter,
	span *spanAndTime,
	dumpDesc *DumpDescriptor,
) (DumpDescriptor_File, error) {
	sst, err := w.Finish()
	if err != nil {
		return DumpDescriptor_File{}, err
	}

	sstName := fmt.Sprintf("%d.sst", builtins.GenerateUniqueInt(dumpDesc.NodeID))
	if err = exportStore.WriteFile(ctx, sstName, bytes.NewReader(sst)); err != nil {
		return DumpDescriptor_File{}, err
	}
	rows.BulkOpSummary.DataSize = w.DataSize
	checksum, err := storageicl.SHA512ChecksumData(sst)
	if err != nil {
		return DumpDescriptor_File{}, err
	}

	f := DumpDescriptor_File{
		Span:        span.span,
		Path:        sstName,
		Sha512:      checksum,
		EntryCounts: rows.BulkOpSummary,
	}

	if span.start != dumpDesc.StartTime {
		f.StartTime = span.start
		f.EndTime = span.end
	}

	return f, nil
}

// WalDumpTarget uses wal to generate SST and dump files to recover to any time node.
func WalDumpTarget(
	ctx context.Context,
	p sql.PlanHookState,
	db *client.DB,
	prevDumps []DumpDescriptor,
	exportStore dumpsink.DumpSink,
	walDir string,
	targets *tree.TargetList,
	endTime hlc.Timestamp,
) (*DumpDescriptor, error) {

	var lastDumpDesc DumpDescriptor
	if len(prevDumps) > 0 {
		lastDumpDesc = prevDumps[len(prevDumps)-1]
	}

	var dumpDesc DumpDescriptor
	dumpDesc.StartTime = lastDumpDesc.EndTime
	clock := hlc.NewClock(hlc.UnixNano, 0)
	dumpDesc.EndTime = clock.Now()
	dumpDesc.MVCCFilter = MVCCFilter_All
	dumpDesc.Descriptors = lastDumpDesc.Descriptors

	// 需要把database和schema也加入到DescriptorChanges,否则有可能找不到database
	for i, lastDescChange := range lastDumpDesc.Descriptors {
		dumpDesc.DescriptorChanges = append(dumpDesc.DescriptorChanges,
			DumpDescriptor_DescriptorRevision{Time: dumpDesc.StartTime, ID: lastDescChange.GetID(), Desc: &lastDumpDesc.Descriptors[i]})
	}

	dumpDesc.Dir = exportStore.Conf()

	if p != nil {
		dumpDesc.ClusterID = p.ExecCfg().ClusterID()
	} else {
		dumpDesc.ClusterID = lastDumpDesc.ClusterID
	}

	dumpDesc.FormatVersion = DumpFormatDescriptorTrackingVersion
	dumpDesc.BuildInfo = build.GetInfo()
	dumpDesc.Spans = lastDumpDesc.Spans
	dumpDesc.NodeID = lastDumpDesc.NodeID
	dumpDesc.MVCCFilter = MVCCFilter_All
	dumpDesc.Dir = exportStore.Conf()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("znbase-wal-%s", dumpDesc.ClusterID))
	if err != nil {
		return nil, err
	}
	//e := engine.NewRocksDB().NewInMem(roachpb.Attributes{}, 1<<20)
	cache := engine.NewRocksDBCache(128 << 20)
	defer cache.Release()
	e, err := engine.NewRocksDB(engine.RocksDBConfig{
		Dir: tmpDir,
	}, cache)
	defer e.Close()

	var walDirList []string
	walkFunc := func(path string, info os.FileInfo, err error) error {
		if info == nil {
			return errors.Errorf("WAL file path not found: %v", path)
		}
		if info.IsDir() {
			walDirList = append(walDirList, path)
		}
		return nil
	}
	if err := filepath.Walk(walDir, walkFunc); err != nil {
		return nil, err
	}

	for _, dir := range walDirList {
		if err := e.LoadWAL(dir); err != nil {
			return nil, err
		}
	}

	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()

	var lastDatabaseID, lastSchemaID, lastTableID sqlbase.ID

	if err := e.Iterate(engine.MakeMVCCMetadataKey(startKey),
		engine.MakeMVCCMetadataKey(endKey), func(kv engine.MVCCKeyValue) (bool, error) {
			if kv.Key.Timestamp.Less(dumpDesc.StartTime) || (!endTime.IsEmpty() && endTime.Less(kv.Key.Timestamp)) {
				return false, nil
			}
			v := roachpb.Value{RawBytes: kv.Value}
			if len(v.RawBytes) != 0 {
				desc, err := getTableDesc(roachpb.Value{RawBytes: kv.Value})
				if err != nil {
					return true, err
				}
				if desc != nil {
					if dbDesc := desc.GetDatabase(); dbDesc != nil {
						dumpDesc.DescriptorChanges = append(dumpDesc.DescriptorChanges,
							DumpDescriptor_DescriptorRevision{Time: kv.Key.Timestamp, ID: dbDesc.ID, Desc: desc})
						// 对于DatabaseID相同的desc,最新版本的在前面，所以只加第一个版本即可
						if lastDatabaseID != dbDesc.ID {
							dumpDesc.Descriptors = append(dumpDesc.Descriptors, *desc)
							lastDatabaseID = dbDesc.ID
						}
					}
					if scDesc := desc.GetSchema(); scDesc != nil {
						dumpDesc.DescriptorChanges = append(dumpDesc.DescriptorChanges,
							DumpDescriptor_DescriptorRevision{Time: kv.Key.Timestamp, ID: scDesc.ID, Desc: desc})
						// 对于SchemaID相同的desc,最新版本的在前面，所以只加第一个版本即可
						if lastSchemaID != scDesc.ID {
							dumpDesc.Descriptors = append(dumpDesc.Descriptors, *desc)
							lastSchemaID = scDesc.ID
						}
					}
					if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
						dumpDesc.DescriptorChanges = append(dumpDesc.DescriptorChanges,
							DumpDescriptor_DescriptorRevision{Time: kv.Key.Timestamp, ID: tableDesc.ID, Desc: desc})
						if lastTableID != tableDesc.ID {
							dumpDesc.Descriptors = append(dumpDesc.Descriptors, *desc)
							lastTableID = tableDesc.ID
						}
					}
				}
			}
			return false, nil
		}); err != nil {
		return nil, err
	}

	// 按照hlc时间戳重新排序dumpDesc.DescriptorChanges
	sort.Slice(dumpDesc.DescriptorChanges, func(i, j int) bool {
		if dumpDesc.DescriptorChanges[i].Time == dumpDesc.DescriptorChanges[j].Time {
			return dumpDesc.DescriptorChanges[i].ID < dumpDesc.DescriptorChanges[j].ID
		}
		return dumpDesc.DescriptorChanges[i].Time.Less(dumpDesc.DescriptorChanges[j].Time)
	})

	var tables []*sqlbase.TableDescriptor
	for _, descChange := range dumpDesc.Descriptors {
		if tableDesc := descChange.Table(hlc.Timestamp{}); tableDesc != nil {
			tables = append(tables, tableDesc)
		}
	}

	// 规整dumpDesc.Descriptor,每个ID只保留最新版本的结果
	resolveDescMaps := make(map[sqlbase.ID]sqlbase.Descriptor)
	for _, desc := range dumpDesc.Descriptors {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			resolveDescMaps[dbDesc.ID] = desc
		}
		if scDesc := desc.GetSchema(); scDesc != nil {
			resolveDescMaps[scDesc.ID] = desc
		}
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			if v, ok := resolveDescMaps[tableDesc.ID]; ok {
				if tableLast := v.Table(hlc.Timestamp{}); tableLast != nil && tableLast.ModificationTime.Less(tableDesc.ModificationTime) {
					resolveDescMaps[tableDesc.ID] = desc
				}
			} else {
				resolveDescMaps[tableDesc.ID] = desc
			}
		}
	}
	var tmpDescriptors []sqlbase.Descriptor
	for _, v := range resolveDescMaps {
		tmpDescriptors = append(tmpDescriptors, v)
	}
	dumpDesc.Descriptors = tmpDescriptors

	resolver := &sqlDescriptorResolver{
		allDescs:   dumpDesc.Descriptors,
		descByID:   make(map[sqlbase.ID]sqlbase.Descriptor),
		dbsByName:  make(map[string]sqlbase.ID),
		schsByName: make(map[string]sqlbase.ID),
		scsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
		tbsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
		tbsByID:    make(map[sqlbase.ID]map[sqlbase.ID]string),
	}

	err = resolver.initResolver(true)
	if err != nil {
		return nil, err
	}

	var matched matchedDescriptors
	var currentDatabase string
	var searchPath sessiondata.SearchPath
	if p == nil {
		currentDatabase = ""
		searchPath = sessiondata.SearchPath{}
	} else {
		currentDatabase = p.CurrentDatabase()
		searchPath = p.CurrentSearchPath()
	}

	if matched, err = resolver.findMatchedDescriptors(ctx,
		currentDatabase, searchPath, dumpDesc.Descriptors, *targets); err != nil {
		return nil, err
	}
	sort.Slice(matched.descs, func(i, j int) bool { return matched.descs[i].GetID() < matched.descs[j].GetID() })
	dumpDesc.Descriptors = matched.descs
	dumpDesc.CompleteDbs = matched.expandedDB

	// 构建新的DUMP, SST文件
	dumpDesc.Spans = spansForAllTableIndexes(tables, dumpDesc.DescriptorChanges)

	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		// TODO(benesch): limit the range descriptors we fetch to the ranges that
		// are actually relevant in the dump to speed up small dumps on large
		// clusters.
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return nil, err
	}

	mu := struct {
		files    []DumpDescriptor_File
		exported roachpb.BulkOpSummary
	}{}

	spans := splitAndFilterSpans(dumpDesc.Spans, nil, ranges)
	introducedSpans := splitAndFilterSpans(dumpDesc.IntroducedSpans, nil, ranges)

	allSpans := make([]spanAndTime, 0, len(spans)+len(introducedSpans))
	for _, s := range introducedSpans {
		allSpans = append(allSpans, spanAndTime{span: s, start: hlc.Timestamp{}, end: dumpDesc.StartTime})
	}
	for _, s := range spans {
		allSpans = append(allSpans, spanAndTime{span: s, start: dumpDesc.StartTime, end: dumpDesc.EndTime})
	}

	for i := range allSpans {
		span := allSpans[i]

		// Create a SST.
		w, err := engine.MakeRocksDBSstFileWriter()
		if err != nil {
			return nil, err
		}
		defer w.Close()

		var rows engine.RowCounter
		if err := e.Iterate(engine.MakeMVCCMetadataKey(span.span.Key),
			engine.MakeMVCCMetadataKey(span.span.EndKey), func(kv engine.MVCCKeyValue) (bool, error) {
				if kv.Key.Timestamp.Less(dumpDesc.StartTime) || (!endTime.IsEmpty() && endTime.Less(kv.Key.Timestamp)) {
					return false, nil
				}
				if w.DataSize > 64<<20 { // 超过64MB生成新的SST
					f, err := splitSstWhileDumpWal(ctx, exportStore, &w, &rows, &span, &dumpDesc)
					if err != nil {
						return false, nil
					}
					mu.files = append(mu.files, f)
					mu.exported.Add(rows.BulkOpSummary)
					w, err = engine.MakeRocksDBSstFileWriter()
					defer w.Close()
				}
				if err := w.Add(engine.MVCCKeyValue{
					Key:   kv.Key,
					Value: kv.Value,
				}); err != nil {
					return false, err
				}

				if err := rows.Count(kv.Key.Key); err != nil {
					return false, err
				}

				return false, nil
			}); err != nil {
			return nil, err
		}

		if w.DataSize > 0 {
			f, err := splitSstWhileDumpWal(ctx, exportStore, &w, &rows, &span, &dumpDesc)
			if err != nil {
				return nil, err
			}
			mu.files = append(mu.files, f)
			mu.exported.Add(rows.BulkOpSummary)
		}

	}

	dumpDesc.Files = mu.files
	dumpDesc.EntryCounts = mu.exported
	var newSpans roachpb.Spans

	if len(prevDumps) > 0 {
		startTime := dumpDesc.StartTime
		_, coveredTime, err := makeImportSpans(dumpDesc.Spans, prevDumps, keys.MinKey,
			func(span intervalicl.Range, start, end hlc.Timestamp) error {
				if (start == hlc.Timestamp{}) {
					newSpans = append(newSpans, roachpb.Span{Key: span.Low, EndKey: span.High})
					return nil
				}
				return errOnMissingRange(span, start, end)
			})
		if err != nil {
			return nil, errors.Wrap(err, "invalid previous dumps (a new full dump may be required if a table has been created, dropped or truncated)")
		}

		if coveredTime != startTime {
			return nil, errors.Errorf("expected previous dumps to cover until time %v, got %v", startTime, coveredTime)
		}
	}

	dumpDesc.IntroducedSpans = newSpans

	if err := writeDumpDescriptor(ctx, exportStore, DumpDescriptorName, &dumpDesc, nil); err != nil {
		return nil, err
	}
	return &dumpDesc, nil
}

// dump exports a snapshot of every kv entry into ranged sstables.
//
// The output is an sstable per range with files in the following locations:
// - <dir>/<unique_int>.sst
// - <dir> is given by the user and may be cloud storage
// - Each file contains data for a key range that doesn't overlap with any other
//   file.
func dumpTarget(
	ctx context.Context,
	db *client.DB,
	gossip *gossip.Gossip,
	settings *cluster.Settings,
	exportStore dumpsink.DumpSink,
	job *jobs.Job,
	dumpDesc *DumpDescriptor,
	checkpointDesc *DumpDescriptor,
	resultsCh chan<- tree.Datums,
	encryption *roachpb.FileEncryptionOptions,
	codec roachpb.FileCompression,
	online bool,
	jobRes *jobs.Registry,
	httpHeader string,
) (roachpb.BulkOpSummary, error) {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.
	var mu dumpResult
	mu.minStartTime = hlc.MaxTimestamp
	var checkpointMu syncutil.Mutex

	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		// TODO(benesch): limit the range descriptors we fetch to the ranges that
		// are actually relevant in the dump to speed up small dumps on large
		// clusters.
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return mu.exported, err
	}

	var completedSpans, completedIntroducedSpans []roachpb.Span
	if checkpointDesc != nil {
		// TODO(benesch): verify these files, rather than accepting them as truth
		// blindly.
		// No concurrency yet, so these assignments are safe.
		mu.files = checkpointDesc.Files
		mu.exported = checkpointDesc.EntryCounts
		for _, file := range checkpointDesc.Files {
			if file.StartTime.IsEmpty() && !file.EndTime.IsEmpty() {
				completedIntroducedSpans = append(completedIntroducedSpans, file.Span)
			} else {
				completedSpans = append(completedSpans, file.Span)
			}
		}
	}

	// Subtract out any completed spans and split the remaining spans into
	// range-sized pieces so that we can use the number of completed requests as a
	// rough measure of progress.
	spans := splitAndFilterSpans(dumpDesc.Spans, completedSpans, ranges)
	introducedSpans := splitAndFilterSpans(dumpDesc.IntroducedSpans, completedIntroducedSpans, ranges)

	allSpans := make([]spanAndTime, 0, len(spans)+len(introducedSpans))
	for _, s := range introducedSpans {
		allSpans = append(allSpans, spanAndTime{span: s, start: hlc.Timestamp{}, end: dumpDesc.StartTime})
	}
	for _, s := range spans {
		allSpans = append(allSpans, spanAndTime{span: s, start: dumpDesc.StartTime, end: dumpDesc.EndTime})
	}
	var requestFinishedCh chan struct{}
	var totalChunks int
	if online {
		requestFinishedCh = make(chan struct{}, 2*len(spans))
		totalChunks = 2 * len(spans)
	} else {
		requestFinishedCh = make(chan struct{}, len(spans))
		totalChunks = len(spans)
	}
	progressLogger := jobs.ProgressLogger{
		Job:           job,
		TotalChunks:   totalChunks,
		StartFraction: job.FractionCompleted(),
	}

	// We're already limiting these on the server-side, but sending all the
	// Export requests at once would fill up distsender/grpc/something and cause
	// all sorts of badness (node liveness timeouts leading to mass leaseholder
	// transfers, poor performance on SQL workloads, etc) as well as log spam
	// about slow distsender requests. Rate limit them here, too.
	//
	// Each node limits the number of running Export & Import requests it serves
	// to avoid overloading the network, so multiply that by the number of nodes
	// in the cluster and use that as the number of outstanding Export requests
	// for the rate limiting. This attempts to strike a balance between
	// simplicity, not getting slow distsender log spam, and keeping the server
	// side limiter full.
	//
	// TODO(dan): Make this limiting per node.
	//
	// TODO(dan): See if there's some better solution than rate-limiting #14798.
	maxConcurrentExports := clusterNodeCount(gossip) * int(storage.ExportRequestsLimit.Get(&settings.SV))
	exportsSem := make(chan struct{}, maxConcurrentExports)
	g := ctxgroup.WithContext(ctx)
	// enough buffer to never block
	// Only start the progress logger if there are spans, otherwise this will
	// block forever. This is needed for TestDumpRestoreResume which doesn't
	// have any spans. Users should never hit this.
	if len(spans) > 0 {
		g.GoCtx(func(ctx context.Context) error {
			return progressLogger.Loop(ctx, requestFinishedCh)
		})
	}
	var headTs hlc.Timestamp
	//当为在线备份时
	if online {
		group := ctxgroup.WithContext(ctx)
		_, _, err := prevDump(spans, group, requestFinishedCh, allSpans, exportsSem, exportStore, dumpDesc, encryption, codec, db, &mu)
		if err != nil {
			return roachpb.BulkOpSummary{}, err
		}
		if !mu.minStartTime.Equal(hlc.MaxTimestamp) {
			headTs = mu.minStartTime.Prev()
		}
	}
	g.GoCtx(func(ctx context.Context) error {
		for i := range allSpans {
			{
				select {
				case exportsSem <- struct{}{}:
				case <-ctx.Done():
					// Break the for loop to avoid creating more work - the dump
					// has failed because either the context has been canceled or an
					// error has been returned. Either way, Wait() is guaranteed to
					// return an error now.
					return ctx.Err()
				}
			}

			span := allSpans[i]
			g.GoCtx(func(ctx context.Context) error {
				defer func() { <-exportsSem }()
				var rawRes roachpb.Response
				var pErr *roachpb.Error
				switch headTs {
				case hlc.Timestamp{}:
					header := roachpb.Header{Timestamp: span.end}
					req := &roachpb.DumpRequest{
						RequestHeader:    roachpb.RequestHeaderFromSpan(span.span),
						Sink:             exportStore.Conf(),
						StartTime:        span.start,
						MVCCFilter:       roachpb.MVCCFilter(dumpDesc.MVCCFilter),
						Encryption:       encryption,
						HttpHeader:       httpHeader,
						CompressionCodec: codec,
					}
					rawRes, pErr = client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
				default:
					header := roachpb.Header{Timestamp: headTs, ReadConsistency: roachpb.READ_UNCOMMITTED}
					if !span.start.Equal(hlc.Timestamp{}) {
						span.end = headTs
					}
					req := &roachpb.DumpOnlineRequest{
						RequestHeader:    roachpb.RequestHeaderFromSpan(span.span),
						Sink:             exportStore.Conf(),
						StartTime:        span.start,
						MVCCFilter:       roachpb.MVCCFilter(dumpDesc.MVCCFilter),
						Encryption:       encryption,
						CompressionCodec: codec,
						JobId:            *job.ID(),
					}
					rawRes, pErr = client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
				}
				var checkpointFiles FileDescriptors
				if !headTs.Equal(hlc.Timestamp{}) {
					err := onlineHandle(pErr, rawRes, &mu, dumpDesc, span, &checkpointFiles)
					if err != nil {
						return err
					}
				} else {
					if pErr != nil {
						return pErr.GoError()
					}
					res := rawRes.(*roachpb.DumpResponse)
					mu.Lock()
					if dumpDesc.RevisionStartTime.Less(res.StartTime) {
						dumpDesc.RevisionStartTime = res.StartTime
					}
					for _, file := range res.Files {
						f := DumpDescriptor_File{
							Span:        file.Span,
							Path:        file.Path,
							Sha512:      file.Sha512,
							EntryCounts: file.Dumped,
						}
						if span.start != dumpDesc.StartTime {
							f.StartTime = span.start
							f.EndTime = span.end
						}
						mu.files = append(mu.files, f)
						mu.exported.Add(file.Dumped)
					}
					if timeutil.Since(mu.lastCheckpoint) > DumpCheckpointInterval {
						// We optimistically assume the checkpoint will succeed to prevent
						// multiple threads from attempting to checkpoint.
						mu.lastCheckpoint = timeutil.Now()
						checkpointFiles = append(checkpointFiles, mu.files...)
					}
					mu.Unlock()
				}
				requestFinishedCh <- struct{}{}
				if checkpointFiles != nil {
					checkpointMu.Lock()
					dumpDesc.Files = checkpointFiles
					if !headTs.Equal(hlc.Timestamp{}) {
						dumpDesc.EndTime = headTs
					}
					err := writeDumpDescriptor(
						ctx, exportStore, DumpDescriptorCheckpointName, dumpDesc, encryption,
					)
					checkpointMu.Unlock()
					if err != nil {
						log.Errorf(ctx, "unable to checkpoint dump descriptor: %+v", err)
					}
				}
				return nil
			})
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return mu.exported, errors.Wrapf(err, "exporting %d ranges", len(spans))
	}

	// No more concurrency, so no need to acquire locks below.
	//For the current online backup, a request cannot guarantee its complete return.
	//Therefore, the relevant backup information is stored in the job details.
	//TODO The next step should replace the current online backup plan with a new one
	if online && !headTs.Equal(hlc.Timestamp{}) {
		mu.files = []DumpDescriptor_File{}
		mu.exported = roachpb.BulkOpSummary{}
		job, err := jobRes.LoadJob(ctx, *job.ID())
		if err != nil {
			return roachpb.BulkOpSummary{}, err
		}
		details := job.Details().(jobspb.DumpDetails)
		for _, file := range details.DumpOnlineFiles.Files {
			f := DumpDescriptor_File{
				Span:        file.Span,
				Path:        file.Path,
				Sha512:      file.Sha512,
				EntryCounts: file.Dumped,
			}
			mu.files = append(mu.files, f)
			mu.exported.Add(file.Dumped)
		}
	}
	dumpDesc.Files = mu.files
	dumpDesc.EntryCounts = mu.exported
	if err := writeDumpDescriptor(ctx, exportStore, DumpDescriptorName, dumpDesc, encryption); err != nil {
		return mu.exported, err
	}

	return mu.exported, nil
}

func onlineHandle(
	pErr *roachpb.Error,
	rawRes roachpb.Response,
	mu *dumpResult,
	dumpDesc *DumpDescriptor,
	span spanAndTime,
	checkpointFiles *FileDescriptors,
) error {
	if pErr != nil {
		return pErr.GoError()
	}
	res := rawRes.(*roachpb.DumpOnlineResponse)
	mu.Lock()
	if dumpDesc.RevisionStartTime.Less(res.StartTime) {
		dumpDesc.RevisionStartTime = res.StartTime
	}
	for _, file := range res.Files {
		f := DumpDescriptor_File{
			Span:        file.Span,
			Path:        file.Path,
			Sha512:      file.Sha512,
			EntryCounts: file.Dumped,
		}
		if span.start != dumpDesc.StartTime {
			f.StartTime = span.start
			f.EndTime = span.end
		}
		mu.files = append(mu.files, f)
		mu.exported.Add(file.Dumped)
	}

	if timeutil.Since(mu.lastCheckpoint) > DumpCheckpointInterval {
		// We optimistically assume the checkpoint will succeed to prevent
		// multiple threads from attempting to checkpoint.
		mu.lastCheckpoint = timeutil.Now()
		*checkpointFiles = append(*checkpointFiles, mu.files...)
	}
	mu.Unlock()
	return nil
}

func prevDump(
	spans []roachpb.Span,
	group ctxgroup.Group,
	prevRequestFinishedCh chan struct{},
	allSpans []spanAndTime,
	exportsSem chan struct{},
	exportStore dumpsink.DumpSink,
	dumpDesc *DumpDescriptor,
	encryption *roachpb.FileEncryptionOptions,
	codec roachpb.FileCompression,
	db *client.DB,
	mu *dumpResult,
) (roachpb.BulkOpSummary, bool, error) {
	group.GoCtx(func(ctx context.Context) error {
		//There are currently two situations:
		//1. The backup does not affect online business and the backup will not be suspended all the time.
		//	This implementation method will read historical data,
		//	Note that the time stamp t1->t0 corresponding to the backup will be dynamically adjusted at this time.
		//	At this time, the business data between t0-t1 will be lost
		//2. The backup does not affect online business, but when the conflicting transaction is not committed, the backup operation will always be suspended.
		//And may hang indefinitely.
		for i := range allSpans {
			{
				select {
				case exportsSem <- struct{}{}:
				case <-ctx.Done():
					// Break the for loop to avoid creating more work - the dump
					// has failed because either the context has been canceled or an
					// error has been returned. Either way, Wait() is guaranteed to
					// return an error now.
					return ctx.Err()
				}
			}

			span := allSpans[i]
			group.GoCtx(func(ctx context.Context) error {
				defer func() { <-exportsSem }()
				header := roachpb.Header{Timestamp: span.end, ReadConsistency: roachpb.READ_UNCOMMITTED}
				req := &roachpb.DumpOnlineRequest{
					RequestHeader:    roachpb.RequestHeaderFromSpan(span.span),
					Sink:             exportStore.Conf(),
					StartTime:        span.start,
					MVCCFilter:       roachpb.MVCCFilter(dumpDesc.MVCCFilter),
					Encryption:       encryption,
					CompressionCodec: codec,
					SeekTime:         true,
				}
				_, pErr := client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
				if pErr != nil {
					switch t := pErr.GetDetail().(type) {
					case *roachpb.WriteIntentError:
						minTime, result, err := handleDumpWriteIntentError(ctx, db, t, header)
						if err != nil {
							return err.GoError()
						}
						mu.Lock()
						if minTime.Less(mu.minStartTime) {
							mu.minStartTime = minTime
						}
						mu.result = append(mu.result, result...)
						mu.Unlock()
					default:
						return pErr.GoError()

						//return pErr.GoError()
					}
				}
				prevRequestFinishedCh <- struct{}{}
				return nil
			})
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		return mu.exported, true, errors.Wrapf(err, "exporting %d ranges", len(spans))
	}
	return roachpb.BulkOpSummary{}, false, nil
}

func handleDumpWriteIntentError(
	ctx context.Context, db *client.DB, t *roachpb.WriteIntentError, head roachpb.Header,
) (hlc.Timestamp, []roachpb.Result, *roachpb.Error) {
	minTime := hlc.MaxTimestamp
	var result []roachpb.Result
	for _, v := range t.Intents {
		b := &client.Batch{}
		//b.Header.Timestamp = head.Timestamp
		b.Header.Timestamp = db.Clock().Now()
		b.AddRawRequest(&roachpb.QueryTxnRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: v.Txn.Key,
			},
			Txn: v.Txn,
		})
		if err := db.Run(ctx, b); err != nil {
			return hlc.Timestamp{}, result, roachpb.NewError(err)
		}
		res := b.RawResponse().Responses
		queryTxnResp := res[0].GetInner().(*roachpb.QueryTxnResponse)
		queriedTxn := &queryTxnResp.QueriedTxn
		switch queriedTxn.Status {
		//When the query has been submitted
		case roachpb.COMMITTED:
			//When the transaction has been submitted, no matter what degree of the timestamp,
			//as long as the corresponding intent is queried again, the data should be retained at this time
			result = append(result, roachpb.Result{Key: v.Txn.Key, Remove: true})
		//When the query has been rolled back
		case roachpb.ABORTED:
			result = append(result, roachpb.Result{Key: v.Txn.Key, Remove: false})
		//When the query transaction is PENDING
		case roachpb.PENDING:
			//Find the minimum time
			if v.IntentTimestamp.Less(minTime) {
				minTime = v.IntentTimestamp
			}
			result = append(result, roachpb.Result{Key: v.Txn.Key, Remove: false})
		//When the query transaction is STAGING
		case roachpb.STAGING:
			//Find the minimum time
			//if v.IntentTimestamp.Less(minTime) {
			//	minTime = v.IntentTimestamp
			//}
			result = append(result, roachpb.Result{Key: v.Txn.Key, Remove: true})
		}
	}
	return minTime, result, nil
}

//func errOnMissingRange(span intervalccl.Range, start, end hlc.Timestamp) error {
//	return errors.Errorf(
//		"no dump covers time [%s,%s) for range [%s,%s) (or dumps out of order)",
//		start, end, roachpb.Key(span.Start), roachpb.Key(span.End),
//	)
//}
