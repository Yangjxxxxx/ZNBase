package storageicl

import (
	"bytes"
	"context"
	"crypto/sha512"
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl/engineicl"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/storage/batcheval"
	"github.com/znbasedb/znbase/pkg/storage/batcheval/result"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/spanset"
	"github.com/znbasedb/znbase/pkg/util/gziputil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

func init() {
	batcheval.RegisterCommand(roachpb.Dump, declareKeysExport, evalDump)
}

func declareKeysExport(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	batcheval.DefaultDeclareIsolatedKeys(desc, header, req, latchSpans, lockSpans)
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
}

// evalDump dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalDump(
	ctx context.Context, batch engine.ReadWriter, cArgs batcheval.CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.DumpRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.DumpResponse)

	ctx, span := tracing.ChildSpan(ctx, fmt.Sprintf("Dump [%s,%s)", args.Key, args.EndKey))
	defer tracing.FinishSpan(span)

	// If the startTime is zero, then we're doing a full backup and the gc
	// threshold is irrelevant for MVCC_Lastest backups. Otherwise, make sure
	// startTime is after the gc threshold. If it's not, the mvcc tombstones could
	// have been deleted and the resulting RocksDB tombstones compacted, which
	// means we'd miss deletions in the incremental backup. For MVCC_All backups
	// with no start time, they'll only be capturing the *revisions* since the
	// gc threshold, so noting that in the reply allows the BACKUP to correctly
	// note the supported time bounds for RESTORE AS OF SYSTEM TIME.
	gcThreshold := cArgs.EvalCtx.GetGCThreshold()
	if !args.StartTime.IsEmpty() {
		if !gcThreshold.Less(args.StartTime) {
			return result.Result{}, errors.Errorf("start timestamp %v must be after replica GC threshold %v", args.StartTime, gcThreshold)
		}
	} else if args.MVCCFilter == roachpb.MVCCFilter_All {
		reply.StartTime = gcThreshold
	}

	if err := cArgs.EvalCtx.GetLimiters().ConcurrentExportRequests.Begin(ctx); err != nil {
		return result.Result{}, err
	}
	defer cArgs.EvalCtx.GetLimiters().ConcurrentExportRequests.Finish()

	makeDumpSink := !args.ReturnSST || (args.Sink != roachpb.DumpSink{})
	if makeDumpSink || log.V(1) {
		log.Infof(ctx, "dump [%s,%s)", args.Key, args.EndKey)
	} else {
		// Requests that don't write to export storage are expected to be small.
		log.Eventf(ctx, "dump [%s,%s)", args.Key, args.EndKey)
	}

	var dumpSink dumpsink.DumpSink
	if makeDumpSink {
		var err error
		dumpSink, err = cArgs.EvalCtx.GetDumpSink(ctx, args.Sink)
		if err != nil {
			return result.Result{}, err
		}
		defer dumpSink.Close()
	}

	sstFile := &engine.MemFile{}
	sst := engine.MakeIngestionSSTWriter(sstFile)

	defer sst.Close()

	var skipTombstones bool
	var iterFn func(*engineicl.MVCCIncrementalIterator)
	switch args.MVCCFilter {
	case roachpb.MVCCFilter_Latest:
		skipTombstones = true
		iterFn = (*engineicl.MVCCIncrementalIterator).NextKey
	case roachpb.MVCCFilter_All:
		skipTombstones = false
		iterFn = (*engineicl.MVCCIncrementalIterator).Next
	default:
		return result.Result{}, errors.Errorf("unknown MVCC filter: %s", args.MVCCFilter)
	}

	debugLog := log.V(3)

	var rows engine.RowCounter
	// TODO(dan): Move all this iteration into cpp to avoid the cgo calls.
	// TODO(dan): Consider checking ctx periodically during the MVCCIterate call.
	iter := engineicl.NewMVCCIncrementalIterator(batch, engineicl.IterOptions{
		StartTime:                           args.StartTime,
		EndTime:                             h.Timestamp,
		UpperBound:                          args.EndKey,
		EnableTimeBoundIteratorOptimization: args.EnableTimeBoundIteratorOptimization,
	})
	defer iter.Close()
	for iter.Seek(engine.MakeMVCCMetadataKey(args.Key)); ; iterFn(iter) {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			return result.Result{}, err
		}
		if !ok || iter.UnsafeKey().Key.Compare(args.EndKey) >= 0 {
			break
		}

		// Skip tombstone (len=0) records when startTime is zero
		// (non-incremental) and we're not exporting all versions.
		if skipTombstones && args.StartTime.IsEmpty() && len(iter.UnsafeValue()) == 0 {
			continue
		}

		if debugLog {
			// Calling log.V is more expensive than you'd think. Keep it out of
			// the hot path.
			v := roachpb.Value{RawBytes: iter.UnsafeValue()}
			log.Infof(ctx, "Export %s %s", iter.UnsafeKey(), v.PrettyPrint())
		}

		if err := rows.Count(iter.UnsafeKey().Key); err != nil {
			return result.Result{}, errors.Wrapf(err, "decoding %s", iter.UnsafeKey())
		}
		if err := sst.Put(iter.UnsafeKey(), iter.UnsafeValue()); err != nil {
			return result.Result{}, errors.Wrapf(err, "adding key %s", iter.UnsafeKey())
		}
	}

	if sst.DataSize == 0 {
		// Let the defer Close the sstable.
		reply.Files = []roachpb.DumpResponse_File{}
		return result.Result{}, nil
	}
	rows.BulkOpSummary.DataSize = sst.DataSize

	err := sst.Finish()
	if err != nil {
		return result.Result{}, err
	}

	var checksum []byte
	data := sstFile.Data()
	if !args.OmitChecksum {
		// Compute the checksum before we upload and remove the local file.
		checksum, err = SHA512ChecksumData(data)
		if err != nil {
			return result.Result{}, err
		}
	}
	if args.Encryption != nil {
		data, err = EncryptFile(data, args.Encryption.Key)
		if err != nil {
			return result.Result{}, err
		}
	}

	dumped := roachpb.DumpResponse_File{
		Span:   args.Span(),
		Dumped: rows.BulkOpSummary,
		Sha512: checksum,
	}

	if dumpSink != nil {
		dumped.Path = fmt.Sprintf("%d.sst", builtins.GenerateUniqueInt(cArgs.EvalCtx.NodeID()))
		if args.CompressionCodec == roachpb.FileCompression_Gzip {
			data, rows.BulkOpSummary.DataSize, err = gziputil.GzipBytes(data)
			if err != nil {
				return result.Result{}, err
			}
			dumped.Path += ".gz"
		}
		if dumpSink.Conf().Provider == roachpb.ExportStorageProvider_Http && args.HttpHeader != "" {
			err := dumpSink.SetConfig(args.HttpHeader)
			if err != nil {
				return result.Result{}, err
			}
		}
		if err := dumpSink.WriteFile(ctx, dumped.Path, bytes.NewReader(data)); err != nil {
			return result.Result{}, err
		}
	}

	if args.ReturnSST {
		dumped.SST = data
	}

	reply.Files = []roachpb.DumpResponse_File{dumped}
	return result.Result{}, nil
}

// SHA512ChecksumData returns the SHA512 checksum of data.
func SHA512ChecksumData(data []byte) ([]byte, error) {
	h := sha512.New()
	if _, err := h.Write(data); err != nil {
		panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return h.Sum(nil), nil
}
