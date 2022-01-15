package storageicl

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl/engineicl"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
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
	batcheval.RegisterCommand(roachpb.DumpOnline, declareKeysDumpOnline, evalDumpOnline)
}

func declareKeysDumpOnline(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	batcheval.DefaultDeclareIsolatedKeys(desc, header, req, latchSpans, lockSpans)
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
}

// evalDumpOnline dumps the requested keys into files of non-overlapping key ranges
// in a format suitable for bulk ingest.
func evalDumpOnline(
	ctx context.Context, batch engine.ReadWriter, cArgs batcheval.CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.DumpOnlineRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.DumpOnlineResponse)
	resultIntents := args.Results
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
		iterFn = (*engineicl.MVCCIncrementalIterator).DumpNextKey
	case roachpb.MVCCFilter_All:
		skipTombstones = false
		iterFn = (*engineicl.MVCCIncrementalIterator).DumpNext
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
	//realDumpTime := hlc.MaxTimestamp
	//maxReadTS := cArgs.EvalCtx.GetMaxRead(args.Key, args.EndKey)
	//针对于每个intent对应的事务的minCommitTs
	intents := &roachpb.WriteIntentError{}
	for iter.DumpSeek(engine.MakeMVCCMetadataKey(args.Key)); ; iterFn(iter) {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			if args.SeekTime {
				if intent, ok := err.(*roachpb.WriteIntentError); ok {
					//处理完的Intent，不在committedIntents的则为脏数据，移除；
					//在committedIntents的则为commit数据，需要添加到备份数据中;
					if !hasTxn(intents.Intents, intent.Intents[0]) {
						//if maxReadTS.Less(intent.Intents[0].IntentTimestamp) {
						//	intent.Intents[0].IntentTimestamp = maxReadTS
						//}
						intents.Intents = append(intents.Intents, intent.Intents...)
					}
					iter.DumpReset()
					iter.DumpNext()
					ok, err := iter.Valid()
					if err != nil {
						continue
					}
					if !ok || iter.UnsafeKey().Key.Compare(args.EndKey) >= 0 {
						break
					}
					//continue
					iter.DumpReset()
				} else {
					return result.Result{}, err
				}
			} else {
				if wiErr, ok := err.(*roachpb.WriteIntentError); ok {
					//处理完的Intent，不在committedIntents的则为脏数据，移除；
					//在committedIntents的则为commit数据，需要添加到备份数据中;
					if !equalIntents(wiErr.Intents[0], resultIntents) {
						iter.DumpReset()
						iter.DumpNext()
						ok, err := iter.Valid()
						if err != nil {
							continue
						}
						if !ok || iter.UnsafeKey().Key.Compare(args.EndKey) >= 0 {
							break
						}
						//continue
					}
					iter.DumpReset()
				}
			}
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
	//在线备份,第一次请求，返回所有确定抛弃的写意图中的最小时间戳。从而确定实际的备份时间
	if args.SeekTime {
		return result.Result{}, intents
	}
	if sst.DataSize == 0 {
		// Let the defer Close the sstable.
		reply.Files = []roachpb.DumpOnlineResponse_File{}
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

	dumped := roachpb.DumpOnlineResponse_File{
		Span:   args.Span(),
		Dumped: rows.BulkOpSummary,
		Sha512: checksum,
	}

	if dumpSink != nil {
		uniqueID := builtins.GenerateUniqueInt(cArgs.EvalCtx.NodeID())
		dumped.Path = fmt.Sprintf("%d.sst", uniqueID)
		if args.CompressionCodec == roachpb.FileCompression_Gzip {
			data, rows.BulkOpSummary.DataSize, err = gziputil.GzipBytes(data)
			if err != nil {
				return result.Result{}, err
			}
			dumped.Path += ".gz"
		}
		if err := dumpSink.WriteFile(ctx, dumped.Path, bytes.NewReader(data)); err != nil {
			return result.Result{}, err
		}
	}

	if args.ReturnSST {
		dumped.SST = data
	}
	reply.Files = append(reply.Files, []roachpb.DumpOnlineResponse_File{dumped}...)
	//因为dumpOnlineRequest在range 分裂时，请求被划分处理，response并没有收集完全，目前通过写临时文件的方式进行记录
	//获取JOB信息并更新details
	return recordDumpFiles(ctx, cArgs, args, dumped)
}

func recordDumpFiles(
	ctx context.Context,
	cArgs batcheval.CommandArgs,
	args *roachpb.DumpOnlineRequest,
	dumped roachpb.DumpOnlineResponse_File,
) (result.Result, error) {
	{
		execCfgInterface, err := cArgs.EvalCtx.GetExecCfg()
		if err != nil {
			return result.Result{}, err
		}
		//此处确定为ExecutorConfig类型
		execCfg, ok := execCfgInterface.(*sql.ExecutorConfig)
		if !ok {
			return result.Result{}, errors.New("ExecutorConfig conversion failed")
		}
		//switch execCfgInterface.(type) {
		//case *sql.InternalExecutor:
		//	executor := execCfgInterface.(*sql.InternalExecutor)
		//	exec, err := executor.GetExecCfg()
		//	if err != nil {
		//		return result.Result{}, err
		//	}
		//	execCfg = exec.(*sql.ExecutorConfig)
		//case *sql.SessionBoundInternalExecutor:
		//	executor := execCfgInterface.(*sql.SessionBoundInternalExecutor)
		//	exec, err := executor.GetExecCfg()
		//	if err != nil {
		//		return result.Result{}, err
		//	}
		//	execCfg = exec.(*sql.ExecutorConfig)
		//}
		job, err := execCfg.JobRegistry.LoadJob(ctx, args.JobId)
		if err != nil {
			return result.Result{}, err
		}
		err = job.FractionDetailProgressed(ctx, func(ctx context.Context, details jobspb.Details, progress jobspb.ProgressDetails) float32 {
			d := details.(*jobspb.Payload_Dump).Dump
			d.DumpOnlineFiles.Files = append(d.DumpOnlineFiles.Files, dumped)
			return job.FractionCompleted()
		})
		if err != nil {
			return result.Result{}, err
		}
	}
	return result.Result{}, nil
}

func hasTxn(intents []roachpb.Intent, preIntent roachpb.Intent) bool {
	for i, intent := range intents {
		if bytes.Equal(intent.Txn.Key, preIntent.Txn.Key) {
			if preIntent.IntentTimestamp.Less(intent.IntentTimestamp) {
				intents[i].IntentTimestamp = preIntent.IntentTimestamp
			}
			return true
		}
	}
	return false
}

func equalIntents(intents roachpb.Intent, commitedIntents []roachpb.Result) bool {
	for _, cIntent := range commitedIntents {
		if cIntent.Key.Equal(intents.Txn.Key) && cIntent.Remove {
			return true
		}
	}
	return false
}
