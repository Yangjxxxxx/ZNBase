// Copyright 2017 The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package storageicl

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/batcheval"
	"github.com/znbasedb/znbase/pkg/storage/bulk"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/retry"
)

var loadSplitSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.load.split_size",
		"the maximum size of large file split size",
		2<<30,
	)
	s.SetSensitive()
	return s
}()

var loadBatchSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.load.batch_size",
		"the maximum size of the payload in an AddSSTable request",
		32<<20,
	)
	s.SetSensitive()
	return s
}()

// cluster setting不要用runtime.NumCPU()否则文档默认值不定
// 如果想用runtime.NumCPU()请使用envutil
var loadConcurrency = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"kv.load.concurrency",
		"number of concurrent goroutines that convert kv data during load",
		4,
	)
	s.SetSensitive()
	return s
}()

var loadRecordBatchSize = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"kv.load.record.batch.size",
		"read the maximum number of file records, when it is exceeded, put it in the record channel",
		500,
	)
	s.SetSensitive()
	return s
}()

var loadKvRecordSize = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"kv.load.kvrecord.size",
		"kv records the maximum number, when it is exceeded, it is placed in the kv channel",
		5000,
	)
	s.SetSensitive()
	return s
}()

var loadKvChCap = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"kv.load.kvch.cap",
		"the capacity of the kv channel during the load process",
		10,
	)
	s.SetSensitive()
	return s
}()

var loadFlushSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.load.flush.size",
		"when this value is reached, the data in sstbatch is flushed once, that is, the data in kvch is consumed",
		16<<20,
	)
	s.SetSensitive()
	return s
}()

var loadSortBatchSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.load.sort.batch.size",
		"the maximum value of the total cache memory space in the ingestkv process, if it exceeds this value, it is directly flushed to sstbatch",
		48<<20,
	)
	s.SetSensitive()
	return s
}()

var loadBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.load.buffer.size",
		"bulkadder's cache size",
		64<<20,
	)
	s.SetSensitive()
	return s
}()

var loadRecordChCap = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"kv.load.recordch.size",
		"recordCh size",
		2,
	)
	s.SetSensitive()
	return s
}()

var loadIngestConcurrency = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"kv.load.ingest.concurrency",
		"concurrent number of coroutines written to SST file",
		int64(4),
	)
	s.SetSensitive()
	return s
}()

var loadKafkaConcurrency = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"kv.load.kafka.concurrency",
		"concurrent number of coroutines consumer kafka data",
		int64(2),
	)
	s.SetSensitive()
	return s
}()

var loadIntoRevertBatchSize = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		"kv.load.revert.batch_size",
		"revertTableDefaultBatchSize is the default batch size for reverting tables. This only needs to be small enough to keep raft/rocks happy -- there is no reply size to worry about.",
		500000,
	)
	s.SetSensitive()
	return s
}()

var loadKafkaFlushDuration = func() *settings.DurationSetting {
	s := settings.RegisterDurationSetting(
		"kv.load.kafka.flush_duration",
		"kafka pushes data each time",
		5*time.Second,
	)
	s.SetSensitive()
	return s
}()

// commandMetadataEstimate is an estimate of how much metadata Raft will add to
// an AddSSTable command. It is intentionally a vast overestimate to avoid
// embedding intricate knowledge of the Raft encoding scheme here.
const commandMetadataEstimate = 1 << 20 // 1 MB

func init() {
	storage.SetImportCmd(evalImport)

	// Ensure that the user cannot set the maximum raft command size so low that
	// more than half of an Import or AddSSTable command will be taken up by Raft
	// metadata.
	if commandMetadataEstimate > storage.MaxCommandSizeFloor/2 {
		panic(fmt.Sprintf("raft command size floor (%s) is too small for import commands",
			humanizeutil.IBytes(storage.MaxCommandSizeFloor)))
	}
}

// MaxImportBatchSize determines the maximum size of the payload in an
// AddSSTable request. It uses the ImportBatchSize setting directly unless the
// specified value would exceed the maximum Raft command size, in which case it
// returns the maximum batch size that will fit within a Raft command.
func MaxImportBatchSize(st *cluster.Settings) int64 {
	desiredSize := loadBatchSize.Get(&st.SV)
	maxCommandSize := storage.MaxCommandSize.Get(&st.SV)
	if desiredSize+commandMetadataEstimate > maxCommandSize {
		return maxCommandSize - commandMetadataEstimate
	}
	return desiredSize
}

// MaxLoadSplitSize determines the maximum size of large file split size.
func MaxLoadSplitSize(st *cluster.Settings) int64 {
	return loadSplitSize.Get(&st.SV)
}

// LoadConcurrency 返回转换协程并发数
func LoadConcurrency(st *cluster.Settings) int64 {
	return loadConcurrency.Get(&st.SV)
}

// LoadIngestConcurrency 返回ingestKvs协程并发数
func LoadIngestConcurrency(st *cluster.Settings) int64 {
	return loadIngestConcurrency.Get(&st.SV)
}

// LoadRecordBatchSize 返回读取文件最大记录数，超过此值则将其放入recordCh中
func LoadRecordBatchSize(st *cluster.Settings) int64 {
	return loadRecordBatchSize.Get(&st.SV)
}

// LoadKvRecordSize 返回转换kv最大暂存数，超过此值则将其放入kvCh中
func LoadKvRecordSize(st *cluster.Settings) int64 {
	return loadKvRecordSize.Get(&st.SV)
}

// LoadKvChCap 返回kv通道容量也即缓存
func LoadKvChCap(st *cluster.Settings) int64 {
	return loadKvChCap.Get(&st.SV)
}

// LoadFlushSize 返回ingestKvs过程中bulkAddr flush数据到sstbatcher中的阈值
func LoadFlushSize(st *cluster.Settings) int64 {
	return loadFlushSize.Get(&st.SV)
}

// LoadSortBatchSize 返回ingestKvs总的最大缓存区间阈值，超过此阈值则将其中数据flush到sstbatcher中
func LoadSortBatchSize(st *cluster.Settings) int64 {
	return loadSortBatchSize.Get(&st.SV)
}

// LoadBufferSize 返回BulkAdder的缓存大小
func LoadBufferSize(st *cluster.Settings) int64 {
	return loadBufferSize.Get(&st.SV)
}

// LoadRecordChCap 返回record通道的大小
func LoadRecordChCap(st *cluster.Settings) int64 {
	return loadRecordChCap.Get(&st.SV)
}

// LoadKafkaFlushDuration 返回load kafka 数据推送时间间隔
func LoadKafkaFlushDuration(st *cluster.Settings) time.Duration {
	return loadKafkaFlushDuration.Get(&st.SV)
}

// LoadIntoRevertBatchSize 返回load into 失败回滚默认的批次大小
func LoadIntoRevertBatchSize(st *cluster.Settings) int64 {
	return loadIntoRevertBatchSize.Get(&st.SV)
}

// LoadKafkaConcurrency 返回kafka消费协程并发数
func LoadKafkaConcurrency(st *cluster.Settings) int64 {
	return loadKafkaConcurrency.Get(&st.SV)
}

// evalImport bulk loads key/value entries.
func evalImport(ctx context.Context, cArgs batcheval.CommandArgs) (*roachpb.ImportResponse, error) {
	args := cArgs.Args.(*roachpb.LoadRequest)
	db := cArgs.EvalCtx.DB()
	kr, err := MakeKeyRewriterFromRekeys(args.Rekeys)
	if err != nil {
		return nil, errors.Wrap(err, "make key rewriter")
	}
	for _, desc := range kr.descs {
		log.Errorf(ctx, "name :%s,id:%d", desc.Name, desc.ID)
	}

	if err := cArgs.EvalCtx.GetLimiters().ConcurrentImportRequests.Begin(ctx); err != nil {
		return nil, err
	}
	defer cArgs.EvalCtx.GetLimiters().ConcurrentImportRequests.Finish()

	var iters []engine.SimpleIterator
	for _, file := range args.Files {
		log.VEventf(ctx, 2, "import file %s %s", file.Path, args.Key)

		//dir, err := MakeDumpSink(ctx, file.Dir, cArgs.EvalCtx.ClusterSettings())
		dir, err := cArgs.EvalCtx.GetDumpSink(ctx, file.Dir)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := dir.Close(); err != nil {
				log.Warningf(ctx, "close export storage failed %v", err)
			}
		}()
		if dir.Conf().Provider == roachpb.ExportStorageProvider_Http && args.HttpHeader != "" {
			err := dir.SetConfig(args.HttpHeader)
			if err != nil {
				return nil, err
			}
		}

		const maxAttempts = 3
		var fileContents []byte
		if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			f, err := dir.ReadFile(ctx, file.Path)
			if err != nil {
				return err
			}
			if strings.HasSuffix(file.Path, ".gz") {
				f, err = gzip.NewReader(f)
			}
			if err != nil {
				return err
			}
			defer f.Close()
			fileContents, err = ioutil.ReadAll(f)
			return err
		}); err != nil {
			return nil, errors.Wrapf(err, "fetching %q", file.Path)
		}
		dataSize := int64(len(fileContents))
		log.Eventf(ctx, "fetched file (%s)", humanizeutil.IBytes(dataSize))

		if args.Encryption != nil {
			fileContents, err = DecryptFile(fileContents, args.Encryption.Key)
			if err != nil {
				return nil, err
			}
		}
		if len(file.Sha512) > 0 {
			checksum, err := SHA512ChecksumData(fileContents)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(checksum, file.Sha512) {
				return nil, errors.Errorf("checksum mismatch for %s", file.Path)
			}
		}
		iter, err := engine.NewMemSSTIterator(fileContents, false)
		if err != nil {
			return nil, err
		}

		defer iter.Close()
		iters = append(iters, iter)
	}

	batcher, err := bulk.MakeSSTBatcher(ctx, db, MaxImportBatchSize(cArgs.EvalCtx.ClusterSettings()))
	if err != nil {
		return nil, err
	}
	defer batcher.Close()

	startKeyMVCC, endKeyMVCC := engine.MVCCKey{Key: args.DataSpan.Key}, engine.MVCCKey{Key: args.DataSpan.EndKey}
	iter := engine.MakeMultiIterator(iters)
	defer iter.Close()
	var keyScratch, valueScratch []byte

	for iter.Seek(startKeyMVCC); ; {
		ok, err := iter.Valid()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		if args.EndTime != (hlc.Timestamp{}) {
			// TODO(dan): If we have to skip past a lot of versions to find the
			// latest one before args.EndTime, then this could be slow.
			if args.EndTime.Less(iter.UnsafeKey().Timestamp) {
				iter.Next()
				continue
			}
		}

		if !ok || !iter.UnsafeKey().Less(endKeyMVCC) {
			break
		}
		if len(iter.UnsafeValue()) == 0 {
			// Value is deleted.
			iter.NextKey()
			continue
		}

		keyScratch = append(keyScratch[:0], iter.UnsafeKey().Key...)
		valueScratch = append(valueScratch[:0], iter.UnsafeValue()...)
		key := engine.MVCCKey{Key: keyScratch, Timestamp: iter.UnsafeKey().Timestamp}
		value := roachpb.Value{RawBytes: valueScratch}
		iter.NextKey()

		key.Key, ok, err = kr.RewriteKey(key.Key)
		if err != nil {
			return nil, err
		}
		if !ok {
			// If the key rewriter didn't match this key, it's not data for the
			// table(s) we're interested in.
			if log.V(3) {
				log.Infof(ctx, "skipping %s %s", key.Key, value.PrettyPrint())
			}
			continue
		}

		// Rewriting the key means the checksum needs to be updated.
		value.ClearChecksum()
		value.InitChecksum(key.Key)

		if log.V(3) {
			log.Infof(ctx, "Put %s -> %s", key.Key, value.PrettyPrint())
		}
		if err := batcher.AddMVCCKey(ctx, key, value.RawBytes); err != nil {
			return nil, errors.Wrapf(err, "adding to batch: %s -> %s", key, value.PrettyPrint())
		}
	}
	// Flush out the last batch.
	if err := batcher.Flush(ctx); err != nil {
		return nil, err
	}
	log.Event(ctx, "done")
	return &roachpb.ImportResponse{Imported: batcher.GetSummary()}, nil
}
