package load

import (
	"context"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/encoding/csv"
	"github.com/znbasedb/znbase/pkg/util/log"
)

const tableRegionSizeWarningThreshold int64 = 1024 * 1024 * 1024

// Chunk represents a portion of the data file.

type regionSlice []*encoding.TableRegion

// Len Number of current TableRegion
func (rs regionSlice) Len() int {
	return len(rs)
}
func (rs regionSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}
func (rs regionSlice) Less(i, j int) bool {
	if rs[i].File == rs[j].File {
		return rs[i].Chunk.Offset < rs[j].Chunk.Offset
	}
	return rs[i].File < rs[j].File
}

// MakeTableRegions Logically split the table data, the default size is 2GB
func MakeTableRegions(
	ctx context.Context, meta *jobspb.ImportDetails, p sql.PlanHookState, files []string, jobID int64,
) ([]*encoding.TableRegion, error) {
	// Split files into regions
	meta.URIs = files
	filesRegions := make(regionSlice, 0, len(meta.URIs))
	if meta.Format.Format == roachpb.IOFileFormat_Kafka {
		for _, file := range files {
			urlParse, err := url.Parse(file)
			if err != nil {
				return nil, err
			}
			if urlParse.RawQuery == "" {
				return filesRegions, errors.Errorf("the topic %s corresponding to kafka cannot be empty", file)
			}
			q := urlParse.Query()
			topic := q.Get(sinkParamTopic)
			kafkaSink, err := NewKafkaSink(ctx, []string{file}, strconv.FormatInt(jobID, 10))
			if err != nil {
				return filesRegions, errors.Wrap(err, file)
			}
			defer kafkaSink.Close()
			//获取当前topic 对应的partitions
			partitions, err := kafkaSink.consumer.Partitions(topic)
			if err != nil {
				return filesRegions, errors.Wrap(err, file)
			}
			for _, partition := range partitions {
				//For kafka, offset corresponds to partition
				chunk := encoding.Chunk{Offset: int64(partition), EndOffset: jobID}
				tableRegion := &encoding.TableRegion{
					DB:    "",
					Table: meta.Tables[0].Name,
					File:  file,
					Chunk: chunk,
				}
				filesRegions = append(filesRegions, tableRegion)
			}
		}
		return filesRegions, nil
	}
	prevRowIDMax := int64(0)
	for _, dataFile := range meta.URIs {
		conf, err := dumpsink.ConfFromURI(ctx, dataFile)
		if err != nil {
			return nil, err
		}
		makeDumpSink := p.ExecCfg().DistSQLSrv.DumpSink
		es, err := makeDumpSink(ctx, conf)
		if err != nil {
			return nil, err
		}
		sz, err := es.Size(ctx, "")

		isCsvFile := strings.HasSuffix(strings.ToLower(dataFile), ".csv")
		// If a csv file is overlarge, we need to split it into mutiple regions.
		// Note: We can only split a csv file whose format is strict and header is empty.
		maxRegionSize := storageicl.MaxLoadSplitSize(p.ExecCfg().Settings)
		if isCsvFile && sz > maxRegionSize {
			var (
				regions []*encoding.TableRegion
			)
			prevRowIDMax, regions, err = SplitLargeFile(ctx, meta, dataFile, sz, prevRowIDMax, makeDumpSink, maxRegionSize)
			if err != nil {
				return nil, err
			}
			filesRegions = append(filesRegions, regions...)
			continue
		}

		tableRegion := &encoding.TableRegion{
			DB:    "",
			Table: meta.Tables[0].Name,
			File:  dataFile,
			Chunk: encoding.Chunk{
				Offset:    0,
				EndOffset: sz,
				Size:      sz,
			},
		}
		filesRegions = append(filesRegions, tableRegion)
		if tableRegion.Size() > tableRegionSizeWarningThreshold {
			log.Warningf(
				ctx,
				"%v is too big to be processed efficiently; we suggest splitting it at 256 MB each",
				dataFile)
		}
	}

	return filesRegions, nil
}

// SplitLargeFile splits a large csv file into multiple regions, the size of
// each regions is specified by `config.MaxRegionSize`.
// Note: We split the file coarsely, thus the format of csv file is needed to be
// strict.
// e.g.
// - CSV file with header is invalid
// - a complete tuple split into multiple lines is invalid
func SplitLargeFile(
	ctx context.Context,
	meta *jobspb.ImportDetails,
	dataFilePath string,
	dataFileSize int64,
	prevRowIdxMax int64,
	makeDumpSink dumpsink.Factory,
	maxRegionSize int64,
) (prevRowIDMax int64, regions []*encoding.TableRegion, err error) {
	startOffset, endOffset := int64(0), maxRegionSize
	for {
		if endOffset != dataFileSize {
			//读取整个文件进行逻辑上分片操作
			conf, err := dumpsink.ConfFromURI(ctx, dataFilePath)
			if err != nil {
				return 0, nil, err
			}
			es, err := makeDumpSink(ctx, conf)
			if err != nil {
				return 0, nil, err
			}
			defer es.Close()
			f, err := es.Seek(ctx, "", endOffset, dataFileSize, io.SeekStart)
			if err != nil {
				return 0, nil, err
			}
			reader := csv.NewReader(f)
			endOffset, err = reader.ReadUntil(endOffset)
			if err != nil {
				return 0, nil, err
			}

		}
		regions = append(regions,
			&encoding.TableRegion{
				DB:    "",
				Table: meta.Tables[0].Desc.Name,
				File:  dataFilePath,
				Chunk: encoding.Chunk{
					Offset:    startOffset,
					EndOffset: endOffset,
					Size:      dataFileSize,
				},
			})
		if endOffset == dataFileSize {
			break
		}
		startOffset = endOffset
		if endOffset += maxRegionSize; endOffset > dataFileSize {
			endOffset = dataFileSize
		}
	}
	return prevRowIdxMax, regions, nil
}
