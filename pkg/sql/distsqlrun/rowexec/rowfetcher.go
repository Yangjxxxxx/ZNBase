// Copyright 2019  The Cockroach Authors.

package rowexec

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

// rowFetcher is an interface used to abstract a row fetcher so that a stat
// collector wrapper can be plugged in.
type rowFetcher interface {
	StartScan(
		_ context.Context, _ *client.Txn, _ roachpb.Spans, limitBatches bool, limitHint int64, traceKV bool,
	) error
	StartInconsistentScan(
		_ context.Context,
		_ *client.DB,
		initialTimestamp hlc.Timestamp,
		maxTimestampAge time.Duration,
		spans roachpb.Spans,
		limitBatches bool,
		limitHint int64,
		traceKV bool,
	) error

	NextRow(ctx context.Context) (
		sqlbase.EncDatumRow, *sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, error)

	// PartialKey is not stat-related but needs to be supported.
	PartialKey(int) (roachpb.Key, error)
	Reset()
	GetBytesRead() int64
	GetRangesInfo() []roachpb.RangeInfo
	NextRowWithErrors(context.Context) (sqlbase.EncDatumRow, error)
	//StartGetBatch read blocks of kvs until nil
	StartGetBatch(
		ctx context.Context,
		txn *client.Txn,
		spans roachpb.Spans,
		limitBatches bool,
		limitHint int64,
		traceKV bool,
		response chan roachpb.BlockStruct,
		errCh chan error,
		switchCount bool,
	) error

	// GetDesc return the table's name
	GetDesc() string
	CountRet() int
	MakeCount()

	//GetIfFunc return the index if func
	GetIfFuncIndex() bool
}

// initRowFetcher initializes the fetcher.
func initRowFetcher(
	fetcher *row.Fetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
	scanVisibility distsqlpb.ScanVisibility,
	lockStrength sqlbase.ScanLockingStrength,
	lockWaitPolicy sqlbase.ScanLockingWaitPolicy,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*desc)
	index, isSecondaryIndex, err = immutDesc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	cols := immutDesc.Columns
	if scanVisibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC {
		cols = immutDesc.ReadableColumns
	}
	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		reverseScan,
		true, /* returnRangeInfo */
		isCheck,
		alloc,
		lockStrength,
		lockWaitPolicy, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}
