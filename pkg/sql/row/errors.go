// Copyright 2018  The Cockroach Authors.
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

package row

import (
	"context"
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
)

// singleKVFetcher is a kvBatchFetcher that returns a single kv.
type singleKVFetcher struct {
	kvs  [1]roachpb.KeyValue
	done bool
}

// nextBatch implements the kvBatchFetcher interface.
func (f *singleKVFetcher) nextBatch(
	_ context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, span roachpb.Span, err error) {
	if f.done {
		return false, nil, nil, roachpb.Span{}, nil
	}
	f.done = true
	return true, f.kvs[:], nil, roachpb.Span{}, nil
}

// GetRangesInfo implements the kvBatchFetcher interface.
func (f *singleKVFetcher) GetRangesInfo() []roachpb.RangeInfo {
	panic("GetRangesInfo() called on singleKVFetcher")
}

// ConvertBatchError returns a user friendly constraint violation error.
func ConvertBatchError(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor, b *client.Batch,
) error {
	origPErr := b.MustPErr()
	if origPErr.Index == nil {
		return origPErr.GoError()
	}
	j := origPErr.Index.Index
	if j >= int32(len(b.Results)) {
		panic(fmt.Sprintf("index %d outside of results: %+v", j, b.Results))
	}
	result := b.Results[j]
	if cErr, ok := origPErr.GetDetail().(*roachpb.ConditionFailedError); ok && len(result.Rows) > 0 {
		key := result.Rows[0].Key
		return NewUniquenessConstraintViolationError(ctx, tableDesc, key, cErr.ActualValue)
	}
	return origPErr.GoError()
}

// NewUniquenessConstraintViolationError creates an error that represents a
// violation of a UNIQUE constraint.
func NewUniquenessConstraintViolationError(
	ctx context.Context,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	key roachpb.Key,
	value *roachpb.Value,
) error {
	index, names, values, err := DecodeRowInfo(ctx, tableDesc, key, value, false)
	if err != nil {
		return pgerror.Newf(pgcode.UniqueViolation,
			"duplicate key value: decoding err=%s", err)
	}

	if index.ID == 1 || tableDesc.PrimaryIndex.ID == index.ID {
		return pgerror.NewErrorf(pgcode.UniqueViolation,
			"相同的键值 %s=%s 违反了主键约束 %q",
			strings.Join(names, ","),
			strings.Join(values, ","),
			index.Name)
	}

	return pgerror.NewErrorf(pgcode.UniqueViolation,
		"相同的键值 %s=%s 违反了唯一性约束 %q",
		strings.Join(names, ","),
		strings.Join(values, ","),
		index.Name)
}

// KeyToDescTranslator is capable of translating a key found in an error to a
// table descriptor for error reporting.
type KeyToDescTranslator interface {
	// KeyToDesc attempts to translate the key found in an error to a table
	// descriptor. An implementation can return (nil, false) if the translation
	// failed because the key is not part of a table it was scanning, but is
	// instead part of an interleaved relative (parent/sibling/child) table.
	KeyToDesc(roachpb.Key) (*sqlbase.ImmutableTableDescriptor, bool)
}

// ConvertFetchError attempts to a map key-value error generated during a
// key-value fetch to a user friendly SQL error.
func ConvertFetchError(ctx context.Context, descForKey KeyToDescTranslator, err error) error {
	if wiErr, ok := err.(*roachpb.WriteIntentError); ok {
		key := wiErr.Intents[0].Key
		desc, _ := descForKey.KeyToDesc(key)
		return NewLockNotAvailableError(ctx, desc, key)
	}
	return err
}

// NewLockNotAvailableError creates an error that represents an inability to
// acquire a lock. A nil tableDesc can be provided, which indicates that the
// table descriptor corresponding to the key is unknown due to a table
// interleaving.
func NewLockNotAvailableError(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor, key roachpb.Key,
) error {
	if tableDesc == nil {
		return pgerror.NewErrorf(pgcode.LockNotAvailable,
			"could not obtain lock on row in interleaved table")
	}

	index, colNames, values, err := DecodeRowInfo(ctx, tableDesc, key, nil, false)
	if err != nil {
		return pgerror.NewErrorf(pgcode.LockNotAvailable,
			"could not obtain lock on row: decoding err=%s", err)
	}

	return pgerror.NewErrorf(pgcode.LockNotAvailable,
		"could not obtain lock on row (%s)=(%s) in %s@%s",
		strings.Join(colNames, ","),
		strings.Join(values, ","),
		tableDesc.GetName(),
		index.Name)
}

// DecodeRowInfo takes a table descriptor, a key, and an optional value and
// returns information about the corresponding SQL row. If successful, the index
// and corresponding column names and values to the provided KV are returned.
func DecodeRowInfo(
	ctx context.Context,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	key roachpb.Key,
	value *roachpb.Value,
	allColumns bool,
) (_ *sqlbase.IndexDescriptor, columnNames []string, columnValues []string, _ error) {
	indexID, _, err := sqlbase.DecodeIndexKeyPrefix(tableDesc.TableDesc(), key)
	if err != nil {
		return nil, nil, nil, err
	}
	index, err := tableDesc.FindIndexByID(indexID)
	if err != nil {
		return nil, nil, nil, err
	}
	var rf Fetcher

	colIDs := index.ColumnIDs
	if allColumns {
		if index.ID == tableDesc.GetPrimaryIndex().ID {
			publicColumns := tableDesc.GetColumns()
			colIDs = make([]sqlbase.ColumnID, len(publicColumns))
			for i := range publicColumns {
				colIDs[i] = publicColumns[i].ID
			}
		} else {
			colIDs, _ = index.FullColumnIDs()
			colIDs = append(colIDs, index.StoreColumnIDs...)
		}
	}
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(colIDs)-1)

	colIdxMap := make(map[sqlbase.ColumnID]int, len(colIDs))
	cols := make([]sqlbase.ColumnDescriptor, len(colIDs))
	for i, colID := range colIDs {
		colIdxMap[colID] = i
		col, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, nil, nil, err
		}
		cols[i] = *col
	}

	tableArgs := FetcherTableArgs{
		Desc:             tableDesc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: indexID != tableDesc.GetPrimaryIndex().ID,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := rf.Init(
		false, /* reverse */
		false, /* isCheck */
		false,
		&sqlbase.DatumAlloc{},
		sqlbase.ScanLockingStrength_FOR_NONE,
		sqlbase.ScanLockingWaitPolicy{LockLevel: sqlbase.ScanLockingWaitLevel_BLOCK},
		tableArgs,
	); err != nil {
		return nil, nil, nil, err
	}
	f := singleKVFetcher{kvs: [1]roachpb.KeyValue{{Key: key}}}
	if value != nil {
		f.kvs[0].Value = *value
	}
	// Use the Fetcher to decode the single kv pair above by passing in
	// this singleKVFetcher implementation, which doesn't actually hit KV.
	if err := rf.StartScanFrom(ctx, &f); err != nil {
		return nil, nil, nil, err
	}
	datums, _, _, err := rf.NextRowDecoded(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	names := make([]string, len(cols))
	values := make([]string, len(cols))
	for i := range cols {
		names[i] = cols[i].Name
		if datums[i] == tree.DNull {
			continue
		}
		values[i] = datums[i].String()
	}
	return index, names, values, nil
}
