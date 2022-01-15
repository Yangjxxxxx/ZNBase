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

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

// encodeRow holds all the pieces necessary to encode a row change into a key or
// value.
type encodeRow struct {
	// datums is the new value of a changed table row.
	datums sqlbase.EncDatumRow
	// updated is the mvcc timestamp corresponding to the latest update in
	// `datums`.
	updated hlc.Timestamp
	// deleted is true if row is a deletion. In this case, only the primary
	// key columns are guaranteed to be set in `datums`.
	deleted bool
	// tableDesc is a TableDescriptor for the table containing `datums`.
	// It's valid for interpreting the row at `updated`.
	tableDesc *sqlbase.TableDescriptor
	// prevDatums is the old value of a changed table row. The field is set
	// to nil if the before value for changes was not requested (optDiff).
	prevDatums sqlbase.EncDatumRow
	// prevDeleted is true if prevDatums is missing or is a deletion.
	prevDeleted bool
	// prevTableDesc is a TableDescriptor for the table containing `prevDatums`.
	// It's valid for interpreting the row at `updated.Prev()`.
	prevTableDesc *sqlbase.TableDescriptor

	ddlEvent DDLEvent

	source Source
}

//Source DML 数据来源，分为：临时文件，和rangefeed
type Source int32

const (
	//FromRangeFeed 来源为:rangefeed
	FromRangeFeed Source = 0
	//FromTmpFile 来源为:临时文件
	FromTmpFile Source = 1
)

// Encoder turns a row into a serialized cdc key, value, or resolved
// timestamp. It represents one of the `format=` cdc options.
type Encoder interface {
	// EncodeKey encodes the primary key of the given row. The columns of the
	// datums are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`, but only the primary key fields will be used. The
	// returned bytes are only valid until the next call to Encode*.
	EncodeKey(encodeRow) ([]byte, error)
	// EncodeValue encodes the primary key of the given row. The columns of the
	// datums are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeValue(context.Context, encodeRow, *client.DB) ([]byte, bool, sqlbase.DescriptorProto, string, string, TableOperate, error)
	// EncodeResolvedTimestamp encodes a resolved timestamp payload for the
	// given topic name. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeResolvedTimestamp(string, hlc.Timestamp) ([]byte, error)
}

// create an encoder and return
func getEncoder(opts map[string]string) (Encoder, error) {
	switch formatType(opts[optFormat]) {
	case ``, optFormatJSON:
		return makeJSONEncoder(opts)
	case optFormatAvro:
		return newAvroEncoder(opts)
	default:
		return nil, errors.Errorf(`unknown %s: %s`, optFormat, opts[optFormat])
	}
}
