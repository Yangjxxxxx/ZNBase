// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra

import (
	"context"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// StreamEncoder converts EncDatum rows into a sequence of ProducerMessage.
//
// Sample usage:
//   se := StreamEncoder{}
//
//   for {
//       for ... {
//          err := se.AddRow(...)
//          ...
//       }
//       msg := se.FormMessage(nil)
//       // Send out message.
//       ...
//   }
type StreamEncoder struct {
	// infos is fully initialized when the first row is received.
	infos            []distsqlpb.DatumInfo
	infosInitialized bool

	rowBuf       []byte
	numEmptyRows int
	metadata     []distsqlpb.RemoteProducerMetadata

	// headerSent is set after the first message (which contains the header) has
	// been sent.
	headerSent bool
	// typingSent is set after the first message that contains any rows has been
	// sent.
	typingSent bool
	alloc      sqlbase.DatumAlloc

	// Preallocated structures to avoid allocations.
	msg    distsqlpb.ProducerMessage
	msgHdr distsqlpb.ProducerHeader
}

//unused
// HasHeaderBeenSent returns whether the header has been sent.
//func (se *StreamEncoder) HasHeaderBeenSent() bool {
//	return se.headerSent
//}

// SetHeaderFields sets the header fields.
func (se *StreamEncoder) SetHeaderFields(flowID distsqlpb.FlowID, streamID distsqlpb.StreamID) {
	se.msgHdr.FlowID = flowID
	se.msgHdr.StreamID = streamID
}

// Init initializes the encoder.
func (se *StreamEncoder) Init(types []types.T) {
	se.infos = make([]distsqlpb.DatumInfo, len(types))
	for i := range types {
		ct, _ := sqlbase.DatumTypeToColumnType(types[i])
		se.infos[i].Type = ct
	}
}

func (se *StreamEncoder) init(types []sqlbase.ColumnType) {
	se.infos = make([]distsqlpb.DatumInfo, len(types))
	for i := range types {
		se.infos[i].Type = types[i]
	}
}

// AddMetadata encodes a metadata message. Unlike AddRow(), it cannot fail. This
// is important for the caller because a failure to encode a piece of metadata
// (particularly one that contains an error) would not be recoverable.
//
// Metadata records lose their ordering wrt the data rows. The convention is
// that the StreamDecoder will return them first, before the data rows, thus
// ensuring that rows produced _after_ an error are not received _before_ the
// error.
func (se *StreamEncoder) AddMetadata(ctx context.Context, meta distsqlpb.ProducerMetadata) {
	var enc distsqlpb.RemoteProducerMetadata
	if meta.Ranges != nil {
		enc.Value = &distsqlpb.RemoteProducerMetadata_RangeInfo{
			RangeInfo: &distsqlpb.RemoteProducerMetadata_RangeInfos{
				RangeInfo: meta.Ranges,
			},
		}
	} else if meta.TraceData != nil {
		enc.Value = &distsqlpb.RemoteProducerMetadata_TraceData_{
			TraceData: &distsqlpb.RemoteProducerMetadata_TraceData{
				CollectedSpans: meta.TraceData,
			},
		}
	} else if meta.TxnCoordMeta != nil {
		enc.Value = &distsqlpb.RemoteProducerMetadata_TxnCoordMeta{
			TxnCoordMeta: meta.TxnCoordMeta,
		}
	} else if meta.RowNum != nil {
		enc.Value = &distsqlpb.RemoteProducerMetadata_RowNum_{
			RowNum: meta.RowNum,
		}
	} else if meta.SamplerProgress != nil {
		enc.Value = &distsqlpb.RemoteProducerMetadata_SamplerProgress_{
			SamplerProgress: meta.SamplerProgress,
		}
	} else {
		enc.Value = &distsqlpb.RemoteProducerMetadata_Error{
			Error: distsqlpb.NewError(meta.Err),
		}
	}
	se.metadata = append(se.metadata, enc)
}

// AddRow encodes a message.
func (se *StreamEncoder) AddRow(row sqlbase.EncDatumRow) error {
	if se.infos == nil {
		panic("Init not called")
	}
	if len(se.infos) != len(row) {
		return errors.Errorf("inconsistent row length: expected %d, got %d", len(se.infos), len(row))
	}
	if !se.infosInitialized {
		// First row. Initialize encodings.
		for i := range row {
			enc, ok := row[i].Encoding()
			if !ok {
				enc = PreferredEncoding
			}
			sType := se.infos[i].Type.SemanticType
			if enc != sqlbase.DatumEncoding_VALUE &&
				(sqlbase.HasCompositeKeyEncoding(sType) || sqlbase.MustBeValueEncoded(sType)) {
				// Force VALUE encoding for composite types (key encodings may lose data).
				enc = sqlbase.DatumEncoding_VALUE
			}
			se.infos[i].Encoding = enc
		}
		se.infosInitialized = true
	}
	if len(row) == 0 {
		se.numEmptyRows++
		return nil
	}
	for i := range row {
		var err error
		se.rowBuf, err = row[i].Encode(&se.infos[i].Type, &se.alloc, se.infos[i].Encoding, se.rowBuf)
		if err != nil {
			return err
		}
	}
	return nil
}

// FormMessage populates a message containing the rows added since the last call
// to FormMessage. The returned ProducerMessage should be treated as immutable.
func (se *StreamEncoder) FormMessage(ctx context.Context) *distsqlpb.ProducerMessage {
	msg := &se.msg
	msg.Header = nil
	msg.Data.RawBytes = se.rowBuf
	msg.Data.NumEmptyRows = int32(se.numEmptyRows)
	msg.Data.Metadata = make([]distsqlpb.RemoteProducerMetadata, len(se.metadata))
	copy(msg.Data.Metadata, se.metadata)
	se.metadata = se.metadata[:0]

	if !se.headerSent {
		msg.Header = &se.msgHdr
		se.headerSent = true
	}
	if !se.typingSent {
		if se.infosInitialized {
			msg.Typing = se.infos
			se.typingSent = true
		}
	} else {
		msg.Typing = nil
	}

	se.rowBuf = se.rowBuf[:0]
	se.numEmptyRows = 0
	return msg
}
