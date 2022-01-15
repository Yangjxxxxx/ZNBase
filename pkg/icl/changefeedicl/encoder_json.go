/*
Package changefeedicl add by jerry
* @Author       :jerry
* @Version      :1.0.0
* @Date         :下午3:00 19-8-27
* @Description  : jsonEncoder implement the interface of Encoder
*/
package changefeedicl

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"math"
	"strconv"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/json"
)

// jsonEncoder is an implement of Encoder in file encoder.go.
// jsonEncoder encodes cdc entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__exdb__` key in the top-level JSON object.
type jsonEncoder struct {
	updatedField, beforeField, wrapped, keyOnly, keyInValue, operationType bool

	alloc sqlbase.DatumAlloc
	buf   bytes.Buffer
}

var _ Encoder = &jsonEncoder{}

/**
create an instance of jsonEncoder
*/
func makeJSONEncoder(opts map[string]string) (*jsonEncoder, error) {
	e := &jsonEncoder{
		keyOnly: envelopeType(opts[optEnvelope]) == optEnvelopeKeyOnly,
		wrapped: envelopeType(opts[optEnvelope]) == optEnvelopeWrapped,
	}
	_, e.updatedField = opts[optUpdatedTimestamps]
	_, e.beforeField = opts[optDiff]
	_, e.operationType = opts[optType]
	if e.operationType && !e.beforeField {
		return nil, errors.Errorf(`%s is only usable with %s=%t`,
			optType, optDiff, true)
	}
	if e.beforeField && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			optDiff, optEnvelope, optEnvelopeWrapped)
	}
	_, e.keyInValue = opts[optKeyInValue]
	if e.keyInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			optKeyInValue, optEnvelope, optEnvelopeWrapped)
	}
	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *jsonEncoder) EncodeKey(row encodeRow) ([]byte, error) {
	colIdxByID := row.tableDesc.ColumnIdxMap()
	jsonEntries := make([]interface{}, len(row.tableDesc.PrimaryIndex.ColumnIDs))
	for i, colID := range row.tableDesc.PrimaryIndex.ColumnIDs {
		idx, ok := colIdxByID[colID]
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		datum, col := row.datums[idx], row.tableDesc.Columns[idx]
		if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[i], err = tree.AsJSON(datum.Datum)
		if err != nil {
			return nil, err
		}
	}
	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

func (e *jsonEncoder) encodeKeyRaw(row encodeRow) ([]interface{}, error) {
	colIdxByID := row.tableDesc.ColumnIdxMap()
	jsonEntries := make([]interface{}, len(row.tableDesc.PrimaryIndex.ColumnIDs))
	for i, colID := range row.tableDesc.PrimaryIndex.ColumnIDs {
		idx, ok := colIdxByID[colID]
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		datum, col := row.datums[idx], &row.tableDesc.Columns[idx]
		if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[i], err = tree.AsJSON(datum.Datum)
		if err != nil {
			return nil, err
		}
	}
	return jsonEntries, nil
}

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(
	ctx context.Context, row encodeRow, DB *client.DB,
) ([]byte, bool, sqlbase.DescriptorProto, string, string, TableOperate, error) {
	encoderDDL := false
	var descProto sqlbase.DescriptorProto
	var databaseName, schemaName string
	var operate TableOperate
	var err error
	if e.keyOnly || (!e.wrapped && row.deleted) {
		return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, nil
	}

	if row.tableDesc.ID == keys.DescriptorTableID {
		descTs := row.updated
		descProto, databaseName, schemaName, err = parseDescriptorProto(ctx, row.tableDesc.Columns, row.datums, DB, descTs)
		if err != nil {
			return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, err
		}
		ddlEventJSON, err := gojson.Marshal(row.ddlEvent)
		if err != nil {
			return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, err
		}
		return ddlEventJSON, true, descProto, databaseName, schemaName, row.ddlEvent.Operate, nil
	}
	var after map[string]interface{}
	if !row.deleted {
		columns := row.tableDesc.Columns
		after = make(map[string]interface{}, len(columns))
		for i, col := range columns {
			datum := row.datums[i]

			if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
				return nil, encoderDDL, nil, databaseName, schemaName, UnknownOperation, err
			}
			var err error
			err = formatInfAndNan(col, &datum.Datum)
			if err != nil {
				return nil, encoderDDL, nil, databaseName, schemaName, UnknownOperation, err
			}
			if !(row.tableDesc.ID == keys.DescriptorTableID && col.Name == "descriptor") {
				after[col.Name], err = tree.AsJSON(datum.Datum)
			}
			if err != nil {
				return nil, encoderDDL, nil, databaseName, schemaName, UnknownOperation, err
			}
		}
	}

	var before map[string]interface{}
	if row.prevDatums != nil && !row.prevDeleted {
		columns := row.prevTableDesc.Columns
		before = make(map[string]interface{}, len(columns))
		for i := range columns {
			col := &columns[i]
			datum := row.prevDatums[i]
			if row.prevTableDesc.ID == keys.DescriptorTableID {
				encoderDDL = true
				desc, err := parseDescriptor(*col, datum.Datum, row.updated)
				if err != nil {
					return nil, encoderDDL, nil, databaseName, schemaName, UnknownOperation, err
				}
				if desc != nil {
					//TODO 包装desc信息，发送到各个sink
				}
			}
			if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
				return nil, encoderDDL, nil, databaseName, schemaName, UnknownOperation, err
			}
			var err error
			if !(row.tableDesc.ID == keys.DescriptorTableID && col.Name == "descriptor") {
				before[col.Name], err = tree.AsJSON(datum.Datum)
			}
			if err != nil {
				return nil, encoderDDL, nil, databaseName, schemaName, UnknownOperation, err
			}
		}
	}

	var jsonEntries map[string]interface{}
	if e.wrapped {
		if after != nil {
			jsonEntries = map[string]interface{}{`after`: after}
		} else {
			jsonEntries = map[string]interface{}{`after`: nil}
		}
		if e.beforeField {
			if before != nil {
				jsonEntries[`before`] = before
			} else {
				jsonEntries[`before`] = nil
			}
		}
		if e.keyInValue {
			keyEntries, err := e.encodeKeyRaw(row)
			if err != nil {
				return nil, encoderDDL, nil, databaseName, schemaName, UnknownOperation, err
			}
			jsonEntries[`key`] = keyEntries
		}
		if e.operationType && e.beforeField {
			if row.deleted {
				jsonEntries[`operate`] = `DELETE`
			} else if before == nil {
				jsonEntries[`operate`] = `INSERT`
			} else {
				jsonEntries[`operate`] = `UPDATE`
			}
		}
	} else {
		jsonEntries = after
	}

	if e.updatedField {
		var meta map[string]interface{}
		if e.wrapped {
			meta = jsonEntries
		} else {
			meta = make(map[string]interface{}, 1)
			jsonEntries[jsonMetaSentinel] = meta
		}
		meta[`updated`] = row.updated.AsOfSystemTime()
	}

	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, encoderDDL, nil, databaseName, schemaName, UnknownOperation, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), encoderDDL, descProto, databaseName, schemaName, operate, nil
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *jsonEncoder) EncodeResolvedTimestamp(_ string, resolved hlc.Timestamp) ([]byte, error) {
	meta := map[string]interface{}{
		`resolved`: tree.TimestampToDecimal(resolved).Decimal.String(),
	}
	var jsonEntries interface{}
	if e.wrapped {
		jsonEntries = meta
	} else {
		jsonEntries = map[string]interface{}{
			jsonMetaSentinel: meta,
		}
	}
	return gojson.Marshal(jsonEntries)
}

//formatInfAndNan format +Inf,-Inf,NAN to string
func formatInfAndNan(col sqlbase.ColumnDescriptor, datum *tree.Datum) error {
	switch col.Type.SemanticType {
	case sqlbase.ColumnType_FLOAT:
		dFloat, ok := tree.AsDFloat(*datum)
		if ok {
			if math.IsInf(float64(dFloat), -1) || math.IsInf(float64(dFloat), 1) || math.IsNaN(float64(dFloat)) {
				*datum = tree.NewDString(dFloat.String())
			}
		}
	case sqlbase.ColumnType_DECIMAL:
		dDecimal, ok := tree.AsDDecimal(*datum)
		if ok {
			floatDecimal, err := dDecimal.Float64()
			if err != nil {
				return err
			}
			if math.IsInf(floatDecimal, -1) || math.IsInf(floatDecimal, 1) || math.IsNaN(floatDecimal) {
				*datum = tree.NewDString(strconv.FormatFloat(floatDecimal, 'G', -1, 64))
			}
		}
	}
	return nil
}
