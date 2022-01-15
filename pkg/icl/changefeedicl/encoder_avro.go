/*
Package changefeedicl add by jerry
* @Author       :jerry
* @Version      :1.0.0
* @Date         :下午2:56 19-8-27
* @Description  : avroEncoder implement the interface of Encoder. And you should especially known that this AVRO format
									is ONLY used for confluent.
*/
package changefeedicl

import (
	"bytes"
	"context"
	"encoding/binary"
	gojson "encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

const (
	avroSchemaContentType  = `application/vnd.schemaregistry.v1+json`
	avroSubjectSuffixKey   = `-key`
	avroSubjectSuffixValue = `-value`
	avroWireFormatMagic    = byte(0)
)

type registeredKeySchema struct {
	schema     *avroDataRecord
	registryID int32
}

type registeredEnvelopeSchema struct {
	schema     *avroEnvelopeRecord
	registryID int32
}

type tableIDAndVersion uint64
type tableIDAndVersionPair [2]tableIDAndVersion // [before, after]
// avroEncoder encodes cdc entries as Avro's binary or textual
// JSON format. Keys are the primary key columns in a record. Values are all
// columns in a record.
type avroEncoder struct {
	registryURL                                                string
	updatedField, beforeField, keyOnly, operationType, haveDdl bool

	keyCache      map[tableIDAndVersion]registeredKeySchema
	valueCache    map[tableIDAndVersionPair]registeredEnvelopeSchema
	resolvedCache map[string]registeredEnvelopeSchema
}

var _ Encoder = &avroEncoder{}

// combine table id with version. Table ID as the high 32-bit. The tableIDAndVersion result is returned as cacheKey.
func makeTableIDAndVersion(id sqlbase.ID, version sqlbase.DescriptorVersion) tableIDAndVersion {
	return tableIDAndVersion(id)<<32 + tableIDAndVersion(version)
}

// create and return an avroEncoder instance
func newAvroEncoder(opts map[string]string) (*avroEncoder, error) {
	registryURL := opts[optSchemaRegistry]
	if len(registryURL) == 0 {
		return nil, errors.Errorf(`WITH option %s is required for %s=%s`,
			optSchemaRegistry, optFormat, optFormatAvro)
	}
	e := &avroEncoder{registryURL: registryURL}

	switch opts[optEnvelope] {
	case string(optEnvelopeKeyOnly):
		e.keyOnly = true
	case string(optEnvelopeWrapped):
	default:
		return nil, errors.Errorf(`%s=%s is not supported with %s=%s`,
			optEnvelope, opts[optEnvelope], optFormat, optFormatAvro)
	}
	_, e.updatedField = opts[optUpdatedTimestamps]
	_, e.beforeField = opts[optDiff]
	_, e.haveDdl = opts[optWithDdl]
	if e.beforeField && e.keyOnly {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			optDiff, optEnvelope, optEnvelopeWrapped)
	}
	_, e.operationType = opts[optType]
	if !e.beforeField && e.operationType {
		return nil, errors.Errorf(`%s is only usable with %s=%t`,
			optType, optDiff, true)
	}
	e.keyCache = make(map[tableIDAndVersion]registeredKeySchema)
	e.valueCache = make(map[tableIDAndVersionPair]registeredEnvelopeSchema)
	e.resolvedCache = make(map[string]registeredEnvelopeSchema)
	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *avroEncoder) EncodeKey(row encodeRow) ([]byte, error) {
	cacheKey := makeTableIDAndVersion(row.tableDesc.ID, row.tableDesc.Version)
	registered, ok := e.keyCache[cacheKey]
	if !ok {
		var err error
		registered.schema, err = indexToAvroSchema(row.tableDesc, &row.tableDesc.PrimaryIndex)
		if err != nil {
			return nil, MarkTerminalError(err)
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(row.tableDesc.Name) + avroSubjectSuffixKey
		registered.registryID, err = e.register(&registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		// TODO(dan): Bound the size of this cache.
		e.keyCache[cacheKey] = registered
	}

	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		avroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, row.datums)
}

// EncodeValue implements the Encoder interface.
func (e *avroEncoder) EncodeValue(
	ctx context.Context, row encodeRow, DB *client.DB,
) ([]byte, bool, sqlbase.DescriptorProto, string, string, TableOperate, error) {
	encoderDDL := false
	var descProto sqlbase.DescriptorProto
	var databaseName, schemaName string
	var operate TableOperate
	var err error
	if row.tableDesc.ID == keys.DescriptorTableID {
		encoderDDL = true
	}
	if e.keyOnly {
		return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, nil
	}

	var cacheKey tableIDAndVersionPair
	if (e.beforeField || e.haveDdl) && row.prevTableDesc != nil {
		cacheKey[0] = makeTableIDAndVersion(row.prevTableDesc.ID, row.prevTableDesc.Version)
	}
	cacheKey[1] = makeTableIDAndVersion(row.tableDesc.ID, row.tableDesc.Version)
	registered, ok := e.valueCache[cacheKey]
	if !ok {
		var beforeDataSchema *avroDataRecord
		if (e.beforeField || e.haveDdl) && row.prevTableDesc != nil {
			var err error
			beforeDataSchema, err = tableToAvroSchema(row.prevTableDesc, `before`)
			if err != nil {
				return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, MarkTerminalError(err)
			}
		}

		afterDataSchema, err := tableToAvroSchema(row.tableDesc, avroSchemaNoSuffix)
		if err != nil {
			return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, MarkTerminalError(err)
		}
		opts := avroEnvelopeOpts{afterField: true, beforeField: e.beforeField, updatedField: e.updatedField, typeField: e.operationType, haveDdl: false}
		if encoderDDL {
			opts = avroEnvelopeOpts{afterField: false, beforeField: e.beforeField, updatedField: e.updatedField, typeField: e.operationType, haveDdl: e.haveDdl}
		}
		registered.schema, err = envelopeToAvroSchema(row.tableDesc.Name, opts, beforeDataSchema, afterDataSchema)
		if err != nil {
			return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(row.tableDesc.Name) + avroSubjectSuffixValue
		registered.registryID, err = e.register(&registered.schema.avroRecord, subject)
		if err != nil {
			return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, err
		}
		// TODO(dan): Bound the size of this cache.
		e.valueCache[cacheKey] = registered
	}
	var meta avroMetadata
	if registered.schema.opts.updatedField {
		meta = map[string]interface{}{
			`updated`: row.updated,
		}
	}
	var beforeDatums, afterDatums sqlbase.EncDatumRow
	if row.prevDatums != nil && !row.prevDeleted {
		beforeDatums = row.prevDatums
	}
	if !row.deleted {
		afterDatums = row.datums
	}
	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		avroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	if registered.schema.opts.typeField {
		var opts string
		opts = "INSERT"
		if beforeDatums != nil && afterDatums == nil {
			opts = "DELETE"
		}
		if beforeDatums != nil && afterDatums != nil {
			opts = "UPDATE"
		}
		if beforeDatums == nil && afterDatums != nil {
			opts = "INSERT"
		}
		meta = map[string]interface{}{
			`operate`: opts,
		}
	}
	if row.tableDesc.ID == keys.DescriptorTableID {
		descProto, databaseName, schemaName, err = registered.schema.after.avroParseDescriptorProto(ctx, afterDatums, DB)
		if err != nil {
			return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, err
		}
		if row.ddlEvent.IsEmpty() {
			ddlEventJSON, err := gojson.Marshal(row.ddlEvent)
			if err != nil {
				return nil, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, err
			}

			meta = map[string]interface{}{
				`ddl`: string(ddlEventJSON),
			}
			binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
			buf, err := registered.schema.BinaryFromRow(header, meta, nil, nil, true)
			return buf, encoderDDL, descProto, databaseName, schemaName, operate, err
		}

	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	buf, err := registered.schema.BinaryFromRow(header, meta, beforeDatums, afterDatums, false)
	return buf, encoderDDL, descProto, databaseName, schemaName, UnknownOperation, err
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *avroEncoder) EncodeResolvedTimestamp(
	topic string, resolved hlc.Timestamp,
) ([]byte, error) {
	registered, ok := e.resolvedCache[topic]
	if !ok {
		opts := avroEnvelopeOpts{resolvedField: true}
		var err error
		registered.schema, err = envelopeToAvroSchema(topic, opts, nil /* before */, nil /* after */)
		if err != nil {
			return nil, err
		}

		// NB: This uses the kafka name escaper because it has to match the name
		// of the kafka topic.
		subject := SQLNameToKafkaName(topic) + avroSubjectSuffixValue
		registered.registryID, err = e.register(&registered.schema.avroRecord, subject)
		if err != nil {
			return nil, err
		}
		// TODO(dan): Bound the size of this cache.
		e.resolvedCache[topic] = registered
	}
	var meta avroMetadata
	if registered.schema.opts.resolvedField {
		meta = map[string]interface{}{
			`resolved`: resolved,
		}
	}
	// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
	header := []byte{
		avroWireFormatMagic,
		0, 0, 0, 0, // Placeholder for the ID.
	}
	binary.BigEndian.PutUint32(header[1:5], uint32(registered.registryID))
	return registered.schema.BinaryFromRow(header, meta, nil /* beforeRow */, nil /* afterRow */, false)
}

func (e *avroEncoder) register(schema *avroRecord, subject string) (int32, error) {
	type schemaVersionRequest struct {
		Schema string `json:"schema"`
	}
	type schemaVersionResponse struct {
		ID int32 `json:"id"`
	}

	registryURL, err := url.Parse(e.registryURL)
	if err != nil {
		return 0, err
	}
	registryURL.Path = filepath.Join(registryURL.EscapedPath(), `subjects`, subject, `versions`)

	schemaStr := schema.codec.Schema()
	if log.V(1) {
		log.Infof(context.TODO(), "registering avro schema %s %s", registryURL, schemaStr)
	}

	req := schemaVersionRequest{Schema: schemaStr}
	var buf bytes.Buffer
	if err := gojson.NewEncoder(&buf).Encode(req); err != nil {
		return 0, err
	}

	resp, err := http.Post(registryURL.String(), avroSchemaContentType, &buf)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := ioutil.ReadAll(resp.Body)
		return 0, errors.Errorf(`registering schema to %s %s: %s`, registryURL.String(), resp.Status, body)
	}
	var res schemaVersionResponse
	if err := gojson.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, err
	}

	return res.ID, nil
}
func (r *avroDataRecord) avroParseDescriptorProto(
	ctx context.Context, row sqlbase.EncDatumRow, DB *client.DB,
) (sqlbase.DescriptorProto, string, string, error) {
	var desc sqlbase.DescriptorProto
	var databaseName string
	var schemaName string
	descProto := &sqlbase.Descriptor{}
	for fieldIdx, field := range r.Fields {
		d := row[r.colIdxByFieldIdx[fieldIdx]]
		if field.Name == "descriptor" {
			err := protoutil.Unmarshal([]byte(*d.Datum.(*tree.DBytes)), descProto)
			if err != nil {
				return desc, databaseName, schemaName, err
			}
			switch t := descProto.Union.(type) {
			case *sqlbase.Descriptor_Table:
				desc = descProto.Table(hlc.Timestamp{})
				if desc.(*sqlbase.TableDescriptor).ID > keys.PostgresSchemaID {
					schema, err := sqlbase.GetSchemaDescFromID(ctx, DB.NewTxn(ctx, "cdc-ddl-findSchemaName"), desc.(*sqlbase.TableDescriptor).ParentID)
					if err != nil {
						return desc, databaseName, schemaName, err
					}
					schemaName = schema.Name
					database, err := sqlbase.GetDatabaseDescFromID(ctx, DB.NewTxn(ctx, "cdc-ddl-findDatabaseName"), schema.ParentID)
					if err != nil {
						return desc, databaseName, schemaName, err
					}
					databaseName = database.Name
				}
			case *sqlbase.Descriptor_Database, *sqlbase.Descriptor_Schema:
				return desc, databaseName, schemaName, nil
			default:
				return desc, databaseName, schemaName, errors.Errorf("Descriptor.Union has unexpected type %T", t)
			}
		}
	}
	return desc, databaseName, schemaName, nil
}
