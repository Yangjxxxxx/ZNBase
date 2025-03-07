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
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/icl/load"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/randutil"
)

func parseTableDesc(createTableStmt string) (*sqlbase.TableDescriptor, error) {
	ctx := context.Background()
	stmt, err := parser.ParseOne(createTableStmt, false)
	if err != nil {
		return nil, errors.Wrapf(err, `parsing %s`, createTableStmt)
	}
	createTable, ok := stmt.AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("expected *tree.CreateTable got %T", stmt)
	}
	st := cluster.MakeTestingClusterSettings()
	const parentID = sqlbase.ID(keys.MaxReservedDescID + 1)
	const tableID = sqlbase.ID(keys.MaxReservedDescID + 2)
	mutDesc, err := load.MakeSimpleTableDescriptor(
		ctx, st, createTable, parentID, tableID, load.NoFKs, hlc.UnixNano(), nil, sqlbase.AdminRole)
	if err != nil {
		return nil, err
	}
	return mutDesc.TableDesc(), mutDesc.TableDesc().ValidateTable(st)
}

func parseValues(tableDesc *sqlbase.TableDescriptor, values string) ([]sqlbase.EncDatumRow, error) {
	semaCtx := &tree.SemaContext{}
	evalCtx := &tree.EvalContext{}

	valuesStmt, err := parser.ParseOne(values, false)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := valuesStmt.AST.(*tree.Select)
	if !ok {
		return nil, errors.Errorf("expected *tree.Select got %T", valuesStmt)
	}
	valuesClause, ok := selectStmt.Select.(*tree.ValuesClause)
	if !ok {
		return nil, errors.Errorf("expected *tree.ValuesClause got %T", selectStmt.Select)
	}

	var rows []sqlbase.EncDatumRow
	for _, rowTuple := range valuesClause.Rows {
		var row sqlbase.EncDatumRow
		for colIdx, expr := range rowTuple {
			col := tableDesc.Columns[colIdx]
			typedExpr, err := sqlbase.SanitizeVarFreeExpr(
				expr, col.Type.ToDatumType(), "avro", semaCtx, false /* allowImpure */, false)
			if err != nil {
				return nil, err
			}
			datum, err := typedExpr.Eval(evalCtx)
			if err != nil {
				return nil, errors.Wrap(err, typedExpr.String())
			}
			row = append(row, sqlbase.DatumToEncDatum(col.Type, datum))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseAvroSchema(j string) (*avroDataRecord, error) {
	var s avroDataRecord
	if err := json.Unmarshal([]byte(j), &s); err != nil {
		return nil, err
	}
	// This avroDataRecord doesn't have any of the derived fields we need for
	// serde. Instead of duplicating the logic, fake out a TableDescriptor, so
	// we can reuse tableToAvroSchema and get them for free.
	tableDesc := &sqlbase.TableDescriptor{
		Name: AvroNameToSQLName(s.Name),
	}
	for _, f := range s.Fields {
		// s.Fields[idx] has `Name` and `SchemaType` set but nothing else.
		// They're needed for serialization/deserialization, so fake out a
		// column descriptor so that we can reuse columnDescToAvroSchema to get
		// all the various fields of avroSchemaField populated for free.
		colDesc, err := avroFieldMetadataToColDesc(f.Metadata)
		if err != nil {
			return nil, err
		}
		tableDesc.Columns = append(tableDesc.Columns, *colDesc)
	}
	return tableToAvroSchema(tableDesc, avroSchemaNoSuffix)
}

func avroFieldMetadataToColDesc(metadata string) (*sqlbase.ColumnDescriptor, error) {
	parsed, err := parser.ParseOne(`ALTER TABLE FOO ADD COLUMN `+metadata, false)
	if err != nil {
		return nil, err
	}
	def := parsed.AST.(*tree.AlterTable).Cmds[0].(*tree.AlterTableAddColumn).ColumnDef
	privs := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Column, sqlbase.AdminRole)
	col, _, _, err := sqlbase.MakeColumnDefDescs(def, &tree.SemaContext{}, privs, 0)
	return col, err
}

func TestAvroSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()

	type test struct {
		name   string
		schema string
		values string
	}
	tests := []test{
		{
			name:   `NULLABLE`,
			schema: `(a INT PRIMARY KEY, b INT NULL)`,
			values: `(1, 2), (3, NULL)`,
		},
		{
			name:   `TUPLE`,
			schema: `(a INT PRIMARY KEY, b STRING)`,
			values: `(1, 'a')`,
		},
		{
			name:   `MULTI_WIDTHS`,
			schema: `(a INT PRIMARY KEY, b DECIMAL (3,2), c DECIMAL (2, 1))`,
			values: `(1, 1.23, 4.5)`,
		},
	}
	// Generate a test for each column type with a random datum of that type.
	for semTypeID, semTypeName := range sqlbase.ColumnType_SemanticType_name {
		typ := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_SemanticType(semTypeID)}
		switch typ.SemanticType {
		case sqlbase.ColumnType_NAME, sqlbase.ColumnType_OID, sqlbase.ColumnType_TUPLE:
			// These aren't expected to be needed for changefeeds.
			continue
		case sqlbase.ColumnType_INTERVAL, sqlbase.ColumnType_ARRAY, sqlbase.ColumnType_BIT,
			sqlbase.ColumnType_COLLATEDSTRING, sqlbase.ColumnType_ENUM, sqlbase.ColumnType_SET:
			// Implement these as customer demand dictates.
			continue
		}
		datum := sqlbase.RandDatum(rng, typ, false /* nullOk */)
		if datum == tree.DNull {
			// DNull is returned by RandDatum for ColumnType_NULL or if the
			// column type is unimplemented in RandDatum. In either case, the
			// correct thing to do is skip this one.
			continue
		}
		switch typ.SemanticType {
		case sqlbase.ColumnType_TIMESTAMP:
			// Truncate to millisecond instead of microsecond because of a bug
			// in the avro lib's deserialization code. The serialization seems
			// to be fine and we only use deserialization for testing, so we
			// should patch the bug but it's not currently affecting changefeed
			// correctness.
			t := datum.(*tree.DTimestamp).Time.Truncate(time.Millisecond)
			datum = tree.MakeDTimestamp(t, time.Microsecond)
		case sqlbase.ColumnType_DECIMAL:
			// TODO(dan): Make RandDatum respect Precision and Width instead.
			// TODO(dan): The precision is really meant to be in [1,10], but it
			// sure looks like there's an off by one error in the avro library
			// that makes this test flake if it picks precision of 1.
			typ.Precision = rng.Int31n(10) + 2
			typ.Width = rng.Int31n(typ.Precision + 1)
			coeff := rng.Int63n(int64(math.Pow10(int(typ.Precision))))
			datum = &tree.DDecimal{Decimal: *apd.New(coeff, -typ.Width)}
		}
		serializedDatum := tree.Serialize(datum)
		// schema is used in a fmt.Sprintf to fill in the table name, so we have
		// to escape any stray %s.
		escapedDatum := strings.Replace(serializedDatum, `%`, `%%`, -1)
		randTypeTest := test{
			name:   semTypeName,
			schema: fmt.Sprintf(`(a INT PRIMARY KEY, b %s)`, typ.SQLString()),
			values: fmt.Sprintf(`(1, %s)`, escapedDatum),
		}
		tests = append(tests, randTypeTest)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.schema))
			require.NoError(t, err)
			origSchema, err := tableToAvroSchema(tableDesc, avroSchemaNoSuffix)
			require.NoError(t, err)
			jsonSchema := origSchema.codec.Schema()
			roundtrippedSchema, err := parseAvroSchema(jsonSchema)
			require.NoError(t, err)
			// It would require some work, but we could also check that the
			// roundtrippedSchema can be used to recreate the original `CREATE
			// TABLE`.

			rows, err := parseValues(tableDesc, `VALUES `+test.values)
			require.NoError(t, err)

			for _, row := range rows {
				evalCtx := &tree.EvalContext{SessionData: &sessiondata.SessionData{}}
				serialized, err := origSchema.textualFromRow(row)
				require.NoError(t, err)
				roundtripped, err := roundtrippedSchema.rowFromTextual(serialized)
				require.NoError(t, err)
				require.Equal(t, 0, row[1].Datum.Compare(evalCtx, roundtripped[1].Datum),
					`%s != %s`, row[1].Datum, roundtripped[1].Datum)

				serialized, err = origSchema.BinaryFromRow(nil, row)
				require.NoError(t, err)
				roundtripped, err = roundtrippedSchema.RowFromBinary(serialized)
				require.NoError(t, err)
				require.Equal(t, 0, row[1].Datum.Compare(evalCtx, roundtripped[1].Datum),
					`%s != %s`, row[1].Datum, roundtripped[1].Datum)
			}
		})
	}

	t.Run("escaping", func(t *testing.T) {
		tableDesc, err := parseTableDesc(`CREATE TABLE "☃" (🍦 INT PRIMARY KEY)`)
		require.NoError(t, err)
		tableSchema, err := tableToAvroSchema(tableDesc, avroSchemaNoSuffix)
		require.NoError(t, err)
		require.Equal(t,
			`{"type":"record","name":"_u2603_","fields":[`+
				`{"type":["null","long"],"name":"_u0001f366_","default":null,`+
				`"__exdb__":"🍦 INT NOT NULL"}]}`,
			tableSchema.codec.Schema())
		indexSchema, err := indexToAvroSchema(tableDesc, &tableDesc.PrimaryIndex)
		require.NoError(t, err)
		require.Equal(t,
			`{"type":"record","name":"_u2603_","fields":[`+
				`{"type":["null","long"],"name":"_u0001f366_","default":null,`+
				`"__exdb__":"🍦 INT NOT NULL"}]}`,
			indexSchema.codec.Schema())
	})

	// This test shows what avro schema each sql column maps to, for easy
	// reference.
	t.Run("type_goldens", func(t *testing.T) {
		goldens := map[string]string{
			`BOOL`:         `["null","boolean"]`,
			`BYTES`:        `["null","bytes"]`,
			`DATE`:         `["null",{"type":"int","logicalType":"date"}]`,
			`FLOAT8`:       `["null","double"]`,
			`INET`:         `["null","string"]`,
			`INT`:          `["null","long"]`,
			`JSONB`:        `["null","string"]`,
			`STRING`:       `["null","string"]`,
			`TIME`:         `["null",{"type":"long","logicalType":"time-micros"}]`,
			`TIMESTAMP`:    `["null",{"type":"long","logicalType":"timestamp-micros"}]`,
			`TIMESTAMPTZ`:  `["null",{"type":"long","logicalType":"timestamp-micros"}]`,
			`UUID`:         `["null","string"]`,
			`DECIMAL(3,2)`: `["null",{"type":"bytes","logicalType":"decimal","precision":3,"scale":2}]`,
		}

		for semTypeID := range sqlbase.ColumnType_SemanticType_name {
			typ := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_SemanticType(semTypeID)}
			switch typ.SemanticType {
			case sqlbase.ColumnType_INTERVAL, sqlbase.ColumnType_NAME, sqlbase.ColumnType_OID,
				sqlbase.ColumnType_ARRAY, sqlbase.ColumnType_ENUM, sqlbase.ColumnType_SET, sqlbase.ColumnType_BIT, sqlbase.ColumnType_TUPLE,
				sqlbase.ColumnType_COLLATEDSTRING, sqlbase.ColumnType_INT2VECTOR,
				sqlbase.ColumnType_OIDVECTOR, sqlbase.ColumnType_NULL:
				continue
			case sqlbase.ColumnType_DECIMAL:
				typ.Precision = 3
				typ.Width = 2
			}

			colType := typ.SQLString()
			tableDesc, err := parseTableDesc(`CREATE TABLE foo (pk INT PRIMARY KEY, a ` + colType + `)`)
			require.NoError(t, err)
			field, err := columnDescToAvroSchema(&tableDesc.Columns[1])
			require.NoError(t, err)
			schema, err := json.Marshal(field.SchemaType)
			require.NoError(t, err)
			require.Equal(t, goldens[colType], string(schema), `SQL type %s`, colType)

			// Delete from goldens for the following assertion that we don't have any
			// unexpectedly unused goldens.
			delete(goldens, colType)
		}
		if len(goldens) > 0 {
			t.Fatalf("expected all goldens to be consumed: %v", goldens)
		}
	})

	// This test shows what avro value some sql datums map to, for easy reference.
	// The avro golden strings are in the textual format defined in the spec.
	t.Run("value_goldens", func(t *testing.T) {
		goldens := []struct {
			sqlType string
			sql     string
			avro    string
		}{
			{sqlType: `INT`, sql: `NULL`, avro: `null`},
			{sqlType: `INT`,
				sql:  `1`,
				avro: `{"long":1}`},

			{sqlType: `BOOL`, sql: `NULL`, avro: `null`},
			{sqlType: `BOOL`,
				sql:  `true`,
				avro: `{"boolean":true}`},

			{sqlType: `FLOAT`, sql: `NULL`, avro: `null`},
			{sqlType: `FLOAT`,
				sql:  `1.2`,
				avro: `{"double":1.2}`},

			{sqlType: `STRING`, sql: `NULL`, avro: `null`},
			{sqlType: `STRING`,
				sql:  `'foo'`,
				avro: `{"string":"foo"}`},

			{sqlType: `BYTES`, sql: `NULL`, avro: `null`},
			{sqlType: `BYTES`,
				sql:  `'foo'`,
				avro: `{"bytes":"foo"}`},

			{sqlType: `DATE`, sql: `NULL`, avro: `null`},
			{sqlType: `DATE`,
				sql:  `'2019-01-02'`,
				avro: `{"int.date":17898}`},

			{sqlType: `TIME`, sql: `NULL`, avro: `null`},
			{sqlType: `TIME`,
				sql:  `'03:04:05'`,
				avro: `{"long.time-micros":11045000000}`},

			{sqlType: `TIMESTAMP`, sql: `NULL`, avro: `null`},
			{sqlType: `TIMESTAMP`,
				sql:  `'2019-01-02 03:04:05'`,
				avro: `{"long.timestamp-micros":1546398245000000}`},

			{sqlType: `TIMESTAMPTZ`, sql: `NULL`, avro: `null`},
			{sqlType: `TIMESTAMPTZ`,
				sql:  `'2019-01-02 03:04:05'`,
				avro: `{"long.timestamp-micros":1546398245000000}`},

			{sqlType: `DECIMAL(4,1)`, sql: `NULL`, avro: `null`},
			{sqlType: `DECIMAL(4,1)`,
				sql:  `1.2`,
				avro: `{"bytes.decimal":"\f"}`},

			{sqlType: `UUID`, sql: `NULL`, avro: `null`},
			{sqlType: `UUID`,
				sql:  `'27f4f4c9-e35a-45dd-9b79-5ff0f9b5fbb0'`,
				avro: `{"string":"27f4f4c9-e35a-45dd-9b79-5ff0f9b5fbb0"}`},

			{sqlType: `INET`, sql: `NULL`, avro: `null`},
			{sqlType: `INET`,
				sql:  `'190.0.0.0'`,
				avro: `{"string":"190.0.0.0"}`},
			{sqlType: `INET`,
				sql:  `'190.0.0.0/24'`,
				avro: `{"string":"190.0.0.0\/24"}`},
			{sqlType: `INET`,
				sql:  `'2001:4f8:3:ba:2e0:81ff:fe22:d1f1'`,
				avro: `{"string":"2001:4f8:3:ba:2e0:81ff:fe22:d1f1"}`},
			{sqlType: `INET`,
				sql:  `'2001:4f8:3:ba:2e0:81ff:fe22:d1f1/120'`,
				avro: `{"string":"2001:4f8:3:ba:2e0:81ff:fe22:d1f1\/120"}`},
			{sqlType: `INET`,
				sql:  `'::ffff:192.168.0.1/24'`,
				avro: `{"string":"::ffff:192.168.0.1\/24"}`},

			{sqlType: `JSONB`, sql: `NULL`, avro: `null`},
			{sqlType: `JSONB`,
				sql:  `'null'`,
				avro: `{"string":"null"}`},
			{sqlType: `JSONB`,
				sql:  `'{"b": 1}'`,
				avro: `{"string":"{\"b\": 1}"}`},
		}

		for _, test := range goldens {
			tableDesc, err := parseTableDesc(
				`CREATE TABLE foo (pk INT PRIMARY KEY, a ` + test.sqlType + `)`)
			require.NoError(t, err)
			rows, err := parseValues(tableDesc, `VALUES (1, `+test.sql+`)`)
			require.NoError(t, err)

			schema, err := tableToAvroSchema(tableDesc, avroSchemaNoSuffix)
			require.NoError(t, err)
			textual, err := schema.textualFromRow(rows[0])
			require.NoError(t, err)
			// Trim the outermost {}.
			value := string(textual[1 : len(textual)-1])
			// Strip out the pk field.
			value = strings.Replace(value, `"pk":{"long":1}`, ``, -1)
			// Trim the `,`, which could be on either side because of the avro library
			// doesn't deterministically order the fields.
			value = strings.Trim(value, `,`)
			// Strip out the field name.
			value = strings.Replace(value, `"a":`, ``, -1)
			require.Equal(t, test.avro, value)
		}
	})
}

func (f *avroSchemaField) defaultValueNative() (interface{}, bool) {
	schemaType := f.SchemaType
	if union, ok := schemaType.([]avroSchemaType); ok {
		// "Default values for union fields correspond to the first schema in
		// the union."
		schemaType = union[0]
	}
	switch schemaType {
	case avroSchemaNull:
		return nil, true
	}
	panic(errors.Errorf(`unimplemented %T: %v`, schemaType, schemaType))
}

// rowFromBinaryEvolved decodes `buf` using writerSchema but evolves/resolves it
// to readerSchema using the rules from the avro spec:
// https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution
//
// It'd be nice if our avro library handled this for us, but neither of the
// popular golang once seem to have it implemented.
func rowFromBinaryEvolved(
	buf []byte, writerSchema, readerSchema *avroDataRecord,
) (sqlbase.EncDatumRow, error) {
	native, newBuf, err := writerSchema.codec.NativeFromBinary(buf)
	if err != nil {
		return nil, err
	}
	if len(newBuf) > 0 {
		return nil, errors.New(`only one row was expected`)
	}
	nativeMap, ok := native.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf(`unknown avro native type: %T`, native)
	}
	adjustNative(nativeMap, writerSchema, readerSchema)
	return readerSchema.rowFromNative(nativeMap)
}

func adjustNative(native map[string]interface{}, writerSchema, readerSchema *avroDataRecord) {
	for _, writerField := range writerSchema.Fields {
		if _, inReader := readerSchema.fieldIdxByName[writerField.Name]; !inReader {
			// "If the writer's record contains a field with a name not present
			// in the reader's record, the writer's value for that field is
			// ignored."
			delete(native, writerField.Name)
		}
	}
	for _, readerField := range readerSchema.Fields {
		if _, inWriter := writerSchema.fieldIdxByName[readerField.Name]; !inWriter {
			// "If the reader's record schema has a field that contains a
			// default value, and writer's schema does not have a field with the
			// same name, then the reader should use the default value from its
			// field."
			if readerFieldDefault, ok := readerField.defaultValueNative(); ok {
				native[readerField.Name] = readerFieldDefault
			}
		}
	}
}

func TestAvroMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type test struct {
		name           string
		writerSchema   string
		writerValues   string
		readerSchema   string
		expectedValues string
	}
	tests := []test{
		{
			name:           `add_nullable`,
			writerSchema:   `(a INT PRIMARY KEY)`,
			writerValues:   `(1)`,
			readerSchema:   `(a INT PRIMARY KEY, b INT)`,
			expectedValues: `(1, NULL)`,
		},
		// TODO(dan): add a column with a default value
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writerDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.writerSchema))
			require.NoError(t, err)
			writerSchema, err := tableToAvroSchema(writerDesc, avroSchemaNoSuffix)
			require.NoError(t, err)
			readerDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.readerSchema))
			require.NoError(t, err)
			readerSchema, err := tableToAvroSchema(readerDesc, avroSchemaNoSuffix)
			require.NoError(t, err)

			writerRows, err := parseValues(writerDesc, `VALUES `+test.writerValues)
			require.NoError(t, err)
			expectedRows, err := parseValues(readerDesc, `VALUES `+test.expectedValues)
			require.NoError(t, err)

			for i := range writerRows {
				writerRow, expectedRow := writerRows[i], expectedRows[i]
				encoded, err := writerSchema.BinaryFromRow(nil, writerRow)
				require.NoError(t, err)
				row, err := rowFromBinaryEvolved(encoded, writerSchema, readerSchema)
				require.NoError(t, err)
				require.Equal(t, expectedRow, row)
			}
		})
	}
}

func TestDecimalRatRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run(`table`, func(t *testing.T) {
		tests := []struct {
			scale int32
			dec   *apd.Decimal
		}{
			{0, apd.New(0, 0)},
			{0, apd.New(1, 0)},
			{0, apd.New(-1, 0)},
			{0, apd.New(123, 0)},
			{1, apd.New(0, -1)},
			{1, apd.New(1, -1)},
			{1, apd.New(123, -1)},
			{5, apd.New(1, -5)},
		}
		for d, test := range tests {
			rat, err := decimalToRat(*test.dec, test.scale)
			require.NoError(t, err)
			roundtrip := ratToDecimal(rat, test.scale)
			if test.dec.CmpTotal(&roundtrip) != 0 {
				t.Errorf(`%d: %s != %s`, d, test.dec, &roundtrip)
			}
		}
	})
	t.Run(`error`, func(t *testing.T) {
		_, err := decimalToRat(*apd.New(1, -2), 1)
		require.EqualError(t, err, "0.01 will not roundtrip at scale 1")
		_, err = decimalToRat(*apd.New(1, -1), 2)
		require.EqualError(t, err, "0.1 will not roundtrip at scale 2")
		_, err = decimalToRat(apd.Decimal{Form: apd.Infinite}, 0)
		require.EqualError(t, err, "cannot convert Infinite form decimal")
	})
	t.Run(`rand`, func(t *testing.T) {
		rng, _ := randutil.NewPseudoRand()
		precision := rng.Int31n(10) + 1
		scale := rng.Int31n(precision + 1)
		coeff := rng.Int63n(int64(math.Pow10(int(precision))))
		dec := apd.New(coeff, -scale)
		rat, err := decimalToRat(*dec, scale)
		require.NoError(t, err)
		roundtrip := ratToDecimal(rat, scale)
		if dec.CmpTotal(&roundtrip) != 0 {
			t.Errorf(`%s != %s`, dec, &roundtrip)
		}
	})
}
