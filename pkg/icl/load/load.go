// Copyright 2016  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package load

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"math/rand"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/blobs"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/icl/dump"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/transform"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// LoadHeader is the header for Load stmt results.
var LoadHeader = sqlbase.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "index_entries", Typ: types.Int},
	{Name: "system_records", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
	{Name: "rejected_rows", Typ: types.Int},
	{Name: "rejected_address", Typ: types.String},
}

// Load converts r into SSTables and backup descriptors. database is the name
// of the database into which the SSTables will eventually be written. uri
// is the storage location. ts is the time at which the MVCC data will
// be set. loadChunkBytes is the size at which to create a new SSTable
// (which will translate into a new range during restore); set to 0 to use
// the zone's default range max / 2.
func Load(
	ctx context.Context,
	db *gosql.DB,
	r io.Reader,
	database, uri string,
	ts hlc.Timestamp,
	loadChunkBytes int64,
	tempPrefix string,
	writeToDir string,
) (dump.DumpDescriptor, error) {
	if loadChunkBytes == 0 {
		loadChunkBytes = *config.DefaultZoneConfig().RangeMaxBytes / 2
	}

	var txCtx transform.ExprTransformContext
	curTime := timeutil.Unix(0, ts.WallTime)
	evalCtx := tree.EvalContext{}
	evalCtx.SetTxnTimestamp(curTime)
	evalCtx.SetStmtTimestamp(curTime)

	blobClientFactory := blobs.TestBlobServiceClient(writeToDir)
	conf, err := dumpsink.ConfFromURI(ctx, uri)
	if err != nil {
		return dump.DumpDescriptor{}, err
	}
	dir, err := dumpsink.MakeDumpSink(ctx, conf, cluster.NoSettings, blobClientFactory)
	if err != nil {
		return dump.DumpDescriptor{}, errors.Wrap(err, "export storage from URI")
	}
	defer dir.Close()

	var dbDescBytes []byte
	if err := db.QueryRow(`
		SELECT
			d.descriptor
		FROM system.namespace n INNER JOIN system.descriptor d ON n.id = d.id
		WHERE n."parentID" = $1
		AND n.name = $2`,
		keys.RootNamespaceID,
		database,
	).Scan(&dbDescBytes); err != nil {
		return dump.DumpDescriptor{}, errors.Wrap(err, "fetch database descriptor")
	}
	var dbDescWrapper sqlbase.Descriptor
	if err := protoutil.Unmarshal(dbDescBytes, &dbDescWrapper); err != nil {
		return dump.DumpDescriptor{}, errors.Wrap(err, "unmarshal database descriptor")
	}
	dbDesc := dbDescWrapper.GetDatabase()

	if err := db.QueryRow(`
		SELECT
			d.descriptor
		FROM system.namespace n INNER JOIN system.descriptor d ON n.id = d.id
		WHERE n."parentID" = $1
		AND n.name = 'public'`,
		dbDesc.ID,
	).Scan(&dbDescBytes); err != nil {
		return dump.DumpDescriptor{}, errors.Wrap(err, "fetch database descriptor")
	}
	var scDescWrapper sqlbase.Descriptor
	if err := protoutil.Unmarshal(dbDescBytes, &scDescWrapper); err != nil {
		return dump.DumpDescriptor{}, errors.Wrap(err, "unmarshal database descriptor")
	}
	scDesc := scDescWrapper.GetSchema()

	// privs := dbDesc.GetPrivileges()
	privs := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, dbDesc.Privileges.Owner)

	tableDescs := make(map[string]*sqlbase.ImmutableTableDescriptor)

	var currentCmd bytes.Buffer
	scanner := bufio.NewReader(r)
	var ri row.Inserter
	var defaultExprs []tree.TypedExpr
	var cols []sqlbase.ColumnDescriptor
	var tableDesc *sqlbase.ImmutableTableDescriptor
	var tableName string
	var prevKey roachpb.Key
	var kvs []engine.MVCCKeyValue
	var kvBytes int64
	backup := dump.DumpDescriptor{
		Descriptors: []sqlbase.Descriptor{
			{Union: &sqlbase.Descriptor_Database{Database: dbDesc}},
			{Union: &sqlbase.Descriptor_Schema{Schema: scDesc}},
		},
	}
	for {
		line, err := scanner.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return dump.DumpDescriptor{}, errors.Wrap(err, "read line")
		}
		currentCmd.WriteString(line)
		if !parser.EndsInSemicolon(currentCmd.String()) {
			currentCmd.WriteByte('\n')
			continue
		}
		cmd := currentCmd.String()
		currentCmd.Reset()
		stmt, err := parser.ParseOne(cmd, false)
		if err != nil {
			return dump.DumpDescriptor{}, errors.Wrapf(err, "parsing: %q", cmd)
		}
		switch s := stmt.AST.(type) {
		case *tree.CreateTable:
			if tableDesc != nil {
				if err := writeSST(ctx, &backup, dir, tempPrefix, kvs, ts); err != nil {
					return dump.DumpDescriptor{}, errors.Wrap(err, "writeSST")
				}
				kvs = kvs[:0]
				kvBytes = 0
			}

			// TODO(mjibson): error for now on FKs and CHECK constraints
			// TODO(mjibson): differentiate between qualified (with database) and unqualified (without database) table names

			tableName = s.Table.String()
			tableDesc = tableDescs[tableName]
			if tableDesc != nil {
				return dump.DumpDescriptor{}, errors.Errorf("duplicate CREATE TABLE for %s", tableName)
			}

			// Using test cluster settings means that we'll generate a backup using
			// the latest cluster version available in this binary. This will be safe
			// once we verify the cluster version during restore.
			//
			// TODO(benesch): ensure backups from too-old or too-new nodes are
			// rejected during restore.
			st := cluster.MakeTestingClusterSettings()

			affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
			// A nil txn is safe because it is only used by sql.MakeTableDesc, which
			// only uses txn for resolving FKs and interleaved tables, neither of which
			// are present here. Ditto for the schema accessor.
			var txn *client.Txn
			// At this point the CREATE statements in the loaded SQL do not
			// use the SERIAL type so we need not process SERIAL types here.gs

			desc, err := sql.MakeTableDesc(ctx, txn, nil /* vt */, st, s, scDesc.ID,
				0 /* table ID */, ts, privs, affected, nil, &evalCtx, nil, nil)
			if err != nil {
				return dump.DumpDescriptor{}, errors.Wrap(err, "make table desc")
			}

			tableDesc = sqlbase.NewImmutableTableDescriptor(*desc.TableDesc())
			tableDescs[tableName] = tableDesc
			backup.Descriptors = append(backup.Descriptors, sqlbase.Descriptor{
				Union: &sqlbase.Descriptor_Table{Table: desc.TableDesc()},
			})

			for _, col := range tableDesc.Columns {
				if col.IsComputed() {
					return dump.DumpDescriptor{}, errors.Errorf("computed columns are not allowed")
				}
			}

			ri, err = row.MakeInserter(nil, nil, nil, tableDesc, nil, tableDesc.Columns,
				true, &sqlbase.DatumAlloc{})
			if err != nil {
				return dump.DumpDescriptor{}, errors.Wrap(err, "make row inserter")
			}
			cols, defaultExprs, err =
				sqlbase.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &txCtx, &evalCtx)
			if err != nil {
				return dump.DumpDescriptor{}, errors.Wrap(err, "process default columns")
			}

		case *tree.Insert:
			name := tree.AsString(s.Table)
			if tableDesc == nil {
				return dump.DumpDescriptor{}, errors.Errorf("expected previous CREATE TABLE %s statement", name)
			}
			if name != tableName {
				return dump.DumpDescriptor{}, errors.Errorf("unexpected INSERT for table %s after CREATE TABLE %s", name, tableName)
			}
			outOfOrder := false
			err := insertStmtToKVs(ctx, tableDesc, defaultExprs, cols, evalCtx, ri, s, func(kv roachpb.KeyValue) {
				if outOfOrder || prevKey.Compare(kv.Key) >= 0 {
					outOfOrder = true
					return
				}
				prevKey = kv.Key
				kvBytes += int64(len(kv.Key) + len(kv.Value.RawBytes))
				kvs = append(kvs, engine.MVCCKeyValue{
					Key:   engine.MVCCKey{Key: kv.Key, Timestamp: kv.Value.Timestamp},
					Value: kv.Value.RawBytes,
				})
			})
			if err != nil {
				return dump.DumpDescriptor{}, errors.Wrapf(err, "insertStmtToKVs")
			}
			if outOfOrder {
				return dump.DumpDescriptor{}, errors.Errorf("out of order row: %s", cmd)
			}

			if kvBytes > loadChunkBytes {
				if err := writeSST(ctx, &backup, dir, tempPrefix, kvs, ts); err != nil {
					return dump.DumpDescriptor{}, errors.Wrap(err, "writeSST")
				}
				kvs = kvs[:0]
				kvBytes = 0
			}

		default:
			return dump.DumpDescriptor{}, errors.Errorf("unsupported load statement: %q", stmt.SQL)
		}
	}

	if tableDesc != nil {
		if err := writeSST(ctx, &backup, dir, tempPrefix, kvs, ts); err != nil {
			return dump.DumpDescriptor{}, errors.Wrap(err, "writeSST")
		}
	}

	descBuf, err := protoutil.Marshal(&backup)
	if err != nil {
		return dump.DumpDescriptor{}, errors.Wrap(err, "marshal backup descriptor")
	}
	if err := dir.WriteFile(ctx, dump.DumpDescriptorName, bytes.NewReader(descBuf)); err != nil {
		return dump.DumpDescriptor{}, errors.Wrap(err, "uploading backup descriptor")
	}

	return backup, nil
}

func insertStmtToKVs(
	ctx context.Context,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	defaultExprs []tree.TypedExpr,
	cols []sqlbase.ColumnDescriptor,
	evalCtx tree.EvalContext,
	ri row.Inserter,
	stmt *tree.Insert,
	f func(roachpb.KeyValue),
) error {
	if stmt.OnConflict != nil {
		return errors.Errorf("load insert: ON CONFLICT not supported: %q", stmt)
	}
	if tree.HasReturningClause(stmt.Returning) {
		return errors.Errorf("load insert: RETURNING not supported: %q", stmt)
	}
	if len(stmt.Columns) > 0 {
		if len(stmt.Columns) != len(cols) {
			return errors.Errorf("load insert: wrong number of columns: %q", stmt)
		}
		for i, col := range tableDesc.Columns {
			if stmt.Columns[i].String() != col.Name {
				return errors.Errorf("load insert: unexpected column order: %q", stmt)
			}
		}
	}
	if stmt.Rows.Limit != nil {
		return errors.Errorf("load insert: LIMIT not supported: %q", stmt)
	}
	if stmt.Rows.OrderBy != nil {
		return errors.Errorf("load insert: ORDER BY not supported: %q", stmt)
	}
	values, ok := stmt.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return errors.Errorf("load insert: expected VALUES clause: %q", stmt)
	}

	b := inserter(f)
	computedIVarContainer := sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    tableDesc.Columns,
	}
	for _, tuple := range values.Rows {
		insertRow := make([]tree.Datum, len(tuple))
		for i, expr := range tuple {
			if expr == tree.DNull {
				insertRow[i] = tree.DNull
				continue
			}
			c, ok := expr.(tree.Constant)
			if !ok {
				return errors.Errorf("unsupported expr: %q", expr)
			}
			var err error
			insertRow[i], err = c.ResolveAsType(nil, tableDesc.Columns[i].Type.ToDatumType(), false)
			if err != nil {
				return err
			}
		}

		// We have disallowed computed exprs.
		var computeExprs []tree.TypedExpr
		var computedCols []sqlbase.ColumnDescriptor

		insertRow, err := sql.GenerateInsertRow(
			defaultExprs, computeExprs, cols, computedCols, evalCtx, tableDesc, insertRow, &computedIVarContainer,
		)
		if err != nil {
			return errors.Wrapf(err, "process insert %q", insertRow)
		}
		// TODO(bram): Is the checking of FKs here required? If not, turning them
		// off may provide a speed boost.
		var pm schemaexpr.PartialIndexUpdateHelper
		if err := ri.InsertRow(ctx, b, insertRow, true, row.CheckFKs, false /* traceKV */, pm); err != nil {
			return errors.Wrapf(err, "insert %q", insertRow)
		}
	}
	return nil
}

type inserter func(roachpb.KeyValue)

func (i inserter) CPut(key, value, expValue interface{}) {
	panic("unimplemented")
}

func (i inserter) Del(key ...interface{}) {
	panic("unimplemented")
}

func (i inserter) Put(key, value interface{}) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

func (i inserter) InitPut(key, value interface{}, failOnTombstones bool) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

func writeSST(
	ctx context.Context,
	backup *dump.DumpDescriptor,
	base dumpsink.DumpSink,
	tempPrefix string,
	kvs []engine.MVCCKeyValue,
	ts hlc.Timestamp,
) error {
	if len(kvs) == 0 {
		return nil
	}

	filename := fmt.Sprintf("load-%d.sst", rand.Int63())
	log.Info(ctx, "writesst ", filename)

	sstFile := &engine.MemFile{}
	sst := engine.MakeBackupSSTWriter(sstFile)
	defer sst.Close()

	for _, kv := range kvs {
		kv.Key.Timestamp = ts
		if err := sst.Put(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	err := sst.Finish()
	if err != nil {
		return err
	}

	if err := base.WriteFile(ctx, filename, bytes.NewReader(sstFile.Data())); err != nil {
		return err
	}

	backup.Files = append(backup.Files, dump.DumpDescriptor_File{
		Span: roachpb.Span{
			Key: kvs[0].Key.Key,
			// The EndKey is exclusive, so use PrefixEnd to get the first key
			// greater than the last key in the sst.
			EndKey: kvs[len(kvs)-1].Key.Key.PrefixEnd(),
		},
		Path: filename,
	})
	backup.EntryCounts.DataSize += sst.DataSize
	return nil
}
