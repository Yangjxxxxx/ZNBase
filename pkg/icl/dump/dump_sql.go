package dump

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/encoding/csv"
)

const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".sql"

// WriteSQL realize the export of databases, schemas and tables as SQL files
func WriteSQL(
	ctx context.Context,
	sp *csvWriter,
	resultsCh chan<- tree.Datums,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	evalCtx *tree.EvalContext,
	dumpStmt *annotatedBackupStatement,
	skipDDL bool,
	onlyDDL bool,
	header string,
) error {
	pattern := exportFilePatternDefault
	if sp.spec.NamePattern != "" {
		pattern = sp.spec.NamePattern
	}

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	var dbName, queryType, ts string
	var tableNames []*tree.TableName
	var mds []BasicMetadata
	var err error
	//var err error
	switch sp.spec.Type {
	case roachpb.DUMPFileFormat_DATABASE:
		dbName = sp.spec.ExportName
		err = HasDatabase(ctx, ex, txn, evalCtx, "", dbName)
		if err != nil {
			return err
		}
		schemaNames := make(map[string]string)
		mds, ts, err = getDumpMetadata(ctx, ex, txn, evalCtx, dbName, tableNames, "", false)
		if err != nil {
			return err
		}
		err = writer.WriteString(fmt.Sprintf("CREATE DATABASE %s;\n", dbName))
		if err != nil {
			return err
		}
		names, err := getTableNames(ctx, ex, txn, dbName, "")
		if err != nil {
			return err
		}
		schemaNames = GetSchemas(names)
		for schemaName := range schemaNames {
			schemaName = strings.Join([]string{dbName, schemaName}, ".")
			err := writer.WriteString(fmt.Sprintf("CREATE SCHEMA %s;\n", schemaName))
			if err != nil {
				return err
			}
		}
	case roachpb.DUMPFileFormat_SCHEMA:
		err = HasSchema(ctx, ex, txn, evalCtx, "", dumpStmt.ExpendQuery.ScName.Catalog(), dumpStmt.ExpendQuery.ScName.Schema())
		if err != nil {
			return err
		}
		mds, ts, err = GetDumpSchemaData(ctx, ex, txn, evalCtx, dumpStmt.ExpendQuery.ScName.Schema(), dumpStmt.ExpendQuery.ScName.Catalog(), tableNames, "")
		if err != nil {
			return err
		}
		err = writer.WriteString(fmt.Sprintf("CREATE SCHEMA %s;\n", dumpStmt.ExpendQuery.ScName.Schema()))
		if err != nil {
			return err
		}

	case roachpb.DUMPFileFormat_TABLE:
		dbName = sp.flowCtx.EvalCtx.SessionData.Database
		tableNames, _, err = parseTableDesc(dumpStmt.ExpendQuery.Query.Select, dbName)
		if err != nil {
			return err
		}
		if len(tableNames) < 1 {
			return errors.Errorf("cannot parse tableName")
		}
		dbName = string(tableNames[0].CatalogName)
		mds, ts, err = getDumpMetadata(ctx, ex, txn, evalCtx, dbName, tableNames, "", dumpStmt.IsSettings)
		if err != nil {
			return err
		}
		namesTmp := strings.Split(sp.spec.ExportName, " ")
		if len(namesTmp) < 1 {
			return errors.Errorf("Not expected length %d", len(namesTmp))
		}
		queryType = namesTmp[0]
		if queryType != "TABLE" {
			return errors.Errorf("unsupport type %s", queryType)
		}
	}
	for i, md := range mds {
		if i > 0 {
			fmt.Fprintln(writer.GetBufio())
		}
		if sp.spec.Type == roachpb.DUMPFileFormat_DATABASE {
			if err := dumpCreateTable(writer.GetBufio(), md); err != nil {
				return err
			}
		}
		if sp.spec.Type == roachpb.DUMPFileFormat_SCHEMA {
			if err := schemaCreateTable(writer.GetBufio(), md); err != nil {
				return err
			}
		}
		if sp.spec.Type == roachpb.DUMPFileFormat_TABLE {
			if err := dumpCreateTable(writer.GetBufio(), md); err != nil {
				return err
			}
		}
	}
	writer.Flush()
	conf, err := dumpsink.ConfFromURI(ctx, sp.spec.Destination)
	if err != nil {
		return err
	}
	es, err := sp.flowCtx.Cfg.DumpSink(ctx, conf)
	if err != nil {
		return err
	}
	defer es.Close()

	//导出对应的元数据信息（DDL）
	if !skipDDL && !dumpStmt.IsSettings {
		part := fmt.Sprintf("create_stmt")
		filename := strings.Replace(pattern, exportFilePatternPart, part, -1)
		bufBytes := buf.Bytes()
		if sp.spec.Encryption != nil {
			bufBytes, err = storageicl.EncryptFile(bufBytes, sp.spec.Encryption.Key)
			if err != nil {
				return err
			}
		}
		size := len(bufBytes)

		if es.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
			err := es.SetConfig(header)
			if err != nil {
				return err
			}
		}

		if err := es.WriteFile(ctx, filename, bytes.NewReader(bufBytes)); err != nil {
			return err
		}
		resultsCh <- tree.Datums{
			tree.NewDString(filename),
			tree.NewDInt(tree.DInt(size)),
		}
	}
	group := ctxgroup.WithContext(ctx)
	if onlyDDL == false {
		for _, md := range mds {
			tempMd := md
			group.GoCtx(func(ctx context.Context) error {
				err := TableData(ctx, ex, txn, evalCtx, sp.flowCtx, &sp.spec, ts, tempMd, resultsCh, dumpStmt.IsSettings)
				if err != nil {
					return err
				}
				return nil
			})
		}
	}
	return group.Wait()
}

// parse to get the queryType,database name and TableName
func parseTableDesc(
	sel tree.SelectStatement, database string,
) (names []*tree.TableName, needCheckPrivilege bool, err error) {
	var tblName *tree.TableName
	var tblSelect *tree.SelectClause
	var tblExpr *tree.AliasedTableExpr
	switch n := sel.(type) {
	case *tree.SelectClause:
		tblSelect = n
	default:
		return names, needCheckPrivilege, nil
	}
	switch n := tblSelect.From.Tables[0].(type) {
	case *tree.AliasedTableExpr:
		tblExpr = n
	case *tree.TableName:
		tblName = n
		if tblName.CatalogName == "" {
			tblName.CatalogName = tree.Name(database)
		}
		if tblName.SchemaName == "" {
			tblName.SchemaName = "public"
		}
		names = append(names, tblName)
		return names, needCheckPrivilege, nil
	default:
		return names, needCheckPrivilege, nil
	}
	switch n := tblExpr.Expr.(type) {
	case *tree.TableName:
		tblName = n
		needCheckPrivilege = true
		if tblName.CatalogName == "" {
			tblName.CatalogName = tree.Name(database)
		}
		if tblName.SchemaName == "" {
			tblName.SchemaName = "public"
		}
		names = append(names, tblName)
	default:
	}
	return names, needCheckPrivilege, nil
}

// Write table creation statement
func dumpCreateTable(w io.Writer, md BasicMetadata) error {
	table := md.Name
	a := strings.Split(md.CreateStmt, " ")
	if len(a) > 2 {
		a[2] = strings.Join([]string{table.CatalogName.String(), table.SchemaName.String(), table.TableName.String()}, ".")
	}
	md.CreateStmt = strings.Join(a, " ")
	if _, err := w.Write([]byte(md.CreateStmt)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(";\n")); err != nil {
		return err
	}
	return nil
}
