package dump

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/encoding/csv"
	"github.com/znbasedb/znbase/pkg/util/gziputil"
)

const (
	// insertRows is the number of rows per INSERT statement.
	insertRows = 100000
)

// BasicMetadata 元数据信息
type BasicMetadata struct {
	ID         int64
	Name       *tree.TableName
	CreateStmt string
	dependsOn  []int64
	kind       string // "string", "table", or "view"
	alter      []string
	validate   []string
}

// tableMetadata describes one table to dump.
type tableMetadata struct {
	BasicMetadata

	columnNames   string
	columnTypes   map[string]coltypes.T
	colNamesSlice []string
}

// getDumpMetadata retrieves the table information for the specified table(s).
// It also retrieves the cluster timestamp at which the metadata was
// retrieved.
func getDumpMetadata(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	evalCtx *tree.EvalContext,
	dbName string,
	tableNames []*tree.TableName,
	asOf string,
	isSettings bool,
) (mds []BasicMetadata, clusterTS string, err error) {
	if asOf == "" {
		vals, err := ex.QueryRow(ctx, "DUMP", txn, "SELECT cluster_logical_timestamp()")
		if err != nil {
			return nil, "", err
		}
		clusterTS = vals[0].String()
	} else {
		// Validate the timestamp. This prevents SQL injection.
		if _, err := tree.ParseDTimestamp(nil, asOf, time.Nanosecond); err != nil {
			return nil, "", err
		}
		clusterTS = asOf
	}

	if tableNames == nil {
		tableNames, err = getTableNames(ctx, ex, txn, dbName, clusterTS)
		if err != nil {
			return nil, "", err
		}
	}

	mds = make([]BasicMetadata, len(tableNames))
	for i, tableName := range tableNames {
		basicMD, err := getBasicMetadata(ctx, ex, txn, evalCtx, dbName, tableName, clusterTS, isSettings)
		if err != nil {
			return nil, "", err
		}
		mds[i] = basicMD
	}

	return mds, clusterTS, nil
}

// getTableNames retrieves all tables names in the given database.
func getTableNames(
	ctx context.Context, ex *sql.InternalExecutor, txn *client.Txn, dbName string, ts string,
) (names []*tree.TableName, err error) {
	if ts == "" {
		vals, err := ex.QueryRow(ctx, "DUMP", txn, "SELECT cluster_logical_timestamp()")
		if err != nil {
			return nil, err
		}
		ts = vals[0].String()
	}
	rows, err := ex.Query(ctx, "DUMP", txn, fmt.Sprintf(`
		SELECT schema_name,descriptor_name
		FROM "".zbdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1 AND descriptor_type='table'
		`, lex.EscapeSQLString(ts)), dbName)
	if err != nil {
		return nil, err
	}
	for _, vals := range rows {
		schema := vals[0]
		schemaName, ok := schema.(*tree.DString)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", schemaName)
		}
		nameI := vals[1]
		name, ok := nameI.(*tree.DString)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", nameI)
		}
		tableName := MakeTableName(string(*name), string(*schemaName), dbName)
		names = append(names, tableName)
	}
	return names, nil
}

//MakeTableName 根据db schema table name 生成对应的tableName
func MakeTableName(name string, schemaName string, dbName string) *tree.TableName {
	tableName := tree.TableName{}
	tableName.TableName = tree.Name(name)
	tableName.SchemaName = tree.Name(schemaName)
	tableName.CatalogName = tree.Name(dbName)
	// ExplicitCatalog is true if the catalog was explicitly specified
	// or it needs to be rendered during pretty-printing.
	tableName.ExplicitCatalog = true
	tableName.ExplicitSchema = true
	return &tableName
}
func getBasicMetadata(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	evalCtx *tree.EvalContext,
	dbName string,
	tableName *tree.TableName,
	ts string,
	isSettings bool,
) (BasicMetadata, error) {
	//name := tree.NewTableName(tree.Name(dbName), tree.Name(tableName))
	name := tableName
	// Fetch table ID.
	if tableName.SchemaName.String() == "" {
		tableName.SchemaName = "public"
	}
	dbNameEscaped := tree.NameString(dbName)
	var vals tree.Datums
	var err error
	//导出系统参数时，查询表不在任何数据库下，所以需要特殊处理
	if isSettings {
		vals, err = ex.QueryRow(ctx, "DUMP", txn, fmt.Sprintf(`
		SELECT
			descriptor_id,
			create_statement,
			descriptor_type,
			alter_statements,
			validate_statements
		FROM %s.zbdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE descriptor_name = $1
            AND schema_name = $2
	`, dbNameEscaped, lex.EscapeSQLString(ts)), string(tableName.TableName), string(tableName.SchemaName))
	} else {
		vals, err = ex.QueryRow(ctx, "DUMP", txn, fmt.Sprintf(`
		SELECT
			descriptor_id,
			create_statement,
			descriptor_type,
			alter_statements,
			validate_statements
		FROM %s.zbdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1
			AND descriptor_name = $2
            AND schema_name = $3
	`, dbNameEscaped, lex.EscapeSQLString(ts)), dbName, string(tableName.TableName), string(tableName.SchemaName))
	}
	if err != nil {
		if err == io.EOF {
			return BasicMetadata{}, errors.Wrap(
				errors.Errorf("relation %s does not exist", tree.ErrString(name)),
				"getBasicMetadata",
			)
		}
		return BasicMetadata{}, errors.Wrap(err, "getBasicMetadata")
	}
	if len(vals) == 5 {
		idI := vals[0]
		idD, ok := idI.(*tree.DInt)
		if !ok {
			return BasicMetadata{}, fmt.Errorf("unexpected value: %T", idI)
		}
		id := int64(*idD)
		if err != nil {
			return BasicMetadata{}, fmt.Errorf("unexpected value: %T", idI)
		}
		createStatementI := vals[1]
		createStatement, ok := createStatementI.(*tree.DString)
		if !ok {
			return BasicMetadata{}, fmt.Errorf("unexpected value: %T", createStatementI)
		}
		kindI := vals[2]
		kind, ok := kindI.(*tree.DString)
		if !ok {
			return BasicMetadata{}, fmt.Errorf("unexpected value: %T", kindI)
		}
		alterI := vals[3]
		alter, ok := alterI.(*tree.DArray)
		if !ok {
			return BasicMetadata{}, fmt.Errorf("unexpected value: %T", alterI)
		}
		alterStatements, err := extractArray(alter)
		if err != nil {
			return BasicMetadata{}, err
		}
		validateI := vals[4]
		validate, ok := alterI.(*tree.DArray)
		if !ok {
			return BasicMetadata{}, fmt.Errorf("unexpected value: %T", validateI)
		}
		validateStatements, err := extractArray(validate)
		if err != nil {
			return BasicMetadata{}, err
		}
		// Get dependencies.
		rows, err := ex.Query(ctx, "DUMP", txn, fmt.Sprintf(`
		SELECT dependson_id
		FROM %s.zbdb_internal.backward_dependencies
		AS OF SYSTEM TIME %s
		WHERE descriptor_id = $1
		`, dbNameEscaped, lex.EscapeSQLString(ts)), id)
		if err != nil {
			return BasicMetadata{}, err
		}
		var refs []int64
		for _, vals := range rows {
			val := vals[0].(*tree.DInt)
			id := int64(*val)
			refs = append(refs, id)
		}
		md := BasicMetadata{
			ID:         id,
			Name:       name,
			CreateStmt: string(*createStatement),
			dependsOn:  refs,
			kind:       string(*kind),
			alter:      alterStatements,
			validate:   validateStatements,
		}
		return md, nil
	}
	return BasicMetadata{}, fmt.Errorf("unexpected vals len: %d", len(vals))
}

func extractArray(arr *tree.DArray) ([]string, error) {
	res := make([]string, len(arr.Array))
	for i, v := range arr.Array {
		res[i] = string(*v.(*tree.DString))
	}
	return res, nil
}

// CreateTable 导出创表语句
func CreateTable(w io.Writer, md BasicMetadata) error {
	table := md.Name
	a := strings.Split(md.CreateStmt, " ")
	if len(a) > 2 {
		a[2] = strings.Join([]string{string(table.CatalogName), string(table.SchemaName), string(table.TableName)}, ".")
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

func schemaCreateTable(w io.Writer, md BasicMetadata) error {
	table := md.Name
	a := strings.Split(md.CreateStmt, " ")
	if len(a) > 2 {
		a[2] = strings.Join([]string{string(table.SchemaName), string(table.TableName)}, ".")
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

// GetDumpSchemaData 获取模式的元数据信息
func GetDumpSchemaData(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	evalCtx *tree.EvalContext,
	scName string,
	dbName string,
	tableNames []*tree.TableName,
	asOf string,
) (mds []BasicMetadata, clusterTS string, err error) {
	if asOf == "" {
		vals, err := ex.QueryRow(ctx, "DUMP", txn, "SELECT cluster_logical_timestamp()")
		if err != nil {
			return nil, "", err
		}
		clusterTS = vals[0].String()
	} else {
		// Validate the timestamp. This prevents SQL injection.
		if _, err := tree.ParseDTimestamp(nil, asOf, time.Nanosecond); err != nil {
			return nil, "", err
		}
		clusterTS = asOf
	}

	if tableNames == nil {
		tableNames, err = GetTableNamesFromSchema(ctx, ex, txn, dbName, scName, clusterTS)
		if err != nil {
			return nil, "", err
		}
	}

	mds = make([]BasicMetadata, len(tableNames))
	for i, tableName := range tableNames {
		basicMD, err := getBasicMetadata(ctx, ex, txn, evalCtx, dbName, tableName, clusterTS, false)
		if err != nil {
			return nil, "", err
		}
		mds[i] = basicMD
	}

	return mds, clusterTS, nil
}

// GetTableNamesFromSchema 获取指定模式下的所有表信息
func GetTableNamesFromSchema(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	dbName string,
	scName string,
	ts string,
) (names []*tree.TableName, err error) {
	if ts == "" {
		vals, err := ex.QueryRow(ctx, "DUMP", txn, "SELECT cluster_logical_timestamp()")
		if err != nil {
			return nil, err
		}
		ts = vals[0].String()
	}
	rows, err := ex.Query(ctx, "DUMP", txn, fmt.Sprintf(`
		SELECT descriptor_name
		FROM "".zbdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1
        AND schema_name = $2 AND descriptor_type='table'
		`, lex.EscapeSQLString(ts)), dbName, scName)
	if err != nil {
		return nil, err
	}
	for _, vals := range rows {
		name := vals[0].(*tree.DString)
		tableName := tree.TableName{}
		tableName.TableName = tree.Name(*name)
		tableName.SchemaName = tree.Name(scName)
		tableName.CatalogName = tree.Name(dbName)
		tableName.ExplicitCatalog = true
		tableName.ExplicitSchema = true
		names = append(names, &tableName)
	}
	return names, nil
}

// HasDatabase verify database exists or not
func HasDatabase(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	evalCtx *tree.EvalContext,
	ts string,
	dbName string,
) error {
	if ts == "" {
		vals, err := ex.QueryRow(ctx, "DUMP", txn, "SELECT cluster_logical_timestamp()")
		if err != nil {
			return err
		}
		ts = vals[0].String()
	}
	rows, err := ex.Query(ctx, "DUMP", txn, fmt.Sprintf(`
		SELECT catalog_name
		FROM "".information_schema.schemata
		AS OF SYSTEM TIME %s
		`, lex.EscapeSQLString(ts)))
	if err != nil {
		return err
	}
	var hasDb bool
	for _, vals := range rows {
		name := vals[0]
		if name.Compare(evalCtx, tree.NewDString(dbName)) == 0 {
			hasDb = true
		}
	}
	if hasDb == false {
		return errors.Errorf("database %s not exist", dbName)
	}
	return nil
}

// HasSchema verify schema exists or not in database
func HasSchema(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	evalCtx *tree.EvalContext,
	ts string,
	dbName string,
	scName string,
) error {
	if ts == "" {
		vals, err := ex.QueryRow(ctx, "DUMP", txn, "SELECT cluster_logical_timestamp()")
		if err != nil {
			return err
		}
		ts = vals[0].String()
	}
	rows, err := ex.Query(ctx, "DUMP", txn, fmt.Sprintf(`
		SELECT schema_name
		FROM "".information_schema.schemata
		AS OF SYSTEM TIME %s
		WHERE catalog_name = $1
		`, lex.EscapeSQLString(ts)), dbName)
	if err != nil {
		return err
	}
	var hasSc bool
	for _, vals := range rows {
		name := vals[0]
		if name.Compare(evalCtx, tree.NewDString(scName)) == 0 {
			hasSc = true
		}
	}
	if hasSc == false {
		return errors.Errorf("schema %s not exist in %s", scName, dbName)
	}
	return nil
}

// GetSchemas get schemaNames from tableNames
func GetSchemas(tableNames []*tree.TableName) (schemaNames map[string]string) {
	schemaNames = make(map[string]string)
	for _, names := range tableNames {
		if _, ok := schemaNames[string(names.SchemaName)]; !ok {
			schemaNames[string(names.SchemaName)] = ""
		}
	}
	return schemaNames
}

// TableData 导出对应表数据
func TableData(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	evalCtx *tree.EvalContext,
	flowCtx *runbase.FlowCtx,
	spec *distsqlpb.CSVWriterSpec,
	clusterTS string,
	bmd BasicMetadata,
	resultsCh chan<- tree.Datums,
	isSettings bool,
) error {
	md, err := getMetadataForTable(ctx, ex, txn, evalCtx, bmd, clusterTS)
	if err != nil {
		return err
	}
	if md.Name == nil {
		return errors.Errorf("Name cannot be empty")
	}
	var bs string
	if isSettings {
		names := strings.Join([]string{md.Name.SchemaName.String(), md.Name.TableName.String()}, ".")
		bs = fmt.Sprintf("SELECT %s FROM %s AS OF SYSTEM TIME %s",
			md.columnNames,
			names,
			lex.EscapeSQLString(clusterTS),
		)
	} else {
		names := strings.Join([]string{md.Name.CatalogName.String(), md.Name.SchemaName.String(), md.Name.TableName.String()}, ".")
		bs = fmt.Sprintf("SELECT %s FROM %s AS OF SYSTEM TIME %s ORDER BY PRIMARY KEY %[2]s",
			md.columnNames,
			names,
			lex.EscapeSQLString(clusterTS),
		)
	}

	inserts := make([]string, 0, insertRows)
	rows, err := ex.Query(ctx, "DUMP", txn, bs)
	if err != nil {
		return err
	}
	cols := strings.Split(md.columnNames, ",")

	var valArray [2][]tree.Datum
	for i := range valArray {
		valArray[i] = make([]tree.Datum, len(cols))
	}

	g := ctxgroup.WithContext(context.Background())
	valsCh := make(chan tree.Datums)
	stringsCh := make(chan string, insertRows)

	g.GoCtx(func(ctx context.Context) error {
		// Fetch SQL rows and put them onto valsCh.
		defer close(valsCh)
		done := ctx.Done()
		for _, v := range rows {
			select {
			case <-done:
				return ctx.Err()
			case valsCh <- v:
			}
		}
		return nil
	})
	g.GoCtx(func(ctx context.Context) error {
		// Convert SQL rows into VALUE strings.
		defer close(stringsCh)
		f := tree.NewFmtCtx(tree.FmtParsableNumerics)
		defer f.Close()
		done := ctx.Done()
		for vals := range valsCh {
			f.Reset()
			// Values need to be correctly encoded for INSERT statements in a text file.
			for si, sv := range vals {
				if isSettings && si > 1 {
					break
				}
				if si > 0 {
					_, err = f.WriteString(", ")
					if err != nil {
						return err
					}
				}
				sv.Format(f)
			}
			select {
			case <-done:
				return ctx.Err()
			case stringsCh <- f.String():
			}
		}
		return nil
	})
	conf, err := dumpsink.ConfFromURI(ctx, spec.Destination)
	if err != nil {
		return err
	}
	es, err := flowCtx.Cfg.DumpSink(ctx, conf)
	if err != nil {
		return err
	}
	defer es.Close()

	g.Go(func() error {
		var buf bytes.Buffer
		w := csv.NewWriter(&buf)
		var fileCount uint64
		// Batch SQL strings into groups and write to output.
		for s := range stringsCh {
			inserts = append(inserts, s)
			if len(inserts) == cap(inserts) {
				buf.Reset()
				err = writeInserts(w.GetBufio(), md, inserts)
				if err != nil {
					return err
				}
				w.Flush()
				inserts = inserts[:0]
				bufBytes := buf.Bytes()
				var filename string
				part := fmt.Sprintf("n%d.%d", flowCtx.EvalCtx.NodeID, fileCount)
				filename = strings.Replace(spec.NamePattern, exportFilePatternPart, part, -1)
				if spec.Encryption != nil {
					bufBytes, err = storageicl.EncryptFile(bufBytes, spec.Encryption.Key)
					if err != nil {
						return err
					}
				}
				if spec.CompressionCodec == roachpb.FileCompression_Gzip {
					bufBytes, filename, _, err = gziputil.RunGzip(bufBytes, part, *spec)
					if err != nil {
						return err
					}
				}
				fullName := strings.Join([]string{string(md.Name.SchemaName), string(md.Name.TableName)}, ".")
				filename = strings.Join([]string{fullName, filename}, string(os.PathSeparator))

				if es.Conf().Provider == roachpb.ExportStorageProvider_Http && spec.HttpHeader != "" {
					err := es.SetConfig(spec.HttpHeader)
					if err != nil {
						return err
					}
				}

				if err := es.WriteFile(ctx, filename, bytes.NewReader(bufBytes)); err != nil {
					return err
				}
				atomic.AddUint64(&fileCount, 1)
				size := len(bufBytes)
				resultsCh <- tree.Datums{
					tree.NewDString(filename),
					tree.NewDInt(tree.DInt(size)),
				}
			}
		}
		if len(inserts) != 0 {
			buf.Reset()
			if isSettings {
				err = writeSet(w.GetBufio(), md, inserts)
			} else {
				err = writeInserts(w.GetBufio(), md, inserts)
			}
			if err != nil {
				return err
			}
			w.Flush()
			inserts = inserts[:0]
			var filename string
			part := fmt.Sprintf("n%d.%d", flowCtx.EvalCtx.NodeID, fileCount)
			filename = strings.Replace(spec.NamePattern, exportFilePatternPart, part, -1)
			bufBytes := buf.Bytes()
			if spec.Encryption != nil {
				bufBytes, err = storageicl.EncryptFile(bufBytes, spec.Encryption.Key)
				if err != nil {
					return err
				}
			}
			if spec.CompressionCodec == roachpb.FileCompression_Gzip {
				bufBytes, filename, _, err = gziputil.RunGzip(bufBytes, part, *spec)
				if err != nil {
					return err
				}
			}
			if !isSettings {
				fullName := strings.Join([]string{string(md.Name.SchemaName), string(md.Name.TableName)}, ".")
				filename = strings.Join([]string{fullName, filename}, string(os.PathSeparator))
			}

			if es.Conf().Provider == roachpb.ExportStorageProvider_Http && spec.HttpHeader != "" {
				err := es.SetConfig(spec.HttpHeader)
				if err != nil {
					return err
				}
			}
			if err := es.WriteFile(ctx, filename, bytes.NewReader(bufBytes)); err != nil {
				return err
			}
			atomic.AddUint64(&fileCount, 1)
			size := len(bufBytes)
			resultsCh <- tree.Datums{
				tree.NewDString(filename),
				tree.NewDInt(tree.DInt(size)),
			}
		}
		return nil
	})
	return g.Wait()
}

func getMetadataForTable(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *client.Txn,
	evalCtx *tree.EvalContext,
	md BasicMetadata,
	ts string,
) (tableMetadata, error) {

	makeQuery := func(colname string) string {
		return fmt.Sprintf(`
		SELECT COLUMN_NAME, %s
		FROM %s.information_schema.columns
		AS OF SYSTEM TIME %s
		WHERE TABLE_CATALOG = $1
			AND TABLE_SCHEMA = $2
			AND TABLE_NAME = $3
			AND GENERATION_EXPRESSION = ''
		`, colname, &md.Name.CatalogName, lex.EscapeSQLString(ts))
	}
	rows, err := ex.Query(ctx, "DUMP", txn, makeQuery("ZNBASE_SQL_TYPE")+` AND IS_HIDDEN = 'NO'`,
		md.Name.Catalog(), md.Name.Schema(), md.Name.Table())
	if err != nil {
		if strings.Contains(err.Error(), "column \"znbase_sql_type\" does not exist") {
			rows, err = ex.Query(ctx, "DUMP", txn, makeQuery("DATA_TYPE")+` AND IS_HIDDEN = 'NO'`,
				md.Name.Catalog(), md.Name.Schema(), md.Name.Table())
		}
		if err != nil {
			if strings.Contains(err.Error(), "column \"is_hidden\" does not exist") {
				rows, err = ex.Query(ctx, "DUMP", txn, makeQuery("DATA_TYPE"),
					md.Name.Catalog(), md.Name.Schema(), md.Name.Table())
			}
		}
		if err != nil {
			return tableMetadata{}, err
		}
	}
	colTypes := make(map[string]coltypes.T)
	colNamesSlice := make([]string, 0, len(rows))
	colnames := tree.NewFmtCtx(tree.FmtSimple)
	defer colnames.Close()
	for _, vals := range rows {
		name, typ := vals[0].(*tree.DString), vals[1].(*tree.DString)
		sqlRow := fmt.Sprintf("ALTER TABLE woo ALTER COLUMN woo SET DATA TYPE %s", *typ)
		stmt, err := parser.ParseOne(sqlRow, ex.GetCaseSensitive())
		if err != nil {
			return tableMetadata{}, fmt.Errorf("type %s is not a valid ZNBaseDB type", *typ)
		}
		coltyp := stmt.AST.(*tree.AlterTable).Cmds[0].(*tree.AlterTableAlterColumnType).ToType
		colTypes[string(*name)] = coltyp
		if colnames.Len() > 0 {
			_, err = colnames.WriteString(", ")
			if err != nil {
				return tableMetadata{}, err
			}
		}
		colNamesSlice = append(colNamesSlice, string(*name))
		colnames.FormatName(string(*name))
	}
	return tableMetadata{
		BasicMetadata: md,
		columnNames:   colnames.String(),
		colNamesSlice: colNamesSlice,
		columnTypes:   colTypes,
	}, nil
}
func writeInserts(w io.Writer, tmd tableMetadata, inserts []string) error {
	_, err := fmt.Fprintf(w, "\nINSERT INTO %s (%s) VALUES", tmd.Name.FQString(), tmd.columnNames)
	if err != nil {
		return err
	}
	for idx, values := range inserts {
		if idx > 0 {
			_, err = fmt.Fprint(w, ",")
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(w, "\n\t(%s)", values)
		if err != nil {
			return err
		}
	}
	_, err = fmt.Fprintln(w, ";")
	if err != nil {
		return err
	}
	return nil
}

func writeSet(w *bufio.Writer, tmd tableMetadata, inserts []string) error {
	var err error
	for idx, v := range inserts {
		if idx > 0 {
			slice := strings.Split(v, ",")
			if len(slice) != 2 {
				return errors.New("unexpected data format")
			}
			//拼接SQL语句时参数名称不可带有单引号，否则数据库无法识别
			vTrim := strings.Trim(slice[0], "'")
			_, err = fmt.Fprintf(w, "\nSET CLUSTER SETTING  %s = %s", vTrim, slice[1])
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(w, ";")
			if err != nil {
				return err
			}
		}
	}
	return nil
}
