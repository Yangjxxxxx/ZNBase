// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/CCL.txt

package load

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/build"
	"github.com/znbasedb/znbase/pkg/icl/dump"
	"github.com/znbasedb/znbase/pkg/icl/gossipicl"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/stats"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	csvDelimiter         = "delimiter"
	csvRowDelimiter      = "rowdelimiter"
	csvfileEscape        = "csv_escaped_by"
	csvfileEnclose       = "csv_enclosed_by"
	csvComment           = "comment"
	csvNullIf            = "nullif"
	csvSkip              = "skip"
	csvNull              = `\N`
	csvDist              = "nulldist"
	mysqlOutfileRowSep   = "rows_terminated_by"
	mysqlOutfileFieldSep = "fields_terminated_by"
	mysqlOutfileEnclose  = "fields_enclosed_by"
	mysqlOutfileEscape   = "fields_escaped_by"

	importOptionTransform  = "transform"
	importOptionSSTSize    = "sstsize"
	importOptionDecompress = "decompress"
	importOptionOversample = "oversample"
	importOptionSkipFKs    = "skip_foreign_keys"
	importOptionsIntoDb    = "into_db"
	importOptionEncoding   = "encoding"
	dumpOptEncPassphrase   = "encryption_passphrase"
	importOptionHTTPHeader = "http_header"

	importRejectedRows       = "rejectrows"
	importOptionDirectIngest = "direct_ingestion"
	importOptionSaveRejected = "save_rejected"
	importOptionOnlySchema   = "only_schema"
	importRejectedAddress    = "rejectaddress"
	pgCopyDelimiter          = "delimiter"
	pgCopyNull               = "nullif"

	pgMaxRowSize = "max_row_size"

	loadKafkaGroupID                 = "kafka_group_id"
	loadKafkaSkipPrimaryKeyConflicts = "skip_primary_key"
	importOptionAllFiles             = "allfiles"
)

var importOptionExpectValues = map[string]sql.KVStringOptValidate{
	csvDelimiter:    sql.KVStringOptRequireValue,
	csvRowDelimiter: sql.KVStringOptRequireValue,
	csvfileEscape:   sql.KVStringOptRequireValue,
	csvfileEnclose:  sql.KVStringOptRequireValue,
	csvComment:      sql.KVStringOptRequireValue,
	csvNullIf:       sql.KVStringOptRequireValue,
	csvSkip:         sql.KVStringOptRequireValue,
	csvDist:         sql.KVStringOptRequireNoValue,

	mysqlOutfileRowSep:   sql.KVStringOptRequireValue,
	mysqlOutfileFieldSep: sql.KVStringOptRequireValue,
	mysqlOutfileEnclose:  sql.KVStringOptRequireValue,
	mysqlOutfileEscape:   sql.KVStringOptRequireValue,

	importOptionTransform:  sql.KVStringOptRequireValue,
	importOptionSSTSize:    sql.KVStringOptRequireValue,
	importOptionDecompress: sql.KVStringOptRequireValue,
	importOptionOversample: sql.KVStringOptRequireValue,
	importOptionsIntoDb:    sql.KVStringOptRequireValue,
	importOptionEncoding:   sql.KVStringOptRequireValue,

	importOptionSkipFKs:      sql.KVStringOptRequireNoValue,
	importOptionOnlySchema:   sql.KVStringOptRequireNoValue,
	importRejectedRows:       sql.KVStringOptRequireValue,
	importRejectedAddress:    sql.KVStringOptRequireValue,
	importOptionDirectIngest: sql.KVStringOptRequireNoValue,
	importOptionSaveRejected: sql.KVStringOptRequireNoValue,
	pgMaxRowSize:             sql.KVStringOptRequireValue,
	dumpOptEncPassphrase:     sql.KVStringOptRequireValue,

	loadKafkaGroupID:                 sql.KVStringOptRequireValue,
	loadKafkaSkipPrimaryKeyConflicts: sql.KVStringOptRequireNoValue,
	importOptionHTTPHeader:           sql.KVStringOptRequireValue,
	importOptionAllFiles:             sql.KVStringOptRequireNoValue,
}

const (
	// We need to choose arbitrary database and table IDs. These aren't important,
	// but they do match what would happen when creating a new database and
	// table on an empty cluster.
	defaultCSVParentID sqlbase.ID = keys.MinNonPredefinedUserDescID
	defaultCSVTableID             = defaultCSVParentID + 2
	jobID                         = "job_id"
	recordStatus                  = "status"
	rows                          = "rows"
	indexEntries                  = "index_entries"
	systemRecords                 = "system_records"
	recordBytes                   = "bytes"
)

var clusterHeader = sqlbase.ResultColumns{
	{Name: "prompt_information", Typ: types.String},
}

func readCreateTableFromStore(
	ctx context.Context, filename string, makeDumpSinkFromURI dumpsink.FromURIFactory, header string,
) (*tree.CreateTable, error) {
	store, err := makeDumpSinkFromURI(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	if store.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
		err := store.SetConfig(header)
		if err != nil {
			return nil, err
		}
	}
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	stmt, err := parser.ParseOne(string(tableDefStr), false)
	if err != nil {
		return nil, err
	}
	create, ok := stmt.AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.New("expected CREATE TABLE statement in table file")
	}
	return create, nil
}

func readCreateDDLFromStore(
	ctx context.Context,
	filename string,
	p sql.PlanHookState,
	evalCtx *tree.EvalContext,
	match string,
	parentID sqlbase.ID,
	walltime int64,
	fks fkHandler,
	max int,
	statusServer *serverpb.StatusServer,
	encryption *roachpb.FileEncryptionOptions,
	header string,
) (ParseDDL, error) {
	ddl := ParseDDL{}
	store, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, filename)
	if err != nil {
		return ddl, err
	}
	defer store.Close()
	if store.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
		err := store.SetConfig(header)
		if err != nil {
			return ddl, err
		}
	}
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return ddl, err
	}
	defer reader.Close()
	return ReadParseDDL(reader, evalCtx, p.ExecCfg().Settings, match, parentID, walltime, fks, max, statusServer, encryption, p.User())
}

func readCreateDDLFromSchema(
	ctx context.Context,
	filename string,
	p sql.PlanHookState,
	evalCtx *tree.EvalContext,
	match string,
	parentID sqlbase.ID,
	walltime int64,
	fks fkHandler,
	max int,
	statusServer *serverpb.StatusServer,
	encryption *roachpb.FileEncryptionOptions,
	dbName string,
	header string,
) (ParseDDL, error) {
	ddl := ParseDDL{}
	store, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, filename)
	if err != nil {
		return ddl, err
	}
	defer store.Close()
	if store.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
		err := store.SetConfig(header)
		if err != nil {
			return ddl, err
		}
	}
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return ddl, err
	}
	defer reader.Close()
	return readParseDDLFromSchema(ctx, p, reader, evalCtx, p.ExecCfg().Settings, match, parentID, walltime, fks, max, dbName, statusServer, encryption, p.User())
}

type fkHandler struct {
	allowed  bool
	skip     bool
	resolver fkResolver
}

// NoFKs is used by formats that do not support FKs.
var NoFKs = fkHandler{resolver: make(fkResolver)}

// MakeSimpleTableDescriptor creates a MutableTableDescriptor from a CreateTable parse
// node without the full machinery. Many parts of the syntax are unsupported
// (see the implementation and TestMakeSimpleTableDescriptorErrors for details),
// but this is enough for our csv IMPORT and for some unit tests.
//
// Any occurrence of SERIAL in the column definitions is handled using
// the CockroachDB legacy behavior, i.e. INT NOT NULL DEFAULT
// unique_rowid().
func MakeSimpleTableDescriptor(
	ctx context.Context,
	st *cluster.Settings,
	create *tree.CreateTable,
	parentID, tableID sqlbase.ID,
	fks fkHandler,
	walltime int64,
	statusServer *serverpb.StatusServer,
	user string,
) (*sqlbase.MutableTableDescriptor, error) {
	create.HoistConstraints()
	if create.IfNotExists {
		return nil, pgerror.Unimplemented("import.if-no-exists", "unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return nil, pgerror.Unimplemented("import.interleave", "interleaved not supported")
	}
	if create.AsSource != nil {
		return nil, pgerror.Unimplemented("import.create-as", "CREATE AS not supported")
	}

	// if hash partition, construct 'hashnum' column
	if create.PartitionBy != nil && create.PartitionBy.IsHash {
		for _, def := range create.Defs {
			d, ok := def.(*tree.ColumnTableDef)
			if !ok {
				continue
			}
			if d.Name == "hashnum" {
				return nil, errors.New("unexpected column name 'hashnum' in hash partition table")
			}
		}
		hashNumCol := &tree.ColumnTableDef{
			Name: "hashnum",
			Type: &coltypes.TInt{Width: 64},
			Nullable: struct {
				Nullability    tree.Nullability
				ConstraintName tree.Name
			}{Nullability: 2, ConstraintName: ""},
			PrimaryKey:           false,
			Unique:               false,
			UniqueConstraintName: "",
			DefaultExpr: struct {
				Expr                     tree.Expr
				ConstraintName           tree.Name
				OnUpdateCurrentTimeStamp bool
			}{Expr: nil, ConstraintName: "", OnUpdateCurrentTimeStamp: false},
			CheckExprs: nil,
			References: struct {
				Table          *tree.TableName
				Col            tree.Name
				ConstraintName tree.Name
				Actions        tree.ReferenceActions
				Match          tree.CompositeKeyMatchMethod
			}{Table: nil, Col: "", ConstraintName: "", Actions: tree.ReferenceActions{Delete: tree.NoAction, Update: tree.NoAction}, Match: tree.MatchSimple},
			Computed: struct {
				Computed bool
				Expr     tree.Expr
			}{Computed: false, Expr: nil},
			Family: struct {
				Name        tree.Name
				Create      bool
				IfNotExists bool
			}{Name: "", Create: false, IfNotExists: false},
		}
		create.Defs = append(create.Defs, hashNumCol)
	}

	filteredDefs := create.Defs[:0]
	for i := range create.Defs {
		switch def := create.Defs[i].(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if def.Computed.Expr != nil {
				return nil, pgerror.Unimplemented("import.computed", "computed columns not supported: %s", tree.AsString(def))
			}

			if err := sql.SimplifySerialInColumnDefWithRowID(ctx, def, &create.Table); err != nil {
				return nil, err
			}

		case *tree.ForeignKeyConstraintTableDef:
			if !fks.allowed {
				return nil, pgerror.Unimplemented("import.fk", "this IMPORT format does not support foreign keys")
			}
			if fks.skip {
				continue
			}
			// Strip the schema/db prefix.
			def.Table = tree.MakeUnqualifiedTableName(def.Table.TableName)

		default:
			return nil, pgerror.Unimplemented(fmt.Sprintf("import.%T", def), "unsupported table definition: %s", tree.AsString(def))
		}
		// only append this def after we make it past the error checks and continues
		filteredDefs = append(filteredDefs, create.Defs[i])
	}
	create.Defs = filteredDefs

	semaCtx := tree.SemaContext{}
	evalCtx := tree.EvalContext{
		Context:  ctx,
		Sequence: &importSequenceOperators{},
	}
	affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)

	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		fks.resolver,
		st,
		create,
		parentID,
		tableID,
		hlc.Timestamp{WallTime: walltime},
		sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, user), // todo(xz): user?
		affected,
		&semaCtx,
		&evalCtx,
		statusServer,
		nil,
	)
	if err != nil {
		return nil, err
	}
	if err := fixDescriptorFKState(tableDesc.TableDesc()); err != nil {
		return nil, err
	}

	return &tableDesc, nil
}

// fixDescriptorFKState repairs validity and table states set during descriptor
// creation. sql.MakeTableDesc and ResolveFK set the table to the ADD state
// and mark references an validated. This function sets the table to PUBLIC
// and the FKs to unvalidated.
func fixDescriptorFKState(tableDesc *sqlbase.TableDescriptor) error {
	tableDesc.State = sqlbase.TableDescriptor_PUBLIC
	return tableDesc.ForeachNonDropIndex(func(idx *sqlbase.IndexDescriptor) error {
		if idx.ForeignKey.IsSet() {
			idx.ForeignKey.Validity = sqlbase.ConstraintValidity_Unvalidated
		}
		return nil
	})
}

var (
	errSequenceOperators = errors.New("sequence operations unsupported")
	errSchemaResolver    = errors.New("schema resolver unsupported")
)

// Implements the tree.SequenceOperators interface.
type importSequenceOperators struct {
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) ParseQualifiedTableName(
	ctx context.Context, sql string,
) (*tree.TableName, error) {
	return parser.ParseTableName(sql, 0)
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) ResolveTableName(ctx context.Context, tn *tree.TableName) error {
	return errSequenceOperators
}

// Implements the tree.EvalDatabase interface.
func (so *importSequenceOperators) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	return false, nil, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) IncrementSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	return 0, errSequenceOperators
}

// Implements the tree.SequenceOperators interface.
func (so *importSequenceOperators) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	return errSequenceOperators
}

type fkResolver map[string]*sqlbase.MutableTableDescriptor

var _ sql.SchemaResolver = fkResolver{}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) Txn() *client.Txn {
	return nil
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) LogicalSchemaAccessor() sql.SchemaAccessor {
	return nil
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CurrentDatabase() string {
	return ""
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CurrentSearchPath() sessiondata.SearchPath {
	return sessiondata.SearchPath{}
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) CommonLookupFlags(required bool) sql.CommonLookupFlags {
	return sql.CommonLookupFlags{}
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) ObjectLookupFlags(required bool, requireMutable bool) sql.ObjectLookupFlags {
	return sql.ObjectLookupFlags{}
}

// Implements the tree.TableNameExistingResolver interface.
func (r fkResolver) LookupObject(
	ctx context.Context, requireMutable bool, requireFunction bool, dbName, scName, obName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	if scName != "" {
		obName = strings.TrimPrefix(obName, scName+".")
	}
	tbl, ok := r[obName]
	if ok {
		return true, tbl, nil
	}
	names := make([]string, 0, len(r))
	for k := range r {
		names = append(names, k)
	}
	suggestions := strings.Join(names, ",")
	return false, nil, errors.Errorf("referenced table %q not found in tables being imported (%s)", obName, suggestions)
}

// Implements the tree.TableNameTargetResolver interface.
func (r fkResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	return false, nil, errSchemaResolver
}

// Implements the sql.SchemaResolver interface.
func (r fkResolver) LookupTableByID(ctx context.Context, id sqlbase.ID) (row.TableEntry, error) {
	return row.TableEntry{}, errSchemaResolver
}

const csvDatabaseName = "csv"

func finalizeCSVBackup(
	ctx context.Context,
	backupDesc *dump.DumpDescriptor,
	parentID sqlbase.ID,
	tables map[string]*sqlbase.TableDescriptor,
	es dumpsink.DumpSink,
	execCfg *sql.ExecutorConfig,
) error {
	sort.Sort(dump.FileDescriptors(backupDesc.Files))

	backupDesc.Spans = make([]roachpb.Span, 0, len(tables))
	backupDesc.Descriptors = make([]sqlbase.Descriptor, 2, len(tables)+2)
	backupDesc.Descriptors[0] = *sqlbase.WrapDescriptor(
		&sqlbase.DatabaseDescriptor{Name: csvDatabaseName, ID: parentID - 1},
	)
	backupDesc.Descriptors[1] = *sqlbase.WrapDescriptor(
		&sqlbase.SchemaDescriptor{Name: "public", ID: parentID, ParentID: parentID - 1},
	)

	for _, table := range tables {
		backupDesc.Spans = append(backupDesc.Spans, table.TableSpan())
		backupDesc.Descriptors = append(backupDesc.Descriptors, *sqlbase.WrapDescriptor(table))
	}

	backupDesc.FormatVersion = dump.BackupFormatInitialVersion
	backupDesc.BuildInfo = build.GetInfo()
	if execCfg != nil {
		backupDesc.NodeID = execCfg.NodeID.Get()
		backupDesc.ClusterID = execCfg.ClusterID()
	}
	descBuf, err := protoutil.Marshal(backupDesc)
	if err != nil {
		return err
	}
	return es.WriteFile(ctx, dump.DumpDescriptorName, bytes.NewReader(descBuf))
}

func importJobDescription(
	orig *tree.LoadImport, defs tree.TableDefs, files []string, opts map[string]string,
) (string, error) {
	stmt := *orig
	stmt.CreateFile = nil
	stmt.CreateDefs = defs
	stmt.Files = nil
	for _, file := range files {
		clean, err := dumpsink.SanitizeDumpSinkURI(file)
		if err != nil {
			return "", err
		}
		stmt.Files = append(stmt.Files, tree.NewDString(clean))
	}
	stmt.Options = nil
	for k, v := range opts {
		switch k {
		case importOptionTransform:
			clean, err := dumpsink.SanitizeDumpSinkURI(v)
			if err != nil {
				return "", err
			}
			v = clean
		}
		opt := tree.KVOption{Key: tree.Name(k)}
		val := importOptionExpectValues[k] == sql.KVStringOptRequireValue
		val = val || (importOptionExpectValues[k] == sql.KVStringOptAny && len(v) > 0)
		if val {
			opt.Value = tree.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	return tree.AsStringWithFlags(&stmt, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType), nil
}

func getAllFilesInPath(
	ctx context.Context, p sql.PlanHookState, fileName string, format roachpb.IOFileFormat,
) ([]string, error) {
	conf, err := dumpsink.ConfFromURI(ctx, fileName)
	if err != nil {
		return nil, err
	}
	store, err := p.ExecCfg().DistSQLSrv.DumpSink(ctx, conf)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	if store.Conf().Provider == roachpb.ExportStorageProvider_Http && format.HttpHeader != "" {
		err := store.SetConfig(format.HttpHeader)
		if err != nil {
			return nil, err
		}
	}
	//TODO 暂时不完全支持http服务，这是个很大的问题，目前的实现仅仅是通过解析html文件中的href
	patternSuffix := ""
	switch format.Format {
	case roachpb.IOFileFormat_CSV:
		patternSuffix = ".csv"
	case roachpb.IOFileFormat_MysqlOutfile:
		patternSuffix = ".csv"
	case roachpb.IOFileFormat_Mysqldump:
		patternSuffix = ".sql"
	case roachpb.IOFileFormat_PgDump:
		patternSuffix = ".sql"
	case roachpb.IOFileFormat_TXT:
		patternSuffix = ".txt"
	}
	patternSuffixOrig := patternSuffix

	tableFilesTmp, err := store.ListFiles(ctx, "")
	if err != nil {
		return nil, err
	}
	var tableFiles []string
	for _, tableFile := range tableFilesTmp {
		patternSuffix = patternSuffixOrig
		compression := guessCompressionFromName(tableFile, format.Compression)
		if compression == roachpb.IOFileFormat_Gzip {
			patternSuffix = strings.Join([]string{patternSuffix, "gz"}, ".")
		}
		if compression == roachpb.IOFileFormat_Bzip {
			patternSuffix = strings.Join([]string{patternSuffix, "bz"}, ".")
		}
		if strings.HasSuffix(tableFile, patternSuffix) {
			tableFiles = append(tableFiles, tableFile)
		}
	}
	if len(tableFiles) == 0 {
		return nil, errors.New("the corresponding file cannot be found")
	}
	for i := 0; i < len(tableFiles); i++ {
		tableFiles[i] = strings.Join([]string{fileName, tableFiles[i]}, string(os.PathSeparator))
	}
	return tableFiles, nil
}

func getFormatParas(
	importStmt *tree.LoadImport, opts map[string]string, p sql.PlanHookState,
) (roachpb.IOFileFormat, error) {
	_, nullDist := opts[csvDist]
	format := roachpb.IOFileFormat{}
	if value, ok := opts[importRejectedRows]; ok {
		maxRejectedRows, err := strconv.Atoi(value)
		if err != nil {
			return format, errors.Wrapf(err, "invalid %d value", maxRejectedRows)
		}
		format.MaxRejectedRows = int64(maxRejectedRows)
		if importStmt.FileFormat == "KAFKA" {
			return format, pgerror.NewError(pgcode.InvalidParameterValue, "load kafka is temporarily not supported rejectrows, the default value is 1<<63-1")
		}
	} else {
		format.MaxRejectedRows = 1000
	}
	if override, ok := opts[importOptionHTTPHeader]; ok {
		format.HttpHeader = override
	}

	//Import only metadata information
	if _, ok := opts[importOptionOnlySchema]; ok {
		if importStmt.Into || importStmt.Bundle {
			return format, pgerror.NewError(pgcode.InvalidParameterValue, "only_schema does not support the current operation")
		}
		format.OnlySchema = true
	} else {
		format.OnlySchema = false
	}
	if value, ok := opts[importRejectedAddress]; ok {
		format.RejectedAddress = value
	}
	switch importStmt.FileFormat {
	case "CSV":
		telemetry.Count("import.format.csv")
		format.Format = roachpb.IOFileFormat_CSV
		format.Csv.Null = csvNull
		format.Csv.Rowma = '\n'
		format.Csv.Comma = ','

		if nullDist {
			format.Csv.NullEncoding = &format.Csv.Null
		}
		if override, ok := opts[csvDelimiter]; ok {
			comma, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrap(err, "invalid comma value")
			}
			format.Csv.Comma = comma
		}
		if override, ok := opts[csvRowDelimiter]; ok {
			rowma, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrap(err, "invalid rowma value")
			}
			format.Csv.Rowma = rowma
		}
		if override, ok := opts[csvfileEscape]; ok {
			c, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %q value", csvfileEscape)
			}
			format.Csv.HasEscape = true
			format.Csv.Escape = c
		}
		if override, ok := opts[csvfileEnclose]; ok {
			c, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %q value", csvfileEnclose)
			}
			format.Csv.Enclose = roachpb.CSVOptions_Always
			format.Csv.Encloser = c
		}
		if override, ok := opts[csvComment]; ok {
			comment, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrap(err, "invalid comment value")
			}
			format.Csv.Comment = comment
		}

		if override, ok := opts[csvNullIf]; ok {
			format.Csv.NullEncoding = &override
		}

		if override, ok := opts[csvSkip]; ok {
			skip, err := strconv.Atoi(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %s value", csvSkip)
			}
			if skip < 0 {
				return format, errors.Errorf("%s must be >= 0", csvSkip)
			}
			// We need to handle the case where the user wants StatusServerto skip records and the node
			// interpreting the statement might be newer than other nodes in the cluster.
			if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionImportSkipRecords) {
				return format, errors.Errorf("Using non-CSV import format requires all nodes to be upgraded to %s",
					cluster.VersionByKey(cluster.VersionImportSkipRecords))
			}
			format.Csv.Skip = uint32(skip)
		}
		if _, ok := opts[importOptionSaveRejected]; ok {
			format.SaveRejected = true
		}
		if importStmt.Conversions != nil {
			conversionFunc := make(map[string]*roachpb.ConversionFunc)
			for _, conversion := range importStmt.Conversions {
				params := make([]string, len(conversion.Params))
				for i, expr := range conversion.Params {
					paramFunc, err := p.TypeAsString(expr, "IMPORT")
					if err != nil {
						return format, errors.Wrapf(err, "invalid %q value", csvfileEscape)
					}
					param, err := paramFunc()
					if err != nil {
						return format, errors.Wrapf(err, "invalid %q value", csvfileEscape)
					}
					params[i] = param
				}
				ConversionFunc := &roachpb.ConversionFunc{Name: conversion.FuncName, Param: params}
				conversionFunc[conversion.ColumnName.String()] = ConversionFunc
			}
			format.ConversionFunc = conversionFunc
		}

	case "TXT":
		telemetry.Count("import.format.txt")
		format.Format = roachpb.IOFileFormat_TXT
		format.Txt.Null = csvNull
		if nullDist {
			format.Txt.NullEncoding = &format.Txt.Null
		}
		if override, ok := opts[csvDelimiter]; ok {
			comma, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrap(err, "invalid comma value")
			}
			format.Txt.Comma = comma
		}

		if override, ok := opts[csvComment]; ok {
			comment, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrap(err, "invalid comment value")
			}
			format.Txt.Comment = comment
		}

		if override, ok := opts[csvNullIf]; ok {
			format.Txt.NullEncoding = &override
		}

		if override, ok := opts[csvSkip]; ok {
			skip, err := strconv.Atoi(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %s value", csvSkip)
			}
			if skip < 0 {
				return format, errors.Errorf("%s must be >= 0", csvSkip)
			}
			// We need to handle the case where the user wants StatusServerto skip records and the node
			// interpreting the statement might be newer than other nodes in the cluster.
			if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionImportSkipRecords) {
				return format, errors.Errorf("Using non-TXT import format requires all nodes to be upgraded to %s",
					cluster.VersionByKey(cluster.VersionImportSkipRecords))
			}
			format.Txt.Skip = uint32(skip)
		}
		if _, ok := opts[importOptionSaveRejected]; ok {
			format.SaveRejected = true
		}
	case "ZNBASEDUMP":
		telemetry.Count("import.format.znbasedump")
		format.Format = roachpb.IOFileFormat_ZNBaseDUMP
		maxRowSize := int32(defaultScanBuffer)
		if override, ok := opts[pgMaxRowSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return format, err
			}
			if sz < 1 || sz > math.MaxInt32 {
				return format, errors.Errorf("%s out of range: %d", pgMaxRowSize, sz)
			}
			maxRowSize = int32(sz)
		}
		format.PgDump.MaxRowSize = maxRowSize
	case "MYSQLOUTFILE":
		telemetry.Count("import.format.mysqlout")
		format.Format = roachpb.IOFileFormat_MysqlOutfile
		format.MysqlOut = roachpb.MySQLOutfileOptions{
			RowSeparator:   '\n',
			FieldSeparator: '\t',
		}
		if override, ok := opts[mysqlOutfileRowSep]; ok {
			c, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %q value", mysqlOutfileRowSep)
			}
			format.MysqlOut.RowSeparator = c
		}

		if override, ok := opts[mysqlOutfileFieldSep]; ok {
			c, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %q value", mysqlOutfileFieldSep)
			}
			format.MysqlOut.FieldSeparator = c
		}

		if override, ok := opts[mysqlOutfileEnclose]; ok {
			c, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %q value", mysqlOutfileRowSep)
			}
			format.MysqlOut.Enclose = roachpb.MySQLOutfileOptions_Always
			format.MysqlOut.Encloser = c
		}

		if override, ok := opts[mysqlOutfileEscape]; ok {
			c, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %q value", mysqlOutfileRowSep)
			}
			format.MysqlOut.HasEscape = true
			format.MysqlOut.Escape = c
		}
		if _, ok := opts[importOptionSaveRejected]; ok {
			format.SaveRejected = true
		}
	case "MYSQLDUMP":
		telemetry.Count("import.format.mysqldump")
		format.Format = roachpb.IOFileFormat_Mysqldump
	case "PGCOPY":
		telemetry.Count("import.format.pgcopy")
		format.Format = roachpb.IOFileFormat_PgCopy
		format.PgCopy = roachpb.PgCopyOptions{
			Delimiter: '\t',
			Null:      `\N`,
		}
		if override, ok := opts[pgCopyDelimiter]; ok {
			c, err := util.GetSingleRune(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %q value", pgCopyDelimiter)
			}
			format.PgCopy.Delimiter = c
		}
		if override, ok := opts[pgCopyNull]; ok {
			format.PgCopy.Null = override
		}
		maxRowSize := int32(defaultScanBuffer)
		if override, ok := opts[pgMaxRowSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return format, err
			}
			if sz < 1 || sz > math.MaxInt32 {
				return format, errors.Errorf("%s out of range: %d", pgMaxRowSize, sz)
			}
			maxRowSize = int32(sz)
		}
		format.PgCopy.MaxRowSize = maxRowSize
	case "PGDUMP":
		telemetry.Count("import.format.pgdump")
		format.Format = roachpb.IOFileFormat_PgDump
		maxRowSize := int32(defaultScanBuffer)
		if override, ok := opts[pgMaxRowSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return format, err
			}
			if sz < 1 || sz > math.MaxInt32 {
				return format, errors.Errorf("%s out of range: %d", pgMaxRowSize, sz)
			}
			maxRowSize = int32(sz)
		}
		format.PgDump.MaxRowSize = maxRowSize

	case "KAFKA":
		telemetry.Count("import.format.kafka")
		format.Format = roachpb.IOFileFormat_Kafka
		format.Csv.Null = csvNull
		format.Csv.Rowma = '\n'
		format.Csv.Comma = ','
		if nullDist {
			format.Csv.NullEncoding = &format.Csv.Null
		}
		if _, ok := opts[csvDelimiter]; ok {
			//comma, err := util.GetSingleRune(override)
			//if err != nil {
			//	return errors.Wrap(err, "invalid comma value")
			//}
			//format.Csv.Comma = comma
			return format, errors.Errorf("load kafka is temporarily not supported delimiter")
		}
		if _, ok := opts[csvRowDelimiter]; ok {
			//rowma, err := util.GetSingleRune(override)
			//if err != nil {
			//	return errors.Wrap(err, "invalid rowma value")
			//}
			//format.Csv.Rowma = rowma
			return format, errors.Errorf("load kafka is temporarily not supported rowdelimiter")
		}
		if _, ok := opts[csvfileEscape]; ok {
			//c, err := util.GetSingleRune(override)
			//if err != nil {
			//	return errors.Wrapf(err, "invalid %q value", csvfileEscape)
			//}
			//format.Csv.HasEscape = true
			//format.Csv.Escape = c
			return format, errors.Errorf("load kafka is temporarily not supported csv_escaped_by")
		}
		if _, ok := opts[csvfileEnclose]; ok {
			//c, err := util.GetSingleRune(override)
			//if err != nil {
			//	return errors.Wrapf(err, "invalid %q value", csvfileEnclose)
			//}
			//format.Csv.Enclose = roachpb.CSVOptions_Always
			//format.Csv.Encloser = c
			return format, errors.Errorf("load kafka is temporarily not supported csv_enclosed_by")
		}
		if _, ok := opts[csvComment]; ok {
			//comment, err := util.GetSingleRune(override)
			//if err != nil {
			//	return errors.Wrap(err, "invalid comment value")
			//}
			//format.Csv.Comment = comment
			return format, errors.Errorf("load kafka is temporarily not supported comment")
		}

		if override, ok := opts[csvNullIf]; ok {
			format.Csv.NullEncoding = &override
		}

		if override, ok := opts[csvSkip]; ok {
			skip, err := strconv.Atoi(override)
			if err != nil {
				return format, errors.Wrapf(err, "invalid %s value", csvSkip)
			}
			if skip < 0 {
				return format, errors.Errorf("%s must be >= 0", csvSkip)
			}
			// We need to handle the case where the user wants StatusServerto skip records and the node
			// interpreting the statement might be newer than other nodes in the cluster.
			if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionImportSkipRecords) {
				return format, errors.Errorf("Using non-CSV import format requires all nodes to be upgraded to %s",
					cluster.VersionByKey(cluster.VersionImportSkipRecords))
			}
			format.Csv.Skip = uint32(skip)
		}
		if _, ok := opts[importOptionSaveRejected]; ok {
			format.SaveRejected = true
		}
		//kafka 对应的消费者组groupID配置
		if group, ok := opts[loadKafkaGroupID]; ok {
			format.Kafka.GroupId = &group
		}

		//kafka skip主键冲突配置
		if _, ok := opts[loadKafkaSkipPrimaryKeyConflicts]; ok {
			format.Kafka.SkipPrimaryKeyConflicts = true
		}
	default:
		return format, pgerror.Unimplemented("import.format", "unsupported import format: %q", importStmt.FileFormat)
	}
	if format.Format != roachpb.IOFileFormat_CSV && format.Format != roachpb.IOFileFormat_TXT {
		if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionImportFormats) {
			return format, errors.Errorf("Using %s requires all nodes to be upgraded to %s",
				csvSkip, cluster.VersionByKey(cluster.VersionImportFormats))
		}
	}
	if override, ok := opts[importOptionDecompress]; ok {
		found := false
		for name, value := range roachpb.IOFileFormat_Compression_value {
			if strings.EqualFold(name, override) {
				format.Compression = roachpb.IOFileFormat_Compression(value)
				found = true
				break
			}
		}
		if !found {
			return format, pgerror.Unimplemented("import.compression", "unsupported compression value: %q", override)
		}
	}
	return format, nil
}

// importPlanHook implements sql.PlanHookFn.
func importPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	importStmt, ok := stmt.(*tree.LoadImport)
	if !ok {
		return nil, nil, nil, false, nil
	}

	/*
		if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionPartitionedBackup) {
			return nil, nil, nil, false, errors.Errorf("IMPORT requires a cluster fully upgraded to version >= 19.2")
		} */

	filesFn, err := p.TypeAsStringArray(importStmt.Files, "IMPORT")
	if err != nil {
		return nil, nil, nil, false, err
	}

	var createFileFn func() (string, error)
	if !importStmt.Bundle && !importStmt.Into && importStmt.CreateDefs == nil {
		createFileFn, err = p.TypeAsString(importStmt.CreateFile, "IMPORT")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	optsFn, err := p.TypeAsStringOpts(importStmt.Options, importOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}
	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag())
		defer tracing.FinishSpan(span)

		walltime := p.ExecCfg().Clock.Now().WallTime
		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("IMPORT cannot be used inside a transaction")
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		files, err := filesFn()
		if err != nil {
			return err
		}
		table := importStmt.Table
		database := importStmt.Database
		transform := opts[importOptionTransform]
		ingestDirectly := false
		if _, ok := opts[importOptionDirectIngest]; ok {
			ingestDirectly = true
		}
		if _, renaming := opts[importOptionsIntoDb]; renaming {
			if importStmt.SchemaName == nil {
				return errors.Errorf("into_db only can be used while load schema")
			}
		}
		if transform != "" {
			ingestDirectly = false
		}
		if importStmt.Conversions != nil && importStmt.FileFormat != dump.CSV {
			return errors.Errorf("the column type conversion function can only be used when the file format is CSV")
		}
		var parentID sqlbase.ID
		var schema *sqlbase.SchemaDescriptor
		searchPaths := p.SessionData().SearchPath.GetPathArray()

		//Use the query metadata from the store to replace the original query metadata from the cache,
		//because after the metadata is placed on the disk,
		//it may not be broadcast to the cache of each node immediately.
		findSchema := func(dbDesc *sqlbase.DatabaseDescriptor, table *tree.TableName) (sqlbase.ID, *sqlbase.SchemaDescriptor, error) {
			var searchPath string
			var partParentID sqlbase.ID
			var schema *sqlbase.SchemaDescriptor
			sc := p.LogicalSchemaAccessor()
			if table != nil {
				if table.SchemaName != "" {
					schema, err = sc.GetSchemaDesc(ctx, p.Txn(), dbDesc.ID, string(table.SchemaName), p.CommonLookupFlags(true))
					if err != nil {
						return partParentID, schema, err
					}
					partParentID = GetParentIDFromUncachedSchema(ctx, p.Txn(), sc, dbDesc.ID, string(table.SchemaName), p.CommonLookupFlags(true))
					return partParentID, schema, nil
				}
			}
			for _, searchPath = range searchPaths {
				schema, err = sc.GetSchemaDesc(ctx, p.Txn(), dbDesc.ID, searchPath, p.CommonLookupFlags(true))
				if err != nil {
					return partParentID, schema, err
				}
				partParentID = GetParentIDFromUncachedSchema(ctx, p.Txn(), sc, dbDesc.ID, searchPath, p.CommonLookupFlags(true))
				if partParentID != dbDesc.ID {
					return partParentID, schema, nil
				}
			}
			return partParentID, schema, nil
		}

		if transform != "" {
			// If we're not ingesting the data, we don't care what DB we pick.
			// parentID = defaultCSVParentID
			parentID = defaultCSVTableID + 2
			found, schema, err := table.ResolveTarget(ctx,
				p, p.SessionData().Database, p.SessionData().SearchPath)
			print(found, schema)
			if err != nil {
				return err
			}
		} else if table != nil {
			// We have a target table, so it might specify a DB in its name.
			found, descI, err := table.ResolveTarget(ctx,
				p, p.SessionData().Database, p.SessionData().SearchPath)

			if err != nil {
				return errors.Wrap(err, "resolving target import name")
			}
			if !found {
				// Check if database exists right now. It might not after the import is done,
				// but it's better to fail fast than wait until restore.
				return errors.Errorf("Unable to resolve the current import target, please check whether the target schema or database exists: %q", table)
			}
			parentID, schema, err = findSchema(descI.(*sqlbase.DatabaseDescriptor), table)
			if err != nil {
				return err
			}

			rows, err := p.ExecCfg().InternalExecutor.Query(
				ctx,
				"SHOW SNAPSHOT",
				p.Txn(),
				`SELECT id,name,type,object_id,asof FROM system.snapshots WHERE type=$1 AND object_id = $2`,
				tree.DatabaseSnapshotType,
				descI.(*sqlbase.DatabaseDescriptor).ID,
			)
			if err != nil {
				return err
			}
			if len(rows) > 0 {
				return errors.New("cannot load table that has snapshots")
			}
			// If this is a non-INTO import that will thus be making a new table, we
			// need the CREATE priv in the target DB.
			//TODO In ZNBase, here should be to check whether the schema has create permission
			if !importStmt.Into {
				if err := p.CheckPrivilege(ctx, schema, privilege.USAGE); err != nil && table != nil {
					return err
				}
				if err := p.CheckPrivilege(ctx, schema, privilege.CREATE); err != nil {
					return err
				}
			}
			if parentID == descI.(*sqlbase.DatabaseDescriptor).ID {
				return errors.Errorf("schema %q does not exist", searchPaths)
			}
		} else if importStmt.Database != "" {
			//进行整库导入的权限检查
			err = p.RequireAdminRole(ctx, "LOAD DATABASE")
			if err != nil {
				return pgerror.Wrap(err, pgcode.InvalidRoleSpecification, "the current user does not have this permission")
			}
			parentID = keys.SystemDatabaseID
		} else if importStmt.SchemaName != nil {
			dbName := p.SessionData().Database
			if overrideDB, renaming := opts[importOptionsIntoDb]; renaming {
				dbName = overrideDB
			}
			//进行导入模式的权限检查
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, dbName, true)
			if err != nil {
				return err
			}
			err = p.CheckPrivilege(ctx, dbDesc, privilege.USAGE)
			if err != nil {
				return err
			}
			err = p.CheckPrivilege(ctx, dbDesc, privilege.CREATE)
			if err != nil {
				return err
			}
			parentID = dbDesc.ID
		} else {
			//数据库和模式的导入导出权限检查
			// No target table means we're importing whatever we find into the session
			// database, so it must exist.
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, p.SessionData().Database, true /*required*/)
			if err != nil {
				return errors.Wrap(err, "could not resolve current database")
			}
			// If this is a non-INTO import that will thus be making a new table, we
			// need the CREATE priv in the target DB.
			//TODO In ZNBase, here should be to check whether the schema has create permission
			parentID, schema, err = findSchema(dbDesc, table)
			if err != nil {
				return err
			}
			if !importStmt.Into {
				if err := p.CheckPrivilege(ctx, schema, privilege.USAGE); err != nil && table != nil {
					return err
				}
				if err := p.CheckPrivilege(ctx, schema, privilege.CREATE); err != nil && table != nil {
					return err
				}
			}

			if parentID == dbDesc.ID {
				return errors.Errorf("schema %q does not exist", searchPaths)
			}
		}
		format, err := getFormatParas(importStmt, opts, p)
		if err != nil {
			return err
		}
		if _, ok := opts[importOptionAllFiles]; ok {
			conf, err := dumpsink.ConfFromURI(ctx, files[0])
			if err != nil {
				return err
			}
			if (conf.Provider != roachpb.ExportStorageProvider_LocalFile) && (conf.Provider != roachpb.ExportStorageProvider_Http) {
				return errors.Errorf("load table with allfiles only surports LocalFile and Http")
			}
			files, err = getAllFilesInPath(ctx, p, files[0], format)
			if err != nil {
				return err
			}
		}

		if importStmt.Settings {
			switch format.Format {
			case roachpb.IOFileFormat_CSV, roachpb.IOFileFormat_ZNBaseDUMP:
			default:
				return errors.Errorf("unsupported file format:%s while import cluster settings", format.Format)
			}
		}
		if importStmt.Settings {
			if len(files) == 0 {
				return errors.Errorf("files cannot be nil")
			}
			es, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, files[0])
			if err != nil {
				return err
			}
			defer es.Close()
			if es.Conf().Provider == roachpb.ExportStorageProvider_Http && format.HttpHeader != "" {
				err := es.SetConfig(format.HttpHeader)
				if err != nil {
					return err
				}
			}
			f, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			defer f.Close()
			bc := &byteCounter{r: f}
			src, err := decompressingReader(bc, files[0], format.Compression)
			cr := csv.NewReader(src)
			if format.Csv.Comma != 0 {
				cr.Comma = format.Csv.Comma
			}
			for {
				record, err := cr.Read()
				finished := err == io.EOF
				if finished {
					break
				}
				var query string
				if format.Format == roachpb.IOFileFormat_ZNBaseDUMP {
					if len(record) != 1 {
						return errors.Errorf("Wrong file format")
					}
					query = record[0]
				} else {
					if len(record) < 2 {
						return errors.Errorf("Wrong file format")
					}
					if len(opts) != 0 {
						return errors.Errorf("cannot use  options while set cluster settings")
					}
					query = fmt.Sprintf(`SET cluster setting %s = %s`, record[0], tree.NewDString(record[1]))
				}
				_, err = p.ExecCfg().InternalExecutor.Exec(
					ctx, "load_settings", nil,
					query,
				)
				if err != nil {
					return err
				}
			}
			resultsCh <- tree.Datums{
				tree.NewDString("The cluster settings have been set"),
			}
			return nil
		}
		// sstSize, if 0, will be set to an appropriate default by the specific
		// implementation (local or distributed) since each has different optimal
		// settings.
		var sstSize int64
		if override, ok := opts[importOptionSSTSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return err
			}
			sstSize = sz
		}
		var oversample int64
		if override, ok := opts[importOptionOversample]; ok {
			os, err := strconv.ParseInt(override, 10, 64)
			if err != nil {
				return err
			}
			oversample = os
		}

		var skipFKs bool
		if _, ok := opts[importOptionSkipFKs]; ok {
			skipFKs = true
		}

		if ingestDirectly {
			if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionDirectImport) {
				return errors.Errorf("Using %q requires all nodes to be upgraded to %s",
					importOptionDirectIngest, cluster.VersionByKey(cluster.VersionDirectImport))
			}
			if transform != "" {
				return errors.Errorf("cannot use %q and %q options together", importOptionDirectIngest, importOptionTransform)
			}
		}

		var encoding string
		if override, ok := opts[importOptionEncoding]; ok {
			encoding = override
		}

		var tableDetails []jobspb.ImportDetails_Table
		var databaseDetails []jobspb.ImportDetails_Database
		var schemasDetails []*sqlbase.SchemaDescriptor
		var scName string
		if importStmt.SchemaName != nil {
			scName = importStmt.SchemaName.Schema()
		}
		jobDesc, err := importJobDescription(importStmt, nil, files, opts)
		if err != nil {
			return err
		}

		if importStmt.Into {
			// TODO(dt): this is a prototype for incremental import but there are many
			// TODOs remaining before it is ready to graduate to prime-time. Some of
			// them are captured in specific TODOs below, but some of the big, scary
			// things to do are:
			// - review planner vs txn use very carefully. We should try to get to a
			//   single txn used to plan the job and create it. Using the planner's
			//   txn today is very wrong since it will not commit until after the job
			//   has run, so starting a job based on reads it returned is very wrong.
			// - audit every place that we resolve/lease/read table descs to be sure
			//   that the IMPORTING state is handled correctly. SQL lease acquisition
			//   is probably the easy one here since it has single read path -- the
			//   things that read directly like the queues or background jobs are the
			//   ones we'll need to really carefully look though.
			// - Look at if/how cleanup/rollback works. Reconsider the cpu from the
			//   desc version (perhaps we should be re-reading instead?).
			// - Write _a lot_ of tests.
			dbName := p.SessionData().Database
			if table.CatalogName != "" {
				dbName = string(table.CatalogName)
			}
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, dbName, true /*required*/)
			if err != nil {
				return errors.Wrap(err, "could not resolve current database")
			}
			_, schema, err = findSchema(dbDesc, table)
			if err != nil {
				return err
			}
			found, err := p.ResolveTableDescriptor(ctx, table, true)
			if err != nil {
				return err
			}

			// TODO(dt): checking *CREATE* on an *existing table* is weird.
			if err := p.CheckPrivilege(ctx, found, privilege.INSERT); err != nil {
				return err
			}

			// IMPORT INTO does not currently support interleaved tables.
			if found.IsInterleaved() {
				// TODO(miretskiy): Handle import into when tables are interleaved.
				return pgerror.NewError(pgcode.FeatureNotSupported, "Cannot use LOAD INTO with interleaved tables")
			}

			// Validate target columns.
			var intoCols []string
			var isTargetCol = make(map[string]bool)
			for _, name := range importStmt.IntoCols {
				var err error
				if _, err = found.FindActiveColumnByName(name.String()); err != nil {
					return errors.Wrap(err, "verifying target columns")
				}

				isTargetCol[name.String()] = true
				intoCols = append(intoCols, name.String())
			}

			// IMPORT INTO does not support columns with DEFAULT expressions. Ensure
			// that all non-target columns are nullable until we support DEFAULT
			// expressions.
			for _, col := range found.VisibleColumns() {
				//No longer limit the default value,See http://10.10.7.5:8090/pages/viewpage.action?pageId=28283154 for details
				//if col.HasDefault() {
				//	return errors.Errorf("cannot LOAD INTO a table with a DEFAULT expression for any of its columns")
				//}
				if len(isTargetCol) != 0 && !isTargetCol[col.Name] && !col.IsNullable() {
					return errors.Errorf("all non-target columns in LOAD INTO must be nullable")
				}
			}

			tableDetails = []jobspb.ImportDetails_Table{{Desc: &found.TableDescriptor, IsNew: false, TargetCols: intoCols}}
		} else {
			var tableDescs []*sqlbase.TableDescriptor
			var names []string
			seqVals := make(map[sqlbase.ID]int64)
			if importStmt.Bundle {
				store, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, files[0])
				if err != nil {
					return err
				}
				defer store.Close()
				if store.Conf().Provider == roachpb.ExportStorageProvider_Http && format.HttpHeader != "" {
					err := store.SetConfig(format.HttpHeader)
					if err != nil {
						return err
					}
				}
				raw, err := store.ReadFile(ctx, "")
				if err != nil {
					return err
				}
				defer raw.Close()
				reader, err := decompressingReader(raw, files[0], format.Compression)
				if err != nil {
					return err
				}
				defer reader.Close()

				var match string
				if table != nil {
					match = table.TableName.String()
				}
				fks := fkHandler{skip: skipFKs, allowed: true, resolver: make(fkResolver)}
				switch format.Format {
				case roachpb.IOFileFormat_Mysqldump:
					evalCtx := &p.ExtendedEvalContext().EvalContext
					tableDescs, err = readMysqlCreateTable(ctx, reader, evalCtx, defaultCSVTableID, parentID, match, fks, seqVals, &p.ExecCfg().StatusServer, p.User(), p)
				case roachpb.IOFileFormat_PgDump:
					evalCtx := &p.ExtendedEvalContext().EvalContext
					tableDescs, err = readPostgresCreateTable(ctx, reader, evalCtx, p.ExecCfg().Settings, match, parentID, walltime, fks, int(format.PgDump.MaxRowSize), &p.ExecCfg().StatusServer, p.User(), p)
				default:
					return errors.Errorf("non-bundle format %q does not support reading schemas", format.Format.String())
				}
				if err != nil {
					return err
				}
				if tableDescs == nil && table != nil {
					names = []string{table.TableName.String()}
				}

				descStr, err := importJobDescription(importStmt, nil, files, opts)
				if err != nil {
					return err
				}
				jobDesc = descStr
			} else {
				//整库导入CSV
				if database != "" {
					if format.Format != roachpb.IOFileFormat_CSV && format.Format != roachpb.IOFileFormat_ZNBaseDUMP && format.Format != roachpb.IOFileFormat_TXT {
						return errors.Errorf("format %q does not support load database", format.Format.String())
					}
					//获取整库的相关定义
					filename, err := createFileFn()
					if err != nil {
						return err
					}
					evalCtx := &p.ExtendedEvalContext().EvalContext
					fks := fkHandler{skip: skipFKs, allowed: true, resolver: make(fkResolver)}
					encryption, err := getEncryption(ctx, files, opts, p, importStmt.Database, scName)
					if err != nil {
						return err
					}
					parseDDL, err := readCreateDDLFromStore(ctx, filename, p, evalCtx, "", parentID, walltime, fks, defaultScanBuffer, &p.ExecCfg().StatusServer, encryption, format.HttpHeader)
					if err != nil {
						return err
					}
					tableDescs = parseDDL.tables
					databaseDescs := parseDDL.databases
					if len(databaseDescs) == 0 {
						return errors.Errorf("unable to parse database %q", database)
					}
					if databaseDescs[0].Name != database {
						return errors.Errorf("expect dbName %q get %q", databaseDescs[0].Name, database)
					}
					//当前仅支持单库的解析操作
					schema := parseDDL.schemas
					databaseDetails = append(databaseDetails, jobspb.ImportDetails_Database{Database: databaseDescs[0], Schemas: schema})
					//TODO 后续修改对应的job描述信息
					descStr, err := importJobDescription(importStmt, nil, files, opts)
					if err != nil {
						return err
					}
					jobDesc = descStr
				} else if importStmt.SchemaName != nil {
					dbName := p.SessionData().Database
					if overrideDB, renaming := opts[importOptionsIntoDb]; renaming {
						dbName = overrideDB
					}
					if format.Format != roachpb.IOFileFormat_CSV && format.Format != roachpb.IOFileFormat_ZNBaseDUMP && format.Format != roachpb.IOFileFormat_TXT {
						return errors.Errorf("format %q does not support load database", format.Format.String())
					}
					filename, err := createFileFn()
					if err != nil {
						return err
					}
					evalCtx := &p.ExtendedEvalContext().EvalContext
					fks := fkHandler{skip: skipFKs, allowed: true, resolver: make(fkResolver)}
					encryption, err := getEncryption(ctx, files, opts, p, importStmt.Database, scName)
					if err != nil {
						return err
					}
					parseDDL, err := readCreateDDLFromSchema(ctx, filename, p, evalCtx, "", parentID, walltime, fks, defaultScanBuffer, &p.ExecCfg().StatusServer, encryption, dbName, format.HttpHeader)
					if err != nil {
						return err
					}
					tableDescs = parseDDL.tables
					databaseDescs := parseDDL.databases
					if len(databaseDescs) == 0 {
						return errors.Errorf("unable to parse database %q", database)
					}
					//当前仅支持单库的解析操作
					schemasDetails = parseDDL.schemas
					//TODO 后续修改对应的job描述信息
					descStr, err := importJobDescription(importStmt, nil, files, opts)
					if err != nil {
						return err
					}
					jobDesc = descStr
				} else {
					if table == nil {
						return errors.Errorf("non-bundle format %q should always have a table name", importStmt.FileFormat)
					}
					var create *tree.CreateTable
					if importStmt.CreateDefs != nil {
						create = &tree.CreateTable{
							Table:           *importStmt.Table,
							Defs:            importStmt.CreateDefs,
							PartitionBy:     importStmt.PartitionBy,
							LocateSpaceName: importStmt.LocateSpaceName,
						}
					} else {
						filename, err := createFileFn()
						if err != nil {
							return err
						}
						create, err = readCreateTableFromStore(ctx, filename, p.ExecCfg().DistSQLSrv.DumpSinkFromURI, format.HttpHeader)
						if err != nil {
							return err
						}

						if table.TableName != create.Table.TableName {
							return errors.Errorf(
								"importing table %s, but file specifies a schema for table %s",
								table.TableName, create.Table.TableName,
							)
						}
					}
					tbl, err := MakeSimpleTableDescriptor(
						ctx, p.ExecCfg().Settings, create, parentID, defaultCSVTableID, NoFKs, walltime, &p.ExecCfg().StatusServer, p.User())
					if err != nil {
						return err
					}
					tableDescs = []*sqlbase.TableDescriptor{tbl.TableDesc()}
					descStr, err := importJobDescription(importStmt, create.Defs, files, opts)
					if err != nil {
						return err
					}
					jobDesc = descStr
				}
			}
			if transform != "" {
				transformStorage, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, transform)

				if err != nil {
					return err
				}
				if transformStorage.Conf().Provider == roachpb.ExportStorageProvider_Http && format.HttpHeader != "" {
					err := transformStorage.SetConfig(format.HttpHeader)
					if err != nil {
						return err
					}
				}
				// Delay writing the BACKUP-CHECKPOINT file until as late as possible.
				err = dump.VerifyUsableExportTarget(ctx, transformStorage, transform, nil)
				transformStorage.Close()
				if err != nil {
					return err
				}
				telemetry.Count("import.transform")
			} else {
				if database != "" || importStmt.SchemaName != nil {
					for _, tableDesc := range tableDescs {
						if err := dump.CheckTableExists(ctx, p.Txn(), tableDesc.ParentID, tableDesc.Name); err != nil {
							return err
						}
					}
					// Verification steps have passed, generate a new table ID if we're
					// restoring. We do this last because we want to avoid calling
					// GenerateUniqueDescID if there's any kind of error above.
					// ReservidoDistributedCSVTransformng a table ID now means we can avoid the rekey work during restore.
					tableRewrites := make(dump.TableRewriteMap)
					newSeqVals := make(map[sqlbase.ID]int64, len(seqVals))
					for _, tableDesc := range tableDescs {
						id, err := sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
						if err != nil {
							return err
						}
						tableRewrites[tableDesc.ID] = &jobspb.RestoreDetails_TableRewrite{
							TableID:  id,
							ParentID: tableDesc.GetParentID(),
						}
						newSeqVals[id] = seqVals[tableDesc.ID]
					}
					seqVals = newSeqVals
					if importStmt.SchemaName != nil {
						if err := dump.RewriteSchemaDescs(schemasDetails, tableRewrites, ""); err != nil {
							return err
						}
					}
					if err := dump.RewriteTableDescs(ctx, p.Txn(), tableDescs, tableRewrites, "", false); err != nil {
						return err
					}
				} else {
					for _, tableDesc := range tableDescs {
						if err := dump.CheckTableExists(ctx, p.Txn(), parentID, tableDesc.Name); err != nil {
							return err
						}
					}
					// Verification steps have passed, generate a new table ID if we're
					// restoring. We do this last because we want to avoid calling
					// GenerateUniqueDescID if there's any kind of error above.
					// ReservidoDistributedCSVTransformng a table ID now means we can avoid the rekey work during restore.
					tableRewrites := make(dump.TableRewriteMap)
					newSeqVals := make(map[sqlbase.ID]int64, len(seqVals))
					for _, tableDesc := range tableDescs {
						id, err := sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
						if err != nil {
							return err
						}
						tableRewrites[tableDesc.ID] = &jobspb.RestoreDetails_TableRewrite{
							TableID:  id,
							ParentID: parentID,
						}
						newSeqVals[id] = seqVals[tableDesc.ID]
					}
					seqVals = newSeqVals
					if err := dump.RewriteTableDescs(ctx, p.Txn(), tableDescs, tableRewrites, "", false); err != nil {
						return err
					}
				}
			}

			tableDetails = make([]jobspb.ImportDetails_Table, 0, len(tableDescs))
			for _, tbl := range tableDescs {
				tableDetails = append(tableDetails, jobspb.ImportDetails_Table{Desc: tbl, IsNew: true, SeqVal: seqVals[tbl.ID]})
			}
			for _, name := range names {
				tableDetails = append(tableDetails, jobspb.ImportDetails_Table{Name: name, IsNew: true})
			}
		}

		telemetry.CountBucketed("import.files", int64(len(files)))
		encryption, err := getEncryption(ctx, files, opts, p, importStmt.Database, scName)
		if err != nil {
			return err
		}
		var spanPros []*jobspb.ImportProgress_SpanProgress
		for _, tbl := range tableDetails {
			spanProgress := &jobspb.ImportProgress_SpanProgress{Table: *(tbl.Desc)}
			spanPros = append(spanPros, spanProgress)
		}
		_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
			Description: jobDesc,
			Username:    p.User(),
			Details: jobspb.ImportDetails{
				URIs:           files,
				Format:         format,
				ParentID:       parentID,
				Tables:         tableDetails,
				BackupPath:     transform,
				SSTSize:        sstSize,
				Oversample:     oversample,
				Walltime:       walltime,
				SkipFKs:        skipFKs,
				IngestDirectly: ingestDirectly,
				Encryption:     encryption,
				Databases:      databaseDetails,
				Schemas:        schemasDetails,
				Encoding:       encoding,
			},
			Progress: jobspb.ImportProgress{SpanProgress: spanPros},
		})
		if err != nil {
			return err
		}
		return <-errCh
	}
	log.Info(ctx, "Start Load Job")
	//根据是否支持save_rejected功能，回显给用户不同的信息
	if importStmt.Options.FindByName(importOptionSaveRejected) {
		return fn, LoadHeader, nil, false, nil
	}
	if importStmt.Settings {
		return fn, clusterHeader, nil, false, nil
	}
	return fn, dump.RestoreHeader, nil, false, nil
}

//GetParentIDFromUncachedSchema get table parent ID from the store.
func GetParentIDFromUncachedSchema(
	ctx context.Context,
	txn *client.Txn,
	sc sql.SchemaAccessor,
	dbID sqlbase.ID,
	scName string,
	flags sql.DatabaseLookupFlags,
) sqlbase.ID {
	schema, err := sc.GetSchemaDesc(ctx, txn, dbID, scName, flags)
	if err != nil {
		return dbID
	}
	return schema.ID
}

// Dir returns the path before the file
func Dir(path string) string {
	vol := filepath.VolumeName(path)
	i := len(path) - 1
	for i >= len(vol) && !os.IsPathSeparator(path[i]) {
		i--
	}
	dir := path[len(vol) : i+1]
	if dir == "." && len(vol) > 2 {
		// must be UNC
		return vol
	}
	return vol + dir
}

func getEncryption(
	ctx context.Context,
	files []string,
	opts map[string]string,
	p sql.PlanHookState,
	Database string,
	scName string,
) (encryption *roachpb.FileEncryptionOptions, err error) {
	if passphrase, ok := opts[dumpOptEncPassphrase]; ok {
		if len(files) < 1 {
			return nil, errors.New("invalid base backup specified")
		}
		var infoFile string
		if Database != "" || scName != "" {
			infoFile = files[0]
		} else {
			infoFile = Dir(files[0])
		}
		store, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, infoFile)
		if err != nil {
			return nil, errors.Wrapf(err, "make storage")
		}
		defer store.Close()
		var header string
		if override, ok := opts[importOptionHTTPHeader]; ok {
			header = override
		}
		if store.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
			err := store.SetConfig(header)
			if err != nil {
				return nil, err
			}
		}
		opts, err := dump.ReadEncryptionOptions(ctx, store)
		if err != nil {
			return nil, err
		}
		encryptionKey := storageicl.GenerateKey([]byte(passphrase), opts.Salt)
		encryption = &roachpb.FileEncryptionOptions{Key: encryptionKey}
	}
	return encryption, nil
}
func doDistributedCSVTransform(
	ctx context.Context,
	job *jobs.Job,
	files []string,
	p sql.PlanHookState,
	parentID sqlbase.ID,
	tables map[string]*sqlbase.TableDescriptor,
	transformOnly string,
	format roachpb.IOFileFormat,
	walltime int64,
	sstSize int64,
	oversample int64,
	ingestDirectly bool,
	details jobspb.ImportDetails,
) (roachpb.BulkOpSummary, error) {
	//TODO 在此处进行文件的逻辑拆分工作
	// Attempt to fetch total number of bytes for all files.
	var tableRegions []*encoding.TableRegion
	var jobID int64
	if job != nil {
		jobID = *job.ID()
	}
	tableRegion, err := MakeTableRegions(ctx, &details, p, files, jobID)
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	tableRegions = append(tableRegions, tableRegion...)

	if ingestDirectly {
		return sql.DistIngest(ctx, p, job, tables, tableRegions, format, walltime)
		// TODO(dt): check for errors in job records as is done below.
	}

	evalCtx := p.ExtendedEvalContext()

	ci := sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_STRING},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
		{SemanticType: sqlbase.ColumnType_BYTES},
	})
	rows := rowcontainer.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	if err := sql.LoadCSV(
		ctx,
		p,
		job,
		sql.NewRowResultWriter(rows),
		tables,
		tableRegions,
		transformOnly,
		format,
		walltime,
		sstSize,
		oversample,
		func(descs map[sqlbase.ID]*sqlbase.TableDescriptor) (sql.KeyRewriter, error) {
			return storageicl.MakeKeyRewriter(descs)
		},
	); err != nil {
		// Check if this was a context canceled error and restart if it was.
		if s, ok := status.FromError(errors.Cause(err)); ok {
			if s.Code() == codes.Canceled && s.Message() == context.Canceled.Error() {
				return roachpb.BulkOpSummary{}, jobs.NewRetryJobError("node failure")
			}
		}
		// If the job was canceled, any of the distsql processors could have been
		// the first to encounter the .Progress error. This error's string is sent
		// through distsql back here, so we can't examine the err type in this case
		// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
		// job progress to coerce out the correct error type. If the update succeeds
		// then return the original error, otherwise return this error instead so
		// it can be cleaned up at a higher level.
		if err := job.FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			d := details.(*jobspb.Progress_Import).Import
			return d.Completed()
		}); err != nil {
			return roachpb.BulkOpSummary{}, err
		}
		return roachpb.BulkOpSummary{}, err
	}

	backupDesc := dump.DumpDescriptor{
		EndTime: hlc.Timestamp{WallTime: walltime},
	}
	n := rows.Len()
	for i := 0; i < n; i++ {
		row := rows.At(i)
		name := row[0].(*tree.DString)
		var counts roachpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[1].(*tree.DBytes)), &counts); err != nil {
			return roachpb.BulkOpSummary{}, err
		}
		backupDesc.EntryCounts.Add(counts)
		checksum := row[2].(*tree.DBytes)
		spanStart := row[3].(*tree.DBytes)
		spanEnd := row[4].(*tree.DBytes)
		backupDesc.Files = append(backupDesc.Files, dump.DumpDescriptor_File{
			Path: string(*name),
			Span: roachpb.Span{
				Key:    roachpb.Key(*spanStart),
				EndKey: roachpb.Key(*spanEnd),
			},
			Sha512: []byte(*checksum),
		})
	}

	if transformOnly == "" {
		return backupDesc.EntryCounts, nil
	}

	// The returned spans are from the SSTs themselves, and so don't perfectly
	// overlap. Sort the files so we can fix the spans to be correctly
	// overlapping. This is needed because RESTORE splits at both the start
	// and end of each SST, and so there are tiny ranges (like {NULL-/0/0} at
	// the start) that get created. During non-transform IMPORT this isn't a
	// problem because it only splits on the end key. Replicate that behavior
	// here by copying the end key from each span to the start key of the next.
	sort.Slice(backupDesc.Files, func(i, j int) bool {
		return backupDesc.Files[i].Span.Key.Compare(backupDesc.Files[j].Span.Key) < 0
	})

	var minTableSpan, maxTableSpan roachpb.Key
	for _, tableDesc := range tables {
		span := tableDesc.TableSpan()
		if minTableSpan == nil || span.Key.Compare(minTableSpan) < 0 {
			minTableSpan = span.Key
		}
		if maxTableSpan == nil || span.EndKey.Compare(maxTableSpan) > 0 {
			maxTableSpan = span.EndKey
		}
	}
	backupDesc.Files[0].Span.Key = minTableSpan
	for i := 1; i < len(backupDesc.Files); i++ {
		backupDesc.Files[i].Span.Key = backupDesc.Files[i-1].Span.EndKey
	}
	backupDesc.Files[len(backupDesc.Files)-1].Span.EndKey = maxTableSpan

	dest, err := dumpsink.ConfFromURI(ctx, transformOnly)
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	es, err := p.ExecCfg().DistSQLSrv.DumpSink(ctx, dest)
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	defer es.Close()

	return backupDesc.EntryCounts, finalizeCSVBackup(ctx, &backupDesc, parentID, tables, es, p.ExecCfg())
}

type importResumer struct {
	settings        *cluster.Settings
	res             roachpb.BulkOpSummary
	statsRefresher  *stats.Refresher
	dumpSinkFromURI dumpsink.FromURIFactory
	dumpSink        dumpsink.Factory
}

func (r *importResumer) Resume(
	ctx context.Context, job *jobs.Job, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := job.Details().(jobspb.ImportDetails)
	p := phs.(sql.PlanHookState)
	r.dumpSinkFromURI = p.ExecCfg().DistSQLSrv.DumpSinkFromURI
	r.dumpSink = p.ExecCfg().DistSQLSrv.DumpSink
	onlySchema := details.Format.OnlySchema
	log.Info(ctx, "Load job resume start")
	// TODO(dt): consider looking at the legacy fields used in 2.0.
	if onlySchema {
		r.res = roachpb.BulkOpSummary{}
		r.statsRefresher = p.ExecCfg().StatsRefresher
		return nil
	}
	walltime := details.Walltime
	transform := details.BackupPath
	files := details.URIs
	parentID := details.ParentID
	sstSize := details.SSTSize
	format := details.Format
	oversample := details.Oversample
	ingestDirectly := details.IngestDirectly
	if sstSize == 0 {
		// The distributed importer will correctly chunk up large ranges into
		// multiple ssts that can be imported. In order to reduce the number of
		// ranges and increase the average range size after import, set a target of
		// some arbitrary multiple larger than the maximum sst size. Without this
		// the range sizes were somewhere between 1MB and > 64MB. Targeting a much
		// higher size should cause many ranges to be somewhere around the max range
		// size. This should also cause the distsql plan and range router to be much
		// smaller since there are fewer overall ranges.
		sstSize = storageicl.MaxImportBatchSize(r.settings) * 5
	}

	tables := make(map[string]*sqlbase.TableDescriptor, len(details.Tables))
	tableDBs := make(map[sqlbase.ID]*sqlbase.TableDescriptor, len(details.Tables))
	if details.Tables != nil {
		for _, i := range details.Tables {
			if i.Name != "" {
				tables[i.Name] = i.Desc
				tableDBs[i.Desc.ID] = i.Desc
			} else if i.Desc != nil {
				tables[i.Desc.Name] = i.Desc
				tableDBs[i.Desc.ID] = i.Desc
			} else {
				return errors.Errorf("invalid table specification")
			}
		}
	}

	{
		// Disable merging for the table IDs being imported into. We don't want the
		// merge queue undoing the splits performed during IMPORT.
		tableIDs := make([]uint32, 0, len(tableDBs))
		for _, t := range tableDBs {
			tableIDs = append(tableIDs, uint32(t.ID))
		}
		disableCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		gossipicl.DisableMerges(disableCtx, p.ExecCfg().Gossip, tableIDs)
	}

	if metrics, ok := p.ExecCfg().JobRegistry.MetricsStruct().Load.(*Metrics); ok {
		metrics.LoadStart()
	}
	//尝试向进行整库的导入操作
	if details.Databases != nil || details.Schemas != nil {
		if format.Format == roachpb.IOFileFormat_CSV || format.Format == roachpb.IOFileFormat_TXT || format.Format == roachpb.IOFileFormat_ZNBaseDUMP {
			var rets roachpb.BulkOpSummary
			for _, table := range tableDBs {
				tableDB := make(map[string]*sqlbase.TableDescriptor, 1)
				tableDB[table.Name] = table
				//组件每个表的数据文件所在位置,当前整库导入的时候暂时先默认支持一个文件目录
				if len(files) > 1 || len(files) == 0 {
					return errors.Errorf("Database import does not support multiple file paths temporarily，%q", files)
				}
				//此处读取代表当前表数据的文件夹中的所有文件
				dirbase := files[0]
				//不可以这么拼接
				//此处不使用filepath是因为会导致http路径出错
				var fileName string
				if details.Schemas != nil {
					fileName = strings.Join([]string{dirbase, details.Schemas[0].Name + "." + table.Name}, string(os.PathSeparator))
				}
				if details.Databases != nil {
					schemaName := "public"
					for _, schema := range details.Databases[0].Schemas {
						if table.ParentID == schema.ID {
							schemaName = schema.Name
						}
					}
					fileName = strings.Join([]string{dirbase, schemaName + "." + table.Name}, string(os.PathSeparator))
				}
				tableFiles, error := getAllFilesInPath(ctx, p, fileName, format)
				if error != nil {
					return error
				}
				ret, err := doDistributedCSVTransform(
					ctx, job, tableFiles, p, table.ParentID, tableDB, transform, format, walltime, sstSize, oversample, ingestDirectly, details,
				)
				if err != nil {
					return err
				}
				{
					// Update job details with the sampled keys, as well as any parsed tables,
					// clear SamplingProgress and prep second stage job details for progress
					// tracking.
					if err := job.FractionDetailProgressed(ctx,
						func(ctx context.Context, details jobspb.Details, progress jobspb.ProgressDetails) float32 {
							prog := progress.(*jobspb.Progress_Import).Import
							d := details.(*jobspb.Payload_Import).Import
							d.Samples = nil
							return prog.Completed()
						},
					); err != nil {
						return err
					}
				}
				atomic.AddInt64(&rets.Rows, ret.Rows)
				atomic.AddInt64(&rets.DataSize, ret.DataSize)
				atomic.AddInt64(&rets.IndexEntries, ret.IndexEntries)
				atomic.AddInt64(&rets.SystemRecords, ret.SystemRecords)
			}
			r.res = rets
			r.statsRefresher = p.ExecCfg().StatsRefresher
			if metrics, ok := p.ExecCfg().JobRegistry.MetricsStruct().Load.(*Metrics); ok {
				metrics.LoadStop()
			}
			return nil
		}
	}
	res, err := doDistributedCSVTransform(
		ctx, job, files, p, parentID, tables, transform, format, walltime, sstSize, oversample, ingestDirectly, details,
	)
	if err != nil {
		return err
	}
	r.res = res
	r.statsRefresher = p.ExecCfg().StatsRefresher
	if metrics, ok := p.ExecCfg().JobRegistry.MetricsStruct().Load.(*Metrics); ok {
		metrics.LoadStop()
	}
	if &r.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "finish resume load job :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, recordStatus, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "finish resume load job :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusRunning, r.res.Rows, r.res.IndexEntries, r.res.SystemRecords, r.res.DataSize)
	} else {
		log.Error(ctx, "r.res or job is a nil Pointer")
	}

	return nil
}

// OnFailOrCancel removes KV data that has been committed from a import that
// has failed or been canceled. It does this by adding the table descriptors
// in DROP state, which causes the schema change stuff to delete the keys
// in the background.
func (r *importResumer) OnFailOrCancel(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	if &r.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "load job is onFailOrCancel :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, recordStatus, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "load job is onFailOrCancel :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusFailed, r.res.Rows, r.res.IndexEntries, r.res.SystemRecords, r.res.DataSize)
	} else {
		log.Error(ctx, "r.res or job is a nil Pointer")
	}

	details := job.Details().(jobspb.ImportDetails)
	if details.BackupPath != "" {
		return nil
	}
	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}
	// If the prepare step of the import job was not completed then the
	// descriptors do not need to be rolled back as the txn updating them never
	// completed.
	if !details.PrepareComplete {
		return nil
	}

	var revert []*sqlbase.TableDescriptor
	for _, tbl := range details.Tables {
		if !tbl.IsNew {
			// Take the table offline for import.
			// TODO(dt): audit everywhere we get table descs (leases or otherwise) to
			// ensure that filtering by state handles IMPORTING correctly.
			tbl.Desc.State = sqlbase.TableDescriptor_OFFLINE
			revert = append(revert, tbl.Desc)
		}
	}

	// NB: if a revert fails it will abort the rest of this failure txn, which is
	// also what brings tables back online. We _could_ change the error handling
	// or just move the revert into Resume()'s error return path, however it isn't
	// clear that just bringing a table back online with partially imported data
	// that may or may not be partially reverted is actually a good idea. It seems
	// better to do the revert here so that the table comes back if and only if,
	// it was rolled back to its pre-IMPORT state, and instead provide a manual
	// admin knob (e.g. ALTER TABLE REVERT TO SYSTEM TIME) if anything goes wrong.
	//TODO For real-time streaming import, there is no need to roll back the load into data
	if len(revert) > 0 && details.Format.Format != roachpb.IOFileFormat_Kafka {
		// Sanity check Walltime so it doesn't become a TRUNCATE if there's a bug.
		if details.Walltime == 0 {
			return errors.Errorf("invalid pre-IMPORT time to rollback")
		}
		ts := hlc.Timestamp{WallTime: details.Walltime}.Prev()
		revertTableDefaultBatchSize := storageicl.LoadIntoRevertBatchSize(r.settings)
		if err := sql.RevertTables(ctx, txn.DB(), revert, ts, revertTableDefaultBatchSize); err != nil {
			return errors.Wrap(err, "rolling back partially completed IMPORT")
		}
	}
	b := txn.NewBatch()
	for _, tbl := range details.Tables {
		tableDesc := *tbl.Desc
		tableDesc.Version++
		if tbl.IsNew {
			tableDesc.State = sqlbase.TableDescriptor_DROP
			// If the DropTime if set, a table uses RangeClear for fast data removal. This
			// operation starts at DropTime + the GC TTL. If we used now() here, it would
			// not clean up data until the TTL from the time of the error. Instead, use 1
			// (that is, 1ns past the epoch) to allow this to be cleaned up as soon as
			// possible. This is safe since the table data was never visible to users,
			// and so we don't need to preserve MVCC semantics.
			tableDesc.DropTime = 1
			b.CPut(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(&tableDesc), nil)

		} else {
			// IMPORT did not create this table, so we should not drop it.
			tableDesc.State = sqlbase.TableDescriptor_PUBLIC
		}
	}
	err := errors.Wrap(txn.Run(ctx, b), "rolling back tables")
	if err != nil {
		return err
	}
	//For real-time stream import, the statistics update of the table needs to be triggered
	if details.Format.Format == roachpb.IOFileFormat_Kafka {
		// Initiate a run of CREATE STATISTICS. We don't know the actual number of
		// rows affected per table, so we use a large number because we want to make
		// sure that stats always get created/refreshed here.
		for i := range details.Tables {
			r.statsRefresher.NotifyMutation(details.Tables[i].Desc.ID, math.MaxInt32 /* rowsAffected */)
		}
	}
	return nil
}

func (r *importResumer) OnSuccess(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	log.Event(ctx, "making tables live")
	if &r.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "load job success :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, recordStatus, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "load job success :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusSucceeded, r.res.Rows, r.res.IndexEntries, r.res.SystemRecords, r.res.DataSize)
	} else {
		log.Error(ctx, "r.res or job is a nil Pointer")
	}

	details := job.Details().(jobspb.ImportDetails)

	//目前默认单库
	databasesDetail := details.Databases
	var database []*sqlbase.DatabaseDescriptor
	var schemas []*sqlbase.SchemaDescriptor
	if len(databasesDetail) > 0 {
		database = append(database, databasesDetail[0].Database)
		schemas = databasesDetail[0].Schemas
	}
	if len(details.Schemas) > 0 {
		schemas = details.Schemas
	}
	if details.BackupPath != "" {
		return nil
	}

	var toWrite []*sqlbase.TableDescriptor //make([]*sqlbase.TableDescriptor, len(details.Tables))
	var seqs []roachpb.KeyValue
	for i, tbl := range details.Tables {
		if !tbl.IsNew {
			//禁用外键及其他约束检查
		} else {
			newTnl := details.Tables[i].Desc
			//判断当前是否为
			if len(database) == 0 && len(schemas) == 0 {
				newTnl.ParentID = details.ParentID
			}
			toWrite = append(toWrite, newTnl)
			if d := details.Tables[i]; d.SeqVal != 0 {
				key, val, err := sql.MakeSequenceKeyVal(d.Desc, d.SeqVal, false)
				if err != nil {
					return err
				}
				kv := roachpb.KeyValue{Key: key}
				kv.Value.SetInt(val)
				seqs = append(seqs, kv)
			}
		}
	}
	if len(toWrite) > 0 {
		// Write the new TableDescriptors and flip the namespace entries over to
		// them. After this call, any queries on a table will be served by the newly
		// imported data.
		if err := dump.WriteTableDescs(ctx, txn, database, schemas, toWrite, job.Payload().Username, r.settings, seqs, tree.RequestedDescriptors); err != nil {
			return errors.Wrapf(err, "creating tables")
		}

	}
	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range details.Tables {
		r.statsRefresher.NotifyMutation(details.Tables[i].Desc.ID, math.MaxInt32 /* rowsAffected */)
	}
	return nil
}

func (r *importResumer) OnTerminal(
	ctx context.Context, job *jobs.Job, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	if &r.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "load job is onTerminal :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, recordStatus, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "load job is onTerminal :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusSucceeded, r.res.Rows, r.res.IndexEntries, r.res.SystemRecords, r.res.DataSize)
	} else {
		log.Error(ctx, "r.res or job is a nil Pointer")
	}

	details := job.Details().(jobspb.ImportDetails)

	if transform := details.BackupPath; transform != "" {
		transformStorage, err := r.dumpSinkFromURI(ctx, transform)
		if err != nil {
			log.Warningf(ctx, "unable to create storage: %+v", err)
		} else {
			// Always attempt to cleanup the checkpoint even if the import failed.
			if err := transformStorage.Delete(ctx, dump.DumpDescriptorCheckpointName); err != nil {
				log.Warningf(ctx, "unable to delete checkpointed backup descriptor: %+v", err)
			}
			transformStorage.Close()
		}
	}

	if status == jobs.StatusSucceeded {
		telemetry.CountBucketed("import.rows", r.res.Rows)
		const mb = 1 << 20
		telemetry.CountBucketed("import.size-mb", r.res.DataSize/mb)
		rejectedRows := int64(0)
		if details.IngestDirectly {
			rejectedRows = details.Format.RejectedRows
		} else {
			rejectedRows = details.Format.RejectedRows / 2
		}
		//添加行转失败不终止JOB的功能,当开启的时候回显给用户的显示不同
		if details.Format.SaveRejected {
			resultsCh <- tree.Datums{
				tree.NewDInt(tree.DInt(*job.ID())),
				tree.NewDString(string(jobs.StatusSucceeded)),
				tree.NewDFloat(tree.DFloat(1.0)),
				tree.NewDInt(tree.DInt(r.res.Rows)),
				tree.NewDInt(tree.DInt(r.res.IndexEntries)),
				tree.NewDInt(tree.DInt(r.res.SystemRecords)),
				tree.NewDInt(tree.DInt(r.res.DataSize)),
				tree.NewDInt(tree.DInt(rejectedRows)),
				tree.NewDString(string(details.Format.RejectedAddress)),
			}
		} else {
			resultsCh <- tree.Datums{
				tree.NewDInt(tree.DInt(*job.ID())),
				tree.NewDString(string(jobs.StatusSucceeded)),
				tree.NewDFloat(tree.DFloat(1.0)),
				tree.NewDInt(tree.DInt(r.res.Rows)),
				tree.NewDInt(tree.DInt(r.res.IndexEntries)),
				tree.NewDInt(tree.DInt(r.res.SystemRecords)),
				tree.NewDInt(tree.DInt(r.res.DataSize)),
			}
		}
	}
}

// SupportSeek Support specified offset reading
func SupportSeek(conf roachpb.DumpSink) bool {
	return conf.HttpPath != roachpb.DumpSink_Http{} || conf.LocalFile != roachpb.DumpSink_LocalFilePath{}

}

var _ jobs.Resumer = &importResumer{}

func importResumeHook(typ jobspb.Type, settings *cluster.Settings) jobs.Resumer {
	if typ != jobspb.TypeImport {
		return nil
	}

	return &importResumer{
		settings: settings,
	}
}

func init() {
	sql.AddPlanHook(importPlanHook)
	jobs.AddResumeHook(importResumeHook)
}
