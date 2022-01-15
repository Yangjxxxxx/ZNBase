package dump

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/icl/utilicl/intervalicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/encoding/csv"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/interval"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

const (
	dumpOptDelimiter       = "delimiter"
	dumpOptionRowDelimiter = "rowdelimiter"
	dumpOptfileEscape      = "csv_escaped_by"
	dumpOptNullAs          = "nullas"
	dumpOptChunkSize       = "chunk_rows"
	dumpOptEncoding        = "encoding"
	dumpOptFileName        = "filename"
	dumpOptRevisionHistory = "revision_history"
	dumpOptEncPassphrase   = "encryption_passphrase"
	dumpOptExportDDL       = "skip_ddl"
	dumpOptExportOnlyDDL   = "only_ddl"
	dumpOptionCompression  = "compression"
	dumpOptionOnline       = "online"
	// DumpDescriptorName is the file name used for serialized
	// DumpDescriptor protos.
	DumpDescriptorName = "DUMP"
	// DumpDescriptorCheckpointName is the file name used to store the
	// serialized DumpDescriptor proto while the dump is in progress.
	DumpDescriptorCheckpointName = "DUMP-CHECKPOINT"
	// BackupFormatInitialVersion is the first version of dump and its files.
	BackupFormatInitialVersion uint32 = 0
	//DumpFormatDescriptorTrackingVersion is that dump format descriptor tracking version
	DumpFormatDescriptorTrackingVersion uint32 = 1
	dumpOptionHTTPHeader                       = "http_header"
)

var dumpOptExpectValues = map[string]sql.KVStringOptValidate{
	dumpOptChunkSize:       sql.KVStringOptRequireValue,
	dumpOptDelimiter:       sql.KVStringOptRequireValue,
	dumpOptEncoding:        sql.KVStringOptRequireValue,
	dumpOptionRowDelimiter: sql.KVStringOptRequireValue,
	dumpOptfileEscape:      sql.KVStringOptRequireValue,
	dumpOptFileName:        sql.KVStringOptRequireValue,
	dumpOptNullAs:          sql.KVStringOptRequireValue,
	dumpOptRevisionHistory: sql.KVStringOptRequireNoValue,
	dumpOptExportDDL:       sql.KVStringOptRequireNoValue,
	dumpOptEncPassphrase:   sql.KVStringOptRequireValue,
	dumpOptionCompression:  sql.KVStringOptRequireValue,
	dumpOptExportOnlyDDL:   sql.KVStringOptRequireNoValue,
	dumpOptionOnline:       sql.KVStringOptRequireNoValue,
	dumpOptionHTTPHeader:   sql.KVStringOptRequireValue,
}

const dumpChunkSizeDefault = 100000
const dumpFilePatternPart = "%part%"
const dumpFilePatternDefault = dumpFilePatternPart + ".csv"
const dumpFilePatternSQL = dumpFilePatternPart + ".sql"

// CSV Magic value CSV
const CSV = "CSV"

// SST Magic value SST
const SST = "SST"

// SQL Magic value SQL
const SQL = "SQL"

// TXT Magic value TXT
const TXT = "TXT"
const dumpCompressionCodec = "gzip"
const dumpFilePatternTXT = dumpFilePatternPart + ".txt"
const (
	jobID         = "job_id"
	status        = "status"
	rows          = "rows"
	indexEntries  = "index_entries"
	systemRecords = "system_records"
	recordBytes   = "bytes"
)

type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

// ID is part of `interval.Interface` but unused in makeImportSpans.
func (ie intervalSpan) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ie intervalSpan) Range() interval.Range {
	return interval.Range{Start: []byte(ie.Key), End: []byte(ie.EndKey)}
}

// annotatedBackupStatement is a tree.Backup, optionally
// annotated with the scheduling information.
type annotatedBackupStatement struct {
	*tree.Dump
	*jobs.CreatedByInfo
}

type spanAndTime struct {
	span       roachpb.Span
	start, end hlc.Timestamp
}

func init() {
	sql.AddPlanHook(dumpPlanHook)
	jobs.AddResumeHook(dumpResumeHook)
	rowexec.NewCSVWriterProcessor = newCSVWriterProcessor
	//distsqlrun.NewCSVWriterProcessor = newSSTWriterProcessor
}

func getDumpStatement(stmt tree.Statement) *annotatedBackupStatement {
	switch dump := stmt.(type) {
	case *annotatedBackupStatement:
		return dump
	case *tree.Dump:
		return &annotatedBackupStatement{Dump: dump}
	default:
		return nil
	}
}

func dumpPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	//dumpStmt, ok := stmt.(*tree.Dump)
	dumpStmt := getDumpStatement(stmt)
	if dumpStmt == nil {
		return nil, nil, nil, false, nil
	}
	fileFn, err := p.TypeAsString(dumpStmt.File, "DUMP")
	if err != nil {
		return nil, nil, nil, false, err
	}
	if dumpStmt.IsQuery && dumpStmt.FileFormat != CSV && dumpStmt.FileFormat != TXT && dumpStmt.FileFormat != SQL {
		return nil, nil, nil, false, errors.Errorf("unsupported format for dump query: %q", dumpStmt.FileFormat)
	}

	if !dumpStmt.IsQuery && dumpStmt.FileFormat != SST {
		return nil, nil, nil, false, errors.Errorf("unsupported format for dump target: %q", dumpStmt.FileFormat)
	}
	//此处进行导出以及备份的部分权限检查
	if dumpStmt.IsQuery {
		//整库导出检查usage权限
		if dumpStmt.ExpendQuery.DbName != "" {
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(dumpStmt.ExpendQuery.DbName), true /*required*/)
			if err != nil {
				return nil, nil, nil, false, errors.Wrap(err, "could not resolve current database")
			}
			err = p.CheckPrivilege(ctx, dbDesc, privilege.USAGE)
			if err != nil {
				return nil, nil, nil, false, errors.Wrapf(err, "the current user does not have this permission")
			}
		}
		//指定模式导出检查usage权限
		if dumpStmt.ExpendQuery.ScName != nil {
			if dumpStmt.ExpendQuery.ScName.Catalog() == "" {
				dumpStmt.ExpendQuery.ScName.SetCatalog(p.CurrentDatabase())
			}
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, dumpStmt.ExpendQuery.ScName.Catalog(), true /*required*/)
			if err != nil {
				return nil, nil, nil, false, errors.Wrap(err, "could not resolve current database")
			}
			err = p.CheckPrivilege(ctx, dbDesc, privilege.USAGE)
			if err != nil {
				return nil, nil, nil, false, err
			}
			scDesc, err := dbDesc.GetSchemaByName(dumpStmt.ExpendQuery.ScName.Schema())
			if err != nil {
				return nil, nil, nil, false, errors.Wrap(err, "could not resolve current schema")
			}
			err = p.CheckPrivilege(ctx, scDesc, privilege.USAGE)
			if err != nil {
				return nil, nil, nil, false, errors.Wrapf(err, "the current user does not have this permission")
			}
		}
		//指定单表导出检查usage权限
		if dumpStmt.ExpendQuery.Query != nil {
			if dumpStmt.IsSettings {
				err := p.RequireAdminRole(ctx, "EXPORT CLUSTER SETTINGS")
				if err != nil {
					return nil, nil, nil, false, err
				}
			} else {
				table, needCheckPrivilege, err := parseTableDesc(dumpStmt.ExpendQuery.Query.Select, p.CurrentDatabase())
				if err != nil {
					return nil, nil, nil, false, err
				}
				if needCheckPrivilege {
					if len(table) < 1 {
						return nil, nil, nil, false, errors.Errorf("cannot parse tableName")
					}
					dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(table[0].CatalogName), true /*required*/)
					if err != nil {
						return nil, nil, nil, false, errors.Wrap(err, "could not resolve current database")
					}
					err = p.CheckPrivilege(ctx, dbDesc, privilege.USAGE)
					if err != nil {
						return nil, nil, nil, false, err
					}
					if table[0].CatalogName.String() != sqlbase.SystemDB.Name {
						scDesc, err := dbDesc.GetSchemaByName(string(table[0].SchemaName))
						if err != nil {
							return nil, nil, nil, false, errors.Wrap(err, "could not resolve current schema")
						}
						err = p.CheckPrivilege(ctx, scDesc, privilege.USAGE)
						if err != nil {
							return nil, nil, nil, false, err
						}
					}
				}
			}
		}
	}

	var dumpHeader sqlbase.ResultColumns
	//Select语句将生成PlanNode
	var selNodes []sql.PlanNode
	var tables []*tree.TableName
	switch dumpStmt.FileFormat {
	case SQL:
		dumpHeader = sqlbase.ResultColumns{
			{Name: "filename", Typ: types.String},
			{Name: "bytes", Typ: types.Int},
		}
	case SST:
		dumpHeader = sqlbase.ResultColumns{
			{Name: "job_id", Typ: types.Int},
			{Name: "status", Typ: types.String},
			{Name: "fraction_completed", Typ: types.Float},
			{Name: "rows", Typ: types.Int},
			{Name: "index_entries", Typ: types.Int},
			{Name: "system_records", Typ: types.Int},
			{Name: "bytes", Typ: types.Int},
		}
	default:
		if dumpStmt.FileFormat != CSV && dumpStmt.FileFormat != TXT {
			return nil, nil, nil, false, errors.Errorf("unsupported format for dump query: %q", dumpStmt.FileFormat)
		}
		dumpHeader = sqlbase.ResultColumns{
			{Name: "queryname", Typ: types.String},
			{Name: "filename", Typ: types.String},
			{Name: "rows", Typ: types.Int},
			{Name: "bytes", Typ: types.Int},
		}
		switch dumpStmt.ExpendQuery.Type {
		case roachpb.DUMPFileFormat_DATABASE:
			database := dumpStmt.ExpendQuery.DbName
			tables, err = getTableNames(ctx, p.ExecCfg().InternalExecutor, p.Txn(), string(database), "")
			if err != nil {
				return nil, nil, nil, false, err
			}
			//Build planNode for multi-table query
			selNodes, err = multiTablePlanNodes(ctx, tables, p, selNodes)
			if err != nil {
				return nil, nil, nil, false, err
			}
		case roachpb.DUMPFileFormat_SCHEMA:
			tables, err = GetTableNamesFromSchema(ctx, p.ExecCfg().InternalExecutor, p.Txn(), dumpStmt.ExpendQuery.ScName.Catalog(), dumpStmt.ExpendQuery.ScName.Schema(), "")
			if err != nil {
				return nil, nil, nil, false, err
			}
			selNodes, err = multiTablePlanNodes(ctx, tables, p, selNodes)
			if err != nil {
				return nil, nil, nil, false, err
			}
		case roachpb.DUMPFileFormat_TABLE:
			sel, err := p.Select(ctx, dumpStmt.ExpendQuery.Query, nil)
			if err != nil {
				return nil, nil, nil, false, err
			}
			selNodes = append(selNodes, sel)
		}
	}
	if dumpStmt.FileFormat == SQL {
		switch dumpStmt.ExpendQuery.Type {
		case roachpb.DUMPFileFormat_DATABASE:
			database := dumpStmt.ExpendQuery.DbName
			tables, err = getTableNames(ctx, p.ExecCfg().InternalExecutor, p.Txn(), string(database), "")
			if err != nil {
				return nil, nil, nil, false, err
			}
			//Build planNode for multi-table query
			for _, table := range tables {
				tableDesc, err := p.ResolveTableDescriptor(ctx, table, true)
				if err != nil {
					return nil, nil, nil, false, err
				}
				err = p.CheckPrivilege(ctx, tableDesc, privilege.SELECT)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case roachpb.DUMPFileFormat_SCHEMA:
			tables, err = GetTableNamesFromSchema(ctx, p.ExecCfg().InternalExecutor, p.Txn(), dumpStmt.ExpendQuery.ScName.Catalog(), dumpStmt.ExpendQuery.ScName.Schema(), "")
			if err != nil {
				return nil, nil, nil, false, err
			}
			for _, table := range tables {
				tableDesc, err := p.ResolveTableDescriptor(ctx, table, true)
				if err != nil {
					return nil, nil, nil, false, err
				}
				err = p.CheckPrivilege(ctx, tableDesc, privilege.SELECT)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case roachpb.DUMPFileFormat_TABLE:
			table, needCheckPrivilege, err := parseTableDesc(dumpStmt.ExpendQuery.Query.Select, p.CurrentDatabase())
			if err != nil {
				return nil, nil, nil, false, err
			}
			if needCheckPrivilege {
				if len(table) < 1 {
					return nil, nil, nil, false, errors.Errorf("cannot parse tableName")
				}
				tableDesc, err := p.ResolveTableDescriptor(ctx, table[0], true)
				if err != nil {
					return nil, nil, nil, false, err
				}
				err = p.CheckPrivilege(ctx, tableDesc, privilege.SELECT)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		}
	}
	incFromFn, err := p.TypeAsStringArray(dumpStmt.IncrementalFrom, "DUMP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	optsFn, err := p.TypeAsStringOpts(dumpStmt.Options, dumpOptExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, plans []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)
		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("DUMP cannot be used inside a transaction")
		}

		file, err := fileFn()
		if err != nil {
			return err
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		var header string
		if override, ok := opts[dumpOptionHTTPHeader]; ok {
			header = override
		}

		incrementalFrom, err := incFromFn()
		if err != nil {
			return err
		}

		endTime := p.ExecCfg().Clock.Now()
		if dumpStmt.AsOf.Expr != nil {
			var err error
			if endTime, err = p.EvalAsOfTimestamp(dumpStmt.AsOf); err != nil {
				return err
			}
		}

		//csv格式的参数设置
		csvOpts := roachpb.CSVOptions{}
		if dumpStmt.FileFormat == CSV {
			if override, ok := opts[dumpOptDelimiter]; ok {
				csvOpts.Comma, err = util.GetSingleRune(override)
				if err != nil {
					return pgerror.NewError(pgcode.InvalidParameterValue, "invalid delimiter")
				}
			}
			if override, ok := opts[dumpOptionRowDelimiter]; ok {
				csvOpts.Rowma, err = util.GetSingleRune(override)
				if err != nil {
					return pgerror.NewError(pgcode.InvalidParameterValue, "invalid rowdelimiter")
				}
			}
			if override, ok := opts[dumpOptfileEscape]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return errors.Wrapf(err, "invalid %q value", dumpOptfileEscape)
				}
				csvOpts.HasEscape = true
				csvOpts.Escape = c
			}
			if override, ok := opts[dumpOptNullAs]; ok {
				csvOpts.NullEncoding = &override
			}

			if override, ok := opts[dumpOptEncoding]; ok {
				csvOpts.Encoding = override
			}
		}
		txtOpts := roachpb.TXTOptions{}
		if dumpStmt.FileFormat == TXT {
			if override, ok := opts[dumpOptDelimiter]; ok {
				txtOpts.Comma, err = util.GetSingleRune(override)
				if err != nil {
					return pgerror.NewError(pgcode.InvalidParameterValue, "invalid delimiter")
				}
			}

			if override, ok := opts[dumpOptNullAs]; ok {
				txtOpts.NullEncoding = &override
			}

			if override, ok := opts[dumpOptEncoding]; ok {
				txtOpts.Encoding = override
			}
		}
		skipDDL := false
		if _, ok := opts[dumpOptExportDDL]; ok {
			if dumpStmt.ExpendQuery == nil {
				return pgerror.NewError(pgcode.InvalidParameterValue, "the current file type is only supported export")
			}
			if dumpStmt.FileFormat != CSV && dumpStmt.FileFormat != SQL {
				return pgerror.NewError(pgcode.InvalidParameterValue, "the current file type is not supported, please refer to the user manual")
			}
			if dumpStmt.ExpendQuery.Type != roachpb.DUMPFileFormat_DATABASE && dumpStmt.ExpendQuery.Type != roachpb.DUMPFileFormat_SCHEMA {
				return pgerror.NewError(pgcode.InvalidParameterValue, "the current operation only supports database export, please look forward to the subsequent improvements")
			}
			skipDDL = true
		}
		onlyDDL := false
		if _, ok := opts[dumpOptExportOnlyDDL]; ok {
			if dumpStmt.ExpendQuery == nil {
				return pgerror.NewError(pgcode.InvalidParameterValue, "the current file type is only supported export")
			}
			if dumpStmt.FileFormat != CSV && dumpStmt.FileFormat != SQL && dumpStmt.FileFormat != TXT {
				return pgerror.NewError(pgcode.InvalidParameterValue, "the current file type is not supported, please refer to the user manual")
			}
			if dumpStmt.ExpendQuery.Type != roachpb.DUMPFileFormat_DATABASE && dumpStmt.ExpendQuery.Type != roachpb.DUMPFileFormat_SCHEMA {
				return pgerror.NewError(pgcode.InvalidParameterValue, "the current operation only supports database export, please look forward to the subsequent improvements")
			}
			onlyDDL = true
		}

		chunk := dumpChunkSizeDefault
		if override, ok := opts[dumpOptChunkSize]; ok {
			chunk, err = strconv.Atoi(override)
			if err != nil {
				return pgerror.NewError(pgcode.InvalidParameterValue, err.Error())
			}
			if chunk < 1 {
				return pgerror.NewError(pgcode.InvalidParameterValue, "invalid csv chunk size")
			}
		}

		exportStore, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, file)
		if err != nil {
			return err
		}
		defer exportStore.Close()

		if exportStore.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
			err := exportStore.SetConfig(header)
			if err != nil {
				return err
			}
		}

		var encryption *roachpb.FileEncryptionOptions
		var codec roachpb.FileCompression
		if name, ok := opts[dumpOptionCompression]; ok && len(name) != 0 {
			if strings.EqualFold(name, dumpCompressionCodec) {
				codec = roachpb.FileCompression_Gzip
			} else {
				return pgerror.NewError(pgcode.InvalidParameterValue, fmt.Sprintf("unsupported compression codec %s", name))
			}
		}
		if dumpStmt.FileFormat != SST {
			var encryptionPassphrase []byte
			if passphrase, ok := opts[dumpOptEncPassphrase]; ok {
				encryptionPassphrase = []byte(passphrase)
				salt, err := storageicl.GenerateSalt()
				if err != nil {
					return err
				}
				exportStore, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, file)
				if err != nil {
					return err
				}
				defer exportStore.Close()
				if exportStore.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
					err := exportStore.SetConfig(header)
					if err != nil {
						return err
					}
				}
				if err := writeEncryptionOptions(ctx, &EncryptionInfo{Salt: salt}, exportStore); err != nil {
					return err
				}
				encryption = &roachpb.FileEncryptionOptions{Key: storageicl.GenerateKey(encryptionPassphrase, salt)}
			}

			fileFormat := dumpStmt.FileFormat
			Type := dumpStmt.ExpendQuery.Type
			var dumpFilePattern string
			if fileFormat == SQL {
				dumpFilePattern = dumpFilePatternSQL
			} else {
				dumpFilePattern = dumpFilePatternDefault
			}
			//根据查询语句导出数据
			var outs []distsqlpb.ProcessorCoreUnion
			if fileFormat != SQL {
				if fileFormat == CSV {
					for _, table := range tables {
						schemaName := string(table.SchemaName)
						tableName := string(table.TableName)
						fullName := strings.Join([]string{schemaName, tableName}, ".")
						//dest := filepath.Join(file, fullName)
						dest := strings.Join([]string{file, fullName}, string(os.PathSeparator))

						out := distsqlpb.ProcessorCoreUnion{CSVWriter: &distsqlpb.CSVWriterSpec{
							Destination:      dest,
							NamePattern:      dumpFilePattern,
							Options:          csvOpts,
							ChunkRows:        int64(chunk),
							Encryption:       encryption,
							FileFormat:       fileFormat,
							ExportName:       fullName,
							CompressionCodec: codec,
							HttpHeader:       header,
						}}
						outs = append(outs, out)
					}
					if dumpStmt.ExpendQuery != nil && dumpStmt.ExpendQuery.Query != nil {
						//说明并不是整库导出，而是查询导出
						out := distsqlpb.ProcessorCoreUnion{CSVWriter: &distsqlpb.CSVWriterSpec{
							Destination:      file,
							NamePattern:      dumpFilePattern,
							Options:          csvOpts,
							ChunkRows:        int64(chunk),
							Encryption:       encryption,
							FileFormat:       fileFormat,
							ExportName:       dumpStmt.ExpendQuery.Query.String(),
							CompressionCodec: codec,
							HttpHeader:       header,
						}}
						outs = append(outs, out)
					}
				} else if fileFormat == TXT {
					for _, table := range tables {
						schemaName := table.SchemaName.String()
						tableName := table.TableName.String()
						fullName := strings.Join([]string{schemaName, tableName}, ".")
						dest := strings.Join([]string{file, fullName}, string(os.PathSeparator))
						out := distsqlpb.ProcessorCoreUnion{TXTWriter: &distsqlpb.TXTWriterSpec{
							Destination:      dest,
							NamePattern:      dumpFilePatternTXT,
							Options:          txtOpts,
							ChunkRows:        int64(chunk),
							Encryption:       encryption,
							FileFormat:       fileFormat,
							ExportName:       fullName,
							CompressionCodec: codec,
						}}
						outs = append(outs, out)
					}
					if dumpStmt.ExpendQuery != nil && dumpStmt.ExpendQuery.Query != nil {
						//说明并不是整库导出，而是查询导出
						out := distsqlpb.ProcessorCoreUnion{TXTWriter: &distsqlpb.TXTWriterSpec{
							Destination:      file,
							NamePattern:      dumpFilePatternTXT,
							Options:          txtOpts,
							ChunkRows:        int64(chunk),
							Encryption:       encryption,
							FileFormat:       fileFormat,
							ExportName:       dumpStmt.ExpendQuery.Query.String(),
							CompressionCodec: codec,
						}}
						outs = append(outs, out)
					}
				}
				//导出对应的元数据信息（DDL）
				if !skipDDL && dumpStmt.ExpendQuery != nil && dumpStmt.ExpendQuery.Type == roachpb.DUMPFileFormat_DATABASE {
					{
						database := string(dumpStmt.ExpendQuery.DbName)
						evalCtx := &tree.EvalContext{
							NodeID: p.ExtendedEvalContext().NodeID,
						}
						mds, _, err := getDumpMetadata(ctx, p.ExecCfg().InternalExecutor, p.Txn(), evalCtx, database, tables, "", dumpStmt.IsSettings)
						if err != nil {
							return err
						}
						err = dumpDDL(ctx, file, p, mds, database, encryption, header)
						if err != nil {
							return err
						}
					}
				}
				if !skipDDL && dumpStmt.ExpendQuery != nil && dumpStmt.ExpendQuery.Type == roachpb.DUMPFileFormat_SCHEMA {
					evalCtx := &tree.EvalContext{
						NodeID: p.ExtendedEvalContext().NodeID,
					}
					mds, _, err := GetDumpSchemaData(ctx, p.ExecCfg().InternalExecutor, p.Txn(), evalCtx, dumpStmt.ExpendQuery.ScName.Schema(), dumpStmt.ExpendQuery.ScName.Catalog(), tables, "")
					if err != nil {
						return err
					}
					err = dumpSchemaDDL(ctx, file, p, mds, dumpStmt.ExpendQuery.ScName.Schema(), encryption, header)
					if err != nil {
						return err
					}

				}
				if len(plans) == 0 {
					return errors.New("there are no tables in the current database, so export to CSV is not supported")
				}
				if onlyDDL {
					return nil
				}
				log.Info(ctx, "Start Dump Job")
				return dumpQuery(ctx, p, plans, outs, resultsCh)
			}
			var exportName string
			if dumpStmt.ExpendQuery.Type == roachpb.DUMPFileFormat_TABLE {
				exportName = dumpStmt.ExpendQuery.Query.Select.String()
			} else if dumpStmt.ExpendQuery.Type == roachpb.DUMPFileFormat_DATABASE {
				exportName = string(dumpStmt.ExpendQuery.DbName)
			} else if dumpStmt.ExpendQuery.Type == roachpb.DUMPFileFormat_SCHEMA {
				exportName = dumpStmt.ExpendQuery.ScName.String()
			}
			sp := &csvWriter{
				spec: distsqlpb.CSVWriterSpec{
					Destination:      file,
					NamePattern:      dumpFilePattern,
					Options:          csvOpts,
					ChunkRows:        int64(chunk),
					Encryption:       encryption,
					FileFormat:       fileFormat,
					ExportName:       exportName,
					Type:             Type,
					CompressionCodec: codec,
					HttpHeader:       header,
				},
				flowCtx: &runbase.FlowCtx{
					Cfg: &runbase.ServerConfig{
						Settings: p.ExtendedEvalContext().Settings,
						DumpSink: p.ExecCfg().DistSQLSrv.DumpSink,
					},
					EvalCtx: &tree.EvalContext{
						SessionData: p.SessionData(),
						NodeID:      p.ExtendedEvalContext().NodeID,
					},
				},
			}
			evalCtx := &tree.EvalContext{
				NodeID: p.ExtendedEvalContext().NodeID,
			}

			err = WriteSQL(ctx, sp, resultsCh, p.ExecCfg().InternalExecutor, p.Txn(), evalCtx, dumpStmt, skipDDL, onlyDDL, header)
			if err != nil {
				return err
			}
			return nil

		}
		//导出数据库或表
		//TODO 暂时先为nil值，当前为测试版本
		if err := VerifyUsableExportTarget(ctx, exportStore, file, nil); err != nil {
			return err
		}

		//log.Println(file, incrementalFrom, targetDescs, completeDBs)
		defer func() {
			err := exportStore.Delete(ctx, DumpDescriptorCheckpointName)
			if err != nil {
				log.Errorf(ctx, "failed to delete DUMP-CHECKPOINT file")
			}
		}()
		return startDumpJob(ctx, p, dumpStmt, file, incrementalFrom, opts, endTime, resultsCh)

		// return nil
	}
	return fn, dumpHeader, selNodes, false, nil
}

func dumpDDL(
	ctx context.Context,
	file string,
	p sql.PlanHookState,
	mds []BasicMetadata,
	database string,
	encryption *roachpb.FileEncryptionOptions,
	header string,
) error {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	es, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, file)
	if err != nil {
		return err
	}
	defer es.Close()
	if es.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
		err := es.SetConfig(header)
		if err != nil {
			return err
		}
	}
	schemasMap := make(map[string]string)
	var schemas []string
	{
		//获取导出数据库的所有模式信息，当模式下面无表时，暂时不生成对应的DDL
		for _, md := range mds {
			schemaName := strings.Join([]string{database, md.Name.Schema()}, ".")
			if _, ok := schemasMap[schemaName]; !ok {
				schemasMap[schemaName] = schemaName
				schemas = append(schemas, schemaName)
			}
		}
		if err := dumpCreateSchemas(writer.GetBufio(), database, schemas); err != nil {
			return err
		}
	}

	for i, md := range mds {
		if i > 0 {
			if _, err := fmt.Fprintln(writer.GetBufio()); err != nil {
				return err
			}
		}
		if err := CreateTable(writer.GetBufio(), md); err != nil {
			return err
		}
	}
	writer.Flush()
	bufBytes := buf.Bytes()
	if encryption != nil {
		bufBytes, err = storageicl.EncryptFile(bufBytes, encryption.Key)
		if err != nil {
			return err
		}
	}
	part := fmt.Sprintf("%s", database)
	filename := strings.Replace(dumpFilePatternSQL, dumpFilePatternPart, part, -1)
	err = es.WriteFile(ctx, filename, bytes.NewReader(bufBytes))
	if err != nil {
		return err
	}
	return nil
}

func dumpSchemaDDL(
	ctx context.Context,
	file string,
	p sql.PlanHookState,
	mds []BasicMetadata,
	scName string,
	encryption *roachpb.FileEncryptionOptions,
	header string,
) error {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	es, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, file)
	if err != nil {
		return err
	}
	defer es.Close()
	if es.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
		err := es.SetConfig(header)
		if err != nil {
			return err
		}
	}

	if err := writer.WriteString(fmt.Sprintf("CREATE SCHEMA %s;\n", scName)); err != nil {
		return err
	}
	for i, md := range mds {
		if i > 0 {
			if _, err := fmt.Fprintln(writer.GetBufio()); err != nil {
				return err
			}
		}
		if err := schemaCreateTable(writer.GetBufio(), md); err != nil {
			return err
		}
	}
	writer.Flush()
	bufBytes := buf.Bytes()
	if encryption != nil {
		bufBytes, err = storageicl.EncryptFile(bufBytes, encryption.Key)
		if err != nil {
			return err
		}
	}
	part := fmt.Sprintf("%s", scName)
	filename := strings.Replace(dumpFilePatternSQL, dumpFilePatternPart, part, -1)
	err = es.WriteFile(ctx, filename, bytes.NewReader(bufBytes))
	if err != nil {
		return err
	}
	return nil
}

//dumpCreateSchemas 生成创库语句以及指定SCHEMA的语句
func dumpCreateSchemas(w io.Writer, database string, schemas []string) error {
	if _, err := w.Write([]byte(fmt.Sprintf("CREATE DATABASE %s;\n", database))); err != nil {
		return err
	}
	for _, schema := range schemas {
		if _, err := w.Write([]byte(fmt.Sprintf("CREATE SCHEMA %s;\n", schema))); err != nil {
			return err
		}
	}
	return nil
}

//multiTablePlanNodes 为多个表导出查询创建多个planNode
func multiTablePlanNodes(
	ctx context.Context, tables []*tree.TableName, p sql.PlanHookState, selNodes []sql.PlanNode,
) ([]sql.PlanNode, error) {
	for _, table := range tables {
		expr := tree.SelectExpr{Expr: tree.UnqualifiedStar{}}
		exprs := tree.SelectExprs{expr}
		selectClause := tree.SelectClause{
			From:        &tree.From{Tables: []tree.TableExpr{table}},
			Exprs:       exprs,
			TableSelect: true,
		}
		selectStatement := tree.Select{
			Select: &selectClause,
		}
		sel, err := p.Select(ctx, &selectStatement, nil)
		if err != nil {
			return nil, err
		}
		selNodes = append(selNodes, sel)
	}
	return selNodes, nil
}

// ReadEncryptionOptions 获取加密信息
func ReadEncryptionOptions(ctx context.Context, src dumpsink.DumpSink) (*EncryptionInfo, error) {
	r, err := src.ReadFile(ctx, "encryption-info")
	if err != nil {
		return nil, err
	}
	defer r.Close()
	encInfoBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var encInfo EncryptionInfo
	if err := protoutil.Unmarshal(encInfoBytes, &encInfo); err != nil {
		return nil, err
	}
	return &encInfo, nil
}

func writeEncryptionOptions(
	ctx context.Context, opts *EncryptionInfo, dest dumpsink.DumpSink,
) error {
	buf, err := protoutil.Marshal(opts)
	if err != nil {
		return err
	}
	err = dest.WriteFile(ctx, "encryption-info", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	return nil
}

// VerifyUsableExportTarget ensures that the target location does not already
// contain a DUMP or checkpoint and writes an empty checkpoint, both verifying
// that the location is writable and locking out accidental concurrent
// operations on that location if subsequently try this check. Callers must
// clean up the written checkpoint file (DumpDescriptorCheckpointName) only
// after writing to the dump file location (DumpDescriptorName).
func VerifyUsableExportTarget(
	ctx context.Context,
	exportStore dumpsink.DumpSink,
	readable string,
	encryption *roachpb.FileEncryptionOptions,
) error {
	if r, err := exportStore.ReadFile(ctx, DumpDescriptorName); err == nil {
		// TODO(dt): If we audit exactly what not-exists error each ExportStorage
		// returns (and then wrap/tag them), we could narrow this check.
		r.Close()
		return errors.Errorf("%s already contains a %s file",
			readable, DumpDescriptorName)
	}
	if r, err := exportStore.ReadFile(ctx, DumpDescriptorCheckpointName); err == nil {
		r.Close()
		return errors.Errorf("%s already contains a %s file (is another operation already in progress?)",
			readable, DumpDescriptorCheckpointName)
	}
	if err := writeDumpDescriptor(
		ctx, exportStore, DumpDescriptorCheckpointName, &DumpDescriptor{}, encryption,
	); err != nil {
		return errors.Wrapf(err, "cannot write to %s", readable)
	}
	return nil
}

func optsToKVOptions(opts map[string]string) tree.KVOptions {
	if len(opts) == 0 {
		return nil
	}
	sortedOpts := make([]string, 0, len(opts))
	for k := range opts {
		sortedOpts = append(sortedOpts, k)
	}
	sort.Strings(sortedOpts)
	kvopts := make(tree.KVOptions, 0, len(opts))
	for _, k := range sortedOpts {
		opt := tree.KVOption{Key: tree.Name(k)}
		if v := opts[k]; v != "" {
			if k == dumpOptEncPassphrase {
				v = "redacted"
			}
			opt.Value = tree.NewDString(v)
		}
		kvopts = append(kvopts, opt)
	}
	return kvopts
}

// FileDescriptors is an alias on which to implement sort's interface.
type FileDescriptors []DumpDescriptor_File

func (r FileDescriptors) Len() int      { return len(r) }
func (r FileDescriptors) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r FileDescriptors) Less(i, j int) bool {
	if cmp := bytes.Compare(r[i].Span.Key, r[j].Span.Key); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(r[i].Span.EndKey, r[j].Span.EndKey) < 0
}

func writeDumpDescriptor(
	ctx context.Context,
	exportStore dumpsink.DumpSink,
	filename string,
	desc *DumpDescriptor,
	encryption *roachpb.FileEncryptionOptions,
) error {
	sort.Sort(FileDescriptors(desc.Files))

	descBuf, err := protoutil.Marshal(desc)
	if err != nil {
		return err
	}
	if encryption != nil {
		descBuf, err = storageicl.EncryptFile(descBuf, encryption.Key)
		if err != nil {
			return err
		}
	}
	return exportStore.WriteFile(ctx, filename, bytes.NewReader(descBuf))
}

// readDumpDescriptor reads and unmarshals a DumpDescriptor from filename in
// the provided export store.
func readDumpDescriptor(
	ctx context.Context,
	exportStore dumpsink.DumpSink,
	filename string,
	encryption *roachpb.FileEncryptionOptions,
) (DumpDescriptor, error) {
	r, err := exportStore.ReadFile(ctx, filename)
	if err != nil {
		return DumpDescriptor{}, err
	}
	defer r.Close()
	descBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return DumpDescriptor{}, err
	}
	if encryption != nil {
		descBytes, err = storageicl.DecryptFile(descBytes, encryption.Key)
		if err != nil {
			return DumpDescriptor{}, err
		}
	}
	var dumpDesc DumpDescriptor
	if err := protoutil.Unmarshal(descBytes, &dumpDesc); err != nil {
		if encryption == nil && storageicl.AppearsEncrypted(descBytes) {
			return DumpDescriptor{}, errors.Wrapf(
				err, "file appears encrypted -- try specifying %q", dumpOptEncPassphrase)
		}
		return DumpDescriptor{}, err
	}
	return dumpDesc, err
}

// ReadDumpDescriptorFromURI creates an export store from the given URI, then
// reads and unmarshals a dumpccl.DumpDescriptor at the standard location in the
// export storage.
func ReadDumpDescriptorFromURI(
	ctx context.Context,
	uri string,
	makeDumpSinkFromURI dumpsink.FromURIFactory,
	encryption *roachpb.FileEncryptionOptions,
	header string,
) (DumpDescriptor, error) {
	exportStore, err := makeDumpSinkFromURI(ctx, uri)
	if err != nil {
		return DumpDescriptor{}, err
	}
	defer exportStore.Close()
	if exportStore.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
		err := exportStore.SetConfig(header)
		if err != nil {
			return DumpDescriptor{}, err
		}
	}
	dumpDesc, err := readDumpDescriptor(ctx, exportStore, DumpDescriptorName, encryption)
	if err != nil {
		return DumpDescriptor{}, err
	}
	dumpDesc.Dir = exportStore.Conf()
	// TODO(dan): Sanity check this dumpccl.DumpDescriptor: non-empty EndTime,
	// non-empty Paths, and non-overlapping Spans and keyranges in Files.
	return dumpDesc, nil
}

type versionedValues struct {
	Key    roachpb.Key
	Values []roachpb.Value
}

// getAllRevisions scans all keys between startKey and endKey getting all
// revisions between startTime and endTime.
// TODO(dt): if/when client gets a ScanRevisionsRequest or similar, use that.
func getAllRevisions(
	ctx context.Context,
	db *client.DB,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
) ([]versionedValues, error) {
	// TODO(dt): version check.
	header := roachpb.Header{Timestamp: endTime}
	req := &roachpb.DumpRequest{
		RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey},
		StartTime:     startTime,
		MVCCFilter:    roachpb.MVCCFilter_All,
		ReturnSST:     true,
		OmitChecksum:  true,
	}
	resp, pErr := client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	var res []versionedValues
	for _, file := range resp.(*roachpb.DumpResponse).Files {
		sst := engine.MakeRocksDBSstFileReader()
		defer sst.Close()

		if err := sst.IngestExternalFile(file.SST); err != nil {
			return nil, err
		}
		start, end := engine.MVCCKey{Key: startKey}, engine.MVCCKey{Key: endKey}
		if err := sst.Iterate(start, end, func(kv engine.MVCCKeyValue) (bool, error) {
			if len(res) == 0 || !res[len(res)-1].Key.Equal(kv.Key.Key) {
				res = append(res, versionedValues{Key: kv.Key.Key})
			}
			res[len(res)-1].Values = append(res[len(res)-1].Values, roachpb.Value{Timestamp: kv.Key.Timestamp, RawBytes: kv.Value})
			return false, nil
		}); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// splitAndFilterSpans returns the spans that represent the set difference
// (includes - excludes) while also guaranteeing that each output span does not
// cross the endpoint of a RangeDescriptor in ranges.
func splitAndFilterSpans(
	includes []roachpb.Span, excludes []roachpb.Span, ranges []roachpb.RangeDescriptor,
) []roachpb.Span {
	type includeMarker struct{}
	type excludeMarker struct{}

	includeCovering := coveringFromSpans(includes, includeMarker{})
	excludeCovering := coveringFromSpans(excludes, excludeMarker{})

	var rangeCovering intervalicl.Ranges
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, intervalicl.Range{
			Low:  []byte(rangeDesc.StartKey),
			High: []byte(rangeDesc.EndKey),
		})
	}

	splits := intervalicl.OverlapCoveringMerge(
		[]intervalicl.Ranges{includeCovering, excludeCovering, rangeCovering},
	)

	var out []roachpb.Span
	for _, split := range splits {
		include := false
		exclude := false
		for _, payload := range split.Payload.([]interface{}) {
			switch payload.(type) {
			case includeMarker:
				include = true
			case excludeMarker:
				exclude = true
			}
		}
		if include && !exclude {
			out = append(out, roachpb.Span{
				Key:    roachpb.Key(split.Low),
				EndKey: roachpb.Key(split.High),
			})
		}
	}
	return out
}

// coveringFromSpans creates an intervalccl.Ranges with a fixed payload from a
// slice of roachpb.Spans.
func coveringFromSpans(spans []roachpb.Span, payload interface{}) intervalicl.Ranges {
	var covering intervalicl.Ranges
	for _, span := range spans {
		covering = append(covering, intervalicl.Range{
			Low:     []byte(span.Key),
			High:    []byte(span.EndKey),
			Payload: payload,
		})
	}
	return covering
}

func allRangeDescriptors(ctx context.Context, txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to scan range descriptors")
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(g *gossip.Gossip) int {
	var nodes int
	_ = g.IterateInfos(gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
		nodes++
		return nil
	})
	return nodes
}
