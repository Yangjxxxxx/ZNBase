// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/icl/utilicl"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/pgwire"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/encoding/txt"
	"github.com/znbasedb/znbase/pkg/util/gziputil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// exportHeader is the header for EXPORT stmt results.
var exportHeaderTXT = sqlbase.ResultColumns{
	{Name: "queryname", Typ: types.String},
	{Name: "filename", Typ: types.String},
	{Name: "rows", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

const exportChunkSizeDefaultTXT = 100000
const exportFilePatternPartTXT = "%part%"
const exportFilePatternDefaultTXT = exportFilePatternPartTXT + ".txt"
const exportNullAsTXT = `\N`

// exportPlanHook implements sql.PlanHook.
func exportPlanHookTXT(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	exportStmt, ok := stmt.(*tree.Export)
	if !ok {
		return nil, nil, nil, false, nil
	}

	fileFn, err := p.TypeAsString(exportStmt.File, "EXPORT")
	if err != nil {
		return nil, nil, nil, false, err
	}

	if exportStmt.FileFormat != "TXT" {
		return nil, nil, nil, false, errors.Errorf("unsupported export format: %q", exportStmt.FileFormat)
	}

	optsFn, err := p.TypeAsStringOpts(exportStmt.Options, exportOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	sel, err := p.Select(ctx, exportStmt.Query, nil)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, plans []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, exportStmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := utilicl.CheckCommercialFeatureEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "EXPORT",
		); err != nil {
			return err
		}

		if err := p.RequireAdminRole(ctx, "EXPORT"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("EXPORT cannot be used inside a transaction")
		}

		file, err := fileFn()
		if err != nil {
			return err
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		txtOpts := roachpb.TXTOptions{}
		txtOpts.Null = exportNullAsTXT
		if override, ok := opts[exportOptionDelimiter]; ok {
			txtOpts.Comma, err = util.GetSingleRune(override)
			if err != nil {
				return pgerror.NewError(pgcode.InvalidParameterValue, "invalid delimiter")
			}
		}

		if override, ok := opts[exportOptionNullAs]; ok {
			txtOpts.NullEncoding = &override
		}

		chunk := exportChunkSizeDefaultTXT
		if override, ok := opts[exportOptionChunkSize]; ok {
			chunk, err = strconv.Atoi(override)
			if err != nil {
				return pgerror.NewError(pgcode.InvalidParameterValue, err.Error())
			}
			if chunk < 1 {
				return pgerror.NewError(pgcode.InvalidParameterValue, "invalid txt chunk size")
			}
		}
		outs := make([]distsqlpb.ProcessorCoreUnion, 1)
		out := distsqlpb.ProcessorCoreUnion{TXTWriter: &distsqlpb.TXTWriterSpec{
			Destination: file,
			NamePattern: exportFilePatternDefaultTXT,
			Options:     txtOpts,
			ChunkRows:   int64(chunk),
		}}
		outs = append(outs, out)
		rows := rowcontainer.NewRowContainer(
			p.ExtendedEvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromColTypes(sql.ExportPlanResultTypes), 0,
		)
		rw := sql.NewRowResultWriter(rows)

		if err := sql.PlanAndRunExport(
			ctx, p.DistSQLPlanner(), p.ExecCfg(), p.Txn(), p.ExtendedEvalContext(), plans, outs, rw,
		); err != nil {
			return err
		}
		for i := 0; i < rows.Len(); i++ {
			resultsCh <- rows.At(i)
		}
		rows.Close(ctx)
		return rw.Err()
	}

	return fn, exportHeaderTXT, []sql.PlanNode{sel}, false, nil
}

func newTXTWriterProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec distsqlpb.TXTWriterSpec,
	input runbase.RowSource,
	output runbase.RowReceiver,
) (runbase.Processor, error) {
	c := &txtWriter{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}
	if err := c.out.Init(&distsqlpb.PostProcessSpec{}, sql.ExportPlanResultTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return c, nil
}

type txtWriter struct {
	flowCtx     *runbase.FlowCtx
	processorID int32
	spec        distsqlpb.TXTWriterSpec
	input       runbase.RowSource
	out         runbase.ProcOutputHelper
	output      runbase.RowReceiver
}

var _ runbase.Processor = &txtWriter{}

func (sp *txtWriter) OutputTypes() []sqlbase.ColumnType {
	return sql.ExportPlanResultTypes
}

func (sp *txtWriter) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "txtWriter")
	defer tracing.FinishSpan(span)

	err := func() error {
		pattern := exportFilePatternDefaultTXT
		if sp.spec.NamePattern != "" {
			pattern = sp.spec.NamePattern
		}

		types := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := runbase.MakeNoMetadataRowSource(sp.input, sp.output)

		alloc := &sqlbase.DatumAlloc{}

		var buf bytes.Buffer
		writer := txt.NewWriter(&buf)
		if sp.spec.Options.Comma != 0 {
			writer.Comma = sp.spec.Options.Comma
		}
		sp.spec.Options.Null = exportNullAsTXT
		nullsAs := sp.spec.Options.Null
		if sp.spec.Options.NullEncoding != nil {
			nullsAs = *sp.spec.Options.NullEncoding
		}
		f := tree.NewFmtCtx(tree.FmtExport)
		defer f.Close()

		txtRow := make([]string, len(types))

		chunk := 0
		done := false
		for {
			var rows int64
			buf.Reset()
			for {
				if sp.spec.ChunkRows > 0 && rows >= sp.spec.ChunkRows {
					break
				}
				row, err := input.NextRow()
				if err != nil {
					return err
				}
				if row == nil {
					done = true
					break
				}
				rows++

				for i, ed := range row {
					if ed.IsNull() {
						txtRow[i] = nullsAs
						continue
					}
					if err := ed.EnsureDecoded(&types[i], alloc); err != nil {
						return err
					}
					ed.Datum.Format(f)

					txtRow[i] = f.String()
					if csvNull == nullsAs {
						txtRow[i], err = unescapeStringTXT(txtRow[i])
						if err != nil {
							return err
						}
					}
					f.Reset()
				}
				if err := writer.Write(txtRow); err != nil {
					return err
				}
			}
			if rows < 1 {
				break
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

			size := buf.Len()
			bufBytes := buf.Bytes()
			if sp.spec.Options.Encoding != "" {
				judge := utf8.Valid(bufBytes)
				switch sp.spec.Options.Encoding {
				case "gbk":
					if judge {
						bufBytes, err = pgwire.Utf8ToGbk(bufBytes)
						if err != nil {
							return err
						}
					}
				case "utf8":
					if !judge {
						bufBytes, err = pgwire.GbkToUtf8(bufBytes)
						if err != nil {
							return err
						}
					}
				default:
					return errors.Errorf("Unsupported encoding format: %v", sp.spec.Options.Encoding)
				}
			}
			if sp.spec.Encryption != nil {
				bufBytes, err = storageicl.EncryptFile(bufBytes, sp.spec.Encryption.Key)
				if err != nil {
					return err
				}
			}
			part := fmt.Sprintf("n%d.%d", sp.flowCtx.EvalCtx.NodeID, chunk)
			chunk++
			filename := strings.Replace(pattern, exportFilePatternPartTXT, part, -1)

			if sp.spec.CompressionCodec == roachpb.FileCompression_Gzip {
				bufBytes, filename, size, err = gziputil.RunGzipTXT(bufBytes, part, sp.spec)
				if err != nil {
					return err
				}
			}
			if err := es.WriteFile(ctx, filename, bytes.NewReader(bufBytes)); err != nil {
				return err
			}
			res := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING},
					tree.NewDString(sp.spec.ExportName),
				),
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING},
					tree.NewDString(filename),
				),
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
					tree.NewDInt(tree.DInt(rows)),
				),
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
					tree.NewDInt(tree.DInt(size)),
				),
			}

			cs, err := sp.out.EmitRow(ctx, res)
			if err != nil {
				return err
			}
			if cs != runbase.NeedMoreRows {
				// TODO(dt): presumably this is because our recv already closed due to
				// another error... so do we really need another one?
				return errors.New("unexpected closure of consumer")
			}
			if done {
				break
			}
		}

		return nil
	}()

	// TODO(dt): pick up tracing info in trailing meta
	runbase.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}
func unescapeStringTXT(value string) (string, error) {
	reg, err := regexp.MatchString("^\\\\[\\\\]*N$", value)
	if err != nil {
		return value, err
	}
	if !reg {
		return value, nil
	}
	value = strings.Replace(value, "\\", "\\\\", -1)
	return value, nil

}
func init() {
	sql.AddPlanHook(exportPlanHookTXT)
	rowexec.NewTXTWriterProcessor = newTXTWriterProcessor
}
