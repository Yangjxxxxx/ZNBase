package dump

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/encoding/csv"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

func dumpQuery(
	ctx context.Context,
	p sql.PlanHookState,
	plans []sql.PlanNode,
	outs []distsqlpb.ProcessorCoreUnion,
	resultsCh chan<- tree.Datums,
) error {
	rows := rowcontainer.NewRowContainer(
		p.ExtendedEvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromColTypes(sql.ExportPlanResultTypes), 0,
	)
	defer rows.Close(ctx)
	rw := sql.NewRowResultWriter(rows)
	if err := sql.PlanAndRunExport(
		ctx, p.DistSQLPlanner(), p.ExecCfg(), p.Txn(), p.ExtendedEvalContext(), plans, outs, rw,
	); err != nil {
		return err
	}
	for i := 0; i < rows.Len(); i++ {
		resultsCh <- rows.At(i)
	}
	return rw.Err()
}

func newCSVWriterProcessor(
	flowCtx *runbase.FlowCtx,
	processorID int32,
	spec distsqlpb.CSVWriterSpec,
	input runbase.RowSource,
	output runbase.RowReceiver,
) (runbase.Processor, error) {
	c := &csvWriter{
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

type csvWriter struct {
	flowCtx     *runbase.FlowCtx
	processorID int32
	spec        distsqlpb.CSVWriterSpec
	input       runbase.RowSource
	out         runbase.ProcOutputHelper
	output      runbase.RowReceiver
}

var _ runbase.Processor = &csvWriter{}

func (sp *csvWriter) OutputTypes() []sqlbase.ColumnType {
	return sql.ExportPlanResultTypes
}

func (sp *csvWriter) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter")
	defer tracing.FinishSpan(span)

	err := func() error {
		pattern := dumpFilePatternDefault
		if sp.spec.NamePattern != "" {
			pattern = sp.spec.NamePattern
		}

		types := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := runbase.MakeNoMetadataRowSource(sp.input, sp.output)

		alloc := &sqlbase.DatumAlloc{}

		var buf bytes.Buffer
		writer := csv.NewWriter(&buf)
		if sp.spec.Options.Comma != 0 {
			writer.Comma = sp.spec.Options.Comma
		}
		nullsAs := ""
		if sp.spec.Options.NullEncoding != nil {
			nullsAs = *sp.spec.Options.NullEncoding
		}
		f := tree.NewFmtCtx(tree.FmtExport)
		defer f.Close()

		csvRow := make([]string, len(types))

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
						csvRow[i] = nullsAs
						continue
					}
					if err := ed.EnsureDecoded(&types[i], alloc); err != nil {
						return err
					}
					ed.Datum.Format(f)
					csvRow[i] = f.String()
					f.Reset()
				}
				if err := writer.Write(csvRow); err != nil {
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
			//es, err := storageccl.MakeExportStorage(ctx, conf, sp.flowCtx.Settings)
			es, err := sp.flowCtx.Cfg.DumpSink(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()

			size := buf.Len()

			part := fmt.Sprintf("n%d.%d", sp.flowCtx.EvalCtx.NodeID, chunk)
			chunk++
			filename := strings.Replace(pattern, dumpFilePatternPart, part, -1)
			if es.Conf().Provider == roachpb.ExportStorageProvider_Http && sp.spec.HttpHeader != "" {
				err := es.SetConfig(sp.spec.HttpHeader)
				if err != nil {
					return err
				}
			}
			if err := es.WriteFile(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
				return err
			}
			res := sqlbase.EncDatumRow{
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
