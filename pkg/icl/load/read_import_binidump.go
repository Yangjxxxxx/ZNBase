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
	"io"
	"io/ioutil"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/log"
)

type znbaseDumpReader struct {
	tables      map[string]*rowConverter
	descs       map[string]*sqlbase.TableDescriptor
	kvCh        chan KVBatch
	opts        roachpb.PgDumpOptions
	metrics     *Metrics
	fileContext importFileContext
}

func (m *znbaseDumpReader) closeRejectCh(ctx context.Context) {
	if m.fileContext.rejected != nil {
		close(m.fileContext.rejected)
	}
}

func (m *znbaseDumpReader) saveRejectRecord(
	ctx context.Context, cp *readImportDataProcessor, group *ctxgroup.Group,
) error {
	return nil
}

var _ inputConverter = &znbaseDumpReader{}

// newPgDumpReader creates a new inputConverter for pg_dump files.
func newBiniDumpReader(
	kvCh chan KVBatch,
	opts roachpb.PgDumpOptions,
	descs map[string]*sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
	metrics *Metrics,
) (*znbaseDumpReader, error) {
	converters := make(map[string]*rowConverter, len(descs))
	for name, desc := range descs {
		if desc.IsTable() {
			conv, err := newRowConverter(desc, evalCtx, kvCh)
			if err != nil {
				return nil, err
			}
			converters[name] = conv
		}
	}
	return &znbaseDumpReader{
		kvCh:    kvCh,
		tables:  converters,
		descs:   descs,
		opts:    opts,
		metrics: metrics,
	}, nil
}

func (m *znbaseDumpReader) start(ctx ctxgroup.Group) {
}

func (m *znbaseDumpReader) inputFinished(ctx context.Context) {
	close(m.kvCh)
}

func (m *znbaseDumpReader) readFiles(
	ctx context.Context,
	cp *readImportDataProcessor,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
	details jobspb.ImportDetails,
) error {
	return readInputFiles(ctx, cp, m.readFile, &m.fileContext, encryption, encoding, details)
}

func (m *znbaseDumpReader) readFile(
	ctx context.Context,
	input io.Reader,
	inputIdx int32,
	inputName *distsqlpb.ReadImportDataSpec_TableURL,
	progressFn progressFn,
	fileContext *importFileContext,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
) error {
	var inserts, count int64
	if encryption != nil {
		bufByte, err := ioutil.ReadAll(input)
		if err != nil {
			return err
		}
		byte, err := storageicl.DecryptFile(bufByte, encryption.Key)
		if err != nil {
			return err
		}
		input = bytes.NewReader(byte)
		if err != nil {
			return err
		}
	}
	ps := newPostgreStream(input, int(m.opts.MaxRowSize))
	semaCtx := &tree.SemaContext{}
	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "postgres parse error")
		}
		switch i := stmt.(type) {
		case *tree.Insert:
			n, ok := i.Table.(*tree.TableName)
			if !ok {
				return errors.Errorf("unexpected: %T", i.Table)
			}
			name, err := getTableName(n)
			if err != nil {
				return errors.Wrapf(err, "%s", i)
			}
			conv, ok := m.tables[name.TableName.String()]
			if !ok {
				// not importing this table.
				continue
			}
			if ok && conv == nil {
				return errors.Errorf("missing schema info for requested table %q", name.TableName)
			}
			values, ok := i.Rows.Select.(*tree.ValuesClause)
			if !ok {
				return errors.Errorf("unsupported: %s", i.Rows.Select)
			}
			inserts++
			startingCount := count
			for _, tuple := range values.Rows {
				count++
				if expected, got := len(conv.visibleCols), len(tuple); expected != got {
					return errors.Errorf("expected %d values, got %d: %v", expected, got, tuple)
				}
				for i, expr := range tuple {
					typed, err := expr.TypeCheck(semaCtx, conv.visibleColTypes[i], false)
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					converted, err := typed.Eval(conv.evalCtx)
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					conv.datums[i] = converted
				}
				if err := conv.row(ctx, inputIdx, count, m.metrics); err != nil {
					return err
				}
			}
		case *tree.CopyFrom:
			if !i.Stdin {
				return errors.New("expected STDIN option on COPY FROM")
			}
			name, err := getTableName(&i.Table)
			if err != nil {
				return errors.Wrapf(err, "%s", i)
			}
			conv, importing := m.tables[name.TableName.String()]
			if importing && conv == nil {
				return errors.Errorf("missing schema info for requested table %q", name.TableName)
			}
			if conv != nil {
				if expected, got := len(conv.visibleCols), len(i.Columns); expected != got {
					return errors.Errorf("expected %d columns, got %d", expected, got)
				}
				for colI, col := range i.Columns {
					if string(col) != conv.visibleCols[colI].Name {
						return errors.Errorf("COPY columns do not match table columns for table %s", name.TableName)
					}
				}
			}
			for {
				row, err := ps.Next()
				// We expect an explicit copyDone here. io.EOF is unexpected.
				if err == io.EOF {
					return makeRowErr(inputName.Path, count, "unexpected EOF")
				}
				if row == errCopyDone {
					break
				}
				count++
				if err != nil {
					return makeRowErr(inputName.Path, count, "%s", err)
				}
				if !importing {
					continue
				}
				switch row := row.(type) {
				case copyData:
					if expected, got := len(conv.visibleCols), len(row); expected != got {
						return errors.Errorf("expected %d values, got %d", expected, got)
					}
					for i, s := range row {
						if s == nil {
							conv.datums[i] = tree.DNull
						} else {
							conv.datums[i], err = tree.ParseDatumStringAs(conv.visibleColTypes[i], *s, conv.evalCtx, true)
							if err != nil {
								col := conv.visibleCols[i]
								return makeRowErr(inputName.Path, count, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
							}
						}
					}
					if err := conv.row(ctx, inputIdx, count, m.metrics); err != nil {
						return err
					}
				default:
					return makeRowErr(inputName.Path, count, "unexpected: %v", row)
				}
			}
		case *tree.Select:
			// Look for something of the form "SELECT pg_catalog.setval(...)". Any error
			// or unexpected value silently breaks out of this branch. We are silent
			// instead of returning an error because we expect input to be well-formatted
			// by pg_dump, and thus if it isn't, we don't try to figure out what to do.
			sc, ok := i.Select.(*tree.SelectClause)
			if !ok {
				break
			}
			if len(sc.Exprs) != 1 {
				break
			}
			fn, ok := sc.Exprs[0].Expr.(*tree.FuncExpr)
			if !ok || len(fn.Exprs) < 2 {
				break
			}
			if name := strings.ToLower(fn.Func.String()); name != "setval" && name != "pg_catalog.setval" {
				break
			}
			seqname, ok := fn.Exprs[0].(*tree.StrVal)
			if !ok {
				break
			}
			seqval, ok := fn.Exprs[1].(*tree.NumVal)
			if !ok {
				break
			}
			val, err := seqval.AsInt64()
			if err != nil {
				break
			}
			isCalled := false
			if len(fn.Exprs) > 2 {
				called, ok := fn.Exprs[2].(*tree.DBool)
				if !ok {
					break
				}
				isCalled = bool(*called)
			}
			name, err := parser.ParseTableName(seqname.RawString(), 0)
			if err != nil {
				break
			}
			seq := m.descs[name.Table()]
			if seq == nil {
				break
			}
			key, val, err := sql.MakeSequenceKeyVal(seq, val, isCalled)
			if err != nil {
				return makeRowErr(inputName.Path, count, "%s", err)
			}
			kv := roachpb.KeyValue{Key: key}
			kv.Value.SetInt(val)
			kvBatch := KVBatch{
				Source:  0,
				LastRow: 0,
				KVs:     []roachpb.KeyValue{kv},
			}
			m.kvCh <- kvBatch
		default:
			if log.V(3) {
				log.Infof(ctx, "ignoring %T stmt: %v", i, i)
			}
			continue
		}
	}
	for _, conv := range m.tables {
		if err := conv.sendBatch(ctx, m.metrics); err != nil {
			return err
		}
	}
	return nil
}
