// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/CCL.txt

package load

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/workload"
)

type workloadReader struct {
	conv        *rowConverter
	kvCh        chan KVBatch
	metrics     *Metrics
	fileContext importFileContext
}

func (w *workloadReader) closeRejectCh(ctx context.Context) {
	if w.fileContext.rejected != nil {
		close(w.fileContext.rejected)
	}
}

func (w *workloadReader) saveRejectRecord(
	ctx context.Context, cp *readImportDataProcessor, group *ctxgroup.Group,
) error {
	return nil
}

var _ inputConverter = &workloadReader{}

func newWorkloadReader(
	kvCh chan KVBatch, table *sqlbase.TableDescriptor, evalCtx *tree.EvalContext, metrics *Metrics,
) (*workloadReader, error) {
	conv, err := newRowConverter(table, evalCtx, kvCh)
	if err != nil {
		return nil, err
	}
	return &workloadReader{kvCh: kvCh, conv: conv, metrics: metrics}, nil
}

func (w *workloadReader) start(ctx ctxgroup.Group) {
}

func (w *workloadReader) inputFinished(ctx context.Context) {
	close(w.kvCh)
}

// makeDatumFromRaw tries to fast-path a few workload-generated types into
// directly datums, to dodge making a string and then the parsing it.
func makeDatumFromRaw(
	alloc *sqlbase.DatumAlloc, datum interface{}, hint types.T, evalCtx *tree.EvalContext,
) (tree.Datum, error) {
	if datum == nil {
		return tree.DNull, nil
	}
	switch t := datum.(type) {
	case int:
		return alloc.NewDInt(tree.DInt(t)), nil
	case int64:
		return alloc.NewDInt(tree.DInt(t)), nil
	case []byte:
		return alloc.NewDBytes(tree.DBytes(t)), nil
	case time.Time:
		switch hint {
		case types.TimestampTZ:
			return tree.MakeDTimestampTZ(t, time.Microsecond), nil
		case types.Timestamp:
			return tree.MakeDTimestamp(t, time.Microsecond), nil
		}
	case string:
		return tree.ParseDatumStringAs(hint, t, evalCtx, true)
	}
	return tree.ParseDatumStringAs(hint, fmt.Sprint(datum), evalCtx, true)
}

func (w *workloadReader) readFiles(
	ctx context.Context,
	cp *readImportDataProcessor,
	encryption *roachpb.FileEncryptionOptions,
	encoding string,
	details jobspb.ImportDetails,
) error {
	dataFiles := cp.spec.Uri

	var alloc sqlbase.DatumAlloc

	numFiles := float32(len(dataFiles))
	var filesCompleted int
	for inputIdx, fileName := range dataFiles {
		file, err := url.Parse(fileName.Path)
		if err != nil {
			return err
		}
		conf, err := dumpsink.ParseWorkloadConfig(file)
		if err != nil {
			return err
		}
		meta, err := workload.Get(conf.Generator)
		if err != nil {
			return err
		}
		// Different versions of the workload could generate different data, so
		// disallow this.
		if meta.Version != conf.Version {
			return errors.Errorf(
				`expected %s version "%s" but got "%s"`, meta.Name, conf.Version, meta.Version)
		}
		gen := meta.New()
		if f, ok := gen.(workload.Flagser); ok {
			if err := f.Flags().Parse(conf.Flags); err != nil {
				return errors.Wrapf(err, `parsing parameters %s`, strings.Join(conf.Flags, ` `))
			}
		}
		var t workload.Table
		for _, tbl := range gen.Tables() {
			if tbl.Name == conf.Table {
				t = tbl
				break
			}
		}
		if t.Name == `` {
			return errors.Wrapf(err, `unknown table %s for generator %s`, conf.Table, meta.Name)
		}

		// how is this reader, before starting this file.
		initialProgress := float32(filesCompleted) / numFiles
		numBatches := conf.BatchEnd - conf.BatchBegin
		var rows int64
		lastProgress := rows
		for b := conf.BatchBegin; b < conf.BatchEnd; b++ {
			if rows-lastProgress > 10000 {
				// how far we are on this file
				fileProgress := float32(b-conf.BatchBegin) / float32(numBatches)
				progress := initialProgress + fileProgress/numFiles
				if err := FractionProg(ctx, cp, progress); err != nil {
					return err
				}
				lastProgress = rows
			}
			for _, row := range t.InitialRows.Batch(int(b)) {
				rows++
				for i, value := range row {
					converted, err := makeDatumFromRaw(&alloc, value, w.conv.visibleColTypes[i], w.conv.evalCtx)
					if err != nil {
						return err
					}
					w.conv.datums[i] = converted
				}
				if err := w.conv.row(ctx, inputIdx, rows, w.metrics); err != nil {
					return err
				}
			}
		}
		filesCompleted++
	}
	return w.conv.sendBatch(ctx, w.metrics)
}
