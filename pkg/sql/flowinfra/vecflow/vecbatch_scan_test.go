// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Note that this file is not in pkg/sql/vecexec because it instantiates a
// server, and if it were moved into sql/vecexec, that would create a cycle
// with pkg/server.

package vecflow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
)

func BenchmarkColBatchScan(b *testing.B) {
	defer leaktest.AfterTest(b)()
	logScope := log.Scope(b)
	defer logScope.Close(b)
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const numCols = 2
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		tableName := fmt.Sprintf("t%d", numRows)
		sqlutils.CreateTable(
			b, sqlDB, tableName,
			"k INT PRIMARY KEY, v INT",
			numRows,
			sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(42)),
		)
		tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "public", tableName)
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			spec := distsqlpb.ProcessorSpec{
				Core: distsqlpb.ProcessorCoreUnion{
					TableReader: &distsqlpb.TableReaderSpec{
						Table: *tableDesc,
						Spans: []distsqlpb.TableReaderSpan{{Span: tableDesc.PrimaryIndexSpan()}},
					}},
				Post: distsqlpb.PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{0, 1},
				},
			}

			evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
			defer evalCtx.Stop(ctx)

			flowCtx := runbase.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg:     &runbase.ServerConfig{Settings: s.ClusterSettings()},
				Txn:     client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
				NodeID:  s.NodeID(),
			}

			b.SetBytes(int64(numRows * numCols * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				args := vecexec.NewColOperatorArgs{
					Spec:                               &spec,
					StreamingMemAccount:                testMemAcc,
					UseStreamingMemAccountForBuffering: true,
				}
				res, err := vecexec.NewColOperator(ctx, &flowCtx, args)
				if err != nil {
					b.Fatal(err)
				}
				tr := res.Op
				tr.Init()
				b.StartTimer()
				for {
					bat := tr.Next(ctx)
					if bat.Length() == 0 {
						break
					}
				}
			}
		})
	}
}
