package distsqlsrv

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/distsql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/randutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

var (
	// diffCtx is a decimal context used to perform subtractions between
	// local and non-local decimal results to check if they are within
	// 1ulp. Decimals within 1ulp is acceptable for high-precision
	// decimal calculations.
	diffCtx = tree.DecimalCtx.WithPrecision(0)
	// Use to check for 1ulp.
	bigOne = big.NewInt(1)
	// floatPrecFmt is the format string with a precision of 3 (after
	// decimal point) specified for float comparisons. Float aggregation
	// operations involve unavoidable off-by-last-few-digits errors, which
	// is expected.
	floatPrecFmt = "%.3f"
)

// runTestFlow runs a flow with the given processors and returns the results.
// Any errors stop the current test.
type fakeExprContext struct{}

var _ distsqlplan.ExprContext = fakeExprContext{}

func (fakeExprContext) EvalContext() *tree.EvalContext {
	return &tree.EvalContext{}
}

func (fakeExprContext) IsLocal() bool {
	return false
}

func (fakeExprContext) EvaluateSubqueries() bool {
	return true
}

// checkDistAggregationInfo tests that a flow with multiple local stages and a
// final stage (in accordance with per DistAggregationInfo) gets the same result
// with a naive aggregation flow that has a single non-distributed stage.
//
// Both types of flows are set up and ran against the first numRows of the given
// table. We assume the table's first column is the primary key, with values
// from 1 to numRows. A non-PK column that works with the function is chosen.
func checkDistAggregationInfo(
	ctx context.Context,
	t *testing.T,
	srv serverutils.TestServerInterface,
	tableDesc *sqlbase.TableDescriptor,
	colIdx int,
	numRows int,
	fn distsqlpb.AggregatorSpec_Func,
	info DistAggregationInfo,
) {
	colType := tableDesc.Columns[colIdx].Type

	makeTableReader := func(startPK, endPK int, streamID int) distsqlpb.ProcessorSpec {
		tr := distsqlpb.TableReaderSpec{
			Table: *tableDesc,
			Spans: make([]distsqlpb.TableReaderSpan, 1),
		}

		var err error
		tr.Spans[0].Span.Key, err = sqlbase.TestingMakePrimaryIndexKey(tableDesc, startPK)
		if err != nil {
			t.Fatal(err)
		}
		tr.Spans[0].Span.EndKey, err = sqlbase.TestingMakePrimaryIndexKey(tableDesc, endPK)
		if err != nil {
			t.Fatal(err)
		}

		return distsqlpb.ProcessorSpec{
			Core: distsqlpb.ProcessorCoreUnion{TableReader: &tr},
			Post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{uint32(colIdx)},
			},
			Output: []distsqlpb.OutputRouterSpec{{
				Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
				Streams: []distsqlpb.StreamEndpointSpec{
					{Type: distsqlpb.StreamEndpointSpec_LOCAL, StreamID: distsqlpb.StreamID(streamID)},
				},
			}},
		}
	}

	txn := client.NewTxn(ctx, srv.DB(), srv.NodeID(), client.RootTxn)

	// First run a flow that aggregates all the rows without any local stages.

	rowsNonDist := RunTestFlow(
		t, srv, txn,
		makeTableReader(1, numRows+1, 0),
		distsqlpb.ProcessorSpec{
			Input: []distsqlpb.InputSyncSpec{{
				Type:        distsqlpb.InputSyncSpec_UNORDERED,
				ColumnTypes: []sqlbase.ColumnType{colType},
				Streams: []distsqlpb.StreamEndpointSpec{
					{Type: distsqlpb.StreamEndpointSpec_LOCAL, StreamID: 0},
				},
			}},
			Core: distsqlpb.ProcessorCoreUnion{Aggregator: &distsqlpb.AggregatorSpec{
				Aggregations: []distsqlpb.AggregatorSpec_Aggregation{{Func: fn, ColIdx: []uint32{0}}},
			}},
			Output: []distsqlpb.OutputRouterSpec{{
				Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
				Streams: []distsqlpb.StreamEndpointSpec{
					{Type: distsqlpb.StreamEndpointSpec_SYNC_RESPONSE},
				},
			}},
		},
	)

	numIntermediary := len(info.LocalStage)
	numFinal := len(info.FinalStage)
	for _, finalInfo := range info.FinalStage {
		if len(finalInfo.LocalIdxs) == 0 {
			t.Fatalf("final stage must specify input local indices: %#v", info)
		}
		for _, localIdx := range finalInfo.LocalIdxs {
			if localIdx >= uint32(numIntermediary) {
				t.Fatalf("local index %d out of bounds of local stages: %#v", localIdx, info)
			}
		}
	}

	// Now run a flow with 4 separate table readers, each with its own local
	// stage, all feeding into a single final stage.

	numParallel := 4

	// The type(s) outputted by the local stage can be different than the input type
	// (e.g. DECIMAL instead of INT).
	intermediaryTypes := make([]sqlbase.ColumnType, numIntermediary)
	for i, fn := range info.LocalStage {
		var err error
		_, intermediaryTypes[i], err = distsqlpb.GetAggregateInfo(fn, colType)
		if err != nil {
			t.Fatal(err)
		}
	}

	localAggregations := make([]distsqlpb.AggregatorSpec_Aggregation, numIntermediary)
	for i, fn := range info.LocalStage {
		// Local aggregations have the same input.
		localAggregations[i] = distsqlpb.AggregatorSpec_Aggregation{Func: fn, ColIdx: []uint32{0}}
	}
	finalAggregations := make([]distsqlpb.AggregatorSpec_Aggregation, numFinal)
	for i, finalInfo := range info.FinalStage {
		// Each local aggregation feeds into a final aggregation.
		finalAggregations[i] = distsqlpb.AggregatorSpec_Aggregation{
			Func:   finalInfo.Fn,
			ColIdx: finalInfo.LocalIdxs,
		}
	}

	if numParallel < numRows {
		numParallel = numRows
	}
	finalProc := distsqlpb.ProcessorSpec{
		Input: []distsqlpb.InputSyncSpec{{
			Type:        distsqlpb.InputSyncSpec_UNORDERED,
			ColumnTypes: intermediaryTypes,
		}},
		Core: distsqlpb.ProcessorCoreUnion{Aggregator: &distsqlpb.AggregatorSpec{
			Aggregations: finalAggregations,
		}},
		Output: []distsqlpb.OutputRouterSpec{{
			Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
			Streams: []distsqlpb.StreamEndpointSpec{
				{Type: distsqlpb.StreamEndpointSpec_SYNC_RESPONSE},
			},
		}},
	}

	// The type(s) outputted by the final stage can be different than the
	// input type (e.g. DECIMAL instead of INT).
	finalOutputTypes := make([]sqlbase.ColumnType, numFinal)
	// Passed into FinalIndexing as the indices for the IndexedVars inputs
	// to the post processor.
	varIdxs := make([]int, numFinal)
	for i, finalInfo := range info.FinalStage {
		inputTypes := make([]sqlbase.ColumnType, len(finalInfo.LocalIdxs))
		for i, localIdx := range finalInfo.LocalIdxs {
			inputTypes[i] = intermediaryTypes[localIdx]
		}
		var err error
		_, finalOutputTypes[i], err = distsqlpb.GetAggregateInfo(finalInfo.Fn, inputTypes...)
		if err != nil {
			t.Fatal(err)
		}
		varIdxs[i] = i
	}

	var procs []distsqlpb.ProcessorSpec
	for i := 0; i < numParallel; i++ {
		tr := makeTableReader(1+i*numRows/numParallel, 1+(i+1)*numRows/numParallel, 2*i)
		agg := distsqlpb.ProcessorSpec{
			Input: []distsqlpb.InputSyncSpec{{
				Type:        distsqlpb.InputSyncSpec_UNORDERED,
				ColumnTypes: []sqlbase.ColumnType{colType},
				Streams: []distsqlpb.StreamEndpointSpec{
					{Type: distsqlpb.StreamEndpointSpec_LOCAL, StreamID: distsqlpb.StreamID(2 * i)},
				},
			}},
			Core: distsqlpb.ProcessorCoreUnion{Aggregator: &distsqlpb.AggregatorSpec{
				Aggregations: localAggregations,
			}},
			Output: []distsqlpb.OutputRouterSpec{{
				Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
				Streams: []distsqlpb.StreamEndpointSpec{
					{Type: distsqlpb.StreamEndpointSpec_LOCAL, StreamID: distsqlpb.StreamID(2*i + 1)},
				},
			}},
		}
		procs = append(procs, tr, agg)
		finalProc.Input[0].Streams = append(finalProc.Input[0].Streams, distsqlpb.StreamEndpointSpec{
			Type:     distsqlpb.StreamEndpointSpec_LOCAL,
			StreamID: distsqlpb.StreamID(2*i + 1),
		})
	}

	if info.FinalRendering != nil {
		h := tree.MakeTypesOnlyIndexedVarHelper(sqlbase.ColumnTypesToDatumTypes(finalOutputTypes))
		renderExpr, err := info.FinalRendering(&h, varIdxs)
		if err != nil {
			t.Fatal(err)
		}
		var expr distsqlpb.Expression
		expr, err = distsqlplan.MakeExpression(renderExpr, fakeExprContext{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		finalProc.Post.RenderExprs = []distsqlpb.Expression{expr}

	}

	procs = append(procs, finalProc)
	rowsDist := RunTestFlow(t, srv, txn, procs...)

	if len(rowsDist[0]) != len(rowsNonDist[0]) {
		t.Errorf("different row lengths (dist: %d non-dist: %d)", len(rowsDist[0]), len(rowsNonDist[0]))
	} else {
		for i := range rowsDist[0] {
			rowDist := rowsDist[0][i]
			rowNonDist := rowsNonDist[0][i]
			if !rowDist.Datum.ResolvedType().FamilyEqual(rowNonDist.Datum.ResolvedType()) {
				t.Fatalf("different type for column %d (dist: %s non-dist: %s)", i, rowDist.Datum.ResolvedType(), rowNonDist.Datum.ResolvedType())
			}

			var equiv bool
			var strDist, strNonDist string
			switch typedDist := rowDist.Datum.(type) {
			case *tree.DDecimal:
				// For some decimal operations, non-local and
				// local computations may differ by the last
				// digit (by 1 ulp).
				decDist := &typedDist.Decimal
				decNonDist := &rowNonDist.Datum.(*tree.DDecimal).Decimal
				strDist = decDist.String()
				strNonDist = decNonDist.String()
				// We first check if they're equivalent, and if
				// not, we check if they're within 1ulp.
				equiv = decDist.Cmp(decNonDist) == 0
				if !equiv {
					if _, err := diffCtx.Sub(decNonDist, decNonDist, decDist); err != nil {
						t.Fatal(err)
					}
					equiv = decNonDist.Coeff.Cmp(bigOne) == 0
				}
			case *tree.DFloat:
				// Float results are highly variable and
				// loss of precision between non-local and
				// local is expected. We reduce the precision
				// specified by floatPrecFmt and compare
				// their string representations.
				floatDist := float64(*typedDist)
				floatNonDist := float64(*rowNonDist.Datum.(*tree.DFloat))
				strDist = fmt.Sprintf(floatPrecFmt, floatDist)
				strNonDist = fmt.Sprintf(floatPrecFmt, floatNonDist)
				equiv = strDist == strNonDist
			default:
				// For all other types, a simple string
				// representation comparison will suffice.
				strDist = rowDist.Datum.String()
				strNonDist = rowNonDist.Datum.String()
				equiv = strDist == strNonDist
			}
			if !equiv {
				t.Errorf("different results for column %d\nw/o local stage:   %s\nwith local stage:  %s", i, strDist, strNonDist)
			}
		}
	}
}

// Test that distributing agg functions according to DistAggregationTable
// yields correct results. We're going to run each aggregation as either the
// two-stage process described by the DistAggregationTable or as a single global
// process, and verify that the results are the same.
func TestDistAggregationTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numRows = 100

	tc := serverutils.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.TODO())

	// Create a table with a few columns:
	//  - random integer values from 0 to numRows
	//  - random integer values (with some NULLs)
	//  - random bool value (mostly false)
	//  - random bool value (mostly true)
	//  - random decimals
	//  - random decimals (with some NULLs)
	rng, _ := randutil.NewPseudoRand()
	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t",
		"k INT PRIMARY KEY, int1 INT, int2 INT, bool1 BOOL, bool2 BOOL, dec1 DECIMAL, dec2 DECIMAL, float1 FLOAT, float2 FLOAT, b BYTES",
		numRows,
		func(row int) []tree.Datum {
			return []tree.Datum{
				tree.NewDInt(tree.DInt(row)),
				tree.NewDInt(tree.DInt(rng.Intn(numRows))),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}, true),
				tree.MakeDBool(tree.DBool(rng.Intn(10) == 0)),
				tree.MakeDBool(tree.DBool(rng.Intn(10) != 0)),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}, false),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}, true),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}, false),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}, true),
				tree.NewDBytes(tree.DBytes(randutil.RandBytes(rng, 10))),
			}
		},
	)

	kvDB := tc.Server(0).DB()
	desc := sqlbase.GetTableDescriptor(kvDB, "test", "public", "t")

	for fn, info := range DistAggregationTable {
		if fn == distsqlpb.AggregatorSpec_ANY_NOT_NULL {
			// ANY_NOT_NULL only has a definite result if all rows have the same value
			// on the relevant column; skip testing this trivial case.
			continue
		}
		if fn == distsqlpb.AggregatorSpec_COUNT_ROWS {
			// COUNT_ROWS takes no arguments; skip it in this test.
			continue
		}
		// We're going to test each aggregation function on every column that can be
		// used as input for it.
		foundCol := false
		for colIdx := 1; colIdx < len(desc.Columns); colIdx++ {
			// See if this column works with this function.
			_, _, err := distsqlpb.GetAggregateInfo(fn, desc.Columns[colIdx].Type)
			if err != nil {
				continue
			}
			foundCol = true
			for _, numRows := range []int{5, numRows / 10, numRows / 2, numRows} {
				name := fmt.Sprintf("%s/%s/%d", fn, desc.Columns[colIdx].Name, numRows)
				t.Run(name, func(t *testing.T) {
					checkDistAggregationInfo(
						context.Background(), t, tc.Server(0), desc, colIdx, numRows, fn, info)
				})
			}
		}
		if !foundCol {
			t.Errorf("aggregation function %s was not tested (no suitable column)", fn)
		}
	}
}

func RunTestFlow(
	t *testing.T,
	srv serverutils.TestServerInterface,
	txn *client.Txn,
	procs ...distsqlpb.ProcessorSpec,
) sqlbase.EncDatumRows {
	distSQLSrv := srv.DistSQLServer().(*distsql.ServerImpl)

	txnCoordMeta := txn.GetTxnCoordMeta(context.TODO())
	txnCoordMeta.StripRootToLeaf()
	req := distsqlpb.SetupFlowRequest{
		Version:      22,
		TxnCoordMeta: &txnCoordMeta,
		Flow: distsqlpb.FlowSpec{
			FlowID:     distsqlpb.FlowID{UUID: uuid.MakeV4()},
			Processors: procs,
		},
	}

	var rowBuf runbase.RowBuffer

	ctx, flow, err := distSQLSrv.SetupSyncFlow(context.TODO(), distSQLSrv.ParentMemoryMonitor, &req, &rowBuf)
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.Start(ctx, func() {}); err != nil {
		t.Fatal(err)
	}
	flow.Wait()
	flow.Cleanup(ctx, false)

	if !rowBuf.ProducerClosed() {
		t.Errorf("output not closed")
	}

	var res sqlbase.EncDatumRows
	for {
		row, meta := rowBuf.Next()
		if meta != nil {
			if meta.TxnCoordMeta != nil {
				continue
			}
			t.Fatalf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		res = append(res, row)
	}

	return res
}

// FinalStageInfo is a wrapper around an aggregation function performed
// in the final stage of distributed aggregations that allows us to specify the
// corresponding inputs from the local aggregations by their indices in the LocalStage.
type FinalStageInfo struct {
	Fn distsqlpb.AggregatorSpec_Func
	// Specifies the ordered slice of outputs from local aggregations to propagate
	// as inputs to Fn. This must be ordered according to the underlying aggregate builtin
	// arguments signature found in aggregate_builtins.go.
	LocalIdxs []uint32
}

// DistAggregationInfo is a blueprint for planning distributed aggregations. It
// describes two stages - a local stage performs local aggregations wherever
// data is available and generates partial results, and a final stage aggregates
// the partial results from all data "partitions".
//
// The simplest example is SUM: the local stage computes the SUM of the items
// on each node, and a final stage SUMs those partial sums into a final sum.
// Similar functions are MIN, MAX, BOOL_AND, BOOL_OR.
//
// A less trivial example is COUNT: the local stage counts (COUNT), the final stage
// adds the counts (SUM_INT).
//
// A more complex example is AVG, for which we have to do *multiple*
// aggregations in each stage: we need to get a sum and a count, so the local
// stage does SUM and COUNT, and the final stage does SUM and SUM_INT. We also
// need an expression that takes these two values and generates the final AVG
// result.
type DistAggregationInfo struct {
	// The local stage consists of one or more aggregations. All aggregations have
	// the same input.
	LocalStage []distsqlpb.AggregatorSpec_Func

	// The final stage consists of one or more aggregations that take in an
	// arbitrary number of inputs from the local stages. The inputs are ordered and
	// mapped by the indices of the local aggregations in LocalStage (specified by
	// LocalIdxs).
	FinalStage []FinalStageInfo

	// An optional rendering expression used to obtain the final result; required
	// if there is more than one aggregation in each of the stages.
	//
	// Conceptually this is an expression that has access to the final stage
	// results (via IndexedVars), to be run as the PostProcessing step of the
	// final stage processor.  However, there are some complications:
	//   - this structure is a blueprint for aggregating inputs of different
	//     types, and in some cases the expression may be different depending on
	//     the types (see AVG below).
	//   - we support comznbaseng multiple "top level" aggregations into the same
	//     processors, so the correct indexing of the input variables is not
	//     predetermined.
	//
	// Instead of defining a canonical non-typed expression and then tweaking it
	// with visitors, we use a function that directly creates a typed expression
	// on demand. The expression will refer to the final stage results using
	// IndexedVars, with indices specified by varIdxs (1-1 mapping).
	FinalRendering func(h *tree.IndexedVarHelper, varIdxs []int) (tree.TypedExpr, error)
}

// Convenient value for FinalStageInfo.LocalIdxs when there is only one aggregation
// function in each of the LocalStage and FinalStage. Otherwise, specify the explicit
// index corresponding to the local stage.
var passThroughLocalIdxs = []uint32{0}

// DistAggregationTable is DistAggregationInfo look-up table. Functions that
// don't have an entry in the table are not optimized with a local stage.
var DistAggregationTable = map[distsqlpb.AggregatorSpec_Func]DistAggregationInfo{
	distsqlpb.AggregatorSpec_ANY_NOT_NULL: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_ANY_NOT_NULL},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_ANY_NOT_NULL,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlpb.AggregatorSpec_BOOL_AND: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_BOOL_AND},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_BOOL_AND,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlpb.AggregatorSpec_BOOL_OR: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_BOOL_OR},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_BOOL_OR,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlpb.AggregatorSpec_COUNT: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_COUNT},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_SUM_INT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlpb.AggregatorSpec_COUNT_ROWS: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_COUNT_ROWS},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_SUM_INT,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlpb.AggregatorSpec_MAX: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_MAX},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_MAX,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlpb.AggregatorSpec_MIN: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_MIN},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_MIN,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlpb.AggregatorSpec_SUM: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_SUM},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_SUM,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	distsqlpb.AggregatorSpec_XOR_AGG: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{distsqlpb.AggregatorSpec_XOR_AGG},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_XOR_AGG,
				LocalIdxs: passThroughLocalIdxs,
			},
		},
	},

	// AVG is more tricky than the ones above; we need two intermediate values in
	// the local and final stages:
	//  - the local stage accumulates the SUM and the COUNT;
	//  - the final stage sums these partial results (SUM and SUM_INT);
	//  - a final rendering then divides the two results.
	//
	// At a high level, this is analogous to rewriting AVG(x) as SUM(x)/COUNT(x).
	distsqlpb.AggregatorSpec_AVG: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{
			distsqlpb.AggregatorSpec_SUM,
			distsqlpb.AggregatorSpec_COUNT,
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_SUM,
				LocalIdxs: []uint32{0},
			},
			{
				Fn:        distsqlpb.AggregatorSpec_SUM_INT,
				LocalIdxs: []uint32{1},
			},
		},
		FinalRendering: func(h *tree.IndexedVarHelper, varIdxs []int) (tree.TypedExpr, error) {
			if len(varIdxs) < 2 {
				panic("fewer than two final aggregation values passed into final render")
			}
			sum := h.IndexedVar(varIdxs[0])
			count := h.IndexedVar(varIdxs[1])

			expr := &tree.BinaryExpr{
				Operator: tree.Div,
				Left:     sum,
				Right:    count,
			}

			// There is no "FLOAT / INT" operator; cast the denominator to float in
			// this case. Note that there is a "DECIMAL / INT" operator, so we don't
			// need the same handling for that case.
			if sum.ResolvedType().Equivalent(types.Float) {
				expr.Right = &tree.CastExpr{
					Expr: count,
					Type: coltypes.Float8,
				}
			}
			ctx := &tree.SemaContext{IVarContainer: h.Container()}
			return expr.TypeCheck(ctx, types.Any, false)
		},
	},

	// For VARIANCE/STDDEV the local stage consists of three aggregations,
	// and the final stage aggregation uses all three values.
	// respectively:
	//  - the local stage accumulates the SQRDIFF, SUM and the COUNT
	//  - the final stage calculates the FINAL_(VARIANCE|STDDEV)
	//
	// At a high level, this is analogous to rewriting VARIANCE(x) as
	// SQRDIFF(x)/(COUNT(x) - 1) (and STDDEV(x) as sqrt(VARIANCE(x))).
	distsqlpb.AggregatorSpec_VARIANCE: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{
			distsqlpb.AggregatorSpec_SQRDIFF,
			distsqlpb.AggregatorSpec_SUM,
			distsqlpb.AggregatorSpec_COUNT,
		},
		// Instead of have a SUM_SQRDIFFS and SUM_INT (for COUNT) stage
		// for VARIANCE (and STDDEV) then tailoring a FinalRendering
		// stage specific to each, it is better to use a specific
		// FINAL_(VARIANCE|STDDEV) aggregation stage: - For underlying
		// Decimal results, it is not possible to reduce trailing zeros
		// since the expression is wrapped in IndexVar. Taking the
		// BinaryExpr Pow(0.5) for STDDEV would result in trailing
		// zeros which is not ideal.
		// TODO(richardwu): Consolidate FinalStage and FinalRendering:
		// have one or the other
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_FINAL_VARIANCE,
				LocalIdxs: []uint32{0, 1, 2},
			},
		},
	},

	distsqlpb.AggregatorSpec_STDDEV: {
		LocalStage: []distsqlpb.AggregatorSpec_Func{
			distsqlpb.AggregatorSpec_SQRDIFF,
			distsqlpb.AggregatorSpec_SUM,
			distsqlpb.AggregatorSpec_COUNT,
		},
		FinalStage: []FinalStageInfo{
			{
				Fn:        distsqlpb.AggregatorSpec_FINAL_STDDEV,
				LocalIdxs: []uint32{0, 1, 2},
			},
		},
	},
}
