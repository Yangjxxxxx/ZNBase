package sql

import (
	"context"
	"fmt"

	types2 "github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/concurrency/lock"
)

type queryLockstatNode struct {
	rows    planNode
	results *valuesNode
}

var txnMessageHeader = []string{"txn_id", "txn_key", "txn_name", "is_active", "txn_type", "isolation_level", "Lock_Durability_Replicated", "Lock_Durability_Unreplicated", "lock_strength", "lock_level"}

func (p *planner) QueryLockstat(ctx context.Context, n *tree.QueryLockstat) (planNode, error) {
	rows, err := p.newPlan(ctx, n.Txnid, []types.T{types.String})
	if err != nil {
		return nil, err
	}
	cols := planColumns(rows)
	if len(cols) != 1 {
		return nil, errors.Errorf("QUERY LOCKSTAT expects a single column source, got %d columns", len(cols))
	}
	if !cols[0].Typ.Equivalent(types.String) {
		return nil, errors.Errorf("QUERY TRANSACTIONS requires string values, not type %s", cols[0].Typ)
	}

	resultColumns := sqlbase.ResultColumns{}

	for _, colName := range txnMessageHeader {
		resultColumns = append(resultColumns, sqlbase.ResultColumn{Name: colName, Typ: types.String})
	}

	return &queryLockstatNode{
		rows:    rows,
		results: p.newContainerValuesNode(resultColumns, 0),
	}, nil
}

func (q *queryLockstatNode) startExec(params runParams) error {
	if ok, err := q.rows.Next(params); err != nil || !ok {
		return err
	}

	datum := q.rows.Values()[0]
	if datum == tree.DNull {
		return nil
	}

	request := &serverpb.QueryLockstatRequest{TxnId: datum.String(), Username: params.SessionData().User}
	statusServer := params.extendedEvalCtx.StatusServer
	spanResponse, err := statusServer.QueryLockstat(params.ctx, request)
	if err != nil {
		return err
	}

	if spanResponse == nil {
		return fmt.Errorf("query ID %s not found", datum.String())
	}

	replspan, unreplspan := make(map[string]roachpb.Span), make(map[string]roachpb.Span)
	var replspanStr, unreplspanStr string
	if spanResponse.WriteSpans != nil {
		batch := params.p.txn.NewBatch()
		for _, span := range spanResponse.WriteSpans {
			if len(span.EndKey) != 0 {
				batch.QueryLock(span.Key, span.EndKey, spanResponse.TxnId, false)
			} else {
				endKey := roachpb.Key(span.Key).Next()
				batch.QueryLock(span.Key, endKey, spanResponse.TxnId, false)
			}
		}
		err = params.p.txn.Run(params.ctx, batch)

		batchResponse := batch.RawResponse()
		if batchResponse != nil {
			unions := batchResponse.Responses
			if unions != nil {
				for _, union := range unions {
					queryLockResponse := union.GetQueryLock()
					if queryLockResponse != nil {
						if queryLockResponse.DurabilityReplicated {
							replspan[queryLockResponse.KVspan.String()] = *queryLockResponse.KVspan
						}
						if queryLockResponse.DurabilityUnreplicated {
							unreplspan[queryLockResponse.KVspan.String()] = *queryLockResponse.KVspan
						}
					}
				}
			}
		}

		for k := range replspan {
			replspanStr = appendStr(replspanStr, k)
		}

		for k := range unreplspan {
			unreplspanStr = appendStr(unreplspanStr, k)
		}
	}

	if spanResponse.WriteSpans != nil {
		intentSet := make(map[string]types2.Empty)
		scanIntentBatch := params.p.Txn().NewBatch()
		for _, span := range spanResponse.WriteSpans {
			scanIntentBatch.QueryLock(span.Key, span.EndKey, spanResponse.TxnId, true)
		}
		err = params.p.Txn().Run(params.ctx, scanIntentBatch)
		if err != nil {
			return err
		}
		scanIntentBatchResponse := scanIntentBatch.RawResponse()
		if scanIntentBatchResponse != nil {
			unions := scanIntentBatchResponse.Responses
			if unions != nil {
				for _, union := range unions {
					queryLockResponse := union.GetQueryLock()
					if queryLockResponse != nil {
						for _, key := range queryLockResponse.IntentKeys {
							intentSet[roachpb.Key(key).String()] = types2.Empty{}
						}
					}
				}
			}
		}
		for k := range intentSet {
			replspanStr = appendStr(replspanStr, k)
		}
	}

	var lockStrength lock.Strength
	var lockLevel string
	if (replspanStr == "" || replspanStr == "/Min") && (unreplspanStr == "" || unreplspanStr == "/Min") {
		lockStrength = -1
		lockLevel = ""
	} else {
		lockStrength = lock.Exclusive
		lockLevel = "row_lock"
	}

	_, err = q.results.rows.AddRow(params.ctx, tree.Datums{
		tree.NewDString(spanResponse.TxnId),
		tree.NewDString(spanResponse.TxnKey),
		tree.NewDString(spanResponse.TxnName),
		tree.NewDString(spanResponse.TxnIsActive),
		tree.NewDString(spanResponse.TxnType),
		tree.NewDString(spanResponse.TxnIsolationLevel),
		tree.NewDString(replspanStr),
		tree.NewDString(unreplspanStr),
		tree.NewDString(lock.Strength_name[int32(lockStrength)]),
		tree.NewDString(lockLevel),
	})
	if err != nil {
		return err
	}
	return nil
}

func (q *queryLockstatNode) Next(params runParams) (bool, error) {
	return q.results.Next(params)
}

func (q *queryLockstatNode) Values() tree.Datums {
	return q.results.Values()
}

func (q *queryLockstatNode) Close(ctx context.Context) {
	q.rows.Close(ctx)
	q.results.Close(ctx)
}

func appendStr(mainStr, subStr string) string {
	if mainStr == "" || mainStr == "/Min" {
		mainStr = subStr
	} else {
		mainStr = mainStr + "," + subStr
	}
	return mainStr
}
