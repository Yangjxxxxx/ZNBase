package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestHint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := "CREATE DATABASE t;" +
		"CREATE TABLE t.hint_test(a int, b int, c int, INDEX a_idx(a), INDEX b_idx(b));" +
		"CREATE TABLE t.hint_join(a int, b int, c int);"
	testCases := []*TestData{
		{
			SQL: "SELECT /*+ USE_INDEX(hint_test, a_idx) */ * FROM t.hint_test;",
			ExpectedPlanString: `0 index-join  (a int, b int, c int) 
0 .table hint_test@primary (a int, b int, c int) 
1 scan  (a int, b int, c int) 
1 .table hint_test@a_idx (a int, b int, c int) 
1 .spans ALL (a int, b int, c int) 
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "index-join",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "table",
						Value: "hint_test@primary",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "scan",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "table",
								Value: "hint_test@a_idx",
							},
							{
								Key:   "spans",
								Value: "ALL",
							},
						},
					},
				},
			},
		},
		{
			SQL: "SELECT /*+ USE_INDEX(hint_test, a_idx) */ a FROM t.hint_test;",
			ExpectedPlanString: `0 scan  (a int) 
0 .table hint_test@a_idx (a int) 
0 .spans ALL (a int) 
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "scan",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "table",
						Value: "hint_test@a_idx",
					},
					{
						Key:   "spans",
						Value: "ALL",
					},
				},
			},
		},
		{
			SQL: "SELECT /*+ merge_join(hint_test, hint_join) */ * FROM t.hint_test, t.hint_join;",
			ExpectedPlanString: `0 merge-join  (a int, b int, c int, a int, b int, c int) 
0 .type inner (a int, b int, c int, a int, b int, c int) 
0 .equality (a) = (a) (a int, b int, c int, a int, b int, c int) 
0 .mergeJoinOrder +"(a=a)" (a int, b int, c int, a int, b int, c int) 
1 scan  (a int, b int, c int, a int, b int, c int) 
1 .table hint_test@primary (a int, b int, c int, a int, b int, c int) 
1 .spans ALL (a int, b int, c int, a int, b int, c int) 
1 scan  (a int, b int, c int, a int, b int, c int) 
1 .table hint_join@primary (a int, b int, c int, a int, b int, c int) 
1 .spans ALL (a int, b int, c int, a int, b int, c int) 
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "merge-join",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "type",
						Value: "inner",
					},
					{
						Key:   "equality",
						Value: "(a) = (a)",
					},
					{
						Key:   "mergeJoinOrder",
						Value: "+\"(a=a)\"",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "scan",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "table",
								Value: "hint_test@primary",
							},
							{
								Key:   "spans",
								Value: "ALL",
							},
						},
					},
					{
						Name: "scan",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "table",
								Value: "hint_join@primary",
							},
							{
								Key:   "spans",
								Value: "ALL",
							},
						},
					},
				},
			},
		},
		{
			SQL: "SELECT /*+ hash_join(hint_test, hint_join) */ * FROM t.hint_test, t.hint_join;",
			ExpectedPlanString: `0 hash-join  (a int, b int, c int, a int, b int, c int) 
0 .type cross (a int, b int, c int, a int, b int, c int) 
0 .can parallel true (a int, b int, c int, a int, b int, c int) 
1 scan  (a int, b int, c int, a int, b int, c int) 
1 .table hint_test@primary (a int, b int, c int, a int, b int, c int) 
1 .spans ALL (a int, b int, c int, a int, b int, c int) 
1 scan  (a int, b int, c int, a int, b int, c int) 
1 .table hint_join@primary (a int, b int, c int, a int, b int, c int) 
1 .spans ALL (a int, b int, c int, a int, b int, c int) 
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "hash-join",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "type",
						Value: "cross",
					},
					{
						Key:   "can parallel",
						Value: "true",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "scan",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "table",
								Value: "hint_test@primary",
							},
							{
								Key:   "spans",
								Value: "ALL",
							},
						},
					},
					{
						Name: "scan",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "table",
								Value: "hint_join@primary",
							},
							{
								Key:   "spans",
								Value: "ALL",
							},
						},
					},
				},
			},
		},
		{
			SQL: "SELECT * FROM t.hint_test, t.hint_join;",
			ExpectedPlanString: `0 hash-join  (a int, b int, c int, a int, b int, c int) 
0 .type cross (a int, b int, c int, a int, b int, c int) 
0 .can parallel true (a int, b int, c int, a int, b int, c int) 
1 scan  (a int, b int, c int, a int, b int, c int) 
1 .table hint_test@primary (a int, b int, c int, a int, b int, c int) 
1 .spans ALL (a int, b int, c int, a int, b int, c int) 
1 scan  (a int, b int, c int, a int, b int, c int) 
1 .table hint_join@primary (a int, b int, c int, a int, b int, c int) 
1 .spans ALL (a int, b int, c int, a int, b int, c int) 
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "hash-join",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "type",
						Value: "cross",
					},
					{
						Key:   "can parallel",
						Value: "true",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "scan",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "table",
								Value: "hint_test@primary",
							},
							{
								Key:   "spans",
								Value: "ALL",
							},
						},
					},
					{
						Name: "scan",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "table",
								Value: "hint_join@primary",
							},
							{
								Key:   "spans",
								Value: "ALL",
							},
						},
					},
				},
			},
		},
	}
	assertExpectedOptPlansForTests(t, s, testCases)
}

func assertExpectedOptPlansForTests(t *testing.T, sqlSetup string, plansToTest []*TestData) {
	// Setup.
	ctx := context.Background()
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.ExecContext(
		ctx, sqlSetup); err != nil {
		t.Fatal(err)
	}
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		client.NewTxn(ctx, db, s.NodeID(), client.RootTxn),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()
	if p, ok := internalPlanner.(*planner); ok {
		// Run planToTree() and assert expected value for each plan in plansToTest.
		for _, test := range plansToTest {
			stmt, err := parser.ParseOne(test.SQL, false)
			if err != nil {
				t.Fatal(err)
			}
			p.stmt = &Statement{Statement: stmt}
			p.optPlanningCtx.init(p)
			if err := p.makeOptimizerPlan(ctx); err != nil {
				t.Fatal(err)
			}
			actualPlanTree := planToTree(ctx, &p.curPlan)
			assert.Equal(t, test.ExpectedPlanTree, actualPlanTree,
				"planToTree for %s:\nexpected:%s\nactual:%s", test.SQL, test.ExpectedPlanTree, actualPlanTree)
			actualPlanString := planToString(ctx, p.curPlan.main.planNode, p.curPlan.subqueryPlans)
			assert.Equal(t, test.ExpectedPlanString, actualPlanString,
				"planToString for %s:\nexpected:%s\nactual:%s", test.SQL, test.ExpectedPlanString, actualPlanString)
		}
	}
}
