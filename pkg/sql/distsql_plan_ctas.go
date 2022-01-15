// Copyright 2020 The Bidb Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// PlanAndRunCTAS plans and runs the CREATE TABLE AS command.
func (dsp *DistSQLPlanner) PlanAndRunCTAS(
	ctx context.Context,
	planner *planner,
	txn *client.Txn,
	local bool,
	in planMaybePhysical,
	out distsqlpb.ProcessorCoreUnion,
	recv *DistSQLReceiver,
) {
	planCtx := dsp.NewPlanningCtx1(ctx, planner.ExtendedEvalContext(), planner, txn, !local)
	planCtx.stmtType = tree.Rows

	physPlan, err := dsp.createPhysPlan(planCtx, in)
	if err != nil {
		recv.SetError(errors.Wrapf(err, "constructing distSQL plan"))
		return
	}
	physPlan.AddNoGroupingStage(
		out, distsqlpb.PostProcessSpec{}, physPlan.ResultTypes, distsqlpb.Ordering{},
	)

	// The bulk row writers will emit a binary encoded BulkOpSummary.
	physPlan.PlanToStreamColMap = []int{0}

	// Make copy of evalCtx as Run might modify it.
	evalCtxCopy := planner.ExtendedEvalContextCopy()
	dsp.FinalizePlan(planCtx, &physPlan)
	dsp.Run(planCtx, txn, &physPlan, recv, evalCtxCopy, nil /* finishedSetupFn */)
}
