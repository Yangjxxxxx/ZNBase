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

	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

type refreshMaterializedViewNode struct {
	n    *tree.RefreshMaterializedView
	desc *sqlbase.MutableTableDescriptor
}

func (p *planner) RefreshMaterializedView(
	ctx context.Context, n *tree.RefreshMaterializedView,
) (planNode, error) {
	if !p.EvalContext().TxnImplicit {
		return nil, pgerror.Newf(pgcode.InvalidTransactionState, "cannot refresh view in an explicit transaction")
	}
	desc, err := p.ResolveMutableTableDescriptorEx(ctx, n.Name, true /* required */, requireViewDesc)
	if err != nil {
		return nil, err
	}
	if !desc.MaterializedView() {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", desc.Name)
	}
	// TODO (rohany): Not sure if this is a real restriction, but let's start with
	//  it to be safe.
	for i := range desc.Mutations {
		mut := &desc.Mutations[i]
		if mut.GetMaterializedViewRefresh() != nil {
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "view is already being refreshed")
		}
	}

	// Only the owner or an admin (superuser) can refresh the view.
	hasOwnership, err := p.HasOwnership(ctx, desc)
	if err != nil {
		return nil, err
	}

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}

	if !(hasOwnership || hasAdminRole) {
		return nil, pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"must be owner of materialized view %s",
			desc.Name,
		)
	}
	return &refreshMaterializedViewNode{n: n, desc: desc}, nil
}

func (n *refreshMaterializedViewNode) startExec(params runParams) error {
	// We refresh a materialized view by creating a new set of indexes to write
	// the result of the view query into. The existing set of indexes will remain
	// present and readable so that reads of the view during the refresh operation
	// will return consistent data. The schema change process will backfill the
	// results of the view query into the new set of indexes, and then change the
	// set of indexes over to the new set of indexes atomically.

	// Prepare the new set of indexes by cloning all existing indexes on the view.
	newPrimaryIndex := protoutil.Clone(&n.desc.PrimaryIndex).(*sqlbase.IndexDescriptor)
	newIndexes := make([]sqlbase.IndexDescriptor, len(n.desc.Indexes))
	for i := range n.desc.Indexes {
		newIndexes[i] = *protoutil.Clone(&n.desc.Indexes[i]).(*sqlbase.IndexDescriptor)
	}

	// Reset and allocate new IDs for the new indexes.
	getID := func() sqlbase.IndexID {
		res := n.desc.NextIndexID
		n.desc.NextIndexID++
		return res
	}
	newPrimaryIndex.ID = getID()
	for i := range newIndexes {
		newIndexes[i].ID = getID()
	}

	// Queue the refresh mutation.
	n.desc.AddMaterializedViewRefreshMutation(&sqlbase.MaterializedViewRefresh{
		NewPrimaryIndex: *newPrimaryIndex,
		NewIndexes:      newIndexes,
		AsOf:            params.p.Txn().ReadTimestamp(),
	})
	mutationID, err := params.p.createOrUpdateSchemaChangeJob(params.ctx, n.desc,
		tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType))
	if err != nil {
		return err
	}

	return params.p.writeSchemaChange(
		params.ctx,
		n.desc,
		mutationID,
	)
}

func (n *refreshMaterializedViewNode) Next(params runParams) (bool, error) { return false, nil }
func (n *refreshMaterializedViewNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *refreshMaterializedViewNode) Close(ctx context.Context)           {}
