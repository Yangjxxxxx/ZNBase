// Copyright 2017 The Cockroach Authors.
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
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgnotice"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type dropIndexNode struct {
	n        *tree.DropIndex
	idxNames []fullIndexName
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//   Notes: postgres allows only the index owner to DROP an index.
//          mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(ctx context.Context, n *tree.DropIndex) (planNode, error) {
	// Keep a track of the indexes that exist to check. When the IF EXISTS
	// options are provided, we will simply not include any indexes that
	// don't exist and continue execution.
	idxNames := make([]fullIndexName, 0, len(n.IndexList))
	for _, index := range n.IndexList {
		tn, tableDesc, err := expandMutableIndexName(ctx, p, index, !n.IfExists /* requireTable */)
		if err != nil {
			// Error or table did not exist.
			return nil, err
		}
		if tableDesc == nil {
			// IfExists specified and table did not exist.
			continue
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.REFERENCES); err != nil {
			return nil, err
		}

		idxNames = append(idxNames, fullIndexName{tn: tn, idxName: index.Index})
	}
	return &dropIndexNode{n: n, idxNames: idxNames}, nil
}

func (n *dropIndexNode) startExec(params runParams) error {
	ctx := params.ctx
	for _, index := range n.idxNames {
		// Need to retrieve the descriptor again for each index name in
		// the list: when two or more index names refer to the same table,
		// the mutation list and new version number created by the first
		// drop need to be visible to the second drop.
		tableDesc, err := params.p.ResolveMutableTableDescriptor(
			ctx, index.tn, true /*required*/, requireTableOrViewDesc)
		if err != nil {
			// Somehow the descriptor we had during newPlan() is not there
			// any more.
			return errors.Wrapf(err, "table descriptor for %q became unavailable within same txn",
				tree.ErrString(index.tn))
		}

		if tableDesc.IsView() && !tableDesc.MaterializedView() {
			return pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
		}

		if err := params.p.dropIndexByName(
			ctx, index.tn, index.idxName, tableDesc, n.n.IfExists, n.n.DropBehavior, checkIdxConstraint,
			tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType),
		); err != nil {
			return err
		}
	}

	return nil
}

func (*dropIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*dropIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropIndexNode) Close(context.Context)        {}

type fullIndexName struct {
	tn      *tree.TableName
	idxName tree.UnrestrictedName
}

// dropIndexConstraintBehavior is used when dropping an index to signal whether
// it is okay to do so even if it is in use as a constraint (outbound FK or
// unique). This is a subset of what is implied by DropBehavior CASCADE, which
// implies dropping *all* dependencies. This is used e.g. when the element
// constrained is being dropped anyway.
type dropIndexConstraintBehavior bool

const (
	checkIdxConstraint  dropIndexConstraintBehavior = true
	ignoreIdxConstraint dropIndexConstraintBehavior = false
)

func (p *planner) dropIndexByName(
	ctx context.Context,
	tn *tree.TableName,
	idxName tree.UnrestrictedName,
	tableDesc *sqlbase.MutableTableDescriptor,
	ifExists bool,
	behavior tree.DropBehavior,
	constraintBehavior dropIndexConstraintBehavior,
	jobDesc string,
) error {
	idx, dropped, err := tableDesc.FindIndexByName(string(idxName))
	if err != nil {
		// Only index names of the form "table@idx" throw an error here if they
		// don't exist.
		if ifExists {
			// Noop.
			return nil
		}
		// Index does not exist, but we want it to: error out.
		return err
	}
	if dropped {
		return nil
	}
	// Check if requires ICL binary for eventual zone config removal.
	_, zone, _, err := GetZoneConfigInTxn(ctx, p.txn, uint32(tableDesc.ID), nil, "", false)
	if err != nil {
		return err
	}

	for _, s := range zone.Subzones {
		if s.IndexID != uint32(idx.ID) {
			_, err = GenerateSubzoneSpans(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), tableDesc.TableDesc(), zone.Subzones, false /* newSubzones */)
			if sqlbase.IsICLRequiredError(err) {
				return sqlbase.NewICLRequiredError(fmt.Errorf("schema change requires a ICL binary "+
					"because table %q has at least one remaining index or partition with a zone config",
					tableDesc.Name))
			}
			break
		}
	}

	// Queue the mutation.
	var droppedViews []string
	if idx.ForeignKey.IsSet() {
		foreignTable, err := p.Tables().getMutableTableVersionByID(ctx, idx.ForeignKey.Table, p.txn)
		if err != nil {
			return err
		}
		if err := p.CheckPrivilege(ctx, foreignTable, privilege.REFERENCES); err != nil {
			return err
		}
		if behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
			return errors.Errorf("index %q is in use as a foreign key constraint", idx.Name)
		}
		if err := p.removeFKBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}

	if len(idx.Interleave.Ancestors) > 0 {
		if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}

	for _, ref := range idx.ReferencedBy {
		fetched, err := p.canRemoveFK(ctx, idx.Name, ref, behavior)
		if err != nil {
			return err
		}
		if err := p.removeFK(ctx, ref, fetched); err != nil {
			return err
		}
	}
	for _, ref := range idx.InterleavedBy {
		interleaveTable, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
		if err != nil {
			return err
		}
		if err := p.CheckPrivilege(ctx, interleaveTable, privilege.REFERENCES); err != nil {
			return err
		}
		if err := p.removeInterleave(ctx, ref); err != nil {
			return err
		}
	}

	if idx.Unique && behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
		return errors.Errorf("index %q is in use as unique constraint (use CASCADE if you really want to drop it)", idx.Name)
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID == idx.ID {
			// Ensure that we have DROP privilege on all dependent views
			err := p.canRemoveDependentViewGeneric(
				ctx, "index", idx.Name, tableDesc.ParentID, tableRef, behavior)
			if err != nil {
				return err
			}
			viewDesc, err := p.getViewDescForCascade(
				ctx, "index", idx.Name, tableDesc.ParentID, tableRef.ID, behavior,
			)
			if err != nil {
				return err
			}
			cascadedViews, err := p.removeDependentView(ctx, tableDesc, viewDesc)
			if err != nil {
				return err
			}
			droppedViews = append(droppedViews, viewDesc.Name)
			droppedViews = append(droppedViews, cascadedViews...)
		}
	}

	// Overwriting tableDesc.Index may mess up with the idx object we collected above. Make a copy.
	idxCopy := *idx
	idx = &idxCopy

	found := false
	for i, idxEntry := range tableDesc.Indexes {
		if idxEntry.ID == idx.ID {
			// the idx we picked up with FindIndexByID at the top may not
			// contain the same field any more due to other schema changes
			// intervening since the initial lookup. So we send the recent
			// copy idxEntry for drop instead.
			if err := tableDesc.AddIndexMutation(&idxEntry, sqlbase.DescriptorMutation_DROP); err != nil {
				return err
			}
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("index %q in the middle of being added, try again later", idxName)
	}

	if err := p.removeIndexComment(ctx, tableDesc.ID, idx.ID); err != nil {
		return err
	}

	if err := p.removeConstraintComment(
		ctx,
		tableDesc.ID,
		sqlbase.ConstraintDetail{
			Kind:  sqlbase.ConstraintTypeUnique,
			Index: &sqlbase.IndexDescriptor{Name: idx.Name},
		}); err != nil {
		return err
	}

	if err := tableDesc.RefreshValidate(ctx, p.txn, p.EvalContext().Settings); err != nil {
		return err
	}
	mutationID, err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc)
	if err != nil {
		return err
	}
	if err := p.writeSchemaChange(ctx, tableDesc, mutationID); err != nil {
		return err
	}
	p.noticeSender.BufferNotice(pgnotice.Newf(
		"index %q will be dropped asynchronously and will be complete after the GC TTL",
		idxName.String(),
	))
	// Record index drop in the event log. This is an auditable log event
	// and is recorded in the same transaction as the table descriptor
	// update.
	p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogDropIndex),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(tableDesc.ID),
			Desc: struct {
				User      string
				TableName string
				IndexName string
			}{
				p.SessionData().User,
				tn.FQString(),
				string(idxName),
			},
		},
		Info: &infos.DropIndexInfo{
			TableName:           tn.FQString(),
			IndexName:           string(idxName),
			Statement:           jobDesc,
			User:                p.SessionData().User,
			MutationID:          uint32(mutationID),
			CascadeDroppedViews: droppedViews,
		},
	}
	return nil
}

// dropLocalIndex is function dropIndexByName without schemaChange
// this just for drop local index when `alter table partition by nothing`
// 该函数是dropIndexByName的截取, 仅仅为调整表分区时删除本地分区索引使用, 由于alter_table时会有自己的schemaChange
// 因此去除了该函数schemaChange部分。同时修改了通过Name获取索引描述符等部分。
func (p *planner) dropLocalIndex(
	ctx context.Context,
	indexDesc *sqlbase.IndexDescriptor,
	tableDesc *sqlbase.MutableTableDescriptor,
	behavior tree.DropBehavior,
	constraintBehavior dropIndexConstraintBehavior,
) error {
	// Check if requires ICL binary for eventual zone config removal.
	_, zone, _, err := GetZoneConfigInTxn(ctx, p.txn, uint32(tableDesc.ID), nil, "", false)
	if err != nil {
		return err
	}

	for _, s := range zone.Subzones {
		if s.IndexID != uint32(indexDesc.ID) {
			_, err = GenerateSubzoneSpans(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), tableDesc.TableDesc(), zone.Subzones, false /* newSubzones */)
			if sqlbase.IsICLRequiredError(err) {
				return sqlbase.NewICLRequiredError(fmt.Errorf("schema change requires a ICL binary "+
					"because table %q has at least one remaining index or partition with a zone config",
					tableDesc.Name))
			}
			break
		}
	}

	// Queue the mutation.
	var droppedViews []string
	if indexDesc.ForeignKey.IsSet() {
		foreignTable, err := p.Tables().getMutableTableVersionByID(ctx, indexDesc.ForeignKey.Table, p.txn)
		if err != nil {
			return err
		}
		if err := p.CheckPrivilege(ctx, foreignTable, privilege.REFERENCES); err != nil {
			return err
		}
		if behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
			return errors.Errorf("index %q is in use as a foreign key constraint", indexDesc.Name)
		}
		if err := p.removeFKBackReference(ctx, tableDesc, indexDesc); err != nil {
			return err
		}
	}

	if len(indexDesc.Interleave.Ancestors) > 0 {
		if err := p.removeInterleaveBackReference(ctx, tableDesc, indexDesc); err != nil {
			return err
		}
	}

	for _, ref := range indexDesc.ReferencedBy {
		fetched, err := p.canRemoveFK(ctx, indexDesc.Name, ref, behavior)
		if err != nil {
			return err
		}
		if err := p.removeFK(ctx, ref, fetched); err != nil {
			return err
		}
	}
	for _, ref := range indexDesc.InterleavedBy {
		interleaveTable, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
		if err != nil {
			return err
		}
		if err := p.CheckPrivilege(ctx, interleaveTable, privilege.REFERENCES); err != nil {
			return err
		}
		if err := p.removeInterleave(ctx, ref); err != nil {
			return err
		}
	}

	if indexDesc.Unique && behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
		return errors.Errorf("index %q is in use as unique constraint (use CASCADE if you really want to drop it)", indexDesc.Name)
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID == indexDesc.ID {
			// Ensure that we have DROP privilege on all dependent views
			err := p.canRemoveDependentViewGeneric(
				ctx, "index", indexDesc.Name, tableDesc.ParentID, tableRef, behavior)
			if err != nil {
				return err
			}
			viewDesc, err := p.getViewDescForCascade(
				ctx, "index", indexDesc.Name, tableDesc.ParentID, tableRef.ID, behavior,
			)
			if err != nil {
				return err
			}
			cascadedViews, err := p.removeDependentView(ctx, tableDesc, viewDesc)
			if err != nil {
				return err
			}
			droppedViews = append(droppedViews, viewDesc.Name)
			droppedViews = append(droppedViews, cascadedViews...)
		}
	}

	// Overwriting tableDesc.Index may mess up with the indexDesc object we collected above. Make a copy.
	idxCopy := *indexDesc
	indexDesc = &idxCopy

	found := false
	for i, idxEntry := range tableDesc.Indexes {
		if idxEntry.ID == indexDesc.ID {
			// the indexDesc we picked up with FindIndexByID at the top may not
			// contain the same field any more due to other schema changes
			// intervening since the initial lookup. So we send the recent
			// copy idxEntry for drop instead.
			if err := tableDesc.AddIndexMutation(&idxEntry, sqlbase.DescriptorMutation_DROP); err != nil {
				return err
			}
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("index %q in the middle of being added, try again later", indexDesc.Name)
	}

	if err := p.removeIndexComment(ctx, tableDesc.ID, indexDesc.ID); err != nil {
		return err
	}

	err = tableDesc.RefreshValidate(ctx, p.txn, p.EvalContext().Settings)
	return err
}
