// Copyright 2015  The Cockroach Authors.
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
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type dropTableNode struct {
	n  *tree.DropTable
	td []toDeleteTable
}

type toDeleteTable struct {
	tn   *tree.TableName
	desc *sqlbase.MutableTableDescriptor
}

// DropTable drops a table.
// Privileges: DROP on table.
//   Notes: postgres allows only the table owner to DROP a table.
//          mysql requires the DROP privilege on the table.
func (p *planner) DropTable(ctx context.Context, n *tree.DropTable) (planNode, error) {
	td := make([]toDeleteTable, 0, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, requireTableDesc)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			continue
		}

		td = append(td, toDeleteTable{tn, droppedDesc})
	}

	for _, toDel := range td {
		droppedDesc := toDel.desc
		for _, ref := range droppedDesc.InheritsBy {
			if descInSlice(ref, td) {
				continue
			}
			if n.DropBehavior != tree.DropCascade {
				childT, err := p.Tables().getMutableTableVersionByID(ctx, ref, p.txn)
				if err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("cannot drop table %s,because %s depend on it", droppedDesc.Name, childT.Name)
			}
		}
	}

	dropping := make(map[sqlbase.ID]bool)
	for _, d := range td {
		dropping[d.desc.ID] = true
	}

	for _, toDel := range td {
		droppedDesc := toDel.desc
		for _, idx := range droppedDesc.AllNonDropIndexes() {
			for _, ref := range idx.ReferencedBy {
				if !dropping[ref.Table] {
					if _, err := p.canRemoveFK(ctx, droppedDesc.Name, ref, n.DropBehavior); err != nil {
						return nil, err
					}
				}
			}
			for _, ref := range idx.InterleavedBy {
				if !dropping[ref.Table] {
					if err := p.canRemoveInterleave(ctx, droppedDesc.Name, ref, n.DropBehavior); err != nil {
						return nil, err
					}
				}
			}
		}
		for _, ref := range droppedDesc.DependedOnBy {
			if !dropping[ref.ID] {
				if err := p.canRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
					return nil, err
				}
			}
		}
		if err := p.canRemoveAllTableOwnedSequences(ctx, droppedDesc, n.DropBehavior); err != nil {
			return nil, err
		}
	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}
	return &dropTableNode{n: n, td: td}, nil
}

func (n *dropTableNode) startExec(params runParams) error {
	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		if droppedDesc == nil {
			continue
		}
		dropBeginTime := params.p.txn.ReadTimestamp().GoTime()
		if err := CheckTableSnapShots(params.ctx,
			params.ExecCfg().InternalExecutor,
			params.EvalContext().Txn,
			droppedDesc,
			"cannot drop table that has snapshots"); err != nil {
			return err
		}
		// update flashback record
		if err := UpdateTableDropTimeIfTableEnabled(
			ctx,
			params.p.execCfg.InternalExecutor,
			params.p.txn,
			dropBeginTime,
			droppedDesc.ID); err != nil {
			return err
		}

		droppedDetails := jobspb.DroppedTableDetails{Name: toDel.tn.FQString(), ID: toDel.desc.ID}
		if _, err := params.p.createDropTablesJob(
			ctx,
			[]*sqlbase.MutableTableDescriptor{droppedDesc},
			[]jobspb.DroppedTableDetails{droppedDetails},
			tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType),
			true, /* drainNames */
			sqlbase.InvalidID /* droppedDatabaseID */); err != nil {
			return err
		}

		droppedViews, err := params.p.dropTableImpl(params, droppedDesc, n.n.DropBehavior)
		if err != nil {
			return err
		}
		// at last of drop table delete location map
		for _, todesc := range n.td {
			if todesc.desc.TableDescriptor.LocationNums != 0 {
				if lMap, err := GetLocationMapICL(&todesc.desc.TableDescriptor); err != nil {
					return err
				} else if lMap != nil {
					err = params.p.UpdateLocationMap(params.ctx, todesc.desc,
						params.extendedEvalCtx.Tables.databaseCache.systemConfig, nil)
					if err != nil {
						return err
					}
				}
			}
		}
		// Log a Drop Table event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		params.p.curPlan.auditInfo = &server.AuditInfo{
			EventTime: timeutil.Now(),
			EventType: string(EventLogDropTable),
			TargetInfo: &server.TargetInfo{
				TargetID: int32(droppedDesc.ID),
				Desc: struct {
					TableName           string
					CascadeDroppedViews []string
				}{
					toDel.tn.FQString(),
					droppedViews,
				},
			},
			Info: &infos.DropTableInfo{
				TableName:           toDel.tn.FQString(),
				Statement:           n.n.String(),
				User:                params.SessionData().User,
				CascadeDroppedViews: droppedViews,
			},
		}
	}
	return nil
}

func (*dropTableNode) Next(runParams) (bool, error) { return false, nil }
func (*dropTableNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropTableNode) Close(context.Context)        {}

// prepareDrop/dropTableImpl is used to drop a single table by
// name, which can result from a DROP TABLE, DROP VIEW, DROP SEQUENCE,
// or DROP DATABASE statement. This method returns the dropped table
// descriptor, to be used for the purpose of logging the event.  The table
// is not actually truncated or deleted synchronously. Instead, it is marked
// as deleted (meaning up_version is set and deleted is set) and the
// actual deletion happens async in a schema changer. Note that,
// courtesy of up_version, the actual truncation and dropping will
// only happen once every node ACKs the version of the descriptor with
// the deleted bit set, meaning the lease manager will not hand out
// new leases for it and existing leases are released).
// If the table does not exist, this function returns a nil descriptor.
func (p *planner) prepareDrop(
	ctx context.Context, name *tree.TableName, required bool, requiredType requiredType,
) (*sqlbase.MutableTableDescriptor, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, name, required, requiredType)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, err
	}

	//if tableDesc.InheritsBy != nil{
	//	errI :=fmt.Errorf("cannot drop table a because other objects depend on it")
	//	return nil,errI
	//}
	//
	//if tableDesc.Inherits != nil{
	//
	//	for _,id := range tableDesc.Inherits{
	//
	//		descParent , _ := p.Tables().getMutableTableVersionByID(ctx,id, p.txn )
	//
	//		for i,idP := range descParent.InheritsBy {
	//			if idP == tableDesc.ID {
	//				descParent.InheritsBy = append(descParent.InheritsBy[:i], descParent.InheritsBy[i+1:]...)
	//			}
	//		}
	//		err=p.writeSchemaChange(ctx,descParent,sqlbase.InvalidMutationID)
	//		if err!=nil{
	//			return nil,err
	//		}
	//	}
	//}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}
	return tableDesc, nil
}

func (p *planner) prepareDropWithTableDesc(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	return p.CheckPrivilege(ctx, tableDesc, privilege.DROP)
}

func (p *planner) canRemoveFK(
	ctx context.Context, from string, ref sqlbase.ForeignKeyReference, behavior tree.DropBehavior,
) (*sqlbase.MutableTableDescriptor, error) {
	table, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
	if err != nil {
		return nil, err
	}
	if behavior != tree.DropCascade {
		fullName, err := table.GetFullName(p.getFullNameFunc(ctx, table))
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%q is referenced by foreign key from table %q", from, fullName)
	}
	if err := p.CheckPrivilege(ctx, table, privilege.REFERENCES); err != nil {
		return nil, err
	}
	return table, nil
}

func (p *planner) canRemoveInterleave(
	ctx context.Context, from string, ref sqlbase.ForeignKeyReference, behavior tree.DropBehavior,
) error {
	table, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
	if err != nil {
		return err
	}
	// TODO(dan): It's possible to DROP a table that has a child interleave, but
	// some loose ends would have to be addresssed. The zone would have to be
	// kept and deleted when the last table in it is removed. Also, the dropped
	// table's descriptor would have to be kept around in some Dropped but
	// non-public state for referential integrity of the `InterleaveDescriptor`
	// pointers.
	if behavior != tree.DropCascade {
		return pgerror.UnimplementedWithIssueErrorf(
			8036, "%q is interleaved by table %q", from, table.Name)
	}
	return p.CheckPrivilege(ctx, table, privilege.REFERENCES)
}

func (p *planner) removeFK(
	ctx context.Context, ref sqlbase.ForeignKeyReference, table *sqlbase.MutableTableDescriptor,
) error {
	if table == nil {
		var err error
		table, err = p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
		if err != nil {
			return err
		}
	}
	if table.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	idx, err := table.FindIndexByID(ref.Index)
	if err != nil {
		return err
	}
	idx.ForeignKey = sqlbase.ForeignKeyReference{}
	return p.writeSchemaChange(ctx, table, sqlbase.InvalidMutationID)
}

func (p *planner) removeInterleave(ctx context.Context, ref sqlbase.ForeignKeyReference) error {
	table, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
	if err != nil {
		return err
	}
	if table.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	idx, err := table.FindIndexByID(ref.Index)
	if err != nil {
		return err
	}
	idx.Interleave.Ancestors = nil
	return p.writeSchemaChange(ctx, table, sqlbase.InvalidMutationID)
}

// dropTableImpl does the work of dropping a table (and everything that depends
// on it if `cascade` is enabled). It returns a list of view names that were
// dropped due to `cascade` behavior.
func (p *planner) dropTableImpl(
	params runParams, tableDesc *sqlbase.MutableTableDescriptor, behavior tree.DropBehavior,
) ([]string, error) {
	ctx := params.ctx

	var droppedViews []string
	if util.EnableUDR {
		tn := strconv.Itoa(int(tableDesc.ID))
		query := "delete from system.triggers where relid=" + tn
		_, err := params.ExecCfg().InternalExecutor.Query(ctx, "drop_trigger_cascade", params.p.Txn(), query)
		if err != nil {
			return droppedViews, err
		}
	}

	// Remove FK and interleave relationships.
	for _, idx := range tableDesc.AllNonDropIndexes() {
		if idx.ForeignKey.IsSet() {
			if err := p.removeFKBackReference(ctx, tableDesc, idx); err != nil {
				return droppedViews, err
			}
		}
		if len(idx.Interleave.Ancestors) > 0 {
			if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
				return droppedViews, err
			}
		}
		for _, ref := range idx.ReferencedBy {
			// Nil forces re-fetching tables, since they may have been modified.
			if err := p.removeFK(ctx, ref, nil); err != nil {
				return droppedViews, err
			}
		}
		for _, ref := range idx.InterleavedBy {
			if err := p.removeInterleave(ctx, ref); err != nil {
				return droppedViews, err
			}
		}
	}

	var dropSeqDescs []*sqlbase.MutableTableDescriptor
	for _, columnDesc := range tableDesc.Columns {
		// Find sequences created by table (set experimental_serial_normalization=sql_sequence;)
		seqName := tree.NewUnqualifiedTableName(tree.Name(tableDesc.Name + "_" + columnDesc.Name + "_seq"))
		for _, sequenceID := range columnDesc.UsesSequenceIds {
			seqDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, sequenceID, params.p.txn)
			if err == nil {
				if strings.Index(seqDesc.Name, seqName.Table()) >= 0 && seqDesc.CreateByTable == true {
					dropSeqDescs = append(dropSeqDescs, seqDesc)
				}
			}
		}
		// Remove sequence dependencies.
		if err := removeSequenceDependencies(tableDesc, &columnDesc, params, false); err != nil {
			return droppedViews, err
		}
	}
	// Drop sequences that the columns of the table own.
	for _, col := range tableDesc.Columns {
		//if col.Name=="b"{
		//	col.OwnsSequenceIds = append(col.OwnsSequenceIds, 59)
		//}
		if err := params.p.dropSequencesOwnedByCol(ctx, &col, true, behavior); err != nil {
			return droppedViews, err
		}
	}
	// Drop sequences that the columns of the table own
	//for _, col := range tableDesc.Columns {
	//	if err := dropSequencesOwnedByCol(&col, params); err != nil {
	//		return droppedViews, err
	//	}
	//}
	// Drop sequences created by table
	for _, seqDesc := range dropSeqDescs {
		_ = p.dropSequenceImpl(ctx, seqDesc, tree.DropDefault)
	}

	if tableDesc.Inherits != nil {

		for _, id := range tableDesc.Inherits {

			descParent, _ := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
			if descParent.Dropped() {
				continue
			}
			for i, idP := range descParent.InheritsBy {
				if idP == tableDesc.ID {
					descParent.InheritsBy = append(descParent.InheritsBy[:i], descParent.InheritsBy[i+1:]...)
				}
			}
			err := p.writeSchemaChange(ctx, descParent, sqlbase.InvalidMutationID)
			if err != nil {
				return droppedViews, err
			}
		}
	}

	// Drop all views that depend on this table, assuming that we wouldn't have
	// made it to this point if `cascade` wasn't enabled.
	for _, ref := range tableDesc.DependedOnBy {
		viewDesc, err := p.getViewDescForCascade(
			ctx, tableDesc.TypeName(), tableDesc.Name, tableDesc.ParentID, ref.ID, tree.DropCascade,
		)
		if err != nil {
			return droppedViews, err
		}
		// This view is already getting dropped. Don't do it twice.
		if viewDesc.Dropped() {
			continue
		}
		var cascadedViews []string
		if viewDesc.IsView() {
			cascadedViews, err = p.dropViewImpl(ctx, viewDesc, behavior)
			if err != nil {
				return droppedViews, err
			}
		} else {
			cascadedViews, err = p.dropTableImpl(params, viewDesc, tree.DropCascade)
			if err != nil {
				return droppedViews, err
			}
		}
		droppedViews = append(droppedViews, cascadedViews...)
		droppedViews = append(droppedViews, viewDesc.Name)
	}

	if tableDesc.InheritsBy != nil {
		for _, ref := range tableDesc.InheritsBy {

			var cascadedTables []string
			childDesc, err := p.Tables().getMutableTableVersionByID(ctx, ref, p.txn)
			if err != nil {
				return droppedViews, err
			}
			if childDesc.Dropped() {
				continue
			}
			cascadedTables, err = p.dropInheritsTableImpl(params, childDesc, behavior)
			if err != nil {
				return cascadedTables, err
			}

			droppedViews = append(droppedViews, cascadedTables...)
			droppedViews = append(droppedViews, childDesc.Name)
		}
	}

	err := p.removeTableComment(ctx, tableDesc)
	if err != nil {
		return droppedViews, err
	}

	err = p.initiateDropTable(ctx, tableDesc, true /* drain name */)
	return droppedViews, err
}

func (p *planner) dropInheritsTableImpl(
	params runParams, tableDesc *sqlbase.MutableTableDescriptor, behavior tree.DropBehavior,
) ([]string, error) {
	ctx := params.ctx
	var cascadeDroppedInheritsTables []string

	if tableDesc.Inherits != nil {

		for _, id := range tableDesc.Inherits {

			descParent, _ := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
			if descParent.Dropped() {
				continue
			}
			for i, idP := range descParent.InheritsBy {
				if idP == tableDesc.ID {
					descParent.InheritsBy = append(descParent.InheritsBy[:i], descParent.InheritsBy[i+1:]...)
				}
			}
			err := p.writeSchemaChange(ctx, descParent, sqlbase.InvalidMutationID)
			if err != nil {
				return cascadeDroppedInheritsTables, err
			}
		}
	}

	if behavior == tree.DropCascade {
		for _, ref := range tableDesc.InheritsBy {
			childDesc, _ := p.Tables().getMutableTableVersionByID(ctx, ref, p.txn)
			if childDesc.Dropped() {
				continue
			}
			cascadedTables, err := p.dropInheritsTableImpl(params, childDesc, tree.DropCascade)
			if err != nil {
				return cascadeDroppedInheritsTables, err
			}
			cascadeDroppedInheritsTables = append(cascadeDroppedInheritsTables, cascadedTables...)
			cascadeDroppedInheritsTables = append(cascadeDroppedInheritsTables, childDesc.Name)
		}
	}

	if err := p.initiateDropTable(ctx, tableDesc, true /* drainName */); err != nil {
		return cascadeDroppedInheritsTables, err
	}

	return cascadeDroppedInheritsTables, nil
}

// drainName when set implies that the name needs to go through the draining
// names process. This parameter is always passed in as true except from
// TRUNCATE which directly deletes the old name to id map and doesn't need
// drain the old map.
func (p *planner) initiateDropTable(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, drainName bool,
) error {
	if tableDesc.Dropped() {
		return fmt.Errorf("table %q is being dropped", tableDesc.Name)
	}

	// If the table is not interleaved and the ClearRange feature is
	// enabled in the cluster, use the delayed GC mechanism to schedule
	// usage of the more efficient ClearRange pathway. ClearRange will
	// only work if the entire hierarchy of interleaved tables are
	// dropped at once, as with ON DELETE CASCADE where the top-level
	// "root" table is dropped.
	//
	// TODO(bram): If interleaved and ON DELETE CASCADE, we will be
	// able to use this faster mechanism.
	if tableDesc.IsTable() && !tableDesc.IsInterleaved() &&
		p.ExecCfg().Settings.Version.IsActive(cluster.VersionClearRange) {
		// Get the zone config applying to this table in order to
		// ensure there is a GC TTL.
		_, _, _, err := GetZoneConfigInTxn(
			ctx, p.txn, uint32(tableDesc.ID), &sqlbase.IndexDescriptor{}, "", false, /* getInheritedDefault */
		)
		if err != nil {
			return err
		}

		tableDesc.DropTime = timeutil.Now().UnixNano()
	}

	tableDesc.State = sqlbase.TableDescriptor_DROP
	if drainName {
		// Queue up name for draining.
		nameDetails := sqlbase.TableDescriptor_NameInfo{
			ParentID: tableDesc.ParentID,
			Name:     tableDesc.Name}
		tableDesc.DrainingNames = append(tableDesc.DrainingNames, nameDetails)
	}

	// Mark all jobs scheduled for schema changes as successful.
	jobIDs := make(map[int64]struct{})
	var id sqlbase.MutationID
	for _, m := range tableDesc.Mutations {
		if id != m.MutationID {
			id = m.MutationID
			jobID, err := getJobIDForMutationWithDescriptor(ctx, tableDesc.TableDesc(), id)
			if err != nil {
				return err
			}
			jobIDs[jobID] = struct{}{}
		}
	}
	for _, gcm := range tableDesc.GCMutations {
		jobIDs[gcm.JobID] = struct{}{}
	}
	for jobID := range jobIDs {
		job, err := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.txn)
		if err != nil {
			return err
		}

		if err := job.WithTxn(p.txn).Succeeded(ctx, jobs.NoopFn); err != nil {
			return errors.Wrapf(err, "failed to mark job %d as as successful", jobID)
		}
	}

	// Initiate an immediate schema change. When dropping a table
	// in a session, the data and the descriptor are not deleted.
	// Instead, that is taken care of asynchronously by the schema
	// change manager, which is notified via a system config gossip.
	// The schema change manager will properly schedule deletion of
	// the underlying data when the GC deadline expires.
	return p.writeDropTable(ctx, tableDesc)
}

func (p *planner) removeFKBackReference(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, idx *sqlbase.IndexDescriptor,
) error {
	var t *sqlbase.MutableTableDescriptor
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == idx.ForeignKey.Table {
		t = tableDesc
	} else {
		lookup, err := p.Tables().getMutableTableVersionByID(ctx, idx.ForeignKey.Table, p.txn)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", idx.ForeignKey.Table, err)
		}
		t = lookup
	}
	if t.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	targetIdx, err := t.FindIndexByID(idx.ForeignKey.Index)
	if err != nil {
		return err
	}
	for k, ref := range targetIdx.ReferencedBy {
		if ref.Table == tableDesc.ID && ref.Index == idx.ID {
			targetIdx.ReferencedBy = append(targetIdx.ReferencedBy[:k], targetIdx.ReferencedBy[k+1:]...)
		}
	}
	return p.writeSchemaChange(ctx, t, sqlbase.InvalidMutationID)
}

func (p *planner) removeInterleaveBackReference(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, idx *sqlbase.IndexDescriptor,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	ancestor := idx.Interleave.Ancestors[len(idx.Interleave.Ancestors)-1]
	var t *sqlbase.MutableTableDescriptor
	if ancestor.TableID == tableDesc.ID {
		t = tableDesc
	} else {
		lookup, err := p.Tables().getMutableTableVersionByID(ctx, ancestor.TableID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", ancestor.TableID, err)
		}
		t = lookup
	}
	if t.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	targetIdx, err := t.FindIndexByID(ancestor.IndexID)
	if err != nil {
		return err
	}
	foundAncestor := false
	for k, ref := range targetIdx.InterleavedBy {
		if ref.Table == tableDesc.ID && ref.Index == idx.ID {
			if foundAncestor {
				return errors.Errorf(
					"ancestor entry in %s for %s@%s found more than once", t.Name, tableDesc.Name, idx.Name)
			}
			targetIdx.InterleavedBy = append(targetIdx.InterleavedBy[:k], targetIdx.InterleavedBy[k+1:]...)
			foundAncestor = true
		}
	}
	if t != tableDesc {
		return p.writeSchemaChange(ctx, t, sqlbase.InvalidMutationID)
	}
	return nil
}

// removeMatchingReferences removes all refs from the provided slice that
// match the provided ID, returning the modified slice.
func removeMatchingReferences(
	refs []sqlbase.TableDescriptor_Reference, id sqlbase.ID,
) []sqlbase.TableDescriptor_Reference {
	updatedRefs := refs[:0]
	for _, ref := range refs {
		if ref.ID != id {
			updatedRefs = append(updatedRefs, ref)
		}
	}
	return updatedRefs
}

func (p *planner) removeTableComment(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx,
		"delete-table-comment",
		p.txn,
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.TableCommentType,
		tableDesc.ID)
	if err != nil {
		return err
	}

	_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx,
		"delete-comment",
		p.txn,
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2",
		keys.ColumnCommentType,
		tableDesc.ID)

	for _, indexDesc := range tableDesc.Indexes {
		err = p.removeIndexComment(
			ctx,
			tableDesc.ID,
			indexDesc.ID)
	}
	constraintMap, err := tableDesc.GetConstraintInfo(ctx, p.Txn())
	if err != nil {
		return err
	}
	for _, v := range constraintMap {
		err = p.removeConstraintComment(
			ctx,
			tableDesc.ID,
			v)
	}

	return err
}
