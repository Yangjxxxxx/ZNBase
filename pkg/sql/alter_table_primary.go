// Copyright 2020  The Cockroach Authors.

package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgnotice"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/errorutil/unimplemented"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// alterFkMutationWithSpecificPrimaryKey
// todo(gzq) 合并fk重构前临时解决方案
func alterFKMutationWithSpecificPrimaryKey(
	ctx context.Context,
	p *planner,
	table *sqlbase.MutableTableDescriptor,
	oldID sqlbase.IndexID,
	newIndex *sqlbase.IndexDescriptor,
) error {
	if newIndex.ForeignKey.IsSet() {
		if err := func(tableDesc *sqlbase.MutableTableDescriptor, idx *sqlbase.IndexDescriptor) error {
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
				if ref.Table == tableDesc.ID && ref.Index == oldID {
					targetIdx.ReferencedBy[k].Index = idx.ID
				}
			}
			return p.writeSchemaChange(ctx, t, sqlbase.InvalidMutationID)
		}(table, newIndex); err != nil {
			return err
		}
	}

	for _, ref := range newIndex.ReferencedBy {
		if err := func(tableDesc *sqlbase.MutableTableDescriptor) error {
			var t *sqlbase.MutableTableDescriptor
			// We don't want to lookup/edit a second copy of the same table.
			if tableDesc.ID == ref.Table {
				t = tableDesc
			} else {
				lookup, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
				if err != nil {
					return errors.Errorf("error resolving referenced table ID %d: %v", ref.Table, err)
				}
				t = lookup
			}
			if t.Dropped() {
				// The referenced table is being dropped. No need to modify it further.
				return nil
			}
			targetIdx, err := t.FindIndexByID(ref.Index)
			if err != nil {
				return err
			}
			targetIdx.ForeignKey.Index = newIndex.ID
			return p.writeSchemaChange(ctx, t, sqlbase.InvalidMutationID)
		}(table); err != nil {
			return err
		}
	}
	return nil
}

// addIndexMutationWithSpecificPrimaryKey adds an index mutation into the given table descriptor, but sets up
// the index with ExtraColumnIDs from the given index, rather than the table's primary key.
func addIndexMutationWithSpecificPrimaryKey(
	table *sqlbase.MutableTableDescriptor,
	toAdd *sqlbase.IndexDescriptor,
	primary *sqlbase.IndexDescriptor,
) error {
	// Reset the ID so that a call to AllocateIDs will set up the index.
	toAdd.ID = 0
	if err := table.AddIndexMutation(toAdd, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := table.AllocateIDs(); err != nil {
		return err
	}

	// Use the columns in the given primary index to construct this indexes ExtraColumnIDs list.
	//primary.ExtraColumnIDs = nil
	toAdd.ExtraColumnIDs = nil
	//if isDrop {
	//	return nil
	//}
	for _, colID := range primary.ColumnIDs {
		if !toAdd.ContainsColumnID(colID) {
			toAdd.ExtraColumnIDs = append(toAdd.ExtraColumnIDs, colID)
		}
	}
	return nil
}

func (p *planner) AlterPrimaryKey(
	ctx context.Context,
	tableDesc *MutableTableDescriptor,
	alterPK *tree.AlterTableAlterPrimaryKey,
	info map[string]sqlbase.ConstraintDetail,
) error {
	// Make sure that all nodes in the cluster are able to perform primary key changes before proceeding.
	if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionPrimaryKeyChanges) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"all nodes are not the correct version for primary key changes")
	}

	if !p.EvalContext().SessionData.PrimaryKeyChangesEnabled {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"session variable enable_primary_key_changes is set to false, cannot perform primary key change")
	}

	// Ensure that there is not another primary key change attempted within this transaction.
	currentMutationID := tableDesc.ClusterVersion.NextMutationID
	for i := range tableDesc.Mutations {
		if desc := tableDesc.Mutations[i].GetPrimaryKeySwap(); desc != nil {
			if tableDesc.Mutations[i].MutationID == currentMutationID {
				return unimplemented.NewWithIssue(
					43376, "multiple primary key changes in the same transaction are unsupported")
			}
			return unimplemented.New(
				"ddl", "multiple primary key changes in the concurrent transactions are unsupported")
		}
	}

	for _, elem := range alterPK.Columns {
		col, dropped, err := tableDesc.FindColumnByName(elem.Column)
		if err != nil {
			return err
		}
		if dropped {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q is being dropped", col.Name)
		}
		if col.Nullable {
			return pgerror.Newf(pgcode.InvalidSchemaDefinition, "cannot use nullable column %q in primary key", col.Name)
		}
	}

	// Disable primary key changes on tables that are interleaved parents.
	if len(tableDesc.PrimaryIndex.InterleavedBy) != 0 {
		return errors.New("cannot change the primary key of an interleaved parent")
	}

	nameExists := func(name string) bool {
		// 修复同名约束级联问题jira：12285
		if _, ok := info[name]; ok {
			return true
		}
		_, drop, err := tableDesc.FindIndexByName(name)
		if drop {
			return false
		}
		return err == nil
	}

	// Make a new index that is suitable to be a primary index.
	baseName, name := "new_primary_key", "new_primary_key"
	for try := 1; nameExists(name); try++ {
		name = fmt.Sprintf("%s_%d", baseName, try)
	}
	if alterPK.Name != "" &&
		// Allow reuse of existing primary key's name.
		tableDesc.PrimaryIndex.Name != string(alterPK.Name) &&
		nameExists(string(alterPK.Name)) {
		return errors.Errorf("constraint with name %s already exists", alterPK.Name)
	}
	newPrimaryIndexDesc := &sqlbase.IndexDescriptor{
		Name:         name,
		Unique:       true,
		EncodingType: sqlbase.PrimaryIndexEncoding,
		Type:         sqlbase.IndexDescriptor_FORWARD,
	}
	if err := newPrimaryIndexDesc.FillColumns(alterPK.Columns); err != nil {
		return err
	}
	if err := tableDesc.AddIndexMutation(newPrimaryIndexDesc, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := tableDesc.AllocateIDs(); err != nil {
		return err
	}

	// Ensure that the new primary index stores all columns in the table. We can't
	// use AllocateID's to fill the stored columns here because it assumes
	// that the indexed columns are n.PrimaryIndex.ColumnIDs, but here we want
	// to consider the indexed columns to be newPrimaryIndexDesc.ColumnIDs.
	// 确保新的主键索引存储表中的所有列.
	// 不能使用AllocateID's来填充这里存储的列, 因为假定索引列是n.PrimaryIndex.columnIDs,
	// 但我们想要将索引列视为newPrimaryIndexDesc.ColumnIDs.
	newPrimaryIndexDesc.StoreColumnNames, newPrimaryIndexDesc.StoreColumnIDs = nil, nil
	for _, col := range tableDesc.Columns {
		containsCol := false
		for _, colID := range newPrimaryIndexDesc.ColumnIDs {
			if colID == col.ID {
				containsCol = true
				break
			}
		}
		if !containsCol {
			newPrimaryIndexDesc.StoreColumnIDs = append(newPrimaryIndexDesc.StoreColumnIDs, col.ID)
			newPrimaryIndexDesc.StoreColumnNames = append(newPrimaryIndexDesc.StoreColumnNames, col.Name)
		}
	}

	if alterPK.Interleave != nil {
		if err := p.addInterleave(ctx, tableDesc, newPrimaryIndexDesc, alterPK.Interleave); err != nil {
			return err
		}
		if err := p.finalizeInterleave(ctx, tableDesc, newPrimaryIndexDesc); err != nil {
			return err
		}
	}

	// TODO (rohany,solongordon): Until it is clear what to do with column families, disallow primary key changes
	//  on tables with multiple column families.
	if len(tableDesc.Families) > 1 {
		return errors.New("unable to perform primary key change on tables with multiple column families")
	}

	// fkNameExists := func(name string) bool {
	// 	if tableDesc.PrimaryIndex.ForeignKey.Name == name {
	// 		return true
	// 	}
	// 	for _, idx := range tableDesc.Indexes {
	// 		if idx.ForeignKey.Name == name {
	// 			return true
	// 		}
	// 	}
	// 	return false
	// }

	// TODO (rohany): gate this behind a flag so it doesn't happen all the time.
	// Create a new index that indexes everything the old primary index does, but doesn't store anything.
	if !tableDesc.IsPrimaryIndexDefaultRowID() {
		oldPrimaryIndexCopy := protoutil.Clone(&tableDesc.PrimaryIndex).(*sqlbase.IndexDescriptor)
		// Clear the name of the index so that it gets generated by AllocateIDs.
		oldID := oldPrimaryIndexCopy.ID
		if oldPrimaryIndexCopy.ForeignKey.IsSet() {
			// 改外键名
			name := oldPrimaryIndexCopy.ForeignKey.Name
			// for try := 1; fkNameExists(name); try++ {
			// 	name = fmt.Sprintf("%s#%d", name, try)
			// }
			oldPrimaryIndexCopy.ForeignKey.Name = name
		}
		oldPrimaryIndexCopy.Name = ""
		oldPrimaryIndexCopy.StoreColumnIDs = nil
		oldPrimaryIndexCopy.StoreColumnNames = nil
		// Make the copy of the old primary index not-interleaved. This decision
		// can be revisited based on user experience.
		oldPrimaryIndexCopy.Interleave = sqlbase.InterleaveDescriptor{}
		//if err := addIndexMutationWithSpecificPrimaryKey(tableDesc, oldPrimaryIndexCopy, newPrimaryIndexDesc, isDrop); err != nil {
		if err := addIndexMutationWithSpecificPrimaryKey(tableDesc, oldPrimaryIndexCopy, newPrimaryIndexDesc); err != nil {
			return err
		}

		if err := alterFKMutationWithSpecificPrimaryKey(ctx, p, tableDesc, oldID, oldPrimaryIndexCopy); err != nil {
			return err
		}
	}

	// We have to rewrite all indexes that either:
	// * depend on uniqueness from the old primary key (inverted, non-unique, or unique with nulls).
	// * don't store or index all columns in the new primary key.
	shouldRewriteIndex := func(idx *sqlbase.IndexDescriptor) bool {
		shouldRewrite := false
		for _, colID := range newPrimaryIndexDesc.ColumnIDs {
			if !idx.ContainsColumnID(colID) {
				shouldRewrite = true
				break
			}
		}
		if idx.Unique {
			for _, colID := range idx.ColumnIDs {
				col, err := tableDesc.FindColumnByID(colID)
				if err != nil {
					panic(err)
				}
				if col.Nullable {
					shouldRewrite = true
					break
				}
			}
		}
		return shouldRewrite || !idx.Unique || idx.Type == sqlbase.IndexDescriptor_INVERTED
	}
	var indexesToRewrite []*sqlbase.IndexDescriptor
	for i := range tableDesc.Indexes {
		idx := &tableDesc.Indexes[i]
		if idx.ID != newPrimaryIndexDesc.ID && shouldRewriteIndex(idx) {
			indexesToRewrite = append(indexesToRewrite, idx)
		}
	}

	// Queue up a mutation for each index that needs to be rewritten.
	// This new index will have an altered ExtraColumnIDs to allow it to be rewritten
	// using the unique-ifying columns from the new table.
	var oldIndexIDs, newIndexIDs []sqlbase.IndexID
	for _, idx := range indexesToRewrite {
		// Clone the index that we want to rewrite.
		newIndex := protoutil.Clone(idx).(*sqlbase.IndexDescriptor)
		name := newIndex.Name + "_rewrite_for_primary_key_change"
		for try := 1; nameExists(name); try++ {
			name = fmt.Sprintf("%s#%d", name, try)
		}
		newIndex.Name = name
		oldID := newIndex.ID
		if newIndex.ForeignKey.IsSet() {
			// 改外键名
			name := newIndex.ForeignKey.Name
			// for try := 1; fkNameExists(name); try++ {
			// 	name = fmt.Sprintf("%s#%d", name, try)
			// }
			newIndex.ForeignKey.Name = name
		}
		//if err := addIndexMutationWithSpecificPrimaryKey(tableDesc, newIndex, newPrimaryIndexDesc, isDrop); err != nil {
		if err := addIndexMutationWithSpecificPrimaryKey(tableDesc, newIndex, newPrimaryIndexDesc); err != nil {
			return err
		}

		if err := alterFKMutationWithSpecificPrimaryKey(ctx, p, tableDesc, oldID, newIndex); err != nil {
			return err
		}
		// If the index that we are rewriting is interleaved, we need to setup the rewritten
		// index to be interleaved as well. Since we cloned the index, the interleave descriptor
		// on the new index is already set up. So, we just need to add the backreference from the
		// parent to this new index.
		if len(newIndex.Interleave.Ancestors) != 0 {
			if err := p.finalizeInterleave(ctx, tableDesc, newIndex); err != nil {
				return err
			}
		}
		oldIndexIDs = append(oldIndexIDs, idx.ID)
		newIndexIDs = append(newIndexIDs, newIndex.ID)
	}

	swapArgs := &sqlbase.PrimaryKeySwap{
		OldPrimaryIndexId:   tableDesc.PrimaryIndex.ID,
		NewPrimaryIndexId:   newPrimaryIndexDesc.ID,
		NewIndexes:          newIndexIDs,
		OldIndexes:          oldIndexIDs,
		NewPrimaryIndexName: string(alterPK.Name),
	}
	tableDesc.AddPrimaryKeySwapMutation(swapArgs)

	// Mark the primary key of the table as valid.
	tableDesc.PrimaryIndex.Disabled = false

	// Send a notice to users about the async cleanup jobs.
	p.BufferClientNotice(
		ctx,
		pgnotice.Newf(
			"primary key changes spawn async cleanup jobs. Future schema changes on %q may be delayed as these jobs finish",
			tableDesc.Name),
	)

	return nil
}
