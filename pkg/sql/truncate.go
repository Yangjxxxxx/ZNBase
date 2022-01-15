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
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// TableTruncateChunkSize is the maximum number of keys deleted per chunk
// during a table truncation.
const TableTruncateChunkSize = indexTruncateChunkSize

type truncateNode struct {
	n *tree.Truncate
}

// Truncate deletes all rows from a table.
// Privileges: DROP on table.
//   Notes: postgres requires TRUNCATE.
//          mysql requires DROP (for mysql >= 5.1.16, DELETE before that).
func (p *planner) Truncate(ctx context.Context, n *tree.Truncate) (planNode, error) {
	return &truncateNode{n: n}, nil
}

func (t *truncateNode) startExec(params runParams) error {
	p := params.p
	n := t.n
	ctx := params.ctx

	// Since truncation may cascade to a given table any number of times, start by
	// building the unique set (ID->name) of tables to truncate.
	toTruncate := make(map[sqlbase.ID]string, len(n.Tables))
	// toTraverse is the list of tables whose references need to be traversed
	// while constructing the list of tables that should be truncated.
	toTraverse := make([]sqlbase.MutableTableDescriptor, 0, len(n.Tables))

	// This is the list of descriptors of truncated tables in the statement. These tables will have a mutationID and
	// MutationJob related to the created job of the TRUNCATE statement.
	stmtTableDescs := make([]*sqlbase.MutableTableDescriptor, 0, len(n.Tables))
	droppedTableDetails := make([]jobspb.DroppedTableDetails, 0, len(n.Tables))

	for i := range n.Tables {
		tn := &n.Tables[i]
		tableDesc, err := p.ResolveMutableTableDescriptor(
			ctx, tn, true /*required*/, requireTableDesc)
		if err != nil {
			return err
		}
		if err := CheckTableSnapShots(params.ctx,
			params.ExecCfg().InternalExecutor,
			params.EvalContext().Txn,
			tableDesc,
			"cannot truncate table that has snapshots"); err != nil {
			return err
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
			return err
		}
		scDesc, err := p.Tables().schemaCache.getSchemaDescByID(ctx, p.txn, tableDesc.ParentID)
		if err != nil {
			return err
		}
		if err := p.CheckPrivilege(ctx, scDesc, privilege.CREATE); err != nil {
			return err
		}

		toTruncate[tableDesc.ID] = tn.FQString()
		toTraverse = append(toTraverse, *tableDesc)

		stmtTableDescs = append(stmtTableDescs, tableDesc)
		droppedTableDetails = append(droppedTableDetails, jobspb.DroppedTableDetails{
			Name: tn.FQString(),
			ID:   tableDesc.ID,
		})
		dropBeginTime := params.p.txn.ReadTimestamp().GoTime()
		// update flashback record
		if err := UpdateTableDropTimeIfTableEnabled(
			ctx,
			params.p.execCfg.InternalExecutor,
			params.p.txn,
			dropBeginTime,
			tableDesc.ID); err != nil {
			return err
		}
	}

	dropJobID, err := p.createDropTablesJob(
		ctx,
		stmtTableDescs,
		droppedTableDetails,
		tree.AsStringWithFlags(n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType),
		false, /* drainNames */
		sqlbase.InvalidID /* droppedDatabaseID */)
	if err != nil {
		return err
	}

	// Check that any referencing tables are contained in the set, or, if CASCADE
	// requested, add them all to the set.
	for len(toTraverse) > 0 {
		// Pick last element.
		idx := len(toTraverse) - 1
		tableDesc := toTraverse[idx]
		toTraverse = toTraverse[:idx]

		maybeEnqueue := func(ref sqlbase.ForeignKeyReference, msg string) error {
			// Check if we're already truncating the referencing table.
			if _, ok := toTruncate[ref.Table]; ok {
				return nil
			}
			other, err := p.Tables().getMutableTableVersionByID(ctx, ref.Table, p.txn)
			if err != nil {
				return err
			}

			if n.DropBehavior != tree.DropCascade {
				fullName, err := other.GetFullName(p.getFullNameFunc(ctx, other))
				if err != nil {
					return err
				}
				return errors.Errorf("%q is %s table %q", tableDesc.Name, msg, fullName)
			}
			if err := p.CheckPrivilege(ctx, other, privilege.DROP); err != nil {
				return err
			}
			otherName, err := p.getQualifiedTableName(ctx, other.TableDesc())
			if err != nil {
				return err
			}
			toTruncate[other.ID] = otherName
			toTraverse = append(toTraverse, *other)
			return nil
		}

		for _, idx := range tableDesc.AllNonDropIndexes() {
			for _, ref := range idx.ReferencedBy {
				if err := maybeEnqueue(ref, "referenced by foreign key from"); err != nil {
					return err
				}
			}

			for _, ref := range idx.InterleavedBy {
				if err := maybeEnqueue(ref, "interleaved by"); err != nil {
					return err
				}
			}
		}
	}

	// Mark this query as non-cancellable if autocommitting.
	if err := p.cancelChecker.Check(); err != nil {
		return err
	}

	traceKV := p.extendedEvalCtx.Tracing.KVTracingEnabled()
	for id, name := range toTruncate {
		tableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}

		tgDesc, err := MakeTriggerDesc(ctx, p.txn, tableDesc)
		if err != nil {
			return err
		}

		err = ExecuteTriggers(ctx, p.txn, params, tgDesc, nil, nil, nil,
			TriggerTypeTruncate, TriggerTypeBefore, TriggerTypeStatement)
		if err != nil {
			return err
		}

		if err := p.truncateTable(ctx, id, dropJobID, traceKV, params); err != nil {
			return err
		}
		// Log a Truncate Table event for this table.
		params.p.curPlan.auditInfo = &server.AuditInfo{
			EventTime: timeutil.Now(),
			EventType: string(EventLogTruncateTable),
			TargetInfo: &server.TargetInfo{
				TargetID: int32(id),
				Desc: struct {
					TableName string
				}{
					name,
				},
			},
			Info: &infos.TruncateInfo{
				TableName: name,
				Statement: n.String(),
				User:      p.SessionData().User,
			},
		}
		err = ExecuteTriggers(ctx, p.txn, params, tgDesc, nil, nil, nil,
			TriggerTypeTruncate, TriggerTypeAfter, TriggerTypeStatement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *truncateNode) Next(runParams) (bool, error) { return false, nil }
func (t *truncateNode) Values() tree.Datums          { return tree.Datums{} }
func (t *truncateNode) Close(context.Context)        {}

// truncateTable truncates the data of a table in a single transaction. It
// drops the table and recreates it with a new ID. The dropped table is
// GC-ed later through an asynchronous schema change.
func (p *planner) truncateTable(
	ctx context.Context, id sqlbase.ID, dropJobID int64, traceKV bool, params runParams,
) error {
	// Read the table descriptor because it might have changed
	// while another table in the truncation list was truncated.
	tableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
	if err != nil {
		return err
	}

	tgDesc, err := MakeTriggerDesc(ctx, p.txn, tableDesc)
	if err != nil {
		return err
	}

	tableDesc.DropJobID = dropJobID
	newTableDesc := sqlbase.NewMutableCreatedTableDescriptor(tableDesc.TableDescriptor)
	newTableDesc.ReplacementOf = sqlbase.TableDescriptor_Replacement{
		ID: id, Time: p.txn.CommitTimestamp(),
	}
	newTableDesc.SetID(0)
	newTableDesc.Version = 1

	// Remove old name -> id map.
	// This is a violation of consistency because once the TRUNCATE commits
	// some nodes in the cluster can have cached the old name to id map
	// for the table and applying operations using the old table id.
	// This violation is needed because it is not uncommon for TRUNCATE
	// to be used along with other CRUD commands that follow it in the
	// same transaction. Commands that follow the TRUNCATE in the same
	// transaction will use the correct descriptor (through uncommittedTables)
	// See the comment about problem 3 related to draining names in
	// structured.proto
	//
	// TODO(vivek): Fix properly along with #12123.
	zoneKey, nameKey, _ := GetKeysForTableDescriptor(tableDesc.TableDesc())
	b := &client.Batch{}
	// Use CPut because we want to remove a specific name -> id map.
	if traceKV {
		log.VEventf(ctx, 2, "CPut %s -> nil", nameKey)
	}
	b.CPut(nameKey, nil, tableDesc.ID)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Drop table.
	if err := p.initiateDropTable(ctx, tableDesc, false /* drainName */); err != nil {
		return err
	}

	newID, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return err
	}
	scDesc, err := p.Tables().schemaCache.getSchemaDescByID(ctx, p.txn, tableDesc.ParentID)
	if err != nil {
		return err
	}
	if err := UpdateFlashbackIfDBEnabled(
		ctx,
		p.execCfg.InternalExecutor,
		p.txn,
		newID,
		tableDesc.ParentID,
		scDesc.ParentID,
		tableDesc.Name); err != nil {
		return err
	}
	for _, id := range tableDesc.Inherits {
		fatherDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if fatherDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		for i := range fatherDesc.InheritsBy {
			if fatherDesc.InheritsBy[i] == tableDesc.ID {
				fatherDesc.InheritsBy[i] = newID
				err := p.writeSchemaChange(ctx, fatherDesc, sqlbase.InvalidMutationID)
				if err != nil {
					return err
				}
				break
			}
		}
	}

	for _, id := range tableDesc.InheritsBy {
		sonDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if sonDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		for i := range sonDesc.Inherits {
			if sonDesc.Inherits[i] == tableDesc.ID {
				sonDesc.Inherits[i] = newID
				err := p.writeSchemaChange(ctx, sonDesc, sqlbase.InvalidMutationID)
				if err != nil {
					return err
				}
				break
			}
		}
	}

	// update all the references to this table.
	tables, err := p.findAllReferences(ctx, *tableDesc)
	if err != nil {
		return err
	}
	if changed, err := reassignReferencedTables(tables, tableDesc.ID, newID); err != nil {
		return err
	} else if changed {
		newTableDesc.State = sqlbase.TableDescriptor_ADD
	}

	for _, table := range tables {
		if err := p.writeSchemaChange(ctx, table, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}

	// Reassign all self references.
	if changed, err := reassignReferencedTables(
		[]*sqlbase.MutableTableDescriptor{newTableDesc}, tableDesc.ID, newID,
	); err != nil {
		return err
	} else if changed {
		newTableDesc.State = sqlbase.TableDescriptor_ADD
	}

	// Resolve all outstanding mutations. Make all new schema elements
	// public because the table is empty and doesn't need to be backfilled.
	for _, m := range newTableDesc.Mutations {
		if err := newTableDesc.MakeMutationComplete(m); err != nil {
			return err
		}
	}
	newTableDesc.Mutations = nil
	newTableDesc.GCMutations = nil
	newTableDesc.ModificationTime = p.txn.CommitTimestamp()
	tKey := tableKey{parentID: newTableDesc.ParentID, name: newTableDesc.Name}
	key := tKey.Key()
	if err := p.createDescriptorWithID(
		ctx, key, newID, newTableDesc, p.ExtendedEvalContext().Settings); err != nil {
		return err
	}

	// Reassign comment.
	if err := reassignComment(ctx, p, tableDesc, newTableDesc); err != nil {
		return err
	}

	if tgDesc != nil {
		for _, tg := range tgDesc.Triggers {
			err = p.changeID(ctx, tableDesc, uint32(newID), uint32(tableDesc.ID), tg.Tgname)
		}
	}

	// Copy the zone config.
	b = &client.Batch{}
	b.Get(zoneKey)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	val := b.Results[0].Rows[0].Value
	if val == nil {
		return nil
	}
	zoneCfg, err := val.GetBytes()
	if err != nil {
		return err
	}
	const insertZoneCfg = `INSERT INTO system.zones (id, config) VALUES ($1, $2)`
	_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx, "insert-zone", p.txn, insertZoneCfg, newID, zoneCfg)
	return err
}

// For all the references from a table
func (p *planner) findAllReferences(
	ctx context.Context, table sqlbase.MutableTableDescriptor,
) ([]*sqlbase.MutableTableDescriptor, error) {
	refs, err := table.FindAllReferences()
	if err != nil {
		return nil, err
	}
	tables := make([]*sqlbase.MutableTableDescriptor, 0, len(refs))
	for id := range refs {
		if id == table.ID {
			continue
		}
		t, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}

	return tables, nil
}

// reassign all the references from oldID to newID.
func reassignReferencedTables(
	tables []*sqlbase.MutableTableDescriptor, oldID, newID sqlbase.ID,
) (bool, error) {
	changed := false
	for _, table := range tables {
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			for j, a := range index.Interleave.Ancestors {
				if a.TableID == oldID {
					index.Interleave.Ancestors[j].TableID = newID
					changed = true
				}
			}
			for j, c := range index.InterleavedBy {
				if c.Table == oldID {
					index.InterleavedBy[j].Table = newID
					changed = true
				}
			}

			if index.ForeignKey.IsSet() {
				if to := index.ForeignKey.Table; to == oldID {
					index.ForeignKey.Table = newID
					changed = true
				}
			}

			origRefs := index.ReferencedBy
			index.ReferencedBy = nil
			for _, ref := range origRefs {
				if ref.Table == oldID {
					ref.Table = newID
					changed = true
				}
				index.ReferencedBy = append(index.ReferencedBy, ref)
			}
			return nil
		}); err != nil {
			return false, err
		}

		for i, dest := range table.DependsOn {
			if dest == oldID {
				table.DependsOn[i] = newID
				changed = true
			}
		}
		if table.ViewUpdatable {
			table.ViewDependsOn = uint32(newID)
		}

		origRefs := table.DependedOnBy
		table.DependedOnBy = nil
		for _, ref := range origRefs {
			if ref.ID == oldID {
				ref.ID = newID
				changed = true
			}
			table.DependedOnBy = append(table.DependedOnBy, ref)
		}
	}
	return changed, nil
}

// reassignComment reassign comment on table
func reassignComment(
	ctx context.Context, p *planner, oldTableDesc, newTableDesc *sqlbase.MutableTableDescriptor,
) error {
	comment, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRow(
		ctx,
		"select-table-comment",
		p.txn,
		`SELECT comment FROM system.comments WHERE type=$1 AND object_id=$2`,
		keys.TableCommentType,
		oldTableDesc.ID)
	if err != nil {
		return err
	}

	if comment != nil {
		_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
			ctx,
			"set-table-comment",
			p.txn,
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.TableCommentType,
			newTableDesc.ID,
			comment[0])
		if err != nil {
			return err
		}

		_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
			ctx,
			"delete-comment",
			p.txn,
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.TableCommentType,
			oldTableDesc.ID)
		if err != nil {
			return err
		}
	}

	for _, column := range oldTableDesc.Columns {
		err = reassignColumnComment(ctx, p, oldTableDesc.ID, newTableDesc.ID, column.ID)
		if err != nil {
			return err
		}
	}

	for _, indexDesc := range oldTableDesc.Indexes {
		err = reassignIndexComment(ctx, p, oldTableDesc.ID, newTableDesc.ID, indexDesc.ID)
		if err != nil {
			return err
		}
	}

	mapConstraint, err := oldTableDesc.GetConstraintInfo(ctx, p.txn)
	if err != nil {
		return err
	}
	for _, v := range mapConstraint {
		err = reassignConstraintComment(ctx, p, oldTableDesc.ID, newTableDesc.ID, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// reassignColumnComment reassign comment on column
func reassignColumnComment(
	ctx context.Context, p *planner, oldID sqlbase.ID, newID sqlbase.ID, columnID sqlbase.ColumnID,
) error {
	comment, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRow(
		ctx,
		"select-column-comment",
		p.txn,
		`SELECT comment FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3`,
		keys.ColumnCommentType,
		oldID,
		columnID)
	if err != nil {
		return err
	}

	if comment != nil {
		_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
			ctx,
			"set-column-comment",
			p.txn,
			"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
			keys.ColumnCommentType,
			newID,
			columnID,
			comment[0])
		if err != nil {
			return err
		}

		_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
			ctx,
			"delete-column-comment",
			p.txn,
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
			keys.ColumnCommentType,
			oldID,
			columnID)
		if err != nil {
			return err
		}
	}

	return nil
}

// reassignIndexComment reassigns a comment on an index.
func reassignIndexComment(
	ctx context.Context, p *planner, oldTableID, newTableID sqlbase.ID, indexID sqlbase.IndexID,
) error {
	comment, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRow(
		ctx,
		"select-index-comment",
		p.txn,
		`SELECT comment FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3`,
		keys.IndexCommentType,
		oldTableID,
		indexID)
	if err != nil {
		return err
	}

	if comment != nil {
		err = p.upsertIndexComment(
			ctx,
			newTableID,
			indexID,
			string(tree.MustBeDString(comment[0])))
		if err != nil {
			return err
		}

		err = p.removeIndexComment(ctx, oldTableID, indexID)
		if err != nil {
			return err
		}
	}

	return nil
}

// reassignConstraintComment reassigns a comment on an constraint.
func reassignConstraintComment(
	ctx context.Context,
	p *planner,
	oldTableID, newTableID sqlbase.ID,
	constraint sqlbase.ConstraintDetail,
) error {
	constraintID := transformConstraint(oldTableID, constraint)
	comment, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRow(
		ctx,
		"select-constraint-comment",
		p.txn,
		`SELECT comment FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3`,
		keys.ConstraintCommentType,
		oldTableID,
		constraintID)
	if err != nil {
		return err
	}

	if comment != nil {
		err = p.upsertConstraintComment(
			ctx,
			newTableID,
			constraint,
			string(tree.MustBeDString(comment[0])))
		if err != nil {
			return err
		}

		err = p.removeConstraintComment(ctx, oldTableID, constraint)
		if err != nil {
			return err
		}
	}

	return nil
}

// ClearTableDataInChunks truncates the data of a table in chunks. It deletes a
// range of data for the table, which includes the PK and all indexes.
// The table has already been marked for deletion and has been purged from the
// descriptor cache on all nodes.
//
// TODO(vivek): No node is reading/writing data on the table at this stage,
// therefore the entire table can be deleted with no concern for conflicts (we
// can even eliminate the need to use a transaction for each chunk at a later
// stage if it proves inefficient).
func ClearTableDataInChunks(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, db *client.DB, traceKV bool,
) error {
	const chunkSize = TableTruncateChunkSize
	var resume roachpb.Span
	alloc := &sqlbase.DatumAlloc{}
	for rowIdx, done := 0, false; !done; rowIdx += chunkSize {
		resumeAt := resume
		if traceKV {
			log.VEventf(ctx, 2, "table %s truncate at row: %d, span: %s", tableDesc.Name, rowIdx, resume)
		}
		if err := db.TxnWithSteppingEnabled(ctx, func(ctx context.Context, txn *client.Txn) error {
			rd, err := row.MakeDeleter(
				txn,
				sqlbase.NewImmutableTableDescriptor(*tableDesc),
				nil,
				nil,
				row.SkipFKs,
				nil, /* *tree.EvalContext */
				alloc,
			)
			if err != nil {
				return err
			}
			td := tableDeleter{rd: rd, alloc: alloc}
			if err := td.init(txn, nil /* *tree.EvalContext */); err != nil {
				return err
			}
			resume, err = td.deleteAllRows(ctx, resumeAt, chunkSize, traceKV)
			return err
		}); err != nil {
			return err
		}
		done = resume.Key == nil
	}
	return nil
}

// UpdateTableDropTimeIfTableEnabled 如果Table开启了flashback更新dropTime
func UpdateTableDropTimeIfTableEnabled(
	ctx context.Context,
	executor *InternalExecutor,
	txn *client.Txn,
	dropTime time.Time,
	tblID sqlbase.ID,
) error {
	rows, err := executor.QueryRow(
		ctx,
		"SELECT DB FLASHBACK",
		txn,
		`SELECT ttl_days FROM system.flashback WHERE type=$1 AND object_id = $2`,
		tree.TableFlashbackType,
		tblID,
	)
	if err != nil {
		return err
	}
	if rows != nil {
		if _, err := executor.Exec(
			ctx,
			"update flashback record",
			txn,
			`UPDATE system.flashback SET drop_time = $1 WHERE (type = $2) AND (object_id = $3)`,
			dropTime,
			tree.TableFlashbackType,
			tblID,
		); err != nil {
			return errors.Wrapf(err, "failed to update table drop time record of the flashback table")
		}
	}
	return nil
}
