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
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgnotice"
	"github.com/znbasedb/znbase/pkg/sql/schemachange"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/stats"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"golang.org/x/text/language"
)

type alterTableNode struct {
	n         *tree.AlterTable
	tableDesc *MutableTableDescriptor
	// statsData is populated with data for "alter table inject statistics"
	// commands - the JSON stats expressions.
	// It is parallel with n.Cmds (for the inject stats commands).
	statsData map[int]tree.TypedExpr
}

// AlterTable applies a schema change on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterTable(ctx context.Context, n *tree.AlterTable) (planNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, requireTableDesc)
	if errors.Is(err, errNoPrimaryKey) {
		if len(n.Cmds) > 0 && isAlterCmdValidWithoutPrimaryKey(n.Cmds[0]) {
			tableDesc, err = p.ResolveMutableTableDescriptorExAllowNoPrimaryKey(
				ctx, p, n.Table, !n.IfExists, true, requireTableDesc,
			)
		}
	}
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	var parentDesc sqlbase.DescriptorProto
	dbDesc, err := p.ResolveUncachedDatabase(ctx, &n.Table)
	if err != nil {
		return nil, err
	}
	parentDesc = dbDesc
	scName := string(n.Table.SchemaName)
	if dbDesc.ID != keys.SystemDatabaseID {
		if isInternal := CheckVirtualSchema(scName); isInternal {
			return nil, fmt.Errorf("schema cannot be modified: %q", n.Table.SchemaName.String())
		}
		scDesc, err := dbDesc.GetSchemaByName(scName)
		if err != nil {
			return nil, err
		}
		parentDesc = scDesc
	}
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, parentDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	n.HoistAddColumnConstraints()

	// See if there's any "inject statistics" in the query and type check the
	// expressions.
	statsData := make(map[int]tree.TypedExpr)
	for i, cmd := range n.Cmds {
		injectStats, ok := cmd.(*tree.AlterTableInjectStats)
		if !ok {
			continue
		}
		typedExpr, err := p.analyzeExpr(
			ctx, injectStats.Stats,
			nil, /* sources - no name resolution */
			tree.IndexedVarHelper{},
			types.JSON, true, /* requireType */
			"INJECT STATISTICS" /* typingContext */)
		if err != nil {
			return nil, err
		}
		statsData[i] = typedExpr
	}
	return &alterTableNode{n: n, tableDesc: tableDesc, statsData: statsData}, nil
}

func isAlterCmdValidWithoutPrimaryKey(cmd tree.AlterTableCmd) bool {
	switch t := cmd.(type) {
	case *tree.AlterTableAlterPrimaryKey:
		return true
	case *tree.AlterTableAddConstraint:
		cs, ok := t.ConstraintDef.(*tree.UniqueConstraintTableDef)
		if ok && cs.PrimaryKey {
			return true
		}
	default:
		return false
	}
	return false
}

func (n *alterTableNode) startExec(params runParams) error {
	//start := timeutil.Now()
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.

	//by lz
	//when the local variable descriptorChanged in any case is true, descriptorChangedGlobal is true
	descriptorChangedGlobal := false
	useLocateIn := false
	origNumMutations := len(n.tableDesc.Mutations)
	hasPrimaryKey := n.tableDesc.HasPrimaryKey()
	var oldLocationNums, newLocationNums int32
	//var droppedViews []string
	tn := &n.n.Table
	if err := CheckTableSnapShots(params.ctx,
		params.ExecCfg().InternalExecutor,
		params.EvalContext().Txn,
		n.tableDesc,
		"cannot alter table that has snapshots"); err != nil {
		return err
	}

	for i, cmd := range n.n.Cmds {

		if !hasPrimaryKey && !isAlterCmdValidWithoutPrimaryKey(cmd) {
			return errors.Errorf("table %q does not have a primary key, cannot perform%s", n.tableDesc.Name, tree.AsString(cmd))
		}

		switch t := cmd.(type) {
		case *tree.AlterTableDropIndex:
			ctx := params.ctx
			// Need to retrieve the descriptor again for each index name in
			// the list: when two or more index names refer to the same table,
			// the mutation list and new version number created by the first
			// drop need to be visible to the second drop.
			tableDesc, err := params.p.ResolveMutableTableDescriptor(
				ctx, &n.n.Table, true /*required*/, requireTableOrViewDesc)
			if err != nil {
				// Somehow the descriptor we had during newPlan() is not there
				// any more.
				return errors.Wrapf(err, "table descriptor for %q became unavailable within same txn",
					tree.ErrString(&n.n.Table))
			}
			if tableDesc.IsView() && !tableDesc.MaterializedView() {
				return pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
			}
			if err := params.p.dropIndexByName(
				ctx, &n.n.Table, t.Index, tableDesc, n.n.IfExists, t.DropBehavior, checkIdxConstraint,
				tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType),
			); err != nil {
				return err
			}
			return nil

		case *tree.AlterTableRenameIndex:
			p := params.p
			ctx := params.ctx
			tableDesc := n.tableDesc
			idx, _, err := tableDesc.FindIndexByName(string(t.OldIndexName))
			if err != nil {
				return err
			}
			for _, tableRef := range tableDesc.DependedOnBy {
				if tableRef.IndexID != idx.ID {
					continue
				}
				return p.dependentViewRenameError(
					ctx, "index", t.OldIndexName.String(), tableDesc.ParentID, tableRef.ID)
			}
			if t.NewIndexName == "" {
				return errEmptyIndexName
			}
			if t.OldIndexName == t.NewIndexName {
				// Noop.
				return nil
			}
			if _, _, err := tableDesc.FindIndexByName(string(t.NewIndexName)); err == nil {
				return fmt.Errorf("index name %q already exists", string(t.NewIndexName))
			}
			if err := p.updateCheckConstraintComment(
				ctx,
				tableDesc.ID,
				sqlbase.ConstraintDetail{
					Kind:  sqlbase.ConstraintTypeUnique,
					Index: &sqlbase.IndexDescriptor{Name: idx.Name},
				},
				string(t.NewIndexName)); err != nil {
				return err
			}

			if err := tableDesc.RenameIndexDescriptor(idx, string(t.NewIndexName)); err != nil {
				return err
			}

			if err := tableDesc.RefreshValidate(ctx, p.txn, p.EvalContext().Settings); err != nil {
				return err
			}

			return p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)

		case *tree.AlterTableAddIndex:
			_, dropped, err := n.tableDesc.FindIndexByName(string(t.Name))
			if err == nil {
				if dropped {
					return fmt.Errorf("index %q being dropped, try again later", string(t.Name))
				}
				if t.IfNotExists {
					return nil
				}
			}

			for _, c := range t.Columns {
				if c.IsFuncIndex() {
					semaContext := tree.MakeSemaContext()
					semaContext.Properties.Require("CREATE FUNCTION INDEX", 26)
					semactx := &semaContext
					var searchPath sessiondata.SearchPath
					searchPath = semaContext.SearchPath
					funcexpr, ok := c.Function.(*tree.FuncExpr)
					if !ok {
						return errors.Errorf("Non-function index is not allowed")
					}
					def, err := funcexpr.Func.Resolve(searchPath, false)
					if err != nil {
						return err
					}

					if err := semactx.CheckFunctionUsage(funcexpr, def); err != nil {
						return errors.Wrapf(err, "%s()", def.Name)
					}
					for _, expr := range funcexpr.Exprs {
						if _, ok := expr.(*tree.FuncExpr); ok {
							return errors.Errorf("Nested-function index is not allowed")
						}
					}
				}
			}

			indexDesc, err := MakeAddIndexDescriptor(n, t)
			if err != nil {
				return err
			}
			n.tableDesc.LocationNums += indexDesc.LocationNums
			mutationIdx := len(n.tableDesc.Mutations)
			if err := n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD); err != nil {
				return err
			}
			if err := n.tableDesc.AllocateIDs(); err != nil {
				return err
			}
			// The index name may have changed as a result of
			// AllocateIDs(). Retrieve it for the event log below.
			index := n.tableDesc.Mutations[mutationIdx].GetIndex()
			indexName := index.Name
			mutationID, err := params.p.createOrUpdateSchemaChangeJob(params.ctx, n.tableDesc,
				tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType))
			if err != nil {
				return err
			}
			if err := params.p.writeSchemaChange(params.ctx, n.tableDesc, mutationID); err != nil {
				return err
			}
			// Record index creation in the event log. This is an auditable log
			// event and is recorded in the same transaction as the table descriptor
			// update.
			params.p.curPlan.auditInfo = &server.AuditInfo{
				EventTime: timeutil.Now(),
				EventType: string(EventLogCreateIndex),
				TargetInfo: &server.TargetInfo{
					TargetID: int32(n.tableDesc.ID),
					Desc: struct {
						User      string
						TableName string
						IndexName string
					}{
						params.SessionData().User,
						n.n.Table.FQString(),
						indexName,
					},
				},
				Info: &infos.CreateIndexInfo{
					TableName: n.n.Table.FQString(), IndexName: indexName, Statement: n.n.String(),
					User: params.SessionData().User, MutationID: uint32(mutationID),
				},
			}
			return nil
		case *tree.AlterTableAddColumn:
			d := t.ColumnDef
			if d.HasFKConstraint() {
				return pgerror.UnimplementedWithIssueError(32917,
					"adding a REFERENCES constraint while also adding a column via ALTER not supported")
			}

			newDef, seqDbDesc, seqName, seqOpts, err := params.p.processSerialInColumnDef(params.ctx, d, tn)
			if err != nil {
				return err
			}
			if seqName != nil {
				if err := doCreateSequence(params, n.n.String(), seqDbDesc, seqName, seqOpts); err != nil {
					return err
				}
			}
			d = newDef

			columnPrivs := sqlbase.InheritFromTablePrivileges(n.tableDesc.Privileges)
			col, idx, expr, err := sqlbase.MakeColumnDefDescs(d, &params.p.semaCtx, columnPrivs, n.tableDesc.ID)
			if err != nil {
				return err
			}

			// If the new column has a DEFAULT expression that uses a sequence, add references between
			// its descriptor and this column descriptor.
			if d.HasDefaultExpr() {
				changedSeqDescs, err := maybeAddSequenceDependencies(params.ctx, params.p, n.tableDesc, col, expr)
				if err != nil {
					return err
				}
				for _, changedSeqDesc := range changedSeqDescs {
					if err := params.p.writeSchemaChange(params.ctx, changedSeqDesc, sqlbase.InvalidMutationID); err != nil {
						return err
					}
				}
			}

			if d.PrimaryKey {
				// default rowid primary index or if a DROP PRIMARY KEY statement
				// was processed before this statement. If a DROP PRIMARY KEY
				// statement was processed, then n.tableDesc.HasPrimaryKey() = false.
				if hasPrimaryKey && !n.tableDesc.IsPrimaryIndexDefaultRowID() {
					return pgerror.Newf(pgcode.InvalidTableDefinition,
						"multiple primary keys for table %q are not allowed", n.tableDesc.Name)
				}
			}

			// We're checking to see if a user is trying add a non-nullable column without a default to a
			// non empty table by scanning the primary index span with a limit of 1 to see if any key exists.
			if !col.Nullable && (col.DefaultExpr == nil && !col.IsComputed()) {
				kvs, err := params.p.txn.Scan(params.ctx, n.tableDesc.PrimaryIndexSpan().Key, n.tableDesc.PrimaryIndexSpan().EndKey, 1)
				if err != nil {
					return err
				}
				if len(kvs) > 0 {
					return sqlbase.NewNonNullViolationError(col.Name)
				}
			}
			_, dropped, err := n.tableDesc.FindColumnByName(d.Name)
			if err == nil {
				if dropped {
					return fmt.Errorf("column %q being dropped, try again later", col.Name)
				}
				if t.IfNotExists {
					continue
				}
			}

			n.tableDesc.AddColumnMutation(*col, sqlbase.DescriptorMutation_ADD)
			if idx != nil {
				if err := n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD); err != nil {
					return err
				}
			}
			if d.HasColumnFamily() {
				err := n.tableDesc.AddColumnToFamilyMaybeCreate(
					col.Name, string(d.Family.Name), d.Family.Create,
					d.Family.IfNotExists)
				if err != nil {
					return err
				}
			}

			if d.IsComputed() {
				if len(n.tableDesc.InheritsBy) > 0 {
					return fmt.Errorf("compute column can not be inherited")
				}
				if err := validateComputedColumn(n.tableDesc, d, &params.p.semaCtx); err != nil {
					return err
				}
			}

			for _, id := range n.tableDesc.InheritsBy {
				inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
				if err != nil {
					return err
				}
				if inhTableDesc == nil {
					return fmt.Errorf(`cannot find inherit table id: %d`, id)
				}
				err = params.p.addInheritCol(params.ctx, params, n.n, inhTableDesc, col)
				if err != nil {
					return err
				}
			}

		case *tree.AlterTableAddConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			inuseNames := make(map[string]struct{}, len(info))
			for k := range info {
				inuseNames[k] = struct{}{}
			}
			switch d := t.ConstraintDef.(type) {
			case *tree.UniqueConstraintTableDef:
				if d.PrimaryKey {
					// default rowid primary index or if a DROP PRIMARY KEY statement
					// was processed before this statement. If a DROP PRIMARY KEY
					// statement was processed, then n.tableDesc.HasPrimaryKey() = false.
					if hasPrimaryKey && !n.tableDesc.IsPrimaryIndexDefaultRowID() {
						return pgerror.Newf(pgcode.InvalidTableDefinition,
							"multiple primary keys for table %q are not allowed", n.tableDesc.Name)
					}
					// Translate this operation into an ALTER PRIMARY KEY command.
					alterPK := &tree.AlterTableAlterPrimaryKey{
						Columns:    d.Columns,
						Interleave: d.Interleave,
						Name:       d.Name,
					}
					//if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, alterPK, info, false); err != nil {
					if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, alterPK, info); err != nil {
						return err
					}
					hasPrimaryKey = true
					continue
				}
				idx := sqlbase.IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					StoreColumnNames: d.Storing.ToStrings(),
				}
				if err := idx.FillColumns(d.Columns); err != nil {
					return err
				}
				if d.PartitionBy != nil {
					partitioning, err := CreatePartitioning(
						params.ctx, params.p.ExecCfg().Settings,
						params.EvalContext(), n.tableDesc, &idx, d.PartitionBy, params.StatusServer())
					if err != nil {
						return err
					}
					idx.Partitioning = partitioning
				}
				_, dropped, err := n.tableDesc.FindIndexByName(string(d.Name))
				if err == nil {
					if dropped {
						return fmt.Errorf("index %q being dropped, try again later", d.Name)
					}
				}
				if err := n.tableDesc.AddIndexMutation(&idx, sqlbase.DescriptorMutation_ADD); err != nil {
					return err
				}

			case *tree.CheckConstraintTableDef:
				ck, err := MakeCheckConstraint(params.ctx,
					n.tableDesc, d, inuseNames, &params.p.semaCtx, n.n.Table)
				if err != nil {
					return err
				}
				if detail, ok := info[ck.Name]; ok {
					// 同名约束存在，并且不是check约束
					if detail.CheckConstraint == nil {
						return fmt.Errorf(`constraint "%s" for relation "%s" already exists`, ck.Name, n.tableDesc.Name)
					}
					if detail.CheckConstraint.Expr == ck.Expr {
						if detail.CheckConstraint.IsInherits {
							detail.CheckConstraint.IsInherits = false
							detail.CheckConstraint.InhCount++
							detail.CheckConstraint.Able = ck.Able
							descriptorChangedGlobal = true
						} else {
							return fmt.Errorf(`constraint "%s" for relation "%s" already exists`, ck.Name, n.tableDesc.Name)
						}
					} else {
						return fmt.Errorf(`constraint "%s" for relation "%s" already exists`, ck.Name, n.tableDesc.Name)
					}
				} else {
					ck.Validity = sqlbase.ConstraintValidity_Validating
					n.tableDesc.AddCheckValidationMutation(ck)
				}

				for _, id := range n.tableDesc.InheritsBy {
					inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
					if err != nil {
						return err
					}
					if inhTableDesc == nil {
						return fmt.Errorf(`cannot find inherit table id: %d`, id)
					}
					err = params.p.addInheritCheckConstraint(params.ctx, params, n.n, inhTableDesc, d, &params.p.semaCtx, ck.Name)
					if err != nil {
						return err
					}
				}

			case *tree.ForeignKeyConstraintTableDef:
				foreignTable, err := ResolveExistingObject(
					params.ctx, params.p, &d.Table, true /*required*/, requireTableDesc,
				)
				if err != nil {
					return err
				}
				if err := params.p.CheckPrivilege(params.ctx, foreignTable, privilege.REFERENCES); err != nil {
					return err
				}
				for _, colName := range d.FromCols {
					col, err := n.tableDesc.FindActiveColumnByName(string(colName))
					if err != nil {
						if _, dropped, inactiveErr := n.tableDesc.FindColumnByName(colName); inactiveErr == nil && !dropped {
							return pgerror.UnimplementedWithIssueError(32917,
								"adding a REFERENCES constraint while the column is being added not supported")
						}
						return err
					}

					if err := col.CheckCanBeFKRef(); err != nil {
						return err
					}
				}
				affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)

				// If there are any FKs, we will need to update the table descriptor of the
				// depended-on table (to register this table against its DependedOnBy field).
				// This descriptor must be looked up uncached, and we'll allow FK dependencies
				// on tables that were just added. See the comment at the start of
				// the global-scope resolveFK().
				// TODO(vivek): check if the cache can be used.
				params.p.runWithOptions(resolveFlags{skipCache: true}, func() {
					// Check whether the table is empty, and pass the result to resolveFK(). If
					// the table is empty, then resolveFK will automatically add the necessary
					// index for a fk constraint if the index does not exist.
					kvs, scanErr := params.p.txn.Scan(params.ctx, n.tableDesc.PrimaryIndexSpan().Key, n.tableDesc.PrimaryIndexSpan().EndKey, 1)
					if scanErr != nil {
						err = scanErr
						return
					}
					var tableState FKTableState
					if len(kvs) == 0 {
						tableState = EmptyTable
					} else {
						tableState = NonEmptyTable
					}
					err = params.p.resolveFK(params.ctx, n.tableDesc, d, affected, tableState)
				})
				if err != nil {
					return err
				}
				descriptorChangedGlobal = true
				for _, updated := range affected {
					if err := params.p.writeSchemaChange(params.ctx, updated, sqlbase.InvalidMutationID); err != nil {
						return err
					}
				}

			default:
				return fmt.Errorf("unsupported constraint: %T", t.ConstraintDef)
			}

		case *tree.AlterTableAlterPrimaryKey:
			if n.tableDesc.PrimaryIndex.Partitioning.IsHashPartition {
				return errors.New("remove hash partition first before altering primary key")
			}
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			//if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, t, info, false); err != nil {
			if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, t, info); err != nil {
				return err
			}

			// N.B. We don't schedule index deletions here because the existing indexes need to be visible to the user
			// until the primary key swap actually occurs. Deletions will get enqueued in the phase when the swap happens.

			// Mark descriptorChanged so that a mutation job is scheduled at the end of startExec.
			descriptorChangedGlobal = true
			hasPrimaryKey = true

		case *tree.AlterTableDropColumn:
			if params.SessionData().SafeUpdates {
				return pgerror.NewDangerousStatementErrorf("ALTER TABLE DROP COLUMN will remove all data in that column")
			}

			col, dropped, err := n.tableDesc.FindColumnByName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return err
			}
			if dropped {
				continue
			}

			if !(col.InhCount == 1 && !col.IsInherits) {
				return fmt.Errorf("inherit column %s can not be dropped", col.Name)
			}

			// If the dropped column uses a sequence, remove references to it from that sequence.
			if len(col.UsesSequenceIds) > 0 {
				if err := removeSequenceDependencies(n.tableDesc, &col, params, true); err != nil {
					return err
				}
			}
			// You can't remove a column that owns a sequence that is depended on
			// by another column
			if err := params.p.canRemoveAllColumnOwnedSequences(params.ctx, n.tableDesc, &col, t.DropBehavior); err != nil {
				return err
			}

			// If the dropped column owns a sequence, drop the sequence as well.
			if len(col.OwnsSequenceIds) > 0 {
				if err := params.p.dropSequencesOwnedByCol(params.ctx, &col, true /* queueJob */, t.DropBehavior); err != nil {
					return err
				}
			}
			//// If the dropped column owns a sequence, drop the sequence as well.
			//if len(col.OwnsSequenceIds) > 0 {
			//	if err := dropSequencesOwnedByCol(&col, params); err != nil {
			//		return err
			//	}
			//}

			// You can't drop a column depended on by a view unless CASCADE was
			// specified.
			for _, ref := range n.tableDesc.DependedOnBy {
				found := false
				for _, colID := range ref.ColumnIDs {
					if colID == col.ID {
						found = true
						break
					}
				}
				if !found {
					continue
				}
				err := params.p.canRemoveDependentViewGeneric(
					params.ctx, "column", string(t.Column), n.tableDesc.ParentID, ref, t.DropBehavior,
				)
				if err != nil {
					return err
				}
				viewDesc, err := params.p.getViewDescForCascade(
					params.ctx, "column", string(t.Column), n.tableDesc.ParentID, ref.ID, t.DropBehavior,
				)
				if err != nil {
					return err
				}
				_, err = params.p.removeDependentView(params.ctx, n.tableDesc, viewDesc)
				if err != nil {
					return err
				}
			}

			if n.tableDesc.PrimaryIndex.ContainsColumnID(col.ID) {
				return fmt.Errorf("column %q is referenced by the primary key", col.Name)
			}
			for _, idx := range n.tableDesc.AllNonDropIndexes() {
				// We automatically drop indexes on that column that only
				// index that column (and no other columns). If CASCADE is
				// specified, we also drop other indices that refer to this
				// column.  The criteria to determine whether an index "only
				// indexes that column":
				//
				// Assume a table created with CREATE TABLE foo (a INT, b INT).
				// Then assume the user issues ALTER TABLE foo DROP COLUMN a.
				//
				// INDEX i1 ON foo(a) -> i1 deleted
				// INDEX i2 ON foo(a) STORING(b) -> i2 deleted
				// INDEX i3 ON foo(a, b) -> i3 not deleted unless CASCADE is specified.
				// INDEX i4 ON foo(b) STORING(a) -> i4 not deleted unless CASCADE is specified.

				// containsThisColumn becomes true if the index is defined
				// over the column being dropped.
				containsThisColumn := false
				// containsOnlyThisColumn becomes false if the index also
				// includes non-PK columns other than the one being dropped.
				containsOnlyThisColumn := true

				// Analyze the index.
				for _, id := range idx.ColumnIDs {
					if id == col.ID {
						containsThisColumn = true
					} else {
						containsOnlyThisColumn = false
					}
				}
				for _, id := range idx.ExtraColumnIDs {
					if n.tableDesc.PrimaryIndex.ContainsColumnID(id) {
						// All secondary indices necessary contain the PK
						// columns, too. (See the comments on the definition of
						// IndexDescriptor). The presence of a PK column in the
						// secondary index should thus not be seen as a
						// sufficient reason to reject the DROP.
						continue
					}
					if id == col.ID {
						containsThisColumn = true
					}
				}
				// The loop above this comment is for the old STORING encoding. The
				// loop below is for the new encoding (where the STORING columns are
				// always in the value part of a KV).
				for _, id := range idx.StoreColumnIDs {
					if id == col.ID {
						containsThisColumn = true
					}
				}

				// If the column being dropped is referenced in the partial
				// index predicate, then the index should be dropped.
				if !containsThisColumn && idx.IsPartial() {
					expr, err := parser.ParseExpr(idx.PredExpr)
					if err != nil {
						return err
					}

					colIDs, err := schemaexpr.ExtractColumnIDs(n.tableDesc.TableDescriptor, expr)
					if err != nil {
						return err
					}

					if colIDs.Contains(int(col.ID)) {
						containsThisColumn = true
					}
				}

				// Perform the DROP.
				if containsThisColumn {
					if containsOnlyThisColumn || t.DropBehavior == tree.DropCascade {
						if err := params.p.dropIndexByName(
							params.ctx, tn, tree.UnrestrictedName(idx.Name), n.tableDesc, false,
							t.DropBehavior, ignoreIdxConstraint,
							tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType),
						); err != nil {
							return err
						}
					} else {
						return fmt.Errorf("column %q is referenced by existing index %q", col.Name, idx.Name)
					}
				}
			}

			// Drop check constraints which reference the column.
			validChecks := n.tableDesc.Checks[:0]
			for _, check := range n.tableDesc.AllActiveAndInactiveChecks() {
				if used, err := check.UsesColumn(n.tableDesc.TableDesc(), col.ID); err != nil {
					return err
				} else if used {
					if check.Validity == sqlbase.ConstraintValidity_Validating {
						return fmt.Errorf("referencing constraint %q in the middle of being added, try again later", check.Name)
					}
				} else {
					validChecks = append(validChecks, check)
				}
			}

			if len(validChecks) != len(n.tableDesc.Checks) {
				n.tableDesc.Checks = validChecks
				descriptorChangedGlobal = true
			}

			if err != nil {
				return err
			}
			if err := params.p.removeColumnComment(params.ctx, n.tableDesc.ID, col.ID); err != nil {
				return err
			}

			found := false
			for i := range n.tableDesc.Columns {
				if n.tableDesc.Columns[i].ID == col.ID {
					n.tableDesc.AddColumnMutation(col, sqlbase.DescriptorMutation_DROP)
					n.tableDesc.Columns = append(n.tableDesc.Columns[:i], n.tableDesc.Columns[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("column %q in the middle of being added, try again later", t.Column)
			}

			for _, id := range n.tableDesc.InheritsBy {
				inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
				if err != nil {
					return err
				}
				if inhTableDesc == nil {
					return fmt.Errorf(`cannot find inherit table id: %d`, id)
				}
				err = params.p.dropInheritColumn(params.ctx, params, n.n, inhTableDesc, col.Name, t.DropBehavior)
				if err != nil {
					return err
				}
			}

		case *tree.AlterTableDropConstraint:
			if n.tableDesc.PrimaryIndex.Partitioning.IsHashPartition {
				return errors.New("remove hash partition first before altering primary key")
			}
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			if t.Constraint == "" {
				name = n.tableDesc.PrimaryIndex.Name
			}
			details, ok := info[name]
			if !ok {
				if t.IfExists {
					continue
				}
				return fmt.Errorf("constraint %q does not exist", t.Constraint)
			}
			constraint, _, err := n.tableDesc.FindConstraintByName(params.ctx, params.p.txn, name)
			if err != nil {
				return err
			}
			if err := params.p.removeConstraintComment(params.ctx, n.tableDesc.ID, *constraint); err != nil {
				return err
			}
			mapReferences, err := params.p.findAllReferences(params.ctx, *n.tableDesc)
			if err != nil {
				return err
			}

			for _, ref := range mapReferences {
				constraintDetail, err := ref.GetConstraintInfo(params.ctx, params.p.Txn())
				if err != nil {
					return err
				}
				for _, constraint := range constraintDetail {
					if details.Kind == sqlbase.ConstraintTypePK && constraint.Kind == sqlbase.ConstraintTypeFK {
						if t.DropBehavior == tree.DropCascade {
							return pgerror.NewError(pgcode.FeatureNotSupported, "not support drop primary key with cascade")
						}
						msg := fmt.Sprintf("cannot drop constraint %s on %s because other objects depend on it", name, n.tableDesc.Name)
						return sqlbase.NewDependentObjectErrorWithHint(msg, "Use DROP ... CASCADE to drop the dependent objects too.")
					}
				}
			}
			if details.FK != nil {
				referencedTable := &sqlbase.TableDescriptor{}
				if err := getDescriptorByID(params.ctx, params.p.Txn(), details.FK.Table, referencedTable); err != nil {
					return err
				}
				if err := params.p.CheckPrivilege(params.ctx, referencedTable, privilege.REFERENCES); err != nil {
					return err
				}
			}
			if err := n.tableDesc.DropConstraint(
				name, details,
				func(desc *sqlbase.MutableTableDescriptor, idx *sqlbase.IndexDescriptor) error {
					return params.p.removeFKBackReference(params.ctx, desc, idx)
				}); err != nil {
				return err
			}
			descriptorChangedGlobal = true
			if details.Kind == sqlbase.ConstraintTypePK {

				// Ensure a Primary Key exists.
				nameExists := func(name string) bool {
					_, _, err := n.tableDesc.FindColumnByName(tree.Name(name))
					return err == nil
				}
				s := "unique_rowid()"
				name := sqlbase.GenerateUniqueConstraintName("rowid", nameExists)
				col := sqlbase.ColumnDescriptor{
					Name: name,
					Type: sqlbase.ColumnType{
						SemanticType:    sqlbase.ColumnType_INT,
						VisibleTypeName: coltypes.VisibleTypeName[coltypes.VisINT],
					},
					DefaultExpr: &s,
					Hidden:      true,
					Nullable:    false,
				}
				n.tableDesc.AddColumnMutation(col, sqlbase.DescriptorMutation_ADD)
				alterPK := &tree.AlterTableAlterPrimaryKey{
					Columns: tree.IndexElemList{
						tree.IndexElem{Column: tree.Name(name)},
					},
					Interleave: nil,
				}
				//if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, alterPK, info, true); err != nil {
				if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, alterPK, info); err != nil {
					return err
				}
				hasPrimaryKey = false
				continue
			}
			if details.Kind == sqlbase.ConstraintTypeCheck {

				for _, id := range n.tableDesc.InheritsBy {
					inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
					if err != nil {
						return err
					}
					if inhTableDesc == nil {
						return fmt.Errorf(`cannot find inherit table id: %d`, id)
					}
					err = params.p.dropInheritCheck(params.ctx, inhTableDesc, t.Constraint.String())
					if err != nil {
						return err
					}
				}
			}

		case *tree.AlterTableAbleConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			details, ok := info[name]
			if !ok {
				if t.IfExists {
					continue
				}
				return fmt.Errorf("constraint %q does not exist", t.Constraint)
			}
			if details.ReferencedIndex != nil {
				if err := params.p.CheckPrivilege(params.ctx, details.ReferencedTable, privilege.REFERENCES); err != nil {
					return err
				}
			}
			if err := n.tableDesc.AbleConstraint(
				name,
				details,
				t.Able); err != nil {
				return err
			}
			descriptorChangedGlobal = true

			var alterTableMap = make(map[sqlbase.ID]struct{})
			for _, id := range n.tableDesc.InheritsBy {
				alterTableMap[id] = struct{}{}
			}

			for _, id := range n.tableDesc.InheritsBy {
				inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
				if err != nil {
					return err
				}
				if inhTableDesc == nil {
					return fmt.Errorf(`cannot find inherit table id: %d`, id)
				}
				err = params.p.ableInheritCheck(params.ctx, inhTableDesc, name, t.Able, alterTableMap)
				if err != nil {
					return err
				}
			}

		case *tree.AlterTableAbleFlashBack:
			dbDesc, err := params.p.ResolveUncachedDatabase(params.ctx, tn)
			if err != nil {
				return err
			}

			if err := checkDatabaseFlashback(
				params.ctx,
				params.ExecCfg().InternalExecutor,
				params.EvalContext().Txn,
				dbDesc.ID,
				int(t.TTLDays),
				t.Able,
			); err != nil {
				return err
			}

			ttlDays := int8(t.TTLDays)
			if err = UpdateFlashback(
				params.ctx,
				params.EvalContext().Txn,
				params.ExecCfg().InternalExecutor,
				n.tableDesc.ID,
				n.tableDesc.ParentID,
				dbDesc.ID,
				n.tableDesc.GetName(),
				timeutil.Now(),
				t.Able,
				ttlDays,
				true,
				tree.TableFlashbackType,
			); err != nil {
				return err
			}
			if err = UpdateDescNamespaceFlashbackRecord(
				params.ctx,
				params.EvalContext().Txn,
				params.ExecCfg().InternalExecutor,
				timeutil.Now(),
			); err != nil {
				return err
			}

		case *tree.AlterTableNoInherit:

			fatherTableDesc, err := params.p.ResolveMutableTableDescriptor(params.ctx, &t.Table, true, requireTableDesc)
			if err != nil {
				return err
			}
			if fatherTableDesc == nil {
				return fmt.Errorf("table '%s' does not exist", string(t.Table.TableName))
			}
			if len(fatherTableDesc.InheritsBy) == 0 || len(n.tableDesc.Inherits) == 0 {
				return fmt.Errorf(`relation "%s" is not a parent of relation "%s"`, fatherTableDesc.Name, n.tableDesc.Name)
			}
			for i, id := range fatherTableDesc.InheritsBy {
				if id == n.tableDesc.ID {
					fatherTableDesc.InheritsBy = append(fatherTableDesc.InheritsBy[:i], fatherTableDesc.InheritsBy[i+1:]...)
					break
				}
				if i == len(fatherTableDesc.InheritsBy)-1 {
					return fmt.Errorf(`relation "%s" is not a parent of relation "%s"`, fatherTableDesc.Name, n.tableDesc.Name)
				}
			}
			for i, id := range n.tableDesc.Inherits {
				if id == fatherTableDesc.ID {
					n.tableDesc.Inherits = append(n.tableDesc.Inherits[:i], n.tableDesc.Inherits[i+1:]...)
					break
				}
				if i == len(n.tableDesc.Inherits)-1 {
					return fmt.Errorf(`relation "%s" is not a parent of relation "%s"`, fatherTableDesc.Name, n.tableDesc.Name)
				}
			}
			for _, col := range fatherTableDesc.Columns {
				if col.Hidden {
					continue
				}
				inhCol, dropped, err := n.tableDesc.FindColumnByName(tree.Name(col.Name))
				if err != nil {
					return fmt.Errorf("cannot find inherit column '%s'", col.Name)
				}
				if dropped {
					continue
				}
				if inhCol.IsInherits {
					if inhCol.InhCount == 1 {
						inhCol.IsInherits = false
					} else {
						inhCol.InhCount--
					}
				} else {
					inhCol.InhCount--
				}
				n.tableDesc.UpdateColumnDescriptor(inhCol)
			}
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			for _, check := range fatherTableDesc.AllActiveAndInactiveChecks() {
				if detail, ok := info[check.Name]; ok {

					if detail.CheckConstraint.IsInherits {
						if detail.CheckConstraint.InhCount == 1 {
							detail.CheckConstraint.IsInherits = false
						} else {
							detail.CheckConstraint.InhCount--
						}
					} else {
						detail.CheckConstraint.InhCount--
					}

				} else {
					continue
				}
			}
			err = params.p.writeSchemaChange(params.ctx, fatherTableDesc, sqlbase.InvalidMutationID)
			if err != nil {
				return err
			}

			descriptorChangedGlobal = true

		case *tree.AlterTableValidateConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			constraint, ok := info[name]
			if !ok {
				return fmt.Errorf("constraint %q does not exist", t.Constraint)
			}
			if !constraint.Unvalidated {
				continue
			}
			switch constraint.Kind {
			case sqlbase.ConstraintTypeCheck:
				found := false
				var idx int
				for idx = range n.tableDesc.Checks {
					if n.tableDesc.Checks[idx].Name == name {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("constraint %q in the middle of being added, try again later", t.Constraint)
				}

				ck := n.tableDesc.Checks[idx]
				if err := validateCheckExpr(
					params.ctx, ck.Expr, n.tableDesc.TableDesc(), params.EvalContext().InternalExecutor, params.EvalContext().Txn,
				); err != nil {
					return err
				}
				n.tableDesc.Checks[idx].Validity = sqlbase.ConstraintValidity_Validated
				descriptorChangedGlobal = true

			case sqlbase.ConstraintTypeFK:
				found := false
				var id sqlbase.IndexID
				for _, idx := range n.tableDesc.AllNonDropIndexes() {
					if idx.ForeignKey.IsSet() && idx.ForeignKey.Name == name {
						found = true
						id = idx.ID
						break
					}
				}
				if !found {
					panic("constraint returned by GetConstraintInfo not found")
				}
				idx, err := n.tableDesc.FindIndexByID(id)
				if err != nil {
					panic(err)
				}
				if err := params.p.validateForeignKey(params.ctx, n.tableDesc.TableDesc(), idx); err != nil {
					return err
				}
				idx.ForeignKey.Validity = sqlbase.ConstraintValidity_Validated
				descriptorChangedGlobal = true

			default:
				return errors.Errorf("validating %s constraint %q unsupported", constraint.Kind, t.Constraint)
			}

		case tree.ColumnMutationCmd:
			// Column mutations
			col, dropped, err := n.tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return err
			}
			if dropped {
				return fmt.Errorf("column %q in the middle of being dropped", t.GetColumn())
			}
			if err := applyColumnMutation(n.tableDesc, &col, t, params); err != nil {
				return err
			}
			n.tableDesc.UpdateColumnDescriptor(col)
			descriptorChangedGlobal = true

		case *tree.AlterTablePartitionBy:
			if t.PartitionBy != nil && t.IsHash {
				var partitioning sqlbase.PartitioningDescriptor
				// deal with hash_partition_quantity
				if t.PartitionBy.IsHashQuantity {
					if t.PartitionBy.List[0].HashParts <= 0 && t.PartitionBy.List[0].Name != "p1" {
						return errors.New("hash parts can not be zero or negative")
					}
					if t.PartitionBy.List[0].Name != "p1" && len(t.PartitionBy.List[0].LocateSpaceNames) != 0 && len(t.PartitionBy.List[0].LocateSpaceNames) != int(t.PartitionBy.List[0].HashParts) {
						return errors.New("count of locate space should be equal to hash parts")
					}
					var partitionlist []tree.ListPartition
					if len(t.PartitionBy.List[0].LocateSpaceNames) != 0 {
						for i := 0; i < int(t.PartitionBy.List[0].HashParts); i++ {
							partIndex := strconv.Itoa(i + 1)
							partName := "p" + partIndex
							tmp := tree.ListPartition{
								Name:            tree.UnrestrictedName(partName),
								LocateSpaceName: t.PartitionBy.List[0].LocateSpaceNames[i],
								IsHash:          true,
							}
							partitionlist = append(partitionlist, tmp)
						}
					} else {
						for i := 0; i < int(t.PartitionBy.List[0].HashParts); i++ {
							partIndex := strconv.Itoa(i + 1)
							partName := "p" + partIndex
							tmp := tree.ListPartition{
								Name:   tree.UnrestrictedName(partName),
								IsHash: true,
							}
							partitionlist = append(partitionlist, tmp)
						}
					}
					if t.PartitionBy.List[0].LocateSpaceName == nil && t.PartitionBy.List[0].Name != "p1" {
						t.PartitionBy.List = partitionlist
					}
				}

				isAlreadyHashPartition := false

				if n.tableDesc.IsHashPartition {
					for _, col := range n.tableDesc.Columns {
						if col.Name == "hashnum" && col.IsHidden() {
							isAlreadyHashPartition = true
						}
					}
				}
				var idx *sqlbase.IndexDescriptor
				var col *sqlbase.ColumnDescriptor
				var expr tree.TypedExpr

				// old primary key
				field := string(t.PartitionBy.Fields[0])
				// record old primary key, used to calculate hash value
				n.tableDesc.HashField = field
				// check whether the field is primary key or not
				isPrimary := false
				for _, val := range n.tableDesc.PrimaryIndex.ColumnNames {
					if field == val {
						isPrimary = true
						break
					}
				}
				if !isPrimary {
					return errors.New("can only use primary key as HASH field")
				}

				n.tableDesc.IsHashPartition = true
				// record hashparts number in tableDesc
				n.tableDesc.HashParts = int32(len(t.PartitionBy.List))
				// construct hashnum's ColumnTableDef, this is used to construct colDesc and idxDesc
				d := &tree.ColumnTableDef{
					Name: "hashnum",
					Type: &coltypes.TInt{Width: 64},
					Nullable: struct {
						Nullability    tree.Nullability
						ConstraintName tree.Name
					}{Nullability: 2, ConstraintName: ""},
					PrimaryKey:           false,
					Unique:               false,
					UniqueConstraintName: "",
					DefaultExpr: struct {
						Expr                     tree.Expr
						ConstraintName           tree.Name
						OnUpdateCurrentTimeStamp bool
					}{Expr: tree.NewDInt(tree.DInt(2)), ConstraintName: "", OnUpdateCurrentTimeStamp: false}, // this is used to go through expr check, and then be replaced in backfill stage.
					CheckExprs: nil,
					References: struct {
						Table          *tree.TableName
						Col            tree.Name
						ConstraintName tree.Name
						Actions        tree.ReferenceActions
						Match          tree.CompositeKeyMatchMethod
					}{Table: nil, Col: "", ConstraintName: "", Actions: tree.ReferenceActions{Delete: tree.NoAction, Update: tree.NoAction}, Match: tree.MatchSimple},
					Computed: struct {
						Computed bool
						Expr     tree.Expr
					}{Computed: false, Expr: nil},
					Family: struct {
						Name        tree.Name
						Create      bool
						IfNotExists bool
					}{Name: "", Create: false, IfNotExists: false},
					IsHash: true,
				}

				newDef, seqDbDesc, seqName, seqOpts, err := params.p.processSerialInColumnDef(params.ctx, d, tn)
				if err != nil {
					return err
				}
				if seqName != nil {
					if err := doCreateSequence(params, n.n.String(), seqDbDesc, seqName, seqOpts); err != nil {
						return err
					}
				}
				d = newDef
				// get colDesc and idxDesc
				col, idx, expr, err = sqlbase.MakeHashColumnDefDescs(d, &params.p.semaCtx)
				if err != nil {
					return err
				}
				// If the new column has a DEFAULT expression that uses a sequence, add references between
				// its descriptor and this column descriptor.
				if d.HasDefaultExpr() {
					changedSeqDescs, err := maybeAddSequenceDependencies(params.ctx, params.p, n.tableDesc, col, expr)
					if err != nil {
						return err
					}
					for _, changedSeqDesc := range changedSeqDescs {
						if err := params.p.writeSchemaChange(params.ctx, changedSeqDesc, sqlbase.InvalidMutationID); err != nil {
							return err
						}
					}
				}

				if !isAlreadyHashPartition {
					// We're checking to see if a user is trying add a non-nullable column without a default to a
					// non empty table by scanning the primary index span with a limit of 1 to see if any key exists.
					if !col.Nullable && (col.DefaultExpr == nil && !col.IsComputed()) {
						kvs, err := params.p.txn.Scan(params.ctx, n.tableDesc.PrimaryIndexSpan().Key, n.tableDesc.PrimaryIndexSpan().EndKey, 1)
						if err != nil {
							return err
						}
						if len(kvs) > 0 {
							return sqlbase.NewNonNullViolationError(col.Name)
						}
					}

					// add colDesc to tableDesc's mutations, create a job.
					n.tableDesc.AddColumnMutation(*col, sqlbase.DescriptorMutation_ADD)
					if err := n.tableDesc.AllocateIDs(); err != nil {
						return err
					}
					mutationID := sqlbase.InvalidMutationID
					mutationID, err = params.p.createOrUpdateSchemaChangeJob(params.ctx, n.tableDesc,
						tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames))
					if err != nil {
						return err
					}

					if err := params.p.writeSchemaChange(params.ctx, n.tableDesc, mutationID); err != nil {
						return err
					}

					// amend partition field to 'hashnum'
					t.PartitionBy.Fields = tree.NameList{"hashnum"}
					// use idxDesc to create partitioningDesc
					partitioning, err = CreatePartitioning(
						params.ctx, params.p.ExecCfg().Settings,
						params.EvalContext(),
						n.tableDesc, idx, t.PartitionBy, params.StatusServer())
					if err != nil {
						return err
					}
					// use partitioningDesc above to assign idxDesc's partitioningDesc
					idx.Partitioning = partitioning
					// create job.
					if err := n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD); err != nil {
						return err
					}
					// assign field using old pk
					t.PartitionBy.Fields[0] = tree.Name(field)
				} else {
					var oldidx sqlbase.IndexDescriptor
					for _, val := range n.tableDesc.Indexes {
						if val.Name == "hashpartitionidx" && val.ColumnNames[0] == "hashnum" {
							oldidx = val
							break
						}
					}
					if oldidx.Name != "" {
						// amend partition field to 'hashnum'
						t.PartitionBy.Fields = tree.NameList{"hashnum"}
						// use idxDesc to create partitioningDesc
						partitioning, err := CreatePartitioning(
							params.ctx, params.p.ExecCfg().Settings,
							params.EvalContext(),
							n.tableDesc, &oldidx, t.PartitionBy, params.StatusServer())
						if err != nil {
							return err
						}
						// use partitioningDesc above to assign idxDesc's partitioningDesc
						oldidx.Partitioning = partitioning
						// assign field using old pk
						t.PartitionBy.Fields[0] = tree.Name(field)
					} else {
						t.PartitionBy.Fields = tree.NameList{"hashnum"}
						// use idxDesc to create partitioningDesc
						partitioning, err := CreatePartitioning(
							params.ctx, params.p.ExecCfg().Settings,
							params.EvalContext(),
							n.tableDesc, idx, t.PartitionBy, params.StatusServer())
						if err != nil {
							return err
						}
						// use partitioningDesc above to assign idxDesc's partitioningDesc
						idx.Partitioning = partitioning
						// create job.
						if err := n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD); err != nil {
							return err
						}
						// assign field using old pk
						t.PartitionBy.Fields[0] = tree.Name(field)
					}
				}

				if d.HasColumnFamily() {
					err := n.tableDesc.AddColumnToFamilyMaybeCreate(
						col.Name, string(d.Family.Name), d.Family.Create,
						d.Family.IfNotExists)
					if err != nil {
						return err
					}
				}

				if d.IsComputed() {
					if err := validateComputedColumn(n.tableDesc, d, &params.p.semaCtx); err != nil {
						return err
					}
				}

				descriptorChangedGlobal = true
				//by lz
				//read locationNum from tableDesc directly instead of from index
				oldLocationNums = n.tableDesc.LocationNums
				newLocationNums = n.tableDesc.LocationNums -
					n.tableDesc.PrimaryIndex.Partitioning.LocationNums + partitioning.LocationNums
				if err := updateIndexLocationNums(n.tableDesc.TableDesc(),
					&n.tableDesc.PrimaryIndex, newLocationNums); err != nil {
					return err
				}
				if oldLocationNums > 0 || newLocationNums > 0 {
					useLocateIn = true
				}

			} else {
				partitioning, err := CreatePartitioning(
					params.ctx, params.p.ExecCfg().Settings,
					params.EvalContext(),
					n.tableDesc, &n.tableDesc.PrimaryIndex, t.PartitionBy, params.StatusServer())
				if err != nil {
					return err
				}
				if n.tableDesc.IsHashPartition {
					for _, idx := range n.tableDesc.Indexes {
						if idx.Name == "hashpartitionidx" && n.tableDesc.IsHashPartition {
							if err := params.p.dropIndexByName(
								params.ctx, tn, tree.UnrestrictedName(idx.Name), n.tableDesc, false,
								0, ignoreIdxConstraint,
								tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType),
							); err != nil {
								return err
							}
						}
					}
				}
				descriptorChanged := !proto.Equal(
					&n.tableDesc.PrimaryIndex.Partitioning,
					&partitioning,
				)
				if descriptorChanged {
					descriptorChangedGlobal = true
					//by lz
					//read locationNum from tableDesc directly instead of from index
					oldLocationNums = n.tableDesc.LocationNums
					newLocationNums = n.tableDesc.LocationNums -
						n.tableDesc.PrimaryIndex.Partitioning.LocationNums + partitioning.LocationNums
					if err := updateIndexLocationNums(n.tableDesc.TableDesc(),
						&n.tableDesc.PrimaryIndex, newLocationNums); err != nil {
						return err
					}
					if oldLocationNums > 0 || newLocationNums > 0 {
						useLocateIn = true
					}
				}
				n.tableDesc.PrimaryIndex.Partitioning = partitioning
			}
			// 当表的新分区确定以后,依据表的分区对本地分区索引的分区进行调整
			// 如果表变成了不分区的表, 需要删除所有的本地分区索引, 否则需要修改相应的本地索引分区。
			indexS := n.tableDesc.Indexes
			for i := range indexS {
				if indexS[i].IsLocal {
					descriptorChanged := false
					if n.tableDesc.PrimaryIndex.Partitioning.NumColumns == 0 {
						// drop the local-index
						err := params.p.dropLocalIndex(params.ctx, &indexS[i], n.tableDesc, 0, false)
						if err != nil {
							return pgerror.NewError(pgcode.LocalIndexDrop, err.Error())
						}
					} else {
						// alter index partition
						partitioning, err := alterLocalIndexPartitioning(n.tableDesc, n.tableDesc.PrimaryIndex, indexS[i])
						if err != nil {
							return err
						}
						descriptorChanged = !proto.Equal(
							&indexS[i].Partitioning,
							&partitioning,
						)
						indexS[i].Partitioning = partitioning
						if descriptorChanged {
							if err := updateIndexLocationNums(n.tableDesc.TableDesc(),
								&indexS[i], newLocationNums); err != nil {
								return err
							}
						}
					}
				}
			}
		case *tree.AlterTableSetAudit:
			var err error
			descriptorChanged := false
			descriptorChanged, err = params.p.setAuditMode(params.ctx, n.tableDesc.TableDesc(), t.Mode)
			if err != nil {
				return err
			}
			if descriptorChanged {
				descriptorChangedGlobal = true
			}

		case *tree.AlterTableInjectStats:
			sd, ok := n.statsData[i]
			if !ok {
				return pgerror.NewAssertionErrorf("missing stats data")
			}
			if err := injectTableStats(params, n.tableDesc.TableDesc(), sd); err != nil {
				return err
			}

		case *tree.AlterTableRenameColumn:
			descriptorChanged, err := params.p.renameColumn(params.ctx, n.tableDesc, &t.Column, &t.NewName)
			if err != nil {
				return err
			}
			if descriptorChanged {
				descriptorChangedGlobal = true
			}

		case *tree.AlterTableRenameConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			details, ok := info[string(t.Constraint)]
			if !ok {
				return fmt.Errorf("constraint %q does not exist", tree.ErrString(&t.Constraint))
			}
			if t.Constraint == t.NewName {
				// Nothing to do.
				break
			}

			if _, ok := info[string(t.NewName)]; ok {
				return errors.Errorf("duplicate constraint name: %q", tree.ErrString(&t.NewName))
			}

			depViewRenameError := func(objType string, refTableID sqlbase.ID) error {
				return params.p.dependentViewRenameError(params.ctx,
					objType, tree.ErrString(&t.NewName), n.tableDesc.ParentID, refTableID)
			}

			constraint, _, err := n.tableDesc.FindConstraintByName(params.ctx, params.p.txn, string(t.Constraint))
			if err != nil {
				return err
			}
			if err := params.p.updateCheckConstraintComment(params.ctx, n.tableDesc.ID, *constraint, string(t.NewName)); err != nil {
				return err
			}

			if err := n.tableDesc.RenameConstraint(
				details, string(t.Constraint), string(t.NewName), depViewRenameError); err != nil {
				return err
			}

			if details.Kind == sqlbase.ConstraintTypeCheck {

				var alterTableMap = make(map[sqlbase.ID]*RenameColCmds)
				for _, id := range n.tableDesc.InheritsBy {
					inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
					if err != nil {
						return err
					}
					if inhTableDesc == nil {
						return fmt.Errorf(`cannot find inherit table id: %d`, id)
					}
					if _, ok := alterTableMap[id]; ok {
						alterTableMap[id].cmdCount++
					} else {
						alterTableMap[id] = &RenameColCmds{desc: inhTableDesc, cmdCount: 1}
					}
					err = params.p.addInhCmds(params.ctx, inhTableDesc, alterTableMap)
					if err != nil {
						return err
					}
				}

				for id := range alterTableMap {
					err = params.p.renameInheritCheckConstraint(params.ctx, alterTableMap[id].desc, string(t.Constraint), string(t.NewName), alterTableMap[id].cmdCount)
					if err != nil {
						return err
					}
				}
			}

			descriptorChangedGlobal = true

		case *tree.AlterTableLocateIn:
			spaceName := t.LocateSpaceName.ToValue()
			if err := CheckLocateSpaceNameExistICL(params.ctx,
				spaceName, *params.StatusServer()); err != nil {
				return err
			}

			oldLocationNums = n.tableDesc.LocationNums
			if descriptorChanged, changeValue := changeLocationNums(spaceName,
				n.tableDesc.LocateSpaceName); descriptorChanged {
				newLocationNums = n.tableDesc.LocationNums + changeValue
				if err := updateTableLocationNums(n.tableDesc.TableDesc(),
					newLocationNums); err != nil {
					return err
				}
				n.tableDesc.LocateSpaceName = spaceName
				removeSpaceNull(n.tableDesc.LocateSpaceName)
				if oldLocationNums > 0 || newLocationNums > 0 {
					useLocateIn = true
				}
				descriptorChangedGlobal = true
			}

		case *tree.AlterParitionLocateIn:
			spaceName := t.LocateSpaceName.ToValue()
			partitionName := string(t.ParitionName)
			if err := CheckLocateSpaceNameExistICL(params.ctx,
				spaceName, *params.StatusServer()); err != nil {
				return err
			}
			if partitionDesc, indexDesc, err :=
				n.tableDesc.FindNonDropPartitionByName(partitionName); err != nil {
				return err
			} else if partitionDesc == nil {
				return errors.New("Partition Name not exist")
			} else {
				oldLocationNums = partitionDesc.LocationNums
				newLocationNums = partitionDesc.LocationNums
				var changeValue int32
				descriptorChanged := false
				for i := 0; i < len(partitionDesc.List); i++ {
					if partitionDesc.List[i].Name == partitionName {
						if descriptorChanged, changeValue = changeLocationNums(spaceName,
							partitionDesc.List[i].LocateSpaceName); descriptorChanged {
							newLocationNums += changeValue
							partitionDesc.LocationNums += changeValue
							partitionDesc.List[i].LocateSpaceName = spaceName
							// maybe can delete in alter table
							removeSpaceNull(partitionDesc.List[i].LocateSpaceName)
						}
					}
				}
				for i := 0; i < len(partitionDesc.Range); i++ {
					if partitionDesc.Range[i].Name == partitionName {
						if descriptorChanged, changeValue = changeLocationNums(spaceName,
							partitionDesc.Range[i].LocateSpaceName); descriptorChanged {
							newLocationNums += changeValue
							partitionDesc.LocationNums += changeValue
							partitionDesc.Range[i].LocateSpaceName = spaceName
							removeSpaceNull(partitionDesc.Range[i].LocateSpaceName)
						}
					}
				}
				if descriptorChanged {
					descriptorChangedGlobal = true
					if err := updateIndexLocationNums(n.tableDesc.TableDesc(), indexDesc,
						newLocationNums); err != nil {
						return err
					}
				}
				if oldLocationNums > 0 || newLocationNums > 0 {
					useLocateIn = true
				}
			}

		default:
			return pgerror.NewAssertionErrorf("unsupported alter command: %T", cmd)
		}

		// Allocate IDs now, so new IDs are available to subsequent commands
		if err := n.tableDesc.AllocateIDs(); err != nil {
			return err
		}
	}
	// Were some changes made?
	//
	// This is only really needed for the unittests that add dummy mutations
	// before calling ALTER TABLE commands. We do not want to apply those
	// dummy mutations. Most tests trigger errors above
	// this line, but tests that run redundant operations like dropping
	// a column when it's already dropped will hit this condition and exit.
	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	if !addedMutations && !descriptorChangedGlobal {
		return nil
	}

	if descriptorChangedGlobal {
		if useLocateIn {
			if err := params.p.LocationMapChange(params.ctx, n.tableDesc,
				params.extendedEvalCtx.Tables.databaseCache.systemConfig); err != nil {
				return err
			}
		}
	}
	mutationID := sqlbase.InvalidMutationID
	if addedMutations {
		var err error
		mutationID, err = params.p.createOrUpdateSchemaChangeJob(params.ctx, n.tableDesc,
			tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType))
		if err != nil {
			return err
		}
	}

	if err := params.p.writeSchemaChange(params.ctx, n.tableDesc, mutationID); err != nil {
		return err
	}

	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogAlterTable),
		TargetInfo: &server.TargetInfo{
			Desc: struct {
				TableName string
			}{
				n.n.Table.FQString(),
			},
		},
		Info: &infos.AlterTableInfo{
			TableName:  n.n.Table.FQString(),
			Statement:  n.n.String(),
			User:       params.SessionData().User,
			MutationID: uint32(mutationID),
		},
	}
	return nil
}

func (p *planner) setAuditMode(
	ctx context.Context, desc *sqlbase.TableDescriptor, auditMode tree.AuditMode,
) (bool, error) {
	// An auditing config change is itself auditable!
	// We record the event even if the permission check below fails:
	// auditing wants to know who tried to change the settings.
	p.curPlan.auditEvents = append(p.curPlan.auditEvents,
		auditEvent{desc: desc, writing: true})

	// We require root for now. Later maybe use a different permission?
	if err := p.RequireAdminRole(ctx, "change auditing settings on a table"); err != nil {
		return false, err
	}

	return desc.SetAuditMode(auditMode)
}

func (n *alterTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableNode) Close(context.Context)        {}

// applyColumnMutation applies the mutation specified in `mut` to the given
// columnDescriptor, and saves the containing table descriptor. If the column's
// dependencies on sequences change, it updates them as well.
func applyColumnMutation(
	tableDesc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	mut tree.ColumnMutationCmd,
	params runParams,
) error {
	switch t := mut.(type) {
	case *tree.AlterTableAlterColumnType:
		// Convert the parsed type into one of the basic datum types.
		datum := coltypes.CastTargetToDatumType(t.ToType)

		// Special handling for STRING COLLATE xy to verify that we recognize the language.
		if t.Collation != "" {
			if types.IsStringType(datum) {
				if _, err := language.Parse(t.Collation); err != nil {
					return pgerror.NewErrorf(pgcode.Syntax, `invalid locale %s`, t.Collation)
				}
				datum = types.TCollatedString{Locale: t.Collation}
			} else {
				return pgerror.NewError(pgcode.Syntax, "COLLATE can only be used with string types")
			}
		}

		// First pass at converting the datum type to the SQL column type.
		nextType, err := sqlbase.DatumTypeToColumnType(datum)
		if err != nil {
			return err
		}

		// Finish populating width, precision, etc. from parsed data.
		nextType, err = sqlbase.PopulateTypeAttrs(nextType, t.ToType)
		if err != nil {
			return err
		}

		// No-op if the types are Equal.  We don't use Equivalent here
		// because the user may want to change the visible type of the
		// column without changing the underlying semantic type.
		if col.Type.EqualVisible(nextType) {
			return nil
		}

		kind, err := schemachange.ClassifyConversion(&col.Type, &nextType)
		if err != nil {
			return err
		}
		if nextType.SemanticType == sqlbase.ColumnType_SET && kind == schemachange.ColumnConversionTrivial {
			params.p.noticeSender.BufferNotice(
				pgnotice.Newf(`The old values of column "%s" will not change`, col.Name),
			)
		}
		if nextType.VisibleTypeName == "ENUM()" && kind == schemachange.ColumnConversionTrivial {
			params.p.noticeSender.BufferNotice(
				pgnotice.Newf(`The old values of column "%s" may not be compatible with new types`, col.Name),
			)
		}

		switch kind {
		case schemachange.ColumnConversionDangerous, schemachange.ColumnConversionImpossible:
			// We're not going to make it impossible for the user to perform
			// this conversion, but we do want them to explicit about
			// what they're going for.
			return pgerror.NewErrorf(pgcode.CannotCoerce,
				"the requested type conversion (%s -> %s) requires an explicit USING expression",
				col.Type.SQLString(), nextType.SQLString())
		case schemachange.ColumnConversionTrivial:
			col.Type = nextType
		default:
			return pgerror.UnimplementedWithIssueDetailError(9851,
				fmt.Sprintf("%s->%s", col.Type.SQLString(), nextType.SQLString()),
				"type conversion not yet implemented")
		}

	case *tree.AlterTableSetDefault:
		if len(col.UsesSequenceIds) > 0 {
			if err := removeSequenceDependencies(tableDesc, col, params, true); err != nil {
				return err
			}
		}
		if t.Default == nil {
			col.DefaultExpr = nil

			var alterTableMap = make(map[sqlbase.ID]struct{})
			for _, id := range tableDesc.InheritsBy {
				alterTableMap[id] = struct{}{}
			}

			for _, id := range tableDesc.InheritsBy {
				inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
				if err != nil {
					return err
				}
				if inhTableDesc == nil {
					return fmt.Errorf(`cannot find inherit table id: %d`, id)
				}
				err = params.p.dropInheritDefaultExpr(params.ctx, inhTableDesc, col.Name, alterTableMap)
				if err != nil {
					return err
				}
			}
		} else {
			colDatumType := col.Type.ToDatumType()
			expr, err := sqlbase.SanitizeVarFreeExpr(
				t.Default, colDatumType, "DEFAULT", &params.p.semaCtx, true /* allowImpure */, false,
			)
			if err != nil {
				return err
			}
			//s := tree.Serialize(t.Default)
			s := tree.SerializeWithoutSuffix(t.Default)
			col.DefaultExpr = &s

			// Add references to the sequence descriptors this column is now using.
			changedSeqDescs, err := maybeAddSequenceDependencies(params.ctx, params.p, tableDesc, col, expr)
			if err != nil {
				return err
			}
			for _, changedSeqDesc := range changedSeqDescs {
				if err := params.p.writeSchemaChange(params.ctx, changedSeqDesc, sqlbase.InvalidMutationID); err != nil {
					return err
				}
			}

			var alterTableMap = make(map[sqlbase.ID]struct{})
			for _, id := range tableDesc.InheritsBy {
				alterTableMap[id] = struct{}{}
			}
			for _, id := range tableDesc.InheritsBy {
				inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
				if err != nil {
					return err
				}
				if inhTableDesc == nil {
					return fmt.Errorf(`cannot find inherit table id: %d`, id)
				}
				err = params.p.setInheritDefaultExpr(params.ctx, inhTableDesc, col.Name, s, expr, alterTableMap)
				if err != nil {
					return err
				}
			}
		}

	case *tree.AlterTableDropNotNull:
		col.Nullable = true

		var alterTableMap = make(map[sqlbase.ID]struct{})
		for _, id := range tableDesc.InheritsBy {
			alterTableMap[id] = struct{}{}
		}

		for _, id := range tableDesc.InheritsBy {
			inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
			if err != nil {
				return err
			}
			if inhTableDesc == nil {
				return fmt.Errorf(`cannot find inherit table id: %d`, id)
			}
			err = params.p.inheritDropNotNull(params.ctx, inhTableDesc, col.Name, alterTableMap)
			if err != nil {
				return err
			}
		}

	case *tree.AlterTableSetNotNull:
		var alterTableMap = make(map[sqlbase.ID]struct{})
		for _, id := range tableDesc.InheritsBy {
			alterTableMap[id] = struct{}{}
		}
		if !col.Nullable {
			for _, id := range tableDesc.InheritsBy {
				inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
				if err != nil {
					return err
				}
				if inhTableDesc == nil {
					return fmt.Errorf(`cannot find inherit table id: %d`, id)
				}
				err = params.p.InheritSetNotNull(params, inhTableDesc, col.Name, alterTableMap)
				if err != nil {
					return err
				}
			}
			break
		}

		err := validateNotNull(params.ctx, params.p.txn, params.EvalContext().InternalExecutor, tableDesc, col)
		if err != nil {
			return err
		}
		col.Nullable = false

		for _, id := range tableDesc.InheritsBy {
			inhTableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
			if err != nil {
				return err
			}
			if inhTableDesc == nil {
				return fmt.Errorf(`cannot find inherit table id: %d`, id)
			}
			err = params.p.InheritSetNotNull(params, inhTableDesc, col.Name, alterTableMap)
			if err != nil {
				return err
			}
		}

	case *tree.AlterTableDropStored:
		if !col.IsComputed() {
			return pgerror.NewErrorf(pgcode.InvalidColumnDefinition,
				"column %q is not a computed column", col.Name)
		}
		col.ComputeExpr = nil
	}
	return nil
}

func labeledRowValues(cols []sqlbase.ColumnDescriptor, values tree.Datums) string {
	var s bytes.Buffer
	for i := range cols {
		if i != 0 {
			s.WriteString(`, `)
		}
		s.WriteString(cols[i].Name)
		s.WriteString(`=`)
		s.WriteString(values[i].String())
	}
	return s.String()
}

// injectTableStats implements the INJECT STATISTICS command, which deletes any
// existing statistics on the table and replaces them with the statistics in the
// given json object (in the same format as the result of SHOW STATISTICS USING
// JSON). This is useful for reproducing planning issues without importing the
// data.
func injectTableStats(
	params runParams, desc *sqlbase.TableDescriptor, statsExpr tree.TypedExpr,
) error {
	val, err := statsExpr.Eval(params.EvalContext())
	if err != nil {
		return err
	}
	if val == tree.DNull {
		return fmt.Errorf("statistics cannot be NULL")
	}
	jsonStr := val.(*tree.DJSON).JSON.String()
	var jsonStats []stats.JSONStatistic
	if err := gojson.Unmarshal([]byte(jsonStr), &jsonStats); err != nil {
		return err
	}

	// First, delete all statistics for the table.
	if _ /* rows */, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"delete-stats",
		params.EvalContext().Txn,
		`DELETE FROM system.table_statistics WHERE "tableID" = $1`, desc.ID,
	); err != nil {
		return errors.Wrapf(err, "failed to delete old stats")
	}

	// Insert each statistic.
	for i := range jsonStats {
		s := &jsonStats[i]
		h, err := s.GetHistogram(params.EvalContext())
		if err != nil {
			return err
		}
		// histogram will be passed to the INSERT statement; we want it to be a
		// nil interface{} if we don't generate a histogram.
		var histogram interface{}
		if h != nil {
			histogram, err = protoutil.Marshal(h)
			if err != nil {
				return err
			}
		}

		columnIDs := tree.NewDArray(types.Int)
		for _, colName := range s.Columns {
			colDesc, _, err := desc.FindColumnByName(tree.Name(colName))
			if err != nil {
				return err
			}
			if err := columnIDs.Append(tree.NewDInt(tree.DInt(colDesc.ID))); err != nil {
				return err
			}
		}
		var name interface{}
		if s.Name != "" {
			name = s.Name
		}
		if _ /* rows */, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"insert-stats",
			params.EvalContext().Txn,
			`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			desc.ID,
			name,
			columnIDs,
			s.CreatedAt,
			s.RowCount,
			s.DistinctCount,
			s.NullCount,
			histogram,
		); err != nil {
			return errors.Wrapf(err, "failed to insert stats")
		}
	}

	// Invalidate the local cache synchronously; this guarantees that the next
	// statement in the same session won't use a stale cache (whereas the gossip
	// update is handled asynchronously).
	params.extendedEvalCtx.ExecCfg.TableStatsCache.InvalidateTableStats(params.ctx, desc.ID)

	return stats.GossipTableStatAdded(params.extendedEvalCtx.ExecCfg.Gossip, desc.ID)
}

func (p *planner) removeColumnComment(
	ctx context.Context, tableID sqlbase.ID, columnID sqlbase.ColumnID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx,
		"delete-column-comment",
		p.txn,
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.ColumnCommentType,
		tableID,
		columnID)

	return err
}

// removeSpaceNull removes invalid spaces and leases
func removeSpaceNull(locationSpace *roachpb.LocationValue) {
	if locationSpace == nil {
		return
	}
	for i, space := range locationSpace.Spaces {
		if space == "" && len(locationSpace.Spaces) == 1 {
			locationSpace.Spaces = nil
		} else if space == "" {
			locationSpace.Spaces = append(locationSpace.Spaces[:i], locationSpace.Spaces[i+1:]...)
		}
	}
	for i, lease := range locationSpace.Leases {
		if lease == "" && len(locationSpace.Leases) == 1 {
			locationSpace.Leases = nil
		} else if lease == "" {
			locationSpace.Leases = append(locationSpace.Leases[:i], locationSpace.Leases[i+1:]...)
		}
	}
}

func (p *planner) addInheritCol(
	ctx context.Context,
	params runParams,
	n *tree.AlterTable,
	tableDesc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
) error {
	_, dropped, err := tableDesc.FindColumnByName(tree.Name(col.Name))
	if err == nil {
		if dropped {
			return fmt.Errorf("column %q being dropped, try again later", col.Name)
		}

		for i, existCol := range tableDesc.Columns {
			if existCol.Name == col.Name {
				if !existCol.Type.Equal(col.Type) {
					return fmt.Errorf(`child table "%s" has different type for column "%s"`, tableDesc.Name, col.Name)
				}
				if col.Nullable == false {
					//add check set not null
					err := validateNotNull(ctx, p.txn, params.EvalContext().InternalExecutor, tableDesc, col)
					if err != nil {
						return err
					}
					tableDesc.Columns[i].Nullable = false
				}
				if col.HasDefault() {
					tableDesc.Columns[i].DefaultExpr = col.DefaultExpr
				}
				tableDesc.Columns[i].InhCount++
			}
		}

		for _, m := range tableDesc.Mutations {
			if c := m.GetColumn(); c != nil {
				if c.Name == col.Name {
					if !c.Type.Equal(col.Type) {
						return fmt.Errorf(`child table "%s" has different type for column "%s"`, tableDesc.Name, col.Name)
					}
					if col.Nullable == false {
						//add check set not null
						err := validateNotNull(ctx, p.txn, params.EvalContext().InternalExecutor, tableDesc, col)
						if err != nil {
							return err
						}
						c.Nullable = false
					}
					if col.HasDefault() {
						c.DefaultExpr = col.DefaultExpr
					}
					c.InhCount++
				}
			}
		}

		err := p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
		if err != nil {
			return err
		}
	} else {
		var inhcol = *col
		inhcol.IsInherits = true
		inhcol.InhCount = 1
		inhcol.ID = tableDesc.NextColumnID
		tableDesc.NextColumnID++
		tableDesc.AddColumnMutation(inhcol, sqlbase.DescriptorMutation_ADD)
		err := tableDesc.AllocateIDs()
		if err != nil {
			return err
		}
		mutationID := sqlbase.InvalidMutationID
		mutationID, err = p.createOrUpdateSchemaChangeJob(ctx, tableDesc,
			tree.AsStringWithFlags(n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType))
		if err != nil {
			return err
		}
		//tableDesc.AddColumnMutation(inhcol, sqlbase.DescriptorMutation_ADD)
		err = p.writeSchemaChange(ctx, tableDesc, mutationID)
		if err != nil {
			return err
		}
	}

	for _, id := range tableDesc.InheritsBy {
		inherTable, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inherTable == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		err = p.addInheritCol(ctx, params, n, inherTable, col)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) renameInheritCheckConstraint(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	oldName, newName string,
	cmdCount int,
) error {
	info, err := tableDesc.GetConstraintInfo(ctx, nil)
	if err != nil {
		return err
	}

	details, ok := info[oldName]
	if !ok {
		return fmt.Errorf("constraint %q does not exist", oldName)
	}
	if _, ok := info[newName]; ok {
		return fmt.Errorf("duplicate constraint name: %s in table: %s", newName, tableDesc.Name)
	}
	var count = details.CheckConstraint.InhCount
	if !details.CheckConstraint.IsInherits {
		count--
	}
	if count > uint32(cmdCount) {
		return fmt.Errorf("inherited check constraint can not be rename")
	}

	if details.CheckConstraint.Validity == sqlbase.ConstraintValidity_Validating {
		return pgerror.Unimplemented("rename-constraint-check-mutation",
			"constraint %q in the middle of being added, try again later",
			tree.ErrNameStringP(&details.CheckConstraint.Name))
	}
	details.CheckConstraint.Name = newName
	err = p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
	if err != nil {
		return err
	}

	return nil
}

func (p *planner) dropInheritDefaultExpr(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	name string,
	tableMap map[sqlbase.ID]struct{},
) error {
	col, dropped, err := desc.FindColumnByName(tree.Name(name))
	if err == nil {
		if dropped {
			return fmt.Errorf("column %q being dropped, try again later", name)
		}
		col.DefaultExpr = nil
		desc.UpdateColumnDescriptor(col)
		err := p.writeSchemaChange(ctx, desc, sqlbase.InvalidMutationID)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("cannot find inherit column '%s'", name)
	}

	var inheritsByTables = make([]sqlbase.ID, 0, len(desc.InheritsBy))
	for _, id := range desc.InheritsBy {
		if _, ok := tableMap[id]; !ok {
			inheritsByTables = append(inheritsByTables, id)
			tableMap[id] = struct{}{}
		}
	}

	for _, id := range inheritsByTables {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inhTableDesc == nil {
			return fmt.Errorf(`cannot find inheri table id: %d`, id)
		}
		err = p.dropInheritDefaultExpr(ctx, inhTableDesc, col.Name, tableMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) setInheritDefaultExpr(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	name string,
	s string,
	expr tree.TypedExpr,
	tableMap map[sqlbase.ID]struct{},
) error {
	col, dropped, err := desc.FindColumnByName(tree.Name(name))
	if err == nil {
		if dropped {
			return fmt.Errorf("column %q being dropped, try again later", name)
		}
		col.DefaultExpr = &s
		desc.UpdateColumnDescriptor(col)
		changedSeqDescs, err := maybeAddSequenceDependencies(ctx, p, desc, &col, expr)
		if err != nil {
			return err
		}
		for _, changedSeqDesc := range changedSeqDescs {
			if err := p.writeSchemaChange(ctx, changedSeqDesc, sqlbase.InvalidMutationID); err != nil {
				return err
			}
		}
		err = p.writeSchemaChange(ctx, desc, sqlbase.InvalidMutationID)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("cannot find inherit column '%s'", name)
	}

	var inheritsByTables = make([]sqlbase.ID, 0, len(desc.InheritsBy))
	for _, id := range desc.InheritsBy {
		if _, ok := tableMap[id]; !ok {
			inheritsByTables = append(inheritsByTables, id)
			tableMap[id] = struct{}{}
		}
	}

	for _, id := range inheritsByTables {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inhTableDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		err = p.setInheritDefaultExpr(ctx, inhTableDesc, col.Name, s, expr, tableMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) inheritDropNotNull(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	name string,
	tableMap map[sqlbase.ID]struct{},
) error {
	col, dropped, err := desc.FindColumnByName(tree.Name(name))
	if err == nil {
		if dropped {
			return fmt.Errorf("column %q being dropped, try again later", name)
		}
		col.Nullable = true
		desc.UpdateColumnDescriptor(col)
		err = p.writeSchemaChange(ctx, desc, sqlbase.InvalidMutationID)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("cannot find inherit column '%s'", name)
	}

	var inheritsByTables = make([]sqlbase.ID, 0, len(desc.InheritsBy))
	for _, id := range desc.InheritsBy {
		if _, ok := tableMap[id]; !ok {
			inheritsByTables = append(inheritsByTables, id)
			tableMap[id] = struct{}{}
		}
	}

	for _, id := range inheritsByTables {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inhTableDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		err = p.inheritDropNotNull(ctx, inhTableDesc, name, tableMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) InheritSetNotNull(
	params runParams,
	desc *sqlbase.MutableTableDescriptor,
	name string,
	tableMap map[sqlbase.ID]struct{},
) error {

	var inheritsByTables = make([]sqlbase.ID, 0, len(desc.InheritsBy))
	for _, id := range desc.InheritsBy {
		if _, ok := tableMap[id]; !ok {
			inheritsByTables = append(inheritsByTables, id)
			tableMap[id] = struct{}{}
		}
	}

	col, dropped, err := desc.FindColumnByName(tree.Name(name))
	if err == nil {
		if dropped {
			return fmt.Errorf("column %q being dropped, try again later", name)
		}
		if col.Nullable {
			err := validateNotNull(params.ctx, params.p.txn, params.EvalContext().InternalExecutor, desc, &col)
			if err != nil {
				return err
			}
			col.Nullable = false
			desc.UpdateColumnDescriptor(col)
			err = p.writeSchemaChange(params.ctx, desc, sqlbase.InvalidMutationID)
			if err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("cannot find inherit column '%s'", name)
	}

	for _, id := range inheritsByTables {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(params.ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inhTableDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		err = p.InheritSetNotNull(params, inhTableDesc, name, tableMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) addInheritCheckConstraint(
	ctx context.Context,
	params runParams,
	n *tree.AlterTable,
	desc *sqlbase.MutableTableDescriptor,
	d *tree.CheckConstraintTableDef,
	semaCtx *tree.SemaContext,
	ckname string,
) error {
	tableName, err := sqlbase.GetTableName(ctx, p.txn, desc.ParentID, desc.Name)
	if err != nil {
		return err
	}
	info, err := desc.GetConstraintInfo(ctx, nil)
	if err != nil {
		return err
	}

	expr, colIDsUsed, err := replaceVars(desc, d.Expr)
	if err != nil {
		return err
	}

	if _, err := sqlbase.SanitizeVarFreeExpr(
		expr, types.Bool, "CHECK", semaCtx, true /* allowImpure */, false,
	); err != nil {
		return err
	}

	colIDs := make([]sqlbase.ColumnID, 0, len(colIDsUsed))
	for colID := range colIDsUsed {
		colIDs = append(colIDs, colID)
	}
	sort.Sort(sqlbase.ColumnIDs(colIDs))

	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		*tableName, sqlbase.ResultColumnsFromColDescs(desc.TableDesc().AllNonDropColumns()),
	)
	sources := sqlbase.MultiSourceInfo{sourceInfo}

	expr, err = schemaexpr.DequalifyColumnRefs(ctx, sources, d.Expr)
	if err != nil {
		return err
	}

	if detail, ok := info[ckname]; ok {
		if detail.CheckConstraint.Expr == tree.Serialize(expr) {
			detail.CheckConstraint.InhCount++
		} else {
			return fmt.Errorf("duplicate check %s", ckname)
		}
		err = p.writeSchemaChange(ctx, desc, sqlbase.InvalidMutationID)
		if err != nil {
			return err
		}
	} else {
		ck := &sqlbase.TableDescriptor_CheckConstraint{
			Expr:       tree.Serialize(expr),
			Name:       ckname,
			ColumnIDs:  colIDs,
			Able:       d.Able,
			IsInherits: true,
			InhCount:   1,
		}

		ck.Validity = sqlbase.ConstraintValidity_Validating
		desc.AddCheckValidationMutation(ck)

		mutationID := sqlbase.InvalidMutationID
		mutationID, err = p.createOrUpdateSchemaChangeJob(ctx, desc,
			tree.AsStringWithFlags(n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType))
		if err != nil {
			return err
		}

		//if err = validateCheckExpr(
		//	ctx, ck.Expr, desc.TableDesc(), params.EvalContext().InternalExecutor, params.EvalContext().Txn,
		//); err != nil {
		//	return err
		//}
		//
		//ck.Validity = sqlbase.ConstraintValidity_Validated
		//desc.Checks = append(desc.Checks, ck)
		err = p.writeSchemaChange(ctx, desc, mutationID)
		if err != nil {
			return err
		}
	}

	for _, id := range desc.InheritsBy {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inhTableDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		err = p.addInheritCheckConstraint(ctx, params, n, inhTableDesc, d, semaCtx, ckname)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) ableInheritCheck(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	name string,
	able bool,
	tableMap map[sqlbase.ID]struct{},
) error {
	info, err := desc.GetConstraintInfo(ctx, nil)
	if err != nil {
		return err
	}
	details, ok := info[name]
	if !ok {
		return fmt.Errorf("inherit constraint %q does not exist", name)
	}

	if err := desc.AbleConstraint(
		name,
		details,
		able); err != nil {
		return err
	}

	err = p.writeSchemaChange(ctx, desc, sqlbase.InvalidMutationID)
	if err != nil {
		return err
	}

	var inheritsByTables = make([]sqlbase.ID, 0, len(desc.InheritsBy))
	for _, id := range desc.InheritsBy {
		if _, ok := tableMap[id]; !ok {
			inheritsByTables = append(inheritsByTables, id)
			tableMap[id] = struct{}{}
		}
	}

	for _, id := range inheritsByTables {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inhTableDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		err = p.ableInheritCheck(ctx, inhTableDesc, name, able, tableMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) dropInheritCheck(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, name string,
) error {
	info, err := desc.GetConstraintInfo(ctx, nil)
	if err != nil {
		return err
	}
	details, ok := info[name]
	if !ok {
		return fmt.Errorf("inherit constraint %q does not exist", name)
	}

	if details.CheckConstraint.Validity == sqlbase.ConstraintValidity_Validating {
		return pgerror.Unimplemented("rename-constraint-check-mutation",
			"constraint %q in the middle of being added, try again later",
			tree.ErrNameStringP(&details.CheckConstraint.Name))
	}
	if details.CheckConstraint.InhCount > 1 {
		details.CheckConstraint.InhCount--
	} else if details.CheckConstraint.InhCount == 1 && details.CheckConstraint.IsInherits {
		for i, c := range desc.Checks {
			if c.Name == name {
				desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
				break
			}
		}
	}

	err = p.writeSchemaChange(ctx, desc, sqlbase.InvalidMutationID)
	if err != nil {
		return err
	}

	for _, id := range desc.InheritsBy {
		inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return err
		}
		if inhTableDesc == nil {
			return fmt.Errorf(`cannot find inherit table id: %d`, id)
		}
		err = p.dropInheritCheck(ctx, inhTableDesc, name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) dropInheritColumn(
	ctx context.Context,
	params runParams,
	n *tree.AlterTable,
	desc *sqlbase.MutableTableDescriptor,
	name string,
	behavior tree.DropBehavior,
) error {
	var isDrop bool
	tn, err := sqlbase.GetTableName(ctx, p.txn, desc.ParentID, desc.Name)
	if err != nil {
		return err
	}
	col, dropped, err := desc.FindColumnByName(tree.Name(name))
	if err != nil {
		return fmt.Errorf("cannot find inherit column '%s'", name)
	}
	if dropped {

		for _, id := range desc.InheritsBy {
			inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
			if err != nil {
				return err
			}
			if inhTableDesc == nil {
				return fmt.Errorf(`cannot find inherit table id: %d`, id)
			}
			err = p.dropInheritColumn(ctx, params, n, inhTableDesc, name, behavior)
			if err != nil {
				return err
			}
		}
	} else {
		if col.InhCount > 1 {
			col.InhCount--
			desc.UpdateColumnDescriptor(col)
			isDrop = false
		} else if col.InhCount == 1 && !col.IsInherits {
			return fmt.Errorf("chu xian le cuo wu")
		} else if col.InhCount == 1 && col.IsInherits {
			isDrop = true
			if len(col.UsesSequenceIds) > 0 {
				if err := removeSequenceDependencies(desc, &col, params, true); err != nil {
					return err
				}
			}

			for _, ref := range desc.DependedOnBy {
				found := false
				for _, colID := range ref.ColumnIDs {
					if colID == col.ID {
						found = true
						break
					}
				}
				if !found {
					continue
				}
				err := params.p.canRemoveDependentViewGeneric(
					params.ctx, "column", name, desc.ParentID, ref, behavior,
				)
				if err != nil {
					return err
				}
				viewDesc, err := params.p.getViewDescForCascade(
					params.ctx, "column", name, desc.ParentID, ref.ID, behavior,
				)
				if err != nil {
					return err
				}
				_, err = params.p.removeDependentView(params.ctx, desc, viewDesc)
				if err != nil {
					return err
				}
			}

			if desc.PrimaryIndex.ContainsColumnID(col.ID) {
				return fmt.Errorf("column %q is referenced by the primary key", col.Name)
			}
			for _, idx := range desc.AllNonDropIndexes() {
				// We automatically drop indexes on that column that only
				// index that column (and no other columns). If CASCADE is
				// specified, we also drop other indices that refer to this
				// column.  The criteria to determine whether an index "only
				// indexes that column":
				//
				// Assume a table created with CREATE TABLE foo (a INT, b INT).
				// Then assume the user issues ALTER TABLE foo DROP COLUMN a.
				//
				// INDEX i1 ON foo(a) -> i1 deleted
				// INDEX i2 ON foo(a) STORING(b) -> i2 deleted
				// INDEX i3 ON foo(a, b) -> i3 not deleted unless CASCADE is specified.
				// INDEX i4 ON foo(b) STORING(a) -> i4 not deleted unless CASCADE is specified.

				// containsThisColumn becomes true if the index is defined
				// over the column being dropped.
				containsThisColumn := false
				// containsOnlyThisColumn becomes false if the index also
				// includes non-PK columns other than the one being dropped.
				containsOnlyThisColumn := true

				// Analyze the index.
				for _, id := range idx.ColumnIDs {
					if id == col.ID {
						containsThisColumn = true
					} else {
						containsOnlyThisColumn = false
					}
				}
				for _, id := range idx.ExtraColumnIDs {
					if desc.PrimaryIndex.ContainsColumnID(id) {
						// All secondary indices necessary contain the PK
						// columns, too. (See the comments on the definition of
						// IndexDescriptor). The presence of a PK column in the
						// secondary index should thus not be seen as a
						// sufficient reason to reject the DROP.
						continue
					}
					if id == col.ID {
						containsThisColumn = true
					}
				}
				// The loop above this comment is for the old STORING encoding. The
				// loop below is for the new encoding (where the STORING columns are
				// always in the value part of a KV).
				for _, id := range idx.StoreColumnIDs {
					if id == col.ID {
						containsThisColumn = true
					}
				}

				// Perform the DROP.
				if containsThisColumn {
					if containsOnlyThisColumn || behavior == tree.DropCascade {
						if err := params.p.dropIndexByName(
							params.ctx, tn, tree.UnrestrictedName(idx.Name), desc, false,
							behavior, ignoreIdxConstraint,
							tree.AsStringWithFlags(n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType),
						); err != nil {
							return err
						}
					} else {
						return fmt.Errorf("column %q is referenced by existing index %q", col.Name, idx.Name)
					}
				}
			}

			// Drop check constraints which reference the column.
			validChecks := desc.Checks[:0]
			for _, check := range desc.AllActiveAndInactiveChecks() {
				if used, err := check.UsesColumn(desc.TableDesc(), col.ID); err != nil {
					return err
				} else if used {
					if check.Validity == sqlbase.ConstraintValidity_Validating {
						return fmt.Errorf("referencing constraint %q in the middle of being added, try again later", check.Name)
					}
				} else {
					validChecks = append(validChecks, check)
				}
			}

			if len(validChecks) != len(desc.Checks) {
				desc.Checks = validChecks
			}
			if err := params.p.removeColumnComment(params.ctx, desc.ID, col.ID); err != nil {
				return err
			}

			found := false
			for i := range desc.Columns {
				if desc.Columns[i].ID == col.ID {
					desc.AddColumnMutation(col, sqlbase.DescriptorMutation_DROP)
					desc.Columns = append(desc.Columns[:i], desc.Columns[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("column %q in the middle of being added, try again later", name)
			}

		}

		mutationID := sqlbase.InvalidMutationID
		if isDrop {
			mutationID, err = params.p.createOrUpdateSchemaChangeJob(params.ctx, desc,
				tree.AsStringWithFlags(n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType))
			if err != nil {
				return err
			}
		}

		err = p.writeSchemaChange(ctx, desc, mutationID)
		if err != nil {
			return err
		}

		for _, id := range desc.InheritsBy {
			inhTableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
			if err != nil {
				return err
			}
			if inhTableDesc == nil {
				return fmt.Errorf(`cannot find inherit table id: %d`, id)
			}
			err = p.dropInheritColumn(ctx, params, n, inhTableDesc, name, behavior)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// UpdateFlashback 用于更新flashback系统表
func UpdateFlashback(
	ctx context.Context,
	txn *client.Txn,
	interExec *InternalExecutor,
	tblID, scID, dbID sqlbase.ID,
	objName string,
	ctTime time.Time,
	able bool,
	ttlDays int8,
	visible bool,
	objType int,
) error {
	if able {
		if ttlDays > 8 {
			return errors.New("ttl_days cannot be greater than 8 and less than 1")
		} else if ttlDays < 1 {
			return errors.New("ttl_days cannot be greater than 8 and less than 1")
		}
		if _, err := interExec.Exec(
			ctx,
			"UPDATE FLASHBACK RECORD",
			txn,
			`UPSERT INTO system.flashback (
							"type",
							"object_id",
							"object_name",
							"ct_time",
							"drop_time",
							"ttl_days",
							"visible",
							"parent_id",
							"db_id"
						) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			objType,
			tblID,
			objName,
			ctTime,
			time.Time{},
			ttlDays,
			visible,
			scID,
			dbID,
		); err != nil {
			return errors.Wrapf(err, "failed to update flashback record")
		}
	} else {
		row, err := interExec.QueryRow(
			ctx,
			"SELECT FLASHBACK",
			txn,
			`SELECT ttl_days FROM system.flashback WHERE type=$1 and object_id=$2`,
			objType,
			tblID,
		)
		if err != nil {
			return err
		}
		if row != nil {
			if _, err := interExec.Exec(
				ctx,
				"DELETE FLASHBACK",
				txn,
				`DELETE FROM system.flashback WHERE type=$1 and object_id=$2`,
				objType,
				tblID,
			); err != nil {
				return errors.Wrapf(err, "failed to delete flashback record")
			}
		} else {
			if objType == tree.DatabaseFlashbackType {
				return errors.Errorf("DB: %s not be enabled flashback", objName)
			} else if objType == tree.TableFlashbackType {
				return errors.Errorf("Table: %s not be enabled flashback", objName)
			}
		}
	}
	return nil

}

// UpdateDescNamespaceFlashbackRecord updates the system-namespace flashback;
func UpdateDescNamespaceFlashbackRecord(
	ctx context.Context, txn *client.Txn, interExec *InternalExecutor, ctTime time.Time,
) error {
	row, err := interExec.QueryRow(
		ctx,
		"SELECT MAX TTLDAYS",
		txn,
		`SELECT max(ttl_days) FROM system.flashback WHERE NOT (object_id=$1 or object_id=$2)`,
		keys.NamespaceTableID,
		keys.DescriptorTableID,
	)
	if err != nil {
		return err
	}
	if row != nil {
		var maxTTLDays int
		if row[0].Compare(nil, tree.DNull) == 0 {
			maxTTLDays = 3
		} else {
			maxTTLDays = int(*row[0].(*tree.DInt))
		}

		if _, err := interExec.Exec(
			ctx,
			"UPDATE DESCRIPTOR FLASHBACK",
			txn,
			`UPSERT INTO system.flashback (
							"type",
							"object_id",
							"object_name",
							"ct_time",
							"drop_time",
							"ttl_days",
							"visible",
							"parent_id",
							"db_id"
						) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			tree.TableFlashbackType,
			keys.DescriptorTableID,
			"descriptor",
			ctTime,
			time.Time{},
			maxTTLDays,
			false,
			keys.SystemDatabaseID,
			keys.SystemDatabaseID,
		); err != nil {
			return errors.Wrapf(err, "failed to update system-descriptor flashback record")
		}
		if _, err := interExec.Exec(
			ctx,
			"UPDATE NAMESPACE FLASHBACK",
			txn,
			`UPSERT INTO system.flashback (
							"type",
							"object_id",
							"object_name",
							"ct_time",
							"drop_time",
							"ttl_days",
							"visible",
							"parent_id",
							"db_id"
						) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			tree.TableFlashbackType,
			keys.NamespaceTableID,
			"namespace",
			ctTime,
			time.Time{},
			maxTTLDays,
			false,
			keys.SystemDatabaseID,
			keys.SystemDatabaseID,
		); err != nil {
			return errors.Wrapf(err, "failed to update system-namespace flashback record")
		}
	}
	return nil
}

// checkDatabaseFlashback 如果库开启,表不能disable,可以enable flashback,表 ttl_days 需与库 ttl_days 保持一致;
func checkDatabaseFlashback(
	ctx context.Context,
	executor *InternalExecutor,
	txn *client.Txn,
	dbID sqlbase.ID,
	ttlDaysTable int,
	able bool,
) error {
	row, err := executor.QueryRow(
		ctx,
		"SELECT DB FLASHBACK",
		txn,
		`SELECT ttl_days FROM system.flashback WHERE type=$1 AND object_id = $2`,
		tree.DatabaseFlashbackType,
		dbID,
	)
	if err != nil {
		return err
	}
	if row != nil {
		if able {
			ttlDaysDB := int(*row[0].(*tree.DInt))
			if ttlDaysDB != ttlDaysTable {
				return errors.New("The table ttl_days should be consistent with the database ttl_days")
			}
		} else {
			return errors.New("cannot alter table disable flashback on a database that has flashback")
		}
	}
	return nil
}

//MakeAddIndexDescriptor make index desc
func MakeAddIndexDescriptor(
	node *alterTableNode, n *tree.AlterTableAddIndex,
) (*sqlbase.IndexDescriptor, error) {
	indexDesc := sqlbase.IndexDescriptor{
		Name:   string(n.Name),
		Unique: n.Unique,
	}
	var isFunc bool
	for _, c := range n.Columns {
		if c.IsFuncIndex() {
			isFunc = true
		}
	}
	if isFunc {
		indexDesc.FillFuncColumns(node.tableDesc)
		if err := indexDesc.FillFunctionColumns(n.Columns); err != nil {
			return nil, err
		}
	} else {
		if err := indexDesc.FillColumns(n.Columns); err != nil {
			return nil, err
		}
	}

	return &indexDesc, nil
}
