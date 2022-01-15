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
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type dropDatabaseNode struct {
	n      *tree.DropDatabase
	dbDesc *sqlbase.DatabaseDescriptor
	tdt    []toDeleteTable
	tdf    []toDeleteFunction
}

// DropDatabase drops a database.
// Privileges: DROP on database and DROP on all tables in the database.
//   Notes: postgres allows only the database owner to DROP a database.
//          mysql requires the DROP privileges on the database.
// TODO(XisiHuang): our DROP DATABASE is like the postgres DROP SCHEMA
// (znbase database == postgres schema). the postgres default of not
// dropping the schema if there are dependent objects is more sensible
// (see the RESTRICT and CASCADE options).
func (p *planner) DropDatabase(ctx context.Context, n *tree.DropDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.NewDangerousStatementErrorf("DROP DATABASE on current database")
	}

	// Check that the database exists.
	dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Name), !n.IfExists)
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		// IfExists was specified and database was not found.
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	scDescs := dbDesc.GetSchemaDesc(dbDesc.GetSchemaNames())

	for _, scDesc := range scDescs {
		if err := p.CheckPrivilege(ctx, &scDesc, privilege.DROP); err != nil {
			return nil, err
		}
	}
	var tdt []toDeleteTable
	var tdf []toDeleteFunction
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tdt, tdf, err = p.DoDropWithBehavior(ctx, n.DropBehavior, DropDatabase, dbDesc, dbDesc.GetSchemaNames())
	})

	if err != nil {
		return nil, err
	}

	return &dropDatabaseNode{n: n, dbDesc: dbDesc, tdt: tdt, tdf: tdf}, nil
}

func (n *dropDatabaseNode) startExec(params runParams) error {
	if err := CheckDatabaseSnapShots(params.ctx,
		params.ExecCfg().InternalExecutor,
		params.EvalContext().Txn,
		n.dbDesc,
		"cannot drop database that has snapshots"); err != nil {
		return err
	}
	tblDescs, _, err := allTablesOfDB(params.ctx, params.EvalContext().Txn, n.dbDesc)
	if err != nil {
		return err
	}
	for _, tbl := range tblDescs {
		if err := CheckTableSnapShots(params.ctx,
			params.ExecCfg().InternalExecutor,
			params.EvalContext().Txn,
			&tbl,
			"cannot drop database that has snapshots"); err != nil {
			return err
		}
	}
	ctx := params.ctx
	p := params.p
	dropBeginTime := p.txn.ReadTimestamp().GoTime()
	// update flashback record
	if err := updateDBDropTimeIfDBEnabled(ctx,
		params.p.execCfg.InternalExecutor,
		params.p.txn,
		dropBeginTime,
		n.dbDesc.ID); err != nil {
		return err
	}

	tbNameStrings, jobID, err := p.DoDrop(params,
		tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType), n.tdt, n.dbDesc.ID)
	if err != nil {
		return err
	}
	for _, d := range n.tdf {
		oldFuncKey := functionKey{parentID: d.desc.ParentID, name: "", funcName: d.desc.Name}
		oldKey := oldFuncKey.Key()
		descKey := sqlbase.MakeDescMetadataKey(d.desc.ID)
		if err := params.p.txn.Del(ctx, oldKey, descKey); err != nil {
			return err
		}
	}

	_ /* zoneKey */, nameKey, descKey := getKeysForDatabaseDescriptor(n.dbDesc)

	delKeys := p.dropSchemaImpl(ctx, n.dbDesc, n.dbDesc.GetSchemaNames())

	b := &client.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Del %s", descKey)
		log.VEventf(ctx, 2, "Del %s", nameKey)
	}

	delKeys = append(delKeys, descKey, nameKey)
	b.Del(delKeys...)

	// No job was created because no tables were dropped, so zone config can be
	// immediately removed.
	if jobID == 0 {
		zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(n.dbDesc.ID))
		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
		}
		// Delete the zone config entry for this database.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
	}

	p.Tables().addUncommittedDatabase(n.dbDesc.Name, n.dbDesc.ID, dbDropped)

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	if err := p.removeDbComment(ctx, n.dbDesc.ID); err != nil {
		return err
	}

	// Log Drop Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogDropDatabase),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(n.dbDesc.ID),
			Desc: struct {
				DatabaseName string
				TableList    []string
			}{
				n.n.Name.String(),
				tbNameStrings,
			},
		},
		Info: &infos.DropDatabaseInfo{
			DatabaseName:         n.n.Name.String(),
			Statement:            n.n.String(),
			User:                 p.SessionData().User,
			DroppedSchemaObjects: tbNameStrings,
		},
	}
	return nil
}

func (*dropDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (*dropDatabaseNode) Close(context.Context)        {}
func (*dropDatabaseNode) Values() tree.Datums          { return tree.Datums{} }

// filterCascadedTables takes a list of table descriptors and removes any
// descriptors from the list that are dependent on other descriptors in the
// list (e.g. if view v1 depends on table t1, then v1 will be filtered from
// the list).
func (p *planner) filterCascadedTables(
	ctx context.Context, tables []toDeleteTable,
) ([]toDeleteTable, error) {
	// Accumulate the set of all tables/views that will be deleted by cascade
	// behavior so that we can filter them out of the list.
	cascadedTables := make(map[sqlbase.ID]bool)
	for _, toDel := range tables {
		desc := toDel.desc
		if err := p.accumulateDependentTables(ctx, cascadedTables, desc); err != nil {
			return nil, err
		}
	}
	filteredTableList := make([]toDeleteTable, 0, len(tables))
	for _, toDel := range tables {
		if !cascadedTables[toDel.desc.ID] {
			filteredTableList = append(filteredTableList, toDel)
		}
	}
	return filteredTableList, nil
}

func (p *planner) accumulateDependentTables(
	ctx context.Context, dependentTables map[sqlbase.ID]bool, desc *sqlbase.MutableTableDescriptor,
) error {
	for _, ref := range desc.DependedOnBy {
		dependentTables[ref.ID] = true
		dependentDesc, err := p.Tables().getMutableTableVersionByID(ctx, ref.ID, p.txn)
		if err != nil {
			return err
		}
		if err := p.accumulateDependentTables(ctx, dependentTables, dependentDesc); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) removeDbComment(ctx context.Context, dbID sqlbase.ID) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx,
		"delete-db-comment",
		p.txn,
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.DatabaseCommentType,
		dbID)

	return err
}

func updateDBDropTimeIfDBEnabled(
	ctx context.Context,
	executor *InternalExecutor,
	txn *client.Txn,
	dropTime time.Time,
	dbID sqlbase.ID,
) error {
	rows, err := executor.QueryRow(
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
	if rows != nil {
		if _, err := executor.Exec(
			ctx,
			"update flashback record",
			txn,
			`UPDATE system.flashback SET drop_time = $1 WHERE db_id = $2 and drop_time=$3`,
			dropTime,
			dbID,
			time.Time{},
		); err != nil {
			return errors.Wrapf(err, "failed to update DB drop time record of the flashback table")
		}
	}
	return nil
}

func allTablesOfDB(
	ctx context.Context, txn *client.Txn, dbDesc *DatabaseDescriptor,
) ([]sqlbase.MutableTableDescriptor, map[sqlbase.ID]sqlbase.TableDescriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	rows, err := txn.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, nil, err
	}
	schemaID := make(map[sqlbase.ID]string)
	schemaName := dbDesc.GetSchemaNames()
	for _, sName := range schemaName {
		schemaID[dbDesc.GetSchemaID(sName)] = sName
	}
	var tblDescs []sqlbase.MutableTableDescriptor
	// Other tables not belonging to this database
	externalTbl := make(map[sqlbase.ID]sqlbase.TableDescriptor)
	for _, row := range rows {
		desc := sqlbase.Descriptor{}
		if err := row.ValueProto(&desc); err != nil {
			return nil, nil, errors.Wrapf(err, "%s: Unable to unmarshal SQL descriptor", row.Key)
		}
		if tbl := desc.GetTable(); tbl != nil {
			if tbl.Dropped() {
				continue
			}
			if _, ok := schemaID[tbl.ParentID]; ok {
				tblDescs = append(tblDescs, sqlbase.MutableTableDescriptor{TableDescriptor: *tbl})
			} else {
				externalTbl[tbl.ID] = *tbl
			}
		}
	}
	return tblDescs, externalTbl, nil
}
