package snapshot

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/icl/flashback"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

const (
	descriptionOpt = "description"
)

var createOptExpectValues = map[string]sql.KVStringOptValidate{
	descriptionOpt: sql.KVStringOptRequireValue,
}

func init() {
	sql.AddPlanHook(createSnapshotPlanHook)
	sql.AddPlanHook(dropSnapshotPlanHook)
	sql.AddPlanHook(showSnapshotPlanHook)
	sql.AddPlanHook(revertSnapshotPlanHook)
}

func createSnapshotPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "CREATE SNAPSHOT"
	createStmt, ok := stmt.(*tree.CreateSnapshot)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	createHeader := sqlbase.ResultColumns{
		{Name: "created", Typ: types.Int},
	}
	fn := func(ctx context.Context, n []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		currTime := p.ExecCfg().Clock.Now()
		endTime := currTime
		if createStmt.AsOf.Expr != nil {
			var err error
			if endTime, err = p.EvalAsOfTimestamp(createStmt.AsOf); err != nil {
				return pgerror.NewErrorf(pgcode.ConfigFile,
					"as of system time cause error")
			}
			// asof不能早于当前时间24小时（默认，可修改zone的配置），否则那些数据可能已经被GC回收了
			if currTime.GoTime().Sub(endTime.GoTime()).Hours() >= 24 {
				return pgerror.NewErrorf(pgcode.ConfigFile,
					"as of system time cannot be earlier than the current time of 24 hours.")
			}
		}

		desc, err := GetObjectDescriptor(ctx, p, tree.Snapshot{Type: createStmt.Type, DatabaseName: createStmt.DatabaseName, TableName: createStmt.TableName})
		if err != nil {
			return err
		}

		optsFn, err := p.TypeAsStringOpts(createStmt.Options, createOptExpectValues)
		if err != nil {
			return err
		}
		opts, err := optsFn()
		if err != nil {
			return err
		}
		var description string
		if desc, ok := opts[descriptionOpt]; ok {
			description = desc
		}
		row, err := p.ExecCfg().InternalExecutor.QueryRow(
			ctx,
			opName,
			p.Txn(),
			`select count(1) from system.snapshots`,
		)
		if err != nil {
			return errors.Wrapf(err, "error looking up snapshot")
		}
		if row != nil {
			cnt := int(*row[0].(*tree.DInt))
			if cnt > 1000 {
				return pgerror.NewErrorf(pgcode.DuplicateObject,
					"the total snapshot count already exceed %d", 1000)
			}
		}

		// Check if the snapshot name exists
		row, err = p.ExecCfg().InternalExecutor.QueryRow(
			ctx,
			opName,
			p.Txn(),
			`select count(1) as cnt from system.snapshots where name = $1 `,
			createStmt.Name,
		)
		if err != nil {
			return errors.Wrapf(err, "error looking up snapshot")
		}
		if row != nil {
			cnt := int(*row[0].(*tree.DInt))
			if cnt > 0 {
				return pgerror.NewErrorf(pgcode.DuplicateObject,
					"snapshot named %s already exists", createStmt.Name)
			}
		}
		// Check snapshot count > 3.
		row, err = p.ExecCfg().InternalExecutor.QueryRow(
			ctx,
			opName,
			p.Txn(),
			`select count(1) from system.snapshots where type = $1 and object_id=$2`,
			createStmt.Type,
			desc.GetID(),
		)
		if err != nil {
			return err
		}
		if row != nil {
			cnt := int(*row[0].(*tree.DInt))
			if cnt >= 3 {
				return pgerror.NewErrorf(pgcode.DuplicateObject,
					"snapshot num on object[%s] already exceed 3, can not create new. ", desc.GetName())
			}
		}

		parentID, err := uuid.NewV4()
		if err != nil {
			return err
		}
		snapshots := []storage.Snapshot{
			{ID: parentID, ParentID: nil, Type: createStmt.Type, ObjectID: uint32(desc.GetID())},
		}
		if createStmt.Type == tree.DatabaseSnapshotType {
			// 查询得到所有表，建立内部快照
			tblDescs, externalTbl, err := allTablesOfDB(ctx, p.Txn(), desc.GetDatabase())
			if err != nil {
				return err
			}
			for _, desc := range tblDescs {
				ids, err := desc.FindReferences()
				if err != nil {
					return err
				}
				for id := range ids {
					if _, ok := externalTbl[id]; ok {
						return errors.Errorf("table[%s] have some depends on other DB ,can not create snapshot.", desc.Name)
					}
				}
				snapID, err := uuid.NewV4()
				if err != nil {
					return err
				}
				snapshots = append(snapshots,
					storage.Snapshot{ID: snapID, ParentID: &parentID, Type: tree.TableSnapshotType, ObjectID: uint32(desc.GetID())})
			}
		} else {
			if tblDesc := desc.GetTable(); tblDesc != nil {
				// 检查是否有外键等约束
				// 只有无外键约束的情况下，才能创建快照
				references, err := tblDesc.FindReferences()
				if err != nil {
					return err
				}
				if len(references) > 0 {
					tblDesc.AllIndexSpans()
					return errors.Errorf("table[%s] have some constraints ,can not create snapshot.", tblDesc.Name)
				}
			}
		}
		rowsCount := 0
		for _, snap := range snapshots {
			var rows int
			var err error
			if snap.ParentID != nil {
				rows, err = p.ExecCfg().InternalExecutor.Exec(
					ctx,
					opName,
					p.Txn(),
					"insert into system.snapshots (id,name,type,object_id,sub_id,parent_id,asof,description) values ($1, $2, $3, $4, 0, $5, $6, $7)",
					snap.ID.GetBytes(),
					createStmt.Name,
					snap.Type,
					snap.ObjectID,
					snap.ParentID.GetBytes(),
					timeutil.Unix(0, endTime.WallTime),
					description,
				)
			} else {
				rows, err = p.ExecCfg().InternalExecutor.Exec(
					ctx,
					opName,
					p.Txn(),
					"insert into system.snapshots (id,name,type,object_id,sub_id,asof,description) values ($1, $2, $3, $4, 0, $5, $6)",
					snap.ID.GetBytes(),
					createStmt.Name,
					snap.Type,
					desc.GetID(),
					timeutil.Unix(0, endTime.WallTime),
					description,
				)
			}
			if err != nil {
				return err
			} else if rows != 1 {
				return pgerror.NewAssertionErrorf("%d rows affected by user creation; expected exactly one row affected",
					rows,
				)
			}
			rowsCount += rows
		}
		rowsAffected := tree.DInt(rowsCount)
		resultsCh <- tree.Datums{tree.NewDInt(rowsAffected)}
		return nil
	}
	return fn, createHeader, nil, false, nil
}

func dropSnapshotPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "DROP SNAPSHOT"
	dropStmt, ok := stmt.(*tree.DropSnapshot)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	dropHeader := sqlbase.ResultColumns{
		{Name: "deleted", Typ: types.Int},
	}
	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		row, err := p.ExecCfg().InternalExecutor.Query(
			ctx,
			opName,
			p.Txn(),
			`SELECT  id,name,type,object_id,asof FROM system.snapshots WHERE name=$1`,
			dropStmt.Name,
		)
		if err != nil {
			return errors.Wrapf(err, "error looking up snapshot")
		}
		if len(row) <= 0 {
			return pgerror.NewErrorf(pgcode.UndefinedObject,
				"object have not snapshot named '%s' ", dropStmt.Name)
		}
		rows, err := p.ExecCfg().InternalExecutor.Exec(ctx, opName, p.Txn(),
			"DELETE FROM system.snapshots WHERE name=$1", dropStmt.Name)
		if err != nil {
			return err
		}
		rowsAffected := tree.DInt(rows)
		rs := tree.Datums{tree.NewDInt(rowsAffected)}
		resultsCh <- rs
		return nil
	}
	return fn, dropHeader, nil, false, nil
}

func showSnapshotPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "SHOW SNAPSHOT"
	showStmt, ok := stmt.(*tree.ShowSnapshot)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	showHeader := sqlbase.ResultColumns{
		{Name: "id", Typ: types.UUID},
		{Name: "name", Typ: types.String},
		{Name: "type", Typ: types.String},
		{Name: "objectName", Typ: types.String},
		{Name: "asof", Typ: types.Timestamp},
		{Name: "description", Typ: types.String},
	}
	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		where := "WHERE parent_id is null"
		if !showStmt.All {
			if showStmt.Name != "" {
				where = fmt.Sprintf(`%s AND name = '%s'`, where, showStmt.Name)
			} else {
				objectID, err := GetObjectID(ctx, p, tree.Snapshot{Type: showStmt.Type, DatabaseName: showStmt.DatabaseName, TableName: showStmt.TableName})
				if err != nil {
					return err
				}
				where = fmt.Sprintf("%s AND type=%d AND object_id = %d", where, showStmt.Type, objectID)
			}
		}
		query := "SELECT id, name, type, object_id, asof, description FROM system.snapshots " + where
		rows, err := p.ExecCfg().InternalExecutor.Query(ctx, opName, p.Txn(), query)
		if err != nil {
			return err
		}
		for _, r := range rows {
			otype := *r[2].(*tree.DInt)
			if otype == tree.DatabaseSnapshotType {
				r[2] = tree.NewDString("Database")
				objectDesc, err := sqlbase.GetDatabaseDescFromID(ctx, p.Txn(), sqlbase.ID(*r[3].(*tree.DInt)))
				if err != nil {
					r[3] = tree.NewDString(err.Error())
				}
				r[3] = tree.NewDString(objectDesc.Name)
			} else if otype == tree.TableSnapshotType {
				r[2] = tree.NewDString("Table")
				tbl, err := p.LookupTableByID(ctx, sqlbase.ID(*r[3].(*tree.DInt)))
				if err != nil {
					r[3] = tree.NewDString(err.Error())
				} else {
					r[3] = tree.NewDString(tbl.Desc.Name)
				}
			} else {
				r[2] = tree.NewDString("unknown type")
				r[3] = tree.NewDString("unknown object")
			}
			resultsCh <- r
		}
		return nil
	}
	return fn, showHeader, nil, false, nil
}
func revertSnapshotPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "REVERT SNAPSHOT"
	revertStmt, ok := stmt.(*tree.RevertSnapshot)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	dropHeader := sqlbase.ResultColumns{
		{Name: "reverted", Typ: types.Int},
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		txn := p.Txn()
		desc, err := GetObjectDescriptor(ctx, p, tree.Snapshot{
			Type:         revertStmt.Type,
			TableName:    revertStmt.TableName,
			DatabaseName: revertStmt.DatabaseName,
		})
		if err != nil {
			return err
		}

		//查询快照时间
		rs, err := p.ExecCfg().InternalExecutor.QueryRow(
			ctx,
			opName,
			txn,
			`SELECT asof FROM system.snapshots WHERE name=$1 AND type=$2 AND object_id = $3`,
			revertStmt.Name,
			revertStmt.Type,
			desc.GetID(),
		)
		if err != nil {
			return errors.Wrapf(err, "error looking up snapshot")
		}
		if rs == nil {
			return pgerror.NewErrorf(pgcode.UndefinedObject,
				"object[%s] have not snapshot named '%s' ", desc.GetName(), revertStmt.Name)
		}
		snapTs := *rs[0].(*tree.DTimestamp)

		var immutableTbls []sqlbase.ImmutableTableDescriptor
		if tblDesc := desc.Table(hlc.Timestamp{}); tblDesc != nil {
			// 检查是否有外键等约束
			// 只有无外键约束的情况下，才能恢复快照
			references, err := tblDesc.FindReferences()
			if err != nil {
				return err
			}
			if len(references) > 0 {
				tblDesc.AllIndexSpans()
				return errors.Errorf("table[%s] have some constraints ,can not revert.", tblDesc.Name)
			}
			immutableTbls = append(immutableTbls, sqlbase.ImmutableTableDescriptor{TableDescriptor: *desc.Table(hlc.Timestamp{})})
		} else {
			// 查询得到所有表，建立内部快照
			tblDescs, externalTbl, err := allTablesOfDB(ctx, p.Txn(), desc.GetDatabase())
			if err != nil {
				return err
			}
			for _, tbl := range tblDescs {
				// 库级快照，检查外部依赖是否在同一数据库内

				ids, err := tbl.FindReferences()
				if err != nil {
					return err
				}
				for id := range ids {
					if _, ok := externalTbl[id]; ok {
						return errors.Errorf("table[%s] have some depends on other DB ,can not revert.", tbl.Name)
					}
				}
				immutableTbls = append(immutableTbls, sqlbase.ImmutableTableDescriptor{TableDescriptor: tbl})
			}
		}

		var numReverted int64

		for _, immutable := range immutableTbls {

			spans := roachpb.Spans{immutable.TableDescriptor.TableSpan()}

			var rangeRaws []client.KeyValue
			for _, span := range spans {
				ranges, err := sql.ScanMetaKVs(ctx, txn, span)
				if err != nil {
					return err
				}
				rangeRaws = append(rangeRaws, ranges...)
			}

			var ranges []roachpb.RangeDescriptor

			for _, rangeRaw := range rangeRaws {
				var desc roachpb.RangeDescriptor
				if err := rangeRaw.ValueProto(&desc); err != nil {
					return errors.Wrapf(err, "error parse range")
				}
				ranges = append(ranges, desc)
			}

			var rangeSpans []roachpb.Span

			for _, rng := range ranges {
				span := roachpb.Span{Key: rng.StartKey.AsRawKey(), EndKey: rng.EndKey.AsRawKey()}
				rangeSpans = append(rangeSpans, span)
			}

			n, err := flashback.RevertSpanRangesWithIntents(ctx, txn, p.ExecCfg().DB, rangeSpans, hlc.Timestamp{WallTime: snapTs.Time.UnixNano()})
			if err != nil {
				return errors.Wrapf(err, "error revert table %v", immutable.Name)
			}

			numReverted += n

			// Possibly initiate a run of CREATE STATISTICS.
			p.ExecCfg().StatsRefresher.NotifyMutation(immutable.GetID(), int(numReverted))
			// TODO: 目前显示影响数据行数结果不正确
			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(numReverted))}
		}

		return nil
	}
	return fn, dropHeader, nil, false, nil
}

// GetObjectID is that get ID by PlanHookState and Snapshot
func GetObjectID(ctx context.Context, p sql.PlanHookState, n tree.Snapshot) (sqlbase.ID, error) {
	desc, err := GetObjectDescriptor(ctx, p, n)
	if err != nil {
		return 0, err
	}
	return desc.GetID(), nil
}

// GetObjectDescriptor is that get object descriptor
func GetObjectDescriptor(
	ctx context.Context, p sql.PlanHookState, snap tree.Snapshot,
) (*sqlbase.Descriptor, error) {
	var object = &sqlbase.Descriptor{}
	//n := s.(tree.Snapshot)
	if snap.Type == tree.DatabaseSnapshotType {
		dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(snap.DatabaseName), true /*required*/)
		if err != nil {
			return nil, err
		}
		object.Union = &sqlbase.Descriptor_Database{Database: dbDesc}
	} else if snap.Type == tree.TableSnapshotType {
		var tableName *tree.TableName
		if snap.TableName.Schema() == "" {
			tableName = tree.NewTableName(tree.Name(p.CurrentDatabase()), snap.TableName.TableName)
		} else {
			tableName = &snap.TableName
		}
		tDesc, err := sql.ResolveExistingObject(ctx, p, tableName, true /*required*/, 1)
		if err != nil {
			return nil, err
		}
		object.Union = &sqlbase.Descriptor_Table{Table: tDesc.TableDesc()}
	}
	return object, nil
}
func allTablesOfDB(
	ctx context.Context, txn *client.Txn, dbDesc *sql.DatabaseDescriptor,
) ([]sqlbase.TableDescriptor, map[sqlbase.ID]sqlbase.TableDescriptor, error) {
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
	var tblDescs []sqlbase.TableDescriptor
	// 本数据库之外的其他数据表
	externalTbl := make(map[sqlbase.ID]sqlbase.TableDescriptor)
	for _, row := range rows {
		desc := sqlbase.Descriptor{}
		if err := row.ValueProto(&desc); err != nil {
			return nil, nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
		}
		if tbl := desc.Table(row.Value.Timestamp); tbl != nil {
			if tbl.Dropped() {
				continue
			}
			if _, ok := schemaID[tbl.ParentID]; ok {
				tblDescs = append(tblDescs, *tbl)
			} else {
				externalTbl[tbl.ID] = *tbl
			}
		}
	}
	return tblDescs, externalTbl, nil
}
