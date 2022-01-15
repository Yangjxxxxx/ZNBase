package flashback

import (
	"context"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/icl/dump"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

func init() {
	sql.AddPlanHook(revertDropFlashbackPlanHook)
	sql.AddPlanHook(revertFlashbackPlanHook)
	sql.AddPlanHook(showFlashbackPlanHook)
	sql.AddPlanHook(alterDatabaseAbleFlashBackPlanHook)
	sql.AddPlanHook(clearIntentsPlanHook)
}

func clearIntentsPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "CLEAR INTENT"
	clearStmt, ok := stmt.(*tree.ClearIntent)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	showHeader := sqlbase.ResultColumns{
		{Name: "table", Typ: types.String},
		{Name: "clear_intents", Typ: types.Int},
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		txn := p.Txn()
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		var desc = &sqlbase.Descriptor{}
		if clearStmt.Type == tree.DatabaseFlashbackType {
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, clearStmt.DatabaseName.String(), true /*required*/)
			if err != nil {
				return err
			}
			desc.Union = &sqlbase.Descriptor_Database{Database: dbDesc}
		} else if clearStmt.Type == tree.TableFlashbackType {
			tExistDesc, err := sql.ResolveExistingObject(ctx, p, &clearStmt.TableName, true, 1)
			if err != nil {
				return err
			}
			desc.Union = &sqlbase.Descriptor_Table{Table: tExistDesc.TableDesc()}
		}

		var immutableTbls []sqlbase.ImmutableTableDescriptor
		if clearStmt.Type == tree.TableFlashbackType {
			tblDesc := desc.Table(hlc.Timestamp{})
			immutableTbls = append(immutableTbls, sqlbase.ImmutableTableDescriptor{TableDescriptor: *tblDesc})
		} else {
			tblDescs, _, err := allTablesOfDB(ctx, p.Txn(), desc.GetDatabase(), true)
			if err != nil {
				return err
			}
			for _, tbl := range tblDescs {
				immutableTbls = append(immutableTbls, sqlbase.ImmutableTableDescriptor{TableDescriptor: tbl})
			}
		}

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
			clearNums, err := clearIntents(ctx, txn, p.ExecCfg().DB, rangeSpans)
			if err != nil {
				return err
			}

			// Possibly initiate a run of CREATE STATISTICS.
			p.ExecCfg().StatsRefresher.NotifyMutation(immutable.GetID(), int(clearNums))

			resultsCh <- tree.Datums{
				tree.NewDString(immutable.Name),
				tree.NewDInt(tree.DInt(clearNums)),
			}
		}

		return nil
	}
	return fn, showHeader, nil, false, nil
}

// clearIntents 清理写意图
func clearIntents(
	ctx context.Context, txn *client.Txn, db *client.DB, rangeSpans []roachpb.Span,
) (int64, error) {
	var intents []roachpb.Intent
	var revertKeyNums int64
	for len(rangeSpans) != 0 {

		var b client.Batch
		for _, span := range rangeSpans {
			b.ClearIntentRange(span.Key, span.EndKey, false, intents)
		}

		if err := txn.Run(ctx, &b); err != nil {
			return 0, err
		}

		//intents = intents[:0]
		rangeSpans = rangeSpans[:0]
		for _, raw := range b.RawResponse().Responses {
			r := raw.GetClearIntent()
			if r.ResumeSpan != nil {
				if !r.ResumeSpan.Valid() {
					return 0, errors.Errorf("invalid resume span: %s", r.ResumeSpan)
				}
				rangeSpans = append(rangeSpans, *r.ResumeSpan)
			}
			if r.UnknownIntents != nil {
				for _, intent := range r.UnknownIntents {
					status, err := checkIntentStatus(ctx, db, intent)
					if err != nil {
						return 0, err
					}
					if !status.IsFinalized() {
						return 0, errors.Errorf("encounter other transaction[%v] during flashback, status[%v]", intent.Txn.ID, status.String())
					}

					intents = append(intents, intent)

				}

			}
			revertKeyNums += r.Reverted
		}
	}
	return revertKeyNums, nil
}

// GetObjectDescriptor is that get object descriptor
func GetObjectDescriptor(
	ctx context.Context, p sql.PlanHookState, flash tree.Flashback,
) (*sqlbase.Descriptor, error) {
	var object = &sqlbase.Descriptor{}
	if flash.Type == tree.DatabaseFlashbackType {
		dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, flash.DatabaseName.String(), true /*required*/)
		if err != nil {
			return nil, err
		}
		object.Union = &sqlbase.Descriptor_Database{Database: dbDesc}
	} else if flash.Type == tree.TableFlashbackType {
		if flash.TableName.SchemaName == "" {
			flash.TableName.CatalogName = tree.Name(p.SessionData().Database)
			flash.TableName.SchemaName = tree.Name(p.SessionData().SearchPath.String())
		} else if flash.TableName.CatalogName == "" {
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, p.SessionData().Database, true /*required*/)
			if err != nil {
				return nil, err
			}
			if dbDesc.ExistSchema(flash.TableName.SchemaName.String()) {
				flash.TableName.CatalogName = tree.Name(p.SessionData().Database)
			} else {
				flash.TableName.CatalogName = flash.TableName.SchemaName
				flash.TableName.SchemaName = tree.Name(p.SessionData().SearchPath.String())
			}
		}
		dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, flash.TableName.CatalogName.String(), true /*required*/)
		if err != nil {
			return nil, err
		}
		scDesc, err := dbDesc.GetSchemaByName(flash.TableName.SchemaName.String())
		if err != nil {
			return nil, err
		}
		row, err := p.ExecCfg().InternalExecutor.QueryRow(
			ctx,
			"SELECT TABLE_ID FROM FLASHBACK",
			p.Txn(),
			`SELECT object_id FROM system.flashback WHERE (type = $1) AND (parent_id = $2) AND (object_name = $3) AND (drop_time = $4)`,
			flash.Type,
			scDesc.GetID(),
			flash.TableName.TableName.String(),
			time.Time{},
		)
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, pgerror.NewErrorf(pgcode.NoData,
				"Table: %s have not enable flashback", flash.TableName.TableName.String())
		}
		tblID := *row[0].(*tree.DInt)

		tExistDesc, err := sqlbase.GetTableDescFromID(ctx, p.Txn(), sqlbase.ID(tblID))
		if err != nil {
			return nil, err
		}
		object.Union = &sqlbase.Descriptor_Table{Table: tExistDesc}
	}
	return object, nil
}

func allTablesOfDB(
	ctx context.Context, txn *client.Txn, dbDesc *sql.DatabaseDescriptor, ifExist bool,
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
	// Other tables not belonging to this database
	externalTbl := make(map[sqlbase.ID]sqlbase.TableDescriptor)
	for _, row := range rows {
		desc := sqlbase.Descriptor{}
		if err := row.ValueProto(&desc); err != nil {
			return nil, nil, errors.Wrapf(err, "%s: Unable to unmarshal SQL descriptor", row.Key)
		}
		if tbl := desc.Table(row.Value.Timestamp); tbl != nil {
			if tbl.Dropped() && ifExist {
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

// revertDescName 用于回滚系统表namespace和descriptor
func revertDescName(
	ctx context.Context,
	p sql.PlanHookState,
	ts int64,
	objectID sqlbase.ID,
	parentID sqlbase.ID,
	objectName string,
) error {
	var rangeSpans []roachpb.Span

	startKey := sqlbase.MakeNameMetadataKey(parentID, objectName)
	endKey := startKey.PrefixEnd()
	rangeSpans = append(rangeSpans, roachpb.Span{Key: startKey, EndKey: endKey})

	startKey = sqlbase.MakeDescMetadataKey(objectID)
	endKey = startKey.PrefixEnd()
	rangeSpans = append(rangeSpans, roachpb.Span{Key: startKey, EndKey: endKey})

	if _, err := RevertSpanRangesWithIntents(ctx, p.Txn(), p.ExecCfg().DB, rangeSpans, hlc.Timestamp{WallTime: ts}); err != nil {
		return errors.Wrapf(err, "Flashback descriptor error")
	}

	return nil
}

// revertDescName 用于回滚系统表descriptor
func revertDesc(ctx context.Context, p sql.PlanHookState, ts int64, objectID sqlbase.ID) error {
	startKey := sqlbase.MakeDescMetadataKey(objectID)
	endKey := startKey.PrefixEnd()

	var rangeSpans []roachpb.Span
	rangeSpans = append(rangeSpans, roachpb.Span{Key: startKey, EndKey: endKey})
	if _, err := RevertSpanRangesWithIntents(ctx, p.Txn(), p.ExecCfg().DB, rangeSpans, hlc.Timestamp{WallTime: ts}); err != nil {
		return errors.Wrapf(err, "Flashback descriptor error")
	}
	return nil
}

// MakeTableIDKey returns the gossip key for node ID info.
func MakeTableIDKey(tableID sqlbase.ID) string {
	return gossip.MakeKey(gossip.KeyTableDescDropCache, strconv.FormatInt(int64(tableID), 10))
}

func revertFlashbackPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "FLASHBACK"
	revertStmt, ok := stmt.(*tree.RevertFlashback)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	dropHeader := sqlbase.ResultColumns{
		{Name: "FLASHBACK", Typ: types.Int},
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		txn := p.Txn()
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		currTime := p.ExecCfg().Clock.Now()
		endTime := currTime
		if revertStmt.AsOf.Expr != nil {
			var err error
			if endTime, err = p.EvalAsOfTimestamp(revertStmt.AsOf); err != nil {
				return err
			}
		} else {
			return pgerror.NewErrorf(pgcode.UndefinedObject,
				"As of system time must be set")
		}

		desc, err := GetObjectDescriptor(ctx, p, tree.Flashback{
			Type:         revertStmt.Type,
			TableName:    revertStmt.TableName,
			DatabaseName: revertStmt.DatabaseName,
		})
		if err != nil {
			return err
		}
		// Query ttlDays. If ttlDays is empty, as of time cannot be earlier than 24 hours
		var ttlDays tree.DInt
		var ctTime tree.DTimestamp
		if revertStmt.Type == keys.DatabaseCommentType {
			rs, err := p.ExecCfg().InternalExecutor.QueryRow(
				ctx,
				opName,
				txn,
				`SELECT ttl_days, ct_time FROM system.flashback WHERE (db_id = $1) AND (drop_time = $2) order by ct_time desc limit 1`,
				desc.GetID(),
				time.Time{},
			)
			if err != nil {
				return errors.Wrapf(err, "Failed to looking up ttlDays")
			}
			if rs == nil {
				return pgerror.NewErrorf(pgcode.NoData,
					"Database: %s have not enable flashback", revertStmt.DatabaseName.String())
			}
			ttlDays = *rs[0].(*tree.DInt)
			ctTime = *rs[1].(*tree.DTimestamp)
		} else {
			rs, err := p.ExecCfg().InternalExecutor.QueryRow(
				ctx,
				opName,
				txn,
				`SELECT ttl_days, ct_time FROM system.flashback WHERE (type = $1) AND (object_id = $2)`,
				revertStmt.Type,
				desc.GetID(),
			)
			if err != nil {
				return errors.Wrapf(err, "Failed to looking up table ttlDays")
			}
			if rs == nil {
				return pgerror.NewErrorf(pgcode.NoData,
					"Table: %s have not enable flashback", revertStmt.TableName.TableName.String())
			}
			ttlDays = *rs[0].(*tree.DInt)
			ctTime = *rs[1].(*tree.DTimestamp)
		}

		if ctTime.Time.Sub(endTime.GoTime()).Seconds() > 0 {
			return pgerror.NewErrorf(pgcode.NoData,
				"As of system time cannot be earlier than the last ct_time.")
		}
		// as of cannot be earlier than the current time ttlDays * 24 hours
		if currTime.GoTime().Sub(endTime.GoTime()).Hours() >= float64(ttlDays*24) {
			return pgerror.NewErrorf(pgcode.DatetimeFieldOverflow,
				"As of system time cannot be earlier than the ttlDays.")
		}
		var immutableTbls []sqlbase.ImmutableTableDescriptor
		if revertStmt.Type == tree.TableFlashbackType {
			// Check whether there are foreign key, index and other constraints
			// It can only be restored if there is no constraint
			tblDesc := desc.Table(hlc.Timestamp{})
			refs, err := tblDesc.FindReferences()
			if err != nil {
				return err
			}
			if len(refs) > 0 {
				tblDesc.AllIndexSpans()
				return errors.Errorf("Table[%s] have some constraints ,can not revert.", tblDesc.Name)
			}
			immutableTbls = append(immutableTbls, sqlbase.ImmutableTableDescriptor{TableDescriptor: *tblDesc})
		} else {
			// Query all tables
			tblDescs, externalTbl, err := allTablesOfDB(ctx, p.Txn(), desc.GetDatabase(), true)
			if err != nil {
				return err
			}
			for _, tbl := range tblDescs {
				if row, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
					ctx,
					"select table flashback record",
					txn,
					`SELECT * FROM system.flashback WHERE  (type = $1) AND (object_id = $2)`,
					tree.TableFlashbackType,
					tbl.GetID(),
				); err != nil {
					return err
				} else if row == nil {
					continue
				}
				// Library level flashback to check whether external dependencies are in the same database
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
			if err := revertDesc(ctx, p, endTime.WallTime, immutable.ID); err != nil {
				return err
			}
			// Clear table cache
			if err := p.ExecCfg().Gossip.AddInfo(MakeTableIDKey(immutable.ID), nil, 0); err != nil {
				return err
			}
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

			n, err := RevertSpanRangesWithIntents(ctx, txn, p.ExecCfg().DB, rangeSpans, endTime)
			if err != nil {
				return errors.Wrapf(err, "error flashback table %v", immutable.Name)
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

// RevertSpanRangesWithIntents 回滚range到指定时间点，并检查里面的写意图，如果写意图已提交可正常清理
func RevertSpanRangesWithIntents(
	ctx context.Context,
	txn *client.Txn,
	db *client.DB,
	rangeSpans []roachpb.Span,
	endTime hlc.Timestamp,
) (int64, error) {
	var intents []roachpb.Intent
	var revertKeyNums int64
	for len(rangeSpans) != 0 {

		var b client.Batch
		for _, span := range rangeSpans {
			b.RevertRange(span.Key, span.EndKey, endTime.WallTime, false, intents)
		}

		// TODO: 不设置MaxSpanRequestKeys会导致报错-command is too large.暂时通过增大kv.raft.command.max_size处理
		//b.Header.MaxSpanRequestKeys = 300000

		if err := txn.Run(ctx, &b); err != nil {
			return 0, err
		}

		//intents = intents[:0]
		rangeSpans = rangeSpans[:0]
		for _, raw := range b.RawResponse().Responses {
			r := raw.GetRevert()
			if r.ResumeSpan != nil {
				if !r.ResumeSpan.Valid() {
					return 0, errors.Errorf("invalid resume span: %s", r.ResumeSpan)
				}
				rangeSpans = append(rangeSpans, *r.ResumeSpan)
			}
			if r.UnknownIntents != nil {
				for _, intent := range r.UnknownIntents {
					//isExists := false
					//for _, v := range intents {
					//	if v.Txn.ID == intent.Txn.ID {
					//		isExists = true
					//		break
					//	}
					//}
					// TODO: 这里intents正常是应该要排重,但是经测试会有数据恢复不正常的问题
					status, err := checkIntentStatus(ctx, db, intent)
					if err != nil {
						return 0, err
					}
					if !status.IsFinalized() {
						return 0, errors.Errorf("encounter other transaction[%v] during flashback, status[%v]", intent.Txn.ID, status.String())
					}

					intents = append(intents, intent)

				}

			}
			revertKeyNums += r.Reverted
		}
	}
	return revertKeyNums, nil
}

// checkIntentStatus 检查Intent对应事务状态
func checkIntentStatus(
	ctx context.Context, db *client.DB, intent roachpb.Intent,
) (roachpb.TransactionStatus, error) {

	b := &client.Batch{}
	//b.Header.Timestamp = head.Timestamp
	b.Header.Timestamp = db.Clock().Now()
	b.AddRawRequest(&roachpb.QueryTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: intent.Key,
		},
		Txn: intent.Txn,
	})
	if err := db.Run(ctx, b); err != nil {
		return roachpb.COMMITTED, err
	}
	res := b.RawResponse().Responses
	queryTxnResp := res[0].GetInner().(*roachpb.QueryTxnResponse)
	queriedTxn := &queryTxnResp.QueriedTxn

	return queriedTxn.Status, nil
}

func revertDropFlashbackPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "FLASHBACK DROP"
	revertStmt, ok := stmt.(*tree.RevertDropFlashback)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	dropHeader := sqlbase.ResultColumns{
		{Name: "flashback drop", Typ: types.Int},
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		txn := p.Txn()
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		row, err := p.ExecCfg().InternalExecutor.QueryRow(
			ctx,
			"flashback",
			p.Txn(),
			`SELECT object_name, drop_time FROM system.flashback WHERE (type = $1) AND (object_id = $2)`,
			revertStmt.Type,
			revertStmt.ObjectID,
		)
		if err != nil {
			if revertStmt.Type == tree.DatabaseFlashbackType {
				return errors.Wrapf(err, "failed to find database: %s record", revertStmt.DatabaseName.String())
			}
			return errors.Wrapf(err, "failed to find table: %s record", revertStmt.TableName.String())
		}
		if row == nil {
			if revertStmt.Type == tree.DatabaseFlashbackType {
				return pgerror.NewErrorf(pgcode.NoData,
					"Database[%s] have not enable flashback ", revertStmt.DatabaseName.String())
			}
			return pgerror.NewErrorf(pgcode.NoData,
				"Table[%s] have not enable flashback ", revertStmt.TableName.String())
		}
		var enterName string
		if revertStmt.Type == tree.DatabaseFlashbackType {
			enterName = "'" + revertStmt.DatabaseName.String() + "'"
		} else {
			enterName = "'" + revertStmt.TableName.TableName.String() + "'"
		}
		objName := *row[0].(*tree.DString)
		dropTime := *row[1].(*tree.DTimestamp)
		recordDropTime := dropTime
		// back to 0.1s before drop time
		dropTime.Time = dropTime.Time.Add(-100 * time.Millisecond)

		objID := sqlbase.ID(revertStmt.ObjectID)
		if objName.String() != enterName {
			if revertStmt.Type == tree.DatabaseFlashbackType {
				return errors.Errorf("The database name entered: %s does not match the database name: %s in the flashback record",
					revertStmt.DatabaseName.String(),
					objName.String())
			}
			return errors.Errorf("The table name entered: %s does not match the table name: %s in the flashback record",
				revertStmt.TableName.String(),
				objName.String())
		}
		if revertStmt.Type == tree.DatabaseFlashbackType {
			dbDescExit, _ := p.ResolveUncachedDatabaseByName(ctx, revertStmt.DatabaseName.String(), true /*required*/)
			if dbDescExit != nil {
				return errors.Errorf("DB: %s already exists", revertStmt.DatabaseName.String())
			}
			if err := revertDescName(ctx, p, dropTime.UnixNano(), objID, keys.RootNamespaceID, revertStmt.DatabaseName.String()); err != nil {
				return err
			}
			if err := p.Txn().Step(ctx); err != nil {
				return err
			}
			dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, txn, objID)
			if err != nil {
				return err
			}
			for _, schema := range dbDesc.GetSchemas() {
				if err := revertDescName(ctx, p, dropTime.UnixNano(), schema.ID, schema.ParentID, schema.Name); err != nil {
					return err
				}
			}

			// need to revert table descriptor? y
			var immutableTbls []sqlbase.ImmutableTableDescriptor
			// Query all tables
			tblDescs, externalTbl, err := allTablesOfDB(ctx, p.Txn(), dbDesc, false)
			if err != nil {
				return err
			}
			for _, tbl := range tblDescs {
				if open, err := queryTableEnableFlashbackWithDBDropTime(
					ctx,
					p.Txn(),
					p.ExtendedEvalContext().ExecCfg.InternalExecutor,
					tbl.GetID(),
					recordDropTime.Time,
				); !open {
					if err != nil {
						return err
					}
					continue
				}
				// Library level flashback to check whether external dependencies are in the same database
				ids, err := tbl.FindReferences()
				if err != nil {
					return err
				}
				for id := range ids {
					if _, ok := externalTbl[id]; ok {
						return errors.Errorf("Table[%s] have some depends on other DB ,can not flashback.", tbl.Name)
					}
				}
				immutableTbls = append(immutableTbls, sqlbase.ImmutableTableDescriptor{TableDescriptor: tbl})
			}
			numReverted := 0
			for _, immutable := range immutableTbls {
				if err := revertDescName(ctx, p, dropTime.UnixNano(), immutable.ID, immutable.ParentID, immutable.Name); err != nil {
					return err
				}
				// Clear table cache
				if err := p.ExecCfg().Gossip.AddInfo(MakeTableIDKey(immutable.ID), nil, 0); err != nil {
					return err
				}
				numReverted++
			}
			_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
				ctx,
				"update flashback record",
				txn,
				`UPDATE system.flashback SET ct_time = $1, drop_time = $2 WHERE (db_id = $3) AND (drop_time = $4)`,
				timeutil.Now(),
				time.Time{},
				objID,
				recordDropTime.Time,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to update database flashback record")
			}
			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(numReverted))}
		} else {
			if revertStmt.TableName.SchemaName == "" {
				revertStmt.TableName.CatalogName = tree.Name(p.SessionData().Database)
				revertStmt.TableName.SchemaName = tree.Name(p.SessionData().SearchPath.String())
			} else if revertStmt.TableName.CatalogName == "" {
				dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, p.SessionData().Database, true /*required*/)
				if err != nil {
					return err
				}
				if dbDesc.ExistSchema(revertStmt.TableName.SchemaName.String()) {
					revertStmt.TableName.CatalogName = tree.Name(p.SessionData().Database)
				} else {
					revertStmt.TableName.CatalogName = revertStmt.TableName.SchemaName
					revertStmt.TableName.SchemaName = tree.Name(p.SessionData().SearchPath.String())
				}
			}
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, revertStmt.TableName.CatalogName.String(), true /*required*/)
			if err != nil {
				return err
			}
			scDesc, err := dbDesc.GetSchemaByName(revertStmt.TableName.SchemaName.String())
			if err != nil {
				return err
			}
			if err := dump.CheckTableExists(ctx, p.Txn(), scDesc.ID, revertStmt.TableName.TableName.String()); err != nil {
				return err
			}
			tblDesc, err := sqlbase.GetTableDescFromID(ctx, p.Txn(), objID)
			if err != nil {
				return err
			}
			if scDesc.ID != tblDesc.ParentID {
				return errors.Errorf("No table record found")
			}
			if !tblDesc.Dropped() {
				return errors.Errorf("Table: %s with id: %d existing", tblDesc.GetName(), objID)
			}
			refs, err := tblDesc.FindReferences()
			if err != nil {
				return err
			}
			if len(refs) > 0 {
				tblDesc.AllIndexSpans()
				return errors.Errorf("Table[%s] have some constraints ,can not revert.", tblDesc.Name)
			}
			if err := revertDescName(ctx, p, dropTime.UnixNano(), objID, tblDesc.GetParentID(), tblDesc.GetName()); err != nil {
				return err
			}
			_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
				ctx,
				"update flashback record",
				txn,
				`UPDATE system.flashback SET ct_time = $1, drop_time = $2 WHERE (type = $3) AND (object_id = $4)`,
				timeutil.Now(),
				time.Time{},
				revertStmt.Type,
				objID,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to update table flashback record")
			}
			if err := p.ExecCfg().Gossip.AddInfo(MakeTableIDKey(objID), nil, 0); err != nil {
				return err
			}
			p.ExecCfg().StatsRefresher.NotifyMutation(objID, 1)
			resultsCh <- tree.Datums{tree.NewDInt(1)}
		}
		return nil
	}
	return fn, dropHeader, nil, false, nil
}

func queryTableEnableFlashbackWithDBDropTime(
	ctx context.Context,
	txn *client.Txn,
	interExec *sql.InternalExecutor,
	tblID sqlbase.ID,
	dropTime time.Time,
) (bool, error) {
	row, err := interExec.Query(
		ctx,
		"select table flashback record",
		txn,
		`SELECT * FROM system.flashback WHERE  (type = $1) AND (object_id = $2) AND (drop_time = $3)`,
		tree.TableFlashbackType,
		tblID,
		dropTime,
	)
	return row != nil, err
}

func showFlashbackPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "SHOW FLASHBACK ALL"
	_, ok := stmt.(*tree.ShowFlashback)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	showHeader := sqlbase.ResultColumns{
		{Name: "object_id", Typ: types.Int},
		{Name: "type", Typ: types.String},
		{Name: "object_name", Typ: types.String},
		{Name: "parent_id", Typ: types.Int},
		{Name: "db_id", Typ: types.Int},
		{Name: "ct_time", Typ: types.Timestamp},
		{Name: "ttl_days", Typ: types.Int},
		{Name: "status", Typ: types.String},
	}
	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		query := "SELECT object_id, type, object_name, parent_id, db_id, ct_time, ttl_days, drop_time FROM system.flashback ORDER BY object_id"
		rows, err := p.ExecCfg().InternalExecutor.Query(ctx, opName, p.Txn(), query)
		if err != nil {
			return err
		}
		for _, r := range rows {
			objectID := *r[0].(*tree.DInt)
			objType := *r[1].(*tree.DInt)
			ttlDays := *r[6].(*tree.DInt)
			dropTime := *r[7].(*tree.DTimestamp)
			//visible := *r[7].(*tree.DBool)
			dropped := dropTime.Sub(time.Time{}) != 0
			if dropped {
				r[7] = tree.NewDString("Dropped")
			} else {
				r[7] = tree.NewDString("Exist")
			}
			if objectID > keys.MaxReservedDescID { //&& visible {
				currTime := p.ExecCfg().Clock.Now()
				if objType == tree.DatabaseFlashbackType {
					r[1] = tree.NewDString("Database")
					if dropped && currTime.GoTime().Sub(dropTime.Time).Hours() >= float64(ttlDays*24) {
						if _, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
							ctx,
							"DELETE DB FLASHBACK RECORD",
							p.Txn(),
							`DELETE FROM system.flashback WHERE type=$1 and object_id=$2`,
							tree.DatabaseFlashbackType,
							objectID,
						); err != nil {
							return errors.Wrapf(err, "failed to delete db flashback record")
						}
						continue
					}
				} else if objType == tree.TableSnapshotType {
					r[1] = tree.NewDString("Table")
					if dropped && currTime.GoTime().Sub(dropTime.Time).Hours() >= float64(ttlDays*24) {
						if _, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
							ctx,
							"DELETE TABLE FLASHBACK RECORD",
							p.Txn(),
							`DELETE FROM system.flashback WHERE type=$1 and object_id=$2`,
							tree.TableFlashbackType,
							objectID,
						); err != nil {
							return errors.Wrapf(err, "failed to delete table flashback record")
						}
						continue
					}
				} else {
					r[1] = tree.NewDString("unknown type")
					r[2] = tree.NewDString("unknown object")
				}
				//r = r[:len(r)-1]
				resultsCh <- r
			}
		}
		return nil
	}
	return fn, showHeader, nil, false, nil
}

func alterDatabaseAbleFlashBackPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	opName := "ALTER DATABASE"
	alterStmt, ok := stmt.(*tree.AlterDatabaseFlashBack)
	if !ok {
		return nil, nil, nil, false, nil
	}
	if err := p.RequireAdminRole(ctx, opName); err != nil {
		return nil, nil, nil, false, err
	}
	alterHeader := sqlbase.ResultColumns{
		{Name: "ALTER DATABASE", Typ: types.Int},
	}
	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		var desc = &sqlbase.Descriptor{}
		dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(alterStmt.DatabaseName), true /*required*/)
		if err != nil {
			return err
		}
		desc.Union = &sqlbase.Descriptor_Database{Database: dbDesc}
		ctTime := hlc.NewClock(hlc.UnixNano, 0).Now()
		ttlDays := int8(alterStmt.TTLDays)
		dbID := desc.GetID()
		if err = sql.UpdateFlashback(
			ctx,
			p.Txn(),
			p.ExecCfg().InternalExecutor,
			dbID,
			keys.RootNamespaceID,
			dbID,
			desc.GetName(),
			ctTime.GoTime(),
			alterStmt.Able,
			ttlDays,
			true,
			tree.DatabaseFlashbackType,
		); err != nil {
			return err
		}
		tblDescs, _, err := allTablesOfDB(ctx, p.Txn(), desc.GetDatabase(), true)
		if err != nil {
			return err
		}
		rowsCount := 1
		for _, td := range tblDescs {
			if err := sql.UpdateFlashback(
				ctx,
				p.Txn(),
				p.ExecCfg().InternalExecutor,
				td.GetID(),
				td.ParentID,
				dbID,
				td.GetName(),
				ctTime.GoTime(),
				alterStmt.Able,
				ttlDays,
				false,
				tree.TableFlashbackType,
			); err != nil {
				return err
			}
			rowsCount++
			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(rowsCount + 1))}
		}

		return sql.UpdateDescNamespaceFlashbackRecord(
			ctx,
			p.Txn(),
			p.ExecCfg().InternalExecutor,
			ctTime.GoTime(),
		)
	}
	return fn, alterHeader, nil, false, nil
}
