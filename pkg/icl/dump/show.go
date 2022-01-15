package dump

import (
	"context"
	"time"

	"github.com/znbasedb/znbase/pkg/icl/utilicl"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// showBackupPlanHook implements PlanHookFn.
func showBackupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	backup, ok := stmt.(*tree.ShowBackup)
	if !ok {
		return nil, nil, nil, false, nil
	}

	if err := utilicl.CheckCommercialFeatureEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "SHOW BACKUP",
	); err != nil {
		return nil, nil, nil, false, err
	}

	if err := p.RequireAdminRole(ctx, "SHOW BACKUP"); err != nil {
		return nil, nil, nil, false, err
	}

	toFn, err := p.TypeAsString(backup.Path, "SHOW BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	var shower backupShower
	switch backup.Details {
	case tree.BackupRangeDetails:
		shower = backupShowerRanges
	case tree.BackupFileDetails:
		shower = backupShowerFiles
	default:
		shower = backupShowerDefault
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		str, err := toFn()
		if err != nil {
			return err
		}
		desc, err := ReadDumpDescriptorFromURI(ctx, str, p.ExecCfg().DistSQLSrv.DumpSinkFromURI, nil, "")
		if err != nil {
			return err
		}

		for _, row := range shower.fn(desc) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case resultsCh <- row:
			}
		}
		return nil
	}

	return fn, shower.header, nil, false, nil
}

type backupShower struct {
	header sqlbase.ResultColumns
	fn     func(DumpDescriptor) []tree.Datums
}

var backupShowerDefault = backupShower{
	header: sqlbase.ResultColumns{
		{Name: "database_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "start_time", Typ: types.Timestamp},
		{Name: "end_time", Typ: types.Timestamp},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
	},

	fn: func(desc DumpDescriptor) []tree.Datums {
		descs := make(map[sqlbase.ID]string)
		for _, descriptor := range desc.Descriptors {
			if database := descriptor.GetDatabase(); database != nil {
				if _, ok := descs[database.ID]; !ok {
					descs[database.ID] = database.Name
				}
			}
		}
		descSizes := make(map[sqlbase.ID]roachpb.BulkOpSummary)
		for _, file := range desc.Files {
			// TODO(dan): This assumes each file in the backup only contains
			// data from a single table, which is usually but not always
			// correct. It does not account for interleaved tables or if a
			// BACKUP happened to catch a newly created table that hadn't yet
			// been split into its own range.
			_, tableID, err := encoding.DecodeUvarintAscending(file.Span.Key)
			if err != nil {
				continue
			}
			s := descSizes[sqlbase.ID(tableID)]
			s.Add(file.EntryCounts)
			descSizes[sqlbase.ID(tableID)] = s
		}
		start := tree.DNull
		if desc.StartTime.WallTime != 0 {
			start = tree.MakeDTimestamp(timeutil.Unix(0, desc.StartTime.WallTime), time.Nanosecond)
		}
		var rows []tree.Datums
		for _, descriptor := range desc.Descriptors {

			//if table := descriptor.GetTable(); table != nil {
			if table := descriptor.Table(hlc.Timestamp{}); table != nil {
				dbName := descs[table.ParentID]
				rows = append(rows, tree.Datums{
					tree.NewDString(dbName),
					tree.NewDString(table.Name),
					start,
					tree.MakeDTimestamp(timeutil.Unix(0, desc.EndTime.WallTime), time.Nanosecond),
					tree.NewDInt(tree.DInt(descSizes[table.ID].DataSize)),
					tree.NewDInt(tree.DInt(descSizes[table.ID].Rows)),
				})
			}
		}
		return rows
	},
}

var backupShowerRanges = backupShower{
	header: sqlbase.ResultColumns{
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_pretty", Typ: types.String},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "end_key", Typ: types.Bytes},
	},

	fn: func(desc DumpDescriptor) (rows []tree.Datums) {
		for _, span := range desc.Spans {
			rows = append(rows, tree.Datums{
				tree.NewDString(span.Key.String()),
				tree.NewDString(span.EndKey.String()),
				tree.NewDBytes(tree.DBytes(span.Key)),
				tree.NewDBytes(tree.DBytes(span.EndKey)),
			})
		}
		return rows
	},
}

var backupShowerFiles = backupShower{
	header: sqlbase.ResultColumns{
		{Name: "path", Typ: types.String},
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_pretty", Typ: types.String},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "end_key", Typ: types.Bytes},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
	},

	fn: func(desc DumpDescriptor) (rows []tree.Datums) {
		for _, file := range desc.Files {
			rows = append(rows, tree.Datums{
				tree.NewDString(file.Path),
				tree.NewDString(file.Span.Key.String()),
				tree.NewDString(file.Span.EndKey.String()),
				tree.NewDBytes(tree.DBytes(file.Span.Key)),
				tree.NewDBytes(tree.DBytes(file.Span.EndKey)),
				tree.NewDInt(tree.DInt(file.EntryCounts.DataSize)),
				tree.NewDInt(tree.DInt(file.EntryCounts.Rows)),
			})
		}
		return rows
	},
}

func init() {
	sql.AddPlanHook(showBackupPlanHook)
}
