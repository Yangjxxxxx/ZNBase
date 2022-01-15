package sql

import (
	"context"
	"reflect"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/optbuilder"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

type opaqueMetadata struct {
	info    string
	plan    planNode
	columns sqlbase.ResultColumns
}

var _ opt.OpaqueMetadata = &opaqueMetadata{}

func (o *opaqueMetadata) ImplementsOpaqueMetadata()      {}
func (o *opaqueMetadata) String() string                 { return o.info }
func (o *opaqueMetadata) Columns() sqlbase.ResultColumns { return o.columns }

func buildOpaque(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	stmt tree.Statement,
	desiredTypes []types.T,
) (opt.OpaqueMetadata, error) {
	p := evalCtx.Planner.(*planner)

	// Opaque statements handle their own scalar arguments, with no help from the
	// optimizer. As such, they cannot contain subqueries.
	scalarProps := &semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	scalarProps.Require(stmt.StatementTag(), tree.RejectSubqueries)

	var plan planNode
	var err error
	switch n := stmt.(type) {
	case *tree.AlterIndex:
		plan, err = p.AlterIndex(ctx, n)
	case *tree.AlterTable:
		plan, err = p.AlterTable(ctx, n)
	case *tree.AlterSequence:
		plan, err = p.AlterSequence(ctx, n)
	case *tree.AlterUser:
		plan, err = p.AlterUser(ctx, n)
	case *tree.AlterSchema:
		plan, err = p.AlterSchema(ctx, n)
	case *tree.AlterUserDefaultSchma:
		plan, err = p.AlterUserDefaultSchma(ctx, n)
	case *tree.AlterViewAS:
		plan, err = p.AlterView(ctx, n)
	case *tree.CommentOnColumn:
		plan, err = p.CommentOnColumn(ctx, n)
	case *tree.CommentOnDatabase:
		plan, err = p.CommentOnDatabase(ctx, n)
	case *tree.CommentOnTable:
		plan, err = p.CommentOnTable(ctx, n)
	case *tree.CommentOnIndex:
		plan, err = p.CommentOnIndex(ctx, n)
	case *tree.CommentOnConstraint:
		plan, err = p.CommentOnConstraint(ctx, n)
	case *tree.Scrub:
		plan, err = p.Scrub(ctx, n)
	case *tree.CreateDatabase:
		plan, err = p.CreateDatabase(ctx, n)
	case *tree.CreateIndex:
		plan, err = p.CreateIndex(ctx, n)
	case *tree.CreateFunction:
		plan, err = p.CreateFunction(ctx, n)
	case *tree.CreateTable:
		plan, err = p.CreateTable(ctx, n)
	case *tree.CreateUser:
		plan, err = p.CreateUser(ctx, n)
	case *tree.CreateSequence:
		plan, err = p.CreateSequence(ctx, n)
	case *tree.CreateStats:
		plan, err = p.CreateStatistics(ctx, n)
	case *tree.CreateSchema:
		plan, err = p.CreateSchema(ctx, n)
	case *tree.Deallocate:
		plan, err = p.Deallocate(ctx, n)
	case *tree.DeclareVariable:
		plan, err = p.DeclareVariable(ctx, n)
	case *tree.VariableAccess:
		plan, err = p.VariableAccess(ctx, n)
	case *tree.DeclareCursor:
		plan, err = p.DeclareCursor(ctx, n)
	case *tree.CursorAccess:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.Discard:
		plan, err = p.Discard(ctx, n)
	case *tree.DropDatabase:
		plan, err = p.DropDatabase(ctx, n)
	case *tree.DropIndex:
		plan, err = p.DropIndex(ctx, n)
	case *tree.DropTable:
		plan, err = p.DropTable(ctx, n)
	case *tree.DropView:
		plan, err = p.DropView(ctx, n)
	case *tree.DropSequence:
		plan, err = p.DropSequence(ctx, n)
	case *tree.DropUser:
		plan, err = p.DropUser(ctx, n)
	case *tree.DropSchema:
		plan, err = p.DropSchema(ctx, n)
	case *tree.Explain:
		plan, err = p.Explain(ctx, n)
	case *tree.Grant:
		plan, err = p.Grant(ctx, n)
	case *tree.Revoke:
		plan, err = p.Revoke(ctx, n)
	case *tree.Insert:
		plan, err = p.Insert(ctx, n, desiredTypes)
	case *tree.RefreshMaterializedView:
		plan, err = p.RefreshMaterializedView(ctx, n)
	case *tree.RenameColumn:
		plan, err = p.RenameColumn(ctx, n)
	case *tree.RenameDatabase:
		plan, err = p.RenameDatabase(ctx, n)
	case *tree.RenameIndex:
		plan, err = p.RenameIndex(ctx, n)
	case *tree.RenameTable:
		plan, err = p.RenameTable(ctx, n)
	case *tree.Scatter:
		plan, err = p.Scatter(ctx, n)
	case *tree.SetClusterSetting:
		plan, err = p.SetClusterSetting(ctx, n)
	case *tree.SetReplica:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.SetZoneConfig:
		plan, err = p.SetZoneConfig(ctx, n)
	case *tree.SetVar:
		plan, err = p.SetVar(ctx, n)
	case *tree.SetTransaction:
		plan, err = p.SetTransaction(n)
	case *tree.SetSessionCharacteristics:
		plan, err = p.SetSessionCharacteristics(n)
	case *tree.ShowCursor:
		plan, err = p.ShowCursors(ctx, n)
	case *tree.ShowClusterSetting:
		plan, err = p.ShowClusterSetting(ctx, n)
	case *tree.ControlSchedules:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.ShowVar:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.ShowGrants:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.ShowTables:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.ShowHistogram:
		plan, err = p.ShowHistogram(ctx, n)
	case *tree.ShowTableStats:
		plan, err = p.ShowTableStats(ctx, n)
	case *tree.ShowTraceForSession:
		plan, err = p.ShowTrace(ctx, n)
	case *tree.ShowZoneConfig:
		plan, err = p.ShowZoneConfig(ctx, n)
	case *tree.ShowRanges:
		plan, err = p.ShowRanges(ctx, n)
	case *tree.ShowFingerprints:
		plan, err = p.ShowFingerprints(ctx, n)
	case *tree.Truncate:
		plan, err = p.Truncate(ctx, n)
	case *tree.UnionClause:
		plan, err = p.Union(ctx, n, desiredTypes)
	case *tree.ValuesClause:
		plan, err = p.Values(ctx, n, desiredTypes)
	case *tree.ValuesClauseWithNames:
		plan, err = p.Values(ctx, n, desiredTypes)
	case *tree.Export:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.Dump:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.ShowFlashback:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.RevertFlashback, *tree.RevertDropFlashback:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.AlterDatabaseFlashBack:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.ClearIntent:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case tree.CCLOnlyStatement:
		plan, err = p.maybePlanHook(ctx, stmt)
		if plan == nil && err == nil {
			return nil, pgerror.NewErrorf(pgcode.CCLRequired,
				"a CCL binary is required to use this statement type: %T", stmt)
		}
	case *tree.ShowTriggers, *tree.ShowTriggersOnTable, *tree.ShowCreateTrigger:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.ShowFunctions:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	case *tree.QueryLockstat:
		return nil, optbuilder.TryOldPath{ERR: errors.Errorf("try old path")}
	default:
		plan, err = nil, errors.AssertionFailedf("unknown opaque statement %T", stmt)
	}
	if err != nil {
		return nil, err
	}
	res := &opaqueMetadata{
		info:    stmt.StatementTag(),
		plan:    plan,
		columns: planColumns(plan),
	}
	return res, nil
}

func init() {
	for _, stmt := range []tree.Statement{
		&tree.AlterIndex{},
		&tree.AlterTable{},
		&tree.AlterSequence{},
		&tree.AlterUser{},
		&tree.AlterSchema{},
		&tree.AlterUserDefaultSchma{},
		&tree.AlterViewAS{},
		&tree.CommentOnColumn{},
		&tree.CommentOnDatabase{},
		&tree.CommentOnTable{},
		&tree.CommentOnIndex{},
		&tree.CommentOnConstraint{},
		&tree.Scrub{},
		&tree.CallStmt{},
		&tree.CreateDatabase{},
		&tree.CreateIndex{},
		&tree.CreateFunction{},
		&tree.CreateTable{},
		&tree.CreateTrigger{},
		&tree.CreateUser{},
		&tree.CreateSequence{},
		&tree.CreateStats{},
		&tree.CreateSchema{},
		&tree.Deallocate{},
		&tree.DeclareVariable{},
		&tree.VariableAccess{},
		&tree.DeclareCursor{},
		&tree.CursorAccess{},
		&tree.ControlSchedules{},
		&tree.ShowVar{},
		&tree.ShowTables{},
		&tree.ShowFlashback{},
		&tree.Discard{},
		&tree.DropDatabase{},
		&tree.DropIndex{},
		&tree.DropFunction{},
		&tree.DropTable{},
		&tree.DropTrigger{},
		&tree.DropView{},
		&tree.DropSequence{},
		&tree.DropUser{},
		&tree.DropSchema{},
		&tree.Explain{},
		&tree.Grant{},
		&tree.Insert{Returning: &tree.ReturningNothing{}},
		&tree.RefreshMaterializedView{},
		&tree.RenameColumn{},
		&tree.RenameDatabase{},
		&tree.RenameIndex{},
		&tree.RenameFunction{},
		&tree.RenameTable{},
		&tree.RenameTrigger{},
		&tree.Revoke{},
		&tree.Scatter{},
		&tree.SetClusterSetting{},
		&tree.SetReplica{},
		&tree.SetZoneConfig{},
		&tree.SetVar{},
		&tree.SetTransaction{},
		&tree.SetSessionCharacteristics{},
		&tree.ShowCursor{},
		&tree.ShowClusterSetting{},
		&tree.ShowGrants{},
		&tree.ShowHistogram{},
		&tree.ShowTableStats{},
		&tree.ShowTraceForSession{},
		&tree.ShowZoneConfig{},
		&tree.ShowRanges{},
		&tree.ShowFunctions{},
		&tree.ShowCreateTrigger{},
		&tree.ShowTriggers{},
		&tree.ShowTriggersOnTable{},
		&tree.ShowFingerprints{},
		&tree.Truncate{},
		&tree.UnionClause{},
		&tree.ValuesClause{},
		&tree.ValuesClauseWithNames{},
		&tree.RevertFlashback{},
		&tree.RevertDropFlashback{},
		&tree.AlterDatabaseFlashBack{},
		&tree.ClearIntent{},

		//CCLOnlyStatement
		&tree.Backup{},
		&tree.ShowBackup{},
		&tree.ShowKafka{},
		&tree.CreateChangefeed{},
		&tree.Export{},
		&tree.Dump{},
		&tree.LoadRestore{},
		&tree.LoadImport{},
		&tree.CreateSnapshot{},
		&tree.DropSnapshot{},
		&tree.ShowSnapshot{},
		&tree.RevertSnapshot{},
		&tree.CreateRole{},
		&tree.DropRole{},
		&tree.QueryLockstat{},
		&tree.GrantRole{},
		&tree.RevokeRole{},
		&tree.ScheduledBackup{},
	} {
		typ := optbuilder.OpaqueReadOnly
		if tree.CanModifySchema(stmt) {
			typ = optbuilder.OpaqueDDL
		} else if tree.CanWriteData(stmt) {
			typ = optbuilder.OpaqueMutation
		}
		optbuilder.RegisterOpaque(reflect.TypeOf(stmt), typ, buildOpaque)
	}
}
