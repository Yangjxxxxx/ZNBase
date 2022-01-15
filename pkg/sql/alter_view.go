package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type alterViewNode struct {
	n             *tree.AlterViewAS
	dbDesc        *sqlbase.DatabaseDescriptor
	tableDesc     *MutableTableDescriptor
	sourceColumns sqlbase.ResultColumns
	// planDeps tracks which tables and views the view being created
	// depends on. This is collected during the construction of
	// the view query's logical plan.
	planDeps planDependencies
}

func (p *planner) AlterView(ctx context.Context, n *tree.AlterViewAS) (planNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Name, !n.IfExists, requireViewDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, fmt.Errorf("table '%s' does not exist", string(n.Name.TableName))
	}
	if tableDesc.IsMaterializedView {
		return nil, fmt.Errorf("cannot modify materialized views")
	}

	for _, ref := range tableDesc.DependedOnBy {
		err := p.getViewDescForAlterView(ctx, "view", tableDesc.Name, tableDesc.ParentID, ref.ID)
		if err != nil {
			return nil, err
		}
	}

	var parentDesc sqlbase.DescriptorProto
	dbDesc, err := p.ResolveUncachedDatabase(ctx, &n.Name)
	if err != nil {
		return nil, err
	}
	parentDesc = dbDesc
	scName := string(n.Name.SchemaName)
	if dbDesc.ID != keys.SystemDatabaseID {
		if isInternal := CheckVirtualSchema(scName); isInternal {
			return nil, fmt.Errorf("schema cannot be modified: %q", n.Name.SchemaName.String())
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

	p.skipSelectPrivilegeChecks = true
	defer func() { p.skipSelectPrivilegeChecks = false }()

	var planDeps planDependencies
	var sourceColumns sqlbase.ResultColumns
	var hasStar bool
	// To avoid races with ongoing schema changes to tables that the view
	// depends on, make sure we use the most recent versions of table
	// descriptors rather than the copies in the lease cache.
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		planDeps, sourceColumns, hasStar, err = p.analyzeViewQuery(ctx, n.AsSource)
	})
	if err != nil {
		return nil, err
	}
	if hasStar {
		return nil, fmt.Errorf("alter view do not currently support * expressions")
	}

	// Ensure that all the table names pretty-print as fully qualified,
	// so we store that in the view descriptor.
	//
	// The traversal will update the TableNames in-place, so the changes are
	// persisted in n.AsSource. We exploit the fact that semantic analysis above
	// has populated any missing db/schema details in the table names in-place.
	// We use tree.FormatNode merely as a traversal method; its output buffer is
	// discarded immediately after the traversal because it is not needed further.
	{
		f := tree.NewFmtCtx(tree.FmtParsable)
		f.SetReformatTableNames(
			func(_ *tree.FmtCtx, tn *tree.TableName) {
				// Persist the database prefix expansion.
				if tn.SchemaName != "" {
					// All CTE or table aliases have no schema
					// information. Those do not turn into explicit.
					tn.ExplicitSchema = true
					tn.ExplicitCatalog = true
				}
			},
		)
		f.FormatNode(n.AsSource)
		f.Close() // We don't need the string.
	}

	return &alterViewNode{n: n,
		dbDesc:        dbDesc,
		tableDesc:     tableDesc,
		sourceColumns: sourceColumns,
		planDeps:      planDeps,
	}, nil
}

func (n *alterViewNode) startExec(params runParams) error {
	p := params.p
	txn := params.p.txn
	ctx := params.ctx
	//drop := fmt.Sprintf("drop view %s",n.tableDesc.Name)
	//_, err := params.p.extendedEvalCtx.InternalExecutor.(*SessionBoundInternalExecutor).Exec(ctx, "drop", txn, drop)
	//if err != nil {
	//	return err
	//}
	_, privSrc, err := params.p.dropViewImpl1(ctx, n.tableDesc, tree.DropDefault)
	if err != nil {
		return err
	}
	_, nameKey, _ := GetKeysForTableDescriptor(n.tableDesc.TableDesc())
	b := &client.Batch{}
	// Use CPut because we want to remove a specific name -> id map.
	b.CPut(nameKey, nil, n.tableDesc.ID)
	if err := txn.Run(ctx, b); err != nil {
		return err
	}
	// 需要drop生效
	if err := txn.Step(ctx); err != nil {
		return err
	}
	viewName := n.n.Name.Table()
	tKey := tableKey{parentID: n.dbDesc.GetSchemaID(n.n.Name.Schema()), name: viewName}
	key := tKey.Key()
	if exists, err := descExists(params.ctx, params.p.txn, key); err == nil && exists {
		// TODO(a-robinson): Support CREATE OR REPLACE commands.
		return sqlbase.NewRelationAlreadyExistsError(tKey.Name())
	} else if err != nil {
		return err
	}

	id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return err
	}

	// Inherit permissions from the schema descriptor.
	privs := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, privSrc.Owner)
	//privs := privSrc
	updatable, baseID, columnsReflect, _ := analyzeViewUpdatable(p, n.planDeps, n.n.AsSource, viewAlter, nil, false)

	desc, err := n.makeViewTableDesc(
		params,
		viewName,
		tKey.parentID,
		id,
		n.sourceColumns,
		updatable,
		baseID,
		privs,
		columnsReflect,
	)
	if err != nil {
		return err
	}

	*desc.Privileges = *privSrc

	// Collect all the tables/views this view depends on.
	for backrefID := range n.planDeps {
		desc.DependsOn = append(desc.DependsOn, backrefID)
	}

	if err = params.p.createDescriptorWithID(
		params.ctx, key, id, &desc, params.EvalContext().Settings); err != nil {
		return err
	}

	// Persist the back-references in all referenced table descriptors.
	for _, updated := range n.planDeps {
		backrefID := updated.desc.ID
		backRefMutable := params.p.Tables().getUncommittedTableByID(backrefID).MutableTableDescriptor
		if backRefMutable == nil {
			backRefMutable = sqlbase.NewMutableExistingTableDescriptor(*updated.desc.TableDesc())
		}
		for _, dep := range updated.deps {
			// The logical plan constructor merely registered the dependencies.
			// It did not populate the "ID" field of TableDescriptor_Reference,
			// because the ID of the newly created view descriptor was not
			// yet known.
			// We need to do it here.
			dep.ID = desc.ID
			backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
		}
		if err := params.p.writeSchemaChange(params.ctx, backRefMutable, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}

	if err := desc.RefreshValidate(params.ctx, params.p.txn, params.EvalContext().Settings); err != nil {
		return err
	}

	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	tn := tree.MakeTableName(tree.Name(n.dbDesc.Name), tree.Name(viewName))
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogCreateView),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(desc.ID),
			Desc: struct {
				ViewName string
			}{
				tn.FQString(),
			},
		},
		Info: &infos.CreateViewInfo{
			ViewName:  tn.FQString(),
			Statement: tree.AsStringWithFlags(n.n.AsSource, tree.FmtParsable),
			User:      params.SessionData().User,
		},
	}

	return nil
}

func (*alterViewNode) Next(runParams) (bool, error) { return false, nil }
func (*alterViewNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterViewNode) Close(context.Context)        {}

func (n *alterViewNode) makeViewTableDesc(
	params runParams,
	viewName string,
	parentID sqlbase.ID,
	id sqlbase.ID,
	resultColumns []sqlbase.ResultColumn,
	updatable bool,
	baseID sqlbase.ID,
	privileges *sqlbase.PrivilegeDescriptor,
	columnsReflect []string,
) (sqlbase.MutableTableDescriptor, error) {
	desc := InitTableDescriptor(id, parentID, viewName,
		params.p.txn.CommitTimestamp(), privileges, false)
	desc.ViewUpdatable = updatable
	desc.ViewDependsOn = uint32(baseID)
	desc.ViewQuery = tree.AsStringWithFlags(n.n.AsSource, tree.FmtParsable)

	for i, colRes := range resultColumns {
		colType, err := coltypes.DatumTypeToColumnType(colRes.Typ)
		if err != nil {
			return desc, err
		}
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colType}
		// The new types in the CREATE VIEW column specs never use
		// SERIAL so we need not process SERIAL types here.
		privs := sqlbase.InheritFromTablePrivileges(desc.Privileges)
		col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, &params.p.semaCtx, privs, id)
		if colRes.VisibleTypeName != "" && col != nil {
			col.Type.VisibleTypeName = colRes.VisibleTypeName
		}
		if updatable && col != nil {
			col.UpdatableViewDependsColName = columnsReflect[i]
		}
		if err != nil {
			return desc, err
		}
		desc.AddColumn(*col)
	}
	// AllocateIDs mutates its receiver. `return tableDesc, tableDesc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := desc.AllocateIDs()
	return desc, err
}
