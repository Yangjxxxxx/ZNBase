package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type createSchemaNode struct {
	n      *tree.CreateSchema
	dbDesc *DatabaseDescriptor
}

// CreateSchema creates a schema
// Privileges: CREATE on database.
func (p *planner) CreateSchema(ctx context.Context, n *tree.CreateSchema) (planNode, error) {
	var databaseName string
	if n.Schema.IsExplicitCatalog() {
		databaseName = n.Schema.Catalog()
	} else {
		databaseName = p.CurrentDatabase()
	}

	dbDesc, err := getDatabaseDesc(ctx, p.txn, databaseName)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if isInternal := CheckInternalSchema(n.Schema); isInternal {
		return nil, fmt.Errorf("schema %q collides with builtin schema's name", n.Schema.Schema())
	}

	return &createSchemaNode{n: n, dbDesc: dbDesc}, nil
}

func (n *createSchemaNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p

	if err := sessiondata.IsSchemaNameValid(n.n.Schema.Schema()); err != nil {
		return err
	}
	desc := sqlbase.SchemaDescriptor{
		Name:       n.n.Schema.Schema(),
		Privileges: sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Schema, p.User()),
		ParentID:   n.dbDesc.ID,
	}

	created, err := p.createSchema(ctx, &desc, n.n.IfNotExists)
	if err != nil {
		return err
	}
	if created {
		b := &client.Batch{}

		n.dbDesc.Schemas = append(n.dbDesc.Schemas, desc)
		if err := p.UpdateDescriptor(ctx, b, n.dbDesc); err != nil {
			return err
		}

		if err := p.txn.Run(ctx, b); err != nil {
			return err
		}
		// Log Create Schema event. This is an auditable log event and is
		// recorded in the same transaction as the schema descriptor update.
		params.p.curPlan.auditInfo = &server.AuditInfo{
			EventTime: timeutil.Now(),
			EventType: string(EventLogCreateSchema),
			TargetInfo: &server.TargetInfo{
				TargetID: int32(desc.ID),
				Desc: struct {
					SchemaName string
				}{
					n.n.Schema.String(),
				},
			},
			Info: "User name:" + params.SessionData().User + " SHOW SYNTAX:" + params.p.stmt.SQL,
		}
		// schema 修改了database的信息
		params.extendedEvalCtx.Tables.addUncommittedSchema(desc.Name, desc.ID, n.dbDesc.Name, desc.ParentID, dbCreated)
	}
	return nil
}

func (*createSchemaNode) Next(params runParams) (bool, error) { return false, nil }

func (*createSchemaNode) Values() tree.Datums { return nil }

func (*createSchemaNode) Close(ctx context.Context) {}
