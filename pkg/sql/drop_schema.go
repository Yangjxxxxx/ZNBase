package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type dropSchemaNode struct {
	n   *tree.DropSchema
	tdt []toDeleteTable
	tdf []toDeleteFunction
	// schemasMap: key is the databaseName and value is all schemasMap to drop in the database.
	schemasMap map[string]*toDropSchema
}

type toDropSchema struct {
	databaseDesc *DatabaseDescriptor
	schemasName  []string
	id           sqlbase.ID
}

func (p *planner) DropSchema(ctx context.Context, n *tree.DropSchema) (planNode, error) {
	schemas := make(map[string]*toDropSchema)

	for _, schema := range n.Schemas {
		scName := schema.Schema()
		var databaseName string

		// get database name
		if schema.IsExplicitCatalog() {
			databaseName = schema.Catalog()
		} else {
			databaseName = p.CurrentDatabase()
		}

		dbDesc, err := getDatabaseDesc(ctx, p.txn, databaseName)
		if err != nil {
			return nil, err
		}

		if isInternal := CheckInternalSchema(schema); isInternal {
			return nil, fmt.Errorf("schema %q can not be dropped which collides with builtin schema", scName)
		}
		if !dbDesc.ExistSchema(scName) {
			if !n.IfExists {
				return nil, fmt.Errorf("schema %q does not exist in database %q", scName, databaseName)
			}
			break
		}

		scDesc, err := dbDesc.GetSchemaByName(scName)
		if err != nil {
			return nil, err
		}
		// check the drop privilege of schema
		if err := p.CheckPrivilege(ctx, scDesc, privilege.DROP); err != nil {
			return nil, err
		}

		if value, ok := schemas[databaseName]; ok {
			value.schemasName = append(value.schemasName, scName)
		} else {
			addSchemaName := []string{scName}
			id, err := getSchemaID(ctx, p.txn, dbDesc.ID, scName, true)
			if err != nil {
				return nil, err
			}
			schemas[databaseName] = &toDropSchema{databaseDesc: dbDesc, schemasName: addSchemaName, id: id}
		}
	}
	// check the drop privilege of tables and the drop privilege of view.
	var (
		td  []toDeleteTable
		tdf []toDeleteFunction
		err error
	)
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		td, tdf, err = p.DoDropInMultiDatabaseWithBehavior(ctx, n.DropBehavior, DropSchema, schemas)
	})
	if err != nil {
		return nil, err
	}
	return &dropSchemaNode{n: n, tdt: td, tdf: tdf, schemasMap: schemas}, nil
}

func (n *dropSchemaNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p

	_, _, err := p.DoDrop(params,
		tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType), n.tdt, sqlbase.InvalidID)
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

	b := &client.Batch{}
	b.Del(p.dropSchemaInMultiDatabaseImpl(ctx, n.schemasMap)...)
	for _, value := range n.schemasMap {
		value.databaseDesc.RemoveSchemas(value.schemasName)
		if err := p.UpdateDescriptor(params.ctx, b, value.databaseDesc); err != nil {
			return err
		}
	}

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Log a Drop Schema event for this table. This is an auditable log event
	// and is recorded in the same transaction as the table descriptor
	// update.
	for _, value := range n.schemasMap {
		params.p.curPlan.auditInfo = &server.AuditInfo{
			EventTime: timeutil.Now(),
			EventType: string(EventLogDropSchema),
			TargetInfo: &server.TargetInfo{
				TargetID: int32(value.id),
				Desc: struct {
					DatabaseName string
					SchemaName   []string
				}{
					value.databaseDesc.Name,
					value.schemasName,
				},
			},
			Info: "User name:" + params.SessionData().User + " SHOW SYNTAX:" + params.p.stmt.SQL,
		}
	}
	return nil
}

func (*dropSchemaNode) Next(params runParams) (bool, error) { return false, nil }

func (*dropSchemaNode) Values() tree.Datums { return nil }

func (*dropSchemaNode) Close(ctx context.Context) {}
