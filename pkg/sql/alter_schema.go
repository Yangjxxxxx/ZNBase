package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

var errEmptySchemaName = pgerror.NewError(pgcode.Syntax, "empty Schema name")

type alterSchemaNode struct {
	n         *tree.AlterSchema
	dbDescOld *DatabaseDescriptor
	dbDescNew *DatabaseDescriptor
	idKeyOld  roachpb.Key
	idKeyNew  roachpb.Key
}

// AlterSchema applies a schema change on a schema.
// Privileges: CREATE on schema.
func (p *planner) AlterSchema(ctx context.Context, n *tree.AlterSchema) (planNode, error) {
	// Checking whether the user try to alter(or alter to) the invalid schema(internal schema,
	// blank schema or the unchanging schema)
	if isInternal := CheckInternalSchema(n.Schema); isInternal {
		return nil, fmt.Errorf("schema %q collides with builtin schema's name", n.Schema.Schema())
	}

	if isInternal := CheckInternalSchema(n.NewSchema); isInternal {
		return nil, fmt.Errorf("schema %q collides with builtin schema's name", n.NewSchema.Schema())
	}

	if err := sessiondata.IsSchemaNameValid(n.NewSchema.Schema()); err != nil {
		return nil, err
	}

	if n.Schema.Schema() == "" || n.NewSchema.Schema() == "" {
		return nil, errEmptySchemaName
	}

	if strings.HasPrefix(n.Schema.Schema(), sessiondata.PgTempSchemaName) || strings.HasPrefix(n.NewSchema.Schema(), sessiondata.PgTempSchemaName) {
		return nil, fmt.Errorf("cannot move objects into or out of temporary schemas")
	}

	var oldDbName string
	var newDbName string
	if n.Schema.IsExplicitCatalog() {
		oldDbName = n.Schema.Catalog()
	} else {
		oldDbName = p.CurrentDatabase()
	}
	if n.NewSchema.IsExplicitCatalog() {
		newDbName = n.NewSchema.Catalog()
	} else {
		newDbName = p.CurrentDatabase()
	}

	if newDbName == oldDbName && n.Schema.Schema() == n.NewSchema.Schema() {
		return newZeroNode(nil), nil
	}

	var dbDescOld, dbDescNew *DatabaseDescriptor
	var errs error
	if oldDbName == newDbName {
		dbDescOld, errs = getDatabaseDesc(ctx, p.txn, oldDbName)
		if errs != nil {
			return nil, errs
		}
		dbDescNew = dbDescOld
	} else {
		dbDescOld, errs = getDatabaseDesc(ctx, p.txn, oldDbName)
		if errs != nil {
			return nil, errs
		}
		dbDescNew, errs = getDatabaseDesc(ctx, p.txn, newDbName)
		if errs != nil {
			return nil, errs
		}
	}

	plainKeyOld := schemaKey{dbDescOld.ID, n.Schema.Schema(), ""}
	idKeyOld := plainKeyOld.Key()

	plainKeyNew := schemaKey{dbDescNew.ID, n.NewSchema.Schema(), ""}
	idKeyNew := plainKeyNew.Key()

	if existed, err := descExists(ctx, p.txn, idKeyOld); err != nil || !existed {
		if n.IfExists {
			return newZeroNode(nil), nil
		}
		return nil, sqlbase.SchemaNotExistsError(plainKeyOld.Name())
	}

	if existed, err := descExists(ctx, p.txn, idKeyNew); err == nil && existed {
		return nil, sqlbase.NewSchemaAlreadyExistsError(newDbName, plainKeyNew.Name())
	}

	return &alterSchemaNode{n: n, dbDescOld: dbDescOld, dbDescNew: dbDescNew, idKeyOld: idKeyOld, idKeyNew: idKeyNew}, nil
}

func (n *alterSchemaNode) startExec(params runParams) error {
	scName := n.n.Schema.Schema()
	p := params.p
	ctx := params.ctx
	scNames := make([]string, 0)
	scNames = append(scNames, scName)
	// Check if any views depend on tables in the schema. Because our views
	// are currently just stored as strings, they explicitly specify the schema
	// name. Rather than trying to rewrite them with the changed Schema name, we
	// simply disallow such renames for now.
	phyAccessor := p.PhysicalSchemaAccessor()
	lookupFlags := p.CommonLookupFlags(true /*required*/)
	// DDL statements bypass the cache.
	lookupFlags.avoidCached = true
	//Determine the location of the schema which needs to be altered
	toAlter := n.dbDescOld.LocateAlteredSchema(n.n)
	schemaDesc := n.dbDescOld.Schemas[toAlter]
	tbNames, err := GetObjectNames(ctx, p.txn, p, n.dbDescOld, scNames, true)
	if err != nil {
		return err
	}

	// check the drop privilege of old schema, the drop privilege of old tables,
	// and the create privilege of new database.
	if err := p.CheckPrivilege(ctx, &schemaDesc, privilege.DROP); err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, n.dbDescNew, privilege.CREATE); err != nil {
		return err
	}

	lookupFlags.required = false
	for i := range tbNames {
		objDesc, err := phyAccessor.GetObjectDesc(ctx, p.txn, &tbNames[i],
			ObjectLookupFlags{CommonLookupFlags: lookupFlags})
		if err != nil {
			return err
		}
		if objDesc == nil {
			continue
		}
		tbDesc := objDesc.TableDesc()
		// check the drop privilege of old tables.
		if err := p.CheckPrivilege(ctx, tbDesc, privilege.DROP); err != nil {
			return err
		}
		if len(tbDesc.DependedOnBy) > 0 {
			viewDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, tbDesc.DependedOnBy[0].ID)
			if err != nil {
				return err
			}
			viewName := viewDesc.Name
			if scName != tree.PublicSchema {
				viewName = fmt.Sprintf("%s.%s", scName, viewName)
			} else {
				var err error
				viewName, err = p.getQualifiedTableName(ctx, viewDesc)
				if err != nil {
					log.Warningf(ctx, "unable to retrieve fully-qualified name of view %d: %v",
						viewDesc.ID, err)
					msg := fmt.Sprintf("cannot rename schema because a view depends on table %q", tbDesc.Name)
					return sqlbase.NewDependentObjectError(msg)
				}
			}
			msg := fmt.Sprintf("cannot rename schema because view %s depends on table %q", viewName, tbDesc.Name)
			hint := fmt.Sprintf("you can drop %s instead.", viewName)
			return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
		}
	}

	b := &client.Batch{}
	schemaID := n.dbDescOld.Schemas[toAlter].GetID()
	b.Del(n.idKeyOld)
	b.Put(n.idKeyNew, schemaID)
	var schemaIndex int
	if n.dbDescOld.ID == n.dbDescNew.ID {
		n.dbDescOld.Schemas[toAlter].Name = n.n.NewSchema.Schema()
		p.Tables().addUncommittedSchema(n.n.Schema.Schema(), schemaID, n.dbDescOld.Name, n.dbDescOld.ID, dbDropped)
		p.Tables().addUncommittedSchema(n.n.NewSchema.Schema(), schemaID, n.dbDescOld.Name, n.dbDescOld.ID, dbCreated)

		// grant the user new schema privilege.
		schemaIndex = toAlter

		if err := params.p.UpdateDescriptor(params.ctx, b, &n.dbDescOld.Schemas[schemaIndex]); err != nil {
			return err
		}
		if err := params.p.UpdateDescriptor(params.ctx, b, n.dbDescOld); err != nil {
			return err
		}
	} else {
		schemaDesc.ParentID = n.dbDescNew.ID
		schemaDesc.Name = n.n.NewSchema.Schema()
		// append schema in new database
		n.dbDescNew.Schemas = append(n.dbDescNew.Schemas, schemaDesc)
		// delete schema in old database
		n.dbDescOld.Schemas = append(n.dbDescOld.Schemas[:toAlter], n.dbDescOld.Schemas[toAlter+1:]...)

		p.Tables().addUncommittedSchema(n.n.Schema.Schema(), schemaID, n.dbDescOld.Name, n.dbDescOld.ID, dbDropped)
		p.Tables().addUncommittedSchema(n.n.NewSchema.Schema(), schemaID, n.dbDescNew.Name, n.dbDescNew.ID, dbCreated)

		// update schema descriptor
		schemaIndex = len(n.dbDescNew.Schemas) - 1

		if err := params.p.UpdateDescriptor(params.ctx, b, &n.dbDescNew.Schemas[schemaIndex]); err != nil {
			return err
		}
		if err := params.p.UpdateDescriptor(params.ctx, b, n.dbDescOld); err != nil {
			return err
		}
		if err := params.p.UpdateDescriptor(params.ctx, b, n.dbDescNew); err != nil {
			return err
		}
	}
	return params.p.txn.Run(params.ctx, b)
}

func (*alterSchemaNode) Next(params runParams) (bool, error) { return false, nil }

func (*alterSchemaNode) Values() tree.Datums { return nil }

func (*alterSchemaNode) Close(ctx context.Context) {}
