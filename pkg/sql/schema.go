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
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/opt/optbuilder"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// schemaKey implements sqlbase.DescriptorKey.
type schemaKey namespaceKey

func (sk schemaKey) Key() roachpb.Key {
	return sqlbase.MakeNameMetadataKey(sk.parentID, sk.name)
}

func (sk schemaKey) Name() string {
	return sk.name
}

// disable means schemaCache is too old
func (sc *schemaCache) disable() {
	// use system set schema cache disable
	sc.schemas.Store("system", sqlbase.InvalidID)
}

// isValid check schemaCache available
func (sc *schemaCache) isValid() bool {
	// use system check schema cache available }
	val, ok := sc.schemas.Load("system")
	return !ok || val == sqlbase.SystemDB.ID
}

// CheckInternalSchema checks whether the schema is 'public', 'information_schema',
// 'zbdb_internal' or 'pg_catalog'
func CheckInternalSchema(name tree.SchemaName) bool {
	sc := name.Schema()
	return sc == pgCatalogName || sc == informationSchemaName || sc == znbaseInternalName || sc == sqlbase.SystemDB.Name
}

// CheckVirtualSchema checks whether the schema is 'pg_catalog', 'zbdb_internal' or 'information_schema'
func CheckVirtualSchema(sc string) bool {
	return sc == pgCatalogName || sc == informationSchemaName || sc == znbaseInternalName || sc == sqlbase.SystemDB.Name
}

// makeSchemaDesc construct a schema descriptor for test purpose.
func makeSchemaDesc(p *tree.CreateSchema, user string) sqlbase.SchemaDescriptor {
	return sqlbase.SchemaDescriptor{
		Name:       p.Schema.Schema(),
		Privileges: sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Database, user),
	}
}

func getDatabaseDesc(
	ctx context.Context, txn *client.Txn, databaseName string,
) (*DatabaseDescriptor, error) {
	id, err := getDatabaseID(ctx, txn, databaseName, true)
	if err != nil {
		return nil, err
	}

	desc, err := MustGetDatabaseDescByID(ctx, txn, id)
	if err != nil {
		return nil, err
	}

	return desc, nil
}

// createSchema takes Schema descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createSchema implements the SchemaDescEditor interface.
func (p *planner) createSchema(
	ctx context.Context, desc *SchemaDescriptor, ifNotExists bool,
) (bool, error) {
	plainKey := schemaKey{parentID: desc.ParentID, name: desc.Name}
	idKey := plainKey.Key()

	if exists, err := descExists(ctx, p.txn, idKey); err == nil && exists {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		databaseDesc, _ := getDatabaseDescByID(ctx, p.txn, desc.ParentID)
		return false, sqlbase.NewSchemaAlreadyExistsError(databaseDesc.Name, plainKey.Name())
	} else if err != nil {
		return false, err
	}

	id, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return false, err
	}

	return true, p.createDescriptorWithID(ctx, idKey, id, desc, nil)
}

func (p *planner) UpdateDescriptor(
	ctx context.Context, b *client.Batch, desc sqlbase.DescriptorProto,
) error {
	descKey := sqlbase.MakeDescMetadataKey(desc.GetID())
	descDesc := sqlbase.WrapDescriptor(desc)
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
	}
	b.Put(descKey, descDesc)

	return nil
}

type droppedType string

// Drop schema or database
const (
	DropSchema   droppedType = "schema"
	DropDatabase droppedType = "database"
)

func (p *planner) DoDropWithBehavior(
	ctx context.Context,
	DropBehavior tree.DropBehavior,
	dt droppedType,
	dbDesc *DatabaseDescriptor,
	schemas []string,
) ([]toDeleteTable, []toDeleteFunction, error) {
	tbNames, err := GetObjectNames(ctx, p.txn, p, dbDesc, schemas, true)
	if err != nil {
		return nil, nil, err
	}

	if len(tbNames) > 0 {
		var typeContent string
		switch dt {
		case DropSchema:
			typeContent = string(tbNames[0].SchemaName)
		case DropDatabase:
			typeContent = string(tbNames[0].CatalogName)
		}

		switch DropBehavior {
		case tree.DropRestrict:
			return nil, nil, pgerror.NewErrorf(pgcode.DependentObjectsStillExist,
				" %s %q is not empty and RESTRICT was specified", dt, typeContent)
		case tree.DropDefault:
			// The default is CASCADE, however be cautious if CASCADE was
			// not specified explicitly.
			if p.SessionData().SafeUpdates {
				return nil, nil, pgerror.NewDangerousStatementErrorf(
					"DROP %s on non-empty %s without explicit CASCADE", strings.ToUpper(string(dt)), dt)
			}
		}
	}

	td := make([]toDeleteTable, 0, len(tbNames))
	tdf := make([]toDeleteFunction, 0, len(tbNames))
	for i := range tbNames {
		// check the drop privilege of table in the "prepareDrop" function.
		// when drop database, using lookupObject to get tbDesc to avoid limitation of temporary schema
		var tbDesc *sqlbase.MutableTableDescriptor
		var err error
		if dt == DropDatabase {
			found, desc, err := p.LookupObject(
				ctx,
				true,
				tbNames[i].ResolvedAsFunction,
				tbNames[i].Catalog(),
				tbNames[i].Schema(),
				tbNames[i].Table(),
			)
			if err != nil {
				return td, nil, err
			}

			if found {
				var ok bool
				tbDesc, ok = desc.(*sqlbase.MutableTableDescriptor)
				if !ok {
					return nil, nil, pgerror.NewErrorf(pgcode.Syntax, "descriptor for %q is not MutableTableDescriptor", tbNames[i].Table())
				}
				err = p.prepareDropWithTableDesc(ctx, tbDesc)
				if err != nil {
					return td, nil, err
				}
			} else if strings.HasPrefix(tbNames[i].Schema(), sessiondata.PgTempSchemaName) {
				// continue may affect delete of udr func. only continue on temp schema
				continue
			}

		} else {
			tbDesc, err = p.prepareDrop(ctx, &tbNames[i], false /*required*/, anyDescType)
			if err != nil {
				return td, nil, err
			}
		}

		if tbDesc == nil {
			tbNames[i].ResolvedAsFunction = true
			fcDescs, err := p.prepareDropFunc(ctx, &tbNames[i], false /*required*/, requireFunctionDesc)
			if err != nil {
				return td, nil, err
			}
			for _, fcDesc := range fcDescs {
				tdf = append(tdf, toDeleteFunction{&tbNames[i], &fcDesc})
			}
			continue

		}
		// Recursively check permissions on all dependent views, since some may
		// be in different databases.
		//for i, ref := range tbDesc.DependedOnBy {
		for i := 0; i < len(tbDesc.DependedOnBy); i++ {
			referencedTable := &sqlbase.TableDescriptor{}
			if err := getDescriptorByID(ctx, p.Txn(), tbDesc.DependedOnBy[i].ID, referencedTable); err != nil {
				return nil, tdf, err
			}
			if err := p.CheckPrivilege(ctx, referencedTable, privilege.DROP); err != nil {
				return nil, tdf, err
			}
			if referencedTable.ParentID != tbDesc.ParentID {
				tbDesc.DependedOnBy = append(tbDesc.DependedOnBy[:i], tbDesc.DependedOnBy[i+1:]...)
				i--
				continue
			}
			if err := p.canRemoveDependentView(ctx, tbDesc, tbDesc.DependedOnBy[i], tree.DropCascade); err != nil {
				return td, tdf, err
			}
		}
		td = append(td, toDeleteTable{&tbNames[i], tbDesc})
	}

	td, err = p.filterCascadedTables(ctx, td)
	return td, tdf, err

}

func (p *planner) DoDropInMultiDatabaseWithBehavior(
	ctx context.Context,
	DropBehavior tree.DropBehavior,
	dt droppedType,
	schemas map[string]*toDropSchema,
) ([]toDeleteTable, []toDeleteFunction, error) {
	var dropTableArr []toDeleteTable
	var dropFuncArr []toDeleteFunction
	for key := range schemas {
		dropTable, dropFunction, err := p.DoDropWithBehavior(ctx, DropBehavior, dt,
			schemas[key].databaseDesc, schemas[key].schemasName)
		if err != nil {
			return nil, nil, err
		}
		dropTableArr = append(dropTableArr, dropTable...)
		dropFuncArr = append(dropFuncArr, dropFunction...)
	}
	return dropTableArr, dropFuncArr, nil
}

func (p *planner) DoDrop(
	params runParams, stmt string, td []toDeleteTable, dbID sqlbase.ID,
) ([]string, int64, error) {
	ctx := params.ctx
	tbNameStrings := make([]string, 0, len(td))
	droppedTableDetails := make([]jobspb.DroppedTableDetails, 0, len(td))
	tableDescs := make([]*sqlbase.MutableTableDescriptor, 0, len(td))

	for _, toDel := range td {
		if toDel.desc.IsView() {
			continue
		}
		droppedTableDetails = append(droppedTableDetails, jobspb.DroppedTableDetails{
			Name: toDel.tn.FQString(),
			ID:   toDel.desc.ID,
		})
		tableDescs = append(tableDescs, toDel.desc)
	}

	jobID, err := p.createDropTablesJob(
		ctx,
		tableDescs,
		droppedTableDetails,
		stmt,
		true, /* drainNames */
		dbID)
	if err != nil {
		return tbNameStrings, 0, err
	}

	for _, toDel := range td {
		tbDesc := toDel.desc
		if tbDesc.IsView() {
			cascadedViews, err := p.dropViewImpl(ctx, tbDesc, tree.DropCascade)
			if err != nil {
				return tbNameStrings, 0, err
			}
			// TODO(knz): dependent dropped views should be qualified here.
			tbNameStrings = append(tbNameStrings, cascadedViews...)
		} else {
			cascadedViews, err := p.dropTableImpl(params, tbDesc, tree.DropCascade)
			if err != nil {
				return tbNameStrings, 0, err
			}
			// TODO(knz): dependent dropped table names should be qualified here.
			tbNameStrings = append(tbNameStrings, cascadedViews...)
		}
		tbNameStrings = append(tbNameStrings, toDel.tn.FQString())
	}

	return tbNameStrings, jobID, nil
}

func (p *planner) dropSchemaImpl(
	ctx context.Context, dbDesc *DatabaseDescriptor, schemas []string,
) []interface{} {
	var res []interface{}
	scDescs := dbDesc.GetSchemaDesc(schemas)
	for _, scDesc := range scDescs {
		nameKey := sqlbase.MakeNameMetadataKey(dbDesc.ID, scDesc.Name)
		descKey := sqlbase.MakeDescMetadataKey(scDesc.ID)
		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "Del %s", nameKey)
			log.VEventf(ctx, 2, "Del %s", descKey)
		}
		res = append(res, nameKey, descKey)
		p.Tables().addUncommittedSchema(scDesc.Name, scDesc.ID, dbDesc.Name, scDesc.ParentID, dbDropped)
	}
	return res
}

// dropSchemaInMultiDatabaseImpl delete schemasMap in different database
func (p *planner) dropSchemaInMultiDatabaseImpl(
	ctx context.Context, schemas map[string]*toDropSchema,
) []interface{} {
	for _, value := range schemas {
		err := p.dropSchemaImpl(ctx, value.databaseDesc, value.schemasName)
		if err != nil {
			return err
		}
	}
	return nil
}

// ExecuteInNewTxn to reduce txn on system intent
func (p *planner) ExecuteInNewTxn(ctx context.Context, b *client.Batch) (err error) {
	err = p.extendedEvalCtx.DB.Txn(ctx, func(_ context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	return err
}

// schemaCache holds a cache from schema name and schema parentID to schema ID. It is
// populated as schema IDs are requested and a new cache is created whenever
// the system config changes. As such, no attempt is made to limit its size
// which is naturally limited by the number of schema descriptors in the
// system the periodic reset whenever the system config is gossiped.
type schemaCache struct {
	// schemasMap is really a map of string -> sqlbase.ID
	schemas sync.Map

	// systemConfig holds a copy of the latest system config since the last
	// call to resetForBatch.
	systemConfig *config.SystemConfig
}

func newSchemaCache(cfg *config.SystemConfig) *schemaCache {
	return &schemaCache{
		systemConfig: cfg,
	}
}

func (sc *schemaCache) getID(key schemaKey) sqlbase.ID {

	val, ok := sc.schemas.Load(key)
	if !ok {
		return sqlbase.InvalidID
	}
	return val.(sqlbase.ID)
}

func (sc *schemaCache) setID(parentID sqlbase.ID, name string, id sqlbase.ID) {
	sKey := schemaKey{parentID: parentID, name: name}
	sc.schemas.Store(sKey, id)
}

// getSchemaID resolves a schema name and schema parentID into a schema ID.
// Returns InvalidID on failure.
func getSchemaID(
	ctx context.Context, txn *client.Txn, parentID sqlbase.ID, name string, required bool,
) (sqlbase.ID, error) {
	// when the parentID is InvalidID, return schemaID is InvalidID
	if parentID == sqlbase.InvalidID {
		return sqlbase.InvalidID, nil
	}
	scID, err := getSchemaDescriptorID(ctx, txn, schemaKey{parentID: parentID, name: name})
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if scID == sqlbase.InvalidID && required {
		return scID, sqlbase.NewUndefinedSchemaError(name)
	}
	return scID, nil
}

// getSchemaID returns the ID of a schema given its name and parentID. It
// uses the descriptor cache if possible, otherwise falls back to KV
// operations.
func (sc *schemaCache) getSchemaID(
	ctx context.Context,
	txnRunner func(context.Context, func(context.Context, *client.Txn) error) error,
	parentID sqlbase.ID,
	name string,
	required bool,
) (sqlbase.ID, error) {
	scID, err := sc.getCachedSchemaID(parentID, name)
	if err != nil {
		return scID, err
	}
	if scID == sqlbase.InvalidID {
		if err := txnRunner(ctx, func(ctx context.Context, txn *client.Txn) error {
			var err error
			scID, err = getSchemaID(ctx, txn, parentID, name, required)
			return err
		}); err != nil {
			return sqlbase.InvalidID, err
		}
	}
	sc.setID(parentID, name, scID)
	return scID, nil
}

// getCachedSchemaID returns the ID of a schema given its schemaKey
// from the cache. This method never goes to the store to resolve
// the schemaKey to ID mapping. Returns InvalidID if the schemaKey to id mapping or
// the schema descriptor are not in the cache.
func (sc *schemaCache) getCachedSchemaID(parentID sqlbase.ID, name string) (sqlbase.ID, error) {
	if !sc.isValid() {
		return sqlbase.InvalidID, nil
	}
	sKey := schemaKey{parentID: parentID, name: name}
	if id := sc.getID(sKey); id != sqlbase.InvalidID {
		return id, nil
	}
	sVal := sc.systemConfig.GetValue(sKey.Key())
	if sVal == nil {
		return sqlbase.InvalidID, nil
	}
	id, err := sVal.GetInt()
	return sqlbase.ID(id), err
}

// getDatabaseDescByID looks up the database descriptor given its ID,
// returning nil if the descriptor is not found. If you want the "not
// found" condition to return an error, use mustGetDatabaseDescByID() instead.
func getSchemaDescByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (*sqlbase.SchemaDescriptor, error) {
	desc := &sqlbase.SchemaDescriptor{}
	if err := getDescriptorByID(ctx, txn, id, desc); err != nil {
		return nil, err
	}
	return desc, nil
}

// MustGetSchemaDescByID looks up the schema descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetSchemaDescByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (*sqlbase.SchemaDescriptor, error) {
	desc, err := getSchemaDescByID(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return desc, nil
}

// getCachedDatabaseDescByID looks up the database descriptor from the descriptor cache,
// given its ID.
func (sc *schemaCache) getCachedSchemaDescByID(id sqlbase.ID) (*sqlbase.SchemaDescriptor, error) {
	if !sc.isValid() {
		return nil, nil
	}
	descKey := sqlbase.MakeDescMetadataKey(id)
	descVal := sc.systemConfig.GetValue(descKey)
	if descVal == nil {
		return nil, nil
	}

	desc := &sqlbase.Descriptor{}
	if err := descVal.GetProto(desc); err != nil {
		return nil, err
	}

	schema := desc.GetSchema()
	if schema == nil {
		return nil, errors.Errorf("[%d] is not a schema", id)
	}

	return schema, schema.Validate()
}

// getSchemaDescByID returns the database descriptor given its ID
// if it exists in the cache, otherwise falls back to KV operations.
func (sc *schemaCache) getSchemaDescByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (*sqlbase.SchemaDescriptor, error) {
	desc, err := sc.getCachedSchemaDescByID(id)
	if err != nil || desc == nil {
		log.VEventf(ctx, 3, "error getting database descriptor from cache: %s", err)
		desc, err = MustGetSchemaDescByID(ctx, txn, id)
	}
	return desc, err
}

// getCachedSchemaDesc looks up the schema descriptor from the descriptor cache,
// given its name. Returns nil and no error if the name is not present in the
// cache.
func (sc *schemaCache) getCachedSchemaDesc(
	parentID sqlbase.ID, name string,
) (*sqlbase.SchemaDescriptor, error) {
	scID, err := sc.getCachedSchemaID(parentID, name)
	if scID == sqlbase.InvalidID || err != nil {
		return nil, err
	}

	return sc.getCachedSchemaDescByID(scID)
}

// getDatabaseDesc returns the database descriptor given its name
// if it exists in the cache, otherwise falls back to KV operations.
func (sc *schemaCache) getSchemaDesc(
	ctx context.Context,
	txnRunner func(context.Context, func(context.Context, *client.Txn) error) error,
	parentID sqlbase.ID,
	name string,
	required bool,
) (*sqlbase.SchemaDescriptor, error) {
	// Lookup the schema in the cache first, falling back to the KV store if it
	// isn't present. The cache might cause the usage of a recently renamed
	// schema, but that's a race that could occur anyways.
	// The cache lookup may fail.
	desc, err := sc.getCachedSchemaDesc(parentID, name)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if err := txnRunner(ctx, func(ctx context.Context, txn *client.Txn) error {
			a := UncachedPhysicalAccessor{}
			desc, err = a.GetSchemaDesc(ctx, txn, parentID, name,
				DatabaseLookupFlags{required: required})
			return err
		}); err != nil {
			return nil, err
		}
	}
	if desc != nil {
		sc.setID(parentID, name, desc.ID)
	}
	return desc, err
}

func init() {
	optbuilder.CheckVirtualSchema = CheckVirtualSchema
}
