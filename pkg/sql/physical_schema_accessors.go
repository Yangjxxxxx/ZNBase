// Copyright 2018  The Cockroach Authors.
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
	"bytes"
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// This file provides reference implementations of the schema accessor
// interface defined in schema_accessors.go.
//
// They are meant to be used to access stored descriptors only.
// For a higher-level implementation that also knows about
// virtual schemasMap, check out logical_schema_accessors.go.
//
// The following implementations are provided:
//
// - UncachedPhysicalAccessor, for uncached db accessors
//
// - CachedPhysicalAccessor, which adds an object cache
//   - plugged on top another SchemaAccessor.
//   - uses a `*TableCollection` (table.go) as cache.
//

// UncachedPhysicalAccessor implements direct access to DB descriptors,
// without any kind of caching.
type UncachedPhysicalAccessor struct{}

var _ SchemaAccessor = UncachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context, txn *client.Txn, name string, flags DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	descID, err := getDescriptorID(ctx, txn, databaseKey{name})
	if err != nil {
		return nil, err
	}
	if descID == sqlbase.InvalidID {
		if flags.required {
			return nil, sqlbase.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}

	desc = &sqlbase.DatabaseDescriptor{}
	if err := getDescriptorByID(ctx, txn, descID, desc); err != nil {
		return nil, err
	}

	return desc, nil
}

// GetSchemaDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetSchemaDesc(
	ctx context.Context, txn *client.Txn, parentID sqlbase.ID, name string, flags DatabaseLookupFlags,
) (desc *SchemaDescriptor, err error) {
	if CheckVirtualSchema(name) {
		if flags.required {
			return nil, sqlbase.NewUndefinedSchemaError(name)
		}
		return nil, nil
	}

	descID, err := getDescriptorID(ctx, txn, schemaKey{parentID: parentID, name: name})
	if err != nil {
		return nil, err
	}
	if descID == sqlbase.InvalidID {
		if flags.required {
			return nil, sqlbase.NewUndefinedSchemaError(name)
		}
		return nil, nil
	}

	desc = &sqlbase.SchemaDescriptor{}
	if err := getDescriptorByID(ctx, txn, descID, desc); err != nil {
		return nil, err
	}

	return desc, nil
}

// IsValidSchema implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) IsValidSchema(
	ctx context.Context,
	txn *client.Txn,
	dbDesc *DatabaseDescriptor,
	scName string,
	flags DatabaseLookupFlags,
) bool {
	// At this point, only the public schema is recognized.
	if dbDesc.Name == sqlbase.SystemDB.Name {
		return scName == tree.PublicSchema || scName == ""
	}
	_, err := a.GetSchemaDesc(ctx, txn, dbDesc.ID, scName, flags)
	if err != nil {
		return false
	}
	return true
}

// GetObjectNames implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectNames(
	ctx context.Context,
	txn *client.Txn,
	dbDesc *DatabaseDescriptor,
	scName string,
	flags DatabaseListFlags,
) (TableNames, error) {
	if ok := a.IsValidSchema(ctx, txn, dbDesc, scName, flags.CommonLookupFlags); !ok {
		if flags.required {
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.Name), tree.Name(scName), "")
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(&tn.TableNamePrefix))
		}
		return nil, nil
	}

	log.Eventf(ctx, "fetching list of objects for %q", dbDesc.Name)
	scan := func(id sqlbase.ID) (roachpb.Key, []client.KeyValue, error) {
		prefix := sqlbase.MakeNameMetadataKey(id, "")
		res, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		return prefix, res, err
	}

	funcScan := func(id sqlbase.ID) (roachpb.Key, []client.KeyValue, error) {
		prefix := sqlbase.MakeFuncNameMetadataKey(id, "", "")
		res, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		return prefix, res, err
	}

	prefix, sr, err := scan(dbDesc.ID)
	if err != nil {
		return nil, err
	}
	funcPrefix, funcSr, funcErr := funcScan(dbDesc.ID)
	if funcErr != nil {
		return nil, funcErr
	}

	var tableNames tree.TableNames

	addTableName := func(kv []client.KeyValue, pre roachpb.Key, isSchema bool) error {
		for _, row := range kv {
			if !isSchema {
				schemaID, err := row.Value.GetInt()
				if err != nil {
					return err
				}
				if dbDesc.ExistSchemaID(sqlbase.ID(schemaID)) {
					continue
				}
			}
			_, tableName, err := encoding.DecodeUnsafeStringAscending(
				bytes.TrimPrefix(row.Key, pre), nil)
			if err != nil {
				return err
			}
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.Name), tree.Name(scName), tree.Name(tableName))
			tn.ExplicitCatalog = flags.explicitPrefix
			tn.ExplicitSchema = flags.explicitPrefix
			tableNames = append(tableNames, tn)
		}
		return nil
	}

	if dbDesc.Name == sqlbase.SystemDB.Name {
		if err := addTableName(sr, prefix, false); err != nil {
			return nil, err
		}
		if err := addTableName(funcSr, funcPrefix, false); err != nil {
			return nil, err
		}
		return tableNames, nil
	}

	for _, row := range sr {
		_, schemaName, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		if schemaName == scName {
			schemaID, err := row.Value.GetInt()
			if err != nil {
				return nil, err
			}
			prefixSc, sc, err := scan(sqlbase.ID(schemaID))
			if err != nil {
				return nil, err
			}
			if err := addTableName(sc, prefixSc, true); err != nil {
				return nil, err
			}
			funcPrefixSc, funcSc, funcErr := funcScan(sqlbase.ID(schemaID))
			if funcErr != nil {
				return nil, funcErr
			}
			if err := addTableName(funcSc, funcPrefixSc, true); err != nil {
				return nil, err
			}
			break
		}
	}

	return tableNames, nil
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context, txn *client.Txn, name *ObjectName, flags ObjectLookupFlags,
) (ObjectDescriptor, error) {
	if name.Schema() == sqlbase.SystemDB.Name {
		if flags.required {
			return nil, sqlbase.NewInvalidWildcardError(name.Schema())
		}
		return nil, nil
	}

	// Look up the database ID.
	dbID, err := getDatabaseID(ctx, txn, name.Catalog(), flags.required)
	if err != nil || dbID == sqlbase.InvalidID {
		// dbID can still be invalid if required is false and the database is not found.
		return nil, err
	}

	if name.Catalog() != sqlbase.SystemDB.Name {
		// if schema name is not public then dbID is schemaID
		dbID, err = getDescriptorID(ctx, txn, schemaKey{parentID: dbID, name: name.Schema()})
		if err != nil && dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	// Try to use the system name resolution bypass. This avoids a hotspot.
	// Note: we can only bypass name to ID resolution. The desc
	// lookup below must still go through KV because system descriptors
	// can be modified on a running cluster.
	descID := sqlbase.LookupSystemTableDescriptorID(dbID, name.Table())
	if flags.requireFunctionDesc {
		relationKey := functionKey{parentID: dbID, name: "", funcName: name.Table()}
		descIDs, err := getFunctionGroupIDs(ctx, txn, relationKey)
		if err != nil {
			return nil, err
		}
		funcDescGroup := &sqlbase.ImmutableFunctionDescriptor{
			FuncGroup:         make([]sqlbase.FuncDesc, 0),
			FunctionGroupName: name.Table(),
			FunctionParentID:  dbID,
		}
		for _, descID := range descIDs {
			funcDesc := sqlbase.FunctionDescriptor{}
			err := getDescriptorByID(ctx, txn, descID, &funcDesc)
			if err != nil {
				return nil, err
			}
			if funcDesc.Name == name.Table() {
				// always latest version, and no need to store in cache, so no need to store expiration time
				desc := sqlbase.FuncDesc{
					Desc: &funcDesc,
				}
				funcDescGroup.FuncGroup = append(funcDescGroup.FuncGroup, desc)
			}
		}
		return funcDescGroup, err

	}
	desc := &sqlbase.TableDescriptor{}
	relationKey := tableKey{parentID: dbID, name: name.Table()}
	descID, err = getDescriptorID(ctx, txn, relationKey)
	if err != nil {
		return nil, err
	}

	if descID == sqlbase.InvalidID {
		// KV name resolution failed.
		if flags.required {
			return nil, sqlbase.NewUndefinedRelationError(name)
		}
		return nil, nil
	}
	err = getDescriptorByID(ctx, txn, descID, desc)
	if err != nil {
		return nil, err
	}

	// We have a descriptor. Is it in the right state? We'll keep it if
	// it is in the ADD state.
	if err := filterTableState(desc); err == nil || err == errTableAdding {
		// Immediately after a RENAME an old name still points to the
		// descriptor during the drain phase for the name. Do not
		// return a descriptor during draining.
		if desc.Name == name.Table() {
			if flags.requireMutable {
				return sqlbase.NewMutableExistingTableDescriptor(*desc), nil
			}
			return sqlbase.NewImmutableTableDescriptor(*desc), nil
		}
	}

	return nil, nil
}

// CachedPhysicalAccessor adds a cache on top of any SchemaAccessor.
type CachedPhysicalAccessor struct {
	SchemaAccessor
	tc *TableCollection
}

var _ SchemaAccessor = &CachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetDatabaseDesc(
	ctx context.Context, txn *client.Txn, name string, flags DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	isSystemDB := name == sqlbase.SystemDB.Name
	if !(flags.avoidCached || isSystemDB || testDisableTableLeases) {
		refuseFurtherLookup, dbID, err := a.tc.getUncommittedDatabaseID(name, flags.required)
		if refuseFurtherLookup || err != nil {
			return nil, err
		}

		if dbID != sqlbase.InvalidID {
			// Some database ID was found in the list of uncommitted DB changes.
			// Use that to get the descriptor.
			desc, err := a.tc.databaseCache.getDatabaseDescByID(ctx, txn, dbID)
			if desc == nil && flags.required {
				return nil, sqlbase.NewUndefinedDatabaseError(name)
			}
			return desc, err
		}

		// The database was not known in the uncommitted list. Have the db
		// cache look it up by name for us.
		return a.tc.databaseCache.getDatabaseDesc(ctx,
			a.tc.leaseMgr.execCfg.DB.Txn, name, flags.required)
	}

	// We avoided the cache. Go lower.
	return a.SchemaAccessor.GetDatabaseDesc(ctx, txn, name, flags)
}

// GetSchemaDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetSchemaDesc(
	ctx context.Context, txn *client.Txn, parentID sqlbase.ID, name string, flags DatabaseLookupFlags,
) (desc *SchemaDescriptor, err error) {
	if !(flags.avoidCached || testDisableTableLeases) {
		refuseFurtherLookup, scID, err := a.tc.getUncommittedSchemaID(name, flags.required)
		if refuseFurtherLookup || err != nil {
			return nil, err
		}

		if scID != sqlbase.InvalidID {
			// Some database ID was found in the list of uncommitted DB changes.
			// Use that to get the descriptor.
			desc, err := a.tc.schemaCache.getSchemaDescByID(ctx, txn, scID)
			if desc == nil && flags.required {
				return nil, sqlbase.NewUndefinedDatabaseError(name)
			}
			return desc, err
		}

		// The database was not known in the uncommitted list. Have the db
		// cache look it up by name for us.
		return a.tc.schemaCache.getSchemaDesc(ctx,
			a.tc.leaseMgr.execCfg.DB.Txn, parentID, name, flags.required)
	}

	// We avoided the cache. Go lower.
	return a.SchemaAccessor.GetSchemaDesc(ctx, txn, parentID, name, flags)
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetObjectDesc(
	ctx context.Context, txn *client.Txn, name *ObjectName, flags ObjectLookupFlags,
) (ObjectDescriptor, error) {
	if flags.requireFunctionDesc == true {
		// todo: 区分 flags.requireMutable
		if flags.requireMutable {
			functionGroup, err := a.tc.getMutableFuncDescriptor(ctx, txn, name, flags)
			if err != nil {
				return nil, err
			}
			return functionGroup, nil
		}
		// get function desc group and return
		functions, err := a.tc.getFunctionGroup(ctx, txn, name, flags)
		if functions == nil {
			return nil, err
		}
		return functions, err
	}

	if flags.requireMutable {
		table, err := a.tc.getMutableTableDescriptor(ctx, txn, name, flags)
		if table == nil {
			// return nil interface.
			return nil, err
		}
		return table, err
	}
	table, err := a.tc.getTableVersion(ctx, txn, name, flags)
	if table == nil {
		// return nil interface.
		return nil, err
	}
	return table, err
}
