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
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/stats"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/encoding"
)

// optCatalog implements the cat.Catalog interface over the SchemaResolver
// interface for the use of the new optimizer. The interfaces are simplified to
// only include what the optimizer needs, and certain common lookups are cached
// for faster performance.
type optCatalog struct {
	// planner needs to be set via a call to init before calling other methods.
	planner *planner

	// cfg is the gossiped and cached system config. It may be nil if the node
	// does not yet have it available.
	cfg *config.SystemConfig

	// dataSources is a cache of table and view objects that's used to satisfy
	// repeated calls for the same data source.
	// Note that the data source object might still need to be recreated if
	// something outside of the descriptor has changed (e.g. table stats).
	dataSources map[*sqlbase.ImmutableTableDescriptor]cat.DataSource

	// functionGroup
	functionGroupSource map[*sqlbase.ImmutableFunctionDescriptor]cat.UdrFunction

	// tn is a temporary name used during resolution to avoid heap allocation.
	tn tree.TableName
}

var _ cat.Catalog = &optCatalog{}

// SysTemTable defines tables which has no datasourceengine info.
var SysTemTable = []string{"lease", "jobs", "settings", "eventlog", "rangelog", "snapshots", "table_statistics", "zones"}

// init initializes an optCatalog instance (which the caller can pre-allocate).
// The instance can be used across multiple queries, but reset() should be
// called for each query.
func (oc *optCatalog) init(planner *planner) {
	oc.planner = planner
	oc.dataSources = make(map[*sqlbase.ImmutableTableDescriptor]cat.DataSource)
	oc.functionGroupSource = make(map[*sqlbase.ImmutableFunctionDescriptor]cat.UdrFunction)
}

// reset prepares the optCatalog to be used for a new query.
func (oc *optCatalog) reset() {
	// If we have accumulated too many tables in our map, throw everything away.
	// This deals with possible edge cases where we do a lot of DDL in a
	// long-lived session.
	if len(oc.dataSources) > 100 {
		oc.dataSources = make(map[*sqlbase.ImmutableTableDescriptor]cat.DataSource)
	}

	// Gossip can be nil in testing scenarios.
	if oc.planner.execCfg.Gossip != nil {
		oc.cfg = oc.planner.execCfg.Gossip.GetSystemConfig()
	}
}

// optSchema is a wrapper around sqlbase.DatabaseDescriptor that implements the
// cat.Object and cat.Schema interfaces.
type optSchema struct {
	desc sqlbase.DescriptorProto

	name cat.SchemaName
}

// ID is part of the cat.Object interface.
func (os *optSchema) ID() cat.StableID {
	return cat.StableID(os.desc.GetID())
}

// SetScDesc is part of the cat.Object interface.
func (os optSchema) ReplaceDesc(scName string) cat.Schema {
	dbDesc := os.DbDesc()
	if dbDesc == nil {
		return &os
	}
	if isInternal := CheckVirtualSchema(scName); isInternal {
		return &os
	}
	scDesc, err := dbDesc.GetSchemaByName(scName)
	if err != nil {
		return &os
	}
	os.desc = scDesc
	return &os
}

// Equals is part of the cat.Object interface.
func (os *optSchema) Equals(other cat.Object) bool {
	otherSchema, ok := other.(*optSchema)
	return ok && os.desc.GetID() == otherSchema.desc.GetID()
}

// Name is part of the cat.Schema interface.
func (os *optSchema) Name() *cat.SchemaName {
	return &os.name
}

func (os *optSchema) DbDesc() *sqlbase.DatabaseDescriptor {
	if dbDesc, ok := os.desc.(*sqlbase.DatabaseDescriptor); ok {
		return dbDesc
	}
	return nil
}

// ResolveTableDesc returns the table descriptor via table name.
func (oc *optCatalog) ResolveTableDesc(name *tree.TableName) (interface{}, error) {
	objDesc, err := ResolveExistingObject(context.Background(), oc.planner, name, true, requireTableOrMviewDesc)
	if err != nil {
		return nil, err
	}
	tableDesc := objDesc.TableDesc()
	return tableDesc, nil
}

// GetDatabaseDescByName is part of the cat.Catalog interface.
func (oc *optCatalog) GetDatabaseDescByName(
	ctx context.Context, txn *client.Txn, dbName string,
) (interface{}, error) {
	p := oc.planner
	sc := p.LogicalSchemaAccessor()
	db, err := sc.GetDatabaseDesc(ctx, txn, dbName, p.CommonLookupFlags(true /*required*/))
	if err != nil {
		return nil, err
	}
	return db, err
}

// GetSchemaDescByName is part of the cat.Catalog interface.
func (oc *optCatalog) GetSchemaDescByName(
	ctx context.Context, txn *client.Txn, scName string,
) (interface{}, error) {
	p := oc.planner
	sc := p.LogicalSchemaAccessor()
	db, err := sc.GetDatabaseDesc(ctx, txn, p.CurrentDatabase(), p.CommonLookupFlags(true /*required*/))
	if err != nil {
		return nil, err
	}
	schema, err := sc.GetSchemaDesc(ctx, txn, db.ID, scName, p.CommonLookupFlags(true /*required*/))
	if err != nil {
		return nil, err
	}
	return schema, err
}

// GetCurrentDatabase is part of the cat.Catalog interface.
func (oc *optCatalog) GetCurrentDatabase(ctx context.Context) string {
	return oc.planner.SessionData().Database
}

// ResolveUDRFunctionDesc returns the function descriptors via function name.
func (oc *optCatalog) ResolveUDRFunctionDesc(
	name *tree.TableName,
) (interface{}, cat.UdrFunction, error) {
	desc, err := resolveExistingObjectImpl(context.Background(), oc.planner, name, true, false, requireFunctionDesc)
	if err != nil {
		return nil, nil, err
	}
	funcDescGroup := desc.(*sqlbase.ImmutableFunctionDescriptor)
	// after resolve udr, push immutable function group into optCatalog's map
	var of *optUdrFunction
	if _, ok := oc.functionGroupSource[funcDescGroup]; !ok {
		of = newOptFunction(funcDescGroup, *name)
		oc.functionGroupSource[funcDescGroup] = of
	}
	return funcDescGroup, of, nil
}

// GetCurrentSchema is part of the cat.Catalog interface.
func (oc *optCatalog) GetCurrentSchema(
	ctx context.Context, name tree.Name,
) (found bool, schema string, err error) {
	defer func(prev bool) {
		oc.planner.avoidCachedDescriptors = prev
	}(oc.planner.avoidCachedDescriptors)
	oc.planner.avoidCachedDescriptors = true
	iter := oc.planner.CurrentSearchPath().IterWithoutImplicitPGCatalog()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if found, _, err = oc.planner.LookupSchema(ctx, string(name), scName); found || err != nil {
			if err == nil {
				schema = scName
			}
			break
		}
	}
	return found, schema, err
}

// ResolveSchema is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveSchema(
	ctx context.Context, flags cat.Flags, name *cat.SchemaName,
) (cat.Schema, cat.SchemaName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}

	// ResolveTargetObject wraps ResolveTarget in order to raise "schema not
	// found" and "schema cannot be modified" errors. However, ResolveTargetObject
	// assumes that a data source object is being resolved, which is not the case
	// for ResolveSchema. Therefore, call ResolveTarget directly and produce a
	// more general error.
	oc.tn.TableName = ""
	oc.tn.TableNamePrefix = *name
	found, desc, err := oc.tn.ResolveTarget(
		ctx,
		oc.planner,
		oc.planner.CurrentDatabase(),
		oc.planner.CurrentSearchPath(),
	)
	if err != nil {
		return nil, cat.SchemaName{}, err
	}
	if !found {
		return nil, cat.SchemaName{}, pgerror.NewErrorf(pgcode.InvalidSchemaName,
			"target database or schema does not exist")
	}
	return &optSchema{desc: desc.(*DatabaseDescriptor)}, oc.tn.TableNamePrefix, nil
}

var replica sync.Map

// ResolveAllDataSource is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveAllDataSource(
	ctx context.Context, flags cat.Flags, name *cat.SchemaName,
) ([]cat.DataSourceName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}
	found, descI, err := name.Resolve(
		ctx, oc.planner, oc.planner.CurrentDatabase(), oc.planner.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sqlbase.NewInvalidWildcardError(tree.ErrString(name))
	}

	return GetObjectNames(ctx, oc.planner.txn, oc.planner, descI.(*DatabaseDescriptor), []string{name.Schema()}, name.ExplicitSchema)
}

// ResolveDataSource is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSource(
	ctx context.Context, flags cat.Flags, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}
	oc.tn = *name
	desc, err := ResolveExistingObject(ctx, oc.planner, &oc.tn, true /* required */, anyDescType)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}

	if desc.ReplicationTable {
		if _, ok := replica.Load(desc.ID); !ok {
			if err := oc.maybeSetZone(ctx, oc.tn, desc); err != nil {
				return nil, cat.DataSourceName{}, err
			}
		}
	}

	if err := oc.planner.checkPrivilegeAccessToDataSource(ctx, &oc.tn); err != nil {
		return nil, cat.DataSourceName{}, err
	}
	ds, err := oc.dataSourceForDesc(ctx, desc, &oc.tn)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	return ds, oc.tn, nil
}

func (oc *optCatalog) maybeSetZone(
	ctx context.Context, t tree.TableName, table *ImmutableTableDescriptor,
) error {
	request := &serverpb.NodesRequest{}
	nodes, err := oc.planner.execCfg.StatusServer.Nodes(ctx, request)
	if err != nil {
		return err
	}
	zoneSpecifier := tree.ZoneSpecifier{}
	zoneSpecifier.TableOrIndex.Table = t
	targetID, err := resolveZone(
		ctx, oc.planner.txn, &zoneSpecifier)
	if err != nil {
		return err
	}
	value, err := GetNumReplicas(ctx, oc.planner.txn, uint32(targetID))
	if err != nil {
		return err
	}
	num := len(nodes.Nodes)
	if value != num {
		partialZone, err := getZoneConfigRaw(ctx, oc.planner.txn, targetID)
		if err != nil {
			return err
		}
		if partialZone == nil {
			partialZone = config.NewZoneConfig()
		}
		_, completeZone, _, err := GetZoneConfigInTxn(ctx, oc.planner.txn,
			uint32(targetID), nil, "", false)

		if err == errNoZoneConfigApplies {
			// No zone config yet.
			//
			// GetZoneConfigInTxn will fail with errNoZoneConfigApplies when
			// the target ID is not a database object, i.e. one of the system
			// ranges (liveness, meta, etc.), and did not have a zone config
			// already.
			defZone := config.DefaultZoneConfig()
			completeZone = &defZone
		} else if err != nil {
			return err
		}
		newZone := *completeZone
		finalZone := *partialZone
		newZone.NumReplicas = proto.Int32(int32(num))
		finalZone.NumReplicas = proto.Int32(int32(num))
		if err := validateZoneAttrsAndLocalities(
			ctx,
			oc.planner.execCfg.StatusServer.Nodes,
			&newZone,
		); err != nil {
			return err
		}
		completeZone = &newZone
		partialZone = &finalZone
		if err := completeZone.Validate(); err != nil {
			return pgerror.NewErrorf(pgcode.CheckViolation,
				"could not validate zone config: %v", err)
		}
		execConfig := oc.planner.execCfg
		zoneToWrite := partialZone
		if !execConfig.Settings.Version.IsActive(cluster.VersionCascadingZoneConfigs) {
			zoneToWrite = completeZone
		}
		if err := zoneToWrite.ValidateTandemFields(); err != nil {
			return pgerror.NewErrorf(pgcode.InvalidParameterValue,
				"could not validate zone config: %v", err).SetHintf(
				"try ALTER ... CONFIGURE ZONE USING <field_name> = COPY FROM PARENT [, ...] so populate the field")
		}
		_, err = writeZoneConfig(ctx, oc.planner.txn,
			targetID, table.TableDesc(), zoneToWrite, execConfig, false)
		if err != nil {
			return err
		}
	}
	replica.Store(table.ID, struct{}{})
	return nil
}

// ResolveDataSourceByID is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSourceByID(
	ctx context.Context, flags cat.Flags, dataSourceID cat.StableID,
) (cat.DataSource, error) {
	if flags.AvoidDescriptorCaches {
		defer func(prev bool) {
			oc.planner.avoidCachedDescriptors = prev
		}(oc.planner.avoidCachedDescriptors)
		oc.planner.avoidCachedDescriptors = true
	}

	tableLookup, err := oc.planner.LookupTableByID(ctx, sqlbase.ID(dataSourceID))

	if err != nil || tableLookup.IsAdding {
		if err == sqlbase.ErrDescriptorNotFound || tableLookup.IsAdding {
			return nil, sqlbase.NewUndefinedRelationError(&tree.TableRef{TableID: int64(dataSourceID)})
		}
		return nil, err
	}
	desc := tableLookup.Desc

	tbName, err := sqlbase.GetTableName(ctx, oc.planner.Txn(), desc.ParentID, desc.Name)
	if err != nil {
		return nil, err
	}

	return oc.dataSourceForDesc(ctx, desc, tbName)
}

// ResolveUdrFunction will get function group from table collection, if found will return a UdrFunction
func (oc *optCatalog) ResolveUdrFunction(
	ctx context.Context, name *cat.DataSourceName,
) (cat.UdrFunction, error) {
	p := oc.planner
	flags := ObjectLookupFlags{
		CommonLookupFlags: CommonLookupFlags{
			avoidCached:         p.avoidCachedDescriptors,
			required:            true,
			requireFunctionDesc: true,
		},
	}
	functionGroupDesc, err := p.Tables().getFunctionGroup(ctx, p.txn, name, flags)
	if err != nil {
		return nil, err
	}
	optFunc, _ := oc.functionGroupSource[functionGroupDesc]
	if optFunc == nil {
		of := newOptFunction(functionGroupDesc, *name)
		oc.functionGroupSource[functionGroupDesc] = of
		return of, nil
	}
	return optFunc, nil
}

// CheckPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
	switch t := o.(type) {
	case *optSchema:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	case *optTable:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	case *optTableXQ:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	case *optView:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	case *optSequence:
		return oc.planner.CheckPrivilege(ctx, t.desc, priv)
	case *optUdrFunction:
		return nil
	default:
		return pgerror.NewAssertionErrorf("invalid object type: %T", o)
	}
}

// CheckDMLPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckDMLPrivilege(
	ctx context.Context, o cat.Object, cols tree.NameList, priv privilege.Kind, user string,
) error {
	switch t := o.(type) {
	case *optTable:
		return oc.planner.CheckDMLPrivilege(ctx, &t.desc.TableDescriptor, cols, priv, user)
	case *optTableXQ:
		return oc.planner.CheckDMLPrivilege(ctx, &t.desc.TableDescriptor, cols, priv, user)
	case *optView:
		return oc.planner.CheckDMLPrivilege(ctx, &t.desc.TableDescriptor, cols, priv, user)
	case *optSequence:
		return oc.planner.CheckDMLPrivilege(ctx, &t.desc.TableDescriptor, cols, priv, user)
	default:
		return pgerror.NewAssertionErrorf("invalid object type: %T", o)
	}
}

// CheckAnyColumnPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckAnyColumnPrivilege(ctx context.Context, o cat.Object) error {
	switch t := o.(type) {
	case *optTable:
		return oc.planner.CheckAnyColumnPrivilege(ctx, t.desc)
	case *optTableXQ:
		return oc.planner.CheckAnyColumnPrivilege(ctx, t.desc)
	case *optView:
		return oc.planner.CheckAnyColumnPrivilege(ctx, t.desc)
	default:
		return pgerror.NewAssertionErrorf("invalid object type: %T", o)
	}
}

// dataSourceForDesc returns a data source wrapper for the given descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) dataSourceForDesc(
	ctx context.Context, desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName,
) (cat.DataSource, error) {
	if desc.IsTable() || desc.MaterializedView() {
		// Tables require invalidation logic for cached wrappers.
		// Because they are backed by physical data, we treat materialized views
		// as tables for the purposes of planning.
		if len(oc.planner.execCfg.LocationName.Names) > 0 {
			return oc.dataSourceForTableXQ(ctx, desc, name)
		}
		return oc.dataSourceForTable(ctx, desc, name)
	}

	ds, ok := oc.dataSources[desc]
	if ok {
		return ds, nil
	}

	switch {
	case desc.IsView():
		ds = newOptView(desc, name)

	case desc.IsSequence():
		ds = newOptSequence(desc, name)

	default:
		return nil, pgerror.NewAssertionErrorf("unexpected table descriptor: %+v", desc)
	}

	oc.dataSources[desc] = ds
	return ds, nil
}

// dataSourceForTable returns a table data source wrapper for the given descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) dataSourceForTable(
	ctx context.Context, desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName,
) (cat.DataSource, error) {
	// Even if we have a cached data source, we still have to cross-check that
	// statistics and the zone config haven't changed.
	tableStats, err := oc.planner.execCfg.TableStatsCache.GetTableStats(context.TODO(), desc.ID)
	if err != nil {
		// Ignore any error. We still want to be able to run queries even if we lose
		// access to the statistics table.
		// TODO(radu): at least log the error.
		tableStats = nil
	}

	zoneConfig, err := oc.getZoneConfig(desc)
	if err != nil {
		return nil, err
	}

	// Check to see if there's already a data source wrapper for this descriptor,
	// and it was created with the same stats and zone config.
	if ds, ok := oc.dataSources[desc]; ok && !ds.(*optTable).isStale(tableStats, zoneConfig) {
		return ds, nil
	}

	id := cat.StableID(desc.ID)
	if desc.IsVirtualTable() {
		// A virtual table can effectively have multiple instances, with different
		// contents. For example `db1.pg_catalog.pg_sequence` contains info about
		// sequences in db1, whereas `db2.pg_catalog.pg_sequence` contains info
		// about sequences in db2.
		//
		// These instances should have different stable IDs. To achieve this, we
		// prepend the database ID.
		//
		// Note that some virtual tables have a special instance with empty catalog,
		// for example "".information_schema.tables contains info about tables in
		// all databases. We treat the empty catalog as having database ID 0.
		if name.Catalog() != "" {
			// TODO(radu): it's unfortunate that we have to lookup the schema again.
			_, dbDesc, err := oc.planner.LookupSchema(ctx, name.Catalog(), name.Schema())
			if err != nil {
				return nil, err
			}
			if dbDesc == nil {
				// The database was not found. This can happen e.g. when
				// accessing a virtual schema over a non-existent
				// database. This is a common scenario when the current db
				// in the session points to a database that was not created
				// yet.
				//
				// In that case we use an invalid database ID. We
				// distinguish this from the empty database case because the
				// virtual tables do not "contain" the same information in
				// both cases.
				id |= cat.StableID(math.MaxUint32) << 32
			} else {
				id |= cat.StableID(dbDesc.(*DatabaseDescriptor).ID) << 32
			}
		}
	}

	ds := newOptTable(desc, id, name, tableStats, zoneConfig)
	if !desc.IsVirtualTable() {
		// Virtual tables can have multiple effective instances that utilize the
		// same descriptor (see above).
		oc.dataSources[desc] = ds
	}
	return ds, nil
}

// dataSourceForTable returns a table data source wrapper for the given descriptor.
// The wrapper might come from the cache, or it may be created now.XQ
func (oc *optCatalog) dataSourceForTableXQ(
	ctx context.Context, desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName,
) (cat.DataSource, error) {
	// Even if we have a cached data source, we still have to cross-check that
	// statistics and the zone config haven't changed.
	tableStats, err := oc.planner.execCfg.TableStatsCache.GetTableStats(context.TODO(), desc.ID)
	if err != nil {
		// Ignore any error. We still want to be able to run queries even if we lose
		// access to the statistics table.
		// TODO(radu): at least log the error.
		tableStats = nil
	}

	locationMap, err := oc.getLocationMapConfig(desc)
	if err != nil {
		return nil, err
	}

	// Check to see if there's already a data source wrapper for this descriptor,
	// and it was created with the same stats and zone config.
	if ds, ok := oc.dataSources[desc]; ok && !ds.(*optTableXQ).isStaleXQ(tableStats, locationMap) {
		return ds, nil
	}

	id := cat.StableID(desc.ID)
	if desc.IsVirtualTable() {
		// A virtual table can effectively have multiple instances, with different
		// contents. For example `db1.pg_catalog.pg_sequence` contains info about
		// sequences in db1, whereas `db2.pg_catalog.pg_sequence` contains info
		// about sequences in db2.
		//
		// These instances should have different stable IDs. To achieve this, we
		// prepend the database ID.
		//
		// Note that some virtual tables have a special instance with empty catalog,
		// for example "".information_schema.tables contains info about tables in
		// all databases. We treat the empty catalog as having database ID 0.
		if name.Catalog() != "" {
			// TODO(radu): it's unfortunate that we have to lookup the schema again.
			_, dbDesc, err := oc.planner.LookupSchema(ctx, name.Catalog(), name.Schema())
			if err != nil {
				return nil, err
			}
			if dbDesc == nil {
				// The database was not found. This can happen e.g. when
				// accessing a virtual schema over a non-existent
				// database. This is a common scenario when the current db
				// in the session points to a database that was not created
				// yet.
				//
				// In that case we use an invalid database ID. We
				// distinguish this from the empty database case because the
				// virtual tables do not "contain" the same information in
				// both cases.
				id |= cat.StableID(math.MaxUint32) << 32
			} else {
				id |= cat.StableID(dbDesc.(*DatabaseDescriptor).ID) << 32
			}
		}
	}

	zoneConfig, err := oc.getZoneConfig(desc)
	if err != nil {
		return nil, err
	}

	ds := newOptTableXQ(desc, id, name, tableStats, locationMap, zoneConfig)
	if !desc.IsVirtualTable() {
		// Virtual tables can have multiple effective instances that utilize the
		// same descriptor (see above).
		oc.dataSources[desc] = ds
	}
	return ds, nil
}

var emptyZoneConfig = &config.ZoneConfig{}
var emptyLocationMapConfig = &roachpb.LocationMap{}

// getZoneConfig returns the ZoneConfig data structure for the given table.
// ZoneConfigs are stored in protobuf binary format in the SystemConfig, which
// is gossiped around the cluster. Note that the returned ZoneConfig might be
// somewhat stale, since it's taken from the gossiped SystemConfig.
func (oc *optCatalog) getZoneConfig(
	desc *sqlbase.ImmutableTableDescriptor,
) (*config.ZoneConfig, error) {
	// Lookup table's zone if system config is available (it may not be as node
	// is starting up and before it's received the gossiped config). If it is
	// not available, use an empty config that has no zone constraints.
	if oc.cfg == nil || desc.IsVirtualTable() {
		return emptyZoneConfig, nil
	}
	zone, err := oc.cfg.GetZoneConfigForObject(uint32(desc.ID))
	if err != nil {
		return nil, err
	}
	if zone == nil {
		// This can happen with tests that override the hook.
		zone = emptyZoneConfig
	}
	return zone, err
}

// getZoneConfig returns the ZoneConfig data structure for the given table.
// ZoneConfigs are stored in protobuf binary format in the SystemConfig, which
// is gossiped around the cluster. Note that the returned ZoneConfig might be
// somewhat stale, since it's taken from the gossiped SystemConfig.
func (oc *optCatalog) getLocationMapConfig(
	desc *sqlbase.ImmutableTableDescriptor,
) (*roachpb.LocationMap, error) {
	// Lookup table's zone if system config is available (it may not be as node
	// is starting up and before it's received the gossiped config). If it is
	// not available, use an empty config that has no zone constraints.
	if oc.cfg == nil || desc.IsVirtualTable() {
		return emptyLocationMapConfig, nil
	}
	locationMap, err := oc.cfg.GetLocateMapForObject(uint32(desc.ID))
	if err != nil {
		return emptyLocationMapConfig, err
	}
	if locationMap == nil {
		// This can happen with tests that override the hook.
		locationMap = emptyLocationMapConfig
	}
	return locationMap, err
}

// optView is a wrapper around sqlbase.ImmutableTableDescriptor that implements
// the cat.Object, cat.DataSource, and cat.View interfaces.
type optView struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of
	// the view.
	name cat.DataSourceName
}

var _ cat.View = &optView{}

func newOptView(desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName) *optView {
	ov := &optView{desc: desc, name: *name}

	// The cat.View interface requires that view names be fully qualified.
	ov.name.ExplicitSchema = true
	ov.name.ExplicitCatalog = true

	return ov
}

// ID is part of the cat.Object interface.
func (ov *optView) ID() cat.StableID {
	return cat.StableID(ov.desc.ID)
}

// Equals is part of the cat.Object interface.
func (ov *optView) Equals(other cat.Object) bool {
	otherView, ok := other.(*optView)
	if !ok {
		return false
	}
	return ov.desc.ID == otherView.desc.ID && ov.desc.Version == otherView.desc.Version
}

// Name is part of the cat.View interface.
func (ov *optView) Name() *cat.DataSourceName {
	return &ov.name
}

// GetOwner is part of the cat.DataSource interface.
func (ov *optView) GetOwner() string {
	return getOwnerOfDesc(ov.desc)
}

// Query is part of the cat.View interface.
func (ov *optView) Query() string {
	return ov.desc.ViewQuery
}

// ColumnNameCount is part of the cat.View interface.
func (ov *optView) ColumnNameCount() int {
	return len(ov.desc.Columns)
}

// ColumnName is part of the cat.View interface.
func (ov *optView) ColumnName(i int) tree.Name {
	return tree.Name(ov.desc.Columns[i].Name)
}

// IsSystemView is part of the cat.View interface.
func (ov *optView) IsSystemView() bool {
	return ov.desc.IsVirtualTable()
}

// GetBaseTable will return base table of an updatable view.
func (ov *optView) GetBaseTableID() (int, error) {
	if ov.desc != nil && ov.desc.ViewUpdatable {
		return int(ov.desc.ViewDependsOn), nil
	}
	return -1, fmt.Errorf("it is not an updatable view")
}

// GetViewDependsColName will return a map which map columns in view and base table.
func (ov *optView) GetViewDependsColName() (map[string]string, bool, error) {
	columnsReflectMap := make(map[string]string)
	viewFlag := false
	if ov.desc != nil && ov.desc.ViewUpdatable {
		viewFlag = true
		for i := range ov.desc.Columns {
			columnsReflectMap[ov.desc.Columns[i].Name] = ov.desc.Columns[i].UpdatableViewDependsColName
		}
		return columnsReflectMap, viewFlag, nil
	}
	return columnsReflectMap, viewFlag, fmt.Errorf("Error:  %s is not an updatable view", ov.name.String())
}

// optSequence is a wrapper around sqlbase.ImmutableTableDescriptor that
// implements the cat.Object and cat.DataSource interfaces.
type optSequence struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of the
	// sequence.
	name cat.DataSourceName
}

var _ cat.DataSource = &optSequence{}
var _ cat.Sequence = &optSequence{}

func newOptSequence(desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName) *optSequence {
	os := &optSequence{desc: desc, name: *name}

	// The cat.Sequence interface requires that table names be fully qualified.
	os.name.ExplicitSchema = true
	os.name.ExplicitCatalog = true

	return os
}

// ID is part of the cat.Object interface.
func (os *optSequence) ID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// Equals is part of the cat.Object interface.
func (os *optSequence) Equals(other cat.Object) bool {
	otherSeq, ok := other.(*optSequence)
	if !ok {
		return false
	}
	return os.desc.ID == otherSeq.desc.ID && os.desc.Version == otherSeq.desc.Version
}

// Name is part of the cat.DataSource interface.
func (os *optSequence) Name() *cat.DataSourceName {
	return &os.name
}

// GetOwner is part of the cat.DataSource interface.
func (os *optSequence) GetOwner() string {
	return getOwnerOfDesc(os.desc)
}

// SequenceName is part of the cat.Sequence interface.
func (os *optSequence) SequenceName() *tree.TableName {
	return os.Name()
}

// optTable is a wrapper around sqlbase.ImmutableTableDescriptor that caches
// index wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// This is the descriptor ID, except for virtual tables.
	id cat.StableID

	// name is the fully qualified, fully resolved, fully normalized name of the
	// table.
	name cat.DataSourceName

	// indexes are the inlined wrappers for the table's primary and secondary
	// indexes.
	indexes []optIndex

	// rawStats stores the original table statistics slice. Used for a fast-path
	// check that the statistics haven't changed.
	rawStats []*stats.TableStatistic

	// stats are the inlined wrappers for table statistics.
	stats []optTableStat

	zone *config.ZoneConfig

	locationMap *roachpb.LocationMap

	// family is the inlined wrapper for the table's primary family. The primary
	// family is the first family explicitly specified by the user. If no families
	// were explicitly specified, then the primary family is synthesized.
	primaryFamily optFamily

	// families are the inlined wrappers for the table's non-primary families,
	// which are all the families specified by the user after the first. The
	// primary family is kept separate since the common case is that there's just
	// one family.
	families []optFamily

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int
}

type optTableXQ struct {
	*optTable
	LocationMap *roachpb.LocationMap
	indexXQs    []optIndexXQ
}

var _ cat.Table = &optTable{}
var _ cat.TableXQ = &optTableXQ{}

func newOptTable(
	desc *sqlbase.ImmutableTableDescriptor,
	id cat.StableID,
	name *cat.DataSourceName,
	stats []*stats.TableStatistic,
	tblZone *config.ZoneConfig,
) *optTable {
	ot := &optTable{
		desc:     desc,
		id:       id,
		name:     *name,
		rawStats: stats,
		zone:     tblZone,
	}

	// The cat.Table interface requires that table names be fully qualified.
	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	// Create the table's column mapping from sqlbase.ColumnID to column ordinal.
	ot.colMap = make(map[sqlbase.ColumnID]int, ot.DeletableColumnCount())
	for i, n := 0, ot.DeletableColumnCount(); i < n; i++ {
		ot.colMap[sqlbase.ColumnID(ot.Column(i).ColID())] = i
	}

	if !ot.desc.IsVirtualTable() {
		// Build the indexes (add 1 to account for lack of primary index in
		// DeletableIndexes slice).
		ot.indexes = make([]optIndex, 1+len(ot.desc.DeletableIndexes()))

		for i := range ot.indexes {
			var idxDesc *sqlbase.IndexDescriptor
			if i == 0 {
				idxDesc = &desc.PrimaryIndex
			} else {
				idxDesc = &ot.desc.DeletableIndexes()[i-1]
			}

			// If there is a subzone that applies to the entire index, use that,
			// else use the table zone. Skip subzones that apply to partitions,
			// since they apply only to a subset of the index.
			idxZone := tblZone
			for j := range tblZone.Subzones {
				subzone := &tblZone.Subzones[j]
				if subzone.IndexID == uint32(idxDesc.ID) && subzone.PartitionName == "" {
					copyZone := subzone.Config
					copyZone.InheritFromParent(tblZone)
					idxZone = &copyZone
				}
			}

			ot.indexes[i].init(ot, i, idxDesc, idxZone)
		}
	}

	if len(desc.Families) == 0 {
		// This must be a virtual table, so synthesize a primary family. Only
		// column ids are needed by the family wrapper.
		family := &sqlbase.ColumnFamilyDescriptor{Name: "primary", ID: 0}
		family.ColumnIDs = make([]sqlbase.ColumnID, len(desc.Columns))
		for i := range family.ColumnIDs {
			family.ColumnIDs[i] = desc.Columns[i].ID
		}
		ot.primaryFamily.init(ot, family)
	} else {
		ot.primaryFamily.init(ot, &desc.Families[0])
		ot.families = make([]optFamily, len(desc.Families)-1)
		for i := range ot.families {
			ot.families[i].init(ot, &desc.Families[i+1])
		}
	}

	// Add stats last, now that other metadata is initialized.
	if stats != nil {
		ot.stats = make([]optTableStat, len(stats))
		n := 0
		for i := range stats {
			// We skip any stats that have columns that don't exist in the table anymore.
			if ot.stats[n].init(ot, stats[i]) {
				n++
			}
		}
		ot.stats = ot.stats[:n]
	}

	return ot
}

func newOptTableXQ(
	desc *sqlbase.ImmutableTableDescriptor,
	id cat.StableID,
	name *cat.DataSourceName,
	stats []*stats.TableStatistic,
	tblMap *roachpb.LocationMap,
	tblZone *config.ZoneConfig,
) *optTableXQ {

	ot := newOptTable(desc, id, name, stats, tblZone)

	var otXQ = optTableXQ{ot, tblMap, nil}

	if len(ot.indexes) > 0 {
		otXQ.indexXQs = make([]optIndexXQ, len(ot.indexes))

		for i := range otXQ.indexXQs {
			//var idxDesc *sqlbase.IndexDescriptor
			//if i == 0 {
			//	idxDesc = &desc.PrimaryIndex
			//} else {
			//	idxDesc = &ot.desc.DeletableIndexes()[i-1]
			//}

			// If there is a subzone that applies to the entire index, use that,
			// else use the table zone. Skip subzones that apply to partitions,
			// since they apply only to a subset of the index.
			var idxSpace *roachpb.LocationValue
			if tblMap.IndexSpace == nil {
				idxSpace = nil
			} else {
				idxSpace = tblMap.IndexSpace[uint32(i+1)]
			}

			if idxSpace == nil {
				idxSpace = tblMap.TableSpace
			}
			otXQ.indexXQs[i] = optIndexXQ{&(ot.indexes[i]), idxSpace}
		}
	}

	return &otXQ
}

// ID is part of the cat.Object interface.
func (ot *optTable) ID() cat.StableID {
	return ot.id
}

func (ot *optTable) IsHashPartition() bool {
	return ot.desc.IsHashPartition
}

// isStale checks if the optTable object needs to be refreshed because the stats
// or zone config have changed. False positives are ok.
func (ot *optTable) isStale(tableStats []*stats.TableStatistic, zone *config.ZoneConfig) bool {
	// Fast check to verify that the statistics haven't changed: we check the
	// length and the address of the underlying array. This is not a perfect
	// check (in principle, the stats could have left the cache and then gotten
	// regenerated), but it works in the common case.
	if len(tableStats) != len(ot.rawStats) {
		return true
	}
	if len(tableStats) > 0 && &tableStats[0] != &ot.rawStats[0] {
		return true
	}
	if !zonesAreEqual(zone, ot.zone) {
		return true
	}
	return false
}

// Equals is part of the cat.Object interface.
func (ot *optTable) Equals(other cat.Object) bool {
	otherTable, ok := other.(*optTable)
	if !ok {
		return false
	}
	if ot == otherTable {
		// Fast path when it is the same object.
		return true
	}
	if ot.id != otherTable.id || ot.desc.Version != otherTable.desc.Version {
		return false
	}

	// Verify the stats are identical.
	if len(ot.stats) != len(otherTable.stats) {
		return false
	}
	for i := range ot.stats {
		if !ot.stats[i].equals(&otherTable.stats[i]) {
			return false
		}
	}

	// Verify that indexes are in same zones. For performance, skip deep equality
	// check if it's the same as the previous index (common case).
	var prevLeftZone, prevRightZone *config.ZoneConfig
	for i := range ot.indexes {
		leftZone := ot.indexes[i].zone
		rightZone := otherTable.indexes[i].zone
		if leftZone == prevLeftZone && rightZone == prevRightZone {
			continue
		}
		if !zonesAreEqual(leftZone, rightZone) {
			return false
		}
		prevLeftZone = leftZone
		prevRightZone = rightZone
	}

	return true
}

// isStale checks if the optTable object needs to be refreshed because the stats
// or zone config have changed. False positives are ok.
func (ot *optTable) isStaleXQ(
	tableStats []*stats.TableStatistic, locationMap *roachpb.LocationMap,
) bool {
	// Fast check to verify that the statistics haven't changed: we check the
	// length and the address of the underlying array. This is not a perfect
	// check (in principle, the stats could have left the cache and then gotten
	// regenerated), but it works in the common case.
	if len(tableStats) != len(ot.rawStats) {
		return true
	}
	if len(tableStats) > 0 && &tableStats[0] != &ot.rawStats[0] {
		return true
	}
	if !locationMap.Equal(ot.locationMap) {
		return true
	}
	return false
}

// Name is part of the cat.DataSource interface.
func (ot *optTable) Name() *cat.DataSourceName {
	return &ot.name
}

// GetOwner is part of the cat.DataSource interface.
func (ot *optTable) GetOwner() string {
	return getOwnerOfDesc(ot.desc)
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return ot.desc.IsVirtualTable()
}

// IsMaterializedView implements the cat.Table interface.
func (ot *optTable) IsMaterializedView() bool {
	return ot.desc.MaterializedView()
}

// IsInterleaved is part of the cat.Table interface.
func (ot *optTable) IsInterleaved() bool {
	return ot.desc.IsInterleaved()
}

// IsReferenced is part of the cat.Table interface.
func (ot *optTable) IsReferenced() bool {
	for i, n := 0, ot.DeletableIndexCount(); i < n; i++ {
		if len(ot.Index(i).(*optIndex).desc.ReferencedBy) != 0 {
			return true
		}
	}
	return false
}

// ColumnCount is part of the cat.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.desc.Columns)
}

// WritableColumnCount is part of the cat.Table interface.
func (ot *optTable) WritableColumnCount() int {
	return len(ot.desc.WritableColumns())
}

// DeletableColumnCount is part of the cat.Table interface.
func (ot *optTable) DeletableColumnCount() int {
	return len(ot.desc.DeletableColumns())
}

// Column is part of the cat.Table interface.
func (ot *optTable) Column(i int) cat.Column {
	return &ot.desc.DeletableColumns()[i]
}

// ColumnByIDAndName return a Column by colID.
func (ot *optTable) ColumnByIDAndName(colID int, colName string) cat.Column {
	for _, oCol := range ot.desc.DeletableColumns() {
		if oCol.ID == sqlbase.ColumnID(colID) {
			return &oCol
		}
		if oCol.Name == colName {
			return &oCol
		}
	}
	return nil
}

// IndexCount is part of the cat.Table interface.
func (ot *optTable) IndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optTable) WritableIndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.WritableIndexes())
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optTable) DeletableIndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.DeletableIndexes())
}

// Index is part of the cat.Table interface.
func (ot *optTable) Index(i int) cat.Index {
	return &ot.indexes[i]
}

// Index1 is part of the cat.Table interface.
func (ot *optTable) Index1(i int) cat.Index {
	for _, index := range ot.indexes {
		if index.desc.IsFunc != nil {
			for _, isfunc := range index.desc.IsFunc {
				if isfunc {
					return &ot.indexes[0]
				}
			}
		}
	}
	return &ot.indexes[i]
}

// StatisticCount is part of the cat.Table interface.
func (ot *optTable) StatisticCount() int {
	return len(ot.stats)
}

// Statistic is part of the cat.Table interface.
func (ot *optTable) Statistic(i int) cat.TableStatistic {
	return &ot.stats[i]
}

// CheckCount is part of the cat.Table interface.
func (ot *optTable) CheckCount() int {
	return len(ot.desc.ActiveChecks())
}

// Check is part of the cat.Table interface.
func (ot *optTable) Check(i int) cat.CheckConstraint {
	check := ot.desc.ActiveChecks()[i]
	if check.Able {
		return "true"
	}
	return cat.CheckConstraint(check.Expr)
}

// FamilyCount is part of the cat.Table interface.
func (ot *optTable) FamilyCount() int {
	return 1 + len(ot.families)
}

// Family is part of the cat.Table interface.
func (ot *optTable) Family(i int) cat.Family {
	if i == 0 {
		return &ot.primaryFamily
	}
	return &ot.families[i-1]
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optTable) lookupColumnOrdinal(colID sqlbase.ColumnID) (int, error) {
	col, ok := ot.colMap[colID]
	if ok {
		return col, nil
	}
	return col, pgerror.NewErrorf(pgcode.UndefinedColumn,
		"column [%d] does not exist", colID)
}

func (ot *optTable) isSystemTable(name string) bool {
	for _, item := range SysTemTable {
		if item == name {
			return true
		}
	}
	return false
}

func (ot *optTable) GetInhertisBy() []uint32 {
	inheritsBy := make([]uint32, 0)
	descInherits := ot.desc.TableDescriptor.GetInheritsBy()

	if descInherits == nil {
		return nil
	}
	for _, v := range descInherits {
		// use uint32 to avoid importing package loop
		// v's type is tree.ID, an alias of uint32
		inheritsBy = append(inheritsBy, uint32(v))
	}
	return inheritsBy
}

func (ot *optTable) DataStoreEngineInfo() cat.DataStoreEngine {
	var engineTypeStrSlice []string
	isSysTable := ot.isSystemTable(string(ot.Name().TableName))
	if v, ok := ot.desc.EngineTypeSetMap[0]; !ok {
		if v == nil {
			if !isSysTable {
				// fake set by assigned const value;
				engineTypeStrSlice = append(engineTypeStrSlice, "KVStore", "ColumnStore")
			}
		} else {
			copy(engineTypeStrSlice, v.Value)
		}
	}
	var eType, tmp cat.EngineTypeSet
	for _, item := range engineTypeStrSlice {
		if item == "KVStore" {
			tmp = 1
		}
		if item == "ColumnStore" {
			tmp = 2
		}
		eType = eType | tmp
	}
	//if (!isSysTable) {
	//	fmt.Println("\n *********************", ot.Name().TableName, "***************** \n")
	//	if eType == 1 {
	//		fmt.Println("\n only support kv store！ \n")
	//	}
	//	if eType == 2 {
	//		fmt.Println("\n only support column store！\n")
	//	}
	//	if eType == 3 {
	//		fmt.Println("\n support all store engine! \n")
	//	}
	//}
	return cat.DataStoreEngine{ETypeSet: eType}
}

func (ot *optTableXQ) Index(i int) cat.IndexXQ {
	return &ot.indexXQs[i]
}

func (ot *optTableXQ) Table() cat.Table {
	return ot.optTable
}

// GetOrdinal is part of the cat.Index interface.
func (oi *optIndex) GetOrdinal() int {
	return oi.indexOrdinal
}

// optIndex is a wrapper around sqlbase.IndexDescriptor that caches some
// commonly accessed information and keeps a reference to the table wrapper.
type optIndex struct {
	tab  *optTable
	desc *sqlbase.IndexDescriptor
	zone *config.ZoneConfig

	// storedCols is the set of non-PK columns if this is the primary index,
	// otherwise it is desc.StoreColumnIDs.
	storedCols []sqlbase.ColumnID

	indexOrdinal  int
	numCols       int
	numKeyCols    int
	numLaxKeyCols int

	// foreignKey stores IDs of another table and one of its indexes,
	// if this index is part of an outbound foreign key relation.
	foreignKey cat.ForeignKeyReference
}

type optIndexXQ struct {
	*optIndex
	locatespace *roachpb.LocationValue
}

var _ cat.Index = &optIndex{}
var _ cat.IndexXQ = &optIndexXQ{}

func (oiXQ *optIndexXQ) LocateSpace() *roachpb.LocationValue {
	return oiXQ.locatespace
}

func (oiXQ *optIndexXQ) Index() cat.Index {
	return oiXQ.optIndex
}

// init can be used instead of newOptIndex when we have a pre-allocated instance
// (e.g. as part of a bigger struct).
func (oi *optIndex) init(
	tab *optTable, indexOrdinal int, desc *sqlbase.IndexDescriptor, zone *config.ZoneConfig,
) {
	oi.tab = tab
	oi.desc = desc
	oi.zone = zone
	oi.indexOrdinal = indexOrdinal
	if desc == &tab.desc.PrimaryIndex {
		// Although the primary index contains all columns in the table, the index
		// descriptor does not contain columns that are not explicitly part of the
		// primary key. Retrieve those columns from the table descriptor.
		oi.storedCols = make([]sqlbase.ColumnID, 0, tab.DeletableColumnCount()-len(desc.ColumnIDs))
		var pkCols util.FastIntSet
		for i := range desc.ColumnIDs {
			pkCols.Add(int(desc.ColumnIDs[i]))
		}
		for i, n := 0, tab.DeletableColumnCount(); i < n; i++ {
			id := tab.Column(i).ColID()
			if !pkCols.Contains(int(id)) {
				oi.storedCols = append(oi.storedCols, sqlbase.ColumnID(id))
			}
		}
		oi.numCols = tab.DeletableColumnCount()
	} else {
		oi.storedCols = desc.StoreColumnIDs
		oi.numCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs) + len(desc.StoreColumnIDs)
	}

	if desc.Unique {
		notNull := true
		for _, id := range desc.ColumnIDs {
			ord, _ := tab.lookupColumnOrdinal(id)
			if tab.desc.DeletableColumns()[ord].Nullable {
				notNull = false
				break
			}
		}

		if notNull {
			// Unique index with no null columns: columns from index are sufficient
			// to form a key without needing extra primary key columns. There is no
			// separate lax key.
			oi.numLaxKeyCols = len(desc.ColumnIDs)
			oi.numKeyCols = oi.numLaxKeyCols
		} else {
			// Unique index with at least one nullable column: extra primary key
			// columns will be added to the row key when one of the unique index
			// columns has a NULL value.
			oi.numLaxKeyCols = len(desc.ColumnIDs)
			oi.numKeyCols = oi.numLaxKeyCols + len(desc.ExtraColumnIDs)
		}
	} else {
		// Non-unique index: extra primary key columns are always added to the row
		// key. There is no separate lax key.
		oi.numLaxKeyCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs)
		oi.numKeyCols = oi.numLaxKeyCols
	}

	if desc.ForeignKey.IsSet() {
		oi.foreignKey.TableID = cat.StableID(desc.ForeignKey.Table)
		oi.foreignKey.IndexID = cat.StableID(desc.ForeignKey.Index)
		oi.foreignKey.PrefixLen = desc.ForeignKey.SharedPrefixLen
		oi.foreignKey.Validated = (desc.ForeignKey.Validity == sqlbase.ConstraintValidity_Validated)
		oi.foreignKey.Match = sqlbase.ForeignKeyReferenceMatchValue[desc.ForeignKey.Match]
	}
}

// ID is part of the cat.Index interface.
func (oi *optIndex) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Index interface.
func (oi *optIndex) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// IsUnique is part of the cat.Index interface.
func (oi *optIndex) IsUnique() bool {
	return oi.desc.Unique
}

// IsFunc is part of the cat.Index interface.
func (oi *optIndex) IsFunc() bool {
	for _, isfunc := range oi.desc.IsFunc {
		if isfunc {
			return true
		}
	}
	return false
}

func (oi *optIndex) ColumnIsFunc(i int) bool {
	if oi.desc.IsFunc != nil {
		if i >= len(oi.desc.IsFunc) {
			return false
		}
		if oi.desc.IsFunc[i] {
			return true
		}
	}
	return false
}

// GetColumnNames is part of the cat.Index interface.
func (oi *optIndex) GetColumnNames() []string {
	return oi.desc.ColumnNames
}

// GetColumnID is part of the cat.Index interface.
func (oi *optIndex) GetColumnID() map[int]string {
	columnMap := make(map[int]string)
	for _, column := range oi.tab.desc.Columns {
		columnMap[int(column.ID)] = column.Name
	}
	return columnMap
}

// IsInverted is part of the cat.Index interface.
func (oi *optIndex) IsInverted() bool {
	return oi.desc.Type == sqlbase.IndexDescriptor_INVERTED
}

// ColumnCount is part of the cat.Index interface.
func (oi *optIndex) ColumnCount() int {
	return oi.numCols
}

// KeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) KeyColumnCount() int {
	return oi.numKeyCols
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) LaxKeyColumnCount() int {
	return oi.numLaxKeyCols
}

// ColumnGetID is part of the cat.Index interface.
func (oi *optIndex) ColumnGetID(i int, ord int) int {
	return int(oi.desc.ColumnIDs[i])
}

// ColumnForFunc is part of the cat.Index interface.
func (oi *optIndex) ColumnForFunc(i int) cat.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	if i < length {
		if oi.desc.IsFunc != nil {
			if oi.desc.IsFunc[i] {
				FuncColumn := &sqlbase.ColumnDescriptor{
					ID:   oi.desc.ColumnIDs[i],
					Name: oi.desc.ColumnNames[i],
				}
				return cat.IndexColumn{
					Column:     FuncColumn,
					Ordinal:    int(oi.desc.ColumnIDs[i]) - 1,
					Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
				}
			}
		}
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return cat.IndexColumn{
			Column:     oi.tab.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		}
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
	}

	i -= length
	ord, _ := oi.tab.lookupColumnOrdinal(oi.storedCols[i])
	return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Column is part of the cat.Index interface.
func (oi *optIndex) Column(i int) cat.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return cat.IndexColumn{
			Column:     oi.tab.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		}
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
	}

	i -= length
	ord, _ := oi.tab.lookupColumnOrdinal(oi.storedCols[i])
	return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Column is part of the cat.Index interface.
func (oi *optIndex) ColumnsForFunc(i int) []cat.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	IndexColumns := make([]cat.IndexColumn, 0)
	if i < length {
		if oi.desc.IsFunc != nil {
			if oi.desc.IsFunc[i] {
				expr, _ := parser.ParseExpr(oi.desc.ColumnNames[i])
				for _, e := range expr.(*tree.FuncExpr).Exprs {
					unresolveexpr, ok := e.(*tree.UnresolvedName)
					if !ok {
						break
					}
					for numparts := 0; numparts < unresolveexpr.NumParts; numparts++ {
						column := unresolveexpr.Parts[numparts]
						for _, tablecolumn := range oi.tab.desc.Columns {
							if column == tablecolumn.Name {
								ord, _ := oi.tab.lookupColumnOrdinal(tablecolumn.ID)
								IndexColumns = append(IndexColumns, cat.IndexColumn{
									Column:     oi.tab.Column(ord),
									Ordinal:    ord,
									Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
								})
							}
						}
						return IndexColumns
					}
				}
			}
		}
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		IndexColumns = append(IndexColumns, cat.IndexColumn{
			Column:     oi.tab.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		})
		return IndexColumns
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		IndexColumns = append(IndexColumns, cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord})
		return IndexColumns
	}

	i -= length
	ord, _ := oi.tab.lookupColumnOrdinal(oi.storedCols[i])
	IndexColumns = append(IndexColumns, cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord})
	return IndexColumns
}

// ForeignKey is part of the cat.Index interface.
func (oi *optIndex) ForeignKey() (cat.ForeignKeyReference, bool) {
	return oi.foreignKey, oi.foreignKey.TableID != 0
}

// Zone is part of the cat.Index interface.
func (oi *optIndex) Zone() cat.Zone {
	return oi.zone
}

// Table is part of the cat.Index interface.
func (oi *optIndex) Table() cat.Table {
	return oi.tab
}

// Predicate is part of the cat.Index interface. It returns the predicate
// expression and true if the index is a partial index. If the index is not
// partial, the empty string and false is returned.
func (oi *optIndex) Predicate() (string, bool) {
	return oi.desc.PredExpr, oi.desc.PredExpr != ""
}

type optTableStat struct {
	columnOrdinals []int
	stats          stats.TableStatistic
}

var _ cat.TableStatistic = &optTableStat{}

func (os *optTableStat) init(tab *optTable, stat *stats.TableStatistic) (ok bool) {
	os.stats = *stat
	os.columnOrdinals = make([]int, len(stat.ColumnIDs))
	for i, c := range stat.ColumnIDs {
		var ok bool
		os.columnOrdinals[i], ok = tab.colMap[c]
		if !ok {
			// Column not in table (this is possible if the column was removed since
			// the statistic was calculated).
			return false
		}
	}
	return true
}

func (os *optTableStat) equals(other *optTableStat) bool {
	// Two table statistics are considered equal if they have been created at the
	// same time, on the same set of columns.
	if os.stats.CreatedAt != other.stats.CreatedAt || len(os.columnOrdinals) != len(other.columnOrdinals) {
		return false
	}
	for i, c := range os.columnOrdinals {
		if c != other.columnOrdinals[i] {
			return false
		}
	}
	return true
}

// CreatedAt is part of the cat.TableStatistic interface.
func (os *optTableStat) CreatedAt() time.Time {
	return os.stats.CreatedAt
}

// ColumnCount is part of the cat.TableStatistic interface.
func (os *optTableStat) ColumnCount() int {
	return len(os.columnOrdinals)
}

// ColumnOrdinal is part of the cat.TableStatistic interface.
func (os *optTableStat) ColumnOrdinal(i int) int {
	return os.columnOrdinals[i]
}

// RowCount is part of the cat.TableStatistic interface.
func (os *optTableStat) RowCount() uint64 {
	return os.stats.RowCount
}

// DistinctCount is part of the cat.TableStatistic interface.
func (os *optTableStat) DistinctCount() uint64 {
	return os.stats.DistinctCount
}

// NullCount is part of the cat.TableStatistic interface.
func (os *optTableStat) NullCount() uint64 {
	return os.stats.NullCount
}

func (os *optTableStat) Histogram() []cat.HistogramBucket {
	if os.stats.Histogram == nil {
		return nil
	}
	histogram := *(os.stats.Histogram)
	bs := make([]cat.HistogramBucket, 0, len(histogram.Buckets))
	for _, b := range histogram.Buckets {
		var a sqlbase.DatumAlloc
		typ := histogram.ColumnType.ToDatumType()
		upperBound, _, err1 := sqlbase.DecodeTableKey(&a, typ, b.UpperBound, encoding.Ascending)
		lowerBound, _, err2 := sqlbase.DecodeTableKey(&a, typ, b.LowerBound, encoding.Ascending)
		if err1 != nil || err2 != nil {
			return nil
		}
		bs = append(bs, cat.HistogramBucket{
			NumEq:         float64(b.NumEq),
			NumRange:      float64(b.NumRange),
			DistinctRange: b.DistinctRange,
			UpperBound:    upperBound,
			LowerBound:    lowerBound,
		})
	}
	return bs
}

// optFamily is a wrapper around sqlbase.ColumnFamilyDescriptor that keeps a
// reference to the table wrapper.
type optFamily struct {
	tab  *optTable
	desc *sqlbase.ColumnFamilyDescriptor
}

var _ cat.Family = &optFamily{}

// init can be used instead of newOptFamily when we have a pre-allocated
// instance (e.g. as part of a bigger struct).
func (oi *optFamily) init(tab *optTable, desc *sqlbase.ColumnFamilyDescriptor) {
	oi.tab = tab
	oi.desc = desc
}

// ID is part of the cat.Family interface.
func (oi *optFamily) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Family interface.
func (oi *optFamily) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// ColumnCount is part of the cat.Family interface.
func (oi *optFamily) ColumnCount() int {
	return len(oi.desc.ColumnIDs)
}

// Column is part of the cat.Family interface.
func (oi *optFamily) Column(i int) cat.FamilyColumn {
	ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
	return cat.FamilyColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Table is part of the cat.Family interface.
func (oi *optFamily) Table() cat.Table {
	return oi.tab
}

// zonesAreEqual compares two zones for equality. Note that only fields actually
// exposed by the cat.Zone interface and needed by the optimizer are compared.
func zonesAreEqual(left, right *config.ZoneConfig) bool {
	if left == right {
		return true
	}
	if len(left.Constraints) != len(right.Constraints) {
		return false
	}
	if len(left.Subzones) != len(right.Subzones) {
		return false
	}
	if len(left.LeasePreferences) != len(right.LeasePreferences) {
		return false
	}

	for i := range left.Subzones {
		leftSubzone := &left.Subzones[i]
		rightSubzone := &right.Subzones[i]

		// Skip subzones that only apply to one partition of an index, since
		// they're also skipped in newOptTable.
		if len(leftSubzone.PartitionName) != 0 && len(rightSubzone.PartitionName) != 0 {
			continue
		}

		if leftSubzone.IndexID != rightSubzone.IndexID {
			return false
		}

		return zonesAreEqual(&leftSubzone.Config, &rightSubzone.Config)
	}

	for i := range left.Constraints {
		leftReplCons := &left.Constraints[i]
		rightReplCons := &right.Constraints[i]
		if leftReplCons.NumReplicas != rightReplCons.NumReplicas {
			return false
		}
		if !constraintsAreEqual(leftReplCons.Constraints, rightReplCons.Constraints) {
			return false
		}
	}

	for i := range left.LeasePreferences {
		leftLeasePrefs := &left.LeasePreferences[i]
		rightLeasePrefs := &right.LeasePreferences[i]
		if !constraintsAreEqual(leftLeasePrefs.Constraints, rightLeasePrefs.Constraints) {
			return false
		}
	}

	return true
}

// constraintsAreEqual compares two sets of constraints for equality.
func constraintsAreEqual(left, right []config.Constraint) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		leftCons := &left[i]
		rightCons := &right[i]
		if leftCons.Type != rightCons.Type {
			return false
		}
		if leftCons.Key != rightCons.Key {
			return false
		}
		if leftCons.Value != rightCons.Value {
			return false
		}
	}
	return true
}

func getDescForDataSource(o cat.DataSource) (*sqlbase.ImmutableTableDescriptor, error) {
	switch t := o.(type) {
	case *optTable:
		return t.desc, nil
	//case *optVirtualTable:
	//	return t.desc, nil
	case *optView:
		return t.desc, nil
	case *optSequence:
		return t.desc, nil
	default:
		return nil, errors.AssertionFailedf("invalid object type: %T", o)
	}
}

// CheckAnyPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckAnyPrivilege(ctx context.Context, o cat.Object) error {
	switch t := o.(type) {
	case *optTable:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	case *optView:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	case *optSequence:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	case *optSchema:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	case *optTableXQ:
		return oc.planner.CheckAnyPrivilege(ctx, t.desc)
	default:
		return pgerror.NewAssertionErrorf("invalid object type: %T", o)
	}
}

// HasRoleOption is part of the cat.Catalog interface.
func (oc *optCatalog) HasRoleOption(ctx context.Context, action string) error {
	return oc.planner.RequireAdminRole(ctx, action)
}

// optFunction is wrapper function descriptor for optcatalog
type optUdrFunction struct {
	// desc is descriptor of a opt function
	desc *sqlbase.ImmutableFunctionDescriptor
	// name is name of this function group
	name cat.DataSourceName
	// groupNumber is number of this function group
	groupNumber int
}

// ID is part of the cat.optUdrFunction interface.
func (of *optUdrFunction) ID() cat.StableID {
	return cat.StableID(of.desc.FunctionLeaderID)
}

// Name is part of the UdrFunction interface.
func (of *optUdrFunction) Name() *cat.DataSourceName {
	return &of.name
}

// GetOwner is part of the cat.DataSource interface.
// function does not involve the use of owner.
// FuncGroup is a group of function desc with same name, but different owner.
func (of *optUdrFunction) GetOwner() string {
	return ""
}

// Equals is part of the cat.Object interface.
func (of *optUdrFunction) Equals(other cat.Object) bool {
	return false
}

//
func (of *optUdrFunction) GroupNumber() int {
	return of.groupNumber
}

func newOptFunction(
	desc *sqlbase.ImmutableFunctionDescriptor, name cat.DataSourceName,
) *optUdrFunction {
	funcLen := 0
	if desc != nil {
		funcLen = len(desc.FuncGroup)
	}

	of := &optUdrFunction{
		desc:        desc,
		name:        name,
		groupNumber: funcLen,
	}
	return of
}

// CheckFunctionPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckFunctionPrivilege(ctx context.Context, ds cat.DataSource) []error {
	if err := oc.planner.checkPrivilegeAccessToDataSource(ctx, ds.Name()); err != nil {
		return []error{err}
	}
	funcs := ds.(*optUdrFunction)
	errs := make([]error, 0, len(funcs.desc.FuncGroup))
	for _, f := range funcs.desc.FuncGroup {
		errs = append(errs, oc.planner.CheckPrivilege(ctx, f.Desc, privilege.EXECUTE))
	}
	return errs
}
