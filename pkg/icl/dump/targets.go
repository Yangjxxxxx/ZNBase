package dump

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

var fullClusterSystemTables = []string{
	sqlbase.UsersTable.Name,
	sqlbase.ZonesTable.Name,
	sqlbase.SettingsTable.Name,
	sqlbase.LocationTable.Name,
	sqlbase.LeaseTable.Name,
	sqlbase.EventLogTable.Name,
	sqlbase.RangeEventTable.Name,
	sqlbase.UITable.Name,
	sqlbase.JobsTable.Name,
	sqlbase.WebSessionsTable.Name,
	sqlbase.TableStatisticsTable.Name,
	sqlbase.LocationsTable.Name,
	sqlbase.RoleMembersTable.Name,
	sqlbase.CommentsTable.Name,
	sqlbase.SnapshotsTable.Name,
	sqlbase.AuthenticationTable.Name,
}

type matchedDescriptors struct {
	// all tables that match targets plus their parent databases.
	descs []sqlbase.Descriptor

	// the schemas from which all tables were matched (eg a.* or schema a).
	expandedSCH []sqlbase.ID
	// explicitly requested schemas (e.g. schema a).
	requestedSCs []*sqlbase.SchemaDescriptor
	// the databases from which all tables were matched (eg a.* or DATABASE a).
	expandedDB []sqlbase.ID

	// explicitly requested DBs (e.g. DATABASE a).
	requestedDBs []*sqlbase.DatabaseDescriptor
}

// ResolveTargetsToDescriptors is that resolve targets to descriptors
func ResolveTargetsToDescriptors(
	ctx context.Context,
	p sql.PlanHookState,
	endTime hlc.Timestamp,
	targets tree.TargetList,
	descriptorCoverage tree.DescriptorCoverage,
	dumpDropTable bool,
) ([]sqlbase.Descriptor, []sqlbase.ID, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	defer f.Close()
	//tree.TargetList.Format(f)
	//stmt, err := parser.ParseOne(target)

	allDescs, err := loadAllDescs(ctx, p.ExecCfg().DB, endTime)
	if err != nil {
		return nil, nil, err
	}

	if descriptorCoverage == tree.AllDescriptors {
		return fullClusterTargetsBackup(allDescs)
	}

	resolver := &sqlDescriptorResolver{
		allDescs:   allDescs,
		descByID:   make(map[sqlbase.ID]sqlbase.Descriptor),
		dbsByName:  make(map[string]sqlbase.ID),
		schsByName: make(map[string]sqlbase.ID),
		scsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
		tbsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
		tbsByID:    make(map[sqlbase.ID]map[sqlbase.ID]string),
	}
	err = resolver.initResolver(dumpDropTable)
	if err != nil {
		return nil, nil, err
	}

	var matched matchedDescriptors
	if matched, err = resolver.findMatchedDescriptors(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets); err != nil {
		return nil, nil, err
	}

	// Ensure interleaved tables appear after their parent. Since parents must be
	// created before their children, simply sorting by ID accomplishes this.
	sort.Slice(matched.descs, func(i, j int) bool { return matched.descs[i].GetID() < matched.descs[j].GetID() })
	return matched.descs, matched.expandedDB, nil
}

func loadAllDescs(
	ctx context.Context, db *client.DB, asOf hlc.Timestamp,
) ([]sqlbase.Descriptor, error) {
	var allDescs []sqlbase.Descriptor
	if err := db.Txn(
		ctx,
		func(ctx context.Context, txn *client.Txn) error {
			var err error
			txn.SetFixedTimestamp(ctx, asOf)

			startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
			endKey := startKey.PrefixEnd()
			rows, err := txn.Scan(ctx, startKey, endKey, 0)
			if err != nil {
				return err
			}
			allDescs = make([]sqlbase.Descriptor, len(rows))
			for i, row := range rows {
				err := row.ValueProto(&allDescs[i])
				if err != nil {
					return errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
				}
				if tableDesc := allDescs[i].GetTable(); tableDesc != nil {
					allDescs[i].SetModificationTime(row.Value.Timestamp)
				}

			}
			return nil
		}); err != nil {
		return nil, err
	}
	return allDescs, nil
}

// fullClusterTargetsBackup returns the same descriptors referenced in
// fullClusterTargets, but rather than returning the entire database
// descriptor as the second argument, it only returns their IDs.
func fullClusterTargetsBackup(
	allDescs []sqlbase.Descriptor,
) ([]sqlbase.Descriptor, []sqlbase.ID, error) {
	fullClusterDescs, fullClusterDBs, err := fullClusterTargets(allDescs)
	if err != nil {
		return nil, nil, err
	}

	fullClusterDBIDs := make([]sqlbase.ID, 0)
	for _, desc := range fullClusterDBs {
		fullClusterDBIDs = append(fullClusterDBIDs, desc.GetID())
	}
	return fullClusterDescs, fullClusterDBIDs, nil
}

// fullClusterTargets returns all of the tableDescriptors to be included in a
// full cluster backup, and all the user databases.
func fullClusterTargets(
	allDescs []sqlbase.Descriptor,
) ([]sqlbase.Descriptor, []*sqlbase.DatabaseDescriptor, error) {
	fullClusterDescs := make([]sqlbase.Descriptor, 0, len(allDescs))
	fullClusterDBs := make([]*sqlbase.DatabaseDescriptor, 0)

	systemTablesToBackup := make(map[string]struct{}, len(fullClusterSystemTables))
	for _, tableName := range fullClusterSystemTables {
		systemTablesToBackup[tableName] = struct{}{}
	}

	for _, desc := range allDescs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			fullClusterDescs = append(fullClusterDescs, desc)
			fullClusterDBs = append(fullClusterDBs, dbDesc)
		}
		if scDesc := desc.GetSchema(); scDesc != nil {
			fullClusterDescs = append(fullClusterDescs, desc)
		}
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			if tableDesc.ParentID == sqlbase.SystemDB.ID {
				// Add only the system tables that we plan to include in a full cluster
				// backup.
				if _, ok := systemTablesToBackup[tableDesc.Name]; ok {
					fullClusterDescs = append(fullClusterDescs, desc)
				}
			} else {
				// Add all user tables that are not in a DROP state.
				if tableDesc.State != sqlbase.TableDescriptor_DROP {
					fullClusterDescs = append(fullClusterDescs, desc)
				}
			}
		}
	}
	return fullClusterDescs, fullClusterDBs, nil
}

type sqlDescriptorResolver struct {
	// all sqlbase.Descriptor
	allDescs []sqlbase.Descriptor

	descByID map[sqlbase.ID]sqlbase.Descriptor
	// Map: db name -> dbID
	dbsByName map[string]sqlbase.ID
	//Map:sch name -> schID
	schsByName map[string]sqlbase.ID
	// Map: dbID -> schema name -> scID
	scsByName map[sqlbase.ID]map[string]sqlbase.ID
	// Map: scID / system -> table name -> tbID
	tbsByName map[sqlbase.ID]map[string]sqlbase.ID
	// Map: scID / system -> tbID -> table name
	tbsByID map[sqlbase.ID]map[sqlbase.ID]string
}

func (r *sqlDescriptorResolver) initResolver(dumpDropTable bool) error {
	schemaMap := make(map[sqlbase.ID]*sqlbase.SchemaDescriptor)
	// 添加系统表schema
	//r.scsByName[sqlbase.SystemDB.ID] = make(map[string]sqlbase.ID)
	//r.scsByName[sqlbase.SystemDB.ID]["public"] = sqlbase.SystemDB.ID
	// 记录ID->descriptor映射，及数据库名->ID的映射
	for _, desc := range r.allDescs {
		// Incidentally, also remember all the descriptors by ID.
		if existDesc, ok := r.descByID[desc.GetID()]; ok {
			return errors.Errorf("duplicate descriptor ID: %d used by %q and %q",
				desc.GetID(), existDesc.GetName(), desc.GetName())
		}
		r.descByID[desc.GetID()] = desc

		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if _, ok := r.dbsByName[dbDesc.Name]; ok {
				return errors.Errorf("duplicate database name: %q used for ID %d and %d",
					dbDesc.Name, r.dbsByName[dbDesc.Name], dbDesc.ID)
			}
			r.dbsByName[dbDesc.Name] = dbDesc.ID
		}
	}

	// Now on to the schemas.
	for _, desc := range r.allDescs {
		if scDesc := desc.GetSchema(); scDesc != nil {
			parentDesc, ok := r.descByID[scDesc.ParentID]
			if !ok {
				return errors.Errorf("schema %q has unknown ParentID %d", scDesc.Name, scDesc.ParentID)
			}
			if _, ok := r.dbsByName[parentDesc.GetName()]; !ok {
				return errors.Errorf("schema %q's ParentID %d (%q) is not a database",
					scDesc.Name, scDesc.ParentID, parentDesc.GetName())
			}
			scMap := r.scsByName[parentDesc.GetID()]
			if scMap == nil {
				scMap = make(map[string]sqlbase.ID)
			}
			if _, ok := scMap[scDesc.Name]; ok {
				return errors.Errorf("duplicate schema name: %q.%q used for ID %d and %d",
					parentDesc.GetName(), scDesc.Name, scDesc.ID, scMap[scDesc.Name])
			}
			scMap[scDesc.Name] = scDesc.ID
			schemaMap[scDesc.ID] = scDesc
			r.schsByName[scDesc.Name] = scDesc.ID
			r.scsByName[parentDesc.GetID()] = scMap
		}
	}

	// Now on to the tables.
	for _, desc := range r.allDescs {
		if tbDesc := desc.Table(hlc.Timestamp{}); tbDesc != nil {

			//Todo:当前仅支持备份已删除的表所的的模式存在时，如表已删除且他所在的模式也被删除，则无法备份
			if tbDesc.ParentID > sqlbase.SystemDB.ID {
				_, ok := schemaMap[tbDesc.ParentID]
				if (tbDesc.Dropped() && !dumpDropTable) || !ok {
					continue
				}
			}

			parentDesc, ok := r.descByID[tbDesc.ParentID]
			if !ok {
				return errors.Errorf("table %q has unknown ParentID %d", tbDesc.Name, tbDesc.ParentID)
			}
			if parentDesc.GetID() != sqlbase.SystemDB.ID {
				if schema := parentDesc.GetSchema(); schema == nil {
					return errors.Errorf("table %q's ParentID %d (%q) is not a schema",
						tbDesc.Name, tbDesc.ParentID, parentDesc.GetName())
				}
			}
			tbMap := r.tbsByID[parentDesc.GetID()]
			if tbMap == nil {
				tbMap = make(map[sqlbase.ID]string)
			}
			if _, ok := tbMap[tbDesc.ID]; ok {
				return errors.Errorf("duplicate table name: %q.%q used for ID %d",
					parentDesc.GetName(), tbDesc.Name, tbDesc.ID)
			}
			tbMap[tbDesc.ID] = tbDesc.Name
			r.tbsByID[parentDesc.GetID()] = tbMap
			tbNameMap := r.tbsByName[parentDesc.GetID()]
			if tbNameMap == nil {
				tbNameMap = make(map[string]sqlbase.ID)
			}
			tbNameMap[tbDesc.Name] = tbDesc.ID
			r.tbsByName[parentDesc.GetID()] = tbNameMap
		}
	}
	return nil
}

func (r *sqlDescriptorResolver) findMatchedDescriptors(
	ctx context.Context,
	currentDatabase string,
	searchPath sessiondata.SearchPath,
	descriptors []sqlbase.Descriptor,
	targets tree.TargetList,
) (matchedDescriptors, error) {

	// TODO(dan): once ZNBaseDB supports schemas in addition to
	// catalogs, then this method will need to support it.

	ret := matchedDescriptors{}

	alreadyRequestedDBs := make(map[sqlbase.ID]struct{})
	alreadyRequestedSCs := make(map[sqlbase.ID]struct{})
	alreadyExpandedSCs := make(map[sqlbase.ID]struct{})
	alreadyExpandedDBs := make(map[sqlbase.ID]struct{})

	// Process all the DATABASE requests.
	for _, d := range targets.Databases {
		dbID, ok := r.dbsByName[string(d)]
		if !ok {
			return ret, errors.Errorf("unknown database %q", d)
		}
		if _, ok := alreadyRequestedDBs[dbID]; !ok {
			desc := r.descByID[dbID]
			ret.descs = append(ret.descs, desc)
			ret.requestedDBs = append(ret.requestedDBs, desc.GetDatabase())
			ret.expandedDB = append(ret.expandedDB, dbID)
			alreadyRequestedDBs[dbID] = struct{}{}
			alreadyExpandedDBs[dbID] = struct{}{}
		}
	}

	for _, sch := range targets.Schemas {
		databaseName := sch.Catalog()
		if databaseName == "" {
			databaseName = currentDatabase
		}
		dbID, ok := r.dbsByName[databaseName]
		if !ok {
			return ret, errors.Errorf("unknown database %q", databaseName)
		}
		schMap, ok := r.scsByName[dbID]
		if !ok {
			return ret, errors.Errorf("unknown schema %q", sch.Schema())
		}
		schID, ok := schMap[sch.Schema()]
		if !ok {
			return ret, errors.Errorf("unknown schema %q", sch.Schema())
		}

		parentID := schID
		if _, ok := alreadyRequestedSCs[schID]; !ok {
			desc := r.descByID[schID]
			ret.descs = append(ret.descs, desc)
			parentID = desc.GetSchema().ParentID
			ret.requestedSCs = append(ret.requestedSCs, desc.GetSchema())
			ret.expandedSCH = append(ret.expandedSCH, schID)
			alreadyRequestedSCs[schID] = struct{}{}
			alreadyExpandedSCs[schID] = struct{}{}
		}
		if parentID != schID {
			if _, ok := alreadyRequestedDBs[parentID]; !ok {
				parentDesc := r.descByID[parentID]
				ret.descs = append(ret.descs, parentDesc)
				alreadyRequestedDBs[parentID] = struct{}{}
			}
		}

	}

	// Process all the TABLE requests.
	// Pulling in a table needs to pull in the underlying database too.
	alreadyRequestedTables := make(map[sqlbase.ID]struct{})
	if targets.Views != nil {
		targets.Tables = targets.Views
	}
	if targets.Sequences != nil {
		targets.Tables = targets.Sequences
	}
	for _, pattern := range targets.Tables {
		var err error
		pattern, err = pattern.NormalizeTablePattern()
		if err != nil {
			return ret, err
		}

		switch p := pattern.(type) {
		case *tree.TableName:
			found, descI, err := p.ResolveExisting(ctx, r, false /*requireMutable*/, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			if !found {
				return ret, errors.Errorf(`table %q does not exist`, tree.ErrString(p))
			}
			desc := descI.(sqlbase.Descriptor)

			// If the parent database is not requested already, request it now
			var schema *sqlbase.SchemaDescriptor
			parentID := desc.Table(hlc.Timestamp{}).GetParentID()
			if parentID != sqlbase.SystemDB.ID {
				sc, ok := r.descByID[parentID]
				if !ok {
					return ret, errors.Errorf(`The schema isn't found.`)
				}
				schema = sc.GetSchema()
				if schema == nil {
					return ret, errors.Errorf(`The schema does not exist.`)
				}
				parentID = schema.ParentID
			}
			if _, ok := alreadyRequestedDBs[parentID]; !ok {
				parentDesc := r.descByID[parentID]
				ret.descs = append(ret.descs, parentDesc)
				alreadyRequestedDBs[parentID] = struct{}{}
			}
			if schema != nil {
				if _, ok := alreadyRequestedSCs[schema.ID]; !ok {
					parentDesc := r.descByID[schema.ID]
					ret.descs = append(ret.descs, parentDesc)
					alreadyRequestedSCs[schema.ID] = struct{}{}
				}
			}
			for tblID, tblName := range r.tbsByID[desc.Table(hlc.Timestamp{}).ParentID] {
				if tblName == desc.Table(hlc.Timestamp{}).Name {
					if tblID != desc.Table(hlc.Timestamp{}).ID {
						ret.descs = append(ret.descs, r.descByID[tblID])
					}
				}
			}
			// Then request the table itself.
			if _, ok := alreadyRequestedTables[desc.GetID()]; !ok {
				alreadyRequestedTables[desc.GetID()] = struct{}{}
				ret.descs = append(ret.descs, desc)
			}

		case *tree.AllTablesSelector:
			found, descI, err := p.TableNamePrefix.Resolve(ctx, r, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			if !found {
				return ret, sqlbase.NewInvalidWildcardError(tree.ErrString(p))
			}
			desc := descI.(sqlbase.Descriptor)

			// If the database is not requested already, request it now.
			dbID := desc.GetID()
			if _, ok := alreadyRequestedDBs[dbID]; !ok {
				ret.descs = append(ret.descs, desc)
				alreadyRequestedDBs[dbID] = struct{}{}
			}

			// Then request the expansion.
			if _, ok := alreadyExpandedDBs[desc.GetID()]; !ok {
				ret.expandedDB = append(ret.expandedDB, desc.GetID())
				alreadyExpandedDBs[desc.GetID()] = struct{}{}
			}

		default:
			return ret, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	}

	// Then process the database expansions.
	for dbID := range alreadyExpandedDBs {
		if dbID == sqlbase.SystemDB.ID {
			for tblID := range r.tbsByID[dbID] {
				if _, ok := alreadyRequestedTables[tblID]; !ok {
					ret.descs = append(ret.descs, r.descByID[tblID])
				}
			}
		} else {
			for _, scID := range r.scsByName[dbID] {
				if scID != sqlbase.SystemDB.ID {
					if _, ok := alreadyRequestedSCs[scID]; !ok {
						ret.descs = append(ret.descs, r.descByID[scID])
						alreadyRequestedSCs[scID] = struct{}{}
					}
				}
				for tblID := range r.tbsByID[scID] {
					if _, ok := alreadyRequestedTables[tblID]; !ok {
						ret.descs = append(ret.descs, r.descByID[tblID])
					}
				}
			}
		}
	}
	// Then process the schemas expansions.
	for schID := range alreadyExpandedSCs {
		for _, tblID := range r.tbsByName[schID] {
			if _, ok := alreadyRequestedTables[tblID]; !ok {
				ret.descs = append(ret.descs, r.descByID[tblID])
			}
		}
	}
	return ret, nil

}

// LookupSchema implements the tree.TableNameTargetResolver interface.
func (r *sqlDescriptorResolver) LookupSchema(
	_ context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	if dbID, ok := r.dbsByName[dbName]; ok {
		return true, r.descByID[dbID], nil
	}
	return false, nil, nil
}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (r *sqlDescriptorResolver) LookupObject(
	_ context.Context, requireMutable bool, requireFunction bool, dbName, scName, tbName string,
) (bool, tree.NameResolutionResult, error) {
	var tableID sqlbase.ID
	if requireMutable {
		panic("did not expect request for mutable descriptor")
	}
	dbID, ok := r.dbsByName[dbName]
	if !ok {
		return false, nil, nil
	}
	if dbID == sqlbase.SystemDB.ID {
		if tbMap, ok := r.tbsByName[dbID]; ok {
			if tbID, ok := tbMap[tbName]; ok {
				return true, r.descByID[tbID], nil
			}
		}
	} else if scMap, ok := r.scsByName[dbID]; ok {
		if scID, ok := scMap[scName]; ok {
			if tbMap, ok := r.tbsByID[scID]; ok {
				for tblID, tblName := range tbMap {
					if tblName == tbName && tblID > tableID {
						tableID = tblID
					}
				}
				if _, ok := r.descByID[tableID]; ok {
					return true, r.descByID[tableID], nil
				}
			}
		}
	}
	return false, nil, nil
}

type descriptorsMatched struct {
	// all tables that match targets plus their parent databases.
	descs []sqlbase.Descriptor

	// the databases from which all tables were matched (eg a.* or SCHEMA a).
	expandedSCs []sqlbase.ID
	// explicitly requested DBs (e.g. SCHEMA a).
	requestedScs []*sqlbase.SchemaDescriptor

	// the databases from which all tables were matched (eg a.* or DATABASE a).
	expandedDB []sqlbase.ID

	// explicitly requested DBs (e.g. DATABASE a).
	requestedDBs []*sqlbase.DatabaseDescriptor
}

func (d descriptorsMatched) checkExpansions(coveredDBs []sqlbase.ID) error {
	covered := make(map[sqlbase.ID]bool)
	for _, i := range coveredDBs {
		covered[i] = true
	}
	for _, i := range d.requestedDBs {
		if !covered[i.ID] {
			return errors.Errorf("cannot LOAD DATABASE from a backup of individual tables (use SHOW BACKUP to determine available tables)")
		}
	}
	for _, i := range d.expandedDB {
		if !covered[i] {
			return errors.Errorf("cannot LOAD <database>.* from a backup of individual tables (use SHOW BACKUP to determine available tables)")
		}
	}
	return nil
}

// newDescriptorResolver prepares a descriptorResolver for the given
// known set of descriptors.
func newDescriptorResolver(
	descs []sqlbase.Descriptor, dumpDropTable bool,
) (*sqlDescriptorResolver, error) {
	r := &sqlDescriptorResolver{
		descByID:   make(map[sqlbase.ID]sqlbase.Descriptor),
		dbsByName:  make(map[string]sqlbase.ID),
		schsByName: make(map[string]sqlbase.ID),
		scsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
		tbsByName:  make(map[sqlbase.ID]map[string]sqlbase.ID),
		tbsByID:    make(map[sqlbase.ID]map[sqlbase.ID]string),
	}

	//r.scsByName[sqlbase.SystemDB.ID] = make(map[string]sqlbase.ID)
	//r.scsByName[sqlbase.SystemDB.ID]["public"] = sqlbase.SystemDB.ID

	// Iterate to find the databases first. We need that because we also
	// check the ParentID for tables, and all the valid parents must be
	// known before we start to check that.
	for _, desc := range descs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if _, ok := r.dbsByName[dbDesc.Name]; ok {
				return nil, errors.Errorf("duplicate database name: %q used for ID %d and %d",
					dbDesc.Name, r.dbsByName[dbDesc.Name], dbDesc.ID)
			}
			r.dbsByName[dbDesc.Name] = dbDesc.ID
		}

		// Incidentally, also remember all the descriptors by ID.
		if prevDesc, ok := r.descByID[desc.GetID()]; ok {
			return nil, errors.Errorf("duplicate descriptor ID: %d used by %q and %q",
				desc.GetID(), prevDesc.GetName(), desc.GetName())
		}
		r.descByID[desc.GetID()] = desc
	}

	// Now on to the schemas.
	for _, desc := range descs {
		if scDesc := desc.GetSchema(); scDesc != nil {
			parentDesc, ok := r.descByID[scDesc.ParentID]
			if !ok {
				return nil, errors.Errorf("schema %q has unknown ParentID %d", scDesc.Name, scDesc.ParentID)
			}
			if _, ok := r.dbsByName[parentDesc.GetName()]; !ok {
				return nil, errors.Errorf("schema %q's ParentID %d (%q) is not a database",
					scDesc.Name, scDesc.ParentID, parentDesc.GetName())
			}
			scMap := r.scsByName[parentDesc.GetID()]
			if scMap == nil {
				scMap = make(map[string]sqlbase.ID)
			}
			if _, ok := scMap[scDesc.Name]; ok {
				return nil, errors.Errorf("duplicate schema name: %q.%q used for ID %d and %d",
					parentDesc.GetName(), scDesc.Name, scDesc.ID, scMap[scDesc.Name])
			}
			scMap[scDesc.Name] = scDesc.ID
			r.schsByName[scDesc.Name] = scDesc.ID
			r.scsByName[parentDesc.GetID()] = scMap
		}
	}

	// Now on to the tables.
	for _, desc := range descs {
		if tbDesc := desc.Table(hlc.Timestamp{}); tbDesc != nil {
			if tbDesc.Dropped() && !dumpDropTable {
				continue
			}
			parentDesc, ok := r.descByID[tbDesc.ParentID]
			if !ok {
				return nil, errors.Errorf("table %q has unknown ParentID %d", tbDesc.Name, tbDesc.ParentID)
			}
			if parentDesc.GetID() != sqlbase.SystemDB.ID {
				if schema := parentDesc.GetSchema(); schema == nil {
					return nil, errors.Errorf("table %q's ParentID %d (%q) is not a schema",
						tbDesc.Name, tbDesc.ParentID, parentDesc.GetName())
				}
			}
			//if _, ok := r.dbsByName[parentDesc.GetName()]; !ok {
			//
			//}
			tbMap := r.tbsByID[parentDesc.GetID()]
			if tbMap == nil {
				tbMap = make(map[sqlbase.ID]string)
			}
			if _, ok := tbMap[tbDesc.ID]; ok {
				return nil, errors.Errorf("duplicate table name: %q.%q used for ID %d",
					parentDesc.GetName(), tbDesc.Name, tbDesc.ID)
			}
			tbMap[tbDesc.ID] = tbDesc.Name
			r.tbsByID[parentDesc.GetID()] = tbMap
			tbNameMap := r.tbsByName[parentDesc.GetID()]
			if tbNameMap == nil {
				tbNameMap = make(map[string]sqlbase.ID)
			}
			tbNameMap[tbDesc.Name] = tbDesc.ID
			r.tbsByName[parentDesc.GetID()] = tbNameMap
		}
	}

	return r, nil
}

// descriptorsMatchingTargets returns the descriptors that match the targets. A
// database descriptor is included in this set if it matches the targets (or the
// session database) or if one of its tables matches the targets. All expanded
// DBs, via either `foo.*` or `DATABASE foo` are noted, as are those explicitly
// named as DBs (e.g. with `DATABASE foo`, not `foo.*`). These distinctions are
// used e.g. by RESTORE.
//
// This is guaranteed to not return duplicates.
// 重构的函数
func descriptorsMatchingTargetsObject(
	ctx context.Context,
	phs sql.PlanHookState,
	currentDatabase string,
	searchPath sessiondata.SearchPath,
	descriptors []sqlbase.Descriptor,
	restoreStmt *tree.LoadRestore,
) (descriptorsMatched, error) {
	// TODO(dan): once CockroachDB supports schemas in addition to
	// catalogs, then this method will need to support it.

	ret := descriptorsMatched{}
	restoreDropTable := false
	if restoreStmt.AsOf.Expr != nil {
		restoreDropTable = true
	}
	resolver, err := newDescriptorResolver(descriptors, restoreDropTable)
	if err != nil {
		return ret, err
	}

	alreadyRequestedDBs := make(map[sqlbase.ID]struct{})
	alreadyRequestedSCs := make(map[sqlbase.ID]struct{})
	alreadyExpandedDBs := make(map[sqlbase.ID]struct{})
	alreadyExpandeSCs := make(map[sqlbase.ID]struct{})
	databaseName := restoreStmt.DatabaseName
	tableName := restoreStmt.TableName
	schemaName := restoreStmt.SchemaName
	// Process all the DATABASE requests.
	//重构后只有单库情况，不是[]string类型
	//for _, d := range DatabaseName {
	if databaseName != "" {
		err := phs.RequireAdminRole(ctx, "LOAD DATABASE")
		if err != nil {
			return ret, err
		}
		d := databaseName
		dbID, ok := resolver.dbsByName[d]
		if !ok {
			return ret, errors.Errorf("unknown database %q", d)
		}
		if _, ok := alreadyRequestedDBs[dbID]; !ok {
			desc := resolver.descByID[dbID]
			ret.descs = append(ret.descs, desc)
			ret.requestedDBs = append(ret.requestedDBs, desc.GetDatabase())
			ret.expandedDB = append(ret.expandedDB, dbID)
			////添加每个数据库下面的模式
			for schName := range resolver.scsByName[desc.GetID()] {
				schID := resolver.scsByName[desc.GetID()][schName]
				if sch, ok := resolver.descByID[schID]; ok {
					ret.descs = append(ret.descs, sch)
					ret.requestedScs = append(ret.requestedScs, sch.GetSchema())
					ret.expandedSCs = append(ret.expandedSCs, schID)
					alreadyRequestedSCs[schID] = struct{}{}
				}
			}
			alreadyRequestedDBs[dbID] = struct{}{}
			alreadyExpandedDBs[dbID] = struct{}{}
		}
	}

	//支持单模式还原
	if schemaName != nil {
		var schema *sqlbase.SchemaDescriptor
		schemaID, ok := resolver.schsByName[schemaName.Schema()]
		if !ok {
			return ret, errors.Errorf(`The schema does not exist.`)
		}
		sc, ok := resolver.descByID[schemaID]
		schema = sc.GetSchema()
		if schema == nil {
			return ret, errors.Errorf(`The schema does not exist.`)
		}
		databaseID := schema.GetParentID()
		if database, ok := resolver.descByID[databaseID]; ok {
			realDatabase, err := sqlbase.GetDatabaseDescFromID(ctx, phs.Txn(), databaseID)
			if err != nil {
				err = phs.CheckPrivilege(ctx, database.GetDatabase(), privilege.CREATE)
				if err != nil {
					return ret, err
				}
			} else {
				err = phs.CheckPrivilege(ctx, realDatabase, privilege.CREATE)
				if err != nil {
					return ret, err
				}
			}
			if _, ok := alreadyRequestedDBs[database.GetID()]; !ok {
				parentDesc := resolver.descByID[database.GetID()]
				ret.descs = append(ret.descs, parentDesc)
				alreadyRequestedDBs[database.GetID()] = struct{}{}
			}
		} else {
			return ret, errors.Errorf(`The database does not exist.`)
		}
		if schema != nil {
			if _, ok := alreadyRequestedSCs[schema.ID]; !ok {
				parentDesc := resolver.descByID[schema.ID]
				ret.descs = append(ret.descs, parentDesc)
				ret.requestedScs = append(ret.requestedScs, parentDesc.GetSchema())
				ret.expandedSCs = append(ret.expandedSCs, schema.ID)
				alreadyRequestedSCs[schema.ID] = struct{}{}
				alreadyExpandeSCs[schema.ID] = struct{}{}
			}
		}
	}

	//}

	// Process all the TABLE requests.
	// Pulling in a table needs to pull in the underlying database too.
	//重构后只有还原单表情况
	alreadyRequestedTables := make(map[sqlbase.ID]struct{})
	//for _, pattern := range *TableName {
	//var err error
	if tableName != nil {
		pattern := tableName
		/*
			pattern, err = pattern.NormalizeTablePattern()
			if err != nil {
				return ret, err
			}
		*/
		//		switch p := pattern.(type) {
		//		case *tree.TableName:
		p := pattern
		found, descI, err := p.ResolveExisting(ctx, resolver, false /*requireMutable*/, currentDatabase, searchPath)
		if err != nil {
			return ret, err
		}
		if !found {
			return ret, errors.Errorf(`table %q does not exist`, tree.ErrString(p))
		}
		desc := descI.(sqlbase.Descriptor)

		var schema *sqlbase.SchemaDescriptor
		// If the parent database is not requested already, request it now
		parentID := desc.Table(hlc.Timestamp{}).GetParentID()
		if parentID != sqlbase.SystemDB.ID {
			sc, ok := resolver.descByID[parentID]
			if !ok {
				return ret, errors.Errorf(`The schema isn't found.`)
			}
			schema = sc.GetSchema()
			if schema == nil {
				return ret, errors.Errorf(`The schema does not exist.`)
			}
			realSchema, err := sqlbase.GetSchemaDescFromID(ctx, phs.Txn(), parentID)
			if err != nil {
				err = phs.CheckPrivilege(ctx, schema, privilege.CREATE)
				if err != nil {
					return ret, err
				}
			} else {
				err = phs.CheckPrivilege(ctx, realSchema, privilege.CREATE)
				if err != nil {
					return ret, err
				}
			}
			parentID = schema.ParentID
		}
		if _, ok := alreadyRequestedDBs[parentID]; !ok {
			parentDesc := resolver.descByID[parentID]
			ret.descs = append(ret.descs, parentDesc)
			alreadyRequestedDBs[parentID] = struct{}{}
		}

		if schema != nil {
			if _, ok := alreadyRequestedSCs[schema.ID]; !ok {
				parentDesc := resolver.descByID[schema.ID]
				ret.descs = append(ret.descs, parentDesc)
				alreadyRequestedSCs[schema.ID] = struct{}{}
			}
		}
		// Then request the table itself.
		if _, ok := alreadyRequestedTables[desc.GetID()]; !ok {
			alreadyRequestedTables[desc.GetID()] = struct{}{}
			ret.descs = append(ret.descs, desc)
		}
	}

	/*
		case *tree.AllTablesSelector:
			found, descI, err := p.TableNamePrefix.Resolve(ctx, resolver, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			if !found {
				return ret, sqlbase.NewInvalidWildcardError(tree.ErrString(p))
			}
			desc := descI.(sqlbase.Descriptor)

			// If the database is not requested already, request it now.
			dbID := desc.GetID()
			if _, ok := alreadyRequestedDBs[dbID]; !ok {
				ret.descs = append(ret.descs, desc)
				alreadyRequestedDBs[dbID] = struct{}{}
			}

			// Then request the expansion.
			if _, ok := alreadyExpandedDBs[desc.GetID()]; !ok {
				ret.expandedDB = append(ret.expandedDB, desc.GetID())
				alreadyExpandedDBs[desc.GetID()] = struct{}{}
			}

		default:
			return ret, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	*/

	// Then process the database expansions.
	for dbID := range alreadyExpandedDBs {
		for _, scID := range resolver.scsByName[dbID] {
			//alreadyRequestedSchemas := scID == sqlbase.SystemDB.ID
			for _, tblID := range resolver.tbsByName[scID] {
				if _, ok := alreadyRequestedTables[tblID]; !ok {
					ret.descs = append(ret.descs, resolver.descByID[tblID])
					//alreadyRequestedSchemas = true
				}
			}
		}
	}

	for schID := range alreadyExpandeSCs {
		alreadyRequestedSchemas := schID == sqlbase.SystemDB.ID
		for _, tblID := range resolver.tbsByName[schID] {
			if _, ok := alreadyRequestedTables[tblID]; !ok {
				if !alreadyRequestedSchemas {
					ret.descs = append(ret.descs, resolver.descByID[schID])
				}
				ret.descs = append(ret.descs, resolver.descByID[tblID])
				alreadyRequestedSchemas = true
			}
		}
	}
	return ret, nil
}
