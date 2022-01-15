// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/CCL.txt

package dump

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/icl/gossipicl"
	"github.com/znbasedb/znbase/pkg/icl/locatespaceicl"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/icl/utilicl/intervalicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/stats"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/interval"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
)

// TableRewriteMap maps old table IDs to new table and parent IDs.
type TableRewriteMap map[sqlbase.ID]*jobspb.RestoreDetails_TableRewrite

const (
	restoreOptIntoDB               = "into_db"
	restoreOptSkipMissingFKs       = "skip_missing_foreign_keys"
	restoreOptSkipMissingSequences = "skip_missing_sequences"
	restoreOptWalRevert            = "last_log_collection"

	restoreTempSystemDB     = "zbdb_temp_system"
	restoreTempPublicSchema = "public"
	restoreOptionHTTPHeader = "http_header"
)

var restoreOptionExpectValues = map[string]sql.KVStringOptValidate{
	restoreOptIntoDB:               sql.KVStringOptRequireValue,
	restoreOptSkipMissingFKs:       sql.KVStringOptRequireNoValue,
	restoreOptSkipMissingSequences: sql.KVStringOptRequireNoValue,
	dumpOptEncPassphrase:           sql.KVStringOptRequireValue,
	restoreOptWalRevert:            sql.KVStringOptRequireValue,
	restoreOptionHTTPHeader:        sql.KVStringOptRequireValue,
}

func fullClusterTargetsRestore(
	ctx context.Context, p sql.PlanHookState, allDescs []sqlbase.Descriptor,
) ([]sqlbase.Descriptor, []*sqlbase.DatabaseDescriptor, []*sqlbase.SchemaDescriptor, error) {
	fullClusterDescs, fullClusterDBs, err := fullClusterTargets(allDescs)
	if err != nil {
		return nil, nil, nil, err
	}

	filteredDescs := make([]sqlbase.Descriptor, 0, len(fullClusterDescs))
	filteredSCs := make([]*sqlbase.SchemaDescriptor, 0, len(fullClusterDescs))
	for _, desc := range fullClusterDescs {
		filteredDescs = append(filteredDescs, desc)
		if scDesc := desc.GetSchema(); scDesc != nil {
			scID := scDesc.ID
			if scID == keys.DefaultdbSchemaID || scID == keys.PostgresSchemaID {
				_, err = sqlbase.GetSchemaDescFromID(ctx, p.Txn(), scID)
				if err == nil {
					continue
				}
			}
			filteredSCs = append(filteredSCs, scDesc)
		}
	}
	filteredDBs := make([]*sqlbase.DatabaseDescriptor, 0, len(fullClusterDBs))
	for _, db := range fullClusterDBs {
		dbID := db.ID
		if dbID == keys.DefaultdbDatabaseID || dbID == keys.PostgresDatabaseID || dbID == keys.SystemDatabaseID {
			_, err = sqlbase.GetDatabaseDescFromID(ctx, p.Txn(), dbID)
			if err == nil {
				continue
			}
		}
		filteredDBs = append(filteredDBs, db)
	}

	return filteredDescs, filteredDBs, filteredSCs, nil
}

func loadBackupDescs(
	ctx context.Context,
	uris []string,
	settings *cluster.Settings,
	dumpSinkFromURI dumpsink.FromURIFactory,
	encryption *roachpb.FileEncryptionOptions,
	header string,
) ([]DumpDescriptor, error) {
	backupDescs := make([]DumpDescriptor, len(uris))
	for i, uri := range uris {
		desc, err := ReadDumpDescriptorFromURI(ctx, uri, dumpSinkFromURI, encryption, header)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read backup descriptor")
		}
		backupDescs[i] = desc
	}
	if len(backupDescs) == 0 {
		return nil, errors.Errorf("no backups found")
	}
	return backupDescs, nil
}

func loadSQLDescsFromBackupsAtTime(
	backupDescs []DumpDescriptor, asOf hlc.Timestamp,
) ([]sqlbase.Descriptor, DumpDescriptor) {
	lastBackupDesc := backupDescs[len(backupDescs)-1]

	if asOf.IsEmpty() {
		return lastBackupDesc.Descriptors, lastBackupDesc
	}

	for _, b := range backupDescs {
		if asOf.Less(b.StartTime) {
			break
		}
		lastBackupDesc = b
	}
	if len(lastBackupDesc.DescriptorChanges) == 0 {
		return lastBackupDesc.Descriptors, lastBackupDesc
	}

	byID := make(map[sqlbase.ID]*sqlbase.Descriptor, len(lastBackupDesc.Descriptors))
	for _, rev := range lastBackupDesc.DescriptorChanges {
		if asOf.Less(rev.Time) {
			break
		}
		if rev.Desc == nil {
			delete(byID, rev.ID)
		} else {
			byID[rev.ID] = rev.Desc
		}
	}

	allDescs := make([]sqlbase.Descriptor, 0, len(byID))
	for _, desc := range byID {
		if t := desc.Table(hlc.Timestamp{}); t != nil {
			// A table revisions may have been captured before it was in a DB that is
			// backed up -- if the DB is missing, filter the table.
			if byID[t.ParentID] == nil {
				continue
			}
		}
		allDescs = append(allDescs, *desc)
	}
	return allDescs, lastBackupDesc
}

func selectTargetsObject(
	ctx context.Context,
	p sql.PlanHookState,
	backupDescs []DumpDescriptor,
	restoreStmt *tree.LoadRestore,
	asOf hlc.Timestamp,
	descriptorCoverage tree.DescriptorCoverage,
) ([]sqlbase.Descriptor, []*sqlbase.DatabaseDescriptor, []*sqlbase.SchemaDescriptor, error) {
	allDescs, lastBackupDesc := loadSQLDescsFromBackupsAtTime(backupDescs, asOf)
	if descriptorCoverage == tree.AllDescriptors {
		return fullClusterTargetsRestore(ctx, p, allDescs)
	}
	matched, err := descriptorsMatchingTargetsObject(ctx, p,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, restoreStmt)
	if err != nil {
		return nil, nil, nil, err
	}
	//查询可以找到还原的基本对象（表、模式、库）
	seenObject := false
	for _, desc := range matched.descs {
		if desc.Table(hlc.Timestamp{}) != nil || desc.GetDatabase() != nil || desc.GetSchema() != nil {
			seenObject = true
			break
		}
	}
	if !seenObject {
		return nil, nil, nil, errors.Errorf("no table/schema/database found")
	}

	if lastBackupDesc.FormatVersion >= DumpFormatDescriptorTrackingVersion {
		if err := matched.checkExpansions(lastBackupDesc.CompleteDbs); err != nil {
			return nil, nil, nil, err
		}
	}

	return matched.descs, matched.requestedDBs, matched.requestedScs, nil
}

// rewriteViewQueryDBNames rewrites the passed table's ViewQuery replacing all
// non-empty db qualifiers with `newDB`.
//
// TODO: this AST traversal misses tables named in strings (#24556).
func rewriteViewQueryDBNames(table *sqlbase.TableDescriptor, newDB string) error {
	stmt, err := parser.ParseOne(table.ViewQuery, false)
	if err != nil {
		return errors.Wrapf(err, "failed to parse underlying query from view %q", table.Name)
	}
	// Re-format to change all DB names to `newDB`.
	f := tree.NewFmtCtx(tree.FmtParsable)
	f.SetReformatTableNames(func(ctx *tree.FmtCtx, tn *tree.TableName) {
		// empty catalog e.g. ``"".information_schema.tables` should stay empty.
		if tn.CatalogName != "" {
			tn.CatalogName = tree.Name(newDB)
		}
		ctx.WithReformatTableNames(nil, func() {
			ctx.FormatNode(tn)
		})
	})
	f.FormatNode(stmt.AST)
	table.ViewQuery = f.CloseAndGetString()
	return nil
}

// allocateTableRewrites determines the new ID and parentID (a "TableRewrite")
// for each table in sqlDescs and returns a mapping from old ID to said
// TableRewrite. It first validates that the provided sqlDescs can be restored
// into their original database (or the database specified in opst) to avoid
// leaking table IDs if we can be sure the restore would fail.
func allocateTableRewrites(
	ctx context.Context,
	p sql.PlanHookState,
	sqlDescs []sqlbase.Descriptor,
	restoreDBs []*sqlbase.DatabaseDescriptor,
	restoreSCs []*sqlbase.SchemaDescriptor,
	descriptorCoverage tree.DescriptorCoverage,
	opts map[string]string,
) (TableRewriteMap, error) {
	tableRewrites := make(TableRewriteMap)
	overrideDB, renaming := opts[restoreOptIntoDB]

	restoreDBNames := make(map[string]*sqlbase.DatabaseDescriptor, len(restoreDBs))
	restoreSCNames := make(map[string]*sqlbase.SchemaDescriptor, len(restoreSCs))
	for _, db := range restoreDBs {
		restoreDBNames[db.Name] = db
	}
	for _, sc := range restoreSCs {
		restoreSCNames[sc.Name] = sc
	}
	if len(restoreDBNames) > 0 && renaming {
		return nil, errors.Errorf("cannot use %q option when restoring database(s)", restoreOptIntoDB)
	}
	if len(restoreSCNames) > 0 && renaming {
		return nil, errors.Errorf("cannot use %q option when restoring schema(s)", restoreOptIntoDB)
	}
	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
	schemaByID := make(map[sqlbase.ID]*sqlbase.SchemaDescriptor)
	schemaByID[sqlbase.SystemDB.ID] = &sqlbase.SchemaDescriptor{
		ParentID: sqlbase.SystemDB.ID,
		ID:       sqlbase.SystemDB.ID,
		Name:     "public",
	}
	tablesByID := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	for _, desc := range sqlDescs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			databasesByID[dbDesc.ID] = dbDesc
		} else if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			tablesByID[tableDesc.ID] = tableDesc
		} else if schemaDesc := desc.GetSchema(); schemaDesc != nil {
			schemaByID[schemaDesc.ID] = schemaDesc
		}
	}

	// The logic at the end of this function leaks table IDs, so fail fast if
	// we can be certain the restore will fail.

	// Fail fast if the tables to restore are incompatible with the specified
	// options.
	// Check that foreign key targets exist.
	maxDescIDInBackup := int64(keys.MinNonPredefinedUserDescID)
	for _, table := range tablesByID {
		if int64(table.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(table.ID)
		}
		//maxDescIDInBackup = int64(table.ID)
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			if index.ForeignKey.IsSet() {
				to := index.ForeignKey.Table
				if _, ok := tablesByID[to]; !ok {
					if _, ok := opts[restoreOptSkipMissingFKs]; !ok {
						return errors.Errorf(
							"cannot restore table %q without referenced table %d (or %q option)",
							table.Name, to, restoreOptSkipMissingFKs,
						)
					}
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}

		// Check that referenced sequences exist.
		for _, col := range table.Columns {
			for _, seqID := range col.UsesSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if _, ok := opts[restoreOptSkipMissingSequences]; !ok {
						return nil, errors.Errorf(
							"cannot restore table %q without referenced sequence %d (or %q option)",
							table.Name, seqID, restoreOptSkipMissingSequences,
						)
					}
				}
			}
		}
	}
	for _, db := range databasesByID {
		if int64(db.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(db.ID)
		}
	}
	for _, schema := range schemaByID {
		if int64(schema.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(schema.ID)
		}
	}

	needsNewParentIDs := make(map[string][]sqlbase.ID)
	needsNewParentSCIDs := make(map[sqlbase.ID][]sqlbase.ID)
	// Increment the DescIDSequenceKey so that it is higher than the max desc ID
	// in the backup. This generator keeps produced the next descriptor ID.
	var tempSysSCID, temSysDBID sqlbase.ID
	var err error

	if descriptorCoverage == tree.AllDescriptors {
		if err = p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			b := txn.NewBatch()
			// N.B. This key is usually mutated using the Inc command. That
			// command warns that if the key was every Put directly, Inc will
			// return an error. This is only to ensure that the type of the key
			// doesn't change. Here we just need to be very careful that we only
			// write int64 values.
			// The generator's value should be set to the value of the next ID
			// to generate.
			b.Put(keys.DescIDGenerator, maxDescIDInBackup+1)
			return txn.Run(ctx, b)
		}); err != nil {
			return nil, err
		}
		temSysDBID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return nil, err
		}
		tempSysSCID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return nil, err
		}
		// Remap all of the descriptor belonging to system tables to the temp system
		// DB.
		tableRewrites[temSysDBID] = &jobspb.RestoreDetails_TableRewrite{TableID: temSysDBID}
		tableRewrites[tempSysSCID] = &jobspb.RestoreDetails_TableRewrite{TableID: tempSysSCID, ParentID: temSysDBID}
		for _, table := range tablesByID {
			if table.GetParentID() == sqlbase.SystemDB.ID {
				tableRewrites[table.GetID()] = &jobspb.RestoreDetails_TableRewrite{ParentID: tempSysSCID}
			}
		}
	}
	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Check that any DBs being restored do _not_ exist.
		for name := range restoreDBNames {
			existingDatabaseID, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, name))
			if err != nil {
				return err
			}
			if existingDatabaseID.Value != nil {
				return errors.Errorf("database %q already exists", name)
			}
		}

		// Check that any SCHs being restored do _not_ exist.
		for name, sc := range restoreSCNames {
			//TODO 如果是将要还原的数据库下面的模式需要还原则不需要进行相应的判断,该判断会出现public已存在的问题
			isRestoreDatabase := false
			for _, db := range restoreDBs {
				if sc.ParentID == db.ID {
					isRestoreDatabase = true
				}
			}
			if isRestoreDatabase {
				continue
			}
			existingSchemaID, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(sc.ParentID, name))
			if err != nil {
				return err
			}
			if existingSchemaID.Value != nil {
				return errors.Errorf("schema %q already exists", name)
			}
		}

		for _, table := range tablesByID {
			var targetDB string
			var targetSC *sqlbase.SchemaDescriptor
			targetSC, ok := schemaByID[table.ParentID]
			if !ok {
				if descriptorCoverage == tree.AllDescriptors {
					scDesc, err := sqlbase.GetSchemaDescFromID(ctx, txn, table.ParentID)
					if err != nil {
						return err
					}
					_, err = sqlbase.GetDatabaseDescFromID(ctx, txn, scDesc.ParentID)
					if err != nil {
						return err
					}
				} else {
					return errors.Errorf("no schema with ID %d in backup for table %q", table.ParentID, table.Name)
				}
			}
			if renaming {
				targetDB = overrideDB
			} else if descriptorCoverage == tree.AllDescriptors && table.ParentID < keys.PostgresSchemaID+1 {
				if table.ParentID == sqlbase.SystemDB.ID {
					// For full cluster backups, put the system tables in the temporary
					// system table.
					targetDB = restoreTempSystemDB
					tableRewrites[table.ID] = &jobspb.RestoreDetails_TableRewrite{ParentID: tempSysSCID}
				} else if table.ParentID == keys.DefaultdbSchemaID {
					targetDB = sessiondata.DefaultDatabaseName
				} else if table.ParentID == keys.PostgresSchemaID {
					targetDB = sessiondata.PgDatabaseName
				}
			} else {
				database, ok := databasesByID[targetSC.ParentID]
				if !ok {
					return errors.Errorf("no database with ID %d in backup for table %q",
						table.ParentID, table.Name)
				}
				targetDB = database.Name
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], targetSC.ID)
				needsNewParentSCIDs[targetSC.ID] = append(needsNewParentSCIDs[targetSC.ID], table.ID)
			} else if descriptorCoverage == tree.AllDescriptors {
				// Set the remapped ID to the original parent ID, except for system tables which
				// should be RESTOREd to the temporary system database.
				if targetDB != restoreTempSystemDB {
					tableRewrites[table.ID] = &jobspb.RestoreDetails_TableRewrite{ParentID: table.ParentID}
				}
			} else {
				var parentID sqlbase.ID
				{
					existingDatabaseID, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, targetDB))
					if err != nil {
						return err
					}
					if existingDatabaseID.Value == nil {
						return errors.Errorf("a database named %q needs to exist to restore table %q",
							targetDB, table.Name)
					}

					newParentID, err := existingDatabaseID.Value.GetInt()
					if err != nil {
						return err
					}
					parentID = sqlbase.ID(newParentID)
				}

				// Check privileges. These will be checked again in the transaction
				// that actually writes the new table descriptors.
				if parentID == 1 { // database system does not have schema
					parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, parentID)
					if err != nil {
						return errors.Wrapf(err, "failed to lookup parent DB %d", parentID)
					}
					if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
						return err
					}
				}

				schemaID := targetSC.ID
				{

					existingSchemaID, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(parentID, targetSC.Name))
					if err != nil {
						return err
					}
					if existingSchemaID.Value == nil {
						if len(restoreDBs) == 0 && restoreSCNames[targetSC.Name] == nil {
							return errors.Errorf("a schema named %q needs to exist to restore table %q",
								targetSC.Name, table.Name)
						}
						tableRewrites[targetSC.ID] = &jobspb.RestoreDetails_TableRewrite{ParentID: parentID}
						needsNewParentSCIDs[targetSC.ID] = append(needsNewParentSCIDs[targetSC.ID], table.ID)
					} else {
						newParentID, err := existingSchemaID.Value.GetInt()
						if err != nil {
							return err
						}
						schemaID = sqlbase.ID(newParentID)
					}
				}
				// Create the table rewrite with the new parent ID. We've done all the
				// up-front validation that we can.
				tableRewrites[table.ID] = &jobspb.RestoreDetails_TableRewrite{ParentID: schemaID}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Allocate new IDs for each database and table.
	//
	// NB: we do this in a standalone transaction, not one that covers the
	// entire restore since restarts would be terrible (and our bulk import
	// primitive are non-transactional), but this does mean if something fails
	// during restore we've "leaked" the IDs, in that the generator will have
	// been incremented.
	//
	// NB: The ordering of the new IDs must be the same as the old ones,
	// otherwise the keys may sort differently after they're rekeyed. We could
	// handle this by chunking the AddSSTable calls more finely in Import, but
	// it would be a big performance hit.

	for _, db := range restoreDBs {
		var newID sqlbase.ID
		var err error
		if descriptorCoverage == tree.AllDescriptors {
			newID = db.ID
		} else {
			newID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
			if err != nil {
				return nil, err
			}
		}

		tableRewrites[db.ID] = &jobspb.RestoreDetails_TableRewrite{TableID: newID}
		for _, scID := range needsNewParentIDs[db.Name] {
			tableRewrites[scID] = &jobspb.RestoreDetails_TableRewrite{ParentID: newID}
		}
	}
	//添加空模式的还原
	for _, sc := range restoreSCs {
		var newID sqlbase.ID
		var err error
		if descriptorCoverage == tree.AllDescriptors {
			newID = sc.ID
			tableRewrites[sc.ID] = &jobspb.RestoreDetails_TableRewrite{TableID: newID, ParentID: sc.ParentID}
		} else {
			if _, ok := tableRewrites[sc.ID]; !ok {
				newID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
				if err != nil {
					return nil, err
				}
				tableRewrites[sc.ID] = &jobspb.RestoreDetails_TableRewrite{TableID: newID, ParentID: sc.ParentID}
			}
		}
	}

	for _, sc := range schemaByID {
		if _, ok := tableRewrites[sc.ID]; ok {
			var newID sqlbase.ID
			var err error
			if descriptorCoverage == tree.AllDescriptors {
				newID = sc.ID
			} else {
				newID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
				if err != nil {
					return nil, err
				}
			}
			tableRewrites[sc.ID].TableID = newID
			for _, tableID := range needsNewParentSCIDs[sc.ID] {
				tableRewrites[tableID] = &jobspb.RestoreDetails_TableRewrite{ParentID: newID}
			}
		}
	}

	// tablesToRemap usually contains all tables that are being restored. In a
	// full cluster restore this should only include the system tables that need
	// to be remapped to the temporary table. All other tables in a full cluster
	// backup should have the same ID as they do in the backup.
	tablesToRemap := make([]*sqlbase.TableDescriptor, 0, len(tablesByID))
	for _, table := range tablesByID {
		if descriptorCoverage == tree.AllDescriptors {
			if table.ParentID == sqlbase.SystemDB.ID {
				// This is a system table that should be marked for descriptor creation.
				tablesToRemap = append(tablesToRemap, table)
			} else {
				// This table does not need to be remapped.
				tableRewrites[table.ID].TableID = table.ID
			}
		} else {
			tablesToRemap = append(tablesToRemap, table)
		}
	}
	sort.Sort(sqlbase.TableDescriptors(tablesToRemap))

	// Generate new IDs for the tables that need to be remapped.
	for _, table := range tablesToRemap {
		newTableID, err := sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return nil, err
		}
		if _, ok := tableRewrites[table.ID]; !ok {
			print(1)
		}
		tableRewrites[table.ID].TableID = newTableID
	}

	return tableRewrites, nil
}

// CheckTableExists returns an error if a table already exists with given
// parent and name.
func CheckTableExists(
	ctx context.Context, txn *client.Txn, parentID sqlbase.ID, name string,
) error {
	nameKey := sqlbase.MakeNameMetadataKey(parentID, name)
	res, err := txn.Get(ctx, nameKey)
	if err != nil {
		return err
	}
	if res.Exists() {
		return sqlbase.NewRelationAlreadyExistsError(name)
	}
	return nil
}

// RewriteSchemaDescs mutates schemas to match the ID and privilege specified
// in tableRewrites, as well as adjusting schema references to use the new
// IDs. overrideDB can be specified to set database names.
func RewriteSchemaDescs(
	schemas []*sqlbase.SchemaDescriptor, tableRewrites TableRewriteMap, overrideDB string,
) error {
	for _, schema := range schemas {
		if tableRewrite, ok := tableRewrites[schema.ID]; ok {
			schema.ID = tableRewrite.TableID
			schema.ParentID = tableRewrite.ParentID
		}
	}
	return nil
}

// RewriteTableDescs mutates tables to match the ID and privilege specified
// in tableRewrites, as well as adjusting cross-table references to use the
// new IDs. overrideDB can be specified to set database names in views.
func RewriteTableDescs(
	ctx context.Context,
	txn *client.Txn,
	tables []*sqlbase.TableDescriptor,
	tableRewrites TableRewriteMap,
	overrideDB string,
	ViewOnly bool,
) error {
	for _, table := range tables {
		tableRewrite, ok := tableRewrites[table.ID]
		if !ok {
			return errors.Errorf("missing table rewrite for table %d", table.ID)
		}
		if table.IsView() && overrideDB != "" {
			// restore checks that all dependencies are also being restored, but if
			// the restore is overriding the destination database, qualifiers in the
			// view query string may be wrong. Since the destination override is
			// applied to everything being restored, anything the view query
			// references will be in the override DB post-restore, so all database
			// qualifiers in the view query should be replaced with overrideDB.
			if err := rewriteViewQueryDBNames(table, overrideDB); err != nil {
				return err
			}
		}

		table.ID = tableRewrite.TableID
		table.ParentID = tableRewrite.ParentID

		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			// Verify that for any interleaved index being restored, the interleave
			// parent is also being restored. Otherwise, the interleave entries in the
			// restored IndexDescriptors won't have anything to point to.
			// TODO(dan): It seems like this restriction could be lifted by restoring
			// stub TableDescriptors for the missing interleave parents.
			for j, a := range index.Interleave.Ancestors {
				ancestorRewrite, ok := tableRewrites[a.TableID]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave parent %d", table.Name, a.TableID,
					)
				}
				index.Interleave.Ancestors[j].TableID = ancestorRewrite.TableID
			}
			for j, c := range index.InterleavedBy {
				childRewrite, ok := tableRewrites[c.Table]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave child table %d", table.Name, c.Table,
					)
				}
				index.InterleavedBy[j].Table = childRewrite.TableID
			}

			if index.ForeignKey.IsSet() {
				to := index.ForeignKey.Table
				if indexRewrite, ok := tableRewrites[to]; ok {
					index.ForeignKey.Table = indexRewrite.TableID
				} else {
					// If indexRewrite doesn't exist, the user has specified
					// restoreOptSkipMissingFKs. Error checking in the case the user hasn't has
					// already been done in allocateTableRewrites.
					index.ForeignKey = sqlbase.ForeignKeyReference{}
				}

				// TODO(dt): if there is an existing (i.e. non-restoring) table with
				// a db and name matching the one the FK pointed to at backup, should
				// we update the FK to point to it?
			}

			origRefs := index.ReferencedBy
			index.ReferencedBy = nil
			for _, ref := range origRefs {
				if refRewrite, ok := tableRewrites[ref.Table]; ok {
					ref.Table = refRewrite.TableID
					index.ReferencedBy = append(index.ReferencedBy, ref)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if !ViewOnly {
			if table.IsView() && table.ViewUpdatable {
				//Updatable view, restore support
				viewDependsRewrite, ok := tableRewrites[sqlbase.ID(table.ViewDependsOn)]
				if !ok {
					return errors.Errorf(
						"cannot restore %q without restoring referenced table %d in same operation",
						table.Name, table.ViewDependsOn)
				}
				table.ViewDependsOn = uint32(viewDependsRewrite.TableID)
			}
			// 还原视图时，会有表的依赖找不到
			for i, dest := range table.DependsOn {
				if depRewrite, ok := tableRewrites[dest]; ok {
					table.DependsOn[i] = depRewrite.TableID
				} else {
					return errors.Errorf(
						"cannot restore %q without restoring referenced table %d in same operation",
						table.Name, dest)
				}
			}
		} else {
			// 仅还原视图时,还原的视图包括可更新视图和物化视图
			for _, dest := range table.DependsOn {
				PhysicalTable, err := sqlbase.GetTableDescFromID(ctx, txn, dest)
				if err != nil {
					return errors.Wrapf(
						err, "cannot restore %q without restoring referenced table %d in same operation",
						table.Name, dest)
				}
				if PhysicalTable.Dropped() {
					return errors.Errorf(
						"cannot restore %q without restoring referenced table %d in same operation",
						table.Name, dest)
				}
			}
		}
		origRefs := table.DependedOnBy
		table.DependedOnBy = nil
		for _, ref := range origRefs {
			if refRewrite, ok := tableRewrites[ref.ID]; ok {
				ref.ID = refRewrite.TableID
				table.DependedOnBy = append(table.DependedOnBy, ref)
			}
		}

		// Rewrite sequence references in column descriptors.
		for idx, col := range table.Columns {
			var newSeqRefs []sqlbase.ID
			for _, seqID := range col.UsesSequenceIds {
				if rewrite, ok := tableRewrites[seqID]; ok {
					newSeqRefs = append(newSeqRefs, rewrite.TableID)
				} else {
					// The referenced sequence isn't being restored.
					// Strip the DEFAULT expression and sequence references.
					// To get here, the user must have specified 'skip_missing_sequences' --
					// otherwise, would have errored out in allocateTableRewrites.
					newSeqRefs = []sqlbase.ID{}
					col.DefaultExpr = nil
					break
				}
			}
			col.UsesSequenceIds = newSeqRefs
			table.Columns[idx] = col
		}

		// since this is a "new" table in eyes of new cluster, any leftover change
		// lease is obviously bogus (plus the nodeID is relative to backup cluster).
		table.Lease = nil
	}
	return nil
}

var _ interval.Interface = intervalSpan{}

// ID is part of `interval.Interface` but unused in makeImportSpans.

// Range is part of `interval.Interface`.
//func (ie intervalSpan) Range() interval.Range {
//	return interval.Range{Low: []byte(ie.Key), High: []byte(ie.EndKey)}
//}

//type importEntryType int

//const (
//	backupSpan importEntryType = iota
//	backupFile
//	tableSpan
//	completedSpan
//	request
//)

//type importEntry struct {
//	roachpb.Span
//	entryType importEntryType
//
//	// Only set if entryType is backupSpan
//	start, end hlc.Timestamp
//
//	// Only set if entryType is backupFile
//	dir  roachpb.DumpSink
//	file DumpDescriptor_File
//
//	// Only set if entryType is request
//	files []roachpb.LoadRequest_File
//
//	// for progress tracking we assign the spans numbers as they can be executed
//	// out-of-order based on splitAndScatter's scheduling.
//	progressIdx int
//}

func errOnMissingRange(span intervalicl.Range, start, end hlc.Timestamp) error {
	return errors.Errorf(
		"no dump covers time [%s,%s) for range [%s,%s) (or backups out of order)",
		start, end, roachpb.Key(span.Low), roachpb.Key(span.High),
	)
}

// makeImportSpans pivots the backups, which are grouped by time, into
// spans for import, which are grouped by keyrange.
//
// The core logic of this is in OverlapCoveringMerge, which accepts sets of
// non-overlapping key ranges (aka coverings) each with a payload, and returns
// them aligned with the payloads in the same order as in the input.
//
// Example (input):
// - [A, C) backup t0 to t1 -> /file1
// - [C, D) backup t0 to t1 -> /file2
// - [A, B) backup t1 to t2 -> /file3
// - [B, C) backup t1 to t2 -> /file4
// - [C, D) backup t1 to t2 -> /file5
// - [B, D) requested table data to be restored
//
// Example (output):
// - [A, B) -> /file1, /file3
// - [B, C) -> /file1, /file4, requested (note that file1 was split into two ranges)
// - [C, D) -> /file2, /file5, requested
//
// This would be turned into two Import spans, one restoring [B, C) out of
// /file1 and /file3, the other restoring [C, D) out of /file2 and /file5.
// Nothing is restored out of /file3 and only part of /file1 is used.
//
// NB: All grouping operates in the pre-rewrite keyspace, meaning the keyranges
// as they were backed up, not as they're being restored.
//
// If a span is not covered, the onMissing function is called with the span and
// time missing to determine what error, if any, should be returned.
func makeImportSpans(
	tableSpans []roachpb.Span,
	backups []DumpDescriptor,
	lowWaterMark roachpb.Key,
	onMissing func(span intervalicl.Range, start, end hlc.Timestamp) error,
) ([]importEntry, hlc.Timestamp, error) {
	// Put the covering for the already-completed spans into the
	// OverlapCoveringMerge input first. Payloads are returned in the same order
	// that they appear in the input; putting the completedSpan first means we'll
	// see it first when iterating over the output of OverlapCoveringMerge and
	// avoid doing unnecessary work.
	completedCovering := intervalicl.Ranges{
		{
			Low:     []byte(keys.MinKey),
			High:    []byte(lowWaterMark),
			Payload: importEntry{entryType: completedSpan},
		},
	}

	// Put the merged table data covering into the OverlapCoveringMerge input
	// next.
	var tableSpanCovering intervalicl.Ranges
	for _, span := range tableSpans {
		tableSpanCovering = append(tableSpanCovering, intervalicl.Range{
			Low:  span.Key,
			High: span.EndKey,
			Payload: importEntry{
				Span:      span,
				entryType: tableSpan,
			},
		})
	}

	backupCoverings := []intervalicl.Ranges{completedCovering, tableSpanCovering}

	// Iterate over backups creating two coverings for each. First the spans
	// that were backed up, then the files in the backup. The latter is a subset
	// when some of the keyranges in the former didn't change since the previous
	// backup. These alternate (backup1 spans, backup1 files, backup2 spans,
	// backup2 files) so they will retain that alternation in the output of
	// OverlapCoveringMerge.
	var maxEndTime hlc.Timestamp
	for _, b := range backups {
		if maxEndTime.Less(b.EndTime) {
			maxEndTime = b.EndTime
		}

		var backupNewSpanCovering intervalicl.Ranges
		for _, s := range b.IntroducedSpans {
			backupNewSpanCovering = append(backupNewSpanCovering, intervalicl.Range{
				Low:     s.Key,
				High:    s.EndKey,
				Payload: importEntry{Span: s, entryType: dumpSpan, start: hlc.Timestamp{}, end: b.StartTime},
			})
		}
		backupCoverings = append(backupCoverings, backupNewSpanCovering)

		var backupSpanCovering intervalicl.Ranges
		for _, s := range b.Spans {
			backupSpanCovering = append(backupSpanCovering, intervalicl.Range{
				Low:     s.Key,
				High:    s.EndKey,
				Payload: importEntry{Span: s, entryType: dumpSpan, start: b.StartTime, end: b.EndTime},
			})
		}
		backupCoverings = append(backupCoverings, backupSpanCovering)
		var backupFileCovering intervalicl.Ranges
		for _, f := range b.Files {
			backupFileCovering = append(backupFileCovering, intervalicl.Range{
				Low:  f.Span.Key,
				High: f.Span.EndKey,
				Payload: importEntry{
					Span:      f.Span,
					entryType: dumpFile,
					dir:       b.Dir,
					file:      f,
				},
			})
		}
		backupCoverings = append(backupCoverings, backupFileCovering)
	}

	// Group ranges covered by backups with ones needed to restore the selected
	// tables. Note that this breaks intervals up as necessary to align them.
	// See the function godoc for details.
	importRanges := intervalicl.OverlapCoveringMerge(backupCoverings)

	// Translate the output of OverlapCoveringMerge into requests.
	var requestEntries []importEntry
rangeLoop:
	for _, importRange := range importRanges {
		needed := false
		var ts hlc.Timestamp
		var files []roachpb.LoadRequest_File
		payloads := importRange.Payload.([]interface{})
		for _, p := range payloads {
			ie := p.(importEntry)
			switch ie.entryType {
			case completedSpan:
				continue rangeLoop
			case tableSpan:
				needed = true
			case dumpSpan:
				if ts != ie.start {
					return nil, hlc.Timestamp{}, errors.Errorf(
						"no dump covers time [%s,%s) for range [%s,%s) or dump listed out of order (mismatched start time)",
						ts, ie.start,
						roachpb.Key(importRange.Low), roachpb.Key(importRange.High))
				}
				ts = ie.end
			case dumpFile:
				if len(ie.file.Path) > 0 {
					files = append(files, roachpb.LoadRequest_File{
						Dir:    ie.dir,
						Path:   ie.file.Path,
						Sha512: ie.file.Sha512,
					})
				}
			}
		}
		if needed {
			if ts != maxEndTime {
				if err := onMissing(importRange, ts, maxEndTime); err != nil {
					return nil, hlc.Timestamp{}, err
				}
			}
			// If needed is false, we have data backed up that is not necessary
			// for this restore. Skip it.
			requestEntries = append(requestEntries, importEntry{
				Span:      roachpb.Span{Key: importRange.Low, EndKey: importRange.High},
				entryType: request,
				files:     files,
			})
		}
	}
	return requestEntries, maxEndTime, nil
}

// splitAndScatter creates new ranges for importSpans and scatters replicas and
// leaseholders to be as evenly balanced as possible. It does this with some
// amount of parallelism but also staying as close to the order in importSpans
// as possible (the more out of order, the more work is done if a RESTORE job
// loses its lease and has to be restarted).
//
// At a high level, this is accomplished by splitting and scattering large
// "chunks" from the front of importEntries in one goroutine, each of which are
// in turn passed to one of many worker goroutines that split and scatter the
// individual entries.
//
// importEntries are sent to readyForImportCh as they are scattered, so letting
// that channel send block can be used for backpressure on the splits and
// scatters.
//
// TODO(dan): This logic is largely tuned by running BenchmarkRestore2TB. See if
// there's some way to test it without running an O(hour) long benchmark.
func splitAndScatter(
	RestoreCtx context.Context,
	db *client.DB,
	kr *storageicl.KeyRewriter,
	numClusterNodes int,
	importSpans []importEntry,
	readyForImportCh chan<- importEntry,
) error {
	var span opentracing.Span
	ctx, span := tracing.ChildSpan(RestoreCtx, "presplit-scatter")
	defer tracing.FinishSpan(span)

	g := ctxgroup.WithContext(ctx)

	// TODO(dan): This not super principled. I just wanted something that wasn't
	// a constant and grew slower than linear with the length of importSpans. It
	// seems to be working well for BenchmarkRestore2TB but worth revisiting.
	chunkSize := int(math.Sqrt(float64(len(importSpans))))
	importSpanChunks := make([][]importEntry, 0, len(importSpans)/chunkSize)
	for start := 0; start < len(importSpans); {
		importSpanChunk := importSpans[start:]
		end := start + chunkSize
		if end < len(importSpans) {
			importSpanChunk = importSpans[start:end]
		}
		importSpanChunks = append(importSpanChunks, importSpanChunk)
		start = end
	}

	importSpanChunksCh := make(chan []importEntry)
	g.GoCtx(func(ctx context.Context) error {
		defer close(importSpanChunksCh)
		for idx, importSpanChunk := range importSpanChunks {
			// TODO(dan): The structure between this and the below are very
			// similar. Dedup.
			chunkKey, err := rewriteBackupSpanKey(kr, importSpanChunk[0].Key)
			if err != nil {
				return err
			}

			// TODO(dan): Really, this should be splitting the Key of the first
			// entry in the _next_ chunk.
			log.VEventf(ctx, 1, "presplitting chunk %d of %d", idx, len(importSpanChunks))
			if err := db.AdminSplit(ctx, chunkKey, chunkKey); err != nil {
				return err
			}

			log.VEventf(ctx, 1, "scattering chunk %d of %d", idx, len(importSpanChunks))
			scatterReq := &roachpb.AdminScatterRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(roachpb.Span{Key: chunkKey, EndKey: chunkKey.Next()}),
			}
			if _, pErr := client.SendWrapped(ctx, db.NonTransactionalSender(), scatterReq); pErr != nil {
				// TODO(dan): Unfortunately, Scatter is still too unreliable to
				// fail the RESTORE when Scatter fails. I'm uncomfortable that
				// this could break entirely and not start failing the tests,
				// but on the bright side, it doesn't affect correctness, only
				// throughput.
				log.Errorf(ctx, "failed to scatter chunk %d: %s", idx, pErr.GoError())
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case importSpanChunksCh <- importSpanChunk:
			}
		}
		return nil
	})

	// TODO(dan): This tries to cover for a bad scatter by having 2 * the number
	// of nodes in the cluster. Is it necessary?
	splitScatterWorkers := numClusterNodes * 2
	var splitScatterStarted uint64 // Only access using atomic.
	for worker := 0; worker < splitScatterWorkers; worker++ {
		g.GoCtx(func(ctx context.Context) error {
			for importSpanChunk := range importSpanChunksCh {
				for _, importSpan := range importSpanChunk {
					idx := atomic.AddUint64(&splitScatterStarted, 1)

					newSpanKey, err := rewriteBackupSpanKey(kr, importSpan.Span.Key)
					if err != nil {
						return err
					}

					// TODO(dan): Really, this should be splitting the Key of
					// the _next_ entry.
					log.VEventf(ctx, 1, "presplitting %d of %d", idx, len(importSpans))
					if err := db.AdminSplit(ctx, newSpanKey, newSpanKey); err != nil {
						return err
					}

					log.VEventf(ctx, 1, "scattering %d of %d", idx, len(importSpans))
					scatterReq := &roachpb.AdminScatterRequest{
						RequestHeader: roachpb.RequestHeaderFromSpan(roachpb.Span{Key: newSpanKey, EndKey: newSpanKey.Next()}),
					}
					if _, pErr := client.SendWrapped(ctx, db.NonTransactionalSender(), scatterReq); pErr != nil {
						// TODO(dan): Unfortunately, Scatter is still too unreliable to
						// fail the RESTORE when Scatter fails. I'm uncomfortable that
						// this could break entirely and not start failing the tests,
						// but on the bright side, it doesn't affect correctness, only
						// throughput.
						log.Errorf(ctx, "failed to scatter %d: %s", idx, pErr.GoError())
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case readyForImportCh <- importSpan:
					}
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// updateIndexLocationNums by partitioning or locate in
func updateIndexLocationNums(
	td *sqlbase.TableDescriptor, id *sqlbase.IndexDescriptor, newLocationNums int32,
) error {
	if td == nil || id == nil {
		return fmt.Errorf("TabDescriptor or IndexDescriptor should not be nil")
	}
	if newLocationNums < 0 {
		return fmt.Errorf("LocationNums can not below zero")
	}

	td.LocationNums -= id.LocationNums
	if td.LocationNums < 0 {
		return fmt.Errorf("LocationNums can not below zero")
	}

	id.LocationNums = newLocationNums
	td.LocationNums += id.LocationNums
	return nil
}

// updateTableLocationNums by table locate in
func updateTableLocationNums(td *sqlbase.TableDescriptor, newLocationNums int32) error {
	if td == nil {
		return fmt.Errorf("TableDescriptor should not nil")
	}
	if newLocationNums < 0 {
		return fmt.Errorf("LocationNums can not below zero")
	}
	td.LocationNums = newLocationNums
	return nil
}

// setLocationRaw update or delete the LocationMap with the given ID
func setLocationRaw(
	ctx context.Context, b *client.Batch, id sqlbase.ID, locationMap *roachpb.LocationMap,
) error {
	if locationMap == nil {
		b.Del(ctx, config.MakeLocationKey(uint32(id)))
		return nil
	}
	value, err := protoutil.Marshal(locationMap)
	if err != nil {
		return err
	}
	b.Put(config.MakeLocationKey(uint32(id)), value)
	return nil
}

// updateLocationNumber will traverse all index-descriptor and update location number of the tabledesc
func updateLocationNumber(
	ctx context.Context, b *client.Batch, desc *sqlbase.TableDescriptor,
) error {
	// for dump load ,dont need update
	if desc.LocationNums > 0 {
		return nil
	}
	if desc.LocateSpaceName != nil {
		if err := updateTableLocationNums(desc, 1); err != nil {
			return err
		}
	}
	// 2. second deal with primary key's location number
	primaryKey := &desc.PrimaryIndex
	var newLocationNumber int32
	if primaryKey.LocateSpaceName != nil {
		newLocationNumber = 1
	}
	newLocationNumber += primaryKey.Partitioning.LocationNums
	if newLocationNumber != 0 {
		if err := updateIndexLocationNums(desc, primaryKey, newLocationNumber); err != nil {
			return err
		}
	}
	// 3. travers all index array to update location number
	indexes := desc.Indexes
	for i, idx := range indexes {
		if idx.LocateSpaceName != nil {
			newLocationNumber = 1
		}
		newLocationNumber += idx.Partitioning.LocationNums
		if newLocationNumber != 0 {
			if err := updateIndexLocationNums(desc, &indexes[i], newLocationNumber); err != nil {
				return err
			}
		}
	}
	// 4. at Last if table Location Numbers not zero will update system.locations
	if desc.LocationNums != 0 {
		locationMap, err := locatespaceicl.GetLocationMapICL(desc)
		if err != nil {
			return err
		}
		if err := setLocationRaw(ctx, b, desc.ID, locationMap); err != nil {
			return err
		}
	}
	return nil
}

// WriteTableDescs writes all the the new descriptors: First the ID ->
// TableDescriptor for the new table, then flip (or initialize) the name -> ID
// entry so any new queries will use the new one. The tables are assigned the
// permissions of their parent database and the user must have CREATE permission
// on that database at the time this function is called.
func WriteTableDescs(
	ctx context.Context,
	txn *client.Txn,
	databases []*sqlbase.DatabaseDescriptor,
	schemas []*sqlbase.SchemaDescriptor,
	tables []*sqlbase.TableDescriptor,
	user string,
	settings *cluster.Settings,
	extra []roachpb.KeyValue,
	descCoverage tree.DescriptorCoverage,
) error {
	ctx, span := tracing.ChildSpan(ctx, "WriteTableDescs")
	defer tracing.FinishSpan(span)
	err := func() error {
		b := txn.NewBatch()
		wroteDBs := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
		wroteSCs := make(map[sqlbase.ID]*sqlbase.SchemaDescriptor)
		for _, desc := range databases {
			// TODO(dt): support restoring privs.
			if desc.GetPrivileges() == nil {
				if err := desc.Privileges.RemoveUserPriNotExist(ctx, txn); err != nil {
					return err
				}
				desc.Privileges = sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Database, user) //todo(xz): owner?
			}
			wroteDBs[desc.ID] = desc
			//当还原数据库时，同时更新对应模式的ParentID
			for _, sch := range desc.Schemas {
				for _, schID := range schemas {
					if schID.ID == sch.ID {
						schID.ParentID = desc.ID
					}
				}
			}
			for _, sch := range schemas {
				for k, sc := range desc.Schemas {
					if sc.ID == sch.ID {
						desc.Schemas = append(desc.Schemas[:k], desc.Schemas[k+1:]...)
						break
					}
				}
			}
			b.CPut(sqlbase.MakeDescMetadataKey(desc.ID), sqlbase.WrapDescriptor(desc), nil)
			b.CPut(sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, desc.Name), desc.ID, nil)
		}
		for _, schema := range schemas {
			if wrote, ok := wroteDBs[schema.ParentID]; ok {
				if descCoverage == tree.AllDescriptors {
					if schema.GetPrivileges() == nil {
						schema.Privileges = wrote.GetPrivileges()
					}
				}
			} else {
				parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, schema.ParentID)
				if err != nil {
					return errors.Wrapf(err, "failed to lookup parent DB %d", schema.ParentID)
				}
				// TODO(mberhault): CheckPrivilege wants a planner.
				//if err := sql.checkPrivilegeForUser(ctx, txn, user, parentDB, privilege.CREATE); err != nil {
				//	return err
				//}
				// Default is to copy privs from restoring parent db, like CREATE TABLE.
				// TODO(dt): Make this more configurable.
				// schema.Privileges = sqlbase.NewDefaultDataBasePrivilegeDescriptor()
				wroteDBs[parentDB.ID] = parentDB
			}
			// If the restore is not a full cluster restore we cannot know that
			// the users on the restoring cluster match the ones that were on the
			// cluster that was backed up. So we wipe the priviledges on the database.
			// remove the user in the privilegeDesc if it is no exist in system.users
			if schema.GetPrivileges() == nil {
				if err := schema.Privileges.RemoveUserPriNotExist(ctx, txn); err != nil {
					return err
				}
				schema.Privileges.MaybeFixPrivileges(schema.ID, privilege.DatabasePrivileges)
			}
			wroteSCs[schema.ID] = schema
			descMetadaKey := sqlbase.MakeDescMetadataKey(schema.GetID())
			nameMetadataKey := sqlbase.MakeNameMetadataKey(schema.GetParentID(), schema.GetName())
			b.CPut(nameMetadataKey, schema.ID, nil)
			b.CPut(descMetadaKey, sqlbase.WrapDescriptor(schema), nil)
			if database, ok := wroteDBs[schema.ParentID]; ok {
				database.Schemas = append(database.Schemas, *schema)
				descKey := sqlbase.MakeDescMetadataKey(database.GetID())
				descDesc := sqlbase.WrapDescriptor(database)
				b.Put(descKey, descDesc)
			} else {
				database, err := sqlbase.GetDatabaseDescFromID(ctx, txn, schema.ParentID)
				//修改database 所对应的schema
				if err != nil {
					return errors.Wrapf(err, "failed to lookup parent DB %d", schema.ParentID)
				}
				database.Schemas = append(database.Schemas, *schema)
				descKey := sqlbase.MakeDescMetadataKey(database.GetID())
				descDesc := sqlbase.WrapDescriptor(database)
				//descDesc.Reset()
				b.Put(descKey, descDesc)
			}
		}
		if len(tables) == 1 {
			//when only restoring the view, modify the dependency of the table
			//In addition, when the materialized view is restored, the dependency is also added
			for _, table := range tables {
				if table.IsView() {
					for _, dest := range table.DependsOn {
						PhysicalTable, err := sqlbase.GetTableDescFromID(ctx, txn, dest)
						if err != nil {
							return err
						}
						var origRefs sqlbase.TableDescriptor_Reference
						origRefs.ID = table.ID
						origRefs.IndexID = 0
						for i := 0; i < len(PhysicalTable.Columns); i++ {
							origRefs.ColumnIDs = append(origRefs.ColumnIDs, sqlbase.ColumnID(i+1))
						}
						PhysicalTable.DependedOnBy = append(PhysicalTable.DependedOnBy, origRefs)
						descKey := sqlbase.MakeDescMetadataKey(PhysicalTable.GetID())
						descDesc := sqlbase.WrapDescriptor(PhysicalTable)
						b.Put(descKey, descDesc)
					}
				}
			}
		}
		for _, table := range tables {
			// For full cluster restore, keep privileges as they were.
			if wrote, ok := wroteSCs[table.ParentID]; ok {
				if descCoverage == tree.AllDescriptors || wrote.Name == restoreTempSystemDB {
					if table.GetPrivileges() == nil {
						table.Privileges = wrote.GetPrivileges()
					}
				}
			} else {
				_, err := sqlbase.GetSchemaDescFromID(ctx, txn, table.ParentID)
				if err != nil {
					return errors.Wrapf(err, "failed to lookup parent SC %d", table.ParentID)
				}
				// TODO(mberhault): CheckPrivilege wants a planner.
				//if err := sql.checkPrivilegeForUser(ctx, txn, user, parentSC, privilege.CREATE); err != nil {
				//	return err
				//}
				// Default is to copy privs from restoring parent db, like CREATE TABLE.
				// TODO(dt): Make this more configurable.
				// table.Privileges = sqlbase.NewDefaultTablePrivilegeDescriptor()
			}
			if table.GetPrivileges() == nil {
				if err := table.Privileges.RemoveUserPriNotExist(ctx, txn); err != nil {
					return err
				}
				table.Privileges.MaybeFixPrivileges(table.ID, privilege.TablePrivileges)
			}
			for i := range table.Columns {
				if table.Columns[i].Privileges != nil {
					if err := table.Columns[i].Privileges.RemoveUserPriNotExist(ctx, txn); err != nil {
						return err
					}
				}
			}
			b.CPut(table.GetDescMetadataKey(), sqlbase.WrapDescriptor(table), nil)
			b.CPut(table.GetNameMetadataKey(), table.ID, nil)
			if err := updateLocationNumber(ctx, b, table); err != nil {
				return err
			}
		}
		for _, kv := range extra {
			b.InitPut(kv.Key, &kv.Value, false)
		}
		if err := txn.Run(ctx, b); err != nil {
			if _, ok := errors.Cause(err).(*roachpb.ConditionFailedError); ok {
				return errors.New("table already exists")
			}
			return err
		}
		if err := txn.SetSystemConfigTrigger(); err != nil {
			//return errors.Wrap(err, "restore failed")
			//此处只是为了触发事务对当前的System Config加锁，使得实际写入为真实的Descriptor,而不是上个版本数据，此处的错误不抛出。因为此处一定会出现transaction anchor key already set的错误。
		}
		prevSteppingMode := txn.ConfigureStepping(ctx, client.SteppingEnabled)
		defer func() { _ = txn.ConfigureStepping(ctx, prevSteppingMode) }()
		for _, table := range tables {
			if err := table.RefreshValidate(ctx, txn, settings); err != nil {
				return errors.Wrapf(err, "validate table %d", table.ID)
			}
		}
		for _, schema := range schemas {
			if err := schema.Validate(); err != nil {
				return errors.Wrapf(err, "validate schema %d", schema.ID)
			}
		}
		return nil
	}()
	if err != nil {
		return errors.Wrap(err, "restoring table desc and namespace entries")
	}
	return nil
}

func restoreJobDescription(
	restore *tree.LoadRestore, from []string, opts map[string]string,
) (string, error) {
	r := &tree.LoadRestore{
		AsOf:         restore.AsOf,
		Options:      optsToKVOptions(opts),
		TableName:    restore.TableName,
		DatabaseName: restore.DatabaseName,
		SchemaName:   restore.SchemaName,
		Files:        make(tree.Exprs, len(restore.Files)),
	}

	for i, f := range from {
		sf, err := dumpsink.SanitizeDumpSinkURI(f)
		if err != nil {
			return "", err
		}
		r.Files[i] = tree.NewDString(sf)
	}

	return tree.AsStringWithFlags(r, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType), nil
}

// rewriteBackupSpanKey rewrites a backup span start key for the purposes of
// splitting up the target key-space to send out the actual work of restoring.
//
// Keys for the primary index of the top-level table are rewritten to the just
// the overall start of the table. That is, /Table/51/1 becomes /Table/51.
//
// Any suffix of the key that does is not rewritten by kr's configured rewrites
// is truncated. For instance if a passed span has key /Table/51/1/77#/53/2/1
// but kr only configured with a rewrite for 51, it would return /Table/51/1/77.
// Such span boundaries are usually due to a interleaved table which has since
// been dropped -- any splits that happened to pick one of its rows live on, but
// include an ID of a table that no longer exists.
//
// Note that the actual restore process (i.e. inside ImportRequest) does not use
// these keys -- they are only used to split the key space and distribute those
// requests, thus truncation is fine. In the rare case where multiple backup
// spans are truncated to the same prefix (i.e. entire spans resided under the
// same interleave parent row) we'll generate some no-op splits and route the
// work to the same range, but the actual imported data is unaffected.
func rewriteBackupSpanKey(kr *storageicl.KeyRewriter, key roachpb.Key) (roachpb.Key, error) {
	newKey, rewritten, err := kr.RewriteKey(append([]byte(nil), key...))
	if err != nil {
		return nil, errors.Wrapf(err, "could not rewrite span start key: %s", key)
	}
	if !rewritten && bytes.Equal(newKey, key) {
		// if nothing was changed, we didn't match the top-level key at all.
		return nil, errors.Errorf("no rewrite for span start key: %s", key)
	}
	// Modify all spans that begin at the primary index to instead begin at the
	// start of the table. That is, change a span start key from /Table/51/1 to
	// /Table/51. Otherwise a permanently empty span at /Table/51-/Table/51/1
	// will be created.
	if b, id, idx, err := sqlbase.DecodeTableIDIndexID(newKey); err != nil {
		return nil, errors.Wrapf(err, "could not rewrite span start key: %s", key)
	} else if idx == 1 && len(b) == 0 {
		newKey = keys.MakeTablePrefix(uint32(id))
	}
	return newKey, nil
}

// restore imports a SQL table (or tables) from sets of non-overlapping sstable
// files.
func restore(
	restoreCtx context.Context,
	db *client.DB,
	gossip *gossip.Gossip,
	backupDescs []DumpDescriptor,
	endTime hlc.Timestamp,
	sqlDescs []sqlbase.Descriptor,
	tableRewrites TableRewriteMap,
	overrideDB string,
	job *jobs.Job,
	resultsCh chan<- tree.Datums,
	encryption *roachpb.FileEncryptionOptions,
	httpHeader string,
) (
	roachpb.BulkOpSummary,
	[]*sqlbase.DatabaseDescriptor,
	[]*sqlbase.SchemaDescriptor,
	[]*sqlbase.TableDescriptor,
	error,
) {
	// A note about contexts and spans in this method: the top-level context
	// `restoreCtx` is used for orchestration logging. All operations that carry
	// out work get their individual contexts.

	mu := struct {
		syncutil.Mutex
		res               roachpb.BulkOpSummary
		requestsCompleted []bool
		highWaterMark     int
	}{
		highWaterMark: -1,
	}

	var databases []*sqlbase.DatabaseDescriptor
	var schemas []*sqlbase.SchemaDescriptor
	var tables []*sqlbase.TableDescriptor
	var oldTableIDs []sqlbase.ID
	var views []*sqlbase.TableDescriptor
	for _, desc := range sqlDescs {
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			if tableDesc.IsView() && !tableDesc.IsMaterializedView {
				views = append(views, tableDesc)
			} else {
				tables = append(tables, tableDesc)
				oldTableIDs = append(oldTableIDs, tableDesc.ID)
			}
		}
		if scDesc := desc.GetSchema(); scDesc != nil {
			if rewrite, ok := tableRewrites[scDesc.ID]; ok {
				scDesc.ID = rewrite.TableID
				scDesc.ParentID = rewrite.ParentID
				schemas = append(schemas, scDesc)
			}
		}
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if rewrite, ok := tableRewrites[dbDesc.ID]; ok {
				dbDesc.ID = rewrite.TableID
				var scDescs []sqlbase.SchemaDescriptor
				for _, sc := range dbDesc.Schemas {
					if rewrite, ok := tableRewrites[sc.ID]; ok {
						sc.ID = rewrite.TableID
						sc.ParentID = rewrite.ParentID
						scDescs = append(scDescs, sc)
					}
				}
				dbDesc.Schemas = scDescs
				if dbDesc.ID != sqlbase.SystemDB.ID {
					databases = append(databases, dbDesc)
				}
			}
		}
	}

	log.Eventf(restoreCtx, "starting load for %d tables", len(tables))

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	spans := spansForAllTableIndexes(tables, nil)

	//if err := RewriteSchemaDescs(schemas, tableRewrites, overrideDB); err != nil {
	//	return mu.res, nil, nil, nil, err
	//}

	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	var ViewOnly bool
	if len(views) == 1 && len(tables) == 0 {
		ViewOnly = true
	}
	//Rewrite normal view
	if err := RewriteTableDescs(restoreCtx, db.NewTxn(restoreCtx, "ViewDescById"), views, tableRewrites, overrideDB, ViewOnly); err != nil {
		return mu.res, nil, nil, nil, err
	}
	//If there is no table, there is no need to restore the relevant data.
	//materialized view also needs to restore the data.
	if tables == nil && len(tables) == 0 {
		if len(views) != 0 {
			tables = append(tables, views...)
		}
		return mu.res, databases, schemas, tables, nil
	}
	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	var MaterializedViewOnly bool
	if len(tables) == 1 {
		if tables[0].IsMaterializedView {
			MaterializedViewOnly = true
		}
	}
	//When rewriting materialized views and tables.
	//we need to distinguish whether it is only rewriting materialized views
	if err := RewriteTableDescs(restoreCtx, db.NewTxn(restoreCtx, "TableDescById"), tables, tableRewrites, overrideDB, MaterializedViewOnly); err != nil {
		return mu.res, nil, nil, nil, err
	}

	{
		// Disable merging for the table IDs being restored into. We don't want the
		// merge queue undoing the splits performed during RESTORE.
		tableIDs := make([]uint32, 0, len(tables))
		for _, t := range tables {
			tableIDs = append(tableIDs, uint32(t.ID))
		}
		disableCtx, cancel := context.WithCancel(restoreCtx)
		defer cancel()
		gossipicl.DisableMerges(disableCtx, gossip, tableIDs)
	}

	// Get TableRekeys to use when importing raw data.
	var rekeys []roachpb.LoadRequest_TableRekey
	for i := range tables {
		newDescBytes, err := protoutil.Marshal(sqlbase.WrapDescriptor(tables[i]))
		if err != nil {
			return mu.res, nil, nil, nil, errors.Wrap(err, "marshaling descriptor")
		}
		rekeys = append(rekeys, roachpb.LoadRequest_TableRekey{
			OldID:   uint32(oldTableIDs[i]),
			NewDesc: newDescBytes,
		})
	}
	kr, err := storageicl.MakeKeyRewriterFromRekeys(rekeys)
	if err != nil {
		return mu.res, nil, nil, nil, err
	}

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	highWaterMark := job.Progress().Details.(*jobspb.Progress_Restore).Restore.HighWater
	importSpans, _, err := makeImportSpans(spans, backupDescs, highWaterMark, errOnMissingRange)
	if err != nil {
		return mu.res, nil, nil, nil, errors.Wrapf(err, "making load requests for %d dump", len(backupDescs))
	}

	for i := range importSpans {
		importSpans[i].progressIdx = i
	}
	mu.requestsCompleted = make([]bool, len(importSpans))

	progressLogger := jobs.ProgressLogger{
		Job:           job,
		TotalChunks:   len(importSpans),
		StartFraction: job.FractionCompleted(),
		ProgressedFn: func(progressedCtx context.Context, details jobspb.ProgressDetails) {
			switch d := details.(type) {
			case *jobspb.Progress_Restore:
				mu.Lock()
				if mu.highWaterMark >= 0 {
					d.Restore.HighWater = importSpans[mu.highWaterMark].Key
				}
				mu.Unlock()
			default:
				log.Errorf(progressedCtx, "job payload had unexpected type %T", d)
			}
		},
	}

	// We're already limiting these on the server-side, but sending all the
	// Import requests at once would fill up distsender/grpc/something and cause
	// all sorts of badness (node liveness timeouts leading to mass leaseholder
	// transfers, poor performance on SQL workloads, etc) as well as log spam
	// about slow distsender requests. Rate limit them here, too.
	//
	// Use the number of cpus across all nodes in the cluster as the number of
	// outstanding Import requests for the rate limiting. Note that this assumes
	// all nodes in the cluster have the same number of cpus, but it's okay if
	// that's wrong.
	//
	// TODO(dan): Make this limiting per node.
	numClusterNodes := clusterNodeCount(gossip)
	maxConcurrentImports := numClusterNodes * runtime.NumCPU()
	importsSem := make(chan struct{}, maxConcurrentImports)

	g := ctxgroup.WithContext(restoreCtx)

	// The Import (and resulting AddSSTable) requests made below run on
	// leaseholders, so presplit and scatter the ranges to balance the work
	// among many nodes.
	//
	// We're about to start off some goroutines that presplit & scatter each
	// import span. Once split and scattered, the span is submitted to
	// readyForImportCh to indicate it's ready for Import. Since import is so
	// much slower, we buffer the channel to keep the split/scatter work from
	// getting too far ahead. This both naturally rate limits the split/scatters
	// and bounds the number of empty ranges crated if the RESTORE fails (or is
	// canceled).
	const presplitLeadLimit = 10
	readyForImportCh := make(chan importEntry, presplitLeadLimit)
	g.GoCtx(func(ctx context.Context) error {
		defer close(readyForImportCh)
		return splitAndScatter(ctx, db, kr, numClusterNodes, importSpans, readyForImportCh)
	})

	requestFinishedCh := make(chan struct{}, len(importSpans)) // enough buffer to never block
	g.GoCtx(func(ctx context.Context) error {
		ctx, progressSpan := tracing.ChildSpan(ctx, "progress-log")
		defer tracing.FinishSpan(progressSpan)
		return progressLogger.Loop(ctx, requestFinishedCh)
	})
	g.GoCtx(func(ctx context.Context) error {
		log.Eventf(restoreCtx, "commencing import of data with concurrency %d", maxConcurrentImports)
		for readyForImportSpan := range readyForImportCh {
			newSpanKey, err := rewriteBackupSpanKey(kr, readyForImportSpan.Span.Key)
			if err != nil {
				return err
			}
			log.Errorf(ctx, "readyForImportSpan key :%s", newSpanKey)
			idx := readyForImportSpan.progressIdx

			importRequest := &roachpb.LoadRequest{
				// Import is a point request because we don't want DistSender to split
				// it. Assume (but don't require) the entire post-rewrite span is on the
				// same range.
				RequestHeader: roachpb.RequestHeader{Key: newSpanKey},
				DataSpan:      readyForImportSpan.Span,
				Files:         readyForImportSpan.files,
				EndTime:       endTime,
				Rekeys:        rekeys,
				Encryption:    encryption,
				HttpHeader:    httpHeader,
			}

			log.VEventf(restoreCtx, 1, "importing %d of %d", idx, len(importSpans))

			select {
			case importsSem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}

			g.GoCtx(func(ctx context.Context) error {
				ctx, importSpan := tracing.ChildSpan(ctx, "import")
				log.Event(ctx, "acquired semaphore")
				defer tracing.FinishSpan(importSpan)
				defer func() { <-importsSem }()

				importRes, pErr := client.SendWrapped(ctx, db.NonTransactionalSender(), importRequest)
				if pErr != nil {
					return errors.Wrapf(pErr.GoError(), "importing span %v", importRequest.DataSpan)

				}

				mu.Lock()
				mu.res.Add(importRes.(*roachpb.ImportResponse).Imported)

				// Assert that we're actually marking the correct span done. See #23977.
				if !importSpans[idx].Key.Equal(importRequest.DataSpan.Key) {
					mu.Unlock()
					return errors.Errorf(
						"request %d for span %v (to %v) does not match import span for same idx: %v",
						idx, importRequest.DataSpan, newSpanKey, importSpans[idx],
					)
				}
				mu.requestsCompleted[idx] = true
				for j := mu.highWaterMark + 1; j < len(mu.requestsCompleted) && mu.requestsCompleted[j]; j++ {
					mu.highWaterMark = j
				}
				mu.Unlock()

				requestFinishedCh <- struct{}{}
				return nil
			})
		}
		log.Event(restoreCtx, "wait for outstanding imports to finish")
		return nil
	})

	if err := g.Wait(); err != nil {
		// This leaves the data that did get imported in case the user wants to
		// retry.
		// TODO(dan): Build tooling to allow a user to restart a failed restore.
		return mu.res, nil, nil, nil, errors.Wrapf(err, "importing %d ranges", len(importSpans))
	}
	if len(views) != 0 {
		tables = append(tables, views...)
	}
	return mu.res, databases, schemas, tables, nil
}

// RestoreHeader is the header for RESTORE stmt results.
var RestoreHeader = sqlbase.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "index_entries", Typ: types.Int},
	{Name: "system_records", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

// restorePlanHook implements sql.PlanHookFn.
func restorePlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	restoreStmt, ok := stmt.(*tree.LoadRestore)
	if !ok {
		return nil, nil, nil, false, nil
	}

	fromFn, err := p.TypeAsStringArray(restoreStmt.Files, "LOAD")
	if err != nil {
		return nil, nil, nil, false, err
	}

	optsFn, err := p.TypeAsStringOpts(restoreStmt.Options, restoreOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)
		/*
			if err := utilccl.CheckEnterpriseEnabled(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "LOAD",
			); err != nil {
				return err
			}
		*/

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("LOAD cannot be used inside a transaction")
		}

		// Older nodes don't know about many new fields and flags, e.g. as-of-time,
		// and our testing does not comprehensively cover mixed-version clusters.
		// Refusing to initiate RESTOREs on a new node while old nodes may evaluate
		// the RPCs it issues or even try to resume the RESTORE job and mishandle it
		// avoid any potential unexepcted behavior. This errs on the side of being too
		// restrictive, but an operator can still send the job to the remaining 1.x
		// nodes if needed. VersionClearRange was introduced after many of these
		// fields and the refactors to how jobs were saved and resumed, though we may
		// want to bump this to 2.0 for simplicity.
		if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionClearRange) {
			return errors.Errorf(
				"running LOAD on a 2.x node requires cluster version >= %s",
				cluster.VersionByKey(cluster.VersionClearRange).String(),
			)
		}

		from, err := fromFn()
		if err != nil {
			return err
		}
		var endTime hlc.Timestamp
		if restoreStmt.AsOf.Expr != nil {
			var err error
			endTime, err = p.EvalAsOfTimestamp(restoreStmt.AsOf)
			if err != nil {
				return err
			}
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}
		return doRestorePlan(ctx, restoreStmt, p, from, endTime, opts, resultsCh)
	}
	return fn, RestoreHeader, nil, false, nil
}

func doRestorePlan(
	ctx context.Context,
	restoreStmt *tree.LoadRestore,
	p sql.PlanHookState,
	from []string,
	endTime hlc.Timestamp,
	opts map[string]string,
	resultsCh chan<- tree.Datums,
) error {
	var header string
	if override, ok := opts[restoreOptionHTTPHeader]; ok {
		header = override
	}
	var encryption *roachpb.FileEncryptionOptions
	if passphrase, ok := opts[dumpOptEncPassphrase]; ok {
		if len(from) < 1 {
			return errors.New("invalid base backup specified")
		}
		store, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, from[0])
		if err != nil {
			return errors.Wrapf(err, "make storage")
		}
		defer store.Close()
		if store.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
			err := store.SetConfig(header)
			if err != nil {
				return err
			}
		}
		opts, err := ReadEncryptionOptions(ctx, store)
		if err != nil {
			return err
		}
		encryptionKey := storageicl.GenerateKey([]byte(passphrase), opts.Salt)
		encryption = &roachpb.FileEncryptionOptions{Key: encryptionKey}
	}
	backupDescs, err := loadBackupDescs(ctx, from, p.ExecCfg().Settings, p.ExecCfg().DistSQLSrv.DumpSinkFromURI, encryption, header)
	if err != nil {
		return err
	}
	//进行wal日志的解析，并组装成DUMP文件
	if walPath, ok := opts[restoreOptWalRevert]; ok {
		//walDumpPath := filepath.Join(walPath, timeutil.Now().String())
		//conf, err := dumpsink.ConfFromURI(ctx, walPath)
		if err != nil {
			return err
		}
		exportStore, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, walPath)
		if err != nil {
			return err
		}
		defer exportStore.Close()
		if exportStore.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
			err := exportStore.SetConfig(header)
			if err != nil {
				return err
			}
		}
		var db tree.NameList
		if restoreStmt.DatabaseName != "" {
			db = tree.NameList{tree.Name(restoreStmt.DatabaseName)}
		}
		var sc tree.SchemaList
		if restoreStmt.SchemaName != nil {
			sc = tree.SchemaList{*restoreStmt.SchemaName}
		}
		tb := tree.TablePatterns{}
		if restoreStmt.TableName != nil {
			tb = append(tb, restoreStmt.TableName)
		}
		targets := tree.TargetList{
			Databases: db,
			Schemas:   sc,
			Tables:    tb,
		}
		//sinkAbs为本届点日志的绝对路径，sink为本节点日志的相对路径

		var sinkAbs string
		if exportStore.(*dumpsink.LocalFileSink).IsNodeLocal() {
			sinkAbs = exportStore.(*dumpsink.LocalFileSink).AbsPath()
		} else {
			//将远端节点的log文件拷贝到本地
			sinkAbs, err = copyLogToNodeLocal(ctx, exportStore, walPath, p)
			if err != nil {
				return err
			}
		}

		walDumpPath := strings.Join([]string{walPath, "DUMP-" + timeutil.Now().Format("20060102-150405")}, string(os.PathSeparator))

		dumpStore, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, walDumpPath)
		if err != nil {
			return err
		}
		defer dumpStore.Close()
		var header string
		if exportStore.Conf().Provider == roachpb.ExportStorageProvider_Http && header != "" {
			err := exportStore.SetConfig(header)
			if err != nil {
				return err
			}
		}

		//TODO 当前的wal日志路径仅支持节点本地目录，需要进一步支持远端路径
		dumpDesc, err := WalDumpTarget(ctx, p, p.ExecCfg().DB, backupDescs, dumpStore, sinkAbs, &targets, endTime)
		if err != nil {
			return err
		}
		backupDescs = append(backupDescs, *dumpDesc)
		from = append(from, walDumpPath)
		restoreStmt.Files = append(restoreStmt.Files, tree.NewDString(walDumpPath))
	}

	// Validate that the table coverage of the backup matches that of the restore.
	// This prevents FULL CLUSTER backups to be restored as anything but full
	// cluster restores and vice-versa.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors && backupDescs[0].DescriptorCoverage == tree.RequestedDescriptors {
		return errors.Errorf("full cluster RESTORE can only be used on full cluster BACKUP files")
	}

	// Ensure that no user table descriptors exist for a full cluster restore.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		if err = retryCountUserDescriptors(ctx, p); err != nil {
			return err
		}
	}

	if !endTime.IsEmpty() {
		ok := false
		for _, b := range backupDescs {
			// Find the backup that covers the requested time.
			if b.StartTime.Less(endTime) && !b.EndTime.Less(endTime) {
				ok = true
				// Ensure that the backup actually has revision history.
				if b.MVCCFilter != MVCCFilter_All {
					return errors.Errorf(
						"incompatible LOAD timestamp: DUMP for requested time  needs option '%s'", dumpOptRevisionHistory,
					)
				}
				// Ensure that the revision history actually covers the requested time -
				// while the BACKUP's start and end might contain the requested time for
				// example if start time is 0 (full backup), the revision history was
				// only captured since the GC window. Note that the RevisionStartTime is
				// the latest for ranges backed up.
				if !b.RevisionStartTime.Less(endTime) {
					return errors.Errorf(
						"incompatible LOAD timestamp: DUMP for requested time only has revision history from %v", b.RevisionStartTime,
					)
				}
			}
		}
		if !ok {
			return errors.Errorf(
				"incompatible LOAD timestamp: supplied dumps do not cover requested time",
			)
		}
	}

	var sqlDescs []sqlbase.Descriptor
	var restoreDBs []*sqlbase.DatabaseDescriptor
	var restoreSCs []*sqlbase.SchemaDescriptor

	if restoreStmt.TableName != nil || restoreStmt.SchemaName != nil || restoreStmt.DatabaseName != "" || restoreStmt.DescriptorCoverage != tree.RequestedDescriptors {
		//修改该方法使其可对数据库任意对象服务（目前是DATABASES、SCHEMA、TABLE）
		sqlDescs, restoreDBs, restoreSCs, err = selectTargetsObject(ctx, p, backupDescs, restoreStmt, endTime, restoreStmt.DescriptorCoverage)
		if err != nil {
			return err
		}
		//对于整库还原进行super user 检查
		if len(restoreDBs) > 0 {
			err = p.RequireAdminRole(ctx, "LOAD DATABASE")
			if err != nil {
				return err
			}
		}
	}

	tableRewrites, err := allocateTableRewrites(ctx, p, sqlDescs, restoreDBs, restoreSCs, restoreStmt.DescriptorCoverage, opts)
	if err != nil {
		return err
	}
	description, err := restoreJobDescription(restoreStmt, from, opts)
	if err != nil {
		return err
	}
	var id int32
	var tables []*sqlbase.TableDescriptor
	var schemas []*sqlbase.SchemaDescriptor
	for _, desc := range sqlDescs {
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			tables = append(tables, tableDesc)
		} else if schemaDesc := desc.GetSchema(); schemaDesc != nil {
			schemas = append(schemas, schemaDesc)
		}
	}
	if err := RewriteSchemaDescs(schemas, tableRewrites, opts[restoreOptIntoDB]); err != nil {
		return err
	}
	var ViewOnly bool
	if len(tables) == 1 {
		for _, table := range tables {
			if table.IsView() {
				ViewOnly = true
			}
		}
	}
	if err := RewriteTableDescs(ctx, p.Txn(), tables, tableRewrites, opts[restoreOptIntoDB], ViewOnly); err != nil {
		return err
	}

	_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
		Description: description,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
			for _, tableRewrite := range tableRewrites {
				sqlDescIDs = append(sqlDescIDs, tableRewrite.TableID)
			}
			return sqlDescIDs
		}(),
		Details: jobspb.RestoreDetails{
			EndTime:            endTime,
			TableRewrites:      tableRewrites,
			URIs:               from,
			TableDescs:         tables,
			OverrideDB:         opts[restoreOptIntoDB],
			DescriptorCoverage: restoreStmt.DescriptorCoverage,
			Encryption:         encryption,
			HttpHeader:         header,
		},
		Progress: jobspb.RestoreProgress{},
	})
	if err != nil {
		return err
	}
	log.Info(ctx, "Start Restore Job")
	if restoreStmt.DatabaseName != "" {
		if len(schemas) == 1 && len(tables) == 0 {
			i := int32(len(schemas)) + 1
			id = int32(schemas[0].ID)
			id = id - i
		}
		if len(schemas) == 1 && len(tables) != 0 {
			i := int32(len(schemas))
			id = int32(schemas[0].ID)
			id = id - i
		}
		if len(schemas) != 1 && len(tables) == 0 {
			i := int32(len(schemas)) + 1
			id = int32(schemas[0].ID)
			id = id - i
		}
		if len(schemas) != 1 && len(tables) != 0 {
			i := int32(len(schemas))
			id = int32(schemas[0].ID)
			id = id - i
		}
	}
	if restoreStmt.SchemaName != nil {
		id = int32(schemas[0].ID)
	}
	if restoreStmt.TableName != nil {
		id = int32(tables[0].ID)
	}
	*p.GetAuditInfo() = server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(sql.EventLogRestore),
		TargetInfo: &server.TargetInfo{
			TargetID: id,
		},
		Info: "User name:" + p.User() + " Statement:" + description,
	}
	return <-errCh
}

//在集群还原失败时，需等待上次集群GC完毕再进行集群的还原
//这是一个重试函数，重复查询集群中是否存在用户数据
//只有超过重试次数集群任然存在用户表才会返回错误
func retryCountUserDescriptors(ctx context.Context, p sql.PlanHookState) error {
	var descCount int
	var err error
	//判断当前是否存在desc状态为public,如果有无需重试，可以直接返回err
	var noWaitForGC bool
	for i := 0; i < 20; i++ {
		txnName := fmt.Sprintf("count-user-descs %d", i)
		txn := p.ExecCfg().DB.NewTxn(ctx, txnName)
		descCount, noWaitForGC, err = sql.CountUserDescriptors(ctx, txn)
		if err != nil {
			return errors.Wrap(err, "looking up user descriptors during restore")
		}
		if noWaitForGC {
			break
		}
		if err := txn.Commit(ctx); err != nil {
			return err
		}
		if descCount == 0 {
			break
		}
		select {
		case <-time.After(5 * time.Second):
		}
	}
	if descCount != 0 {
		return errors.Errorf(
			"full cluster restore can only be run on a cluster with no tables or databases but found %d descriptors",
			descCount,
		)
	}
	return nil
}

//copyLogToNodeLocal copy log files from remote to local
func copyLogToNodeLocal(
	ctx context.Context, remote dumpsink.DumpSink, walPath string, p sql.PlanHookState,
) (sinkAbs string, err error) {
	//获取本地绝对路径
	wal := strings.Split(walPath, string(os.PathSeparator))
	walend := wal[len(wal)-1]
	sink := strings.Join([]string{"nodelocal:/", p.ExecCfg().NodeID.String(), walend}, string(os.PathSeparator))
	sinkPath := strings.Join([]string{sink, "WAL-" + timeutil.Now().Format("20060102-150405")}, string(os.PathSeparator))
	local, err := p.ExecCfg().DistSQLSrv.DumpSinkFromURI(ctx, sinkPath)
	if err != nil {
		return sinkAbs, err
	}
	conf, err := dumpsink.ConfFromURI(ctx, sinkPath)
	url, err := p.ExecCfg().DistSQLSrv.DumpSink(ctx, conf)

	sinkAbs = url.(*dumpsink.LocalFileSink).AbsPath()

	files, err := remote.(*dumpsink.LocalFileSink).RecursionLogList(ctx)
	if err != nil {
		return sinkAbs, err
	}

	for _, file := range files {
		if strings.HasPrefix(file, "WAL-") {
			continue
		}
		f, err := remote.ReadFile(ctx, file)
		if err != nil {
			return sinkAbs, err
		}
		err = local.WriteFileWithReader(ctx, file, f)
		if err != nil {
			return sinkAbs, err
		}
	}
	return sinkAbs, err
}
func loadBackupSQLDescs(
	ctx context.Context,
	details jobspb.RestoreDetails,
	settings *cluster.Settings,
	dumpSinkFromURI dumpsink.FromURIFactory,
	encryption *roachpb.FileEncryptionOptions,
) ([]DumpDescriptor, []sqlbase.Descriptor, error) {
	backupDescs, err := loadBackupDescs(ctx, details.URIs, settings, dumpSinkFromURI, encryption, details.HttpHeader)
	if err != nil {
		return nil, nil, err
	}

	allDescs, _ := loadSQLDescsFromBackupsAtTime(backupDescs, details.EndTime)

	var sqlDescs []sqlbase.Descriptor
	for _, desc := range allDescs {
		if _, ok := details.TableRewrites[desc.GetID()]; ok {
			sqlDescs = append(sqlDescs, desc)
		}
	}
	return backupDescs, sqlDescs, nil
}

type restoreResumer struct {
	settings           *cluster.Settings
	res                roachpb.BulkOpSummary
	databases          []*sqlbase.DatabaseDescriptor
	schemas            []*sqlbase.SchemaDescriptor
	tables             []*sqlbase.TableDescriptor
	descriptorCoverage tree.DescriptorCoverage
	statsRefresher     *stats.Refresher
	execCfg            *sql.ExecutorConfig
	// duringSystemTableRestoration is called once for every system table we
	// restore. It is used to simulate any errors that we may face at this point
	// of the restore.
	testingKnobs struct {
		duringSystemTableRestoration func(systemTableName string) error
	}
}

func (r *restoreResumer) Resume(
	ctx context.Context, job *jobs.Job, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := job.Details().(jobspb.RestoreDetails)
	p := phs.(sql.PlanHookState)
	r.execCfg = p.ExecCfg()
	backupDescs, sqlDescs, err := loadBackupSQLDescs(ctx, details, r.settings, p.ExecCfg().DistSQLSrv.DumpSinkFromURI, details.Encryption)
	if err != nil {
		return err
	}
	res, databases, schemas, tables, err := restore(
		ctx,
		p.ExecCfg().DB,
		p.ExecCfg().Gossip,
		backupDescs,
		details.EndTime,
		sqlDescs,
		details.TableRewrites,
		details.OverrideDB,
		job,
		resultsCh,
		details.Encryption,
		details.HttpHeader,
	)
	if err != nil {
		return err
	}
	var tempSystemDBID sqlbase.ID
	tempSystemDBID = sql.MaxDefaultDescriptorID
	for id := range details.TableRewrites {
		if uint32(id) > uint32(tempSystemDBID) {
			tempSystemDBID = id
		}
	}
	systemTable := make([]*sqlbase.TableDescriptor, 0, len(tables))
	userTable := make([]*sqlbase.TableDescriptor, 0, len(tables))
	if details.DescriptorCoverage == tree.AllDescriptors {
		for _, tbl := range tables {
			if tbl.ParentID == tempSystemDBID {
				systemTable = append(systemTable, tbl)
			} else {
				userTable = append(userTable, tbl)
			}
		}
		tables = userTable
		schemasTmp := make([]*sqlbase.SchemaDescriptor, 0, 1)
		databasesTmp := make([]*sqlbase.DatabaseDescriptor, 0, 1)
		schemasTmp = append(schemasTmp, &sqlbase.SchemaDescriptor{
			Name:     restoreTempPublicSchema,
			ID:       tempSystemDBID,
			ParentID: tempSystemDBID - 1,
		})
		databasesTmp = append(databasesTmp, &sqlbase.DatabaseDescriptor{
			ID:         tempSystemDBID - 1,
			Name:       restoreTempSystemDB,
			Privileges: sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Database, p.User()), // todo(xz): owner?
		})
		if !details.SystemTmpRestored {
			err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				if err := WriteTableDescs(ctx, txn, databasesTmp, schemasTmp, systemTable, job.Payload().Username, r.settings, nil, details.DescriptorCoverage); err != nil {
					return errors.Wrapf(err, "restoring %d TableDescriptors", len(systemTable))
				}
				details.SystemTmpRestored = true
				details.PrepareCompleted = true
				err = job.WithTxn(txn).SetDetails(ctx, details)
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	r.res = res
	r.databases = databases
	r.schemas = schemas
	r.tables = tables
	r.descriptorCoverage = details.DescriptorCoverage
	r.statsRefresher = p.ExecCfg().StatsRefresher
	if r.descriptorCoverage == tree.AllDescriptors {
		if err := r.restoreSystemTables(ctx, job); err != nil {
			return err
		}
		r.cleanupTempSystemTables(ctx)
	}
	if &r.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "finish resume restore job :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, status, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "finish resume restore job :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusRunning, r.res.Rows, r.res.IndexEntries, r.res.SystemRecords, r.res.DataSize)
	} else {
		log.Error(ctx, "r.res or job is a nil Pointer")
	}
	return err
}

// OnFailOrCancel removes KV data that has been committed from a restore that
// has failed or been canceled. It does this by adding the table descriptors
// in DROP state, which causes the schema change stuff to delete the keys
// in the background.
func (r *restoreResumer) OnFailOrCancel(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	if &r.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "restore job is onFailOrCancel :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, status, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "restore job is onFailOrCancel :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusRunning, r.res.Rows, r.res.IndexEntries, r.res.SystemRecords, r.res.DataSize)
	} else {
		log.Error(ctx, "r.res or job is a nil Pointer")
	}
	details := job.Details().(jobspb.RestoreDetails)

	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}
	if details.DescriptorCoverage == tree.AllDescriptors {
		r.cleanupTempSystemTables(ctx)
	}
	return r.dropDescriptors(ctx, r.execCfg.JobRegistry, txn, job)
}

func (r *restoreResumer) OnSuccess(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	if &r.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "restore job is onSuccess :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, status, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "restore job is onSuccess :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusRunning, r.res.Rows, r.res.IndexEntries, r.res.SystemRecords, r.res.DataSize)
	} else {
		log.Error(ctx, "r.res or job is a nil Pointer")
	}
	log.Event(ctx, "making tables live")

	details := job.Details().(jobspb.RestoreDetails)
	// Write the new TableDescriptors and flip the namespace entries over to
	// them. After this call, any queries on a table will be served by the newly
	// restored data.
	if err := WriteTableDescs(ctx, txn, r.databases, r.schemas, r.tables, job.Payload().Username, r.settings, nil, details.DescriptorCoverage); err != nil {
		return errors.Wrapf(err, "restoring %d TableDescriptors", len(r.tables))
	}
	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range r.tables {
		r.statsRefresher.NotifyMutation(r.tables[i].ID, math.MaxInt32 /* rowsAffected */)
	}

	return nil
}

func (r *restoreResumer) OnTerminal(
	ctx context.Context, job *jobs.Job, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	if &r.res != nil && job != nil && job.ID() != nil {
		log.Infof(ctx, "restore job is onTerminal :%18s|%10s|%10s|%13s|%20s|%20s|", jobID, status, rows, indexEntries, systemRecords, recordBytes)
		log.Infof(ctx, "restore job is onTerminal :%18d|%10s|%10d|%13d|%20d|%20d|", *job.ID(), jobs.StatusRunning, r.res.Rows, r.res.IndexEntries, r.res.SystemRecords, r.res.DataSize)
	} else {
		log.Error(ctx, "r.res or job is a nil Pointer")
	}

	if status == jobs.StatusSucceeded {
		// TODO(benesch): emit periodic progress updates.

		// TODO(mjibson): if a restore was resumed, then these counts will only have
		// the current coordinator's counts.

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(r.res.Rows)),
			tree.NewDInt(tree.DInt(r.res.IndexEntries)),
			tree.NewDInt(tree.DInt(r.res.SystemRecords)),
			tree.NewDInt(tree.DInt(r.res.DataSize)),
		}
	}
}
func (r *restoreResumer) restoreSystemTables(ctx context.Context, job *jobs.Job) error {
	var err error
	details := job.Details().(jobspb.RestoreDetails)
	if details.SystemTablesRestored == nil {
		details.SystemTablesRestored = make(map[string]bool)
	}

	for systemTableName, config := range SystemTableBackupConfiguration {
		if details.SystemTablesRestored[systemTableName] {
			// We've already restored this table.
			continue
		}
		stagingTableName := restoreTempSystemDB + "." + systemTableName
		if err := r.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			err := txn.SetDebugName("system-restore-txn")
			if err != nil {
				return err
			}

			restoreFunc := DefaultSystemTableRestoreFunc
			if config.CustomRestoreFunc != nil {
				restoreFunc = config.CustomRestoreFunc
				log.Eventf(ctx, "using custom restore function for table %s", systemTableName)
			}

			log.Eventf(ctx, "restoring system table %s", systemTableName)
			err = restoreFunc(ctx, r.execCfg, txn, systemTableName, stagingTableName)
			if err != nil {
				return errors.Wrapf(err, "restoring system table %s", systemTableName)
			}
			return nil
		}); err != nil {
			return err
		}
		// System table restoration may not be idempotent, so we need to keep
		// track of what we've restored.
		details.SystemTablesRestored[systemTableName] = true
		err = job.SetDetails(ctx, details)
		if err != nil {
			return err
		}
		if fn := r.testingKnobs.duringSystemTableRestoration; fn != nil {
			if err := fn(systemTableName); err != nil {
				return err
			}
		}
	}
	return nil
}

// dropDescriptors implements the OnFailOrCancel logic.
// TODO (lucy): If the descriptors have already been published, we need to queue
// drop jobs for all the descriptors.
func (r *restoreResumer) dropDescriptors(
	ctx context.Context, jr *jobs.Registry, txn *client.Txn, job *jobs.Job,
) error {
	details := job.Details().(jobspb.RestoreDetails)

	// No need to mark the tables as dropped if they were not even created in the
	// first place.
	if !details.PrepareCompleted {
		return nil
	}

	b := txn.NewBatch()

	// Remove any back references installed from existing types to tables being restored.
	//if err := r.removeExistingTypeBackReferences(
	//	ctx, txn, descsCol, b, mutableTables, &details,
	//); err != nil {
	//	return err
	//}

	// Drop the table descriptors that were created at the start of the restore.
	tablesToGC := make([]sqlbase.ID, 0, len(details.TableDescs))
	for _, tblDesc := range details.TableDescs {
		tableToDrop := tblDesc
		tablesToGC = append(tablesToGC, tblDesc.ID)
		tableToDrop.State = sqlbase.TableDescriptor_DROP
		b.Del(tableToDrop.GetDescMetadataKey())
		b.Del(tableToDrop.GetNameMetadataKey())
		if err := sql.WriteDescToBatch(ctx, false /* kvTrace */, nil, b, tableToDrop.ID, tableToDrop); err != nil {
			return errors.Wrap(err, "writing dropping table to batch")
		}
	}

	// Queue a GC job.
	// Set the drop time as 1 (ns in Unix time), so that the table gets GC'd
	// immediately.
	dropTime := int64(1)
	gcDetails := jobspb.SchemaChangeGCDetails{}
	for _, tableID := range tablesToGC {
		gcDetails.Tables = append(gcDetails.Tables, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       tableID,
			DropTime: dropTime,
		})
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for %s", job.Payload().Description),
		Username:      job.Payload().Username,
		DescriptorIDs: tablesToGC,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := jr.CreateJobWithTxn(ctx, gcJobRecord, txn); err != nil {
		return err
	}

	// Drop the database and schema descriptors that were created at the start of
	// the restore if they are now empty (i.e. no user created a table, etc. in
	// the database or schema during the restore).
	ignoredChildDescIDs := make(map[sqlbase.ID]struct{})
	for _, table := range details.TableDescs {
		ignoredChildDescIDs[table.ID] = struct{}{}
	}
	for _, schema := range details.SchemaDescs {
		ignoredChildDescIDs[schema.ID] = struct{}{}
	}
	allDescs, err := sql.GetAllDescriptors(ctx, txn)
	if err != nil {
		return err
	}

	// Delete any schema descriptors that this restore created. Also collect the
	// descriptors so we can update their parent databases later.
	dbsWithDeletedSchemas := make(map[sqlbase.ID]map[sqlbase.ID]*sqlbase.SchemaDescriptor)
	for _, schemaDesc := range details.SchemaDescs {
		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isSchemaEmpty, err := isSchemaEmpty(ctx, txn, schemaDesc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if schema %s is empty during restore cleanup", schemaDesc.GetName())
		}

		if !isSchemaEmpty {
			log.Warningf(ctx, "preserving schema %s on restore failure because it contains new child objects", schemaDesc.GetName())
			continue
		}

		b.Del(sqlbase.MakeNameMetadataKey(schemaDesc.GetParentID(), schemaDesc.Name))
		b.Del(sqlbase.MakeDescMetadataKey(schemaDesc.GetID()))
		if schemaMap, ok := dbsWithDeletedSchemas[schemaDesc.GetParentID()]; ok {
			schemaMap[schemaDesc.ID] = schemaDesc
			dbsWithDeletedSchemas[schemaDesc.GetParentID()] = schemaMap
		} else {
			schemaMap = make(map[sqlbase.ID]*sqlbase.SchemaDescriptor)
			schemaMap[schemaDesc.ID] = schemaDesc
			dbsWithDeletedSchemas[schemaDesc.GetParentID()] = schemaMap
		}
	}

	// Delete the database descriptors.
	deletedDBs := make(map[sqlbase.ID]struct{})
	for _, dbDesc := range details.DatabaseDescs {

		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isDBEmpty, err := isDatabaseEmpty(ctx, txn, dbDesc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if database %s is empty during restore cleanup", dbDesc.GetName())
		}
		if !isDBEmpty {
			log.Warningf(ctx, "preserving database %s on restore failure because it contains new child objects or schemas", dbDesc.GetName())
			continue
		}

		db, err := sqlbase.GetDatabaseDescFromID(ctx, txn, dbDesc.GetID())
		if err != nil {
			return err
		}

		// Mark db as dropped and add uncommitted version to pass pre-txn
		// descriptor validation.
		//db.SetDropped()
		//db.MaybeIncrementVersion()
		//if err := descsCol.AddUncommittedDescriptor(db); err != nil {
		//	return err
		//}

		descKey := sqlbase.MakeDescMetadataKey(db.GetID())
		b.Del(descKey)
		nameKey := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, db.GetName())
		b.Del(nameKey)
		//descsCol.AddDeletedDescriptor(db)
		deletedDBs[db.GetID()] = struct{}{}
	}

	// For each database that had a child schema deleted (regardless of whether
	// the db was created in the restore job), if it wasn't deleted just now,
	// delete the now-deleted child schema from its schema map.
	for dbID, schemas := range dbsWithDeletedSchemas {
		log.Infof(ctx, "deleting %d schema entries from database %d", len(schemas), dbID)
		desc, err := sqlbase.GetDatabaseDescFromID(ctx, txn, dbID)
		if err != nil {
			return err
		}
		for i, sc := range desc.Schemas {
			if schemaInfo, ok := schemas[sc.ID]; !ok {
				log.Warningf(ctx, "unexpected missing schema entry for %s from db %d; skipping deletion",
					sc.GetName(), dbID)
			} else if schemaInfo.ID != sc.GetID() {
				log.Warningf(ctx, "unexpected schema entry %d for %s from db %d, expecting %d; skipping deletion",
					schemaInfo.ID, sc.GetName(), dbID, sc.GetID())
			} else {
				desc.Schemas = append(desc.Schemas[:i], desc.Schemas[i+1:]...)
			}
		}
		if err := sql.WriteDescToBatch(ctx, false /* kvTrace */, nil, b, desc.ID, desc); err != nil {
			return err
		}
	}

	if err := txn.Run(ctx, b); err != nil {
		return errors.Wrap(err, "dropping tables created at the start of restore caused by fail/cancel")
	}

	return nil
}

// removeExistingTypeBackReferences removes back references from types that
// exist in the cluster to tables restored. It is used when rolling back from
// a failed restore.
//func (r *restoreResumer) removeExistingTypeBackReferences(
//	ctx context.Context,
//	txn *client.Txn,
//	descsCol *sqlbase.Collection,
//	b *client.Batch,
//	restoredTables []*tabledesc.Mutable,
//	details *jobspb.RestoreDetails,
//) error {
//	// We first collect the restored types to be addressable by ID.
//	restoredTypes := make(map[sqlbase.ID]catalog.TypeDescriptor)
//	existingTypes := make(map[sqlbase.ID]*typedesc.Mutable)
//	for i := range details.TypeDescs {
//		typ := details.TypeDescs[i]
//		restoredTypes[typ.ID] = typedesc.NewBuilder(typ).BuildImmutableType()
//	}
//	for _, tbl := range restoredTables {
//		lookup := func(id descpb.ID) (catalog.TypeDescriptor, error) {
//			// First see if the type was restored.
//			restored, ok := restoredTypes[id]
//			if ok {
//				return restored, nil
//			}
//			// Finally, look it up using the transaction.
//			typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
//			if err != nil {
//				return nil, err
//			}
//			existingTypes[typ.GetID()] = typ
//			return typ, nil
//		}
//
//		_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
//			ctx, txn, tbl.GetParentID(), tree.DatabaseLookupFlags{
//				Required:       true,
//				AvoidCached:    true,
//				IncludeOffline: true,
//			})
//		if err != nil {
//			return err
//		}
//
//		// Get all types that this descriptor references.
//		referencedTypes, err := tbl.GetAllReferencedTypeIDs(dbDesc, lookup)
//		if err != nil {
//			return err
//		}
//
//		// For each type that is existing, remove the backreference from tbl.
//		for _, id := range referencedTypes {
//			_, restored := restoredTypes[id]
//			if !restored {
//				desc, err := lookup(id)
//				if err != nil {
//					return err
//				}
//				existing := desc.(*typedesc.Mutable)
//				existing.MaybeIncrementVersion()
//				existing.RemoveReferencingDescriptorID(tbl.ID)
//			}
//		}
//	}
//
//	// Now write any changed existing types.
//	for _, typ := range existingTypes {
//		if typ.IsUncommittedVersion() {
//			if err := descsCol.WriteDescToBatch(
//				ctx, false /* kvTrace */, typ, b,
//			); err != nil {
//				return err
//			}
//		}
//	}
//
//	return nil
//}

func isSchemaEmpty(
	ctx context.Context,
	txn *client.Txn,
	schemaID sqlbase.ID,
	allDescs []sqlbase.DescriptorProto,
	ignoredChildren map[sqlbase.ID]struct{},
) (bool, error) {
	for _, desc := range allDescs {
		if _, ok := ignoredChildren[desc.GetID()]; ok {
			continue
		}
		if tbl, ok := desc.(*sqlbase.TableDescriptor); ok {
			if tbl.ParentID == schemaID {
				return false, nil
			}
		}

	}
	return true, nil
}

// isDatabaseEmpty checks if there exists any tables in the given database.
// It pretends that the ignoredChildren do not exist for the purposes of
// checking if a database is empty.
//
// It is used to construct a transaction which deletes a set of tables as well
// as some empty databases. However, we want to check that the databases are
// empty _after_ the transaction would have completed, so we want to ignore
// the tables that we're deleting in the same transaction. It is done this way
// to avoid having 2 transactions reading and writing the same keys one right
// after the other.
func isDatabaseEmpty(
	ctx context.Context,
	txn *client.Txn,
	dbID sqlbase.ID,
	allDescs []sqlbase.DescriptorProto,
	ignoredChildren map[sqlbase.ID]struct{},
) (bool, error) {
	for _, desc := range allDescs {
		if _, ok := ignoredChildren[desc.GetID()]; ok {
			continue
		}
		if sc, ok := desc.(*sqlbase.SchemaDescriptor); ok {
			if sc.ParentID == dbID {
				return false, nil
			}
		}
	}
	return true, nil
}
func (r *restoreResumer) cleanupTempSystemTables(ctx context.Context) {
	executor := r.execCfg.InternalExecutor
	// After restoring the system tables, drop the temporary database holding the
	// system tables.
	//alterDefaultRange := fmt.Sprintf("ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds=1")
	//_, err := executor.Exec(ctx, "drop-temp-system-db" /* opName */, nil /* txn */, alterDefaultRange)
	//if err != nil {
	//	log.Errorf(ctx, "dropping temporary system db:%s", err)
	//}
	dropTableQuery := fmt.Sprintf("DROP DATABASE %s CASCADE", restoreTempSystemDB)
	_, err := executor.Exec(ctx, "drop-temp-system-db" /* opName */, nil /* txn */, dropTableQuery)
	if err != nil {
		log.Errorf(ctx, "dropping temporary system db:%s", err)
	}
}

var _ jobs.Resumer = &restoreResumer{}

func restoreResumeHook(typ jobspb.Type, settings *cluster.Settings) jobs.Resumer {
	if typ != jobspb.TypeRestore {
		return nil
	}

	return &restoreResumer{
		settings: settings,
	}
}

func init() {
	sql.AddPlanHook(restorePlanHook)
	jobs.AddResumeHook(restoreResumeHook)
}
