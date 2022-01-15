// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package dump

import (
	"context"
	fmt "fmt"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// clusterBackupInclusion is an enum that specifies whether a system table
// should be included in a cluster backup.
type clusterBackupInclusion int

const (
	// OptInToClusterBackup indicates that the system table
	// should be included in the cluster backup.
	OptInToClusterBackup = iota
	// OptOutOfClusterBackup indicates that the system table
	// should not be included in the cluster backup.
	OptOutOfClusterBackup
)

// systemBackupConfiguration holds any configuration related to backing up
// system tables. System tables differ from normal tables with respect to backup
// for 2 reasons:
// 1) For some tables, their contents are read during the restore, so it is non-
//    trivial to restore this data without affecting the restore job itself.
//
// 2) It may reference system data which could be rewritten. This is particularly
//    problematic for data that references tables. At time of writing, cluster
//    restore creates descriptors with the same ID as they had in the backing up
//    cluster so there is no need to rewrite system table data.
type systemBackupConfiguration struct {
	IncludeInClusterBackup clusterBackupInclusion
	// RestoreBeforeData indicates that this system table should be fully restored
	// before restoring the user data. If a system table is restored before the
	// user data, the restore will see this system table during the restore.
	// The default is `false` because most of the system tables should be set up
	// to support the restore (e.g. users that can run the restore, cluster settings
	// that control how the restore runs, etc...).
	RestoreBeforeData bool
	// CustomRestoreFunc is responsible for restoring the data from a table that
	// holds the restore system table data into the given system table. If none
	// is provided then `defaultRestoreFunc` is used.
	CustomRestoreFunc func(ctx context.Context, execCtx *sql.ExecutorConfig, txn *client.Txn, systemTableName, tempTableName string) error
}

// DefaultSystemTableRestoreFunc is how system table data is restored. This can
// be overwritten with the system table's
// systemBackupConfiguration.CustomRestoreFunc.
func DefaultSystemTableRestoreFunc(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *client.Txn,
	systemTableName, tempTableName string,
) error {
	executor := execCfg.InternalExecutor

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	opName := systemTableName + "-data-deletion"
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err := executor.Exec(ctx, opName, txn, deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}
	selectQuery := fmt.Sprintf("SELECT * FROM %s", tempTableName)
	opName = systemTableName + "-data-select"
	datums, err := executor.Query(ctx, opName, txn, selectQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}
	for _, datum := range datums {
		for _, d := range datum {
			log.Errorf(ctx, "%s", d)
		}
	}

	restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s);",
		systemTableName, tempTableName)
	opName = systemTableName + "-data-insert"
	if _, err := executor.Exec(ctx, opName, txn, restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}
	return nil
}

// Custom restore functions for different system tables.

// When restoring the jobs table we don't want to remove existing jobs, since
// that includes the restore that we're running.
func jobsRestoreFunc(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *client.Txn,
	systemTableName, tempTableName string,
) error {
	executor := execCfg.InternalExecutor

	// When restoring jobs, don't clear the existing table.

	restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s);",
		systemTableName, tempTableName)
	opName := systemTableName + "-data-insert"
	if _, err := executor.Exec(ctx, opName, txn, restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}
	return nil
}

// When restoring the settings table, we want to make sure to not override the
// version.
func settingsRestoreFunc(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *client.Txn,
	systemTableName, tempTableName string,
) error {
	executor := execCfg.InternalExecutor

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE name <> 'version'", systemTableName)
	opName := systemTableName + "-data-deletion"
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err := executor.Exec(ctx, opName, txn, deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s WHERE name <> 'version');",
		systemTableName, tempTableName)
	opName = systemTableName + "-data-insert"
	if _, err := executor.Exec(ctx, opName, txn, restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}
	return nil
}

// SystemTableBackupConfiguration is a map from every systemTable present in the
// cluster to a configuration struct which specifies how it should be treated by
// backup. Every system table should have a specification defined here, enforced
// by TestAllSystemTablesHaveBackupConfig.
var SystemTableBackupConfiguration = map[string]systemBackupConfiguration{
	sqlbase.UsersTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},
	sqlbase.ZonesTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
		// The zones table should be restored before the user data so that the range
		// allocator properly distributes ranges during the restore.
		RestoreBeforeData: true,
	},
	sqlbase.SettingsTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
		CustomRestoreFunc:      settingsRestoreFunc,
	},
	sqlbase.LocationsTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},
	sqlbase.LeaseTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},
	sqlbase.EventLogTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},
	sqlbase.RangeEventTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},
	sqlbase.UITable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},

	sqlbase.JobsTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
		CustomRestoreFunc:      jobsRestoreFunc,
	},
	sqlbase.TableStatisticsTable.GetName(): {
		// Table statistics are backed up in the backup descriptor for now.
		IncludeInClusterBackup: OptOutOfClusterBackup,
	},
	sqlbase.WebSessionsTable.GetName(): {
		IncludeInClusterBackup: OptOutOfClusterBackup,
	},
	sqlbase.TableStatisticsTable.GetName(): {
		IncludeInClusterBackup: OptOutOfClusterBackup,
	},
	sqlbase.LocationsTable.GetName(): {
		IncludeInClusterBackup: OptOutOfClusterBackup,
	},
	sqlbase.RoleMembersTable.GetName(): {
		IncludeInClusterBackup: OptOutOfClusterBackup,
	},
	sqlbase.CommentsTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},
	sqlbase.SnapshotsTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},
	sqlbase.AuthenticationTable.GetName(): {
		IncludeInClusterBackup: OptInToClusterBackup,
	},
}
