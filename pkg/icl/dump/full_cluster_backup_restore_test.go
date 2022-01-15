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
	gosql "database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/testutils/testcluster"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/workload"
	"github.com/znbasedb/znbase/pkg/workload/bank"
)

const (
	singleNode = 1
	localFoo   = "nodelocal:///foo"
)

func initNone(_ *testcluster.TestCluster) {}

// Large test to ensure that all of the system table data is being restored in
// the new cluster. Ensures that all the moving pieces are working together.
func TestFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	// The claim_session_id field in jobs is a uuid and so needs to be excluded
	// when comparing jobs pre/post restore.
	const jobsQuery = `
SELECT id, status, created, payload, progress, created_by_type, created_by_id
FROM system.jobs
	`

	// Disable automatic stats collection on the backup and restoring clusters to ensure
	// the test is deterministic.
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)
	sqlDBRestore.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	// Create some other descriptors as well.
	sqlDB.Exec(t, `
USE data;
CREATE SCHEMA test_data_schema;
CREATE TABLE data.test_data_schema.test_table (a int);
INSERT INTO data.test_data_schema.test_table VALUES (1), (2);

USE defaultdb;
CREATE SCHEMA test_schema;
CREATE TABLE defaultdb.test_schema.test_table (a int);
INSERT INTO defaultdb.test_schema.test_table VALUES (1), (2);
CREATE TABLE defaultdb.foo (a int);

CREATE DATABASE data2;
USE data2;
CREATE SCHEMA empty_schema;
CREATE TABLE data2.foo (a int);
`)

	// Setup the system systemTablesToVerify to ensure that they are copied to the new cluster.
	// Populate system.users.
	numUsers := 1000
	if util.RaceEnabled {
		numUsers = 10
	}
	for i := 0; i < numUsers; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
	}
	// Populate system.zones.
	sqlDB.Exec(t, `ALTER TABLE data.bank CONFIGURE ZONE USING gc.ttlseconds = 3600`)
	sqlDB.Exec(t, `ALTER TABLE defaultdb.foo CONFIGURE ZONE USING gc.ttlseconds = 45`)
	sqlDB.Exec(t, `ALTER DATABASE data2 CONFIGURE ZONE USING gc.ttlseconds = 900`)
	// Populate system.jobs.
	// Note: this is not the backup under test, this just serves as a job which
	// should appear in the restore.
	// This job will eventually fail since it will run from a new cluster.
	sqlDB.Exec(t, `DUMP data.bank TO SST 'nodelocal://0/throwawayjob'`)
	preBackupJobs := sqlDB.QueryStr(t, jobsQuery)
	// Populate system.settings.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_io_write.concurrent_addsstable_requests = 5`)
	sqlDB.Exec(t, `INSERT INTO system.ui (key, value, "lastUpdated") VALUES ($1, $2, now())`, "some_key", "some_val")
	// Populate system.comments.
	sqlDB.Exec(t, `COMMENT ON TABLE data.bank IS 'table comment string'`)
	sqlDB.Exec(t, `COMMENT ON DATABASE data IS 'database comment string'`)

	sqlDB.Exec(t,
		`INSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES ($1, $2, $3, $4)`,
		"city", "New York City", 40.71427, -74.00597,
	)

	// Populate system.scheduled_jobs table.
	//sqlDB.Exec(t, `CREATE SCHEDULE FOR BACKUP data.bank INTO $1 RECURRING '@hourly' FULL BACKUP ALWAYS`, localFoo)

	injectStats(t, sqlDB, "data.bank", "id")
	sqlDB.Exec(t, `DUMP ALL CLUSTER TO SST $1`, localFoo)

	// Create a bunch of user tables on the restoring cluster that we're going
	// to delete.
	numTables := 0
	if util.RaceEnabled {
		numTables = 2
	}
	for i := 0; i < numTables; i++ {
		sqlDBRestore.Exec(t, `CREATE DATABASE db_to_drop`)
		sqlDBRestore.Exec(t, `CREATE TABLE db_to_drop.table_to_drop (a int)`)
		sqlDBRestore.Exec(t, `DROP DATABASE db_to_drop cascade`)
		sqlDBRestore.Exec(t, `ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds=1`)
	}
	// Wait for the GC job to finish to ensure the descriptors no longer exist.
	sqlDBRestore.CheckQueryResultsRetry(
		t, "SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE' AND status = 'running'",
		[][]string{{"0"}},
	)

	sqlDBRestore.Exec(t, `LOAD CLUSTER $1`, localFoo)

	t.Run("ensure all databases restored", func(t *testing.T) {
		sqlDBRestore.CheckQueryResults(t,
			`SHOW DATABASES`,
			[][]string{
				{"data", security.RootUser},
				{"data2", security.RootUser},
				{"defaultdb", security.RootUser},
				{"postgres", security.RootUser},
				{"system", security.NodeUser},
			})
	})

	t.Run("ensure all schemas are restored", func(t *testing.T) {
		expectedSchemas := map[string][][]string{
			"defaultdb": {{"information_schema"}, {"pg_catalog"}, {"public"}, {"test_schema"}, {"zbdb_internal"}},
			"data":      {{"information_schema"}, {"pg_catalog"}, {"public"}, {"test_data_schema"}, {"zbdb_internal"}},
			"data2":     {{"empty_schema"}, {"information_schema"}, {"pg_catalog"}, {"public"}, {"zbdb_internal"}},
		}
		for dbName, expectedSchemas := range expectedSchemas {
			sqlDBRestore.CheckQueryResults(t,
				fmt.Sprintf(`USE %s; SELECT schema_name FROM [SHOW SCHEMAS] ORDER BY schema_name;`, dbName),
				expectedSchemas)
		}
	})

	t.Run("ensure system table data restored", func(t *testing.T) {
		// Note the absence of the jobs table. Jobs are tested by another test as
		// jobs are created during the RESTORE process.
		systemTablesToVerify := []string{
			sqlbase.CommentsTable.Name,
			sqlbase.LocationsTable.Name,
			sqlbase.RoleMembersTable.Name,
			sqlbase.SettingsTable.Name,
			sqlbase.TableStatisticsTable.Name,
			sqlbase.UITable.Name,
			sqlbase.UsersTable.Name,
			sqlbase.ZonesTable.Name,
			sqlbase.ScheduledJobsTable.Name,
		}

		verificationQueries := make([]string, len(systemTablesToVerify))
		// Populate the list of tables we expect to be restored as well as queries
		// that can be used to ensure that data in those tables is restored.
		for i, table := range systemTablesToVerify {
			switch table {
			case sqlbase.TableStatisticsTable.Name:
				// createdAt and statisticsID are re-generated on RESTORE.
				query := fmt.Sprintf("SELECT \"tableID\", name, \"columnIDs\", \"rowCount\" FROM system.table_statistics")
				verificationQueries[i] = query
			case sqlbase.SettingsTable.Name:
				// We don't include the cluster version.
				query := fmt.Sprintf("SELECT * FROM system.%s WHERE name <> 'version'", table)
				verificationQueries[i] = query
			default:
				query := fmt.Sprintf("SELECT * FROM system.%s", table)
				verificationQueries[i] = query
			}
		}

		for _, read := range verificationQueries {
			sqlDBRestore.CheckQueryResults(t, read, sqlDB.QueryStr(t, read))
		}
	})

	t.Run("ensure table IDs have not changed", func(t *testing.T) {
		// Check that all tables have been restored. DISTINCT is needed in order to
		// deal with the inclusion of schemas in the system.namespace table.
		tableIDCheck := "SELECT DISTINCT name, id FROM system.namespace"
		sqlDBRestore.CheckQueryResults(t, tableIDCheck, sqlDB.QueryStr(t, tableIDCheck))
	})

	t.Run("ensure user table data restored", func(t *testing.T) {
		expectedUserTables := [][]string{
			{"data", "bank"},
			{"data2", "foo"},
			{"defaultdb", "foo"},
		}

		for _, table := range expectedUserTables {
			query := fmt.Sprintf("SELECT * FROM %s.%s", table[0], table[1])
			sqlDBRestore.CheckQueryResults(t, query, sqlDB.QueryStr(t, query))
		}
	})

	t.Run("ensure that grants are restored", func(t *testing.T) {
		grantCheck := "use system; SHOW grants"
		sqlDBRestore.CheckQueryResults(t, grantCheck, sqlDB.QueryStr(t, grantCheck))
		grantCheck = "use data; SHOW grants"
		sqlDBRestore.CheckQueryResults(t, grantCheck, sqlDB.QueryStr(t, grantCheck))
	})

	t.Run("ensure that jobs are restored", func(t *testing.T) {
		// Ensure that the jobs in the RESTORE cluster is a superset of the jobs
		// that were in the BACKUP cluster (before the full cluster BACKUP job was
		// run). There may be more jobs now because the restore can run jobs of
		// its own.
		newJobsStr := sqlDBRestore.QueryStr(t, jobsQuery)
		newJobs := make(map[string][]string)

		for _, newJob := range newJobsStr {
			// The first element of the slice is the job id.
			newJobs[newJob[0]] = newJob
		}
		for _, oldJob := range preBackupJobs {
			newJob, ok := newJobs[oldJob[0]]
			if !ok {
				t.Errorf("Expected to find job %+v in RESTORE cluster, but not found", oldJob)
			}
			require.Equal(t, oldJob, newJob)
		}
	})

	t.Run("ensure that tables can be created at the excepted ID", func(t *testing.T) {
		var maxID, dbID, tableID int
		sqlDBRestore.QueryRow(t, "SELECT max(id) FROM system.namespace").Scan(&maxID)
		dbName, tableName := "new_db", "new_table"
		sqlDBRestore.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))
		sqlDBRestore.Exec(t, fmt.Sprintf("CREATE TABLE %s.%s (a int)", dbName, tableName))
		sqlDBRestore.QueryRow(t,
			fmt.Sprintf("SELECT id FROM system.namespace WHERE name = '%s'", dbName)).Scan(&dbID)
		require.True(t, dbID > maxID)
		sqlDBRestore.QueryRow(t,
			fmt.Sprintf("SELECT id FROM system.namespace WHERE name = '%s'", tableName)).Scan(&tableID)
		require.True(t, tableID > maxID)
		require.NotEqual(t, dbID, tableID)
	})
}

//TODO(鲍之骁):修复测试用例
func TestFullClusterBackupDroppedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("测试用例无法通过，原因未知")
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	_, tablesToCheck := generateInterleavedData(sqlDB, t, numAccounts)

	sqlDB.Exec(t, `DUMP ALL CLUSTER TO SST $1`, localFoo)
	sqlDBRestore.Exec(t, `LOAD CLUSTER $1`, localFoo)

	for _, table := range tablesToCheck {
		query := fmt.Sprintf("SELECT * FROM data.%s", table)
		sqlDBRestore.CheckQueryResults(t, query, sqlDB.QueryStr(t, query))
	}
}

//
func TestIncrementalFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	const incrementalBackupLocation = "nodelocal://0/inc-full-backup"
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `DUMP ALL CLUSTER  TO SST $1`, localFoo)
	sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach1"))

	sqlDB.Exec(t, `DUMP ALL CLUSTER  TO SST $1 INCREMENTAL FROM $2`, incrementalBackupLocation, localFoo)
	sqlDBRestore.Exec(t, `LOAD CLUSTER $1, $2`, localFoo, incrementalBackupLocation)

	checkQuery := "SELECT * FROM system.users"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

// TestEmptyFullClusterResotre ensures that we can backup and restore a full
// cluster backup with only metadata (no user data). Regression test for #49573.
func TestEmptyFullClusterRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlDB, tempDir, cleanupFn := createEmptyCluster(t, singleNode)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `CREATE USER alice`)
	sqlDB.Exec(t, `CREATE USER bob`)
	sqlDB.Exec(t, `DUMP ALL CLUSTER TO SST $1`, localFoo)
	sqlDBRestore.Exec(t, `LOAD CLUSTER $1`, localFoo)

	checkQuery := "SELECT * FROM system.users"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

// Regression test for #50561.
func TestClusterRestoreEmptyDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `CREATE DATABASE some_db`)
	sqlDB.Exec(t, `CREATE DATABASE some_db_2`)
	sqlDB.Exec(t, `DUMP ALL CLUSTER TO SST $1`, localFoo)
	sqlDBRestore.Exec(t, `LOAD CLUSTER $1`, localFoo)

	checkQuery := "SHOW DATABASES"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

func TestDisallowFullClusterRestoreOnNonFreshCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `DUMP ALL CLUSTER TO SST $1`, localFoo)
	sqlDBRestore.Exec(t, `CREATE DATABASE foo`)
	sqlDBRestore.ExpectErr(t,
		"pq: full cluster restore can only be run on a cluster with no tables or databases but found 2 descriptors",
		`LOAD CLUSTER $1`, localFoo,
	)
}

func TestDisallowFullClusterRestoreOfNonFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `DUMP TABLE data.bank TO SST $1`, localFoo)
	sqlDBRestore.ExpectErr(
		t, "pq: full cluster RESTORE can only be used on full cluster BACKUP files",
		`LOAD CLUSTER $1`, localFoo,
	)
}

func TestAllowNonFullClusterRestoreOfFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `DUMP ALL CLUSTER TO SST $1`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `LOAD TABLE data.bank FROM $1 WITH into_db='data2'`, localFoo)

	checkResults := "SELECT * FROM data.bank"
	sqlDB.CheckQueryResults(t, checkResults, sqlDB.QueryStr(t, checkResults))
}

func TestRestoreFromFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `DUMP ALL CLUSTER TO SST $1`, localFoo)
	sqlDB.Exec(t, `DROP DATABASE data`)

	t.Run("database", func(t *testing.T) {
		sqlDB.Exec(t, `LOAD DATABASE data FROM $1`, localFoo)
		defer sqlDB.Exec(t, `DROP DATABASE data`)
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM data.bank", [][]string{{"10"}})
	})

	t.Run("table", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE data`)
		defer sqlDB.Exec(t, `DROP DATABASE data`)
		sqlDB.Exec(t, `LOAD TABLE data.bank FROM $1`, localFoo)
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM data.bank", [][]string{{"10"}})
	})

	t.Run("system tables", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE temp_sys`)
		sqlDB.Exec(t, `LOAD TABLE system.users FROM $1 WITH into_db='temp_sys'`, localFoo)
		sqlDB.CheckQueryResults(t, "SELECT * FROM temp_sys.users", sqlDB.QueryStr(t, "SELECT * FROM system.users"))
	})
}

// TestClusterRestoreFailCleanup tests that a failed RESTORE is cleaned up.
func TestClusterRestoreFailCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	// Setup the system systemTablesToVerify to ensure that they are copied to the new cluster.
	// Populate system.users.
	for i := 0; i < 1000; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
	}

	sqlDB.Exec(t, ` DUMP ALL CLUSTER TO SST 'nodelocal://1/missing-ssts'`)

	// Bugger the backup by removing the SST files. (Note this messes up all of
	// the backups, but there is only one at this point.)
	if err := filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == DumpDescriptorName || !strings.HasSuffix(path, ".sst") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		t.Fatal(err)
	}

	// Create a non-corrupted backup.
	// Populate system.jobs.
	// Note: this is not the backup under test, this just serves as a job which
	// should appear in the restore.
	// This job will eventually fail since it will run from a new cluster.
	sqlDB.Exec(t, `DUMP TABLE data.bank TO SST 'nodelocal://0/throwawayjob'`)
	sqlDB.Exec(t, `DUMP ALL CLUSTER TO SST $1`, localFoo)

	t.Run("during restoration of data", func(t *testing.T) {
		_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
		defer cleanupEmptyCluster()
		sqlDBRestore.ExpectErr(t, "sst: no such file", `LOAD CLUSTER 'nodelocal://1/missing-ssts'`)
		// Verify the failed RESTORE added some DROP tables.
		// Note that the system tables here correspond to the temporary tables
		// imported, not the system tables themselves.
		sqlDBRestore.CheckQueryResults(t,
			`SELECT name FROM zbdb_internal.tables WHERE state = 'DROP' ORDER BY name`,
			[][]string{},
		)
	})

	// This test retries the job (by injected a retry error) after restoring a
	// every system table that has a custom restore function. This tried to tease
	// out any errors that may occur if some of the system table restoration
	// functions are not idempotent (e.g. jobs table), but are retried by the
	// restore anyway.
	t.Run("retry-during-custom-system-table-restore", func(t *testing.T) {
		defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

		customRestoreSystemTables := []string{sqlbase.SettingsTable.Name, sqlbase.JobsTable.Name}
		for _, customRestoreSystemTable := range customRestoreSystemTables {
			t.Run(customRestoreSystemTable, func(t *testing.T) {
				_, tcRestore, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
				defer cleanupEmptyCluster()

				// Inject a retry error
				for _, server := range tcRestore.Servers {
					registry := server.JobRegistry().(*jobs.Registry)
					registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
						jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
							r := raw.(*restoreResumer)
							r.testingKnobs.duringSystemTableRestoration = func(systemTableName string) error {
								if systemTableName == customRestoreSystemTable {
									return jobs.NewRetryJobError("injected error")
								}
								return nil
							}
							return r
						},
					}
				}

				// The initial restore will fail, and restart.
				sqlDBRestore.ExpectErr(t, `injected error: restarting in background`, `LOAD CLUSTER $1`, localFoo)
				// Expect the job to succeed. If the job fails, it's likely due to
				// attempting to restore the same system table data twice.
				sqlDBRestore.CheckQueryResultsRetry(t,
					`SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'RESTORE' AND status = 'succeeded'`,
					[][]string{{"1"}})
			})
		}
	})

	t.Run("during system table restoration", func(t *testing.T) {
		_, tcRestore, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
		defer cleanupEmptyCluster()

		// Bugger the backup by injecting a failure while restoring the system data.
		for _, server := range tcRestore.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.duringSystemTableRestoration = func(_ string) error {
						return errors.New("injected error")
					}
					return r
				},
			}
		}

		sqlDBRestore.ExpectErr(t, "injected error", `LOAD CLUSTER $1`, localFoo)
		// Verify the failed RESTORE added some DROP tables.
		// Note that the system tables here correspond to the temporary tables
		// imported, not the system tables themselves.
		sqlDBRestore.CheckQueryResults(t,
			`SELECT name FROM zbdb_internal.tables WHERE state = 'DROP' ORDER BY name`,
			[][]string{
				{"authentication"},
				{"bank"},
				{"comments"},
				{"eventlog"},
				{"jobs"},
				{"lease"},
				{"location"},
				{"locations"},
				{"rangelog"},
				{"role_members"},
				{"settings"},
				{"snapshots"},
				{"table_statistics"},
				{"ui"},
				{"users"},
				{"web_sessions"},
				{"zones"},
			},
		)
	})
}

func BackupRestoreTestSetup(
	t testing.TB, clusterSize int, numAccounts int, init func(*testcluster.TestCluster),
) (
	ctx context.Context,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	tempDir string,
	cleanup func(),
) {
	return backupRestoreTestSetupWithParams(t, clusterSize, numAccounts, init, base.TestClusterArgs{})
}

func backupRestoreTestSetupEmpty(
	t testing.TB,
	clusterSize int,
	tempDir string,
	init func(*testcluster.TestCluster),
	params base.TestClusterArgs,
) (ctx context.Context, tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, cleanup func()) {
	return backupRestoreTestSetupEmptyWithParams(t, clusterSize, tempDir, init, params)
}
func backupRestoreTestSetupEmptyWithParams(
	t testing.TB,
	clusterSize int,
	dir string,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (ctx context.Context, tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, cleanup func()) {
	ctx = context.Background()

	params.ServerArgs.ExternalIODir = dir
	if len(params.ServerArgsPerNode) > 0 {
		for i := range params.ServerArgsPerNode {
			param := params.ServerArgsPerNode[i]
			param.ExternalIODir = dir
			params.ServerArgsPerNode[i] = param
		}
	}
	tc = testcluster.StartTestCluster(t, clusterSize, params)
	init(tc)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
	}

	return ctx, tc, sqlDB, cleanupFn
}
func backupRestoreTestSetupWithParams(
	t testing.TB,
	clusterSize int,
	numAccounts int,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (
	ctx context.Context,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	tempDir string,
	cleanup func(),
) {
	ctx = context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	params.ServerArgs.ExternalIODir = dir
	params.ServerArgs.UseDatabase = "data"
	if len(params.ServerArgsPerNode) > 0 {
		for i := range params.ServerArgsPerNode {
			param := params.ServerArgsPerNode[i]
			param.ExternalIODir = dir
			param.UseDatabase = "data"
			params.ServerArgsPerNode[i] = param
		}
	}

	tc = testcluster.StartTestCluster(t, clusterSize, params)
	init(tc)

	const payloadSize = 100
	splits := 10
	if numAccounts == 0 {
		splits = 0
	}
	bankData := bank.FromConfig(numAccounts, payloadSize, splits)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)
	if _, err := workload.Setup(ctx, sqlDB.DB.(*gosql.DB), bankData, 1000, 4); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
		dirCleanupFn()         // cleans up dir, which is the nodelocal:// storage
	}

	return ctx, tc, sqlDB, dir, cleanupFn
}
func injectStats(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableName string, columnName string,
) [][]string {
	return injectStatsWithRowCount(t, sqlDB, tableName, columnName, 100 /* rowCount */)
}

// injectStatsWithRowCount directly injects some statistics specifying some row
// count for a column in the given table.
// N.B. This should be used in backup testing over CREATE STATISTICS since it
// ensures that the stats cache will be up to date during a subsequent BACKUP.
func injectStatsWithRowCount(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableName string, columnName string, rowCount int,
) [][]string {
	sqlDB.Exec(t, fmt.Sprintf(`ALTER TABLE %s INJECT STATISTICS '[
	{
		"columns": ["%s"],
		"created_at": "2018-01-01 1:00:00.00000+00:00",
		"row_count": %d,
		"distinct_count": %d
	}
	]'`, tableName, columnName, rowCount, rowCount))
	return sqlDB.QueryStr(t, getStatsQuery(tableName))
}

// getStatsQuery returns a SQL query that will return the properties of the
// statistics on a table that are expected to remain the same after being
// restored on a new cluster.
func getStatsQuery(tableName string) string {
	return fmt.Sprintf(`SELECT
	  statistics_name,
	  column_names,
	  row_count,
	  distinct_count,
	  null_count
	FROM [SHOW STATISTICS FOR TABLE %s]`, tableName)
}

func generateInterleavedData(
	sqlDB *sqlutils.SQLRunner, t *testing.T, numAccounts int,
) (int, []string) {
	_ = sqlDB.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue_enabled = false`)
	// TODO(dan): The INTERLEAVE IN PARENT clause currently doesn't allow the
	// `db.table` syntax. Fix that and use it here instead of `SET DATABASE`.
	_ = sqlDB.Exec(t, `USE  data`)
	_ = sqlDB.Exec(t, `CREATE TABLE strpk (id string, v int, primary key (id, v)) PARTITION BY LIST (id) ( PARTITION ab VALUES IN (('a'), ('b')), PARTITION xy VALUES IN (('x'), ('y')) );`)
	_ = sqlDB.Exec(t, `ALTER PARTITION ab OF TABLE strpk CONFIGURE ZONE USING gc.ttlseconds = 60`)
	_ = sqlDB.Exec(t, `INSERT INTO strpk VALUES ('a', 1), ('a', 2), ('x', 100), ('y', 101)`)
	const numStrPK = 4
	_ = sqlDB.Exec(t, `CREATE TABLE strpkchild (a string, b int, c int, primary key (a, b, c)) INTERLEAVE IN PARENT strpk (a, b)`)
	// i0 interleaves in parent with a, and has a multi-col PK of its own b, c
	_ = sqlDB.Exec(t, `CREATE TABLE i0 (a INT, b INT, c INT, PRIMARY KEY (a, b, c)) INTERLEAVE IN PARENT bank (a)`)
	// Split at at a _strict prefix_ of the cols in i_0's PK
	_ = sqlDB.Exec(t, `ALTER TABLE i0 SPLIT AT VALUES (1, 1)`)
	// i0_0 interleaves into i0.
	_ = sqlDB.Exec(t, `CREATE TABLE i0_0 (a INT, b INT, c INT, d INT, PRIMARY KEY (a, b, c, d)) INTERLEAVE IN PARENT i0 (a, b, c)`)
	_ = sqlDB.Exec(t, `CREATE TABLE i1 (a INT, b CHAR, PRIMARY KEY (a, b)) INTERLEAVE IN PARENT bank (a)`)
	_ = sqlDB.Exec(t, `CREATE TABLE i2 (a INT, b CHAR, PRIMARY KEY (a, b)) INTERLEAVE IN PARENT bank (a)`)
	// The bank table has numAccounts accounts, put 2x that in i0, 3x in i0_0,
	// and 4x in i1.
	totalRows := numAccounts + numStrPK
	for i := 0; i < numAccounts; i++ {
		_ = sqlDB.Exec(t, `INSERT INTO i0 VALUES ($1, 1, 1), ($1, 2, 2)`, i)
		totalRows += 2
		_ = sqlDB.Exec(t, `INSERT INTO i0_0 VALUES ($1, 1, 1, 1), ($1, 2, 2, 2), ($1, 3, 3, 3)`, i)
		totalRows += 3
		_ = sqlDB.Exec(t, `INSERT INTO i1 VALUES ($1, 'a'), ($1, 'b'), ($1, 'c'), ($1, 'd')`, i)
		totalRows += 4
		_ = sqlDB.Exec(t, `INSERT INTO i2 VALUES ($1, 'e'), ($1, 'f'), ($1, 'g'), ($1, 'h')`, i)
		totalRows += 4
	}
	// Split some rows to attempt to exercise edge conditions in the key rewriter.
	_ = sqlDB.Exec(t, `ALTER TABLE i0 SPLIT AT SELECT * from i0 where a % 2 = 0 LIMIT $1`, numAccounts)
	_ = sqlDB.Exec(t, `ALTER TABLE i0_0 SPLIT AT SELECT * from i0_0 LIMIT $1`, numAccounts)
	_ = sqlDB.Exec(t, `ALTER TABLE i1 SPLIT AT SELECT * from i1 WHERE a % 3 = 0`)
	_ = sqlDB.Exec(t, `ALTER TABLE i2 SPLIT AT SELECT * from i2 WHERE a % 5 = 0`)
	// Truncate will allocate a new ID for i1. At that point the splits we created
	// above will still exist, but will contain the old table ID. Since the table
	// does not exist anymore, it will not be in the backup, so the rewriting will
	// not have a configured rewrite for that part of those splits, but we still
	// expect RESTORE to succeed.
	_ = sqlDB.Exec(t, `TRUNCATE i1`)
	for i := 0; i < numAccounts; i++ {
		_ = sqlDB.Exec(t, `INSERT INTO i1 VALUES ($1, 'a'), ($1, 'b'), ($1, 'c'), ($1, 'd')`, i)
	}
	tableNames := []string{
		"strpk",
		"strpkchild",
		"i0",
		"i0_0",
		"i1",
		"i2",
	}
	return totalRows, tableNames
}

func createEmptyCluster(
	t testing.TB, clusterSize int,
) (sqlDB *sqlutils.SQLRunner, tempDir string, cleanup func()) {
	ctx := context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	tc := testcluster.StartTestCluster(t, clusterSize, params)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
		dirCleanupFn()         // cleans up dir, which is the nodelocal:// storage
	}

	return sqlDB, dir, cleanupFn
}
