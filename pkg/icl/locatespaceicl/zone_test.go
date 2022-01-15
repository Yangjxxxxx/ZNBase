package locatespaceicl

import (
	"context"
	"fmt"
	"testing"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
)

func TestValidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `
		CREATE DATABASE d;
		USE d;
		CREATE TABLE t (c STRING PRIMARY KEY, id INT, INDEX i (id, c)) PARTITION BY LIST (c) (
			PARTITION p0 VALUES IN ('a'),
			PARTITION p1 VALUES IN (DEFAULT)
		);
		ALTER INDEX t@i PARTITION BY LIST (id) (
    		PARTITION i0 VALUES IN (0),
			PARTITION i1 VALUES IN (DEFAULT)
  		);`)

	yamlDefault := fmt.Sprintf("gc: {ttlseconds: %d}", config.DefaultZoneConfig().GC.TTLSeconds)
	yamlOverride := "gc: {ttlseconds: 42}"
	zoneOverride := config.DefaultZoneConfig()
	zoneOverride.GC = &config.GCPolicy{TTLSeconds: 42}
	partialZoneOverride := *config.NewZoneConfig()
	partialZoneOverride.GC = &config.GCPolicy{TTLSeconds: 42}

	dbID := sqlutils.QueryDatabaseID(t, db, "d")
	tableID := sqlutils.QueryTableID(t, db, "d", "t")

	defaultRow := sqlutils.ZoneRow{
		ID:           keys.RootNamespaceID,
		CLISpecifier: ".default",
		Config:       config.DefaultZoneConfig(),
	}
	defaultOverrideRow := sqlutils.ZoneRow{
		ID:           keys.RootNamespaceID,
		CLISpecifier: ".default",
		Config:       zoneOverride,
	}
	dbRow := sqlutils.ZoneRow{
		ID:           dbID,
		CLISpecifier: "d",
		Config:       zoneOverride,
	}
	tableRow := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t",
		Config:       zoneOverride,
	}
	primaryRow := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t@primary",
		Config:       zoneOverride,
	}
	p0Row := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t.p0",
		Config:       zoneOverride,
	}
	p1Row := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t.p1",
		Config:       zoneOverride,
	}
	i0Row := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t.i0",
		Config:       zoneOverride,
	}
	i1Row := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t.i1",
		Config:       zoneOverride,
	}

	// Partially filled config rows
	partialDbRow := sqlutils.ZoneRow{
		ID:           dbID,
		CLISpecifier: "d",
		Config:       partialZoneOverride,
	}
	partialTableRow := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t",
		Config:       partialZoneOverride,
	}
	partialPrimaryRow := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t@primary",
		Config:       partialZoneOverride,
	}
	partialP0Row := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t.p0",
		Config:       partialZoneOverride,
	}
	partialP1Row := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t.p1",
		Config:       partialZoneOverride,
	}
	partialI0Row := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t.i0",
		Config:       partialZoneOverride,
	}
	partialI1Row := sqlutils.ZoneRow{
		ID:           tableID,
		CLISpecifier: "d.t.i1",
		Config:       partialZoneOverride,
	}

	// Remove stock zone configs installed at cluster bootstrap. Otherwise this
	// test breaks whenever these stock zone configs are adjusted.
	sqlutils.RemoveAllZoneConfigs(t, sqlDB)

	// Ensure the default is reported for all zones at first.
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", defaultRow)

	// Ensure a database zone config applies to that database, its tables, and its
	// tables' indices and partitions.
	sqlutils.SetZoneConfig(t, sqlDB, "DATABASE d", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", dbRow)

	// Ensure a table zone config applies to that table and its indices and
	// partitions, but no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", tableRow)

	// Ensure an index zone config applies to that index and its partitions, but
	// no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX d.t@primary", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow, partialPrimaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", tableRow)

	// Ensure a partition zone config applies to that partition, but no other
	// zones.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", tableRow)

	// Ensure a partition-of-index zone config applies to that partition, but no other
	// zones.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow, partialPrimaryRow, partialP0Row, partialI0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", i0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", tableRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialDbRow, partialTableRow, partialPrimaryRow, partialP0Row, partialI0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	//sqlutils.SetZoneConfig(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", yamlOverride)
	//sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", I1Row)

	// Ensure deleting a database zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialTableRow, partialPrimaryRow, partialP0Row, partialI0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure deleting a table zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialPrimaryRow, partialP0Row, partialI0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure deleting an index zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "INDEX d.t@primary")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialP0Row, partialI0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)

	// Ensure deleting a partition zone works.
	sqlutils.DeleteZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialI0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", defaultOverrideRow)

	// Ensure deleting a partition-of-index zone works.
	sqlutils.DeleteZoneConfig(t, sqlDB, "PARTITION i0 OF INDEX d.t@i")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", defaultOverrideRow)

	// Ensure deleting non-overridden zones is not an error.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")
	sqlutils.DeleteZoneConfig(t, sqlDB, "PARTITION p1 OF TABLE d.t")

	// Ensure updating the default zone config applies to zones that have had
	// overrides added and removed.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlDefault)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", defaultRow)

	// Ensure subzones can be created even when no table zone exists.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE d.t", yamlOverride)
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p1 OF TABLE d.t", yamlOverride)
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", yamlOverride)
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialP0Row, partialP1Row, partialI0Row, partialI1Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", p1Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i0 OF INDEX d.t@i", i0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION i1 OF INDEX d.t@i", i1Row)

	// Ensure the shorthand index syntax works.
	sqlutils.SetZoneConfig(t, sqlDB, `INDEX "primary"`, yamlOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, `INDEX "primary"`, primaryRow)

	// Ensure the session database is respected.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE t", yamlOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE t", p0Row)
}

func TestInvalidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	for i, tc := range []struct {
		query string
		err   string
	}{
		{
			"ALTER INDEX foo CONFIGURE ZONE USING DEFAULT",
			`index "foo" does not exist`,
		},
		{
			"SHOW ZONE CONFIGURATION FOR INDEX foo",
			`index "foo" does not exist`,
		},
		{
			"USE system; ALTER INDEX foo CONFIGURE ZONE USING DEFAULT",
			`index "foo" does not exist`,
		},
		{
			"USE system; SHOW ZONE CONFIGURATION FOR INDEX foo",
			`index "foo" does not exist`,
		},
		{
			"ALTER PARTITION p0 OF TABLE system.jobs CONFIGURE ZONE = 'foo'",
			`partition "p0" does not exist`,
		},
		{
			"SHOW ZONE CONFIGURATION FOR PARTITION p0 OF TABLE system.jobs",
			`partition "p0" does not exist`,
		},
	} {
		if _, err := db.Exec(tc.query); !testutils.IsError(err, tc.err) {
			t.Errorf("#%d: expected error matching %q, but got %v", i, tc.err, err)
		}
	}
}
