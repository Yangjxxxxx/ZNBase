package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/security/audit/event"
	"github.com/znbasedb/znbase/pkg/security/audit/event/events"
	"github.com/znbasedb/znbase/pkg/server"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/sql"
	_ "github.com/znbasedb/znbase/pkg/sql/gcjob"
	"github.com/znbasedb/znbase/pkg/sql/tests"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/testcluster"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// Test function IsAudit
func TestAuditServer_IsAudit(t *testing.T) {

	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if !s.AuditServer().IsAudit(string(sql.EventSQLLenOverflow)) {
		t.Fatalf("expected %s exist in event map, got %s", string(sql.EventSQLLenOverflow), "false")
	}
}

func TestDatabaseMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// start test server
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// TODO need to find a better way
	// waiting for server complete initialization
	time.Sleep(5 * time.Second)

	// enable force sync
	s.AuditServer().TestSyncConfig()

	// get event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// prepare table
	if _, err := sqlDB.Exec("CREATE DATABASE test"); err != nil {
		t.Fatal(err)
	}

	// test rename database
	renameDBMetric := ae.GetMetric(sql.EventLogRenameDatabase)[0].RegistMetric().(*events.RenameDBMetrics)
	renameDBExpect := renameDBMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("ALTER DATABASE test RENAME TO tdatabase"); err != nil {
		t.Fatal(err)
	}

	if renameDBMetric.CatchCount.Count() != renameDBExpect {
		t.Errorf("expected db count:%d, but got %d", renameDBExpect, renameDBMetric.CatchCount.Count())
	}

	// test drop database metric
	dropDBMetric := ae.GetMetric(sql.EventLogDropDatabase)[0].RegistMetric().(*events.DropDBMetrics)

	dropDBExpect := dropDBMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("DROP DATABASE tdatabase"); err != nil {
		t.Fatal(err)
	}

	if dropDBMetric.CatchCount.Count() != dropDBExpect {
		t.Errorf("expected db count:%d, but got %d", dropDBExpect, dropDBMetric.CatchCount.Count())
	}

}

// TODO(xz): waiting for implementation of operations for tables
func TestTableMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	if _, err := sqlDB.Exec("CREATE TABLE ttable (id INT)"); err != nil {
		t.Fatal(err)
	}

	// test rename table
	renameTbMetric := ae.GetMetric(sql.EventLogRenameTable)[0].RegistMetric().(*events.RenameTBMetrics)
	renameTbExpect := renameTbMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("ALTER TABLE ttable RENAME TO test"); err != nil {
		t.Fatal(err)
	}

	if renameTbMetric.CatchCount.Count() != renameTbExpect {
		t.Errorf("expected db count:%d, but got %d", renameTbExpect, renameTbMetric.CatchCount.Count())
	}

	// test alter table add column
	alterTbMetric := ae.GetMetric(sql.EventLogAlterTable)[0].RegistMetric().(*events.AlterTBMetrics)
	alterTbExpect := alterTbMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("ALTER TABLE test ADD COLUMN names STRING"); err != nil {
		t.Fatal(err)
	}

	if alterTbMetric.CatchCount.Count() != alterTbExpect {
		t.Errorf("expected db count:%d, but got %d", alterTbExpect, alterTbMetric.CatchCount.Count())
	}

	// test truncate table
	truncateTbMetric := ae.GetMetric(sql.EventLogTruncateTable)[0].RegistMetric().(*events.TruncateTBMetrics)
	truncateTbExpect := truncateTbMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("TRUNCATE test"); err != nil {
		t.Fatal(err)
	}

	if truncateTbMetric.CatchCount.Count() != truncateTbExpect {
		t.Errorf("expected db count:%d, but got %d", truncateTbExpect, truncateTbMetric.CatchCount.Count())
	}

	// test drop table
	dropTbMetric := ae.GetMetric(sql.EventLogDropTable)[0].RegistMetric().(*events.DropTBMetrics)
	dropTbExpect := dropTbMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("DROP TABLE test"); err != nil {
		t.Fatal(err)
	}

	if dropTbMetric.CatchCount.Count() != dropTbExpect {
		t.Errorf("expected db count:%d, but got %d", dropTbExpect, dropTbMetric.CatchCount.Count())
	}
}

func TestIndexMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// prepare table and index
	if _, err := sqlDB.Exec("CREATE TABLE ttable (id INT, gender STRING, name STRING, address STRING, tel STRING)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE INDEX ON ttable (id)"); err != nil {
		t.Fatal(err)
	}

	// test rename index
	renameIDXMetric := ae.GetMetric(sql.EventLogRenameIndex)[0].RegistMetric().(*events.RenameIDXMetrics)
	renameIDXExpect := renameIDXMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("ALTER INDEX ttable@primary RENAME TO ttable_primary"); err != nil {
		t.Fatal(err)
	}

	if renameIDXMetric.CatchCount.Count() != renameIDXExpect {
		t.Errorf("expected db count:%d, but got %d", renameIDXExpect, renameIDXMetric.CatchCount.Count())
	}

	// drop index
	dropIDXMetric := ae.GetMetric(sql.EventLogDropIndex)[0].RegistMetric().(*events.DropIDXMetrics)
	dropIDXExpect := dropIDXMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("DROP INDEX ttable@ttable_id_idx"); err != nil {
		t.Fatal(err)
	}

	if dropIDXMetric.CatchCount.Count() != dropIDXExpect {
		t.Errorf("expected db count:%d, but got %d", dropIDXExpect, dropIDXMetric.CatchCount.Count())
	}
}

func TestViewMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// prepare table and view
	if _, err := sqlDB.Exec("CREATE TABLE customers (id INT, name STRING, age INT, address STRING, salary INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE VIEW customers_view AS SELECT id FROM customers"); err != nil {
		t.Fatal(err)
	}

	// test drop view
	dropViewMetric := ae.GetMetric(sql.EventLogDropView)[0].RegistMetric().(*events.DropVMetrics)
	dropViewExpect := dropViewMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("DROP VIEW customers_view"); err != nil {
		t.Fatal(err)
	}

	if dropViewMetric.CatchCount.Count() != dropViewExpect {
		t.Errorf("expected db count:%d, but got %d", dropViewExpect, dropViewMetric.CatchCount.Count())
	}
}

func TestSequenceMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// prepare sequence
	createSequenceExpect := int64(0)

	createSequenceExpect++
	if _, err := sqlDB.Exec("CREATE SEQUENCE customer_seq"); err != nil {
		t.Fatal(err)
	}

	// test alter sequence
	alterSequenceMetric := ae.GetMetric(sql.EventLogAlterSequence)[0].RegistMetric().(*events.AlterSQMetrics)
	alterSequenceExpect := alterSequenceMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("ALTER SEQUENCE customer_seq INCREMENT 2"); err != nil {
		t.Fatal(err)
	}
	if alterSequenceMetric.CatchCount.Count() != alterSequenceExpect {
		fmt.Println("1111")
		t.Errorf("expected db count:%d, but got %d", alterSequenceExpect, alterSequenceMetric.CatchCount.Count())
	}

	// test drop sequence
	dropSequenceMetric := ae.GetMetric(sql.EventLogDropSequence)[0].RegistMetric().(*events.DropSQMetrics)
	dropSequenceExpect := dropSequenceMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("DROP SEQUENCE customer_seq"); err != nil {
		t.Fatal(err)
	}

	if dropSequenceMetric.CatchCount.Count() != dropSequenceExpect {
		t.Errorf("expected db count:%d, but got %d", dropSequenceExpect, dropSequenceMetric.CatchCount.Count())
	}
}

func TestSQLLenOverFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	//start test server
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	//TODO need to find a better way
	//waiting for server complete initialization
	time.Sleep(5 * time.Second)

	//enable force sync
	s.AuditServer().TestSyncConfig()

	//get a event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	SQLLenMetrics := ae.GetMetric(sql.EventSQLLenOverflow)[0].RegistMetric().(*events.SQLLOMetrics)
	SQLLenExpect := SQLLenMetrics.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("set cluster setting audit.sql.length.bypass.enabled=false"); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec("set cluster setting audit.sql.length=100"); err != nil {
		t.Fatal(err)
	}
	//if _, err := sqlDB.Exec("create database foo"); err != nil {
	//	t.Fatal(err)
	//}
	if _, err := sqlDB.Exec("create database foo99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999"); err != nil {
		t.Fatalf("expected sql len overflow count:%d,but got %d", SQLLenExpect, SQLLenMetrics.CatchCount.Count())
	}
	//} else if err==nil{
	//	t.Fatalf("the length limitataion didn't work,check the length setting or your SQl length")
}

// TODO(xz): waiting for implementation of trigger to sql injection
//func TestSQLInjectMetric(t *testing.T) {
//defer leaktest.AfterTest(t)()
//s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
//defer s.Stopper().Stop(context.TODO())
//
//setupQueries := []string{
//	"CREATE DATABASE test",
//	"CREATE TABLE test.tbl (a INT)",
//	"INSERT INTO test.tbl VALUES (1)",
//	"SET CLUSTER SETTING audit.sql.inject.bypass = true;",
//}
//
//for _, q := range setupQueries {
//	if _, err := sqlDB.Exec(q); err != nil {
//		t.Fatalf("error executing '%s': %s", q, err)
//	}
//}
//
//s.AuditServer().EnableForceSync()
//
//ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
//}

func TestSQLInjectionMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// start test server
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// TODO need to find a better way
	// waiting for server complete initialization
	time.Sleep(5 * time.Second)

	// enable force sync
	s.AuditServer().TestSyncConfig()

	// get a event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	injectionMetric := ae.GetMetric(sql.EventSQLInjection)[0].RegistMetric().(*events.SQLInjectMetrics)
	injectionExpect := injectionMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("SET CLUSTER SETTING audit.sql.inject.bypass.enabled = false"); err != nil {
		t.Fatal(err)
	}
	// waiting for refreshInterval which is 10 seconds default
	time.Sleep(10 * time.Second)

	if _, err := sqlDB.Exec(`SELECT (1 OR ( SELECT * FROM test))`); !strings.Contains(err.Error(), "E(1&(") {
		t.Fatal(err)
	}

	if injectionMetric.CatchCount.Count() != injectionExpect {
		t.Errorf("expected db count:%d, but got %d", injectionExpect, injectionMetric.CatchCount.Count())
	}
}

// TODO(xz): flags_test.go for reference
//func TestUserLoginMetric(t *testing.T) {
//defer leaktest.AfterTest(t)()
//
//// Avoid leaking configuration changes after the tests end.
//defer initCLIDefaults()
//
//params, _ := tests.CreateTestServerParams()
//s, sqlDB, _ := serverutils.StartServer(t, params)
//defer s.Stopper().Stop(context.TODO())
//
//// waiting for server complete initialization
//time.Sleep(5 * time.Second)
//s.AuditServer().EnableForceSync()
//
//ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
//
//userLoginMetric := ae.GetMetric(sql.EventLogUserLogin)[0].RegistMetric().(*audit.UserLoginMetrics)
//userLoginExpect := userLoginMetric.CatchCount.Count() + 1
//
//if _, err := sqlDB.Exec(""); err != nil {
//	t.Fatal(err)
//}
//if userLoginMetric.CatchCount.Count() != userLoginExpect {
//	t.Errorf("expected db count:%d, but got %d", userLoginExpect, userLoginMetric.CatchCount.Count())
//}

//}

func TestCancelQueryMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// cancel query test

	CancelQueryMetric := ae.GetMetric(sql.EventLogCancelQueries)[0].RegistMetric().(*events.CancelQueriesMetrics)
	CancelQueryExpect := CancelQueryMetric.CatchCount.Count() + 1

	go func() {
		_, _ = sqlDB.Exec("select pg_sleep(30);")
	}()
	time.Sleep(500 * time.Millisecond)
	if _, err := sqlDB.Exec("cancel query (select query_id from [show cluster queries] where query ='SELECT pg_sleep(30)');"); err != nil {
		t.Fatal(err)
	}

	if CancelQueryMetric.CatchCount.Count() != CancelQueryExpect {
		t.Errorf("expected db count:%d, but got %d", CancelQueryExpect, CancelQueryMetric.CatchCount.Count())
	}
}

func TestRoleMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// prepare Role
	createRoleExpect := int64(0)

	createRoleExpect++
	//TODO the role operation is contain in user ,tree type error
	if _, err := sqlDB.Exec("CREATE ROLE dev_ops"); err != nil {
		//TODO(yhq)role and user share common tree type so there will be tree type error now
		err = nil
	}

	// test drop Role
	dropRoleMetric := ae.GetMetric(sql.EventLogDropRole)[0].RegistMetric().(*events.DropRLMetrics)
	dropRoleExpect := dropRoleMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("DROP ROLE dev_ops"); err != nil {
		err = nil
	}

	if dropRoleMetric.CatchCount.Count() != dropRoleExpect {
		t.Errorf("expected db count:%d, but got %d", dropRoleExpect, dropRoleMetric.CatchCount.Count())
	}
}

func TestUserMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// prepare User
	createUserExpect := int64(0)

	createUserExpect++
	if _, err := sqlDB.Exec("CREATE USER 'u1'"); err != nil {
		t.Fatal(err)
	}

	//test alter User
	alterUserMetric := ae.GetMetric(sql.EventLogAlterUser)[0].RegistMetric().(*events.AlterUserMetrics)
	alterUserExpect := alterUserMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("ALTER USER 'u1'  WITH PASSWORD '11aaAA^^'"); err != nil {
		t.Fatal(err)
	}
	if alterUserMetric.CatchCount.Count() != alterUserExpect {
		t.Errorf("expected db count:%d, but got %d", alterUserExpect, alterUserMetric.CatchCount.Count())
	}

	// test drop User
	dropUserMetric := ae.GetMetric(sql.EventLogDropUser)[0].RegistMetric().(*events.DropUserMetrics)
	dropUserExpect := dropUserMetric.CatchCount.Count() + 1

	if _, err := sqlDB.Exec("DROP USER 'u1'"); err != nil {
		t.Fatal(err)
	}

	if dropUserMetric.CatchCount.Count() != dropUserExpect {
		t.Errorf("expected db count:%d, but got %d", dropUserExpect, dropUserMetric.CatchCount.Count())
	}
}

func TestSplitAtMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//SPLIT AT operation preparation
	if _, err := sqlDB.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled=false;"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE t (k1 INT, k2 INT, v INT, w INT, PRIMARY KEY (k1, k2))"); err != nil {
		t.Fatal(err)
	}

	// test split at
	alterSplitAtMetric := ae.GetMetric(sql.EventLogSplit)[0].RegistMetric().(*events.SplitAtMetrics)
	alterSplitAtExpect := alterSplitAtMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("ALTER TABLE t SPLIT AT VALUES (5,1), (5,2), (5,3);"); err != nil {
		t.Fatal(err)
	}
	if alterSplitAtMetric.CatchCount.Count() != alterSplitAtExpect {
		t.Errorf("expected db count:%d, but got %d", alterSplitAtExpect, alterSplitAtMetric.CatchCount.Count())
	}
}

func TestSetClusterSettingMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// test SetClusterSetting
	SetClusterSettingMetric := ae.GetMetric(sql.EventLogSetClusterSetting)[0].RegistMetric().(*events.SetCSMetrics)
	SetClusterSettingExpect := SetClusterSettingMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled=false;"); err != nil {
		t.Fatal(err)
	}
	if SetClusterSettingMetric.CatchCount.Count() != SetClusterSettingExpect {
		t.Errorf("expected db count:%d, but got %d", SetClusterSettingExpect, SetClusterSettingMetric.CatchCount.Count())
	}
}

func TestControlJobsMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// test Job_control

	//PAUSE JOBS
	ControlJobsMetric := ae.GetMetric(sql.EventLogControlJobs)[0].RegistMetric().(*events.ControlJobsMetrics)
	ControlJobsExpect := ControlJobsMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("PAUSE JOBS (SELECT job_id FROM [SHOW JOBS] WHERE user_name = 'root');"); err != nil {
		t.Fatal(err)
	}
	if ControlJobsMetric.CatchCount.Count() != ControlJobsExpect {
		t.Errorf("expected db count:%d, but got %d", ControlJobsExpect, ControlJobsMetric.CatchCount.Count())
	}

	//RESUME JOBS
	ControlJobsExpect = ControlJobsMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("RESUME JOBS (SELECT job_id FROM [SHOW JOBS] WHERE user_name = 'root');"); err != nil {
		t.Fatal(err)
	}
	if ControlJobsMetric.CatchCount.Count() != ControlJobsExpect {
		t.Errorf("expected db count:%d, but got %d", ControlJobsExpect, ControlJobsMetric.CatchCount.Count())
	}

	//CANCEL JOBS
	ControlJobsExpect = ControlJobsMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("CANCEL JOBS (SELECT job_id FROM [SHOW JOBS] WHERE user_name = 'root');"); err != nil {
		t.Fatal(err)
	}
	if ControlJobsMetric.CatchCount.Count() != ControlJobsExpect {
		t.Errorf("expected db count:%d, but got %d", ControlJobsExpect, ControlJobsMetric.CatchCount.Count())
	}
}

func TestSetZoneConfigMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// test Set zone config

	SetZoneConfigMetric := ae.GetMetric(sql.EventLogSetZoneConfig)[0].RegistMetric().(*events.SetZCMetrics)
	SetZoneConfigExpect := SetZoneConfigMetric.CatchCount.Count() + 1

	//Create test database and table
	if _, err := sqlDB.Exec("CREATE DATABASE TEST"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE t (i Int)"); err != nil {
		t.Fatal(err)
	}

	//range
	if _, err := sqlDB.Exec("ALTER RANGE default CONFIGURE ZONE USING num_replicas = 5, gc.ttlseconds = 100000;"); err != nil {
		t.Fatal(err)
	}
	if SetZoneConfigMetric.CatchCount.Count() != SetZoneConfigExpect {
		t.Errorf("expected db count:%d, but got %d", SetZoneConfigExpect, SetZoneConfigMetric.CatchCount.Count())
	}
	//database
	SetZoneConfigExpect = SetZoneConfigMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("ALTER DATABASE test CONFIGURE ZONE USING num_replicas = 5, gc.ttlseconds = 100000;"); err != nil {
		t.Fatal(err)
	}
	if SetZoneConfigMetric.CatchCount.Count() != SetZoneConfigExpect {
		t.Errorf("expected db count:%d, but got %d", SetZoneConfigExpect, SetZoneConfigMetric.CatchCount.Count())
	}
	//table
	SetZoneConfigExpect = SetZoneConfigMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("ALTER TABLE t CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 90000, gc.ttlseconds = 89999, num_replicas = 4, constraints = '[-region=west]';"); err != nil {
		t.Fatal(err)
	}
	if SetZoneConfigMetric.CatchCount.Count() != SetZoneConfigExpect {
		t.Errorf("expected db count:%d, but got %d", SetZoneConfigExpect, SetZoneConfigMetric.CatchCount.Count())
	}
}

func TestGrandRevokeMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//Create test database and user
	if _, err := sqlDB.Exec("CREATE DATABASE db1"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE DATABASE db2"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE USER u1;"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE USER u2;"); err != nil {
		t.Fatal(err)
	}

	//test GrandPrivileges
	GrandPrivilegesMetric := ae.GetMetric(sql.EventLogGrantPrivileges)[0].RegistMetric().(*events.GrantPrivilegesMetrics)
	GrandPrivilegesExpect := GrandPrivilegesMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("GRANT CREATE ON DATABASE db1, db2 TO u1, u2;"); err != nil {
		t.Fatal(err)
	}
	if GrandPrivilegesMetric.CatchCount.Count() != GrandPrivilegesExpect {
		t.Errorf("expected db count:%d, but got %d", GrandPrivilegesExpect, GrandPrivilegesMetric.CatchCount.Count())
	}

	//test RevokePrivileges
	RevokePrivilegesMetric := ae.GetMetric(sql.EventLogRevokePrivileges)[0].RegistMetric().(*events.RevokePrivilegesMetrics)
	RevokePrivilegesExpect := RevokePrivilegesMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("REVOKE CREATE ON DATABASE db1, db2 FROM u1, u2;"); err != nil {
		t.Fatal(err)
	}
	if RevokePrivilegesMetric.CatchCount.Count() != RevokePrivilegesExpect {
		t.Errorf("expected db count:%d, but got %d", RevokePrivilegesExpect, RevokePrivilegesMetric.CatchCount.Count())
	}
}

func TestUserLoginMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	UserLoginMetric := ae.GetMetric(sql.EventLogUserLogin)[0].RegistMetric().(*events.UserLoginMetrics)
	UserLoginExpect := UserLoginMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("CREATE USER U1"); err != nil {
		t.Fatal(err)
	}
	if UserLoginMetric.CatchCount.Count() != UserLoginExpect {
		t.Errorf("expected db count:%d, but got %d", UserLoginExpect, UserLoginMetric.CatchCount.Count())
	}
}

func TestCLusterInitMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	ClusterInitMetric := ae.GetMetric(sql.EventLogClusterInit)[0].RegistMetric().(*events.ClusterInitMetrics)
	ClusterInitExpect := ClusterInitMetric.CatchCount.Count() + 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := s.RPCContext().GRPCDialNode(s.Addr(), s.NodeID(), rpc.DefaultClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("got err: %s", err)
		}
	}()

	c := serverpb.NewInitClient(conn)
	for i := 0; i < 2; i++ {
		if _, err = c.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
			_ = fmt.Errorf("%s", err)
		}
	}

	if ClusterInitMetric.CatchCount.Count() != ClusterInitExpect {
		t.Errorf("expected db count:%d, but got %d", ClusterInitExpect, ClusterInitMetric.CatchCount.Count())
	}
}

func TestSetSessionMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)

	defer s.Stopper().Stop(context.TODO())

	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//create test database
	if _, err := sqlDB.Exec("CREATE DATABASE dbtest"); err != nil {
		t.Fatal(err)
	}
	// set test
	SetSessionMetric := ae.GetMetric(sql.EventLogSetVar)[0].RegistMetric().(*events.SetSessionVarMetrics)
	SetSessionExpect := SetSessionMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("SET DATABASE=dbtest;"); err != nil {
		t.Fatal(err)
	}
	if SetSessionMetric.CatchCount.Count() != SetSessionExpect {
		t.Errorf("expected db count:%d, but got %d", SetSessionExpect, SetSessionMetric.CatchCount.Count())
	}
	//reset test
	SetSessionExpect = SetSessionMetric.CatchCount.Count() + 1
	if _, err := sqlDB.Exec("RESET database;"); err != nil {
		t.Fatal(err)
	}
	if SetSessionMetric.CatchCount.Count() != SetSessionExpect {
		t.Errorf("expected db count:%d, but got %d", SetSessionExpect, SetSessionMetric.CatchCount.Count())
	}

}

func TestCancelSessionMetric(t *testing.T) {

	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	CancelSessionMetric := ae.GetMetric(sql.EventLogCancelSession)[0].RegistMetric().(*events.CancelSessionMetrics)
	CancelSessionExpect := CancelSessionMetric.CatchCount.Count() + 1

	go func() {
		_, _ = db.Exec("select pg_sleep(30)")
	}()
	time.Sleep(500 * time.Millisecond)

	var sessionID string
	if err := db.QueryRow("select session_id from zbdb_internal.node_sessions where active_queries=$1", "SELECT pg_sleep(30.0)").Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("cancel session $1", sessionID); err != nil {
		t.Fatal(err)
	}

	if CancelSessionMetric.CatchCount.Count() != CancelSessionExpect {
		t.Errorf("expected db count:%d, but got %d", CancelSessionExpect, CancelSessionMetric.CatchCount.Count())
	}
}

func TestNodeJoinMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	ctx := context.TODO()
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0).(*server.TestServer)

	//waiting for server complete initialization
	time.Sleep(5 * time.Second)

	//enable force sync
	s.AuditServer().TestSyncConfig()

	//get a event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//node join test
	NodeJoinMetric := ae.GetMetric(sql.EventLogNodeJoin)[0].RegistMetric().(*events.NJoinMetrics)
	NodeJoinExpect := NodeJoinMetric.CatchCount.Count()
	if NodeJoinMetric.CatchCount.Count() != NodeJoinExpect {
		t.Errorf("expected db count:%d, but got %d", NodeJoinExpect, NodeJoinMetric.CatchCount.Count())
	}
	//node restart
	s.Stop()
	s = tc.Server(0).(*server.TestServer)
	time.Sleep(5 * time.Second)
	s.AuditServer().TestSyncConfig()
	ae2 := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	NodeRestartMetric := ae2.GetMetric(sql.EventLogNodeRestart)[0].RegistMetric().(*events.NReStartMetrics)
	NodeRestartExpect := NodeRestartMetric.CatchCount.Count()
	if NodeRestartMetric.CatchCount.Count() != NodeRestartExpect {
		t.Errorf("expected db count:%d, but got %d", NodeRestartExpect, NodeRestartMetric.CatchCount.Count())
	}
}

func TestNodeDecommissionMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	ctx := context.TODO()
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0).(*server.TestServer)

	//waiting for server complete initialization
	time.Sleep(5 * time.Second)

	//enable force sync
	s.AuditServer().TestSyncConfig()

	//get a event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//node decommission test
	NodeDecommissionMetric := ae.GetMetric(sql.EventLogNodeDecommissioned)[0].RegistMetric().(*events.NDCMMetrics)
	NodeDecommissionExpect := NodeDecommissionMetric.CatchCount.Count() + 1

	decommissioningNodeID := s.NodeID()
	if err := s.Decommission(ctx, true, []roachpb.NodeID{decommissioningNodeID}); err != nil {
		t.Fatal(err)
	}
	if NodeDecommissionMetric.CatchCount.Count() != NodeDecommissionExpect {
		t.Errorf("expected db count:%d, but got %d", NodeDecommissionExpect, NodeDecommissionMetric.CatchCount.Count())
	}

	//node recommission test
	NodeRecommissionMetric := ae.GetMetric(sql.EventLogNodeRecommissioned)[0].RegistMetric().(*events.NRCMMetrics)
	NodeRecommissionExpect := NodeRecommissionMetric.CatchCount.Count() + 1
	NodeRecommissionNodeID := s.NodeID()
	if err := s.Decommission(ctx, false, []roachpb.NodeID{NodeRecommissionNodeID}); err != nil {
		t.Fatal(err)
	}
	if NodeRecommissionMetric.CatchCount.Count() != NodeRecommissionExpect {
		t.Errorf("expected db count:%d, but got %d", NodeRecommissionExpect, NodeRecommissionMetric.CatchCount.Count())
	}
}
