package test

import (
	"context"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/security/audit/event"
	"github.com/znbasedb/znbase/pkg/security/audit/event/events"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestDatabaseClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	time.Sleep(5 * time.Second)
	testCluster.UpdateAudit()

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createDatabaseMetric0 := ae0.GetMetric(sql.EventLogCreateDatabase)[0].(*events.CreateDatabaseMetric)
	createDatabaseMetric1 := ae1.GetMetric(sql.EventLogCreateDatabase)[0].(*events.CreateDatabaseMetric)
	createDatabaseMetric2 := ae2.GetMetric(sql.EventLogCreateDatabase)[0].(*events.CreateDatabaseMetric)
	createDatabaseExpect := 0

	// test CREATE DATABASE on node1
	createDatabaseExpect++
	if _, err := sqlDB0.Exec("CREATE DATABASE test1"); err != nil {
		t.Fatal(err)
	}
	frequency, ok := createDatabaseMetric0.EventLog(context.TODO())
	if ok {
		if frequency != createDatabaseExpect {
			t.Errorf("expected db count:%d, but got %d", createDatabaseExpect, frequency)
		}
	}

	// test CREATE DATABASE on node2
	createDatabaseExpect++
	if _, err := sqlDB1.Exec("CREATE DATABASE test2"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createDatabaseMetric1.EventLog(context.TODO())
	if ok {
		if frequency != createDatabaseExpect {
			t.Errorf("expected db count:%d, but got %d", createDatabaseExpect, frequency)
		}
	}

	// test CREATE DATABASE on node3
	createDatabaseExpect++
	if _, err := sqlDB2.Exec("CREATE DATABASE test3"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createDatabaseMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createDatabaseExpect {
			t.Errorf("expected db count:%d, but got %d", createDatabaseExpect, frequency)
		}
	}

	// test CREATE DATABASE after 5s
	time.Sleep(5 * time.Second)
	createDatabaseExpect = 1
	if _, err := sqlDB2.Exec("CREATE DATABASE test4"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createDatabaseMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createDatabaseExpect {
			t.Errorf("expected db count:%d, but got %d", createDatabaseExpect, frequency)
		}
	}

}

func TestTableClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	time.Sleep(5 * time.Second)
	testCluster.UpdateAudit()

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createTableMetric0 := ae0.GetMetric(sql.EventLogCreateTable)[0].(*events.CreateTableMetric)
	createTableMetric1 := ae1.GetMetric(sql.EventLogCreateTable)[0].(*events.CreateTableMetric)
	createTableMetric2 := ae2.GetMetric(sql.EventLogCreateTable)[0].(*events.CreateTableMetric)
	createTableExpect := 0

	// test CREATE TABLE on node1
	createTableExpect++
	if _, err := sqlDB0.Exec("CREATE TABLE ttable1 (id INT)"); err != nil {
		t.Fatal(err)
	}
	frequency, ok := createTableMetric0.EventLog(context.TODO())
	if ok {
		if frequency != createTableExpect {
			t.Errorf("expected db count:%d, but got %d", createTableExpect, frequency)
		}
	}

	// test CREATE TABLE on node2
	createTableExpect++
	if _, err := sqlDB1.Exec("CREATE TABLE ttable2 (id INT)"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createTableMetric1.EventLog(context.TODO())
	if ok {
		if frequency != createTableExpect {
			t.Errorf("expected db count:%d, but got %d", createTableExpect, frequency)
		}
	}

	// test CREATE TABLE on node3
	createTableExpect++
	if _, err := sqlDB2.Exec("CREATE TABLE ttable3 (id INT)"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createTableMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createTableExpect {
			t.Errorf("expected db count:%d, but got %d", createTableExpect, frequency)
		}
	}

	// test CREATE TABLE after 5s
	time.Sleep(5 * time.Second)
	createTableExpect = 1
	if _, err := sqlDB2.Exec("CREATE TABLE ttable4 (id INT)"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createTableMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createTableExpect {
			t.Errorf("expected db count:%d, but got %d", createTableExpect, frequency)
		}
	}
}

func TestIndexClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	time.Sleep(5 * time.Second)
	testCluster.UpdateAudit()

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createIndexMetric0 := ae0.GetMetric(sql.EventLogCreateIndex)[0].(*events.CreateIndexMetric)
	createIndexMetric1 := ae1.GetMetric(sql.EventLogCreateIndex)[0].(*events.CreateIndexMetric)
	createIndexMetric2 := ae2.GetMetric(sql.EventLogCreateIndex)[0].(*events.CreateIndexMetric)
	createIndexExpect := 0

	// prepare table
	if _, err := sqlDB0.Exec("CREATE TABLE ttable (id INT, gender STRING, name STRING, address STRING, tel STRING)"); err != nil {
		t.Fatal(err)
	}

	// test CREATE INDEX on node1
	createIndexExpect++
	if _, err := sqlDB0.Exec("CREATE INDEX ON ttable (id)"); err != nil {
		t.Fatal(err)
	}
	frequency, ok := createIndexMetric0.EventLog(context.TODO())
	if ok {
		if frequency != createIndexExpect {
			t.Errorf("expected db count:%d, but got %d", createIndexExpect, frequency)
		}
	}

	// test CREATE INDEX on node2
	createIndexExpect++
	if _, err := sqlDB1.Exec("CREATE INDEX ON ttable (gender)"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createIndexMetric1.EventLog(context.TODO())
	if ok {
		if frequency != createIndexExpect {
			t.Errorf("expected db count:%d, but got %d", createIndexExpect, frequency)
		}
	}

	// test CREATE INDEX on node3
	createIndexExpect++
	if _, err := sqlDB2.Exec("CREATE INDEX ON ttable (name)"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createIndexMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createIndexExpect {
			t.Errorf("expected db count:%d, but got %d", createIndexExpect, frequency)
		}
	}

	// test CREATE INDEX after 5s
	time.Sleep(5 * time.Second)
	createIndexExpect = 1
	if _, err := sqlDB2.Exec("CREATE INDEX ON ttable (address)"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createIndexMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createIndexExpect {
			t.Errorf("expected db count:%d, but got %d", createIndexExpect, frequency)
		}
	}
}

func TestViewClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	time.Sleep(5 * time.Second)
	testCluster.UpdateAudit()

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createViewMetric0 := ae0.GetMetric(sql.EventLogCreateView)[0].(*events.CreateViewMetric)
	createViewMetric1 := ae1.GetMetric(sql.EventLogCreateView)[0].(*events.CreateViewMetric)
	createViewMetric2 := ae2.GetMetric(sql.EventLogCreateView)[0].(*events.CreateViewMetric)
	createViewExpect := 0

	// prepare table
	if _, err := sqlDB0.Exec("CREATE TABLE customers (id INT, name STRING, age INT, address STRING, salary INT)"); err != nil {
		t.Fatal(err)
	}

	createViewExpect++
	if _, err := sqlDB0.Exec("CREATE VIEW customers_view1 AS SELECT id FROM customers"); err != nil {
		t.Fatal(err)
	}
	frequency, ok := createViewMetric0.EventLog(context.TODO())
	if ok {
		if frequency != createViewExpect {
			t.Errorf("expected db count:%d, but got %d", createViewExpect, frequency)
		}
	}

	createViewExpect++
	if _, err := sqlDB1.Exec("CREATE VIEW customers_view2 AS SELECT name FROM customers"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createViewMetric1.EventLog(context.TODO())
	if ok {
		if frequency != createViewExpect {
			t.Errorf("expected db count:%d, but got %d", createViewExpect, frequency)
		}
	}

	createViewExpect++
	if _, err := sqlDB2.Exec("CREATE VIEW customers_view3 AS SELECT age FROM customers"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createViewMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createViewExpect {
			t.Errorf("expected db count:%d, but got %d", createViewExpect, frequency)
		}
	}

	// test CREATE VIEW after 5s
	time.Sleep(5 * time.Second)
	createViewExpect = 1
	if _, err := sqlDB2.Exec("CREATE VIEW customers_view4 AS SELECT address FROM customers"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createViewMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createViewExpect {
			t.Errorf("expected db count:%d, but got %d", createViewExpect, frequency)
		}
	}
}

func TestSequenceClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	time.Sleep(5 * time.Second)
	testCluster.UpdateAudit()

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createSequenceMetric0 := ae0.GetMetric(sql.EventLogCreateSequence)[0].(*events.CreateSequenceMetric)
	createSequenceMetric1 := ae1.GetMetric(sql.EventLogCreateSequence)[0].(*events.CreateSequenceMetric)
	createSequenceMetric2 := ae2.GetMetric(sql.EventLogCreateSequence)[0].(*events.CreateSequenceMetric)
	createSequenceExpect := 0

	createSequenceExpect++
	if _, err := sqlDB0.Exec("CREATE SEQUENCE customer_seq1"); err != nil {
		t.Fatal(err)
	}
	frequency, ok := createSequenceMetric0.EventLog(context.TODO())
	if ok {
		if frequency != createSequenceExpect {
			t.Errorf("expected db count:%d, but got %d", createSequenceExpect, frequency)
		}
	}

	createSequenceExpect++
	if _, err := sqlDB1.Exec("CREATE SEQUENCE customer_seq2"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createSequenceMetric1.EventLog(context.TODO())
	if ok {
		if frequency != createSequenceExpect {
			t.Errorf("expected db count:%d, but got %d", createSequenceExpect, frequency)
		}
	}

	createSequenceExpect++
	if _, err := sqlDB2.Exec("CREATE SEQUENCE customer_seq3"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createSequenceMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createSequenceExpect {
			t.Errorf("expected db count:%d, but got %d", createSequenceExpect, frequency)
		}
	}

	// test CREATE VIEW after 5s
	time.Sleep(5 * time.Second)
	createSequenceExpect = 1
	if _, err := sqlDB2.Exec("CREATE SEQUENCE customer_seq4"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createSequenceMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createSequenceExpect {
			t.Errorf("expected db count:%d, but got %d", createSequenceExpect, frequency)
		}
	}
}

func TestRoleClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	time.Sleep(5 * time.Second)
	testCluster.UpdateAudit()

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createRoleMetric0 := ae0.GetMetric(sql.EventLogCreateRole)[0].(*events.CreateRoleMetric)
	createRoleMetric1 := ae1.GetMetric(sql.EventLogCreateRole)[0].(*events.CreateRoleMetric)
	createRoleMetric2 := ae2.GetMetric(sql.EventLogCreateRole)[0].(*events.CreateRoleMetric)
	createRoleExpect := 0

	createRoleExpect++
	if _, err := sqlDB0.Exec("CREATE ROLE r1"); err != nil {
		//TODO(yhq)role and user share common tree type so there will be tree type error now
		err = nil
	}
	frequency, ok := createRoleMetric0.EventLog(context.TODO())
	if ok {
		if frequency != createRoleExpect {
			t.Errorf("expected db count:%d, but got %d", createRoleExpect, frequency)
		}
	}

	createRoleExpect++
	if _, err := sqlDB1.Exec("CREATE ROLE r2"); err != nil {
		err = nil
	}
	frequency, ok = createRoleMetric1.EventLog(context.TODO())
	if ok {
		if frequency != createRoleExpect {
			t.Errorf("expected db count:%d, but got %d", createRoleExpect, frequency)
		}
	}

	createRoleExpect++
	if _, err := sqlDB2.Exec("CREATE ROLE r3"); err != nil {
		err = nil
	}
	frequency, ok = createRoleMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createRoleExpect {
			t.Errorf("expected db count:%d, but got %d", createRoleExpect, frequency)
		}
	}

	// test CREATE Role after 5s
	time.Sleep(5 * time.Second)
	createRoleExpect = 1
	if _, err := sqlDB2.Exec("CREATE ROLE r4"); err != nil {
		err = nil
	}
	frequency, ok = createRoleMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createRoleExpect {
			t.Errorf("expected db count:%d, but got %d", createRoleExpect, frequency)
		}
	}
}

func TestUserClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	time.Sleep(5 * time.Second)
	testCluster.UpdateAudit()

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createUserMetric0 := ae0.GetMetric(sql.EventLogCreateUser)[0].(*events.CreateUserMetric)
	createUserMetric1 := ae1.GetMetric(sql.EventLogCreateUser)[0].(*events.CreateUserMetric)
	createUserMetric2 := ae2.GetMetric(sql.EventLogCreateUser)[0].(*events.CreateUserMetric)
	createUserExpect := 0

	createUserExpect++
	if _, err := sqlDB0.Exec("CREATE User u1"); err != nil {
		t.Fatal(err)
	}
	frequency, ok := createUserMetric0.EventLog(context.TODO())
	if ok {
		if frequency != createUserExpect {
			t.Errorf("expected db count:%d, but got %d", createUserExpect, frequency)
		}
	}

	createUserExpect++
	if _, err := sqlDB1.Exec("CREATE User u2"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createUserMetric1.EventLog(context.TODO())
	if ok {
		if frequency != createUserExpect {
			t.Errorf("expected db count:%d, but got %d", createUserExpect, frequency)
		}
	}

	createUserExpect++
	if _, err := sqlDB2.Exec("CREATE User u3"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createUserMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createUserExpect {
			t.Errorf("expected db count:%d, but got %d", createUserExpect, frequency)
		}
	}

	// test CREATE User after 5s
	time.Sleep(5 * time.Second)
	createUserExpect = 1
	if _, err := sqlDB2.Exec("CREATE User u4"); err != nil {
		t.Fatal(err)
	}
	frequency, ok = createUserMetric2.EventLog(context.TODO())
	if ok {
		if frequency != createUserExpect {
			t.Errorf("expected db count:%d, but got %d", createUserExpect, frequency)
		}
	}
}
