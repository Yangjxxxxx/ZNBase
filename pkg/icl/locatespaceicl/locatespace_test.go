package locatespaceicl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/testutils/testcluster"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
)

const (
	onlyTableCreateType = iota
	onlyIndexCreateType
	onlyPartitionCreateType
	// to do : need add related test for this type
	// onlyTableIndexCreateType
	// onlyTableIndexPartitionCreateType
	// onlyIndexPartitionCreateType
	// partitionCreateWithLeaseInType
	// IndexCreateWithLeaseInType
	// TableCreateWithLeaseInType
)

// global testing db
var db *gosql.DB

// global testCluster
var tc *testcluster.TestCluster

// global SQLRunner
var sqlDB *sqlutils.SQLRunner

// global cleanup function
var cleanup func()

type partitionSpace struct {
	spaceName         *roachpb.LocationValue
	subPartitionSpace []partitionSpace
}

// locatespaceTest represent the locatespace test struct set
type locateSpaceTest struct {
	// testOrNot  switch for testing
	testOrNot bool
	// name represent the name of this case
	name string

	preSchema string
	// sqlSchema represent the pattern of this case and full with different argument
	sqlSchema string
	// tableLocateSpace will store the table locateSpace name only one
	tableLocateSpace *roachpb.LocationValue
	// sqlType
	sqlType int32
	// indexLocateSpace store the index locateSpace name may be a array
	indexLocateSpace []*roachpb.LocationValue
	// partitionLocateSpace store all partition locate information
	partitionLocateSpace []partitionSpace
	// output map by GetLocationMapICL
	expect roachpb.LocationMap
	// scans store the expect node in each ruler condition
	scans map[string][]string
	// parse struct is the parse process object
	parse struct {
		// create stmt will construct stmt for create table or create index
		createStmt string
		// table descriptor will store the output desc of create/alter stmt
		tabDesc *sqlbase.TableDescriptor
	}
}

var testCase = []locateSpaceTest{
	{
		testOrNot:        true,
		name:             "SimpleTable",
		sqlSchema:        "create table %s (col1 int PRIMARY KEY) locate in (%s)",
		sqlType:          onlyTableCreateType,
		tableLocateSpace: stringToValue("sp0"),
		expect: roachpb.LocationMap{
			TableSpace: stringToValue("sp0"),
			IndexSpace: map[uint32]*roachpb.LocationValue{
				1: stringToValue("sp0"),
			},
			PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
		},
	},
	{
		testOrNot:        true,
		name:             "SimpleIndex",
		preSchema:        "create table t1(a int)",
		sqlSchema:        "create index %s on t1(a) locate in (%s)",
		sqlType:          onlyIndexCreateType,
		indexLocateSpace: []*roachpb.LocationValue{stringToValue("sp0")},
	},
	{
		testOrNot: true,
		name:      "simplepartition",
		sqlSchema: "create table %s (id int, name string, PRIMARY KEY(id)) partition by list(id) " +
			"(partition p1 values in(3) locate in (%s)," +
			"partition p2 values in(4) locate in (%s)) locate in (sp0)",
		sqlType: onlyPartitionCreateType,
		partitionLocateSpace: []partitionSpace{
			{
				stringToValue("sp0"),
				nil,
			},
			{
				stringToValue("sp1"),
				nil,
			},
		},
		expect: roachpb.LocationMap{
			TableSpace: stringToValue("sp0"),
			IndexSpace: map[uint32]*roachpb.LocationValue{
				1: stringToValue("sp0"),
			},
			PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
				{
					Name:     "p1",
					StartKey: roachpb.RKey([]byte{187, 137, 139}),
					EndKey:   roachpb.RKey([]byte{187, 137, 140}),
					Space:    stringToValue("sp0"),
					IndexID:  1,
				},
				{
					Name:     "p2",
					StartKey: roachpb.RKey([]byte{187, 137, 140}),
					EndKey:   roachpb.RKey([]byte{187, 137, 141}),
					Space:    stringToValue("sp1"),
					IndexID:  1,
				},
			},
		},

		scans: map[string][]string{
			`id = 3`: {`n1`, `n2`, `n3`},
			`id = 4`: {`n4`, `n5`, `n6`},
		},
	},
	{
		testOrNot: true,
		name:      "SimpleSubPartition",
		sqlSchema: "create table %s (id int, name string, PRIMARY KEY(ID, NAME)) partition by list(id) " +
			"(partition p1 values in(3) locate in (%s)," +
			"partition p2 values in(4) partition by list(name) (" +
			"partition p21 values in('tianjin') locate in (%s), " +
			"partition p22 values in('beijing') locate in (%s)) locate in (%s) )",
		sqlType: onlyPartitionCreateType,
		partitionLocateSpace: []partitionSpace{
			{
				stringToValue("sp0"),
				nil,
			},
			{
				stringToValue("sp1"),
				[]partitionSpace{
					{
						stringToValue("sp2"),
						nil,
					},
					{
						stringToValue("sp2"),
						nil,
					},
				},
			},
		},
		expect: roachpb.LocationMap{
			TableSpace: stringToValue(""),
			IndexSpace: map[uint32]*roachpb.LocationValue{
				1: stringToValue(""),
			},
			// incorrect
			PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
				{
					Name:     "p1",
					StartKey: roachpb.RKey([]byte{187, 137, 139}),
					EndKey:   roachpb.RKey([]byte{187, 137, 140}),
					Space:    stringToValue("sp0"),
					IndexID:  1,
				},
				{
					Name:     "p2",
					StartKey: roachpb.RKey([]byte{187, 137, 140}),
					EndKey:   roachpb.RKey([]byte{187, 137, 141}),
					Space:    stringToValue("sp1"),
					IndexID:  1,
				},
				{
					Name:     "p22",
					StartKey: roachpb.RKey([]byte{187, 137, 140, 18, 98, 101, 105, 106, 105, 110, 103, 0, 1}),
					EndKey:   roachpb.RKey([]byte{187, 137, 140, 18, 98, 101, 105, 106, 105, 110, 103, 0, 2}),
					Space:    stringToValue("sp4"),
					IndexID:  1,
				},
				{
					Name:     "p21",
					StartKey: roachpb.RKey([]byte{187, 137, 140, 18, 116, 105, 97, 110, 106, 105, 110, 0, 1}),
					EndKey:   roachpb.RKey([]byte{187, 137, 140, 18, 116, 105, 97, 110, 106, 105, 110, 0, 2}),
					Space:    stringToValue("sp3"),
					IndexID:  1,
				},
			},
		},
		scans: map[string][]string{
			`id = 3`: {`n1`, `n2`, `n3`},
			`id = 4 and name != 'tianjin' and name != 'beijing'`: {`n4`, `n5`, `n6`},
			`id = 4 and name = 'tianjin'`:                        {`n7`, `n8`, `n9`},
			`id = 4 and name = 'beijing'`:                        {`n7`, `n8`, `n9`},
		},
	},
}

// for old test cases
func stringToValue(space string) *roachpb.LocationValue {
	return &roachpb.LocationValue{
		Spaces: []string{space},
		Leases: nil,
	}
}

// setupTestCluster will setup server cluster node
func setupTestCluster(
	_ context.Context,
	t testing.TB,
	space [][]string,
	nodes []string,
	storePath []string,
	nodeNumber int,
) (*gosql.DB, *sqlutils.SQLRunner, *testcluster.TestCluster, func()) {
	cfg := config.DefaultZoneConfig()
	cfg.NumReplicas = proto.Int32(1)
	resetZoneConfig := config.TestingSetDefaultZoneConfig(cfg)

	tsArgs := func(attr string, space []string, storePath string, inMemory bool) base.TestServerArgs {
		return base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &storage.StoreTestingKnobs{
					// Disable LBS because when the scan is happening at the rate it's happening
					// below, it's possible that one of the system ranges trigger a split.
					DisableLoadBasedSplitting: true,
				},
			},
			LocationName: roachpb.LocationName{Names: space},
			ScanInterval: 100 * time.Millisecond,
			StoreSpecs: []base.StoreSpec{
				{Path: storePath, InMemory: inMemory, Attributes: roachpb.Attributes{Attrs: []string{attr}}},
			},
			UseDatabase: "data",
		}
	}
	testServerMap := make(map[int]base.TestServerArgs, nodeNumber)
	for i := 0; i < nodeNumber; i++ {
		var inMemory = true
		if storePath[i] != "" {
			inMemory = false
		}
		testServerMap[i] = tsArgs(nodes[i], space[i], storePath[i], inMemory)
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: testServerMap}
	tc := testcluster.StartTestCluster(t, nodeNumber, tcArgs)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)

	// Disabling store throttling vastly speeds up rebalancing.
	sqlDB.Exec(t, `SET CLUSTER SETTING server.declined_reservation_timeout = '0s'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.failed_reservation_timeout = '0s'`)
	// setup the replica number, default is three
	sqlDB.Exec(t, `ALTER RANGE default CONFIGURE ZONE USING num_replicas = 3;`)

	return tc.Conns[0], sqlDB, tc, func() {
		tc.Stopper().Stop(context.Background())
		resetZoneConfig()
	}
}

// parserPartitionLocateSpace will traver partitionSpace and fill the locateSpace into sqlSchema
func parsePartitionLocateSpace(
	sqlFormatStmt string, t *testing.T, ptSpace []partitionSpace,
) string {

	for _, item := range ptSpace {
		if item.subPartitionSpace != nil {
			sqlFormatStmt = parsePartitionLocateSpace(sqlFormatStmt, t, item.subPartitionSpace)
		}
		sqlFormatStmt = strings.Replace(sqlFormatStmt, "%s", item.spaceName.Spaces[0], 1)
	}
	return sqlFormatStmt
}

// verifyScansFn returns a closure that runs the test's `scans` and returns a
// descriptive error if any of them fail. It is not required for `parse` to have
// been called.
func (t *locateSpaceTest) verifyScansFn(ctx context.Context, db *gosql.DB) func() error {
	return func() error {

		for where, expectedNodes := range t.scans {
			query := fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, tree.NameStringP(&t.name), where)
			log.Infof(ctx, "query: %s", query)
			if err := verifyScansOnNode(db, query, expectedNodes); err != nil {
				if log.V(1) {
					log.Errorf(ctx, "scan verification failed: %s", err)
				}
				return err
			}
		}
		return nil
	}
}

// verifyScanOnNode will check the result of location which each partition/table/index
func verifyScansOnNode(db *gosql.DB, query string, nodes []string) error {
	// TODO(dan): This is a stopgap. At some point we should have a syntax for
	// doing this directly (running a query and getting back the nodes it ran on
	// and attributes/localities of those nodes). Users will also want this to
	// be sure their partitioning is working.
	if _, err := db.Exec(fmt.Sprintf(`SET tracing = on; %s; SET tracing = off`, query)); err != nil {
		return err
	}
	rows, err := db.Query(`SELECT concat(tag, ' ', message) FROM [SHOW TRACE FOR SESSION]`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var scansWrongNode []string
	var traceLines []string
	var traceLine gosql.NullString
	for rows.Next() {
		if err := rows.Scan(&traceLine); err != nil {
			return err
		}
		traceLines = append(traceLines, traceLine.String)
		if strings.Contains(traceLine.String, "read completed") {
			if strings.Contains(traceLine.String, "SystemCon") {
				// Ignore trace lines for the system config range (abbreviated as
				// "SystemCon" in pretty printing of the range descriptor). A read might
				// be performed to the system config range to update the table lease.
				continue
			}
			str := fmt.Sprintf("----trace: %s\n", traceLine.String)
			println(str)
			var flag = false
			for _, node := range nodes {
				if strings.Contains(traceLine.String, node) {
					flag = true
					break
				}
			}
			if !flag {
				scansWrongNode = append(scansWrongNode, traceLine.String)
			}
		}
	}
	if len(scansWrongNode) > 0 {
		var err bytes.Buffer
		nodeString := strings.Join(nodes, ",")
		fmt.Fprintf(&err, "expected to scan on %s: %s\n%s\nfull trace:",
			nodeString, query, strings.Join(scansWrongNode, "\n"))
		for _, traceLine := range traceLines {
			err.WriteString("\n  ")
			err.WriteString(traceLine)
		}
		return errors.New(err.String())
	}
	return nil
}

func mkUserTableDescriptor(
	crtab *tree.CreateTable, t *testing.T,
) (*sqlbase.MutableTableDescriptor, error) {

	var tabDesc sqlbase.MutableTableDescriptor
	var err error

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	crtab.HoistConstraints()
	if crtab.IfNotExists {
		return nil, pgerror.Unimplemented("import.if-no-exists", "unsupported IF NOT EXISTS")
	}
	if crtab.Interleave != nil {
		return nil, pgerror.Unimplemented("import.interleave", "interleaved not supported")
	}
	if crtab.AsSource != nil {
		return nil, pgerror.Unimplemented("import.create-as", "CREATE AS not supported")
	}

	filteredDefs := crtab.Defs[:0]
	for i := range crtab.Defs {
		switch def := crtab.Defs[i].(type) {
		case *tree.CheckConstraintTableDef,
			*tree.FamilyTableDef,
			*tree.IndexTableDef,
			*tree.UniqueConstraintTableDef:
			// ignore
		case *tree.ColumnTableDef:
			if def.Computed.Expr != nil {
				return nil, pgerror.Unimplemented("import.computed", "computed columns not supported: %s", tree.AsString(def))
			}

			if err := sql.SimplifySerialInColumnDefWithRowID(ctx, def, &crtab.Table); err != nil {
				return nil, err
			}

		default:
			return nil, pgerror.Unimplemented(fmt.Sprintf("import.%T", def), "unsupported table definition: %s", tree.AsString(def))
		}
		filteredDefs = append(filteredDefs, crtab.Defs[i])

	}
	crtab.Defs = filteredDefs

	const parentID, id = keys.MinUserDescID, keys.MinUserDescID + 1
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	//Plan a statement.
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	internalPlanner, _ := sql.NewInternalPlanner(
		"test",
		client.NewTxn(ctx, db, s.NodeID(), client.RootTxn),
		security.RootUser,
		&sql.MemoryMetrics{},
		&execCfg,
	)
	vt := internalPlanner.(sql.SchemaResolver)
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(st)
	privileges := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, sqlbase.AdminRole)

	status := tc.Servers[0].GetStatus()
	tabDesc, err = sql.MakeTableDesc(
		ctx,
		nil,
		vt,
		st,
		crtab,
		parentID,
		id,
		hlc.Timestamp{},
		privileges,
		nil,
		&semaCtx,
		&evalCtx,
		&status,
		nil,
	)

	tabDesc.LocateSpaceName = crtab.LocateSpaceName.ToValue()

	if err != nil {
		return nil, err
	}

	return &tabDesc, err

}

// TestInit will do the cluster init  job
// default setup three center and nine node
func Init(t *testing.T) {
	// t.Log("Test Init!!")
	ctx := context.Background()
	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"}
	spaces := [][]string{
		{"sp0"},
		{"sp0"},
		{"sp0"},
		{"sp1"},
		{"sp1"},
		{"sp1"},
		{"sp2"},
		{"sp2"},
		{"sp2"},
	}
	storePath := []string{
		"",
		"",
		"",
		"",
		"",
		"",
		"",
		"",
		"",
		"",
		"",
	}
	db, sqlDB, tc, cleanup = setupTestCluster(ctx, t, spaces, nodes, storePath, len(nodes))
	// t.Log("Cluster has been setup!!")
}

// TestPartitionDescriptorGeneration will do the PartitionDescriptor construct test
func TestPartitionDescriptorGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	Init(t)
	defer cleanup()
	for i := 0; i < len(testCase); i++ {
		oneCase := testCase[i]
		if !oneCase.testOrNot {
			continue
		}

		switch oneCase.sqlType {
		case onlyTableCreateType:
			oneCase.parse.createStmt = fmt.Sprintf(oneCase.sqlSchema, oneCase.name, oneCase.tableLocateSpace.Spaces[0])

		case onlyIndexCreateType:
			oneCase.parse.createStmt = fmt.Sprintf(oneCase.sqlSchema, oneCase.name, oneCase.indexLocateSpace[0].Spaces[0])
		case onlyPartitionCreateType:
			// first replace the table name
			oneCase.sqlSchema = strings.Replace(oneCase.sqlSchema, "%s", oneCase.name, 1)
			oneCase.parse.createStmt = parsePartitionLocateSpace(oneCase.sqlSchema, t, oneCase.partitionLocateSpace)
		}

		// parse input string to AST stmt
		if oneCase.preSchema != "" {
			sqlDB.Exec(t, oneCase.preSchema)
		}
		stmt, err := parser.ParseOne(oneCase.parse.createStmt, false)

		if err != nil {
			t.Errorf(oneCase.parse.createStmt)
			t.Errorf(err.Error())
			t.Fatal("Parse failed!!!")
		}
		if oneCase.sqlType == onlyTableCreateType ||
			oneCase.sqlType == onlyPartitionCreateType {
			createTable, ok := stmt.AST.(*tree.CreateTable)
			if !ok {
				t.Fatal("parse to AST failed!!")
			}
			st := cluster.MakeTestingClusterSettings()
			// first construct mkUserTableDescriptor
			tabDesc, err := mkUserTableDescriptor(createTable, t)
			//	ctx, st, createTable, parentID, tableID, importccl.NoFKs, hlc.UnixNano())
			if err != nil {
				t.Fatal("table descriptor construct failed!!")
			}
			oneCase.parse.tabDesc = tabDesc.TableDesc()
			oneCase.parse.tabDesc.LocateSpaceName = tabDesc.LocateSpaceName
			testCase[i].parse.tabDesc = oneCase.parse.tabDesc

			// Second check the TableDescriptor is valid or not
			if err := oneCase.parse.tabDesc.ValidateTable(st); err != nil {
				t.Fatal("Descriptor construct failed!!")
			}
		}
	}

	t.Run("LocationMapGeneration", locationMapGeneration)
	t.Run("RangeSplitAndNode", rangeSplitAndNodeTest)
}

// locationMapGeneration will do the LocationMap construct test
func locationMapGeneration(t *testing.T) {
	t.Skip("skip by gzq")
	for _, oneCase := range testCase {
		if oneCase.testOrNot {
			switch oneCase.sqlType {
			case onlyTableCreateType:
				oneCase.parse.createStmt = fmt.Sprintf(oneCase.sqlSchema, oneCase.name, oneCase.tableLocateSpace)

			case onlyIndexCreateType:
				oneCase.parse.createStmt = fmt.Sprintf(oneCase.sqlSchema, oneCase.name, oneCase.indexLocateSpace[0])
			case onlyPartitionCreateType:
				// first replace the table name
				oneCase.sqlSchema = strings.Replace(oneCase.sqlSchema, "%s", oneCase.name, 1)
				oneCase.parse.createStmt = parsePartitionLocateSpace(oneCase.sqlSchema, t, oneCase.partitionLocateSpace)
			}

			// case index create stmt there is no tabDesc construct so jump getLocationMap
			if oneCase.parse.tabDesc == nil {
				continue
			}
			locationMap, err := GetLocationMapICL(oneCase.parse.tabDesc)
			if err != nil {
				t.Fatal("LocationMap construct failed!")
			}

			if !locationMap.Equal(oneCase.expect) {
				fmt.Printf("%q\n", locationMap)
				fmt.Printf("%q", oneCase.expect)
				t.Fatal("LocationMap is constructed in wrong way!")
			}
		}
	}
}

func queryRangeSplitStatus(t *testing.T) error {
	rows, err := db.Query(`select concat(start_pretty,'----' ,end_pretty, '-----', table_name,'-----',replicas::string)  
			from zbdb_internal.ranges where end_pretty not like '%System%';`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var traceLine gosql.NullString
	// t.Log("*** Split traceLines ***")
	for rows.Next() {
		if err := rows.Scan(&traceLine); err != nil {
			return err
		}
		// t.Log(traceLine)
	}
	return nil
}

// rangeSplitAndNodeTest will construct test sql and run it, at last it will verify result of echo test job
func rangeSplitAndNodeTest(t *testing.T) {
	// t.Log("-- Feature test!(Contains:table/index/partition locate space test.)")
	for _, oneCase := range testCase {
		if !oneCase.testOrNot {
			continue
		}
		t.Run(oneCase.name, func(t *testing.T) {
			switch oneCase.sqlType {
			case onlyTableCreateType:
				oneCase.parse.createStmt = fmt.Sprintf(oneCase.sqlSchema, tree.NameStringP(&oneCase.name), oneCase.tableLocateSpace.Spaces[0])
			case onlyIndexCreateType:
				oneCase.parse.createStmt = fmt.Sprintf(oneCase.sqlSchema, tree.NameStringP(&oneCase.name), oneCase.indexLocateSpace[0].Spaces[0])
			case onlyPartitionCreateType:
				// first replace the table name
				oneCase.sqlSchema = strings.Replace(oneCase.sqlSchema, "%s", tree.NameStringP(&oneCase.name), 1)
				oneCase.parse.createStmt = parsePartitionLocateSpace(oneCase.sqlSchema, t, oneCase.partitionLocateSpace)
			}
			ctx := context.Background()
			sqlDB.Exec(t, oneCase.parse.createStmt)
			time.Sleep(time.Second * 10)
			if err := queryRangeSplitStatus(t); err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, oneCase.verifyScansFn(ctx, db))
		})
	}
}
