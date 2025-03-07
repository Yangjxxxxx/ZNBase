// Copyright 2014  The Cockroach Authors.
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

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/server/debug"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/storagepb"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getAdminJSONProto(
	ts serverutils.TestServerInterface, path string, response protoutil.Message,
) error {
	return serverutils.GetJSONProto(ts, adminPrefix+path, response)
}

func postAdminJSONProto(
	ts serverutils.TestServerInterface, path string, request, response protoutil.Message,
) error {
	return serverutils.PostJSONProto(ts, adminPrefix+path, request, response)
}

// getText fetches the HTTP response body as text in the form of a
// byte slice from the specified URL.
func getText(ts serverutils.TestServerInterface, url string) ([]byte, error) {
	httpClient, err := ts.GetAuthenticatedHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// getJSON fetches the JSON from the specified URL and returns
// it as unmarshaled JSON. Returns an error on any failure to fetch
// or unmarshal response body.
func getJSON(ts serverutils.TestServerInterface, url string) (interface{}, error) {
	body, err := getText(ts, url)
	if err != nil {
		return nil, err
	}
	var jI interface{}
	if err := json.Unmarshal(body, &jI); err != nil {
		return nil, errors.Wrapf(err, "body is:\n%s", body)
	}
	return jI, nil
}

// debugURL returns the root debug URL.
func debugURL(s serverutils.TestServerInterface) string {
	return s.AdminURL() + debug.Endpoint
}

// TestAdminDebugExpVar verifies that cmdline and memstats variables are
// available via the /debug/vars link.
func TestAdminDebugExpVar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	jI, err := getJSON(s, debugURL(s)+"vars")
	if err != nil {
		t.Fatalf("failed to fetch JSON: %v", err)
	}
	j := jI.(map[string]interface{})
	if _, ok := j["cmdline"]; !ok {
		t.Error("cmdline not found in JSON response")
	}
	if _, ok := j["memstats"]; !ok {
		t.Error("memstats not found in JSON response")
	}
}

// TestAdminDebugMetrics verifies that cmdline and memstats variables are
// available via the /debug/metrics link.
func TestAdminDebugMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	jI, err := getJSON(s, debugURL(s)+"metrics")
	if err != nil {
		t.Fatalf("failed to fetch JSON: %v", err)
	}
	j := jI.(map[string]interface{})
	if _, ok := j["cmdline"]; !ok {
		t.Error("cmdline not found in JSON response")
	}
	if _, ok := j["memstats"]; !ok {
		t.Error("memstats not found in JSON response")
	}
}

// TestAdminDebugPprof verifies that pprof tools are available.
// via the /debug/pprof/* links.
func TestAdminDebugPprof(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	body, err := getText(s, debugURL(s)+"pprof/block?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	if exp := "contention:\ncycles/second="; !bytes.Contains(body, []byte(exp)) {
		t.Errorf("expected %s to contain %s", body, exp)
	}
}

// TestAdminDebugTrace verifies that the net/trace endpoints are available
// via /debug/{requests,events}.
func TestAdminDebugTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	tc := []struct {
		segment, search string
	}{
		{"requests", "<title>/debug/requests</title>"},
		{"events", "<title>events</title>"},
	}

	for _, c := range tc {
		body, err := getText(s, debugURL(s)+c.segment)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(body, []byte(c.search)) {
			t.Errorf("expected %s to be contained in %s", c.search, body)
		}
	}
}

// TestAdminDebugRedirect verifies that the /debug/ endpoint is redirected to on
// incorrect /debug/ paths.
func TestAdminDebugRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	expURL := debugURL(s)
	origURL := expURL + "incorrect"

	// There are no particular permissions on admin endpoints, TestUser is fine.
	client, err := testutils.NewTestBaseContext(TestUser).GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	// Don't follow redirects automatically.
	redirectAttemptedError := errors.New("redirect")
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return redirectAttemptedError
	}

	resp, err := client.Get(origURL)
	if urlError, ok := err.(*url.Error); ok && urlError.Err == redirectAttemptedError {
		// Ignore the redirectAttemptedError.
		err = nil
	}
	if err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusMovedPermanently {
			t.Errorf("expected status code %d; got %d", http.StatusMovedPermanently, resp.StatusCode)
		}
		if redirectURL, err := resp.Location(); err != nil {
			t.Error(err)
		} else if foundURL := redirectURL.String(); foundURL != expURL {
			t.Errorf("expected location %s; got %s", expURL, foundURL)
		}
	}
}

func TestAdminAPIDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	ac := log.AmbientContext{Tracer: s.ClusterSettings().Tracer}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	// Test databases endpoint.
	const testdb = "test"
	query := "CREATE DATABASE " + testdb
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	var resp serverpb.DatabasesResponse
	if err := getAdminJSONProto(s, "databases", &resp); err != nil {
		t.Fatal(err)
	}

	expectedDBs := []string{"defaultdb", "postgres", "system", testdb}
	if a, e := len(resp.Databases), len(expectedDBs); a != e {
		t.Fatalf("length of result %d != expected %d", a, e)
	}

	sort.Strings(resp.Databases)
	for i, e := range expectedDBs {
		if a := resp.Databases[i]; a != e {
			t.Fatalf("database name %s != expected %s", a, e)
		}
	}

	// Test database details endpoint.
	privileges := []string{"USAGE"}
	allPrivileges := []string{"CREATE", "DROP", "USAGE"}
	testuser := "testuser"
	createUserQuery := "CREATE USER " + testuser
	if _, err := db.Exec(createUserQuery); err != nil {
		t.Fatal(err)
	}

	grantQuery := "GRANT " + strings.Join(privileges, ", ") + " ON DATABASE " + testdb + " TO " + testuser
	if _, err := db.Exec(grantQuery); err != nil {
		t.Fatal(err)
	}

	var details serverpb.DatabaseDetailsResponse
	if err := getAdminJSONProto(s, "databases/"+testdb, &details); err != nil {
		t.Fatal(err)
	}

	if a, e := len(details.Grants), 7; a != e {
		t.Fatalf("# of grants %d != expected %d", a, e)
	}

	userGrants := make(map[string][]string)
	for _, grant := range details.Grants {
		switch grant.User {
		case sqlbase.AdminRole, security.RootUser, sqlbase.PublicRole, testuser:
			userGrants[grant.User] = append(userGrants[grant.User], grant.Privileges...)
		default:
			t.Fatalf("unknown grant to user %s", grant.User)
		}
	}
	for u, p := range userGrants {
		switch u {
		case sqlbase.AdminRole:
			if !reflect.DeepEqual(p, allPrivileges) {
				t.Fatalf("privileges %v != expected %v", p, privileges)
			}
		case security.RootUser:
			if !reflect.DeepEqual(p, allPrivileges) {
				t.Fatalf("privileges %v != expected %v", p, privileges)
			}
		case testuser:
			sort.Strings(p)
			if !reflect.DeepEqual(p, privileges) {
				t.Fatalf("privileges %v != expected %v", p, privileges)
			}
		case sqlbase.PublicRole:
			sort.Strings(p)
			if !reflect.DeepEqual(p, privileges) {
				t.Fatalf("privileges %v != expected %v", p, privileges)
			}
		default:
			t.Fatalf("unknown grant to user %s", u)
		}
	}

	// Verify Descriptor ID.
	path, err := ts.admin.queryDescriptorIDPath(ctx, security.RootUser, []string{testdb})
	if err != nil {
		t.Fatal(err)
	}
	if a, e := details.DescriptorID, int64(path[1]); a != e {
		t.Fatalf("db had descriptorID %d, expected %d", a, e)
	}
}

func TestAdminAPIDatabaseDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const errPattern = "database.+does not exist"
	if err := getAdminJSONProto(s, "databases/i_do_not_exist", nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPIDatabaseSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const fakedb = "system;DROP DATABASE system;"
	const path = "databases/" + fakedb
	const errPattern = `database \\"` + fakedb + `\\" does not exist`
	if err := getAdminJSONProto(s, path, nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPINonTableStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())
	s := testCluster.Server(0)

	// Skip TableStatsResponse.Stats comparison, since it includes data which
	// aren't consistent (time, bytes).
	expectedResponse := serverpb.NonTableStatsResponse{
		TimeSeriesStats: &serverpb.TableStatsResponse{
			RangeCount:   1,
			ReplicaCount: 3,
			NodeCount:    3,
		},
		InternalUseStats: &serverpb.TableStatsResponse{
			RangeCount:   8,
			ReplicaCount: 12,
			NodeCount:    3,
		},
	}

	var resp serverpb.NonTableStatsResponse
	if err := getAdminJSONProto(s, "nontablestats", &resp); err != nil {
		t.Fatal(err)
	}

	assertExpectedStatsResponse := func(expected, actual *serverpb.TableStatsResponse) {
		assert.Equal(t, expected.RangeCount, actual.RangeCount)
		assert.Equal(t, expected.ReplicaCount, actual.ReplicaCount)
		assert.Equal(t, expected.NodeCount, actual.NodeCount)
	}

	assertExpectedStatsResponse(expectedResponse.TimeSeriesStats, resp.TimeSeriesStats)
	assertExpectedStatsResponse(expectedResponse.InternalUseStats, resp.InternalUseStats)
}

// TODO(celia): I expect all the ranges listed on the Database page to equal
// the RangeCount returned from doing a span on [LocalMax, MaxKey). For a cluster
// with no user data, all the ranges on the Databases page consist of:
// 1) the total ranges listed for the system database
// 2) the total ranges listed for the Non-Table data
func TestRangeCount_MissingOneRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())
	s := testCluster.Server(0)

	// Sum up ranges for non-table parts of the system returned
	// from the "nontablestats" enpoint.
	getNonTableRangeCount := func() int64 {
		var resp serverpb.NonTableStatsResponse
		if err := getAdminJSONProto(s, "nontablestats", &resp); err != nil {
			t.Fatal(err)
		}
		return resp.TimeSeriesStats.RangeCount + resp.InternalUseStats.RangeCount
	}

	// Sum up ranges from system database tables returned
	// from the "databases/system/tables/{table}" endpoints.
	getSystemTableRangeCount := func() int64 {
		var systemTableRangeCount int64
		var dbResp serverpb.DatabaseDetailsResponse
		if err := getAdminJSONProto(s, "databases/system", &dbResp); err != nil {
			t.Fatal(err)
		}
		for _, tableName := range dbResp.TableNames {
			var tblResp serverpb.TableStatsResponse
			path := "databases/system/tables/" + tableName + "/stats"
			if err := getAdminJSONProto(s, path, &tblResp); err != nil {
				t.Fatal(err)
			}
			systemTableRangeCount += tblResp.RangeCount
		}
		return systemTableRangeCount
	}

	getRangeCountFromFullSpan := func() int64 {
		adminServer := s.(*TestServer).Server.admin
		stats, err := adminServer.statsForSpan(context.Background(), roachpb.Span{
			Key:    keys.LocalMax,
			EndKey: keys.MaxKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		return stats.RangeCount
	}

	totalRangeCount := getRangeCountFromFullSpan()
	nonTableRangeCount := getNonTableRangeCount()
	systemTableRangeCount := getSystemTableRangeCount()

	// expected: expectedRangeCount == nonTableRangeCount+systemTableRangeCount
	// actual: expectedRangeCount > nonTableRangeCount+systemTableRangeCount
	if totalRangeCount == nonTableRangeCount+systemTableRangeCount {
		t.Fail()
	}

	// TODO(celia): We're missing 1 range -- where is it?
	expectedMissingRangeCount := int64(1)
	schemaRangeCount := int64(2)
	assert.Equal(t,
		totalRangeCount,
		nonTableRangeCount+systemTableRangeCount+expectedMissingRangeCount+schemaRangeCount)
}

func TestAdminAPITableDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const fakename = "i_do_not_exist"
	const badDBPath = "databases/" + fakename + "/tables/foo"
	const dbErrPattern = `relation \\"` + fakename + `.foo\\" does not exist`
	if err := getAdminJSONProto(s, badDBPath, nil); !testutils.IsError(err, dbErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, dbErrPattern)
	}

	const badTablePath = "databases/system/tables/" + fakename
	const tableErrPattern = `relation \\"system.` + fakename + `\\" does not exist`
	if err := getAdminJSONProto(s, badTablePath, nil); !testutils.IsError(err, tableErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, tableErrPattern)
	}
}

func TestAdminAPITableSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	const fakeTable = "users;DROP DATABASE system;"
	const path = "databases/system/tables/" + fakeTable
	const errPattern = `relation \"system.` + fakeTable + `\" does not exist`
	if err := getAdminJSONProto(s, path, nil); !testutils.IsError(err, regexp.QuoteMeta(errPattern)) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPITableDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name, dbName, tblName string
	}{
		{name: "lower", dbName: "test", tblName: "tbl"},
		{name: "lower with space", dbName: "test test", tblName: "tbl tbl"},
		{name: "upper", dbName: "TEST", tblName: "TBL"}, // Regression test for issue #14056
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(context.TODO())
			ts := s.(*TestServer)

			escDBName := tree.NameStringP(&tc.dbName)
			escTblName := tree.NameStringP(&tc.tblName)

			ac := log.AmbientContext{Tracer: s.ClusterSettings().Tracer}
			ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
			defer span.Finish()

			setupQueries := []string{
				fmt.Sprintf("CREATE DATABASE %s", escDBName),
				fmt.Sprintf(`CREATE TABLE %s.%s (
							nulls_allowed INT8,
							nulls_not_allowed INT8 NOT NULL DEFAULT 1000,
							default2 INT8 DEFAULT 2,
							string_default STRING DEFAULT 'default_string'
						)`, escDBName, escTblName),
				fmt.Sprintf("CREATE USER readonly"),
				fmt.Sprintf("CREATE USER app"),
				fmt.Sprintf("GRANT SELECT ON %s.%s TO readonly", escDBName, escTblName),
				fmt.Sprintf("GRANT SELECT,UPDATE,DELETE ON %s.%s TO app", escDBName, escTblName),
				fmt.Sprintf("CREATE INDEX descidx ON %s.%s (default2 DESC)", escDBName, escTblName),
			}

			for _, q := range setupQueries {
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}

			// Perform API call.
			var resp serverpb.TableDetailsResponse
			url := fmt.Sprintf("databases/%s/tables/%s", tc.dbName, tc.tblName)
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}

			// Verify columns.
			expColumns := []serverpb.TableDetailsResponse_Column{
				{Name: "nulls_allowed", Type: "INT8", Nullable: true, DefaultValue: ""},
				{Name: "nulls_not_allowed", Type: "INT8", Nullable: false, DefaultValue: "1000"},
				{Name: "default2", Type: "INT8", Nullable: true, DefaultValue: "2"},
				{Name: "string_default", Type: "STRING", Nullable: true, DefaultValue: "'default_string'"},
				{Name: "rowid", Type: "INT", Nullable: false, DefaultValue: "unique_rowid()", Hidden: true},
			}
			testutils.SortStructs(expColumns, "Name")
			testutils.SortStructs(resp.Columns, "Name")
			if a, e := len(resp.Columns), len(expColumns); a != e {
				t.Fatalf("# of result columns %d != expected %d (got: %#v)", a, e, resp.Columns)
			}
			for i, a := range resp.Columns {
				e := expColumns[i]
				if a.String() != e.String() {
					t.Fatalf("mismatch at column %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify grants.
			expGrants := []serverpb.TableDetailsResponse_Grant{
				{Grantor: sqlbase.AdminRole, User: sqlbase.AdminRole, Privileges: []string{"DROP", "DELETE", "INSERT", "REFERENCES", "SELECT", "TRIGGER", "UPDATE"}},
				{Grantor: sqlbase.AdminRole, User: security.RootUser, Privileges: []string{"DROP", "DELETE", "INSERT", "REFERENCES", "SELECT", "TRIGGER", "UPDATE"}},
				{Grantor: security.RootUser, User: "app", Privileges: []string{"DELETE", "SELECT", "UPDATE"}},
				{Grantor: security.RootUser, User: "readonly", Privileges: []string{"SELECT"}},
			}
			testutils.SortStructs(expGrants, "User", "Grantor")
			testutils.SortStructs(resp.Grants, "User", "Grantor")
			if a, e := len(resp.Grants), len(expGrants); a != e {
				t.Fatalf("# of grant columns %d != expected %d (got: %#v)", a, e, resp.Grants)
			}
			for i, a := range resp.Grants {
				e := expGrants[i]
				sort.Strings(a.Privileges)
				sort.Strings(e.Privileges)
				if a.String() != e.String() {
					t.Fatalf("mismatch at index %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify indexes.
			expIndexes := []serverpb.TableDetailsResponse_Index{
				{Name: "primary", Column: "rowid", Direction: "ASC", Unique: true, Seq: 1},
				{Name: "descidx", Column: "rowid", Direction: "ASC", Unique: false, Seq: 2, Implicit: true},
				{Name: "descidx", Column: "default2", Direction: "DESC", Unique: false, Seq: 1},
			}
			testutils.SortStructs(expIndexes, "Name", "Seq")
			testutils.SortStructs(resp.Indexes, "Name", "Seq")
			for i, a := range resp.Indexes {
				e := expIndexes[i]
				if a.String() != e.String() {
					t.Fatalf("mismatch at index %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify range count.
			if a, e := resp.RangeCount, int64(1); a != e {
				t.Fatalf("# of ranges %d != expected %d", a, e)
			}

			// Verify Create Table Statement.
			{

				showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE %s.%s", escDBName, escTblName)

				row := db.QueryRow(showCreateTableQuery)
				var createStmt, tableName string
				if err := row.Scan(&tableName, &createStmt); err != nil {
					t.Fatal(err)
				}

				if a, e := resp.CreateTableStatement, createStmt; a != e {
					t.Fatalf("mismatched create table statement; expected %s, got %s", e, a)
				}
			}

			// Verify Descriptor ID.
			path, err := ts.admin.queryDescriptorIDPath(ctx,
				security.RootUser, []string{tc.dbName, tc.tblName})
			if err != nil {
				t.Fatal(err)
			}
			if a, e := resp.DescriptorID, int64(path[2]); a != e {
				t.Fatalf("table had descriptorID %d, expected %d", a, e)
			}
		})
	}
}

// TestAdminAPIZoneDetails verifies the zone configuration information returned
// for both DatabaseDetailsResponse AND TableDetailsResponse.
func TestAdminAPIZoneDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	// Create database and table.
	ac := log.AmbientContext{Tracer: s.ClusterSettings().Tracer}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()
	setupQueries := []string{
		"CREATE DATABASE test",
		"CREATE TABLE test.tbl (val STRING)",
	}
	for _, q := range setupQueries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("error executing '%s': %s", q, err)
		}
	}

	// Function to verify the zone for table "test.tbl" as returned by the Admin
	// API.
	verifyTblZone := func(
		expectedZone config.ZoneConfig, expectedLevel serverpb.ZoneConfigurationLevel,
	) {
		var resp serverpb.TableDetailsResponse
		if err := getAdminJSONProto(s, "databases/test/tables/tbl", &resp); err != nil {
			t.Fatal(err)
		}
		if a, e := &resp.ZoneConfig, &expectedZone; !proto.Equal(a, e) {
			t.Errorf("actual table zone config %v did not match expected value %v", a, e)
		}
		if a, e := resp.ZoneConfigLevel, expectedLevel; a != e {
			t.Errorf("actual table ZoneConfigurationLevel %s did not match expected value %s", a, e)
		}
		if t.Failed() {
			t.FailNow()
		}
	}

	// Function to verify the zone for database "test" as returned by the Admin
	// API.
	verifyDbZone := func(
		expectedZone config.ZoneConfig, expectedLevel serverpb.ZoneConfigurationLevel,
	) {
		var resp serverpb.DatabaseDetailsResponse
		if err := getAdminJSONProto(s, "databases/test", &resp); err != nil {
			t.Fatal(err)
		}
		if a, e := &resp.ZoneConfig, &expectedZone; !proto.Equal(a, e) {
			t.Errorf("actual db zone config %v did not match expected value %v", a, e)
		}
		if a, e := resp.ZoneConfigLevel, expectedLevel; a != e {
			t.Errorf("actual db ZoneConfigurationLevel %s did not match expected value %s", a, e)
		}
		if t.Failed() {
			t.FailNow()
		}
	}

	// Function to store a zone config for a given object ID.
	setZone := func(zoneCfg config.ZoneConfig, id sqlbase.ID) {
		zoneBytes, err := protoutil.Marshal(&zoneCfg)
		if err != nil {
			t.Fatal(err)
		}
		const query = `INSERT INTO system.zones VALUES($1, $2)`
		if _, err := db.Exec(query, id, zoneBytes); err != nil {
			t.Fatalf("error executing '%s': %s", query, err)
		}
	}

	// Verify zone matches cluster default.
	verifyDbZone(config.DefaultZoneConfig(), serverpb.ZoneConfigurationLevel_CLUSTER)
	verifyTblZone(config.DefaultZoneConfig(), serverpb.ZoneConfigurationLevel_CLUSTER)

	// Get ID path for table. This will be an array of three IDs, containing the ID of the root namespace,
	// the database, and the table (in that order).
	idPath, err := ts.admin.queryDescriptorIDPath(ctx, security.RootUser, []string{"test", "tbl"})
	if err != nil {
		t.Fatal(err)
	}

	// Apply zone configuration to database and check again.
	dbZone := config.ZoneConfig{
		RangeMinBytes: proto.Int64(456),
	}
	setZone(dbZone, idPath[1])
	verifyDbZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)
	verifyTblZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)

	// Apply zone configuration to table and check again.
	tblZone := config.ZoneConfig{
		RangeMinBytes: proto.Int64(789),
	}
	setZone(tblZone, idPath[2])
	verifyDbZone(dbZone, serverpb.ZoneConfigurationLevel_DATABASE)
	verifyTblZone(tblZone, serverpb.ZoneConfigurationLevel_TABLE)
}

func TestAdminAPIUsers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create sample users.
	query := `
INSERT INTO system.users (username, "hashedPassword")
VALUES ('adminUser', 'abc'), ('bob', 'xyz')`
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	// Query the API for users.
	var resp serverpb.UsersResponse
	if err := getAdminJSONProto(s, "users", &resp); err != nil {
		t.Fatal(err)
	}
	expResult := serverpb.UsersResponse{
		Users: []serverpb.UsersResponse_User{
			{Username: "adminUser"},
			{Username: "bob"},
			{Username: "root"},
		},
	}

	// Verify results.
	const sortKey = "Username"
	testutils.SortStructs(resp.Users, sortKey)
	testutils.SortStructs(expResult.Users, sortKey)
	if !reflect.DeepEqual(resp, expResult) {
		t.Fatalf("result %v != expected %v", resp, expResult)
	}
}

func TestAdminAPIEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	setupQueries := []string{
		"CREATE DATABASE api_test",
		"CREATE TABLE api_test.tbl1 (a INT)",
		"CREATE TABLE api_test.tbl2 (a INT)",
		"CREATE TABLE api_test.tbl3 (a INT)",
		"DROP TABLE api_test.tbl1",
		"DROP TABLE api_test.tbl2",
		"SET CLUSTER SETTING cluster.organization = 'somestring';",
	}
	for _, q := range setupQueries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("error executing '%s': %s", q, err)
		}
	}

	const allEvents = ""
	type testcase struct {
		eventType sql.EventLogType
		hasLimit  bool
		limit     int
		expCount  int
	}
	testcases := []testcase{
		{sql.EventLogNodeJoin, false, 0, 1},
		{sql.EventLogNodeRestart, false, 0, 0},
		{sql.EventLogDropDatabase, false, 0, 0},
		{sql.EventLogCreateDatabase, false, 0, 3},
		{sql.EventLogDropTable, false, 0, 2},
		{sql.EventLogCreateTable, false, 0, 3},
		{sql.EventLogSetClusterSetting, false, 0, 5},
		{sql.EventLogCreateTable, true, 0, 3},
		{sql.EventLogCreateTable, true, -1, 3},
		{sql.EventLogCreateTable, true, 2, 2},
	}
	minTotalEvents := 0
	for _, tc := range testcases {
		if !tc.hasLimit {
			minTotalEvents += tc.expCount
		}
	}
	testcases = append(testcases, testcase{allEvents, false, 0, minTotalEvents})

	for i, tc := range testcases {
		url := "events"
		if tc.eventType != allEvents {
			url += "?type=" + string(tc.eventType)
			if tc.hasLimit {
				url += fmt.Sprintf("&limit=%d", tc.limit)
			}
		}

		t.Run(url, func(t *testing.T) {
			var resp serverpb.EventsResponse
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}
			if tc.eventType == allEvents {
				// When retrieving all events, we expect that there will be some system
				// database migrations, unrelated to this test, that add to the log entry
				// count. So, we do a looser check here.
				if a, min := len(resp.Events), tc.expCount; a < tc.expCount {
					t.Fatalf("%d: total # of events %d < min %d", i, a, min)
				}
			} else {
				if a, e := len(resp.Events), tc.expCount; a != e {
					t.Fatalf("%d: # of %s events %d != expected %d", i, tc.eventType, a, e)
				}
			}

			// Ensure we don't have blank / nonsensical fields.
			for _, e := range resp.Events {
				if e.Timestamp == (time.Time{}) {
					t.Errorf("%d: missing/empty timestamp", i)
				}

				if len(tc.eventType) > 0 {
					if a, e := e.EventType, string(tc.eventType); a != e {
						t.Errorf("%d: event type %s != expected %s", i, a, e)
					}
				} else {
					if len(e.EventType) == 0 {
						t.Errorf("%d: missing event type in event", i)
					}
				}

				isSettingChange := e.EventType == string(sql.EventLogSetClusterSetting)

				if e.TargetID == 0 && !isSettingChange {
					t.Errorf("%d: missing/empty TargetID", i)
				}
				if e.ReportingID == 0 {
					t.Errorf("%d: missing/empty ReportingID", i)
				}
				if len(e.Info) == 0 {
					t.Errorf("%d: missing/empty Info", i)
				}
				if isSettingChange && strings.Contains(e.Info, "somestring") {
					t.Errorf("%d: un-redacted 'somestring' in Info", i)
				}
				if len(e.UniqueID) == 0 {
					t.Errorf("%d: missing/empty UniqueID", i)
				}
			}
		})
	}
}

func TestAdminAPISettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := log.Scope(t)
	defer sc.Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Any bool that defaults to true will work here.
	const settingKey = "sql.metrics.statement_details.enabled"
	st := s.ClusterSettings()
	allKeys := settings.Keys()

	checkSetting := func(t *testing.T, k string, v serverpb.SettingsResponse_Value) {
		ref, ok := settings.Lookup(k)
		if !ok {
			t.Fatalf("%s: not found after initial lookup", k)
		}
		typ := ref.Typ()

		if typ == "s" && k != "version" {
			if v.Value != "<redacted>" && v.Value != "" {
				t.Errorf("%s: expected redacted value for %v, got %s", k, ref, v.Value)
			}
		} else {
			if ref.String(&st.SV) != v.Value {
				t.Errorf("%s: expected value %v, got %s", k, ref, v.Value)
			}
		}

		if desc := ref.Description(); desc != v.Description {
			t.Errorf("%s: expected description %s, got %s", k, desc, v.Description)
		}
		if typ != v.Type {
			t.Errorf("%s: expected type %s, got %s", k, typ, v.Type)
		}
	}

	t.Run("all", func(t *testing.T) {
		var resp serverpb.SettingsResponse

		if err := getAdminJSONProto(s, "settings", &resp); err != nil {
			t.Fatal(err)
		}

		// Check that all expected keys were returned
		if len(allKeys) != len(resp.KeyValues) {
			t.Fatalf("expected %d keys, got %d", len(allKeys), len(resp.KeyValues))
		}
		for _, k := range allKeys {
			if _, ok := resp.KeyValues[k]; !ok {
				t.Fatalf("expected key %s not found in response", k)
			}
		}

		// Check that the test key is listed and the values come indeed
		// from the settings package unchanged.
		seenRef := false
		for k, v := range resp.KeyValues {
			if k == settingKey {
				seenRef = true
				if v.Value != "true" {
					t.Errorf("%s: expected true, got %s", k, v.Value)
				}
			}

			checkSetting(t, k, v)
		}

		if !seenRef {
			t.Fatalf("failed to observe test setting %s, got %q", settingKey, resp.KeyValues)
		}
	})

	t.Run("one-by-one", func(t *testing.T) {
		var resp serverpb.SettingsResponse

		// All the settings keys must be retrievable, and their
		// type and description must match.
		for _, k := range allKeys {
			q := make(url.Values)
			q.Add("keys", k)
			url := "settings?" + q.Encode()
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatalf("%s: %v", k, err)
			}
			if len(resp.KeyValues) != 1 {
				t.Fatalf("%s: expected 1 response, got %d", k, len(resp.KeyValues))
			}
			v, ok := resp.KeyValues[k]
			if !ok {
				t.Fatalf("%s: response does not contain key", k)
			}

			checkSetting(t, k, v)
		}
	})
}

func TestAdminAPIUIData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	start := timeutil.Now()

	mustSetUIData := func(keyValues map[string][]byte) {
		if err := postAdminJSONProto(s, "uidata", &serverpb.SetUIDataRequest{
			KeyValues: keyValues,
		}, &serverpb.SetUIDataResponse{}); err != nil {
			t.Fatal(err)
		}
	}

	expectKeyValues := func(expKeyValues map[string][]byte) {
		var resp serverpb.GetUIDataResponse
		queryValues := make(url.Values)
		for key := range expKeyValues {
			queryValues.Add("keys", key)
		}
		url := "uidata?" + queryValues.Encode()
		if err := getAdminJSONProto(s, url, &resp); err != nil {
			t.Fatal(err)
		}
		// Do a two-way comparison. We can't use reflect.DeepEqual(), because
		// resp.KeyValues has timestamps and expKeyValues doesn't.
		for key, actualVal := range resp.KeyValues {
			if a, e := actualVal.Value, expKeyValues[key]; !bytes.Equal(a, e) {
				t.Fatalf("key %s: value = %v, expected = %v", key, a, e)
			}
		}
		for key, expVal := range expKeyValues {
			if a, e := resp.KeyValues[key].Value, expVal; !bytes.Equal(a, e) {
				t.Fatalf("key %s: value = %v, expected = %v", key, a, e)
			}
		}

		// Sanity check LastUpdated.
		for _, val := range resp.KeyValues {
			now := timeutil.Now()
			if val.LastUpdated.Before(start) {
				t.Fatalf("val.LastUpdated %s < start %s", val.LastUpdated, start)
			}
			if val.LastUpdated.After(now) {
				t.Fatalf("val.LastUpdated %s > now %s", val.LastUpdated, now)
			}
		}
	}

	expectValueEquals := func(key string, expVal []byte) {
		expectKeyValues(map[string][]byte{key: expVal})
	}

	expectKeyNotFound := func(key string) {
		var resp serverpb.GetUIDataResponse
		url := "uidata?keys=" + key
		if err := getAdminJSONProto(s, url, &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.KeyValues) != 0 {
			t.Fatal("key unexpectedly found")
		}
	}

	// Basic tests.
	var badResp serverpb.GetUIDataResponse
	const errPattern = "400 Bad Request"
	if err := getAdminJSONProto(s, "uidata", &badResp); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}

	mustSetUIData(map[string][]byte{"k1": []byte("v1")})
	expectValueEquals("k1", []byte("v1"))

	expectKeyNotFound("NON_EXISTENT_KEY")

	mustSetUIData(map[string][]byte{
		"k2": []byte("v2"),
		"k3": []byte("v3"),
	})
	expectValueEquals("k2", []byte("v2"))
	expectValueEquals("k3", []byte("v3"))
	expectKeyValues(map[string][]byte{
		"k2": []byte("v2"),
		"k3": []byte("v3"),
	})

	mustSetUIData(map[string][]byte{"k2": []byte("v2-updated")})
	expectKeyValues(map[string][]byte{
		"k2": []byte("v2-updated"),
		"k3": []byte("v3"),
	})

	// Write a binary blob with all possible byte values, then verify it.
	var buf bytes.Buffer
	for i := 0; i < 997; i++ {
		buf.WriteByte(byte(i % 256))
	}
	mustSetUIData(map[string][]byte{"bin": buf.Bytes()})
	expectValueEquals("bin", buf.Bytes())
}

func TestClusterAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	testutils.RunTrueAndFalse(t, "reportingOn", func(t *testing.T, reportingOn bool) {
		testutils.RunTrueAndFalse(t, "enterpriseOn", func(t *testing.T, enterpriseOn bool) {
			// Override server license check.
			if enterpriseOn {
				old := base.CheckCommercialFeatureEnabled
				base.CheckCommercialFeatureEnabled = func(_ *cluster.Settings, _ uuid.UUID, _, _ string) error {
					return nil
				}
				defer func() { base.CheckCommercialFeatureEnabled = old }()
			}

			if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.enabled = $1`, reportingOn); err != nil {
				t.Fatal(err)
			}

			// We need to retry, because the cluster ID isn't set until after
			// bootstrapping and because setting a cluster setting isn't necessarily
			// instantaneous.
			//
			// Also note that there's a migration that affects `diagnostics.reporting.enabled`,
			// so manipulating the cluster setting var directly is a bad idea.
			testutils.SucceedsSoon(t, func() error {
				var resp serverpb.ClusterResponse
				if err := getAdminJSONProto(s, "cluster", &resp); err != nil {
					return err
				}
				if a, e := resp.ClusterID, s.RPCContext().ClusterID.String(); a != e {
					return errors.Errorf("cluster ID %s != expected %s", a, e)
				}
				if a, e := resp.ReportingEnabled, reportingOn; a != e {
					return errors.Errorf("reportingEnabled = %t, wanted %t", a, e)
				}
				if a, e := resp.EnterpriseEnabled, enterpriseOn; a != e {
					return errors.Errorf("enterpriseEnabled = %t, wanted %t", a, e)
				}
				return nil
			})
		})
	})
}

func TestHealthAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// We need to retry because the node ID isn't set until after
	// bootstrapping.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.HealthResponse
		return getAdminJSONProto(s, "health", &resp)
	})

	// Expire this node's liveness record by pausing heartbeats and advancing the
	// server's clock.
	ts := s.(*TestServer)
	defer ts.nodeLiveness.DisableAllHeartbeatsForTest()()
	self, err := ts.nodeLiveness.Self()
	if err != nil {
		t.Fatal(err)
	}
	s.Clock().Update(hlc.Timestamp(self.Expiration).Add(1, 0))

	expected := "503 Service Unavailable"
	var resp serverpb.HealthResponse
	for {
		if err := getAdminJSONProto(s, "health", &resp); !testutils.IsError(err, expected) {
			type timeouter interface {
				Timeout() bool
			}
			if _, ok := err.(timeouter); ok {
				// Special case for `*http.httpError` which can happen since we
				// have timeouts on our requests and things may not be going so smoothly
				// on the server side. See:
				// https://github.com/znbasedb/znbase/issues/18469
				log.Warningf(context.Background(), "ignoring timeout error: %s (%T)", err, err)
				continue
			}
			t.Errorf("expected %q error, got %v (%T)", expected, err, err)
		}
		break
	}
}

// getSystemJobIDs queries the jobs table for all jobs IDs. Sorted by decreasing creation time.
func getSystemJobIDs(t testing.TB, db *sqlutils.SQLRunner) []int64 {
	rows := db.Query(t, `SELECT job_id FROM zbdb_internal.jobs ORDER BY created DESC;`)
	defer rows.Close()

	res := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		res = append(res, id)
	}
	return res
}

func TestAdminAPIJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Get list of existing jobs (migrations). Assumed to all have succeeded.
	existingIDs := getSystemJobIDs(t, sqlDB)

	testJobs := []struct {
		id       int64
		status   jobs.Status
		details  jobspb.Details
		progress jobspb.ProgressDetails
	}{
		{1, jobs.StatusRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}},
		{2, jobs.StatusRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}},
		{3, jobs.StatusSucceeded, jobspb.BackupDetails{}, jobspb.BackupProgress{}},
		{4, jobs.StatusRunning, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}},
	}
	for _, job := range testJobs {
		payload := jobspb.Payload{Details: jobspb.WrapPayloadDetails(job.details)}
		payloadBytes, err := protoutil.Marshal(&payload)
		if err != nil {
			t.Fatal(err)
		}

		progress := jobspb.Progress{Details: jobspb.WrapProgressDetails(job.progress)}
		// Populate progress.Progress field with a specific progress type based on
		// the job type.
		if _, ok := job.progress.(jobspb.ChangefeedProgress); ok {
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &hlc.Timestamp{},
			}
		} else {
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: 1.0,
			}
		}

		progressBytes, err := protoutil.Marshal(&progress)
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.Exec(t,
			`INSERT INTO system.jobs (id, status, payload, progress) VALUES ($1, $2, $3, $4)`,
			job.id, job.status, payloadBytes, progressBytes,
		)
	}

	const invalidJobType = math.MaxInt32

	testCases := []struct {
		uri         string
		expectedIDs []int64
	}{
		{"jobs", append([]int64{4, 3, 2, 1}, existingIDs...)},
		{"jobs?limit=1", []int64{4}},
		{"jobs?status=running", []int64{4, 2, 1}},
		{"jobs?status=succeeded", append([]int64{3}, existingIDs...)},
		{"jobs?status=pending", []int64{}},
		{"jobs?status=garbage", []int64{}},
		{fmt.Sprintf("jobs?type=%d", jobspb.TypeBackup), []int64{3, 2}},
		{fmt.Sprintf("jobs?type=%d", jobspb.TypeRestore), []int64{1}},
		{fmt.Sprintf("jobs?type=%d", invalidJobType), []int64{}},
		{fmt.Sprintf("jobs?status=running&type=%d", jobspb.TypeBackup), []int64{2}},
	}
	for i, testCase := range testCases {
		var res serverpb.JobsResponse
		if err := getAdminJSONProto(s, testCase.uri, &res); err != nil {
			t.Fatal(err)
		}
		resIDs := []int64{}
		for _, job := range res.Jobs {
			resIDs = append(resIDs, job.ID)
		}
		if e, a := testCase.expectedIDs, resIDs; !reflect.DeepEqual(e, a) {
			t.Errorf("%d: expected job IDs %v, but got %v", i, e, a)
		}
	}
}

func TestAdminAPILocations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	testLocations := []struct {
		localityKey   string
		localityValue string
		latitude      float64
		longitude     float64
	}{
		{"city", "Des Moines", 41.60054, -93.60911},
		{"city", "New York City", 40.71427, -74.00597},
		{"city", "Seattle", 47.60621, -122.33207},
	}
	for _, loc := range testLocations {
		sqlDB.Exec(t,
			`INSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES ($1, $2, $3, $4)`,
			loc.localityKey, loc.localityValue, loc.latitude, loc.longitude,
		)
	}
	var res serverpb.LocationsResponse
	if err := getAdminJSONProto(s, "locations", &res); err != nil {
		t.Fatal(err)
	}
	for i, loc := range testLocations {
		expLoc := serverpb.LocationsResponse_Location{
			LocalityKey:   loc.localityKey,
			LocalityValue: loc.localityValue,
			Latitude:      loc.latitude,
			Longitude:     loc.longitude,
		}
		if !reflect.DeepEqual(res.Locations[i], expLoc) {
			t.Errorf("%d: expected location %v, but got %v", i, expLoc, res.Locations[i])
		}
	}
}

func TestAdminAPIQueryPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE api_test`)
	sqlDB.Exec(t, `CREATE TABLE api_test.t1 (id int primary key, name string)`)
	sqlDB.Exec(t, `CREATE TABLE api_test.t2 (id int primary key, name string)`)

	testCases := []struct {
		query string
		exp   []string
	}{
		{"SELECT sum(id) FROM api_test.t1", []string{"nodeNames\":[\"1\"]", "Out: @1"}},
		{"SELECT sum(1) FROM api_test.t1 JOIN api_test.t2 on t1.id = t2.id", []string{"nodeNames\":[\"1\"]", "Out: @1"}},
	}
	for i, testCase := range testCases {
		var res serverpb.QueryPlanResponse
		queryParam := url.QueryEscape(testCase.query)
		if err := getAdminJSONProto(s, fmt.Sprintf("queryplan?query=%s", queryParam), &res); err != nil {
			t.Errorf("%d: got error %s", i, err)
		}

		for _, exp := range testCase.exp {
			if !strings.Contains(res.DistSQLPhysicalQueryPlan, exp) {
				t.Errorf("%d: expected response %v to contain %s", i, res, exp)
			}
		}
	}

}

func TestAdminAPIRangeLogByRangeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	rangeID := 654321
	testCases := []struct {
		rangeID  int
		hasLimit bool
		limit    int
		expected int
	}{
		{rangeID, true, 0, 2},
		{rangeID, true, -1, 2},
		{rangeID, true, 1, 1},
		{rangeID, false, 0, 2},
		// We'll create one event that has rangeID+1 as the otherRangeID.
		{rangeID + 1, false, 0, 1},
	}

	for _, otherRangeID := range []int{rangeID + 1, rangeID + 2} {
		if _, err := db.Exec(
			`INSERT INTO system.rangelog (
             timestamp, "rangeID", "otherRangeID", "storeID", "eventType"
           ) VALUES (
             now(), $1, $2, $3, $4
          )`,
			rangeID, otherRangeID,
			1, // storeID
			storagepb.RangeLogEventType_add.String(),
		); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range testCases {
		url := fmt.Sprintf("rangelog/%d", tc.rangeID)
		if tc.hasLimit {
			url += fmt.Sprintf("?limit=%d", tc.limit)
		}
		t.Run(url, func(t *testing.T) {
			var resp serverpb.RangeLogResponse
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}

			if e, a := tc.expected, len(resp.Events); e != a {
				t.Fatalf("expected %d events, got %d", e, a)
			}

			for _, event := range resp.Events {
				expID := roachpb.RangeID(tc.rangeID)
				if event.Event.RangeID != expID && event.Event.OtherRangeID != expID {
					t.Errorf("expected rangeID or otherRangeID to be %d, got %d and r%d",
						expID, event.Event.RangeID, event.Event.OtherRangeID)
				}
			}
		})
	}
}

// Test the range log API when queries are not filtered by a range ID (like in
// TestAdminAPIRangeLogByRangeID).
func TestAdminAPIFullRangeLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &storage.StoreTestingKnobs{
					DisableSplitQueue: true,
				},
			},
		})
	defer s.Stopper().Stop(context.Background())

	// Insert something in the rangelog table, otherwise it's empty for new
	// clusters.
	rows, err := db.Query(`SELECT count(1) FROM system.rangelog`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing row")
	}
	var cnt int
	if err := rows.Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("expected 0 rows in system.rangelog, found: %d", cnt)
	}
	const rangeID = 100
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(
			`INSERT INTO system.rangelog (
             timestamp, "rangeID", "storeID", "eventType"
           ) VALUES (now(), $1, 1, $2)`,
			rangeID,
			storagepb.RangeLogEventType_add.String(),
		); err != nil {
			t.Fatal(err)
		}
	}
	expectedEvents := 10

	testCases := []struct {
		hasLimit bool
		limit    int
		expected int
	}{
		{false, 0, expectedEvents},
		{true, 0, expectedEvents},
		{true, -1, expectedEvents},
		{true, 1, 1},
	}

	for _, tc := range testCases {
		url := "rangelog"
		if tc.hasLimit {
			url += fmt.Sprintf("?limit=%d", tc.limit)
		}
		t.Run(url, func(t *testing.T) {
			var resp serverpb.RangeLogResponse
			if err := getAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}
			events := resp.Events
			if e, a := tc.expected, len(events); e != a {
				var sb strings.Builder
				for _, ev := range events {
					sb.WriteString(ev.String() + "\n")
				}
				t.Fatalf("expected %d events, got %d:\n%s", e, a, sb.String())
			}
		})
	}
}

func TestAdminAPIDataDistribution(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	// Create some tables.
	sqlDB.Exec(t, `CREATE DATABASE roachblog`)
	sqlDB.Exec(t, `CREATE TABLE roachblog.posts (id INT PRIMARY KEY, title text, body text)`)
	sqlDB.Exec(t, `CREATE TABLE roachblog.comments (
		id INT PRIMARY KEY,
		post_id INT REFERENCES roachblog.posts,
		body text
	)`)
	// Test special characters in DB and table names.
	sqlDB.Exec(t, `CREATE DATABASE "sp'ec\ch""ars"`)
	sqlDB.Exec(t, `CREATE TABLE "sp'ec\ch""ars"."more\spec'chars" (id INT PRIMARY KEY)`)

	// Verify that we see their replicas in the DataDistribution response, evenly spread
	// across the test cluster's three nodes.

	expectedDatabaseInfo := map[string]serverpb.DataDistributionResponse_DatabaseInfo{
		"roachblog": {
			TableInfo: map[string]serverpb.DataDistributionResponse_TableInfo{
				"posts": {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
				"comments": {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
			},
		},
		`sp'ec\ch"ars`: {
			TableInfo: map[string]serverpb.DataDistributionResponse_TableInfo{
				`more\spec'chars`: {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
			},
		},
	}

	// Wait for the new tables' ranges to be created and replicated.
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.DataDistributionResponse
		if err := getAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
			t.Fatal(err)
		}

		delete(resp.DatabaseInfo, "system") // delete results for system database.
		if !reflect.DeepEqual(resp.DatabaseInfo, expectedDatabaseInfo) {
			return fmt.Errorf("expected %v; got %v", expectedDatabaseInfo, resp.DatabaseInfo)
		}

		// Don't test anything about the zone configs for now; just verify that something is there.
		if len(resp.ZoneConfigs) == 0 {
			return fmt.Errorf("no zone configs returned")
		}

		return nil
	})

	// Verify that the request still works after a table has been dropped,
	// and that dropped_at is set on the dropped table.
	sqlDB.Exec(t, `DROP TABLE roachblog.comments`)

	var resp serverpb.DataDistributionResponse
	if err := getAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
		t.Fatal(err)
	}

	if resp.DatabaseInfo["roachblog"].TableInfo["comments"].DroppedAt == nil {
		t.Fatal("expected roachblog.comments to have dropped_at set but it's nil")
	}

	// Verify that the request still works after a database has been dropped.
	sqlDB.Exec(t, `DROP DATABASE roachblog CASCADE`)

	if err := getAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkAdminAPIDataDistribution(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	testCluster := serverutils.StartTestCluster(b, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	sqlDB.Exec(b, `CREATE DATABASE roachblog`)

	// Create a bunch of tables.
	for i := 0; i < 200; i++ {
		sqlDB.Exec(
			b,
			fmt.Sprintf(`CREATE TABLE roachblog.t%d (id INT PRIMARY KEY, title text, body text)`, i),
		)
		// TODO(vilterp): split to increase the number of ranges for each table
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var resp serverpb.DataDistributionResponse
		if err := getAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func TestEnqueueRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer testCluster.Stopper().Stop(context.Background())

	// Up-replicate r1 to all 3 nodes. We use manual replication to avoid lease
	// transfers causing temporary conditions in which no store is the
	// leaseholder, which can break the the tests below.
	_, err := testCluster.AddReplicas(roachpb.KeyMin, testCluster.Target(1), testCluster.Target(2))
	if err != nil {
		t.Fatal(err)
	}

	// RangeID being queued
	const realRangeID = 1
	const fakeRangeID = 999

	// Who we expect responses from.
	const none = 0
	const leaseholder = 1
	const allReplicas = 3

	testCases := []struct {
		nodeID            roachpb.NodeID
		queue             string
		rangeID           roachpb.RangeID
		expectedDetails   int
		expectedNonErrors int
	}{
		// Success cases
		{0, "gc", realRangeID, allReplicas, leaseholder},
		{0, "split", realRangeID, allReplicas, leaseholder},
		{0, "replicaGC", realRangeID, allReplicas, allReplicas},
		{0, "RaFtLoG", realRangeID, allReplicas, allReplicas},
		{0, "RAFTSNAPSHOT", realRangeID, allReplicas, allReplicas},
		{0, "consistencyChecker", realRangeID, allReplicas, leaseholder},
		{0, "TIMESERIESmaintenance", realRangeID, allReplicas, leaseholder},
		{1, "raftlog", realRangeID, leaseholder, leaseholder},
		{2, "raftlog", realRangeID, leaseholder, 1},
		{3, "raftlog", realRangeID, leaseholder, 1},
		// Error cases
		{0, "gv", realRangeID, allReplicas, none},
		{0, "GC", fakeRangeID, allReplicas, none},
	}

	for _, tc := range testCases {
		t.Run(tc.queue, func(t *testing.T) {
			req := &serverpb.EnqueueRangeRequest{
				NodeID:  tc.nodeID,
				Queue:   tc.queue,
				RangeID: tc.rangeID,
			}
			var resp serverpb.EnqueueRangeResponse
			if err := postAdminJSONProto(testCluster.Server(0), "enqueue_range", req, &resp); err != nil {
				t.Fatal(err)
			}
			if e, a := tc.expectedDetails, len(resp.Details); e != a {
				t.Errorf("expected %d details; got %d: %+v", e, a, resp)
			}
			var numNonErrors int
			for _, details := range resp.Details {
				if len(details.Events) > 0 && details.Error == "" {
					numNonErrors++
				}
			}
			if tc.expectedNonErrors != numNonErrors {
				t.Errorf("expected %d non-error details; got %d: %+v", tc.expectedNonErrors, numNonErrors, resp)
			}
		})
	}

	// Finally, test a few more basic error cases.
	reqs := []*serverpb.EnqueueRangeRequest{
		{NodeID: -1, Queue: "gc"},
		{Queue: ""},
		{RangeID: -1, Queue: "gc"},
	}
	for _, req := range reqs {
		t.Run(fmt.Sprint(req), func(t *testing.T) {
			var resp serverpb.EnqueueRangeResponse
			err := postAdminJSONProto(testCluster.Server(0), "enqueue_range", req, &resp)
			if err == nil {
				t.Fatalf("unexpected success: %+v", resp)
			}
			if !testutils.IsError(err, "400 Bad Request") {
				t.Fatalf("unexpected error type: %+v", err)
			}
		})
	}
}

func TestStatsforSpanOnLocalMax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())
	firstServer := testCluster.Server(0)
	adminServer := firstServer.(*TestServer).Server.admin

	underTest := roachpb.Span{
		Key:    keys.LocalMax,
		EndKey: keys.SystemPrefix,
	}

	_, err := adminServer.statsForSpan(context.Background(), underTest)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
	})
	defer s.Stopper().Stop(context.Background())
	reqs := []serverpb.StoreStateRequest{{StoreID: 1, State: roachpb.ENABLE},
		{StoreID: 1, State: roachpb.DISABLE},
		{StoreID: 1, State: roachpb.ENABLE},
		{StoreID: 1, State: roachpb.DISABLE},
		{StoreID: 2, State: roachpb.DISABLE},
		{StoreID: 2, State: roachpb.ENABLE},
		{StoreID: 2, State: roachpb.DISABLE},
		{StoreID: 2, State: roachpb.ENABLE},
	}
	expectResponse := []serverpb.StoreStateResponse{{Result: serverpb.Success, Original: roachpb.ENABLE, New: roachpb.ENABLE},
		{Result: serverpb.Success, Original: roachpb.ENABLE, New: roachpb.DISABLE},
		{Result: serverpb.Success, Original: roachpb.DISABLE, New: roachpb.ENABLE},
		{Result: serverpb.Success, Original: roachpb.ENABLE, New: roachpb.DISABLE},
		{Result: serverpb.Fail, Original: roachpb.UNKNOWN, New: roachpb.UNKNOWN},
		{Result: serverpb.Fail, Original: roachpb.UNKNOWN, New: roachpb.UNKNOWN},
		{Result: serverpb.Fail, Original: roachpb.UNKNOWN, New: roachpb.UNKNOWN},
		{Result: serverpb.Fail, Original: roachpb.UNKNOWN, New: roachpb.UNKNOWN},
	}
	expectErr := status.Errorf(codes.InvalidArgument, "The store:store id=%d is not in the current host", reqs[4].StoreID)
	ts := s.(*TestServer)
	for i, req := range reqs {
		if response, err := ts.admin.StoreState(ctx, &req); err == nil {
			if response.Result != expectResponse[i].Result && response.New != expectResponse[i].New && response.Original != expectResponse[i].Original {
				t.Fatalf("The result of response :%s is not expected :%s ;or The original of response :%s is not expected :%s ;or The original of response :%s is not expected :%s",
					response.Result, expectResponse[i], response.Original, expectResponse[i].Original, response.New, expectResponse[i].New)
			}
		} else {
			if !reflect.DeepEqual(err, expectErr) {
				t.Fatalf("The err:%s is not expected:%s", err, expectErr)
			}
		}
	}
}

func TestAdminLoadData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("需要手工调整adminServer.RawBatchLoad代码才能执行")
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	// := s.(*TestServer)

	ac := log.AmbientContext{Tracer: s.ClusterSettings().Tracer}
	_, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	// Test databases endpoint.
	query := `CREATE TABLE lineitem (
		l_orderkey      INT8 NOT NULL,
		l_partkey       INT8 NOT NULL,
		l_suppkey       INT8 NOT NULL,
		l_linenumber    INT8 NOT NULL,
		l_quantity      DECIMAL(15,2) NOT NULL,
		l_extendedprice DECIMAL(15,2) NOT NULL,
		l_discount      DECIMAL(15,2) NOT NULL,
		l_tax           DECIMAL(15,2) NOT NULL,
		l_returnflag    CHAR(1) NOT NULL,
		l_linestatus    CHAR(1) NOT NULL,
		l_shipdate      DATE NOT NULL,
		l_commitdate    DATE NOT NULL,
		l_receiptdate   DATE NOT NULL,
		l_shipinstruct  CHAR(25) NOT NULL,
		l_shipmode      CHAR(10) NOT NULL,
		l_comment       VARCHAR(44) NOT NULL,
		PRIMARY KEY     (l_orderkey, l_linenumber),
		INDEX l_ok      (l_orderkey ASC),
		INDEX l_pk      (l_partkey ASC),
		INDEX l_sk      (l_suppkey ASC),
		INDEX l_sd      (l_shipdate ASC),
		INDEX l_cd      (l_commitdate ASC),
		INDEX l_rd      (l_receiptdate ASC),
		INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
		INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
	)`
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	tpchLineItemDataRows := [][]string{
		{"1", "155190", "7706", "1", "17", "21168.23", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"},
		{"1", "67310", "7311", "2", "36", "45983.16", "0.09", "0.06", "N", "O", "1996-04-12", "1996-02-28", "1996-04-20", "TAKE BACK RETURN", "MAIL", "ly final dependencies: slyly bold "},
		{"1", "63700", "3701", "3", "8", "13309.60", "0.10", "0.02", "N", "O", "1996-01-29", "1996-03-05", "1996-01-31", "TAKE BACK RETURN", "REG AIR", "riously. regular, express dep"},
		{"1", "2132", "4633", "4", "28", "28955.64", "0.09", "0.06", "N", "O", "1996-04-21", "1996-03-30", "1996-05-16", "NONE", "AIR", "lites. fluffily even de"},
		{"1", "24027", "1534", "5", "24", "22824.48", "0.10", "0.04", "N", "O", "1996-03-30", "1996-03-14", "1996-04-01", "NONE", "FOB", " pending foxes. slyly re"},
		{"1", "15635", "638", "6", "32", "49620.16", "0.07", "0.02", "N", "O", "1996-01-30", "1996-02-07", "1996-02-03", "DELIVER IN PERSON", "MAIL", "arefully slyly ex"},
		{"2", "106170", "1191", "1", "38", "44694.46", "0.00", "0.05", "N", "O", "1997-01-28", "1997-01-14", "1997-02-02", "TAKE BACK RETURN", "RAIL", "ven requests. deposits breach a"},
		{"3", "4297", "1798", "1", "45", "54058.05", "0.06", "0.00", "R", "F", "1994-02-02", "1994-01-04", "1994-02-23", "NONE", "AIR", "ongside of the furiously brave acco"},
		{"3", "19036", "6540", "2", "49", "46796.47", "0.10", "0.00", "R", "F", "1993-11-09", "1993-12-20", "1993-11-24", "TAKE BACK RETURN", "RAIL", " unusual accounts. eve"},
		{"3", "128449", "3474", "3", "27", "39890.88", "0.06", "0.07", "A", "F", "1994-01-16", "1993-11-22", "1994-01-23", "DELIVER IN PERSON", "SHIP", "nal foxes wake."},
	}

	var req serverpb.RawBatchLoadRequest

	var sep = "~@~"
	for i := 0; i < len(tpchLineItemDataRows); i++ {
		var row string
		for n, v := range tpchLineItemDataRows[i] {
			if n == len(tpchLineItemDataRows[i])-1 {
				row += v
			} else {
				row += v + sep
			}
		}

		req.Values = append(req.Values, row)
	}

	req.Sep = sep
	req.Database = "defaultdb"
	req.Table = "lineitem"
	req.NullEncoding = "\\N"

	var resp serverpb.RawBatchLoadResponse

	if err := postAdminJSONProto(s, "table/rawbatchload", &req, &resp); err != nil {
		t.Fatal(err)
	}

	rows, _ := db.Query("select count(1) from lineitem")
	for rows.Next() {
		r := 0
		_ = rows.Scan(&r)
		fmt.Println("lineitem nums:" + strconv.Itoa(r))
	}

	fmt.Println(resp.PutNums)
	fmt.Println(resp.ErrorMessage)

}
