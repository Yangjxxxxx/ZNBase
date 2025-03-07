// Copyright 2015  The Cockroach Authors.
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
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/build"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/securitytest"
	"github.com/znbasedb/znbase/pkg/server/diagnosticspb"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/server/status/statuspb"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/ts"
	"github.com/znbasedb/znbase/pkg/util/httputil"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

func getStatusJSONProto(
	ts serverutils.TestServerInterface, path string, response protoutil.Message,
) error {
	return serverutils.GetJSONProto(ts, statusPrefix+path, response)
}

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/stacks/local endpoint.
func TestStatusLocalStacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Verify match with at least two goroutine stacks.
	re := regexp.MustCompile("(?s)goroutine [0-9]+.*goroutine [0-9]+.*")

	var stacks serverpb.JSONResponse
	for _, nodeID := range []string{"local", "1"} {
		if err := getStatusJSONProto(s, "stacks/"+nodeID, &stacks); err != nil {
			t.Fatal(err)
		}
		if !re.Match(stacks.Data) {
			t.Errorf("expected %s to match %s", stacks.Data, re)
		}
	}
}

// TestStatusJson verifies that status endpoints return expected Json results.
// The content type of the responses is always httputil.JSONContentType.
func TestStatusJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	nodeID := ts.Gossip().NodeID.Get()
	addr, err := ts.Gossip().GetNodeIDAddress(nodeID)
	if err != nil {
		t.Fatal(err)
	}

	var nodes serverpb.NodesResponse
	testutils.SucceedsSoon(t, func() error {
		if err := getStatusJSONProto(s, "nodes", &nodes); err != nil {
			t.Fatal(err)
		}

		if len(nodes.Nodes) == 0 {
			return errors.Errorf("expected non-empty node list, got: %v", nodes)
		}
		return nil
	})

	for _, path := range []string{
		"/health",
		statusPrefix + "details/local",
		statusPrefix + "details/" + strconv.FormatUint(uint64(nodeID), 10),
	} {
		var details serverpb.DetailsResponse
		if err := serverutils.GetJSONProto(s, path, &details); err != nil {
			t.Fatal(err)
		}
		if a, e := details.NodeID, nodeID; a != e {
			t.Errorf("expected: %d, got: %d", e, a)
		}
		if a, e := details.Address, *addr; a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
		if a, e := details.BuildInfo, build.GetInfo(); a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
	}
}

// TestStatusGossipJson ensures that the output response for the full gossip
// info contains the required fields.
func TestStatusGossipJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	var data gossip.InfoStatus
	if err := getStatusJSONProto(s, "gossip/local", &data); err != nil {
		t.Fatal(err)
	}
	if _, ok := data.Infos["first-range"]; !ok {
		t.Errorf("no first-range info returned: %v", data)
	}
	if _, ok := data.Infos["cluster-id"]; !ok {
		t.Errorf("no clusterID info returned: %v", data)
	}
	if _, ok := data.Infos["node:1"]; !ok {
		t.Errorf("no node 1 info returned: %v", data)
	}
	if _, ok := data.Infos["system-db"]; !ok {
		t.Errorf("no system config info returned: %v", data)
	}
}

// TestStatusEngineStatsJson ensures that the output response for the engine
// stats contains the required fields.
func TestStatusEngineStatsJson(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	s, err := serverutils.StartServerRaw(base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.TODO())

	var engineStats serverpb.EngineStatsResponse
	if err := getStatusJSONProto(s, "enginestats/local", &engineStats); err != nil {
		t.Fatal(err)
	}
	if len(engineStats.Stats) != 1 {
		t.Fatal(errors.Errorf("expected one engine stats, got: %v", engineStats))
	}

	tickers := engineStats.Stats[0].TickersAndHistograms.Tickers
	if len(tickers) == 0 {
		t.Fatal(errors.Errorf("expected non-empty tickers list, got: %v", tickers))
	}
	allTickersZero := true
	for _, ticker := range tickers {
		if ticker != 0 {
			allTickersZero = false
		}
	}
	if allTickersZero {
		t.Fatal(errors.Errorf("expected some tickers nonzero, got: %v", tickers))
	}

	histograms := engineStats.Stats[0].TickersAndHistograms.Histograms
	if len(histograms) == 0 {
		t.Fatal(errors.Errorf("expected non-empty histograms list, got: %v", histograms))
	}
	allHistogramsZero := true
	for _, histogram := range histograms {
		if histogram.Max == 0 {
			allHistogramsZero = false
		}
	}
	if allHistogramsZero {
		t.Fatal(errors.Errorf("expected some histograms nonzero, got: %v", histograms))
	}
}

// startServer will start a server with a short scan interval, wait for
// the scan to complete, and return the server. The caller is
// responsible for stopping the server.
func startServer(t *testing.T) *TestServer {
	tsI, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
		},
	})

	ts := tsI.(*TestServer)

	// Make sure the range is spun up with an arbitrary read command. We do not
	// expect a specific response.
	if _, err := kvDB.Get(context.TODO(), "a"); err != nil {
		t.Fatal(err)
	}

	// Make sure the node status is available. This is done by forcing stores to
	// publish their status, synchronizing to the event feed with a canary
	// event, and then forcing the server to write summaries immediately.
	if err := ts.node.computePeriodicMetrics(context.TODO(), 0); err != nil {
		t.Fatalf("error publishing store statuses: %s", err)
	}

	if err := ts.WriteSummaries(); err != nil {
		t.Fatalf("error writing summaries: %s", err)
	}

	return ts
}

// TestStatusLocalFileRetrieval tests the files/local endpoint.
// See debug/heap roachtest for testing heap profile file collection.
func TestStatusLocalFileRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.TODO())

	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: ts.ClusterSettings().Tracer}, rootConfig, ts.Clock(), ts.Stopper(),
		&ts.ClusterSettings().Version)
	url := ts.ServingAddr()
	conn, err := rpcContext.GRPCDialNode(url, ts.NodeID(), rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	request := serverpb.GetFilesRequest{
		NodeId: "local", ListOnly: true, Type: serverpb.FileType_HEAP, Patterns: []string{"*"}}
	response, err := client.GetFiles(context.Background(), &request)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := len(response.Files), 0; a != e {
		t.Errorf("expected %d files(s), found %d", e, a)
	}

	// Testing path separators in pattern.
	request = serverpb.GetFilesRequest{NodeId: "local", ListOnly: true,
		Type: serverpb.FileType_HEAP, Patterns: []string{"pattern/with/separators"}}
	_, err = client.GetFiles(context.Background(), &request)
	if err == nil {
		t.Errorf("GetFiles: path separators allowed in pattern")
	}

	// Testing invalid filetypes.
	request = serverpb.GetFilesRequest{NodeId: "local", ListOnly: true,
		Type: -1, Patterns: []string{"*"}}
	_, err = client.GetFiles(context.Background(), &request)
	if err == nil {
		t.Errorf("GetFiles: invalid file type allowed")
	}
}

// TestStatusLocalLogs checks to ensure that local/logfiles,
// local/logfiles/{filename} and local/log function
// correctly.
func TestStatusLocalLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if log.V(3) {
		t.Skip("Test only works with low verbosity levels")
	}

	s := log.ScopeWithoutShowLogs(t)
	defer s.Close(t)

	ts := startServer(t)
	defer ts.Stopper().Stop(context.TODO())

	// Log an error of each main type which we expect to be able to retrieve.
	// The resolution of our log timestamps is such that it's possible to get
	// two subsequent log messages with the same timestamp. This test will fail
	// when that occurs. By adding a small sleep in here after each timestamp to
	// ensures this isn't the case and that the log filtering doesn't filter out
	// the log entires we're looking for. The value of 20 μs was chosen because
	// the log timestamps have a fidelity of 10 μs and thus doubling that should
	// be a sufficient buffer.
	// See util/log/clog.go formatHeader() for more details.
	const sleepBuffer = time.Microsecond * 20
	timestamp := timeutil.Now().UnixNano()
	time.Sleep(sleepBuffer)
	log.Errorf(context.Background(), "TestStatusLocalLogFile test message-Error")
	time.Sleep(sleepBuffer)
	timestampE := timeutil.Now().UnixNano()
	time.Sleep(sleepBuffer)
	log.Warningf(context.Background(), "TestStatusLocalLogFile test message-Warning")
	time.Sleep(sleepBuffer)
	timestampEW := timeutil.Now().UnixNano()
	time.Sleep(sleepBuffer)
	log.Infof(context.Background(), "TestStatusLocalLogFile test message-Info")
	time.Sleep(sleepBuffer)
	timestampEWI := timeutil.Now().UnixNano()

	var wrapper serverpb.LogFilesListResponse
	if err := getStatusJSONProto(ts, "logfiles/local", &wrapper); err != nil {
		t.Fatal(err)
	}
	if a, e := len(wrapper.Files), 1; a != e {
		t.Fatalf("expected %d log files; got %d", e, a)
	}

	// Check each individual log can be fetched and is non-empty.
	var foundInfo, foundWarning, foundError bool
	for _, file := range wrapper.Files {
		var wrapper serverpb.LogEntriesResponse
		if err := getStatusJSONProto(ts, "logfiles/local/"+file.Name, &wrapper); err != nil {
			t.Fatal(err)
		}
		for _, entry := range wrapper.Entries {
			switch entry.Message {
			case "TestStatusLocalLogFile test message-Error":
				foundError = true
			case "TestStatusLocalLogFile test message-Warning":
				foundWarning = true
			case "TestStatusLocalLogFile test message-Info":
				foundInfo = true
			}
		}
	}

	if !(foundInfo && foundWarning && foundError) {
		t.Errorf("expected to find test messages in %v", wrapper.Files)
	}

	type levelPresence struct {
		Error, Warning, Info bool
	}

	testCases := []struct {
		MaxEntities    int
		StartTimestamp int64
		EndTimestamp   int64
		Pattern        string
		levelPresence
	}{
		// Test filtering by log severity.
		// // Test entry limit. Ignore Info/Warning/Error filters.
		{1, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		{2, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		{3, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		// Test filtering in different timestamp windows.
		{0, timestamp, timestamp, "", levelPresence{false, false, false}},
		{0, timestamp, timestampE, "", levelPresence{true, false, false}},
		{0, timestampE, timestampEW, "", levelPresence{false, true, false}},
		{0, timestampEW, timestampEWI, "", levelPresence{false, false, true}},
		{0, timestamp, timestampEW, "", levelPresence{true, true, false}},
		{0, timestampE, timestampEWI, "", levelPresence{false, true, true}},
		{0, timestamp, timestampEWI, "", levelPresence{true, true, true}},
		// Test filtering by regexp pattern.
		{0, 0, 0, "Info", levelPresence{false, false, true}},
		{0, 0, 0, "Warning", levelPresence{false, true, false}},
		{0, 0, 0, "Error", levelPresence{true, false, false}},
		{0, 0, 0, "Info|Error|Warning", levelPresence{true, true, true}},
		{0, 0, 0, "Nothing", levelPresence{false, false, false}},
	}

	for i, testCase := range testCases {
		var url bytes.Buffer
		fmt.Fprintf(&url, "logs/local?level=")
		if testCase.MaxEntities > 0 {
			fmt.Fprintf(&url, "&max=%d", testCase.MaxEntities)
		}
		if testCase.StartTimestamp > 0 {
			fmt.Fprintf(&url, "&start_time=%d", testCase.StartTimestamp)
		}
		if testCase.StartTimestamp > 0 {
			fmt.Fprintf(&url, "&end_time=%d", testCase.EndTimestamp)
		}
		if len(testCase.Pattern) > 0 {
			fmt.Fprintf(&url, "&pattern=%s", testCase.Pattern)
		}

		var wrapper serverpb.LogEntriesResponse
		path := url.String()
		if err := getStatusJSONProto(ts, path, &wrapper); err != nil {
			t.Fatal(err)
		}

		if testCase.MaxEntities > 0 {
			if a, e := len(wrapper.Entries), testCase.MaxEntities; a != e {
				t.Errorf("%d expected %d entries, got %d: \n%+v", i, e, a, wrapper.Entries)
			}
		} else {
			var actual levelPresence
			var logsBuf bytes.Buffer
			for _, entry := range wrapper.Entries {
				fmt.Fprintln(&logsBuf, entry.Message)

				switch entry.Message {
				case "TestStatusLocalLogFile test message-Error":
					actual.Error = true
				case "TestStatusLocalLogFile test message-Warning":
					actual.Warning = true
				case "TestStatusLocalLogFile test message-Info":
					actual.Info = true
				}
			}

			if testCase.levelPresence != actual {
				t.Errorf("%d: expected %+v at %s, got:\n%s", i, testCase, path, logsBuf.String())
			}
		}
	}
}

// TestNodeStatusResponse verifies that node status returns the expected
// results.
func TestNodeStatusResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := startServer(t)
	defer s.Stopper().Stop(context.TODO())

	// First fetch all the node statuses.
	wrapper := serverpb.NodesResponse{}
	if err := getStatusJSONProto(s, "nodes", &wrapper); err != nil {
		t.Fatal(err)
	}
	nodeStatuses := wrapper.Nodes

	if len(nodeStatuses) != 1 {
		t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
	}
	if !proto.Equal(&s.node.Descriptor, &nodeStatuses[0].Desc) {
		t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", s.node.Descriptor, nodeStatuses[0].Desc)
	}

	// Now fetch each one individually. Loop through the nodeStatuses to use the
	// ids only.
	for _, oldNodeStatus := range nodeStatuses {
		nodeStatus := statuspb.NodeStatus{}
		if err := getStatusJSONProto(s, "nodes/"+oldNodeStatus.Desc.NodeID.String(), &nodeStatus); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&s.node.Descriptor, &nodeStatus.Desc) {
			t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", s.node.Descriptor, nodeStatus.Desc)
		}
	}
}

// TestMetricsRecording verifies that Node statistics are periodically recorded
// as time series data.
func TestMetricsRecording(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Verify that metrics for the current timestamp are recorded. This should
	// be true very quickly even though DefaultMetricsSampleInterval is large,
	// because the server writes an entry eagerly on startup.
	testutils.SucceedsSoon(t, func() error {
		now := s.Clock().PhysicalNow()

		var data roachpb.InternalTimeSeriesData
		for _, keyName := range []string{
			"cr.store.livebytes.1",
			"cr.node.sys.go.allocbytes.1",
		} {
			key := ts.MakeDataKey(keyName, "", ts.Resolution10s, now)
			if err := kvDB.GetProto(ctx, key, &data); err != nil {
				return err
			}
		}
		return nil
	})
}

// TestMetricsEndpoint retrieves the metrics endpoint, which is currently only
// used for development purposes. The metrics within the response are verified
// in other tests.
func TestMetricsEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := startServer(t)
	defer s.Stopper().Stop(context.TODO())

	if _, err := getText(s, s.AdminURL()+statusPrefix+"metrics/"+s.Gossip().NodeID.String()); err != nil {
		t.Fatal(err)
	}
}

// TestMetricsMetadata ensures that the server's recorder return metrics and
// that each metric has a Name, Help, Unit, and DisplayUnit defined.
func TestMetricsMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := startServer(t)
	defer s.Stopper().Stop(context.TODO())

	metricsMetadata := s.recorder.GetMetricsMetadata()

	if len(metricsMetadata) < 200 {
		t.Fatal("s.recorder.GetMetricsMetadata() failed sanity check; didn't return enough metrics.")
	}

	for _, v := range metricsMetadata {
		if v.Name == "" {
			t.Fatal("metric missing name.")
		}
		if v.Help == "" {
			t.Fatalf("%s missing Help.", v.Name)
		}
		if v.Measurement == "" {
			t.Fatalf("%s missing Measurement.", v.Name)
		}
		if v.Unit == 0 {
			t.Fatalf("%s missing Unit.", v.Name)
		}
	}
}

func TestHotRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.TODO())

	var hotRangesResp serverpb.HotRangesResponse
	if err := getStatusJSONProto(ts, "hotranges", &hotRangesResp); err != nil {
		t.Fatal(err)
	}
	if len(hotRangesResp.HotRangesByNodeID) == 0 {
		t.Fatalf("didn't get hot range responses from any nodes")
	}

	for nodeID, nodeResp := range hotRangesResp.HotRangesByNodeID {
		if len(nodeResp.Stores) == 0 {
			t.Errorf("didn't get any stores in hot range response from n%d: %v",
				nodeID, nodeResp.ErrorMessage)
		}
		for _, storeResp := range nodeResp.Stores {
			// Only the first store will actually have any ranges on it.
			if storeResp.StoreID != roachpb.StoreID(1) {
				continue
			}
			lastQPS := math.MaxFloat64
			if len(storeResp.HotRanges) == 0 {
				t.Errorf("didn't get any hot ranges in response from n%d,s%d: %v",
					nodeID, storeResp.StoreID, nodeResp.ErrorMessage)
			}
			for _, r := range storeResp.HotRanges {
				if r.Desc.RangeID == 0 || (len(r.Desc.StartKey) == 0 && len(r.Desc.EndKey) == 0) {
					t.Errorf("unexpected empty/unpopulated range descriptor: %+v", r.Desc)
				}
				if r.QueriesPerSecond > lastQPS {
					t.Errorf("unexpected increase in qps between ranges; prev=%.2f, current=%.2f, desc=%v",
						lastQPS, r.QueriesPerSecond, r.Desc)
				}
				lastQPS = r.QueriesPerSecond
			}
		}
	}
}

func TestRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer storage.EnableLeaseHistory(100)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.TODO())

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := ts.db.Scan(context.TODO(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

	var response serverpb.RangesResponse
	if err := getStatusJSONProto(ts, "ranges/local", &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}
	for _, ri := range response.Ranges {
		// Do some simple validation based on the fact that this is a
		// single-node cluster.
		if ri.RaftState.State != "StateLeader" && ri.RaftState.State != raftStateDormant {
			t.Errorf("expected to be Raft leader or dormant, but was '%s'", ri.RaftState.State)
		}
		expReplica := roachpb.ReplicaDescriptor{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		}
		if len(ri.State.Desc.Replicas) != 1 || ri.State.Desc.Replicas[0] != expReplica {
			t.Errorf("unexpected replica list %+v", ri.State.Desc.Replicas)
		}
		if ri.State.Lease == nil || *ri.State.Lease == (roachpb.Lease{}) {
			t.Error("expected a nontrivial Lease")
		}
		if ri.State.LastIndex == 0 {
			t.Error("expected positive LastIndex")
		}
		if len(ri.LeaseHistory) == 0 {
			t.Error("expected at least one lease history entry")
		}
	}
}

func TestRaftDebug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := startServer(t)
	defer s.Stopper().Stop(context.TODO())

	var resp serverpb.RaftDebugResponse
	if err := getStatusJSONProto(s, "raft", &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}

	if len(resp.Ranges) < 3 {
		t.Errorf("expected more than 2 ranges, got %d", len(resp.Ranges))
	}

	reqURI := "raft"
	requestedIDs := []roachpb.RangeID{}
	for id := range resp.Ranges {
		if len(requestedIDs) == 0 {
			reqURI += "?"
		} else {
			reqURI += "&"
		}
		reqURI += fmt.Sprintf("range_ids=%d", id)
		requestedIDs = append(requestedIDs, id)
		if len(requestedIDs) >= 2 {
			break
		}
	}

	if err := getStatusJSONProto(s, reqURI, &resp); err != nil {
		t.Fatal(err)
	}

	// Make sure we get exactly two ranges back.
	if len(resp.Ranges) != 2 {
		t.Errorf("expected exactly two ranges in response, got %d", len(resp.Ranges))
	}

	// Make sure the ranges returned are those requested.
	for _, reqID := range requestedIDs {
		if _, ok := resp.Ranges[reqID]; !ok {
			t.Errorf("request URI was %s, but range ID %d not returned: %+v", reqURI, reqID, resp.Ranges)
		}
	}
}

// TestStatusVars verifies that prometheus metrics are available via the
// /_status/vars endpoint.
func TestStatusVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	if body, err := getText(s, s.AdminURL()+statusPrefix+"vars"); err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("# TYPE sql_bytesout counter\nsql_bytesout")) {
		t.Errorf("expected sql_bytesout, got: %s", body)
	}
}

func TestSpanStatsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.TODO())

	httpClient, err := ts.GetAuthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	var response serverpb.SpanStatsResponse
	request := serverpb.SpanStatsRequest{
		NodeID:   "1",
		StartKey: []byte(roachpb.RKeyMin),
		EndKey:   []byte(roachpb.RKeyMax),
	}

	url := ts.AdminURL() + statusPrefix + "span"
	initialRanges, err := ts.ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := httputil.PostJSON(httpClient, url, &request, &response); err != nil {
			return err
		}
		if a, e := int(response.RangeCount), initialRanges; a != e {
			return errors.Errorf("expected %d ranges, found %d", e, a)
		}
		return nil
	})
}

func TestSpanStatsGRPCResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	ts := startServer(t)
	defer ts.Stopper().Stop(ctx)

	rpcStopper := stop.NewStopper()
	defer rpcStopper.Stop(ctx)
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: ts.ClusterSettings().Tracer}, ts.RPCContext().Config, ts.Clock(),
		rpcStopper, &ts.ClusterSettings().Version)
	request := serverpb.SpanStatsRequest{
		NodeID:   "1",
		StartKey: []byte(roachpb.RKeyMin),
		EndKey:   []byte(roachpb.RKeyMax),
	}

	url := ts.ServingAddr()
	conn, err := rpcContext.GRPCDialNode(url, ts.NodeID(), rpc.DefaultClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	response, err := client.SpanStats(ctx, &request)
	if err != nil {
		t.Fatal(err)
	}
	initialRanges, err := ts.ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}
	if a, e := int(response.RangeCount), initialRanges; a != e {
		t.Fatalf("expected %d ranges, found %d", e, a)
	}
}

func TestNodesGRPCResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.TODO())

	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: ts.ClusterSettings().Tracer}, rootConfig, ts.Clock(), ts.Stopper(),
		&ts.ClusterSettings().Version)
	var request serverpb.NodesRequest

	url := ts.ServingAddr()
	conn, err := rpcContext.GRPCDialNode(url, ts.NodeID(), rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	response, err := client.Nodes(context.Background(), &request)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := len(response.Nodes), 1; a != e {
		t.Errorf("expected %d node(s), found %d", e, a)
	}
}

func TestCertificatesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.TODO())

	var response serverpb.CertificatesResponse
	if err := getStatusJSONProto(ts, "certificates/local", &response); err != nil {
		t.Fatal(err)
	}

	// We expect 4 certificates: CA, node, and client certs for root, testuser.
	if a, e := len(response.Certificates), 4; a != e {
		t.Errorf("expected %d certificates, found %d", e, a)
	}

	// Read the certificates from the embedded assets.
	caPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	nodePath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeCert)

	caFile, err := securitytest.EmbeddedAssets.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}

	nodeFile, err := securitytest.EmbeddedAssets.ReadFile(nodePath)
	if err != nil {
		t.Fatal(err)
	}

	// The response is ordered: CA cert followed by node cert.
	cert := response.Certificates[0]
	if a, e := cert.Type, serverpb.CertificateDetails_CA; a != e {
		t.Errorf("wrong type %s, expected %s", a, e)
	} else if cert.ErrorMessage != "" {
		t.Errorf("expected cert without error, got %v", cert.ErrorMessage)
	} else if a, e := cert.Data, caFile; !bytes.Equal(a, e) {
		t.Errorf("mismatched contents: %s vs %s", a, e)
	}

	cert = response.Certificates[1]
	if a, e := cert.Type, serverpb.CertificateDetails_NODE; a != e {
		t.Errorf("wrong type %s, expected %s", a, e)
	} else if cert.ErrorMessage != "" {
		t.Errorf("expected cert without error, got %v", cert.ErrorMessage)
	} else if a, e := cert.Data, nodeFile; !bytes.Equal(a, e) {
		t.Errorf("mismatched contents: %s vs %s", a, e)
	}
}

func TestDiagnosticsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	var resp diagnosticspb.DiagnosticReport
	if err := getStatusJSONProto(s, "diagnostics/local", &resp); err != nil {
		t.Fatal(err)
	}

	// The endpoint just serializes result of getReportingInfo() which is already
	// tested elsewhere, so simply verify that we have a non-empty reply.
	if expected, actual := s.NodeID(), resp.Node.NodeID; expected != actual {
		t.Fatalf("expected %v got %v", expected, actual)
	}
}

func TestRangeResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer storage.EnableLeaseHistory(100)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.TODO())

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := ts.db.Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

	var response serverpb.RangeResponse
	if err := getStatusJSONProto(ts, "range/1", &response); err != nil {
		t.Fatal(err)
	}

	// This is a single node cluster, so only expect a single response.
	if e, a := 1, len(response.ResponsesByNodeID); e != a {
		t.Errorf("got the wrong number of responses, expected %d, actual %d", e, a)
	}

	node1Response := response.ResponsesByNodeID[response.NodeID]

	// The response should come back as valid.
	if !node1Response.Response {
		t.Errorf("node1's response returned as false, expected true")
	}

	// The response should include just the one range.
	if e, a := 1, len(node1Response.Infos); e != a {
		t.Errorf("got the wrong number of ranges in the response, expected %d, actual %d", e, a)
	}

	info := node1Response.Infos[0]
	expReplica := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}

	// Check some other values.
	if len(info.State.Desc.Replicas) != 1 || info.State.Desc.Replicas[0] != expReplica {
		t.Errorf("unexpected replica list %+v", info.State.Desc.Replicas)
	}

	if info.State.Lease == nil || *info.State.Lease == (roachpb.Lease{}) {
		t.Error("expected a nontrivial Lease")
	}

	if info.State.LastIndex == 0 {
		t.Error("expected positive LastIndex")
	}

	if len(info.LeaseHistory) == 0 {
		t.Error("expected at least one lease history entry")
	}
}

func TestRemoteDebugModeSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
		},
	})
	ts := s.(*TestServer)
	defer ts.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`SET CLUSTER SETTING server.remote_debugging.mode = 'off'`); err != nil {
		t.Fatal(err)
	}

	// Create a split so that there's some records in the system.rangelog table.
	// The test needs them.
	if _, err := db.Exec(
		`set experimental_force_split_at = true;
		create table t(x int primary key);
		alter table t split at values(1);`,
	); err != nil {
		t.Fatal(err)
	}

	// Verify that the remote debugging mode is respected for HTTP requests.
	// This needs to be wrapped in SucceedsSoon because settings changes have to
	// propagate through gossip and thus don't always take effect immediately.
	testutils.SucceedsSoon(t, func() error {
		for _, tc := range []struct {
			path     string
			response protoutil.Message
		}{
			{"gossip/local", &gossip.InfoStatus{}},
			{"allocator/node/local", &serverpb.AllocatorResponse{}},
			{"allocator/range/1", &serverpb.AllocatorResponse{}},
			{"logs/local", &serverpb.LogEntriesResponse{}},
			{"logfiles/local/znbase.log", &serverpb.LogEntriesResponse{}},
			{"local_sessions", &serverpb.ListSessionsResponse{}},
			{"sessions", &serverpb.ListSessionsResponse{}},
		} {
			err := getStatusJSONProto(ts, tc.path, tc.response)
			if !testutils.IsError(err, "403 Forbidden") {
				return fmt.Errorf("expected '403 Forbidden' error, but %q returned %+v: %v",
					tc.path, tc.response, err)
			}
		}
		return nil
	})

	// But not for grpc requests. The fact that the above gets an error but these
	// don't indicate that the grpc gateway is correctly adding the necessary
	// metadata for differentiating between the two (and that we're correctly
	// interpreting said metadata).
	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: ts.ClusterSettings().Tracer}, rootConfig, ts.Clock(), ts.Stopper(),
		&ts.ClusterSettings().Version)
	url := ts.ServingAddr()
	conn, err := rpcContext.GRPCDialNode(url, ts.NodeID(), rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)
	if _, err := client.Gossip(ctx, &serverpb.GossipRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.Allocator(ctx, &serverpb.AllocatorRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.Allocator(ctx, &serverpb.AllocatorRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.AllocatorRange(ctx, &serverpb.AllocatorRangeRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.Logs(ctx, &serverpb.LogsRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.ListLocalSessions(ctx, &serverpb.ListSessionsRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.ListSessions(ctx, &serverpb.ListSessionsRequest{}); err != nil {
		t.Error(err)
	}

	// Check that keys are properly omitted from the Ranges, HotRanges, and
	// RangeLog endpoints.
	var rangesResp serverpb.RangesResponse
	if err := getStatusJSONProto(ts, "ranges/local", &rangesResp); err != nil {
		t.Fatal(err)
	}
	if len(rangesResp.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}
	for _, ri := range rangesResp.Ranges {
		if ri.Span.StartKey != omittedKeyStr || ri.Span.EndKey != omittedKeyStr ||
			ri.State.ReplicaState.Desc.StartKey != nil || ri.State.ReplicaState.Desc.EndKey != nil {
			t.Errorf("unexpected key value found in RangeInfo: %+v", ri)
		}
	}

	var hotRangesResp serverpb.HotRangesResponse
	if err := getStatusJSONProto(ts, "hotranges", &hotRangesResp); err != nil {
		t.Fatal(err)
	}
	if len(hotRangesResp.HotRangesByNodeID) == 0 {
		t.Errorf("didn't get hot range responses from any nodes")
	}
	for nodeID, nodeResp := range hotRangesResp.HotRangesByNodeID {
		if len(nodeResp.Stores) == 0 {
			t.Errorf("didn't get any stores in hot range response from n%d: %v",
				nodeID, nodeResp.ErrorMessage)
		}
		for _, storeResp := range nodeResp.Stores {
			// Only the first store will actually have any ranges on it.
			if storeResp.StoreID != roachpb.StoreID(1) {
				continue
			}
			if len(storeResp.HotRanges) == 0 {
				t.Errorf("didn't get any hot ranges in response from n%d,s%d: %v",
					nodeID, storeResp.StoreID, nodeResp.ErrorMessage)
			}
			for _, r := range storeResp.HotRanges {
				if r.Desc.StartKey != nil || r.Desc.EndKey != nil {
					t.Errorf("unexpected key value found in hot ranges range descriptor: %+v", r.Desc)
				}
			}
		}
	}

	var rangelogResp serverpb.RangeLogResponse
	if err := getAdminJSONProto(ts, "rangelog", &rangelogResp); err != nil {
		t.Fatal(err)
	}
	if len(rangelogResp.Events) == 0 {
		t.Errorf("didn't get any Events")
	}
	for _, event := range rangelogResp.Events {
		if event.Event.Info.NewDesc != nil {
			if event.Event.Info.NewDesc.StartKey != nil || event.Event.Info.NewDesc.EndKey != nil ||
				event.Event.Info.UpdatedDesc.StartKey != nil || event.Event.Info.UpdatedDesc.EndKey != nil {
				t.Errorf("unexpected key value found in rangelog event: %+v", event)
			}
		}
		if strings.Contains(event.PrettyInfo.NewDesc, "Min-System") ||
			strings.Contains(event.PrettyInfo.UpdatedDesc, "Min-System") {
			t.Errorf("unexpected key value found in rangelog event info: %+v", event.PrettyInfo)
		}
	}
}

func TestStatusAPIStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0)
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))

	statements := []struct {
		stmt          string
		fingerprinted string
	}{
		{stmt: `CREATE DATABASE roachblog`},
		{stmt: `SET database = roachblog`},
		{stmt: `CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:          `INSERT INTO posts VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts VALUES (_, _)`,
		},
		{stmt: `SELECT * FROM posts`},
	}

	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt.stmt)
	}

	// Hit query endpoint.
	var resp serverpb.StatementsResponse
	if err := getStatusJSONProto(firstServerProto, "statements", &resp); err != nil {
		t.Fatal(err)
	}

	// See if the statements returned are what we executed.
	var expectedStatements []string
	for _, stmt := range statements {
		var expectedStmt = stmt.stmt
		if stmt.fingerprinted != "" {
			expectedStmt = stmt.fingerprinted
		}
		expectedStatements = append(expectedStatements, expectedStmt)
	}

	var statementsInResponse []string
	for _, respStatement := range resp.Statements {
		if respStatement.Key.KeyData.Failed {
			// We ignore failed statements here as the INSERT statement can fail and
			// be automatically retried, confusing the test success check.
			continue
		}
		if strings.HasPrefix(respStatement.Key.KeyData.App, sql.InternalAppNamePrefix) {
			// We ignore internal queries, these are not relevant for the
			// validity of this test.
			continue
		}
		statementsInResponse = append(statementsInResponse, respStatement.Key.KeyData.Query)
	}

	sort.Strings(expectedStatements)
	sort.Strings(statementsInResponse)

	if !reflect.DeepEqual(expectedStatements, statementsInResponse) {
		t.Fatalf("expected queries\n\n%v\n\ngot queries\n\n%v\n%s",
			expectedStatements, statementsInResponse, pretty.Sprint(resp))
	}
}

func TestListSessionsSecurity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ts := s.(*TestServer)
	defer ts.Stopper().Stop(context.TODO())

	// HTTP requests respect the authenticated username from the HTTP session.
	testCases := []struct {
		endpoint    string
		expectedErr string
	}{
		{"local_sessions", ""},
		{"sessions", ""},
		{fmt.Sprintf("local_sessions?username=%s", authenticatedUserName), ""},
		{fmt.Sprintf("sessions?username=%s", authenticatedUserName), ""},
		{"local_sessions?username=root", "does not have permission to view sessions from user"},
		{"sessions?username=root", "does not have permission to view sessions from user"},
	}
	for _, tc := range testCases {
		var response serverpb.ListSessionsResponse
		err := getStatusJSONProto(ts, tc.endpoint, &response)
		if tc.expectedErr == "" {
			if err != nil || len(response.Errors) > 0 {
				t.Errorf("unexpected failure listing sessions from %s; error: %v; response errors: %v",
					tc.endpoint, err, response.Errors)
			}
		} else {
			if !testutils.IsError(err, tc.expectedErr) &&
				!strings.Contains(response.Errors[0].Message, tc.expectedErr) {
				t.Errorf("did not get expected error %q when listing sessions from %s: %v",
					tc.expectedErr, tc.endpoint, err)
			}
		}
	}

	// gRPC requests behave as root and thus are always allowed.
	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: ts.ClusterSettings().Tracer}, rootConfig, ts.Clock(), ts.Stopper(),
		&ts.ClusterSettings().Version)
	url := ts.ServingAddr()
	conn, err := rpcContext.GRPCDialNode(url, ts.NodeID(), rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)
	ctx := context.Background()
	for _, user := range []string{"", authenticatedUserName, "root"} {
		request := &serverpb.ListSessionsRequest{Username: user}
		if resp, err := client.ListLocalSessions(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing local sessions for %q; error: %v; response errors: %v",
				user, err, resp.Errors)
		}
		if resp, err := client.ListSessions(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing sessions for %q; error: %v; response errors: %v",
				user, err, resp.Errors)
		}
	}
}
