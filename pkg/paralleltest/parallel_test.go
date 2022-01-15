// Copyright 2016  The Cockroach Authors.
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
//
// The parallel_test adds an orchestration layer on top of the logic_test code
// with the capability of running multiple test data files in parallel.
//
// Each test lives in a separate subdir under testdata/paralleltest. Each test
// dir contains a "test.yaml" file along with a set of files in logic test
// format. The test.yaml file corresponds to the parTestSpec structure below.

package testdata

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/logictest"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/testutils/sqlutils"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"gopkg.in/yaml.v2"
)

type parallelTest struct {
	*testing.T
	ctx     context.Context
	cluster serverutils.TestClusterInterface
	clients [][]*gosql.DB
}

func (t *parallelTest) close() {
	t.clients = nil
	if t.cluster != nil {
		t.cluster.Stopper().Stop(context.TODO())
	}
}

func (t *parallelTest) processTestFile(path string, nodeIdx int, db *gosql.DB, ch chan bool) {
	if ch != nil {
		defer func() { ch <- true }()
	}
	logictest.ProcessTestFile(t.T, t.cluster, path, nodeIdx, db)
}

func (t *parallelTest) getClient(nodeIdx, clientIdx int) *gosql.DB {
	for len(t.clients[nodeIdx]) <= clientIdx {
		// Add a client.
		pgURL, cleanupFunc := sqlutils.PGUrl(t.T,
			t.cluster.Server(nodeIdx).ServingAddr(),
			"TestParallel",
			url.User(security.RootUser))
		db, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		sqlutils.MakeSQLRunner(db).Exec(t, "SET DATABASE = test")
		t.cluster.Stopper().AddCloser(
			stop.CloserFn(func() {
				_ = db.Close()
				cleanupFunc()
			}))
		t.clients[nodeIdx] = append(t.clients[nodeIdx], db)
	}
	return t.clients[nodeIdx][clientIdx]
}

type parTestRunEntry struct {
	Node int    `yaml:"node"`
	File string `yaml:"file"`
}

type parTestSpec struct {
	SkipReason string `yaml:"skip_reason"`

	// ClusterSize is the number of nodes in the cluster. If 0, single node.
	ClusterSize int `yaml:"cluster_size"`

	RangeSplitSize int `yaml:"range_split_size"`

	// Run contains a set of "run lists". The files in a runlist are run in
	// parallel and they complete before the next run list start.
	Run [][]parTestRunEntry `yaml:"run"`
}

func (t *parallelTest) run(dir string) {
	// Process the spec file.
	mainFile := filepath.Join(dir, "test.yaml")
	yamlData, err := ioutil.ReadFile(mainFile)
	if err != nil {
		t.Fatalf("%s: %s", mainFile, err)
	}
	var spec parTestSpec
	if err := yaml.UnmarshalStrict(yamlData, &spec); err != nil {
		t.Fatalf("%s: %s", mainFile, err)
	}

	if spec.SkipReason != "" {
		t.Skip(spec.SkipReason)
	}

	log.Infof(t.ctx, "Running test %s", dir)
	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "spec: %+v", spec)
	}

	t.setup(&spec)
	defer t.close()

	for runListIdx, runList := range spec.Run {
		if testing.Verbose() || log.V(1) {
			var descr []string
			for _, re := range runList {
				descr = append(descr, fmt.Sprintf("%d:%s", re.Node, re.File))
			}
			log.Infof(t.ctx, "%s: run list %d: %s", mainFile, runListIdx,
				strings.Join(descr, ", "))
		}
		// Store the number of clients used so far (per node).
		numClients := make([]int, spec.ClusterSize)
		ch := make(chan bool)
		for _, re := range runList {
			client := t.getClient(re.Node, numClients[re.Node])
			numClients[re.Node]++
			go t.processTestFile(filepath.Join(dir, re.File), re.Node, client, ch)
		}
		// Wait for all clients to complete.
		for range runList {
			<-ch
		}
	}
}

func (t *parallelTest) setup(spec *parTestSpec) {
	if spec.ClusterSize == 0 {
		spec.ClusterSize = 1
	}

	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Cluster Size: %d", spec.ClusterSize)
	}

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					CheckStmtStringChange: true,
				},
			},
		},
	}
	t.cluster = serverutils.StartTestCluster(t, spec.ClusterSize, args)

	for i := 0; i < t.cluster.NumServers(); i++ {
		server := t.cluster.Server(i)
		mode := sessiondata.DistSQLOff
		st := server.ClusterSettings()
		st.Manual.Store(true)
		sql.DistSQLClusterExecMode.Override(&st.SV, int64(mode))
	}

	t.clients = make([][]*gosql.DB, spec.ClusterSize)
	for i := range t.clients {
		t.clients[i] = append(t.clients[i], t.cluster.ServerConn(i))
	}
	r0 := sqlutils.MakeSQLRunner(t.clients[0][0])

	if spec.RangeSplitSize != 0 {
		if testing.Verbose() || log.V(1) {
			log.Infof(t.ctx, "Setting range split size: %d", spec.RangeSplitSize)
		}
		zoneCfg := config.DefaultZoneConfig()
		zoneCfg.RangeMaxBytes = proto.Int64(int64(spec.RangeSplitSize))
		zoneCfg.RangeMinBytes = proto.Int64(*zoneCfg.RangeMaxBytes / 2)
		buf, err := protoutil.Marshal(&zoneCfg)
		if err != nil {
			t.Fatal(err)
		}
		objID := keys.RootNamespaceID
		r0.Exec(t, `UPDATE system.zones SET config = $2 WHERE id = $1`, objID, buf)
	}

	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Creating database")
	}

	r0.Exec(t, "CREATE DATABASE test")
	for i := range t.clients {
		sqlutils.MakeSQLRunner(t.clients[i][0]).Exec(t, "SET DATABASE = test")
	}

	if testing.Verbose() || log.V(1) {
		log.Infof(t.ctx, "Test setup done")
	}
}

func parallel(t *testing.T, dir string) {
	if testutils.NightlyStress() {
		// See https://github.com/znbasedb/znbase/pull/10966.
		t.Skip()
	}
	files := *flag.String(dir, "testdata/parallel_test/"+dir+"/[^.]*", "test data "+dir)
	paths, err := filepath.Glob(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("No testfiles found (files: %s)", files)
	}
	total := 0
	failed := 0
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			pt := parallelTest{T: t, ctx: context.Background()}
			pt.run(path)
			total++
			if t.Failed() {
				failed++
			}
		})
	}
	if failed == 0 {
		log.Infof(context.Background(), "%d parallel tests passed", total)
	} else {
		log.Infof(context.Background(), "%d out of %d parallel tests failed", failed, total)
	}
}

func TestParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parallel(t, "common")
}

func TestParallelRC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parallel(t, "rc")
}

func TestParallelSSI(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("ssi rc not pass")
	parallel(t, "ssi_rc")
}

func TestParallelJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parallel(t, "join")
}

func TestParallelSFU(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("old sfu no need to test")
	parallel(t, "select_for_update")
}

func TestParallelRC1(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parallel(t, "rc1")
}

func TestParallelFixRC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parallel(t, "fix_rc")
}

func TestParallelRCPass(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parallel(t, "rc_pass")
}

func TestParallelReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parallel(t, "replication_follower_read")
}

func init() {
	util.MaxTxnRefreshAttempts = settings.RegisterNonNegativeIntSetting(
		"test.max_refresh_attempts",
		"number of the maximum number of times a single transactional batch can trigger a refresh spans attempt",
		5000,
	)
}
