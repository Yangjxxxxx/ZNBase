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
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getsentry/raven-go"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/znbasedb/cmux"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/blobs"
	"github.com/znbasedb/znbase/pkg/blobs/blobspb"
	"github.com/znbasedb/znbase/pkg/gossip"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/kv"
	"github.com/znbasedb/znbase/pkg/mail"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/rpc/nodedialer"
	"github.com/znbasedb/znbase/pkg/scheduledjobs"
	"github.com/znbasedb/znbase/pkg/security/audit/event"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/server/debug"
	"github.com/znbasedb/znbase/pkg/server/goroutinedumper"
	"github.com/znbasedb/znbase/pkg/server/heapprofiler"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/server/status"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/distsql"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/pgwire"
	"github.com/znbasedb/znbase/pkg/sql/querycache"
	"github.com/znbasedb/znbase/pkg/sql/rowcontainer"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/sql/stats"
	"github.com/znbasedb/znbase/pkg/sqlmigrations"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/bulk"
	"github.com/znbasedb/znbase/pkg/storage/closedts/container"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/storage/storagebase"
	"github.com/znbasedb/znbase/pkg/ts"
	"github.com/znbasedb/znbase/pkg/ui"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/envutil"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/httputil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/log/logtags"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/netutil"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/retry"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
	"github.com/znbasedb/znbase/pkg/util/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/transport"
)

var (
	// Allocation pool for gzipResponseWriters.
	gzipResponseWriterPool sync.Pool

	// GracefulDrainModes is the standard succession of drain modes entered
	// for a graceful shutdown.
	GracefulDrainModes = []serverpb.DrainMode{serverpb.DrainMode_CLIENT, serverpb.DrainMode_LEASES}

	queryWait = settings.RegisterDurationSetting(
		"server.shutdown.query_wait",
		"the server will wait for at least this amount of time for active queries to finish",
		10*time.Second,
	)

	drainWait = settings.RegisterDurationSetting(
		"server.shutdown.drain_wait",
		"the amount of time a server waits in an unready state before proceeding with the rest "+
			"of the shutdown process",
		0*time.Second,
	)

	forwardClockJumpCheckEnabled = settings.RegisterBoolSetting(
		"server.clock.forward_jump_check_enabled",
		"if enabled, forward clock jumps > max_offset/2 will cause a panic",
		false,
	)

	persistHLCUpperBoundInterval = settings.RegisterDurationSetting(
		"server.clock.persist_upper_bound_interval",
		"the interval between persisting the wall time upper bound of the clock. The clock "+
			"does not generate a wall time greater than the persisted timestamp and will panic if "+
			"it sees a wall time greater than this value. When znbase starts, it waits for the "+
			"wall time to catch-up till this persisted timestamp. This guarantees monotonic wall "+
			"time across server restarts. Not setting this or setting a value of 0 disables this "+
			"feature.",
		0,
	)
)

// TODO(peter): Until go1.11, ServeMux.ServeHTTP was not safe to call
// concurrently with ServeMux.Handle. So we provide our own wrapper with proper
// locking. Slightly less efficient because it locks unnecessarily, but
// safe. See TestServeMuxConcurrency. Should remove once we've upgraded to
// go1.11.
type safeServeMux struct {
	mu  syncutil.RWMutex
	mux http.ServeMux
}

func (mux *safeServeMux) Handle(pattern string, handler http.Handler) {
	mux.mu.Lock()
	mux.mux.Handle(pattern, handler)
	mux.mu.Unlock()
}

func (mux *safeServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux.mu.RLock()
	mux.mux.ServeHTTP(w, r)
	mux.mu.RUnlock()
}

// Server is the znbase server node.
type Server struct {
	nodeIDContainer base.NodeIDContainer

	cfg        Config
	st         *cluster.Settings
	mux        safeServeMux
	clock      *hlc.Clock
	startTime  time.Time
	rpcContext *rpc.Context
	// The gRPC server on which the different RPC handlers will be registered.
	grpc             *grpcServer
	gossip           *gossip.Gossip
	nodeDialer       *nodedialer.Dialer
	nodeLiveness     *storage.NodeLiveness
	storePool        *storage.StorePool
	tcsFactory       *kv.TxnCoordSenderFactory
	distSender       *kv.DistSender
	db               *client.DB
	pgServer         *pgwire.Server
	distSQLServer    *distsql.ServerImpl
	node             *Node
	registry         *metric.Registry
	recorder         *status.MetricsRecorder
	runtime          *status.RuntimeStatSampler
	admin            *adminServer
	status           *statusServer
	authentication   *authenticationServer
	initServer       *initServer
	tsDB             *ts.DB
	tsServer         ts.Server
	raftTransport    *storage.RaftTransport
	stopper          *stop.Stopper
	execCfg          *sql.ExecutorConfig
	internalExecutor *sql.InternalExecutor
	leaseMgr         *sql.LeaseManager
	blobService      *blobs.Service
	// sessionRegistry can be queried for info on running SQL sessions. It is
	// shared between the sql.Server and the statusServer.
	sessionRegistry    *sql.SessionRegistry
	jobRegistry        *jobs.Registry
	statsRefresher     *stats.Refresher
	engines            Engines
	internalMemMetrics sql.MemoryMetrics
	adminMemMetrics    sql.MemoryMetrics
	// sqlMemMetrics are used to track memory usage of sql sessions.
	sqlMemMetrics sql.MemoryMetrics

	// mail server used to send alert mail
	mailServer *mail.Server

	// audit server used to handle security audit events
	auditServer *server.AuditServer

	// cursor store Engine
	// (zh) add for cursor
	tempEngines []engine.Engine

	// temp file mem pool
	HsMgr *rowcontainer.HstoreMemMgr
}

// NewServer creates a Server from a server.Config.
func NewServer(cfg Config, stopper *stop.Stopper) (*Server, error) {
	if err := cfg.ValidateAddrs(context.Background()); err != nil {
		return nil, err
	}

	st := cfg.Settings

	if cfg.AmbientCtx.Tracer == nil {
		panic(errors.New("no tracer set in AmbientCtx"))
	}

	clock := hlc.NewClock(hlc.UnixNano, time.Duration(cfg.MaxOffset))
	s := &Server{
		st:       st,
		clock:    clock,
		stopper:  stopper,
		cfg:      cfg,
		registry: metric.NewRegistry(),
	}

	// If the tracer has a Close function, call it after the server stops.
	if tr, ok := cfg.AmbientCtx.Tracer.(stop.Closer); ok {
		stopper.AddCloser(tr)
	}

	// Attempt to load TLS configs right away, failures are permanent.
	if certMgr, err := cfg.InitializeNodeTLSConfigs(stopper); err != nil {
		return nil, err
	} else if certMgr != nil {
		// The certificate manager is non-nil in secure mode.
		s.registry.AddMetricStruct(certMgr.Metrics())
	}

	// Add a dynamic log tag value for the node ID.
	//
	// We need to pass an ambient context to the various server components, but we
	// won't know the node ID until we Start(). At that point it's too late to
	// change the ambient contexts in the components (various background processes
	// will have already started using them).
	//
	// NodeIDContainer allows us to add the log tag to the context now and update
	// the value asynchronously. It's not significantly more expensive than a
	// regular tag since it's just doing an (atomic) load when a log/trace message
	// is constructed. The node ID is set by the Store if this host was
	// bootstrapped; otherwise a new one is allocated in Node.
	s.cfg.AmbientCtx.AddLogTag("n", &s.nodeIDContainer)

	ctx := s.AnnotateCtx(context.Background())

	// Set up mail server
	s.mailServer = mail.NewMailServer(ctx, s.ClusterSettings(), s.registry)

	s.rpcContext = rpc.NewContext(s.cfg.AmbientCtx, s.cfg.Config, s.clock, s.stopper,
		&cfg.Settings.Version)
	s.rpcContext.HeartbeatCB = func() {
		if err := s.rpcContext.RemoteClocks.VerifyClockOffset(ctx); err != nil {
			log.Fatal(ctx, err)
		}
	}

	s.grpc = newGRPCServer(s.rpcContext)
	s.HsMgr = rowcontainer.NewHstoreMemMgr(0)
	s.gossip = gossip.New(
		s.cfg.AmbientCtx,
		&s.rpcContext.ClusterID,
		&s.nodeIDContainer,
		s.rpcContext,
		s.grpc.Server,
		s.stopper,
		s.registry,
		s.cfg.Locality,
	)
	s.nodeDialer = nodedialer.New(s.rpcContext, gossip.AddressResolver(s.gossip))

	// Create blob service for inter-node file sharing.
	if blobService, err := blobs.NewBlobService(s.ClusterSettings().ExternalIODir); err != nil {
		log.Fatal(ctx, err)
	} else {
		s.blobService = blobService
	}
	blobspb.RegisterBlobServer(s.grpc.Server, s.blobService)

	// A custom RetryOptions is created which uses stopper.ShouldQuiesce() as
	// the Closer. This prevents infinite retry loops from occurring during
	// graceful server shutdown
	//
	// Such a loop occurs when the DistSender attempts a connection to the
	// local server during shutdown, and receives an internal server error (HTTP
	// Code 5xx). This is the correct error for a server to return when it is
	// shutting down, and is normally retryable in a cluster environment.
	// However, on a single-node setup (such as a test), retries will never
	// succeed because the only server has been shut down; thus, the
	// DistSender needs to know that it should not retry in this situation.
	var clientTestingKnobs kv.ClientTestingKnobs
	if kvKnobs := s.cfg.TestingKnobs.KVClient; kvKnobs != nil {
		clientTestingKnobs = *kvKnobs.(*kv.ClientTestingKnobs)
	}
	retryOpts := s.cfg.RetryOptions
	if retryOpts == (retry.Options{}) {
		retryOpts = base.DefaultRetryOptions()
	}
	retryOpts.Closer = s.stopper.ShouldQuiesce()
	distSenderCfg := kv.DistSenderConfig{
		AmbientCtx:      s.cfg.AmbientCtx,
		Settings:        st,
		Clock:           s.clock,
		RPCContext:      s.rpcContext,
		RPCRetryOptions: &retryOpts,
		TestingKnobs:    clientTestingKnobs,
		NodeDialer:      s.nodeDialer,
	}
	s.distSender = kv.NewDistSender(distSenderCfg, s.gossip)
	s.registry.AddMetricStruct(s.distSender.Metrics())

	txnMetrics := kv.MakeTxnMetrics(s.cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(txnMetrics)
	txnCoordSenderFactoryCfg := kv.TxnCoordSenderFactoryConfig{
		AmbientCtx:   s.cfg.AmbientCtx,
		Settings:     st,
		Clock:        s.clock,
		Stopper:      s.stopper,
		Linearizable: s.cfg.Linearizable,
		Metrics:      txnMetrics,
		TestingKnobs: clientTestingKnobs,
	}
	s.tcsFactory = kv.NewTxnCoordSenderFactory(txnCoordSenderFactoryCfg, s.distSender)

	dbCtx := client.DefaultDBContext()
	dbCtx.NodeID = &s.nodeIDContainer
	dbCtx.Stopper = s.stopper
	s.db = client.NewDBWithContext(s.cfg.AmbientCtx, s.tcsFactory, s.clock, dbCtx)

	nlActive, nlRenewal := s.cfg.NodeLivenessDurations()

	s.nodeLiveness = storage.NewNodeLiveness(
		s.cfg.AmbientCtx,
		s.clock,
		s.db,
		s.engines,
		s.gossip,
		nlActive,
		nlRenewal,
		s.st,
		s.cfg.HistogramWindowInterval(),
	)
	s.registry.AddMetricStruct(s.nodeLiveness.Metrics())

	s.storePool = storage.NewStorePool(
		s.cfg.AmbientCtx,
		s.st,
		s.gossip,
		s.clock,
		s.nodeLiveness.GetNodeCount,
		storage.MakeStorePoolNodeLivenessFunc(s.nodeLiveness),
		/* deterministic */ false,
	)

	s.raftTransport = storage.NewRaftTransport(
		s.cfg.AmbientCtx, st, s.nodeDialer, s.grpc.Server, s.stopper,
	)

	// Set up internal memory metrics for use by internal SQL executors.
	s.internalMemMetrics = sql.MakeMemMetrics("internal", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.internalMemMetrics)

	// Set up Lease Manager
	var lmKnobs sql.LeaseManagerTestingKnobs
	if leaseManagerTestingKnobs := cfg.TestingKnobs.SQLLeaseManager; leaseManagerTestingKnobs != nil {
		lmKnobs = *leaseManagerTestingKnobs.(*sql.LeaseManagerTestingKnobs)
	}
	s.leaseMgr = sql.NewLeaseManager(
		s.cfg.AmbientCtx,
		nil, /* execCfg - will be set later because of circular dependencies */
		lmKnobs,
		s.stopper,
		s.cfg.LeaseManagerConfig,
	)

	// We do not set memory monitors or a noteworthy limit because the children of
	// this monitor will be setting their own noteworthy limits.
	rootSQLMemoryMonitor := mon.MakeMonitor(
		"root",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment: use default increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	rootSQLMemoryMonitor.Start(context.Background(), nil, mon.MakeStandaloneBudget(s.cfg.SQLMemoryPoolSize))

	// Set up the DistSQL temp engine.

	useStoreSpec := cfg.Stores.Specs[s.cfg.TempStorageConfig.SpecIdx]
	tempEngine, err := engine.NewTempEngine(ctx, s.cfg.StorageEngine, s.cfg.TempStorageConfig, useStoreSpec)
	if err != nil {
		return nil, errors.Wrap(err, "could not create temp storage")
	}
	s.stopper.AddCloser(tempEngine)
	s.stopper.AddCloser(s.HsMgr)
	// Remove temporary directory linked to tempEngine after closing
	// tempEngine.
	s.stopper.AddCloser(stop.CloserFn(func() {
		firstStore := cfg.Stores.Specs[s.cfg.TempStorageConfig.SpecIdx]
		var err error
		if firstStore.InMemory {
			// First store is in-memory so we remove the temp
			// directory directly since there is no record file.
			err = os.RemoveAll(s.cfg.TempStorageConfig.Path)
		} else {
			// If record file exists, we invoke CleanupTempDirs to
			// also remove the record after the temp directory is
			// removed.
			recordPath := filepath.Join(firstStore.Path, TempDirsRecordFilename)
			err = engine.CleanupTempDirs(recordPath)
		}
		if err != nil {
			log.Errorf(context.TODO(), "could not remove temporary store directory: %v", err.Error())
		}
	}))

	// Set up admin memory metrics for use by admin SQL executors.
	s.adminMemMetrics = sql.MakeMemMetrics("admin", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.adminMemMetrics)

	s.tsDB = ts.NewDB(s.db, s.cfg.Settings)
	s.registry.AddMetricStruct(s.tsDB.Metrics())
	nodeCountFn := func() int64 {
		return s.nodeLiveness.Metrics().LiveNodes.Value()
	}
	s.tsServer = ts.MakeServer(s.cfg.AmbientCtx, s.tsDB, nodeCountFn, s.cfg.TimeSeriesServerConfig, s.stopper, s.cfg.Stores.Specs[0].Path)

	// The InternalExecutor will be further initialized later, as we create more
	// of the server's components. There's a circular dependency - many things
	// need an InternalExecutor, but the InternalExecutor needs an ExecutorConfig,
	// which in turn needs many things. That's why everybody that needs an
	// InternalExecutor uses this one instance.
	internalExecutor := &sql.InternalExecutor{}

	// This function defines how ExternalStorage objects are created.
	dumpSink := func(ctx context.Context, dest roachpb.DumpSink) (dumpsink.DumpSink, error) {
		return dumpsink.MakeDumpSink(
			ctx, dest, st,
			blobs.NewBlobClientFactory(
				s.nodeIDContainer.Get(),
				s.nodeDialer,
				st.ExternalIODir,
			),
		)
	}
	dumpSinkFromURI := func(ctx context.Context, uri string) (dumpsink.DumpSink, error) {
		return dumpsink.FromURI(
			ctx, uri, st,
			blobs.NewBlobClientFactory(
				s.nodeIDContainer.Get(),
				s.nodeDialer,
				st.ExternalIODir,
			),
		)
	}

	// Similarly for execCfg.
	var execCfg sql.ExecutorConfig

	// TODO(bdarnell): make StoreConfig configurable.
	storeCfg := storage.StoreConfig{
		Settings:                st,
		AmbientCtx:              s.cfg.AmbientCtx,
		RaftConfig:              s.cfg.RaftConfig,
		Clock:                   s.clock,
		DB:                      s.db,
		Gossip:                  s.gossip,
		NodeLiveness:            s.nodeLiveness,
		Transport:               s.raftTransport,
		NodeDialer:              s.nodeDialer,
		RPCContext:              s.rpcContext,
		ScanInterval:            s.cfg.ScanInterval,
		ScanMinIdleTime:         s.cfg.ScanMinIdleTime,
		ScanMaxIdleTime:         s.cfg.ScanMaxIdleTime,
		TimestampCachePageSize:  s.cfg.TimestampCachePageSize,
		HistogramWindowInterval: s.cfg.HistogramWindowInterval(),
		StorePool:               s.storePool,
		SQLExecutor:             internalExecutor,
		LogRangeEvents:          s.cfg.EventLogEnabled,
		RangeDescriptorCache:    s.distSender.RangeDescriptorCache(),
		TimeSeriesDataStore:     s.tsDB,

		// Initialize the closed timestamp subsystem. Note that it won't
		// be ready until it is .Start()ed, but the grpc server can be
		// registered early.
		ClosedTimestamp: container.NewContainer(container.Config{
			Settings: st,
			Stopper:  s.stopper,
			Clock:    s.nodeLiveness.AsLiveClock(),
			// NB: s.node is not defined at this point, but it will be
			// before this is ever called.
			Refresh: func(rangeIDs ...roachpb.RangeID) {
				for _, rangeID := range rangeIDs {
					repl, err := s.node.stores.GetReplicaForRangeID(rangeID)
					if err != nil || repl == nil {
						continue
					}
					repl.EmitMLAI()
				}
			},
			Dialer: s.nodeDialer.CTDialer(),
		}),

		EnableEpochRangeLeases: true,
		DumpSink:               dumpSink,
		DumpSinkFromURI:        dumpSinkFromURI,
	}
	if storeTestingKnobs := s.cfg.TestingKnobs.Store; storeTestingKnobs != nil {
		storeCfg.TestingKnobs = *storeTestingKnobs.(*storage.StoreTestingKnobs)
	}

	s.recorder = status.NewMetricsRecorder(s.clock, s.nodeLiveness, s.rpcContext, s.gossip, st)
	s.registry.AddMetricStruct(s.rpcContext.RemoteClocks.Metrics())

	s.runtime = status.NewRuntimeStatSampler(ctx, s.clock)
	s.registry.AddMetricStruct(s.runtime)

	// Set up audit server
	s.auditServer = server.NewAuditServer(ctx, s.ClusterSettings(), s.registry)

	// Set up temp Engines for cursor
	cursorEngines := make([]engine.Engine, 0)
	// Cursor Engine
	// 游标存储引擎
	cursorTempStorageConfig := s.cfg.CursorTempStorageConfig
	cursorUseStoreSpec := useStoreSpec
	cursorTempEngine, err2 := engine.NewTempEngine(ctx, enginepb.EngineTypeRocksDB, cursorTempStorageConfig, cursorUseStoreSpec)
	if err2 != nil {
		return nil, errors.Wrap(err, "could not create cursor temp storage")
	}
	s.stopper.AddCloser(cursorTempEngine)
	// Remove temporary directory linked to tempEngine after closing tempEngine.
	// 退出游标临时存储引擎后，删除与其关联的临时存储目录
	s.stopper.AddCloser(stop.CloserFn(func() {
		firstStore := cfg.Stores.Specs[s.cfg.CursorTempStorageConfig.SpecIdx]
		var err error
		if firstStore.InMemory {
			// First store is in-memory so we remove the temp
			// directory directly since there is no record file.
			err = os.RemoveAll(s.cfg.CursorTempStorageConfig.Path)
		} else {
			// If record file exists, we invoke CleanupTempDirs to
			// also remove the record after the temp directory is
			// removed.
			recordPath := filepath.Join(firstStore.Path, CursorTempDirsRecordFilename)
			err = engine.CleanupTempDirs(recordPath)
		}
		if err != nil {
			log.Errorf(context.TODO(), "could not remove cursor temporary store directory: %v", err.Error())
		}
	}))
	cursorEngines = append(cursorEngines, cursorTempEngine)
	s.tempEngines = cursorEngines

	s.node = NewNode(
		storeCfg, s.recorder, s.registry, s.stopper,
		txnMetrics, s.auditServer, &s.rpcContext.ClusterID)
	roachpb.RegisterInternalServer(s.grpc.Server, s.node)
	storage.RegisterPerReplicaServer(s.grpc.Server, s.node.perReplicaServer)
	s.node.storeCfg.ClosedTimestamp.RegisterClosedTimestampServer(s.grpc.Server)

	s.sessionRegistry = sql.NewSessionRegistry()
	s.jobRegistry = jobs.MakeRegistry(
		s.cfg.AmbientCtx,
		s.stopper,
		s.clock,
		s.db,
		internalExecutor,
		&s.nodeIDContainer,
		st,
		s.cfg.HistogramWindowInterval(),
		func(opName, user string) (interface{}, func()) {
			// This is a hack to get around a Go package dependency cycle. See comment
			// in sql/jobs/registry.go on planHookMaker.
			return sql.NewInternalPlanner(opName, nil, user, &sql.MemoryMetrics{}, &execCfg)
		},
	)
	s.registry.AddMetricStruct(s.jobRegistry.MetricsStruct())

	distSQLMetrics := runbase.MakeDistSQLMetrics(cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(distSQLMetrics)
	// Set up the DistSQL server.
	distSQLCfg := runbase.ServerConfig{
		HsMgr:          s.HsMgr,
		TempPath:       s.cfg.Stores.Specs[0].Path,
		AmbientContext: s.cfg.AmbientCtx,
		Settings:       st,
		RuntimeStats:   s.runtime,
		DB:             s.db,
		Executor:       internalExecutor,
		FlowDB:         client.NewDB(s.cfg.AmbientCtx, s.tcsFactory, s.clock),
		RPCContext:     s.rpcContext,
		Stopper:        s.stopper,
		NodeID:         &s.nodeIDContainer,
		ClusterID:      &s.rpcContext.ClusterID,

		TempStorage: tempEngine,
		BulkAdder: func(ctx context.Context, db *client.DB, bufferSize, flushSize int64,
			ts hlc.Timestamp, disallowShadowing bool) (storagebase.BulkAdder, error) {
			return bulk.MakeBulkAdder(db, s.distSender.RangeDescriptorCache(), bufferSize, flushSize,
				ts, disallowShadowing)
		},
		DiskMonitor: s.cfg.TempStorageConfig.Mon,

		ParentMemoryMonitor: &rootSQLMemoryMonitor,

		Metrics: &distSQLMetrics,

		JobRegistry:  s.jobRegistry,
		Gossip:       s.gossip,
		NodeDialer:   s.nodeDialer,
		LeaseManager: s.leaseMgr,

		DumpSink:        dumpSink,
		DumpSinkFromURI: dumpSinkFromURI,
	}
	if distSQLTestingKnobs := s.cfg.TestingKnobs.DistSQL; distSQLTestingKnobs != nil {
		distSQLCfg.TestingKnobs.CDC = (*distSQLTestingKnobs.(*distsql.TestingKnobs)).CDC
		distSQLCfg.TestingKnobs.DeterministicStats = (*distSQLTestingKnobs.(*distsql.TestingKnobs)).DeterministicStats
		//distSQLCfg.TestingKnobs.BulkAdderFlushesEveryBatch = (*distSQLTestingKnobs.(*runbase.TestingKnobs)).BulkAdderFlushesEveryBatch
		distSQLCfg.TestingKnobs.DrainFast = (*distSQLTestingKnobs.(*distsql.TestingKnobs)).DrainFast
		distSQLCfg.TestingKnobs.EnableVectorizedInvariantsChecker = (*distSQLTestingKnobs.(*distsql.TestingKnobs)).EnableVectorizedInvariantsChecker
		distSQLCfg.TestingKnobs.MemoryLimitBytes = (*distSQLTestingKnobs.(*distsql.TestingKnobs)).MemoryLimitBytes
		distSQLCfg.TestingKnobs.MetadataTestLevel = runbase.MetadataTestLevel((*distSQLTestingKnobs.(*distsql.TestingKnobs)).MetadataTestLevel)
		distSQLCfg.TestingKnobs.RunAfterBackfillChunk = (*distSQLTestingKnobs.(*distsql.TestingKnobs)).RunAfterBackfillChunk
		distSQLCfg.TestingKnobs.RunBeforeBackfillChunk = (*distSQLTestingKnobs.(*distsql.TestingKnobs)).RunBeforeBackfillChunk
	}
	_ = os.MkdirAll(distSQLCfg.TempPath+"/hstore", 0777)
	s.distSQLServer = distsql.NewServer(ctx, distSQLCfg)
	distsqlpb.RegisterDistSQLServer(s.grpc.Server, s.distSQLServer)

	s.admin = newAdminServer(s)
	s.status = newStatusServer(
		s.cfg.AmbientCtx,
		st,
		s.cfg.Config,
		s.admin,
		s.db,
		s.gossip,
		s.recorder,
		s.nodeLiveness,
		s.storePool,
		s.rpcContext,
		s.node.stores,
		s.stopper,
		s.sessionRegistry,
	)
	s.authentication = newAuthenticationServer(s)
	for _, gw := range []grpcGatewayServer{s.admin, s.status, s.authentication, &s.tsServer} {
		gw.RegisterService(s.grpc.Server)
	}

	// TODO(andrei): We're creating an initServer even through the inspection of
	// our engines in Server.Start() might reveal that we're already bootstrapped
	// and so we don't need to accept a Bootstrap RPC. The creation of this server
	// early means that a Bootstrap RPC might erroneously succeed. We should
	// figure out early if our engines are bootstrapped and, if they are, create a
	// dummy implementation of the InitServer that rejects all RPCs.
	s.initServer = newInitServer(s.gossip.Connected, s.stopper.ShouldStop(), s.auditServer, s.node.clusterID)
	serverpb.RegisterInitServer(s.grpc.Server, s.initServer)

	nodeInfo := sql.NodeInfo{
		AdminURL:  cfg.AdminURL,
		PGURL:     cfg.PGURL,
		ClusterID: s.ClusterID,
		NodeID:    &s.nodeIDContainer,
	}

	virtualSchemas, err := sql.NewVirtualSchemaHolder(ctx, st)
	if err != nil {
		log.Fatal(ctx, err)
	}

	// Set up Executor

	var sqlExecutorTestingKnobs sql.ExecutorTestingKnobs
	if k := s.cfg.TestingKnobs.SQLExecutor; k != nil {
		sqlExecutorTestingKnobs = *k.(*sql.ExecutorTestingKnobs)
	} else {
		sqlExecutorTestingKnobs = sql.ExecutorTestingKnobs{}
	}

	loggerCtx, _ := s.stopper.WithCancelOnStop(ctx)

	execCfg = sql.ExecutorConfig{
		Settings:                s.st,
		NodeInfo:                nodeInfo,
		Locality:                s.cfg.Locality,
		LocationName:            s.cfg.LocationName,
		AmbientCtx:              s.cfg.AmbientCtx,
		DB:                      s.db,
		Gossip:                  s.gossip,
		MetricsRecorder:         s.recorder,
		DistSender:              s.distSender,
		RPCContext:              s.rpcContext,
		LeaseManager:            s.leaseMgr,
		Clock:                   s.clock,
		DistSQLSrv:              s.distSQLServer,
		StatusServer:            s.status,
		SessionRegistry:         s.sessionRegistry,
		JobRegistry:             s.jobRegistry,
		VirtualSchemas:          virtualSchemas,
		HistogramWindowInterval: s.cfg.HistogramWindowInterval(),
		RangeDescriptorCache:    s.distSender.RangeDescriptorCache(),
		LeaseHolderCache:        s.distSender.LeaseHolderCache(),
		TestingKnobs:            sqlExecutorTestingKnobs,
		AuditServer:             s.auditServer,

		DistSQLPlanner: sql.NewDistSQLPlanner(
			ctx,
			distsql.Version,
			s.st,
			// The node descriptor will be set later, once it is initialized.
			roachpb.NodeDescriptor{},
			s.rpcContext,
			s.distSQLServer,
			s.distSender,
			s.gossip,
			s.stopper,
			s.nodeLiveness,
			s.nodeDialer,
		),

		TableStatsCache: stats.NewTableStatisticsCache(
			s.cfg.SQLTableStatCacheSize,
			s.gossip,
			s.db,
			internalExecutor,
		),

		DropTableCache: sql.NewDropTableCache(
			s.gossip,
			s.db,
			s.leaseMgr,
		),

		ExecLogger: log.NewSecondaryLogger(
			loggerCtx, nil /* dirName */, "sql-exec", true /* enableGc */, false, /*forceSyncWrites*/
		),

		AuditLogger: log.NewSecondaryLogger(
			loggerCtx, s.cfg.SQLAuditLogDirName, "sql-audit", true /*enableGc*/, true, /*forceSyncWrites*/
		),

		QueryCache: querycache.New(s.cfg.SQLQueryCacheSize),

		CursorEngines: s.tempEngines,
	}

	if sqlSchemaChangerTestingKnobs := s.cfg.TestingKnobs.SQLSchemaChanger; sqlSchemaChangerTestingKnobs != nil {
		execCfg.SchemaChangerTestingKnobs = sqlSchemaChangerTestingKnobs.(*sql.SchemaChangerTestingKnobs)
	} else {
		execCfg.SchemaChangerTestingKnobs = new(sql.SchemaChangerTestingKnobs)
	}
	if distSQLRunTestingKnobs := s.cfg.TestingKnobs.DistSQL; distSQLRunTestingKnobs != nil {
		execCfg.DistSQLRunTestingKnobs = distSQLRunTestingKnobs.(*distsql.TestingKnobs)
	} else {
		execCfg.DistSQLRunTestingKnobs = new(distsql.TestingKnobs)
	}
	if sqlEvalContext := s.cfg.TestingKnobs.SQLEvalContext; sqlEvalContext != nil {
		execCfg.EvalContextTestingKnobs = *sqlEvalContext.(*tree.EvalContextTestingKnobs)
	}
	distSQLCfg.TestingKnobs.JobsTestingKnobs = cfg.TestingKnobs.JobsTestingKnobs

	s.statsRefresher = stats.MakeRefresher(
		s.st,
		internalExecutor,
		execCfg.TableStatsCache,
		stats.DefaultAsOfTime,
	)
	execCfg.StatsRefresher = s.statsRefresher

	// Set up internal memory metrics for use by internal SQL executors.
	s.sqlMemMetrics = sql.MakeMemMetrics("sql", cfg.HistogramWindowInterval())
	s.registry.AddMetricStruct(s.sqlMemMetrics)
	s.pgServer = pgwire.MakeServer(
		s.cfg.AmbientCtx,
		s.cfg.Config,
		s.ClusterSettings(),
		s.sqlMemMetrics,
		&rootSQLMemoryMonitor,
		s.cfg.HistogramWindowInterval(),
		&execCfg,
	)

	// Now that we have a pgwire.Server (which has a sql.Server), we can close a
	// circular dependency between the distsqlrun.Server and sql.Server and set
	// SessionBoundInternalExecutorFactory.
	s.distSQLServer.ServerConfig.SessionBoundInternalExecutorFactory =
		func(
			ctx context.Context, sessionData *sessiondata.SessionData,
		) sqlutil.InternalExecutor {
			ie := sql.NewSessionBoundInternalExecutor(
				ctx,
				sessionData,
				s.pgServer.SQLServer,
				s.sqlMemMetrics,
				s.st,
			)
			return ie
		}
	for _, m := range s.pgServer.Metrics() {
		s.registry.AddMetricStruct(m)
	}
	*internalExecutor = sql.MakeInternalExecutor(
		ctx, s.pgServer.SQLServer, s.internalMemMetrics, s.ClusterSettings(),
	)
	s.internalExecutor = internalExecutor
	execCfg.InternalExecutor = internalExecutor

	s.execCfg = &execCfg

	s.leaseMgr.SetExecCfg(&execCfg)
	s.leaseMgr.RefreshLeases(s.stopper, s.db, s.gossip)
	s.leaseMgr.PeriodicallyRefreshSomeLeases()

	// init audit server
	s.auditServer.InitLogHandler(event.InitEvents(ctx, s.mailServer, s.execCfg, s.registry))
	// init DataFilePath for reports
	GetDataPath(s)
	return s, nil
}

// ClusterSettings returns the cluster settings.
func (s *Server) ClusterSettings() *cluster.Settings {
	gossip.GossipMaxhops.SetOnChange(&s.st.SV, func() {
		gossip.CurrentGossipMaxhops = (int)(gossip.GossipMaxhops.Get(&s.st.SV))
		s.gossip.RecomputeMaxPeersLocked()
		ctx := s.AnnotateCtx(context.Background())
		s.gossip.TightenNetwork(ctx)
	})

	return s.st
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *Server) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// AnnotateCtxWithSpan is a convenience wrapper; see AmbientContext.
func (s *Server) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, opentracing.Span) {
	return s.cfg.AmbientCtx.AnnotateCtxWithSpan(ctx, opName)
}

// ClusterID returns the ID of the cluster this server is a part of.
func (s *Server) ClusterID() uuid.UUID {
	return s.rpcContext.ClusterID.Get()
}

// NodeID returns the ID of this node within its cluster.
func (s *Server) NodeID() roachpb.NodeID {
	return s.node.Descriptor.NodeID
}

// InitialBoot returns whether this is the first time the node has booted.
// Only intended to help print debugging info during server startup.
func (s *Server) InitialBoot() bool {
	return s.node.initialBoot
}

// grpcGatewayServer represents a grpc service with HTTP endpoints through GRPC
// gateway.
type grpcGatewayServer interface {
	RegisterService(g *grpc.Server)
	RegisterGateway(
		ctx context.Context,
		mux *gwruntime.ServeMux,
		conn *grpc.ClientConn,
	) error
}

// ListenError is returned from Start when we fail to start listening on either
// the main ZNBase port or the HTTP port, so that the CLI can instruct the
// user on what might have gone wrong.
type ListenError struct {
	error
	Addr string
}

func inspectEngines(
	ctx context.Context,
	engines []engine.Engine,
	minVersion, serverVersion roachpb.Version,
	clusterIDContainer *base.ClusterIDContainer,
) (
	bootstrappedEngines []engine.Engine,
	emptyEngines []engine.Engine,
	_ cluster.ClusterVersion,
	_ error,
) {
	for _, engine := range engines {
		storeIdent, err := storage.ReadStoreIdent(ctx, engine)
		if _, notBootstrapped := err.(*storage.NotBootstrappedError); notBootstrapped {
			emptyEngines = append(emptyEngines, engine)
			continue
		} else if err != nil {
			return nil, nil, cluster.ClusterVersion{}, err
		}
		clusterID := clusterIDContainer.Get()
		if storeIdent.ClusterID != uuid.Nil {
			if clusterID == uuid.Nil {
				clusterIDContainer.Set(ctx, storeIdent.ClusterID)
			} else if storeIdent.ClusterID != clusterID {
				return nil, nil, cluster.ClusterVersion{},
					errors.Errorf("conflicting store cluster IDs: %s, %s", storeIdent.ClusterID, clusterID)
			}
		}
		bootstrappedEngines = append(bootstrappedEngines, engine)
	}

	cv, err := storage.SynthesizeClusterVersionFromEngines(ctx, bootstrappedEngines, minVersion, serverVersion)
	if err != nil {
		return nil, nil, cluster.ClusterVersion{}, err
	}
	return bootstrappedEngines, emptyEngines, cv, nil
}

// listenerInfo is a helper used to write files containing various listener
// information to the store directories. In contrast to the "listening url
// file", these are written once the listeners are available, before the server
// is necessarily ready to serve.
type listenerInfo struct {
	listen    string // the (RPC) listen address
	advertise string // equals `listen` unless --advertise-addr is used
	http      string // the HTTP endpoint
}

// Iter returns a mapping of file names to desired contents.
func (li listenerInfo) Iter() map[string]string {
	return map[string]string{
		"znbase.advertise-addr": li.advertise,
		"znbase.http-addr":      li.http,
		"znbase.listen-addr":    li.listen,
	}
}

type singleListener struct {
	conn net.Conn
}

func (s *singleListener) Accept() (net.Conn, error) {
	if c := s.conn; c != nil {
		s.conn = nil
		return c, nil
	}
	return nil, io.EOF
}

func (s *singleListener) Close() error {
	return nil
}

func (s *singleListener) Addr() net.Addr {
	return s.conn.LocalAddr()
}

// startMonitoringForwardClockJumps starts a background task to monitor forward
// clock jumps based on a cluster setting
func (s *Server) startMonitoringForwardClockJumps(ctx context.Context) {
	forwardJumpCheckEnabled := make(chan bool, 1)
	s.stopper.AddCloser(stop.CloserFn(func() { close(forwardJumpCheckEnabled) }))

	forwardClockJumpCheckEnabled.SetOnChange(&s.st.SV, func() {
		forwardJumpCheckEnabled <- forwardClockJumpCheckEnabled.Get(&s.st.SV)
	})

	if err := s.clock.StartMonitoringForwardClockJumps(
		forwardJumpCheckEnabled,
		time.NewTicker,
		nil, /* tick callback */
	); err != nil {
		log.Fatal(ctx, err)
	}

	log.Info(ctx, "monitoring forward clock jumps based on server.clock.forward_jump_check_enabled")
}

// ensureClockMonotonicity sleeps till the wall time reaches
// prevHLCUpperBound. prevHLCUpperBound > 0 implies we need to guarantee HLC
// monotonicity across server restarts. prevHLCUpperBound is the last
// successfully persisted timestamp greater then any wall time used by the
// server.
//
// If prevHLCUpperBound is 0, the function sleeps up to max offset
func ensureClockMonotonicity(
	ctx context.Context,
	clock *hlc.Clock,
	startTime time.Time,
	prevHLCUpperBound int64,
	sleepUntilFn func(until int64, currTime func() int64),
) {
	var sleepUntil int64
	if prevHLCUpperBound != 0 {
		// Sleep until previous HLC upper bound to ensure wall time monotonicity
		sleepUntil = prevHLCUpperBound + 1
	} else {
		// Previous HLC Upper bound is not known
		// We might have to sleep a bit to protect against this node producing non-
		// monotonic timestamps. Before restarting, its clock might have been driven
		// by other nodes' fast clocks, but when we restarted, we lost all this
		// information. For example, a client might have written a value at a
		// timestamp that's in the future of the restarted node's clock, and if we
		// don't do something, the same client's read would not return the written
		// value. So, we wait up to MaxOffset; we couldn't have served timestamps more
		// than MaxOffset in the future (assuming that MaxOffset was not changed, see
		// #9733).
		//
		// As an optimization for tests, we don't sleep if all the stores are brand
		// new. In this case, the node will not serve anything anyway until it
		// synchronizes with other nodes.

		// Don't have to sleep for monotonicity when using clockless reads
		// (nor can we, for we would sleep forever).
		if maxOffset := clock.MaxOffset(); maxOffset != timeutil.ClocklessMaxOffset {
			sleepUntil = startTime.UnixNano() + int64(maxOffset) + 1
		}
	}

	currentWallTimeFn := func() int64 { /* function to report current time */
		return clock.Now().WallTime
	}
	currentWallTime := currentWallTimeFn()
	delta := time.Duration(sleepUntil - currentWallTime)
	if delta > 0 {
		log.Infof(
			ctx,
			"Sleeping till wall time %v to catches up to %v to ensure monotonicity. Delta: %v",
			currentWallTime,
			sleepUntil,
			delta,
		)
		sleepUntilFn(sleepUntil, currentWallTimeFn)
	}
}

// periodicallyPersistHLCUpperBound periodically persists an upper bound of
// the HLC's wall time. The interval for persisting is read from
// persistHLCUpperBoundIntervalCh. An interval of 0 disables persisting.
//
// persistHLCUpperBoundFn is used to persist the hlc upper bound, and should
// return an error if the persist fails.
//
// tickerFn is used to create the ticker used for persisting
//
// tickCallback is called whenever a tick is processed
func periodicallyPersistHLCUpperBound(
	clock *hlc.Clock,
	persistHLCUpperBoundIntervalCh chan time.Duration,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
	stopCh <-chan struct{},
	tickCallback func(),
) {
	// Create a ticker which can be used in selects.
	// This ticker is turned on / off based on persistHLCUpperBoundIntervalCh
	ticker := tickerFn(time.Hour)
	ticker.Stop()

	// persistInterval is the interval used for persisting the
	// an upper bound of the HLC
	var persistInterval time.Duration
	var ok bool

	persistHLCUpperBound := func() {
		if err := clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(persistInterval*3), /* delta to compute upper bound */
		); err != nil {
			log.Fatalf(
				context.Background(),
				"error persisting HLC upper bound: %v",
				err,
			)
		}
	}

	for {
		select {
		case persistInterval, ok = <-persistHLCUpperBoundIntervalCh:
			ticker.Stop()
			if !ok {
				return
			}

			if persistInterval > 0 {
				ticker = tickerFn(persistInterval)
				persistHLCUpperBound()
				log.Info(context.Background(), "persisting HLC upper bound is enabled")
			} else {
				if err := clock.ResetHLCUpperBound(persistHLCUpperBoundFn); err != nil {
					log.Fatalf(
						context.Background(),
						"error resetting hlc upper bound: %v",
						err,
					)
				}
				log.Info(context.Background(), "persisting HLC upper bound is disabled")
			}

		case <-ticker.C:
			if persistInterval > 0 {
				persistHLCUpperBound()
			}

		case <-stopCh:
			ticker.Stop()
			return
		}

		if tickCallback != nil {
			tickCallback()
		}
	}
}

// startPersistingHLCUpperBound starts a goroutine to persist an upper bound
// to the HLC.
//
// persistHLCUpperBoundFn is used to persist upper bound of the HLC, and should
// return an error if the persist fails
//
// tickerFn is used to create a new ticker
//
// tickCallback is called whenever persistHLCUpperBoundCh or a ticker tick is
// processed
func (s *Server) startPersistingHLCUpperBound(
	hlcUpperBoundExists bool,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
) {
	persistHLCUpperBoundIntervalCh := make(chan time.Duration, 1)
	persistHLCUpperBoundInterval.SetOnChange(&s.st.SV, func() {
		persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)
	})

	if hlcUpperBoundExists {
		// The feature to persist upper bounds to wall times is enabled.
		// Persist a new upper bound to continue guaranteeing monotonicity
		// Going forward the goroutine launched below will take over persisting
		// the upper bound
		if err := s.clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(5*time.Second),
		); err != nil {
			log.Fatal(context.TODO(), err)
		}
	}

	s.stopper.RunWorker(
		context.TODO(),
		func(context.Context) {
			periodicallyPersistHLCUpperBound(
				s.clock,
				persistHLCUpperBoundIntervalCh,
				persistHLCUpperBoundFn,
				tickerFn,
				s.stopper.ShouldStop(),
				nil, /* tick callback */
			)
		},
	)
}

// Start starts the server on the specified port, starts gossip and initializes
// the node using the engines from the server's context. This is complex since
// it sets up the listeners and the associated port muxing, but especially since
// it has to solve the "bootstrapping problem": nodes need to connect to Gossip
// fairly early, but what drives Gossip connectivity are the first range
// replicas in the kv store. This in turn suggests opening the Gossip server
// early. However, naively doing so also serves most other services prematurely,
// which exposes a large surface of potentially underinitialized services. This
// is avoided with some additional complexity that can be summarized as follows:
//
// - before blocking trying to connect to the Gossip network, we already open
//   the admin UI (so that its diagnostics are available)
// - we also allow our Gossip and our connection health Ping service
// - everything else returns Unavailable errors (which are retryable)
// - once the node has started, unlock all RPCs.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *Server) Start(ctx context.Context) error {
	if !s.st.Initialized {
		return errors.New("must pass initialized ClusterSettings")
	}
	ctx = s.AnnotateCtx(ctx)
	s.HsMgr.Start()
	s.startTime = timeutil.Now()
	s.startMonitoringForwardClockJumps(ctx)

	// Start mail server
	s.mailServer.Start(ctx, s.stopper)

	uiTLSConfig, err := s.cfg.GetUIServerTLSConfig()
	if err != nil {
		return err
	}

	httpServer := netutil.MakeServer(s.stopper, uiTLSConfig, s)

	// The following code is a specialization of util/net.go's ListenAndServe
	// which adds pgwire support. A single port is used to serve all protocols
	// (pg, http, h2) via the following construction:
	//
	// non-TLS case:
	// net.Listen -> cmux.New
	//               |
	//               -  -> pgwire.Match -> pgwire.Server.ServeConn
	//               -  -> cmux.Any -> grpc.(*Server).Serve
	//
	// TLS case:
	// net.Listen -> cmux.New
	//               |
	//               -  -> pgwire.Match -> pgwire.Server.ServeConn
	//               -  -> cmux.Any -> grpc.(*Server).Serve
	//
	// Note that the difference between the TLS and non-TLS cases exists due to
	// Go's lack of an h2c (HTTP2 Clear Text) implementation. See inline comments
	// in util.ListenAndServe for an explanation of how h2c is implemented there
	// and here.

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return ListenError{error: err, Addr: s.cfg.Addr}
	}
	if err := base.UpdateAddrs(ctx, &s.cfg.Addr, &s.cfg.AdvertiseAddr, ln.Addr()); err != nil {
		return errors.Wrapf(err, "internal error: cannot parse listen address")
	}
	log.Eventf(ctx, "listening on port %s", s.cfg.Addr)

	s.rpcContext.SetLocalInternalServer(s.node)

	// The cmux matches don't shut down properly unless serve is called on the
	// cmux at some point. Use serveOnMux to ensure it's called during shutdown
	// if we wouldn't otherwise reach the point where we start serving on it.
	var serveOnMux sync.Once
	m := cmux.New(ln)

	pgL := m.Match(func(r io.Reader) bool {
		return pgwire.Match(r)
	})

	anyL := m.Match(cmux.Any())

	httpLn, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		return ListenError{
			error: err,
			Addr:  s.cfg.HTTPAddr,
		}
	}
	if err := base.UpdateAddrs(ctx, &s.cfg.HTTPAddr, &s.cfg.HTTPAdvertiseAddr, httpLn.Addr()); err != nil {
		return errors.Wrapf(err, "internal error: cannot parse http listen address")
	}

	// Check the compatibility between the configured addresses and that
	// provided in certificates. This also logs the certificate
	// addresses in all cases to aid troubleshooting.
	// This must be called after both calls to UpdateAddrs() above.
	s.cfg.CheckCertificateAddrs(ctx)

	workersCtx := s.AnnotateCtx(context.Background())

	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Fatal(workersCtx, err)
		}
	})

	if uiTLSConfig != nil {
		httpMux := cmux.New(httpLn)
		clearL := httpMux.Match(cmux.HTTP1())
		tlsL := httpMux.Match(cmux.Any())

		s.stopper.RunWorker(workersCtx, func(context.Context) {
			netutil.FatalIfUnexpected(httpMux.Serve())
		})

		s.stopper.RunWorker(workersCtx, func(context.Context) {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
			})
			mux.Handle("/health", s)

			plainRedirectServer := netutil.MakeServer(s.stopper, uiTLSConfig, mux)

			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		})

		httpLn = tls.NewListener(tlsL, uiTLSConfig)
	}

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(httpServer.Serve(httpLn))
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		<-s.stopper.ShouldQuiesce()
		// TODO(bdarnell): Do we need to also close the other listeners?
		netutil.FatalIfUnexpected(anyL.Close())
		<-s.stopper.ShouldStop()
		s.grpc.Stop()
		serveOnMux.Do(func() {
			// A cmux can't gracefully shut down without Serve being called on it.
			netutil.FatalIfUnexpected(m.Serve())
		})
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(s.grpc.Serve(anyL))
	})

	// Running the SQL migrations safely requires that we aren't serving SQL
	// requests at the same time -- to ensure that, block the serving of SQL
	// traffic until the migrations are done, as indicated by this channel.
	serveSQL := make(chan bool)

	tcpKeepAlive := envutil.EnvOrDefaultDuration("ZNBASE_SQL_TCP_KEEP_ALIVE", time.Minute)
	var loggedKeepAliveStatus int32

	// Attempt to set TCP keep-alive on connection. Don't fail on errors.
	setTCPKeepAlive := func(ctx context.Context, conn net.Conn) {
		if tcpKeepAlive == 0 {
			return
		}

		muxConn, ok := conn.(*cmux.MuxConn)
		if !ok {
			return
		}
		tcpConn, ok := muxConn.Conn.(*net.TCPConn)
		if !ok {
			return
		}

		// Only log success/failure once.
		doLog := atomic.CompareAndSwapInt32(&loggedKeepAliveStatus, 0, 1)
		if err := tcpConn.SetKeepAlive(true); err != nil {
			if doLog {
				log.Warningf(ctx, "failed to enable TCP keep-alive for pgwire: %v", err)
			}
			return

		}
		if err := tcpConn.SetKeepAlivePeriod(tcpKeepAlive); err != nil {
			if doLog {
				log.Warningf(ctx, "failed to set TCP keep-alive duration for pgwire: %v", err)
			}
			return
		}

		if doLog {
			log.VEventf(ctx, 2, "setting TCP keep-alive to %s for pgwire", tcpKeepAlive)
		}
	}

	// Enable the debug endpoints first to provide an earlier window into what's
	// going on with the node in advance of exporting node functionality.
	//
	// TODO(marc): when cookie-based authentication exists, apply it to all web
	// endpoints.
	s.mux.Handle(debug.Endpoint, debug.NewServer(s.st))

	// Initialize grpc-gateway mux and context in order to get the /health
	// endpoint working even before the node has fully initialized.
	jsonpb := &protoutil.JSONPb{
		EnumsAsInts:  true,
		EmitDefaults: true,
		Indent:       "  ",
	}
	protopb := new(protoutil.ProtoPb)
	gwMux := gwruntime.NewServeMux(
		gwruntime.WithMarshalerOption(gwruntime.MIMEWildcard, jsonpb),
		gwruntime.WithMarshalerOption(httputil.JSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(httputil.AltJSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(httputil.ProtoContentType, protopb),
		gwruntime.WithMarshalerOption(httputil.AltProtoContentType, protopb),
		gwruntime.WithOutgoingHeaderMatcher(authenticationHeaderMatcher),
		gwruntime.WithMetadata(forwardAuthenticationMetadata),
	)
	gwCtx, gwCancel := context.WithCancel(s.AnnotateCtx(context.Background()))
	s.stopper.AddCloser(stop.CloserFn(gwCancel))

	// Setup HTTP<->gRPC handlers.
	c1, c2 := net.Pipe()

	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		for _, c := range []net.Conn{c1, c2} {
			if err := c.Close(); err != nil {
				log.Fatal(workersCtx, err)
			}
		}
	})

	s.stopper.RunWorker(workersCtx, func(context.Context) {
		netutil.FatalIfUnexpected(s.grpc.Serve(&singleListener{
			conn: c1,
		}))
	})

	// Eschew `(*rpc.Context).GRPCDial` to avoid unnecessary moving parts on the
	// uniquely in-process connection.
	dialOpts, err := s.rpcContext.GRPCDialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.DialContext(ctx, s.cfg.AdvertiseAddr, append(
		dialOpts,
		grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return c2, nil
		}),
	)...)
	if err != nil {
		return err
	}
	s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
		<-s.stopper.ShouldQuiesce()
		if err := conn.Close(); err != nil {
			log.Fatal(workersCtx, err)
		}
	})

	for _, gw := range []grpcGatewayServer{s.admin, s.status, s.authentication, &s.tsServer} {
		if err := gw.RegisterGateway(gwCtx, gwMux, conn); err != nil {
			return err
		}
	}
	s.mux.Handle("/health", gwMux)

	s.engines, err = s.cfg.CreateEngines(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create engines")
	}
	s.stopper.AddCloser(&s.engines)

	startAssertEngineHealth(ctx, s.stopper, s.engines)

	// Write listener info files early in the startup sequence. `listenerInfo` has a comment.
	listenerFiles := listenerInfo{
		advertise: s.cfg.AdvertiseAddr,
		http:      s.cfg.HTTPAdvertiseAddr,
		listen:    s.cfg.Addr,
	}.Iter()

	for _, storeSpec := range s.cfg.Stores.Specs {
		if storeSpec.InMemory {
			continue
		}
		for base, val := range listenerFiles {
			file := filepath.Join(storeSpec.Path, base)
			if err := ioutil.WriteFile(file, []byte(val), 0644); err != nil {
				return errors.Wrapf(err, "failed to write %s", file)
			}
		}
	}

	bootstrappedEngines, _, _, err := inspectEngines(
		ctx, s.engines, s.cfg.Settings.Version.MinSupportedVersion,
		s.cfg.Settings.Version.ServerVersion, &s.rpcContext.ClusterID)
	if err != nil {
		return errors.Wrap(err, "inspecting engines")
	}

	// Filter the gossip bootstrap resolvers based on the listen and
	// advertise addresses.
	listenAddrU := util.NewUnresolvedAddr("tcp", s.cfg.Addr)
	advAddrU := util.NewUnresolvedAddr("tcp", s.cfg.AdvertiseAddr)
	filtered := s.cfg.FilterGossipBootstrapResolvers(ctx, listenAddrU, advAddrU)
	s.gossip.Start(advAddrU, filtered)
	log.Event(ctx, "started gossip")

	if s.cfg.DelayedBootstrapFn != nil {
		defer time.AfterFunc(30*time.Second, s.cfg.DelayedBootstrapFn).Stop()
	}

	var hlcUpperBoundExists bool
	// doBootstrap is set if we're the ones who bootstrapped the cluster.
	var doBootstrap bool
	if len(bootstrappedEngines) > 0 {
		// The cluster was already initialized.
		doBootstrap = false
		if s.cfg.ReadyFn != nil {
			s.cfg.ReadyFn(false /*waitForInit*/)
		}

		hlcUpperBound, err := storage.ReadMaxHLCUpperBound(ctx, bootstrappedEngines)
		if err != nil {
			log.Fatal(ctx, err)
		}

		if hlcUpperBound > 0 {
			hlcUpperBoundExists = true
		}

		ensureClockMonotonicity(
			ctx,
			s.clock,
			s.startTime,
			hlcUpperBound,
			timeutil.SleepUntil,
		)

		// Ensure that any subsequent use of `znbase init` will receive
		// an error "the cluster was already initialized."
		if _, err := s.initServer.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
			log.Fatal(ctx, err)
		}

	} else if len(s.cfg.GossipBootstrapResolvers) == 0 {
		// If the _unfiltered_ list of hosts from the --join flag is
		// empty, then this node can bootstrap a new cluster. We disallow
		// this if this node is being started with itself specified as a
		// --join host, because that's too likely to be operator error.
		//
		doBootstrap = true
		if s.cfg.ReadyFn != nil {
			// TODO(knz): when ZNBaseDB stops auto-initializing when --join
			// is not specified, this needs to be adjusted as well. See issue
			// #24118 and #28495 for details.
			//
			s.cfg.ReadyFn(false /*waitForInit*/)
		}

		if err := s.bootstrapCluster(ctx); err != nil {
			return err
		}

		log.Infof(ctx, "**** add additional nodes by specifying --join=%s", s.cfg.AdvertiseAddr)
	} else {
		// We have no existing stores and we've been told to join a cluster. Wait
		// for the initServer to bootstrap the cluster or connect to an existing
		// one.
		//
		// TODO(knz): This may need tweaking when #24118 is addressed.

		s.stopper.RunWorker(workersCtx, func(context.Context) {
			serveOnMux.Do(func() {
				netutil.FatalIfUnexpected(m.Serve())
			})
		})

		ready := make(chan struct{})
		if s.cfg.ReadyFn != nil {
			// s.cfg.ReadyFn must be called in any case because the `start`
			// command requires it to signal readiness to a process manager.
			//
			// However we want to be somewhat precisely informative to the user
			// about whether the node is waiting on init / join, or whether
			// the join was successful straight away. So we spawn this gogoroutine
			// and either:
			// - its timer will fire after 2 seconds and we call ReadyFn(true)
			// - bootstrap completes earlier and the ready chan gets closed,
			//   then we call ReadyFn(false).
			go func() {
				waitForInit := false
				tm := time.After(2 * time.Second)
				select {
				case <-tm:
					waitForInit = true
				case <-ready:
				}
				s.cfg.ReadyFn(waitForInit)
			}()
		}

		log.Info(ctx, "no stores bootstrapped and --join flag specified, awaiting init command or join with an already initialized node.")

		initRes, err := s.initServer.awaitBootstrap()
		close(ready)
		if err != nil {
			return err
		}

		doBootstrap = initRes == needBootstrap
		if doBootstrap {
			if err := s.bootstrapCluster(ctx); err != nil {
				return err
			}
		}
	}

	// This opens the main listener.
	s.stopper.RunWorker(workersCtx, func(context.Context) {
		serveOnMux.Do(func() {
			netutil.FatalIfUnexpected(m.Serve())
		})
	})

	// We ran this before, but might've bootstrapped in the meantime. This time
	// we'll get the actual list of bootstrapped and empty engines.
	bootstrappedEngines, emptyEngines, cv, err := inspectEngines(
		ctx, s.engines, s.cfg.Settings.Version.MinSupportedVersion,
		s.cfg.Settings.Version.ServerVersion, &s.rpcContext.ClusterID)
	if err != nil {
		return errors.Wrap(err, "inspecting engines")
	}

	// Record a walltime that is lower than the lowest hlc timestamp this current
	// instance of the node can use. We do not use startTime because it is lower
	// than the timestamp used to create the bootstrap schema.
	timeThreshold := s.clock.Now().WallTime

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been bootstrapped or
	// we're joining an existing cluster for the first time.
	if err := s.node.start(
		ctx,
		advAddrU,
		bootstrappedEngines, emptyEngines,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		s.cfg.LocationName.Duplicate(),
		cv,
		s.cfg.LocalityAddresses,
		s.execCfg.DistSQLPlanner.SetNodeDesc,
	); err != nil {
		return err
	}

	// update node id in audit server
	s.auditServer.InitNodeID(int32(s.node.Descriptor.NodeID))

	log.Event(ctx, "started node")
	s.startPersistingHLCUpperBound(
		hlcUpperBoundExists,
		func(t int64) error { /* function to persist upper bound of HLC to all stores */
			return s.node.SetHLCUpperBound(context.Background(), t)
		},
		time.NewTicker,
	)

	// Cluster ID should have been determined by this point.
	if s.rpcContext.ClusterID.Get() == uuid.Nil {
		log.Fatal(ctx, "Cluster ID failed to be determined during node startup.")
	}

	s.refreshSettings()

	raven.SetTagsContext(map[string]string{
		"cluster":     s.ClusterID().String(),
		"node":        s.NodeID().String(),
		"server_id":   fmt.Sprintf("%s-%s", s.ClusterID().Short(), s.NodeID()),
		"engine_type": s.cfg.StorageEngine.String(),
	})

	// We can now add the node registry.
	s.recorder.AddNode(s.registry, s.node.Descriptor, s.node.startedAt, s.cfg.AdvertiseAddr, s.cfg.HTTPAdvertiseAddr)

	// Begin recording runtime statistics.
	s.startSampleEnvironment(DefaultMetricsSampleInterval)

	// Begin recording time series data collected by the status monitor.
	s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, DefaultMetricsSampleInterval, ts.Resolution10s, s.stopper,
	)

	// Begin recording status summaries.
	s.node.startWriteNodeStatus(DefaultMetricsSampleInterval)

	var graphiteOnce sync.Once
	graphiteEndpoint.SetOnChange(&s.st.SV, func() {
		if graphiteEndpoint.Get(&s.st.SV) != "" {
			graphiteOnce.Do(func() {
				s.node.startGraphiteStatsExporter(s.st)
			})
		}
	})

	// Create and start the schema change manager only after a NodeID
	// has been assigned.
	var testingKnobs *sql.SchemaChangerTestingKnobs
	if s.cfg.TestingKnobs.SQLSchemaChanger != nil {
		testingKnobs = s.cfg.TestingKnobs.SQLSchemaChanger.(*sql.SchemaChangerTestingKnobs)
	} else {
		testingKnobs = new(sql.SchemaChangerTestingKnobs)
	}

	sql.NewSchemaChangeManager(
		s.cfg.AmbientCtx,
		s.execCfg,
		testingKnobs,
		*s.db,
		s.node.Descriptor,
		s.execCfg.DistSQLPlanner,
		// We're reusing the ieFactory from the distSQLServer.
		s.distSQLServer.ServerConfig.SessionBoundInternalExecutorFactory,
	).Start(s.stopper)

	s.distSQLServer.Start()
	s.pgServer.Start(ctx, s.stopper)

	s.grpc.setMode(modeOperational)

	log.Infof(ctx, "starting %s server at %s (use: %s)",
		s.cfg.HTTPRequestScheme(), s.cfg.HTTPAddr, s.cfg.HTTPAdvertiseAddr)
	log.Infof(ctx, "starting grpc/postgres server at %s", s.cfg.Addr)
	log.Infof(ctx, "advertising ZNBaseDB node at %s", s.cfg.AdvertiseAddr)

	log.Event(ctx, "accepting connections")

	// Begin the node liveness heartbeat. Add a callback which records the local
	// store "last up" timestamp for every store whenever the liveness record is
	// updated.
	s.nodeLiveness.StartHeartbeat(ctx, s.stopper, func(ctx context.Context) {
		now := s.clock.Now()
		if err := s.node.stores.VisitStores(func(s *storage.Store) error {
			return s.WriteLastUpTimestamp(ctx, now)
		}); err != nil {
			log.Warning(ctx, errors.Wrap(err, "writing last up timestamp"))
		}
	})
	{
		var regLiveness jobs.NodeLiveness = s.nodeLiveness
		if testingLiveness := s.cfg.TestingKnobs.RegistryLiveness; testingLiveness != nil {
			regLiveness = testingLiveness.(*jobs.FakeNodeLiveness)
		}
		if err := s.jobRegistry.Start(
			ctx, s.stopper, regLiveness, jobs.DefaultCancelInterval, jobs.DefaultAdoptInterval,
		); err != nil {
			return err
		}
	}

	// Cancel session when it is timeout.
	if err := s.SessionCancelJob(s.stopper); err != nil {
		return err
	}

	// Start the background thread for periodically refreshing table statistics.
	if err := s.statsRefresher.Start(ctx, s.stopper, stats.DefaultRefreshInterval); err != nil {
		return err
	}

	// Before serving SQL requests, we have to make sure the database is
	// in an acceptable form for this version of the software.
	// We have to do this after actually starting up the server to be able to
	// seamlessly use the kv client against other nodes in the cluster.
	var mmKnobs sqlmigrations.MigrationManagerTestingKnobs
	if migrationManagerTestingKnobs := s.cfg.TestingKnobs.SQLMigrationManager; migrationManagerTestingKnobs != nil {
		mmKnobs = *migrationManagerTestingKnobs.(*sqlmigrations.MigrationManagerTestingKnobs)
	}
	migMgr := sqlmigrations.NewManager(
		s.stopper,
		s.db,
		s.internalExecutor,
		s.clock,
		mmKnobs,
		s.NodeID().String(),
	)
	migrationFilter := sqlmigrations.AllMigrations
	if doBootstrap {
		// If we've just bootstrapped, some migrations are unnecessary because
		// they're included in the metadata schema that we've written to the store
		// by hand.
		migrationFilter = sqlmigrations.ExcludeMigrationsIncludedInBootstrap
	}
	if err := migMgr.EnsureMigrations(ctx, migrationFilter); err != nil {
		select {
		case <-s.stopper.ShouldQuiesce():
			// Avoid turning an early shutdown into a fatal error. See #19579.
			return errors.New("server is shutting down")
		default:
			log.Fatal(ctx, err)
		}
	}
	log.Infof(ctx, "done ensuring all necessary migrations have run")
	close(serveSQL)

	log.Info(ctx, "serving sql connections")
	// Start servicing SQL connections.

	// Serve UI assets.
	//
	// The authentication mux used here is created in "allow anonymous" mode so that the UI
	// assets are served up whether or not there is a session. If there is a session, the mux
	// adds it to the context, and it is templated into index.html so that the UI can show
	// the username of the currently-logged-in user.
	authenticatedUIHandler := newAuthenticationMuxAllowAnonymous(
		s.authentication,
		ui.Handler(ui.Config{
			ExperimentalUseLogin: s.cfg.EnableWebSessionAuthentication,
			LoginEnabled:         s.cfg.RequireWebSession(),
			NodeID:               &s.nodeIDContainer,
			GetUser: func(ctx context.Context) *string {
				if u, ok := ctx.Value(webSessionUserKey{}).(string); ok {
					return &u
				}
				return nil
			},
		}),
	)
	s.mux.Handle("/", authenticatedUIHandler)

	// Register gRPC-gateway endpoints used by the admin UI.
	var authHandler http.Handler = gwMux
	if s.cfg.RequireWebSession() {
		authHandler = newAuthenticationMux(s.authentication, authHandler)
	}

	//make sure the Data Path is updated for reports
	GetDataPath(s)
	s.mux.Handle(adminPrefix, authHandler)
	// Exempt the health check endpoint from authentication.
	s.mux.Handle("/_admin/v1/health", gwMux)
	s.mux.Handle(ts.URLPrefix, authHandler)
	s.mux.Handle(statusPrefix, authHandler)
	s.mux.Handle(loginPath, gwMux)
	s.mux.Handle(logoutPath, authHandler)
	s.mux.Handle(statusVars, http.HandlerFunc(s.status.handleVars))
	//use to set a FileServer for reports folder which shown in the AdminUI
	s.mux.Handle("/reportFile/", http.StripPrefix("/reportFile/", http.FileServer(http.Dir(GetReportPath()))))
	log.Event(ctx, "added http endpoints")

	// Attempt to upgrade cluster version.
	s.startAttemptUpgrade(ctx)
	pgCtx := s.pgServer.AmbientCtx.AnnotateCtx(context.Background())
	s.stopper.RunWorker(pgCtx, func(pgCtx context.Context) {
		select {
		case <-serveSQL:
		case <-s.stopper.ShouldQuiesce():
			return
		}
		netutil.FatalIfUnexpected(httpServer.ServeWith(pgCtx, s.stopper, pgL, func(conn net.Conn) {
			connCtx := logtags.AddTag(pgCtx, "client", conn.RemoteAddr().String())
			setTCPKeepAlive(connCtx, conn)

			// Unless this is a simple disconnect or context timeout, report the error on
			// this connection's context, so that we know which remote client caused the
			// error when looking at the logs. Note that we pass a non-cancelable context
			// in, but the callee eventually wraps the context so that it can get
			// canceled and it may return the error here.
			if err := errors.Cause(s.pgServer.ServeConn(connCtx, conn)); err != nil && !netutil.IsClosedConnection(err) && err != context.Canceled && err != context.DeadlineExceeded {
				log.Error(connCtx, err)
			}
		}))
	})
	if len(s.cfg.SocketFile) != 0 {
		log.Infof(ctx, "starting postgres server at unix:%s", s.cfg.SocketFile)

		// Unix socket enabled: postgres protocol only.
		unixLn, err := net.Listen("unix", s.cfg.SocketFile)
		if err != nil {
			return err
		}

		s.stopper.RunWorker(workersCtx, func(workersCtx context.Context) {
			<-s.stopper.ShouldQuiesce()
			if err := unixLn.Close(); err != nil {
				log.Fatal(workersCtx, err)
			}
		})

		s.stopper.RunWorker(pgCtx, func(pgCtx context.Context) {
			select {
			case <-serveSQL:
			case <-s.stopper.ShouldQuiesce():
				return
			}
			netutil.FatalIfUnexpected(httpServer.ServeWith(pgCtx, s.stopper, unixLn, func(conn net.Conn) {
				connCtx := logtags.AddTag(pgCtx, "client", conn.RemoteAddr().String())
				if err := s.pgServer.ServeConn(connCtx, conn); err != nil &&
					!netutil.IsClosedConnection(err) {
					// Report the error on this connection's context, so that we
					// know which remote client caused the error when looking at
					// the logs.
					log.Error(connCtx, err)
				}
			}))
		})
	}

	s.startSystemLogsGC(ctx)

	// Record node start in telemetry. Get the right counter for this storage
	// engine type as well as type of start (initial boot vs restart).
	nodeStartCounter := "storage.engine."
	switch s.cfg.StorageEngine {
	case enginepb.EngineTypePebble:
		nodeStartCounter += "pebble."
	case enginepb.EngineTypeDefault:
		nodeStartCounter += "default."
	case enginepb.EngineTypeRocksDB:
		nodeStartCounter += "rocksdb."
	case enginepb.EngineTypeTeePebbleRocksDB:
		nodeStartCounter += "pebble+rocksdb."
	}
	if s.InitialBoot() {
		nodeStartCounter += "initial-boot"
	} else {
		nodeStartCounter += "restart"
	}
	telemetry.Count(nodeStartCounter)

	// Record that this node joined the cluster in the event log. Since this
	// executes a SQL query, this must be done after the SQL layer is ready.
	s.node.recordJoinEvent(timeutil.SubTimes(timeutil.Now(), s.startTime))

	// Delete all orphaned table leases created by a prior instance of this
	// node.
	s.leaseMgr.DeleteOrphanedLeases(timeThreshold)

	// Start scheduled jobs daemon.
	jobs.StartJobSchedulerDaemon(
		ctx,
		s.stopper,
		&scheduledjobs.JobExecutionConfig{
			Settings:         s.execCfg.Settings,
			InternalExecutor: s.internalExecutor,
			DB:               s.execCfg.DB,
			TestingKnobs:     s.cfg.TestingKnobs.JobsTestingKnobs,
			PlanHookMaker: func(opName string, txn *client.Txn, user string) (interface{}, func()) {
				// This is a hack to get around a Go package dependency cycle. See comment
				// in sql/jobs/registry.go on planHookMaker.
				return sql.NewInternalPlanner(opName, txn, user, &sql.MemoryMetrics{}, s.execCfg)
			},
		},
		scheduledjobs.ProdJobSchedulerEnv,
	)

	// start audit server
	s.auditServer.Start(ctx, s.stopper)

	log.Event(ctx, "server ready")

	return nil
}

func (s *Server) bootstrapCluster(ctx context.Context) error {
	bootstrapVersion := s.cfg.Settings.Version.BootstrapVersion()
	if s.cfg.TestingKnobs.Store != nil {
		if storeKnobs, ok := s.cfg.TestingKnobs.Store.(*storage.StoreTestingKnobs); ok && storeKnobs.BootstrapVersion != nil {
			bootstrapVersion = *storeKnobs.BootstrapVersion
		}
	}

	if err := s.node.bootstrap(ctx, s.engines, bootstrapVersion); err != nil {
		return err
	}
	// Force all the system ranges through the replication queue so they
	// upreplicate as quickly as possible when a new node joins. Without this
	// code, the upreplication would be up to the whim of the scanner, which
	// might be too slow for new clusters.
	done := false
	return s.node.stores.VisitStores(func(store *storage.Store) error {
		if !done {
			done = true
			return store.ForceReplicationScanAndProcess()
		}
		return nil
	})
}

func (s *Server) doDrain(
	ctx context.Context, modes []serverpb.DrainMode, setTo bool,
) ([]serverpb.DrainMode, error) {
	for _, mode := range modes {
		switch mode {
		case serverpb.DrainMode_CLIENT:
			if setTo {
				s.grpc.setMode(modeDraining)
				// Wait for drainUnreadyWait. This will fail load balancer checks and
				// delay draining so that client traffic can move off this node.
				time.Sleep(drainWait.Get(&s.st.SV))
			}
			if err := func() error {
				if !setTo {
					// Execute this last.
					defer func() { s.grpc.setMode(modeOperational) }()
				}
				// Since enabling the lease manager's draining mode will prevent
				// the acquisition of new leases, the switch must be made after
				// the pgServer has given sessions a chance to finish ongoing
				// work.
				defer s.leaseMgr.SetDraining(setTo)

				if !setTo {
					s.distSQLServer.Undrain(ctx)
					s.pgServer.Undrain()
					return nil
				}

				drainMaxWait := queryWait.Get(&s.st.SV)
				if err := s.pgServer.Drain(drainMaxWait); err != nil {
					return err
				}
				s.distSQLServer.Drain(ctx, drainMaxWait)
				return nil
			}(); err != nil {
				return nil, err
			}
		case serverpb.DrainMode_LEASES:
			s.nodeLiveness.SetDraining(ctx, setTo)
			if err := s.node.SetDraining(setTo); err != nil {
				return nil, err
			}
		default:
			return nil, errors.Errorf("unknown drain mode: %s", mode)
		}
	}
	var nowOn []serverpb.DrainMode
	if s.pgServer.IsDraining() {
		nowOn = append(nowOn, serverpb.DrainMode_CLIENT)
	}
	if s.node.IsDraining() {
		nowOn = append(nowOn, serverpb.DrainMode_LEASES)
	}
	return nowOn, nil
}

// Drain idempotently activates the given DrainModes on the Server in the order
// in which they are supplied.
// For example, Drain is typically called with [CLIENT,LEADERSHIP] before
// terminating the process for graceful shutdown.
// On success, returns all active drain modes after carrying out the request.
// On failure, the system may be in a partially drained state and should be
// recovered by calling Undrain() with the same (or a larger) slice of modes.
func (s *Server) Drain(ctx context.Context, on []serverpb.DrainMode) ([]serverpb.DrainMode, error) {
	return s.doDrain(ctx, on, true)
}

// Undrain idempotently deactivates the given DrainModes on the Server in the
// order in which they are supplied.
// On success, returns any remaining active drain modes.
func (s *Server) Undrain(ctx context.Context, off []serverpb.DrainMode) []serverpb.DrainMode {
	nowActive, err := s.doDrain(ctx, off, false)
	if err != nil {
		panic(fmt.Sprintf("error returned to Undrain: %s", err))
	}
	return nowActive
}

// Decommission idempotently sets the decommissioning flag for specified nodes.
func (s *Server) Decommission(ctx context.Context, setTo bool, nodeIDs []roachpb.NodeID) error {
	start := timeutil.Now()
	eventType := sql.EventLogNodeDecommissioned
	if !setTo {
		eventType = sql.EventLogNodeRecommissioned
	}

	ci := server.ClientInfo{}
	if st, ok := grpc.ServerTransportStreamFromContext(ctx).(*transport.Stream); ok {
		ci.Client = st.ServerTransport().RemoteAddr().String()
	}

	for _, nodeID := range nodeIDs {
		changeCommitted, err := s.nodeLiveness.SetDecommissioning(ctx, nodeID, setTo)
		if err != nil {
			return errors.Wrapf(err, "during liveness update %d -> %t", nodeID, setTo)
		}
		if changeCommitted {
			// If we die right now or if this transaction fails to commit, the
			// commissioning event will not be recorded to the event log. While we
			// could insert the event record in the same transaction as the liveness
			// update, this would force a 2PC and potentially leave write intents in
			// the node liveness range. Better to make the event logging best effort
			// than to slow down future node liveness transactions.
			var nd *roachpb.NodeDescriptor
			if nd, err = s.gossip.GetNodeDescriptor(nodeID); err != nil {
				log.Warningf(ctx, "error when got node:%d descriptor, err:%s", nodeID, err)
			}
			if err := s.auditServer.LogAudit(
				ctx,
				false,
				&server.AuditInfo{
					EventTime:    timeutil.Now(),
					EventType:    string(eventType),
					ClientInfo:   &ci,
					TargetInfo:   &server.TargetInfo{TargetID: int32(nodeID)},
					Opt:          string(eventType),
					OptTime:      timeutil.SubTimes(timeutil.Now(), start),
					AffectedSize: 1,
					Result:       "OK",
					Ctx:          ctx,
					Info:         nd.String(),
				},
			); err != nil {
				log.Errorf(ctx, "unable to record %s event for node %d: %s", eventType, nodeID, err)
			}
		}
	}
	return nil
}

// startSampleEnvironment begins the heap profiler worker and a worker that
// periodically instructs the runtime stat sampler to sample the environment.
func (s *Server) startSampleEnvironment(frequency time.Duration) {
	// Immediately record summaries once on server startup.
	ctx := s.AnnotateCtx(context.Background())
	goroutineDumper, err := goroutinedumper.NewGoroutineDumper(s.cfg.GoroutineDumpDirName)
	if err != nil {
		log.Infof(ctx, "Could not start goroutine dumper worker due to: %s", err)
	}
	var heapProfiler *heapprofiler.HeapProfiler

	{
		systemMemory, err := status.GetTotalMemory(ctx)
		if err != nil {
			log.Warningf(ctx, "Could not compute system memory due to: %s", err)
		} else {
			heapProfiler, err = heapprofiler.NewHeapProfiler(s.cfg.HeapProfileDirName, systemMemory)
			if err != nil {
				log.Infof(ctx, "Could not start heap profiler worker due to: %s", err)
			}
		}
	}

	// We run two separate sampling loops, one for memory stats (via
	// ReadMemStats) and one for all other runtime stats. This is necessary
	// because as of go1.11, runtime.ReadMemStats() "stops the world" and
	// requires waiting for any current GC run to finish. With a large heap, a
	// single GC run may take longer than the default sampling period (10s).
	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(frequency)
		for {
			select {
			case <-timer.C:
				timer.Read = true
				s.runtime.SampleMemStats(ctx)
				timer.Reset(frequency)
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})

	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(frequency)
		for {
			select {
			case <-timer.C:
				timer.Read = true
				s.runtime.SampleEnvironment(ctx)
				if goroutineDumper != nil {
					goroutineDumper.MaybeDump(ctx, s.ClusterSettings(), s.runtime.Goroutines.Value())
				}
				if heapProfiler != nil {
					heapProfiler.MaybeTakeProfile(ctx, s.ClusterSettings(), s.runtime.Rss.Value())
				}
				timer.Reset(frequency)
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}

// Stop stops the server.
func (s *Server) Stop() {
	s.stopper.Stop(context.TODO())
}

// ServeHTTP is necessary to implement the http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// This is our base handler, so catch all panics and make sure they stick.
	defer log.FatalOnPanic()

	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	ae := r.Header.Get(httputil.AcceptEncodingHeader)
	switch {
	case strings.Contains(ae, httputil.GzipEncoding):
		w.Header().Set(httputil.ContentEncodingHeader, httputil.GzipEncoding)
		gzw := newGzipResponseWriter(w)
		defer func() {
			// Certain requests must not have a body, yet closing the gzip writer will
			// attempt to write the gzip header. Avoid logging a warning in this case.
			// This is notably triggered by:
			//
			// curl -H 'Accept-Encoding: gzip' \
			// 	    -H 'If-Modified-Since: Thu, 29 Mar 2018 22:36:32 GMT' \
			//      -v http://localhost:8080/favicon.ico > /dev/null
			//
			// which results in a 304 Not Modified.
			if err := gzw.Close(); err != nil && err != http.ErrBodyNotAllowed {
				ctx := s.AnnotateCtx(r.Context())
				log.Warningf(ctx, "error closing gzip response writer: %v", err)
			}
		}()
		w = gzw
	}
	s.mux.ServeHTTP(w, r)
}

// TempDir returns the filepath of the temporary directory used for temp storage.
// It is empty for an in-memory temp storage.
func (s *Server) TempDir() string {
	return s.cfg.TempStorageConfig.Path
}

// PGServer exports the pgwire server. Used by tests.
func (s *Server) PGServer() *pgwire.Server {
	return s.pgServer
}

//SessionCancelJob start background thread for periodically cancel session when it is timeout.
func (s *Server) SessionCancelJob(stopper *stop.Stopper) error {
	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		timer := time.NewTimer(time.Minute * 10)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				s.sessionRegistry.Lock()
				sessions := s.sessionRegistry.GetSessions()
				for clusterWideID, registrySession := range sessions {
					conn := registrySession.GetConnExecutor(registrySession)
					sessionData := conn.GetSessionData()
					phaseTime := conn.GetPhaseTimes()
					timeToCancel := timeutil.Now().Sub(phaseTime[len(phaseTime)-1]).Hours()
					applicationName := sessionData.ApplicationName
					find := strings.Contains(applicationName, "$ internal")

					if !find && sessionData.CloseSessionTimeout > 0 && timeToCancel > float64(sessionData.CloseSessionTimeout) {
						if phaseTime[len(phaseTime)-1].Sub(phaseTime[len(phaseTime)-2]) > 0 && sessionData.ApplicationName == "$ znbase sql" {
							s.sessionRegistry.Unlock()
							_, _ = s.sessionRegistry.CancelSession(
								clusterWideID.GetBytes(),
								s.sessionRegistry.GetUserName(registrySession),
							)
							s.sessionRegistry.Lock()
						}
					}
				}
				timer.Reset(time.Minute * 10)
				s.sessionRegistry.Unlock()
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	return nil
}

// TODO(benesch): Use https://github.com/NYTimes/gziphandler instead.
// gzipResponseWriter reinvents the wheel and is not as robust.
type gzipResponseWriter struct {
	gz gzip.Writer
	http.ResponseWriter
}

func newGzipResponseWriter(rw http.ResponseWriter) *gzipResponseWriter {
	var w *gzipResponseWriter
	if wI := gzipResponseWriterPool.Get(); wI == nil {
		w = new(gzipResponseWriter)
	} else {
		w = wI.(*gzipResponseWriter)
	}
	w.Reset(rw)
	return w
}

func (w *gzipResponseWriter) Reset(rw http.ResponseWriter) {
	w.gz.Reset(rw)
	w.ResponseWriter = rw
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	// The underlying http.ResponseWriter can't sniff gzipped data properly, so we
	// do our own sniffing on the uncompressed data.
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", http.DetectContentType(b))
	}
	return w.gz.Write(b)
}

// Flush implements http.Flusher as required by grpc-gateway for clients
// which access streaming endpoints (as exercised by the acceptance tests
// at time of writing).
func (w *gzipResponseWriter) Flush() {
	// If Flush returns an error, we'll see it on the next call to Write or
	// Close as well, so we can ignore it here.
	if err := w.gz.Flush(); err == nil {
		// Flush the wrapped ResponseWriter as well, if possible.
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// Close implements the io.Closer interface. It is not safe to use the
// writer after calling Close.
func (w *gzipResponseWriter) Close() error {
	err := w.gz.Close()
	w.Reset(nil) // release ResponseWriter reference.
	gzipResponseWriterPool.Put(w)
	return err
}

func init() {
	tracing.RegisterTagRemapping("n", "node")
}
