// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqltelemetry"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/envutil"
	"github.com/znbasedb/znbase/pkg/util/errorutil"
	"github.com/znbasedb/znbase/pkg/util/fsm"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
	"golang.org/x/net/trace"
)

// noteworthyMemoryUsageBytes is the minimum size tracked by a
// transaction or session monitor before the monitor starts explicitly
// logging overall usage growth in the log.
var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("ZNBASE_NOTEWORTHY_SESSION_MEMORY_USAGE", 1024*1024)

// A connExecutor is in charge of executing queries received on a given client
// connection. The connExecutor implements a state machine (dictated by the
// Postgres/pgwire session semantics). The state machine is supposed to run
// asynchronously wrt the client connection: it receives input statements
// through a stmtBuf and produces results through a clientComm interface. The
// connExecutor maintains a cursor over the statementBuffer and executes
// statements / produces results for one statement at a time. The cursor points
// at all times to the statement that the connExecutor is currently executing.
// Results for statements before the cursor have already been produced (but not
// necessarily delivered to the client). Statements after the cursor are queued
// for future execution. Keeping already executed statements in the buffer is
// useful in case of automatic retries (in which case statements from the
// retried transaction have to be executed again); the connExecutor is in charge
// of removing old statements that are no longer needed for retries from the
// (head of the) buffer. Separately, the implementer of the clientComm interface
// (e.g. the pgwire module) is in charge of keeping track of what results have
// been delivered to the client and what results haven't (yet).
//
// The connExecutor has two main responsibilities: to dispatch queries to the
// execution engine(s) and relay their results to the clientComm, and to
// implement the state machine maintaining the various aspects of a connection's
// state. The state machine implementation is further divided into two aspects:
// maintaining the transaction status of the connection (outside of a txn,
// inside a txn, in an aborted txn, in a txn awaiting client restart, etc.) and
// maintaining the cursor position (i.e. correctly jumping to whatever the
// "next" statement to execute is in various situations).
//
// The cursor normally advances one statement at a time, but it can also skip
// some statements (remaining statements in a query string are skipped once an
// error is encountered) and it can sometimes be rewound when performing
// automatic retries. Rewinding can only be done if results for the rewound
// statements have not actually been delivered to the client; see below.
//
//                                                   +---------------------+
//                                                   |connExecutor         |
//                                                   |                     |
//                                                   +->execution+--------------+
//                                                   ||  +                 |    |
//                                                   ||  |fsm.Event        |    |
//                                                   ||  |                 |    |
//                                                   ||  v                 |    |
//                                                   ||  fsm.Machine(TxnStateTransitions)
//                                                   ||  +  +--------+     |    |
//      +--------------------+                       ||  |  |txnState|     |    |
//      |stmtBuf             |                       ||  |  +--------+     |    |
//      |                    | statements are read   ||  |                 |    |
//      | +-+-+ +-+-+ +-+-+  +------------------------+  |                 |    |
//      | | | | | | | | | |  |                       |   |   +-------------+    |
//  +---> +-+-+ +++-+ +-+-+  |                       |   |   |session data |    |
//  |   |        ^           |                       |   |   +-------------+    |
//  |   |        |   +-----------------------------------+                 |    |
//  |   |        +   v       | cursor is advanced    |  advanceInfo        |    |
//  |   |       cursor       |                       |                     |    |
//  |   +--------------------+                       +---------------------+    |
//  |                                                                           |
//  |                                                                           |
//  +-------------+                                                             |
//                +--------+                                                    |
//                | parser |                                                    |
//                +--------+                                                    |
//                |                                                             |
//                |                                                             |
//                |                                   +----------------+        |
//        +-------+------+                            |execution engine<--------+
//        | pgwire conn  |               +------------+(local/DistSQL) |
//        |              |               |            +----------------+
//        |   +----------+               |
//        |   |clientComm<---------------+
//        |   +----------+           results are produced
//        |              |
//        +-------^------+
//                |
//                |
//        +-------+------+
//        | SQL client   |
//        +--------------+
//
// The connExecutor is disconnected from client communication (i.e. generally
// network communication - i.e. pgwire.conn); the module doing client
// communication is responsible for pushing statements into the buffer and for
// providing an implementation of the clientConn interface (and thus sending
// results to the client). The connExecutor does not control when
// results are delivered to the client, but still it does have some influence
// over that; this is because of the fact that the possibility of doing
// automatic retries goes away the moment results for the transaction in
// question are delivered to the client. The communication module has full
// freedom in sending results whenever it sees fit; however the connExecutor
// influences communication in the following ways:
//
// a) When deciding whether an automatic retry can be performed for a
// transaction, the connExecutor needs to:
//
//   1) query the communication status to check that no results for the txn have
//   been delivered to the client and, if this check passes:
//   2) lock the communication so that no further results are delivered to the
//   client, and, eventually:
//   3) Rewind the clientComm to a certain position corresponding to the start
//   of the transaction, thereby discarding all the results that had been
//   accumulated for the previous attempt to run the transaction in question.
//
// These steps are all orchestrated through clientComm.lockCommunication() and
// rewindCapability{}.
//
// b) The connExecutor sometimes ask the clientComm to deliver everything
// (most commonly in response to a Sync command).
//
// As of Feb 2018, the pgwire.conn delivers results synchronously to the client
// when its internal buffer overflows. In principle, delivery of result could be
// done asynchronously wrt the processing of commands (e.g. we could have a
// timing policy in addition to the buffer size). The first implementation of
// that showed a performance impact of involving a channel communication in the
// Sync processing path.
//
//
// Implementation notes:
//
// --- Error handling ---
//
// The key to understanding how the connExecutor handles errors is understanding
// the fact that there's two distinct categories of errors to speak of. There
// are "query execution errors" and there are the rest. Most things fall in the
// former category: invalid queries, queries that fail constraints at runtime,
// data unavailability errors, retriable errors (i.e. serializability
// violations) "internal errors" (e.g. connection problems in the cluster). This
// category of errors doesn't represent dramatic events as far as the connExecutor
// is concerned: they produce "results" for the query to be passed to the client
// just like more successful queries do and they produce Events for the
// state machine just like the successful queries (the events in question
// are generally event{non}RetriableErr and they generally cause the
// state machine to move to the Aborted state, but the connExecutor doesn't
// concern itself with this). The way the connExecutor reacts to these errors is
// the same as how it reacts to a successful query completing: it moves the
// cursor over the incoming statements as instructed by the state machine and
// continues running statements.
//
// And then there's other errors that don't have anything to do with a
// particular query, but with the connExecutor itself. In other languages, these
// would perhaps be modeled as Exceptions: we want them to unwind the stack
// significantly. These errors cause the connExecutor.run() to break out of its
// loop and return an error. Example of such errors include errors in
// communication with the client (e.g. the network connection is broken) or the
// connection's context being canceled.
//
// All of connExecutor's methods only return errors for the 2nd category. Query
// execution errors are written to a CommandResult. Low-level methods don't
// operate on a CommandResult directly; instead they operate on a wrapper
// (resultWithStoredErr), which provides access to the query error for purposes
// of building the correct state machine event.
//
// --- Context management ---
//
// At the highest level, there's connExecutor.run() that takes a context. That
// context is supposed to represent "the connection's context": its lifetime is
// the client connection's lifetime and it is assigned to
// connEx.ctxHolder.connCtx. Below that, every SQL transaction has its own
// derived context because that's the level at which we trace operations. The
// lifetime of SQL transactions is determined by the txnState: the state machine
// decides when transactions start and end in txnState.performStateTransition().
// When we're inside a SQL transaction, most operations are considered to happen
// in the context of that txn. When there's no SQL transaction (i.e.
// stateNoTxn), everything happens in the connection's context.
//
// High-level code in connExecutor is agnostic of whether it currently is inside
// a txn or not. To deal with both cases, such methods don't explicitly take a
// context; instead they use connEx.Ctx(), which returns the appropriate ctx
// based on the current state.
// Lower-level code (everything from connEx.execStmt() and below which runs in
// between state transitions) knows what state its running in, and so the usual
// pattern of explicitly taking a context as an argument is used.

// Server is the top level singleton for handling SQL connections. It creates
// connExecutors to server every incoming connection.
type Server struct {
	_ util.NoCopy

	cfg *ExecutorConfig

	// sqlStats tracks per-application statistics for all applications on each
	// node.
	sqlStats sqlStats

	reCache *tree.RegexpCache

	// pool is the parent monitor for all session monitors except "internal" ones.
	pool *mon.BytesMonitor

	// Metrics is used to account normal queries.
	Metrics Metrics

	// InternalMetrics is used to account internal queries.
	InternalMetrics Metrics

	// dbCache is a cache for database descriptors, maintained through Gossip
	// updates.
	dbCache *databaseCacheHolder
}

// Metrics collects timeseries data about SQL activity.
type Metrics struct {
	// EngineMetrics is exported as required by the metrics.Struct magic we use
	// for metrics registration.
	EngineMetrics EngineMetrics

	StatementCounters StatementCounters
}

// NewServer creates a new Server. Start() needs to be called before the Server
// is used.
func NewServer(cfg *ExecutorConfig, pool *mon.BytesMonitor) *Server {
	c := config.NewSystemConfig()
	return &Server{
		cfg:             cfg,
		Metrics:         makeMetrics(false /*internal*/),
		InternalMetrics: makeMetrics(true /*internal*/),
		// dbCache will be updated on Start().
		dbCache:  newDatabaseCacheHolder(newDatabaseCache(c), newSchemaCache(c)),
		pool:     pool,
		sqlStats: sqlStats{st: cfg.Settings, apps: make(map[string]*appStats)},
		reCache:  tree.NewRegexpCache(512),
	}
}

func makeMetrics(internal bool) Metrics {
	return Metrics{
		EngineMetrics: EngineMetrics{
			DistSQLSelectCount:    metric.NewCounter(getMetricMeta(MetaDistSQLSelect, internal)),
			SQLOptCount:           metric.NewCounter(getMetricMeta(MetaSQLOpt, internal)),
			SQLOptFallbackCount:   metric.NewCounter(getMetricMeta(MetaSQLOptFallback, internal)),
			SQLOptPlanCacheHits:   metric.NewCounter(getMetricMeta(MetaSQLOptPlanCacheHits, internal)),
			SQLOptPlanCacheMisses: metric.NewCounter(getMetricMeta(MetaSQLOptPlanCacheMisses, internal)),

			// TODO(mrtracy): See HistogramWindowInterval in server/config.go for the 6x factor.
			DistSQLExecLatency: metric.NewLatency(getMetricMeta(MetaDistSQLExecLatency, internal),
				6*metricsSampleInterval),
			SQLExecLatency: metric.NewLatency(getMetricMeta(MetaSQLExecLatency, internal),
				6*metricsSampleInterval),
			DistSQLServiceLatency: metric.NewLatency(getMetricMeta(MetaDistSQLServiceLatency, internal),
				6*metricsSampleInterval),
			SQLServiceLatency: metric.NewLatency(getMetricMeta(MetaSQLServiceLatency, internal),
				6*metricsSampleInterval),

			TxnAbortCount: metric.NewCounter(getMetricMeta(MetaTxnAbort, internal)),
			FailureCount:  metric.NewCounter(getMetricMeta(MetaFailure, internal)),
		},
		StatementCounters: makeStatementCounters(internal),
	}
}

// Start starts the Server's background processing.
func (s *Server) Start(ctx context.Context, stopper *stop.Stopper) {
	gossipUpdateC := s.cfg.Gossip.RegisterSystemConfigChannel()
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case <-gossipUpdateC:
				sysCfg := s.cfg.Gossip.GetSystemConfig()
				s.dbCache.updateSystemConfig(sysCfg)
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	s.PeriodicallyClearStmtStats(ctx, stopper)
}

// ResetStatementStats resets the executor's collected statement statistics.
func (s *Server) ResetStatementStats(ctx context.Context) {
	s.sqlStats.resetStats(ctx)
}

// GetScrubbedStmtStats returns the statement statistics by app, with the
// queries scrubbed of their identifiers. Any statements which cannot be
// scrubbed will be omitted from the returned map.
func (s *Server) GetScrubbedStmtStats() []roachpb.CollectedStatementStatistics {
	return s.sqlStats.getScrubbedStmtStats(s.cfg.VirtualSchemas)
}

// GetUnscrubbedStmtStats returns the same thing as GetScrubbedStmtStats, except
// identifiers (e.g. table and column names) aren't scrubbed from the statements.
func (s *Server) GetUnscrubbedStmtStats() []roachpb.CollectedStatementStatistics {
	return s.sqlStats.getUnscrubbedStmtStats(s.cfg.VirtualSchemas)
}

// GetStmtStatsLastReset returns the time at which the statement statistics were
// last cleared.
func (s *Server) GetStmtStatsLastReset() time.Time {
	return s.sqlStats.getLastReset()
}

// GetExecutorConfig returns this server's executor config.
func (s *Server) GetExecutorConfig() *ExecutorConfig {
	if s == nil {
		return nil
	}
	return s.cfg
}

// SetupConn creates a connExecutor for the client connection.
//
// When this method returns there are no resources allocated yet that
// need to be close()d.
//
// Args:
// args: The initial session parameters. They are validated by SetupConn
//   and an error is returned if this validation fails.
// stmtBuf: The incoming statement for the new connExecutor.
// clientComm: The interface through which the new connExecutor is going to
// 	 produce results for the client.
// memMetrics: The metrics that statements executed on this connection will
//   contribute to.
func (s *Server) SetupConn(
	ctx context.Context,
	args *SessionArgs,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	memMetrics MemoryMetrics,
) (ConnectionHandler, error) {
	sd, sdMut := s.newSessionDataAndMutator(args)
	ex, err := s.newConnExecutor(ctx, sd, sdMut, stmtBuf, clientComm, memMetrics, &s.Metrics)
	return ConnectionHandler{ex}, err
}

// ConnectionHandler is the interface between the result of SetupConn
// and the ServeConn below. It encapsulates the connExecutor and hides
// it away from other packages.
type ConnectionHandler struct {
	ex *connExecutor
}

// GetClientEncoding is the func that get client_encoding session
func (h ConnectionHandler) GetClientEncoding() string {
	if h.ex == nil {
		return "utf8"
	}
	if h.ex.dataMutator == nil {
		return "utf8"
	}
	if h.ex.dataMutator.data == nil {
		return "utf8"
	}
	return h.ex.dataMutator.data.ClientEncoding
}

// GetDefaultIntSize implements pgwire.sessionDataProvider and returns
// the type that INT should be parsed as.
func (h ConnectionHandler) GetDefaultIntSize() *coltypes.TInt {
	var size int
	if h.ex != nil {
		// The executor will be nil in certain testing situations where
		// no server is actually present.
		size = h.ex.sessionData.DefaultIntSize
	}
	switch size {
	case 4, 32:
		return coltypes.Int4
	default:
		return coltypes.Int8
	}
}

// GetStatusParam retrieves the configured value of the session
// variable identified by varName. This is used for the initial
// message sent to a client during a session set-up.
func (h ConnectionHandler) GetStatusParam(ctx context.Context, varName string) string {
	name := strings.ToLower(varName)
	v, ok := varGen[name]
	if !ok {
		log.Fatalf(ctx, "programming error: status param %q must be defined session var", varName)
		return ""
	}
	hasDefault, defVal := getSessionVarDefaultString(name, v, h.ex.dataMutator)
	if !hasDefault {
		log.Fatalf(ctx, "programming error: status param %q must have a default value", varName)
		return ""
	}
	return defVal
}

// GetCaseSensitive 返回大小写敏感开关的状态
func (h ConnectionHandler) GetCaseSensitive() bool {
	if h.ex != nil {
		return h.ex.getCaseSensitive()
	}
	return false
}

//代表GetSearchPathList解析字符串时的六种状态
const (
	start int = iota
	simpleStr
	quoteStr
	success
	tempState
	panicState
)

//GetSearchPathList 通过字符串src，返回search_path
//解决要设置的search_path存在非法字符','的问题
//例如：src 为 `s1, s2, "s,3", s4` 返回 字符串切片[s1 s2 s,3 s4]
func GetSearchPathList(src string) []string {
	res := make([]string, 0)
	state := start
	temp := ""
	src += " "
	for _, v := range src {
		switch state {
		case start:
			switch v {
			case ' ':
				state = start
			case ',':
				state = panicState
			case '"':
				state = quoteStr
			default:
				temp += string(v)
				state = simpleStr
			}
		case simpleStr:
			switch v {
			case ' ':
				res = append(res, temp)
				temp = ""
				state = success
			case ',':
				res = append(res, temp)
				temp = ""
				state = tempState
			case '"':
				state = panicState
			default:
				temp += string(v)
			}
		case quoteStr:
			switch v {
			case '"':
				res = append(res, temp)
				temp = ""
				state = success
			default:
				temp += string(v)
			}
		case success:
			switch v {
			case ' ':
				continue
			case ',':
				state = tempState
			default:
				state = panicState
			}
		case tempState:
			switch v {
			case ' ':
				continue
			case '"':
				state = quoteStr
			case ',':
				state = panicState
			default:
				temp += string(v)
				state = simpleStr
			}
		case panicState:
			panic("输入的search_path不符合规范")
		}
	}
	if state == start || state == success {
		return res
	}
	panic("字符串结尾是其他特殊字符")
}

// ServeConn serves a client connection by reading commands from
// the stmtBuf embedded in the ConnHandler.
func (s *Server) ServeConn(
	ctx context.Context, h ConnectionHandler, reserved mon.BoundAccount, cancel context.CancelFunc,
) error {
	defer func() {
		r := recover()
		h.ex.closeWrapper(ctx, r)
	}()
	ex := h.ex
	userDefaultSearchPath := make([]string, 0)
	mapDbSearchPath := make(map[string]string)
	ex.sessionData.DBDefaultSearchPath = make(map[uint32][]string)
	if ex.planner.execCfg.InternalExecutor != nil {
		searchPaths, err := ex.planner.execCfg.InternalExecutor.QueryRow(ctx, "select-user-dbsearchPath", nil,
			fmt.Sprintf(`select usersearchpath, dbsearchpath from system.users where username = '%s'`, ex.planner.User()))
		if err != nil || len(searchPaths) != 2 {
			return fmt.Errorf("can not find this user")
		}
		userSearchPath := searchPaths[0]
		dbSearchPath := searchPaths[1]
		if userSearchPathArray, ok := userSearchPath.(*tree.DArray); ok {
			for _, value := range userSearchPathArray.Array {
				userDefaultSearchPath = append(userDefaultSearchPath, string(tree.MustBeDString(value)))
			}
		}
		//searchPath 存储要赋值给当前session的search_path
		searchPath := make([]string, len(userDefaultSearchPath))
		copy(searchPath, userDefaultSearchPath)
		//将json类型字符串转换为map
		if dbSearchPath, ok := dbSearchPath.(*tree.DString); ok {
			if "NULL" != string(*dbSearchPath) {
				err := json.Unmarshal([]byte(*dbSearchPath), &mapDbSearchPath)
				if err != nil {
					return fmt.Errorf(fmt.Sprintf(`dbsearchpath Incorrect format at row %s in system.users, expected like {"1":"public",}`,
						ex.planner.User()))
				}
			}
		}
		//获得当前数据库ID
		var dbID sqlbase.ID
		err = ex.server.cfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error { //解决txn错误
			dbID, err = getDatabaseID(ctx, txn, ex.sessionData.Database, true)
			if err != nil {
				return err
			}
			return nil
		})
		//将users表中的dbsearchpath存储到sessiondata中,
		for k, v := range mapDbSearchPath {
			k, err := strconv.ParseUint(k, 10, 32)
			if err != nil {
				return err
			}
			ex.sessionData.DBDefaultSearchPath[uint32(k)] = GetSearchPathList(v)
			if uint32(dbID) == uint32(k) { //判断users表的dbsearchpath中是否设置了初始数据库
				searchPath = GetSearchPathList(v)
			}
		}
		ex.sessionData.UserDefaultSearchPath = userDefaultSearchPath
		ex.sessionData.SearchPath = sessiondata.MakeSearchPath(searchPath)
		sqlbase.DefaultSearchPath = sessiondata.MakeSearchPath(searchPath)
	}
	return h.ex.run(ctx, s.pool, reserved, cancel)
}

// newSessionDataAndMutator creates a SessionData and sessionDataMutator that
// can be passed to newConnExecutor.
func (s *Server) newSessionDataAndMutator(
	args *SessionArgs,
) (*sessiondata.SessionData, *sessionDataMutator) {
	sd := &sessiondata.SessionData{
		User:          args.User,
		RemoteAddr:    args.RemoteAddr,
		SequenceState: sessiondata.NewSequenceState(),
		DataConversion: sessiondata.DataConversionConfig{
			Location: time.UTC,
		},
		ResultsBufferSize:   args.ConnResultsBufferSize,
		SessionStatus:       sessiondata.SessionIDLE,
		CloseSessionTimeout: SQLSessionTimeout.Get(&s.cfg.Settings.SV),
	}

	m := &sessionDataMutator{
		data:     sd,
		defaults: SessionDefaults{SessionDefaultsMp: args.SessionDefaults.SessionDefaultsMp},
		settings: s.cfg.Settings,
	}

	return sd, m
}

// newConnExecutor creates a new connExecutor.
//
// sdMutator can be nil if SET statements are not going to be executed; this is
// appropriate for session-bound internal executors.
func (s *Server) newConnExecutor(
	ctx context.Context,
	sd *sessiondata.SessionData,
	sdMutator *sessionDataMutator,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	memMetrics MemoryMetrics,
	srvMetrics *Metrics,
) (*connExecutor, error) {
	// Create the various monitors.
	// The session monitors are started in activate().
	sessionRootMon := mon.MakeMonitor(
		"session root",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		-1 /* increment */, math.MaxInt64, s.cfg.Settings,
	)
	sessionMon := mon.MakeMonitor(
		"session",
		mon.MemoryResource,
		memMetrics.SessionCurBytesCount,
		memMetrics.SessionMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes, s.cfg.Settings,
	)
	// The txn monitor is started in txnState.resetForNewSQLTxn().
	txnMon := mon.MakeMonitor(
		"txn",
		mon.MemoryResource,
		memMetrics.TxnCurBytesCount,
		memMetrics.TxnMaxBytesHist,
		-1 /* increment */, noteworthyMemoryUsageBytes, s.cfg.Settings,
	)

	ex := &connExecutor{
		server:      s,
		metrics:     srvMetrics,
		stmtBuf:     stmtBuf,
		clientComm:  clientComm,
		mon:         &sessionRootMon,
		sessionMon:  &sessionMon,
		sessionData: sd,
		dataMutator: sdMutator,
		state: txnState{
			settings: s.cfg.Settings,
			mon:      &txnMon,
			connCtx:  ctx,
		},
		transitionCtx: transitionCtx{
			db:     s.cfg.DB,
			nodeID: s.cfg.NodeID.Get(),
			clock:  s.cfg.Clock,
			// Future transaction's monitors will inherits from sessionRootMon.
			connMon:  &sessionRootMon,
			tracer:   s.cfg.AmbientCtx.Tracer,
			settings: s.cfg.Settings,
		},
		parallelizeQueue: MakeParallelizeQueue(NewSpanBasedDependencyAnalyzer()),
		memMetrics:       memMetrics,
		planner:          planner{execCfg: s.cfg},

		// ctxHolder will be reset at the start of run(). We only define
		// it here so that an early call to close() doesn't panic.
		ctxHolder: ctxHolder{connCtx: ctx},
	}

	ex.state.txnAbortCount = ex.metrics.EngineMetrics.TxnAbortCount

	if sdMutator != nil {
		sdMutator.setCurTxnReadOnly = func(val bool) {
			ex.state.readOnly = val
		}

		sdMutator.setCurTxnSupDDL = func(val bool) {
			ex.state.supDDL = val
		}

		sdMutator.applicationNameChanged = func(newName string) {
			ex.appStats = ex.server.sqlStats.getStatsForApplication(newName)
			ex.applicationName.Store(newName)
		}

		sdMutator.onTempSchemaCreation = func() {
			ex.hasCreatedTemporarySchema = true
		}

		// Initialize the session data from provided defaults. We need to do this early
		// because other initializations below use the configured values.
		if err := resetSessionVars(ctx, sdMutator); err != nil {
			log.Errorf(ctx, "error setting up client session: %v", err)
			return nil, err
		}
	} else {
		// We have set the ex.sessionData without using the dataMutator.
		// So we need to update the application name manually.
		ex.applicationName.Store(ex.sessionData.ApplicationName)
		// When the connEx is serving an internal executor, it can inherit
		// the application name from an outer session. This happens
		// e.g. during ::regproc casts and built-in functions that use SQL internally.
		// In that case, we do not want to record statistics against
		// the outer application name directly; instead we want
		// to use a separate bucket. However we will still
		// want to have separate buckets for different applications so that
		// we can measure their respective "pressure" on internal queries.
		// Hence the choice here to add the delegate prefix
		// to the current app name.
		appStatsBucketName := DelegatedAppNamePrefix + ex.sessionData.ApplicationName
		ex.appStats = s.sqlStats.getStatsForApplication(appStatsBucketName)
	}

	ex.phaseTimes[sessionInit] = timeutil.Now()
	ex.extraTxnState.prepStmtsNamespace = prepStmtNamespace{
		prepStmts: make(map[string]*PreparedStatement),
		portals:   make(map[string]*PreparedPortal),
	}
	ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos = prepStmtNamespace{
		prepStmts: make(map[string]*PreparedStatement),
		portals:   make(map[string]*PreparedPortal),
	}
	ex.extraTxnState.tables = TableCollection{
		leaseMgr:          s.cfg.LeaseManager,
		databaseCache:     s.dbCache.getDatabaseCache(),
		schemaCache:       s.dbCache.getSchemaCache(),
		dbCacheSubscriber: s.dbCache,
	}
	ex.extraTxnState.txnRewindPos = -1
	ex.mu.ActiveQueries = make(map[ClusterWideID]*queryMeta)
	ex.machine = fsm.MakeMachine(TxnStateTransitions, stateNoTxn{}, &ex.state)

	ex.sessionTracing.ex = ex
	ex.transitionCtx.sessionTracing = &ex.sessionTracing
	ex.initPlanner(ctx, &ex.planner)

	return ex, nil
}

// newConnExecutorWithTxn creates a connExecutor that will execute statements
// under a higher-level txn. This connExecutor runs with a different state
// machine, much reduced from the regular one. It cannot initiate or end
// transactions (so, no BEGIN, COMMIT, ROLLBACK, no auto-commit, no automatic
// retries).
//
// If there is no error, this function also activate()s the returned
// executor, so the caller does not need to run the
// activation. However this means that run() or close() must be called
// to release resources.
func (s *Server) newConnExecutorWithTxn(
	ctx context.Context,
	sd *sessiondata.SessionData,
	sdMutator *sessionDataMutator,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	parentMon *mon.BytesMonitor,
	memMetrics MemoryMetrics,
	srvMetrics *Metrics,
	txn *client.Txn,
	tcModifier tableCollectionModifier,
) (*connExecutor, error) {
	ex, err := s.newConnExecutor(ctx, sd, sdMutator, stmtBuf, clientComm, memMetrics, srvMetrics)
	if err != nil {
		return nil, err
	}

	// The new transaction stuff below requires active monitors and traces, so
	// we need to activate the executor now.
	ex.activate(ctx, parentMon, mon.BoundAccount{})

	// Perform some surgery on the executor - replace its state machine and
	// initialize the state.
	ex.machine = fsm.MakeMachine(
		BoundTxnStateTransitions,
		stateOpen{ImplicitTxn: fsm.False},
		&ex.state,
	)
	ex.state.resetForNewSQLTxn(
		ctx,
		explicitTxn,
		txn.ReadTimestamp().GoTime(),
		nil, /* historicalTimestamp */
		tree.IsolationLevel(txn.IsolationLevel()),
		txn.UserPriority(),
		tree.ReadWrite,
		txn.DebugName(),
		txn,
		ex.transitionCtx)
	if s.cfg.LeaseManager != nil {
		ex.state.mu.txn.SetLeaseMgr(s.cfg.LeaseManager)
	}

	// Modify the TableCollection to match the parent executor's TableCollection.
	// This allows the InternalExecutor to see schema changes made by the
	// parent executor.
	if tcModifier != nil {
		tcModifier.copyModifiedSchema(&ex.extraTxnState.tables)
	}
	return ex, nil
}

var maxStmtStatReset = settings.RegisterNonNegativeDurationSetting(
	"diagnostics.forced_stat_reset.interval",
	"interval after which pending diagnostics statistics should be discarded even if not reported",
	time.Hour*2, // 2 x diagnosticReportFrequency
)

// PeriodicallyClearStmtStats runs a loop to ensure that sql stats are reset.
// Usually we expect those stats to be reset by diagnostics reporting, after it
// generates its reports. However if the diagnostics loop crashes and stops
// resetting stats, this loop ensures stats do not accumulate beyond a
// the diagnostics.forced_stat_reset.interval limit.
func (s *Server) PeriodicallyClearStmtStats(ctx context.Context, stopper *stop.Stopper) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		var timer timeutil.Timer
		for {

			s.sqlStats.Lock()
			last := s.sqlStats.lastReset
			s.sqlStats.Unlock()

			next := last.Add(maxStmtStatReset.Get(&s.cfg.Settings.SV))
			wait := next.Sub(timeutil.Now())
			if wait < 0 {
				s.ResetStatementStats(ctx)
			} else {
				timer.Reset(wait)
				select {
				case <-stopper.ShouldQuiesce():
					return
				case <-timer.C:
					timer.Read = true
				}
			}
		}
	})
}

type closeType int

const (
	normalClose closeType = iota
	panicClose
	// externalTxnClose means that the connExecutor has been used within a
	// higher-level txn (through the InternalExecutor).
	externalTxnClose
)

func (ex *connExecutor) closeWrapper(ctx context.Context, recovered interface{}) {
	p := ex.planner
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err == nil {
		//ex.close(ctx, panicClose)
		sessionID := p.extendedEvalCtx.CurrentSessionID
		// 所有curDesc key
		curRStartKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte{})
		curREndKey := curRStartKey.PrefixEnd()
		curStartKey := engine.MakeMVCCMetadataKey(curRStartKey)
		curEndKey := engine.MakeMVCCMetadataKey(curREndKey)

		// iterator func
		funcDeleteRow := func(keyVal engine.MVCCKeyValue) (stop bool, err error) {
			key := keyVal.Key
			curDesc := sqlbase.CursorDescriptor{}
			ok, _, _, err := memEngine.GetProto(key, &curDesc)
			if ok {
				// all row key
				if err := DeleteCursorRows(curDesc, memEngine); err != nil {
					return true, err
				}

			}
			return false, nil
		}

		if err := memEngine.Iterate(curStartKey, curEndKey, funcDeleteRow); err != nil {
			fmt.Println(err)
		}
		if err := memEngine.ClearRange(curStartKey, curEndKey); err != nil {
			fmt.Println(err)
		}

		// 所有variableDesc key
		varRStartKey := keys.MakeVariableMetaKey(sessionID.GetBytes(), []byte{})
		varREndKey := varRStartKey.PrefixEnd()
		varStartKey := engine.MakeMVCCMetadataKey(varRStartKey)
		varEndKey := engine.MakeMVCCMetadataKey(varREndKey)
		// iterator func
		funcDeleteVarData := func(keyVal engine.MVCCKeyValue) (stop bool, err error) {
			key := keyVal.Key
			varDesc := sqlbase.VariableDescriptor{}
			ok, _, _, err := memEngine.GetProto(key, &varDesc)
			if ok {
				// variable DataKey
				varKey := MakeVariableDataMVCCKey(uint64(varDesc.ID))
				if err := memEngine.SingleClear(varKey); err != nil {
					return true, errors.Errorf("删除数据失败 变量名：%s\n", varDesc.Name)
				}
			}
			return false, nil
		}
		if err := memEngine.Iterate(varStartKey, varEndKey, funcDeleteVarData); err != nil {
			fmt.Println(err)
		}
		if err := memEngine.ClearRange(varStartKey, varEndKey); err != nil {
			fmt.Println(err)
		}
	}

	if recovered != nil {
		// A warning header guaranteed to go to stderr. This is unanonymized.
		var cutStmt string
		var stmt string
		if ex.curStmt != nil {
			stmt = ex.curStmt.String()
			cutStmt = stmt
		}
		if len(cutStmt) > panicLogOutputCutoffChars {
			cutStmt = cutStmt[:panicLogOutputCutoffChars] + " [...]"
		}

		log.Shout(ctx, log.Severity_ERROR,
			fmt.Sprintf("a SQL panic has occurred while executing %q: %s", cutStmt, recovered))

		ex.close(ctx, panicClose)

		safeErr := AnonymizeStatementsForReporting("executing", stmt, recovered)

		log.ReportPanic(ctx, &ex.server.cfg.Settings.SV, safeErr, 1 /* depth */)

		// Propagate the (sanitized) panic further.
		// NOTE(andrei): It used to be that we sanitized the panic and then a higher
		// layer was in charge of doing the log.ReportPanic() call. Now that the
		// call is above, it's unclear whether we should propagate the original
		// panic or safeErr. I'm propagating safeErr to be on the safe side.
		// todo online change stmt to safeErr
		panic(stmt)
	}
	ex.close(ctx, normalClose)
}

func (ex *connExecutor) close(ctx context.Context, closeType closeType) {
	ex.sessionEventf(ctx, "finishing connExecutor")

	// Make sure that no statements remain in the ParallelizeQueue. If no statements
	// are in the queue, this will be a no-op. If there are statements in the
	// queue, they would have eventually drained on their own, but if we don't
	// wait here, we risk alarming the MemoryMonitor. We ignore the error because
	// it will only ever be non-nil if there are statements in the queue, meaning
	// that the Session was abandoned in the middle of a transaction, in which
	// case the error doesn't matter.
	//
	// TODO(nvanbenschoten): Once we have better support for canceling ongoing
	// statement execution by the infrastructure added to support CancelRequest,
	// we should try to actively drain this queue instead of passively waiting
	// for it to drain. (andrei, 2017/09) - We now have support for statement
	// cancellation. Now what?
	if ex.hasCreatedTemporarySchema && !ex.server.cfg.TestingKnobs.DisableTempObjectsCleanupOnSessionExit {
		err := cleanupSessionTempObjects(
			context.TODO(),
			ex.server.cfg.DB,
			ex.planner.extendedEvalCtx.InternalExecutor,
			ex.sessionID,
		)
		if err != nil {
			log.Errorf(
				ctx,
				"error deleting temporary objects at session close, "+
					"the temp tables deletion job will retry periodically: %s",
				err,
			)
		}
	}

	_ = ex.synchronizeParallelStmts(ctx)

	ev := noEvent
	if _, noTxn := ex.machine.CurState().(stateNoTxn); !noTxn {
		ev = txnAborted
	}

	if closeType == normalClose {
		// We'll cleanup the SQL txn by creating a non-retriable (commit:true) event.
		// This event is guaranteed to be accepted in every state.
		ev := eventNonRetriableErr{IsCommit: fsm.FromBool(true)}
		payload := eventNonRetriableErrPayload{err: fmt.Errorf("connExecutor closing")}
		if err := ex.machine.ApplyWithPayload(ctx, ev, payload); err != nil {
			log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
		}
	} else if closeType == externalTxnClose {
		ex.state.finishExternalTxn()
	}
	if err := ex.resetExtraTxnState(ctx, ex.server.dbCache, ev); err != nil {
		log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
	}

	if closeType != panicClose {
		// Close all statements and prepared portals.
		ex.extraTxnState.prepStmtsNamespace.resetTo(ctx, prepStmtNamespace{})
		ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetTo(ctx, prepStmtNamespace{})
	}

	if ex.sessionTracing.Enabled() {
		if err := ex.sessionTracing.StopTracing(); err != nil {
			log.Warningf(ctx, "error stopping tracing: %s", err)
		}
	}

	if ex.eventLog != nil {
		ex.eventLog.Finish()
		ex.eventLog = nil
	}

	if closeType != panicClose {
		ex.state.mon.Stop(ctx)
		ex.sessionMon.Stop(ctx)
		ex.mon.Stop(ctx)
	} else {
		ex.state.mon.EmergencyStop(ctx)
		ex.sessionMon.EmergencyStop(ctx)
		ex.mon.EmergencyStop(ctx)
	}
}

type connExecutor struct {
	_ util.NoCopy

	// The server to which this connExecutor is attached. The reference is used
	// for getting access to configuration settings.
	// Note: do not use server.Metrics directly. Use metrics below instead.
	server *Server

	// The metrics to which the statement metrics should be accounted.
	// This is different whether the executor is for regular client
	// queries or for "internal" queries.
	metrics *Metrics

	// mon tracks memory usage for SQL activity within this session. It
	// is not directly used, but rather indirectly used via sessionMon
	// and state.mon. sessionMon tracks session-bound objects like prepared
	// statements and result sets.
	//
	// The reason why state.mon and mon are split is to enable
	// separate reporting of statistics per transaction and per
	// session. This is because the "interesting" behavior w.r.t memory
	// is typically caused by transactions, not sessions. The reason why
	// sessionMon and mon are split is to enable separate reporting of
	// statistics for result sets (which escape transactions).
	mon        *mon.BytesMonitor
	sessionMon *mon.BytesMonitor
	// memMetrics contains the metrics that statements executed on this connection
	// will contribute to.
	memMetrics MemoryMetrics

	// The buffer with incoming statements to execute.
	stmtBuf *StmtBuf
	// The interface for communicating statement results to the client.
	clientComm ClientComm
	// Finity "the machine" Automaton is the state machine controlling the state
	// below.
	machine fsm.Machine
	// state encapsulates fields related to the ongoing SQL txn. It is mutated as
	// the machine's ExtendedState.
	// state封装与正在进行的SQL txn相关的字段。它作为机器的扩展状态发生突变。
	state          txnState
	transitionCtx  transitionCtx
	sessionTracing SessionTracing

	// eventLog for SQL statements and other important session events. Will be set
	// if traceSessionEventLogEnabled; it is used by ex.sessionEventf()
	eventLog trace.EventLog

	// extraTxnState groups fields scoped to a SQL txn that are not handled by
	// ex.state, above. The rule of thumb is that, if the state influences state
	// transitions, it should live in state, otherwise it can live here.
	// This is only used in the Open state. extraTxnState is reset whenever a
	// transaction finishes or gets retried.
	extraTxnState struct {
		// tables collects descriptors used by the current transaction.
		tables TableCollection

		// schemaChangers accumulate schema changes staged for execution. Staging
		// happens when executing DDL statements. The staged changes are executed once
		// the transaction that staged them commits (which is once the DDL statement
		// is done if the statement was executed in an implicit txn).
		schemaChangers schemaChangerCollection

		// autoRetryCounter keeps track of the which iteration of a transaction
		// auto-retry we're currently in. It's 0 whenever the transaction state is not
		// stateOpen.
		autoRetryCounter int

		// numDDL keeps track of how many DDL statements have been
		// executed so far.
		numDDL int

		// txnRewindPos is the position within stmtBuf to which we'll Rewind when
		// performing automatic retries. This is more or less the position where the
		// current transaction started.
		// This field is only defined while in stateOpen.
		//
		// Set via setTxnRewindPos().
		txnRewindPos CmdPos

		// prepStmtNamespace contains the prepared statements and portals that the
		// session currently has access to.
		// Portals are bound to a transaction and they're all destroyed once the
		// transaction finishes.
		// Prepared statements are not transactional and so it's a bit weird that
		// they're part of extraTxnState, but it's convenient to put them here
		// because they need the same kind of "snapshoting" as the portals (see
		// prepStmtsNamespaceAtTxnRewindPos).
		prepStmtsNamespace prepStmtNamespace

		// prepStmtsNamespaceAtTxnRewindPos is a snapshot of the prep stmts/portals
		// (ex.prepStmtsNamespace) before processing the command at position
		// txnRewindPos.
		// Here's the deal: prepared statements are not transactional, but they do
		// need to interact properly with automatic retries (i.e. rewinding the
		// command buffer). When doing a Rewind, we need to be able to restore the
		// prep stmts as they were. We do this by taking a snapshot every time
		// txnRewindPos is advanced. Prepared statements are shared between the two
		// collections, but these collections are periodically reconciled.
		prepStmtsNamespaceAtTxnRewindPos prepStmtNamespace

		// savepoints maintains the stack of savepoints currently open.
		savepoints savepointStack
		// savepointsAtTxnRewindPos is a snapshot of the savepoints stack before
		// processing the command at position txnRewindPos. When rewinding, we're
		// going to restore this snapshot.
		savepointsAtTxnRewindPos savepointStack
	}

	// sessionData contains the user-configurable connection variables.
	sessionData *sessiondata.SessionData
	// dataMutator is nil for session-bound internal executors; we shouldn't issue
	// statements that manipulate session state to an internal executor.
	dataMutator *sessionDataMutator
	// appStats tracks per-application SQL usage statistics. It is maintained to
	// represent statistrics for the application currently identified by
	// sessiondata.ApplicationName.
	appStats *appStats
	// applicationName is the same as sessionData.ApplicationName. It's copied
	// here as an atomic so that it can be read concurrently by serialize().
	applicationName atomic.Value

	// ctxHolder contains the connection's context in which all command executed
	// on the connection are running. This generally should not be used directly,
	// but through the Ctx() method; if we're inside a transaction, Ctx() is going
	// to return a derived context. See the Context Management comments at the top
	// of the file.
	ctxHolder ctxHolder

	// onCancelSession is called when the SessionRegistry is cancels this session.
	// For pgwire connections, this is hooked up to canceling the connection's
	// context.
	// If nil, canceling this session will be a no-op.
	onCancelSession context.CancelFunc

	// planner is the "default planner" on a session, to save planner allocations
	// during serial execution. Since planners are not threadsafe, this is only
	// safe to use when a statement is not being parallelized. It must be reset
	// before using.
	planner planner
	// phaseTimes tracks session-level phase times. It is copied-by-value
	// to each planner in session.newPlanner.
	phaseTimes phaseTimes

	// parallelizeQueue is a queue managing all parallelized SQL statements
	// running on this connection.
	parallelizeQueue ParallelizeQueue

	// mu contains of all elements of the struct that can be changed
	// after initialization, and may be accessed from another thread.
	mu struct {
		syncutil.RWMutex

		// ActiveQueries contains all queries in flight.
		ActiveQueries map[ClusterWideID]*queryMeta

		// LastActiveQuery contains a reference to the AST of the last
		// query that ran on this session.
		LastActiveQuery tree.Statement
	}

	// curStmt is the statement that's currently being prepared or executed, if
	// any. This is printed by high-level panic recovery.
	curStmt tree.Statement

	sessionID ClusterWideID

	// activated determines whether activate() was called already.
	// When this is set, close() must be called to release resources.
	activated bool

	// hasCreatedTemporarySchema is set if the executor has created a
	//temporary schema, which requires special cleanup on Close.
	hasCreatedTemporarySchema bool
}

// ctxHolder contains a connection's context and, while session tracing is
// enabled, a derived context with a recording span. The connExecutor should use
// the latter while session tracing is active, or the former otherwise; that's
// what the ctx() method returns.
type ctxHolder struct {
	connCtx           context.Context
	sessionTracingCtx context.Context
}

func (ch *ctxHolder) ctx() context.Context {
	if ch.sessionTracingCtx != nil {
		return ch.sessionTracingCtx
	}
	return ch.connCtx
}

func (ch *ctxHolder) hijack(sessionTracingCtx context.Context) {
	if ch.sessionTracingCtx != nil {
		panic("hijack already in effect")
	}
	ch.sessionTracingCtx = sessionTracingCtx
}

func (ch *ctxHolder) unhijack() {
	if ch.sessionTracingCtx == nil {
		panic("hijack not in effect")
	}
	ch.sessionTracingCtx = nil
}

type prepStmtNamespace struct {
	// prepStmts contains the prepared statements currently available on the
	// session.
	prepStmts map[string]*PreparedStatement
	// portals contains the portals currently available on the session.
	portals map[string]*PreparedPortal
}

func (ns prepStmtNamespace) String() string {
	var sb strings.Builder
	sb.WriteString("Prep stmts: ")
	for name := range ns.prepStmts {
		sb.WriteString(name + " ")
	}
	sb.WriteString("Portals: ")
	for name := range ns.portals {
		sb.WriteString(name + " ")
	}
	return sb.String()
}

// resetTo resets a namespace to equate another one (`to`). All the receiver's
// references are release and all the to's references are duplicated.
//
// An empty `to` can be passed in to deallocate everything.
func (ns *prepStmtNamespace) resetTo(ctx context.Context, to prepStmtNamespace) {
	for name, p := range ns.prepStmts {
		p.decRef(ctx)
		delete(ns.prepStmts, name)
	}
	for name, p := range ns.portals {
		p.decRef(ctx)
		delete(ns.portals, name)
	}

	for name, ps := range to.prepStmts {
		ps.incRef(ctx)
		ns.prepStmts[name] = ps
	}
	for name, p := range to.portals {
		p.incRef(ctx)
		ns.portals[name] = p
	}
}

// resetExtraTxnState resets the fields of ex.extraTxnState when a transaction
// commits, rolls back or restarts.
func (ex *connExecutor) resetExtraTxnState(
	ctx context.Context, dbCacheHolder *databaseCacheHolder, ev txnEvent,
) error {
	ex.extraTxnState.schemaChangers.reset()

	ex.extraTxnState.tables.releaseTables(ctx)

	ex.extraTxnState.tables.databaseCache = dbCacheHolder.getDatabaseCache()

	ex.extraTxnState.tables.schemaCache = dbCacheHolder.getSchemaCache()

	// Close all portals.
	for name, p := range ex.extraTxnState.prepStmtsNamespace.portals {
		p.decRef(ctx)
		delete(ex.extraTxnState.prepStmtsNamespace.portals, name)
	}

	switch ev {
	case txnCommit, txnAborted:
		ex.extraTxnState.savepoints.clear()
	}
	// NOTE: on txnRestart we don't need to muck with the savepoints stack. It's either a
	// a ROLLBACK TO SAVEPOINT that generated the event, and that statement deals with the
	// savepoints, or it's a Rewind which also deals with them.

	return nil
}

// Ctx returns the transaction's ctx, if we're inside a transaction, or the
// session's context otherwise.
func (ex *connExecutor) Ctx() context.Context {
	if _, ok := ex.machine.CurState().(stateNoTxn); ok {
		return ex.ctxHolder.ctx()
	}
	// stateInternalError is used by the InternalExecutor.
	if _, ok := ex.machine.CurState().(stateInternalError); ok {
		return ex.ctxHolder.ctx()
	}
	return ex.state.Ctx
}

// activate engages the use of resources that must be cleaned up
// afterwards. after activate() completes, the close() method must be
// called.
//
// Args:
// parentMon: The root monitor.
// reserved: An amount on memory reserved for the connection. The connExecutor
// 	 takes ownership of this memory.
func (ex *connExecutor) activate(
	ctx context.Context, parentMon *mon.BytesMonitor, reserved mon.BoundAccount,
) {
	// Note: we pass `reserved` to sessionRootMon where it causes it to act as a
	// buffer. This is not done for sessionMon nor state.mon: these monitors don't
	// start with any buffer, so they'll need to ask their "parent" for memory as
	// soon as the first allocation. This is acceptable because the session is
	// single threaded, and the point of buffering is just to avoid contention.
	ex.mon.Start(ctx, parentMon, reserved)
	ex.sessionMon.Start(ctx, ex.mon, mon.BoundAccount{})

	// Enable the trace if configured.
	if traceSessionEventLogEnabled.Get(&ex.server.cfg.Settings.SV) {
		remoteStr := "<admin>"
		if ex.sessionData.RemoteAddr != nil {
			remoteStr = ex.sessionData.RemoteAddr.String()
		}
		ex.eventLog = trace.NewEventLog(
			fmt.Sprintf("sql session [%s]", ex.sessionData.User), remoteStr)
	}

	ex.activated = true
}

// run implements the run loop for a connExecutor. Commands are read one by one
// from the input buffer; they are executed and the resulting state transitions
// are performed.
//
// run returns when either the stmtBuf is closed by someone else or when an
// error is propagated from query execution. Note that query errors are not
// propagated as errors to this layer; only things that are supposed to
// terminate the session are (e.g. client communication errors and ctx
// cancelations).
// run() is expected to react on ctx cancelation, but the caller needs to also
// close the stmtBuf at the same time as canceling the ctx. If cancelation
// happens in the middle of a query execution, that's expected to interrupt the
// execution and generate an error. run() is then supposed to return because the
// buffer is closed and no further commands can be read.
//
// When this returns, ex.close() needs to be called and  the connection to the
// client needs to be terminated. If it returns with an error, that error may
// represent a communication error (in which case the connection might already
// also have an error from the reading side), or some other unexpected failure.
// Returned errors have not been communicated to the client: it's up to the
// caller to do that if it wants.
//
// onCancel, if not nil, will be called when the SessionRegistry cancels the
// session. TODO(andrei): This is hooked up to canceling the pgwire connection's
// context (of which ctx is also a child). It seems uncouth for the connExecutor
// to cancel a higher-level task. A better design would probably be for pgwire
// to own the SessionRegistry, instead of it being owned by the sql.Server -
// then pgwire would directly cancel its own tasks; the sessions also more
// naturally belong there. There is a problem, however, as query cancelation (as
// opposed to session cancelation) is done through the SessionRegistry and that
// does belong with the connExecutor. Introducing a query registry, separate
// from the session registry, might be too costly - the way query cancelation
// works is that every session is asked to cancel a given query until the right
// one is found. That seems like a good performance trade-off.
func (ex *connExecutor) run(
	ctx context.Context,
	parentMon *mon.BytesMonitor,
	reserved mon.BoundAccount,
	onCancel context.CancelFunc,
) error {
	if !ex.activated {
		ex.activate(ctx, parentMon, reserved)
	}
	ex.ctxHolder.connCtx = ctx
	ex.onCancelSession = onCancel

	ex.sessionID = ex.generateID()
	ex.server.cfg.SessionRegistry.register(ex.sessionID, ex)
	ex.planner.extendedEvalCtx.setSessionID(ex.sessionID)
	defer ex.server.cfg.SessionRegistry.deregister(ex.sessionID)

	pinfo := &tree.PlaceholderInfo{}

	var draining bool
	for {
		ex.curStmt = nil
		if err := ctx.Err(); err != nil {
			return err
		}

		cmd, pos, err := ex.stmtBuf.CurCmd()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if log.ExpensiveLogEnabled(ex.Ctx(), 2) || ex.eventLog != nil {
			ex.sessionEventf(ex.Ctx(), "[%s pos:%d] executing %s",
				ex.machine.CurState(), pos, cmd)
		}

		var ev fsm.Event
		var payload fsm.EventPayload
		var res ResultBase

		switch tcmd := cmd.(type) {
		case ExecStmt:
			if tcmd.AST == nil {
				res = ex.clientComm.CreateEmptyQueryResult(pos)
				break
			}
			ex.curStmt = tcmd.AST

			stmtRes := ex.clientComm.CreateStatementResult(
				tcmd.AST, NeedRowDesc, pos, nil, /* formatCodes */
				ex.sessionData.DataConversion, "", 0, ex.implicitTxn())
			res = stmtRes
			curStmt := Statement{Statement: tcmd.Statement}

			ex.phaseTimes[sessionQueryReceived] = tcmd.TimeReceived
			ex.phaseTimes[sessionStartParse] = tcmd.ParseStart
			ex.phaseTimes[sessionEndParse] = tcmd.ParseEnd

			ctx := withStatement(ex.Ctx(), ex.curStmt)
			ev, payload, err = ex.execStmt(ctx, curStmt, stmtRes, nil /* pinfo */)
			if err != nil {
				return err
			}
		case ExecPortal:
			// ExecPortal is handled like ExecStmt, except that the placeholder info
			// is taken from the portal.

			portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[tcmd.Name]
			if !ok {
				err := pgerror.NewErrorf(
					pgcode.InvalidCursorName, "unknown portal %q", tcmd.Name)
				ev = eventNonRetriableErr{IsCommit: fsm.False}
				payload = eventNonRetriableErrPayload{err: err}
				res = ex.clientComm.CreateErrorResult(pos)
				break
			}
			portal.Stmt.IsExecPortal = true
			if log.ExpensiveLogEnabled(ex.Ctx(), 2) {
				log.VEventf(ex.Ctx(), 2, "portal resolved to: %s", portal.Stmt.AST.String())
			}
			ex.curStmt = portal.Stmt.AST

			*pinfo = tree.PlaceholderInfo{
				PlaceholderTypesInfo: tree.PlaceholderTypesInfo{
					TypeHints: portal.Stmt.TypeHints,
					Types:     portal.Stmt.Types,
				},
				Values: portal.Qargs,
			}

			ex.phaseTimes[sessionQueryReceived] = tcmd.TimeReceived
			// When parsing has been done earlier, via a separate parse
			// message, it is not any more part of the statistics collected
			// for this execution. In that case, we simply report that
			// parsing took no time.
			ex.phaseTimes[sessionStartParse] = time.Time{}
			ex.phaseTimes[sessionEndParse] = time.Time{}

			if portal.Stmt.AST == nil {
				res = ex.clientComm.CreateEmptyQueryResult(pos)
				break
			}

			stmtRes := ex.clientComm.CreateStatementResult(
				portal.Stmt.AST,
				// The client is using the extended protocol, so no row description is
				// needed.
				DontNeedRowDesc,
				pos, portal.OutFormats,
				ex.sessionData.DataConversion,
				tcmd.Name,
				tcmd.Limit,
				ex.implicitTxn())
			//stmtRes.SetLimit(tcmd.Limit)
			res = stmtRes
			curStmt := Statement{
				Statement:     portal.Stmt.Statement,
				Prepared:      portal.Stmt,
				ExpectedTypes: portal.Stmt.Columns,
				AnonymizedStr: portal.Stmt.AnonymizedStr,
			}
			ctx := withStatement(ex.Ctx(), ex.curStmt)
			ev, payload, err = ex.execStmt(ctx, curStmt, stmtRes, pinfo)
			if err != nil {
				return err
			}
		case PrepareStmt:
			ex.curStmt = tcmd.AST
			res = ex.clientComm.CreatePrepareResult(pos)
			ctx := withStatement(ex.Ctx(), ex.curStmt)
			ev, payload = ex.execPrepare(ctx, tcmd)
		case DescribeStmt:
			descRes := ex.clientComm.CreateDescribeResult(pos)
			res = descRes
			ev, payload = ex.execDescribe(ex.Ctx(), tcmd, descRes)
		case BindStmt:
			res = ex.clientComm.CreateBindResult(pos)
			ev, payload = ex.execBind(ex.Ctx(), tcmd)
		case DeletePreparedStmt:
			res = ex.clientComm.CreateDeleteResult(pos)
			ev, payload = ex.execDelPrepStmt(ex.Ctx(), tcmd)
		case SendError:
			res = ex.clientComm.CreateErrorResult(pos)
			ev = eventNonRetriableErr{IsCommit: fsm.False}
			payload = eventNonRetriableErrPayload{err: tcmd.Err}
		case Sync:
			// Note that the Sync result will flush results to the network connection.
			res = ex.clientComm.CreateSyncResult(pos)
			if draining {
				// If we're draining, check whether this is a good time to finish the
				// connection. If we're not inside a transaction, we stop processing
				// now. If we are inside a transaction, we'll check again the next time
				// a Sync is processed.
				if snt, ok := ex.machine.CurState().(stateNoTxn); ok {
					res.Close(ctx, stateToTxnStatusIndicator(snt))
					return nil
				}
				if snt, ok := ex.machine.CurState().(stateInternalError); ok {
					res.Close(ctx, stateToTxnStatusIndicator(snt))
					return nil
				}
			}
		case CopyIn:
			res = ex.clientComm.CreateCopyInResult(pos)
			var err error
			ev, payload, err = ex.execCopyIn(ex.Ctx(), tcmd)
			if err != nil {
				return err
			}
		case DrainRequest:
			// We received a drain request. We terminate immediately if we're not in a
			// transaction. If we are in a transaction, we'll finish as soon as a Sync
			// command (i.e. the end of a batch) is processed outside of a
			// transaction.
			draining = true
			res = ex.clientComm.CreateDrainResult(pos)
			if _, ok := ex.machine.CurState().(stateNoTxn); ok {
				return nil
			}
		case Flush:
			// Closing the res will flush the connection's buffer.
			res = ex.clientComm.CreateFlushResult(pos)
		default:
			panic(fmt.Sprintf("unsupported command type: %T", cmd))
		}

		var advInfo advanceInfo

		// If an event was generated, feed it to the state machine.
		if ev != nil {
			var err error
			if res.Err() != nil && strings.Contains(res.Err().Error(), "ZNBase") {
				ex.state.mon.SetUdrState()
			}
			advInfo, err = ex.txnStateTransitionsApplyWrapper(ev, payload, res, pos)
			if err != nil {
				return err
			}
		} else {
			// If no event was generated synthesize an advance code.
			advInfo = advanceInfo{
				code: advanceOne,
			}
		}

		// Decide if we need to close the result or not. We don't need to do it if
		// we're staying in place or rewinding - the statement will be executed
		// again.
		if advInfo.code != stayInPlace && advInfo.code != rewind {
			// Close the result. In case of an execution error, the result might have
			// its error set already or it might not.
			resErr := res.Err()

			pe, ok := payload.(payloadWithError)
			if ok {
				ex.sessionEventf(ex.Ctx(), "execution error: %s", pe.errorCause())
			}
			if resErr == nil && ok {
				// Depending on whether the result has the error already or not, we have
				// to call either Close or CloseWithErr.
				res.CloseWithErr(pe.errorCause())
			} else {
				ex.recordError(resErr)
				res.Close(ctx, stateToTxnStatusIndicator(ex.machine.CurState()))
			}
		} else {
			res.Discard()
		}

		// Move the cursor according to what the state transition told us to do.
		switch advInfo.code {
		case advanceOne:
			ex.stmtBuf.AdvanceOne()
		case skipBatch:
			// We'll flush whatever results we have to the network. The last one must
			// be an error. This flush may seem unnecessary, as we generally only
			// flush when the client requests it through a Sync or a Flush but without
			// it the Node.js driver isn't happy. That driver likes to send "flush"
			// command and only sends Syncs once it received some data. But we ignore
			// flush commands (just like we ignore any other commands) when skipping
			// to the next batch.
			if err := ex.clientComm.Flush(pos); err != nil {
				return err
			}
			if err := ex.stmtBuf.seekToNextBatch(); err != nil {
				return err
			}
		case rewind:
			ex.rewindPrepStmtNamespace(ex.Ctx())
			ex.extraTxnState.savepoints = ex.extraTxnState.savepointsAtTxnRewindPos
			advInfo.rewCap.rewindAndUnlock(ex.Ctx())
		case stayInPlace:
			if advInfo.txnEvent == noEvent {
				continue
			}
			// Nothing to do. The same statement will be executed again.
		default:
			panic(fmt.Sprintf("unexpected advance code: %s", advInfo.code))
		}

		if err := ex.updateTxnRewindPosMaybe(ex.Ctx(), cmd, pos, advInfo); err != nil {
			return err
		}
	}
}

// updateTxnRewindPosMaybe checks whether the ex.extraTxnState.txnRewindPos
// should be advanced, based on the advInfo produced by running cmd at position
// pos.
func (ex *connExecutor) updateTxnRewindPosMaybe(
	ctx context.Context, cmd Command, pos CmdPos, advInfo advanceInfo,
) error {
	// txnRewindPos is only maintained while in stateOpen.
	if _, ok := ex.machine.CurState().(stateOpen); !ok {
		return nil
	}
	if advInfo.txnEvent == txnStart || advInfo.txnEvent == txnRestart {
		var nextPos CmdPos
		switch advInfo.code {
		case stayInPlace:
			nextPos = pos
		case advanceOne:
			// Future rewinds will refer to the next position; the statement that
			// started the transaction (i.e. BEGIN) will not be itself be executed
			// again.
			nextPos = pos + 1
		case rewind:
			if advInfo.rewCap.rewindPos != ex.extraTxnState.txnRewindPos {
				return errors.Errorf("unexpected Rewind position: %d when txn start is: %d",
					advInfo.rewCap.rewindPos, ex.extraTxnState.txnRewindPos)
			}
			// txnRewindPos stays unchanged.
			return nil
		default:
			return errors.Errorf("unexpected advance code when starting a txn: %s", advInfo.code)
		}
		ex.setTxnRewindPos(ctx, nextPos)
	} else {
		// See if we can advance the Rewind point even if this is not the point
		// where the transaction started. We can do that after running a special
		// statement (e.g. SET TRANSACTION or SAVEPOINT) or after most commands that
		// don't execute statements.
		// The idea is that, for example, we don't want the following sequence to
		// disable retries for what comes after the sequence:
		// 1: PrepareStmt BEGIN
		// 2: BindStmt
		// 3: ExecutePortal
		// 4: Sync

		// Note that the current command cannot influence the Rewind point if
		// if the Rewind point is not current set to the command's position
		// (i.e. we don't do anything if txnRewindPos != pos).

		if advInfo.code != advanceOne {
			panic(fmt.Sprintf("unexpected advanceCode: %s", advInfo.code))
		}

		var canAdvance bool
		_, inOpen := ex.machine.CurState().(stateOpen)
		if inOpen && (ex.extraTxnState.txnRewindPos == pos) {
			switch tcmd := cmd.(type) {
			case ExecStmt:
				canAdvance = ex.stmtDoesntNeedRetry(tcmd.AST)
			case ExecPortal:
				portal := ex.extraTxnState.prepStmtsNamespace.portals[tcmd.Name]
				canAdvance = ex.stmtDoesntNeedRetry(portal.Stmt.AST)
			case PrepareStmt:
				canAdvance = true
			case DescribeStmt:
				canAdvance = true
			case BindStmt:
				canAdvance = true
			case DeletePreparedStmt:
				canAdvance = true
			case SendError:
				canAdvance = true
			case Sync:
				canAdvance = true
			case CopyIn:
				// Can't advance.
			case DrainRequest:
				canAdvance = true
			case Flush:
				canAdvance = true
			default:
				panic(fmt.Sprintf("unsupported cmd: %T", cmd))
			}
			if canAdvance {
				ex.setTxnRewindPos(ctx, pos+1)
			}
		}
	}
	return nil
}

// setTxnRewindPos updates the position to which future rewinds will refer.
//
// All statements with lower position in stmtBuf (if any) are removed, as we
// won't ever need them again.
func (ex *connExecutor) setTxnRewindPos(ctx context.Context, pos CmdPos) {
	if pos <= ex.extraTxnState.txnRewindPos {
		panic(fmt.Sprintf("can only move the  txnRewindPos forward. "+
			"Was: %d; new value: %d", ex.extraTxnState.txnRewindPos, pos))
	}
	ex.extraTxnState.txnRewindPos = pos
	ex.stmtBuf.ltrim(ctx, pos)
	ex.commitPrepStmtNamespace(ctx)
	ex.extraTxnState.savepointsAtTxnRewindPos = ex.extraTxnState.savepoints.clone()
}

// stmtDoesntNeedRetry returns true if the given statement does not need to be
// retried when performing automatic retries. This means that the results of the
// statement do not change with retries.
func (ex *connExecutor) stmtDoesntNeedRetry(stmt tree.Statement) bool {
	wrap := Statement{Statement: parser.Statement{AST: stmt}}
	return isSavepoint(wrap) || isSetTransaction(wrap)
}

func stateToTxnStatusIndicator(s fsm.State) TransactionStatusIndicator {
	switch s.(type) {
	case stateOpen:
		return InTxnBlock
	case stateAborted:
		return InFailedTxnBlock
	case stateNoTxn:
		return IdleTxnBlock
	case stateCommitWait:
		return InTxnBlock
	case stateInternalError:
		return InTxnBlock
	default:
		panic(fmt.Sprintf("unknown state: %T", s))
	}
}

// We handle the CopyFrom statement by creating a copyMachine and handing it
// control over the connection until the copying is done. The contract is that,
// when this is called, the pgwire.conn is not reading from the network
// connection any more until this returns. The copyMachine will to the reading
// and writing up to the CommandComplete message.
func (ex *connExecutor) execCopyIn(
	ctx context.Context, cmd CopyIn,
) (fsm.Event, fsm.EventPayload, error) {

	// When we're done, unblock the network connection.
	defer cmd.CopyDone.Done()

	state := ex.machine.CurState()
	_, isNoTxn := state.(stateNoTxn)
	_, isOpen := state.(stateOpen)
	if !isNoTxn && !isOpen {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionAbortedError("" /* customMsg */)}
		return ev, payload, nil
	}

	// If we're in an explicit txn, then the copying will be done within that
	// txn. Otherwise, we tell the copyMachine to manage its own transactions.
	var txnOpt copyTxnOpt
	if isOpen {
		txnOpt = copyTxnOpt{
			txn:           ex.state.mu.txn,
			txnTimestamp:  ex.state.sqlTimestamp,
			stmtTimestamp: ex.server.cfg.Clock.PhysicalTime(),
		}
	}

	var monToStop *mon.BytesMonitor
	defer func() {
		if monToStop != nil {
			monToStop.Stop(ctx)
		}
	}()
	if isNoTxn {
		// HACK: We're reaching inside ex.state and starting the monitor. Normally
		// that's driven by the state machine, but we're bypassing the state machine
		// here.
		ex.state.mon.Start(ctx, ex.sessionMon, mon.BoundAccount{} /* reserved */)
		monToStop = ex.state.mon
	}
	cm, err := newCopyMachine(
		ctx, cmd.Conn, cmd.Stmt, txnOpt, ex.server.cfg,
		// resetPlanner
		func(p *planner, txn *client.Txn, txnTS time.Time, stmtTS time.Time) {
			// HACK: We're reaching inside ex.state and changing sqlTimestamp by hand.
			// It is used by resetPlanner. Normally sqlTimestamp is updated by the
			// state machine, but the copyMachine manages its own transactions without
			// going through the state machine.
			ex.state.sqlTimestamp = txnTS
			ex.initPlanner(ctx, p)
			ex.resetPlanner(ctx, p, txn, stmtTS)
		},
	)
	if err != nil {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: err}
		return ev, payload, nil
	}
	if err := cm.run(ctx); err != nil {
		// TODO(andrei): We don't have a retriable error story for the copy machine.
		// When running outside of a txn, the copyMachine should probably do retries
		// internally. When not, it's unclear what we should do. For now, we abort
		// the txn (if any).
		// We also don't have a story for distinguishing communication errors (which
		// should terminate the connection) from query errors. For now, we treat all
		// errors as query errors.
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{err: err}
		return ev, payload, nil
	}
	return nil, nil, nil
}

// stmtHasNoData returns true if describing a result of the input statement
// type should return NoData.
func stmtHasNoData(stmt tree.Statement) bool {
	return stmt == nil || stmt.StatementType() != tree.Rows
}

// generateID generates a unique ID based on the node's ID and its current HLC
// timestamp. These IDs are either scoped at the query level or at the session
// level.
func (ex *connExecutor) generateID() ClusterWideID {
	return GenerateClusterWideID(ex.server.cfg.Clock.Now(), ex.server.cfg.NodeID.Get())
}

// commitPrepStmtNamespace deallocates everything in
// prepStmtsNamespaceAtTxnRewindPos that's not part of prepStmtsNamespace.
func (ex *connExecutor) commitPrepStmtNamespace(ctx context.Context) {
	ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.resetTo(
		ctx, ex.extraTxnState.prepStmtsNamespace)
}

// commitPrepStmtNamespace deallocates everything in prepStmtsNamespace that's
// not part of prepStmtsNamespaceAtTxnRewindPos.
func (ex *connExecutor) rewindPrepStmtNamespace(ctx context.Context) {
	ex.extraTxnState.prepStmtsNamespace.resetTo(
		ctx, ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos)
}

// getRewindTxnCapability checks whether rewinding to the position previously
// set through setTxnRewindPos() is possible and, if it is, returns a
// rewindCapability bound to that position. The returned bool is true if the
// Rewind is possible. If it is, client communication is blocked until the
// rewindCapability is exercised.
func (ex *connExecutor) getRewindTxnCapability() (rewindCapability, bool) {
	cl := ex.clientComm.LockCommunication()

	// If we already delivered results at or past the start position, we can't
	// Rewind.
	if cl.ClientPos() >= ex.extraTxnState.txnRewindPos {
		cl.Close()
		return rewindCapability{}, false
	}
	return rewindCapability{
		cl:        cl,
		buf:       ex.stmtBuf,
		rewindPos: ex.extraTxnState.txnRewindPos,
	}, true
}

// isCommit returns true if stmt is a "COMMIT" statement.
func isCommit(stmt tree.Statement) bool {
	_, ok := stmt.(*tree.CommitTransaction)
	return ok
}

func errIsRetriable(err error) bool {
	_, retriable := err.(*roachpb.TransactionRetryWithProtoRefreshError)
	return retriable
}

// makeErrEvent takes an error and returns either an eventRetriableErr or an
// eventNonRetriableErr, depending on the error type.
func (ex *connExecutor) makeErrEvent(err error, stmt tree.Statement) (fsm.Event, fsm.EventPayload) {
	retriable := errIsRetriable(err)
	ctx := ex.Ctx()
	ex.state.mu.Lock()
	txn := ex.state.mu.txn
	ex.state.mu.Unlock()
	if retriable {
		if txn != nil && txn.IsolationLevel() == util.ReadCommittedIsolation {
			if retryErr := err.(*roachpb.TransactionRetryWithProtoRefreshError); retryErr.RcRetry {
				if ex.state.retry > 0 {
					ex.state.retry--
					ev := eventRCRetry{}
					payload := eventRetriableErrPayload{
						err:    err,
						rewCap: noRewind,
					}
					return ev, payload
				}
				log.Errorf(ctx, "read committed retry failed")
			}
		}
		rc, canAutoRetry := ex.getRewindTxnCapability()
		ev := eventRetriableErr{
			IsCommit:     fsm.FromBool(isCommit(stmt)),
			CanAutoRetry: fsm.FromBool(canAutoRetry),
		}
		payload := eventRetriableErrPayload{
			err:    err,
			rewCap: rc,
		}
		return ev, payload
	}
	ev := eventNonRetriableErr{
		IsCommit: fsm.FromBool(isCommit(stmt)),
	}
	payload := eventNonRetriableErrPayload{err: err}
	return ev, payload
}

// synchronizeParallelStmts waits for all statements in the parallelizeQueue to
// finish. If errors are seen in the parallel batch, we attempt to turn these
// errors into a single error we can send to the client. We do this by prioritizing
// non-retryable errors over retryable errors.
// Note that the returned error is to always be considered a "query execution
// error". This means that it should never interrupt the connection.
func (ex *connExecutor) synchronizeParallelStmts(ctx context.Context) error {
	if errs := ex.parallelizeQueue.Wait(); len(errs) > 0 {
		ex.state.mu.Lock()
		defer ex.state.mu.Unlock()

		// Sort the errors according to their importance.
		curTxnID := ex.state.mu.txn.ID()
		curTxnEpoch := ex.state.mu.txn.Epoch()
		sort.Slice(errs, func(i, j int) bool {
			errPriority := func(err error) int {
				switch t := err.(type) {
				case *roachpb.TransactionRetryWithProtoRefreshError:
					errTxn := t.Transaction
					if errTxn.ID == curTxnID && errTxn.Epoch == curTxnEpoch {
						// A retryable error for the current transaction
						// incarnation is given the highest priority.
						return 1
					}
					return 2
				case *roachpb.TxnAlreadyEncounteredErrorError:
					// Another parallel stmt got an error that caused this one.
					return 5
				default:
					// Any other error. We sort these behind retryable errors
					// and errors we know to be their symptoms because it is
					// impossible to conclusively determine in all cases whether
					// one of these errors is a symptom of a concurrent retry or
					// not. If the error is a symptom then we want to ignore it.
					// If it is not, we expect to see the same error during a
					// transaction retry.
					return 4
				}
			}
			return errPriority(errs[i]) < errPriority(errs[j])
		})

		// Return the "best" error.
		bestErr := errs[0]
		switch bestErr.(type) {
		case *roachpb.TransactionRetryWithProtoRefreshError:
			// If any of the errors are retryable, we need to bump the transaction
			// epoch to invalidate any writes performed by any workers after the
			// retry updated the txn's proto but before we synchronized (some of
			// these writes might have been performed at the wrong epoch). Note
			// that we don't need to lock the client.Txn because we're synchronized.
			// See #17197.
			ex.state.mu.txn.ManualRestart(ctx, hlc.Timestamp{})
		}
		return bestErr
	}
	return nil
}

// setTransactionModes implements the txnModesSetter interface.
func (ex *connExecutor) setTransactionModes(
	modes tree.TransactionModes, asOfTs hlc.Timestamp,
) error {
	// This method cheats and manipulates ex.state directly, not through an event.
	// The alternative would be to create a special event, but it's unclear how
	// that'd work given that this method is called while executing a statement.

	// Transform the transaction options into the types needed by the state
	// machine.
	if modes.UserPriority != tree.UnspecifiedUserPriority {
		pri, err := priorityToProto(modes.UserPriority)
		if err != nil {
			return err
		}
		if err := ex.state.setPriority(pri); err != nil {
			return err
		}
	}
	if modes.Isolation < tree.UnspecifiedIsolation && modes.Isolation >= tree.UnreachableIsolation {
		return errors.Errorf("unknown isolation level: %v", modes.Isolation)
	}
	ex.state.setIsolationLevel(modes.Isolation)
	rwMode := modes.ReadWriteMode
	if modes.AsOf.Expr != nil && (asOfTs == hlc.Timestamp{}) {
		return pgerror.NewAssertionErrorf("expected an evaluated AS OF timestamp")
	}
	if (asOfTs != hlc.Timestamp{}) {
		ex.state.setHistoricalTimestamp(ex.Ctx(), asOfTs)
		ex.state.sqlTimestamp = asOfTs.GoTime()
		if rwMode == tree.UnspecifiedReadWriteMode {
			rwMode = tree.ReadOnly
		}
	}
	if modes.Name != "" {
		if err := ex.state.setDebugName(modes.Name); err != nil {
			return err
		}
	}

	if modes.TxnDDL == tree.SupportDDL {
		if err := ex.state.setTxnDDL(tree.SupportDDL); err != nil {
			log.Errorf(context.Background(), "set transaction supDDL throw error: %v", err)
		}

	}

	return ex.state.setReadOnlyMode(rwMode)
}

func priorityToProto(mode tree.UserPriority) (roachpb.UserPriority, error) {
	var pri roachpb.UserPriority
	switch mode {
	case tree.UnspecifiedUserPriority:
		pri = roachpb.NormalUserPriority
	case tree.Low:
		pri = roachpb.MinUserPriority
	case tree.Normal:
		pri = roachpb.NormalUserPriority
	case tree.High:
		pri = roachpb.MaxUserPriority
	default:
		return roachpb.UserPriority(0), errors.Errorf("unknown user priority: %s", mode)
	}
	return pri, nil
}

func (ex *connExecutor) readWriteModeWithSessionDefault(
	mode tree.ReadWriteMode,
) tree.ReadWriteMode {
	if mode == tree.UnspecifiedReadWriteMode {
		if ex.sessionData.DefaultReadOnly {
			return tree.ReadOnly
		}
		return tree.ReadWrite
	}
	return mode
}

// initEvalCtx initializes the fields of an extendedEvalContext that stay the
// same across multiple statements. resetEvalCtx must also be called before each
// statement, to reinitialize other fields.
func (ex *connExecutor) initEvalCtx(ctx context.Context, evalCtx *extendedEvalContext, p *planner) {
	scInterface := newSchemaInterface(&ex.extraTxnState.tables, ex.server.cfg.VirtualSchemas)

	ie := NewSessionBoundInternalExecutor(
		ctx,
		ex.sessionData,
		ex.server,
		ex.memMetrics,
		ex.server.cfg.Settings,
	)

	*evalCtx = extendedEvalContext{
		EvalContext: tree.EvalContext{
			Planner:          p,
			Sequence:         p,
			SessionData:      ex.sessionData,
			SessionAccessor:  p,
			Settings:         ex.server.cfg.Settings,
			TestingKnobs:     ex.server.cfg.EvalContextTestingKnobs,
			ClusterID:        ex.server.cfg.ClusterID(),
			NodeID:           ex.server.cfg.NodeID.Get(),
			Locality:         ex.server.cfg.Locality,
			ReCache:          ex.server.reCache,
			InternalExecutor: ie,
			DB:               ex.server.cfg.DB,
			Params:           make([]tree.UDRVar, 0),
		},
		SessionMutator: ex.dataMutator,
		// curTxnMap perform transaction of cursors.
		curTxnMap: make(map[string]CurTxn),
		// CurrentSessionID store CurrentSessionID to help name cursor.
		CurrentSessionID: ex.sessionID,
		VirtualSchemas:   ex.server.cfg.VirtualSchemas,
		Tracing:          &ex.sessionTracing,
		StatusServer:     ex.server.cfg.StatusServer,
		MemMetrics:       &ex.memMetrics,
		Tables:           &ex.extraTxnState.tables,
		ExecCfg:          ex.server.cfg,
		DistSQLPlanner:   ex.server.cfg.DistSQLPlanner,
		TxnModesSetter:   ex,
		SchemaChangers:   &ex.extraTxnState.schemaChangers,
		schemaAccessors:  scInterface,
	}
	// save extendedEvalContext's address for udr function execute env
	evalCtx.EvalContext.MagicExtent = unsafe.Pointer(evalCtx)
}

// resetEvalCtx initializes the fields of evalCtx that can change
// during a session (i.e. the fields not set by initEvalCtx).
//
// stmtTS is the timestamp that the statement_timestamp() SQL builtin will
// return for statements executed with this evalCtx. Since generally each
// statement is supposed to have a different timestamp, the evalCtx generally
// shouldn't be reused across statements.
func (ex *connExecutor) resetEvalCtx(
	evalCtx *extendedEvalContext, txn *client.Txn, stmtTS time.Time,
) {
	evalCtx.TxnState = ex.getTransactionState()
	evalCtx.TxnReadOnly = ex.state.readOnly
	evalCtx.TxnImplicit = ex.implicitTxn()
	evalCtx.StmtTimestamp = stmtTS
	evalCtx.TxnTimestamp = ex.state.sqlTimestamp
	evalCtx.Placeholders = nil
	evalCtx.IVarContainer = nil
	evalCtx.Context = ex.Ctx()
	evalCtx.Txn = txn
	evalCtx.Mon = ex.state.mon
	evalCtx.PrepareOnly = false
	evalCtx.SkipNormalize = false
	// udf select count, use to help check the depth of udr function/procedure's calling or select
	evalCtx.SelectCount = 0
	// CurrentSessionID store CurrentSessionID to help name cursor.
	evalCtx.CurrentSessionID = ex.sessionID
}

// getTransactionState retrieves a text representation of the given state.
func (ex *connExecutor) getTransactionState() string {
	state := ex.machine.CurState()
	if ex.implicitTxn() {
		// If the statement reading the state is in an implicit transaction, then we
		// want to tell NoTxn to the client.
		state = stateNoTxn{}
	}
	return state.(fmt.Stringer).String()
}

func (ex *connExecutor) implicitTxn() bool {
	state := ex.machine.CurState()
	os, ok := state.(stateOpen)
	return ok && os.ImplicitTxn.Get()
}

// initPlanner initializes a planner so it can can be used for planning a
// query in the context of this session.
func (ex *connExecutor) initPlanner(ctx context.Context, p *planner) {
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)
	p.statsCollector = ex.newStatsCollector()

	ex.initEvalCtx(ctx, &p.extendedEvalCtx, p)

	p.sessionDataMutator = ex.dataMutator
	p.noticeSender = nil
	p.preparedStatements = ex.getPrepStmtsAccessor()

	p.queryCacheSession.Init()
	p.optPlanningCtx.init(p)
}

func (ex *connExecutor) resetPlanner(
	ctx context.Context, p *planner, txn *client.Txn, stmtTS time.Time,
) {
	p.txn = txn
	p.stmt = nil

	p.cancelChecker.Reset(ctx)
	p.statsCollector.Reset(&ex.server.sqlStats, ex.appStats, &ex.phaseTimes)

	p.semaCtx = tree.MakeSemaContext()
	for i := range p.semaCtxs {
		p.semaCtxs[i] = tree.MakeSemaContext()
	}
	p.semaCtx.Location = &ex.sessionData.DataConversion.Location
	p.semaCtx.SearchPath = ex.sessionData.SearchPath
	p.semaCtx.AsOfTimestamp = nil

	ex.resetEvalCtx(&p.extendedEvalCtx, txn, stmtTS)

	p.autoCommit = false
	p.isPreparing = false
	p.avoidCachedDescriptors = false
}

// txnStateTransitionsApplyWrapper is a wrapper on top of Machine built with the
// TxnStateTransitions above. Its point is to detect when we go in and out of
// transactions and update some state.
//
// Any returned error indicates an unrecoverable error for the session;
// execution on this connection should be interrupted.
func (ex *connExecutor) txnStateTransitionsApplyWrapper(
	ev fsm.Event, payload fsm.EventPayload, res ResultBase, pos CmdPos,
) (advanceInfo, error) {
	var implicitTxn bool
	if os, ok := ex.machine.CurState().(stateOpen); ok {
		implicitTxn = os.ImplicitTxn.Get()
	}

	err := ex.machine.ApplyWithPayload(withStatement(ex.Ctx(), ex.curStmt), ev, payload)
	if err != nil {
		if _, ok := err.(fsm.TransitionNotFoundError); ok {
			panic(err)
		}
		return advanceInfo{}, err
	}

	advInfo := ex.state.consumeAdvanceInfo()

	if advInfo.code == rewind {
		ex.extraTxnState.autoRetryCounter++
	}

	// Handle transaction events which cause updates to txnState.
	switch advInfo.txnEvent {
	case noEvent:
	case txnStart:
		ex.extraTxnState.autoRetryCounter = 0
	case txnCommit:
		if res.Err() != nil {
			err := errorutil.UnexpectedWithIssueErrorf(
				26687,
				"programming error: non-error event "+
					advInfo.txnEvent.String()+ //the event is included like this so that it doesn't get sanitized
					" generated even though res.Err() has been set to: %s",
				res.Err())
			log.Error(ex.Ctx(), err)
			err.(errorutil.UnexpectedWithIssueErr).SendReport(ex.Ctx(), &ex.server.cfg.Settings.SV)
			return advanceInfo{}, err
		}
		scc := &ex.extraTxnState.schemaChangers
		if mviewStmt, ok := ex.curStmt.(*tree.CreateView); ok && mviewStmt.Materialized {
			for i := range scc.schemaChangers {
				scc.schemaChangers[i].internalSessiondata = ex.sessionData
			}
		}
		if _, ok := ex.curStmt.(*tree.RefreshMaterializedView); ok {
			for i := range scc.schemaChangers {
				scc.schemaChangers[i].internalSessiondata = ex.sessionData
			}
		}
		if len(scc.schemaChangers) != 0 {
			ieFactory := func(ctx context.Context, sd *sessiondata.SessionData) sqlutil.InternalExecutor {
				ie := NewSessionBoundInternalExecutor(
					ctx,
					sd,
					ex.server,
					ex.memMetrics,
					ex.server.cfg.Settings,
				)
				return ie
			}
			if schemaChangeErr := scc.execSchemaChanges(
				ex.Ctx(), ex.server.cfg, &ex.sessionTracing, ieFactory,
			); schemaChangeErr != nil {
				// We got a schema change error. We'll return it to the client as the
				// result of the current statement - which is either the DDL statement or
				// a COMMIT statement if the DDL was part of an explicit transaction. In
				// the explicit transaction case, we return a funky error code to the
				// client to seed fear about what happened to the transaction. The reality
				// is that the transaction committed, but at least some of the staged
				// schema changes failed. We don't have a good way to indicate this.
				if implicitTxn {
					res.SetError(schemaChangeErr)
				} else {
					res.SetError(sqlbase.NewStatementCompletionUnknownError(schemaChangeErr))
				}

				//When the materialized view data backfill fails, it indicates that the
				//materialized view creation failed and the table name needs to be released.
				//Call the internal executor here to remove it. This is a temporary solution.
				if viewStmt, ok := ex.planner.curPlan.AST.(*tree.CreateView); ok && viewStmt.Materialized {
					query := fmt.Sprintf("DROP MATERIALIZED VIEW %s.%s.%s",
						viewStmt.Name.CatalogName, viewStmt.Name.SchemaName, viewStmt.Name.TableName.String())
					_, _ = ex.planner.extendedEvalCtx.InternalExecutor.Query(ex.Ctx(), "dropMaterializedView", nil, query)
				}
			}
		}

		// Wait for the cache to reflect the dropped databases if any.
		ex.extraTxnState.tables.waitForCacheToDropDatabases(ex.Ctx())

		fallthrough
	case txnRestart, txnAborted:
		if err := ex.resetExtraTxnState(ex.Ctx(), ex.server.dbCache, advInfo.txnEvent); err != nil {
			return advanceInfo{}, err
		}
	default:
		return advanceInfo{}, errors.Errorf("unexpected event: %v", advInfo.txnEvent)
	}

	return advInfo, nil
}

// initStatementResult initializes res according to a query.
//
// cols represents the columns of the result rows. Should be nil if
// stmt.AST.StatementType() != tree.Rows.
//
// If an error is returned, it is to be considered a query execution error.
func (ex *connExecutor) initStatementResult(
	ctx context.Context, res RestrictedCommandResult, stmt *Statement, cols sqlbase.ResultColumns,
) error {
	for _, c := range cols {
		if err := checkResultType(c.Typ); err != nil {
			return err
		}
	}
	if stmt.AST.StatementType() == tree.Rows {
		// Note that this call is necessary even if cols is nil.
		res.SetColumns(ctx, cols)
	}
	return nil
}

// recordError processes an error at the end of query execution.
// This triggers telemetry and, if the error is an internal error,
// triggers the emission of a sentry report.
func (ex *connExecutor) recordError(err error) {
	sqltelemetry.RecordError(ex.Ctx(), err, &ex.server.cfg.Settings.SV)
}

// newStatsCollector returns an sqlStatsCollector that will record stats in the
// session's stats containers.
func (ex *connExecutor) newStatsCollector() sqlStatsCollector {
	return newSQLStatsCollectorImpl(&ex.server.sqlStats, ex.appStats, &ex.phaseTimes)
}

// cancelQuery is part of the registrySession interface.
func (ex *connExecutor) cancelQuery(queryID ClusterWideID) bool {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if queryMeta, exists := ex.mu.ActiveQueries[queryID]; exists {
		queryMeta.cancel()
		return true
	}
	return false
}

// queryTransaction is part of the registrySession interface.
func (ex *connExecutor) queryTransaction(txnID string) (*serverpb.QueryLockstatResponse, bool) {
	ex.mu.Lock()
	defer ex.mu.Unlock()

	txn := ex.state.mu.txn
	txnID = txnID[1 : len(txnID)-1]

	if txn != nil && txn.ID().String() == txnID {
		response := &serverpb.QueryLockstatResponse{}
		response.TxnId = txnID
		k := roachpb.Key(txn.GetTxnCoordMeta(ex.Ctx()).Txn.Key)
		response.TxnKey = string(roachpb.PrettyPrintKey(nil, k))
		response.TxnStatus = txn.GetTxnCoordMeta(ex.Ctx()).Txn.Status.String()
		response.TxnIsActive = strconv.FormatBool(txn.Active())
		response.TxnType = client.TxnTypeName[txn.Type()]
		response.TxnName = txn.GetTxnCoordMeta(ex.Ctx()).Txn.Name
		response.TxnIsolationLevel = util.IsolationLevelNames[txn.IsolationLevel()]
		//将span返回
		txnSpans := txn.Sender().GetTxnSpans()
		for _, span := range txnSpans.RefresherSpans {
			response.Spans = append(response.Spans, &serverpb.Span{Key: span.Key, EndKey: span.EndKey})
		}
		for _, span := range txnSpans.PipelinerSpans {
			response.WriteSpans = append(response.WriteSpans, &serverpb.Span{Key: span.Key, EndKey: span.EndKey})
		}
		return response, true
	}
	return nil, false
}

// GetConnExecutor is part of the registrySession interface.
func (ex *connExecutor) GetConnExecutor(r registrySession) *connExecutor {
	if connExecutor, ok := r.(*connExecutor); ok {
		return connExecutor
	}
	return nil
}

// cancelSession is part of the registrySession interface.
func (ex *connExecutor) cancelSession() {
	if ex.onCancelSession == nil {
		return
	}
	// TODO(abhimadan): figure out how to send a nice error message to the client.
	ex.onCancelSession()
}

// user is part of the registrySession interface.
func (ex *connExecutor) user() string {
	return ex.sessionData.User
}

// serialize is part of the registrySession interface.
func (ex *connExecutor) serialize() serverpb.Session {
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	ex.state.mu.RLock()
	defer ex.state.mu.RUnlock()

	var kvTxnID *uuid.UUID
	txn := ex.state.mu.txn
	if txn != nil {
		id := txn.ID()
		kvTxnID = &id
	}

	activeQueries := make([]serverpb.ActiveQuery, 0, len(ex.mu.ActiveQueries))
	truncateSQL := func(sql string) string {
		if len(sql) > MaxSQLBytes {
			sql = sql[:MaxSQLBytes-utf8.RuneLen('…')]
			// Ensure the resulting string is valid utf8.
			for {
				if r, _ := utf8.DecodeLastRuneInString(sql); r != utf8.RuneError {
					break
				}
				sql = sql[:len(sql)-1]
			}
			sql += "…"
		}
		return sql
	}

	for id, query := range ex.mu.ActiveQueries {
		if query.hidden {
			continue
		}
		sql := truncateSQL(query.stmt.String())
		activeQueries = append(activeQueries, serverpb.ActiveQuery{
			ID:            id.String(),
			Start:         query.start.UTC(),
			Sql:           sql,
			IsDistributed: query.isDistributed,
			Phase:         (serverpb.ActiveQuery_Phase)(query.phase),
		})
	}
	lastActiveQuery := ""
	if ex.mu.LastActiveQuery != nil {
		lastActiveQuery = truncateSQL(ex.mu.LastActiveQuery.String())
	}

	remoteStr := "<admin>"
	if ex.sessionData.RemoteAddr != nil {
		remoteStr = ex.sessionData.RemoteAddr.String()
	}

	return serverpb.Session{
		Username:        ex.sessionData.User,
		ClientAddress:   remoteStr,
		ApplicationName: ex.applicationName.Load().(string),
		Start:           ex.phaseTimes[sessionInit].UTC(),
		ActiveQueries:   activeQueries,
		KvTxnID:         kvTxnID,
		LastActiveQuery: lastActiveQuery,
		ID:              ex.sessionID.GetBytes(),
		AllocBytes:      ex.mon.AllocBytes(),
		MaxAllocBytes:   ex.mon.MaximumBytes(),
	}
}

func (ex *connExecutor) getPrepStmtsAccessor() preparedStatementsAccessor {
	return connExPrepStmtsAccessor{
		ex: ex,
	}
}

func (ex *connExecutor) getCaseSensitive() bool {
	if ex.sessionData != nil {
		return ex.sessionData.CaseSensitive
	}
	return false
}

// GetSessionData is use for getting sessionData from connExecutor.
func (ex *connExecutor) GetSessionData() *sessiondata.SessionData {
	if ex.sessionData != nil {
		return ex.sessionData
	}
	return nil
}

// GetPhaseTimes is use for getting phaseTimes from connExecutor.
func (ex *connExecutor) GetPhaseTimes() phaseTimes {
	return ex.phaseTimes
}

// sessionEventf logs a message to the session event log (if any).
func (ex *connExecutor) sessionEventf(ctx context.Context, format string, args ...interface{}) {
	if log.ExpensiveLogEnabled(ex.Ctx(), 2) {
		log.VEventfDepth(ex.Ctx(), 1 /* depth */, 2 /* level */, format, args...)
	}
	if ex.eventLog != nil {
		ex.eventLog.Printf(format, args...)
	}
}

// StatementCounters groups metrics for counting different types of
// statements. These metrics count user-initiated operations,
// regardless of success (in particular, TxnCommitCount is the number
// of COMMIT statements attempted, not the number of transactions that
// successfully commit).
type StatementCounters struct {
	// QueryCount includes all statements and it is therefore the sum of
	// all the below metrics.
	QueryCount telemetry.CounterWithMetric

	// Basic CRUD statements.
	SelectCount telemetry.CounterWithMetric
	UpdateCount telemetry.CounterWithMetric
	InsertCount telemetry.CounterWithMetric
	DeleteCount telemetry.CounterWithMetric

	// Transaction operations.
	TxnBeginCount    telemetry.CounterWithMetric
	TxnCommitCount   telemetry.CounterWithMetric
	TxnRollbackCount telemetry.CounterWithMetric

	// Savepoint operations. SavepointCount is for real SQL savepoints
	// (which we don't yet support; this is just a placeholder for
	// telemetry); the RestartSavepoint variants are for the
	// znbase-specific client-side retry protocol.
	SavepointCount                  telemetry.CounterWithMetric
	ReleaseSavepointCount           telemetry.CounterWithMetric
	RollbackToSavepointCount        telemetry.CounterWithMetric
	RestartSavepointCount           telemetry.CounterWithMetric
	ReleaseRestartSavepointCount    telemetry.CounterWithMetric
	RollbackToRestartSavepointCount telemetry.CounterWithMetric

	// DdlCount counts all statements whose StatementType is DDL.
	DdlCount telemetry.CounterWithMetric

	// MiscCount counts all statements not covered by a more specific stat above.
	MiscCount telemetry.CounterWithMetric
}

func makeStatementCounters(internal bool) StatementCounters {
	return StatementCounters{
		TxnBeginCount:         telemetry.NewCounterWithMetric(getMetricMeta(MetaTxnBegin, internal)),
		TxnCommitCount:        telemetry.NewCounterWithMetric(getMetricMeta(MetaTxnCommit, internal)),
		TxnRollbackCount:      telemetry.NewCounterWithMetric(getMetricMeta(MetaTxnRollback, internal)),
		SavepointCount:        telemetry.NewCounterWithMetric(getMetricMeta(MetaSavepoint, internal)),
		RestartSavepointCount: telemetry.NewCounterWithMetric(getMetricMeta(MetaRestartSavepoint, internal)),
		ReleaseRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseRestartSavepoint, internal)),
		RollbackToRestartSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToRestartSavepoint, internal)),
		ReleaseSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaReleaseSavepointStarted, internal)),
		RollbackToSavepointCount: telemetry.NewCounterWithMetric(
			getMetricMeta(MetaRollbackToSavepointStarted, internal)),
		SelectCount: telemetry.NewCounterWithMetric(getMetricMeta(MetaSelect, internal)),
		UpdateCount: telemetry.NewCounterWithMetric(getMetricMeta(MetaUpdate, internal)),
		InsertCount: telemetry.NewCounterWithMetric(getMetricMeta(MetaInsert, internal)),
		DeleteCount: telemetry.NewCounterWithMetric(getMetricMeta(MetaDelete, internal)),
		DdlCount:    telemetry.NewCounterWithMetric(getMetricMeta(MetaDdl, internal)),
		MiscCount:   telemetry.NewCounterWithMetric(getMetricMeta(MetaMisc, internal)),
		QueryCount:  telemetry.NewCounterWithMetric(getMetricMeta(MetaQuery, internal)),
	}
}

func (sc *StatementCounters) incrementCount(ex *connExecutor, stmt tree.Statement) {
	sc.QueryCount.Inc()
	switch t := stmt.(type) {
	case *tree.BeginTransaction:
		sc.TxnBeginCount.Inc()
	case *tree.Select:
		sc.SelectCount.Inc()
	case *tree.Update:
		sc.UpdateCount.Inc()
	case *tree.Insert:
		sc.InsertCount.Inc()
	case *tree.Delete:
		sc.DeleteCount.Inc()
	case *tree.CommitTransaction:
		sc.TxnCommitCount.Inc()
	case *tree.RollbackTransaction:
		sc.TxnRollbackCount.Inc()
	case *tree.Savepoint:
		if ex.isCommitOnReleaseSavepoint(t.Name) {
			sc.RestartSavepointCount.Inc()
		} else {
			sc.SavepointCount.Inc()
		}
		//if err := ex.validateSavepointName(t.Name); err == nil {
		//	sc.RestartSavepointCount.Inc()
		//} else {
		//	sc.SavepointCount.Inc()
		//}
	case *tree.ReleaseSavepoint:
		if ex.isCommitOnReleaseSavepoint(t.Savepoint) {
			sc.ReleaseRestartSavepointCount.Inc()
		} else {
			sc.ReleaseSavepointCount.Inc()
		}
		//sc.ReleaseRestartSavepointCount.Inc()
	case *tree.RollbackToSavepoint:
		// sc.RollbackToRestartSavepointCount.Inc()
		if ex.isCommitOnReleaseSavepoint(t.Savepoint) {
			sc.RollbackToRestartSavepointCount.Inc()
		} else {
			sc.RollbackToSavepointCount.Inc()
		}
	default:
		if tree.CanModifySchema(stmt) {
			sc.DdlCount.Inc()
		} else {
			sc.MiscCount.Inc()
		}
	}
}

// connExPrepStmtsAccessor is an implementation of preparedStatementsAccessor
// that gives access to a connExecutor's prepared statements.
type connExPrepStmtsAccessor struct {
	ex *connExecutor
}

var _ preparedStatementsAccessor = connExPrepStmtsAccessor{}

// Get is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) Get(name string) (*PreparedStatement, bool) {
	s, ok := ps.ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
	return s, ok
}

// Delete is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) Delete(ctx context.Context, name string) bool {
	_, ok := ps.Get(name)
	if !ok {
		return false
	}
	ps.ex.deletePreparedStmt(ctx, name)
	return true
}

// DeleteAll is part of the preparedStatementsAccessor interface.
func (ps connExPrepStmtsAccessor) DeleteAll(ctx context.Context) {
	ps.ex.extraTxnState.prepStmtsNamespace.resetTo(ctx, prepStmtNamespace{})
}

// contextStatementKey is an empty type for the handle associated with the
// statement value (see context.Value).
type contextStatementKey struct{}

// withStatement adds a SQL statement to the provided context. The statement
// will then be included in crash reports which use that context.
func withStatement(ctx context.Context, stmt tree.Statement) context.Context {
	return context.WithValue(ctx, contextStatementKey{}, stmt)
}

// statementFromCtx returns the statement value from a context, or nil if unset.
func statementFromCtx(ctx context.Context) tree.Statement {
	stmt := ctx.Value(contextStatementKey{})
	if stmt == nil {
		return nil
	}
	return stmt.(tree.Statement)
}

func init() {
	// Register a function to include the anonymized statement in crash reports.
	log.RegisterTagFn("statement", func(ctx context.Context) string {
		stmt := statementFromCtx(ctx)
		if stmt == nil {
			return ""
		}
		// Anonymize the statement for reporting.
		return anonymizeStmtAndConstants(stmt)
	})
}
