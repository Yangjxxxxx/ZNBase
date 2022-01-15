package server

import (
	"context"
	"sync"
	"time"

	"github.com/znbasedb/znbase/pkg/security/audit/task"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// AuditServer used to log events to table and log file, metric event and do actions when trigger rules
type AuditServer struct {
	ctx        context.Context   // context from server
	cs         *cluster.Settings // cluster settings
	runner     *task.Runner      // used to handle audit event data
	nodeID     int32             // current nodeID used to log events as default reporting id
	logHandler LogHandler        // log handler
	settings   sync.Map          // settings for audit server
	base       *BaseInfo         // server base info
	chanState  chan bool         // audit server state channel
	registry   *metric.Registry  //add registry to collect runner worker metrics
}

// BaseInfo hold all base info for audit server
type BaseInfo struct {
	status    bool // server already started or not
	forceSync bool // if true, will disable async and all event be process sync
}

// AuditInfo base info for audit log
type AuditInfo struct {
	EventTime    time.Time       // time when event happened
	EventType    string          // event type
	ClientInfo   *ClientInfo     // client info
	AppName      string          // client application name
	TargetInfo   *TargetInfo     // opt target
	ReportID     int32           // where the opt happened, always be the node id for now
	Opt          string          // opt
	OptParam     string          // param for opt
	OptTime      int64           // opt exec duration time(microsecond)
	AffectedSize int             // object size affected by this event
	Result       string          // event result, ok or error:error code
	Info         interface{}     // extra data
	Ctx          context.Context // event ctx, get node/client/user from this
}

// ClientInfo with client base info
type ClientInfo struct {
	NodeID int32  // node id where event happened
	Client string // client host with port
	User   string // user name
}

// TargetInfo with target id and detail
type TargetInfo struct {
	TargetID int32
	Desc     interface{}
}

// LogHandler as interface to log audit data
type LogHandler interface {
	LogAudit(auditLog *AuditInfo) error // log audit data to db and file system
	IsAudit(eventType string) bool      // check if eventType need to be audit
	Disable(eventTypeList string)       // disable audit for eventType in list
}

// NewAuditServer return a new initialed audit server
func NewAuditServer(ctx context.Context, cs *cluster.Settings, mr *metric.Registry) *AuditServer {

	// define new audit server
	auditServer := &AuditServer{
		ctx:      ctx,
		cs:       cs,
		settings: sync.Map{},
		base: &BaseInfo{
			forceSync: false,
		},
		chanState: make(chan bool, DefaultChannelStateCapacity),
		registry:  mr,
	}
	// reload last saved or default value for settings
	auditServer.refreshSettings()

	return auditServer
}

// InitLogHandler init log handler
func (as *AuditServer) InitLogHandler(handler LogHandler) {
	as.logHandler = handler
	as.runner = task.NewRunner(
		as.ctx,
		"audit_server",
		DefaultChannelDataCapacity,
		DefaultParallelism,
		DefaultStopTimeout,
		DefaultWorkTimeout,
		func(data interface{}) error {
			return as.logHandler.LogAudit(data.(*AuditInfo))
		},
		as.registry,
	)
}

// InitNodeID using node id after node started
func (as *AuditServer) InitNodeID(nodeID int32) {
	as.nodeID = nodeID
}

// Start to handle event async and refresh audit settings by specified interval
func (as *AuditServer) Start(ctx context.Context, s *stop.Stopper) {
	//	Start a goroutine to refresh settings every 'audit.refresh.interval' interval
	s.RunWorker(
		ctx, func(ct context.Context) {
			refresh := timeutil.NewTimer()
			defer refresh.Stop()
			refresh.Reset(0 * time.Second)
			for {
				select {
				case <-refresh.C:
					log.Infof(ct, "refresh audit server settings")
					// todo(xz): Disable will execute even if the list not refreshed
					// TODO maybe use function SetOnChange as callback
					as.logHandler.Disable(eventDisableList.Get(&as.cs.SV))
					as.refreshSettings()
					if !as.base.status {
						as.base.status = true

						// TODO better to timeout this write
						as.chanState <- true
					}
					refresh.Read = true
					refresh.Reset(time.Duration(refreshInterval.Get(&as.cs.SV)) * time.Second)
				case <-s.ShouldStop():
					log.Infof(ct, "stop refresh audit server settings")
					return
				}
			}
		},
	)

	// Start goroutine to handle async audit event
	s.RunWorker(ctx, func(ctx context.Context) {
		as.runner.Start()
		for {
			select {
			case <-s.ShouldStop():
				log.Infof(ctx, "stop audit server")
				as.runner.Stop()
				return
			}
		}
	})
}

// LogAudit log audit info to table system.eventlog and audit file with sync or async
func (as *AuditServer) LogAudit(ctx context.Context, sync bool, auditLog *AuditInfo) error {

	// audit server not start, so only support async
	if !as.base.status {
		sync = false
	} else {
		// if enable force sync
		if as.base.forceSync {
			sync = true
		}
	}

	//	reset event time if necessary
	if auditLog.EventTime.IsZero() {
		auditLog.EventTime = timeutil.Now()
	}
	// reset event ctx if nil
	auditLog.Ctx = ctx
	// reset event client info with node id
	if auditLog.ClientInfo == nil {
		auditLog.ClientInfo = &ClientInfo{NodeID: as.nodeID}
	} else {
		auditLog.ClientInfo.NodeID = as.nodeID
	}
	// reset event target if nil
	if auditLog.TargetInfo == nil {
		auditLog.TargetInfo = &TargetInfo{}
	}
	if auditLog.Info == nil {
		auditLog.Info = ""
	}
	// reset event report id
	auditLog.ReportID = as.nodeID

	//if the log func disabled, don't do audit log
	if as.GetSetting(SettingAuditLogEnable).(bool) {
		// if async, push to box
		if !sync {
			return as.runner.PutData(auditLog)
		}
		// handle event synchronously
		return as.logHandler.LogAudit(auditLog)
	}
	return nil
}

// GetSetting return audit settings for key
// TODO better to use default value instead of nil
func (as *AuditServer) GetSetting(key AuditSetting) interface{} {
	if v, ok := as.settings.Load(key); ok {
		return v
	}
	return nil
}

// IsAudit check if event need to be audit
func (as *AuditServer) IsAudit(eventType string) bool {

	// TODO remove this check after refactor audit server to support test that not using audit
	if as == nil || as.logHandler == nil {
		return false
	}
	return as.logHandler.IsAudit(eventType)
}

// GetHandler return handler used by audit server
func (as *AuditServer) GetHandler() LogHandler {
	return as.logHandler
}

// EnableForceSync force all event process synchronously
func (as *AuditServer) EnableForceSync() {
	as.base.forceSync = true
}

// WaitUntilReady wait util audit server state is ready
// used to test
func (as *AuditServer) WaitUntilReady() {
	for {
		select {
		case s := <-as.chanState:
			if s {
				return
			}
		case <-time.After(DefaultWaitTimeout * time.Microsecond):
			log.Warningf(as.ctx, "take too much time to wait until ready")
		}
	}
}

// ResetRetry reset runner write retry
func (as *AuditServer) ResetRetry(retry int) {
	as.runner.UpdateRetry(retry)
}

// refreshSettings load settings in db
func (as *AuditServer) refreshSettings() {
	as.settings.Store(SettingSQLLength, int(sqlLength.Get(&as.cs.SV)))
	as.settings.Store(SettingSQLInjectBypass, sqlInjectBypass.Get(&as.cs.SV))
	as.settings.Store(SettingSQLLengthBypass, sqlLengthBypass.Get(&as.cs.SV))
	as.settings.Store(SettingAuditLogEnable, auditLogEnable.Get(&as.cs.SV))
}

// TestConfig reset some config for test
func (as *AuditServer) TestConfig() {
	as.runner.TestConfig()
}

// TestSyncConfig sync
func (as *AuditServer) TestSyncConfig() {
	as.EnableForceSync()
}
