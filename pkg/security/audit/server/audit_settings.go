package server

import "github.com/znbasedb/znbase/pkg/settings"

//AuditSetting define
type AuditSetting string

const (
	//DefaultChannelDataCapacity define
	DefaultChannelDataCapacity = 100
	//DefaultChannelStateCapacity define
	DefaultChannelStateCapacity = 5
	//DefaultSQLLengthLimit  define
	DefaultSQLLengthLimit = 10000
	//DefaultRefreshInterval define
	DefaultRefreshInterval = 10
	//DefaultParallelism define
	DefaultParallelism = 3
	//DefaultStopTimeout define (ms)
	DefaultStopTimeout = 5000
	//DefaultWorkTimeout define
	DefaultWorkTimeout = 500
	//DefaultWaitTimeout define
	DefaultWaitTimeout = 500

	//SettingSQLLength :sql length setting
	SettingSQLLength AuditSetting = "sqlLength"
	//SettingSQLLengthBypass : bypass of the sql length limitation
	SettingSQLLengthBypass AuditSetting = "sqlLengthBypass"
	//SettingSQLInjectBypass : bypass the detection for sql injection or not
	SettingSQLInjectBypass AuditSetting = "sqlInjectBypass"
	//SettingAuditLogEnable : the switch of audit server
	SettingAuditLogEnable AuditSetting = "auditLogEnable"
)

var (
	// audit settings refresh interval
	refreshInterval = settings.RegisterIntSetting(
		"audit.refresh.interval",
		"interval seconds for settings refresh",
		DefaultRefreshInterval,
	)

	// set sql length
	sqlLength = settings.RegisterIntSetting(
		"audit.sql.length",
		"the limitation of the sql length",
		DefaultSQLLengthLimit,
	)

	//set whether the sql length injectBypass be opened
	sqlLengthBypass = settings.RegisterBoolSetting(
		"audit.sql.length.bypass.enabled",
		"the switch whether the limitation for sql length be opened",
		true,
	)

	// set if sql inject detection open
	sqlInjectBypass = settings.RegisterBoolSetting(
		"audit.sql.inject.bypass.enabled",
		"the switch whether sql injection open",
		true,
	)

	// set event audit disable list
	eventDisableList = settings.RegisterStringSetting(
		"audit.event.disable.list",
		"the list of event audit disable, use ',' as separator",
		"",
	)

	//audit log disable switch
	auditLogEnable = settings.RegisterBoolSetting(
		"audit.log.enabled",
		"the switch of the audit log",
		true,
	)
)
