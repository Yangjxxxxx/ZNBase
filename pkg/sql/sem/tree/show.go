// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import "github.com/znbasedb/znbase/pkg/sql/lex"

// ShowVar represents a SHOW statement.
type ShowVar struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (n *ShowVar) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	// Session var names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize, func() {
		ctx.FormatNameP(&n.Name)
	})
}

// ShowClusterSetting represents a SHOW CLUSTER SETTING statement.
type ShowClusterSetting struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (n *ShowClusterSetting) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CLUSTER SETTING ")
	// Cluster setting names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize, func() {
		ctx.FormatNameP(&n.Name)
	})
}

// ShowClusterSettingList represents a SHOW CLUSTER SETTINGS statement.
type ShowClusterSettingList struct{}

// Format implements the NodeFormatter interface.
func (node *ShowClusterSettingList) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	ctx.WriteString("ALL")
	ctx.WriteString(" CLUSTER SETTINGS")
}

// BackupDetails represents the type of details to display for a SHOW BACKUP
// statement.
type BackupDetails int

const (
	// BackupDefaultDetails identifies a bare SHOW BACKUP statement.
	BackupDefaultDetails BackupDetails = iota
	// BackupRangeDetails identifies a SHOW BACKUP RANGES statement.
	BackupRangeDetails
	// BackupFileDetails identifies a SHOW BACKUP FILES statement.
	BackupFileDetails
)

// ShowBackup represents a SHOW BACKUP statement.
type ShowBackup struct {
	Path    Expr
	Details BackupDetails
}

// Format implements the NodeFormatter interface.
func (n *ShowBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW DUMP ")
	if n.Details == BackupRangeDetails {
		ctx.WriteString("RANGES ")
	} else if n.Details == BackupFileDetails {
		ctx.WriteString("FILES ")
	}
	ctx.FormatNode(n.Path)
}

// ShowKafka represents a SHOW KAFKA statement.
type ShowKafka struct {
	//Topic    Expr
	GroupID Expr
	URL     Expr
}

// Format implements the NodeFormatter interface.
func (n *ShowKafka) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW KAFKA ")
	//if n.Topic != nil {
	//	ctx.WriteString("TOPIC ")
	//	ctx.FormatNode(n.Topic)
	//} else
	if n.GroupID != nil {
		ctx.WriteString("GROUPID ")
		ctx.FormatNode(n.GroupID)
	} else if n.URL != nil {
		ctx.WriteString("URL ")
		ctx.FormatNode(n.URL)
	}
}

// ShowColumns represents a SHOW COLUMNS statement.
type ShowColumns struct {
	Table       TableName
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowColumns) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW COLUMNS FROM ")
	ctx.FormatNode(&node.Table)

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowDatabases represents a SHOW DATABASES statement.
type ShowDatabases struct {
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (n *ShowDatabases) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW DATABASES")
	if n.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowTraceType is an enum of SHOW TRACE variants.
type ShowTraceType string

// A list of the SHOW TRACE variants.
const (
	ShowTraceRaw     ShowTraceType = "TRACE"
	ShowTraceKV      ShowTraceType = "KV TRACE"
	ShowTraceReplica ShowTraceType = "EXPERIMENTAL_REPLICA TRACE"
)

// ShowTraceForSession represents a SHOW TRACE FOR SESSION statement.
type ShowTraceForSession struct {
	TraceType ShowTraceType
	Compact   bool
}

// Format implements the NodeFormatter interface.
func (n *ShowTraceForSession) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if n.Compact {
		ctx.WriteString("COMPACT ")
	}
	ctx.WriteString(string(n.TraceType))
	ctx.WriteString(" FOR SESSION")
}

// ShowIndexes represents a SHOW INDEX statement.
type ShowIndexes struct {
	Table       TableName
	Index       UnrestrictedName
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (n *ShowIndexes) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW INDEXES FROM ")
	ctx.FormatNode(&n.Table)

	if n.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowQueries represents a SHOW QUERIES statement
type ShowQueries struct {
	All     bool
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (n *ShowQueries) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if n.All {
		ctx.WriteString("ALL ")
	}
	if n.Cluster {
		ctx.WriteString("CLUSTER QUERIES")
	} else {
		ctx.WriteString("LOCAL QUERIES")
	}
}

// ShowJobs represents a SHOW JOBS statement
type ShowJobs struct {
	// If Automatic is true, show only automatically-generated jobs such
	// as automatic CREATE STATISTICS jobs. If Automatic is false, show
	// only non-automatically-generated jobs.
	Automatic bool
}

// Format implements the NodeFormatter interface.
func (n *ShowJobs) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if n.Automatic {
		ctx.WriteString("AUTOMATIC ")
	}
	ctx.WriteString("JOBS")
}

// ShowSessions represents a SHOW SESSIONS statement
type ShowSessions struct {
	All     bool
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (n *ShowSessions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if n.All {
		ctx.WriteString("ALL ")
	}
	if n.Cluster {
		ctx.WriteString("CLUSTER SESSIONS")
	} else {
		ctx.WriteString("LOCAL SESSIONS")
	}
}

// ShowSchemas represents a SHOW SCHEMAS statement.
type ShowSchemas struct {
	Database Name
}

// Format implements the NodeFormatter interface.
func (n *ShowSchemas) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SCHEMAS")
	if n.Database != "" {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&n.Database)
	}
}

// ShowSequences represents a SHOW SEQUENCES statement.
type ShowSequences struct {
	Database Name
}

// Format implements the NodeFormatter interface.
func (n *ShowSequences) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SEQUENCES")
	if n.Database != "" {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&n.Database)
	}
}

// ShowTables represents a SHOW TABLES statement.
type ShowTables struct {
	TableNamePrefix
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (n *ShowTables) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TABLES")
	if n.ExplicitSchema {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&n.TableNamePrefix)
	}

	if n.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowFunctions represents a SHOW FUNCTIONS statement.
type ShowFunctions struct {
	TableNamePrefix
	FuncName string
}

// Format implements the NodeFormatter interface.
func (n *ShowFunctions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW FUNCTIONS")
	if n.ExplicitSchema {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&n.TableNamePrefix)
	}
	if n.FuncName != "" {
		ctx.WriteString(" WITH NAME ")
		ctx.WriteString(n.FuncName)
	}
}

// ShowConstraints represents a SHOW CONSTRAINTS statement.
type ShowConstraints struct {
	Table       TableName
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (n *ShowConstraints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CONSTRAINTS FROM ")
	ctx.FormatNode(&n.Table)
	if n.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowGrants represents a SHOW GRANTS statement.
// TargetList is defined in grant.go.
type ShowGrants struct {
	Targets  *TargetList
	Grantees NameList
}

// Format implements the NodeFormatter interface.
func (n *ShowGrants) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW GRANTS")
	if n.Targets != nil {
		ctx.WriteString(" ON ")
		ctx.FormatNode(n.Targets)
	}
	if n.Grantees != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(&n.Grantees)
	}
}

// ShowRoleGrants represents a SHOW GRANTS ON ROLE statement.
type ShowRoleGrants struct {
	Roles    NameList
	Grantees NameList
}

// Format implements the NodeFormatter interface.
func (n *ShowRoleGrants) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW GRANTS ON ROLE")
	if n.Roles != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&n.Roles)
	}
	if n.Grantees != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(&n.Grantees)
	}
}

// ShowCreate represents a SHOW CREATE statement.
type ShowCreate struct {
	Name         TableName
	WithLocation bool
	ShowType     *string
}

// Format implements the NodeFormatter interface.
func (n *ShowCreate) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ")
	ctx.FormatNode(&n.Name)
}

// ShowCreateTrigger represents a SHOW CREATE Trigger statement.
type ShowCreateTrigger struct {
	Name      Name
	TableName TableName
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateTrigger) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ")
	ctx.FormatNode(&node.Name)
}

//ShowTriggers used to show trigger
type ShowTriggers struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowTriggers) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TRIGGERS")
}

//ShowTriggersOnTable used to show triggers from table
type ShowTriggersOnTable struct {
	Tablename TableName
}

// Format implements the NodeFormatter interface.
func (node *ShowTriggersOnTable) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TRIGGERS ON ")
	ctx.FormatNode(&node.Tablename)
}

// ShowSyntax represents a SHOW SYNTAX statement.
// This the most lightweight thing that can be done on a statement
// server-side: just report the statement that was entered without
// any processing. Meant for use for syntax checking on clients,
// when the client version might differ from the server.
type ShowSyntax struct {
	Statement string
}

// Format implements the NodeFormatter interface.
func (n *ShowSyntax) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SYNTAX ")
	ctx.WriteString(lex.EscapeSQLString(n.Statement))
}

// ShowTransactionStatus represents a SHOW TRANSACTION STATUS statement.
type ShowTransactionStatus struct {
}

// Format implements the NodeFormatter interface.
func (n *ShowTransactionStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TRANSACTION STATUS")
}

// ShowSavepointStatus represents a SHOW SAVEPOINT STATUS statement.
type ShowSavepointStatus struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowSavepointStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SAVEPOINT STATUS")
}

// ShowUsers represents a SHOW USERS statement.
type ShowUsers struct {
}

// Format implements the NodeFormatter interface.
func (n *ShowUsers) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW USERS")
}

// ShowRoles represents a SHOW ROLES statement.
type ShowRoles struct {
}

// Format implements the NodeFormatter interface.
func (n *ShowRoles) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ROLES")
}

// ShowSpaces represents a SHOW SPACES statement.
type ShowSpaces struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowSpaces) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SPACES")
}

// ShowRanges represents a SHOW EXPERIMENTAL_RANGES statement.
// Only one of Table and Index can be set.
type ShowRanges struct {
	Table *TableName
	Index *TableIndexName
}

// Format implements the NodeFormatter interface.
func (n *ShowRanges) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW EXPERIMENTAL_RANGES FROM ")
	if n.Index != nil {
		ctx.WriteString("INDEX ")
		ctx.FormatNode(n.Index)
	} else {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(n.Table)
	}
}

// ShowFingerprints represents a SHOW EXPERIMENTAL_FINGERPRINTS statement.
type ShowFingerprints struct {
	Table TableName
}

// Format implements the NodeFormatter interface.
func (n *ShowFingerprints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ")
	ctx.FormatNode(&n.Table)
}

// ShowTableStats represents a SHOW STATISTICS FOR TABLE statement.
type ShowTableStats struct {
	Table     TableName
	UsingJSON bool
}

// Format implements the NodeFormatter interface.
func (n *ShowTableStats) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW STATISTICS ")
	if n.UsingJSON {
		ctx.WriteString("USING JSON ")
	}
	ctx.WriteString("FOR TABLE ")
	ctx.FormatNode(&n.Table)
}

// ShowHistogram represents a SHOW HISTOGRAM statement.
type ShowHistogram struct {
	HistogramID int64
}

// Format implements the NodeFormatter interface.
func (n *ShowHistogram) Format(ctx *FmtCtx) {
	ctx.Printf("SHOW HISTOGRAM %d", n.HistogramID)
}

// ShowSchedules represents a SHOW SCHEDULES statement.
type ShowSchedules struct {
	ScheduleID Expr
}

// StatAbbr implements the StatAbbr interface.
func (n *ShowSchedules) StatAbbr() string {
	return "ShowSchedules"
}

// StatObj implements the StatObj interface.
func (n *ShowSchedules) StatObj() string {
	return ""
}

var _ Statement = &ShowSchedules{}

// Format implements the NodeFormatter interface.
func (n *ShowSchedules) Format(ctx *FmtCtx) {
	if n.ScheduleID != nil {
		ctx.Printf("SHOW SCHEDULE %s", AsString(n.ScheduleID))
		return
	}
	ctx.Printf("SHOW SCHEDULES")
}
