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

import (
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/security/audit/util"
)

// Instructions for creating new types: If a type needs to satisfy an
// interface, declare that function along with that interface. This
// will help users identify the list of types to which they can assert
// those interfaces. If the member of a type has a string with a
// predefined list of values, declare those values as const following
// the type. For interfaces that define dummy functions to
// consolidate a set of types, define the function as typeName().
// This will help avoid name collisions.

// StatementType is the enumerated type for Statement return styles on
// the wire.
type StatementType int

//go:generate stringer -type=StatementType
const (
	// Ack indicates that the statement does not have a meaningful
	// return. Examples include SET, BEGIN, COMMIT.
	Ack StatementType = iota
	// DDL indicates that the statement mutates the database schema.
	//
	// Note: this is the type indicated back to the client; it is not a
	// sufficient test for schema mutation for planning purposes. There
	// are schema-modifying statements (e.g. CREATE TABLE AS) which
	// report RowsAffected to the client, not DDL.
	// Use CanModifySchema() below instead.
	DDL
	// RowsAffected indicates that the statement returns the count of
	// affected rows.
	RowsAffected
	// Rows indicates that the statement returns the affected rows after
	// the statement was applied.
	Rows
	// CopyIn indicates a COPY FROM statement.
	CopyIn
	// Unknown indicates that the statement does not have a known
	// return style at the time of parsing. This is not first in the
	// enumeration because it is more convenient to have Ack as a zero
	// value, and because the use of Unknown should be an explicit choice.
	// The primary example of this statement type is EXECUTE, where the
	// statement type depends on the statement type of the prepared statement
	// being executed.
	Unknown
)

// Statement represents a statement.
type Statement interface {
	fmt.Stringer
	NodeFormatter
	StatementType() StatementType
	// StatementTag is a short string identifying the type of statement
	// (usually a single verb). This is different than the Stringer output,
	// which is the actual statement (including args).
	// TODO(dt): Currently tags are always pg-compatible in the future it
	// might make sense to pass a tag format specifier.
	StatementTag() string

	// StatAbbr represent statement abbreviation
	StatAbbr() string

	// StatObj which involved in statement
	StatObj() string
}

// canModifySchema is to be implemented by statements that can modify
// the database schema but may have StatementType() != DDL.
// See CanModifySchema() below.
type canModifySchema interface {
	modifiesSchema() bool
}

// CanModifySchema returns true if the statement can modify
// the database schema.
func CanModifySchema(stmt Statement) bool {
	if stmt.StatementType() == DDL {
		return true
	}
	scm, ok := stmt.(canModifySchema)
	return ok && scm.modifiesSchema()
}

// CanWriteData returns true if the statement can modify data.
func CanWriteData(stmt Statement) bool {
	switch stmt.(type) {
	// Normal write operations.
	case *Insert, *Delete, *Update, *Truncate:
		return true
	// Import operations.
	case *CopyFrom, *LoadImport, *LoadRestore:
		return true
	// ZNBaseDB extensions.
	case *Split, *Relocate, *Scatter:
		return true
	}
	return false
}

// IsStmtParallelized determines if a given statement's execution should be
// parallelized. This means that its results should be mocked out, and that
// it should be run asynchronously and in parallel with other statements that
// are independent.
func IsStmtParallelized(stmt Statement) bool {
	parallelizedRetClause := func(ret ReturningClause) bool {
		_, ok := ret.(*ReturningNothing)
		return ok
	}
	switch s := stmt.(type) {
	case *Delete:
		return parallelizedRetClause(s.Returning)
	case *Insert:
		return parallelizedRetClause(s.Returning)
	case *Update:
		return parallelizedRetClause(s.Returning)
	}
	return false
}

// HiddenFromShowQueries is a pseudo-interface to be implemented
// by statements that should not show up in SHOW QUERIES (and are hence
// not cancellable using CANCEL QUERIES either). Usually implemented by
// statements that spawn jobs.
type HiddenFromShowQueries interface {
	hiddenFromShowQueries()
}

// ObserverStatement is a marker interface for statements which are allowed to
// run regardless of the current transaction state: statements other than
// rollback are generally rejected if the session is in a failed transaction
// state, but it's convenient to allow some statements (e.g. "show syntax; set
// tracing").
// Such statements are not expected to modify the database, the transaction or
// session state (other than special cases such as enabling/disabling tracing).
//
// These statements short-circuit the regular execution - they don't get planned
// (there are no corresponding planNodes). The connExecutor recognizes them and
// handles them.
type ObserverStatement interface {
	observerStatement()
}

// CCLOnlyStatement is a marker interface for statements that require
// a CCL binary for successful planning or execution.
// It is used to enhance error messages when attempting to use these
// statements in non-CCL binaries.
type CCLOnlyStatement interface {
	cclOnlyStatement()
}

var _ CCLOnlyStatement = &Backup{}
var _ CCLOnlyStatement = &ShowBackup{}
var _ CCLOnlyStatement = &ShowKafka{}
var _ CCLOnlyStatement = &CreateChangefeed{}
var _ CCLOnlyStatement = &LoadRestore{}
var _ CCLOnlyStatement = &LoadImport{}
var _ CCLOnlyStatement = &CreateSnapshot{}
var _ CCLOnlyStatement = &DropSnapshot{}
var _ CCLOnlyStatement = &ShowSnapshot{}
var _ CCLOnlyStatement = &RevertSnapshot{}
var _ CCLOnlyStatement = &CreateRole{}
var _ CCLOnlyStatement = &DropRole{}
var _ CCLOnlyStatement = &GrantRole{}
var _ CCLOnlyStatement = &RevokeRole{}
var _ CCLOnlyStatement = &ScheduledBackup{}

func (n *Backup) cclOnlyStatement()           {}
func (n *ShowBackup) cclOnlyStatement()       {}
func (n *ShowKafka) cclOnlyStatement()        {}
func (c *CreateChangefeed) cclOnlyStatement() {}
func (n *LoadRestore) cclOnlyStatement()      {}
func (n *LoadImport) cclOnlyStatement()       {}
func (n *CreateSnapshot) cclOnlyStatement()   {}
func (n *DropSnapshot) cclOnlyStatement()     {}
func (n *ShowSnapshot) cclOnlyStatement()     {}
func (n *RevertSnapshot) cclOnlyStatement()   {}
func (n *CreateRole) cclOnlyStatement()       {}
func (n *DropRole) cclOnlyStatement()         {}
func (n *GrantRole) cclOnlyStatement()        {}
func (n *RevokeRole) cclOnlyStatement()       {}
func (n *ScheduledBackup) cclOnlyStatement()  {}

// StatementType implements the Statement interface.
func (*AlterIndex) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*AlterIndex) StatAbbr() string { return "alter_index" }

// StatObj implements the StatObj interface.
func (n *AlterIndex) StatObj() string {
	return util.StringConcat("-", n.Index.Table.Table(), n.Index.Index.String())
}

// StatementTag returns a short string identifying the type of statement.
func (*AlterIndex) StatementTag() string { return "ALTER INDEX" }

func (*AlterIndex) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*AlterTable) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*AlterTable) StatAbbr() string { return "alter_table" }

// StatObj implements the StatObj interface.
func (n *AlterTable) StatObj() string { return n.Table.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTable) StatementTag() string { return "ALTER TABLE" }

func (*AlterTable) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*AlterSchema) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterSchema) StatementTag() string { return "ALTER SCHEMA" }

// StatAbbr implements the StatAbbr interface.
func (*AlterSchema) StatAbbr() string { return "alter_schema" }

// StatObj implements the StatObj interface.
func (n *AlterSchema) StatObj() string { return n.Schema.String() + " -> " + n.NewSchema.String() }

// StatementType implements the Statement interface.
func (*AlterSequence) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*AlterSequence) StatAbbr() string { return "alter_sequence" }

// StatObj implements the StatObj interface.
func (n *AlterSequence) StatObj() string { return n.Name.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*AlterSequence) StatementTag() string { return "ALTER SEQUENCE" }

// StatementType implements the Statement interface.
func (*AlterUser) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*AlterUser) StatAbbr() string { return "alter_user_set_password" }

// StatObj implements the StatObj interface.
func (n *AlterUser) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*AlterUser) StatementTag() string { return "ALTER USER" }

func (*AlterUser) hiddenFromShowQueries() {}

var _ Statement = &AlterUserDefaultSchma{}

// StatementType implements the Statement interface.
func (*AlterUserDefaultSchma) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*AlterUserDefaultSchma) StatAbbr() string { return "alter_user_set_searchPath" }

// StatObj implements the StatObj interface.
func (n *AlterUserDefaultSchma) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*AlterUserDefaultSchma) StatementTag() string { return "ALTER ROLE" }

func (*AlterUserDefaultSchma) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*AlterViewAS) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterViewAS) StatementTag() string { return "ALTER VIEW" }

// StatAbbr implements the StatAbbr interface.
func (*AlterViewAS) StatAbbr() string { return "alter_view" }

// StatObj implements the StatObj interface.
func (n *AlterViewAS) StatObj() string { return n.Name.Table() }

func (*AlterViewAS) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*Backup) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*Backup) StatAbbr() string { return "backup" }

// StatObj implements the StatObj interface.
func (n *Backup) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Backup) StatementTag() string { return "BACKUP" }

func (*Backup) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*ScheduledBackup) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ScheduledBackup) StatementTag() string { return "SCHEDULED BACKUP" }

// StatAbbr implements the StatAbbr interface.
func (*ScheduledBackup) StatAbbr() string { return "SCHEDULE" }

// StatObj implements the StatObj interface.
func (*ScheduledBackup) StatObj() string { return "" }

func (*ScheduledBackup) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*BeginTransaction) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*BeginTransaction) StatAbbr() string { return "begin_transaction" }

// StatObj implements the StatObj interface.
func (*BeginTransaction) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*BeginTransaction) StatementTag() string { return "BEGIN" }

// StatementType implements the Statement interface.
func (*BeginCommit) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*BeginCommit) StatementTag() string { return "" }

// StatAbbr implements the StatAbbr interface.
func (*BeginCommit) StatAbbr() string { return "" }

// StatObj implements the StatObj interface.
func (n *BeginCommit) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*ControlJobs) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*ControlJobs) StatAbbr() string { return "control_jobs" }

// StatObj implements the StatObj interface.
func (c *ControlJobs) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (c *ControlJobs) StatementTag() string {
	return fmt.Sprintf("%s JOBS", JobCommandToStatement[c.Command])
}

// StatementType implements the Statement interface.
func (*ControlSchedules) StatementType() StatementType { return RowsAffected }

// StatementTag returns a short string identifying the type of statement.
func (c *ControlSchedules) StatementTag() string {
	return fmt.Sprintf("%s SCHEDULES", c.Command)
}

// StatementType implements the Statement interface.
func (*CancelQueries) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*CancelQueries) StatAbbr() string { return "cancel_queries" }

// StatObj implements the StatObj interface.
func (*CancelQueries) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*CancelQueries) StatementTag() string { return "CANCEL QUERIES" }

// StatementType implements the Statement interface.
func (*CancelSessions) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*CancelSessions) StatAbbr() string { return "cancel_sessions" }

// StatObj implements the StatObj interface.
func (n *CancelSessions) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*CancelSessions) StatementTag() string { return "CANCEL SESSIONS" }

// StatementType implements the Statement interface.
func (*CannedOptPlan) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*CannedOptPlan) StatAbbr() string { return "cancel_opt_plan" }

// StatObj implements the StatObj interface.
func (*CannedOptPlan) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*CannedOptPlan) StatementTag() string { return "PREPARE AS OPT PLAN" }

// StatementType implements the Statement interface.
func (*CommentOnColumn) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CommentOnColumn) StatAbbr() string { return "comment_on_column" }

// StatObj implements the StatObj interface.
func (n *CommentOnColumn) StatObj() string { return n.Column() }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnColumn) StatementTag() string { return "COMMENT ON COLUMN" }

// StatementType implements the Statement interface.
func (*CommentOnDatabase) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CommentOnDatabase) StatAbbr() string { return "comment_on_database" }

// StatObj implements the StatObj interface.
func (n *CommentOnDatabase) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnDatabase) StatementTag() string { return "COMMENT ON DATABASE" }

// StatementType implements the Statement interface.
func (*CommentOnTable) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CommentOnTable) StatAbbr() string { return "comment_on_table" }

// StatObj implements the StatObj interface.
func (n *CommentOnTable) StatObj() string { return n.Table.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnTable) StatementTag() string { return "COMMENT ON TABLE" }

// StatementType implements the Statement interface.
func (*CommentOnIndex) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CommentOnIndex) StatAbbr() string { return "comment_on_index" }

// StatObj implements the StatObj interface.
func (n *CommentOnIndex) StatObj() string { return n.Index.String() }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnIndex) StatementTag() string { return "COMMENT ON INDEX" }

// StatementType implements the Statement interface.
func (*CommentOnConstraint) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CommentOnConstraint) StatAbbr() string { return "comment_on_constraint" }

// StatObj implements the StatObj interface.
func (n *CommentOnConstraint) StatObj() string { return n.Constraint.String() }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnConstraint) StatementTag() string { return "COMMENT ON CONSTRAINT" }

// StatementType implements the Statement interface.
func (*CommitTransaction) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*CommitTransaction) StatAbbr() string { return "commit_transaction" }

// StatObj implements the StatObj interface.
func (*CommitTransaction) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*CommitTransaction) StatementTag() string { return "COMMIT" }

// StatementType implements the Statement interface.
func (*CopyFrom) StatementType() StatementType { return CopyIn }

// StatAbbr implements the StatAbbr interface.
func (*CopyFrom) StatAbbr() string { return "copy_from" }

// StatObj implements the StatObj interface.
func (n *CopyFrom) StatObj() string { return n.Table.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*CopyFrom) StatementTag() string { return "COPY" }

// StatementType implements the Statement interface.
func (*CreateChangefeed) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*CreateChangefeed) StatAbbr() string { return "create_change_feed" }

// StatObj implements the StatObj interface.
func (c *CreateChangefeed) StatObj() string {
	return c.Targets.Databases.String()
}

// StatementTag returns a short string identifying the type of statement.
func (c *CreateChangefeed) StatementTag() string {
	if c.SinkURI == nil {
		return "EXPERIMENTAL CHANGEFEED"
	}
	return "CREATE CHANGEFEED"
}

// StatementType implements the Statement interface.
func (*CreateDatabase) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CreateDatabase) StatAbbr() string { return "create_database" }

// StatObj implements the StatObj interface.
func (n *CreateDatabase) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*CreateDatabase) StatementTag() string { return "CREATE DATABASE" }

// StatementType implements the Statement interface.
func (n *CreateTrigger) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateTrigger) StatementTag() string { return "CREATE TRIGGER" }

// StatAbbr implements the StatAbbr interface.
func (*CreateTrigger) StatAbbr() string { return "create_trig_clause" }

// StatObj implements the StatObj interface.
func (*CreateTrigger) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*CreateIndex) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CreateIndex) StatAbbr() string { return "create_index" }

// StatObj implements the StatObj interface.
func (n *CreateIndex) StatObj() string {
	return util.StringConcat("-", n.Table.Table(), n.Name.String())
}

// StatementTag returns a short string identifying the type of statement.
func (*CreateIndex) StatementTag() string { return "CREATE INDEX" }

// StatementType implements the Statement interface.
func (c *CreateTable) StatementType() StatementType {
	if c.As() {
		return RowsAffected
	}
	return DDL
}

// StatAbbr implements the StatAbbr interface.
func (*CreateTable) StatAbbr() string { return "create_table" }

// StatObj implements the StatObj interface.
func (c *CreateTable) StatObj() string { return c.Table.Table() }

// StatementTag returns a short string identifying the type of statement.
func (c *CreateTable) StatementTag() string {
	if c.As() {
		return "SELECT"
	}
	return "CREATE TABLE"
}

// StatementType implements the Statement interface.
func (n *CreateFunction) StatementType() StatementType {
	return DDL
}

// StatementTag returns a short string identifying the type of statement.
func (n *CreateFunction) StatementTag() string {
	if n.IsProcedure {
		return "CREATE PROCEDURE"
	}
	return "CREATE FUNCTION"
}

// StatAbbr implements the StatAbbr interface.
func (n *CreateFunction) StatAbbr() string {
	if n.IsProcedure {
		return "create_procedure"
	}
	return "create_function"
}

// StatObj implements the StatObj interface.
func (n *CreateFunction) StatObj() string { return n.FuncName.String() }

func (n *CreateFunction) String() string { return AsString(n) }

// StatementType implements the Statement interface.
func (n *CallStmt) StatementType() StatementType {
	return Rows
}

// StatementTag returns a short string identifying the type of statement.
func (n *CallStmt) StatementTag() string { return "CALL FUNCTION" }

// StatAbbr implements the StatAbbr interface.
func (*CallStmt) StatAbbr() string { return "call_function" }

// StatObj implements the StatObj interface.
func (n *CallStmt) StatObj() string { return n.FuncName.String() }
func (n *CallStmt) String() string  { return AsString(n) }

// modifiesSchema implements the canModifySchema interface.
func (*CreateTable) modifiesSchema() bool { return true }

// StatementType implements the Statement interface.
func (*CreateUser) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*CreateUser) StatAbbr() string { return "create_user" }

// StatObj implements the StatObj interface.
func (n *CreateUser) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*CreateUser) StatementTag() string { return "CREATE USER" }

func (*CreateUser) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*CreateRole) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*CreateRole) StatAbbr() string { return "create_role" }

// StatObj implements the StatObj interface.
func (n *CreateRole) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*CreateRole) StatementTag() string { return "CREATE ROLE" }

func (*CreateRole) hiddenFromShowQueries() {}

// StatementType implements the Statement interface.
func (*CreateView) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CreateView) StatAbbr() string { return "create_view" }

// StatObj implements the StatObj interface.
func (n *CreateView) StatObj() string { return n.Name.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*CreateView) StatementTag() string { return "CREATE VIEW" }

// StatementType implements the Statement interface.
func (*CreateSchema) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateSchema) StatementTag() string { return "CREATE SCHEMA" }

// StatAbbr implements the StatAbbr interface.
func (*CreateSchema) StatAbbr() string { return "create_schema" }

// StatObj implements the StatObj interface.
func (n *CreateSchema) StatObj() string { return n.Schema.String() }

// StatementType implements the Statement interface.
func (*CreateSequence) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CreateSequence) StatAbbr() string { return "create_sequence" }

// StatObj implements the StatObj interface.
func (n *CreateSequence) StatObj() string { return n.Name.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*CreateSequence) StatementTag() string { return "CREATE SEQUENCE" }

// StatementType implements the Statement interface.
func (*CreateStats) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*CreateStats) StatAbbr() string { return "create_stats" }

// StatObj implements the StatObj interface.
func (n *CreateStats) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*CreateStats) StatementTag() string { return "CREATE STATISTICS" }

// StatementType implements the Statement interface.
func (*CursorAccess) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*CursorAccess) StatAbbr() string { return "cursor_access" }

// StatObj implements the StatObj interface.
func (n *CursorAccess) StatObj() string { return n.CursorName.String() }

// StatementTag returns a short string identifying the type of statement.
func (*CursorAccess) StatementTag() string { return "CURSOR ACCESS " }

// StatementType implements the Statement interface.
func (*Deallocate) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*Deallocate) StatAbbr() string { return "deallocate" }

// StatObj implements the StatObj interface.
func (d *Deallocate) StatObj() string { return d.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (d *Deallocate) StatementTag() string {
	// Postgres distinguishes the command tags for these two cases of Deallocate statements.
	if d.Name == "" {
		return "DEALLOCATE ALL"
	}
	return "DEALLOCATE"
}

// StatementType implements the Statement interface.
func (*Discard) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*Discard) StatAbbr() string { return "discard" }

// StatObj implements the StatObj interface.
func (d *Discard) StatObj() string { return string(rune(d.Mode)) }

// StatementTag returns a short string identifying the type of statement.
func (*Discard) StatementTag() string { return "DISCARD" }

// StatementType implements the Statement interface.
func (n *Delete) StatementType() StatementType { return n.Returning.statementType() }

// StatAbbr implements the StatAbbr interface.
func (n *Delete) StatAbbr() string { return "delete" }

// StatObj implements the StatObj interface.
func (n *Delete) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Delete) StatementTag() string { return "DELETE" }

// StatementType implements the Statement interface.
func (*DropDatabase) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*DropDatabase) StatAbbr() string { return "drop_database" }

// StatObj implements the StatObj interface.
func (n *DropDatabase) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DropDatabase) StatementTag() string { return "DROP DATABASE" }

// StatementType returns a short string identifying the type of statement.
func (*DropTrigger) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropTrigger) StatementTag() string { return "DROP TRIGGER" }

// StatAbbr implements the StatAbbr interface.
func (*DropTrigger) StatAbbr() string { return "drop_trigger_clause" }

// StatObj implements the StatObj interface.
func (*DropTrigger) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*DropIndex) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*DropIndex) StatAbbr() string { return "drop_index" }

// StatObj implements the StatObj interface.
func (n *DropIndex) StatObj() string {
	var sb strings.Builder
	for i, x := range n.IndexList {
		sb.WriteString(x.Table.Table())
		if i < len(n.IndexList)-1 {
			sb.WriteString("-")
		}
	}
	return sb.String()
}

// StatementTag returns a short string identifying the type of statement.
func (*DropIndex) StatementTag() string { return "DROP INDEX" }

// StatementType implements the Statement interface.
func (*DeclareCursor) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*DeclareCursor) StatAbbr() string { return "declare_cursor" }

// StatObj implements the StatObj interface.
func (n *DeclareCursor) StatObj() string { return n.Cursor.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DeclareCursor) StatementTag() string { return "DECLARE CURSOR" }

// StatementType implements the Statement interface.
func (*DeclareVariable) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*DeclareVariable) StatAbbr() string { return "declare_variable" }

// StatObj implements the StatObj interface.
func (n *DeclareVariable) StatObj() string { return n.VariableName.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DeclareVariable) StatementTag() string { return "DECLARE VARIABLE" }

// StatementType implements the Statement interface.
func (*VariableAccess) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*VariableAccess) StatAbbr() string { return "variable_access" }

// StatObj implements the StatObj interface.
func (n *VariableAccess) StatObj() string { return n.VariableName.String() }

// StatementTag returns a short string identifying the type of statement.
func (*VariableAccess) StatementTag() string { return "VARIABLE ACCESS" }

// StatementType implements the Statement interface.
func (*DropTable) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*DropTable) StatAbbr() string { return "drop_table" }

// StatObj implements the StatObj interface.
func (n *DropTable) StatObj() string { return n.Names.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DropTable) StatementTag() string { return "DROP TABLE" }

// StatementType implements the Statement interface.
func (*DropView) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*DropView) StatAbbr() string { return "drop_view" }

// StatObj implements the StatObj interface.
func (n *DropView) StatObj() string { return n.Names.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DropView) StatementTag() string { return "DROP VIEW" }

// StatementType implements the Statement interface.
func (*DropFunction) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (n *DropFunction) StatAbbr() string {
	if n.IsFunction {
		return "drop_function"
	}
	return "drop_procedure"
}

// StatObj implements the StatObj interface.
func (n *DropFunction) StatObj() string { return n.FuncName.String() }

// StatementTag returns a short string identifying the type of statement.
func (n *DropFunction) StatementTag() string {
	if n.IsFunction {
		return "DROP FUNCTION"
	}
	return "DROP PROCEDURE"
}

// StatementType implements the Statement interface.
func (*DropSequence) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*DropSequence) StatAbbr() string { return "drop_sequence" }

// StatObj implements the StatObj interface.
func (n *DropSequence) StatObj() string { return n.Names.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DropSequence) StatementTag() string { return "DROP SEQUENCE" }

// StatementType implements the Statement interface.
func (*DropUser) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*DropUser) StatAbbr() string { return "drop_user" }

// StatObj implements the StatObj interface.
func (n *DropUser) StatObj() string { return n.Names.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DropUser) StatementTag() string { return "DROP USER" }

// StatementType implements the Statement interface.
func (*DropRole) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*DropRole) StatAbbr() string { return "drop_role" }

// StatObj implements the StatObj interface.
func (n *DropRole) StatObj() string { return n.Names.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DropRole) StatementTag() string { return "DROP ROLE" }

// StatementType implements the Statement interface.
func (*DropSchema) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropSchema) StatementTag() string { return "DROP SCHEMA" }

// StatAbbr implements the StatAbbr interface.
func (*DropSchema) StatAbbr() string { return "drop_schema" }

// StatObj implements the StatObj interface.
func (n *DropSchema) StatObj() string { return n.Schemas.String() }

// StatementType implements the Statement interface.
func (*Execute) StatementType() StatementType { return Unknown }

// StatAbbr implements the StatAbbr interface.
func (*Execute) StatAbbr() string { return "execute" }

// StatObj implements the StatObj interface.
func (n *Execute) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*Execute) StatementTag() string { return "EXECUTE" }

// StatementType implements the Statement interface.
func (*Explain) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*Explain) StatAbbr() string { return "explain" }

// StatObj implements the StatObj interface.
func (*Explain) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Explain) StatementTag() string { return "EXPLAIN" }

// StatementType implements the Statement interface.
func (*Export) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*Export) StatAbbr() string { return "export" }

// StatObj implements the StatObj interface.
func (*Export) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Export) StatementTag() string { return "EXPORT" }

// StatementType implements the Statement interface.
func (*Grant) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*Grant) StatAbbr() string { return "grant" }

// StatObj implements the StatObj interface.
func (n *Grant) StatObj() string { return n.Targets.Databases.String() }

// StatementTag returns a short string identifying the type of statement.
func (*Grant) StatementTag() string { return "GRANT" }

// StatementType implements the Statement interface.
func (*GrantRole) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*GrantRole) StatAbbr() string { return "grant_role" }

// StatObj implements the StatObj interface.
func (n *GrantRole) StatObj() string {
	return util.StringConcat("-", n.Members.String(), n.Roles.String())
}

// StatementTag returns a short string identifying the type of statement.
func (*GrantRole) StatementTag() string { return "GRANT" }

// StatementType implements the Statement interface.
func (n *Insert) StatementType() StatementType { return n.Returning.statementType() }

// StatAbbr implements the StatAbbr interface.
func (n *Insert) StatAbbr() string { return "insert" }

// StatObj implements the StatObj interface.
func (n *Insert) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Insert) StatementTag() string { return "INSERT" }

// StatementType implements the Statement interface.
func (*ParenSelect) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ParenSelect) StatAbbr() string { return "paren_select" }

// StatObj implements the StatObj interface.
func (*ParenSelect) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ParenSelect) StatementTag() string { return "SELECT" }

// StatementType implements the Statement interface.
func (*Prepare) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*Prepare) StatAbbr() string { return "prepare" }

// StatObj implements the StatObj interface.
func (n *Prepare) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*Prepare) StatementTag() string { return "PREPARE" }

// StatementType implements the Statement interface.
func (*RefreshMaterializedView) StatementType() StatementType { return DDL }

// StatementTag implements the Statement interface.
func (*RefreshMaterializedView) StatementTag() string { return "REFRESH MATERIALIZED VIEW" }

// StatAbbr implements the StatAbbr interface.
func (*RefreshMaterializedView) StatAbbr() string { return "refresh_materialized_view" }

// StatObj implements the StatObj interface.
func (n *RefreshMaterializedView) StatObj() string { return n.Name.String() }

// StatementType implements the Statement interface.
func (*ReleaseSavepoint) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*ReleaseSavepoint) StatAbbr() string { return "release_savepoint" }

// StatObj implements the StatObj interface.
func (n *ReleaseSavepoint) StatObj() string { return n.Savepoint.String() }

// StatementTag returns a short string identifying the type of statement.
func (*ReleaseSavepoint) StatementTag() string { return "RELEASE" }

// StatementType implements the Statement interface.
func (*RenameColumn) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*RenameColumn) StatAbbr() string { return "rename_column" }

// StatObj implements the StatObj interface.
func (n *RenameColumn) StatObj() string {
	return util.StringConcat("-", n.Table.Table(), n.Name.String())
}

// StatementTag returns a short string identifying the type of statement.
func (*RenameColumn) StatementTag() string { return "RENAME COLUMN" }

// StatementType implements the Statement interface.
func (*RenameDatabase) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*RenameDatabase) StatAbbr() string { return "rename_database" }

// StatObj implements the StatObj interface.
func (n *RenameDatabase) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*RenameDatabase) StatementTag() string { return "RENAME DATABASE" }

// StatementType implements the Statement interface.
func (*RenameIndex) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*RenameIndex) StatAbbr() string { return "rename_index" }

// StatObj implements the StatObj interface.
func (n *RenameIndex) StatObj() string { return n.Index.Index.String() }

// StatementTag returns a short string identifying the type of statement.
func (*RenameIndex) StatementTag() string { return "RENAME INDEX" }

// StatementType implements the Statement interface.
func (*RenameTable) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*RenameTable) StatAbbr() string { return "rename_table" }

// StatObj implements the StatObj interface.
func (r *RenameTable) StatObj() string { return r.Name.Table() }

// StatementTag returns a short string identifying the type of statement.
func (r *RenameTable) StatementTag() string {
	if r.IsView {
		return "RENAME VIEW"
	} else if r.IsSequence {
		return "RENAME SEQUENCE"
	}
	return "RENAME TABLE"
}

// StatementType returns a short string identifying the type of statement.
func (*RenameTrigger) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameTrigger) StatementTag() string { return "RENAME TRIGGER" }

// StatAbbr implements the StatAbbr interface.
func (*RenameTrigger) StatAbbr() string { return "rename_trigger_clause" }

// StatObj implements the StatObj interface.
func (*RenameTrigger) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*Relocate) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*Relocate) StatAbbr() string { return "relocate" }

// StatObj implements the StatObj interface.
func (*Relocate) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (n *Relocate) StatementTag() string {
	if n.RelocateLease {
		return "EXPERIMENTAL_RELOCATE LEASE"
	}
	return "EXPERIMENTAL_RELOCATE"
}

// StatAbbr implements the StatAbbr interface.
func (*Dump) StatAbbr() string { return "dump" }

// StatObj implements the StatObj interface.
func (*Dump) StatObj() string { return "" }

// StatAbbr implements the StatAbbr interface.
func (*LoadImport) StatAbbr() string { return "load_import" }

// StatObj implements the StatObj interface.
func (*LoadImport) StatObj() string { return "" }

// StatAbbr implements the StatAbbr interface.
func (*LoadRestore) StatAbbr() string { return "load_restore" }

// StatObj implements the StatObj interface.
func (*LoadRestore) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*Revoke) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*Revoke) StatAbbr() string { return "revoke" }

// StatObj implements the StatObj interface.
func (n *Revoke) StatObj() string { return n.Targets.Databases.String() }

// StatementTag returns a short string identifying the type of statement.
func (*Revoke) StatementTag() string { return "REVOKE" }

// StatementType implements the Statement interface.
func (*RevokeRole) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*RevokeRole) StatAbbr() string { return "revoke_role" }

// StatObj implements the StatObj interface.
func (n *RevokeRole) StatObj() string { return n.Roles.String() }

// StatementTag returns a short string identifying the type of statement.
func (*RevokeRole) StatementTag() string { return "REVOKE" }

// StatementType implements the Statement interface.
func (*RollbackToSavepoint) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*RollbackToSavepoint) StatAbbr() string { return "rollback_to_savepoint" }

// StatObj implements the StatObj interface.
func (n *RollbackToSavepoint) StatObj() string { return n.Savepoint.String() }

// StatementTag returns a short string identifying the type of statement.
func (*RollbackToSavepoint) StatementTag() string { return "ROLLBACK" }

// StatementType implements the Statement interface.
func (*RollbackTransaction) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*RollbackTransaction) StatAbbr() string { return "rollback_transaction" }

// StatObj implements the StatObj interface.
func (*RollbackTransaction) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*RollbackTransaction) StatementTag() string { return "ROLLBACK" }

// StatementType implements the Statement interface.
func (*Savepoint) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*Savepoint) StatAbbr() string { return "savepoint" }

// StatObj implements the StatObj interface.
func (n *Savepoint) StatObj() string { return n.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*Savepoint) StatementTag() string { return "SAVEPOINT" }

// StatementType implements the Statement interface.
func (*Scatter) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*Scatter) StatAbbr() string { return "scatter" }

// StatObj implements the StatObj interface.
func (*Scatter) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Scatter) StatementTag() string { return "SCATTER" }

// StatementType implements the Statement interface.
func (*Scrub) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*Scrub) StatAbbr() string { return "scrub" }

// StatObj implements the StatObj interface.
func (*Scrub) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (n *Scrub) StatementTag() string { return "SCRUB" }

// StatementType implements the Statement interface.
func (*Select) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*Select) StatAbbr() string { return "select" }

// StatObj implements the StatObj interface.
func (*Select) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Select) StatementTag() string { return "SELECT" }

// StatementType implements the Statement interface.
func (*SelectClause) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*SelectClause) StatAbbr() string { return "select_clause" }

// StatObj implements the StatObj interface.
func (*SelectClause) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*SelectClause) StatementTag() string { return "SELECT" }

// StatementType implements the Statement interface.
func (*SetReplica) StatementType() StatementType { return DDL }

// StatAbbr implements the StatAbbr interface.
func (*SetReplica) StatAbbr() string { return "set_replica" }

// StatObj implements the StatObj interface.
func (n *SetReplica) StatObj() string { return n.Table.String() }

// StatementTag returns a short string identifying the type of statement.
func (n *SetReplica) StatementTag() string {
	if n.Disable {
		return "ALTER NOT-REPLICATION OK"
	}
	return "ALTER REPLICATION OK"
}

// StatementType implements the Statement interface.
func (*SetVar) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*SetVar) StatAbbr() string { return "set_var" }

// StatObj implements the StatObj interface.
func (n *SetVar) StatObj() string { return n.Name }

// StatementTag returns a short string identifying the type of statement.
func (*SetVar) StatementTag() string { return "SET" }

// StatementType implements the Statement interface.
func (*SetClusterSetting) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*SetClusterSetting) StatAbbr() string { return "set_cluster_setting" }

// StatObj implements the StatObj interface.
func (n *SetClusterSetting) StatObj() string { return n.Name }

// StatementTag returns a short string identifying the type of statement.
func (*SetClusterSetting) StatementTag() string { return "SET CLUSTER SETTING" }

// StatementType implements the Statement interface.
func (*SetTransaction) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*SetTransaction) StatAbbr() string { return "set_transaction" }

// StatObj implements the StatObj interface.
func (*SetTransaction) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*SetTransaction) StatementTag() string { return "SET TRANSACTION" }

// StatementType implements the Statement interface.
func (*SetTracing) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*SetTracing) StatAbbr() string { return "set_tracing" }

// StatObj implements the StatObj interface.
func (*SetTracing) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*SetTracing) StatementTag() string { return "SET TRACING" }

// observerStatement implements the ObserverStatement interface.
func (*SetTracing) observerStatement() {}

// StatementType implements the Statement interface.
func (*SetZoneConfig) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*SetZoneConfig) StatAbbr() string { return "set_zone_config" }

// StatObj implements the StatObj interface.
func (n *SetZoneConfig) StatObj() string {
	return util.StringConcat(n.Database.String(), n.NamedZone.String())
}

// StatementTag returns a short string identifying the type of statement.
func (*SetZoneConfig) StatementTag() string { return "CONFIGURE ZONE" }

// StatementType implements the Statement interface.
func (*SetSessionCharacteristics) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*SetSessionCharacteristics) StatAbbr() string { return "set_session_characteristics" }

// StatObj implements the StatObj interface.
func (*SetSessionCharacteristics) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*SetSessionCharacteristics) StatementTag() string { return "SET" }

// StatementType implements the Statement interface.
func (*ShowCursor) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowCursor) StatAbbr() string { return "show_cursor" }

// StatObj implements the StatObj interface.
func (n *ShowCursor) StatObj() string { return n.CursorName.String() }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCursor) StatementTag() string { return "SHOW" }

// StatementType implements the Statement interface.
func (*ShowVar) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowVar) StatAbbr() string { return "show_var" }

// StatObj implements the StatObj interface.
func (n *ShowVar) StatObj() string { return n.Name }

// StatementTag returns a short string identifying the type of statement.
func (*ShowVar) StatementTag() string { return "SHOW" }

// StatementType implements the Statement interface.
func (*ShowClusterSetting) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowClusterSetting) StatAbbr() string { return "show_cluster_setting" }

// StatObj implements the StatObj interface.
func (n *ShowClusterSetting) StatObj() string { return n.Name }

// StatementTag returns a short string identifying the type of statement.
func (*ShowClusterSetting) StatementTag() string { return "SHOW" }

// StatementType implements the Statement interface.
func (*ShowClusterSettingList) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowClusterSettingList) StatementTag() string { return "SHOW" }

// StatAbbr implements the StatAbbr interface.
func (*ShowClusterSettingList) StatAbbr() string { return "show_cluster_setting" }

// StatObj implements the StatObj interface.
func (n *ShowClusterSettingList) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*ShowColumns) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowColumns) StatAbbr() string { return "show_columns" }

// StatObj implements the StatObj interface.
func (n *ShowColumns) StatObj() string { return n.Table.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*ShowColumns) StatementTag() string { return "SHOW COLUMNS" }

// StatementType implements the Statement interface.
func (*ShowCreate) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowCreate) StatAbbr() string { return "show_create" }

// StatObj implements the StatObj interface.
func (n *ShowCreate) StatObj() string { return n.Name.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreate) StatementTag() string { return "SHOW CREATE" }

// StatementType returns a short string identifying the type of statement.
func (*ShowTriggers) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTriggers) StatementTag() string { return "SHOW Triggers" }

// StatAbbr implements the StatAbbr interface.
func (*ShowTriggers) StatAbbr() string { return "show_triggers_clause" }

// StatObj implements the StatObj interface.
func (*ShowTriggers) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*ShowBackup) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowBackup) StatAbbr() string { return "show_backup" }

// StatObj implements the StatObj interface.
func (*ShowBackup) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowBackup) StatementTag() string { return "SHOW BACKUP" }

// StatementType implements the Statement interface.
func (*ShowKafka) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowKafka) StatAbbr() string { return "show_kafka" }

// StatObj implements the StatObj interface.
func (*ShowKafka) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowKafka) StatementTag() string { return "SHOW KAFKA" }

// StatementType implements the Statement interface.
func (*ShowDatabases) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowDatabases) StatAbbr() string { return "show_databases" }

// StatObj implements the StatObj interface.
func (*ShowDatabases) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowDatabases) StatementTag() string { return "SHOW DATABASES" }

// StatementType implements the Statement interface.
func (*ShowTraceForSession) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowTraceForSession) StatAbbr() string { return "show_trace_for_session" }

// StatObj implements the StatObj interface.
func (*ShowTraceForSession) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTraceForSession) StatementTag() string { return "SHOW TRACE FOR SESSION" }

// StatementType implements the Statement interface.
func (*ShowGrants) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowGrants) StatAbbr() string { return "show_grants" }

// StatObj implements the StatObj interface.
func (n *ShowGrants) StatObj() string { return n.Targets.Roles.String() }

// StatementTag returns a short string identifying the type of statement.
func (*ShowGrants) StatementTag() string { return "SHOW GRANTS" }

// StatementType implements the Statement interface.
func (*ShowIndexes) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowIndexes) StatAbbr() string { return "show_Indexes" }

// StatObj implements the StatObj interface.
func (n *ShowIndexes) StatObj() string { return n.Table.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*ShowIndexes) StatementTag() string { return "SHOW INDEXES" }

// StatementType implements the Statement interface.
func (*ShowQueries) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowQueries) StatAbbr() string { return "show_queries" }

// StatObj implements the StatObj interface.
func (*ShowQueries) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowQueries) StatementTag() string { return "SHOW QUERIES" }

// StatementType implements the Statement interface.
func (*ShowJobs) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowJobs) StatAbbr() string { return "show_jobs" }

// StatObj implements the StatObj interface.
func (*ShowJobs) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowJobs) StatementTag() string { return "SHOW JOBS" }

// StatementType implements the Statement interface.
func (*ShowRoleGrants) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowRoleGrants) StatAbbr() string { return "show_role_grants" }

// StatObj implements the StatObj interface.
func (n *ShowRoleGrants) StatObj() string { return n.Roles.String() }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRoleGrants) StatementTag() string { return "SHOW GRANTS ON ROLE" }

// StatementType implements the Statement interface.
func (*ShowSessions) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowSessions) StatAbbr() string { return "show_sessions" }

// StatObj implements the StatObj interface.
func (*ShowSessions) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSessions) StatementTag() string { return "SHOW SESSIONS" }

// StatementType implements the Statement interface.
func (*ShowTableStats) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowTableStats) StatAbbr() string { return "show_table_stats" }

// StatObj implements the StatObj interface.
func (n *ShowTableStats) StatObj() string { return n.Table.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTableStats) StatementTag() string { return "SHOW STATISTICS" }

// StatementType implements the Statement interface.
func (*ShowHistogram) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowHistogram) StatAbbr() string { return "show_histogram" }

// StatObj implements the StatObj interface.
func (n *ShowHistogram) StatObj() string { return string(rune(n.HistogramID)) }

// StatementTag returns a short string identifying the type of statement.
func (*ShowHistogram) StatementTag() string { return "SHOW HISTOGRAM" }

// StatementType implements the Statement interface.
func (*ShowSchedules) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSchedules) StatementTag() string { return "SHOW SCHEDULES" }

// StatementType implements the Statement interface.
func (*ShowSyntax) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowSyntax) StatAbbr() string { return "show_syntax" }

// StatObj implements the StatObj interface.
func (*ShowSyntax) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSyntax) StatementTag() string { return "SHOW SYNTAX" }

func (*ShowSyntax) observerStatement() {}

// StatementType implements the Statement interface.
func (*ShowTransactionStatus) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowTransactionStatus) StatAbbr() string { return "show_transaction_status" }

// StatObj implements the StatObj interface.
func (*ShowTransactionStatus) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTransactionStatus) StatementTag() string { return "SHOW TRANSACTION STATUS" }

func (*ShowTransactionStatus) observerStatement() {}

// StatementType implements the Statement interface.
func (*ShowSavepointStatus) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSavepointStatus) StatementTag() string { return "SHOW SAVEPOINT STATUS" }

func (*ShowSavepointStatus) observerStatement() {}

// StatAbbr implements the StatAbbr interface.
func (*ShowSavepointStatus) StatAbbr() string { return "show_savepoint_status" }

// StatObj implements the StatObj interface.
func (*ShowSavepointStatus) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*ShowUsers) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowUsers) StatAbbr() string { return "show_users" }

// StatObj implements the StatObj interface.
func (*ShowUsers) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowUsers) StatementTag() string { return "SHOW USERS" }

// StatementType implements the Statement interface.
func (*ShowRoles) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowRoles) StatAbbr() string { return "show_roles" }

// StatObj implements the StatObj interface.
func (*ShowRoles) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRoles) StatementTag() string { return "SHOW ROLES" }

// StatementType implements the Statement interface.
func (*ShowSpaces) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSpaces) StatementTag() string { return "SHOW Spaces" }

// StatAbbr implements the StatAbbr interface.
func (*ShowSpaces) StatAbbr() string { return "show_spaces" }

// StatObj implements the StatObj interface.
func (*ShowSpaces) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*ShowZoneConfig) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowZoneConfig) StatAbbr() string { return "show_zone_config" }

// StatObj implements the StatObj interface.
func (*ShowZoneConfig) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowZoneConfig) StatementTag() string { return "SHOW ZONE CONFIGURATION" }

// StatementType implements the Statement interface.
func (*ShowRanges) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowRanges) StatAbbr() string { return "show_ranges" }

// StatObj implements the StatObj interface.
func (*ShowRanges) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRanges) StatementTag() string { return "SHOW EXPERIMENTAL_RANGES" }

// StatementType implements the Statement interface.
func (*ShowFingerprints) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowFingerprints) StatAbbr() string { return "show_fingerprints" }

// StatObj implements the StatObj interface.
func (*ShowFingerprints) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowFingerprints) StatementTag() string { return "SHOW EXPERIMENTAL_FINGERPRINTS" }

// StatementType implements the Statement interface.
func (*ShowConstraints) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowConstraints) StatAbbr() string { return "show_constraints" }

// StatObj implements the StatObj interface.
func (*ShowConstraints) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowConstraints) StatementTag() string { return "SHOW CONSTRAINTS" }

// StatementType returns a short string identifying the type of statement.
func (*ShowCreateTrigger) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateTrigger) StatementTag() string { return "SHOW CREATE TRIGGER" }

// StatAbbr implements the StatAbbr interface.
func (*ShowCreateTrigger) StatAbbr() string { return "show_create_trigger_clause" }

// StatObj implements the StatObj interface.
func (*ShowCreateTrigger) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*ShowTriggersOnTable) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTriggersOnTable) StatementTag() string { return "SHOW Triggers on table" }

// StatAbbr implements the StatAbbr interface.
func (*ShowTriggersOnTable) StatAbbr() string { return "show_triggers_on_table_clause" }

// StatObj implements the StatObj interface.
func (*ShowTriggersOnTable) StatObj() string { return "" }

// StatementType implements the Statement interface.
func (*ShowTables) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowTables) StatAbbr() string { return "show_tables" }

// StatObj implements the StatObj interface.
func (*ShowTables) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTables) StatementTag() string { return "SHOW TABLES" }

// StatementType implements the Statement interface.
func (*ShowFunctions) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowFunctions) StatAbbr() string { return "show_functions" }

// StatObj implements the StatObj interface.
func (*ShowFunctions) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowFunctions) StatementTag() string { return "SHOW FUNCTIONS" }

// StatementType implements the Statement interface.
func (*ShowSchemas) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowSchemas) StatAbbr() string { return "show_schemas" }

// StatObj implements the StatObj interface.
func (*ShowSchemas) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSchemas) StatementTag() string { return "SHOW SCHEMAS" }

// StatementType implements the Statement interface.
func (*ShowSequences) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowSequences) StatAbbr() string { return "show_sequences" }

// StatObj implements the StatObj interface.
func (*ShowSequences) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSequences) StatementTag() string { return "SHOW SCHEMAS" }

// StatementType implements the Statement interface.
func (*Split) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*Split) StatAbbr() string { return "split" }

// StatObj implements the StatObj interface.
func (*Split) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Split) StatementTag() string { return "SPLIT" }

// StatementType implements the Statement interface.
func (*Truncate) StatementType() StatementType { return Ack }

// StatAbbr implements the StatAbbr interface.
func (*Truncate) StatAbbr() string { return "truncate" }

// StatObj implements the StatObj interface.
func (*Truncate) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Truncate) StatementTag() string { return "TRUNCATE" }

// modifiesSchema implements the canModifySchema interface.
func (*Truncate) modifiesSchema() bool { return true }

// StatementType implements the Statement interface.
func (n *Update) StatementType() StatementType { return n.Returning.statementType() }

// StatAbbr implements the StatAbbr interface.
func (n *Update) StatAbbr() string { return "update" }

// StatObj implements the StatObj interface.
func (n *Update) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*Update) StatementTag() string { return "UPDATE" }

// StatementType implements the Statement interface.
func (*UnionClause) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*UnionClause) StatAbbr() string { return "union_clause" }

// StatObj implements the StatObj interface.
func (n *UnionClause) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*UnionClause) StatementTag() string { return "UNION" }

// StatementType implements the Statement interface.
func (*ValuesClause) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ValuesClause) StatAbbr() string { return "values_clause" }

// StatObj implements the StatObj interface.
func (*ValuesClause) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ValuesClause) StatementTag() string { return "VALUES" }

// StatementType implements the Statement interface.
func (q *QueryLockstat) StatementType() StatementType { return Rows }

// StatementTag implements the Statement interface.
func (q *QueryLockstat) StatementTag() string { return "QUERY LOCKSTAT" }

// StatAbbr implements the StatAbbr interface.
func (q *QueryLockstat) StatAbbr() string { return "query_lockstat" }

//StatObj implements the StatObj interface.
func (q *QueryLockstat) StatObj() string { return "" }

func (n *AlterIndex) String() string                { return AsString(n) }
func (n *AlterTable) String() string                { return AsString(n) }
func (n *AlterTableCmds) String() string            { return AsString(n) }
func (n *AlterTableAddColumn) String() string       { return AsString(n) }
func (n *AlterTableAddConstraint) String() string   { return AsString(n) }
func (n *AlterTableAlterColumnType) String() string { return AsString(n) }
func (n *AlterTableDropColumn) String() string      { return AsString(n) }
func (n *AlterTableDropConstraint) String() string  { return AsString(n) }
func (n *AlterTableDropNotNull) String() string     { return AsString(n) }
func (n *AlterTableDropStored) String() string      { return AsString(n) }
func (n *AlterTableSetDefault) String() string      { return AsString(n) }
func (n *AlterTableSetNotNull) String() string      { return AsString(n) }
func (n *AlterUser) String() string                 { return AsString(n) }
func (n *AlterUserDefaultSchma) String() string     { return AsString(n) }
func (n *AlterSequence) String() string             { return AsString(n) }
func (n *AlterSchema) String() string               { return AsString(n) }
func (n *AlterViewAS) String() string               { return AsString(n) }
func (n *Backup) String() string                    { return AsString(n) }
func (n *BeginCommit) String() string               { return AsString(n) }
func (n *BeginTransaction) String() string          { return AsString(n) }
func (c *ControlJobs) String() string               { return AsString(c) }
func (c *ControlSchedules) String() string          { return AsString(c) }
func (n *CancelQueries) String() string             { return AsString(n) }
func (n *CancelSessions) String() string            { return AsString(n) }
func (n *CannedOptPlan) String() string             { return AsString(n) }
func (n *CommentOnColumn) String() string           { return AsString(n) }
func (n *CommentOnDatabase) String() string         { return AsString(n) }
func (n *CommentOnTable) String() string            { return AsString(n) }
func (n *CommentOnIndex) String() string            { return AsString(n) }
func (n *CommentOnConstraint) String() string       { return AsString(n) }
func (n *CommitTransaction) String() string         { return AsString(n) }
func (n *CopyFrom) String() string                  { return AsString(n) }
func (c *CreateChangefeed) String() string          { return AsString(c) }
func (n *CreateDatabase) String() string            { return AsString(n) }
func (n *CreateIndex) String() string               { return AsString(n) }
func (n *CreateRole) String() string                { return AsString(n) }
func (c *CreateTable) String() string               { return AsString(c) }
func (n *CreateTrigger) String() string             { return AsString(n) }
func (n *CreateSequence) String() string            { return AsString(n) }
func (n *CreateStats) String() string               { return AsString(n) }
func (n *CreateUser) String() string                { return AsString(n) }
func (n *CreateView) String() string                { return AsString(n) }
func (n *CreateSchema) String() string              { return AsString(n) }
func (n *CursorAccess) String() string              { return AsString(n) }
func (n *VariableAccess) String() string            { return AsString(n) }
func (d *Deallocate) String() string                { return AsString(d) }
func (n *DeclareCursor) String() string             { return AsString(n) }
func (n *DeclareVariable) String() string           { return AsString(n) }
func (n *Delete) String() string                    { return AsString(n) }
func (n *DropDatabase) String() string              { return AsString(n) }
func (n *DropIndex) String() string                 { return AsString(n) }
func (n *DropRole) String() string                  { return AsString(n) }
func (n *DropTable) String() string                 { return AsString(n) }
func (n *DropTrigger) String() string               { return AsString(n) }
func (n *DropView) String() string                  { return AsString(n) }
func (n *DropSequence) String() string              { return AsString(n) }
func (n *DropFunction) String() string              { return AsString(n) }
func (n *DropUser) String() string                  { return AsString(n) }
func (n *DropSchema) String() string                { return AsString(n) }
func (n *Execute) String() string                   { return AsString(n) }
func (n *Explain) String() string                   { return AsString(n) }
func (n *Export) String() string                    { return AsString(n) }
func (n *Grant) String() string                     { return AsString(n) }
func (n *GrantRole) String() string                 { return AsString(n) }
func (n *Insert) String() string                    { return AsString(n) }
func (n *ParenSelect) String() string               { return AsString(n) }
func (n *Prepare) String() string                   { return AsString(n) }
func (q *QueryLockstat) String() string             { return AsString(q) }
func (n *ReleaseSavepoint) String() string          { return AsString(n) }
func (n *Relocate) String() string                  { return AsString(n) }
func (n *RefreshMaterializedView) String() string   { return AsString(n) }
func (n *RenameColumn) String() string              { return AsString(n) }
func (n *RenameDatabase) String() string            { return AsString(n) }
func (n *RenameTrigger) String() string             { return AsString(n) }
func (n *RenameIndex) String() string               { return AsString(n) }
func (r *RenameTable) String() string               { return AsString(r) }
func (n *Revoke) String() string                    { return AsString(n) }
func (n *RevokeRole) String() string                { return AsString(n) }
func (n *RollbackToSavepoint) String() string       { return AsString(n) }
func (n *RollbackTransaction) String() string       { return AsString(n) }
func (n *Savepoint) String() string                 { return AsString(n) }
func (n *Scatter) String() string                   { return AsString(n) }
func (n *ScheduledBackup) String() string           { return AsString(n) }
func (n *Scrub) String() string                     { return AsString(n) }
func (n *Select) String() string                    { return AsString(n) }
func (n *SelectClause) String() string              { return AsString(n) }
func (n *SetClusterSetting) String() string         { return AsString(n) }
func (n *SetZoneConfig) String() string             { return AsString(n) }
func (n *SetSessionCharacteristics) String() string { return AsString(n) }
func (n *SetTransaction) String() string            { return AsString(n) }
func (n *SetTracing) String() string                { return AsString(n) }
func (n *SetVar) String() string                    { return AsString(n) }
func (n *SetReplica) String() string                { return AsString(n) }
func (n *ShowBackup) String() string                { return AsString(n) }
func (n *ShowKafka) String() string                 { return AsString(n) }
func (n *ShowClusterSetting) String() string        { return AsString(n) }
func (n *ShowClusterSettingList) String() string    { return AsString(n) }
func (n *ShowColumns) String() string               { return AsString(n) }
func (n *ShowConstraints) String() string           { return AsString(n) }
func (n *ShowCreateTrigger) String() string         { return AsString(n) }
func (n *ShowCreate) String() string                { return AsString(n) }
func (n *ShowCursor) String() string                { return AsString(n) }
func (n *ShowDatabases) String() string             { return AsString(n) }
func (n *ShowGrants) String() string                { return AsString(n) }
func (n *ShowHistogram) String() string             { return AsString(n) }
func (n *ShowSchedules) String() string             { return AsString(n) }
func (n *ShowIndexes) String() string               { return AsString(n) }
func (n *ShowJobs) String() string                  { return AsString(n) }
func (n *ShowQueries) String() string               { return AsString(n) }
func (n *ShowRanges) String() string                { return AsString(n) }
func (n *ShowRoleGrants) String() string            { return AsString(n) }
func (n *ShowRoles) String() string                 { return AsString(n) }
func (n *ShowSavepointStatus) String() string       { return AsString(n) }
func (n *ShowSpaces) String() string                { return AsString(n) }
func (n *ShowSchemas) String() string               { return AsString(n) }
func (n *ShowSequences) String() string             { return AsString(n) }
func (n *ShowSessions) String() string              { return AsString(n) }
func (n *ShowSyntax) String() string                { return AsString(n) }
func (n *ShowTableStats) String() string            { return AsString(n) }
func (n *ShowTables) String() string                { return AsString(n) }
func (n *ShowFunctions) String() string             { return AsString(n) }
func (n *ShowTraceForSession) String() string       { return AsString(n) }
func (n *ShowTransactionStatus) String() string     { return AsString(n) }
func (n *ShowTriggers) String() string              { return AsString(n) }
func (n *ShowTriggersOnTable) String() string       { return AsString(n) }
func (n *ShowUsers) String() string                 { return AsString(n) }
func (n *ShowVar) String() string                   { return AsString(n) }
func (n *ShowZoneConfig) String() string            { return AsString(n) }
func (n *ShowFingerprints) String() string          { return AsString(n) }
func (n *Split) String() string                     { return AsString(n) }
func (n *Truncate) String() string                  { return AsString(n) }
func (n *UnionClause) String() string               { return AsString(n) }
func (n *Update) String() string                    { return AsString(n) }
func (n *ValuesClause) String() string              { return AsString(n) }
