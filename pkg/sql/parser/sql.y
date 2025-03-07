// Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California

// Portions of this file are additionally subject to the following
// license and copyright.
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

// Going to add a new statement?
// Consider taking a look at our codelab guide to learn what is needed to add a statement.
// https://github.com/znbasedb/znbase/blob/master/docs/codelabs/01-sql-statement.md

%{
package parser

import (
    "fmt"
    "strings"

    "go/constant"

    "github.com/znbasedb/znbase/pkg/sql/coltypes"
    "github.com/znbasedb/znbase/pkg/sql/lex"
    "github.com/znbasedb/znbase/pkg/security/privilege"
    "github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// MaxUint is the maximum value of an uint.
const MaxUint = ^uint(0)
// MaxInt is the maximum value of an int.
const MaxInt = int(MaxUint >> 1)

func unimplemented(sqllex sqlLexer, feature string) int {
    sqllex.(*lexer).Unimplemented(feature)
    return 1
}

func setErr(sqllex sqlLexer, err error) int {
    sqllex.(*lexer).setErr(err)
    return 1
}

func unimplementedWithIssue(sqllex sqlLexer, issue int) int {
    sqllex.(*lexer).UnimplementedWithIssue(issue)
    return 1
}

func unimplementedWithIssueDetail(sqllex sqlLexer, issue int, detail string) int {
    sqllex.(*lexer).UnimplementedWithIssueDetail(issue, detail)
    return 1
}
%}

%{
// sqlSymUnion represents a union of types, providing accessor methods
// to retrieve the underlying type stored in the union's empty interface.
// The purpose of the sqlSymUnion struct is to reduce the memory footprint of
// the sqlSymType because only one value (of a variety of types) is ever needed
// to be stored in the union field at a time.
//
// By using an empty interface, we lose the type checking previously provided
// by yacc and the Go compiler when dealing with union values. Instead, runtime
// type assertions must be relied upon in the methods below, and as such, the
// parser should be thoroughly tested whenever new syntax is added.
//
// It is important to note that when assigning values to sqlSymUnion.val, all
// nil values should be typed so that they are stored as nil instances in the
// empty interface, instead of setting the empty interface to nil. This means
// that:
//     $$ = []String(nil)
// should be used, instead of:
//     $$ = nil
// to assign a nil string slice to the union.
type sqlSymUnion struct {
    val        interface{}
    isUp       bool
    AsNameIsUp bool
}

// The following accessor methods come in three forms, depending on the
// type of the value being accessed and whether a nil value is admissible
// for the corresponding grammar rule.
// - Values and pointers are directly type asserted from the empty
//   interface, regardless of whether a nil value is admissible or
//   not. A panic occurs if the type assertion is incorrect; no panic occurs
//   if a nil is not expected but present. (TODO(knz): split this category of
//   accessor in two; with one checking for unexpected nils.)
//   Examples: bool(), tableIndexName().
//
// - Interfaces where a nil is admissible are handled differently
//   because a nil instance of an interface inserted into the empty interface
//   becomes a nil instance of the empty interface and therefore will fail a
//   direct type assertion. Instead, a guarded type assertion must be used,
//   which returns nil if the type assertion fails.
//   Examples: expr(), stmt().
//
// - Interfaces where a nil is not admissible are implemented as a direct
//   type assertion, which causes a panic to occur if an unexpected nil
//   is encountered.
//   Examples: tblDef().
//
func (u *sqlSymUnion) numVal() *tree.NumVal {
    return u.val.(*tree.NumVal)
}
func (u *sqlSymUnion) strVal() *tree.StrVal {
    if stmt, ok := u.val.(*tree.StrVal); ok {
        return stmt
    }
    return nil
}
func (u *sqlSymUnion) placeholder() *tree.Placeholder {
    return u.val.(*tree.Placeholder)
}
func (u *sqlSymUnion) auditMode() tree.AuditMode {
    return u.val.(tree.AuditMode)
}
func (u *sqlSymUnion) bool() bool {
    return u.val.(bool)
}
func (u *sqlSymUnion) strPtr() *string {
    return u.val.(*string)
}
func (u *sqlSymUnion) strs() []string {
    return u.val.([]string)
}
func (u *sqlSymUnion) newTableIndexName() *tree.TableIndexName {
    tn := u.val.(tree.TableIndexName)
    return &tn
}
func (u *sqlSymUnion) tableIndexName() tree.TableIndexName {
    return u.val.(tree.TableIndexName)
}
func (u *sqlSymUnion) newTableIndexNames() tree.TableIndexNames {
    return u.val.(tree.TableIndexNames)
}
func (u *sqlSymUnion) nameList() tree.NameList {
    return u.val.(tree.NameList)
}
func (u *sqlSymUnion) unresolvedName() *tree.UnresolvedName {
    return u.val.(*tree.UnresolvedName)
}
func (u *sqlSymUnion) unresolvedObjectName() *tree.UnresolvedObjectName {
    return u.val.(*tree.UnresolvedObjectName)
}
func (u *sqlSymUnion) functionReference() tree.FunctionReference {
    return u.val.(tree.FunctionReference)
}
func (u *sqlSymUnion) tablePatterns() tree.TablePatterns {
    return u.val.(tree.TablePatterns)
}
func (u *sqlSymUnion) tableNames() tree.TableNames {
    return u.val.(tree.TableNames)
}
func (u *sqlSymUnion) schemaNames() tree.SchemaList {
    return u.val.(tree.SchemaList)
}
func (u *sqlSymUnion) indexFlags() *tree.IndexFlags {
    return u.val.(*tree.IndexFlags)
}
func (u *sqlSymUnion) arraySubscript() *tree.ArraySubscript {
    return u.val.(*tree.ArraySubscript)
}
func (u *sqlSymUnion) arraySubscripts() tree.ArraySubscripts {
    if as, ok := u.val.(tree.ArraySubscripts); ok {
        return as
    }
    return nil
}
func (u *sqlSymUnion) stmt() tree.Statement {
    if stmt, ok := u.val.(tree.Statement); ok {
        return stmt
    }
    return nil
}
func (u *sqlSymUnion) cte() *tree.CTE {
    if cte, ok := u.val.(*tree.CTE); ok {
        return cte
    }
    return nil
}
func (u *sqlSymUnion) ctes() []*tree.CTE {
    return u.val.([]*tree.CTE)
}
func (u *sqlSymUnion) with() *tree.With {
    if with, ok := u.val.(*tree.With); ok {
        return with
    }
    return nil
}
func (u *sqlSymUnion) slct() *tree.Select {
    return u.val.(*tree.Select)
}
func (u *sqlSymUnion) ins() *tree.Insert {
    return u.val.(*tree.Insert)
}
func (u *sqlSymUnion) expendSlct() *tree.ExpendSelect {
    return u.val.(*tree.ExpendSelect)
}
func (u *sqlSymUnion) selectStmt() tree.SelectStatement {
    return u.val.(tree.SelectStatement)
}
func (u *sqlSymUnion) colDef() *tree.ColumnTableDef {
    return u.val.(*tree.ColumnTableDef)
}
func (u *sqlSymUnion) constraintDef() tree.ConstraintTableDef {
    return u.val.(tree.ConstraintTableDef)
}
func (u *sqlSymUnion) tblDef() tree.TableDef {
    return u.val.(tree.TableDef)
}
func (u *sqlSymUnion) tblDefs() tree.TableDefs {
    return u.val.(tree.TableDefs)
}
func (u *sqlSymUnion) colQual() tree.NamedColumnQualification {
    return u.val.(tree.NamedColumnQualification)
}
func (u *sqlSymUnion) colQualElem() tree.ColumnQualification {
    return u.val.(tree.ColumnQualification)
}
func (u *sqlSymUnion) colQuals() []tree.NamedColumnQualification {
    return u.val.([]tree.NamedColumnQualification)
}

func (u *sqlSymUnion) persistenceType() bool {
 return u.val.(bool)
}

func (u *sqlSymUnion) colType() coltypes.T {
    if colType, ok := u.val.(coltypes.T); ok {
        return colType
    }
    return nil
}
func (u *sqlSymUnion) tableRefCols() []tree.ColumnID {
    if refCols, ok := u.val.([]tree.ColumnID); ok {
        return refCols
    }
    return nil
}
func (u *sqlSymUnion) castTargetType() coltypes.CastTargetType {
    return u.val.(coltypes.CastTargetType)
}
func (u *sqlSymUnion) colTypes() []coltypes.T {
    return u.val.([]coltypes.T)
}
func (u *sqlSymUnion) int64() int64 {
    return u.val.(int64)
}
func (u *sqlSymUnion) seqOpt() tree.SequenceOption {
    return u.val.(tree.SequenceOption)
}
func (u *sqlSymUnion) seqOpts() []tree.SequenceOption {
    return u.val.([]tree.SequenceOption)
}
func (u *sqlSymUnion) expr() tree.Expr {
    if expr, ok := u.val.(tree.Expr); ok {
        return expr
    }
    return nil
}
func (u *sqlSymUnion) exprs() tree.Exprs {
    return u.val.(tree.Exprs)
}
func (u *sqlSymUnion) selExpr() tree.SelectExpr {
    return u.val.(tree.SelectExpr)
}
func (u *sqlSymUnion) selExprs() tree.SelectExprs {
    return u.val.(tree.SelectExprs)
}
func (u *sqlSymUnion) retClause() tree.ReturningClause {
        return u.val.(tree.ReturningClause)
}
func (u *sqlSymUnion) aliasClause() tree.AliasClause {
    return u.val.(tree.AliasClause)
}
func (u *sqlSymUnion) asOfClause() tree.AsOfClause {
    return u.val.(tree.AsOfClause)
}
func (u *sqlSymUnion) asSnapshotClause() tree.AsSnapshotClause {
    return u.val.(tree.AsSnapshotClause)
}
func (u *sqlSymUnion) tblExpr() tree.TableExpr {
    return u.val.(tree.TableExpr)
}
func (u *sqlSymUnion) tblExprs() tree.TableExprs {
    return u.val.(tree.TableExprs)
}
func (u *sqlSymUnion) ConversionFunc() tree.ConversionFunc {
    return u.val.(tree.ConversionFunc)
}
func (u *sqlSymUnion) ConversionFunctions() tree.ConversionFunctions {
    return u.val.(tree.ConversionFunctions)
}
func (u *sqlSymUnion) from() *tree.From {
    return u.val.(*tree.From)
}
func (u *sqlSymUnion) int32s() []int32 {
    return u.val.([]int32)
}
func (u *sqlSymUnion) joinCond() tree.JoinCond {
    return u.val.(tree.JoinCond)
}
func (u *sqlSymUnion) when() *tree.When {
    return u.val.(*tree.When)
}
func (u *sqlSymUnion) whens() []*tree.When {
    return u.val.([]*tree.When)
}
func (u *sqlSymUnion) lockingClause() tree.LockingClause {
    return u.val.(tree.LockingClause)
}
func (u *sqlSymUnion) lockingItem() *tree.LockingItem {
    return u.val.(*tree.LockingItem)
}
func (u *sqlSymUnion) lockingStrength() tree.LockingStrength {
    return u.val.(tree.LockingStrength)
}
func (u *sqlSymUnion) lockingWaitPolicy() tree.LockingWaitPolicy {
    return u.val.(tree.LockingWaitPolicy)
}
func (u *sqlSymUnion) updateExpr() *tree.UpdateExpr {
    return u.val.(*tree.UpdateExpr)
}
func (u *sqlSymUnion) updateExprs() tree.UpdateExprs {
    return u.val.(tree.UpdateExprs)
}
func (u *sqlSymUnion) limit() *tree.Limit {
    return u.val.(*tree.Limit)
}
func (u *sqlSymUnion) targetList() tree.TargetList {
    return u.val.(tree.TargetList)
}
func (u *sqlSymUnion) targetListPtr() *tree.TargetList {
    return u.val.(*tree.TargetList)
}
func (u *sqlSymUnion) privilegeType() privilege.Kind {
    return u.val.(privilege.Kind)
}
func (u *sqlSymUnion) privilegeGroup() tree.PrivilegeGroup {
    return u.val.(tree.PrivilegeGroup)
}
func (u *sqlSymUnion) privilegeGroups() tree.PrivilegeGroups {
    return u.val.(tree.PrivilegeGroups)
}
func (u *sqlSymUnion) onConflict() *tree.OnConflict {
    return u.val.(*tree.OnConflict)
}
func (u *sqlSymUnion) orderBy() tree.OrderBy {
    return u.val.(tree.OrderBy)
}
func (u *sqlSymUnion) order() *tree.Order {
    return u.val.(*tree.Order)
}
func (u *sqlSymUnion) orders() []*tree.Order {
    return u.val.([]*tree.Order)
}
func (u *sqlSymUnion) hint() tree.Hint {
    if res, ok := u.val.(*tree.TableHint);ok {
        return res
    }
    return u.val.(*tree.IndexHint)
}
func (u *sqlSymUnion) hintset() tree.HintSet {
    return u.val.(tree.HintSet)
}
func (u *sqlSymUnion) readFromStorage() *tree.ReadFromStorage {
    return u.val.(*tree.ReadFromStorage)
}
func (u *sqlSymUnion) readFromStorageList() tree.ReadFromStorageList {
    return u.val.(tree.ReadFromStorageList)
}
func (u *sqlSymUnion) groupBy() tree.GroupBy {
    return u.val.(tree.GroupBy)
}
func (u *sqlSymUnion) windowFrame() *tree.WindowFrame {
    return u.val.(*tree.WindowFrame)
}
func (u *sqlSymUnion) windowFrameBounds() tree.WindowFrameBounds {
    return u.val.(tree.WindowFrameBounds)
}
func (u *sqlSymUnion) windowFrameBound() *tree.WindowFrameBound {
    return u.val.(*tree.WindowFrameBound)
}
func (u *sqlSymUnion) distinctOn() tree.DistinctOn {
    return u.val.(tree.DistinctOn)
}
func (u *sqlSymUnion) dir() tree.Direction {
    return u.val.(tree.Direction)
}
func (u *sqlSymUnion) alterTableCmd() tree.AlterTableCmd {
    return u.val.(tree.AlterTableCmd)
}
func (u *sqlSymUnion) alterTableCmds() tree.AlterTableCmds {
    return u.val.(tree.AlterTableCmds)
}
func (u *sqlSymUnion) alterIndexCmd() tree.AlterIndexCmd {
    return u.val.(tree.AlterIndexCmd)
}
func (u *sqlSymUnion) alterIndexCmds() tree.AlterIndexCmds {
    return u.val.(tree.AlterIndexCmds)
}
func (u *sqlSymUnion) isoLevel() tree.IsolationLevel {
    return u.val.(tree.IsolationLevel)
}
func (u *sqlSymUnion) userPriority() tree.UserPriority {
    return u.val.(tree.UserPriority)
}
func (u *sqlSymUnion) readWriteMode() tree.ReadWriteMode {
    return u.val.(tree.ReadWriteMode)
}
func (u *sqlSymUnion) support_ddl() tree.SUPDDL {
    return u.val.(tree.SUPDDL)
}
func (u *sqlSymUnion) idxElem() tree.IndexElem {
    return u.val.(tree.IndexElem)
}
func (u *sqlSymUnion) idxElems() tree.IndexElemList {
    return u.val.(tree.IndexElemList)
}
func (u *sqlSymUnion) dropBehavior() tree.DropBehavior {
    return u.val.(tree.DropBehavior)
}
func (u *sqlSymUnion) validationBehavior() tree.ValidationBehavior {
    return u.val.(tree.ValidationBehavior)
}
func (u *sqlSymUnion) interleave() *tree.InterleaveDef {
    return u.val.(*tree.InterleaveDef)
}
func (u *sqlSymUnion) partitionBy() *tree.PartitionBy {
    return u.val.(*tree.PartitionBy)
}
func (u *sqlSymUnion) listPartition() tree.ListPartition {
    return u.val.(tree.ListPartition)
}
func (u *sqlSymUnion) listPartitions() []tree.ListPartition {
    return u.val.([]tree.ListPartition)
}
func (u *sqlSymUnion) hashPartition() tree.ListPartition {
    return u.val.(tree.ListPartition)
}
func (u *sqlSymUnion) hashPartitions() []tree.ListPartition {
    return u.val.([]tree.ListPartition)
}
func (u *sqlSymUnion) rangePartition() tree.RangePartition {
    return u.val.(tree.RangePartition)
}
func (u *sqlSymUnion) rangePartitions() []tree.RangePartition {
    return u.val.([]tree.RangePartition)
}
func (u *sqlSymUnion) setZoneConfig() *tree.SetZoneConfig {
    return u.val.(*tree.SetZoneConfig)
}
func (u *sqlSymUnion) tuples() []*tree.Tuple {
    return u.val.([]*tree.Tuple)
}
func (u *sqlSymUnion) tuple() *tree.Tuple {
    return u.val.(*tree.Tuple)
}
func (u *sqlSymUnion) windowDef() *tree.WindowDef {
    return u.val.(*tree.WindowDef)
}
func (u *sqlSymUnion) window() tree.Window {
    return u.val.(tree.Window)
}
func (u *sqlSymUnion) op() tree.Operator {
    return u.val.(tree.Operator)
}
func (u *sqlSymUnion) cmpOp() tree.ComparisonOperator {
    return u.val.(tree.ComparisonOperator)
}
func (u *sqlSymUnion) durationField() tree.DurationField {
    return u.val.(tree.DurationField)
}
func (u *sqlSymUnion) kvOption() tree.KVOption {
    return u.val.(tree.KVOption)
}
func (u *sqlSymUnion) kvOptions() []tree.KVOption {
    if colType, ok := u.val.([]tree.KVOption); ok {
        return colType
    }
    return nil
}
func (u *sqlSymUnion) transactionModes() tree.TransactionModes {
    return u.val.(tree.TransactionModes)
}
func (u *sqlSymUnion) compositeKeyMatchMethod() tree.CompositeKeyMatchMethod {
  return u.val.(tree.CompositeKeyMatchMethod)
}
func (u *sqlSymUnion) referenceAction() tree.ReferenceAction {
    return u.val.(tree.ReferenceAction)
}
func (u *sqlSymUnion) referenceActions() tree.ReferenceActions {
    return u.val.(tree.ReferenceActions)
}
func (u *sqlSymUnion) createStatsOptions() *tree.CreateStatsOptions {
    return u.val.(*tree.CreateStatsOptions)
}
func (u *sqlSymUnion) scrubOptions() tree.ScrubOptions {
    return u.val.(tree.ScrubOptions)
}
func (u *sqlSymUnion) scrubOption() tree.ScrubOption {
    return u.val.(tree.ScrubOption)
}
func (u *sqlSymUnion) resolvableFuncRefFromName() tree.ResolvableFunctionReference {
    return tree.ResolvableFunctionReference{FunctionReference: u.unresolvedName()}
}
func (u *sqlSymUnion) rowsFromExpr() *tree.RowsFromExpr {
    return u.val.(*tree.RowsFromExpr)
}
func (u *sqlSymUnion) transactionName() tree.Name {
    return u.val.(tree.Name)
}
func (u *sqlSymUnion) partitionedBackup() tree.PartitionedBackup {
    return u.val.(tree.PartitionedBackup)
}
func (u *sqlSymUnion) partitionedBackups() []tree.PartitionedBackup {
    return u.val.([]tree.PartitionedBackup)
}
func (u *sqlSymUnion) fullBackupClause() *tree.FullBackupClause {
    return u.val.(*tree.FullBackupClause)
}
func newNameFromStr(s string) *tree.Name {
    return (*tree.Name)(&s)
}
func (u *sqlSymUnion) condAndOp() *tree.CondAndOp {
    return u.val.(*tree.CondAndOp)
}
func (u *sqlSymUnion) condAndOpList() tree.CondAndOpList {
    return u.val.(tree.CondAndOpList)
}
func (u *sqlSymUnion) nmCondAndOpList() tree.NmCondAndOpList {
    return u.val.(tree.NmCondAndOpList )
}
func (u *sqlSymUnion) nmCondAndOp() *tree.NmCondAndOp {
    return u.val.(*tree.NmCondAndOp)
}
func (u *sqlSymUnion) valuesClause() *tree.ValuesClause {
    return u.val.(*tree.ValuesClause)
}
func (u *sqlSymUnion) signal() *tree.Signal {
    return u.val.(*tree.Signal)
}
func (u *sqlSymUnion) location() *tree.Location {
    return u.val.(*tree.Location)
}
func (u *sqlSymUnion) locationList() tree.LocationList {
    return u.val.(tree.LocationList)
}
func (u *sqlSymUnion) funcExpr() *tree.FuncExpr {
	return u.val.(*tree.FuncExpr)
}
func (u *sqlSymUnion) functionParameter() *tree.FunctionParameter {
	return u.val.(*tree.FunctionParameter)
}
func (u *sqlSymUnion) functionParameters() tree.FunctionParameters {
	return u.val.(tree.FunctionParameters)
}
func (u *sqlSymUnion) defElem() *tree.DefElem {
	return u.val.(*tree.DefElem)
}
func (u *sqlSymUnion) defElems() tree.DefElems {
	return u.val.(tree.DefElems)
}
func (u *sqlSymUnion) funcParam() tree.FuncParam {
    if res, ok := u.val.(tree.FuncParam); ok {
        return res
    }
    return tree.FuncParamIN
}
func (u *sqlSymUnion) direction() tree.CursorDirection {
    return u.val.(tree.CursorDirection)
}
func (u *sqlSymUnion) pLFileLanguage() tree.PLFileLanguage {
    return u.val.(tree.PLFileLanguage)
}
func (u *sqlSymUnion) cursorAccess() *tree.CursorAccess {
    return u.val.(*tree.CursorAccess)
}
func (u *sqlSymUnion) variableAccess() *tree.VariableAccess {
    return u.val.(*tree.VariableAccess)
}
func (u *sqlSymUnion) cursorVar() *tree.CursorVar {
    return u.val.(*tree.CursorVar)
}
func (u *sqlSymUnion) cursorVars() tree.CursorVars {
    return u.val.(tree.CursorVars)
}
func (u *sqlSymUnion) likeTableOption() tree.LikeTableOption {
    return u.val.(tree.LikeTableOption)
}
func (u *sqlSymUnion) likeTableOptionList() []tree.LikeTableOption {
    return u.val.([]tree.LikeTableOption)
}
%}

// NB: the %token definitions must come before the %type definitions in this
// file to work around a bug in goyacc. See #16369 for more details.

// Non-keyword token types.
%token <str> IDENT SCONST BCONST BITCONST
%token <*tree.NumVal> ICONST FCONST
%token <*tree.Placeholder> PLACEHOLDER
%token <str> TYPECAST TYPEANNOTATE DOT_DOT
%token <str> LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%token <str> NOT_REGMATCH REGIMATCH NOT_REGIMATCH NOT_LIKE_OP NOT_ILIKE_OP
%token <str> ERROR

// If you want to make any keyword changes, add the new keyword here as well as
// to the appropriate one of the reserved-or-not-so-reserved keyword lists,
// below; search this file for "Keyword category lists".

// Ordinary key words in alphabetical order.
%token <str> ABORT ABSOLUTE ACTION ADD ADMIN AFTER AGGREGATE STORAGE IDENTITY
%token <str> ALL ALTER ALWAYS ANALYSE ANALYZE AND ANY ANNOTATE_TYPE ARRAY AS ASC
%token <str> ASYMMETRIC AT AUTOMATIC INCLUDING

%token <str> BACKWARD BCRYPT BEFORE BEGIN BETWEEN BIGINT BIGSERIAL BIT
%token <str> BLOB BOOL BOOLEAN BOTH BY BYTEA BYTES CLOB

%token <str> CACHE CANCEL CASCADE CASE CAST CHANGEFEED CHAR CALL

%token <str> CHARACTER CHARACTERISTICS CHECK CLEAR GENERATED EXCLUDING DEFAULTS COMMENTS

%token <str> CLOSE NONE CLUSTER COALESCE COLLATE COLLATION COLUMN COLUMNS COMMENT COMMIT

%token <str> COMMITTED COMPACT CONCAT CONFIGURATION CONFIGURATIONS CONFIGURE CONNECT_BY_ROOT

%token <str> CONFLICT CONSTRAINT CONSTRAINTS CONTAINS CONNECTION CONVERSION COPY COVERING CREATE CREATEUSER

%token <str> CROSS CSTATUS CUBE CURSOR CURSORS CURRENT CURRENT_CATALOG CURRENT_DATE CURRENT_SCHEMA LOCAL_TIME LOCAL_TIMESTAMP
%token <str> CURRENT_ROLE CURRENT_TIME CURRENT_TIMESTAMP
%token <str> CURRENT_USER CYCLE CONNECT

%token <str> DATA DATABASE DATABASES DATE DAY DBTIMEZONE  DDL DEC DECIMAL DECLARE DEFAULT
%token <str> DEALLOCATE DEFERRABLE DEFERRED DELETE DESC DETACHED
%token <str> DISABLE DISCARD DISTINCT DO DOMAIN DOUBLE DROP DUMP DUPLICATE

%token <str> EACH ELSE ENABLE ENCODING ENCRYPTION ENCRYPTION_PASSPHRASE END ENUM ESCAPE EXCEPT
%token <str> EXISTS EXECUTE EXECUTION EXPERIMENTAL
%token <str> EXPERIMENTAL_FINGERPRINTS EXPERIMENTAL_REPLICA EQNU
%token <str> EXPERIMENTAL_AUDIT
%token <str> EXPLAIN EXTENSION EXTRACT EXTRACT_DURATION

%token <str> FALSE FAMILY FETCH FETCHVAL FETCHTEXT FETCHVAL_PATH FETCHTEXT_PATH
%token <str> FILES FILTER
%token <str> FIRST FLASHBACK FLOAT FLOAT4 FLOAT8 FLOORDIV FOLLOWING FOR FORCE_INDEX FOREIGN FORWARD FROM FULL FUNCTION FUNCTIONS

%token <str> GLOBAL GRANT GRANTS GREATEST GROUP GROUPING GROUPS GROUPID

%token <str> HAVING HASH HIGH HISTOGRAM HOUR

%token <str> IF IFERROR IFNULL IGNORE ILIKE ILIKE_OP IMMEDIATE IN INCREMENT INCREMENTAL INOUT INSTEAD
%token <str> INET INET_CONTAINED_BY_OR_EQUALS INET_CONTAINS_OR_CONTAINED_BY
%token <str> INET_CONTAINS_OR_EQUALS INDEX INDEXES INHERIT INHERITS INJECT INTERLEAVE INITIALLY
%token <str> INNER INSERT INT INT2VECTOR INT2 INT4 INT8 INT64 INTEGER INTENT
%token <str> INTERSECT INTERVAL INTO INVERTED IS ISERROR ISNULL ISOLATION

%token <str> JOB JOBS JOIN JSON JSONB JSON_SOME_EXISTS JSON_ALL_EXISTS

%token <str> KEY KEYS KV

%token <str> LANGUAGE LAST LATERAL LC_CTYPE LC_COLLATE

%token <str> LEADING LEASE LEAST LEFT LESS LEVEL LIKE LIKE_OP LIMIT LIST LOAD LOAD_KAFKA LOCAL LOCKSTAT

%token <str> LOCALTIME LOCALTIMESTAMP LOCKED LOCATE LOCATION LOOKUP LOW LSHIFT

%token <str> MANUALCAST MATCH MATCHED MATERIALIZED MEM MEMORY MERGE MESSAGE_TEXT MINVALUE MAXVALUE MINUTE MONTH MOVE

%token <str> USE_INDEX IGNORE_INDEX NTH_PLAN READ_FROM_STORAGE  MERGE_JOIN COLENGINEREAD ROWENGINEREAD
%token <str> HASH_JOIN LOOKUP_JOIN QB_NAME

%token <str> NAN NAME NAMES NATURAL NEVER NEXT NO NO_INDEX_JOIN NORMAL NULLS NVL

%token <str> NOCREATEUSER NOCSTATUS
%token <str> NOCYCLE NOT NOTHING NOTNULL NOWAIT NULL NULLIF NUMERIC NOTSUPDDL

%token <str> OBJECT_ID OF OFF OFFSET OID OIDS OIDVECTOR ON ONLY OPT OPTION OPTIONS OR
%token <str> ORDER ORDINALITY OUT OUTER OVER OVERLAPS OVERLAY OWNED OPERATOR OPEN

%token <str> PARENT PARTIAL PARTITION PASSWORD PAUSE PHYSICAL PLACING PARTITIONS PBKDF2

%token <str> PLAN PLANS POSITION PRECEDING PRECISION PREPARE PRIMARY PRIOR PRIORITY
%token <str> PROCEDURAL PUBLICATION PROCEDURE


%token <str> QUERIES QUERY

%token <str> RANGE RANGES READ REAL RECURSIVE RECURRING REF REFCURSOR REFERENCES REFRESH
%token <str> REGCLASS REGPROC REGPROCEDURE REGNAMESPACE REGTYPE RELATIVE
%token <str> REMOVE_PATH RENAME REPEATABLE REPLACE REPLICATION RESPECT RETURNS
%token <str> RELEASE RESET RESTRICT RESUME RETURNING RETRY REVISION_HISTORY REVERT REVOKE RIGHT
%token <str> ROLE ROLES ROLLBACK ROLLUP ROW ROWNUM ROWS RSHIFT RULE

%token <str> SAVEPOINT SCATTER SCHEDULE SCHEDULES SCHEMA SCHEMAS SCRUB SEARCH SEARCH_PATH SECOND SELECT SEQUENCE SEQUENCES SESSIONTIMEZONE
%token <str> SERIAL SERIAL2 SERIAL4 SERIAL8
%token <str> SERIALIZABLE SERVER SESSION SESSIONS SESSION_USER SET SETTING SETTINGS
%token <str> SHARE SHOW SIMILAR SIMPLE SKIP SIGNAL SM3 SMALLINT SMALLSERIAL SNAPSHOT SOME SPACES SPLIT SQL SQLSTATE
%token <str> START STATEMENT STATISTICS STATUS STDIN STRICT STRING STORE STORED STORING SUBSTRING
%token <str> SYMMETRIC SYNTAX SYSTEM SUBSCRIPTION SUPDDL

%token <str> TABLE TABLES TEMP TEMPLATE TEMPORARY TESTING_RANGES EXPERIMENTAL_RANGES TESTING_RELOCATE EXPERIMENTAL_RELOCATE TEXT THEN
%token <str> TIME TIMETZ TIMESTAMP TIMESTAMPTZ TO THROTTLING TRAILING TRACE TRANSACTION TREAT TRIGGER TRIGGERS TRIM TRUE
%token <str> TRUNCATE TRUSTED TTLDAYS TYPE
%token <str> TRACING TZ_OFFSET

%token <str> UNBOUNDED UNCOMMITTED UNION UNIQUE UNKNOWN UNLOGGED
%token <str> UPDATE UPSERT UNTIL USE USER USERS USING UUID

%token <str> VALID VALIDATE VALUE VALUES VARBIT VARCHAR VARIABLE VARIADIC VIEW VARYING VIRTUAL VOID

%token <str> WAIT WHEN WHERE WINDOW WITH WITHIN WITHOUT WORK WRITE

%token <str> YEAR

%token <str> ZONE

%token <str> EXTERN PATH
%token <str> XOR
// The grammar thinks these are keywords, but they are not in any category
// and so can never be entered directly. The filter in scan.go creates these
// tokens when required (based on looking one token ahead).
//
// NOT_LA exists so that productions such as NOT LIKE can be given the same
// precedence as LIKE; otherwise they'd effectively have the same precedence as
// NOT, at least with respect to their left-hand subexpression. WITH_LA is
// needed to make the grammar LALR(1).
%token NOT_LA WITH_LA AS_LA

%union {
  id    int32
  pos   int32
  str   string
  union sqlSymUnion
}

%type <tree.Statement> stmt_block
%type <tree.Statement> stmt

%type <tree.Statement> alter_stmt
%type <tree.Statement> alter_ddl_stmt
%type <tree.Statement> alter_table_stmt
%type <tree.Statement> alter_schema_stmt
%type <tree.Statement> alter_index_stmt
%type <tree.Statement> alter_view_stmt
%type <tree.Statement> alter_sequence_stmt
%type <tree.Statement> alter_database_stmt
%type <tree.Statement> alter_user_stmt
%type <tree.Statement> alter_range_stmt
%type <tree.Statement> alter_function_stmt
%type <tree.Statement> alter_trigger_stmt
// ALTER RANGE
%type <tree.Statement> alter_zone_range_stmt

// ALTER TABLE
%type <tree.Statement> alter_onetable_stmt
%type <tree.Statement> alter_split_stmt
%type <tree.Statement> alter_rename_table_stmt
%type <tree.Statement> alter_scatter_stmt
%type <tree.Statement> alter_relocate_stmt
%type <tree.Statement> alter_relocate_lease_stmt
%type <tree.Statement> alter_zone_table_stmt
%type <tree.Statement> alter_partition_table_stmt
%type <tree.Statement> alter_partition_index_stmt
%type <tree.Statement> alter_replication_table_stmt

//ALTER SCHEMA
%type <tree.Statement> alter_rename_schema_stmt

// ALTER DATABASE
%type <tree.Statement> alter_rename_database_stmt
%type <tree.Statement> alter_zone_database_stmt

// ALTER USER
%type <tree.Statement> alter_user_options_stmt
%type <tree.Statement> alter_user_searchpath_stmt

// ALTER INDEX
%type <tree.Statement> alter_oneindex_stmt
%type <tree.Statement> alter_scatter_index_stmt
%type <tree.Statement> alter_split_index_stmt
%type <tree.Statement> alter_rename_index_stmt
%type <tree.Statement> alter_relocate_index_stmt
%type <tree.Statement> alter_relocate_index_lease_stmt
%type <tree.Statement> alter_zone_index_stmt

// ALTER VIEW
%type <tree.Statement> alter_rename_view_stmt

// ALTER SEQUENCE
%type <tree.Statement> alter_rename_sequence_stmt
%type <tree.Statement> alter_sequence_options_stmt

%type <tree.Statement> begin_stmt

%type <tree.Statement> call_stmt
%type <tree.Statement> cancel_stmt
%type <tree.Statement> cancel_jobs_stmt
%type <tree.Statement> cancel_queries_stmt
%type <tree.Statement> cancel_sessions_stmt

// SCRUB
%type <tree.Statement> scrub_stmt
%type <tree.Statement> scrub_database_stmt
%type <tree.Statement> scrub_table_stmt
%type <tree.ScrubOptions> opt_scrub_options_clause
%type <tree.ScrubOptions> scrub_option_list
%type <tree.ScrubOption> scrub_option

%type <tree.Statement> comment_stmt
%type <tree.Statement> commit_stmt
%type <tree.Statement> copy_from_stmt

%type <tree.Statement> create_stmt
%type <tree.Statement> create_changefeed_stmt
%type <tree.Statement> create_ddl_stmt
%type <tree.Statement> create_database_stmt
%type <tree.Statement> create_index_stmt
%type <tree.Statement> create_role_stmt
%type <tree.Statement> create_schedule_for_dump_stmt
%type <tree.Statement> create_table_stmt
%type <tree.Statement> create_schema_stmt
%type <tree.Statement> create_table_as_stmt
%type <tree.Statement> create_user_stmt
%type <tree.Statement> create_view_stmt
%type <tree.Statement> create_sequence_stmt
%type <tree.Statement> create_snapshot_stmt
%type <tree.Statement> create_trigger_stmt
// add by cms - for test plsql
%type <tree.Statement> create_function_stmt
%type <tree.Statement> drop_function_stmt
%type <tree.FunctionParameters> func_args func_args_list func_args_with_defaults
%type <*tree.FunctionParameter> func_arg func_arg_with_default
%type <tree.FunctionParameters> func_args_with_defaults_list
%type <tree.Statement> func_arg
%type <tree.Statement> arg_class
%type <tree.Statement> func_return
%type <tree.Statement> func_type
%type <tree.DefElems> createfunc_opt_list
%type <*tree.DefElem> createfunc_opt_item
%type <tree.PLFileLanguage> pl_file_language


%type <tree.Statement> create_stats_stmt
%type <*tree.CreateStatsOptions> opt_create_stats_options
%type <*tree.CreateStatsOptions> create_stats_option_list
%type <*tree.CreateStatsOptions> create_stats_option

%type <tree.Statement> create_type_stmt
// cursor stmt
%type <tree.Statement> cursor_stmt
%type <tree.Statement> declare_cursor_stmt
%type <tree.Statement> oc_cursor_stmt
%type <tree.Statement> fetch_move_cursor_stmt
%type <tree.Statement> show_cursors_stmt
%type <tree.Statement> fetch_args
%type <tree.Statement> opt_cursor_into
%type <empty> from_or_in for_or_is

//where_current_of
%type <tree.Expr> where_current_of

//cursor param
%type <tree.Statement> opt_cursor_param_def_clause
%type <tree.CursorVars> cursor_param_def_list
%type <*tree.CursorVar> cursor_param_def
%type <tree.Statement> opt_cursor_param_assign_clause
%type <tree.CursorVars> cursor_param_assign_list
%type <*tree.CursorVar> cursor_param_assign

// variable stmt
%type <tree.Statement> variable_stmt
%type <tree.Statement> declare_variable_stmt
%type <tree.Statement> fetch_variable_stmt
%type <tree.Statement> variable_name_list

%type <tree.Statement> delete_stmt
%type <tree.Statement> discard_stmt

%type <tree.Statement> drop_stmt
%type <tree.Statement> drop_ddl_stmt
%type <tree.Statement> drop_trigger_stmt
%type <tree.Statement> drop_database_stmt
%type <tree.Statement> drop_index_stmt
%type <tree.Statement> drop_snapshot_stmt
%type <tree.Statement> drop_role_stmt
%type <tree.Statement> drop_table_stmt
%type <tree.Statement> drop_schema_stmt
%type <tree.Statement> drop_user_stmt
%type <tree.Statement> drop_view_stmt
%type <tree.Statement> drop_sequence_stmt
%type <tree.Statement> dump_stmt

%type <tree.Statement> explain_stmt
%type <tree.Statement> prepare_stmt
%type <tree.Statement> preparable_stmt
%type <tree.Statement> execute_stmt
%type <tree.Statement> deallocate_stmt
%type <tree.Statement> grant_stmt
%type <tree.Statement> insert_stmt
%type <tree.Statement> load_stmt
%type <tree.Statement> pause_stmt
%type <tree.Statement> release_stmt
%type <tree.Statement> reset_stmt reset_session_stmt reset_csetting_stmt
%type <tree.Statement> resume_stmt
%type <tree.Statement> drop_schedule_stmt
%type <tree.Statement> revert_stmt
%type <tree.Statement> flashback_stmt
%type <tree.Statement> clear_stmt
%type <tree.Statement> revoke_stmt
%type <tree.Statement> refresh_stmt
%type <*tree.Select>   select_stmt select_stmt_no_as
%type <*tree.ExpendSelect>   expend_select_stmt
%type <tree.Statement> abort_stmt
%type <tree.Statement> rollback_stmt
%type <tree.Statement> savepoint_stmt

%type <tree.Statement> preparable_set_stmt nonpreparable_set_stmt
%type <tree.Statement> query_lockstat_stmt
%type <tree.Statement> set_session_stmt
%type <tree.Statement> set_csetting_stmt
%type <tree.Statement> set_transaction_stmt
%type <tree.Statement> set_exprs_internal
%type <tree.Statement> generic_set
%type <tree.Statement> set_rest_more
%type <tree.Statement> set_names

%type <tree.Statement> show_stmt
%type <tree.Statement> show_create_trigger_stmt
%type <tree.Statement> show_backup_stmt
%type <tree.Statement> show_kafka_stmt
%type <tree.Statement> show_columns_stmt
%type <tree.Statement> show_constraints_stmt
%type <tree.Statement> show_create_stmt
%type <tree.Statement> show_csettings_stmt
%type <tree.Statement> show_databases_stmt
%type <tree.Statement> show_fingerprints_stmt
%type <tree.Statement> show_grants_stmt
%type <tree.Statement> show_histogram_stmt
%type <tree.Statement> show_indexes_stmt
%type <tree.Statement> show_jobs_stmt
%type <tree.Statement> show_queries_stmt
%type <tree.Statement> show_ranges_stmt
%type <tree.Statement> show_roles_stmt
%type <tree.Statement> show_spaces_stmt
%type <tree.Statement> show_schemas_stmt
%type <tree.Statement> show_sequences_stmt
%type <tree.Statement> show_session_stmt
%type <tree.Statement> show_sessions_stmt
%type <tree.Statement> show_savepoint_stmt
%type <tree.Statement> show_stats_stmt
%type <tree.Statement> show_syntax_stmt
%type <tree.Statement> show_tables_stmt
%type <tree.Statement> show_triggers_stmt
%type <tree.Statement> show_trace_stmt
%type <tree.Statement> show_transaction_stmt
%type <tree.Statement> show_users_stmt
%type <tree.Statement> show_zone_stmt
%type <tree.Statement> show_schedules_stmt
%type <tree.Statement> show_snapshot_stmt
%type <tree.Statement> show_flashback_stmt
%type <tree.Statement> show_functions_stmt
/* geo-partition feature add by jiye */
%type <bool> with_cache
%type <str> session_var param_name
%type <*string> comment_text create_kw

%type <tree.Statement> transaction_stmt
%type <tree.Statement> truncate_stmt
%type <tree.Statement> update_stmt
%type <tree.Statement> upsert_stmt
%type <tree.Statement> merge_stmt
%type <tree.Statement> use_stmt

%type <[]string> opt_incremental
%type <tree.KVOption> kv_option
%type <[]tree.KVOption> kv_option_list opt_with_options var_set_list opt_with_schedule_options
%type <str> import_format
%type <str> load_format
%type <str> col_convert_func

%type <*tree.Select> select_no_parens
%type <tree.SelectStatement> select_clause select_with_parens simple_select values_clause values_clause_temp table_clause simple_select_clause values_clause_merge
%type <tree.LockingClause> for_locking_clause opt_for_locking_clause for_locking_items
%type <*tree.LockingItem> for_locking_item
%type <tree.LockingStrength> for_locking_strength
%type <tree.LockingWaitPolicy> opt_nowait_or_skip
%type <tree.SelectStatement> set_operation

%type <tree.Expr> alter_column_default
%type <tree.Direction> opt_asc_desc

%type <tree.AlterTableCmd> alter_table_cmd
%type <tree.AlterTableCmds> alter_table_cmds
%type <tree.AlterIndexCmd> alter_index_cmd
%type <tree.AlterIndexCmds> alter_index_cmds

%type <tree.DropBehavior> opt_drop_behavior
%type <tree.DropBehavior> opt_interleave_drop_behavior

%type <tree.ValidationBehavior> opt_validate_behavior

%type <str> template_clause encoding_clause opt_lc_collate_clause opt_lc_ctype_clause
%type <[]string> opt_template_encoding
%type <tree.Expr> opt_aexpr

%type <tree.IsolationLevel> transaction_iso_level
%type <tree.UserPriority> transaction_user_priority
%type <tree.ReadWriteMode> transaction_read_mode
%type <tree.SUPDDL> transaction_sup_ddl
%type <tree.Name> transaction_name_stmt

%type <str> name opt_name opt_name_parens opt_to_savepoint opt_with_func_name current_name new_name
%type <str> privilege savepoint_name
%type <tree.KVOption> user_option
%type <tree.KVOption> nocst
%type <tree.Operator> subquery_op
%type <*tree.UnresolvedName> func_name
%type <str> opt_collate

%type <str> cursor_name variable_name

%type <str> database_name index_name opt_index_name column_name insert_column_item statistics_name window_name pl_language file_path
%type <str> family_name opt_family_name table_alias_name constraint_name target_name zone_name partition_name collation_name
%type <str> db_object_name_component transaction_name
%type <*tree.UnresolvedObjectName> table_name schema_name standalone_index_name sequence_name type_name view_name db_object_name simple_db_object_name complex_db_object_name sc_object_name simple_sc_object_name complex_sc_object_name function_name old_function_name new_function_name
%type <*tree.UnresolvedName> table_pattern complex_table_pattern
%type <*tree.UnresolvedName> column_path prefixed_column_path column_path_with_star
%type <tree.TableExpr> insert_target create_stats_target

%type <*tree.TableIndexName> table_index_name
%type <tree.TableIndexNames> table_index_name_list

%type <tree.Operator> math_op

%type <tree.IsolationLevel> iso_level
%type <tree.UserPriority> user_priority

%type <tree.TableDefs> opt_table_elem_list table_elem_list
%type <*tree.InterleaveDef> opt_interleave
%type <*tree.PartitionBy> opt_partition_by partition_by
%type <str> partition opt_partition
%type <[]string> opt_partition_list
%type <[]string> opt_enum opt_set
%type <tree.ListPartition> list_partition hash_partition
%type <[]tree.ListPartition> list_partitions hash_partitions hash_partition_quantity
%type <tree.RangePartition> range_partition
%type <[]tree.RangePartition> range_partitions
%type <*tree.Location> opt_locate_in
%type <tree.LocationList> location_list opt_locate_ins
%type <empty> opt_all_clause
%type <tree.HintSet> select_hint_set hint_clause
%type <tree.Hint> hint_single
%type <*tree.ReadFromStorage> storage_single
%type <tree.ReadFromStorageList> storage_clause
%type <bool> distinct_clause
%type <bool> opt_or_replace
%type <tree.DistinctOn> distinct_on_clause
%type <tree.NameList> opt_column_list insert_column_list opt_stats_columns
%type <tree.OrderBy> sort_clause opt_sort_clause
%type <[]*tree.Order> sortby_list
%type <tree.IndexElemList> index_params
%type <tree.NameList> name_list privilege_list
%type <[]int32> opt_array_bounds
%type <*tree.From> from_clause
%type <tree.TableExprs> from_list rowsfrom_list opt_from_list
%type <tree.TablePatterns> table_pattern_list single_table_pattern_list
%type <tree.TableNames> table_name_list opt_inherits opt_locked_rels
%type <tree.Exprs> expr_list opt_expr_list tuple1_ambiguous_values tuple1_unambiguous_values
%type <tree.SchemaList> schema_name_list
%type <*tree.Tuple> expr_tuple1_ambiguous expr_tuple_unambiguous
%type <tree.NameList> attrs
%type <tree.SelectExprs> target_list
%type <tree.UpdateExprs> set_clause_list
%type <*tree.UpdateExpr> set_clause multiple_set_clause
%type <tree.ArraySubscripts> array_subscripts
%type <tree.GroupBy> group_clause
%type <*tree.Limit> select_limit opt_select_limit
%type <tree.TableNames> relation_expr_list
%type <tree.ReturningClause> returning_clause

%type <[]tree.SequenceOption> sequence_option_list opt_sequence_option_list
%type <tree.SequenceOption> sequence_option_elem

%type <bool> all_or_distinct
%type <bool> with_comment
%type <empty> join_outer
%type <tree.JoinCond> join_qual
%type <str> join_type
%type <str> opt_join_hint

%type <tree.Exprs> extract_list
%type <tree.Exprs> overlay_list
%type <tree.Exprs> position_list
%type <tree.Exprs> substr_list
%type <tree.Exprs> trim_list
%type <tree.Exprs> execute_param_clause
%type <tree.DurationField> opt_interval interval_second interval_qualifier
%type <tree.Expr> overlay_placing

%type <bool> opt_unique opt_cluster
%type <bool> opt_using_gin_btree
%type <bool> opt_constraint_able
%type <bool> trigger_for_spec
%type <bool> function_or_procedure

%type <*tree.Limit> limit_clause offset_clause opt_limit_clause
%type <tree.Expr> select_limit_value
%type <tree.Expr> opt_select_fetch_first_value
%type <empty> row_or_rows
%type <empty> first_or_next

%type <tree.Statement> insert_rest merge_rest
%type <tree.NameList> opt_conf_expr opt_col_def_list
%type <*tree.OnConflict> on_conflict
%type <tree.CondAndOpList> match_list notmatch_list
%type <*tree.CondAndOp> match_item notmatch_item

%type <tree.Statement> begin_transaction
%type <tree.TransactionModes> transaction_mode_list transaction_mode

%type <tree.NameList> opt_storing
%type <*tree.ColumnTableDef> column_def
%type <tree.TableDef> table_elem
%type <tree.Expr> where_clause opt_where_clause opt_where_or_current_clause opt_idx_where where_idx_clause
%type <*tree.ArraySubscript> array_subscript
%type <tree.Expr> opt_slice_bound
%type <*tree.IndexFlags> opt_index_flags
%type <*tree.IndexFlags> index_flags_param
%type <*tree.IndexFlags> index_flags_param_list
%type <tree.Expr> a_expr b_expr c_expr d_expr prior_expr
%type <tree.Expr> start_with connect_by opt_start_with
%type <tree.Expr> substr_from substr_for
%type <tree.Expr> in_expr
%type <tree.Expr> having_clause
%type <tree.Expr> array_expr
%type <tree.Expr> interval
%type <[]coltypes.T> type_list prep_type_clause
%type <tree.Exprs> array_expr_list
%type <*tree.Tuple> row labeled_row
%type <tree.Expr> case_expr case_arg case_default
%type <*tree.When> when_clause
%type <[]*tree.When> when_clause_list
%type <tree.ComparisonOperator> sub_type
%type <tree.Expr> numeric_only
%type <tree.AliasClause> alias_clause opt_alias_clause
%type <bool> ordinality opt_ordinality opt_compact opt_automatic
%type <*tree.Order> sortby
%type <tree.IndexElem> index_elem
%type <tree.TableExpr> table_ref func_table
%type <tree.Exprs> rowsfrom_list
%type <tree.Expr> rowsfrom_item
%type <tree.TableExpr> joined_table
%type <*tree.UnresolvedObjectName> relation_expr
%type <tree.TableExpr> table_name_expr_opt_alias_idx table_name_expr_with_index
%type <tree.SelectExpr> target_elem
%type <*tree.UpdateExpr> single_set_clause
%type <tree.AsOfClause> as_of_clause opt_as_of_clause
%type <tree.AsSnapshotClause> as_snapshot_clause
%type <tree.Expr> opt_changefeed_sink

%type <str> explain_option_name
%type <[]string> explain_option_list
%type <[]string> trigger_events
%type <str> trigger_one_event
%type <[]string> trig_func_args
%type <str> trig_func_arg
%type <[]string>	func_as

%type <coltypes.T> typename simple_typename const_typename enum_typename set_typename
%type <bool> opt_timezone
%type <coltypes.T> numeric opt_numeric_modifiers opt_decimal_modifiers opt_dec_modifiers
%type <coltypes.T> opt_float
%type <coltypes.T> character_with_length character_without_length
%type <coltypes.T> const_datetime const_interval
%type <coltypes.T> bit_with_length bit_without_length
%type <coltypes.T> character_base
%type <coltypes.CastTargetType> postgres_oid
%type <coltypes.CastTargetType> cast_target
%type <str> extract_arg
%type <bool> opt_varying

%type <*tree.NumVal> signed_iconst
%type <int64> signed_iconst64
%type <int64> iconst64
%type <str> trigger_action_time
%type <tree.Expr> var_value
%type <tree.Exprs> var_list
%type <tree.NameList> var_name
%type <str> unrestricted_name type_function_name
%type <str> non_reserved_word
%type <str> non_reserved_word_or_sconst
%type <tree.Expr> zone_value
%type <tree.Expr> string_or_placeholder
%type <tree.Expr> string_or_placeholder_list

%type <tree.ConversionFunc> col_conversion
%type <tree.ConversionFunctions> col_conversion_list

%type <str> unreserved_keyword type_func_name_keyword znbasedb_extra_type_func_name_keyword
%type <str> col_name_keyword reserved_keyword znbasedb_extra_reserved_keyword extra_var_value

%type <tree.ConstraintTableDef> table_constraint constraint_elem
%type <tree.TableDef> index_def
%type <tree.TableDef> family_def
%type <[]tree.NamedColumnQualification> col_qual_list
%type <tree.NamedColumnQualification> col_qualification
%type <tree.ColumnQualification> col_qualification_elem
%type <tree.CompositeKeyMatchMethod> key_match
%type <tree.ReferenceActions> reference_actions
%type <tree.ReferenceAction> reference_action reference_on_delete reference_on_update

%type <tree.Expr> func_application func_expr_common_subexpr special_function
%type <tree.Expr> func_expr func_expr_windowless
%type <tree.Expr> trigger_when
%type <empty> opt_with
%type <*tree.With> with_clause opt_with_clause
%type <[]*tree.CTE> cte_list
%type <*tree.CTE> common_table_expr

%type <empty> within_group_clause
%type <tree.Expr> filter_clause
%type <tree.Expr> encryption_clause
%type <tree.Exprs> opt_partition_clause
%type <tree.Window> window_clause window_definition_list
%type <*tree.WindowDef> window_definition over_clause window_specification
%type <str> opt_existing_window_name
%type <*tree.WindowFrame> opt_frame_clause
%type <tree.WindowFrameBounds> frame_extent
%type <*tree.WindowFrameBound> frame_bound

%type <[]tree.ColumnID> opt_tableref_col_list tableref_col_list

%type <tree.TargetList> targets targets_roles changefeed_targets
%type <*tree.TargetList> opt_on_targets_roles
%type <tree.NameList> for_grantee_clause
%type <tree.PrivilegeGroup> privilege_col
%type <tree.PrivilegeGroups> privileges privilege_cols
%type <[]tree.KVOption> opt_user_options user_options
%type <tree.AuditMode> audit_mode

%type <str> relocate_kw ranges_kw

%type <*tree.SetZoneConfig> set_zone_config

%type <tree.Expr> opt_alter_column_using
%type <*tree.Signal> signal_clause
%type <*string> sqlstate_string_constant diagnostic_string_expression
%type <bool> on_update_current_timestamp
%type <bool> opt_temp
%type <tree.Expr>  cron_expr opt_description sconst_or_placeholder
%type <*tree.FullBackupClause> opt_full_backup_clause

// Precedence: lowest to highest
%nonassoc  VALUES              // see value_clause
%nonassoc  SET                 // see table_name_expr_opt_alias_idx
%left      UNION EXCEPT
%left      INTERSECT
%left      OR
%left      XOR
%left      AND
%right     NOT
%nonassoc  IS ISNULL NOTNULL   // IS sets precedence for IS NULL, etc

%nonassoc  '<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS CONTAINS CONTAINED_BY '?' JSON_SOME_EXISTS JSON_ALL_EXISTS
%nonassoc  '~' BETWEEN IN LIKE ILIKE SIMILAR NOT_REGMATCH REGIMATCH NOT_REGIMATCH NOT_LA
%nonassoc  ESCAPE              // ESCAPE must be just above LIKE/ILIKE/SIMILAR
%nonassoc  OVERLAPS
%left      POSTFIXOP           // dummy for postfix OP rules
// To support target_elem without AS, we must give IDENT an explicit priority
// between POSTFIXOP and OP. We can safely assign the same priority to various
// unreserved keywords as needed to resolve ambiguities (this can't have any
// bad effects since obviously the keywords will still behave the same as if
// they weren't keywords). We need to do this for PARTITION, RANGE, ROWS,
// GROUPS to support opt_existing_window_name; and for RANGE, ROWS, GROUPS so
// that they can follow a_expr without creating postfix-operator problems; and
// for NULL so that it can follow b_expr in col_qual_list without creating
// postfix-operator problems.
//
// To support CUBE and ROLLUP in GROUP BY without reserving them, we give them
// an explicit priority lower than '(', so that a rule with CUBE '(' will shift
// rather than reducing a conflicting rule that takes CUBE as a function name.
// Using the same precedence as IDENT seems right for the reasons given above.
//
// The frame_bound productions UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING are
// even messier: since UNBOUNDED is an unreserved keyword (per spec!), there is
// no principled way to distinguish these from the productions a_expr
// PRECEDING/FOLLOWING. We hack this up by giving UNBOUNDED slightly lower
// precedence than PRECEDING and FOLLOWING. At present this doesn't appear to
// cause UNBOUNDED to be treated differently from other unreserved keywords
// anywhere else in the grammar, but it's definitely risky. We can blame any
// funny behavior of UNBOUNDED on the SQL standard, though.
%nonassoc  UNBOUNDED         // ideally should have same precedence as IDENT
%nonassoc  IDENT NULL PARTITION RANGE ROWS GROUPS PRECEDING FOLLOWING CUBE ROLLUP START CONNECT  UNIQUE
%nonassoc  AS
%nonassoc  KEY
%left      CONCAT FETCHVAL FETCHTEXT FETCHVAL_PATH FETCHTEXT_PATH REMOVE_PATH EQNU RESPECT IGNORE // multi-character ops
%left      LIKE_OP ILIKE_OP NOT_LIKE_OP NOT_ILIKE_OP
%left      '|'
%left      '#'
%left      '&'
%left      LSHIFT RSHIFT INET_CONTAINS_OR_EQUALS INET_CONTAINED_BY_OR_EQUALS INET_CONTAINS_OR_CONTAINED_BY
%left      '+' '-'
%left      '*' '/' FLOORDIV '%'
%left      '^'
// Unary Operators
%left      AT                // sets precedence for AT TIME ZONE
//%left      ATINTERVAL
%left      COLLATE
%right     UMINUS
%left      '[' ']'
%left      '(' ')'
%left      TYPEANNOTATE
%left      TYPECAST
%left      '.'

// These might seem to be low-precedence, but actually they are not part
// of the arithmetic hierarchy at all in their use as JOIN operators.
// We make them high-precedence to support their use as function names.
// They wouldn't be given a precedence at all, were it not that we need
// left-associativity among the JOIN rules themselves.
%left      JOIN CROSS LEFT FULL RIGHT INNER NATURAL
%right     HELPTOKEN

%%

stmt_block:
  stmt
  {
    sqllex.(*lexer).SetStmt($1.stmt())
  }

stmt:
  HELPTOKEN { return helpWith(sqllex, "") }
| preparable_stmt  // help texts in sub-rule
| copy_from_stmt
| comment_stmt
| execute_stmt      // EXTEND WITH HELP: EXECUTE
| deallocate_stmt   // EXTEND WITH HELP: DEALLOCATE
| discard_stmt      // EXTEND WITH HELP: DISCARD
| grant_stmt        // EXTEND WITH HELP: GRANT
| prepare_stmt      // EXTEND WITH HELP: PREPARE
| revoke_stmt       // EXTEND WITH HELP: REVOKE
| savepoint_stmt    // EXTEND WITH HELP: SAVEPOINT
| release_stmt      // EXTEND WITH HELP: RELEASE
| refresh_stmt      // EXTEND WITH HELP: REFRESH
| nonpreparable_set_stmt // help texts in sub-rule
| transaction_stmt  // help texts in sub-rule
| query_lockstat_stmt
| /* EMPTY */
  {
    $$.val = tree.Statement(nil)
  }

// %Help: ALTER
// %Category: Group
// %Text: ALTER TABLE, ALTER INDEX, ALTER SCHEMA, ALTER VIEW, ALTER SEQUENCE, ALTER DATABASE, ALTER USER, ALTER PROCEDURE
alter_stmt:
  alter_ddl_stmt      // help texts in sub-rule
| alter_user_stmt     // EXTEND WITH HELP: ALTER USER
| ALTER error         // SHOW HELP: ALTER

alter_ddl_stmt:
  alter_table_stmt    // EXTEND WITH HELP: ALTER TABLE
| alter_schema_stmt   // EXTEND WITH HELP: ALTER SCHEMA
| alter_index_stmt    // EXTEND WITH HELP: ALTER INDEX
| alter_view_stmt     // EXTEND WITH HELP: ALTER VIEW
| alter_sequence_stmt // EXTEND WITH HELP: ALTER SEQUENCE
| alter_database_stmt // EXTEND WITH HELP: ALTER DATABASE
| alter_range_stmt    // EXTEND WITH HELP: ALTER RANGE
| alter_function_stmt // EXTEND WITH HELP: ALTER PROCEDURE/FUNCTION
| alter_trigger_stmt  // EXTEND WITH HELP: ALTER TRIGGER

// %Help: ALTER TABLE - change the definition of a table
// %Category: DDL
// %Text:
// ALTER TABLE [IF EXISTS] <tablename> <command> [, ...]
//
// Commands:
//   ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS] <colname> <type> [<qualifiers...>]
//   ALTER TABLE ... ADD <constraint>
//   ALTER TABLE ... DROP [COLUMN] [IF EXISTS] <colname> [RESTRICT | CASCADE]
//   ALTER TABLE ... DROP CONSTRAINT [IF EXISTS] <constraintname> [RESTRICT | CASCADE]
//   ALTER TABLE ... ALTER [COLUMN] <colname> {SET DEFAULT <expr> | DROP DEFAULT}
//   ALTER TABLE ... ALTER [COLUMN] <colname> DROP NOT NULL
//   ALTER TABLE ... ALTER [COLUMN] <colname> DROP STORED
//   ALTER TABLE ... ALTER [COLUMN] <colname> [SET DATA] TYPE <type> [COLLATE <collation>]
//   ALTER TABLE ... ALTER PRIMARY KEY USING INDEX <name>
//   ALTER TABLE ... RENAME TO <newname>
//   ALTER TABLE ... RENAME [COLUMN] <colname> TO <newname>
//   ALTER TABLE ... VALIDATE CONSTRAINT <constraintname>
//   ALTER TABLE ... SPLIT AT <selectclause>
//   ALTER TABLE ... SCATTER [ FROM ( <exprs...> ) TO ( <exprs...> ) ]
//   ALTER TABLE ... INJECT STATISTICS ...  (experimental)
//   ALTER TABLE ... PARTITION BY RANGE ( <name...> ) ( <rangespec> )
//   ALTER TABLE ... PARTITION BY LIST ( <name...> ) ( <listspec> )
//   ALTER TABLE ... PARTITION BY NOTHING
//   ALTER TABLE ... REPLICATION
//   ALTER TABLE ... CONFIGURE ZONE <zoneconfig>
//   ALTER TABLE ... ENABLE FLASHBACK WITH TTLDAYS <iconst64>
//   ALTER TABLE ... DISABLE FLASHBACK
//   ALTER PARTITION ... OF TABLE ... CONFIGURE ZONE <zoneconfig>
//
// Column qualifiers:
//   [CONSTRAINT <constraintname>] {NULL | NOT NULL | UNIQUE | PRIMARY KEY | CHECK (<expr>) | DEFAULT <expr>}
//   FAMILY <familyname>, CREATE [IF NOT EXISTS] FAMILY [<familyname>]
//   REFERENCES <tablename> [( <colnames...> )]
//   COLLATE <collationname>
//
// Zone configurations:
//   DISCARD
//   USING <var> = <expr> [, ...]
//   USING <var> = COPY FROM PARENT [, ...]
//   { TO | = } <expr>
//
// %SeeAlso: WEBDOCS/alter-table.html
alter_table_stmt:
  alter_onetable_stmt
| alter_relocate_stmt
| alter_relocate_lease_stmt
| alter_split_stmt
| alter_scatter_stmt
| alter_zone_table_stmt
| alter_rename_table_stmt
| alter_replication_table_stmt
// ALTER TABLE has its error help token here because the ALTER TABLE
// prefix is spread over multiple non-terminals.
| ALTER TABLE error     // SHOW HELP: ALTER TABLE
| ALTER PARTITION error // SHOW HELP: ALTER TABLE

// %Help: ALTER SCHEMA - change the definition of a schema
// %Category: DDL
// %Text:
// ALTER SCHEMA [IF EXISTS] <name> RENAME TO <newname>
// %SeeAlso: WEBDOCS/alter-schema.html
alter_schema_stmt:
  alter_rename_schema_stmt
| ALTER SCHEMA error    // SHOW HELP: ALTER SCHEMA

// %Help: ALTER TRIGGER - change the name of a trigger
// %Category: DDL
// %Text:
// ALTER TRIGGER <oldname> ON <tablename> RENAME TO <newname>
// %SeeAlso: WEBDOCS/alter-trigger.html
alter_trigger_stmt:
  ALTER TRIGGER current_name ON table_name RENAME TO new_name
  {
    Trigname := tree.Name($3)
    Tablename := $5.unresolvedObjectName().ToTableName()
    Newname := tree.Name($8)
    $$.val = &tree.RenameTrigger{Trigname: Trigname, Tablename: Tablename, Newname: Newname, IfExists: false}
  }
|  ALTER TRIGGER error // SHOW HELP: ALTER TRIGGER

// %Help: ALTER PROCEDURE/FUNCTION - rename a procedure/function
// %Category: DDL
// %Text:
// ALTER PROCEDURE/FUNCTION <oldname> <func_args> RENAME TO <newname>
//
// Commands:
//   ALTER PROCEDURE/FUNCTION <oldname> <funcArgTypes> RENAME TO <newname>
//
// %SeeAlso: WEBDOCS/alter-procedure_function.html
alter_function_stmt:
    ALTER PROCEDURE old_function_name func_args RENAME TO new_function_name
    {
	$$.val = &tree.RenameFunction{
		FuncName:	$3.unresolvedObjectName().ToTableName(),
		Parameters:	$4.functionParameters(),
		NewName:	$7.unresolvedObjectName().ToTableName(),
		MissingOk:	false,
    		IsFunction:	false,
	}
    }
|   ALTER FUNCTION old_function_name func_args RENAME TO new_function_name
    {
    	$$.val = &tree.RenameFunction{
    		FuncName:	$3.unresolvedObjectName().ToTableName(),
    		Parameters:	$4.functionParameters(),
    		NewName:	$7.unresolvedObjectName().ToTableName(),
    		MissingOk:	false,
    		IsFunction:	true,
    	}
    }
| ALTER PROCEDURE error // SHOW HELP: ALTER PROCEDURE/FUNCTION
| ALTER FUNCTION error // SHOW HELP: ALTER PROCEDURE/FUNCTION

// %Help: ALTER VIEW - change the definition of a view
// %Category: DDL
// %Text:
// ALTER [MATERIALIZED] VIEW [IF EXISTS] <name> RENAME TO <newname>
// %SeeAlso: WEBDOCS/alter-view.html
alter_view_stmt:
  alter_rename_view_stmt
// ALTER VIEW has its error help token here because the ALTER VIEW
// prefix is spread over multiple non-terminals.
| ALTER VIEW relation_expr AS select_stmt
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.AlterViewAS{Name: name, IfExists: false, AsSource: $5.slct()}
  }
| ALTER VIEW IF EXISTS relation_expr AS select_stmt
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.AlterViewAS{Name: name, IfExists: true, AsSource: $7.slct()}
  }
| ALTER VIEW error // SHOW HELP: ALTER VIEW

// %Help: ALTER SEQUENCE - change the definition of a sequence
// %Category: DDL
// %Text:
// ALTER SEQUENCE [IF EXISTS] <name>
//   [INCREMENT <increment>]
//   [MINVALUE <minvalue> | NO MINVALUE]
//   [MAXVALUE <maxvalue> | NO MAXVALUE]
//   [START <start>]
//   [[NO] CYCLE]
// ALTER SEQUENCE [IF EXISTS] <name> RENAME TO <newname>
alter_sequence_stmt:
  alter_rename_sequence_stmt
| alter_sequence_options_stmt
| ALTER SEQUENCE error // SHOW HELP: ALTER SEQUENCE

alter_sequence_options_stmt:
  ALTER SEQUENCE sequence_name sequence_option_list
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.AlterSequence{Name: name, Options: $4.seqOpts(), IfExists: false}
  }
| ALTER SEQUENCE IF EXISTS sequence_name sequence_option_list
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.AlterSequence{Name: name, Options: $6.seqOpts(), IfExists: true}
  }

// %Help: ALTER USER - change user properties
// %Category: Priv
// %Text:
// ALTER USER [IF EXISTS] <name> WITH PASSWORD <password>
// %SeeAlso: CREATE USER
alter_user_stmt:
  alter_user_options_stmt
| ALTER USER error // SHOW HELP: ALTER USER
| alter_user_searchpath_stmt

// %Help: ALTER DATABASE - change the definition of a database
// %Category: DDL
// %Text:
// ALTER DATABASE <name> RENAME TO <newname>
// %SeeAlso: WEBDOCS/alter-database.html
alter_database_stmt:
  alter_rename_database_stmt
|  alter_zone_database_stmt
// ALTER DATABASE has its error help token here because the ALTER DATABASE
// prefix is spread over multiple non-terminals.
// ALTER DATABASE <name> ENABLE FLASHBACK WITH TTLDAYS <iconst64>
| ALTER DATABASE database_name ENABLE FLASHBACK WITH TTLDAYS iconst64
  {
    $$.val = &tree.AlterDatabaseFlashBack{
      Able: true,
      DatabaseName: tree.Name($3),
      TTLDays: $8.int64(),
    }
  }
  // ALTER TABLE <name> DISABLE FLASHBACK
| ALTER DATABASE database_name DISABLE FLASHBACK
  {
    $$.val = &tree.AlterDatabaseFlashBack{
      Able: false,
      DatabaseName: tree.Name($3),
    }
  }
| ALTER DATABASE error // SHOW HELP: ALTER DATABASE

// %Help: ALTER RANGE - change the parameters of a range
// %Category: DDL
// %Text:
// ALTER RANGE <zonename> <command>
//
// Commands:
//   ALTER RANGE ... CONFIGURE ZONE <zoneconfig>
//
// Zone configurations:
//   DISCARD
//   USING <var> = <expr> [, ...]
//   USING <var> = COPY FROM PARENT [, ...]
//   { TO | = } <expr>
//
// %SeeAlso: ALTER TABLE
alter_range_stmt:
  alter_zone_range_stmt
| ALTER RANGE error // SHOW HELP: ALTER RANGE

// %Help: ALTER INDEX - change the definition of an index
// %Category: DDL
// %Text:
// ALTER INDEX [IF EXISTS] <idxname> <command>
//
// Commands:
//   ALTER INDEX ... RENAME TO <newname>
//   ALTER INDEX ... SPLIT AT <selectclause>
//   ALTER INDEX ... SCATTER [ FROM ( <exprs...> ) TO ( <exprs...> ) ]
//
// %SeeAlso: WEBDOCS/alter-index.html
alter_index_stmt:
  alter_oneindex_stmt
| alter_relocate_index_stmt
| alter_relocate_index_lease_stmt
| alter_split_index_stmt
| alter_scatter_index_stmt
| alter_rename_index_stmt
| alter_partition_table_stmt
| alter_partition_index_stmt
| alter_zone_index_stmt
// ALTER INDEX has its error help token here because the ALTER INDEX
// prefix is spread over multiple non-terminals.
| ALTER INDEX error // SHOW HELP: ALTER INDEX

alter_onetable_stmt:
  ALTER TABLE relation_expr alter_table_cmds
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.AlterTable{Table: name, IfExists: false, Cmds: $4.alterTableCmds()}
  }
| ALTER TABLE IF EXISTS relation_expr alter_table_cmds
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.AlterTable{Table: name, IfExists: true, Cmds: $6.alterTableCmds()}
  }

alter_oneindex_stmt:
  ALTER INDEX table_index_name alter_index_cmds opt_locate_in
  {
  	cmds := append($4.alterIndexCmds(), &tree.AlterIndexLocateIn{LocateSpaceName: $5.location()})
    $$.val = &tree.AlterIndex{Index: $3.newTableIndexName(), IfExists: false, Cmds: cmds}
  }
| ALTER INDEX IF EXISTS table_index_name alter_index_cmds opt_locate_in
  {
  	cmds := append($6.alterIndexCmds(), &tree.AlterIndexLocateIn{LocateSpaceName: $7.location()})
    $$.val = &tree.AlterIndex{Index: $5.newTableIndexName(), IfExists: true, Cmds: cmds}
  }


alter_split_stmt:
  ALTER TABLE table_name SPLIT AT select_stmt
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Split{
      Table: &name,
      TableOrIndex: tree.TableIndexName{Table: name},
      Rows: $6.slct(),
    }
  }

alter_split_index_stmt:
  ALTER INDEX table_index_name SPLIT AT select_stmt
  {
    $$.val = &tree.Split{Index: $3.newTableIndexName(), TableOrIndex: $3.tableIndexName(), Rows: $6.slct()}
  }

relocate_kw:
  TESTING_RELOCATE
| EXPERIMENTAL_RELOCATE

alter_relocate_stmt:
  ALTER TABLE table_name relocate_kw select_stmt
  {
    /* SKIP DOC */
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Relocate{Table: &name, TableOrIndex: tree.TableIndexName{Table: name}, Rows: $5.slct()}
  }

alter_relocate_index_stmt:
  ALTER INDEX table_index_name relocate_kw select_stmt
  {
    /* SKIP DOC */
    $$.val = &tree.Relocate{Index: $3.newTableIndexName(), TableOrIndex: $3.tableIndexName(), Rows: $5.slct()}
  }

alter_relocate_lease_stmt:
  ALTER TABLE table_name relocate_kw LEASE select_stmt
  {
    /* SKIP DOC */
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Relocate{Table: &name, TableOrIndex: tree.TableIndexName{Table: name}, Rows: $6.slct(), RelocateLease: true}
  }

alter_relocate_index_lease_stmt:
  ALTER INDEX table_index_name relocate_kw LEASE select_stmt
  {
    /* SKIP DOC */
    $$.val = &tree.Relocate{Index: $3.newTableIndexName(), TableOrIndex: $3.tableIndexName(), Rows: $6.slct(), RelocateLease: true}
  }

alter_zone_range_stmt:
  ALTER RANGE zone_name set_zone_config
  {
     s := $4.setZoneConfig()
     s.ZoneSpecifier = tree.ZoneSpecifier{NamedZone: tree.UnrestrictedName($3)}
     $$.val = s
  }

set_zone_config:
  CONFIGURE ZONE to_or_eq a_expr
  {
    /* SKIP DOC */
    $$.val = &tree.SetZoneConfig{YAMLConfig: $4.expr()}
  }
| CONFIGURE ZONE USING var_set_list
  {
    $$.val = &tree.SetZoneConfig{Options: $4.kvOptions()}
  }
| CONFIGURE ZONE USING DEFAULT
  {
    /* SKIP DOC */
    $$.val = &tree.SetZoneConfig{SetDefault: true}
  }
| CONFIGURE ZONE DISCARD
  {
    $$.val = &tree.SetZoneConfig{YAMLConfig: tree.DNull}
  }

alter_zone_database_stmt:
  ALTER DATABASE database_name set_zone_config
  {
     s := $4.setZoneConfig()
     s.ZoneSpecifier = tree.ZoneSpecifier{Database: tree.Name($3)}
     $$.val = s
  }

alter_zone_table_stmt:
  ALTER TABLE table_name set_zone_config
  {
    name := $3.unresolvedObjectName().ToTableName()
    s := $4.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: tree.TableIndexName{Table: name},
    }
    $$.val = s
  }

alter_partition_table_stmt:
  ALTER PARTITION partition_name OF TABLE table_name opt_locate_in
  {
    name := $6.unresolvedObjectName().ToTableName()
    cmds := tree.AlterTableCmds{
    		&tree.AlterParitionLocateIn{
    				ParitionName: tree.Name($3),
    				LocateSpaceName: $7.location(),
    		},
    }
    $$.val = &tree.AlterTable{Table: name, IfExists: false, Cmds: cmds}
  }

| ALTER PARTITION partition_name OF TABLE table_name set_zone_config
  {
    name := $6.unresolvedObjectName().ToTableName()
    s := $7.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: tree.TableIndexName{Table: name},
       Partition: tree.Name($3),
    }
    $$.val = s
  }

alter_replication_table_stmt:
  ALTER TABLE table_name REPLICATION ENABLE
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.SetReplica{Table: name, Disable: false}
  }
| ALTER TABLE table_name REPLICATION DISABLE
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.SetReplica{Table: name, Disable: true}
  }

alter_zone_index_stmt:
  ALTER INDEX table_index_name set_zone_config
  {
    s := $4.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: $3.tableIndexName(),
    }
    $$.val = s
  }

alter_partition_index_stmt:
  ALTER PARTITION partition_name OF INDEX table_index_name set_zone_config
  {
    s := $7.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: $6.tableIndexName(),
       Partition: tree.Name($3),
    }
    $$.val = s
  }

var_set_list:
  var_name '=' COPY FROM PARENT
  {
    $$.val = []tree.KVOption{tree.KVOption{Key: tree.Name(strings.Join($1.strs(), "."))}}
  }
| var_name '=' var_value
  {
    $$.val = []tree.KVOption{tree.KVOption{Key: tree.Name(strings.Join($1.strs(), ".")), Value: $3.expr()}}
  }
| var_set_list ',' var_name '=' var_value
  {
    $$.val = append($1.kvOptions(), tree.KVOption{Key: tree.Name(strings.Join($3.strs(), ".")), Value: $5.expr()})
  }
| var_set_list ',' var_name '=' COPY FROM PARENT
  {
    $$.val = append($1.kvOptions(), tree.KVOption{Key: tree.Name(strings.Join($3.strs(), "."))})
  }

alter_scatter_stmt:
  ALTER TABLE table_name SCATTER
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Scatter{Table: &name}
  }
| ALTER TABLE table_name SCATTER FROM '(' expr_list ')' TO '(' expr_list ')'
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Scatter{Table: &name, From: $7.exprs(), To: $11.exprs()}
  }

alter_scatter_index_stmt:
  ALTER INDEX table_index_name SCATTER
  {
    $$.val = &tree.Scatter{Index: $3.newTableIndexName()}
  }
| ALTER INDEX table_index_name SCATTER FROM '(' expr_list ')' TO '(' expr_list ')'
  {
    $$.val = &tree.Scatter{Index: $3.newTableIndexName(), From: $7.exprs(), To: $11.exprs()}
  }

alter_table_cmds:
  alter_table_cmd
  {
    $$.val = tree.AlterTableCmds{$1.alterTableCmd()}
  }
| alter_table_cmds ',' alter_table_cmd
  {
    $$.val = append($1.alterTableCmds(), $3.alterTableCmd())
  }

alter_table_cmd:
  // ALTER TABLE <name> RENAME [COLUMN] <name> TO <newname>
  RENAME opt_column column_name TO column_name
  {
    $$.val = &tree.AlterTableRenameColumn{Column: tree.Name($3), NewName: tree.Name($5) }
  }
  // ALTER TABLE <name> NO INHERIT <name>
| NO INHERIT table_name
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.AlterTableNoInherit{Table: name}
  }
  // ALTER TABLE <name> RENAME CONSTRAINT <name> TO <newname>
| RENAME CONSTRAINT column_name TO column_name
  {
    $$.val = &tree.AlterTableRenameConstraint{Constraint: tree.Name($3), NewName: tree.Name($5) }
  }
  // ALTER TABLE <name> ADD <coldef>
| ADD column_def
  {
    $$.val = &tree.AlterTableAddColumn{IfNotExists: false, ColumnDef: $2.colDef()}
  }
  // ALTER TABLE <name> ADD IF NOT EXISTS <coldef>
| ADD IF NOT EXISTS column_def
  {
    $$.val = &tree.AlterTableAddColumn{IfNotExists: true, ColumnDef: $5.colDef()}
  }
  // ALTER TABLE <name> ADD COLUMN <coldef>
| ADD COLUMN column_def
  {
    $$.val = &tree.AlterTableAddColumn{IfNotExists: false, ColumnDef: $3.colDef()}
  }
  // ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef>
| ADD COLUMN IF NOT EXISTS column_def
  {
    $$.val = &tree.AlterTableAddColumn{IfNotExists: true, ColumnDef: $6.colDef()}
  }
|ADD opt_unique INDEX opt_index_name '(' index_params ')'
 {
   $$.val=&tree.AlterTableAddIndex{
      Name:    tree.Name($4),
      Unique:  $2.bool(),
      Columns: $6.idxElems(),
	}
 }
 |ADD opt_unique KEY opt_index_name '(' index_params ')'
 {
   $$.val=&tree.AlterTableAddIndex{
      Name:    tree.Name($4),
      Unique:  $2.bool(),
      Columns: $6.idxElems(),
	}
 }
 |ADD CONSTRAINT UNIQUE KEY opt_index_name '(' index_params ')'
 {
   $$.val=&tree.AlterTableAddIndex{
      Name:    tree.Name($5),
      Unique:  true,
      Columns: $7.idxElems(),
	}
 }
 |ADD CONSTRAINT UNIQUE INDEX opt_index_name '(' index_params ')'
 {
   $$.val=&tree.AlterTableAddIndex{
      Name:    tree.Name($5),
      Unique:  true,
      Columns: $7.idxElems(),
	}
 }
| ADD CONSTRAINT constraint_name UNIQUE KEY opt_index_name '(' index_params ')'
 {
 indexName:=tree.Name($3)
 if indexName==""{
 indexName=tree.Name($6)
 }
   $$.val=&tree.AlterTableAddIndex{
      Name:    indexName,
      Unique:  true,
      Columns: $8.idxElems(),
	}
 }
| ADD CONSTRAINT constraint_name UNIQUE INDEX opt_index_name '(' index_params ')'
 {
 indexName:=tree.Name($3)
 if indexName==""{
 indexName=tree.Name($6)
 }
   $$.val=&tree.AlterTableAddIndex{
      Name:    indexName,
      Unique:  true,
      Columns: $8.idxElems(),
	}
 }
| RENAME INDEX index_name TO index_name
{
$$.val=&tree.AlterTableRenameIndex{
	OldIndexName:tree.UnrestrictedName($3),
	NewIndexName:tree.UnrestrictedName($5),
	IfExists: false,
}
}
| RENAME KEY index_name TO index_name
{
$$.val=&tree.AlterTableRenameIndex{
	OldIndexName:tree.UnrestrictedName($3),
	NewIndexName:tree.UnrestrictedName($5),
	IfExists: false,
}
}
| DROP INDEX index_name opt_drop_behavior
{
$$.val=&tree.AlterTableDropIndex{
	Index: tree.UnrestrictedName($3),
        IfExists:false,
        DropBehavior: $4.dropBehavior(),
}
}
| DROP KEY index_name opt_drop_behavior
 {
 $$.val=&tree.AlterTableDropIndex{
 	Index: tree.UnrestrictedName($3),
         IfExists:false,
         DropBehavior: $4.dropBehavior(),
 }
 }
| DROP PRIMARY KEY
 {
 $$.val=&tree.AlterTableDropConstraint{
 Constraint:"",
 }
 }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT}
| ALTER opt_column column_name alter_column_default
  {
    $$.val = &tree.AlterTableSetDefault{Column: tree.Name($3), Default: $4.expr()}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL
| ALTER opt_column column_name DROP NOT NULL
  {
    $$.val = &tree.AlterTableDropNotNull{Column: tree.Name($3)}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP STORED
| ALTER opt_column column_name DROP STORED
  {
    $$.val = &tree.AlterTableDropStored{Column: tree.Name($3)}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL
| ALTER opt_column column_name SET NOT NULL
  {
    $$.val = &tree.AlterTableSetNotNull{Column: tree.Name($3)}
  }
  // ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE]
| DROP opt_column IF EXISTS column_name opt_drop_behavior
  {
    $$.val = &tree.AlterTableDropColumn{
      IfExists: true,
      Column: tree.Name($5),
      DropBehavior: $6.dropBehavior(),
    }
  }
  // ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE]
| DROP opt_column column_name opt_drop_behavior
  {
    $$.val = &tree.AlterTableDropColumn{
      IfExists: false,
      Column: tree.Name($3),
      DropBehavior: $4.dropBehavior(),
    }
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname>
  //     [SET DATA] TYPE <typename>
  //     [ COLLATE collation ]
  //     [ USING <expression> ]
| ALTER opt_column column_name opt_set_data TYPE typename opt_collate opt_alter_column_using
  {
    $$.val = &tree.AlterTableAlterColumnType{
      Column: tree.Name($3),
      ToType: $6.colType(),
      Collation: $7,
      Using: $8.expr(),
    }
  }
  // ALTER TABLE <name> ADD CONSTRAINT ...
| ADD table_constraint opt_validate_behavior
  {
    $$.val = &tree.AlterTableAddConstraint{
      ConstraintDef: $2.constraintDef(),
      ValidationBehavior: $3.validationBehavior(),
    }
  }
  // ALTER TABLE <name> ALTER CONSTRAINT ...
| ALTER CONSTRAINT constraint_name error { return unimplementedWithIssueDetail(sqllex, 31632, "alter constraint") }
    // ALTER TABLE <name> VALIDATE CONSTRAINT ...
    // ALTER TABLE <name> ALTER PRIMARY KEY USING INDEX <name>
  | ALTER PRIMARY KEY USING COLUMNS '(' index_params ')' opt_interleave
    {
      $$.val = &tree.AlterTableAlterPrimaryKey{
        Columns: $7.idxElems(),
        Interleave: $9.interleave(),
      }
    }
| VALIDATE CONSTRAINT constraint_name
  {
    $$.val = &tree.AlterTableValidateConstraint{
      Constraint: tree.Name($3),
    }
  }
  //ALTER TABLE <name> ENABLE CONSTRAINT <constraint_name>
| ENABLE CONSTRAINT constraint_name
	{
		$$.val = &tree.AlterTableAbleConstraint{
			Able: false,
			Constraint: tree.Name($3),
		}
	}
	//ALTER TABLE <name> DISABLE CONSTRAINT <constraint_name>
| DISABLE CONSTRAINT constraint_name
	{
		$$.val = &tree.AlterTableAbleConstraint{
  		Able: true,
  		Constraint: tree.Name($3),
  	}
	}
  // ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT IF EXISTS constraint_name opt_drop_behavior
  {
    $$.val = &tree.AlterTableDropConstraint{
      IfExists: true,
      Constraint: tree.Name($5),
      DropBehavior: $6.dropBehavior(),
    }
  }
  // ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT constraint_name opt_drop_behavior
  {
    $$.val = &tree.AlterTableDropConstraint{
      IfExists: false,
      Constraint: tree.Name($3),
      DropBehavior: $4.dropBehavior(),
    }
  }
  // ALTER TABLE <name> EXPERIMENTAL_AUDIT SET <mode>
| EXPERIMENTAL_AUDIT SET audit_mode
  {
    $$.val = &tree.AlterTableSetAudit{Mode: $3.auditMode()}
  }
  // ALTER TABLE <name> PARTITION BY ...
| partition_by
  {
    $$.val = &tree.AlterTablePartitionBy{
      PartitionBy: $1.partitionBy(),
    }
  }
  // ALTER TABLE <name> INJECT STATISTICS <json>
| INJECT STATISTICS a_expr
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTableInjectStats{
      Stats: $3.expr(),
    }
  }
| opt_locate_in
  {
    $$.val = &tree.AlterTableLocateIn{
      LocateSpaceName: $1.location(),
    }
  }
  // ALTER TABLE <name> ENABLE FLASHBACK WITH TTLDAYS <iconst64>
| ENABLE FLASHBACK WITH TTLDAYS iconst64
  {
    $$.val = &tree.AlterTableAbleFlashBack{
	Able: true,
	TTLDays: $5.int64(),
    }
  }
  // ALTER TABLE <name> DISABLE FLASHBACK
| DISABLE FLASHBACK
  {
    $$.val = &tree.AlterTableAbleFlashBack{
    Able: false,
    }
  }

audit_mode:
  READ WRITE { $$.val = tree.AuditModeReadWrite }
| OFF        { $$.val = tree.AuditModeDisable }

alter_index_cmds:
  alter_index_cmd
  {
    $$.val = tree.AlterIndexCmds{$1.alterIndexCmd()}
  }
| alter_index_cmds ',' alter_index_cmd
  {
    $$.val = append($1.alterIndexCmds(), $3.alterIndexCmd())
  }
| /* EMPTY */
  {
    $$.val = tree.AlterIndexCmds{}
  }

alter_index_cmd:
  partition_by
  {
    $$.val = &tree.AlterIndexPartitionBy{
      PartitionBy: $1.partitionBy(),
    }
  }


alter_column_default:
  SET DEFAULT a_expr
  {
    $$.val = $3.expr()
  }
| DROP DEFAULT
  {
    $$.val = nil
  }

opt_alter_column_using:
  USING a_expr
  {
     $$.val = $2.expr()
  }
| /* EMPTY */
  {
     $$.val = nil
  }


opt_drop_behavior:
  CASCADE
  {
    $$.val = tree.DropCascade
  }
| RESTRICT
  {
    $$.val = tree.DropRestrict
  }
| /* EMPTY */
  {
    $$.val = tree.DropDefault
  }

opt_validate_behavior:
  NOT VALID
  {
    $$.val = tree.ValidationSkip
  }
| /* EMPTY */
  {
    $$.val = tree.ValidationDefault
  }

// %Help: DUMP - dump data to external storage
// %Category: Misc
// %Text:
// -- Dump Table/Database data:
// DUMP Targets TO SST <format> <datafile>
//        [ AS OF SYSTEM TIME <expr> ]
//        [ INCREMENTAL FROM <location...> ]
//        [ WITH <option> [= <value>] [, ...] ]
//
// -- Dump data from Query:
// DUMP TO <format> <datafile> FROM <query>
//        [ WITH <option> [= <value>] [, ...] ]
//
// Formats:
//    CSV/SST
//
// Location:
//    "[scheme]://[host]/[path to backup]?[parameters]"

// Targets:
//    TABLE <pattern> [, ...]
//    DATABASE <databasename> [, ...]
//
// Options:
//    SKIP_MISSING_FOREIGN_KEYS
//    delimiter = '...'   [CSV-specific]
//
// %SeeAlso: LOAD, WEBDOCS/dump.html,SELECT
dump_stmt:
  DUMP targets TO import_format string_or_placeholder opt_as_of_clause opt_incremental opt_with_options
  {
    $$.val = &tree.Dump{IsQuery:false, FileFormat:$4,File:$5.expr(),Targets: $2.targetList(), AsOf: $6.asOfClause(), IncrementalFrom: $7.exprs(),  Options: $8.kvOptions()}
  }
| DUMP TO import_format string_or_placeholder FROM expend_select_stmt opt_with_options
  {
    $$.val = &tree.Dump{IsQuery:true, FileFormat:$3,File:$4.expr(), ExpendQuery: $6.expendSlct() , Options: $7.kvOptions()}
  }
| DUMP CLUSTER SETTING TO import_format string_or_placeholder opt_with_options
  {
    $$.val = &tree.Dump{IsSettings:true,IsQuery:true, FileFormat:$5,File:$6.expr() , Options: $7.kvOptions()}
  }
| DUMP ALL CLUSTER TO import_format string_or_placeholder opt_as_of_clause opt_incremental opt_with_options
   {
     $$.val = &tree.Dump{DescriptorCoverage: tree.AllDescriptors,FileFormat:$5,File:$6.expr(),AsOf: $7.asOfClause(), IncrementalFrom: $8.exprs(),Options: $9.kvOptions()}
   }
| DUMP error  { return helpWith(sqllex, "DUMP") }

// %Help: CREATE SCHEDULE FOR DUMP - backup data periodically
// %Text:
// CREATE SCHEDULE [<description>]
// FOR DUMP [<targets>] TO <location...>
// [WITH <backup_option>[=<value>] [, ...]]
// RECURRING [crontab|NEVER] [FULL DUMP <crontab|ALWAYS>]
// [WITH EXPERIMENTAL SCHEDULE OPTIONS <schedule_option>[= <value>] [, ...] ]
//
// All backups run in UTC timezone.
//
// Description:
//   Optional description (or name) for this schedule
//
// Targets:
//   empty targets: Backup entire cluster
//   DATABASE <pattern> [, ...]: comma separated list of databases to backup.
//   TABLE <pattern> [, ...]: comma separated list of tables to backup.
//
// Location:
//   "[scheme]://[host]/[path prefix to backup]?[parameters]"
//   Backup schedule will create subdirectories under this location to store
//   full and periodic backups.
//
// WITH <options>:
//   Options specific to DUMP: See DUMP options
//
// RECURRING <crontab>:
//   The RECURRING expression specifies when we backup.  By default these are incremental
//   backups that capture changes since the last backup, writing to the "current" backup.
//
//   Schedule specified as a string in crontab format.
//   All times in UTC.
//     "5 0 * * *": run schedule 5 minutes past midnight.
//     "@daily": run daily, at midnight
//   See https://en.wikipedia.org/wiki/Cron
//
//   RECURRING NEVER indicates that the schedule is non recurring.
//   If, in addition to 'NEVER', the 'first_run' schedule option is specified,
//   then the schedule will execute once at that time (that is: it's a one-off execution).
//   If the 'first_run' is not specified, then the created scheduled will be in 'PAUSED' state,
//   and will need to be unpaused before it can execute.
//
// FULL DUMP <crontab|ALWAYS>:
//   The optional FULL DUMP '<cron expr>' clause specifies when we'll start a new full backup,
//   which becomes the "current" backup when complete.
//   If FULL DUMP ALWAYS is specified, then the backups triggered by the RECURRING clause will
//   always be full backups. For free users, this is the only accepted value of FULL DUMP.
//
//   If the FULL DUMP clause is omitted, we will select a reasonable default:
//      * RECURRING <= 1 hour: we default to FULL DUMP '@daily';
//      * RECURRING <= 1 day:  we default to FULL DUMP '@weekly';
//      * Otherwise: we default to FULL DUMP ALWAYS.
//
// EXPERIMENTAL SCHEDULE OPTIONS:
//   The schedule can be modified by specifying the following options (which are considered
//   to be experimental at this time):
//   * first_run=TIMESTAMPTZ:
//     execute the schedule at the specified time. If not specified, the default is to execute
//     the scheduled based on it's next RECURRING time.
//   * on_execution_failure='[retry|reschedule|pause]':
//     If an error occurs during the execution, handle the error based as:
//     * retry: retry execution right away
//     * reschedule: retry execution by rescheduling it based on its RECURRING expression.
//       This is the default.
//     * pause: pause this schedule.  Requires manual intervention to unpause.
//   * on_previous_running='[start|skip|wait]':
//     If the previous backup started by this schedule still running, handle this as:
//     * start: start this execution anyway, even if the previous one still running.
//     * skip: skip this execution, reschedule it based on RECURRING (or change_capture_period)
//       expression.
//     * wait: wait for the previous execution to complete.  This is the default.
//
// %SeeAlso: DUMP
create_schedule_for_dump_stmt:
  CREATE SCHEDULE /*$3=*/opt_description FOR
  DUMP /*$6=*/targets TO /*$8=*/import_format
  /*$9=*/string_or_placeholder /*$10=*/opt_with_options
  /*$12=*/cron_expr /*$13=*/opt_full_backup_clause /*$14=*/opt_with_schedule_options
  {
    $$.val = &tree.ScheduledBackup{
      FileFormat:       $8,
      ScheduleName:     $3.expr(),
      Recurrence:       $11.expr(),
      FullBackup:       $12.fullBackupClause(),
      To:               tree.Change($9.expr()),
      Targets:          $6.targetList(),
      ScheduleOptions:  $13.kvOptions(),
      Options:          $10.kvOptions(),
    }
  }
| CREATE SCHEDULE error  // SHOW HELP: CREATE SCHEDULE FOR DUMP

opt_description:
  string_or_placeholder
| /* EMPTY */
  {
     $$.val = nil
  }


// sconst_or_placeholder matches a simple string, or a placeholder.
sconst_or_placeholder:
  SCONST
  {
    $$.val =  tree.NewStrVal($1)
  }
| PLACEHOLDER
  {
    p := $1.placeholder()
    sqllex.(*lexer).UpdateNumPlaceholders(p)
    $$.val = p
  }

cron_expr:
  RECURRING NEVER
  {
    $$.val = nil
  }
| RECURRING sconst_or_placeholder
  // Can't use string_or_placeholder here due to conflict on NEVER branch above
  // (is NEVER a keyword or a variable?).
  {
    $$.val = $2.expr()
  }
opt_full_backup_clause:
  FULL DUMP sconst_or_placeholder
  // Can't use string_or_placeholder here due to conflict on ALWAYS branch below
  // (is ALWAYS a keyword or a variable?).
  {
    $$.val = &tree.FullBackupClause{Recurrence: $3.expr()}
  }
| FULL DUMP ALWAYS
  {
    $$.val = &tree.FullBackupClause{AlwaysFull: true}
  }
| /* EMPTY */
  {
    $$.val = (*tree.FullBackupClause)(nil)
  }

opt_with_schedule_options:
  WITH EXPERIMENTAL SCHEDULE OPTIONS kv_option_list
  {
    $$.val = $5.kvOptions()
  }
| WITH EXPERIMENTAL SCHEDULE OPTIONS '(' kv_option_list ')'
  {
    $$.val = $6.kvOptions()
  }
| /* EMPTY */
  {
    $$.val = nil
  }
import_format:
  name
  {
    $$ = strings.ToUpper($1)
  }

// %Help: LOAD - load data from file in a distributed manner
// %Category: ICL
// %Text:
// -- LOAD both schema and table data:
// LOAD [ TABLE <tablename> FROM ]
//        <format> <datafile>
//        [ WITH <option> [= <value>] [, ...] ]
//
// -- LOAD using specific schema, use only table data from external file:
// LOAD TABLE <tablename>
//        { ( <elements> ) | CREATE USING <schemafile> }
//        <format>
//        DATA ( <datafile> [, ...] )
//        [ WITH <option> [= <value>] [, ...] ]
//
// Formats:
//    CSV
//    MYSQLOUTFILE
//    MYSQLDUMP (mysqldump's SQL output)
//    PGCOPY
//    PGDUMP
//
// Options:
//    distributed = '...'
//    sstsize = '...'
//    temp = '...'
//    delimiter = '...'      [CSV, PGCOPY-specific]
//    nullif = '...'         [CSV, PGCOPY-specific]
//    comment = '...'        [CSV-specific]
//
// %SeeAlso: CREATE TABLE

load_format:
  IDENT
  {
    $$ = strings.ToUpper($1)
  }

load_stmt:
LOAD load_format '(' string_or_placeholder ')' opt_with_options
  {
    $$.val = &tree.LoadImport{Bundle: true, FileFormat: $2, Files: tree.Exprs{$4.expr()}, Options: $6.kvOptions()}
  }
| LOAD TABLE table_name FROM load_format '(' string_or_placeholder ')' opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.LoadImport{Bundle: true, Table: &name, FileFormat: $5, Files: tree.Exprs{$7.expr()}, Options: $9.kvOptions()}
  }
| LOAD TABLE table_name CREATE USING string_or_placeholder load_format DATA '(' string_or_placeholder_list ')' col_conversion_list opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.LoadImport{Table: &name, CreateFile: $6.expr(), FileFormat: $7, Files: $10.exprs(), Conversions: $12.ConversionFunctions(),Options: $13.kvOptions()}
  }
| LOAD DATABASE database_name CREATE USING string_or_placeholder load_format DATA '(' string_or_placeholder_list ')' opt_with_options
    {
      $$.val = &tree.LoadImport{Database: $3, CreateFile: $6.expr(), FileFormat: $7, Files: $10.exprs(), Options: $12.kvOptions()}
    }
| LOAD SCHEMA  schema_name CREATE USING string_or_placeholder load_format DATA '(' string_or_placeholder_list ')' opt_with_options
    {
      name:=$3.unresolvedObjectName().ToSchemaName()
      $$.val = &tree.LoadImport{SchemaName : &name, CreateFile: $6.expr(), FileFormat: $7, Files: $10.exprs(), Options: $12.kvOptions()}
    }
| LOAD TABLE table_name '(' table_elem_list ')'  opt_partition_by  opt_locate_in load_format DATA '(' string_or_placeholder_list ')' col_conversion_list opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.LoadImport{Table: &name, CreateDefs: $5.tblDefs(), PartitionBy: $7.partitionBy(),LocateSpaceName: $8.location(),FileFormat: $9, Files: $12.exprs(), Conversions: $14.ConversionFunctions(),Options: $15.kvOptions()}
  }
| LOAD INTO CLUSTER SETTING  load_format DATA '(' string_or_placeholder_list ')' opt_with_options
  {
    $$.val = &tree.LoadImport{Settings: true, Into: true, FileFormat: $5, Files: $8.exprs(), Options: $10.kvOptions()}
  }
| LOAD INTO table_name '(' insert_column_list ')' load_format DATA '(' string_or_placeholder_list ')' col_conversion_list opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.LoadImport{Table: &name, Into: true, IntoCols: $5.nameList(), FileFormat: $7, Files: $10.exprs(), Conversions: $12.ConversionFunctions(),Options: $13.kvOptions()}
  }
| LOAD INTO table_name load_format DATA '(' string_or_placeholder_list ')' col_conversion_list opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.LoadImport{Table: &name, Into: true, IntoCols: nil, FileFormat: $4, Files: $7.exprs(), Conversions: $9.ConversionFunctions(),Options: $10.kvOptions()}
  }
| LOAD TABLE table_name FROM string_or_placeholder_list opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.LoadRestore{TableName: &name, Files: $5.exprs(), Options: $6.kvOptions()}
  }
| LOAD VIEW view_name FROM string_or_placeholder_list opt_with_options
  {
     name := $3.unresolvedObjectName().ToTableName()
     $$.val = &tree.LoadRestore{TableName: &name, Files: $5.exprs(), Options: $6.kvOptions()}
  }
 | LOAD SEQUENCE sequence_name FROM string_or_placeholder_list opt_with_options
   {
      name := $3.unresolvedObjectName().ToTableName()
      $$.val = &tree.LoadRestore{TableName: &name, Files: $5.exprs(), Options: $6.kvOptions()}
   }
| LOAD SCHEMA schema_name FROM string_or_placeholder_list opt_with_options
  {
    name:=$3.unresolvedObjectName().ToSchemaName()
    $$.val = &tree.LoadRestore{SchemaName: &name, Files: $5.exprs(), Options: $6.kvOptions()}
  }
| LOAD DATABASE database_name FROM string_or_placeholder_list opt_with_options
  {
    $$.val = &tree.LoadRestore{DatabaseName: $3, Files: $5.exprs(), Options: $6.kvOptions()}
  }
| LOAD TABLE table_name FROM string_or_placeholder_list as_of_clause opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.LoadRestore{TableName: &name, Files: $5.exprs(), AsOf: $6.asOfClause(), Options: $7.kvOptions()}
  }
| LOAD SCHEMA schema_name FROM string_or_placeholder_list as_of_clause opt_with_options
  {
    name:=$3.unresolvedObjectName().ToSchemaName()
    $$.val = &tree.LoadRestore{SchemaName: &name, Files: $5.exprs(), AsOf: $6.asOfClause(), Options: $7.kvOptions()}
  }
| LOAD DATABASE database_name FROM string_or_placeholder_list as_of_clause opt_with_options
  {
    $$.val = &tree.LoadRestore{DatabaseName: $3, Files: $5.exprs(), AsOf: $6.asOfClause(), Options: $7.kvOptions()}
  }
| LOAD CLUSTER string_or_placeholder_list opt_with_options
   {
     $$.val = &tree.LoadRestore{DescriptorCoverage: tree.AllDescriptors, Files: $3.exprs(), Options: $4.kvOptions()}
   }
| LOAD CLUSTER string_or_placeholder_list as_of_clause opt_with_options
   {
     $$.val = &tree.LoadRestore{DescriptorCoverage: tree.AllDescriptors, Files: $3.exprs(),  AsOf: $4.asOfClause(), Options: $5.kvOptions()}
   }
| LOAD error // SHOW HELP: LOAD


col_convert_func:
  IDENT
  {
    $$ = strings.ToUpper($1)
  }
col_conversion:
  SET column_name opt_equal col_convert_func '<' string_or_placeholder_list '>'
  {
  $$.val = tree.ConversionFunc{ColumnName: tree.Name($2),FuncName:$4, Params:$6.exprs()}
  }

col_conversion_list:
  col_conversion
  {
    $$.val = tree.ConversionFunctions{$1.ConversionFunc()}
  }
| col_conversion_list ',' col_conversion
  {
    $$.val = append($1.ConversionFunctions(), $3.ConversionFunc())
  }
| /* EMPTY */
  {
    $$.val = (tree.ConversionFunctions)(nil)
  }
// %Help: EXPORT - export data to file in a distributed manner
// %Category: ICL
// %Text:
// EXPORT INTO <format> <datafile> [WITH <option> [= value] [,...]] FROM <query>
//
// Formats:
//    CSV
//
// Options:
//    delimiter = '...'   [CSV-specific]
//
// %SeeAlso: SELECT

string_or_placeholder:
  non_reserved_word_or_sconst
  {
    $$.val = tree.NewStrVal($1)
  }
| PLACEHOLDER
  {
    p := $1.placeholder()
    sqllex.(*lexer).UpdateNumPlaceholders(p)
    $$.val = p
  }

string_or_placeholder_list:
  string_or_placeholder
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| string_or_placeholder_list ',' string_or_placeholder
  {
    $$.val = append($1.exprs(), $3.expr())
  }

opt_incremental:
  INCREMENTAL FROM string_or_placeholder_list
  {
    $$.val = $3.exprs()
  }
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

kv_option:
  name '=' string_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: $3.expr()}
  }
|  name
  {
    $$.val = tree.KVOption{Key: tree.Name($1)}
  }
|  SCONST '=' string_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: $3.expr()}
  }
|  SCONST
  {
    $$.val = tree.KVOption{Key: tree.Name($1)}
  }

kv_option_list:
  kv_option
  {
    $$.val = []tree.KVOption{$1.kvOption()}
  }
|  kv_option_list ',' kv_option
  {
    $$.val = append($1.kvOptions(), $3.kvOption())
  }

opt_with_options:
  WITH kv_option_list
  {
    $$.val = $2.kvOptions()
  }
| WITH OPTIONS '(' kv_option_list ')'
  {
    $$.val = $4.kvOptions()
  }
| /* EMPTY */
  {
    $$.val = nil
  }

copy_from_stmt:
  COPY table_name opt_column_list FROM STDIN
  {
    name := $2.unresolvedObjectName().ToTableName()
    $$.val = &tree.CopyFrom{
       Table: name,
       Columns: $3.nameList(),
       Stdin: true,
    }
  }

// %Help: CANCEL
// %Category: Group
// %Text: CANCEL JOBS, CANCEL QUERIES, CANCEL SESSIONS
cancel_stmt:
  cancel_jobs_stmt     // EXTEND WITH HELP: CANCEL JOBS
| cancel_queries_stmt  // EXTEND WITH HELP: CANCEL QUERIES
| cancel_sessions_stmt // EXTEND WITH HELP: CANCEL SESSIONS
| CANCEL error         // SHOW HELP: CANCEL

// %Help: CANCEL JOBS - cancel background jobs
// %Category: Misc
// %Text:
// CANCEL JOBS <selectclause>
// CANCEL JOB <jobid>
// %SeeAlso: SHOW JOBS, PAUSE JOBS, RESUME JOBS
cancel_jobs_stmt:
  CANCEL JOB a_expr
  {
    $$.val = &tree.ControlJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.CancelJob,
    }
  }
| CANCEL JOB error // SHOW HELP: CANCEL JOBS
| CANCEL JOBS select_stmt
  {
    $$.val = &tree.ControlJobs{Jobs: $3.slct(), Command: tree.CancelJob}
  }
| CANCEL JOBS error // SHOW HELP: CANCEL JOBS

// %Help: CANCEL QUERIES - cancel running queries
// %Category: Misc
// %Text:
// CANCEL QUERIES [IF EXISTS] <selectclause>
// CANCEL QUERY [IF EXISTS] <expr>
// %SeeAlso: SHOW QUERIES
cancel_queries_stmt:
  CANCEL QUERY a_expr
  {
    $$.val = &tree.CancelQueries{
      Queries: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      IfExists: false,
    }
  }
| CANCEL QUERY IF EXISTS a_expr
  {
    $$.val = &tree.CancelQueries{
      Queries: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$5.expr()}}},
      },
      IfExists: true,
    }
  }
| CANCEL QUERY error // SHOW HELP: CANCEL QUERIES
| CANCEL QUERIES select_stmt
  {
    $$.val = &tree.CancelQueries{Queries: $3.slct(), IfExists: false}
  }
| CANCEL QUERIES IF EXISTS select_stmt
  {
    $$.val = &tree.CancelQueries{Queries: $5.slct(), IfExists: true}
  }
| CANCEL QUERIES error // SHOW HELP: CANCEL QUERIES

// %Help: CANCEL SESSIONS - cancel open sessions
// %Category: Misc
// %Text:
// CANCEL SESSIONS [IF EXISTS] <selectclause>
// CANCEL SESSION [IF EXISTS] <sessionid>
// %SeeAlso: SHOW SESSIONS
cancel_sessions_stmt:
  CANCEL SESSION a_expr
  {
   $$.val = &tree.CancelSessions{
      Sessions: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      IfExists: false,
    }
  }
| CANCEL SESSION IF EXISTS a_expr
  {
   $$.val = &tree.CancelSessions{
      Sessions: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$5.expr()}}},
      },
      IfExists: true,
    }
  }
| CANCEL SESSION error // SHOW HELP: CANCEL SESSIONS
| CANCEL SESSIONS select_stmt
  {
    $$.val = &tree.CancelSessions{Sessions: $3.slct(), IfExists: false}
  }
| CANCEL SESSIONS IF EXISTS select_stmt
  {
    $$.val = &tree.CancelSessions{Sessions: $5.slct(), IfExists: true}
  }
| CANCEL SESSIONS error // SHOW HELP: CANCEL SESSIONS

comment_stmt:
  COMMENT ON DATABASE database_name IS comment_text
  {
    $$.val = &tree.CommentOnDatabase{Name: tree.Name($4), Comment: $6.strPtr()}
  }
| COMMENT ON TABLE table_name IS comment_text
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CommentOnTable{Table: name, Comment: $6.strPtr()}
  }
| COMMENT ON COLUMN column_path IS comment_text
  {
    varName, err := $4.unresolvedName().NormalizeVarName()
    if err != nil {
      return setErr(sqllex, err)
    }

    columnItem, ok := varName.(*tree.ColumnItem)
    if !ok {
      sqllex.Error(fmt.Sprintf("invalid column name: %q", tree.ErrString($4.unresolvedName())))
      return 1
    }

    $$.val = &tree.CommentOnColumn{ColumnItem: columnItem, Comment: $6.strPtr()}
  }
| COMMENT ON INDEX table_index_name IS comment_text
  {
    $$.val = &tree.CommentOnIndex{Index: $4.tableIndexName(), Comment: $6.strPtr()}
  }
| COMMENT ON CONSTRAINT constraint_name ON table_name IS comment_text
  {
    table := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.CommentOnConstraint{Constraint: tree.Name($4), Table: table, Comment: $8.strPtr()}
  }
| COMMENT ON error
  {
    return unimplementedWithIssue(sqllex, 19472)
  }

comment_text:
  SCONST
  {
    t := $1
    $$.val = &t
  }
| NULL
  {
    var str *string
    $$.val = str
  }

// %Help: CREATE
// %Category: Group
// %Text:
// CREATE DATABASE, CREATE TABLE, CREATE INDEX, CREATE TABLE AS,
// CREATE USER, CREATE VIEW, CREATE SEQUENCE, CREATE STATISTICS,CREATE SNAPSHOT
// CREATE ROLE CREATE SCHEMA
create_stmt:
  create_user_stmt     // EXTEND WITH HELP: CREATE USER
| create_role_stmt     // EXTEND WITH HELP: CREATE ROLE
| create_ddl_stmt      // help texts in sub-rule
| create_stats_stmt    // EXTEND WITH HELP: CREATE STATISTICS
| create_schedule_for_dump_stmt   // EXTEND WITH HELP: CREATE SCHEDULE FOR DUMP
| create_snapshot_stmt     // EXTEND WITH HELP: CREATE SNAPSHOT
| create_unsupported   {}
| CREATE error         // SHOW HELP: CREATE

create_unsupported:
  CREATE AGGREGATE error { return unimplemented(sqllex, "create aggregate") }
| CREATE CAST error { return unimplemented(sqllex, "create cast") }
| CREATE CONSTRAINT TRIGGER error { return unimplementedWithIssueDetail(sqllex, 28296, "create constraint") }
| CREATE CONVERSION error { return unimplemented(sqllex, "create conversion") }
| CREATE DEFAULT CONVERSION error { return unimplemented(sqllex, "create def conv") }
| CREATE EXTENSION IF NOT EXISTS name error { return unimplemented(sqllex, "create extension " + $6) }
| CREATE EXTENSION name error { return unimplemented(sqllex, "create extension " + $3) }
| CREATE FOREIGN TABLE error { return unimplemented(sqllex, "create foreign table") }
| CREATE FOREIGN DATA error { return unimplemented(sqllex, "create fdw") }
//| CREATE FUNCTION error { return unimplementedWithIssueDetail(sqllex, 17511, "create function") }
//| CREATE OR REPLACE FUNCTION error { return unimplementedWithIssueDetail(sqllex, 17511, "create function") }
| CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE name error { return unimplementedWithIssueDetail(sqllex, 17511, "create language " + $6) }
| CREATE OPERATOR error { return unimplemented(sqllex, "create operator") }
| CREATE PUBLICATION error { return unimplemented(sqllex, "create publication") }
| CREATE opt_or_replace RULE error { return unimplemented(sqllex, "create rule") }
| CREATE SERVER error { return unimplemented(sqllex, "create server") }
| CREATE SUBSCRIPTION error { return unimplemented(sqllex, "create subscription") }
| CREATE TEXT error { return unimplementedWithIssueDetail(sqllex, 7821, "create text") }

opt_or_replace:
  OR REPLACE { $$.val = true}
| /* EMPTY */ { $$.val = false}

opt_trusted:
  TRUSTED {}
| /* EMPTY */ {}

opt_procedural:
  PROCEDURAL {}
| /* EMPTY */ {}

drop_unsupported:
  DROP AGGREGATE error { return unimplemented(sqllex, "drop aggregate") }
| DROP CAST error { return unimplemented(sqllex, "drop cast") }
| DROP COLLATION error { return unimplemented(sqllex, "drop collation") }
| DROP CONVERSION error { return unimplemented(sqllex, "drop conversion") }
| DROP DOMAIN error { return unimplementedWithIssueDetail(sqllex, 27796, "drop") }
| DROP EXTENSION IF EXISTS name error { return unimplemented(sqllex, "drop extension " + $5) }
| DROP EXTENSION name error { return unimplemented(sqllex, "drop extension " + $3) }
| DROP FOREIGN TABLE error { return unimplemented(sqllex, "drop foreign table") }
| DROP FOREIGN DATA error { return unimplemented(sqllex, "drop fdw") }
| DROP MANUALCAST error { return unimplemented(sqllex, "drop manualcast") }
| DROP opt_procedural LANGUAGE name error { return unimplementedWithIssueDetail(sqllex, 17511, "drop language " + $4) }
| DROP OPERATOR error { return unimplemented(sqllex, "drop operator") }
| DROP PUBLICATION error { return unimplemented(sqllex, "drop publication") }
| DROP RULE error { return unimplemented(sqllex, "drop rule") }
| DROP SERVER error { return unimplemented(sqllex, "drop server") }
| DROP SUBSCRIPTION error { return unimplemented(sqllex, "drop subscription") }
| DROP TEXT error { return unimplementedWithIssueDetail(sqllex, 7821, "drop text") }
| DROP TYPE error { return unimplementedWithIssueDetail(sqllex, 27793, "drop type") }

create_ddl_stmt:
  create_changefeed_stmt
| create_database_stmt // EXTEND WITH HELP: CREATE DATABASE
| create_schema_stmt   // EXTEND WITH HELP: CREATE SCHEMA
| create_index_stmt    // EXTEND WITH HELP: CREATE INDEX
| create_table_stmt    // EXTEND WITH HELP: CREATE TABLE
| create_table_as_stmt // EXTEND WITH HELP: CREATE TABLE
// Error case for both CREATE TABLE and CREATE TABLE ... AS in one
| CREATE opt_temp TABLE error   // SHOW HELP: CREATE TABLE
| create_type_stmt     { /* SKIP DOC */ }
| create_view_stmt     // EXTEND WITH HELP: CREATE VIEW
| create_sequence_stmt // EXTEND WITH HELP: CREATE SEQUENCE
| create_trigger_stmt  // EXTEND WITH HELP: CREATE TRIGGER
| create_function_stmt

// %Help: CREATE TRIGGER - create a new trigger
// %Category: DDL
// %Text:
// CREATE TRIGGER <triggername> <trigger_action_time> <trigger_events> ON <table_name> <trigger_for> <when> EXECUTE PROCEDURE <proc_name>
// CREATE TRIGGER <triggername> <trigger_action_time> <trigger_events> ON <table_name> <trigger_for> BEGIN SCONST END
// %SeeAlso: SHOW CREATE, WEBDOCS/create-trigger.html
create_trigger_stmt:
  CREATE TRIGGER name trigger_action_time trigger_events ON table_name
  trigger_for_spec trigger_when EXECUTE function_or_procedure func_application
  {
    name := $7.unresolvedObjectName().ToTableName()
    temp := $12.funcExpr()
    $$.val = &tree.CreateTrigger{
    Trigname: tree.Name($3),
    Tablename: name,
    FuncName: temp.Func,
    Row: $8.bool(),
    Timing: $4,
    Events: $5.strs(),
    WhenClause: $9.expr(),
    Args: temp.Exprs,
    Isconstraint: false,
    Deferrable: false,
    Initdeferred: false,
    Funorpro: $11.bool(),
    //constrre: nil,
    HasProcedure: false,
    }
  }
| CREATE TRIGGER name trigger_action_time trigger_events ON table_name trigger_for_spec trigger_when BEGIN SCONST END
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTrigger{
    Trigname: tree.Name($3),
    Tablename: name,
    Row: $8.bool(),
    Timing: $4,
    Events: $5.strs(),
    WhenClause: $9.expr(),
    Isconstraint: false,
    Deferrable: false,
    Initdeferred: false,
    Funorpro: false,

    HasProcedure: true,
    Procedure: $11,
    }
  }
|  CREATE TRIGGER error // SHOW HELP: CREATE TRIGGER

trigger_action_time:
  BEFORE
| AFTER
| INSTEAD OF

trigger_events:
  trigger_one_event {$$.val = []string{$1}}
| trigger_events OR trigger_one_event {$$.val = append($1.strs(), $3)}

trigger_one_event:
  INSERT
| UPDATE
| DELETE
| TRUNCATE

trigger_for_spec:
  FOR EACH ROW {$$.val = true}
| FOR EACH STATEMENT {$$.val = false}
| /*EMPTY*/ {$$.val = false}

trigger_when:
  WHEN '(' a_expr ')' {$$.val = $3.expr()}
| /*EMPTY*/ {$$.val = nil}

function_or_procedure:
  FUNCTION {$$.val = true}
| PROCEDURE {$$.val = false}

trig_func_args:
  trig_func_arg {$$.val = []string{$1}}
| trig_func_args ',' trig_func_arg {$$.val = append($1.strs(), $3)}
| {$$.val = []string(nil)}

trig_func_arg:
  ICONST {$$ = $1.numVal().OrigString}
| FCONST {$$ = $1.numVal().OrigString}
| SCONST {$$ = "'"+ $1 + "'"}
| unrestricted_name

// %Help: CREATE STATISTICS - create a new table statistic
// %Category: Misc
// %Text:
// CREATE STATISTICS <statisticname>
//   [ON <colname> [, ...]]
//   FROM <tablename> [AS OF SYSTEM TIME <expr>]
create_stats_stmt:
  CREATE STATISTICS statistics_name opt_stats_columns FROM create_stats_target opt_create_stats_options
  {
    $$.val = &tree.CreateStats{
      Name: tree.Name($3),
      ColumnNames: $4.nameList(),
      Table: $6.tblExpr(),
      Options: *$7.createStatsOptions(),
    }
  }
| CREATE STATISTICS error // SHOW HELP: CREATE STATISTICS

opt_stats_columns:
  ON name_list
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

create_stats_target:
  table_name
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = &name
  }
| '[' iconst64 ']'
  {
    /* SKIP DOC */
    $$.val = &tree.TableRef{
      TableID: $2.int64(),
    }
  }

opt_create_stats_options:
  WITH OPTIONS create_stats_option_list
  {
    /* SKIP DOC */
    $$.val = $3.createStatsOptions()
  }
// Allow AS OF SYSTEM TIME without WITH OPTIONS, for consistency with other
// statements.
| as_of_clause
  {
    $$.val = &tree.CreateStatsOptions{
      AsOf: $1.asOfClause(),
    }
  }
| /* EMPTY */
  {
    $$.val = &tree.CreateStatsOptions{}
  }

create_stats_option_list:
  create_stats_option
  {
    $$.val = $1.createStatsOptions()
  }
| create_stats_option_list create_stats_option
  {
    a := $1.createStatsOptions()
    b := $2.createStatsOptions()
    if err := a.CombineWith(b); err != nil {
      return setErr(sqllex, err)
    }
    $$.val = a
  }

create_stats_option:
  THROTTLING FCONST
  {
    /* SKIP DOC */
    value, _ := constant.Float64Val($2.numVal().Value)
    if value < 0.0 || value >= 1.0 {
      sqllex.Error("THROTTLING fraction must be between 0 and 1")
      return 1
    }
    $$.val = &tree.CreateStatsOptions{
      Throttling: value,
    }
  }
| as_of_clause
  {
    $$.val = &tree.CreateStatsOptions{
      AsOf: $1.asOfClause(),
    }
  }

create_changefeed_stmt:
  CREATE CHANGEFEED FOR changefeed_targets opt_changefeed_sink opt_with_options
  {
    $$.val = &tree.CreateChangefeed{
      Targets: $4.targetList(),
      SinkURI: $5.expr(),
      Options: $6.kvOptions(),
    }
  }
| EXPERIMENTAL CHANGEFEED FOR changefeed_targets opt_with_options
  {
    /* SKIP DOC */
    $$.val = &tree.CreateChangefeed{
      Targets: $4.targetList(),
      Options: $5.kvOptions(),
    }
  }
| CREATE DDL CHANGEFEED opt_changefeed_sink opt_with_options
  {
    $$.val = &tree.CreateChangefeed{
      DDL: true,
      SinkURI: $4.expr(),
      Options: $5.kvOptions(),
    }
  }


changefeed_targets:
  single_table_pattern_list
  {
    $$.val = tree.TargetList{Tables: $1.tablePatterns()}
  }
| TABLE single_table_pattern_list
  {
    $$.val = tree.TargetList{Tables: $2.tablePatterns()}
  }

single_table_pattern_list:
  table_name
  {
    $$.val = tree.TablePatterns{$1.unresolvedObjectName().ToUnresolvedName()}
  }
| single_table_pattern_list ',' table_name
  {
    $$.val = append($1.tablePatterns(), $3.unresolvedObjectName().ToUnresolvedName())
  }

opt_changefeed_sink:
  INTO string_or_placeholder
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    /* SKIP DOC */
    $$.val = nil
  }

cursor_stmt:
  declare_cursor_stmt
| oc_cursor_stmt
| fetch_move_cursor_stmt

// cursor declare
declare_cursor_stmt:
DECLARE cursor_name CURSOR opt_cursor_param_def_clause for_or_is select_stmt
  {
    $$.val = &tree.DeclareCursor {
        Cursor: tree.Name($2),
        Select: $6.slct(),
        CursorVars: $4.cursorVars(),
    }
  }
| DECLARE cursor_name REFCURSOR
  {
    $$.val = &tree.DeclareCursor {
        Cursor: tree.Name($2),
        Select: nil,
    }
  }

// open/close cursor
oc_cursor_stmt:
  OPEN cursor_name opt_cursor_param_assign_clause
  {
    $$.val = &tree.CursorAccess{
        CursorName: tree.Name($2),
        Action: tree.OPEN,
        CurVars: $3.cursorVars(),
    }
  }
| OPEN cursor_name FOR select_stmt
  {
    $$.val = &tree.CursorAccess{
        CursorName: tree.Name($2),
        Action: tree.OPEN,
        Select: $4.slct(),
    }
  }
| CLOSE cursor_name
  {
    $$.val = &tree.CursorAccess{
        CursorName: tree.Name($2),
        Action: tree.CLOSE,
    }
  }

// fetch cursor
fetch_move_cursor_stmt:
  FETCH fetch_args opt_cursor_into
  {
    c := $2.cursorAccess()
    c.Action = tree.FETCH
    c.VariableNames = $3.nameList()
    $$.val = c
  }

| MOVE fetch_args
 {
    c := $2.cursorAccess()
    c.Action = tree.MOVE
    $$.val = c
 }

opt_cursor_into:
  INTO variable_name_list
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

// fetch args
fetch_args:
  cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($1),
        Rows: 1,
        Direction: tree.NEXT,
    }
  }
| from_or_in cursor_name
   {
     $$.val = &tree.CursorAccess {
         CursorName: tree.Name($2),
         Rows: 1,
         Direction: tree.NEXT,
     }
   }
| NEXT from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($3),
        Rows: 1,
        Direction: tree.NEXT,
    }
  }
| PRIOR from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($3),
        Rows: 1,
        Direction: tree.PRIOR,
    }
  }
| FIRST from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($3),
        Rows: 1,
        Direction: tree.FIRST,
    }
  }
| LAST from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($3),
        Rows: 1,
        Direction: tree.LAST,
    }
  }
| ABSOLUTE signed_iconst64 from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($4),
        Rows: $2.int64(),
        Direction: tree.ABSOLUTE,
    }
  }
| RELATIVE signed_iconst64 from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($4),
        Rows: $2.int64(),
        Direction: tree.RELATIVE,
    }
  }
| FORWARD from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($3),
        Rows: 1,
        Direction: tree.FORWARD,
    }
  }
| signed_iconst64 from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($3),
        Rows: $1.int64(),
        Direction: tree.FORWARD,
    }
  }
| FORWARD signed_iconst64 from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($4),
        Rows: $2.int64(),
        Direction: tree.FORWARD,
    }
  }
| ALL from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($3),
        Rows: tree.RowMax,
        Direction: tree.FORWARD,
    }
  }
| FORWARD ALL from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($4),
        Rows: tree.RowMax,
        Direction: tree.FORWARD,
    }
  }
| BACKWARD from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($3),
        Rows: 1,
        Direction: tree.BACKWARD,
    }
  }
| BACKWARD signed_iconst64 from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($4),
        Rows: $2.int64(),
        Direction: tree.BACKWARD,
    }
  }
| BACKWARD ALL from_or_in cursor_name
  {
    $$.val = &tree.CursorAccess {
        CursorName: tree.Name($4),
        Rows: tree.RowMax,
        Direction: tree.BACKWARD,
    }
  }

// cursor_from_or_in
from_or_in:
  FROM {}
| IN {}

// cursor_for_or_is
for_or_is:
  FOR {}
| IS {}

// where_current_of
where_current_of:
  WHERE CURRENT OF cursor_name
  {
    $$.val = &tree.CurrentOfExpr{CursorName: tree.Name($4)}
  }

variable_name_list:
  variable_name
  {
    $$.val = tree.NameList{tree.Name($1)}
  }

| variable_name_list ',' variable_name
  {
     $$.val = append($1.nameList(), tree.Name($3))
  }

opt_cursor_param_def_clause:
 '(' cursor_param_def_list ')'
  {
    $$.val = $2.cursorVars()
  }
| /* EMPTY */
  {
    $$.val = tree.CursorVars(nil)
  }

cursor_param_def_list:
  cursor_param_def
  {
    $$.val = tree.CursorVars{$1.cursorVar()}
  }
| cursor_param_def_list ',' cursor_param_def
  {
    $$.val = append($1.cursorVars(), $3.cursorVar())
  }

cursor_param_def:
  variable_name typename
  {
    $$.val = &tree.CursorVar {
        CurVarName: tree.Name($1),
        CurVarColType: $2.colType(),
    }
  }

opt_cursor_param_assign_clause:
  '(' cursor_param_assign_list ')'
  {
    $$.val = $2.cursorVars()
  }
| /* EMPTY */
  {
    $$.val = tree.CursorVars(nil)
  }

cursor_param_assign_list:
  cursor_param_assign
  {
    $$.val = tree.CursorVars{$1.cursorVar()}
  }
| cursor_param_assign_list ',' cursor_param_assign
  {
    $$.val = append($1.cursorVars(), $3.cursorVar())
  }

cursor_param_assign:
  variable_name '=' c_expr
  {
    $$.val = &tree.CursorVar {
	CurVarName: tree.Name($1),
	CurVarDataExpr: $3.expr(),
    }
  }
| c_expr
  {
    $$.val = &tree.CursorVar {
	CurVarDataExpr: $1.expr(),
    }
  }

variable_stmt:
  declare_variable_stmt
| fetch_variable_stmt

// declare cursor variable
declare_variable_stmt:
  DECLARE variable_name typename
  {
    $$.val = &tree.DeclareVariable {
        VariableName: tree.Name($2),
        VariableType: $3.colType(),
    }
  }
|
  DECLARE variable_name VOID
  {
    $$.val = &tree.DeclareVariable {
        VariableName: tree.Name($2),
        VariableType: coltypes.Void,
    }
  }

fetch_variable_stmt:
  FETCH VARIABLE variable_name
  {
    $$.val = &tree.VariableAccess {
        VariableName: tree.Name($3),
    }
  }

// %Help: DELETE - delete rows from a table
// %Category: DML
// %Text: DELETE FROM <tablename> [WHERE <expr>]
//               [ORDER BY <exprs...>]
//               [LIMIT <expr>]
//               [RETURNING <exprs...>]
// %SeeAlso: WEBDOCS/delete.html
delete_stmt:
  opt_with_clause DELETE FROM table_name_expr_opt_alias_idx opt_where_or_current_clause opt_sort_clause opt_limit_clause returning_clause
  {
    $$.val = &tree.Delete{
      With: $1.with(),
      Table: $4.tblExpr(),
      Where: tree.NewWhere(tree.AstWhere, $5.expr()),
      OrderBy: $6.orderBy(),
      Limit: $7.limit(),
      Returning: $8.retClause(),
    }
  }
| opt_with_clause DELETE error // SHOW HELP: DELETE

// %Help: DISCARD - reset the session to its initial state
// %Category: Cfg
// %Text: DISCARD ALL
discard_stmt:
  DISCARD ALL
  {
    $$.val = &tree.Discard{Mode: tree.DiscardModeAll}
  }
| DISCARD PLANS { return unimplemented(sqllex, "discard plans") }
| DISCARD SEQUENCES { return unimplemented(sqllex, "discard sequences") }
| DISCARD TEMP { return unimplemented(sqllex, "discard temp") }
| DISCARD TEMPORARY { return unimplemented(sqllex, "discard temp") }
| DISCARD error // SHOW HELP: DISCARD

// %Help: DROP
// %Category: Group
// %Text:
// DROP DATABASE, DROP SCHEMA, DROP INDEX, DROP TABLE, DROP TRIGGER, DROP VIEW, DROP SEQUENCE,
// DROP USER, DROP ROLE, DROP SNAPSHOT, DROP PROCEDURE, DROP FUNCTION
drop_stmt:
  drop_ddl_stmt      // help texts in sub-rule
| drop_role_stmt     // EXTEND WITH HELP: DROP ROLE
| drop_schedule_stmt // EXTEND WITH HELP: DROP SCHEDULES
| drop_user_stmt     // EXTEND WITH HELP: DROP USER
| drop_snapshot_stmt     // EXTEND WITH HELP: DROP SNAPSHOT
| drop_unsupported   {}
| DROP error         // SHOW HELP: DROP

drop_ddl_stmt:
  drop_database_stmt // EXTEND WITH HELP: DROP DATABASE
| drop_schema_stmt   // EXTEND WITH HELP: DROP SCHEMA
| drop_index_stmt    // EXTEND WITH HELP: DROP INDEX
| drop_table_stmt    // EXTEND WITH HELP: DROP TABLE
| drop_view_stmt     // EXTEND WITH HELP: DROP VIEW
| drop_sequence_stmt // EXTEND WITH HELP: DROP SEQUENCE
| drop_trigger_stmt  // EXTEND WITH HELP: DROP TRIGGER
| drop_function_stmt // EXTEND WITH HELP: DROP PROCEDURE/FUNCTION

// %Help: DROP VIEW - remove a view
// %Category: DDL
// %Text: DROP [MATERIALIZED] VIEW [IF EXISTS] <tablename> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-index.html
drop_view_stmt:
  DROP VIEW table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropView{Names: $3.tableNames(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP VIEW IF EXISTS table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropView{Names: $5.tableNames(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }
| DROP MATERIALIZED VIEW table_name_list opt_drop_behavior
  {
     $$.val = &tree.DropView{
       Names: $4.tableNames(),
       IfExists: false,
       DropBehavior: $5.dropBehavior(),
       IsMaterialized: true,
     }
  }
| DROP MATERIALIZED VIEW IF EXISTS table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropView{
      Names: $6.tableNames(),
      IfExists: true,
      DropBehavior: $7.dropBehavior(),
      IsMaterialized: true,
    }
  }
| DROP VIEW error // SHOW HELP: DROP VIEW

// %Help: DROP TRIGGER - remove a trigger
// %Category: DDL
// %Text: DROP TRIGGER [IF EXISTS] <triggerName> [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-trigger.html
drop_trigger_stmt:
  DROP TRIGGER name ON table_name opt_drop_behavior
  {
    Trigname := tree.Name($3)
    Tablename := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.DropTrigger{Trigname: Trigname, Tablename: Tablename, IfExists: false, DropBehavior: $6.dropBehavior()}
  }
| DROP TRIGGER IF EXISTS name ON table_name opt_drop_behavior
  {
    Trigname := tree.Name($5)
    Tablename := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.DropTrigger{Trigname: Trigname, Tablename: Tablename, IfExists: true, DropBehavior: $8.dropBehavior()}
  }
| DROP TRIGGER error// SHOW HELP: DROP TRIGGER

// %Help: DROP SEQUENCE - remove a sequence
// %Category: DDL
// %Text: DROP SEQUENCE [IF EXISTS] <sequenceName> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: DROP
drop_sequence_stmt:
  DROP SEQUENCE table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropSequence{Names: $3.tableNames(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP SEQUENCE IF EXISTS table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropSequence{Names: $5.tableNames(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }
| DROP SEQUENCE error // SHOW HELP: DROP VIEW

// %Help: DROP TABLE - remove a table
// %Category: DDL
// %Text: DROP TABLE [IF EXISTS] <tablename> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-table.html
drop_table_stmt:
  DROP TABLE table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropTable{Names: $3.tableNames(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP TABLE IF EXISTS table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropTable{Names: $5.tableNames(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }
| DROP TABLE error // SHOW HELP: DROP TABLE

// %Help: DROP SCHEMA - remove a schema
// %Category: DDL
// %Text: DROP SCHEMA [IF EXISTS] <schemaname> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-schema.html
drop_schema_stmt:
  DROP SCHEMA schema_name_list opt_drop_behavior
  {
    $$.val = &tree.DropSchema{Schemas: $3.schemaNames(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP SCHEMA IF EXISTS schema_name_list opt_drop_behavior
  {
    $$.val = &tree.DropSchema{Schemas: $5.schemaNames(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }
| DROP SCHEMA error// SHOW HELP: DROP SCHEMA

// %Help: DROP INDEX - remove an index
// %Category: DDL
// %Text: DROP INDEX [IF EXISTS] <idxname> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-index.html
drop_index_stmt:
  DROP INDEX table_index_name_list opt_drop_behavior
  {
    $$.val = &tree.DropIndex{
      IndexList: $3.newTableIndexNames(),
      IfExists: false,
      DropBehavior: $4.dropBehavior(),
    }
  }
| DROP INDEX IF EXISTS table_index_name_list opt_drop_behavior
  {
    $$.val = &tree.DropIndex{
      IndexList: $5.newTableIndexNames(),
      IfExists: true,
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP INDEX error // SHOW HELP: DROP INDEX

// %Help: DROP DATABASE - remove a database
// %Category: DDL
// %Text: DROP DATABASE [IF EXISTS] <databasename> [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-database.html
drop_database_stmt:
  DROP DATABASE database_name opt_drop_behavior
  {
    $$.val = &tree.DropDatabase{
      Name: tree.Name($3),
      IfExists: false,
      DropBehavior: $4.dropBehavior(),
    }
  }
| DROP DATABASE IF EXISTS database_name opt_drop_behavior
  {
    $$.val = &tree.DropDatabase{
      Name: tree.Name($5),
      IfExists: true,
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP DATABASE error // SHOW HELP: DROP DATABASE

// %Help: DROP USER - remove a user
// %Category: Priv
// %Text: DROP USER [IF EXISTS] <user> [, ...]
// %SeeAlso: CREATE USER, SHOW USERS
drop_user_stmt:
  DROP USER string_or_placeholder_list
  {
    $$.val = &tree.DropUser{Names: $3.exprs(), IfExists: false}
  }
| DROP USER IF EXISTS string_or_placeholder_list
  {
    $$.val = &tree.DropUser{Names: $5.exprs(), IfExists: true}
  }
| DROP USER error // SHOW HELP: DROP USER

// %Help: DROP ROLE - remove a role
// %Category: Priv
// %Text: DROP ROLE [IF EXISTS] <role> [, ...]
// %SeeAlso: CREATE ROLE, SHOW ROLES
drop_role_stmt:
  DROP ROLE string_or_placeholder_list
  {
    $$.val = &tree.DropRole{Names: $3.exprs(), IfExists: false}
  }
| DROP ROLE IF EXISTS string_or_placeholder_list
  {
    $$.val = &tree.DropRole{Names: $5.exprs(), IfExists: true}
  }
| DROP ROLE error // SHOW HELP: DROP ROLE

// %Help: DROP PROCEDURE/FUNCTION - remove a PROCEDURE/FUNCTION
// %Category: DDL
// %Text: DROP PROCEDURE/FUNCTION [IF EXISTS] <func_name> <args' types> [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-procedure/function.html
drop_function_stmt:
  DROP FUNCTION function_name func_args opt_drop_behavior
  {
    $$.val = &tree.DropFunction{
    	IsFunction:	true,
    	FuncName:	$3.unresolvedObjectName().ToTableName(),
    	Parameters:	$4.functionParameters(),
    	DropBehavior:	$5.dropBehavior(),
    	MissingOk:	false,
    	Concurrent:	false,
    }
  }
| DROP FUNCTION IF EXISTS function_name func_args opt_drop_behavior
  {
    $$.val = &tree.DropFunction{
    	IsFunction:	true,
    	FuncName:	$5.unresolvedObjectName().ToTableName(),
    	Parameters:	$6.functionParameters(),
    	DropBehavior:	$7.dropBehavior(),
    	MissingOk:	true,
    	Concurrent:	false,
    }
  }
| DROP PROCEDURE function_name func_args opt_drop_behavior
  {
    $$.val = &tree.DropFunction{
    	IsFunction:	false,
    	FuncName:	$3.unresolvedObjectName().ToTableName(),
    	Parameters:	$4.functionParameters(),
    	DropBehavior:	$5.dropBehavior(),
    	MissingOk:	false,
    	Concurrent:	false,
    }
  }
| DROP PROCEDURE IF EXISTS function_name func_args opt_drop_behavior
  {
    $$.val = &tree.DropFunction{
    	IsFunction:	false,
    	FuncName:	$5.unresolvedObjectName().ToTableName(),
    	Parameters:	$6.functionParameters(),
    	DropBehavior:	$7.dropBehavior(),
    	MissingOk:	true,
    	Concurrent:	false,
    }
  }
| DROP PROCEDURE error  // SHOW HELP: DROP PROCEDURE/FUNCTION
| DROP FUNCTION error  // SHOW HELP: DROP PROCEDURE/FUNCTION



table_name_list:
  table_name
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = tree.TableNames{name}
  }
| table_name_list ',' table_name
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = append($1.tableNames(), name)
  }

schema_name_list:
  schema_name
  {
    name := $1.unresolvedObjectName().ToSchemaName()
    $$.val = tree.SchemaList{name}
  }
| schema_name_list ',' schema_name
  {
    name := $3.unresolvedObjectName().ToSchemaName()
    $$.val = append($1.schemaNames(), name)
  }

// %Help: EXPLAIN - show the logical plan of a query
// %Category: Misc
// %Text:
// EXPLAIN <statement>
// EXPLAIN ([PLAN ,] <planoptions...> ) <statement>
// EXPLAIN [ANALYZE] (DISTSQL) <statement>
// EXPLAIN ANALYZE [(DISTSQL)] <statement>
//
// Explainable statements:
//     SELECT, CREATE, DROP, ALTER, INSERT, UPSERT, UPDATE, DELETE,
//     SHOW, EXPLAIN
//
// Plan options:
//     TYPES, VERBOSE, OPT
//
// %SeeAlso: WEBDOCS/explain.html
explain_stmt:
  EXPLAIN preparable_stmt
  {
    $$.val = &tree.Explain{Statement: $2.stmt()}
  }
| EXPLAIN error // SHOW HELP: EXPLAIN
| EXPLAIN '(' explain_option_list ')' preparable_stmt
  {
    $$.val = &tree.Explain{Options: $3.strs(), Statement: $5.stmt()}
  }
| EXPLAIN ANALYZE preparable_stmt
  {
    $$.val = &tree.Explain{Options: []string{"DISTSQL", $2}, Statement: $3.stmt()}
  }
| EXPLAIN ANALYZE '(' explain_option_list ')' preparable_stmt
  {
    $$.val = &tree.Explain{Options: append($4.strs(), $2), Statement: $6.stmt()}
  }
// This second error rule is necessary, because otherwise
// preparable_stmt also provides "selectclause := '(' error ..." and
// cause a help text for the select clause, which will be confusing in
// the context of EXPLAIN.
| EXPLAIN '(' error // SHOW HELP: EXPLAIN

preparable_stmt:
  alter_stmt        // help texts in sub-rule
| cancel_stmt       // help texts in sub-rule
| create_stmt       // help texts in sub-rule
| cursor_stmt       // help texts in sub-rule
| variable_stmt     // help texts in sub-rule
| delete_stmt       // EXTEND WITH HELP: DELETE
| drop_stmt         // help texts in sub-rule
| dump_stmt         // EXTEND WITH HELP: DUMP
| explain_stmt      // EXTEND WITH HELP: EXPLAIN
| insert_stmt       // EXTEND WITH HELP: INSERT
| load_stmt         // EXTEND WITH HELP: LOAD
| pause_stmt        // EXTEND WITH HELP: PAUSE JOBS
| reset_stmt        // help texts in sub-rule
| resume_stmt       // EXTEND WITH HELP: RESUME JOBS
| revert_stmt       // EXTEND WITH HELP: REVERT SNAPSHOT
| flashback_stmt    // EXTEND WITH HELP: FLASHBACK
| clear_stmt        // EXTEND WITH HELP: CLEAR
| scrub_stmt        // help texts in sub-rule
| select_stmt       // help texts in sub-rule
  {
    $$.val = $1.slct()
  }
| preparable_set_stmt // help texts in sub-rule
| show_stmt         // help texts in sub-rule
| truncate_stmt     // EXTEND WITH HELP: TRUNCATE
| update_stmt       // EXTEND WITH HELP: UPDATE
| upsert_stmt       // EXTEND WITH HELP: UPSERT
| merge_stmt
| call_stmt         // EXTEND WITH HELP: CALL

explain_option_list:
  explain_option_name
  {
    $$.val = []string{$1}
  }
| explain_option_list ',' explain_option_name
  {
    $$.val = append($1.strs(), $3)
  }

// %Help: PREPARE - prepare a statement for later execution
// %Category: Misc
// %Text: PREPARE <name> [ ( <types...> ) ] AS <query>
// %SeeAlso: EXECUTE, DEALLOCATE, DISCARD
prepare_stmt:
  PREPARE table_alias_name prep_type_clause AS preparable_stmt
  {
    $$.val = &tree.Prepare{
      Name: tree.Name($2),
      Types: $3.colTypes(),
      Statement: $5.stmt(),
    }
  }
| PREPARE table_alias_name prep_type_clause AS OPT PLAN SCONST
  {
    /* SKIP DOC */
    $$.val = &tree.Prepare{
      Name: tree.Name($2),
      Types: $3.colTypes(),
      Statement: &tree.CannedOptPlan{Plan: $7},
    }
  }
| PREPARE error // SHOW HELP: PREPARE

prep_type_clause:
  '(' type_list ')'
  {
    $$.val = $2.colTypes();
  }
| /* EMPTY */
  {
    $$.val = []coltypes.T(nil)
  }

// %Help: EXECUTE - execute a statement prepared previously
// %Category: Misc
// %Text: EXECUTE <name> [ ( <exprs...> ) ]
// %SeeAlso: PREPARE, DEALLOCATE, DISCARD
execute_stmt:
  EXECUTE table_alias_name execute_param_clause
  {
    $$.val = &tree.Execute{
      Name: tree.Name($2),
      Params: $3.exprs(),
    }
  }
| EXECUTE table_alias_name execute_param_clause DISCARD ROWS
  {
    /* SKIP DOC */
    $$.val = &tree.Execute{
      Name: tree.Name($2),
      Params: $3.exprs(),
      DiscardRows: true,
    }
  }
| EXECUTE error // SHOW HELP: EXECUTE

execute_param_clause:
  '(' expr_list ')'
  {
    $$.val = $2.exprs()
  }
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

// %Help: DEALLOCATE - remove a prepared statement
// %Category: Misc
// %Text: DEALLOCATE [PREPARE] { <name> | ALL }
// %SeeAlso: PREPARE, EXECUTE, DISCARD
deallocate_stmt:
  DEALLOCATE name
  {
    $$.val = &tree.Deallocate{Name: tree.Name($2)}
  }
| DEALLOCATE PREPARE name
  {
    $$.val = &tree.Deallocate{Name: tree.Name($3)}
  }
| DEALLOCATE ALL
  {
    $$.val = &tree.Deallocate{}
  }
| DEALLOCATE PREPARE ALL
  {
    $$.val = &tree.Deallocate{}
  }
| DEALLOCATE error // SHOW HELP: DEALLOCATE

// %Help: GRANT - define access privileges and role memberships
// %Category: Priv
// %Text:
// Grant privileges:
//   GRANT {ALL | <privileges...> } ON <targets...> TO <grantees...>
// Grant role membership (ICL only):
//   GRANT <roles...> TO <grantees...> [WITH ADMIN OPTION]
//
// Privileges:
//   CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE
//
// Targets:
//   DATABASE <databasename> [, ...]
//   [TABLE] [<databasename> .] { <tablename> | * } [, ...]
//
// %SeeAlso: REVOKE, WEBDOCS/grant.html
grant_stmt:
  GRANT privileges ON targets TO name_list
  {
    $$.val = &tree.Grant{Privileges: $2.privilegeGroups(), Grantees: $6.nameList(), Targets: $4.targetList()}
  }
| GRANT privileges ON targets TO name_list WITH GRANT OPTION
  {
    $$.val = &tree.Grant{Privileges: $2.privilegeGroups(), Grantees: $6.nameList(), Targets: $4.targetList(), GrantAble: true}
  }
| GRANT privilege_cols ON targets TO name_list
  {
    $$.val = &tree.Grant{Privileges: $2.privilegeGroups(), Grantees: $6.nameList(), Targets: $4.targetList()}
  }
| GRANT privilege_cols ON targets TO name_list WITH GRANT OPTION
  {
    $$.val = &tree.Grant{Privileges: $2.privilegeGroups(), Grantees: $6.nameList(), Targets: $4.targetList(), GrantAble: true}
  }
| GRANT privilege_list TO name_list
  {
    $$.val = &tree.GrantRole{Roles: $2.nameList(), Members: $4.nameList(), AdminOption: false}
  }
| GRANT privilege_list TO name_list WITH ADMIN OPTION
  {
    $$.val = &tree.GrantRole{Roles: $2.nameList(), Members: $4.nameList(), AdminOption: true}
  }
| GRANT error // SHOW HELP: GRANT

// %Help: REVOKE - remove access privileges and role memberships
// %Category: Priv
// %Text:
// Revoke privileges:
//   REVOKE {ALL | <privileges...> } ON <targets...> FROM <grantees...>
// Revoke role membership (ICL only):
//   REVOKE [ADMIN OPTION FOR] <roles...> FROM <grantees...>
//
// Privileges:
//   CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE
//
// Targets:
//   DATABASE <databasename> [, <databasename>]...
//   [TABLE] [<databasename> .] { <tablename> | * } [, ...]
//
// %SeeAlso: GRANT, WEBDOCS/revoke.html
revoke_stmt:
  REVOKE privileges ON targets FROM name_list
  {
    $$.val = &tree.Revoke{Privileges: $2.privilegeGroups(), Grantees: $6.nameList(), Targets: $4.targetList()}
  }
| REVOKE GRANT OPTION FOR privileges ON targets FROM name_list
  {
    $$.val = &tree.Revoke{Privileges: $5.privilegeGroups(), Grantees: $9.nameList(), Targets: $7.targetList(), GrantAble: true}
  }
| REVOKE privilege_cols ON targets FROM name_list
  {
    $$.val = &tree.Revoke{Privileges: $2.privilegeGroups(), Grantees: $6.nameList(), Targets: $4.targetList()}
  }
| REVOKE GRANT OPTION FOR privilege_cols ON targets FROM name_list
  {
    $$.val = &tree.Revoke{Privileges: $5.privilegeGroups(), Grantees: $9.nameList(), Targets: $7.targetList(), GrantAble: true}
  }
| REVOKE privilege_list FROM name_list
  {
    $$.val = &tree.RevokeRole{Roles: $2.nameList(), Members: $4.nameList(), AdminOption: false }
  }
| REVOKE ADMIN OPTION FOR privilege_list FROM name_list
  {
    $$.val = &tree.RevokeRole{Roles: $5.nameList(), Members: $7.nameList(), AdminOption: true }
  }
| REVOKE error // SHOW HELP: REVOKE

// ALL is always by itself.
privileges:
  ALL
  {
   $$.val = tree.PrivilegeGroups{tree.PrivilegeGroup{Privilege: privilege.ALL}}
  }
| ALL '(' insert_column_list ')'
  {
   $$.val = tree.PrivilegeGroups{tree.PrivilegeGroup{Privilege: privilege.ALL, Columns: $3.nameList()}}
  }
  | privilege_list
  {
     privList, err := privilege.ListFromStrings($1.nameList().ToStrings())
     if err != nil {
       return setErr(sqllex, err)
     }
     privColList := tree.PrivilegeGroups{}
     for _, p := range privList {
       privColList = append(privColList, tree.PrivilegeGroup{Privilege : p})
     }
     $$.val = privColList
  }

privilege_list:
  privilege
  {
    $$.val = tree.NameList{tree.Name($1)}
  }
| privilege_list ',' privilege
  {
    $$.val = append($1.nameList(), tree.Name($3))
  }

privilege_cols :
  privilege_col
  {
    $$.val = tree.PrivilegeGroups{$1.privilegeGroup()}
  }
| privilege_cols ',' privilege_col
  {
    $$.val = append($1.privilegeGroups(), $3.privilegeGroup())
  }

privilege_col :
 privilege '(' insert_column_list ')' {
    priv, err := privilege.KindFromString(string($1))
    if err != nil {
    	return setErr(sqllex, err)
    }
    $$.val = tree.PrivilegeGroup{Privilege: priv, Columns: $3.nameList()}
 }

// Privileges are parsed at execution time to avoid having to make them reserved.
// Any privileges above `col_name_keyword` should be listed here.
// The full list is in sql/privilege/privilege.go.
privilege:
  name
| CREATE
| GRANT
| SELECT
| REFERENCES

reset_stmt:
  reset_session_stmt  // EXTEND WITH HELP: RESET
| reset_csetting_stmt // EXTEND WITH HELP: RESET CLUSTER SETTING

// %Help: RESET - reset a session variable to its default value
// %Category: Cfg
// %Text: RESET [SESSION] <var>
// %SeeAlso: RESET CLUSTER SETTING, WEBDOCS/set-vars.html
reset_session_stmt:
  RESET session_var
  {
    $$.val = &tree.SetVar{Name: $2, Values:tree.Exprs{tree.DefaultVal{}}}
  }
| RESET SESSION session_var
  {
    $$.val = &tree.SetVar{Name: $3, Values:tree.Exprs{tree.DefaultVal{}}}
  }
| RESET error // SHOW HELP: RESET

// %Help: RESET CLUSTER SETTING - reset a cluster setting to its default value
// %Category: Cfg
// %Text: RESET CLUSTER SETTING <var>
// %SeeAlso: SET CLUSTER SETTING, RESET
reset_csetting_stmt:
  RESET CLUSTER SETTING var_name
  {
    $$.val = &tree.SetClusterSetting{Name: strings.Join($4.strs(), "."), Value:tree.DefaultVal{}}
  }
| RESET CLUSTER error // SHOW HELP: RESET CLUSTER SETTING

// USE is the MSSQL/MySQL equivalent of SET DATABASE. Alias it for convenience.
// %Help: USE - set the current database
// %Category: Cfg
// %Text: USE <dbname>
//
// "USE <dbname>" is an alias for "SET [SESSION] database = <dbname>".
// %SeeAlso: SET SESSION, WEBDOCS/set-vars.html
use_stmt:
  USE var_value
  {
    $$.val = &tree.SetVar{Name: "database", Values: tree.Exprs{$2.expr()}}
  }
| USE error // SHOW HELP: USE

// SET remainder, e.g. SET TRANSACTION
nonpreparable_set_stmt:
  set_transaction_stmt // EXTEND WITH HELP: SET TRANSACTION
| set_exprs_internal   { /* SKIP DOC */ }
| SET CONSTRAINTS error { return unimplemented(sqllex, "set constraints") }
| SET LOCAL error { return unimplementedWithIssue(sqllex, 32562) }

// SET SESSION / SET CLUSTER SETTING
preparable_set_stmt:
  set_session_stmt     // EXTEND WITH HELP: SET SESSION
| set_csetting_stmt    // EXTEND WITH HELP: SET CLUSTER SETTING
| use_stmt             // EXTEND WITH HELP: USE

// %Help: SCRUB - run checks against databases or tables
// %Category: Experimental
// %Text:
// EXPERIMENTAL SCRUB TABLE <table> ...
// EXPERIMENTAL SCRUB DATABASE <database>
//
// The various checks that ca be run with SCRUB includes:
//   - Physical table data (encoding)
//   - Secondary index integrity
//   - Constraint integrity (NOT NULL, CHECK, FOREIGN KEY, UNIQUE)
// %SeeAlso: SCRUB TABLE, SCRUB DATABASE
scrub_stmt:
  scrub_table_stmt
| scrub_database_stmt
| EXPERIMENTAL SCRUB error // SHOW HELP: SCRUB

// %Help: SCRUB DATABASE - run scrub checks on a database
// %Category: Experimental
// %Text:
// EXPERIMENTAL SCRUB DATABASE <database>
//                             [AS OF SYSTEM TIME <expr>]
//
// All scrub checks will be run on the database. This includes:
//   - Physical table data (encoding)
//   - Secondary index integrity
//   - Constraint integrity (NOT NULL, CHECK, FOREIGN KEY, UNIQUE)
// %SeeAlso: SCRUB TABLE, SCRUB
scrub_database_stmt:
  EXPERIMENTAL SCRUB DATABASE database_name opt_as_of_clause
  {
    $$.val = &tree.Scrub{Typ: tree.ScrubDatabase, Database: tree.Name($4), AsOf: $5.asOfClause()}
  }
| EXPERIMENTAL SCRUB DATABASE error // SHOW HELP: SCRUB DATABASE

// %Help: SCRUB TABLE - run scrub checks on a table
// %Category: Experimental
// %Text:
// SCRUB TABLE <tablename>
//             [AS OF SYSTEM TIME <expr>]
//             [WITH OPTIONS <option> [, ...]]
//
// Options:
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS INDEX ALL
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS INDEX (<index>...)
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS CONSTRAINT ALL
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS CONSTRAINT (<constraint>...)
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS PHYSICAL
// %SeeAlso: SCRUB DATABASE, SRUB
scrub_table_stmt:
  EXPERIMENTAL SCRUB TABLE table_name opt_as_of_clause opt_scrub_options_clause
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.Scrub{
      Typ: tree.ScrubTable,
      Table: name,
      AsOf: $5.asOfClause(),
      Options: $6.scrubOptions(),
    }
  }
| EXPERIMENTAL SCRUB TABLE error // SHOW HELP: SCRUB TABLE

opt_scrub_options_clause:
  WITH OPTIONS scrub_option_list
  {
    $$.val = $3.scrubOptions()
  }
| /* EMPTY */
  {
    $$.val = tree.ScrubOptions{}
  }

scrub_option_list:
  scrub_option
  {
    $$.val = tree.ScrubOptions{$1.scrubOption()}
  }
| scrub_option_list ',' scrub_option
  {
    $$.val = append($1.scrubOptions(), $3.scrubOption())
  }

scrub_option:
  INDEX ALL
  {
    $$.val = &tree.ScrubOptionIndex{}
  }
| INDEX '(' name_list ')'
  {
    $$.val = &tree.ScrubOptionIndex{IndexNames: $3.nameList()}
  }
| CONSTRAINT ALL
  {
    $$.val = &tree.ScrubOptionConstraint{}
  }
| CONSTRAINT '(' name_list ')'
  {
    $$.val = &tree.ScrubOptionConstraint{ConstraintNames: $3.nameList()}
  }
| PHYSICAL
  {
    $$.val = &tree.ScrubOptionPhysical{}
  }

// %Help: SET CLUSTER SETTING - change a cluster setting
// %Category: Cfg
// %Text: SET CLUSTER SETTING <var> { TO | = } <value>
// %SeeAlso: SHOW CLUSTER SETTING, RESET CLUSTER SETTING, SET SESSION,
// WEBDOCS/cluster-settings.html
set_csetting_stmt:
  SET CLUSTER SETTING var_name to_or_eq var_value
  {
    $$.val = &tree.SetClusterSetting{Name: strings.Join($4.strs(), "."), Value: $6.expr()}
  }
| SET CLUSTER error // SHOW HELP: SET CLUSTER SETTING

to_or_eq:
  '='
| TO

set_exprs_internal:
  /* SET ROW serves to accelerate parser.parseExprs().
     It cannot be used by clients. */
  SET ROW '(' expr_list ')'
  {
    $$.val = &tree.SetVar{Values: $4.exprs()}
  }

// %Help: SET SESSION - change a session variable
// %Category: Cfg
// %Text:
// SET [SESSION] <var> { TO | = } <values...>
// SET [SESSION] TIME ZONE <tz>
// SET [SESSION] CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE }
// SET [SESSION] TRACING { TO | = } { on | off | cluster | local | kv | results } [,...]
//
// %SeeAlso: SHOW SESSION, RESET, DISCARD, SHOW, SET CLUSTER SETTING, SET TRANSACTION,
// WEBDOCS/set-vars.html
set_session_stmt:
  SET SESSION set_rest_more
  {
    $$.val = $3.stmt()
  }
| SET set_rest_more
  {
    $$.val = $2.stmt()
  }
// Special form for pg compatibility:
| SET SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
  {
    $$.val = &tree.SetSessionCharacteristics{Modes: $6.transactionModes()}
  }

// %Help: SET TRANSACTION - configure the transaction settings
// %Category: Txn
// %Text:
// SET [SESSION] TRANSACTION <txnparameters...>
//
// Transaction parameters:
//    ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE }
//    PRIORITY { LOW | NORMAL | HIGH }
//    DDL { TRUE | FALSE }
//
// %SeeAlso: SHOW TRANSACTION, SET SESSION,
// WEBDOCS/set-transaction.html
set_transaction_stmt:
  SET TRANSACTION transaction_mode_list
  {
    $$.val = &tree.SetTransaction{Modes: $3.transactionModes()}
  }
| SET TRANSACTION error // SHOW HELP: SET TRANSACTION
| SET SESSION TRANSACTION transaction_mode_list
  {
    $$.val = &tree.SetTransaction{Modes: $4.transactionModes()}
  }
| SET SESSION TRANSACTION error // SHOW HELP: SET TRANSACTION

generic_set:
  var_name to_or_eq var_list
  {
    // We need to recognize the "set tracing" specially here; couldn't make "set
    // tracing" a different grammar rule because of ambiguity.
    varName := $1.strs()
    if len(varName) == 1 && varName[0] == "tracing" {
      $$.val = &tree.SetTracing{Values: $3.exprs()}
    } else {
      $$.val = &tree.SetVar{Name: strings.Join($1.strs(), "."), Values: $3.exprs()}
    }
  }

set_rest_more:
// Generic SET syntaxes:
   generic_set
// Special SET syntax forms in addition to the generic form.
// See: https://www.postgresql.org/docs/10/static/sql-set.html
//
// "SET TIME ZONE value is an alias for SET timezone TO value."
| TIME ZONE zone_value
  {
    /* SKIP DOC */
    $$.val = &tree.SetVar{Name: "timezone", Values: tree.Exprs{$3.expr()}}
  }
// "SET SCHEMA 'value' is an alias for SET search_path TO value. Only
// one schema can be specified using this syntax."
| SCHEMA var_value
  {
    /* SKIP DOC */
    $$.val = &tree.SetVar{Name: "search_path", Values: tree.Exprs{$2.expr()}}
  }
// See comment for the non-terminal for SET NAMES below.
| set_names
| var_name FROM CURRENT { return unimplemented(sqllex, "set from current") }
| error // SHOW HELP: SET SESSION

// SET NAMES is the SQL standard syntax for SET client_encoding.
// "SET NAMES value is an alias for SET client_encoding TO value."
// See https://www.postgresql.org/docs/10/static/sql-set.html
// Also see https://www.postgresql.org/docs/9.6/static/multibyte.html#AEN39236
set_names:
  NAMES var_value
  {
    /* SKIP DOC */
    $$.val = &tree.SetVar{Name: "client_encoding", Values: tree.Exprs{$2.expr()}}
  }
| NAMES
  {
    /* SKIP DOC */
    $$.val = &tree.SetVar{Name: "client_encoding", Values: tree.Exprs{tree.DefaultVal{}}}
  }

var_name:
  name
  {
    $$.val = []string{$1}
  }
| name attrs
  {
    $$.val = append([]string{$1}, $2.strs()...)
  }

attrs:
  '.' unrestricted_name
  {
    $$.val = []string{$2}
  }
| attrs '.' unrestricted_name
  {
    $$.val = append($1.strs(), $3)
  }

var_value:
  a_expr
| extra_var_value
  {
    $$.val = tree.Expr(&tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{$1}})
  }

// The RHS of a SET statement can contain any valid expression, which
// themselves can contain identifiers like TRUE, FALSE. These are parsed
// as column names (via a_expr) and later during semantic analysis
// assigned their special value.
//
// In addition, for compatibility with ZNBaseDB we need to support
// the reserved keyword ON (to go along OFF, which is a valid column name).
//
// Finally, in PostgreSQL the ZNBaseDB-reserved words "index",
// "nothing", etc. are not special and are valid in SET. These need to
// be allowed here too.
extra_var_value:
  ON
| znbasedb_extra_reserved_keyword

var_list:
  var_value
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| var_list ',' var_value
  {
    $$.val = append($1.exprs(), $3.expr())
  }

iso_level:
  READ UNCOMMITTED
  {
    $$.val = tree.ReadCommittedIsolation
  }
| READ COMMITTED
  {
    $$.val = tree.ReadCommittedIsolation
  }
| SNAPSHOT
  {
    $$.val = tree.SerializableIsolation
  }
| REPEATABLE READ
  {
    $$.val = tree.SerializableIsolation
  }
| SERIALIZABLE
  {
    $$.val = tree.SerializableIsolation
  }

user_priority:
  LOW
  {
    $$.val = tree.Low
  }
| NORMAL
  {
    $$.val = tree.Normal
  }
| HIGH
  {
    $$.val = tree.High
  }

// Timezone values can be:
// - a string such as 'pst8pdt'
// - an identifier such as "pst8pdt"
// - an integer or floating point number
// - a time interval per SQL99
zone_value:
  SCONST
  {
    $$.val = tree.NewStrVal($1)
  }
| IDENT
  {
    $$.val = tree.NewStrVal($1)
  }
| interval
  {
    $$.val = $1.expr()
  }
| numeric_only
| DEFAULT
  {
    $$.val = tree.DefaultVal{}
  }
| LOCAL
  {
    $$.val = tree.NewStrVal($1)
  }

// %Help: SHOW
// %Category: Group
// %Text:
// SHOW DUMP, SHOW CLUSTER SETTING, SHOW COLUMNS, SHOW CONSTRAINTS,
// SHOW CREATE, SHOW DATABASES, SHOW HISTOGRAM, SHOW INDEXES, SHOW
// JOBS, SHOW QUERIES, SHOW ROLES, SHOW SCHEMAS, SHOW SEQUENCES, SHOW
// SESSION, SHOW SESSIONS, SHOW STATISTICS, SHOW SYNTAX, SHOW TABLES,
// SHOW TRACE SHOW TRANSACTION, SHOW USERS
show_stmt:
  show_backup_stmt          // EXTEND WITH HELP: SHOW DUMP
| show_columns_stmt         // EXTEND WITH HELP: SHOW COLUMNS
| show_constraints_stmt     // EXTEND WITH HELP: SHOW CONSTRAINTS
| show_create_stmt          // EXTEND WITH HELP: SHOW CREATE
| show_create_trigger_stmt  // EXTEND WITH HELP: SHOW CREATE TRIGGER
| show_csettings_stmt       // EXTEND WITH HELP: SHOW CLUSTER SETTING
| show_databases_stmt       // EXTEND WITH HELP: SHOW DATABASES
| show_fingerprints_stmt
| show_grants_stmt          // EXTEND WITH HELP: SHOW GRANTS
| show_histogram_stmt       // EXTEND WITH HELP: SHOW HISTOGRAM
| show_indexes_stmt         // EXTEND WITH HELP: SHOW INDEXES
| show_jobs_stmt            // EXTEND WITH HELP: SHOW JOBS
| show_kafka_stmt           // EXTEND WITH HELP: SHOW LOAD_KAFKA
| show_schedules_stmt       // EXTEND WITH HELP: SHOW SCHEDULES
| show_queries_stmt         // EXTEND WITH HELP: SHOW QUERIES
| show_ranges_stmt          // EXTEND WITH HELP: SHOW RANGES
| show_roles_stmt           // EXTEND WITH HELP: SHOW ROLES
| show_savepoint_stmt       // EXTEND WITH HELP: SHOW SAVEPOINT
| show_schemas_stmt         // EXTEND WITH HELP: SHOW SCHEMAS
| show_sequences_stmt       // EXTEND WITH HELP: SHOW SEQUENCES
| show_session_stmt         // EXTEND WITH HELP: SHOW SESSION
| show_sessions_stmt        // EXTEND WITH HELP: SHOW SESSIONS
| show_spaces_stmt	        // EXTEND WITH HELP: SHOW SPACES
| show_stats_stmt           // EXTEND WITH HELP: SHOW STATISTICS
| show_syntax_stmt          // EXTEND WITH HELP: SHOW SYNTAX
| show_tables_stmt          // EXTEND WITH HELP: SHOW TABLES
| show_trace_stmt           // EXTEND WITH HELP: SHOW TRACE
| show_transaction_stmt     // EXTEND WITH HELP: SHOW TRANSACTION
| show_triggers_stmt	    // EXTEND WITH HELP: SHOW TRIGGERS
| show_users_stmt           // EXTEND WITH HELP: SHOW USERS
| show_zone_stmt
| show_snapshot_stmt        // EXTEND WITH HELP: SHOW SNAPSHOT
| show_flashback_stmt       // EXTEND WITH HELP: SHOW FLASHBACK
| show_cursors_stmt         // EXTEND WITH HELP: SHOW CURSORS
| show_functions_stmt       // EXTEND WITH HELP: SHOW FUNCTIONS
| SHOW error                // SHOW HELP: SHOW

// %Help: SHOW SESSION - display session variables
// %Category: Cfg
// %Text: SHOW [SESSION] { <var> | ALL }
// %SeeAlso: WEBDOCS/show-vars.html
show_session_stmt:
  SHOW session_var         { $$.val = &tree.ShowVar{Name: $2} }
| SHOW SESSION session_var { $$.val = &tree.ShowVar{Name: $3} }
| SHOW SESSION error // SHOW HELP: SHOW SESSION

session_var:
  IDENT
// Although ALL, SESSION_USER and DATABASE are identifiers for the
// purpose of SHOW, they lex as separate token types, so they need
// separate rules.
| ALL
| DATABASE
// SET NAMES is standard SQL for SET client_encoding.
// See https://www.postgresql.org/docs/9.6/static/multibyte.html#AEN39236
| NAMES { $$ = "client_encoding" }
| SESSION_USER
// TIME ZONE is special: it is two tokens, but is really the identifier "TIME ZONE".
| TIME ZONE { $$ = "timezone" }
| SEARCH_PATH { $$ = "search_path"}
| TIME error // SHOW HELP: SHOW SESSION

// %Help: SHOW STATISTICS - display table statistics (experimental)
// %Category: Experimental
// %Text: SHOW STATISTICS [USING JSON] FOR TABLE <table_name>
//
// Returns the available statistics for a table.
// The statistics can include a histogram ID, which can
// be used with SHOW HISTOGRAM.
// If USING JSON is specified, the statistics and histograms
// are encoded in JSON format.
// %SeeAlso: SHOW HISTOGRAM
show_stats_stmt:
  SHOW STATISTICS FOR TABLE table_name
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowTableStats{Table: name}
  }
| SHOW STATISTICS USING JSON FOR TABLE table_name
  {
    /* SKIP DOC */
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowTableStats{Table: name, UsingJSON: true}
  }
| SHOW STATISTICS error // SHOW HELP: SHOW STATISTICS

// %Help: SHOW HISTOGRAM - display histogram (experimental)
// %Category: Experimental
// %Text: SHOW HISTOGRAM <histogram_id>
//
// Returns the data in the histogram with the
// given ID (as returned by SHOW STATISTICS).
// %SeeAlso: SHOW STATISTICS
show_histogram_stmt:
  SHOW HISTOGRAM ICONST
  {
    /* SKIP DOC */
    id, err := $3.numVal().AsInt64()
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = &tree.ShowHistogram{HistogramID: id}
  }
| SHOW HISTOGRAM error // SHOW HELP: SHOW HISTOGRAM

// %Help: SHOW DUMP - list backup contents
// %Category: ICL
// %Text: SHOW DUMP [FILES|RANGES] <location>

// %SeeAlso: WEBDOCS/show-backup.html
show_backup_stmt:
  SHOW DUMP string_or_placeholder
  {
    $$.val = &tree.ShowBackup{
      Details: tree.BackupDefaultDetails,
      Path:    $3.expr(),
    }
  }
| SHOW DUMP RANGES string_or_placeholder
  {
    /* SKIP DOC */
    $$.val = &tree.ShowBackup{
      Details: tree.BackupRangeDetails,
      Path:    $4.expr(),
    }
  }
| SHOW DUMP FILES string_or_placeholder
  {
    /* SKIP DOC */
    $$.val = &tree.ShowBackup{
      Details: tree.BackupFileDetails,
      Path:    $4.expr(),
    }
  }
| SHOW DUMP error // SHOW HELP: SHOW DUMP

// %Help: SHOW LOAD_KAFKA - list kafka contents
// %Category: ICL
// %Text: SHOW LOAD_KAFKA  GroupID <group_id> FROM <url>

// %SeeAlso: WEBDOCS/show-kafka.html
show_kafka_stmt:
  SHOW LOAD_KAFKA  GROUPID string_or_placeholder FROM string_or_placeholder
  {
      $$.val = &tree.ShowKafka{
      GroupID: $4.expr(),
      URL: $6.expr(),
      }
  }
| SHOW LOAD_KAFKA error // SHOW HELP: SHOW LOAD_KAFKA

// %Help: SHOW CLUSTER SETTING - display cluster settings
// %Category: Cfg
// %Text:
// SHOW CLUSTER SETTING <var>
// SHOW ALL CLUSTER SETTINGS
// %SeeAlso: WEBDOCS/cluster-settings.html
show_csettings_stmt:
  SHOW CLUSTER SETTING var_name
  {
    $$.val = &tree.ShowClusterSetting{Name: strings.Join($4.strs(), ".")}
  }
| SHOW CLUSTER SETTING ALL
  {
    $$.val = &tree.ShowClusterSettingList{}
  }
| SHOW CLUSTER error // SHOW HELP: SHOW CLUSTER SETTING
| SHOW ALL CLUSTER SETTINGS
  {
    $$.val = &tree.ShowClusterSettingList{}
  }
| SHOW ALL CLUSTER error // SHOW HELP: SHOW CLUSTER SETTING

// %Help: SHOW COLUMNS - list columns in relation
// %Category: DDL
// %Text: SHOW COLUMNS FROM <tablename>
// %SeeAlso: WEBDOCS/show-columns.html
show_columns_stmt:
  SHOW COLUMNS FROM table_name with_comment
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowColumns{Table: name, WithComment: $5.bool()}
  }
| SHOW COLUMNS error // SHOW HELP: SHOW COLUMNS

// %Help: SHOW DATABASES - list databases
// %Category: DDL
// %Text: SHOW DATABASES
// %SeeAlso: WEBDOCS/show-databases.html
show_databases_stmt:
  SHOW DATABASES with_comment
  {
    $$.val = &tree.ShowDatabases{WithComment: $3.bool()}
  }
| SHOW DATABASES error // SHOW HELP: SHOW DATABASES

// %Help: SHOW GRANTS - list grants
// %Category: Priv
// %Text:
// Show privilege grants:
//   SHOW GRANTS [ON <targets...>] [FOR <users...>]
// Show role grants:
//   SHOW GRANTS ON ROLE [<roles...>] [FOR <grantees...>]
//
// %SeeAlso: WEBDOCS/show-grants.html
show_grants_stmt:
  SHOW GRANTS opt_on_targets_roles for_grantee_clause
  {
    lst := $3.targetListPtr()
    if lst != nil && lst.ForRoles {
      $$.val = &tree.ShowRoleGrants{Roles: lst.Roles, Grantees: $4.nameList()}
    } else {
      $$.val = &tree.ShowGrants{Targets: lst, Grantees: $4.nameList()}
    }
  }
| SHOW GRANTS error // SHOW HELP: SHOW GRANTS

// %Help: SHOW INDEXES - list indexes
// %Category: DDL
// %Text: SHOW INDEXES FROM <tablename> [WITH COMMENT]
// %SeeAlso: WEBDOCS/show-index.html
show_indexes_stmt:
  SHOW INDEX FROM table_name with_comment
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowIndexes{Table: name, WithComment: $5.bool()}
  }
| SHOW INDEX table_name '@' index_name with_comment
  {
    name := $3.unresolvedObjectName().ToTableName()
    index := tree.UnrestrictedName($5)
    $$.val = &tree.ShowIndexes{Table: name, Index: index, WithComment: $6.bool()}
  }
| SHOW INDEX error // SHOW HELP: SHOW INDEXES
| SHOW INDEXES FROM table_name with_comment
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowIndexes{Table: name, WithComment: $5.bool()}
  }
| SHOW INDEXES error // SHOW HELP: SHOW INDEXES
| SHOW KEYS FROM table_name with_comment
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowIndexes{Table: name, WithComment: $5.bool()}
  }
| SHOW KEYS error // SHOW HELP: SHOW INDEXES

// %Help: SHOW CONSTRAINTS - list constraints
// %Category: DDL
// %Text: SHOW CONSTRAINTS FROM <tablename>
// %SeeAlso: WEBDOCS/show-constraints.html
show_constraints_stmt:
  SHOW CONSTRAINT FROM table_name with_comment
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowConstraints{Table: name, WithComment: $5.bool()}
  }
| SHOW CONSTRAINT error // SHOW HELP: SHOW CONSTRAINTS
| SHOW CONSTRAINTS FROM table_name with_comment
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowConstraints{Table: name, WithComment: $5.bool()}
  }
| SHOW CONSTRAINTS error // SHOW HELP: SHOW CONSTRAINTS

// %Help: SHOW QUERIES - list running queries
// %Category: Misc
// %Text: SHOW [ALL] [CLUSTER | LOCAL] QUERIES
// %SeeAlso: CANCEL QUERIES
show_queries_stmt:
  SHOW opt_cluster QUERIES
  {
    $$.val = &tree.ShowQueries{All: false, Cluster: $2.bool()}
  }
| SHOW opt_cluster QUERIES error // SHOW HELP: SHOW QUERIES
| SHOW ALL opt_cluster QUERIES
  {
    $$.val = &tree.ShowQueries{All: true, Cluster: $3.bool()}
  }
| SHOW ALL opt_cluster QUERIES error // SHOW HELP: SHOW QUERIES

opt_cluster:
  /* EMPTY */
  { $$.val = true }
| CLUSTER
  { $$.val = true }
| LOCAL
  { $$.val = false }

// %Help: SHOW JOBS - list background jobs
// %Category: Misc
// %Text: SHOW [AUTOMATIC] JOBS
// %SeeAlso: CANCEL JOBS, PAUSE JOBS, RESUME JOBS
show_jobs_stmt:
  SHOW opt_automatic JOBS
  {
    $$.val = &tree.ShowJobs{Automatic: $2.bool()}
  }
| SHOW opt_automatic JOBS error // SHOW HELP: SHOW JOBS

// %Help: SHOW SCHEDULES - list periodic schedules
// %Text:
// SHOW SCHEDULES
// SHOW SCHEDULE <schedule_id>
// %SeeAlso: DROP SCHEDULES
show_schedules_stmt:
  SHOW SCHEDULES
  {
    $$.val = &tree.ShowSchedules{}
  }
| SHOW SCHEDULES error // SHOW HELP: SHOW SCHEDULES
| SHOW SCHEDULE a_expr
  {
    $$.val = &tree.ShowSchedules{
      ScheduleID:  $3.expr(),
    }
  }
| SHOW SCHEDULE error  // SHOW HELP: SHOW SCHEDULES

opt_automatic:
  AUTOMATIC { $$.val = true }
| /* EMPTY */ { $$.val = false }

// %Help: SHOW TRACE - display an execution trace
// %Category: Misc
// %Text:
// SHOW [COMPACT] [KV] TRACE FOR SESSION
// %SeeAlso: EXPLAIN
show_trace_stmt:
  SHOW opt_compact TRACE FOR SESSION
  {
    $$.val = &tree.ShowTraceForSession{TraceType: tree.ShowTraceRaw, Compact: $2.bool()}
  }
| SHOW opt_compact TRACE error // SHOW HELP: SHOW TRACE
| SHOW opt_compact KV TRACE FOR SESSION
  {
    $$.val = &tree.ShowTraceForSession{TraceType: tree.ShowTraceKV, Compact: $2.bool()}
  }
| SHOW opt_compact KV error // SHOW HELP: SHOW TRACE
| SHOW opt_compact EXPERIMENTAL_REPLICA TRACE FOR SESSION
  {
    /* SKIP DOC */
    $$.val = &tree.ShowTraceForSession{TraceType: tree.ShowTraceReplica, Compact: $2.bool()}
  }
| SHOW opt_compact EXPERIMENTAL_REPLICA error // SHOW HELP: SHOW TRACE

opt_compact:
  COMPACT { $$.val = true }
| /* EMPTY */ { $$.val = false }

// %Help: SHOW SESSIONS - list open client sessions
// %Category: Misc
// %Text: SHOW [ALL] [CLUSTER | LOCAL] SESSIONS
// %SeeAlso: CANCEL SESSIONS
show_sessions_stmt:
  SHOW opt_cluster SESSIONS
  {
    $$.val = &tree.ShowSessions{Cluster: $2.bool()}
  }
| SHOW opt_cluster SESSIONS error // SHOW HELP: SHOW SESSIONS
| SHOW ALL opt_cluster SESSIONS
  {
    $$.val = &tree.ShowSessions{All: true, Cluster: $3.bool()}
  }
| SHOW ALL opt_cluster SESSIONS error // SHOW HELP: SHOW SESSIONS

// %Help: SHOW TABLES - list tables
// %Category: DDL
// %Text: SHOW TABLES [FROM <databasename> [ . <schemaname> ] ] [WITH COMMENT]
// %SeeAlso: WEBDOCS/show-tables.html
show_tables_stmt:
  SHOW TABLES FROM name '.' name with_comment
  {
    $$.val = &tree.ShowTables{TableNamePrefix:tree.TableNamePrefix{
        CatalogName: tree.Name($4),
        ExplicitCatalog: true,
        SchemaName: tree.Name($6),
        ExplicitSchema: true,
    },
    WithComment: $7.bool()}
  }
| SHOW TABLES FROM name with_comment
  {
    $$.val = &tree.ShowTables{TableNamePrefix:tree.TableNamePrefix{
        // Note: the schema name may be interpreted as database name,
        // see name_resolution.go.
        SchemaName: tree.Name($4),
        ExplicitSchema: true,
    },
    WithComment: $5.bool()}
  }
| SHOW TABLES with_comment
  {
    $$.val = &tree.ShowTables{WithComment: $3.bool()}
  }
| SHOW TABLES error // SHOW HELP: SHOW TABLES

with_comment:
  WITH COMMENT { $$.val = true }
| /* EMPTY */  { $$.val = false }

// %Help: SHOW SCHEMAS - list schemas
// %Category: DDL
// %Text: SHOW SCHEMAS [FROM <databasename> ]
// %SeeAlso: WEBDOCS/show-schemas.html
show_schemas_stmt:
  SHOW SCHEMAS FROM name
  {
    $$.val = &tree.ShowSchemas{Database: tree.Name($4)}
  }
| SHOW SCHEMAS
  {
    $$.val = &tree.ShowSchemas{}
  }
| SHOW SCHEMAS error // SHOW HELP: SHOW SCHEMAS

// %Help: SHOW SEQUENCES - list sequences
// %Category: DDL
// %Text: SHOW SEQUENCES [FROM <databasename> ]
show_sequences_stmt:
  SHOW SEQUENCES FROM name
  {
    $$.val = &tree.ShowSequences{Database: tree.Name($4)}
  }
| SHOW SEQUENCES
  {
    $$.val = &tree.ShowSequences{}
  }
| SHOW SEQUENCES error // SHOW HELP: SHOW SEQUENCES

// %Help: SHOW SYNTAX - analyze SQL syntax
// %Category: Misc
// %Text: SHOW SYNTAX <string>
show_syntax_stmt:
  SHOW SYNTAX SCONST
  {
    /* SKIP DOC */
    $$.val = &tree.ShowSyntax{Statement: $3}
  }
| SHOW SYNTAX error // SHOW HELP: SHOW SYNTAX

// %Help: SHOW SAVEPOINT - display current savepoint properties
// %Category: Cfg
// %Text: SHOW SAVEPOINT STATUS
show_savepoint_stmt:
  SHOW SAVEPOINT STATUS
  {
    /* SKIP DOC */
    $$.val = &tree.ShowSavepointStatus{}
  }
| SHOW SAVEPOINT error // SHOW HELP: SHOW SAVEPOINT

// %Help: SHOW TRANSACTION - display current transaction properties
// %Category: Cfg
// %Text: SHOW TRANSACTION {ISOLATION LEVEL | PRIORITY | STATUS}
// %SeeAlso: WEBDOCS/show-transaction.html
show_transaction_stmt:
  SHOW TRANSACTION ISOLATION LEVEL
  {
    /* SKIP DOC */
    $$.val = &tree.ShowVar{Name: "transaction_isolation"}
  }
| SHOW TRANSACTION PRIORITY
  {
    /* SKIP DOC */
    $$.val = &tree.ShowVar{Name: "transaction_priority"}
  }
| SHOW TRANSACTION STATUS
  {
    /* SKIP DOC */
    $$.val = &tree.ShowTransactionStatus{}
  }
| SHOW TRANSACTION error // SHOW HELP: SHOW TRANSACTION

// %Help: SHOW CREATE - display the CREATE statement for a table, sequence or view
// %Category: DDL
// %Text: SHOW CREATE [ TABLE | SEQUENCE | VIEW ] <tablename>
// %SeeAlso: WEBDOCS/show-create-table.html
show_create_stmt:
  SHOW CREATE table_name with_cache
  {
    name := $3.unresolvedObjectName().ToTableName()
    withLocation := $4.bool()
    $$.val = &tree.ShowCreate{Name: name, WithLocation: withLocation}
  }
| SHOW CREATE create_kw table_name with_cache
  {
    /* SKIP DOC */
    name := $4.unresolvedObjectName().ToTableName()
    withLocation := $5.bool()
    showtype := $3.strPtr()
    $$.val = &tree.ShowCreate{Name: name, WithLocation: withLocation, ShowType: showtype}
  }
| SHOW CREATE error // SHOW HELP: SHOW CREATE

// %Help: SHOW CREATE TRIGGER - display the CREATE statement for a trigger
// %Category: DDL
// %Text: SHOW CREATE TRIGGER <triggername> ON <tablename>
// %SeeAlso: WEBDOCS/show-create-trigger.html
show_create_trigger_stmt:
  SHOW CREATE TRIGGER name ON table_name
  {
    Trigname := tree.Name($4)
    name := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowCreateTrigger{Name: Trigname,TableName: name}
  }
| SHOW CREATE TRIGGER error // SHOW HELP: SHOW CREATE TRIGGER

create_kw:
  TABLE  {
             t := $1
             $$.val = &t
           }
| VIEW {
           t := $1
           $$.val = &t
         }
| SEQUENCE {
               t := $1
               $$.val = &t
             }

// geo-partition feature add by jiye
with_cache:
	WITH CACHE
	{
		$$.val = true
	}
| {
	/* empty*/
		$$.val = false
	}

// %Help: SHOW TRIGGERS - list triggers
// %Category: DDL
// %Text: SHOW TRIGGERS [ON <table_name>]
// %SeeAlso: WEBDOCS/show-triggers.html
show_triggers_stmt:
  SHOW TRIGGERS
  {
    $$.val = &tree.ShowTriggers{}
  }
| SHOW TRIGGERS ON table_name
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowTriggersOnTable{Tablename:name}
  }
| SHOW TRIGGERS error // SHOW HELP: SHOW TRIGGERS

// %Help: SHOW USERS - list defined users
// %Category: Priv
// %Text: SHOW USERS
// %SeeAlso: CREATE USER, DROP USER, WEBDOCS/show-users.html
show_users_stmt:
  SHOW USERS
  {
    $$.val = &tree.ShowUsers{}
  }
| SHOW USERS error // SHOW HELP: SHOW USERS

// %Help: SHOW ROLES - list defined roles
// %Category: Priv
// %Text: SHOW ROLES
// %SeeAlso: CREATE ROLE, DROP ROLE
show_roles_stmt:
  SHOW ROLES
  {
    $$.val = &tree.ShowRoles{}
  }
| SHOW ROLES error // SHOW HELP: SHOW ROLES

// %Help: SHOW SPACES - list node spaces
// %Category: Priv
// %Text: SHOW SPACES
// %SeeAlso: SHOW SPACES
show_spaces_stmt:
  SHOW SPACES
  {
    $$.val = &tree.ShowSpaces{}
  }
| SHOW SPACES error // SHOW HELP: SHOW SPACES

show_zone_stmt:
  SHOW ZONE CONFIGURATION FOR RANGE zone_name
  {
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{NamedZone: tree.UnrestrictedName($6)}}
  }
| SHOW ZONE CONFIGURATION FOR DATABASE database_name
  {
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{Database: tree.Name($6)}}
  }
| SHOW ZONE CONFIGURATION FOR TABLE table_name opt_partition
  {
    name := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{
        TableOrIndex: tree.TableIndexName{Table: name},
    }}
  }
| SHOW ZONE CONFIGURATION FOR PARTITION partition_name OF TABLE table_name
  {
    name := $9.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{
      TableOrIndex: tree.TableIndexName{Table: name},
        Partition: tree.Name($6),
    }}
  }
| SHOW ZONE CONFIGURATION FOR PARTITION partition_name OF INDEX table_index_name
  {
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{
          TableOrIndex: $9.tableIndexName(),
           Partition: tree.Name($6),
        }}
  }
| SHOW ZONE CONFIGURATION FOR INDEX table_index_name
  {
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{
      TableOrIndex: $6.tableIndexName(),
    }}
  }
| SHOW ZONE CONFIGURATIONS
  {
    $$.val = &tree.ShowZoneConfig{}
  }
| SHOW ALL ZONE CONFIGURATIONS
  {
    $$.val = &tree.ShowZoneConfig{}
  }

// %Help: SHOW RANGES - list ranges
// %Category: Misc
// %Text:
// SHOW EXPERIMENTAL_RANGES FROM TABLE <tablename>
// SHOW EXPERIMENTAL_RANGES FROM INDEX [ <tablename> @ ] <indexname>
show_ranges_stmt:
  SHOW ranges_kw FROM TABLE table_name
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowRanges{Table: &name}
  }
| SHOW ranges_kw FROM INDEX table_index_name
  {
    $$.val = &tree.ShowRanges{Index: $5.newTableIndexName()}
  }
| SHOW ranges_kw error // SHOW HELP: SHOW RANGES

ranges_kw:
  TESTING_RANGES
| EXPERIMENTAL_RANGES

show_fingerprints_stmt:
  SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE table_name
  {
    /* SKIP DOC */
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowFingerprints{Table: name}
  }

opt_on_targets_roles:
  ON targets_roles
  {
    tmp := $2.targetList()
    $$.val = &tmp
  }
| /* EMPTY */
  {
    $$.val = (*tree.TargetList)(nil)
  }

// targets is a non-terminal for a list of privilege targets, either a
// list of databases or a list of tables.
//
// This rule is complex and cannot be decomposed as a tree of
// non-terminals because it must resolve syntax ambiguities in the
// SHOW GRANTS ON ROLE statement. It was constructed as follows.
//
// 1. Start with the desired definition of targets:
//
//    targets ::=
//        table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 2. Now we must disambiguate the first rule "table_pattern_list"
//    between one that recognizes ROLE and one that recognizes
//    <some table pattern list>". So first, inline the definition of
//    table_pattern_list.
//
//    targets ::=
//        table_pattern                          # <- here
//        table_pattern_list ',' table_pattern   # <- here
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 3. We now must disambiguate the "ROLE" inside the prefix "table_pattern".
//    However having "table_pattern_list" as prefix is cumbersome, so swap it.
//
//    targets ::=
//        table_pattern
//        table_pattern ',' table_pattern_list   # <- here
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 4. The rule that has table_pattern followed by a comma is now
//    non-problematic, because it will never match "ROLE" followed
//    by an optional name list (neither "ROLE;" nor "ROLE <ident>"
//    would match). We just need to focus on the first one "table_pattern".
//    This needs to tweak "table_pattern".
//
//    Here we could inline table_pattern but now we don't have to any
//    more, we just need to create a variant of it which is
//    unambiguous with a single ROLE keyword. That is, we need a
//    table_pattern which cannot contain a single name. We do
//    this as follows.
//
//    targets ::=
//        complex_table_pattern                  # <- here
//        table_pattern ',' table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//    complex_table_pattern ::=
//        name '.' unrestricted_name
//        name '.' unrestricted_name '.' unrestricted_name
//        name '.' unrestricted_name '.' '*'
//        name '.' '*'
//        '*'
//
// 5. At this point the rule cannot start with a simple identifier any
//    more, keyword or not. But more importantly, any token sequence
//    that starts with ROLE cannot be matched by any of these remaining
//    rules. This means that the prefix is now free to use, without
//    ambiguity. We do this as follows, to gain a syntax rule for "ROLE
//    <namelist>". (We'll handle a ROLE with no name list below.)
//
//    targets ::=
//        ROLE name_list                        # <- here
//        complex_table_pattern
//        table_pattern ',' table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 6. Now on to the finishing touches. First we'd like to regain the
//    ability to use "<tablename>" when the table name is a simple
//    identifier. This is done as follows:
//
//    targets ::=
//        ROLE name_list
//        name                                  # <- here
//        complex_table_pattern
//        table_pattern ',' table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 7. Then, we want to recognize "ROLE" without any subsequent name
//    list. This requires some care: we can't add "ROLE" to the set of
//    rules above, because "name" would then overlap. To disambiguate,
//    we must first inline "name" as follows:
//
//    targets ::=
//        ROLE name_list
//        IDENT                    # <- here, always <table>
//        col_name_keyword         # <- here, always <table>
//        unreserved_keyword       # <- here, either ROLE or <table>
//        complex_table_pattern
//        table_pattern ',' table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 8. And now the rule is sufficiently simple that we can disambiguate
//    in the action, like this:
//
//    targets ::=
//        ...
//        unreserved_keyword {
//             if $1 == "role" { /* handle ROLE */ }
//             else { /* handle ON <tablename> */ }
//        }
//        ...
//
//   (but see the comment on the action of this sub-rule below for
//   more nuance.)
//
// Tada!
targets:
  IDENT
  {
    $$.val = tree.TargetList{Tables: tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}}}
  }
| col_name_keyword
  {
    $$.val = tree.TargetList{Tables: tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}}}
  }
| unreserved_keyword
  {
    // This sub-rule is meant to support both ROLE and other keywords
    // used as table name without the TABLE prefix. The keyword ROLE
    // here can have two meanings:
    //
    // - for all statements except SHOW GRANTS, it must be interpreted
    //   as a plain table name.
    // - for SHOW GRANTS specifically, it must be handled as an ON ROLE
    //   specifier without a name list (the rule with a name list is separate,
    //   see above).
    //
    // Yet we want to use a single "targets" non-terminal for all
    // statements that use targets, to share the code. This action
    // achieves this as follows:
    //
    // - for all statements (including SHOW GRANTS), it populates the
    //   Tables list in TargetList{} with the given name. This will
    //   include the given keyword as table pattern in all cases,
    //   including when the keyword was ROLE.
    //
    // - if ROLE was specified, it remembers this fact in the ForRoles
    //   field. This distinguishes `ON ROLE` (where "role" is
    //   specified as keyword), which triggers the special case in
    //   SHOW GRANTS, from `ON "role"` (where "role" is specified as
    //   identifier), which is always handled as a table name.
    //
    //   Both `ON ROLE` and `ON "role"` populate the Tables list in the same way,
    //   so that other statements than SHOW GRANTS don't observe any difference.
    //
    // Arguably this code is a bit too clever. Future work should aim
    // to remove the special casing of SHOW GRANTS altogether instead
    // of increasing (or attempting to modify) the grey magic occurring
    // here.
    $$.val = tree.TargetList{
      Tables: tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}},
      ForRoles: $1 == "role", // backdoor for "SHOW GRANTS ON ROLE" (no name list)
    }
  }
| complex_table_pattern
  {
    $$.val = tree.TargetList{Tables: tree.TablePatterns{$1.unresolvedName()}}
  }
| table_pattern ',' table_pattern_list
  {
    remainderPats := $3.tablePatterns()
    $$.val = tree.TargetList{Tables: append(tree.TablePatterns{$1.unresolvedName()}, remainderPats...)}
  }
| VIEW table_pattern_list
  {
    $$.val = tree.TargetList{Views: $2.tablePatterns()}
  }
| SEQUENCE table_pattern_list
  {
    $$.val = tree.TargetList{Sequences: $2.tablePatterns()}
  }
| TABLE table_pattern_list
  {
    $$.val = tree.TargetList{Tables: $2.tablePatterns()}
  }
| DATABASE name_list
  {
    $$.val = tree.TargetList{Databases: $2.nameList()}
  }
| SCHEMA schema_name_list
  {
    $$.val = tree.TargetList{Schemas: $2.schemaNames()}
  }
| FUNCTION table_name func_args_with_defaults
  {
    name := $2.unresolvedObjectName().ToTableName()
    function := tree.Function{FuncName: name, Parameters: $3.functionParameters(), IsProcedure: false}
    $$.val = tree.TargetList{Function: function}
  }
| PROCEDURE table_name func_args_with_defaults
  {
    name := $2.unresolvedObjectName().ToTableName()
    function := tree.Function{FuncName: name, Parameters: $3.functionParameters(), IsProcedure: true}
    $$.val = tree.TargetList{Function: function}
  }

// target_roles is the variant of targets which recognizes ON ROLES
// with a name list. This cannot be included in targets directly
// because some statements must not recognize this syntax.
targets_roles:
  ROLE name_list
  {
     $$.val = tree.TargetList{ForRoles: true, Roles: $2.nameList()}
  }
| targets

for_grantee_clause:
  FOR name_list
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

// %Help: DROP SCHEDULES - destroy specified schedules
// %Category: Misc
// %Text:
// DROP SCHEDULES <selectclause>
//  selectclause: select statement returning schedule IDs to resume.
//
// DROP SCHEDULE <jobid>
// %SeeAlso: SHOW SCHEDULES
drop_schedule_stmt:
  DROP SCHEDULE a_expr
  {
    $$.val = &tree.ControlSchedules{
      Schedules: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.DropSchedule,
    }
  }
| DROP SCHEDULE error // SHOW HELP: DROP SCHEDULES
| DROP SCHEDULES select_stmt
  {
    $$.val = &tree.ControlSchedules{
      Schedules: $3.slct(),
      Command: tree.DropSchedule,
    }
  }
| DROP SCHEDULES error // SHOW HELP: DROP SCHEDULES

// %Help: PAUSE JOBS - pause background jobs
// %Category: Misc
// %Text:
// PAUSE JOBS <selectclause>
// PAUSE JOB <jobid>
// %SeeAlso: SHOW JOBS, CANCEL JOBS, RESUME JOBS
pause_stmt:
  PAUSE JOB a_expr
  {
    $$.val = &tree.ControlJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.PauseJob,
    }
  }
| PAUSE JOBS select_stmt
  {
    $$.val = &tree.ControlJobs{Jobs: $3.slct(), Command: tree.PauseJob}
  }
| PAUSE error // SHOW HELP: PAUSE JOBS

// %Help: CREATE TABLE - create a new table
// %Category: DDL
// %Text:
// CREATE TABLE [IF NOT EXISTS] <tablename> ( <elements...> ) [<interleave>]
// CREATE TABLE [IF NOT EXISTS] <tablename> [( <colnames...> )] AS <source>
//
// Table elements:
//    <name> <type> [<qualifiers...>]
//    [UNIQUE | INVERTED] INDEX [<name>] ( <colname> [ASC | DESC] [, ...] )
//                            [STORING ( <colnames...> )] [<interleave>]
//    FAMILY [<name>] ( <colnames...> )
//    [CONSTRAINT <name>] <constraint>
//
// Table constraints:
//    PRIMARY KEY ( <colnames...> )
//    FOREIGN KEY ( <colnames...> ) REFERENCES <tablename> [( <colnames...> )] [ON DELETE {NO ACTION | RESTRICT}] [ON UPDATE {NO ACTION | RESTRICT}]
//    UNIQUE ( <colnames... ) [STORING ( <colnames...> )] [<interleave>]
//    CHECK ( <expr> )
//
// Column qualifiers:
//   [CONSTRAINT <constraintname>] {NULL | NOT NULL | UNIQUE | PRIMARY KEY | CHECK (<expr>) | DEFAULT <expr>}
//   FAMILY <familyname>, CREATE [IF NOT EXISTS] FAMILY [<familyname>]
//   REFERENCES <tablename> [( <colnames...> )] [ON DELETE {NO ACTION | RESTRICT}] [ON UPDATE {NO ACTION | RESTRICT}]
//   COLLATE <collationname>
//   AS ( <expr> ) STORED
//
// Interleave clause:
//    INTERLEAVE IN PARENT <tablename> ( <colnames...> ) [CASCADE | RESTRICT]
//
// %SeeAlso: SHOW TABLES, CREATE VIEW, SHOW CREATE,
// WEBDOCS/create-table.html
// WEBDOCS/create-table-as.html
create_table_stmt:
  CREATE opt_temp TABLE table_name '(' opt_table_elem_list ')' opt_inherits opt_interleave opt_partition_by opt_table_with opt_locate_in
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTable{
      Table: name,
      IfNotExists: false,
      Temporary: $2.persistenceType(),
      InhRelations: $8.tableNames(),
      Interleave: $9.interleave(),
      Defs: $6.tblDefs(),
      AsSource: nil,
      AsColumnNames: nil,
      PartitionBy: $10.partitionBy(),
      LocateSpaceName: $12.location(),
    }
  }
| CREATE opt_temp TABLE IF NOT EXISTS table_name '(' opt_table_elem_list ')' opt_inherits opt_interleave opt_partition_by opt_table_with opt_locate_in
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTable{
      Table: name,
      IfNotExists: true,
      Temporary: $2.persistenceType(),
      InhRelations: $11.tableNames(),
      Interleave: $12.interleave(),
      Defs: $9.tblDefs(),
      AsSource: nil,
      AsColumnNames: nil,
      PartitionBy: $13.partitionBy(),
      LocateSpaceName: $15.location(),
    }
  }
|  CREATE opt_temp TABLE IF NOT EXISTS table_name LIKE  table_name
    {
        Liketbl:= &tree.LikeTableDef{
       		Name: $9.unresolvedObjectName().ToTableName(),
       		Options: nil,
       	}
       	Liketbls:=[]tree.TableDef{Liketbl}
      name := $7.unresolvedObjectName().ToTableName()
      $$.val = &tree.CreateTable{
        Table: name,
        IfNotExists: false,
        Temporary: $2.persistenceType(),
        InhRelations: nil,
        Interleave: nil,
        Defs:Liketbls,
        AsSource: nil,
        AsColumnNames: nil,
        PartitionBy: nil,
        LocateSpaceName: nil,
        IsLike: true,
      }
    }
 |   CREATE opt_temp TABLE table_name LIKE  table_name
        {
            Liketbl:= &tree.LikeTableDef{
           		Name: $6.unresolvedObjectName().ToTableName(),
           		Options: nil,
           	}
           	Liketbls:=[]tree.TableDef{Liketbl}
          name := $4.unresolvedObjectName().ToTableName()
          $$.val = &tree.CreateTable{
            Table: name,
            IfNotExists: false,
            Temporary: $2.persistenceType(),
            InhRelations: nil,
            Interleave: nil,
            Defs:Liketbls,
            AsSource: nil,
            AsColumnNames: nil,
            PartitionBy: nil,
            LocateSpaceName: nil,
             IsLike: true,
          }
        }
opt_inherits:
  /* EMPTY */     { $$.val = tree.TableNames(nil) }
| INHERITS '(' table_name_list ')'
 {
   $$.val = $3.tableNames()
 }

opt_table_with:
  /* EMPTY */     { /* no error */ }
| WITHOUT OIDS    { /* SKIP DOC */ /* this is also the default in ZNBaseDB */ }
| WITH name error { return unimplemented(sqllex, "create table with " + $2) }

// %Help: CREATE SCHEMA - create a new schema
// %Category: DDL
// %Text: CREATE SCHEMA [IF NOT EXISTS] <schema_name>
// %SeeAlso: WEBDOCS/create-schema.html
create_schema_stmt:
  CREATE SCHEMA schema_name
  {
    $$.val = &tree.CreateSchema{
      Schema: $3.unresolvedObjectName().ToSchemaName(),
      IfNotExists: false,
    }
  }
| CREATE SCHEMA IF NOT EXISTS schema_name
  {
    $$.val = &tree.CreateSchema{
      Schema: $6.unresolvedObjectName().ToSchemaName(),
      IfNotExists: true,
    }
  }
| CREATE SCHEMA error // SHOW HELP: CREATE SCHEMA

create_table_as_stmt:
  CREATE opt_temp TABLE table_name opt_column_list opt_table_with opt_locate_in AS select_stmt opt_create_as_data
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTable{
      Table: name,
      IfNotExists: false,
      Interleave: nil,
      Defs: nil,
      Temporary: $2.persistenceType(),
      AsSource: $9.slct(),
      AsColumnNames: $5.nameList(),
      LocateSpaceName: $7.location(),
    }
  }
| CREATE opt_temp TABLE IF NOT EXISTS table_name opt_column_list opt_table_with opt_locate_in AS select_stmt opt_create_as_data
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTable{
      Table: name,
      IfNotExists: true,
      Interleave: nil,
      Defs: nil,
      Temporary: $2.persistenceType(),
      AsSource: $12.slct(),
      AsColumnNames: $8.nameList(),
      LocateSpaceName: $10.location(),
    }
  }

opt_create_as_data:
  /* EMPTY */  { /* no error */ }
| WITH DATA    { /* SKIP DOC */ /* This is the default */ }
| WITH NO DATA { return unimplemented(sqllex, "create table as with no data") }

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTempTableName.
 *
 * NOTE: we accept both GLOBAL and LOCAL options.  They currently do nothing,
 * but future versions might consider GLOBAL to request SQL-spec-compliant
 * temp table behavior, so warn about that.  Since we have no modules the
 * LOCAL keyword is really meaningless; furthermore, some other products
 * implement LOCAL as meaning the same as our default temp table behavior,
 * so we'll probably continue to treat LOCAL as a noise word.
 */
opt_temp:
  TEMPORARY         { $$.val = true }
| TEMP              { $$.val = true  }
| LOCAL TEMPORARY   { $$.val = true  }
| LOCAL TEMP        { $$.val = true  }
| GLOBAL TEMPORARY  { $$.val = true  }
| GLOBAL TEMP       { $$.val = true  }
| UNLOGGED          { return unimplemented(sqllex, "create unlogged") }
| /*EMPTY*/         { $$.val = false }
opt_table_elem_list:
  table_elem_list
| /* EMPTY */
  {
    $$.val = tree.TableDefs(nil)
  }

table_elem_list:
  table_elem
  {
    $$.val = tree.TableDefs{$1.tblDef()}
  }
| table_elem_list ',' table_elem
  {
    $$.val = append($1.tblDefs(), $3.tblDef())
  }

table_elem:
  column_def
  {
    $$.val = $1.colDef()
  }
| index_def
| family_def
| table_constraint
  {
    $$.val = $1.constraintDef()
  }

opt_interleave:
  INTERLEAVE IN PARENT table_name '(' name_list ')' opt_interleave_drop_behavior
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.InterleaveDef{
      Parent: name,
      Fields: $6.nameList(),
      DropBehavior: $8.dropBehavior(),
    }
  }
| /* EMPTY */
  {
    $$.val = (*tree.InterleaveDef)(nil)
  }

// TODO(dan): This can be removed in favor of opt_drop_behavior when #7854 is fixed.
opt_interleave_drop_behavior:
  CASCADE
  {
    /* SKIP DOC */
    $$.val = tree.DropCascade
  }
| RESTRICT
  {
    /* SKIP DOC */
    $$.val = tree.DropRestrict
  }
| /* EMPTY */
  {
    $$.val = tree.DropDefault
  }

partition:
  PARTITION partition_name
  {
    $$ = $2
  }

opt_partition:
  partition
| /* EMPTY */
  {
    $$ = ""
  }

opt_partition_list:
  partition
  {
    $$.val = []string{$1}
  }
| opt_partition_list ',' partition
  {
    $$.val = append($1.strs(), $3)
  }

opt_partition_by:
  partition_by
| /* EMPTY */
  {
    $$.val = (*tree.PartitionBy)(nil)
  }

partition_by:
  PARTITION BY LIST '(' name_list ')' '(' list_partitions ')'
  {
    $$.val = &tree.PartitionBy{
      Fields: $5.nameList(),
      List: $8.listPartitions(),
    }
  }
| PARTITION BY RANGE '(' name_list ')' '(' range_partitions ')'
  {
    $$.val = &tree.PartitionBy{
      Fields: $5.nameList(),
      Range: $8.rangePartitions(),
    }
  }
| PARTITION BY HASH '(' column_name ')' '(' hash_partitions ')'
  {

  p := &tree.PartitionBy{
      Fields: tree.NameList{tree.Name($5)},
      List: $8.hashPartitions(),
      IsHash: true,
    }
    $$.val = p
  }
| PARTITION BY HASH '(' column_name ')' '('hash_partition_quantity')'
  {
    $$.val = &tree.PartitionBy{
      Fields: tree.NameList{tree.Name($5)},
      List: $8.hashPartitions(),
      IsHash: true,
      IsHashQuantity: true,
    }
  }

| PARTITION BY NOTHING
  {
    $$.val = (*tree.PartitionBy)(nil)
  }

list_partitions:
  list_partition
  {
    $$.val = []tree.ListPartition{$1.listPartition()}
  }
| list_partitions ',' list_partition
  {
    $$.val = append($1.listPartitions(), $3.listPartition())
  }

list_partition:
  partition VALUES IN '(' expr_list ')' opt_partition_by opt_locate_in
  {
    $$.val = tree.ListPartition{
      Name: tree.UnrestrictedName($1),
      LocateSpaceName: $8.location(),
      Exprs: $5.exprs(),
      Subpartition: $7.partitionBy(),
    }
  }

hash_partitions:
  hash_partition
  {
    $$.val = []tree.ListPartition{$1.hashPartition()}
  }
| hash_partitions ',' hash_partition
  {
    $$.val = append($1.hashPartitions(), $3.hashPartition())
  }

hash_partition_quantity:
  PARTITIONS ICONST opt_locate_ins
  {
    count, _ := $2.numVal().AsInt64()
    tmp := tree.ListPartition{
	LocateSpaceNames: $3.locationList(),
	HashParts: count,
    }
    $$.val = []tree.ListPartition{tmp}
  }

hash_partition:
  partition opt_locate_in
  {
    $$.val = tree.ListPartition{
      Name: tree.UnrestrictedName($1),
      LocateSpaceName: $2.location(),
      IsHash: true,
    }
  }

range_partitions:
  range_partition
  {
    $$.val = []tree.RangePartition{$1.rangePartition()}
  }
| range_partitions ',' range_partition
  {
    $$.val = append($1.rangePartitions(), $3.rangePartition())
  }

range_partition:
  partition VALUES FROM '(' expr_list ')' TO '(' expr_list ')' opt_partition_by opt_locate_in
  {
    $$.val = tree.RangePartition{
      Name: tree.UnrestrictedName($1),
      From: $5.exprs(),
      To: $9.exprs(),
      LocateSpaceName:	$12.location(),
      Subpartition: $11.partitionBy(),
    }
  }

/* geo-partition feature add by jiye */
opt_locate_in:
  LOCATE IN '(' name_list ')'
  {
    $$.val = &tree.Location{
      Locate: $4.nameList(),
    }
  }
| LOCATE IN '(' name_list ')' LEASE IN '(' name_list ')'
  {
    $$.val = &tree.Location{
      Locate: $4.nameList(),
      Lease: $9.nameList(),
    }
  }
| /* EMPTY */
  {
    $$.val = (*tree.Location)(nil)
  }

opt_locate_ins:
  LOCATE IN '(' location_list ')'
    {
      $$.val = $4.locationList()
    }
| /* EMPTY */
  {
    $$.val = tree.LocationList{}
  }

column_def:
  column_name typename col_qual_list
  {
    tableDef, err := tree.NewColumnTableDef(tree.Name($1), $2.colType(), $3.colQuals())
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = tableDef
  }

col_qual_list:
  col_qual_list col_qualification
  {
    $$.val = append($1.colQuals(), $2.colQual())
  }
| /* EMPTY */
  {
    $$.val = []tree.NamedColumnQualification(nil)
  }

col_qualification:
  CONSTRAINT constraint_name col_qualification_elem
  {
    $$.val = tree.NamedColumnQualification{Name: tree.Name($2), Qualification: $3.colQualElem()}
  }
| col_qualification_elem
  {
    $$.val = tree.NamedColumnQualification{Qualification: $1.colQualElem()}
  }
| COLLATE collation_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: tree.ColumnCollation($2)}
  }
| FAMILY family_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Family: tree.Name($2)}}
  }
| CREATE FAMILY family_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Family: tree.Name($3), Create: true}}
  }
| CREATE FAMILY
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Create: true}}
  }
| CREATE IF NOT EXISTS FAMILY family_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Family: tree.Name($6), Create: true, IfNotExists: true}}
  }

// DEFAULT NULL is already the default for Postgres. But define it here and
// carry it forward into the system to make it explicit.
// - thomas 1998-09-13
//
// WITH NULL and NULL are not SQL-standard syntax elements, so leave them
// out. Use DEFAULT NULL to explicitly indicate that a column may have that
// value. WITH NULL leads to shift/reduce conflicts with WITH TIME ZONE anyway.
// - thomas 1999-01-08
//
// DEFAULT expression must be b_expr not a_expr to prevent shift/reduce
// conflict on NOT (since NOT might start a subsequent NOT NULL constraint, or
// be part of a_expr NOT LIKE or similar constructs).
col_qualification_elem:
  NOT NULL
  {
    $$.val = tree.NotNullConstraint{}
  }
| NULL
  {
    $$.val = tree.NullConstraint{}
  }
| UNIQUE
  {
    $$.val = tree.UniqueConstraint{}
  }
| PRIMARY KEY
  {
    $$.val = tree.PrimaryKeyConstraint{}
  }
| CHECK '(' a_expr ')' opt_constraint_able
  {
    $$.val = &tree.ColumnCheckConstraint{
    	Expr: $3.expr(),
    	Able: $5.bool(),
    }
  }
| DEFAULT b_expr on_update_current_timestamp
  {
    $$.val = &tree.ColumnDefault{Expr: $2.expr(), OnUpdateCurrentTimeStamp: $3.bool()}
  }
| REFERENCES table_name opt_name_parens key_match reference_actions
 {
    name := $2.unresolvedObjectName().ToTableName()
    $$.val = &tree.ColumnFKConstraint{
      Table: name,
      Col: tree.Name($3),
      Actions: $5.referenceActions(),
      Match: $4.compositeKeyMatchMethod(),
    }
 }
| AS '(' a_expr ')' STORED
 {
    $$.val = &tree.ColumnComputedDef{Expr: $3.expr()}
 }
| AS '(' a_expr ')' VIRTUAL
 {
    return unimplemented(sqllex, "virtual computed columns")
 }
| AS error
 {
    sqllex.Error("syntax error: use AS ( <expr> ) STORED")
    return 1
 }

on_update_current_timestamp:
{
    /* empty*/
    $$.val = false
}
| ON UPDATE CURRENT_TIMESTAMP
{
    $$.val = true
}

index_def:
  INDEX opt_index_name '(' index_params ')' opt_storing opt_interleave opt_partition_by opt_idx_where opt_locate_in
  {
    $$.val = &tree.IndexTableDef{
      Name:    tree.Name($2),
      Columns: $4.idxElems(),
      Storing: $6.nameList(),
      Interleave: $7.interleave(),
      PartitionBy: $8.partitionBy(),
      LocateSpaceName :	$10.location(),
      Where: tree.NewWhere(tree.AstWhere, $9.expr()),
    }
  }
| UNIQUE INDEX opt_index_name '(' index_params ')' opt_storing opt_interleave opt_partition_by opt_idx_where opt_locate_in
  {
    $$.val = &tree.UniqueConstraintTableDef{
      IndexTableDef: tree.IndexTableDef {
        Name:    tree.Name($3),
        Columns: $5.idxElems(),
        Storing: $7.nameList(),
        Interleave: $8.interleave(),
        PartitionBy: $9.partitionBy(),
        LocateSpaceName :	$11.location(),
        Where: tree.NewWhere(tree.AstWhere, $10.expr()),
      },
    }
  }
| INVERTED INDEX opt_name '(' index_params ')'
  {
    $$.val = &tree.IndexTableDef{
      Name:    tree.Name($3),
      Columns: $5.idxElems(),
      Inverted: true,
    }
  }

family_def:
  FAMILY opt_family_name '(' name_list ')'
  {
    $$.val = &tree.FamilyTableDef{
      Name: tree.Name($2),
      Columns: $4.nameList(),
    }
  }

// constraint_elem specifies constraint syntax which is not embedded into a
// column definition. col_qualification_elem specifies the embedded form.
// - thomas 1997-12-03
// geo-partition feature add by jiye
table_constraint:
  CONSTRAINT constraint_name constraint_elem opt_locate_in
  {
    $$.val = $3.constraintDef()
    $$.val.(tree.ConstraintTableDef).SetName(tree.Name($2))
		$$.val.(tree.ConstraintTableDef).SetLocateSpaceName($4.location())
  }
| constraint_elem opt_locate_in
  {
    $$.val = $1.constraintDef()
    $$.val.(tree.ConstraintTableDef).SetLocateSpaceName($2.location())
  }

constraint_elem:
  CHECK '(' a_expr ')' opt_deferrable opt_constraint_able
  {
    $$.val = &tree.CheckConstraintTableDef{
      Expr: $3.expr(),
      Able: $6.bool(),
      InhCount: 1,
    }
  }
| UNIQUE '(' index_params ')' opt_storing opt_interleave opt_partition_by  opt_deferrable
  {
    $$.val = &tree.UniqueConstraintTableDef{
      IndexTableDef: tree.IndexTableDef{
        Columns: $3.idxElems(),
        Storing: $5.nameList(),
        Interleave: $6.interleave(),
        PartitionBy: $7.partitionBy(),
      },
    }
  }
| PRIMARY KEY '(' index_params ')' opt_partition_by opt_interleave
  {
    $$.val = &tree.UniqueConstraintTableDef{
      IndexTableDef: tree.IndexTableDef{
        Columns: $4.idxElems(),
        PartitionBy: $6.partitionBy(),
        Interleave: $7.interleave(),
      },
      PrimaryKey:    true,
    }
  }
| FOREIGN KEY '(' name_list ')' REFERENCES table_name
    opt_column_list key_match reference_actions opt_deferrable
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.ForeignKeyConstraintTableDef{
      Table: name,
      FromCols: $4.nameList(),
      ToCols: $8.nameList(),
      Match: $9.compositeKeyMatchMethod(),
      Actions: $10.referenceActions(),
    }
  }

opt_deferrable:
  /* EMPTY */ { /* no error */ }
| DEFERRABLE { return unimplementedWithIssueDetail(sqllex, 31632, "deferrable") }
| DEFERRABLE INITIALLY DEFERRED { return unimplementedWithIssueDetail(sqllex, 31632, "def initially deferred") }
| DEFERRABLE INITIALLY IMMEDIATE { return unimplementedWithIssueDetail(sqllex, 31632, "def initially immediate") }
| INITIALLY DEFERRED { return unimplementedWithIssueDetail(sqllex, 31632, "initially deferred") }
| INITIALLY IMMEDIATE { return unimplementedWithIssueDetail(sqllex, 31632, "initially immediate") }

opt_constraint_able:
 /* EMPTY */ { $$.val=false}
| DISABLE
  {
   $$.val=true
   }
| ENABLE
  {
  $$.val=false
  }

storing:
  COVERING
| STORING

// TODO(pmattis): It would be nice to support a syntax like STORING
// ALL or STORING (*). The syntax addition is straightforward, but we
// need to be careful with the rest of the implementation. In
// particular, columns stored at indexes are currently encoded in such
// a way that adding a new column would require rewriting the existing
// index values. We will need to change the storage format so that it
// is a list of <columnID, value> pairs which will allow both adding
// and dropping columns without rewriting indexes that are storing the
// adjusted column.
opt_storing:
  storing '(' name_list ')'
  {
    $$.val = $3.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

opt_column_list:
  '(' name_list ')'
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

// https://www.postgresql.org/docs/10/sql-createtable.html
//
// "A value inserted into the referencing column(s) is matched against
// the values of the referenced table and referenced columns using the
// given match type. There are three match types: MATCH FULL, MATCH
// PARTIAL, and MATCH SIMPLE (which is the default). MATCH FULL will
// not allow one column of a multicolumn foreign key to be null unless
// all foreign key columns are null; if they are all null, the row is
// not required to have a match in the referenced table. MATCH SIMPLE
// allows any of the foreign key columns to be null; if any of them
// are null, the row is not required to have a match in the referenced
// table. MATCH PARTIAL is not yet implemented. (Of course, NOT NULL
// constraints can be applied to the referencing column(s) to prevent
// these cases from arising.)"
key_match:
  MATCH SIMPLE
  {
    $$.val = tree.MatchSimple
  }
| MATCH FULL
  {
    $$.val = tree.MatchFull
  }
| MATCH PARTIAL
  {
    return unimplementedWithIssueDetail(sqllex, 20305, "match partial")
  }
| /* EMPTY */
  {
    $$.val = tree.MatchSimple
  }

// We combine the update and delete actions into one value temporarily for
// simplicity of parsing, and then break them down again in the calling
// production.
reference_actions:
  reference_on_update
  {
     $$.val = tree.ReferenceActions{Update: $1.referenceAction()}
  }
| reference_on_delete
  {
     $$.val = tree.ReferenceActions{Delete: $1.referenceAction()}
  }
| reference_on_update reference_on_delete
  {
    $$.val = tree.ReferenceActions{Update: $1.referenceAction(), Delete: $2.referenceAction()}
  }
| reference_on_delete reference_on_update
  {
    $$.val = tree.ReferenceActions{Delete: $1.referenceAction(), Update: $2.referenceAction()}
  }
| /* EMPTY */
  {
    $$.val = tree.ReferenceActions{}
  }

reference_on_update:
  ON UPDATE reference_action
  {
    $$.val = $3.referenceAction()
  }

reference_on_delete:
  ON DELETE reference_action
  {
    $$.val = $3.referenceAction()
  }

reference_action:
// NO ACTION is currently the default behavior. It is functionally the same as
// RESTRICT.
  NO ACTION
  {
    $$.val = tree.NoAction
  }
| RESTRICT
  {
    $$.val = tree.Restrict
  }
| CASCADE
  {
    $$.val = tree.Cascade
  }
| SET NULL
  {
    $$.val = tree.SetNull
  }
| SET DEFAULT
  {
    $$.val = tree.SetDefault
  }

numeric_only:
  FCONST
  {
    $$.val = $1.numVal()
  }
| '-' FCONST
  {
    n := $2.numVal()
    n.Negative = true
    $$.val = n
  }
| signed_iconst
  {
    $$.val = $1.numVal()
  }

// %Help: CREATE SEQUENCE - create a new sequence
// %Category: DDL
// %Text:
// CREATE SEQUENCE <seqname>
//   [INCREMENT <increment>]
//   [MINVALUE <minvalue> | NO MINVALUE]
//   [MAXVALUE <maxvalue> | NO MAXVALUE]
//   [START [WITH] <start>]
//   [CACHE <cache>]
//   [NO CYCLE]
//   [VIRTUAL]
//
// %SeeAlso: CREATE TABLE
create_sequence_stmt:
  CREATE opt_temp SEQUENCE sequence_name opt_sequence_option_list
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateSequence{
        Name: name,
        Options: $5.seqOpts(),
        Temporary: $2.persistenceType(),
        }
  }
| CREATE opt_temp SEQUENCE IF NOT EXISTS sequence_name opt_sequence_option_list
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateSequence{
        Name: name,
        Options: $8.seqOpts(),
        Temporary: $2.persistenceType(),
        IfNotExists: true,
        }
  }
| CREATE opt_temp SEQUENCE error // SHOW HELP: CREATE SEQUENCE

opt_sequence_option_list:
  sequence_option_list
| /* EMPTY */          { $$.val = []tree.SequenceOption(nil) }

sequence_option_list:
  sequence_option_elem                       { $$.val = []tree.SequenceOption{$1.seqOpt()} }
| sequence_option_list sequence_option_elem  { $$.val = append($1.seqOpts(), $2.seqOpt()) }

sequence_option_elem:
  AS typename                  { return unimplementedWithIssueDetail(sqllex, 25110, $2.colType().String()) }
| CYCLE                        { /* SKIP DOC */
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptCycle, Cycle: true} }
| NO CYCLE                     { $$.val = tree.SequenceOption{Name: tree.SeqOptNoCycle, Cycle: false} }
| OWNED BY NONE                { $$.val = tree.SequenceOption{Name: tree.SeqOptOwnedBy}}
| OWNED BY column_path         { varName, err := $3.unresolvedName().NormalizeVarName()
                                   if err != nil {
                                       return setErr(sqllex, err)
                                     }
                                     columnItem, ok := varName.(*tree.ColumnItem)
                                     if !ok {
                                       sqllex.Error(fmt.Sprintf("invalid column name: %q", tree.ErrString($3.unresolvedName())))
                                             return 1
                                     }
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptOwnedBy, ColumnItemVal: columnItem} }
| CACHE signed_iconst64        { /* SKIP DOC */
                                 x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptCache, IntVal: &x} }
| INCREMENT signed_iconst64    { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptIncrement, IntVal: &x} }
| INCREMENT BY signed_iconst64 { x := $3.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptIncrement, IntVal: &x, OptionalWord: true} }
| MINVALUE signed_iconst64     { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptMinValue, IntVal: &x} }
| NO MINVALUE                  { $$.val = tree.SequenceOption{Name: tree.SeqOptMinValue} }
| MAXVALUE signed_iconst64     { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptMaxValue, IntVal: &x} }
| NO MAXVALUE                  { $$.val = tree.SequenceOption{Name: tree.SeqOptMaxValue} }
| START signed_iconst64        { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptStart, IntVal: &x} }
| START WITH signed_iconst64   { x := $3.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptStart, IntVal: &x, OptionalWord: true} }
| VIRTUAL                      { $$.val = tree.SequenceOption{Name: tree.SeqOptVirtual} }

// CREATE FUNCTION STMT
// %Help: CREATE PROCEDURE - create a new procedure or replace an existing procedure
// %Category: DDL
// %Text: CREATE [OR REPLACE] PROCEDURE <proc_name> <proc_args_with_defaults> <proc_source>
// %SeeAlso: WEBDOCS/create-function.html
create_function_stmt:
  	CREATE opt_or_replace PROCEDURE function_name func_args_with_defaults createfunc_opt_list
  	{
  		$$.val = &tree.CreateFunction {
  			IsProcedure:	true,
  			Replace:	$2.bool(),
  			FuncName:	$4.unresolvedObjectName().ToTableName(),
  			Parameters:	$5.functionParameters(),
  			Options:	$6.defElems(),
  		}
  	}
|       CREATE opt_or_replace FUNCTION function_name func_args_with_defaults createfunc_opt_list
        {
        	$$.val = &tree.CreateFunction {
        		IsProcedure:	false,
        		Replace:	$2.bool(),
        		FuncName:	$4.unresolvedObjectName().ToTableName(),
        		Parameters:	$5.functionParameters(),
        		Options:	$6.defElems(),
        	}
        }
|   	CREATE opt_or_replace FUNCTION function_name func_args_with_defaults RETURNS func_return createfunc_opt_list
        {
         	$$.val = &tree.CreateFunction {
         	 	IsProcedure:	false,
         	 	Replace:	$2.bool(),
         	 	FuncName:	$4.unresolvedObjectName().ToTableName(),
         	 	Parameters:	$5.functionParameters(),
         	 	ReturnType:	$7.colType(),
         	 	Options:	$8.defElems(),
         	}
        }
| 	CREATE opt_or_replace PROCEDURE function_name func_args_with_defaults pl_file_language
	{
		$$.val = &tree.CreateFunction {
                  	IsProcedure:	true,
			Replace:	$2.bool(),
			FuncName:	$4.unresolvedObjectName().ToTableName(),
			Parameters:	$5.functionParameters(),
			PlFL:	       	$6.pLFileLanguage(),
                }
	}
| 	CREATE opt_or_replace FUNCTION function_name func_args_with_defaults pl_file_language
	{
		$$.val = &tree.CreateFunction {
                  	IsProcedure:	false,
			Replace:	$2.bool(),
			FuncName:	$4.unresolvedObjectName().ToTableName(),
			Parameters:	$5.functionParameters(),
			PlFL:	       	$6.pLFileLanguage(),
                }
	}
|	CREATE opt_or_replace FUNCTION function_name func_args_with_defaults RETURNS func_return pl_file_language
	{
		$$.val = &tree.CreateFunction {
                        IsProcedure:   	false,
                        Replace:       	$2.bool(),
                        FuncName:      	$4.unresolvedObjectName().ToTableName(),
                        Parameters:    	$5.functionParameters(),
                        ReturnType:    	$7.colType(),
                        PlFL:	       	$8.pLFileLanguage(),
                }
	}

pl_file_language:
	AS EXTERN FUNCTION IN PATH file_path LANGUAGE pl_language
	{
		$$.val = tree.PLFileLanguage {
			Language:	string($8),
			FileName:       string($6),
		}
	}

file_path:
	SCONST

func_args:
	'(' func_args_list ')'
	{
		$$ = $2
	}
|	'(' ')'
	{
		$$.val = tree.FunctionParameters{
        		tree.FunctionParameter{},
                }
	}

func_args_list:
	func_arg
	{
		$$.val = tree.FunctionParameters{*$1.functionParameter()}
	}
| 	func_args_list ',' func_arg
	{
		$$.val = append($1.functionParameters(), *$3.functionParameter())
	}

func_args_with_defaults:
	'(' func_args_with_defaults_list ')'
	{
		$$ = $2
	}
|	'(' ')'
	{
		$$.val = tree.FunctionParameters{
        		tree.FunctionParameter{
                  		Name: "",
                  		Typ: nil,
                  		Mode: 0,
                  	},
                }
	}

func_args_with_defaults_list:
	func_arg_with_default
	{
		$$.val = tree.FunctionParameters{*$1.functionParameter()}
	}
|	func_args_with_defaults_list ',' func_arg_with_default
	{
		$$.val = append($1.functionParameters(), *$3.functionParameter())
	}

func_arg:
	arg_class param_name func_type
	{
		$$.val = &tree.FunctionParameter{
            Name: $2,
			Typ: $3.colType(),
			Mode: $1.funcParam(),
		}
	}
|	param_name arg_class func_type
    {
        $$.val = &tree.FunctionParameter{
            Name: $1,
            Mode: $2.funcParam(),
            Typ: $3.colType(),
        }
    }
|	param_name func_type
	{
		$$.val = &tree.FunctionParameter{
            Name: $1,
			Typ: $2.colType(),
			Mode: tree.FuncParamIN,
		}
	}
| 	arg_class func_type
    {
        $$.val = &tree.FunctionParameter{
            Typ: $2.colType(),
            Mode: $1.funcParam(),
        }
    }
|	func_type
    {
        $$.val = &tree.FunctionParameter{
        Typ: $1.colType(),
        Mode: tree.FuncParamIN,
    }
    }

arg_class:
	IN			{$$.val = tree.FuncParamIN}
|	OUT			{$$.val = tree.FuncParamOUT}
|	INOUT			{$$.val = tree.FuncParamINOUT}
|	IN OUT			{$$.val = tree.FuncParamINOUT}
|	VARIADIC		{$$.val = tree.FuncParamVariadic}

func_return:
	func_type
	{
	 	$$ = $1
	}
| 	VOID
	{
		$$.val = coltypes.Void
	}

func_type:
	typename							{$$ = $1}

func_arg_with_default:
	func_arg
	{
		$$ = $1
	}
|	func_arg DEFAULT a_expr
    {
        temp := $1.functionParameter()
        temp.DefExpr = $3.expr()
        $$.val = temp
        return unimplemented(sqllex, "currently plsql does not support default value in function/procedure inout parameters.")
    }
|	func_arg '=' a_expr
    {
        temp := $1.functionParameter()
        temp.DefExpr = $3.expr()
        $$.val = temp
        return unimplemented(sqllex, "currently plsql does not support default value in function/procedure inout parameters.")
    }

func_as:
	SCONST
	{
		$$.val = []string{$1}
	}
|	SCONST ',' SCONST
	{
		$$.val = append([]string{$1}, $3)
	}

createfunc_opt_list:
	createfunc_opt_item
	{
		$$.val = tree.DefElems{*$1.defElem()}
	}
|	createfunc_opt_list createfunc_opt_item
	{
		$$.val = append($1.defElems(), *$2.defElem())
	}

createfunc_opt_item:
	AS func_as
	{
		$$.val = &tree.DefElem{
			IsAsPart:	true,
			Arg:		$2.strs(),
		}
	}
|	LANGUAGE non_reserved_word_or_sconst
	{
		temp := []string{$2}
		$$.val = &tree.DefElem{
			IsAsPart:	false,
    			Arg:		temp,
    		}
	}


// %Help: TRUNCATE - empty one or more tables
// %Category: DML
// %Text: TRUNCATE [TABLE] <tablename> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/truncate.html
truncate_stmt:
  TRUNCATE opt_table relation_expr_list opt_drop_behavior
  {
    $$.val = &tree.Truncate{Tables: $3.tableNames(), DropBehavior: $4.dropBehavior()}
  }
| TRUNCATE error // SHOW HELP: TRUNCATE

// %Help: CREATE USER - define a new user
// %Category: Priv
// %Text: CREATE USER [IF NOT EXISTS] <name> [ [WITH] PASSWORD <passwd> ]
// %SeeAlso: DROP USER, SHOW USERS, WEBDOCS/create-user.html
create_user_stmt:
  CREATE USER string_or_placeholder opt_user_options
  {
    $$.val = &tree.CreateUser{Name: $3.expr(), KVOptions: $4.kvOptions()}
  }
| CREATE USER IF NOT EXISTS string_or_placeholder opt_user_options
  {
    $$.val = &tree.CreateUser{Name: $6.expr(), KVOptions: $7.kvOptions(), IfNotExists: true}
  }
| CREATE USER error // SHOW HELP: CREATE USER

user_option:
  CREATEUSER
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOCREATEUSER
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| PASSWORD string_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: $2.expr()}
  }
| PASSWORD NULL
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: tree.DNull}
  }
| VALID UNTIL string_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name(fmt.Sprintf("%s_%s",$1, $2)), Value: $3.expr()}
  }
| VALID UNTIL NULL
  {
    $$.val = tree.KVOption{Key: tree.Name(fmt.Sprintf("%s_%s",$1, $2)), Value: tree.DNull}
  }
| ENCRYPTION encryption_clause
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: $2.expr()}
  }
| CONNECTION LIMIT var_value
  {
   $$.val = tree.KVOption{Key: tree.Name(fmt.Sprintf("%s_%s", $1, $2)), Value: $3.expr()}
  }
| CSTATUS
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| nocst
  {
   $$.val=$1.kvOption()
  }

nocst:
 NOCSTATUS
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }

user_options:
  user_option
  {
    $$.val = []tree.KVOption{$1.kvOption()}
  }
| user_options user_option
  {
   $$.val = append($1.kvOptions(), $2.kvOption())
  }

opt_user_options:
  opt_with user_options
  {
    $$.val = $2.kvOptions()
  }
| /* EMPTY */
  {
    $$.val = nil
  }

encryption_clause:
  BCRYPT
  {
    $$.val = tree.NewStrVal(strings.ToUpper($1))
  }
| SM3
  {
    $$.val = tree.NewStrVal(strings.ToUpper($1))
  }
| PBKDF2
  {
    $$.val = tree.NewStrVal(strings.ToUpper($1))
  }

// %Help: CREATE ROLE - define a new role
// %Category: Priv
// %Text: CREATE ROLE [IF NOT EXISTS] <name>
// %SeeAlso: DROP ROLE, SHOW ROLES
create_role_stmt:
  CREATE role_or_group string_or_placeholder
  {
    $$.val = &tree.CreateRole{Name: $3.expr()}
  }
| CREATE role_or_group IF NOT EXISTS string_or_placeholder
  {
    $$.val = &tree.CreateRole{Name: $6.expr(), IfNotExists: true}
  }
| CREATE role_or_group error // SHOW HELP: CREATE ROLE

// "CREATE GROUP is now an alias for CREATE ROLE"
// https://www.postgresql.org/docs/10/static/sql-creategroup.html
role_or_group:
  ROLE  { }
| GROUP { /* SKIP DOC */ }

// %Help: CREATE VIEW - create a new view
// %Category: DDL
// %Text: CREATE VIEW <viewname> [( <colnames...> )] AS <source>
// %SeeAlso: CREATE TABLE, SHOW CREATE, WEBDOCS/create-view.html
create_view_stmt:
  CREATE opt_temp opt_view_recursive VIEW view_name opt_column_list AS select_stmt
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $6.nameList(),
      Temporary: $2.persistenceType(),
      AsSource: $8.slct(),
      IsReplace: false,
    }
  }
| CREATE OR REPLACE opt_temp opt_view_recursive VIEW view_name opt_column_list AS select_stmt
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $8.nameList(),
      Temporary: $4.persistenceType(),
      AsSource: $10.slct(),
      IsReplace: true,
    }
  }
| CREATE MATERIALIZED VIEW view_name opt_column_list AS select_stmt
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $5.nameList(),
      AsSource: $7.slct(),
      Materialized: true,
    }
  }
| CREATE MATERIALIZED VIEW IF NOT EXISTS view_name opt_column_list AS select_stmt
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $8.nameList(),
      AsSource: $10.slct(),
      Materialized: true,
      IfNotExists: true,
    }
  }
| CREATE opt_temp opt_view_recursive VIEW error // SHOW HELP: CREATE VIEW

opt_view_recursive:
  /* EMPTY */ { /* no error */ }
| RECURSIVE { return unimplemented(sqllex, "create recursive view") }

// CREATE TYPE/DOMAIN is not yet supported by ZNBaseDB but we
// want to report it with the right issue number.
create_type_stmt:
  // Record/Composite types.
  CREATE TYPE type_name AS '(' error      { return unimplementedWithIssue(sqllex, 27792) }
  // Enum types.
| CREATE TYPE type_name AS ENUM '(' error { return unimplementedWithIssue(sqllex, 24873) }
  // Range types.
| CREATE TYPE type_name AS RANGE error    { return unimplementedWithIssue(sqllex, 27791) }
  // Base (primitive) types.
| CREATE TYPE type_name '(' error         { return unimplementedWithIssueDetail(sqllex, 27793, "base") }
  // Shell types, gateway to define base types using the previous syntax.
| CREATE TYPE type_name                   { return unimplementedWithIssueDetail(sqllex, 27793, "shell") }
  // Domain types.
| CREATE DOMAIN type_name error           { return unimplementedWithIssueDetail(sqllex, 27796, "create") }


// %Help: CREATE SNAPSHOT - create a SNAPSHOT for table/database
// %Category: Priv
// %Text:
//  --Table Snapshot
//  CREATE SNAPSHOT <name> ON TABLE table_name
//          AS OF SYSTEM TIME <expr>
//					[ WITH <option> [= <value>] [, ...] ]
//  --Database Snapshot
//  CREATE SNAPSHOT <name> ON DATABASE database_name
//          AS OF SYSTEM TIME <expr>
//					[ WITH <option> [= <value>] [, ...] ]
// %SeeAlso: SHOW SNAPSHOT , DROP SNAPSHOT, REVERT SNAPSHOT
create_snapshot_stmt:
  CREATE SNAPSHOT name ON DATABASE database_name opt_as_of_clause opt_with_options
  {
    $$.val = &tree.CreateSnapshot{Type: 0, Name: tree.Name($3), DatabaseName: tree.Name($6), AsOf: $7.asOfClause(), Options: $8.kvOptions(),}
  }
| CREATE SNAPSHOT name ON TABLE table_name opt_as_of_clause opt_with_options
  {
    name := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateSnapshot{Type: 1, Name: tree.Name($3), TableName: name, AsOf: $7.asOfClause(), Options: $8.kvOptions(),}
  }
| CREATE SNAPSHOT error { return helpWith(sqllex, "CREATE SNAPSHOT") }


// %Help: SHOW SNAPSHOT - show snapshot list of table/database
// %Category: Priv
// %Text:
// -- show all snapshot:
//  SHOW SNAPSHOT ALL
// -- show snapshot by <name>:
//  SHOW SNAPSHOT name
// -- show snapshot from table
//  SHOW SNAPSHOT FROM TABLE <table_name>
// -- show snapshot from DATABASE
//  SHOW SNAPSHOT FROM DATABASE <database_name>
// %SeeAlso: CREATE SNAPSHOT
show_snapshot_stmt:
  SHOW SNAPSHOT name
  {
    $$.val = &tree.ShowSnapshot{All:false,Name:  tree.Name($3),}
  }
| SHOW SNAPSHOT FROM DATABASE database_name
  {
    $$.val = &tree.ShowSnapshot{All:false,Type: 0, DatabaseName: tree.Name($5),}
  }
| SHOW SNAPSHOT FROM TABLE table_name
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowSnapshot{All:false,Type: 1, TableName: name,}
  }
| SHOW SNAPSHOT ALL
  {
    $$.val = &tree.ShowSnapshot{All:true,}
  }
| SHOW SNAPSHOT error { return helpWith(sqllex, "SHOW SNAPSHOT") }

// %Help: SHOW FLASHBACK - show flashback list of table/database
// %Category: Priv
// %Text:
// -- show all flashback:
//  SHOW FLASHBACK ALL
show_flashback_stmt:
  SHOW FLASHBACK ALL
  {
    $$.val = &tree.ShowFlashback{}
  }
| SHOW FLASHBACK error { return helpWith(sqllex, "SHOW FLASHBACK") }

// %Help: SHOW CURSORS - show cusors of this session created
// %Category: Priv
// %Text:
// -- show all cursor list of this session created:
//  SHOW CURSORS
// -- show desc of this name cursor
//  SHOW NAME CURSOR
// %SeeAlso: DECLARE CURSOR
show_cursors_stmt:
  SHOW CURSORS
  {
    $$.val = &tree.ShowCursor{ShowAll: true,}
  }
| SHOW CURSORS cursor_name
  {
    $$.val = &tree.ShowCursor{ShowAll: false, CursorName: tree.Name($3),}
  }
| SHOW CURSORS error { return helpWith(sqllex, "SHOW CURSORS") }

// %Help: SHOW FUNCTIONS - list functions
// %Category: DDL
// %Text: SHOW FUNCTIONS [FROM <databasename> [ . <schemaname> ] ]
// %SeeAlso: WEBDOCS/show-functions.html
show_functions_stmt:
  SHOW FUNCTIONS FROM name '.' name opt_with_func_name
  {
    $$.val = &tree.ShowFunctions{
    	TableNamePrefix:tree.TableNamePrefix{
    	    CatalogName: tree.Name($4),
    	    ExplicitCatalog: true,
    	    SchemaName: tree.Name($6),
    	    ExplicitSchema: true,
    	},
    	FuncName: $7,
    }
  }
| SHOW FUNCTIONS FROM name opt_with_func_name
  {
    $$.val = &tree.ShowFunctions{
    	TableNamePrefix:tree.TableNamePrefix{
    	    // Note: the schema name may be interpreted as database name,
    	    // see name_resolution.go.
    	    SchemaName: tree.Name($4),
    	    ExplicitSchema: true,
    	},
    	FuncName: $5,
    }
  }
| SHOW FUNCTIONS opt_with_func_name
  {
    $$.val = &tree.ShowFunctions{
    	FuncName: $3,
    }
  }
| SHOW FUNCTIONS error // SHOW HELP: SHOW FUNCTIONS

// %Help: DROP SNAPSHOT - drop snapshot by name
// %Category: Priv
// %Text:
//  DROP SNAPSHOT name
// %SeeAlso: CREATE SNAPSHOT
drop_snapshot_stmt:
  DROP SNAPSHOT name
  {
    $$.val = &tree.DropSnapshot{Name: tree.Name($3),}
  }
| DROP SNAPSHOT error { return helpWith(sqllex, "DROP SNAPSHOT") }

// %Help: REVERT SNAPSHOT - revert snapshot
// %Category: Priv
// %Text:
//  --Revert table Snapshot
//  REVERT SNAPSHOT <name> ON TABLE table_name
//  --Revert database Snapshot
//  REVERT SNAPSHOT <name> ON DATABASE database_name
// %SeeAlso: CREATE SNAPSHOT
revert_stmt:
  REVERT SNAPSHOT name FROM DATABASE database_name
  {
    $$.val = &tree.RevertSnapshot{Type: 0, Name: tree.Name($3), DatabaseName: tree.Name($6),}
  }
| REVERT SNAPSHOT name FROM TABLE table_name
  {
    tblname := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.RevertSnapshot{Type: 1, Name: tree.Name($3), TableName: tblname,}
  }
| REVERT SNAPSHOT error  { return helpWith(sqllex, "REVERT SNAPSHOT") }

// %Help: FLASHBACK - flashback
// %Category: Priv
// %Text:
//  --Flashback database
//  FLASHBACK DATABASE <name> TO BEFORE DROP WITH OBJECT_ID <id>
//  FLASHBACK DATABASE <name> AS OF SYSTEM TIME <expr>
//  --Flashback table
//  FLASHBACK TABLE <name> TO BEFORE DROP WITH OBJECT_ID <id>
//  FLASHBACK TABLE <name> AS OF SYSTEM TIME <expr>
flashback_stmt:
  FLASHBACK DATABASE database_name TO BEFORE DROP WITH OBJECT_ID iconst64
  {
    $$.val = &tree.RevertDropFlashback{Type: 0, DatabaseName: tree.Name($3), ObjectID: $9.int64(),}
  }
| FLASHBACK DATABASE database_name as_of_clause
  {
    $$.val = &tree.RevertFlashback{Type: 0, DatabaseName: tree.Name($3), AsOf: $4.asOfClause(),}
  }
| FLASHBACK TABLE table_name TO BEFORE DROP WITH OBJECT_ID iconst64
  {
    tblname := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.RevertDropFlashback{Type: 1, TableName: tblname, ObjectID: $9.int64(),}
  }
| FLASHBACK TABLE table_name as_of_clause
  {
    tblname := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.RevertFlashback{Type: 1, TableName: tblname, AsOf: $4.asOfClause(),}
  }
| FLASHBACK error  { return helpWith(sqllex, "FLASHBACK") }

// %Help: CLEAR - clear
// %Category: Priv
// %Text:
//  --Clear database
//  CLEAR INTENT FROM DATABASE <name>
//  --Clear table
//  CLEAR INTENT FROM TABLE <name>
clear_stmt:
  CLEAR INTENT FROM DATABASE database_name
  {
    $$.val = &tree.ClearIntent{Type: 0, DatabaseName: tree.Name($5),}
  }
| CLEAR INTENT FROM TABLE table_name
  {
    tblname := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.ClearIntent{Type: 1, TableName: tblname,}
  }
| CLEAR error  { return helpWith(sqllex, "CLEAR") }

// %Help: CREATE INDEX - create a new index
// %Category: DDL
// %Text:
// CREATE [UNIQUE | INVERTED] INDEX [IF NOT EXISTS] [<idxname>]
//        ON <tablename> ( <colname> [ASC | DESC] [, ...] )
//        [STORING ( <colnames...> )] [<interleave>]
//
// Interleave clause:
//    INTERLEAVE IN PARENT <tablename> ( <colnames...> ) [CASCADE | RESTRICT]
//
// %SeeAlso: CREATE TABLE, SHOW INDEXES, SHOW CREATE,
// WEBDOCS/create-index.html
create_index_stmt:
  CREATE opt_unique INDEX opt_index_name ON table_name opt_using_gin_btree '(' index_params ')' opt_storing opt_interleave opt_partition_by opt_idx_where opt_locate_in
  {
    table := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:    tree.Name($4),
      Table:   table,
      Unique:  $2.bool(),
      Columns: $9.idxElems(),
      Storing: $11.nameList(),
      Interleave: $12.interleave(),
      PartitionBy: $13.partitionBy(),
      Inverted: $7.bool(),
    	LocateSpaceName: $15.location(),
    	Where: tree.NewWhere(tree.AstWhere, $14.expr()),
    }
  }
| CREATE opt_unique INDEX opt_index_name ON table_name opt_using_gin_btree '(' index_params ')' opt_storing opt_interleave LOCAL
  {
    table := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:    tree.Name($4),
      Table:   table,
      Unique:  $2.bool(),
      Columns: $9.idxElems(),
      Storing: $11.nameList(),
      Interleave: $12.interleave(),
      PartitionBy: nil,
      IsLocal: true,
      Inverted: $7.bool(),
    }
  }
| CREATE opt_unique INDEX opt_index_name ON table_name opt_using_gin_btree '(' index_params ')' opt_storing opt_interleave LOCAL '(' opt_partition_list ')'
  {
    table := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:    tree.Name($4),
      Table:   table,
      Unique:  $2.bool(),
      Columns: $9.idxElems(),
      Storing: $11.nameList(),
      Interleave: $12.interleave(),
      PartitionBy: nil,
      IsLocal: true,
      LocalIndexPartitionName: $15.strs(),
      Inverted: $7.bool(),
    }
  }
| CREATE opt_unique INDEX IF NOT EXISTS index_name ON table_name opt_using_gin_btree '(' index_params ')' opt_storing opt_interleave opt_partition_by opt_idx_where opt_locate_in
  {
    table := $9.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:        tree.Name($7),
      Table:       table,
      Unique:      $2.bool(),
      IfNotExists: true,
      Columns:     $12.idxElems(),
      Storing:     $14.nameList(),
      Interleave:  $15.interleave(),
      PartitionBy: $16.partitionBy(),
      Inverted:    $10.bool(),
    	LocateSpaceName: $18.location(),
    	Where: tree.NewWhere(tree.AstWhere, $17.expr()),
    }
  }
| CREATE opt_unique INVERTED INDEX opt_index_name ON table_name '(' index_params ')' opt_storing opt_interleave opt_partition_by opt_idx_where	opt_locate_in
  {
    table := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:       tree.Name($5),
      Table:      table,
      Unique:     $2.bool(),
      Inverted:   true,
      Columns:    $9.idxElems(),
      Storing:     $11.nameList(),
      Interleave:  $12.interleave(),
      PartitionBy: $13.partitionBy(),
    	LocateSpaceName: $15.location(),
    	Where: tree.NewWhere(tree.AstWhere, $14.expr()),
    }
  }
| CREATE opt_unique INVERTED INDEX IF NOT EXISTS index_name ON table_name '(' index_params ')' opt_storing opt_interleave opt_partition_by opt_idx_where	opt_locate_in
  {
    table := $10.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:        tree.Name($8),
      Table:       table,
      Unique:      $2.bool(),
      Inverted:    true,
      IfNotExists: true,
      Columns:     $12.idxElems(),
      Storing:     $14.nameList(),
      Interleave:  $15.interleave(),
      PartitionBy: $16.partitionBy(),
    	LocateSpaceName: $18.location(),
    	Where: tree.NewWhere(tree.AstWhere, $17.expr()),
    }
  }
| CREATE opt_unique INDEX error // SHOW HELP: CREATE INDEX

opt_idx_where:
  where_idx_clause
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

where_idx_clause:
  WHERE a_expr
  {
    $$.val = $2.expr()
  }


opt_using_gin_btree:
  USING name
  {
    /* FORCE DOC */
    switch $2 {
      case "gin":
        $$.val = true
      case "btree":
        $$.val = false
      case "hash", "gist", "spgist", "brin":
        return unimplemented(sqllex, "index using " + $2)
      default:
        sqllex.Error("unrecognized access method: " + $2)
        return 1
    }
  }
| /* EMPTY */
  {
    $$.val = false
  }

opt_unique:
  UNIQUE
  {
    $$.val = true
  }
| /* EMPTY */ %prec CONCAT
  {
    $$.val = false
  }

index_params:
  index_elem
  {
    $$.val = tree.IndexElemList{$1.idxElem()}
  }
| index_params ',' index_elem
  {
    $$.val = append($1.idxElems(), $3.idxElem())
  }

// Index attributes can be either simple column references, or arbitrary
// expressions in parens. For backwards-compatibility reasons, we allow an
// expression that's just a function call to be written without parens.
index_elem:
  a_expr opt_asc_desc
  {
    /* FORCE DOC */
    e := $1.expr()
    if colName, ok := e.(*tree.UnresolvedName); ok && colName.NumParts == 1 {
      $$.val = tree.IndexElem{Column: tree.Name(colName.Parts[0]), Direction: $2.dir()}
    } else if funExpr, ok := e.(*tree.FuncExpr); ok { // 这里需要加入判断条件, 不能是一个没有参数的函数
      $$.val = tree.IndexElem{
      Column:tree.Name(funExpr.String()),
      Function: funExpr,
      Direction: $2.dir(),
      }
    } else {
      return unimplementedWithIssueDetail(sqllex, 9682, fmt.Sprintf("%T", e))
    }
  }

opt_collate:
  COLLATE collation_name { $$ = $2 }
| /* EMPTY */ { $$ = "" }

opt_asc_desc:
  ASC
  {
    $$.val = tree.Ascending
  }
| DESC
  {
    $$.val = tree.Descending
  }
| /* EMPTY */
  {
    $$.val = tree.DefaultDirection
  }

alter_rename_database_stmt:
  ALTER DATABASE database_name RENAME TO database_name
  {
    $$.val = &tree.RenameDatabase{Name: tree.Name($3), NewName: tree.Name($6)}
  }

// https://www.postgresql.org/docs/10/static/sql-alteruser.html
alter_user_options_stmt:
  ALTER USER string_or_placeholder opt_user_options
  {
    $$.val = &tree.AlterUser{Name: $3.expr(), KVOptions: $4.kvOptions()}
  }
| ALTER USER IF EXISTS string_or_placeholder opt_user_options
  {
    $$.val = &tree.AlterUser{Name: $5.expr(), KVOptions: $6.kvOptions(), IfExists: true}
  }

alter_user_searchpath_stmt:
  ALTER USER string_or_placeholder SET SEARCH_PATH to_or_eq string_or_placeholder_list
  {
    $$.val = &tree.AlterUserDefaultSchma{Name: $3.expr(), Values: $7.exprs()}
  }
| ALTER USER string_or_placeholder IN DATABASE database_name SET SEARCH_PATH to_or_eq string_or_placeholder_list
  {
    $$.val = &tree.AlterUserDefaultSchma{Name: $3.expr(), Values: $10.exprs(), DBName: tree.Name($6)}
  }
| ALTER USER string_or_placeholder SET SEARCH_PATH to_or_eq DEFAULT
  {
    $$.val = &tree.AlterUserDefaultSchma{Name: $3.expr(), IsDefalut: true}
  }
| ALTER USER string_or_placeholder IN DATABASE database_name SET SEARCH_PATH to_or_eq DEFAULT
  {
    $$.val = &tree.AlterUserDefaultSchma{Name: $3.expr(), IsDefalut: true, DBName: tree.Name($6)}
  }

alter_rename_table_stmt:
  ALTER TABLE relation_expr RENAME TO table_name
  {
    name := $3.unresolvedObjectName().ToTableName()
    newName := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: false, IsView: false}
  }
| ALTER TABLE IF EXISTS relation_expr RENAME TO table_name
  {
    name := $5.unresolvedObjectName().ToTableName()
    newName := $8.unresolvedObjectName().ToTableName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: true, IsView: false}
  }

alter_rename_schema_stmt:
  ALTER SCHEMA schema_name RENAME TO schema_name
  {
    name := $3.unresolvedObjectName().ToSchemaName()
    newName := $6.unresolvedObjectName().ToSchemaName()
    $$.val = &tree.AlterSchema{Schema: name, NewSchema: newName, IfExists: false}
  }
| ALTER SCHEMA IF EXISTS schema_name RENAME TO schema_name
  {
    name := $5.unresolvedObjectName().ToSchemaName()
    newName := $8.unresolvedObjectName().ToSchemaName()
    $$.val = &tree.AlterSchema{Schema: name, NewSchema: newName, IfExists: true}
  }

alter_rename_view_stmt:
  ALTER VIEW relation_expr RENAME TO view_name
  {
    name := $3.unresolvedObjectName().ToTableName()
    newName := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: false, IsView: true}
  }
| ALTER MATERIALIZED VIEW relation_expr RENAME TO view_name
  {
    name := $4.unresolvedObjectName().ToTableName()
    newName := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.RenameTable{
      Name: name,
      NewName: newName,
      IfExists: false,
      IsView: true,
      IsMaterialized: true,
    }
  }
| ALTER VIEW IF EXISTS relation_expr RENAME TO view_name
  {
    name := $5.unresolvedObjectName().ToTableName()
    newName := $8.unresolvedObjectName().ToTableName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: true, IsView: true}
  }
| ALTER MATERIALIZED VIEW IF EXISTS relation_expr RENAME TO view_name
  {
    name := $6.unresolvedObjectName().ToTableName()
    newName := $9.unresolvedObjectName().ToTableName()
    $$.val = &tree.RenameTable{
      Name: name,
      NewName: newName,
      IfExists: true,
      IsView: true,
      IsMaterialized: true,
    }
  }

alter_rename_sequence_stmt:
  ALTER SEQUENCE relation_expr RENAME TO sequence_name
  {
    name := $3.unresolvedObjectName().ToTableName()
    newName := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: false, IsSequence: true}
  }
| ALTER SEQUENCE IF EXISTS relation_expr RENAME TO sequence_name
  {
    name := $5.unresolvedObjectName().ToTableName()
    newName := $8.unresolvedObjectName().ToTableName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: true, IsSequence: true}
  }

alter_rename_index_stmt:
  ALTER INDEX table_index_name RENAME TO index_name
  {
    $$.val = &tree.RenameIndex{Index: $3.newTableIndexName(), NewName: tree.UnrestrictedName($6), IfExists: false}
  }
| ALTER INDEX IF EXISTS table_index_name RENAME TO index_name
  {
    $$.val = &tree.RenameIndex{Index: $5.newTableIndexName(), NewName: tree.UnrestrictedName($8), IfExists: true}
  }

opt_column:
  COLUMN {}
| /* EMPTY */ {} %prec IDENT

opt_set_data:
  SET DATA {}
| /* EMPTY */ {}

// %Help: REFRESH - recalculate a materialized view
// %Category: Misc
// %Text:
// REFRESH MATERIALIZED VIEW view_name
refresh_stmt:
  REFRESH MATERIALIZED VIEW view_name
  {
    $$.val = &tree.RefreshMaterializedView{Name: $4.unresolvedObjectName(),}
  }
| REFRESH error // SHOW HELP: REFRESH

// %Help: RELEASE - complete a retryable block
// %Category: Txn
// %Text: RELEASE [SAVEPOINT] znbasedb_restart
// %SeeAlso: SAVEPOINT, WEBDOCS/savepoint.html
release_stmt:
  RELEASE savepoint_name
  {
    $$.val = &tree.ReleaseSavepoint{Savepoint: tree.Name($2)}
  }
| RELEASE error // SHOW HELP: RELEASE

// %Help: RESUME JOBS - resume background jobs
// %Category: Misc
// %Text:
// RESUME JOBS <selectclause>
// RESUME JOB <jobid>
// %SeeAlso: SHOW JOBS, CANCEL JOBS, PAUSE JOBS
resume_stmt:
  RESUME JOB a_expr
  {
    $$.val = &tree.ControlJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.ResumeJob,
    }
  }
| RESUME JOBS select_stmt
  {
    $$.val = &tree.ControlJobs{Jobs: $3.slct(), Command: tree.ResumeJob}
  }
| RESUME error // SHOW HELP: RESUME JOBS

// %Help: SAVEPOINT - start a retryable block
// %Category: Txn
// %Text: SAVEPOINT znbasedb_restart
// %SeeAlso: RELEASE, WEBDOCS/savepoint.html
savepoint_stmt:
  SAVEPOINT name
  {
    $$.val = &tree.Savepoint{Name: tree.Name($2)}
  }
| SAVEPOINT error // SHOW HELP: SAVEPOINT

// BEGIN / START / COMMIT / END / ROLLBACK / ...
transaction_stmt:
  begin_stmt    // EXTEND WITH HELP: BEGIN
| commit_stmt   // EXTEND WITH HELP: COMMIT
| rollback_stmt // EXTEND WITH HELP: ROLLBACK
| abort_stmt    /* SKIP DOC */

// %Help: BEGIN - start a transaction
// %Category: Txn
// %Text:
// BEGIN [TRANSACTION] [ <txnparameter> [[,] ...] ]
// START TRANSACTION [ <txnparameter> [[,] ...] ]
//
// Transaction parameters:
//    ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE }
//    PRIORITY { LOW | NORMAL | HIGH }
//
// %SeeAlso: COMMIT, ROLLBACK, WEBDOCS/begin-transaction.html
begin_stmt:
  BEGIN opt_transaction begin_transaction
  {
    $$.val = $3.stmt()
  }
| BEGIN error // SHOW HELP: BEGIN
| START TRANSACTION begin_transaction
  {
    $$.val = $3.stmt()
  }
| START error // SHOW HELP: BEGIN

// %Help: COMMIT - commit the current transaction
// %Category: Txn
// %Text:
// COMMIT [TRANSACTION]
// END [TRANSACTION]
// %SeeAlso: BEGIN, ROLLBACK, WEBDOCS/commit-transaction.html
commit_stmt:
  COMMIT opt_transaction
  {
    $$.val = &tree.CommitTransaction{}
  }
| COMMIT error // SHOW HELP: COMMIT
| END opt_transaction
  {
    $$.val = &tree.CommitTransaction{}
  }
| END error // SHOW HELP: COMMIT

abort_stmt:
  ABORT opt_abort_mod
  {
    $$.val = &tree.RollbackTransaction{}
  }

opt_abort_mod:
  TRANSACTION {}
| WORK        {}
| /* EMPTY */ {}

// %Help: ROLLBACK - abort the current transaction
// %Category: Txn
// %Text: ROLLBACK [TRANSACTION] [TO [SAVEPOINT] znbasedb_restart]
// %SeeAlso: BEGIN, COMMIT, SAVEPOINT, WEBDOCS/rollback-transaction.html
rollback_stmt:
  ROLLBACK opt_to_savepoint
  {
    if $2 != "" {
      $$.val = &tree.RollbackToSavepoint{Savepoint: tree.Name($2)}
    } else {
      $$.val = &tree.RollbackTransaction{}
    }
  }
| ROLLBACK error // SHOW HELP: ROLLBACK

opt_transaction:
  TRANSACTION {}
| /* EMPTY */ {}

opt_to_savepoint:
  TRANSACTION
  {
    $$ = ""
  }
| TRANSACTION TO savepoint_name
  {
    $$ = $3
  }
| TO savepoint_name
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = ""
  }

savepoint_name:
  SAVEPOINT name
  {
    $$ = $2
  }
| name
  {
    $$ = $1
  }

begin_transaction:
  transaction_mode_list
  {
    $$.val = &tree.BeginTransaction{Modes: $1.transactionModes()}
  }
| /* EMPTY */
  {
    $$.val = &tree.BeginTransaction{}
  }

transaction_name_stmt:
  NAME transaction_name
  {
    $$.val = tree.Name($2)
  }

transaction_mode_list:
  transaction_mode
  {
    $$.val = $1.transactionModes()
  }
| transaction_mode_list opt_comma transaction_mode
  {
    a := $1.transactionModes()
    b := $3.transactionModes()
    err := a.Merge(b)
    if err != nil { return setErr(sqllex, err) }
    $$.val = a
  }

// The transaction mode list after BEGIN should use comma-separated
// modes as per the SQL standard, but PostgreSQL historically allowed
// them to be listed without commas too.
opt_comma:
  ','
  { }
| /* EMPTY */
  { }

transaction_mode:
  transaction_iso_level
  {
    /* SKIP DOC */
    $$.val = tree.TransactionModes{Isolation: $1.isoLevel()}
  }
| transaction_user_priority
  {
    $$.val = tree.TransactionModes{UserPriority: $1.userPriority()}
  }
| transaction_read_mode
  {
    $$.val = tree.TransactionModes{ReadWriteMode: $1.readWriteMode()}
  }
| as_of_clause
  {
    $$.val = tree.TransactionModes{AsOf: $1.asOfClause()}
  }
| transaction_name_stmt
  {
    $$.val = tree.TransactionModes{Name: $1.transactionName()}
  }
| transaction_sup_ddl
  {
    $$.val = tree.TransactionModes{TxnDDL: $1.support_ddl()}
  }

transaction_user_priority:
  PRIORITY user_priority
  {
    $$.val = $2.userPriority()
  }

transaction_iso_level:
  ISOLATION LEVEL iso_level
  {
    $$.val = $3.isoLevel()
  }

transaction_sup_ddl:
  SUPDDL
  {
    $$.val = tree.SupportDDL
  }
| NOTSUPDDL
  {
    $$.val = tree.NotSupportDDL
  }

transaction_read_mode:
  READ ONLY
  {
    $$.val = tree.ReadOnly
  }
| READ WRITE
  {
    $$.val = tree.ReadWrite
  }


// %Help: CALL - call user define procedure
// %Category: DML
// %Text:
// CALL <proc_name> <args>
//
// Commands:
//   CALL <proc_name> <args>
// %SeeAlso: WEBDOCS/call-procedure.html
call_stmt:
CALL func_application
  {
    temp := $2.funcExpr()
    $$.val = &tree.CallStmt {
    	FuncName: temp.Func,
    	Args: temp.Exprs,
    }
  }
| CALL error  // SHOW HELP: CALL

// %Help: CREATE DATABASE - create a new database
// %Category: DDL
// %Text: CREATE DATABASE [IF NOT EXISTS] <name>
// %SeeAlso: WEBDOCS/create-database.html
create_database_stmt:
  CREATE DATABASE database_name opt_with opt_template_encoding opt_lc_collate_clause opt_lc_ctype_clause
  {
    templateEncoding := $5.strs()
    $$.val = &tree.CreateDatabase{
      Name: tree.Name($3),
      Template: templateEncoding[0],
      Encoding: templateEncoding[1],
      Collate: $6,
      CType: $7,
    }
  }
| CREATE DATABASE IF NOT EXISTS database_name opt_with opt_template_encoding opt_lc_collate_clause opt_lc_ctype_clause
  {
    templateEncoding := $8.strs()
    $$.val = &tree.CreateDatabase{
      IfNotExists: true,
      Name: tree.Name($6),
      Template: templateEncoding[0],
      Encoding: templateEncoding[1],
      Collate: $9,
      CType: $10,
    }
   }
| CREATE DATABASE error // SHOW HELP: CREATE DATABASE

template_clause:
  TEMPLATE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }

encoding_clause:
  ENCODING opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }

opt_template_encoding:
  template_clause encoding_clause
  {
  	$$.val = []string{$1, $2}
  }
| encoding_clause template_clause
  {
  	$$.val = []string{$2, $1}
  }
| template_clause
  {
  	$$.val = []string{$1, ""}
  }
| encoding_clause
  {
  	$$.val = []string{"", $1}
  }
| /* EMPTY */
  {
   	$$.val = []string{"", ""}
  }

opt_lc_collate_clause:
  LC_COLLATE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_lc_ctype_clause:
  LC_CTYPE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_equal:
  '=' {}
| /* EMPTY */ {}

// %Help: INSERT - create new rows in a table
// %Category: DML
// %Text:
// INSERT INTO <tablename> [[AS] <name>] [( <colnames...> )]
//        <selectclause>
//        [ON CONFLICT [( <colnames...> )] {DO UPDATE SET ... [WHERE <expr>] | DO NOTHING}]
//        [RETURNING <exprs...>]
// %SeeAlso: UPSERT, UPDATE, DELETE, WEBDOCS/insert.html
insert_stmt:
  opt_with_clause INSERT INTO insert_target insert_rest returning_clause
  {
    $$.val = $5.stmt()
    $$.val.(*tree.Insert).With = $1.with()
    $$.val.(*tree.Insert).Table = $4.tblExpr()
    $$.val.(*tree.Insert).Returning = $6.retClause()
  }
| opt_with_clause INSERT INTO insert_target insert_rest on_conflict returning_clause
  {
    conflict := $6.onConflict()
    if conflict.IsDuplicate {
      $$.val = $5.stmt()
      $$.val.(*tree.Insert).With = nil
      $$.val.(*tree.Insert).Table = $4.tblExpr()
      $$.val.(*tree.Insert).OnConflict = $6.onConflict()
      $$.val.(*tree.Insert).Returning = tree.AbsentReturningClause
    } else {
      $$.val = $5.stmt()
      $$.val.(*tree.Insert).With = $1.with()
      $$.val.(*tree.Insert).Table = $4.tblExpr()
      $$.val.(*tree.Insert).OnConflict = $6.onConflict()
      $$.val.(*tree.Insert).Returning = $7.retClause()
    }

  }
| opt_with_clause INSERT error // SHOW HELP: INSERT

merge_stmt:
  MERGE INTO insert_target USING merge_rest
    ON a_expr
    match_list
    notmatch_list
    opt_ignore
  {
    $$.val = $5.stmt()
    $$.val.(*tree.Insert).With = nil
    $$.val.(*tree.Insert).Table = $3.tblExpr()
    $$.val.(*tree.Insert).OnConflict = &tree.OnConflict{
	    // Where: tree.NewWhere(tree.AstWhere, $10.expr()),
	    Condition: &tree.OnJoinCond{Expr: $7.expr()},
	    MatchList: $8.condAndOpList(),
	    //Exprs: $14.updateExprs(),
	    NotMatchList: $9.nmCondAndOpList(),
            MergeOrNot: true,
	    }
    $$.val.(*tree.Insert).Returning = tree.AbsentReturningClause
  }
| MERGE INTO insert_target USING merge_rest
    ON a_expr
    match_list
    opt_ignore
  {
    $$.val = $5.stmt()
    $$.val.(*tree.Insert).With = nil
    $$.val.(*tree.Insert).Table = $3.tblExpr()
    $$.val.(*tree.Insert).OnConflict = &tree.OnConflict{
	    // Where: tree.NewWhere(tree.AstWhere, $10.expr()),
	    Condition: &tree.OnJoinCond{Expr: $7.expr()},
	    MatchList: $8.condAndOpList(),
	    //Exprs: $14.updateExprs(),
	    MergeOrNot: true,
	    }
    $$.val.(*tree.Insert).Returning = tree.AbsentReturningClause
  }
| MERGE INTO insert_target USING merge_rest
    ON a_expr
    notmatch_list
    opt_ignore
  {
    $$.val = $5.stmt()
    $$.val.(*tree.Insert).With = nil
    $$.val.(*tree.Insert).Table = $3.tblExpr()
    $$.val.(*tree.Insert).OnConflict = &tree.OnConflict{
  	    // Where: tree.NewWhere(tree.AstWhere, $10.expr()),
  	    Condition: &tree.OnJoinCond{Expr: $7.expr()},
  	    //Exprs: $14.updateExprs(),
  	    NotMatchList: $8.nmCondAndOpList(),
  	    MergeOrNot: true,
  	    }
    $$.val.(*tree.Insert).Returning = tree.AbsentReturningClause
  }

match_list:
  match_item
  {
    $$.val = tree.CondAndOpList{*$1.condAndOp()}
  }
| match_list match_item
  {
    $$.val = append($1.condAndOpList(), *$2.condAndOp())
  }

notmatch_list:
  notmatch_item
  {
    $$.val = tree.NmCondAndOpList{*$1.nmCondAndOp()}
  }
| notmatch_list notmatch_item
  {
    $$.val = append($1.nmCondAndOpList(), *$2.nmCondAndOp())
  }

match_item:
  WHEN MATCHED opt_aexpr THEN UPDATE SET set_clause_list
  {
    $$.val = &tree.CondAndOp{Condition: $3.expr(), Operation: $7.updateExprs()}
  }
| WHEN MATCHED opt_aexpr THEN DELETE
  {
    $$.val = &tree.CondAndOp{Condition: $3.expr(), IsDelete: true}
  }
| WHEN MATCHED opt_aexpr THEN signal_clause
  {
    $$.val = &tree.CondAndOp{Condition: $3.expr(), Sig: $5.signal()}
  }

notmatch_item:
  WHEN NOT MATCHED opt_aexpr THEN INSERT '(' name_list ')' values_clause_merge
  {
    $$.val = &tree.NmCondAndOp{Condition: $4.expr(), ColNames: $8.nameList(), Operation: $10.valuesClause().Rows}
  }
| WHEN NOT MATCHED opt_aexpr THEN INSERT values_clause_merge
  {
    $$.val = &tree.NmCondAndOp{Condition: $4.expr(), Operation: $7.valuesClause().Rows}
  }
| WHEN NOT MATCHED opt_aexpr THEN signal_clause
  {
    $$.val = &tree.NmCondAndOp{Condition: $4.expr(), Sig: $6.signal()}
  }

signal_clause:
  SIGNAL SQLSTATE opt_value sqlstate_string_constant SET MESSAGE_TEXT diagnostic_string_expression
  {
    $$.val = &tree.Signal{SQLState: $4.strPtr() , MessageText: $7.strPtr()}
  }

opt_value:
  VALUE {}
|  /* EMPTY */  {}

sqlstate_string_constant:
  SCONST
  {
    t := $1
    $$.val = &t
  }

diagnostic_string_expression:
  SCONST
  {
    t := $1
    $$.val = &t
  }

merge_rest:
  table_ref
  {
    row := &tree.SelectClause{
          Exprs:       tree.SelectExprs{tree.StarSelectExpr()},
          From:        &tree.From{Tables: tree.TableExprs{$1.tblExpr()}},
          TableSelect: true,
        }
    $$.val = &tree.Insert{Rows: &tree.Select{Select: row}}
  }

opt_aexpr:
  AND a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

opt_ignore:
  ELSE IGNORE {}
| /* EMPTY */ {}

// %Help: UPSERT - create or replace rows in a table
// %Category: DML
// %Text:
// UPSERT INTO <tablename> [AS <name>] [( <colnames...> )]
//        <selectclause>
//        [RETURNING <exprs...>]
// %SeeAlso: INSERT, UPDATE, DELETE, WEBDOCS/upsert.html
upsert_stmt:
  opt_with_clause UPSERT INTO insert_target insert_rest returning_clause
  {
    $$.val = $5.stmt()
    $$.val.(*tree.Insert).With = $1.with()
    $$.val.(*tree.Insert).Table = $4.tblExpr()
    $$.val.(*tree.Insert).OnConflict = &tree.OnConflict{}
    $$.val.(*tree.Insert).Returning = $6.retClause()
  }
| opt_with_clause UPSERT error // SHOW HELP: UPSERT

insert_target:
  table_name
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = &name
  }
// Can't easily make AS optional here, because VALUES in insert_rest would have
// a shift/reduce conflict with VALUES as an optional alias. We could easily
// allow unreserved_keywords as optional aliases, but that'd be an odd
// divergence from other places. So just require AS for now.
| table_name AS table_alias_name
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = &tree.AliasedTableExpr{Expr: &name, As: tree.AliasClause{Alias: tree.Name($3)}}
  }

//insert_clause:
//  select_stmt
//| select_stmt AS table_alias_name opt_column_list
//  {
//    $$.val = $1.ins()
//    $$.val.(*tree.Insert).Alias = tree.AliasClause{Alias:tree.Name($3), Cols:$4.nameList()}
//  }

insert_rest:
  select_stmt
  {
    $$.val = &tree.Insert{Rows: $1.slct()}
  }
| '(' insert_column_list ')' select_stmt
  {
    $$.val = &tree.Insert{Columns: $2.nameList(), Rows: $4.slct()}
  }
| DEFAULT VALUES
  {
    $$.val = &tree.Insert{Rows: &tree.Select{}}
  }

insert_column_list:
  insert_column_item
  {
    $$.val = tree.NameList{tree.Name($1)}
  }
| insert_column_list ',' insert_column_item
  {
    $$.val = append($1.nameList(), tree.Name($3))
  }

// insert_column_item represents the target of an INSERT/UPSERT or one
// of the LHS operands in an UPDATE SET statement.
//
//    INSERT INTO foo (x, y) VALUES ...
//                     ^^^^ here
//
//    UPDATE foo SET x = 1+2, (y, z) = (4, 5)
//                   ^^ here   ^^^^ here
//
// Currently ZNBaseDB only supports simple column names in this
// position. The rule below can be extended to support a sequence of
// field subscript or array indexing operators to designate a part of
// a field, when partial updates are to be supported. This likely will
// be needed together with support for composite types (#27792).
insert_column_item:
  column_name
| column_name '.' error { return unimplementedWithIssue(sqllex, 27792) }

on_conflict:
  ON CONFLICT opt_conf_expr DO UPDATE SET set_clause_list opt_where_clause
  {
    $$.val = &tree.OnConflict{Columns: $3.nameList(), Exprs: $7.updateExprs(), Where: tree.NewWhere(tree.AstWhere, $8.expr())}
  }
| ON CONFLICT opt_conf_expr DO NOTHING
  {
    $$.val = &tree.OnConflict{Columns: $3.nameList(), DoNothing: true}
  }
| ON DUPLICATE KEY UPDATE set_clause_list
  {
    $$.val = &tree.OnConflict{IsDuplicate:true,Exprs: $5.updateExprs()}
  }
opt_conf_expr:
  '(' name_list ')'
  {
    $$.val = $2.nameList()
  }
| '(' name_list ')' where_clause { return unimplementedWithIssue(sqllex, 32557) }
| ON CONSTRAINT constraint_name { return unimplementedWithIssue(sqllex, 28161) }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

returning_clause:
  RETURNING target_list
  {
    ret := tree.ReturningExprs($2.selExprs())
    $$.val = &ret
  }
| RETURNING NOTHING
  {
    $$.val = tree.ReturningNothingClause
  }
| /* EMPTY */
  {
    $$.val = tree.AbsentReturningClause
  }

// %Help: UPDATE - update rows of a table
// %Category: DML
// %Text:
// UPDATE <tablename> [[AS] <name>]
//        SET ...
//        [WHERE <expr>]
//        [ORDER BY <exprs...>]
//        [LIMIT <expr>]
//        [RETURNING <exprs...>]
// %SeeAlso: INSERT, UPSERT, DELETE, WEBDOCS/update.html
update_stmt:
  opt_with_clause UPDATE table_name_expr_opt_alias_idx
    SET set_clause_list opt_from_list opt_where_or_current_clause opt_sort_clause opt_limit_clause returning_clause
  {
    $$.val = &tree.Update{
      With: $1.with(),
      Table: $3.tblExpr(),
      Exprs: $5.updateExprs(),
      From: $6.tblExprs(),
      Where: tree.NewWhere(tree.AstWhere, $7.expr()),
      OrderBy: $8.orderBy(),
      Limit: $9.limit(),
      Returning: $10.retClause(),
    }
  }
| opt_with_clause UPDATE error // SHOW HELP: UPDATE

opt_from_list:
  FROM from_list {
    $$.val = $2.tblExprs()
  }
| /* EMPTY */ {
    $$.val = tree.TableExprs{}
}

// %Help: QUERY LOCKSTAT- query hold lock status of a transaction
// %Category: Misc
// %Text:
// QUERY LOCKSTAT <selectclause>
query_lockstat_stmt:
  QUERY LOCKSTAT a_expr
  {
    $$.val = &tree.QueryLockstat{
      Txnid: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
    }
  }
| QUERY LOCKSTAT error // SHOW HELP: QUERY LOCKSTAT

set_clause_list:
  set_clause
  {
    $$.val = tree.UpdateExprs{$1.updateExpr()}
  }
| set_clause_list ',' set_clause
  {
    $$.val = append($1.updateExprs(), $3.updateExpr())
  }

// TODO(knz): The LHS in these can be extended to support
// a path to a field member when compound types are supported.
// Keep it simple for now.
set_clause:
  single_set_clause
| multiple_set_clause

single_set_clause:
  column_name '=' a_expr
  {
    $$.val = &tree.UpdateExpr{Names: tree.NameList{tree.Name($1)}, Expr: $3.expr()}
  }
| column_name '.' error { return unimplementedWithIssue(sqllex, 27792) }

multiple_set_clause:
  '(' insert_column_list ')' '=' in_expr
  {
    $$.val = &tree.UpdateExpr{Tuple: true, Names: $2.nameList(), Expr: $5.expr()}
  }
expend_select_stmt:
select_stmt
  {
    $$.val = &tree.ExpendSelect{Query: $1.slct()}
  }
| DATABASE name
  {
    $$.val = &tree.ExpendSelect{Type: 1, DbName: tree.Name($2)}
  }
| SCHEMA schema_name
  {
    name:=$2.unresolvedObjectName().ToSchemaName()
    $$.val = &tree.ExpendSelect{Type: 2,ScName: &name}
  }
// A complete SELECT statement looks like this.
//
// The rule returns either a single select_stmt node or a tree of them,
// representing a set-operation tree.
//
// There is an ambiguity when a sub-SELECT is within an a_expr and there are
// excess parentheses: do the parentheses belong to the sub-SELECT or to the
// surrounding a_expr?  We don't really care, but bison wants to know. To
// resolve the ambiguity, we are careful to define the grammar so that the
// decision is staved off as long as possible: as long as we can keep absorbing
// parentheses into the sub-SELECT, we will do so, and only when it's no longer
// possible to do that will we decide that parens belong to the expression. For
// example, in "SELECT (((SELECT 2)) + 3)" the extra parentheses are treated as
// part of the sub-select. The necessity of doing it that way is shown by
// "SELECT (((SELECT 2)) UNION SELECT 2)". Had we parsed "((SELECT 2))" as an
// a_expr, it'd be too late to go back to the SELECT viewpoint when we see the
// UNION.
//
// This approach is implemented by defining a nonterminal select_with_parens,
// which represents a SELECT with at least one outer layer of parentheses, and
// being careful to use select_with_parens, never '(' select_stmt ')', in the
// expression grammar. We will then have shift-reduce conflicts which we can
// resolve in favor of always treating '(' <select> ')' as a
// select_with_parens. To resolve the conflicts, the productions that conflict
// with the select_with_parens productions are manually given precedences lower
// than the precedence of ')', thereby ensuring that we shift ')' (and then
// reduce to select_with_parens) rather than trying to reduce the inner
// <select> nonterminal to something else. We use UMINUS precedence for this,
// which is a fairly arbitrary choice.
//
// To be able to define select_with_parens itself without ambiguity, we need a
// nonterminal select_no_parens that represents a SELECT structure with no
// outermost parentheses. This is a little bit tedious, but it works.
//
// In non-expression contexts, we use select_stmt which can represent a SELECT
// with or without outer parentheses.
select_stmt:
  select_no_parens %prec UMINUS
| select_with_parens %prec UMINUS
  {
    $$.val = &tree.Select{Select: $1.selectStmt()}
    if sel,ok := $$.val.(*tree.Select).Select.(*tree.SelectClause);ok{
      $$.val.(*tree.Select).HintSet = sel.HintSet
    }
  }

select_stmt_no_as:
  select_no_parens %prec UMINUS
| select_with_parens %prec UMINUS
  {
    $$.val = &tree.Select{Select: $1.selectStmt()}
  }

select_with_parens:
  '(' select_no_parens ')'
  {
    $$.val = &tree.ParenSelect{Select: $2.slct()}
  }
| '(' select_with_parens ')'
  {
    $$.val = &tree.ParenSelect{Select: &tree.Select{Select: $2.selectStmt()}}
  }

// This rule parses the equivalent of the standard's <query expression>. The
// duplicative productions are annoying, but hard to get rid of without
// creating shift/reduce conflicts.
//
//      The locking clause (FOR UPDATE etc) may be before or after
//      LIMIT/OFFSET. In <=7.2.X, LIMIT/OFFSET had to be after FOR UPDATE We
//      now support both orderings, but prefer LIMIT/OFFSET before the locking
//      clause.
//      - 2002-08-28 bjm

select_no_parens:
  simple_select
  {
    $$.val = &tree.Select{Select: $1.selectStmt()}
     if sel,ok := $$.val.(*tree.Select).Select.(*tree.SelectClause);ok{
        if sel.ConnectBy != nil{
          name := "recursive_temp"
          tableExpr := $$.val.(*tree.Select).Select.(*tree.SelectClause).From.Tables
          $$.val.(*tree.Select).With = tree.YaccBuildCET(sel,name,tableExpr)
        }
     }
  }
| select_clause sort_clause
  {
    $$.val = &tree.Select{Select: $1.selectStmt(), OrderBy: $2.orderBy()}
     if sel,ok := $$.val.(*tree.Select).Select.(*tree.SelectClause);ok{
        if sel.ConnectBy != nil{
          name := "recursive_temp"
          tableExpr := $$.val.(*tree.Select).Select.(*tree.SelectClause).From.Tables
          $$.val.(*tree.Select).With = tree.YaccBuildCET(sel,name,tableExpr)
        }
     }
  }
| select_clause opt_sort_clause for_locking_clause opt_select_limit
  {
    $$.val = &tree.Select{Select: $1.selectStmt(), OrderBy: $2.orderBy(), Limit: $4.limit(), ForLocked: $3.lockingClause()}
  }
| select_clause opt_sort_clause select_limit opt_for_locking_clause
  {
    $$.val = &tree.Select{Select: $1.selectStmt(), OrderBy: $2.orderBy(), Limit: $3.limit(), ForLocked: $4.lockingClause()}
     if sel,ok := $$.val.(*tree.Select).Select.(*tree.SelectClause);ok{
        if sel.ConnectBy != nil{
          name := "recursive_temp"
          tableExpr := $$.val.(*tree.Select).Select.(*tree.SelectClause).From.Tables
          $$.val.(*tree.Select).With = tree.YaccBuildCET(sel,name,tableExpr)
        }
     }
  }


| with_clause select_clause
  {
    $$.val = &tree.Select{With: $1.with(), Select: $2.selectStmt()}
  }
| with_clause select_clause sort_clause
  {
    $$.val = &tree.Select{With: $1.with(), Select: $2.selectStmt(), OrderBy: $3.orderBy()}
  }
| with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit
  {
    $$.val = &tree.Select{With: $1.with(), Select: $2.selectStmt(), OrderBy: $3.orderBy(), Limit: $5.limit(), ForLocked: $4.lockingClause()}
  }
| with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
  {
    $$.val = &tree.Select{With: $1.with(), Select: $2.selectStmt(), OrderBy: $3.orderBy(), Limit: $4.limit(), ForLocked: $5.lockingClause()}
  }

opt_start_with:
start_with
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }
start_with:
  START WITH a_expr
  {
    $$.val = $3.expr()
  }

connect_by:

  CONNECT BY PRIOR prior_expr '=' prior_expr
  {
   left := $4.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Operator: tree.EQ, Left: left, Right: $6.unresolvedName()}
  }
| CONNECT BY PRIOR prior_expr LESS_EQUALS prior_expr
  {
   left := $4.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Operator: tree.LE, Left: left, Right: $6.unresolvedName()}
  }
| CONNECT BY PRIOR prior_expr GREATER_EQUALS prior_expr
  {
   left := $4.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Operator: tree.GE, Left: left, Right: $6.unresolvedName()}
  }
| CONNECT BY PRIOR prior_expr NOT_EQUALS prior_expr
  {
   left := $4.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Operator: tree.NE, Left: left, Right: $6.unresolvedName()}
  }
| CONNECT BY PRIOR prior_expr '<' prior_expr
  {
   left := $4.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Operator: tree.LT, Left: left, Right: $6.unresolvedName()}
  }
| CONNECT BY PRIOR prior_expr '>' prior_expr
  {
   left := $4.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Operator: tree.GT, Left: left, Right: $6.unresolvedName()}
  }

| CONNECT BY prior_expr '<' PRIOR prior_expr
  {
    right := $6.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Operator: tree.LT, Left: $3.unresolvedName(), Right: right}
  }
| CONNECT BY prior_expr '>' PRIOR prior_expr
  {
    right := $6.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Operator: tree.GT, Left: $3.unresolvedName(), Right: right}
  }
| CONNECT BY prior_expr '=' PRIOR prior_expr
  {
    right := $6.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Operator: tree.EQ, Left: $3.unresolvedName(), Right: right}
  }
| CONNECT BY prior_expr LESS_EQUALS PRIOR prior_expr
  {
    right := $6.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Operator: tree.LE, Left: $3.unresolvedName(), Right: right}
  }
| CONNECT BY prior_expr GREATER_EQUALS PRIOR prior_expr
  {
    right := $6.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Operator: tree.GE, Left: $3.unresolvedName(), Right: right}
  }
| CONNECT BY prior_expr NOT_EQUALS PRIOR prior_expr
  {
    right := $6.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Operator: tree.NE, Left: $3.unresolvedName(), Right: right}
  }

| CONNECT BY NOCYCLE PRIOR prior_expr '=' prior_expr
  {
   left := $5.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.EQ, Left: left, Right: $7.unresolvedName()}
  }
| CONNECT BY NOCYCLE PRIOR prior_expr LESS_EQUALS prior_expr
  {
   left := $5.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.LE, Left: left, Right: $7.unresolvedName()}
  }
| CONNECT BY NOCYCLE PRIOR prior_expr GREATER_EQUALS prior_expr
  {
   left := $5.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.GE, Left: left, Right: $7.unresolvedName()}
  }
| CONNECT BY NOCYCLE PRIOR prior_expr NOT_EQUALS prior_expr
  {
   left := $5.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.NE, Left: left, Right: $7.unresolvedName()}
  }
| CONNECT BY NOCYCLE PRIOR prior_expr '<' prior_expr
  {
   left := $5.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.LT, Left: left, Right: $7.unresolvedName()}
  }
| CONNECT BY NOCYCLE PRIOR prior_expr '>' prior_expr
  {
   left := $5.unresolvedName()
   left.Parts[1] = "recursive_temp"
   $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.GT, Left: left, Right: $7.unresolvedName()}
  }

| CONNECT BY NOCYCLE prior_expr '<' PRIOR prior_expr
  {
    right := $7.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.LT, Left: $4.unresolvedName(), Right: right}
  }
| CONNECT BY NOCYCLE prior_expr '>' PRIOR prior_expr
  {
    right := $7.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.GT, Left: $4.unresolvedName(), Right: right}
  }
| CONNECT BY NOCYCLE prior_expr '=' PRIOR prior_expr
  {
    right := $7.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.EQ, Left: $4.unresolvedName(), Right: right}
  }
| CONNECT BY NOCYCLE prior_expr LESS_EQUALS PRIOR prior_expr
  {
    right := $7.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.LE, Left: $4.unresolvedName(), Right: right}
  }
| CONNECT BY NOCYCLE prior_expr GREATER_EQUALS PRIOR prior_expr
  {
    right := $7.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.GE, Left: $4.unresolvedName(), Right: right}
  }
| CONNECT BY NOCYCLE prior_expr NOT_EQUALS PRIOR prior_expr
  {
    right := $7.unresolvedName()
    right.Parts[1] = "recursive_temp"
    $$.val = &tree.ComparisonExpr{Nocycle:true,Operator: tree.NE, Left: $4.unresolvedName(), Right: right}
  }

prior_expr:
   unrestricted_name
  {
    $$.val = &tree.UnresolvedName{NumParts:2, Parts: tree.NameParts{$1,"host"}}
  }
|  unrestricted_name '.' unrestricted_name
  {
    $$.val = &tree.UnresolvedName{NumParts:2, Parts: tree.NameParts{$1+"."+$3,"host"}}
  }

for_locking_clause:
  for_locking_items { $$.val = $1.lockingClause() }
| FOR READ ONLY     { $$.val = (tree.LockingClause)(nil) }

opt_for_locking_clause:
  for_locking_clause { $$.val = $1.lockingClause() }
| /* EMPTY */        { $$.val = (tree.LockingClause)(nil) }

for_locking_items:
  for_locking_item
  {
    $$.val = tree.LockingClause{$1.lockingItem()}
  }
| for_locking_items for_locking_item
  {
    $$.val = append($1.lockingClause(), $2.lockingItem())
  }

for_locking_item:
  for_locking_strength opt_locked_rels opt_nowait_or_skip
  {
    $$.val = &tree.LockingItem{
      Strength:   $1.lockingStrength(),
      Targets:    $2.tableNames(),
      WaitPolicy: $3.lockingWaitPolicy(),
    }
  }

for_locking_strength:
  FOR UPDATE        { $$.val = tree.ForUpdate }
| FOR NO KEY UPDATE { $$.val = tree.ForNoKeyUpdate }
| FOR SHARE         { $$.val = tree.ForShare }
| FOR KEY SHARE     { $$.val = tree.ForKeyShare }

opt_locked_rels:
  /* EMPTY */        { $$.val = tree.TableNames{} }
| OF table_name_list { $$.val = $2.tableNames() }

opt_nowait_or_skip:
  /* EMPTY */ { $$.val = tree.LockingWaitPolicy{LockingType: tree.LockWaitBlock}}
| SKIP LOCKED { $$.val = tree.LockingWaitPolicy{LockingType: tree.LockWaitSkip}}
| NOWAIT      { $$.val = tree.LockingWaitPolicy{LockingType: tree.LockWaitError}}
| WAIT iconst64     { $$.val = tree.LockingWaitPolicy{LockingType: tree.LockWait, WaitTime: $2.int64()}}

select_clause:
// We only provide help if an open parenthesis is provided, because
// otherwise the rule is ambiguous with the top-level statement list.
  '(' error // SHOW HELP: <SELECTCLAUSE>
| simple_select
| select_with_parens

// This rule parses SELECT statements that can appear within set operations,
// including UNION, INTERSECT and EXCEPT. '(' and ')' can be used to specify
// the ordering of the set operations. Without '(' and ')' we want the
// operations to be ordered per the precedence specs at the head of this file.
//
// As with select_no_parens, simple_select cannot have outer parentheses, but
// can have parenthesized subclauses.
//
// Note that sort clauses cannot be included at this level --- SQL requires
//       SELECT foo UNION SELECT bar ORDER BY baz
// to be parsed as
//       (SELECT foo UNION SELECT bar) ORDER BY baz
// not
//       SELECT foo UNION (SELECT bar ORDER BY baz)
//
// Likewise for WITH, FOR UPDATE and LIMIT. Therefore, those clauses are
// described as part of the select_no_parens production, not simple_select.
// This does not limit functionality, because you can reintroduce these clauses
// inside parentheses.
//
// NOTE: only the leftmost component select_stmt should have INTO. However,
// this is not checked by the grammar; parse analysis must check it.
//
// %Help: <SELECTCLAUSE> - access tabular data
// %Category: DML
// %Text:
// Select clause:
//   TABLE <tablename>
//   VALUES ( <exprs...> ) [ , ... ]
//   SELECT ... [ { INTERSECT | UNION | EXCEPT } [ ALL | DISTINCT ] <selectclause> ]
simple_select:
  simple_select_clause // EXTEND WITH HELP: SELECT
| values_clause_temp        // EXTEND WITH HELP: VALUES
| table_clause         // EXTEND WITH HELP: TABLE
| set_operation

values_clause_temp:
  values_clause
| values_clause AS table_alias_name opt_column_list
  {
    $$.val = $1.valuesClause()
    $$.val.(*tree.ValuesClause).Alias = tree.AliasClause{Alias:tree.Name($3), Cols:$4.nameList()}
  }

// %Help: SELECT - retrieve rows from a data source and compute a result
// %Category: DML
// %Text:
// SELECT [DISTINCT [ ON ( <expr> [ , ... ] ) ] ]
//        { <expr> [[AS] <name>] | [ [<dbname>.] <tablename>. ] * } [, ...]
//        [ FROM <source> ]
//        [ WHERE <expr> ]
//        [ GROUP BY <expr> [ , ... ] ]
//        [ HAVING <expr> ]
//        [ WINDOW <name> AS ( <definition> ) ]
//        [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] <selectclause> ]
//        [ ORDER BY <expr> [ ASC | DESC ] [, ...] ]
//        [ LIMIT { <expr> | ALL } ]
//        [ OFFSET <expr> [ ROW | ROWS ] ]
// %SeeAlso: WEBDOCS/select-clause.html
simple_select_clause:
  SELECT select_hint_set opt_all_clause target_list
    from_clause opt_where_clause
    group_clause having_clause window_clause
  {
    $$.val = &tree.SelectClause{
      HintSet: $2.hintset(),
      Exprs:   $4.selExprs(),
      From:    $5.from(),
      Where:   tree.NewWhere(tree.AstWhere, $6.expr()),
      GroupBy: $7.groupBy(),
      Having:  tree.NewWhere(tree.AstHaving, $8.expr()),
      Window:  $9.window(),
    }
  }
|SELECT select_hint_set opt_all_clause target_list
     from_clause opt_where_clause start_with connect_by
     group_clause having_clause window_clause
   {
     $$.val = &tree.SelectClause{
       HintSet: $2.hintset(),
       Exprs:   $4.selExprs(),
       From:    $5.from(),
       Where:   tree.NewWhere(tree.AstWhere, $6.expr()),
       StartWith: $7.expr(),
       ConnectBy: $8.expr(),
       GroupBy: $9.groupBy(),
       Having:  tree.NewWhere(tree.AstHaving, $10.expr()),
       Window:  $11.window(),
     }
   }
|SELECT select_hint_set opt_all_clause target_list
     from_clause opt_where_clause connect_by opt_start_with
     group_clause having_clause window_clause
   {
     $$.val = &tree.SelectClause{
       HintSet: $2.hintset(),
       Exprs:   $4.selExprs(),
       From:    $5.from(),
       Where:   tree.NewWhere(tree.AstWhere, $6.expr()),
       ConnectBy: $7.expr(),
       StartWith: $8.expr(),
       GroupBy: $9.groupBy(),
       Having:  tree.NewWhere(tree.AstHaving, $10.expr()),
       Window:  $11.window(),
     }
   }
| SELECT distinct_clause target_list
    from_clause opt_where_clause
    group_clause having_clause window_clause
  {
    $$.val = &tree.SelectClause{
      Distinct: $2.bool(),
      Exprs:    $3.selExprs(),
      From:     $4.from(),
      Where:    tree.NewWhere(tree.AstWhere, $5.expr()),
      GroupBy:  $6.groupBy(),
      Having:   tree.NewWhere(tree.AstHaving, $7.expr()),
      Window:   $8.window(),
    }
  }
| SELECT distinct_on_clause target_list
    from_clause opt_where_clause
    group_clause having_clause window_clause
  {
    $$.val = &tree.SelectClause{
      Distinct:   true,
      DistinctOn: $2.distinctOn(),
      Exprs:      $3.selExprs(),
      From:       $4.from(),
      Where:      tree.NewWhere(tree.AstWhere, $5.expr()),
      GroupBy:    $6.groupBy(),
      Having:     tree.NewWhere(tree.AstHaving, $7.expr()),
      Window:     $8.window(),
    }
  }
| SELECT error // SHOW HELP: SELECT

set_operation:
  select_clause UNION all_or_distinct select_clause
  {
    $$.val = &tree.UnionClause{
      Type:  tree.UnionOp,
      Left:  &tree.Select{Select: $1.selectStmt()},
      Right: &tree.Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
      if sel,ok := $$.val.(*tree.UnionClause).Left.Select.(*tree.SelectClause);ok{
              if sel.ConnectBy != nil{
              name := "recursive_temp"
              tableExpr := $$.val.(*tree.UnionClause).Left.Select.(*tree.SelectClause).From.Tables
              $$.val.(*tree.UnionClause).Left.With = tree.YaccBuildCET(sel,name,tableExpr)
              }
         }
      if sel,ok := $$.val.(*tree.UnionClause).Right.Select.(*tree.SelectClause);ok{
              if sel.ConnectBy != nil{
              name := "recursive_temp"
              tableExpr := $$.val.(*tree.UnionClause).Right.Select.(*tree.SelectClause).From.Tables
              $$.val.(*tree.UnionClause).Right.With = tree.YaccBuildCET(sel,name,tableExpr)
              }
         }
  }
| select_clause INTERSECT all_or_distinct select_clause
  {
    $$.val = &tree.UnionClause{
      Type:  tree.IntersectOp,
      Left:  &tree.Select{Select: $1.selectStmt()},
      Right: &tree.Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
  }
| select_clause EXCEPT all_or_distinct select_clause
  {
    $$.val = &tree.UnionClause{
      Type:  tree.ExceptOp,
      Left:  &tree.Select{Select: $1.selectStmt()},
      Right: &tree.Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
  }

// %Help: TABLE - select an entire table
// %Category: DML
// %Text: TABLE <tablename>
// %SeeAlso: SELECT, VALUES, WEBDOCS/table-expressions.html
table_clause:
  TABLE table_ref
  {
    $$.val = &tree.SelectClause{
      Exprs:       tree.SelectExprs{tree.StarSelectExpr()},
      From:        &tree.From{Tables: tree.TableExprs{$2.tblExpr()}},
      TableSelect: true,
    }
  }
| TABLE error // SHOW HELP: TABLE

// SQL standard WITH clause looks like:
//
// WITH [ RECURSIVE ] <query name> [ (<column> [, ...]) ]
//        AS (query) [ SEARCH or CYCLE clause ]
//
// We don't currently support the SEARCH or CYCLE clause.
//
// Recognizing WITH_LA here allows a CTE to be named TIME or ORDINALITY.
with_clause:
  WITH cte_list
  {
    $$.val = &tree.With{CTEList: $2.ctes()}
  }
| WITH_LA cte_list
  {
    /* SKIP DOC */
    $$.val = &tree.With{CTEList: $2.ctes()}
  }
| WITH RECURSIVE cte_list
  {
    $$.val = &tree.With{Recursive: true, CTEList: $3.ctes()}
  }

cte_list:
  common_table_expr
  {
    $$.val = []*tree.CTE{$1.cte()}
  }
| cte_list ',' common_table_expr
  {
    $$.val = append($1.ctes(), $3.cte())
  }

common_table_expr:
  table_alias_name opt_column_list AS '(' preparable_stmt ')'
  {
    $$.val = &tree.CTE{
      Name: tree.AliasClause{Alias: tree.Name($1), Cols: $2.nameList() },
      Stmt: $5.stmt(),
    }
  }

opt_with:
  WITH {}
| /* EMPTY */ {}

opt_with_clause:
  with_clause
  {
    $$.val = $1.with()
  }
| /* EMPTY */
  {
    $$.val = nil
  }

opt_table:
  TABLE {}
| /* EMPTY */ {}

all_or_distinct:
  ALL
  {
    $$.val = true
  }
| DISTINCT
  {
    $$.val = false
  }
| /* EMPTY */
  {
    $$.val = false
  }

distinct_clause:
  DISTINCT
  {
    $$.val = true
  }

distinct_on_clause:
  DISTINCT ON '(' expr_list ')'
  {
    $$.val = tree.DistinctOn($4.exprs())
  }

opt_all_clause:
ALL {}
| /* EMPTY */ {}

select_hint_set:
'/' '*' '+' hint_clause '*' '/'
  {
   $$.val = $4.hintset()
  }
| /* EMPTY */ {$$.val = (tree.HintSet)(nil)}

hint_clause:
  hint_single
  {
    $$.val = tree.HintSet{$1.hint()}
  }
| hint_clause ',' hint_single
  {
    $$.val = append($1.hintset(), $3.hint())
  }

hint_single:
  HASH_JOIN '(' table_name_list ')'
  {
    $$.val = &tree.TableHint{
      HintName: "hash_join",
      Tables: $3.tableNames(),
    }
  }
| HASH_JOIN '(' '@' param_name table_name '@' param_name ',' table_name_list ')'
  {
    qbname1, qbname2 := $4, $7
    if qbname1 != qbname2 {
      sqllex.Error(fmt.Sprintf("query block names %s and %s are not identical", qbname1, qbname2))
      return 1
    }
    $$.val = &tree.TableHint{
      HintName: "hash_join",
      HintData: $5.unresolvedObjectName().ToTableName(),
      QBName: $4,
      Tables: $9.tableNames(),
    }
  }
| LOOKUP_JOIN '(' table_name_list ')'
  {
    $$.val = &tree.TableHint{
      HintName: "lookup_join",
      Tables: $3.tableNames(),
    }
  }
| LOOKUP_JOIN '(' '@' param_name table_name '@' param_name ',' table_name_list ')'
  {
    qbname1, qbname2 := $4, $7
    if qbname1 != qbname2 {
      sqllex.Error(fmt.Sprintf("query block names %s and %s are not identical", qbname1, qbname2))
      return 1
    }
    $$.val = &tree.TableHint{
      HintName: "lookup_join",
      HintData: $5.unresolvedObjectName().ToTableName(),
      QBName: $4,
      Tables: $9.tableNames(),
    }
  }
| MERGE_JOIN '(' table_name_list ')'
  {
    $$.val = &tree.TableHint{
      HintName: "merge_join",
      Tables: $3.tableNames(),
    }
  }
| MERGE_JOIN '(' '@' param_name table_name '@' param_name ',' table_name_list ')'
  {
    qbname1, qbname2 := $4, $7
    if qbname1 != qbname2 {
      sqllex.Error(fmt.Sprintf("query block names %s and %s are not identical", qbname1, qbname2))
      return 1
    }
    $$.val = &tree.TableHint{
      HintName: "merge_join",
      HintData: $5.unresolvedObjectName().ToTableName(),
      QBName: $4,
      Tables: $9.tableNames(),
    }
  }
| NTH_PLAN '(' iconst64 ')'
  {
    $$.val = &tree.TableHint{
      HintName: "nth_plan",
      HintData: int($3.int64()),
    }
  }
| QB_NAME '(' param_name ')'
  {
    $$.val = &tree.TableHint{
      HintName: "qb_name",
      QBName: $3,
    }
  }

| READ_FROM_STORAGE '(' storage_clause ')'
  {
    $$.val = &tree.TableHint{
      HintName: "read_from_storage",
      HintData: $3.readFromStorageList(),
    }
  }
| USE_INDEX '(' table_name ',' table_index_name_list ')'
  {
    $$.val = &tree.IndexHint{
      IndexHintType: keys.IndexHintUse,
      TableName: $3.unresolvedObjectName().ToTableName(),
      IndexNames: $5.newTableIndexNames(),
    }
  }
| IGNORE_INDEX '(' table_name ',' table_index_name_list ')'
  {
    $$.val = &tree.IndexHint{
    IndexHintType: keys.IndexHintIgnore,
    TableName: $3.unresolvedObjectName().ToTableName(),
    IndexNames: $5.newTableIndexNames(),
    }
  }
| FORCE_INDEX '(' table_name ',' table_index_name ')'
  {
    $$.val = &tree.IndexHint{
    IndexHintType: keys.IndexHintForce,
    TableName: $3.unresolvedObjectName().ToTableName(),
    IndexNames: tree.TableIndexNames{$5.newTableIndexName()},
    }
  }

storage_clause:
  storage_single
  {
    $$.val = tree.ReadFromStorageList{$1.readFromStorage()}
  }
| storage_clause ',' storage_single
  {
    $$.val = append($1.readFromStorageList(), $3.readFromStorage())
  }

storage_single:
  COLENGINEREAD '[' table_name_list']'
  {
    $$.val = &tree.ReadFromStorage{
      EngineType: keys.ColEngineRead,
      TableNames: $3.tableNames(),
    }
  }
| ROWENGINEREAD '[' table_name_list']'
  {
    $$.val = &tree.ReadFromStorage{
      EngineType: keys.RowEngineRead,
      TableNames: $3.tableNames(),
    }
  }

opt_sort_clause_err:
  sort_clause { return unimplementedWithIssue(sqllex, 23620) }
| /* EMPTY */ {}

opt_sort_clause:
  sort_clause
  {
    $$.val = $1.orderBy()
  }
| /* EMPTY */
  {
    $$.val = tree.OrderBy(nil)
  }

sort_clause:
  ORDER BY sortby_list
  {
    $$.val = tree.OrderBy($3.orders())
  }

sortby_list:
  sortby
  {
    $$.val = []*tree.Order{$1.order()}
  }
| sortby_list ',' sortby
  {
    $$.val = append($1.orders(), $3.order())
  }

sortby:
  a_expr opt_asc_desc
  {
    $$.val = &tree.Order{OrderType: tree.OrderByColumn, Expr: $1.expr(), Direction: $2.dir()}
  }
| PRIMARY KEY table_name opt_asc_desc
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Order{OrderType: tree.OrderByIndex, Direction: $4.dir(), Table: name}
  }
| INDEX table_name '@' index_name opt_asc_desc
  {
    name := $2.unresolvedObjectName().ToTableName()
    $$.val = &tree.Order{
      OrderType: tree.OrderByIndex,
      Direction: $5.dir(),
      Table:     name,
      Index:     tree.UnrestrictedName($4),
    }
  }

// TODO(pmattis): Support ordering using arbitrary math ops?
// | a_expr USING math_op {}

select_limit:
  limit_clause offset_clause
  {
    if $1.limit() == nil {
      $$.val = $2.limit()
    } else {
      $$.val = $1.limit()
      $$.val.(*tree.Limit).Offset = $2.limit().Offset
    }
  }
| offset_clause limit_clause
  {
    $$.val = $1.limit()
    if $2.limit() != nil {
      $$.val.(*tree.Limit).Count = $2.limit().Count
    }
  }
| LIMIT select_limit_value ',' select_limit_value
  {
    $$.val = &tree.Limit{Count: $4.expr(), Offset: $2.expr()}
  }
| limit_clause
| offset_clause

opt_select_limit:
  select_limit { $$.val = $1.limit() }
| /* EMPTY */  { $$.val = (*tree.Limit)(nil) }

opt_limit_clause:
  limit_clause
| /* EMPTY */ { $$.val = (*tree.Limit)(nil) }

limit_clause:
  LIMIT select_limit_value
  {
    if $2.expr() == nil {
      $$.val = (*tree.Limit)(nil)
    } else {
      $$.val = &tree.Limit{Count: $2.expr()}
    }
  }
// SQL:2008 syntax
| FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY
  {
    $$.val = &tree.Limit{Count: $3.expr()}
  }

offset_clause:
  OFFSET a_expr
  {
    $$.val = &tree.Limit{Offset: $2.expr()}
  }
  // SQL:2008 syntax
  // The trailing ROW/ROWS in this case prevent the full expression
  // syntax. c_expr is the best we can do.
| OFFSET c_expr row_or_rows
  {
    $$.val = &tree.Limit{Offset: $2.expr()}
  }

select_limit_value:
  a_expr
| ALL
  {
    $$.val = tree.Expr(nil)
  }

// Allowing full expressions without parentheses causes various parsing
// problems with the trailing ROW/ROWS key words. SQL only calls for constants,
// so we allow the rest only with parentheses. If omitted, default to 1.
 opt_select_fetch_first_value:
   signed_iconst
   {
     $$.val = $1.expr()
   }
 | '(' a_expr ')'
   {
     $$.val = $2.expr()
   }
 | /* EMPTY */
   {
     $$.val = &tree.NumVal{Value: constant.MakeInt64(1)}
   }

// noise words
row_or_rows:
  ROW {}
| ROWS {}

first_or_next:
  FIRST {}
| NEXT {}

// This syntax for group_clause tries to follow the spec quite closely.
// However, the spec allows only column references, not expressions,
// which introduces an ambiguity between implicit row constructors
// (a,b) and lists of column references.
//
// We handle this by using the a_expr production for what the spec calls
// <ordinary grouping set>, which in the spec represents either one column
// reference or a parenthesized list of column references. Then, we check the
// top node of the a_expr to see if it's an RowExpr, and if so, just grab and
// use the list, discarding the node. (this is done in parse analysis, not here)
//
// Each item in the group_clause list is either an expression tree or a
// GroupingSet node of some type.
group_clause:
  GROUP BY expr_list
  {
    $$.val = tree.GroupBy($3.exprs())
  }
| /* EMPTY */
  {
    $$.val = tree.GroupBy(nil)
  }

having_clause:
  HAVING a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

// Given "VALUES (a, b)" in a table expression context, we have to
// decide without looking any further ahead whether VALUES is the
// values clause or a set-generating function. Since VALUES is allowed
// as a function name both interpretations are feasible. We resolve
// the shift/reduce conflict by giving the first values_clause
// production a higher precedence than the VALUES token has, causing
// the parser to prefer to reduce, in effect assuming that the VALUES
// is not a function name.
//
// %Help: VALUES - select a given set of values
// %Category: DML
// %Text: VALUES ( <exprs...> ) [, ...]
// %SeeAlso: SELECT, TABLE, WEBDOCS/table-expressions.html
values_clause:
  VALUES '(' expr_list ')' %prec UMINUS
  {
    $$.val = &tree.ValuesClause{Rows: []tree.Exprs{$3.exprs()}}
  }
| VALUES error // SHOW HELP: VALUES
| values_clause ',' '(' expr_list ')'
  {
    valNode := $1.selectStmt().(*tree.ValuesClause)
    valNode.Rows = append(valNode.Rows, $4.exprs())
    $$.val = valNode
  }

values_clause_merge:
  VALUES '(' expr_list ')' %prec UMINUS
  {
    $$.val = &tree.ValuesClause{Rows: []tree.Exprs{$3.exprs()}}
  }
| VALUES error // SHOW HELP: VALUES

// clauses common to all optimizable statements:
//  from_clause   - allow list of both JOIN expressions and table names
//  where_clause  - qualifications for joins or restrictions

from_clause:
  FROM from_list opt_as_of_clause
  {
    $$.val = &tree.From{Tables: $2.tblExprs(), AsOf: $3.asOfClause()}
  }
|
  FROM from_list as_snapshot_clause
  {
    $$.val = &tree.From{Tables: $2.tblExprs(), AsSnapshot: $3.asSnapshotClause()}
  }
| FROM error // SHOW HELP: <SOURCE>
| /* EMPTY */
  {
    $$.val = &tree.From{}
  }

from_list:
  table_ref
  {
    $$.val = tree.TableExprs{$1.tblExpr()}
  }
| from_list ',' table_ref
  {
    $$.val = append($1.tblExprs(), $3.tblExpr())
  }

index_flags_param:
  FORCE_INDEX '=' index_name
  {
     $$.val = &tree.IndexFlags{Index: tree.UnrestrictedName($3)}
  }
| FORCE_INDEX '=' '[' iconst64 ']'
  {
    /* SKIP DOC */
    $$.val = &tree.IndexFlags{IndexID: tree.IndexID($4.int64())}
  }
| ASC
  {
    /* SKIP DOC */
    $$.val = &tree.IndexFlags{Direction: tree.Ascending}
  }
| DESC
  {
    /* SKIP DOC */
    $$.val = &tree.IndexFlags{Direction: tree.Descending}
  }
|
  NO_INDEX_JOIN
  {
     $$.val = &tree.IndexFlags{NoIndexJoin: true}
  }

index_flags_param_list:
  index_flags_param
  {
    $$.val = $1.indexFlags()
  }
|
  index_flags_param_list ',' index_flags_param
  {
    a := $1.indexFlags()
    b := $3.indexFlags()
    if err := a.CombineWith(b); err != nil {
      return setErr(sqllex, err)
    }
    $$.val = a
  }

opt_index_flags:
  '@' index_name
  {
    $$.val = &tree.IndexFlags{Index: tree.UnrestrictedName($2)}
  }
| '@' '[' iconst64 ']'
  {
    $$.val = &tree.IndexFlags{IndexID: tree.IndexID($3.int64())}
  }
| '@' '{' index_flags_param_list '}'
  {
    flags := $3.indexFlags()
    if err := flags.Check(); err != nil {
      return setErr(sqllex, err)
    }
    $$.val = flags
  }
| /* EMPTY */
  {
    $$.val = (*tree.IndexFlags)(nil)
  }

// %Help: <SOURCE> - define a data source for SELECT
// %Category: DML
// %Text:
// Data sources:
//   <tablename> [ @ { <idxname> | <indexflags> } ]
//   <tablefunc> ( <exprs...> )
//   ( { <selectclause> | <source> } )
//   <source> [AS] <alias> [( <colnames...> )]
//   <source> [ <jointype> ] JOIN <source> ON <expr>
//   <source> [ <jointype> ] JOIN <source> USING ( <colnames...> )
//   <source> NATURAL [ <jointype> ] JOIN <source>
//   <source> CROSS JOIN <source>
//   <source> WITH ORDINALITY
//   '[' EXPLAIN ... ']'
//   '[' SHOW ... ']'
//
// Index flags:
//   '{' FORCE_INDEX = <idxname> [, ...] '}'
//   '{' NO_INDEX_JOIN [, ...] '}'
//
// Join types:
//   { INNER | { LEFT | RIGHT | FULL } [OUTER] } [ { HASH | MERGE | LOOKUP } ]
//
// %SeeAlso: WEBDOCS/table-expressions.html
table_ref:
  '[' iconst64 opt_tableref_col_list alias_clause ']' opt_index_flags opt_ordinality opt_alias_clause
  {
    /* SKIP DOC */
    $$.val = &tree.AliasedTableExpr{
        Expr: &tree.TableRef{
           TableID: $2.int64(),
           Columns: $3.tableRefCols(),
           As:      $4.aliasClause(),
        },
        IndexFlags: $6.indexFlags(),
        Ordinality: $7.bool(),
        As:         $8.aliasClause(),
    }
  }
| '[' ONLY iconst64 opt_tableref_col_list alias_clause ']' opt_index_flags opt_ordinality opt_alias_clause
  {
    /* SKIP DOC */
    $$.val = &tree.AliasedTableExpr{
        Expr: &tree.TableRef{
           IsOnly: true,
           TableID: $3.int64(),
           Columns: $4.tableRefCols(),
           As:      $5.aliasClause(),
        },
        IndexFlags: $7.indexFlags(),
        Ordinality: $8.bool(),
        As:         $9.aliasClause(),
    }
  }
| relation_expr opt_index_flags opt_ordinality opt_alias_clause
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = &tree.AliasedTableExpr{
      Expr:       &name,
      IndexFlags: $2.indexFlags(),
      Ordinality: $3.bool(),
      As:         $4.aliasClause(),
    }
  }
| select_with_parens opt_ordinality opt_alias_clause
  {
    $$.val = &tree.AliasedTableExpr{
      Expr:       &tree.Subquery{Select: $1.selectStmt()},
      Ordinality: $2.bool(),
      As:         $3.aliasClause(),
    }
  }
| LATERAL select_with_parens opt_ordinality opt_alias_clause { return unimplementedWithIssueDetail(sqllex, 24560, "select") }
| joined_table
  {
    $$.val = $1.tblExpr()
  }
| '(' joined_table ')' ordinality alias_clause
  {
    $$.val = &tree.AliasedTableExpr{Expr: &tree.ParenTableExpr{Expr: $2.tblExpr()}, Ordinality: $4.bool(), As: $5.aliasClause()}
  }
| '(' joined_table ')' alias_clause
  {
    $$.val = &tree.AliasedTableExpr{Expr: &tree.ParenTableExpr{Expr: $2.tblExpr()}, Ordinality: false, As: $4.aliasClause()}
  }
| func_table opt_ordinality opt_alias_clause
  {
    f := $1.tblExpr()
    $$.val = &tree.AliasedTableExpr{Expr: f, Ordinality: $2.bool(), As: $3.aliasClause()}
  }
| LATERAL func_table opt_ordinality opt_alias_clause { return unimplementedWithIssueDetail(sqllex, 24560, "srf") }
// The following syntax is a ZNBaseDB extension:
//     SELECT ... FROM [ EXPLAIN .... ] WHERE ...
//     SELECT ... FROM [ SHOW .... ] WHERE ...
//     SELECT ... FROM [ INSERT ... RETURNING ... ] WHERE ...
// A statement within square brackets can be used as a table expression (data source).
// We use square brackets for two reasons:
// - the grammar would be terribly ambiguous if we used simple
//   parentheses or no parentheses at all.
// - it carries visual semantic information, by marking the table
//   expression as radically different from the other things.
//   If a user does not know this and encounters this syntax, they
//   will know from the unusual choice that something rather different
//   is going on and may be pushed by the unusual syntax to
//   investigate further in the docs.
| '[' preparable_stmt ']' opt_ordinality opt_alias_clause
  {
    $$.val = &tree.AliasedTableExpr{Expr: &tree.StatementSource{ Statement: $2.stmt() }, Ordinality: $4.bool(), As: $5.aliasClause() }
  }
| '[' PARTITION partition_name ']' OF relation_expr
  {
    name := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.AliasedTableExpr{Expr: &tree.PartitionExpr{PartitionName: tree.Name($3), TableName: name}}
  }
| '[' PARTITION partition_name DEFAULT']' OF relation_expr
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.AliasedTableExpr{Expr: &tree.PartitionExpr{PartitionName: tree.Name($3), TableName: name, IsDefault: true}}
  }

func_table:
  func_expr_windowless
  {
    $$.val = &tree.RowsFromExpr{Items: tree.Exprs{$1.expr()}}
  }
| ROWS FROM '(' rowsfrom_list ')'
  {
    $$.val = &tree.RowsFromExpr{Items: $4.exprs()}
  }

rowsfrom_list:
  rowsfrom_item
  { $$.val = tree.Exprs{$1.expr()} }
| rowsfrom_list ',' rowsfrom_item
  { $$.val = append($1.exprs(), $3.expr()) }

rowsfrom_item:
  func_expr_windowless opt_col_def_list
  {
    $$.val = $1.expr()
  }

opt_col_def_list:
  /* EMPTY */
  { }
| AS '(' error
  { return unimplemented(sqllex, "ROWS FROM with col_def_list") }

opt_tableref_col_list:
  /* EMPTY */               { $$.val = nil }
| '(' ')'                   { $$.val = []tree.ColumnID{} }
| '(' tableref_col_list ')' { $$.val = $2.tableRefCols() }

tableref_col_list:
  iconst64
  {
    $$.val = []tree.ColumnID{tree.ColumnID($1.int64())}
  }
| tableref_col_list ',' iconst64
  {
    $$.val = append($1.tableRefCols(), tree.ColumnID($3.int64()))
  }

opt_ordinality:
  WITH_LA ORDINALITY
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }
ordinality:
WITH_LA ORDINALITY
  {
    $$.val = true
  }
// It may seem silly to separate joined_table from table_ref, but there is
// method in SQL's madness: if you don't do it this way you get reduce- reduce
// conflicts, because it's not clear to the parser generator whether to expect
// alias_clause after ')' or not. For the same reason we must treat 'JOIN' and
// 'join_type JOIN' separately, rather than allowing join_type to expand to
// empty; if we try it, the parser generator can't figure out when to reduce an
// empty join_type right after table_ref.
//
// Note that a CROSS JOIN is the same as an unqualified INNER JOIN, and an
// INNER JOIN/ON has the same shape but a qualification expression to limit
// membership. A NATURAL JOIN implicitly matches column names between tables
// and the shape is determined by which columns are in common. We'll collect
// columns during the later transformations.

joined_table:
  '(' joined_table ')'
  {
    $$.val = &tree.ParenTableExpr{Expr: $2.tblExpr()}
  }
| table_ref CROSS opt_join_hint JOIN table_ref
  {
    $$.val = &tree.JoinTableExpr{JoinType: tree.AstCross, Left: $1.tblExpr(), Right: $5.tblExpr(), Hint: $3}
  }
| table_ref join_type opt_join_hint JOIN table_ref join_qual
  {
    $$.val = &tree.JoinTableExpr{JoinType: $2, Left: $1.tblExpr(), Right: $5.tblExpr(), Cond: $6.joinCond(), Hint: $3}
  }
| table_ref JOIN table_ref join_qual
  {
    $$.val = &tree.JoinTableExpr{Left: $1.tblExpr(), Right: $3.tblExpr(), Cond: $4.joinCond()}
  }
| table_ref NATURAL join_type opt_join_hint JOIN table_ref
  {
    $$.val = &tree.JoinTableExpr{JoinType: $3, Left: $1.tblExpr(), Right: $6.tblExpr(), Cond: tree.NaturalJoinCond{}, Hint: $4}
  }
| table_ref NATURAL JOIN table_ref
  {
    $$.val = &tree.JoinTableExpr{Left: $1.tblExpr(), Right: $4.tblExpr(), Cond: tree.NaturalJoinCond{}}
  }

alias_clause:
  AS table_alias_name opt_column_list
  {
    $$.val = tree.AliasClause{Alias: tree.Name($2), Cols: $3.nameList()}
  }
| table_alias_name opt_column_list
  {
    $$.val = tree.AliasClause{Alias: tree.Name($1), Cols: $2.nameList()}
  }

opt_alias_clause:
  alias_clause
| /* EMPTY */%prec CONCAT
  {
    $$.val = tree.AliasClause{}
  }

as_of_clause:
  AS_LA OF SYSTEM TIME a_expr
  {
    $$.val = tree.AsOfClause{Expr: $5.expr()}
  }

opt_as_of_clause:
  as_of_clause
| /* EMPTY */
  {
    $$.val = tree.AsOfClause{}
  }

as_snapshot_clause:
  AS_LA OF SNAPSHOT name
  {
    $$.val = tree.AsSnapshotClause{Snapshot: tree.Name($4)}
  }

join_type:
  FULL join_outer
  {
    $$ = tree.AstFull
  }
| LEFT join_outer
  {
    $$ = tree.AstLeft
  }
| RIGHT join_outer
  {
    $$ = tree.AstRight
  }
| INNER
  {
    $$ = tree.AstInner
  }

// OUTER is just noise...
join_outer:
  OUTER {}
| /* EMPTY */ {}


// Join hint specifies that the join in the query should use a
// specific method.

// The semantics are as follows:
//  - HASH forces a hash join; in other words, it disables merge and lookup
//    join. A hash join is always possible; even if there are no equality
//    columns - we consider cartesian join a degenerate case of the hash join
//    (one bucket).
//  - MERGE forces a merge join, even if it requires resorting both sides of
//    the join.
//  - LOOKUP forces a lookup join into the right side; the right side must be
//    a table with a suitable index. `LOOKUP` can only be used with INNER and
//    LEFT joins (though this is not enforced by the syntax).
//  - If it is not possible to use the algorithm in the hint, an error is
//    returned.
//  - When a join hint is specified, the two tables will not be reordered
//    by the optimizer.
opt_join_hint:
  HASH
  {
    $$ = tree.AstHash
  }
| MERGE
  {
    $$ = tree.AstMerge
  }
| LOOKUP
  {
    $$ = tree.AstLookup
  }
| /* EMPTY */
  {
    $$ = ""
  }

// JOIN qualification clauses
// Possibilities are:
//      USING ( column list ) allows only unqualified column names,
//          which must match between tables.
//      ON expr allows more general qualifications.
//
// We return USING as a List node, while an ON-expr will not be a List.
join_qual:
  USING '(' name_list ')'
  {
    $$.val = &tree.UsingJoinCond{Cols: $3.nameList()}
  }
| ON a_expr
  {
    $$.val = &tree.OnJoinCond{Expr: $2.expr()}
  }

relation_expr:
  table_name              { $$.val = $1.unresolvedObjectName() }
| table_name '*'          { $$.val = $1.unresolvedObjectName() }
| ONLY table_name
  {
    name := $2.unresolvedObjectName()
    name.IsOnly = true
    $$.val = name
  }
| ONLY '(' table_name ')'
  {
    name := $3.unresolvedObjectName()
    name.IsOnly = true
    $$.val = name
  }

relation_expr_list:
  relation_expr
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = tree.TableNames{name}
  }
| relation_expr_list ',' relation_expr
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = append($1.tableNames(), name)
  }

// Given "UPDATE foo set set ...", we have to decide without looking any
// further ahead whether the first "set" is an alias or the UPDATE's SET
// keyword. Since "set" is allowed as a column name both interpretations are
// feasible. We resolve the shift/reduce conflict by giving the first
// table_name_expr_opt_alias_idx production a higher precedence than the SET token
// has, causing the parser to prefer to reduce, in effect assuming that the SET
// is not an alias.
table_name_expr_opt_alias_idx:
  table_name_expr_with_index %prec UMINUS
  {
     $$.val = $1.tblExpr()
  }
| table_name_expr_with_index table_alias_name
  {
     alias := $1.tblExpr().(*tree.AliasedTableExpr)
     alias.As = tree.AliasClause{Alias: tree.Name($2)}
     $$.val = alias
  }
| table_name_expr_with_index AS table_alias_name
  {
     alias := $1.tblExpr().(*tree.AliasedTableExpr)
     alias.As = tree.AliasClause{Alias: tree.Name($3)}
     $$.val = alias
  }

table_name_expr_with_index:
  table_name opt_index_flags
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = &tree.AliasedTableExpr{
      Expr: &name,
      IndexFlags: $2.indexFlags(),
    }
  }

where_clause:
  WHERE a_expr
  {
    $$.val = $2.expr()
  }

opt_where_clause:
  where_clause
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

opt_where_or_current_clause:
  where_clause
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }
| where_current_of
  {
    $$.val = $1.expr()
  }

// Type syntax
//   SQL introduces a large amount of type-specific syntax.
//   Define individual clauses to handle these cases, and use
//   the generic case to handle regular type-extensible Postgres syntax.
//   - thomas 1997-10-10

typename:
  simple_typename opt_array_bounds
  {
    if bounds := $2.int32s(); bounds != nil {
      var err error
      $$.val, err = coltypes.ArrayOf($1.colType(), bounds)
      if err != nil {
        return setErr(sqllex, err)
      }
    } else {
      $$.val = $1.colType()
    }
  }
| enum_typename '(' opt_enum ')'
  {
    if bounds := $3.strs(); bounds != nil {
      var err error
      $$.val, err = coltypes.EnumArrayOf($1.colType(), bounds)
      if err != nil {
        return setErr(sqllex, err)
      }
    } else {
      sqllex.Error("Enum must have content")
      return 1
    }
  }
| set_typename '(' opt_set ')'
   {
     if bounds := $3.strs(); bounds != nil {
       var err error
       $$.val, err = coltypes.SetArrayOf($1.colType(), bounds)
       if err != nil {
         return setErr(sqllex, err)
       }
     } else {
       sqllex.Error("Set must have content")
       return 1
     }
   }
  // SQL standard syntax, currently only one-dimensional
  // Undocumented but support for potential Postgres compat
| simple_typename ARRAY '[' ICONST ']' {
    /* SKIP DOC */
    var err error
    $$.val, err = coltypes.ArrayOf($1.colType(), []int32{-1})
    if err != nil {
      return setErr(sqllex, err)
    }
  }
| simple_typename ARRAY '[' ICONST ']' '[' error { return unimplementedWithIssue(sqllex, 32552) }
| simple_typename ARRAY {
    var err error
    $$.val, err = coltypes.ArrayOf($1.colType(), []int32{-1})
    if err != nil {
      return setErr(sqllex, err)
    }
  }
| postgres_oid
  {
    $$.val = $1.castTargetType()
  }

cast_target:
  typename
  {
    $$.val = $1.colType()
  }

opt_array_bounds:
  // TODO(justin): reintroduce multiple array bounds
  // opt_array_bounds '[' ']' { $$.val = append($1.int32s(), -1) }
  '[' ']' { $$.val = []int32{-1} }
| '[' ']' '[' error { return unimplementedWithIssue(sqllex, 32552) }
| '[' ICONST ']'
  {
    /* SKIP DOC */
    bound, err := $2.numVal().AsInt32()
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = []int32{bound}
  }
| '[' ICONST ']' '[' error { return unimplementedWithIssue(sqllex, 32552) }
| /* EMPTY */ { $$.val = []int32(nil) }

opt_enum:
  SCONST
  {
    $$.val = []string{$1}
  }
| opt_enum ',' SCONST
  {
    $$.val = append($1.strs(), $3)
  }

enum_typename:
  enum
  {
    $$.val = coltypes.String
  }

enum:
  ENUM

opt_set:
 SCONST
  {
    $$.val = []string{$1}
  }
| opt_set ',' SCONST
  {
    $$.val = append($1.strs(), $3)
  }

set_typename:
  set
  {
    $$.val = coltypes.String
  }

set:
  SET


json:
  JSON

jsonb:
  JSONB

simple_typename:
  const_typename
| bit_with_length
| character_with_length
| const_interval
| const_interval interval_qualifier { return unimplemented(sqllex, "interval with unit qualifier") }
| const_interval '(' ICONST ')' { return unimplementedWithIssue(sqllex, 32564) }

// We have a separate const_typename to allow defaulting fixed-length types
// such as CHAR() and BIT() to an unspecified length. SQL9x requires that these
// default to a length of one, but this makes no sense for constructs like CHAR
// 'hi' and BIT '0101', where there is an obvious better choice to make. Note
// that const_interval is not included here since it must be pushed up higher
// in the rules to accommodate the postfix options (e.g. INTERVAL '1'
// YEAR). Likewise, we have to handle the generic-type-name case in
// a_expr_const to avoid premature reduce/reduce conflicts against function
// names.
const_typename:
  numeric
| bit_without_length
| character_without_length
| const_datetime
| json
  {
    $$.val = coltypes.JSON
  }
| jsonb
  {
    $$.val = coltypes.JSONB
  }
| BLOB
  {
    $$.val = coltypes.Blob
  }
| CLOB
  {
    $$.val = coltypes.Clob
  }
| BYTES
  {
    $$.val = coltypes.Bytes
  }
| BYTEA
  {
    $$.val = coltypes.Bytea
  }
| TEXT
  {
    $$.val = coltypes.Text
  }
| NAME
  {
    $$.val = coltypes.Name
  }
| SERIAL
  {
    $$.val = &coltypes.TSerial{
    	TInt: &coltypes.TInt{Width: sqllex.(*lexer).nakedSerialType.Width, VisibleType: coltypes.VisSERIAL},
    }
  }
| SERIAL2
  {
    $$.val = coltypes.Serial2
  }
| SMALLSERIAL
  {
    $$.val = coltypes.SmallSerial
  }
| SERIAL4
  {
    $$.val = coltypes.Serial4
  }
| SERIAL8
  {
    $$.val = coltypes.Serial8
  }
| BIGSERIAL
  {
    $$.val = coltypes.BigSerial
  }
| UUID
  {
    $$.val = coltypes.UUID
  }
| INET
  {
    $$.val = coltypes.INet
  }
| OID
  {
    $$.val = coltypes.Oid
  }
| OIDVECTOR
  {
    $$.val = coltypes.OidVector
  }
| INT2VECTOR
  {
    $$.val = coltypes.Int2vector
  }
| IDENT
  {
    /* FORCE DOC */
    // See https://www.postgresql.org/docs/9.1/static/datatype-character.html
    // Postgres supports a special character type named "char" (with the quotes)
    // that is a single-character column type. It's used by system tables.
    // Eventually this clause will be used to parse user-defined types as well,
    // since their names can be quoted.
    if $1 == "char" {
      $$.val = coltypes.QChar
    } else {
      var ok bool
      var unimp int
      $$.val, ok, unimp = coltypes.TypeForNonKeywordTypeName($1)
      if !ok {
          switch unimp {
              case 0:
                // Note: we can only report an unimplemented error for specific
                // known type names. Anything else may return PII.
                sqllex.Error("type does not exist")
                return 1
              case -1:
                return unimplemented(sqllex, "type name " + $1)
              default:
                return unimplementedWithIssueDetail(sqllex, unimp, $1)
          }
      }
    }
  }

opt_dec_modifiers:
  '(' iconst64 ')'
  {
    $$.val = &coltypes.TDecimal{Prec: int($2.int64()), VisibleType: coltypes.VisDEC}
  }
| '(' iconst64 ',' iconst64 ')'
  {
    $$.val = &coltypes.TDecimal{Prec: int($2.int64()), Scale: int($4.int64()), VisibleType: coltypes.VisDEC}
  }
| /* EMPTY */
  {
    $$.val = nil
  }

opt_decimal_modifiers:
  '(' iconst64 ')'
  {
    $$.val = &coltypes.TDecimal{Prec: int($2.int64()), VisibleType: coltypes.VisDECIMAL}
  }
| '(' iconst64 ',' iconst64 ')'
  {
    $$.val = &coltypes.TDecimal{Prec: int($2.int64()), Scale: int($4.int64()), VisibleType: coltypes.VisDECIMAL}
  }
| /* EMPTY */
  {
    $$.val = nil
  }

opt_numeric_modifiers:
  '(' iconst64 ')'
  {
    $$.val = &coltypes.TDecimal{Prec: int($2.int64()), VisibleType: coltypes.VisNUMERIC}
  }
| '(' iconst64 ',' iconst64 ')'
  {
    $$.val = &coltypes.TDecimal{Prec: int($2.int64()), Scale: int($4.int64()), VisibleType: coltypes.VisNUMERIC}
  }
| /* EMPTY */
  {
    $$.val = nil
  }

// SQL numeric data types
numeric:
  INT
  {
    $$.val = &coltypes.TInt{Width: sqllex.(*lexer).nakedIntType.Width, VisibleType: coltypes.VisINT}
  }
| INTEGER
  {
    $$.val = &coltypes.TInt{Width: sqllex.(*lexer).nakedIntType.Width, VisibleType: coltypes.VisINTEGER}
  }
| INT2
  {
    $$.val = coltypes.Int2
  }
| SMALLINT
  {
    $$.val = coltypes.SmallInt
  }
| INT4
  {
    $$.val = coltypes.Int4
  }
| INT8
  {
    $$.val = coltypes.Int8
  }
| INT64
  {
    $$.val = coltypes.Int64
  }
| BIGINT
  {
    $$.val = coltypes.BigInt
  }
| REAL
  {
    $$.val = coltypes.Real
  }
| FLOAT4
    {
      $$.val = coltypes.Float4
    }
| FLOAT8
    {
      $$.val = coltypes.Float8
    }
| FLOAT opt_float
  {
    $$.val = $2.colType()
  }
| DOUBLE PRECISION
  {
    $$.val = coltypes.Double
  }
| DECIMAL opt_decimal_modifiers
  {
    $$.val = $2.colType()
    if $$.val == nil {
      $$.val = coltypes.Decimal
    }
  }
| DEC opt_dec_modifiers
  {
    $$.val = $2.colType()
    if $$.val == nil {
      $$.val = coltypes.Dec
    }
  }
| NUMERIC opt_numeric_modifiers
  {
    $$.val = $2.colType()
    if $$.val == nil {
      $$.val = coltypes.Numeric
    }
  }
| BOOLEAN
  {
    $$.val = coltypes.Boolean
  }
| BOOL
  {
    $$.val = coltypes.Bool
  }

// Postgres OID pseudo-types. See https://www.postgresql.org/docs/9.4/static/datatype-oid.html.
postgres_oid:
  REGPROC
  {
    $$.val = coltypes.RegProc
  }
| REGPROCEDURE
  {
    $$.val = coltypes.RegProcedure
  }
| REGCLASS
  {
    $$.val = coltypes.RegClass
  }
| REGTYPE
  {
    $$.val = coltypes.RegType
  }
| REGNAMESPACE
  {
    $$.val = coltypes.RegNamespace
  }

opt_float:
  '(' ICONST ')'
  {
    nv := $2.numVal()
    prec, err := nv.AsInt64()
    if err != nil {
      return setErr(sqllex, err)
    }
    typ, err := coltypes.NewFloat(prec)
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = typ
  }
| /* EMPTY */
  {
    $$.val = coltypes.Float
  }

bit_with_length:
  BIT opt_varying '(' iconst64 ')'
  {
    bit, err := coltypes.NewBitArrayType(int($4.int64()), $2.bool(), $1)
    if err != nil { return setErr(sqllex, err) }
    $$.val = bit
  }
| VARBIT '(' iconst64 ')'
  {
    bit, err := coltypes.NewBitArrayType(int($3.int64()), true, $1)
    if err != nil { return setErr(sqllex, err) }
    $$.val = bit
  }

bit_without_length:
  BIT
  {
    $$.val = coltypes.Bit
  }
| BIT VARYING
  {
    $$.val = coltypes.BitV
  }
| VARBIT
  {
    $$.val = coltypes.VarBit
  }

character_with_length:
  character_base '(' iconst64 ')'
  {
    colTyp := *($1.colType().(*coltypes.TString))
    n := $3.int64()
    if n == 0 {
      sqllex.Error(fmt.Sprintf("length for type %s must be at least 1", &colTyp))
      return 1
    }
    colTyp.N = uint(n)
    $$.val = &colTyp
  }

character_without_length:
  character_base
  {
    $$.val = $1.colType()
  }

character_base:
  char_aliases
  {
    $$.val = coltypes.Char
  }
| character_aliases
  {
    $$.val = coltypes.Character
  }
| char_aliases VARYING
  {
    $$.val = coltypes.CharV
  }
| character_aliases VARYING
  {
    $$.val = coltypes.CharacterV
  }
| VARCHAR
  {
    $$.val = coltypes.VarChar
  }
| STRING
  {
    $$.val = coltypes.String
  }

char_aliases:
  CHAR

character_aliases:
 CHARACTER

opt_varying:
  VARYING     { $$.val = true }
| /* EMPTY */ { $$.val = false }

// SQL date/time types
const_datetime:
  DATE
  {
    $$.val = coltypes.Date
  }
| TIME opt_timezone
  {
    if $2.bool() { return unimplementedWithIssueDetail(sqllex, 26097, "type") }
    $$.val = coltypes.Time
  }
| TIME '(' iconst64 ')' opt_timezone
  {
    prec := $3.int64()
    if prec != 6 {
         return unimplementedWithIssue(sqllex, 32565)
    }
    $$.val = &coltypes.TTime{PrecisionSet: true, Precision: int(prec), VisibleType: coltypes.VisTIME}
  }
| TIMETZ                             { return unimplementedWithIssueDetail(sqllex, 26097, "type") }
| TIMETZ '(' ICONST ')'              { return unimplementedWithIssueDetail(sqllex, 26097, "type with precision") }
| TIMESTAMP opt_timezone
  {
    if $2.bool() {
      $$.val = coltypes.TimestampWithTZ
    } else {
      $$.val = coltypes.Timestamp
    }
  }
| TIMESTAMP '(' iconst64 ')' opt_timezone
  {
    prec := $3.int64()
    if prec > 6 {
         sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
         return 1
    }
    if $5.bool() {
      $$.val = &coltypes.TTimestampTZ{PrecisionSet: true, Precision: int(prec), VisibleType: coltypes.VisTIMESTAMPTZ}
    } else {
      $$.val = &coltypes.TTimestamp{PrecisionSet: true, Precision: int(prec), VisibleType: coltypes.VisTIMESTAMP}
    }
  }
| TIMESTAMPTZ
  {
    $$.val = coltypes.TimestampTZ
  }
| TIMESTAMPTZ '(' iconst64 ')'
  {
    prec := $3.int64()
    if prec > 6 {
    	sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
        return 1
    }
    $$.val = &coltypes.TTimestampTZ{PrecisionSet: true, Precision: int(prec), VisibleType: coltypes.VisTIMESTAMPTZ}
  }

opt_timezone:
  WITH_LA TIME ZONE { $$.val = true; }
| WITHOUT TIME ZONE { $$.val = false; }
| /*EMPTY*/         { $$.val = false; }


const_interval:
  INTERVAL {
    $$.val = coltypes.Interval
  }

interval_qualifier:
  YEAR
  {
    $$.val = tree.Year
  }
| MONTH
  {
    $$.val = tree.Month
  }
| DAY
  {
    $$.val = tree.Day
  }
| HOUR
  {
    $$.val = tree.Hour
  }
| MINUTE
  {
    $$.val = tree.Minute
  }
| interval_second
  {
    $$.val = $1.durationField()
  }
// Like Postgres, we ignore the left duration field. See explanation:
// https://www.postgresql.org/message-id/20110510040219.GD5617%40tornado.gateway.2wire.net
| YEAR TO MONTH
  {
    $$.val = tree.Month
  }
| DAY TO HOUR
  {
    $$.val = tree.Hour
  }
| DAY TO MINUTE
  {
    $$.val = tree.Minute
  }
| DAY TO interval_second
  {
    $$.val = $3.durationField()
  }
| HOUR TO MINUTE
  {
    $$.val = tree.Minute
  }
| HOUR TO interval_second
  {
    $$.val = $3.durationField()
  }
| MINUTE TO interval_second
  {
    $$.val = $3.durationField()
  }

opt_interval:
  interval_qualifier
| /* EMPTY */
  {
    $$.val = nil
  }

interval_second:
  SECOND
  {
    $$.val = tree.Second
  }
| SECOND '(' ICONST ')' { return unimplementedWithIssueDetail(sqllex, 32564, "interval second") }

// General expressions. This is the heart of the expression syntax.
//
// We have two expression types: a_expr is the unrestricted kind, and b_expr is
// a subset that must be used in some places to avoid shift/reduce conflicts.
// For example, we can't do BETWEEN as "BETWEEN a_expr AND a_expr" because that
// use of AND conflicts with AND as a boolean operator. So, b_expr is used in
// BETWEEN and we remove boolean keywords from b_expr.
//
// Note that '(' a_expr ')' is a b_expr, so an unrestricted expression can
// always be used by surrounding it with parens.
//
// c_expr is all the productions that are common to a_expr and b_expr; it's
// factored out just to eliminate redundant coding.
//
// Be careful of productions involving more than one terminal token. By
// default, bison will assign such productions the precedence of their last
// terminal, but in nearly all cases you want it to be the precedence of the
// first terminal instead; otherwise you will not get the behavior you expect!
// So we use %prec annotations freely to set precedences.
a_expr:
  c_expr
| a_expr TYPECAST cast_target
  {
    $$.val = &tree.CastExpr{Expr: $1.expr(), Type: $3.castTargetType(), SyntaxMode: tree.CastShort}
  }
| a_expr TYPEANNOTATE typename
  {
    $$.val = &tree.AnnotateTypeExpr{Expr: $1.expr(), Type: $3.colType(), SyntaxMode: tree.AnnotateShort}
  }
| a_expr COLLATE collation_name
  {
    $$.val = &tree.CollateExpr{Expr: $1.expr(), Locale: $3}
  }
// | a_expr AT TIME ZONE a_expr %prec AT { return unimplementedWithIssue(sqllex, 32005) }
| a_expr AT TIME ZONE a_expr %prec AT
  {
    sOutTime, err := tree.AtTimeZone($1.expr().String(), $5.expr().String())
    if err != nil {
    	return setErr(sqllex, err)
    }
    $$.val = tree.NewDString(sOutTime)
  }
  // These operators must be called out explicitly in order to make use of
  // bison's automatic operator-precedence handling. All other operator names
  // are handled by the generic productions using "OP", below; and all those
  // operators will have the same precedence.
  //
  // If you add more explicitly-known operators, be sure to add them also to
  // b_expr and to the math_op list below.
| '+' a_expr %prec UMINUS
  {
    // Unary plus is a no-op. Desugar immediately.
    $$.val = $2.expr()
  }
| '-' a_expr %prec UMINUS
  {
    $$.val = unaryNegation($2.expr())
  }
| '~' a_expr %prec UMINUS
  {
    $$.val = &tree.UnaryExpr{Operator: tree.UnaryComplement, Expr: $2.expr()}
  }
| a_expr '+' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Plus, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '-' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Minus, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '*' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Mult, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '/' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Div, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FLOORDIV a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.FloorDiv, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '%' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Mod, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '^' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Pow, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '#' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Bitxor, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr XOR a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Xor, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '&' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Bitand, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '|' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Bitor, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '<' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.LT, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '>' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.GT, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '?' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.JSONExists, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr JSON_SOME_EXISTS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.JSONSomeExists, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr JSON_ALL_EXISTS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.JSONAllExists, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr CONTAINS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.Contains, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr CONTAINED_BY a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.ContainedBy, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '=' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.EQ, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr EQNU a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.EQNU, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr CONCAT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Concat, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr LSHIFT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.LShift, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr RSHIFT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.RShift, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FETCHVAL a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.JSONFetchVal, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FETCHTEXT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.JSONFetchText, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FETCHVAL_PATH a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.JSONFetchValPath, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FETCHTEXT_PATH a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.JSONFetchTextPath, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr REMOVE_PATH a_expr
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("json_remove_path"), Exprs: tree.Exprs{$1.expr(), $3.expr()}}
  }
| a_expr INET_CONTAINED_BY_OR_EQUALS a_expr
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("inet_contained_by_or_equals"), Exprs: tree.Exprs{$1.expr(), $3.expr()}}
  }
| a_expr INET_CONTAINS_OR_CONTAINED_BY a_expr
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("inet_contains_or_contained_by"), Exprs: tree.Exprs{$1.expr(), $3.expr()}}
  }
| a_expr INET_CONTAINS_OR_EQUALS a_expr
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("inet_contains_or_equals"), Exprs: tree.Exprs{$1.expr(), $3.expr()}}
  }
| a_expr LESS_EQUALS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.LE, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr GREATER_EQUALS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.GE, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_EQUALS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NE, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr AND a_expr
  {
    $$.val = &tree.AndExpr{Left: $1.expr(), Right: $3.expr()}
  }
| a_expr OR a_expr
  {
    $$.val = &tree.OrExpr{Left: $1.expr(), Right: $3.expr()}
  }
| NOT a_expr
  {
    $$.val = &tree.NotExpr{Expr: $2.expr()}
  }
| NOT_LA a_expr %prec NOT
  {
    $$.val = &tree.NotExpr{Expr: $2.expr()}
  }
| a_expr LIKE_OP a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.Like, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr LIKE a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.Like, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr LIKE a_expr ESCAPE a_expr %prec ESCAPE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("like_escape"), Exprs: tree.Exprs{$1.expr(), $3.expr(), $5.expr()}}
  }
| a_expr NOT_LA LIKE a_expr %prec NOT_LA
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NotLike, Left: $1.expr(), Right: $4.expr()}
  }
| a_expr NOT_LA LIKE a_expr ESCAPE a_expr %prec ESCAPE
 {
   $$.val = &tree.FuncExpr{Func: tree.WrapFunction("not_like_escape"), Exprs: tree.Exprs{$1.expr(), $4.expr(), $6.expr()}}
 }
| a_expr ILIKE_OP a_expr
  {
   $$.val = &tree.ComparisonExpr{Operator: tree.ILike, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr ILIKE a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.ILike, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr ILIKE a_expr ESCAPE a_expr %prec ESCAPE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("ilike_escape"), Exprs: tree.Exprs{$1.expr(), $3.expr(), $5.expr()}}
  }
| a_expr NOT_LA ILIKE a_expr %prec NOT_LA
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NotILike, Left: $1.expr(), Right: $4.expr()}
  }
| a_expr NOT_LA ILIKE a_expr ESCAPE a_expr %prec ESCAPE
 {
   $$.val = &tree.FuncExpr{Func: tree.WrapFunction("not_ilike_escape"), Exprs: tree.Exprs{$1.expr(), $4.expr(), $6.expr()}}
 }
| a_expr SIMILAR TO a_expr %prec SIMILAR
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.SimilarTo, Left: $1.expr(), Right: $4.expr()}
  }
| a_expr SIMILAR TO a_expr ESCAPE a_expr %prec ESCAPE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("similar_to_escape"), Exprs: tree.Exprs{$1.expr(), $4.expr(), $6.expr()}}
  }
| a_expr NOT_LA SIMILAR TO a_expr %prec NOT_LA
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NotSimilarTo, Left: $1.expr(), Right: $5.expr()}
  }
| a_expr NOT_LA SIMILAR TO a_expr ESCAPE a_expr %prec ESCAPE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("not_similar_to_escape"), Exprs: tree.Exprs{$1.expr(), $5.expr(), $7.expr()}}
  }
| a_expr '~' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.RegMatch, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_REGMATCH a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NotRegMatch, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_LIKE_OP a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NotLike, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_ILIKE_OP a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NotILike, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr REGIMATCH a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.RegIMatch, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_REGIMATCH a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NotRegIMatch, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr IS NAN %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.EQ, Left: $1.expr(), Right: tree.NewStrVal("NaN")}
  }
| a_expr IS NOT NAN %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NE, Left: $1.expr(), Right: tree.NewStrVal("NaN")}
  }
| a_expr IS NULL %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: $1.expr(), Right: tree.DNull}
  }
| a_expr ISNULL %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: $1.expr(), Right: tree.DNull}
  }
| a_expr IS NOT NULL %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsDistinctFrom, Left: $1.expr(), Right: tree.DNull}
  }
| a_expr NOTNULL %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsDistinctFrom, Left: $1.expr(), Right: tree.DNull}
  }
| row OVERLAPS row { return unimplemented(sqllex, "overlaps") }
| a_expr IS TRUE %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: $1.expr(), Right: tree.MakeDBool(true)}
  }
| a_expr IS NOT TRUE %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsDistinctFrom, Left: $1.expr(), Right: tree.MakeDBool(true)}
  }
| a_expr IS FALSE %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: $1.expr(), Right: tree.MakeDBool(false)}
  }
| a_expr IS NOT FALSE %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsDistinctFrom, Left: $1.expr(), Right: tree.MakeDBool(false)}
  }
| a_expr IS UNKNOWN %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: $1.expr(), Right: tree.DNull}
  }
| a_expr IS NOT UNKNOWN %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsDistinctFrom, Left: $1.expr(), Right: tree.DNull}
  }
| a_expr IS DISTINCT FROM a_expr %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsDistinctFrom, Left: $1.expr(), Right: $5.expr()}
  }
| a_expr IS NOT DISTINCT FROM a_expr %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: $1.expr(), Right: $6.expr()}
  }
| a_expr IS OF '(' type_list ')' %prec IS
  {
    $$.val = &tree.IsOfTypeExpr{Expr: $1.expr(), Types: $5.colTypes()}
  }
| a_expr IS NOT OF '(' type_list ')' %prec IS
  {
    $$.val = &tree.IsOfTypeExpr{Not: true, Expr: $1.expr(), Types: $6.colTypes()}
  }
| a_expr BETWEEN opt_asymmetric b_expr AND a_expr %prec BETWEEN
  {
    $$.val = &tree.RangeCond{Left: $1.expr(), From: $4.expr(), To: $6.expr()}
  }
| a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr %prec NOT_LA
  {
    $$.val = &tree.RangeCond{Not: true, Left: $1.expr(), From: $5.expr(), To: $7.expr()}
  }
| a_expr BETWEEN SYMMETRIC b_expr AND a_expr %prec BETWEEN
  {
    $$.val = &tree.RangeCond{Symmetric: true, Left: $1.expr(), From: $4.expr(), To: $6.expr()}
  }
| a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr %prec NOT_LA
  {
    $$.val = &tree.RangeCond{Not: true, Symmetric: true, Left: $1.expr(), From: $5.expr(), To: $7.expr()}
  }
| a_expr IN in_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.In, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_LA IN in_expr %prec NOT_LA
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NotIn, Left: $1.expr(), Right: $4.expr()}
  }
| a_expr subquery_op sub_type a_expr %prec CONCAT
  {
    op := $3.cmpOp()
    subOp := $2.op()
    subOpCmp, ok := subOp.(tree.ComparisonOperator)
    if !ok {
      sqllex.Error(fmt.Sprintf("%s %s <array> is invalid because %q is not a boolean operator",
        subOp, op, subOp))
      return 1
    }
    $$.val = &tree.ComparisonExpr{
      Operator: op,
      SubOperator: subOpCmp,
      Left: $1.expr(),
      Right: $4.expr(),
    }
  }
| DEFAULT
  {
    $$.val = tree.DefaultVal{}
  }
// The UNIQUE predicate is a standard SQL feature but not yet implemented
// in PostgreSQL (as of 10.5).
| UNIQUE '(' error { return unimplemented(sqllex, "UNIQUE predicate") }

// Restricted expressions
//
// b_expr is a subset of the complete expression syntax defined by a_expr.
//
// Presently, AND, NOT, IS, and IN are the a_expr keywords that would cause
// trouble in the places where b_expr is used. For simplicity, we just
// eliminate all the boolean-keyword-operator productions from b_expr.
b_expr:
  c_expr
| b_expr TYPECAST cast_target
  {
    $$.val = &tree.CastExpr{Expr: $1.expr(), Type: $3.castTargetType(), SyntaxMode: tree.CastShort}
  }
| b_expr TYPEANNOTATE typename
  {
    $$.val = &tree.AnnotateTypeExpr{Expr: $1.expr(), Type: $3.colType(), SyntaxMode: tree.AnnotateShort}
  }
| '+' b_expr %prec UMINUS
  {
    $$.val = $2.expr()
  }
| '-' b_expr %prec UMINUS
  {
    $$.val = unaryNegation($2.expr())
  }
| '~' b_expr %prec UMINUS
  {
    $$.val = &tree.UnaryExpr{Operator: tree.UnaryComplement, Expr: $2.expr()}
  }
| b_expr '+' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Plus, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '-' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Minus, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '*' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Mult, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '/' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Div, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr FLOORDIV b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.FloorDiv, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '%' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Mod, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '^' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Pow, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '#' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Bitxor, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr XOR b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Xor, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '&' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Bitand, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '|' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Bitor, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '<' b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.LT, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '>' b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.GT, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '=' b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.EQ, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr EQNU b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.EQNU, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr CONCAT b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.Concat, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr LSHIFT b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.LShift, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr RSHIFT b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: tree.RShift, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr LESS_EQUALS b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.LE, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr GREATER_EQUALS b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.GE, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr NOT_EQUALS b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.NE, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr IS DISTINCT FROM b_expr %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsDistinctFrom, Left: $1.expr(), Right: $5.expr()}
  }
| b_expr IS NOT DISTINCT FROM b_expr %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: $1.expr(), Right: $6.expr()}
  }
| b_expr IS OF '(' type_list ')' %prec IS
  {
    $$.val = &tree.IsOfTypeExpr{Expr: $1.expr(), Types: $5.colTypes()}
  }
| b_expr IS NOT OF '(' type_list ')' %prec IS
  {
    $$.val = &tree.IsOfTypeExpr{Not: true, Expr: $1.expr(), Types: $6.colTypes()}
  }

// Productions that can be used in both a_expr and b_expr.
//
// Note: productions that refer recursively to a_expr or b_expr mostly cannot
// appear here. However, it's OK to refer to a_exprs that occur inside
// parentheses, such as function arguments; that cannot introduce ambiguity to
// the b_expr syntax.
//
c_expr:
  d_expr
| d_expr array_subscripts
  {
    $$.val = &tree.IndirectionExpr{
      Expr: $1.expr(),
      Indirection: $2.arraySubscripts(),
    }
  }
| case_expr
| EXISTS select_with_parens
  {
    $$.val = &tree.Subquery{Select: $2.selectStmt(), Exists: true}
  }

// Productions that can be followed by a postfix operator.
//
// Currently we support array indexing (see c_expr above).
//
// TODO(knz/jordan): this is the rule that can be extended to support
// composite types (#27792) with e.g.:
//
//     | '(' a_expr ')' field_access_ops
//
//     [...]
//
//     // field_access_ops supports the notations:
//     // - .a
//     // - .a[123]
//     // - .a.b[123][5456].c.d
//     // NOT [123] directly, this is handled in c_expr above.
//
//     field_access_ops:
//       field_access_op
//     | field_access_op other_subscripts
//
//     field_access_op:
//       '.' name
//     other_subscripts:
//       other_subscript
//     | other_subscripts other_subscript
//     other_subscript:
//        field_access_op
//     |  array_subscripts

d_expr:
  ICONST
  {
    $$.val = $1.numVal()
  }
| FCONST
  {
    $$.val = $1.numVal()
  }
| SCONST
  {
    $$.val = tree.NewStrVal($1)
  }
| BCONST
  {
    $$.val = tree.NewBytesStrVal($1)
  }
| BITCONST
  {
    d, err := tree.ParseDBitArray($1)
    if err != nil { return setErr(sqllex, err) }
    $$.val = d
  }
| func_name '(' expr_list opt_sort_clause_err ')' SCONST { return unimplemented(sqllex, $1.unresolvedName().String() + "(...) SCONST") }
//| STRING '(' iconst64 ')' SCONST
//  {
//    n := $3.int64()
//    if n == 0 {
//      sqllex.Error("length for type string must be at least 1")
//      return 1
//    }
//    coltype := &coltypes.TString{Variant: coltypes.TStringVariantSTRING, N: uint(n), VisibleType: coltypes.VisSTRING}
//    $$.val = &tree.CastExpr{Expr: tree.NewStrVal($5), Type: coltype, SyntaxMode: tree.CastPrepend}
//  }
| const_typename SCONST
  {
    $$.val = &tree.CastExpr{Expr: tree.NewStrVal($2), Type: $1.colType(), SyntaxMode: tree.CastPrepend}
  }
| interval
  {
    $$.val = $1.expr()
  }
| const_interval '(' ICONST ')' SCONST { return unimplementedWithIssue(sqllex, 32564) }
| TRUE
  {
    $$.val = tree.MakeDBool(true)
  }
| FALSE
  {
    $$.val = tree.MakeDBool(false)
  }
| NULL
  {
    $$.val = tree.DNull
  }
| column_path_with_star
  {
    $$.val = tree.Expr($1.unresolvedName())
  }
| '@' iconst64
  {
    colNum := $2.int64()
    if colNum < 1 || colNum > int64(MaxInt) {
      sqllex.Error(fmt.Sprintf("invalid column ordinal: @%d", colNum))
      return 1
    }
    $$.val = tree.NewOrdinalReference(int(colNum-1))
  }
| PLACEHOLDER
  {
    p := $1.placeholder()
    sqllex.(*lexer).UpdateNumPlaceholders(p)
    $$.val = p
  }
// TODO(knz/jordan): extend this for compound types. See explanation above.
| '(' a_expr ')' '.' '*'
  {
    $$.val = &tree.TupleStar{Expr: $2.expr()}
  }
| '(' a_expr ')' '.' unrestricted_name
  {
    $$.val = &tree.ColumnAccessExpr{Expr: $2.expr(), ColName: $5 }
  }
| '(' a_expr ')'
  {
    $$.val = &tree.ParenExpr{Expr: $2.expr()}
  }
| func_expr
| select_with_parens %prec UMINUS
  {
    $$.val = &tree.Subquery{Select: $1.selectStmt()}
  }
| labeled_row
  {
    $$.val = $1.tuple()
  }
| ARRAY select_with_parens %prec UMINUS
  {
    $$.val = &tree.ArrayFlatten{Subquery: &tree.Subquery{Select: $2.selectStmt()}}
  }
| ARRAY row
  {
    $$.val = &tree.Array{Exprs: $2.tuple().Exprs}
  }
| ARRAY array_expr
  {
    $$.val = $2.expr()
  }
| GROUPING '(' expr_list ')' { return unimplemented(sqllex, "d_expr grouping") }
| ROWNUM
  {
    $$.val = &tree.UnresolvedName{Star: false, NumParts: 1, Parts: tree.NameParts{"row_num"}}
  }

func_application:
  func_name '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRefFromName()}
  }
| func_name '(' expr_list opt_sort_clause_err ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRefFromName(), Exprs: $3.exprs()}
  }
| func_name '(' expr_list RESPECT NULLS opt_sort_clause_err')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRefFromName(), Exprs: $3.exprs()}
  }
| func_name '(' expr_list IGNORE NULLS opt_sort_clause_err')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRefFromName(), Exprs: $3.exprs()}
    $$.val.(*tree.FuncExpr).WindowDef = &tree.WindowDef{Frame: &tree.WindowFrame{IsIgnoreNulls:true}}
  }
| func_name '(' VARIADIC a_expr opt_sort_clause_err ')' { return unimplemented(sqllex, "variadic") }
| func_name '(' expr_list ',' VARIADIC a_expr opt_sort_clause_err ')' { return unimplemented(sqllex, "variadic") }
| func_name '(' ALL expr_list opt_sort_clause_err ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRefFromName(), Type: tree.AllFuncType, Exprs: $4.exprs()}
  }
| func_name '(' DISTINCT expr_list opt_sort_clause_err ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRefFromName(), Type: tree.DistinctFuncType, Exprs: $4.exprs()}
  }
| func_name '(' '*' ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRefFromName(), Exprs: tree.Exprs{tree.StarExpr()}}
  }
| func_name '(' error { return helpWithFunction(sqllex, $1.resolvableFuncRefFromName()) }

// func_expr and its cousin func_expr_windowless are split out from c_expr just
// so that we have classifications for "everything that is a function call or
// looks like one". This isn't very important, but it saves us having to
// document which variants are legal in places like "FROM function()" or the
// backwards-compatible functional-index syntax for CREATE INDEX. (Note that
// many of the special SQL functions wouldn't actually make any sense as
// functional index entries, but we ignore that consideration here.)
func_expr:
  func_application within_group_clause filter_clause over_clause
  {
    f := $1.expr().(*tree.FuncExpr)
    f.Filter = $3.expr()
    win := $4.windowDef()
    if f.WindowDef != nil && f.WindowDef.Frame != nil && f.WindowDef.Frame.IsIgnoreNulls {
    	if win != nil && win.Frame != nil {
    	    win.Frame.IsIgnoreNulls = true
    	}
    }
    f.WindowDef = win
    $$.val = f
  }
| func_expr_common_subexpr
  {
    $$.val = $1.expr()
  }

// As func_expr but does not accept WINDOW functions directly (but they can
// still be contained in arguments for functions etc). Use this when window
// expressions are not allowed, where needed to disambiguate the grammar
// (e.g. in CREATE INDEX).
func_expr_windowless:
  func_application { $$.val = $1.expr() }
| func_expr_common_subexpr { $$.val = $1.expr() }

// Special expressions that are considered to be functions.
func_expr_common_subexpr:
  COLLATION FOR '(' a_expr ')' { return unimplementedWithIssue(sqllex, 32563) }
| CURRENT_DATE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction(strings.ToLower($1))}
  }
| LOCAL_TIME
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction(strings.ToLower($1))}
  }
| LOCAL_TIMESTAMP
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction(strings.ToLower($1))}
  }

| CURRENT_SCHEMA
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction(strings.ToLower($1))}
  }
// Special identifier current_catalog is equivalent to current_database().
// https://www.postgresql.org/docs/10/static/functions-info.html
| CURRENT_CATALOG
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("current_database")}
  }
| CURRENT_TIMESTAMP
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction(strings.ToLower($1))}
  }
| CURRENT_TIME
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction(strings.ToLower($1))}
  }
| CURRENT_USER
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction(strings.ToLower($1))}
  }
// Special identifier current_role is equivalent to current_user.
// https://www.postgresql.org/docs/10/static/functions-info.html
| CURRENT_ROLE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("current_user")}
  }
| SESSION_USER
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("current_user")}
  }
| USER
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction(strings.ToLower($1))}
  }
| CAST '(' a_expr AS cast_target ')'
  {
    $$.val = &tree.CastExpr{Expr: $3.expr(), Type: $5.castTargetType(), SyntaxMode: tree.CastExplicit}
  }
| MANUALCAST '(' a_expr AS cast_target ')'
  {
    $$.val = &tree.CastExpr{Expr: $3.expr(), Type: $5.castTargetType(), SyntaxMode: tree.CastManual}
  }
| ANNOTATE_TYPE '(' a_expr ',' typename ')'
  {
    $$.val = &tree.AnnotateTypeExpr{Expr: $3.expr(), Type: $5.colType(), SyntaxMode: tree.AnnotateExplicit}
  }
| IF '(' a_expr ',' a_expr ',' a_expr ')'
  {
    $$.val = &tree.IfExpr{Cond: $3.expr(), True: $5.expr(), Else: $7.expr()}
  }
| IFERROR '(' a_expr ',' a_expr ',' a_expr ')'
  {
    $$.val = &tree.IfErrExpr{Cond: $3.expr(), Else: $5.expr(), ErrCode: $7.expr()}
  }
| IFERROR '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.IfErrExpr{Cond: $3.expr(), Else: $5.expr()}
  }
| ISERROR '(' a_expr ')'
  {
    $$.val = &tree.IfErrExpr{Cond: $3.expr()}
  }
| ISERROR '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.IfErrExpr{Cond: $3.expr(), ErrCode: $5.expr()}
  }
| NULLIF '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.NullIfExpr{Expr1: $3.expr(), Expr2: $5.expr()}
  }
| IFNULL '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.NvlExpr{Name: "IFNULL", Exprs: tree.Exprs{$3.expr(), $5.expr()}}
  }
| NVL '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.NvlExpr{Name: "NVL", Exprs: tree.Exprs{$3.expr(), $5.expr()}}
  }
| COALESCE '(' expr_list ')'
  {
    $$.val = &tree.CoalesceExpr{Name: "COALESCE", Exprs: $3.exprs()}
  }
| CHAR '(' expr_list ')'
  {
    $$.val = &tree.CharExpr{Name: "CHAR", Exprs: $3.exprs()}
  }
| special_function

special_function:
  CURRENT_DATE '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| LOCAL_TIME '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| LOCAL_TIMESTAMP '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_DATE '(' error { return helpWithFunctionByName(sqllex, $1) }
| LOCAL_TIME '(' error { return helpWithFunctionByName(sqllex, $1) }
| LOCAL_TIMESTAMP '(' error { return helpWithFunctionByName(sqllex, $1) }
| CURRENT_SCHEMA '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_SCHEMA '(' error { return helpWithFunctionByName(sqllex, $1) }
| CURRENT_TIMESTAMP '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_TIMESTAMP '(' error { return helpWithFunctionByName(sqllex, $1) }
| CURRENT_TIME '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_TIME '(' error { return helpWithFunctionByName(sqllex, $1) }
| CURRENT_USER '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_USER '(' error { return helpWithFunctionByName(sqllex, $1) }
| EXTRACT '(' extract_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| EXTRACT '(' error { return helpWithFunctionByName(sqllex, $1) }
| EXTRACT_DURATION '(' extract_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| EXTRACT_DURATION '(' error { return helpWithFunctionByName(sqllex, $1) }
| OVERLAY '(' overlay_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| OVERLAY '(' error { return helpWithFunctionByName(sqllex, $1) }
| POSITION '(' position_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("strpos"), Exprs: $3.exprs()}
  }
| SUBSTRING '(' substr_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| SUBSTRING '(' error { return helpWithFunctionByName(sqllex, $1) }
| TREAT '(' a_expr AS typename ')' { return unimplemented(sqllex, "treat") }
| TRIM '(' BOTH trim_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("btrim"), Exprs: $4.exprs()}
  }
| TRIM '(' LEADING trim_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("ltrim"), Exprs: $4.exprs()}
  }
| TRIM '(' TRAILING trim_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("rtrim"), Exprs: $4.exprs()}
  }
| TRIM '(' trim_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("trim"), Exprs: $3.exprs()}
  }
| TRIM '(' error { return helpWithFunctionByName(sqllex, $1) }
| USER '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| USER '(' error { return helpWithFunctionByName(sqllex, $1) }
| GREATEST '(' expr_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| GREATEST '(' error { return helpWithFunctionByName(sqllex, $1) }
| LEAST '(' expr_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| LEAST '(' error { return helpWithFunctionByName(sqllex, $1) }
| TZ_OFFSET '(' SESSIONTIMEZONE ')'
  {
    stz := tree.NewStrVal("SESSIONTIMEZONE")
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: []tree.Expr{stz}}
  }
| TZ_OFFSET '(' DBTIMEZONE ')'
  {
    dtz := tree.NewStrVal("DBTIMEZONE")
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: []tree.Expr{dtz}}
  }
| TZ_OFFSET '(' a_expr ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: []tree.Expr{$3.expr()}}
  }
| TZ_OFFSET '(' '+' a_expr ')'
  {
    expr := tree.NewStrVal($4.expr().String())
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: []tree.Expr{expr}}
  }
| TZ_OFFSET '(' '-' a_expr ')'
  {
    expr := tree.NewStrVal($4.expr().String())
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: []tree.Expr{expr}}
  }
| TZ_OFFSET '(' error { return helpWithFunctionByName(sqllex, $1) }

// Aggregate decoration clauses
within_group_clause:
WITHIN GROUP '(' sort_clause ')' { return unimplemented(sqllex, "within group") }
| /* EMPTY */ {}

filter_clause:
  FILTER '(' WHERE a_expr ')'
  {
    $$.val = $4.expr()
  }
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

// Window Definitions
window_clause:
  WINDOW window_definition_list
  {
    $$.val = $2.window()
  }
| /* EMPTY */
  {
    $$.val = tree.Window(nil)
  }

window_definition_list:
  window_definition
  {
    $$.val = tree.Window{$1.windowDef()}
  }
| window_definition_list ',' window_definition
  {
    $$.val = append($1.window(), $3.windowDef())
  }

window_definition:
  window_name AS window_specification
  {
    n := $3.windowDef()
    n.Name = tree.Name($1)
    $$.val = n
  }

over_clause:
  OVER window_specification
  {
    $$.val = $2.windowDef()
  }
| RESPECT NULLS OVER window_specification
  {
    $$.val = $4.windowDef()
  }
| IGNORE NULLS OVER window_specification
  {
    window := $4.windowDef()
    window.Frame.IsIgnoreNulls = true
    $$.val = window
  }
| OVER window_name
  {
    $$.val = &tree.WindowDef{Name: tree.Name($2)}
  }
| /* EMPTY */%prec UNBOUNDED
  {
    $$.val = (*tree.WindowDef)(nil)
  }

window_specification:
  '(' opt_existing_window_name opt_partition_clause
    opt_sort_clause opt_frame_clause ')'
  {
    $$.val = &tree.WindowDef{
      RefName: tree.Name($2),
      Partitions: $3.exprs(),
      OrderBy: $4.orderBy(),
      Frame: $5.windowFrame(),
    }
  }

// If we see PARTITION, RANGE, ROWS, or GROUPS as the first token after the '('
// of a window_specification, we want the assumption to be that there is no
// existing_window_name; but those keywords are unreserved and so could be
// names. We fix this by making them have the same precedence as IDENT and
// giving the empty production here a slightly higher precedence, so that the
// shift/reduce conflict is resolved in favor of reducing the rule. These
// keywords are thus precluded from being an existing_window_name but are not
// reserved for any other purpose.
opt_existing_window_name:
  name
| /* EMPTY */ %prec CONCAT
  {
    $$ = ""
  }

opt_partition_clause:
  PARTITION BY expr_list
  {
    $$.val = $3.exprs()
  }
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

// For frame clauses, we return a tree.WindowDef, but only some fields are used:
// frameOptions, startOffset, and endOffset.
//
// This is only a subset of the full SQL:2008 frame_clause grammar. We don't
// support <window frame exclusion> yet.
opt_frame_clause:
  RANGE frame_extent
  {
    $$.val = &tree.WindowFrame{
      Mode: tree.RANGE,
      Bounds: $2.windowFrameBounds(),
    }
  }
| ROWS frame_extent
  {
    $$.val = &tree.WindowFrame{
      Mode: tree.ROWS,
      Bounds: $2.windowFrameBounds(),
    }
  }
| GROUPS frame_extent
  {
    $$.val = &tree.WindowFrame{
      Mode: tree.GROUPS,
      Bounds: $2.windowFrameBounds(),
    }
  }
| /* EMPTY */
  {
    $$.val = (*tree.WindowFrame)(nil)
  }

frame_extent:
  frame_bound
  {
    startBound := $1.windowFrameBound()
    switch {
    case startBound.BoundType == tree.UnboundedFollowing:
      sqllex.Error("frame start cannot be UNBOUNDED FOLLOWING")
      return 1
    case startBound.BoundType == tree.OffsetFollowing:
      sqllex.Error("frame starting from following row cannot end with current row")
      return 1
    }
    $$.val = tree.WindowFrameBounds{StartBound: startBound}
  }
| BETWEEN frame_bound AND frame_bound
  {
    startBound := $2.windowFrameBound()
    endBound := $4.windowFrameBound()
    switch {
    case startBound.BoundType == tree.UnboundedFollowing:
      sqllex.Error("frame start cannot be UNBOUNDED FOLLOWING")
      return 1
    case endBound.BoundType == tree.UnboundedPreceding:
      sqllex.Error("frame end cannot be UNBOUNDED PRECEDING")
      return 1
    case startBound.BoundType == tree.CurrentRow && endBound.BoundType == tree.OffsetPreceding:
      sqllex.Error("frame starting from current row cannot have preceding rows")
      return 1
    case startBound.BoundType == tree.OffsetFollowing && endBound.BoundType == tree.OffsetPreceding:
      sqllex.Error("frame starting from following row cannot have preceding rows")
      return 1
    case startBound.BoundType == tree.OffsetFollowing && endBound.BoundType == tree.CurrentRow:
      sqllex.Error("frame starting from following row cannot have preceding rows")
      return 1
    }
    $$.val = tree.WindowFrameBounds{StartBound: startBound, EndBound: endBound}
  }

// This is used for both frame start and frame end, with output set up on the
// assumption it's frame start; the frame_extent productions must reject
// invalid cases.
frame_bound:
  UNBOUNDED PRECEDING
  {
    $$.val = &tree.WindowFrameBound{BoundType: tree.UnboundedPreceding}
  }
| UNBOUNDED FOLLOWING
  {
    $$.val = &tree.WindowFrameBound{BoundType: tree.UnboundedFollowing}
  }
| CURRENT ROW
  {
    $$.val = &tree.WindowFrameBound{BoundType: tree.CurrentRow}
  }
| a_expr PRECEDING
  {
    $$.val = &tree.WindowFrameBound{
      OffsetExpr: $1.expr(),
      BoundType: tree.OffsetPreceding,
    }
  }
| a_expr FOLLOWING
  {
    $$.val = &tree.WindowFrameBound{
      OffsetExpr: $1.expr(),
      BoundType: tree.OffsetFollowing,
    }
  }

// Supporting nonterminals for expressions.

// Explicit row production.
//
// SQL99 allows an optional ROW keyword, so we can now do single-element rows
// without conflicting with the parenthesized a_expr production. Without the
// ROW keyword, there must be more than one a_expr inside the parens.
row:
  ROW '(' opt_expr_list ')'
  {
    $$.val = &tree.Tuple{Exprs: $3.exprs(), Row: true}
  }
| expr_tuple_unambiguous
  {
    $$.val = $1.tuple()
  }

labeled_row:
  row
| '(' row AS name_list ')'
  {
    t := $2.tuple()
    labels := $4.nameList()
    t.Labels = make([]string, len(labels))
    for i, l := range labels {
      t.Labels[i] = string(l)
    }
    $$.val = t
  }

sub_type:
  ANY
  {
    $$.val = tree.Any
  }
| SOME
  {
    $$.val = tree.Some
  }
| ALL
  {
    $$.val = tree.All
  }

math_op:
  '+' { $$.val = tree.Plus  }
| '-' { $$.val = tree.Minus }
| '*' { $$.val = tree.Mult  }
| '/' { $$.val = tree.Div   }
| FLOORDIV { $$.val = tree.FloorDiv }
| '%' { $$.val = tree.Mod    }
| '&' { $$.val = tree.Bitand }
| '|' { $$.val = tree.Bitor  }
| '^' { $$.val = tree.Pow }
| '#' { $$.val = tree.Bitxor }
| '<' { $$.val = tree.LT }
| '>' { $$.val = tree.GT }
| '=' { $$.val = tree.EQ }
| XOR { $$.val = tree.Xor }
| LESS_EQUALS    { $$.val = tree.LE }
| GREATER_EQUALS { $$.val = tree.GE }
| NOT_EQUALS     { $$.val = tree.NE }

subquery_op:
  math_op
| LIKE         { $$.val = tree.Like     }
| NOT_LA LIKE  { $$.val = tree.NotLike  }
| ILIKE        { $$.val = tree.ILike    }
| NOT_LA ILIKE { $$.val = tree.NotILike }
| LIKE_OP      { $$.val = tree.Like     }
| NOT_LIKE_OP  { $$.val = tree.NotLike  }
| ILIKE_OP     { $$.val = tree.ILike    }
| NOT_ILIKE_OP { $$.val = tree.NotILike }
  // cannot put SIMILAR TO here, because SIMILAR TO is a hack.
  // the regular expression is preprocessed by a function (similar_escape),
  // and the ~ operator for posix regular expressions is used.
  //        x SIMILAR TO y     ->    x ~ similar_escape(y)
  // this transformation is made on the fly by the parser upwards.
  // however the SubLink structure which handles any/some/all stuff
  // is not ready for such a thing.

// expr_tuple1_ambiguous is a tuple expression with at least one expression.
// The allowable syntax is:
// ( )         -- empty tuple.
// ( E )       -- just one value, this is potentially ambiguous with
//             -- grouping parentheses. The ambiguity is resolved
//             -- by only allowing expr_tuple1_ambiguous on the RHS
//             -- of a IN expression.
// ( E, E, E ) -- comma-separated values, no trailing comma allowed.
// ( E, )      -- just one value with a comma, makes the syntax unambiguous
//             -- with grouping parentheses. This is not usually produced
//             -- by SQL clients, but can be produced by pretty-printing
//             -- internally in ZNBaseDB.
expr_tuple1_ambiguous:
  '(' ')'
  {
    $$.val = &tree.Tuple{}
  }
| '(' tuple1_ambiguous_values ')'
  {
    $$.val = &tree.Tuple{Exprs: $2.exprs()}
  }

tuple1_ambiguous_values:
  a_expr
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| a_expr ','
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| a_expr ',' expr_list
  {
     $$.val = append(tree.Exprs{$1.expr()}, $3.exprs()...)
  }

// expr_tuple_unambiguous is a tuple expression with zero or more
// expressions. The allowable syntax is:
// ( )         -- zero values
// ( E, )      -- just one value. This is unambiguous with the (E) grouping syntax.
// ( E, E, E ) -- comma-separated values, more than 1.
expr_tuple_unambiguous:
  '(' ')'
  {
    $$.val = &tree.Tuple{}
  }
| '(' tuple1_unambiguous_values ')'
  {
    $$.val = &tree.Tuple{Exprs: $2.exprs()}
  }

tuple1_unambiguous_values:
  a_expr ','
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| a_expr ',' expr_list
  {
     $$.val = append(tree.Exprs{$1.expr()}, $3.exprs()...)
  }

opt_expr_list:
  expr_list
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

expr_list:
  a_expr
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| expr_list ',' a_expr
  {
    $$.val = append($1.exprs(), $3.expr())
  }

type_list:
  typename
  {
    $$.val = []coltypes.T{$1.colType()}
  }
| type_list ',' typename
  {
    $$.val = append($1.colTypes(), $3.colType())
  }

array_expr:
  '[' opt_expr_list ']'
  {
    $$.val = &tree.Array{Exprs: $2.exprs()}
  }
| '[' array_expr_list ']'
  {
    $$.val = &tree.Array{Exprs: $2.exprs()}
  }

array_expr_list:
  array_expr
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| array_expr_list ',' array_expr
  {
    $$.val = append($1.exprs(), $3.expr())
  }

extract_list:
  extract_arg FROM a_expr
  {
    $$.val = tree.Exprs{tree.NewStrVal($1), $3.expr()}
  }
| expr_list
  {
    $$.val = $1.exprs()
  }

// TODO(vivek): Narrow down to just IDENT once the other
// terms are not keywords.
extract_arg:
  IDENT
| YEAR
| MONTH
| DAY
| HOUR
| MINUTE
| SECOND

// OVERLAY() arguments
// SQL99 defines the OVERLAY() function:
//   - overlay(text placing text from int for int)
//   - overlay(text placing text from int)
// and similarly for binary strings
overlay_list:
  a_expr overlay_placing substr_from substr_for
  {
    $$.val = tree.Exprs{$1.expr(), $2.expr(), $3.expr(), $4.expr()}
  }
| a_expr overlay_placing substr_from
  {
    $$.val = tree.Exprs{$1.expr(), $2.expr(), $3.expr()}
  }
| expr_list
  {
    $$.val = $1.exprs()
  }

overlay_placing:
  PLACING a_expr
  {
    $$.val = $2.expr()
  }

// position_list uses b_expr not a_expr to avoid conflict with general IN
position_list:
  b_expr IN b_expr
  {
    $$.val = tree.Exprs{$3.expr(), $1.expr()}
  }
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

// SUBSTRING() arguments
// SQL9x defines a specific syntax for arguments to SUBSTRING():
//   - substring(text from int for int)
//   - substring(text from int) get entire string from starting point "int"
//   - substring(text for int) get first "int" characters of string
//   - substring(text from pattern) get entire string matching pattern
//   - substring(text from pattern for escape) same with specified escape char
// We also want to support generic substring functions which accept
// the usual generic list of arguments. So we will accept both styles
// here, and convert the SQL9x style to the generic list for further
// processing. - thomas 2000-11-28
substr_list:
  a_expr substr_from substr_for
  {
    $$.val = tree.Exprs{$1.expr(), $2.expr(), $3.expr()}
  }
| a_expr substr_for substr_from
  {
    $$.val = tree.Exprs{$1.expr(), $3.expr(), $2.expr()}
  }
| a_expr substr_from
  {
    $$.val = tree.Exprs{$1.expr(), $2.expr()}
  }
| a_expr substr_for
  {
    $$.val = tree.Exprs{$1.expr(), tree.NewDInt(1), $2.expr()}
  }
| opt_expr_list
  {
    $$.val = $1.exprs()
  }

substr_from:
  FROM a_expr
  {
    $$.val = $2.expr()
  }

substr_for:
  FOR a_expr
  {
    $$.val = $2.expr()
  }

trim_list:
  a_expr FROM expr_list
  {
  switch t := $1.expr().(type) {
  case *tree.StrVal:
  	if t.RawString() == "" {
  	  $$.val = append($3.exprs(), tree.DNull)
  	} else {
  	  $$.val = append($3.exprs(), $1.expr())
  	}
  default: $$.val = append($3.exprs(), $1.expr())
  }

  }
| FROM expr_list
  {
    $$.val = $2.exprs()
  }
| expr_list
  {
    $$.val = $1.exprs()
  }

in_expr:
  select_with_parens
  {
    $$.val = &tree.Subquery{Select: $1.selectStmt()}
  }
| expr_tuple1_ambiguous

// Define SQL-style CASE clause.
// - Full specification
//      CASE WHEN a = b THEN c ... ELSE d END
// - Implicit argument
//      CASE a WHEN b THEN c ... ELSE d END
case_expr:
  CASE case_arg when_clause_list case_default END
  {
    $$.val = &tree.CaseExpr{Expr: $2.expr(), Whens: $3.whens(), Else: $4.expr()}
  }

when_clause_list:
  // There must be at least one
  when_clause
  {
    $$.val = []*tree.When{$1.when()}
  }
| when_clause_list when_clause
  {
    $$.val = append($1.whens(), $2.when())
  }

when_clause:
  WHEN a_expr THEN a_expr
  {
    $$.val = &tree.When{Cond: $2.expr(), Val: $4.expr()}
  }

case_default:
  ELSE a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

case_arg:
  a_expr
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

array_subscript:
  '[' a_expr ']'
  {
    $$.val = &tree.ArraySubscript{Begin: $2.expr()}
  }
| '[' opt_slice_bound ':' opt_slice_bound ']'
  {
    $$.val = &tree.ArraySubscript{Begin: $2.expr(), End: $4.expr(), Slice: true}
  }

opt_slice_bound:
  a_expr
| /*EMPTY*/
  {
    $$.val = tree.Expr(nil)
  }

array_subscripts:
  array_subscript
  {
    $$.val = tree.ArraySubscripts{$1.arraySubscript()}
  }
| array_subscripts array_subscript
  {
    $$.val = append($1.arraySubscripts(), $2.arraySubscript())
  }

opt_asymmetric:
  ASYMMETRIC {}
| /* EMPTY */ {}

target_list:
  target_elem
  {
    $$.val = tree.SelectExprs{$1.selExpr()}
  }
| target_list ',' target_elem
  {
    $$.val = append($1.selExprs(), $3.selExpr())
  }

target_elem:
  a_expr AS target_name
  {
    if $$.AsNameIsUp {
        $$.val = tree.SelectExpr{Expr: $1.expr(), As: tree.UnrestrictedName($3), AsNameIsDoublequotes:true}
    } else {
        $$.val = tree.SelectExpr{Expr: $1.expr(), As: tree.UnrestrictedName($3)}
    }
  }
  // We support omitting AS only for column labels that aren't any known
  // keyword. There is an ambiguity against postfix operators: is "a ! b" an
  // infix expression, or a postfix expression and a column label?  We prefer
  // to resolve this as an infix expression, which we accomplish by assigning
  // IDENT a precedence higher than POSTFIXOP.
| a_expr IDENT
  {
    $$.val = tree.SelectExpr{Expr: $1.expr(), As: tree.UnrestrictedName($2)}
  }
| a_expr
  {
    $$.val = tree.SelectExpr{Expr: $1.expr()}
  }
| '*'
  {
    $$.val = tree.StarSelectExpr()
  }
|CONNECT_BY_ROOT '(' a_expr ')' target_name
  {
    $$.val = tree.SelectExpr{Expr: $3.expr(), As: tree.UnrestrictedName($5)}
  }

// Names and constants.

table_index_name_list:
  table_index_name
  {
    $$.val = tree.TableIndexNames{$1.newTableIndexName()}
  }
| table_index_name_list ',' table_index_name
  {
    $$.val = append($1.newTableIndexNames(), $3.newTableIndexName())
  }

table_pattern_list:
  table_pattern
  {
    $$.val = tree.TablePatterns{$1.unresolvedName()}
  }
| table_pattern_list ',' table_pattern
  {
    $$.val = append($1.tablePatterns(), $3.unresolvedName())
  }

// An index can be specified in a few different ways:
//
//   - with explicit table name:
//       <table>@<index>
//       <schema>.<table>@<index>
//       <catalog/db>.<table>@<index>
//       <catalog/db>.<schema>.<table>@<index>
//
//   - without explicit table name:
//       <index>
//       <schema>.<index>
//       <catalog/db>.<index>
//       <catalog/db>.<schema>.<index>
table_index_name:
  table_name '@' index_name
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = tree.TableIndexName{
       Table: name,
       Index: tree.UnrestrictedName($3),
    }
  }
| standalone_index_name
  {
    // Treat it as a table name, then pluck out the TableName.
    name := $1.unresolvedObjectName().ToTableName()
    indexName := tree.UnrestrictedName(name.TableName)
    name.TableName = ""
    $$.val = tree.TableIndexName{
        Table: name,
        Index: indexName,
    }
  }

// table_pattern selects zero or more tables using a wildcard.
// Accepted patterns:
// - Patterns accepted by db_object_name
//   <table>
//   <schema>.<table>
//   <catalog/db>.<schema>.<table>
// - Wildcards:
//   <db/catalog>.<schema>.*
//   <schema>.*
//   *
table_pattern:
  simple_db_object_name
  {
     $$.val = $1.unresolvedObjectName().ToUnresolvedName()
  }
| complex_table_pattern

// complex_table_pattern is the part of table_pattern which recognizes
// every pattern not composed of a single identifier.
complex_table_pattern:
  complex_db_object_name
  {
     $$.val = $1.unresolvedObjectName().ToUnresolvedName()
  }
| db_object_name_component '.' unrestricted_name '.' '*'
  {
     $$.val = &tree.UnresolvedName{Star: true, NumParts: 3, Parts: tree.NameParts{"", $3, $1}}
  }
| db_object_name_component '.' '*'
  {
     $$.val = &tree.UnresolvedName{Star: true, NumParts: 2, Parts: tree.NameParts{"", $1}}
  }
| '*'
  {
     $$.val = &tree.UnresolvedName{Star: true, NumParts: 1}
  }

name_list:
  name
  {
    $$.val = tree.NameList{tree.Name($1)}
  }
| name_list ',' name
  {
    $$.val = append($1.nameList(), tree.Name($3))
  }


location_list:
  '(' name_list ')'
  {
    $$.val = tree.LocationList{
      &tree.Location{
            Locate: $2.nameList(),
          },
      }
  }
| location_list ',' '(' name_list ')'
  {
    locationlist := &tree.Location{ Locate: $4.nameList()}
    $$.val = append($1.locationList(), locationlist)
  }

// Constants
signed_iconst:
  ICONST
| '+' ICONST
  {
    $$.val = $2.numVal()
  }
| '-' ICONST
  {
    n := $2.numVal()
    n.Negative = true
    $$.val = n
  }

// signed_iconst64 is a variant of signed_iconst which only accepts (signed) integer literals that fit in an int64.
// If you use signed_iconst, you have to call AsInt64(), which returns an error if the value is too big.
// This rule just doesn't match in that case.
signed_iconst64:
  signed_iconst
  {
    val, err := $1.numVal().AsInt64()
    if err != nil { return setErr(sqllex, err) }
    $$.val = val
  }

// iconst64 accepts only unsigned integer literals that fit in an int64.
iconst64:
  ICONST
  {
    val, err := $1.numVal().AsInt64()
    if err != nil { return setErr(sqllex, err) }
    $$.val = val
  }

interval:
  const_interval SCONST opt_interval
  {
    // We don't carry opt_interval information into the column type, so we need
    // to parse the interval directly.
    var err error
    var d tree.Datum
    if $3.val == nil {
      d, err = tree.ParseDInterval($2)
    } else {
      d, err = tree.ParseDIntervalWithField($2, $3.durationField())
    }
    if err != nil { return setErr(sqllex, err) }
    $$.val = d
  }

// Name classification hierarchy.
//
// IDENT is the lexeme returned by the lexer for identifiers that match no
// known keyword. In most cases, we can accept certain keywords as names, not
// only IDENTs. We prefer to accept as many such keywords as possible to
// minimize the impact of "reserved words" on programmers. So, we divide names
// into several possible classes. The classification is chosen in part to make
// keywords acceptable as names wherever possible.

// Names specific to syntactic positions.
//
// The non-terminals "name", "unrestricted_name", "non_reserved_word",
// "unreserved_keyword", "non_reserved_word_or_sconst" etc. defined
// below are low-level, structural constructs.
//
// They are separate only because having them all as one rule would
// make the rest of the grammar ambiguous. However, because they are
// separate the question is then raised throughout the rest of the
// grammar: which of the name non-terminals should one use when
// defining a grammar rule?  Is an index a "name" or
// "unrestricted_name"? A partition? What about an index option?
//
// To make the decision easier, this section of the grammar creates
// meaningful, purpose-specific aliases to the non-terminals. These
// both make it easier to decide "which one should I use in this
// context" and also improves the readability of
// automatically-generated syntax diagrams.

// Note: newlines between non-terminals matter to the doc generator.

collation_name:        unrestricted_name

cursor_name:           name

variable_name:         name

partition_name:        unrestricted_name

index_name:            unrestricted_name

opt_index_name:        opt_name

zone_name:             unrestricted_name

target_name:           unrestricted_name

constraint_name:       name

database_name:         name

column_name:           name

family_name:           name

opt_family_name:       opt_name

table_alias_name:      name

statistics_name:       name

window_name:           name

view_name:             table_name

type_name:             db_object_name

sequence_name:         db_object_name

schema_name:           sc_object_name

table_name:            db_object_name

function_name:	       db_object_name

old_function_name:     db_object_name

new_function_name:     db_object_name

standalone_index_name: db_object_name

explain_option_name:   non_reserved_word

transaction_name:      unrestricted_name

pl_language:	       name

// Names for column references.
// Accepted patterns:
// <colname>
// <table>.<colname>
// <schema>.<table>.<colname>
// <catalog/db>.<schema>.<table>.<colname>
//
// Note: the rule for accessing compound types, if those are ever
// supported, is not to be handled here. The syntax `a.b.c.d....y.z`
// in `select a.b.c.d from t` *always* designates a column `z` in a
// table `y`, regardless of the meaning of what's before.
column_path:
  name
  {
      $$.val = &tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}
      if $$.isUp {
          $$.val.(*tree.UnresolvedName).IsDoublequotes = true
      }
  }
| prefixed_column_path
//| VALUES '(' IDENT ')'
//  {
//      $$.val = &tree.UnresolvedName{NumParts:2, Parts: tree.NameParts{$3, "excluded"}}
//  }

prefixed_column_path:
  db_object_name_component '.' unrestricted_name
  {
      $$.val = &tree.UnresolvedName{NumParts:2, Parts: tree.NameParts{$3,$1}}
      if $$.isUp {
          $$.val.(*tree.UnresolvedName).IsDoublequotes = true
      }
  }
| db_object_name_component '.' unrestricted_name '.' unrestricted_name
  {
      $$.val = &tree.UnresolvedName{NumParts:3, Parts: tree.NameParts{$5,$3,$1}}
      if $$.isUp {
          $$.val.(*tree.UnresolvedName).IsDoublequotes = true
      }
  }
| db_object_name_component '.' unrestricted_name '.' unrestricted_name '.' unrestricted_name
  {
      $$.val = &tree.UnresolvedName{NumParts:4, Parts: tree.NameParts{$7,$5,$3,$1}}
      if $$.isUp {
          $$.val.(*tree.UnresolvedName).IsDoublequotes = true
      }
  }

// Names for column references and wildcards.
// Accepted patterns:
// - those from column_path
// - <table>.*
// - <schema>.<table>.*
// - <catalog/db>.<schema>.<table>.*
// The single unqualified star is handled separately by target_elem.
column_path_with_star:
  column_path
| db_object_name_component '.' unrestricted_name '.' unrestricted_name '.' '*'
  {
    $$.val = &tree.UnresolvedName{Star:true, NumParts:4, Parts: tree.NameParts{"",$5,$3,$1}}
  }
| db_object_name_component '.' unrestricted_name '.' '*'
  {
    $$.val = &tree.UnresolvedName{Star:true, NumParts:3, Parts: tree.NameParts{"",$3,$1}}
  }
| db_object_name_component '.' '*'
  {
    $$.val = &tree.UnresolvedName{Star:true, NumParts:2, Parts: tree.NameParts{"",$1}}
  }

// Names for functions.
// The production for a qualified func_name has to exactly match the production
// for a column_path, because we cannot tell which we are parsing until
// we see what comes after it ('(' or SCONST for a func_name, anything else for
// a name).
// However we cannot use column_path directly, because for a single function name
// we allow more possible tokens than a simple column name.
func_name:
  type_function_name
  {
    $$.val = &tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}
  }
| prefixed_column_path

// Names for database objects (tables, sequences, views, stored functions).
// Accepted patterns:
// <table>
// <schema>.<table>
// <catalog/db>.<schema>.<table>
sc_object_name:
  simple_sc_object_name
| complex_sc_object_name

// simple_db_object_name is the part of db_object_name that recognizes
// simple identifiers.
simple_sc_object_name:
  db_object_name_component
  {
    res, err := tree.NewUnresolvedSchemaName(1, [3]string{$1})
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }

// complex_db_object_name is the part of db_object_name that recognizes
// composite names (not simple identifiers).
// It is split away from db_object_name in order to enable the definition
// of table_pattern.
complex_sc_object_name:
  db_object_name_component '.' unrestricted_name
  {
    res, err := tree.NewUnresolvedSchemaName(2, [3]string{$3, $1})
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }

// Names for database objects (tables, sequences, views, stored functions).
// Accepted patterns:
// <table>
// <schema>.<table>
// <catalog/db>.<schema>.<table>
db_object_name:
  simple_db_object_name
| complex_db_object_name

// simple_db_object_name is the part of db_object_name that recognizes
// simple identifiers.
simple_db_object_name:
  db_object_name_component
  {
    res, err := tree.NewUnresolvedObjectName(1, [3]string{$1})
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }

// complex_db_object_name is the part of db_object_name that recognizes
// composite names (not simple identifiers).
// It is split away from db_object_name in order to enable the definition
// of table_pattern.
complex_db_object_name:
  db_object_name_component '.' unrestricted_name
  {
    res, err := tree.NewUnresolvedObjectName(2, [3]string{$3, $1})
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }
| db_object_name_component '.' unrestricted_name '.' unrestricted_name
  {
    res, err := tree.NewUnresolvedObjectName(3, [3]string{$5, $3, $1})
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }

// DB object name component -- this cannot not include any reserved
// keyword because of ambiguity after FROM, but we've been too lax
// with reserved keywords and made INDEX and FAMILY reserved, so we're
// trying to gain them back here.
db_object_name_component:
  name
| znbasedb_extra_type_func_name_keyword
| znbasedb_extra_reserved_keyword

// General name --- names that can be column, table, etc names.
name:
  IDENT
| unreserved_keyword
| col_name_keyword

current_name:
  IDENT
| unreserved_keyword
| col_name_keyword

new_name:
  IDENT
| unreserved_keyword
| col_name_keyword

param_name:
   IDENT

opt_name:
  name
| /* EMPTY */
  {
    $$ = ""
  }

opt_name_parens:
  '(' name ')'
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_with_func_name:
  WITH NAME name
  {
    $$ = $3
  }
| /* EMPTY */
  {
  $$ = ""
  }

// Structural, low-level names

// Non-reserved word and also string literal constants.
non_reserved_word_or_sconst:
  non_reserved_word
| SCONST

// Type/function identifier --- names that can be type or function names.
type_function_name:
  IDENT
| unreserved_keyword
| type_func_name_keyword

// Any not-fully-reserved word --- these names can be, eg, variable names.
non_reserved_word:
  IDENT
| unreserved_keyword
| col_name_keyword
| type_func_name_keyword

// Unrestricted name --- allowable names when there is no ambiguity with even
// reserved keywords, like in "AS" clauses. This presently includes *all*
// Postgres keywords.
unrestricted_name:
  IDENT
| unreserved_keyword
| col_name_keyword
| type_func_name_keyword
| reserved_keyword

// Keyword category lists. Generally, every keyword present in the Postgres
// grammar should appear in exactly one of these lists.
//
// Put a new keyword into the first list that it can go into without causing
// shift or reduce conflicts. The earlier lists define "less reserved"
// categories of keywords.
//
// "Unreserved" keywords --- available for use as any kind of name.
unreserved_keyword:
  ABORT
| ABSOLUTE
| ACTION
| ADD
| ADMIN
| AFTER
| COMMENTS
| DEFAULTS
| EXCLUDING
| GENERATED
| IDENTITY
| STORAGE
| AGGREGATE
| ALTER
| ALWAYS
| AT
| AUTOMATIC
| BCRYPT
| BEFORE
| BEGIN
| BIGSERIAL
| BLOB
| CLOB
| BOOL
| BY
| BYTEA
| BYTES
| CACHE
| CANCEL
| CASCADE
| CHANGEFEED
| CLEAR
| CLUSTER
| COLUMNS
| COLENGINEREAD
| COMMENT
| COMMIT
| COMMITTED
| COMPACT
| CONFLICT
| CONFIGURATION
| CONFIGURATIONS
| CONFIGURE
| CONSTRAINTS
| CONVERSION
| COPY
| COVERING
| CREATEUSER
| CSTATUS
| CUBE
| CURSOR
| CURSORS
| CYCLE
| DATA
| DATABASE
| DATABASES
| DATE
| DAY
| DEALLOCATE
| DELETE
| DEFERRED
| DETACHED
| DISCARD
| DISABLE
| DOMAIN
| DOUBLE
| DROP
| DUMP
| DUPLICATE
| ENABLE
| EACH
| ENCODING
| ENCRYPTION
| ENCRYPTION_PASSPHRASE
| ENUM
| ESCAPE
| EXECUTE
| EXECUTION
| EXPERIMENTAL
| EXPERIMENTAL_AUDIT
| EXPERIMENTAL_FINGERPRINTS
| EXPERIMENTAL_RANGES
| EXPERIMENTAL_RELOCATE
| EXPERIMENTAL_REPLICA
| EXPLAIN
| EXTENSION
| EXTERN
| FILES
| FILTER
| FIRST
| FLASHBACK
| FLOAT4
| FLOAT8
| FOLLOWING
| FORCE_INDEX
| FUNCTION
| FUNCTIONS
| GLOBAL
| GRANTS
| GROUPS
| GROUPID
| HASH
| HASH_JOIN
| HIGH
| HISTOGRAM
| HOUR
| IGNORE
| IGNORE_INDEX
| IMMEDIATE
| INCREMENT
| INCREMENTAL
| INDEXES
| INET
| INHERIT
| INHERITS
| INJECT
| INOUT
| INT2
| INT2VECTOR
| INT4
| INT8
| INT64
| INSERT
| INSTEAD
| INTENT
| INTERLEAVE
| INVERTED
| ISNULL
| ISOLATION
| JOB
| JOBS
| JSON
| JSONB
| LOAD_KAFKA
| KEY
| KEYS
| KV
| LANGUAGE
| LC_COLLATE
| LC_CTYPE
| LEASE
| LESS
| LEVEL
| LIST
| LOAD
| LOCAL
| LOCKED
| LOOKUP
| LOOKUP_JOIN
| LOCKSTAT
| LOW
| MATCH
| MATCHED
| MATERIALIZED
| MAXVALUE
| MERGE
| MERGE_JOIN
| MESSAGE_TEXT
| MINUTE
| MINVALUE
| MONTH
| NAMES
| NAN
| NAME
| NEVER
| NEXT
| NO
| NOCSTATUS
| NOCYCLE
| NOCREATEUSER
| NORMAL
| NO_INDEX_JOIN
| NOWAIT
| NTH_PLAN
| NULLS
| OBJECT_ID
| OF
| OFF
| OID
| OIDS
| OIDVECTOR
| OPERATOR
| OPT
| OPTION
| OPTIONS
| ORDINALITY
| OVER
| OWNED
| PARENT
| PARTIAL
| PARTITION
| PARTITIONS
| PASSWORD
| PATH
| PAUSE
| PBKDF2
| PHYSICAL
| PLAN
| PLANS
| PRECEDING
| PREPARE
| PRIORITY
| PROCEDURE
| PUBLICATION
| QB_NAME
| QUERIES
| QUERY
| RANGE
| RANGES
| READ
| READ_FROM_STORAGE
| RECURRING
| RECURSIVE
| REF
| REFRESH
| REGCLASS
| REGPROC
| REGPROCEDURE
| REGNAMESPACE
| REGTYPE
| RELATIVE
| RELEASE
| RENAME
| REPEATABLE
| REPLACE
| RESET
| RESPECT
| RESTRICT
| RESUME
| RETRY
| RETURNS
| REVISION_HISTORY
| REVERT
| REVOKE
| ROLE
| ROLES
| ROLLBACK
| ROLLUP
// | ROWNUM
| ROWS
| ROWENGINEREAD
| RULE
| SCHEDULE
| SCHEDULES
| SETTING
| SETTINGS
| STATEMENT
| STATUS
| SAVEPOINT
| SCATTER
| SCHEMA
| SCHEMAS
| SCRUB
| SEARCH
| SEARCH_PATH
| SECOND
| SERIAL
| SERIALIZABLE
| SERIAL2
| SERIAL4
| SERIAL8
| SEQUENCE
| SEQUENCES
| SERVER
| SESSION
| SESSIONS
| SET
| SHARE
| SHOW
| SIMPLE
| SKIP
| SIGNAL
| SM3
| SMALLSERIAL
| SNAPSHOT
| SPLIT
| SQL
| SQLSTATE
| START
| STATISTICS
| STDIN
| STORE
| STORED
| STORING
| STRICT
| STRING
| SPACES
| SUBSCRIPTION
| SYNTAX
| SYSTEM
| TABLES
| TEMP
| TEMPLATE
| TEMPORARY
| TESTING_RANGES
| TESTING_RELOCATE
| TEXT
| TRACE
| TRANSACTION
| TRIGGER
| TRIGGERS
| TRUNCATE
| TRUSTED
| TTLDAYS
| TYPE
| THROTTLING
| UNBOUNDED
| UNCOMMITTED
| UNKNOWN
| UNLOGGED
| UNTIL
| UPDATE
| UPSERT
| UUID
| USE
| USERS
| USE_INDEX
| VALID
| VALIDATE
| VALUE
| VARIABLE
| VARYING
| VIEW
| VOID
| WAIT
| WITHIN
| WITHOUT
| WRITE
| YEAR
| ZONE


// Column identifier --- keywords that can be column, table, etc names.
//
// Many of these keywords will in fact be recognized as type or function names
// too; but they have special productions for the purpose, and so can't be
// treated as "generic" type or function names.
//
// The type names appearing here are not usable as function names because they
// can be followed by '(' in typename productions, which looks too much like a
// function call for an LR(1) parser.
col_name_keyword:
  ANNOTATE_TYPE
| BETWEEN
| BIGINT
| BIT
| BOOLEAN
| CHAR
| CHARACTER
| CHARACTERISTICS
| COALESCE
| DEC
| DECIMAL
| EXISTS
| EXTRACT
| EXTRACT_DURATION
| FLOAT
| GREATEST
| GROUPING
| IF
| IFERROR
| IFNULL
| INT
| INTEGER
| INTERVAL
| ISERROR
| LEAST
| NVL
| NULLIF
| NUMERIC
| OUT
| OVERLAY
| POSITION
| PRECISION
| REAL
| ROW
| REPLICATION
| SMALLINT
| SUBSTRING
| TIME
| TIMETZ
| TIMESTAMP
| TIMESTAMPTZ
| TREAT
| TRIM
| TZ_OFFSET
| VALUES
| VARBIT
| VARCHAR
| VIRTUAL
| WORK

// Type/function identifier --- keywords that can be type or function names.
//
// Most of these are keywords that are used as operators in expressions; in
// general such keywords can't be column names because they would be ambiguous
// with variables, but they are unambiguous as function identifiers.
//
// Do not include POSITION, SUBSTRING, etc here since they have explicit
// productions in a_expr to support the goofy SQL9x argument syntax.
// - thomas 2000-11-28
//
// *** DO NOT ADD INSPURCLOUDDB-SPECIFIC KEYWORDS HERE ***
//
// See znbasedb_extra_type_func_name_keyword below.
type_func_name_keyword:
  COLLATION
| CONNECT
| CROSS
| DBTIMEZONE
| FULL
| INNER
| NONE
| ILIKE
| ILIKE_OP
| IS
| JOIN
| LEFT
| LIKE
| LIKE_OP
| NATURAL
| NOTNULL
| NOT_LIKE_OP
| NOT_ILIKE_OP
| OUTER
| OVERLAPS
| RIGHT
| SIMILAR
| SESSIONTIMEZONE
| znbasedb_extra_type_func_name_keyword

// ZNBaseDB-specific keywords that can be used in type/function
// identifiers.
//
// *** REFRAIN FROM ADDING KEYWORDS HERE ***
//
// Adding keywords here creates non-resolvable incompatibilities with
// postgres clients.
//
znbasedb_extra_type_func_name_keyword:
  FAMILY

// Reserved keyword --- these keywords are usable only as a unrestricted_name.
//
// Keywords appear here if they could not be distinguished from variable, type,
// or function names in some contexts.
//
// *** NEVER ADD KEYWORDS HERE ***
//
// See znbasedb_extra_reserved_keyword below.
//
reserved_keyword:
  ALL
| ANALYSE
| ANALYZE
| AND
| ANY
| ARRAY
| AS
| ASC
| ASYMMETRIC
| BACKWARD
| BOTH
| CALL
| CASE
| CAST
| MANUALCAST
| CHECK
| CLOSE
| COLLATE
| COLUMN
| CONNECTION
| CONSTRAINT
| CREATE
| CURRENT
| CURRENT_CATALOG
| CURRENT_DATE
| LOCAL_TIME
| LOCAL_TIMESTAMP
| CURRENT_ROLE
| CURRENT_SCHEMA
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| DDL
| DECLARE
| DEFAULT
| DEFERRABLE
| DESC
| DISTINCT
| DO
| ELSE
| END
| EXCEPT
| FALSE
| FETCH
| FOR
| FOREIGN
| FORWARD
| FROM
| GRANT
| GROUP
| HAVING
| IN
| INITIALLY
| INTERSECT
| INTO
| LAST
| LATERAL
| LEADING
| LIMIT
| LOCALTIME
| LOCALTIMESTAMP
| LOCATE
| MOVE
| NOT
| NOTSUPDDL
| NULL
| OFFSET
| ON
| ONLY
| OPEN
| OR
| ORDER
| PRIOR
| PLACING
| PRIMARY
| REFCURSOR
| REFERENCES
| RETURNING
| SELECT
| SESSION_USER
| SOME
| SUPDDL
| SYMMETRIC
| TABLE
| THEN
| TO
| TRAILING
| TRUE
| UNION
| UNIQUE
| USER
| USING
| VARIADIC
| WHEN
| WHERE
| WINDOW
| WITH
| XOR
| znbasedb_extra_reserved_keyword

// Reserved keywords in ZNBaseDB, in addition to those reserved in
// PostgreSQL.
//
// *** REFRAIN FROM ADDING KEYWORDS HERE ***
//
// Adding keywords here creates non-resolvable incompatibilities with
// postgres clients.
znbasedb_extra_reserved_keyword:
  INDEX
| NOTHING

%%
