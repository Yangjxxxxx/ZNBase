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
	"errors"
	"fmt"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util/pretty"
)

// SelectStatement represents any SELECT statement.
type SelectStatement interface {
	Statement
	selectStatement()
}

func (*ParenSelect) selectStatement()  {}
func (*SelectClause) selectStatement() {}
func (*UnionClause) selectStatement()  {}
func (*ValuesClause) selectStatement() {}

// Select represents a SelectStatement with an ORDER and/or LIMIT.
type Select struct {
	With      *With
	Select    SelectStatement
	OrderBy   OrderBy
	Limit     *Limit
	ForLocked LockingClause
	HintSet   HintSet
}

// LockingClause represents a locking clause, like FOR UPDATE.
// LockingClause表示一个锁定子句，例如FOR UPDATE
type LockingClause []*LockingItem

// Format implements the NodeFormatter interface.
// Format实现NodeFormatter接口。
func (node *LockingClause) Format(ctx *FmtCtx) {
	for _, n := range *node {
		ctx.FormatNode(n)
	}
}

// LockingItem represents a locking clause, like FOR UPDATE.
type LockingItem struct {
	Strength   LockingStrength
	WaitPolicy LockingWaitPolicy
	Targets    TableNames
}

// Format implements the NodeFormatter interface.
func (f *LockingItem) Format(ctx *FmtCtx) {
	f.Strength.Format(ctx)
	if len(f.Targets) > 0 {
		ctx.WriteString(" OF ")
		f.Targets.Format(ctx)
	}
	f.WaitPolicy.Format(ctx)
}

// LockingStrength represents the possible row-level lock modes for a SELECT
// statement.
type LockingStrength byte

// The ordering of the variants is important, because the highest numerical
// value takes precedence when row-level locking is specified multiple ways.
// 变量的顺序很重要，因为数值最高。
// 当以多种方式指定行级别锁定时，值优先。
const (
	// ForNone represents the default - no for statement at all.
	ForNone LockingStrength = iota
	// ForUpdate represents FOR UPDATE.
	// ForKeyShare represents FOR KEY SHARE.
	ForKeyShare
	// ForShare represents FOR SHARE.
	// ForShare 代表 FOR SHARE.
	ForShare
	// ForNoKeyUpdate represents FOR NO KEY UPDATE.
	// ForNoKeyUpdate 代表 FOR NO KEY UPDATE.
	ForNoKeyUpdate
	// ForUpdate represents FOR UPDATE.
	// ForUpdate 代表 FOR UPDATE.
	ForUpdate
)

var lockingStrengthName = [...]string{
	ForNone:        "",
	ForKeyShare:    "FOR KEY SHARE",
	ForShare:       "FOR SHARE",
	ForNoKeyUpdate: "FOR NO KEY UPDATE",
	ForUpdate:      "FOR UPDATE",
}

func (f LockingStrength) String() string {
	return lockingStrengthName[f]
}

// Format implements the NodeFormatter interface.
func (f LockingStrength) Format(ctx *FmtCtx) {
	if f != ForNone {
		ctx.WriteString(" ")
		ctx.WriteString(f.String())
	}
}

// Max returns the maximum of the two locking strengths.
// 最大值返回两个锁定强度中的最大值。
func (f LockingStrength) Max(s2 LockingStrength) LockingStrength {
	return LockingStrength(max(byte(f), byte(s2)))
}

//GetStartWithIsJoin get flag IsJoin
func (sel *Select) GetStartWithIsJoin() bool {
	if sel.With != nil && sel.With.StartWith {
		selStmt, ok := sel.With.CTEList[0].Stmt.(*Select)
		if ok {
			if unionSel, ok := selStmt.Select.(*UnionClause); ok {
				return unionSel.GetLeftSelectClauseIsJoin()
			}
		}
	}
	return false
}

// Format implements the NodeFormatter interface.
func (sel *Select) Format(ctx *FmtCtx) {
	ctx.FormatNode(sel.With)
	ctx.FormatNode(sel.Select)
	if len(sel.OrderBy) > 0 {
		ctx.WriteByte(' ')
		ctx.FormatNode(&sel.OrderBy)
	}
	if sel.Limit != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(sel.Limit)
	}
	ctx.FormatNode(&sel.ForLocked)
}

// LockingWaitPolicy represents the possible policies for handling conflicting
// locks held by other active transactions when attempting to lock rows due to
// FOR UPDATE/SHARE clauses (i.e. it represents the NOWAIT and SKIP LOCKED
// options).
// LockingWaitPolicy表示可能的策略，用于处理由于FOR UPDATE / SHARE子句而试图锁定行时其
// 他活动事务持有的冲突锁（即，它表示NOWAIT和SKIP LOCKED选项）。
type LockingWaitPolicy struct {
	LockingType LockingType
	WaitTime    int64
}

// LockingType 锁类型
type LockingType byte

// The ordering of the variants is important, because the highest numerical
// value takes precedence when row-level locking is specified multiple ways.
// 变量的顺序很重要，因为当以多种方式指定行级别锁定时，最高数值优先。

const (
	// LockWaitBlock represents the default - wait for the lock to become
	// available.
	LockWaitBlock LockingType = iota
	// LockWaitSkip represents SKIP LOCKED - skip rows that can't be locked.
	LockWaitSkip
	// LockWait represents WAIT
	LockWait
	// LockWaitError represents NOWAIT - raise an error if a row cannot be
	// locked.
	LockWaitError
)

var lockingWaitPolicyName = [...]string{
	LockWaitBlock: "",
	LockWaitSkip:  "SKIP LOCKED",
	LockWaitError: "NOWAIT",
	LockWait:      "WAIT",
}

func (f LockingWaitPolicy) String() string {
	return lockingWaitPolicyName[f.LockingType]
}

// Format implements the NodeFormatter interface.
func (f LockingWaitPolicy) Format(ctx *FmtCtx) {
	if f.LockingType == LockWait {
		ctx.WriteString(" ")
		ctx.WriteString(f.String())
		ctx.WriteString(" ")
		ctx.WriteString(string(rune(f.WaitTime)))
	} else if f.LockingType != LockWaitBlock {
		ctx.WriteString(" ")
		ctx.WriteString(f.String())
	}
}

// Max returns the maximum of the two locking wait policies.
// Max返回两个锁定等待策略中的最大值。
func (f LockingWaitPolicy) Max(f2 LockingWaitPolicy) LockingWaitPolicy {
	LockingType := LockingType(max(byte(f.LockingType), byte(f2.LockingType)))
	LockingWaitPolicy := LockingWaitPolicy{LockingType: LockingType}
	if LockingType == LockWait {
		LockingWaitPolicy.WaitTime = f.WaitTime
	}
	return LockingWaitPolicy
}

func max(a, b byte) byte {
	if a > b {
		return a
	}
	return b
}

// ExpendSelect  整库、模式、表以及SELECT查询
type ExpendSelect struct {
	Query  *Select
	DbName Name
	ScName *SchemaName
	Type   roachpb.DUMPFileFormat
}

// Format implements the NodeFormatter interface.
func (n *ExpendSelect) Format(ctx *FmtCtx) {
	if n.Type == roachpb.DUMPFileFormat_DATABASE {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&n.DbName)
	} else if n.Type == roachpb.DUMPFileFormat_SCHEMA {
		ctx.WriteString("SCHEMA ")
		ctx.FormatNode(n.ScName)
	} else {
		ctx.FormatNode(n.Query)
	}
}

// ParenSelect represents a parenthesized SELECT/UNION/VALUES statement.
type ParenSelect struct {
	Select *Select
}

func (n *ExpendSelect) doc(p *PrettyCfg) pretty.Doc {
	return n.Query.doc(p)
}

// Format implements the NodeFormatter interface.
func (n *ParenSelect) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(n.Select)
	ctx.WriteByte(')')
}

// SelectClause represents a SELECT statement.
type SelectClause struct {
	Distinct        bool
	DistinctOn      DistinctOn
	HintSet         HintSet
	Exprs           SelectExprs
	From            *From
	Where           *Where
	GroupBy         GroupBy
	Having          *Where
	Window          Window
	TableSelect     bool
	StartWith       Expr
	ConnectBy       Expr
	StartWithIsJoin bool
	AsNameList      []string
}

// Format implements the NodeFormatter interface.
func (n *SelectClause) Format(ctx *FmtCtx) {
	if n.TableSelect {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(n.From.Tables[0])
	} else {
		ctx.WriteString("SELECT ")
		if n.Distinct {
			if n.DistinctOn != nil {
				ctx.FormatNode(&n.DistinctOn)
				ctx.WriteByte(' ')
			} else {
				ctx.WriteString("DISTINCT ")
			}
		}
		if n.HintSet != nil && len(n.HintSet) > 0 {
			ctx.WriteString("/*+ ")
			sep := ""
			for i := 0; i < len(n.HintSet); i++ {
				ctx.WriteString(sep)
				switch t := n.HintSet[i].(type) {
				case *TableHint:
					ctx.FormatNode(t)
				case *IndexHint:
					ctx.FormatNode(t)
				}
				sep = ", "
			}
			ctx.WriteString(" */ ")
		}
		ctx.FormatNode(&n.Exprs)
		if len(n.From.Tables) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(n.From)
		}
		if n.Where != nil {
			ctx.WriteByte(' ')
			ctx.FormatNode(n.Where)
		}
		if len(n.GroupBy) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&n.GroupBy)
		}
		if n.Having != nil {
			ctx.WriteByte(' ')
			ctx.FormatNode(n.Having)
		}
		if len(n.Window) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&n.Window)
		}
	}
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

// Format implements the NodeFormatter interface.
func (node *SelectExprs) Format(ctx *FmtCtx) {
	for i := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*node)[i])
	}
}

// SelectExpr represents a SELECT expression.
type SelectExpr struct {
	Expr                 Expr
	As                   UnrestrictedName
	AsNameIsDoublequotes bool
}

// NormalizeTopLevelVarName preemptively expands any UnresolvedName at
// the top level of the expression into a VarName. This is meant
// to catch stars so that sql.checkRenderStar() can see it prior to
// other expression transformations.
func (sel *SelectExpr) NormalizeTopLevelVarName() error {
	if vBase, ok := sel.Expr.(VarName); ok {
		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err
		}
		sel.Expr = v
	}
	return nil
}

// StarSelectExpr is a convenience function that represents an unqualified "*"
// in a select expression.
func StarSelectExpr() SelectExpr {
	return SelectExpr{Expr: StarExpr()}
}

// Format implements the NodeFormatter interface.
func (sel *SelectExpr) Format(ctx *FmtCtx) {
	ctx.FormatNode(sel.Expr)
	if sel.As != "" {
		ctx.WriteString(" AS ")
		ctx.FormatNode(&sel.As)
	}
}

// AliasClause represents an alias, optionally with a column list:
// "AS name" or "AS name(col1, col2)".
type AliasClause struct {
	Alias Name
	Cols  NameList
}

// Format implements the NodeFormatter interface.
func (a *AliasClause) Format(ctx *FmtCtx) {
	ctx.FormatNode(&a.Alias)
	if len(a.Cols) != 0 {
		// Format as "alias (col1, col2, ...)".
		ctx.WriteString(" (")
		ctx.FormatNode(&a.Cols)
		ctx.WriteByte(')')
	}
}

// AsOfClause represents an as of time.
type AsOfClause struct {
	Expr Expr
}

// Format implements the NodeFormatter interface.
func (a *AsOfClause) Format(ctx *FmtCtx) {
	ctx.WriteString("AS OF SYSTEM TIME ")
	ctx.FormatNode(a.Expr)
}

// AsSnapshotClause represents an as of time.
type AsSnapshotClause struct {
	Snapshot Name
}

// Format implements the NodeFormatter interface.
func (node *AsSnapshotClause) Format(ctx *FmtCtx) {
	ctx.WriteString("AS OF SNAPSHOT ")
	ctx.FormatNode(&node.Snapshot)
}

// From represents a FROM clause.
type From struct {
	Tables     TableExprs
	AsOf       AsOfClause
	AsSnapshot AsSnapshotClause
}

// Format implements the NodeFormatter interface.
func (node *From) Format(ctx *FmtCtx) {
	ctx.WriteString("FROM ")
	ctx.FormatNode(&node.Tables)
	if node.AsOf.Expr != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.AsOf)
	}
	if node.AsSnapshot.Snapshot != "" {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.AsSnapshot)
	}
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

// Format implements the NodeFormatter interface.
func (node *TableExprs) Format(ctx *FmtCtx) {
	prefix := ""
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// TableExpr represents a table expression.
type TableExpr interface {
	NodeFormatter
	tableExpr()
}

func (*AliasedTableExpr) tableExpr() {}
func (*ParenTableExpr) tableExpr()   {}
func (*JoinTableExpr) tableExpr()    {}
func (*RowsFromExpr) tableExpr()     {}
func (*Subquery) tableExpr()         {}
func (*PartitionExpr) tableExpr()    {}
func (*StatementSource) tableExpr()  {}

// StatementSource encapsulates one of the other statements as a data source.
type StatementSource struct {
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *StatementSource) Format(ctx *FmtCtx) {
	ctx.WriteByte('[')
	ctx.FormatNode(node.Statement)
	ctx.WriteByte(']')
}

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID uint32

// IndexFlags represents "@<index_name|index_id>" or "@{param[,param]}" where
// param is one of:
//  - FORCE_INDEX=<index_name|index_id>
//  - ASC / DESC
//  - NO_INDEX_JOIN
// It is used optionally after a table name in SELECT statements.
type IndexFlags struct {
	Index   UnrestrictedName
	IndexID IndexID
	// Direction of the scan, if provided. Can only be set if
	// one of Index or IndexID is set.
	Direction Direction
	// NoIndexJoin cannot be specified together with an index.
	NoIndexJoin bool
}

// ForceIndex returns true if a forced index was specified, either using a name
// or an IndexID.
func (ih *IndexFlags) ForceIndex() bool {
	return ih.Index != "" || ih.IndexID != 0
}

// CombineWith combines two IndexFlags structures, returning an error if they
// conflict with one another.
func (ih *IndexFlags) CombineWith(other *IndexFlags) error {
	if ih.NoIndexJoin && other.NoIndexJoin {
		return errors.New("NO_INDEX_JOIN specified multiple times")
	}
	result := *ih
	result.NoIndexJoin = ih.NoIndexJoin || other.NoIndexJoin

	if other.Direction != 0 {
		if ih.Direction != 0 {
			return errors.New("ASC/DESC specified multiple times")
		}
		result.Direction = other.Direction
	}

	if other.ForceIndex() {
		if ih.ForceIndex() {
			return errors.New("FORCE_INDEX specified multiple times")
		}
		result.Index = other.Index
		result.IndexID = other.IndexID
	}

	// We only set at the end to avoid a partially changed structure in one of the
	// error cases above.
	*ih = result
	return nil
}

// Check verifies if the flags are valid:
//  - ascending/descending is not specified without an index;
//  - no_index_join isn't specified with an index.
func (ih *IndexFlags) Check() error {
	if ih.NoIndexJoin && ih.ForceIndex() {
		return errors.New("FORCE_INDEX cannot be specified in conjunction with NO_INDEX_JOIN")
	}
	if ih.Direction != 0 && !ih.ForceIndex() {
		return errors.New("ASC/DESC must be specified in conjunction with an index")
	}
	return nil
}

// Format implements the NodeFormatter interface.
func (ih *IndexFlags) Format(ctx *FmtCtx) {
	if !ih.NoIndexJoin && ih.Direction == 0 {
		ctx.WriteByte('@')
		if ih.Index != "" {
			ctx.FormatNode(&ih.Index)
		} else {
			ctx.Printf("[%d]", ih.IndexID)
		}
	} else {
		if ih.Index == "" && ih.IndexID == 0 {
			ctx.WriteString("@{NO_INDEX_JOIN}")
		} else {
			ctx.WriteString("@{FORCE_INDEX=")
			if ih.Index != "" {
				ctx.FormatNode(&ih.Index)
			} else {
				ctx.Printf("[%d]", ih.IndexID)
			}

			if ih.Direction != 0 {
				ctx.Printf(",%s", ih.Direction)
			}
			if ih.NoIndexJoin {
				ctx.WriteString(",NO_INDEX_JOIN")
			}
			ctx.WriteString("}")
		}
	}
}

// AliasedTableExpr represents a table expression coupled with an optional
// alias.
type AliasedTableExpr struct {
	Expr       TableExpr
	IndexFlags *IndexFlags
	Ordinality bool
	As         AliasClause
}

// Format implements the NodeFormatter interface.
func (node *AliasedTableExpr) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.Expr)
	if node.IndexFlags != nil {
		ctx.FormatNode(node.IndexFlags)
	}
	if node.Ordinality {
		ctx.WriteString(" WITH ORDINALITY")
	}
	if node.As.Alias != "" {
		ctx.WriteString(" AS ")
		ctx.FormatNode(&node.As)
	}
}

// ParenTableExpr represents a parenthesized TableExpr.
type ParenTableExpr struct {
	Expr TableExpr
}

// Format implements the NodeFormatter interface.
func (node *ParenTableExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteByte(')')
}

// StripTableParens strips any parentheses surrounding a selection clause.
func StripTableParens(expr TableExpr) TableExpr {
	if p, ok := expr.(*ParenTableExpr); ok {
		return StripTableParens(p.Expr)
	}
	return expr
}

// JoinTableExpr represents a TableExpr that's a JOIN operation.
type JoinTableExpr struct {
	JoinType string
	Left     TableExpr
	Right    TableExpr
	Cond     JoinCond
	Hint     string
}

// JoinTableExpr.Join
const (
	AstFull  = "FULL"
	AstLeft  = "LEFT"
	AstRight = "RIGHT"
	AstCross = "CROSS"
	AstInner = "INNER"
)

// JoinTableExpr.Hint
const (
	AstHash   = "HASH"
	AstLookup = "LOOKUP"
	AstMerge  = "MERGE"
)

// Format implements the NodeFormatter interface.
func (node *JoinTableExpr) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.Left)
	ctx.WriteByte(' ')
	if _, isNatural := node.Cond.(NaturalJoinCond); isNatural {
		// Natural joins have a different syntax: "<a> NATURAL <join_type> <b>"
		ctx.FormatNode(node.Cond)
		ctx.WriteByte(' ')
		if node.JoinType != "" {
			ctx.WriteString(node.JoinType)
			ctx.WriteByte(' ')
			if node.Hint != "" {
				ctx.WriteString(node.Hint)
				ctx.WriteByte(' ')
			}
		}
		ctx.WriteString("JOIN ")
		ctx.FormatNode(node.Right)
	} else {
		// General syntax: "<a> <join_type> [<join_hint>] JOIN <b> <condition>"
		if node.JoinType != "" {
			ctx.WriteString(node.JoinType)
			ctx.WriteByte(' ')
			if node.Hint != "" {
				ctx.WriteString(node.Hint)
				ctx.WriteByte(' ')
			}
		}
		ctx.WriteString("JOIN ")
		ctx.FormatNode(node.Right)
		if node.Cond != nil {
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Cond)
		}
	}
}

// JoinCond represents a join condition.
type JoinCond interface {
	NodeFormatter
	joinCond()
}

func (NaturalJoinCond) joinCond() {}
func (*OnJoinCond) joinCond()     {}
func (*UsingJoinCond) joinCond()  {}

// NaturalJoinCond represents a NATURAL join condition
type NaturalJoinCond struct{}

// Format implements the NodeFormatter interface.
func (NaturalJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("NATURAL")
}

// OnJoinCond represents an ON join condition.
type OnJoinCond struct {
	Expr Expr
}

func (node *OnJoinCond) String() string {
	panic("implement me")
}

//Walk is remained to be implemented.
func (node *OnJoinCond) Walk(Visitor) Expr {
	panic("implement me")
}

//TypeCheck is remained to be implemented.
func (node *OnJoinCond) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	panic("implement me")
}

// Format implements the NodeFormatter interface.
func (node *OnJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("ON ")
	ctx.FormatNode(node.Expr)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols NameList
}

// Format implements the NodeFormatter interface.
func (node *UsingJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("USING (")
	ctx.FormatNode(&node.Cols)
	ctx.WriteByte(')')
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr Expr
}

// Where.Type
const (
	AstWhere  = "WHERE"
	AstHaving = "HAVING"
)

// NewWhere creates a WHERE or HAVING clause out of an Expr. If the expression
// is nil, it returns nil.
func NewWhere(typ string, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

// Format implements the NodeFormatter interface.
func (node *Where) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Type)
	ctx.WriteByte(' ')
	ctx.FormatNode(node.Expr)
}

// GroupBy represents a GROUP BY clause.
type GroupBy []Expr

// Format implements the NodeFormatter interface.
func (node *GroupBy) Format(ctx *FmtCtx) {
	prefix := "GROUP BY "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// DistinctOn represents a DISTINCT ON clause.
type DistinctOn []Expr

// Format implements the NodeFormatter interface.
func (node *DistinctOn) Format(ctx *FmtCtx) {
	ctx.WriteString("DISTINCT ON (")
	ctx.FormatNode((*Exprs)(node))
	ctx.WriteByte(')')
}

// OrderBy represents an ORDER By clause.
type OrderBy []*Order

// Format implements the NodeFormatter interface.
func (node *OrderBy) Format(ctx *FmtCtx) {
	prefix := "ORDER BY "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (d Direction) String() string {
	if d < 0 || d > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", d)
	}
	return directionName[d]
}

// OrderType indicates which type of expression is used in ORDER BY.
type OrderType int

const (
	// OrderByColumn is the regular "by expression/column" ORDER BY specification.
	OrderByColumn OrderType = iota
	// OrderByIndex enables the user to specify a given index' columns implicitly.
	OrderByIndex
)

// Order represents an ordering expression.
type Order struct {
	OrderType OrderType
	Expr      Expr
	Direction Direction
	// Table/Index replaces Expr when OrderType = OrderByIndex.
	Table TableName
	// If Index is empty, then the order should use the primary key.
	Index UnrestrictedName
}

// Format implements the NodeFormatter interface.
func (node *Order) Format(ctx *FmtCtx) {
	if node.OrderType == OrderByColumn {
		ctx.FormatNode(node.Expr)
	} else {
		if node.Index == "" {
			ctx.WriteString("PRIMARY KEY ")
			ctx.FormatNode(&node.Table)
		} else {
			ctx.WriteString("INDEX ")
			ctx.FormatNode(&node.Table)
			ctx.WriteByte('@')
			ctx.FormatNode(&node.Index)
		}
	}
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
}

// Limit represents a LIMIT clause.
type Limit struct {
	Offset, Count Expr
}

// Format implements the NodeFormatter interface.
func (node *Limit) Format(ctx *FmtCtx) {
	needSpace := false
	if node.Count != nil {
		ctx.WriteString("LIMIT ")
		ctx.FormatNode(node.Count)
		needSpace = true
	}
	if node.Offset != nil {
		if needSpace {
			ctx.WriteByte(' ')
		}
		ctx.WriteString("OFFSET ")
		ctx.FormatNode(node.Offset)
	}
}

// PartitionExpr represents a FROM [PARTITION ...]... expression.
type PartitionExpr struct {
	PartitionName Name
	TableName     TableName
	IsDefault     bool
}

// Format implements the NodeFormatter interface.
func (node *PartitionExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("[PARTITION ")
	ctx.FormatNode(&node.PartitionName)
	if node.IsDefault {
		ctx.WriteString(" DEFAULT")
	}
	ctx.WriteString("] OF ")
	ctx.FormatNode(&node.TableName)
}

// RowsFromExpr represents a ROWS FROM(...) expression.
type RowsFromExpr struct {
	Items Exprs
}

// Format implements the NodeFormatter interface.
func (node *RowsFromExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("ROWS FROM (")
	ctx.FormatNode(&node.Items)
	ctx.WriteByte(')')
}

// Window represents a WINDOW clause.
type Window []*WindowDef

// Format implements the NodeFormatter interface.
func (node *Window) Format(ctx *FmtCtx) {
	prefix := "WINDOW "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(&n.Name)
		ctx.WriteString(" AS ")
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// WindowDef represents a single window definition expression.
type WindowDef struct {
	Name       Name
	RefName    Name
	Partitions Exprs
	OrderBy    OrderBy
	Frame      *WindowFrame
}

// Format implements the NodeFormatter interface.
func (node *WindowDef) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	needSpaceSeparator := false
	if node.RefName != "" {
		ctx.FormatNode(&node.RefName)
		needSpaceSeparator = true
	}
	if len(node.Partitions) > 0 {
		if needSpaceSeparator {
			ctx.WriteByte(' ')
		}
		ctx.WriteString("PARTITION BY ")
		ctx.FormatNode(&node.Partitions)
		needSpaceSeparator = true
	}
	if len(node.OrderBy) > 0 {
		if needSpaceSeparator {
			ctx.WriteByte(' ')
		}
		ctx.FormatNode(&node.OrderBy)
		needSpaceSeparator = true
	}
	if node.Frame != nil {
		if needSpaceSeparator {
			ctx.WriteByte(' ')
		}
		ctx.FormatNode(node.Frame)
	}
	ctx.WriteRune(')')
}

// WindowFrameMode indicates which mode of framing is used.
type WindowFrameMode int

const (
	// RANGE is the mode of specifying frame in terms of logical range (e.g. 100 units cheaper).
	RANGE WindowFrameMode = iota
	// ROWS is the mode of specifying frame in terms of physical offsets (e.g. 1 row before etc).
	ROWS
	// GROUPS is the mode of specifying frame in terms of peer groups.
	GROUPS
)

// OverrideWindowDef implements the logic to have a base window definition which
// then gets augmented by a different window definition.
func OverrideWindowDef(base *WindowDef, override WindowDef) (WindowDef, error) {
	// referencedSpec.Partitions is always used.
	if len(override.Partitions) > 0 {
		return WindowDef{}, pgerror.Newf(pgcode.Windowing, "cannot override PARTITION BY clause of window %q", base.Name)
	}
	override.Partitions = base.Partitions

	// referencedSpec.OrderBy is used if set.
	if len(base.OrderBy) > 0 {
		if len(override.OrderBy) > 0 {
			return WindowDef{}, pgerror.Newf(pgcode.Windowing, "cannot override ORDER BY clause of window %q", base.Name)
		}
		override.OrderBy = base.OrderBy
	}

	if base.Frame != nil {
		return WindowDef{}, pgerror.Newf(pgcode.Windowing, "cannot copy window %q because it has a frame clause", base.Name)
	}

	return override, nil
}

// WindowFrameBoundType indicates which type of boundary is used.
type WindowFrameBoundType int

const (
	// UnboundedPreceding represents UNBOUNDED PRECEDING type of boundary.
	UnboundedPreceding WindowFrameBoundType = iota
	// OffsetPreceding represents 'value' PRECEDING type of boundary.
	OffsetPreceding
	// CurrentRow represents CURRENT ROW type of boundary.
	CurrentRow
	// OffsetFollowing represents 'value' FOLLOWING type of boundary.
	OffsetFollowing
	// UnboundedFollowing represents UNBOUNDED FOLLOWING type of boundary.
	UnboundedFollowing
)

// WindowFrameBound specifies the offset and the type of boundary.
type WindowFrameBound struct {
	BoundType  WindowFrameBoundType
	OffsetExpr Expr
}

// HasOffset returns whether node contains an offset.
func (node *WindowFrameBound) HasOffset() bool {
	return node.BoundType == OffsetPreceding || node.BoundType == OffsetFollowing
}

// WindowFrameBounds specifies boundaries of the window frame.
// The row at StartBound is included whereas the row at EndBound is not.
type WindowFrameBounds struct {
	StartBound *WindowFrameBound
	EndBound   *WindowFrameBound
}

// IsOffset returns true if the WindowFrameBoundType is an offset.
func (ft WindowFrameBoundType) IsOffset() bool {
	return ft == OffsetPreceding || ft == OffsetFollowing
}

// HasOffset returns whether node contains an offset in either of the bounds.
func (node *WindowFrameBounds) HasOffset() bool {
	return node.StartBound.HasOffset() || (node.EndBound != nil && node.EndBound.HasOffset())
}

// WindowFrame represents static state of window frame over which calculations are made.
type WindowFrame struct {
	Mode          WindowFrameMode   // the mode of framing being used
	Bounds        WindowFrameBounds // the bounds of the frame
	IsIgnoreNulls bool
}

// Format implements the NodeFormatter interface.
func (node *WindowFrameBound) Format(ctx *FmtCtx) {
	switch node.BoundType {
	case UnboundedPreceding:
		ctx.WriteString("UNBOUNDED PRECEDING")
	case OffsetPreceding:
		ctx.FormatNode(node.OffsetExpr)
		ctx.WriteString(" PRECEDING")
	case CurrentRow:
		ctx.WriteString("CURRENT ROW")
	case OffsetFollowing:
		ctx.FormatNode(node.OffsetExpr)
		ctx.WriteString(" FOLLOWING")
	case UnboundedFollowing:
		ctx.WriteString("UNBOUNDED FOLLOWING")
	default:
		panic(fmt.Sprintf("unhandled case: %d", node.BoundType))
	}
}

// Format implements the NodeFormatter interface.
func (node *WindowFrame) Format(ctx *FmtCtx) {
	switch node.Mode {
	case RANGE:
		ctx.WriteString("RANGE ")
	case ROWS:
		ctx.WriteString("ROWS ")
	case GROUPS:
		ctx.WriteString("GROUPS ")
	default:
		panic(fmt.Sprintf("unhandled case: %d", node.Mode))
	}
	if node.Bounds.EndBound != nil {
		ctx.WriteString("BETWEEN ")
		ctx.FormatNode(node.Bounds.StartBound)
		ctx.WriteString(" AND ")
		ctx.FormatNode(node.Bounds.EndBound)
	} else {
		ctx.FormatNode(node.Bounds.StartBound)
	}
}
