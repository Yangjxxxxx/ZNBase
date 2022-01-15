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
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security/useroption"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"golang.org/x/text/language"
)

// CreateDatabase represents a CREATE DATABASE statement.
type CreateDatabase struct {
	IfNotExists bool
	Name        Name
	Template    string
	Encoding    string
	Collate     string
	CType       string
}

// Format implements the NodeFormatter interface.
func (n *CreateDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE DATABASE ")
	if n.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&n.Name)
	if n.Template != "" {
		ctx.WriteString(" TEMPLATE = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, n.Template, ctx.flags.EncodeFlags())
	}
	if n.Encoding != "" {
		ctx.WriteString(" ENCODING = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, n.Encoding, ctx.flags.EncodeFlags())
	}
	if n.Collate != "" {
		ctx.WriteString(" LC_COLLATE = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, n.Collate, ctx.flags.EncodeFlags())
	}
	if n.CType != "" {
		ctx.WriteString(" LC_CTYPE = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, n.CType, ctx.flags.EncodeFlags())
	}
}

//CreateTrigger used to create trigger
type CreateTrigger struct {
	Trigname     Name
	Tablename    TableName
	Row          bool
	Timing       string
	Events       []string
	WhenClause   Expr
	FuncName     ResolvableFunctionReference
	Args         Exprs
	Isconstraint bool
	Deferrable   bool
	Initdeferred bool
	Funorpro     bool

	// TODO(cms): add some for procedure
	HasProcedure bool
	Procedure    string
	// just for compile
	Funcname string
}

// Format implements the NodeFormatter interface.
func (node *CreateTrigger) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE TRIGGER ")
	ctx.FormatNode(&node.Trigname)
	if node.Timing == "before" {
		ctx.WriteString(" BEFORE ")
	} else if node.Timing == "after" {
		ctx.WriteString(" AFTER ")
	} else {
		ctx.WriteString(" INSTEAD OF ")
	}
	for i := 0; i < len(node.Events); i++ {
		ctx.WriteString(node.Events[i])
		if i == len(node.Events)-1 {
			ctx.WriteString(" ON ")
		} else {
			ctx.WriteString(" OR ")
		}
	}
	ctx.FormatNode(&node.Tablename)
	if node.Row {
		ctx.WriteString(" FOR EACH ROW ")
	} else {
		ctx.WriteString(" FOR EACH STATEMENT ")
	}
	if node.WhenClause != nil {
		ctx.WriteString("WHEN (")
		ctx.FormatNode(node.WhenClause)
		ctx.WriteByte(')')
	}
	if node.HasProcedure {
		ctx.WriteString("BEGIN $$ ")
		ctx.WriteString(node.Procedure)
		ctx.WriteString(" $$ END")
	} else {
		ctx.WriteString(" EXECUTE ")

		if node.Funorpro {
			ctx.WriteString("FUNCTION ")
		} else {
			ctx.WriteString("PROCEDURE ")
		}
		ctx.WriteString("tmp")
		ctx.WriteString(" (")
		for i, pam := range node.Args {
			if i != 0 {
				ctx.WriteString(",")
			}
			ctx.WriteString(pam.String())
		}
		ctx.WriteString(")")
	}
}

// IndexElem represents a column with a direction in a CREATE INDEX statement.
type IndexElem struct {
	Column    Name
	Function  Expr
	Direction Direction
}

// IsFuncIndex return whether it is a function index or not
func (node *IndexElem) IsFuncIndex() bool {
	if node.Function == nil {
		return false
	}
	return true
}

// Format implements the NodeFormatter interface.
func (node *IndexElem) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.Column)
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
}

// IndexElemList is list of IndexElem.
type IndexElemList []IndexElem

// Format pretty-prints the contained names separated by commas.
// Format implements the NodeFormatter interface.
func (l *IndexElemList) Format(ctx *FmtCtx) {
	for i := range *l {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*l)[i])
	}
}

// CreateIndex represents a CREATE INDEX statement.
type CreateIndex struct {
	Name        Name
	Table       TableName
	Unique      bool
	Inverted    bool
	IfNotExists bool
	Columns     IndexElemList
	// Extra columns to be stored together with the indexed ones as an optimization
	// for improved reading performance.
	Storing                 NameList
	Interleave              *InterleaveDef
	IsLocal                 bool
	LocalIndexPartitionName []string
	PartitionBy             *PartitionBy
	// geo-partition feature add by jiye
	LocateSpaceName *Location
	Where           *Where
}

// Format implements the NodeFormatter interface.
func (n *CreateIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if n.Unique {
		ctx.WriteString("UNIQUE ")
	}
	if n.Inverted {
		ctx.WriteString("INVERTED ")
	}
	ctx.WriteString("INDEX ")
	if n.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	if n.Name != "" {
		ctx.FormatNode(&n.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("ON ")
	ctx.FormatNode(&n.Table)
	ctx.WriteString(" (")
	ctx.FormatNode(&n.Columns)
	ctx.WriteByte(')')
	if len(n.Storing) > 0 {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&n.Storing)
		ctx.WriteByte(')')
	}
	if n.Interleave != nil {
		ctx.FormatNode(n.Interleave)
	}
	if n.IsLocal {
		ctx.WriteString(" LOCAL ")
	}
	if n.PartitionBy != nil {
		ctx.FormatNode(n.PartitionBy)
	}
	if n.LocateSpaceName != nil {
		ctx.FormatNode(n.LocateSpaceName)
	}
}

// TableDef represents a column, index or constraint definition within a CREATE
// TABLE statement.
type TableDef interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types (*TableDef) conform
	// to the TableDef interface.
	tableDef()
	SetName(name Name)
	// SetName replaces the name of the definition in-place. Used in the parser.
}

func (*ColumnTableDef) tableDef() {}
func (*IndexTableDef) tableDef()  {}
func (*FamilyTableDef) tableDef() {}
func (*LikeTableDef) tableDef()   {}

// TableDefs represents a list of table definitions.
type TableDefs []TableDef

// Format implements the NodeFormatter interface.
func (node *TableDefs) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(n)
	}
}

// Nullability represents either NULL, NOT NULL or an unspecified value (silent
// NULL).
type Nullability int

// The values for NullType.
const (
	NotNull Nullability = iota
	Null
	SilentNull
)

// ColumnTableDef represents a column definition within a CREATE TABLE
// statement.
type ColumnTableDef struct {
	Name     Name
	Type     coltypes.T
	Nullable struct {
		Nullability    Nullability
		ConstraintName Name
	}
	PrimaryKey           bool
	Unique               bool
	UniqueConstraintName Name
	DefaultExpr          struct {
		Expr                     Expr
		ConstraintName           Name
		OnUpdateCurrentTimeStamp bool
	}
	CheckExprs []ColumnTableDefCheckExpr
	References struct {
		Table          *TableName
		Col            Name
		ConstraintName Name
		Actions        ReferenceActions
		Match          CompositeKeyMatchMethod
	}
	Computed struct {
		Computed bool
		Expr     Expr
	}
	Family struct {
		Name        Name
		Create      bool
		IfNotExists bool
	}
	IsHash  bool
	IsMerge bool
}

// ColumnTableDefCheckExpr represents a check constraint on a column definition
// within a CREATE TABLE statement.
type ColumnTableDefCheckExpr struct {
	Expr           Expr
	Able           bool
	ConstraintName Name
}

func processCollationOnType(name Name, typ coltypes.T, c ColumnCollation) (coltypes.T, error) {
	locale := string(c)
	switch s := typ.(type) {
	case *coltypes.TString:
		return &coltypes.TCollatedString{
			TString: coltypes.TString{Variant: s.Variant, N: s.N, VisibleType: s.VisibleType},
			Locale:  locale,
		}, nil
	case *coltypes.TCollatedString:
		return nil, pgerror.NewErrorf(pgcode.Syntax,
			"multiple COLLATE declarations for column %q", name)
	case *coltypes.TArray:
		var err error
		s.ParamType, err = processCollationOnType(name, s.ParamType, c)
		if err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, pgerror.NewErrorf(pgcode.DatatypeMismatch,
			"COLLATE declaration for non-string-typed column %q", name)
	}
}

// NewColumnTableDef constructs a column definition for a CreateTable statement.
func NewColumnTableDef(
	name Name, typ coltypes.T, qualifications []NamedColumnQualification,
) (*ColumnTableDef, error) {
	d := &ColumnTableDef{
		Name: name,
		Type: typ,
	}
	d.Nullable.Nullability = SilentNull
	for _, c := range qualifications {
		switch t := c.Qualification.(type) {
		case ColumnCollation:
			locale := string(t)
			_, err := language.Parse(locale)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid locale %s", locale)
			}
			d.Type, err = processCollationOnType(name, d.Type, t)
			if err != nil {
				return nil, err
			}
		case *ColumnDefault:
			if d.HasDefaultExpr() {
				return nil, pgerror.NewErrorf(pgcode.Syntax,
					"multiple default values specified for column %q", name)
			}
			d.DefaultExpr.Expr = t.Expr
			d.DefaultExpr.ConstraintName = c.Name
			d.DefaultExpr.OnUpdateCurrentTimeStamp = t.OnUpdateCurrentTimeStamp
		case NotNullConstraint:
			if d.Nullable.Nullability == Null {
				return nil, pgerror.NewErrorf(pgcode.Syntax,
					"conflicting NULL/NOT NULL declarations for column %q", name)
			}
			d.Nullable.Nullability = NotNull
			d.Nullable.ConstraintName = c.Name
		case NullConstraint:
			if d.Nullable.Nullability == NotNull {
				return nil, pgerror.NewErrorf(pgcode.Syntax,
					"conflicting NULL/NOT NULL declarations for column %q", name)
			}
			d.Nullable.Nullability = Null
			d.Nullable.ConstraintName = c.Name
		case PrimaryKeyConstraint:
			d.PrimaryKey = true
			d.UniqueConstraintName = c.Name
		case UniqueConstraint:
			d.Unique = true
			d.UniqueConstraintName = c.Name
		case *ColumnCheckConstraint:
			d.CheckExprs = append(d.CheckExprs, ColumnTableDefCheckExpr{
				Expr:           t.Expr,
				Able:           t.Able,
				ConstraintName: c.Name,
			})
		case *ColumnFKConstraint:
			if d.HasFKConstraint() {
				return nil, pgerror.NewErrorf(pgcode.InvalidTableDefinition,
					"multiple foreign key constraints specified for column %q", name)
			}
			d.References.Table = &t.Table
			d.References.Col = t.Col
			d.References.ConstraintName = c.Name
			d.References.Actions = t.Actions
			d.References.Match = t.Match
		case *ColumnComputedDef:
			d.Computed.Computed = true
			d.Computed.Expr = t.Expr
		case *ColumnFamilyConstraint:
			if d.HasColumnFamily() {
				return nil, pgerror.NewErrorf(pgcode.InvalidTableDefinition,
					"multiple column families specified for column %q", name)
			}
			d.Family.Name = t.Family
			d.Family.Create = t.Create
			d.Family.IfNotExists = t.IfNotExists
		default:
			panic(fmt.Sprintf("unexpected column qualification: %T", c))
		}
	}
	return d, nil
}

// SetName implements the TableDef interface.
func (node *ColumnTableDef) SetName(name Name) {
	node.Name = name
}

// HasDefaultExpr returns if the ColumnTableDef has a default expression.
func (node *ColumnTableDef) HasDefaultExpr() bool {
	return node.DefaultExpr.Expr != nil
}

// HasFKConstraint returns if the ColumnTableDef has a foreign key constraint.
func (node *ColumnTableDef) HasFKConstraint() bool {
	return node.References.Table != nil
}

// IsComputed returns if the ColumnTableDef is a computed column.
func (node *ColumnTableDef) IsComputed() bool {
	return node.Computed.Computed
}

// HasColumnFamily returns if the ColumnTableDef has a column family.
func (node *ColumnTableDef) HasColumnFamily() bool {
	return node.Family.Name != "" || node.Family.Create
}

// Format implements the NodeFormatter interface.
func (node *ColumnTableDef) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.Name)
	ctx.WriteByte(' ')
	if ctx.flags.HasFlags(FmtVisableType) {
		ctx.WriteString(node.Type.ColumnType())
	} else {
		node.Type.Format(&ctx.Buffer, ctx.flags.EncodeFlags())
	}
	if node.Nullable.Nullability != SilentNull && node.Nullable.ConstraintName != "" {
		ctx.WriteString(" CONSTRAINT ")
		ctx.FormatNode(&node.Nullable.ConstraintName)
	}
	switch node.Nullable.Nullability {
	case Null:
		ctx.WriteString(" NULL")
	case NotNull:
		ctx.WriteString(" NOT NULL")
	}
	if node.PrimaryKey || node.Unique {
		if node.UniqueConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.UniqueConstraintName)
		}
		if node.PrimaryKey {
			ctx.WriteString(" PRIMARY KEY")
		} else if node.Unique {
			ctx.WriteString(" UNIQUE")
		}
	}
	if node.HasDefaultExpr() {
		if node.DefaultExpr.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.DefaultExpr.ConstraintName)
		}
		ctx.WriteString(" DEFAULT ")
		ctx.FormatNode(node.DefaultExpr.Expr)
	}
	for _, checkExpr := range node.CheckExprs {
		if checkExpr.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&checkExpr.ConstraintName)
		}
		ctx.WriteString(" CHECK (")
		ctx.FormatNode(checkExpr.Expr)
		ctx.WriteByte(')')
	}
	if node.HasFKConstraint() {
		if node.References.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.References.ConstraintName)
		}
		ctx.WriteString(" REFERENCES ")
		ctx.FormatNode(node.References.Table)
		if node.References.Col != "" {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.References.Col)
			ctx.WriteByte(')')
		}
		if node.References.Match != MatchSimple {
			ctx.WriteByte(' ')
			ctx.WriteString(node.References.Match.String())
		}
		ctx.FormatNode(&node.References.Actions)
	}
	if node.IsComputed() {
		ctx.WriteString(" AS (")
		ctx.FormatNode(node.Computed.Expr)
		ctx.WriteString(") STORED")
	}
	if node.HasColumnFamily() {
		if node.Family.Create {
			ctx.WriteString(" CREATE")
		}
		if node.Family.IfNotExists {
			ctx.WriteString(" IF NOT EXISTS")
		}
		ctx.WriteString(" FAMILY")
		if len(node.Family.Name) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&node.Family.Name)
		}
	}
}

// String implements the fmt.Stringer interface.
func (node *ColumnTableDef) String() string { return AsString(node) }

// NamedColumnQualification wraps a NamedColumnQualification with a name.
type NamedColumnQualification struct {
	Name          Name
	Qualification ColumnQualification
}

// ColumnQualification represents a constraint on a column.
type ColumnQualification interface {
	columnQualification()
}

func (ColumnCollation) columnQualification()         {}
func (*ColumnDefault) columnQualification()          {}
func (NotNullConstraint) columnQualification()       {}
func (NullConstraint) columnQualification()          {}
func (PrimaryKeyConstraint) columnQualification()    {}
func (UniqueConstraint) columnQualification()        {}
func (*ColumnCheckConstraint) columnQualification()  {}
func (*ColumnComputedDef) columnQualification()      {}
func (*ColumnFKConstraint) columnQualification()     {}
func (*ColumnFamilyConstraint) columnQualification() {}

// ColumnCollation represents a COLLATE clause for a column.
type ColumnCollation string

// ColumnDefault represents a DEFAULT clause for a column.
type ColumnDefault struct {
	Expr                     Expr
	OnUpdateCurrentTimeStamp bool
}

// NotNullConstraint represents NOT NULL on a column.
type NotNullConstraint struct{}

// NullConstraint represents NULL on a column.
type NullConstraint struct{}

// PrimaryKeyConstraint represents NULL on a column.
type PrimaryKeyConstraint struct{}

// UniqueConstraint represents UNIQUE on a column.
type UniqueConstraint struct{}

// ColumnCheckConstraint represents either a check on a column.
type ColumnCheckConstraint struct {
	Expr Expr
	Able bool
}

// ColumnFKConstraint represents a FK-constaint on a column.
type ColumnFKConstraint struct {
	Table   TableName
	Col     Name // empty-string means use PK
	Actions ReferenceActions
	Match   CompositeKeyMatchMethod
}

// ColumnComputedDef represents the description of a computed column.
type ColumnComputedDef struct {
	Expr Expr
}

// ColumnFamilyConstraint represents FAMILY on a column.
type ColumnFamilyConstraint struct {
	Family      Name
	Create      bool
	IfNotExists bool
}

// IndexTableDef represents an index definition within a CREATE TABLE
// statement.
type IndexTableDef struct {
	Name            Name
	Columns         IndexElemList
	Storing         NameList
	Interleave      *InterleaveDef
	Inverted        bool
	PartitionBy     *PartitionBy
	LocateSpaceName *Location
	Where           *Where
}

// SetName implements the TableDef interface.
func (node *IndexTableDef) SetName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *IndexTableDef) Format(ctx *FmtCtx) {
	if node.Inverted {
		ctx.WriteString("INVERTED ")
	}
	ctx.WriteString("INDEX ")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Storing != nil {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&node.Storing)
		ctx.WriteByte(')')
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
	if node.PartitionBy != nil {
		ctx.FormatNode(node.PartitionBy)
	}
}

// ConstraintTableDef represents a constraint definition within a CREATE TABLE
// statement.
type ConstraintTableDef interface {
	TableDef
	// Placeholder function to ensure that only desired types
	// (*ConstraintTableDef) conform to the ConstraintTableDef interface.
	constraintTableDef()
	SetLocateSpaceName(locateSpaceName *Location)
	//setLocateSpaceName()

	// SetName replaces the name of the definition in-place. Used in the parser.
	SetName(name Name)
}

func (*UniqueConstraintTableDef) constraintTableDef() {}

// UniqueConstraintTableDef represents a unique constraint within a CREATE
// TABLE statement.
type UniqueConstraintTableDef struct {
	IndexTableDef
	PrimaryKey      bool
	LocateSpaceName *Location
}

// SetName implements the TableDef interface.
func (node *UniqueConstraintTableDef) SetName(name Name) {
	node.Name = name
}

// SetLocateSpaceName implements the ConstraintTableDef interface.
func (node *UniqueConstraintTableDef) SetLocateSpaceName(locateSpaceName *Location) {
	node.LocateSpaceName = locateSpaceName
}

// Format implements the NodeFormatter interface.
func (node *UniqueConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	if node.PrimaryKey {
		ctx.WriteString("PRIMARY KEY ")
	} else {
		ctx.WriteString("UNIQUE ")
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Storing != nil {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&node.Storing)
		ctx.WriteByte(')')
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
	if node.PartitionBy != nil {
		ctx.FormatNode(node.PartitionBy)
	}
}

// ReferenceAction is the method used to maintain referential integrity through
// foreign keys.
type ReferenceAction int

// The values for ReferenceAction.
const (
	NoAction ReferenceAction = iota
	Restrict
	SetNull
	SetDefault
	Cascade
)

var referenceActionName = [...]string{
	NoAction:   "NO ACTION",
	Restrict:   "RESTRICT",
	SetNull:    "SET NULL",
	SetDefault: "SET DEFAULT",
	Cascade:    "CASCADE",
}

func (ra ReferenceAction) String() string {
	return referenceActionName[ra]
}

// ReferenceActions contains the actions specified to maintain referential
// integrity through foreign keys for different operations.
type ReferenceActions struct {
	Delete ReferenceAction
	Update ReferenceAction
}

// Format implements the NodeFormatter interface.
func (node *ReferenceActions) Format(ctx *FmtCtx) {
	if node.Delete != NoAction {
		ctx.WriteString(" ON DELETE ")
		ctx.WriteString(node.Delete.String())
	}
	if node.Update != NoAction {
		ctx.WriteString(" ON UPDATE ")
		ctx.WriteString(node.Update.String())
	}
}

// CompositeKeyMatchMethod is the algorithm use when matching composite keys.
// See https://github.com/znbasedb/znbase/issues/20305 or
// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
// different composite foreign key matching methods.
type CompositeKeyMatchMethod int

// The values for CompositeKeyMatchMethod.
const (
	MatchSimple CompositeKeyMatchMethod = iota
	MatchFull
	MatchPartial // Note: PARTIAL not actually supported at this point.
)

var compositeKeyMatchMethodName = [...]string{
	MatchSimple:  "MATCH SIMPLE",
	MatchFull:    "MATCH FULL",
	MatchPartial: "MATCH PARTIAL",
}

func (c CompositeKeyMatchMethod) String() string {
	return compositeKeyMatchMethodName[c]
}

// ForeignKeyConstraintTableDef represents a FOREIGN KEY constraint in the AST.
type ForeignKeyConstraintTableDef struct {
	Name            Name
	Table           TableName
	FromCols        NameList
	ToCols          NameList
	Actions         ReferenceActions
	Match           CompositeKeyMatchMethod
	LocateSpaceName *Location
}

// SetLocateSpaceName implements the ConstraintTableDef interface.
func (node *ForeignKeyConstraintTableDef) SetLocateSpaceName(locateSpaceName *Location) {
	node.LocateSpaceName = locateSpaceName
}

// Format implements the NodeFormatter interface.
func (node *ForeignKeyConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("FOREIGN KEY (")
	ctx.FormatNode(&node.FromCols)
	ctx.WriteString(") REFERENCES ")
	ctx.FormatNode(&node.Table)

	if len(node.ToCols) > 0 {
		ctx.WriteByte(' ')
		ctx.WriteByte('(')
		ctx.FormatNode(&node.ToCols)
		ctx.WriteByte(')')
	}

	if node.Match != MatchSimple {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Match.String())
	}

	ctx.FormatNode(&node.Actions)
}

// SetName implements the TableDef interface.
func (node *ForeignKeyConstraintTableDef) SetName(name Name) {
	node.Name = name
}

func (*ForeignKeyConstraintTableDef) tableDef()           {}
func (*ForeignKeyConstraintTableDef) constraintTableDef() {}

func (*CheckConstraintTableDef) tableDef()           {}
func (*CheckConstraintTableDef) constraintTableDef() {}

// CheckConstraintTableDef represents a check constraint within a CREATE
// TABLE statement.
type CheckConstraintTableDef struct {
	Name            Name
	Expr            Expr
	LocateSpaceName *Location
	Able            bool
	IsInherit       bool
	InhCount        int
}

// SetName implements the TableDef interface.
func (node *CheckConstraintTableDef) SetName(name Name) {
	node.Name = name
}

// SetLocateSpaceName implements the ConstraintTableDef interface.
func (node *CheckConstraintTableDef) SetLocateSpaceName(locateSpaceName *Location) {
	node.LocateSpaceName = locateSpaceName
}

// Format implements the NodeFormatter interface.
func (node *CheckConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("CHECK (")
	ctx.FormatNode(node.Expr)
	ctx.WriteByte(')')
}

// FamilyTableDef represents a family definition within a CREATE TABLE
// statement.
type FamilyTableDef struct {
	Name    Name
	Columns NameList
}

// SetName implements the TableDef interface.
func (node *FamilyTableDef) SetName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *FamilyTableDef) Format(ctx *FmtCtx) {
	ctx.WriteString("FAMILY ")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
}

// InterleaveDef represents an interleave definition within a CREATE TABLE
// or CREATE INDEX statement.
type InterleaveDef struct {
	Parent       TableName
	Fields       NameList
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *InterleaveDef) Format(ctx *FmtCtx) {
	ctx.WriteString(" INTERLEAVE IN PARENT ")
	ctx.FormatNode(&node.Parent)
	ctx.WriteString(" (")
	for i := range node.Fields {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.Fields[i])
	}
	ctx.WriteString(")")
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}

// PartitionBy represents an PARTITION BY definition within a CREATE/ALTER
// TABLE/INDEX statement.
type PartitionBy struct {
	Fields NameList
	// Exactly one of List or Range is required to be non-empty.
	List           []ListPartition
	Range          []RangePartition
	LocationSpace  *Location
	IsHash         bool
	IsHashQuantity bool
}

// Format implements the NodeFormatter interface.
func (node *PartitionBy) Format(ctx *FmtCtx) {
	if node == nil {
		ctx.WriteString(` PARTITION BY NOTHING`)
		return
	}
	if len(node.List) > 0 {
		if node.IsHash {
			ctx.WriteString(` PARTITION BY HASH (`)
		} else {
			ctx.WriteString(` PARTITION BY LIST (`)
		}
	} else if len(node.Range) > 0 {
		ctx.WriteString(` PARTITION BY RANGE (`)
	}
	ctx.FormatNode(&node.Fields)
	ctx.WriteString(`) (`)
	for i := range node.List {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.List[i])
	}
	for i := range node.Range {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.Range[i])
	}
	ctx.WriteString(`)`)
	if node.LocationSpace != nil {
		ctx.FormatNode(node.LocationSpace)
	}
}

// ListPartition represents a PARTITION definition within a PARTITION BY LIST.
type ListPartition struct {
	Name UnrestrictedName
	// geo-partition feature add by jiye
	LocateSpaceName  *Location
	Exprs            Exprs
	Subpartition     *PartitionBy
	IsHash           bool
	LocateSpaceNames LocationList
	HashParts        int64
}

// Format implements the NodeFormatter interface.
func (node *ListPartition) Format(ctx *FmtCtx) {
	if node.HashParts != 0 {
		ctx.WriteString(`PARTITIONS `)
		string := strconv.FormatInt(node.HashParts, 10)
		ctx.WriteString(string)
	} else {
		ctx.WriteString(`PARTITION `)
		ctx.FormatNode(&node.Name)
		if !node.IsHash {
			ctx.WriteString(` VALUES IN (`)
			ctx.FormatNode(&node.Exprs)
			ctx.WriteByte(')')
		}
		if node.Subpartition != nil {
			ctx.FormatNode(node.Subpartition)
		}
		if node.LocateSpaceName != nil {
			ctx.FormatNode(node.LocateSpaceName)
		}
	}
}

// RangePartition represents a PARTITION definition within a PARTITION BY RANGE.
type RangePartition struct {
	Name UnrestrictedName
	//geo-partition feature add by jiye
	LocateSpaceName *Location
	From            Exprs
	To              Exprs
	Subpartition    *PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *RangePartition) Format(ctx *FmtCtx) {
	ctx.WriteString(`PARTITION `)
	ctx.FormatNode(&node.Name)
	ctx.WriteString(` VALUES FROM (`)
	ctx.FormatNode(&node.From)
	ctx.WriteString(`) TO (`)
	ctx.FormatNode(&node.To)
	ctx.WriteByte(')')
	if node.Subpartition != nil {
		ctx.FormatNode(node.Subpartition)
	}
	if node.LocateSpaceName != nil {
		ctx.FormatNode(node.LocateSpaceName)
	}
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	IfNotExists   bool
	Table         TableName
	Temporary     bool
	InhRelations  TableNames
	Interleave    *InterleaveDef
	PartitionBy   *PartitionBy
	Defs          TableDefs
	AsSource      *Select
	AsColumnNames NameList // Only to be used in conjunction with AsSource
	// geo-parition feature add by jiye
	LocateSpaceName *Location
	IsLike          bool
}

// As returns true if this table represents a CREATE TABLE ... AS statement,
// false otherwise.
func (c *CreateTable) As() bool {
	return c.AsSource != nil
}

// Format implements the NodeFormatter interface.
func (c *CreateTable) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if c.Temporary {
		ctx.WriteString("TEMPORARY ")
	}
	ctx.WriteString("TABLE ")
	if c.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&c.Table)
	c.FormatBody(ctx)
}

// FormatBody formats the "body" of the create table definition - everything
// but the CREATE TABLE tableName part.
func (c *CreateTable) FormatBody(ctx *FmtCtx) {
	if c.As() {
		if len(c.AsColumnNames) > 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&c.AsColumnNames)
			ctx.WriteByte(')')
		}
		ctx.WriteString(" AS ")
		ctx.FormatNode(c.AsSource)
	} else {
		ctx.WriteString(" (")
		ctx.FormatNode(&c.Defs)
		ctx.WriteByte(')')
		if c.Interleave != nil {
			ctx.FormatNode(c.Interleave)
		}
		if c.PartitionBy != nil {
			ctx.FormatNode(c.PartitionBy)
		}
	}
}

// HoistConstraints finds column constraints defined inline with their columns
// and makes them table-level constraints, stored in n.Defs. For example, the
// foreign key constraint in
//
//     CREATE TABLE foo (a INT REFERENCES bar(a))
//
// gets pulled into a top-level constraint like:
//
//     CREATE TABLE foo (a INT, FOREIGN KEY (a) REFERENCES bar(a))
//
// Similarly, the CHECK constraint in
//
//    CREATE TABLE foo (a INT CHECK (a < 1), b INT)
//
// gets pulled into a top-level constraint like:
//
//    CREATE TABLE foo (a INT, b INT, CHECK (a < 1))
//
// Note that some SQL databases require that a constraint attached to a column
// to refer only to the column it is attached to. We follow Postgres' behavior,
// however, in omitting this restriction by blindly hoisting all column
// constraints. For example, the following table definition is accepted in
// ZNBaseDB and Postgres, but not necessarily other SQL databases:
//
//    CREATE TABLE foo (a INT CHECK (a < b), b INT)
//
func (c *CreateTable) HoistConstraints() {
	for _, d := range c.Defs {
		if col, ok := d.(*ColumnTableDef); ok {
			for _, checkExpr := range col.CheckExprs {
				c.Defs = append(c.Defs,
					&CheckConstraintTableDef{
						Expr:     checkExpr.Expr,
						Name:     checkExpr.ConstraintName,
						Able:     checkExpr.Able,
						InhCount: 1,
					},
				)
			}
			col.CheckExprs = nil
			if col.HasFKConstraint() {
				var targetCol NameList
				if col.References.Col != "" {
					targetCol = append(targetCol, col.References.Col)
				}
				c.Defs = append(c.Defs, &ForeignKeyConstraintTableDef{
					Table:    *col.References.Table,
					FromCols: NameList{col.Name},
					ToCols:   targetCol,
					Name:     col.References.ConstraintName,
					Actions:  col.References.Actions,
					Match:    col.References.Match,
				})
				col.References.Table = nil
			}
		}
	}
}

// CreateSequence represents a CREATE SEQUENCE statement.
type CreateSequence struct {
	IfNotExists bool
	Name        TableName
	Temporary   bool
	Options     SequenceOptions
}

// Format implements the NodeFormatter interface.
func (n *CreateSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if n.Temporary {
		ctx.WriteString("TEMPORARY ")
	}
	ctx.WriteString("SEQUENCE ")
	if n.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&n.Name)
	ctx.FormatNode(&n.Options)
}

// SequenceOptions represents a list of sequence options.
type SequenceOptions []SequenceOption

// Format implements the NodeFormatter interface.
func (node *SequenceOptions) Format(ctx *FmtCtx) {
	for i := range *node {
		option := &(*node)[i]
		ctx.WriteByte(' ')
		switch option.Name {
		case SeqOptCycle, SeqOptNoCycle:
			ctx.WriteString(option.Name)
		case SeqOptCache:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			ctx.Printf("%d", *option.IntVal)
		case SeqOptMaxValue, SeqOptMinValue:
			if option.IntVal == nil {
				ctx.WriteString("NO ")
				ctx.WriteString(option.Name)
			} else {
				ctx.WriteString(option.Name)
				ctx.WriteByte(' ')
				ctx.Printf("%d", *option.IntVal)
			}
		case SeqOptStart:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			if option.OptionalWord {
				ctx.WriteString("WITH ")
			}
			ctx.Printf("%d", *option.IntVal)
		case SeqOptIncrement:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			if option.OptionalWord {
				ctx.WriteString("BY ")
			}
			ctx.Printf("%d", *option.IntVal)
		case SeqOptVirtual:
			ctx.WriteString(option.Name)
		case SeqOptOwnedBy:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			switch option.ColumnItemVal {
			case nil:
				ctx.WriteString("NONE")
			default:
				ctx.FormatNode(option.ColumnItemVal)
			}
		default:
			panic(fmt.Sprintf("unexpected SequenceOption: %v", option))
		}
	}
}

// SequenceOption represents an option on a CREATE SEQUENCE statement.
type SequenceOption struct {
	Name string

	IntVal *int64

	OptionalWord  bool
	Cycle         bool
	ColumnItemVal *ColumnItem
}

// Names of options on CREATE SEQUENCE.
const (
	SeqOptAs        = "AS"
	SeqOptCycle     = "CYCLE"
	SeqOptNoCycle   = "NO CYCLE"
	SeqOptOwnedBy   = "OWNED BY"
	SeqOptCache     = "CACHE"
	SeqOptIncrement = "INCREMENT"
	SeqOptMinValue  = "MINVALUE"
	SeqOptMaxValue  = "MAXVALUE"
	SeqOptStart     = "START"
	SeqOptVirtual   = "VIRTUAL"

	// Avoid unused warning for constants.
	_ = SeqOptAs
	_ = SeqOptOwnedBy
)

//LikeTableDef represents a LIKE table declaration on a CREATE TABLE statement.
type LikeTableDef struct {
	Name    TableName
	Options []LikeTableOption
}

//SetName func
func (node *LikeTableDef) SetName(name Name) {
}

//
// LikeTableOption represents an individual INCLUDING / EXCLUDING statement
// on a LIKE table declaration.
type LikeTableOption struct {
	Excluded bool
	Opt      LikeTableOpt
}

//Format h
func (node *LikeTableDef) Format(ctx *FmtCtx) {
	ctx.WriteString("LIKE ")
	ctx.FormatNode(&node.Name)
	for _, o := range node.Options {
		ctx.WriteString(" ")
		ctx.FormatNode(o)
	}
}

// Format implements the NodeFormatter interface.
func (l LikeTableOption) Format(ctx *FmtCtx) {
	if l.Excluded {
		ctx.WriteString("EXCLUDING ")
	} else {
		ctx.WriteString("INCLUDING ")
	}
	ctx.WriteString(l.Opt.String())
}

// LikeTableOpt represents one of the types of things that can be included or
// excluded in a LIKE table declaration. It's a bitmap, where each of the Opt
// values is a single enabled bit in the map.
type LikeTableOpt int

// The values for LikeTableOpt.
const (
	LikeTableOptConstraints LikeTableOpt = 1 << iota
	LikeTableOptDefaults
	LikeTableOptGenerated
	LikeTableOptIndexes

	// Make sure this field stays last!
	likeTableOptInvalid
)

// LikeTableOptAll is the full LikeTableOpt bitmap.
const LikeTableOptAll = ^likeTableOptInvalid

func (o LikeTableOpt) String() string {
	switch o {
	case LikeTableOptConstraints:
		return "CONSTRAINTS"
	case LikeTableOptDefaults:
		return "DEFAULTS"
	case LikeTableOptGenerated:
		return "GENERATED"
	case LikeTableOptIndexes:
		return "INDEXES"
	case LikeTableOptAll:
		return "ALL"
	default:
		panic("unknown like table opt" + strconv.Itoa(int(o)))
	}
}

// ToUserOptions converts KVOptions to useroption.List using
// typeAsString to convert exprs to strings.
func (o KVOptions) ToUserOptions(
	typeAsStringOrNull func(e Expr, op string) (func() (bool, string, error), error), op string,
) (useroption.List, error) {
	userOptions := make(useroption.List, len(o))

	for i, kvo := range o {
		option, err := useroption.ToOption(kvo.Key.String())
		if err != nil {
			return nil, err
		}

		if kvo.Value != nil {
			if kvo.Value == DNull {
				userOptions[i] = useroption.UserOption{
					Option: option, HasValue: true, Value: func() (bool, string, error) {
						return true, "", nil
					},
				}
			} else {
				strFn, err := typeAsStringOrNull(kvo.Value, op)
				if err != nil {
					return nil, err
				}
				userOptions[i] = useroption.UserOption{
					Option: option, Value: strFn, HasValue: true,
				}
			}
		} else {
			userOptions[i] = useroption.UserOption{
				Option: option, HasValue: false,
			}
		}
	}
	return userOptions, nil
}

// CreateUser represents a CREATE USER statement.
type CreateUser struct {
	Name        Expr
	KVOptions   KVOptions
	IfNotExists bool
}

// Format implements the NodeFormatter interface.
func (n *CreateUser) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE USER ")
	if n.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(n.Name)
	if len(n.KVOptions) > 0 {
		ctx.WriteString(" WITH")
		n.KVOptions.formatAsUserOptions(ctx)
	}
}

// AlterUser represents an ALTER USER statement.
type AlterUser struct {
	Name      Expr
	IfExists  bool
	KVOptions KVOptions
}

//Format implements
func (n *AlterUser) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER USER ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(n.Name)
	if len(n.KVOptions) > 0 {
		ctx.WriteString(" WITH")
		n.KVOptions.formatAsUserOptions(ctx)
	}
}

func (o *KVOptions) formatAsUserOptions(ctx *FmtCtx) {
	for _, kvo := range *o {
		ctx.WriteString(" ")
		ctx.WriteString(
			// "_" replaces space (" ") in YACC for handling tree.Name formatting.
			strings.ReplaceAll(strings.ToUpper(kvo.Key.String()), "_", " "),
		)

		if strings.ToUpper(kvo.Key.String()) == "PASSWORD" {
			ctx.WriteString(" ")
			if ctx.flags.HasFlags(FmtShowPasswords) {
				ctx.FormatNode(kvo.Value)
			} else {
				ctx.WriteString("*****")
			}
		} else if kvo.Value == DNull {
			ctx.WriteString(" ")
			ctx.FormatNode(kvo.Value)
		} else if kvo.Value != nil {
			ctx.WriteString(" ")
			ctx.FormatNode(kvo.Value)
		}
	}
}

//AlterUserDefaultSchma represent an ALTER USER "user" SET SEARCH_PATH TO "user"
type AlterUserDefaultSchma struct {
	Name      Expr
	Values    Exprs
	DBName    Name
	IsDefalut bool
}

// Format implements the NodeFormatter interface.
func (n *AlterUserDefaultSchma) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER USER ")
	ctx.FormatNode(n.Name)
	if n.DBName != "" {
		ctx.WriteString(" IN DATABASE ")
		ctx.FormatNode(&n.DBName)
	}
	ctx.WriteString(" SET SEARCH_PATH TO ")
	if n.IsDefalut {
		ctx.WriteString(" DEFAULT ")
	} else {
		ctx.FormatNode(&n.Values)
	}
}

// CreateRole represents a CREATE ROLE statement.
type CreateRole struct {
	Name        Expr
	IfNotExists bool
}

// Format implements the NodeFormatter interface.
func (n *CreateRole) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ROLE ")
	if n.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(n.Name)
}

// CreateView represents a CREATE VIEW statement.
type CreateView struct {
	Name        TableName
	ColumnNames NameList
	AsSource    *Select
	IfNotExists bool
	IsReplace   bool
	Temporary   bool

	Materialized bool
}

// Format implements the NodeFormatter interface.
func (n *CreateView) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if n.IsReplace {
		ctx.WriteString("OR REPLACE ")
	}
	if n.Temporary {
		ctx.WriteString("TEMPORARY ")
	}
	if n.Materialized {
		ctx.WriteString("MATERIALIZED ")
	}
	ctx.WriteString("VIEW ")

	if n.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&n.Name)

	if len(n.ColumnNames) > 0 {
		ctx.WriteByte(' ')
		ctx.WriteByte('(')
		ctx.FormatNode(&n.ColumnNames)
		ctx.WriteByte(')')
	}

	ctx.WriteString(" AS ")
	ctx.FormatNode(n.AsSource)
}

// RefreshMaterializedView represents a REFRESH MATERIALIZED VIEW statement.
type RefreshMaterializedView struct {
	Name *UnresolvedObjectName
}

var _ Statement = &RefreshMaterializedView{}

// Format implements the NodeFormatter interface.
func (node *RefreshMaterializedView) Format(ctx *FmtCtx) {
	ctx.WriteString("REFRESH MATERIALIZED VIEW ")
	ctx.FormatNode(node.Name)
}

// CreateStats represents a CREATE STATISTICS statement.
type CreateStats struct {
	Name        Name
	ColumnNames NameList
	Table       TableExpr
	Options     CreateStatsOptions
}

// Format implements the NodeFormatter interface.
func (n *CreateStats) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE STATISTICS ")
	ctx.FormatNode(&n.Name)

	if len(n.ColumnNames) > 0 {
		ctx.WriteString(" ON ")
		ctx.FormatNode(&n.ColumnNames)
	}

	ctx.WriteString(" FROM ")
	ctx.FormatNode(n.Table)

	if !n.Options.Empty() {
		ctx.WriteString(" WITH OPTIONS ")
		ctx.FormatNode(&n.Options)
	}
}

// CreateStatsOptions contains options for CREATE STATISTICS.
type CreateStatsOptions struct {
	// Throttling enables throttling and indicates the fraction of time we are
	// idling (between 0 and 1).
	Throttling float64

	// AsOf performs a historical read at the given timestamp.
	AsOf AsOfClause
}

// Empty returns true if no options were provided.
func (o *CreateStatsOptions) Empty() bool {
	return o.Throttling == 0 && o.AsOf.Expr == nil
}

// Format implements the NodeFormatter interface.
func (o *CreateStatsOptions) Format(ctx *FmtCtx) {
	sep := ""
	if o.Throttling != 0 {
		fmt.Fprintf(ctx, "THROTTLING %g", o.Throttling)
		sep = " "
	}
	if o.AsOf.Expr != nil {
		ctx.WriteString(sep)
		ctx.FormatNode(&o.AsOf)
		sep = " "
	}
}

// CombineWith combines two options, erroring out if the two options contain
// incompatible settings.
func (o *CreateStatsOptions) CombineWith(other *CreateStatsOptions) error {
	if other.Throttling != 0 {
		if o.Throttling != 0 {
			return errors.New("THROTTLING specified multiple times")
		}
		o.Throttling = other.Throttling
	}
	if other.AsOf.Expr != nil {
		if o.AsOf.Expr != nil {
			return errors.New("AS OF specified multiple times")
		}
		o.AsOf = other.AsOf
	}
	return nil
}

// CreateSchema represents a CREATE SCHEMA statement.
type CreateSchema struct {
	IfNotExists bool
	Schema      SchemaName
}

// Format implements the NodeFormatter interface.
func (n *CreateSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SCHEMA ")
	if n.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&n.Schema)
}
