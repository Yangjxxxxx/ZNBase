// Copyright 2015 The Cockroach Authors.
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

package tree

import (
	"bytes"
	"fmt"

	"github.com/znbasedb/znbase/pkg/sql/coltypes"
)

// AlterTable represents an ALTER TABLE statement.
type AlterTable struct {
	IfExists bool
	Table    TableName
	Cmds     AlterTableCmds
	IsAuto   bool
}

// Format implements the NodeFormatter interface.
func (node *AlterTable) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TABLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Table)
	ctx.FormatNode(&node.Cmds)
}

// AlterViewAS represents an ALTER VIEW statement.
type AlterViewAS struct {
	Name     TableName
	IfExists bool
	AsSource *Select
}

// Format implements the NodeFormatter interface.
func (node *AlterViewAS) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER VIEW ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" AS ")
	ctx.FormatNode(node.AsSource)
}

// AlterTableCmds represents a list of table alterations.
type AlterTableCmds []AlterTableCmd

// Format implements the NodeFormatter interface.
func (node *AlterTableCmds) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(",")
		}
		ctx.FormatNode(n)
	}
}

// AlterTableCmd represents a table modification operation.
type AlterTableCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterTable*) conform to the AlterTableCmd interface.
	alterTableCmd()
}

func (*AlterTableAddColumn) alterTableCmd()   {}
func (*AlterTableAddIndex) alterTableCmd()    {}
func (*AlterTableRenameIndex) alterTableCmd() {}
func (*AlterTableDropIndex) alterTableCmd()   {}

//func (*AlterTableDropPrimary) alterTableCmd()        {}
func (*AlterTableAddConstraint) alterTableCmd()      {}
func (*AlterTableAlterColumnType) alterTableCmd()    {}
func (*AlterTableAlterPrimaryKey) alterTableCmd()    {}
func (*AlterTableDropColumn) alterTableCmd()         {}
func (*AlterTableDropConstraint) alterTableCmd()     {}
func (*AlterTableDropNotNull) alterTableCmd()        {}
func (*AlterTableDropStored) alterTableCmd()         {}
func (*AlterTableNoInherit) alterTableCmd()          {}
func (*AlterTableRenameColumn) alterTableCmd()       {}
func (*AlterTableRenameConstraint) alterTableCmd()   {}
func (*AlterTableRenameTable) alterTableCmd()        {}
func (*AlterTableSetAudit) alterTableCmd()           {}
func (*AlterTableSetDefault) alterTableCmd()         {}
func (*AlterTableSetNotNull) alterTableCmd()         {}
func (*AlterTableValidateConstraint) alterTableCmd() {}
func (*AlterTablePartitionBy) alterTableCmd()        {}
func (*AlterTableInjectStats) alterTableCmd()        {}
func (*AlterTableAbleConstraint) alterTableCmd()     {}
func (*AlterTableAbleFlashBack) alterTableCmd()      {}

var _ AlterTableCmd = &AlterTableAddColumn{}
var _ AlterTableCmd = &AlterTableAddIndex{}
var _ AlterTableCmd = &AlterTableRenameIndex{}
var _ AlterTableCmd = &AlterTableDropIndex{}

//var _ AlterTableCmd = &AlterTableDropPrimary{}
var _ AlterTableCmd = &AlterTableAddConstraint{}
var _ AlterTableCmd = &AlterTableAlterColumnType{}
var _ AlterTableCmd = &AlterTableDropColumn{}
var _ AlterTableCmd = &AlterTableDropConstraint{}
var _ AlterTableCmd = &AlterTableDropNotNull{}
var _ AlterTableCmd = &AlterTableDropStored{}
var _ AlterTableCmd = &AlterTableNoInherit{}
var _ AlterTableCmd = &AlterTableRenameColumn{}
var _ AlterTableCmd = &AlterTableRenameConstraint{}
var _ AlterTableCmd = &AlterTableRenameTable{}
var _ AlterTableCmd = &AlterTableSetAudit{}
var _ AlterTableCmd = &AlterTableSetDefault{}
var _ AlterTableCmd = &AlterTableSetNotNull{}
var _ AlterTableCmd = &AlterTableValidateConstraint{}
var _ AlterTableCmd = &AlterTablePartitionBy{}
var _ AlterTableCmd = &AlterTableInjectStats{}
var _ AlterTableCmd = &AlterTableAbleConstraint{}
var _ AlterTableCmd = &AlterTableAbleFlashBack{}

// ColumnMutationCmd is the subset of AlterTableCmds that modify an
// existing column.
type ColumnMutationCmd interface {
	AlterTableCmd
	GetColumn() Name
}

//AlterTableAddColumn represents an ADD COLUMN command.
type AlterTableAddColumn struct {
	IfNotExists bool
	ColumnDef   *ColumnTableDef
}

//Format implements the NodeFormatter interface.
func (node *AlterTableAddColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD COLUMN ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(node.ColumnDef)
}

//AlterTableAddIndex h
type AlterTableAddIndex struct {
	Name        Name
	Unique      bool
	IfNotExists bool
	Columns     IndexElemList
}

//Format ADD opt_unique INDEX opt_index_name  '(' index_params ')'
func (node *AlterTableAddIndex) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD ")
	if node.Unique {
		ctx.WriteString("UNIQUE ")
	}
	ctx.WriteString(" INDEX ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
}

//AlterTableRenameIndex struct
type AlterTableRenameIndex struct {
	OldIndexName UnrestrictedName
	NewIndexName UnrestrictedName
	IfExists     bool
}

//Format RENAME INDEX  index_name TO index_name
func (node *AlterTableRenameIndex) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME ")
	ctx.WriteString(" INDEX ")
	ctx.FormatNode(&node.OldIndexName)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewIndexName)
}

//AlterTableDropIndex struct
type AlterTableDropIndex struct {
	Index        UnrestrictedName
	IfExists     bool
	DropBehavior DropBehavior
}

//Format DROP INDEX index_name
func (node *AlterTableDropIndex) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP ")
	ctx.WriteString(" INDEX ")
	ctx.FormatNode(&node.Index)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

//HoistAddColumnConstraints a func
func (node *AlterTable) HoistAddColumnConstraints() {
	var normalizedCmds AlterTableCmds

	for _, cmd := range node.Cmds {
		normalizedCmds = append(normalizedCmds, cmd)

		if t, ok := cmd.(*AlterTableAddColumn); ok {
			d := t.ColumnDef
			for _, checkExpr := range d.CheckExprs {
				normalizedCmds = append(normalizedCmds,
					&AlterTableAddConstraint{
						ConstraintDef: &CheckConstraintTableDef{
							Expr:     checkExpr.Expr,
							Able:     checkExpr.Able,
							Name:     checkExpr.ConstraintName,
							InhCount: 1,
						},
						ValidationBehavior: ValidationDefault,
					},
				)
			}
			d.CheckExprs = nil
		}
	}
	node.Cmds = normalizedCmds
}

// ValidationBehavior specifies whether or not a constraint is validated.
type ValidationBehavior int

const (
	// ValidationDefault is the default validation behavior (immediate).
	ValidationDefault ValidationBehavior = iota
	// ValidationSkip skips validation of any existing data.
	ValidationSkip
)

// AlterTableAddConstraint represents an ADD CONSTRAINT command.
type AlterTableAddConstraint struct {
	ConstraintDef      ConstraintTableDef
	ValidationBehavior ValidationBehavior
}

//Format implements the NodeFormatter interface.
func (n *AlterTableAddConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD ")
	ctx.FormatNode(n.ConstraintDef)
	if n.ValidationBehavior == ValidationSkip {
		ctx.WriteString(" NOT VALID")
	}
}

//AlterTableAlterColumnType represents an ALTER TABLE ALTER COLUMN TYPE command.
type AlterTableAlterColumnType struct {
	Collation string
	Column    Name
	ToType    coltypes.T
	Using     Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterColumnType) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET DATA TYPE ")
	if ctx.flags.HasFlags(FmtVisableType) {
		ctx.WriteString(node.ToType.ColumnType())
	} else {
		node.ToType.Format(&ctx.Buffer, ctx.flags.EncodeFlags())
	}
	if len(node.Collation) > 0 {
		ctx.WriteString(" COLLATE ")
		ctx.WriteString(node.Collation)
	}
	if node.Using != nil {
		ctx.WriteString(" USING ")
		ctx.FormatNode(node.Using)
	}
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableAlterColumnType) GetColumn() Name {
	return node.Column
}

// AlterTableAlterPrimaryKey represents an ALTER TABLE ALTER PRIMARY KEY command.
type AlterTableAlterPrimaryKey struct {
	Columns    IndexElemList
	Interleave *InterleaveDef
	Name       Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterPrimaryKey) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER PRIMARY KEY USING COLUMNS (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteString(")")
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	IfExists     bool
	Column       Name
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP COLUMN ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Column)
	if node.DropBehavior != DropDefault {
		ctx.Printf(" %s", node.DropBehavior)
	}
}

// AlterTableDropConstraint represents a DROP CONSTRAINT command.
type AlterTableDropConstraint struct {
	IfExists     bool
	Constraint   Name
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP CONSTRAINT ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Constraint)
	if node.DropBehavior != DropDefault {
		ctx.Printf(" %s", node.DropBehavior)
	}
}

// AlterTableAbleConstraint represents a ALTER TABLE command.
type AlterTableAbleConstraint struct {
	IfExists   bool
	Able       bool
	Constraint Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAbleConstraint) Format(ctx *FmtCtx) {
	if node.Able {
		ctx.WriteString(" DISABLE CONSTRAINT ")
	} else {
		ctx.WriteString(" ENABLE CONSTRAINT ")
	}
	ctx.FormatNode(&node.Constraint)
}

// AlterTableAbleFlashBack represents a ALTER TABLE command.
type AlterTableAbleFlashBack struct {
	Able    bool
	TTLDays int64
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAbleFlashBack) Format(ctx *FmtCtx) {
	if node.Able {
		ctx.WriteString(" ENABLE FLASHBACK")
		TTLDays := fmt.Sprintf(" WITH TTLDAYS %d", node.TTLDays)
		ctx.WriteString(TTLDays)
	} else {
		ctx.WriteString(" DISABLE FLASHBACK")
	}
}

// AlterTableNoInherit represents a NO INHERIT command.
type AlterTableNoInherit struct {
	Table TableName
}

// Format implements the NodeFormatter interface.
func (node *AlterTableNoInherit) Format(ctx *FmtCtx) {
	ctx.WriteString(" NO INHERIT ")
	ctx.FormatNode(&node.Table)
}

// AlterTableValidateConstraint represents a VALIDATE CONSTRAINT command.
type AlterTableValidateConstraint struct {
	Constraint Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableValidateConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" VALIDATE CONSTRAINT ")
	ctx.FormatNode(&node.Constraint)
}

// AlterTableRenameTable represents an ALTE RTABLE RENAME TO command.
type AlterTableRenameTable struct {
	NewName TableName
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameTable) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableRenameColumn represents an ALTER TABLE RENAME [COLUMN] command.
type AlterTableRenameColumn struct {
	Column  Name
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableRenameConstraint represents an ALTER TABLE RENAME CONSTRAINT command.
type AlterTableRenameConstraint struct {
	Constraint Name
	NewName    Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME CONSTRAINT ")
	ctx.FormatNode(&node.Constraint)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableSetDefault represents an ALTER COLUMN SET DEFAULT
// or DROP DEFAULT command.
type AlterTableSetDefault struct {
	Column  Name
	Default Expr
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetDefault) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetDefault) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	if node.Default == nil {
		ctx.WriteString(" DROP DEFAULT")
	} else {
		ctx.WriteString(" SET DEFAULT ")
		ctx.FormatNode(node.Default)
	}
}

// AlterTableDropNotNull represents an ALTER COLUMN DROP NOT NULL
// command.
type AlterTableDropNotNull struct {
	Column Name
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableDropNotNull) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP NOT NULL")
}

// AlterTableDropStored represents an ALTER COLUMN DROP STORED command
// to remove the computed-ness from a column.
type AlterTableDropStored struct {
	Column Name
}

// GetColumn implemnets the ColumnMutationCmd interface.
func (node *AlterTableDropStored) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropStored) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP STORED")
}

// AlterTablePartitionBy represents an ALTER TABLE PARTITION BY
// command.
type AlterTablePartitionBy struct {
	*PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *AlterTablePartitionBy) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.PartitionBy)
}

// AuditMode represents a table audit mode
type AuditMode int

const (
	// AuditModeDisable is the default mode - no audit.
	AuditModeDisable AuditMode = iota
	// AuditModeReadWrite enables audit on read or write statements.
	AuditModeReadWrite
)

var auditModeName = [...]string{
	AuditModeDisable:   "OFF",
	AuditModeReadWrite: "READ WRITE",
}

func (m AuditMode) String() string {
	return auditModeName[m]
}

// AlterTableSetAudit represents an ALTER TABLE AUDIT SET statement.
type AlterTableSetAudit struct {
	Mode AuditMode
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetAudit) Format(ctx *FmtCtx) {
	ctx.WriteString(" EXPERIMENTAL_AUDIT SET ")
	ctx.WriteString(node.Mode.String())
}

// AlterTableInjectStats represents an ALTER TABLE INJECT STATISTICS statement.
type AlterTableInjectStats struct {
	Stats Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterTableInjectStats) Format(ctx *FmtCtx) {
	ctx.WriteString(" INJECT STATISTICS ")
	ctx.FormatNode(node.Stats)
}

var _ AlterTableCmd = &AlterTableLocateIn{}

func (*AlterTableLocateIn) alterTableCmd() {}

// AlterTableLocateIn represents an ALTER TABLE LOCATE IN
// command.
type AlterTableLocateIn struct {
	LocateSpaceName *Location
}

// Format implements the NodeFormatter interface.
func (node *AlterTableLocateIn) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.LocateSpaceName)
}

var _ AlterTableCmd = &AlterParitionLocateIn{}

// alterTableCmd implements the AlterTableCmd interface.
func (*AlterParitionLocateIn) alterTableCmd() {}

// AlterParitionLocateIn represents an ALTER TABLE LOCATE IN
// command.
type AlterParitionLocateIn struct {
	ParitionName    Name
	LocateSpaceName *Location
}

// Format implements the NodeFormatter interface.
func (node *AlterParitionLocateIn) Format(ctx *FmtCtx) {
	tempName := ctx.Buffer.Bytes()
	flagExplain := 0
	if tempName[0] == 69 {
		flagExplain = 1
	}
	tempName = tempName[bytes.LastIndexByte(tempName, byte(' ')):]
	ctx.Buffer.Reset()
	tableName := string(tempName)
	if flagExplain == 1 {
		_, _ = ctx.WriteString(`EXPLAIN `)
	}
	_, _ = ctx.WriteString(`ALTER PARTITION `)
	ctx.FormatNode(&node.ParitionName)
	_, _ = ctx.WriteString(` OF TABLE`)
	_, _ = ctx.WriteString(tableName)
	ctx.FormatNode(node.LocateSpaceName)
}

// AlterTableSetNotNull represents an ALTER COLUMN SET NOT NULL
// command.
type AlterTableSetNotNull struct {
	Column Name
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetNotNull) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET NOT NULL")
}
