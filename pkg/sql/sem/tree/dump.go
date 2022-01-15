// Copyright 2018  The Cockroach Authors.
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
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/util/pretty"
)

// CurrentSession includes Database and SearchPath.
type CurrentSession struct {
	Database   string
	SearchPath sessiondata.SearchPath
}

// DescriptorCoverage specifies whether or not a subset of descriptors were
// requested or if all the descriptors were requested, so all the descriptors
// are covered in a given backup.
type DescriptorCoverage int32

const (
	// RequestedDescriptors table coverage means that the backup is not
	// guaranteed to have all of the cluster data. This can be accomplished by
	// backing up a specific subset of tables/databases. Note that even if all
	// of the tables and databases have been included in the backup manually, a
	// backup is not said to have complete table coverage unless it was created
	// by a `BACKUP TO` command.
	RequestedDescriptors DescriptorCoverage = iota
	// AllDescriptors table coverage means that backup is guaranteed to have all the
	// relevant data in the cluster. These can only be created by running a
	// full cluster backup with `BACKUP TO`.
	AllDescriptors
)

// Dump represents a DUMP statement.
type Dump struct {
	FileFormat         string
	File               Expr
	Targets            TargetList
	DescriptorCoverage DescriptorCoverage
	ExpendQuery        *ExpendSelect
	AsOf               AsOfClause
	IncrementalFrom    Exprs
	Options            KVOptions
	IsQuery            bool
	IsSettings         bool
	To                 PartitionedBackup
	Nested             bool
	AppendToLatest     bool
	CurrentPath        *CurrentSession
	ScheduleID         int64
}

var _ Statement = &Dump{}

// Format implements the NodeFormatter interface.
func (node *Dump) Format(ctx *FmtCtx) {
	if node.IsSettings {
		tbl := MakeTableNameWithSchema("", "zbdb_internal", "cluster_settings")
		selectClause := &SelectClause{
			Exprs:       SelectExprs{StarSelectExpr()},
			From:        &From{Tables: []TableExpr{&tbl}},
			TableSelect: true,
		}
		ExpendSelectClause := &Select{Select: selectClause}
		clusterSettings := &ExpendSelect{Query: ExpendSelectClause}
		node.ExpendQuery = clusterSettings
	}
	if node.IsQuery {
		ctx.WriteString("DUMP TO ")
	} else {
		ctx.WriteString("DUMP ")
		if node.DescriptorCoverage == RequestedDescriptors {
			ctx.FormatNode(&node.Targets)
		}
		ctx.WriteString(" TO ")
	}
	ctx.WriteString(node.FileFormat)
	ctx.WriteString(" ")
	ctx.FormatNode(node.File)
	if node.IsQuery {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(node.ExpendQuery)
	}
	if node.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
	}
	if node.IncrementalFrom != nil {
		ctx.WriteString(" INCREMENTAL FROM ")
		ctx.FormatNode(&node.IncrementalFrom)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// StatementType implements the Statement interface.
func (*Dump) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Dump) StatementTag() string {
	return "DUMP"
}

func (node *Dump) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.RLTableRow, 0, 8)
	if node.IsQuery {
		items = append(items, p.row("DUMP TO", pretty.Nil))
	} else {
		items = append(items, p.row("DUMP", pretty.Nil))
		items = append(items, node.Targets.docRow(p))
		items = append(items, p.row("TO", pretty.Nil))
	}
	items = append(items, p.row(" "+node.FileFormat, p.Doc(node.File)))
	if node.IsQuery {
		items = append(items, p.row("FROM", p.Doc(node.ExpendQuery)))
	}

	if node.AsOf.Expr != nil {
		items = append(items, node.AsOf.docRow(p))
	}
	if node.IncrementalFrom != nil {
		items = append(items, p.row("INCREMENTAL FROM", p.Doc(&node.IncrementalFrom)))
	}

	if node.Options != nil {
		items = append(items, p.row("WITH", p.Doc(&node.Options)))
	}
	return p.rlTable(items...)
}

// string is part of the stringer interface.
func (node *Dump) String() string { return AsString(node) }

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (node *Dump) copyNode() *Dump {
	stmtCopy := *node
	stmtCopy.IncrementalFrom = append(Exprs(nil), node.IncrementalFrom...)
	stmtCopy.Options = append(KVOptions(nil), node.Options...)
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (node *Dump) walkStmt(v Visitor) Statement {
	ret := node
	if node.AsOf.Expr != nil {
		e, changed := WalkExpr(v, node.AsOf.Expr)
		if changed {
			if ret == node {
				ret = node.copyNode()
			}
			ret.AsOf.Expr = e
		}
	}

	{
		e, changed := WalkExpr(v, node.File)
		if changed {
			if ret == node {
				ret = node.copyNode()
			}
			ret.File = e
		}
	}

	for i, expr := range node.IncrementalFrom {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == node {
				ret = node.copyNode()
			}
			ret.IncrementalFrom[i] = e
		}
	}
	{
		opts, changed := walkKVOptions(v, node.Options)
		if changed {
			if ret == node {
				ret = node.copyNode()
			}
			ret.Options = opts
		}
	}
	return ret
}

// PartitionedBackup is a list of destination URIs for a single BACKUP. A single
// URI corresponds to the special case of a regular backup, and multiple URIs
// correspond to a partitioned backup whose locality configuration is
// specified by LOCALITY url params.
type PartitionedBackup []Expr

// Change changes Expr to []Expr
func Change(expr Expr) (exprs []Expr) {
	exprs = append(exprs, expr)
	return exprs
}

// Format implements the NodeFormatter interface.
func (node *PartitionedBackup) Format(ctx *FmtCtx) {
	if len(*node) > 1 {
		ctx.WriteString("(")
	}
	ctx.FormatNode((*Exprs)(node))
	if len(*node) > 1 {
		ctx.WriteString(")")
	}
}

// StringOrPlaceholderOptList is a list of strings or placeholders.
// It is unused now.
// type StringOrPlaceholderOptList []Expr

// Format implements the NodeFormatter interface.
// It is unused now.
//func (node *StringOrPlaceholderOptList) Format(ctx *FmtCtx) {
//	if len(*node) > 1 {
//		ctx.WriteString("(")
//	}
//	ctx.FormatNode((*Exprs)(node))
//	if len(*node) > 1 {
//		ctx.WriteString(")")
//	}
//}
