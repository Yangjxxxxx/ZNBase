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

import "github.com/znbasedb/znbase/pkg/util/pretty"

const (
	// DatabaseSnapshotType is the database type snapshot
	DatabaseSnapshotType = 0
	// TableSnapshotType is the table type snap shot.
	TableSnapshotType = 1
	// PartitionSnapshotType is the partition sanp shot.
	// PartitionSnapshotType = 2
)

// Snapshot represents the Snapshot
type Snapshot struct {
	Type         int
	TableName    TableName
	DatabaseName Name
}

// CreateSnapshot represents a CREATE SNAPSHOT statement.
type CreateSnapshot struct {
	Name         Name
	Type         int
	TableName    TableName
	DatabaseName Name
	AsOf         AsOfClause
	Options      KVOptions
}

// Format implements the NodeFormatter interface.
func (n *CreateSnapshot) Format(ctx *FmtCtx) {
	_, _ = ctx.WriteString("CREATE SNAPSHOT ")
	ctx.FormatNode(&n.Name)
	_, _ = ctx.WriteString(" ON ")
	if n.Type == DatabaseSnapshotType {
		_, _ = ctx.WriteString("DATABASE ")
		ctx.FormatNode(&n.DatabaseName)
	} else if n.Type == TableSnapshotType {
		_, _ = ctx.WriteString("TABLE ")
		ctx.FormatNode(&n.TableName)
	}
	_, _ = ctx.WriteString(" ")
	if n.AsOf.Expr != nil {
		ctx.FormatNode(&n.AsOf)
	}

	if n.Options != nil {
		_, _ = ctx.WriteString(" WITH ")
		ctx.FormatNode(&n.Options)
	}
}

// StatementType implements the Statement interface.
func (*CreateSnapshot) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*CreateSnapshot) StatAbbr() string { return "create_snapshot" }

// StatObj implements the StatObj interface.
func (n *CreateSnapshot) StatObj() string { return n.TableName.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*CreateSnapshot) StatementTag() string { return "CREATE SNAPSHOT" }

func (n *CreateSnapshot) String() string { return AsString(n) }

// ShowSnapshot represents a SHOW SNAPSHOT statement.
type ShowSnapshot struct {
	Name         Name
	Type         int
	TableName    TableName
	DatabaseName Name
	All          bool
}

// Format implements the NodeFormatter interface.
func (s *ShowSnapshot) Format(ctx *FmtCtx) {
	_, _ = ctx.WriteString("SHOW SNAPSHOT ")
	if s.All {
		_, _ = ctx.WriteString("ALL")
	} else if s.Name != "" {
		ctx.FormatNode(&s.Name)
	} else {
		_, _ = ctx.WriteString("FROM ")
		if s.Type == DatabaseSnapshotType {
			_, _ = ctx.WriteString("DATABASE ")
			ctx.FormatNode(&s.DatabaseName)
		} else if s.Type == TableSnapshotType {
			_, _ = ctx.WriteString("TABLE ")
			ctx.FormatNode(&s.TableName)
		}
	}
}

// StatementType implements the Statement interface.
func (*ShowSnapshot) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowSnapshot) StatAbbr() string { return "show_snapshot" }

// StatObj implements the StatObj interface.
func (s *ShowSnapshot) StatObj() string { return s.TableName.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSnapshot) StatementTag() string { return "SHOW SNAPSHOT" }

func (s *ShowSnapshot) String() string { return AsString(s) }

// DropSnapshot represents a DROP SNAPSHOT statement.
type DropSnapshot struct {
	Name Name
}

// Format implements the NodeFormatter interface.
func (d *DropSnapshot) Format(ctx *FmtCtx) {
	_, _ = ctx.WriteString("DROP SNAPSHOT ")
	ctx.FormatNode(&d.Name)
}

// StatementType implements the Statement interface.
func (*DropSnapshot) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*DropSnapshot) StatAbbr() string { return "drop_snapshot" }

// StatObj implements the StatObj interface.
func (d *DropSnapshot) StatObj() string { return d.Name.String() }

// StatementTag returns a short string identifying the type of statement.
func (*DropSnapshot) StatementTag() string { return "DROP SNAPSHOT" }

func (d *DropSnapshot) String() string { return AsString(d) }

// RevertSnapshot represents a REVERT SNAPSHOT statement.
type RevertSnapshot struct {
	Name         Name
	Type         int
	TableName    TableName
	DatabaseName Name
}

// Format implements the NodeFormatter interface.
func (node *RevertSnapshot) Format(ctx *FmtCtx) {
	_, _ = ctx.WriteString("REVERT SNAPSHOT ")
	ctx.FormatNode(&node.Name)
	_, _ = ctx.WriteString(" FROM ")
	if node.Type == DatabaseSnapshotType {
		_, _ = ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.DatabaseName)
	} else if node.Type == TableSnapshotType {
		_, _ = ctx.WriteString("TABLE ")
		ctx.FormatNode(&node.TableName)
	}
}

// StatementType implements the Statement interface.
func (*RevertSnapshot) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*RevertSnapshot) StatAbbr() string { return "revert_snapshot" }

// StatObj implements the StatObj interface.
func (node *RevertSnapshot) StatObj() string { return node.TableName.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*RevertSnapshot) StatementTag() string { return "REVERT SNAPSHOT" }

func (node *RevertSnapshot) String() string { return AsString(node) }

// Select ... AS OF SNAPSHOT
func (node *AsSnapshotClause) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *AsSnapshotClause) docRow(p *PrettyCfg) pretty.RLTableRow {
	return p.row("AS OF SNAPSHOT", pretty.Text(node.Snapshot.String()))
}
