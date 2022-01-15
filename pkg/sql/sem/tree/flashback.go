package tree

import "fmt"

const (
	// DatabaseFlashbackType is the database type flashback.
	DatabaseFlashbackType = 0
	// TableFlashbackType is the table type flashback.
	TableFlashbackType = 1
)

// Flashback represents the Flashback
type Flashback struct {
	Type         int
	TableName    TableName
	DatabaseName Name
}

// RevertFlashback represents a REVERT FLASHBACK statement.
type RevertFlashback struct {
	Type         int
	TableName    TableName
	DatabaseName Name
	AsOf         AsOfClause
}

// Format implements the NodeFormatter interface.
func (node *RevertFlashback) Format(ctx *FmtCtx) {
	_, _ = ctx.WriteString("FLASHBACK ")
	if node.Type == DatabaseFlashbackType {
		_, _ = ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.DatabaseName)
	} else if node.Type == TableFlashbackType {
		_, _ = ctx.WriteString("TABLE ")
		ctx.FormatNode(&node.TableName)
	}
	if node.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
	}
}

// StatementType implements the Statement interface.
func (*RevertFlashback) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*RevertFlashback) StatAbbr() string { return "flashback" }

// StatObj implements the StatObj interface.
func (node *RevertFlashback) StatObj() string { return node.TableName.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*RevertFlashback) StatementTag() string { return "FLASHBACK" }

func (node *RevertFlashback) String() string { return AsString(node) }

// RevertDropFlashback represents a REVERT DROP FLASHBACK statement.
type RevertDropFlashback struct {
	Type         int
	ObjectID     int64
	TableName    TableName
	DatabaseName Name
}

// Format implements the NodeFormatter interface.
func (node *RevertDropFlashback) Format(ctx *FmtCtx) {
	ctx.WriteString("FLASHBACK ")
	if node.Type == DatabaseFlashbackType {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.DatabaseName)
	} else if node.Type == TableFlashbackType {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(&node.TableName)
	}
	ObjectIDExpr := fmt.Sprintf(" TO BEFORE DROP WITH OBJECT_ID %d", node.ObjectID)
	ctx.WriteString(ObjectIDExpr)
}

// StatementType implements the Statement interface.
func (*RevertDropFlashback) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*RevertDropFlashback) StatAbbr() string { return "flashback drop" }

// StatObj implements the StatObj interface.
func (node *RevertDropFlashback) StatObj() string { return node.TableName.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*RevertDropFlashback) StatementTag() string { return "FLASHBACK DROP" }

func (node *RevertDropFlashback) String() string { return AsString(node) }

// ShowFlashback represents a SHOW FLASHBACK statement.
type ShowFlashback struct {
}

// Format implements the NodeFormatter interface.
func (*ShowFlashback) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW FLASHBACK ALL")
}

// StatementType implements the Statement interface.
func (*ShowFlashback) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ShowFlashback) StatAbbr() string { return "show_flashback" }

// StatObj implements the StatObj interface.
func (*ShowFlashback) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*ShowFlashback) StatementTag() string { return "SHOW FLASHBACK" }

func (s *ShowFlashback) String() string { return AsString(s) }

// AlterDatabaseFlashBack represents a REVERT FLASHBACK statement.
type AlterDatabaseFlashBack struct {
	Able         bool
	DatabaseName Name
	TTLDays      int64
}

// Format implements the NodeFormatter interface.
func (node *AlterDatabaseFlashBack) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DATABASE ")
	ctx.FormatNode(&node.DatabaseName)
	if node.Able {
		ctx.WriteString(" ENABLE FLASHBACK")
		TTLDays := fmt.Sprintf(" WITH TTLDAYS %d", node.TTLDays)
		ctx.WriteString(TTLDays)
	} else {
		ctx.WriteString(" DISABLE FLASHBACK")
	}
}

// StatementType implements the Statement interface.
func (*AlterDatabaseFlashBack) StatementType() StatementType { return RowsAffected }

// StatAbbr implements the StatAbbr interface.
func (*AlterDatabaseFlashBack) StatAbbr() string { return "alter_database" }

// StatObj implements the StatObj interface.
func (*AlterDatabaseFlashBack) StatObj() string { return "" }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseFlashBack) StatementTag() string { return "ALTER DATABASE" }

func (node *AlterDatabaseFlashBack) String() string { return AsString(node) }

// ClearIntent represents a CLEAR INTENTS statement.
type ClearIntent struct {
	Type         int
	DatabaseName Name
	TableName    TableName
}

// Format implements the NodeFormatter interface.
func (node *ClearIntent) Format(ctx *FmtCtx) {
	ctx.WriteString("CLEAR INTENT FROM ")
	if node.Type == DatabaseFlashbackType {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.DatabaseName)
	} else if node.Type == TableFlashbackType {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(&node.TableName)
	}
}

// StatementType implements the Statement interface.
func (*ClearIntent) StatementType() StatementType { return Rows }

// StatAbbr implements the StatAbbr interface.
func (*ClearIntent) StatAbbr() string { return "clear_intent" }

// StatObj implements the StatObj interface.
func (node *ClearIntent) StatObj() string { return node.TableName.Table() }

// StatementTag returns a short string identifying the type of statement.
func (*ClearIntent) StatementTag() string { return "CLEAR INTENT" }

func (node *ClearIntent) String() string { return AsString(node) }
