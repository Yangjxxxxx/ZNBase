package tree

// AlterSchema represents an ALTER SCHEMA statement.
type AlterSchema struct {
	IfExists  bool
	Schema    SchemaName
	NewSchema SchemaName
}

// Format implements the NodeFormatter interface.
func (n *AlterSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	ctx.WriteString("SCHEMA ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&n.Schema)
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&n.NewSchema)
}
