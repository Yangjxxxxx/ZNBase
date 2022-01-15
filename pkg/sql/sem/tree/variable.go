package tree

import "github.com/znbasedb/znbase/pkg/sql/coltypes"

// DeclareVariable represents a variable use defined statement.
type DeclareVariable struct {
	VariableName Name
	VariableType coltypes.T
}

// Format implements  the NodeFormatter interface.
func (d *DeclareVariable) Format(ctx *FmtCtx) {
	ctx.WriteString("DECLARE ")
	ctx.FormatNode(&d.VariableName)
	ctx.WriteString(" " + d.VariableType.ColumnType())
}

// VariableAccess represents a variable use statement.
type VariableAccess struct {
	VariableName Name
	//VariableValue Datum
}

// Format implements the NodeFormatter interface.
func (d *VariableAccess) Format(ctx *FmtCtx) {
	ctx.WriteString("FETCH VARIABLE ")
	ctx.FormatNode(&d.VariableName)
}
