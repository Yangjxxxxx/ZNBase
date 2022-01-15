package optbuilder

import (
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// buildAlterTableSplit builds an ALTER TABLE/INDEX .. SPLIT AT .. statement.
func (b *Builder) buildAlterTableSplit(split *tree.Split, inScope *scope) (outScope *scope) {
	flags := cat.Flags{
		AvoidDescriptorCaches: true,
		NoTableStats:          true,
	}
	index, _, err := cat.ResolveTableIndex(b.ctx, b.catalog, flags, &split.TableOrIndex)
	if err != nil {
		panic(builderError{err})
	}
	table := index.Table()
	if err := b.catalog.CheckPrivilege(b.ctx, table, privilege.INSERT); err != nil {
		panic(err)
	}

	b.DisableMemoReuse = true

	// Calculate the desired types for the input expression. It is OK if it
	// returns fewer columns (the relevant prefix is used).
	colNames, colTypes := getIndexColumnNamesAndTypes(index)

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	inputScope := b.buildStmt(split.Rows, colTypes, emptyScope)
	checkInputColumns("SPLIT AT", inputScope, colNames, colTypes, 1)

	// Build the expiration scalar.
	var expiration opt.ScalarExpr
	expiration = b.factory.ConstructNull(types.String)

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, sqlbase.AlterTableSplitColumns)
	outScope.expr = b.factory.ConstructAlterTableSplit(
		inputScope.expr.(memo.RelExpr),
		expiration,
		&memo.AlterTableSplitPrivate{
			Table:   b.factory.Metadata().AddTable(table),
			Index:   index.GetOrdinal(),
			Columns: colsToColList(outScope.cols),
			Props:   inputScope.makePhysicalProps(),
		},
	)
	return outScope
}

// buildAlterTableRelocate builds an ALTER TABLE/INDEX .. UNSPLIT AT/ALL .. statement.
func (b *Builder) buildAlterTableRelocate(
	relocate *tree.Relocate, inScope *scope,
) (outScope *scope) {
	flags := cat.Flags{
		AvoidDescriptorCaches: true,
		NoTableStats:          true,
	}
	index, _, err := cat.ResolveTableIndex(b.ctx, b.catalog, flags, &relocate.TableOrIndex)
	if err != nil {
		panic(err)
	}
	table := index.Table()
	if err := b.catalog.CheckPrivilege(b.ctx, table, privilege.INSERT); err != nil {
		panic(err)
	}

	b.DisableMemoReuse = true

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, sqlbase.AlterTableRelocateColumns)

	// Calculate the desired types for the input expression. It is OK if it
	// returns fewer columns (the relevant prefix is used).
	colNames, colTypes := getIndexColumnNamesAndTypes(index)

	// The first column is the target leaseholder or the relocation array,
	// depending on variant.
	cmdName := "EXPERIMENTAL_RELOCATE"
	if relocate.RelocateLease {
		cmdName += " LEASE"
		colNames = append([]string{"target leaseholder"}, colNames...)
		colTypes = append([]types.T{types.Int}, colTypes...)
	} else {
		colNames = append([]string{"relocation array"}, colNames...)
		colTypes = append([]types.T{types.TArray{Typ: types.Int}}, colTypes...)
	}

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	inputScope := b.buildStmt(relocate.Rows, colTypes, b.allocScope())
	checkInputColumns(cmdName, inputScope, colNames, colTypes, 2)

	outScope.expr = b.factory.ConstructAlterTableRelocate(
		inputScope.expr.(memo.RelExpr),
		&memo.AlterTableRelocatePrivate{
			RelocateLease: relocate.RelocateLease,
			AlterTableSplitPrivate: memo.AlterTableSplitPrivate{
				Table:   b.factory.Metadata().AddTable(table),
				Index:   index.GetOrdinal(),
				Columns: colsToColList(outScope.cols),
				Props:   inputScope.makePhysicalProps(),
			},
		},
	)
	return outScope
}

// checkInputColumns verifies the types of the columns in the given scope. The
// input must have at least minPrefix columns, and their types must match that
// prefix of colTypes.
func checkInputColumns(
	context string, inputScope *scope, colNames []string, colTypes []types.T, minPrefix int,
) {
	if len(inputScope.cols) < minPrefix {
		if len(inputScope.cols) == 0 {
			panic(pgerror.NewErrorf(pgcode.Syntax, "no columns in %s data", context))
		}
		panic(pgerror.NewErrorf(pgcode.Syntax, "less than %d columns in %s data", minPrefix, context))
	}
	if len(inputScope.cols) > len(colTypes) {
		panic(pgerror.NewErrorf(pgcode.Syntax, "too many columns in %s data", context))
	}
	for i := range inputScope.cols {
		if !inputScope.cols[i].typ.Equivalent(colTypes[i]) {
			panic(pgerror.NewErrorf(
				pgcode.Syntax, "%s data column %d (%s) must be of type %s, not type %s",
				context, i+1, colNames[i], colTypes[i], inputScope.cols[i].typ,
			))
		}
	}
}

// getIndexColumnNamesAndTypes returns the names and types of the index columns.
func getIndexColumnNamesAndTypes(index cat.Index) (colNames []string, colTypes []types.T) {
	colNames = make([]string, index.LaxKeyColumnCount())
	colTypes = make([]types.T, index.LaxKeyColumnCount())
	for i := range colNames {
		c := index.Column(i)
		colNames[i] = string(c.ColName())
		colTypes[i] = c.DatumType()
	}
	return colNames, colTypes
}
