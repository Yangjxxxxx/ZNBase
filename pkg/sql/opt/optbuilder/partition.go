package optbuilder

import (
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// BuildPartitionExprs builds expressions through the syntax tree
func BuildPartitionExprs(
	tables tree.TableExprs, getTableDesc func(name tree.TableName) (*sqlbase.TableDescriptor, error),
) ([]tree.Expr, error) {
	var result []tree.Expr
	for _, table := range tables {
		if aliasedTable, ok := table.(*tree.AliasedTableExpr); ok {
			t, ok := aliasedTable.Expr.(*tree.PartitionExpr)
			if ok {
				tableDesc, err := getTableDesc(t.TableName)
				if err != nil {
					return nil, err
				}
				list, index, err := sqlbase.GetPartitionByTableDesc(tableDesc, t.PartitionName, t.IsDefault)
				if err != nil {
					return nil, err
				}
				partitionExpr := sqlbase.BuildPartitionWhere(index, list)
				if partitionExpr != nil {
					result = append(result, partitionExpr)
				}
			}
		}
	}
	return result, nil
}
