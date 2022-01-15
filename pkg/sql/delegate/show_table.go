package delegate

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
)

// ShowColumns of a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW COLUMNS statement.
//          mysql only returns columns you have privileges on.
func (d *delegator) delegateShowColumns(n *tree.ShowColumns) (tree.Statement, error) {
	getColumnsQuery := `
SELECT
  column_name AS column_name,
  znbase_sql_type AS data_type,
  is_nullable::BOOL,
  column_default,
  generation_expression,
  IF(inames[1] IS NULL, ARRAY[]:::STRING[], inames) AS indices,
  is_hidden::BOOL`

	if n.WithComment {
		getColumnsQuery += `,
   col_description(%[6]d, attnum) AS comment`
	}

	getColumnsQuery += `
FROM
  (SELECT column_name, znbase_sql_type, is_nullable, column_default, generation_expression, ordinal_position, is_hidden,
          array_agg(index_name) AS inames
     FROM
         (SELECT column_name, znbase_sql_type, is_nullable, column_default, generation_expression, ordinal_position, is_hidden
            FROM %[4]s.information_schema.columns
           WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s)
         LEFT OUTER JOIN
         (SELECT column_name, index_name
            FROM %[4]s.information_schema.statistics
           WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s)
         USING(column_name)
    GROUP BY column_name, znbase_sql_type, is_nullable, column_default, generation_expression, ordinal_position, is_hidden
   )`

	if n.WithComment {
		getColumnsQuery += `
          LEFT OUTER JOIN %[4]s.pg_catalog.pg_attribute 
            ON column_name = pg_attribute.attname
            AND attrelid = %[6]d`
	}

	getColumnsQuery += `
ORDER BY ordinal_position`
	return d.showTableDetails(&n.Table, getColumnsQuery, true, nil)
}

func (d *delegator) delegateShowConstraints(n *tree.ShowConstraints) (tree.Statement, error) {
	getConstraintsQuery := `
SELECT
	t.relname AS table_name,
	c.conname AS constraint_name,
	CASE c.contype
	WHEN 'p' THEN 'PRIMARY KEY'
	WHEN 'u' THEN 'UNIQUE'
	WHEN 'c' THEN 'CHECK'
	WHEN 'f' THEN 'FOREIGN KEY'
	ELSE c.contype
	END AS constraint_type,
	c.condef AS details,
	c.convalidated AS validated`

	if n.WithComment {
		getConstraintsQuery += `,
	obj_description(c.oid,%[7]s) AS comment`
	}

	getConstraintsQuery += `
FROM
	%[4]s.pg_catalog.pg_class t,
	%[4]s.pg_catalog.pg_namespace n,
	%[4]s.pg_catalog.pg_constraint c`

	getConstraintsQuery += `
WHERE t.relname = %[2]s
	AND n.nspname = %[5]s AND t.relnamespace = n.oid
	AND t.oid = c.conrelid`

	if n.WithComment {
		getConstraintsQuery += `
	ORDER BY 1, 2, constraint_name`
	} else {
		getConstraintsQuery += `
	ORDER BY 1, 2`
	}

	return d.showTableDetails(&n.Table, getConstraintsQuery, false, nil)
}

func (d *delegator) delegateShowCreate(n *tree.ShowCreate) (tree.Statement, error) {

	const showCreateQuery = `
     SELECT %[3]s AS table_name,
            create_statement
       FROM %[4]s.zbdb_internal.create_statements
      WHERE (database_name IS NULL OR database_name = %[1]s)
        AND schema_name = %[5]s
        AND descriptor_name = %[2]s`
	return d.showTableDetails(&n.Name, showCreateQuery, false, n.ShowType)
}

func (d *delegator) delegateShowIndexes(n *tree.ShowIndexes) (tree.Statement, error) {
	// const getIndexes = `
	// 			SELECT
	// 				table_name,
	// 				index_name,
	// 				non_unique::BOOL,
	// 				seq_in_index,
	// 				column_name,
	// 				direction,
	// 				storing::BOOL,
	// 				implicit::BOOL
	// 			FROM %[4]s.information_schema.statistics
	// 			WHERE table_catalog=%[1]s AND table_schema=%[5]s AND table_name=%[2]s`
	if n.Index == "" {
		getIndexesQuery := `
SELECT
	table_name,
	index_name,
	non_unique::BOOL,
	seq_in_index,
	column_name,
	direction,
	storing::BOOL,
	implicit::BOOL,
	locality,
	partitioned`

		if n.WithComment {
			getIndexesQuery += `,
	obj_description(statistics.INDEX_ID) AS comment`
		}

		getIndexesQuery += `
FROM
	%[4]s.information_schema.statistics`

		//	if n.WithComment {
		//		getIndexesQuery += `
		//LEFT JOIN pg_class ON
		//	statistics.index_name = pg_class.relname`
		//	}

		getIndexesQuery += `
WHERE
	table_catalog=%[1]s
	AND table_schema=%[5]s
	AND table_name=%[2]s`

		if n.WithComment {
			getIndexesQuery += `
	ORDER BY table_name, index_name, column_name`
		}
		return d.showTableDetails(&n.Table, getIndexesQuery, false, nil)
	}
	getIndexPartitionQuery := `
SELECT
	index_name,
	partition_name,
	subpartition_name`

	if n.WithComment {
		getIndexPartitionQuery += `,
	obj_description(index_partition.INDEX_ID) AS comment`
	}

	getIndexPartitionQuery += `
FROM
	%[4]s.information_schema.index_partition`

	//if n.WithComment {
	//	getIndexPartitionQuery += `
	//LEFT JOIN pg_class ON
	//	statistics.index_name = pg_class.relname`
	//}

	getIndexPartitionQuery += `
WHERE
	table_catalog=%[1]s
	AND table_schema=%[5]s
	AND table_name=%[2]s
	AND index_name=%[6]s`

	if n.WithComment {
		getIndexPartitionQuery += `
	ORDER BY index_name, partition_name`
	}
	return d.showTableAndIndexDetails(&n.Table, getIndexPartitionQuery, string(n.Index))
}

// showTableDetails returns the AST of a query which extracts information about
// the given table using the given query patterns in SQL. The query pattern must
// accept the following formatting parameters:
//   %[1]s the database name as SQL string literal.
//   %[2]s the unqualified table name as SQL string literal.
//   %[3]s the given table name as SQL string literal.
//   %[4]s the database name as SQL identifier.
//   %[5]s the schema name as SQL string literal.
//   %[6]s the table ID.
func (d *delegator) showTableDetails(
	name *tree.TableName, query string, isColumn bool, showType *string,
) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	// We avoid the cache so that we can observe the details without
	// taking a lease, like other SHOW commands.
	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, name)
	if err != nil {
		return nil, err
	}
	typeStr := reflect.TypeOf(dataSource).String()
	if typeStr == "*sql.optSequence" && isColumn {
		panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: %s is a sequence, can't show columns", name))
	}
	if showType != nil {
		t := reflect.TypeOf(dataSource).String()
		switch t {
		case "*sql.optSequence":
			if strings.ToLower(*showType) != "sequence" {
				panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: %s is not a %s", name, *showType))
			}
		case "*sql.optTable", "*sql.optTableXQ":
			if strings.ToLower(*showType) != "table" {
				panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: %s is not a %s", name, *showType))
			}
		case "*sql.optView":
			if strings.ToLower(*showType) != "view" {
				panic(pgerror.NewErrorf(pgcode.Syntax, "ERROR: %s is not a %s", name, *showType))
			}
		}
	}
	//if isColumn {
	//	if err := d.catalog.CheckAnyColumnPrivilege(d.ctx, dataSource); err != nil {
	//		return nil, err
	//	}
	//} else {
	//	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
	//		return nil, err
	//	}
	//}

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(resName.Catalog()),
		lex.EscapeSQLString(resName.Table()),
		lex.EscapeSQLString(resName.String()),
		resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(resName.Schema()),
		dataSource.ID(),
		sqlutil.AddExtraMark(string(resName.CatalogName), sqlutil.SingleQuotation),
	)

	return parse(fullQuery)
}

func (d *delegator) showTableAndIndexDetails(
	name *tree.TableName, query string, index string,
) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	// We avoid the cache so that we can observe the details without
	// taking a lease, like other SHOW commands.
	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, name)
	if err != nil {
		return nil, err
	}
	tb, err := d.catalog.ResolveTableDesc(name)
	if err != nil {
		return nil, err
	}
	desc, ok := tb.(*sqlbase.TableDescriptor)
	if !ok {
		return nil, errors.New("table descriptor does not exist")
	}
	indexDesc, beingDrop, err := desc.FindIndexByName(index)
	if err != nil {
		return nil, err
	}
	if beingDrop {
		return nil, fmt.Errorf(fmt.Sprintf("index %s is being dropped", indexDesc.Name))
	}
	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(resName.Catalog()),
		lex.EscapeSQLString(resName.Table()),
		lex.EscapeSQLString(resName.String()),
		resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(resName.Schema()),
		lex.EscapeSQLString(indexDesc.Name),
		dataSource.ID(),
	)

	return parse(fullQuery)
}

//func addSingleQuotation(name string) string {
//	var build strings.Builder
//	start := 0
//	for j := 0; j < len(name); j++ {
//		if name[j] == '\'' {
//			build.WriteString(name[start : j+1])
//			build.WriteString("'")
//			start = j + 1
//		}
//		if j == len(name)-1 {
//			build.WriteString(name[start : j+1])
//		}
//	}
//	name = build.String()
//	name = "'" + strings.ReplaceAll(strings.Trim(name, "\""), "\"\"", "\"") + "'"
//	return name
//}
