package delegate

import "github.com/znbasedb/znbase/pkg/sql/sem/tree"

func (d *delegator) delegateShowDatabases(n *tree.ShowDatabases) (tree.Statement, error) {
	queryString := `SELECT name as database_name, owner `
	if n.WithComment {
		queryString += `,obj_description(ID) AS comment `
	}
	queryString += `FROM "".zbdb_internal.databases ORDER BY 1`
	return parse(queryString)
	//return parse(
	//	`SELECT DISTINCT table_catalog AS database_name
	//   FROM "".information_schema.database_privileges
	//  ORDER BY 1`,
	//)
}
