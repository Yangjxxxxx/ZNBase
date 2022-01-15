package delegate

import "github.com/znbasedb/znbase/pkg/sql/sem/tree"

func (d *delegator) delegateShowQueries(n *tree.ShowQueries) (tree.Statement, error) {
	const query = `SELECT query_id, node_id, user_name, start, query, client_address, application_name, distributed, phase FROM zbdb_internal.`
	table := `node_queries`
	if n.Cluster {
		table = `cluster_queries`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + "$ " + "internal" + "%'"
	}
	return parse(query + table + filter)
}
