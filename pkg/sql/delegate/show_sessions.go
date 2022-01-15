package delegate

import "github.com/znbasedb/znbase/pkg/sql/sem/tree"

func (d *delegator) delegateShowSessions(n *tree.ShowSessions) (tree.Statement, error) {
	const query = `SELECT node_id, session_id, user_name, client_address, application_name, active_queries, last_active_query, session_start, oldest_query_start FROM zbdb_internal.`
	table := `node_sessions`
	if n.Cluster {
		table = `cluster_sessions`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + "$ " + "internal" + "%'"
	}
	return parse(query + table + filter)
}
