package delegate

import "github.com/znbasedb/znbase/pkg/sql/sem/tree"

func (d *delegator) delegateShowSpaces() (tree.Statement, error) {
	return parse(
		`SELECT node_id, tag FROM pg_catalog.pg_nodeStatus`)
}
