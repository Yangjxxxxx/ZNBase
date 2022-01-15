package delegate

import "github.com/znbasedb/znbase/pkg/sql/sem/tree"

// ShowTransactionStatus implements the delegator for SHOW TRANSACTION STATUS.
func (d *delegator) delegateShowSavepointStatus() (tree.Statement, error) {
	return parse(`SELECT savepoint_name, is_initial_savepoint FROM zbdb_internal.savepoint_status`)
}
