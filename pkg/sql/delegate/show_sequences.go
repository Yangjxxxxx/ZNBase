package delegate

import (
	"fmt"

	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// ShowSequences returns all the schemas in the given or current database.
// Privileges: None.
//   Notes: postgres does not have a SHOW SEQUENCES statement.
func (d *delegator) delegateShowSequences(n *tree.ShowSequences) (tree.Statement, error) {
	name, err := d.getSpecifiedOrCurrentDatabase(n.Database)
	if err != nil {
		return nil, err
	}

	getSequencesQuery := `
	  SELECT sequence_schema, sequence_name, rolname AS owner
	    FROM %[1]s.information_schema.sequences i
		INNER JOIN %[1]s.pg_catalog.pg_class pc ON (pc.relname = i.sequence_name) AND pc.relkind = 'S'
		LEFT JOIN %[1]s.pg_catalog.pg_roles pr ON (pc.relowner = pr.oid)
		JOIN %[1]s.pg_catalog.pg_namespace ns ON (ns.oid = pc.relnamespace AND ns.nspname = sequence_schema)
	   WHERE sequence_catalog = %[2]s
	   ORDER BY sequence_schema, sequence_name
	`

	return parse(fmt.Sprintf(getSequencesQuery, name.String(), lex.EscapeSQLString(string(name))))
}
