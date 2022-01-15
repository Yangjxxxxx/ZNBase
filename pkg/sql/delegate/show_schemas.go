package delegate

import (
	"fmt"

	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// delegateShowSchemas implements SHOW SCHEMAS which returns all the schemas in
// the given or current database.
// Privileges: None.
func (d *delegator) delegateShowSchemas(n *tree.ShowSchemas) (tree.Statement, error) {
	name, err := d.getSpecifiedOrCurrentDatabase(n.Database)
	if err != nil {
		return nil, err
	}
	const getSchemasQuery = `
				SELECT schema_name, rolname AS owner
				FROM %[1]s.information_schema.schemata i
				INNER JOIN %[1]s.pg_catalog.pg_namespace n ON (n.nspname = i.schema_name)
				LEFT JOIN %[1]s.pg_catalog.pg_roles r ON (n.nspowner = r.oid)
				WHERE catalog_name = %[2]s
				ORDER BY schema_name`

	return parse(fmt.Sprintf(getSchemasQuery, name.String(), lex.EscapeSQLString(string(name))))
}

// getSpecifiedOrCurrentDatabase returns the name of the specified database, or
// of the current database if the specified name is empty.
//
// Returns an error if there is no current database, or if the specified
// database doesn't exist.
func (d *delegator) getSpecifiedOrCurrentDatabase(specifiedDB tree.Name) (tree.Name, error) {
	var name cat.SchemaName
	if specifiedDB != "" {
		// Note: the schema name may be interpreted as database name,
		// see name_resolution.go.
		name.SchemaName = specifiedDB
		name.ExplicitSchema = true
	} else {
		return tree.Name(d.catalog.GetCurrentDatabase(d.ctx)), nil
	}

	flags := cat.Flags{AvoidDescriptorCaches: true}
	_, resName, err := d.catalog.ResolveSchema(d.ctx, flags, &name)
	if err != nil {
		return "", err
	}
	return resName.CatalogName, nil
}
