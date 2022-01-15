package delegate

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// delegateShowGrants implements SHOW GRANTS which returns grant details for the
// specified objects and users.
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (d *delegator) delegateShowGrants(n *tree.ShowGrants) (tree.Statement, error) {
	var params []string

	const dbPrivQuery = `
SELECT table_catalog AS database_name,
       grantor,
       grantee,
       privilege_type,
       is_grantable AS grantAble
  FROM "".information_schema.database_privileges`
	const schemaPrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       grantor,
       grantee,
       privilege_type,
       is_grantable AS grantAble
  FROM "".information_schema.schema_privileges`
	const tablePrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       table_name AS %s_name,
       grantor,
       grantee,
       privilege_type,
       is_grantable AS grantAble
FROM "".information_schema.table_privileges`

	var source bytes.Buffer
	var cond bytes.Buffer
	var orderBy string

	if n.Targets != nil && n.Targets.Databases != nil {
		// Get grants of database from information_schema.database_privileges
		// if the type of target is database.
		dbNames := n.Targets.Databases.ToStrings()

		for _, db := range dbNames {
			name := cat.SchemaName{
				CatalogName:     tree.Name(db),
				SchemaName:      tree.Name(tree.PublicSchema),
				ExplicitCatalog: true,
				ExplicitSchema:  true,
			}
			_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &name)
			if err != nil {
				return nil, err
			}
			params = append(params, lex.EscapeSQLString(db))
		}

		fmt.Fprint(&source, dbPrivQuery)
		orderBy = "1,2,3,4,5"
		if len(params) == 0 {
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			source.WriteString(` WHERE false`)
		} else {
			fmt.Fprintf(&source, ` WHERE table_catalog IN (%s)`, strings.Join(params, ","))
		}
	} else {
		if n.Targets != nil && n.Targets.Schemas != nil {
			// Get grants of schema from information_schema.schema_privileges
			// if the type of target is schema
			scNames := n.Targets.Schemas

			for i := range scNames {
				name := cat.SchemaName{
					SchemaName:     tree.Name(scNames[i].Schema()),
					ExplicitSchema: true,
				}
				_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &name)
				if err != nil {
					return nil, err
				}
				params = append(params, lex.EscapeSQLString(scNames[i].Schema()))
			}
			fmt.Fprint(&source, schemaPrivQuery)
			orderBy = "1,2,3,4,5,6"
			if len(params) == 0 {
				// There are no rows, but we can't simply return emptyNode{} because
				// the result columns must still be defined.
				source.WriteString(` WHERE false`)
			} else {
				fmt.Fprintf(&source, ` WHERE (table_catalog, table_schema) IN (%s)`, strings.Join(params, ","))
			}
		} else {
			orderBy = "1,2,3,4,5,6,7"
			if n.Targets != nil {
				//fmt.Fprint(&source, tablePrivQuery)
				var targets tree.TablePatterns
				if n.Targets.Tables != nil {
					targets = n.Targets.Tables
					fmt.Fprintf(&source, tablePrivQuery, "table")
				} else if n.Targets.Views != nil {
					targets = n.Targets.Views
					fmt.Fprintf(&source, tablePrivQuery, "view")
				} else if n.Targets.Sequences != nil {
					targets = n.Targets.Sequences
					fmt.Fprintf(&source, tablePrivQuery, "sequence")
				}
				// Get grants of table from information_schema.table_privileges
				// if the type of target is table.
				var allTables tree.TableNames

				for _, tableTarget := range targets {
					tableGlob, err := tableTarget.NormalizeTablePattern()
					if err != nil {
						return nil, err
					}
					// We avoid the cache so that we can observe the grants taking
					// a lease, like other SHOW commands.
					tables, err := cat.ExpandDataSourceGlob(
						d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, tableGlob,
					)
					if err != nil {
						return nil, err
					}
					allTables = append(allTables, tables...)
				}

				for i := range allTables {
					params = append(params, fmt.Sprintf("(%s,%s,%s)",
						lex.EscapeSQLString(allTables[i].Catalog()),
						lex.EscapeSQLString(allTables[i].Schema()),
						lex.EscapeSQLString(allTables[i].Table())))
				}

				if len(params) == 0 {
					// The glob pattern has expanded to zero matching tables.
					// There are no rows, but we can't simply return emptyNode{} because
					// the result columns must still be defined.
					source.WriteString(` WHERE false`)
				} else {
					if n.Targets.Tables != nil {
						fmt.Fprintf(&source, ` WHERE table_type='BASE TABLE' AND (table_catalog, table_schema, table_name) IN (%s)`, strings.Join(params, ","))
					} else if n.Targets.Views != nil {
						fmt.Fprintf(&source, ` WHERE table_type in ('SYSTEM VIEW', 'VIEW') AND (table_catalog, table_schema, table_name) IN (%s)`, strings.Join(params, ","))
					} else if n.Targets.Sequences != nil {
						fmt.Fprintf(&source, ` WHERE table_type='SEQUENCE' AND (table_catalog, table_schema, table_name) IN (%s)`, strings.Join(params, ","))
					}
				}
			} else {
				fmt.Fprintf(&source, tablePrivQuery, "relation")
				// No target: only look at tables and schemasMap in the current database.
				source.WriteString(` UNION ALL ` +
					`SELECT database_name, schema_name, NULL::STRING AS table_name, grantor, grantee, privilege_type, grantAble FROM (`)
				source.WriteString(schemaPrivQuery)
				source.WriteByte(')')
				source.WriteString(` UNION ALL ` +
					`SELECT database_name, NULL::STRING AS schema_name, NULL::STRING AS table_name, grantor, grantee, privilege_type, grantAble FROM (`)
				source.WriteString(dbPrivQuery)
				source.WriteByte(')')
				// If the current database is set, restrict the command to it.
				if currDB := d.evalCtx.SessionData.Database; currDB != "" {
					fmt.Fprintf(&cond, ` WHERE database_name = %s`, lex.EscapeSQLString(currDB))
				} else {
					cond.WriteString(`WHERE true`)
				}
			}
		}
	}

	if n.Grantees != nil {
		params = []string{"'public'"}
		for _, grantee := range n.Grantees.ToStrings() {
			memberOf, err := d.resolveMemberOfWithAdminOption(d.ctx, grantee)
			if err != nil {
				return nil, err
			}
			for role := range memberOf {
				params = append(params, lex.EscapeSQLString(role))
			}

			params = append(params, lex.EscapeSQLString(grantee))
		}
		if cond.Len() > 0 {
			fmt.Fprintf(&cond, ` AND grantee IN (%s)`, strings.Join(params, ","))
		} else {
			fmt.Fprintf(&cond, ` WHERE grantee IN (%s)`, strings.Join(params, ","))
		}

	}
	return parse(
		fmt.Sprintf("SELECT * FROM (%s) %s ORDER BY %s", source.String(), cond.String(), orderBy),
	)
}

func (d *delegator) delegateShowRoleGrants(n *tree.ShowRoleGrants) (tree.Statement, error) {
	const selectQuery = `
SELECT role_name, member, is_admin
 FROM information_schema.administrable_role_authorizations`

	var query bytes.Buffer
	query.WriteString(selectQuery)

	if n.Roles != nil {
		var roles []string
		for _, r := range n.Roles.ToStrings() {
			roles = append(roles, lex.EscapeSQLString(r))
		}
		fmt.Fprintf(&query, ` WHERE "role_name" IN (%s)`, strings.Join(roles, ","))
	}

	if n.Grantees != nil {
		if n.Roles == nil {
			// No roles specified: we need a WHERE clause.
			query.WriteString(" WHERE ")
		} else {
			// We have a WHERE clause for roles.
			query.WriteString(" AND ")
		}

		var grantees []string
		for _, g := range n.Grantees.ToStrings() {
			grantees = append(grantees, lex.EscapeSQLString(g))
		}
		fmt.Fprintf(&query, ` member IN (%s)`, strings.Join(grantees, ","))

	}

	return parse(query.String())
}

func (d *delegator) delegateShowRoles() (tree.Statement, error) {
	return parse(
		`select role_name  from information_schema.applicable_roles order by role_name;`)
}

func (d *delegator) delegateShowUsers() (tree.Statement, error) {
	return parse(
		`SELECT u.username AS user_name,
			IFNULL(string_agg(o.option || COALESCE('=' || o.value, ''), ', '), '') AS options	
		FROM system.users AS u LEFT JOIN system.user_options AS o ON u.username = o.username	
	WHERE "isRole" = false GROUP BY u.username ORDER BY 1`)
}
