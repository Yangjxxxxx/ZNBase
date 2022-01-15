// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// ShowGrants returns grant details for the specified objects and users.
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (p *planner) ShowGrants(ctx context.Context, n *tree.ShowGrants) (planNode, error) {
	var params []string
	var initCheck func(context.Context) error

	const dbPrivQuery = `
SELECT table_catalog AS database_name,
       grantor,
       grantee,
       privilege_type,
       is_grantable AS grantable
  FROM "".information_schema.database_privileges`
	const schemaPrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       grantor,
       grantee,
       privilege_type,
       is_grantable AS grantable
  FROM "".information_schema.schema_privileges`
	const tablePrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       table_name AS %s_name,
       NULL::STRING AS column_name,
       grantor,
       grantee,
       privilege_type,
       is_grantable AS grantable
FROM "".information_schema.table_privileges`
	const columnPrivQuery = `
SELECT table_catalog AS database_name,
	table_schema AS schema_name,
	table_name AS table_name,
	column_name,
	grantor,
	grantee,
	privilege_type,
	is_grantable AS grantable 
FROM "".information_schema.column_privileges `

	const functionPrivQuery = `
SELECT function_catalog AS database_name,
       function_schema AS schema_name,
       function_name,
       grantor,
       grantee,
       privilege_type,
       is_grantable AS grantable
FROM "".zbdb_internal.function_privileges`

	var source bytes.Buffer
	var cond bytes.Buffer
	var orderBy string

	if n.Targets != nil && n.Targets.Databases != nil {
		// Get grants of database from information_schema.database_privileges
		// if the type of target is database.
		dbNames := n.Targets.Databases.ToStrings()

		initCheck = func(ctx context.Context) error {
			for _, db := range dbNames {
				if err := checkDBExists(ctx, p, db); err != nil {
					return err
				}
			}
			return nil
		}

		for _, db := range dbNames {
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
			initCheck = func(ctx context.Context) error {
				for _, sc := range scNames {
					if err := checkSchemaExists(ctx, p, sc); err != nil {
						return err
					}
				}
				return nil
			}

			for i := range scNames {
				if scNames[i].Catalog() == "" {
					params = append(params, fmt.Sprintf("(%s,%s)",
						lex.EscapeSQLString(p.CurrentDatabase()),
						lex.EscapeSQLString(scNames[i].Schema())))
				} else {
					params = append(params, fmt.Sprintf("(%s,%s)",
						lex.EscapeSQLString(scNames[i].Catalog()),
						lex.EscapeSQLString(scNames[i].Schema())))
				}
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
		} else if n.Targets != nil && n.Targets.Function.Parameters != nil {
			orderBy = "1,2,3,4,5,6,7"
			fmt.Fprintf(&source, functionPrivQuery)
			source.WriteString(" ")

			keyName := n.Targets.Function.FuncName.Table()
			paraStr, err := n.Targets.Function.Parameters.GetFuncParasTypeStr()
			if err != nil {
				return nil, err
			}
			keyName += paraStr

			schemaID, err := getSchemaIDByFunction(ctx, p, n.Targets.Function)
			if err != nil {
				return nil, err
			}

			ID, err := getFunctionID(ctx, p.Txn(), schemaID, keyName, true)
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(&source, ` WHERE id = %s`, lex.EscapeSQLString(strconv.Itoa(int(ID))))

			if err := checkFunctionExist(ctx, p, n.Targets.Function); err != nil {
				return nil, err
			}
		} else {
			orderBy = "1,2,3,4,5,6,7,8"
			if n.Targets != nil {
				var targets tree.TablePatterns
				var relationsType requiredType
				if n.Targets.Tables != nil {
					targets = n.Targets.Tables
					relationsType = requireTableDesc
				} else if n.Targets.Views != nil {
					targets = n.Targets.Views
					relationsType = requireViewDesc
				} else if n.Targets.Sequences != nil {
					targets = n.Targets.Sequences
					relationsType = requireSequenceDesc
				}
				// Get grants of table from information_schema.table_privileges
				// if the type of target is table.
				rPrivilege := fmt.Sprintf(tablePrivQuery, requiredTypeNames[relationsType])
				fmt.Fprint(&source, rPrivilege)
				var allTables tree.TableNames

				for _, tableTarget := range targets {
					var err error
					_, err = checkRelationType(ctx, p, len(targets), tableTarget, relationsType)
					if err != nil {
						return nil, err
					}
					tableGlob, err := tableTarget.NormalizeTablePattern()
					if err != nil {
						return nil, err
					}
					var tables tree.TableNames
					// We avoid the cache so that we can observe the grants taking
					// a lease, like other SHOW commands.
					//
					// TODO(vivek): check if the cache can be used.
					p.runWithOptions(resolveFlags{skipCache: true}, func() {
						tables, err = expandTableGlob(ctx, p.txn, p, tableGlob)
					})
					if err != nil {
						return nil, err
					}
					allTables = append(allTables, tables...)
				}

				initCheck = func(ctx context.Context) error { return nil }

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
						fmt.Fprintf(&source, `
							WHERE table_type='BASE TABLE' AND (table_catalog, table_schema, table_name) IN (%s)`, strings.Join(params, ","))
					} else if n.Targets.Views != nil {
						fmt.Fprintf(&source, `
						WHERE table_type in ('SYSTEM VIEW', 'VIEW') AND (table_catalog, table_schema, table_name) IN (%s)`, strings.Join(params, ","))
					} else if n.Targets.Sequences != nil {
						fmt.Fprintf(&source, `
						WHERE table_type='SEQUENCE' AND (table_catalog, table_schema, table_name) IN (%s)`, strings.Join(params, ","))
					}
				}
				source.WriteString(` UNION`)
				source.WriteString(columnPrivQuery)
				fmt.Fprintf(&source, ` WHERE (table_catalog, table_schema, table_name) IN (%s) 
					AND (table_catalog, table_schema, table_name, grantor, grantee, privilege_type, is_grantable) NOT IN (
					SELECT database_name, schema_name, %s_name, grantor, grantee, privilege_type, grantable FROM (%s))`, strings.Join(params, ","), requiredTypeNames[relationsType], rPrivilege)
			} else {
				fmt.Fprintf(&source, tablePrivQuery, "relation")
				source.WriteString(` UNION ALL `)
				source.WriteString(columnPrivQuery)
				fmt.Fprintf(&source, ` WHERE (table_catalog, table_schema, table_name, grantor, grantee, privilege_type, is_grantable) NOT IN (
					SELECT database_name, schema_name, relation_name, grantor, grantee, privilege_type, grantable FROM (%s))`, fmt.Sprintf(tablePrivQuery, "relation"))
				// No target: only look at tables and schemasMap in the current database.
				source.WriteString(` UNION ALL ` +
					`SELECT database_name, schema_name, NULL::STRING AS table_name, NULL::STRING AS column_name, grantor, grantee, privilege_type, grantable FROM (`)
				source.WriteString(schemaPrivQuery)
				source.WriteByte(')')
				source.WriteString(` UNION ALL ` +
					`SELECT database_name, NULL::STRING AS schema_name, NULL::STRING AS table_name, NULL::STRING AS column_name, grantor, grantee, privilege_type, grantable FROM (`)
				source.WriteString(dbPrivQuery)
				source.WriteByte(')')
				source.WriteString(` UNION ALL ` +
					`SELECT database_name, schema_name, function_name, NULL::STRING AS column_name, grantor, grantee, privilege_type, grantable FROM (`)
				source.WriteString(functionPrivQuery)
				source.WriteByte(')')
				// If the current database is set, restrict the command to it.
				if p.CurrentDatabase() != "" {
					fmt.Fprintf(&cond, ` WHERE database_name = %s`, lex.EscapeSQLString(p.CurrentDatabase()))
				} else {
					cond.WriteString(` WHERE true`)
				}
			}
		}
	}

	if n.Grantees != nil {
		params = []string{"'public'"}
		for _, grantee := range n.Grantees.ToStrings() {
			users, err := p.GetAllUsersAndRoles(ctx)
			if err != nil {
				return nil, err
			}
			if _, ok := users[grantee]; !ok {
				return nil, errors.Errorf("user or role %s does not exist", grantee)
			}
			memberOf, err := p.resolveMemberOfWithAdminOption(ctx, grantee)
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
	return p.delegateQuery(ctx, "SHOW GRANTS",
		fmt.Sprintf("SELECT * FROM (%s) %s ORDER BY %s", source.String(), cond.String(), orderBy),
		initCheck, nil)
}

func checkSchemaExists(ctx context.Context, p *planner, schema tree.SchemaName) error {
	var dbName string
	scName := schema.Schema()

	if CheckVirtualSchema(scName) {
		return nil
	}

	if schema.IsExplicitCatalog() {
		dbName = schema.Catalog()
	} else {
		dbName = p.CurrentDatabase()
	}
	dbDesc, err := p.LogicalSchemaAccessor().GetDatabaseDesc(ctx, p.txn, dbName, p.CommonLookupFlags(true /*required*/))
	if err != nil {
		return sqlbase.NewUndefinedSchemaError(scName)
	}
	if dbDesc.ExistSchema(scName) {
		return nil
	}
	return sqlbase.NewUndefinedSchemaError(scName)
}
