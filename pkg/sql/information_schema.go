// Copyright 2016  The Cockroach Authors.
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
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/vtable"
	"github.com/znbasedb/znbase/pkg/util"
)

const (
	informationSchemaName = "information_schema"
	pgCatalogName         = sessiondata.PgCatalogName
)

// informationSchema lists all the table definitions for
// information_schema.
var informationSchema = virtualSchema{
	name: informationSchemaName,
	allTableNames: buildStringSet(
		// Generated with:
		// select distinct '"'||table_name||'",' from information_schema.tables
		//    where table_schema='information_schema' order by table_name;
		"_pg_foreign_data_wrappers",
		"_pg_foreign_servers",
		"_pg_foreign_table_columns",
		"_pg_foreign_tables",
		"_pg_user_mappings",
		"administrable_role_authorizations",
		"applicable_roles",
		"attributes",
		"character_sets",
		"check_constraint_routine_usage",
		"check_constraints",
		"collation_character_set_applicability",
		"collations",
		"column_domain_usage",
		"column_options",
		"column_privileges",
		"column_udt_usage",
		"columns",
		"constraint_column_usage",
		"constraint_table_usage",
		"data_type_privileges",
		"domain_constraints",
		"domain_udt_usage",
		"domains",
		"element_types",
		"enabled_roles",
		"foreign_data_wrapper_options",
		"foreign_data_wrappers",
		"foreign_server_options",
		"foreign_servers",
		"foreign_table_options",
		"foreign_tables",
		"information_schema_catalog_name",
		"key_column_usage",
		"parameters",
		"referential_constraints",
		"role_column_grants",
		"role_routine_grants",
		"role_table_grants",
		"role_udt_grants",
		"role_usage_grants",
		"routine_privileges",
		"routines",
		"schemata",
		"sequences",
		"sql_features",
		"sql_implementation_info",
		"sql_languages",
		"sql_packages",
		"sql_parts",
		"sql_sizing",
		"sql_sizing_profiles",
		"table_constraints",
		"table_privileges",
		"tables",
		"transforms",
		"triggered_update_columns",
		"triggers",
		"udt_privileges",
		"usage_privileges",
		"user_defined_types",
		"user_mapping_options",
		"user_mappings",
		"view_column_usage",
		"view_routine_usage",
		"view_table_usage",
		"views",
		"user_define_functions",
		"trigger_privileges",
	),
	tableDefs: map[sqlbase.ID]virtualSchemaDef{
		sqlbase.InformationSchemaAdministrableRoleAuthorizationsID: informationSchemaAdministrableRoleAuthorizations,
		sqlbase.InformationSchemaApplicableRolesID:                 informationSchemaApplicableRoles,
		sqlbase.InformationSchemaCheckConstraints:                  informationSchemaCheckConstraints,
		sqlbase.InformationSchemaColumnPrivilegesID:                informationSchemaColumnPrivileges,
		sqlbase.InformationSchemaColumnsTableID:                    informationSchemaColumnsTable,
		sqlbase.InformationSchemaConstraintColumnUsageTableID:      informationSchemaConstraintColumnUsageTable,
		sqlbase.InformationSchemaEnabledRolesID:                    informationSchemaEnabledRoles,
		sqlbase.InformationSchemaKeyColumnUsageTableID:             informationSchemaKeyColumnUsageTable,
		sqlbase.InformationSchemaParametersTableID:                 informationSchemaParametersTable,
		sqlbase.InformationSchemaReferentialConstraintsTableID:     informationSchemaReferentialConstraintsTable,
		sqlbase.InformationSchemaRoleTableGrantsID:                 informationSchemaRoleTableGrants,
		sqlbase.InformationSchemaRoutineTableID:                    informationSchemaRoutineTable,
		sqlbase.InformationSchemaSchemataTableID:                   informationSchemaSchemataTable,
		sqlbase.InformationSchemaSchemataTablePrivilegesID:         informationSchemaSchemataTablePrivileges,
		sqlbase.InformationSchemaSchemataDatabasePrivilegesID:      informationSchemaSchemataDatabasePrivileges,
		sqlbase.InformationSchemaSequencesID:                       informationSchemaSequences,
		sqlbase.InformationSchemaStatisticsTableID:                 informationSchemaStatisticsTable,
		sqlbase.InformationSchemaTableConstraintTableID:            informationSchemaTableConstraintTable,
		sqlbase.InformationSchemaTablePrivilegesID:                 informationSchemaTablePrivileges,
		sqlbase.InformationSchemaTablesTableID:                     informationSchemaTablesTable,
		sqlbase.InformationSchemaViewsTableID:                      informationSchemaViewsTable,
		sqlbase.InformationSchemaFunctionsTableID:                  informationSchemaUserDefineFuncsTable,
		sqlbase.InformationSchemaUserPrivilegesID:                  informationSchemaUserPrivileges,
		sqlbase.InformationSchemaTriggerPrivilegesID:               informationSchemaTriggerPrivileges,
		sqlbase.InformationSchemaIndexPartitionID:                  informationSchemaIndexPartitionTable,
	},
	tableValidator:             validateInformationSchemaTable,
	validWithNoDatabaseContext: true,
}

func buildStringSet(ss ...string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

var (
	// information_schema was defined before the BOOLEAN data type was added to
	// the SQL specification. Because of this, boolean values are represented as
	// STRINGs. The BOOLEAN data type should NEVER be used in information_schema
	// tables. Instead, define columns as STRINGs and map bools to STRINGs using
	// yesOrNoDatum.
	yesString = tree.NewDString("YES")
	noString  = tree.NewDString("NO")

	globalString = tree.NewDString("global")
	localString  = tree.NewDString("local")
)

func yesOrNoDatum(b bool) tree.Datum {
	if b {
		return yesString
	}
	return noString
}

func globalOrLocalDatum(b bool) tree.Datum {
	if b {
		return localString
	}
	return globalString
}

func dNameOrNull(s string) tree.Datum {
	if s == "" {
		return tree.DNull
	}
	return tree.NewDName(s)
}

func dStringPtrOrEmpty(s *string) tree.Datum {
	if s == nil {
		return emptyString
	}
	return tree.NewDString(*s)
}

func dStringPtrOrNull(s *string) tree.Datum {
	if s == nil {
		return tree.DNull
	}
	return tree.NewDString(*s)
}

func dIntFnOrNull(fn func() (int32, bool)) tree.Datum {
	if n, ok := fn(); ok {
		return tree.NewDInt(tree.DInt(n))
	}
	return tree.DNull
}

func validateInformationSchemaTable(table *sqlbase.TableDescriptor) error {
	// Make sure no tables have boolean columns.
	for _, col := range table.Columns {
		if col.Type.SemanticType == sqlbase.ColumnType_BOOL {
			return errors.Errorf("information_schema tables should never use BOOL columns. "+
				"See the comment about yesOrNoDatum. Found BOOL column in %s.", table.Name)
		}
	}
	return nil
}

var informationSchemaAdministrableRoleAuthorizations = virtualSchemaTable{
	comment: `roles for which the current user has admin option
` + base.DocsURL("information-schema.html#administrable_role_authorizations") + `
https://www.postgresql.org/docs/9.5/infoschema-administrable-role-authorizations.html`,
	schema: vtable.InformationSchemaAdministrableRoleAuthorizations,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		query := `SELECT "role", "member", "isAdmin" FROM system.role_members`
		rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
			ctx, "read-members", p.txn, query,
		)
		if err != nil {
			return err
		}

		for _, row := range rows {
			roleName := tree.MustBeDString(row[0])
			memberName := tree.MustBeDString(row[1])
			isAdmin := yesString
			if row[2].(*tree.DBool) == tree.DBoolFalse {
				isAdmin = noString
			}
			if err := addRow(
				&roleName,
				&memberName,
				isAdmin,
			); err != nil {
				return err
			}
		}

		return nil
	},
}

var informationSchemaApplicableRoles = virtualSchemaTable{
	comment: `roles available to the current user
` + base.DocsURL("information-schema.html#applicable_roles") + `
https://www.postgresql.org/docs/9.5/infoschema-applicable-roles.html`,
	schema: vtable.InformationSchemaApplicableRoles,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		currentUser := p.SessionData().User
		memberMap, err := p.MemberOfWithAdminOption(ctx, currentUser)
		if err != nil {
			return err
		}

		grantee := tree.NewDString(currentUser)

		for roleName, isAdmin := range memberMap {
			if err := addRow(
				grantee,                   // grantee: always the current user
				tree.NewDString(roleName), // role_name
				yesOrNoDatum(isAdmin),     // is_grantable
			); err != nil {
				return err
			}
		}

		return nil
	},
}

var informationSchemaCheckConstraints = virtualSchemaTable{
	comment: `check constraints
` + base.DocsURL("information-schema.html#check_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-check-constraints.html`,
	schema: vtable.InformationSchemaCheckConstraints,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /* no constraints in virtual tables */, func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
			if err != nil {
				return err
			}
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			for conName, con := range conInfo {
				// Only Check constraints are included.
				if con.Kind != sqlbase.ConstraintTypeCheck {
					continue
				}
				conNameStr := tree.NewDString(conName)
				// Like with pg_catalog.pg_constraint, Postgres wraps the check
				// constraint expression in two pairs of parentheses.
				chkExprStr := tree.NewDString(fmt.Sprintf("((%s))", con.Details))
				if err := addRow(
					dbNameStr,  // constraint_catalog
					scNameStr,  // constraint_schema
					conNameStr, // constraint_name
					chkExprStr, // check_clause
				); err != nil {
					return err
				}
			}

			// Unlike with pg_catalog.pg_constraint, Postgres also includes NOT
			// NULL column constraints in information_schema.check_constraints.
			// Cockroach doesn't track these constraints as check constraints,
			// but we can pull them off of the table's column descriptors.
			colNum := 0
			return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
				colNum++
				// Only visible, non-nullable columns are included.
				if column.Hidden || column.Nullable {
					return nil
				}
				// Generate a unique name for each NOT NULL constraint. Postgres
				// uses the format <namespace_oid>_<table_oid>_<col_idx>_not_null.
				// We might as well do the same.
				conNameStr := tree.NewDString(fmt.Sprintf(
					"%s_%s_%d_not_null", h.NamespaceOid(db, scName), defaultOid(table.ID), colNum,
				))
				chkExprStr := tree.NewDString(fmt.Sprintf(
					"%s IS NOT NULL", column.Name,
				))
				return addRow(
					dbNameStr,  // constraint_catalog
					scNameStr,  // constraint_schema
					conNameStr, // constraint_name
					chkExprStr, // check_clause
				)
			})
		})
	},
}

var informationSchemaColumnPrivileges = virtualSchemaTable{
	comment: `column privilege grants
` + base.DocsURL("information-schema.html#column_privileges") + `
https://www.postgresql.org/docs/9.5/infoschema-column-privileges.html`,
	schema: vtable.InformationSchemaColumnPrivileges,
	populate: func(ctx context.Context, planner *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, planner, dbContext, virtualMany, func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
			user := planner.User()
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			tbNameStr := tree.NewDString(table.Name)
			for _, cs := range table.Columns {
				colNameStr := tree.NewDString(cs.Name)
				if cs.Privileges != nil { // sequence does not have column privileges
					for _, u := range cs.Privileges.Show() {
						for _, priv := range u.Privileges {
							// general user couldn't show grants on object without any privileges.
							hasAdminRole, err := planner.HasAdminRole(ctx)
							if err != nil {
								return err
							}
							if !hasAdminRole {
								// if user is not root or in Admin Role
								hasPriv, err := checkRowPriv(ctx, planner, priv.Grantor, u.User, user)
								if err != nil {
									return err
								}
								if !hasPriv {
									continue
								}
							}

							if err := addRow(
								tree.NewDString(priv.Grantor), // grantor
								tree.NewDString(u.User),       // grantee
								dbNameStr,                     // table_catalog
								scNameStr,                     // table_schema
								tbNameStr,                     // table_name
								colNameStr,                    // column_name
								tree.NewDString(priv.Type),    // privilege_type
								yesOrNoDatum(priv.GrantAble),  // is_grantable
							); err != nil {
								return err
							}
						}
					}
				}

			}
			return nil
		})
	},
}

var informationSchemaColumnsTable = virtualSchemaTable{
	comment: `table and view columns (incomplete)
` + base.DocsURL("information-schema.html#columns") + `
https://www.postgresql.org/docs/9.5/infoschema-columns.html`,
	schema: vtable.InformationSchemaColumns,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, virtualMany, func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			// Table descriptors already holds columns in-order.
			visible := 0
			return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
				visible++
				return addRow(
					dbNameStr,                            // table_catalog
					scNameStr,                            // table_schema
					tree.NewDString(table.Name),          // table_name
					tree.NewDString(column.Name),         // column_name
					tree.NewDInt(tree.DInt(visible)),     // ordinal_position, 1-indexed
					dStringPtrOrNull(column.DefaultExpr), // column_default
					yesOrNoDatum(column.Nullable),        // is_nullable
					tree.NewDString(column.Type.InformationSchemaVisibleType()), // data_type
					characterMaximumLength(column.Type),                         // character_maximum_length
					characterOctetLength(column.Type),                           // character_octet_length
					numericPrecision(column.Type),                               // numeric_precision
					numericPrecisionRadix(column.Type),                          // numeric_precision_radix
					numericScale(column.Type),                                   // numeric_scale
					datetimePrecision(column.Type),                              // datetime_precision
					tree.DNull,                                                  // character_set_catalog
					tree.DNull,                                                  // character_set_schema
					tree.DNull,                                                  // character_set_name
					tree.DNull,                                                  // domain_catalog
					tree.DNull,                                                  // domain_schema
					tree.DNull,                                                  // domain_name
					dStringPtrOrEmpty(column.ComputeExpr),                       // generation_expression
					yesOrNoDatum(column.Hidden),                                 // is_hidden
					tree.NewDString(column.Type.SQLString()),                    // znbase_sql_type
				)
			})
		})
	},
}

var informationSchemaEnabledRoles = virtualSchemaTable{
	comment: `roles for the current user
` + base.DocsURL("information-schema.html#enabled_roles") + `
https://www.postgresql.org/docs/9.5/infoschema-enabled-roles.html`,
	schema: `
CREATE TABLE information_schema.enabled_roles (
	ROLE_NAME STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasAdminRole, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}
		if hasAdminRole {
			stmt := `SELECT * FROM system.users`
			rows, err := p.ExecCfg().InternalExecutor.Query(
				ctx, "expand-roles", nil /* txn */, stmt)
			if err != nil {
				return nil
			}
			for _, row := range rows {
				roleName := tree.MustBeDString(row[0])
				if err := addRow(
					&roleName, // role_name
				); err != nil {
					return err
				}
			}
			return nil
		}
		currentUser := p.SessionData().User
		memberMap, err := p.MemberOfWithAdminOption(ctx, currentUser)
		if err != nil {
			return err
		}

		// The current user is always listed.
		if err := addRow(
			tree.NewDString(currentUser), // role_name: the current user
		); err != nil {
			return err
		}

		for roleName := range memberMap {
			if err := addRow(
				tree.NewDString(roleName), // role_name
			); err != nil {
				return err
			}
		}

		return nil
	},
}

func characterMaximumLength(colType sqlbase.ColumnType) tree.Datum {
	return dIntFnOrNull(colType.MaxCharacterLength)
}

func characterOctetLength(colType sqlbase.ColumnType) tree.Datum {
	return dIntFnOrNull(colType.MaxOctetLength)
}

func numericPrecision(colType sqlbase.ColumnType) tree.Datum {
	return dIntFnOrNull(colType.NumericPrecision)
}

func numericPrecisionRadix(colType sqlbase.ColumnType) tree.Datum {
	return dIntFnOrNull(colType.NumericPrecisionRadix)
}

func numericScale(colType sqlbase.ColumnType) tree.Datum {
	return dIntFnOrNull(colType.NumericScale)
}

func datetimePrecision(colType sqlbase.ColumnType) tree.Datum {
	// We currently do not support a datetime precision.
	return tree.DNull
}

var informationSchemaConstraintColumnUsageTable = virtualSchemaTable{
	comment: `columns usage by constraints
https://www.postgresql.org/docs/9.5/infoschema-constraint-column-usage.html`,
	schema: `
CREATE TABLE information_schema.constraint_column_usage (
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	COLUMN_NAME        STRING NOT NULL,
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /* no constraints in virtual tables */, func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
			if err != nil {
				return err
			}
			scNameStr := tree.NewDString(scName)
			dbNameStr := tree.NewDString(db.Name)

			for conName, con := range conInfo {
				conTable := table
				conCols := con.Columns
				conNameStr := tree.NewDString(conName)
				if con.Kind == sqlbase.ConstraintTypeFK {
					// For foreign key constraint, constraint_column_usage
					// identifies the table/columns that the foreign key
					// references.
					conTable = con.ReferencedTable
					conCols = con.ReferencedIndex.ColumnNames
				}
				tableNameStr := tree.NewDString(conTable.Name)
				for _, col := range conCols {
					if err := addRow(
						dbNameStr,            // table_catalog
						scNameStr,            // table_schema
						tableNameStr,         // table_name
						tree.NewDString(col), // column_name
						dbNameStr,            // constraint_catalog
						scNameStr,            // constraint_schema
						conNameStr,           // constraint_name
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/key-column-usage-table.html
var informationSchemaKeyColumnUsageTable = virtualSchemaTable{
	comment: `column usage by indexes and key constraints
` + base.DocsURL("information-schema.html#key_column_usage") + `
https://www.postgresql.org/docs/9.5/infoschema-key-column-usage.html`,
	schema: `
CREATE TABLE information_schema.key_column_usage (
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL,
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	COLUMN_NAME        STRING NOT NULL,
	ORDINAL_POSITION   INT NOT NULL,
	POSITION_IN_UNIQUE_CONSTRAINT INT
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /* no constraints in virtual tables */, func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
			if err != nil {
				return err
			}
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			tbNameStr := tree.NewDString(table.Name)
			for conName, con := range conInfo {
				// Only Primary Key, Foreign Key, and Unique constraints are included.
				switch con.Kind {
				case sqlbase.ConstraintTypePK:
				case sqlbase.ConstraintTypeFK:
				case sqlbase.ConstraintTypeUnique:
				default:
					continue
				}

				cstNameStr := tree.NewDString(conName)

				for pos, col := range con.Columns {
					ordinalPos := tree.NewDInt(tree.DInt(pos + 1))
					uniquePos := tree.DNull
					if con.Kind == sqlbase.ConstraintTypeFK {
						uniquePos = ordinalPos
					}
					if err := addRow(
						dbNameStr,            // constraint_catalog
						scNameStr,            // constraint_schema
						cstNameStr,           // constraint_name
						dbNameStr,            // table_catalog
						scNameStr,            // table_schema
						tbNameStr,            // table_name
						tree.NewDString(col), // column_name
						ordinalPos,           // ordinal_position, 1-indexed
						uniquePos,            // position_in_unique_constraint
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-parameters.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/parameters-table.html
var informationSchemaParametersTable = virtualSchemaTable{
	comment: `built-in function parameters (empty - introspection not yet supported)
https://www.postgresql.org/docs/9.5/infoschema-parameters.html`,
	schema: `
CREATE TABLE information_schema.parameters (
	SPECIFIC_CATALOG STRING,
	SPECIFIC_SCHEMA STRING,
	SPECIFIC_NAME STRING,
	ORDINAL_POSITION INT,
	PARAMETER_MODE STRING,
	IS_RESULT STRING,
	AS_LOCATOR STRING,
	PARAMETER_NAME STRING,
	DATA_TYPE STRING,
	CHARACTER_MAXIMUM_LENGTH INT,
	CHARACTER_OCTET_LENGTH INT,
	CHARACTER_SET_CATALOG STRING,
	CHARACTER_SET_SCHEMA STRING,
	CHARACTER_SET_NAME STRING,
	COLLATION_CATALOG STRING,
	COLLATION_SCHEMA STRING,
	COLLATION_NAME STRING,
	NUMERIC_PRECISION INT,
	NUMERIC_PRECISION_RADIX INT,
	NUMERIC_SCALE INT,
	DATETIME_PRECISION INT,
	INTERVAL_TYPE STRING,
	INTERVAL_PRECISION INT,
	UDT_CATALOG STRING,
	UDT_SCHEMA STRING,
	UDT_NAME STRING,
	SCOPE_CATALOG STRING,
	SCOPE_SCHEMA STRING,
	SCOPE_NAME STRING,
	MAXIMUM_CARDINALITY INT,
	DTD_IDENTIFIER STRING,
	PARAMETER_DEFAULT STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

var (
	matchOptionFull    = tree.NewDString("FULL")
	matchOptionPartial = tree.NewDString("PARTIAL")
	matchOptionNone    = tree.NewDString("NONE")

	matchOptionMap = map[sqlbase.ForeignKeyReference_Match]tree.Datum{
		sqlbase.ForeignKeyReference_SIMPLE:  matchOptionNone,
		sqlbase.ForeignKeyReference_FULL:    matchOptionFull,
		sqlbase.ForeignKeyReference_PARTIAL: matchOptionPartial,
	}

	refConstraintRuleNoAction   = tree.NewDString("NO ACTION")
	refConstraintRuleRestrict   = tree.NewDString("RESTRICT")
	refConstraintRuleSetNull    = tree.NewDString("SET NULL")
	refConstraintRuleSetDefault = tree.NewDString("SET DEFAULT")
	refConstraintRuleCascade    = tree.NewDString("CASCADE")
)

func dStringForFKAction(action sqlbase.ForeignKeyReference_Action) tree.Datum {
	switch action {
	case sqlbase.ForeignKeyReference_NO_ACTION:
		return refConstraintRuleNoAction
	case sqlbase.ForeignKeyReference_RESTRICT:
		return refConstraintRuleRestrict
	case sqlbase.ForeignKeyReference_SET_NULL:
		return refConstraintRuleSetNull
	case sqlbase.ForeignKeyReference_SET_DEFAULT:
		return refConstraintRuleSetDefault
	case sqlbase.ForeignKeyReference_CASCADE:
		return refConstraintRuleCascade
	}
	panic(errors.Errorf("unexpected ForeignKeyReference_Action: %v", action))
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/referential-constraints-table.html
var informationSchemaReferentialConstraintsTable = virtualSchemaTable{
	comment: `foreign key constraints
` + base.DocsURL("information-schema.html#referential_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-referential-constraints.html`,
	schema: `
CREATE TABLE information_schema.referential_constraints (
	CONSTRAINT_CATALOG        STRING NOT NULL,
	CONSTRAINT_SCHEMA         STRING NOT NULL,
	CONSTRAINT_NAME           STRING NOT NULL,
	UNIQUE_CONSTRAINT_CATALOG STRING NOT NULL,
	UNIQUE_CONSTRAINT_SCHEMA  STRING NOT NULL,
	UNIQUE_CONSTRAINT_NAME    STRING,
	MATCH_OPTION              STRING NOT NULL,
	UPDATE_RULE               STRING NOT NULL,
	DELETE_RULE               STRING NOT NULL,
	TABLE_NAME                STRING NOT NULL,
	REFERENCED_TABLE_NAME     STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /* no constraints in virtual tables */, func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			tbNameStr := tree.NewDString(table.Name)
			return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
				fk := index.ForeignKey
				if !fk.IsSet() {
					return nil
				}

				refTable, err := tableLookup.getTableByID(fk.Table)
				if err != nil {
					return err
				}
				refIndex, err := refTable.FindIndexByID(fk.Index)
				if err != nil {
					return err
				}
				var matchType tree.Datum = tree.DNull
				if r, ok := matchOptionMap[fk.Match]; ok {
					matchType = r
				}
				return addRow(
					dbNameStr,                       // constraint_catalog
					scNameStr,                       // constraint_schema
					tree.NewDString(fk.Name),        // constraint_name
					dbNameStr,                       // unique_constraint_catalog
					scNameStr,                       // unique_constraint_schema
					tree.NewDString(refIndex.Name),  // unique_constraint_name
					matchType,                       // match_option
					dStringForFKAction(fk.OnUpdate), // update_rule
					dStringForFKAction(fk.OnDelete), // delete_rule
					tbNameStr,                       // table_name
					tree.NewDString(refTable.Name),  // referenced_table_name
				)
			})
		})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-role-table-grants.html
// MySQL:    missing
var informationSchemaRoleTableGrants = virtualSchemaTable{
	comment: `privileges granted on table or views (incomplete; see also information_schema.table_privileges; may contain excess users or roles)
` + base.DocsURL("information-schema.html#role_table_grants") + `
https://www.postgresql.org/docs/9.5/infoschema-role-table-grants.html`,
	schema: `
CREATE TABLE information_schema.role_table_grants (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
	TABLE_TYPE     STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING,
	WITH_HIERARCHY STRING
)`,
	// This is the same as information_schema.table_privileges. In postgres, this virtual table does
	// not show tables with grants provided through PUBLIC, but table_privileges does.
	// Since we don't have the PUBLIC concept, the two virtual tables are identical.
	populate: populateRoleTableGrants,
}

// MySQL:    https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/routines-table.html
var informationSchemaRoutineTable = virtualSchemaTable{
	comment: `built-in functions (empty - introspection not yet supported)
https://www.postgresql.org/docs/9.5/infoschema-routines.html`,
	schema: `
CREATE TABLE information_schema.routines (
	SPECIFIC_CATALOG STRING,
	SPECIFIC_SCHEMA STRING,
	SPECIFIC_NAME STRING,
	ROUTINE_CATALOG STRING,
	ROUTINE_SCHEMA STRING,
	ROUTINE_NAME STRING,
	ROUTINE_TYPE STRING,
	MODULE_CATALOG STRING,
	MODULE_SCHEMA STRING,
	MODULE_NAME STRING,
	UDT_CATALOG STRING,
	UDT_SCHEMA STRING,
	UDT_NAME STRING,
	DATA_TYPE STRING,
	CHARACTER_MAXIMUM_LENGTH INT,
	CHARACTER_OCTET_LENGTH INT,
	CHARACTER_SET_CATALOG STRING,
	CHARACTER_SET_SCHEMA STRING,
	CHARACTER_SET_NAME STRING,
	COLLATION_CATALOG STRING,
	COLLATION_SCHEMA STRING,
	COLLATION_NAME STRING,
	NUMERIC_PRECISION INT,
	NUMERIC_PRECISION_RADIX INT,
	NUMERIC_SCALE INT,
	DATETIME_PRECISION INT,
	INTERVAL_TYPE STRING,
	INTERVAL_PRECISION STRING,
	TYPE_UDT_CATALOG STRING,
	TYPE_UDT_SCHEMA STRING,
	TYPE_UDT_NAME STRING,
	SCOPE_CATALOG STRING,
	SCOPE_NAME STRING,
	MAXIMUM_CARDINALITY INT,
	DTD_IDENTIFIER STRING,
	ROUTINE_BODY STRING,
	ROUTINE_DEFINITION STRING,
	EXTERNAL_NAME STRING,
	EXTERNAL_LANGUAGE STRING,
	PARAMETER_STYLE STRING,
	IS_DETERMINISTIC STRING,
	SQL_DATA_ACCESS STRING,
	IS_NULL_CALL STRING,
	SQL_PATH STRING,
	SCHEMA_LEVEL_ROUTINE STRING,
	MAX_DYNAMIC_RESULT_SETS INT,
	IS_USER_DEFINED_CAST STRING,
	IS_IMPLICITLY_INVOCABLE STRING,
	SECURITY_TYPE STRING,
	TO_SQL_SPECIFIC_CATALOG STRING,
	TO_SQL_SPECIFIC_SCHEMA STRING,
	TO_SQL_SPECIFIC_NAME STRING,
	AS_LOCATOR STRING,
	CREATED  TIMESTAMPTZ,
	LAST_ALTERED TIMESTAMPTZ,
	NEW_SAVEPOINT_LEVEL  STRING,
	IS_UDT_DEPENDENT STRING,
	RESULT_CAST_FROM_DATA_TYPE STRING,
	RESULT_CAST_AS_LOCATOR STRING,
	RESULT_CAST_CHAR_MAX_LENGTH  INT,
	RESULT_CAST_CHAR_OCTET_LENGTH STRING,
	RESULT_CAST_CHAR_SET_CATALOG STRING,
	RESULT_CAST_CHAR_SET_SCHEMA  STRING,
	RESULT_CAST_CHAR_SET_NAME STRING,
	RESULT_CAST_COLLATION_CATALOG STRING,
	RESULT_CAST_COLLATION_SCHEMA STRING,
	RESULT_CAST_COLLATION_NAME STRING,
	RESULT_CAST_NUMERIC_PRECISION INT,
	RESULT_CAST_NUMERIC_PRECISION_RADIX INT,
	RESULT_CAST_NUMERIC_SCALE INT,
	RESULT_CAST_DATETIME_PRECISION STRING,
	RESULT_CAST_INTERVAL_TYPE STRING,
	RESULT_CAST_INTERVAL_PRECISION INT,
	RESULT_CAST_TYPE_UDT_CATALOG STRING,
	RESULT_CAST_TYPE_UDT_SCHEMA  STRING,
	RESULT_CAST_TYPE_UDT_NAME STRING,
	RESULT_CAST_SCOPE_CATALOG STRING,
	RESULT_CAST_SCOPE_SCHEMA STRING,
	RESULT_CAST_SCOPE_NAME STRING,
	RESULT_CAST_MAXIMUM_CARDINALITY INT,
	RESULT_CAST_DTD_IDENTIFIER STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/schemata-table.html
var informationSchemaSchemataTable = virtualSchemaTable{
	comment: `database schemasMap (may contain schemata without permission)
` + base.DocsURL("information-schema.html#schemata") + `
https://www.postgresql.org/docs/9.5/infoschema-schemata.html`,
	schema: vtable.InformationSchemaSchemata,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, false, func(db *sqlbase.DatabaseDescriptor) error {
			return forEachSchemaName(ctx, p, db, true, func(sc *sqlbase.SchemaDescriptor) error {
				return addRow(
					tree.NewDString(db.Name), // catalog_name
					tree.NewDString(sc.Name), // schema_name
					tree.DNull,               // default_character_set_name
					tree.DNull,               // sql_path
				)
			})
		})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/schema-privileges-table.html
var informationSchemaSchemataTablePrivileges = virtualSchemaTable{
	comment: `schema privileges (incomplete; may contain excess users or roles)
` + base.DocsURL("information-schema.html#schema_privileges"),
	schema: `
CREATE TABLE information_schema.schema_privileges (
	GRANTOR         STRING NOT NULL,
	GRANTEE         STRING NOT NULL,
	TABLE_CATALOG   STRING NOT NULL,
	TABLE_SCHEMA    STRING NOT NULL,
	PRIVILEGE_TYPE  STRING NOT NULL,
	IS_GRANTABLE    STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, false, func(db *sqlbase.DatabaseDescriptor) error {
			return forEachSchemaName(ctx, p, db, true, func(sc *sqlbase.SchemaDescriptor) error {
				privs := sc.Privileges.Show()
				dbNameStr := tree.NewDString(db.Name)
				scNameStr := tree.NewDString(sc.Name)
				// TODO(knz): This should filter for the current user, see
				// https://github.com/znbasedb/znbase/issues/35572
				for _, u := range privs {
					userNameStr := tree.NewDString(u.User)
					for _, priv := range u.Privileges {
						// general user couldn't show grants on object without any privileges.
						hasAdminRole, err := p.HasAdminRole(ctx)
						if err != nil {
							return err
						}
						if !hasAdminRole {
							// if user is not root or in Admin Role
							hasPriv, err := checkRowPriv(ctx, p, priv.Grantor, u.User, p.User())
							if err != nil {
								return err
							}
							if !hasPriv {
								continue
							}
						}

						if err := addRow(
							tree.NewDString(priv.Grantor), // grantor
							userNameStr,                   // grantee
							dbNameStr,                     // table_catalog
							scNameStr,                     // table_schema
							tree.NewDString(priv.Type),    // privilege_type
							yesOrNoDatum(priv.GrantAble),  // is_grantable
						); err != nil {
							return err
						}
					}
				}
				return nil
			})
		})
	},
}

var informationSchemaSchemataDatabasePrivileges = virtualSchemaTable{
	comment: `schema privileges (incomplete; may contain excess users or roles)
` + base.DocsURL("information-schema.html#database_privileges"),
	schema: `
CREATE TABLE information_schema.database_privileges (
	GRANTOR			STRING NOT NULL,
	GRANTEE         STRING NOT NULL,
	TABLE_CATALOG   STRING NOT NULL,
	PRIVILEGE_TYPE  STRING NOT NULL,
	IS_GRANTABLE    STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, func(db *sqlbase.DatabaseDescriptor) error {
			// show database privilege
			privs := db.Privileges.Show()
			dbNameStr := tree.NewDString(db.Name)
			for _, u := range privs {
				userNameStr := tree.NewDString(u.User)
				for _, priv := range u.Privileges {
					// general user couldn't show grants on object without any privileges.
					hasAdminRole, err := p.HasAdminRole(ctx)
					if err != nil {
						return err
					}
					if !hasAdminRole {
						// if user is not root or in Admin Role
						hasPriv, err := checkRowPriv(ctx, p, priv.Grantor, u.User, p.User())
						if err != nil {
							return err
						}
						if !hasPriv {
							continue
						}
					}
					if err := addRow(
						tree.NewDString(priv.Grantor), // grantor
						userNameStr,                   // grantee
						dbNameStr,                     // table_catalog
						tree.NewDString(priv.Type),    // privilege_type
						yesOrNoDatum(priv.GrantAble),  // is_grantable
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

var (
	indexDirectionNA   = tree.NewDString("N/A")
	indexDirectionAsc  = tree.NewDString(sqlbase.IndexDescriptor_ASC.String())
	indexDirectionDesc = tree.NewDString(sqlbase.IndexDescriptor_DESC.String())
)

func dStringForIndexDirection(dir sqlbase.IndexDescriptor_Direction) tree.Datum {
	switch dir {
	case sqlbase.IndexDescriptor_ASC:
		return indexDirectionAsc
	case sqlbase.IndexDescriptor_DESC:
		return indexDirectionDesc
	}
	panic("unreachable")
}

var informationSchemaSequences = virtualSchemaTable{
	comment: `sequences
` + base.DocsURL("information-schema.html#sequences") + `
https://www.postgresql.org/docs/9.5/infoschema-sequences.html`,
	schema: `
CREATE TABLE information_schema.sequences (
    SEQUENCE_CATALOG         STRING NOT NULL,
    SEQUENCE_SCHEMA          STRING NOT NULL,
    SEQUENCE_NAME            STRING NOT NULL,
    DATA_TYPE                STRING NOT NULL,
    NUMERIC_PRECISION        INT NOT NULL,
    NUMERIC_PRECISION_RADIX  INT NOT NULL,
    NUMERIC_SCALE            INT NOT NULL,
    START_VALUE              STRING NOT NULL,
    MINIMUM_VALUE            STRING NOT NULL,
    MAXIMUM_VALUE            STRING NOT NULL,
    INCREMENT                STRING NOT NULL,
    CYCLE_OPTION             STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* no sequences in virtual schemasMap */
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				if !table.IsSequence() {
					return nil
				}
				return addRow(
					tree.NewDString(db.GetName()),    // catalog
					tree.NewDString(scName),          // schema
					tree.NewDString(table.GetName()), // name
					tree.NewDString("bigint"),        // type
					tree.NewDInt(64),                 // numeric precision
					tree.NewDInt(2),                  // numeric precision radix
					tree.NewDInt(0),                  // numeric scale                                                   // numeric scale
					tree.NewDString(strconv.FormatInt(table.SequenceOpts.Start, 10)),     // start value
					tree.NewDString(strconv.FormatInt(table.SequenceOpts.MinValue, 10)),  // min value
					tree.NewDString(strconv.FormatInt(table.SequenceOpts.MaxValue, 10)),  // max value
					tree.NewDString(strconv.FormatInt(table.SequenceOpts.Increment, 10)), // increment
					noString, // cycle
				)
			})
	},
}

// Postgres: missing
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/statistics-table.html
var informationSchemaStatisticsTable = virtualSchemaTable{
	comment: `index metadata and statistics (incomplete)
` + base.DocsURL("information-schema.html#statistics"),
	schema: `
CREATE TABLE information_schema.statistics (
	TABLE_CATALOG STRING NOT NULL,
	TABLE_SCHEMA  STRING NOT NULL,
	TABLE_NAME    STRING NOT NULL,
	INDEX_ID 	  OID,
	NON_UNIQUE    STRING NOT NULL,
	INDEX_SCHEMA  STRING NOT NULL,
	INDEX_NAME    STRING NOT NULL,
	SEQ_IN_INDEX  INT NOT NULL,
	COLUMN_NAME   STRING NOT NULL,
	"COLLATION"   STRING,
	CARDINALITY   INT,
	DIRECTION     STRING NOT NULL,
	STORING       STRING NOT NULL,
	IMPLICIT      STRING NOT NULL,
	LOCALITY	  STRING NOT NULL,
	PARTITIONED   STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual tables have no indexes */
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				h := makeOidHasher()
				dbNameStr := tree.NewDString(db.GetName())
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.GetName())

				appendRow := func(index *sqlbase.IndexDescriptor, colName string, sequence int,
					direction tree.Datum, isStored, isImplicit bool,
				) error {
					return addRow(
						dbNameStr,                           // table_catalog
						scNameStr,                           // table_schema
						tbNameStr,                           // table_name
						h.IndexOid(table.ID, index.ID),      // index_oid
						yesOrNoDatum(!index.Unique),         // non_unique
						scNameStr,                           // index_schema
						tree.NewDString(index.Name),         // index_name
						tree.NewDInt(tree.DInt(sequence)),   // seq_in_index
						tree.NewDString(colName),            // column_name
						tree.DNull,                          // collation
						tree.DNull,                          // cardinality
						direction,                           // direction
						yesOrNoDatum(isStored),              // storing
						yesOrNoDatum(isImplicit),            // implicit
						globalOrLocalDatum(index.IsLocal),   // locality
						yesOrNoDatum(index.IsPartitioned()), // partitioned
					)
				}

				return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
					// Columns in the primary key that aren't in index.ColumnNames or
					// index.StoreColumnNames are implicit columns in the index.
					var implicitCols map[string]struct{}
					var hasImplicitCols bool
					if index.HasOldStoredColumns() {
						// Old STORING format: implicit columns are extra columns minus stored
						// columns.
						hasImplicitCols = len(index.ExtraColumnIDs) > len(index.StoreColumnNames)
					} else {
						// New STORING format: implicit columns are extra columns.
						hasImplicitCols = len(index.ExtraColumnIDs) > 0
					}
					if hasImplicitCols {
						implicitCols = make(map[string]struct{})
						for _, col := range table.PrimaryIndex.ColumnNames {
							implicitCols[col] = struct{}{}
						}
					}

					sequence := 1
					for i, col := range index.ColumnNames {
						// We add a row for each column of index.
						if index.IsRealFunc != nil {
							if !index.IsRealFunc[i] {
								continue
							}
						}

						dir := dStringForIndexDirection(index.ColumnDirections[i])
						if err := appendRow(index, col, sequence, dir, false, false); err != nil {
							return err
						}
						sequence++
						delete(implicitCols, col)
					}
					for _, col := range index.StoreColumnNames {
						// We add a row for each stored column of index.
						if err := appendRow(index, col, sequence,
							indexDirectionNA, true, false); err != nil {
							return err
						}
						sequence++
						delete(implicitCols, col)
					}
					for col := range implicitCols {
						// We add a row for each implicit column of index.
						if err := appendRow(index, col, sequence,
							indexDirectionAsc, false, true); err != nil {
							return err
						}
						sequence++
					}
					return nil
				})
			})
	},
}

// 用于兼容Oracle show index table@index 语句显示索引分区信息
var informationSchemaIndexPartitionTable = virtualSchemaTable{
	comment: `index partition information`,
	schema: `
CREATE TABLE information_schema.index_partition (
	TABLE_CATALOG STRING NOT NULL,
	TABLE_SCHEMA  STRING NOT NULL,
	TABLE_NAME    STRING NOT NULL,
	NON_UNIQUE    STRING NOT NULL,
	INDEX_SCHEMA  STRING NOT NULL,
	INDEX_NAME    STRING NOT NULL,
	INDEX_ID	  OID,
	PARTITION_NAME STRING,
	SUBPARTITION_NAME STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual tables have no indexes */
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				h := makeOidHasher()
				dbNameStr := tree.NewDString(db.GetName())
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.GetName())

				appendRow := func(index *sqlbase.IndexDescriptor, partition string, subpartition string,
				) error {
					return addRow(
						dbNameStr,                   // table_catalog
						scNameStr,                   // table_schema
						tbNameStr,                   // table_name
						yesOrNoDatum(!index.Unique), // non_unique
						scNameStr,                   // index_schema
						tree.NewDString(index.Name), // index_name
						h.IndexOid(table.ID, index.ID),
						tree.NewDString(partition),
						tree.NewDString(subpartition),
					)
				}

				return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
					if index.IsPartitioned() {
						for _, k := range index.Partitioning.Range {
							if err := appendRow(index, k.Name, ""); err != nil {
								return err
							}
						}
						for _, k := range index.Partitioning.List {
							var subPartitionList = ""
							var getSubPartitionName func(list sqlbase.PartitioningDescriptor_List)
							getSubPartitionName = func(list sqlbase.PartitioningDescriptor_List) {
								flagHasList := false
								for i, subList := range list.Subpartitioning.List {
									flagHasList = true
									if i != 0 {
										subPartitionList += ","
									}
									subPartitionList += subList.Name
									if subList.Subpartitioning.List != nil {
										subPartitionList += "("
									}
									getSubPartitionName(subList)
									if subList.Subpartitioning.List != nil {
										subPartitionList += ")"
									}
								}
								if flagHasList {
									subPartitionList += ","
								}
								for i, subRange := range list.Subpartitioning.Range {
									if i != 0 {
										subPartitionList += ","
									}
									subPartitionList += subRange.Name
								}
							}
							getSubPartitionName(k)
							if err := appendRow(index, k.Name, subPartitionList); err != nil {
								return err
							}
						}
					} else {
						if err := appendRow(index, "", ""); err != nil {
							return err
						}
					}
					return nil
				})
			})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/table-constraints-table.html
var informationSchemaTableConstraintTable = virtualSchemaTable{
	comment: `table constraints
` + base.DocsURL("information-schema.html#table_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-table-constraints.html`,
	schema: `
CREATE TABLE information_schema.table_constraints (
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL,
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	CONSTRAINT_TYPE    STRING NOT NULL,
	IS_DEFERRABLE      STRING NOT NULL,
	INITIALLY_DEFERRED STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual, /* virtual tables have no constraints */
			func(
				db *sqlbase.DatabaseDescriptor,
				scName string,
				table *sqlbase.TableDescriptor,
				tableLookup tableLookupFn,
			) error {
				conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
				if err != nil {
					return err
				}

				dbNameStr := tree.NewDString(db.Name)
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.Name)

				for conName, c := range conInfo {
					if err := addRow(
						dbNameStr,                       // constraint_catalog
						scNameStr,                       // constraint_schema
						tree.NewDString(conName),        // constraint_name
						dbNameStr,                       // table_catalog
						scNameStr,                       // table_schema
						tbNameStr,                       // table_name
						tree.NewDString(string(c.Kind)), // constraint_type
						yesOrNoDatum(false),             // is_deferrable
						yesOrNoDatum(false),             // initially_deferred
					); err != nil {
						return err
					}
				}
				return nil
			})
	},
}

// Postgres: not provided
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/user-privileges-table.html
// TODO(knz): this introspection facility is of dubious utility.
var informationSchemaUserPrivileges = virtualSchemaTable{
	comment: `grantable privileges (incomplete)`,
	schema: `
CREATE TABLE information_schema.user_privileges (
	GRANTOR        STRING NOT NULL,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, func(dbDesc *DatabaseDescriptor) error {
			privs := dbDesc.Privileges.Show()
			dbNameStr := tree.NewDString(dbDesc.Name)
			for _, u := range privs {
				grantee := tree.NewDString(u.User)
				for _, pp := range u.Privileges {
					hasAdminRole, err := p.HasAdminRole(ctx)
					if err != nil {
						return err
					}
					if !hasAdminRole &&
						p.sessionDataMutator.data.User != pp.Grantor &&
						p.sessionDataMutator.data.User != u.User {
						continue
					}
					if err := addRow(
						tree.NewDString(pp.Grantor),
						grantee,                    // grantee
						dbNameStr,                  // table_catalog
						tree.NewDString(pp.Type),   // privilege_type
						yesOrNoDatum(pp.GrantAble), // is_grantable
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/table-privileges-table.html
var informationSchemaTablePrivileges = virtualSchemaTable{
	comment: `privileges granted on table or views (incomplete; may contain excess users or roles)
` + base.DocsURL("information-schema.html#table_privileges") + `
https://www.postgresql.org/docs/9.5/infoschema-table-privileges.html`,
	schema: `
CREATE TABLE information_schema.table_privileges (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
    TABLE_TYPE     STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING,
	WITH_HIERARCHY STRING NOT NULL
)`,
	populate: populateTablePrivileges,
}

// populateTablePrivileges is used to populate both table_privileges.
func populateTablePrivileges(
	ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error,
) error {
	return forEachTableDesc(ctx, p, dbContext, virtualMany,
		func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
			// check the type of the relation
			tableType := tableTypeBaseTable
			if table.IsVirtualTable() {
				tableType = tableTypeSystemView
			} else if table.IsView() {
				tableType = tableTypeView
			} else if table.IsSequence() {
				tableType = tableTypeSequence
			} else if table.Temporary {
				tableType = tableTypeTemporary
			}
			user := p.User()
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			tbNameStr := tree.NewDString(table.Name)
			// TODO(knz): This should filter for the current user, see
			// https://github.com/znbasedb/znbase/issues/35572
			for _, u := range table.Privileges.Show() {
				for _, priv := range u.Privileges {
					// general user couldn't show grants on object without any privileges.
					hasAdminRole, err := p.HasAdminRole(ctx)
					if err != nil {
						return err
					}
					if !hasAdminRole {
						// if user is not root or in Admin Role
						hasPriv, err := checkRowPriv(ctx, p, priv.Grantor, u.User, user)
						if err != nil {
							return err
						}
						if !hasPriv {
							continue
						}
					}

					if err := addRow(
						tree.NewDString(priv.Grantor),       // grantor
						tree.NewDString(u.User),             // grantee
						dbNameStr,                           // table_catalog
						scNameStr,                           // table_schema
						tbNameStr,                           // table_name
						tableType,                           // table_type
						tree.NewDString(priv.Type),          // privilege_type
						yesOrNoDatum(priv.GrantAble),        // is_grantable
						yesOrNoDatum(priv.Type == "SELECT"), // with_hierarchy
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
}

// populateRoleTableGrants is used to populate role_table_grants.
func populateRoleTableGrants(
	ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error,
) error {
	return forEachTableDesc(ctx, p, dbContext, virtualMany,
		func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
			// check the type of the relation
			tableType := tableTypeBaseTable
			if table.IsVirtualTable() {
				tableType = tableTypeSystemView
			} else if table.IsView() {
				tableType = tableTypeView
			} else if table.IsSequence() {
				tableType = tableTypeSequence
			} else if table.Temporary {
				tableType = tableTypeTemporary
			}
			user := p.User()
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			tbNameStr := tree.NewDString(table.Name)
			// TODO(knz): This should filter for the current user, see
			// https://github.com/znbasedb/znbase/issues/35572
			for _, u := range table.Privileges.Show() {
				for _, priv := range u.Privileges {
					// general user couldn't show grants on object without any privileges.
					hasAdminRole, err := p.HasAdminRole(ctx)
					if err != nil {
						return err
					}
					if !hasAdminRole {
						// if user is not root or in Admin Role
						hasPriv, err := checkRowPriv(ctx, p, priv.Grantor, u.User, user)
						if err != nil {
							return err
						}
						if !hasPriv {
							continue
						}
					}

					roles, err := p.MemberOfWithAdminOption(ctx, user)
					if err != nil {
						return err
					}
					_, ok := roles[u.User]
					_, isAdmin := roles["admin"]
					// just show privileges in current user.
					if user == "root" || isAdmin || u.User == user || ok {
						if err := addRow(
							tree.NewDString(priv.Grantor),       // grantor
							tree.NewDString(u.User),             // grantee
							dbNameStr,                           // table_catalog
							scNameStr,                           // table_schema
							tbNameStr,                           // table_name
							tableType,                           // table_type
							tree.NewDString(priv.Type),          // privilege_type
							yesOrNoDatum(priv.GrantAble),        // is_grantable
							yesOrNoDatum(priv.Type == "SELECT"), // with_hierarchy
						); err != nil {
							return err
						}
					}

				}
			}
			return nil
		})
}

var (
	tableTypeSystemView = tree.NewDString("SYSTEM VIEW")
	tableTypeBaseTable  = tree.NewDString("BASE TABLE")
	tableTypeView       = tree.NewDString("VIEW")
	tableTypeSequence   = tree.NewDString("SEQUENCE")
	tableTypeTemporary  = tree.NewDString("LOCAL TEMPORARY")
)

var informationSchemaTablesTable = virtualSchemaTable{
	comment: `tables and views
` + base.DocsURL("information-schema.html#tables") + `
https://www.postgresql.org/docs/9.5/infoschema-tables.html`,
	schema: vtable.InformationSchemaTables,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, virtualMany,
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				if table.IsSequence() {
					return nil
				}
				tableType := tableTypeBaseTable
				insertable := yesString
				if table.IsVirtualTable() {
					tableType = tableTypeSystemView
					insertable = noString
				} else if table.IsView() {
					tableType = tableTypeView
					insertable = noString
				} else if table.Temporary {
					tableType = tableTypeTemporary
				}
				dbNameStr := tree.NewDString(db.Name)
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.Name)
				return addRow(
					dbNameStr,                              // table_catalog
					scNameStr,                              // table_schema
					tbNameStr,                              // table_name
					tableType,                              // table_type
					insertable,                             // is_insertable_into
					tree.NewDInt(tree.DInt(table.Version)), // version
				)
			})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-views.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/views-table.html
var informationSchemaViewsTable = virtualSchemaTable{
	comment: `views (incomplete)
` + base.DocsURL("information-schema.html#views") + `
https://www.postgresql.org/docs/9.5/infoschema-views.html`,
	schema: `
CREATE TABLE information_schema.views (
    TABLE_CATALOG              STRING NOT NULL,
    TABLE_SCHEMA               STRING NOT NULL,
    TABLE_NAME                 STRING NOT NULL,
    VIEW_DEFINITION            STRING NOT NULL,
    CHECK_OPTION               STRING,
    IS_UPDATABLE               STRING NOT NULL,
    IS_INSERTABLE_INTO         STRING NOT NULL,
    IS_TRIGGER_UPDATABLE       STRING NOT NULL,
    IS_TRIGGER_DELETABLE       STRING NOT NULL,
    IS_TRIGGER_INSERTABLE_INTO STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual schemasMap have no views */
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				if !table.IsView() {
					return nil
				}
				if p.CheckAnyPrivilege(ctx, table) != nil {
					return nil
				}
				// Note that the view query printed will not include any column aliases
				// specified outside the initial view query into the definition returned,
				// unlike Postgres. For example, for the view created via
				//  `CREATE VIEW (a) AS SELECT b FROM foo`
				// we'll only print `SELECT b FROM foo` as the view definition here,
				// while Postgres would more accurately print `SELECT b AS a FROM foo`.
				// TODO(a-robinson): Insert column aliases into view query once we
				// have a semantic query representation to work with (#10083).
				return addRow(
					tree.NewDString(db.Name),         // table_catalog
					tree.NewDString(scName),          // table_schema
					tree.NewDString(table.Name),      // table_name
					tree.NewDString(table.ViewQuery), // view_definition
					tree.DNull,                       // check_option
					noString,                         // is_updatable
					noString,                         // is_insertable_into
					noString,                         // is_trigger_updatable
					noString,                         // is_trigger_deletable
					noString,                         // is_trigger_insertable_into
				)
			})
	},
}

var informationSchemaUserDefineFuncsTable = virtualSchemaTable{
	comment: `user define functions`,
	schema: `
CREATE TABLE information_schema.functions (
    FUNCTION_CATALOG              STRING NOT NULL,
    FUNCTION_SCHEMA               STRING NOT NULL,
    FUNCTION_NAME                 STRING NOT NULL,
    FUNC_TYPE                  	  STRING NOT NULL,
    ARG_NAME_TYPES            	  STRING NOT NULL,
    RETURN_TYPE         		  STRING,
    FUNCTION_DEFINITION           STRING NOT NULL,
    PL_LANGUAGE       	 		  STRING NOT NULL,
    IS_TRIGGER_DELETABLE          STRING NOT NULL,
    IS_TRIGGER_INSERTABLE_INTO    STRING NOT NULL,
	FUNCTION_OWNER				  NAME
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachFunctionDesc(ctx, p, dbContext,
			func(db *sqlbase.DatabaseDescriptor, scName string, function *sqlbase.FunctionDescriptor) error {
				if p.CheckAnyPrivilege(ctx, function) != nil {
					return nil
				}
				// funcType
				funcType := ""
				if function.IsProcedure {
					funcType = "proc"
				} else {
					funcType = "func"
				}

				// arg inout, name and type string
				argAndTypeStr := ""
				for i, arg := range function.Args {
					if i != 0 {
						argAndTypeStr += ", "
					}
					if arg.InOutMode != "in" {
						argAndTypeStr += arg.InOutMode + " "
					}
					argAndTypeStr += arg.Name + " "
					argAndTypeStr += arg.ColumnTypeString
				}

				// function definition
				var funcDefinition string
				if function.Language == "plsql" {
					funcDefinition = function.FuncDef
				} else {
					funcDefinition = "extern function in path \n" + function.FilePath
				}

				// return type, procedure don't show return type
				retTypStr := function.ReturnType
				if function.IsProcedure {
					retTypStr = ""
				}
				return addRow(
					tree.NewDString(db.Name),           // function_catalog
					tree.NewDString(scName),            // function_schema
					tree.NewDString(function.Name),     // function_name
					tree.NewDString(funcType),          // func_type
					tree.NewDString(argAndTypeStr),     // arg_name_types
					tree.NewDString(retTypStr),         // return_type
					tree.NewDString(funcDefinition),    // function_definition
					tree.NewDString(function.Language), // pl_language
					noString,                           // is_trigger_deletable
					noString,                           // is_trigger_insertable_into
					getOwnerName(function),             // function_owner
				)
			})
	},
}

// forEachSchemaName iterates over the physical and virtual schemasMap.
func forEachSchemaName(
	ctx context.Context,
	p *planner,
	db *sqlbase.DatabaseDescriptor,
	requiresPrivileges bool,
	fn func(descriptor *sqlbase.SchemaDescriptor) error,
) error {
	var scs []*sqlbase.SchemaDescriptor
	newVirtualSchema := func(scName string) *sqlbase.SchemaDescriptor {
		return &sqlbase.SchemaDescriptor{
			Name:       scName,
			Privileges: db.Privileges,
		}
	}
	if db.Name == sqlbase.SystemDB.Name {
		scs = append(scs, newVirtualSchema(tree.PublicSchema))
	}
	// Handle virtual schemasMap.
	for _, schema := range p.getVirtualTabler().getEntries() {
		scs = append(scs, newVirtualSchema(schema.desc.Name))
	}
	for i := range db.Schemas {
		if requiresPrivileges {
			if p.CheckAnyPrivilege(ctx, &db.Schemas[i]) != nil {
				continue
			}
		}
		scs = append(scs, &db.Schemas[i])
	}
	sort.Slice(scs, func(i, j int) bool {
		return scs[i].Name < scs[j].Name
	})
	for _, sc := range scs {
		if err := fn(sc); err != nil {
			return err
		}
	}
	return nil
}

// forEachDatabaseDesc calls a function for the given DatabaseDescriptor, or if
// it is nil, retrieves all database descriptors and iterates through them in
// lexicographical order with respect to their name. If privileges are required,
// the function is only called if the user has privileges on the database.
func forEachDatabaseDesc(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	requiresPrivileges bool,
	fn func(*sqlbase.DatabaseDescriptor) error,
) error {
	descs, err := p.Tables().getAllDescriptors(ctx, p.txn)
	if err != nil {
		return err
	}

	// Ignore table descriptors.
	var dbDescs []*sqlbase.DatabaseDescriptor
	for _, desc := range descs {
		if dbDesc, ok := desc.(*sqlbase.DatabaseDescriptor); ok &&
			(dbContext == nil || dbContext.ID == dbDesc.ID) {
			// If dbContext is not nil, we can't just use dbContext because we need to
			// fetch descriptor with privileges from kv.
			dbDescs = append(dbDescs, dbDesc)
		}
	}

	// Ignore databases that the user cannot see.
	for _, db := range dbDescs {
		if !requiresPrivileges || userCanSeeDatabase(ctx, p, db) {
			if err := fn(db); err != nil {
				return err
			}
		}
	}
	return nil
}

// forEachTableDesc retrieves all table descriptors from the current
// database and all system databases and iterates through them. For
// each table, the function will call fn with its respective database
// and table descriptor.
//
// The dbContext argument specifies in which database context we are
// requesting the descriptors. In context nil all descriptors are
// visible, in non-empty contexts only the descriptors of that
// database are visible.
//
// The virtualOpts argument specifies how virtual tables are made
// visible.
func forEachTableDesc(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	fn func(*sqlbase.DatabaseDescriptor, string, *sqlbase.TableDescriptor) error,
) error {
	return forEachTableDescWithTableLookup(ctx, p, dbContext, virtualOpts, func(
		db *sqlbase.DatabaseDescriptor,
		scName string,
		table *sqlbase.TableDescriptor,
		_ tableLookupFn,
	) error {
		return fn(db, scName, table)
	})
}

func forEachFunctionDesc(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	fn func(*sqlbase.DatabaseDescriptor, string, *sqlbase.FunctionDescriptor) error,
) error {
	return forEachFunctionDescWithFunctionLookupInternal(ctx, p, dbContext, false,
		func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			function *sqlbase.FunctionDescriptor,
			_ tableLookupFn,
			_ *planner,
		) error {
			return fn(db, scName, function)
		})
}

type virtualOpts int

const (
	// virtualMany iterates over virtual schemasMap in every catalog/database.
	virtualMany virtualOpts = iota
	// virtualOnce iterates over virtual schemasMap once, in the nil database.
	virtualOnce
	// hideVirtual completely hides virtual schemasMap during iteration.
	hideVirtual
)

// forEachTableDescAll does the same as forEachTableDesc but also
// includes newly added non-public descriptors.
func forEachTableDescAll(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	fn func(*sqlbase.DatabaseDescriptor, string, *sqlbase.TableDescriptor) error,
) error {
	return forEachTableDescWithTableLookupInternal(ctx,
		p, dbContext, virtualOpts, true, /* allowAdding */
		func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			_ tableLookupFn,
			_ *planner,
		) error {
			return fn(db, scName, table)
		})
}

// forEachTableDescWithTableLookup acts like forEachTableDesc, except it also provides a
// tableLookupFn when calling fn to allow callers to lookup fetched table descriptors
// on demand. This is important for callers dealing with objects like foreign keys, where
// the metadata for each object must be augmented by looking at the referenced table.
//
// The dbContext argument specifies in which database context we are
// requesting the descriptors.  In context "" all descriptors are
// visible, in non-empty contexts only the descriptors of that
// database are visible.
func forEachTableDescWithTableLookup(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	fn func(*sqlbase.DatabaseDescriptor, string, *sqlbase.TableDescriptor, tableLookupFn) error,
) error {
	return forEachTableDescWithTableLookupInternal(ctx, p, dbContext, virtualOpts, false,
		func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
			_ *planner,
		) error {
			return fn(db, scName, table, tableLookup)
		})
}

// forEachTableDescWithTableLookupInternal is the logic that supports
// forEachTableDescWithTableLookup.
//
// The allowAdding argument if true includes newly added tables that
// are not yet public.
func forEachTableDescWithTableLookupInternal(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	allowAdding bool,
	fn func(*DatabaseDescriptor, string, *TableDescriptor, tableLookupFn, *planner) error,
) error {
	descs, err := p.Tables().getAllDescriptors(ctx, p.txn)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(descs, dbContext)

	if virtualOpts == virtualMany || virtualOpts == virtualOnce {
		// Virtual descriptors first.
		vt := p.getVirtualTabler()
		vEntries := vt.getEntries()
		vSchemaNames := vt.getSchemaNames()
		iterate := func(dbDesc *DatabaseDescriptor) error {
			for _, virtSchemaName := range vSchemaNames {
				e := vEntries[virtSchemaName]
				for _, tName := range e.orderedDefNames {
					te := e.defs[tName]
					if err := fn(dbDesc, virtSchemaName, te.desc, lCtx, p); err != nil {
						return err
					}
				}
			}
			return nil
		}

		switch virtualOpts {
		case virtualOnce:
			if err := iterate(nil); err != nil {
				return err
			}
		case virtualMany:
			for _, dbID := range lCtx.dbIDs {
				dbDesc := lCtx.dbDescs[dbID]
				if err := iterate(dbDesc); err != nil {
					return err
				}
			}
		}
	}

	// Physical descriptors next.
	for _, tbID := range lCtx.tbIDs {
		table := lCtx.tbDescs[tbID]
		if table.Dropped() {
			continue
		}
		scDesc, err := lCtx.getSchemaByID(table.GetParentID())
		if err != nil {
			dbDesc, err := lCtx.getDatabaseByID(table.GetParentID())
			if err != nil {
				return err
			}
			if err := fn(dbDesc, tree.PublicSchema, table, lCtx, p); err != nil {
				return err
			}
		}
		if scDesc != nil {
			dbDesc, err := lCtx.getDatabaseByID(scDesc.ParentID)
			if err != nil {
				return err
			}
			if !userCanSeeTable(ctx, p, table, scDesc, allowAdding) {
				continue
			}
			if err := fn(dbDesc, scDesc.Name, table, lCtx, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// forEachFunctionDescWithFunctionLookupInternal is the logic that supports
// forEachFunctionDescWithFunctionLookup.
//
// The allowAdding argument if true includes newly added tables that
// are not yet public.
func forEachFunctionDescWithFunctionLookupInternal(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	allowAdding bool,
	fn func(*DatabaseDescriptor, string, *FunctionDescriptor, tableLookupFn, *planner) error,
) error {
	descs, err := p.Tables().getAllDescriptors(ctx, p.txn)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(descs, dbContext)

	// Physical descriptors next.
	for _, funcID := range lCtx.funcIDs {
		function := lCtx.funcDescs[funcID]
		if function.Dropped() || !userCanSeeFunction(ctx, p, function, allowAdding) {
			continue
		}

		scDesc, err := lCtx.getSchemaByID(function.GetParentID())
		if err != nil {
			dbDesc, err := lCtx.getDatabaseByID(function.GetParentID())
			if err != nil {
				return err
			}
			if err := fn(dbDesc, tree.PublicSchema, function, lCtx, p); err != nil {
				return err
			}
		}
		if scDesc != nil {
			dbDesc, err := lCtx.getDatabaseByID(scDesc.ParentID)
			if err != nil {
				return err
			}
			if err := fn(dbDesc, scDesc.Name, function, lCtx, p); err != nil {
				return err
			}
		}
	}
	return nil
}

func forEachIndexInTable(
	table *sqlbase.TableDescriptor, fn func(*sqlbase.IndexDescriptor) error,
) error {
	if table.IsPhysicalTable() {
		if err := fn(&table.PrimaryIndex); err != nil {
			return err
		}
	}
	for i := range table.Indexes {
		if err := fn(&table.Indexes[i]); err != nil {
			return err
		}
	}
	return nil
}

func forEachColumnInTable(
	table *sqlbase.TableDescriptor, fn func(*sqlbase.ColumnDescriptor) error,
) error {
	// Table descriptors already hold columns in-order.
	for i := range table.Columns {
		if err := fn(&table.Columns[i]); err != nil {
			return err
		}
	}
	return nil
}

// forEachColumnCanSeeInTable is modified based on forEachColumnInTable,
// to satisfy show_columns where user only has column privileges on table.
//func forEachColumnCanSeeInTable(
//	ctx context.Context,
//	p *planner,
//	table *sqlbase.TableDescriptor,
//	fn func(*sqlbase.ColumnDescriptor) error,
//) error {
//	for i := range table.Privileges.Users {
//		if table.Privileges.Users[i].User == p.User() {
//			return forEachColumnInTable(table, fn)
//		}
//	}
//	// Table descriptors already hold columns in-order.
//	for i := range table.Columns {
//		if !table.IsSequence() {
//			if err := p.CheckAnyPrivilege(ctx, &table.Columns[i]); err != nil {
//				continue
//			}
//		}
//		if err := fn(&table.Columns[i]); err != nil {
//			return err
//		}
//	}
//	return nil
//}

func forEachColumnInIndex(
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	fn func(*sqlbase.ColumnDescriptor) error,
) error {
	colMap := make(map[sqlbase.ColumnID]*sqlbase.ColumnDescriptor, len(table.Columns))
	for i, column := range table.Columns {
		colMap[column.ID] = &table.Columns[i]
	}
	for _, columnID := range index.ColumnIDs {
		column, ok := colMap[columnID]
		if !ok {
			continue
		}
		if err := fn(column); err != nil {
			return err
		}
	}
	return nil
}

func forEachRole(
	ctx context.Context, p *planner, fn func(username string, isRole bool) error,
) error {
	query := `SELECT username, "isRole" FROM system.users`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-roles", p.txn, query,
	)
	if err != nil {
		return err
	}

	for _, row := range rows {
		username := tree.MustBeDString(row[0])
		isRole, ok := row[1].(*tree.DBool)
		if !ok {
			return errors.Errorf("isRole should be a boolean value, found %s instead", row[1].ResolvedType())
		}

		if err := fn(string(username), bool(*isRole)); err != nil {
			return err
		}
	}
	return nil
}

func forEachRoleMembership(
	ctx context.Context, p *planner, fn func(role, member string, isAdmin bool) error,
) error {
	query := `SELECT "role", "member", "isAdmin" FROM system.role_members`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-members", p.txn, query,
	)
	if err != nil {
		return err
	}

	for _, row := range rows {
		roleName := tree.MustBeDString(row[0])
		memberName := tree.MustBeDString(row[1])
		isAdmin := row[2].(*tree.DBool)

		if err := fn(string(roleName), string(memberName), bool(*isAdmin)); err != nil {
			return err
		}
	}
	return nil
}

func forEachNodeInTable(
	nodes *serverpb.NodesResponse, fn func(nodeID, nodeAddress, nodeName tree.Datum) error,
) error {
	for _, node := range nodes.Nodes {
		nameList := node.Desc.LocationName.Names
		address := node.Desc.Address.AddressField
		nodeID := node.Desc.NodeID
		for _, name := range nameList {
			if err := fn(tree.Datum(tree.NewDInt(tree.DInt(nodeID))), tree.NewDString(address), tree.NewDString(name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func userCanSeeDatabase(ctx context.Context, p *planner, db *sqlbase.DatabaseDescriptor) bool {
	return p.CheckAnyPrivilege(ctx, db) == nil
}

func userCanSeeTable(
	ctx context.Context,
	p *planner,
	table *sqlbase.TableDescriptor,
	schema *sqlbase.SchemaDescriptor,
	allowAdding bool,
) bool {
	return tableIsVisible(table, allowAdding) && p.CheckAnyPrivilege(ctx, schema) == nil
}

func tableIsVisible(table *TableDescriptor, allowAdding bool) bool {
	return table.State == sqlbase.TableDescriptor_PUBLIC ||
		(allowAdding && table.State == sqlbase.TableDescriptor_ADD)
}

func userCanSeeFunction(
	ctx context.Context, p *planner, function *sqlbase.FunctionDescriptor, allowAdding bool,
) bool {
	return functionIsVisible(function, allowAdding) && p.CheckAnyPrivilege(ctx, function) == nil
}

func functionIsVisible(function *FunctionDescriptor, allowAdding bool) bool {
	return function.State == sqlbase.FunctionDescriptor_PUBLIC ||
		(allowAdding && function.State == sqlbase.FunctionDescriptor_ADD)
}

func checkRowPriv(
	ctx context.Context, planner *planner, granter string, grantee string, user string,
) (bool, error) {
	if user != granter && user != grantee {
		if granter == sqlbase.PublicRole || grantee == sqlbase.PublicRole {
			return true, nil
		}
		membersOf, err := planner.MemberOfWithAdminOption(ctx, user)
		if err != nil {
			return false, err
		}
		for role := range membersOf {
			if role == granter || role == grantee {
				return true, nil
			}
		}
		return false, nil
	}
	return true, nil
}

var informationSchemaTriggerPrivileges = virtualSchemaTable{
	comment: `Trigger privileges (incomplete; may contain excess users or roles)
` + base.DocsURL("information-schema.html#trigger_privileges"),
	schema: `
CREATE TABLE information_schema.trigger_privileges (
   "relid"   INT NOT NULL,
   "name"    STRING NOT NULL,
   "funcbody"  STRING,
   "funcid" INT8,
   "type"    STRING,
   "enable"  STRING NOT NULL,
   "isconstraint"    STRING NOT NULL, 
   "constrname"      STRING,
   "constrrelid"     INT8,
   "deferrable"      STRING,
   "initdeferred"    STRING,
   "nargs"           INT8,
   "args"            STRING[],
   "whenexpr"        BYTES,
   "funcHint"		 STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil, false, func(db *sqlbase.DatabaseDescriptor) error {
			return forEachTableDesc(ctx, p, db, virtualMany, func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				return forEachTriggerName(ctx, p, table, func(tr *sqlbase.Trigger) error {

					var funcBody, rowTimeEvent tree.DString
					var commentTmpForBody, commentTmpForRTE, commentArgs tree.Datum
					var commentID tree.Datum = tree.NewDInt(tree.DInt(tr.TgfuncID))
					var rowTimeEve string
					// deal args
					str := make([]tree.Datum, 0)
					for i := 0; i < len(tr.Tgargs); i++ {
						var value = tr.Tgargs[i]
						str = append(str, tree.NewDString(value))
					}
					commentArgs = &tree.DArray{
						ParamTyp: types.String,
						Array:    str,
						HasNulls: false,
					}

					// used to show function body of function bound to trigger
					if des, err := sqlbase.GetFunctionDescFromID(ctx, p.txn, sqlbase.ID(tr.TgfuncID)); err == nil {
						funcBody = tree.DString(des.FuncDef)
						commentTmpForBody = &funcBody
					} else {
						commentTmpForBody = tree.DNull
						commentID = tree.DNull
					}

					// deal types
					// the number tr.Tgtype means
					// truncate update delete insert | instead after before | statement row
					if tr.Tgtype&28 == 4 { // time
						rowTimeEve += "BEFORE|"
					} else if tr.Tgtype&28 == 8 {
						rowTimeEve += "AFTER|"
					} else {
						rowTimeEve += "INSTEAD_OF|"
					}
					if tr.Tgtype&480 == 256 { // events
						rowTimeEve += "TRUNCATE|"
					} else if tr.Tgtype&480 == 128 {
						rowTimeEve += "UPDATE|"
					} else if tr.Tgtype&480 == 64 {
						rowTimeEve += "DELETE|"
					} else {
						rowTimeEve += "INSERT|"
					}
					if tr.Tgtype&1 == 1 { // row or statement
						rowTimeEve += "ROW"
					} else {
						rowTimeEve += "STATEMENT"
					}
					rowTimeEvent = tree.DString(rowTimeEve)
					commentTmpForRTE = &rowTimeEvent

					// function used to convert bool to string
					boolToString := func(old bool) string {
						if old == true {
							return "true"
						}
						return "false"
					}
					err := addRow(
						tree.NewDInt(tree.DInt(tr.Tgrelid)),
						tree.NewDString(tr.Tgname),
						commentTmpForBody,
						commentID,
						commentTmpForRTE,
						tree.NewDString(boolToString(tr.Tgenable)),
						tree.NewDString(boolToString(tr.Tgsconstraint)),
						tree.NewDString(tr.Tgconstrname),
						tree.NewDInt(tree.DInt(tr.Tgconstrrelid)),
						tree.NewDString(boolToString(tr.Tgdeferrable)),
						tree.NewDString(boolToString(tr.Tginitdeferred)),
						tree.NewDInt(tree.DInt(tr.Tgnargs)),
						commentArgs,
						tree.NewDBytes(tree.DBytes(tr.Tgwhenexpr)),
						tree.NewDString(boolToString(tr.TgProHint)))
					if err != nil {
						return err
					}
					return nil
				})
			})
		})
	},
}

func forEachTriggerName(
	ctx context.Context,
	p *planner,
	table *sqlbase.TableDescriptor,
	fn func(descriptor *sqlbase.Trigger) error,
) error {
	if !util.EnableUDR {
		return pgerror.NewError(pgcode.FeatureNotSupported, "not support trigger now!")
	}

	descs, err := MakeTriggerDesc(ctx, p.txn, table)
	if err != nil {
		return err
	}
	if p.CheckPrivilege(ctx, table, privilege.TRIGGER) == nil {
		for _, tr := range descs.Triggers {

			if int(tr.Tgrelid) != int(table.ID) {
				continue
			}
			if err := fn(&tr); err != nil {
				return err
			}
		}
		if len(descs.Triggers) > 1 {
			sort.Slice(descs.Triggers, func(i, j int) bool {
				return descs.Triggers[i].Tgname < descs.Triggers[j].Tgname
			})
		}
	}
	return nil
}
