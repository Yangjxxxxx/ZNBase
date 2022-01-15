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
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
)

// ShowFunctions returns all the functions.
// Privileges: None.
//   Notes: postgres does not have a SHOW FUNCTIONS statement.
//          mysql only returns functions you have privileges on.
func (p *planner) ShowFunctions(ctx context.Context, n *tree.ShowFunctions) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support user define function now!")
	}
	var found bool
	var err error
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		found, _, err = n.Resolve(ctx, p, p.CurrentDatabase(), p.CurrentSearchPath())
	})
	if err != nil {
		return nil, err
	}
	if !found {
		if p.CurrentDatabase() == "" && !n.ExplicitSchema {
			return nil, errNoDatabase
		}
		if !n.TableNamePrefix.ExplicitCatalog && !n.TableNamePrefix.ExplicitSchema {
			return nil, pgerror.NewErrorf(
				pgcode.InvalidCatalogName,
				"current search_path does not match any valid schema")
		}
		return nil, sqlbase.NewInvalidWildcardError(tree.ErrString(&n.TableNamePrefix))

	}

	var query string
	const getFunctionsQueryWithoutFuncName = `
  SELECT function_name, function_owner as owner, arg_name_types, return_type, function_definition, func_type, pl_language
    FROM %[1]s.information_schema.functions
   WHERE function_schema = %[2]s
ORDER BY function_schema, function_name, arg_name_types`

	const getFunctionsQueryWithFuncName = `
  SELECT function_name, function_owner as owner, arg_name_types, return_type, function_definition, func_type, pl_language
    FROM %[1]s.information_schema.functions
   WHERE function_schema = %[2]s AND function_name = %[3]s
ORDER BY function_schema, function_name, arg_name_types`

	if n.FuncName == "" {
		query = fmt.Sprintf(getFunctionsQueryWithoutFuncName,
			&n.CatalogName, lex.EscapeSQLString(n.Schema()))
	} else {
		query = fmt.Sprintf(getFunctionsQueryWithFuncName,
			&n.CatalogName, lex.EscapeSQLString(n.Schema()), lex.EscapeSQLString(n.FuncName))
	}

	return p.delegateQuery(ctx, "SHOW FUNCTIONS",
		query,
		func(_ context.Context) error { return nil }, nil)
}
