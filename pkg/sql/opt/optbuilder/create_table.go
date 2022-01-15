// Copyright 2018  The Cockroach Authors.
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

package optbuilder

import (
	"fmt"

	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/opt/props/physical"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
)

// buildCreateTable constructs a CreateTable operator based on the CREATE TABLE
// statement.
func (b *Builder) buildCreateTable(ct *tree.CreateTable, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	isTemp := resolveTemporaryStatus(&ct.Table, ct.Temporary)
	if isTemp {
		// Postgres allows using `pg_temp` as an alias for the session specific temp
		// schema. In PG, the following are equivalent:
		//CREATE TEMP TABLE T <=> CREATE TABLE pg_temp.t <=> CREATE TEMP TABLE pg_temp.t
		// The temporary schema is created the first time a session creates
		// a temporary object, so it is possible to use `pg_temp` in a fully
		// qualified name when the temporary schema does not exist. To allow this,
		// we explicitly set the SchemaName to `public` for temporary tables, as
		// the public schema is guaranteed to exist. This ensures the FQN can be
		// resolved correctly.
		ct.Table.TableNamePrefix.SchemaName = tree.PublicSchemaName
		ct.Temporary = true
	}

	sch, resName := b.resolveSchemaForCreate(&ct.Table)
	// TODO(radu): we are modifying the AST in-place here. We should be storing
	// the resolved name separately.
	ct.Table.TableNamePrefix = resName
	schID := b.factory.Metadata().AddSchema(sch)

	// HoistConstraints normalizes any column constraints in the CreateTable AST
	// node.
	ct.HoistConstraints()

	var input memo.RelExpr
	var inputCols physical.Presentation
	if ct.As() {
		// Build the input query.
		// todo lixinze: create table的as中，没有select for update用法
		outScope := b.buildSelect(ct.AsSource, noRowLocking, nil, inScope, nil)

		numColNames := len(ct.AsColumnNames)
		numColumns := len(outScope.cols)
		if numColNames != 0 && numColNames != numColumns {
			panic(builderError{sqlbase.NewSyntaxError(fmt.Sprintf(
				"CREATE TABLE specifies %d column name%s, but data source has %d column%s",
				numColNames, util.Pluralize(int64(numColNames)),
				numColumns, util.Pluralize(int64(numColumns))))})
		}

		// Synthesize rowid column, and append to end of column list.
		props, overloads := builtins.GetBuiltinProperties("unique_rowid")
		private := &memo.FunctionPrivate{
			Name:       "unique_rowid",
			Typ:        types.Int,
			Properties: props,
			Overload:   &overloads[0],
		}
		fn := b.factory.ConstructFunction(memo.EmptyScalarListExpr, private)
		scopeCol := b.synthesizeColumn(outScope, "rowid", types.Int, nil /* expr */, fn)
		input = b.factory.CustomFuncs().ProjectExtraCol(outScope.expr, fn, scopeCol.id)
		inputCols = outScope.makePhysicalProps().Presentation
	} else {
		// Create dummy empty input.
		input = b.factory.ConstructZeroValues()
	}

	expr := b.factory.ConstructCreateTable(
		input,
		&memo.CreateTablePrivate{
			Schema:    schID,
			InputCols: inputCols,
			Syntax:    ct,
		},
	)
	return &scope{builder: b, expr: expr}
}
