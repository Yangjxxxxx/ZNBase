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
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// showTableDetails extracts information about the given table using
// the given query patterns in SQL. The query pattern must accept
// the following formatting parameters:
// %[1]s the database name as SQL string literal.
// %[2]s the unqualified table name as SQL string literal.
// %[3]s the given table name as SQL string literal.
// %[4]s the database name as SQL identifier.
// %[5]s the schema name as SQL string literal.
func (p *planner) showTableDetails(
	ctx context.Context,
	showType string,
	tn *tree.TableName,
	query string,
	fn func(ctx context.Context, descriptor sqlbase.DescriptorProto) error,
) (planNode, error) {
	// We avoid the cache so that we can observe the details without
	// taking a lease, like other SHOW commands.
	desc, err := p.ResolveUncachedTableDescriptor(ctx, tn, true /*required*/, anyDescType)
	if err != nil {
		return nil, err
	}
	if err := fn(ctx, desc); err != nil {
		return nil, err
	}

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(tn.Catalog()),
		lex.EscapeSQLString(tn.Table()),
		lex.EscapeSQLString(tn.String()),
		tn.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(tn.Schema()),
		desc.ID,
	)

	return p.delegateQuery(ctx, showType, fullQuery,
		func(_ context.Context) error { return nil }, nil)
}

// showTableAndIndexDetails 在 showTableDetails 的基础上进行了索引的检查, 判断索引是否
// 属于该表, 而后补齐SQL语句。 SQL语句showTableDetails的基础上必须满足以下的格式:
// %[6]s the index name as SQL string literal.
func (p *planner) showTableAndIndexDetails(
	ctx context.Context, showType string, tn *tree.TableName, query string, index string,
) (planNode, error) {
	// We avoid the cache so that we can observe the details without
	// taking a lease, like other SHOW commands.
	desc, err := p.ResolveUncachedTableDescriptor(ctx, tn, true /*required*/, anyDescType)
	if err != nil {
		return nil, err
	}
	indexDesc, beingDrop, err := desc.FindIndexByName(index)
	if err != nil {
		return nil, err
	}
	if beingDrop {
		return nil, fmt.Errorf(fmt.Sprintf("index %s is being dropped", indexDesc.Name))
	}
	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return nil, err
	}

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(tn.Catalog()),
		lex.EscapeSQLString(tn.Table()),
		lex.EscapeSQLString(tn.String()),
		tn.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(tn.Schema()),
		lex.EscapeSQLString(indexDesc.Name),
		desc.ID,
	)

	return p.delegateQuery(ctx, showType, fullQuery,
		func(_ context.Context) error { return nil }, nil)
}

// checkDBExists checks if the database exists by using the security.RootUser.
func checkDBExists(ctx context.Context, p *planner, db string) error {
	_, err := p.PhysicalSchemaAccessor().GetDatabaseDesc(ctx, p.txn, db,
		p.CommonLookupFlags(true /*required*/))
	return err
}
