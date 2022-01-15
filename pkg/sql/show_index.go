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

	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// ShowIndexes returns all the indexes for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW INDEXES statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowIndex(ctx context.Context, n *tree.ShowIndexes) (planNode, error) {
	// const getIndexes = `
	// 			SELECT
	// 				table_name,
	// 				index_name,
	// 				non_unique::BOOL,
	// 				seq_in_index,
	// 				column_name,
	// 				direction,
	// 				storing::BOOL,
	// 				implicit::BOOL
	// 			FROM %[4]s.information_schema.statistics
	// 			WHERE table_catalog=%[1]s AND table_schema=%[5]s AND table_name=%[2]s`
	if n.Index == "" {
		getIndexesQuery := `
SELECT
	table_name,
	index_name,
	non_unique::BOOL,
	seq_in_index,
	column_name,
	direction,
	storing::BOOL,
	implicit::BOOL,
	locality,
	partitioned`

		if n.WithComment {
			getIndexesQuery += `,
	obj_description(statistics.INDEX_ID) AS comment`
		}

		getIndexesQuery += `
FROM
	%[4]s.information_schema.statistics`

		//	if n.WithComment {
		//		getIndexesQuery += `
		//LEFT JOIN pg_class ON
		//	statistics.index_name = pg_class.relname`
		//	}

		getIndexesQuery += `
WHERE
	table_catalog=%[1]s
	AND table_schema=%[5]s
	AND table_name=%[2]s`

		if n.WithComment {
			getIndexesQuery += `
	ORDER BY table_name, index_name, column_name`
		}
		return p.showTableDetails(ctx, "SHOW INDEX", &n.Table, getIndexesQuery, p.CheckAnyPrivilege)
	}
	getIndexPartitionQuery := `
SELECT
	index_name,
	partition_name,
	subpartition_name`

	if n.WithComment {
		getIndexPartitionQuery += `,
	obj_description(index_partition.INDEX_ID) AS comment`
	}

	getIndexPartitionQuery += `
FROM
	%[4]s.information_schema.index_partition`

	//if n.WithComment {
	//	getIndexPartitionQuery += `
	//LEFT JOIN pg_class ON
	//	statistics.index_name = pg_class.relname`
	//}

	getIndexPartitionQuery += `
WHERE
	table_catalog=%[1]s
	AND table_schema=%[5]s
	AND table_name=%[2]s
	AND index_name=%[6]s`

	if n.WithComment {
		getIndexPartitionQuery += `
	ORDER BY index_name, partition_name`
	}
	return p.showTableAndIndexDetails(ctx, "SHOW INDEX PARTITION", &n.Table, getIndexPartitionQuery, string(n.Index))
}
