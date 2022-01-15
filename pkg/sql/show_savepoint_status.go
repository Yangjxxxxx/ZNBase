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

import "context"

// ShowTransactionStatus implements the plan for SHOW TRANSACTION STATUS.
// This statement is usually handled as a special case in Executor,
// but for FROM [SHOW SAVEPOINT STATUS] we will arrive here too.
func (p *planner) ShowSavepointStatus(ctx context.Context) (planNode, error) {
	return p.delegateQuery(ctx, "SHOW SAVEPOINT STATUS",
		`SELECT savepoint_name, is_initial_savepoint FROM zbdb_internal.savepoint_status`, nil, nil)
	//return nil, unimplemented.NewWithIssue(47333, "cannot use SHOW SAVEPOINT STATUS as a statement source")
}
