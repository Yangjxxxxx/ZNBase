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

package sql

import (
	"context"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type commentOnDatabaseNode struct {
	n      *tree.CommentOnDatabase
	dbDesc *sqlbase.DatabaseDescriptor
}

// CommentOnDatabase add comment on a database.
// Privileges: CREATE on database.
//   notes: postgres requires CREATE on the database.
func (p *planner) CommentOnDatabase(
	ctx context.Context, n *tree.CommentOnDatabase,
) (planNode, error) {
	dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Name), true)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnDatabaseNode{n: n, dbDesc: dbDesc}, nil
}

func (n *commentOnDatabaseNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"set-db-comment",
			params.p.Txn(),
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.DatabaseCommentType,
			n.dbDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"delete-db-comment",
			params.p.Txn(),
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.DatabaseCommentType,
			n.dbDesc.ID)
		if err != nil {
			return err
		}
	}
	// some audit data
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogCommentOnDatabase),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(n.dbDesc.ID),
			Desc: struct {
				User         string
				DatabaseName string
			}{
				params.SessionData().User,
				n.n.Name.String(),
			},
		},
	}
	return nil
}

func (n *commentOnDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnDatabaseNode) Close(context.Context)        {}
