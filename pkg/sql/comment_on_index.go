// Copyright 2019  The Cockroach Authors.
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

type commentOnIndexNode struct {
	n         *tree.CommentOnIndex
	tableDesc *sqlbase.TableDescriptor
	indexDesc *sqlbase.IndexDescriptor
}

// CommentOnIndex adds a comment on an index.
// Privileges: CREATE on table.
func (p *planner) CommentOnIndex(ctx context.Context, n *tree.CommentOnIndex) (planNode, error) {
	tableDesc, indexDesc, err := p.getTableAndIndex(ctx, &n.Index.Table, &n.Index, privilege.REFERENCES)
	if err != nil {
		return nil, err
	}

	return &commentOnIndexNode{n: n, tableDesc: tableDesc.TableDesc(), indexDesc: indexDesc}, nil
}

func (n *commentOnIndexNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		err := params.p.upsertIndexComment(
			params.ctx,
			n.tableDesc.ID,
			n.indexDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		err := params.p.removeIndexComment(params.ctx, n.tableDesc.ID, n.indexDesc.ID)
		if err != nil {
			return err
		}
	}

	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogCommentOnIndex),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(n.tableDesc.ID),
		},
		Info: struct {
			TableName string
			IndexName string
			Statement string
			User      string
			Comment   *string
		}{
			n.tableDesc.Name,
			string(n.n.Index.Index),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment},
	}

	return nil
}

func (p *planner) upsertIndexComment(
	ctx context.Context, tableID sqlbase.ID, indexID sqlbase.IndexID, comment string,
) error {
	_, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		ctx,
		"set-index-comment",
		p.Txn(),
		"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
		keys.IndexCommentType,
		tableID,
		indexID,
		comment)

	return err
}

func (p *planner) removeIndexComment(
	ctx context.Context, tableID sqlbase.ID, indexID sqlbase.IndexID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx,
		"delete-index-comment",
		p.txn,
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.IndexCommentType,
		tableID,
		indexID)

	return err
}

func (n *commentOnIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnIndexNode) Close(context.Context)        {}
