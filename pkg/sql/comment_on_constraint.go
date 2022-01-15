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
	"hash/crc32"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type commentOnConstraintNode struct {
	n              *tree.CommentOnConstraint
	tableDesc      *sqlbase.TableDescriptor
	constraintDesc *sqlbase.ConstraintDetail
}

// CommentOnConstraint adds a comment on a constraint.
// Privileges: CREATE on table.
func (p *planner) CommentOnConstraint(
	ctx context.Context, n *tree.CommentOnConstraint,
) (planNode, error) {
	tableDesc, constraintDesc, err := p.getTableAndConstraint(ctx, &n.Table, &n.Constraint, privilege.REFERENCES)
	if err != nil {
		return nil, err
	}

	return &commentOnConstraintNode{n: n, tableDesc: tableDesc.TableDesc(), constraintDesc: constraintDesc}, nil
}

// transformConstraint return num by encoding constraint
func transformConstraint(tableID sqlbase.ID, constraint sqlbase.ConstraintDetail) uint64 {
	kindID := func(s string) uint64 {
		return uint64(crc32.ChecksumIEEE([]byte(s)))
	}

	var constraintID uint64
	switch constraint.Kind {
	case sqlbase.ConstraintTypeCheck:
		constraintID += kindID(constraint.CheckConstraint.Name) + uint64(tableID)
	default:
		constraintID = kindID(constraint.Index.Name) + uint64(tableID)
	}
	return constraintID
}

func (n *commentOnConstraintNode) startExec(params runParams) error {
	constraint, _, err := n.tableDesc.FindConstraintByName(params.ctx, params.p.txn, string(n.n.Constraint))
	if err != nil {
		return err
	}
	if n.n.Comment != nil {
		err := params.p.upsertConstraintComment(
			params.ctx,
			n.tableDesc.ID,
			*constraint,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		err := params.p.removeConstraintComment(params.ctx, n.tableDesc.ID, *constraint)
		if err != nil {
			return err
		}
	}

	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		timeutil.Now(),
		params.p.txn,
		string(EventLogCommentOnConstraint),
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			TableName      string
			ConstraintName string
			Statement      string
			User           string
			Comment        *string
		}{
			n.tableDesc.Name,
			string(n.n.Constraint),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment},
	)
}

func (p *planner) upsertConstraintComment(
	ctx context.Context, tableID sqlbase.ID, constraint sqlbase.ConstraintDetail, comment string,
) error {
	constraintID := transformConstraint(tableID, constraint)
	_, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		ctx,
		"set-constraint-comment",
		p.Txn(),
		"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
		keys.ConstraintCommentType,
		tableID,
		constraintID,
		comment)

	return err
}

func (p *planner) updateCheckConstraintComment(
	ctx context.Context, tableID sqlbase.ID, constraint sqlbase.ConstraintDetail, newName string,
) error {
	var newConstraint sqlbase.ConstraintDetail
	if constraint.Kind == sqlbase.ConstraintTypeCheck {
		newConstraint = sqlbase.ConstraintDetail{
			Kind: constraint.Kind,
			CheckConstraint: &sqlbase.TableDescriptor_CheckConstraint{
				Name: newName,
			},
		}
	} else {
		newConstraint = sqlbase.ConstraintDetail{
			Kind: constraint.Kind,
			Index: &sqlbase.IndexDescriptor{
				Name: newName,
			},
		}
	}
	oldConstraintID := transformConstraint(tableID, constraint)
	newConstraintID := transformConstraint(tableID, newConstraint)
	_, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		ctx,
		"update-constraint-comment",
		p.Txn(),
		"UPDATE system.comments SET sub_id = $3 WHERE type= $1 AND object_id = $2 AND sub_id = $4",
		keys.ConstraintCommentType,
		tableID,
		newConstraintID,
		oldConstraintID)

	return err
}

func (p *planner) removeConstraintComment(
	ctx context.Context, tableID sqlbase.ID, constraint sqlbase.ConstraintDetail,
) error {
	constraintID := transformConstraint(tableID, constraint)
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx,
		"delete-constraint-comment",
		p.txn,
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.ConstraintCommentType,
		tableID,
		constraintID)

	return err
}

func (n *commentOnConstraintNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnConstraintNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnConstraintNode) Close(context.Context)        {}
