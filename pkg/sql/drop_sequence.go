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

	"github.com/pkg/errors"
	"github.com/znbasedb/errors/markers"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/errorutil/unimplemented"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

//ErrDescriptorDropped tips errors
var ErrDescriptorDropped = errors.New("descriptor is being dropped")

type dropSequenceNode struct {
	n  *tree.DropSequence
	td []toDeleteTable
}

func (p *planner) DropSequence(ctx context.Context, n *tree.DropSequence) (planNode, error) {
	td := make([]toDeleteTable, 0, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, requireSequenceDesc)
		if droppedDesc != nil {
			p.sessionDataMutator.data.SequenceState.RemoveLastAndCacheVal(uint32(droppedDesc.ID))
		}
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			// IfExists specified and descriptor does not exist.
			continue
		}

		if depErr := p.sequenceDependencyError(ctx, droppedDesc); depErr != nil {
			return nil, depErr
		}

		if err := p.CheckPrivilege(ctx, droppedDesc, privilege.DROP); err != nil {
			return nil, err
		}

		td = append(td, toDeleteTable{tn, droppedDesc})
	}
	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}

	return &dropSequenceNode{
		n:  n,
		td: td,
	}, nil
}

func (n *dropSequenceNode) startExec(params runParams) error {
	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		err := params.p.dropSequenceImpl(ctx, droppedDesc, n.n.DropBehavior)
		if err != nil {
			return err
		}
		// Log a Drop Sequence event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		params.p.curPlan.auditInfo = &server.AuditInfo{
			EventTime: timeutil.Now(),
			EventType: string(EventLogDropSequence),
			TargetInfo: &server.TargetInfo{
				TargetID: int32(droppedDesc.ID),
				Desc: struct {
					SequenceName string
				}{
					toDel.tn.FQString(),
				},
			},
			Info: &infos.DropSequenceInfo{
				SequenceName: toDel.tn.FQString(),
				Statement:    n.n.String(),
				User:         params.SessionData().User,
			},
		}
	}

	return nil
}

func (*dropSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (*dropSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropSequenceNode) Close(context.Context)        {}

func (p *planner) dropSequenceImpl(
	ctx context.Context, seqDesc *sqlbase.MutableTableDescriptor, behavior tree.DropBehavior,
) error {
	return p.initiateDropTable(ctx, seqDesc, true /* drainName */)
}

// sequenceDependency error returns an error if the given sequence cannot be dropped because
// a table uses it in a DEFAULT expression on one of its columns, or nil if there is no
// such dependency.
func (p *planner) sequenceDependencyError(
	ctx context.Context, droppedDesc *sqlbase.MutableTableDescriptor,
) error {
	if len(droppedDesc.DependedOnBy) > 0 {
		return pgerror.NewErrorf(
			pgcode.DependentObjectsStillExist,
			"cannot drop sequence %s because other objects depend on it",
			droppedDesc.Name,
		)
	}
	return nil
}

func (p *planner) canRemoveAllTableOwnedSequences(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, behavior tree.DropBehavior,
) error {
	for _, col := range desc.Columns {
		err := p.canRemoveOwnedSequencesImpl(ctx, desc, &col, behavior, false /* isColumnDrop */)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) canRemoveAllColumnOwnedSequences(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	behavior tree.DropBehavior,
) error {
	return p.canRemoveOwnedSequencesImpl(ctx, desc, col, behavior, true /* isColumnDrop */)
}

func (p *planner) canRemoveOwnedSequencesImpl(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	behavior tree.DropBehavior,
	isColumnDrop bool,
) error {
	for _, sequenceID := range col.OwnsSequenceIds {
		seqLookup, err := p.LookupTableByID(ctx, sequenceID)
		if err != nil {

			// Special case error swallowing for #50711 and #50781, which can cause a
			// column to own sequences that have been dropped/do not exist.
			///var ErrDescriptorDropped = errors.New("descriptor is being dropped")
			if markers.Is(err, ErrDescriptorDropped) ||
				pgerror.GetPGCode(err) == pgcode.UndefinedTable {
				log.Eventf(ctx, "swallowing error during sequence ownership unlinking: %s", err.Error())
				return nil
			}
			return err
		}
		seqDesc := seqLookup.Desc
		affectsNoColumns := len(seqDesc.DependedOnBy) == 0
		// It is okay if the sequence is depended on by columns that are being
		// dropped in the same transaction
		canBeSafelyRemoved := len(seqDesc.DependedOnBy) == 1 && seqDesc.DependedOnBy[0].ID == desc.ID
		// If only the column is being dropped, no other columns of the table can
		// depend on that sequence either
		if isColumnDrop {
			canBeSafelyRemoved = canBeSafelyRemoved && len(seqDesc.DependedOnBy[0].ColumnIDs) == 1 &&
				seqDesc.DependedOnBy[0].ColumnIDs[0] == col.ID
		}

		canRemove := affectsNoColumns || canBeSafelyRemoved

		// Once Drop Sequence Cascade actually respects the drop behavior, this
		// check should go away.
		if behavior == tree.DropCascade && !canRemove {
			return unimplemented.NewWithIssue(20965, "DROP SEQUENCE CASCADE is currently unimplemented")
		}
		// If Cascade is not enabled, and more than 1 columns depend on it, and the
		if behavior != tree.DropCascade && !canRemove {
			return pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop table %s because other objects depend on it",
				desc.Name,
			)
		}
	}
	return nil
}
