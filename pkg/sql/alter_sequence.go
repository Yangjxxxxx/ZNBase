// Copyright 2015  The Cockroach Authors.
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
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type alterSequenceNode struct {
	n        *tree.AlterSequence
	seqDesc  *sqlbase.MutableTableDescriptor
	isCalled bool
}

// AlterSequence transforms a tree.AlterSequence into a plan node.
func (p *planner) AlterSequence(ctx context.Context, n *tree.AlterSequence) (planNode, error) {
	seqDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Name, !n.IfExists, requireSequenceDesc)
	seqOpts, err := ResolveExistingObject(ctx, p, &n.Name, true /*required*/, requireSequenceDesc)
	var isCalled bool
	if seqOpts != nil {
		isCalled = seqOpts.SequenceOpts.IsCall
	}
	if err != nil {
		return nil, err
	}
	if seqDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	dbDesc, err := p.ResolveUncachedDatabase(ctx, &n.Name)
	if err != nil {
		return nil, err
	}

	scDesc, err := dbDesc.GetSchemaByName(string(n.Name.SchemaName))

	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, seqDesc, privilege.DROP); err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, scDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	return &alterSequenceNode{n: n, seqDesc: seqDesc, isCalled: isCalled}, nil
}

func (n *alterSequenceNode) startExec(params runParams) error {
	desc := n.seqDesc
	seqValueKey := keys.MakeSequenceKey(uint32(desc.ID))
	res, errG := params.p.txn.Get(params.ctx, seqValueKey)
	if errG != nil {
		return errG
	}
	currentVal := res.ValueInt()
	// check whether seq has been increased. if seqNeedReset == true, seq has to be reset before altering.
	seqNeedReset := false

	if !n.isCalled {
		if desc.SequenceOpts.Increment > 0 {
			if currentVal == desc.SequenceOpts.Start-desc.SequenceOpts.Increment {
				seqNeedReset = currentVal < desc.SequenceOpts.Start
			}
		} else {
			if currentVal == desc.SequenceOpts.Start-desc.SequenceOpts.Increment {
				seqNeedReset = currentVal > desc.SequenceOpts.Start
			}
		}
	}
	if seqNeedReset {
		// seq hasn't been increased after creating. reset it
		_, err := params.p.txn.Inc(params.ctx, seqValueKey, desc.SequenceOpts.Increment-desc.SequenceOpts.Start)
		if err != nil {
			return err
		}
	}
	seqID := uint32(desc.TableDescriptor.ID)
	oldIncrement := desc.SequenceOpts.Increment
	oldMaxvalue := desc.SequenceOpts.MaxValue
	oldMinvalue := desc.SequenceOpts.MinValue
	oldStartVal := desc.SequenceOpts.Start
	isAlter := true
	//err := assignSequenceOptions(desc.SequenceOpts, n.n.Options, false /* setDefaults */, isAlter)
	err := assignSequenceOptions(
		desc.SequenceOpts, n.n.Options, false /* setDefaults */, isAlter, &params, desc.GetID(), desc.ParentID,
	)
	if err != nil {
		return err
	}

	// pg:可选的子句START WITH更改该序列被记录的开始值。 这对于当前序列值没有影响，它会简单地设置。
	// 未来ALTER SEQUENCE RESTART命令将会使用的值。
	// znbase not support 'RESTART WITH' (TODO later)
	if !seqNeedReset {
		// Append current value to start value if sequence has been changed.
		desc.SequenceOpts.Start = currentVal
		if desc.SequenceOpts.Start > desc.SequenceOpts.MaxValue {
			return pgerror.NewErrorf(
				pgcode.InvalidParameterValue,
				"START value (%d) cannot be greater than MAXVALUE (%d)", desc.SequenceOpts.Start, desc.SequenceOpts.MaxValue)
		}
		if desc.SequenceOpts.Start < desc.SequenceOpts.MinValue {
			return pgerror.NewErrorf(
				pgcode.InvalidParameterValue,
				"START value (%d) cannot be less than MINVALUE (%d)", desc.SequenceOpts.Start, desc.SequenceOpts.MinValue)
		}
	} else {
		// 'START WITH' is inoperative in 'ALTER SEQUENCE'. Turn it off for the moment.
		desc.SequenceOpts.Start = oldStartVal
	}

	if seqNeedReset {
		// same code as create_sequence
		_, err = params.p.txn.Inc(params.ctx, seqValueKey, desc.SequenceOpts.Start-desc.SequenceOpts.Increment)
		if err != nil {
			return err
		}
	}
	if desc.SequenceOpts.Cache <= 1 {
		params.p.sessionDataMutator.data.SequenceState.RemoveCacheval(seqID)
	}
	if params.p.sessionDataMutator.data.SequenceState.ReturnCacheValsCount(seqID) > 0 {
		params.p.sessionDataMutator.data.SequenceState.SetCacheValslast(seqID, oldIncrement, oldMaxvalue, oldMinvalue)
	}
	/*	if oldStartVal != desc.SequenceOpts.Start {
		desc.SequenceOpts.IsRestart = true
	}*/
	if err := params.p.writeSchemaChange(params.ctx, n.seqDesc, sqlbase.InvalidMutationID); err != nil {
		return err
	}
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogAlterSequence),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(n.seqDesc.ID),
			Desc: struct {
				SequenceName string
			}{
				n.n.Name.FQString(),
			},
		},
		Info: &infos.AlterSequenceInfo{
			SequenceName: n.n.Name.FQString(),
			Statement:    n.n.String(),
			User:         params.SessionData().User,
		},
	}
	return nil
}

func (n *alterSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterSequenceNode) Close(context.Context)        {}
