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

	"github.com/gogo/protobuf/proto"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type alterIndexNode struct {
	n         *tree.AlterIndex
	tableDesc *sqlbase.MutableTableDescriptor
	indexDesc *sqlbase.IndexDescriptor
}

// AlterIndex applies a schema change on an index.
// Privileges: CREATE on table.
func (p *planner) AlterIndex(ctx context.Context, n *tree.AlterIndex) (planNode, error) {
	tableDesc, indexDesc, err := p.getTableAndIndex(ctx, nil, n.Index, privilege.REFERENCES)
	if err != nil {
		return nil, err
	}
	// As an artifact of finding the index by name, we get a pointer to a
	// different copy than the one in the tableDesc. To make it easier for the
	// code below, get a pointer to the index descriptor that's actually in
	// tableDesc.
	indexDesc, err = tableDesc.FindIndexByID(indexDesc.ID)
	if err != nil {
		return nil, err
	}
	return &alterIndexNode{n: n, tableDesc: tableDesc, indexDesc: indexDesc}, nil
}

func (n *alterIndexNode) startExec(params runParams) error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	useLocateIn := false
	var oldLocationNums, newLocationNums int32
	origNumMutations := len(n.tableDesc.Mutations)

	for _, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterIndexPartitionBy:
			if t.PartitionBy != nil && t.IsHash {
				return pgerror.NewError(pgcode.IndexHashPartition, "Can not append hash partition to index.")
			}
			if n.indexDesc.IsLocal { // 本地分区索引不能被显式重新分区
				return pgerror.NewError(pgcode.LocalIndexAlter,
					"Submitted alter index partition operation is not valid for local partitioned index")
			}
			partitioning, err := CreatePartitioning(
				params.ctx, params.extendedEvalCtx.Settings,
				params.EvalContext(),
				n.tableDesc, n.indexDesc, t.PartitionBy, params.StatusServer())
			if err != nil {
				return err
			}
			descriptorChanged = !proto.Equal(
				&n.indexDesc.Partitioning,
				&partitioning,
			)
			oldLocationNums = n.indexDesc.LocationNums
			newLocationNums = n.indexDesc.LocationNums -
				n.indexDesc.Partitioning.LocationNums + partitioning.LocationNums
			n.indexDesc.Partitioning = partitioning
			if oldLocationNums > 0 || newLocationNums > 0 {
				useLocateIn = true
			}
		case *tree.AlterIndexLocateIn:
			spaceName := t.LocateSpaceName.ToValue()
			if err := CheckLocateSpaceNameExistICL(params.ctx,
				spaceName, *params.StatusServer()); err != nil {
				return err
			}
			var changeValue int32
			descChange := false
			oldLocationNums = n.indexDesc.LocationNums
			if descChange, changeValue = changeLocationNums(spaceName,
				n.indexDesc.LocateSpaceName); descChange {
				newLocationNums = n.indexDesc.LocationNums + changeValue + 1
				if spaceName != nil {
					n.indexDesc.LocateSpaceName = spaceName
				}
			}
			if descChange {
				descriptorChanged = descChange
			}
			if oldLocationNums > 0 || newLocationNums > 0 {
				useLocateIn = true
			}
		default:
			return fmt.Errorf("unsupported alter command: %T", cmd)
		}
	}
	if descriptorChanged {
		if err := updateIndexLocationNums(n.tableDesc.TableDesc(),
			n.indexDesc, newLocationNums); err != nil {
			return err
		}
		if useLocateIn {
			if err := params.p.LocationMapChange(params.ctx, n.tableDesc,
				params.extendedEvalCtx.Tables.databaseCache.systemConfig); err != nil {
				return err
			}
		}
	}
	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	mutationID := sqlbase.InvalidMutationID
	var err error
	if addedMutations {
		mutationID, err = params.p.createOrUpdateSchemaChangeJob(params.ctx, n.tableDesc,
			tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType))
	} else if !descriptorChanged {
		// Nothing to be done
		return nil
	}
	if err != nil {
		return err
	}

	result := "OK"
	if err := params.p.writeSchemaChange(params.ctx, n.tableDesc, mutationID); err != nil {
		result = "ERROR"
	}

	// Record this index alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return params.extendedEvalCtx.ExecCfg.AuditServer.LogAudit(
		params.ctx,
		false,
		&server.AuditInfo{
			EventTime: timeutil.Now(),
			EventType: string(EventLogAlterIndex),
			//TargetID:     int32(n.tableDesc.ID),
			//Opt:          n.n.String(),
			//OptTime:      timeutil.SubTimes(timeutil.Now(), start),
			AffectedSize: 1,
			Result:       result,
			Info: struct {
				TableName  string
				IndexName  string
				Statement  string
				User       string
				MutationID uint32
			}{
				n.n.Index.Table.FQString(), n.indexDesc.Name, n.n.String(),
				params.SessionData().User, uint32(mutationID),
			},
		},
	)
}

func (n *alterIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexNode) Close(context.Context)        {}
