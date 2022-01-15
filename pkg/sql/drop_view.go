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

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type dropViewNode struct {
	n  *tree.DropView
	td []toDeleteTable
}

// DropView drops a view.
// Privileges: DROP on view.
//   Notes: postgres allows only the view owner to DROP a view.
//          mysql requires the DROP privilege on the view.
func (p *planner) DropView(ctx context.Context, n *tree.DropView) (planNode, error) {
	td := make([]toDeleteTable, 0, len(n.Names))
	for i := range n.Names {
		tn := &n.Names[i]
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, requireViewDesc)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			// IfExists specified and the view did not exist.
			continue
		}
		if err := checkViewMatchesMaterialized(*droppedDesc, true /* requireView */, n.IsMaterialized); err != nil {
			return nil, err
		}

		td = append(td, toDeleteTable{tn, droppedDesc})
	}

	// Ensure this view isn't depended on by any other views, or that if it is
	// then `cascade` was specified or it was also explicitly specified in the
	// DROP VIEW command.
	for _, toDel := range td {
		droppedDesc := toDel.desc
		for _, ref := range droppedDesc.DependedOnBy {
			// Don't verify that we can remove a dependent view if that dependent
			// view was explicitly specified in the DROP VIEW command.
			if descInSlice(ref.ID, td) {
				continue
			}
			if err := p.canRemoveDependentView(ctx, droppedDesc, ref, n.DropBehavior); err != nil {
				return nil, err
			}
		}
	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}
	return &dropViewNode{n: n, td: td}, nil
}

func (n *dropViewNode) startExec(params runParams) error {
	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		if droppedDesc == nil {
			continue
		}

		cascadeDroppedViews, err := params.p.dropViewImpl(ctx, droppedDesc, n.n.DropBehavior)
		if err != nil {
			return err
		}
		// Log a Drop View event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		params.p.curPlan.auditInfo = &server.AuditInfo{
			EventTime: timeutil.Now(),
			EventType: string(EventLogDropView),
			TargetInfo: &server.TargetInfo{
				TargetID: int32(droppedDesc.ID),
				Desc: struct {
					ViewName            string
					CascadeDroppedViews []string
				}{
					toDel.tn.FQString(),
					cascadeDroppedViews,
				},
			},
			Info: &infos.DropViewInfo{
				ViewName:            toDel.tn.FQString(),
				Statement:           n.n.String(),
				User:                params.SessionData().User,
				CascadeDroppedViews: cascadeDroppedViews,
			},
		}
	}

	return nil
}

func (*dropViewNode) Next(runParams) (bool, error) { return false, nil }
func (*dropViewNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropViewNode) Close(context.Context)        {}

func descInSlice(descID sqlbase.ID, td []toDeleteTable) bool {
	for _, toDel := range td {
		if descID == toDel.desc.ID {
			return true
		}
	}
	return false
}

func (p *planner) canRemoveDependentView(
	ctx context.Context,
	from *sqlbase.MutableTableDescriptor,
	ref sqlbase.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	return p.canRemoveDependentViewGeneric(ctx, from.TypeName(), from.Name, from.ParentID, ref, behavior)
}

func (p *planner) canRemoveDependentViewGeneric(
	ctx context.Context,
	typeName string,
	objName string,
	parentID sqlbase.ID,
	ref sqlbase.TableDescriptor_Reference,
	behavior tree.DropBehavior,
) error {
	viewDesc, err := p.getViewDescForCascade(ctx, typeName, objName, parentID, ref.ID, behavior)
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, viewDesc, privilege.DROP); err != nil {
		return err
	}
	// If this view is depended on by other views, we have to check them as well.
	for _, ref := range viewDesc.DependedOnBy {
		if err := p.canRemoveDependentView(ctx, viewDesc, ref, behavior); err != nil {
			return err
		}
	}
	return nil
}

// Drops the view and any additional views that depend on it.
// Returns the names of any additional views that were also dropped
// due to `cascade` behavior.
func (p *planner) removeDependentView(
	ctx context.Context, tableDesc, viewDesc *sqlbase.MutableTableDescriptor,
) ([]string, error) {
	// In the table whose index is being removed, filter out all back-references
	// that refer to the view that's being removed.
	tableDesc.DependedOnBy = removeMatchingReferences(tableDesc.DependedOnBy, viewDesc.ID)
	// Then proceed to actually drop the view and log an event for it.
	return p.dropViewImpl(ctx, viewDesc, tree.DropCascade)
}

// dropViewImpl does the work of dropping a view (and views that depend on it
// if `cascade is specified`). Returns the names of any additional views that
// were also dropped due to `cascade` behavior.
func (p *planner) dropViewImpl(
	ctx context.Context, viewDesc *sqlbase.MutableTableDescriptor, behavior tree.DropBehavior,
) ([]string, error) {
	var cascadeDroppedViews []string

	// Remove back-references from the tables/views this view depends on.
	for _, depID := range viewDesc.DependsOn {
		dependencyDesc, err := p.Tables().getMutableTableVersionByID(ctx, depID, p.txn)
		if err != nil {
			return cascadeDroppedViews,
				errors.Errorf("error resolving dependency relation ID %d: %v", depID, err)
		}
		// The dependency is also being deleted, so we don't have to remove the
		// references.
		if dependencyDesc.Dropped() {
			continue
		}
		dependencyDesc.DependedOnBy = removeMatchingReferences(dependencyDesc.DependedOnBy, viewDesc.ID)
		if err := p.writeSchemaChange(ctx, dependencyDesc, sqlbase.InvalidMutationID); err != nil {
			return cascadeDroppedViews, err
		}
	}
	viewDesc.DependsOn = nil

	if behavior == tree.DropCascade {
		for _, ref := range viewDesc.DependedOnBy {
			dependentDesc, err := p.getViewDescForCascade(
				ctx, viewDesc.TypeName(), viewDesc.Name, viewDesc.ParentID, ref.ID, behavior,
			)
			if err != nil {
				return cascadeDroppedViews, err
			}
			if dependentDesc.Dropped() {
				continue
			}
			cascadedViews, err := p.dropViewImpl(ctx, dependentDesc, behavior)
			if err != nil {
				return cascadeDroppedViews, err
			}
			cascadeDroppedViews = append(cascadeDroppedViews, cascadedViews...)
			cascadeDroppedViews = append(cascadeDroppedViews, dependentDesc.Name)
		}
	}
	if err := p.initiateDropTable(ctx, viewDesc, true /* drainName */); err != nil {
		return cascadeDroppedViews, err
	}

	return cascadeDroppedViews, nil
}

func (p *planner) dropViewImpl1(
	ctx context.Context, viewDesc *sqlbase.MutableTableDescriptor, behavior tree.DropBehavior,
) ([]string, *sqlbase.PrivilegeDescriptor, error) {
	var cascadeDroppedViews []string
	// Remove back-references from the tables/views this view depends on.
	for _, depID := range viewDesc.DependsOn {
		dependencyDesc, err := p.Tables().getMutableTableVersionByID(ctx, depID, p.txn)
		if err != nil {
			return cascadeDroppedViews, viewDesc.Privileges,
				errors.Errorf("error resolving dependency relation ID %d: %v", depID, err)
		}
		// The dependency is also being deleted, so we don't have to remove the
		// references.
		if dependencyDesc.Dropped() {
			continue
		}
		dependencyDesc.DependedOnBy = removeMatchingReferences(dependencyDesc.DependedOnBy, viewDesc.ID)
		if err := p.writeSchemaChange(ctx, dependencyDesc, sqlbase.InvalidMutationID); err != nil {
			return cascadeDroppedViews, viewDesc.Privileges, err
		}
	}
	viewDesc.DependsOn = nil

	if behavior == tree.DropCascade {
		for _, ref := range viewDesc.DependedOnBy {
			dependentDesc, err := p.getViewDescForCascade(
				ctx, viewDesc.TypeName(), viewDesc.Name, viewDesc.ParentID, ref.ID, behavior,
			)
			if err != nil {
				return cascadeDroppedViews, viewDesc.Privileges, err
			}
			cascadedViews, err := p.dropViewImpl(ctx, dependentDesc, behavior)
			if err != nil {
				return cascadeDroppedViews, viewDesc.Privileges, err
			}
			cascadeDroppedViews = append(cascadeDroppedViews, cascadedViews...)
			cascadeDroppedViews = append(cascadeDroppedViews, dependentDesc.Name)
		}
	}

	if err := p.initiateDropTable(ctx, viewDesc, false /* drainName */); err != nil {
		return cascadeDroppedViews, viewDesc.Privileges, err
	}

	return cascadeDroppedViews, viewDesc.Privileges, nil
}

func (p *planner) getViewDescForCascade(
	ctx context.Context,
	typeName string,
	objName string,
	parentID, viewID sqlbase.ID,
	behavior tree.DropBehavior,
) (*sqlbase.MutableTableDescriptor, error) {
	viewDesc, err := p.Tables().getMutableTableVersionByID(ctx, viewID, p.txn)
	if err != nil {
		log.Warningf(ctx, "unable to retrieve descriptor for view %d: %v", viewID, err)
		return nil, errors.Wrapf(err, "error resolving dependent view ID %d", viewID)
	}
	if behavior != tree.DropCascade {
		viewName := viewDesc.Name
		if viewDesc.ParentID != parentID {
			var err error
			viewName, err = p.getQualifiedTableName(ctx, viewDesc.TableDesc())
			if err != nil {
				log.Warningf(ctx, "unable to retrieve qualified name of view %d: %v", viewID, err)
				msg := fmt.Sprintf("cannot drop %s %q because a view depends on it", typeName, objName)
				return nil, sqlbase.NewDependentObjectError(msg)
			}
		}
		msg := fmt.Sprintf("cannot drop %s %q because view %q depends on it",
			typeName, objName, viewName)
		hint := fmt.Sprintf("you can drop %s instead.", viewName)
		return nil, sqlbase.NewDependentObjectErrorWithHint(msg, hint)
	}
	return viewDesc, nil
}

func (p *planner) getViewDescForAlterView(
	ctx context.Context, typeName string, objName string, parentID, viewID sqlbase.ID,
) error {
	viewDesc, err := p.Tables().getMutableTableVersionByID(ctx, viewID, p.txn)
	if err != nil {
		log.Warningf(ctx, "unable to retrieve descriptor for view %d: %v", viewID, err)
		return errors.Wrapf(err, "error resolving dependent view ID %d", viewID)
	}
	if viewDesc != nil {
		viewName := viewDesc.Name
		if viewDesc.ParentID != parentID {
			var err error
			viewName, err = p.getQualifiedTableName(ctx, viewDesc.TableDesc())
			if err != nil {
				log.Warningf(ctx, "unable to retrieve qualified name of view %d: %v", viewID, err)
				msg := fmt.Sprintf("cannot alter %s %q because a view depends on it", typeName, objName)
				return sqlbase.NewDependentObjectError(msg)
			}
		}
		msg := fmt.Sprintf("cannot alter %s %q because view %q depends on it",
			typeName, objName, viewName)
		hint := fmt.Sprintf("you can drop %s instead.", viewName)
		return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
	}
	return nil
}

func (p *planner) getViewDescForReplaceView(
	ctx context.Context, typeName string, objName string, parentID, viewID sqlbase.ID,
) error {
	viewDesc, err := p.Tables().getMutableTableVersionByID(ctx, viewID, p.txn)
	if err != nil {
		log.Warningf(ctx, "unable to retrieve descriptor for view %d: %v", viewID, err)
		return errors.Wrapf(err, "error resolving dependent view ID %d", viewID)
	}
	if viewDesc != nil {
		viewName := viewDesc.Name
		if viewDesc.ParentID != parentID {
			var err error
			viewName, err = p.getQualifiedTableName(ctx, viewDesc.TableDesc())
			if err != nil {
				log.Warningf(ctx, "unable to retrieve qualified name of view %d: %v", viewID, err)
				msg := fmt.Sprintf("cannot replace %s %q because a view depends on it", typeName, objName)
				return sqlbase.NewDependentObjectError(msg)
			}
		}
		msg := fmt.Sprintf("cannot replace %s %q because view %q depends on it",
			typeName, objName, viewName)
		hint := fmt.Sprintf("you can drop %s instead.", viewName)
		return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
	}
	return nil
}
