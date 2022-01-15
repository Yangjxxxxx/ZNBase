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
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/useroption"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// DropUserNode deletes entries from the system.users table.
// This is called from DROP USER and DROP ROLE.
type DropUserNode struct {
	ifExists bool
	isRole   bool
	names    func() ([]string, error)

	run dropUserRun
}

// DropUser drops a list of users.
// Privileges: DELETE on system.users.
func (p *planner) DropUser(ctx context.Context, n *tree.DropUser) (planNode, error) {
	return p.DropUserNode(ctx, n.Names, n.IfExists, false /* isRole */, "DROP USER")
}

// DropUserNode creates a "drop user" plan node. This can be called from DROP USER or DROP ROLE.
func (p *planner) DropUserNode(
	ctx context.Context, namesE tree.Exprs, ifExists bool, isRole bool, opName string,
) (*DropUserNode, error) {
	if err := p.CheckUserOption(ctx, useroption.CREATEUSER); err != nil {
		return nil, err
	}

	names, err := p.TypeAsStringArray(namesE, opName)
	if err != nil {
		return nil, err
	}

	return &DropUserNode{
		ifExists: ifExists,
		isRole:   isRole,
		names:    names,
	}, nil
}

// dropUserRun contains the run-time state of DropUserNode during local execution.
type dropUserRun struct {
	// The number of users deleted.
	numDeleted int
}

type objectAndType struct {
	ObjectType string
	ObjectName string
}

func (n *DropUserNode) startExec(params runParams) error {
	var entryType string
	if n.isRole {
		entryType = "role"
	} else {
		entryType = "user"
	}

	names, err := n.names()
	if err != nil {
		return err
	}

	// userNames maps user to objects they own.
	userNames := make(map[string][]objectAndType)
	for i, name := range names {
		normalizedUsername, err := NormalizeAndValidateUsername(name)
		if err != nil {
			return err
		}

		// Update the name in the names slice since we will re-use the name later.
		names[i] = normalizedUsername
		userNames[normalizedUsername] = make([]objectAndType, 0)
	}

	f := tree.NewFmtCtx(tree.FmtAlwaysQualifyTableNames)
	defer f.Close()

	// Now check whether the user still has permission on any object in the database or schema.

	// First check all the database.
	if err := forEachDatabaseDesc(params.ctx, params.p, nil /*nil prefix =  all databases*/, true,
		func(db *sqlbase.DatabaseDescriptor) error {
			if _, ok := userNames[db.GetPrivileges().Owner]; ok {
				userNames[db.GetPrivileges().Owner] = append(
					userNames[db.GetPrivileges().Owner],
					objectAndType{
						ObjectType: db.TypeName(),
						ObjectName: db.GetName(),
					})
			}

			for _, u := range db.GetPrivileges().Users {
				if _, ok := userNames[u.User]; ok {
					if f.Len() > 0 {
						f.WriteString(", ")
					}
					f.FormatNameP(&db.Name)
					break
				}
			}
			return nil
		}); err != nil {
		return err
	}

	// Then check all the schemas.
	descs, err := params.p.Tables().getAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}
	lCtx := newInternalLookupCtx(descs, nil /*prefix - we want all descriptors */)

	for _, scID := range lCtx.scIDs {
		schema := lCtx.scDescs[scID]
		if _, ok := userNames[schema.GetPrivileges().Owner]; ok {
			fullPath, err := schema.GetFullName(params.p.getSchemaFullNameFunc(params.ctx))
			if err != nil {
				return err
			}
			userNames[schema.GetPrivileges().Owner] = append(
				userNames[schema.GetPrivileges().Owner],
				objectAndType{
					ObjectType: schema.TypeName(),
					ObjectName: fullPath,
				})
		}
		for _, u := range schema.GetPrivileges().Users {
			if _, ok := userNames[u.User]; ok {
				if f.Len() > 0 {
					f.WriteString(", ")
				}
				dbDes, err := lCtx.getDatabaseByID(schema.ParentID)
				if err != nil {
					return err
				}
				dbName := tree.Name(dbDes.Name)
				scDes, err := lCtx.getSchemaByID(schema.ID)
				if err != nil {
					return err
				}
				scName := tree.Name(scDes.Name)
				scn := tree.MakeSchemaName(dbName, scName)
				f.FormatNode(&scn)
			}
		}
	}

	// check all the user define functions.
	for _, funcID := range lCtx.funcIDs {
		function := lCtx.funcDescs[funcID]
		if _, ok := userNames[function.GetPrivileges().Owner]; ok {
			fullPath, err := function.GetFullName(params.p.getFullNameFunc(params.ctx, function))
			if err != nil {
				return err
			}
			userNames[function.GetPrivileges().Owner] = append(
				userNames[function.GetPrivileges().Owner],
				objectAndType{
					ObjectType: function.TypeName(),
					ObjectName: fullPath,
				})
		}
		for _, u := range function.GetPrivileges().Users {
			if _, ok := userNames[u.User]; ok {
				if f.Len() > 0 {
					f.WriteString(", ")
				}
				dbName, scName := lCtx.getFuncParentName(function)
				fn := tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(function.FullFuncName))
				f.FormatNode(&fn)
			}
		}
	}

	// Then check all the tables.
	//
	// We need something like forEachTableAll here, however we can't use
	// the predefined forEachTableAll() function because we need to look
	// at all _visible_ descriptors, not just those on which the current
	// user has permission.
	for _, tbID := range lCtx.tbIDs {
		table := lCtx.tbDescs[tbID]
		if !tableIsVisible(table, true /*allowAdding*/) {
			continue
		}
		runflag, err := checkTableDependOnUser(userNames, table, params.p.getTableFullNameFunc(params.ctx))
		if err != nil {
			return err
		}
		if runflag {
			if f.Len() > 0 {
				f.WriteString(", ")
			}
			dbName, scName := lCtx.getParentName(table)
			tn := tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(table.Name))
			f.FormatNode(&tn)
		}

	}

	// Was there any object dependin on that user?
	if f.Len() > 0 {
		fnl := tree.NewFmtCtx(tree.FmtSimple)
		defer fnl.Close()
		for i, name := range names {
			if i > 0 {
				fnl.WriteString(", ")
			}
			fnl.FormatName(name)
		}
		return pgerror.NewErrorf(pgcode.Grouping,
			"cannot drop user%s or role%s %s: grants still exist on %s",
			util.Pluralize(int64(len(names))), util.Pluralize(int64(len(names))),
			fnl.String(), f.String(),
		)
	}

	for _, name := range names {
		// Did the user own any objects?
		ownedObjects := userNames[name]
		if len(ownedObjects) > 0 {
			objectsMsg := tree.NewFmtCtx(tree.FmtSimple)
			for _, obj := range ownedObjects {
				objectsMsg.WriteString(fmt.Sprintf("\nowner of %s %s", obj.ObjectType, obj.ObjectName))
			}
			objects := objectsMsg.CloseAndGetString()
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"role %s cannot be dropped because some objects depend on it%s",
				name, objects)
		}
	}

	// All safe - do the work.
	var numUsersDeleted, numRoleMembershipsDeleted int
	for normalizedUsername := range userNames {
		// Specifically reject special users and roles. Some (root, admin) would fail with
		// "privileges still exist" first.
		if normalizedUsername == sqlbase.AdminRole || normalizedUsername == sqlbase.PublicRole {
			return pgerror.NewErrorf(
				pgcode.InvalidParameterValue, "cannot drop special role %s", normalizedUsername)
		}
		if normalizedUsername == security.RootUser {
			return pgerror.NewErrorf(
				pgcode.InvalidParameterValue, "cannot drop special user %s", normalizedUsername)
		}

		rowsAffected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-user",
			params.p.txn,
			`DELETE FROM system.users WHERE username=$1 AND "isRole" = $2`,
			normalizedUsername,
			n.isRole,
		)
		if err != nil {
			return err
		}

		if rowsAffected == 0 && !n.ifExists {
			return errors.Errorf("%s %s does not exist", entryType, normalizedUsername)
		}
		numUsersDeleted += rowsAffected

		// Drop all role memberships involving the user/role.
		rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-role-membership",
			params.p.txn,
			`DELETE FROM system.role_members WHERE "role" = $1 OR "member" = $1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		_, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-user",
			params.p.txn,
			`DELETE FROM system.user_options WHERE username=$1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		numRoleMembershipsDeleted += rowsAffected
	}

	if numRoleMembershipsDeleted > 0 {
		// Some role memberships have been deleted, bump role_members table version to
		// force a refresh of role membership.
		if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
			return err
		}
	}

	n.run.numDeleted = numUsersDeleted

	p := params.p
	if p.semaCtx.InUDR {
		if ex, ok := p.extendedEvalCtx.TxnModesSetter.(*connExecutor); ok {
			if err := ex.resetExtraTxnState(params.ctx, ex.server.dbCache, noEvent); err != nil {
				log.Warningf(params.ctx, "error while cleaning up connExecutor in UDR drop %s: %s", entryType, err)
			}
		}
	}

	return nil
}

func checkTableDependOnUser(
	userNames map[string][]objectAndType,
	table *TableDescriptor,
	getFullNameFunc func(string, sqlbase.ID) (tree.NodeFormatter, error),
) (bool, error) {
	if _, ok := userNames[table.GetPrivileges().Owner]; ok {
		selfType := "table"
		if table.IsView() {
			selfType = "view"
		} else if table.IsSequence() {
			selfType = "sequence"
		}
		fullName, err := table.GetFullName(getFullNameFunc)
		if err != nil {
			return true, err
		}
		userNames[table.GetPrivileges().Owner] = append(
			userNames[table.GetPrivileges().Owner],
			objectAndType{
				ObjectType: selfType, //"table",
				ObjectName: fullName, //table.GetName(),
			})
	}

	for _, u := range table.Privileges.Users {
		if _, ok := userNames[u.User]; ok {
			return true, nil
		}
	}

	for _, col := range table.Columns {
		if col.Privileges != nil {
			for _, u := range col.GetPrivileges().Users {
				if _, ok := userNames[u.User]; ok {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// Next implements the planNode interface.
func (*DropUserNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*DropUserNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*DropUserNode) Close(context.Context) {}

// FastPathResults implements the planNodeFastPath interface.
func (n *DropUserNode) FastPathResults() (int, bool) { return n.run.numDeleted, true }
