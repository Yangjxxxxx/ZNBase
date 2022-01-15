// Copyright 2017 The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package roleicl

import (
	"context"

	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

func checkPermissions(
	ctx context.Context, p sql.PlanHookState, Roles tree.NameList,
) (sql.PlanNode, error) {
	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	// check permissions on each role.
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return nil, err
	}

	for i := range Roles {
		r := string(Roles[i])
		// If the user is an admin, don't check if the user is allowed to add/drop
		// roles in the role. However, if the role being modified is the admin role, then
		// make sure the user is an admin with the admin option.
		if hasAdminRole && r != sqlbase.AdminRole {
			continue
		}
		if isAdmin, ok := allRoles[r]; !ok || !isAdmin {
			if r == sqlbase.AdminRole {
				return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
					"%s is not a role admin for role %s", p.User(), r)
			}
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
				"%s is not a superuser or role admin for role %s", p.User(), r)
		}
	}
	return nil, nil
}

func checkExist(
	ctx context.Context, p sql.PlanHookState, Roles tree.NameList, Members tree.NameList,
) (sql.PlanNode, error) {
	// Check that users and roles exist.
	// TODO(mberhault): just like GRANT/REVOKE privileges, we fetch the list of all users.
	// This is wasteful when we have a LOT of users compared to the number of users being operated on.
	users, err := p.GetAllUsersAndRoles(ctx)
	if err != nil {
		return nil, err
	}

	// NOTE: membership manipulation involving the "public" pseudo-role fails with
	// "role public does not exist". This matches postgres behavior.

	// Check roles: these have to be roles.
	for _, r := range Roles {
		if isRole, ok := users[string(r)]; !ok || !isRole {
			if string(r) == sqlbase.PublicRole {
				return nil, pgerror.NewErrorf(pgcode.UndefinedObject, " can't revoke or grant role %s, all users belong to it ", r)
			}
			return nil, pgerror.NewErrorf(pgcode.UndefinedObject, "role %s does not exist", r)
		}
	}

	// Check grantees/members: these can be users or roles.
	for _, m := range Members {
		if _, ok := users[string(m)]; !ok {
			return nil, pgerror.NewErrorf(pgcode.UndefinedObject, "user or role %s does not exist", m)
		}
	}
	return nil, nil
}

func makeCreateRolePlan(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	createRole, ok := stmt.(*tree.CreateRole)
	if !ok {
		return nil, nil
	}

	// Call directly into the OSS code.
	return p.CreateUserNode(ctx, createRole.Name, createRole.IfNotExists, true /* isRole */, "CREATE ROLE", nil)
}

func makeDropRolePlan(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	dropRole, ok := stmt.(*tree.DropRole)
	if !ok {
		return nil, nil
	}

	// Call directly into the OSS code.
	return p.DropUserNode(ctx, dropRole.Names, dropRole.IfExists, true /* isRole */, "DROP ROLE")
}

func makeGrantRolePlan(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	grant, ok := stmt.(*tree.GrantRole)
	if !ok {
		return nil, nil
	}

	if errNode, err := checkPermissions(ctx, p, grant.Roles); err != nil {
		return errNode, err
	}

	if errNode, err := checkExist(ctx, p, grant.Roles, grant.Members); err != nil {
		return errNode, err
	}

	return p.GrantRole(ctx, grant)
}

func makeRevokeRolePlan(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	revoke, ok := stmt.(*tree.RevokeRole)
	if !ok {
		return nil, nil
	}

	if errNode, err := checkPermissions(ctx, p, revoke.Roles); err != nil {
		return errNode, err
	}

	if errNode, err := checkExist(ctx, p, revoke.Roles, revoke.Members); err != nil {
		return errNode, err
	}

	return p.RevokeRole(ctx, revoke)
}

func init() {
	sql.AddWrappedPlanHook(makeCreateRolePlan)
	sql.AddWrappedPlanHook(makeDropRolePlan)
	sql.AddWrappedPlanHook(makeGrantRolePlan)
	sql.AddWrappedPlanHook(makeRevokeRolePlan)
}
