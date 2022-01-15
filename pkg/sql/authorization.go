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
	"runtime/debug"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/security/useroption"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
)

type membershipCache struct {
	syncutil.Mutex
	tableVersion sqlbase.DescriptorVersion
	// userCache is a mapping from username to userRoleMembership.
	userCache map[string]userRoleMembership
}

var roleMembersCache membershipCache

// userRoleMembership is a mapping of "rolename" -> "with admin option".
type userRoleMembership map[string]bool

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that current user has `privilege` on `descriptor`.
	CheckPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind) error

	// CheckAnyPrivilege returns nil if user has any privileges at all.
	CheckAnyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error

	// UserHasAdminRole returns tuple of bool and error:
	// (true, nil) means that the user has an admin role (i.e. root or node)
	// (false, nil) means that the user has NO admin role
	// (false, err) means that there was an error running the query on
	// the `system.users` table
	UserHasAdminRole(ctx context.Context, user string) (bool, error)

	// HasAdminRole checks if the current session's user has admin role.
	HasAdminRole(ctx context.Context) (bool, error)

	// RequireAdminRole is a wrapper on top of HasAdminRole.
	// It errors if HasAdminRole errors or if the user isn't a super-user.
	// Includes the named action in the error message.
	RequireAdminRole(ctx context.Context, action string) error

	// MemberOfWithAdminOption looks up all the roles (direct and indirect) that 'member' is a member
	// of and returns a map of role -> isAdmin.
	MemberOfWithAdminOption(ctx context.Context, member string) (map[string]bool, error)
}

var _ AuthorizationAccessor = &planner{}

func (p *planner) getFullNameFunc(
	ctx context.Context, descriptor sqlbase.DescriptorProto,
) func(
	name string,
	parentID sqlbase.ID,
) (tree.NodeFormatter, error) {
	switch descriptor.TypeName() {
	case "schema":
		return p.getSchemaFullNameFunc(ctx)
	case "relation", "function", "procedure":
		return p.getTableFullNameFunc(ctx)
	case "column":
		return p.getColumnFullNameFunc(ctx)
	default:
		return func(_ string, _ sqlbase.ID) (tree.NodeFormatter, error) {
			return nil, fmt.Errorf("invalid object type: %T", descriptor)
		}
	}
}

func (p *planner) getSchemaFullNameFunc(
	ctx context.Context,
) func(
	name string,
	parentID sqlbase.ID,
) (tree.NodeFormatter, error) {
	return func(name string, parentID sqlbase.ID) (tree.NodeFormatter, error) {
		dbDesc, err := p.Tables().databaseCache.getDatabaseDescByID(ctx, p.txn, parentID)
		if err != nil {
			return nil, err
		}
		scName := tree.MakeSchemaName(tree.Name(dbDesc.Name), tree.Name(name))
		return &scName, nil
	}
}

func (p *planner) getTableFullNameFunc(
	ctx context.Context,
) func(
	name string,
	parentID sqlbase.ID,
) (tree.NodeFormatter, error) {
	return func(name string, parentID sqlbase.ID) (tree.NodeFormatter, error) {
		if parentID == sqlbase.SystemDB.ID {
			tbName := tree.MakeTableNameWithSchema(
				tree.Name("system"),
				tree.Name("public"),
				tree.Name(name),
			)
			return &tbName, nil
		}
		if parentID == sqlbase.InvalidID {
			// virtualSchema
			return (*tree.Name)(&name), nil
		}
		scDesc, err := p.Tables().schemaCache.getSchemaDescByID(ctx, p.txn, parentID)
		if err != nil {
			return nil, err
		}
		dbDesc, err := p.Tables().databaseCache.getDatabaseDescByID(ctx, p.txn, scDesc.ParentID)
		if err != nil {
			return nil, err
		}
		tbName := tree.MakeTableNameWithSchema(
			tree.Name(dbDesc.Name),
			tree.Name(scDesc.Name),
			tree.Name(name),
		)
		return &tbName, nil
	}
}

func (p *planner) getColumnFullNameFunc(
	ctx context.Context,
) func(
	name string,
	parentID sqlbase.ID,
) (tree.NodeFormatter, error) {
	return func(name string, parentID sqlbase.ID) (tree.NodeFormatter, error) {
		tblDesc, err := sqlbase.GetTableDescFromID(p.extendedEvalCtx.Ctx(), p.Txn(), parentID)
		if err != nil {
			return nil, err
		}
		if tblDesc.ParentID == sqlbase.SystemDB.ID {
			colName := tree.MakeColNameWithTable(
				tree.Name("system"),
				tree.Name("public"),
				tree.Name(tblDesc.Name),
				tree.Name(name),
			)
			return &colName, nil
		}
		if tblDesc.ParentID == sqlbase.InvalidID {
			// virtualSchema
			return (*tree.Name)(&name), nil
		}
		scDesc, err := p.Tables().schemaCache.getSchemaDescByID(ctx, p.txn, tblDesc.ParentID)
		if err != nil {
			return nil, err
		}
		dbDesc, err := p.Tables().databaseCache.getDatabaseDescByID(ctx, p.txn, scDesc.ParentID)
		if err != nil {
			return nil, err
		}
		colName := tree.MakeColNameWithTable(
			tree.Name(dbDesc.Name),
			tree.Name(scDesc.Name),
			tree.Name(tblDesc.Name),
			tree.Name(name),
		)
		return &colName, nil
	}
}

// CheckPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckPrivilege(
	ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
) error {
	return p.checkPrivilege(ctx, descriptor, privilege, p.User(), false)
}

// checkPrivilegeForUser used for optimizer.
func (p *planner) checkPrivilegeForUser(
	ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind, user string,
) error {
	return p.checkPrivilege(ctx, descriptor, privilege, user, false)
}

// checkPrivilege used for grant and revoke.
func (p *planner) checkPrivilege(
	ctx context.Context,
	descriptor sqlbase.DescriptorProto,
	priv privilege.Kind,
	user string,
	grantAble bool,
) error {
	// Test whether the object is being audited, and if so, record an
	// audit event. We place this check here to increase the likelihood
	// it will not be forgotten if features are added that access
	// descriptors (since every use of descriptors presumably need a
	// permission check).
	p.maybeAudit(descriptor, priv)
	privs := descriptor.GetPrivileges()

	// Check if the 'public' pseudo-role has privileges.
	if privs.CheckPrivilege(sqlbase.PublicRole, priv, grantAble) {
		return nil
	}

	// Check if 'user' itself or any role the 'user' is a member of, who has privileges
	// or is owner of the object.
	hasPriv, err := p.checkRolePredicate(ctx, user, func(role string) bool {
		return IsOwner(descriptor, role) || privs.CheckPrivilege(role, priv, grantAble)
	})
	if err != nil {
		return err
	}
	if hasPriv {
		return nil
	}

	fullName, err := descriptor.GetFullName(p.getFullNameFunc(ctx, descriptor))
	if err != nil {
		return err
	}
	return pgerror.NewErrorf(pgcode.InsufficientPrivilege, "user %s does not have %s privilege on %s %s",
		user, priv.String(), descriptor.TypeName(), fullName)
}

func getOwnerOfDesc(desc sqlbase.DescriptorProto) string {
	owner := desc.GetPrivileges().Owner
	if len(owner) == 0 { // todo(xz): refactor to owner.Undefined()
		// If the descriptor is ownerless and the descriptor is part of the system db,
		// node is the owner.
		if desc.GetID() == keys.SystemDatabaseID { // todo(xz): GetParentID()
			owner = security.NodeUser
		} else {
			// This check is redundant in this case since admin already has privilege
			// on all non-system objects.
			owner = sqlbase.AdminRole
		}
	}
	return owner
}

// IsOwner returns if the role has ownership on the descriptor.
func IsOwner(desc sqlbase.DescriptorProto, role string) bool {
	return role == getOwnerOfDesc(desc)
}

// HasOwnership returns if the role or any role the role is a member of
// has ownership privilege of the desc.
func (p *planner) HasOwnership(
	ctx context.Context, descriptor sqlbase.DescriptorProto,
) (bool, error) {
	user := p.User()

	return p.checkRolePredicate(ctx, user, func(role string) bool {
		return IsOwner(descriptor, role)
	})
}

// checkRolePredicate checks if the predicate is true for the user or
// any roles the user is a member of.
func (p *planner) checkRolePredicate(
	ctx context.Context, user string, predicate func(role string) bool,
) (bool, error) {
	if ok := predicate(user); ok {
		return true, nil
	}
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return false, err
	}
	for role := range memberOf {
		if ok := predicate(role); ok {
			return true, nil
		}
	}
	return false, nil
}

// CheckAnyColumnPrivilege is used to check whether user can use table which only has columns privilege.
func (p *planner) CheckAnyColumnPrivilege(
	ctx context.Context, descriptor sqlbase.DescriptorProto,
) error {
	if err := p.CheckAnyPrivilege(ctx, descriptor); err != nil {
		var columns []sqlbase.ColumnDescriptor
		switch t := descriptor.(type) {
		case *sqlbase.ImmutableTableDescriptor:
			columns = t.Columns
		case *sqlbase.TableDescriptor:
			columns = t.Columns
		}
		for i := range columns {
			if columns[i].Privileges != nil {
				if err := p.CheckAnyPrivilege(ctx, &columns[i]); err == nil {
					return nil
				}
			}
		}
		return err
	}

	return nil
}

// CheckAnyPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckAnyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error {
	user := p.SessionData().User
	privs := descriptor.GetPrivileges()

	if privs == nil {
		full, err := descriptor.GetFullName(p.getFullNameFunc(ctx, descriptor))
		if err != nil {
			return err
		}

		log.Errorf(ctx, "checkAnyPrivilege privs nil, SQL: %s, User: %s\n", p.stmt.SQL, p.User())
		log.Errorf(ctx, "full name: %s", full)
		log.Errorf(ctx, string(debug.Stack()))

		if user == security.RootUser {
			return nil
		}

		memberOf, err := p.MemberOfWithAdminOption(ctx, user)
		if err != nil {
			return err
		}
		if ok, _ := memberOf[sqlbase.AdminRole]; ok {
			return nil
		}

	} else {
		// Check if 'user' itself has privileges.
		if privs.AnyPrivilege(user) {
			return nil
		}

		// Check if 'public' has privileges.
		if privs.AnyPrivilege(sqlbase.PublicRole) {
			return nil
		}

		// Expand role memberships.
		memberOf, err := p.MemberOfWithAdminOption(ctx, user)
		if err != nil {
			return err
		}

		// Iterate over the roles that 'user' is a member of. We don't care about the admin option.
		for role := range memberOf {
			if privs.AnyPrivilege(role) {
				return nil
			}
		}
	}

	fullName, err := descriptor.GetFullName(p.getFullNameFunc(ctx, descriptor))
	if err != nil {
		return err
	}

	return pgerror.NewErrorf(pgcode.InsufficientPrivilege, "user %s has no privileges on %s %s",
		p.SessionData().User, descriptor.TypeName(), fullName)
}

// UserHasAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) UserHasAdminRole(ctx context.Context, user string) (bool, error) {
	if user == "" {
		return false, errors.AssertionFailedf("empty user")
	}
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return false, errors.AssertionFailedf("cannot use HasAdminRole without a txn")
	}

	// Check if user is 'root' or 'node'.
	// TODO(knz): planner HasAdminRole has no business authorizing
	// the "node" principal - node should not be issuing SQL queries.
	if user == security.RootUser || user == security.NodeUser {
		return true, nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return false, err
	}

	// Check is 'user' is a member of role 'admin'.
	if _, ok := memberOf[security.AdminRole]; ok {
		return true, nil
	}

	return false, nil
}

// HasAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) HasAdminRole(ctx context.Context) (bool, error) {
	return p.UserHasAdminRole(ctx, p.User())
}

// RequireAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) RequireAdminRole(ctx context.Context, action string) error {
	ok, err := p.HasAdminRole(ctx)

	if err != nil {
		return err
	}
	if !ok {
		//raise error if user is not a super-user
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to %s", action)
	}
	return nil
}

// MemberOfWithAdminOption looks up all the roles 'member' belongs to (direct and indirect) and
// returns a map of "role" -> "isAdmin".
// The "isAdmin" flag applies to both direct and indirect members.
func (p *planner) MemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	// Lookup table version.
	objDesc, err := p.PhysicalSchemaAccessor().GetObjectDesc(ctx, p.txn, &roleMembersTableName,
		p.ObjectLookupFlags(true /*required*/, false /*requireMutable*/))
	if err != nil {
		return nil, err
	}
	tableDesc := objDesc.TableDesc()
	tableVersion := tableDesc.Version

	// We loop in case the table version changes while we're looking up memberships.
	for {
		// Check version and maybe clear cache while holding the mutex.
		// We release the lock here instead of using defer as we need to keep
		// going and re-lock if adding the looked-up entry.
		roleMembersCache.Lock()
		if roleMembersCache.tableVersion != tableVersion {
			// Update version and drop the map.
			roleMembersCache.tableVersion = tableDesc.Version
			roleMembersCache.userCache = make(map[string]userRoleMembership)
		}

		userMapping, ok := roleMembersCache.userCache[member]
		roleMembersCache.Unlock()

		if ok {
			// Found: return.
			return userMapping, nil
		}

		// Lookup memberships outside the lock.
		memberships, err := p.resolveMemberOfWithAdminOption(ctx, member)
		if err != nil {
			return nil, err
		}

		// Update membership.
		roleMembersCache.Lock()
		if roleMembersCache.tableVersion != tableVersion {
			// Table version has changed while we were looking, unlock and start over.
			tableVersion = roleMembersCache.tableVersion
			roleMembersCache.Unlock()
			continue
		}

		// Table version remains the same: update map, unlock, return.
		roleMembersCache.userCache[member] = memberships
		roleMembersCache.Unlock()

		return memberships, nil
	}
}

// resolveMemberOfWithAdminOption performs the actual recursive role membership lookup.
// TODO(mberhault): this is the naive way and performs a full lookup for each user,
// we could save detailed memberships (as opposed to fully expanded) and reuse them
// across users. We may then want to lookup more than just this user.
func (p *planner) resolveMemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	ret := map[string]bool{}

	// Keep track of members we looked up.
	visited := map[string]struct{}{}
	toVisit := []string{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		rows, err := p.ExecCfg().InternalExecutor.Query(
			ctx, "expand-roles", nil /* txn */, lookupRolesStmt, m,
		)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			roleName := tree.MustBeDString(row[0])
			isAdmin := row[1].(*tree.DBool)

			ret[string(roleName)] = bool(*isAdmin)

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, string(roleName))
		}
	}

	return ret, nil
}

func (p planner) checkPrivilegeAccessToDataSource(
	ctx context.Context, table *cat.DataSourceName,
) error {
	if CheckVirtualSchema(table.Schema()) {
		return nil
	}

	database, err := p.LogicalSchemaAccessor().GetDatabaseDesc(ctx, p.Txn(), table.Catalog(), p.CommonLookupFlags(true))
	if err != nil {
		return err
	}
	if err = p.CheckPrivilege(ctx, database, privilege.USAGE); err != nil {
		return sqlbase.NewNoAccessDBPrivilegeError(p.User(), table.Catalog())
	}
	// Because system database does not have any schema, to skip the schema privilege check.
	if database.ID == sqlbase.SystemDB.ID {
		return nil
	}

	schema, getScNameErr := database.GetSchemaByName(table.Schema())
	if getScNameErr != nil {
		// when create a view on a created temp table, if the operation
		// time is close enough, the temp schema has not write to database
		// descriptor yet, the GetSchemaByName will only find public schema
		// in database descriptor, and will not get any schema descriptor,
		// this will cause an about 3/1000 chance error
		// in this situation, we just judge the session id
		tempSchemaNameThisSession := temporarySchemaName(p.ExtendedEvalContext().SessionID)
		if table.Schema() == tempSchemaNameThisSession {
			return nil
		}
		return getScNameErr
	}
	if err = p.CheckPrivilege(ctx, schema, privilege.USAGE); err != nil {
		return sqlbase.NewNoAccessSchemaPrivilegeError(p.User(), table.Catalog(), table.Schema())
	}

	return nil
}

// checkPrivilegeAccessToTable checks if the table's schema and database usage privilege.
func (p *planner) checkPrivilegeAccessToTable(
	ctx context.Context, object sqlbase.DescriptorProto,
) error {
	var parentID sqlbase.ID
	switch d := object.(type) {
	case *FunctionDescriptor:
		parentID = d.ParentID
	case *ImmutableTableDescriptor:
		parentID = d.ParentID
	default:
		return pgerror.NewAssertionErrorf("invalid object type: %T", d)
	}

	if parentID == sqlbase.InvalidID {
		// 没有schema 认为有访问权限
		return nil
	}
	if parentID == sqlbase.SystemDB.ID {
		dbDesc, err := p.Tables().databaseCache.getDatabaseDescByID(ctx, p.Txn(), parentID)
		if err != nil {
			return err
		}
		if err := p.CheckPrivilege(ctx, dbDesc, privilege.USAGE); err != nil {
			return sqlbase.NewNoAccessDBPrivilegeError(p.User(), dbDesc.Name)
		}
		return nil
	}

	scDesc, err := p.Tables().schemaCache.getSchemaDescByID(ctx, p.Txn(), parentID)
	if err != nil {
		return err
	}

	dbDesc, err := p.Tables().databaseCache.getDatabaseDescByID(ctx, p.Txn(), scDesc.ParentID)
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, dbDesc, privilege.USAGE); err != nil {
		return sqlbase.NewNoAccessDBPrivilegeError(p.User(), dbDesc.Name)
	}
	if err := p.CheckPrivilege(ctx, scDesc, privilege.USAGE); err != nil {
		return sqlbase.NewNoAccessSchemaPrivilegeError(p.User(), dbDesc.Name, scDesc.Name)
	}

	return nil
}

// CheckDMLPrivilege check if the user has DML or DQL operation privilege type
// (including SELECT, INSERT, UPDATE, DELETE).
// If cols is nil, it will be treated as all columns of object.
// If user is an empty string, then check the current user.
func (p *planner) CheckDMLPrivilege(
	ctx context.Context,
	descriptor *sqlbase.TableDescriptor,
	cols tree.NameList,
	priv privilege.Kind,
	user string,
) error {
	if len(user) == 0 {
		user = p.User()
	}
	if terr := p.checkPrivilegeForUser(ctx, descriptor, priv, user); terr != nil {
		// do not check column privileges on virtualSchemaTable
		if descriptor.ParentID == sqlbase.InvalidID {
			return terr
		}
		if pgerror.GetPGCode(terr) == pgcode.InsufficientPrivilege { // table privilege error
			if privilege.IsColumnPrivilegeType(priv) {
				// if the data source is a sequence, return error
				if descriptor.IsSequence() {
					return terr
				}
				// check column privilege
				if cerr := p.checkColumnPrivilege(ctx, descriptor, cols, priv, user); cerr != nil {
					if pgerror.GetPGCode(cerr) == pgcode.InsufficientPrivilege {
						// cols == nil && column privilege = true && table privilege = error
						return terr
					}
					// cols == nil && column privilege = error
					return cerr
				}
				return nil
			}
			return terr
		}
		// table other error
		return terr
	}
	return nil
}

// checkColumnPrivilege check user has the privilege on columns.
// cols checked to be not nil and privilege type matches column.
func (p *planner) checkColumnPrivilege(
	ctx context.Context,
	descriptor *sqlbase.TableDescriptor,
	cols tree.NameList,
	priv privilege.Kind,
	user string,
) error {
	if len(cols) == 0 {
		for i, tc := range descriptor.Columns {
			if !descriptor.Columns[i].IsHidden() {
				if columnErr := p.checkPrivilegeForUser(ctx, &tc, priv, user); columnErr != nil {
					return columnErr
				}
			}
		}
	} else {
		for _, c := range cols {
			for i := range descriptor.Columns {
				tc := &descriptor.Columns[i]
				if c == tc.ColName() {
					if tc.Privileges == nil {
						tc.Privileges = sqlbase.InheritFromTablePrivileges(descriptor.Privileges)
						tc.ParentID = descriptor.ID
					}
					if columnErr := p.checkPrivilegeForUser(ctx, tc, priv, user); columnErr != nil {
						return columnErr
					}
					break
				}
			}
		}
	}
	return nil
}
func (p *planner) HasUserOption(ctx context.Context, option useroption.Option) (bool, error) {
	if p.txn == nil {
		return false, errors.AssertionFailedf("cannot use HasUserOption without a txn")
	}

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return false, err
	}
	if hasAdminRole {
		return true, nil
	}

	normalizedName, err := NormalizeAndValidateUsername(p.SessionData().User)
	if err != nil {
		return false, err
	}
	hasUserOption, err := p.ExecCfg().InternalExecutor.Query(
		ctx, "has-user-options", p.Txn(),
		`SELECT 1 from system.user_options WHERE username = $1 and option = $2  LIMIT 1`,
		normalizedName,
		option.String())
	if err != nil {
		return false, err
	}
	if len(hasUserOption) != 0 {
		return true, nil
	}
	return false, err
}

func (p *planner) CheckUserOption(ctx context.Context, option useroption.Option) error {
	hasUserOption, err := p.HasUserOption(ctx, option)
	if err != nil {
		return err
	}

	if !hasUserOption {
		return pgerror.NewErrorf(pgcode.InsufficientPrivilege,
			"user %s does not have %s option", p.User(), option)
	}
	return nil
}
