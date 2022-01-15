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
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// Grant adds privileges to users.
// Current status:
// - Target: single database, table, or view.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table/view.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Grant(ctx context.Context, n *tree.Grant) (planNode, error) {
	var grantOn privilege.ObjectType
	switch {
	case n.Targets.Databases != nil:
		grantOn = privilege.Database
	case n.Targets.Schemas != nil:
		grantOn = privilege.Schema
	case n.Targets.Tables != nil, n.Targets.Views != nil:
		grantOn = privilege.Table
	case n.Targets.Sequences != nil:
		grantOn = privilege.Sequence
	case len(n.Targets.Function.FuncName.Table()) != 0:
		grantOn = privilege.Function
	}
	if err := tree.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	return &changePrivilegesNode{
		isGrant:      true,
		targets:      n.Targets,
		grantees:     n.Grantees,
		desiredprivs: n.Privileges,
		changePrivilege: func(privDesc *sqlbase.PrivilegeDescriptor, privileges privilege.List, grantee string) {
			privDesc.Grant(p.User(), grantee, privileges, n.GrantAble)
		},
		grantOption: n.GrantAble,
		grantOn:     grantOn,
	}, nil
}

// Revoke removes privileges from users.
// Current status:
// - Target: single database, table, or view.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table/view.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Revoke(ctx context.Context, n *tree.Revoke) (planNode, error) {
	var grantOn privilege.ObjectType
	switch {
	case n.Targets.Databases != nil:
		grantOn = privilege.Database
	case n.Targets.Schemas != nil:
		grantOn = privilege.Schema
	case n.Targets.Tables != nil, n.Targets.Views != nil:
		grantOn = privilege.Table
	case n.Targets.Sequences != nil:
		grantOn = privilege.Sequence
	case len(n.Targets.Function.FuncName.Table()) != 0:
		grantOn = privilege.Function
	}
	if err := tree.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	return &changePrivilegesNode{
		isGrant:      false,
		targets:      n.Targets,
		grantees:     n.Grantees,
		desiredprivs: n.Privileges,
		changePrivilege: func(privDesc *sqlbase.PrivilegeDescriptor, privileges privilege.List, grantee string) {
			privDesc.Revoke(p.User(), grantee, privileges, n.GrantAble)
		},
		grantOn: grantOn,
	}, nil
}

type changePrivilegesNode struct {
	isGrant         bool
	targets         tree.TargetList
	grantees        tree.NameList
	desiredprivs    tree.PrivilegeGroups
	changePrivilege func(*sqlbase.PrivilegeDescriptor, privilege.List, string)
	grantOption     bool
	grantOn         privilege.ObjectType
}

func (n *changePrivilegesNode) startExec(params runParams) error {
	if err := params.p.changePrivileges(params.ctx, n.targets, n.grantees, n.desiredprivs, n.grantOption, n.changePrivilege); err != nil {
		return err
	}
	var eventType EventLogType
	if n.isGrant {
		eventType = EventLogGrantPrivileges
	} else {
		eventType = EventLogRevokePrivileges
	}

	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(eventType),
		Result:    "OK",
		TargetInfo: &server.TargetInfo{
			Desc: struct {
				Target string
			}{
				Target: params.p.stmt.AST.String(),
			},
		},
		Info: "User name: " + params.p.SessionData().User + " SHOW SYNTAX:" + params.p.stmt.SQL,
	}
	return nil
}

// ChangePrivileges is responsible for determining whether a statement is legitimate.
func (p *planner) changePrivileges(
	ctx context.Context,
	targets tree.TargetList,
	grantees tree.NameList,
	privileges tree.PrivilegeGroups,
	grantOption bool,
	changePrivilege func(*sqlbase.PrivilegeDescriptor, privilege.List, string),
) error {
	// Check whether grantees exists
	users, err := p.GetAllUsersAndRoles(ctx)
	if err != nil {
		return err
	}

	// We're allowed to grant/revoke privileges to/from the "public" role even though
	// it does not exist: add it to the list of all users and roles.
	users[sqlbase.PublicRole] = true // isRole

	for _, grantee := range grantees {
		if isRole, ok := users[string(grantee)]; !ok {
			return errors.Errorf("user or role %s does not exist", &grantee)
		} else if isRole && grantOption {
			return errors.Errorf("role cannot get grant option")
		}
	}

	// 防止重复更新db
	dbMap := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
	tbMap := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)

	if privileges[0].Columns != nil {
		for _, priv := range privileges {
			// priv : (privilege) columns
			targets.Columns = priv.Columns
			if err := p.doChangePrivilege(ctx, targets, grantees, tree.PrivilegeGroups{priv}, changePrivilege, dbMap, tbMap); err != nil {
				return err
			}
		}
		return nil
	}

	// priv : (privileges) (databases| schemas| tables)
	return p.doChangePrivilege(ctx, targets, grantees, privileges, changePrivilege, dbMap, tbMap)
}

func (p *planner) doChangePrivilege(
	ctx context.Context,
	targets tree.TargetList,
	grantees tree.NameList,
	privilegeList tree.PrivilegeGroups,
	changePrivilege func(*sqlbase.PrivilegeDescriptor, privilege.List, string),
	dbMap map[sqlbase.ID]*sqlbase.DatabaseDescriptor,
	tbMap map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
) error {
	var descriptors []sqlbase.DescriptorProto
	hasSystemTable := false
	var err error

	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.

	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		descriptors, err = getDescriptorsFromTargetList(ctx, p, targets)
	})
	if err != nil {
		return err
	}
	// First, update the descriptors. We want to catch all errors before
	// we update them in KV below.
	b := p.txn.NewBatch()
	isAll, list := detailALLPrivileges(descriptors, privilegeList)
	for _, descriptor := range descriptors {
		list, err := p.validateGrantPrivilege(ctx, descriptor, list, isAll)
		if err != nil {
			return err
		}

		privileges := descriptor.GetPrivileges()
		for _, grantee := range grantees {
			changePrivilege(privileges, list, string(grantee))
		}

		switch d := descriptor.(type) {
		case *sqlbase.DatabaseDescriptor:
			// Validate privilege descriptors directly as the db/table level Validate
			// may fix up the descriptor.
			if err := privileges.Validate(descriptor.GetID(), privilege.DatabasePrivileges); err != nil {
				return err
			}
			if err := d.Validate(); err != nil {
				return err
			}
			if err := WriteDescToBatch(
				ctx,
				p.extendedEvalCtx.Tracing.KVTracingEnabled(),
				p.execCfg.Settings,
				b,
				descriptor.GetID(),
				descriptor,
			); err != nil {
				return err
			}

		case *sqlbase.SchemaDescriptor:
			// Validate privilege descriptors directly as the db/table level Validate
			// may fix up the descriptor.
			if err := privileges.Validate(descriptor.GetID(), privilege.DatabasePrivileges); err != nil {
				return err
			}
			// change the schemaDescriptor.privilege
			if err := d.Validate(); err != nil {
				return err
			}
			descKey := sqlbase.MakeDescMetadataKey(descriptor.GetID())
			desc := sqlbase.WrapDescriptor(descriptor)
			b.Put(descKey, desc)

			// 如果该db已经被更新过，则重复更新一个
			dbDesc, ok := dbMap[d.ParentID]
			if !ok {
				// 不能使用缓存，会出现更新失效的现象
				dbDesc, err = MustGetDatabaseDescByID(ctx, p.txn, d.ParentID)
				if err != nil {
					return err
				}
				dbMap[d.ParentID] = dbDesc
			}

			// set schemaDescriptor into the databaseDescriptor.Schemas
			for i, schema := range dbDesc.Schemas {
				if schema.Name == d.Name {
					dbDesc.Schemas[i] = *d
					break
				}
			}

		case *sqlbase.MutableTableDescriptor:
			// Validate privilege descriptors directly as the db/table level Validate
			// may fix up the descriptor.
			priv := privilege.TablePrivileges
			if d.IsSequence() {
				priv = privilege.SequencePrivileges
			}
			if err := privileges.Validate(descriptor.GetID(), priv); err != nil {
				return err
			}
			for _, grantee := range grantees {
				cp := privilege.List{}
				for _, kind := range list {
					if privilege.IsColumnPrivilegeType(kind) {
						cp = append(cp, kind)
					}
				}
				if len(cp) > 0 {
					for i := range d.Columns {
						// sequence does not have column privileges
						if d.Columns[i].Privileges != nil {
							cPrivileges := d.Columns[i].Privileges
							changePrivilege(cPrivileges, cp, string(grantee))
						}
					}
				}
			}
			if !d.Dropped() {
				if err := p.writeSchemaChangeToBatch(
					ctx, d, sqlbase.InvalidMutationID, b); err != nil {
					return err
				}
				if d.ParentID == sqlbase.SystemDB.ID && d.ID == sqlbase.LeaseTable.ID {
					hasSystemTable = true
				}
			}

		case *sqlbase.ColumnDescriptor:
			// todo(xz): Is there any missed list due to mixed-versions?
			// todo(xz): system allowed list may have DELETE privilege.
			if err := privileges.Validate(d.ParentID, privilege.ColumnPrivileges); err != nil {
				return err
			}
			tblDesc, ok := tbMap[d.ParentID]
			if !ok {
				tblDesc, err = sqlbase.GetMutableTableDescFromID(ctx, p.txn, d.ParentID)
				if err != nil {
					return err
				}
				tbMap[d.ParentID] = tblDesc
			}

			for i := range tblDesc.Columns {
				if tblDesc.Columns[i].Name == d.Name {
					// todo(xz): what about assign privilege only?
					tblDesc.Columns[i] = *d
					break
				}
			}
			for _, grantee := range grantees {
				for _, priv := range list {
					for _, tblUser := range tblDesc.Privileges.Users {
						if tblUser.User == string(grantee) {
							for _, tblPriv := range tblUser.Privileges {
								if tblPriv.Grantor == p.User() && tblPriv.PrivilegeType == priv {
									d.Privileges.Grant(p.User(), string(grantee), list, tblPriv.GrantAble)
								}
							}
						}
					}
				}
			}
			if err := p.writeSchemaChangeToBatch(
				ctx, tblDesc, sqlbase.InvalidMutationID, b); err != nil {
				return err
			}
			if tblDesc.ParentID == sqlbase.SystemDB.ID && tblDesc.ID == sqlbase.LeaseTable.ID {
				hasSystemTable = true
			}

		case *sqlbase.FunctionDescriptor:
			if targets.Function.IsProcedure != d.IsProcedure {
				funcType := "function"
				if targets.Function.IsProcedure {
					funcType = "procedure"
				}

				paraStr, err := targets.Function.Parameters.GetFuncParasTypeStr()
				if err != nil {
					return err
				}
				keyName := targets.Function.FuncName.Table() + paraStr

				return errors.Errorf("%s is not a %s", keyName, funcType)
			}

			if err := privileges.Validate(descriptor.GetID(), privilege.FunctionPrivileges); err != nil {
				return err
			}

			desc := sqlbase.NewMutableExistingFunctionDescriptor(*d)
			if !d.Dropped() {
				if err := p.writeFunctionSchemaChangeToBatch(
					ctx, desc, sqlbase.InvalidMutationID, b); err != nil {
					return err
				}
			}
		}
	}
	for dbDesc := range dbMap {
		dbDescProto := sqlbase.DescriptorProto(dbMap[dbDesc])
		descKey := sqlbase.MakeDescMetadataKey(dbDescProto.GetID())
		desc := sqlbase.WrapDescriptor(dbDescProto)
		b.Put(descKey, desc)
	}
	// Now update the descriptors transactionally.
	if hasSystemTable {
		return p.ExecuteInNewTxn(ctx, b)
	}
	return p.txn.Run(ctx, b)
}

func detailALLPrivileges(
	descriptors []sqlbase.DescriptorProto, privileges tree.PrivilegeGroups,
) (bool, privilege.List) {
	if len(descriptors) == 0 {
		return false, nil
	}
	if privileges[0].Privilege == privilege.ALL {
		switch descriptors[0].(type) {
		case *sqlbase.DatabaseDescriptor:
			return true, privilege.DatabasePrivileges
		case *sqlbase.SchemaDescriptor:
			return true, privilege.SchemaPrivileges
		case *sqlbase.MutableTableDescriptor:
			if descriptors[0].(*sqlbase.MutableTableDescriptor).IsSequence() {
				return true, privilege.SequencePrivileges
			}
			return true, privilege.TablePrivileges
		case *sqlbase.ColumnDescriptor:
			return true, privilege.ColumnPrivileges
		case *sqlbase.FunctionDescriptor:
			return true, privilege.FunctionPrivileges
		}
	}
	privs := make(privilege.List, len(privileges))
	for i, priv := range privileges {
		privs[i] = priv.Privilege
	}
	return false, privs
}

func grantorErrorMessage(
	descriptor sqlbase.DescriptorProto,
	getFullNameFunc func(string, sqlbase.ID) (tree.NodeFormatter, error),
	user string,
	privilege string,
) error {
	fullName, err := descriptor.GetFullName(getFullNameFunc)
	if err != nil {
		return err
	}

	return fmt.Errorf("user %s does not have GRANT %s privilege on %s %s",
		user, privilege, descriptor.TypeName(), fullName)
}

// validateGrantPrivilege make sure grantor has needed privileges with grant option.
func (p *planner) validateGrantPrivilege(
	ctx context.Context, descriptor sqlbase.DescriptorProto, privileges privilege.List, isAll bool,
) (privilege.List, error) {
	list := make(privilege.List, 0)
	for _, priv := range privileges {
		// `ALL` term can get the privileges able to granted.
		if err := p.checkPrivilege(ctx, descriptor, priv, p.User(), true); err != nil {
			if !isAll {
				// There is not ALL, and no privilege for grantor.
				if err := p.checkPrivilege(ctx, descriptor, priv, p.User(), false); err != nil {
					return nil, err
				}
				return nil, grantorErrorMessage(descriptor, p.getFullNameFunc(ctx, descriptor), p.SessionData().User, priv.String())
			}
			// If no privilege for grantor but there is `ALL` term, then no-op.
		} else {
			list = append(list, priv)
		}
	}
	if len(list) == 0 && isAll {
		return nil, grantorErrorMessage(descriptor, p.getFullNameFunc(ctx, descriptor), p.SessionData().User, "any")
	}
	return list, nil
}

func (*changePrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changePrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changePrivilegesNode) Close(context.Context)        {}
