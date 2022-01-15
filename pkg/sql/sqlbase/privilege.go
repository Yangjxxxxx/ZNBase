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

package sqlbase

import (
	"context"
	"fmt"
	"sort"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/log"
)

func isPrivilege(bits uint32, priv privilege.Kind) bool {
	return bits&priv.Mask() != 0
}

// IsPrivilegeSet judge whether this type exists and is grantAble permissions.
func isPrivilegeSet(privileges []Privilege, priv privilege.Kind, grantAble bool) bool {
	for _, p := range privileges {
		if p.PrivilegeType == priv && (p.GrantAble || !grantAble) {
			return true
		}
	}
	return false
}

// HasAdmin determines whether there are privileges granted by admin.
func hasAdmin(privileges []Privilege, priv privilege.Kind) bool {
	for _, p := range privileges {
		if p.PrivilegeType == priv && p.Grantor == AdminRole {
			return true
		}
	}
	return false
}

// isPrivilegeSetList judge whether this type exists and is grantAble permissions.
func isPrivilegeSetList(privileges []Privilege, priv privilege.List, grantAble bool) bool {
	var bits uint32
	for _, p := range privileges {
		if p.GrantAble || !grantAble {
			bits |= p.PrivilegeType.Mask()
		}
	}
	for _, p := range priv {
		if !isPrivilege(bits, p) {
			return false
		}
	}
	return true
}

// findUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (p PrivilegeDescriptor) findUserIndex(user string) int {
	idx := sort.Search(len(p.Users), func(i int) bool {
		return p.Users[i].User >= user
	})
	if idx < len(p.Users) && p.Users[idx].User == user {
		return idx
	}
	return -1
}

// findUser looks for a specific user in the list.
// Returns (nil, false) if not found, or (obj, true) if found.
func (p PrivilegeDescriptor) findUser(user string) (*UserPrivileges, bool) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		return nil, false
	}
	return &p.Users[idx], true
}

// findOrCreateUser looks for a specific user in the list, creating it if needed.
func (p *PrivilegeDescriptor) findOrCreateUser(user string) *UserPrivileges {
	idx := sort.Search(len(p.Users), func(i int) bool {
		return p.Users[i].User >= user
	})
	if idx == len(p.Users) {
		// Not found but should be inserted at the end.
		p.Users = append(p.Users, UserPrivileges{User: user})
	} else if p.Users[idx].User == user {
		// Found.
	} else {
		// New element to be inserted at idx.
		p.Users = append(p.Users, UserPrivileges{})
		copy(p.Users[idx+1:], p.Users[idx:])
		p.Users[idx] = UserPrivileges{User: user}
	}
	return &p.Users[idx]
}

// removeUser looks for a given user in the list and removes it if present.
func (p *PrivilegeDescriptor) removeUser(user string) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		// Not found.
		return
	}
	p.Users = append(p.Users[:idx], p.Users[idx+1:]...)
}

// removeEmptyUser looks none UserPrivileges user and removes it if present.
func (p *PrivilegeDescriptor) removeEmptyUser() {
	var res []UserPrivileges
	for _, user := range p.Users {
		if len(user.Privileges) > 0 {
			res = append(res, user)
		}
	}
	if len(res) != len(p.Users) {
		p.Users = res
	}
}

// NewPrivileges gives the specified user new privileges
func NewPrivileges(grantor string, priv privilege.List, grant bool) []Privilege {
	var ret []Privilege
	for _, p := range priv {
		ret = append(ret, Privilege{
			Grantor:       grantor,
			PrivilegeType: p,
			GrantAble:     grant,
		})
	}
	return ret
}

// NewCustomSuperuserPrivilegeDescriptor returns a privilege descriptor for the root user
// and the admin role with specified privileges.
func NewCustomSuperuserPrivilegeDescriptor(priv privilege.List, owner string) *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		Owner: owner,
		Users: []UserPrivileges{
			{
				User:       AdminRole,
				Privileges: NewPrivileges(AdminRole, priv, true),
			},
			{
				User:       security.RootUser,
				Privileges: NewPrivileges(AdminRole, priv, true),
			},
		},
	}
}

// NewPrivilegeDescriptor returns a privilege descriptor for the given
// user with the specified list of privileges.
func NewPrivilegeDescriptor(user string, priv privilege.List, owner string) *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		Owner: owner,
		Users: []UserPrivileges{
			{
				User:       user,
				Privileges: NewPrivileges(AdminRole, priv, false),
			},
		},
	}
}

// NewDefaultObjectPrivilegeDescriptor returns an object privilege descriptor
// with owner to set and ALL privileges for the root user and admin role.
func NewDefaultObjectPrivilegeDescriptor(
	objectType privilege.ObjectType, owner string,
) *PrivilegeDescriptor {
	return NewCustomSuperuserPrivilegeDescriptor(privilege.GetValidPrivilegesForObject(objectType), owner)
}

// Grant adds new privileges to this descriptor for a given list of users.
// TODO(marc): if all privileges other than ALL are set, should we collapse
// them into ALL?
func (p *PrivilegeDescriptor) Grant(
	grantor string, grantee string, privList privilege.List, grantOption bool,
) {
	// 注意grantor权限在之前已判断
	granteePriv := p.findOrCreateUser(grantee)
	if isPrivilegeSet(granteePriv.Privileges, privilege.ALL, false) {
		// User already has 'ALL' privilege: no-op.
		log.Fatal(context.Background(), "privilege is ALL")
		return
	}
	if len(granteePriv.Privileges) == 0 {
		for _, v := range privList {
			temp := Privilege{Grantor: grantor, PrivilegeType: v, GrantAble: grantOption}
			granteePriv.Privileges = append(granteePriv.Privileges, temp)
		}
		return
	}
	for _, v := range privList {
		found := false
		for j := 0; j < len(granteePriv.Privileges); j++ {
			// 如果已经存在该权限，相同grantor只改变grantAble，不同则增加新的一列
			if v == granteePriv.Privileges[j].PrivilegeType {
				if grantor == granteePriv.Privileges[j].Grantor {
					if granteePriv.Privileges[j].GrantAble == false {
						granteePriv.Privileges[j].GrantAble = grantOption
					}
					found = true
					break
				}
			}
		}
		if !found {
			temp := Privilege{Grantor: grantor, PrivilegeType: v, GrantAble: grantOption}
			granteePriv.Privileges = append(granteePriv.Privileges, temp)
		}
	}

}

// Revoke updates descriptor and removes permissions.
func (p *PrivilegeDescriptor) revoke(grantee *UserPrivileges, priv Privilege, grantors []string) {
	idx := -1
	needRecursion := false

	for i, pr := range grantee.Privileges {
		if pr.Grantor == priv.Grantor && pr.PrivilegeType == priv.PrivilegeType {
			if !priv.GrantAble {
				// 删除权限
				idx = i
				if pr.GrantAble {
					// 级联删除grant权限
					needRecursion = true
				}
			} else {
				// 删除grant权限
				needRecursion = true
				grantee.Privileges[i].GrantAble = false
			}
			// 同一个grantor grantee priv只应该有一条，如果有多条就是grant的bug
			break
		}
	}

	// 删除权限
	if idx > -1 {
		grantee.Privileges = append(grantee.Privileges[:idx], grantee.Privileges[idx+1:]...)
	}

	// 防止级联删除被扩大
	for _, v := range grantors {
		if grantee.User == v {
			return
		}
	}

	// 需要级联时先级联删除自己赋予自己的权限
	if needRecursion {
		for i, pr := range grantee.Privileges {
			if pr.PrivilegeType == priv.PrivilegeType && pr.Grantor == grantee.User {
				grantee.Privileges = append(grantee.Privileges[:i], grantee.Privileges[i+1:]...)
			}
		}
	}

	hasGrant := isPrivilegeSet(grantee.Privileges, priv.PrivilegeType, true)

	grantors = append(grantors, priv.Grantor)

	if needRecursion && !hasGrant {
		for i := range p.Users {
			temp := grantors
			if p.Users[i].User != grantee.User {
				p.revoke(&p.Users[i], Privilege{Grantor: grantee.User, PrivilegeType: priv.PrivilegeType, GrantAble: false}, temp)
			}
		}
	}
}

// Revoke removes privileges from this descriptor for a given list of users.
func (p *PrivilegeDescriptor) Revoke(
	grantor string, grantee string, privList privilege.List, grantOption bool,
) {
	// 注意grantor权限在之前已判断
	granteePriv, ok := p.findUser(grantee)
	if !ok {
		// Removing privileges from a user without privileges is a no-op.
		return
	}

	if len(granteePriv.Privileges) == 0 {
		p.removeUser(grantee)
		return
	}

	for _, priv := range privList {
		grantors := make([]string, 0)
		p.revoke(granteePriv, Privilege{Grantor: grantor, PrivilegeType: priv, GrantAble: grantOption}, grantors)
	}

	p.removeEmptyUser()
}

// MaybeFixPrivileges fixes the privilege descriptor if needed, including:
// * adding default privileges for the "admin" role
// * fixing default privileges for the "root" user
// * fixing maximum privileges for users.
// Returns true if the privilege descriptor was modified.
func (p *PrivilegeDescriptor) MaybeFixPrivileges(id ID, allowedPrivileges privilege.List) bool {
	if IsReservedID(id) {
		// System databases and tables have custom maximum allowed privileges.
		allowedPrivileges = SystemAllowedPrivileges[id]
	}

	var modified bool
	fixSystem := func(user string) {
		privs := p.findOrCreateUser(user)
		var temp []Privilege
		for _, ap := range allowedPrivileges {
			if len(privs.Privileges) != len(allowedPrivileges) {
				modified = true
			}
			if !isPrivilegeSet(privs.Privileges, ap, true) {
				modified = true
			}
			temp = append(temp, Privilege{Grantor: AdminRole, PrivilegeType: ap, GrantAble: true})
		}
		privs.Privileges = temp
	}

	fixSuperUser := func(user string) {
		privs := p.findOrCreateUser(user)
		equal := false
		for _, ap := range allowedPrivileges {
			if !isPrivilegeSet(privs.Privileges, ap, true) {
				equal = true
			} else if !hasAdmin(privs.Privileges, ap) {
				equal = true
			}
			if equal {
				privs.Privileges = append(privs.Privileges, Privilege{Grantor: AdminRole, PrivilegeType: ap, GrantAble: true})
				modified = true
				equal = false
			}
		}
	}

	if IsReservedID(id) {
		fixSystem(security.RootUser)
		fixSystem(AdminRole)
	} else {
		// Check "root" user and "admin" role.
		fixSuperUser(security.RootUser)
		fixSuperUser(AdminRole)
	}

	if isPrivilege(allowedPrivileges.ToBitField(), privilege.ALL) {
		// User already has 'ALL' privilege: no-op.
		log.Fatal(context.Background(), "privilege is ALL")
		return modified
	}

	allowedPrivilegesBits := allowedPrivileges.ToBitField()
	for i, u := range p.Users {
		if u.User == security.RootUser || u.User == AdminRole {
			// We've already checked super users.
			continue
		}

		var privs []Privilege
		for _, priv := range u.Privileges {
			if isPrivilege(allowedPrivilegesBits, priv.PrivilegeType) {
				privs = append(privs, priv)
			} else {
				modified = true
			}
		}

		p.Users[i].Privileges = privs
	}

	return modified
}

// InheritFromTablePrivileges make column privileges inherit from table privileges.
// InheritFromTablePrivileges fixes column privileges into SELECT, INSERT, UPDATE.
func InheritFromTablePrivileges(table *PrivilegeDescriptor) *PrivilegeDescriptor {
	// todo(xz): Do we need remain owner information in column privilege ?
	// Inherit each column privilege from table privilege.
	length := len(table.Users)
	column := &PrivilegeDescriptor{Users: make([]UserPrivileges, length), Owner: table.Owner}
	for i := range column.Users {
		colUser := &column.Users[i]
		tableUser := &table.Users[i]

		colUser.Privileges = make([]Privilege, len(tableUser.Privileges))
		colUser.User = tableUser.User
		copy(colUser.Privileges, tableUser.Privileges)
	}

	// Fix each column privilege.
	allowedPrivilegesBits := privilege.ColumnPrivileges.ToBitField()
	for i := range column.Users {
		var privs []Privilege
		colUser := &column.Users[i]
		for _, priv := range colUser.Privileges {
			if isPrivilege(allowedPrivilegesBits, priv.PrivilegeType) {
				privs = append(privs, priv)
			}
		}
		colUser.Privileges = privs
	}

	for i := range column.Users {
		if column.Users[i].Privileges == nil {
			column.Users = append(column.Users[:i], column.Users[i+1:]...)
		}
	}
	return column
}

// Validate is called when writing a database or table descriptor.
// It takes the descriptor ID which is used to determine if
// it belongs to a system descriptor, in which case the maximum
// set of allowed privileges is looked up and applied.
func (p *PrivilegeDescriptor) Validate(id ID, allowedPrivileges privilege.List) error {
	if IsReservedID(id) {
		var ok bool
		allowedPrivileges, ok = SystemAllowedPrivileges[id]
		if !ok {
			return fmt.Errorf("no allowed privileges found for system object with ID=%d", id)
		}
	}

	// Check "root" user.
	if err := p.validateRequiredSuperuser(id, allowedPrivileges, security.RootUser); err != nil {
		return err
	}

	// We expect an "admin" role. Check that it has desired superuser permissions.
	if err := p.validateRequiredSuperuser(id, allowedPrivileges, AdminRole); err != nil {
		return err
	}

	allowedPrivilegesBits := allowedPrivileges.ToBitField()
	if isPrivilege(allowedPrivilegesBits, privilege.ALL) {
		// ALL privileges allowed, we can skip regular users.
		return nil
	}

	// For all non-super users, privileges must not exceed the allowed privileges.
	for _, u := range p.Users {
		if u.User == security.RootUser || u.User == AdminRole {
			// We've already checked super users.
			continue
		}

		for _, priv := range u.Privileges {
			if !isPrivilege(allowedPrivilegesBits, priv.PrivilegeType) {
				return fmt.Errorf("user %s must not have %s privileges on system object with ID=%d",
					u.User, priv.PrivilegeType, id)
			}
		}
	}
	if p.HasLoop() {
		return fmt.Errorf("privilege has loop")
	}
	return nil
}

// ValidateRequiredSuperuser whether superUser users have the correct permissions.
func (p *PrivilegeDescriptor) validateRequiredSuperuser(
	id ID, allowedPrivileges privilege.List, user string,
) error {
	superPriv, ok := p.findUser(user)
	if !ok {
		return fmt.Errorf("user %s does not have privileges", user)
	}

	// The super users must match the allowed privilege set exactly.
	if !isPrivilegeSetList(superPriv.Privileges, allowedPrivileges, true) {
		return fmt.Errorf("user %s must have exactly %s privileges on system object with ID=%d",
			user, allowedPrivileges, id)
	}

	return nil
}

// todo(xz): no use till now. If useful, think about owner's privilege information.
// may used when rename object.
//func (p *PrivilegeDescriptor) SetOwner(owner string) {
//	p.Owner = owner
//}

// PrivilegeString converts Privilege to a string type
type PrivilegeString struct {
	Grantor   string
	Type      string
	GrantAble bool
}

// UserPrivilegeString is a pair of strings describing the
// privileges for a given user.
type UserPrivilegeString struct {
	User       string
	Privileges []PrivilegeString
}

// PrivilegeString returns a string of comma-separted privilege names.
func (u UserPrivilegeString) PrivilegeString() string {
	// return strings.Join(u.Privileges, ",")
	// todo lixinze used for test
	return ""
}

// Show returns the list of {username, privileges} sorted by username.
// 'privileges' is a string of comma-separated sorted privilege names.
func (p *PrivilegeDescriptor) Show() []UserPrivilegeString {
	ret := make([]UserPrivilegeString, 0, len(p.Users))

	for _, userPriv := range p.Users {
		ps := make([]PrivilegeString, 0, len(userPriv.Privileges))
		for _, up := range userPriv.Privileges {
			ps = append(ps, PrivilegeString{
				Grantor:   up.Grantor,
				Type:      up.PrivilegeType.String(),
				GrantAble: up.GrantAble,
			})
		}
		ret = append(ret, UserPrivilegeString{
			User:       userPriv.User,
			Privileges: ps,
		})
	}
	return ret
}

// CheckPrivilege returns true if 'user' has 'privilege' on this descriptor.
func (p *PrivilegeDescriptor) CheckPrivilege(
	user string, priv privilege.Kind, grantAble bool,
) bool {
	userPriv, ok := p.findUser(user)
	if !ok {
		// User "node" has all privileges.
		return user == security.NodeUser
	}
	return isPrivilegeSet(userPriv.Privileges, priv, grantAble)
}

// AnyPrivilege returns true if 'user' has any privilege on this descriptor.
func (p *PrivilegeDescriptor) AnyPrivilege(user string) bool {
	if p.Owner == user {
		return true
	}
	userPriv, ok := p.findUser(user)
	if !ok {
		return false
	}
	return len(userPriv.Privileges) != 0
}

// CheckAccessPrivilege returns true if 'user' has 'access' privilege on this descriptor (database / schema)
func (p *PrivilegeDescriptor) CheckAccessPrivilege(user string) bool {
	for _, kind := range privilege.Access {
		if p.CheckPrivilege(user, kind, false) {
			return true
		}
	}
	return false
}

// RemoveUserPriNotExist if user does not exist in local, remove it when import data
func (p *PrivilegeDescriptor) RemoveUserPriNotExist(ctx context.Context, txn *client.Txn) error {
	for _, userPri := range p.Users {
		user := userPri.User
		isExist, err := isExistUser(ctx, txn, user)
		if err != nil {
			return err
		}
		if !isExist {
			p.removeUser(user)
		}
	}
	return nil
}

func isExistUser(ctx context.Context, txn *client.Txn, user string) (bool, error) {
	var startKey, endKey []byte

	// Table role_member's ID as StartKey
	startKey = encoding.EncodeUvarintAscending(startKey, uint64(keys.UsersTableID))
	endKey = encoding.EncodeUvarintAscending(startKey, uint64(keys.UsersTableID+1))
	span := roachpb.Span{Key: startKey, EndKey: endKey}

	// Make a ScanRequest to get the elements of the table role_member
	var ba client.Batch
	ba.AddRawRequest(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeaderFromSpan(span)})
	err := txn.Run(ctx, &ba)
	if err != nil {
		return false, err
	}

	kvs := ba.RawResponse().Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	for _, kv := range kvs {
		remaining, _, err := encoding.DecodeUvarintAscending(kv.Key)
		if err != nil {
			return false, err
		}
		// get the primary key id (1), ignore it
		remaining, _, err = encoding.DecodeUvarintAscending(remaining)
		if err != nil {
			return false, err
		}
		// get the userName in system.users
		_, userName, err := encoding.DecodeUnsafeStringAscending(remaining, nil)
		if err != nil {
			return false, err
		}
		if userName == user {
			return true, nil
		}
	}
	return false, nil
}

// HasLoop judges whether or not each privilege in the privilege descriptor is in a loop.
func (p *PrivilegeDescriptor) HasLoop() bool {
	// [Kind][Grantor][Grantee...]
	//       [Grantor][Grantee...]
	// ... ...
	privileges := map[privilege.Kind]map[string][]string{}
	for _, u := range p.Users {
		if u.User == AdminRole {
			continue
		}
		for _, v := range u.Privileges {
			if v.GrantAble == true {
				if m, ok := privileges[v.PrivilegeType]; ok {
					if g, ok := m[v.Grantor]; ok {
						g = append(g, u.User)
					} else {
						m[v.Grantor] = []string{u.User}
					}
				} else {
					privileges[v.PrivilegeType] = make(map[string][]string)
					privileges[v.PrivilegeType][v.Grantor] = []string{u.User}
				}
			}
		}
	}
	for m := range privileges {
		if hasLoop(privileges[m]) {
			return true
		}
	}
	return false
}

// hasLoop judges whether or not each privilege in the privilege descriptor is in a loop.
func hasLoop(privileges map[string][]string) bool {
	// seen[item]
	// == 0 not been seen
	// == 1 has been seen
	// == -1 being seen
	flag := false
	seen := make(map[string]int)
	var visitAll func(item []string)
	visitAll = func(items []string) {
		for _, item := range items {
			if seen[item] == 0 {
				seen[item] = -1
				visitAll(privileges[item]) // search the son firstly: privileges[item]
				seen[item] = 1
			} else if seen[item] == -1 {
				flag = true
				return
			}
		}
	}
	var keys []string
	for key := range privileges {
		keys = append(keys, key)
	}
	visitAll(keys)
	return flag
}
