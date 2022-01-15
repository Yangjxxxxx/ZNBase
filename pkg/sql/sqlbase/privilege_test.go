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
	"reflect"
	"testing"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

var AllPrivilege = privilege.List{
	privilege.CREATE, privilege.DROP, privilege.USAGE,
	privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE,
}

func TestPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	descriptor := NewDefaultObjectPrivilegeDescriptor(privilege.Database, AdminRole)

	showRootAllPrivilege := []PrivilegeString{
		{
			Grantor:   "root",
			Type:      "CREATE",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "DROP",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "USAGE",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "SELECT",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "INSERT",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "DELETE",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "UPDATE",
			GrantAble: true,
		},
	}

	showRootExp1 := []PrivilegeString{
		{
			Grantor:   "root",
			Type:      "CREATE",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "DROP",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "USAGE",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "DELETE",
			GrantAble: true,
		},
		{
			Grantor:   "root",
			Type:      "UPDATE",
			GrantAble: true,
		},
	}

	showAllPrivilege := []PrivilegeString{
		{
			Grantor:   AdminRole,
			Type:      "CREATE",
			GrantAble: true,
		},
		{
			Grantor:   AdminRole,
			Type:      "DROP",
			GrantAble: true,
		},
		{
			Grantor:   AdminRole,
			Type:      "USAGE",
			GrantAble: true,
		},
		{
			Grantor:   AdminRole,
			Type:      "SELECT",
			GrantAble: true,
		},
		{
			Grantor:   AdminRole,
			Type:      "INSERT",
			GrantAble: true,
		},
		{
			Grantor:   AdminRole,
			Type:      "DELETE",
			GrantAble: true,
		},
		{
			Grantor:   AdminRole,
			Type:      "UPDATE",
			GrantAble: true,
		},
	}

	testCases := []struct {
		grantor       string
		grantee       string // User to grant/revoke privileges on.
		grantable     bool
		grant, revoke privilege.List
		show          []UserPrivilegeString
	}{
		{AdminRole, AdminRole, true, nil, nil,
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{security.RootUser, showAllPrivilege},
			},
		},
		{AdminRole, security.RootUser, true, AllPrivilege, nil,
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{security.RootUser, showAllPrivilege},
			},
		},
		{AdminRole, security.RootUser, true, privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{security.RootUser, showAllPrivilege},
			},
		},
		{security.RootUser, "foo", true, privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{"foo", []PrivilegeString{
					{
						Grantor:   security.RootUser,
						Type:      "INSERT",
						GrantAble: true,
					},
					{
						Grantor:   security.RootUser,
						Type:      "DROP",
						GrantAble: true,
					},
				}},
				{security.RootUser, showAllPrivilege},
			},
		},
		{security.RootUser, "bar", true, nil, AllPrivilege,
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{"foo", []PrivilegeString{
					{
						Grantor:   security.RootUser,
						Type:      "INSERT",
						GrantAble: true,
					},
					{
						Grantor:   security.RootUser,
						Type:      "DROP",
						GrantAble: true,
					},
				}},
				{security.RootUser, showAllPrivilege},
			},
		},
		{security.RootUser, "foo", true, AllPrivilege, nil,
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{"foo", showRootAllPrivilege},
				{security.RootUser, showAllPrivilege},
			},
		},
		{security.RootUser, "foo", false, nil, privilege.List{privilege.SELECT, privilege.INSERT},
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{"foo", showRootExp1},
				{security.RootUser, showAllPrivilege},
			},
		},
		{security.RootUser, "foo", false, nil, AllPrivilege,
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{security.RootUser, showAllPrivilege},
			},
		},
		// Validate checks that root still has ALL privileges, but we do not call it here.
		{security.RootUser, security.RootUser, true, nil, AllPrivilege,
			[]UserPrivilegeString{
				{AdminRole, showAllPrivilege},
				{security.RootUser, showAllPrivilege},
			},
		},
	}

	for tcNum, tc := range testCases {
		if tc.grantee != "" {
			if tc.grant != nil {
				descriptor.Grant(tc.grantor, tc.grantee, tc.grant, tc.grantable)
			}
			if tc.revoke != nil {
				descriptor.Revoke(tc.grantor, tc.grantee, tc.revoke, tc.grantable)
			}
		}
		show := descriptor.Show()
		if len(show) != len(tc.show) {
			t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v\n, expected %+v",
				tcNum, descriptor, show, tc.show)
		}
		for i := 0; i < len(show); i++ {
			if show[i].User != tc.show[i].User || show[i].PrivilegeString() != tc.show[i].PrivilegeString() {
				t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
					tcNum, descriptor, show, tc.show)
			}
		}
	}
}

func TestCheckPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pd   *PrivilegeDescriptor
		user string
		priv privilege.Kind
		exp  bool
	}{
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, AdminRole),
			"foo", privilege.CREATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, AdminRole),
			"bar", privilege.CREATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, AdminRole),
			"bar", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, AdminRole),
			"foo", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", AllPrivilege, AdminRole),
			"foo", privilege.CREATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, AdminRole),
			"foo", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", AllPrivilege, AdminRole),
			"foo", privilege.UPDATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{}, AdminRole),
			"foo", privilege.UPDATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{}, AdminRole),
			"foo", privilege.CREATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}, AdminRole),
			"foo", privilege.UPDATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}, AdminRole),
			"foo", privilege.DROP, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, AdminRole),
			"foo", privilege.DROP, false},
	}

	for tcNum, tc := range testCases {
		if found := tc.pd.CheckPrivilege(tc.user, tc.priv, false); found != tc.exp {
			t.Errorf("#%d: CheckPrivilege(%s, %v) for descriptor %+v = %t, expected %t",
				tcNum, tc.user, tc.priv, tc.pd, found, tc.exp)
		}
	}
}

func TestAnyPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pd   *PrivilegeDescriptor
		user string
		exp  bool
	}{
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, AdminRole),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, AdminRole),
			"bar", false},
		{NewPrivilegeDescriptor("foo", AllPrivilege, AdminRole),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{}, AdminRole),
			"foo", false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}, AdminRole),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}, AdminRole),
			"bar", false},
	}

	for tcNum, tc := range testCases {
		if found := tc.pd.AnyPrivilege(tc.user); found != tc.exp {
			t.Errorf("#%d: AnyPrivilege(%s) for descriptor %+v = %t, expected %t",
				tcNum, tc.user, tc.pd, found, tc.exp)
		}
	}
}

// TestPrivilegeValidate exercises validation for non-system descriptors.
func TestPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	AllDatabasePrivilege := privilege.List{
		privilege.CREATE, privilege.DROP, privilege.USAGE,
	}
	id := ID(keys.MinUserDescID)
	descriptor := NewDefaultObjectPrivilegeDescriptor(privilege.Database, AdminRole)
	if err := descriptor.Validate(id, AllDatabasePrivilege); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant("root", "foo", AllDatabasePrivilege, true)
	if err := descriptor.Validate(id, AllDatabasePrivilege); err != nil {
		t.Fatal(err)
	}
	//
	descriptor.Grant(AdminRole, security.RootUser, privilege.List{privilege.SELECT}, true)
	if err := descriptor.Validate(id, AllDatabasePrivilege); err != nil {
		t.Fatal(err)
	}
	descriptor.Revoke(AdminRole, security.RootUser, privilege.List{privilege.CREATE}, false)
	if err := descriptor.Validate(id, AllDatabasePrivilege); err == nil {
		t.Fatalf("unexpected success! err: %s", err)
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant(AdminRole, security.RootUser, privilege.List{privilege.CREATE}, true)
	if err := descriptor.Validate(id, AllDatabasePrivilege); err != nil {
		t.Fatalf("unexpected success! err: %s", err)
	}
	descriptor.Revoke(AdminRole, security.RootUser, AllDatabasePrivilege, true)
	if err := descriptor.Validate(id, AllDatabasePrivilege); err == nil {
		t.Fatalf("unexpected success! err: %s", err)
	}
}

// TestSystemPrivilegeValidate exercises validation for system config
// descriptors. We use a dummy system table installed for testing
// purposes.
func TestSystemPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	id := ID(keys.MaxReservedDescID)
	if _, exists := SystemAllowedPrivileges[id]; exists {
		t.Fatalf("system object with maximum id %d already exists--is the reserved id space full?", id)
	}
	SystemAllowedPrivileges[id] = privilege.List{privilege.SELECT}
	defer delete(SystemAllowedPrivileges, id)

	rootWrongPrivilegesErr := "user root must have exactly SELECT " +
		"privileges on system object with ID=.*"
	adminWrongPrivilegesErr := "user admin must have exactly SELECT " +
		"privileges on system object with ID=.*"

	{
		// Valid: root user has one of the allowable privilege sets.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT},
			AdminRole,
		)
		if err := descriptor.Validate(id, privilege.List{privilege.SELECT}); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant("root", "foo", privilege.List{privilege.SELECT}, false)
		if err := descriptor.Validate(id, privilege.List{privilege.SELECT}); err != nil {
			t.Fatal(err)
		}
	}

	{
		// Valid: root has exactly the allowed privileges.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT},
			AdminRole,
		)

		// Valid: foo can have privileges revoked, including privileges it doesn't currently have.
		descriptor.Revoke("root", "foo", privilege.List{privilege.UPDATE}, false)
		if err := descriptor.Validate(id, privilege.List{privilege.SELECT}); err != nil {
			t.Fatal(err)
		}

		// Invalid: root user has too many privileges.
		descriptor.Grant(AdminRole, security.RootUser, privilege.List{privilege.UPDATE}, false)
		descriptor.Revoke(AdminRole, security.RootUser, privilege.List{privilege.SELECT}, false)
		if err := descriptor.Validate(id, privilege.List{privilege.SELECT}); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}
	}

	{
		// Invalid: root has a non-allowable privilege set.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(privilege.List{privilege.UPDATE}, AdminRole)
		if err := descriptor.Validate(id, privilege.List{privilege.SELECT}); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}

		// Invalid: root's invalid privileges are revoked and replaced with allowable privileges,
		// but admin is still wrong.
		descriptor.Revoke(AdminRole, security.RootUser, privilege.List{privilege.UPDATE}, false)
		descriptor.Grant(AdminRole, security.RootUser, privilege.List{privilege.SELECT}, true)
		if err := descriptor.Validate(id, privilege.List{privilege.SELECT}); !testutils.IsError(err, adminWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", adminWrongPrivilegesErr, err)
		}

		// Valid: admin's invalid privileges are revoked and replaced with allowable privileges.
		descriptor.Revoke(AdminRole, AdminRole, privilege.List{privilege.UPDATE}, false)
		descriptor.Grant(AdminRole, AdminRole, privilege.List{privilege.SELECT}, true)
		if err := descriptor.Validate(id, privilege.List{privilege.SELECT}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFixPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a non-system ID.
	userID := ID(keys.MinUserDescID)
	userPrivs := AllPrivilege

	// And create an entry for a fake system table.
	systemID := ID(keys.MaxReservedDescID)
	if _, exists := SystemAllowedPrivileges[systemID]; exists {
		t.Fatalf("system object with maximum id %d already exists--is the reserved id space full?", systemID)
	}
	systemPrivs := privilege.List{privilege.SELECT}
	SystemAllowedPrivileges[systemID] = systemPrivs
	defer delete(SystemAllowedPrivileges, systemID)

	getPrivs := func(id ID) privilege.List {
		if id == userID {
			return userPrivs
		}
		return systemPrivs
	}

	type userPrivileges map[string]privilege.List

	testCases := []struct {
		id       ID
		input    userPrivileges
		modified bool
		output   userPrivileges
	}{
		{
			// Empty privileges for system ID.
			systemID,
			userPrivileges{},
			true,
			userPrivileges{
				security.RootUser: systemPrivs,
				AdminRole:         systemPrivs,
			},
		},
		{
			// Valid requirements for system ID.
			systemID,
			userPrivileges{
				security.RootUser: systemPrivs,
				AdminRole:         systemPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{},
				"baz":             privilege.List{privilege.SELECT},
			},
			false,
			userPrivileges{
				security.RootUser: systemPrivs,
				AdminRole:         systemPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{},
				"baz":             privilege.List{privilege.SELECT},
			},
		},
		{
			// Too many privileges for system ID.
			systemID,
			userPrivileges{
				security.RootUser: AllPrivilege,
				AdminRole:         AllPrivilege,
				"foo":             AllPrivilege,
				"bar":             privilege.List{privilege.SELECT, privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUser: systemPrivs,
				AdminRole:         systemPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{privilege.SELECT},
			},
		},
		{
			// Empty privileges for non-system ID.
			userID,
			userPrivileges{},
			true,
			userPrivileges{
				security.RootUser: userPrivs,
				AdminRole:         userPrivs,
			},
		},
		{
			// Valid requirements for non-system ID.
			userID,
			userPrivileges{
				security.RootUser: userPrivs,
				AdminRole:         userPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{},
				"baz":             privilege.List{privilege.SELECT},
			},
			false,
			userPrivileges{
				security.RootUser: userPrivs,
				AdminRole:         userPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{},
				"baz":             privilege.List{privilege.SELECT},
			},
		},
		{
			// All privileges are allowed for non-system ID, but we need super users.
			userID,
			userPrivileges{
				"foo": AllPrivilege,
				"bar": privilege.List{privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUser: AllPrivilege,
				AdminRole:         AllPrivilege,
				"foo":             AllPrivilege,
				"bar":             privilege.List{privilege.UPDATE},
			},
		},
	}

	for num, testCase := range testCases {
		desc := &PrivilegeDescriptor{}
		for u, p := range testCase.input {
			desc.Grant(AdminRole, u, p, true)
		}

		if a, e := desc.MaybeFixPrivileges(testCase.id, getPrivs(testCase.id)), testCase.modified; a != e {
			t.Errorf("#%d: expected modified=%t, got modified=%t", num, e, a)
			continue
		}

		if a, e := len(desc.Users), len(testCase.output); a != e {
			t.Errorf("#%d: expected %d users (%v), got %d (%v)", num, e, testCase.output, a, desc.Users)
			continue
		}

		for u, p := range testCase.output {
			outputUser, ok := desc.findUser(u)
			if !ok {
				t.Fatalf("#%d: expected user %s in output, but not found (%v)", num, u, desc.Users)
			}
			var privs privilege.List
			for _, priv := range outputUser.Privileges {
				privs = append(privs, priv.PrivilegeType)
			}
			if privs.ToBitField() != p.ToBitField() {
				t.Errorf("#%d: user %s: expected privileges %v, got %v", num, u, p, privs)
			}
		}
	}
}

// TestRevoke verify the revoke function. We use a dummy PrivilegeDescriptor
// for testing purposes.
func TestRevoke(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pd            *PrivilegeDescriptor
		grantor       string
		grantee       string
		PrivilegeType privilege.Kind
		grantAble     bool
		exp           *PrivilegeDescriptor
	}{
		{&PrivilegeDescriptor{
			Users: []UserPrivileges{
				{
					User:       "a",
					Privileges: NewPrivileges("root", privilege.DatabasePrivileges, true),
				},
				{
					User: "b",
					Privileges: append(
						NewPrivileges("a", privilege.List{privilege.DROP}, true),
						NewPrivileges("b", privilege.List{privilege.DROP}, true)...,
					),
				},
			},
		},
			"a",
			"b",
			privilege.DROP,
			false,
			&PrivilegeDescriptor{
				Users: []UserPrivileges{
					{
						User:       "a",
						Privileges: NewPrivileges("root", privilege.DatabasePrivileges, true),
					},
					{
						User:       "b",
						Privileges: []Privilege{},
					},
				},
			},
		},
		{&PrivilegeDescriptor{
			Users: []UserPrivileges{
				{
					User:       "a",
					Privileges: NewPrivileges("root", privilege.DatabasePrivileges, true),
				},
				{
					User: "b",
					Privileges: append(
						NewPrivileges("a", privilege.List{privilege.DROP}, true),
						NewPrivileges("b", privilege.List{privilege.DROP}, true)...,
					),
				},
			},
		},
			"a",
			"b",
			privilege.DROP,
			true,
			&PrivilegeDescriptor{
				Users: []UserPrivileges{
					{
						User:       "a",
						Privileges: NewPrivileges("root", privilege.DatabasePrivileges, true),
					},
					{
						User:       "b",
						Privileges: NewPrivileges("a", privilege.List{privilege.DROP}, false),
					},
				},
			},
		},
		{&PrivilegeDescriptor{
			Users: []UserPrivileges{
				{
					User:       "a",
					Privileges: NewPrivileges("root", privilege.List{privilege.DROP}, true),
				},
				{
					User: "b",
					Privileges: []Privilege{
						{
							Grantor:       "a",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
						{
							Grantor:       "f",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
					},
				},
				{
					User: "c",
					Privileges: []Privilege{
						{
							Grantor:       "b",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
						{
							Grantor:       "e",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
					},
				},
				{
					User: "d",
					Privileges: []Privilege{
						{
							Grantor:       "c",
							PrivilegeType: privilege.DROP,
							GrantAble:     false,
						},
						{
							Grantor:       "f",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
					},
				},
				{
					User: "e",
					Privileges: []Privilege{
						{
							Grantor:       "b",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
						{
							Grantor:       "f",
							PrivilegeType: privilege.DROP,
							GrantAble:     false,
						},
					},
				},
				{
					User: "f",
					Privileges: []Privilege{
						{
							Grantor:       "c",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
						{
							Grantor:       "e",
							PrivilegeType: privilege.DROP,
							GrantAble:     false,
						},
					},
				},
			},
		},
			"c",
			"f",
			privilege.DROP,
			false,
			&PrivilegeDescriptor{
				Users: []UserPrivileges{
					{
						User:       "a",
						Privileges: NewPrivileges("root", privilege.List{privilege.DROP}, true),
					},
					{
						User: "b",
						Privileges: []Privilege{
							{
								Grantor:       "a",
								PrivilegeType: privilege.DROP,
								GrantAble:     true,
							},
						},
					},
					{
						User: "c",
						Privileges: []Privilege{
							{
								Grantor:       "b",
								PrivilegeType: privilege.DROP,
								GrantAble:     true,
							},
							{
								Grantor:       "e",
								PrivilegeType: privilege.DROP,
								GrantAble:     true,
							},
						},
					},
					{
						User: "d",
						Privileges: []Privilege{
							{
								Grantor:       "c",
								PrivilegeType: privilege.DROP,
								GrantAble:     false,
							},
						},
					},
					{
						User: "e",
						Privileges: []Privilege{
							{
								Grantor:       "b",
								PrivilegeType: privilege.DROP,
								GrantAble:     true,
							},
						},
					},
					{
						User: "f",
						Privileges: []Privilege{
							{
								Grantor:       "e",
								PrivilegeType: privilege.DROP,
								GrantAble:     false,
							},
						},
					},
				},
			},
		},
	}

	for tcNum, tc := range testCases {
		grantee, ok := tc.pd.findUser(tc.grantee)
		if !ok {
			t.Fatalf("找不到第 %d 个grantee", tcNum+1)
		}
		find := false
		var grantors []string
		for _, p := range grantee.Privileges {
			if p.Grantor == tc.grantor && p.PrivilegeType == tc.PrivilegeType {
				find = true
				p.GrantAble = tc.grantAble
				tc.pd.revoke(grantee, p, grantors)
				break
			}
		}
		if !find {
			t.Fatalf("第 %d 个用例revoke失败", tcNum+1)
		}
		if grantors != nil {
			t.Fatalf("第 %d 个用例revok时扩大了grantors", tcNum+1)
		}
		if !reflect.DeepEqual(tc.pd, tc.exp) {
			t.Fatalf("第 %d 个用例revoke后期望%v与结果%v不一致", tcNum+1, tc.exp, tc.pd)
		}
	}
}

// TestRevoke verify the HasLoop function. We use a dummy PrivilegeDescriptor
// for testing purposes.
func TestPrivilegeDescriptor_HasLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pd  *PrivilegeDescriptor
		exp bool
	}{
		{&PrivilegeDescriptor{
			Users: []UserPrivileges{
				{
					User:       "a",
					Privileges: NewPrivileges("root", privilege.DatabasePrivileges, true),
				},
				{
					User: "b",
					Privileges: append(
						NewPrivileges("a", privilege.List{privilege.DROP}, true),
						NewPrivileges("b", privilege.List{privilege.DROP}, true)...,
					),
				},
			},
		},
			true,
		},
		{&PrivilegeDescriptor{
			Users: []UserPrivileges{
				{
					User: "a",
					Privileges: append(
						NewPrivileges("root", privilege.DatabasePrivileges, true),
						NewPrivileges("b", privilege.List{privilege.DROP}, true)...,
					),
				},
				{
					User:       "b",
					Privileges: NewPrivileges("a", privilege.DatabasePrivileges, true),
				},
				{
					User: "c",
					Privileges: append(
						NewPrivileges("a", privilege.DatabasePrivileges, false),
						NewPrivileges("b", privilege.List{privilege.DROP}, true)...,
					),
				},
			},
		},
			true,
		},
		{&PrivilegeDescriptor{
			Users: []UserPrivileges{
				{
					User:       "a",
					Privileges: NewPrivileges("root", privilege.DatabasePrivileges, true),
				},
				{
					User:       "b",
					Privileges: NewPrivileges("a", privilege.DatabasePrivileges, true),
				},
				{
					User: "c",
					Privileges: append(
						NewPrivileges("a", privilege.DatabasePrivileges, false),
						NewPrivileges("b", privilege.List{privilege.DROP}, true)...,
					),
				},
			},
		},
			false,
		},
		{&PrivilegeDescriptor{
			Users: []UserPrivileges{
				{
					User:       "a",
					Privileges: NewPrivileges("root", privilege.DatabasePrivileges, true),
				},
				{
					User: "b",
					Privileges: append(
						NewPrivileges("a", privilege.DatabasePrivileges, true),
						NewPrivileges("e", privilege.List{privilege.DROP}, true)...,
					),
				},
				{
					User: "c",
					Privileges: []Privilege{
						{
							Grantor:       "b",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
						{
							Grantor:       "d",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
						{
							Grantor:       "e",
							PrivilegeType: privilege.DROP,
							GrantAble:     true,
						},
					},
				},
				{
					User: "d",
					Privileges: append(
						NewPrivileges("a", privilege.DatabasePrivileges, true),
						NewPrivileges("e", privilege.List{privilege.DROP}, true)...,
					),
				},
				{
					User:       "e",
					Privileges: NewPrivileges("root", privilege.DatabasePrivileges, true),
				},
			},
		},
			false,
		},
	}

	for tcNum, tc := range testCases {
		hasLoop := tc.pd.HasLoop()
		if !reflect.DeepEqual(hasLoop, tc.exp) {
			t.Fatalf("第 %d 个用例期望%v与结果%v不一致", tcNum+1, tc.exp, hasLoop)
		}
	}
}
