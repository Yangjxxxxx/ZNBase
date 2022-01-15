// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import (
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
)

// Grant represents a GRANT statement.
type Grant struct {
	Privileges PrivilegeGroups
	Targets    TargetList
	Grantees   NameList
	GrantAble  bool
}

// TargetList represents a list of targets.
// Only one field may be non-nil.
type TargetList struct {
	Databases NameList
	Schemas   SchemaList
	Tables    TablePatterns
	Views     TablePatterns
	Sequences TablePatterns
	Columns   NameList
	Function  Function

	// ForRoles and Roles are used internally in the parser and not used
	// in the AST. Therefore they do not participate in pretty-printing,
	// etc.
	ForRoles bool
	Roles    NameList
}

// Format implements the NodeFormatter interface.
func (tl *TargetList) Format(ctx *FmtCtx) {
	if tl.Databases != nil {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&tl.Databases)
	} else if tl.Schemas != nil {
		ctx.WriteString("SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.Tables != nil {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(&tl.Tables)
	} else if tl.Views != nil { // todo(xz): column on view and sequence?
		ctx.WriteString("VIEW ")
		ctx.FormatNode(&tl.Views)
	} else if tl.Sequences != nil {
		ctx.WriteString("SEQUENCE ")
		ctx.FormatNode(&tl.Sequences)
	} else {
		ctx.WriteString("FUNCTION ")
		ctx.FormatNode(&tl.Function)
	}
}

// Format implements the NodeFormatter interface.
func (node *Grant) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	ctx.FormatNode(&node.Privileges)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.Targets)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.Grantees)
	if node.GrantAble {
		ctx.WriteString(" WITH GRANT OPTION")
	}
}

// GrantRole represents a GRANT <role> statement.
type GrantRole struct {
	Roles       NameList
	Members     NameList
	AdminOption bool
	GrantAble   bool
}

// Format implements the NodeFormatter interface.
func (node *GrantRole) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	ctx.FormatNode(&node.Roles)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.Members)
	if node.GrantAble {
		ctx.WriteString(" WITH GRANT OPTION")
	}
	if node.AdminOption {
		ctx.WriteString(" WITH ADMIN OPTION")
	}
}

// PrivilegeGroup represents privilege and columns.
type PrivilegeGroup struct {
	Privilege privilege.Kind
	Columns   NameList
}

// Format implements the NodeFormatter interface.
func (node *PrivilegeGroup) Format(ctx *FmtCtx) {
	node.Privilege.Format(&ctx.Buffer)
	if node.Columns != nil {
		ctx.WriteString(" ( ")
		ctx.FormatNode(&node.Columns)
		ctx.WriteString(" ) ")
	}
}

// PrivilegeGroups is a list of PrivilegeGroup.
type PrivilegeGroups []PrivilegeGroup

// Format implements the NodeFormatter interface.
func (pl *PrivilegeGroups) Format(ctx *FmtCtx) {
	for i := range *pl {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*pl)[i])
	}
}

// ValidatePrivileges returns an error if any privilege in
// privileges cannot be granted on the given objectType.
// Currently db/schema/table can all be granted the same privileges.
func ValidatePrivileges(pGroups PrivilegeGroups, objectType privilege.ObjectType) error {
	validPrivs := privilege.GetValidPrivilegesForObject(objectType)
	var validColumnPrivs privilege.List
	if objectType == privilege.Table {
		validColumnPrivs = privilege.GetValidPrivilegesForObject(privilege.Column)
	}
	for _, pgroup := range pGroups {
		if pgroup.Privilege != privilege.ALL {
			if validPrivs.ToBitField()&pgroup.Privilege.Mask() == 0 {
				return pgerror.Newf(pgcode.InvalidGrantOperation,
					"invalid privilege type %s for %s", pgroup.Privilege.String(), objectType)
			}
			if objectType == privilege.Table && pgroup.Columns != nil {
				if validColumnPrivs.ToBitField()&pgroup.Privilege.Mask() == 0 {
					return pgerror.Newf(pgcode.InvalidGrantOperation,
						"invalid privilege type %s for table column", pgroup.Privilege.String())
				}
			}
		}
	}

	return nil
}
