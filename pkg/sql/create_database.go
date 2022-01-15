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
	"strings"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type createDatabaseNode struct {
	n *tree.CreateDatabase
}

// CreateDatabase creates a database.
// Privileges: superuser.
//   Notes: postgres requires superuser or "CREATEDB".
//          mysql uses the mysqladmin command.
func (p *planner) CreateDatabase(ctx context.Context, n *tree.CreateDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if tmpl := n.Template; tmpl != "" {
		// See https://www.postgresql.org/docs/current/static/manage-ag-templatedbs.html
		if !strings.EqualFold(tmpl, "template0") {
			return nil, fmt.Errorf("unsupported template: %s", tmpl)
		}
	}

	if enc := n.Encoding; enc != "" {
		// We only support UTF8 (and aliases for UTF8).
		if !(strings.EqualFold(enc, "UTF8") ||
			strings.EqualFold(enc, "UTF-8") ||
			strings.EqualFold(enc, "UNICODE")) {
			return nil, fmt.Errorf("unsupported encoding: %s", enc)
		}
	}

	if col := n.Collate; col != "" {
		// We only support C and C.UTF-8.
		if col != "C" && col != "C.UTF-8" {
			return nil, fmt.Errorf("unsupported collation: %s", col)
		}
	}

	if ctype := n.CType; ctype != "" {
		// We only support C and C.UTF-8.
		if ctype != "C" && ctype != "C.UTF-8" {
			return nil, fmt.Errorf("unsupported character classification: %s", ctype)
		}
	}

	if err := p.RequireAdminRole(ctx, "CREATE DATABASE"); err != nil {
		return nil, err
	}

	return &createDatabaseNode{n: n}, nil
}

func (n *createDatabaseNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p

	desc, created, err := p.createDatabase(ctx, n.n)
	if err != nil {
		return err
	}
	if created {
		// todo(xz): change to create public schema
		scDesc := sqlbase.SchemaDescriptor{
			ParentID:   desc.ID,
			Name:       "public",
			Privileges: sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Schema, p.User()),
		}
		// todo(xz): don't use SetDefaultPrivilege
		//desc.Privileges.SetDefaultPrivilege(params.p.User(), sqlbase.DatabasePrivileges)
		// todo(xz): no need grant privilege to public
		if desc.Name != sqlbase.SystemDB.Name && desc.Name != sessiondata.PgDatabaseName {
			scDesc.Privileges.Grant(security.RootUser, sqlbase.PublicRole, privilege.List{privilege.USAGE}, false)
		}
		created, err := p.createSchema(ctx, &scDesc, true)
		if err != nil {
			return err
		}
		if created {
			b := &client.Batch{}
			desc.Schemas = append(desc.Schemas, scDesc)
			if err := p.UpdateDescriptor(ctx, b, desc); err != nil {
				return err
			}
			if err := p.txn.Run(ctx, b); err != nil {
				return err
			}
		}

		// Log Create Database event. This is an auditable log event and is
		// recorded in the same transaction as the table descriptor update.
		// some audit data
		params.p.curPlan.auditInfo = &server.AuditInfo{
			EventTime: timeutil.Now(),
			EventType: string(EventLogCreateDatabase),
			TargetInfo: &server.TargetInfo{
				TargetID: int32(desc.ID),
				Desc: struct {
					DatabaseName string
				}{
					n.n.Name.String(),
				},
			},
			Info: &infos.CreateDatabaseInfo{
				DatabaseName: n.n.Name.String(),
				Statement:    n.n.String(),
				User:         params.SessionData().User,
			},
		}
		params.extendedEvalCtx.Tables.addUncommittedSchema(scDesc.Name, scDesc.ID, desc.Name, desc.ID, dbCreated)
	}
	return nil
}

func (*createDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (*createDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (*createDatabaseNode) Close(context.Context)        {}
