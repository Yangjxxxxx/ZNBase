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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/security/useroption"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// alterUserNode represents an ALTER USER ... [WITH] OPTION... statement.
type alterUserNode struct {
	userNameInfo
	ifExists    bool
	userOptions useroption.List

	run alterUserRun
}

// AlterUser changes a user's password.
// Privileges: UPDATE on the users table.
func (p *planner) AlterUser(ctx context.Context, n *tree.AlterUser) (planNode, error) {
	return p.AlterUserNode(ctx, n.Name, n.IfExists, "ALTER USER", n.KVOptions)
}

func (p *planner) AlterUserNode(
	ctx context.Context, nameE tree.Expr, ifExists bool, opName string, kvOptions tree.KVOptions,
) (planNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser.
	// CockroachDB does not support superuser privilege right now.
	// However we make it so the admin role cannot be edited (done in startExec).
	if err := p.CheckUserOption(ctx, useroption.CREATEUSER); err != nil {
		return nil, err
	}

	asStringOrNull := func(e tree.Expr, op string) (func() (bool, string, error), error) {
		return p.TypeAsStringOrNull(ctx, e, op)
	}

	userOptions, err := kvOptions.ToUserOptions(asStringOrNull, opName)
	if err != nil {
		return nil, err
	}
	if err := userOptions.CheckUserOptionConflicts(); err != nil {
		return nil, err
	}

	ua, err := p.getUserAuthInfo(nameE, "ALTER USER")
	if err != nil {
		return nil, err
	}

	return &alterUserNode{
		userNameInfo: ua,
		ifExists:     ifExists,
		userOptions:  userOptions,
	}, nil
}

// alterUserRun is the run-time state of
// alterUserNode for local execution.
type alterUserRun struct {
	rowsAffected int
}

func (n *alterUserNode) startExec(params runParams) error {
	name, err := n.name()
	if err != nil {
		return err
	}
	if name == "" {
		return errNoUserNameSpecified
	}
	if name == "admin" {
		return pgerror.NewErrorf(pgcode.InsufficientPrivilege,
			"cannot edit admin role")
	}
	normalizedUsername, err := NormalizeAndValidateUsername(name)
	if err != nil {
		return err
	}

	// Check if role exists.
	row, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRow(
		params.ctx,
		"alter user",
		params.p.txn,
		fmt.Sprintf("SELECT 1 FROM %s WHERE username = $1", userTableName),
		normalizedUsername)
	if err != nil {
		return err
	}
	if row == nil {
		if n.ifExists {
			return nil
		}
		return errors.Newf("role/user %s does not exist", normalizedUsername)
	}

	if n.userOptions.Contains(useroption.PASSWORD) {
		if normalizedUsername == security.RootUser && params.p.User() != security.RootUser {
			return errors.Errorf("Cannot change password for user %s", security.RootUser)
		}
		isNull, resolvedPassword, err := n.userOptions.GetPassword()
		if err != nil {
			return err
		}
		if !isNull && params.extendedEvalCtx.ExecCfg.RPCContext.Config.Insecure {
			// We disallow setting a non-empty password in insecure mode
			// because insecure means an observer may have MITM'ed the change
			// and learned the password.
			//
			// It's valid to clear the password (WITH PASSWORD NULL) however
			// since that forces cert auth when moving back to secure mode,
			// and certs can't be MITM'ed over the insecure SQL connection.
			return pgerror.Newf(pgcode.InvalidPassword,
				"setting or updating a password is not supported in insecure mode")
		}

		// The root user is not allowed a password.
		//if normalizedUsername == security.RootUser {
		//	return errors.Errorf("user %s cannot use password authentication", security.RootUser)
		//}

		encryption := "BCRYPT"
		if n.userOptions.Contains(useroption.ENCRYPTION) {
			for _, o := range n.userOptions {
				if o.Option == useroption.ENCRYPTION {
					_, encryption, err = o.Value()
					if err != nil {
						return err
					}
				}
			}
		} else {
			rows, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Query(
				params.ctx,
				"get-user-encryption",
				params.p.txn, /* txn */
				`SELECT value FROM system.user_options WHERE username=$1 AND option = 'ENCRYPTION'`,
				normalizedUsername,
			)
			if err != nil {
				return err
			}

			for _, row := range rows {
				if tree.DNull.Compare(nil, row[0]) != 0 {
					encryption = string(tree.MustBeDString(row[0]))
				}
			}
		}

		var hashedPassword []byte
		if !isNull {
			if hashedPassword, err = params.p.checkPasswordAndGetHash(normalizedUsername, resolvedPassword, encryption); err != nil {
				return err
			}
		}

		if hashedPassword == nil {
			hashedPassword = []byte{}
		}

		n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"update-user",
			params.p.txn,
			`UPDATE system.users SET "hashedPassword" = $2 WHERE username = $1 AND "isRole" = false`,
			normalizedUsername,
			hashedPassword,
		)
		if err != nil {
			return err
		}

		if n.run.rowsAffected == 0 && !n.ifExists {
			return errors.Errorf("user %s does not exist", normalizedUsername)
		}
	}

	stmts, err := n.userOptions.GetSQLStmts()
	if err != nil {
		return err
	}

	for stmt, value := range stmts {
		qargs := []interface{}{normalizedUsername}

		if value != nil {
			isNull, val, err := value()
			if err != nil {
				return err
			}
			if isNull {
				// If the value of the role option is NULL, ensure that nil is passed
				// into the statement placeholder, since val is string type "NULL"
				// will not be interpreted as NULL by the InternalExecutor.
				qargs = append(qargs, nil)
			} else {
				qargs = append(qargs, val)
			}
		}

		numAltered, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"update-user",
			params.p.Txn(),
			stmt,
			qargs...)
		if err != nil {
			return err
		}

		if n.run.rowsAffected == 0 && numAltered != 0 {
			n.run.rowsAffected++
		}
	}
	return err
}

func (*alterUserNode) Next(runParams) (bool, error) { return false, nil }
func (*alterUserNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterUserNode) Close(context.Context)        {}

func (n *alterUserNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}

type alterUserDefaultSchemaNode struct {
	userName   string
	dbname     string
	searchPath []string
	values     string
	isDefault  bool
	run        alterUserDefaultSchemaRun
}

type alterUserDefaultSchemaRun struct {
	rowsAffected int
}

func (p *planner) AlterUserDefaultSchma(
	ctx context.Context, n *tree.AlterUserDefaultSchma,
) (planNode, error) {
	tDesc, err := ResolveExistingObject(ctx, p, userTableName, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}

	//获得从yacc层传入的信息
	//去除字符串前后多余的单引号
	userName := n.Name.String()
	userName = userName[1 : len(userName)-1]
	if userName != p.User() {
		if err := p.CheckPrivilege(ctx, tDesc, privilege.UPDATE); err != nil {
			return nil, err
		}
	}

	allUsers, err := p.GetAllUsersAndRoles(ctx)
	if err != nil {
		return nil, err
	}

	//判断用户是否user存在
	if _, ok := allUsers[userName]; !ok {
		return nil, fmt.Errorf(`role "%s" does not exist`, userName)
	}

	schemas := make([]string, 0)
	//获得search_path的字符串列表
	schemasFunc, err := p.TypeAsStringArray(n.Values, "alter user")
	if err != nil {
		return nil, err
	}
	schemas, err = schemasFunc()

	//解决search_path名字中存在字符逗号的问题
	for i, v := range schemas {
		if ok := strings.Contains(v, ","); ok {
			schemas[i] = fmt.Sprintf(`"%s"`, v)
		}
	}

	return &alterUserDefaultSchemaNode{
		userName:   userName,
		dbname:     string(n.DBName),
		searchPath: schemas,
		isDefault:  n.IsDefalut,
		values:     n.Values.String(),
	}, nil
}

func (n *alterUserDefaultSchemaNode) startExec(params runParams) error {
	if n.dbname == "" { //为空则没有指定数据库
		if n.isDefault {
			_, err := params.p.execCfg.InternalExecutor.Exec(params.ctx, "alter-user-searchpath", params.p.Txn(),
				fmt.Sprintf(`update system.users set usersearchpath = ARRAY['%s'] where username = '%s'`,
					"public", n.userName))
			if err != nil {
				return err
			}
		} else {
			_, err := params.p.execCfg.InternalExecutor.Exec(params.ctx, "alter-user-searchpath", params.p.Txn(),
				fmt.Sprintf(`update system.users set usersearchpath = ARRAY[%s] where username = '%s'`,
					n.values, n.userName))
			if err != nil {
				return err
			}
		}
	} else { // 指定修改某个数据库下的search_path
		//从users表中拿出必要的数据
		dbSearchPath, err := params.p.execCfg.InternalExecutor.QueryRow(params.ctx, "select-user", params.p.Txn(),
			fmt.Sprintf(`select dbsearchpath from system.users where username = '%s'`, n.userName))
		if err != nil || len(dbSearchPath) == 0 {
			return fmt.Errorf(fmt.Sprintf("user %s does not exist", n.userName))
		}
		dbID, err := getDatabaseID(params.ctx, params.p.txn, n.dbname, true)
		if err != nil {
			return err
		}
		//dbIDStr 存储dbID字符串形式
		dbIDStr := strconv.FormatUint(uint64(dbID), 10)
		//mapDbSerchPath是每个数据库对应search_path的map
		mapDbSerchPath := make(map[string]interface{})
		if dbSearchPath[0] == tree.DNull || "NULL" == string(tree.MustBeDString(dbSearchPath[0])) { //dbdefaultsearchpath列为空
			if !n.isDefault {
				mapDbSerchPath[dbIDStr] = strings.Join(n.searchPath, ",")
				bytes, err := json.Marshal(mapDbSerchPath)
				if err != nil {
					return err
				}
				jsonstr := string(bytes)
				_, err = params.p.execCfg.InternalExecutor.Exec(params.ctx, "alter-user-searchpath", params.p.Txn(),
					fmt.Sprintf(`update system.users set dbsearchpath = '%s' where username = '%s'`,
						jsonstr, n.userName))
				if err != nil {
					return err
				}
			}
		} else { //dbdefaultsearchpath列不为空，需调出dbdefaultsearchpath的数据，并更改其中对应数据库的值
			if dbSearchPath, ok := dbSearchPath[0].(*tree.DString); ok {
				err := json.Unmarshal([]byte(*dbSearchPath), &mapDbSerchPath)
				if err != nil {
					return err
				}
			}
			if n.isDefault {
				if _, ok := mapDbSerchPath[dbIDStr]; ok {
					delete(mapDbSerchPath, dbIDStr)
				}
			} else {
				mapDbSerchPath[dbIDStr] = strings.Join(n.searchPath, ",")
			}
			if len(mapDbSerchPath) == 0 { //system.users表dbsearchpath列已经为空
				_, err := params.p.execCfg.InternalExecutor.Exec(params.ctx, "alter-user-searchpath", params.p.Txn(),
					fmt.Sprintf(`update system.users set dbsearchpath = '%s' where username = '%s'`,
						"NULL", n.userName))
				if err != nil {
					return err
				}
			} else {
				bytes, err := json.Marshal(mapDbSerchPath)
				if err != nil {
					return fmt.Errorf(fmt.Sprintf(`dbsearchpath Incorrect format at Row %s in system.users, expected like {"1":"public",}`,
						n.userName))
				}
				jsonStr := string(bytes)
				_, err = params.p.execCfg.InternalExecutor.Exec(params.ctx, "alter-user-searchpath", params.p.Txn(),
					fmt.Sprintf(`update system.users set dbsearchpath = '%s' where username = '%s'`,
						jsonStr, n.userName))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (*alterUserDefaultSchemaNode) Next(runParams) (bool, error) { return false, nil }
func (*alterUserDefaultSchemaNode) Values() tree.Datums          { return nil }
func (*alterUserDefaultSchemaNode) Close(context.Context)        {}
func (n *alterUserDefaultSchemaNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
