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
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/useroption"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// CreateUserNode creates entries in the system.users table.
// This is called from CREATE USER and CREATE ROLE.
type CreateUserNode struct {
	ifNotExists bool
	isRole      bool
	userOptions useroption.List
	userNameInfo

	run createUserRun
}

var userTableName = tree.NewTableName("system", "users")

// CreateUser creates a user.
// Privileges: INSERT on system.users.
//   notes: postgres allows the creation of users with an empty password. We do
//          as well, but disallow password authentication for these users.
func (p *planner) CreateUser(ctx context.Context, n *tree.CreateUser) (planNode, error) {
	return p.CreateUserNode(ctx, n.Name, n.IfNotExists, false /* isRole */, "CREATE USER", n.KVOptions)
}

// CreateUserNode creates a "create user" plan node. This can be called from CREATE USER or CREATE ROLE.
func (p *planner) CreateUserNode(
	ctx context.Context,
	nameE tree.Expr,
	ifNotExists bool,
	isRole bool,
	opName string,
	kvOptions tree.KVOptions,
) (*CreateUserNode, error) {
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

	ua, err := p.getUserAuthInfo(nameE, opName)
	if err != nil {
		return nil, err
	}

	return &CreateUserNode{
		userNameInfo: ua,
		ifNotExists:  ifNotExists,
		isRole:       isRole,
		userOptions:  userOptions,
	}, nil
}

func (n *CreateUserNode) startExec(params runParams) error {
	normalizedUsername, err := n.userNameInfo.resolveUsername()
	if err != nil {
		return err
	}
	// Reject the "public" role. It does not have an entry in the users table but is reserved.
	if normalizedUsername == sqlbase.PublicRole {
		return pgerror.Newf(pgcode.ReservedName, "role name %q is reserved", sqlbase.PublicRole)
	}

	var hashedPassword []byte
	if n.userOptions.Contains(useroption.PASSWORD) {
		isNull, password, err := n.userOptions.GetPassword()
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

		encryption := "BCRYPT"
		for _, o := range n.userOptions {
			if o.Option == useroption.ENCRYPTION {
				_, encryption, err = o.Value()
				if err != nil {
					return err
				}
			}
		}

		if !isNull {
			if hashedPassword, err = params.p.checkPasswordAndGetHash(normalizedUsername, password, encryption); err != nil {
				return err
			}
		}
	}

	var opName string
	if n.isRole {
		opName = "create-role"
	} else {
		opName = "create-user"
	}

	// Check if the user/role exists.
	row, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRow(
		params.ctx,
		opName,
		params.p.txn,
		`select "isRole" from system.users where username = $1`,
		normalizedUsername,
	)
	if err != nil {
		return errors.Wrapf(err, "error looking up user")
	}
	if row != nil {
		isRole := bool(*row[0].(*tree.DBool))
		if isRole == n.isRole && n.ifNotExists {
			// The username exists with the same role setting, and we asked to skip
			// if it exists: no error.
			return nil
		}
		msg := "a user"
		if isRole {
			msg = "a role"
		}
		return pgerror.NewErrorf(pgcode.DuplicateObject,
			"%s named %s already exists",
			msg, normalizedUsername)
	}

	n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		opName,
		params.p.txn,
		"insert into system.users values ($1, $2, $3)",
		normalizedUsername,
		hashedPassword,
		n.isRole,
	)
	if err != nil {
		return err
	} else if n.run.rowsAffected != 1 {
		return pgerror.NewAssertionErrorf("%d rows affected by user creation; expected exactly one row affected",
			n.run.rowsAffected,
		)
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

		_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			opName,
			params.p.Txn(),
			stmt,
			qargs...)
		if err != nil {
			return err
		}
	}

	_, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"insert-user-auth",
		params.p.txn,
		`UPSERT INTO system.authentication ("username", "failureCount") VALUES ($1, 0)`,
		normalizedUsername,
	)
	if err != nil {
		return err
	}

	return nil
}

type createUserRun struct {
	rowsAffected int
}

// Next implements the planNode interface.
func (*CreateUserNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*CreateUserNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*CreateUserNode) Close(context.Context) {}

// FastPathResults implements the planNodeFastPath interface.
func (n *CreateUserNode) FastPathResults() (int, bool) { return n.run.rowsAffected, true }

const usernameHelp = "usernames are case insensitive, must start with a letter " +
	"or underscore, may contain letters, digits, dashes, or underscores, and must not exceed 63 characters"

const passwordSettingHelp = "the maximum length of the passwords cannot be less than the minimum length," +
	" and the maximum or minimum length cannot be less than" +
	" the total length of the following settings: capital letters, lowercase letters, digits, and symbol characters"

var usernameRE = regexp.MustCompile(`^[\p{Ll}_][\p{Ll}0-9_-]{0,62}$`)

var blacklistedUsernames = map[string]struct{}{
	security.NodeUser: {},
}

// NormalizeAndValidateUsername case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
// It rejects reserved user names.
func NormalizeAndValidateUsername(username string) (string, error) {
	username, err := NormalizeAndValidateUsernameNoBlacklist(username)
	if err != nil {
		return "", err
	}
	if _, ok := blacklistedUsernames[username]; ok {
		return "", errors.Errorf("username %q reserved", username)
	}
	return username, nil
}

// NormalizeAndValidateUsernameNoBlacklist case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
func NormalizeAndValidateUsernameNoBlacklist(username string) (string, error) {
	username = tree.Name(username).Normalize()
	if !usernameRE.MatchString(username) {
		return "", errors.Errorf("username %q invalid; %s", username, usernameHelp)
	}
	return username, nil
}

var errNoUserNameSpecified = errors.New("no username specified")

type userNameInfo struct {
	name func() (string, error)
}

func (p *planner) getUserAuthInfo(nameE tree.Expr, ctx string) (userNameInfo, error) {
	name, err := p.TypeAsString(nameE, ctx)
	if err != nil {
		return userNameInfo{}, err
	}
	return userNameInfo{name: name}, nil
}

// resolveUsername returns the actual user name.
func (ua *userNameInfo) resolveUsername() (string, error) {
	name, err := ua.name()
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", errNoUserNameSpecified
	}
	normalizedUsername, err := NormalizeAndValidateUsername(name)
	if err != nil {
		return "", err
	}

	return normalizedUsername, nil
}

func (p *planner) checkPasswordAndGetHash(
	name string, password string, encryption string,
) (hashedPassword []byte, err error) {
	if password == "" {
		return hashedPassword, security.ErrEmptyPassword
	}
	st := p.ExecCfg().Settings
	r := []rune(password)

	maxLength, minLength := security.MaxPasswordLength.Get(&st.SV), security.MinPasswordLength.Get(&st.SV)
	mixedCase, number, special :=
		security.MixedCaseCount.Get(&st.SV), security.NumberCount.Get(&st.SV), security.SpecialCharCount.Get(&st.SV)
	checkUserName := security.CheckUserName.Get(&st.SV)
	var passwordHelp bytes.Buffer
	passwordError := false

	if maxLength == -1 {
		if int64(len(r)) < minLength {
			passwordError = true
		}
		fmt.Fprintf(&passwordHelp, `passwords must be longer than %d`, minLength)
	} else {
		if int64(len(r)) < minLength || int64(len(r)) > maxLength {
			passwordError = true
		}
		fmt.Fprintf(&passwordHelp, `passwords must be longer than %d and shorter than %d`, minLength, maxLength)
	}

	if !checkComplex(password, mixedCase, number, special) {
		passwordError = true
	}
	fmt.Fprintf(&passwordHelp, `, at least contain %d capital letters, %d lowercase letters, %d digits, and %d symbol characters`,
		mixedCase, mixedCase, number, special)

	if checkUserName {
		if containOrReverse(name, password) {
			passwordError = true
		}
		fmt.Fprintf(&passwordHelp, `, and must not contain username or its reverse`)
	}

	if passwordError {
		return hashedPassword, errors.Errorf(passwordHelp.String())
	}

	hashedPassword, err = security.HashPassword(password, encryption)
	if err != nil {
		return hashedPassword, err
	}

	return hashedPassword, nil
}

func containOrReverse(name, password string) bool {
	password = strings.ToLower(password)
	if strings.Contains(password, name) {
		return true
	}
	n := []rune(name)
	r := []rune(name)
	length := len(n)
	for i := 0; i < length; i++ {
		r[i] = n[length-i-1]
	}
	if strings.Contains(password, string(r)) {
		return true
	}
	return false
}

func checkComplex(password string, caseCount int64, numberCount int64, charCount int64) bool {
	var upperCase, lowerCase, number, specialChar int64
	for _, s := range password {
		if strings.ToUpper(string(s)) != string(s) {
			lowerCase++
		}
		if strings.ToLower(string(s)) != string(s) {
			upperCase++
		}
		if regexp.MustCompile(`\d`).MatchString(string(s)) {
			number++
		}
		if !regexp.MustCompile(`^[a-zA-Z0-9]$`).MatchString(string(s)) {
			specialChar++
		}
	}

	if upperCase < caseCount || lowerCase < caseCount || number < numberCount || specialChar < charCount {
		return false
	}
	return true
}

// checkPasswordSetting check constraints among cluster settings about password length.
func (p *planner) checkPasswordSetting(name string, value int64) error {
	st := p.ExecCfg().Settings

	mixedCase, number, special :=
		security.MixedCaseCount.Get(&st.SV), security.NumberCount.Get(&st.SV), security.SpecialCharCount.Get(&st.SV)

	min, max := security.MinPasswordLength.Get(&st.SV), security.MaxPasswordLength.Get(&st.SV)
	total := mixedCase*2 + number + special

	switch name {
	case "password.validate.max_length":
		if value == -1 {
			return nil
		} else if value < min || value < total {
			return errors.Errorf(passwordSettingHelp)
		}
	case "password.validate.min_length":
		if max == -1 {
			if value < total {
				return errors.Errorf(passwordSettingHelp)
			}
		} else if value > max || value < total {
			return errors.Errorf(passwordSettingHelp)
		}
	case "password.validate.mixed_case_count":
		total = value*2 + number + special
		if max == -1 {
			if total > min {
				return errors.Errorf(passwordSettingHelp)
			}
		} else if total > max || total > min {
			return errors.Errorf(passwordSettingHelp)
		}
	case "password.validate.number_count":
		total = mixedCase*2 + value + special
		if max == -1 {
			if total > min {
				return errors.Errorf(passwordSettingHelp)
			}
		} else if total > max || total > min {
			return errors.Errorf(passwordSettingHelp)
		}
	case "password.validate.special_char_count":
		total = mixedCase*2 + number + value
		if max == -1 {
			if total > min {
				return errors.Errorf(passwordSettingHelp)
			}
		} else if total > max || total > min {
			return errors.Errorf(passwordSettingHelp)
		}
	}

	return nil
}
