// Copyright 2016  The Cockroach Authors.
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
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// GetUserHashedPassword returns the hashedPassword for the given username if
// found in system.users.
func GetUserHashedPassword(
	ctx context.Context, ie *InternalExecutor, metrics *MemoryMetrics, username string,
) (bool, []byte, *tree.DTimestamp, string, int64, error) {
	normalizedUsername := tree.Name(username).Normalize()

	const getHashedPassword = `SELECT "hashedPassword" FROM system.users ` +
		`WHERE username=$1 AND "isRole" = false`
	values, err := ie.QueryRow(
		ctx, "get-hashed-pwd", nil /* txn */, getHashedPassword, normalizedUsername)
	if err != nil {
		return false, nil, nil, "", 0, errors.Wrapf(err, "error looking up user %s", normalizedUsername)
	}
	if values == nil {
		return false, nil, nil, "", 0, nil
	}
	hashedPassword := []byte(*(values[0].(*tree.DBytes)))

	getLoginDependencies := `SELECT option, value FROM system.user_options ` +
		`WHERE username=$1 AND option IN ('VALID UNTIL', 'ENCRYPTION', 'CONNECTION LIMIT')`

	loginDependencies, err := ie.Query(
		ctx, "get-login-dependencies", nil /* txn */, getLoginDependencies, normalizedUsername)
	if err != nil {
		return false, nil, nil, "", 0, errors.Wrapf(err, "error looking up user %s", normalizedUsername)
	}

	var validUntil *tree.DTimestamp
	encryption := "BCRYPT"
	var limit int64 = -1
	for _, row := range loginDependencies {
		option := string(tree.MustBeDString(row[0]))

		if option == "VALID UNTIL" {
			if tree.DNull.Compare(nil, row[1]) != 0 {
				ts := string(tree.MustBeDString(row[1]))
				// This is okay because the VALID UNTIL is stored as a string
				// representation of a TimestampTZ which has the same underlying
				// representation in the table as a Timestamp (UTC time).
				timeCtx := tree.NewParseTimeContext(true, timeutil.Now())
				validUntil, err = tree.ParseDTimestamp(timeCtx, ts, time.Microsecond)
				if err != nil {
					return false, nil, nil, "", 0,
						errors.Wrap(err, "error trying to parse timestamp while retrieving password valid until value")
				}
			}
		}

		if option == "ENCRYPTION" {
			if tree.DNull.Compare(nil, row[1]) != 0 {
				encryption = string(tree.MustBeDString(row[1]))
			}
		}

		if option == "CONNECTION LIMIT" {
			if tree.DNull.Compare(nil, row[1]) != 0 {
				limit = int64(tree.MustBeDInt(row[1]))
			}
		}
	}

	return true, hashedPassword, validUntil, encryption, limit, nil
}

// The map value is true if the map key is a role, false if it is a user.
func (p *planner) GetAllUsersAndRoles(ctx context.Context) (map[string]bool, error) {
	query := `SELECT username,"isRole"  FROM system.users`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-users", p.txn, query)
	if err != nil {
		return nil, err
	}

	users := make(map[string]bool)
	for _, row := range rows {
		username := tree.MustBeDString(row[0])
		isRole := row[1].(*tree.DBool)
		users[string(username)] = bool(*isRole)
	}
	return users, nil
}

var roleMembersTableName = tree.MakeTableName("system", "role_members")

// BumpRoleMembershipTableVersion increases the table version for the
// role membership table.
func (p *planner) BumpRoleMembershipTableVersion(ctx context.Context) error {
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &roleMembersTableName, true, anyDescType)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
}

const lockDurationString = "+10m"

// ApplyAuthPermit apply a auth permit to system.authentication.Return permit and the remaining time(second).
func ApplyAuthPermit(
	ctx context.Context, ie *InternalExecutor, metrics *MemoryMetrics, username string,
) (bool, int, error) {
	const (
		selectAuthFail       = `SELECT "lockTime", "enable" FROM system.authentication WHERE "username" = $1`
		selectAuthFailOpName = "select-auth-fail"
		selectAuthFailErrMsg = "error looking up user %s in system.authentication"
	)
	normalizedUsername := tree.Name(username).Normalize()
	values, err := ie.QueryRow(ctx, selectAuthFailOpName, nil /* txn */, selectAuthFail, normalizedUsername)
	if err != nil {
		return false, 0, errors.Wrapf(err, selectAuthFailErrMsg, normalizedUsername)
	}
	//The user has not authenticated yet.
	if values == nil {
		return true, 0, nil
	}
	tempLockTime, ok := values[0].(*tree.DTimestamp)
	if !ok {
		//values[0] must be a dNull.The user has not auth fail yet.
		return true, 0, nil
	}
	lockTime := *tempLockTime
	enable := *(values[1].(*tree.DBool))
	if !enable {
		return true, 0, nil
	}
	lockDuration, err := time.ParseDuration(lockDurationString)
	if err != nil {
		return false, 0, errors.Wrapf(err, "time count error when %s try auth", normalizedUsername)
	}
	expectTime := lockTime.Add(lockDuration)
	nowTime := timeutil.Now()
	if expectTime.Before(nowTime) {
		return true, 0, nil
	}
	remainingDuration := expectTime.Sub(nowTime)
	return false, int(math.Ceil(remainingDuration.Seconds())), nil
	//never return from here
	//return false, 0, nil
}

// RecordUserAuthFail record a failed auth to system.authentication.
func RecordUserAuthFail(
	ctx context.Context, ie *InternalExecutor, metrics *MemoryMetrics, username string,
) error {
	const (
		selectAuthFail       = `SELECT "failureCount", "maxTryCount", "enable" FROM system.authentication WHERE "username" = $1`
		selectAuthFailOpName = "select-auth-fail"
		selectAuthFailErrMsg = "error looking up user %s in system.authentication"

		insertAuthFail       = `INSERT INTO system.authentication ("username", "failureCount") VALUES ($1, 1)`
		insertAuthFailOpName = "insert-auth-fail"
		insertAuthFailErrMsg = "error insert user %s in system.authentication"

		updateAuthFail1      = `UPDATE system.authentication SET "failureCount" = $1 WHERE "username" = $2`
		updateAuthFail2      = `UPDATE system.authentication SET "failureCount" = $1, "lockTime" = now() WHERE "username" = $2`
		updateAuthFailOpName = "update-auth-fail"
		updateAuthFailErrMsg = "error update user %s in system.authentication"
	)
	normalizedUsername := tree.Name(username).Normalize()
	values, err := ie.QueryRow(ctx, selectAuthFailOpName, nil /* txn */, selectAuthFail, normalizedUsername)
	if err != nil {
		return errors.Wrapf(err, selectAuthFailErrMsg, normalizedUsername)
	}
	if values == nil {
		_, err := ie.Exec(ctx, insertAuthFailOpName, nil /* txn */, insertAuthFail, normalizedUsername)
		if err != nil {
			return errors.Wrapf(err, insertAuthFailErrMsg, normalizedUsername)
		}
	} else {
		failureCount := *(values[0].(*tree.DInt))
		maxTryCount := *(values[1].(*tree.DInt))
		enable := *(values[2].(*tree.DBool))
		if !enable {
			return nil
		}
		failureCount++
		if failureCount < maxTryCount {
			_, err := ie.Exec(ctx, updateAuthFailOpName, nil /* txn */, updateAuthFail1, failureCount, normalizedUsername)
			if err != nil {
				return errors.Wrapf(err, updateAuthFailErrMsg, normalizedUsername)
			}
		} else {
			_, err := ie.Exec(ctx, updateAuthFailOpName, nil /* txn */, updateAuthFail2, failureCount, normalizedUsername)
			if err != nil {
				return errors.Wrapf(err, updateAuthFailErrMsg, normalizedUsername)
			}
		}
	}
	return nil
}

// ResetUserAuthFail reset failureCount to 0 in system.authentication.
func ResetUserAuthFail(
	ctx context.Context, ie *InternalExecutor, metrics *MemoryMetrics, username string,
) error {
	const (
		updateAuthFail       = `UPDATE system.authentication SET "failureCount" = 0 WHERE "username" = $1 AND "failureCount" > 0`
		updateAuthFailOpName = "update-auth-fail"
		updateAuthFailErrMsg = "error update user %s in system.authentication"
	)
	normalizedUsername := tree.Name(username).Normalize()
	_, err := ie.Exec(ctx, updateAuthFailOpName, nil /* txn */, updateAuthFail, normalizedUsername)
	if err != nil {
		return errors.Wrapf(err, updateAuthFailErrMsg, normalizedUsername)
	}
	return nil
}

// CheckAccessPrivilege check the user and the user's role's access privilege
func CheckAccessPrivilege(
	ctx context.Context, txn *client.Txn, user string, dbDesc *DatabaseDescriptor,
) error {
	priv := dbDesc.Privileges
	dbName := dbDesc.Name
	if priv.CheckAccessPrivilege(user) {
		return nil
	}

	if priv.CheckAccessPrivilege(sqlbase.PublicRole) {
		return nil
	}

	memberOf, err := MemberOfWithAdminOption(ctx, txn, user)
	if err != nil {
		return err
	}

	// Iterate over the roles that 'user' is a member of. We don't care about the admin option.
	for role := range memberOf {
		if priv.CheckAccessPrivilege(role) {
			return nil
		}
	}

	return sqlbase.NewNoAccessDBPrivilegeError(user, dbName)
}

// MemberOfWithAdminOption is like planner.MemberOfWithAdminOption without planner (no SQL query but ScanRequest)
func MemberOfWithAdminOption(
	ctx context.Context, txn *client.Txn, member string,
) (map[string]bool, error) {
	roleMemberList := membershipCache{}
	roleMemberList.userCache = make(map[string]userRoleMembership)
	// Lookup memberships
	memberships, err := resolveMemberOfWithAdminOption(ctx, txn, member)
	if err != nil {
		return nil, err
	}
	return memberships, nil
}

type roleMembersElements struct {
	role   string
	member string
	// we don't need isAdmin exactly, it is always false
	isAdmin bool
}

func resolveMemberOfWithAdminOption(
	ctx context.Context, txn *client.Txn, member string,
) (map[string]bool, error) {
	var startKey, endKey []byte
	ret := map[string]bool{}

	// Table role_member's ID as StartKey
	startKey = encoding.EncodeUvarintAscending(startKey, uint64(keys.RoleMembersTableID))
	endKey = encoding.EncodeUvarintAscending(startKey, uint64(keys.RoleMembersTableID+1))
	span := roachpb.Span{Key: startKey, EndKey: endKey}

	// Make a ScanRequest to get the elements of the table role_member
	var ba client.Batch
	ba.AddRawRequest(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeaderFromSpan(span)})
	err := txn.Run(ctx, &ba)
	if err != nil {
		return nil, err
	}

	var elements []roleMembersElements
	kvs := ba.RawResponse().Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	for _, kv := range kvs {
		// get the tableID, ignore it
		remaining, _, err := encoding.DecodeUvarintAscending(kv.Key)
		if err != nil {
			return nil, err
		}

		// get the primary key id (1), ignore it
		remaining, _, err = encoding.DecodeUvarintAscending(remaining)
		if err != nil {
			return nil, err
		}
		// get the roleName
		remaining, role, err := encoding.DecodeUnsafeStringAscending(remaining, nil)
		if err != nil {
			return nil, err
		}
		// get the userName
		_, userName, err := encoding.DecodeUnsafeStringAscending(remaining, nil)
		if err != nil {
			return nil, err
		}
		if userName == member {
			elements = append(elements, roleMembersElements{role, userName, false})
		}
	}

	visited := map[string]struct{}{}
	toVisit := []string{member}

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		for _, element := range elements {
			ret[string(element.role)] = element.isAdmin

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, element.role)
		}
	}
	return ret, nil
}
