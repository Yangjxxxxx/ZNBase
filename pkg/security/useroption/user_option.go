package useroption

import (
	"strconv"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
)

//go:generate stringer -type=Option

// Option defines a user option. This is output by the parser
type Option uint32

// UserOption represents an Option with a value.
type UserOption struct {
	Option
	HasValue bool
	// Need to resolve value in Exec for the case of placeholders.
	Value func() (bool, string, error)
}

// KindList of user options.
const (
	_ Option = iota
	CREATEUSER
	NOCREATEUSER
	PASSWORD
	VALIDUNTIL
	ENCRYPTION
	CONNECTIONLIMIT
	CSTATUS
	NOCSTATUS
)

// toSQLStmts is a map of Kind -> SQL statement string for applying the
// option to the user.
var toSQLStmts = map[Option]string{
	CREATEUSER:      `UPSERT INTO system.user_options (username, option) VALUES ($1, 'CREATEUSER')`,
	NOCREATEUSER:    `DELETE FROM system.user_options WHERE username = $1 AND option = 'CREATEUSER'`,
	VALIDUNTIL:      `UPSERT INTO system.user_options (username, option, value) VALUES ($1, 'VALID UNTIL', $2::timestamptz::string)`,
	ENCRYPTION:      `UPSERT INTO system.user_options (username, option, value) VALUES ($1, 'ENCRYPTION', $2)`,
	CONNECTIONLIMIT: `UPSERT INTO system.user_options (userName, option, value) VALUES ($1, 'CONNECTION LIMIT', $2)`,
	CSTATUS:         `UPSERT INTO system.user_options (username, option) VALUES ($1, 'CSTATUS')`,
	NOCSTATUS:       `DELETE FROM system.user_options WHERE username = $1 AND option = 'CSTATUS'`,
}

// Mask returns the bitmask for a given user option.
func (o Option) Mask() uint32 {
	return 1 << o
}

// ByName is a map of string -> kind value.
var ByName = map[string]Option{
	"CREATEUSER":       CREATEUSER,
	"NOCREATEUSER":     NOCREATEUSER,
	"PASSWORD":         PASSWORD,
	"VALID_UNTIL":      VALIDUNTIL,
	"ENCRYPTION":       ENCRYPTION,
	"CONNECTION_LIMIT": CONNECTIONLIMIT,
	"CSTATUS":          CSTATUS,
	"NOCSTATUS":        NOCSTATUS,
}

// ToOption takes a string and returns the corresponding Option.
func ToOption(str string) (Option, error) {
	ret := ByName[strings.ToUpper(str)]
	if ret == 0 {
		return 0, pgerror.Newf(pgcode.Syntax, "unrecognized user option %s", str)
	}
	return ret, nil
}

// List is a list of user options.
type List []UserOption

// GetSQLStmts returns a map of SQL stmts to apply each user option.
// Maps stmts to values (value of the user option).
func (l List) GetSQLStmts() (map[string]func() (bool, string, error), error) {
	if len(l) <= 0 {
		return nil, nil
	}

	stmts := make(map[string]func() (bool, string, error), len(l))

	err := l.CheckUserOptionConflicts()
	if err != nil {
		return stmts, err
	}

	for _, u := range l {
		// Skip PASSWORD option.
		// Since PASSWORD still resides in system.users, we handle setting PASSWORD
		// outside of this set stmt.
		if u.Option == PASSWORD {
			continue
		}

		// default value don't record in table system.user_options
		// BCRYPT for ENCRYPTION and -1 for CONNECTIONLIMIT
		if u.Option == ENCRYPTION {
			isNull, value, err := u.Value()
			if err != nil {
				return nil, err
			}
			if !isNull && value == "BCRYPT" {
				stmts[`DELETE FROM system.user_options WHERE username = $1 AND option = 'ENCRYPTION'`] = nil
				continue
			}
		}

		if u.Option == CONNECTIONLIMIT {
			isNull, value, err := u.Value()
			if err != nil {
				return nil, err
			}
			if !isNull {
				i, err := strconv.Atoi(value)
				if err != nil {
					return nil, err
				}
				if i == -1 {
					stmts[`DELETE FROM system.user_options WHERE username = $1 AND option = 'CONNECTION LIMIT'`] = nil
					continue
				}
			}
		}

		stmt := toSQLStmts[u.Option]
		if u.HasValue {
			stmts[stmt] = u.Value
		} else {
			stmts[stmt] = nil
		}
	}
	return stmts, nil
}

// ToBitField returns the bitfield representation of
// a list of user options.
func (l List) ToBitField() (uint32, error) {
	var ret uint32
	for _, p := range l {
		if ret&p.Option.Mask() != 0 {
			return 0, pgerror.Newf(pgcode.Syntax, "redundant user options")
		}
		ret |= p.Option.Mask()
	}
	return ret, nil
}

// Contains returns true if List contains option, false otherwise.
func (l List) Contains(p Option) bool {
	for _, u := range l {
		if u.Option == p {
			return true
		}
	}
	return false
}

// CheckUserOptionConflicts returns an error if two or more options conflict with each other.
func (l List) CheckUserOptionConflicts() error {
	userOptionBits, err := l.ToBitField()
	if err != nil {
		return err
	}

	if userOptionBits&CREATEUSER.Mask() != 0 &&
		userOptionBits&NOCREATEUSER.Mask() != 0 {
		return pgerror.Newf(pgcode.Syntax, "conflicting user options")
	}

	if userOptionBits&CSTATUS.Mask() != 0 &&
		userOptionBits&NOCSTATUS.Mask() != 0 {
		return pgerror.Newf(pgcode.Syntax, "conflicting user options")
	}

	if userOptionBits&PASSWORD.Mask() == 0 &&
		userOptionBits&ENCRYPTION.Mask() != 0 {
		return pgerror.Newf(pgcode.Syntax, "password is required")
	}
	return nil
}

// GetPassword returns the value of the password or whether the
// password was set to NULL. Returns error if the string was invalid
// or if no password option is found.
func (l List) GetPassword() (isNull bool, password string, err error) {
	for _, u := range l {
		if u.Option == PASSWORD {
			return u.Value()
		}
	}
	// Password option not found.
	return false, "", errors.New("password not found in user options")
}
