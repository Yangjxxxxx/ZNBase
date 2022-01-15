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

package security

import (
	"bytes"
	crypto_rand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/tjfoc/gmsm/sm3"
	"github.com/znbasedb/znbase/pkg/settings"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/crypto/ssh/terminal"
)

// BcryptCost is the cost to use when hashing passwords. It is exposed for
// testing.
//
// BcryptCost should increase along with computation power.
// For estimates, see: http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
// For now, we use the library's default cost.
var BcryptCost = bcrypt.DefaultCost

// ErrEmptyPassword indicates that an empty password was attempted to be set.
var ErrEmptyPassword = errors.New("empty passwords are not permitted")

// CompareHashAndPassword tests that the provided bytes are equivalent to the
// hash of the supplied password. If they are not equivalent, returns an
// error.
func CompareHashAndPassword(encryption string, hashedPassword []byte, password string) error {
	if encryption == "SM3" {
		return compareSM3AndPassword(hashedPassword, password)
	} else if encryption == "PBKDF2" {
		return comparePBKDF2AndPassword(hashedPassword, password)
	} else {
		h := sha256.New()
		// TODO(benesch): properly apply SHA-256 to the password. The current code
		// erroneously appends the SHA-256 of the empty hash to the unhashed password
		// instead of actually hashing the password. Fixing this requires a somewhat
		// complicated backwards compatibility dance. This is not a security issue
		// because the round of SHA-256 was only intended to achieve a fixed-length
		// input to bcrypt; it is bcrypt that provides the cryptographic security, and
		// bcrypt is correctly applied.
		//
		//lint:ignore HC1000 backwards compatibility
		return bcrypt.CompareHashAndPassword(hashedPassword, h.Sum([]byte(password)))
	}
}

// HashPassword takes a raw password and returns a bcrypt hashed password.
func HashPassword(password string, encryption string) ([]byte, error) {
	if encryption == "SM3" {
		return hashPasswordFromSM3(password)
	} else if encryption == "PBKDF2" {
		return hashPasswordFromPBKDF2(password)
	} else {
		h := sha256.New()
		//lint:ignore HC1000 backwards compatibility (see CompareHashAndPassword)
		return bcrypt.GenerateFromPassword(h.Sum([]byte(password)), BcryptCost)
	}
}

// PromptForPassword prompts for a password.
// This is meant to be used when using a password.
func PromptForPassword() (string, error) {
	fmt.Print("Enter password: ")
	password, err := terminal.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	// Make sure stdout moves on to the next line.
	fmt.Print("\n")

	return string(password), nil
}

// PromptForPasswordTwice prompts for a password twice, returning the read string if
// they match, or an error.
// This is meant to be used when setting a password.
func PromptForPasswordTwice() (string, error) {
	fmt.Print("Enter password: ")
	one, err := terminal.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	if len(one) == 0 {
		return "", ErrEmptyPassword
	}
	fmt.Print("\nConfirm password: ")
	two, err := terminal.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	// Make sure stdout moves on to the next line.
	fmt.Print("\n")
	if !bytes.Equal(one, two) {
		return "", errors.New("password mismatch")
	}

	return string(one), nil
}

// MaxPasswordLength controls the maximum length for passwords.
var MaxPasswordLength = settings.RegisterValidatedIntSetting(
	"password.validate.max_length",
	"the maximum length accepted for passwords set in cleartext via SQL. "+
		"Note that -1 means no length limit.",
	63,
	func(v int64) error {
		if v < -1 || v == 0 {
			return errors.Errorf("cannot set password.validate.max_length to a value < 1")
		}
		return nil
	})

// MinPasswordLength controls the minimum length for passwords.
var MinPasswordLength = settings.RegisterPositiveIntSetting(
	"password.validate.min_length",
	"the minimum length accepted for passwords set in cleartext via SQL. "+
		"Note that a value lower than 1 is ignored: passwords cannot be empty in any case.",
	8,
)

// MixedCaseCount controls the minimum number of uppercase and lowercase letters for passwords.
var MixedCaseCount = settings.RegisterNonNegativeIntSetting(
	"password.validate.mixed_case_count",
	"the minimum number of uppercase and lowercase letters accepted for passwords set in cleartext via SQL",
	1,
)

// NumberCount controls the minimum number of digits for passwords.
var NumberCount = settings.RegisterNonNegativeIntSetting(
	"password.validate.number_count",
	"the minimum number of digits accepted for passwords set in cleartext via SQL",
	1,
)

// SpecialCharCount controls the minimum number of special characters for passwords.
var SpecialCharCount = settings.RegisterNonNegativeIntSetting(
	"password.validate.special_char_count",
	"the minimum number of special characters accepted for passwords set in cleartext via SQL",
	1,
)

// CheckUserName controls whether check passwords are similar to the username.
var CheckUserName = settings.RegisterBoolSetting(
	"password.validate.check_user_name.enabled",
	"whether check passwords are similar to the username",
	true,
)

// hashPasswordFromSM3 returns the SM3 hash of the password.
func hashPasswordFromSM3(password string) ([]byte, error) {
	h := sm3.New()
	hash := h.Sum([]byte(password))
	str := hex.EncodeToString(hash[:])
	hashByte := []byte(str)
	return hashByte, nil
}

// compareSM3AndPassword compares a SM3 hashed password with its possible
// plaintext equivalent. Returns nil on success, or an error on failure.
func compareSM3AndPassword(hashedPassword []byte, password string) error {
	h := sm3.New()
	hash := h.Sum([]byte(password))
	str := hex.EncodeToString(hash[:])
	hashByte := []byte(str)
	if bytes.Equal(hashByte, hashedPassword) {
		return nil
	}
	return errors.Errorf("hashedPassword is not the hash of the given password")
}

const saltSize = 16

// generateSalt generates a 16 byte random salt.
func generateSalt() ([]byte, error) {
	salt := make([]byte, saltSize)
	if _, err := crypto_rand.Read(salt); err != nil {
		return nil, err
	}
	return salt, nil
}

// hashPasswordFromPBKDF2 returns the SM3 hash of the password.
func hashPasswordFromPBKDF2(password string) ([]byte, error) {
	salt, err := generateSalt()
	if err != nil {
		return nil, err
	}

	hash := pbkdf2.Key([]byte(password), salt, 4096, 32, sha256.New)
	str := hex.EncodeToString(hash[:])

	saltStr := hex.EncodeToString(salt)
	hashPassword := str + saltStr

	return []byte(hashPassword), nil
}

// comparePBKDF2AndPassword compares a SM3 hashed password with its possible
// plaintext equivalent. Returns nil on success, or an error on failure.
func comparePBKDF2AndPassword(hashedPassword []byte, password string) error {
	salt := hashedPassword[len(hashedPassword)-saltSize*2:]
	saltStr := string(salt)

	saltByte, err := hex.DecodeString(saltStr)
	if err != nil {
		return err
	}

	hash := pbkdf2.Key([]byte(password), saltByte, 4096, 32, sha256.New)
	str := hex.EncodeToString(hash[:])

	hashStr := str + saltStr
	hashByte := []byte(hashStr)

	if bytes.Equal(hashByte, hashedPassword) {
		return nil
	}

	return errors.Errorf("hashedPassword is not the hash of the given password")
}
