// Copyright 2020  The Cockroach Authors.
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
	"testing"

	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
)

func TestUserName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		username   string
		normalized string
		isValidate bool
	}{
		{"Abc123", "abc123", true},
		{"0123121132", "", false},
		{"HeLlO", "hello", true},
		{"Ομηρος", "ομηρος", true},
		{"_HeLlO", "_hello", true},
		{"a-BC-d", "a-bc-d", true},
		{"A.Bcd", "", false},
		{"WWW.BIGSITE.NET", "", false},
		{"", "", false},
		{"-ABC", "", false},
		{".ABC", "", false},
		{"*.wildcard", "", false},
		{"foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoof", "", false},
		{"M", "m", true},
		{".", "", false},
	}
	for _, tc := range testCases {
		normalized, err := NormalizeAndValidateUsername(tc.username)
		if (err == nil) == !tc.isValidate {
			t.Errorf("%q: expected is valid: %t, got %t", tc.username, tc.isValidate, err == nil)
			continue
		}
		if normalized != tc.normalized {
			t.Errorf("%q: expected %q, got %q", tc.username, tc.normalized, normalized)
		}
	}
}

func TestCheckComplexity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		password string
		expect   bool
	}{
		{"123PPPWWW", false},
		{"123pppwww", false},
		{"pppSSSwww", false},
		{"123&&&%%%", false},
		{"ppp&&&%%%", false},
		{"PPP&&&%%%", false},
		{"123PPPwww", false},
		{"123PPP%%%", false},
		{"123www&&&", false},
		{"PPPwww%%%", false},
		{"%23AAabcd", false},
		{"4**AAabcd", false},
		{"a&&123ABC", false},
		{"A^^123abc", false},
		{"##123AAab", true},
		{"5%aCe&8XX", true},
	}
	for _, tc := range testCases {
		ans := checkComplex(tc.password, 2, 2, 2)
		if tc.expect != ans {
			t.Errorf("%q: expected it is meeted the requirement of complexity: %t. got %t", tc.password, tc.expect, ans)
			continue
		}
	}
}
