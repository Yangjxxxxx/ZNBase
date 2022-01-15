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

package settings

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

// IsolationSetting is a StringSetting that restricts the values to be one of the `IsolationValues`
type IsolationSetting struct {
	StringSetting
	IsolationValues map[string]string
}

var _ Setting = &IsolationSetting{}

// Typ returns the short (1 char) string denoting the type of setting.
func (e *IsolationSetting) Typ() string {
	return "l"
}

// ParseIsolation returns the Isolation value, and a boolean that indicates if it was parseable.
func (e *IsolationSetting) ParseIsolation(raw string) (string, bool) {
	rawUpper := strings.ToUpper(raw)
	v, ok := e.IsolationValues[rawUpper]
	if !ok {
		return "", false
	}
	return v, true
}

func (e *IsolationSetting) set(sv *Values, k string) error {
	k = strings.ToUpper(k)
	v, ok := e.IsolationValues[strings.ToUpper(k)]
	if !ok {
		return errors.Errorf("unrecognized value %s", k)
	}
	return e.StringSetting.set(sv, v)
}

// IsolationValuesToDesc make the map as a string description.
func IsolationValuesToDesc(IsolationValues map[string]string) string {
	var buffer bytes.Buffer
	values := make([]string, 0, len(IsolationValues))
	for k := range IsolationValues {
		values = append(values, k)
	}
	sort.Strings(values)
	values = append(values, values...)
	buffer.WriteString("[")
	for i, k := range values {
		if i > 0 {
			buffer.WriteString(", ")
		}
		fmt.Fprintf(&buffer, "%s = %s", strings.ToUpper(k), IsolationValues[k])
	}
	buffer.WriteString("]")
	return buffer.String()
}

// RegisterIsolationSetting defines a new setting with type Isolation.
func RegisterIsolationSetting(
	key, desc string, defaultValue string, IsolationValues map[string]string,
) *IsolationSetting {
	IsolationValuesUpper := make(map[string]string)
	var i string
	var found bool
	for k, v := range IsolationValues {
		IsolationValuesUpper[k] = strings.ToUpper(v)
		if v == defaultValue && !found {
			i = v
			found = true
		}
	}

	if !found {
		panic(fmt.Sprintf("Isolation registered with default value %s not in map %s", defaultValue, IsolationValuesToDesc(IsolationValuesUpper)))
	}

	setting := &IsolationSetting{
		StringSetting:   StringSetting{defaultValue: i},
		IsolationValues: IsolationValuesUpper,
	}

	register(key, fmt.Sprintf("%s %s", desc, IsolationValuesToDesc(IsolationValues)), setting)
	return setting
}
