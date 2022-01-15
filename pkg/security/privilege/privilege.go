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

package privilege

import (
	"bytes"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

//go:generate stringer -type=Kind

// Kind defines a privilege. This is output by the parser,
// and used to generate the privilege bitfields in the PrivilegeDescriptor.
type Kind uint32

// List of privileges. ALL is specifically encoded so that it will automatically
// pick up new privileges.
const (
	_ Kind = iota
	ALL
	CREATE
	DROP
	USAGE
	SELECT
	INSERT
	DELETE
	UPDATE
	REFERENCES
	TRIGGER
	EXECUTE
)

// ObjectType represents objects that can have privileges.
type ObjectType string

const (
	// Any represents any object type.
	Any ObjectType = "any"
	// Database represents a database object.
	Database ObjectType = "database"
	// Schema represents a schema object.
	Schema ObjectType = "schema"
	// Table represents a table or a view object.
	Table ObjectType = "table"
	// Sequence represents a sequence object.
	Sequence ObjectType = "sequence"
	// Function represents a function object.
	Function ObjectType = "function"
	// Column represents a column object.
	Column ObjectType = "column"
)

// Predefined sets of privileges.
var (
	AllPrivileges = List{CREATE, DROP, USAGE, SELECT, INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, EXECUTE}
	ReadData      = List{SELECT}
	ReadWriteData = List{SELECT, INSERT, DELETE, UPDATE}
	Access        = List{USAGE}
	// ALL privilege for specified object type.
	DatabasePrivileges = List{CREATE, DROP, USAGE}
	SchemaPrivileges   = List{CREATE, DROP, USAGE}
	TablePrivileges    = List{DROP, SELECT, INSERT, DELETE, UPDATE, REFERENCES, TRIGGER}
	SequencePrivileges = List{DROP, USAGE, SELECT, UPDATE}
	FunctionPrivileges = List{DROP, EXECUTE}
	ColumnPrivileges   = List{SELECT, INSERT, UPDATE}
)

// Mask returns the bitmask for a given privilege.
func (k Kind) Mask() uint32 {
	return 1 << k
}

// ByValue is just an array of privilege kinds sorted by value.
var ByValue = [...]Kind{
	ALL, CREATE, DROP, USAGE, SELECT, INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, EXECUTE,
}

// ByName is a map of string -> kind value.
var ByName = map[string]Kind{
	"ALL":        ALL,
	"CREATE":     CREATE,
	"DROP":       DROP,
	"SELECT":     SELECT,
	"INSERT":     INSERT,
	"DELETE":     DELETE,
	"UPDATE":     UPDATE,
	"USAGE":      USAGE,
	"REFERENCES": REFERENCES,
	"TRIGGER":    TRIGGER,
	"EXECUTE":    EXECUTE,
}

// List is a list of privileges.
type List []Kind

// Len, Swap, and Less implement the Sort interface.
func (pl List) Len() int {
	return len(pl)
}

func (pl List) Swap(i, j int) {
	pl[i], pl[j] = pl[j], pl[i]
}

func (pl List) Less(i, j int) bool {
	return pl[i] < pl[j]
}

// names returns a list of privilege names in the same
// order as 'pl'.
func (pl List) names() []string {
	ret := make([]string, len(pl))
	for i, p := range pl {
		ret[i] = p.String()
	}
	return ret
}

// String implements the Stringer interface.
// This keeps the existing order and uses ", " as separator.
func (pl List) String() string {
	return strings.Join(pl.names(), ", ")
}

// SortedString is similar to String() but returns
// privileges sorted by name and uses "," as separator.
func (pl List) SortedString() string {
	names := pl.SortedNames()
	return strings.Join(names, ",")
}

// SortedNames returns a list of privilege names
// in sorted order.
func (pl List) SortedNames() []string {
	names := pl.names()
	sort.Strings(names)
	return names
}

// ToBitField returns the bitfield representation of
// a list of privileges.
func (pl List) ToBitField() uint32 {
	var ret uint32
	for _, p := range pl {
		ret |= p.Mask()
	}
	return ret
}

//// Duplicate the LocationName after start node
//func (pl List) Duplicate() List {
//	p := map[Kind]struct{}{}
//	for _, kind := range pl {
//		p[kind] = struct{}{}
//	}
//	var res List
//	for kind := range p {
//		res = append(res, kind)
//	}
//	return res
//}

// ListFromBitField takes a bitfield of privileges and
// returns a list. It is ordered in increasing
// value of privilege.Kind.
func ListFromBitField(m uint32) List {
	ret := List{}
	for _, p := range ByValue {
		if m&p.Mask() != 0 {
			ret = append(ret, p)
		}
	}
	return ret
}

// ListFromStrings takes a list of strings and attempts to build a list of Kind.
// We convert each string to uppercase and search for it in the ByName map.
// If an entry is not found in ByName, an error is returned.
func ListFromStrings(strs []string) (List, error) {
	ret := make(List, len(strs))
	for i, s := range strs {
		k, ok := ByName[strings.ToUpper(s)]
		if !ok {
			return nil, errors.Errorf("not a valid privilege: %q", s)
		}
		ret[i] = k
	}
	return ret, nil
}

// KindFromString transform string to Kind.
func KindFromString(str string) (Kind, error) {
	k, ok := ByName[strings.ToUpper(str)]
	if !ok {
		return 0, errors.Errorf("not a valid privilege: %q", str)
	}
	return k, nil
}

// Format prints out the kind to the buffer.
func (k Kind) Format(buf *bytes.Buffer) {
	buf.WriteString(k.String())
}

// GetValidPrivilegesForObject returns the list of valid privileges for the
// specified object type.
func GetValidPrivilegesForObject(objectType ObjectType) List {
	switch objectType {
	case Any:
		return AllPrivileges
	case Database:
		return DatabasePrivileges
	case Schema:
		return SchemaPrivileges
	case Table:
		return TablePrivileges
	case Function:
		return FunctionPrivileges
	case Sequence:
		return SequencePrivileges
	case Column:
		return ColumnPrivileges
	default:
		return nil
	}
}

// IsColumnPrivilegeType determine whether priv is one of column privilege type(SELECT, INSERT, UPDATE).
func IsColumnPrivilegeType(priv Kind) bool {
	return ColumnPrivileges.ToBitField()&priv.Mask() != 0
}
