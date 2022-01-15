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

package types

import (
	"bytes"
	"fmt"
	"math"

	"github.com/lib/pq/oid"
)

// T represents a SQL type.
type T interface {
	fmt.Stringer
	// Equivalent returns whether the receiver and the other type are equivalent.
	// We say that two type patterns are "equivalent" when they are structurally
	// equivalent given that a wildcard is equivalent to any type. When neither
	// Type is ambiguous (see IsAmbiguous), equivalency is the same as type equality.
	Equivalent(other T) bool
	// FamilyEqual returns whether the receiver and the other type have the same
	// constructor.
	FamilyEqual(other T) bool

	// Oid returns the type's Postgres object ID.
	Oid() oid.Oid
	// SQLName returns the type's SQL standard name. This can be looked up for a
	// type `t` in postgres by running `SELECT format_type(t::regtype, NULL)`.
	SQLName() string

	// IsAmbiguous returns whether the type is ambiguous or fully defined. This
	// is important for parameterized types to determine whether they are fully
	// concrete type specification or not.
	IsAmbiguous() bool

	// Len() returns the type's length.
	Len() int16

	// Type() returns the type's PostgreSQL-Type.
	//
	// typtype is 'b' for a base type, 'c' for a composite type (e.g., a
	// table's rowtype), 'd' for a domain, 'e' for an enum type, 'p' for a
	// pseudo-type, or 'r' for a range type. (Use the TYPTYPE macros below.)
	//
	// If typtype is 'c', typrelid is the OID of the class' entry in pg_class.
	Type() byte

	// If typelem is not 0 then it identifies another row in pg_type. The
	// current type can then be subscripted like an array yielding values of
	// type typelem. A non-zero typelem does not guarantee this type to be a
	// "real" array type; some ordinary fixed-length types can also be
	// subscripted (e.g., name, point). Variable-length types can *not* be
	// turned into pseudo-arrays like that. Hence, the way to determine
	// whether a type is a "true" array type is if:
	//
	// typelem != 0 and typlen == -1.
	Lem() oid.Oid

	// typstorage tells if the type is prepared for toasting and what
	// the default strategy for attributes of this type should be.
	//
	// 'p' PLAIN	  type not prepared for toasting
	// 'e' EXTERNAL   external storage possible, don't try to compress
	// 'x' EXTENDED   try to compress and store external if required
	// 'm' MAIN		  like 'x' but try to keep in main tuple
	// (Use the TYPSTORAGE macros below for these.)
	//
	// Note that 'm' fields can also be moved out to secondary storage,
	// but only as a last resort ('e' and 'x' fields are moved first).
	//Storage() byte
}

var (
	// Unknown is the type of an expression that statically evaluates to
	// NULL. Can be compared with ==.
	Unknown T = tUnknown{}
	// Bool is the type of a DBool. Can be compared with ==.
	Bool T = tBool{}
	// BitArray is the type of a DBitArray. Can be compared with ==.
	BitArray T = tBitArray{}
	// Int is the type of a DInt8. Can be compared with ==.
	Int T = tInt{}
	// Int2 is the type of a DInt2. Can be compared with ==.
	Int2 T = tInt{Length: 16}
	// Int4 is the type of a DInt4. Can be compared with ==.
	Int4 T = tInt{Length: 32}
	// Float is the type of a DFloat8. Can be compared with ==.
	Float T = tFloat{}
	// Float4 is the type of a DFloat4. Can be compared with ==.
	Float4 T = tFloat{Precision: 32}
	// Decimal is the type of a DDecimal. Can be compared with ==.
	Decimal T = tDecimal{}
	// String is the type of a DString. Can be compared with ==.
	String T = tString{}
	// Bytes is the type of a DBytes. Can be compared with ==.
	Bytes T = tBytes{}
	// Date is the type of a DDate. Can be compared with ==.
	Date T = tDate{}
	// Time is the type of a DTime. Can be compared with ==.
	Time T = tTime{}
	// Timestamp is the type of a DTimestamp. Can be compared with ==.
	Timestamp T = tTimestamp{}
	// TimestampTZ is the type of a DTimestampTZ. Can be compared with ==.
	TimestampTZ T = tTimestampTZ{}
	// Interval is the type of a DInterval. Can be compared with ==.
	Interval T = tInterval{}
	// JSON is the type of a DJSON. Can be compared with ==.
	JSON T = tJSON{}
	// UUID is the type of a DUuid. Can be compared with ==.
	UUID T = tUUID{}
	// INet is the type of a DIPAddr. Can be compared with ==.
	INet T = tINet{}
	// AnyArray is the type of a DArray with a wildcard parameterized type.
	// Can be compared with ==.
	AnyArray T = TArray{Any}
	// Any can be any type. Can be compared with ==.
	Any T = tAny{}
	// Void represent void type.
	Void T = tVoid{}
	// Cursor is the type of cursor
	Cursor T = tCursor{}
	//Set represent set type.
	Set T = TtSet{}

	// AnyNonArray contains all non-array types.
	AnyNonArray = []T{
		Bool,
		BitArray,
		Int,
		Float,
		Decimal,
		String,
		Bytes,
		Date,
		Time,
		Timestamp,
		TimestampTZ,
		Interval,
		UUID,
		INet,
		JSON,
		Oid,
		// FamCollatedString,
	}

	// FamCollatedString is the type family of a DString. CANNOT be
	// compared with ==.
	FamCollatedString T = TCollatedString{}
	// FamTuple is the type family of a DTuple. CANNOT be compared with ==.
	FamTuple T = TTuple{}
	// FamArray is the type family of a DArray. CANNOT be compared with ==.
	FamArray T = TArray{}
	// FamPlaceholder is the type family of a placeholder. CANNOT be compared
	// with ==.
	FamPlaceholder T = TPlaceholder{}
)

// Do not instantiate the tXxx types elsewhere. The variables above are intended
// to be singletons.
type tUnknown struct{}

func (tUnknown) String() string           { return "unknown" }
func (tUnknown) Equivalent(other T) bool  { return other == Unknown || other == Any }
func (tUnknown) FamilyEqual(other T) bool { return other == Unknown }
func (tUnknown) Oid() oid.Oid             { return oid.T_unknown }
func (tUnknown) SQLName() string          { return "unknown" }
func (tUnknown) IsAmbiguous() bool        { return true }
func (tUnknown) Len() int16               { return -2 }
func (tUnknown) Type() byte               { return 'p' }
func (tUnknown) Lem() oid.Oid             { return 0 }

type tBool struct{}

func (tBool) String() string           { return "bool" }
func (tBool) Equivalent(other T) bool  { return UnwrapType(other) == Bool || other == Any }
func (tBool) FamilyEqual(other T) bool { return UnwrapType(other) == Bool }
func (tBool) Oid() oid.Oid             { return oid.T_bool }
func (tBool) SQLName() string          { return "boolean" }
func (tBool) IsAmbiguous() bool        { return false }
func (tBool) Len() int16               { return 1 }
func (tBool) Type() byte               { return 'b' }
func (tBool) Lem() oid.Oid             { return 0 }

type tInt struct{ Length int }

func (tInt) String() string { return "int" }
func (tInt) Equivalent(other T) bool {
	return UnwrapType(other) == Int || UnwrapType(other) == Int2 || UnwrapType(other) == Int4 || other == Any
}
func (tInt) FamilyEqual(other T) bool {
	return UnwrapType(other) == Int || UnwrapType(other) == Int2 || UnwrapType(other) == Int4
}
func (t tInt) Oid() oid.Oid {
	if t.Length == 32 {
		return oid.T_int4
	} else if t.Length == 16 {
		return oid.T_int2
	}
	return oid.T_int8
}
func (tInt) SQLName() string   { return "bigint" }
func (tInt) IsAmbiguous() bool { return false }
func (tInt) Len() int16        { return 8 }
func (tInt) Type() byte        { return 'b' }
func (tInt) Lem() oid.Oid      { return 0 }

type tBitArray struct{}

func (tBitArray) String() string           { return "varbit" }
func (tBitArray) Equivalent(other T) bool  { return UnwrapType(other) == BitArray || other == Any }
func (tBitArray) FamilyEqual(other T) bool { return UnwrapType(other) == BitArray }
func (tBitArray) Oid() oid.Oid             { return oid.T_varbit }
func (tBitArray) SQLName() string          { return "bit varying" }
func (tBitArray) IsAmbiguous() bool        { return false }
func (tBitArray) Len() int16               { return -1 }
func (tBitArray) Type() byte               { return 'b' }
func (tBitArray) Lem() oid.Oid             { return 0 }

type tFloat struct{ Precision int }

func (tFloat) String() string { return "float" }
func (tFloat) Equivalent(other T) bool {
	return UnwrapType(other) == Float || UnwrapType(other) == Float4 || other == Any
}
func (tFloat) FamilyEqual(other T) bool {
	return UnwrapType(other) == Float || UnwrapType(other) == Float4
}
func (t tFloat) Oid() oid.Oid {
	if t.Precision == 32 {
		return oid.T_float4
	}
	return oid.T_float8
}
func (tFloat) SQLName() string   { return "double precision" }
func (tFloat) IsAmbiguous() bool { return false }
func (tFloat) Len() int16        { return 8 }
func (tFloat) Type() byte        { return 'b' }
func (tFloat) Lem() oid.Oid      { return 0 }

type tDecimal struct{}

func (tDecimal) String() string { return "decimal" }
func (tDecimal) Equivalent(other T) bool {
	return UnwrapType(other) == Decimal || other == Any
}

func (tDecimal) FamilyEqual(other T) bool { return UnwrapType(other) == Decimal }
func (tDecimal) Oid() oid.Oid             { return oid.T_numeric }
func (tDecimal) SQLName() string          { return "numeric" }
func (tDecimal) IsAmbiguous() bool        { return false }
func (tDecimal) Len() int16               { return -1 }
func (tDecimal) Type() byte               { return 'b' }
func (tDecimal) Lem() oid.Oid             { return 0 }

type tString struct{}

func (tString) String() string { return "string" }
func (tString) Equivalent(other T) bool {
	if _, ok := other.(TtSet); ok {
		return true
	}
	return UnwrapType(other) == String || other == Any
}
func (tString) FamilyEqual(other T) bool { return UnwrapType(other) == String }
func (tString) Oid() oid.Oid             { return oid.T_text }
func (tString) SQLName() string          { return "text" }
func (tString) IsAmbiguous() bool        { return false }
func (tString) Len() int16               { return -1 }
func (tString) Type() byte               { return 'b' }
func (tString) Lem() oid.Oid             { return 0 }

// TCollatedString is the type of strings with a locale.
type TCollatedString struct {
	Locale string
}

// String implements the fmt.Stringer interface.
func (t TCollatedString) String() string {
	if t.Locale == "" {
		// Used in telemetry.
		return "collatedstring{*}"
	}
	return fmt.Sprintf("collatedstring{%s}", t.Locale)
}

// Equivalent implements the T interface.
func (t TCollatedString) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	u, ok := UnwrapType(other).(TCollatedString)
	if ok {
		return t.Locale == "" || u.Locale == "" || t.Locale == u.Locale
	}
	return false
}

// FamilyEqual implements the T interface.
func (TCollatedString) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TCollatedString)
	return ok
}

// Oid implements the T interface.
func (TCollatedString) Oid() oid.Oid { return oid.T_text }

// SQLName implements the T interface.
func (TCollatedString) SQLName() string { return "text" }

// IsAmbiguous implements the T interface.
func (t TCollatedString) IsAmbiguous() bool {
	return t.Locale == ""
}

// Len implements the T interface.
func (TCollatedString) Len() int16 { return -1 }

// Type implements the T interface.
func (TCollatedString) Type() byte { return 'b' }

// Lem implements the T interface.
func (TCollatedString) Lem() oid.Oid { return 0 }

type tBytes struct{}

func (tBytes) String() string           { return "bytes" }
func (tBytes) Equivalent(other T) bool  { return UnwrapType(other) == Bytes || other == Any }
func (tBytes) FamilyEqual(other T) bool { return UnwrapType(other) == Bytes }
func (tBytes) Oid() oid.Oid             { return oid.T_bytea }
func (tBytes) SQLName() string          { return "bytea" }
func (tBytes) IsAmbiguous() bool        { return false }
func (tBytes) Len() int16               { return -1 }
func (tBytes) Type() byte               { return 'b' }
func (tBytes) Lem() oid.Oid             { return 0 }

type tDate struct{}

func (tDate) String() string           { return "date" }
func (tDate) Equivalent(other T) bool  { return UnwrapType(other) == Date || other == Any }
func (tDate) FamilyEqual(other T) bool { return UnwrapType(other) == Date }
func (tDate) Oid() oid.Oid             { return oid.T_date }
func (tDate) SQLName() string          { return "date" }
func (tDate) IsAmbiguous() bool        { return false }
func (tDate) Len() int16               { return 4 }
func (tDate) Type() byte               { return 'b' }
func (tDate) Lem() oid.Oid             { return 0 }

type tTime struct{}

func (tTime) String() string           { return "time" }
func (tTime) Equivalent(other T) bool  { return UnwrapType(other) == Time || other == Any }
func (tTime) FamilyEqual(other T) bool { return UnwrapType(other) == Time }
func (tTime) Oid() oid.Oid             { return oid.T_time }
func (tTime) SQLName() string          { return "time" }
func (tTime) IsAmbiguous() bool        { return false }
func (tTime) Len() int16               { return 8 }
func (tTime) Type() byte               { return 'b' }
func (tTime) Lem() oid.Oid             { return 0 }

type tTimestamp struct{}

func (tTimestamp) String() string { return "timestamp" }
func (tTimestamp) Equivalent(other T) bool {
	return UnwrapType(other) == Timestamp || other == Any
}

func (tTimestamp) FamilyEqual(other T) bool { return UnwrapType(other) == Timestamp }
func (tTimestamp) Oid() oid.Oid             { return oid.T_timestamp }
func (tTimestamp) SQLName() string          { return "timestamp without time zone" }
func (tTimestamp) IsAmbiguous() bool        { return false }
func (tTimestamp) Len() int16               { return 8 }
func (tTimestamp) Type() byte               { return 'b' }
func (tTimestamp) Lem() oid.Oid             { return 0 }

type tTimestampTZ struct{}

func (tTimestampTZ) String() string { return "timestamptz" }
func (tTimestampTZ) Equivalent(other T) bool {
	return UnwrapType(other) == TimestampTZ || other == Any
}

func (tTimestampTZ) FamilyEqual(other T) bool { return UnwrapType(other) == TimestampTZ }
func (tTimestampTZ) Oid() oid.Oid             { return oid.T_timestamptz }
func (tTimestampTZ) SQLName() string          { return "timestamp with time zone" }
func (tTimestampTZ) IsAmbiguous() bool        { return false }
func (tTimestampTZ) Len() int16               { return 8 }
func (tTimestampTZ) Type() byte               { return 'b' }
func (tTimestampTZ) Lem() oid.Oid             { return 0 }

type tInterval struct{}

func (tInterval) String() string { return "interval" }
func (tInterval) Equivalent(other T) bool {
	return UnwrapType(other) == Interval || other == Any
}

func (tInterval) FamilyEqual(other T) bool { return UnwrapType(other) == Interval }
func (tInterval) Oid() oid.Oid             { return oid.T_interval }
func (tInterval) SQLName() string          { return "interval" }
func (tInterval) IsAmbiguous() bool        { return false }
func (tInterval) Len() int16               { return 16 }
func (tInterval) Type() byte               { return 'b' }
func (tInterval) Lem() oid.Oid             { return 0 }

type tJSON struct{}

func (tJSON) String() string { return "jsonb" }
func (tJSON) Equivalent(other T) bool {
	return UnwrapType(other) == JSON || other == Any
}

func (tJSON) FamilyEqual(other T) bool { return UnwrapType(other) == JSON }
func (tJSON) Oid() oid.Oid             { return oid.T_jsonb }
func (tJSON) SQLName() string          { return "json" }
func (tJSON) IsAmbiguous() bool        { return false }
func (tJSON) Len() int16               { return -1 }
func (tJSON) Type() byte               { return 'b' }
func (tJSON) Lem() oid.Oid             { return 0 }

type tUUID struct{}

func (tUUID) String() string           { return "uuid" }
func (tUUID) Equivalent(other T) bool  { return UnwrapType(other) == UUID || other == Any }
func (tUUID) FamilyEqual(other T) bool { return UnwrapType(other) == UUID }
func (tUUID) Oid() oid.Oid             { return oid.T_uuid }
func (tUUID) SQLName() string          { return "uuid" }
func (tUUID) IsAmbiguous() bool        { return false }
func (tUUID) Len() int16               { return 16 }
func (tUUID) Type() byte               { return 'b' }
func (tUUID) Lem() oid.Oid             { return 0 }

type tINet struct{}

func (tINet) String() string           { return "inet" }
func (tINet) Equivalent(other T) bool  { return UnwrapType(other) == INet || other == Any }
func (tINet) FamilyEqual(other T) bool { return UnwrapType(other) == INet }
func (tINet) Oid() oid.Oid             { return oid.T_inet }
func (tINet) SQLName() string          { return "inet" }
func (tINet) IsAmbiguous() bool        { return false }
func (tINet) Len() int16               { return -1 }
func (tINet) Type() byte               { return 'b' }
func (tINet) Lem() oid.Oid             { return 0 }

// TTuple is the type of a DTuple.
type TTuple struct {
	Types  []T
	Labels []string
}

// String implements the fmt.Stringer interface.
func (t TTuple) String() string {
	var buf bytes.Buffer
	buf.WriteString("tuple")
	if t.Types != nil {
		buf.WriteByte('{')
		for i, typ := range t.Types {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(typ.String())
			if t.Labels != nil {
				buf.WriteString(" AS ")
				buf.WriteString(t.Labels[i])
			}
		}
		buf.WriteByte('}')
	}
	return buf.String()
}

// Equivalent implements the T interface.
func (t TTuple) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	u, ok := UnwrapType(other).(TTuple)
	if !ok {
		return false
	}
	if len(t.Types) == 0 || len(u.Types) == 0 {
		// Tuples that aren't fully specified (have a nil subtype list) are always
		// equivalent to other tuples, to allow overloads to specify that they take
		// an arbitrary tuple type.
		return true
	}
	if len(t.Types) != len(u.Types) {
		return false
	}
	for i, typ := range t.Types {
		if !typ.Equivalent(u.Types[i]) {
			return false
		}
	}
	return true
}

// FamilyEqual implements the T interface.
func (TTuple) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TTuple)
	return ok
}

// Oid implements the T interface.
func (TTuple) Oid() oid.Oid { return oid.T_record }

// SQLName implements the T interface.
func (TTuple) SQLName() string { return "record" }

// IsAmbiguous implements the T interface.
func (t TTuple) IsAmbiguous() bool {
	for _, typ := range t.Types {
		if typ == nil || typ.IsAmbiguous() {
			return true
		}
	}
	return len(t.Types) == 0
}

// Len implements the T interface.
func (TTuple) Len() int16 { return -1 }

// Type implements the T interface.
func (TTuple) Type() byte { return 'p' }

// Lem implements the T interface.
func (TTuple) Lem() oid.Oid { return 0 }

// PlaceholderIdx is the 0-based index of a placeholder. Placeholder "$1"
// has PlaceholderIdx=0.
type PlaceholderIdx uint16

// MaxPlaceholderIdx is the maximum allowed value of a PlaceholderIdx.
// The pgwire protocol is limited to 2^16 placeholders, so we limit the IDs to
// this range as well.
const MaxPlaceholderIdx = math.MaxUint16

// String returns the index as a placeholder string representation ($1, $2 etc).
func (idx PlaceholderIdx) String() string {
	return fmt.Sprintf("$%d", idx+1)
}

// TPlaceholder is the type of a placeholder.
type TPlaceholder struct {
	Idx PlaceholderIdx
}

// String implements the fmt.Stringer interface.
func (t TPlaceholder) String() string { return fmt.Sprintf("placeholder{%d}", t.Idx+1) }

// Equivalent implements the T interface.
func (t TPlaceholder) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	if other.IsAmbiguous() {
		return true
	}
	u, ok := UnwrapType(other).(TPlaceholder)
	return ok && t.Idx == u.Idx
}

// FamilyEqual implements the T interface.
func (TPlaceholder) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TPlaceholder)
	return ok
}

// Oid implements the T interface.
func (TPlaceholder) Oid() oid.Oid { panic("TPlaceholder.Oid() is undefined") }

// SQLName implements the T interface.
func (TPlaceholder) SQLName() string { panic("TPlaceholder.SQLName() is undefined") }

// IsAmbiguous implements the T interface.
func (TPlaceholder) IsAmbiguous() bool { panic("TPlaceholder.IsAmbiguous() is undefined") }

// Len implements the T interface.
func (TPlaceholder) Len() int16 { panic("TPlaceholder.Len() is undefined") }

// Type implements the T interface.
func (TPlaceholder) Type() byte { panic("TPlaceholder.Type() is undefined") }

// Lem implements the T interface.
func (TPlaceholder) Lem() oid.Oid { panic("TPlaceholder.Lem() is undefined") }

// TArray is the type of a DArray.
type TArray struct{ Typ T }

func (a TArray) String() string {
	if a.Typ == nil {
		// Used in telemetry.
		return "*[]"
	}
	return a.Typ.String() + "[]"
}

// Equivalent implements the T interface.
func (a TArray) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	if u, ok := UnwrapType(other).(TArray); ok {
		return a.Typ.Equivalent(u.Typ)
	}
	return false
}

// FamilyEqual implements the T interface.
func (TArray) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TArray)
	return ok
}

const noArrayType = 0

// ArrayOids is a set of all oids which correspond to an array type.
var ArrayOids = map[oid.Oid]struct{}{}

func init() {
	for _, v := range oidToArrayOid {
		ArrayOids[v] = struct{}{}
	}
}

// Oid implements the T interface.
func (a TArray) Oid() oid.Oid {
	if o, ok := oidToArrayOid[a.Typ.Oid()]; ok {
		return o
	}
	return noArrayType
}

// SQLName implements the T interface.
func (a TArray) SQLName() string {
	return a.Typ.SQLName() + "[]"
}

// IsAmbiguous implements the T interface.
func (a TArray) IsAmbiguous() bool {
	return a.Typ == nil || a.Typ.IsAmbiguous()
}

// Len implements the T interface.
func (TArray) Len() int16 { return -1 }

// Type implements the T interface.
func (a TArray) Type() byte {
	switch a.Typ.Oid() {
	case oid.T_anyelement:
		return 'p'
	default:
		return 'b'
	}
}

// Lem implements the T interface.
func (a TArray) Lem() oid.Oid {
	switch a.Typ.Oid() {
	case oid.T_anyelement:
		return 0
	default:
		return a.Typ.Oid()
	}
}

// TEnum is the type of a DArray.
type TEnum struct{ Typ T }

func (a TEnum) String() string {
	if a.Typ == nil {
		// Used in telemetry.
		return "*[]"
	}
	return "ENUM" + "()"
}

// Equivalent implements the T interface.
func (a TEnum) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	if u, ok := UnwrapType(other).(TEnum); ok {
		return a.Typ.Equivalent(u.Typ)
	}
	return false
}

// FamilyEqual implements the T interface.
func (TEnum) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TEnum)
	return ok
}

//const noArrayType = 0
//
//// ArrayOids is a set of all oids which correspond to an array type.
//var ArrayOids = map[oid.Oid]struct{}{}

//func init() {
//	for _, v := range oidToArrayOid {
//		ArrayOids[v] = struct{}{}
//	}
//}

// Oid implements the T interface.
func (a TEnum) Oid() oid.Oid {
	if o, ok := oidToArrayOid[a.Typ.Oid()]; ok {
		return o
	}
	return noArrayType
}

// SQLName implements the T interface.
func (a TEnum) SQLName() string {
	return "ENUM" + "()"
}

// IsAmbiguous implements the T interface.
func (a TEnum) IsAmbiguous() bool {
	return a.Typ == nil || a.Typ.IsAmbiguous()
}

// Len implements the T interface.
func (TEnum) Len() int16 { return -1 }

// Type implements the T interface.
func (a TEnum) Type() byte {
	switch a.Typ.Oid() {
	case oid.T_anyelement:
		return 'p'
	default:
		return 'b'
	}
}

// Lem implements the T interface.
func (a TEnum) Lem() oid.Oid {
	switch a.Typ.Oid() {
	case oid.T_anyelement:
		return 0
	default:
		return a.Typ.Oid()
	}
}

// TtSet is the type of a DString.
type TtSet struct {
	Typ    T
	Bounds []string
}

func (a TtSet) String() string { return "SET()" }

// Equivalent implements the T interface.
func (a TtSet) Equivalent(other T) bool { return true }

// FamilyEqual implements the T interface.
func (TtSet) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TtSet)
	return ok
}

// Oid implements the T interface.
func (a TtSet) Oid() oid.Oid {
	if o, ok := oidToArrayOid[a.Typ.Oid()]; ok {
		return o
	}
	return noArrayType
}

// SQLName implements the T interface.
func (a TtSet) SQLName() string {
	return "set"
}

// IsAmbiguous implements the T interface.
func (a TtSet) IsAmbiguous() bool {
	return false
}

// Len implements the T interface.
func (TtSet) Len() int16 { return -1 }

// Type implements the T interface.
func (a TtSet) Type() byte {
	switch a.Typ.Oid() {
	case oid.T_anyelement:
		return 'p'
	default:
		return 'b'
	}
}

// Lem implements the T interface.
func (a TtSet) Lem() oid.Oid {
	switch a.Typ.Oid() {
	case oid.T_anyelement:
		return 0
	default:
		return a.Typ.Oid()
	}
}

type tAny struct{}

func (tAny) String() string           { return "anyelement" }
func (tAny) Equivalent(other T) bool  { return true }
func (tAny) FamilyEqual(other T) bool { return other == Any }
func (tAny) Oid() oid.Oid             { return oid.T_anyelement }
func (tAny) SQLName() string          { return "anyelement" }
func (tAny) IsAmbiguous() bool        { return true }
func (tAny) Len() int16               { return 4 }
func (tAny) Type() byte               { return 'p' }
func (tAny) Lem() oid.Oid             { return 0 }

type tVoid struct{}

func (tVoid) String() string           { return "void" }
func (tVoid) Equivalent(other T) bool  { return true }
func (tVoid) FamilyEqual(other T) bool { return other == Void }
func (tVoid) Oid() oid.Oid             { return oid.T_void }
func (tVoid) SQLName() string          { return "void" }
func (tVoid) IsAmbiguous() bool        { return true }
func (tVoid) Len() int16               { return 4 }
func (tVoid) Type() byte               { return 'p' }
func (tVoid) Lem() oid.Oid             { return 0 }

type tCursor struct{}

func (tCursor) String() string           { return "cursor" }
func (tCursor) Equivalent(other T) bool  { return true }
func (tCursor) FamilyEqual(other T) bool { return other == Cursor }
func (tCursor) Oid() oid.Oid             { return oid.T_refcursor }
func (tCursor) SQLName() string          { return "cursor" }
func (tCursor) IsAmbiguous() bool        { return true }
func (tCursor) Len() int16               { return -1 }
func (tCursor) Type() byte               { return 'b' }
func (tCursor) Lem() oid.Oid             { return 0 }

// IsStringType returns true iff t is String
// or a collated string type.
func IsStringType(t T) bool {
	switch t.(type) {
	case tString, TCollatedString:
		return true
	default:
		return false
	}
}

// IsValidArrayElementType returns true if the T
// can be used in TArray.
// If the valid return is false, the issue number should
// be included in the error report to inform the user.
func IsValidArrayElementType(t T) (valid bool, issueNum int) {
	switch t {
	case JSON:
		return false, 23468
	default:
		return true, 0
	}
}

// IsDateTimeType returns true if the T is
// date- or time-related type.
func IsDateTimeType(t T) bool {
	switch t {
	case Date:
		return true
	case Time:
		return true
	case Timestamp:
		return true
	case TimestampTZ:
		return true
	case Interval:
		return true
	default:
		return false
	}
}

// IsAdditiveType returns true if the T
// supports addition and subtraction.
func IsAdditiveType(t T) bool {
	switch t {
	case Int, Int2, Int4:
		return true
	case Float, Float4:
		return true
	case Decimal:
		return true
	default:
		return IsDateTimeType(t)
	}
}

// TypeStrGetdatumType TypeStrGetdatumType
func TypeStrGetdatumType(typStr string) T {
	switch typStr {
	case "INTEGER", "SMALLINT", "BIGINT":
		return Int
	case "REAL", "DOUBLE_PRECISION":
		return Float
	case "VARCHAR", "CHAR", "QCHAR":
		return Date
	case "VARBIT":
		return BitArray
	case "NONE", "NULL":
		return Unknown
	default:
		return nil
	}
}

// MakeTypeByVisibleTypName 根据VisibleTypeName 调整 type 中的数值
func MakeTypeByVisibleTypName(typ T, visibleTypeName string) T {
	length := 0
	newType := typ

	switch visibleTypeName {
	case "INT2", "INT2[]", "SERIAL2", "SERIAL2[]", "SMALLSERIAL", "SMALLSERIAL[]":
		length = 16
	case "INT4", "INT4[]", "SERIAL4", "SERIAL4[]", "REAL", "REAL[]", "FLOAT4", "FLOAT4[]":
		length = 32
	default:
		return typ
	}
	switch t := typ.(type) {
	case tInt:
		t.Length = length
		newType = t
	case tFloat:
		t.Precision = length
		newType = t
	case TArray:
		if newT, ok := t.Typ.(tInt); ok {
			newT.Length = length
			t.Typ = newT
		} else if newT, ok := t.Typ.(tFloat); ok {
			newT.Precision = length
			t.Typ = newT
		}
		newType = t
	}
	return newType
}
