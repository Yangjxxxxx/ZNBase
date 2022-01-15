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

package coltypes

import (
	"strings"

	"github.com/lib/pq/oid"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

var (
	// Bool is an immutable T instance.
	Bool = &TBool{VisBool}
	// Boolean is same as Bool
	Boolean = &TBool{VisBoolean}
	// Bit is an immutable T instance.
	Bit = &TBitArray{Width: 1, VisibleType: VisBIT}
	// VarBit is an immutable T instance.
	VarBit = &TBitArray{Width: 0, Variable: true, VisibleType: VisVARBIT}
	// BitV is same as VarBit
	BitV = &TBitArray{Width: 0, Variable: true, VisibleType: VisBITV}

	// Int2 is an immutable T instance.
	Int2 = &TInt{Width: 16, VisibleType: VisINT2}
	// SmallInt is same as Int2
	SmallInt = &TInt{Width: 16, VisibleType: VisSMALLINT}
	// Int4 is an immutable T instance.
	Int4 = &TInt{Width: 32, VisibleType: VisINT4}
	// Int8 is an immutable T instance.
	Int8 = &TInt{Width: 64, VisibleType: VisINT8}
	// Int64 is same as Int8
	Int64 = &TInt{Width: 64, VisibleType: VisINT64}
	// BigInt is same as Int8
	BigInt = &TInt{Width: 64, VisibleType: VisBIGINT}

	// Serial2 is an immutable T instance.
	Serial2 = &TSerial{&TInt{Width: 16, VisibleType: VisSERIAL2}}
	// Serial4 is an immutable T instance.
	Serial4 = &TSerial{&TInt{Width: 32, VisibleType: VisSERIAL4}}
	// Serial8 is an immutable T instance.
	Serial8 = &TSerial{&TInt{Width: 64, VisibleType: VisSERIAL8}}
	// BigSerial is same as Serial
	BigSerial = &TSerial{&TInt{Width: 64, VisibleType: VisBIGSERIAL}}
	// SmallSerial is same as Serial
	SmallSerial = &TSerial{&TInt{Width: 16, VisibleType: VisSMALLSERIAL}}

	// Real is same as Float4
	Real = &TFloat{Short: true, VisibleType: VisREAL}
	// Float4 is an immutable T instance.
	Float4 = &TFloat{Short: true, VisibleType: VisFLOAT4}
	// Float8 is an immutable T instance.
	Float8 = &TFloat{VisibleType: VisFLOAT8}
	// Double is same as Float8
	Double = &TFloat{VisibleType: VisDouble}
	// Float is same as Float8
	Float = &TFloat{VisibleType: VisFLOAT}
	// Decimal is an immutable T instance.
	Decimal = &TDecimal{VisibleType: VisDECIMAL}
	// Dec is same as Decimal
	Dec = &TDecimal{VisibleType: VisDEC}
	// Numeric is same as Decimal
	Numeric = &TDecimal{VisibleType: VisNUMERIC}

	// Date is an immutable T instance.
	Date = &TDate{VisibleType: VisDATE}

	// Time is an immutable T instance.
	Time = &TTime{VisibleType: VisTIME}

	// Timestamp is an immutable T instance.
	Timestamp = &TTimestamp{VisibleType: VisTIMESTAMP}
	// TimestampWithTZ is an immutable T instance.
	TimestampWithTZ = &TTimestampTZ{VisibleType: VisTIMESTAMPWTZ}
	// TimestampWithoutTZ is same as TimestampWithTZ
	TimestampWithoutTZ = &TTimestampTZ{VisibleType: VisTIMESTAMPWOTZ}
	// TimestampTZ is same as TimestampWithTZ
	TimestampTZ = &TTimestampTZ{VisibleType: VisTIMESTAMPTZ}

	// Interval is an immutable T instance.
	Interval = &TInterval{VisibleType: VisINTERVAL}

	// Char is an immutable T instance. See strings.go for details.
	Char = &TString{Variant: TStringVariantCHAR, N: 1, VisibleType: VisCHAR}
	// VarChar is an immutable T instance. See strings.go for details.
	VarChar = &TString{Variant: TStringVariantVARCHAR, VisibleType: VisVARCHAR}
	// String is an immutable T instance. See strings.go for details.
	String = &TString{Variant: TStringVariantSTRING, VisibleType: VisSTRING}
	// Void is an immutable T instance. See strings.go for details. now only use for udr function return type
	Void = &TVoid{Variant: TStringVariantVOID, VisibleType: VisVOID}
	// QChar is an immutable T instance. See strings.go for details.
	QChar = &TString{Variant: TStringVariantQCHAR, VisibleType: VisCHAR}
	// Character is same as Char
	Character = &TString{Variant: TStringVariantCHAR, N: 1, VisibleType: VisCHARACTER}
	// Text is same as String
	Text = &TString{Variant: TStringVariantSTRING, VisibleType: VisTEXT}
	// CharV is same as String
	CharV = &TString{Variant: TStringVariantSTRING, VisibleType: VisCHARV}
	// CharacterV is same as String
	CharacterV = &TString{Variant: TStringVariantSTRING, VisibleType: VisCHARACTERV}

	// Name is an immutable T instance.
	Name = &TName{}

	// Bytes is an immutable T instance.
	Bytes = &TBytes{VisibleType: VisBYTES}
	// Bytea is same as Bytes
	Bytea = &TBytes{VisibleType: VisBYTEA}
	// Blob is same as Bytes
	Blob = &TBytes{VisibleType: VisBLOB}
	// Clob is same as Text
	Clob = &TString{Variant: TStringVariantSTRING, VisibleType: VisCLOB}

	// Int2vector is an immutable T instance.
	Int2vector = &TVector{Name: "INT2VECTOR", ParamType: Int8}

	// UUID is an immutable T instance.
	UUID = &TUUID{VisibleType: VisUUID}

	// INet is an immutable T instance.
	INet = &TIPAddr{VisibleType: VisINET}

	// JSON is an immutable T instance.
	JSON = &TJSON{VisibleType: VisJSON}
	// JSONB is same as JSON
	JSONB = &TJSON{VisibleType: VisJSONB}

	// Oid is an immutable T instance.
	Oid = &TOid{Name: "OID"}
	// RegClass is an immutable T instance.
	RegClass = &TOid{Name: "REGCLASS"}
	// RegNamespace is an immutable T instance.
	RegNamespace = &TOid{Name: "REGNAMESPACE"}
	// RegProc is an immutable T instance.
	RegProc = &TOid{Name: "REGPROC"}
	// RegProcedure is an immutable T instance.
	RegProcedure = &TOid{Name: "REGPROCEDURE"}
	// RegType is an immutable T instance.
	RegType = &TOid{Name: "REGTYPE"}

	// OidVector is an immutable T instance.
	OidVector = &TVector{Name: "OIDVECTOR", ParamType: Oid}
)

var errBitLengthNotPositive = pgerror.NewError(pgcode.InvalidParameterValue,
	"length for type bit must be at least 1")

var errBitTypeNotPositive = pgerror.NewError(pgcode.InvalidParameterValue,
	"BitType only support BIT, VARBIT, BIT VARYING")

// NewBitArrayType creates a new BIT type with the given bit width.
func NewBitArrayType(width int, varying bool, visibleType string) (*TBitArray, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	switch visibleType {
	case "varbit":
		return &TBitArray{Width: uint(width), Variable: varying, VisibleType: VisVARBIT}, nil
	case "bit":
		if varying {
			return &TBitArray{Width: uint(width), Variable: varying, VisibleType: VisBITV}, nil
		}
		return &TBitArray{Width: uint(width), Variable: varying, VisibleType: VisBIT}, nil
	}
	return nil, errBitTypeNotPositive
}

var errFloatPrecAtLeast1 = pgerror.NewError(pgcode.InvalidParameterValue,
	"precision for type float must be at least 1 bit")
var errFloatPrecMax54 = pgerror.NewError(pgcode.InvalidParameterValue,
	"precision for type float must be less than 54 bits")

// NewFloat creates a type alias for FLOAT with the given precision.
func NewFloat(prec int64) (*TFloat, error) {
	if prec < 1 {
		return nil, errFloatPrecAtLeast1
	}
	if prec <= 24 {
		return Float4, nil
	}
	if prec <= 54 {
		return Float8, nil
	}
	return nil, errFloatPrecMax54
}

// ArrayOf creates a type alias for an array of the given element type and fixed bounds.
func ArrayOf(colType T, bounds []int32) (T, error) {
	if ok, issueNum := canBeInArrayColType(colType); !ok {
		return nil, pgerror.UnimplementedWithIssueDetailErrorf(issueNum,
			colType.String(), "arrays of %s not allowed", colType)
	}
	return &TArray{ParamType: colType, Bounds: bounds}, nil
}

// EnumArrayOf stores enum values
func EnumArrayOf(colType T, bounds []string) (T, error) {
	if ok, issueNum := canBeInArrayColType(colType); !ok {
		return nil, pgerror.UnimplementedWithIssueDetailErrorf(issueNum,
			colType.String(), "arrays of %s not allowed", colType)
	}
	return &TEnum{ParamType: colType, Bounds: bounds}, nil
}

// SetArrayOf stores set values
func SetArrayOf(colType T, bounds []string) (T, error) {
	for _, bound := range bounds {
		if strings.Contains(bound, ",") {
			return nil, pgerror.NewErrorf(pgcode.InvalidColumnReference, "Illegal SET '%s' value found during parsing", bound)
		}
	}
	return &TSet{Variant: TStringVariantSET, VisibleType: VisSET, Bounds: bounds}, nil
}

var typNameLiterals map[string]T

func init() {
	typNameLiterals = make(map[string]T)
	for o, t := range types.OidToType {
		name := strings.ToLower(oid.TypeName[o])
		if _, ok := typNameLiterals[name]; !ok {
			colTyp, err := DatumTypeToColumnType(t)
			if err != nil {
				continue
			}
			typNameLiterals[name] = colTyp
		}
	}
}

// TypeForNonKeywordTypeName returns the column type for the string name of a
// type, if one exists. The third return value indicates:
// 0 if no error or the type is not known in postgres.
// -1 if the type is known in postgres.
// >0 for a github issue number.
func TypeForNonKeywordTypeName(name string) (T, bool, int) {
	t, ok := typNameLiterals[name]
	if ok {
		return t, ok, 0
	}
	return nil, false, postgresPredefinedTypeIssues[name]
}

// The following map must include all types predefined in PostgreSQL
// that are also not yet defined in ZNBaseDB and link them to
// github issues. It is also possible, but not necessary, to include
// PostgreSQL types that are already implemented in ZNBaseDB.
var postgresPredefinedTypeIssues = map[string]int{
	"box":           21286,
	"cidr":          18846,
	"circle":        21286,
	"line":          21286,
	"lseg":          21286,
	"macaddr":       -1,
	"macaddr8":      -1,
	"money":         -1,
	"path":          21286,
	"pg_lsn":        -1,
	"point":         21286,
	"polygon":       21286,
	"tsquery":       7821,
	"tsvector":      7821,
	"txid_snapshot": -1,
	"xml":           -1,
}
