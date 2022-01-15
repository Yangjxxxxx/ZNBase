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

package coltypes

import (
	"bytes"
	"fmt"

	"github.com/znbasedb/znbase/pkg/sql/lex"
)

// VisibleType represents the visible type key of the VisibleTypeName
type VisibleType int32

// VisXXX represents the visible type map key
const (
	VisNONE VisibleType = iota
	VisINT
	VisINTEGER
	VisINT2
	VisSMALLINT
	VisINT4
	VisINT8
	VisINT64
	VisBIGINT
	VisFLOAT
	VisREAL
	VisFLOAT4
	VisFLOAT8
	VisBoolean
	VisBool
	VisDouble
	VisSERIAL
	VisSERIAL2
	VisSMALLSERIAL
	VisSERIAL4
	VisSERIAL8
	VisBIGSERIAL
	VisINTA
	VisDECIMAL
	VisDEC
	VisNUMERIC
	VisBIT
	VisVARBIT
	VisBITV
	VisBYTES
	VisBYTEA
	VisBLOB
	VisCLOB
	VisSTRING
	VisSTRINGA
	VisCHARACTER
	VisCHAR
	VisVARCHAR
	VisTEXT
	VisDATE
	VisTIME
	VisTIMEWTZ
	VisTIMESTAMP
	VisTIMESTAMPP
	VisTIMESTAMPTZ
	VisTIMESTAMPWOTZ
	VisTIMESTAMPWTZ
	VisINTERVAL
	VisINET
	VisUUID
	VisJSON
	VisJSONB
	VisCHARACTERV
	VisCHARV
	VisVOID
	VisSET
)

// VisibleTypeName represents the visible type name
var VisibleTypeName = map[VisibleType]string{

	VisCHARV:         "CHAR VARYING",
	VisCHARACTERV:    "CHARACTER VARYING",
	VisJSONB:         "JSONB",
	VisJSON:          "JSON",
	VisUUID:          "UUID",
	VisINET:          "INET",
	VisINTERVAL:      "INTERVAL",
	VisTIMESTAMPWTZ:  "TIMESTAMP WITH TIME ZONE",
	VisTIMESTAMPWOTZ: "TIMESTAMP WITHOUT TIME ZONE",
	VisTIMESTAMPTZ:   "TIMESTAMPTZ",
	VisTIMESTAMP:     "TIMESTAMP",
	VisTIMESTAMPP:    "TIMESTAMPP",
	VisTIMEWTZ:       "TIME WITHOUT TIME ZONE",
	VisTIME:          "TIME",
	VisDATE:          "DATE",
	VisTEXT:          "TEXT",
	VisVARCHAR:       "VARCHAR",
	VisNONE:          "NONE",
	VisINT:           "INT",
	VisINTEGER:       "INTEGER",
	VisINT2:          "INT2",
	VisSMALLINT:      "SMALLINT",
	VisINT4:          "INT4",
	VisINT8:          "INT8",
	VisINT64:         "INT64",
	VisBIGINT:        "BIGINT",
	VisFLOAT:         "FLOAT",
	VisREAL:          "REAL",
	VisFLOAT4:        "FLOAT4",
	VisFLOAT8:        "FLOAT8",
	VisBoolean:       "BOOLEAN",
	VisBool:          "BOOL",
	VisDouble:        "DOUBLE PRECISION",
	VisSERIAL:        "SERIAL",
	VisSERIAL2:       "SERIAL2",
	VisSMALLSERIAL:   "SMALLSERIAL",
	VisSERIAL4:       "SERIAL4",
	VisSERIAL8:       "SERIAL8",
	VisBIGSERIAL:     "BIGSERIAL",
	VisINTA:          "INT[]",
	VisDECIMAL:       "DECIMAL",
	VisDEC:           "DEC",
	VisNUMERIC:       "NUMERIC",
	VisBIT:           "BIT",
	VisVARBIT:        "VARBIT",
	VisBITV:          "BIT VARYING",
	VisBYTES:         "BYTES",
	VisBYTEA:         "BYTEA",
	VisBLOB:          "BLOB",
	VisCLOB:          "CLOB",
	VisSTRING:        "STRING",
	VisSTRINGA:       "STRING[]",
	VisCHARACTER:     "CHARACTER",
	VisCHAR:          "CHAR",
	VisVOID:          "VOID",
}

// ColTypeFormatter knows how to format a ColType to a bytes.Buffer.
type ColTypeFormatter interface {
	fmt.Stringer

	// TypeName returns the base name of the type, suitable to generate
	// column names for cast expressions.
	TypeName() string

	// Format returns a non-lossy string representation of the coltype.
	// NOTE: It is important that two coltypes that should be different print out
	//       different string representations. The optimizer relies on unique
	//       string representations in order to intern the coltypes during
	//       memoization.
	Format(buf *bytes.Buffer, flags lex.EncodeFlags)
}

// ColTypeAsString print a T to a string.
func ColTypeAsString(n ColTypeFormatter) string {
	var buf bytes.Buffer
	n.Format(&buf, lex.EncNoFlags)
	return buf.String()
}

// CastTargetType represents a type that is a valid cast target.
type CastTargetType interface {
	ColTypeFormatter
	castTargetType()
}

// T represents a type in a column definition.
type T interface {
	CastTargetType

	ColumnType() string
}

// ColumnType implements the interface T, returns the visible type
func (node *TArray) ColumnType() string {
	if collation, ok := node.ParamType.(*TCollatedString); ok {
		// We cannot use node.ParamType.Format() directly here (and DRY
		// across the two branches of the if) because if we have an array
		// of collated strings, the COLLATE string must appear after the
		// square brackets.
		buf := &bytes.Buffer{}
		buf.WriteString(collation.TString.ColumnType())
		buf.WriteString("[] COLLATE ")
		lex.EncodeUnrestrictedSQLIdent(buf, collation.Locale, lex.EncNoFlags)
		return buf.String()
	}
	return node.ParamType.ColumnType() + "[]"
}

// ColumnType implements the interface T, returns the visible type
func (node *TEnum) ColumnType() string {
	if collation, ok := node.ParamType.(*TCollatedString); ok {
		// We cannot use node.ParamType.Format() directly here (and DRY
		// across the two branches of the if) because if we have an array
		// of collated strings, the COLLATE string must appear after the
		// square brackets.
		buf := &bytes.Buffer{}
		buf.WriteString(collation.TString.ColumnType())
		buf.WriteString("[] COLLATE ")
		lex.EncodeUnrestrictedSQLIdent(buf, collation.Locale, lex.EncNoFlags)
		return buf.String()
	}
	return "ENUM" + "()"
}

// ColumnType implements the interface T, returns the visible type
func (node *TSet) ColumnType() string {
	return "SET" + "()"
}

// ColumnType implements the interface T, returns the visible type
func (node *TBitArray) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString(VisibleTypeName[node.VisibleType])
	if node.Width > 0 {
		if node.Width == 1 && !node.Variable {
			// BIT(1) pretty-prints as just BIT.
			return buf.String()
		}
		fmt.Fprintf(buf, "(%d)", node.Width)
	}
	return buf.String()
}

// ColumnType implements the interface T, returns the visible type
func (node *TBool) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TBytes) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TCollatedString) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString(node.TString.ColumnType())
	buf.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(buf, node.Locale, lex.EncNoFlags)
	return buf.String()
}

// ColumnType implements the interface T, returns the visible type
func (node *TDate) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TDecimal) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString(VisibleTypeName[node.VisibleType])
	if node.Prec > 0 {
		fmt.Fprintf(buf, "(%d", node.Prec)
		if node.Scale > 0 {
			fmt.Fprintf(buf, ",%d", node.Scale)
		}
		buf.WriteByte(')')
	}
	return buf.String()
}

// ColumnType implements the interface T, returns the visible type
func (node *TFloat) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TIPAddr) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TInt) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TInterval) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TJSON) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TName) ColumnType() string { return node.TypeName() }

// ColumnType implements the interface T, returns the visible type
func (node *TOid) ColumnType() string { return node.TypeName() }

// ColumnType implements the interface T, returns the visible type
func (node *TSerial) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TString) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString(VisibleTypeName[node.VisibleType])
	if !(node.Variant == TStringVariantCHAR && node.N == 1) && node.N > 0 {
		fmt.Fprintf(buf, "(%d)", node.N)
	}
	return buf.String()
}

// ColumnType implements the interface T, returns the visible type
func (node *TVoid) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString(VisibleTypeName[node.VisibleType])
	return buf.String()
}

// ColumnType implements the interface T, returns the visible type
func (node *TTime) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString(VisibleTypeName[node.VisibleType])
	if node.PrecisionSet {
		fmt.Fprintf(buf, "(%d)", node.Precision)
	}
	return buf.String()
}

// ColumnType implements the interface T, returns the visible type
func (node *TTimestamp) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString(VisibleTypeName[node.VisibleType])
	if node.PrecisionSet {
		fmt.Fprintf(buf, "(%d)", node.Precision)
	}
	return buf.String()
}

// ColumnType implements the interface T, returns the visible type
func (node *TTimestampTZ) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString(VisibleTypeName[node.VisibleType])
	if node.PrecisionSet {
		fmt.Fprintf(buf, "(%d)", node.Precision)
	}
	return buf.String()
}

// ColumnType implements the interface T, returns the visible type
func (node *TUUID) ColumnType() string { return VisibleTypeName[node.VisibleType] }

// ColumnType implements the interface T, returns the visible type
func (node *TVector) ColumnType() string { return node.TypeName() }

// ColumnType implements the interface T, returns the visible type
func (node TTuple) ColumnType() string {
	buf := &bytes.Buffer{}
	buf.WriteString("(")
	for i := range node {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(node[i].ColumnType())
	}
	buf.WriteString(")")
	return buf.String()
}

// All Ts also implement CastTargetType.
func (*TArray) castTargetType()          {}
func (*TEnum) castTargetType()           {}
func (*TSet) castTargetType()            {}
func (*TBitArray) castTargetType()       {}
func (*TBool) castTargetType()           {}
func (*TBytes) castTargetType()          {}
func (*TCollatedString) castTargetType() {}
func (*TDate) castTargetType()           {}
func (*TDecimal) castTargetType()        {}
func (*TFloat) castTargetType()          {}
func (*TIPAddr) castTargetType()         {}
func (*TInt) castTargetType()            {}
func (*TInterval) castTargetType()       {}
func (*TJSON) castTargetType()           {}
func (*TName) castTargetType()           {}
func (*TOid) castTargetType()            {}
func (*TSerial) castTargetType()         {}
func (*TString) castTargetType()         {}
func (*TVoid) castTargetType()           {}
func (*TTime) castTargetType()           {}
func (*TTimestamp) castTargetType()      {}
func (*TTimestampTZ) castTargetType()    {}
func (*TUUID) castTargetType()           {}
func (*TVector) castTargetType()         {}
func (TTuple) castTargetType()           {}

func (node *TArray) String() string          { return ColTypeAsString(node) }
func (node *TEnum) String() string           { return ColTypeAsString(node) }
func (node *TSet) String() string            { return ColTypeAsString(node) }
func (node *TBitArray) String() string       { return ColTypeAsString(node) }
func (node *TBool) String() string           { return ColTypeAsString(node) }
func (node *TBytes) String() string          { return ColTypeAsString(node) }
func (node *TCollatedString) String() string { return ColTypeAsString(node) }
func (node *TDate) String() string           { return ColTypeAsString(node) }
func (node *TDecimal) String() string        { return ColTypeAsString(node) }
func (node *TFloat) String() string          { return ColTypeAsString(node) }
func (node *TIPAddr) String() string         { return ColTypeAsString(node) }
func (node *TInt) String() string            { return ColTypeAsString(node) }
func (node *TInterval) String() string       { return ColTypeAsString(node) }
func (node *TJSON) String() string           { return ColTypeAsString(node) }
func (node *TName) String() string           { return ColTypeAsString(node) }
func (node *TOid) String() string            { return ColTypeAsString(node) }
func (node *TSerial) String() string         { return ColTypeAsString(node) }
func (node *TString) String() string         { return ColTypeAsString(node) }
func (node *TVoid) String() string           { return ColTypeAsString(node) }
func (node *TTime) String() string           { return ColTypeAsString(node) }
func (node *TTimestamp) String() string      { return ColTypeAsString(node) }
func (node *TTimestampTZ) String() string    { return ColTypeAsString(node) }
func (node *TUUID) String() string           { return ColTypeAsString(node) }
func (node *TVector) String() string         { return ColTypeAsString(node) }
func (node TTuple) String() string           { return ColTypeAsString(node) }
