package tree

import (
	"fmt"
	"strings"

	"github.com/lib/pq/oid"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
)

// FunctionParameters is a slice of FunctionParameter
type FunctionParameters []FunctionParameter

// Format a FunctionParameters method
func (*FunctionParameters) Format(ctx *FmtCtx) {}

// FunctionParameter a function
type FunctionParameter struct {
	Name    string
	Typ     coltypes.T
	Mode    FuncParam
	DefExpr Expr
}

// FuncArgNameType use to help judge args when create function
type FuncArgNameType struct {
	Name     string
	Oid      oid.Oid
	TypeName string
	Mode     FuncParam
}

// Format a FunctionParameter method
func (*FunctionParameter) Format(ctx *FmtCtx) {}

// FuncParam is byte type
type FuncParam byte

// Format a FuncParam method
func (*FuncParam) Format(ctx *FmtCtx) {}

// MaxFuncSelectNum define the max function/procedure select/call times
const MaxFuncSelectNum = 30

const (
	// FuncParamIN represent func param can use in
	FuncParamIN FuncParam = 'i'
	// FuncParamOUT represent func param can use out
	FuncParamOUT FuncParam = 'o'
	// FuncParamINOUT represent func param can use inout
	FuncParamINOUT FuncParam = 'b'
	// FuncParamVariadic represent func param can use variadic
	FuncParamVariadic FuncParam = 'v'
	// FuncParamTable represent func param can use as a table
	FuncParamTable FuncParam = 't'
)

// CursorState an new type of int
type CursorState int

const (
	// NotCursor represent variable not cursor
	NotCursor CursorState = 0
	// CursorUndeclared represents a cursor definition failure
	CursorUndeclared CursorState = 1
	// CursorDeclared represent cursor is declared
	CursorDeclared CursorState = 2
	// CursorOpened represent cursor is opened
	CursorOpened CursorState = 3
	// CursorClosed represent cursor is closed
	CursorClosed CursorState = 4
)

// DefElem store function define
type DefElem struct {
	IsAsPart bool
	Arg      []string /* currently does not implement Node *arg */
}

// Format format DefElem
func (*DefElem) Format(ctx *FmtCtx) {}

// String implement interface Stringer
func (de *DefElem) String() string { return AsString(de) }

// DefElems is a slice of DefElem
type DefElems []DefElem

// CreateFunction use for create function/procedure
type CreateFunction struct {
	IsProcedure bool
	Replace     bool
	FuncName    TableName
	Parameters  FunctionParameters
	Options     DefElems
	ReturnType  coltypes.T
	PlFL        PLFileLanguage
}

// PLFileLanguage store procedure/function's language and file path
type PLFileLanguage struct {
	FileName string
	Language string
}

//UDRExternLanguageList []string Supported UDR languages
var UDRExternLanguageList = []string{"plgolang", "plgo", "plclang", "plc", "plpython"}

// Format implements the NodeFormatter interface.
func (n *CreateFunction) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if n.Replace {
		ctx.WriteString("OR REPLACE ")
	}
	ctx.WriteString("PROCEDURE ")
	ctx.FormatNode(&n.FuncName)
	n.FormatBody(ctx)
}

// FormatBody format the create function
func (n *CreateFunction) FormatBody(ctx *FmtCtx) {
	ctx.WriteString("(")
	ctx.WriteString("a int")
	ctx.FormatNode(&n.Parameters)
	ctx.WriteByte(')')
	if n.PlFL.FileName == "" {
		if n.Options[0].IsAsPart {
			ctx.WriteString(" AS $$ ")
			ctx.WriteString(n.Options[0].Arg[0])
			ctx.WriteString(" $$")
			if len(n.Options) > 1 {
				ctx.WriteString(" LANGUAGE ")
				ctx.WriteString(n.Options[1].Arg[0])
			}
		} else {
			ctx.WriteString(" LANGUAGE ")
			ctx.WriteString(n.Options[0].Arg[0])
			ctx.WriteString(" AS $$ ")
			ctx.WriteString(n.Options[1].Arg[0])
			ctx.WriteString(" $$")
		}
	} else {
		ctx.WriteString(" AS EXTERN FUNCTION IN PATH ")
		ctx.WriteString(n.PlFL.FileName)
		ctx.WriteString(" LANGUAGE ")
		ctx.WriteString(n.PlFL.Language)
	}
}

// CallStmt use for call procedure
type CallStmt struct {
	FuncName          ResolvableFunctionReference
	Args              Exprs
	FixedFunctionName TableName
}

// Format implements the NodeFormatter interface.
func (node *CallStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("CALL ")
	ctx.FormatNode(&node.FuncName)
	node.FormatBody(ctx)
}

// FormatBody format call statement
func (node *CallStmt) FormatBody(ctx *FmtCtx) {
	ctx.WriteString(" (")
	ctx.FormatNode(&node.Args)
	ctx.WriteByte(')')

}

// DropFunction use for drop procedure/function
type DropFunction struct {
	IsFunction   bool
	FuncName     TableName
	Parameters   FunctionParameters
	DropBehavior DropBehavior
	MissingOk    bool
	Concurrent   bool
}

// Format implements the NodeFormatter interface.
func (n *DropFunction) Format(ctx *FmtCtx) {
	if n.IsFunction {
		ctx.WriteString("DROP FUNCTION ")
	} else {
		ctx.WriteString("DROP PROCEDURE ")
	}
	if n.MissingOk {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.WriteString(n.FuncName.String())
	paramStr, _ := n.Parameters.GetFuncParasTypeStr()
	ctx.WriteString(paramStr)
}

// GetFuncParasTypeStr generate a string that contain funcName and parasType string.
func (funcParas FunctionParameters) GetFuncParasTypeStr() (string, error) {
	var resStr string
	resStr += "("
	for _, para := range funcParas {
		if para.Typ != nil && (para.Mode == FuncParamIN || para.Mode == FuncParamINOUT) {
			typStr := strings.ToUpper(para.Typ.TypeName())
			//colStr := para.Typ.ColumnType()
			//if strings.HasPrefix(colStr, "NUMERIC") {
			//	//UDR-TODO：？
			//	typStr = strings.Replace(typStr, "DECIMAL", "NUMERIC", 1)
			//}

			// char, character, varchar, text 均为string类型的别名
			// 规定 char, varchar, character, string, text 为同一类型
			if tstring, okk := para.Typ.(*coltypes.TString); okk {
				if tstring.Variant == coltypes.TStringVariantCHAR || tstring.Variant == coltypes.TStringVariantCHARACTER ||
					tstring.Variant == coltypes.TStringVariantVARCHAR ||
					(tstring.Variant == coltypes.TStringVariantSTRING && tstring.VisibleType == coltypes.VisTEXT) ||
					(tstring.Variant == coltypes.TStringVariantSTRING && tstring.VisibleType == coltypes.VisSTRING) {
					// idOfType = oid.T_varchar
					typStr = "STRING"
				}
			}
			resStr += typStr
			resStr += ","
		}
	}
	if len(resStr) > 1 {
		resStr = resStr[:len(resStr)-1]
	}
	resStr += ")"
	resStr = strings.Replace(resStr, "SERIAL2", "INT", -1)
	resStr = strings.Replace(resStr, "SERIAL4", "INT", -1)
	resStr = strings.Replace(resStr, "SERIAL8", "INT", -1)
	resStr = strings.Replace(resStr, "SERIAL", "INT", -1)
	resStr = strings.Replace(resStr, "SMALLSERIAL", "INT", -1)
	resStr = strings.Replace(resStr, "BIGSERIAL", "INT", -1)
	resStr = strings.Replace(resStr, "SMALLINT", "INT", -1)
	resStr = strings.Replace(resStr, "BIGINT", "INT", -1)
	resStr = strings.Replace(resStr, "INT64", "INT", -1)
	resStr = strings.Replace(resStr, "INTEGER", "INT", -1)
	resStr = strings.Replace(resStr, "INT8", "INT", -1)
	resStr = strings.Replace(resStr, "INT4", "INT", -1)
	resStr = strings.Replace(resStr, "INT2", "INT", -1)
	resStr = strings.Replace(resStr, "FLOAT4", "FLOAT", -1)
	resStr = strings.Replace(resStr, "FLOAT8", "FLOAT", -1)
	return resStr, nil
}

// UDRVar store UDR variable information
// contain name, oid, datum, state
type UDRVar struct {
	VarName     string
	VarOid      oid.Oid
	IsNull      bool
	NotNull     bool
	IsCursorArg bool
	VarDatum    Datum
	State       CursorState
	ColType     coltypes.T
	AliasNames  []string
}

// UDRVars is a slice of UDRVar
type UDRVars []UDRVar

// FindNameInNames use to find name in a strig slice
func FindNameInNames(name string, names []string) bool {
	for _, s := range names {
		if s == name {
			return true
		}
	}
	return false
}

// ErrPretty pretty the error and return a string
func ErrPretty(err error) string {
	if pgerr, ok := err.(*pgerror.Error); ok {
		goerr := fmt.Sprintf("invalid syntax: %v", err)
		if pgerr.Detail != "" {
			goerr = fmt.Sprintf("%s\nDETAIL:%s", goerr, pgerr.Detail)
		}
		if pgerr.Hint != "" {
			goerr = fmt.Sprintf("%s\nHINT:%s", goerr, pgerr.Hint)
		}
		return goerr
	}
	return err.Error()
}

// TypeNameGetOID :
// 该map和znbase类型对应不同，使用时注意
// 主要不同点在于：
// string 和 varchar 对应同一id
// 原本的"_XXX" 修改成为 "XXX[]"
var TypeNameGetOID = map[string]oid.Oid{
	"BOOL":             oid.T_bool,
	"BYTEA":            oid.T_bytea,
	"BYTES":            oid.T_bytea,
	"BYTES[]":          oid.T__bytea,
	"CHAR":             oid.T_bpchar,
	"NAME":             oid.T_name,
	"INT":              oid.T_int8,
	"INT8":             oid.T_int8,
	"INT2":             oid.T_int2,
	"INT2VECTOR":       oid.T_int2vector,
	"INT4":             oid.T_int4,
	"SERIAL":           oid.T_int8,
	"SERIAL2":          oid.T_int2,
	"SERIAL4":          oid.T_int4,
	"SERIAL8":          oid.T_int8,
	"REGPROC":          oid.T_regproc,
	"TEXT":             oid.T_text,
	"STRING":           oid.T_text,
	"OID":              oid.T_oid,
	"TID":              oid.T_tid,
	"XID":              oid.T_xid,
	"CID":              oid.T_cid,
	"OIDVECTOR":        oid.T_oidvector,
	"PG_DDL_COMMAND":   oid.T_pg_ddl_command,
	"PG_TYPE":          oid.T_pg_type,
	"PG_ATTRIBUTE":     oid.T_pg_attribute,
	"PG_PROC":          oid.T_pg_proc,
	"PG_CLASS":         oid.T_pg_class,
	"JSON":             oid.T_json,
	"XML":              oid.T_xml,
	"XML[]":            oid.T__xml,
	"PG_NODE_TREE":     oid.T_pg_node_tree,
	"JSON[]":           oid.T__json,
	"SMGR":             oid.T_smgr,
	"INDEX_AM_HANDLER": oid.T_index_am_handler,
	"POINT":            oid.T_point,
	"LSEG":             oid.T_lseg,
	"PATH":             oid.T_path,
	"BOX":              oid.T_box,
	"POLYGON":          oid.T_polygon,
	"LINE":             oid.T_line,
	"LINE[]":           oid.T__line,
	"CIDR":             oid.T_cidr,
	"_CIDR[]":          oid.T__cidr,
	"FLOAT4":           oid.T_float4,
	"FLOAT8":           oid.T_float8,
	"ABSTIME":          oid.T_abstime,
	"RELTIME":          oid.T_reltime,
	"TINTERVAL":        oid.T_tinterval,
	"UNKNOWN":          oid.T_unknown,
	"CIRCLE":           oid.T_circle,
	"CIRCLE[]":         oid.T__circle,
	"MONEY":            oid.T_money,
	"MONEY[]":          oid.T__money,
	"MACADDR":          oid.T_macaddr,
	"INET":             oid.T_inet,
	"BOOL[]":           oid.T__bool,
	"BYTEA[]":          oid.T__bytea,
	"CHAR[]":           oid.T__bpchar,
	"NAME[]":           oid.T__name,
	"INT2[]":           oid.T__int2,
	"INT2VECTOR[]":     oid.T__int2vector,
	"INT4[]":           oid.T__int4,
	"REGPROC[]":        oid.T__regproc,
	"TEXT[]":           oid.T__text,
	"STRING[]":         oid.T__text,
	"TID[]":            oid.T__tid,
	"XID[]":            oid.T__xid,
	"CID[]":            oid.T__cid,
	"OIDVECTOR[]":      oid.T__oidvector,
	"BPCHAR[]":         oid.T__bpchar,
	"VARCHAR[]":        oid.T__varchar,
	"INT8[]":           oid.T__int8,
	"POINT[]":          oid.T__point,
	"LSEG[]":           oid.T__lseg,
	"PATH[]":           oid.T__path,
	"BOX[]":            oid.T__box,
	"FLOAT4[]":         oid.T__float4,
	"FLOAT8[]":         oid.T__float8,
	"ABSTIME[]":        oid.T__abstime,
	"RELTIME[]":        oid.T__reltime,
	"TINTERVAL[]":      oid.T__tinterval,
	"POLYGON[]":        oid.T__polygon,
	"OID[]":            oid.T__oid,
	"ACLITEM":          oid.T_aclitem,
	"ACLITEM[]":        oid.T__aclitem,
	"MACADDR[]":        oid.T__macaddr,
	"INET[]":           oid.T__inet,
	"BPCHAR":           oid.T_bpchar,
	"VARCHAR":          oid.T_varchar,
	"DATE":             oid.T_date,
	"TIME":             oid.T_time,
	"TIMESTAMP":        oid.T_timestamp,
	"TIMESTAMP[]":      oid.T__timestamp,
	"DATE[]":           oid.T__date,
	"TIME[]":           oid.T__time,
	"TIMESTAMPTZ":      oid.T_timestamptz,
	"TIMESTAMPTZ[]":    oid.T__timestamptz,
	"INTERVAL":         oid.T_interval,
	"INTERVAL[]":       oid.T__interval,
	"NUMERIC[]":        oid.T__numeric,
	"PG_DATABASE":      oid.T_pg_database,
	"CSTRING[]":        oid.T__cstring,
	"TIMETZ":           oid.T_timetz,
	"TIMETZ[]":         oid.T__timetz,
	"BIT":              oid.T_bit,
	"BIT[]":            oid.T__bit,
	"VARBIT":           oid.T_varbit,
	"VARBIT[]":         oid.T__varbit,
	"DECIMAL":          oid.T_numeric,
	"NUMERIC":          oid.T_numeric,
	"REFCURSOR":        oid.T_refcursor,
	"REFCURSOR[]":      oid.T__refcursor,
	"REGPROCEDURE":     oid.T_regprocedure,
	"REGOPER":          oid.T_regoper,
	"REGOPERATOR":      oid.T_regoperator,
	"REGCLASS":         oid.T_regclass,
	"REGTYPE":          oid.T_regtype,
	"REGPROCEDURE[]":   oid.T__regprocedure,
	"REGOPER[]":        oid.T__regoper,
	"REGOPERATOR[]":    oid.T__regoperator,
	"REGCLASS[]":       oid.T__regclass,
	"REGTYPE[]":        oid.T__regtype,
	"RECORD":           oid.T_record,
	"TUPLE":            oid.T_record,
	"CSTRING":          oid.T_cstring,
	"ANY":              oid.T_any,
	"ANYARRAY":         oid.T_anyarray,
	"VOID":             oid.T_void,
	"TRIGGER":          oid.T_trigger,
	"LANGUAGE_HANDLER": oid.T_language_handler,
	"INTERNAL":         oid.T_internal,
	"OPAQUE":           oid.T_opaque,
	"ANYELEMENT":       oid.T_anyelement,
	"RECORD[]":         oid.T__record,
	"ANYNONARRAY":      oid.T_anynonarray,
	"PG_AUTHID":        oid.T_pg_authid,
	"PG_AUTH_MEMBERS":  oid.T_pg_auth_members,
	"TXID_SNAPSHOT[]":  oid.T__txid_snapshot,
	"UUID":             oid.T_uuid,
	"UUID[]":           oid.T__uuid,
	"TXID_SNAPSHOT":    oid.T_txid_snapshot,
	"FDW_HANDLER":      oid.T_fdw_handler,
	"PG_LSN":           oid.T_pg_lsn,
	"PG_LSN[]":         oid.T__pg_lsn,
	"TSM_HANDLER":      oid.T_tsm_handler,
	"ANYENUM":          oid.T_anyenum,
	"TSVECTOR":         oid.T_tsvector,
	"TSQUERY":          oid.T_tsquery,
	"GTSVECTOR":        oid.T_gtsvector,
	"TSVECTOR[]":       oid.T__tsvector,
	"GTSVECTOR[]":      oid.T__gtsvector,
	"TSQUERY[]":        oid.T__tsquery,
	"REGCONFIG":        oid.T_regconfig,
	"REGCONFIG[]":      oid.T__regconfig,
	"REGDICTIONARY":    oid.T_regdictionary,
	"REGDICTIONARY[]":  oid.T__regdictionary,
	"JSONB":            oid.T_json,
	"JSONB[]":          oid.T__json,
	"ANYRANGE":         oid.T_anyrange,
	"EVENT_TRIGGER":    oid.T_event_trigger,
	"INT4RANGE":        oid.T_int4range,
	"_INT4RANGE":       oid.T__int4range,
	"NUMRANGE":         oid.T_numrange,
	"NUMRANGE[]":       oid.T__numrange,
	"TSRANGE":          oid.T_tsrange,
	"TSRANGE[]":        oid.T__tsrange,
	"TSTZRANGE":        oid.T_tstzrange,
	"TSTZRANGE[]":      oid.T__tstzrange,
	"DATERANGE":        oid.T_daterange,
	"DATERANGE[]":      oid.T__daterange,
	"INT8RANGE":        oid.T_int8range,
	"INT8RANGE[]":      oid.T__int8range,
	"PG_SHSECLABEL":    oid.T_pg_shseclabel,
	"REGNAMESPACE":     oid.T_regnamespace,
	"REGNAMESPACE[]":   oid.T__regnamespace,
	"REGROLE":          oid.T_regrole,
	"REGROLE[]":        oid.T__regrole,
}

// Function use for grant/revoke procedure/function
type Function struct {
	FuncName    TableName
	Parameters  FunctionParameters
	IsProcedure bool
}

// Format implements the NodeFormatter interface.
func (f *Function) Format(ctx *FmtCtx) {
	ctx.FormatNode(&f.FuncName)
	ctx.WriteString("(")
	ctx.FormatNode(&f.Parameters)
	ctx.WriteByte(')')
}
