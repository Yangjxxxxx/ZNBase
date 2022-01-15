package sql

// #cgo CPPFLAGS: -I../../../c-deps/libplsql/include
// #cgo LDFLAGS: -lplsql
// #cgo LDFLAGS: -ldl
// #include <stdlib.h>
// #include <dlfcn.h>
// #include <string.h>
// // C helper functions:
// static int clang_udr_check(char* file_path, char* func_name){
//     typedef int (*FPTR)(int,int);
//     void *funcHandle = NULL;
//     char *perr = NULL;
//     funcHandle = dlopen(file_path, RTLD_LAZY);
//     if (!funcHandle) {
//         return -1;
//     }
//     FPTR fptr = (FPTR)dlsym(funcHandle, func_name);
//     perr = dlerror();
//     if (perr != NULL) {
//         dlclose(funcHandle);
//         return -2;
//     }
//     dlclose(funcHandle);
//     return 0;
// }
import "C"

import (
	"context"
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unsafe"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type createFunctionNode struct {
	n            *tree.CreateFunction
	dbDesc       *sqlbase.DatabaseDescriptor
	replaceHelp  replaceHelper
	fullFuncName string
}

//When the statement has `or replace`, isReplace is true.
//Identical means that the parameters are consistent with the function body
//and need to be replaced.
//Notice :if identical is true,it indicates that
//n is exactly the same as funcdesc, without any changes.
type replaceHelper struct {
	isReplace bool
	identical bool
}

//DoubleQM make double quotation marks twice
func DoubleQM(name string) string { // make double quotation marks twice
	var finalName string
	for _, c := range name {
		finalName += string(c)
		if c == '"' {
			finalName += string(c)
		}
	}
	return finalName
}

//DoubleQMSingle make single quotation marks twice
func DoubleQMSingle(name string) string { // make single quotation marks twice
	var finalName string
	for _, c := range name {
		finalName += string(c)
		if c == '\'' {
			finalName += string(c)
		}
	}
	return finalName
}

// IsLegalFuncName is to check that whether FuncName is legal,
// only allow letters, numbers and underline
func IsLegalFuncName(funcname string) error {
	funcname = strings.Trim(funcname, "\"") // trim  "
	funcname = strings.TrimSpace(funcname)  // trim blank space
	if len(funcname) == 0 {
		return errEmptyFunctionName
	} else if len(funcname) == 1 {
		if !unicode.IsLetter(rune(funcname[0])) {
			return errOneLetterFunctionName
		}
	}
	// funcname can not contains "(" or ")"
	if strings.Contains(funcname, "(") || strings.Contains(funcname, ")") {
		return errIllegalFunctionName
	}
	return nil
}

// CreateFunction creates a procedure.
// Privileges: CREATE on schema.
// Notes: postgres requires CREATE on schema.
// enable user to store a set of commands, and use them with calling procedure
func (p *planner) CreateFunction(ctx context.Context, n *tree.CreateFunction) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support user define function now!")
	}
	n.FuncName.ResolvedAsFunction = true
	FuncNameStr := n.FuncName.Table()
	if FuncNameStr == "" {
		return nil, errEmptyFunctionName
	}
	if err := IsLegalFuncName(FuncNameStr); err != nil {
		return nil, err
	}
	// all build in function can not overload
	if builtins.CheckBuiltinFunctionWithName(FuncNameStr) {
		return nil, pgerror.NewError(pgcode.InvalidName, "Build in function does not allow to overload!")
	}
	// length limitation
	if len(FuncNameStr) > 64 {
		return nil, pgerror.NewError(pgcode.InvalidName, "the length of function/procedure name can not be longer than 64")
	}
	// plsql don't support transaction now
	if n.PlFL.FileName == "" {
		var funcBody string
		if n.Options[0].IsAsPart {
			funcBody = strings.ToLower(n.Options[0].Arg[0])
		} else {
			funcBody = strings.ToLower(n.Options[1].Arg[0])
		}
		begin, end := strings.Index(funcBody, "begin"), strings.LastIndex(funcBody, "end")
		if begin > -1 && end > -1 {
			funcBody = funcBody[begin+5 : end]
		} else {
			return nil, pgerror.NewError(pgcode.Syntax, "syntax error :the begin or end not exist")
		}
		blocks := strings.Split(funcBody, ";")
		for _, v := range blocks {
			v = strings.TrimSpace(v)
			if len(v) != 0 {
				if strings.HasPrefix(v, "abort") {
					return nil, errors.New("procedure does not support transaction")
				}
			}
		}
		plsqlLanguage := ""
		if n.Options[0].IsAsPart {
			if len(n.Options) > 1 {
				plsqlLanguage = strings.ToLower(n.Options[1].Arg[0])
			} else {
				plsqlLanguage = "plsql"
			}
		} else {
			plsqlLanguage = strings.ToLower(n.Options[0].Arg[0])
		}
		if plsqlLanguage != "plsql" && plsqlLanguage != "plpgsql" {
			return nil, errors.Errorf("plsql's language name can only be \"PLSQL\" or \"PLPGSQL\"")
		}
		n.PlFL.Language = "plsql"
	} else {
		if !plLanguageCheck(n.PlFL.Language, tree.UDRExternLanguageList) {
			return nil, errors.Errorf("extern procedure/function language name can only use \"PLC\", \"PLCLANG\", \"PLGO\", \"PLGOLANG\", \"PLPYTHON\"")
		}
	}

	dbDesc, err := getDbDescByTable(ctx, p, &n.FuncName)
	if err != nil {
		return nil, err
	}

	scDesc, err := getScDesc(ctx, p, n.FuncName.SchemaName.String(), &n.FuncName)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, scDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	for _, parameter := range n.Parameters {
		if parameter.Mode == tree.FuncParamOUT {
			if n.IsProcedure {
				return nil, errors.Errorf("procedures cannot have OUT arguments")
			}
		}
	}

	paraStr, err := n.Parameters.GetFuncParasTypeStr()
	if err != nil {
		return nil, err
	}

	keyName := n.FuncName.Table() + paraStr
	oldKey := functionKey{parentID: scDesc.GetID(), name: keyName, funcName: n.FuncName.Table()}
	exist, err := descExists(ctx, p.txn, oldKey.Key())
	if err != nil {
		return nil, err
	}
	filePath := n.PlFL.FileName
	funcLanguage := n.PlFL.Language
	if funcLanguage == "plclang" {
		checkRes := C.clang_udr_check(C.CString(filePath), C.CString(FuncNameStr))
		if checkRes == -2 {
			return nil, errors.Errorf("Creation failed. This function is not defined in the external file")
		}
	}
	// 1. 如果有关键字 or replace
	// 	1.1 function descriptor中存在此funcName，则取出此function descriptor，比较funcDef
	// 		- 如果与sqlStr中的function body不同，则设置replace标识为true；
	// 		- 如果与sqlStr中的function body相同，则实际上function 没有改变，
	// 	1.2 function descriptor中不存在此funcName，则事实上为一个create function statement，执行正常的crete流程
	// 2. 如果没有关键字 or replace
	// 	2.1 为简单的create function statement
	//UDR-TODO： 抽象出一个函数
	replaceHelp := &replaceHelper{}
	if err = SetNodeReturnType(n); err != nil {
		return nil, err
	}
	if n.Replace {
		if exist {
			funcDesc, err := getFuncDesc(ctx, p, n.FuncName.Schema(), dbDesc, keyName)
			if err != nil {
				return nil, err
			}
			if err = p.CheckPrivilege(ctx, funcDesc, privilege.DROP); err != nil {
				return nil, err
			}
			if funcDesc.IsProcedure && !n.IsProcedure {
				return nil, errors.Errorf("cannot change routine kind, \"%s\" is a procedure.", keyName)
			} else if !funcDesc.IsProcedure && n.IsProcedure {
				return nil, errors.Errorf("cannot change routine kind, \"%s\" is a function.", keyName)
			}
			replaceHelp, err = checkInsTypeAndFuncBody(n, funcDesc)
			if err != nil {
				return nil, err
			}
			err = checkOutsType(keyName, n, funcDesc, replaceHelp)
			if err != nil {
				return nil, err
			}
		}
	} else {
		if exist {
			return nil, sqlbase.NewFunctionAlreadyExistsError(n.FuncName.Table())
		}
		_, nodeOuts, err := GetInoutParametersFromNode(n.Parameters)
		if err != nil {
			return nil, err
		}
		if n.ReturnType == nil && len(nodeOuts) == 0 && !n.IsProcedure {
			return nil, errors.Errorf("function result type must be specified")
		}
	}

	return &createFunctionNode{n: n, dbDesc: dbDesc, replaceHelp: *replaceHelp, fullFuncName: keyName}, nil
}

// CountFuncHashSQL through function define string generator hash string
func CountFuncHashSQL(sql string) string {
	sql = strings.ReplaceAll(sql, " ", "")
	sha1er := sha1.New()
	sha1er.Write([]byte(sql))
	ret := sha1er.Sum([]byte(""))
	return string(ret)
}

func (n *createFunctionNode) startExec(params runParams) error {
	r := MakeUDRRunParam(params.ctx, params.p.txn, params.ExecCfg(), params.extendedEvalCtx, nil)
	pl := NewPLSQL(unsafe.Pointer(&r), r.MagicCode)
	if isInternal := CheckVirtualSchema(n.n.FuncName.Schema()); isInternal {
		return fmt.Errorf("schema cannot be modified: %q", n.n.FuncName.Schema())
	}
	// exists and without change, return directly
	if n.replaceHelp.identical {
		return nil
	}
	var funcDesc *FunctionDescriptor
	defer func() {
		leaseMgr := params.p.extendedEvalCtx.Tables.leaseMgr
		leaseMgr.mu.Lock()
		defer leaseMgr.mu.Unlock()
		if funcDesc == nil {
			return
		}
		tables, ok := leaseMgr.mu.tables[funcDesc.ID]
		if ok {
			tables.mu.active.data = tables.mu.active.data[0:0]
		}
		params.p.extendedEvalCtx.Tables.leaseMgr.functionNames.removeFuncGrpByIDAndName(funcDesc.ParentID, funcDesc.Name)

	}()
	hashsql := CountFuncHashSQL(params.p.stmt.SQL)
	if !n.replaceHelp.isReplace { // crate new descriptor and write in batch
		creationTime := params.p.txn.CommitTimestamp()
		// Inherit permissions from the schema descriptor.
		privs := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Function, params.p.User())
		muTablefuncDesc, err := makeFunctionDesc(params, n.n, n.dbDesc.GetSchemaID(n.n.FuncName.Schema()), creationTime, privs, n.n.PlFL.Language)
		if err != nil {
			return err
		}
		funcDesc = &(muTablefuncDesc.FunctionDescriptor)
		funcDesc.ModificationTime = params.p.txn.ReadTimestamp()
		_, err = params.p.createFunction(params.ctx, funcDesc, n, false)
		if err != nil {
			return err
		}
	} else { // get keyName and descriptor to update descriptor in batch
		ctx := params.ctx
		p := params.p
		dbDesc, err := getDbDescByTable(ctx, p, &n.n.FuncName)
		if err != nil {
			return err
		}
		funcDesc, err = getFuncDesc(ctx, p, n.n.FuncName.Schema(), dbDesc, n.fullFuncName)
		if err != nil {
			return err
		}

		funcDesc.Language = strings.ToLower(n.n.PlFL.Language)
		funcDesc.ModificationTime = params.p.txn.ReadTimestamp()
		if n.n.PlFL.FileName == "" {
			funcDesc.FuncDef = getFuncDef(p.stmt.SQL)
			funcDesc.FileName = ""
			funcDesc.FilePath = ""
		} else {
			if _, err := os.Stat(n.n.PlFL.FileName); err == nil {
				funcDesc.FuncDef = "begin \n NULL ;\n end"
				funcDesc.FilePath = n.n.PlFL.FileName
				funcDesc.FileName = filepath.Base(funcDesc.FilePath)
			} else {
				if os.IsNotExist(err) {
					errMsg := fmt.Sprintf("Can not find file or directory named %s", n.n.PlFL.FileName)
					return pgerror.NewError(pgcode.UndefinedFile, errMsg)
				}
			}
		}
		funcDesc.Args = funcDesc.Args[0:0]
		for i, para := range n.n.Parameters {
			if _, ok := para.Typ.(*coltypes.TVector); ok {
				return pgerror.NewErrorf(
					pgcode.FeatureNotSupported,
					"VECTOR column types are unsupported",
				)
			}
			if para.Typ == nil {
				continue
			}
			tempFuncArg, err := sqlbase.MakeFuncArgDefDescs(&para, &params.p.semaCtx)
			if err != nil {
				return err
			}
			tempFuncArg.ID = sqlbase.ColumnID(i + 1)
			funcDesc.Args = append(funcDesc.Args, *tempFuncArg)
		}
		funcDesc.ArgNum = uint32(len(funcDesc.Args))
		if !n.n.IsProcedure {
			retstr, retOid, err := getTypstrAndTypOidFromColtyp(n.n.ReturnType)
			if err != nil {
				return err
			}
			funcDesc.ReturnType = retstr
			funcDesc.RetOid = retOid
		}

		funcDesc.Version++
		b := &client.Batch{}
		funcDesc.Hashsql = hashsql
		if err := p.UpdateDescriptor(ctx, b, funcDesc); err != nil {
			return err
		}

		mutableFuncDesc := sqlbase.NewMutableExistingFunctionDescriptor(*funcDesc)
		bb := p.txn.NewBatch()
		if err := p.writeFunctionDescToBatch(ctx, mutableFuncDesc, sqlbase.InvalidMutationID, bb); err != nil {
			return err
		}
		// if err := p.Tables().addUncommittedFunction(*mutableFuncDesc); err != nil {
		// 	return err
		// }
		if err := p.txn.Run(ctx, b); err != nil {
			return err
		}
	}
	funcDesc.Hashsql = hashsql
	if n.n.PlFL.FileName == "" {
		if err := pl.CallPlpgSQLHandler(params.ctx, Validator, unsafe.Pointer(funcDesc)); err != nil {
			return err
		}
	}

	logEventType := string(EventLogCreateFunction)
	funcOrProc := "Function"
	if n.n.IsProcedure {
		logEventType = string(EventLogCreateProcedure)
		funcOrProc = "Procedure"
	}
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: logEventType,
		TargetInfo: &server.TargetInfo{
			TargetID: int32(funcDesc.ID),
			Desc: struct {
				name string
			}{
				name: funcDesc.Name,
			},
		},
		Info: "User: " + params.SessionData().User + ", " + funcOrProc + ": " + n.fullFuncName + ", Language: " + funcDesc.Language,
	}
	return nil
}

func (*createFunctionNode) Next(runParams) (bool, error) { return false, nil }
func (*createFunctionNode) Values() tree.Datums          { return tree.Datums{} }
func (*createFunctionNode) Close(context.Context)        {}

// InitFunctionDescriptor returns a blank FunctionDescriptor.
func InitFunctionDescriptor(
	parentID sqlbase.ID,
	name string,
	privileges *sqlbase.PrivilegeDescriptor,
	language string,
	funcDef string,
) sqlbase.MutableFunctionDescriptor {
	return *sqlbase.NewMutableCreatedFunctionDescriptor(sqlbase.FunctionDescriptor{
		Name:       name,
		ParentID:   parentID,
		Args:       make([]sqlbase.FuncArgDescriptor, 0),
		Version:    1,
		Privileges: privileges,
		Language:   language,
		FuncDef:    funcDef,
	})
}

// makeFunctionDesc creates a function descriptor from a CreateFunction statement.
func makeFunctionDesc(
	params runParams,
	n *tree.CreateFunction,
	parentID sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	language string,
) (ret *sqlbase.MutableFunctionDescriptor, err error) {
	ret, err = MakeFunctionDesc(
		params.ctx,
		params.p.txn,
		params.p,
		params.p.ExecCfg().Settings,
		n,
		parentID,
		creationTime,
		privileges,
		&params.p.semaCtx,
		language,
	)
	return ret, err
}

// MakeFunctionDesc creates a table descriptor from a CreateTable statement.
func MakeFunctionDesc(
	ctx context.Context,
	txn *client.Txn,
	p *planner,
	st *cluster.Settings,
	n *tree.CreateFunction,
	parentID sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	semaCtx *tree.SemaContext,
	language string,
) (*sqlbase.MutableFunctionDescriptor, error) {
	stmt := ""
	if n.PlFL.FileName == "" {
		stmt = getFuncDef(p.stmt.SQL)
	} else {
		//stmt = "extern function in path " + n.PlFL.FileName
		stmt = "begin \n NULL ;\n end"
	}
	desc := InitFunctionDescriptor(parentID, n.FuncName.Table(), privileges, language, stmt)
	desc.IsProcedure = n.IsProcedure

	funcOutTypes := make([]coltypes.T, 0)
	for i, para := range n.Parameters {
		if _, ok := para.Typ.(*coltypes.TVector); ok {
			return nil, pgerror.NewErrorf(
				pgcode.FeatureNotSupported,
				"VECTOR column types are unsupported",
			)
		}

		if para.Typ == nil {
			continue
		}
		tempFuncArg, err := sqlbase.MakeFuncArgDefDescs(&para, semaCtx)
		if err != nil {
			return nil, err
		}
		tempFuncArg.ID = sqlbase.ColumnID(i + 1)
		desc.Args = append(desc.Args, *tempFuncArg)
		if para.Mode == tree.FuncParamINOUT || para.Mode == tree.FuncParamOUT {
			funcOutTypes = append(funcOutTypes, para.Typ)
		}
	}
	desc.ArgNum = uint32(len(desc.Args))
	// default return type is void
	desc.ReturnType = "VOID"
	desc.RetOid = oid.T_void
	if !n.IsProcedure {
		retstr, retOid, err := getTypstrAndTypOidFromColtyp(n.ReturnType)
		if err != nil {
			return nil, err
		}
		desc.ReturnType = retstr
		desc.RetOid = retOid
	}
	if n.PlFL.FileName == "" {
		desc.FilePath = ""
		desc.FileName = ""
	} else {
		if _, err := os.Stat(n.PlFL.FileName); err == nil {
			desc.FilePath = n.PlFL.FileName
			desc.FileName = filepath.Base(n.PlFL.FileName)
		} else {
			if os.IsNotExist(err) {
				errMsg := fmt.Sprintf("Can not find file or directory named %s", n.PlFL.FileName)
				return nil, pgerror.NewError(pgcode.UndefinedFile, errMsg)
			}
		}
	}

	return &desc, nil
}

func getTypstrAndTypOidFromColtyp(t coltypes.T) (string, oid.Oid, error) {
	retStr := strings.ToUpper(t.TypeName())
	if retOid, ok := tree.TypeNameGetOID[retStr]; ok {
		// 特殊处理几种无法区分的类型
		if _, okk := t.(*coltypes.TDecimal); okk {
			//retStr = fmt.Sprintf("Decimal(%d, %d)", tstring.Prec, tstring.Scale)
			retStr = t.ColumnType()
			return retStr, oid.T_numeric, nil
		}
		if _, okk := t.(*coltypes.TString); okk {
			return t.ColumnType(), oid.T_text, nil
		}
		if _, okk := t.(coltypes.TTuple); okk {
			return "RECORD", oid.T_record, nil
		}
		retStr = t.ColumnType()
		return retStr, retOid, nil
		//desc.RetOid = retOid
	}
	strErr := fmt.Sprintf("%s is not an invalid type.", retStr)
	return "", oid.T_unknown, errors.New(strErr)
}

// getFuncDef get function definition from a sql string
func getFuncDef(sqlStr string) string {
	currentSQLStr := sqlStr
	begin, end := strings.Index(currentSQLStr, "$$"), strings.LastIndex(currentSQLStr, "$$")
	currentSQLStr = currentSQLStr[begin+2 : end]
	return currentSQLStr
}

// pl language check, only support plsql, plpgsql, plclang, plgolang, plpython
func plLanguageCheck(language string, languageList []string) bool {
	for _, lang := range languageList {
		if lang == language {
			return true
		}
	}
	return false
}

//SetNodeReturnType function is used to set the ReturnType of the CreateFunction node.
//If the setting is successful, return nil, otherwise return the corresponding error message.
//If n is Function, its ReturnType cannot be nil after calling the function.
func SetNodeReturnType(n *tree.CreateFunction) error {
	if n.IsProcedure {
		return nil
	}
	funcOutTypes := make([]coltypes.T, 0)
	for _, para := range n.Parameters {
		if _, ok := para.Typ.(*coltypes.TVector); ok {
			return pgerror.NewErrorf(
				pgcode.FeatureNotSupported,
				"VECTOR column types are unsupported",
			)
		}
		//TODO : why para.Typ can be nil ?
		if para.Typ == nil {
			continue
		}
		switch para.Mode {
		case tree.FuncParamINOUT:
			fallthrough
		case tree.FuncParamOUT:
			funcOutTypes = append(funcOutTypes, para.Typ)
		case tree.FuncParamIN:
			continue
		default:
			return errors.Errorf("unsupported function parameter mode%q", para.Mode)
		}
	}
	switch {
	case n.ReturnType == nil && len(funcOutTypes) == 0:
		return errors.Errorf("function result type must be specified")
	case n.ReturnType == nil && len(funcOutTypes) == 1:
		n.ReturnType = funcOutTypes[0]
	case n.ReturnType == nil && len(funcOutTypes) > 1:
		n.ReturnType = coltypes.TTuple{}
	case n.ReturnType != nil && len(funcOutTypes) == 0:
		break
	case n.ReturnType != nil && len(funcOutTypes) == 1:
		if n.ReturnType.ColumnType() == funcOutTypes[0].ColumnType() {
			return nil
		}
		if l1, ok := isStringType(n.ReturnType.ColumnType()); ok {
			if l2, okk := isStringType(funcOutTypes[0].ColumnType()); okk && l1 == l2 {
				return nil
			}
		}
		return errors.Errorf("function result type must be %s because of OUT parameters", funcOutTypes[0].ColumnType())

	case n.ReturnType != nil && len(funcOutTypes) > 1:
		if _, ok := n.ReturnType.(coltypes.TTuple); !ok {
			return errors.Errorf("function result type must be tuple because of OUT parameters")
		}
	}
	return nil
}

//This function is used to check whether a string typName
//is of string type.
func isStringType(typName string) (int, bool) {
	declareStmt := fmt.Sprintf("declare %s %s", "tempVar", typName)
	stmt, err := parser.ParseOne(declareStmt, false)
	if err != nil {
		return -1, false
	}
	if td, ok := stmt.AST.(*tree.DeclareVariable); ok {
		if tstring, okk := td.VariableType.(*coltypes.TString); okk {
			return int(tstring.N), true
		}
	}
	return -1, false
}

//Check whether the return value types of
//n *tree.CreateFunction,
//funcDesc *FunctionDescriptor
//are consistent.
//See IsSameOutArgType(arg1, arg2 tree.FuncArgNameType) for more details.
func isSameReturnType(n *tree.CreateFunction, funcDesc *FunctionDescriptor) bool {
	if n.ReturnType != nil && funcDesc.ReturnType != "" {
		var nArg, fArg tree.FuncArgNameType
		if _, ok := n.ReturnType.(coltypes.TTuple); ok {
			nArg = tree.FuncArgNameType{TypeName: "RECORD"}
		} else {
			nArg = tree.FuncArgNameType{TypeName: n.ReturnType.ColumnType()}
		}
		fArg = tree.FuncArgNameType{TypeName: funcDesc.ReturnType}
		switch nArg.TypeName {
		case "INT2", "SERIAL2", "SMALLINT", "SMALLSERIAL":
			if fArg.TypeName == "INT2" || fArg.TypeName == "SERIAL2" || fArg.TypeName == "SMALLINT" || fArg.TypeName == "SMALLSERIAL" {
				return true
			}
		case "INT4", "SERIAL4":
			if fArg.TypeName == "INT4" || fArg.TypeName == "SERIAL4" {
				return true
			}
		case "INT8", "SERIAL8", "BIGINT", "BIGSERIAL", "INTEGER", "INT64", "INT", "SERIAL":
			if fArg.TypeName == "INT8" || fArg.TypeName == "SERIAL8" || fArg.TypeName == "BIGINT" || fArg.TypeName == "BIGSERIAL" ||
				fArg.TypeName == "INTEGER" || fArg.TypeName == "INT64" || fArg.TypeName == "INT" || fArg.TypeName == "SERIAL" {
				return true
			}
		case "BOOL", "BOOLEAN":
			if fArg.TypeName == "BOOL" || fArg.TypeName == "BOOLEAN" {
				return true
			}
		case "FLOAT4", "REAL":
			if fArg.TypeName == "FLOAT4" || fArg.TypeName == "REAL" {
				return true
			}
		case "FLOAT8", "DOUBLE PRECISION":
			if fArg.TypeName == "FLOAT8" || fArg.TypeName == "DOUBLE PRECISION" {
				return true
			}
		case "VARBIT", "BIT VARYING":
			if fArg.TypeName == "VARBIT" || fArg.TypeName == "BIT VARYING" {
				return true
			}
		case "BYTES", "BYTEA", "BLOB":
			if fArg.TypeName == "BYTES" || fArg.TypeName == "BYTEA" || fArg.TypeName == "BLOB" {
				return true
			}
		case "JSONB", "JSON":
			if fArg.TypeName == "JSONB" || fArg.TypeName == "JSON" {
				return true
			}
		case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
			if fArg.TypeName == "TIMESTAMP" || fArg.TypeName == "TIMESTAMP WITHOUT TIME ZONE" {
				return true
			}
		case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
			if fArg.TypeName == "TIMESTAMPTZ" || fArg.TypeName == "TIMESTAMP WITH TIME ZONE" {
				return true
			}
		default:
			return isSameOutArgType(nArg, fArg)
		}
	}
	return false
}

//Check whether the Out Args types are consistent.
//There are two different types between string with length and string without length.
//Strings without length can be replaced with each other.(string,varchar,text)
//Strings with lengths can be replaced if they have the same length.(char(x),character(x),string(x),varchar(x),char,character)
//Notice that char and character are the same as char(1)
//Can not replace between int2, int4, int8, float4, float8...
func isSameOutArgType(arg1, arg2 tree.FuncArgNameType) bool {

	switch {
	case arg1.TypeName == arg2.TypeName:
		return true
	case arg1.TypeName != arg2.TypeName:
		//all string
		if l1, ok := isStringType(arg1.TypeName); ok {
			if l2, okk := isStringType(arg2.TypeName); okk && l1 == l2 {
				return true
			}
			return false
		}
		//numeric and decimal
		stmt, err := parser.ParseOne(fmt.Sprintf("declare %s %s", "tempVar", arg1.TypeName), false)
		if err != nil {
			return false
		}
		if td, ok := stmt.AST.(*tree.DeclareVariable); ok {
			if _, okk := td.VariableType.(*coltypes.TDecimal); okk {
				stmt1, err := parser.ParseOne(fmt.Sprintf("declare %s %s", "tempVar", arg2.TypeName), false)
				if err != nil {
					return false
				}
				if td1, ok := stmt1.AST.(*tree.DeclareVariable); ok {
					if _, okk := td1.VariableType.(*coltypes.TDecimal); okk {
						return true
					}
				}
			}
		}

	default:
		return false
	}
	return false
}

//Check whether In Args and FuncDef are the same, and set replaceHelper.
//If args are the same,then they can replace each other.
//For example, in in args, all integer numbers are of the same type and
//can be replaced with each other. All string types, regardless of length information,
//are of the same type and can be replaced with each other.
func checkInsTypeAndFuncBody(
	n *tree.CreateFunction, funcDesc *FunctionDescriptor,
) (*replaceHelper, error) {
	replaceHelp := &replaceHelper{}
	replaceHelp.isReplace = n.Replace
	replaceHelp.identical = true
	descIns, _, err := GetInoutParametersFromDesc(funcDesc.Args)
	if err != nil {
		return nil, err
	}
	nodeIns, _, err := GetInoutParametersFromNode(n.Parameters)
	if err != nil {
		return nil, err
	}
	if len(descIns) == len(nodeIns) {
		for i := range nodeIns {
			if nodeIns[i].Name != descIns[i].Name {
				if descIns[i].Name != "" {
					return nil, pgerror.NewErrorf(pgcode.InvalidFunctionDefinition, "can not change the name of input parameter \"%s\"", descIns[i].Name)
				}
			}
			if isSameType := isSameInArgType(descIns[i], nodeIns[i], replaceHelp); !isSameType {
				replaceHelp.isReplace = false
				replaceHelp.identical = false
				return replaceHelp, nil
			}
		}
	}
	if (len(n.Options) > 0 && len(n.Options[0].Arg) > 0) && funcDesc.FuncDef != n.Options[0].Arg[0] {
		replaceHelp.identical = false
	}
	return replaceHelp, nil

}

//Check whether the In Args types are consistent.
//All string types are the same in types.
//All int types are the same in types.
//Numeric and decimal are the same in types.
func isSameInArgType(arg1, arg2 tree.FuncArgNameType, replaceHelp *replaceHelper) bool {
	switch {
	case arg1.TypeName == arg2.TypeName:
		return true
	case arg1.TypeName != arg2.TypeName:
		//arg1 and arg2 are all string type
		if _, ok1 := isStringType(arg1.TypeName); ok1 {
			if _, ok2 := isStringType(arg2.TypeName); ok2 {
				replaceHelp.identical = false
				return true
			}
		}
		//all int type
		switch arg1.TypeName {
		case "INTEGER", "INT64", "INT", "INT2", "INT4", "INT8", "SMALLINT", "BIGINT", "SERIAL", "SERIAL2", "SERIAL4", "SERIAL8", "SMALLSERIAL", "BIGSERIAL":
			if arg2.TypeName == "INTEGER" || arg2.TypeName == "INT64" || arg2.TypeName == "INT" || arg2.TypeName == "INT2" || arg2.TypeName == "INT4" || arg2.TypeName == "INT8" || arg2.TypeName == "SMALLINT" || arg2.TypeName == "BIGINT" ||
				arg2.TypeName == "SERIAL" || arg2.TypeName == "SMALLSERIAL" || arg2.TypeName == "BIGSERIAL" || arg2.TypeName == "SERIAL2" || arg2.TypeName == "SERIAL4" || arg2.TypeName == "SERIAL8" {
				replaceHelp.identical = false
				return true
			}
		}
		//all float type
		if arg1.TypeName == "FLOAT" || arg1.TypeName == "FLOAT4" || arg1.TypeName == "FLOAT8" || arg1.TypeName == "REAL" || arg1.TypeName == "DOUBLE PRECISION" {
			if arg2.TypeName == "FLOAT" || arg2.TypeName == "FLOAT4" || arg2.TypeName == "FLOAT8" || arg2.TypeName == "REAL" || arg2.TypeName == "DOUBLE PRECISION" {
				replaceHelp.identical = false
				return true
			}
		}
		//numeric and decimal
		stmt, err := parser.ParseOne(fmt.Sprintf("declare %s %s", "tempVar", arg1.TypeName), false)
		if err != nil {
			return false
		}
		if td, ok := stmt.AST.(*tree.DeclareVariable); ok {
			if _, okk := td.VariableType.(*coltypes.TDecimal); okk {
				stmt1, err := parser.ParseOne(fmt.Sprintf("declare %s %s", "tempVar", arg2.TypeName), false)
				if err != nil {
					return false
				}
				if td1, ok := stmt1.AST.(*tree.DeclareVariable); ok {
					if _, okk := td1.VariableType.(*coltypes.TDecimal); okk {
						replaceHelp.identical = false
						return true
					}
				}
			}
		}
		//BOOL ang BOOLEAN types
		if arg1.TypeName == "BOOL" || arg1.TypeName == "BOOLEAN" {
			if arg2.TypeName == "BOOL" || arg2.TypeName == "BOOLEAN" {
				replaceHelp.identical = false
				return true
			}
		}
		//VARBIT ang BIT VARYING types
		if arg1.TypeName == "VARBIT" || arg1.TypeName == "BIT VARYING" {
			if arg2.TypeName == "VARBIT" || arg2.TypeName == "BIT VARYING" {
				replaceHelp.identical = false
				return true
			}
		}
		//bytes bytea ang blob types
		if arg1.TypeName == "BYTES" || arg1.TypeName == "BYTEA" || arg1.TypeName == "BLOB" {
			if arg2.TypeName == "BYTES" || arg2.TypeName == "BYTEA" || arg1.TypeName == "BLOB" {
				replaceHelp.identical = false
				return true
			}
		}
		//jsonb ang json types
		if arg1.TypeName == "JSONB" || arg1.TypeName == "JSON" {
			if arg2.TypeName == "JSONB" || arg2.TypeName == "JSON" {
				replaceHelp.identical = false
				return true
			}
		}
		//TIMESTAMP ang TIMESTAMP WITHOUT TIME ZONE types
		if arg1.TypeName == "TIMESTAMP" || arg1.TypeName == "TIMESTAMP WITHOUT TIME ZONE" {
			if arg2.TypeName == "TIMESTAMP" || arg2.TypeName == "TIMESTAMP WITHOUT TIME ZONE" {
				replaceHelp.identical = false
				return true
			}
		}
		//TIMESTAMPTZ ang TIMESTAMP WITH TIME ZONE types
		if arg1.TypeName == "TIMESTAMPTZ" || arg1.TypeName == "TIMESTAMP WITH TIME ZONE" {
			if arg2.TypeName == "TIMESTAMPTZ" || arg2.TypeName == "TIMESTAMP WITH TIME ZONE" {
				replaceHelp.identical = false
				return true
			}
		}

		replaceHelp.isReplace = false
		replaceHelp.identical = false
		return false
	default:
		return false
	}
}

//This function should be used if All the In Args have been checked.
//This function is used to check the Arg of the Out type
//and whether the ReturnType is consistent.
//int2, int4, int8 are different types,
//strings with length and strings without length are different types,
//strings with different lengths are different types,
//they cannot be replaced with each other
//However, strings of the same length can be replaced with each other,
//and strings without length can be replaced with each other.
func checkOutsType(
	keyName string, n *tree.CreateFunction, funcDesc *FunctionDescriptor, replaceHelp *replaceHelper,
) error {
	_, descOuts, err := GetInoutParametersFromDesc(funcDesc.Args)
	if err != nil {
		return err
	}
	_, nodeOuts, err := GetInoutParametersFromNode(n.Parameters)
	if err != nil {
		return err
	}
	if funcDesc.IsProcedure {
		err = checkProcedureOutsType(descOuts, nodeOuts, keyName, n, funcDesc)
		return err
	}
	err = checkFunctionOutsType(descOuts, nodeOuts, keyName, n, funcDesc)
	if err == nil && n.ReturnType.ColumnType() != funcDesc.ReturnType {
		replaceHelp.identical = false
	}
	return err
}

//Check out args when n and funcDesc are all procedures.
func checkProcedureOutsType(
	descOuts, nodeOuts []tree.FuncArgNameType,
	keyName string,
	n *tree.CreateFunction,
	funcDesc *FunctionDescriptor,
) error {
	// All the In Args have been checked
	descOutsLen := len(descOuts)
	nodeOutsLen := len(nodeOuts)
	switch {
	case descOutsLen != nodeOutsLen:
		if descOutsLen == 0 || nodeOutsLen == 0 {
			return errors.Errorf("cannot change whether a procedure has output parameters\nUse DROP PROCEDURE %s first.", keyName)
		}
		return errors.Errorf("cannot change return type of existing function\nDetail: Row type defined by OUT parameters is different.\nUse DROP PROCEDURE %s first.", keyName)
	case descOutsLen == 0 && nodeOutsLen == 0:
		return nil
	case descOutsLen == 1 && nodeOutsLen == 1:
		fallthrough
	case descOutsLen > 1 && nodeOutsLen > 1:
		// descOutsLen == nodeOutsLen
		for i := 0; i < descOutsLen; i++ {
			// check name
			if descOuts[i].Name != nodeOuts[i].Name {
				return errors.Errorf("cannot change return type of existing function\nDetail: Row type defined by OUT parameters is different.\nUse DROP PROCEDURE %s first.", keyName)
			}
			// check type
			if !isSameOutArgType(descOuts[i], nodeOuts[i]) {
				return errors.Errorf("cannot change return type of existing function\nDetail: Row type defined by OUT parameters is different.\nUse DROP PROCEDURE %s first.", keyName)
			}
		}
	}
	return nil
}

//Check out args when n and funcDesc are all functions.
func checkFunctionOutsType(
	descOuts, nodeOuts []tree.FuncArgNameType,
	keyName string,
	n *tree.CreateFunction,
	funcDesc *FunctionDescriptor,
) error {
	// All the In Args have been checked

	//descIns, descOuts, err := GetInoutParametersFromDesc(funcDesc.Args)
	//if err != nil {
	//	return false, err
	//}
	//nodeIns, nodeOuts, err := GetInoutParametersFromNode(n.Parameters)
	//if err != nil {
	//	return false, err
	//}
	descOutsLen := len(descOuts)
	nodeOutsLen := len(nodeOuts)
	switch {
	case descOutsLen == 0 && nodeOutsLen == 0:
		if !isSameReturnType(n, funcDesc) {
			return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
		}
	case descOutsLen == 0 && nodeOutsLen == 1:
		if !isSameReturnType(n, funcDesc) {
			return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
		}
	case descOutsLen == 0 && nodeOutsLen > 1:
		return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
	case descOutsLen == 1 && nodeOutsLen == 0:
		if !isSameReturnType(n, funcDesc) {
			return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
		}
	case descOutsLen == 1 && nodeOutsLen == 1:
		if descOuts[0].Name != nodeOuts[0].Name {
			return errors.Errorf("cannot change return type of existing function\nDetail: Row type defined by OUT parameters is different.\nUse DROP FUNCTION %s first.", keyName)
		}
		if !isSameReturnType(n, funcDesc) {
			return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
		}
	case descOutsLen == 1 && nodeOutsLen > 1:
		return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
	case descOutsLen > 1 && nodeOutsLen == 0:
		return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
	case descOutsLen > 1 && nodeOutsLen == 1:
		return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
	case descOutsLen > 1 && nodeOutsLen > 1:
		if len(descOuts) == len(nodeOuts) {
			for i := range nodeOuts {
				if descOuts[i].Name != nodeOuts[i].Name {
					return errors.Errorf("cannot change return type of existing function\nDetail: Row type defined by OUT parameters is different.\nUse DROP FUNCTION %s first.", keyName)
				}
				if !isSameOutArgType(descOuts[i], nodeOuts[i]) {
					return errors.Errorf("cannot change return type of existing function\nUse DROP FUNCTION %s first.", keyName)
				}
			}
		} else {
			return errors.Errorf("cannot change return type of existing function\nDetail: Row type defined by OUT parameters is different.\nUse DROP FUNCTION %s first.", keyName)
		}
	}
	return nil
}
