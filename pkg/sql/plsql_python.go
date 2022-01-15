// +build PLPYTHON

package sql

// #cgo CPPFLAGS: -I../../../c-deps/libplsql/include
// #cgo linux LDFLAGS: -lpython3.8
// #cgo LDFLAGS: -lplsql
// #cgo LDFLAGS: -ldl
// #include <stdlib.h>
// #ifndef PLSQL
// #define PLSQL
// #include "../../../c-deps/libplsql/include/plpgsql.h"
// #endif
// #include "../../../c-deps/libplsql/include/znbase/znbase.h"
import "C"
import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

type actionType int

const (
	_ actionType = iota
	// CallFunction represent call/select statement
	CallFunction = 1
	// Validator represent create function statement
	Validator = 2
)

type funcType int

//UnsafePointerMap because the uintptr unsupport transfer to UnsafePointer,so use map to cover
var UnsafePointerMap sync.Map

const (
	_ funcType = iota
	// ProcedureType is always 43 now
	ProcedureType = 43
)

func init() {
	//UnsafePointerMap = make(map[uint64]interface{})
	C._PG_init()
}

// PLSQL store handler(runParams) and magic code(verify code)
type PLSQL struct {
	// handler is the address of runParams
	Handler   unsafe.Pointer
	MagicCode int
}

// PLFunctionInfo store some functionCall info
type PLFunctionInfo struct {
	BaseData C.FunctionCallInfoBaseDataForGO
}

// NewPLSQL return a new PLSQL
func NewPLSQL(handler unsafe.Pointer, magicCode int) PLSQL {
	return PLSQL{
		Handler:   handler,
		MagicCode: magicCode,
	}
}

// MakePLFunctionInfo make plFunction info from a function ID
func MakePLFunctionInfo(functionID oid.Oid) *PLFunctionInfo {
	var fmgrInfo C.FmgrInfo
	fmgrInfo.fn_oid = C.Oid(functionID)
	var baseData C.FunctionCallInfoBaseDataForGO
	baseData.flinfo = &fmgrInfo

	function := &PLFunctionInfo{
		BaseData: baseData,
	}
	return function
}

// CallPlpgSQLHandler is a entrance to call pl_gram, which for plsql and plpython
func (p *PLSQL) CallPlpgSQLHandler(
	ctx context.Context, action actionType, Pfuncdesc unsafe.Pointer,
) error {
	var baseData C.FunctionCallInfoBaseDataForGO
	var sflinfo C.FmgrInfo
	baseData.sflinfo = sflinfo
	var handler C.Handler_t
	var vec C.Pointer_vec
	//转换unsafePointer到uint64
	unsafePointerDesc := UnsafePointer2Int64(Pfuncdesc)
	handler.Pointer_func_desc = (C.ulong)(unsafePointerDesc)
	UnsafePointerMap.Store(unsafePointerDesc, (*sqlbase.FunctionDescriptor)(Pfuncdesc))
	//UnsafePointerMap[unsafePointerDesc] = (*sqlbase.FunctionDescriptor)(Pfuncdesc)

	unsafePointerHandler := UnsafePointer2Int64(p.Handler)
	vec.pointer = (C.ulong)(unsafePointerHandler)
	handler.magicCode = (C.int)(p.MagicCode)
	UnsafePointerMap.Store(unsafePointerHandler, (*UDRRunParam)(p.Handler))
	//UnsafePointerMap[unsafePointerHandler] = (*UDRRunParam)(p.Handler)

	errMessage := make([]byte, 8192)
	logPath := p.GetLogsDirPath(ctx)
	state := C.PLSQL_external_for_znbase(handler, C.PLSQLACTION(action),
		(*C.char)(unsafe.Pointer(&errMessage[0])),
		vec,
		C.CString(logPath),
		baseData,
	)
	success := bool(state)
	//释放map中的unsafePointer
	UnsafePointerMap.Store(unsafePointerHandler, nil)
	UnsafePointerMap.Store(unsafePointerDesc, nil)
	//UnsafePointerMap[unsafePointerHandler] = nil
	//UnsafePointerMap[unsafePointerDesc] = nil
	if !success {
		return errors.Errorf(string(errMessage))
	}
	return nil
}

// UnsafePointer2Int64 convert unsafe.pointer to int64
func UnsafePointer2Int64(u unsafe.Pointer) uint64 {
	int16String := fmt.Sprintf("%p", u)

	pointerInt64, err := strconv.ParseInt(int16String, 0, 0)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	return (uint64)(pointerInt64)
}

// GetLogsDirPath get log dir
func (p *PLSQL) GetLogsDirPath(ctx context.Context) string {
	path := flag.Lookup("log-dir").Value.String()
	return path
}
