// Copyright 2016  The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"plugin"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/opt/exec/execbuilder"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/opt/optbuilder"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgwirebase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/udr/bepi"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// #cgo CPPFLAGS: -I../../c-deps/libplsql/include
// #cgo LDFLAGS: -lplsql
// #cgo LDFLAGS: -ldl
// #cgo linux LDFLAGS: -lrt -lpthread
// #cgo windows LDFLAGS: -lshlwapi -lrpcrt4
//
// #include <plpgsql.h>
// #include <stdlib.h>
//
// // C helper functions:
//
// static char **makeCharArray(int size) {
//     return calloc(sizeof(char*), size);
// }
//
// static void setArrayString(char **a, char *s, int n) {
//     a[n] = s;
// }
//
// static void freeCharArray(char **a, int size) {
//     int i;
//     for (i = 0; i < size; i++)
//         free(a[i]);
//     free(a);
// }
import "C"

//MagicCodeMin a const
const MagicCodeMin = 0

//MagicCodeMax a const
const MagicCodeMax = 100000

//ExecInUDR a var bool type
var ExecInUDR bool

// UDRExecResult a sturct
type UDRExecResult struct {
	Res      []tree.Datums
	RowCount int
	Names    []string
}

// UDRExecRes a var use to get execute result
var UDRExecRes = &UDRExecResult{Res: make([]tree.Datums, 0), RowCount: 0}

func init() {
	tree.ExecChan = make(chan tree.ExecUdrArgs)
	_ = os.Setenv("GODEBUG", "cgocheck=0")
	go WaitAndExecUDR()
}

//WaitAndExecUDR a fuction
func WaitAndExecUDR() {
	for {
		accept := <-tree.ExecChan
		go ExecUdrFunc(accept)
	}
}

//ExecUdrFunc use to exexcute udr function
func ExecUdrFunc(arg tree.ExecUdrArgs) {
	arg.Ctx.SelectCount++
	extent := (*extendedEvalContext)(arg.Ctx.MagicExtent)
	if extent == nil {
		ret := tree.ResultData{
			Result: nil,
			Err:    errors.Errorf("Exec Udr Function happen unexpected error!"),
		}
		arg.Ret <- ret
		return
	}
	cfg := extent.ExecCfg
	var FuncDesc sqlbase.FunctionDescriptor
	if err := protoutil.Unmarshal(arg.FuncDescBytes, &FuncDesc); err != nil {
		ret := tree.ResultData{
			Result: nil,
			Err:    err,
		}
		arg.Ret <- ret
		return
	}
	if FuncDesc.IsProcedure {
		ret := tree.ResultData{
			Result: nil,
			Err:    errors.Errorf("%s is a procedure, to call a procedure, use CALL", FuncDesc.FullFuncName),
		}
		arg.Ret <- ret
		return
	}
	unsafePointerDesc := UnsafePointer2Int64(unsafe.Pointer(&FuncDesc))
	UnsafePointerMap.Store(unsafePointerDesc, &FuncDesc)
	defer func() {
		UnsafePointerMap.Store(unsafePointerDesc, nil)
	}()
	r := MakeUDRRunParam(arg.Ctx.Context, arg.Ctx.Txn, cfg, extent, &FuncDesc)
	r.SelectCount = arg.Ctx.SelectCount
	r.EvalCtx.Params = make([]tree.UDRVar, 0)
	i := 0
	for j := 0; j < len(FuncDesc.Args) && i < len(arg.Args); j++ {
		tempArg := &tree.UDRVar{}
		// for in parameters, should compute
		if strings.ToLower(FuncDesc.Args[j].InOutMode) == "in" || strings.ToLower(FuncDesc.Args[j].InOutMode) == "inout" {
			typeExp, err := arg.Args[i].TypeCheck(nil, arg.ArgTypes[i].Typ, false)
			datum, err := typeExp.Eval(arg.Ctx)
			if err != nil {
				ret := tree.ResultData{
					Result: nil,
					Err:    err,
				}
				arg.Ret <- ret
			}
			//UDR-TODO： 删除注释
			// limit check
			// colType, _ := sqlbase.DatumTypeToColumnType(arg.ArgTypes[i].Typ)
			colType := FuncDesc.Args[j].Type
			outDatum, err := sqlbase.LimitValueWidth(colType, datum, &arg.ArgTypes[i].Name, true)
			if err != nil {
				ret := tree.ResultData{
					Result: nil,
					Err:    err,
				}
				arg.Ret <- ret
				return
			}
			i++
			tempArg.VarDatum = outDatum
		}
		tempArg.VarOid = FuncDesc.Args[j].Oid
		tempArg.VarName = FuncDesc.Args[j].Name

		typStr := FuncDesc.Args[j].ColumnTypeString
		fakeDeclareStmt := fmt.Sprintf("declare fakeVar %s", typStr)
		declareStmt, decErr := parser.ParseOne(fakeDeclareStmt, false)
		if decErr != nil {
			ret := tree.ResultData{
				Result: nil,
				Err:    decErr,
			}
			arg.Ret <- ret
			return
		}
		varColType := declareStmt.AST.(*tree.DeclareVariable).VariableType
		tempArg.ColType = varColType

		r.Params = append(r.Params, *tempArg)
	}
	switch FuncDesc.Language {
	case "plsql":
		fallthrough
	case "plpython", "plclang":
		pl := NewPLSQL(unsafe.Pointer(&r), r.MagicCode)
		if err := pl.CallPlpgSQLHandler(arg.Ctx.Context, CallFunction, unsafe.Pointer(&FuncDesc)); err != nil {
			ret := tree.ResultData{
				Result: nil,
				Err:    err,
			}
			arg.Ret <- ret
			return
		}
	case "plgolang":
		defer func() {
			if p := recover(); p != nil {
				ret := tree.ResultData{
					Result: nil,
					Err:    fmt.Errorf("%s", p),
				}
				arg.Ret <- ret
				return
			}
		}()
		scriptPath := FuncDesc.FilePath
		funcName := FuncDesc.Name
		plug, err := plugin.Open(scriptPath)
		if err != nil {
			ret := tree.ResultData{
				Result: nil,
				Err:    err,
			}
			arg.Ret <- ret
			return
		}
		foundFunction, err := plug.Lookup(funcName)
		if err != nil {
			ret := tree.ResultData{
				Result: nil,
				Err:    err,
			}
			arg.Ret <- ret
			return
		}
		FuncImpl := reflect.ValueOf(foundFunction)
		argsNum := FuncDesc.ArgNum
		argsDatum := make([]reflect.Value, 0)
		argsDatum = append(argsDatum, reflect.ValueOf(&r))
		for i := 0; i < int(argsNum); i++ {
			datum := r.Params[i].VarDatum
			value := DatumToReflectValue(datum)
			argsDatum = append(argsDatum, value)
		}
		resultValues := FuncImpl.Call(argsDatum)
		r.Return, err = ReflectValueToDatum(resultValues[0], FuncDesc.RetOid)
		if err != nil {
			ret := tree.ResultData{
				Result: nil,
				Err:    err,
			}
			arg.Ret <- ret
			return
		}
	default:
		ret := tree.ResultData{
			Result: nil,
			Err:    errors.New("no supports for this language"),
		}
		arg.Ret <- ret
		return

	}

	// deal with OUT parameters
	tupleTypes := make([]types.T, 0)
	tupleColNames := make([]string, 0)
	args := make([]sqlbase.FuncArgDescriptor, 0)
	for _, arg := range FuncDesc.Args {
		if arg.InOutMode == "out" || arg.InOutMode == "inout" {
			tupleTypes = append(tupleTypes, arg.Type.ToDatumType())
			tupleColNames = append(tupleColNames, arg.Name)
			args = append(args, arg)
		}
	}
	retTuple := tree.NewDTupleWithLen(types.TTuple{Types: tupleTypes, Labels: tupleColNames}, len(args))
	// has OUT parameters
	if len(args) > 0 {
		startIdx := 0
		if len(args) > 1 {
			for _, outArg := range args {
				// 不要试图用名字找，用序号，此处能保证序号不会超过下标，所以不再进行长度的判断
				// 临时变量避免可能的for range问题
				tempArg := outArg
				if r.Params[tempArg.ID-1].IsNull || r.Params[tempArg.ID-1].VarDatum == nil {
					retTuple.D[startIdx] = tree.DNull
				} else {
					retTuple.D[startIdx] = r.Params[tempArg.ID-1].VarDatum
				}
				startIdx++
			}
			// for _, para := range r.Params {
			// 	if para.VarName == tupleColNames[startIdx] {
			// 		tempPara := para
			// 		retTuple.D[startIdx] = tempPara.VarDatum
			// 		if startIdx >= len(tupleColNames) {
			// 			break
			// 		}
			// 	}
			// }
			arg.Ret <- tree.ResultData{
				Result: retTuple,
				Err:    nil,
			}
		} else {
			var singleOut tree.Datum
			if r.Params[args[0].ID-1].IsNull || r.Params[args[0].ID-1].VarDatum == nil {
				singleOut = tree.DNull
			} else {
				singleOut = r.Params[args[0].ID-1].VarDatum
			}
			arg.Ret <- tree.ResultData{
				Result: singleOut,
				Err:    nil,
			}
		}
	} else { // without OUT parameters
		var err error
		var stmt parser.Statement
		destTyp, _ := types.OidToType[FuncDesc.RetOid]
		declareStmt := fmt.Sprintf("declare %s %s", "tempVar", FuncDesc.ReturnType)
		stmt, err = parser.ParseOne(declareStmt, false)
		castTyp := stmt.AST.(*tree.DeclareVariable).VariableType
		if destTyp != types.Void {
			//castTyp, _ := coltypes.DatumTypeToColumnType(destTyp)
			r.Return, err = perforCastWithoutNull(&r.EvalCtx.EvalContext, r.Return, castTyp)
			if err == nil {
				var typ sqlbase.ColumnType
				typ, err = StrToColtype(FuncDesc.ReturnType)
				if err == nil {
					returnStr := "Return"
					r.Return, err = sqlbase.LimitValueWidth(typ, r.Return, &returnStr, true)
				}
			}
		} else {
			r.Return = tree.NewDString("")
			// r.Return = tree.DNull
		}
		ret := tree.ResultData{
			Result: r.Return,
			Err:    err,
		}
		arg.Ret <- ret
	}
}

// MakeUDRRunParam is to make UDR Run Param
func MakeUDRRunParam(
	ctx context.Context,
	ptxn *client.Txn,
	exeCfg *ExecutorConfig,
	evalCtx *extendedEvalContext,
	funcDesc *FunctionDescriptor,
) UDRRunParam {
	rp := UDRRunParam{
		Ctx:            ctx,
		ExecCfg:        exeCfg,
		EvalCtx:        evalCtx,
		Params:         make([]tree.UDRVar, 0),
		Result:         make([]tree.Datum, 0),
		CursorFetchRow: make([]tree.Datum, 0),
		Return:         tree.DNull,
		FuncDesc:       funcDesc,
	}
	rp.MagicCode = randomMinToMax(MagicCodeMin, MagicCodeMax)
	// init CachedPlan
	// UDR-TODO: 增加一个并发控制保护
	rp.CachedPlan = make(map[uint32]UDRPlanner, 0)
	return rp
}

func randomMinToMax(a, b int) int {
	s1 := rand.NewSource(timeutil.Now().UnixNano())
	r1 := rand.New(s1)
	if b < a {
		a, b = b, a
	}
	len := int(math.Ceil(math.Log2(float64(b) - float64(a)))) //二进制切片的长度
	arr := make([]int, len)
	for {
		resultNum := 0 //十进制的数字
		for i := 0; i < len; i++ {
			arr[i] = r1.Intn(2)
			resultNum += arr[i] * int(math.Pow(2, float64(i)))
		}
		if resultNum >= 0 && resultNum <= b-a {
			return (a + resultNum)
		}
	}
}

//ExecStmt exe a sqlstmt
// UDR-TODO：所有plgolang的接口需要替换内部执行器
func (r *UDRRunParam) ExecStmt(stmt string) (int, error) {
	// UDR-TODO：替换内部执行器
	res, err := r.EvalCtx.InternalExecutor.(*SessionBoundInternalExecutor).Query(r.Ctx, "query Internal", r.EvalCtx.Txn, stmt)
	return len(res), err
}

//ExecReturnRows exe a sqlstmt and returnrows
func (r *UDRRunParam) ExecReturnRows(stmt string) ([]bepi.RowDatums, error) {
	// UDR-TODO：替换内部执行器
	rows, cols, err := r.EvalCtx.InternalExecutor.(*SessionBoundInternalExecutor).QueryWithCols(r.Ctx, "query Internal", r.EvalCtx.Txn, stmt)
	if err != nil {
		return nil, err
	}
	resr := make([]bepi.RowDatums, 0)
	for _, row := range rows {
		rd := make([]bepi.RowDatum, 0)
		for i, v := range row {
			// 将tree.Datum类型的数据转换为自定义的bepi.Val类型,并和该列名称、类型一起存储
			rd = append(rd, bepi.RowDatum{Name: cols[i].Name, Typ: cols[i].Typ, Value: TypeConvBepi(v)})
		}
		resr = append(resr, rd)
	}
	return resr, err
}

//PrePare a stmt
func (r *UDRRunParam) PrePare(name string, stmt string, argsType ...string) (string, error) {
	rstmt := "prepare " + name + "("
	for k, v := range argsType {
		if k != 0 {
			rstmt += ","
		}
		rstmt += v
	}
	rstmt = rstmt + ")" + " as " + stmt
	// UDR-TODO：替换内部执行器
	_, err := r.EvalCtx.InternalExecutor.(*SessionBoundInternalExecutor).Query(r.Ctx, "query Internal", r.EvalCtx.Txn, rstmt)
	if err != nil {
		return "fail to prepare", err
	}
	return name, err
}

//ExecPrepareWithName UDR-TODO：delete？
//ExecPrepareWithName to prepare and execute a stmt
func (r *UDRRunParam) ExecPrepareWithName(name string, argsVal ...string) error {
	rstmt := "execute " + name + "("
	for k, v := range argsVal {
		if k != 0 {
			rstmt += ","
		}
		rstmt += v
	}
	rstmt += ")"
	// UDR-TODO：替换内部执行器
	_, err := r.EvalCtx.InternalExecutor.(*SessionBoundInternalExecutor).Query(r.Ctx, "query Internal", r.EvalCtx.Txn, rstmt)
	if err != nil {
		_, err = r.ExecCfg.InternalExecutor.Query(r.Ctx, "query Internal", r.EvalCtx.Txn, rstmt)
	}
	return err
}

//BepiPreparePlanGo use to prepare a plan
func (r *UDRRunParam) BepiPreparePlanGo(queryString string, planIndex *uint) error {
	plan, _ := NewInternalPlanner("bepi-prepare",
		r.EvalCtx.Txn,
		"root", // TODO: pass user
		&MemoryMetrics{},
		r.ExecCfg,
	)
	p := plan.(*planner)
	// set sesssion mutator
	p.EvalContext().SessionData = r.EvalCtx.SessionMutator.data
	p.extendedEvalCtx = *r.EvalCtx

	stmt, err := parser.ParseOne(queryString, r.EvalCtx.GetCaseSensitive())
	if err != nil {
		return err
	}
	p.stmt = &Statement{Statement: stmt}
	p.optPlanningCtx.init(p)
	opc := &p.optPlanningCtx
	opc.reset()
	opc.InUDR = true
	defer func() {
		opc.InUDR = false
	}()

	/* Check to see if it's a simple expression */
	isSimple := p.checkSimpleExpr()
	if isSimple { //  && strings.Compare(strings.ToLower(queryString), "select not found") != 0
		f := opc.optimizer.Factory()
		bld := optbuilder.New(r.Ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, stmt.AST)
		bld.SetInUDR(true)
		bld.SetUDRParam(r.Params)
		defer func() {
			bld.SetInUDR(false)
			bld.SetUDRParam(nil)
		}()
		builtScalarExpr, builtExpr := bld.BuildStmtAndReturnScalarExpr(stmt.AST)
		hashKey := util.CRC32Origin([]byte(queryString))
		*planIndex = uint(hashKey)
		scalarBuilder := execbuilder.Builder{}
		var scalarRet tree.Datum
		scalarResult, err := scalarBuilder.BuildScalarExprToTypedExpr(nil, builtScalarExpr)
		if err == nil {
			//UDR-TODO：err return
			scalarRet, _ = scalarResult.Eval(p.EvalContext())
		}
		r.CachedPlan[hashKey] = UDRPlanner{
			Planner:  p,
			Expr:     builtExpr,
			Result:   scalarRet,
			IsSimple: isSimple}
		return err
	}
	//build Memo and store
	// present is update/delete current of cursor statement
	isUpCOC := false
	isDelCOC := false
	var execMemo *memo.Memo
	if updateStmt, ok := stmt.AST.(*tree.Update); ok {
		if updateStmt.Where != nil {
			if _, ok := updateStmt.Where.Expr.(*tree.CurrentOfExpr); ok {
				isUpCOC = true
			}
		}
	} else if deleteStmt, ok := stmt.AST.(*tree.Delete); ok {
		if deleteStmt.Where != nil {
			if _, ok := deleteStmt.Where.Expr.(*tree.CurrentOfExpr); ok {
				isDelCOC = true
			}
		}
	}
	if err := checkOptSupportForTopStatement(stmt.AST); err == nil && (!isUpCOC && !isDelCOC) {
		opc.catalog.planner.semaCtx.UDRParms = r.Params
		execMemo, err = opc.buildExecMemo(r.Ctx)
		if err != nil {
			return err
		}
	} else {
		err = p.makePlan(r.Ctx)
		if err != nil {
			return err
		}
	}
	// 根据表达式计算hash 值/
	hashKey := util.CRC32Origin([]byte(queryString))
	*planIndex = uint(hashKey)
	r.CachedPlan[hashKey] = UDRPlanner{Planner: p, Memo: execMemo, IsSimple: isSimple}
	return err
}

// BepiGetGoroutineID use to Get Goroutine ID
//export BepiGetGoroutineID
func BepiGetGoroutineID() uint64 {
	b := make([]byte, 64)
	runtime.Stack(b, false)
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	//UDR-TODO：err return
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

// BepiExecPlanGo use to execute a plan
func (r *UDRRunParam) BepiExecPlanGo(planIndex uint) error {
	//UDR-TODO:全局变量需要放到结构体内
	UDRExecRes.Res = UDRExecRes.Res[:0]
	UDRExecRes.RowCount = 0
	_, err := ExecInternalGo(*r, planIndex)
	if err != nil {
		return err
	}
	return err
}

// ExecInternalGo use to internal
func ExecInternalGo(udrRunParams UDRRunParam, planIndex uint) (ret tree.Datums, err error) {
	defer func() {
		if r := recover(); r != nil {
			rmsg := fmt.Sprintln(r)
			err = errors.New(rmsg)
		}
	}()
	if err != nil {
		return nil, err
	}
	udrPlanner, ok := udrRunParams.CachedPlan[uint32(planIndex)]
	evalCtx := udrRunParams.EvalCtx
	execCfg := udrRunParams.ExecCfg
	if ok {
		if udrPlanner.IsSimple {
			if udrPlanner.Result != nil {
				udrRunParams.Result = []tree.Datum{udrPlanner.Result}
				return udrRunParams.Result, nil
			}
			err := udrRunParams.BepiExecEvalExprAndStoreInResultAPIGo(planIndex)
			if err == nil {
				return udrRunParams.Result, nil
			}

		}
		// found a logic plan, execute it
		planner := udrPlanner.Planner
		planner.EvalContext().Params = udrRunParams.Params
		opc := &planner.optPlanningCtx
		if udrPlanner.Memo != nil {
			err := opc.runExecBuilder(
				&planner.curPlan,
				planner.stmt,
				newExecFactory(planner),
				udrPlanner.Memo,
				planner.EvalContext(),
				planner.autoCommit,
			)
			// err := buildCurPlanNode(planner, udrPlanner.Memo)
			if err != nil {
				return nil, err
			}
		}
		ctx := udrRunParams.Ctx
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			return nil
		})
		colNames := make([]string, 0)
		renderNode, isRender := planner.curPlan.plan.(*renderNode)
		scanNode, isScanNode := planner.curPlan.plan.(*scanNode)
		if isRender || isScanNode {
			if isScanNode {
				for i := range scanNode.cols {
					colNames = append(colNames, scanNode.cols[i].Name)
				}
			} else {
				for i := range renderNode.columns {
					colNames = append(colNames, renderNode.columns[i].Name)
				}

			}
			UDRExecRes.Names = colNames
		}
		recv := MakeDistSQLReceiver(
			ctx,
			rw,
			planner.stmt.AST.StatementType(),
			execCfg.RangeDescriptorCache,
			execCfg.LeaseHolderCache,
			planner.txn, //evalCtx.Txn,
			func(ts hlc.Timestamp) {
				_ = execCfg.Clock.Update(ts)
			},
			planner.ExtendedEvalContext().Tracing,
		)
		planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, nil /* txn */)
		planCtx.isLocal = true

		planCtx.planner = planner
		evalCtx.Planner = planner
		planCtx.stmtType = recv.stmtType
		if len(planner.curPlan.subqueryPlans) != 0 {
			var evalCtx2 = udrRunParams.EvalCtx.copy()
			// using evalCtx2'd deep copy for subquery in case of corrupt main query contex
			evalCtx2.Planner = planner
			evalCtxFactory := func() *extendedEvalContext {
				evalCtx2.TxnModesSetter.(*connExecutor).resetEvalCtx(evalCtx2, planner.txn, planner.ExtendedEvalContext().StmtTimestamp)
				evalCtx2.Placeholders = &planner.semaCtx.Placeholders
				return evalCtx2
			}
			if !udrRunParams.ExecCfg.DistSQLPlanner.PlanAndRunSubqueries(
				ctx, planner, evalCtxFactory, planner.curPlan.subqueryPlans, recv, true,
			) {
				return nil, errors.New("can't exec plan and run sub queries")
			}
		}
		if planner.curPlan.plan == nil {
			planner.curPlan.plan = &valuesNode{}
		}
		ExecInUDR = true
		planner.semaCtx.SetInUDR(true)
		execCfg.DistSQLPlanner.PlanAndRun(
			ctx, evalCtx, planCtx, evalCtx.Txn, planner.curPlan.main, recv)
		udrRunParams.EvalCtx = planner.ExtendedEvalContext().copy()
		if recv.row != nil {
			udrRunParams.Result = recv.row
		}
		err := rw.Err()
		if err != nil {
			return nil, err
		}
	} else {
		err = errors.New("Can not found a cached plan to execute this plan")
		return nil, err
	}

	result := make([]tree.Datum, 0)
	//UDR-TODO: reuse udrRunParams.result
	for _, v := range udrRunParams.Result {
		result = append(result, v)
	}
	return result, nil
}

//TypeConvBepi use to judge conv type
func TypeConvBepi(datum tree.Datum) bepi.Val {
	switch typ := datum.(type) {
	case *tree.DInt:
		return bepi.PInt(int64(*typ))
	case *tree.DFloat:
		return bepi.PFloat(float64(*typ))
	default:
		return bepi.PString(typ.String())
	}
}

//ReportErrorToPlsql a function use to report error
func ReportErrorToPlsql(err interface{}) *C.char {
	var errMsg string
	if pgErr, ok := err.(*pgerror.Error); ok {
		// if an pgerror, then parse code and message to errMsg
		errMsg = pgErr.Code.String() + ":" + pgErr.Error()
	} else if msg, ok := err.(string); ok {
		if len(msg) != 0 {
			errMsg = "XXOOO:" + msg
		} else {
			errMsg = ""
		}
	} else if msg, ok := err.(error); ok {
		if len(msg.Error()) != 0 {
			errMsg = "XXOOO:" + msg.Error()
		} else {
			errMsg = ""
		}
	} else {
		errMsg = ""
	}
	return C.CString(errMsg)
}

//GetOidByName use to get oid by name
func GetOidByName(typName string) oid.Oid {
	var id oid.Oid
	if strings.HasPrefix(typName, "varbit(") {
		return oid.T__varbit
	} else if strings.HasPrefix(typName, "bit(") {
		return oid.T__bit
	} else if strings.HasPrefix(typName, "character(") || strings.HasPrefix(typName, "char(") {
		return oid.T_char
	} else if strings.HasPrefix(typName, "character varying(") || strings.HasPrefix(typName, "char varying(") || strings.HasPrefix(typName, "varchar(") {
		return oid.T_varchar
	} else if strings.HasPrefix(typName, "string(") {
		return oid.T_text
	}
	//UDR-TODO:考虑make一个镜像map
	for k, v := range types.OidToType {
		if v.String() == typName {
			id = k
			return id
		}

		if len(types.ExtraOidNames[k]) > 0 {
			for _, customName := range types.ExtraOidNames[k] {
				if customName == typName {
					id = k
					return id
				}
			}
		}
	}
	return id
}

// ConstructUDRRunParamByHandler ,That rule must be preserved during C execution, in that the program must not store any Go pointers into that memory.
// 因为GOLANG 对CGO的支持存在一些问题，当传入参数为指针该指针对象包含了GO 中申请的其他指针时候，会崩溃。参考以下
// 因此采用地址传递，将地址传递给plsql，再由plsql 回传回来，并做指针内容校验，校验成功则使用。
// https://github.com/golang/go/issues/12416
// Go code may pass a Go pointer to C provided that the Go memory to which it points does not contain any Go pointers.
func ConstructUDRRunParamByHandler(handler C.Handler_t) (*UDRRunParam, *engine.RocksDB, error) {
	handlerUint64 := uint64(handler.pointer)
	UDRRunParamPointer, ok := UnsafePointerMap.Load(handlerUint64)
	if !ok {
		return nil, nil, pgerror.NewError(pgcode.NullValueNotAllowed, "UDR Runtime Params does not exist, can not continue to execute")
	}
	params, ok := UDRRunParamPointer.(*UDRRunParam)
	if !ok || params.MagicCode != int(handler.magicCode) {
		return nil, nil, pgerror.NewError(pgcode.NullValueNotAllowed, "UDR Runtime Params does not exist, can not continue to execute")
	}
	return params, nil, nil
}

// SearchFunctionMetaCache use to serach fuction meatacache
//这个函数用于查找内存缓冲中的一系列对象，第一个参数表示找的类型id（typeid,procid,realoid）,第二个是缓冲池中对象的id
//export SearchFunctionMetaCache
func SearchFunctionMetaCache(handler C.Handler_t, catchID C.int) C.FormData_pg_proc {
	switch catchID {
	case ProcedureType:
		formDataPgProc, err := findFuncDescAndTransferFormDataPgProc(handler)
		if err != nil {
			return C.FormData_pg_proc{}
		}
		return formDataPgProc
	default:
		return C.FormData_pg_proc{}
	}
}

//找到funcDesc 并且适配到FormData_pg_proc中
func findFuncDescAndTransferFormDataPgProc(handler C.Handler_t) (C.FormData_pg_proc, error) {
	a := uint64(handler.Pointer_func_desc)
	funcDescPointer, ok := UnsafePointerMap.Load(a)
	if !ok {
		var cFromDataPgProc C.FormData_pg_proc
		return cFromDataPgProc, errors.New("can't find func desc")
	}
	funcDesc, ok := funcDescPointer.(*sqlbase.FunctionDescriptor)
	if !ok {
		var cFromDataPgProc C.FormData_pg_proc
		return cFromDataPgProc, errors.New("can't find func desc")
	}
	return funcDesc2FormDataPGProc(*funcDesc), nil
}

//转换过程
func funcDesc2FormDataPGProc(funcDesc sqlbase.FunctionDescriptor) C.FormData_pg_proc {
	var cFromDataPgProc C.FormData_pg_proc
	var cNameData C.NameData
	for i, v := range funcDesc.Name {
		cNameData.data[i] = C.char(v)
	}

	// get arg types, names and modes
	argTypes := [255]C.Oid{}
	argNames := C.makeCharArray(C.int(funcDesc.ArgNum))
	// arg type names
	argTypeNames := C.makeCharArray(C.int(funcDesc.ArgNum))
	argModes := ""
	for i, v := range funcDesc.Args {
		argTypes[i] = C.Oid(v.Oid)
		C.setArrayString(argTypeNames, C.CString(v.ColumnTypeString), C.int(i))
		C.setArrayString(argNames, C.CString(v.Name), C.int(i))
		switch v.InOutMode {
		case "in":
			argModes += "i"
		case "out":
			argModes += "o"
		case "inout":
			argModes += "b"
		case "table":
			argModes += "t"
		case "variadic":
			argModes += "v"
		}
	}
	cFromDataPgProc.argnames = argNames
	cFromDataPgProc.argtypes = argTypes
	cFromDataPgProc.argTypeNames = argTypeNames
	cFromDataPgProc.argModes = C.CString(argModes)
	cFromDataPgProc.proname = cNameData
	cFromDataPgProc.pronargs = C.uint(funcDesc.ArgNum)
	cFromDataPgProc.prorettype = C.uint(funcDesc.RetOid)
	cFromDataPgProc.proretset = false
	cFromDataPgProc.proc_source = C.CString(funcDesc.FuncDef)

	cFromDataPgProc.file_path = C.CString(filepath.Dir(funcDesc.FilePath))
	cFromDataPgProc.file_name = C.CString(strings.Split(funcDesc.FileName, ".")[0])
	cFromDataPgProc.file_name_full = C.CString(funcDesc.FileName)
	cFromDataPgProc.language = C.CString(funcDesc.Language)
	if funcDesc.IsProcedure {
		cFromDataPgProc.prokind = C.char('p')
	} else {
		cFromDataPgProc.prokind = C.char('f')
	}
	cFromDataPgProc.provolatile = C.char('v')
	return cFromDataPgProc
}

// SearchTypeMetaCache use to serach type metacache
//export SearchTypeMetaCache
func SearchTypeMetaCache(handler C.Handler_t, id C.Oid) C.TYPEDESC {
	var nID = oid.Oid(id)
	var dataType C.FormData_pg_type
	typeT, ok := types.OidToType[nID]
	if ok {
		dataType.oid = C.Oid(typeT.Oid())
		dataType.typlen = C.int16(typeT.Len())
		dataType.typtype = C.char(typeT.Type())
		dataType.typbyval = C.bool(func(len int16) bool {
			if len == 1 || len == 2 || len == 4 || len == 8 {
				return true
			}
			return false
		}(typeT.Len()))
		dataType.typmod = C.int32(-1)
		dataType.typisdefined = C.bool(true)
		dataType.typrelid = C.Oid(0)
		dataType.typbasetype = C.Oid(0)
		dataType.typcollation = C.Oid(0)
		dataType.typelem = C.Oid(typeT.Lem())
		dataType.typstorage = C.char('p')
	}
	return dataType
}

// SearchTypeOidByName use to serach type oid by name
//export SearchTypeOidByName
func SearchTypeOidByName(handler C.Handler_t, name *C.char) C.Oid {
	nameStr := C.GoString(name)
	nameStr = strings.ToLower(nameStr)
	nameStr = strings.Trim(nameStr, " ")
	var nID C.Oid
	nID = C.Oid(0)
	// nID = C.Oid(GetOidByName(nameStr))

	declareStmt := fmt.Sprintf("declare %s %s", "tempVar", nameStr)
	stmt, err := parser.ParseOne(declareStmt, false)
	if err != nil {
		return C.Oid(0)
	}
	if stmt.AST != nil {
		colTyp := stmt.AST.(*tree.DeclareVariable).VariableType
		colTypName := colTyp.TypeName()
		nID = C.Oid(tree.TypeNameGetOID[colTypName])
	}
	return nID
}

var cursorFetchAssignIndex int

// BepiResetCursorAssign use to Reset cursor assign
//export BepiResetCursorAssign
func BepiResetCursorAssign() {
	cursorFetchAssignIndex = 0
}

// GetNthParamListLength use to Get NthParamList Length
//export GetNthParamListLength
func GetNthParamListLength(handler C.Handler_t, n C.int) C.int {

	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return C.int(-1)
		//return ReportErrorToPlsql(err)
	}
	if len(udrRunParams.Params) < int(n) {
		return C.int(-1)
	}
	if udrRunParams.Params[n].VarDatum == tree.DNull {
		return C.int(-1)
	}
	array := tree.MustBeDArray(udrRunParams.Params[n].VarDatum)
	length := len(array.Array)
	return C.int(length)
}

// GetNthParamListI use to Get NthParamList I
//export GetNthParamListI
func GetNthParamListI(handler C.Handler_t, n C.int, i C.int, isNull *C.bool) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	if len(udrRunParams.Params) < int(n) {
		return ReportErrorToPlsql(err)
	}
	array := tree.MustBeDArray(udrRunParams.Params[n].VarDatum)
	if array.Array[i] == tree.DNull {
		*isNull = C.bool(true)
		return C.CString("")
	}
	*isNull = C.bool(false)
	paramStr := array.Array[i].String()
	//paramStr := udrRunParams.Params[n].VarDatum.String()
	switch udrRunParams.Params[n].VarOid {
	case oid.T__varchar, oid.T__text:
		paramStr = string(tree.MustBeDString(tree.MustBeDArray(udrRunParams.Params[n].VarDatum).Array[i]))
	case oid.T__int8, oid.T__int4, oid.T__int2:

	}

	return C.CString(paramStr)
}

// GetNthParam use to Get NthParam
//export GetNthParam
func GetNthParam(handler C.Handler_t, n C.int, isNull *C.bool) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	if len(udrRunParams.Params) < int(n) {
		return ReportErrorToPlsql(err)
	}
	if udrRunParams.Params[n].VarDatum == tree.DNull {
		*isNull = C.bool(true)
		return C.CString("")
	}
	*isNull = C.bool(false)
	paramStr := udrRunParams.Params[n].VarDatum.String()
	switch udrRunParams.Params[n].VarOid {
	case oid.T_varchar, oid.T_text:
		paramStr = string(tree.MustBeDString(udrRunParams.Params[n].VarDatum))
	case oid.T_bit, oid.T_varbit:
		if paramStr[0] == 'B' {
			paramStr = paramStr[2 : len(paramStr)-1]
		}
	case oid.T_numeric:
		return C.CString(paramStr)
	default:
		if len(paramStr) >= 2 && paramStr[0] == '\'' && paramStr[len(paramStr)-1] == '\'' {
			paramStr = paramStr[1 : len(paramStr)-1]
		}
	}

	return C.CString(paramStr)

}

//GetNthBytesParam use to Get NthParam if the type of param is bytea/bytes
//export GetNthBytesParam
func GetNthBytesParam(handler C.Handler_t, n C.int, length *C.int, isNull *C.bool) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	if len(udrRunParams.Params) < int(n) {
		return ReportErrorToPlsql(err)
	}
	if udrRunParams.Params[n].VarDatum == tree.DNull {
		*isNull = C.bool(true)
		return C.CString("")
	}
	*isNull = C.bool(false)
	paramStr := udrRunParams.Params[n].VarDatum.String()
	switch udrRunParams.Params[n].VarOid {
	case oid.T_varchar, oid.T_text:
		paramStr = string(tree.MustBeDString(udrRunParams.Params[n].VarDatum))
	case oid.T_bytea:
		paramStr = string(tree.MustBeDBytes(udrRunParams.Params[n].VarDatum))
	case oid.T_bit, oid.T_varbit:
		if paramStr[0] == 'B' {
			paramStr = paramStr[2 : len(paramStr)-1]
		}
	default:
		if len(paramStr) >= 2 && paramStr[0] == '\'' && paramStr[len(paramStr)-1] == '\'' {
			paramStr = paramStr[1 : len(paramStr)-1]
		}
	}
	*length = C.int(len(paramStr))
	return C.CString(paramStr)

}

// AssignTheValue use to Assign TheValue
//export AssignTheValue
func AssignTheValue(
	handler C.Handler_t,
	paramTarget C.int,
	valTarget C.int,
	inFetch C.bool,
	inForEach C.bool,
	index C.int,
) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	// defer func() { udrRunParams.CursorColNum = 0 }()
	PTarget := (int)(paramTarget)
	VTarget := (int)(valTarget)
	infetch := bool(inFetch)
	inForeach := bool(inForEach)
	goIdx := int(index)
	var newDatum tree.Datum

	var castTyp coltypes.T
	if udrRunParams.Params[PTarget].ColType != nil {
		castTyp = udrRunParams.Params[PTarget].ColType
	} else {
		OID := udrRunParams.Params[PTarget].VarOid
		destTyp, ok := types.OidToType[OID]
		if !ok {
			return ReportErrorToPlsql("udr params the type is unkone")
		}
		castTyp, err = coltypes.DatumTypeToColumnType(destTyp)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
	}

	// record type
	if udrRunParams.Params[PTarget].VarOid == oid.T_record {
		newDatum = evalRecord(udrRunParams, infetch)
		err := checkNotNull(udrRunParams.Params[PTarget], newDatum)
		if err != nil {
			return C.CString(err.Error())
		}
		udrRunParams.Params[PTarget].VarDatum = newDatum
		udrRunParams.Params[PTarget].IsNull = false
		return C.CString("")
	}

	// when not in fetch
	if !infetch {
		if PTarget >= len(udrRunParams.Params) {
			return ReportErrorToPlsql("params out of index")
		}
		if VTarget >= len(udrRunParams.Result) {
			if len(udrRunParams.Result) == 1 {
				if Array, ok := udrRunParams.Result[0].(*tree.DArray); ok && !inForeach {
					udrRunParams.Result = Array.Array
				}
			} else {
				if len(udrRunParams.Result) != 0 {
					return ReportErrorToPlsql("params out of index")
				}
				err := checkNotNull(udrRunParams.Params[PTarget], tree.DNull)
				if err != nil {
					return C.CString(err.Error())
				}
				udrRunParams.Params[PTarget].VarDatum = tree.DNull
				udrRunParams.Params[PTarget].IsNull = true
				return ReportErrorToPlsql("")
			}
		}

		if inForeach {
			if arr, ok := udrRunParams.Result[VTarget].(*tree.DArray); ok {
				// can ensure goIdx is less than len(arr.Array), so don't need check idx here
				newDatum = arr.Array[goIdx]
			} else {
				return ReportErrorToPlsql("FOREACH ... SLICE loop variable must be of an array type")
			}
		} else {
			newDatum = udrRunParams.Result[VTarget]
		}
		tempDatum := newDatum
		newDatum, err = perforCastWithoutNull(&udrRunParams.EvalCtx.EvalContext, newDatum, castTyp)
		if err != nil {
			return ReportErrorToPlsql(fmt.Sprintf("cannot cast %s to type %s\nDETAIL: %s", tempDatum.String(), castTyp.String(), err.Error()))
		}

		// value width limit check
		err = GetColumnTypAndCheckLimit(udrRunParams.Params, PTarget, newDatum)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		err := checkNotNull(udrRunParams.Params[PTarget], newDatum)
		if err != nil {
			return C.CString(err.Error())
		}
		if _, ok := udrRunParams.Result[0].(*tree.DBitArray); ok {
			udrRunParams.Params[PTarget].VarDatum = tempDatum
		} else {
			udrRunParams.Params[PTarget].VarDatum = newDatum
		}
		udrRunParams.Params[PTarget].IsNull = false
	} else {
		if cursorFetchAssignIndex < udrRunParams.CursorColNum && len(udrRunParams.CursorFetchRow) != 0 {
			newDatum := udrRunParams.CursorFetchRow[cursorFetchAssignIndex]
			newDatum, err = perforCastWithoutNull(&udrRunParams.EvalCtx.EvalContext, newDatum, castTyp)
			if err != nil {
				return ReportErrorToPlsql(fmt.Sprintf("cannot cast %s to type %s\nDETAIL: %s", udrRunParams.CursorFetchRow[cursorFetchAssignIndex].String(), castTyp.String(), err.Error()))
			}
			// value width limit check
			err = GetColumnTypAndCheckLimit(udrRunParams.Params, PTarget, newDatum)
			if err != nil {
				return ReportErrorToPlsql(err)
			}

			err := checkNotNull(udrRunParams.Params[PTarget], newDatum)
			if err != nil {
				return C.CString(err.Error())
			}
			udrRunParams.Params[PTarget].VarDatum = newDatum
			udrRunParams.Params[PTarget].IsNull = false
			cursorFetchAssignIndex++
		} else {
			err := checkNotNull(udrRunParams.Params[PTarget], tree.DNull)
			if err != nil {
				return C.CString(err.Error())
			}
			udrRunParams.Params[PTarget].VarDatum = tree.DNull
			udrRunParams.Params[PTarget].IsNull = true
		}
	}
	return ReportErrorToPlsql("")
}

func evalRecord(udrRunParams *UDRRunParam, inFetch bool) tree.Datum {
	tupleTypes := make([]types.T, 0)
	args := make([]tree.Datum, 0)
	isSet := false
	if !inFetch {
		if len(udrRunParams.Result) == 1 {
			if dTuple, ok := udrRunParams.Result[0].(*tree.DTuple); ok {
				return dTuple
			}
			if arr, ok := udrRunParams.Result[0].(*tree.DArray); ok {
				args = arr.Array
				for _, v := range arr.Array {
					tupleTypes = append(tupleTypes, v.ResolvedType())
					isSet = true
				}
			}
		}
		if !isSet {
			for _, v := range udrRunParams.Result {
				tupleTypes = append(tupleTypes, v.ResolvedType())
				args = append(args, v)
			}
		}
	} else {
		if len(udrRunParams.CursorFetchRow) == 1 {
			if dTuple, ok := udrRunParams.CursorFetchRow[0].(*tree.DTuple); ok {
				return dTuple
			}
			if arr, ok := udrRunParams.CursorFetchRow[0].(*tree.DArray); ok {
				args = arr.Array
				for _, v := range arr.Array {
					tupleTypes = append(tupleTypes, v.ResolvedType())
					isSet = true
				}
			}
		}
		if !isSet {
			for _, v := range udrRunParams.CursorFetchRow {
				tupleTypes = append(tupleTypes, v.ResolvedType())
				args = append(args, v)
			}
		}
	}
	retTuple := tree.NewDTupleWithLen(types.TTuple{Types: tupleTypes}, len(args))
	retTuple.D = args
	return retTuple
}

// BepiFetchIntoValue query a statement which format like"fetch direction from cursorName into var1,var2,var3"
//export BepiFetchIntoValue
func BepiFetchIntoValue(handler C.Handler_t, querystmt *C.char) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	queryStr := C.GoString(querystmt)
	ss := strings.Split(queryStr, " ")
	//UDR-TODO：逻辑有问题，var1 中间如果有空格 var1, var2，就会报错
	if len(ss) != 6 {
		return ReportErrorToPlsql("queryStr format error")
	}
	var dire tree.CursorDirection
	//UDR-TODO：default 报错
	switch ss[1] {
	case "NEXT":
		dire = tree.NEXT
	case "PRIOR":
		dire = tree.PRIOR
	case "FIRST":
		dire = tree.FIRST
	case "LAST":
		dire = tree.LAST
	case "ABSOLUTE":
		dire = tree.ABSOLUTE
	case "RELATIVE":
		dire = tree.RELATIVE
	case "FORWARD":
		dire = tree.FORWARD
	case "BACKWARD":
		dire = tree.BACKWARD
	}
	boo := udrRunParams.BepiCursorFetchGo(&ss[3], int(dire), 1)
	if !boo {
		return ReportErrorToPlsql("cursor fetch error")
	}
	// into 边的变量
	varstr := ss[5]
	// into变量 数组
	vardatum := strings.Split(varstr, ",")
	count := 0
	for i := 0; i < len(vardatum); i++ {
		// into变量数 > select的列数，后边的变量就不赋值了
		if i > len(udrRunParams.CursorFetchRow)-1 {
			break
		}
		for j := 0; j < len(udrRunParams.Params); j++ {
			if vardatum[i] == udrRunParams.Params[j].VarName {
				count = 0
				err = udrRunParams.BepiAssignTheValueGo(j, -1, true)
				if err != nil {
					return ReportErrorToPlsql(err)
				}
				break
			} else {
				count++
			}
			// 计数器 == udr定义的变量总个数时候，说明declare的变量没有与into后的变量同名
			if count == len(udrRunParams.Params) {
				return ReportErrorToPlsql(vardatum[i] + "cannot find")
			}
		}
	}
	return ReportErrorToPlsql("")
}

// BepiAssignTheValueGo use for plgolang to assign the value
// 参数:
//		paramTarget: into后的变量数组 下标
func (r *UDRRunParam) BepiAssignTheValueGo(paramTarget int, valTarget int, inFetch bool) error {
	var err error
	if !inFetch {
		OID := r.Params[paramTarget].VarOid
		if paramTarget >= len(r.Params) {
			return errors.New("params out of index")
		}
		if valTarget >= len(r.Result) {
			if len(r.Result) == 1 {
				if Array, ok := r.Result[0].(*tree.DArray); ok {
					r.Result = Array.Array
				}
			} else {
				return errors.New("params out of index")
			}
		}
		OID = r.Params[paramTarget].VarOid
		destTyp, ok := types.OidToType[OID]
		if !ok {
			return errors.New("udr params the type is unkone")
		}
		castTyp, err := coltypes.DatumTypeToColumnType(destTyp)
		if err != nil {
			return errors.New("")
		}
		newDatum, err := perforCastWithoutNull(&r.EvalCtx.EvalContext, r.Result[valTarget], castTyp)
		if err != nil {
			return errors.Errorf("cannot cast %s to type %s\nDETAIL: %s", r.Result[valTarget].String(), castTyp.String(), err.Error())
		}
		r.Params[paramTarget].VarDatum = newDatum
	} else {
		if cursorFetchAssignIndex < len(r.CursorFetchRow) {
			r.Params[paramTarget].VarDatum = r.CursorFetchRow[cursorFetchAssignIndex]
			cursorFetchAssignIndex++
		}
	}
	return err
}

// DeclareDrDBVarible use to Declare ZNBase Varible
//export DeclareDrDBVarible
func DeclareDrDBVarible(
	handler C.Handler_t,
	varName *C.char,
	typeName *C.char,
	i C.int,
	isCursorArg C.bool,
	notNull C.bool,
	defaultVal *C.char,
	errMsg **C.char,
) C.bool {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return C.bool(false)
	}
	var defaultValGO tree.Datum

	// get column type
	goName := C.GoString(varName)
	typeStr := C.GoString(typeName)
	var typ oid.Oid
	var colTyp coltypes.T
	if strings.ToLower(typeStr) == "cursor" {
		typ = oid.T_refcursor
	} else {
		if goName == "(unnamed row)" {
			typeStr = "string"
		}
		declareStmt := fmt.Sprintf("declare \"%s\" %s", goName, typeStr)
		stmt, err := parser.ParseOne(declareStmt, false)
		if err != nil {
			*errMsg = C.CString(err.Error())
			return C.bool(false)
		}

		if stmt.AST != nil {
			colTyp = stmt.AST.(*tree.DeclareVariable).VariableType
			colTypName := colTyp.TypeName()
			typ = tree.TypeNameGetOID[colTypName]
		} else {
			typ = oid.Oid(0)
		}
	}

	// consider cursor variable
	if len(C.GoString(defaultVal)) != 0 && typ != oid.T_refcursor {
		var planIndex C.uint
		var errmsg *C.char
		var isQueryORHasReturning C.bool
		errmsg = BepiPreparePlan(handler, defaultVal, 0, &planIndex)
		if len(C.GoString(errmsg)) != 0 {
			*errMsg = errmsg
			return C.bool(false)
		}
		results, err := ExecInternal(handler, planIndex, &isQueryORHasReturning)
		if err != nil || len(results) == 0 {
			return C.bool(false)
		}
		defaultValGO = results[0]
	}
	// 这里有可能发生空指针错误
	goNotNull := bool(notNull)
	index := (int)(i)
	goIsCurArg := bool(isCursorArg)
	if index < len(udrRunParams.Params) && index >= 0 {
		udrRunParams.EvalCtx.Params = udrRunParams.Params
		if goName != "" && goName != udrRunParams.Params[index].VarName {
			udrRunParams.Params[index].AliasNames = append(udrRunParams.Params[index].AliasNames, goName)
		}
		return C.bool(true)
	}

	datum, _ := pgwirebase.OidGetDefaultDatum(typ)
	tempUDRVar := &tree.UDRVar{}
	tempUDRVar.IsNull = true
	tempUDRVar.NotNull = goNotNull
	if defaultValGO != nil {
		tempUDRVar.VarDatum = defaultValGO
	} else {
		tempUDRVar.VarDatum = datum
	}
	tempUDRVar.VarName = goName
	tempUDRVar.IsCursorArg = goIsCurArg
	if tempUDRVar.VarName == "found" {
		udrRunParams.FoundIdx = index
		tempUDRVar.ColType = coltypes.Bool
		tempUDRVar.IsNull = false
	} else {
		// serial type resolve to int type in UDR
		if colTyp != nil {
			if TSerial, ok := colTyp.(*coltypes.TSerial); ok {
				colTyp = TSerial.TInt
			}
			tempUDRVar.ColType = colTyp
		}
	}
	tempUDRVar.VarOid = typ
	if typ != oid.T_refcursor {
		tempUDRVar.State = tree.NotCursor
	} else {
		tempUDRVar.State = tree.CursorUndeclared
	}
	tempUDRVar.AliasNames = make([]string, 0)
	udrRunParams.Params = append(udrRunParams.Params, *tempUDRVar)
	udrRunParams.EvalCtx.Params = append(udrRunParams.EvalCtx.Params, *tempUDRVar)
	// udrRunParams.Result = append(udrRunParams.Result, datum)
	return C.bool(true)
}

// DeclareVarible for C LANG
//export DeclareVarible
func DeclareVarible(handler C.Handler_t, varName *C.char, typName *C.char, notNull C.bool) C.bool {
	// oid := SearchTypeOidByName(handler, typName)
	errMsg := C.makeCharArray(C.int(1))
	defer func() {
		C.freeCharArray(errMsg, 1)
	}()
	return DeclareDrDBVarible(handler, varName, typName, -1, C.bool(false), C.bool(notNull), C.CString(""), errMsg)
}

// DeclareDrDBVariableGo use to declare variable for plgolang
func (r *UDRRunParam) DeclareDrDBVariableGo(varName *string, t oid.Oid, i int) bool {
	index := (int)(i)
	typ := (oid.Oid)((uint)(t))
	datum, _ := pgwirebase.OidGetDefaultDatum(typ)
	if index < len(r.Params) {
		r.EvalCtx.Params = append(r.EvalCtx.Params, r.Params[index])
		return true
	}
	tempUDRVar := &tree.UDRVar{}
	tempUDRVar.VarDatum = datum
	tempUDRVar.VarName = *varName
	if tempUDRVar.VarName == "found" {
		r.FoundIdx = index
	}
	tempUDRVar.VarOid = typ
	if typ != oid.T_refcursor {
		tempUDRVar.State = tree.NotCursor
	} else {
		tempUDRVar.State = tree.CursorDeclared
	}
	r.Params = append(r.Params, *tempUDRVar)
	r.EvalCtx.Params = append(r.EvalCtx.Params, *tempUDRVar)
	r.Result = append(r.Result, datum)
	return true
}

// DeclareVarGo use to Declare variable
func (r *UDRRunParam) DeclareVarGo(declareStr string) (string, oid.Oid) {

	ss := strings.Split(declareStr, " ")
	if len(ss) != 2 {
		return "declare var format default", 0
	}
	varname := ss[0]
	typOid := GetOidByName(ss[1])
	return varname, typOid
}

// SetupFound use to Setup Found
//export SetupFound
func SetupFound(handler C.Handler_t) C.bool {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return C.bool(false)
	}
	typ := oid.T_bool
	datum, _ := pgwirebase.OidGetDefaultDatum(typ)
	tempUDRVar := &tree.UDRVar{}
	tempUDRVar.VarDatum = datum
	tempUDRVar.VarName = "found"
	udrRunParams.FoundIdx = len(udrRunParams.Params)
	tempUDRVar.VarOid = typ
	tempUDRVar.IsNull = false
	tempUDRVar.State = tree.NotCursor
	udrRunParams.Params = append(udrRunParams.Params, *tempUDRVar)
	udrRunParams.EvalCtx.Params = append(udrRunParams.EvalCtx.Params, *tempUDRVar)
	udrRunParams.Result = append(udrRunParams.Result, datum)
	return C.bool(true)
}

// FindParamsName use to Find ParamsName
//export FindParamsName
func FindParamsName(handler C.Handler_t, paramsIndex C.int) *C.char {
	index := int(paramsIndex)
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	return C.CString(udrRunParams.Params[index].VarDatum.String())
}

// LookUpParams use to LookUp Params
//export LookUpParams
func LookUpParams(handler C.Handler_t) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	fmt.Println(udrRunParams.Params, len(udrRunParams.Params))
	return ReportErrorToPlsql("")
}

// BepiPreparePlan a fuction use to Prepare a Plan
//export BepiPreparePlan
func BepiPreparePlan(
	handler C.Handler_t, str *C.char, cursorOption C.int, planIndex *C.uint,
) (errmsg *C.char) {
	defer func() {
		if r := recover(); r != nil {
			errmsg = ReportErrorToPlsql(fmt.Sprintln(r))
		}
	}()
	queryString := C.GoString(str)
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	plan, _ := NewInternalPlanner("bepi-prepare",
		udrRunParams.EvalCtx.Txn,
		"root", // TODO: pass user
		&MemoryMetrics{},
		udrRunParams.ExecCfg,
	)
	p := plan.(*planner)
	// set sesssion mutator
	if udrRunParams.EvalCtx.SessionMutator != nil {
		p.EvalContext().SessionData = udrRunParams.EvalCtx.SessionMutator.data
		p.sessionDataMutator.data = udrRunParams.EvalCtx.SessionMutator.data
	}
	p.extendedEvalCtx = *udrRunParams.EvalCtx

	if strings.HasPrefix(queryString, "e'") && strings.HasSuffix(queryString, "'") {
		queryString = queryString[2 : len(queryString)-1]
		queryString = strings.Replace(queryString, "\\n", " ", -1)
		queryString = strings.Replace(queryString, "\\'", "'", -1)
	}
	stmt, err := parser.ParseOne(queryString, false)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	if _, ok := stmt.AST.(*tree.CreateDatabase); ok {
		errmsg = ReportErrorToPlsql("CREATE DATABASE cannot be executed from a function")
		return errmsg
	}
	p.stmt = &Statement{Statement: stmt}
	p.optPlanningCtx.init(p)
	p.semaCtx.InUDR = true
	p.semaCtx.UDRParms = udrRunParams.Params
	p.nameResolutionVisitor.InUDR = true
	p.nameResolutionVisitor.Params = udrRunParams.Params
	defer func() {
		p.semaCtx.InUDR = false
		p.nameResolutionVisitor.InUDR = false
	}()

	if err := p.semaCtx.Placeholders.Assign(nil, stmt.NumPlaceholders); err != nil {
		return ReportErrorToPlsql(err)
	}
	opc := &p.optPlanningCtx
	opc.reset()
	opc.InUDR = true
	defer func() {
		opc.InUDR = false
	}()

	/* Check to see if it's a simple expression */
	isSimple := p.checkSimpleExpr()
	if isSimple { //  && strings.Compare(strings.ToLower(queryString), "select not found") != 0
		f := opc.optimizer.Factory()
		bld := optbuilder.New(udrRunParams.Ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, stmt.AST)
		bld.SetInUDR(true)
		bld.SetUDRParam(udrRunParams.Params)
		defer func() {
			bld.SetInUDR(false)
			bld.SetUDRParam(nil)
		}()
		builtScalarExpr, builtExpr := bld.BuildStmtAndReturnScalarExpr(stmt.AST)
		hashKey := util.CRC32Origin([]byte(queryString))
		*planIndex = C.uint(hashKey)
		scalarBuilder := execbuilder.Builder{}
		var scalarRet tree.Datum
		scalarResult, err := scalarBuilder.BuildScalarExprToTypedExpr(nil, builtScalarExpr)
		if err == nil {
			p.EvalContext().SelectCount = udrRunParams.SelectCount
			scalarRet, err = scalarResult.Eval(p.EvalContext())
			if err != nil {
				return ReportErrorToPlsql(err)
			}
		}
		udrRunParams.CachedPlan[hashKey] = UDRPlanner{
			Planner:  p,
			Expr:     builtExpr,
			Result:   scalarRet,
			IsSimple: isSimple}
		return ReportErrorToPlsql("")
	}
	//build Memo and store
	// present is update/delete current of cursor statement
	isUpCOC := false
	isDelCOC := false
	var execMemo *memo.Memo
	if updateStmt, ok := stmt.AST.(*tree.Update); ok {
		if updateStmt.Where != nil {
			if _, ok := updateStmt.Where.Expr.(*tree.CurrentOfExpr); ok {
				isUpCOC = true
			}
		}
	} else if deleteStmt, ok := stmt.AST.(*tree.Delete); ok {
		if deleteStmt.Where != nil {
			if _, ok := deleteStmt.Where.Expr.(*tree.CurrentOfExpr); ok {
				isDelCOC = true
			}
		}
	}
	if err := checkOptSupportForTopStatement(stmt.AST); err == nil && (!isUpCOC && !isDelCOC) {
		opc.catalog.planner.semaCtx.UDRParms = udrRunParams.Params
		execMemo, err = opc.buildExecMemo(udrRunParams.Ctx)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
	} else {
		err = p.makePlan(udrRunParams.Ctx)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
	}
	// 根据表达式计算hash 值/
	hashKey := util.CRC32Origin([]byte(queryString))
	*planIndex = C.uint(hashKey)
	udrRunParams.CachedPlan[hashKey] = UDRPlanner{Planner: p, Memo: execMemo, IsSimple: isSimple}
	return ReportErrorToPlsql("")
}

// BepiExecPlan a func use to Execute a Plan
//export BepiExecPlan
func BepiExecPlan(handler C.Handler_t, planIndex C.uint, isQueryORHasReturning *C.bool) *C.char {
	UDRExecRes.Res = UDRExecRes.Res[:0]
	UDRExecRes.RowCount = 0
	_, err := ExecInternal(handler, planIndex, isQueryORHasReturning)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	return ReportErrorToPlsql("")
}

// BepiExecBoolExpr a fuction
//export BepiExecBoolExpr
func BepiExecBoolExpr(handler C.Handler_t, planIndex C.uint, errorMsg **C.char) C.bool {
	var isQueryOrHasReturning C.bool
	ret, err := ExecInternal(handler, planIndex, &isQueryOrHasReturning)
	if err != nil {
		return C.bool(false)
	}
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return C.bool(false)
	}
	retBool, err := perforCastWithoutNull(&udrRunParams.EvalCtx.EvalContext, ret[0], coltypes.Bool)
	// retBool, err := tree.ParseStringAs(types.Bool, ret[0].String(), &udrRunParams.EvalCtx.EvalContext, false)
	if err != nil {
		return C.bool(false)
	}
	if retBool == tree.DNull {
		//*errorMsg = C.CString("Cannot use NULL values for comparison in plpgsql")
		return C.bool(false)
	}
	if retBool.(*tree.DBool) == tree.DBoolTrue {
		return C.bool(true)
	}
	return C.bool(false)
}

// BepiExecBoolExprGo eval bool expr
func (r *UDRRunParam) BepiExecBoolExprGo(planIndex uint) bool {
	ret, err := ExecInternalGo(*r, planIndex)
	if err != nil {
		return false
	}
	if ret[0].String() == "true" {
		return true
	}
	return false
}

// BepiSetResultInt a fuc use to Set ResultInt
//export BepiSetResultInt
func BepiSetResultInt(handler C.Handler_t, num C.int) {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return
	}
	udrRunParams.Result = []tree.Datum{tree.Datum(tree.NewDInt(tree.DInt(num)))}
}

// BepiGetResultLen a func use to Get ResultLen
//export BepiGetResultLen
func BepiGetResultLen(handler C.Handler_t) C.int {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return C.int(0)
	}
	if len(udrRunParams.Result) > 0 {
		if array, ok := udrRunParams.Result[0].(*tree.DArray); ok {
			udrRunParams.Result = array.Array
			return C.int(len(array.Array))
		}
	}
	return C.int(len(udrRunParams.Result))

}

// ExecInternal a function use to execute internal
func ExecInternal(
	handler C.Handler_t, planIndex C.uint, isQueryORHasReturning *C.bool,
) (ret tree.Datums, err error) {
	defer func() {
		if r := recover(); r != nil {
			rmsg := fmt.Sprintln(r)
			err = errors.New(rmsg)
		}
	}()
	var udrRunParams *UDRRunParam
	udrRunParams, _, err = ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return nil, err
	}
	udrPlanner, ok := udrRunParams.CachedPlan[uint32(planIndex)]
	evalCtx := udrRunParams.EvalCtx
	execCfg := udrRunParams.ExecCfg
	if ok {
		if udrPlanner.IsSimple {
			*isQueryORHasReturning = C.bool(true)
			var res C.VarRes
			if udrPlanner.Result != nil {
				udrRunParams.Result = []tree.Datum{udrPlanner.Result}
				udrPlanner.Result = nil
				udrRunParams.CachedPlan[uint32(planIndex)] = udrPlanner

				UDRExecRes.Res = append(UDRExecRes.Res, udrRunParams.Result)
				UDRExecRes.RowCount = 1
				a := udrPlanner.Planner.stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs[0]
				s, err := tree.GetRenderColName(udrRunParams.EvalCtx.SessionData.SearchPath, a)
				if err != nil {
					return nil, err
				}
				colNames := []string{s}
				UDRExecRes.Names = colNames

				return udrRunParams.Result, nil
			}
			isNull := false
			err := BepiExecEvalExprAndStoreInResultAPI(handler, planIndex, &res, &isNull)
			if err == nil {
				return udrRunParams.Result, nil
			}
			return nil, err
		}
		// found a logic plan, execute it
		planner := udrPlanner.Planner
		planner.EvalContext().Params = udrRunParams.Params
		opc := &planner.optPlanningCtx
		if udrPlanner.Memo != nil {
			err := opc.runExecBuilder(
				&planner.curPlan,
				planner.stmt,
				newExecFactory(planner),
				udrPlanner.Memo,
				planner.EvalContext(),
				planner.autoCommit,
			)
			// err := buildCurPlanNode(planner, udrPlanner.Memo)
			if err != nil {
				return nil, err
			}
			planner.curPlan.AST = planner.stmt.AST
			planner.curPlan.subqueryPlans = planner.curPlan.planComponents.subqueryPlans
		}
		*isQueryORHasReturning = C.bool(checkIsQueryOrHasReturning(planner.curPlan.AST))
		ctx := udrRunParams.Ctx
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			return nil
		})
		rw.IsUDRPlanner = true
		colNames := make([]string, 0)
		columns := planColumns(planner.curPlan.main.planNode)
		for _, col := range columns {
			colNames = append(colNames, col.Name)
		}
		UDRExecRes.Names = colNames

		recv := MakeDistSQLReceiver(
			ctx,
			rw,
			planner.stmt.AST.StatementType(),
			execCfg.RangeDescriptorCache,
			execCfg.LeaseHolderCache,
			planner.txn, //evalCtx.Txn,
			func(ts hlc.Timestamp) {
				_ = execCfg.Clock.Update(ts)
			},
			planner.ExtendedEvalContext().Tracing,
		)
		planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, nil /* txn */)
		planCtx.isLocal = true

		planCtx.planner = planner
		oldEvalPlanner := evalCtx.Planner
		evalCtx.Planner = planner
		defer func() {
			evalCtx.Planner = oldEvalPlanner
		}()
		planCtx.stmtType = recv.stmtType
		if len(planner.curPlan.subqueryPlans) != 0 {
			var evalCtx2 = udrRunParams.EvalCtx.copy()
			// using evalCtx2'd deep copy for subquery in case of corrupt main query contex
			evalCtx2.Planner = planner
			evalCtxFactory := func() *extendedEvalContext {
				evalCtx2.TxnModesSetter.(*connExecutor).resetEvalCtx(evalCtx2, planner.txn, planner.ExtendedEvalContext().StmtTimestamp)
				evalCtx2.Placeholders = &planner.semaCtx.Placeholders
				return evalCtx2
			}
			if !udrRunParams.ExecCfg.DistSQLPlanner.PlanAndRunSubqueries(
				ctx, planner, evalCtxFactory, planner.curPlan.subqueryPlans, recv, true,
			) {
				return nil, errors.New("can't exec plan and run sub queries")
			}
		}
		if planner.curPlan.plan == nil {
			planner.curPlan.plan = &valuesNode{}
		}
		ExecInUDR = true
		planner.semaCtx.SetInUDR(true)
		execCfg.DistSQLPlanner.PlanAndRun(
			ctx, evalCtx, planCtx, evalCtx.Txn, planner.curPlan.main, recv)
		if recv.resultWriter.Err() != nil {
			return nil, recv.resultWriter.Err()
		}
		udrRunParams.EvalCtx = planner.ExtendedEvalContext().copy()
		if rw.IsUDRPlanner && rw.curRow != nil {
			udrRunParams.Result = rw.curRow
		} else {
			if recv.row != nil {
				udrRunParams.Result = recv.row
			} else {
				udrRunParams.Result = udrRunParams.Result[0:0]
			}
		}
		err := rw.Err()
		if err != nil {
			return nil, err
		}
		udrRunParams.RowAffected = rw.rowsAffected
	} else {
		err = errors.New("Can not found a cached plan to execute this plan")
		return nil, err
	}

	result := make([]tree.Datum, 0)
	for _, v := range udrRunParams.Result {
		result = append(result, v)
	}
	return result, nil
}

// BepiGetReturn a function kuse to Get Return
//export BepiGetReturn
func BepiGetReturn(
	handler C.Handler_t,
	Oid **C.uint,
	rows *C.long,
	attrNames ***C.char,
	attrNum *C.int,
	attrSizes **C.int,
) **C.char {
	typArray := make([]C.uint, 0)
	names := make([]*C.char, 0)
	for _, name := range UDRExecRes.Names {
		names = append(names, C.CString(name))
	}
	sizeArray := make([]C.int, 0)
	if len(UDRExecRes.Res) == 0 {
		*rows = C.long(0)
		return C.makeCharArray(C.int(1))
	}
	cResStrs := C.makeCharArray(C.int(UDRExecRes.RowCount * len(UDRExecRes.Res[0])))
	for i, v := range UDRExecRes.Res {
		for j, v2 := range v {
			{
				typOid := v2.ResolvedType().Oid()
				typArray = append(typArray, C.uint(typOid))
			}
			if v2.ResolvedType().Oid() == oid.T_text {
				tempStr := string(tree.MustBeDString(v2))
				sizeArray = append(sizeArray, C.int(len(tempStr)))
				C.setArrayString(cResStrs, C.CString(tempStr), C.int(i*len(UDRExecRes.Res[0])+j))
				continue
			}
			if v2.ResolvedType().Oid() == oid.T_bytea {
				tempStr := string(tree.MustBeDBytes(v2))
				sizeArray = append(sizeArray, C.int(len(tempStr)))
				C.setArrayString(cResStrs, C.CString(tempStr), C.int(i*len(UDRExecRes.Res[0])+j))
				continue
			}
			valStr := v2.String()
			if valStr[0] == '\'' && valStr[len(valStr)-1] == '\'' {
				valStr = valStr[1 : len(valStr)-1]
			}
			sizeArray = append(sizeArray, C.int(len(valStr)))
			C.setArrayString(cResStrs, C.CString(valStr), C.int(i*len(UDRExecRes.Res[0])+j))
		}
	}
	*attrNames = (**C.char)(unsafe.Pointer(&names[0]))
	*Oid = (*C.uint)(unsafe.Pointer(&typArray[0]))
	*rows = C.long(UDRExecRes.RowCount)
	*attrNum = C.int(len(UDRExecRes.Res[0]))
	*attrSizes = (*C.int)(unsafe.Pointer(&sizeArray[0]))
	return cResStrs
}

// BepiGetCursorFetchRow a function use to Get Cursor Fetch Row
//export BepiGetCursorFetchRow
func BepiGetCursorFetchRow(
	handler C.Handler_t,
	Oid **C.uint,
	rows *C.int,
	attrNames ***C.char,
	attrNum *C.int,
	attrSizes **C.int,
	errorMsg **C.char,
) **C.char {
	typArray := make([]C.uint, 0)
	names := make([]*C.char, 0)
	udrRunParams, _, _ := ConstructUDRRunParamByHandler(handler)
	if udrRunParams.CursorColNum == 0 {
		*rows = C.int(0)
		*errorMsg = C.CString("BEPI_cursor_get_fetch_row() should be used after BEPI_cursor_fetch()")
		return C.makeCharArray(C.int(1))
	}

	sizeofRows := len(udrRunParams.CursorFetchRow) / udrRunParams.CursorColNum
	if len(udrRunParams.CursorFetchRow)%udrRunParams.CursorColNum != 0 {
		sizeofRows++
	}
	if sizeofRows <= 0 {
		*rows = C.int(0)
		return C.makeCharArray(C.int(1))
	}
	for i := 0; i < udrRunParams.CursorColNum; i++ {
		names = append(names, C.CString("col"+strconv.Itoa(i)))
	}
	sizeArray := make([]C.int, 0)
	//cResStrs := C.makeCharArray(C.int(UDRExecRes.RowCount * len(UDRExecRes.Res[0])))
	cResStrs := C.makeCharArray(C.int(len(udrRunParams.CursorFetchRow)))
	for i := 0; i < sizeofRows; i++ {
		for j := 0; j < udrRunParams.CursorColNum; j++ {
			v := udrRunParams.CursorFetchRow[i*udrRunParams.CursorColNum+j]
			typOid := v.ResolvedType().Oid()
			typArray = append(typArray, C.uint(typOid))
			if v.ResolvedType().Oid() == oid.T_text {
				tempStr := string(tree.MustBeDString(v))
				sizeArray = append(sizeArray, C.int(len(tempStr)))
				C.setArrayString(cResStrs, C.CString(tempStr), C.int(i))
				continue
			}
			if v.ResolvedType().Oid() == oid.T_bytea {
				tempStr := string(tree.MustBeDBytes(v))
				sizeArray = append(sizeArray, C.int(len(tempStr)))
				C.setArrayString(cResStrs, C.CString(tempStr), C.int(i))
				continue
			}
			valStr := v.String()
			if valStr[0] == '\'' && valStr[len(valStr)-1] == '\'' {
				valStr = valStr[1 : len(valStr)-1]
			}
			sizeArray = append(sizeArray, C.int(len(valStr)))
			C.setArrayString(cResStrs, C.CString(valStr), C.int(i*udrRunParams.CursorColNum+j))
		}

	}
	*attrNames = (**C.char)(unsafe.Pointer(&names[0]))
	*Oid = (*C.uint)(unsafe.Pointer(&typArray[0]))
	*rows = C.int(sizeofRows)
	*attrNum = C.int(udrRunParams.CursorColNum)
	*attrSizes = (*C.int)(unsafe.Pointer(&sizeArray[0]))
	*errorMsg = C.CString("")
	return cResStrs
}

// BepiExecEvalExpr a function use to ExecEval Expr
//export BepiExecEvalExpr
func BepiExecEvalExpr(handler C.Handler_t, planIndex C.uint, varRes *C.VarRes) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	udrPlanner, ok := udrRunParams.CachedPlan[uint32(planIndex)]
	if ok {
		if udrPlanner.IsSimple {
			expr := udrPlanner.Expr
			data, err := expr.Eval(&udrRunParams.EvalCtx.EvalContext)
			if err != nil {
				return ReportErrorToPlsql(err)
			}
			varRes.typ = C.uint(data.ResolvedType().Oid())
			varRes.len = C.int(len(data.String()))
			varRes.data = (*C.char)(unsafe.Pointer(C.CString(data.String())))
			udrRunParams.Result = []tree.Datum{data}
		}
	}

	return (*C.char)(unsafe.Pointer(C.CString("")))
}

// BepiExecEvalExprGo a function use to Execute EvalExpr
func (r *UDRRunParam) BepiExecEvalExprGo(planIndex uint) error {
	udrPlanner, ok := r.CachedPlan[uint32(planIndex)]
	if ok {
		if udrPlanner.IsSimple {
			expr := udrPlanner.Expr
			data, err := expr.Eval(&r.EvalCtx.EvalContext)
			if err != nil {
				return err
			}
			r.Result = []tree.Datum{data}
		}
	}
	return errors.New("")
}

// BepiExecReturnAndSet a function use to return and set
//export BepiExecReturnAndSet
func BepiExecReturnAndSet(handler C.Handler_t, sqlStr *C.char, target C.int) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	tar := int(target)
	if len(udrRunParams.Params) < tar+1 {
		return ReportErrorToPlsql("the target is overflow")
	}
	if tar == -1 {
		if udrRunParams.Result == nil {
			return ReportErrorToPlsql("the target is overflow")
		}
		if len(udrRunParams.Result) > 1 {
			sql := C.GoString(sqlStr)
			return ReportErrorToPlsql(fmt.Sprintf("ERROR: query \"%s\" returned %d columns",
				sql,
				len(udrRunParams.Result)))
		}
		udrRunParams.Return = udrRunParams.Result[0]
		return (*C.char)(unsafe.Pointer(C.CString("")))
	}
	if udrRunParams.Params[tar].IsNull || udrRunParams.Params[tar].VarDatum == nil {
		udrRunParams.Return = tree.DNull
	} else {
		udrRunParams.Return = udrRunParams.Params[tar].VarDatum
	}
	return (*C.char)(unsafe.Pointer(C.CString("")))
}

// BepiExecEvalExprAndStoreInResultAPI : bepi interface for plclang to eval expr and store datum in result
func BepiExecEvalExprAndStoreInResultAPI(
	handler C.Handler_t, planIndex C.uint, varRes *C.VarRes, isNull *bool,
) error {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return err
	}
	udrPlanner, ok := udrRunParams.CachedPlan[uint32(planIndex)]
	if ok {
		if udrPlanner.IsSimple {
			expr := udrPlanner.Expr
			udrRunParams.EvalCtx.EvalContext.Params = udrRunParams.Params
			data, err := expr.Eval(&udrRunParams.EvalCtx.EvalContext)
			if err != nil {
				return err
			}
			udrRunParams.Result = []tree.Datum{data}
			if len(udrRunParams.Result) == 1 {
				if data == nil || data == tree.DNull {
					*isNull = true
				} else {
					varRes.typ = C.uint(udrRunParams.Result[0].ResolvedType().Oid())
					varRes.len = C.int(len(udrRunParams.Result[0].String()))
					varRes.data = (*C.char)(unsafe.Pointer(C.CString(udrRunParams.Result[0].String())))
				}
			} else if len(udrRunParams.Result) == 0 {
				*isNull = true
			}
		}
	}
	return nil
}

// BepiExecEvalExprAndStoreInResultAPIGo eval a expr and store in udrRunParam.result
func (r *UDRRunParam) BepiExecEvalExprAndStoreInResultAPIGo(planIndex uint) error {
	udrPlanner, ok := r.CachedPlan[uint32(planIndex)]
	if ok {
		if udrPlanner.IsSimple {
			expr := udrPlanner.Expr
			r.EvalCtx.EvalContext.Params = r.Params
			data, err := expr.Eval(&r.EvalCtx.EvalContext)
			if err != nil {
				return err
			}
			r.Result = []tree.Datum{data}
		}
	}
	return nil
}

// BepiExecEvalExprAndStoreInResult  a function execute evalexpr and store in result
//export BepiExecEvalExprAndStoreInResult
func BepiExecEvalExprAndStoreInResult(
	handler C.Handler_t, planIndex C.uint, varRes *C.VarRes, isNull *C.bool,
) *C.char {
	isNullGO := false
	err := BepiExecEvalExprAndStoreInResultAPI(handler, planIndex, varRes, &isNullGO)
	*isNull = C.bool(isNullGO)
	return ReportErrorToPlsql(err)
}

// BepiExecEvalExprAndStoreInResultGo a function execute evalexpr and store in result
func (r *UDRRunParam) BepiExecEvalExprAndStoreInResultGo(planIndex uint) string {
	ret := ""
	if err := r.BepiExecEvalExprAndStoreInResultAPIGo(planIndex); err != nil {
		ret = err.Error()
	}
	return ret
}

// BepiCursorDeclare declare a cursor variable
//export BepiCursorDeclare
func BepiCursorDeclare(
	handler C.Handler_t, cursorName *C.char, query *C.char, argStr *C.char, errmsg **C.char,
) C.int {
	var declareCursorStr string

	goArgStr := C.GoString(argStr)
	declareCursorStr = "declare " + C.GoString(cursorName) + " cursor" + goArgStr + " for " + C.GoString(query)
	declareCursorStmt, err := parser.ParseOne(declareCursorStr, false)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.int(0)
	}
	if declareCursorStmt.AST != nil {

	}

	// stmt, err := parser.ParseOne(C.GoString(query))
	// if err != nil {
	// 	*errmsg = ReportErrorToPlsql(err)
	// 	return C.int(0)
	// }
	// selectStmt, ok := stmt.AST.(*tree.Select)
	// if !ok {
	// 	*errmsg = ReportErrorToPlsql("statement for cursor is not a select statement\n")
	// 	return C.int(0)
	// }

	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	// parse queryString to get planNode
	plan, _ := NewInternalPlanner("bepi-cursor-declare",
		udrRunParams.EvalCtx.Txn,
		"root", // TODO: pass user
		&MemoryMetrics{},
		udrRunParams.ExecCfg,
	)
	p := plan.(*planner)

	// set sesssion mutator
	p.EvalContext().SessionData = udrRunParams.EvalCtx.SessionMutator.data
	// CurrrentSessionID and CursorTxnMap
	p.extendedEvalCtx.CurrentSessionID = udrRunParams.EvalCtx.CurrentSessionID
	p.extendedEvalCtx.curTxnMap = udrRunParams.EvalCtx.curTxnMap
	// declareCursor := &tree.DeclareCursor{Cursor: tree.Name(C.GoString(cursorName)), Select: selectStmt}
	cursorPlanNode, err := p.DeclareCursor(udrRunParams.Ctx, declareCursorStmt.AST.(*tree.DeclareCursor))
	if err != nil {
		*errmsg = ReportErrorToPlsql(err.Error())
		return C.int(0)
	}

	err = cursorPlanNode.startExec(
		runParams{
			ctx:             udrRunParams.Ctx,
			extendedEvalCtx: udrRunParams.EvalCtx,
			p:               p,
		},
	)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.int(0)
	}

	// assign the cursor state to CURSOR_DECLARED
	findCursor := false
	for i := len(udrRunParams.Params) - 1; i >= 0; i-- {
		if udrRunParams.Params[i].VarName == C.GoString(cursorName) && udrRunParams.Params[i].VarOid == oid.T_refcursor {
			findCursor = true
			udrRunParams.Params[i].State = tree.CursorDeclared
			break
		}
	}
	if !findCursor {
		udrRunParams.Params = append(udrRunParams.Params,
			tree.UDRVar{
				VarName: C.GoString(cursorName),
				VarOid:  oid.T_refcursor,
				State:   tree.CursorDeclared,
			})
	}
	return C.int(1)
}

// BepiCursorDeclareGo a method
func (r *UDRRunParam) BepiCursorDeclareGo(cursorName *string, query *string) int {
	stmt, err := parser.ParseOne(*query, r.EvalCtx.GetCaseSensitive())
	if err != nil {
		return int(0)
	}
	selectStmt, ok := stmt.AST.(*tree.Select)
	if !ok {
		return int(0)
	}

	// parse queryString to get planNode
	plan, _ := NewInternalPlanner("bepi-cursor-declare",
		r.EvalCtx.Txn,
		"root", // TODO: pass user
		&MemoryMetrics{},
		r.ExecCfg,
	)
	p := plan.(*planner)

	// set session mutator
	p.EvalContext().SessionData = r.EvalCtx.SessionMutator.data
	// CurrentSessionID and CursorTxnMap
	p.extendedEvalCtx.CurrentSessionID = r.EvalCtx.CurrentSessionID
	p.extendedEvalCtx.curTxnMap = r.EvalCtx.curTxnMap
	declareCursor := &tree.DeclareCursor{Cursor: tree.Name(*cursorName), Select: selectStmt}
	cursorPlanNode, err := p.DeclareCursor(r.Ctx, declareCursor)
	if err != nil {
		return int(0)
	}

	err = cursorPlanNode.startExec(
		runParams{
			ctx:             r.Ctx,
			extendedEvalCtx: r.EvalCtx,
			p:               p,
		},
	)
	if err != nil {
		return int(0)
	}

	// assign the cursor state to CURSOR_DECLARED
	isfound := 0
	for i := len(r.Params) - 1; i >= 0; i-- {
		if r.Params[i].VarName == *cursorName {
			r.Params[i].State = tree.CursorDeclared
			isfound = 1

		}
	}
	if isfound == 0 {
		tempArg := &tree.UDRVar{}
		tempArg.VarOid = oid.T_refcursor
		tempArg.VarName = *cursorName
		tempArg.State = tree.CursorDeclared
		r.Params = append(r.Params, *tempArg)
	}

	return 1
}

// BepiCursorFind a functin use to find cursor
//export BepiCursorFind
func BepiCursorFind(handler C.Handler_t, cursorName *C.char, errmsg **C.char) C.ulong {
	var cursorDescPLSQL C.struct_CursorDesc

	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.ulong(UnsafePointer2Int64(unsafe.Pointer(&cursorDescPLSQL)))
	}
	_, cursorDescZNBase, err2 := FindCursor(udrRunParams.ExecCfg, udrRunParams.EvalCtx, C.GoString(cursorName))
	if err2 != nil {
		*errmsg = ReportErrorToPlsql(err2.Error())
		return C.ulong(UnsafePointer2Int64(unsafe.Pointer(&cursorDescPLSQL)))
	}

	// construct struct CursorDesc in C side with existed cursorDesc in znbase
	cursorDescPLSQL.name = C.CString(cursorDescZNBase.Name)
	cursorDescPLSQL.id = C.uint(cursorDescZNBase.ID)
	cursorDescPLSQL.queryStr = C.CString(cursorDescZNBase.QueryString)
	cursorDescPLSQL.position = C.long(cursorDescZNBase.Position)
	return C.ulong(UnsafePointer2Int64(unsafe.Pointer(&cursorDescPLSQL)))
}

// BepiCursorFindGo a method of UDRRunParam
func (r *UDRRunParam) BepiCursorFindGo(cursorName *string) uint {

	_, cursorDescZNBase, err2 := FindCursor(r.ExecCfg, r.EvalCtx, *cursorName)
	if err2 != nil {
		return 0
	}

	return uint(UnsafePointer2Int64(unsafe.Pointer(&cursorDescZNBase)))
}

// BepiGetCusorFetchValueGo bepi interface for cursor fetch value
func (r *UDRRunParam) BepiGetCusorFetchValueGo() string {
	return r.CursorFetchRow[0].String()
}

// BepiCursorFetch use to scroll fetch
//export BepiCursorFetch
func BepiCursorFetch(
	handler C.Handler_t, cursorName *C.char, direction C.int, rows C.long, errmsg **C.char,
) C.bool {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	// reset udrRunParams.CursorFetchRow for each fetch
	udrRunParams.CursorFetchRow = udrRunParams.CursorFetchRow[0:0]

	cursorKey, cursorDescZNBase, err := FindCursor(udrRunParams.ExecCfg, udrRunParams.EvalCtx, C.GoString(cursorName))
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	if cursorDescZNBase.State == uint32(tree.CLOSE) {
		*errmsg = ReportErrorToPlsql(C.GoString(cursorName) + " has not been declared!\n")
		return C.bool(false)
	}
	if cursorDescZNBase.State == uint32(tree.DECLARE) {
		*errmsg = ReportErrorToPlsql(C.GoString(cursorName) + " is not open, unable to fetch!\n")
		return C.bool(false)
	}
	oldCursorDescZNBase := *cursorDescZNBase

	memEngine, err := GetCursorEngine(udrRunParams.ExecCfg)
	if err != nil {
		return C.bool(false)
	}

	direc := UDRCursorDirectionToGoCursorDirection(int8(direction))
	goRows := int64(rows)
	cursorAccess := &tree.CursorAccess{
		CursorName: tree.Name(cursorDescZNBase.Name),
		Action:     tree.FETCH,
		Rows:       goRows,
		Direction:  direc,
	}
	startPosition, endPosition := GetFetchActionStartpositionAndEndposition(cursorAccess, cursorDescZNBase)
	startKey, _ := MakeStartKeyAndEndKey(cursorDescZNBase, startPosition, endPosition)

	//if !startKey.Less(endKey) {
	//	*errmsg = ReportErrorToPlsql("start key less than end key")
	//	return C.bool(false)
	//}
	udrRunParams.CursorColNum = 0
	for _, col := range cursorDescZNBase.Columns {
		if col.Hidden {
			continue
		}
		udrRunParams.CursorColNum++
	}
	// update position of cursor Descriptor
	MoveCursorPosition(cursorAccess, cursorDescZNBase)
	cursorDescZNBase.State = uint32(tree.FETCH)
	cursorDescZNBase.TxnState = tree.PENDING
	// update descriptor
	value, err := protoutil.Marshal(cursorDescZNBase)

	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	// write meta data to cursor Engine directly
	if err := memEngine.Put(cursorKey, value); err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	AddCursorTxn(udrRunParams.EvalCtx.curTxnMap, oldCursorDescZNBase, *cursorDescZNBase, C.GoString(cursorName))

	bytes, err := memEngine.Get(startKey)
	if err != nil {
		*errmsg = ReportErrorToPlsql("error happen in bepi_fetch_cursor")
		return C.bool(false)
	}
	if bytes == nil {
		udrRunParams.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolFalse
		udrRunParams.EvalCtx.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolFalse
		*errmsg = ReportErrorToPlsql("")
		return C.bool(false)
	}

	for _, columnDesc := range cursorDescZNBase.Columns {
		var datum tree.Datum
		datum, bytes, err = sqlbase.DecodeTableValue(&sqlbase.DatumAlloc{}, columnDesc.DatumType(), bytes)
		if err != nil {
			*errmsg = ReportErrorToPlsql("error when decode value")
			return C.bool(false)
		}
		if columnDesc.Hidden {
			continue
		}
		udrRunParams.CursorFetchRow = append(udrRunParams.CursorFetchRow, datum)
	}

	udrRunParams.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolTrue
	udrRunParams.EvalCtx.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolTrue

	return C.bool(true)
}

// BepiCursorFetchPython use to scroll fetch for Python udr
//export BepiCursorFetchPython
func BepiCursorFetchPython(
	handler C.Handler_t, cursorName *C.char, direction C.int, rows C.long, errmsg **C.char,
) C.bool {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	// reset udrRunParams.CursorFetchRow for each fetch
	udrRunParams.CursorFetchRow = udrRunParams.CursorFetchRow[0:0]

	cursorKey, cursorDescZNBase, err := FindCursor(udrRunParams.ExecCfg, udrRunParams.EvalCtx, C.GoString(cursorName))
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	if cursorDescZNBase.State == uint32(tree.CLOSE) {
		*errmsg = ReportErrorToPlsql(C.GoString(cursorName) + " has not been declared!\n")
		return C.bool(false)
	}
	if cursorDescZNBase.State == uint32(tree.DECLARE) {
		*errmsg = ReportErrorToPlsql(C.GoString(cursorName) + " is not open, unable to fetch!\n")
		return C.bool(false)
	}
	oldCursorDescZNBase := *cursorDescZNBase

	memEngine, err := GetCursorEngine(udrRunParams.ExecCfg)
	if err != nil {
		return C.bool(false)
	}

	direc := UDRCursorDirectionToGoCursorDirection(int8(direction))
	goRows := int64(rows)
	cursorAccess := &tree.CursorAccess{
		CursorName: tree.Name(cursorDescZNBase.Name),
		Action:     tree.FETCH,
		Rows:       goRows,
		Direction:  direc,
	}
	startPosition, endPosition := GetFetchActionStartpositionAndEndposition(cursorAccess, cursorDescZNBase)
	udrRunParams.CursorColNum = 0
	for _, col := range cursorDescZNBase.Columns {
		if col.Hidden {
			continue
		}
		udrRunParams.CursorColNum++
	}
	// update position of cursor Descriptor
	MoveCursorPosition(cursorAccess, cursorDescZNBase)
	cursorDescZNBase.State = uint32(tree.FETCH)
	cursorDescZNBase.TxnState = tree.PENDING
	// update descriptor
	value, err := protoutil.Marshal(cursorDescZNBase)

	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	// write meta data to cursor Engine directly
	if err := memEngine.Put(cursorKey, value); err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	AddCursorTxn(udrRunParams.EvalCtx.curTxnMap, oldCursorDescZNBase, *cursorDescZNBase, C.GoString(cursorName))
	for startPosition < endPosition {
		startKey, _ := MakeStartKeyAndEndKey(cursorDescZNBase, startPosition, endPosition)
		bytes, err := memEngine.Get(startKey)
		if err != nil {
			*errmsg = ReportErrorToPlsql("error happen in bepi_fetch_cursor")
			return C.bool(false)
		}
		if bytes == nil {
			udrRunParams.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolFalse
			udrRunParams.EvalCtx.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolFalse
			*errmsg = ReportErrorToPlsql("")
			return C.bool(false)
		}

		for _, columnDesc := range cursorDescZNBase.Columns {
			var datum tree.Datum
			datum, bytes, err = sqlbase.DecodeTableValue(&sqlbase.DatumAlloc{}, columnDesc.DatumType(), bytes)
			if err != nil {
				*errmsg = ReportErrorToPlsql("error when decode value")
				return C.bool(false)
			}
			if columnDesc.Hidden {
				continue
			}
			udrRunParams.CursorFetchRow = append(udrRunParams.CursorFetchRow, datum)
		}
		startPosition++
	}

	udrRunParams.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolTrue
	udrRunParams.EvalCtx.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolTrue

	return C.bool(true)
}

// BepiCursorFetchGo is bepi interface for plgolang to fetch data from cursor
func (r *UDRRunParam) BepiCursorFetchGo(cursorName *string, direction int, rows int) bool {
	// reset udrRunParams.CursorFetchRow for each fetch
	r.CursorFetchRow = r.CursorFetchRow[0:0]
	r.EvalCtx.Params = r.Params
	cursorKey, cursorDescZNBase, err := FindCursor(r.ExecCfg, r.EvalCtx, *cursorName)
	if err != nil {
		return bool(false)
	}
	oldCursorDescZNBase := *cursorDescZNBase
	if cursorDescZNBase.State == uint32(tree.DECLARE) || cursorDescZNBase.State == uint32(tree.CLOSE) {
		return bool(false)
	}

	memEngine, err := GetCursorEngine(r.ExecCfg)
	if err != nil {
		return bool(false)
	}

	direc := UDRCursorDirectionToGoCursorDirection(int8(direction))
	goRows := int64(rows)
	cursorAccess := &tree.CursorAccess{
		CursorName: tree.Name(cursorDescZNBase.Name),
		Action:     tree.FETCH,
		Rows:       goRows,
		Direction:  direc,
	}
	startPosition, endPosition := GetFetchActionStartpositionAndEndposition(cursorAccess, cursorDescZNBase)
	startKey, endKey := MakeStartKeyAndEndKey(cursorDescZNBase, startPosition, endPosition)

	if !startKey.Less(endKey) {
		return bool(false)
	}
	// update position of cursor Descriptor
	MoveCursorPosition(cursorAccess, cursorDescZNBase)
	cursorDescZNBase.State = uint32(tree.FETCH)
	cursorDescZNBase.TxnState = tree.PENDING
	// update descriptor
	value, err := protoutil.Marshal(cursorDescZNBase)
	if err != nil {
		return bool(false)
	}
	// write meta data to cursor Engine directly
	if err := memEngine.Put(cursorKey, value); err != nil {
		return bool(false)
	}
	AddCursorTxn(r.EvalCtx.curTxnMap, oldCursorDescZNBase, *cursorDescZNBase, *cursorName)

	bytes, err := memEngine.Get(startKey)
	if err != nil {
		return bool(false)
	}
	if bytes == nil {
		r.Params[r.FoundIdx].VarDatum = tree.DBoolFalse
		r.EvalCtx.Params[r.FoundIdx].VarDatum = tree.DBoolFalse
		return bool(false)
	}

	for _, columnDesc := range cursorDescZNBase.Columns {
		var datum tree.Datum
		datum, bytes, err = sqlbase.DecodeTableValue(&sqlbase.DatumAlloc{}, columnDesc.DatumType(), bytes)
		if err != nil {
			return bool(false)
		}
		if columnDesc.Hidden {
			continue
		}
		r.CursorFetchRow = append(r.CursorFetchRow, datum)
	}

	r.Params[r.FoundIdx].VarDatum = tree.DBoolTrue
	r.EvalCtx.Params[r.FoundIdx].VarDatum = tree.DBoolTrue

	return bool(true)
}

// BepiCursorMove  a function use to Scroll CursorMove
//export BepiCursorMove
func BepiCursorMove(
	handler C.Handler_t, cursorName *C.char, direction C.int, moveCount C.long, errmsg **C.char,
) C.bool {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}

	cursorKey, cursorDescZNBase, err := FindCursor(udrRunParams.ExecCfg, udrRunParams.EvalCtx, C.GoString(cursorName))
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	oldCursorDescZNBase := *cursorDescZNBase
	if cursorDescZNBase.State == uint32(tree.DECLARE) || cursorDescZNBase.State == uint32(tree.CLOSE) {
		return C.bool(false)
	}
	// calculate how many row should b fetch in this action

	// direction |    action
	//     0     |   forward
	//     1     |   backward
	//     2     |   absolute
	//     3     |   relative
	direc := UDRCursorDirectionToGoCursorDirection(int8(direction))
	rows := int64(moveCount)
	cursorAccess := &tree.CursorAccess{
		CursorName: tree.Name(cursorDescZNBase.Name),
		Action:     tree.MOVE,
		Rows:       rows,
		Direction:  direc,
	}
	// update position of cursor Descriptor
	MoveCursorPosition(cursorAccess, cursorDescZNBase)
	cursorDescZNBase.State = uint32(tree.MOVE)
	cursorDescZNBase.TxnState = tree.PENDING
	value, err := protoutil.Marshal(cursorDescZNBase)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	// write meta data to cursor Engine directly
	memEngine, err := GetCursorEngine(udrRunParams.ExecCfg)
	if err != nil {
		return C.bool(false)
	}
	if err := memEngine.Put(cursorKey, value); err != nil {
		*errmsg = ReportErrorToPlsql(err)
		return C.bool(false)
	}
	AddCursorTxn(udrRunParams.EvalCtx.curTxnMap, oldCursorDescZNBase, *cursorDescZNBase, C.GoString(cursorName))
	return C.bool(true)
}

// BepiCursorMoveGo a method
func (r *UDRRunParam) BepiCursorMoveGo(cursorName *string, direction int, moveCount int) bool {

	cursorKey, cursorDescZNBase, err := FindCursor(r.ExecCfg, r.EvalCtx, *cursorName)
	if err != nil {
		return bool(false)
	}
	if cursorDescZNBase.State == uint32(tree.DECLARE) || cursorDescZNBase.State == uint32(tree.CLOSE) {
		return bool(false)
	}
	oldCursorDescZNBase := *cursorDescZNBase
	// calculate how many row should b fetch in this action
	direc := UDRCursorDirectionToGoCursorDirection(int8(direction))
	rows := int64(moveCount)
	cursorAccess := &tree.CursorAccess{
		CursorName: tree.Name(cursorDescZNBase.Name),
		Action:     tree.FETCH,
		Rows:       rows,
		Direction:  direc,
	}
	// update position of cursor Descriptor
	MoveCursorPosition(cursorAccess, cursorDescZNBase)
	cursorDescZNBase.State = uint32(tree.MOVE)
	cursorDescZNBase.TxnState = tree.PENDING
	value, err := protoutil.Marshal(cursorDescZNBase)
	if err != nil {
		return bool(false)
	}
	// write meta data to cursor Engine directly
	memEngine, err := GetCursorEngine(r.ExecCfg)
	if err != nil {
		return bool(false)
	}
	if err := memEngine.Put(cursorKey, value); err != nil {
		return bool(false)
	}
	AddCursorTxn(r.EvalCtx.curTxnMap, oldCursorDescZNBase, *cursorDescZNBase, *cursorName)
	return bool(true)
}

// BepiAssignCursorOpen use to open cursor
//export BepiAssignCursorOpen
func BepiAssignCursorOpen(handler C.Handler_t, cursorName *C.char) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	// assign the cursor state to CURSOR_OPENED
	for i := len(udrRunParams.Params) - 1; i >= 0; i-- {
		if udrRunParams.Params[i].VarName == C.GoString(cursorName) {
			udrRunParams.Params[i].State = tree.CursorOpened
		}
	}
	return ReportErrorToPlsql("")
}

// BepiCursorOpenGo :a method to open cursor
func (r *UDRRunParam) BepiCursorOpenGo(cursorName *string) error {
	var index uint
	index = 0
	querystr := "open " + *cursorName
	err := r.BepiPreparePlanGo(querystr, &index)
	if err != nil {
		return err
	}
	err = r.BepiExecPlanGo(index)
	if err != nil {
		return err
	}
	// assign the cursor state to CURSOR_OPENED
	for i := len(r.Params) - 1; i >= 0; i-- {
		if r.Params[i].VarName == *cursorName {
			r.Params[i].State = tree.CursorOpened
		}
	}
	return err
}

// BepiCursorClose use to close cursor
//export BepiCursorClose
func BepiCursorClose(handler C.Handler_t, cursorName *C.char, errmsg **C.char) {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
	}
	cursorKey, cursorDescZNBase, err2 := FindCursor(udrRunParams.ExecCfg, udrRunParams.EvalCtx, C.GoString(cursorName))
	if err2 != nil {
		*errmsg = ReportErrorToPlsql(err2.Error())
	}
	if cursorDescZNBase.State == uint32(tree.CLOSE) {
		*errmsg = ReportErrorToPlsql(C.GoString(cursorName) + " is not exist!")
	}
	oldCursorDescZNBase := *cursorDescZNBase
	cursorDescZNBase.State = uint32(tree.CLOSE)
	cursorDescZNBase.TxnState = tree.PENDING
	//if err := ClearCursorDataAndDesc(descKeyPtr, memEngine, cursorDescZNBase); err != nil {
	//	fmt.Print(err)
	//}
	value, err := protoutil.Marshal(cursorDescZNBase)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err.Error())
	}
	memEngine, err := GetCursorEngine(udrRunParams.ExecCfg)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err)
	}

	if err := memEngine.Put(cursorKey, value); err != nil {
		*errmsg = ReportErrorToPlsql(err)
	}
	AddCursorTxn(udrRunParams.EvalCtx.curTxnMap, oldCursorDescZNBase, *cursorDescZNBase, C.GoString(cursorName))
	//if err := ClearCursorDataAndDesc(cursorKey, memEngine, cursorDescZNBase); err != nil {
	//	*errmsg = ReportErrorToPlsql(err.Error())
	//}
	// assign the cursor state to CURSOR_DECLARED
	for i := len(udrRunParams.Params) - 1; i >= 0; i-- {
		if udrRunParams.Params[i].VarName == C.GoString(cursorName) {
			udrRunParams.Params[i].State = tree.CursorClosed
			break
		}
	}
}

// BepiCursorCloseGo use to Cursor Close
func (r *UDRRunParam) BepiCursorCloseGo(cursorName *string) {

	cursorKey, cursorDescZNBase, err2 := FindCursor(r.ExecCfg, r.EvalCtx, *cursorName)
	if err2 != nil {
		fmt.Print(err2)
	}
	oldCursorDescZNBase := *cursorDescZNBase

	cursorDescZNBase.State = uint32(tree.CLOSE)
	cursorDescZNBase.TxnState = tree.PENDING
	//if err := ClearCursorDataAndDesc(descKeyPtr, memEngine, cursorDescZNBase); err != nil {
	//	fmt.Print(err)
	//}
	value, err := protoutil.Marshal(cursorDescZNBase)
	if err != nil {
		fmt.Print(err)
	}
	memEngine, err := GetCursorEngine(r.ExecCfg)
	if err != nil {
		fmt.Print(err)
	}

	if err := memEngine.Put(cursorKey, value); err != nil {
		fmt.Print(err)
	}
	AddCursorTxn(r.EvalCtx.curTxnMap, oldCursorDescZNBase, *cursorDescZNBase, *cursorName)
	// assign the cursor state to CURSOR_DECLARED
	for i := len(r.Params) - 1; i >= 0; i-- {
		if r.Params[i].VarName == *cursorName {
			r.Params[i].State = tree.CursorClosed
			break
		}
	}
}

// BepiTryToCloseAllUnClosedCursor TryTo Close AllUnClosed Cursor
//export BepiTryToCloseAllUnClosedCursor
func BepiTryToCloseAllUnClosedCursor(handler C.Handler_t) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	// traverse all params to close unclosed cursor
	if err := CloseAllUnclosedCursors(udrRunParams.ExecCfg, udrRunParams.EvalCtx, udrRunParams.Params); err != nil {
		return ReportErrorToPlsql(err)
	}
	return ReportErrorToPlsql("")
}

// BepiCastValueToType use to cast cursorvalue
//export BepiCastValueToType
func BepiCastValueToType(
	handler C.Handler_t, valStr *C.char, resStr **C.char, srcOid C.int, castOid C.int,
) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}

	// first, parse value string to datum
	beforeStr := C.GoString(valStr)
	srcTyp, ok := types.OidToType[oid.Oid(srcOid)]
	if !ok {
		return ReportErrorToPlsql(fmt.Sprintf("Unknown type oid: %q", oid.Oid(srcOid)))
	}
	datum, err := tree.ParseStringAs(srcTyp, beforeStr, &udrRunParams.EvalCtx.EvalContext, false)
	if err != nil {
		return ReportErrorToPlsql(err)
	}

	// second, cast datum to castType
	destTyp, ok := types.OidToType[oid.Oid(castOid)]
	if !ok {
		return ReportErrorToPlsql(fmt.Sprintf("Unknown type oid: %q", oid.Oid(castOid)))
	}
	castTyp, err := coltypes.DatumTypeToColumnType(destTyp)
	if err != nil {
		return ReportErrorToPlsql(err)
	}

	// finally, exec cast
	newDatum, err := perforCastWithoutNull(&udrRunParams.EvalCtx.EvalContext, datum, castTyp)
	if err != nil {
		return ReportErrorToPlsql(err)
	}

	// return datum string after cast
	*resStr = C.CString(newDatum.String())
	return C.CString("")
}

// BepiCastValueToTypeGo use to CastValue To Type
func (r *UDRRunParam) BepiCastValueToTypeGo(
	valStr *string, resStr **string, srcOid int, castOid int,
) error {

	// first, parse value string to datum

	srcTyp, ok := types.OidToType[oid.Oid(srcOid)]
	if !ok {
		return errors.New("BepiCastValueToTypeGo faults")
	}
	datum, err := tree.ParseStringAs(srcTyp, *valStr, &r.EvalCtx.EvalContext, false)
	if err != nil {
		return err
	}

	// second, cast datum to castType
	destTyp, ok := types.OidToType[oid.Oid(castOid)]
	if !ok {
		return errors.New("BepiCastValueToTypeGo faults")
	}
	castTyp, err := coltypes.DatumTypeToColumnType(destTyp)
	if err != nil {
		return err
	}

	// finally, exec cast
	newDatum, err := perforCastWithoutNull(&r.EvalCtx.EvalContext, datum, castTyp)
	if err != nil {
		return err
	}

	// return datum string after cast
	**resStr = newDatum.String()
	return err
}

// BepiFindVarDatumByNameToString find udr variable by var_name and return var value's string
//export BepiFindVarDatumByNameToString
func BepiFindVarDatumByNameToString(
	handler C.Handler_t, varName *C.char, varIndex C.int, errMsg *C.bool,
) *C.char {
	*errMsg = true
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		*errMsg = false
		return C.CString(err.Error())
	}
	name := C.GoString(varName)
	index := int(varIndex)
	var result string
	i := len(udrRunParams.Params) - 1
	if index >= 0 && index <= i {
		result = udrRunParams.Params[index].VarDatum.String()
		return C.CString(result)
	}
	for ; i >= 0; i-- {
		if udrRunParams.Params[i].VarName == name {
			if udrRunParams.Params[i].VarDatum != nil {
				result = udrRunParams.Params[i].VarDatum.String()
				return C.CString(result)
			}
			return C.CString("")
		}
	}
	*errMsg = false
	return ReportErrorToPlsql("Can not find Var")
}

// BepiFindVarByName use to Find Var By Name
//export BepiFindVarByName
func BepiFindVarByName(
	handler C.Handler_t, varName *C.char, varIndex C.int, found *C.bool,
) C.ulong {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		*found = C.bool(true)
		return C.ulong(0)
	}
	name := C.GoString(varName)
	index := int(varIndex)
	result := tree.UDRVar{}

	i := len(udrRunParams.Params) - 1
	if index >= 0 && index <= i {
		result = udrRunParams.Params[index]
		*found = C.bool(true)
		return C.ulong(UnsafePointer2Int64(unsafe.Pointer(&result)))
	}

	for ; i >= 0; i-- {
		if udrRunParams.Params[i].VarName == name {
			result = udrRunParams.Params[i]
			*found = C.bool(true)
			break
		}
	}
	return C.ulong(UnsafePointer2Int64(unsafe.Pointer(&result)))
}

// BepiFindVarByNameGo use to Find Var By Name
func (r *UDRRunParam) BepiFindVarByNameGo(varName *string, varIndex int, found *bool) uint {

	index := int(varIndex)
	result := tree.UDRVar{}

	i := len(r.Params) - 1
	if index >= 0 && index <= i {
		result = r.Params[index]
		*found = bool(true)
		return uint(UnsafePointer2Int64(unsafe.Pointer(&result)))
	}

	for ; i >= 0; i-- {
		if r.Params[i].VarName == *varName {
			result = r.Params[i]
			*found = bool(true)
			break
		}
	}
	return uint(UnsafePointer2Int64(unsafe.Pointer(&result)))
}

//StrToColtype convert udrRunParams.FuncDesc.ReturnType (type : string) to ColumnType
func StrToColtype(typeStr string) (sqlbase.ColumnType, error) {
	declareStmt := fmt.Sprintf("declare %s %s", "tempVar", typeStr)
	stmt, err := parser.ParseOne(declareStmt, false)
	if err != nil {
		return sqlbase.ColumnType{}, err
	}
	if stmt.AST != nil {
		colTyp := stmt.AST.(*tree.DeclareVariable).VariableType
		if ts, ok := colTyp.(*coltypes.TSerial); ok {
			colTyp = ts.TInt
		}
		argDatumType := coltypes.CastTargetToDatumType(colTyp)
		argTyp, err := sqlbase.DatumTypeToColumnType(argDatumType)
		if err != nil {
			return sqlbase.ColumnType{}, err
		}
		columnType := sqlbase.ColumnType{}
		columnType, err = sqlbase.PopulateTypeAttrs(argTyp, colTyp)
		if err != nil {
			return sqlbase.ColumnType{}, err
		}
		return columnType, nil
	}
	return sqlbase.ColumnType{}, err

}

// BepiAssignResultToReturn use to Assign Result To Return
//export BepiAssignResultToReturn
func BepiAssignResultToReturn(handler C.Handler_t) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	udrRunParams.Return = udrRunParams.Result[0]
	return C.CString("")
}

// BepiAssignBytesToReturn use to Assign Bytes To Return
//export BepiAssignBytesToReturn
func BepiAssignBytesToReturn(handler C.Handler_t, retStr *C.char, typ C.Oid, length C.int) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	goType := uint(typ)
	switch oid.Oid(goType) {
	case oid.T_bytea:
		udrRunParams.Return = tree.NewDBytes(tree.DBytes(C.GoBytes(unsafe.Pointer(retStr), length)))
	case oid.T_varchar, oid.T_text, oid.T_bit, oid.T_varbit:
		udrRunParams.Return = tree.NewDString(string(tree.DString(C.GoBytes(unsafe.Pointer(retStr), length))))
	}
	return C.CString("")
}

//BepiAssignArrayToReturn used to get convert results from C to Go types. Used only for DArray
//export BepiAssignArrayToReturn
func BepiAssignArrayToReturn(
	handler C.Handler_t, retStr **C.char, typ C.Oid, arraySize C.int, arrayElementLength *C.int,
) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	length := int(arraySize)
	tmpStrslice := (*[1 << 30]*C.char)(unsafe.Pointer(retStr))[:length:length]
	tmpSizeslice := (*[1 << 30]C.int)(unsafe.Pointer(arrayElementLength))[:length:length]
	goStrs := make([]string, length)
	for i := 0; i < length; i++ {
		goStrs[i] = string(C.GoBytes(unsafe.Pointer(tmpStrslice[i]), tmpSizeslice[i]))
	}
	goType := uint(typ)
	switch oid.Oid(goType) {
	case oid.T__int2, oid.T__int4, oid.T__int8:
		intList := tree.NewDArray(types.Int)
		for _, str := range goStrs {
			num, err := strconv.ParseInt(str, 10, 0)
			if err != nil {
				return ReportErrorToPlsql(err)
			}
			value := tree.NewDInt(tree.DInt(num))
			if err := intList.Append(value); err != nil {
				return C.CString(err.Error())
			}
		}
		typ, err := StrToColtype(udrRunParams.FuncDesc.ReturnType)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		returnStr := "Value"
		_, err = sqlbase.LimitValueWidth(typ, intList, &returnStr, true)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = intList
	case oid.T__float4, oid.T__float8:
		floatList := tree.NewDArray(types.Float)
		for _, str := range goStrs {
			num, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return ReportErrorToPlsql(err)
			}
			value := tree.NewDFloat(tree.DFloat(num))
			if err := floatList.Append(value); err != nil {
				return C.CString(err.Error())
			}
		}
		typ, err := StrToColtype(udrRunParams.FuncDesc.ReturnType)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		returnStr := "Value"
		_, err = sqlbase.LimitValueWidth(typ, floatList, &returnStr, true)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = floatList
	case oid.T__text, oid.T__varchar:
		strList := tree.NewDArray(types.String)
		for _, str := range goStrs {
			value := tree.NewDString(str)
			if err := strList.Append(value); err != nil {
				return C.CString(err.Error())
			}
		}
		udrRunParams.Return = strList
	case oid.T__bool:
		boolList := tree.NewDArray(types.Bool)
		for _, str := range goStrs {
			val, err := strconv.ParseBool(str)
			if err != nil {
				return ReportErrorToPlsql(err)
			}
			if err := boolList.Append(tree.MakeDBool(tree.DBool(val))); err != nil {
				return C.CString(err.Error())
			}
		}
		udrRunParams.Return = boolList

	}
	return C.CString("")
}

// BepiAssignStrToReturn use to Assign String To Return
// Note : Do not use this function to assign array params ,use BepiAssignArrayToReturn
//export BepiAssignStrToReturn
func BepiAssignStrToReturn(handler C.Handler_t, retStr *C.char, typ C.Oid, retTyp *C.char) *C.char {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return ReportErrorToPlsql(err)
	}
	goType := uint(typ)
	switch oid.Oid(goType) {
	case oid.T_bool:
		rettype := C.GoString(retTyp)
		if rettype == "" || rettype == "list" || rettype == "dict" || rettype == "tuple" || rettype == "set" {
			vbool, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.Bool)
			if err != nil {
				return ReportErrorToPlsql(err)
			}
			udrRunParams.Return = vbool
		} else {
			if rettype == "str" {
				rettype = "string"
			}
			typ, err := parser.ParseType(rettype)
			if err != nil {
				return ReportErrorToPlsql(err)
			}
			v, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), typ)
			if err != nil {
				return ReportErrorToPlsql(err)
			}
			vbool, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, v, coltypes.Bool)
			if err != nil {
				return ReportErrorToPlsql(err)
			}
			udrRunParams.Return = vbool
		}
	case oid.T_int8, oid.T_int4, oid.T_int2:
		str := C.GoString(retStr)
		num, err := strconv.ParseInt(str, 10, 0)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		value := tree.NewDInt(tree.DInt(num))
		typ, err := StrToColtype(udrRunParams.FuncDesc.ReturnType)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		returnStr := "Return"
		_, err = sqlbase.LimitValueWidth(typ, value, &returnStr, true)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = value
	case oid.T_numeric:
		str := C.GoString(retStr)
		columnTyp, err := StrToColtype(udrRunParams.FuncDesc.ReturnType)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		typ := coltypes.TDecimal{
			Prec:        int(columnTyp.Precision),
			Scale:       int(columnTyp.Width),
			VisibleType: coltypes.VisNONE,
		}
		value, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(str), &typ)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = value
	case oid.T_float4, oid.T_float8:
		str := C.GoString(retStr)
		num, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = tree.NewDFloat(tree.DFloat(num))
	case oid.T_text, oid.T_varchar:
		udrRunParams.Return = tree.NewDString(C.GoString(retStr))
	case oid.T__int8, oid.T__int4, oid.T__int2:
		str := C.GoString(retStr)
		str = strings.Replace(str, "[", "", -1)
		str = strings.Replace(str, "]", "", -1)
		sl := strings.Split(str, ",")
		intList := tree.NewDArray(types.Int)
		for _, v := range sl {
			v = strings.Replace(v, " ", "", -1)
			if num, err := strconv.Atoi(v); err == nil {
				if err := intList.Append(tree.NewDInt(tree.DInt(num))); err != nil {
					return C.CString(err.Error())
				}
			}
		}
		udrRunParams.Return = intList
	case oid.T__float4, oid.T__float8:
		str := C.GoString(retStr)
		str = strings.Replace(str, "[", "", -1)
		str = strings.Replace(str, "]", "", -1)
		sl := strings.Split(str, ",")
		intList := tree.NewDArray(types.Float4)
		for _, v := range sl {
			v = strings.Replace(v, " ", "", -1)
			if num, err := strconv.ParseFloat(v, 64); err == nil {
				if err := intList.Append(tree.NewDFloat(tree.DFloat(num))); err != nil {
					return C.CString(err.Error())
				}
			}
		}
		udrRunParams.Return = intList
	case oid.T__bool:
		str := C.GoString(retStr)
		str = strings.Replace(str, "[", "", -1)
		str = strings.Replace(str, "]", "", -1)
		sl := strings.Split(str, ",")
		intList := tree.NewDArray(types.Bool)
		for _, v := range sl {
			v = strings.Replace(v, " ", "", -1)
			if v == "False" {
				if err := intList.Append(tree.MakeDBool(false)); err != nil {
					return C.CString(err.Error())
				}
			} else {
				if err := intList.Append(tree.MakeDBool(false)); err != nil {
					return C.CString(err.Error())
				}
			}
		}
		udrRunParams.Return = intList
	case oid.T__text, oid.T__varchar:
		str := C.GoString(retStr)
		str = strings.Replace(str, "[", "", -1)
		str = strings.Replace(str, "]", "", -1)
		sl := strings.Split(str, ",")
		strList := tree.NewDArray(types.String)
		for _, v := range sl {
			if err := strList.Append(tree.NewDString(v)); err != nil {
				return C.CString(err.Error())
			}
		}
		udrRunParams.Return = strList
	case oid.T_varbit:
		varbit, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.VarBit)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = varbit
	case oid.T_bit:
		bit, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.Bit)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = bit
	case oid.T_date:
		date, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.Date)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = date
	case oid.T_time:
		time, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.Time)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = time
	case oid.T_timestamp:
		timestamp, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.Timestamp)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = timestamp
	case oid.T_timestamptz:
		tstz, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.TimestampTZ)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = tstz
	case oid.T_interval:
		interval, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.Interval)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = interval
	case oid.T_jsonb, oid.T_json:
		jsonb, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.JSONB)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = jsonb
	case oid.T_uuid:
		uuid, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.UUID)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = uuid
	case oid.T_inet:
		inet, err := tree.PerformCast(&udrRunParams.EvalCtx.EvalContext, tree.NewDString(C.GoString(retStr)), coltypes.INet)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		udrRunParams.Return = inet

	}
	return C.CString("")
}

// BepiGetArgCount use to Get Arg Counts
//export BepiGetArgCount
func BepiGetArgCount(handler C.Handler_t) C.int {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return C.int(0)
	}
	return C.int(len(udrRunParams.Params))
}

// BepiGetArgCountGo get arg count
func (r *UDRRunParam) BepiGetArgCountGo() int {
	return int(len(r.Params))
}

// BepiGetArgTypeID a func use to get arg type
//export BepiGetArgTypeID
func BepiGetArgTypeID(handler C.Handler_t, argIndex C.int) C.uint32 {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return C.uint32(0)
	}
	return C.uint32(udrRunParams.Params[argIndex].VarOid)
}

// BepiGetArgTypeIDGo a func use to get arg typeID
func (r *UDRRunParam) BepiGetArgTypeIDGo(argIndex int) uint32 {
	return uint32(r.Params[argIndex].VarOid)
}

// GetTypeNameFromOid return type name from given type oid
//export GetTypeNameFromOid
func GetTypeNameFromOid(coid C.uint, errMsg **C.char) *C.char {
	goOid := oid.Oid(int(coid))
	typ := types.OidToType[goOid]

	if typ == nil {
		*errMsg = ReportErrorToPlsql("unknown type with oid:" + fmt.Sprint(int(goOid)))
		return C.CString("")
	}
	return C.CString(typ.String())
}

// GetReturnColNum return the col number of result
//export GetReturnColNum
func GetReturnColNum(handler C.Handler_t) C.int {
	udrRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		return C.int(0)
	}
	return C.int(len(udrRunParams.Result))
}

// GetIntFromResult return the int value from udrRunParams.result, now mostly use in exec_stmt_fori
//export GetIntFromResult
func GetIntFromResult(handler C.Handler_t, errmsg **C.char) C.int {
	UDRRunParams, _, err := ConstructUDRRunParamByHandler(handler)
	if err != nil {
		*errmsg = ReportErrorToPlsql(err.Error())
		return C.int(0)
	}
	if len(UDRRunParams.Result) == 0 {
		*errmsg = ReportErrorToPlsql("unknown value")
		return C.int(0)
	}
	retInt := UDRRunParams.Result[0]
	if retInt.ResolvedType() != types.Int {
		retInt, err = perforCastWithoutNull(&UDRRunParams.EvalCtx.EvalContext, retInt, coltypes.Int8)
		if err != nil {
			*errmsg = ReportErrorToPlsql(err.Error())
			return C.int(0)
		}
	}
	return C.int(int(*retInt.(*tree.DInt)))
}

// CheckStmtParse will check a statement from PLSQL and return error string
//export CheckStmtParse
func CheckStmtParse(query *C.char) *C.char {
	goQuery := C.GoString(query)
	stmt, err := parser.ParseOne(goQuery, false)
	if err != nil {
		errStr := tree.ErrPretty(err)
		return C.CString(errStr)
	}
	isShow := false
	// don't support show syntax in procedure or function in plsql
	switch stmt.AST.(type) {
	case *tree.ShowBackup, *tree.ShowColumns, *tree.ShowConstraints, *tree.ShowCreate,
		*tree.ShowClusterSetting, *tree.ShowDatabases, *tree.ShowFingerprints, *tree.ShowRoleGrants,
		*tree.ShowGrants, *tree.ShowHistogram, *tree.ShowIndexes, *tree.ShowJobs, *tree.ShowKafka, *tree.ShowQueries,
		*tree.ShowRanges, *tree.ShowRoles, *tree.ShowSavepointStatus, *tree.ShowSchemas, *tree.ShowSequences,
		*tree.ShowSessions, *tree.ShowSpaces, *tree.ShowTableStats, *tree.ShowSyntax, *tree.ShowTables,
		*tree.ShowTraceForSession, *tree.ShowTransactionStatus, *tree.ShowVar, *tree.ShowUsers,
		*tree.ShowZoneConfig, *tree.ShowSnapshot, *tree.ShowCursor, *tree.ShowFunctions:
		isShow = true
	}
	if isShow {
		return C.CString("can not use SHOW in procedure/function")
	}
	return C.CString("")
}

// UDRCursorDirectionToGoCursorDirection converts "UDR Direction" to "GO Direction"
func UDRCursorDirectionToGoCursorDirection(cDirc int8) tree.CursorDirection {
	var goDirc tree.CursorDirection
	switch cDirc {
	case 0:
		goDirc = tree.FORWARD
	case 1:
		goDirc = tree.BACKWARD
	case 2:
		goDirc = tree.ABSOLUTE
	case 3:
		goDirc = tree.RELATIVE
		// The directions followed are supported in python, not in c
	case 4:
		goDirc = tree.FIRST
	case 5:
		goDirc = tree.LAST
	case 6:
		goDirc = tree.NEXT
	case 7:
		goDirc = tree.PRIOR

	}
	return goDirc
}

// BepiSetFound set param "found" in GO.
//export BepiSetFound
func BepiSetFound(handler C.Handler_t) {
	udrRunParams, _, _ := ConstructUDRRunParamByHandler(handler)
	if udrRunParams.RowAffected > 0 {
		udrRunParams.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolTrue
		udrRunParams.EvalCtx.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolTrue
	} else {
		udrRunParams.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolFalse
		udrRunParams.EvalCtx.Params[udrRunParams.FoundIdx].VarDatum = tree.DBoolFalse
	}
	udrRunParams.RowAffected = 0
}

func checkNotNull(udrVar tree.UDRVar, newValue tree.Datum) error {
	if udrVar.NotNull && newValue == tree.DNull {
		return errors.Errorf("null value cannot be assigned to variable %s declared NOT NULL", udrVar.VarName)
	}
	return nil
}

// BepiCtime2str convert value which type is ctime to string
//export BepiCtime2str
func BepiCtime2str(handler C.Handler_t, ts C.BEPI_TIMESTAMP, valstr **C.char) *C.char {
	year, mon, day, hour, min, sec, off, microsec := int(ts.tm.tm_year), int(ts.tm.tm_mon), int(ts.tm.tm_mday), int(ts.tm.tm_hour), int(ts.tm.tm_min), int(ts.tm.tm_sec), int(ts.tm.tm_gmtoff), int(ts.microseconds)
	year += 1900
	mon++
	str := fmt.Sprintf("%d-%d-%d %d:%d:%d.%d", year, mon, day, hour, min, sec, microsec)
	if off > 0 {
		str += "+"
	} else {
		str += "-"
	}
	str += strconv.Itoa(off/3600) + ":" + strconv.Itoa((off%3600)/60)
	udrRunParams, _, _ := ConstructUDRRunParamByHandler(handler)
	var s string
	switch int(ts.time_type) {
	case 0:
		value, err := tree.ParseStringAs(types.Date, str, &udrRunParams.EvalCtx.EvalContext, false)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		s = value.String()
	case 1:
		value, err := tree.ParseStringAs(types.Time, str, &udrRunParams.EvalCtx.EvalContext, false)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		s = value.String()

	case 2:
		value, err := tree.ParseStringAs(types.Timestamp, str, &udrRunParams.EvalCtx.EvalContext, false)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		s = value.String()

	case 3:
		value, err := tree.ParseStringAs(types.TimestampTZ, str, &udrRunParams.EvalCtx.EvalContext, false)
		if err != nil {
			return ReportErrorToPlsql(err)
		}
		s = value.String()

	}
	if s[0] == '\'' && s[len(s)-1] == '\'' {
		s = s[1 : len(s)-1]
	}
	fmt.Println(s)
	*valstr = C.CString(s)

	return C.CString("")

}
