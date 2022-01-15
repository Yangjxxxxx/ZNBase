package sql

import (
	"context"
	"fmt"
	"plugin"
	"reflect"
	"strings"
	"unsafe"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type callStmtNode struct {
	n *tree.CallStmt
	// columns is set if this procedure with INOUT parameters is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is any INOUT parameter with some scalar expressions.
	columns sqlbase.ResultColumns

	rows          tree.Datums
	FuncDesc      sqlbase.FunctionDescriptor
	InoutFuncArgs []sqlbase.FuncArgDescriptor

	// rowCount is set either to the total row count if fastPath is true,
	// or to the row count of the current batch otherwise.
	// for call statement, the row count is always 1, now procedure can not return rows more than 1
	rowCount int

	// rowIdx is the index of the current row in the current batch.
	rowIdx int
}

func (p *planner) CallStmt(ctx context.Context, n *tree.CallStmt) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support user define function now!")
	}
	if n.FuncName.String() == "" {
		return nil, pgerror.NewError(pgcode.Syntax, "empty function name")
	}

	unresolvedFuncName, ok := n.FuncName.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.Errorf("assert function name %s to UnresolvedName failed.", n.FuncName.String())
	}
	funcName := tree.MakeTableNameFromUnresolvedName(unresolvedFuncName)

	// function descriptor group, a list of function with same function name and same schema
	funcName.DefinedAsFunction()
	desc, err := resolveExistingObjectImpl(context.Background(), p, &funcName, true, false, requireFunctionDesc)
	if err != nil {
		if pgErr, ok := err.(*pgerror.Error); ok && pgErr.Code == pgcode.UndefinedFunction {
			pgErr.Message = "No function/procedure matches the given name and argument types."
			return nil, pgErr
		}
		return nil, err
	}
	funcDescGroup, ok := desc.(*sqlbase.ImmutableFunctionDescriptor)
	if !ok {
		return nil, sqlbase.NewWrongObjectTypeError(&funcName, requiredTypeNames[requireFunctionDesc])
	}

	n.FixedFunctionName = funcName

	_, funcDescOverload, err := getFuncDescInOverloads(p, n, funcDescGroup)
	if err != nil {
		return nil, err
	}
	if err := p.checkPrivilegeAccessToTable(ctx, funcDescOverload); err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, funcDescOverload, privilege.EXECUTE); err != nil {
		return nil, err
	}
	if !funcDescOverload.IsProcedure {
		return nil, errors.Errorf("%s is not a procedure, to call a function, use SELECT", funcDescOverload.FullFuncName)
	}

	inOutArgDescs := getInoutFuncArgs(funcDescOverload.Args)
	var columns sqlbase.ResultColumns
	if len(inOutArgDescs) > 0 {
		columns = sqlbase.ResultColumnsFromFuncInoutArgDescs(funcDescOverload.Args)
	}

	return &callStmtNode{n: n, FuncDesc: *funcDescOverload, InoutFuncArgs: inOutArgDescs, columns: columns, rowCount: 1, rowIdx: 0}, nil
}

//DatumToReflectValue ...(datum tree.Datum) reflect.Value
func DatumToReflectValue(datum tree.Datum) reflect.Value {
	var value interface{}
	switch datum.(type) {
	case *tree.DInt:
		value = int64(*datum.(*tree.DInt))
	case *tree.DString:
		value = string(*datum.(*tree.DString))
	case *tree.DFloat:
		value = float64(*datum.(*tree.DFloat))
	case *tree.DBool:
		value = bool(*datum.(*tree.DBool))
	case *tree.DBytes:
		value = string(*datum.(*tree.DBytes))
	case *tree.DDate:
		value = int64(*datum.(*tree.DDate))
	case *tree.DArray:
		switch datum.(*tree.DArray).ParamTyp.Oid() {
		case oid.T_int2, oid.T_int4, oid.T_int8:
			values := make([]int64, 0)
			for _, v := range datum.(*tree.DArray).Array {
				values = append(values, int64(*v.(*tree.DInt)))
			}
			value = values
		case oid.T_float4, oid.T_float8:
			values := make([]float64, 0)
			for _, v := range datum.(*tree.DArray).Array {
				values = append(values, float64(*v.(*tree.DFloat)))
			}
			value = values
		case oid.T_text, oid.T_varchar:
			values := make([]string, 0)
			for _, v := range datum.(*tree.DArray).Array {
				values = append(values, string(*v.(*tree.DString)))
			}
			value = values
		case oid.T_bool:
			values := make([]bool, 0)
			for _, v := range datum.(*tree.DArray).Array {
				values = append(values, bool(*v.(*tree.DBool)))
			}
			value = values
		case oid.T_date:
			values := make([]int64, 0)
			for _, v := range datum.(*tree.DArray).Array {
				values = append(values, int64(*v.(*tree.DDate)))
			}
			value = values
		default:
		}
	default:

	}
	return reflect.ValueOf(value)
}

//ReflectValueToDatum ...
func ReflectValueToDatum(value reflect.Value, typOid oid.Oid) (tree.Datum, error) {
	fmt.Println(value.Type())
	switch value.Kind() {
	//TODO
	case reflect.Slice, reflect.Array:
		switch typOid {
		case oid.T__int2, oid.T__int4, oid.T__int8:
			result := tree.NewDArray(types.Int4)
			for i := 0; i < value.Len(); i++ {
				if err := result.Append(tree.NewDInt(tree.DInt(value.Index(i).Int()))); err != nil {
					return nil, err
				}
			}
			return result, nil
		case oid.T__float4, oid.T__float8:
			result := tree.NewDArray(types.Float4)
			for i := 0; i < value.Len(); i++ {
				if err := result.Append(tree.NewDFloat(tree.DFloat(value.Index(i).Float()))); err != nil {
					return nil, err
				}
			}
			return result, nil
		case oid.T__text, oid.T__varchar:
			result := tree.NewDArray(types.String)
			for i := 0; i < value.Len(); i++ {
				if err := result.Append(tree.NewDString(string(value.Index(i).Bytes()))); err != nil {
					return nil, err
				}
			}
			return result, nil
		case oid.T__bool:
			result := tree.NewDArray(types.Bool)
			for i := 0; i < value.Len(); i++ {
				if err := result.Append(tree.MakeDBool(tree.DBool(value.Index(i).Bool()))); err != nil {
					return nil, err
				}
			}
			return result, nil
		case oid.T__date:
			result := tree.NewDArray(types.Int4)
			for i := 0; i < value.Len(); i++ {
				if err := result.Append(tree.NewDDate(tree.DDate(value.Index(i).Int()))); err != nil {
					return nil, err
				}
			}
			return result, nil
		default:
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return tree.NewDInt(tree.DInt(value.Int())), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return tree.NewDInt(tree.DInt(value.Uint())), nil
	case reflect.Float32, reflect.Float64:
		return tree.NewDFloat(tree.DFloat(value.Float())), nil
	case reflect.Bool:
		return tree.MakeDBool(tree.DBool(value.Bool())), nil
	case reflect.String:
		return tree.NewDString(string(value.Bytes())), nil
	default:

	}

	return nil, nil
}

func (n *callStmtNode) startExec(params runParams) (err error) {
	r := MakeUDRRunParam(params.ctx, params.p.txn, params.ExecCfg(), params.extendedEvalCtx, &n.FuncDesc)
	r.SelectCount = r.EvalCtx.SelectCount + 1
	if r.SelectCount > tree.MaxFuncSelectNum {
		return errors.New("user defined function stack depth limit exceeded, max depth" + string(rune(tree.MaxFuncSelectNum)))
	}
	for i, arg := range n.n.Args {
		typOid := n.FuncDesc.Args[i].Oid
		argTyp := types.OidToType[typOid]

		typStr := n.FuncDesc.Args[i].ColumnTypeString
		fakeDeclareStmt := fmt.Sprintf("declare fakeVar %s", typStr)
		declareStmt, decErr := parser.ParseOne(fakeDeclareStmt, params.EvalContext().GetCaseSensitive())
		if decErr != nil {
			return decErr
		}
		// UDR-TODO: 强转加保护
		varColType := declareStmt.AST.(*tree.DeclareVariable).VariableType

		typeExp, err := arg.TypeCheck(&params.p.semaCtx, types.UnwrapType(argTyp), true)
		if err != nil {
			return err
		}
		datum, err := typeExp.Eval(params.p.EvalContext())
		if err != nil {
			return err
		}
		outVal, err := sqlbase.LimitValueWidth(n.FuncDesc.Args[i].Type, datum, &n.FuncDesc.Args[i].Name, true)
		if err != nil {
			return err
		}
		tempArg := &tree.UDRVar{}
		tempArg.VarDatum = outVal
		tempArg.VarOid = typOid
		tempArg.ColType = varColType
		tempArg.VarName = n.FuncDesc.Args[i].Name
		r.Params = append(r.Params, *tempArg)
	}
	switch n.FuncDesc.Language {
	case "plsql", "plpython", "plclang":
		pl := NewPLSQL(unsafe.Pointer(&r), r.MagicCode)
		if err := pl.CallPlpgSQLHandler(params.ctx, CallFunction, unsafe.Pointer(&n.FuncDesc)); err != nil {
			// UDR-TODO: 是否会引发内存异常？
			params.extendedEvalCtx.Mon.SetUdrState()
			params.p.extendedEvalCtx.Mon.SetUdrState()
			return err
		}

	case "plgolang":
		// add variable "FOUND" into udr params
		tempArg := &tree.UDRVar{}
		tempArg.VarDatum = tree.DBoolTrue
		tempArg.VarOid = oid.T_bool
		tempArg.VarName = "found"
		r.Params = append(r.Params, *tempArg)
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("%s", p)
			}
		}()
		scriptPath := n.FuncDesc.FilePath
		funcName := n.FuncDesc.Name
		plug, err := plugin.Open(scriptPath)
		if err != nil {
			return err
		}
		foundFunction, err := plug.Lookup(funcName)
		if err != nil {
			return err
		}
		FuncImpl := reflect.ValueOf(foundFunction)
		argsNum := n.FuncDesc.ArgNum
		argsDatum := make([]reflect.Value, 0)
		argsDatum = append(argsDatum, reflect.ValueOf(&r))
		for i := 0; i < int(argsNum); i++ {
			datum := r.Params[i].VarDatum
			// UDR-TODO: 类型是否完备， 不足的如何处理?
			value := DatumToReflectValue(datum)
			argsDatum = append(argsDatum, value)
		}
		resultValues := FuncImpl.Call(argsDatum)
		// UDR-TODO: 返回值如何处理？
		// 需加强测试和自测
		for i := 0; i < len(resultValues); i++ {
			switch resultValues[i].Type() {
			case reflect.TypeOf(int(0)):
				fmt.Println("result: ", i, ", ", resultValues[i].Interface().(int))
			case reflect.TypeOf(string("")):
				fmt.Println("result: ", i, ", ", resultValues[i].Interface().(string))
			default:
				fmt.Printf("type: %s[%s], value: %v \n", resultValues[i].Type().Kind(), resultValues[i].Type().Name(), resultValues[i].Interface())
			}
		}
	default:
		// UDR-TODO: 报错有问题，call 里面不能created?
		return errors.Errorf("Specified pl lang(%s) can not be created!", n.FuncDesc.Language)
	}
	// have out parameters
	if len(n.InoutFuncArgs) > 0 {
		rowTup := make(tree.Datums, 0)
		for _, outArg := range n.InoutFuncArgs {
			// 不要试图用名字找，用序号，此处能保证序号不会超过下标，所以不再进行长度的判断
			// 临时变量避免可能的for range问题
			tempArg := outArg
			rowTup = append(rowTup, r.Params[tempArg.ID-1].VarDatum)
		}
		n.rows = rowTup
	}
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogCallFunction),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(n.FuncDesc.ID),
			Desc: struct {
				name string
			}{
				name: n.FuncDesc.Name,
			},
		},
		Info: "SHOW SYNTAX: " + params.p.stmt.SQL,
	}
	return nil
}

func (n *callStmtNode) Next(runParams) (bool, error) {
	if n.rowIdx < n.rowCount {
		n.rowIdx++
		return true, nil
	}
	return false, nil
}

func (n *callStmtNode) Values() tree.Datums       { return n.rows }
func (n *callStmtNode) Close(ctx context.Context) {}

// generator overloads from all function descriptor with funcName and parentId
func fromAllDescGetOverloads(
	allFuncs []*FunctionDescriptor, funcName string, parentID sqlbase.ID,
) ([]tree.OverloadImpl, []int) {
	overLoads := make([]tree.OverloadImpl, 0)
	acturalFuncs := make([]int, 0)
	for i, funcDesc := range allFuncs {
		if funcDesc.Name == funcName && funcDesc.ParentID == parentID {
			tempOverload := &tree.Overload{
				Info: funcDesc.FuncDef,
			}
			var argTypes tree.ArgTypes
			for _, arg := range funcDesc.Args {
				argType := tree.ArgType{Name: arg.Name, Typ: arg.Type.ToDatumType()}
				argTypes = append(argTypes, argType)
			}
			tempOverload.Types = argTypes
			overLoads = append(overLoads, tempOverload)
			acturalFuncs = append(acturalFuncs, i)
		}
	}
	return overLoads, acturalFuncs
}

func getFuncDescInOverloads(
	p *planner, n *tree.CallStmt, funcGroup *sqlbase.ImmutableFunctionDescriptor,
) (*tree.Overload, *FunctionDescriptor, error) {
	allFuncDescs := make([]*FunctionDescriptor, 0)
	for _, f := range funcGroup.FuncGroup {
		allFuncDescs = append(allFuncDescs, f.Desc)
	}
	overloads, funcIdxs := fromAllDescGetOverloads(allFuncDescs, n.FixedFunctionName.Table(), funcGroup.FunctionParentID)
	typedExprs, fns, overloadIdxs, err2 := n.ChooseFunctionInOverload(&p.semaCtx, types.Any, overloads)
	if err2 != nil {
		return nil, nil, err2
	}
	if len(fns) != 1 {
		typeNames := make([]string, 0, len(n.Args))
		for _, expr := range typedExprs {
			typeNames = append(typeNames, expr.ResolvedType().String())
		}
		var desStr string
		sig := fmt.Sprintf("%s(%s)%s", n.FixedFunctionName.Table(), strings.Join(typeNames, ", "), desStr)
		if len(fns) == 0 {
			return nil, nil, pgerror.NewErrorf(pgcode.UndefinedFunction, "No function/procedure matches the given name and argument types.")
		}
		fnsStr := tree.FormatCandidates(n.FixedFunctionName.Table(), fns, true)
		return nil, nil, pgerror.NewErrorf(pgcode.AmbiguousFunction, "ambiguous call: %s, candidates are:\n%s", sig, fnsStr)
	}
	overloadImpl := fns[0].(*tree.Overload)
	resFuncDesc := allFuncDescs[funcIdxs[overloadIdxs[0]]]
	return overloadImpl, resFuncDesc, nil
}

// getInoutFuncArgs get INOUT parameters from funcArgDescriptors
func getInoutFuncArgs(funcArgs []sqlbase.FuncArgDescriptor) []sqlbase.FuncArgDescriptor {
	outFuncArgs := make([]sqlbase.FuncArgDescriptor, 0)
	for _, funcArg := range funcArgs {
		if funcArg.InOutMode == "inout" || funcArg.InOutMode == "out" {
			tempFuncArg := funcArg
			outFuncArgs = append(outFuncArgs, tempFuncArg)
		}
	}
	return outFuncArgs
}
