package main

import (
	"errors"
	"fmt"
	"github.com/znbasedb/znbase/pkg/udr/bepi"
)

func InsertAnyTimes(runParams bepi.RunParamInterface, times int64) (int, error) {

	affRows := 0
	var err error
	for i := 0; i < int(times); i++ {
		if i%2 == 0 {
			affRows, err = runParams.ExecStmt("insert into t1 values(1000);")
		} else {
			affRows, err = runParams.ExecStmt("insert into t1 values(1001);")
		}
		affRows++
	}
	if err != nil {
		panic(err)
	}
	return affRows, nil
}

func SumTwoTables(runParams bepi.RunParamInterface, tableA string, tableB string) (int, error) {

	result := 0
	var err error
	resultRows, err := runParams.ExecReturnRows("select count(*) from " + tableA + ";")
	if err != nil {
		return -1, err
	}
	fmt.Println(len(resultRows))
	for _, row := range resultRows {
		if len(row) != 1 {
			return -1, nil
		}
		//name := row[0].Name
		val, ok := row[0].Value.(bepi.PInt)
		if !ok {
			return -1, nil
		}
		result += int(val)
	}
	resultRows, err = runParams.ExecReturnRows("select count(*) from " + tableB + ";")
	for _, row := range resultRows {
		if len(row) != 1 {
			return -1, nil
		}
		//name := row[0].Name
		val, ok := row[0].Value.(bepi.PInt)
		if !ok {
			return -1, nil
		}
		result += int(val)
	}
	return result, nil
}

func PrepareAndExec(runParams bepi.RunParamInterface) error {
	var index uint
	index = 0
	varname := "found"
	boo := false
	err := runParams.BepiPreparePlanGo("Insert into t2 values(101);", &index)
	if err != nil {
		return err
	}
	count := runParams.BepiGetArgCountGo()
	oid := runParams.BepiGetArgTypeIDGo(0)
	varadd := runParams.BepiFindVarByNameGo(&varname, 0, &boo)
	res := runParams.BepiExecEvalExprAndStoreInResultGo(index)
	err1 := runParams.BepiExecEvalExprGo(index)
	execbool := runParams.BepiExecBoolExprGo(0)
	fmt.Println("count:", count, "\t oid:", oid, "\t varadd:", varadd, "\t result:", res, "\t err1:", err1, "\t execbool:", execbool)
	err = runParams.BepiExecPlanGo(index)
	return err
}

func InsertValToTable(runParams bepi.RunParamInterface, n string, TableName string) error {
	var index uint
	index = 0
	err := runParams.BepiPreparePlanGo("insert into "+TableName+" values("+n+");", &index)
	if err != nil {
		return err
	}
	res := runParams.BepiExecEvalExprAndStoreInResultGo(index)
	err1 := runParams.BepiExecEvalExprGo(index)
	fmt.Println("\t res:", res, "\t err1:", err1)
	err = runParams.BepiExecPlanGo(index)
	return err
}

//参数计数和参数ID接口测试
//何绍泽
func ArgTest(runParams bepi.RunParamInterface) {

	count := runParams.BepiGetArgCountGo()
	oid := runParams.BepiGetArgTypeIDGo(0)
	fmt.Println("the argcounts : ", count, "the argtypeid: ", oid)
}

func AvgCol(runParams bepi.RunParamInterface, TableName string, ColName string) (bepi.PString, error) {
	varname := "found"
	boo := false
	var err error
	resultRows, err := runParams.ExecReturnRows("select avg(" + ColName + ") from " + TableName + ";")
	varadd := runParams.BepiFindVarByNameGo(&varname, 0, &boo)

	execbool := runParams.BepiExecBoolExprGo(0)
	fmt.Println("varadd:", varadd, "\t execbool:", execbool)
	if err != nil {
		return "-1", err
	}
	for _, row := range resultRows {
		if len(row) != 1 {
			return "-1", nil
		}
		//name := row[0].Name
		val, ok := row[0].Value.(bepi.PString)
		if !ok {
			return "-1", nil
		}
		return val, err
	}
	return "-1", err
}

func FetchCursorIntoVar(runParams bepi.RunParamInterface) error {
	curname := "testcur1"
	querystr := "select a from t3"
	declareres := runParams.BepiCursorDeclareGo(&curname, &querystr)
	if declareres != 1 {
		fmt.Println("cursor has benn declared!")
	}
	err := runParams.BepiCursorOpenGo(&curname)
	if err != nil {
		return errors.New("cursor open default")
	}
	boo := runParams.BepiCursorFetchGo(&curname, 6, 5)
	if !boo {
		return errors.New("cursor fetch default")
	}
	boo = runParams.BepiCursorFetchGo(&curname, 6, 0)
	if !boo {
		return errors.New("cursor fetch default")
	}
	var1, var1oid := runParams.DeclareVarGo("var1 int")
	boo = runParams.DeclareDrDBVariableGo(&var1, var1oid, 2)
	if !boo {
		return errors.New("declared var default")
	}
	err2 := runParams.BepiAssignTheValueGo(2, -1, true)
	if err2 != nil {
		return errors.New("assign the value default")
	}
	boo = runParams.BepiCursorMoveGo(&curname, 6, 2)
	boo = runParams.BepiCursorFetchGo(&curname, 6, 0)
	if !boo {
		return errors.New("cursor move fault")
	}
	runParams.BepiCursorCloseGo(&curname)
	return err
}

func FetchCursorVar(runParams bepi.RunParamInterface) error {
	curname := "testcur2"
	querystr := "select a,b from t4"
	declareres := runParams.BepiCursorDeclareGo(&curname, &querystr)
	if declareres != 1 {
		fmt.Println("cursor has been declared!")
	}
	err := runParams.BepiCursorOpenGo(&curname)
	if err != nil {
		return errors.New("cursor open default")
	}

	val := runParams.BepiGetCusorFetchValueGo()
	fmt.Println("cursor value:", val)
	//runParams.BepiCursorCloseGo(&curname)
	return err
}
