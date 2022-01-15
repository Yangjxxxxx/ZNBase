package bepi

import (
	"github.com/lib/pq/oid"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

//RunParamInterface the interface of RunParam
type RunParamInterface interface {
	ExecStmt(stmt string) (int, error)
	ExecReturnRows(stmt string) ([]RowDatums, error)
	PrePare(name string, stmt string, argsType ...string) (string, error)
	ExecPrepareWithName(name string, argsVal ...string) error
	BepiPreparePlanGo(queryString string, planIndex *uint) error
	BepiExecPlanGo(planIndex uint) error
	BepiGetArgCountGo() int
	BepiGetArgTypeIDGo(argIndex int) uint32
	BepiFindVarByNameGo(varName *string, varIndex int, found *bool) uint
	BepiCastValueToTypeGo(valStr *string, resStr **string, srcOid int, castOid int) error
	BepiCursorCloseGo(cursorName *string)
	BepiCursorMoveGo(cursorName *string, direction int, moveCount int) bool
	BepiCursorFetchGo(cursorName *string, direction int, rows int) bool
	BepiCursorFindGo(cursorName *string) uint
	BepiCursorDeclareGo(cursorName *string, query *string) int
	BepiExecEvalExprAndStoreInResultGo(planIndex uint) string
	BepiExecEvalExprAndStoreInResultAPIGo(planIndex uint) error
	BepiExecEvalExprGo(planIndex uint) error
	BepiExecBoolExprGo(planIndex uint) bool
	BepiCursorOpenGo(cursorName *string) error
	DeclareVarGo(declareStr string) (string, oid.Oid)
	BepiAssignTheValueGo(paramTarget int, valTarget int, inFetch bool) error
	DeclareDrDBVariableGo(varName *string, t oid.Oid, i int) bool
	BepiGetCusorFetchValueGo() string
}

//Val the value interface
type Val interface {
	IsBepiVal()
}

//RowDatum the datum of Row type
type RowDatum struct {
	Name  string
	Typ   types.T
	Value Val
}

//RowDatums array of RowDatum
type RowDatums []RowDatum

//PInt type
type PInt int64

//IsBepiVal method of interface
func (PInt) IsBepiVal() {}

// func NewPInt(p PInt) *PInt {
// 	return &p
// }

//PFloat type
type PFloat float64

//IsBepiVal method of interface
func (PFloat) IsBepiVal() {}

//PString type
type PString string

//IsBepiVal method of interface
func (PString) IsBepiVal() {}
