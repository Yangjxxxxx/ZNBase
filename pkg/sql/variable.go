package sql

import (
	"context"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// 作为datakey使用
var variableUniqueID sqlbase.ID = 1

type declareVariableNode struct {
	n *tree.DeclareVariable
}

// implement planNode
func (n *declareVariableNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *declareVariableNode) Values() tree.Datums {
	return tree.Datums{}
}

func (n *declareVariableNode) Close(ctx context.Context) {

}

// DeclareVariable
func (p *planner) DeclareVariable(ctx context.Context, n *tree.DeclareVariable) (planNode, error) {
	node := &declareVariableNode{n: n}
	return node, nil
}

func (n *declareVariableNode) startExec(params runParams) error {
	// cursor temp engine
	memEngine, err := GetCursorEngine(params.ExecCfg())
	if err != nil {
		return err
	}
	//permEngine := cursorEngines[1]
	variableName := n.n.VariableName.String()
	sessionID := params.extendedEvalCtx.CurrentSessionID
	rKey := keys.MakeVariableMetaKey(sessionID.GetBytes(), []byte(variableName))
	key := engine.MakeMVCCMetadataKey(rKey)
	varDesc := sqlbase.VariableDescriptor{}
	ok, _, _, err := memEngine.GetProto(key, &varDesc)
	if err != nil {
		return err
	}
	if ok {
		return errors.Errorf("variable %q has been declared!", varDesc.Name)
	}

	columnTableDef := tree.ColumnTableDef{Name: n.n.VariableName, Type: n.n.VariableType}
	columnTableDef.Nullable.Nullability = tree.SilentNull
	privs := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Column, params.p.User())
	col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, &params.p.semaCtx, privs, variableUniqueID)
	if err != nil {
		return err
	}
	//更新VariableDescriptor信息
	varDesc = sqlbase.VariableDescriptor{
		Name:     variableName,
		ID:       variableUniqueID,
		Column:   *col,
		TxnState: tree.PENDING,
	}
	variableUniqueID++
	value, err := protoutil.Marshal(&varDesc)
	if err != nil {
		return err
	}
	// write meta data to cursor Engine directly
	if err == memEngine.Put(key, value) {
		return err
	}
	return nil
}

type variableAccessNode struct {
	//n       *tree.VariableAccess
	//varDesc sqlbase.VariableDescriptor
}

// implement planNode
func (n *variableAccessNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *variableAccessNode) Values() tree.Datums {
	return tree.Datums{}
}

func (n *variableAccessNode) Close(ctx context.Context) {

}

//
func (p *planner) VariableAccess(ctx context.Context, n *tree.VariableAccess) (planNode, error) {
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return nil, err
	}
	//permEngine := cursorEngines[1]
	variableName := n.VariableName.String()
	sessionID := p.extendedEvalCtx.CurrentSessionID
	rKey := keys.MakeVariableMetaKey(sessionID.GetBytes(), []byte(variableName))
	key := engine.MakeMVCCMetadataKey(rKey)

	varDesc := sqlbase.VariableDescriptor{}
	//获取了变量对应key的元数据信息 填充varDesc
	ok, _, _, err := memEngine.GetProto(key, &varDesc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf("variable %q has not been declared!\n", variableName)
	}

	varDataKey := MakeVariableDataMVCCKey(uint64(varDesc.ID))
	//一个变量只能有一种类型，desiredTypes中只有一个类型
	desiredTypes := make([]types.T, 0)
	valNode := &valuesNode{}
	desiredTypes = append(desiredTypes, varDesc.Column.DatumType())
	valStmt := tree.ValuesClause{
		Rows: make([]tree.Exprs, 0),
	}

	varDataValue, err := memEngine.Get(varDataKey)
	if err != nil {
		return nil, err
	}
	rowTup := make([]tree.Expr, 0)
	var datum tree.Datum
	datum, varDataValue, err = sqlbase.DecodeTableValue(&sqlbase.DatumAlloc{}, varDesc.Column.DatumType(), varDataValue)
	if err != nil {
		return nil, err
	}
	rowTup = append(rowTup, datum)
	valStmt.Rows = append(valStmt.Rows, rowTup)
	//把包含变量值的结果集、变量类型 封在一个planNode中
	node, err := p.Values(ctx, &valStmt, desiredTypes)
	if err != nil {
		return nil, err
	}
	//node转为显示结果的valueNode
	valNode, ok = node.(*valuesNode)
	if !ok {
		return nil, errors.Errorf("canot construct a value node")
	}
	if len(valNode.tuples) == 0 {
		columns := make([]sqlbase.ColumnDescriptor, 1)
		columns = append(columns, varDesc.Column)
		valNode.columns = sqlbase.ResultColumnsFromColDescs(columns)
	}
	// fill back column name
	valNode.columns[0].Name = varDesc.Column.Name
	return valNode, nil
}

func (n *variableAccessNode) startExec(params runParams) error {
	return nil
}

// MakeVariableDataMVCCKey make Variable Data MVCC Key by Variable.ID.
func MakeVariableDataMVCCKey(id uint64) engine.MVCCKey {
	varIDByte := encoding.EncodeUvarintAscending([]byte{}, id)
	varDataKey := keys.MakeVariableDataKey(varIDByte)
	varDataMVCCkey := engine.MakeMVCCMetadataKey(varDataKey)
	return varDataMVCCkey
}

func (p *planner) CommitVariables() error {
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return err
	}
	sessionID := p.extendedEvalCtx.CurrentSessionID

	varRStartKey := keys.MakeVariableMetaKey(sessionID.GetBytes(), []byte{})
	varREndKey := varRStartKey.PrefixEnd()
	varStartKey := engine.MakeMVCCMetadataKey(varRStartKey)
	varEndKey := engine.MakeMVCCMetadataKey(varREndKey)
	// iterator func
	funcRollBackVar := func(keyVal engine.MVCCKeyValue) (stop bool, err error) {
		key := keyVal.Key
		varDesc := sqlbase.VariableDescriptor{}
		ok, _, _, err := memEngine.GetProto(key, &varDesc)
		if ok {
			// variable DataKey
			if varDesc.TxnState != tree.COMMIT {
				varDesc.TxnState = tree.COMMIT
				varDescValue, err := protoutil.Marshal(&varDesc)
				if err != nil {
					return true, errors.Errorf("变量元数据编码失败! 游标名：%s\n", varDesc.Name)
				}
				// write meta data to cursor Engine directly
				if err := memEngine.Put(key, varDescValue); err != nil {
					return stop, errors.Errorf("保存变量元数据失败! 游标名：%s\n", varDesc.Name)
				}
			}
		}
		return false, nil
	}
	if err := memEngine.Iterate(varStartKey, varEndKey, funcRollBackVar); err != nil {
		return errors.Errorf("迭代 变量key失败\n")
	}
	return nil
}

func (p *planner) RollBackVariables() error {
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return err
	}
	sessionID := p.extendedEvalCtx.CurrentSessionID

	varRStartKey := keys.MakeVariableMetaKey(sessionID.GetBytes(), []byte{})
	varREndKey := varRStartKey.PrefixEnd()
	varStartKey := engine.MakeMVCCMetadataKey(varRStartKey)
	varEndKey := engine.MakeMVCCMetadataKey(varREndKey)
	// iterator func
	funcRollBackVar := func(keyVal engine.MVCCKeyValue) (stop bool, err error) {
		key := keyVal.Key
		varDesc := sqlbase.VariableDescriptor{}
		ok, _, _, err := memEngine.GetProto(key, &varDesc)
		if ok {
			// variable DataKey
			if varDesc.TxnState != tree.COMMIT {
				varKey := MakeVariableDataMVCCKey(uint64(varDesc.ID))
				if err := memEngine.SingleClear(varKey); err != nil {
					return true, errors.Errorf("回滚变量失败 变量名：%s\n", varDesc.Name)
				}
				if err := memEngine.SingleClear(key); err != nil {
					return true, errors.Errorf("回滚变量元数据失败 变量名：%s\n", varDesc.Name)
				}
			}
		}
		return false, nil
	}
	if err := memEngine.Iterate(varStartKey, varEndKey, funcRollBackVar); err != nil {
		return errors.Errorf("迭代 变量key失败\n")
	}
	return nil
}
