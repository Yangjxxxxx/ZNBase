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

var cursorUniqueID sqlbase.ID = 1

// CurVarDesc represents the assignment status
type CurVarDesc struct {
	desc  sqlbase.VariableDescriptor
	state bool
}

// CurTxn represents cursor txn state.
type CurTxn struct {
	txnDescs []sqlbase.CursorDescriptor
}

type declareCursorNode struct {
	n *tree.DeclareCursor
	//sourcePlan planNode
}

func (n *declareCursorNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *declareCursorNode) Values() tree.Datums {
	return tree.Datums{}
}

func (n *declareCursorNode) Close(ctx context.Context) {

}

// DeclareCursor will construct a declareCursorNode
func (p *planner) DeclareCursor(ctx context.Context, n *tree.DeclareCursor) (planNode, error) {
	// Construct createTable statement with cursor name
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return nil, err
	}
	//permEngine := cursorEngines[1]
	cursorName := n.Cursor.String()
	sessionID := p.extendedEvalCtx.CurrentSessionID
	rKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte(cursorName))
	descKey := engine.MakeMVCCMetadataKey(rKey)
	curDesc := sqlbase.CursorDescriptor{}
	ok, _, _, err := memEngine.GetProto(descKey, &curDesc)
	if err != nil {
		return nil, err
	}
	if ok && curDesc.State != uint32(tree.CLOSE) {
		return nil, errors.Errorf("%s has been declared!", curDesc.Name)
	}
	node := &declareCursorNode{n: n}
	return node, nil
}

// DeclareCursor will record meta data of this cursor in cursor store engine
// cursor meta data as below
//   key : /Meta/Session/CursorName
//   Value: cusror Desc
func (n *declareCursorNode) startExec(params runParams) error {
	// cursor temp engine
	memEngine, err := GetCursorEngine(params.ExecCfg())
	if err != nil {
		return err
	}
	//permEngine := cursorEngines[1]
	cursorName := n.n.Cursor.String()
	sessionID := params.extendedEvalCtx.CurrentSessionID
	rKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte(cursorName))
	descKey := engine.MakeMVCCMetadataKey(rKey)
	curDesc := sqlbase.CursorDescriptor{}

	// 带参数游标，存参数
	var curVarDescs []sqlbase.VariableDescriptor
	if n.n.CursorVars != nil {
		var cursorVarUniqueID sqlbase.ID = 1
		curVarDesc := sqlbase.VariableDescriptor{}
		//var curVarMVCCMetaKey engine.MVCCKey
		for _, curVar := range n.n.CursorVars {
			curVarName := curVar.CurVarName.String()
			//curVarMVCCMetaKey = MakeCurVarMVCCMetaKey(cursorName,curVarName)

			columnTableDef := tree.ColumnTableDef{Name: curVar.CurVarName, Type: curVar.CurVarColType}
			columnTableDef.Nullable.Nullability = tree.SilentNull
			privs := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Column, params.p.User())
			curVarColDesc, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, &params.p.semaCtx, privs, cursorVarUniqueID)
			if err != nil {
				return err
			}

			curVarDesc = sqlbase.VariableDescriptor{
				Name:   curVarName,
				ID:     cursorVarUniqueID,
				Column: *curVarColDesc,
			}
			curVarDescs = append(curVarDescs, curVarDesc)
		}
		curDesc.CurVarDescriptors = curVarDescs

		//

	}
	//
	if n.n.Select != nil && n.n.CursorVars != nil {
		curDesc = sqlbase.CursorDescriptor{
			Name:              cursorName,
			ID:                cursorUniqueID,
			QueryString:       n.n.Select.String(),
			Position:          0,
			Columns:           nil,
			CurVarDescriptors: curVarDescs,
			TxnState:          tree.PENDING,
		}
	} else if n.n.Select != nil && n.n.CursorVars == nil {
		curDesc = sqlbase.CursorDescriptor{
			Name:              cursorName,
			ID:                cursorUniqueID,
			QueryString:       n.n.Select.String(),
			Position:          0,
			Columns:           nil,
			CurVarDescriptors: nil,
			TxnState:          tree.PENDING,
		}
	} else {
		curDesc = sqlbase.CursorDescriptor{
			Name:              cursorName,
			ID:                cursorUniqueID,
			QueryString:       "",
			Position:          0,
			Columns:           nil,
			CurVarDescriptors: nil,
			TxnState:          tree.PENDING,
		}
	}
	//cursorID全局变量+1
	cursorUniqueID++
	value, err := protoutil.Marshal(&curDesc)
	if err != nil {
		return err
	}
	// write meta data to cursor Engine directly
	err = memEngine.Put(descKey, value)
	if err != nil {
		return err
	}

	// txn
	AddCursorTxn(params.extendedEvalCtx.curTxnMap, sqlbase.CursorDescriptor{TxnState: tree.COMMIT}, curDesc, cursorName)
	return nil
}

type cursorAccessNode struct {
	n       *tree.CursorAccess
	oldDesc sqlbase.CursorDescriptor
	desc    sqlbase.CursorDescriptor
	// sourcePlan is a valuesNode used for fetch this cursor
	// and in open action it stand for query node
	sourcePlan planNode
}

func (n *cursorAccessNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (n *cursorAccessNode) Values() tree.Datums {
	return tree.Datums{}
}

func (n *cursorAccessNode) Close(ctx context.Context) {

}

// CursorAccess will return a cursorAccessNode which will do action about this cursor
func (p *planner) CursorAccess(ctx context.Context, n *tree.CursorAccess) (planNode, error) {
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return nil, err
	}
	//permEngine := cursorEngines[1]
	cursorName := n.CursorName.String()
	descKey, desc, err := FindCursor(p.ExecCfg(), p.ExtendedEvalContext(), cursorName)
	if err != nil {
		return nil, err
	}
	if desc.State == uint32(tree.CLOSE) {
		return nil, errors.Errorf("%q has not been declared!\n", cursorName)
	}
	oldDesc := *desc

	node := &cursorAccessNode{
		n:    n,
		desc: *desc,
	}
	switch n.Action {
	case tree.OPEN:
		if desc.State != uint32(tree.DECLARE) {
			return nil, errors.Errorf("Cannot open a cursor that is already open!\n%s", cursorName)
		}

		//确保 queryString存在，用于后续拿到stmt
		//防止 打开未绑定select语句的游标
		if desc.QueryString == "" {
			if n.Select == nil {
				return nil, errors.Errorf("\"%s\" unbound query statement!\n", cursorName)
			}
			queryString := n.Select.String()
			desc.QueryString = queryString
		} else {
			if n.Select != nil {
				return nil, errors.Errorf("\"%s\" has bound query statement!\n", cursorName)
			}
		}
		//打开带参数游标，为变量赋值
		if desc.CurVarDescriptors != nil {
			if n.CurVars == nil {
				return nil, errors.Errorf("\"%s\" needs parameters!\n", cursorName)
			}
			// 初始化列解析时候用的map
			p.nameResolutionVisitor.CurVarMap = make(map[string]*tree.CursorVar)
			// 用于 顺序赋值
			curVarArray := make([]*CurVarDesc, 0)
			// 用于 变量名赋值
			curVarDescMap := make(map[string]*CurVarDesc)

			for _, desc := range desc.CurVarDescriptors {
				curVarName := desc.Name
				curVarDesc := &CurVarDesc{desc: desc, state: false}
				curVarDescMap[curVarName] = curVarDesc
				curVarArray = append(curVarArray, curVarDesc)
			}
			// open语句中给游标参数赋值了
			err := AssignValToCursorParam(p.EvalContext(), &p.semaCtx, n, desc, curVarDescMap, curVarArray, p.nameResolutionVisitor.CurVarMap)
			if err != nil {
				return nil, err
			}
			// 改变InCursor开关状态，对游标查询sql进行解析时候可以去CurVarMap中寻找变量.
			// 使用位置: pkg/sql/sqlbase/select_name_resolution.go
			p.nameResolutionVisitor.InCursor = true
		} else {
			if n.CurVars != nil {
				return nil, errors.Errorf("\"%s\" is not a cursor with parameters!\n", cursorName)
			}
		}

		//以下步骤用于构造node的sourcePlan
		sourcePlan, hideColumns, err := ParseCursor(ctx, p, desc)
		p.nameResolutionVisitor.InCursor = false
		p.nameResolutionVisitor.CurVarMap = nil
		if err != nil {
			return nil, err
		}
		node.sourcePlan = sourcePlan
		//根据sourcePlan得到ColumnDescriptor，填入游标的desc
		columnDescs, err := GetCursorMetaDataColumnDescs(p, sourcePlan, hideColumns, desc.ID)
		if err != nil {
			return nil, err
		}
		desc.Columns = columnDescs
		desc.State = uint32(tree.OPEN)
		desc.TxnState = tree.PENDING
		node.oldDesc = oldDesc
		node.desc = *desc
		return node, nil
	case tree.FETCH:
		if desc.State == uint32(tree.DECLARE) {
			return nil, errors.Errorf("Cannot fetch a cursor that has not been opened!\n%s", cursorName)
		}
		//FETCH INTO VARIABLE
		if n.VariableNames != nil {
			if (n.Direction == tree.FORWARD || n.Direction == tree.BACKWARD) && (n.Rows < -1 || n.Rows > 1) {
				return nil, errors.Errorf("Unable to store more than one piece of data into a variable!")
			}
			startPosition, endPosition := GetFetchActionStartpositionAndEndposition(n, desc)
			startKey, _ := MakeStartKeyAndEndKey(desc, startPosition, endPosition)
			bytes, err := memEngine.Get(startKey)
			if err != nil {
				return nil, err
			}
			datums := make(tree.Datums, 0)
			colTypes := make([]string, 0)
			columnCount := 0
			for _, column := range desc.Columns {
				if !column.Hidden {
					columnCount++
				}
			}
			if columnCount != len(n.VariableNames) {
				return nil, errors.Errorf("The number of variables does not match the number of cursor columns!")
			}
			// 根据每一列的类型，解出每一列的值，扔到rowTup中组成一条记录（row）
			for _, columnDesc := range desc.Columns {
				var datum tree.Datum
				//返回解码后的值，和切割后的value值
				datum, bytes, err = sqlbase.DecodeTableValue(&sqlbase.DatumAlloc{}, columnDesc.DatumType(), bytes)
				if err != nil {
					return nil, err
				}
				if columnDesc.Hidden {
					continue
				}
				datums = append(datums, datum)
				colTypes = append(colTypes, columnDesc.Type.SemanticType.String())
			}
			// 循环遍历要存入数据的变量，进行类型匹配检查和赋值
			for i, variableName := range n.VariableNames {
				//获取variable元数据信息
				varName := variableName.String()
				sessionID := p.extendedEvalCtx.CurrentSessionID
				rKey := keys.MakeVariableMetaKey(sessionID.GetBytes(), []byte(varName))
				varKey := engine.MakeMVCCMetadataKey(rKey)
				varDesc := sqlbase.VariableDescriptor{}
				//获取了var对应key的元数据信息 填充varDesc
				ok, _, _, err := memEngine.GetProto(varKey, &varDesc)
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, errors.Errorf("Variable %s has not been declared!\n", varName)
				}
				// 对比列类型赋值是否可行
				varType := varDesc.Column.Type.SemanticType.String()
				if varType != colTypes[i] {
					return nil, errors.Errorf("Column type mismatch!\n +"+
						"%s's column type should be \"%s\" instead of \"%s\".\n", varName, varType, colTypes[i])
				}
				// Store the Encoded datum into variable
				varBytes := []byte{}
				varBytes, err = sqlbase.EncodeTableValue(varBytes, varDesc.Column.ID, datums[i], nil)
				varDataMVCCKey := MakeVariableDataMVCCKey(uint64(varDesc.ID))
				if err := memEngine.Put(varDataMVCCKey, varBytes); err != nil {
					return nil, errors.Errorf(err.Error() + "\nFailed to store variable data!")
				}
			}
			desc.State = uint32(tree.FETCH)
			desc.TxnState = tree.PENDING
			node.oldDesc = oldDesc
			node.desc = *desc
			return node, nil
		}
		// FETCH INTO END

		// FETCH数据到Client
		visibleDescColumns := make([]sqlbase.ColumnDescriptor, 0)
		for _, col := range desc.Columns {
			if col.Hidden {
				continue
			}
			visibleDescColumns = append(visibleDescColumns, col)
		}
		desiredTypes := make([]types.T, 0)
		valNode := &valuesNode{}
		for _, cols := range visibleDescColumns {
			desiredTypes = append(desiredTypes, cols.DatumType())
		}

		// 创建供rocksdb查询用的：startkey和endkey
		startPosition, endPosition := GetFetchActionStartpositionAndEndposition(n, desc)
		startKey, endKey := MakeStartKeyAndEndKey(desc, startPosition, endPosition)

		valStmt := tree.ValuesClause{
			Rows: make([]tree.Exprs, 0),
		}
		//为迭代器写的函数：根据start和end KEY，对取出value进行遍历,根据cursor里的column，解码取出列，先添加到rowTup，再把rowTup添加到上面定义的valStmt
		//(for range内部对bytes进行了解码取值，offset处理)
		funcAddValues := func(keyVal engine.MVCCKeyValue) (stop bool, err error) {
			key := keyVal.Key
			bytes, err := memEngine.Get(key)
			if err != nil {
				return true, err
			}
			rowTup := make([]tree.Expr, 0)
			//根据根据每一列的类型，解出每一列的值，扔到rowTup中组成一条记录（row)
			//这里解码还是要按照存进去的原类型解码
			for _, columnDesc := range desc.Columns {
				var datum tree.Datum
				//返回解码后的值，和切割后的value值
				datum, bytes, err = sqlbase.DecodeTableValue(&sqlbase.DatumAlloc{}, columnDesc.DatumType(), bytes)
				if err != nil {
					return true, err
				}
				if columnDesc.Hidden {
					continue
				}
				rowTup = append(rowTup, datum)
			}

			valStmt.Rows = append(valStmt.Rows, rowTup)
			return false, nil
		}
		if err := memEngine.Iterate(startKey, endKey, funcAddValues); err != nil {
			return nil, err
		}

		//部分反向fetch操作，但从存储中取出的结果集是正向排列的，这里进行颠倒
		if (n.Direction == tree.BACKWARD && n.Rows > 0) || (n.Direction == tree.FORWARD && n.Rows < 0) {
			/* reverse row if backword action */
			for front, tail := 0, len(valStmt.Rows)-1; front < tail; {
				valStmt.Rows[front], valStmt.Rows[tail] = valStmt.Rows[tail], valStmt.Rows[front]
				front++
				tail--
			}
		}
		node, err := p.Values(ctx, &valStmt, desiredTypes)
		if err != nil {
			return nil, err
		}
		var ok bool
		valNode, ok = node.(*valuesNode)
		if !ok {
			return nil, errors.Errorf("Cannot construct a value node")
		}

		if len(valNode.tuples) == 0 {
			valNode.columns = sqlbase.ResultColumnsFromColDescs(visibleDescColumns)
		}
		// fill back column name
		for i := 0; i < len(visibleDescColumns); i++ {
			valNode.columns[i].Name = visibleDescColumns[i].Name
		}
		// at last rewrite position to desc
		MoveCursorPosition(n, desc)
		desc.State = uint32(tree.FETCH)
		desc.TxnState = tree.PENDING
		// txn
		AddCursorTxn(p.extendedEvalCtx.curTxnMap, oldDesc, *desc, cursorName)
		value, err := protoutil.Marshal(desc)
		if err != nil {
			return nil, err
		}
		// write meta data to cursor Engine directly
		if err := memEngine.Put(descKey, value); err != nil {
			return nil, errors.Errorf(err.Error() + "\nFailed to store cursor meta data!\n")
		}
		return valNode, nil
	case tree.MOVE:
		if desc.State == uint32(tree.DECLARE) {
			return nil, errors.Errorf("Cannot move a cursor that has not been opened!\n%s", cursorName)
		}
		desc.TxnState = tree.PENDING
		desc.State = uint32(tree.MOVE)
		node.oldDesc = oldDesc
		node.desc = *desc
		return node, nil
	case tree.CLOSE:
		desc.TxnState = tree.PENDING
		desc.State = uint32(tree.CLOSE)
		node.oldDesc = oldDesc
		node.desc = *desc
		return node, nil
	default:
		node.desc = *desc
		return node, nil
	}
}

func (n *cursorAccessNode) startExec(params runParams) error {
	// cursor temp engine
	memEngine, err := GetCursorEngine(params.ExecCfg())
	if err != nil {
		return err
	}
	sessionID := params.extendedEvalCtx.CurrentSessionID
	cursorName := n.n.CursorName.String()
	rKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte(cursorName))
	descKey := engine.MakeMVCCMetadataKey(rKey)
	switch n.n.Action {
	case tree.OPEN:
		if n.sourcePlan == nil {
			return errors.Errorf("can not do this action in cursor access!")
		}
		rowBuffer := make(tree.Datums, len(n.desc.Columns))
		var rowNumber int64 = 1
		for {
			if err := params.p.cancelChecker.Check(); err != nil {
				return err
			}
			//查找一行记录，如果没有下一行了就跳出循环
			if next, err := n.sourcePlan.Next(params); !next {
				if err != nil {
					return err
				}
				// fetch data done
				break
			}
			singleDatum := n.sourcePlan.Values()
			copy(rowBuffer, singleDatum)
			// cursor data key
			idByte := encoding.EncodeUvarintAscending([]byte{}, uint64(n.desc.ID))
			rowNumberByte := encoding.EncodeUvarintAscending([]byte{}, uint64(rowNumber))
			rKey := keys.MakeCursorDataKey(idByte, rowNumberByte)
			key := engine.MakeMVCCMetadataKey(rKey)
			values := []byte{}
			var err error
			//根据列id，编码出一个个的value，增加到values里面(一条values代表一行数据)
			for i, value := range rowBuffer {
				values, err = sqlbase.EncodeTableValue(values, n.desc.Columns[i].ID, value, nil)
				if err != nil {
					return err
				}
			}
			//把打开游标的结果集存入rocksdb内存引擎
			if err := memEngine.Put(key, values); err != nil {
				return err
			}
			rowNumber++
		}
		n.desc.MaxRownum = rowNumber - 1
		value, err := protoutil.Marshal(&n.desc)
		if err != nil {
			return err
		}
		if err := memEngine.Put(descKey, value); err != nil {
			return err
		}
		// txn
		AddCursorTxn(params.extendedEvalCtx.curTxnMap, n.oldDesc, n.desc, cursorName)
	case tree.CLOSE:
		value, err := protoutil.Marshal(&n.desc)
		if err != nil {
			return err
		}
		// Write meta data to cursor Engine directly
		if err := memEngine.Put(descKey, value); err != nil {
			return err
		}
		// txn
		AddCursorTxn(params.extendedEvalCtx.curTxnMap, n.oldDesc, n.desc, cursorName)
	case tree.FETCH:
		// Rewrite position to desc
		MoveCursorPosition(n.n, &n.desc)
		value, err := protoutil.Marshal(&n.desc)
		if err != nil {
			return err
		}
		// Write meta data to cursor Engine directly
		if err := memEngine.Put(descKey, value); err != nil {
			return err
		}
		// txn
		AddCursorTxn(params.extendedEvalCtx.curTxnMap, n.oldDesc, n.desc, cursorName)
		return nil
	case tree.MOVE:
		MoveCursorPosition(n.n, &n.desc)
		value, err := protoutil.Marshal(&n.desc)
		if err != nil {
			return err
		}
		// write meta data to cursor Engine directly
		if err := memEngine.Put(descKey, value); err != nil {
			return err
		}
		// txn
		AddCursorTxn(params.extendedEvalCtx.curTxnMap, n.oldDesc, n.desc, cursorName)
		return nil
	default:
		return errors.Errorf("Can not do this action in cursor access!\n")
	}
	return nil
}

// DeclareCursor will construct a declareCursorNode
func (p *planner) ShowCursors(ctx context.Context, n *tree.ShowCursor) (planNode, error) {
	// Construct createTable statement with cursor name
	//node := &showCursorsNode{n: n}
	// cursor temp engine
	valNode := &valuesNode{}
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return nil, err
	}
	sessionID := p.extendedEvalCtx.CurrentSessionID
	desiredTypes := make([]types.T, 0)
	desiredTypes = append(desiredTypes, types.String)
	desiredTypes = append(desiredTypes, types.Int)
	desiredTypes = append(desiredTypes, types.String)
	if !n.ShowAll {
		cursorName := n.CursorName.String()
		rKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte(cursorName))
		desc := sqlbase.CursorDescriptor{}
		key := engine.MakeMVCCMetadataKey(rKey)
		ok, _, _, err := memEngine.GetProto(key, &desc)
		if err != nil {
			return nil, err
		}
		if ok {
			// construct values exprs
			valStmt := tree.ValuesClause{
				Rows: make([]tree.Exprs, 1),
			}
			rowTup := make([]tree.Expr, 0)
			rowTup = append(rowTup, tree.NewDString(string(desc.Name)))
			rowTup = append(rowTup, tree.NewDInt(tree.DInt(desc.ID)))
			rowTup = append(rowTup, tree.NewDString(string(desc.QueryString)))
			valStmt.Rows[0] = rowTup
			node, err := p.Values(ctx, &valStmt, desiredTypes)
			if err != nil {
				return nil, err
			}
			valNode, ok = node.(*valuesNode)
			if !ok {
				return nil, errors.Errorf("Cannot construct a value node")
			}
		} else {
			// not found
			return nil, errors.Errorf("%s has not been declared!\n", cursorName)
		}
	} else {
		// show all cursor in this session
		rStartKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte{})
		rEndKey := rStartKey.PrefixEnd()
		startKey := engine.MakeMVCCMetadataKey(rStartKey)
		endKey := engine.MakeMVCCMetadataKey(rEndKey)
		valStmt := tree.ValuesClause{
			Rows: make([]tree.Exprs, 0),
		}
		funcAddValues := func(keyVal engine.MVCCKeyValue) (stop bool, err error) {
			key := keyVal.Key
			desc := sqlbase.CursorDescriptor{}
			ok, _, _, err := memEngine.GetProto(key, &desc)
			if ok {
				rowTup := make([]tree.Expr, 0)
				rowTup = append(rowTup, tree.NewDString(desc.Name))
				rowTup = append(rowTup, tree.NewDInt(tree.DInt(desc.ID)))
				rowTup = append(rowTup, tree.NewDString(desc.QueryString))
				valStmt.Rows = append(valStmt.Rows, rowTup)
			}
			return false, nil
		}
		if err := memEngine.Iterate(startKey, endKey, funcAddValues); err != nil {
			return nil, err
		}
		node, err := p.Values(ctx, &valStmt, desiredTypes)
		if err != nil {
			return nil, err
		}
		var ok bool
		valNode, ok = node.(*valuesNode)
		if !ok {
			return nil, errors.Errorf("Cannot construct a value node")
		}
	}
	if len(valNode.columns) != 3 {
		if valNode.columns == nil {
			return nil, errors.Errorf("No cursors exist!\n")
		}
		return nil, errors.Errorf("Show cursor stmt internal error!\n")
	}
	valNode.columns[0].Name = "cursorName"
	valNode.columns[1].Name = "cursorID"
	valNode.columns[2].Name = "cursorQuery"
	return valNode, nil
}

func (p *planner) CommitCursors() error {
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return err
	}
	sessionID := p.extendedEvalCtx.CurrentSessionID
	// 所有curDesc key
	curRStartKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte{})
	curREndKey := curRStartKey.PrefixEnd()
	curStartKey := engine.MakeMVCCMetadataKey(curRStartKey)
	curEndKey := engine.MakeMVCCMetadataKey(curREndKey)

	funcCommitCur := func(keyVal engine.MVCCKeyValue) (stop bool, err error) {
		key := keyVal.Key
		curDesc := sqlbase.CursorDescriptor{}
		ok, _, _, err := memEngine.GetProto(key, &curDesc)
		if ok {
			if curDesc.TxnState != tree.COMMIT {
				cursorName := curDesc.Name
				if curTxn, ok := p.extendedEvalCtx.curTxnMap[cursorName]; ok {
					for i, txnDesc := range curTxn.txnDescs {
						switch txnDesc.State {
						case uint32(tree.CLOSE):
							// remove cursor data first
							if err := DeleteCursorRows(txnDesc, memEngine); err != nil {
								return true, err
							}
							// remove meta data then
							if err := memEngine.Clear(key); err != nil {
								return true, err
							}
							// 如果事务的最后一个操作是删除游标，游标元数据已经被删除，那么后续就无需进行对游标改变状态的逻辑了
							if i == len(curTxn.txnDescs)-1 {
								delete(p.extendedEvalCtx.curTxnMap, cursorName)
								return false, nil
							}
						default:
							curDescValue, err := protoutil.Marshal(&txnDesc)
							if err != nil {
								return true, err
							}
							if err := memEngine.Put(key, curDescValue); err != nil {
								return stop, errors.Errorf("Failed to update %s's meta data!\n", curDesc.Name)
							}
						}
					}
				} else {
					return true, errors.Errorf("Commit cursor error!\n")
				}
				curDesc.TxnState = tree.COMMIT
				curDescValue, err := protoutil.Marshal(&curDesc)
				if err != nil {
					return true, err
				}
				// write meta data to cursor Engine directly
				if err := memEngine.Put(key, curDescValue); err != nil {
					return true, errors.Errorf("Failed to update %s's meta data!\n", curDesc.Name)
				}
				delete(p.extendedEvalCtx.curTxnMap, cursorName)
			}
		}
		return false, nil
	}
	if err := memEngine.Iterate(curStartKey, curEndKey, funcCommitCur); err != nil {
		return errors.Errorf("Commit cursor error!\n")
	}
	return nil
}

func (p *planner) RollBackCursors() error {
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return err
	}
	sessionID := p.extendedEvalCtx.CurrentSessionID
	// 所有curDesc key
	curRStartKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte{})
	curREndKey := curRStartKey.PrefixEnd()
	curStartKey := engine.MakeMVCCMetadataKey(curRStartKey)
	curEndKey := engine.MakeMVCCMetadataKey(curREndKey)

	funcRollBackCur := func(keyVal engine.MVCCKeyValue) (stop bool, err error) {
		key := keyVal.Key
		curDesc := sqlbase.CursorDescriptor{}
		ok, _, _, err := memEngine.GetProto(key, &curDesc)
		if ok {
			if curDesc.TxnState != tree.COMMIT {
				cursorName := curDesc.Name
				if curTxn, ok := p.extendedEvalCtx.curTxnMap[cursorName]; ok {
					for i := len(curTxn.txnDescs) - 1; i > 0; i-- {
						txnDesc := curTxn.txnDescs[i]
						if txnDesc.TxnState == tree.COMMIT {
							break
						}
						switch txnDesc.State {
						case uint32(tree.DECLARE):
							rKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte(cursorName))
							descKey := engine.MakeMVCCMetadataKey(rKey)
							if err := memEngine.Clear(descKey); err != nil {
								return true, err
							}
						case uint32(tree.OPEN):
							if err := DeleteCursorRows(txnDesc, memEngine); err != nil {
								return true, err
							}
						default:
						}
					}
					if curTxn.txnDescs[1].State == uint32(tree.DECLARE) {
						delete(p.extendedEvalCtx.curTxnMap, cursorName)
						return false, nil
					}
					curDescValue, err := protoutil.Marshal(&curTxn.txnDescs[0])
					if err != nil {
						return true, err
					}
					if err := memEngine.Put(key, curDescValue); err != nil {
						return true, errors.Errorf("Failed to update %s's meta data!\n", curDesc.Name)
					}
				}
				delete(p.extendedEvalCtx.curTxnMap, cursorName)
			}
		}
		return false, nil
	}
	if err := memEngine.Iterate(curStartKey, curEndKey, funcRollBackCur); err != nil {
		return errors.Errorf("Rollback cursor error!\n")
	}
	return nil
}
