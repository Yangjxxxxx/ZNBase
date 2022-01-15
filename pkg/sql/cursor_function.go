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
	"strings"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// GetCursorMetaDataColumnDescs 根据sourcePlan返回Cursor columnDescriptor
// 并且确定哪些是需要在显示的时候需要被隐藏的
func GetCursorMetaDataColumnDescs(
	p *planner, sourcePlan planNode, hideColumns []string, parentID sqlbase.ID,
) ([]sqlbase.ColumnDescriptor, error) {
	resultColumns := planColumns(sourcePlan)
	columDescs := make([]sqlbase.ColumnDescriptor, 0)
	var id uint32 = 1
	for _, colRes := range resultColumns {
		//ResultColumn转换为colTypes.T接口类型
		colType, err := coltypes.DatumTypeToColumnType(colRes.Typ)
		if err != nil {
			return nil, err
		}
		//colTypes.T构造ColumnTableDef
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colType}
		columnTableDef.Nullable.Nullability = tree.SilentNull
		//ColumnTableDef转换为ColumnDescriptor
		privs := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Column, p.User())
		col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, &p.semaCtx, privs, parentID)
		if err != nil {
			return nil, err
		}
		col.ID = sqlbase.ColumnID(id)
		for _, hideColumn := range hideColumns {
			if hideColumn == col.Name {
				col.Hidden = true
				break
			}
		}
		columDescs = append(columDescs, *col)
		id++
	}
	return columDescs, nil
}

// GetFetchActionStartpositionAndEndposition 拿到游标fetch操作需要从rocksdb查询的startkey和endkey范围
func GetFetchActionStartpositionAndEndposition(
	n *tree.CursorAccess, desc *sqlbase.CursorDescriptor,
) (startPosition, endPosition int64) {
	switch n.Direction {

	case tree.NEXT:
		startPosition = desc.Position + 1
		endPosition = startPosition + 1

	case tree.PRIOR:
		startPosition = desc.Position - 1
		endPosition = startPosition + 1

	case tree.FIRST:
		startPosition = 1
		endPosition = startPosition + 1

	case tree.LAST:
		startPosition = desc.MaxRownum
		endPosition = startPosition + 1

	case tree.ABSOLUTE:
		if n.Rows < 0 {
			startPosition = desc.MaxRownum + n.Rows + 1
			endPosition = startPosition + 1
		} else {
			startPosition = n.Rows
			endPosition = startPosition + 1
		}

	case tree.RELATIVE:
		startPosition = desc.Position + n.Rows
		endPosition = startPosition + 1

	case tree.FORWARD:
		if n.Rows == 0 {
			startPosition = desc.Position
			endPosition = desc.Position + 1
		} else if n.Rows < 0 {
			endPosition = desc.Position
			startPosition = endPosition + n.Rows
			//游标当前位置上面还有数据的情况
			if endPosition > 1 {
				if startPosition < 1 {
					startPosition = 1
				}
			}
		} else {
			startPosition = desc.Position + 1
			if n.Rows == tree.RowMax {
				endPosition = desc.MaxRownum + 1
			} else {
				endPosition = startPosition + n.Rows
			}
		}

	case tree.BACKWARD:
		if n.Rows == 0 {
			startPosition = desc.Position
			endPosition = startPosition + 1
		} else if n.Rows < 0 {
			startPosition = desc.Position + 1
			endPosition = startPosition - n.Rows
		} else {
			endPosition = desc.Position
			startPosition = endPosition - n.Rows
			//游标当前位置上面还有数据的情况
			if endPosition > 1 {
				if startPosition < 1 {
					startPosition = 1
				}
			}
		}
	}
	return startPosition, endPosition
}

// MoveCursorPosition 游标操作完成后，更新游标当前位置（Position）
func MoveCursorPosition(n *tree.CursorAccess, desc *sqlbase.CursorDescriptor) {
	switch n.Direction {

	case tree.NEXT:
		desc.Position++
		if desc.Position > desc.MaxRownum {
			desc.Position = desc.MaxRownum + 1
		}

	case tree.PRIOR:
		desc.Position--
		if desc.Position < 1 {
			desc.Position = 0
		}

	case tree.FIRST:
		desc.Position = 1

	case tree.LAST:
		desc.Position = desc.MaxRownum

	case tree.ABSOLUTE:
		if n.Rows >= 0 {
			desc.Position = n.Rows
			if desc.Position > desc.MaxRownum {
				desc.Position = desc.MaxRownum + 1
			}
		} else {
			desc.Position = desc.MaxRownum + n.Rows + 1
			if desc.Position < 1 {
				desc.Position = 0
			}
		}
		if desc.Position < 1 {
			desc.Position = 0
		}

	case tree.RELATIVE:
		desc.Position += n.Rows
		if desc.Position > desc.MaxRownum {
			desc.Position = desc.MaxRownum + 1
		}
		if desc.Position < 1 {
			desc.Position = 0
		}

	case tree.FORWARD:
		if n.Rows == tree.RowMax {
			desc.Position = desc.MaxRownum + 1
		} else {
			desc.Position += n.Rows
			if desc.Position > desc.MaxRownum {
				desc.Position = desc.MaxRownum + 1
			}
			if desc.Position < 1 {
				desc.Position = 0
			}
		}

	case tree.BACKWARD:
		if n.Rows == tree.RowMax {
			desc.Position = 0
		} else {
			desc.Position -= n.Rows
			if desc.Position > desc.MaxRownum {
				desc.Position = desc.MaxRownum + 1
			}
			if desc.Position < 1 {
				desc.Position = 0
			}
		}

	}
}

// AssignValToCursorParam 打开带参数游标，为变量赋值
// 参数解释：desc为当前打开的游标的元数据
func AssignValToCursorParam(
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	n *tree.CursorAccess,
	desc *sqlbase.CursorDescriptor,
	curVarDescMap map[string]*CurVarDesc,
	curVarDescArray []*CurVarDesc,
	curVarMap map[string]*tree.CursorVar,
) error {
	cursorName := desc.Name
	count := 0
	for _, curVar := range n.CurVars {
		// 参数过多
		if count >= len(curVarDescMap) {
			return errors.Errorf("Cursor '%s' has too many parameters given!\n", cursorName)
		}
		dataExpr := curVar.CurVarDataExpr
		var curVarName string
		var curVarDesc *CurVarDesc
		var datum tree.Datum
		// 1.顺序赋值
		if string(curVar.CurVarName) == "" {
			curVarDesc = curVarDescArray[count]
			curVarName = curVarDesc.desc.Name
		} else {
			// 2.变量名赋值
			curVarName = curVar.CurVarName.String()
			//匹配当前cursor元数据中的同名变量
			if curVarDescInMap, ok := curVarDescMap[curVarName]; ok {
				curVarDesc = curVarDescInMap
			} else {
				return errors.Errorf("Cursor '%s' has no parameter named '%s'!\n", cursorName, curVarName)
			}
		}
		// 重复赋值检查
		if curVarDesc.state {
			return errors.Errorf("Parameter value '%s' in cursor '%s' is specified more than once!\n",
				curVarName, cursorName)
		}

		// 获取变量types.T数据类型
		desc := curVarDesc.desc
		desireType := desc.Column.DatumType()
		// 根据定义时候的type转换接收到的expr
		// useOrigin: true不允许类型转换, false允许类型转换
		typedExpr, err := dataExpr.TypeCheck(semaCtx, desireType, false)
		if err != nil {
			return err
		}
		// 转换为Datum类型
		datum, err = typedExpr.Eval(evalCtx)
		if err != nil {
			return err
		}
		cv := tree.CursorVar{
			CurVarName:     tree.Name(curVarName),
			CurVarColType:  nil,
			CurVarDataExpr: curVar.CurVarDataExpr,
			CurVarType:     desc.Column.DatumType(),
			CurVarDatum:    nil,
		}
		cv.CurVarDatum = datum
		curVarMap[curVarName] = &cv
		curVarDescMap[curVarName].state = true
		count++
	}
	// 参数不足
	if count < len(curVarDescMap) {
		return errors.Errorf("Insufficient cursor '%s' parameters!\n", cursorName)
	}
	return nil
}

// ParseCursor parse queryString to get planNode
func ParseCursor(
	ctx context.Context, p *planner, desc *sqlbase.CursorDescriptor,
) (planNode, []string, error) {
	stmt, err := parser.ParseOne(desc.QueryString, p.EvalContext().GetCaseSensitive())
	if err != nil {
		return nil, nil, err
	}
	if err := p.semaCtx.Placeholders.Assign(nil, stmt.NumPlaceholders); err != nil {
		return nil, nil, err
	}
	queryStmt, ok := stmt.AST.(*tree.Select)
	if !ok {
		return nil, nil, errors.Errorf("%s cursor's query is not a 'SELECT' statement!", desc.Name)
	}

	queryStmtSelect, ok := queryStmt.Select.(*tree.SelectClause)
	if !ok {
		return nil, nil, errors.Errorf("%s cursor's query is not a 'SELECT' statement!", desc.Name)
	}
	// hideColumns:哪些是FETCH时需要被隐藏的列
	hideColumns := make([]string, 0)
	if queryStmtSelect.GroupBy == nil && queryStmtSelect.Having == nil &&
		queryStmtSelect.Distinct == false && queryStmtSelect.DistinctOn == nil {
		// 判断查询类型，如果是普通单表游标查询，就加入primaryKey元数据
		// 这个primaryKey用于在where current of语法中，将where后过滤条件替换为 主键=values
		tableNameExprs := queryStmtSelect.From.Tables
		if len(tableNameExprs) == 1 {
			tableNameExpr := tableNameExprs[0]
		assertTableNameExprType:
			switch tableNameExpr.(type) {
			// 单表查询,拿到表的主键列名数组(primaryKeys),填充游标元数据
			case *tree.AliasedTableExpr:
				aliasedTableExpr := tableNameExpr.(*tree.AliasedTableExpr)

				var tableName *tree.TableName
				switch exprTyped := aliasedTableExpr.Expr.(type) {
				// 普通表
				case *tree.TableName:
					tableName = exprTyped
				// 分区表
				case *tree.PartitionExpr:
					tableName = &exprTyped.TableName
				// 未断言成功，说明内部还有子查询，非单表查询
				default:
					break assertTableNameExprType
				}

				tableDesc, err := ResolveExistingObject(ctx, p, tableName, true /*required*/, requireTableOrViewOrSequenceDesc)
				if err != nil {
					return nil, nil, err
				}
				// 如果是视图就不支持where current of
				if tableDesc.IsView() {
					break assertTableNameExprType
				}
				tableDesc.IsSequence()
				primaryKeys := tableDesc.TableDescriptor.PrimaryIndex.ColumnNames
				desc.TableName = tableName.String()
				desc.PrimaryIndex = primaryKeys

				// 判断是否是自动生成的主键
				// 自动生成主键格式rowid，
				// 如果rowid已经被声明则主键为rowid_1,若rowid_1也已经被声明则主键为rowid_2
				autoGeneratePrimaryKey := false
				if len(primaryKeys) == 1 && strings.HasPrefix(primaryKeys[0], "rowid") {
					for _, column := range tableDesc.TableDescriptor.Columns {
						if column.IsRowIDColumn() {
							autoGeneratePrimaryKey = true
							break
						}
					}
				}

				// 补全表的主键columnDescriptor到curDesc中
			rangePrimaryKeys:
				for _, pk := range primaryKeys {
					hide := true
				rangeExprs:
					for _, expr := range queryStmtSelect.Exprs {
						switch selectExpr := expr.Expr.(type) {
						// select普通列, example: "select id,name from test"
						case *tree.UnresolvedName:
							// 处理掉列名前面的前缀(database name/schema name/alias name)
							columnName := selectExpr.Parts[0]
							if pk == columnName {
								hide = false
								break rangeExprs
							}
						// select *，example: "select * from test"
						// 这里要用类型断言，不要用指针!!!
						case tree.UnqualifiedStar:
							// 如果表的主键是自动生成的,例如:rowid、rowid_1,那么 SELECT * 就不包含主键
							// 如果是用户指定的主键，那么SELECT * 包含了所有的列，主键一定不是隐藏列
							if !autoGeneratePrimaryKey {
								hide = false
								break rangePrimaryKeys
							}
						case *tree.FuncExpr:
							isCountFunc := checkCountFuncExpr(selectExpr)
							if isCountFunc {
								desc.PrimaryIndex = nil
								break rangePrimaryKeys
							}
						case *tree.BinaryExpr:
							containCountFunc := checkBinaryExpr(selectExpr)
							if containCountFunc {
								desc.PrimaryIndex = nil
								break rangePrimaryKeys
							}
						}
					}
					if hide {
						selectExpr := tree.SelectExpr{
							Expr: &tree.UnresolvedName{
								NumParts: 1,
								Star:     false,
								Parts:    [4]string{pk, "", "", ""},
							},
							As: "",
						}
						queryStmtSelect.Exprs = append(queryStmtSelect.Exprs, selectExpr)
						hideColumns = append(hideColumns, pk)
					}
				}
			default:

			}
		}
	}

	sourcePlan, err := p.Select(ctx, queryStmt, []types.T{})
	if err != nil {
		return nil, nil, err
	}
	return sourcePlan, hideColumns, nil
}

// checkBinaryExpr 检测binary表达式中是否包含count()函数
func checkBinaryExpr(binaryExpr *tree.BinaryExpr) bool {
	containCountFunc := false
	left := binaryExpr.Left
	right := binaryExpr.Right

	switch rightExpr := right.(type) {
	case *tree.FuncExpr:
		isCountFunc := checkCountFuncExpr(rightExpr)
		if isCountFunc {
			containCountFunc = true
		}
	case *tree.ParenExpr:
		if subBinaryExpr, ok := rightExpr.Expr.(*tree.BinaryExpr); ok {
			containCountFunc = checkBinaryExpr(subBinaryExpr)
		}
	}
	if containCountFunc {
		return containCountFunc
	}

	switch leftExpr := left.(type) {
	case *tree.FuncExpr:
		isCountFunc := checkCountFuncExpr(leftExpr)
		if isCountFunc {
			containCountFunc = true
		}
	case *tree.ParenExpr:
		if subBinaryExpr, ok := leftExpr.Expr.(*tree.BinaryExpr); ok {
			containCountFunc = checkBinaryExpr(subBinaryExpr)
		}
	}
	return containCountFunc
}

// checkCountFuncExpr 检测Func表达式是否是count()函数
func checkCountFuncExpr(funcExpr *tree.FuncExpr) bool {
	isCountFunc := false
	functionReference := funcExpr.Func.FunctionReference
	if functionReference, ok := functionReference.(*tree.UnresolvedName); ok {
		// 对count函数做特殊处理，当有count函数时候，游标不对表支持update/delete where current of 操作
		if functionReference.Parts[0] == "count" {
			isCountFunc = true
		}
	}
	return isCountFunc
}

// FindCursor try to find a cursorDesc through cursorName with ExecutorConfig and extendedEvalContext
func FindCursor(
	executorConfig *ExecutorConfig, extendedEvalContext *extendedEvalContext, cursorName string,
) (engine.MVCCKey, *sqlbase.CursorDescriptor, error) {
	sessionID := extendedEvalContext.CurrentSessionID
	rKey := keys.MakeCursorMetaKey(sessionID.GetBytes(), []byte(cursorName))
	key := engine.MakeMVCCMetadataKey(rKey)
	memEngine, err := GetCursorEngine(executorConfig)
	if err != nil {
		return key, nil, err
	}
	desc := sqlbase.CursorDescriptor{}
	//获取了游标对应key的元数据信息 填充desc
	ok, _, _, err := memEngine.GetProto(key, &desc)
	if err != nil {
		return key, nil, err
	}
	if !ok {
		return key, nil, errors.Errorf("%q has not been declared!\n", cursorName)
	}
	return key, &desc, nil
}

// MakeStartKeyAndEndKey 根据cursorID、startPosition、endPosition,构建MVCCKey
func MakeStartKeyAndEndKey(
	desc *sqlbase.CursorDescriptor, startPosition int64, endPosition int64,
) (startKey, endKey engine.MVCCKey) {
	idByte := encoding.EncodeUvarintAscending([]byte{}, uint64(desc.ID))
	startByte := encoding.EncodeUvarintAscending([]byte{}, uint64(startPosition))
	endByte := encoding.EncodeUvarintAscending([]byte{}, uint64(endPosition))
	rStartKey := keys.MakeCursorDataKey(idByte, startByte)
	rEndKey := keys.MakeCursorDataKey(idByte, endByte)
	startKey = engine.MakeMVCCMetadataKey(rStartKey)
	endKey = engine.MakeMVCCMetadataKey(rEndKey)
	return startKey, endKey
}

// GetCurrentCursorPrimaryKeyMap get the primary keys map of the table queried by the cursor.
func GetCurrentCursorPrimaryKeyMap(
	p *planner, curName string, tableName string,
) (map[string]tree.Datum, error) {
	memEngine, err := GetCursorEngine(p.ExecCfg())
	if err != nil {
		return nil, err
	}
	var pksMap = make(map[string]tree.Datum)
	_, curDesc, err := FindCursor(p.ExecCfg(), p.ExtendedEvalContext(), curName)
	if err != nil {
		return nil, err
	}
	if curDesc.State == uint32(tree.DECLARE) {
		return nil, errors.Errorf("%s has not been opened!\n", curName)
	}
	if curDesc.State == uint32(tree.CLOSE) {
		return nil, errors.Errorf("%q has not been declared!\n", curName)
	}
	if curDesc.TableName == "" || curDesc.PrimaryIndex == nil {
		return nil, errors.Errorf("'%s' bounds query is not a simple single table query.\n"+
			"Cannot use '%s' to 'UPDATE/DELETE' table/view '%s'!\n", curName, curName, tableName)
	}
	if tableName != curDesc.TableName {
		return nil, errors.Errorf("Cannot use %s update table %s!\n"+
			"The binding table of %s is %s", curName, tableName, curName, curDesc.TableName)
	}
	if curDesc.Position > curDesc.MaxRownum || curDesc.Position < 1 {
		return nil, errors.Errorf("Cursor current position has no data!\n")
	}
	curAccess := &tree.CursorAccess{
		CursorName: tree.Name(curName),
		// 游标动作
		Action: tree.FETCH,
		// 滚动条目数
		Rows: 0,
		// 滚动方向
		Direction: tree.RELATIVE,
	}
	startPosition, endPosition := GetFetchActionStartpositionAndEndposition(curAccess, curDesc)
	startKey, _ := MakeStartKeyAndEndKey(curDesc, startPosition, endPosition)
	rowBytes, err := memEngine.Get(startKey)
	if err != nil {
		return nil, errors.Errorf(err.Error() + "\nFailed to get cursor data!\n")
	}
	for _, columnDesc := range curDesc.Columns {
		var datum tree.Datum
		//返回解码后的值，和切割后的value值
		datum, rowBytes, err = sqlbase.DecodeTableValue(&sqlbase.DatumAlloc{}, columnDesc.DatumType(), rowBytes)
		if err != nil {
			return nil, errors.Errorf(err.Error() + "\nCursor data decoding failed!\n")
		}

		for _, pk := range curDesc.PrimaryIndex {
			if columnDesc.Name == pk {
				pksMap[pk] = datum
			}
		}

		if len(pksMap) == len(curDesc.PrimaryIndex) {
			break
		}
	}
	return pksMap, nil
}

// WithoutSingleQuotationMark 字符串格式化，去除两边的单引号
func WithoutSingleQuotationMark(s string) string {
	prefix := s[0]
	suffix := s[len(s)-1]
	if prefix == '\'' && suffix == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

// DeleteCursorRows delete cursor data on rollback.
func DeleteCursorRows(curDesc sqlbase.CursorDescriptor, memEngine engine.Engine) error {
	// all row key
	rowIDByte := encoding.EncodeUvarintAscending([]byte{}, uint64(curDesc.ID))
	rowRStartKey := keys.MakeCursorDataKey(rowIDByte, []byte{})
	rowREndKey := rowRStartKey.PrefixEnd()
	rowStartKey := engine.MakeMVCCMetadataKey(rowRStartKey)
	rowEndKey := engine.MakeMVCCMetadataKey(rowREndKey)
	if err := memEngine.ClearRange(rowStartKey, rowEndKey); err != nil {
		return errors.Errorf(err.Error()+"\nFailed to delete %s's data!\n", curDesc.Name)
	}
	return nil
}

// AddCursorTxn add cursor transaction to extendedEvalContext.curTxnMap.
func AddCursorTxn(
	curTxnMap map[string]CurTxn, oldDesc, nowDesc sqlbase.CursorDescriptor, cursorName string,
) {
	if curTxnMap == nil {
		curTxnMap = make(map[string]CurTxn)
	}
	if curTxn, ok := curTxnMap[cursorName]; !ok {
		newTxnDesc := append(make([]sqlbase.CursorDescriptor, 0), oldDesc, nowDesc)
		curTxnMap[cursorName] = CurTxn{txnDescs: newTxnDesc}
	} else {
		newTxnDesc := append(curTxn.txnDescs, nowDesc)
		nowCurTxn := CurTxn{txnDescs: newTxnDesc}
		curTxnMap[cursorName] = nowCurTxn
	}

}

// OptimizerModeCheck checks whether "UPDATE/DELETE ... WHERE CURRENT OF" statement is running in optimization mode.
func OptimizerModeCheck(AST tree.Statement) error {
	switch t := AST.(type) {
	case *tree.Update:
		if t.Where != nil {
			if _, ok := t.Where.Expr.(*tree.CurrentOfExpr); ok {
				return errors.Errorf("\"UPDATE ... WHERE CURRENT OF CURSOR\" is not supported in optimization mode!\n")
			}
		}
	case *tree.Delete:
		if t.Where != nil {
			if _, ok := t.Where.Expr.(*tree.CurrentOfExpr); ok {
				return errors.Errorf("\"DELETE ... WHERE CURRENT OF CURSOR\" is not supported in optimization mode!\n")
			}
		}
	default:

	}
	return nil
}

// GetCursorEngine get cursor memory engine.
func GetCursorEngine(executorConfig *ExecutorConfig) (engine.Engine, error) {
	cursorEngines := executorConfig.CursorEngines
	if cursorEngines == nil {
		return nil, errors.Errorf("Cursor storage engine does not exist!\n")
	}
	memEngine := cursorEngines[0]
	return memEngine, nil
}

// CloseAllUnclosedCursors will close all cursors that unclosed in plsql function/procedure
func CloseAllUnclosedCursors(
	execConfig *ExecutorConfig, evalContext *extendedEvalContext, params tree.UDRVars,
) error {
	memEngine, err := GetCursorEngine(execConfig)
	if err != nil {
		return err
	}
	for i, para := range params {
		// only check cursor type variable's state and try to close unclosed cursor
		if para.VarOid == oid.T_refcursor && para.State != tree.CursorClosed && para.State != tree.CursorUndeclared {
			// get cursor key and cursor descriptor through cursor name
			cursorKey, cursorDesc, err := FindCursor(execConfig, evalContext, para.VarName)
			if err != nil {
				return err
			}
			oldCursorDescZNBase := *cursorDesc
			cursorDesc.State = uint32(tree.CLOSE)
			cursorDesc.TxnState = tree.PENDING
			value, err := protoutil.Marshal(cursorDesc)
			if err != nil {
				return err
			}
			if err := memEngine.Put(cursorKey, value); err != nil {
				return err
			}
			AddCursorTxn(evalContext.curTxnMap, oldCursorDescZNBase, *cursorDesc, para.VarName)
			params[i].State = tree.CursorClosed
		}
	}
	return nil
}
