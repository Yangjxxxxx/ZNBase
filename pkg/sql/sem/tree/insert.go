// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015  The Cockroach Authors.
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import (
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

// Insert represents an INSERT statement.
type Insert struct {
	With       *With
	Table      TableExpr
	Columns    NameList
	Rows       *Select
	OnConflict *OnConflict
	Returning  ReturningClause
}

// Format implements the NodeFormatter interface.
func (node *Insert) Format(ctx *FmtCtx) {
	if node.OnConflict.IsMergeStmt() {
		// merge into A using B
		ctx.WriteString("MERGE INTO ")
		ctx.FormatNode(node.Table)
		ctx.WriteString(" USING ")
		usingStmt := node.Rows.String()
		if len(usingStmt) >= 5 && usingStmt[0:6] == "TABLE " {
			usingStmt = usingStmt[6:]
		}
		ctx.WriteString(usingStmt)
		// ON CONDITION
		ctx.WriteString(" ")
		ctx.FormatNode(node.OnConflict.Condition)
		// MATCH LIST
		for _, match := range node.OnConflict.MatchList {
			ctx.WriteString(" WHEN MATCHED ")
			if match.Condition != nil {
				ctx.WriteString("AND ")
				ctx.FormatNode(match.Condition)
			}
			ctx.WriteString(" THEN ")
			if match.Sig != nil {
				// state
				ctx.WriteString("SIGNAL SQLSTATE ")
				ctx.WriteString("'")
				ctx.WriteString(*match.Sig.SQLState)
				ctx.WriteString("'")
				// message
				ctx.WriteString(" SET MESSAGE_TEXT ")
				ctx.WriteString("'")
				ctx.WriteString(*match.Sig.MessageText)
				ctx.WriteString("'")
			} else if match.IsDelete {
				ctx.WriteString("DELETE")
			} else {
				ctx.WriteString("UPDATE SET ")
				for i, setClause := range match.Operation {
					if i != 0 {
						ctx.WriteString(",")
					}
					ctx.WriteString(setClause.Names.String())
					ctx.WriteString(" = ")
					ctx.WriteString(setClause.Expr.String())
				}
			}
		}
		// NOTMATCH LIST
		for _, notMatch := range node.OnConflict.NotMatchList {
			ctx.WriteString(" WHEN NOT MATCHED ")
			if notMatch.Condition != nil {
				ctx.WriteString("AND ")
				ctx.FormatNode(notMatch.Condition)
			}
			ctx.WriteString(" THEN ")
			if notMatch.Sig != nil {
				// state
				ctx.WriteString("SIGNAL SQLSTATE ")
				ctx.WriteString("'")
				ctx.WriteString(*notMatch.Sig.SQLState)
				ctx.WriteString("'")
				// message
				ctx.WriteString(" SET MESSAGE_TEXT ")
				ctx.WriteString("'")
				ctx.WriteString(*notMatch.Sig.MessageText)
				ctx.WriteString("'")
			} else {
				ctx.WriteString("INSERT ")
				if notMatch.ColNames != nil {
					ctx.WriteString("(")
					for i, colName := range notMatch.ColNames {
						if i != 0 {
							ctx.WriteString(",")
						}
						ctx.WriteString(colName.String())
					}
					ctx.WriteString(")")
				}
				ctx.WriteString(" VALUES")
				ctx.WriteString("(")
				for i, insertValues := range notMatch.Operation {
					if i != 0 {
						ctx.WriteString(",")
					}
					ctx.WriteString(insertValues.String())
				}
				ctx.WriteString(")")
			}
		}
	} else {
		ctx.FormatNode(node.With)
		if node.OnConflict.IsUpsertAlias() {
			ctx.WriteString("UPSERT")
		} else {
			ctx.WriteString("INSERT")
		}
		ctx.WriteString(" INTO ")
		ctx.FormatNode(node.Table)
		if node.Columns != nil {
			ctx.WriteByte('(')
			ctx.FormatNode(&node.Columns)
			ctx.WriteByte(')')
		}
		if node.DefaultValues() {
			ctx.WriteString(" DEFAULT VALUES")
		} else {
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Rows)
		}
		if node.OnConflict != nil && !node.OnConflict.IsUpsertAlias() {
			ctx.WriteString(" ON CONFLICT")
			if len(node.OnConflict.Columns) > 0 {
				ctx.WriteString(" (")
				ctx.FormatNode(&node.OnConflict.Columns)
				ctx.WriteString(")")
			}
			if node.OnConflict.DoNothing {
				ctx.WriteString(" DO NOTHING")
			} else {
				ctx.WriteString(" DO UPDATE SET ")
				ctx.FormatNode(&node.OnConflict.Exprs)
				if node.OnConflict.Where != nil {
					ctx.WriteByte(' ')
					ctx.FormatNode(node.OnConflict.Where)
				}
			}
		}
		if HasReturningClause(node.Returning) {
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Returning)
		}
	}
}

// DefaultValues returns true iff only default values are being inserted.
func (node *Insert) DefaultValues() bool {
	return node.Rows.Select == nil
}

// OnConflict represents an `ON CONFLICT (columns) DO UPDATE SET exprs WHERE
// where` clause.
//
// The zero value for OnConflict is used to signal the UPSERT short form, which
// uses the primary key for as the conflict index and the values being inserted
// for Exprs.
type OnConflict struct {
	Columns            NameList
	Exprs              UpdateExprs
	Where              *Where
	DoNothing          bool
	Condition          *OnJoinCond
	MatchList          CondAndOpList
	NotMatchList       NmCondAndOpList
	MergeOrNot         bool
	MergeHashCondition int
	IsDuplicate        bool
}

//CondAndOp represents condition and operation for match
type CondAndOp struct {
	Condition Expr
	Operation UpdateExprs
	IsDelete  bool
	Sig       *Signal
}

// CondAndOpList represents CondAndOp list
type CondAndOpList []CondAndOp

// NmCondAndOp represents condition and operation for not match
type NmCondAndOp struct {
	Condition Expr
	ColNames  NameList
	Operation []Exprs
	Sig       *Signal
}

// NmCondAndOpList represents NmCondAndOp list
type NmCondAndOpList []NmCondAndOp

// IsUpsertAlias returns true if the UPSERT syntactic sugar was used.
func (oc *OnConflict) IsUpsertAlias() bool {
	return oc != nil && oc.Columns == nil && oc.Exprs == nil && oc.Where == nil && !oc.DoNothing && oc.Condition == nil
}

// IsMergeStmt return true if is a merge stmt
func (oc *OnConflict) IsMergeStmt() bool {
	return oc != nil && oc.MergeOrNot == true
}

// OnConflict will implements some method for merge feature, it will transforms
// one column of sourceTab or targetTab to an CaseExpr, As below SQL
// MERGE INTO EMPLOYE AS E USING
// MANAGER AS M ON E.EMPLOYEID = M.MANAGERID
// WHEN MATCHED AND M.SALARY > 5000 THEN  UPDATE SET SALARY = M.SALARY*0.8
// WHEN NOT MATCHED AND M.SALARY < 5000 THEN INSERT VALUES(M.MANAGERID, M.name, M.salary*0.8);
//    =>> it will transforms project cols about left join as below SQL
//    SELECT
//		CASE WHEN E.EMPLOYEID IS NULL THEN CASE WHEN M.SALARY < 5000 THEN M.MANAGERID ELSE NULL END ELSE NULL END,
//		CASE WHEN E.EMPLOYEID IS NULL THEN M.NAME ELSE NULL END,
//		CASE WHEN E.EMPLOYEID IS NULL THEN M.SALARY ELSE NULL END,
//		E.EMPLOYEID,
//		E.NAME,
//		E.SALARY,
//		CASE WHEN E.EMPLOYEID IS NULL THEN NULL ELSE CASE WHEN M.SALARY > 5000 THEN M.SALARY*0.8 ELSE E.SALARY END END
//	  FROM MANAGER M LEFT OUTER JOIN EMPLOYE E ON MANAGERID = EMPLOYEID;

// ConstructCaseExprByColName will build caseExpr for normal insertCols, updateCols etc
func (oc *OnConflict) ConstructCaseExprByColName(
	colName Name, colOrd int, tableName string, matchOrNot bool, ToCast bool, CastTyp types.T,
) (Expr, error) {
	if oc.IsMergeStmt() == false {
		return nil, pgerror.NewErrorf(pgcode.InvalidName, "Fatal Error, Is not a Merget Stmt!")
	}
	caseExpr := &CaseExpr{
		Expr: nil,
	}
	unResolvedExp := &UnresolvedName{
		NumParts: 2,
		Star:     false,
		Parts:    [4]string{string(colName), string(tableName)},
	}
	whensSlice := make([]*When, 0)
	if matchOrNot {
		/* match condition */
		for _, match := range oc.MatchList {
			for _, updateExpr := range match.Operation {
				if updateExpr.Names.String() == string(colName) {
					cond := match.Condition
					if cond == nil {
						cond = DBoolTrue
					}
					when := &When{
						Cond: cond,
						Val:  updateExpr.Expr,
					}
					whensSlice = append(whensSlice, when)
				}
			}
		}
		if len(whensSlice) == 0 {
			/* no any update expr about this name, it will return nil expr for this case */
			return nil, nil
		}
	} else {
		/* not match condition */
		for _, notMatch := range oc.NotMatchList {
			cond := notMatch.Condition
			if cond == nil {
				cond = DBoolTrue
			}
			when := &When{
				Cond: cond,
			}
			// if notMatch.ColNames is empty means exprs's order is same as cols order
			// else should find by colname and get when's val
			if notMatch.ColNames != nil {
				for i, name := range notMatch.ColNames {
					if name == colName {
						when.Val = notMatch.Operation[0][i]
						break
					}
				}
			} else {
				if notMatch.Operation != nil {
					when.Val = notMatch.Operation[0][colOrd]
				}
			}
			if when.Val == nil {
				when.Val = dNull{}
			}
			if ToCast {
				texpr := ChangeToCastExpr(when.Val, CastTyp, 0)
				when.Val = &texpr
			}
			whensSlice = append(whensSlice, when)
		}
	}
	caseExpr.Whens = whensSlice
	caseExpr.Else = unResolvedExp
	if ToCast {
		texpr := ChangeToCastExpr(caseExpr.Else, CastTyp, 0)
		caseExpr.Else = &texpr
	}
	return caseExpr, nil
}

// ConstructCaseExprOfAdditionalCol will build caseExpr for AdditionalCol
func (oc *OnConflict) ConstructCaseExprOfAdditionalCol(
	tableName string, primaryKeyName string,
) (Expr, error) {
	if oc.IsMergeStmt() == false {
		return nil, pgerror.NewErrorf(pgcode.InvalidName, "Fatal Error, Is not a Merget Stmt!")
	}
	caseExpr := &CaseExpr{
		Expr: nil,
	}
	caseExpr.Else = NewTypedAddtionalExpr(NothingOperation, nil)
	whensSlice := make([]*When, 0)
	// matchList
	for _, match := range oc.MatchList {
		cond := match.Condition
		if cond == nil {
			cond = oc.Condition.Expr
		} else {
			cond = &AndExpr{Left: oc.Condition.Expr,
				Right: cond}
		}
		when := &When{
			Cond: cond,
		}
		if match.IsDelete {
			when.Val = NewTypedAddtionalExpr(DeleteOperation, nil)
			whensSlice = append(whensSlice, when)
		} else if match.Sig != nil {
			when.Val = NewTypedAddtionalExpr(SignalOperation, match.Sig)
			whensSlice = append(whensSlice, when)
		}
	}
	// not matchList
	leftCanary := NewUnresolvedName(tableName, primaryKeyName)
	rightCanaryNum := dNull{}
	notJoin := &ComparisonExpr{Operator: IsNotDistinctFrom, SubOperator: EQ, Left: leftCanary, Right: rightCanaryNum}
	notMatchCon := &AndExpr{
		Left:  DBoolTrue,
		Right: notJoin,
	}
	flagSomeNotInsert := true
	for _, notMatch := range oc.NotMatchList {
		cond := notMatch.Condition
		if cond == nil {
			cond = notJoin
			flagSomeNotInsert = false
		} else {
			cond = &AndExpr{Left: notJoin,
				Right: cond}
			notExpr := &NotExpr{
				Expr: cond,
			}
			subNotMatchCon := &AndExpr{
				Left:  notMatchCon.Left,
				Right: notExpr,
			}
			notMatchCon.Left = subNotMatchCon
		}
		when := &When{
			Cond: cond,
		}
		if notMatch.Sig != nil {
			when.Val = NewTypedAddtionalExpr(SignalOperation, notMatch.Sig)
			whensSlice = append(whensSlice, when)
		}
	}
	if flagSomeNotInsert {
		notMatchWhen := &When{
			Cond: notMatchCon,
		}
		notMatchWhen.Val = NewTypedAddtionalExpr(NotInsertOperation, nil)
		whensSlice = append(whensSlice, notMatchWhen)
	}
	caseExpr.Whens = whensSlice
	return caseExpr, nil
}

// GetUnionColListOfMatch returns the union list of column names which will be updated in a merge sql
func (oc *OnConflict) GetUnionColListOfMatch() ([]Name, error) {
	unionCols := make([]Name, 0)
	containNames := make(map[Name]struct{})
	for _, aMatch := range oc.MatchList {
		if aMatch.IsDelete || aMatch.Sig != nil {
			continue
		} else {
			// isRepeat 用于标记在一个Match语句中是否存在对同一列的多次操作
			isRepeat := false
			rowContainName := make(map[Name]struct{})

			for _, expr := range aMatch.Operation {
				for _, colName := range expr.Names {
					if _, ok := rowContainName[colName]; ok {
						isRepeat = true
					} else {
						rowContainName[colName] = struct{}{}
						if _, ok := containNames[colName]; !ok {
							containNames[colName] = struct{}{}
							unionCols = append(unionCols, colName)
						}
					}
				}
				if isRepeat {
					// 判断一个updateExprs中是否多次更新某一列，如有则报错
					return nil, pgerror.NewErrorf(pgcode.Syntax,
						"multiple assignments to the same column in a matched")
				}
			}
		}
	}
	return unionCols, nil
}

// GetUnionColsOfNotMatch will return sorted union of columns which will be insert into when not match on merge
// 		colNames  => column names of table
//		rowidName => hidden column of table without primary key, only useful when hasPrimaryKey is false
func (oc *OnConflict) GetUnionColsOfNotMatch(
	colNames []Name, rowidName string, hasPrimaryKey bool,
) ([]Name, error) {
	unionNmCols := make([]Name, 0)
	containNamesNm := make(map[Name]struct{})
	for i, nm := range oc.NotMatchList {
		if nm.Sig != nil {
			continue
		}
		// 检查NameList
		nameListTmp := make(map[Name]struct{})
		// NameList为空的情况
		if nm.ColNames == nil {
			for _, colName := range colNames {
				if !hasPrimaryKey && string(colName) == rowidName {
					continue
				}
				if _, ok := containNamesNm[colName]; !ok {
					containNamesNm[colName] = struct{}{}
					nameListTmp[colName] = struct{}{}
				}
			}

			if oc.MergeHashCondition == 1 || oc.MergeHashCondition == 2 {
				oc.AppendHashnum(&nm, false, i)
			}

			if len(nameListTmp) != len(nm.Operation[0]) {
				//if hasPrimaryKey || len(nameListTmp) != len(nm.Operation[0]) + 1 {
				return nil, pgerror.NewErrorf(pgcode.Syntax,
					"notMatched's values count is not right")
				//}
			}
		} else {
			if oc.MergeHashCondition == 1 || oc.MergeHashCondition == 2 {
				oc.AppendHashnum(&nm, true, i)
			}
			if len(nm.ColNames) != len(nm.Operation[0]) {
				return nil, pgerror.NewErrorf(pgcode.Syntax,
					"notMatched's values count is not right")
			}
		}

		for _, name := range nm.ColNames {
			// 判断是否重复
			if _, ok := nameListTmp[name]; ok {
				return nil, pgerror.NewErrorf(pgcode.Syntax,
					"multiple assignments to the same column in an NotMatched")
			}
			nameIsLegal := false
			for _, colName := range colNames {
				if colName == name {
					nameIsLegal = true
					break
				}
			}
			if !nameIsLegal {
				return nil, pgerror.NewErrorf(pgcode.Syntax,
					"%s in NotMatched NameList does no exist", name)
			}
			// 加入本条notMatch的map中
			nameListTmp[name] = struct{}{}
			// 19.11.23 将Namelist并集加入
			if _, ok := containNamesNm[name]; !ok {
				containNamesNm[name] = struct{}{}
			}
		}
	}
	// 调整unionNmCols的顺序为A表属性顺序
	for _, colName := range colNames {
		if !hasPrimaryKey && string(colName) == rowidName {
			continue
		}
		if _, ok := containNamesNm[colName]; ok {
			unionNmCols = append(unionNmCols, colName)
		}
	}

	return unionNmCols, nil
}

// AppendHashnum appends hashnum column when this is a merge stmt related to hashpartition table
func (oc *OnConflict) AppendHashnum(nm *NmCondAndOp, hasCol bool, nmIdx int) {
	hasHashnumA := false
	hasHashnumB := false
	if nm.ColNames == nil {
		hasHashnumA = true
	}
	for _, val := range nm.ColNames {
		if val.String() == "hashnum" && oc.MergeHashCondition == 1 {
			hasHashnumA = true
			break
		}
	}

	for _, val := range nm.Operation[0] {
		tmp := strings.Split(val.String(), ".")
		name := tmp[len(tmp)-1]
		if name == "hashnum" && oc.MergeHashCondition == 1 {
			hasHashnumB = true
			break
		}
	}

	//if !hasHashnumA && !hasHashnumB
	if !hasHashnumA && hasCol {
		nm.ColNames = append(nm.ColNames, Name("hashnum"))
		oc.NotMatchList[nmIdx].ColNames = append(oc.NotMatchList[nmIdx].ColNames, Name("hashnum"))
	}

	if !hasHashnumB {
		parts := strings.Split(nm.Operation[0][0].String(), ".")
		hashnum := UnresolvedName{}
		switch len(parts) {
		case 1:
			hashnum = UnresolvedName{
				NumParts: len(parts),
				Star:     false,
				Parts:    [4]string{"hashnum"},
			}
		case 2:
			hashnum = UnresolvedName{
				NumParts: len(parts),
				Star:     false,
				Parts:    [4]string{"hashnum", parts[0]},
			}
		case 3:
			hashnum = UnresolvedName{
				NumParts: len(parts),
				Star:     false,
				Parts:    [4]string{"hashnum", parts[1], parts[0]},
			}
		case 4:
			hashnum = UnresolvedName{
				NumParts: len(parts),
				Star:     false,
				Parts:    [4]string{"hashnum", parts[2], parts[1], parts[0]},
			}
		}
		nm.Operation[0] = append(nm.Operation[0], &hashnum)
	}
}

//Signal means when matched/not matched then signal
type Signal struct {
	SQLState    *string
	MessageText *string
}

const (
	//NothingOperation represents no operation in merge stmt
	NothingOperation = iota
	//DeleteOperation represents delete operation in merge stmt
	DeleteOperation
	//SignalOperation represents throwing a signal in merge stmt
	SignalOperation
	//NotInsertOperation represents not insert in merge stmt
	NotInsertOperation
)

// AdditionalExpr represents above operation mark in merge stmt
type AdditionalExpr struct {
	Expr
	OperationType int
	Sig           *Signal
	typeAnnotation
}

func (expr *AdditionalExpr) String() string {
	panic("implement AdditionalExpr string()")
}

//Format for AdditionalExpr
func (expr *AdditionalExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("THIS IS AdditionalExpr")
}
