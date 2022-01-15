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
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
)

// CursorAction represents cursor action state
type CursorAction int8

// CursorDirection represents FETCH/MOVE action specific operation
type CursorDirection int8

// RowMax represent cursor FETCH/MOVE max operation count
const RowMax = int64(^uint64(0) >> 1)

// Cursor action state const
const (
	DECLARE CursorAction = iota
	OPEN
	FETCH
	MOVE
	CLOSE
)

// Cursor FETCH/MOVE action specific operation constant.
const (
	NEXT CursorDirection = iota
	PRIOR
	FIRST
	LAST
	ABSOLUTE
	RELATIVE
	FORWARD
	BACKWARD
)

// CursorTxn state
const (
	PENDING = "PENDING"
	COMMIT  = "COMMIT"
)

// DeclareCursor represents a cursor use defined statement
type DeclareCursor struct {
	Cursor     Name
	Select     *Select    // cursor select stmt
	CursorVars CursorVars // bind variable
}

// ShowCursor represents a show cursor statement
type ShowCursor struct {
	CursorName Name
	ShowAll    bool
}

// CursorVar 用于从yacc接收客户端数据，也用于存储变量
type CursorVar struct {
	CurVarName Name
	// CurVarColType 定义cursor时，客户端输入类型
	CurVarColType coltypes.T
	// CurVarDataExpr 打开cursor时的表达式
	// 例： open cur(s='Wang') curVarDataExpr为 Wang
	CurVarDataExpr Expr
	// CurVarType 存储变量时候的数据类型
	CurVarType types.T
	// CurVarDatum 根据CurVarType解析出的对应Datum类型
	CurVarDatum Datum
}

// CursorVars is a slice of CursorVar values.
type CursorVars []*CursorVar

// CursorAccess 代表游标操作
type CursorAccess struct {
	CursorName Name
	// 游标动作
	Action CursorAction
	// 滚动条目数
	Rows int64
	// 滚动方向
	Direction CursorDirection
	// bind select sql
	Select *Select
	// fetch into 变量名
	VariableNames []Name
	// 参数游标declare / open
	CurVars CursorVars
}

// CurrentOfExpr 用于Update和Delete where Current cursor功能
type CurrentOfExpr struct {
	CursorName Name
}

func (c *CurrentOfExpr) String() string {
	return c.CursorName.String()
}

// Format implements the NodeFormatter interface.
func (c *CurrentOfExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("CURRENT OF ")
	ctx.WriteString(c.String())
}

// Walk implements the Expr interface.
func (c *CurrentOfExpr) Walk(visitor Visitor) Expr { return c }

// TypeCheck implements the Expr interface.
// TODO(lhz): Can nil be returned?
func (c *CurrentOfExpr) TypeCheck(
	ctx *SemaContext, desired types.T, useOrigin bool,
) (TypedExpr, error) {
	return nil, nil
}

// Format implements the NodeFormatter interface.
func (d *DeclareCursor) Format(ctx *FmtCtx) {
	ctx.WriteString("DECLARE ")
	ctx.FormatNode(&d.Cursor)
	if d.Select != nil {
		ctx.WriteString(" CURSOR FOR ")
		ctx.FormatNode(d.Select)
	} else {
		ctx.WriteString(" REFCURSOR")
	}
}

// Format implements the NodeFormatter interface.
func (s *ShowCursor) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if s.ShowAll {
		ctx.WriteString(string(s.CursorName))
		ctx.WriteString("CURSOR ")
	} else {
		ctx.WriteString("CURSORS ")
	}
}

// Format implements the NodeFormatter interface.
func (c *CursorAccess) Format(ctx *FmtCtx) {

	switch c.Action {
	case OPEN:
		ctx.WriteString("OPEN ")
		ctx.WriteString(string(c.CursorName))
	case FETCH:
		ctx.WriteString("FETCH IN ")
		ctx.WriteString(string(c.CursorName))
	case MOVE:
		ctx.WriteString("MOVE IN ")
		ctx.WriteString(string(c.CursorName))
	case CLOSE:
		ctx.WriteString("CLOSE ")
		ctx.WriteString(string(c.CursorName))
	}
}
