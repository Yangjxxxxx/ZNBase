package sql

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

// 替换exec_util.go的isAsOf功能。首先检查快照子句，然后再检查asOf子句
func (p *planner) asOfSnapshot(ctx context.Context, stmt tree.Statement) (*hlc.Timestamp, error) {
	switch s := stmt.(type) {
	case *tree.Select:
		selStmt := s.Select
		var parenSel *tree.ParenSelect
		var ok bool
		for parenSel, ok = selStmt.(*tree.ParenSelect); ok; parenSel, ok = selStmt.(*tree.ParenSelect) {
			selStmt = parenSel.Select.Select
		}

		sc, ok := selStmt.(*tree.SelectClause)
		if !ok {
			return nil, nil
		}
		if sc.From == nil {
			return nil, nil
		}
		if sc.From.AsSnapshot.Snapshot == "" {
			return p.isAsOf(stmt)
		}
		if len(sc.From.Tables) > 1 {
			//使用快照子句只能查询一个表
			return nil, errors.Errorf("Only one table can be queried using 'AS OF SNAPSHOT' clause.")
		}
		aliasTab, ok := sc.From.Tables[0].(*tree.AliasedTableExpr)
		if !ok {
			//只支持表名，不能使用Join、子查询做为表
			return nil, errors.Errorf("Only query AliasedTable using 'AS OF SNAPSHOT' clause.")
		}
		tblName, ok := aliasTab.Expr.(*tree.TableName)
		if !ok {
			return nil, errors.Errorf("'%v' is not a TableName.", aliasTab.Expr)
		}
		asof, err := p.getAsOfSnapshot(ctx, tblName, sc.From.AsSnapshot.Snapshot)
		if err != nil {
			return nil, err
		}
		//nVal := tree.NumVal{OrigString:string(asof)}
		expr, _ := parser.ParseExpr(fmt.Sprintf("%d", asof))
		sc.From.AsOf = tree.AsOfClause{Expr: expr} //转换为AsOf子句
		return &hlc.Timestamp{WallTime: asof}, nil
	default:
		return p.isAsOf(stmt)
	}
	//return nil, nil
}
func (p *planner) getAsOfSnapshot(
	ctx context.Context, tbl *tree.TableName, asSnapshot tree.Name,
) (int64, error) {
	//查询数据表对象的objectId
	tDesc, err := ResolveExistingObject(ctx, p, tbl, true /*required*/, 1)
	if err != nil {
		return -1, errors.Errorf("Can not get object id of '%s'", tbl.String())
	}
	opName := "SHOW SNAPSHOT"
	row, err := p.ExecCfg().InternalExecutor.QueryRow(
		ctx,
		opName,
		nil,
		`select name,type,object_id,sub_id,asof from system.snapshots where name=$1 and object_id=$2`,
		asSnapshot.String(),
		tDesc.ID,
	)
	if err != nil {
		return -1, err
	}
	if row == nil {
		return -1, errors.Errorf("The table '%s' have no snapshot named '%s'", tbl.String(), asSnapshot.String())
	}
	ts := *row[4].(*tree.DTimestamp)
	return ts.UnixNano(), nil
}
