package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
)

type renameTriggerNode struct {
	TableDesc *sqlbase.MutableTableDescriptor
	oldName   string
	newName   string
}

//to make the renametrigger plan node
func (p *planner) RenameTrigger(ctx context.Context, n *tree.RenameTrigger) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support trigger now!")
	}
	toRequire := requireTableOrViewDesc
	Tablename := &n.Tablename
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, Tablename, !n.IfExists, toRequire)
	//avoid empty
	if n.Trigname == "" || n.Newname == "" {
		return nil, errEmptyTriggerName
	}
	//avoid table not exist
	if tableDesc == nil {
		return nil, fmt.Errorf("table '%s' does not exist", string(n.Tablename.TableName))
	}
	err = nil

	//check the TRIGGER privilege
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.TRIGGER); err != nil {
		return nil, err
	}
	//check Jurisdiction
	//if err := p.RequireAdminRole(ctx, "ALTER TRIGGER ... RENAME"); err != nil {
	//	return nil, err
	//}

	if err != nil {
		return nil, err
	}

	if n.Trigname == n.Newname {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}
	//return plan node
	return &renameTriggerNode{
		TableDesc: tableDesc,
		oldName:   string(n.Trigname),
		newName:   string(n.Newname),
	}, nil
}

//start plan node
func (n *renameTriggerNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	TableDesc := n.TableDesc
	//check if new trigger exists
	return p.renameTrigger(ctx, TableDesc, n.newName, n.oldName)
}

// Next implements the planNode interface.
func (n *renameTriggerNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (n *renameTriggerNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (n *renameTriggerNode) Close(context.Context) {}
