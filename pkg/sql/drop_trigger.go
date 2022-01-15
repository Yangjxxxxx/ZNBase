package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
)

type dropTriggerNode struct {
	TableDesc *sqlbase.MutableTableDescriptor
	Trigname  string
	n         *tree.DropTrigger
}

//to make drop trigger plan node and check
func (p *planner) DropTrigger(ctx context.Context, n *tree.DropTrigger) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support trigger now!")
	}
	query1 := "select * from system.triggers where name='" + n.Trigname + "'"
	row1, err := p.ExecCfg().InternalExecutor.QueryRow(ctx, "search trigger", p.Txn(), string(query1))
	if len(row1) == 0 {
		if n.IfExists {
			return newZeroNode(nil /* columns */), nil
		}
	}
	if n.Tablename.TableName == "" {
		return nil, errEmptyTriggerName
	}
	if n.Trigname == "" {
		return nil, errEmptyTriggerName
	}

	//get droppedDesc to check drop trigger privilege
	droppedDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Tablename, !n.IfExists, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if droppedDesc == nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, droppedDesc, privilege.TRIGGER); err != nil {
		return nil, err
	}

	return &dropTriggerNode{n: n, Trigname: string(n.Trigname), TableDesc: droppedDesc}, nil
}

//start the drop trigger node,first found the triggers id and values,then delete the key ,then the system will Automatic memory recovery
func (n *dropTriggerNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p
	Key := sqlbase.MakeTrigMetadataKey(n.TableDesc.TableDescriptor.ID, triggerKey{n.Trigname}.name)
	kvs, err := p.txn.Get(ctx, Key)
	if err != nil || kvs.Value == nil {
		if n.n.IfExists {
			return nil
		}
		return fmt.Errorf("trigger's name not exists")
	}

	b := &client.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Del %s", Key)
		log.VEventf(ctx, 2, "Del %s", kvs)
	}
	b.Del(Key)
	err = p.txn.Run(ctx, b)
	if err != nil {
		return err
	}
	return nil
}

// Next implements the planNode interface.
func (*dropTriggerNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*dropTriggerNode) Close(context.Context) {}

// Close implements the planNode interface.
func (*dropTriggerNode) Values() tree.Datums { return tree.Datums{} }
