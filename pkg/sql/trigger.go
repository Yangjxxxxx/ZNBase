package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

type triggerKey struct {
	name string
}

//Store triggers in memory
func (p *planner) renameTrigger(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, newName string, oldName string,
) error {
	oldKey := sqlbase.MakeTrigMetadataKey(tableDesc.TableDescriptor.ID, triggerKey{oldName}.name)
	newKey := sqlbase.MakeTrigMetadataKey(tableDesc.TableDescriptor.ID, triggerKey{newName}.name)
	//search for oldKeys value
	kvs, err := p.txn.Get(ctx, oldKey)
	//to search for if newKey exists
	kvs2, err := p.txn.Get(ctx, newKey)
	if err != nil || kvs.Value == nil {
		return fmt.Errorf("oldtrigger's name not exists")
	}
	if err != nil || kvs2.Value != nil {
		return fmt.Errorf("newtrigger's name already exists")
	}
	descID := tableDesc.GetID()
	//to copy the oldKeys value to newKey
	b := &client.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, descID)
		log.VEventf(ctx, 2, "Del %s", oldKey)
	}
	kvs.Value.ClearChecksum()
	b.CPut(newKey, kvs.Value, nil)
	b.Del(oldKey)
	err = p.txn.Run(ctx, b)
	if err != nil {
		return err
	}
	return nil
}
