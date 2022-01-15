package sql

import (
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

//when tables id changed,copy the old TableIDs value to the new TablesID
func (p *planner) changeID(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	newID uint32,
	oldID uint32,
	Name string,
) error {
	oldKey := sqlbase.MakeTrigMetadataKey(sqlbase.ID(oldID), Name)
	newKey := sqlbase.MakeTrigMetadataKey(sqlbase.ID(newID), Name)
	kvs, _ := p.txn.Get(ctx, oldKey)
	descID := tableDesc.GetID()
	//by batch first put,second delete the old
	b := &client.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, descID)
		log.VEventf(ctx, 2, "Del %s", oldKey)
	}
	kvs.Value.ClearChecksum()
	b.CPut(newKey, kvs.Value, nil)
	b.Del(oldKey)
	err := p.txn.Run(ctx, b)
	return err
}
