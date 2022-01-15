package sqlbase

import (
	"context"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// RemoveObjectNamespaceEntry removes entries from both the deprecated and
// new system.namespace table (if one exists).
func RemoveObjectNamespaceEntry(
	ctx context.Context, txn *client.Txn, parentID ID, name string, KVTrace bool,
) error {
	b := txn.NewBatch()
	var toDelete []DescriptorKey
	// The (parentID, name) mapping could be in either the new system.namespace
	// or the deprecated version. Thus we try to remove the mapping from both.
	if parentID == keys.RootNamespaceID {
		toDelete = append(toDelete, NewDatabaseKey(name))
	} else {
		toDelete = append(toDelete, NewSchemaKey(parentID, name))
	}
	for _, delKey := range toDelete {
		if KVTrace {
			log.VEventf(ctx, 2, "Del %s", delKey)
		}
		b.Del(delKey.Key())
	}
	return txn.Run(ctx, b)
}
