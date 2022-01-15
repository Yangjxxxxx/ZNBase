package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// SetReplica completes the transformation of the table between a regular table and a replicated table.
// Privileges: CREATE on schema and DROP on table.
func (p *planner) SetReplica(ctx context.Context, n *tree.SetReplica) (planNode, error) {

	var parentDesc sqlbase.DescriptorProto
	dbDesc, err := p.ResolveUncachedDatabase(ctx, &n.Table)
	if err != nil {
		return nil, err
	}
	parentDesc = dbDesc
	scName := string(n.Table.SchemaName)
	if dbDesc.ID != keys.SystemDatabaseID {
		if isInternal := CheckVirtualSchema(scName); isInternal {
			return nil, fmt.Errorf("schema cannot be modified: %q", n.Table.SchemaName.String())
		}
		scDesc, err := dbDesc.GetSchemaByName(scName)
		if err != nil {
			return nil, err
		}
		parentDesc = scDesc
	}
	//look up if table desc is existed
	tableDesc, err := p.ResolveUncachedTableDescriptor(ctx, &n.Table, true, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, parentDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	if !n.Disable == tableDesc.ReplicationTable {
		return nil, errors.Errorf("table %v replication state already changed", tableDesc.ID)
	}

	tableDesc.ReplicationTable = !n.Disable
	if err := p.writeSchemaChange(ctx, sqlbase.NewMutableExistingTableDescriptor(*tableDesc.TableDesc()), sqlbase.InvalidMutationID); err != nil {
		return nil, err
	}
	zoneSpecifier := tree.ZoneSpecifier{}
	zoneSpecifier.TableOrIndex.Table = n.Table
	options := make(map[tree.Name]optionValue)
	var value int
	if !n.Disable {
		request := &serverpb.NodesRequest{}
		nodes, err := p.execCfg.StatusServer.Nodes(ctx, request)
		if err != nil {
			return nil, err
		}
		value = len(nodes.Nodes)
	} else {
		value, err = GetNumReplicas(ctx, p.txn, uint32(0))
		if err != nil {
			return nil, err
		}
	}
	optionValue := optionValue{
		explicitValue: tree.NewDInt(tree.DInt(value)),
	}
	options["num_replicas"] = optionValue
	return &setZoneConfigNode{
		zoneSpecifier: zoneSpecifier,
		yamlConfig:    nil,
		options:       options,
		setDefault:    false,
	}, nil
}

// GetNumReplicas returns the number of replicas of the settings for the specified zone.default 3
func GetNumReplicas(ctx context.Context, txn *client.Txn, id uint32) (int, error) {
	_, zone, _, err := GetZoneConfigInTxn(ctx, txn,
		id, nil, "", false /* getInheritedDefault */)
	if err == errNoZoneConfigApplies {
		defZone := config.DefaultZoneConfig()
		zone = &defZone
	} else if err != nil {
		return 0, err
	}
	value := int(*zone.NumReplicas)
	return value, nil
}
