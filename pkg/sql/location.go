package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

func (p *planner) LocationMapChange(
	ctx context.Context, td *sqlbase.MutableTableDescriptor, cfg *config.SystemConfig,
) error {
	if td == nil {
		return fmt.Errorf("TableDescriptor cannot nil in LocationMapChange")
	}

	locationMap, err := GetLocationMapICL(&td.TableDescriptor)
	if err != nil {
		return err
	}

	return p.UpdateLocationMap(ctx, td, cfg, locationMap)
}

//LocationMapChange update or delete system.location
func (p *planner) UpdateLocationMap(
	ctx context.Context,
	td *sqlbase.MutableTableDescriptor,
	cfg *config.SystemConfig,
	locationMap *roachpb.LocationMap,
) error {
	if td == nil {
		return fmt.Errorf("TableDescriptor cannot nil in LocationMapChange")
	} else if cfg == nil {
		return fmt.Errorf("SystemConfig cannot nil in UpdateLocationCache")
	}

	if err := setLocationRaw(ctx, p.txn, td.ID, locationMap); err != nil {
		return err
	}
	// update config.system locationCache
	cfg.UpdateLocationCache(uint32(td.ID), locationMap)
	return nil
}

// getLocationMap looks up getLocationRaw with init
func (p *planner) GetLocationMap(ctx context.Context, id sqlbase.ID) (*roachpb.LocationMap, error) {
	locationMap, err := getLocationRaw(ctx, p.txn, id)
	if err != nil {
		return nil, err
	} else if locationMap == nil {
		locationMap = roachpb.NewLocationMap()
	} else if locationMap.IndexSpace == nil {
		locationMap.IndexSpace = map[uint32]*roachpb.LocationValue{}
	}
	return locationMap, nil
}

// getLocationRaw looks up the LocationMap with the given ID
func getLocationRaw(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (*roachpb.LocationMap, error) {
	kv, err := txn.Get(ctx, config.MakeLocationKey(uint32(id)))
	if err != nil {
		return nil, err
	}
	if kv.Value == nil {
		return nil, nil
	}
	locationMap := roachpb.NewLocationMap()
	if err := kv.ValueProto(locationMap); err != nil {
		return nil, err
	}
	return locationMap, nil
}

// setLocationRaw update or delete the LocationMap with the given ID
func setLocationRaw(
	ctx context.Context, txn *client.Txn, id sqlbase.ID, locationMap *roachpb.LocationMap,
) error {
	if locationMap == nil {
		return txn.Del(ctx, config.MakeLocationKey(uint32(id)))
	}
	value, err := protoutil.Marshal(locationMap)
	if err != nil {
		return err
	}
	return txn.Put(ctx, config.MakeLocationKey(uint32(id)), value)
}

// updateIndexLocationNums by partitioning or locate in
func updateIndexLocationNums(
	td *sqlbase.TableDescriptor, id *sqlbase.IndexDescriptor, newLocationNums int32,
) error {
	if td == nil || id == nil {
		return fmt.Errorf("TabDescriptor or IndexDescriptor should not be nil")
	}
	if newLocationNums < 0 {
		return fmt.Errorf("LocationNums can not below zero")
	}
	td.LocationNums -= id.LocationNums
	if td.LocationNums < 0 {
		return fmt.Errorf("LocationNums can not below zero")
	}
	id.LocationNums = newLocationNums
	td.LocationNums += id.LocationNums
	return nil
}

// updateTableLocationNums by table locate in
func updateTableLocationNums(td *sqlbase.TableDescriptor, newLocationNums int32) error {
	if td == nil {
		return fmt.Errorf("TableDescriptor should not nil")
	}
	if newLocationNums < 0 {
		return fmt.Errorf("LocationNums can not below zero")
	}
	td.LocationNums = newLocationNums
	return nil
}

// ChangeLocationNums changes the locationSpace count result
func changeLocationNums(newLocationSpace, oldLocationSpace *roachpb.LocationValue) (bool, int32) {
	removeSpaceNull(newLocationSpace)
	if oldLocationSpace.Equal(newLocationSpace) {
		return false, 0
	} else if oldLocationSpace == nil || len(oldLocationSpace.Spaces) == 0 {
		return true, 1
	} else if newLocationSpace == nil || len(newLocationSpace.Spaces) == 0 {
		return true, -1
	}
	return true, 0
}
