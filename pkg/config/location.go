package config

import (
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
)

func getLocateSpace(key roachpb.RKey, locationMap *roachpb.LocationMap) *roachpb.LocationValue {
	if locationMap == nil {
		return nil
	}
	// todo use binarySearch
	var indexID *uint32
	var res *roachpb.LocationValue
	for _, p := range locationMap.PartitionSpace {
		if p.StartKey.Less(key) || p.StartKey.Equal(key) {
			if key.Less(p.EndKey) {
				res = p.Space
			}
			indexID = &p.IndexID
		} else {
			break
		}
	}
	if res != nil {
		return res
	}
	if indexID != nil {
		for k, v := range locationMap.IndexSpace {
			if k == *indexID {
				return v
			}
		}
	}
	return locationMap.TableSpace
}

func (s *SystemConfig) getLocationRaw(id uint32) (*roachpb.LocationMap, error) {
	kv := s.GetValue(MakeLocationKey(id))
	if kv == nil {
		return nil, nil
	}
	locationMap := roachpb.NewLocationMap()
	if err := kv.GetProto(locationMap); err != nil {
		return nil, err
	}
	return locationMap, nil
}

// GetLocateSpace  Start Key to get LocateSpace
func (s *SystemConfig) GetLocateSpace(key roachpb.RKey) (*roachpb.LocationValue, error) {
	id, _, ok := DecodeObjectID(key)
	if !ok || id <= keys.MaxReservedDescID {
		return nil, nil
	}
	if res, ok := func() (*roachpb.LocationValue, bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if locationMap, ok := s.mu.locateSpaceCache[id]; ok {
			return getLocateSpace(key, locationMap), true
		}
		return nil, false
	}(); ok {
		return res, nil
	}

	var err error
	var locationMap *roachpb.LocationMap
	if locationMap, err = s.getLocationRaw(id); err == nil {
		if locationMap != nil {
			s.setLocateSpace(id, locationMap)
		}
		return getLocateSpace(key, locationMap), nil
	}
	return nil, err
}

// GetLocateMapForObject  Start Key to get LocateSpace
func (s *SystemConfig) GetLocateMapForObject(id uint32) (*roachpb.LocationMap, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()
	if locationMap, ok := s.mu.locateSpaceCache[id]; ok {
		return locationMap, nil
	}

	locationMap, err := s.getLocationRaw(id)
	if err == nil {
		return locationMap, nil
	}
	return nil, err
}

// SetLocateSpace
func (s *SystemConfig) setLocateSpace(id uint32, locationMap *roachpb.LocationMap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.locateSpaceCache[id] = locationMap
}

// deleteLocateSpace
func (s *SystemConfig) deleteLocateSpace(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mu.locateSpaceCache, id)
}

// UpdateLocationCache All update
func (s *SystemConfig) UpdateLocationCache(id uint32, locationMap *roachpb.LocationMap) {
	if locationMap == nil {
		s.deleteLocateSpace(id)
	} else {
		s.setLocateSpace(id, locationMap)
	}
}
