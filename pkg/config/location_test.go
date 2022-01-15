package config

import (
	"testing"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestCapGetLocateSpace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := NewSystemConfig()

	// todo inspur add testCase
	testCases := []struct {
		key         roachpb.RKey
		locationMap *roachpb.LocationMap
		expected    string
	}{
		//0
		{
			key:      []byte{0},
			expected: "",
		},

		//1
		{
			key: []byte{193, 138, 138},
			locationMap: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					1: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
					{
						StartKey: roachpb.RKey([]byte{193, 138, 138}),
						EndKey:   roachpb.RKey([]byte{193, 138, 139}),
						Space: &roachpb.LocationValue{
							Spaces: []string{"test"},
						},
						IndexID: 0,
					},
				},
			},
			expected: "test",
		},

		//2
		{
			key:      []byte{194, 138, 138},
			expected: "",
		},
	}

	for i, c := range testCases {
		expectS := ""
		if c.locationMap != nil {
			cfg.UpdateLocationCache(57, c.locationMap)
			expect, _ := cfg.GetLocateSpace(c.key)
			for _, v := range expect.Spaces {
				expectS += v
			}
			if !(expectS == c.expected) {
				t.Errorf("%d: expected to get %s, got %s", i, c.expected, expect)
			}
		} else {
			expect, _ := cfg.GetLocateSpace(c.key)
			if expect != nil {
				for _, v := range expect.Spaces {
					expectS += v
				}
			}
			if !(expectS == c.expected) {
				t.Errorf("%d: expected to get %s, got %s", i, c.expected, expect)
			}
		}
	}

	// TOUCH SystemConfig.GetValue ERROR
	cfg.Values = []roachpb.KeyValue{
		{
			Key:   roachpb.Key{143, 137, 194, 138, 137},
			Value: roachpb.Value{},
		},
	}
	_, err := cfg.GetLocateSpace([]byte{194, 138, 137})
	err1 := "value type is not BYTES: UNKNOWN"
	if !testutils.IsError(err, err1) {
		t.Errorf("expected error matching %q, but got %v", err1, err)
	}
}

func TestGetLocateSpace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// todo inspur add testCase
	testCases := []struct {
		key         roachpb.RKey
		locationMap roachpb.LocationMap
		expected    string
	}{
		{
			// 0
			key: []byte{1},
			locationMap: roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					0: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
					{
						StartKey: roachpb.RKey([]byte{0}),
						EndKey:   roachpb.RKey([]byte{2}),
						Space: &roachpb.LocationValue{
							Spaces: []string{"test"},
						},
						IndexID: 0,
					},
				},
			},
			expected: "test",
		},
		// 1
		{
			key: []byte{1},
			locationMap: roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{
					Spaces: []string{"tablespace"},
				},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					0: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
					{
						StartKey: roachpb.RKey([]byte{3}),
						EndKey:   roachpb.RKey([]byte{4}),
						Space: &roachpb.LocationValue{
							Spaces: []string{"test"},
						},
						IndexID: 0,
					},
				},
			},
			expected: "tablespace",
		},

		// 2
		{
			key: []byte{2},
			locationMap: roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{
					Spaces: []string{"tablespace"},
				},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					0: {
						Spaces: []string{"indexSpace"},
					},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
					{
						StartKey: roachpb.RKey([]byte{1}),
						EndKey:   roachpb.RKey([]byte{2}),
						Space: &roachpb.LocationValue{
							Spaces: []string{"test"},
						},
						IndexID: 0,
					},
				},
			},
			expected: "indexSpace",
		},
		{
			key:      []byte{3},
			expected: "",
		},
	}

	for i, c := range testCases {
		expect := getLocateSpace(c.key, &c.locationMap)
		expectS := ""
		if expect != nil {
			for _, v := range expect.Spaces {
				expectS += v
			}
		}
		if !(expectS == c.expected) {
			t.Errorf("%d: expected %s, got %s", i, expect, c.expected)
		}
	}
}

func TestUpdateLocationCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg := NewSystemConfig()

	// todo inspur add testCase
	testCases := []struct {
		locationMap *roachpb.LocationMap
		expected    *roachpb.LocationMap
	}{
		// 0 test nil
		{
			locationMap: nil,
			expected:    nil,
		},
		// 1 test empty
		{
			locationMap: &roachpb.LocationMap{
				TableSpace:     &roachpb.LocationValue{},
				IndexSpace:     map[uint32]*roachpb.LocationValue{},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
			},
			expected: &roachpb.LocationMap{
				TableSpace:     &roachpb.LocationValue{},
				IndexSpace:     map[uint32]*roachpb.LocationValue{},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
			},
		},
		// 2 test only PartitionSpace
		{
			locationMap: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					0: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
					{
						StartKey: roachpb.RKey([]byte{0, 1}),
						EndKey:   roachpb.RKey([]byte{0, 2}),
						Space: &roachpb.LocationValue{
							Spaces: []string{"test"},
						},
						IndexID: 0,
					},
				},
			},
			expected: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					0: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
					{
						StartKey: roachpb.RKey([]byte{0, 1}),
						EndKey:   roachpb.RKey([]byte{0, 2}),
						Space: &roachpb.LocationValue{
							Spaces: []string{"test"},
						},
						IndexID: 0,
					},
				},
			},
		},
	}
	for i, c := range testCases {
		cfg.UpdateLocationCache(uint32(i), c.locationMap)
		expect := cfg.mu.locateSpaceCache[uint32(i)]
		if !expect.Equal(c.expected) {
			t.Errorf("%d: expected %q, got %v", i, c.expected, expect)
		}
	}
}

func TestSetDelLocateSpace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := NewSystemConfig()

	// todo inspur add testCase
	testCasesSet := []struct {
		locationMap *roachpb.LocationMap
		expected    *roachpb.LocationMap
	}{
		{
			locationMap: nil,
			expected:    nil,
		},
		{
			locationMap: &roachpb.LocationMap{
				TableSpace:     &roachpb.LocationValue{},
				IndexSpace:     map[uint32]*roachpb.LocationValue{},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
			},
			expected: &roachpb.LocationMap{
				TableSpace:     &roachpb.LocationValue{},
				IndexSpace:     map[uint32]*roachpb.LocationValue{},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
			},
		},
		{
			locationMap: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					0: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
					{
						StartKey: roachpb.RKey([]byte{0, 1}),
						EndKey:   roachpb.RKey([]byte{0, 2}),
						Space: &roachpb.LocationValue{
							Spaces: []string{"test"},
						},
						IndexID: 0,
					},
				},
			},
			expected: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					0: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{
					{
						StartKey: roachpb.RKey([]byte{0, 1}),
						EndKey:   roachpb.RKey([]byte{0, 2}),
						Space: &roachpb.LocationValue{
							Spaces: []string{"test"},
						},
						IndexID: 0,
					},
				},
			},
		},
	}

	for i, c := range testCasesSet {
		cfg.setLocateSpace(uint32(i), c.locationMap)
		expect := cfg.mu.locateSpaceCache[uint32(i)]

		if !expect.Equal(c.expected) {
			t.Errorf("%d: expected %q, got %v", i, c.expected, expect)
		}
	}

	// todo inspur add testCase
	testCasesDel := []uint32{0, 1, 4}

	for i, c := range testCasesDel {
		cfg.deleteLocateSpace(c)
		expect := cfg.mu.locateSpaceCache[c]

		if !expect.Equal(nil) {
			t.Errorf("%d: expected nil, got %v", i, expect)
		}
	}

}
