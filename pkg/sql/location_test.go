package sql

import (
	"context"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestLocationMapChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var td *sqlbase.MutableTableDescriptor
	var cfg *config.SystemConfig
	var p *planner
	err := p.LocationMapChange(ctx, td, cfg)
	expexterr := "TableDescriptor cannot nil in LocationMapChange"
	if !testutils.IsError(err, expexterr) {
		t.Errorf("#%d: expected error matching %q, but got %v", 1, expexterr, err)
		// don't test get
	}
}

func TestUpdateGetLocationMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg := config.NewSystemConfig()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(ExecutorConfig)
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		client.NewTxn(ctx, db, s.NodeID(), client.RootTxn),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	tableDesc := &sqlbase.MutableTableDescriptor{
		TableDescriptor: TableDescriptor{
			ID: 12,
		},
	}
	// todo inspur add testCase
	testCases := []struct {
		td          *sqlbase.MutableTableDescriptor
		locationMap *roachpb.LocationMap
		error       string
	}{
		// case 0
		{
			td: tableDesc,

			locationMap: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					1: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
			},
			error: "",
		},
		// case 1
		{
			td: nil,
			locationMap: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					1: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
			},
			error: "TableDescriptor cannot nil in LocationMapChange",
		},
		// case 2
		{
			td:          tableDesc,
			locationMap: nil,
			error:       "",
		},
		// case 3
		{
			td: tableDesc,

			locationMap: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{
					Spaces: []string{"table"},
				},
				IndexSpace:     map[uint32]*roachpb.LocationValue{},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
			},
			error: "",
		},
	}

	for i, c := range testCases {
		// test update
		if err := internalPlanner.(*planner).UpdateLocationMap(ctx, c.td, cfg, c.locationMap); err != nil {
			if !testutils.IsError(err, c.error) {
				t.Errorf("#%d: expected error matching %q, but got %v", i, c.error, err)
			}
			// don't test get
			continue
		}

		if c.locationMap == nil {
			c.locationMap = roachpb.NewLocationMap()
		}
		// test get
		if expect, err := internalPlanner.(*planner).GetLocationMap(ctx, c.td.TableDescriptor.ID); err != nil {
			if !testutils.IsError(err, c.error) {
				t.Errorf("#%d: expected error matching %q, but got %v", i, c.error, err)
			}
		} else if !expect.Equal(c.locationMap) {
			t.Errorf("%d: expected %q, got %v", i, c.locationMap, expect)
		}
	}

	// SystemConfig nil
	{
		err := internalPlanner.(*planner).UpdateLocationMap(context.TODO(), tableDesc, nil, nil)
		err1 := "SystemConfig cannot nil in UpdateLocationCache"
		if !testutils.IsError(err, err1) {
			t.Errorf("expected error matching %q, but got %v", err1, err)
		}
	}
	// txn ERROR
	{
		ctx := context.Background()

		mc := hlc.NewManualClock(1)
		clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
		db := client.NewDB(
			testutils.MakeAmbientCtx(),
			client.MakeMockTxnSenderFactory(
				func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					err := roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}, nil)
					return nil, roachpb.NewErrorWithTxn(err, nil)
				}),
			clock)
		txn := client.NewTxn(ctx, db, 0 /* gatewayNodeID */, client.RootTxn)
		internalPlanner, cleanup := NewInternalPlanner(
			"test",
			txn,
			security.RootUser,
			&MemoryMetrics{},
			&execCfg,
		)
		defer cleanup()
		_, err := internalPlanner.(*planner).GetLocationMap(ctx, sqlbase.ID(53))
		err1 := "ReadWithinUncertaintyIntervalError: .*"
		if !testutils.IsError(err, err1) {
			t.Errorf("expected error matching %q, but got %v", err1, err)
		}
	}
}

func TestSetGetLocationRaw(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Plan a statement.
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		client.NewTxn(ctx, db, s.NodeID(), client.RootTxn),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	txn := internalPlanner.(*planner).Txn()
	defer cleanup()

	// todo inspur add testCase
	testCases := []struct {
		id          sqlbase.ID
		locationMap *roachpb.LocationMap
		err         string
	}{
		// 0
		{
			id: 53,
			locationMap: &roachpb.LocationMap{
				TableSpace: &roachpb.LocationValue{},
				IndexSpace: map[uint32]*roachpb.LocationValue{
					1: {},
				},
				PartitionSpace: []*roachpb.LocationMap_PartitionSpace{},
			},
			err: "",
		},
	}
	for i, c := range testCases {
		// test set
		if err := setLocationRaw(ctx, txn, c.id, c.locationMap); err != nil {
			if !testutils.IsError(err, c.err) {
				t.Errorf("#%d: expected error matching %q, but got %v", i, c.err, err)
				// don't test get
			}
			continue
		}
		// test get
		if expect, err := getLocationRaw(ctx, txn, c.id); err != nil {
			if !testutils.IsError(err, c.err) {
				t.Errorf("#%d: expected error matching %q, but got %v", i, c.err, err)
			}
		} else if !expect.Equal(c.locationMap) {
			t.Errorf("%d: expected %q, got %v", i, c.locationMap, expect)
		}
	}

	// getLocationRaw KV ERROR
	{
		id := uint32(53)
		key := config.MakeLocationKey(id)
		kv, _ := txn.Get(ctx, key)
		kv.Value = &roachpb.Value{
			RawBytes: []byte{193, 143},
		}
		_ = txn.Put(ctx, key, kv.Value)
		_, err := getLocationRaw(ctx, txn, sqlbase.ID(id))
		err1 := "value type is not BYTES: UNKNOWN"
		if !testutils.IsError(err, err1) {
			t.Errorf("expected error matching %q, but got %v", err1, err)
		}
	}
}

func TestUpdateIndexLocationNums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCaseStruct struct {
		td              *sqlbase.TableDescriptor
		id              *sqlbase.IndexDescriptor
		newLocationNums int32
		err             string
	}

	var testCase = []testCaseStruct{
		{
			err: "TabDescriptor or IndexDescriptor should not be nil",
		},
		{
			td: &sqlbase.TableDescriptor{
				Name: "play1",
			},
			id: &sqlbase.IndexDescriptor{
				Name: "play1",
			},
			newLocationNums: int32(-5),
			err:             "LocationNums can not below zero",
		},
		{
			td: &sqlbase.TableDescriptor{
				Name:         "play2",
				LocationNums: int32(1),
			},
			id: &sqlbase.IndexDescriptor{
				Name:         "play2",
				LocationNums: int32(5),
			},
			newLocationNums: int32(6),
			err:             "LocationNums can not below zero",
		},
		{
			td: &sqlbase.TableDescriptor{
				Name:         "play3",
				LocationNums: int32(10),
			},
			id: &sqlbase.IndexDescriptor{
				Name:         "play3",
				LocationNums: int32(5),
			},
			newLocationNums: int32(6),
			err:             "",
		},
	}
	for i, oneCase := range testCase {
		if err := updateIndexLocationNums(oneCase.td, oneCase.id, oneCase.newLocationNums); err != nil {
			if !testutils.IsError(err, oneCase.err) {
				t.Errorf("#%d: expected error matching %q, but got %v", i, oneCase.err, err)
				//
			}
		}
	}
}

func TestUpdateTableLocationNums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var testCase = []struct {
		td              *sqlbase.TableDescriptor
		newLocationNums int32
		err             string
	}{
		{
			//newLocationNums: int32 (5),
			err: "TableDescriptor should not nil",
		},
		{
			td: &sqlbase.TableDescriptor{
				Name:         "play1",
				LocationNums: int32(2),
			},
			newLocationNums: int32(-5),
			err:             "LocationNums can not below zero",
		},
		{
			td: &sqlbase.TableDescriptor{
				Name:         "play1",
				LocationNums: int32(2),
			},
			newLocationNums: int32(5),
		},
		{
			td: &sqlbase.TableDescriptor{
				Name: "play2",
			},
			newLocationNums: int32(5),
		},
	}
	for i, oneCase := range testCase {
		if err := updateTableLocationNums(oneCase.td, oneCase.newLocationNums); err != nil {
			if !testutils.IsError(err, oneCase.err) {
				t.Errorf("#%d: expected error matching %q, but got %v", i, oneCase.err, err)
				//
			}
		}
	}

}

func TestChangeLocationNums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	newLocationSpace5 := &roachpb.LocationValue{
		Spaces: []string{"5"},
	}
	oldLocationSpace5 := &roachpb.LocationValue{
		Spaces: []string{"5"},
	}
	oldLocationSpace6 := &roachpb.LocationValue{
		Spaces: []string{"6"},
	}
	_, _ = changeLocationNums(newLocationSpace5, oldLocationSpace5)
	expectedbool := false
	expectedint := 1
	//if outbool != expectedbool || expectedint != expectedint {
	//	t.Errorf("when newLocationSpace = oldLocationSpace,expected got %t and %d, but got %t and %d", expectedbool, expectedint, outbool, outint)
	//}

	outbool, outint := changeLocationNums(newLocationSpace5, &roachpb.LocationValue{})
	expectedbool = true
	expectedint = 1
	if outbool != expectedbool {
		t.Errorf("expected got %t and %d, but got %t and %d", expectedbool, expectedint, outbool, outint)
	}

	outbool, outint = changeLocationNums(&roachpb.LocationValue{}, oldLocationSpace5)
	expectedbool = true
	expectedint = -1
	if outbool != expectedbool {
		t.Errorf("expected got %t and %d, but got %t and %d", expectedbool, expectedint, outbool, outint)
	}

	outbool, outint = changeLocationNums(newLocationSpace5, oldLocationSpace6)
	expectedbool = true
	expectedint = 0
	if outbool != expectedbool {
		t.Errorf("expected got %t and %d, but got %t and %d", expectedbool, expectedint, outbool, outint)
	}
}
