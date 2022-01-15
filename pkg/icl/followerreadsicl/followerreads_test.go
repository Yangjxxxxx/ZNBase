// Copyright 2019  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package followerreadsicl

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/icl/utilicl"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan/replicastrategy"
	"github.com/znbasedb/znbase/pkg/storage"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/tracing"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

const expectedFollowerReadOffset = -1 * (30 * (1 + .2*3)) * time.Second

func TestEvalFollowerReadOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilicl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	if offset, err := evalReadOffsetWithFollower(uuid.MakeV4(), st); err != nil {
		t.Fatal(err)
	} else if offset != expectedFollowerReadOffset {
		t.Fatalf("expected %v, got %v", expectedFollowerReadOffset, offset)
	}
	disableEnterprise()
	// skip by gzq, always have license
	//_, err := evalReadOffsetWithFollower(uuid.MakeV4(), st)
	//if !testutils.IsError(err, "requires an enterprise license") {
	//	t.Fatalf("failed to get error when evaluating follower read offset without " +
	//		"an enterprise license")
	//}
}

func TestCanSendToFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilicl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	storage.FollowerReadsEnabled.Override(&st.SV, true)

	old := hlc.Timestamp{
		WallTime: timeutil.Now().Add(2 * expectedFollowerReadOffset).UnixNano(),
	}
	oldHeader := roachpb.Header{Txn: &roachpb.Transaction{
		ReadTimestamp: old,
	}}
	rw := roachpb.BatchRequest{Header: oldHeader}
	rw.Add(&roachpb.PutRequest{})
	if isSendToFollower(uuid.MakeV4(), st, rw) {
		t.Fatalf("should not be able to send a rw request to a follower")
	}
	roNonTxn := roachpb.BatchRequest{Header: oldHeader}
	roNonTxn.Add(&roachpb.QueryTxnRequest{})
	if isSendToFollower(uuid.MakeV4(), st, roNonTxn) {
		t.Fatalf("should not be able to send a non-transactional ro request to a follower")
	}
	roNoTxn := roachpb.BatchRequest{}
	roNoTxn.Add(&roachpb.GetRequest{})
	if isSendToFollower(uuid.MakeV4(), st, roNoTxn) {
		t.Fatalf("should not be able to send a batch with no txn to a follower")
	}
	roOld := roachpb.BatchRequest{Header: oldHeader}
	roOld.Add(&roachpb.GetRequest{})
	if !isSendToFollower(uuid.MakeV4(), st, roOld) {
		t.Fatalf("should be able to send an old ro batch to a follower")
	}
	roRWTxnOld := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			TxnMeta:       enginepb.TxnMeta{Key: []byte("key")},
			ReadTimestamp: old,
		},
	}}
	roRWTxnOld.Add(&roachpb.GetRequest{})
	if isSendToFollower(uuid.MakeV4(), st, roRWTxnOld) {
		t.Fatalf("should not be able to send a ro request from a rw txn to a follower")
	}
	storage.FollowerReadsEnabled.Override(&st.SV, false)
	if isSendToFollower(uuid.MakeV4(), st, roOld) {
		t.Fatalf("should not be able to send an old ro batch to a follower when follower reads are disabled")
	}
	storage.FollowerReadsEnabled.Override(&st.SV, true)
	roNew := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			ReadTimestamp: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		},
	}}
	if isSendToFollower(uuid.MakeV4(), st, roNew) {
		t.Fatalf("should not be able to send a new ro batch to a follower")
	}
	roOldWithNewMax := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			MaxTimestamp: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		},
	}}
	roOldWithNewMax.Add(&roachpb.GetRequest{})
	if isSendToFollower(uuid.MakeV4(), st, roNew) {
		t.Fatalf("should not be able to send a ro batch with new MaxTimestamp to a follower")
	}
	// skip by gzq, always have license can not disableEnterprise
	//disableEnterprise()
	//if isSendToFollower(uuid.MakeV4(), st, roOld) {
	//	t.Fatalf("should not be able to send an old ro batch to a follower without enterprise enabled")
	//}
}

func TestFollowerReadMultipleValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic from setting followerReadMultiple to .1")
		}
	}()
	st := cluster.MakeTestingClusterSettings()
	followerReadMultiple.Override(&st.SV, .1)
}

// TestOracle tests the StrategyFactory exposed by this package.
// This test ends up being rather indirect but works by checking if the type
// of the oracle returned from the factory differs between requests we'd
// expect to support follower reads and that which we'd expect not to.
func TestOracleFactory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilicl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	storage.FollowerReadsEnabled.Override(&st.SV, true)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	c := client.NewDB(log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}, client.MockTxnSenderFactory{},
		hlc.NewClock(hlc.UnixNano, time.Nanosecond))
	txn := client.NewTxn(context.TODO(), c, 0, client.RootTxn)
	of := replicastrategy.NewStrategyFactory(followerReadAwareChoice, replicastrategy.Config{
		Settings:   st,
		RPCContext: rpcContext,
	})
	noFollowerReadOracle := of.Oracle(txn)
	old := hlc.Timestamp{
		WallTime: timeutil.Now().Add(2 * expectedFollowerReadOffset).UnixNano(),
	}
	txn.SetFixedTimestamp(context.TODO(), old)
	followerReadOracle := of.Oracle(txn)
	if reflect.TypeOf(followerReadOracle) == reflect.TypeOf(noFollowerReadOracle) {
		t.Fatalf("expected types of %T and %T to differ", followerReadOracle,
			noFollowerReadOracle)
	}
	// skip by gzq, always have license can not disableEnterprise
	//disableEnterprise()
	//disabledFollowerReadOracle := of.Oracle(txn)
	//if reflect.TypeOf(disabledFollowerReadOracle) != reflect.TypeOf(noFollowerReadOracle) {
	//	t.Fatalf("expected types of %T and %T not to differ", disabledFollowerReadOracle,
	//		noFollowerReadOracle)
	//}
}
