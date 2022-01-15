package engine

import (
	"bytes"
	"testing"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/storage/engine/enginepb"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

type Expect struct {
	result []byte
	len    int64
}

var (
	//timestamp
	Logical1 = int32(1)
	Logical2 = int32(2)
	Logical3 = int32(3)
	Logical5 = int32(5)

	WallTime0 = int64(0)
	WallTime1 = int64(1)
	WallTime2 = int64(2)
	//keys
	testKeyA = roachpb.Key("a")
	testKeyB = roachpb.Key("b")

	//the struct of TestMVCCScanMerge func
	expect1 = Expect{[]byte{5, 4, 1}, 3}
	expect2 = Expect{[]byte{1}, 1}
	expect3 = Expect{[]byte{5, 4}, 2}
	expect4 = Expect{[]byte{}, 0}
	//the struct of TestMVCCString func
	emptySpan = spanAndTime{}
	theSpan1  = spanAndTime{roachpb.Span{Key: testKey1, EndKey: testKey5}, hlc.Timestamp{WallTime: WallTime1, Logical: Logical1}}
	theSpan2  = spanAndTime{roachpb.Span{Key: testKey1}, hlc.Timestamp{WallTime: WallTime2, Logical: Logical2}}
	theSpan3  = spanAndTime{roachpb.Span{Key: testKey1, EndKey: testKey1.Next()}, hlc.Timestamp{WallTime: WallTime0, Logical: Logical2}}
	theSpan4  = spanAndTime{roachpb.Span{Key: testKey3, EndKey: testKey3.Next()}, hlc.Timestamp{WallTime: WallTime0, Logical: Logical5}}
	theSpan5  = spanAndTime{roachpb.Span{Key: testKey2, EndKey: testKey2.Next()}, hlc.Timestamp{WallTime: WallTime0, Logical: Logical5}}
	theSpanA  = spanAndTime{roachpb.Span{Key: testKeyA, EndKey: testKeyA.Next()}, hlc.Timestamp{WallTime: WallTime0, Logical: Logical3}}
	theSpanB  = spanAndTime{roachpb.Span{Key: testKeyB, EndKey: testKeyB.Next()}, hlc.Timestamp{WallTime: WallTime0, Logical: Logical2}}
	//Txn
	txn3 = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{Key: roachpb.Key("/db2"), ID: txn2ID, WriteTimestamp: hlc.Timestamp{Logical: 1}}, ReadTimestamp: hlc.Timestamp{Logical: 1}}
	ts   = []hlc.Timestamp{{Logical: 1}, {Logical: 2}, {Logical: 3}, {Logical: 4}, {Logical: 5}, {Logical: 6}}
	// key = 'a' logic = 3
	txn1test = makeTxn(*txn1, ts[2])
	// key = 'a' logic = 6
	txn2test = makeTxn(*txn2, ts[3])
	txn3test = makeTxn(*txn3, ts[5])

	//The third part of the resultSpans
	resultSpan1 = spanAndTime{roachpb.Span{Key: testKey1, EndKey: testKey2}, hlc.Timestamp{WallTime: WallTime1, Logical: Logical1}}
	resultSpan2 = spanAndTime{roachpb.Span{Key: testKey2, EndKey: testKey2.Next()}, hlc.Timestamp{Logical: Logical3}}
	resultSpan3 = spanAndTime{roachpb.Span{Key: testKey2.Next(), EndKey: testKey5}, hlc.Timestamp{WallTime: WallTime1, Logical: Logical1}}
)

func TestMVCCScanMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		dst    []byte
		dstNum int64
		src    []byte
		srcNum int64
		expect Expect
	}{
		{[]byte{5, 4}, 2, []byte{1}, 1, expect1},
		{[]byte{}, 0, []byte{1}, 1, expect2},
		{[]byte{5, 4}, 2, []byte{}, 0, expect3},
		{[]byte{}, 0, []byte{}, 0, expect4},
	}
	for tcNum, tc := range testCases {
		MVCCScanMerge(&tc.dst, &tc.dstNum, tc.src, tc.srcNum)
		if !bytes.Equal(tc.expect.result, tc.dst) {
			t.Errorf("#%d: expected dst= %p , but got: %p", tcNum, tc.expect.result, tc.dst)
		}
		if tc.expect.len != tc.dstNum {
			t.Errorf("#%d: expected dstNum= %d , but got: %d", tcNum, tc.expect.len, tc.dstNum)
		}
	}
}

func TestMVCCString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		spanAndTime spanAndTime
		expect      string
	}{
		{emptySpan, "/Min,{}"},
		{theSpan1, "/db{1-5},{\"wall_time\":1,\"logical\":1}"},
		{theSpan2, "/db1,{\"wall_time\":2,\"logical\":2}"},
	}
	for tcNum, tc := range testCases {
		str := tc.spanAndTime.String()
		if str != tc.expect {
			t.Errorf("#%d: expected string= %s , but got: %s", tcNum, tc.expect, str)
		}
	}
}

func TestWriteIntentTxnToKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		Intents        []roachpb.LockUpdate
		expSpanAndTime []spanAndTime
		expErr         string
	}{
		{
			Intents: []roachpb.LockUpdate{
				{Span: roachpb.Span{Key: testKey1}, Txn: txn1test.TxnMeta},
				{Span: roachpb.Span{Key: testKey3}, Txn: txn3test.TxnMeta},
				{Span: roachpb.Span{Key: testKeyA}, Txn: txn2test.TxnMeta},
				{Span: roachpb.Span{Key: testKeyB}, Txn: txn1test.TxnMeta},
			},
			expSpanAndTime: []spanAndTime{
				theSpan3,
				theSpan4,
				theSpanA,
				theSpanB,
			},
			expErr: "",
		},
		{
			Intents:        nil,
			expSpanAndTime: nil,
			expErr:         "No write intent to resolve! ",
		},
		{
			Intents: []roachpb.LockUpdate{
				{Span: roachpb.Span{Key: testKey2}, Txn: txn3test.TxnMeta},
			},
			expSpanAndTime: []spanAndTime{
				theSpan5,
			},
			expErr: "",
		},
	}

	for tcNum, tc := range testCases {
		spanAndTime, Err := mvccWriteIntentTxnToKeys(tc.Intents, false)
		if !testutils.IsError(Err, tc.expErr) {
			t.Errorf("#%d: expected error= %v , but got: %s", tcNum, tc.expErr, Err)
		} else if len(spanAndTime) != len(tc.expSpanAndTime) {
			t.Errorf("#%d: expected spanAndTime rows= %v, but got:%v", tcNum, len(tc.expSpanAndTime), len(spanAndTime))
			return
		}
		for i, sat := range spanAndTime {
			str := sat.String()
			if str != tc.expSpanAndTime[i].String() {
				t.Errorf("#(%d, %d) expected SpanAndTime=\"%v\", but got: \"%v\"", tcNum, i, tc.expSpanAndTime[i], sat)
			}
		}
	}
}

func TestMVCCResolveWriteIntentRC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		Intents        []roachpb.LockUpdate
		expSpanAndTime []spanAndTime
		expErr         string
		start, end     roachpb.Key
		timestamp      hlc.Timestamp
	}{
		{
			Intents: []roachpb.LockUpdate{
				{Span: roachpb.Span{Key: testKey1}, Txn: txn1test.TxnMeta},
			},
			timestamp:      hlc.Timestamp{WallTime: WallTime1, Logical: Logical1},
			expSpanAndTime: []spanAndTime{},
			expErr:         "attempted access to empty key",
		},
		{
			Intents: []roachpb.LockUpdate{
				{Span: roachpb.Span{Key: testKey2}, Txn: txn2test.TxnMeta},
			},
			timestamp: hlc.Timestamp{WallTime: WallTime1, Logical: Logical1},
			expSpanAndTime: []spanAndTime{
				resultSpan1,
				resultSpan2,
				resultSpan3,
			},
			expErr: "",
			start:  testKey1,
			end:    testKey5,
		},
		{
			Intents:        nil,
			expSpanAndTime: nil,
			expErr:         "No write intent to resolve! ",
			start:          testKey1,
			end:            testKey2,
		},
		{
			Intents: []roachpb.LockUpdate{
				{Span: roachpb.Span{Key: testKey2}, Txn: txn2test.TxnMeta},
			},
			timestamp:      hlc.Timestamp{WallTime: WallTime1, Logical: Logical1},
			expSpanAndTime: []spanAndTime{},
			expErr:         "intent has crossed",
			start:          testKeyA,
			end:            testKeyB,
		},
		{
			Intents: []roachpb.LockUpdate{
				{Span: roachpb.Span{Key: testKeyA}, Txn: txn2test.TxnMeta},
			},
			timestamp:      hlc.Timestamp{WallTime: WallTime1, Logical: Logical1},
			expSpanAndTime: []spanAndTime{},
			expErr:         "intent key out of range",
			start:          testKey1,
			end:            testKey2,
		},
		{
			Intents: []roachpb.LockUpdate{
				{Span: roachpb.Span{Key: testKey2}, Txn: txn2test.TxnMeta},
			},
			timestamp: hlc.Timestamp{WallTime: WallTime1, Logical: Logical1},
			expSpanAndTime: []spanAndTime{
				resultSpan2,
				resultSpan3,
			},
			expErr: "",
			start:  testKey2,
			end:    testKey5,
		},
	}

	for tcNum, tc := range testCases {
		spanAndTime, Err := mvccResolveWriteIntentRC(tc.start, tc.end, tc.timestamp, tc.Intents, false)
		if !testutils.IsError(Err, tc.expErr) {
			t.Errorf("#%d: expected error= %v , but got: %s", tcNum, tc.expErr, Err)
		} else if len(spanAndTime) != len(tc.expSpanAndTime) {
			t.Errorf("%d: The spanAndTime rows shoud be %d , but got %d", tcNum, len(tc.expSpanAndTime), len(spanAndTime))
			return
		}
		for i, sat := range spanAndTime {
			str := sat.String()
			if str != tc.expSpanAndTime[i].String() {
				t.Errorf("#(%d , %d) expected SpanAndTime=\"%v\", but got: \"%v\"", tcNum, i, tc.expSpanAndTime[i], sat.String())
			}
		}
	}
}
