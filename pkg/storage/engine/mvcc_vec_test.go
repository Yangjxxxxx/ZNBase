package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

var schema1 = []roachpb.ColumnMeta_Type{ //tpch customer
	roachpb.ColumnMeta_INT,
	roachpb.ColumnMeta_INT,
	roachpb.ColumnMeta_STRING,
	roachpb.ColumnMeta_FLOAT,
	roachpb.ColumnMeta_UUID,
	roachpb.ColumnMeta_DECIMAL,
}

var values = []string{"10", "50"}
var basefil = roachpb.BaseFilter{Attribute: "2", Values: values}

//Base过滤条件初始化 @2 IN {10,50}
var filterBase = roachpb.FilterUnion{Type: roachpb.FilterType_In, Value: &roachpb.FilterUnion_Base{Base: &basefil}}

func EncodeTableValuePrefix() []byte {
	var appendTo []byte
	appendTo = append(appendTo, byte(0), byte(0), byte(0), byte(0), byte(0))
	return appendTo
}

//EncodeTableValue returns the value encoded and primary key is int.
func EncodeTableValue(appendTo []byte, colID uint32, i int64) []byte {
	return encoding.EncodeIntValue(appendTo, colID, i)
}

// EncodeTableKey returns the key encoded.
func EncodeTableKey(tableID uint32, primaryVal uint64) []byte {
	key := keys.MakeTablePrefix(tableID)
	key = encoding.EncodeUvarintAscending(key, keys.SequenceIndexID)        // Index id
	key = encoding.EncodeUvarintAscending(key, primaryVal)                  // Primary key value
	key = encoding.EncodeUvarintAscending(key, keys.SequenceColumnFamilyID) // Column family
	return key
}

// TestTableKey test row tablekey encoded.
func TestTableKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tableKey := roachpb.Key(keys.MakeTablePrefix(59))
	endKey := tableKey.PrefixEnd()
	fmt.Printf("table : %v, %v", tableKey, endKey)
}

//TestMVCCVecScanFilter 测试BaseFilter功能
/*
Table Schema信息：
第一列主键为INT 第二列为INT 第三列为STRING 第四列为FLOAT 第五列为UUID 第六列为Decimal
 1  10   jack  2.22    424f137e-a2aa-11e8-98d0-529269fb1459
 2  100  tom   10.2       ffffffff-ffff-1fff-ffff-ffffffffffff
 3  50   susan 0.00   00000000-0000-1000-0000-000000000000
*/
func TestMVCCVecScanColumnType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	engine := createTestEngine()
	defer engine.Close()
	reader := engine.NewReadOnly()
	defer reader.Close()

	key1 := EncodeTableKey(58, 1)
	time1 := hlc.Timestamp{WallTime: 0, Logical: 0}
	valuePrefix1 := EncodeTableValuePrefix() //校验位5位
	valueInt1 := EncodeTableValue(valuePrefix1, 2, 10)
	valueString1 := encoding.EncodeBytesValue(valueInt1, 1, []byte("jack"))
	valueFloat1 := encoding.EncodeFloatValue(valueString1, 1, 2.22)
	uu1 := uuid.Must(uuid.FromString("424f137e-a2aa-11e8-98d0-529269fb1459"))
	valueUUID1 := encoding.EncodeUUIDValue(valueFloat1, 1, uu1)
	dec1 := apd.New(13456, -4)
	value1 := roachpb.Value{RawBytes: encoding.EncodeDecimalValue(valueUUID1, 1, dec1), Timestamp: time1}

	key2 := EncodeTableKey(58, 2)
	time2 := hlc.Timestamp{WallTime: 0, Logical: 0}
	valuePrefix2 := EncodeTableValuePrefix()
	valueInt2 := EncodeTableValue(valuePrefix2, 2, 100)
	valueString2 := encoding.EncodeBytesValue(valueInt2, 1, []byte("tom"))
	valueFloat2 := encoding.EncodeFloatValue(valueString2, 1, 10.2)
	uu2 := uuid.Must(uuid.FromString("ffffffff-ffff-1fff-ffff-ffffffffffff"))
	valueUUID2 := encoding.EncodeUUIDValue(valueFloat2, 1, uu2)
	dec2 := apd.New(-11, -4)
	value2 := roachpb.Value{RawBytes: encoding.EncodeDecimalValue(valueUUID2, 1, dec2), Timestamp: time2}

	key3 := EncodeTableKey(58, 3)
	time3 := hlc.Timestamp{WallTime: 0, Logical: 0}
	valuePrefix3 := EncodeTableValuePrefix()
	valueInt3 := EncodeTableValue(valuePrefix3, 2, 50)
	valueString3 := encoding.EncodeBytesValue(valueInt3, 1, []byte("susan"))
	valueFloat3 := encoding.EncodeFloatValue(valueString3, 1, 0.00)
	uu3 := uuid.Must(uuid.FromString("00000000-0000-1000-0000-000000000000"))
	valueUUID3 := encoding.EncodeUUIDValue(valueFloat3, 1, uu3)
	dec3 := apd.New(11, -6)
	value3 := roachpb.Value{RawBytes: encoding.EncodeDecimalValue(valueUUID3, 1, dec3), Timestamp: time3}

	if err := MVCCPut(ctx, engine, nil, key1, hlc.Timestamp{WallTime: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}

	if err := MVCCPut(ctx, engine, nil, key2, hlc.Timestamp{WallTime: 1}, value2, nil); err != nil {
		t.Fatal(err)
	}

	if err := MVCCPut(ctx, engine, nil, key3, hlc.Timestamp{WallTime: 1}, value3, nil); err != nil {
		t.Fatal(err)
	}

	key := roachpb.Key(keys.MakeTablePrefix(58))
	endKey := key.PrefixEnd()

	pushExpr := roachpb.PushDownExpr{
		ColIds:            []int32{1, 2, 3, 4, 5, 6},
		ColTypes:          schema1, //
		RequiredCols:      []int32{1, 2, 3, 4, 5, 6},
		PrimaryCols:       []int32{1},
		PrimaryColsDirect: []int32{0},
		Filters:           []*roachpb.FilterUnion{&filterBase},
	}
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano(), Logical: 0}
	opts := MVCCScanOptions{
		Inconsistent:     false,
		Txn:              &roachpb.Transaction{ReadTimestamp: ts},
		MaxKeys:          100000,
		FailOnMoreRecent: false,
		Reverse:          false,
	}

	results, _, _, err := MVCCVecScan(ctx, reader, key, endKey, &pushExpr, opts.MaxKeys, ts, opts)
	if err != nil {
		log.Fatal(ctx, err)
	}
	fmt.Printf("Result Count : %d\n", results.Count)
	fmt.Printf("&&&RequireColID  ")
	for _, colValues1 := range results.ColValues {
		fmt.Printf("%d   ", colValues1.ColId)
	}
	fmt.Println(" ")
	for row := int64(0); row < results.Count; row++ {
		fmt.Printf("&&&&&&Row[%d] : ", row)
		for _, colValues := range results.ColValues {
			switch sqlbase.ColumnType_SemanticType(colValues.ColType) {
			case sqlbase.ColumnType_INT:
				fmt.Printf("%d", colValues.VInt[row])
				break
			case sqlbase.ColumnType_STRING:
				fmt.Printf("%s", colValues.Values[row])
				break
			case sqlbase.ColumnType_FLOAT:
				fmt.Printf("%g", colValues.VDouble[row])
				break
			case sqlbase.ColumnType_DECIMAL:
				dec := colValues.VDecimal[row]
				fmt.Printf("%d ", dec.Form)
				fmt.Printf(",%d ", dec.Exponent)
				fmt.Printf(",%t ", dec.Negative)
				fmt.Printf(",%d", dec.Abs[0])
				fmt.Printf(",%t", dec.Neg)
				break
			case sqlbase.ColumnType_UUID:
				fmt.Printf("%s", colValues.Values[row])
				break
			default:
				fmt.Printf("unkwown type")
			}
			fmt.Printf(" , ")
		}
		fmt.Printf("\n")
	}
}

func TestData2Row(t *testing.T) {
	defer leaktest.AfterTest(t)()
	{
		// Key=/Table/62/1/647518279283572737/0, value=/TUPLE/1:1:Bytes/tom/1:2:Int/10
		//key := roachpb.Key([]byte{198, 137, 253, 8, 252, 114, 113, 190, 69, 0, 1, 136})
		key := roachpb.Key(keys.MakeTablePrefix(61))
		key = []byte{256 - 60, 256 - 119, 256 - 128, 256 - 128, 0, 17, 256 - 46, 256 - 103, 256 - 2, 107, 92, 256 - 120}
		end := key.PrefixEnd()
		//value := roachpb.Value{RawBytes: []byte{158, 27, 131, 45, 10, 22, 3, 116, 111, 109, 19, 20}}
		t.Logf("Key=%v, value=%v\n", key, end)
	}

	{
		//TPCH customer
		var key = roachpb.Key{}
		var value = roachpb.Value{}
		//Key = /Table/59/1/-9223352440630447803/0
		//value= /TUPLE/1:1:Int/1/1:2:Bytes/Customer#000000001/1:3:Bytes/IVhzIApeRb ot,c,E/1:4:Int/15/1:5:Bytes/25-989-741-2988/1:6:Decimal/711.56/1:7:Bytes/BUILDING/1:8:Bytes/to the even, regular platelets. regular, ironic epitaphs nag e
		//key = []byte{195, 137, 128, 128, 0, 17, 210, 153, 253, 113, 69, 136}
		//value = roachpb.Value{RawBytes: []byte{86, 247, 142, 227, 10, 19, 2, 22, 18, 67, 117, 115, 116, 111, 109, 101, 114, 35, 48, 48, 48, 48, 48, 48, 48, 48, 49, 22, 17, 73, 86, 104, 122, 73, 65, 112, 101, 82, 98, 32, 111, 116, 44, 99, 44, 69, 19, 30, 22, 15, 50, 53, 45, 57, 56, 57, 45, 55, 52, 49, 45, 50, 57, 56, 56, 21, 5, 52, 139, 1, 21, 244, 22, 8, 66, 85, 73, 76, 68, 73, 78, 71, 22, 62, 116, 111, 32, 116, 104, 101, 32, 101, 118, 101, 110, 44, 32, 114, 101, 103, 117, 108, 97, 114, 32, 112, 108, 97, 116, 101, 108, 101, 116, 115, 46, 32, 114, 101, 103, 117, 108, 97, 114, 44, 32, 105, 114, 111, 110, 105, 99, 32, 101, 112, 105, 116, 97, 112, 104, 115, 32, 110, 97, 103, 32, 101}}
		// java解码调试比对
		key = []byte{256 - 61, 256 - 119, 256 - 128, 256 - 128, 0, 17, 256 - 46, 256 - 103, 256 - 3, 113, 256 - 60, 256 - 120}
		value = roachpb.Value{RawBytes: []byte{26, 256 - 125, 256 - 70, 123, 10, 19, 256 - 128, 2, 22, 18, 67, 117, 115, 116, 111, 109, 101, 114, 35, 48, 48, 48, 48, 48, 48, 49, 50, 56, 22, 20, 65, 109, 75, 85, 77, 108, 74, 102, 50, 78, 82, 72, 99, 75, 71, 109, 75, 106, 76, 83, 19, 8, 22, 15, 49, 52, 45, 50, 56, 48, 45, 56, 55, 52, 45, 56, 48, 52, 52, 21, 5, 26, 256 - 117, 1, 256 - 127, 256 - 120, 22, 9, 72, 79, 85, 83, 69, 72, 79, 76, 68, 22, 111, 105, 110, 103, 32, 112, 97, 99, 107, 97, 103, 101, 115, 32, 105, 110, 116, 101, 103, 114, 97, 116, 101, 32, 97, 99, 114, 111, 115, 115, 32, 116, 104, 101, 32, 115, 108, 121, 108, 121, 32, 117, 110, 117, 115, 117, 97, 108, 32, 100, 117, 103, 111, 117, 116, 115, 46, 32, 98, 108, 105, 116, 104, 101, 108, 121, 32, 115, 105, 108, 101, 110, 116, 32, 105, 100, 101, 97, 115, 32, 115, 117, 98, 108, 97, 116, 101, 32, 99, 97, 114, 101, 102, 117, 108, 108, 121, 46, 32, 98, 108, 105, 116, 104, 101, 108, 121, 32, 101, 120, 112, 114}}
		t.Logf("Key=%v, value=%v\n", key, value.PrettyPrint())
	}
}

func TestSpanKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	start := roachpb.Key([]byte{256 - 59, 256 - 119, 256 - 128, 256 - 128, 0, 17, 256 - 46, 256 - 102, 11, 20, 127, 256 - 120})
	end := roachpb.Key([]byte{256 - 59, 256 - 119, 256 - 128, 256 - 128, 0, 17, 256 - 46, 256 - 102, 37, 60, 42})
	t.Logf("start=%v, end=%v , Compare=%v\n", start, end, start.Compare(end))
}
