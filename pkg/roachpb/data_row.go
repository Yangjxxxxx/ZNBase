package roachpb

import (
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/util/encoding"
)

//ColValue is 列值编码值
type ColValue struct {
	RawBytes []byte
}

// ToRow 解码为：列ID->列值映射
func (v Value) ToRow(maxColID uint32) ([]ColValue, error) {
	if len(v.RawBytes) == 0 {
		return []ColValue{}, nil
	}
	t := v.GetTag()
	if t == ValueType_TUPLE { // 列值组合成的Value
		//t== ValueType_BYTES {  // 二级索引结构

		b := v.dataBytes()
		return Decode2Row(b, maxColID)
	}

	buf := make([]ColValue, maxColID+1) //map[uint32]interface{}{}
	if t == ValueType_BYTES {           // 二级索引结构
		buf[0] = ColValue{RawBytes: v.RawBytes}
	} else {
		buf[0] = ColValue{RawBytes: v.RawBytes}
	}
	return buf, nil
}

const (
	// DefaultColID =初始的最大列ID
	DefaultColID = uint32(10)
	// MaxColID =最大解析列ID
	MaxColID = uint32(256)
)

// Decode2Row value decode
func Decode2Row(b []byte, maxColID uint32) ([]ColValue, error) {
	if maxColID == 0 {
		maxColID = DefaultColID
	}
	buf := make([]ColValue, maxColID+1)
	var colID uint32
	for i := 0; len(b) > 0; i++ {
		typeOffset, dataOffset, colIDDiff, typ, err := encoding.DecodeValueTag(b)
		if err != nil {
			break
		}
		colID += colIDDiff
		var s []byte
		b, s, err = encoding.DeocdeValue(b, typeOffset, dataOffset, typ)
		if err != nil {
			break
		}
		if colID >= MaxColID {
			return nil, errors.Errorf("parsed ColID[%d] larger than MAX_COL_ID[%d]", colID, MaxColID)
		}
		if colID >= uint32(len(buf)) {
			// 扩充数组：当前列ID × 2倍，避免频繁copy
			var newB []ColValue
			if uint32(len(b)) < colID {
				// 如果剩余字节数已经不足于填满colID*2，则扩充1.5倍
				newB = make([]ColValue, (colID*15)/10)
			} else {
				newB = make([]ColValue, colID*2)
			}
			copy(newB, buf)
			buf = newB
		}
		buf[colID] = ColValue{RawBytes: s}
	}
	return buf[:colID+1], nil
}
