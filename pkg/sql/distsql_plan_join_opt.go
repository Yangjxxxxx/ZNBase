package sql

import (
	"bytes"
	"encoding/binary"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/util"
)

type CostModel interface {
	Init()
	DistributeCost() int64
	CalculateCost() int64
	ResultCost() int64
}

// calculate TableReaderSpan contains key counts
func spanKeyCount(span distsqlpb.TableReaderSpan) uint64 {
	var startKey []byte = span.Span.Key
	var endKey []byte = span.Span.EndKey

	fixKey := func (key []byte) []byte {
		length := len(key)
		if length >= 8 {
			key = key[length - 8 : length]
		} else {
			zeroByte := []byte{0, 0, 0, 0, 0, 0, 0, 0}
			copy(zeroByte[8-length:], key)
			key = zeroByte
		}
		return key
	}
	startKey = fixKey(startKey)
	endKey = fixKey(endKey)

	var start, end uint64
	bufferStart := bytes.NewBuffer(startKey)
	bufferEnd := bytes.NewBuffer(endKey)
	binary.Read(bufferStart, binary.BigEndian, &start)
	binary.Read(bufferEnd, binary.BigEndian, &end)

	return end - start
}

type PRPD struct {
	leftHeavyHitters 	[]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter
	rightHeavyHitters 	[]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter
	leftTableReaders 	[]distsqlpb.TableReaderSpec
	rightTableReaders 	[]distsqlpb.TableReaderSpec
	joinNodes			[]roachpb.NodeID

	leftSkewData  *util.FastIntMap
	rightSkewData *util.FastIntMap
}

func(p PRPD) Init() {
}

func(p PRPD) DistributeCost() int64 {
	return 0
}

func(p PRPD) CalculateCost() int64 {
	return 0
}

func(p PRPD) ResultCost() int64 {
	return 0
}

type DivideMirror struct {

}

func(dm DivideMirror) Init() {
}

func(dm DivideMirror) DistributeCost() int64 {
	return 0
}

func(dm DivideMirror) CalculateCost() int64 {
	return 0
}

func(dm DivideMirror) ResultCost() int64 {
	return 0
}

type BaseHash struct {

}

func(bh BaseHash) Init() {
}

func(bh BaseHash) DistributeCost() int64 {
	return 0
}

func(bh BaseHash) CalculateCost() int64 {
	return 0
}

func(bh BaseHash) ResultCost() int64 {
	return 0
}
