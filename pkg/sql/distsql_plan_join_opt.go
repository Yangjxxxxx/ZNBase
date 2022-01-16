package sql

import (
	"bytes"
	"encoding/binary"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"hash/crc32"
)

type CostModel interface {
	Init()
	DistributeCost() int64
	CalculateCost() int64
	ResultCost() int64
}

func tableReaderRowCount(tableReader *distsqlpb.TableReaderSpec) int64 {
	var size int64
	pendingSpans := make([]distsqlpb.TableReaderSpan, 0)
	for _, span := range tableReader.Spans {
		count, ok := spanKeyCount(&span)
		if ok {
			size = size + count
			continue
		}

		pendingSpans = append(pendingSpans, span)
	}

	// todo : deal pending spans
	return size
}

// calculate TableReaderSpan contains key counts
func spanKeyCount(span *distsqlpb.TableReaderSpan) (int64, bool) {
	const KeyCap = 16
	var startKey []byte = span.Span.Key
	var endKey []byte = span.Span.EndKey

	if len(startKey) < KeyCap || len(endKey) < KeyCap {
		return 0, false
	}

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

	var start, end int64
	bufferStart := bytes.NewBuffer(startKey)
	bufferEnd := bytes.NewBuffer(endKey)
	binary.Read(bufferStart, binary.BigEndian, &start)
	binary.Read(bufferEnd, binary.BigEndian, &end)

	return end - start, true
}

type distJoinHelper struct {
	// skew data
	leftHeavyHitters 	[]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter
	rightHeavyHitters 	[]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter
	// left and right routers
	processors 			[]distsqlplan.Processor
	leftRouters 		[]distsqlplan.ProcessorIdx
	rightRouters 		[]distsqlplan.ProcessorIdx
	// join node's ID
	joinNodes			[]roachpb.NodeID
	// getway's ID
	getwayID 			roachpb.NodeID
	frequencyThreshold  int64

	// skew size of every left or right router
	leftRouterSkewSize    []int64
	rightRouterSkewSize   []int64
	// skew data in map, intersect
	leftSkewData  *util.FastIntMap
	rightSkewData *util.FastIntMap
	intersect 	  *util.FastIntSet
}

func (djh *distJoinHelper) init() {
	if djh.leftRouterSkewSize != nil && djh.rightRouterSkewSize != nil {
		return
	}

	// estimate every router' skew data size
	calOneSide := func (routers []distsqlplan.ProcessorIdx, heavyHitters []distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter) []int64 {
		RouterRowCounts := make([]int64, len(routers))
		var allRowCounts int64
		for i, pIdx := range routers {
			proc := djh.processors[pIdx]
			tr := proc.Spec.Core.TableReader
			RouterRowCounts[i] = tableReaderRowCount(tr)
			allRowCounts += RouterRowCounts[i]
		}

		var skewRowCounts int64
		for _, item := range heavyHitters {
			skewRowCounts += item.Frequency
		}
		for i := range RouterRowCounts {
			RouterRowCounts[i] = (RouterRowCounts[i] * skewRowCounts) / allRowCounts;
		}
		return RouterRowCounts
	}

	djh.leftRouterSkewSize = calOneSide(djh.leftRouters, djh.leftHeavyHitters)
	djh.rightRouterSkewSize = calOneSide(djh.rightRouters, djh.rightHeavyHitters)

	djh.leftSkewData = &util.FastIntMap{}
	djh.rightSkewData = &util.FastIntMap{}
	djh.intersect = &util.FastIntSet{}
	// add left/right to map, and get intersect
	for _, item := range djh.leftHeavyHitters {
		djh.leftSkewData.Set(int(item.Value), int(item.Frequency))
	}
	for _, item := range djh.rightHeavyHitters {
		djh.rightSkewData.Set(int(item.Value), int(item.Frequency))
		_, ok := djh.leftSkewData.Get(int(item.Value))
		if ok {
			djh.intersect.Add(int(item.Value))
		}
	}
}

type PRPD struct {
	joinInfo *distJoinHelper

	finalLeftSkewData  []int64
	finalRightSkewData []int64
}

func(p PRPD) Init() {
	p.joinInfo.init()

	intersect := p.joinInfo.intersect
	// get left/right final skew data, two final skew data is not intersect
	leftSkewSize := int64(0)
	for _, item := range p.joinInfo.leftHeavyHitters {
		leftSkewSize += item.Frequency
		if intersect.Contains(int(item.Value)) {
			continue
		}
		p.finalLeftSkewData = append(p.finalLeftSkewData, item.Value)
	}
	rightSkewSize := int64(0)
	for _, item := range p.joinInfo.rightHeavyHitters {
		rightSkewSize += item.Frequency
		if intersect.Contains(int(item.Value)) {
			continue
		}
		p.finalRightSkewData = append(p.finalRightSkewData, item.Value)
	}

	// todo: intersect -> left or right
}

func(p PRPD) DistributeCost() int64 {
	var cost int64
	joinNodeNums := len(p.joinInfo.joinNodes)
	leftSkewData, rightSkewData := p.joinInfo.leftSkewData, p.joinInfo.rightSkewData

	// left mirror
	for _, value := range p.finalRightSkewData {
		count, ok := leftSkewData.Get(int(value))
		if ok {
			cost += int64(count * (joinNodeNums - 1))
			continue
		}
		cost += p.joinInfo.frequencyThreshold * int64((joinNodeNums - 1))
	}

	// right mirror
	for _, value := range p.finalLeftSkewData {
		count, ok := rightSkewData.Get(int(value))
		if ok {
			cost += int64(count * (joinNodeNums - 1))
			continue
		}
		cost += p.joinInfo.frequencyThreshold * int64((joinNodeNums - 1))
	}

	return cost
}

func(p PRPD) CalculateCost() int64 {
	joinNodeNums := len(p.joinInfo.joinNodes)
	buckets := make([]int64, joinNodeNums)

	// nodeID -> bucketIdx
	bucketIdxMap := make(map[roachpb.NodeID]int)
	for i, nodeID := range p.joinInfo.joinNodes {
		bucketIdxMap[nodeID] = i
	}

	// add left local count
	for i, pIdx := range p.joinInfo.leftRouters {
		proc := p.joinInfo.processors[pIdx]
		nodeID := proc.Node
		bucketIdx := bucketIdxMap[nodeID]
		buckets[bucketIdx] += p.joinInfo.leftRouterSkewSize[i]
	}
	// add right local count
	for i, pIdx := range p.joinInfo.rightRouters {
		proc := p.joinInfo.processors[pIdx]
		nodeID := proc.Node
		bucketIdx := bucketIdxMap[nodeID]
		buckets[bucketIdx] += p.joinInfo.rightRouterSkewSize[i]
	}

	// todo : part of intersect need specific handle

	var cost int64
	for _, count := range buckets {
		if count > cost {
			cost = count
		}
	}
	return cost
}

func(p PRPD) ResultCost() int64 {
	joinNodeNums := int64(len(p.joinInfo.joinNodes))
	var netSkewCount int64
	addCount := func (value int) {
		lCount, _ := p.joinInfo.leftSkewData.Get(value)
		rCount, _ := p.joinInfo.rightSkewData.Get(value)
		netSkewCount += int64(lCount) * int64(rCount)
	}
	p.joinInfo.intersect.ForEach(addCount)
	return netSkewCount * (joinNodeNums - 1) / joinNodeNums
}

type PnR struct {
	joinInfo *distJoinHelper

	finalLeftSkewData  []int64
	finalRightSkewData []int64
	finalLeftSkewSize 	int64
	finalRightSkewSize 	int64
}

func(pr PnR) Init() {
	pr.joinInfo.init()

	pr.finalLeftSkewData = make([]int64, 0)
	pr.finalLeftSkewData = make([]int64, 0)
	for _, item := range pr.joinInfo.leftHeavyHitters {
		value := item.Value
		if pr.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		pr.finalLeftSkewData = append(pr.finalLeftSkewData, value)
	}
	for _, item := range pr.joinInfo.rightHeavyHitters {
		value := item.Value
		pr.finalRightSkewData = append(pr.finalRightSkewData, value)
	}


	// all right skew size
	for _, value := range pr.finalRightSkewData {
		count, _ := pr.joinInfo.rightSkewData.Get(int(value))
		pr.finalRightSkewSize += int64(count)
	}
	// all left skew size
	for _, value := range pr.finalLeftSkewData {
		count, _ := pr.joinInfo.leftSkewData.Get(int(value))
		pr.finalLeftSkewSize += int64(count)
	}
}

func(pr PnR) DistributeCost() int64 {
	joinNodeNums := int64(len(pr.joinInfo.joinNodes))

	// right cost
	rightCost := pr.finalRightSkewSize
	rightCost = rightCost * (joinNodeNums - 1) / joinNodeNums

	// left cost
	leftCost := pr.finalLeftSkewSize
	leftCost = leftCost * (joinNodeNums - 1)

	return leftCost + rightCost
}

func(pr PnR) CalculateCost() int64 {
	joinNodeNums := int64(len(pr.joinInfo.joinNodes))
	return pr.finalRightSkewSize / joinNodeNums + pr.finalLeftSkewSize
}

func(pr PnR) ResultCost() int64 {
	joinNodeNums := int64(len(pr.joinInfo.joinNodes))
	var netSkewCount int64
	addCount := func (value int) {
		lCount, _ := pr.joinInfo.leftSkewData.Get(value)
		rCount, _ := pr.joinInfo.rightSkewData.Get(value)
		netSkewCount += int64(lCount) * int64(rCount)
	}
	pr.joinInfo.intersect.ForEach(addCount)
	return netSkewCount * (joinNodeNums - 1) / joinNodeNums
}

type BaseHash struct {
	joinInfo *distJoinHelper

	finalLeftSkewData  []int64
	finalRightSkewData []int64
	finalLeftSkewSize 	int64
	finalRightSkewSize 	int64

	alloc sqlbase.DatumAlloc
}

func(bh BaseHash) Init() {
	bh.joinInfo.init()
	bh.finalLeftSkewData = make([]int64, 0)
	bh.finalLeftSkewData = make([]int64, 0)
	for _, item := range bh.joinInfo.leftHeavyHitters {
		value := item.Value
		if bh.joinInfo.intersect.Contains(int(value)) {
			continue
		}
		bh.finalLeftSkewData = append(bh.finalLeftSkewData, value)
	}
	for _, item := range bh.joinInfo.rightHeavyHitters {
		value := item.Value
		bh.finalRightSkewData = append(bh.finalRightSkewData, value)
	}


	// all right skew size
	for _, value := range bh.finalRightSkewData {
		count, _ := bh.joinInfo.rightSkewData.Get(int(value))
		bh.finalRightSkewSize += int64(count)
	}
	// all left skew size
	for _, value := range bh.finalLeftSkewData {
		count, _ := bh.joinInfo.leftSkewData.Get(int(value))
		bh.finalLeftSkewSize += int64(count)
	}
}

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

func(bh BaseHash) calBucketIdx(key, bucketNums int64) (int, error) {
	encDatum := sqlbase.DatumToEncDatum(sqlbase.IntType, tree.NewDInt(tree.DInt(int64(1))))
	buffer := []byte{}
	typ := sqlbase.IntType
	var err error
	buffer, err = encDatum.Encode(&typ, &bh.alloc, flowinfra.PreferredEncoding, buffer)
	if err != nil {
		return -1, err
	}
	return int(crc32.Update(0, crc32Table, buffer) % uint32(bucketNums)), nil
}

func(bh BaseHash) DistributeCost() int64 {
	return bh.finalRightSkewSize + bh.finalLeftSkewSize
}

func(bh BaseHash) CalculateCost() int64 {
	joinNodeNums := int64(len(bh.joinInfo.joinNodes))
	buckets := make([]int64, joinNodeNums)

	// right
	for _, value := range bh.finalRightSkewData {
		bucketIdx, err := bh.calBucketIdx(value, joinNodeNums)
		if err != nil {
			return -1
		}
		count, _ := bh.joinInfo.rightSkewData.Get(int(value))
		buckets[bucketIdx] += int64(count)
	}
	// left
	for _, value := range bh.finalLeftSkewData {
		bucketIdx, err := bh.calBucketIdx(value, joinNodeNums)
		if err != nil {
			return -1
		}
		count, _ := bh.joinInfo.leftSkewData.Get(int(value))
		buckets[bucketIdx] += int64(count)
	}

	var cost int64
	for _, count := range buckets {
		if cost < count {
			cost = count
		}
	}
	return cost
}

func(bh BaseHash) ResultCost() int64 {
	joinNodeNums := int64(len(bh.joinInfo.joinNodes))
	var netSkewCount int64
	addCount := func (value int) {
		bucketIdx, err := bh.calBucketIdx(int64(value), joinNodeNums)
		if err != nil || bucketIdx == int(bh.joinInfo.getwayID) {
			return
		}
		lCount, _ := bh.joinInfo.leftSkewData.Get(value)
		rCount, _ := bh.joinInfo.rightSkewData.Get(value)
		netSkewCount += int64(lCount) * int64(rCount)
	}
	bh.joinInfo.intersect.ForEach(addCount)

	return netSkewCount
}
