package sql

import (
	"bytes"
	"encoding/binary"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/sql/distsqlplan"
	"github.com/znbasedb/znbase/pkg/sql/flowinfra"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

type CostModel interface {
	Init()
	Type() string
	DistributeCost() int64
	CalculateCost() int64
	ResultCost() int64
	WorkArgs() distsqlplan.HashJoinWorkArgs
}

var planPRPD = settings.RegisterBoolSetting(
	"sql.distsql.hashjoin_prpd.enabled",
	"if set we plan interleaved table joins instead of merge joins when possible",
	false,
)

var planPnR = settings.RegisterBoolSetting(
	"sql.distsql.hashjoin_pnr.enabled",
	"if set we plan interleaved table joins instead of merge joins when possible",
	false,
)

var planBaseHash = settings.RegisterBoolSetting(
	"sql.distsql.hashjoin_bashash.enabled",
	"if set we plan interleaved table joins instead of merge joins when possible",
	false,
)

func tableReaderRowCount(tableReader *distsqlpb.TableReaderSpec) int64 {
	var size, rangeCount int64
	pendingSpans := make([]distsqlpb.TableReaderSpan, 0)
	for _, span := range tableReader.Spans {
		count, ok := spanKeyCount(span.Span.Key, span.Span.EndKey, true)
		if ok {
			size = size + count
			rangeCount = rangeCount + int64(len(span.Ranges))
			continue
		}

		pendingSpans = append(pendingSpans, span)
	}
	if rangeCount == 0 {
		return 0
	}

	// deal pending spans
	rangeAvgSize := size / rangeCount
	for _, span := range pendingSpans {
		var spanSize int64
		for _, r := range span.Ranges {
			count, ok := spanKeyCount(r.StartKey, r.EndKey, true)
			if ok {
				spanSize += count
				continue
			}
			spanSize += rangeAvgSize
		}
		size += spanSize
	}

	return size
}

// calculate TableReaderSpan contains key counts.
// fixedKey indicate startKey/endKey has prefix.
func spanKeyCount(startKey, endKey []byte, fixedKey bool) (int64, bool) {
	const KeyLen = 2

	if fixedKey && (len(startKey) < KeyLen || len(endKey) < KeyLen) {
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
	gatewayID 			roachpb.NodeID
	frequencyThreshold  float64

	// left/right all size
	allLeftSize 		int64
	allRightSize 		int64
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
	calOneSide := func (routers []distsqlplan.ProcessorIdx, heavyHitters []distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter) ([]int64, int64) {
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
		return RouterRowCounts, allRowCounts
	}

	djh.leftRouterSkewSize, djh.allLeftSize = calOneSide(djh.leftRouters, djh.leftHeavyHitters)
	djh.rightRouterSkewSize, djh.allRightSize = calOneSide(djh.rightRouters, djh.rightHeavyHitters)

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

func(p *PRPD) Init() {
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

	// intersect -> left or right
	if p.joinInfo.allLeftSize * leftSkewSize >= p.joinInfo.allRightSize * rightSkewSize {
		addIntersectToLeft := func (value int) {
			p.finalLeftSkewData = append(p.finalLeftSkewData, int64(value))
		}
		p.joinInfo.intersect.ForEach(addIntersectToLeft)
	} else {
		addIntersectToRight := func (value int) {
			p.finalRightSkewData = append(p.finalRightSkewData, int64(value))
		}
		p.joinInfo.intersect.ForEach(addIntersectToRight)
	}
}

func(p *PRPD) Type() string {
	return "PRPD"
}

func(p *PRPD) DistributeCost() int64 {
	var cost int64
	joinNodeNums := len(p.joinInfo.joinNodes)
	leftSkewMap, rightSkewMap := p.joinInfo.leftSkewData, p.joinInfo.rightSkewData

	// left mirror
	for _, value := range p.finalRightSkewData {
		count, ok := leftSkewMap.Get(int(value))
		if ok {
			cost += int64(count * (joinNodeNums - 1))
			continue
		}
		cost += int64(p.joinInfo.frequencyThreshold * float64(p.joinInfo.allLeftSize))
	}

	// right mirror
	for _, value := range p.finalLeftSkewData {
		count, ok := rightSkewMap.Get(int(value))
		if ok {
			cost += int64(count * (joinNodeNums - 1))
			continue
		}
		cost += int64(p.joinInfo.frequencyThreshold * float64(p.joinInfo.allRightSize))
	}

	return cost
}

func(p *PRPD) CalculateCost() int64 {
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

	// suppose that each node has all skew data of other side.
	// left mirror size
	leftMirrorCount := int64(0)
	for _, hv := range p.finalRightSkewData {
		count, ok := p.joinInfo.leftSkewData.Get(int(hv))
		if ok {
			leftMirrorCount += int64(count)
			continue;
		}
		leftMirrorCount += int64(p.joinInfo.frequencyThreshold * float64(p.joinInfo.allLeftSize))
	}
	// right mirror count
	rightMirrorCount := int64(0)
	for _, hv := range p.finalLeftSkewData {
		count, ok := p.joinInfo.rightSkewData.Get(int(hv))
		if ok {
			rightMirrorCount += int64(count)
			continue;
		}
		rightMirrorCount += int64(p.joinInfo.frequencyThreshold * float64(p.joinInfo.allRightSize))
	}

	var cost int64 = leftMirrorCount + rightMirrorCount
	for _, count := range buckets {
		if count > cost {
			cost = count
		}
	}
	return cost
}

func(p *PRPD) ResultCost() int64 {
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

func(p *PRPD) WorkArgs() distsqlplan.HashJoinWorkArgs {
	leftHeavyHitters :=	make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(p.finalLeftSkewData))
	rightHeavyHitters := make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(p.finalRightSkewData))

	for i, value := range p.finalLeftSkewData {
		leftHeavyHitters[i].Value = value
	}
	for i, value := range p.finalRightSkewData {
		rightHeavyHitters[i].Value = value
	}
	return distsqlplan.HashJoinWorkArgs{
		HJType: 			distsqlplan.PRPD,
		LeftHeavyHitters: 	leftHeavyHitters,
		RightHeavyHitters: 	rightHeavyHitters,
	}
}

type PnR struct {
	joinInfo *distJoinHelper

	finalLeftSkewData  []int64
	finalRightSkewData []int64
	finalLeftSkewSize 	int64
	finalRightSkewSize 	int64
}

func(pr *PnR) Init() {
	pr.joinInfo.init()

	// intersect -> left
	pr.finalLeftSkewData = make([]int64, 0)
	pr.finalLeftSkewData = make([]int64, 0)
	for _, item := range pr.joinInfo.leftHeavyHitters {
		value := item.Value
		pr.finalLeftSkewData = append(pr.finalLeftSkewData, value)
	}
	for _, item := range pr.joinInfo.rightHeavyHitters {
		value := item.Value
		if pr.joinInfo.intersect.Contains(int(value)) {
			continue
		}
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

func(pr *PnR) Type() string {
	return "PnR"
}

func(pr *PnR) DistributeCost() int64 {
	joinNodeNums := int64(len(pr.joinInfo.joinNodes))

	// right cost
	rightCost := pr.finalRightSkewSize
	rightCost = rightCost * (joinNodeNums - 1)

	// left cost
	leftCost := pr.finalLeftSkewSize
	leftCost = leftCost * (joinNodeNums - 1)  / joinNodeNums

	return leftCost + rightCost
}

func(pr *PnR) CalculateCost() int64 {
	joinNodeNums := int64(len(pr.joinInfo.joinNodes))
	return pr.finalLeftSkewSize / joinNodeNums + pr.finalRightSkewSize
}

func(pr *PnR) ResultCost() int64 {
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

func(pr *PnR) WorkArgs() distsqlplan.HashJoinWorkArgs {
	leftHeavyHitters :=	make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.finalLeftSkewData))
	rightHeavyHitters := make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(pr.finalRightSkewData))

	for i, value := range pr.finalLeftSkewData {
		leftHeavyHitters[i].Value = value
	}
	for i, value := range pr.finalRightSkewData {
		rightHeavyHitters[i].Value = value
	}
	return distsqlplan.HashJoinWorkArgs{
		HJType: 			distsqlplan.PnR,
		LeftHeavyHitters: 	leftHeavyHitters,
		RightHeavyHitters: 	rightHeavyHitters,
	}
}

type BaseHash struct {
	joinInfo *distJoinHelper

	finalLeftSkewData  []int64
	finalRightSkewData []int64
	finalLeftSkewSize 	int64
	finalRightSkewSize 	int64

	alloc sqlbase.DatumAlloc
}

func(bh *BaseHash) Init() {
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

func(bh *BaseHash) Type() string {
	return "BaseHash"
}


var crc32Table = crc32.MakeTable(crc32.Castagnoli)

func(bh *BaseHash) calBucketIdx(key, bucketNums int64) (int, error) {
	encDatum := sqlbase.DatumToEncDatum(sqlbase.IntType, tree.NewDInt(tree.DInt(key)))
	buffer := []byte{}
	typ := sqlbase.IntType
	var err error
	buffer, err = encDatum.Encode(&typ, &bh.alloc, flowinfra.PreferredEncoding, buffer)
	if err != nil {
		return -1, err
	}
	return int(crc32.Update(0, crc32Table, buffer) % uint32(bucketNums)), nil
}

func(bh *BaseHash) DistributeCost() int64 {
	joinNodeNums := int64(len(bh.joinInfo.joinNodes))
	return (bh.finalRightSkewSize + bh.finalLeftSkewSize) * (joinNodeNums - 1) / joinNodeNums
}

func(bh *BaseHash) CalculateCost() int64 {
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

func(bh *BaseHash) ResultCost() int64 {
	joinNodeNums := int64(len(bh.joinInfo.joinNodes))
	var netSkewCount int64
	addCount := func (value int) {
		bucketIdx, err := bh.calBucketIdx(int64(value), joinNodeNums)
		if err != nil || bh.joinInfo.joinNodes[bucketIdx] == bh.joinInfo.gatewayID {
			return
		}
		lCount, _ := bh.joinInfo.leftSkewData.Get(value)
		rCount, _ := bh.joinInfo.rightSkewData.Get(value)
		netSkewCount += int64(lCount) * int64(rCount)
	}
	bh.joinInfo.intersect.ForEach(addCount)

	return netSkewCount
}

func(bh *BaseHash) WorkArgs() distsqlplan.HashJoinWorkArgs {
	leftHeavyHitters :=	make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(bh.finalLeftSkewData))
	rightHeavyHitters := make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, len(bh.finalRightSkewData))

	for i, value := range bh.finalLeftSkewData {
		leftHeavyHitters[i].Value = value
	}
	for i, value := range bh.finalRightSkewData {
		rightHeavyHitters[i].Value = value
	}
	return distsqlplan.HashJoinWorkArgs{
		HJType: 			distsqlplan.BASEHASH,
		LeftHeavyHitters: 	leftHeavyHitters,
		RightHeavyHitters: 	rightHeavyHitters,
	}
}

func GetHeavyHitters(tableReader *distsqlpb.TableReaderSpec, directory string) []distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter {
	fileName := directory + tableReader.Table.GetName() + ".skew"
	f, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil
	}

	heavyHitters := make([]distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter, 0)
	for idx := 0; idx < len(f);  {
		var value int64
		for f[idx] != ',' {
			value = value * 10 + int64(f[idx] - byte('0'))
			idx++
		}
		idx++
		var frequency int64
		for f[idx] != '\n' {
			frequency = frequency * 10 + int64(f[idx] - byte('0'))
			idx++
		}
		idx++

		heavyHitters = append(heavyHitters, distsqlpb.OutputRouterSpec_MixHashRouterRuleSpec_HeavyHitter{
			Value: value,
			Frequency: frequency,
		})
	}

	return heavyHitters
}

func MakeDecisionForHashJoin(
	dsp 				*DistSQLPlanner,
	n					*joinNode,
	processors 			[]distsqlplan.Processor,
	leftRouters 		[]distsqlplan.ProcessorIdx,
	rightRouters 		[]distsqlplan.ProcessorIdx,
	joinNodes			[]roachpb.NodeID,
	getewayID 			roachpb.NodeID,
	frequencyThreshold  float64,
) distsqlplan.HashJoinWorkArgs {
	_, leftOk := n.left.plan.(*scanNode)
	_, rightOk := n.right.plan.(*scanNode)
	if !leftOk || !rightOk {
		return distsqlplan.HashJoinWorkArgs{
			HJType: distsqlplan.BASEHASH,
		}
	}

	leftTableReader := processors[leftRouters[0]].Spec.Core.TableReader
	rightTableReader := processors[rightRouters[0]].Spec.Core.TableReader
	directory, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	directory += "/zipf/csv/" + rightTableReader.Table.GetName() + "_" + leftTableReader.Table.GetName() + "/"
	leftHeavyHitters := GetHeavyHitters(leftTableReader, directory)
	rightHeavyHitters := GetHeavyHitters(rightTableReader, directory)
	if leftHeavyHitters == nil && rightHeavyHitters == nil {
		return distsqlplan.HashJoinWorkArgs{
			HJType: distsqlplan.BASEHASH,
		}
	}

	helper := &distJoinHelper{
		leftHeavyHitters: 	leftHeavyHitters,
		rightHeavyHitters: 	rightHeavyHitters,
		processors: 		processors,
		leftRouters: 		leftRouters,
		rightRouters: 		rightRouters,
		joinNodes: 			joinNodes,
		gatewayID: 			getewayID,
	}

	if planBaseHash.Get(&dsp.st.SV) {
		return distsqlplan.HashJoinWorkArgs{HJType: distsqlplan.BASEHASH}
	}
	if planPRPD.Get(&dsp.st.SV) {
		prpd := PRPD{joinInfo: helper}
		prpd.Init()
		return prpd.WorkArgs()
	}
	if planPnR.Get(&dsp.st.SV) {
		pnr := PnR{joinInfo: helper}
		pnr.Init()
		return pnr.WorkArgs()
	}

	// adapte select method
	var costModels []CostModel
	costModels = append(costModels, &PRPD{joinInfo: helper})
	costModels = append(costModels, &PnR{joinInfo: helper})
	costModels = append(costModels, &BaseHash{joinInfo: helper})

	minCost := int64(-1)
	workArgs := distsqlplan.HashJoinWorkArgs{
		HJType: distsqlplan.BASEHASH,
	}
	selectType := "BaseHash"
	resultLog := ""
	for _, costModel := range costModels {
		costModel.Init()
		netWork1Cost := costModel.DistributeCost()
		calculateCost := costModel.CalculateCost()
		netWork2Cost := costModel.ResultCost()

		allCost := netWork1Cost + calculateCost + netWork2Cost
		if minCost == -1 || allCost < minCost {
			minCost = allCost
			workArgs = costModel.WorkArgs()
			selectType = costModel.Type()
		}
		resultLog += costModel.Type() + ":\n";
		resultLog += "netWork1Cost : "+ strconv.FormatInt(netWork1Cost, 10) + "\n"
		resultLog += "calculateCost : "+ strconv.FormatInt(calculateCost, 10) + "\n"
		resultLog += "netWork2Cost : "+ strconv.FormatInt(netWork2Cost, 10) + "\n"
		resultLog += "allCost : "+ strconv.FormatInt(allCost, 10) + "\n"
		resultLog += "\n"
	}
	// write result log
	resultLog += "select methods : " + selectType + "\n"
	f, _ := os.OpenFile(directory + "result.log", os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0666)
	io.WriteString(f, resultLog)
	f.Close()

	return workArgs
}