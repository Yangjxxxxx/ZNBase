package intervalicl

import (
	"bytes"
	"sort"
)

// Range 一个有效载荷的间隔。
type Range struct {
	Low     []byte
	High    []byte
	Payload interface{}
}

// Ranges 表示一组不重叠但可能不连续的间隔
type Ranges []Range

var _ sort.Interface = Ranges{}

func (c Ranges) Len() int      { return len(c) }
func (c Ranges) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c Ranges) Less(i, j int) bool {
	if cmp := bytes.Compare(c[i].Low, c[j].Low); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(c[i].High, c[j].High) < 0
}

// OverlapCoveringMerge a返回覆盖输入中每个范围的间隔集，
// 这样就不会有输出范围跨越输入端点。负载以“[]接口{}”的形式返回，其顺序与覆盖层中的相同。
//
// Example:
//  covering 1: [1, 2) -> 'a', [3, 4) -> 'b', [6, 7) -> 'c'
//  covering 2: [1, 5) -> 'd'
//  output: [1, 2) -> 'ad', [2, 3) -> `d`, [3, 4) -> 'bd', [4, 5) -> 'd', [6, 7) -> 'c'
//
func OverlapCoveringMerge(ranges []Ranges) []Range {
	for _, rg := range ranges {
		sort.Sort(rg)
	}
	// 验证每个rg中的范围不重叠。
	////每个rg现在都被排序了。重复遍历第一个范围
	////在每个rg中查找下一个输出范围。然后移除范围
	////这些都在前面的输出中得到了充分的体现
	/// / rg。
	//
	// 这是O(范围的数量*输入范围的总数)。
	////输出的范围数为O(输入范围的总数)和
	////每个的有效载荷是O(范围的数量)，所以我们不能做任何
	////最好不改变输出表示。也就是说,常量
	////重要的是，如果这个过程太慢，我们可以开始排序
	////和结束点(每个点都成为a的开始或结束的“事件”
	////范围)，并按顺序扫描它们，维护一个间隔列表
	/// /当前“开放”
	var ret []Range
	var previousEndKey []byte
	for {
		//找到下一个范围的开始键。它要么是结束键
		//刚刚添加到输出或最小启动键的范围
		//保持在范围内(如果有间隙)。
		var startKey []byte
		startKeySet := false
		for _, rg := range ranges {
			if len(rg) == 0 {
				continue
			}
			if !startKeySet || bytes.Compare(rg[0].Low, startKey) < 0 {
				startKey = rg[0].Low
				startKeySet = true
			}
		}
		if !startKeySet {
			break
		}
		if bytes.Compare(startKey, previousEndKey) < 0 {
			startKey = previousEndKey
		}

		//找到下一个范围的结束键。它是所有结束键的最小值
		//的范围，相交的开始和所有开始键的范围后
		//刚刚添加到输出的范围的结束键。
		var endKey []byte
		endKeySet := false
		for _, rg := range ranges {
			if len(rg) == 0 {
				continue
			}

			if bytes.Compare(rg[0].Low, startKey) > 0 {
				if !endKeySet || bytes.Compare(rg[0].Low, endKey) < 0 {
					endKey = rg[0].Low
					endKeySet = true
				}
			}
			if !endKeySet || bytes.Compare(rg[0].High, endKey) < 0 {
				endKey = rg[0].High
				endKeySet = true
			}
		}

		//收集与开始键和结束键相交的范围的所有有效载荷
		//选择。还可以使用end键<= the one just修剪任何范围
		//选中后，它们将不会输出。
		var payloads []interface{}
		for i := range ranges {
			// 因为我们选择了startKey和endKey，所以我们知道这一点
			// ranges[i][0].High >= endKey和 ranges[i][0].Low 是
			// either <= startKey，要么>= endKey。

			for len(ranges[i]) > 0 {
				if bytes.Compare(ranges[i][0].Low, startKey) > 0 {
					break
				}
				payloads = append(payloads, ranges[i][0].Payload)
				if !bytes.Equal(ranges[i][0].High, endKey) {
					break
				}
				ranges[i] = ranges[i][1:]
			}
		}

		ret = append(ret, Range{
			Low:     startKey,
			High:    endKey,
			Payload: payloads,
		})
		previousEndKey = endKey
	}

	return ret
}
