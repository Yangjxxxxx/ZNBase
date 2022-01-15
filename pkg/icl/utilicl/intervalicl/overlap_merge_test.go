package intervalicl

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// 写一个简单的版本的算法，使用的事实
// 端点是小整数(例如，填写一个[100][]字符串)和
// 用它来反复检查结果。这也可以用来生成测试
// 随机输入。
func TestOverlapCoveringMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
		// 输入是Ranges slice. 内部切片表示Range
		////作为偶数个int，它们是成对的端点。所以(1,2,2，
		//// 4)表示[/1，/2]和[/2，/4]。
		inputs [][]byte
		// 输出的Ranges与输入的格式相同
		expectedIntervals []byte
		// expectedPayloads is the output payloads, corresponding 1:1 with
		// entries in expectedIntervals. Each input range is given an int
		// payload from 0..N-1 where N is the total number of ranges in all
		// input coverings for this test. Each of these is formatted as a string
		// concatenation of these ints.
		//
		// So for covering [1, 2), [2, 3) and covering [1, 3), the output
		// payloads would be "02" for [1, 2) and "12" for [2, 3).
		expectedPayloads []string
	}{
		{"no input",
			[][]byte{},
			nil, nil,
		},
		{"one empty covering",
			[][]byte{{}},
			nil, nil,
		},
		{"two empty coverings",
			[][]byte{{}, {}},
			nil, nil,
		},
		{"one",
			[][]byte{{1, 2}, {}},
			[]byte{1, 2}, []string{"0"},
		},
		{"same",
			[][]byte{{1, 2}, {1, 2}},
			[]byte{1, 2}, []string{"01"},
		},
		{"overlap",
			[][]byte{{1, 3}, {2, 3}},
			[]byte{1, 2, 2, 3}, []string{"0", "01"}},
		{"overlap reversed",
			[][]byte{{2, 3}, {1, 3}},
			[]byte{1, 2, 2, 3}, []string{"1", "01"}},
		{"no overlap",
			[][]byte{{1, 2, 5, 6}, {3, 4}},
			[]byte{1, 2, 3, 4, 5, 6}, []string{"0", "2", "1"},
		},
		{"znbase range splits and merges",
			[][]byte{{1, 3, 3, 4}, {1, 4}, {1, 2, 2, 4}},
			[]byte{1, 2, 2, 3, 3, 4}, []string{"023", "024", "124"},
		},
		{"godoc example",
			[][]byte{{1, 2, 3, 4, 6, 7}, {1, 5}},
			[]byte{1, 2, 2, 3, 3, 4, 4, 5, 6, 7}, []string{"03", "3", "13", "3", "2"},
		},
		{"empty",
			[][]byte{{1, 2, 2, 2, 2, 2, 4, 5}, {1, 5}},
			[]byte{1, 2, 2, 2, 2, 4, 4, 5}, []string{"04", "124", "4", "34"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var payload int
			var inputs []Ranges
			for _, endpoints := range test.inputs {
				var c Ranges
				for i := 0; i < len(endpoints); i += 2 {
					c = append(c, Range{
						Low:     []byte{endpoints[i]},
						High:    []byte{endpoints[i+1]},
						Payload: payload,
					})
					payload++
				}
				inputs = append(inputs, c)
			}
			var outputIntervals []byte
			var outputPayloads []string
			for _, r := range OverlapCoveringMerge(inputs) {
				outputIntervals = append(outputIntervals, r.Low[0], r.High[0])
				var payload bytes.Buffer
				for _, p := range r.Payload.([]interface{}) {
					fmt.Fprintf(&payload, "%d", p.(int))
				}
				outputPayloads = append(outputPayloads, payload.String())
			}
			if !reflect.DeepEqual(outputIntervals, test.expectedIntervals) {
				t.Errorf("intervals got\n%v\nexpected\n%v", outputIntervals, test.expectedIntervals)
			}
			if !reflect.DeepEqual(outputPayloads, test.expectedPayloads) {
				t.Errorf("payloads got\n%v\nexpected\n%v", outputPayloads, test.expectedPayloads)
			}
		})
	}
}
