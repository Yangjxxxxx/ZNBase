package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

type spanAndTime struct {
	roachpb.Span
	Time hlc.Timestamp
}

// String for debug
func (s *spanAndTime) String() string {
	var b bytes.Buffer
	b.WriteString(s.Span.String())
	v, err := json.Marshal(s.Time)
	if err != nil {
		fmt.Println("error:", err)
	}
	b.WriteByte(',')
	b.Write(v)
	return b.String()
}

func intentKeyOutOfRangeError() error {
	return fmt.Errorf("intent key out of range")
}

func intentCrossError() error {
	return fmt.Errorf("intent has crossed")
}

// mvccWriteIntentToKeys from writeInent to Keys
// demo intents 3, 10
// return       3, 3#,	9
func mvccWriteIntentTxnToKeys(intents []roachpb.LockUpdate, reverse bool) ([]spanAndTime, error) {
	if len(intents) == 0 {
		return nil, fmt.Errorf("No write intent to resolve! ")
	}
	if reverse {
		sort.Slice(intents, func(i, j int) bool {
			return bytes.Compare(intents[j].Span.Key, intents[i].Span.Key) < 0
		})
	} else {
		sort.Slice(intents, func(i, j int) bool {
			return bytes.Compare(intents[i].Span.Key, intents[j].Span.Key) < 0
		})
	}
	getSpan := func(intent *roachpb.LockUpdate) spanAndTime {
		endKey := intent.EndKey
		if len(endKey) == 0 {
			endKey = intent.Key.Next()
		}
		return spanAndTime{
			roachpb.Span{
				Key:    intent.Key,
				EndKey: endKey,
			},
			intent.Txn.WriteTimestamp.FloorPrev(),
		}
	}
	last := intents[0].Span
	res := []spanAndTime{getSpan(&intents[0])}
	for _, intent := range intents[1:] {
		if !intent.Span.Equal(last) {
			last = intent.Span
			res = append(res, getSpan(&intent))
		}
	}
	return res, nil
}

// mvccResolveWriteIntentRC is used for read committed
// demo start, end 1, 5  time 20
//			intent   3			 time 10
// return
//			1, 3, 20
//			3, 3#, 9
//			3, 5, 20
func mvccResolveWriteIntentRC(
	start, end roachpb.Key, timestamp hlc.Timestamp, intents []roachpb.LockUpdate, reverse bool,
) (spans []spanAndTime, err error) {
	var compare func(l, r roachpb.Key) bool
	if reverse {
		start, end = end, start
		compare = func(l, r roachpb.Key) bool {
			return roachpb.RKey(r).Less(roachpb.RKey(l))
		}
	} else {
		compare = func(l, r roachpb.Key) bool {
			return roachpb.RKey(l).Less(roachpb.RKey(r))
		}
	}
	if len(end) == 0 {
		return nil, emptyKeyError()
	}

	intentSpans, err := mvccWriteIntentTxnToKeys(intents, reverse)
	if err != nil {
		return nil, err
	}
	for _, span := range intentSpans {
		if compare(end, span.Key) {
			return nil, intentKeyOutOfRangeError()
		}
		if compare(start, span.Key) {
			if reverse {
				spans = append(spans, spanAndTime{
					roachpb.Span{Key: span.Key, EndKey: start}, timestamp,
				})
			} else {
				spans = append(spans, spanAndTime{
					roachpb.Span{Key: start, EndKey: span.Key}, timestamp,
				})
			}
		} else if start.Equal(span.Key) {
			// do nothing
		} else if compare(span.Key, start) {
			// raise error
			return nil, intentCrossError()
		}

		spans = append(spans, spanAndTime{
			span.Span, span.Time,
		})
		start = span.EndKey
	}
	if compare(start, end) {
		if reverse {
			spans = append(spans, spanAndTime{
				roachpb.Span{Key: end, EndKey: start}, timestamp,
			})
		} else {
			spans = append(spans, spanAndTime{
				roachpb.Span{Key: start, EndKey: end}, timestamp,
			})
		}
	}
	return spans, nil
}

// MVCCScanMerge is Merge the sorted kvData
func MVCCScanMerge(dst *[]byte, dstNum *int64, src []byte, srcNum int64) {
	*dst = append(*dst, src...)
	*dstNum += srcNum
}
