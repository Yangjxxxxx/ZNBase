/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"bytes"
	"container/heap"
	"encoding/binary"

	"github.com/pkg/errors"
)

// ValueStruct represents the value info that can be associated with a key, but also the internal
// Meta field.
type ValueStruct struct {
	Meta      byte
	UserMeta  byte
	ExpiresAt uint64
	Value     []byte

	Version uint64 // This field is not serialized. Only for internal usage.
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// EncodedSize is the size of the ValueStruct when encoded
func (v *ValueStruct) EncodedSize() uint16 {
	sz := len(v.Value) + 2 // meta, usermeta.
	if v.ExpiresAt == 0 {
		return uint16(sz + 1)
	}

	enc := sizeVarint(v.ExpiresAt)
	return uint16(sz + enc)
}

// Decode uses the length of the slice to infer the length of the Value field.
func (v *ValueStruct) Decode(b []byte) {
	v.Meta = b[0]
	v.UserMeta = b[1]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(b[2:])
	v.Value = b[2+sz:]
}

// Encode expects a slice of length at least v.EncodedSize().
func (v *ValueStruct) Encode(b []byte) {
	b[0] = v.Meta
	b[1] = v.UserMeta
	sz := binary.PutUvarint(b[2:], v.ExpiresAt)
	copy(b[2+sz:], v.Value)
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
func (v *ValueStruct) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(v.Meta)
	buf.WriteByte(v.UserMeta)
	var enc [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(enc[:], v.ExpiresAt)
	buf.Write(enc[:sz])
	buf.Write(v.Value)
}

// Iterator is an interface for a basic iterator.
type Iterator interface {
	Next()
	Prev()
	Seek(key []byte)
	SeekForPrev(key []byte)
	Rewind()
	SeekToFirst()
	SeekToLast()
	Key() []byte
	Value() ValueStruct
	Valid() bool

	// All iterators should be closed so that file garbage collection works.
	Close() error
}

type elem struct {
	itr  Iterator
	nice int
	//reversed bool
}

type elemHeap []*elem

func (eh elemHeap) Len() int            { return len(eh) }
func (eh elemHeap) Swap(i, j int)       { eh[i], eh[j] = eh[j], eh[i] }
func (eh *elemHeap) Push(x interface{}) { *eh = append(*eh, x.(*elem)) }
func (eh *elemHeap) Pop() interface{} {
	// Remove the last element, because Go has already swapped 0th elem <-> last.
	old := *eh
	n := len(old)
	x := old[n-1]
	*eh = old[0 : n-1]
	return x
}
func (eh elemHeap) Less(i, j int) bool {
	reversed := false //TODO TODO 反向遍历也是按照“小->大”顺序排？?
	cmp := CompareKeys(eh[i].itr.Key(), eh[j].itr.Key())
	if cmp < 0 {
		return !reversed
	}
	if cmp > 0 {
		return reversed
	}
	// The keys are equal. In this case, lower nice take precedence. This is important.
	return eh[i].nice < eh[j].nice
}

type elemMaxHeap []*elem

func (eh elemMaxHeap) Len() int            { return len(eh) }
func (eh elemMaxHeap) Swap(i, j int)       { eh[i], eh[j] = eh[j], eh[i] }
func (eh *elemMaxHeap) Push(x interface{}) { *eh = append(*eh, x.(*elem)) }
func (eh *elemMaxHeap) Pop() interface{} {
	// Remove the last element, because Go has already swapped 0th elem <-> last.
	old := *eh
	n := len(old)
	x := old[n-1]
	*eh = old[0 : n-1]
	return x
}
func (eh elemMaxHeap) Less(i, j int) bool {
	cmp := CompareKeys(eh[i].itr.Key(), eh[j].itr.Key())
	if cmp < 0 {
		return false
	}
	if cmp > 0 {
		return true
	}
	// The keys are equal. In this case, lower nice take precedence. This is important.
	return eh[i].nice < eh[j].nice
}

// MergeIterator merges multiple iterators.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	minH        elemHeap    //小->大前序
	maxH        elemMaxHeap //大->小反序
	curKey      []byte
	curReversed bool //当前方向，默认false-前序遍历

	all []Iterator
}

// NewMergeIterator returns a new MergeIterator from a list of Iterators.
func NewMergeIterator(iters []Iterator) *MergeIterator {
	m := &MergeIterator{all: iters, curReversed: false}
	m.minH = make(elemHeap, 0, len(iters))
	m.maxH = make(elemMaxHeap, 0, len(iters))
	m.initHeap()
	return m
}

func (s *MergeIterator) storeKey(smallest Iterator) {
	if cap(s.curKey) < len(smallest.Key()) {
		s.curKey = make([]byte, 2*len(smallest.Key()))
	}
	s.curKey = s.curKey[:len(smallest.Key())]
	copy(s.curKey, smallest.Key())
}

func (s *MergeIterator) switchToForward() {
	s.curReversed = false
	if s.curKey != nil {
		for _, itr := range s.all {
			itr.Seek(s.curKey)
			if itr.Valid() && CompareKeys(s.curKey, itr.Key()) == 0 {
				//itr.Next()
			}
		}
	}
	s.initMinHeap()
}
func (s *MergeIterator) switchToBackward() {
	s.curReversed = true
	if s.curKey != nil {
		for _, itr := range s.all {
			itr.SeekForPrev(s.curKey)
			if itr.Valid() && CompareKeys(s.curKey, itr.Key()) == 0 {
				//itr.Prev()
			}
		}
	}
	s.initMaxHeap()
}

// initHeap checks all iterators and initializes our heap and array of keys.
// Whenever we reverse direction, we need to run this.
func (s *MergeIterator) initHeap_bak() {
	s.minH = s.minH[:0]
	for idx, itr := range s.all {
		if !itr.Valid() {
			continue
		}
		e := &elem{itr: itr, nice: idx}
		s.minH = append(s.minH, e)
	}
	heap.Init(&s.minH)
	for len(s.minH) > 0 {
		it := s.minH[0].itr
		if it == nil || !it.Valid() {
			heap.Pop(&s.minH)
			continue
		}
		s.storeKey(s.minH[0].itr)
		break
	}
}
func (s *MergeIterator) initHeap() {
	if s.curReversed {
		s.initMaxHeap()
	} else {
		s.initMinHeap()
	}
}
func (s *MergeIterator) initMaxHeap() {
	s.curKey = nil
	s.maxH = s.maxH[:0]
	for idx, itr := range s.all {
		if !itr.Valid() {
			continue
		}

		e := &elem{itr: itr, nice: idx}
		s.maxH = append(s.maxH, e)
	}
	if len(s.maxH) > 0 {
		heap.Init(&s.maxH)
		s.storeKey(s.maxH[0].itr)
	}
}
func (s *MergeIterator) initMinHeap() {
	s.curKey = nil
	s.minH = s.minH[:0]
	for idx, itr := range s.all {
		if !itr.Valid() {
			continue
		}
		e := &elem{itr: itr, nice: idx}
		s.minH = append(s.minH, e)
	}
	if len(s.minH) > 0 {
		heap.Init(&s.minH)
		s.storeKey(s.minH[0].itr)
	}
}
func (s *MergeIterator) currentItr() Iterator {
	if s.curReversed {
		if len(s.maxH) == 0 {
			return nil
		} else {
			return s.maxH[0].itr
		}
	} else {
		if len(s.minH) == 0 {
			return nil
		} else {
			return s.minH[0].itr
		}
	}
}

// Valid returns whether the MergeIterator is at a valid element.
func (s *MergeIterator) Valid() bool {
	if s == nil {
		return false
	}
	itr := s.currentItr()
	if itr == nil {
		return false
	}
	return itr.Valid()
}

// Key returns the key associated with the current iterator
func (s *MergeIterator) Key() []byte {
	itr := s.currentItr()
	if itr == nil {
		return nil
	}
	return s.currentItr().Key()
}

// Value returns the value associated with the iterator.
func (s *MergeIterator) Value() ValueStruct {
	itr := s.currentItr()
	if itr == nil {
		return ValueStruct{}
	}
	return s.currentItr().Value()
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (s *MergeIterator) Next() {
	if s.curReversed {
		s.switchToForward()
	}
	if len(s.minH) == 0 {
		return
	}

	smallest := s.minH[0].itr
	smallest.Next()

	for len(s.minH) > 0 {
		smallest = s.minH[0].itr
		if !smallest.Valid() {
			heap.Pop(&s.minH)
			continue
		}

		heap.Fix(&s.minH, 0)
		smallest = s.minH[0].itr
		if smallest.Valid() {
			if !bytes.Equal(smallest.Key(), s.curKey) {
				break
			}
			smallest.Next()
		}
	}
	if !smallest.Valid() {
		return
	}
	s.storeKey(smallest)
}
func (s *MergeIterator) Prev() {
	if !s.curReversed {
		s.switchToBackward()
	}
	if len(s.maxH) == 0 {
		return
	}

	maximum := s.maxH[0].itr
	maximum.Prev()

	for len(s.maxH) > 0 {
		maximum = s.maxH[0].itr
		if !maximum.Valid() {
			heap.Pop(&s.maxH)
			continue
		}

		heap.Fix(&s.maxH, 0)
		maximum = s.maxH[0].itr
		if maximum.Valid() {
			if !bytes.Equal(maximum.Key(), s.curKey) {
				break
			}
			maximum.Prev()
		}
	}
	if !maximum.Valid() {
		return
	}
	s.storeKey(maximum)
}

// Rewind seeks to first element (or last element for reverse iterator).
func (s *MergeIterator) Rewind() {
	for _, itr := range s.all {
		itr.Rewind()
	}
	s.curReversed = false
	s.initHeap()
}

// Seek brings us to element with key >= given key.
func (s *MergeIterator) Seek(key []byte) {
	for _, itr := range s.all {
		itr.Seek(key)
	}
	s.curReversed = false
	s.initHeap()
}
func (s *MergeIterator) SeekForPre(key []byte) {
	for _, itr := range s.all {
		itr.SeekForPrev(key)
	}
	s.curReversed = true
	s.initHeap()
}
func (s *MergeIterator) SeekToFirst() {
	for _, itr := range s.all {
		itr.SeekToFirst()
	}
	s.curReversed = false
	s.initHeap()
}
func (s *MergeIterator) SeekToLast() {
	for _, itr := range s.all {
		itr.SeekToLast()
	}
	s.curReversed = true
	s.initHeap()
}

// Close implements y.Iterator
func (s *MergeIterator) Close() error {
	for _, itr := range s.all {
		if err := itr.Close(); err != nil {
			return errors.Wrap(err, "MergeIterator")
		}
	}
	return nil
}
