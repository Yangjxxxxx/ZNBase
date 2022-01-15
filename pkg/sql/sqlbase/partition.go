// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"context"
	"fmt"
	"math"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/interval"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// PartitionSpecialValCode identifies a special value.
type PartitionSpecialValCode uint64

// partitionTag identifies range types
type partitionTag bool

const (
	// PartitionDefaultVal represents the special DEFAULT value.
	PartitionDefaultVal PartitionSpecialValCode = 0
	// PartitionMaxVal represents the special MAXVALUE value.
	PartitionMaxVal PartitionSpecialValCode = 1
	// PartitionMinVal represents the special MINVALUE value.
	PartitionMinVal PartitionSpecialValCode = 2

	partitiomTagFrom partitionTag = true
	partitiomTagTo   partitionTag = false
)

func (c PartitionSpecialValCode) String() string {
	switch c {
	case PartitionDefaultVal:
		return (tree.DefaultVal{}).String()
	case PartitionMinVal:
		return (tree.PartitionMinVal{}).String()
	case PartitionMaxVal:
		return (tree.PartitionMaxVal{}).String()
	}
	panic("unreachable")
}

// PartitionTuple represents a tuple in a partitioning specification.
//
// It contains any number of true datums, stored in the Datums field, followed
// by any number of special partitioning values, represented by the Special and
// SpecialCount fields.
type PartitionTuple struct {
	Datums       tree.Datums
	Special      PartitionSpecialValCode
	SpecialCount int
}

func (t *PartitionTuple) String() string {
	f := tree.NewFmtCtx(tree.FmtRawStrings)
	f.WriteByte('(')
	for i := 0; i < len(t.Datums)+t.SpecialCount; i++ {
		if i > 0 {
			f.WriteString(", ")
		}
		if i < len(t.Datums) {
			f.FormatNode(t.Datums[i])
		} else {
			f.WriteString(t.Special.String())
		}
	}
	f.WriteByte(')')
	return f.CloseAndGetString()
}

// DecodePartitionTuple parses columns (which are a prefix of the columns of
// `idxDesc`) encoded with the "value" encoding and returns the parsed datums.
// It also reencodes them into a key as they would be for `idxDesc` (accounting
// for index dirs, interleaves, subpartitioning, etc).
//
// For a list partitioning, this returned key can be used as a prefix scan to
// select all rows that have the given columns as a prefix (this is true even if
// the list partitioning contains DEFAULT).
//
// Examples of the key returned for a list partitioning:
//   - (1, 2) -> /table/index/1/2
//   - (1, DEFAULT) -> /table/index/1
//   - (DEFAULT, DEFAULT) -> /table/index
//
// For a range partitioning, this returned key can be used as a exclusive end
// key to select all rows strictly less than ones with the given columns as a
// prefix (this is true even if the range partitioning contains MINVALUE or
// MAXVALUE).
//
// Examples of the key returned for a range partitioning:
//   - (1, 2) -> /table/index/1/3
//   - (1, MAXVALUE) -> /table/index/2
//   - (MAXVALUE, MAXVALUE) -> (/table/index).PrefixEnd()
//
// NB: It is checked here that if an entry for a list partitioning contains
// DEFAULT, everything in that entry "after" also has to be DEFAULT. So, (1, 2,
// DEFAULT) is valid but (1, DEFAULT, 2) is not. Similarly for range
// partitioning and MINVALUE/MAXVALUE.
func DecodePartitionTuple(
	a *DatumAlloc,
	tableDesc *TableDescriptor,
	idxDesc *IndexDescriptor,
	partDesc *PartitioningDescriptor,
	valueEncBuf []byte,
	prefixDatums tree.Datums,
) (*PartitionTuple, []byte, error) {
	if len(prefixDatums)+int(partDesc.NumColumns) > len(idxDesc.ColumnIDs) {
		return nil, nil, fmt.Errorf("not enough columns in index for this partitioning")
	}

	t := &PartitionTuple{
		Datums: make(tree.Datums, 0, int(partDesc.NumColumns)),
	}

	colIDs := idxDesc.ColumnIDs[len(prefixDatums) : len(prefixDatums)+int(partDesc.NumColumns)]
	for _, colID := range colIDs {
		col, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, nil, err
		}
		if _, dataOffset, _, typ, err := encoding.DecodeValueTag(valueEncBuf); err != nil {
			return nil, nil, errors.Wrap(err, "decoding")
		} else if typ == encoding.NotNull {
			// NOT NULL signals that a PartitionSpecialValCode follows
			var valCode uint64
			valueEncBuf, _, valCode, err = encoding.DecodeNonsortingUvarint(valueEncBuf[dataOffset:])
			if err != nil {
				return nil, nil, err
			}
			nextSpecial := PartitionSpecialValCode(valCode)
			if t.SpecialCount > 0 && t.Special != nextSpecial {
				return nil, nil, errors.Errorf("non-%[1]s value (%[2]s) not allowed after %[1]s",
					t.Special, nextSpecial)
			}
			t.Special = nextSpecial
			t.SpecialCount++
		} else {
			var datum tree.Datum
			datum, valueEncBuf, err = DecodeTableValue(a, col.Type.ToDatumType(), valueEncBuf)
			if err != nil {
				return nil, nil, errors.Wrap(err, "decoding")
			}
			if t.SpecialCount > 0 {
				return nil, nil, errors.Errorf("non-%[1]s value (%[2]s) not allowed after %[1]s",
					t.Special, datum)
			}
			t.Datums = append(t.Datums, datum)
		}
	}
	if len(valueEncBuf) > 0 {
		return nil, nil, errors.New("superfluous data in encoded value")
	}

	allDatums := append(prefixDatums, t.Datums...)
	colMap := make(map[ColumnID]int, len(allDatums))
	for i := range allDatums {
		colMap[idxDesc.ColumnIDs[i]] = i
	}

	indexKeyPrefix := MakeIndexKeyPrefix(tableDesc, idxDesc.ID)
	key, _, err := EncodePartialIndexKey(
		tableDesc, idxDesc, len(allDatums), colMap, allDatums, indexKeyPrefix)
	if err != nil {
		return nil, nil, err
	}

	// Currently, key looks something like `/table/index/1`. Given a range
	// partitioning of (1), we're done. This can be used as the exclusive end
	// key of a scan to fetch all rows strictly less than (1).
	//
	// If `specialIdx` is not the sentinel, then we're actually in a case like
	// `(1, MAXVALUE, ..., MAXVALUE)`. Since this index could have a descending
	// nullable column, we can't rely on `/table/index/1/0xff` to be _strictly_
	// larger than everything it should match. Instead, we need `PrefixEnd()`.
	// This also intuitively makes sense; we're essentially a key that is
	// guaranteed to be less than `(2, MINVALUE, ..., MINVALUE)`.
	if t.SpecialCount > 0 && t.Special == PartitionMaxVal {
		key = roachpb.Key(key).PrefixEnd()
	}

	return t, key, nil
}

//DecodeDefaultPartitionTuple used to encode partition with default value and type is not int
func DecodeDefaultPartitionTuple(
	a *DatumAlloc,
	tableDesc *TableDescriptor,
	idxDesc *IndexDescriptor,
	partDesc *PartitioningDescriptor,
	valueEncBuf []byte,
	prefixDatums tree.Datums,
) (*PartitionTuple, []byte, error) {
	if len(prefixDatums)+int(partDesc.NumColumns) > len(idxDesc.ColumnIDs) {
		return nil, nil, fmt.Errorf("not enough columns in index for this partitioning")
	}

	t := &PartitionTuple{
		Datums: make(tree.Datums, 0, int(partDesc.NumColumns)),
	}

	colIDs := idxDesc.ColumnIDs[len(prefixDatums) : len(prefixDatums)+int(partDesc.NumColumns)]
	for _, colID := range colIDs {
		col, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, nil, err
		}
		if _, dataOffset, _, typ, err := encoding.DecodeValueTag(valueEncBuf); err != nil {
			return nil, nil, errors.Wrap(err, "decoding")
		} else if typ == encoding.NotNull {
			// NOT NULL signals that a PartitionSpecialValCode follows
			var valCode uint64
			valueEncBuf, _, valCode, err = encoding.DecodeNonsortingUvarint(valueEncBuf[dataOffset:])
			if err != nil {
				return nil, nil, err
			}
			nextSpecial := PartitionSpecialValCode(valCode)
			if t.SpecialCount > 0 && t.Special != nextSpecial {
				return nil, nil, errors.Errorf("non-%[1]s value (%[2]s) not allowed after %[1]s",
					t.Special, nextSpecial)
			}
			t.Special = nextSpecial
			t.SpecialCount++
		} else {
			var datum tree.Datum
			_, _, _, typ, _ := encoding.DecodeValueTag(valueEncBuf)
			if col.Type.ToDatumType() != types.Int && typ == encoding.Int {
				datum, valueEncBuf, err = DecodeTableValue(a, types.Int, valueEncBuf)
			} else {
				datum, valueEncBuf, err = DecodeTableValue(a, col.Type.ToDatumType(), valueEncBuf)
			}

			if err != nil {
				return nil, nil, errors.Wrap(err, "decoding")
			}
			if t.SpecialCount > 0 {
				return nil, nil, errors.Errorf("non-%[1]s value (%[2]s) not allowed after %[1]s",
					t.Special, datum)
			}
			t.Datums = append(t.Datums, datum)
		}
	}
	if len(valueEncBuf) > 0 {
		return nil, nil, errors.New("superfluous data in encoded value")
	}

	allDatums := append(prefixDatums, t.Datums...)
	colMap := make(map[ColumnID]int, len(allDatums))
	for i := range allDatums {
		colMap[idxDesc.ColumnIDs[i]] = i
	}

	indexKeyPrefix := MakeIndexKeyPrefix(tableDesc, idxDesc.ID)
	key, _, err := EncodePartialIndexKey(
		tableDesc, idxDesc, len(allDatums), colMap, allDatums, indexKeyPrefix)
	if err != nil {
		return nil, nil, err
	}

	// Currently, key looks something like `/table/index/1`. Given a range
	// partitioning of (1), we're done. This can be used as the exclusive end
	// key of a scan to fetch all rows strictly less than (1).
	//
	// If `specialIdx` is not the sentinel, then we're actually in a case like
	// `(1, MAXVALUE, ..., MAXVALUE)`. Since this index could have a descending
	// nullable column, we can't rely on `/table/index/1/0xff` to be _strictly_
	// larger than everything it should match. Instead, we need `PrefixEnd()`.
	// This also intuitively makes sense; we're essentially a key that is
	// guaranteed to be less than `(2, MINVALUE, ..., MINVALUE)`.
	if t.SpecialCount > 0 && t.Special == PartitionMaxVal {
		key = roachpb.Key(key).PrefixEnd()
	}

	return t, key, nil
}

type partitionInterval struct {
	name  string
	start roachpb.Key
	end   roachpb.Key
}

var _ interval.Interface = partitionInterval{}

// ID is part of `interval.Interface` but unused in validatePartitioningDescriptor.
func (ps partitionInterval) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ps partitionInterval) Range() interval.Range {
	return interval.Range{Start: []byte(ps.start), End: []byte(ps.end)}
}

// GetPartitionByTableDesc is the entry to get the matching partition linked list.
func GetPartitionByTableDesc(
	table *TableDescriptor, partitionName tree.Name, hasDefault bool,
) (partitionList *Partitions, index *IndexDescriptor, err error) {
	fmtCtx1 := tree.NewFmtCtx(tree.FmtSimple)
	partitionName.Format(fmtCtx1)
	matchName := fmtCtx1.String()
	partitionList, index, err = ForeachNonDropIndexForPartition(table, matchName, hasDefault)
	if err != nil {
		return partitionList, nil, err
	}
	if index == nil {
		return nil, nil, errors.New("partition isn't found")
	}
	return partitionList, index, nil
}

// ForeachNonDropIndexForPartition used to iterate through all the indexs' Partitions in the table.
func ForeachNonDropIndexForPartition(
	table *TableDescriptor, matchName string, hasDefault bool,
) (partitionList *Partitions, indexGet *IndexDescriptor, err error) {
	indexEqual := func(l string) bool {
		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		name := tree.Name(l)
		name.Format(fmtCtx)
		return fmtCtx.String() == matchName
	}
	if table.IsPhysicalTable() {
		if indexEqual(table.PrimaryIndex.Name) && hasDefault {
			return addPartitionHasDefault(table, &table.PrimaryIndex, &table.PrimaryIndex.Partitioning, 0)
		}
		partitionList = addPartitioningList(table, &table.PrimaryIndex, &table.PrimaryIndex.Partitioning, matchName, hasDefault, 0)
		if partitionList != nil {
			return judgeIndexWithDefault(partitionList, table, &table.PrimaryIndex, &table.PrimaryIndex.Partitioning, matchName, hasDefault)
		}
	}
	for i := range table.Indexes {
		if indexEqual(table.Indexes[i].Name) && hasDefault {
			return addPartitionHasDefault(table, &table.Indexes[i], &table.Indexes[i].Partitioning, 0)
		}
		partitionList = addPartitioningList(table, &table.Indexes[i], &table.Indexes[i].Partitioning, matchName, hasDefault, 0)
		if partitionList != nil {
			return judgeIndexWithDefault(partitionList, table, &table.Indexes[i], &table.Indexes[i].Partitioning, matchName, hasDefault)
		}
	}
	for _, m := range table.Mutations {
		if idx := m.GetIndex(); idx != nil && m.Direction == DescriptorMutation_ADD {
			if indexEqual(idx.Name) && hasDefault {
				return addPartitionHasDefault(table, idx, &idx.Partitioning, 0)
			}
			partitionList = addPartitioningList(table, idx, &idx.Partitioning, matchName, hasDefault, 0)
			if partitionList != nil {
				return judgeIndexWithDefault(partitionList, table, idx, &idx.Partitioning, matchName, hasDefault)
			}
		}
	}
	// partitionList is nil
	if hasDefault {
		return nil, nil, errors.Errorf("partition %s default does not exist", matchName)
	}
	return nil, nil, errors.Errorf("partition %s does not exist", matchName)
}

func addPartitionHasDefault(
	table *TableDescriptor,
	index *IndexDescriptor,
	partitioning *PartitioningDescriptor,
	colOffset int,
) (*Partitions, *IndexDescriptor, error) {
	a := &DatumAlloc{}
	if partitioning.NumColumns == 0 {
		return nil, index, errors.Errorf("index %s does not have partition", index.Name)
	}
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	partitionList := &Partitions{}
	for _, l := range partitioning.List {
		if err := partitionList.DecodePartitionValues(l, a, table, index, partitioning, fakePrefixDatums, true); err != nil {
			return nil, index, err
		}
		if !(partitionList.Start[0].Special == PartitionDefaultVal && partitionList.Start[0].SpecialCount > 0) {
			tuple := &PartitionTuple{}
			tuple.Special = PartitionDefaultVal
			tuple.SpecialCount = int(partitioning.NumColumns)
			partitionList.Start = append([]*PartitionTuple{tuple}, partitionList.Start...)
		}
	}
	if len(partitionList.Start) == 1 {
		return nil, index, nil
	}

	for _, r := range partitioning.Range {
		fromTuple, _, err := DecodePartitionTuple(
			a, table, index, partitioning, r.FromInclusive, fakePrefixDatums)
		if err != nil {
			return nil, index, err
		}
		toTuple, _, err := DecodePartitionTuple(
			a, table, index, partitioning, r.ToExclusive, fakePrefixDatums)
		if err != nil {
			return nil, index, err
		}
		partitionList.Start = append(partitionList.Start, fromTuple)
		partitionList.End = append(partitionList.End, toTuple)
		partitionList.IsDefault = true
	}
	return partitionList, index, nil
}

func judgeIndexWithDefault(
	partitionList *Partitions,
	table *TableDescriptor,
	index *IndexDescriptor,
	partitioning *PartitioningDescriptor,
	matchName string,
	hasDefault bool,
) (*Partitions, *IndexDescriptor, error) {
	if partitionList.Start[0].Special == PartitionDefaultVal && partitionList.Start[0].SpecialCount > 0 {
		if err := partitionList.addPartitioningIndex(table, index, partitioning, matchName, 0); err != nil {
			return partitionList, nil, err
		}
		has := true
		for _, v := range partitionList.Start {
			if !(v.Special == PartitionDefaultVal && v.SpecialCount > 0) {
				has = false
				break
			}
		}
		if has && !hasDefault {
			return nil, index, nil
		}
	}

	return partitionList, index, nil
}

// addPartitioningIndex returns all siblings of the partition when the first partition is the default value
func (p *Partitions) addPartitioningIndex(
	table *TableDescriptor,
	index *IndexDescriptor,
	partitioning *PartitioningDescriptor,
	matchName string,
	colOffset int,
) error {
	a := &DatumAlloc{}
	if partitioning.NumColumns == 0 {
		return nil
	}
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	for _, l := range partitioning.List {
		if l.Name == matchName {
			continue
		}
		for _, values := range l.Values {
			tuple, _, err := DecodePartitionTuple(
				a, table, index, partitioning, values, fakePrefixDatums)
			if err != nil {
				return err
			}
			p.Start = append(p.Start, tuple)
		}
	}

	for _, r := range partitioning.Range {
		if r.Name == matchName {
			continue
		}
		fromTuple, _, err := DecodePartitionTuple(
			a, table, index, partitioning, r.FromInclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		toTuple, _, err := DecodePartitionTuple(
			a, table, index, partitioning, r.ToExclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		p.Start = append(p.Start, fromTuple)
		p.End = append(p.Start, toTuple)
	}

	return nil
}

// AddPartitioningList returns all siblings of the partition when the subpartition is the default value
func addPartitioningList(
	table *TableDescriptor,
	index *IndexDescriptor,
	partitioning *PartitioningDescriptor,
	matchName string,
	hasDefault bool,
	colOffset int,
) *Partitions {
	a := &DatumAlloc{}
	if partitioning.NumColumns == 0 {
		return nil
	}
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	partitionList := &Partitions{}
	for _, l := range partitioning.List {
		if l.Name == matchName {
			if hasDefault {
				var err error
				if partitionList.Next, _, err = addPartitionHasDefault(table, index, &l.Subpartitioning, colOffset+int(partitioning.NumColumns)); err != nil {
					return nil
				}
			}
			if err := partitionList.DecodePartitionValues(l, a, table, index, partitioning, fakePrefixDatums, hasDefault); err != nil {
				return nil
			}
			return partitionList
		}
		next := addPartitioningList(table, index, &l.Subpartitioning, matchName, hasDefault, colOffset+int(partitioning.NumColumns))
		if next != nil {
			if next.Start[0].Special == PartitionDefaultVal && next.Start[0].SpecialCount > 0 {
				if err := next.DecodeDefaultPartitionValues(a, table, index, &l.Subpartitioning, matchName, fakePrefixDatums); err != nil {
					return nil
				}
			}
			if err := partitionList.DecodePartitionValues(l, a, table, index, partitioning, fakePrefixDatums, hasDefault); err != nil {
				return nil
			}
			partitionList.Next = next
			return partitionList
		}
	}

	for _, r := range partitioning.Range {
		if string(r.Name) == matchName {
			if hasDefault {
				return nil
			}
			fromTuple, _, err := DecodePartitionTuple(
				a, table, index, partitioning, r.FromInclusive, fakePrefixDatums)
			if err != nil {
				return nil
			}
			toTuple, _, err := DecodePartitionTuple(
				a, table, index, partitioning, r.ToExclusive, fakePrefixDatums)
			if err != nil {
				return nil
			}
			partitionList.Start = append(partitionList.Start, fromTuple)
			partitionList.End = append(partitionList.End, toTuple)
			return partitionList
		}
	}
	return nil
}

// DecodePartitionValues decodes all values for the specified PartitioningDescriptor_List
func (p *Partitions) DecodePartitionValues(
	l PartitioningDescriptor_List,
	a *DatumAlloc,
	table *TableDescriptor,
	index *IndexDescriptor,
	partitioning *PartitioningDescriptor,
	fakePrefixDatums tree.Datums,
	hasDefault bool,
) error {
	for _, values := range l.Values {
		tuple, _, err := DecodePartitionTuple(
			a, table, index, partitioning, values, fakePrefixDatums)
		if err != nil {
			return err
		}
		if tuple.Special == PartitionDefaultVal && tuple.SpecialCount > 0 {
			if hasDefault {
				p.Start = append([]*PartitionTuple{tuple}, p.Start...)
			} else {
				p.Start = []*PartitionTuple{tuple}
			}
			return nil
		}
		p.Start = append(p.Start, tuple)
	}
	return nil
}

// DecodeDefaultPartitionValues aimed at the specified index to decode all the value
func (p *Partitions) DecodeDefaultPartitionValues(
	a *DatumAlloc,
	table *TableDescriptor,
	index *IndexDescriptor,
	partitioning *PartitioningDescriptor,
	matchName string,
	fakePrefixDatums tree.Datums,
) error {
	for _, l := range partitioning.List {
		if l.Name == matchName {
			continue
		}
		for _, values := range l.Values {
			tuple, _, err := DecodeDefaultPartitionTuple(
				a, table, index, partitioning, values, fakePrefixDatums)
			if err != nil {
				return err
			}
			p.Start = append(p.Start, tuple)
		}
	}

	for _, r := range partitioning.Range {
		if r.Name == matchName {
			continue
		}
		fromTuple, _, err := DecodePartitionTuple(
			a, table, index, partitioning, r.FromInclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		toTuple, _, err := DecodePartitionTuple(
			a, table, index, partitioning, r.ToExclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		p.Start = append(p.Start, fromTuple)
		p.End = append(p.Start, toTuple)
	}
	return nil
}

// Partitions are used to store partition information for each layer and form linked lists
type Partitions struct {
	Next      *Partitions
	Start     []*PartitionTuple
	End       []*PartitionTuple
	IsDefault bool
}

func isNullExpr(expr tree.Datum) bool {
	return expr.Compare(nil, tree.DNull) == 0
}

var (
	// NotExpr takes the expression nonpositive
	NotExpr = func(expr tree.Expr) tree.Expr {
		return &tree.NotExpr{Expr: expr}
	}
	// AndExpr combines two expressions as and
	AndExpr = func(l, r tree.Expr) tree.Expr {
		return &tree.AndExpr{Left: l, Right: r}
	}
	// OrExpr combines two expressions as or
	OrExpr = func(l, r tree.Expr) tree.Expr {
		return &tree.OrExpr{Left: l, Right: r}
	}
	// TwoExprs merges an array into an expression
	TwoExprs = func(exprs []tree.Expr, f func(l, r tree.Expr) tree.Expr) tree.Expr {
		if exprs == nil {
			return nil
		}
		res := exprs[0]
		for i := 1; i < len(exprs); i++ {
			res = f(res, exprs[i])
		}
		return res
	}
	// TwoExpr left or right may be nil
	TwoExpr = func(left, right tree.Expr, f func(l, r tree.Expr) tree.Expr) tree.Expr {
		var res tree.Expr
		if left != nil {
			if right != nil {
				res = f(left, right)
			} else {
				res = left
			}
		} else if right != nil {
			res = right
		}
		return res
	}
	// ColumnExpr converts the data column dividing the partition into an expression
	ColumnExpr = func(s string) tree.Expr {
		return &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{s}}
	}
	// EqualWithNullExpr combines two expressions as equal
	EqualWithNullExpr = func(s string, v tree.Datum, hasDefault bool) tree.Expr {
		// 等值null判定
		if isNullExpr(v) {
			return &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: ColumnExpr(s), Right: tree.DNull}
		}
		var res tree.Expr
		res = &tree.ComparisonExpr{Operator: tree.EQ, Left: ColumnExpr(s), Right: v}
		if hasDefault {
			// 等值取非 is not null 判定
			res = AndExpr(res, &tree.ComparisonExpr{Operator: tree.IsDistinctFrom, Left: ColumnExpr(s), Right: tree.DNull})
		}
		return res
	}
	// LessExpr combines two expressions as less
	LessExpr = func(l, r tree.Expr) tree.Expr {
		return &tree.ComparisonExpr{Operator: tree.LT, Left: l, Right: r}
	}
	// GreatExpr combines two expressions as great
	GreatExpr = func(l, r tree.Expr) tree.Expr {
		return &tree.ComparisonExpr{Operator: tree.GT, Left: l, Right: r}
	}
	// GreatEqualExpr combines two expressions as GE
	GreatEqualExpr = func(l, r tree.Expr) tree.Expr {
		return &tree.ComparisonExpr{Operator: tree.GE, Left: l, Right: r}
	}
	// IsNullExpr data column is null
	IsNullExpr = func(s string) tree.Expr {
		return &tree.ComparisonExpr{Operator: tree.IsNotDistinctFrom, Left: ColumnExpr(s), Right: tree.DNull}
	}
	falseExpr = tree.DBoolFalse
)

// BuildPartitionWhere returns the entry of the corresponding expression through the specified partition list
func BuildPartitionWhere(index *IndexDescriptor, p *Partitions) tree.Expr {
	return buildPartition(index.ColumnNames, 0, p)
}

// buildPartition returns the corresponding expression through the specified partition list
func buildPartition(cols []string, offset int, p *Partitions) tree.Expr {
	if p == nil {
		return nil
	}

	if offset+len(p.Start[0].Datums) > len(cols) {
		log.Errorf(context.Background(), "buildPartition failed: offset %d + datum %d > cols %d",
			offset, len(p.Start[0].Datums), len(cols))
		return nil
	}
	var left tree.Expr
	var orList []tree.Expr
	var defaultExpr tree.Expr
	defaultCount := math.MaxInt64
	var isDefault = false
	if p.End == nil {
		// list
		for _, s := range p.Start {
			if s.SpecialCount > 0 && !isDefault {
				// 多列取and
				defaultCount = s.SpecialCount
				var andList []tree.Expr
				for i, expr := range s.Datums {
					andList = append(andList, EqualWithNullExpr(cols[offset+i], expr, isDefault))
				}
				defaultExpr = TwoExprs(andList, AndExpr)
				// 所有条件取非
				isDefault = true
			} else {
				// 多列取and
				if s.SpecialCount >= defaultCount {
					continue
				}
				var andList []tree.Expr
				for i, expr := range s.Datums {
					andList = append(andList, EqualWithNullExpr(cols[offset+i], expr, isDefault))
				}
				curExpr := TwoExprs(andList, AndExpr)
				// 多条件用or
				if curExpr != nil {
					orList = append(orList, curExpr)
				}
			}
		}
	} else {
		// range
		isDefault = p.IsDefault
		for i, s := range p.Start {
			curExpr := buildRange(cols, offset, s, p.End[i], isDefault)
			if curExpr != nil {
				orList = append(orList, curExpr)
			}
		}
	}
	left = TwoExprs(orList, OrExpr)
	if isDefault {
		var defaultAnd []tree.Expr
		if defaultExpr != nil {
			defaultAnd = append(defaultAnd, defaultExpr)
		}
		if left != nil {
			defaultAnd = append(defaultAnd, NotExpr(left))
		} else {
			defaultAnd = append(defaultAnd, falseExpr)
		}
		left = TwoExprs(defaultAnd, AndExpr)
	}
	if left == nil {
		return left
	}

	var right tree.Expr
	right = buildPartition(cols, offset+len(p.Start[0].Datums)+p.Start[0].SpecialCount, p.Next)

	if right != nil {
		return AndExpr(left, right)
	}
	return left
}

func buildRange(
	cols []string, offset int, from *PartitionTuple, to *PartitionTuple, hasDefault bool,
) tree.Expr {
	var curOrList, curAndList, eqAndList []tree.Expr
	var curNull, curExpr tree.Expr
	index := 0
	for ; len(from.Datums) > index && len(to.Datums) > index; index++ {
		// todo datum compare is better than string
		if from.Datums[index].String() == to.Datums[index].String() {
			eqAndList = append(eqAndList, EqualWithNullExpr(cols[offset+index], from.Datums[index], hasDefault))
		} else {
			break
		}
	}
	// isNull 判断查询是否需要补充null等值条件
	// hasNull 判断hasDefault是否需要补充null等值条件
	isNull, hasNull := false, true
	if len(from.Datums) > index {
		curExpr = EqualWithNullExpr(cols[offset+index], from.Datums[index], hasDefault)
		curList, eq := buildPartitionRange(cols, offset, from, index+1, curExpr, hasDefault, partitiomTagFrom)
		if curList != nil {
			curOrList = append(curOrList, curList...)
		}
		if isNullExpr(from.Datums[index]) {
			// 需要等值条件时添加
			if eq {
				curNull = curExpr
			}
		} else {
			if eq {
				curExpr = GreatEqualExpr(ColumnExpr(cols[offset+index]), from.Datums[index])
			} else {
				curExpr = GreatExpr(ColumnExpr(cols[offset+index]), from.Datums[index])
			}
			curAndList = append(curAndList, curExpr)
		}
	} else {
		// null 值大于 minvalue
		isNull, hasNull = true, false
	}
	if len(to.Datums) > index {
		curList, _ := buildPartitionRange(cols, offset, to, index+1,
			EqualWithNullExpr(cols[offset+index], to.Datums[index], hasDefault), hasDefault, partitiomTagTo)
		if curList != nil {
			curOrList = append(curOrList, curList...)
		}
		if isNullExpr(to.Datums[index]) {
			// to null小于null， 取消等值判断
			isNull = false
			// hasNull 已经是true 无需操作
			// false 条件
			curAndList = append(curAndList, falseExpr)
		} else {
			curAndList = append(curAndList, LessExpr(ColumnExpr(cols[offset+index]), to.Datums[index]))
		}
	} else {
		// to maxValue， 取消等值判断
		isNull = false
	}
	if isNull {
		curOrList = append(curOrList, IsNullExpr(cols[offset+index]))
	}
	// 当前层范围如1<a<3
	tmpExpr := TwoExprs(curAndList, AndExpr)
	// 当前层hasDefault null值处理
	if hasNull && hasDefault {
		if tmpExpr != nil {
			tmpExpr = AndExpr(tmpExpr, NotExpr(IsNullExpr(cols[offset+index])))
		} else {
			tmpExpr = NotExpr(IsNullExpr(cols[offset+index]))
		}
	}
	// 当前层带null值范围
	if curNull != nil {
		if tmpExpr != nil {
			// 有to边界
			tmpExpr = OrExpr(tmpExpr, curNull)
		}
	}
	// 子层所有条件并集
	tmpExpr = TwoExpr(tmpExpr, TwoExprs(curOrList, OrExpr), OrExpr)
	// 前置等值条件取交集
	return TwoExpr(TwoExprs(eqAndList, AndExpr), tmpExpr, AndExpr)
}

// 返回值orlist 等值判断 bool类型不足 换成status状态
func buildPartitionRange(
	cols []string,
	offset int,
	p *PartitionTuple,
	index int,
	prefix tree.Expr,
	hasDefault bool,
	tag partitionTag,
) ([]tree.Expr, bool) {
	if p == nil {
		return nil, tag == partitiomTagFrom
	}

	if index >= len(p.Datums) {
		if tag == partitiomTagFrom {
			if offset+index < len(cols) && index < len(p.Datums)+p.SpecialCount {
				// MINVALUE 要考虑NULL值情况
				res := AndExpr(prefix, IsNullExpr(cols[offset+index]))
				return []tree.Expr{res}, true
			}
		}
		// to MINVALUE MAXVALUE 无需任何条件
		return nil, tag == partitiomTagFrom
	}

	var res tree.Expr
	// 深搜的prefix条件
	curExpr := AndExpr(prefix, EqualWithNullExpr(cols[offset+index], p.Datums[index], hasDefault))
	// 先进性条件深搜
	next, eq := buildPartitionRange(cols, offset, p, index+1, curExpr, hasDefault, tag)

	if tag == partitiomTagFrom {
		// FROM NULL值 等价于MINVALUE
		if isNullExpr(p.Datums[index]) {
			if next != nil {
				return next, true
			}
			return nil, true
		}
		// 无子条件时
		if next == nil {
			if eq {
				res = AndExpr(prefix, GreatEqualExpr(ColumnExpr(cols[offset+index]), p.Datums[index]))
			}
			return []tree.Expr{res}, false
		}
		res = AndExpr(prefix, GreatExpr(ColumnExpr(cols[offset+index]), p.Datums[index]))
		return append([]tree.Expr{res}, AndExpr(curExpr, TwoExprs(next, OrExpr))), eq
	}
	if tag == partitiomTagTo {
		// to NULL值 等价于MINVALUE
		if isNullExpr(p.Datums[index]) {
			return nil, false
		}
		res = AndExpr(prefix, LessExpr(ColumnExpr(cols[offset+index]), p.Datums[index]))
		if next == nil {
			return []tree.Expr{res}, false
		}
		return append([]tree.Expr{res}, AndExpr(curExpr, TwoExprs(next, OrExpr))), eq
	}
	return nil, false
}
