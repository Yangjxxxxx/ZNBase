// Copyright 2018  The Cockroach Authors.
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

package memo

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"unsafe"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/opt/props/physical"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
)

const (
	// offset64 is the initial hash value, and is taken from fnv.go
	offset64 = 14695981039346656037

	// prime64 is a large-ish prime number used in hashing and taken from fnv.go.
	prime64 = 1099511628211
)

// internHash is a 64-bit hash value, computed using the FNV-1a algorithm.
type internHash uint64

// interner interns relational and scalar expressions, which means that multiple
// equivalent expressions are mapped to a single in-memory instance. If two
// expressions with the same values for all fields are interned, the intern
// method will return the first expression in both cases. Interned expressions
// can therefore be checked for equivalence by simple pointer comparison.
// Equivalence is defined more strictly than SQL equivalence; two values are
// equivalent only if their binary encoded representations are identical. For
// example, positive and negative float64 values *are not* equivalent, whereas
// NaN float values *are* equivalent.
//
// To use interner, first call the Init method to initialize storage. Call the
// appropriate Intern method to retrieve the canonical instance of that
// expression. Release references to other instances, including the expression
// passed to Intern.
//
// The interner computes a hash function for each expression operator type that
// enables quick determination of whether an expression has already been added
// to the interner. A hashXXX and isXXXEqual method must be added for every
// unique type of every field in every expression struct. Optgen generates
// Intern methods which use these methods to compute the hash and check whether
// an equivalent expression is already present in the cache. Because hashing is
// used, interned expressions must remain immutable after interning. That is,
// once set, they can never be modified again, else their hash value would
// become invalid.
//
// The non-cryptographic hash function is adapted from fnv.go in Golang's
// standard library. That in turn was taken from FNV-1a, described here:
//
//   https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function
//
// Each expression type follows the same interning pattern:
//
//   1. Compute an int64 hash value for the expression using FNV-1a.
//   2. Do a fast 64-bit Go map lookup to determine if an expression with the
//      same hash is already in the cache.
//   3. If so, then test whether the existing expression is equivalent, since
//      there may be a hash value collision.
//   4. Expressions with colliding hash values are linked together in a list.
//      Rather than use an explicit linked list data structure, colliding
//      entries are rehashed using a randomly generated hash value that is
//      stored in the existing entry. This effectively uses the Go map as if it
//      were a hash table of size 2^64.
//
// This pattern enables very low overhead hashing of expressions - the
// allocation of a Go map with a fast 64-bit key, plus a couple of reusable
// scratch byte arrays.
type interner struct {
	// hasher is a helper struct to compute hashes and test equality.
	hasher hasher

	// cache is a helper struct that implements the interning pattern described
	// in the header over a Go map.
	cache internCache
}

// Clear clears all interned expressions. Expressions interned before the call
// to Clear will not be connected to expressions interned after.
func (in *interner) Clear() {
	in.cache.Clear()
}

// Count returns the number of expressions that have been interned.
func (in *interner) Count() int {
	return in.cache.Count()
}

var physPropsType = reflect.TypeOf((*physical.Required)(nil))
var physPropsTypePtr = uint64(reflect.ValueOf(physPropsType).Pointer())

// InternPhysicalProps interns a set of physical properties using the same
// pattern as that used by the expression intern methods, with one difference.
// This intern method does not force the incoming physical properties to escape
// to the heap. It does this by making a copy of the physical properties before
// adding them to the cache.
func (in *interner) InternPhysicalProps(val *physical.Required) *physical.Required {
	// Hash the physical.Required reflect type to distinguish it from other values.
	in.hasher.Init()
	in.hasher.HashUint64(physPropsTypePtr)
	in.hasher.HashPhysProps(val)

	// Loop over any items with the same hash value, checking for equality.
	in.cache.Start(in.hasher.hash)
	for in.cache.Next() {
		// There's an existing item, so check for equality.
		if existing, ok := in.cache.Item().(*physical.Required); ok {
			if in.hasher.IsPhysPropsEqual(val, existing) {
				// Found equivalent item, so return it.
				return existing
			}
		}
	}

	// Shallow copy the props to prevent "val" from escaping.
	copy := *val
	in.cache.Add(&copy)
	return &copy
}

// internCache is a helper class that implements the interning pattern described
// in the comment for the interner struct. Here is a usage example:
//
//   var cache internCache
//   cache.Start(hash)
//   for cache.Next() {
//     if isEqual(cache.Item(), other) {
//       // Found existing item in cache.
//     }
//   }
//   cache.Add(other)
//
// The calls to the Next method iterate over any entries with the same hash,
// until either a match is found or it is proven their are no matches, in which
// case the new item can be added to the cache.
type internCache struct {
	// cache is a Go map that's being used as if it were a hash table of size
	// 2^64. Items are hashed according to their 64-bit hash value, and any
	// colliding entries are linked together using the collision field in
	// cacheEntry.
	cache map[internHash]cacheEntry

	// hash stores the lookup value used by the next call to the Next method.
	hash internHash

	// prev stores the cache entry last fetched from the map by the Next method.
	prev cacheEntry
}

// cacheEntry is the Go map value. In case of hash value collisions it functions
// as a linked list node; its collision field is a randomly generated re-hash
// value that "points" to the colliding node. That node in turn can point to yet
// another colliding node, and so on. Walking a collision list therefore
// consists of computing an initial hash based on the value of the node, and
// then following the list of collision hash values by indexing into the cache
// map repeatedly.
type cacheEntry struct {
	item      interface{}
	collision internHash
}

// Item returns the cache item that was fetched by the Next method.
func (c *internCache) Item() interface{} {
	return c.prev.item
}

// Count returns the number of items in the cache.
func (c *internCache) Count() int {
	return len(c.cache)
}

// Clear clears all items in the cache.
func (c *internCache) Clear() {
	c.cache = nil
}

// Start prepares to look up an item in the cache by its hash value. It must be
// called before Next.
func (c *internCache) Start(hash internHash) {
	if c.cache == nil {
		c.cache = make(map[internHash]cacheEntry)
	}
	c.hash = hash
	c.prev = cacheEntry{}
}

// Next iterates over a collision list of cache items. It begins with the hash
// value set via the call to Start, and continues with any collision hash values
// it finds in the collision list. If it is at the end of an existing collision
// list, it generates a new random hash value and links it to the existing
// entry.
func (c *internCache) Next() bool {
	// If item is set, then the previous lookup must not have matched, so try to
	// get the next value in the collision list, or return false if at the end of
	// the list.
	if c.prev.item != nil {
		if c.prev.collision == 0 {
			// This was the last item in the collision list.
			return false
		}

		// A collision link already exists, so follow that.
		c.hash = c.prev.collision
	}

	var ok bool
	c.prev, ok = c.cache[c.hash]
	return ok
}

// Add inserts the given item into the cache. The caller should have already
// checked that the item is not yet in the cache.
func (c *internCache) Add(item interface{}) {
	if item == nil {
		panic(pgerror.NewAssertionErrorf("cannot add the nil value to the cache"))
	}

	if c.prev.item == nil {
		// There was no collision, so directly insert the item into the cache.
		c.cache[c.hash] = cacheEntry{item: item}
		return
	}

	// There was a collision, so re-hash the item and link it to the existing
	// item. Loop until the generated random hash value doesn't collide with any
	// existing item.
	for {
		// Using global rand is OK, since collisions of 64-bit random values should
		// virtually never happen, so there won't be contention.
		newHash := internHash(rand.Uint64())
		if newHash != 0 {
			if _, ok := c.cache[newHash]; !ok {
				c.cache[c.hash] = cacheEntry{item: c.prev.item, collision: newHash}
				c.cache[newHash] = cacheEntry{item: item}
				return
			}
		}
	}
}

// hasher is a helper struct that exposes methods for computing hash values and
// testing equality on all the various types of values. It is embedded in the
// interner. To use, first call the init method, then a series of hash methods.
// The final value is stored in the hash field.
type hasher struct {
	// bytes is a scratch byte array used to serialize certain types of values
	// during hashing and equality testing.
	bytes []byte

	// bytes2 is a scratch byte array used to serialize certain types of values
	// during equality testing.
	bytes2 []byte

	// hash stores the hash value as it is incrementally computed.
	hash internHash
}

func (h *hasher) Init() {
	h.hash = offset64
}

// ----------------------------------------------------------------------
//
// Hash functions
//   Each field in each item to be hashed must have a hash function that
//   corresponds to the type of that field. Each type mutates the hash field
//   of the interner using the FNV-1a hash algorithm.
//
// ----------------------------------------------------------------------

func (h *hasher) HashBool(val bool) {
	i := 0
	if val {
		i = 1
	}
	h.hash ^= internHash(i)
	h.hash *= prime64
}

func (h *hasher) HashInt(val int) {
	h.hash ^= internHash(val)
	h.hash *= prime64
}

func (h *hasher) HashUint64(val uint64) {
	h.hash ^= internHash(val)
	h.hash *= prime64
}

func (h *hasher) HashFloat64(val float64) {
	h.hash ^= internHash(math.Float64bits(val))
	h.hash *= prime64
}

func (h *hasher) HashRune(val rune) {
	h.hash ^= internHash(val)
	h.hash *= prime64
}

func (h *hasher) HashString(val string) {
	for _, c := range val {
		h.HashRune(c)
	}
}

func (h *hasher) HashByte(val byte) {
	h.HashRune(rune(val))
}

func (h *hasher) HashBytes(val []byte) {
	for _, c := range val {
		h.HashByte(c)
	}
}

func (h *hasher) HashOperator(val opt.Operator) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashOpaqueMetadata(val opt.OpaqueMetadata) {
	h.HashUint64(uint64(reflect.ValueOf(val).Pointer()))
}

func (h *hasher) HashType(val reflect.Type) {
	h.HashUint64(uint64(reflect.ValueOf(val).Pointer()))
}

func (h *hasher) HashDatum(val tree.Datum) {
	// Distinguish distinct values with the same representation (i.e. 1 can
	// be a Decimal or Int) using the reflect.Type of the value.
	h.HashType(reflect.TypeOf(val))

	// Special case some datum types that are simple to hash. For the others,
	// hash the key encoding or string representation.
	switch t := val.(type) {
	case *tree.DBool:
		h.HashBool(bool(*t))
	case *tree.DInt:
		h.HashUint64(uint64(*t))
	case *tree.DFloat:
		h.HashFloat64(float64(*t))
	case *tree.DString:
		h.HashString(string(*t))
	case *tree.DBytes:
		h.HashBytes([]byte(*t))
	case *tree.DDate:
		h.HashUint64(uint64(*t))
	case *tree.DTime:
		h.HashUint64(uint64(*t))
	case *tree.DJSON:
		h.HashString(t.String())
	case *tree.DTuple:
		// If labels are present, then hash of tuple's static type is needed to
		// disambiguate when everything is the same except labels.
		alwaysHashType := len(t.ResolvedType().(types.TTuple).Labels) != 0
		h.hashDatumsWithType(t.D, t.ResolvedType(), alwaysHashType)
	case *tree.DArray:
		// If the array is empty, then hash of tuple's static type is needed to
		// disambiguate.
		alwaysHashType := len(t.Array) == 0
		h.hashDatumsWithType(t.Array, t.ResolvedType(), alwaysHashType)
	default:
		h.bytes = encodeDatum(h.bytes[:0], val)
		h.HashBytes(h.bytes)
	}
}

func (h *hasher) hashDatumsWithType(datums tree.Datums, typ types.T, alwaysHashType bool) {
	for _, d := range datums {
		if d == tree.DNull {
			// At least one NULL exists, so need to compare static types (e.g. a
			// NULL::int is indistinguishable from NULL::string).
			alwaysHashType = true
		}
		h.HashDatum(d)
	}
	if alwaysHashType {
		h.HashDatumType(typ)
	}
}

func (h *hasher) HashDatumType(val types.T) {
	h.HashString(val.String())
}

func (h *hasher) HashColType(val coltypes.T) {
	buf := bytes.NewBuffer(h.bytes[:0])
	val.Format(buf, lex.EncNoFlags)
	h.bytes = buf.Bytes()
	h.HashBytes(h.bytes)
}

func (h *hasher) HashTypedExpr(val tree.TypedExpr) {
	h.HashUint64(uint64(reflect.ValueOf(val).Pointer()))
}

func (h *hasher) HashColumnID(val opt.ColumnID) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashColSet(val opt.ColSet) {
	hash := h.hash
	for c, ok := val.Next(0); ok; c, ok = val.Next(c + 1) {
		hash ^= internHash(c)
		hash *= prime64
	}
	h.hash = hash
}

func (h *hasher) HashColList(val opt.ColList) {
	hash := h.hash
	for _, id := range val {
		hash ^= internHash(id)
		hash *= prime64
	}
	h.hash = hash
}

func (h *hasher) HashOrdering(val opt.Ordering) {
	hash := h.hash
	for _, id := range val {
		hash ^= internHash(id)
		hash *= prime64
	}
	h.hash = hash
}

func (h *hasher) HashOrderingChoice(val physical.OrderingChoice) {
	h.HashColSet(val.Optional)

	for i := range val.Columns {
		choice := &val.Columns[i]
		h.HashColSet(choice.Group)
		h.HashBool(choice.Descending)
	}
}

func (h *hasher) HashSchemaID(val opt.SchemaID) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashTableID(val opt.TableID) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashSequenceID(val opt.SequenceID) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashValuesID(val opt.ValuesID) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashScanLimit(val ScanLimit) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashScanFlags(val ScanFlags) {
	h.HashBool(val.NoIndexJoin)
	h.HashBool(val.IndexHintType == keys.IndexHintForce)
	//TODO SCAN FLAG
	if len(val.HintIndexs) != 0 {
		h.HashUint64(uint64(val.HintIndexs[0]))
	}
}

func (h *hasher) HashJoinFlags(val JoinFlags) {
	h.HashBool(val.DisallowHashJoin)
	h.HashBool(val.DisallowMergeJoin)
	h.HashBool(val.DisallowLookupJoin)
}

func (h *hasher) HashExplainOptions(val tree.ExplainOptions) {
	h.HashColSet(val.Flags)
	h.HashUint64(uint64(val.Mode))
}

func (h *hasher) HashStatementType(val tree.StatementType) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashShowTraceType(val tree.ShowTraceType) {
	h.HashString(string(val))
}

func (h *hasher) HashWindowFrame(val WindowFrame) {
	h.HashInt(int(val.StartBoundType))
	h.HashInt(int(val.EndBoundType))
	h.HashInt(int(val.Mode))
}

func (h *hasher) HashTupleOrdinal(val TupleOrdinal) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashPhysProps(val *physical.Required) {
	for i := range val.Presentation {
		col := &val.Presentation[i]
		h.HashString(col.Alias)
		h.HashColumnID(col.ID)
	}
	h.HashOrderingChoice(val.Ordering)
}

func (h *hasher) HashLockingItem(val *tree.LockingItem) {
	if val != nil {
		h.HashByte(byte(val.Strength))
		h.HashByte(byte(val.WaitPolicy.LockingType))
		h.HashByte(byte(val.WaitPolicy.WaitTime))
	}
}

func (h *hasher) HashRelExpr(val RelExpr) {
	h.HashUint64(uint64(reflect.ValueOf(val).Pointer()))
}

func (h *hasher) HashScalarExpr(val opt.ScalarExpr) {
	h.HashUint64(uint64(reflect.ValueOf(val).Pointer()))
}

func (h *hasher) HashScalarListExpr(val ScalarListExpr) {
	for i := range val {
		h.HashScalarExpr(val[i])
	}
}

func (h *hasher) HashFiltersExpr(val FiltersExpr) {
	for i := range val {
		h.HashScalarExpr(val[i].Condition)
	}
}

func (h *hasher) HashProjectionsExpr(val ProjectionsExpr) {
	for i := range val {
		item := &val[i]
		h.HashColumnID(item.Col)
		h.HashScalarExpr(item.Element)
	}
}

func (h *hasher) HashAggregationsExpr(val AggregationsExpr) {
	for i := range val {
		item := &val[i]
		h.HashColumnID(item.Col)
		h.HashScalarExpr(item.Agg)
	}
}

func (h *hasher) HashWindowsExpr(val WindowsExpr) {
	for i := range val {
		item := &val[i]
		h.HashColumnID(item.Col)
		h.HashScalarExpr(item.Function)
		h.HashWindowFrame(item.Frame)
	}
}

func (h *hasher) HashZipExpr(val ZipExpr) {
	for i := range val {
		item := &val[i]
		h.HashColList(item.Cols)
		h.HashScalarExpr(item.Fn)
	}
}

func (h *hasher) HashPresentation(val physical.Presentation) {
	for i := range val {
		col := &val[i]
		h.HashString(col.Alias)
		h.HashColumnID(col.ID)
	}
}

func (h *hasher) HashPointer(val unsafe.Pointer) {
	h.HashUint64(uint64(uintptr(val)))
}

func (h *hasher) HashDataStoreEngine(val cat.DataStoreEngine) {
	h.HashUint64(uint64(val.ETypeSet))
}

func (h *hasher) HashViewDeps(val opt.ViewDeps) {
	// Hash the length and address of the first element.
	h.HashInt(len(val))
	if len(val) > 0 {
		h.HashPointer(unsafe.Pointer(&val[0]))
	}
}

func (h *hasher) HashJobCommand(val tree.JobCommand) {
	h.HashInt(int(val))
}

func (h *hasher) HashIndexOrdinal(val cat.IndexOrdinal) {
	h.HashInt(val)
}

func (h *hasher) HashWithID(val opt.WithID) {
	h.HashUint64(uint64(val))
}

func (h *hasher) HashStatement(val tree.Statement) {
	h.HashUint64(uint64(reflect.ValueOf(val).Pointer()))
}

// ----------------------------------------------------------------------
//
// Equality functions
//   Each field in each item to be hashed must have an equality function that
//   corresponds to the type of that field. An equality function returns true
//   if the two values are equivalent. If all fields in two items are
//   equivalent, then the items are considered equivalent, and only one is
//   retained in the cache.
//
// ----------------------------------------------------------------------

func (h *hasher) IsBoolEqual(l, r bool) bool {
	return l == r
}

func (h *hasher) IsIntEqual(l, r int) bool {
	return l == r
}

func (h *hasher) IsFloat64Equal(l, r float64) bool {
	return math.Float64bits(l) == math.Float64bits(r)
}

func (h *hasher) IsStringEqual(l, r string) bool {
	return bytes.Equal([]byte(l), []byte(r))
}

func (h *hasher) IsBytesEqual(l, r []byte) bool {
	return bytes.Equal(l, r)
}

func (h *hasher) IsTypeEqual(l, r reflect.Type) bool {
	return l == r
}

func (h *hasher) IsOperatorEqual(l, r opt.Operator) bool {
	return l == r
}

func (h *hasher) IsDatumTypeEqual(l, r types.T) bool {
	return l.String() == r.String()
}

func (h *hasher) IsColTypeEqual(l, r coltypes.T) bool {
	lbuf := bytes.NewBuffer(h.bytes[:0])
	l.Format(lbuf, lex.EncNoFlags)
	rbuf := bytes.NewBuffer(h.bytes2[:0])
	r.Format(rbuf, lex.EncNoFlags)
	h.bytes = lbuf.Bytes()
	h.bytes2 = rbuf.Bytes()
	return bytes.Equal(h.bytes, h.bytes2)
}

func (h *hasher) IsDatumEqual(l, r tree.Datum) bool {
	switch lt := l.(type) {
	case *tree.DBool:
		if rt, ok := r.(*tree.DBool); ok {
			return *lt == *rt
		}
	case *tree.DInt:
		if rt, ok := r.(*tree.DInt); ok {
			return *lt == *rt
		}
	case *tree.DFloat:
		if rt, ok := r.(*tree.DFloat); ok {
			return h.IsFloat64Equal(float64(*lt), float64(*rt))
		}
	case *tree.DString:
		if rt, ok := r.(*tree.DString); ok {
			return h.IsStringEqual(string(*lt), string(*rt))
		}
	case *tree.DBytes:
		if rt, ok := r.(*tree.DBytes); ok {
			return bytes.Equal([]byte(*lt), []byte(*rt))
		}
	case *tree.DDate:
		if rt, ok := r.(*tree.DDate); ok {
			return uint64(*lt) == uint64(*rt)
		}
	case *tree.DTime:
		if rt, ok := r.(*tree.DTime); ok {
			return uint64(*lt) == uint64(*rt)
		}
	case *tree.DJSON:
		if rt, ok := r.(*tree.DJSON); ok {
			return h.IsStringEqual(lt.String(), rt.String())
		}
	case *tree.DTuple:
		if rt, ok := r.(*tree.DTuple); ok {
			// Compare datums and then compare static types if nulls or labels
			// are present.
			ltyp := lt.ResolvedType().(types.TTuple)
			rtyp := rt.ResolvedType().(types.TTuple)
			if !h.areDatumsWithTypeEqual(lt.D, rt.D, ltyp, rtyp) {
				return false
			}
			return len(ltyp.Labels) == 0 || h.IsDatumTypeEqual(ltyp, rtyp)
		}
	case *tree.DArray:
		if rt, ok := r.(*tree.DArray); ok {
			// Compare datums and then compare static types if nulls are present
			// or if arrays are empty.
			ltyp := lt.ResolvedType()
			rtyp := rt.ResolvedType()
			if !h.areDatumsWithTypeEqual(lt.Array, rt.Array, ltyp, rtyp) {
				return false
			}
			return len(lt.Array) != 0 || h.IsDatumTypeEqual(ltyp, rtyp)
		}
	default:
		h.bytes = encodeDatum(h.bytes[:0], l)
		h.bytes2 = encodeDatum(h.bytes2[:0], r)
		return bytes.Equal(h.bytes, h.bytes2)
	}

	return false
}

func (h *hasher) areDatumsWithTypeEqual(ldatums, rdatums tree.Datums, ltyp, rtyp types.T) bool {
	if len(ldatums) != len(rdatums) {
		return false
	}
	foundNull := false
	for i := range ldatums {
		if !h.IsDatumEqual(ldatums[i], rdatums[i]) {
			return false
		}
		if ldatums[i] == tree.DNull {
			// At least one NULL exists, so need to compare static types (e.g. a
			// NULL::int is indistinguishable from NULL::string).
			foundNull = true
		}
	}
	if foundNull {
		return h.IsDatumTypeEqual(ltyp, rtyp)
	}
	return true
}

func (h *hasher) IsTypedExprEqual(l, r tree.TypedExpr) bool {
	return l == r
}

func (h *hasher) IsColumnIDEqual(l, r opt.ColumnID) bool {
	return l == r
}

func (h *hasher) IsColSetEqual(l, r opt.ColSet) bool {
	return l.Equals(r)
}

func (h *hasher) IsColListEqual(l, r opt.ColList) bool {
	return l.Equals(r)
}

func (h *hasher) IsOrderingEqual(l, r opt.Ordering) bool {
	return l.Equals(r)
}

func (h *hasher) IsOrderingChoiceEqual(l, r physical.OrderingChoice) bool {
	return l.Equals(&r)
}

func (h *hasher) IsSchemaIDEqual(l, r opt.SchemaID) bool {
	return l == r
}

func (h *hasher) IsTableIDEqual(l, r opt.TableID) bool {
	return l == r
}

func (h *hasher) IsSequenceIDEqual(l, r opt.SequenceID) bool {
	return l == r
}

func (h *hasher) IsValuesIDEqual(l, r opt.ValuesID) bool {
	return l == r
}

func (h *hasher) IsScanLimitEqual(l, r ScanLimit) bool {
	return l == r
}

//TODO:zyk 这里比较是不对的 需要考虑后改正
func (h *hasher) IsScanFlagsEqual(l, r ScanFlags) bool {
	ret := true
	ret = ret && l.IndexHintType == r.IndexHintType
	if len(l.IndexName) != len(r.IndexName) {
		return false
	}
	for i := range l.IndexName {
		ret = ret && l.IndexName[i] == r.IndexName[i]
	}
	if len(l.HintIndexs) != len(r.HintIndexs) {
		return false
	}
	for i := range l.HintIndexs {
		ret = ret && l.HintIndexs[i] == r.HintIndexs[i]
	}
	ret = ret && l.NoIndexJoin == r.NoIndexJoin
	ret = ret && l.Direction == r.Direction
	return ret
}

func (h *hasher) IsJoinFlagsEqual(l, r JoinFlags) bool {
	return l.DisallowHashJoin == r.DisallowHashJoin && l.DisallowLookupJoin == r.DisallowLookupJoin && l.DisallowMergeJoin == r.DisallowMergeJoin
}

func (h *hasher) IsExplainOptionsEqual(l, r tree.ExplainOptions) bool {
	return l.Mode == r.Mode && l.Flags.Equals(r.Flags)
}

func (h *hasher) IsStatementTypeEqual(l, r tree.StatementType) bool {
	return l == r
}

func (h *hasher) IsShowTraceTypeEqual(l, r tree.ShowTraceType) bool {
	return l == r
}

func (h *hasher) IsWindowFrameEqual(l, r WindowFrame) bool {
	return l.StartBoundType == r.StartBoundType &&
		l.EndBoundType == r.EndBoundType &&
		l.Mode == r.Mode
}

func (h *hasher) IsTupleOrdinalEqual(l, r TupleOrdinal) bool {
	return l == r
}

func (h *hasher) IsPhysPropsEqual(l, r *physical.Required) bool {
	return l.Equals(r)
}

func (h *hasher) IsLockingItemEqual(l, r *tree.LockingItem) bool {
	if l == nil || r == nil {
		return l == r
	}
	return l.Strength == r.Strength && l.WaitPolicy == r.WaitPolicy
}

func (h *hasher) IsPointerEqual(l, r unsafe.Pointer) bool {
	return l == r
}

func (h *hasher) IsRelExprEqual(l, r RelExpr) bool {
	return l == r
}

func (h *hasher) IsScalarExprEqual(l, r opt.ScalarExpr) bool {
	return l == r
}

func (h *hasher) IsScalarListExprEqual(l, r ScalarListExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i] != r[i] {
			return false
		}
	}
	return true
}

func (h *hasher) IsFiltersExprEqual(l, r FiltersExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i].Condition != r[i].Condition {
			return false
		}
	}
	return true
}

func (h *hasher) IsProjectionsExprEqual(l, r ProjectionsExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i].Col != r[i].Col || l[i].Element != r[i].Element {
			return false
		}
	}
	return true
}

func (h *hasher) IsAggregationsExprEqual(l, r AggregationsExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i].Col != r[i].Col || l[i].Agg != r[i].Agg {
			return false
		}
	}
	return true
}

func (h *hasher) IsWindowsExprEqual(l, r WindowsExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i].Col != r[i].Col ||
			l[i].Function != r[i].Function ||
			l[i].Frame != r[i].Frame {
			return false
		}
	}
	return true
}

func (h *hasher) IsZipExprEqual(l, r ZipExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if !l[i].Cols.Equals(r[i].Cols) || l[i].Fn != r[i].Fn {
			return false
		}
	}
	return true
}

func (h *hasher) IsPresentationEqual(l, r physical.Presentation) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i].ID != r[i].ID || l[i].Alias != r[i].Alias {
			return false
		}
	}
	return true
}

func (h *hasher) IsDataStoreEngineEqual(l, r cat.DataStoreEngine) bool {
	return l.ETypeSet == r.ETypeSet
}

// encodeDatum turns the given datum into an encoded string of bytes. If two
// datums are equivalent, then their encoded bytes will be identical.
// Conversely, if two datums are not equivalent, then their encoded bytes will
// differ.
func encodeDatum(b []byte, val tree.Datum) []byte {
	// Fast path: encode the datum using table key encoding. This does not always
	// work, because the encoding does not uniquely represent some values which
	// should not be considered equivalent by the interner (e.g. decimal values
	// 1.0 and 1.00).
	if !sqlbase.DatumTypeHasCompositeKeyEncoding(val.ResolvedType()) {
		var err error
		b, err = sqlbase.EncodeTableKey(b, val, encoding.Ascending)
		if err == nil {
			return b
		}
	}

	// Fall back on a string representation which can be used to check for
	// equivalence.
	ctx := tree.NewFmtCtx(tree.FmtCheckEquivalence)
	val.Format(ctx)
	return ctx.Bytes()
}

func (h *hasher) IsViewDepsEqual(l, r opt.ViewDeps) bool {
	if len(l) != len(r) {
		return false
	}
	return len(l) == 0 || &l[0] == &r[0]
}

func (h *hasher) IsOpaqueMetadataEqual(l, r opt.OpaqueMetadata) bool {
	return l == r
}

func (h *hasher) IsJobCommandEqual(l, r tree.JobCommand) bool {
	return l == r
}

func (h *hasher) IsIndexOrdinalEqual(l, r cat.IndexOrdinal) bool {
	return l == r
}

func (h *hasher) IsWithIDEqual(l, r opt.WithID) bool {
	return l == r
}

func (h *hasher) IsStatementEqual(l, r tree.Statement) bool {
	return l == r
}
