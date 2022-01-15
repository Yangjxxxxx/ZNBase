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

package sql

import (
	"context"
	"math"
	"strconv"

	"github.com/pkg/errors"
	"github.com/znbasedb/errors/errutil"
	"github.com/znbasedb/errors/markers"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// IncrementSequence implements the tree.SequenceOperators interface.
func (p *planner) IncrementSequence(ctx context.Context, seqName *tree.TableName) (int64, error) {
	if p.EvalContext().TxnReadOnly {
		return 0, readOnlyError("nextval()")
	}

	descriptor, err := ResolveExistingObject(ctx, p, seqName, true /*required*/, requireSequenceDesc)
	if err != nil {
		return 0, err
	}
	if err := p.CheckPrivilege(ctx, descriptor, privilege.USAGE); err != nil {
		if err = p.CheckPrivilege(ctx, descriptor, privilege.UPDATE); err != nil {
			return 0, err
		}
	}
	descriptor.SequenceOpts.IsCall = true
	seqOpts := descriptor.SequenceOpts
	seqID := uint32(descriptor.TableDescriptor.ID)
	// cacheValsCount is used to determine whether this session calls the SEQ for the first time.if 0 it's the first time.
	cacheValsCount := p.sessionDataMutator.data.SequenceState.ReturnCacheValsCount(seqID)
	var val int64
	if seqOpts.Virtual {
		rowid := builtins.GenerateUniqueInt(p.EvalContext().NodeID)
		val = int64(rowid)
	} else {
		if seqOpts.Cache <= 1 {
			//nocache or cache = 1, it's the same as normal seq.
			seqValueKey := keys.MakeSequenceKey(uint32(descriptor.ID))
			val, err = client.IncrementValRetryable(
				ctx, p.txn.DB(), seqValueKey, seqOpts.Increment)

			//res, err := p.txn.Inc(ctx, seqValueKey, descriptor.SequenceOpts.Increment)
			if err != nil {
				if !seqOpts.Cycle {
					switch err.(type) {
					case *roachpb.IntegerOverflowError:
						return 0, boundsExceededError(descriptor)
					default:
						return 0, err
					}
				} else {
					val, err = cycleSequneceBoundsNoCache(ctx, val, p, seqValueKey, descriptor)
					if err != nil {
						return 0, err
					}
				}
			}
			//val = res.ValueInt()
			if val > seqOpts.MaxValue || val < seqOpts.MinValue {
				isAscending := seqOpts.Increment > 0
				if isAscending && seqOpts.Cycle {
					diff := val - seqOpts.MaxValue
					val, err = client.IncrementValRetryable(
						ctx, p.txn.DB(), seqValueKey, -(seqOpts.MaxValue - seqOpts.MinValue + diff))
					//p.txn.Inc(ctx, seqValueKey, -(seqOpts.MaxValue - seqOpts.MinValue + diff))
					if err != nil {
						return 0, err
					}
					//val = temp.ValueInt()
				} else if !isAscending && seqOpts.Cycle {
					diff := val - seqOpts.MinValue
					val, err = client.IncrementValRetryable(
						ctx, p.txn.DB(), seqValueKey, -(seqOpts.MinValue - seqOpts.MaxValue + diff))
					//p.txn.Inc(ctx, seqValueKey, -(seqOpts.MinValue - seqOpts.MaxValue + diff))
					if err != nil {
						return 0, err
					}
					//val = temp.ValueInt()
				} else {
					count, _ := seqBoundsControl(val, seqOpts.MaxValue, seqOpts.MinValue, seqOpts.Increment)
					if count != 0 {
						descriptor.SequenceOpts.EverOver = true
						_, err = client.IncrementValRetryable(
							ctx, p.txn.DB(), seqValueKey, -seqOpts.Increment*(count))
					}
					return 0, boundsExceededError(descriptor)
				}
			}
		} else {
			//cache > 1
			seqValueKey := keys.MakeSequenceKey(uint32(descriptor.ID))
			// tempVal it's the seq's actual value.
			tempVal, errG := client.IncrementValRetryable(
				ctx, p.txn.DB(), seqValueKey, 0)
			if errG != nil {
				return 0, errG
			}
			var cacheInc int64
			tmp := tempVal
			overInt := false
			cacheLen := len(strconv.FormatInt(seqOpts.Cache, 10))
			incLen := len(strconv.FormatInt(seqOpts.Increment, 10))
			cacheInc = seqOpts.Increment * seqOpts.Cache
			if cacheInc/seqOpts.Increment == seqOpts.Cache {
				tmp += seqOpts.Increment * seqOpts.Cache
				if (seqOpts.Increment > 0 && tmp < 0 && tempVal > 0) || (seqOpts.Increment < 0 && tmp > 0 && tempVal < 0) {
					overInt = true
				}
			} else {
				if cacheLen+incLen > 22 {
					overInt = true
				} else {
					var tmpLen int64
					var tempVal int64
					if cacheLen >= incLen {
						tmpLen = seqOpts.Increment
						tempVal = seqOpts.Cache
					} else {
						tmpLen = seqOpts.Cache
						tempVal = seqOpts.Increment
					}
					for i := tmpLen; i > 0; i-- {
						tmp += tempVal
						if (seqOpts.Increment > 0 && tmp <= 0 && tempVal > 0) || (seqOpts.Increment < 0 && tmp >= 0 && tempVal < 0) {
							overInt = true
							break
						}
					}
				}
			}
			if !overInt {
				cacheInc = seqOpts.Increment * seqOpts.Cache
			}
			// This session calls the nextval method of the SEQ for the first time.
			if cacheValsCount == -1 || cacheValsCount == 0 {
				// The seq has never called nextval after created.
				// 1. call inc to change the actual value of seq to startVal,the increment is seqOpts.increment.
				// 2. Take startVal as the return value.
				// 3. reset the cache val.
				// 4. call inc to change the actual value of seq to the maxvalue of the cache, the increment is seqOpts.increment*seqOpts.cache
				// 5. limit the SEQ actual val.
				p.sessionDataMutator.data.SequenceState.ResetCacheVals(seqID, tempVal, seqOpts.Cache)
				val = p.sessionDataMutator.data.SequenceState.ReturnUpdateCacheVals(seqID, seqOpts.Cache, seqOpts.Increment)
				if !overInt {
					_, err = client.IncrementValRetryable(
						ctx, p.txn.DB(), seqValueKey, cacheInc)
					if err != nil {
						return 0, err
					}
				} else {
					if !seqOpts.Cycle {
						if seqOpts.Increment >= 0 {
							_, err = client.IncrementValRetryable(
								ctx, p.txn.DB(), seqValueKey, -tempVal)
							_, err = client.IncrementValRetryable(
								ctx, p.txn.DB(), seqValueKey, math.MaxInt64)
						} else {
							_, err = client.IncrementValRetryable(
								ctx, p.txn.DB(), seqValueKey, -tempVal)
							_, err = client.IncrementValRetryable(
								ctx, p.txn.DB(), seqValueKey, math.MinInt64)
						}
					} else {
						_, err = cycleSequneceBounds(ctx, val, p, seqValueKey, cacheInc, descriptor, seqID, overInt, true)
						if err != nil {
							return 0, err
						}
					}
				}
				currval, _ := client.IncrementValRetryable(
					ctx, p.txn.DB(), seqValueKey, 0)
				count, _ := seqBoundsControl(currval, seqOpts.MaxValue, seqOpts.MinValue, seqOpts.Increment)
				if count != 0 {
					descriptor.SequenceOpts.EverOver = true
					_, err = client.IncrementValRetryable(
						ctx, p.txn.DB(), seqValueKey, -seqOpts.Increment*(count))
				}
				if err != nil {
					if !seqOpts.Cycle {
						switch err.(type) {
						case *roachpb.IntegerOverflowError:
							return 0, boundsExceededError(descriptor)
						default:
							return 0, err
						}
					} else {
						val, err = cycleSequneceBounds(ctx, val, p, seqValueKey, cacheInc, descriptor, seqID, overInt, true)
						if err != nil {
							return 0, err
						}
					}
				}
			} else {
				// cache val count > 1, directly return the cache val.
				val = p.sessionDataMutator.data.SequenceState.ReturnUpdateCacheVals(seqID, seqOpts.Cache, seqOpts.Increment)
			}
			if val > seqOpts.MaxValue || val < seqOpts.MinValue {
				val, err = cycleSequneceBounds(ctx, val, p, seqValueKey, cacheInc, descriptor, seqID, overInt, false)
				if err != nil {
					return 0, err
				}
				val = p.sessionDataMutator.data.SequenceState.ReturnUpdateCacheVals(seqID, seqOpts.Cache, seqOpts.Increment)
			}
			if val > seqOpts.MaxValue || val < seqOpts.MinValue {
				val, err = cycleSequneceBounds(ctx, val, p, seqValueKey, cacheInc, descriptor, seqID, overInt, false)
				if err != nil {
					return 0, err
				}
				val = p.sessionDataMutator.data.SequenceState.ReturnUpdateCacheVals(seqID, seqOpts.Cache, seqOpts.Increment)
			}
			if tempCount := p.sessionDataMutator.data.SequenceState.ReturnCacheValsCount(seqID); tempCount == 0 {
				if tempVal != val {
					_, err = client.IncrementValRetryable(
						ctx, p.txn.DB(), seqValueKey, val-tempVal)
				}
			}
			if val > seqOpts.MaxValue || val < seqOpts.MinValue {
				val, err = cycleSequneceBounds(ctx, val, p, seqValueKey, cacheInc, descriptor, seqID, overInt, false)
				val = p.sessionDataMutator.data.SequenceState.ReturnUpdateCacheVals(seqID, seqOpts.Cache, seqOpts.Increment)
				if err != nil {
					return 0, err
				}
			}
			if val > seqOpts.MaxValue || val < seqOpts.MinValue {
				val, _ = cycleSequneceBounds(ctx, val, p, seqValueKey, cacheInc, descriptor, seqID, overInt, false)
				val = p.sessionDataMutator.data.SequenceState.ReturnUpdateCacheVals(seqID, seqOpts.Cache, seqOpts.Increment)
			}
		}
	}
	p.ExtendedEvalContext().SessionMutator.RecordLatestSequenceVal(uint32(descriptor.ID), val)
	return val, nil
}

func boundsExceededError(descriptor *sqlbase.ImmutableTableDescriptor) error {
	seqOpts := descriptor.SequenceOpts
	isAscending := seqOpts.Increment > 0

	var word string
	var value int64
	if isAscending {
		word = "maximum"
		value = seqOpts.MaxValue
	} else {
		word = "minimum"
		value = seqOpts.MinValue
	}
	return pgerror.NewErrorf(
		pgcode.SequenceGeneratorLimitExceeded,
		`reached %s value of sequence %q (%d)`, word,
		tree.ErrString((*tree.Name)(&descriptor.Name)), value)
}

func seqBoundsControl(
	currval int64, maxValue int64, minValue int64, increment int64,
) (int64, error) {
	var count int64
	for {
		if (currval > maxValue && increment > 0) || (currval < minValue && increment < 0) {
			currval -= increment
			count++
		} else {
			break
		}
	}
	return count, nil
}

func cycleSequneceBounds(
	ctx context.Context,
	val int64,
	p *planner,
	seqValueKey roachpb.Key,
	cacheInc int64,
	res *ImmutableTableDescriptor,
	seqID uint32,
	overInt bool,
	isrestart bool,
) (int64, error) {
	realVal, _ := client.IncrementValRetryable(
		ctx, p.txn.DB(), seqValueKey, 0)
	seqOpts := res.SequenceOpts
	isAscending := seqOpts.Increment > 0
	if isAscending && seqOpts.Cycle {
		_, err := client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, -realVal)
		if err != nil {
			return 0, err
		}
		val, err = client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, seqOpts.MinValue-seqOpts.Increment)
		if !isrestart {
			p.sessionDataMutator.data.SequenceState.ResetCacheVals(seqID, val, seqOpts.Cache)
		}
		if err != nil {
			return 0, err
		}
		if !overInt {
			_, err = client.IncrementValRetryable(
				ctx, p.txn.DB(), seqValueKey, cacheInc)
			currval, _ := client.IncrementValRetryable(
				ctx, p.txn.DB(), seqValueKey, 0)
			count, _ := seqBoundsControl(currval, seqOpts.MaxValue, seqOpts.MinValue, seqOpts.Increment)
			if count != 0 {
				res.SequenceOpts.EverOver = true
				_, err = client.IncrementValRetryable(
					ctx, p.txn.DB(), seqValueKey, -seqOpts.Increment*(count))
			}
		}
	} else if !isAscending && seqOpts.Cycle {
		_, err := client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, -realVal-1)
		if err != nil {
			return 0, err
		}
		_, err = client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, 1)
		if err != nil {
			return 0, err
		}
		val, err = client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, seqOpts.MaxValue-seqOpts.Increment)
		if !isrestart {
			p.sessionDataMutator.data.SequenceState.ResetCacheVals(seqID, val, seqOpts.Cache)
		}
		if err != nil {
			return 0, err
		}
		if !overInt {
			_, err = client.IncrementValRetryable(
				ctx, p.txn.DB(), seqValueKey, cacheInc)
			currval, _ := client.IncrementValRetryable(
				ctx, p.txn.DB(), seqValueKey, 0)
			count, _ := seqBoundsControl(currval, seqOpts.MaxValue, seqOpts.MinValue, seqOpts.Increment)
			if count != 0 {
				res.SequenceOpts.EverOver = true
				_, err = client.IncrementValRetryable(
					ctx, p.txn.DB(), seqValueKey, -seqOpts.Increment*(count))
			}
		}
	} else {
		return 0, boundsExceededError(res)
	}
	return val, nil
}

func cycleSequneceBoundsNoCache(
	ctx context.Context,
	val int64,
	p *planner,
	seqValueKey roachpb.Key,
	res *ImmutableTableDescriptor,
) (int64, error) {
	realVal, _ := client.IncrementValRetryable(
		ctx, p.txn.DB(), seqValueKey, 0)
	seqOpts := res.SequenceOpts
	isAscending := seqOpts.Increment > 0
	if isAscending && seqOpts.Cycle {
		_, err := client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, -realVal)
		if err != nil {
			return 0, err
		}
		val, err = client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, seqOpts.MinValue)
		if err != nil {
			return 0, err
		}
	} else if !isAscending && seqOpts.Cycle {
		_, err := client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, -realVal-1)
		if err != nil {
			return 0, err
		}
		_, err = client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, 1)
		if err != nil {
			return 0, err
		}
		val, err = client.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, seqOpts.MaxValue)
		if err != nil {
			return 0, err
		}
	} else {
		return 0, boundsExceededError(res)
	}
	return val, nil
}

// GetLatestValueInSessionForSequence implements the tree.SequenceOperators interface.
func (p *planner) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	descriptor, err := ResolveExistingObject(ctx, p, seqName, true /*required*/, requireSequenceDesc)
	if err != nil {
		return 0, err
	}

	if err := p.CheckPrivilege(ctx, descriptor, privilege.USAGE); err != nil {
		if err = p.CheckPrivilege(ctx, descriptor, privilege.SELECT); err != nil {
			return 0, err
		}
	}
	val, ok := p.SessionData().SequenceState.GetLastValueByID(uint32(descriptor.ID))
	if !ok {
		return 0, pgerror.NewErrorf(
			pgcode.ObjectNotInPrerequisiteState,
			`currval of sequence %q is not yet defined in this session`, tree.ErrString(seqName))
	}

	return val, nil
}

// SetSequenceValue implements the tree.SequenceOperators interface.
func (p *planner) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("setval()")
	}

	descriptor, err := ResolveExistingObject(ctx, p, seqName, true /*required*/, requireSequenceDesc)
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, descriptor, privilege.UPDATE); err != nil {
		return err
	}

	if descriptor.SequenceOpts.Virtual {
		// TODO(knz): we currently return an error here, but if/when
		// ZNBaseDB grows to automatically make sequences virtual when
		// clients don't expect it, we may need to make this a no-op
		// instead.
		return pgerror.NewErrorf(
			pgcode.ObjectNotInPrerequisiteState,
			`cannot set the value of virtual sequence %q`, tree.ErrString(seqName))
	}

	seqValueKey, newVal, err := MakeSequenceKeyVal(descriptor.TableDesc(), newVal, isCalled)
	if err != nil {
		return err
	}
	p.sessionDataMutator.data.SequenceState.ResetCacheVals(uint32(descriptor.ID), newVal, descriptor.SequenceOpts.Cache)
	p.ExtendedEvalContext().SessionMutator.RecordLatestSequenceVal(uint32(descriptor.ID), newVal)
	// TODO(vilterp): not supposed to mix usage of Inc and Put on a key,
	// according to comments on Inc operation. Switch to Inc if `desired-current`
	// overflows correctly.
	return p.txn.Put(ctx, seqValueKey, newVal)
}

// MakeSequenceKeyVal returns the key and value of a sequence being set
// with newVal.
func MakeSequenceKeyVal(
	sequence *TableDescriptor, newVal int64, isCalled bool,
) ([]byte, int64, error) {
	opts := sequence.SequenceOpts
	if newVal > opts.MaxValue || newVal < opts.MinValue {
		return nil, 0, pgerror.NewErrorf(
			pgcode.NumericValueOutOfRange,
			`value %d is out of bounds for sequence "%s" (%d..%d)`,
			newVal, sequence.Name, opts.MinValue, opts.MaxValue,
		)
	}
	if !isCalled {
		newVal = newVal - sequence.SequenceOpts.Increment
	}

	seqValueKey := keys.MakeSequenceKey(uint32(sequence.ID))
	return seqValueKey, newVal, nil
}

// GetSequenceValue returns the current value of the sequence.
func (p *planner) GetSequenceValue(
	ctx context.Context, desc *sqlbase.ImmutableTableDescriptor,
) (int64, error) {
	if desc.SequenceOpts == nil {
		return 0, errors.New("descriptor is not a sequence")
	}
	keyValue, err := p.txn.Get(ctx, keys.MakeSequenceKey(uint32(desc.ID)))
	if err != nil {
		return 0, err
	}
	return keyValue.ValueInt(), nil
}

func readOnlyError(s string) error {
	return pgerror.NewErrorf(pgcode.ReadOnlySQLTransaction,
		"cannot execute %s in a read-only transaction", s)
}

// assignSequenceOptions moves options from the AST node to the sequence options descriptor,
// starting with defaults and overriding them with user-provided options.
func assignSequenceOptions(
	opts *sqlbase.TableDescriptor_SequenceOpts,
	optsNode tree.SequenceOptions,
	setDefaults bool,
	isAlter bool,
	params *runParams,
	sequenceID sqlbase.ID,
	sequenceParentID sqlbase.ID,
) error {
	// All other defaults are dependent on the value of increment,
	// i.e. whether the sequence is ascending or descending.
	opts.EverOver = false
	//opts.IsRestart = false
	for _, option := range optsNode {
		if option.Name == tree.SeqOptIncrement {
			opts.Increment = *option.IntVal
		}
	}
	if opts.Increment == 0 {
		return pgerror.NewError(
			pgcode.InvalidParameterValue, "INCREMENT must not be zero")
	}
	isAscending := opts.Increment > 0

	// Set increment-dependent defaults.
	if setDefaults {
		if isAscending {
			opts.MinValue = 1
			opts.MaxValue = math.MaxInt64
			opts.Start = opts.MinValue
		} else {
			opts.MinValue = math.MinInt64
			opts.MaxValue = -1
			opts.Start = opts.MaxValue
		}
	}

	// Fill in all other options.
	optionsSeen := map[string]bool{}
	for _, option := range optsNode {
		// Error on duplicate options.
		_, seenBefore := optionsSeen[option.Name]
		if seenBefore {
			return pgerror.NewError(pgcode.Syntax, "conflicting or redundant options")
		}
		optionsSeen[option.Name] = true

		switch option.Name {
		case tree.SeqOptCycle:
			//return pgerror.UnimplementedWithIssueError(20961,
			//	"CYCLE option is not supported")
			if option.Cycle {
				opts.Cycle = true
			}
		case tree.SeqOptNoCycle:
			// Do nothing; this is the default.
			if option.Cycle == false {
				opts.Cycle = false
			}
		case tree.SeqOptCache:
			v := *option.IntVal
			switch {
			case v < 1:
				return pgerror.NewErrorf(pgcode.InvalidParameterValue,
					"CACHE (%d) must be greater than zero", v)
			case v >= 1:
				opts.Cache = *option.IntVal
				// if cache > 5000000, it will run slower
				//if v > 5000000 {
				//	return pgerror.NewErrorf(pgcode.InvalidParameterValue,
				//		"CACHE (%d) must be less than 5000000", v)
				//}
			}
		case tree.SeqOptIncrement:
			// Do nothing; this has already been set.
		case tree.SeqOptMinValue:
			// A value of nil represents the user explicitly saying `NO MINVALUE`.
			if option.IntVal != nil {
				opts.MinValue = *option.IntVal
			}
			if option.IntVal == nil && isAlter && opts.Increment < 0 {
				opts.MinValue = math.MinInt64
			}
			if option.IntVal == nil && isAlter && opts.Increment > 0 {
				opts.MinValue = 1
			}
		case tree.SeqOptMaxValue:
			// A value of nil represents the user explicitly saying `NO MAXVALUE`.
			if option.IntVal != nil {
				opts.MaxValue = *option.IntVal
			} else {
				if opts.Increment > 0 {
					opts.MaxValue = math.MaxInt64
				}
				if opts.Increment < 0 {
					opts.MaxValue = -1
				}
			}
		case tree.SeqOptStart:
			opts.Start = *option.IntVal
		case tree.SeqOptVirtual:
			opts.Virtual = true
		case tree.SeqOptOwnedBy:
			if params == nil {
				return pgerror.Newf(pgcode.Internal,
					"Trying to add/remove Sequence Owner without access to context")
			}
			// The owner is being removed
			if option.ColumnItemVal == nil {
				if err := removeSequenceOwnerIfExists(params.ctx, params.p, sequenceID, opts); err != nil {
					return err
				}
			} else {
				// The owner is being added/modified
				tableDesc, col, err := resolveColumnItemToDescriptors(
					params.ctx, params.p, option.ColumnItemVal,
				)
				if err != nil {
					return err
				}
				if tableDesc.ParentID != sequenceParentID &&
					!allowCrossDatabaseSeqOwner.Get(&params.p.execCfg.Settings.SV) {
					return pgerror.Newf(pgcode.FeatureNotSupported,
						"OWNED BY cannot refer to other databases; (see the '%s' cluster setting)",
						allowCrossDatabaseSeqOwnerSetting)

				}
				// We only want to trigger schema changes if the owner is not what we
				// want it to be.
				if opts.SequenceOwner.OwnerTableID != tableDesc.ID ||
					opts.SequenceOwner.OwnerColumnID != col.ID {
					if err := removeSequenceOwnerIfExists(params.ctx, params.p, sequenceID, opts); err != nil {
						return err
					}
					err := addSequenceOwner(params.ctx, params.p, option.ColumnItemVal, sequenceID, opts)
					if err != nil {
						return err
					}
				}
			}
			//case tree.SeqOptOwnedBy:
			//	if params == nil {
			//		return pgerror.Newf(pgcode.Internal,
			//			"Trying to add/remove Sequence Owner without access to context")
			//	}
			//	// The owner is being removed
			//	if option.ColumnItemVal == nil {
			//		if err := removeSequenceOwnerIfExists(params.ctx, params.p, sequenceID, opts); err != nil {
			//			return err
			//		}
			//	} else {
			//		// The owner is being added/modified
			//		tableDesc, col, err := resolveColumnItemToDescriptors(
			//			params.ctx, params.p, option.ColumnItemVal,
			//		)
			//		if err != nil {
			//			return err
			//		}
			//		// We only want to trigger schema changes if the owner is not what we
			//		// want it to be.
			//		if opts.SequenceOwner.OwnerTableID != tableDesc.ID ||
			//			opts.SequenceOwner.OwnerColumnID != col.ID {
			//			//if err := removeSequenceOwnerIfExists(params.ctx, params.p, sequenceID, opts); err != nil {
			//			//	return err
			//			//}
			//			err := addSequenceOwner(params.ctx, params.p, tableDesc, col, sequenceID, opts)
			//			if err != nil {
			//				return err
			//			}
			//		}
			//	}
		}
	}

	// If start option not specified, set it to MinValue (for ascending sequences)
	// or MaxValue (for descending sequences).
	if _, startSeen := optionsSeen[tree.SeqOptStart]; !startSeen && setDefaults {
		if opts.Increment > 0 {
			opts.Start = opts.MinValue
		} else {
			opts.Start = opts.MaxValue
		}
	}

	if opts.MinValue >= opts.MaxValue {
		return pgerror.NewErrorf(
			pgcode.InvalidParameterValue,
			"MINVALUE (%d) must be less than MAXVALUE (%d)", opts.MinValue, opts.MaxValue)
	}

	if opts.Start > opts.MaxValue {
		return pgerror.NewErrorf(
			pgcode.InvalidParameterValue,
			"START value (%d) cannot be greater than MAXVALUE (%d)", opts.Start, opts.MaxValue)
	}
	if opts.Start < opts.MinValue {
		return pgerror.NewErrorf(
			pgcode.InvalidParameterValue,
			"START value (%d) cannot be less than MINVALUE (%d)", opts.Start, opts.MinValue)
	}

	return nil
}

// maybeAddSequenceDependencies adds references between the column and sequence descriptors,
// if the column has a DEFAULT expression that uses one or more sequences. (Usually just one,
// e.g. `DEFAULT nextval('my_sequence')`.
// The passed-in column descriptor is mutated, and the modified sequence descriptors are returned.
func maybeAddSequenceDependencies(
	ctx context.Context,
	sc SchemaResolver,
	tableDesc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	expr tree.TypedExpr,
) ([]*MutableTableDescriptor, error) {
	seqNames, err := getUsedSequenceNames(expr)
	if err != nil {
		return nil, err
	}
	var seqDescs []*MutableTableDescriptor
	for _, seqName := range seqNames {
		parsedSeqName, err := parser.ParseTableName(seqName, 1)
		if err != nil {
			return nil, err
		}

		var seqDesc *MutableTableDescriptor
		p, ok := sc.(*planner)
		if ok {
			seqDesc, err = p.ResolveMutableTableDescriptor(ctx, parsedSeqName, true /*required*/, requireSequenceDesc)
			if err != nil {
				return nil, err
			}
			if err := p.CheckPrivilege(ctx, seqDesc, privilege.USAGE); err != nil {
				return nil, err
			}
		} else {
			// This is only executed via IMPORT which uses its own resolver.
			seqDescTemp, err := ResolveMutableExistingObject(ctx, sc, parsedSeqName, true /*required*/, requireSequenceDesc)
			if err != nil {
				return nil, err
			}
			seqDesc, ok = seqDescTemp.(*MutableTableDescriptor)
			if !ok {
				return nil, sqlbase.NewUndefinedRelationError(parsedSeqName)
			}
		}
		col.UsesSequenceIds = append(col.UsesSequenceIds, seqDesc.ID)
		// Add reference from sequence descriptor to column.
		seqDesc.DependedOnBy = append(seqDesc.DependedOnBy, sqlbase.TableDescriptor_Reference{
			ID:        tableDesc.ID,
			ColumnIDs: []sqlbase.ColumnID{col.ID},
		})
		seqDescs = append(seqDescs, seqDesc)
	}
	return seqDescs, nil
}

// removeSequenceDependencies:
//   - removes the reference from the column descriptor to the sequence descriptor.
//   - removes the reference from the sequence descriptor to the column descriptor.
//   - writes the sequence descriptor and notifies a schema change.
// The column descriptor is mutated but not saved to persistent storage; the caller must save it.
func removeSequenceDependencies(
	tableDesc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	params runParams,
	isAlter bool,
) error {

	var inheritTables = make(map[sqlbase.ID]struct{})
	// Eliminate the influence of ROWID.
	cols := tableDesc.Columns
	/*	for i, tempcol := range cols {
		if tempcol.Name == "rowid" {
			cols = append(cols[:i], cols[i+1:]...)
		}
	}*/
	for _, id := range tableDesc.Inherits {
		inheritTables[id] = struct{}{}
		err := addInheritTableID(params, id, inheritTables)
		if err != nil {
			return err
		}
	}

	for _, sequenceID := range col.UsesSequenceIds {
		// Get the sequence descriptor so we can remove the reference from it.
		seqDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, sequenceID, params.p.txn)
		if err != nil {
			return err
		}
		if isAlter {
			if err := params.p.CheckPrivilege(params.ctx, seqDesc, privilege.USAGE); err != nil {
				return err
			}
		}
		// Find an item in seqDesc.DependedOnBy which references tableDesc.
		refIdx := -1
		for i, reference := range seqDesc.DependedOnBy {
			if reference.ID == tableDesc.ID {
				refIdx = i
			}
			if _, ok := inheritTables[reference.ID]; ok {
				return nil
			}
		}
		if refIdx == -1 {
			return pgerror.NewAssertionErrorf("couldn't find reference from sequence to this column")
		}
		// 	// If different columns in a table depend on the same seq,
		//	//the first column that depends on the seq will not be removed until the last column is removed.
		for i := int(col.ID); i < len(cols); i++ {
			for j := 0; j < len(cols[i].UsesSequenceIds); j++ {
				if cols[i].UsesSequenceIds[j] == seqDesc.ID {
					return nil
				}
			}
		}
		seqDesc.DependedOnBy = append(seqDesc.DependedOnBy[:refIdx], seqDesc.DependedOnBy[refIdx+1:]...)
		if err := params.p.writeSchemaChange(params.ctx, seqDesc, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}
	// Remove the reference from the column descriptor to the sequence descriptor.
	col.UsesSequenceIds = []sqlbase.ID{}
	return nil
}

func addInheritTableID(params runParams, id sqlbase.ID, tables map[sqlbase.ID]struct{}) error {
	tableDesc, err := params.p.Tables().getMutableTableVersionByID(params.ctx, id, params.p.txn)
	if err != nil {
		return err
	}
	for _, inhID := range tableDesc.Inherits {
		if _, ok := tables[inhID]; ok {
			continue
		}
		tables[inhID] = struct{}{}
		err := addInheritTableID(params, inhID, tables)
		if err != nil {
			return err
		}
	}
	return nil
}

// getUsedSequenceNames returns the name of the sequence passed to
// a call to nextval in the given expression, or nil if there is
// no call to nextval.
// e.g. nextval('foo') => "foo"; <some other expression> => nil
func getUsedSequenceNames(defaultExpr tree.TypedExpr) ([]string, error) {
	searchPath := sessiondata.SearchPath{}
	var names []string
	_, err := tree.SimpleVisit(
		defaultExpr,
		func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
			switch t := expr.(type) {
			case *tree.FuncExpr:
				def, err := t.Func.Resolve(searchPath, false)
				if err != nil {
					return err, false, expr
				}
				if def.Name == "nextval" || def.Name == "nextval16" || def.Name == "nextval32" || def.Name == "currval" {
					arg := t.Exprs[0]
					switch a := arg.(type) {
					case *tree.DString:
						names = append(names, string(*a))
					}
				}
			}
			return nil, true, expr
		},
	)
	if err != nil {
		return nil, err
	}
	return names, nil
}

func removeSequenceOwnerIfExists(
	ctx context.Context,
	p *planner,
	sequenceID sqlbase.ID,
	opts *sqlbase.TableDescriptor_SequenceOpts,
) error {
	if !opts.HasOwner() {
		return nil
	}
	sqDesc, err := p.Tables().getMutableTableVersionByID(ctx, opts.SequenceOwner.OwnerTableID, p.txn)
	if err != nil {
		// Special case error swallowing for #50711 and #50781, which can cause a
		// column to own sequences that have been dropped/do not exist.

		//var ErrDescriptorDropped = errors.New("descriptor is being dropped")
		if markers.Is(err, ErrDescriptorDropped) ||
			pgerror.GetPGCode(err) == pgcode.UndefinedTable {
			log.Eventf(ctx, "swallowing error during sequence ownership unlinking: %s", err.Error())
			return nil
		}
		return err
	}
	// If the table descriptor has already been dropped, there is no need to
	// remove the reference.
	if sqDesc.Dropped() {
		return nil
	}
	tableDesc, err := p.Tables().getMutableTableVersionByID(ctx, opts.SequenceOwner.OwnerTableID, p.txn)
	if err != nil {
		return err
	}
	col, err := tableDesc.FindActiveColumnByID(opts.SequenceOwner.OwnerColumnID)
	if err != nil {
		return err
	}
	// Find an item in colDesc.OwnsSequenceIds which references SequenceID.
	refIdx := -1

	for i := 0; i < len(col.OwnsSequenceIds); i++ {
		id := col.OwnsSequenceIds[i]
		if id == sequenceID {
			refIdx = i
		}
	}
	if refIdx == -1 {
		//return errors.AssertionFailedf("couldn't find reference from column to this sequence")
		return errutil.AssertionFailedWithDepthf(1, "couldn't find reference from column to this sequence")
	}
	col.OwnsSequenceIds = append(col.OwnsSequenceIds[:refIdx], col.OwnsSequenceIds[refIdx+1:]...)
	if err := p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID,
	); err != nil {
		return err
	}
	// Reset the SequenceOwner to empty
	opts.SequenceOwner.Reset()
	return nil
}

func resolveColumnItemToDescriptors(
	ctx context.Context, p *planner, columnItem *tree.ColumnItem,
) (*sqlbase.MutableTableDescriptor, *sqlbase.ColumnDescriptor, error) {
	if columnItem.TableName == nil {
		err := pgerror.NewError(pgcode.Syntax, "invalid OWNED BY option")
		return nil, nil, errors.WithMessage(err, "Specify OWNED BY table.column or OWNED BY NONE.")
	}
	tableName := columnItem.TableName.ToTableName()
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &tableName, true /* required */, requireTableDesc)
	if err != nil {
		return nil, nil, err
	}
	col, _, err := tableDesc.FindColumnByName(columnItem.ColumnName)
	if err != nil {
		return nil, nil, err
	}
	return tableDesc, &col, nil
}

func addSequenceOwner(
	ctx context.Context,
	p *planner,
	columnItemVal *tree.ColumnItem,
	sequenceID sqlbase.ID,
	opts *sqlbase.TableDescriptor_SequenceOpts,
) error {
	tableDesc, col, err := resolveColumnItemToDescriptors(ctx, p, columnItemVal)
	if err != nil {
		return err
	}

	col.OwnsSequenceIds = append(col.OwnsSequenceIds, sequenceID)
	for k, col := range tableDesc.Columns {
		if col.Name == string(columnItemVal.ColumnName) {
			tableDesc.Columns[k].OwnsSequenceIds = append(col.OwnsSequenceIds, sequenceID)
		}
	}
	opts.SequenceOwner.OwnerColumnID = col.ID
	opts.SequenceOwner.OwnerTableID = tableDesc.GetID()
	return p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID)
}

// dropSequencesOwnedByCol drops all the sequences from col.OwnsSequenceIDs.
// Called when the respective column (or the whole table) is being dropped.
func (p *planner) dropSequencesOwnedByCol(
	ctx context.Context, col *sqlbase.ColumnDescriptor, queueJob bool, behavior tree.DropBehavior,
) error {
	// Copy out the sequence IDs as the code to drop the sequence will reach
	// back around and update the descriptor from underneath us.
	ownsSequenceIDs := append([]sqlbase.ID(nil), col.OwnsSequenceIds...)
	for _, sequenceID := range ownsSequenceIDs {
		seqDesc, err := p.Tables().getMutableTableVersionByID(ctx, sequenceID, p.txn)
		// Special case error swallowing for #50781, which can cause a
		// column to own sequences that do not exist.
		if err != nil {
			if markers.Is(err, ErrDescriptorDropped) ||
				pgerror.GetPGCode(err) == pgcode.UndefinedTable {
				log.Eventf(ctx, "swallowing error dropping owned sequences: %s", err.Error())
				continue
			}
			return err
		}
		// This sequence is already getting dropped. Don't do it twice.
		if seqDesc.Dropped() {
			continue
		}
		//jobDesc := fmt.Sprintf("removing sequence %q dependent on column %q which is being dropped",
		//	seqDesc.Name, col.ColName())
		// Note that this call will end up resolving and modifying the table
		// descriptor.
		if err := p.dropSequenceImpl(
			ctx, seqDesc, behavior,
		); err != nil {
			return err
		}
	}
	return nil
}
