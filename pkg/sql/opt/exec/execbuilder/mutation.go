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

package execbuilder

import (
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util"
)

func (b *Builder) buildMutationInput(
	mutExpr, inputExpr memo.RelExpr, colList opt.ColList, p *memo.MutationPrivate,
) (execPlan, error) {
	if b.shouldApplyImplicitLockingToMutationInput(mutExpr) {
		// Re-entrance is not possible because mutations are never nested.
		b.forceForUpdateLocking = true
		defer func() { b.forceForUpdateLocking = false }()
	}

	input, err := b.buildRelational(inputExpr)
	if err != nil {
		return execPlan{}, err
	}

	input, err = b.ensureColumns(input, colList, nil, inputExpr.ProvidedPhysical().Ordering)
	if err != nil {
		return execPlan{}, err
	}

	return input, nil
}

// forUpdateLocking is the row-level locking mode used by mutations during their
// initial row scan, when such locking is deemed desirable. The locking mode is
// equivalent that used by a SELECT ... FOR UPDATE statement.
var forUpdateLocking = &tree.LockingItem{Strength: tree.ForUpdate}

// shouldApplyImplicitLockingToMutationInput determines whether or not the
// builder should apply a FOR UPDATE row-level locking mode to the initial row
// scan of a mutation expression.
func (b *Builder) shouldApplyImplicitLockingToMutationInput(mutExpr memo.RelExpr) bool {
	switch t := mutExpr.(type) {
	case *memo.InsertExpr:
		// Unlike with the other three mutation expressions, it never makes
		// sense to apply implicit row-level locking to the input of an INSERT
		// expression because any contention results in unique constraint
		// violations.
		return false

	case *memo.UpdateExpr:
		return b.shouldApplyImplicitLockingToUpdateInput(t)

	case *memo.UpsertExpr:
		return b.shouldApplyImplicitLockingToUpsertInput(t)

	case *memo.DeleteExpr:
		return b.shouldApplyImplicitLockingToDeleteInput(t)

	default:
		panic(errors.AssertionFailedf("unexpected mutation expression %T", t))
	}
}

// shouldApplyImplicitLockingToUpdateInput determines whether or not the builder
// should apply a FOR UPDATE row-level locking mode to the initial row scan of
// an UPDATE statement.
//
// Conceptually, if we picture an UPDATE statement as the composition of a
// SELECT statement and an INSERT statement (with loosened semantics around
// existing rows) then this method determines whether the builder should perform
// the following transformation:
//
//   UPDATE t = SELECT FROM t + INSERT INTO t
//   =>
//   UPDATE t = SELECT FROM t FOR UPDATE + INSERT INTO t
//
// The transformation is conditional on the UPDATE expression tree matching a
// pattern. Specifically, the FOR UPDATE locking mode is only used during the
// initial row scan when all row filters have been pushed into the ScanExpr. If
// the statement includes any filters that cannot be pushed into the scan then
// no row-level locking mode is applied. The rationale here is that FOR UPDATE
// locking is not necessary for correctness due to serializable isolation, so it
// is strictly a performance optimization for contended writes. Therefore, it is
// not worth risking the transformation being a pessimization, so it is only
// applied when doing so does not risk creating artificial contention.
func (b *Builder) shouldApplyImplicitLockingToUpdateInput(upd *memo.UpdateExpr) bool {
	if !b.evalCtx.SessionData.ImplicitSelectForUpdate {
		return false
	}

	if b.evalCtx.Txn != nil && b.evalCtx.Txn.IsolationLevel() == util.ReadCommittedIsolation {
		return false
	}
	// Try to match the pattern:
	//
	//  (Update
	//  	$input:(Scan $scanPrivate:*)
	//  	$checks:*
	//  	$mutationPrivate:*
	//  )
	//
	// Or
	//
	//  (Update
	//  	$input:(Project
	//  		(Scan $scanPrivate:*)
	//  		$projections:*
	//  		$passthrough:*
	//  	)
	//  	$checks:*
	//  	$mutationPrivate:*
	//  )
	//
	_, ok := upd.Input.(*memo.ScanExpr)
	if !ok {
		proj, ok := upd.Input.(*memo.ProjectExpr)
		if !ok {
			return false
		}
		sel, ok := proj.Input.(*memo.SelectExpr)
		if !ok {
			return false
		}
		_, ok = sel.Input.(*memo.ScanExpr)
		if !ok {
			return false
		}
	}
	return true
}

// tryApplyImplicitLockingToUpsertInput determines whether or not the builder
// should apply a FOR UPDATE row-level locking mode to the initial row scan of
// an UPSERT statement.
//
// TODO(nvanbenschoten): implement this method to match on appropriate Upsert
// expression trees and apply a row-level locking mode.
func (b *Builder) shouldApplyImplicitLockingToUpsertInput(ups *memo.UpsertExpr) bool {
	return false
}

// tryApplyImplicitLockingToDeleteInput determines whether or not the builder
// should apply a FOR UPDATE row-level locking mode to the initial row scan of
// an DELETE statement.
//
// TODO(nvanbenschoten): implement this method to match on appropriate Delete
// expression trees and apply a row-level locking mode.
func (b *Builder) shouldApplyImplicitLockingToDeleteInput(del *memo.DeleteExpr) bool {
	return false
}
