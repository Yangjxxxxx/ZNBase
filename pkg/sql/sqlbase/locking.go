package sqlbase

import (
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// ToScanLockingStrength converts a tree.LockingStrength to its corresponding
// ScanLockingStrength.
func ToScanLockingStrength(s tree.LockingStrength) ScanLockingStrength {
	switch s {
	case tree.ForNone:
		return ScanLockingStrength_FOR_NONE
	case tree.ForKeyShare:
		return ScanLockingStrength_FOR_KEY_SHARE
	case tree.ForShare:
		return ScanLockingStrength_FOR_SHARE
	case tree.ForNoKeyUpdate:
		return ScanLockingStrength_FOR_NO_KEY_UPDATE
	case tree.ForUpdate:
		return ScanLockingStrength_FOR_UPDATE
	default:
		panic(errors.AssertionFailedf("unknown locking strength %s", s))
	}
}

// ToScanLockingWaitPolicy converts a tree.LockingWaitPolicy to its
// corresponding ScanLockingWaitPolicy.
func ToScanLockingWaitPolicy(wp tree.LockingWaitPolicy) ScanLockingWaitPolicy {
	policy := ScanLockingWaitPolicy{}
	switch wp.LockingType {
	case tree.LockWaitBlock:
		policy.LockLevel = ScanLockingWaitLevel_BLOCK
	case tree.LockWaitSkip:
		policy.LockLevel = ScanLockingWaitLevel_SKIP
	case tree.LockWaitError:
		policy.LockLevel = ScanLockingWaitLevel_ERROR
	case tree.LockWait:
		policy.LockLevel = ScanLockingWaitLevel_WAIT
		policy.WaitTime = wp.WaitTime
	default:
		panic(errors.AssertionFailedf("unknown locking wait policy %s", wp))
	}
	return policy
}
