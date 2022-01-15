package util

import (
	"fmt"

	"github.com/znbasedb/znbase/pkg/settings"
)

// MaxTxnRefreshAttempts defines the maximum number of times a single
// transactional batch can trigger a refresh spans attempt. A batch
// may need multiple refresh attempts if it runs into progressively
// larger timestamps as more and more of its component requests are
// executed.
var MaxTxnRefreshAttempts = settings.RegisterNonNegativeIntSetting(
	"kv.transaction.max_refresh_attempts",
	"number of the maximum number of times a single transactional batch can trigger a refresh spans attempt",
	5,
)

// IsolationLevel holds the isolation level for a transaction.
type IsolationLevel int

// IsolationLevel values
const (
	UnspecifiedIsolation IsolationLevel = iota
	ReadCommittedIsolation
	SerializableIsolation
	UnreachableIsolation
)

// DefaultIsolation value
const DefaultIsolation = SerializableIsolation

// IsolationLevelNames are transaction isolation level names.
var IsolationLevelNames = [...]string{
	UnspecifiedIsolation:   "SERIALIZABLE",
	ReadCommittedIsolation: "READ COMMITTED",
	SerializableIsolation:  "SERIALIZABLE",
}

func (i IsolationLevel) String() string {
	if i < 0 || i >= UnreachableIsolation {
		return fmt.Sprintf("IsolationLevel(%d)", i)
	}
	return IsolationLevelNames[i]
}

// IsolationLevelMap is a map from string isolation level name to isolation
// level, in the lowercase format that set isolation_level supports.
var IsolationLevelMap = map[string]IsolationLevel{
	"serializable":     SerializableIsolation,
	"read committed":   ReadCommittedIsolation,
	"default":          SerializableIsolation,
	"read uncommitted": ReadCommittedIsolation,
	"snapshot":         SerializableIsolation,
	"repeatable read":  SerializableIsolation,
}

// NoneIsolationLevelMap is a map from string isolation level name to
// part isolation level that execute with serializable isolation.
var NoneIsolationLevelMap = map[string]IsolationLevel{
	"default":          SerializableIsolation,
	"read uncommitted": ReadCommittedIsolation,
	"snapshot":         SerializableIsolation,
	"repeatable read":  SerializableIsolation,
}
