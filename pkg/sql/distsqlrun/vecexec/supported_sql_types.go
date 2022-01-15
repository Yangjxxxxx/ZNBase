// Copyright 2019  The Cockroach Authors.

package vecexec

import "github.com/znbasedb/znbase/pkg/sql/sem/types1"

// allSupportedSQLTypes is a slice of all SQL types that the vectorized engine
// currently supports. It should be kept in sync with typeconv.FromColumnType().
var allSupportedSQLTypes = []types1.T{
	*types1.Bool,
	*types1.Bytes,
	*types1.Date,
	*types1.Decimal,
	*types1.Int,
	*types1.Oid,
	*types1.Float,
	*types1.String,
	*types1.Timestamp,
}
