// Copyright 2019  The Cockroach Authors.

package vecexec

// DefaultVectorizeRowCountThreshold denotes the default row count threshold.
// When it is met, the vectorized execution engine will be used if possible.
// The current number 1000 was chosen upon comparing `SELECT count(*) FROM t`
// query running through the row and the vectorized execution engines on a
// single node with tables having different number of columns.
// Note: if you are updating this field, please make sure to update
// vectorize_threshold logic test accordingly.
const DefaultVectorizeRowCountThreshold = 1000
