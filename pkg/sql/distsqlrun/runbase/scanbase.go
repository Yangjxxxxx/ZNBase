// Copyright 2019  The Cockroach Authors.

package runbase

import "github.com/znbasedb/znbase/pkg/sql/sqlbase"

// ParallelScanResultThreshold is the number of results up to which, if the
// maximum number of results returned by a scan is known, the table reader
// disables batch limits in the dist sender. This results in the parallelization
// of these scans.
const ParallelScanResultThreshold = 10000

// ScanShouldLimitBatches returns whether the scan should pace itself.
func ScanShouldLimitBatches(maxResults uint64, limitHint int64, flowCtx *FlowCtx) bool {
	// We don't limit batches if the scan doesn't have a limit, and if the
	// spans scanned will return less than the ParallelScanResultThreshold.
	// This enables distsender parallelism - if we limit batches, distsender
	// does *not* parallelize multi-range scan requests.
	if maxResults != 0 &&
		maxResults < ParallelScanResultThreshold &&
		limitHint == 0 &&
		sqlbase.ParallelScans.Get(&flowCtx.Cfg.Settings.SV) {
		return false
	}
	return true
}
