package icl

// We import each of the ICL packages that use init hooks below, so a single
// import of this package enables building a binary with ICL features.

import (
	// icl init hooks. Don't include cliicl here, it pulls in pkg/cli, which
	// does weird things at init time.
	_ "github.com/znbasedb/znbase/pkg/icl/changefeedicl"
	_ "github.com/znbasedb/znbase/pkg/icl/dump"
	_ "github.com/znbasedb/znbase/pkg/icl/flashback"
	_ "github.com/znbasedb/znbase/pkg/icl/followerreadsicl"
	_ "github.com/znbasedb/znbase/pkg/icl/gssapiicl"
	_ "github.com/znbasedb/znbase/pkg/icl/load"
	_ "github.com/znbasedb/znbase/pkg/icl/locatespaceicl"
	_ "github.com/znbasedb/znbase/pkg/icl/roleicl"
	_ "github.com/znbasedb/znbase/pkg/icl/snapshot"
	_ "github.com/znbasedb/znbase/pkg/icl/storageicl"
	_ "github.com/znbasedb/znbase/pkg/icl/storageicl/engineicl"
	_ "github.com/znbasedb/znbase/pkg/icl/utilicl/intervalicl"
)
