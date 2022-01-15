// Copyright 2019  The Cockroach Authors.

package coldata

import (
	"testing"

	"github.com/znbasedb/znbase/pkg/testutils/buildutil"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buildutil.VerifyNoImports(t,
		"github.com/znbasedb/znbase/pkg/col/coldata", true,
		[]string{
			"github.com/znbasedb/znbase/pkg/sql/sqlbase",
			"github.com/znbasedb/znbase/pkg/sql/sem/tree",
		}, nil,
	)
}
