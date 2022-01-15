// Copyright 2019  The Cockroach Authors.

package rowexec

import (
	"testing"

	"github.com/znbasedb/znbase/pkg/testutils/buildutil"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buildutil.VerifyNoImports(t,
		"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase", true,
		[]string{
			"github.com/znbasedb/znbase/pkg/sql/vecexec",
			"github.com/znbasedb/znbase/pkg/sql/vecflow",
			"github.com/znbasedb/znbase/pkg/sql/flowinfra",
			"github.com/znbasedb/znbase/pkg/sql/rowexec",
			"github.com/znbasedb/znbase/pkg/sql/rowflow",
		}, nil,
	)
}
