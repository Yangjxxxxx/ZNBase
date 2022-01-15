// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"testing"

	"github.com/znbasedb/znbase/pkg/testutils/buildutil"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buildutil.VerifyNoImports(t,
		"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec", true,
		[]string{
			"github.com/znbasedb/znbase/pkg/sql/flowinfra/vecflow",
			"github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec",
			"github.com/znbasedb/znbase/pkg/sql/flowinfra/rowflow",
		}, nil,
	)
}
