// Copyright 2019  The Cockroach Authors.

package coldata

import (
	"os"
	"testing"

	"github.com/znbasedb/znbase/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	os.Exit(m.Run())
}
