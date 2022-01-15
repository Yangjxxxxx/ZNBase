// Copyright 2019  The Cockroach Authors.

package vecexec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestCFetcherUninitialized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Regression test for #36570: make sure it's okay to call GetRangesInfo even
	// before the fetcher was fully initialized.
	var fetcher cFetcher

	assert.Nil(t, fetcher.GetRangesInfo())
}
