// Copyright 2019  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	gosql "database/sql"
	"testing"
	"time"

	"github.com/znbasedb/znbase/pkg/icl/changefeedicl/cdctest"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestCDCNemeses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func(i time.Duration) { jobs.DefaultAdoptInterval = i }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		v, err := cdctest.RunNemesis(f, db)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		for _, failure := range v.Failures() {
			t.Error(failure)
		}
	}
	t.Run(`sinkless`, sinklessTest(testFn))
	// TODO(dan): This deadlocks t.Run(`enterprise`, enterpriseTest(testFn))
	// TODO(dan): This deadlocks t.Run(`poller`, pollerTest(sinklessTest, testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
}
