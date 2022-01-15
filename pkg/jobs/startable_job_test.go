package jobs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// TestStartableJobErrors tests that the StartableJob returns the expected
// errors when used incorrectly and performs the appropriate cleanup in
// CleanupOnRollback.
func TestStartableJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	jr := s.JobRegistry().(*jobs.Registry)
	jobs.AddResumeHook(func(jobType jobspb.Type, settings *cluster.Settings) jobs.Resumer {
		if jobType != jobspb.TypeRestore {
			return nil
		}
		return jobs.FakeResumer{
			OnResume: func(*jobs.Job) error {
				return nil
			},
		}
	})
	rec := jobs.Record{
		Description:   "There's a snake in my boot!",
		Username:      "Woody Pride",
		DescriptorIDs: []sqlbase.ID{1, 2, 3},
		Details:       jobspb.RestoreDetails{},
		Progress:      jobspb.RestoreProgress{},
	}
	createStartableJob := func(t *testing.T) (sj *jobs.StartableJob) {
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *client.Txn) (err error) {
			sj, err = jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
			return err
		}))
		return sj
	}
	t.Run("Start called more than once", func(t *testing.T) {
		sj := createStartableJob(t)
		_, err := sj.Start(ctx)
		require.NoError(t, err)
		_, err = sj.Start(ctx)
		require.Regexp(t, `StartableJob \d+ cannot be started more than once`, err)
	})
	t.Run("Start called with active txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		defer func() {
			require.NoError(t, txn.Rollback(ctx))
		}()
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		_, err = sj.Start(ctx)
		require.Regexp(t, `cannot resume .* job which is not committed`, err)
	})
	t.Run("Start called with aborted txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback(ctx))
		_, err = sj.Start(ctx)
		require.Regexp(t, `cannot resume .* job which is not committed`, err)
	})
	t.Run("CleanupOnRollback called with active txn", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		defer func() {
			require.NoError(t, txn.Rollback(ctx))
		}()
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		err = sj.CleanupOnRollback(ctx)
		require.Regexp(t, `cannot call CleanupOnRollback for a StartableJob with a non-finalized transaction`, err)
	})
	t.Run("CleanupOnRollback called with committed txn", func(t *testing.T) {
		sj := createStartableJob(t)
		err := sj.CleanupOnRollback(ctx)
		require.Regexp(t, `cannot call CleanupOnRollback for a StartableJob created by a committed transaction`, err)
	})
	t.Run("CleanupOnRollback positive case", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback(ctx))
		require.NoError(t, sj.CleanupOnRollback(ctx))
		for _, id := range jr.CurrentlyRunningJobs() {
			require.NotEqual(t, id, *sj.ID())
		}
	})
	t.Run("Cancel", func(t *testing.T) {
		txn := db.NewTxn(ctx, "test")
		sj, err := jr.CreateStartableJobWithTxn(ctx, rec, txn, nil)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctx))
		require.NoError(t, sj.Cancel(ctx))
		status, err := sj.CurrentStatus(ctx)
		require.NoError(t, err)
		require.Equal(t, jobs.StatusCanceled, status)
		for _, id := range jr.CurrentlyRunningJobs() {
			require.NotEqual(t, id, *sj.ID())
		}
		_, err = sj.Start(ctx)
		require.NoError(t, err)
		// require.Regexp(t, "job with status cancel-requested cannot be marked started", err)
	})
}
