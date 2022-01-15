// Copyright 2020  The Cockroach Authors.

package jobs

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// StartableJob is a job created with a transaction to be started later.
// See Registry.CreateStartableJob
type StartableJob struct {
	*Job
	txn        *client.Txn
	resumer    Resumer
	resumerCtx context.Context
	cancel     context.CancelFunc
	resultsCh  chan<- tree.Datums
	starts     int64 // used to detect multiple calls to Start()
}

// Start will resume the job. The transaction used to create the StartableJob
// must be committed. If a non-nil error is returned, the job was not started
// and nothing will be send on errCh. Clients must not start jobs more than
// once.
func (sj *StartableJob) Start(ctx context.Context) (errCh <-chan error, err error) {
	if starts := atomic.AddInt64(&sj.starts, 1); starts != 1 {
		return nil, errors.AssertionFailedf(
			"StartableJob %d cannot be started more than once", *sj.ID())
	}
	defer func() {
		if err != nil {
			sj.registry.unregister(*sj.ID())
		}
	}()
	if !sj.txn.IsCommitted() {
		return nil, fmt.Errorf("cannot resume %T job which is not committed", sj.resumer)
	}
	if err := sj.Started(ctx); err != nil {
		return nil, err
	}
	errCh, err = sj.registry.resume(sj.resumerCtx, sj.resumer, sj.resultsCh, sj.Job)
	if err != nil {
		return nil, err
	}
	return errCh, nil
}

// CleanupOnRollback will unregister the job in the case that the creating
// transaction has been rolled back.
func (sj *StartableJob) CleanupOnRollback(ctx context.Context) error {
	if sj.txn.IsCommitted() {
		return errors.AssertionFailedf(
			"cannot call CleanupOnRollback for a StartableJob created by a committed transaction")
	}
	if !sj.txn.Sender().TxnStatus().IsFinalized() {
		return errors.AssertionFailedf(
			"cannot call CleanupOnRollback for a StartableJob with a non-finalized transaction")
	}
	sj.registry.unregister(*sj.ID())
	return nil
}

// Cancel will mark the job as canceled and release its resources in the
// Registry.
func (sj *StartableJob) Cancel(ctx context.Context) error {
	defer sj.registry.unregister(*sj.ID())
	return sj.registry.Cancel(ctx, nil, *sj.ID())
}
