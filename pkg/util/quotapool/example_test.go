package quotapool

import (
	"context"
	"sort"

	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
)

// AcquireFunc的一个示例用例是试图获取资源以运行一组异构作业的工作线程池。
// 例如，假设我们有一组工人和一个需要运行的作业列表。
// 此函数可用于选择可由现有配额数量运行的最大作业。
func ExampleIntPool_AcquireFunc() {
	const quota = 7
	const workers = 3
	qp := NewIntPool("work units", quota)
	type job struct {
		name string
		cost uint64
	}
	jobs := []*job{
		{name: "foo", cost: 3},
		{name: "bar", cost: 2},
		{name: "baz", cost: 4},
		{name: "qux", cost: 6},
		{name: "quux", cost: 3},
		{name: "quuz", cost: 3},
	}
	// sortJobs sorts the jobs in highest-to-lowest order with nil last.
	sortJobs := func() {
		sort.Slice(jobs, func(i, j int) bool {
			ij, jj := jobs[i], jobs[j]
			if ij != nil && jj != nil {
				return ij.cost > jj.cost
			}
			return ij != nil
		})
	}
	// getJob finds the largest job which can be run with the current quota.
	getJob := func(
		ctx context.Context, qp *IntPool,
	) (j *job, alloc *IntAlloc, err error) {
		alloc, err = qp.AcquireFunc(ctx, func(
			ctx context.Context, pi PoolInfo,
		) (took uint64, err error) {
			sortJobs()
			// There are no more jobs, take 0 and return.
			if jobs[0] == nil {
				return 0, nil
			}
			// Find the largest jobs which can be run.
			for i := range jobs {
				if jobs[i] == nil {
					break
				}
				if jobs[i].cost <= pi.Available {
					j, jobs[i] = jobs[i], nil
					return j.cost, nil
				}
			}
			return 0, ErrNotEnoughQuota
		})
		return j, alloc, err
	}
	runWorker := func(workerNum int) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			for {
				j, alloc, err := getJob(ctx, qp)
				if err != nil {
					return err
				}
				if j == nil {
					return nil
				}
				alloc.Release()
			}
		}
	}
	g := ctxgroup.WithContext(context.Background())
	for i := 0; i < workers; i++ {
		g.GoCtx(runWorker(i))
	}
	if err := g.Wait(); err != nil {
		panic(err)
	}
}
