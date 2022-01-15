package execbuilder

import (
	"github.com/znbasedb/znbase/pkg/sql/opt"
	"github.com/znbasedb/znbase/pkg/sql/opt/exec"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
)

func (b *Builder) buildAlterTableSplit(split *memo.AlterTableSplitExpr) (execPlan, error) {
	input, err := b.buildRelational(split.Input)
	if err != nil {
		return execPlan{}, err
	}
	scalarCtx := buildScalarCtx{}
	expiration, err := b.buildScalar(&scalarCtx, split.Expiration)
	if err != nil {
		return execPlan{}, err
	}
	table := b.mem.Metadata().Table(split.Table)
	node, err := b.factory.ConstructAlterTableSplit(
		table.Index(split.Index),
		input.root,
		expiration,
	)
	if err != nil {
		return execPlan{}, err
	}
	return planWithColumns(node, split.Columns), nil
}

func (b *Builder) buildAlterTableRelocate(relocate *memo.AlterTableRelocateExpr) (execPlan, error) {
	input, err := b.buildRelational(relocate.Input)
	if err != nil {
		return execPlan{}, err
	}
	table := b.mem.Metadata().Table(relocate.Table)
	node, err := b.factory.ConstructAlterTableRelocate(
		table.Index(relocate.Index),
		input.root,
		relocate.RelocateLease,
	)
	if err != nil {
		return execPlan{}, err
	}
	return planWithColumns(node, relocate.Columns), nil
}

func (b *Builder) buildControlJobs(ctl *memo.ControlJobsExpr) (execPlan, error) {
	input, err := b.buildRelational(ctl.Input)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructControlJobs(
		ctl.Command,
		input.root,
	)
	if err != nil {
		return execPlan{}, err
	}
	// ControlJobs returns no columns.
	return execPlan{root: node}, nil
}

func (b *Builder) buildCancelQueries(cancel *memo.CancelQueriesExpr) (execPlan, error) {
	input, err := b.buildRelational(cancel.Input)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructCancelQueries(input.root, cancel.IfExists)
	if err != nil {
		return execPlan{}, err
	}
	// CancelQueries returns no columns.
	return execPlan{root: node}, nil
}

func (b *Builder) buildCancelSessions(cancel *memo.CancelSessionsExpr) (execPlan, error) {
	input, err := b.buildRelational(cancel.Input)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructCancelSessions(input.root, cancel.IfExists)
	if err != nil {
		return execPlan{}, err
	}
	// CancelSessions returns no columns.
	return execPlan{root: node}, nil
}

// planWithColumns creates an execPlan for a node which has a fixed output
// schema.
func planWithColumns(node exec.Node, cols opt.ColList) execPlan {
	ep := execPlan{root: node}
	for i, c := range cols {
		ep.outputCols.Set(int(c), i)
	}
	return ep
}
