package sql

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

func (p *planner) showSchedules(ctx context.Context, n *tree.ShowSchedules) (planNode, error) {
	if n.ScheduleID != nil {
		return p.delegateQuery(ctx, "SHOW SCHEDULE",
			fmt.Sprintf(`SELECT schedule_id id, schedule_name as name, created,
		next_run, schedule_expr, default_database as database,
		default_searchpath searchpath
		FROM system.scheduled_jobs
		WHERE schedule_id = %v;`, n.ScheduleID),
			nil, nil)
	}
	return p.delegateQuery(ctx, "SHOW SCHEDULES",
		fmt.Sprintf(`SELECT schedule_id id, schedule_name as name, created,
		next_run, schedule_expr, default_database as database,
		default_searchpath searchpath
		FROM system.scheduled_jobs
		ORDER BY id;`),
		nil, nil)
}
