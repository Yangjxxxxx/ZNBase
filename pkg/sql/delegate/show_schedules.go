package delegate

import (
	"fmt"

	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowSchedules(n *tree.ShowSchedules) (tree.Statement, error) {
	if n.ScheduleID != nil {
		return parse(
			fmt.Sprintf(`SELECT schedule_id id, schedule_name as name, created,
		next_run, schedule_expr, default_database as database,
		default_searchpath searchpath
		FROM system.scheduled_jobs
		WHERE schedule_id = %v;`, n.ScheduleID))
	}
	return parse(
		fmt.Sprintf(`SELECT schedule_id id, schedule_name as name, created,
		next_run, schedule_expr, default_database as database,
		default_searchpath searchpath
		FROM system.scheduled_jobs
		ORDER BY id;`))
}
