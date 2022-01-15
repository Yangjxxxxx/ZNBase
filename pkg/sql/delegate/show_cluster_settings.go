package delegate

import (
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowClusterSettingList(
	stmt *tree.ShowClusterSettingList,
) (tree.Statement, error) {
	err := d.catalog.HasRoleOption(d.ctx, "SHOW CLUSTER SETTINGS")
	if err != nil {
		return nil, pgerror.NewError(pgcode.InsufficientPrivilege, err.Error())
	}
	return parse(
		`SELECT variable, value, type AS setting_type, description
     FROM   zbdb_internal.cluster_settings`,
	)
}
