package delegate

import "github.com/znbasedb/znbase/pkg/sql/sem/tree"

func (d *delegator) delegateShowZoneConfig(n *tree.ShowZoneConfig) (tree.Statement, error) {
	// Specifying a specific zone; fallback to non-delegation logic.
	if n.ZoneSpecifier != (tree.ZoneSpecifier{}) {
		return nil, nil
	}
	return parse(`SELECT zone_id, cli_specifier AS zone_name, cli_specifier, config_sql
         FROM zbdb_internal.zones
        WHERE cli_specifier IS NOT NULL`)
}
