package delegate

import (
	"context"

	"github.com/znbasedb/znbase/pkg/sql/opt/cat"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// Certain statements (most SHOW variants) are just syntactic sugar for a more
// complicated underlying query.
//
// This package contains the logic to convert the AST of such a statement to the
// AST of the equivalent query to be planned.

// TryDelegate takes a statement and checks if it is one of the statements that
// can be rewritten as a lower level query. If it can, returns a new AST which
// is equivalent to the original statement. Otherwise, returns nil.
func TryDelegate(
	ctx context.Context, catalog cat.Catalog, evalCtx *tree.EvalContext, stmt tree.Statement,
) (tree.Statement, error) {
	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}
	switch t := stmt.(type) {
	case *tree.ShowClusterSettingList:
		return d.delegateShowClusterSettingList(t)
	//case *tree.ShowVar:
	//	return d.delegateShowVar(t)
	case *tree.ShowColumns:
		return d.delegateShowColumns(t)
	case *tree.ShowConstraints:
		return d.delegateShowConstraints(t)
	case *tree.ShowCreate:
		return d.delegateShowCreate(t)
	case *tree.ShowDatabases:
		return d.delegateShowDatabases(t)
	//case *tree.ShowGrants:
	//	return d.delegateShowGrants(t)
	case *tree.ShowIndexes:
		return d.delegateShowIndexes(t)
	case *tree.ShowQueries:
		return d.delegateShowQueries(t)
	case *tree.ShowJobs:
		return d.delegateShowJobs(t)
	case *tree.ShowRoleGrants:
		return d.delegateShowRoleGrants(t)
	case *tree.ShowRoles:
		return d.delegateShowRoles()
	case *tree.ShowSpaces:
		return d.delegateShowSpaces()
	case *tree.ShowSessions:
		return d.delegateShowSessions(t)
	case *tree.ShowSyntax:
		return d.delegateShowSyntax(t)
	//case *tree.ShowTables:
	//	return d.delegateShowTables(t)
	case *tree.ShowSchemas:
		return d.delegateShowSchemas(t)
	case *tree.ShowSequences:
		return d.delegateShowSequences(t)
	case *tree.ShowTransactionStatus:
		return d.delegateShowVar(&tree.ShowVar{Name: "transaction_status"})
	case *tree.ShowSavepointStatus:
		return d.delegateShowSavepointStatus()
	case *tree.ShowUsers:
		return d.delegateShowUsers()
	case *tree.ShowZoneConfig:
		return d.delegateShowZoneConfig(t)
	case *tree.ShowSchedules:
		return d.delegateShowSchedules(t)
	default:
		return nil, nil
	}
}

type delegator struct {
	ctx     context.Context
	catalog cat.Catalog
	evalCtx *tree.EvalContext
}

func (d *delegator) resolveMemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	ret := map[string]bool{}

	// Keep track of members we looked up.
	visited := map[string]struct{}{}
	toVisit := []string{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		rows, err := d.evalCtx.InternalExecutor.Query(
			ctx, "expand-roles", nil /* txn */, lookupRolesStmt, m,
		)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			roleName := tree.MustBeDString(row[0])
			isAdmin := row[1].(*tree.DBool)

			ret[string(roleName)] = bool(*isAdmin)

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, string(roleName))
		}
	}

	return ret, nil
}

func parse(sql string) (tree.Statement, error) {
	s, err := parser.ParseOne(sql, false)
	return s.AST, err
}
