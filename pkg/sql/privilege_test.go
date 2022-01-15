package sql

import (
	"context"
	"testing"

	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestRowIDPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := sqlbase.TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Name:     "foo",
		Columns: []sqlbase.ColumnDescriptor{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		},
		Privileges: sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, sqlbase.AdminRole),
	}
	ctx := context.TODO()
	p := makeTestPlanner()
	cols := tree.NameList{"a", "b", "c"}

	err := p.checkColumnPrivilege(ctx, &desc, cols, privilege.SELECT, security.RootUser)

	if err != nil {
		t.Fatalf("expected no error; got %v", err)
	}

	for _, tc := range desc.Columns {
		if columnErr := p.CheckPrivilege(ctx, &tc, privilege.SELECT); columnErr != nil {
			t.Fatalf("expected no error; got %v", columnErr)
		}
	}
}
