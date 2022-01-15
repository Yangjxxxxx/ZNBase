package sql

import (
	"testing"

	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestGetFetchActionStartpositionAndEndposition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cursorAccess := &tree.CursorAccess{
		CursorName: tree.Name("cur1"),
		Action:     tree.FETCH,
		Rows:       2,
		Direction:  tree.FORWARD,
	}
	cursorDescriptor := &sqlbase.CursorDescriptor{
		Name:      "cur1",
		Position:  2,
		MaxRownum: 3,
	}
	var okStart, okEnd int64 = 3, 5
	start, end := GetFetchActionStartpositionAndEndposition(cursorAccess, cursorDescriptor)
	if start != okStart {
		t.Fatalf("start should be %d,but result is %d !\n", okStart, start)
	}
	if end != okEnd {
		t.Fatalf("end should be %d,but result is %d !\n", okEnd, end)
	}
}
