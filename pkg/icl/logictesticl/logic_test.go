package logictesticl

import (
	"testing"

	_ "github.com/znbasedb/znbase/pkg/icl"
	_ "github.com/znbasedb/znbase/pkg/sql/gcjob"
	"github.com/znbasedb/znbase/pkg/sql/logictest"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestICLLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logictest.RunLogicTest(t, logictest.GetConfig(), "testdata/logic_test/[^.]*")
}
