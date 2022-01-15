// Copyright 2019  The Cockroach Authors.

package main

import (
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

func genMergeJoinBase(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/distsqlrun/vecexec/mergejoinbase_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	assignEqRe := makeFunctionRegex("_ASSIGN_EQ", 3)
	s = assignEqRe.ReplaceAllString(s, `{{.Eq.Assign $1 $2 $3}}`)

	s = replaceManipulationFuncs(".LTyp", s)

	tmpl, err := template.New("mergejoinbase").Parse(s)
	if err != nil {
		return err
	}

	allOverloads := intersectOverloads(sameTypeComparisonOpToOverloads[tree.EQ], sameTypeComparisonOpToOverloads[tree.LT], sameTypeComparisonOpToOverloads[tree.GT])

	// Create an mjOverload for each overload comznbaseng three overloads so that
	// the template code can access all of EQ, LT, and GT in the same range loop.
	mjOverloads := make([]mjOverload, len(allOverloads[0]))
	for i := range allOverloads[0] {
		mjOverloads[i] = mjOverload{
			overload: *allOverloads[0][i],
			Eq:       allOverloads[0][i],
			Lt:       allOverloads[1][i],
			Gt:       allOverloads[2][i],
		}
	}

	return tmpl.Execute(wr, struct {
		MJOverloads interface{}
	}{
		MJOverloads: mjOverloads,
	})
}

func init() {
	registerGenerator(genMergeJoinBase, "mergejoinbase.eg.go")
}
