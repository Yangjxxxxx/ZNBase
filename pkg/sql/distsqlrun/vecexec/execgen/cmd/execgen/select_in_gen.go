// Copyright 2019  The Cockroach Authors.

package main

import (
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

func genSelectIn(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/distsqlrun/vecexec/select_in_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	assignEq := makeFunctionRegex("_ASSIGN_EQ", 3)
	s = assignEq.ReplaceAllString(s, `{{.Assign "$1" "$2" "$3"}}`)
	s = strings.Replace(s, "_GOTYPE", "{{.LGoType}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	s = replaceManipulationFuncs(".LTyp", s)

	tmpl, err := template.New("select_in").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genSelectIn, "select_in.eg.go")
}
