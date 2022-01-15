// Copyright 2019  The Cockroach Authors.

package main

import (
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"text/template"

	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

func genVecComparators(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/distsqlrun/vecexec/vec_comparators_tmpl.go")
	if err != nil {
		return err
	}
	s := string(d)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)
	compareRe := regexp.MustCompile(`_COMPARE\((.*),(.*),(.*)\)`)
	s = compareRe.ReplaceAllString(s, "{{.Compare $1 $2 $3}}")

	s = replaceManipulationFuncs(".LTyp", s)

	tmpl, err := template.New("vec_comparators").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.LT])
}

func init() {
	registerGenerator(genVecComparators, "vec_comparators.eg.go")
}
