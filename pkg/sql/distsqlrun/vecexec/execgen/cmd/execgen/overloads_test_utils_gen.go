// Copyright 2019  The Cockroach Authors.

package main

import (
	"io"
	"text/template"
)

const overloadsTestUtilsTemplate = `
package vecexec

import (
	"bytes"
	"math"
	"time"

	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec/execerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

{{define "opName"}}perform{{.Name}}{{.LTyp}}{{.RTyp}}{{end}}

{{/* The range is over all overloads */}}
{{range .}}

func {{template "opName" .}}(a {{.LTyp.GoTypeName}}, b {{.RTyp.GoTypeName}}) {{.RetTyp.GoTypeName}} {
	var r {{.RetTyp.GoTypeName}}
	{{(.Assign "r" "a" "b")}}
	return r
}

{{end}}
`

// genOverloadsTestUtils creates a file that has a function for each overload
// defined in overloads.go. This is so that we can more easily test each
// overload.
func genOverloadsTestUtils(wr io.Writer) error {
	tmpl, err := template.New("overloads_test_utils").Parse(overloadsTestUtilsTemplate)
	if err != nil {
		return err
	}

	allOverloads := make([]*overload, 0)
	allOverloads = append(allOverloads, binaryOpOverloads...)
	allOverloads = append(allOverloads, comparisonOpOverloads...)
	return tmpl.Execute(wr, allOverloads)
}

func init() {
	registerGenerator(genOverloadsTestUtils, "overloads_test_utils.eg.go")
}
