// Copyright 2019  The Cockroach Authors.

package main

import (
	"io"
	"io/ioutil"
	"strings"
	"text/template"
)

func genCastOperators(wr io.Writer) error {
	t, err := ioutil.ReadFile("pkg/sql/distsqlrun/vecexec/cast_tmpl.go")
	if err != nil {
		return err
	}

	s := string(t)

	assignCast := makeFunctionRegex("_ASSIGN_CAST", 2)
	s = assignCast.ReplaceAllString(s, `{{.Assign "$1" "$2"}}`)
	s = strings.Replace(s, "_ALLTYPES", "{{$typ}}", -1)
	s = strings.Replace(s, "_OVERLOADTYPES", "{{.ToTyp}}", -1)
	s = strings.Replace(s, "_FROMTYPE", "{{.FromTyp}}", -1)
	s = strings.Replace(s, "_TOTYPE", "{{.ToTyp}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.ToGoTyp}}", -1)

	// replace _FROM_TYPE_SLICE's with execgen.SLICE's of the correct type.
	s = strings.Replace(s, "_FROM_TYPE_SLICE", "execgen.SLICE", -1)
	s = replaceManipulationFuncs(".FromTyp", s)

	// replace the _FROM_TYPE_UNSAFEGET's with execgen.UNSAFEGET's of the correct type.
	s = strings.Replace(s, "_FROM_TYPE_UNSAFEGET", "execgen.UNSAFEGET", -1)
	s = replaceManipulationFuncs(".FromTyp", s)

	// replace the _TO_TYPE_SET's with execgen.SET's of the correct type
	s = strings.Replace(s, "_TO_TYPE_SET", "execgen.SET", -1)
	s = replaceManipulationFuncs(".ToTyp", s)

	isCastFuncSet := func(ov castOverload) bool {
		return ov.AssignFunc != nil
	}

	tmpl, err := template.New("cast").Funcs(template.FuncMap{"isCastFuncSet": isCastFuncSet}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, castOverloads)
}

func init() {
	registerGenerator(genCastOperators, "cast.eg.go")
}
