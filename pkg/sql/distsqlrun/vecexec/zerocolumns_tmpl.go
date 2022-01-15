// Copyright 2019  The Cockroach Authors.

// {{/*
// +build execgen_template
//
// This file is the execgen template for min_agg.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package vecexec

import (
	"time"

	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/col/coldata"
)

// {{/*
// Declarations to make the template compile properly

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// */}}

// {{range .}}
var zero_TYPEColumn = make([]_GOTYPE, coldata.BatchSize())

// {{end}}
