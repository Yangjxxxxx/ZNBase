package util

import (
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
)

// AuditResult get opt result
func AuditResult(err error) string {
	result := "OK"
	if err != nil {
		var sb strings.Builder
		sb.WriteString("ERROR")
		if e, ok := err.(*pgerror.Error); ok {
			sb.WriteString(":")
			sb.WriteString(e.Code.String())
		}
		result = sb.String()
	}
	return result
}

// StringConcat concat string
func StringConcat(split string, str ...string) string {
	var sb strings.Builder
	for i, s := range str {
		sb.WriteString(s)
		if i < len(str)-1 {
			sb.WriteString(split)
		}
	}
	return sb.String()
}

// Max return max value between a and b
func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
