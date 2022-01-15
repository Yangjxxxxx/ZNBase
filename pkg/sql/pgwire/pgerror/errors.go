// Copyright 2016  The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgerror

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/lib/pq"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/util/caller"
	"github.com/znbasedb/znbase/pkg/util/log"
)

var _ error = &Error{}

// Error implements the error interface.
func (pg *Error) Error() string {
	return pg.Message
}

// ErrorHint implements the hintdetail.ErrorHinter interface.
func (pg *Error) ErrorHint() string { return pg.Hint }

// ErrorDetail implements the hintdetail.ErrorDetailer interface.
func (pg *Error) ErrorDetail() string { return pg.Detail }

// FullError can be used when the hint and/or detail are to be tested.
func FullError(err error) string {
	var errString string
	if pqErr, ok := err.(*pq.Error); ok {
		errString = formatMsgHintDetail("pq", pqErr.Message, pqErr.Hint, pqErr.Detail)
	} else if pg, ok := GetPGCause(err); ok {
		errString = formatMsgHintDetail(pg.Severity, err.Error(), pg.Hint, pg.Detail)
	} else {
		errString = err.Error()
	}
	return errString
}

func formatMsgHintDetail(prefix, msg, hint, detail string) string {
	var b strings.Builder
	b.WriteString(prefix)
	b.WriteString(": ")
	b.WriteString(msg)
	if hint != "" {
		b.WriteString("\nHINT: ")
		b.WriteString(hint)
	}
	if detail != "" {
		b.WriteString("\nDETAIL: ")
		b.WriteString(detail)
	}
	return b.String()
}

// NewErrorWithDepthf creates an Error and extracts the context
// information at the specified depth level.
func NewErrorWithDepthf(depth int, code pgcode.Code, format string, args ...interface{}) *Error {
	srcCtx := makeSrcCtx(depth + 1)
	return &Error{
		Message: fmt.Sprintf(format, args...),
		Code:    code,
		Source:  &srcCtx,
	}
}

// NewError creates an Error.
func NewError(code pgcode.Code, msg string) *Error {
	return NewErrorWithDepthf(1, code, "%s", msg)
}

// NewErrorf creates an Error with a format string.
func NewErrorf(code pgcode.Code, format string, args ...interface{}) *Error {
	return NewErrorWithDepthf(1, code, format, args...)
}

// NewDangerousStatementErrorf creates a new Error for "rejected dangerous statements".
func NewDangerousStatementErrorf(format string, args ...interface{}) *Error {
	var buf bytes.Buffer
	buf.WriteString("rejected: ")
	fmt.Fprintf(&buf, format, args...)
	buf.WriteString(" (sql_safe_updates = true)")
	return NewErrorWithDepthf(1, pgcode.Warning, "%s", buf.String())
}

// NewWrongNumberOfPreparedStatements creates new an Error for trying to prepare
// a query string containing more than one statement.
func NewWrongNumberOfPreparedStatements(n int) *Error {
	return NewErrorWithDepthf(1, pgcode.InvalidPreparedStatementDefinition,
		"prepared statement had %d statements, expected 1", n)
}

// SetHintf annotates an Error object with a hint.
func (pg *Error) SetHintf(f string, args ...interface{}) *Error {
	pg.Hint = fmt.Sprintf(f, args...)
	return pg
}

// SetDetailf annotates an Error object with details.
func (pg *Error) SetDetailf(f string, args ...interface{}) *Error {
	pg.Detail = fmt.Sprintf(f, args...)
	return pg
}

// ResetSource resets the Source field of the Error object
// with the details on the depth-level caller of ResetSource.
func (pg *Error) ResetSource(depth int) {
	srcCtx := makeSrcCtx(depth + 1)
	pg.Source = &srcCtx
}

// makeSrcCtx creates a Error_Source value with contextual information
// about the caller at the requested depth.
func makeSrcCtx(depth int) Error_Source {
	f, l, fun := caller.Lookup(depth + 1)
	return Error_Source{File: f, Line: int32(l), Function: fun}
}

// GetPGCause returns an unwrapped Error.
func GetPGCause(err error) (*Error, bool) {
	switch pgErr := errors.Cause(err).(type) {
	case *Error:
		return pgErr, true

	default:
		return nil, false
	}
}

// UnimplementedWithIssueErrorf constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssueErrorf(issue int, format string, args ...interface{}) *Error {
	err := NewErrorWithDepthf(1, pgcode.FeatureNotSupported, "unimplemented: "+format, args...)
	err.TelemetryKey = fmt.Sprintf("#%d", issue)
	return err
}

// UnimplementedWithIssueError constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssueError(issue int, msg string) *Error {
	err := NewErrorWithDepthf(1, pgcode.FeatureNotSupported, "unimplemented: %s", msg)
	err.TelemetryKey = fmt.Sprintf("#%d", issue)
	return err
}

// UnimplementedWithIssueDetailError constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
func UnimplementedWithIssueDetailError(issue int, detail, msg string) *Error {
	err := NewErrorWithDepthf(1, pgcode.FeatureNotSupported, "unimplemented: %s", msg)
	if detail == "" {
		err.TelemetryKey = fmt.Sprintf("#%d", issue)
	} else {
		err.TelemetryKey = fmt.Sprintf("#%d.%s", issue, detail)
	}

	return err
}

// UnimplementedWithIssueDetailErrorf is like the above
// but supports message formatting.
func UnimplementedWithIssueDetailErrorf(
	issue int, detail, format string, args ...interface{},
) *Error {
	err := NewErrorWithDepthf(1, pgcode.FeatureNotSupported, "unimplemented: "+format, args...)
	if detail == "" {
		err.TelemetryKey = fmt.Sprintf("#%d", issue)
	} else {
		err.TelemetryKey = fmt.Sprintf("#%d.%s", issue, detail)
	}

	return err
}

// UnimplementedWithIssueHintError constructs an error with the given
// message, hint, and a link to the passed issue. Recorded as "#<issue>"
// in tracking.
func UnimplementedWithIssueHintError(issue int, msg, hint string) *Error {
	err := NewErrorWithDepthf(1, pgcode.FeatureNotSupported, "unimplemented: %s", msg)
	err.TelemetryKey = fmt.Sprintf("#%d", issue)
	return err
}

const unimplementedErrorHint = `This feature is not yet implemented in ZNBaseDB.

Please check docs to check whether this feature is already tracked. 
If you cannot find it there, please contact ZNBaseDB technical support.

The ZNBase Labs team appreciates your feedback.
`

// Unimplemented constructs an unimplemented feature error.
//
// `feature` is used for tracking, and is not included when the error printed.
func Unimplemented(feature, msg string, args ...interface{}) *Error {
	return UnimplementedWithDepth(1, feature, msg, args...)
}

// UnimplementedWithDepth constructs an implemented feature error,
// tracking the context at the specified depth.
func UnimplementedWithDepth(depth int, feature, msg string, args ...interface{}) *Error {
	err := NewErrorWithDepthf(depth+1, pgcode.FeatureNotSupported, msg, args...)
	err.TelemetryKey = feature
	err.Hint = unimplementedErrorHint
	return err
}

var _ fmt.Formatter = &Error{}

// Format implements the fmt.Formatter interface.
//
// %v/%s prints the rror as usual.
// %#v adds the pg error code at the beginning.
// %+v prints all the details, including the embedded stack traces.
func (pg *Error) Format(s fmt.State, verb rune) {
	switch {
	case verb == 'v' && s.Flag('+'):
		// %+v prints all details.
		if pg.Source != nil {
			fmt.Fprintf(s, "%s:%d in %s(): ", pg.Source.File, pg.Source.Line, pg.Source.Function)
		}
		fmt.Fprintf(s, "(%s) %s", pg.Code, pg.Message)
		for _, d := range pg.SafeDetail {
			fmt.Fprintf(s, "\n-- detail --\n%s", d.SafeMessage)
			if d.EncodedStackTrace != "" {
				if st, ok := log.DecodeStackTrace(d.EncodedStackTrace); ok {
					fmt.Fprintf(s, "\n%s", log.PrintStackTrace(st))
				}
			}
		}
		return
	case verb == 'v' && s.Flag('#'):
		// %#v spells out the code as prefix.
		fmt.Fprintf(s, "(%s) %s", pg.Code, pg.Message)
	case verb == 'v':
		fallthrough
	case verb == 's':
		fmt.Fprintf(s, "%s", pg.Message)
	case verb == 'q':
		fmt.Fprintf(s, "%q", pg.Message)
	}
}

// Newf creates an Error with a format string.
func Newf(code pgcode.Code, format string, args ...interface{}) error {
	err := errors.NewWithDepthf(1, format, args...)
	err = WithCandidateCode(err, code)
	return err
}

// IsSQLRetryableError returns true if err is retryable. This is true
// for errors that show a connection issue or an issue with the node
// itself. This can occur when a node is restarting or is unstable in
// some other way. Note that retryable errors may occur event in cases
// where the SQL execution ran to completion.
func IsSQLRetryableError(err error) bool {
	// Don't forget to update the corresponding test when making adjustments
	// here.
	errString := FullError(err)
	matched, merr := regexp.MatchString(
		"(no inbound stream connection|connection reset by peer|connection refused|failed to send RPC|rpc error: code = Unavailable|EOF|result is ambiguous)",
		errString)
	if merr != nil {
		return false
	}
	return matched
}
