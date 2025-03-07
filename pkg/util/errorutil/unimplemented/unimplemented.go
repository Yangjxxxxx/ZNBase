package unimplemented

import (
	"fmt"

	"github.com/znbasedb/errors"
)

// This file re-implements the unimplemented primitives from the
// original pgerror package, using the primitives from the errors
// library instead.

// New constructs an unimplemented feature error.
func New(feature, msg string) error {
	return unimplementedInternal(1 /*depth*/, 0 /*issue*/, feature /*detail*/, false /*format*/, msg)
}

// Newf constructs an unimplemented feature error.
// The message is formatted.
func Newf(feature, format string, args ...interface{}) error {
	return NewWithDepthf(1, feature, format, args...)
}

// NewWithDepthf constructs an implemented feature error,
// tracking the context at the specified depth.
func NewWithDepthf(depth int, feature, format string, args ...interface{}) error {
	return unimplementedInternal(depth+1 /*depth*/, 0 /*issue*/, feature /*detail*/, true /*format*/, format, args...)
}

// NewWithIssue constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func NewWithIssue(issue int, msg string) error {
	return unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, false /*format*/, msg)
}

// NewWithIssuef constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func NewWithIssuef(issue int, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, true /*format*/, format, args...)
}

// NewWithIssueHint constructs an error with the given
// message, hint, and a link to the passed issue. Recorded as "#<issue>"
// in tracking.
func NewWithIssueHint(issue int, msg, hint string) error {
	err := unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, false /*format*/, msg)
	err = errors.WithHint(err, hint)
	return err
}

// NewWithIssueDetail constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
func NewWithIssueDetail(issue int, detail, msg string) error {
	return unimplementedInternal(1 /*depth*/, issue, detail, false /*format*/, msg)
}

// NewWithIssueDetailf is like NewWithIssueDetail but the message is formatted.
func NewWithIssueDetailf(issue int, detail, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, issue, detail, true /*format*/, format, args...)
}

func unimplementedInternal(
	depth, issue int, detail string, format bool, msg string, args ...interface{},
) error {
	// Create the issue link.
	link := errors.IssueLink{Detail: detail}
	// 强制设置issue和detail, issue号目前尚未支持
	issue = 0
	detail = ""
	format = false

	if issue > 0 {
		link.IssueURL = MakeURL(issue)
	}

	// Instantiate the base error.
	var err error
	if format {
		err = errors.UnimplementedErrorf(link, "unimplemented: "+msg, args...)
		err = errors.WithSafeDetails(err, msg, args...)
	} else {
		err = errors.New("unimplemented: " + msg)
	}
	// Decorate with a stack trace.
	err = errors.WithStackDepth(err, 1+depth)

	if issue > 0 {
		// There is an issue number. Decorate with a telemetry annotation.
		var key string
		if detail == "" {
			key = fmt.Sprintf("#%d", issue)
		} else {
			key = fmt.Sprintf("#%d.%s", issue, detail)
		}
		err = errors.WithTelemetry(err, key)
	} else if detail != "" {
		// No issue but a detail string. It's an indication to also
		// perform telemetry.
		err = errors.WithTelemetry(err, detail)
	}
	return err
}

// MakeURL produces a URL to a CockroachDB issue.
func MakeURL(issue int) string {
	return fmt.Sprintf("https://github.com/znbasedb/znbase/issues/%d", issue)
}
