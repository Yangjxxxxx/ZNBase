// Copyright 2017 The Cockroach Authors.
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

package errorutil

import (
	"context"
	"fmt"
	"reflect"
	"runtime"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/errors/errbase"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// UnexpectedWithIssueErr indicates an error with an associated Github issue.
// It's supposed to be used for conditions that would otherwise be checked by
// assertions, except that they fail and we need the public's help for tracking
// it down.
// The error message will invite users to report repros.
//
// Modeled after pgerror.Unimplemented.
type UnexpectedWithIssueErr struct {
	issue   int
	msg     string
	safeMsg string
}

// UnexpectedWithIssueErrorf constructs an UnexpectedWithIssueError with the
// provided issue and formatted message.
func UnexpectedWithIssueErrorf(issue int, format string, args ...interface{}) error {
	return UnexpectedWithIssueErr{
		issue:   issue,
		msg:     fmt.Sprintf(format, args...),
		safeMsg: log.ReportablesToSafeError(1 /* depth */, format, args).Error(),
	}
}

// Error implements the error interface.
func (e UnexpectedWithIssueErr) Error() string {
	return fmt.Sprintf("unexpected error: %s\nWe've been trying to track this particular issue. "+
		"(in this case you might want to update znbasedb to a newer version).",
		e.msg)
}

// SafeMessage implements the SafeMessager interface.
func (e UnexpectedWithIssueErr) SafeMessage() string {
	return fmt.Sprintf("issue #%d: %s", e.issue, e.safeMsg)
}

// SendReport creates a Sentry report about the error, if the settings allow.
// The format string will be reproduced ad litteram in the report; the arguments
// will be sanitized.
func (e UnexpectedWithIssueErr) SendReport(ctx context.Context, sv *settings.Values) {
	log.SendCrashReport(ctx, sv, 1 /* depth */, "%s", []interface{}{e}, log.ReportTypeError)
}

// As finds the first error in err's chain that matches the type to which target
// points, and if so, sets the target to its value and returns true. An error
// matches a type if it is assignable to the target type, or if it has a method
// As(interface{}) bool such that As(target) returns true. As will panic if target
// is not a non-nil pointer to a type which implements error or is of interface type.
//
// The As method should set the target to its value and return true if err
// matches the type to which target points.
//
// Note: this implementation differs from that of xerrors as follows:
// - it also supports recursing through causes with Cause().
// - if it detects an API use error, its panic object is a valid error.
func As(err error, target interface{}) bool {
	if target == nil {
		panic(pgerror.Wrap(err, "", "errors.As: target cannot be nil"))
	}

	// We use introspection for now, of course when/if Go gets generics
	// all this can go away.
	val := reflect.ValueOf(target)
	typ := val.Type()
	if typ.Kind() != reflect.Ptr || val.IsNil() {
		panic(pgerror.Wrap(err, "", "errors.As: target must be a non-nil pointer"))
	}
	if e := typ.Elem(); e.Kind() != reflect.Interface && !e.Implements(errorType) {
		panic(pgerror.Wrap(err, "", "errors.As: *target must be interface or implement error"))
	}

	targetType := typ.Elem()
	for c := err; c != nil; c = errbase.UnwrapOnce(c) {
		if reflect.TypeOf(c).AssignableTo(targetType) {
			val.Elem().Set(reflect.ValueOf(c))
			return true
		}
		if x, ok := c.(interface{ As(interface{}) bool }); ok && x.As(target) {
			return true
		}
	}
	return false
}

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// ShouldCatch is used for catching errors thrown as panics. Its argument is the
// object returned by recover(); it succeeds if the object is an error. If the
// error is a runtime.Error, it is converted to an internal error (see
// errors.AssertionFailedf).
func ShouldCatch(obj interface{}) (ok bool, err error) {
	err, ok = obj.(error)
	if ok {
		if HasInterface(err, (*runtime.Error)(nil)) {
			// Convert runtime errors to internal errors, which display the stack and
			// get reported to Sentry.
			err = errors.HandleAsAssertionFailure(err)
		}
	}
	return ok, err
}

// HasInterface returns true if err contains an error which implements the
// interface pointed to by referenceInterface. The type of referenceInterface
// must be a pointer to an interface type. If referenceInterface is not a
// pointer to an interface, this function will panic.
func HasInterface(err error, referenceInterface interface{}) bool {
	return markersHasInterface(err, referenceInterface)
}

func markersHasInterface(err error, referenceInterface interface{}) bool {
	iface := getInterfaceType("HasInterface", referenceInterface)
	_, isType := If(err, func(err error) (interface{}, bool) {
		return nil, reflect.TypeOf(err).Implements(iface)
	})
	return isType
}

func getInterfaceType(context string, referenceInterface interface{}) reflect.Type {
	typ := reflect.TypeOf(referenceInterface)
	if typ == nil || typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Interface {
		panic(fmt.Errorf("errors.%s: referenceInterface must be a pointer to an interface, "+
			"found %T", context, referenceInterface))
	}
	return typ.Elem()
}

// If iterates on the error's causal chain and returns a predicate's
// return value the first time the predicate returns true.
//
// Note: if any of the error types has been migrated from a previous
// package location or a different type, ensure that
// RegisterTypeMigration() was called prior to If().
func If(err error, pred func(err error) (interface{}, bool)) (interface{}, bool) {
	for c := err; c != nil; c = errbase.UnwrapOnce(c) {
		if v, ok := pred(c); ok {
			return v, ok
		}
	}
	return nil, false
}
