// Copyright 2019  The Cockroach Authors.

package execerror

import (
	"bufio"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
)

const panicLineSubstring = "runtime/panic.go"

// CatchVecRuntimeError 执行操作，捕获运行时错误
// 它来自矢量化的引擎，并返回它。如果错误不是
// 与向量化引擎有关的事件发生时，它没有被恢复过来。
func CatchVecRuntimeError(operation func()) (retErr error) {
	defer func() {
		if err := recover(); err != nil {
			stackTrace := string(debug.Stack())
			scanner := bufio.NewScanner(strings.NewReader(stackTrace))
			panicLineFound := false
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), panicLineSubstring) {
					panicLineFound = true
					break
				}
			}
			if !panicLineFound {
				panic(fmt.Sprintf("panic line %q not found in the stack trace\n%s", panicLineSubstring, stackTrace))
			}
			if scanner.Scan() {
				panicEmittedFrom := strings.TrimSpace(scanner.Text())
				if isPanicFromVecSystem(panicEmittedFrom) {
					// We only want to catch runtime errors coming from the vectorized
					// engine.
					if e, ok := err.(error); ok {
						if _, ok := err.(*HardwareError); ok {
							// A HardwareError was caused by something below SQL, and represents
							// an error that we'd simply like to propagate along.
							// Do nothing.
						} else {
							doNotAnnotate := false
							if nvie, ok := e.(*notVectorizedInternalError); ok {
								// A notVectorizedInternalError was not caused by the
								// vectorized engine and represents an error that we don't
								// want to annotate in case it doesn't have a valid PG code.
								doNotAnnotate = true
								// We want to unwrap notVectorizedInternalError so that in case
								// the original error does have a valid PG code, the code is
								// correctly propagated.
								e = nvie.error
							}
							if code := pgerror.GetPGCode(e); !doNotAnnotate && code == pgcode.Uncategorized {
								// Any error without a code already is "surprising" and
								// needs to be annotated to indicate that it was
								// unexpected.
								e = errors.AssertionFailedf("unexpected error from the vectorized runtime: %+v", e)
							}
						}
						retErr = e
					} else {
						// Not an error object. Definitely unexpected.
						surprisingObject := err
						retErr = errors.AssertionFailedf("unexpected error from the vectorized runtime: %+v", surprisingObject)
					}
				} else {
					// Do not recover from the panic not related to the vectorized
					// engine.
					panic(err)
				}
			} else {
				panic(fmt.Sprintf("unexpectedly there is no line below the panic line in the stack trace\n%s", stackTrace))
			}
		}
		// No panic happened, so the operation must have been executed
		// successfully.
	}()
	operation()
	return retErr
}

const (
	vecPackagePrefix          = "github.com/znbasedb/znbase/pkg/col"
	vecExecPackagePrefix      = "github.com/znbasedb/znbase/pkg/sql/distsqlrun/vecexec"
	vecFlowsetupPackagePrefix = "github.com/znbasedb/znbase/pkg/sql/flowinfra/vecflow"
	rowexecPackagePrefix      = "github.com/znbasedb/znbase/pkg/sql/distsqlrun/rowexec"
	treePackagePrefix         = "github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// isPanicFromVecSystem 检查是否发出了恐慌
//  panicEmittedFrom代码行(其中包括包名和 文件名和行号)来自矢量化的引擎。
// panicEmittedFrom必须被修剪成前缀中没有任何空格。
func isPanicFromVecSystem(panicEmittedFrom string) bool {
	const testExceptionPrefix = "github.com/znbasedb/znbase/pkg/sql/flowinfra/vecflow_test.(*testNonVectorizedPanicEmitter)"
	if strings.HasPrefix(panicEmittedFrom, testExceptionPrefix) {
		//虽然似乎来自矢量化的引擎，但它testNonVectorizedPanicEmitter
		//是为了测试恐慌传播而故意不被捕捉的，所以
		//我们说恐慌不是来自矢量化的引擎。
		return false
	}
	return strings.HasPrefix(panicEmittedFrom, vecPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, vecExecPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, vecFlowsetupPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, rowexecPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, treePackagePrefix)
}

// HardwareError 是由sql下面的组件创建的错误吗
// 堆栈，例如网络或存储层。存储错误将冒泡
// 一直向上，直到SQL层不变。
type HardwareError struct {
	error
}

// Cause implements the Causer interface.
func (s *HardwareError) Cause() error {
	return s.error
}

// NewStorageError returns a new storage error. This can be used to propagate
// an error through the exec subsystem unchanged.
func NewStorageError(err error) *HardwareError {
	return &HardwareError{error: err}
}

// notVectorizedInternalError is an error that originated outside of the
// vectorized engine (for example, it was caused by a non-columnar builtin).
// notVectorizedInternalError will be returned to the client not as an
// "internal error" and without the stack trace.
type notVectorizedInternalError struct {
	error
}

func newNotVectorizedInternalError(err error) *notVectorizedInternalError {
	return &notVectorizedInternalError{error: err}
}

// VectorizedInternalPanic simply panics with the provided object. It will
// always be returned as internal error to the client with the corresponding
// stack trace. This method should be called to propagate all *unexpected*
// errors that originated within the vectorized engine.
func VectorizedInternalPanic(err interface{}) {
	panic(err)
}

// VectorizedExpectedInternalPanic is the same as NonVectorizedPanic. It should
// be called to propagate all *expected* errors that originated within the
// vectorized engine.
func VectorizedExpectedInternalPanic(err error) {
	NonVectorizedPanic(err)
}

// NonVectorizedPanic panics with the error that is wrapped by
// notVectorizedInternalError which will not be treated as internal error and
// will not have a printed out stack trace. This method should be called to
// propagate all errors that originated outside of the vectorized engine and
// all expected errors from the vectorized engine.
func NonVectorizedPanic(err error) {
	panic(newNotVectorizedInternalError(err))
}
