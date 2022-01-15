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

// +build lint

package lint

import (
	"bufio"
	"bytes"
	"fmt"
	"go/build"
	"go/parser"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ghemawat/stream"
	"github.com/kisielk/gotool"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/sem/builtins"
	"golang.org/x/tools/go/buildutil"
	"golang.org/x/tools/go/loader"
	"honnef.co/go/tools/lint"
	"honnef.co/go/tools/simple"
	"honnef.co/go/tools/staticcheck"
	"honnef.co/go/tools/unused"
)

const znbaseDB = "github.com/znbasedb/znbase"

func dirCmd(
	dir string, name string, args ...string,
) (*exec.Cmd, *bytes.Buffer, stream.Filter, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stderr := new(bytes.Buffer)
	cmd.Stderr = stderr
	return cmd, stderr, stream.ReadLines(stdout), nil
}

// TestLint runs a suite of linters on the codebase. This file is
// organized into two sections. First are the global linters, which
// run on the entire repo every time. Second are the package-scoped
// linters, which can be restricted to a specific package with the PKG
// makefile variable. Linters that require anything more than a `git
// grep` should preferably be added to the latter group (and within
// that group, adding to Megacheck is better than creating a new
// test).
//
// Linters may be skipped for two reasons: The "short" flag (i.e.
// `make lintshort`), which skips the most expensive linters (more for
// memory than for CPU usage), and the PKG variable. Some linters in
// the global group may be skipped if the PKG flag is set regardless
// of the short flag since they cannot be restricted to the package.
// It should be reasonable to run `make lintshort` and `make lint
// PKG=some/modified/pkg` locally and rely on CI for the more
// expensive linters.
//
// Linters which run in a single process without internal
// parallelization, and which have reasonable memory consumption
// should be marked with t.Parallel(). As a rule of thumb anything
// that requires type-checking the go code needs too much memory to
// parallelize here (although it's fine for such tests to run multiple
// goroutines internally using a shared loader object).
//
// Performance notes: This needs a lot of memory and CPU time. As of
// 2018-07-13, the largest consumers of memory are
// TestMegacheck/staticcheck (9GB) and TestUnused (6GB). Memory
// consumption of staticcheck could be reduced by running it on a
// subset of the packages at a time, although this comes at the
// expense of increased running time.
func TestLint(t *testing.T) {
	znbase, err := build.Import(znbaseDB, "", build.FindOnly)
	if err != nil {
		t.Skip(err)
	}
	pkgDir := filepath.Join(znbase.Dir, "pkg")

	pkgVar, pkgSpecified := os.LookupEnv("PKG")

	t.Run("TestLowercaseFunctionNames", func(t *testing.T) {
		reSkipCasedFunction, err := regexp.Compile(`^(Binary file.*|[^:]+:\d+:(` +
			`query error .*` + // OK when in logic tests
			`|` +
			`\s*(//|#).*` + // OK when mentioned in comment
			`|` +
			`.*lint: uppercase function OK` + // linter annotation at end of line
			`))$`)
		if err != nil {
			t.Fatal(err)
		}

		var names []string
		for _, name := range builtins.AllBuiltinNames {
			switch name {
			case "extract", "trim", "overlay", "position", "substring":
				// Exempt special forms: EXTRACT(... FROM ...), etc.
			default:
				names = append(names, strings.ToUpper(name))
			}
		}

		cmd, stderr, filter, err := dirCmd(znbase.Dir,
			"git", "grep", "-nE", fmt.Sprintf(`[^_a-zA-Z](%s)\(`, strings.Join(names, "|")),
			"--", "pkg")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			if reSkipCasedFunction.MatchString(s) {
				// OK when mentioned in comment or lint disabled.
				return
			}
			if strings.Contains(s, "FAMILY"+"(") {
				t.Errorf("\n%s <- forbidden; use \"FAMILY (\" (with space) or "+
					"lowercase \"family(\" for the built-in function", s)
			} else {
				t.Errorf("\n%s <- forbidden; use lowercase for SQL built-in functions", s)
			}
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestCopyrightHeaders", func(t *testing.T) {
		// skip by gzq
		t.Skip()
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-LE", `^// (Copyright|Code generated by)`, "--", "*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- missing license header`, s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestMissingLeakTest", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "util/leaktest/check-leaktest.sh")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestTabsInShellScripts", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", "^ *\t", "--", "*.sh")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- tab detected, use spaces instead`, s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	// TestTabsInOptgen verifies tabs aren't used in optgen (.opt) files.
	t.Run("TestTabsInOptgen", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", "^ *\t", "--", "*.opt")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- tab detected, use spaces instead`, s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestEnvutil", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			re       string
			excludes []string
		}{
			{re: `\bos\.(Getenv|LookupEnv)\("ZNBase`},
			{
				re: `\bos\.(Getenv|LookupEnv)\(`,
				excludes: []string{
					":!acceptance",
					":!icl/acceptanceicl/backup_test.go",
					":!icl/storageicl/export_storage_test.go",
					":!icl/workloadicl/fixture_test.go",
					":!icl/dump/dump_cloud_test.go",
					":!icl/locatespaceicl/dml_test.go",
					":!cmd",
					":!nightly",
					":!testutils/lint",
					":!util/envutil/env.go",
					":!util/log/clog.go",
					":!util/sdnotify/sdnotify_unix.go",
				},
			},
		} {
			cmd, stderr, filter, err := dirCmd(
				pkgDir,
				"git",
				append([]string{
					"grep",
					"-nE",
					tc.re,
					"--",
					"*.go",
				}, tc.excludes...)...,
			)
			if err != nil {
				t.Fatal(err)
			}

			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}

			if err := stream.ForEach(filter, func(s string) {
				t.Errorf("\n%s <- forbidden; use 'envutil' instead", s)
			}); err != nil {
				t.Error(err)
			}

			if err := cmd.Wait(); err != nil {
				if out := stderr.String(); len(out) > 0 {
					t.Fatalf("err=%s, stderr=%s", err, out)
				}
			}
		}
	})

	t.Run("TestSyncutil", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\bsync\.(RW)?Mutex`,
			"--",
			"*.go",
			":!*/doc.go",
			":!util/syncutil/mutex_sync.go",
			":!util/syncutil/mutex_sync_race.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'syncutil.{,RW}Mutex' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestSQLTelemetryDirectCount", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`[^[:alnum:]]telemetry\.Count\(`,
			"--",
			"sql",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'sqltelemetry.xxxCounter()' / `telemetry.Inc' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestSQLTelemetryGetCounter", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`[^[:alnum:]]telemetry\.GetCounter`,
			"--",
			"sql",
			":!sql/sqltelemetry",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'sqltelemetry.xxxCounter() instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestCBOPanics", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			fmt.Sprintf(`[^[:alnum:]]panic\((%s|"|[a-z]+Error\{errors\.(New|Errorf)|fmt\.Errorf)`, "`"),
			"--",
			"sql/opt",
			":!sql/opt/optgen",
			":!sql/opt/testutils",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use panic(pgerror.NewAssertionErrorf()) instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestInternalErrorCodes", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`[^[:alnum:]]pgerror\.(NewError|Wrap).*pgerror\.CodeInternalError`,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use pgerror.NewAssertionErrorf() instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestTodoStyle", func(t *testing.T) {
		t.Parallel()
		// TODO(tamird): enforce presence of name.
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `\sTODO\([^)]+\)[^:]`, "--", "*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- use 'TODO(...): ' instead`, s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestNonZeroOffsetInTests", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `hlc\.NewClock\([^)]+, 0\)`, "--", "*_test.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf(`%s <- use non-zero clock offset`, s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestTimeutil", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\btime\.(Now|Since|Unix)\(`,
			"--",
			"*.go",
			":!**/embedded.go",
			":!util/timeutil/time.go",
			":!util/timeutil/now_unix.go",
			":!util/timeutil/now_windows.go",
			":!util/tracing/tracer_span.go",
			":!util/tracing/tracer.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'timeutil' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestContext", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\bcontext\.With(Deadline|Timeout)\(`,
			"--",
			"*.go",
			":!util/contextutil/context.go",
			// TODO(jordan): ban these too?
			":!server/debug/**",
			":!workload/**",
			":!*_test.go",
			":!cli/debug_synctest.go",
			":!cmd/**",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'contextutil.RunWithTimeout' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestGrpc", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\bgrpc\.NewServer\(`,
			"--",
			"*.go",
			":!rpc/context_test.go",
			":!rpc/context.go",
			":!rpc/nodedialer/nodedialer_test.go",
			":!util/grpcutil/grpc_util_test.go",
			":!cli/systembench/network_test_server.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'rpc.NewServer' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestPGErrors", func(t *testing.T) {
		t.Parallel()
		// TODO(justin): we should expand the packages this applies to as possible.
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `((fmt|errors).Errorf|errors.New)`, "--", "sql/parser/*.go", "util/ipaddr/*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'pgerror.NewErrorf' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestProtoClone", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\.Clone\([^)]`,
			"--",
			"*.go",
			":!util/protoutil/clone_test.go",
			":!util/protoutil/clone.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`protoutil\.Clone\(`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use 'protoutil.Clone' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestProtoMarshal", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\.Marshal\(`,
			"--",
			"*.go",
			":!util/protoutil/marshal.go",
			":!util/protoutil/marshaler.go",
			":!settings/settings_test.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`(json|yaml|protoutil|xml|\.Field)\.Marshal\(`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use 'protoutil.Marshal' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestProtoUnmarshal", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nE",
			`\.Unmarshal\(`,
			"--",
			"*.go",
			":!*.pb.go",
			":!util/protoutil/marshal.go",
			":!util/protoutil/marshaler.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`(json|jsonpb|yaml|xml|protoutil)\.Unmarshal\(`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use 'protoutil.Unmarshal' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestProtoMessage", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir,
			"git",
			"grep",
			"-nEw",
			`proto\.Message`,
			"--",
			"*.go",
			":!*.pb.go",
			":!*.pb.gw.go",
			":!util/protoutil/jsonpb_marshal.go",
			":!util/protoutil/marshal.go",
			":!util/protoutil/marshaler.go",
			":!util/tracing/tracer_span.go",
			":!sql/pgwire/pgerror/severity.go",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
		), func(s string) {
			t.Errorf("\n%s <- forbidden; use 'protoutil.Message' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestYaml", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `\byaml\.Unmarshal\(`, "--", "*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- forbidden; use 'yaml.UnmarshalStrict' instead", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestImportNames", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-nE", `^(import|\s+)(\w+ )?"database/sql"$`, "--", "*.go")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`gosql "database/sql"`),
		), func(s string) {
			t.Errorf("\n%s <- forbidden; import 'database/sql' as 'gosql' to avoid confusion with 'znbase/sql'", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestMisspell", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			t.Skip("PKG specified")
		}
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "ls-files")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		ignoredRules := []string{
			"licence",
			"analyse", // required by SQL grammar
		}

		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.GrepNot(`.*\.lock`),
			stream.GrepNot(`^storage\/engine\/rocksdb_error_dict\.go$`),
			stream.Map(func(s string) string {
				return filepath.Join(pkgDir, s)
			}),
			stream.Xargs("misspell", "-locale", "US", "-i", strings.Join(ignoredRules, ",")),
		), func(s string) {
			t.Errorf("\n%s", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestGofmtSimplify", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			t.Skip("PKG specified")
		}

		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "ls-files", "*.go", ":!*/testdata/*")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := stream.ForEach(
			stream.Sequence(
				filter,
				stream.Map(func(s string) string {
					return filepath.Join(pkgDir, s)
				}),
				stream.Xargs("gofmt", "-s", "-d", "-l"),
			), func(s string) {
				fmt.Fprintln(&buf, s)
			}); err != nil {
			t.Error(err)
		}
		errs := buf.String()
		if len(errs) > 0 {
			t.Errorf("\n%s", errs)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestCrlfmt", func(t *testing.T) {
		t.Parallel()
		if pkgSpecified {
			t.Skip("PKG specified")
		}
		ignore := `\.(pb(\.gw)?)|(\.[eo]g)\.go|/testdata/|^sql/parser/sql\.go$`
		cmd, stderr, filter, err := dirCmd(pkgDir, "crlfmt", "-fast", "-ignore", ignore, "-tab", "2", ".")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := stream.ForEach(filter, func(s string) {
			fmt.Fprintln(&buf, s)
		}); err != nil {
			t.Error(err)
		}
		errs := buf.String()
		if len(errs) > 0 {
			t.Errorf("\n%s", errs)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}

		if t.Failed() {
			args := append([]string(nil), cmd.Args[1:len(cmd.Args)-1]...)
			args = append(args, "-w", pkgDir)
			for i := range args {
				args[i] = strconv.Quote(args[i])
			}
			t.Logf("run the following to fix your formatting:\n"+
				"\nbin/crlfmt %s\n\n"+
				"Don't forget to add amend the result to the correct commits.",
				strings.Join(args, " "),
			)
		}
	})

	t.Run("TestAuthorTags", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(pkgDir, "git", "grep", "-lE", "^// Author:")
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("\n%s <- please remove the Author comment within", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestUnused", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		if pkgSpecified {
			t.Skip("PKG specified")
		}
		// This test uses 6GB of RAM (as of 2018-07-13), so it should not be parallelized.

		ctx := gotool.DefaultContext
		releaseTags := ctx.BuildContext.ReleaseTags
		lastTag := releaseTags[len(releaseTags)-1]
		dotIdx := strings.IndexByte(lastTag, '.')
		goVersion, err := strconv.Atoi(lastTag[dotIdx+1:])
		if err != nil {
			t.Fatal(err)
		}
		// Detecting unused exported fields/functions requires analyzing
		// the whole program (and all its tests) at once. Therefore, we
		// must load all packages instead of restricting the test to
		// pkgScope (that's why this is separate from Megacheck, even
		// though it is possible to combine them).
		paths := ctx.ImportPaths([]string{znbaseDB + "/pkg/..."})
		conf := loader.Config{
			Build:      &ctx.BuildContext,
			ParserMode: parser.ParseComments,
			ImportPkgs: make(map[string]bool, len(paths)),
		}
		for _, path := range paths {
			conf.ImportPkgs[path] = true
		}
		lprog, err := conf.Load()
		if err != nil {
			t.Fatal(err)
		}

		unusedChecker := unused.NewChecker(unused.CheckAll)
		unusedChecker.WholeProgram = true

		linter := lint.Linter{
			Checker:   unused.NewLintChecker(unusedChecker),
			GoVersion: goVersion,
			Ignores: []lint.Ignore{
				// sql/parser/yaccpar:14:6: type sqlParser is unused (U1000)
				// sql/parser/yaccpar:15:2: func sqlParser.Parse is unused (U1000)
				// sql/parser/yaccpar:16:2: func sqlParser.Lookahead is unused (U1000)
				// sql/parser/yaccpar:29:6: func sqlNewParser is unused (U1000)
				// sql/parser/yaccpar:152:6: func sqlParse is unused (U1000)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/parser/sql.go", Checks: []string{"U1000"}},
				// Generated file containing many unused postgres error codes.
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror/codes.go", Checks: []string{"U1000"}},
				// add by gzq, ingore badgerdb
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/storage/engine/badgerdb.go", Checks: []string{"U1000"}},
				// sql/logictest/logic.go:2416:6: func ProcessTestFile is unused (U1000)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/logictest/logic.go", Checks: []string{"U1000"}},
				// The methods in exprgen.customFuncs are used via reflection.
				// sql/opt/testutils/exprgen/custom_funcs.go:xx:yy: func (*customFuncs).zz is unused (U1000)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/opt/optgen/exprgen/custom_funcs.go", Checks: []string{"U1000"}},
				//some event type in pkg/sql/event_log.go not used(U1000)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/event_log.go", Checks: []string{"U1000"}},
				//pkg/security/audit/event/audit_actions.go type auditAction is unused,func auditAction.action is unused(U1000)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/security/audit/event/audit_actions.go", Checks: []string{"U1000"}},
				//pkg/security/audit/server/audit_server.go func (*AuditServer).WaitUntilReady is unused,func (*AuditServer).ResetRetry is unused (U1000)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/security/audit/server/audit_server.go", Checks: []string{"U1000"}},
				//pkg/security/audit/server/audit_setting.go const DefaultWaitTimeout is unused (U1000)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/security/audit/server/audit_settings.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase/processorsbase.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/sem/types1/types1.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/util/timeutil/pgdate/pgdate.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/rpc/connection_class.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/sqlbase/column_type_properties.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/util/arith/arith.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/col/coldata/batch.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode/codes.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase/base.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase/server_config.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/lex/encode.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/util/interval/generic/internal/contract.go", Checks: []string{"U1000"}},
				//security/audit/task/runner.go func (*Runner).UpdateRetry is unused (U1000)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/security/audit/task/runner.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/opt/optbuilder/union.go", Checks: []string{"U1000"}},

				//暂时忽略
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/delegate/delegate.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/delegate/show_grants.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/delegate/show_schemas.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/delegate/show_sequences.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/delegate/show_tables.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/opt/cat/table.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/plan.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/sessiondata/search_path.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/sqlbase/trigger.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/udr/bepi/depi.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/sqlbase/trigger.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/roachpb/api.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/server/admin.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/roachpb/data_row.go", Checks: []string{"U1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/util/encoding/encoding.go", Checks: []string{"U1000"}},
			},
		}
		for _, p := range linter.Lint(lprog, &conf) {
			t.Errorf("%s: %s", p.Position, &p)
		}
	})

	// TestLogicTestsLint verifies that all the logic test files start with
	// a LogicTest directive.
	t.Run("TestLogicTestsLint", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(
			pkgDir, "git", "ls-files",
			"sql/logictest/testdata/logic_test/",
			"sql/logictest/testdata/planner_test/",
			"sql/opt/exec/execbuilder/testdata/",
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(filename string) {
			file, err := os.Open(filepath.Join(pkgDir, filename))
			if err != nil {
				t.Error(err)
				return
			}
			defer file.Close()
			firstLine, err := bufio.NewReader(file).ReadString('\n')
			if err != nil {
				t.Errorf("reading first line of %s: %s", filename, err)
				return
			}
			if !strings.HasPrefix(firstLine, "# LogicTest:") {
				t.Errorf("%s must start with a directive, e.g. `# LogicTest: default`", filename)
			}
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	pkgScope := pkgVar
	// Things that are packaged scoped are below here.
	if !pkgSpecified {
		pkgScope = "./pkg/..."
	}

	t.Run("TestVet", func(t *testing.T) {
		t.Parallel()
		// `go vet` is a special snowflake that emits all its output on
		// `stderr.
		cmd := exec.Command("go", "vet", "-all", "-printfuncs",
			strings.Join([]string{
				"Info",
				"Infof",
				"InfofDepth",
				"Warning",
				"Warningf",
				"WarningfDepth",
				"Error",
				"Errorf",
				"ErrorfDepth",
				"Fatal",
				"Fatalf",
				"FatalfDepth",
				"Event",
				"Eventf",
				"ErrEvent",
				"ErrEventf",
				"NewError",
				"NewErrorf",
				"VEvent",
				"VEventf",
				"NewAssertionErrorf",
				"UnimplementedWithIssueErrorf",
				"UnimplementedWithIssueDetailErrorf",
				"Wrapf",
			}, ","),
			pkgScope,
		)
		cmd.Dir = znbase.Dir
		var b bytes.Buffer
		cmd.Stdout = &b
		cmd.Stderr = &b
		switch err := cmd.Run(); err.(type) {
		case nil:
		case *exec.ExitError:
			// Non-zero exit is expected.
		default:
			t.Fatal(err)
		}

		if err := stream.ForEach(stream.Sequence(
			stream.FilterFunc(func(arg stream.Arg) error {
				scanner := bufio.NewScanner(&b)
				for scanner.Scan() {
					if s := scanner.Text(); strings.TrimSpace(s) != "" {
						arg.Out <- s
					}
				}
				return scanner.Err()
			}),
			stream.GrepNot(`declaration of "?(pE|e)rr"? shadows`),
			stream.GrepNot(`\.pb\.gw\.go:[0-9]+: declaration of "?ctx"? shadows`),
			stream.GrepNot(`\.[eo]g\.go:[0-9]+: declaration of ".*" shadows`),
			stream.GrepNot(`\.[eo]g\.go:[0-9]+:[0-9]+: self-assignment of`),
			stream.GrepNot(`^#`), // comment line
			// Upstream compiler error. See: https://github.com/golang/go/issues/23701
			stream.GrepNot(`pkg/sql/pgwire/pgwire_test\.go.*internal compiler error`),
			stream.GrepNot(`^Please file a bug report|^https://golang.org/issue/new`),
			stream.GrepNot(`pkg/sql/distsqlrun/vecexec/hash.go:[0-9:]+: possible misuse of unsafe.Pointer`),
		), func(s string) {
			t.Errorf("\n%s", s)
		}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestForbiddenImports", func(t *testing.T) {
		t.Parallel()

		// forbiddenImportPkg -> permittedReplacementPkg
		forbiddenImports := map[string]string{
			"golang.org/x/net/context":         "context",
			"log":                              "util/log",
			"path":                             "path/filepath",
			"github.com/golang/protobuf/proto": "github.com/gogo/protobuf/proto",
			"github.com/satori/go.uuid":        "util/uuid",
			"golang.org/x/sync/singleflight":   "github.com/znbasedb/znbase/pkg/util/syncutil/singleflight",
			"syscall":                          "sysutil",
		}

		// grepBuf creates a grep string that matches any forbidden import pkgs.
		var grepBuf bytes.Buffer
		grepBuf.WriteByte('(')
		for forbiddenPkg := range forbiddenImports {
			grepBuf.WriteByte('|')
			grepBuf.WriteString(regexp.QuoteMeta(forbiddenPkg))
		}
		grepBuf.WriteString(")$")

		filter := stream.FilterFunc(func(arg stream.Arg) error {
			for _, useAllFiles := range []bool{false, true} {
				buildContext := build.Default
				buildContext.CgoEnabled = true
				buildContext.UseAllFiles = useAllFiles
			outer:
				for path := range buildutil.ExpandPatterns(&buildContext, []string{filepath.Join(znbaseDB, pkgScope)}) {
					importPkg, err := buildContext.Import(path, znbase.Dir, 0)
					switch err.(type) {
					case nil:
						for _, s := range importPkg.Imports {
							arg.Out <- importPkg.ImportPath + ": " + s
						}
						for _, s := range importPkg.TestImports {
							arg.Out <- importPkg.ImportPath + ": " + s
						}
						for _, s := range importPkg.XTestImports {
							arg.Out <- importPkg.ImportPath + ": " + s
						}
					case *build.NoGoError:
					case *build.MultiplePackageError:
						if useAllFiles {
							continue outer
						}
					default:
						return errors.Wrapf(err, "error loading package %s", path)
					}
				}
			}
			return nil
		})
		settingsPkgPrefix := `github.com/znbasedb/znbase/pkg/settings`
		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.Sort(),
			stream.Uniq(),
			stream.Grep(`^`+settingsPkgPrefix+`: | `+grepBuf.String()),
			stream.GrepNot(`znbase/pkg/cmd/`),
			stream.GrepNot(`znbase/pkg/testutils/lint: log$`),
			stream.GrepNot(`znbase/pkg/util/sysutil: syscall$`),
			stream.GrepNot(`znbase/pkg/(base|security|util/(log|randutil|stop)): log$`),
			stream.GrepNot(`znbase/pkg/(server/serverpb|ts/tspb): github\.com/golang/protobuf/proto$`),
			stream.GrepNot(`znbase/pkg/server/debug/pprofui: path$`),
			stream.GrepNot(`znbase/pkg/util/caller: path$`),
			stream.GrepNot(`znbase/pkg/icl/workloadicl: path$`),
			stream.GrepNot(`znbase/pkg/icl/storageicl: path$`),
			stream.GrepNot(`znbase/pkg/util/uuid: github\.com/satori/go\.uuid$`),
		), func(s string) {
			pkgStr := strings.Split(s, ": ")
			importingPkg, importedPkg := pkgStr[0], pkgStr[1]

			// Test that a disallowed package is not imported.
			if replPkg, ok := forbiddenImports[importedPkg]; ok {
				t.Errorf("\n%s <- please use %q instead of %q", s, replPkg, importedPkg)
			}

			// Test that the settings package does not import ZNBase dependencies.
			if importingPkg == settingsPkgPrefix && strings.HasPrefix(importedPkg, znbaseDB) {
				switch {
				case strings.HasSuffix(s, "humanizeutil"):
				case strings.HasSuffix(s, "protoutil"):
				case strings.HasSuffix(s, "testutils"):
				case strings.HasSuffix(s, "syncutil"):
				case strings.HasSuffix(s, settingsPkgPrefix):
				default:
					t.Errorf("%s <- please don't add ZNBase dependencies to settings pkg", s)
				}
			}
		}); err != nil {
			t.Error(err)
		}
	})

	// TODO(tamird): replace this with errcheck.NewChecker() when
	// https://github.com/dominikh/go-tools/issues/57 is fixed.
	t.Run("TestErrCheck", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		excludesPath, err := filepath.Abs(filepath.Join("testdata", "errcheck_excludes.txt"))
		if err != nil {
			t.Fatal(err)
		}
		// errcheck uses 2GB of ram (as of 2017-07-13), so don't parallelize it.
		cmd, stderr, filter, err := dirCmd(
			znbase.Dir,
			"errcheck",
			"-exclude",
			excludesPath,
			pkgScope,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("%s <- unchecked error", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestReturnCheck", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		// returncheck uses 2GB of ram (as of 2017-07-13), so don't parallelize it.
		cmd, stderr, filter, err := dirCmd(znbase.Dir, "returncheck", pkgScope)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(filter, func(s string) {
			t.Errorf("%s <- unchecked error", s)
		}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestGolint", func(t *testing.T) {
		t.Parallel()
		cmd, stderr, filter, err := dirCmd(znbase.Dir, "golint", pkgScope)
		if err != nil {
			t.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		if err := stream.ForEach(
			stream.Sequence(
				filter,
				// _fsm.go files are allowed to dot-import the util/fsm package.
				stream.GrepNot("_fsm.go.*should not use dot imports"),
				stream.GrepNot("sql/.*exported func .* returns unexported type sql.planNode"),
				stream.GrepNot("struct field (XXX_NoUnkeyedLiteral|XXX_sizecache) should be"),
				//stream.GrepNot("type name will be used as mail.MailServer by other packages, and that stutters; consider calling this Server"),
				//stream.GrepNot("type name will be used as mail.MailBase by other packages, and that stutters; consider calling this Base"),
				//stream.GrepNot("if block ends with a return statement, so drop this else and outdent its block"),
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}

		if err := cmd.Wait(); err != nil {
			if out := stderr.String(); len(out) > 0 {
				t.Fatalf("err=%s, stderr=%s", err, out)
			}
		}
	})

	t.Run("TestMegacheck", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		// todo gzq, gitlab ci not have enough RAM
		t.Skip("skip by gzq")
		// This test uses 9GB of RAM (as of 2018-07-13), so it should not be parallelized.

		ctx := gotool.DefaultContext
		releaseTags := ctx.BuildContext.ReleaseTags
		lastTag := releaseTags[len(releaseTags)-1]
		dotIdx := strings.IndexByte(lastTag, '.')
		goVersion, err := strconv.Atoi(lastTag[dotIdx+1:])
		if err != nil {
			t.Fatal(err)
		}
		paths := ctx.ImportPaths([]string{filepath.Join(znbaseDB, pkgScope)})
		conf := loader.Config{
			Build:      &ctx.BuildContext,
			ParserMode: parser.ParseComments,
			ImportPkgs: make(map[string]bool, len(paths)),
		}
		for _, path := range paths {
			conf.ImportPkgs[path] = true
		}
		lprog, err := conf.Load()
		if err != nil {
			t.Fatal(err)
		}

		for checker, ignores := range map[lint.Checker][]lint.Ignore{
			&miscChecker{}:  nil,
			&timerChecker{}: nil,
			&hashChecker{}:  nil,
			simple.NewChecker(): {
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/security/securitytest/embedded.go", Checks: []string{"S1013"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/ui/embedded.go", Checks: []string{"S1013"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/security/audit/server/audit_server.go", Checks: []string{"S1000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/security/audit/task/runner.go", Checks: []string{"S1000"}},
			},
			staticcheck.NewChecker(): {
				// The generated parser is full of `case` arms such as:
				//
				// case 1:
				// 	sqlDollar = sqlS[sqlpt-1 : sqlpt+1]
				// 	//line sql.y:781
				// 	{
				// 		sqllex.(*Scanner).stmts = sqlDollar[1].union.stmts()
				// 	}
				//
				// where the code in braces is generated from the grammar action; if
				// the action does not make use of the matched expression, sqlDollar
				// will be assigned but not used. This is expected and intentional.
				//
				// Concretely, the grammar:
				//
				// stmt:
				//   alter_table_stmt
				// | backup_stmt
				// | copy_from_stmt
				// | create_stmt
				// | delete_stmt
				// | drop_stmt
				// | explain_stmt
				// | help_stmt
				// | prepare_stmt
				// | execute_stmt
				// | deallocate_stmt
				// | grant_stmt
				// | insert_stmt
				// | rename_stmt
				// | revoke_stmt
				// | savepoint_stmt
				// | select_stmt
				//   {
				//     $$.val = $1.slct()
				//   }
				// | set_stmt
				// | show_stmt
				// | split_stmt
				// | transaction_stmt
				// | release_stmt
				// | truncate_stmt
				// | update_stmt
				// | /* EMPTY */
				//   {
				//     $$.val = Statement(nil)
				//   }
				//
				// is compiled into the `case` arm:
				//
				// case 28:
				// 	sqlDollar = sqlS[sqlpt-0 : sqlpt+1]
				// 	//line sql.y:830
				// 	{
				// 		sqlVAL.union.val = Statement(nil)
				// 	}
				//
				// which results in the unused warning:
				//
				// sql/parser/yaccpar:362:3: this value of sqlDollar is never used (SA4006)
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/sql/parser/sql.go", Checks: []string{"SA4006"}},
				// Files generated by github.com/grpc-ecosystem/grpc-gateway use a
				// deprecated logging method (SA1019). Ignore such errors until they
				// fix it and we update to using a newer SHA.
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/util/log/clog.go", Checks: []string{"SA1019"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/*/*/*.pb.gw.go", Checks: []string{"SA1019"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/*/*/rocksdb.go", Checks: []string{"SA4000"}},
				&lint.GlobIgnore{Pattern: "github.com/znbasedb/znbase/pkg/*/*/*/rocksdb.go", Checks: []string{"SA4000"}},
			},
		} {
			t.Run(checker.Name(), func(t *testing.T) {
				linter := lint.Linter{
					Checker:   checker,
					Ignores:   ignores,
					GoVersion: goVersion,
				}
				for _, p := range linter.Lint(lprog, &conf) {
					t.Errorf("%s: %s", p.Position, &p)
				}
			})
		}
	})

}
