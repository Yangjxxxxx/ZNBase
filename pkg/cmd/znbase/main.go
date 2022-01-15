// Copyright 2014  The Cockroach Authors.
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

// This is the default entry point for a ZNBaseDB binary.
//
// The icl hook import below means building this will produce ICL'ed binaries.
// This file itself remains Apache2 to preserve the organization of icl code
// under the /pkg/icl subtree, but is unused for pure FLOSS builds.
package main

import (
	"github.com/znbasedb/znbase/pkg/cli"
	_ "github.com/znbasedb/znbase/pkg/icl"                // icl init hooks
	_ "github.com/znbasedb/znbase/pkg/icl/cliicl"         // icl init hooks
	_ "github.com/znbasedb/znbase/pkg/icl/locatespaceicl" // icl init hooks
	_ "github.com/znbasedb/znbase/pkg/sql/gcjob"          // gcjob init hooks
	_ "github.com/znbasedb/znbase/pkg/ui/disticl"         // icl web UI init hook
)

func main() {
	cli.Main()
}
