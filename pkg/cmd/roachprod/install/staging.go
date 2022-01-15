// Copyright 2018  The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package install

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
)

const (
	edgeBinaryServer    = "https://edge-binaries.znbasedb.com"
	releaseBinaryServer = "https://s3.amazonaws.com/binaries.znbasedb.com/"
)

func getEdgeBinaryURL(binaryName string, SHA string) (*url.URL, error) {
	edgeBinaryLocation, err := url.Parse(edgeBinaryServer)
	if err != nil {
		return nil, err
	}
	edgeBinaryLocation.Path = binaryName
	// If a specific SHA is provided, just attach that.
	if len(SHA) > 0 {
		edgeBinaryLocation.Path += "." + SHA
	} else {
		edgeBinaryLocation.Path += ".LATEST"
		// Otherwise, find the latest SHA binary available. This works because
		// "[executable].LATEST" redirects to the latest SHA.
		resp, err := http.Head(edgeBinaryLocation.String())
		if err != nil {
			return nil, err
		}
		edgeBinaryLocation = resp.Request.URL
	}

	return edgeBinaryLocation, nil
}

// StageRemoteBinary downloads a znbase edge binary with the provided
// application path to each specified by the cluster. If no SHA is specified,
// the latest build of the binary is used instead.
func StageRemoteBinary(c *SyncedCluster, applicationName, binaryPath, SHA string) error {
	binURL, err := getEdgeBinaryURL(binaryPath, SHA)
	if err != nil {
		return err
	}
	fmt.Printf("Resolved binary url for %s: %s\n", applicationName, binURL)
	cmdStr := fmt.Sprintf(
		`curl -sfSL -o %s "%s" && chmod 755 ./%s`, applicationName, binURL, applicationName,
	)
	return c.Run(
		os.Stdout, os.Stderr, c.Nodes, fmt.Sprintf("staging binary (%s)", applicationName), cmdStr,
	)
}

// StageZNBaseRelease downloads an official ZNBaseDB release binary with
// the specified version.
func StageZNBaseRelease(c *SyncedCluster, version string) error {
	if len(version) == 0 {
		return fmt.Errorf(
			"release application cannot be staged without specifying a specific version",
		)
	}
	binURL, err := url.Parse(releaseBinaryServer)
	if err != nil {
		return err
	}
	binURL.Path += fmt.Sprintf("znbase-%s.linux-amd64.tgz", version)
	fmt.Printf("Resolved release url for znbase version %s: %s\n", version, binURL)

	// This command incantation:
	// - Creates a temporary directory on the remote machine
	// - Downloads and unpacks the znbase release into the temp directory
	// - Moves the znbase executable from the binary to '/.' and gives it
	// the correct permissions.
	cmdStr := fmt.Sprintf(`
tmpdir="$(mktemp -d /tmp/znbase-release.XXX)" && \
curl -f -s -S -o- %s | tar xfz - -C "${tmpdir}" --strip-components 1 && \
mv ${tmpdir}/znbase ./znbase && \
chmod 755 ./znbase
`, binURL)
	return c.Run(
		os.Stdout, os.Stderr, c.Nodes, "staging znbase release binary", cmdStr,
	)
}
