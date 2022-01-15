// Copyright 2017 The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package cliicl

import (
	"github.com/spf13/cobra"
	"github.com/znbasedb/znbase/pkg/cli"
	"github.com/znbasedb/znbase/pkg/icl/baseicl"
	"github.com/znbasedb/znbase/pkg/icl/cliicl/cliflagsicl"
)

// This does not define a `start` command, only modifications to the existing command
// in `pkg/cli/start.go`.

var storeEncryptionSpecs baseicl.StoreEncryptedInfoVector

func init() {
	cli.VarFlag(cli.StartCmd.Flags(), &storeEncryptionSpecs, cliflagsicl.EnterpriseEncryption)

	// Add a new pre-run command to match encryption specs to store specs.
	cli.AddPersistentPreRunE(cli.StartCmd, func(cmd *cobra.Command, _ []string) error {
		return populateStoreSpecsEncryption()
	})
}

// populateStoreSpecsEncryption is a PreRun hook that matches store encryption specs with the
// parsed stores and populates some fields in the StoreSpec.
func populateStoreSpecsEncryption() error {
	return baseicl.PopulateStoreSpecWithEncryption(cli.GetServerCfgStores(), storeEncryptionSpecs)
}
