package main

import (
	"fmt"
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_package_test runs a single APTrust bag through the DPN packager.
// This is for ad-hoc dev testing.
func main() {
	procUtil := workers.CreateProcUtil()
	dpnConfig, err := dpn.LoadConfig("dpn/dpn_config.json")
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	packager := dpn.NewPackager(procUtil, dpnConfig.DefaultMetadata)
	dpnResult := packager.RunTest("test.edu/ncsu.1840.16-1004")
	if dpnResult.ErrorMessage == "" {
		fmt.Println("Packager succeeded. Moving to storage.")
		storer := dpn.NewStorer(procUtil)
		storer.RunTest(dpnResult)
	} else {
		fmt.Println("Packager failed. Skipping storage step.")
		fmt.Println(dpnResult.ErrorMessage)
	}

	dpnResult.ErrorMessage += "  Nothing wrong. Just testing the trouble processor."
	troubleProcessor := dpn.NewTroubleProcessor(procUtil)
	troubleProcessor.RunTest(dpnResult)
}
