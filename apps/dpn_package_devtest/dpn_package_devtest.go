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
	defaultMetadata, err := dpn.LoadConfig("dpn/bagbuilder_config.json")
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	packager := dpn.NewPackager(procUtil, defaultMetadata)
	packageResult, storageResult := packager.RunTest("test.edu/ncsu.1840.16-1004")
	if packageResult.Succeeded() {
		fmt.Println("Packager succeeded. Moving to storage.")
		storer := dpn.NewStorer(procUtil)
		storer.RunTest(storageResult)
	} else {
		fmt.Println("Packager failed. Skipping storage step.")
	}
}
