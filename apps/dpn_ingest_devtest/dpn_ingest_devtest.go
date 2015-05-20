package main

import (
	"fmt"
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_package_test runs a single APTrust bag through the DPN packager.
// This is for ad-hoc dev testing.
func main() {
	pathToConfigFile := "dpn/dpn_config.json"
	procUtil := workers.CreateProcUtil()
	dpnConfig, err := dpn.LoadConfig(pathToConfigFile)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	fmt.Println("Creating packager...")
	packager := dpn.NewPackager(procUtil, dpnConfig)
	dpnResult := packager.RunTest("test.edu/ncsu.1840.16-1004")
	if dpnResult.ErrorMessage == "" {
		fmt.Println("Packager succeeded. Moving to storage.")
		storer, err := dpn.NewStorer(procUtil, dpnConfig)
		if err != nil {
			procUtil.MessageLog.Fatal(err.Error())
		}
		storer.RunTest(dpnResult)
	} else {
		fmt.Println("Packager failed. Skipping storage step.")
		fmt.Println(dpnResult.ErrorMessage)
	}

	// The bag that the packager created should still be on disk.
	// Let's validate it.
	// fmt.Println("Creating validator...")
	// validator, err := dpn.NewValidator(procUtil, dpnConfig)
	// if err != nil {
	// 	procUtil.MessageLog.Fatal(err.Error())
	// }
	// // This will print success or error messages to the console & log.
	// validator.RunTest(dpnResult)

	dpnResult.ErrorMessage += "  Nothing wrong. Just testing the trouble processor."
	troubleProcessor := dpn.NewTroubleProcessor(procUtil)
	troubleProcessor.RunTest(dpnResult)
}
