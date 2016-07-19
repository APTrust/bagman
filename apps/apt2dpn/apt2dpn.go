package main

import (
	"fmt"
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// This is a quick, one-off program to convert some APTrust
// test bags to DPN test bags.
func main() {
	procUtil := workers.CreateProcUtil("dpn")
	pathToConfigFile := "dpn/dpn_config.json"
	dpnConfig, err := dpn.LoadConfig(pathToConfigFile)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	fmt.Println("Creating packager...")
	packager := dpn.NewPackager(procUtil, dpnConfig)

	bags := []string {
		"test.edu/test.edu.bag1",
		"test.edu/test.edu.bag2",
		"test.edu/test.edu.bag3",
		"test.edu/test.edu.bag4",
		"test.edu/test.edu.bag5",
		"test.edu/test.edu.bag6",
	}
	for _, bag := range bags {
		dpnResult := packager.RunTest(bag)
		if dpnResult.ErrorMessage == "" {
			fmt.Printf("Bag %s is in %s\n", bag, dpnResult.PackageResult.TarFilePath)
			fmt.Println("Packager succeeded. Moving to storage.")
			storer, err := dpn.NewStorer(procUtil, dpnConfig)
			if err != nil {
				procUtil.MessageLog.Fatal(err.Error())
			}
			storer.RunTest(dpnResult)
			if dpnResult.DPNBag == nil {
				procUtil.MessageLog.Fatal("DPNBag is nil! " +
					"DPNResult should have non-nil DPNBag after storage!")
			}
		} else {
			fmt.Printf("ERROR on bag %s: %s\n", bag, dpnResult.ErrorMessage)
		}
	}
}
