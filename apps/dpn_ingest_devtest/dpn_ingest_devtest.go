package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
	"path/filepath"
)

const NONCE = "12345"

// dpn_package_test runs a single APTrust bag through the DPN packager.
// This is for ad-hoc dev testing.
func main() {
	// The workers below will share one ProcUtil, which means
	// some of ProcUtil's internal variables, like success count
	// and failure count will be updated by each worker.
	procUtil := workers.CreateProcUtil()

	pathToConfigFile := "dpn/dpn_config.json"
	dpnConfig, err := dpn.LoadConfig(pathToConfigFile, "test")
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
		if dpnResult.DPNBag == nil {
			procUtil.MessageLog.Fatal("DPNBag is nil! " +
				"DPNResult should have non-nil DPNBag after storage!")
		}
		verifySizeAndChecksums(dpnResult, procUtil)
	} else {
		fmt.Println("Packager failed. Skipping storage step.")
		fmt.Println(dpnResult.ErrorMessage)
	}

	// The bag that the packager created should still be on disk.
	// Let's validate it. Make sure we have the path to the tar
	// file, so the validator knows where to look.
	dpnResult.LocalPath = dpnResult.PackageResult.TarFilePath
	fmt.Println("Creating validator...")
	validator, err := dpn.NewValidator(procUtil, dpnConfig)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}

	// This will print success or error messages to the console & log.
	// Since this is a local bag, the validator won't try to update
	// a replication request.
	validator.RunTest(dpnResult)

	if dpnResult.ValidationResult == nil {
		procUtil.MessageLog.Error("After validation, bag is missing validation request.")
	} else if dpnResult.ValidationResult.IsValid() == false {
		procUtil.MessageLog.Error("Bag failed validation.")
	}

	// Record the result of the DPN ingest in the local DPN REST
	// service and in Fluctus.
	recorder, err := dpn.NewRecorder(procUtil, dpnConfig)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	recorder.RunTest(dpnResult)

	// Check record result
	if dpnResult.RecordResult == nil {
		procUtil.MessageLog.Error("Record result is nil")
	} else {
		if dpnResult.RecordResult.DPNBagCreatedAt.IsZero() {
			procUtil.MessageLog.Error("DPNBagCreatedAt was not set")
		}
		if len(dpnResult.RecordResult.DPNReplicationRequests) != 2 {
			procUtil.MessageLog.Error("Expected 2 replication requests, found %d",
				len(dpnResult.RecordResult.DPNReplicationRequests))
		}
		if dpnResult.RecordResult.PremisIngestEventId == "" {
			procUtil.MessageLog.Error("PremisIngestEventId was not set")
		}
		if dpnResult.RecordResult.PremisIdentifierEventId == "" {
			procUtil.MessageLog.Error("PremisIdentifierEventId was not set")
		}
		if dpnResult.RecordResult.ErrorMessage != "" {
			procUtil.MessageLog.Error(dpnResult.RecordResult.ErrorMessage)
		}
	}


	dpnResult.ErrorMessage += "  Nothing wrong. Just testing the trouble processor."
	troubleProcessor := dpn.NewTroubleProcessor(procUtil)
	troubleProcessor.RunTest(dpnResult)

	// Make sure the trouble worker wrote its file
	troubleFile := filepath.Join(dpnConfig.LogDirectory, "dpn_trouble", dpnResult.DPNBag.UUID)
	if !bagman.FileExists(troubleFile) {
		procUtil.MessageLog.Error("Trouble worker did not write JSON file to %s", troubleFile)
	} else {
		procUtil.MessageLog.Debug("Trouble worker successfully wrote file to %s", troubleFile)
	}
}

func getClient(config *dpn.DPNConfig, procUtil *bagman.ProcessUtil) (*dpn.DPNRestClient, error) {
	restClient, err := dpn.NewDPNRestClient(
		config.RestClient.LocalServiceURL,
		config.RestClient.LocalAPIRoot,
		config.RestClient.LocalAuthToken,
		config.LocalNode,
		config,
		procUtil.MessageLog)
	if err != nil {
		return nil, err
	}
	return restClient, nil
}

func createXferRequest(client *dpn.DPNRestClient, uuid, checksum string) (*dpn.DPNReplicationTransfer, error) {
	nonce := "12345"
	xfer := &dpn.DPNReplicationTransfer{
		FromNode: "aptrust",
		ToNode: "aptrust",
		UUID: uuid,
		FixityAlgorithm: "sha256",
		FixityNonce: &nonce,
		FixityValue: &checksum,
		Status: "Requested",
		Protocol: "R",
		Link: "rsync://our/sink.tar",
	}
	savedXfer, err := client.ReplicationTransferCreate(xfer)
	if err != nil {
		return nil, err
	}
	return savedXfer, nil
}

func verifySizeAndChecksums(dpnResult *dpn.DPNResult, procUtil *bagman.ProcessUtil) {
	if dpnResult.BagMd5Digest == "" {
		procUtil.MessageLog.Fatal("Result is missing md5 checksum")
	}
	if dpnResult.BagSha256Digest == "" {
		procUtil.MessageLog.Fatal("Result is missing sha256 checksum")
	}
	if dpnResult.BagSize == 0 {
		procUtil.MessageLog.Fatal("Result is missing bag size")
	}
}
