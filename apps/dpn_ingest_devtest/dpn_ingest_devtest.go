package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
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
		if dpnResult.DPNBag == nil {
			procUtil.MessageLog.Fatal("DPNBag is nil! " +
				"DPNResult should have non-nil DPNBag after storage!")
			return
		}
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

	// Now let's add a replication request to the result, so the
	// validator tries to update the replication request.
	// Make sure we create the xfer request with the correct
	dpnResult.ValidationResult.CalculateTagManifestDigest(NONCE)
	xferRequest, err := createXferRequest(dpnConfig,
		procUtil, dpnResult.DPNBag.UUID, dpnResult.ValidationResult.TagManifestChecksum)
	if err != nil {
		procUtil.MessageLog.Error(
			"Could not create replication request on local DPN REST node: %v", err)
	} else {
		procUtil.MessageLog.Info("\n\nTrying validation with replication request. \n" +
			"We have a problem here. The REST server does not store the nonced checksum \n" +
			"of the tag manifest. It actually only stores the fixity value of \n" +
			"the bag, and checks against that, which is wrong, according to the \n" +
			"spec. For now, if you see a message below saying \n" +
			"Remote node did not accept the fixity value we sent for this bag.\n" +
			"Then this test is passing. We need to fix our service implementation.\n")

		dpnResult.TransferRequest = xferRequest
		dpnResult.ValidationResult = nil
		validator.RunTest(dpnResult)
	}

	dpnResult.ErrorMessage += "  Nothing wrong. Just testing the trouble processor."
	troubleProcessor := dpn.NewTroubleProcessor(procUtil)
	troubleProcessor.RunTest(dpnResult)
}

func createXferRequest(config *dpn.DPNConfig, procUtil *bagman.ProcessUtil, uuid, checksum string) (*dpn.DPNReplicationTransfer, error) {
	restClient, err := dpn.NewDPNRestClient(
		config.RestClient.LocalServiceURL,
		config.RestClient.LocalAPIRoot,
		config.RestClient.LocalAuthToken,
		procUtil.MessageLog)
	if err != nil {
		return nil, err
	}

	xfer := &dpn.DPNReplicationTransfer{
		FromNode: "aptrust",
		ToNode: "aptrust",
		UUID: uuid,
		FixityAlgorithm: "sha256",
		FixityNonce: "12345",
		FixityValue: checksum,
		Status: "Requested",
		Protocol: "R",
		Link: "rsync://our/sink.tar",
	}
	savedXfer, err := restClient.ReplicationTransferCreate(xfer)
	if err != nil {
		return nil, err
	}
	return savedXfer, nil
}
