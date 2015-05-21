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

	// Clear these values out. The validator should set them too.
	// They have to be set before the bag goes into the storage
	// queue. For bags we build here, the packager sets these values.
	// For bags built elsewhere, the validator sets them.
	dpnResult.BagMd5Digest = ""
	dpnResult.BagSha256Digest = ""
	dpnResult.BagSize = 0

	// This will print success or error messages to the console & log.
	// Since this is a local bag, the validator won't try to update
	// a replication request.
	validator.RunTest(dpnResult)

	// Make sure validator set these properties.
	verifySizeAndChecksums(dpnResult, procUtil)

	// Now let's add a replication request to the result, so the
	// validator tries to update the replication request.
	// Make sure we create the xfer request with the correct
	client, err := getClient(dpnConfig, procUtil)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	xferRequest, err := createXferRequest(client,
		dpnResult.DPNBag.UUID,
		dpnResult.BagSha256Digest)
	if err != nil {
		procUtil.MessageLog.Error(
			"Could not create replication request on local DPN REST node: %v", err)
	} else {
		dpnResult.TransferRequest = xferRequest
		dpnResult.ValidationResult = nil
		validator.RunTest(dpnResult)
	}

	xferRequest, err = client.ReplicationTransferGet(dpnResult.TransferRequest.ReplicationId)
	if err != nil {
		procUtil.MessageLog.Error("Couldn't get replication record %s " +
			"from DPN REST service: %v", dpnResult.TransferRequest.ReplicationId, err)
	}

	// This xfer request should be marked as Confirmed.
	// Our code sets the status to "Stored", and if the
	// remote REST service accepted the fixity check, it
	// will change that "Stored" to "Confirmed"
	if xferRequest.Status != "Confirmed" {
		procUtil.MessageLog.Error("Replication request on DPN server has status '%s'. " +
			"It should be 'Confirmed'", xferRequest.Status)
	}

	dpnResult.ErrorMessage += "  Nothing wrong. Just testing the trouble processor."
	troubleProcessor := dpn.NewTroubleProcessor(procUtil)
	troubleProcessor.RunTest(dpnResult)
}

func getClient(config *dpn.DPNConfig, procUtil *bagman.ProcessUtil) (*dpn.DPNRestClient, error) {
	restClient, err := dpn.NewDPNRestClient(
		config.RestClient.LocalServiceURL,
		config.RestClient.LocalAPIRoot,
		config.RestClient.LocalAuthToken,
		procUtil.MessageLog)
	if err != nil {
		return nil, err
	}
	return restClient, nil
}

func createXferRequest(client *dpn.DPNRestClient, uuid, checksum string) (*dpn.DPNReplicationTransfer, error) {
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
