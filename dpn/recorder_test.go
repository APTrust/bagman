package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Many of the variables and functions used in this file
// come from dpnrestclient_test.go.

// Make sure that the DPNHome and DPNStaging directories exist,
// and that the files that we expect to be present in testing
// are there.
func recorderTestEnsureFiles(t *testing.T, procUtil *bagman.ProcessUtil) {
	dirs := []string {
		procUtil.Config.DPNStagingDirectory,
		filepath.Join(procUtil.Config.DPNHomeDirectory, "dpn.aptrust", "outbound"),
		filepath.Join(procUtil.Config.DPNHomeDirectory, "dpn.chron", "outbound"),
		filepath.Join(procUtil.Config.DPNHomeDirectory, "dpn.hathi", "outbound"),
		filepath.Join(procUtil.Config.DPNHomeDirectory, "dpn.sdr", "outbound"),
		filepath.Join(procUtil.Config.DPNHomeDirectory, "dpn.tdr", "outbound"),
	}
	for _, dir := range dirs {
		if !bagman.FileExists(dir) {
			os.MkdirAll(dir, 0755)
		}
	}
}

func getRecorder(t *testing.T) (*dpn.Recorder) {
	procUtil := bagman.NewProcessUtil(&testConfig)
	dpnConfig := loadConfig(t, configFile)

	recorder, err := dpn.NewRecorder(procUtil, dpnConfig)
	if err != nil {
		t.Error(err)
		return nil
	}
	return recorder
}

func buildResultWithTransfer(t *testing.T, recorder *dpn.Recorder) (*dpn.DPNResult) {
	dpnBag, err := recorder.LocalRESTClient.DPNBagGet(aptrustBagIdentifier)
	if err != nil {
		t.Error(err)
		return nil
	}
	// In our test data fixtures for the local DPN REST cluster,
	// the transfers with id <namespace>-13 to <namespace>-18 are
	// transfers to APTrust. So tdr-18, sdr-18, chron-18, etc. are
	// all bound for APTrust.
	xfer, err := recorder.RemoteClients["hathi"].ReplicationTransferGet("hathi-18")
	if err != nil {
		t.Error(err)
		return nil
	}
	bag, err := recorder.RemoteClients["hathi"].DPNBagGet(xfer.UUID)
	if err != nil {
		t.Error(err)
		return nil
	}
	if len(bag.Fixities) < 1 {
		t.Errorf("Bag %s has no fixity value!", bag.UUID)
		return nil
	}
	result := dpn.NewDPNResult("")
	result.DPNBag = dpnBag
	result.TransferRequest = xfer
	result.BagSha256Digest = bag.Fixities[0].Sha256
	result.BagMd5Digest = "SomeFakeValue"
	return result
}

// This DPNResult has a BagIdentifier and no TransferRequest,
// mimicking a result for a locally-ingested bag.
func buildLocalResult(t *testing.T, recorder *dpn.Recorder) (*dpn.DPNResult) {
	result := dpn.NewDPNResult("test.edu/test.edu.bag6")
	result.DPNBag = makeBag() // defined in dpnrestclient_test.go
	result.DPNBag.LocalId = "test.edu/test.edu.bag6"
	result.StorageURL = fmt.Sprintf("http://fakeurl.kom/%s", result.DPNBag.UUID)

	ps := &bagman.ProcessStatus{
		ObjectIdentifier: "test.edu/test.edu.bag6",
		Name: "Test Bag Six",
		Bucket: "bukkety-poo",
		ETag: "12345678",
		BagDate: time.Now(),
		Institution: "test.edu",
		Action: "DPN",
		Stage: "Requested",
		Status: "Pending",
		Note: "Requested...",
		Outcome: "Requested...",
		Retry: true,
	}
	err := recorder.ProcUtil.FluctusClient.UpdateProcessedItem(ps)
	if err != nil {
		t.Errorf("Could not create Fluctus ProcessedItem to test DPN ingest: %v", err)
		return nil
	}
	result.FluctusProcessStatus = ps
	return result
}


func TestLocalBag(t *testing.T) {
	if runRestTests(t) == false {
		// Local DPN REST not running.
		return
	}
	if canRunTests() == false {
		// Local Fluctus not running.
		return
	}
	recorder := getRecorder(t)
	dpnResult := buildLocalResult(t, recorder)
	if dpnResult == nil {
		t.Errorf("Cannot perform TestLocalBag due to previous errors")
		return
	}
	recorderTestEnsureFiles(t, recorder.ProcUtil)

	// Make a dummy file so the symlink operation in
	// recorder.go doesn't fail.
	filePath := filepath.Join(
		recorder.ProcUtil.Config.DPNStagingDirectory,
		dpnResult.DPNBag.UUID + ".tar")
	_, err := os.Create(filePath)
	if err != nil {
		t.Errorf("Error creating empty file %s: %v",
			filePath, err)
		return
	}
	defer os.Remove(filePath)

	// Run the test
	recorder.RunTest(dpnResult)
	if dpnResult.ErrorMessage != "" {
		t.Errorf(dpnResult.ErrorMessage)
	}

	if dpnResult.RecordResult.DPNBagCreatedAt.IsZero() {
		t.Errorf("DPNBagCreatedAt was not set")
	}
	// Note that this test will fail if the DPN REST service
	// is configured so that APTrust replicates to fewer than
	// two nodes. This shouldn't happen, since the whole point
	// of DPN is to replicate to at least two other nodes. But
	// if you get a failure here, check the ReplicateTo property
	// of the node entry for APTrust on the local REST service.
	if len(dpnResult.RecordResult.DPNReplicationRequests) < recorder.DPNConfig.ReplicateToNumNodes {
		t.Errorf("Replication requests generated for %d nodes, expected %d",
			dpnResult.RecordResult.DPNReplicationRequests,  recorder.DPNConfig.ReplicateToNumNodes)
	}
	if dpnResult.RecordResult.PremisIngestEventId == "" {
		t.Errorf("PremisIngestEventId was not set")
	}
	if dpnResult.RecordResult.PremisIdentifierEventId == "" {
		t.Errorf("PremisIdentifierEventId was not set")
	}
}

func TestReplicatedBag(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	recorder := getRecorder(t)
	dpnResult := buildResultWithTransfer(t, recorder)
	recorderTestEnsureFiles(t, recorder.ProcUtil)

	// Test a bag that was copied and validated
	// but not stored.
	filePath := filepath.Join(
		recorder.ProcUtil.Config.DPNStagingDirectory,
		dpnResult.DPNBag.UUID + ".tar")
	dpnResult.CopyResult.LocalPath = filePath
	dpnResult.ValidationResult = &dpn.ValidationResult{
		TarFilePath: filePath,
	}

	// Run the test...
	recorder.RunTest(dpnResult)
	if dpnResult.ErrorMessage != "" {
		t.Errorf(dpnResult.ErrorMessage)
	}

	// Make sure RecordResult items were set correctly.
	if dpnResult.RecordResult.CopyReceiptSentAt.IsZero() {
		t.Errorf("CopyReceiptSentAt was not set")
	}
	if !dpnResult.RecordResult.StorageResultSentAt.IsZero() {
		t.Errorf("StorageResultSentAt was set when it should not have been")
	}


	// Test a bag that was stored
	dpnResult.StorageURL = "https://www.yahoo.com"

	// Run the test again
	recorder.RunTest(dpnResult)
	if dpnResult.ErrorMessage != "" {
		t.Errorf(dpnResult.ErrorMessage)
	}

	// Check status...
	if dpnResult.RecordResult.StorageResultSentAt.IsZero() {
		t.Errorf("StorageResultSentAt was not set")
	}
}
