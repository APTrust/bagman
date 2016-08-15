package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"net/url"
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
	procUtil := bagman.NewProcessUtil(&testConfig, "dpn")
	dpnConfig := loadConfig(t, configFile)

	recorder, err := dpn.NewRecorder(procUtil, dpnConfig)
	if err != nil {
		t.Error(err)
		return nil
	}

	// HACK: Some of our tests require us to connect to
	// remote nodes as admin, and the remote node tokens
	// in the dpn_config.json are all admin tokens.
	// The actions that recorder.go takes on remote nodes
	// MUST be completed as a non-admin user. So the following
	// code forces the aptrust_token into all the remote
	// clients so they connect to the remote nodes as a non-admin
	// user. This issue affects the test environment only.
	// In production, we would never connect to a remote node
	// as an admin. We would never even have the credentials
	// to do so.
	for i := range(recorder.RemoteClients) {
		remoteClient := recorder.RemoteClients[i]
		remoteClient.APIKey = recorder.LocalRESTClient.APIKey
	}

	return recorder
}

func buildResultWithTransfer(t *testing.T, recorder *dpn.Recorder) (*dpn.DPNResult) {
	params := url.Values{}
	params.Set("to_node", "aptrust")
	xfers, err := recorder.RemoteClients["hathi"].DPNReplicationListGet(&params)
	if err != nil {
		t.Error(err)
		return nil
	}
	if len(xfers.Results) == 0 {
		t.Errorf("No transfers available from Hathi to APTrust")
		return nil
	}
	xfer := xfers.Results[0]
	bag, err := recorder.RemoteClients["hathi"].DPNBagGet(xfer.BagId)
	if err != nil {
		t.Error(err)
		return nil
	}
	if bag.Fixities == nil || bag.Fixities.Sha256 == "" {
		t.Errorf("Bag %s has no fixity value!", bag.UUID)
		return nil
	}
	result := dpn.NewDPNResult("")
	result.DPNBag = bag
	result.TransferRequest = xfer

	// Need to send this receipt to admin node
	fixityValue := bag.Fixities.Sha256
	result.TransferRequest.FixityValue = &fixityValue

	result.ValidationResult = &dpn.ValidationResult{
		TagManifestChecksum: bag.Fixities.Sha256,
	}
	result.BagMd5Digest = "SomeFakeValue"
	return result
}

// This DPNResult has a BagIdentifier and no TransferRequest,
// mimicking a result for a locally-ingested bag.
func buildLocalResult(t *testing.T, recorder *dpn.Recorder) (*dpn.DPNResult) {
	result := dpn.NewDPNResult("test.edu/test.edu.bag6")
	result.DPNBag = MakeBag() // defined in helper_test.go
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
	// This sucks. We need a better way to create fixtures and do integration tests.
	statusRecords, err := recorder.ProcUtil.FluctusClient.ProcessStatusSearch(ps, true, false)
	if err != nil || len(statusRecords) == 0 {
		t.Errorf("Could not get ProcessedItem ID to test DPN ingest: %v", err)
		return nil
	}
	result.ProcessedItemId = statusRecords[0].Id
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
	if dpnResult == nil {
		t.Errorf("Failed to build test result. Can't run TestReplicatedBag.")
		return
	}
	recorderTestEnsureFiles(t, recorder.ProcUtil)

	// Test a bag that was copied and validated
	// but not stored.
	filePath := filepath.Join(
		recorder.ProcUtil.Config.DPNStagingDirectory,
		dpnResult.DPNBag.UUID + ".tar")
	dpnResult.CopyResult.LocalPath = filePath
	dpnResult.ValidationResult.TarFilePath = filePath

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
