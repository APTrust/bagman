package dpn_test

import (
//	"bufio"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
//	"io"
//	"net/url"
	"os"
//	"os/user"
	"path/filepath"
//	"strings"
	"testing"
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

	// Hack in our API token. On the local test cluster, APTrust uses
	// the same token for all nodes.
	dpnConfig.RemoteNodeTokens["chron"] = dpnConfig.RestClient.LocalAuthToken
	dpnConfig.RemoteNodeTokens["hathi"] = dpnConfig.RestClient.LocalAuthToken
	dpnConfig.RemoteNodeTokens["sdr"] = dpnConfig.RestClient.LocalAuthToken
	dpnConfig.RemoteNodeTokens["tdr"] = dpnConfig.RestClient.LocalAuthToken

	recorder, err := dpn.NewRecorder(procUtil, dpnConfig)
	if err != nil {
		t.Error(err)
		return nil
	}

	// Point the remote clients toward our own local DPN test cluster.
	// This means you have to run the run_cluster.sh script in the
	// DPN REST project to run these tests.
	for nodeNamespace := range recorder.RemoteClients {
		remoteClient := recorder.RemoteClients[nodeNamespace]
		remoteClient.HostUrl = TEST_NODE_URLS[nodeNamespace]
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
	result := dpn.NewDPNResult("")
	result.DPNBag = dpnBag
	result.TransferRequest = xfer
	return result
}

// This DPNResult has a BagIdentifier and no TransferRequest,
// mimicking a result for a locally-ingested bag.
func buildLocalResult(t *testing.T, recorder *dpn.Recorder) (*dpn.DPNResult) {
	result := dpn.NewDPNResult("test.edu/test.edu.bag6")
	result.DPNBag = makeBag() // defined in dpnrestclient_test.go
	result.DPNBag.LocalId = "test.edu/test.edu.bag6"
	result.StorageURL = fmt.Sprintf("http://fakeurl.kom/%s", result.DPNBag.UUID)
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

	recorder.RunTest(dpnResult)
	if dpnResult.ErrorMessage != "" {
		t.Errorf(dpnResult.ErrorMessage)
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

	recorder.RunTest(dpnResult)
	if dpnResult.ErrorMessage != "" {
		t.Errorf(dpnResult.ErrorMessage)
	}

	// Test a bag that was stored
	dpnResult.StorageURL = "https://www.yahoo.com"

	recorder.RunTest(dpnResult)
	if dpnResult.ErrorMessage != "" {
		t.Errorf(dpnResult.ErrorMessage)
	}
}
