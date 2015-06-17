package dpn_test

import (
	"bufio"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"io"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

var testConfig string = "test"

// These bags are on the DPN test server,
// and we have data for them in our local DPN REST
// service (they're in the fixture file TestServerData.json).
var TEST_BAGS = []string {
	"005e7793-8253-4585-7118-2da702e29aa0",
	"06edc30f-af04-4a4e-4ad6-3c679a195d46",
	"41e5376c-cc13-4c3e-6af3-297cc2e005aa",
	"472218b3-95ce-4b8e-6c21-6e514cfbe43f",
	"4d11736c-c0ab-44b0-66c6-a947f414d4f1",
	"8839f294-c7c0-404d-4467-7ab223ff461b",
}

var skipCopyMessagePrinted = false
var entryChecked = false
var hasConfigEntry = false

// Test to see if the current user has an ssh config entry for
// dpn-test. We need this to run the copier tests, because
// copier uses rsync over ssh.
func hasSSHConfigEntry() (bool) {
	if entryChecked == true {
		return hasConfigEntry
	}
	entryChecked = true
	usr, _ := user.Current()
	sshConfigFile := filepath.Join(usr.HomeDir, ".ssh", "config")
	f, err := os.Open(sshConfigFile)
	if err != nil {
		return false
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			break
		}
		if strings.HasPrefix(strings.TrimSpace(line), "Host dpn-test") {
			hasConfigEntry = true
		}
	}
	return hasConfigEntry
}

// Return true/false indicating whether we should try to run
// copy tests. Print a message if we can't run the tests.
func canRunCopyTests(t *testing.T) (bool) {
	ok := hasSSHConfigEntry() && runRestTests(t)
	if ok == false && skipCopyMessagePrinted == false {
		skipCopyMessagePrinted = true
		fmt.Println("**** Skipping DPN copy integration tests: "+
			"No dpn-test entry in ~/.ssh/config and/or local " +
			"DPN REST server is not running")
	}
	return ok
}

// For this to work, you need to have an account that
// can access the dpn test server, and you need a config
// entry in ~/.ssh/config with settings to connect to
// the dpn-test server.
func getTestLink(tarredBagName string) (string) {
	return fmt.Sprintf("dpn-test:/home/earthdiver/staging/%s", tarredBagName)
}

// This builds a DPNResult suitable for feeding to the Copier.RunTest()
// method. We get a real bag record and a real transfer record from the
// local DPN REST server, which should be loaded with the fixture data
// from TestServerData.json. We change the rsync link on the transfer
// request to point to the dpn-test server. The copy test will try to
// pull the tarred bag from that server.
func buildTestResult(bagIdentifier string, t *testing.T) (*dpn.DPNResult) {
	// Build a result object with a DPN Bag...
	result := dpn.NewDPNResult(bagIdentifier)

	localRestClient := getClient(t)
	dpnBag, err := localRestClient.DPNBagGet(bagIdentifier)
	if err != nil {
		t.Errorf("Can't get DPN bag '%s' from local DPN REST service: %v",
			bagIdentifier, err)
		return nil
	}
	result.DPNBag = dpnBag

	// Get a transfer request for this bag...
	params := &url.Values{}
	params.Set("uuid", bagIdentifier)
	xferRequests, err := localRestClient.DPNReplicationListGet(params)
	if err != nil {
		t.Errorf("Can't get transfer request for bag '%s' from local DPN REST service: %v",
			bagIdentifier, err)
		return nil
	}
	if len(xferRequests.Results) == 0 {
		t.Errorf("No transfer requests for bag '%s' in local DPN REST service",
			bagIdentifier)
		return nil
	}
	result.TransferRequest = xferRequests.Results[0]

	// Change the rsync link for the bag to point toward
	// our dpn test server.
	tarredBagName := fmt.Sprintf("%s.tar", bagIdentifier)
	result.TransferRequest.Link = getTestLink(tarredBagName)

	return result
}

// Delete rsynched files after testing
func copyTestCleanup() {
	procUtil := bagman.NewProcessUtil(&testConfig)
	for _, uuid := range TEST_BAGS {
		filePath := filepath.Join(procUtil.Config.DPNStagingDirectory, uuid + ".tar")
		os.Remove(filePath)
	}
}

func TestGetRsyncCommand(t *testing.T) {
	procUtil := bagman.NewProcessUtil(&testConfig)
	copyFrom := getTestLink(TEST_BAGS[0])
	copyTo := procUtil.Config.DPNStagingDirectory
	command := dpn.GetRsyncCommand(copyFrom, copyTo)
	if !strings.HasSuffix(command.Path, "rsync") {
		t.Errorf("Expected Path ending in 'rsync', got '%s'", command.Path)
	}
	if len(command.Args) < 6 {
		t.Errorf("rsync command has %d args, expected 5", len(command.Args))
		return
	}
	if command.Args[3] != "ssh" {
		t.Errorf("rsync command should be using ssh, but it's not")
	}
	if command.Args[4] != copyFrom {
		t.Errorf("rsync command is copying from '%s', expected '%s'",
			command.Args[4], copyFrom)
	}
	if command.Args[5] != copyTo {
		t.Errorf("rsync command is copying to '%s', expected '%s'",
			command.Args[5], copyTo)
	}
}

func TestCopier(t *testing.T) {
	if canRunCopyTests(t) == false {
		return
	}
	// runRestTests is defined in dpnrestclient_test.go
	if runRestTests(t) == false {
		return
	}

	procUtil := bagman.NewProcessUtil(&testConfig)
	dpnConfig := loadConfig(t, configFile)
	copier, err := dpn.NewCopier(procUtil, dpnConfig)
	if err != nil {
		t.Error(err)
		return
	}

	// Get ridda that shizzle
	defer copyTestCleanup()

	for _, uuid := range TEST_BAGS {
		dpnResult := buildTestResult(uuid, t)
		if dpnResult == nil {
			return
		}

		// RunTest will update DPNResult.CopyResult
		copier.RunTest(dpnResult)

		if dpnResult.CopyResult.BagWasCopied == false {
			if dpnResult.CopyResult.ErrorMessage != "" {
				t.Errorf("Error copying bag %s: %s",
					uuid, dpnResult.CopyResult.ErrorMessage)
			} else if dpnResult.CopyResult.InfoMessage != "" {
				t.Errorf("Bag %s was not copied: %s",
					uuid, dpnResult.CopyResult.InfoMessage)
			} else {
				// This shouldn't happen
				t.Errorf("Bag %s was not copied. Reason unknown.", uuid)
			}
		}
		if !bagman.FileExists(dpnResult.CopyResult.LocalPath) {
			t.Errorf("Bag %s was reported copied to %s, " +
				"but that file does not exist", uuid,
				dpnResult.CopyResult.LocalPath)
		}
		if dpnResult.DPNBag.Fixities[0].Sha256 != dpnResult.BagSha256Digest {
			t.Errorf("Fixity did not match for bag %s. Expected %s, " +
				"got %s", uuid, dpnResult.DPNBag.Fixities[0].Sha256,
				dpnResult.BagSha256Digest)
		}
		if len(dpnResult.BagMd5Digest) == 0 {
			t.Errorf("Bg MD5 digest is missing.")
		}
		if dpnResult.BagSize == 0 {
			t.Errorf("Bag size is missing")
		}
	}
}
