package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
//	"github.com/nu7hatch/gouuid"
	"net/http"
//	"os"
//	"path/filepath"
//	"strings"
	"testing"
	"time"
)

/*
This file contains integration that rely on a locally-running instance
of the DPN REST service. The tests will not run if runRestTests()
determines that the DPN REST server is unreachable.

The DPN-REST respository includes a file at data/integration_test_data.json
that contains the test data we're expecting to find in these tests.

See the data/README.md file in that repo for information about how to
load that test data into your DPN instance.
*/

var configFile = "dpn/dpn_config.json"
var skipRestMessagePrinted = false
var aptrustBagIdentifier = "9998e960-fc6d-44f4-9d73-9a60a8eae609"
var chronBagIdentifier = "87f2d0ac-94c8-4bb2-90e8-24c076cef9f0"

func runRestTests(t *testing.T) bool {
	config := loadConfig(t, configFile)
	_, err := http.Get(config.RestClient.LocalServiceURL)
	if err != nil {
		if skipRestMessagePrinted == false {
			skipRestMessagePrinted = true
			fmt.Printf("Skipping DPN REST integration tests: "+
				"DPN REST server is not running at %s\n",
				config.RestClient.LocalServiceURL)
		}
		return false
	}
	return true
}

func getClient(t *testing.T) (*dpn.DPNRestClient) {
	// If you want to debug, change ioutil.Discard to os.Stdout
	// to see log output from the client.
	config := loadConfig(t, configFile)
	logger := bagman.DiscardLogger("dpn_rest_client_test")
	client, err := dpn.NewDPNRestClient(
		config.RestClient.LocalServiceURL,
		config.RestClient.LocalAPIRoot,
		config.RestClient.LocalAuthToken,
		logger)
	if err != nil {
		t.Errorf("Error constructing DPN REST client: %v", err)
	}
	return client
}

func TestBuildUrl(t *testing.T) {
	config := loadConfig(t, configFile)
	client := getClient(t)
	relativeUrl := "/api-v1/popeye/olive/oyl/"
	expectedUrl := config.RestClient.LocalServiceURL + relativeUrl
	if client.BuildUrl(relativeUrl) != expectedUrl {
		t.Errorf("BuildUrl returned '%s', expected '%s'",
			client.BuildUrl(relativeUrl), expectedUrl)
	}
}

func TestDPNBagGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	dpnBag, err := client.DPNBagGet(aptrustBagIdentifier)
	if err != nil {
		t.Error(err)
		return
	}
	if dpnBag.UUID != aptrustBagIdentifier {
		t.Errorf("UUID: expected '%s', got '%s'", aptrustBagIdentifier, dpnBag.UUID)
	}
	if dpnBag.LocalId != "aptrust-12345" {
		t.Errorf("LocalId: expected 'aptrust-12345', got '%s'", dpnBag.LocalId)
	}
	if dpnBag.Size != 2526492640 {
		t.Errorf("Size: expected 2526492640, got %d", dpnBag.Size)
	}
	if dpnBag.FirstVersionUUID != aptrustBagIdentifier {
		t.Errorf("FirstVersionUUID: expected '%s', got '%s'",
			aptrustBagIdentifier, dpnBag.FirstVersionUUID)
	}
	if dpnBag.BagType != "D" {
		t.Errorf("BagType: expected 'D', got '%s'", dpnBag.BagType)
	}
	if dpnBag.Version != 1 {
		t.Errorf("Version: expected 1, got %d", dpnBag.Version)
	}
	if dpnBag.IngestNode != "aptrust" {
		t.Errorf("IngestNode: expected 'aptrust', got '%s'", dpnBag.IngestNode)
	}
	if dpnBag.AdminNode != "aptrust" {
		t.Errorf("AdminNode: expected 'aptrust', got '%s'", dpnBag.AdminNode)
	}
	if dpnBag.CreatedAt.Format(time.RFC3339) != "2015-02-25T16:24:02Z" {
		t.Errorf("CreatedAt: expected '2015-02-25T16:24:02.475138Z', got '%s'",
			dpnBag.CreatedAt.Format(time.RFC3339))
	}
	if dpnBag.UpdatedAt.Format(time.RFC3339) != "2015-02-25T16:24:02Z" {
		t.Errorf("UpdatedAt: expected '2015-02-25T16:24:02.475138Z', got '%s'",
			dpnBag.UpdatedAt.Format(time.RFC3339))
	}
	if len(dpnBag.Rights) != 1 {
		t.Errorf("Rights: expected 1 item, got %d", len(dpnBag.Rights))
	}
	if dpnBag.Rights[0] != "ff297922-a5b2-4b66-9475-3ce98b074d37" {
		t.Errorf("Rights[0]: expected 'ff297922-a5b2-4b66-9475-3ce98b074d37', got '%s'",
			dpnBag.Rights[0])
	}
	if len(dpnBag.Interpretive) != 1 {
		t.Errorf("Interpretive: expected 1 item, got %d", len(dpnBag.Interpretive))
	}
	if dpnBag.Interpretive[0] != "821decbb-4063-48b1-adef-1d3906bf7b87" {
		t.Errorf("Interpretive[0]: expected '821decbb-4063-48b1-adef-1d3906bf7b87', got '%s'",
			dpnBag.Interpretive[0])
	}
	if len(dpnBag.ReplicatingNodes) != 1 {
		t.Errorf("ReplicatingNodes: expected 1 item, got %d", len(dpnBag.ReplicatingNodes))
	}
	if dpnBag.ReplicatingNodes[0] != "chron" {
		t.Errorf("ReplicatingNodes[0]: expected 'chron', got '%s'",
			dpnBag.ReplicatingNodes[0])
	}
	if len(dpnBag.Fixities) != 1 {
		t.Errorf("Fixities: expected 1 item, got %d", len(dpnBag.Fixities))
	}
	if dpnBag.Fixities[0].Algorithm != "sha256" {
		t.Errorf("Fixities[0].Algorithm: expected 'sha256', got '%s'",
			dpnBag.Fixities[0].Algorithm)
	}
	if dpnBag.Fixities[0].Digest != "tums-for-digestion" {
		t.Errorf("Fixities[0].Digest: expected 'tums-for-digestion', got '%s'",
			dpnBag.Fixities[0].Digest)
	}
	if dpnBag.Fixities[0].CreatedAt.Format(time.RFC3339) != "2015-05-01T12:32:17Z" {
		t.Errorf("Fixities[0].CreatedAt: expected '2015-05-01T12:32:17Z', got '%s'",
			dpnBag.Fixities[0].CreatedAt.Format(time.RFC3339))
	}
}
