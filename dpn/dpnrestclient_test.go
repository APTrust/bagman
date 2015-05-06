package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/nu7hatch/gouuid"
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
var replicationIdentifier = "aptrust-999999"
var restoreIdentifier = "aptrust-64"

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

func makeBag() (*dpn.DPNBag) {
	youyoueyedee, _ := uuid.NewV4()
	randChars := youyoueyedee.String()[0:8]
	return &dpn.DPNBag {
		UUID: youyoueyedee.String(),
		Interpretive: []string{},
		Rights: []string{},
		ReplicatingNodes: []string{},
		Fixities: []*dpn.DPNFixity {
			&dpn.DPNFixity{
				Sha256: randChars,
			},
		},
		LocalId: "my_bag",
		Size: 12345678,
		FirstVersionUUID: youyoueyedee.String(),
		Version: 1,
		BagType: "D",
	}
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
	if dpnBag.Fixities[0].Sha256 != "tums-for-digestion" {
		t.Errorf("Fixities[0].Sha256: expected 'tums-for-digestion', got '%s'",
			dpnBag.Fixities[0].Sha256)
	}
}

func TestDPNBagCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bag := makeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// We should get back a copy of the same bag we sent,
	// with some additional info filled in.
	if dpnBag.UUID != bag.UUID {
		t.Errorf("UUIDs don't match. Ours = %s, Theirs = %s", bag.UUID, dpnBag.UUID)
	}
	if dpnBag.LocalId != bag.LocalId {
		t.Errorf("LocalIds don't match. Ours = %s, Theirs = %s", bag.LocalId, dpnBag.LocalId)
	}
	if dpnBag.Size != bag.Size {
		t.Errorf("Sizes don't match. Ours = %d, Theirs = %d", bag.Size, dpnBag.Size)
	}
	if dpnBag.FirstVersionUUID != bag.FirstVersionUUID {
		t.Errorf("FirstVersionUUIDs don't match. Ours = %s, Theirs = %s",
			bag.FirstVersionUUID, dpnBag.FirstVersionUUID)
	}
	if dpnBag.Version != bag.Version {
		t.Errorf("Versions don't match. Ours = %d, Theirs = %d", bag.Version, dpnBag.Version)
	}
	if dpnBag.BagType != bag.BagType {
		t.Errorf("BagTypes don't match. Ours = %s, Theirs = %s", bag.BagType, dpnBag.BagType)
	}
	if dpnBag.Fixities == nil || len(dpnBag.Fixities) == 0 {
		t.Errorf("Bag fixities are missing")
	}
	if dpnBag.Fixities[0].Sha256 != bag.Fixities[0].Sha256 {
		t.Errorf("Fixities don't match. Ours = %s, Theirs = %s",
			bag.Fixities[0].Sha256, dpnBag.Fixities[0].Sha256)
	}

	// These tests really check that the server is behaving correctly,
	// which isn't our business, but if it's not, we want to know.
	if dpnBag.IngestNode == "" {
		t.Errorf("IngestNode was not set")
	}
	if dpnBag.IngestNode != dpnBag.AdminNode {
		t.Errorf("Ingest/Admin node mismatch. Ingest = %s, Admin = %s",
			dpnBag.IngestNode, dpnBag.AdminNode)
	}
	if dpnBag.CreatedAt.IsZero() {
		t.Errorf("CreatedAt was not set")
	}
	if dpnBag.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt was not set")
	}

	// Make sure we can create a bag that has rights and interpretive
	// uuids specified.
	anotherBag := makeBag()
	anotherBag.Rights = append(anotherBag.Rights, bag.UUID)
	anotherBag.Interpretive = append(anotherBag.Interpretive, bag.UUID)

	dpnBag, err = client.DPNBagCreate(anotherBag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error when creating bag " +
			"with rights and interpretive UUIDs: %v", err)
		return
	}

}

func TestDPNBagUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bag := makeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}
	anotherBag := makeBag()
	dpnBag, err = client.DPNBagCreate(anotherBag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Add replicating nodes, rights and interpretive bags.
	// The service we're testing against should have records
	// for the chron and trd nodes, since they are founding
	// member nodes.
	dpnBag.ReplicatingNodes = append(dpnBag.ReplicatingNodes, "chron")
	dpnBag.ReplicatingNodes = append(dpnBag.ReplicatingNodes, "tdr")
	dpnBag.Rights = append(dpnBag.Rights, anotherBag.UUID)
	dpnBag.Interpretive = append(dpnBag.Interpretive, anotherBag.UUID)

	updatedBag, err := client.DPNBagUpdate(dpnBag)
	if err != nil {
		t.Errorf("DPNBagUpdate returned error %v", err)
		return
	}
	if updatedBag.ReplicatingNodes == nil || len(updatedBag.ReplicatingNodes) != 2 {
		t.Errorf("Updated bag should have two replicating nodes")
	}
	if updatedBag.Rights == nil || len(updatedBag.Rights) != 1 {
		t.Errorf("Updated bag should have one Rights bag")
	}
	if updatedBag.Rights[0] != anotherBag.UUID {
		t.Errorf("Rights bag was %s; expected %s", updatedBag.Rights[0], anotherBag.UUID)
	}
	if updatedBag.Interpretive == nil || len(updatedBag.Interpretive) != 1 {
		t.Errorf("Updated bag should have one Interpretive bag")
	}
	if updatedBag.Interpretive[0] != anotherBag.UUID {
		t.Errorf("Interpretive bag was %s; expected %s", updatedBag.Interpretive[0], anotherBag.UUID)
	}
}

func TestReplicationTransferGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xfer, err := client.ReplicationTransferGet(replicationIdentifier)
	if err != nil {
		t.Error(err)
		return
	}
	if xfer.FromNode != "aptrust" {
		t.Errorf("FromNode: expected 'aptrust', got '%s'", xfer.FromNode)
	}
	if xfer.ToNode != "chron" {
		t.Errorf("ToNode: expected 'chron', got '%s'", xfer.ToNode)
	}
	if xfer.UUID != aptrustBagIdentifier {
		t.Errorf("UUID: expected '%s', got '%s'", aptrustBagIdentifier, xfer.UUID)
	}
	if xfer.ReplicationId != replicationIdentifier {
		t.Errorf("ReplicationId: expected '%s', got '%s'", replicationIdentifier, xfer.ReplicationId)
	}
	if xfer.FixityNonce != "dunce" {
		t.Errorf("FixityNonce: expected 'dunce', got '%s'", xfer.FixityNonce)
	}
	if xfer.FixityValue != "98765" {
		t.Errorf("FixityValue: expected '98765', got '%s'", xfer.FixityValue)
	}
	if xfer.FixityAlgorithm != "sha256" {
		t.Errorf("FixityAlgorithm: expected 'sha256', got '%s'", xfer.FixityAlgorithm)
	}
	if xfer.BagValid != true {
		t.Errorf("BagValid: expected true, got %s", xfer.BagValid)
	}
	if xfer.Status != "Confirmed" {
		t.Errorf("Status: expected 'Confirmed', got '%s'", xfer.Status)
	}
	if xfer.Protocol != "R" {
		t.Errorf("Protocol: expected 'R', got '%s'", xfer.Protocol)
	}
	if xfer.Link != "rsync://are/sink" {
		t.Errorf("Link: expected 'rsync://are/sink', got '%s'", xfer.Link)
	}
	if xfer.CreatedAt.Format(time.RFC3339) != "2015-05-01T12:19:44Z" {
		t.Errorf("CreatedAt: expected '2015-05-01T12:19:44Z', got '%s'",
			xfer.CreatedAt.Format(time.RFC3339))
	}
	if xfer.UpdatedAt.Format(time.RFC3339) != "2015-05-01T12:19:44Z" {
		t.Errorf("UpdatedAt: expected '2015-05-01T12:19:44Z', got '%s'",
			xfer.UpdatedAt.Format(time.RFC3339))
	}
	if xfer.Link != "rsync://are/sink" {
		t.Errorf("Link: expected 'rsync://are/sink', got '%s'", xfer.Link)
	}
}

func TestRestoreTransferGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xfer, err := client.RestoreTransferGet(restoreIdentifier)
	if err != nil {
		t.Error(err)
		return
	}
	if xfer.FromNode != "tdr" {
		t.Errorf("FromNode: expected 'tdr', got '%s'", xfer.FromNode)
	}
	if xfer.ToNode != "aptrust" {
		t.Errorf("ToNode: expected 'aptrust', got '%s'", xfer.ToNode)
	}
	if xfer.UUID != "6078e948-d561-42b4-b13b-cf0404575cf7" {
		t.Errorf("UUID: expected '6078e948-d561-42b4-b13b-cf0404575cf7', got '%s'",
			xfer.UUID)
	}
	if xfer.RestoreId != restoreIdentifier {
		t.Errorf("RestoreId: expected '%s', got '%s'", restoreIdentifier, xfer.RestoreId)
	}
	if xfer.Status != "Requested" {
		t.Errorf("Status: expected 'Requested', got '%s'", xfer.Status)
	}
	if xfer.Protocol != "R" {
		t.Errorf("Protocol: expected 'R', got '%s'", xfer.Protocol)
	}
	if xfer.CreatedAt.Format(time.RFC3339) != "2015-02-25T15:27:40Z" {
		t.Errorf("CreatedAt: expected '2015-02-25T15:27:40Z', got '%s'",
			xfer.CreatedAt.Format(time.RFC3339))
	}
	if xfer.UpdatedAt.Format(time.RFC3339) != "2015-05-01T20:11:49Z" {
		t.Errorf("UpdatedAt: expected '2015-05-01T20:11:49Z', got '%s'",
			xfer.UpdatedAt.Format(time.RFC3339))
	}
	if xfer.Link != "rsync://path/to/file.tar" {
		t.Errorf("Link: expected 'rsync://path/to/file.tar', got '%s'", xfer.Link)
	}
}
