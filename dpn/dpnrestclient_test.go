package dpn_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/nu7hatch/gouuid"
	"net/http"
	"net/url"
	"strings"
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
var aptrustBagIdentifier = "472218b3-95ce-4b8e-6c21-6e514cfbe43f"
var replicationIdentifier = "aptrust-1"
var restoreIdentifier = "aptrust-1"

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

func makeXferRequest(fromNode, toNode, bagUuid string) (*dpn.DPNReplicationTransfer) {
	id, _ := uuid.NewV4()
	idString := id.String()
	randChars := idString[0:8]
	return &dpn.DPNReplicationTransfer{
		FromNode: fromNode,
		ToNode: toNode,
		UUID: bagUuid,
		FixityAlgorithm: "sha256",
		FixityNonce: "McNunce",
		FixityValue: randChars,
		FixityAccept: nil,
		BagValid: nil,
		Status: "Requested",
		Protocol: "R",
		Link: fmt.Sprintf("rsync://mnt/staging/%s.tar", idString),
	}
}

func makeRestoreRequest(fromNode, toNode, bagUuid string) (*dpn.DPNRestoreTransfer) {
	id, _ := uuid.NewV4()
	idString := id.String()
	return &dpn.DPNRestoreTransfer{
		FromNode: fromNode,
		ToNode: toNode,
		UUID: bagUuid,
		Status: "Requested",
		Protocol: "R",
		Link: fmt.Sprintf("rsync://mnt/staging/%s.tar", idString),
	}
}

func TestBuildUrl(t *testing.T) {
	config := loadConfig(t, configFile)
	client := getClient(t)
	relativeUrl := "/api-v1/popeye/olive/oyl/"
	expectedUrl := config.RestClient.LocalServiceURL + relativeUrl
	if client.BuildUrl(relativeUrl, nil) != expectedUrl {
		t.Errorf("BuildUrl returned '%s', expected '%s'",
			client.BuildUrl(relativeUrl, nil), expectedUrl)
	}
	params := url.Values{}
	params.Set("color", "blue")
	params.Set("material", "cotton")
	params.Set("size", "extra medium")
	actualUrl := client.BuildUrl(relativeUrl, &params)
	expectedUrl = expectedUrl + "?color=blue&material=cotton&size=extra+medium"
	if actualUrl != expectedUrl {
		t.Errorf("Got URL '%s', expected '%s'", actualUrl, expectedUrl)
	}
}

func TestDPNNodeGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	dpnNode, err := client.DPNNodeGet("aptrust")
	if err != nil {
		t.Error(err)
		return
	}
	if dpnNode.Name != "APTrust" {
		t.Errorf("Name: expected 'APTrust', got '%s'", dpnNode.Name)
	}
	if dpnNode.Namespace != "aptrust" {
		t.Errorf("Namespace: expected 'aptrust', got '%s'", dpnNode.Namespace)
	}
	if !strings.HasPrefix(dpnNode.APIRoot, "https://") && !strings.HasPrefix(dpnNode.APIRoot, "http://") {
		t.Errorf("APIRoot should begin with http:// or https://")
	}
}

func TestDPNNodeListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	nodeList, err := client.DPNNodeListGet(nil)
	if err != nil {
		t.Error(err)
		return
	}
	if nodeList.Count != 5 {
		t.Errorf("Expected 5 nodes, got %d", nodeList.Count)
	}
	if len(nodeList.Results) != 5 {
		t.Errorf("Expected 5 nodes, got %d", len(nodeList.Results))
	}
}

func TestDPNNodeUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	dpnNode, err := client.DPNNodeGet("sdr")
	if err != nil {
		t.Error(err)
		return
	}
	origPullDate := dpnNode.LastPullDate
	newPullDate := time.Date(2015, time.June, 1, 12, 0, 0, 0, time.UTC)
	if !origPullDate.IsZero() {
		newPullDate = dpnNode.LastPullDate.Add(-24 * time.Hour)
	}
	dpnNode.LastPullDate = newPullDate
	savedNode, err := client.DPNNodeUpdate(dpnNode)
	if savedNode.LastPullDate != newPullDate {
		t.Errorf("Expected last pull date %s, got %s",
			newPullDate.Format(time.RFC3339Nano),
			savedNode.LastPullDate.Format(time.RFC3339Nano))
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
	if dpnBag.LocalId != "test.edu/test.edu.bag6" {
		t.Errorf("LocalId: expected 'test.edu/test.edu.bag6', got '%s'", dpnBag.LocalId)
	}
	if dpnBag.Size != 3974144 {
		t.Errorf("Size: expected 3974144, got %d", dpnBag.Size)
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
	if dpnBag.CreatedAt.Format(time.RFC3339) != "2015-05-22T19:08:48Z" {
		t.Errorf("CreatedAt: expected '2015-05-22T19:08:48Z', got '%s'",
			dpnBag.CreatedAt.Format(time.RFC3339))
	}
	if dpnBag.UpdatedAt.Format(time.RFC3339) != "2015-05-22T19:08:48Z" {
		t.Errorf("UpdatedAt: expected '2015-05-22T19:08:48Z', got '%s'",
			dpnBag.UpdatedAt.Format(time.RFC3339))
	}
	//
	// TODO - Add Rights/Interpretive to this test object and then uncomment the
	//        following tests.
	//
	// if len(dpnBag.Rights) != 1 {
	// 	t.Errorf("Rights: expected 1 item, got %d", len(dpnBag.Rights))
	// }
	// if dpnBag.Rights[0] != "ff297922-a5b2-4b66-9475-3ce98b074d37" {
	// 	t.Errorf("Rights[0]: expected 'ff297922-a5b2-4b66-9475-3ce98b074d37', got '%s'",
	// 		dpnBag.Rights[0])
	// }
	// if len(dpnBag.Interpretive) != 1 {
	// 	t.Errorf("Interpretive: expected 1 item, got %d", len(dpnBag.Interpretive))
	// }
	// if dpnBag.Interpretive[0] != "821decbb-4063-48b1-adef-1d3906bf7b87" {
	// 	t.Errorf("Interpretive[0]: expected '821decbb-4063-48b1-adef-1d3906bf7b87', got '%s'",
	// 		dpnBag.Interpretive[0])
	// }
	if len(dpnBag.ReplicatingNodes) != 1 {
		t.Errorf("ReplicatingNodes: expected 1 item, got %d", len(dpnBag.ReplicatingNodes))
	}
	if dpnBag.ReplicatingNodes[0] != "tdr" {
		t.Errorf("ReplicatingNodes[0]: expected 'tdr', got '%s'",
			dpnBag.ReplicatingNodes[0])
	}
	if len(dpnBag.Fixities) != 1 {
		t.Errorf("Fixities: expected 1 item, got %d", len(dpnBag.Fixities))
	}
	if dpnBag.Fixities[0].Sha256 != "5329a5d06216ca9effc42a6f5b7c492952334d8b188ebbdefbbd0b970ab981a3" {
		t.Errorf("Fixities[0].Sha256: expected '5329a5d06216ca9effc42a6f5b7c492952334d8b188ebbdefbbd0b970ab981a3', got '%s'",
			dpnBag.Fixities[0].Sha256)
	}
}

func TestDPNBagListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bagList, err := client.DPNBagListGet(nil)
	if err != nil {
		t.Error(err)
		return
	}
	if bagList == nil {
		t.Errorf("DPNBagListGet returned nil result")
		return
	}
	unfilteredCount := bagList.Count
	if unfilteredCount == 0 {
		t.Errorf("DPNBagListGet returned zero results. Are there any bags in the registry?")
		return
	}
	aptrustCount := 0
	for i := range bagList.Results {
		bag := bagList.Results[i]
		if bag.IngestNode == "aptrust" {
			aptrustCount++
		}
	}

	// Test filters
	// Get all bags updated after December 31, 1999
	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params := url.Values{}
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	bagList, err = client.DPNBagListGet(&params)
	if err != nil {
		t.Error(err)
		return
	}
	if bagList.Count != unfilteredCount {
		t.Errorf("Filter by 'after' returned %d results, expected %d", bagList.Count, unfilteredCount)
	}

	// Get all bags updated after 1 hour from now
	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	bagList, err = client.DPNBagListGet(&params)
	if err != nil {
		t.Error(err)
		return
	}
	if bagList.Count != 0 {
		t.Errorf("Filter by 'after' returned %d results, expected 0", bagList.Count)
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
	if xfer.ToNode != "tdr" {
		t.Errorf("ToNode: expected 'tdr', got '%s'", xfer.ToNode)
	}
	if xfer.UUID != aptrustBagIdentifier {
		t.Errorf("UUID: expected '%s', got '%s'", aptrustBagIdentifier, xfer.UUID)
	}
	if xfer.ReplicationId != replicationIdentifier {
		t.Errorf("ReplicationId: expected '%s', got '%s'", replicationIdentifier, xfer.ReplicationId)
	}
	if xfer.FixityNonce != "" {
		t.Errorf("FixityNonce: expected '', got '%s'", xfer.FixityNonce)
	}
	if xfer.FixityValue != "5329a5d06216ca9effc42a6f5b7c492952334d8b188ebbdefbbd0b970ab981a3" {
		t.Errorf("FixityValue: expected '5329a5d06216ca9effc42a6f5b7c492952334d8b188ebbdefbbd0b970ab981a3', got '%s'", xfer.FixityValue)
	}
	if xfer.FixityAlgorithm != "sha256" {
		t.Errorf("FixityAlgorithm: expected 'sha256', got '%s'", xfer.FixityAlgorithm)
	}
	if xfer.FixityAccept == nil || *xfer.FixityAccept != true {
		t.Errorf("FixityAccept: expected nil, got %s", *xfer.FixityAccept)
	}
	if xfer.BagValid != nil {
		t.Errorf("BagValid: expected nil, got %s", *xfer.BagValid)
	}
	if xfer.Status != "Confirmed" {
		t.Errorf("Status: expected 'Confirmed', got '%s'", xfer.Status)
	}
	if xfer.Protocol != "R" {
		t.Errorf("Protocol: expected 'R', got '%s'", xfer.Protocol)
	}
	if xfer.Link != "dpn.tdr@devops.aptrust.org:outbound/472218b3-95ce-4b8e-6c21-6e514cfbe43f.tar" {
		t.Errorf("Link: expected 'dpn.tdr@devops.aptrust.org:outbound/472218b3-95ce-4b8e-6c21-6e514cfbe43f.tar', got '%s'", xfer.Link)
	}
	if xfer.CreatedAt.Format(time.RFC3339) != "2015-05-22T19:46:45Z" {
		t.Errorf("CreatedAt: expected '2015-05-22T19:46:45Z', got '%s'",
			xfer.CreatedAt.Format(time.RFC3339))
	}
	if xfer.UpdatedAt.Format(time.RFC3339) != "2015-05-28T16:15:35Z" {
		t.Errorf("UpdatedAt: expected '2015-05-28T16:15:35Z', got '%s'",
			xfer.UpdatedAt.Format(time.RFC3339))
	}
	if xfer.Link != "dpn.tdr@devops.aptrust.org:outbound/472218b3-95ce-4b8e-6c21-6e514cfbe43f.tar" {
		t.Errorf("Link: expected 'dpn.tdr@devops.aptrust.org:outbound/472218b3-95ce-4b8e-6c21-6e514cfbe43f.tar', got '%s'", xfer.Link)
	}
}

func TestDPNReplicationListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xferList, err := client.DPNReplicationListGet(nil)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList == nil {
		t.Errorf("DPNReplicationListGet returned nil result")
		return
	}
	if xferList.Count == 0 || len(xferList.Results) == 0 {
		t.Errorf("DPNReplicationListGet returned zero results")
		return
	}

	totalRecordCount := xferList.Count

	params := &url.Values{}
	params.Set("bag_valid", "true")
	xferList, err = client.DPNReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Set("bag_valid", "false")
	xferList, err = client.DPNReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Del("bag_valid")
	params.Set("fixity_accept", "true")
	xferList, err = client.DPNReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Set("fixity_accept", "false")
	xferList, err = client.DPNReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Del("fixity_accept")

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	xferList, err = client.DPNReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList.Count != totalRecordCount {
		t.Errorf("Expected %d records, got %d", totalRecordCount, xferList.Count)
	}

	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	xferList, err = client.DPNReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList.Count != 0 {
		t.Errorf("Expected 0 records, got %d", xferList.Count)
	}
}

func TestReplicationTransferCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := makeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Make sure we can create a transfer request.
	xfer := makeXferRequest("aptrust", "chron", dpnBag.UUID)
	newXfer, err := client.ReplicationTransferCreate(xfer)
	if err != nil {
		t.Errorf("ReplicationTransferCreate returned error %v", err)
	}
	if newXfer == nil {
		t.Errorf("ReplicationTransferCreate did not return an object")
		return
	}

	// Make sure the fields were set correctly.
	if newXfer.FromNode != xfer.FromNode {
		t.Errorf("FromNode is %s; expected %s", newXfer.FromNode, xfer.FromNode)
	}
	if newXfer.ToNode != xfer.ToNode {
		t.Errorf("ToNode is %s; expected %s", newXfer.ToNode, xfer.ToNode)
	}
	if newXfer.UUID != xfer.UUID {
		t.Errorf("UUID is %s; expected %s", newXfer.UUID, xfer.UUID)
	}
	if newXfer.ReplicationId == "" {
		t.Errorf("ReplicationId is missing")
	}
	if newXfer.FixityAlgorithm != xfer.FixityAlgorithm {
		t.Errorf("FixityAlgorithm is %s; expected %s",
			newXfer.FixityAlgorithm, xfer.FixityAlgorithm)
	}
	if newXfer.FixityNonce != xfer.FixityNonce {
		t.Errorf("FixityNonce is %s; expected %s",
			newXfer.FixityNonce, xfer.FixityNonce)
	}
	if newXfer.FixityValue != "" {
		t.Errorf("FixityValue was set to %s but it shouldn't have been. " +
			"(This is a problem with the server implementation, not our code!",
			newXfer.FixityValue)
	}
	if newXfer.FixityAccept != nil {
		t.Errorf("FixityAccept is %s; expected nil", *newXfer.FixityAccept)
	}
	if newXfer.BagValid != nil {
		t.Errorf("BagValid is %s; expected nil", *newXfer.BagValid)
	}
	if newXfer.Status != "Requested" {
		t.Errorf("Status is %s; expected Requested", newXfer.Status)
	}
	if newXfer.Protocol != xfer.Protocol {
		t.Errorf("Protocol is %s; expected %s", newXfer.Protocol, xfer.Protocol)
	}
	if newXfer.Link != xfer.Link {
		t.Errorf("Link is %s; expected %s", newXfer.Link, xfer.Link)
	}
	if newXfer.CreatedAt.IsZero() {
		t.Errorf("CreatedAt was not set")
	}
	if newXfer.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt was not set")
	}
}

func TestReplicationTransferUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := makeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Make sure we can create a transfer request.
	xfer := makeXferRequest("aptrust", "chron", dpnBag.UUID)
	newXfer, err := client.ReplicationTransferCreate(xfer)
	if err != nil {
		t.Errorf("ReplicationTransferCreate returned error %v", err)
		return
	}
	if newXfer == nil {
		t.Errorf("ReplicationTransferCreate did not return an object")
		return
	}

	// Reject this one...
	newXfer.Status = "Rejected"

	updatedXfer, err := client.ReplicationTransferUpdate(newXfer)
	if err != nil {
		t.Errorf("ReplicationTransferUpdate returned error %v", err)
		return
	}
	if updatedXfer == nil {
		t.Errorf("ReplicationTransferUpdate did not return an object")
		return
	}

	// ... make sure status is correct
	if updatedXfer.Status != "Rejected" {
		t.Errorf("Status is %s; expected Rejected", updatedXfer.Status)
	}


	// Update the allowed fields. We're going to send a bad
	// fixity value, because we don't know the good one, so
	// the server will cancel this transfer.
	bagValid := true
	newXfer.Status = "Received"
	newXfer.BagValid = &bagValid
	newXfer.FixityValue = "1234567890"

	updatedXfer, err = client.ReplicationTransferUpdate(newXfer)
	if err != nil {
		t.Errorf("ReplicationTransferUpdate returned error %v", err)
		return
	}
	if updatedXfer == nil {
		t.Errorf("ReplicationTransferUpdate did not return an object")
		return
	}

	// Make sure the fields were set correctly.
	if updatedXfer.FixityValue != "1234567890" {
		t.Errorf("FixityValue was %s; expected 1234567890",
			updatedXfer.FixityValue)
	}
	if *updatedXfer.FixityAccept != false {
		t.Errorf("FixityAccept is %s; expected false", *updatedXfer.FixityAccept)
	}
	if *updatedXfer.BagValid != true {
		t.Errorf("BagValid is %s; expected true", *updatedXfer.BagValid)
	}
	// Note: Status will be cancelled instead of received because
	// we sent a bogus checksum, and that causes the server to cancel
	// the transfer.
	if updatedXfer.Status != "Cancelled" {
		t.Errorf("Status is %s; expected Cancelled", updatedXfer.Status)
	}
	if updatedXfer.UpdatedAt.After(newXfer.UpdatedAt) == false {
		t.Errorf("UpdatedAt was not updated")
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
	if xfer.UUID != "41e5376c-cc13-4c3e-6af3-297cc2e005aa" {
		t.Errorf("UUID: expected '41e5376c-cc13-4c3e-6af3-297cc2e005aa', got '%s'",
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
	if xfer.CreatedAt.Format(time.RFC3339) != "2015-06-01T19:07:53Z" {
		t.Errorf("CreatedAt: expected '2015-06-01T19:07:53Z', got '%s'",
			xfer.CreatedAt.Format(time.RFC3339))
	}
	if xfer.UpdatedAt.Format(time.RFC3339) != "2015-06-01T19:07:53Z" {
		t.Errorf("UpdatedAt: expected '2015-06-01T19:07:53Z', got '%s'",
			xfer.UpdatedAt.Format(time.RFC3339))
	}
	if xfer.Link != "rsync://mnt/staging/41e5376c-cc13-4c3e-6af3-297cc2e005aa.tar" {
		t.Errorf("Link: expected 'rsync://mnt/staging/41e5376c-cc13-4c3e-6af3-297cc2e005aa.tar', got '%s'", xfer.Link)
	}
}

func TestDPNRestoreListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xferList, err := client.DPNRestoreListGet(nil)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList == nil {
		t.Errorf("DPNRestoreListGet returned nil result")
		return
	}
	if xferList.Count == 0 || len(xferList.Results) == 0 {
		t.Errorf("DPNRestoreListGet returned zero results")
		return
	}

	totalRecordCount := xferList.Count

	params := &url.Values{}
	params.Set("bag_valid", "true")
	xferList, err = client.DPNRestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Set("bag_valid", "false")
	xferList, err = client.DPNRestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Del("bag_valid")
	params.Set("fixity_accept", "true")
	xferList, err = client.DPNRestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Set("fixity_accept", "false")
	xferList, err = client.DPNRestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Del("fixity_accept")

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	xferList, err = client.DPNRestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList.Count != totalRecordCount {
		t.Errorf("Expected %d records, got %d", totalRecordCount, xferList.Count)
	}

	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	xferList, err = client.DPNRestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList.Count != 0 {
		t.Errorf("Expected 0 records, got %d", xferList.Count)
	}
}

func TestRestoreTransferCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := makeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Make sure we can create a transfer request.
	xfer := makeRestoreRequest("tdr", "aptrust", dpnBag.UUID)
	newXfer, err := client.RestoreTransferCreate(xfer)
	if err != nil {
		t.Errorf("RestoreTransferCreate returned error %v", err)
		return
	}
	if newXfer == nil {
		t.Errorf("RestoreTransferCreate did not return an object")
		return
	}

	// Make sure the fields were set correctly.
	if newXfer.FromNode != xfer.FromNode {
		t.Errorf("FromNode is %s; expected %s", newXfer.FromNode, xfer.FromNode)
	}
	if newXfer.ToNode != xfer.ToNode {
		t.Errorf("ToNode is %s; expected %s", newXfer.ToNode, xfer.ToNode)
	}
	if newXfer.UUID != xfer.UUID {
		t.Errorf("UUID is %s; expected %s", newXfer.UUID, xfer.UUID)
	}
	if newXfer.RestoreId == "" {
		t.Errorf("RestoreId is missing")
	}
	if newXfer.Status != "Requested" {
		t.Errorf("Status is %s; expected Requested", newXfer.Status)
	}
	if newXfer.Protocol != xfer.Protocol {
		t.Errorf("Protocol is %s; expected %s", newXfer.Protocol, xfer.Protocol)
	}
	if newXfer.Link != xfer.Link {
		t.Errorf("Link is %s; expected %s", newXfer.Link, xfer.Link)
	}
	if newXfer.CreatedAt.IsZero() {
		t.Errorf("CreatedAt was not set")
	}
	if newXfer.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt was not set")
	}
}

func TestRestoreTransferUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := makeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Make sure we can create a transfer request.
	xfer := makeRestoreRequest("chron", "aptrust", dpnBag.UUID)
	newXfer, err := client.RestoreTransferCreate(xfer)
	if err != nil {
		t.Errorf("RestoreTransferCreate returned error %v", err)
		return
	}
	if newXfer == nil {
		t.Errorf("RestoreTransferCreate did not return an object")
		return
	}

	// Reject this one...
	newXfer.Status = "Rejected"

	updatedXfer, err := client.RestoreTransferUpdate(newXfer)
	if err != nil {
		t.Errorf("RestoreTransferUpdate returned error %v", err)
		return
	}
	if updatedXfer == nil {
		t.Errorf("RestoreTransferUpdate did not return an object")
		return
	}

	// ... make sure status is correct
	if updatedXfer.Status != "Rejected" {
		t.Errorf("Status is %s; expected Rejected", updatedXfer.Status)
	}


	// Update the allowed fields. We're going to send a bad
	// fixity value, because we don't know the good one, so
	// the server will cancel this transfer.
	link := "rsync://blah/blah/blah/yadda/yadda/beer"
	newXfer.Status = "Prepared"
	newXfer.Link = link

	updatedXfer, err = client.RestoreTransferUpdate(newXfer)
	if err != nil {
		t.Errorf("RestoreTransferUpdate returned error %v", err)
		return
	}
	if updatedXfer == nil {
		t.Errorf("RestoreTransferUpdate did not return an object")
		return
	}

	// Make sure values were stored...
	if updatedXfer.Status != "Prepared" {
		t.Errorf("Status is %s; expected Prepared", updatedXfer.Status)
	}
	if updatedXfer.Link != link {
		t.Errorf("Status is %s; expected %s", updatedXfer.Link, link)
	}
	if updatedXfer.UpdatedAt.After(newXfer.UpdatedAt) == false {
		t.Errorf("UpdatedAt was not updated")
	}
}

func TestGetRemoteClient(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	config := loadConfig(t, configFile)
	logger := bagman.DiscardLogger("dpnrestclient_test")
	client := getClient(t)
	nodes := []string { "aptrust", "chron", "tdr" }
	for _, node := range nodes {
		_, err := client.GetRemoteClient(node, config, logger)
		if err != nil {
			t.Errorf("Error creating remote client: %v", err)
		}
	}
}

func TestHackNullDates(t *testing.T) {
	jsonString := `{ "id": 5, "last_pull_date": null }`
	testHackNullDates(jsonString, t)
	jsonString = `{"id":5,"last_pull_date":null}`
	testHackNullDates(jsonString, t)
	jsonString = `{
                     "id": 5,
                     "last_pull_date":    null
                   }`
	testHackNullDates(jsonString, t)
}

func testHackNullDates(jsonString string, t *testing.T) {
	data := make(map[string]interface{})
	jsonBytes := []byte(jsonString)
	hackedBytes := dpn.HackNullDates(jsonBytes)
	json.Unmarshal(hackedBytes, &data)
	if data["last_pull_date"] != "1980-01-01T00:00:00Z" {
		t.Errorf("Got unexpected last_pull_date %s", data["last_pull_date"])
	}
}
