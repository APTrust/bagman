package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
	"os"
	"path/filepath"
	"strings"
	"time"
)


// We want to mark these two APTrust bags
// for ingest to DPN, so they'll go into
// the work queue when we run our tests.
var APTrustBags = []string {
	"test.edu/ncsu.1840.16-1028",
	"test.edu/test.edu.bag2",
}
var testBagUuid = "00000000-0000-0000-0000-000000000001"
var goodBagPath = fmt.Sprintf("dpn/testdata/%s.tar", testBagUuid)
var testBagSize = uint64(268800)
var testBagDigest = "c4e254c4432d8f8755de161c42c4f8188455ca1c5ca1e2fd548d2a991dff009a"
var adminTestToken = "0000000000000000000000000000000000000000"
var FaberCollege = "9a000000-0000-4000-a000-000000000002"

// dpn_test_setup.go sets up some test data on our local DPN REST cluster
// so that we can run some end-to-end replication tests. Make sure you're
// running the local DPN cluster before you run this. You can run the cluster
// by running DPN-REST/dpnode/run_cluster.sh. This app will set up some bags
// and replication requests in the local cluster, and will make sure that
// the bag files exist in a location we can copy from using rsync.
// This app runs as part of bagman/scripts/dpn_local_test.sh.
func main() {
	testUtil := NewTestUtil()
	err := testUtil.MakeTestDirs()
	if err != nil {
		testUtil.ProcUtil.MessageLog.Fatal(err)
	}
	err = testUtil.MakeTestData()
	if err != nil {
		testUtil.ProcUtil.MessageLog.Fatal(err)
	}
	err = testUtil.MarkAPTrustBagsForDPN()
	if err != nil {
		testUtil.ProcUtil.MessageLog.Fatal(err)
	}
}

type TestUtil struct {
	ProcUtil             *bagman.ProcessUtil
	DPNConfig            *dpn.DPNConfig
	LocalRestClient      *dpn.DPNRestClient
	RemoteClients        map[string]*dpn.DPNRestClient
	RemoteAdminClients   map[string]*dpn.DPNRestClient
}

func NewTestUtil() (*TestUtil) {
	procUtil := workers.CreateProcUtil()
	procUtil.MessageLog.Info("Starting data setup for local integration test")
	dpnConfig, err := dpn.LoadConfig("dpn/dpn_config.json", "test")
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}

	// Create a local REST client to talk to our local APTrust DPN REST server
	localClient, err := dpn.NewDPNRestClient(
		dpnConfig.RestClient.LocalServiceURL,
		dpnConfig.RestClient.LocalAPIRoot,
		dpnConfig.RestClient.LocalAuthToken,
		dpnConfig.LocalNode,
		dpnConfig,
		procUtil.MessageLog)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}

	// Create clients to talk to remote DPN REST servers.
	// Actually, these are servers in the local cluster that
	// impersonate remote nodes. All of these admin clients
	// need to use the admin API token. We need these admin
	// clients to do admin-only work, such as creating bags
	// and transfer requests.
	remoteClients, err := dpn.GetRemoteClients(localClient, dpnConfig,
		procUtil.MessageLog)
	adminConfig := *dpnConfig
	remoteAdminClients, err := dpn.GetRemoteClients(localClient,
		&adminConfig, procUtil.MessageLog)
	for _, remoteAdminClient := range remoteAdminClients {
		// Give this client the admin API token, so it
		// can perform admin operations.
		remoteAdminClient.APIKey = dpnConfig.RemoteNodeAdminTokensForTesting[remoteAdminClient.Node]
	}

	if err != nil {
		panic(err)
	}

	return &TestUtil{
		ProcUtil: procUtil,
		DPNConfig: dpnConfig,
		LocalRestClient: localClient,
		RemoteClients: remoteClients,
		RemoteAdminClients: remoteAdminClients,
	}
}

func (testUtil *TestUtil) MakeTestDirs() (error) {
	err := os.MkdirAll(testUtil.ProcUtil.Config.DPNStagingDirectory, 0755)
	if err != nil {
		return err
	}
	testUserDir := filepath.Join(testUtil.ProcUtil.Config.DPNHomeDirectory,
		"integration_test")
	err = os.MkdirAll(testUserDir, 0755)
	return err
}

func (testUtil *TestUtil) MakeTestData() (error) {
	count := 0
	for node, _ := range testUtil.DPNConfig.RemoteNodeURLs {
		count += 1

		// Create a symlink from dpn_home/integration_test/<uuid>.tar
		// to our known good bag in dpn/testdata/000...1.tar
		bagUuid := fmt.Sprintf("00000000-0000-4000-a000-00000000000%d", count)
		linkPath, err := testUtil.CreateSymLink(bagUuid)
		if err != nil {
			return err
		} else {
			testUtil.ProcUtil.MessageLog.Info("Created symlink at %s", linkPath)
		}

		// Create an entry for this bag on the remote node.
		bag, err := testUtil.CreateBag(bagUuid, node)
		if err != nil {
			return err
		} else {
			testUtil.ProcUtil.MessageLog.Info("Created bag %s on %s",
				bag.UUID, bag.AdminNode)
		}

		// Create a transfer record for this bag on the remote node.
		xfer, err := testUtil.CreateReplicationRequest(bag, linkPath)
		if err != nil {
			return err
		} else {
			testUtil.ProcUtil.MessageLog.Info(
				"Created replication request %s on %s",
				xfer.ReplicationId, bag.AdminNode)
		}
	}
	return nil
}

func (testUtil *TestUtil) CreateSymLink(bagUuid string) (string, error) {
	sourceFile, err := bagman.RelativeToAbsPath(goodBagPath)
	if err != nil {
		return "", err
	}
	linkPath := filepath.Join(testUtil.ProcUtil.Config.DPNHomeDirectory,
		"integration_test", bagUuid + ".tar")
	if bagman.FileExists(linkPath) {
		return linkPath, nil
	}
	err = os.Symlink(sourceFile, linkPath)
	if err != nil {
		return "", err
	}
	return linkPath, err
}

func (testUtil *TestUtil) CreateBag(bagUuid, node string) (*dpn.DPNBag, error) {
	bag, err := testUtil.RemoteAdminClients[node].DPNBagGet(bagUuid)
	if err == nil && bag != nil {
		// Bag already exists. No need to recreate it.
		return bag, err
	}
	utcNow := time.Now().UTC()
	bag = &dpn.DPNBag{
		UUID: bagUuid,
		LocalId: fmt.Sprintf("integration-test-%s-1000", node),
		Size: testBagSize,
		FirstVersionUUID: bagUuid,
		Version: 1,
		IngestNode: node,
		AdminNode: node,
		BagType: "D",
		Rights: make([]string, 0),
		Interpretive: make([]string, 0),
		ReplicatingNodes: make([]string, 0),
		Member: FaberCollege,
		Fixities: &dpn.DPNFixity{
			Sha256: testBagDigest,
		},
		CreatedAt: utcNow,
		UpdatedAt: utcNow,
	}
	// You have to be node admin to create a bag, so use the admin client.
	return testUtil.RemoteAdminClients[node].DPNBagCreate(bag)
}

func (testUtil *TestUtil) CreateReplicationRequest(bag *dpn.DPNBag, linkPath string) (*dpn.DPNReplicationTransfer, error) {
	utcNow := time.Now().UTC()
	xfer := &dpn.DPNReplicationTransfer{
		FromNode: bag.AdminNode,
		ToNode: testUtil.DPNConfig.LocalNode,
		BagId: bag.UUID,
		ReplicationId: strings.Replace(bag.UUID, "4000", "4444", 1),
		FixityAlgorithm: "sha256",
		Status: "requested",
		Protocol: "rsync",
		Link: linkPath,
		CreatedAt: utcNow,
		UpdatedAt: utcNow,
	}
	// You have to be node admin to create the transfer request,
	// so use the admin client.
	return testUtil.RemoteAdminClients[bag.AdminNode].ReplicationTransferCreate(xfer)
}

// Mark two existing APTrust bags for ingest into DPN
func (testUtil *TestUtil) MarkAPTrustBagsForDPN() (error) {
	for _, identifier := range APTrustBags {
		processedItem, err := testUtil.GetLatestStatusFor(identifier)
		if err != nil {
			return err
		}
		// Clear out the id, so the Fluctus client will create a new
		// ProcessedItem record that says this bag has a pending DPN
		// ingest request.
		processedItem.Id = 0
		processedItem.Date = time.Now()
		processedItem.GenericFileIdentifier = ""
		processedItem.Note = "Requested item be sent to DPN"
		processedItem.User = ""
		processedItem.Action = "DPN"
		processedItem.Status = "Pending"
		processedItem.Stage = "Requested"
		processedItem.Retry = true
		processedItem.Node = ""
		processedItem.Pid = 0
		processedItem.State = ""
		processedItem.NeedsAdminReview = false
		err = testUtil.ProcUtil.FluctusClient.UpdateProcessedItem(processedItem)
		if err != nil {
			return err
		}
		testUtil.ProcUtil.MessageLog.Debug("Created DPN ingest request for bag %s", identifier)
	}
	return nil
}

func (testUtil *TestUtil) GetLatestStatusFor(objectIdentifier string) (*bagman.ProcessStatus, error) {
	testUtil.ProcUtil.MessageLog.Debug(
		"Looking up latest status for bag %s",
		objectIdentifier)
	ps := &bagman.ProcessStatus{
		ObjectIdentifier: objectIdentifier,
		Action: "Ingest",
		Status: "Success",
	}
	statusRecords, err := testUtil.ProcUtil.FluctusClient.ProcessStatusSearch(ps, false, false)
	if err != nil {
		return nil, err
	}
	if len(statusRecords) == 0 {
		return nil, fmt.Errorf("No ingest records found for %s", objectIdentifier)
	}
	var latestStatus *bagman.ProcessStatus
	latestTimestamp, _ := time.Parse(time.RFC3339, "1999-01-01T12:00:00+00:00")
	for i := range statusRecords {
		if statusRecords[i].Date.After(latestTimestamp) {
			latestTimestamp = statusRecords[i].Date
			latestStatus = statusRecords[i]
		}
	}
	return latestStatus, nil
}
