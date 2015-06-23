package main

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
	"math/rand"
	"os"
	"path/filepath"
)

var TEST_NODE_URLS = map[string]string {
	"chron": "http://127.0.0.1:8001",
	"hathi": "http://127.0.0.1:8002",
	"sdr":   "http://127.0.0.1:8003",
	"tdr":   "http://127.0.0.1:8004",
}
var testBagUuid = "00000000-0000-0000-0000-000000000001"
var goodBagPath = fmt.Sprintf("dpn/testdata/%s.tar", testBagUuid)
var testBagSize = uint64(268800)
var testBagDigest = "f9f39a1602cde405042dd8b4859c6a3e2c04092a76eaab858ae28e48403ccba4"
var adminTestToken = "0000000000000000000000000000000000000000"

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
	dpnConfig, err := dpn.LoadConfig("dpn/dpn_config.json")
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}

	// Create a local REST client to talk to our local APTrust DPN REST server
	localClient, err := dpn.NewDPNRestClient(
		dpnConfig.RestClient.LocalServiceURL,
		dpnConfig.RestClient.LocalAPIRoot,
		dpnConfig.RestClient.LocalAuthToken,
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
	adminConfig.RemoteNodeTokens["chron"] = adminTestToken
	adminConfig.RemoteNodeTokens["hathi"] = adminTestToken
	adminConfig.RemoteNodeTokens["sdr"] = adminTestToken
	adminConfig.RemoteNodeTokens["tdr"] = adminTestToken
	remoteAdminClients, err := dpn.GetRemoteClients(localClient,
		&adminConfig, procUtil.MessageLog)

	// Point the remote clients toward our own local DPN test cluster.
	// This means you have to run the run_cluster.sh script in the
	// DPN REST project to run these tests.
	for nodeNamespace := range remoteClients {
		remoteClient := remoteClients[nodeNamespace]
		remoteClient.HostUrl = TEST_NODE_URLS[nodeNamespace]
		remoteAdminClient := remoteAdminClients[nodeNamespace]
		remoteAdminClient.HostUrl = TEST_NODE_URLS[nodeNamespace]
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
	allNodes, err := testUtil.LocalRestClient.DPNNodeListGet(nil)
	if err != nil {
		return err
	}
	count := 0
	for _, node := range allNodes.Results {
		if node.Namespace == testUtil.DPNConfig.LocalNode {
			continue
		}
		count += 1

		// Create a symlink from dpn_home/integration_test/<uuid>.tar
		// to our known good bag in dpn/testdata/000...1.tar
		bagUuid := fmt.Sprintf("%d0000000-0000-0000-0000-000000000001", count)
		linkPath, err := testUtil.CreateSymLink(bagUuid)
		if err != nil {
			return err
		} else {
			testUtil.ProcUtil.MessageLog.Info("Created symlink at %s", linkPath)
		}

		// Create an entry for this bag on the remote node.
		bag, err := testUtil.CreateBag(bagUuid, node.Namespace)
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
	bag, err := testUtil.RemoteClients[node].DPNBagGet(bagUuid)
	if err == nil && bag != nil {
		// Bag already exists. No need to recreate it.
		return bag, err
	}
	bag = &dpn.DPNBag{
		UUID: bagUuid,
		LocalId: fmt.Sprintf("integration-test-%s-1", node),
		Size: testBagSize,
		FirstVersionUUID: bagUuid,
		Version: 1,
		IngestNode: node,
		AdminNode: node,
		BagType: "D",
		Rights: make([]string, 0),
		Interpretive: make([]string, 0),
		ReplicatingNodes: make([]string, 0),
		Fixities: []*dpn.DPNFixity {
			&dpn.DPNFixity{
				Sha256: testBagDigest,
			},
		},
	}
	// You have to be node admin to create a bag, so use the admin client.
	return testUtil.RemoteAdminClients[node].DPNBagCreate(bag)
}

func (testUtil *TestUtil) CreateReplicationRequest(bag *dpn.DPNBag, linkPath string) (*dpn.DPNReplicationTransfer, error) {
	replicationId := fmt.Sprintf("%s-%d", bag.AdminNode, rand.Intn(200000000))
	xfer := &dpn.DPNReplicationTransfer{
		FromNode: bag.AdminNode,
		ToNode: testUtil.DPNConfig.LocalNode,
		UUID: bag.UUID,
		ReplicationId: replicationId,
		FixityAlgorithm: "sha256",
		Status: "Requested",
		Protocol: "R",
		Link: linkPath,
	}
	// You have to be node admin to create the transfer request,
	// so use the admin client.
	return testUtil.RemoteAdminClients[bag.AdminNode].ReplicationTransferCreate(xfer)
}
