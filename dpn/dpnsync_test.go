package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagman/dpn"
	"net/http"
	"testing"
	"time"
)

var TEST_NODE_URLS = map[string]string {
	"chron": "http://127.0.0.1:8001",
	"hathi": "http://127.0.0.1:8002",
	"sdr":   "http://127.0.0.1:8003",
	"tdr":   "http://127.0.0.1:8004",
}


var skipSyncMessagePrinted = false

func runSyncTests(t *testing.T) bool {
	config := loadConfig(t, configFile)
	_, err := http.Get(config.RestClient.LocalServiceURL)
	if !canRunSyncTests("aptrust", config.RestClient.LocalServiceURL, err) {
		return false
	}
	for nodeNamespace, url := range TEST_NODE_URLS {
		_, err := http.Get(url)
		if !canRunSyncTests(nodeNamespace, url, err) {
			return false
		}
	}
	return true
}

func canRunSyncTests(nodeNamespace string, url string, err error) (bool) {
	if err != nil {
		if skipSyncMessagePrinted == false {
			skipSyncMessagePrinted = true
			fmt.Printf("Skipping DPN sync integration tests: "+
				"%s server is not running at %s\n", nodeNamespace, url)
			fmt.Printf("Run the run_cluster.sh script in " +
				"DPN-REST/dpnode to get a local cluster running.")
		}
		return false
	}
	return true
}

func newDPNSync(t *testing.T) (*dpn.DPNSync) {
	// loadConfig and configFile are defined in dpnrestclient_test.go
	config := loadConfig(t, configFile)

	// Hack in our API token. On the local test cluster, APTrust uses
	// the same token for all nodes.
	config.RemoteNodeTokens["chron"] = config.RestClient.LocalAuthToken
	config.RemoteNodeTokens["hathi"] = config.RestClient.LocalAuthToken
	config.RemoteNodeTokens["sdr"] = config.RestClient.LocalAuthToken
	config.RemoteNodeTokens["tdr"] = config.RestClient.LocalAuthToken

	dpnSync, err := dpn.NewDPNSync(config)
	if err != nil {
		t.Error(err)
		return nil
	}

	for namespace, _ := range config.RemoteNodeTokens {
		if dpnSync.RemoteClients[namespace] == nil {
			t.Errorf("Remote client for node '%s' is missing", namespace)
			return nil
		}
	}
	setTestNodeUrls(dpnSync)
	return dpnSync
}

// Point our test node clients toward our local cluster instead of
// the actual URLs of the remote nodes.
func setTestNodeUrls(dpnSync *dpn.DPNSync) {
	for nodeNamespace := range dpnSync.RemoteClients {
		remoteClient := dpnSync.RemoteClients[nodeNamespace]
		remoteClient.HostUrl = TEST_NODE_URLS[nodeNamespace]
	}
}

func TestNewDPNSync(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
}

func TestGetAllNodes(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		t.Error(err)
	}
	if len(nodes) != 5 {
		t.Errorf("Expected 5 nodes, got %d", len(nodes))
	}
}

func TestUpdateLastPullDate(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		t.Error(err)
		return
	}
	if len(nodes) != 5 {
		t.Errorf("Expected 5 nodes, got %d", len(nodes))
		return
	}
	someNode := nodes[2]
	origLastPullDate := someNode.LastPullDate
	newLastPullDate := origLastPullDate.Add(-12 * time.Hour)

	updatedNode, err := dpnSync.UpdateLastPullDate(someNode, newLastPullDate)
	if err != nil {
		t.Error(err)
		return
	}
	if updatedNode.LastPullDate != newLastPullDate {
		t.Errorf("Expected LastPullDate %s, got %s",
			newLastPullDate, updatedNode.LastPullDate)
	}
}

func TestSyncBags(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		t.Error(err)
		return
	}
	for _, node := range nodes {
		if node.Namespace == "aptrust" {
			continue
		}
		aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
		node.LastPullDate = aLongTimeAgo
		_, err := dpnSync.LocalClient.DPNNodeUpdate(node)
		if err != nil {
			t.Errorf("Error setting last pull date to 1999: %v", err)
			return
		}
		bagsSynched, err := dpnSync.SyncBags(node)
		if err != nil {
			t.Errorf("Error synching bags for node %s: %v", node.Namespace, err)
		}
		if len(bagsSynched) != 8 {
			t.Errorf("Synched %d bags for node %s. Expected %d.", len(bagsSynched), node.Namespace, 8)
		}
		updatedNode, err := dpnSync.LocalClient.DPNNodeGet(node.Namespace)
		if err != nil {
			t.Errorf("Can't check timestamp. Error getting node: %v", err)
		}
		if updatedNode.LastPullDate == aLongTimeAgo {
			t.Errorf("LastPullDate was not updated for %s", node.Namespace)
		}
		for _, remoteBag := range(bagsSynched) {
			localBag, _ := dpnSync.LocalClient.DPNBagGet(remoteBag.UUID)
			if localBag == nil {
				t.Errorf("Bag %s didn't make into local registry", remoteBag.UUID)
			}
			if localBag.UpdatedAt != remoteBag.UpdatedAt {
				t.Errorf("Bag %s isn't up to date in local registry", remoteBag.UUID)
			}
		}
	}
}

func TestSyncReplicationRequests(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		t.Error(err)
		return
	}
	for _, node := range nodes {
		if node.Namespace == "aptrust" {
			continue
		}
		aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
		node.LastPullDate = aLongTimeAgo
		_, err := dpnSync.LocalClient.DPNNodeUpdate(node)
		if err != nil {
			t.Errorf("Error setting last pull date to 1999: %v", err)
			return
		}
		xfersSynched, err := dpnSync.SyncReplicationRequests(node)
		if err != nil {
			t.Errorf("Error synching replication requests for node %s: %v",
				node.Namespace, err)
		}
		if len(xfersSynched) != 24 {
			t.Errorf("Synched %d replication requests for %s. Expected %d.",
				len(xfersSynched), node.Namespace, 24)
		}
		for _, xfer := range(xfersSynched) {
			localCopy, _ := dpnSync.LocalClient.ReplicationTransferGet(xfer.ReplicationId)
			if localCopy == nil {
				t.Errorf("Xfer %s didn't make into local registry", xfer.ReplicationId)
			}
			if xfer.UpdatedAt != localCopy.UpdatedAt {
				t.Errorf("Xfer %s isn't up to date in local registry", xfer.ReplicationId)
			}
		}
	}
}

func TestSyncRestoreRequests(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		t.Error(err)
		return
	}
	for _, node := range nodes {
		if node.Namespace == "aptrust" {
			continue
		}
		aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
		node.LastPullDate = aLongTimeAgo
		_, err := dpnSync.LocalClient.DPNNodeUpdate(node)
		if err != nil {
			t.Errorf("Error setting last pull date to 1999: %v", err)
			return
		}
		xfersSynched, err := dpnSync.SyncRestoreRequests(node)
		if err != nil {
			t.Errorf("Error synching restore requests for node %s: %v",
				node.Namespace, err)
		}
		if len(xfersSynched) != 4 {
			t.Errorf("Synched %d restore requests for %s. Expected %d.",
				len(xfersSynched), node.Namespace, 4)
		}
		for _, xfer := range(xfersSynched) {
			localCopy, _ := dpnSync.LocalClient.RestoreTransferGet(xfer.RestoreId)
			if localCopy == nil {
				t.Errorf("Xfer %s didn't make into local registry", xfer.RestoreId)
			}
			if xfer.UpdatedAt != localCopy.UpdatedAt {
				t.Errorf("Xfer %s isn't up to date in local registry", xfer.RestoreId)
			}
		}
	}
}
