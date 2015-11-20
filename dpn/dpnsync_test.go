package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagman/dpn"
	"net/http"
	"testing"
	"time"
)

var skipSyncMessagePrinted = false

const (
	BAG_COUNT     = 6
	REPL_COUNT    = 24
	RESTORE_COUNT = 4
)

func runSyncTests(t *testing.T) bool {
	config := loadConfig(t, configFile)
	_, err := http.Get(config.RestClient.LocalServiceURL)
	if !canRunSyncTests("aptrust", config.RestClient.LocalServiceURL, err) {
		return false
	}
	for nodeNamespace, url := range config.RemoteNodeURLs {
		if url == "" {
			continue
		}
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
			fmt.Printf("**** Skipping DPN sync integration tests: "+
				"%s server is not running at %s\n", nodeNamespace, url)
			fmt.Println("     Run the run_cluster.sh script in " +
				"DPN-REST/dpnode to get a local cluster running.")
		}
		return false
	}
	return true
}

func newDPNSync(t *testing.T) (*dpn.DPNSync) {
	// loadConfig and configFile are defined in dpnrestclient_test.go
	config := loadConfig(t, configFile)

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
	return dpnSync
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

func TestLocalNodeName(t *testing.T) {
	// Local node name is set in dpn_config.json
	config := loadConfig(t, configFile)
	dpnSync := newDPNSync(t)
	if config == nil || dpnSync == nil {
		t.Errorf("Can't complete local name test")
		return
	}
	if dpnSync.LocalNodeName() != config.LocalNode {
		t.Errorf("LocalNodeName() returned '%s', expected '%s'",
			dpnSync.LocalNodeName(), config.LocalNode)
	}
}

func TestRemoteNodeNames(t *testing.T) {
	// Local node name is set in dpn_config.json
	config := loadConfig(t, configFile)
	dpnSync := newDPNSync(t)
	if config == nil || dpnSync == nil {
		t.Errorf("Can't complete remote names test")
		return
	}
	remoteNodeNames := dpnSync.RemoteNodeNames()
	for name, _ := range config.RemoteNodeURLs {
		nameIsPresent := false
		for _, remoteName := range remoteNodeNames {
			if name == remoteName {
				nameIsPresent = true
				break
			}
		}
		if !nameIsPresent {
			t.Errorf("Node %s is in config file, but was not returned " +
				"by RemoteNodeNames()", name)
		}
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
		bagsSynched, err := dpnSync.SyncBags(node)
		if err != nil {
			t.Errorf("Error synching bags for node %s: %v", node.Namespace, err)
		}
		expectedBagCount := BAG_COUNT
		if node.Namespace == "hathi" {
			// From test fixtures, one of the six
			// Hathi bags is already in our registry.
			expectedBagCount = BAG_COUNT - 1
		}
		if len(bagsSynched) != expectedBagCount {
			t.Errorf("Synched %d bags for node %s. Expected %d.",
				len(bagsSynched), node.Namespace, expectedBagCount)
		}
		for _, remoteBag := range(bagsSynched) {
			if remoteBag == nil {
				t.Errorf("Remote bag is nil")
				continue
			}
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
		if len(xfersSynched) != REPL_COUNT {
			t.Errorf("Synched %d replication requests for %s. Expected %d.",
				len(xfersSynched), node.Namespace, REPL_COUNT)
		}
		for _, xfer := range(xfersSynched) {
			if xfer == nil {
				t.Errorf("Xfer is nil")
				return
			}
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
		if len(xfersSynched) != RESTORE_COUNT {
			t.Errorf("Synched %d restore requests for %s. Expected %d.",
				len(xfersSynched), node.Namespace, RESTORE_COUNT)
		}
		for _, xfer := range(xfersSynched) {
			if xfer == nil {
				t.Errorf("xfer is nil")
				continue
			}
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

func TestSyncEverythingFromNode(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}

	recordCount := 10
	mock := NewMock(dpnSync)
	err := mock.AddRecordsToNodes(dpnSync.RemoteNodeNames(), recordCount)
	if err != nil {
		t.Errorf("Error creating mocks: %v", err)
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
		syncResult := dpnSync.SyncEverythingFromNode(node)

		// Bags
		if syncResult.BagSyncError != nil {
			t.Errorf("Got unexpected bag-sync error from node %s: %v",
				node.Namespace, syncResult.BagSyncError)
		}
		if len(syncResult.Bags) != recordCount {
			t.Errorf("Expected %d bags from %s, got %d",
				BAG_COUNT, node.Namespace, len(syncResult.Bags))
		}

		// Replication Transfers
		if syncResult.ReplicationSyncError != nil {
			t.Errorf("Got unexpected replication transfer-sync error from node %s: %v",
				node.Namespace, syncResult.ReplicationSyncError)
		}
		if len(syncResult.ReplicationTransfers) != recordCount {
			t.Errorf("Expected %d replication transfers from %s, got %d",
				REPL_COUNT, node.Namespace, len(syncResult.ReplicationTransfers))
		}

		// Bags
		if syncResult.RestoreSyncError != nil {
			t.Errorf("Got unexpected restore transfer-sync error from node %s: %v",
				node.Namespace, syncResult.RestoreSyncError)
		}
		if len(syncResult.RestoreTransfers) != recordCount {
			t.Errorf("Expected %d restore transfers from %s, got %d",
				RESTORE_COUNT, node.Namespace, len(syncResult.RestoreTransfers))
		}

		// Timestamp update
		updatedNode, err := dpnSync.LocalClient.DPNNodeGet(node.Namespace)
		if err != nil {
			t.Errorf("Can't check timestamp. Error getting node: %v", err)
		}
		if updatedNode.LastPullDate == aLongTimeAgo {
			t.Errorf("LastPullDate was not updated for %s", node.Namespace)
		}
	}
}

func TestSyncWithError(t *testing.T) {
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

	// Pick one node to sync with, and set the API key for that node
	// to a value we know is invalid. This will cause the sync to fail.
	node := nodes[len(nodes) - 1]
	dpnSync.RemoteClients[node.Namespace].APIKey = "0000000000000000"

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	node.LastPullDate = aLongTimeAgo
	_, err = dpnSync.LocalClient.DPNNodeUpdate(node)
	if err != nil {
		t.Errorf("Error setting last pull date to 1999: %v", err)
		return
	}

	syncResult := dpnSync.SyncEverythingFromNode(node)
	if syncResult.BagSyncError == nil {
		t.Errorf("BagSyncError should not be nil")
	}
	if syncResult.ReplicationSyncError == nil {
		t.Errorf("ReplicationSyncError should not be nil")
	}
	if syncResult.RestoreSyncError == nil {
		t.Errorf("RestoreSyncError should not be nil")
	}

	// Because the sync failed (due to the bad API Key), the LastPullDate
	// on the node we tried to pull from should NOT be updated.
	updatedNode, err := dpnSync.LocalClient.DPNNodeGet(node.Namespace)
	if err != nil {
		t.Errorf("Can't check timestamp. Error getting node: %v", err)
	}
	if updatedNode.LastPullDate != aLongTimeAgo {
		t.Errorf("LastPullDate was updated when it should not have been")
	}
}


func TestHasSyncErrors(t *testing.T) {
	syncResult := &dpn.SyncResult{}
	if syncResult.HasSyncErrors() == true {
		t.Errorf("HasSyncErrors() returned true. Expected false.")
	}
	syncResult.BagSyncError = fmt.Errorf("Oops.")
	if syncResult.HasSyncErrors() == false {
		t.Errorf("HasSyncErrors() returned false. Expected true.")
	}
	syncResult.BagSyncError = nil
	syncResult.ReplicationSyncError = fmt.Errorf("Oops.")
	if syncResult.HasSyncErrors() == false {
		t.Errorf("HasSyncErrors() returned false. Expected true.")
	}
	syncResult.ReplicationSyncError = nil
	syncResult.RestoreSyncError = fmt.Errorf("Oops.")
	if syncResult.HasSyncErrors() == false {
		t.Errorf("HasSyncErrors() returned false. Expected true.")
	}
}
