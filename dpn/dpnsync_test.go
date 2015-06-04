package dpn_test

import (
	"fmt"
	"github.com/APTrust/bagman/dpn"
	"net/http"
	"testing"
)

var TEST_NODE_URLS = map[string]string {
	"chron": "http://127.0.0.1:8001",
	"hathi": "http://127.0.0.1:8002",
	"sdr":   "http://127.0.0.1:8003",
	"tdr":   "http://127.0.0.1:8004",
}
// APTrust user token for test nodes
// var TEST_NODES_TOKENS = map[string]string {
// 	"chron": "",
// 	"hathi": "",
// 	"sdr":   "",
// 	"tdr":   "",
// }

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
