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
//	"time"
)

var skipRestMessagePrinted = false

func runRestTests(url string) bool {
	_, err := http.Get(url)
	if err != nil {
		if skipRestMessagePrinted == false {
			skipRestMessagePrinted = true
			fmt.Printf("Skipping DPN REST integration tests: "+
				"DPN REST server is not running at %s\n", url)
		}
		return false
	}
	return true
}

func getClient(t *testing.T) (*dpn.DPNRestClient) {
	// If you want to debug, change ioutil.Discard to os.Stdout
	// to see log output from the client.
	config := loadConfig(t, "dpn/dpn_config.json")
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
