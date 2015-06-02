package dpn_test

import (
	"github.com/APTrust/bagman/dpn"
	"testing"
)

func TestNewDPNSync(t *testing.T) {
	// loadConfig and configFile are defined in dpnrestclient_test.go
	config := loadConfig(t, configFile)
	sync, err := dpn.NewDPNSync(config)
	if err != nil {
		t.Error(err)
	}
	for namespace, _ := range config.RemoteNodeTokens {
		if sync.RemoteClients[namespace] == nil {
			t.Errorf("Remote client for node '%s' is missing", namespace)
		}
	}
}
