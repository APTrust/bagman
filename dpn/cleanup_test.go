package dpn_test

import (
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"testing"
)

func TestDeleteReplicatedBags(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	requestedConfig := "test"
	dpnConfig := loadConfig(t, CONFIG_FILE)
	procUtil := bagman.NewProcessUtil(&requestedConfig)
	cleanup, err := dpn.NewCleanup(procUtil, dpnConfig)
	if err != nil {
		t.Errorf("Could not create Cleanup object: %v", err)
		return
	}
	cleanup.DeleteReplicatedBags()
}
