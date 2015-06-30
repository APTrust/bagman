package dpn_test

// Common functions for dpn_test package

import (
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/crowdmob/goamz/aws"
	"testing"
)

var config bagman.Config
var fluctusUrl string = "http://localhost:3000"

func awsEnvAvailable() (envVarsOk bool) {
	_, err := aws.EnvAuth()
	return err == nil
}

func loadConfig(t *testing.T, configPath string) (*dpn.DPNConfig) {
	if dpnConfig != nil {
		return dpnConfig
	}
	var err error
	dpnConfig, err = dpn.LoadConfig(configPath)
	if err != nil {
		t.Errorf("Error loading %s: %v\n", configPath, err)
		return nil
	}

	// Turn this off to suppress tons of debug messages.
	// dpnConfig.LogToStderr = false

	return dpnConfig
}
