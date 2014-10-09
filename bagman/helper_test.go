// This package contains some helper functions and common vars
// for our unit tests.
package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"github.com/crowdmob/goamz/aws"
)

var config bagman.Config
var fluctusUrl string = "http://localhost:3000"

// Our test fixture describes a bag that includes the following file paths
var expectedPaths [4]string = [4]string{
	"data/metadata.xml",
	"data/object.properties",
	"data/ORIGINAL/1",
	"data/ORIGINAL/1-metadata.xml",
}

func awsEnvAvailable() (envVarsOk bool) {
	_, err := aws.EnvAuth()
	return err == nil
}
