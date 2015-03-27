package dpn_test

import (
	"github.com/APTrust/bagman/bagman"
	"github.com/crowdmob/goamz/aws"
)

var config bagman.Config
var fluctusUrl string = "http://localhost:3000"

func awsEnvAvailable() (envVarsOk bool) {
	_, err := aws.EnvAuth()
	return err == nil
}
