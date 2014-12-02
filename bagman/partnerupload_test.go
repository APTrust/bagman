package bagman_test

import (
	//"fmt"
	"github.com/APTrust/bagman/bagman"
	"os"
	"path/filepath"
	"testing"
)

func partnerUploadConfigFile() (string) {
	configFile, _ := bagman.RelativeToAbsPath(
		filepath.Join("testdata", "partner_config_integration_test.conf"))
	return configFile
}

func partnerConfigForTest() (*bagman.PartnerConfig) {
	return &bagman.PartnerConfig{
		AwsAccessKeyId: "abc",
		AwsSecretAccessKey: "xyz",
		ReceivingBucket: "aptrust.receiving.xyz.edu",
		RestorationBucket: "aptrust.receiving.xyz.edu",
	}
}

func TestNewPartnerUploadFromConfigFile(t *testing.T) {
	// This test will fail if AWS keys are not set in the environment,
	// because they are not set in the config file.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return
	}
 	_, err := bagman.NewPartnerUploadFromConfigFile(partnerUploadConfigFile(), false)
	if err != nil {
		t.Error(err)
	}
}

func TestNewPartnerUploadWithConfig(t *testing.T) {
	partnerConfig := partnerConfigForTest()
 	_, err := bagman.NewPartnerUploadWithConfig(partnerConfig, false)
	if err != nil {
		t.Error(err)
	}
}

func TestNewPartnerUploadLoadConfig(t *testing.T) {
	// This test will fail if AWS keys are not set in the environment,
	// because they are not set in the config file.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return
	}
	partnerConfig := partnerConfigForTest()
 	partnerUpload, err := bagman.NewPartnerUploadWithConfig(partnerConfig, false)
	if err != nil {
		t.Error(err)
	}
	// Load a new config from a file
	err = partnerUpload.LoadConfig(partnerUploadConfigFile())
	if err != nil {
		t.Error(err)
	}
}

func TestNewPartnerUploadValidateConfig(t *testing.T) {
	partnerConfig := partnerConfigForTest()
 	partnerUpload, err := bagman.NewPartnerUploadWithConfig(partnerConfig, false)
	if err != nil {
		t.Error(err)
	}

	// Clear these out for this test, so PartnerUpload can't read them.
	// We want to see that validation fails when these are missing.
	awsKey := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "")
	// And make sure we restore them...
	defer os.Setenv("AWS_ACCESS_KEY_ID", awsKey)
	defer os.Setenv("AWS_SECRET_ACCESS_KEY", awsSecret)

	// Validation should fail on missing AWS credentials
	// and/or missing receiving bucket.
	partnerUpload.PartnerConfig.AwsAccessKeyId = ""
	err = partnerUpload.ValidateConfig()
	if err == nil {
		t.Errorf("Validation should have failed on missing Access Key")
	}

	partnerUpload.PartnerConfig.AwsAccessKeyId = "abc"
	partnerUpload.PartnerConfig.AwsSecretAccessKey = ""
	err = partnerUpload.ValidateConfig()
	if err == nil {
		t.Errorf("Validation should have failed on missing Secret Key")
	}

	partnerUpload.PartnerConfig.AwsSecretAccessKey = "xyz"
	partnerUpload.PartnerConfig.ReceivingBucket = ""
	err = partnerUpload.ValidateConfig()
	if err == nil {
		t.Errorf("Validation should have failed on missing Receiving Bucket")
	}
}

func TestNewPartnerUploadFile(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return
	}
 	partnerUpload, err := bagman.NewPartnerUploadFromConfigFile(partnerUploadConfigFile(), false)
	if err != nil {
		t.Error(err)
	}
	partnerUpload.Test = true // turn off output
	tarFile, _ := bagman.RelativeToAbsPath(
		filepath.Join("testdata", "example.edu.sample_good.tar"))
	file, err := os.Open(tarFile)
	if err != nil {
		t.Error(err)
	}
	md5, err := partnerUpload.UploadFile(file)
	if err != nil {
		t.Error(err)
	}
	if md5 != "48c876800900b64c17c9933143ca168a" {
		t.Errorf("Expected md5 sum '48c876800900b64c17c9933143ca168a', got '%s'", md5)
	}
}

func TestNewPartnerUploadFiles(t *testing.T) {

}
