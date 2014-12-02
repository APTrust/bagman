package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"os"
	"path/filepath"
	"testing"
)

func partnerS3ConfigFile() (string) {
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
		DownloadDir: "~/tmp",
	}
}

func TestNewPartnerS3ClientFromConfigFile(t *testing.T) {
	// This test will fail if AWS keys are not set in the environment,
	// because they are not set in the config file.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return
	}
 	_, err := bagman.NewPartnerS3ClientFromConfigFile(partnerS3ConfigFile(), false)
	if err != nil {
		t.Error(err)
	}
}

func TestNewPartnerS3ClientWithConfig(t *testing.T) {
	partnerConfig := partnerConfigForTest()
 	_, err := bagman.NewPartnerS3ClientWithConfig(partnerConfig, false)
	if err != nil {
		t.Error(err)
	}
}

func TestPartnerS3ClientLoadConfig(t *testing.T) {
	// This test will fail if AWS keys are not set in the environment,
	// because they are not set in the config file.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return
	}
	partnerConfig := partnerConfigForTest()
 	client, err := bagman.NewPartnerS3ClientWithConfig(partnerConfig, false)
	if err != nil {
		t.Error(err)
	}
	// Load a new config from a file
	err = client.LoadConfig(partnerS3ConfigFile())
	if err != nil {
		t.Error(err)
	}
}

func TestPartnerS3ClientUploadFile(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return
	}
 	client, err := bagman.NewPartnerS3ClientFromConfigFile(partnerS3ConfigFile(), false)
	if err != nil {
		t.Error(err)
	}
	client.Test = true // turn off output
	tarFile, _ := bagman.RelativeToAbsPath(
		filepath.Join("testdata", "example.edu.sample_good.tar"))
	file, err := os.Open(tarFile)
	if err != nil {
		t.Error(err)
	}
	md5, err := client.UploadFile(file)
	if err != nil {
		t.Error(err)
	}
	if md5 != "48c876800900b64c17c9933143ca168a" {
		t.Errorf("Expected md5 sum '48c876800900b64c17c9933143ca168a', got '%s'", md5)
	}
}

func TestPartnerS3ClientUploadFiles(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return
	}
 	client, err := bagman.NewPartnerS3ClientFromConfigFile(partnerS3ConfigFile(), false)
	if err != nil {
		t.Error(err)
	}
	client.Test = true // turn off output
	file0, _ := bagman.RelativeToAbsPath(filepath.Join("testdata", "example.edu.multipart.b01.of02.tar"))
	file1, _ := bagman.RelativeToAbsPath(filepath.Join("testdata", "example.edu.multipart.b02.of02.tar"))
	files := make([]string, 2)
	files[0] = file0
	files[1] = file1
	succeeded, failed := client.UploadFiles(files)
	if succeeded != 2 {
		t.Errorf("Expected 2 files to have uploaded, but %d actually succeeded", succeeded)
	}
	if failed != 0 {
		t.Errorf("%d files failed to upload")
	}
}

func TestPartnerS3ClientDownloadFile(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return
	}
 	client, err := bagman.NewPartnerS3ClientFromConfigFile(partnerS3ConfigFile(), false)
	if err != nil {
		t.Error(err)
	}
	client.Test = true // turn off output

}
