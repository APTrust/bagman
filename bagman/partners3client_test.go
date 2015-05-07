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
	config := &bagman.PartnerConfig{
		AwsAccessKeyId: "abc",
		AwsSecretAccessKey: "xyz",
		ReceivingBucket: "aptrust.receiving.xyz.edu",
		RestorationBucket: "aptrust.receiving.xyz.edu",
		DownloadDir: "~/tmp",
	}
	config.ExpandFilePaths()
	return config
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

func TestPartnerS3ClientUploadListAndDelete(t *testing.T) {
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

	// Test upload
	err = client.UploadFile(file)
	if err != nil {
		t.Error(err)
	}

	// Test list. There should be only a handful of files in this test bucket.
	keys, err := client.List(client.PartnerConfig.ReceivingBucket, 500)
	if err != nil {
		t.Errorf("Error trying to list receiving bucket: %v", err)
	}
	foundFile := false
	for _, key := range keys {
		if key.Key == "example.edu.sample_good.tar" {
			foundFile = true
			break
		}
	}
	if !foundFile {
		t.Errorf("Uploaded file 'example.edu.sample_good.tar' not found in test bucket.")
	}

	// Test delete
	err = client.Delete(client.PartnerConfig.ReceivingBucket, "example.edu.sample_good.tar")
	if err != nil {
		t.Errorf("Error deleting example.edu.sample_good.tar from S3: %v", err)
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

	// Download with no checksum
	checksum, err := client.DownloadFile("aptrust.receiving.test.test.edu",
		"virginia.edu.uva-lib_2274765.tar", "none")
	if err != nil {
		t.Error(err)
	}
	if checksum != "" {
		t.Errorf("DownloadFile should have returned an empty checksum")
	}

	// Download with md5 - our test file is about 9.5kb
	checksum, err = client.DownloadFile("aptrust.receiving.test.test.edu",
		"virginia.edu.uva-lib_2274765.tar", "md5")
	if err != nil {
		t.Error(err)
	}
	if checksum != "3f43304f3f7c8d51111d0846e13cb74e" {
		t.Errorf("Expected md5 '3f43304f3f7c8d51111d0846e13cb74e', got '%s'", checksum)
	}

	// Download with sha256 - our test file is about 9.5kb
	checksum, err = client.DownloadFile("aptrust.receiving.test.test.edu",
		"virginia.edu.uva-lib_2274765.tar", "sha256")
	if err != nil {
		t.Error(err)
	}
	if checksum != "6f87f5341df2558967da27b12939d5e88b3af06592104041e57043af150f9309" {
		t.Errorf("Expected sha256 '6f87f5341df2558967da27b12939d5e88b3af06592104041e57043af150f9309', got '%s'", checksum)
	}

	checksum, err = client.DownloadFile("aptrust.receiving.test.test.edu",
		"virginia.edu.uva-lib_2274765.tar", "invalid_algo")
	if err == nil {
		t.Error("DownloadFile should have returned error for invalid checksum algorithm")
	}

}
