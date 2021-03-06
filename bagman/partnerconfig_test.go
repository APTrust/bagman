package bagman_test

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
	"os"
	"strings"
	"testing"
)

func getAbsPath(relativePath string) (string, error) {
	bagmanHome, err := bagman.BagmanHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(bagmanHome, relativePath), nil
}

func TestLoadPartnerConfigGood(t *testing.T) {
	filePath, err := getAbsPath(filepath.Join("testdata", "partner_config_valid.conf"))
	if err != nil {
		t.Errorf("Can't get path to partner config file: %v", err)
	}
	partnerConfig, err := bagman.LoadPartnerConfig(filePath)
	if err != nil {
		t.Error(err)
	}
	if partnerConfig.AwsAccessKeyId != "123456789XYZ" {
		t.Errorf("AwsAccessKeyId: Expected '123456789XYZ', got '%s'",
			partnerConfig.AwsAccessKeyId)
	}
	if partnerConfig.AwsSecretAccessKey != "THIS KEY INCLUDES SPACES AND DOES NOT NEED QUOTES" {
		t.Errorf("AwsAccessKeyId: Expected 'THIS KEY INCLUDES SPACES AND DOES NOT NEED QUOTES', got '%s'",
			partnerConfig.AwsSecretAccessKey)
	}
	// Test that value is correct and quotes are stripped.
	if partnerConfig.ReceivingBucket != "aptrust.receiving.testbucket.edu" {
		t.Errorf("AwsAccessKeyId: Expected 'aptrust.receiving.testbucket.edu', got '%s'",
			partnerConfig.ReceivingBucket)
	}
	if partnerConfig.RestorationBucket != "aptrust.restore.testbucket.edu" {
		t.Errorf("AwsAccessKeyId: Expected 'aptrust.restore.testbucket.edu', got '%s'",
			partnerConfig.RestorationBucket)
	}
}

func TestLoadPartnerConfigWrongFileType(t *testing.T) {
	filePath, err := getAbsPath(filepath.Join("testdata", "intel_obj.json"))
	if err != nil {
		t.Errorf("Can't get path to partner config file: %v", err)
	}
	_, err = bagman.LoadPartnerConfig(filePath)
	if err == nil {
		t.Errorf("LoadPartnerConfig should have returned error describing invalid format.")
	}
}

func TestLoadPartnerConfigMissingFile(t *testing.T) {
	filePath, err := getAbsPath(filepath.Join("testdata", "_non_existent_file.conf_"))
	if err != nil {
		t.Errorf("Can't get path to partner config file: %v", err)
	}
	_, err = bagman.LoadPartnerConfig(filePath)
	if err == nil {
		t.Errorf("LoadPartnerConfig should have returned error saying the file cannot be found.")
	}
}

func TestLoadAwsFromEnv(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		fmt.Println("Skipping AWS env test. Env vars are not set.")
		return
	}
	filePath, err := getAbsPath(filepath.Join("testdata", "partner_config_invalid.conf"))
	if err != nil {
		t.Errorf("Can't get path to partner config file: %v", err)
	}
	partnerConfig, err := bagman.LoadPartnerConfig(filePath)
	if err != nil {
		t.Error(err)
	}
	if partnerConfig.AwsAccessKeyId != "" {
		t.Errorf("Test precondition is invalid. AwsAccessKeyId has a value.")
	}
	if partnerConfig.AwsSecretAccessKey != "" {
		t.Errorf("Test precondition is invalid. AwsSecretAccessKey has a value.")
	}
	partnerConfig.LoadAwsFromEnv()
	if partnerConfig.AwsAccessKeyId == "" {
		t.Errorf("Failed to load AwsAccessKeyId from environment.")
	}
	if partnerConfig.AwsSecretAccessKey == "" {
		t.Errorf("Failed to load AwsSecretAccessKey from environment.")
	}
}

func TestLoadPartnerConfigBad(t *testing.T) {
	filePath, err := getAbsPath(filepath.Join("testdata", "partner_config_invalid.conf"))
	if err != nil {
		t.Errorf("Can't get path to partner config file: %v", err)
	}
	partnerConfig, err := bagman.LoadPartnerConfig(filePath)
	if err != nil {
		t.Error(err)
	}
       // Make sure we get warnings on unexpected settings and on
       // expected settings that are not there.
	warnings := partnerConfig.Warnings()
	if len(warnings) != 7 {
		t.Errorf("Expected 6 warnings, got %d", len(warnings))
	}
	if warnings[0] != "Invalid setting: FavoriteTeam = The home team" {
		t.Errorf("Did not get expected warning about invalid setting")
	}
	if warnings[1] != "Invalid setting: FavoriteFlavor = Green" {
		t.Errorf("Did not get expected warning about invalid setting")
	}
	if !strings.HasPrefix(warnings[2], "AwsAccessKeyId") {
		t.Errorf("Did not get expected warning about missing AwsAccessKeyId")
	}
	if !strings.HasPrefix(warnings[3], "AwsSecretAccessKey is missing") {
		t.Errorf("Did not get expected warning about missing AwsSecretAccessKey")
	}
	if !strings.HasPrefix(warnings[4], "ReceivingBucket is missing") {
		t.Errorf("Did not get expected warning about missing ReceivingBucket")
	}
	if !strings.HasPrefix(warnings[5], "RestorationBucket is missing") {
		t.Errorf("Did not get expected warning about missing RestorationBucket")
	}
	if !strings.HasPrefix(warnings[6], "DownloadDir is missing") {
		t.Errorf("Did not get expected warning about missing DownloadDir")
	}
}

func TestPartnerConfigValidate(t *testing.T) {
	partnerConfig := &bagman.PartnerConfig{
		AwsAccessKeyId: "abc",
		AwsSecretAccessKey: "xyz",
		ReceivingBucket: "aptrust.receiving.xyz.edu",
		RestorationBucket: "aptrust.receiving.xyz.edu",
		DownloadDir: "/home/josie/tmp",
	}

	// Clear these out for this test, so PartnerConfig can't read them.
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
	partnerConfig.AwsAccessKeyId = ""
	err := partnerConfig.Validate()
	if err == nil {
		t.Errorf("Validation should have failed on missing Access Key")
	}

	partnerConfig.AwsAccessKeyId = "abc"
	partnerConfig.AwsSecretAccessKey = ""
	err = partnerConfig.Validate()
	if err == nil {
		t.Errorf("Validation should have failed on missing Secret Key")
	}

	partnerConfig.AwsSecretAccessKey = "xyz"
	partnerConfig.ReceivingBucket = ""
	err = partnerConfig.Validate()
	if err == nil {
		t.Errorf("Validation should have failed on missing Receiving Bucket")
	}

	partnerConfig.ReceivingBucket = "123"
	partnerConfig.RestorationBucket = ""
	err = partnerConfig.Validate()
	if err == nil {
		t.Errorf("Validation should have failed on missing Restoration Bucket")
	}

	partnerConfig.RestorationBucket = "blah"
	partnerConfig.DownloadDir = ""
	err = partnerConfig.Validate()
	if err == nil {
		t.Errorf("Validation should have failed on missing DownloadDir")
	}
}
