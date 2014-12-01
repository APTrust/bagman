package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
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
	if partnerConfig.ReceivingBucket != "aptrust.receive.test.edu" {
		t.Errorf("AwsAccessKeyId: Expected 'aptrust.receive.test.edu', got '%s'",
			partnerConfig.ReceivingBucket)
	}
	if partnerConfig.RestorationBucket != "aptrust.restore.test.edu" {
		t.Errorf("AwsAccessKeyId: Expected 'aptrust.restore.test.edu', got '%s'",
			partnerConfig.RestorationBucket)
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
	if len(warnings) != 6 {
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
	if !strings.HasPrefix(warnings[4], "AwsReceivingBucket is missing") {
		t.Errorf("Did not get expected warning about missing AwsReceivingBucket")
	}
	if !strings.HasPrefix(warnings[5], "AwsRestorationBucket is missing") {
		t.Errorf("Did not get expected warning about missing AwsRestorationBucket")
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
