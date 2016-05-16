package dpn_test

import (
//	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	GOOD_BAG = "00000000-0000-4000-a000-000000000001.tar"
	BAG_MISSING_DATA_FILE = "00000000-0000-4000-a000-000000000002.tar"
	BAG_MISSING_MANIFEST256 = "00000000-0000-4000-a000-000000000003.tar"
	BAG_MISSING_TAGS = "00000000-0000-4000-a000-000000000004.tar"
	BAG_MISSING_TAG_MANIFEST = "00000000-0000-4000-a000-000000000005.tar"
	BAG_BAD_DPN_TAGS = "00000000-0000-4000-a000-000000000006.tar"
)

func getBagPath(whichBag string) (string, error) {
	return bagman.RelativeToAbsPath(filepath.Join("dpn", "testdata", whichBag))
}

func cleanup(result *dpn.ValidationResult) {
	if _, err := os.Stat(result.UntarredPath); os.IsNotExist(err) {
		return
	}
	os.RemoveAll(result.UntarredPath)
}

func TestValidate_Good(t *testing.T) {
	bagPath, err := getBagPath(GOOD_BAG)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := dpn.NewValidationResult(bagPath, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(result)
	result.ValidateBag()
	if !result.IsValid() {
		for _, message := range result.ErrorMessages {
			t.Errorf(message)
		}
		t.Errorf("Bag should be valid.")
	}
}

func TestValidate_BagMissingDataFile(t *testing.T) {
	bagPath, err := getBagPath(BAG_MISSING_DATA_FILE)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := dpn.NewValidationResult(bagPath, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(result)
	result.ValidateBag()
	if result.IsValid() {
		t.Errorf("Bag should not be valid.")
	}
	if len(result.ErrorMessages) != 2 {
		t.Errorf("Bag should have exactly 2 error messages")
		return
	}
	if !strings.Contains(result.ErrorMessages[0], "checksum") {
		t.Errorf("ValidationResult should have noted bad checksum")
	}
	if !strings.Contains(result.ErrorMessages[1], "no such file") {
		t.Errorf("ValidationResult should have noted missing file")
	}
}

func TestValidate_BagMissingManifest256(t *testing.T) {
	bagPath, err := getBagPath(BAG_MISSING_MANIFEST256)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := dpn.NewValidationResult(bagPath, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(result)
	result.ValidateBag()
	if result.IsValid() {
		t.Errorf("Bag should not be valid.")
	}
	if len(result.ErrorMessages) != 1 {
		t.Errorf("Bag should have exactly 1 error message")
		return
	}
	if !strings.Contains(result.ErrorMessages[0],
		"Manifest file 'manifest-sha256.txt' is missing.") {
		t.Errorf("ValidationResult should have noted missing manifest-sha256.txt")
	}
}

func TestValidate_BagMissingTags(t *testing.T) {
	bagPath, err := getBagPath(BAG_MISSING_TAGS)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := dpn.NewValidationResult(bagPath, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(result)
	result.ValidateBag()
	if result.IsValid() {
		t.Errorf("Bag should not be valid.")
	}

	if len(result.ErrorMessages) != 2 {
		t.Errorf("Bag should have exactly 2 error messages")
		return
	}
	if !strings.Contains(result.ErrorMessages[0], "'DPN-Object-ID' is missing") {
		t.Errorf("ValidationResult should have noted missing DPN-Object-ID tag")
	}
	if !strings.Contains(result.ErrorMessages[1], "'Version-Number' is missing") {
		t.Errorf("ValidationResult should have noted missing Version-Number tag")
	}
}

func TestValidate_BagMissingTagManifest(t *testing.T) {
	bagPath, err := getBagPath(BAG_MISSING_TAG_MANIFEST)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := dpn.NewValidationResult(bagPath, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(result)
	result.ValidateBag()
	if result.IsValid() {
		t.Errorf("Bag should not be valid.")
	}
	if len(result.ErrorMessages) != 1 {
		t.Errorf("Bag should have exactly 1 error message")
		return
	}
	if !strings.Contains(result.ErrorMessages[0],
		"'tagmanifest-sha256.txt' is missing") {
		t.Errorf("ValidationResult should have noted missing tagmanifest-sha256.txt")
	}
}

func TestValidate_BagWithBadDPNTags(t *testing.T) {
	bagPath, err := getBagPath(BAG_BAD_DPN_TAGS)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := dpn.NewValidationResult(bagPath, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(result)
	result.ValidateBag()
	if result.IsValid() {
		t.Errorf("Bag should not be valid.")
	}
	if len(result.ErrorMessages) != 6 {
		t.Errorf("Bag should have exactly 6 error message")
		return
	}
	if result.ErrorMessages[0] != "DPN tag DPN-Object-ID must match bag name." {
		t.Errorf("ValidationResult should have noted DPN tag DPN-Object-ID must match bag name.")
	}
	if result.ErrorMessages[1] != "DPN tag Local-ID cannot be empty." {
		t.Errorf("ValidationResult should have noted DPN tag Local-ID cannot be empty.")
	}
	if result.ErrorMessages[2] != "DPN tag Ingest-Node-Name cannot be empty." {
		t.Errorf("ValidationResult should have noted DPN tag Ingest-Node-Name cannot be empty.")
	}
	if result.ErrorMessages[3] != "DPN tag Version-Number must be an integer." {
		t.Errorf("ValidationResult should have noted DPN tag Version-Number must be an integer.")
	}
	if result.ErrorMessages[4] != "DPN tag First-Version-Object-ID must be a valid Version 4 UUID." {
		t.Errorf("ValidationResult should have noted DPN tag First-Version-Object-ID must be a valid Version 4 UUID.")
	}
	if result.ErrorMessages[5] != "DPN tag Bag-Type must be data, rights, or interpretive." {
		t.Errorf("ValidationResult should have noted DPN tag Bag-Type must be data, rights, or interpretive.")
	}
}


func TestValidate_Digest(t *testing.T) {
	bagPath, err := getBagPath(GOOD_BAG)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := dpn.NewValidationResult(bagPath, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(result)

	// Calling this unpacks the bag
	result.ValidateBag()

	result.CalculateTagManifestDigest("")
	expected := "cc6f9d63a699a50eb8ae475eb143bda08c54992e94c996c439c33b06bff5b66a"
	if result.TagManifestChecksum != expected {
		t.Errorf("Got tag manifest checksum '%s', expected '%s'",
			result.TagManifestChecksum, expected)
	}

	result.CalculateTagManifestDigest("GeorgeWBush")
	expected = "47656f7267655742757368cc6f9d63a699a50eb8ae475eb143bda08c54992e94c996c439c33b06bff5b66a"
	if result.TagManifestChecksum != expected {
		t.Errorf("Got tag manifest checksum '%s', expected '%s'",
			result.TagManifestChecksum, expected)
	}
}
