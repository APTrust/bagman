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
	GOOD_BAG = "00000000-0000-0000-0000-000000000001.tar"
	BAG_MISSING_DATA_FILE = "00000000-0000-0000-0000-000000000002.tar"
	BAG_MISSING_MANIFEST256 = "00000000-0000-0000-0000-000000000003.tar"
	BAG_MISSING_TAGS = "00000000-0000-0000-0000-000000000004.tar"
	BAG_MISSING_TAG_MANIFEST = "00000000-0000-0000-0000-000000000005.tar"
)

func getBagPath(whichBag string) (string, error) {
	return bagman.RelativeToAbsPath(filepath.Join("dpn", "testdata", whichBag))
}

func cleanup(validator *dpn.ValidationResult) {
	if _, err := os.Stat(validator.UntarredPath); os.IsNotExist(err) {
		return
	}
	os.RemoveAll(validator.UntarredPath)
}

func TestValidate_Good(t *testing.T) {
	bagPath, err := getBagPath(GOOD_BAG)
	if err != nil {
		t.Error(err)
		return
	}
	validator, err := dpn.NewValidationResult(bagPath)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(validator)
	validator.ValidateBag()
	if !validator.IsValid() {
		for _, message := range validator.ErrorMessages {
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
	validator, err := dpn.NewValidationResult(bagPath)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(validator)
	validator.ValidateBag()
	if validator.IsValid() {
		t.Errorf("Bag should not be valid.")
	}
	if len(validator.ErrorMessages) != 2 {
		t.Errorf("Bag should have exactly 2 error messages")
		return
	}
	if !strings.Contains(validator.ErrorMessages[0], "checksum") {
		t.Errorf("ValidationResult should have noted bad checksum")
	}
	if !strings.Contains(validator.ErrorMessages[1], "no such file") {
		t.Errorf("ValidationResult should have noted missing file")
	}
}

func TestValidate_BagMissingManifest256(t *testing.T) {
	bagPath, err := getBagPath(BAG_MISSING_MANIFEST256)
	if err != nil {
		t.Error(err)
		return
	}
	validator, err := dpn.NewValidationResult(bagPath)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(validator)
	validator.ValidateBag()
	if validator.IsValid() {
		t.Errorf("Bag should not be valid.")
	}
	if len(validator.ErrorMessages) != 1 {
		t.Errorf("Bag should have exactly 1 error message")
		return
	}
	if !strings.Contains(validator.ErrorMessages[0],
		"Manifest 'manifest-sha256.txt' does not exist") {
		t.Errorf("ValidationResult should have noted missing manifest-sha256.txt")
	}
}

func TestValidate_BagMissingTags(t *testing.T) {
	bagPath, err := getBagPath(BAG_MISSING_TAGS)
	if err != nil {
		t.Error(err)
		return
	}
	validator, err := dpn.NewValidationResult(bagPath)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(validator)
	validator.ValidateBag()
	if validator.IsValid() {
		t.Errorf("Bag should not be valid.")
	}
	if len(validator.ErrorMessages) != 2 {
		t.Errorf("Bag should have exactly 2 error messages")
		return
	}
	if !strings.Contains(validator.ErrorMessages[0], "'DPN-Object-ID' is missing") {
		t.Errorf("ValidationResult should have noted missing DPN-Object-ID tag")
	}
	if !strings.Contains(validator.ErrorMessages[1], "'Version-Number' is missing") {
		t.Errorf("ValidationResult should have noted missing Version-Number tag")
	}
}

func TestValidate_BagMissingTagManifest(t *testing.T) {
	bagPath, err := getBagPath(BAG_MISSING_TAG_MANIFEST)
	if err != nil {
		t.Error(err)
		return
	}
	validator, err := dpn.NewValidationResult(bagPath)
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanup(validator)
	validator.ValidateBag()
	if validator.IsValid() {
		t.Errorf("Bag should not be valid.")
	}
	// for _, m := range(validator.ErrorMessages) {
	// 	fmt.Println(m)
	// }
	if len(validator.ErrorMessages) != 1 {
		t.Errorf("Bag should have exactly 1 error message")
		return
	}
	if !strings.Contains(validator.ErrorMessages[0],
		"'tagmanifest-sha256.txt' is missing") {
		t.Errorf("ValidationResult should have noted missing tagmanifest-sha256.txt")
	}
}
