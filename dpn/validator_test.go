package dpn_test

import (
//	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"os"
	"path/filepath"
	"testing"
)

const (
	GOOD_BAG = "00000000-0000-0000-0000-000000000001.tar"
)

func getBagPath(whichBag string) (string, error) {
	return bagman.RelativeToAbsPath(filepath.Join("dpn", "testdata", whichBag))
}

func cleanup(validator *dpn.Validator) {
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
	validator, err := dpn.NewValidator(bagPath)
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
