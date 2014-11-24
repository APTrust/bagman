package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"strings"
	"testing"
)

/* ------ NOTE -------
Many vars used here, such as gopath, testDataPath, sampleGood, sampleNoBagit, etc.
are defined in bag_test.
*/


// Make sure we can untar properly formatted tar files.
func TestValidTarFile(t *testing.T) {
	validator, err := bagman.NewValidator(sampleGood)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == false {
		t.Errorf("Bag should be valid, but validator says it isn't: %s",
			validator.ErrorMessage)
	}
}


// Make sure Untar doesn't blow up when it gets an invalid
// or corrupt tar file. It should return a TarResult with an
// Error property.
func TestInvalidTarFile(t *testing.T) {
	validator, err := bagman.NewValidator(invalidTarFile)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag should NOT be valid, but validator says it is")
	}
}


// Make sure each of the bad bags produces an error in the BagReadResult.
// The underlying bagins library prints some warnings to stderr, so we
// include a note that those are expected.
func TestBadBagsReturnError(t *testing.T) {
	for _, tarFile := range badFiles {
		validator, err := bagman.NewValidator(tarFile)
		if err != nil {
			t.Errorf("Error creating validator: %s", err)
			return
		}
		if validator.IsValid() == true {
			t.Errorf("Bag '%s' should NOT be valid, but validator says it is", tarFile)
		}
		if validator.ErrorMessage == "" {
			t.Errorf("Invalid bag '%s' should have a specific error message", tarFile)
		}
	}
}

func TestBadFolderName(t *testing.T) {
	validator, err := bagman.NewValidator(sampleWrongFolderName)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleWrongFolderName)
	}
	if !strings.Contains(validator.ErrorMessage, "should untar to a folder named") {
		t.Errorf("Untarring file '%s' should have generated an 'incorrect file name' error.",
			sampleWrongFolderName)
	}
}

func TestBadAccessValue(t *testing.T) {
	validator, err := bagman.NewValidator(sampleBadAccess)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleWrongFolderName)
	}
	if !strings.Contains(validator.ErrorMessage, "access (rights) value") {
		t.Errorf("File '%s' should have generated an 'invalid access value' error.",
			sampleBadAccess)
	}
}

func TestMissingTitle(t *testing.T) {
	validator, err := bagman.NewValidator(sampleNoTitle)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleNoTitle)
	}
	if !strings.Contains(validator.ErrorMessage, "Title is missing") {
		t.Errorf("File '%s' should have generated a missing title error.",
			sampleNoTitle)
	}
}
