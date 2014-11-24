package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
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

func TestBadChecksums(t *testing.T) {
	validator, err := bagman.NewValidator(sampleBadChecksums)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleNoTitle)
	}
	if !strings.Contains(validator.ErrorMessage, "The following checksums could not be verified:") {
		t.Errorf("File '%s' should have generated a missing title error.",
			sampleBadChecksums)
	}
}

func TestMissingDataFile(t *testing.T) {
	validator, err := bagman.NewValidator(sampleMissingDataFile)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleMissingDataFile)
	}
	if !strings.Contains(validator.ErrorMessage, "The following checksums could not be verified:") {
		t.Errorf("File '%s' should have generated an error describing a missing file.",
			sampleMissingDataFile)
	}
}

func TestNoBagInfo(t *testing.T) {
	validator, err := bagman.NewValidator(sampleNoBagInfo)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleNoBagInfo)
	}
	if !strings.Contains(validator.ErrorMessage, "Unable to find tagfile bag-info.txt") {
		t.Errorf("File '%s' should have generated an error describing a missing bag-info.txt file.",
			sampleNoBagInfo)
	}
}

func TestNoBagit(t *testing.T) {
	validator, err := bagman.NewValidator(sampleNoBagit)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleNoBagit)
	}
	if !strings.Contains(validator.ErrorMessage, "Bag is missing bagit.txt file") {
		t.Errorf("File '%s' should have generated an error describing a missing bagit.txt file.",
			sampleNoBagit)
	}
}

func TestNoMd5Manifest(t *testing.T) {
	validator, err := bagman.NewValidator(sampleNoMd5Manifest)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleNoMd5Manifest)
	}
	if validator.ErrorMessage != "Required checksum file manifest-md5.txt is missing." {
		t.Errorf("File '%s' should have generated an error describing a missing manifest-md5.txt file.",
			sampleNoMd5Manifest)
		t.Errorf(validator.ErrorMessage)
	}
}

func TestNoAPTrustInfo(t *testing.T) {
	validator, err := bagman.NewValidator(sampleNoAPTrustInfo)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleNoAPTrustInfo)
	}
	if !strings.Contains(validator.ErrorMessage, "Unable to find tagfile aptrust-info.txt") {
		t.Errorf("File '%s' should have generated an error describing a missing aptrust-info.txt file.",
			sampleNoAPTrustInfo)
		t.Errorf(validator.ErrorMessage)
	}
}

func TestNoDataDir(t *testing.T) {
	validator, err := bagman.NewValidator(sampleNoDataDir)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == true {
		t.Errorf("Bag '%s' should NOT be valid, but validator says it is", sampleNoDataDir)
	}
	if validator.ErrorMessage != "Bag is missing the data directory, which should contain the payload files." {
		t.Errorf("File '%s' should have generated an error describing a missing data directory.",
			sampleNoDataDir)
		t.Errorf(validator.ErrorMessage)
	}
}

// Make sure we can validate a bag that is not yet tarred.
func TestValidateFromDirectory(t *testing.T) {
	validator, err := bagman.NewValidator(sampleGoodUntarred)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.IsValid() == false {
		t.Errorf("%s should be valid, but got error: %s",
			sampleGoodUntarred, validator.ErrorMessage)
	}
}


func TestUntarredDir(t *testing.T) {
	validator, err := bagman.NewValidator(sampleGood)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if strings.HasSuffix(validator.UntarredDir(), "bagman/testdata/example.edu.sample_good") == false {
		t.Errorf("UntarredDir() should be 'bagman/testdata/example.edu.sample_good', but got %s", validator.UntarredDir())
	}
}

func TestInstitutionDomain(t *testing.T) {
	validator, err := bagman.NewValidator(sampleGood)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	domain, err := validator.InstitutionDomain()
	if err != nil {
		t.Error(err)
	}
	if domain != "example.edu" {
		t.Errorf("InstitutionDomain() should have returned 'example.edu', but got %s",
			domain)
	}
}

func TestTarFileName(t *testing.T) {
	validator, err := bagman.NewValidator(sampleGood)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.TarFileName() != "example.edu.sample_good.tar" {
		t.Errorf("Tar file name should be example.edu.sample_good.tar")
	}

	validator, err = bagman.NewValidator(sampleGoodUntarred)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	if validator.TarFileName() != "example.edu.sample_good.tar" {
		t.Errorf("Tar file name should be example.edu.sample_good.tar")
	}

}

func TestFileType(t *testing.T) {
	validator, err := bagman.NewValidator(sampleGood)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	fileType, err := validator.FileType()
	if err != nil {
		t.Error(err)
	}
	if fileType != bagman.VAL_TYPE_TAR {
		t.Errorf("File type for %s should be tar", sampleGood)
	}

	validator, err = bagman.NewValidator(sampleGoodUntarred)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	fileType, err = validator.FileType()
	if err != nil {
		t.Error(err)
	}
	if fileType != bagman.VAL_TYPE_DIR {
		t.Errorf("File type for %s should be directory", sampleGoodUntarred)
	}

	pathToJsonFile := filepath.Join(testDataPath, "intel_obj.json")
	validator, err = bagman.NewValidator(pathToJsonFile)
	if err != nil {
		t.Errorf("Error creating validator: %s", err)
		return
	}
	fileType, err = validator.FileType()
	if err == nil {
		t.Error("Invalid file type should have returned an error.")
	}
	if fileType != bagman.VAL_TYPE_ERR {
		t.Errorf("File type for %s should be invalid", pathToJsonFile)
	}

}
