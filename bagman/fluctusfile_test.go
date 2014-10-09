package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"path/filepath"
	"testing"
)

func TestBagName(t *testing.T) {
	fluctusFile := bagman.FluctusFile{}
	fluctusFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	bagname, err := fluctusFile.BagName()
	if err != nil {
		t.Error(err)
	}
	if bagname != "cin.675812" {
		t.Errorf("BagName returned '%s'; expected 'cin.675812'", bagname)
	}
}

func TestInstitutionId(t *testing.T) {
	fluctusFile := bagman.FluctusFile{}
	fluctusFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	instId, err := fluctusFile.InstitutionId()
	if err != nil {
		t.Errorf(err.Error())
	}
	if instId != "uc.edu" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu'", instId)
	}
}

func TestOriginalPath(t *testing.T) {
	fluctusFile := bagman.FluctusFile{}
	fluctusFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	origPath, err := fluctusFile.OriginalPath()
	if err != nil {
		t.Errorf(err.Error())
	}
	if origPath != "data/object.properties" {
		t.Errorf("OriginalPath returned some kinda shizzle. Expected 'data/object.properties', got '%s'",
			origPath)
	}
}


func TestGetChecksum(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	intelObj, err := bagman.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	if intelObj == nil {
		return
	}
	fluctusFile := intelObj.FluctusFiles[1]

	// MD5
	md5Checksum := fluctusFile.GetChecksum("md5")
	if md5Checksum == nil {
		t.Errorf("GetChecksum did not return md5 sum")
	}
	if md5Checksum.Digest != "c6d8080a39a0622f299750e13aa9c200" {
		t.Errorf("GetChecksum did not return md5 sum")
	}

	// SHA256
	sha256Checksum := fluctusFile.GetChecksum("sha256")
	if sha256Checksum == nil {
		t.Errorf("GetChecksum did not return sha256 sum")
	}
	if sha256Checksum.Digest != "a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790" {
		t.Errorf("GetChecksum did not return sha256 sum")
	}

	// bogus checksum
	bogusChecksum := fluctusFile.GetChecksum("bogus")
	if bogusChecksum != nil {
		t.Errorf("GetChecksum returned something it shouldn't have")
	}
}

func TestPreservationStorageFileName(t *testing.T) {
	fluctusFile := bagman.FluctusFile{}
	fluctusFile.URI = ""
	fileName, err := fluctusFile.PreservationStorageFileName()
	if err == nil {
		t.Errorf("PreservationStorageFileName() should have returned an error")
	}
	fluctusFile.URI = "https://s3.amazonaws.com/aptrust.test.preservation/a58a7c00-392f-11e4-916c-0800200c9a66"
	fileName, err = fluctusFile.PreservationStorageFileName()
	if err != nil {
		t.Errorf("PreservationStorageFileName() returned an error: %v", err)
	}
	expected := "a58a7c00-392f-11e4-916c-0800200c9a66"
	if fileName != expected {
		t.Errorf("PreservationStorageFileName() returned '%s', expected '%s'",
			fileName, expected)
	}
}

func TestTotalFileSize(t *testing.T) {
	filepath := filepath.Join("testdata", "intel_obj.json")
	obj, err := bagman.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if obj.TotalFileSize() != 686 {
		t.Errorf("TotalFileSize() returned '%d', expected 686", obj.TotalFileSize())
	}
}
